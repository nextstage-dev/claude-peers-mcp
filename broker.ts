#!/usr/bin/env bun
/**
 * claude-peers broker daemon (federated)
 *
 * A singleton HTTP server backed by SQLite.
 * Tracks all registered Claude Code peers across the fleet and routes messages between them.
 *
 * Binds to 0.0.0.0 for fleet-wide access over Tailscale.
 * Requires CLAUDE_PEERS_TOKEN for authentication.
 *
 * Auto-launched by the MCP server if running locally.
 * Run directly: bun broker.ts
 */

import { Database } from "bun:sqlite";
import { messageContractVersion } from "./shared/message-contract.ts";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  SetNicknameRequest,
  SetContextRequest,
  SetStateRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  ClaimMessagesRequest,
  ClaimMessagesResponse,
  AckMessagesRequest,
  AckMessagesResponse,
  UnregisterRequest,
  LeaseCredentials,
  Peer,
  Message,
} from "./shared/types.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${process.env.HOME}/.claude-peers.db`;
const AUTH_TOKEN = process.env.CLAUDE_PEERS_TOKEN ?? "";
const REQUIRE_AUTH =
  process.env.CLAUDE_PEERS_REQUIRE_AUTH === "1" ||
  process.env.CLAUDE_PEERS_REQUIRE_AUTH === "true";
const TTL_MINUTES = parseInt(process.env.CLAUDE_PEERS_TTL_MINUTES ?? "20", 10);
const STALE_PEER_MS = Math.max(1, Number.isFinite(TTL_MINUTES) ? TTL_MINUTES : 20) * 60 * 1000;
const parsedLeaseTtl = Number(process.env.CLAUDE_PEERS_LEASE_TTL_MS ?? "45000");
const LEASE_TTL_MS = Number.isFinite(parsedLeaseTtl) && parsedLeaseTtl > 0 ? parsedLeaseTtl : 45_000;
const parsedVisibilityTimeout = Number(
  process.env.CLAUDE_PEERS_VISIBILITY_TIMEOUT_MS ?? "30000",
);
const VISIBILITY_TIMEOUT_MS =
  Number.isFinite(parsedVisibilityTimeout) && parsedVisibilityTimeout > 0
    ? parsedVisibilityTimeout
    : 30_000;
const CLAIM_BATCH_SIZE = 100;

if (REQUIRE_AUTH && !AUTH_TOKEN) {
  console.error("[claude-peers broker] FATAL: CLAUDE_PEERS_REQUIRE_AUTH is set but CLAUDE_PEERS_TOKEN is empty.");
  process.exit(1);
} else if (!AUTH_TOKEN) {
  console.error("[claude-peers broker] WARNING: No CLAUDE_PEERS_TOKEN set. Broker is unauthenticated.");
}

// --- Database setup ---

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 3000");

db.run(`
  CREATE TABLE IF NOT EXISTS peers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    cwd TEXT NOT NULL,
    git_root TEXT,
    tty TEXT,
    machine TEXT NOT NULL DEFAULT 'unknown',
    nickname TEXT NOT NULL DEFAULT '',
    context_window INTEGER,
    context_used INTEGER,
    context_note TEXT NOT NULL DEFAULT '',
    tier TEXT NOT NULL DEFAULT 'production',
    payload_version INTEGER NOT NULL DEFAULT 1,
    instance_id TEXT,
    parent_id TEXT,
    runtime TEXT,
    rings TEXT,
    blocked_on TEXT,
    blocked_since TEXT,
    summary TEXT NOT NULL DEFAULT '',
    registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
  )
`);

// Migrate: add machine column if it doesn't exist (for upgrades from pre-federation)
try {
  db.run("ALTER TABLE peers ADD COLUMN machine TEXT NOT NULL DEFAULT 'unknown'");
} catch {
  // Column already exists
}

// Migrate: add nickname column if it doesn't exist
try {
  db.run("ALTER TABLE peers ADD COLUMN nickname TEXT NOT NULL DEFAULT ''");
} catch {
  // Column already exists
}

// Migrate: add context metadata columns if they don't exist
try {
  db.run("ALTER TABLE peers ADD COLUMN context_window INTEGER");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE peers ADD COLUMN context_used INTEGER");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE peers ADD COLUMN context_note TEXT NOT NULL DEFAULT ''");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE peers ADD COLUMN tier TEXT NOT NULL DEFAULT 'production'");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE peers ADD COLUMN payload_version INTEGER NOT NULL DEFAULT 1");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE peers ADD COLUMN instance_id TEXT");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE peers ADD COLUMN parent_id TEXT");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE peers ADD COLUMN runtime TEXT");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE peers ADD COLUMN rings TEXT");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE peers ADD COLUMN blocked_on TEXT");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE peers ADD COLUMN blocked_since TEXT");
} catch {
  // Column already exists
}

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    claim_expires_at TEXT,
    claimed_by_lease_fingerprint TEXT,
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

try {
  db.run("ALTER TABLE messages ADD COLUMN claim_expires_at TEXT");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE messages ADD COLUMN claimed_by_lease_fingerprint TEXT");
} catch {
  // Column already exists
}
try {
  db.run("ALTER TABLE messages ADD COLUMN delivery_attempts INTEGER NOT NULL DEFAULT 0");
} catch {
  // Column already exists
}

// Additive migration keeps existing mail readable. Do not hide migration failures.
const messageColumns = new Set((db.query("PRAGMA table_info(messages)").all() as { name: string }[]).map(row => row.name));
for (const [name, definition] of Object.entries({ kind: "TEXT NOT NULL DEFAULT 'request'", reply_to_id: "INTEGER", client_message_id: "TEXT" })) {
  if (!messageColumns.has(name)) db.run(`ALTER TABLE messages ADD COLUMN ${name} ${definition}`);
}
db.run("CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_client_id ON messages(from_id, client_message_id) WHERE client_message_id IS NOT NULL");

db.run(`
  CREATE TABLE IF NOT EXISTS peer_leases (
    peer_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    lease_id TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (peer_id) REFERENCES peers(id) ON DELETE CASCADE
  )
`);

db.run(`
  CREATE INDEX IF NOT EXISTS idx_messages_delivery_claim
  ON messages (to_id, delivered, claim_expires_at, sent_at, id)
`);

function deletePeerAndUndeliveredMessages(id: string): void {
  db.run("DELETE FROM peer_leases WHERE peer_id = ?", [id]);
  db.run("DELETE FROM peers WHERE id = ?", [id]);
  db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [id]);
}

function peerIsStale(peer: Pick<Peer, "last_seen">): boolean {
  const lastSeen = new Date(peer.last_seen).getTime();
  return !Number.isFinite(lastSeen) || Date.now() - lastSeen > STALE_PEER_MS;
}

function touchPeer(id: string): void {
  updateLastSeen.run(new Date().toISOString(), id);
}

// Clean up stale peers on startup and before broker operations. Local host peers
// get a PID check; all peers are also bounded by last_seen TTL.
function cleanStalePeers() {
  const hostname = require("os").hostname();
  const peers = db.query("SELECT id, pid, machine, last_seen FROM peers").all() as {
    id: string;
    pid: number;
    machine: string;
    last_seen: string;
  }[];

  for (const peer of peers) {
    if (peerIsStale(peer)) {
      deletePeerAndUndeliveredMessages(peer.id);
      continue;
    }

    if (peer.machine === hostname) {
      // Local peer — check if PID is alive
      try {
        process.kill(peer.pid, 0);
      } catch {
        deletePeerAndUndeliveredMessages(peer.id);
      }
    }
  }
}

cleanStalePeers();

// Periodically clean stale peers (every 30s)
setInterval(cleanStalePeers, 30_000);

// --- Auth middleware ---

function checkAuth(req: Request): Response | null {
  if (!AUTH_TOKEN) return null; // No token configured = open (with warning)

  const auth = req.headers.get("Authorization");
  if (!auth || auth !== `Bearer ${AUTH_TOKEN}`) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  return null;
}

// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, nickname, context_window, context_used, context_note, tier, payload_version, instance_id, pid, cwd, git_root, tty, machine, summary, registered_at, last_seen, parent_id, runtime, rings, blocked_on, blocked_since)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const updateNickname = db.prepare(`
  UPDATE peers SET nickname = ? WHERE id = ?
`);

const updateContext = db.prepare(`
  UPDATE peers SET context_window = ?, context_used = ?, context_note = ? WHERE id = ?
`);

const updateHeartbeatContext = db.prepare(`
  UPDATE peers SET last_seen = ?, context_window = COALESCE(?, context_window), context_used = COALESCE(?, context_used) WHERE id = ?
`);

const updateState = db.prepare(`
  UPDATE peers SET blocked_on = ?, blocked_since = ? WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
`);

const deletePeerLease = db.prepare(`
  DELETE FROM peer_leases WHERE peer_id = ?
`);

const selectPeerLease = db.prepare(`
  SELECT peer_id, instance_id, lease_id, expires_at, created_at, updated_at
  FROM peer_leases WHERE peer_id = ?
`);

const insertPeerLease = db.prepare(`
  INSERT INTO peer_leases (peer_id, instance_id, lease_id, expires_at, created_at, updated_at)
  VALUES (?, ?, ?, ?, ?, ?)
`);

const renewPeerLease = db.prepare(`
  UPDATE peer_leases SET expires_at = ?, updated_at = ?
  WHERE peer_id = ? AND instance_id = ? AND lease_id = ?
`);

const selectAllPeers = db.prepare(`
  SELECT * FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT * FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT * FROM peers WHERE git_root = ?
`);

const selectPeersByMachine = db.prepare(`
  SELECT * FROM peers WHERE machine = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered, kind, reply_to_id, client_message_id)
  VALUES (?, ?, ?, ?, 0, ?, ?, ?)
`);
const selectMessageByClientId = db.prepare("SELECT id, to_id, text, kind, reply_to_id FROM messages WHERE from_id = ? AND client_message_id = ?");
const selectMessageById = db.prepare("SELECT id, from_id, to_id, kind FROM messages WHERE id = ?");

const selectUndelivered = db.prepare(`
  SELECT id, from_id, to_id, text, sent_at, delivered, kind, reply_to_id
  FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC, id ASC
`);

const markDelivered = db.prepare(`
  UPDATE messages
  SET delivered = 1, claim_expires_at = NULL, claimed_by_lease_fingerprint = NULL
  WHERE id = ?
`);

const selectClaimableMessages = db.prepare(`
  SELECT id, from_id, to_id, text, sent_at, delivered, kind, reply_to_id
  FROM messages
  WHERE to_id = ?
    AND delivered = 0
    AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
  ORDER BY sent_at ASC, id ASC
  LIMIT ${CLAIM_BATCH_SIZE}
`);

const claimMessage = db.prepare(`
  UPDATE messages
  SET claim_expires_at = ?,
      claimed_by_lease_fingerprint = ?,
      delivery_attempts = delivery_attempts + 1
  WHERE id = ?
    AND delivered = 0
    AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
`);

const ackClaimedMessage = db.prepare(`
  UPDATE messages
  SET delivered = 1, claim_expires_at = NULL, claimed_by_lease_fingerprint = NULL
  WHERE id = ?
    AND to_id = ?
    AND delivered = 0
    AND claimed_by_lease_fingerprint = ?
`);

// --- Generate peer ID ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

interface PeerLease {
  peer_id: string;
  instance_id: string;
  lease_id: string;
  expires_at: string;
  created_at: string;
  updated_at: string;
}

function getPeerLease(id: string): PeerLease | null {
  return selectPeerLease.get(id) as PeerLease | null;
}

function leaseFingerprint(leaseId: string): string {
  return new Bun.CryptoHasher("sha256").update(leaseId).digest("hex");
}

function leaseIsExpired(lease: PeerLease, now = Date.now()): boolean {
  const expiresAt = Date.parse(lease.expires_at);
  return !Number.isFinite(expiresAt) || expiresAt <= now;
}

function normalizeInstanceId(instanceId: unknown): string | null {
  if (typeof instanceId !== "string") return null;
  const trimmed = instanceId.trim();
  return /^[a-zA-Z0-9][a-zA-Z0-9._:-]{2,127}$/.test(trimmed) ? trimmed : null;
}

function requireLeaseOwner(id: string, credentials: LeaseCredentials): PeerLease | null {
  const lease = getPeerLease(id);
  if (!lease) return null;

  if (
    leaseIsExpired(lease) ||
    credentials.instance_id !== lease.instance_id ||
    credentials.lease_id !== lease.lease_id
  ) {
    throw new HttpError(409, "peer lease conflict");
  }
  return lease;
}

function extendLease(lease: PeerLease, now = new Date()): string {
  const expiresAt = new Date(now.getTime() + LEASE_TTL_MS).toISOString();
  const result = renewPeerLease.run(
    expiresAt,
    now.toISOString(),
    lease.peer_id,
    lease.instance_id,
    lease.lease_id,
  );
  if (result.changes !== 1) {
    throw new HttpError(409, "peer lease conflict");
  }
  return expiresAt;
}

function normalizeRequestedId(id: unknown): string | null {
  if (typeof id !== "string") return null;
  const trimmed = id.trim();
  return /^[a-zA-Z0-9_-]{3,64}$/.test(trimmed) ? trimmed : null;
}

function normalizeNickname(nickname: unknown): string {
  if (typeof nickname !== "string") return "";
  return nickname.trim().slice(0, 64);
}

function normalizeOptionalInt(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const num = typeof value === "number" ? value : Number(value);
  return Number.isInteger(num) && num >= 0 ? num : null;
}

function normalizeContextNote(note: unknown): string {
  if (typeof note !== "string") return "";
  return note.trim().slice(0, 120);
}

function normalizeTier(tier: unknown): "production" | "staging" | "infrastructure" {
  return tier === "staging" || tier === "infrastructure" ? tier : "production";
}

// Mirror of the client's auto-nickname (server.ts defaultNickname) so the broker
// can tell an operator-chosen nickname apart from a regenerated default.
// Keep in sync with server.ts cwdBasename/shortTty/defaultNickname.
function cwdBasename(cwd: string): string {
  return cwd.split(/[\\/]/).filter(Boolean).pop() || "home";
}

function shortTty(tty: string | null): string {
  if (!tty) return "no-tty";
  const digits = tty.match(/(\d+)$/)?.[1];
  return digits ? digits.padStart(3, "0") : tty.replace(/[^a-zA-Z0-9_-]/g, "-");
}

function defaultNickname(machine: string, cwd: string, tty: string | null): string {
  return `${machine}:${cwdBasename(cwd)}:${shortTty(tty)}`.slice(0, 64);
}

function normalizePayloadVersion(version: unknown): number {
  const num = typeof version === "number" ? version : Number(version);
  return Number.isInteger(num) && num > 0 ? num : 1;
}

function normalizeParentId(id: unknown): string | null {
  if (typeof id !== "string") return null;
  const trimmed = id.trim().slice(0, 64);
  return trimmed || null;
}

function normalizeRuntime(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim().slice(0, 32);
  return /^[a-zA-Z0-9_-]+$/.test(trimmed) ? trimmed : null;
}

function normalizeRings(value: unknown): string | null {
  let arr: unknown[] | null = null;
  if (Array.isArray(value)) {
    arr = value;
  } else if (typeof value === "string" && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) arr = parsed;
    } catch {
      return null;
    }
  }
  if (!arr) return null;
  const nums = arr.filter((v): v is number => Number.isInteger(v) && v >= 0 && v <= 3);
  return nums.length ? JSON.stringify(nums) : null;
}

function normalizeBlockedOn(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") return null;
  const trimmed = value.trim().slice(0, 128);
  return trimmed === "" ? null : trimmed;
}

function decodeRings(raw: unknown): number[] | null {
  if (Array.isArray(raw)) {
    return raw.filter((v): v is number => Number.isInteger(v) && v >= 0 && v <= 3);
  }
  if (typeof raw !== "string" || !raw) return null;
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed)
      ? parsed.filter((v: unknown): v is number => Number.isInteger(v) && v >= 0 && v <= 3)
      : null;
  } catch {
    return null;
  }
}

function decodePeer(
  row: Peer & { instance_id?: unknown; rings?: unknown },
): Peer {
  const { instance_id: _instanceId, ...publicPeer } = row;
  return { ...publicPeer, rings: decodeRings(row.rings) };
}

// --- Request handlers ---

type PeerIdentity = Pick<
  Peer,
  | "nickname"
  | "summary"
  | "context_window"
  | "context_used"
  | "context_note"
  | "parent_id"
  | "runtime"
  | "blocked_on"
  | "blocked_since"
> & { rings?: unknown };

function replacePeerRegistration(
  body: RegisterRequest,
  id: string,
  payloadVersion: number,
  instanceId: string | null,
): void {
  const nickname = normalizeNickname(body.nickname);
  const contextWindow = normalizeOptionalInt(body.context_window);
  const contextUsed = normalizeOptionalInt(body.context_used);
  const contextNote = normalizeContextNote(body.context_note);
  const tier = normalizeTier(body.tier);
  const now = new Date().toISOString();
  const machine = body.machine || "unknown";

  // Preserve operator-set identity metadata across re-registrations: the
  // delete-then-insert below would otherwise wipe nickname/summary/context on
  // every MCP restart and heartbeat-miss recovery. Capture the prior row first
  // (same id, else same pid+machine lineage), then carry over any field the
  // client sent empty or as its regenerated auto-default.
  const priorById = db
    .query(
      "SELECT nickname, summary, context_window, context_used, context_note, parent_id, runtime, rings, blocked_on, blocked_since FROM peers WHERE id = ?"
    )
    .get(id) as PeerIdentity | null;
  // Remove any existing registration for this PID + machine combo (re-registration)
  const existing = db
    .query(
      "SELECT id, nickname, summary, context_window, context_used, context_note, parent_id, runtime, rings, blocked_on, blocked_since FROM peers WHERE pid = ? AND machine = ? AND id <> ?"
    )
    .get(body.pid, machine, id) as (PeerIdentity & { id: string }) | null;

  deletePeer.run(id);
  if (existing) {
    deletePeerLease.run(existing.id);
    deletePeer.run(existing.id);
  }

  const prior = priorById ?? existing;
  const carriedNickname =
    nickname === "" || nickname === defaultNickname(machine, body.cwd, body.tty ?? null)
      ? (prior?.nickname ?? nickname)
      : nickname;
  const carriedSummary = body.summary ? body.summary : (prior?.summary ?? "");
  const carriedContextWindow = contextWindow ?? prior?.context_window ?? null;
  const carriedContextUsed = contextUsed ?? prior?.context_used ?? null;
  const carriedContextNote = contextNote === "" ? (prior?.context_note ?? "") : contextNote;
  const parentId = normalizeParentId(body.parent_id) ?? prior?.parent_id ?? null;
  const runtime = normalizeRuntime(body.runtime) ?? prior?.runtime ?? null;
  const rings = normalizeRings(body.rings) ?? (typeof prior?.rings === "string" ? prior.rings : normalizeRings(prior?.rings));
  const blockedOn = prior?.blocked_on ?? null;
  const blockedSince = prior?.blocked_since ?? null;

  insertPeer.run(
    id,
    carriedNickname,
    carriedContextWindow,
    carriedContextUsed,
    carriedContextNote,
    tier,
    payloadVersion,
    instanceId,
    body.pid,
    body.cwd,
    body.git_root,
    body.tty,
    machine,
    carriedSummary,
    now,
    now,
    parentId,
    runtime,
    rings,
    blockedOn,
    blockedSince
  );
}

function handleRegister(body: RegisterRequest): RegisterResponse {
  cleanStalePeers();

  const id = normalizeRequestedId(body.requested_id);
  if (!id) {
    throw new HttpError(400, "register requires a valid requested_id");
  }
  const payloadVersion = normalizePayloadVersion(body.payload_version);
  const usesLeaseProtocol = payloadVersion >= 2;
  const instanceId = usesLeaseProtocol ? normalizeInstanceId(body.instance_id) : null;
  if (usesLeaseProtocol && !instanceId) {
    throw new HttpError(400, "payload version 2 requires a valid instance_id");
  }

  const machine = body.machine || "unknown";
  const lineage = db
    .query("SELECT id FROM peers WHERE pid = ? AND machine = ? AND id <> ?")
    .get(body.pid, machine, id) as { id: string } | null;
  if (lineage) {
    const lineageLease = getPeerLease(lineage.id);
    if (
      lineageLease &&
      !leaseIsExpired(lineageLease) &&
      (!instanceId || lineageLease.instance_id !== instanceId)
    ) {
      throw new HttpError(409, "process lineage is owned by another peer lease");
    }
  }

  const currentLease = getPeerLease(id);
  if (!usesLeaseProtocol) {
    if (currentLease && !leaseIsExpired(currentLease)) {
      throw new HttpError(409, "peer id is owned by a leased runtime");
    }
    if (currentLease) deletePeerLease.run(id);
    replacePeerRegistration(body, id, payloadVersion, null);
    return { id };
  }

  if (currentLease && !leaseIsExpired(currentLease)) {
    if (currentLease.instance_id !== instanceId) {
      return {
        id,
        role: "standby",
        lease_id: null,
        lease_expires_at: currentLease.expires_at,
      };
    }
    if (body.lease_id !== currentLease.lease_id) {
      throw new HttpError(409, "peer lease conflict");
    }

    replacePeerRegistration(body, id, payloadVersion, instanceId);
    const leaseExpiresAt = extendLease(currentLease);
    return {
      id,
      role: "owner",
      lease_id: currentLease.lease_id,
      lease_expires_at: leaseExpiresAt,
    };
  }

  if (currentLease) {
    deletePeerLease.run(id);
  }
  replacePeerRegistration(body, id, payloadVersion, instanceId);

  const now = new Date();
  const leaseId = crypto.randomUUID();
  const leaseExpiresAt = new Date(now.getTime() + LEASE_TTL_MS).toISOString();
  insertPeerLease.run(
    id,
    instanceId,
    leaseId,
    leaseExpiresAt,
    now.toISOString(),
    now.toISOString(),
  );
  return {
    id,
    role: "owner",
    lease_id: leaseId,
    lease_expires_at: leaseExpiresAt,
  };
}

function handleHeartbeat(body: HeartbeatRequest): { ok: boolean; found: boolean } {
  const lease = requireLeaseOwner(body.id, body);
  const nowDate = new Date();
  const now = nowDate.toISOString();
  const window = body.context_window !== undefined ? normalizeOptionalInt(body.context_window) : undefined;
  const used = body.context_used !== undefined ? normalizeOptionalInt(body.context_used) : undefined;
  if (window !== undefined || used !== undefined) {
    const result = updateHeartbeatContext.run(now, window ?? null, used ?? null, body.id);
    if (result.changes > 0 && lease) extendLease(lease, nowDate);
    return { ok: true, found: result.changes > 0 };
  }
  const result = updateLastSeen.run(now, body.id);
  if (result.changes > 0 && lease) extendLease(lease, nowDate);
  return { ok: true, found: result.changes > 0 };
}

function handleSetState(body: SetStateRequest): { ok: boolean; found: boolean } {
  requireLeaseOwner(body.id, body);
  const blockedOn =
    body.blocked_on === undefined ? undefined : normalizeBlockedOn(body.blocked_on);
  const blockedSince =
    body.blocked_since === undefined
      ? undefined
      : typeof body.blocked_since === "string" && body.blocked_since.trim()
        ? body.blocked_since.trim()
        : null;
  const row = db
    .query("SELECT blocked_on, blocked_since FROM peers WHERE id = ?")
    .get(body.id) as { blocked_on: string | null; blocked_since: string | null } | null;
  if (!row) {
    return { ok: false, found: false };
  }
  const nextOn = blockedOn === undefined ? row.blocked_on : blockedOn;
  const nextSince =
    blockedSince !== undefined
      ? blockedSince
      : blockedOn === undefined
        ? row.blocked_since
        : blockedOn
          ? new Date().toISOString()
          : null;
  const result = updateState.run(nextOn, nextSince, body.id);
  return { ok: true, found: result.changes > 0 };
}

function handleSetSummary(body: SetSummaryRequest): void {
  requireLeaseOwner(body.id, body);
  updateSummary.run(body.summary, body.id);
}

function handleSetNickname(body: SetNicknameRequest): { ok: boolean; error?: string } {
  requireLeaseOwner(body.id, body);
  const result = updateNickname.run(normalizeNickname(body.nickname), body.id);
  if (result.changes === 0) {
    return { ok: false, error: "peer not found or stale" };
  }
  return { ok: true };
}

function handleSetContext(body: SetContextRequest): void {
  requireLeaseOwner(body.id, body);
  updateContext.run(
    normalizeOptionalInt(body.context_window),
    normalizeOptionalInt(body.context_used),
    normalizeContextNote(body.context_note),
    body.id
  );
}

function handleListPeers(body: ListPeersRequest): Peer[] {
  cleanStalePeers();

  let peers: Peer[];

  switch (body.scope) {
    case "fleet":
      peers = selectAllPeers.all() as Peer[];
      break;
    case "machine":
      if (body.machine) {
        peers = selectPeersByMachine.all(body.machine) as Peer[];
      } else {
        peers = selectAllPeers.all() as Peer[];
      }
      break;
    case "directory":
      peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      break;
    case "repo":
      if (body.git_root) {
        peers = selectPeersByGitRoot.all(body.git_root) as Peer[];
      } else {
        // No git root, fall back to directory
        peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      }
      break;
    default:
      peers = selectAllPeers.all() as Peer[];
  }

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  // For local peers, verify process is still alive
  const hostname = require("os").hostname();
  peers = peers.filter((p) => {
    if (peerIsStale(p)) {
      deletePeerAndUndeliveredMessages(p.id);
      return false;
    }

    if (p.machine === hostname) {
      try {
        process.kill(p.pid, 0);
        return true;
      } catch {
        deletePeerAndUndeliveredMessages(p.id);
        return false;
      }
    }
    // Remote peers — trust heartbeat-based cleanup
    return true;
  });

  // Optional: hide headless "idle, awaiting directive" zombies from tasking lists
  if (body.active_only) {
    const idleRe = /idle,?\s*awaiting directive/i;
    peers = peers.filter((p) => {
      const noTty = !p.tty || p.tty === "" || p.tty === "null";
      const idleSummary = idleRe.test(p.summary || "");
      // Keep if has a real tty OR summary is not the default idle boilerplate
      if (noTty && idleSummary) return false;
      return true;
    });
  }

  return peers.map(decodePeer);
}

const sendMessage = db.transaction((body: SendMessageRequest) => {
  if (!body || typeof body.from_id !== "string" || typeof body.to_id !== "string" || typeof body.text !== "string" || !body.text.trim() || Buffer.byteLength(body.text) > 65536) {
    throw new HttpError(400, "send requires sender, recipient and nonempty text up to 64 KiB");
  }
  const kind = body.kind ?? (body.reply_to_id !== undefined ? "reply" : "request");
  if (!["request", "reply", "notification"].includes(kind)) throw new HttpError(400, "invalid message kind");
  const replyTo = body.reply_to_id ?? null;
  if ((kind === "reply" && (!Number.isSafeInteger(replyTo) || Number(replyTo) < 1)) || (kind !== "reply" && replyTo !== null)) throw new HttpError(400, "reply requires a positive reply_to_id; other kinds cannot set it");
  const clientId = body.client_message_id ?? null;
  if (clientId !== null && (typeof clientId !== "string" || !clientId.trim() || clientId.length > 128)) throw new HttpError(400, "invalid client_message_id");
  requireLeaseOwner(body.from_id, body);
  if (clientId !== null) {
    const existing = selectMessageByClientId.get(body.from_id, clientId) as { id: number; to_id: string; text: string; kind: string; reply_to_id: number | null } | null;
    if (existing) {
      if (existing.to_id !== body.to_id || existing.text !== body.text || existing.kind !== kind || existing.reply_to_id !== replyTo) throw new HttpError(409, "client_message_id already used for a different message");
      return { ok: true, message_id: existing.id, duplicate: true };
    }
  }
  if (replyTo !== null) {
    const original = selectMessageById.get(replyTo) as { from_id: string; to_id: string; kind: string } | null;
    if (!original || original.kind !== "request" || original.to_id !== body.from_id || original.from_id !== body.to_id) throw new HttpError(400, "reply must reverse the participants of an existing request");
  }

  // A completed retry is returned above before stale-target pruning.
  cleanStalePeers();
  requireLeaseOwner(body.from_id, body);
  touchPeer(body.from_id);

  // Verify target exists
  const target = db.query("SELECT id, last_seen FROM peers WHERE id = ?").get(body.to_id) as
    | Pick<Peer, "id" | "last_seen">
    | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }
  if (peerIsStale(target)) {
    deletePeerAndUndeliveredMessages(target.id);
    return { ok: false, error: `Peer ${body.to_id} is stale` };
  }

  const inserted = insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString(), kind, replyTo, clientId);
  return { ok: true, message_id: Number(inserted.lastInsertRowid), duplicate: false };
});

function handleSendMessage(body: SendMessageRequest) {
  return sendMessage(body);
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  if (getPeerLease(body.id)) {
    throw new HttpError(409, "leased peers must use claim-messages");
  }
  touchPeer(body.id);
  const messages = selectUndelivered.all(body.id) as Message[];

  // Mark them as delivered
  for (const msg of messages) {
    markDelivered.run(msg.id);
  }

  return { messages };
}

const claimMessages = db.transaction(
  (id: string, leaseId: string, now: string, claimExpiresAt: string): Message[] => {
    const candidates = selectClaimableMessages.all(id, now) as Message[];
    const claimed: Message[] = [];
    const fingerprint = leaseFingerprint(leaseId);
    for (const message of candidates) {
      const result = claimMessage.run(claimExpiresAt, fingerprint, message.id, now);
      if (result.changes === 1) claimed.push(message);
    }
    return claimed;
  },
);

function handleClaimMessages(body: ClaimMessagesRequest): ClaimMessagesResponse {
  const lease = requireLeaseOwner(body.id, body);
  if (!lease) {
    throw new HttpError(409, "claim-messages requires a leased peer");
  }

  const now = new Date();
  const messages = claimMessages(
    body.id,
    lease.lease_id,
    now.toISOString(),
    new Date(now.getTime() + VISIBILITY_TIMEOUT_MS).toISOString(),
  );
  touchPeer(body.id);
  return { messages };
}

function handleAckMessages(body: AckMessagesRequest): AckMessagesResponse {
  const lease = requireLeaseOwner(body.id, body);
  if (!lease) {
    throw new HttpError(409, "ack-messages requires a leased peer");
  }
  if (!Array.isArray(body.message_ids)) {
    throw new HttpError(400, "ack-messages requires message_ids");
  }

  const messageIds = [
    ...new Set(
      body.message_ids.filter(
        (id): id is number => Number.isSafeInteger(id) && id > 0,
      ),
    ),
  ].slice(0, CLAIM_BATCH_SIZE);
  let acked = 0;
  const ackTransaction = db.transaction(() => {
    const fingerprint = leaseFingerprint(lease.lease_id);
    for (const messageId of messageIds) {
      acked += ackClaimedMessage.run(messageId, body.id, fingerprint).changes;
    }
  });
  ackTransaction();
  touchPeer(body.id);
  return { ok: true, acked };
}

function handleUnregister(body: UnregisterRequest): void {
  requireLeaseOwner(body.id, body);
  deletePeerLease.run(body.id);
  deletePeer.run(body.id);
}

// --- HTTP Server ---

Bun.serve({
  port: PORT,
  hostname: "0.0.0.0",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "POST") {
      if (path === "/health") {
        // Health check doesn't require auth
        return Response.json({ status: "ok", peers: (selectAllPeers.all() as Peer[]).length });
      }
      return new Response("claude-peers broker (federated)", { status: 200 });
    }

    // Auth check for all POST endpoints
    const authError = checkAuth(req);
    if (authError) return authError;

    try {
      const body = await req.json();

      switch (path) {
        case "/capabilities":
          return Response.json({ message_contract: messageContractVersion, claim_ack: true, idempotent_send: true });
        case "/register":
          return Response.json(handleRegister(body as RegisterRequest));
        case "/heartbeat":
          return Response.json(handleHeartbeat(body as HeartbeatRequest));
        case "/set-summary":
          handleSetSummary(body as SetSummaryRequest);
          return Response.json({ ok: true });
        case "/set-nickname":
          return Response.json(handleSetNickname(body as SetNicknameRequest));
        case "/set-context":
          handleSetContext(body as SetContextRequest);
          return Response.json({ ok: true });
        case "/set-state":
          return Response.json(handleSetState(body as SetStateRequest));
        case "/list-peers":
          return Response.json(handleListPeers(body as ListPeersRequest));
        case "/send-message":
          return Response.json(handleSendMessage(body as SendMessageRequest));
        case "/poll-messages":
          return Response.json(handlePollMessages(body as PollMessagesRequest));
        case "/claim-messages":
          return Response.json(handleClaimMessages(body as ClaimMessagesRequest));
        case "/ack-messages":
          return Response.json(handleAckMessages(body as AckMessagesRequest));
        case "/unregister":
          handleUnregister(body as UnregisterRequest);
          return Response.json({ ok: true });
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      const status = e instanceof HttpError ? e.status : 500;
      return Response.json({ error: msg }, { status });
    }
  },
});

console.error(`[claude-peers broker] listening on 0.0.0.0:${PORT} (db: ${DB_PATH})`);
