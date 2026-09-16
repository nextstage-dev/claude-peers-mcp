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
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  Peer,
  Message,
} from "./shared/types.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${process.env.HOME}/.claude-peers.db`;
const AUTH_TOKEN = process.env.CLAUDE_PEERS_TOKEN ?? "";

if (!AUTH_TOKEN) {
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

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

// Clean up stale peers on startup — for local peers, check PID; for remote, use last_seen
function cleanStalePeers() {
  const hostname = require("os").hostname();
  const peers = db.query("SELECT id, pid, machine, last_seen FROM peers").all() as {
    id: string;
    pid: number;
    machine: string;
    last_seen: string;
  }[];

  for (const peer of peers) {
    if (peer.machine === hostname) {
      // Local peer — check if PID is alive
      try {
        process.kill(peer.pid, 0);
      } catch {
        db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
        db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [peer.id]);
      }
    } else {
      // Remote peer — expire if no heartbeat for 5 minutes
      const staleMs = Date.now() - new Date(peer.last_seen).getTime();
      if (staleMs > 5 * 60 * 1000) {
        db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
        db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [peer.id]);
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
  INSERT INTO peers (id, pid, cwd, git_root, tty, machine, summary, registered_at, last_seen)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
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
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered)
  VALUES (?, ?, ?, ?, 0)
`);

const selectUndelivered = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

const markDelivered = db.prepare(`
  UPDATE messages SET delivered = 1 WHERE id = ?
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

// --- Request handlers ---

function handleRegister(body: RegisterRequest): RegisterResponse {
  const id = generateId();
  const now = new Date().toISOString();
  const machine = body.machine || "unknown";

  // Remove any existing registration for this PID + machine combo (re-registration)
  const existing = db
    .query("SELECT id FROM peers WHERE pid = ? AND machine = ?")
    .get(body.pid, machine) as { id: string } | null;
  if (existing) {
    deletePeer.run(existing.id);
  }

  insertPeer.run(id, body.pid, body.cwd, body.git_root, body.tty, machine, body.summary, now, now);
  return { id };
}

function handleHeartbeat(body: HeartbeatRequest): void {
  updateLastSeen.run(new Date().toISOString(), body.id);
}

function handleSetSummary(body: SetSummaryRequest): void {
  updateSummary.run(body.summary, body.id);
}

function handleListPeers(body: ListPeersRequest): Peer[] {
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
  return peers.filter((p) => {
    if (p.machine === hostname) {
      try {
        process.kill(p.pid, 0);
        return true;
      } catch {
        deletePeer.run(p.id);
        return false;
      }
    }
    // Remote peers — trust heartbeat-based cleanup
    return true;
  });
}

function handleSendMessage(body: SendMessageRequest): { ok: boolean; error?: string } {
  // Verify target exists
  const target = db.query("SELECT id FROM peers WHERE id = ?").get(body.to_id) as { id: string } | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }

  insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
  return { ok: true };
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  const messages = selectUndelivered.all(body.id) as Message[];

  // Mark them as delivered
  for (const msg of messages) {
    markDelivered.run(msg.id);
  }

  return { messages };
}

function handleUnregister(body: { id: string }): void {
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
        case "/register":
          return Response.json(handleRegister(body as RegisterRequest));
        case "/heartbeat":
          handleHeartbeat(body as HeartbeatRequest);
          return Response.json({ ok: true });
        case "/set-summary":
          handleSetSummary(body as SetSummaryRequest);
          return Response.json({ ok: true });
        case "/list-peers":
          return Response.json(handleListPeers(body as ListPeersRequest));
        case "/send-message":
          return Response.json(handleSendMessage(body as SendMessageRequest));
        case "/poll-messages":
          return Response.json(handlePollMessages(body as PollMessagesRequest));
        case "/unregister":
          handleUnregister(body as { id: string });
          return Response.json({ ok: true });
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

console.error(`[claude-peers broker] listening on 0.0.0.0:${PORT} (db: ${DB_PATH})`);
