#!/usr/bin/env bun
/**
 * claude-peers MCP server (federated)
 *
 * Spawned by Claude Code as a stdio MCP server (one per instance).
 * Connects to the shared broker daemon for peer discovery and messaging.
 * Declares claude/channel capability to push inbound messages immediately.
 *
 * Supports both local and remote brokers via CLAUDE_PEERS_BROKER_URL.
 *
 * Inbound delivery, in order of preference:
 *   1. Host-session inbox socket (CLAUDE_CODE_MESSAGING_SOCKET, Claude Code
 *      >= 2.1.224): works in every host, including the desktop app, with no
 *      launch flag. Each message is posted as a cross-session user message.
 *   2. claude/channel notification: only honored when the session was launched
 *      with `claude --dangerously-load-development-channels server:claude-peers`.
 *   3. check_messages tool: drains the local buffer on demand.
 *
 * Configure as a regular MCP server:
 *   { "claude-peers": { "command": "bun", "args": ["./server.ts"] } }
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import type {
  PeerId,
  Peer,
  RegisterResponse,
  PollMessagesResponse,
  Message,
} from "./shared/types.ts";
import {
  generateSummary,
  getGitBranch,
  getRecentFiles,
} from "./shared/summarize.ts";
import { fileURLToPath } from "node:url";
import { createConnection } from "node:net";
import { messageToolProperties, sendPeerMessage } from "./shared/message-contract.ts";

// --- Configuration ---

const BROKER_PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const BROKER_URL = process.env.CLAUDE_PEERS_BROKER_URL ?? `http://127.0.0.1:${BROKER_PORT}`;
const AUTH_TOKEN = process.env.CLAUDE_PEERS_TOKEN ?? "";
const MACHINE_NAME = process.env.CLAUDE_PEERS_MACHINE ?? require("os").hostname();
const DEFAULT_NICKNAME = process.env.CLAUDE_PEERS_NICKNAME ?? process.env.CLAUDE_PEER_NAME ?? "";
const DEFAULT_CONTEXT_WINDOW = parseOptionalInt(process.env.CLAUDE_PEERS_CONTEXT_WINDOW);
const DEFAULT_CONTEXT_USED = parseOptionalInt(process.env.CLAUDE_PEERS_CONTEXT_USED);
const DEFAULT_CONTEXT_NOTE = process.env.CLAUDE_PEERS_CONTEXT_NOTE ?? "";
const CHANNEL_DISABLED = process.env.CLAUDE_PEERS_DISABLE_CHANNEL === "1" ||
  process.env.CLAUDE_PEERS_DISABLE_CHANNEL === "true";
// Host-session inbox (Claude Code >= 2.1.224 cross-session messaging).
// Every session binds a Unix socket and exports its path and token into the
// environment of the MCP servers it spawns. Posting a `user` frame there lands
// in the host session as a peer interrupt in ANY host, terminal or desktop,
// with no --dangerously-load-development-channels flag. The session verifies
// the sender is its own descendant, so this server (a child of the session)
// is accepted even when the session bypasses permission prompts.
const HOST_INBOX_SOCKET = process.env.CLAUDE_CODE_MESSAGING_SOCKET ?? "";
const HOST_INBOX_TOKEN = process.env.CLAUDE_CODE_MESSAGING_TOKEN ?? "";
const HOST_INBOX_DISABLED = process.env.CLAUDE_PEERS_DISABLE_INBOX === "1" ||
  process.env.CLAUDE_PEERS_DISABLE_INBOX === "true";
const HOST_INBOX_WRITE_TIMEOUT_MS = 5000;
const CHANNEL_RESPONSE_DELAY_MS = Math.max(
  0,
  parseInt(process.env.CLAUDE_PEERS_RESPONSE_DELAY_MS ?? "0", 10) || 0
);
const POLL_INTERVAL_MS = parseIntervalMs(
  process.env.CLAUDE_PEERS_POLL_INTERVAL_MS,
  1000
);
const HEARTBEAT_INTERVAL_MS = parseIntervalMs(
  process.env.CLAUDE_PEERS_HEARTBEAT_INTERVAL_MS,
  15_000
);
const REGISTER_RETRY_MS = parseIntervalMs(
  process.env.CLAUDE_PEERS_REGISTER_RETRY_MS,
  5000
);
const BROKER_REQUEST_TIMEOUT_MS = parseIntervalMs(
  process.env.CLAUDE_PEERS_BROKER_TIMEOUT_MS,
  5000
);
const BROKER_SCRIPT = fileURLToPath(new URL("./broker.ts", import.meta.url));
const START_PARENT_PID = process.ppid;
const PROCESS_INSTANCE_ID = crypto.randomUUID();
const LOCAL_MESSAGE_BUFFER_MAX_COUNT = 100;
const LOCAL_MESSAGE_BUFFER_MAX_BYTES = 1024 * 1024;

// Detect if broker is local (only auto-launch local brokers)
const isLocalBroker = BROKER_URL.includes("127.0.0.1") || BROKER_URL.includes("localhost");

// --- Broker communication ---

function authHeaders(): Record<string, string> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (AUTH_TOKEN) {
    headers["Authorization"] = `Bearer ${AUTH_TOKEN}`;
  }
  return headers;
}

class BrokerFetchError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function brokerFetch<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BROKER_URL}${path}`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(BROKER_REQUEST_TIMEOUT_MS),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new BrokerFetchError(
      res.status,
      `Broker error (${path}): ${res.status} ${err}`,
    );
  }
  return res.json() as Promise<T>;
}

async function isBrokerAlive(): Promise<boolean> {
  try {
    const res = await fetch(`${BROKER_URL}/health`, { signal: AbortSignal.timeout(3000) });
    return res.ok;
  } catch {
    return false;
  }
}

async function ensureBroker(): Promise<void> {
  if (await isBrokerAlive()) {
    log("Broker already running");
    return;
  }

  if (!isLocalBroker) {
    throw new Error(`Remote broker at ${BROKER_URL} is not reachable. Cannot auto-launch a remote broker.`);
  }

  log("Starting broker daemon...");
  const proc = Bun.spawn([process.execPath, BROKER_SCRIPT], {
    stdio: ["ignore", "ignore", "inherit"],
  });

  proc.unref();

  // Wait for it to come up
  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 200));
    if (await isBrokerAlive()) {
      log("Broker started");
      return;
    }
  }
  throw new Error("Failed to start broker daemon after 6 seconds");
}

// --- Utility ---

function log(msg: string) {
  console.error(`[claude-peers] ${msg}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseOptionalInt(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const num = typeof value === "number" ? value : Number(value);
  return Number.isInteger(num) && num >= 0 ? num : null;
}

function parseIntervalMs(value: unknown, fallback: number): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function formatContext(peer: Pick<Peer, "context_window" | "context_used" | "context_note">): string {
  const note = peer.context_note ? ` (${peer.context_note})` : "";
  if (peer.context_window && peer.context_used !== null && peer.context_used !== undefined) {
    return `${peer.context_used}/${peer.context_window} tokens${note}`;
  }
  if (peer.context_window) {
    return `${peer.context_window} token window${note}`;
  }
  return peer.context_note || "unknown";
}

async function getGitRoot(cwd: string): Promise<string | null> {
  try {
    const proc = Bun.spawn(["git", "rev-parse", "--show-toplevel"], {
      cwd,
      stdout: "pipe",
      stderr: "ignore",
    });
    const text = await new Response(proc.stdout).text();
    const code = await proc.exited;
    if (code === 0) {
      return text.trim();
    }
  } catch {
    // not a git repo
  }
  return null;
}

function getTty(): string | null {
  try {
    const ppid = process.ppid;
    if (ppid) {
      const proc = Bun.spawnSync(["ps", "-o", "tty=", "-p", String(ppid)]);
      const tty = new TextDecoder().decode(proc.stdout).trim();
      if (tty && tty !== "?" && tty !== "??") {
        return tty;
      }
    }
  } catch {
    // ignore
  }
  return null;
}

function parentProcessChanged(): boolean {
  if (!START_PARENT_PID || START_PARENT_PID === 1) return false;
  try {
    const proc = Bun.spawnSync(["ps", "-o", "ppid=", "-p", String(process.pid)]);
    const currentParent = Number(new TextDecoder().decode(proc.stdout).trim());
    return Number.isInteger(currentParent) && currentParent !== START_PARENT_PID;
  } catch {
    return false;
  }
}

function cwdBasename(cwd: string): string {
  return cwd.split(/[\\/]/).filter(Boolean).pop() || "home";
}

function shortTty(tty: string | null): string {
  if (!tty) return "no-tty";
  const match = tty.match(/(\d+)$/);
  return match ? match[1].padStart(3, "0") : tty.replace(/[^a-zA-Z0-9_-]/g, "-");
}

function stablePeerId(machine: string, cwd: string, tty: string | null): string {
  return `${machine}-${cwdBasename(cwd)}-${shortTty(tty)}`
    .replace(/[^a-zA-Z0-9_-]/g, "-")
    .slice(0, 64);
}

function defaultNickname(machine: string, cwd: string, tty: string | null): string {
  return `${machine}:${cwdBasename(cwd)}:${shortTty(tty)}`.slice(0, 64);
}

// --- State ---

let myId: PeerId | null = null;
let myCwd = process.cwd();
let myGitRoot: string | null = null;
let myTty: string | null = null;
let myNickname = DEFAULT_NICKNAME;
let myContextWindow: number | null = DEFAULT_CONTEXT_WINDOW;
let myContextUsed: number | null = DEFAULT_CONTEXT_USED;
let myContextNote = DEFAULT_CONTEXT_NOTE;
let mySummary = "";
let myRequestedId = "";
type RegistrationRole = "unregistered" | "owner" | "standby" | "legacy";
let myRole: RegistrationRole = "unregistered";
let myLeaseId: string | null = null;
let myLeaseExpiresAt: string | null = null;
let shuttingDown = false;

// Local message buffer — messages consumed by poll loop are kept here
// so check_messages can still return them if channel push failed silently
type BufferedMessage = {
  id: number;
  from_id: string;
  from_summary: string;
  from_cwd: string;
  from_machine: string;
  text: string;
  sent_at: string;
  kind?: Message["kind"];
  reply_to_id?: number | null;
};

const localMessageBuffer: BufferedMessage[] = [];
const localMessageIds = new Set<number>();
let localMessageBufferBytes = 0;

function leaseCredentials(): { instance_id: string; lease_id: string } | Record<string, never> {
  if (myRole === "owner" && myLeaseId) {
    return { instance_id: PROCESS_INSTANCE_ID, lease_id: myLeaseId };
  }
  return {};
}

function canMutateBroker(): boolean {
  return myRole === "owner" || myRole === "legacy";
}

function bufferMessage(message: BufferedMessage): boolean {
  if (localMessageIds.has(message.id)) return false;

  const messageBytes = new TextEncoder().encode(message.text).byteLength;
  if (messageBytes > LOCAL_MESSAGE_BUFFER_MAX_BYTES) return false;

  while (
    localMessageBuffer.length > 0 &&
    (localMessageBuffer.length >= LOCAL_MESSAGE_BUFFER_MAX_COUNT ||
      localMessageBufferBytes + messageBytes > LOCAL_MESSAGE_BUFFER_MAX_BYTES)
  ) {
    const removed = localMessageBuffer.shift();
    if (!removed) break;
    localMessageIds.delete(removed.id);
    localMessageBufferBytes -= new TextEncoder().encode(removed.text).byteLength;
  }

  localMessageBuffer.push(message);
  localMessageIds.add(message.id);
  localMessageBufferBytes += messageBytes;
  return true;
}

function drainMessageBuffer(): BufferedMessage[] {
  const messages = localMessageBuffer.splice(0);
  localMessageIds.clear();
  localMessageBufferBytes = 0;
  return messages;
}

function removeBufferedMessages(messageIds: number[]): void {
  const ids = new Set(messageIds);
  for (let index = localMessageBuffer.length - 1; index >= 0; index--) {
    const message = localMessageBuffer[index];
    if (!message || !ids.has(message.id)) continue;
    localMessageBuffer.splice(index, 1);
    localMessageIds.delete(message.id);
    localMessageBufferBytes -= new TextEncoder().encode(message.text).byteLength;
  }
}

// --- MCP Server ---

const mcp = new Server(
  { name: "claude-peers", version: "0.2.0" },
  {
    capabilities: {
      ...(CHANNEL_DISABLED ? {} : { experimental: { "claude/channel": {} } }),
      tools: {},
    },
    instructions: `You are connected to the claude-peers network. Other Claude Code instances across the fleet can see you and send you messages.

When you receive a channel or peer-message, call ack_message with message_ids: [id] after reading it. For kind=request, reply using send_message with kind=reply and reply_to_id set to that message_id. Replies and notifications do not request another answer. Legacy messages without kind can be answered with the legacy send_message fields.

Read the from_id, from_summary, from_cwd, and from_machine attributes to understand who sent the message and which machine they're on. Reply by calling send_message with their from_id.

Available tools:
- peer_status: Show this instance's own peer ID, nickname, broker, cwd, and summary
- list_peers: Discover other Claude Code instances (scope: fleet/machine/directory/repo)
- send_message: Send a message to another instance by ID
- set_nickname: Set a short human-readable name for this instance
- set_context: Set this instance's context window metadata
- set_summary: Set a 1-2 sentence summary of what you're working on (visible to other peers)
- check_messages: Manually check for new messages
- ack_message: Confirm application-level receipt of one or more message IDs after reading them

When you start, proactively call set_summary to describe what you're working on. This helps other instances understand your context.`,
  }
);

// --- Tool definitions ---

const TOOLS = [
  {
    name: "peer_status",
    description: "Show this Claude peer's registration, nickname, broker, machine, cwd, and summary.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
  },
  {
    name: "list_peers",
    description:
      "List Claude/Codex peers. Returns their ID, nickname, working directory, git repo, machine name, and summary. Prefer active_only=true when tasking (hides idle no-tty zombies).",
    inputSchema: {
      type: "object" as const,
      properties: {
        scope: {
          type: "string" as const,
          enum: ["fleet", "machine", "directory", "repo"],
          description:
            'Scope of peer discovery. "fleet" = all instances across all machines. "machine" = instances on the same machine. "directory" = same working directory. "repo" = same git repository.',
        },
        include_self: {
          type: "boolean" as const,
          description: "Include this Claude peer in the result.",
        },
        active_only: {
          type: "boolean" as const,
          description:
            "If true, hide headless peers whose summary is the default idle/awaiting-directive boilerplate. Recommended for orchestration.",
        },
      },
      required: ["scope"],
    },
  },
  {
    name: "send_message",
    description:
      "Send a message to another Claude Code instance by peer ID. The message will be pushed into their session immediately via channel notification. Works across machines.",
    inputSchema: {
      type: "object" as const,
      properties: {
        ...messageToolProperties,
        to_id: {
          type: "string" as const,
          description: "The peer ID of the target Claude Code instance (from list_peers)",
        },
        message: {
          type: "string" as const,
          description: "The message to send",
        },
      },
      required: ["to_id", "message"],
    },
  },
  {
    name: "set_nickname",
    description:
      "Set a short human-readable nickname for this peer. The nickname is visible in list_peers next to the stable peer ID.",
    inputSchema: {
      type: "object" as const,
      properties: {
        nickname: {
          type: "string" as const,
          description: "Short name for this peer, for example repo-lead, review-pane, or mac-codex.",
        },
      },
      required: ["nickname"],
    },
  },
  {
    name: "set_context",
    description:
      "Set context window metadata for this peer. Use context_window for max tokens, context_used when known, and context_note for model/context notes. Unknown values should be omitted.",
    inputSchema: {
      type: "object" as const,
      properties: {
        context_window: {
          type: "number" as const,
          description: "Maximum context window in tokens, if known.",
        },
        context_used: {
          type: "number" as const,
          description: "Approximate used context tokens, if known.",
        },
        context_note: {
          type: "string" as const,
          description: "Short note such as model alias, 1M context, or unknown.",
        },
      },
    },
  },
  {
    name: "set_summary",
    description:
      "Set a brief summary (1-2 sentences) of what you are currently working on. This is visible to other Claude Code instances across the fleet when they list peers.",
    inputSchema: {
      type: "object" as const,
      properties: {
        summary: {
          type: "string" as const,
          description: "A 1-2 sentence summary of your current work",
        },
      },
      required: ["summary"],
    },
  },
  {
    name: "check_messages",
    description:
      "Manually check for new messages from other Claude Code instances. Messages are normally pushed automatically via channel notifications, but you can use this as a fallback.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
  },
  {
    name: "ack_message",
    description:
      "Acknowledge application-level receipt of message IDs after you have read their channel notification or check_messages output. Unacknowledged messages remain recoverable and may be redelivered.",
    inputSchema: {
      type: "object" as const,
      properties: {
        message_ids: {
          type: "array" as const,
          items: { type: ["number", "string"] as const },
          description: "Message IDs that this Claude session has received and read.",
        },
        message_id: {
          type: ["number", "string"] as const,
          description:
            "A single message ID. Accepted because the delivery notification names one id; equivalent to message_ids: [id].",
        },
      },
    },
  },
];

// --- Tool handlers ---

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

// Accept the singular ID named by older notifications as well as the array form.
function normalizeAckIds(args: unknown): number[] {
  const a = (args ?? {}) as { message_ids?: unknown; message_id?: unknown };
  const source = a.message_ids ?? a.message_id;
  const raw = source == null ? [] : Array.isArray(source) ? source : [source];
  const ids = raw
    .map((v) => (typeof v === "string" ? Number(v.trim()) : v))
    .filter((v): v is number => typeof v === "number" && Number.isSafeInteger(v) && v > 0);
  return [...new Set(ids)];
}

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args } = req.params;

  switch (name) {
    case "peer_status": {
      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(
              {
                id: myId,
                nickname: myNickname,
                context_window: myContextWindow,
                context_used: myContextUsed,
                context_note: myContextNote,
                machine: MACHINE_NAME,
                pid: process.pid,
                tty: myTty,
                cwd: myCwd,
                git_root: myGitRoot,
                broker_url: BROKER_URL,
                summary: mySummary,
                role: myRole,
                instance_id: PROCESS_INSTANCE_ID,
                lease_expires_at: myLeaseExpiresAt,
              },
              null,
              2
            ),
          },
        ],
      };
    }

    case "list_peers": {
      const { scope, include_self, active_only } = args as {
        scope: "fleet" | "machine" | "directory" | "repo";
        include_self?: boolean;
        active_only?: boolean;
      };
      try {
        const peers = await brokerFetch<Peer[]>("/list-peers", {
          scope,
          cwd: myCwd,
          git_root: myGitRoot,
          machine: MACHINE_NAME,
          exclude_id: include_self ? undefined : myId,
          active_only: active_only === true,
        });

        if (peers.length === 0) {
          return {
            content: [
              {
                type: "text" as const,
                text: `No other Claude Code instances found (scope: ${scope}${active_only ? ", active_only" : ""}).`,
              },
            ],
          };
        }

        const lines = peers.map((p) => {
          const parts = [
            `ID: ${p.id}`,
            `Name: ${p.nickname || "(none)"}`,
            `Context: ${formatContext(p)}`,
            `Machine: ${p.machine}`,
            `PID: ${p.pid}`,
            `CWD: ${p.cwd}`,
          ];
          if (p.git_root) parts.push(`Repo: ${p.git_root}`);
          if (p.tty) parts.push(`TTY: ${p.tty}`);
          if (p.summary) parts.push(`Summary: ${p.summary}`);
          parts.push(`Last seen: ${p.last_seen}`);
          return parts.join("\n  ");
        });

        return {
          content: [
            {
              type: "text" as const,
              text: `Found ${peers.length} peer(s) (scope: ${scope}${active_only ? ", active_only" : ""}):\n\n${lines.join("\n\n")}`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error listing peers: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "set_nickname": {
      const { nickname } = args as { nickname: string };
      if (!myId || !canMutateBroker()) {
        return {
          content: [{ type: "text" as const, text: myRole === "standby" ? "Standby peer cannot mutate broker state" : "Not registered with broker yet" }],
          isError: true,
        };
      }
      myNickname = nickname.trim().slice(0, 64);
      try {
        await brokerFetch("/set-nickname", {
          id: myId,
          nickname: myNickname,
          ...leaseCredentials(),
        });
        return {
          content: [{ type: "text" as const, text: `Nickname updated: "${myNickname}"` }],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error setting nickname: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "set_context": {
      const input = args as {
        context_window?: number | null;
        context_used?: number | null;
        context_note?: string;
      };
      if (!myId || !canMutateBroker()) {
        return {
          content: [{ type: "text" as const, text: myRole === "standby" ? "Standby peer cannot mutate broker state" : "Not registered with broker yet" }],
          isError: true,
        };
      }
      myContextWindow = parseOptionalInt(input.context_window);
      myContextUsed = parseOptionalInt(input.context_used);
      myContextNote = (input.context_note ?? "").trim().slice(0, 120);
      try {
        await brokerFetch("/set-context", {
          id: myId,
          context_window: myContextWindow,
          context_used: myContextUsed,
          context_note: myContextNote,
          ...leaseCredentials(),
        });
        return {
          content: [{ type: "text" as const, text: `Context updated: ${formatContext({ context_window: myContextWindow, context_used: myContextUsed, context_note: myContextNote })}` }],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error setting context: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "send_message": {
      const { to_id, message } = args as { to_id: string; message: string };
      if (!myId || !canMutateBroker()) {
        return {
          content: [{ type: "text" as const, text: myRole === "standby" ? "Standby peer cannot send messages" : "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        const result = await sendPeerMessage(brokerFetch, {
          from_id: myId,
          to_id,
          text: message,
          ...leaseCredentials(),
        }, args as Record<string, unknown>);
        if (!result.ok) {
          return {
            content: [{ type: "text" as const, text: `Failed to send: ${result.error}` }],
            isError: true,
          };
        }
        return {
          content: [{ type: "text" as const, text: `Message sent to peer ${to_id}. Message ID: ${result.message_id ?? "unavailable"}.` }],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error sending message: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "set_summary": {
      const { summary } = args as { summary: string };
      mySummary = summary;
      if (!myId || !canMutateBroker()) {
        return {
          content: [{ type: "text" as const, text: myRole === "standby" ? "Standby peer cannot mutate broker state" : "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        await brokerFetch("/set-summary", {
          id: myId,
          summary,
          ...leaseCredentials(),
        });
        return {
          content: [{ type: "text" as const, text: `Summary updated: "${summary}"` }],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error setting summary: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "check_messages": {
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      if (myRole === "standby") {
        return {
          content: [{ type: "text" as const, text: "No new messages (this runtime is on standby)." }],
        };
      }
      try {
        const claimed = await fetchInboundMessages();
        const senders = await getSenderDetails(claimed.messages);
        for (const message of claimed.messages) {
          const sender = senders.get(message.from_id);
          bufferMessage(toBufferedMessage(message, sender));
        }

        // Drain local buffer
        if (localMessageBuffer.length === 0) {
          return {
            content: [{ type: "text" as const, text: "No new messages." }],
          };
        }
        const messages = drainMessageBuffer();
        const lines = messages.map(
          (m) => `Message ID ${m.id}, kind=${m.kind ?? "legacy"}${m.reply_to_id ? `, reply_to_id=${m.reply_to_id}` : ""} from ${m.from_id}${m.from_machine ? ` [${m.from_machine}]` : ""} (${m.sent_at}):\n${m.text}`
        );
        return {
          content: [
            {
              type: "text" as const,
              text: `${messages.length} new message(s):\n\n${lines.join("\n\n---\n\n")}\n\nAfter reading these messages, call ack_message with: ${JSON.stringify(messages.map((message) => message.id))}`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error checking messages: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "ack_message": {
      const ids = normalizeAckIds(args);
      if (!myId || myRole !== "owner" || !myLeaseId) {
        return {
          content: [{ type: "text" as const, text: "Only the active leased owner can acknowledge messages." }],
          isError: true,
        };
      }
      if (ids.length === 0) {
        return {
          content: [{ type: "text" as const, text: "ack_message requires at least one positive message ID." }],
          isError: true,
        };
      }
      try {
        const acknowledged = await acknowledgeMessages(ids);
        removeBufferedMessages(ids);
        return {
          content: [{ type: "text" as const, text: `Acknowledged ${acknowledged} message(s).` }],
        };
      } catch (e) {
        return {
          content: [{ type: "text" as const, text: `Error acknowledging messages: ${e instanceof Error ? e.message : String(e)}` }],
          isError: true,
        };
      }
    }

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
});

// --- Polling loop for inbound messages ---

type SenderDetails = Pick<Peer, "summary" | "cwd" | "machine">;

async function fetchInboundMessages(): Promise<{
  messages: Message[];
  requiresAck: boolean;
}> {
  if (!myId || myRole === "standby" || myRole === "unregistered") {
    return { messages: [], requiresAck: false };
  }

  if (myRole === "owner") {
    const result = await brokerFetch<PollMessagesResponse>("/claim-messages", {
      id: myId,
      ...leaseCredentials(),
    });
    return { messages: result.messages, requiresAck: true };
  }

  const result = await brokerFetch<PollMessagesResponse>("/poll-messages", {
    id: myId,
  });
  return { messages: result.messages, requiresAck: false };
}

async function acknowledgeMessages(messageIds: number[]): Promise<number> {
  if (!myId || myRole !== "owner" || messageIds.length === 0) return 0;
  const result = await brokerFetch<{ acked: number }>("/ack-messages", {
    id: myId,
    message_ids: [...new Set(messageIds)],
    ...leaseCredentials(),
  });
  return result.acked;
}

async function getSenderDetails(messages: Message[]): Promise<Map<string, SenderDetails>> {
  const details = new Map<string, SenderDetails>();
  if (messages.length === 0) return details;

  try {
    const peers = await brokerFetch<Peer[]>("/list-peers", {
      scope: "fleet",
      cwd: myCwd,
      git_root: myGitRoot,
      machine: MACHINE_NAME,
    });
    for (const peer of peers) {
      details.set(peer.id, {
        summary: peer.summary,
        cwd: peer.cwd,
        machine: peer.machine,
      });
    }
  } catch {
    // Non-critical, proceed without sender info.
  }
  return details;
}

function toBufferedMessage(
  message: Message,
  sender?: SenderDetails,
): BufferedMessage {
  return {
    id: message.id,
    from_id: message.from_id,
    from_summary: sender?.summary ?? "",
    from_cwd: sender?.cwd ?? "",
    from_machine: sender?.machine ?? "",
    text: message.text,
    sent_at: message.sent_at,
    kind: message.kind,
    reply_to_id: message.reply_to_id,
  };
}

function formatInboxMessage(message: Message, sender: SenderDetails | undefined): string {
  const attr = (v: string) => v.replace(/"/g, "&quot;");
  const header =
    `<peer-message source="claude-peers" message_id="${message.id}" from_id="${attr(message.from_id)}"` +
    ` from_machine="${attr(sender?.machine ?? "")}" from_cwd="${attr(sender?.cwd ?? "")}"` +
    ` sent_at="${attr(message.sent_at)}"${message.kind ? ` kind="${message.kind}"` : ""}${message.reply_to_id ? ` reply_to_id="${message.reply_to_id}"` : ""}>`;
  const summary = sender?.summary ? `from_summary: ${sender.summary}\n` : "";
  return (
    `${header}\n${summary}${message.text}\n</peer-message>\n` +
    `Peer message from claude-peers. Call ack_message with message_ids: [${message.id}], ` +
    (message.kind === "reply" || message.kind === "notification" ? "No reply requested." :
      `reply with send_message to "${message.from_id}"${message.kind ? ` with kind=reply and reply_to_id=${message.id}` : ""}, then resume your work.`)
  );
}

function postToHostInbox(content: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const sock = createConnection({ path: HOST_INBOX_SOCKET });
    const timer = setTimeout(() => {
      sock.destroy();
      reject(new Error("inbox write timed out"));
    }, HOST_INBOX_WRITE_TIMEOUT_MS);
    sock.once("error", (e) => {
      clearTimeout(timer);
      reject(e);
    });
    sock.once("connect", () => {
      const frames: string[] = [];
      if (HOST_INBOX_TOKEN) {
        frames.push(JSON.stringify({ type: "auth", token: HOST_INBOX_TOKEN }));
      }
      frames.push(
        JSON.stringify({
          type: "user",
          priority: "next",
          message: { role: "user", content },
        }),
      );
      sock.end(frames.join("\n") + "\n", () => {
        clearTimeout(timer);
        resolve();
      });
    });
  });
}

async function pollAndPushMessages(): Promise<void> {
  if (!myId || CHANNEL_DISABLED || myRole === "standby" || myRole === "unregistered") {
    return;
  }

  try {
    const claimed = await fetchInboundMessages();
    const senders = await getSenderDetails(claimed.messages);
    for (const message of claimed.messages) {
      const sender = senders.get(message.from_id);
      const buffered = bufferMessage(toBufferedMessage(message, sender));
      let outcome = buffered ? "buffered" : "buffer-skipped";

      if (CHANNEL_RESPONSE_DELAY_MS > 0) {
        await sleep(CHANNEL_RESPONSE_DELAY_MS);
      }

      let delivered = false;
      if (HOST_INBOX_SOCKET && !HOST_INBOX_DISABLED) {
        try {
          await postToHostInbox(formatInboxMessage(message, sender));
          delivered = true;
          outcome = claimed.requiresAck
            ? "inbox-written-awaiting-application-ack"
            : "inbox-written";
        } catch (e) {
          outcome = `inbox-failed (${e instanceof Error ? e.message : String(e)}), trying channel`;
          log(`Message ${message.id}: ${outcome}`);
        }
      }

      if (!delivered) try {
        await mcp.notification({
          method: "notifications/claude/channel",
          params: {
            content: message.text,
            meta: {
              from_id: message.from_id,
              from_summary: sender?.summary ?? "",
              from_cwd: sender?.cwd ?? "",
              from_machine: sender?.machine ?? "",
              sent_at: message.sent_at,
              // Claude Code validates channel meta as Record<string, string>;
              // a numeric id fails with "meta.message_id: Invalid input" and
              // the whole notification is dropped.
              message_id: String(message.id),
              ...(message.kind ? { kind: message.kind } : {}),
              ...(message.reply_to_id ? { reply_to_id: String(message.reply_to_id) } : {}),
              acknowledgment: "Call ack_message after reading this channel message.",
            },
          },
        });
        outcome = claimed.requiresAck
          ? "notification-written-awaiting-application-ack"
          : "notification-written";
      } catch {
        outcome = "notification-failed-buffered";
      }

      const byteLength = new TextEncoder().encode(message.text).byteLength;
      log(`Message ${message.id} from ${message.from_id}: ${byteLength} bytes, ${outcome}`);
    }

  } catch (e) {
    log(`Poll error: ${e instanceof Error ? e.message : String(e)}`);
  }
}

// --- Startup ---

// Clean up on exit. Module-level so the heartbeat (orphan check) and the
// SIGINT/SIGTERM handlers can all reach it. Best-effort unregisters the active
// owner (or a legacy peer) before exiting cleanly. Standbys never unregister.
async function cleanupAndExit(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  if (myId && canMutateBroker()) {
    try {
      await brokerFetch("/unregister", {
        id: myId,
        ...leaseCredentials(),
      });
      log("Unregistered from broker");
    } catch {
      // Best effort
    }
  }
  process.exit(0);
}

function registrationPayload(summary: string) {
  return {
    requested_id: myRequestedId,
    instance_id: PROCESS_INSTANCE_ID,
    ...(myLeaseId ? { lease_id: myLeaseId } : {}),
    nickname: myNickname,
    context_window: myContextWindow,
    context_used: myContextUsed,
    context_note: myContextNote,
    tier: (process.env.CLAUDE_PEERS_TIER as "production" | "staging" | "infrastructure" | undefined) ?? "production",
    payload_version: 2,
    pid: process.pid,
    cwd: myCwd,
    git_root: myGitRoot,
    tty: myTty,
    machine: MACHINE_NAME,
    summary,
  };
}

function applyRegistration(registration: RegisterResponse): void {
  myId = registration.id;
  myLeaseExpiresAt = registration.lease_expires_at ?? null;

  if (registration.role === "standby") {
    myRole = "standby";
    myLeaseId = null;
    return;
  }

  if (registration.role === "owner") {
    if (!registration.lease_id) {
      throw new Error("Broker returned owner role without a lease");
    }
    myRole = "owner";
    myLeaseId = registration.lease_id;
    return;
  }

  // Older brokers return only { id }. Keep their poll/heartbeat/unregister
  // behavior until the broker side has been upgraded.
  myRole = "legacy";
  myLeaseId = null;
  myLeaseExpiresAt = null;
}

async function registerUntilActive(summary: string): Promise<void> {
  for (let attempt = 1; !shuttingDown; attempt++) {
    if (parentProcessChanged()) {
      log(`Parent process changed from ${START_PARENT_PID} to ${process.ppid}; exiting orphaned MCP server before registration`);
      await cleanupAndExit();
      return;
    }
    try {
      await ensureBroker();
      const registration = await brokerFetch<RegisterResponse>(
        "/register",
        registrationPayload(summary),
      );
      applyRegistration(registration);
      mySummary = summary;

      if (myRole === "standby") {
        log(`Peer ${myId} is on standby; retrying registration in ${REGISTER_RETRY_MS}ms`);
        if (parentProcessChanged()) {
          log(`Parent process changed from ${START_PARENT_PID} to ${process.ppid}; exiting orphaned standby MCP server`);
          await cleanupAndExit();
          return;
        }
        await sleep(REGISTER_RETRY_MS);
        continue;
      }

      log(`Registered as peer ${myId} (${myRole}) on machine ${MACHINE_NAME}`);
      return;
    } catch (e) {
      myRole = "unregistered";
      myLeaseId = null;
      myLeaseExpiresAt = null;
      log(`Broker bring-up attempt ${attempt} failed (retrying in ${REGISTER_RETRY_MS}ms): ${e instanceof Error ? e.message : String(e)}`);
      if (parentProcessChanged()) {
        log(`Parent process changed from ${START_PARENT_PID} to ${process.ppid}; exiting orphaned MCP server`);
        await cleanupAndExit();
        return;
      }
      await sleep(REGISTER_RETRY_MS);
    }
  }
}

async function pollLoop(): Promise<void> {
  while (!shuttingDown) {
    await sleep(POLL_INTERVAL_MS);
    if (shuttingDown) return;
    await pollAndPushMessages();
  }
}

async function heartbeatLoop(): Promise<void> {
  while (!shuttingDown) {
    await sleep(HEARTBEAT_INTERVAL_MS);
    if (shuttingDown || !myId || !canMutateBroker()) continue;

    if (parentProcessChanged()) {
      log(`Parent process changed from ${START_PARENT_PID} to ${process.ppid}; exiting orphaned MCP server`);
      await cleanupAndExit();
      return;
    }

    try {
      const heartbeat = await brokerFetch<{
        ok: boolean;
        found?: boolean;
        owner?: boolean;
      }>("/heartbeat", {
        id: myId,
        ...leaseCredentials(),
      });
      if (heartbeat.found === false || heartbeat.owner === false) {
        myRole = "unregistered";
        myLeaseId = null;
        myLeaseExpiresAt = null;
        await registerUntilActive(mySummary);
      }
    } catch (error) {
      if (error instanceof BrokerFetchError && error.status === 409) {
        myRole = "unregistered";
        myLeaseId = null;
        myLeaseExpiresAt = null;
        await registerUntilActive(mySummary);
      }
      // Other broker/network outages are non-critical. The existing lease
      // remains authoritative until the broker explicitly rejects it.
    }
  }
}

// Bring up the broker connection in the BACKGROUND, after the MCP transport is
// already connected. A slow or unreachable broker therefore never delays — or
// kills — the harness handshake. Registration retries forever instead of
// throwing, so a transient blip during the --continue re-init burst self-heals
// rather than leaving a permanently-dead stdio server (Claude Code does not
// auto-restart one). Until registration lands, tool handlers degrade gracefully
// via their "Not registered with broker yet" guards.
async function bringUpBroker(): Promise<void> {
  // Generate initial summary via gpt-5.4-nano (non-blocking, best-effort)
  let initialSummary = "";
  const summaryPromise = (async () => {
    try {
      const branch = await getGitBranch(myCwd);
      const recentFiles = await getRecentFiles(myCwd);
      const summary = await generateSummary({
        cwd: myCwd,
        git_root: myGitRoot,
        git_branch: branch,
        recent_files: recentFiles,
      });
      if (summary) {
        initialSummary = summary;
        log(`Auto-summary: ${summary}`);
      }
    } catch (e) {
      log(`Auto-summary failed (non-critical): ${e instanceof Error ? e.message : String(e)}`);
    }
  })();

  // Wait briefly for summary, but don't block bring-up
  await Promise.race([summaryPromise, new Promise((r) => setTimeout(r, 3000))]);

  // Auto-mesh-status Layer 1: never register blank. If the LLM summary race
  // produced nothing (failed / keyless / over 3s), register a deterministic
  // baseline so a fleet roll-call can always tell who is on the mesh. The LLM
  // summary still wins when it lands (late-retry below) and set_summary still
  // overrides — this only replaces the "" that used to register as a blank peer.
  const baselineSummary = `${myNickname} on ${MACHINE_NAME} (${cwdBasename(myCwd)}) — idle, awaiting directive`;
  const registerSummary = initialSummary || baselineSummary;

  // Ensure broker + register, retrying until this process owns the stable peer
  // identity (or an older broker places it in legacy mode).
  await registerUntilActive(registerSummary);
  if (shuttingDown) return;

  // If summary generation is still running, update it when done
  if (!initialSummary) {
    summaryPromise.then(async () => {
      if (initialSummary && myId && canMutateBroker()) {
        try {
          await brokerFetch("/set-summary", {
            id: myId,
            summary: initialSummary,
            ...leaseCredentials(),
          });
          mySummary = initialSummary;
          log(`Late auto-summary applied: ${initialSummary}`);
        } catch {
          // Non-critical
        }
      }
    });
  }

  // Serialized loops wait for each async body before sleeping again, so slow
  // broker responses cannot produce overlapping polls or heartbeats.
  if (!CHANNEL_DISABLED) void pollLoop();
  void heartbeatLoop();
}

async function main() {
  // Gather cheap local context (no network) — needed for nickname + register.
  myCwd = process.cwd();
  myGitRoot = await getGitRoot(myCwd);
  const tty = getTty();
  myTty = tty;

  log(`Machine: ${MACHINE_NAME}`);
  log(`Nickname: ${myNickname || "(none)"}`);
  log(`Broker: ${BROKER_URL}`);
  log(`CWD: ${myCwd}`);
  log(`Git root: ${myGitRoot ?? "(none)"}`);
  log(`TTY: ${tty ?? "(unknown)"}`);
  if (!myNickname) {
    myNickname = defaultNickname(MACHINE_NAME, myCwd, tty);
    log(`Auto-nickname: ${myNickname}`);
  }
  myRequestedId = process.env.CLAUDE_PEER_ID ?? stablePeerId(MACHINE_NAME, myCwd, tty);

  // Connect MCP over stdio FIRST. The harness handshake must succeed
  // immediately and unconditionally — even if the broker is unreachable —
  // because Claude Code never auto-restarts a stdio server that fails to come
  // up. Tool handlers guard on `myId` ("Not registered with broker yet") until
  // background registration lands, so connecting before the broker is up is
  // safe. (Previously connect was the LAST step, gated behind ensureBroker + a
  // 3s summary race + /register; any blip there threw → process.exit(1) → a
  // permanently-dead client until a manual /mcp reconnect.)
  await mcp.connect(new StdioServerTransport());
  log("MCP connected");

  // Register exit handlers now, before the (retrying) broker bring-up.
  process.on("SIGINT", cleanupAndExit);
  process.on("SIGTERM", cleanupAndExit);

  // Bring up broker + registration in the background; never blocks or kills the
  // already-connected transport.
  bringUpBroker().catch((e) => {
    log(`Broker bring-up crashed (non-fatal): ${e instanceof Error ? e.message : String(e)}`);
  });
}

main().catch((e) => {
  log(`Fatal: ${e instanceof Error ? e.message : String(e)}`);
  process.exit(1);
});
