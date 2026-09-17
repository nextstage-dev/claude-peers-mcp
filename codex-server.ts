#!/usr/bin/env bun
/**
 * codex-peers MCP server
 *
 * Lets Codex participate in the existing claude-peers broker.
 * This intentionally does not use Claude's channel notifications; Codex checks
 * messages through tools and replies with send_message.
 *
 * Usage:
 *   codex mcp add codex-peers -- /path/to/bun /path/to/claude-peers-mcp/codex-server.ts
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import type {
  Peer,
  PeerId,
  PollMessagesResponse,
  RegisterResponse,
} from "./shared/types.ts";
import {
  normalizeSendTarget,
  SEND_TARGET_HELP,
  SEND_TARGET_SCHEMA_PROPERTIES,
} from "./shared/send-target.ts";
import { fileURLToPath } from "node:url";

type ClaudePeersEnv = {
  CLAUDE_PEERS_BROKER_URL?: string;
  CLAUDE_PEERS_TOKEN?: string;
  CLAUDE_PEERS_MACHINE?: string;
};

async function loadClaudePeersEnv(): Promise<ClaudePeersEnv> {
  const paths = [
    `${process.env.HOME}/.mcp.json`,
    `${process.env.HOME}/.claude.json`,
  ];

  for (const path of paths) {
    try {
      const file = Bun.file(path);
      if (!file.exists()) continue;
      const parsed = JSON.parse(await file.text());
      const mcpServers = parsed.mcpServers ?? parsed.mcp_servers ?? {};
      const claudePeers = mcpServers["claude-peers"];
      const env = claudePeers?.env;
      if (env && typeof env === "object") {
        return env as ClaudePeersEnv;
      }
    } catch {
      // Ignore malformed or unavailable config files.
    }
  }

  return {};
}

const INHERITED_ENV = await loadClaudePeersEnv();
const BROKER_PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const BROKER_URL =
  process.env.CLAUDE_PEERS_BROKER_URL ??
  INHERITED_ENV.CLAUDE_PEERS_BROKER_URL ??
  `http://127.0.0.1:${BROKER_PORT}`;
const AUTH_TOKEN = process.env.CLAUDE_PEERS_TOKEN ?? INHERITED_ENV.CLAUDE_PEERS_TOKEN ?? "";
const MACHINE_NAME = process.env.CLAUDE_PEERS_MACHINE ?? INHERITED_ENV.CLAUDE_PEERS_MACHINE ?? require("os").hostname();
const CODEX_PEER_NAME = process.env.CODEX_PEER_NAME ?? "codex";
let myNickname = process.env.CODEX_PEER_NICKNAME ?? CODEX_PEER_NAME;
let myContextWindow = parseOptionalInt(process.env.CODEX_PEER_CONTEXT_WINDOW ?? process.env.CLAUDE_PEERS_CONTEXT_WINDOW);
let myContextUsed = parseOptionalInt(process.env.CODEX_PEER_CONTEXT_USED ?? process.env.CLAUDE_PEERS_CONTEXT_USED);
let myContextNote = process.env.CODEX_PEER_CONTEXT_NOTE ?? process.env.CLAUDE_PEERS_CONTEXT_NOTE ?? "";
const HEARTBEAT_INTERVAL_MS = 15_000;
const BROKER_SCRIPT = fileURLToPath(new URL("./broker.ts", import.meta.url));

const isLocalBroker = BROKER_URL.includes("127.0.0.1") || BROKER_URL.includes("localhost");

let myId: PeerId | null = null;
let myCwd = process.cwd();
let myGitRoot: string | null = null;
let myTty: string | null = null;
let mySummary =
  process.env.CODEX_PEER_SUMMARY ??
  "Codex peer: pragmatic coding/review agent reachable through the claude-peers broker.";

function stableIdPart(value: string): string {
  let hash = 2166136261;
  for (const ch of value) {
    hash ^= ch.charCodeAt(0);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}

function parseOptionalInt(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const num = typeof value === "number" ? value : Number(value);
  return Number.isInteger(num) && num >= 0 ? num : null;
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

function log(msg: string) {
  console.error(`[codex-peers] ${msg}`);
}

function authHeaders(): Record<string, string> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (AUTH_TOKEN) {
    headers.Authorization = `Bearer ${AUTH_TOKEN}`;
  }
  return headers;
}

async function brokerFetch<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BROKER_URL}${path}`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(5000),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Broker error (${path}): ${res.status} ${err}`);
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
    throw new Error(`Remote broker at ${BROKER_URL} is not reachable.`);
  }

  log("Starting local broker daemon...");
  const proc = Bun.spawn([process.execPath, BROKER_SCRIPT], {
    stdio: ["ignore", "ignore", "inherit"],
  });
  proc.unref();

  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 200));
    if (await isBrokerAlive()) {
      log("Broker started");
      return;
    }
  }
  throw new Error("Failed to start broker daemon after 6 seconds");
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
    return code === 0 ? text.trim() : null;
  } catch {
    return null;
  }
}

function getTty(): string | null {
  try {
    const proc = Bun.spawnSync(["ps", "-o", "tty=", "-p", String(process.ppid)]);
    const tty = new TextDecoder().decode(proc.stdout).trim();
    return tty && tty !== "?" && tty !== "??" ? tty : null;
  } catch {
    return null;
  }
}

async function peerContext() {
  await ensureRegistered();
  return {
    id: myId,
    name: CODEX_PEER_NAME,
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
  };
}

async function registerPeer(): Promise<void> {
  const stableId = process.env.CODEX_PEER_ID ?? `codex-${MACHINE_NAME}-${CODEX_PEER_NAME}-${stableIdPart(myCwd)}`;
  const reg = await brokerFetch<RegisterResponse>("/register", {
    requested_id: stableId,
    nickname: myNickname,
    context_window: myContextWindow,
    context_used: myContextUsed,
    context_note: myContextNote,
    tier: (process.env.CLAUDE_PEERS_TIER as "production" | "staging" | "infrastructure" | undefined) ?? "production",
    payload_version: 1,
    pid: process.pid,
    cwd: myCwd,
    git_root: myGitRoot,
    tty: myTty,
    machine: MACHINE_NAME,
    summary: mySummary,
  });
  myId = reg.id;
  log(`Registered as peer ${myId}`);
}

async function brokerHasPeer(id: PeerId): Promise<boolean> {
  const peers = await brokerFetch<Peer[]>("/list-peers", {
    scope: "fleet",
    cwd: myCwd,
    git_root: myGitRoot,
    machine: MACHINE_NAME,
  });
  return peers.some((p) => p.id === id);
}

async function ensureRegistered(): Promise<void> {
  if (!myId) {
    await registerPeer();
    return;
  }

  try {
    if (await brokerHasPeer(myId)) return;
  } catch {
    throw new Error("Unable to verify Codex peer registration with broker.");
  }

  log(`Peer ${myId} is no longer registered with broker; re-registering`);
  await registerPeer();
}

const mcp = new Server(
  { name: "codex-peers", version: "0.1.0" },
  {
    capabilities: { tools: {} },
    instructions: `You are connected to the claude-peers network as a Codex peer.

Use peer_status to see your own stable ID, list_peers to discover Claude peers, check_messages to read incoming peer messages, send_message to reply, set_nickname to label yourself, set_context to publish context-window metadata, and set_summary to keep your fleet status useful.

Codex does not receive Claude channel push notifications. You must explicitly call check_messages when you want to poll the peer inbox.`,
  }
);

const TOOLS = [
  {
    name: "peer_status",
    description: "Show this Codex peer's registration, broker, machine, cwd, and summary.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
  },
  {
    name: "list_peers",
    description: "List Claude/Codex peers registered with the claude-peers broker.",
    inputSchema: {
      type: "object" as const,
      properties: {
        scope: {
          type: "string" as const,
          enum: ["fleet", "machine", "directory", "repo"],
          description:
            'Peer discovery scope. "fleet" = all machines, "machine" = this machine, "directory" = this cwd, "repo" = this git root.',
        },
        include_self: {
          type: "boolean" as const,
          description: "Include this Codex peer in the result.",
        },
      },
      required: ["scope"],
    },
  },
  {
    name: "send_message",
    description:
      'Send a message to any peer. The target peer ID goes in to_id (peer_id is accepted as an alias), e.g. send_message({to_id: "box-repo-042", message: "..."}).',
    inputSchema: {
      type: "object" as const,
      properties: {
        ...SEND_TARGET_SCHEMA_PROPERTIES,
        message: {
          type: "string" as const,
          description: "Message text to send.",
        },
      },
      required: ["message"],
    },
  },
  {
    name: "check_messages",
    description: "Poll this Codex peer's inbox for new messages.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
  },
  {
    name: "set_nickname",
    description: "Set this Codex peer's visible nickname.",
    inputSchema: {
      type: "object" as const,
      properties: {
        nickname: {
          type: "string" as const,
          description: "Short human-readable name visible beside this peer ID.",
        },
      },
      required: ["nickname"],
    },
  },
  {
    name: "set_context",
    description: "Set this Codex peer's visible context-window metadata.",
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
    description: "Set this Codex peer's visible fleet summary.",
    inputSchema: {
      type: "object" as const,
      properties: {
        summary: {
          type: "string" as const,
          description: "A concise 1-2 sentence summary visible to other peers.",
        },
      },
      required: ["summary"],
    },
  },
];

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args } = req.params;

  switch (name) {
    case "peer_status": {
      return {
        content: [{ type: "text" as const, text: JSON.stringify(await peerContext(), null, 2) }],
      };
    }

    case "list_peers": {
      await ensureRegistered();
      const { scope, include_self } = args as {
        scope: "fleet" | "machine" | "directory" | "repo";
        include_self?: boolean;
      };
      const peers = await brokerFetch<Peer[]>("/list-peers", {
        scope,
        cwd: myCwd,
        git_root: myGitRoot,
        machine: MACHINE_NAME,
        exclude_id: include_self ? undefined : myId,
      });

      if (peers.length === 0) {
        return {
          content: [{ type: "text" as const, text: `No peers found (scope: ${scope}).` }],
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
            text: `Found ${peers.length} peer(s) (scope: ${scope}):\n\n${lines.join("\n\n")}`,
          },
        ],
      };
    }

    case "send_message": {
      await ensureRegistered();
      const to_id = normalizeSendTarget(args);
      const message = (args as { message?: unknown })?.message;
      if (!to_id) {
        return {
          content: [{ type: "text" as const, text: `send_message needs the target peer's ID. ${SEND_TARGET_HELP}` }],
          isError: true,
        };
      }
      if (typeof message !== "string" || message.trim().length === 0) {
        return {
          content: [{ type: "text" as const, text: "send_message needs a non-empty message string in `message`." }],
          isError: true,
        };
      }
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet." }],
          isError: true,
        };
      }

      const result = await brokerFetch<{ ok: boolean; error?: string }>("/send-message", {
        from_id: myId,
        to_id,
        text: message,
      });

      if (!result.ok) {
        return {
          content: [{ type: "text" as const, text: `Failed to send: ${result.error}. ${SEND_TARGET_HELP}` }],
          isError: true,
        };
      }

      return {
        content: [{ type: "text" as const, text: `Message sent to peer ${to_id}.` }],
      };
    }

    case "set_nickname": {
      const { nickname } = args as { nickname: string };
      myNickname = nickname.trim().slice(0, 64);
      await ensureRegistered();
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet." }],
          isError: true,
        };
      }

      await brokerFetch("/set-nickname", { id: myId, nickname: myNickname });
      return {
        content: [{ type: "text" as const, text: `Nickname updated: "${myNickname}"` }],
      };
    }

    case "set_context": {
      const input = args as {
        context_window?: number | null;
        context_used?: number | null;
        context_note?: string;
      };
      myContextWindow = parseOptionalInt(input.context_window);
      myContextUsed = parseOptionalInt(input.context_used);
      myContextNote = (input.context_note ?? "").trim().slice(0, 120);
      await ensureRegistered();
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet." }],
          isError: true,
        };
      }

      await brokerFetch("/set-context", {
        id: myId,
        context_window: myContextWindow,
        context_used: myContextUsed,
        context_note: myContextNote,
      });
      return {
        content: [{ type: "text" as const, text: `Context updated: ${formatContext({ context_window: myContextWindow, context_used: myContextUsed, context_note: myContextNote })}` }],
      };
    }

    case "check_messages": {
      await ensureRegistered();
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet." }],
          isError: true,
        };
      }

      const result = await brokerFetch<PollMessagesResponse>("/poll-messages", { id: myId });
      if (result.messages.length === 0) {
        return {
          content: [{ type: "text" as const, text: "No new messages." }],
        };
      }

      const allPeers = await brokerFetch<Peer[]>("/list-peers", {
        scope: "fleet",
        cwd: myCwd,
        git_root: myGitRoot,
        machine: MACHINE_NAME,
      });
      const byId = new Map(allPeers.map((p) => [p.id, p]));
      const lines = result.messages.map((m) => {
        const sender = byId.get(m.from_id);
        const label = sender
          ? `${m.from_id}${sender.nickname ? ` (${sender.nickname})` : ""} [${sender.machine}] ${sender.summary || sender.cwd}`
          : m.from_id;
        return `From ${label} (${m.sent_at}):\n${m.text}`;
      });

      return {
        content: [
          {
            type: "text" as const,
            text: `${result.messages.length} new message(s):\n\n${lines.join("\n\n---\n\n")}`,
          },
        ],
      };
    }

    case "set_summary": {
      const { summary } = args as { summary: string };
      mySummary = summary;
      await ensureRegistered();
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet." }],
          isError: true,
        };
      }

      await brokerFetch("/set-summary", { id: myId, summary });
      return {
        content: [{ type: "text" as const, text: `Summary updated: "${summary}"` }],
      };
    }

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
});

async function main() {
  await ensureBroker();

  myCwd = process.cwd();
  myGitRoot = await getGitRoot(myCwd);
  myTty = getTty();

  await registerPeer();
  log(`Machine: ${MACHINE_NAME}`);
  log(`Broker: ${BROKER_URL}`);
  log(`CWD: ${myCwd}`);
  log(`Git root: ${myGitRoot ?? "(none)"}`);

  await mcp.connect(new StdioServerTransport());
  log("MCP connected");

  const heartbeatTimer = setInterval(async () => {
    try {
      await ensureRegistered();
      if (!myId) return;
      await brokerFetch("/heartbeat", { id: myId });
    } catch {
      // Best effort; do not crash Codex's MCP server on transient broker loss.
    }
  }, HEARTBEAT_INTERVAL_MS);

  const cleanup = async () => {
    clearInterval(heartbeatTimer);
    // Keep the stable Codex peer row in the broker so Claude peers can reply
    // between short-lived MCP process restarts. The next Codex process reuses
    // the same ID and polls any queued messages.
    process.exit(0);
  };

  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);
}

main().catch((e) => {
  log(`Fatal: ${e instanceof Error ? e.message : String(e)}`);
  process.exit(1);
});
