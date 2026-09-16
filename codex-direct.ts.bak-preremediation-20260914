#!/usr/bin/env bun
/**
 * Direct Codex peer fallback.
 *
 * Use this when Codex's stdio MCP transport is closed but the claude-peers
 * broker is healthy. It loads the same broker config as codex-server.ts,
 * registers a short-lived peer, sends a message, and can poll for replies.
 */

export {};

type ClaudePeersEnv = {
  CLAUDE_PEERS_BROKER_URL?: string;
  CLAUDE_PEERS_TOKEN?: string;
  CLAUDE_PEERS_MACHINE?: string;
};

type Peer = {
  id: string;
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  machine: string;
  summary: string;
  registered_at: string;
  last_seen: string;
};

type Message = {
  id: number;
  from_id: string;
  to_id: string;
  text: string;
  sent_at: string;
  delivered: boolean;
};

async function loadClaudePeersEnv(): Promise<ClaudePeersEnv> {
  const paths = [`${process.env.HOME}/.mcp.json`, `${process.env.HOME}/.claude.json`];
  for (const path of paths) {
    try {
      const file = Bun.file(path);
      if (!(await file.exists())) continue;
      const parsed = JSON.parse(await file.text());
      const env = (parsed.mcpServers ?? parsed.mcp_servers ?? {})["claude-peers"]?.env;
      if (env && typeof env === "object") return env as ClaudePeersEnv;
    } catch {
      // Ignore malformed or missing config files.
    }
  }
  return {};
}

const inherited = await loadClaudePeersEnv();
const brokerUrl =
  process.env.CLAUDE_PEERS_BROKER_URL ?? inherited.CLAUDE_PEERS_BROKER_URL ?? "http://127.0.0.1:7899";
const token = process.env.CLAUDE_PEERS_TOKEN ?? inherited.CLAUDE_PEERS_TOKEN ?? "";
const machine = process.env.CLAUDE_PEERS_MACHINE ?? inherited.CLAUDE_PEERS_MACHINE ?? require("os").hostname();

function headers(): Record<string, string> {
  const out: Record<string, string> = { "Content-Type": "application/json" };
  if (token) out.Authorization = `Bearer ${token}`;
  return out;
}

async function brokerFetch<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${brokerUrl}${path}`, {
    method: body ? "POST" : "GET",
    headers: headers(),
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(5000),
  });
  if (!res.ok) throw new Error(`${path}: ${res.status} ${await res.text()}`);
  return res.json() as Promise<T>;
}

async function gitRoot(cwd: string): Promise<string | null> {
  try {
    const proc = Bun.spawn(["git", "rev-parse", "--show-toplevel"], {
      cwd,
      stdout: "pipe",
      stderr: "ignore",
    });
    const text = await new Response(proc.stdout).text();
    return (await proc.exited) === 0 ? text.trim() : null;
  } catch {
    return null;
  }
}

async function register(summary: string): Promise<string> {
  const cwd = process.cwd();
  const requestedId = `codex-direct-${process.pid}-${Date.now().toString(36)}`;
  const result = await brokerFetch<{ id: string }>("/register", {
    requested_id: requestedId,
    nickname: "codex-direct",
    tier: "infrastructure",
    payload_version: 1,
    pid: process.pid,
    cwd,
    git_root: await gitRoot(cwd),
    tty: null,
    machine,
    summary,
  });
  return result.id;
}

async function unregister(id: string): Promise<void> {
  await brokerFetch("/unregister", { id }).catch(() => undefined);
}

async function heartbeat(id: string): Promise<void> {
  await brokerFetch("/heartbeat", { id }).catch(() => undefined);
}

function usage(): never {
  console.error(`Usage:
  bun codex-direct.ts status
  bun codex-direct.ts peers
  bun codex-direct.ts poll [peer-id]
  bun codex-direct.ts send <to-peer-id> <message> [--from peer-id]
  bun codex-direct.ts ask <peer-id> <message> [--seconds 120]

Notes:
  - Loads broker URL/token from ~/.mcp.json or env.
  - Does not print broker tokens.
  - poll reads and marks delivered for the selected peer ID.
  - send defaults to a temporary direct peer unless --from is provided.
  - ask registers a temporary Codex peer and polls for replies.`);
  process.exit(2);
}

const [cmd, ...args] = process.argv.slice(2);

if (cmd === "status") {
  const health = await brokerFetch<{ status: string; peers: number }>("/health");
  console.log(`Broker: ${health.status}; peers=${health.peers}; url=${brokerUrl}`);
} else if (cmd === "peers") {
  const peers = await brokerFetch<Peer[]>("/list-peers", {
    scope: "fleet",
    cwd: process.cwd(),
    git_root: await gitRoot(process.cwd()),
    machine,
  });
  for (const p of peers) {
    const summary = p.summary ? ` — ${p.summary}` : "";
    console.log(`${p.id} [${p.machine}] ${p.cwd}${summary}`);
  }
} else if (cmd === "poll") {
  const id = args[0] ?? process.env.CODEX_PEER_ID;
  if (!id) usage();
  const result = await brokerFetch<{ messages: Message[] }>("/poll-messages", { id });
  if (result.messages.length === 0) {
    console.log(`No new messages for ${id}.`);
  } else {
    for (const msg of result.messages) {
      console.log(`\nfrom ${msg.from_id} at ${msg.sent_at}:\n${msg.text}`);
    }
  }
} else if (cmd === "send") {
  const fromFlag = args.indexOf("--from");
  const fromId = fromFlag >= 0 ? args[fromFlag + 1] : null;
  const messageEnd = fromFlag >= 0 ? fromFlag : args.length;
  const toId = args[0];
  const message = args.slice(1, messageEnd).join(" ").trim();
  if (!toId || !message || (fromFlag >= 0 && !fromId)) usage();

  const id = fromId ?? (await register("codex-direct fallback: temporary Codex peer sending a message."));
  try {
    const result = await brokerFetch<{ ok: boolean; error?: string }>("/send-message", {
      from_id: id,
      to_id: toId,
      text: message,
    });
    if (!result.ok) throw new Error(result.error ?? "send failed");
    console.log(`sent from ${id} to ${toId}`);
  } finally {
    if (!fromId) await unregister(id);
  }
} else if (cmd === "ask") {
  const toId = args[0];
  const secondsFlag = args.indexOf("--seconds");
  const seconds =
    secondsFlag >= 0 && args[secondsFlag + 1] ? Number.parseInt(args[secondsFlag + 1], 10) : 120;
  const messageEnd = secondsFlag >= 0 ? secondsFlag : args.length;
  const message = args.slice(1, messageEnd).join(" ").trim();
  if (!toId || !message || !Number.isFinite(seconds)) usage();

  const id = await register("codex-direct fallback: temporary Codex peer polling for ack/result.");
  const beat = setInterval(() => heartbeat(id), 10_000);
  try {
    await brokerFetch<{ ok: boolean; error?: string }>("/send-message", {
      from_id: id,
      to_id: toId,
      text: message,
    }).then((result) => {
      if (!result.ok) throw new Error(result.error ?? "send failed");
    });
    console.log(`sent from ${id} to ${toId}; polling ${seconds}s`);

    const deadline = Date.now() + seconds * 1000;
    while (Date.now() < deadline) {
      await heartbeat(id);
      const result = await brokerFetch<{ messages: Message[] }>("/poll-messages", { id });
      if (result.messages.length > 0) {
        for (const msg of result.messages) {
          console.log(`\nfrom ${msg.from_id} at ${msg.sent_at}:\n${msg.text}`);
        }
        process.exitCode = 0;
        break;
      }
      await Bun.sleep(2000);
    }
  } finally {
    clearInterval(beat);
    await unregister(id);
  }
} else {
  usage();
}
