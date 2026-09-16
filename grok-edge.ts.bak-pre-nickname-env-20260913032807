#!/usr/bin/env bun
/**
 * Public MCP edge for Grok Chat.
 *
 * Loopback-only. Cloudflare Tunnel in front.
 * Talks to the existing broker over Tailscale.
 * Does not Funnel :7899. Does not expose register/unregister/set_*.
 *
 * Env:
 *   CLAUDE_PEERS_BROKER_URL  default http://100.108.57.10:7899
 *   CLAUDE_PEERS_TOKEN       fleet bearer (never leave the tailnet)
 *   GROK_MCP_TOKEN           public connector bearer (required)
 *   GROK_PEER_ID             default grok-chat
 *   GROK_EDGE_PORT           default 8787
 *   GROK_EDGE_HOST           default 127.0.0.1
 *   GROK_PEER_ALLOWLIST      optional comma-separated to_id allowlist
 */

const BROKER = (process.env.CLAUDE_PEERS_BROKER_URL ?? "http://100.108.57.10:7899").replace(/\/$/, "");
const FLEET_TOKEN = process.env.CLAUDE_PEERS_TOKEN ?? "";
const PUBLIC_TOKEN = process.env.GROK_MCP_TOKEN ?? "";
const PEER_ID = process.env.GROK_PEER_ID ?? "grok-chat";
const PORT = parseInt(process.env.GROK_EDGE_PORT ?? "8787", 10);
const HOST = process.env.GROK_EDGE_HOST ?? "127.0.0.1";
const ALLOWLIST = (process.env.GROK_PEER_ALLOWLIST ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

if (!PUBLIC_TOKEN) {
  console.error("[grok-edge] FATAL: GROK_MCP_TOKEN is required");
  process.exit(1);
}
if (!FLEET_TOKEN) {
  console.error("[grok-edge] FATAL: CLAUDE_PEERS_TOKEN is required");
  process.exit(1);
}
if (!/^[a-zA-Z0-9_-]{3,64}$/.test(PEER_ID)) {
  console.error("[grok-edge] FATAL: GROK_PEER_ID must match [a-zA-Z0-9_-]{3,64}");
  process.exit(1);
}

async function broker(path: string, body: unknown): Promise<any> {
  const res = await fetch(`${BROKER}${path}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${FLEET_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json: any;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    throw new Error(`broker ${path} returned non-json (${res.status}): ${text.slice(0, 200)}`);
  }
  if (!res.ok) {
    throw new Error(`broker ${path} ${res.status}: ${json.error ?? text.slice(0, 200)}`);
  }
  return json;
}

async function registerSeat(): Promise<void> {
  await broker("/register", {
    requested_id: PEER_ID,
    nickname: "Grok Chat",
    pid: process.pid,
    cwd: process.cwd(),
    git_root: null,
    tty: null,
    machine: process.env.GROK_PEER_MACHINE ?? "grok-edge",
    summary: "Grok Chat public MCP edge. Text dispatch only.",
    tier: "infrastructure",
  });
}

async function heartbeat(): Promise<void> {
  await broker("/heartbeat", { id: PEER_ID });
}

const TOOLS = [
  {
    name: "list_peers",
    description:
      "List live fleet peers on the claude-peers broker. Use peer id (not nickname) as to_id.",
    inputSchema: {
      type: "object",
      properties: {
        scope: {
          type: "string",
          enum: ["fleet", "machine"],
          description: "Default fleet",
        },
        machine: {
          type: "string",
          description: "Optional machine filter when scope=machine (pc, mac, clarvis, ...)",
        },
      },
    },
  },
  {
    name: "send_message",
    description:
      "Send a text message to a live peer id. from_id is pinned to grok-chat. Nicknames do not resolve. Offline/stale peers are not queued.",
    inputSchema: {
      type: "object",
      properties: {
        to_id: {
          type: "string",
          description: "Exact peer id, e.g. pc-nagatha-session-no-tty",
        },
        message: { type: "string", description: "Plain text body" },
      },
      required: ["to_id", "message"],
    },
  },
  {
    name: "check_messages",
    description:
      "Poll undelivered replies queued to grok-chat. There is no thread object. Marks messages delivered.",
    inputSchema: { type: "object", properties: {} },
  },
];

function textResult(text: string, isError = false) {
  return {
    content: [{ type: "text", text }],
    isError,
  };
}

async function callTool(name: string, args: Record<string, unknown> | undefined) {
  const a = args ?? {};
  if (name === "list_peers") {
    const scope = a.scope === "machine" ? "machine" : "fleet";
    const peers = await broker("/list-peers", {
      scope,
      cwd: "/",
      git_root: null,
      machine: typeof a.machine === "string" ? a.machine : undefined,
      exclude_id: PEER_ID,
    });
    const rows = (Array.isArray(peers) ? peers : []).map((p: any) => ({
      id: p.id,
      nickname: p.nickname || "",
      machine: p.machine,
      summary: p.summary || "",
      last_seen: p.last_seen,
    }));
    return textResult(JSON.stringify({ count: rows.length, peers: rows }, null, 2));
  }

  if (name === "send_message") {
    const to_id = typeof a.to_id === "string" ? a.to_id.trim() : "";
    const message = typeof a.message === "string" ? a.message : "";
    if (!/^[a-zA-Z0-9_-]{3,64}$/.test(to_id)) {
      return textResult("to_id must match [a-zA-Z0-9_-]{3,64}", true);
    }
    if (!message.trim()) return textResult("message is empty", true);
    if (ALLOWLIST.length && !ALLOWLIST.includes(to_id)) {
      return textResult(`to_id ${to_id} is not on GROK_PEER_ALLOWLIST`, true);
    }
    const sent = await broker("/send-message", {
      from_id: PEER_ID,
      to_id,
      text: message,
    });
    if (!sent?.ok) {
      return textResult(sent?.error ?? "send failed", true);
    }
    return textResult(
      JSON.stringify({
        ok: true,
        from_id: PEER_ID,
        to_id,
        note: "Queued if target is live. No durable mailbox. Poll check_messages for replies to grok-chat.",
      }),
    );
  }

  if (name === "check_messages") {
    const polled = await broker("/poll-messages", { id: PEER_ID });
    const messages = Array.isArray(polled?.messages) ? polled.messages : [];
    return textResult(JSON.stringify({ count: messages.length, messages }, null, 2));
  }

  return textResult(`unknown tool: ${name}`, true);
}

function unauthorized() {
  return new Response(JSON.stringify({ error: "unauthorized" }), {
    status: 401,
    headers: { "Content-Type": "application/json" },
  });
}

function checkPublicAuth(req: Request): boolean {
  const auth = req.headers.get("Authorization") ?? "";
  const headerTok = req.headers.get("X-Api-Key") ?? "";
  return auth === `Bearer ${PUBLIC_TOKEN}` || headerTok === PUBLIC_TOKEN;
}

function rpcResult(id: unknown, result: unknown) {
  return { jsonrpc: "2.0", id: id ?? null, result };
}
function rpcError(id: unknown, code: number, message: string) {
  return { jsonrpc: "2.0", id: id ?? null, error: { code, message } };
}

async function handleRpc(msg: any) {
  if (!msg || msg.jsonrpc !== "2.0" || typeof msg.method !== "string") {
    return rpcError(msg?.id, -32600, "invalid request");
  }
  const { id, method, params } = msg;

  if (method === "initialize") {
    return rpcResult(id, {
      protocolVersion: params?.protocolVersion ?? "2025-03-26",
      capabilities: { tools: {} },
      serverInfo: { name: "grok-peers-edge", version: "0.1.0" },
    });
  }
  if (method === "notifications/initialized" || method === "notifications/cancelled") {
    return null;
  }
  if (method === "ping") return rpcResult(id, {});
  if (method === "tools/list") return rpcResult(id, { tools: TOOLS });
  if (method === "tools/call") {
    const name = params?.name;
    if (typeof name !== "string") return rpcError(id, -32602, "missing tool name");
    try {
      const result = await callTool(name, params?.arguments);
      return rpcResult(id, result);
    } catch (e) {
      const err = e instanceof Error ? e.message : String(e);
      return rpcResult(id, textResult(err, true));
    }
  }
  return rpcError(id, -32601, `method not found: ${method}`);
}

await registerSeat();
await heartbeat();
setInterval(() => {
  heartbeat().catch((e) => console.error("[grok-edge] heartbeat", e));
}, 15_000);

Bun.serve({
  port: PORT,
  hostname: HOST,
  async fetch(req) {
    const url = new URL(req.url);
    if (req.method === "GET" && (url.pathname === "/health" || url.pathname === "/")) {
      return Response.json({
        status: "ok",
        peer_id: PEER_ID,
        broker: BROKER,
        tools: TOOLS.map((t) => t.name),
      });
    }

    if (url.pathname !== "/mcp" && url.pathname !== "/") {
      return new Response("not found", { status: 404 });
    }
    if (req.method !== "POST") {
      return new Response("POST JSON-RPC to /mcp", { status: 405 });
    }
    if (!checkPublicAuth(req)) return unauthorized();

    let body: any;
    try {
      body = await req.json();
    } catch {
      return Response.json(rpcError(null, -32700, "parse error"), { status: 400 });
    }

    if (Array.isArray(body)) {
      const out = [];
      for (const item of body) {
        const r = await handleRpc(item);
        if (r) out.push(r);
      }
      return Response.json(out);
    }

    const r = await handleRpc(body);
    if (!r) return new Response(null, { status: 202 });
    return Response.json(r);
  },
});

console.error(`[grok-edge] ${HOST}:${PORT} → ${BROKER} as ${PEER_ID}`);
