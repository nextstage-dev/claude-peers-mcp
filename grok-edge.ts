#!/usr/bin/env bun
/**
 * Public MCP edge for Grok Bot seats.
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
 *   GROK_PEER_NICKNAME       display name
 *   GROK_PEER_MACHINE        default theoldone
 *   GROK_PEER_SUMMARY        optional register summary override
 *   GROK_EDGE_PORT           default 8787
 *   GROK_EDGE_HOST           default 127.0.0.1
 *   GROK_PEER_ALLOWLIST      optional comma-separated to_id allowlist
 */

import { messageToolProperties, sendPeerMessage } from "./shared/message-contract.ts";

const BROKER = (process.env.CLAUDE_PEERS_BROKER_URL ?? "http://100.108.57.10:7899").replace(/\/$/, "");
const FLEET_TOKEN = process.env.CLAUDE_PEERS_TOKEN ?? "";
const PUBLIC_TOKEN = process.env.GROK_MCP_TOKEN ?? "";
const PEER_ID = process.env.GROK_PEER_ID ?? "grok-chat";
const PEER_NICK = process.env.GROK_PEER_NICKNAME ?? PEER_ID;
const PEER_MACHINE = process.env.GROK_PEER_MACHINE ?? "theoldone";
const PORT = parseInt(process.env.GROK_EDGE_PORT ?? "8787", 10);
const HOST = process.env.GROK_EDGE_HOST ?? "127.0.0.1";
const ALLOWLIST = (process.env.GROK_PEER_ALLOWLIST ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const SUMMARY_MAX = 96;

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

function seatSummary(): string {
  if (process.env.GROK_PEER_SUMMARY?.trim()) return process.env.GROK_PEER_SUMMARY.trim();
  return `Grok Bot seat ${PEER_NICK} (${PEER_ID}). MCP text edge on ${PEER_MACHINE}.`;
}

async function registerSeat(): Promise<void> {
  await broker("/register", {
    requested_id: PEER_ID,
    nickname: PEER_NICK,
    pid: process.pid,
    cwd: process.cwd(),
    git_root: null,
    tty: null,
    machine: PEER_MACHINE,
    summary: seatSummary(),
    tier: "infrastructure",
  });
}

async function heartbeat(): Promise<void> {
  await broker("/heartbeat", { id: PEER_ID });
}

function clipSummary(s: string, max = SUMMARY_MAX): string {
  const one = (s || "").replace(/\s+/g, " ").trim();
  if (one.length <= max) return one;
  return one.slice(0, max - 1) + "…";
}

function ageSeconds(iso: string | undefined): number | null {
  if (!iso) return null;
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return null;
  return Math.max(0, Math.round((Date.now() - t) / 1000));
}

function classifyPeer(p: { id: string; nickname?: string; machine?: string }): string {
  const id = p.id || "";
  const nick = (p.nickname || "").toLowerCase();
  // Only dedicated Grok Bot MCP edges — not Claude seats nicknamed Grok/GrokLead.
  if (id === "grok-chat" || id.startsWith("grok_bot_")) return "grok";
  if (id.startsWith("codex-") || nick === "codex-local" || nick.includes("codex")) return "codex";
  if (id.startsWith("cortext-") || (p.machine || "") === "cortext") return "cortext";
  return "named";
}

type PeerRow = {
  id: string;
  nickname: string;
  machine: string;
  kind: string;
  summary: string;
  age_s: number | null;
  last_seen?: string;
};

function shapePeer(p: any, verbose: boolean): PeerRow {
  const row: PeerRow = {
    id: p.id,
    nickname: p.nickname || "",
    machine: p.machine || "",
    kind: classifyPeer(p),
    summary: verbose ? (p.summary || "") : clipSummary(p.summary || ""),
    age_s: ageSeconds(p.last_seen),
  };
  if (verbose) row.last_seen = p.last_seen;
  return row;
}

function kindRank(kind: string): number {
  switch (kind) {
    case "grok":
      return 0;
    case "named":
      return 1;
    case "cortext":
      return 2;
    case "codex":
      return 3;
    default:
      return 9;
  }
}

const TOOLS = [
  {
    name: "list_peers",
    description:
      "List live fleet peers (compact by default). Returns self + peers. Use peer id (not nickname) as to_id. Filters: kind=grok|named|codex|cortext|all, q=search, active_only, collapse_codex, verbose.",
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
          description: "Optional machine filter (pc, mac, clarvis, theoldone, ...)",
        },
        kind: {
          type: "string",
          enum: ["all", "grok", "named", "codex", "cortext"],
          description: "Default all. grok = Grok Bot edges; named = Claude seats; codex = Codex clones",
        },
        q: {
          type: "string",
          description: "Case-insensitive search across id, nickname, machine, summary",
        },
        active_only: {
          type: "boolean",
          description: "Default true. Hides headless idle-awaiting-directive zombies (broker filter).",
        },
        collapse_codex: {
          type: "boolean",
          description: "Default true. Collapse identical codex-local clones into one group entry.",
        },
        verbose: {
          type: "boolean",
          description: "Default false. Full summaries + last_seen timestamps.",
        },
        limit: {
          type: "number",
          description: "Max peers to return after filters (default 40, max 100).",
        },
      },
    },
  },
  {
    name: "send_message",
    description: `Send a text message to a live peer id. from_id is pinned to ${PEER_ID}. Nicknames do not resolve. Offline/stale peers are not queued.`,
    inputSchema: {
      type: "object",
      properties: {
        to_id: {
          type: "string",
          description: "Exact peer id, e.g. pc-nagatha-session-no-tty",
        },
        message: { type: "string", description: "Plain text body" },
        ...messageToolProperties,
      },
      required: ["to_id", "message"],
    },
  },
  {
    name: "check_messages",
    description: `Poll undelivered replies queued to ${PEER_ID}. There is no thread object. Marks messages delivered.`,
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
    const kind = typeof a.kind === "string" ? a.kind : "all";
    const q = typeof a.q === "string" ? a.q.trim().toLowerCase() : "";
    const activeOnly = a.active_only === false ? false : true;
    const collapseCodex = a.collapse_codex === false ? false : true;
    const verbose = a.verbose === true;
    const limitRaw = typeof a.limit === "number" ? a.limit : 40;
    const limit = Math.max(1, Math.min(100, Number.isFinite(limitRaw) ? limitRaw : 40));
    const machine =
      typeof a.machine === "string" && a.machine.trim()
        ? a.machine.trim()
        : undefined;

    const peers = await broker("/list-peers", {
      scope: machine ? "machine" : scope,
      cwd: "/",
      git_root: null,
      machine,
      exclude_id: PEER_ID,
      active_only: activeOnly,
    });

    let rows: PeerRow[] = (Array.isArray(peers) ? peers : []).map((p: any) =>
      shapePeer(p, verbose),
    );

    if (kind !== "all") {
      rows = rows.filter((p) => p.kind === kind);
    }
    if (q) {
      rows = rows.filter((p) =>
        `${p.id} ${p.nickname} ${p.machine} ${p.summary}`.toLowerCase().includes(q),
      );
    }

    rows.sort((a, b) => {
      const kr = kindRank(a.kind) - kindRank(b.kind);
      if (kr !== 0) return kr;
      return (a.nickname || a.id).localeCompare(b.nickname || b.id);
    });

    let collapsed = 0;
    let out: any[] = rows;
    if (collapseCodex) {
      const keep: any[] = [];
      const codexLocals: PeerRow[] = [];
      for (const p of rows) {
        if (p.kind === "codex" && (p.nickname || "").toLowerCase() === "codex-local") {
          codexLocals.push(p);
        } else {
          keep.push(p);
        }
      }
      if (codexLocals.length > 1) {
        collapsed = codexLocals.length;
        keep.push({
          id: "(codex-local-group)",
          nickname: "codex-local",
          machine: "mac/pc",
          kind: "codex",
          summary: `${codexLocals.length} ephemeral Codex clones collapsed — pass collapse_codex=false or kind=codex to expand`,
          age_s: Math.min(...codexLocals.map((c) => c.age_s ?? 999999)),
          sample_ids: codexLocals.slice(0, 5).map((c) => c.id),
          count: codexLocals.length,
        });
        out = keep;
      }
    }

    out = out.slice(0, limit);

    return textResult(
      JSON.stringify(
        {
          self: { id: PEER_ID, nickname: PEER_NICK, machine: PEER_MACHINE },
          count: out.length,
          total_matched: rows.length,
          collapsed_codex: collapsed || undefined,
          filters: {
            scope: machine ? "machine" : scope,
            machine: machine ?? null,
            kind,
            q: q || null,
            active_only: activeOnly,
            collapse_codex: collapseCodex,
            verbose,
            limit,
          },
          peers: out,
        },
        null,
        2,
      ),
    );
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
    const sent = await sendPeerMessage(broker, {
      from_id: PEER_ID,
      to_id,
      text: message,
    }, a);
    if (!sent?.ok) {
      return textResult(sent?.error ?? "send failed", true);
    }
    return textResult(
      JSON.stringify({
        ok: true,
        from_id: PEER_ID,
        to_id,
        message_id: sent.message_id,
        duplicate: sent.duplicate,
        note: `Queued if target is live. No durable mailbox. Poll check_messages for replies to ${PEER_ID}.`,
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
      serverInfo: { name: "grok-peers-edge", version: "0.2.0" },
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
        nickname: PEER_NICK,
        broker: BROKER,
        tools: TOOLS.map((t) => t.name),
        version: "0.2.0",
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

console.error(`[grok-edge] ${HOST}:${PORT} → ${BROKER} as ${PEER_ID} (${PEER_NICK})`);
