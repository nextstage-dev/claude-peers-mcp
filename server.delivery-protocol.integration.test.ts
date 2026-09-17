import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";

setDefaultTimeout(5_000);

type JsonObject = Record<string, unknown>;

type JsonRpcMessage = {
  jsonrpc?: string;
  id?: number | string;
  method?: string;
  params?: JsonObject;
  result?: unknown;
  error?: unknown;
};

type BrokerRequest = {
  path: string;
  body: JsonObject;
};

type BrokerReply = {
  status?: number;
  json: unknown;
};

type BrokerHandler = (
  path: string,
  body: JsonObject,
  requests: readonly BrokerRequest[],
) => BrokerReply | undefined;

const activeHarnesses = new Set<ClientHarness>();
const textEncoder = new TextEncoder();

function jsonResponse(reply: BrokerReply): Response {
  return Response.json(reply.json, { status: reply.status ?? 200 });
}

async function waitUntil(
  predicate: () => boolean,
  label: string,
  timeoutMs = 1_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error(`Timed out waiting for ${label}`);
}

function defaultBrokerReply(path: string, body: JsonObject): BrokerReply {
  switch (path) {
    case "/list-peers":
      return { json: [] };
    case "/heartbeat":
      return { json: { ok: true, found: true, owner: true } };
    case "/claim-messages":
    case "/poll-messages":
      return { json: { messages: [] } };
    case "/ack-messages":
      return {
        json: {
          ok: true,
          acked: Array.isArray(body.message_ids) ? body.message_ids.length : 0,
        },
      };
    case "/send-message":
    case "/set-nickname":
    case "/set-context":
    case "/set-summary":
    case "/set-state":
    case "/unregister":
      return { json: { ok: true } };
    default:
      return { status: 404, json: { error: "unexpected fake broker route" } };
  }
}

function providerKeysCleared(env: Record<string, string | undefined>) {
  return {
    ...env,
    OPENAI_API_KEY: "",
    ANTHROPIC_API_KEY: "",
    CLAUDE_API_KEY: "",
    GEMINI_API_KEY: "",
    GOOGLE_API_KEY: "",
    XAI_API_KEY: "",
  };
}

class ClientHarness {
  readonly requests: BrokerRequest[] = [];
  readonly rpcMessages: JsonRpcMessage[] = [];
  readonly events: string[] = [];
  readonly broker: ReturnType<typeof Bun.serve>;
  readonly process: Bun.Subprocess<"pipe", "pipe", "pipe">;

  private rpcId = 10;
  private stopped = false;
  private stderrText = "";
  private readonly spawnThroughParent: boolean;
  private readonly stdoutPump: Promise<void>;
  private readonly stderrPump: Promise<void>;

  constructor(
    handler: BrokerHandler,
    envOverrides: Record<string, string> = {},
    spawnThroughParent = false,
  ) {
    this.spawnThroughParent = spawnThroughParent;
    this.broker = Bun.serve({
      port: 0,
      fetch: async (request) => {
        const url = new URL(request.url);
        if (request.method === "GET" && url.pathname === "/health") {
          return Response.json({ ok: true });
        }

        if (request.method !== "POST") {
          return Response.json({ error: "method not allowed" }, { status: 405 });
        }

        const body = (await request.json()) as JsonObject;
        this.requests.push({ path: url.pathname, body });
        this.events.push(`broker:${url.pathname}`);
        return jsonResponse(
          handler(url.pathname, body, this.requests) ??
            defaultBrokerReply(url.pathname, body),
        );
      },
    });

    const command = spawnThroughParent
      ? ["/bin/sh", "-c", "bun server.ts; exit $?"]
      : ["bun", "server.ts"];
    this.process = Bun.spawn(command, {
      cwd: import.meta.dir,
      env: providerKeysCleared({
        ...process.env,
        CLAUDE_PEERS_BROKER_URL: `http://127.0.0.1:${this.broker.port}`,
        CLAUDE_PEERS_TOKEN: "",
        CLAUDE_PEERS_MACHINE: "server-protocol-testbox",
        CLAUDE_PEERS_NICKNAME: "server-protocol-test-client",
        CLAUDE_PEER_ID: "server-protocol-test-peer",
        CLAUDE_PEERS_DISABLE_CHANNEL: "0",
        // Never inherit the host session's inbox socket: the spawned server
        // would deliver test fixtures into the developer's live session.
        CLAUDE_CODE_MESSAGING_SOCKET: "",
        CLAUDE_CODE_MESSAGING_TOKEN: "",
        CLAUDE_PEERS_RESPONSE_DELAY_MS: "0",
        CLAUDE_PEERS_POLL_INTERVAL_MS: "20",
        CLAUDE_PEERS_HEARTBEAT_INTERVAL_MS: "35",
        CLAUDE_PEERS_REGISTER_RETRY_MS: "30",
        ...envOverrides,
      }),
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
    });

    this.stdoutPump = this.readRpcMessages(this.process.stdout);
    this.stderrPump = this.readStderr(this.process.stderr);
  }

  async initialize(): Promise<void> {
    await this.writeRpc({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {
        protocolVersion: "2025-03-26",
        capabilities: {},
        clientInfo: { name: "server-protocol-integration-test", version: "1.0.0" },
      },
    });
    await waitUntil(
      () => this.rpcMessages.some((message) => message.id === 1),
      "MCP initialize response",
    );
    await this.writeRpc({
      jsonrpc: "2.0",
      method: "notifications/initialized",
      params: {},
    });
  }

  requestsFor(path: string): BrokerRequest[] {
    return this.requests.filter((request) => request.path === path);
  }

  async waitForRequest(path: string, count = 1, timeoutMs = 1_000): Promise<void> {
    await waitUntil(
      () => this.requestsFor(path).length >= count,
      `${path} request ${count}`,
      timeoutMs,
    );
  }

  async waitForNotification(method: string, timeoutMs = 1_000): Promise<void> {
    await waitUntil(
      () => this.rpcMessages.some((message) => message.method === method),
      `${method} notification`,
      timeoutMs,
    );
  }

  async callTool(name: string, args: JsonObject): Promise<void> {
    const id = ++this.rpcId;
    await this.writeRpc({
      jsonrpc: "2.0",
      id,
      method: "tools/call",
      params: { name, arguments: args },
    });
    await waitUntil(
      () => this.rpcMessages.some((message) => message.id === id),
      `${name} tool response`,
    );
    const response = this.rpcMessages.find((message) => message.id === id);
    if (response?.error) {
      throw new Error(`${name} returned an MCP error`);
    }
  }

  async killParentOnly(): Promise<void> {
    if (!this.spawnThroughParent) {
      throw new Error("Harness was not started through a parent wrapper");
    }
    this.process.kill("SIGKILL");
    await this.process.exited;
  }

  async stop(): Promise<string> {
    if (this.stopped) return this.stderrText;
    this.stopped = true;

    if (this.spawnThroughParent) {
      const registeredPid = this.requestsFor("/register")[0]?.body.pid;
      if (typeof registeredPid === "number") {
        try {
          process.kill(registeredPid, "SIGTERM");
        } catch {
          // Server already exited after detecting its missing parent.
        }
      }
      await Bun.sleep(100);
    }

    if (this.process.exitCode === null) {
      this.process.kill("SIGTERM");
      const exited = await Promise.race([
        this.process.exited.then(() => true),
        Bun.sleep(750).then(() => false),
      ]);
      if (!exited) {
        this.process.kill("SIGKILL");
        await this.process.exited;
      }
    }

    await Promise.all([this.stdoutPump, this.stderrPump]);
    this.broker.stop(true);
    activeHarnesses.delete(this);
    return this.stderrText;
  }

  private async writeRpc(message: JsonRpcMessage): Promise<void> {
    this.process.stdin.write(textEncoder.encode(`${JSON.stringify(message)}\n`));
    await this.process.stdin.flush();
  }

  private async readRpcMessages(stream: ReadableStream<Uint8Array>): Promise<void> {
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let buffered = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffered += decoder.decode(value, { stream: true });
      const lines = buffered.split("\n");
      buffered = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) continue;
        const message = JSON.parse(line) as JsonRpcMessage;
        this.rpcMessages.push(message);
        if (message.method) this.events.push(`rpc:${message.method}`);
      }
    }
  }

  private async readStderr(stream: ReadableStream<Uint8Array>): Promise<void> {
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      this.stderrText += decoder.decode(value, { stream: true });
    }
    this.stderrText += decoder.decode();
  }
}

async function startClient(
  handler: BrokerHandler,
  envOverrides: Record<string, string> = {},
  spawnThroughParent = false,
): Promise<ClientHarness> {
  const harness = new ClientHarness(handler, envOverrides, spawnThroughParent);
  activeHarnesses.add(harness);
  await harness.initialize();
  return harness;
}

function expectLease(body: JsonObject, instanceId: string, leaseId: string): void {
  expect(body.instance_id === instanceId).toBe(true);
  expect(body.lease_id === leaseId).toBe(true);
}

afterEach(async () => {
  for (const harness of [...activeHarnesses]) {
    await harness.stop();
  }
});

describe("server.ts payload v2 delivery client", () => {
  test("registers payload v2 with a nonempty per-process instance ID", async () => {
    const harness = await startClient((path) => {
      if (path === "/register") {
        return {
          json: {
            id: "server-protocol-test-peer",
            role: "owner",
            lease_id: "owner-lease-registration-test",
            lease_expires_at: new Date(Date.now() + 30_000).toISOString(),
          },
        };
      }
    });

    await harness.waitForRequest("/register");
    const registration = harness.requestsFor("/register")[0]!.body;
    expect(registration.payload_version).toBe(2);
    expect(typeof registration.instance_id).toBe("string");
    expect((registration.instance_id as string).length).toBeGreaterThan(0);
  });

  test("claims, pushes, and acknowledges as owner with the lease on every operation", async () => {
    const incomingText = "private inbound payload must stay out of diagnostic logs";
    const leaseId = "owner-lease-flow-test";
    let claimServed = false;

    const harness = await startClient((path, body) => {
      if (path === "/register") {
        return {
          json: {
            id: "server-protocol-test-peer",
            role: "owner",
            lease_id: leaseId,
            lease_expires_at: new Date(Date.now() + 30_000).toISOString(),
          },
        };
      }
      if (path === "/claim-messages") {
        if (claimServed) return { json: { messages: [] } };
        claimServed = true;
        return {
          json: {
            messages: [
              {
                id: 701,
                from_id: "fake-sender",
                to_id: "server-protocol-test-peer",
                text: incomingText,
                sent_at: "2026-09-01T12:00:00.000Z",
                delivered: false,
              },
            ],
          },
        };
      }
      if (path === "/ack-messages") {
        return {
          json: {
            ok: true,
            acked: Array.isArray(body.message_ids) ? body.message_ids.length : 0,
          },
        };
      }
    });

    await harness.waitForRequest("/register");
    const registration = harness.requestsFor("/register")[0]!.body;
    const instanceId = registration.instance_id as string;

    await harness.waitForRequest("/claim-messages", 1, 1_250);
    await harness.waitForNotification("notifications/claude/channel", 1_250);
    await Bun.sleep(100);
    expect(harness.requestsFor("/ack-messages")).toHaveLength(0);

    await harness.callTool("ack_message", { message_ids: [701] });
    await harness.waitForRequest("/ack-messages", 1, 1_250);
    await harness.waitForRequest("/heartbeat", 1, 1_250);

    await harness.callTool("set_nickname", { nickname: "leased-client" });
    await harness.callTool("set_context", {
      context_window: 200_000,
      context_used: 1_000,
      context_note: "protocol integration test",
    });
    await harness.callTool("set_summary", { summary: "protocol integration test" });
    await harness.callTool("send_message", {
      to_id: "fake-recipient",
      message: "outbound protocol test payload",
    });

    await harness.stop();
    await harness.waitForRequest("/unregister");

    for (const path of [
      "/claim-messages",
      "/ack-messages",
      "/heartbeat",
      "/set-nickname",
      "/set-context",
      "/set-summary",
      "/send-message",
      "/unregister",
    ]) {
      expectLease(harness.requestsFor(path)[0]!.body, instanceId, leaseId);
    }

    const ack = harness.requestsFor("/ack-messages")[0]!.body;
    expect(Array.isArray(ack.message_ids)).toBe(true);
    expect((ack.message_ids as number[]).includes(701)).toBe(true);

    const notificationEvent = harness.events.indexOf(
      "rpc:notifications/claude/channel",
    );
    const ackEvent = harness.events.indexOf("broker:/ack-messages");
    expect(notificationEvent).toBeGreaterThan(-1);
    expect(ackEvent).toBeGreaterThan(notificationEvent);

    const pushed = harness.rpcMessages.some(
      (message) =>
        message.method === "notifications/claude/channel" &&
        typeof message.params?.content === "string" &&
        (message.params.content as string).length === incomingText.length,
    );
    expect(pushed).toBe(true);
    expect((await harness.stop()).includes(incomingText)).toBe(false);
  });

  test("standby retries without claiming or unregistering and takes ownership when granted", async () => {
    const leaseId = "standby-takeover-lease";
    let grantOwnership = false;

    const harness = await startClient((path) => {
      if (path === "/register") {
        if (!grantOwnership) {
          return {
            json: {
              id: "server-protocol-test-peer",
              role: "standby",
              lease_id: null,
              lease_expires_at: new Date(Date.now() + 50).toISOString(),
            },
          };
        }
        return {
          json: {
            id: "server-protocol-test-peer",
            role: "owner",
            lease_id: leaseId,
            lease_expires_at: new Date(Date.now() + 30_000).toISOString(),
          },
        };
      }
    });

    await harness.waitForRequest("/register", 2, 700);
    expect(harness.requestsFor("/claim-messages").length).toBe(0);
    expect(harness.requestsFor("/unregister").length).toBe(0);

    grantOwnership = true;
    await harness.waitForRequest("/register", 3, 700);
    await harness.waitForRequest("/claim-messages", 1, 700);

    const registrations = harness.requestsFor("/register").map((request) => request.body);
    const instanceIds = registrations.map((body) => body.instance_id);
    expect(instanceIds.every((instanceId) => typeof instanceId === "string" && instanceId.length > 0)).toBe(true);
    expect(new Set(instanceIds).size).toBe(1);

    const instanceId = instanceIds[0] as string;
    expectLease(harness.requestsFor("/claim-messages")[0]!.body, instanceId, leaseId);

    await harness.stop();
    await harness.waitForRequest("/unregister");
    expectLease(harness.requestsFor("/unregister")[0]!.body, instanceId, leaseId);
  });

  test("manual checks retain broker recoverability until ack_message is called", async () => {
    const leaseId = "manual-check-lease";
    let claimServed = false;
    const harness = await startClient(
      (path, body) => {
        if (path === "/register") {
          return {
            json: {
              id: "server-protocol-test-peer",
              role: "owner",
              lease_id: leaseId,
              lease_expires_at: new Date(Date.now() + 30_000).toISOString(),
            },
          };
        }
        if (path === "/claim-messages") {
          if (claimServed) return { json: { messages: [] } };
          claimServed = true;
          return {
            json: {
              messages: [
                {
                  id: 702,
                  from_id: "manual-sender",
                  to_id: "server-protocol-test-peer",
                  text: "manual receipt payload",
                  sent_at: "2026-09-01T12:01:00.000Z",
                  delivered: false,
                },
              ],
            },
          };
        }
        if (path === "/ack-messages") {
          return {
            json: {
              ok: true,
              acked: Array.isArray(body.message_ids) ? body.message_ids.length : 0,
            },
          };
        }
      },
      { CLAUDE_PEERS_DISABLE_CHANNEL: "1" },
    );

    await harness.waitForRequest("/register");
    await harness.callTool("check_messages", {});
    expect(harness.requestsFor("/ack-messages")).toHaveLength(0);

    await harness.callTool("ack_message", { message_ids: [702] });
    await harness.waitForRequest("/ack-messages");
    expect(harness.requestsFor("/ack-messages")[0]!.body.message_ids).toEqual([702]);
  });

  // The delivery notification names a single quoted id, so sessions call
  // ack_message with {message_id: "703"}. That used to error, nothing was
  // acked, and the broker redelivered the same message forever.
  test("ack_message accepts the singular message_id the notification asks for", async () => {
    const leaseId = "singular-ack-lease";
    let claimServed = false;
    const harness = await startClient(
      (path, body) => {
        if (path === "/register") {
          return {
            json: {
              id: "server-protocol-test-peer",
              role: "owner",
              lease_id: leaseId,
              lease_expires_at: new Date(Date.now() + 30_000).toISOString(),
            },
          };
        }
        if (path === "/claim-messages") {
          if (claimServed) return { json: { messages: [] } };
          claimServed = true;
          return {
            json: {
              messages: [
                {
                  id: 703,
                  from_id: "singular-sender",
                  to_id: "server-protocol-test-peer",
                  text: "singular ack payload",
                  sent_at: "2026-09-01T12:02:00.000Z",
                  delivered: false,
                },
              ],
            },
          };
        }
        if (path === "/ack-messages") {
          return {
            json: {
              ok: true,
              acked: Array.isArray(body.message_ids) ? body.message_ids.length : 0,
            },
          };
        }
      },
      { CLAUDE_PEERS_DISABLE_CHANNEL: "1" },
    );

    await harness.waitForRequest("/register");
    await harness.callTool("check_messages", {});

    // Exactly the shape the notification text invites: singular, and a string.
    // Before the fix this errored and no /ack-messages call was ever made.
    await harness.callTool("ack_message", { message_id: "703" });
    await harness.waitForRequest("/ack-messages");
    expect(harness.requestsFor("/ack-messages")[0]!.body.message_ids).toEqual([703]);

    await harness.stop();
  });

  // The broker hides a claimed message for CLAUDE_PEERS_VISIBILITY_TIMEOUT_MS
  // (30s) and then makes it claimable again, so a dead owner's mail is
  // redelivered instead of lost. A live owner re-claimed its own message too:
  // the poll loop pushed the same id into the host session once per visibility
  // window, so a session that took ~66s to call ack_message was interrupted
  // with the same message three times. Scaled 1/100 here to keep the test fast.
  test("a slow application ack never re-pushes a message the host already has", async () => {
    const leaseId = "visibility-redelivery-lease";
    const visibilityMs = 300;
    const inbound = [704, 705].map((id) => ({
      id,
      from_id: "patient-sender",
      to_id: "server-protocol-test-peer",
      text: `redelivery guard payload ${id}`,
      sent_at: "2026-09-01T12:03:00.000Z",
      delivered: false,
    }));
    let claimsServed = 0;
    let servedAt = 0;
    let acked = false;

    const harness = await startClient((path, body) => {
      if (path === "/register") {
        return {
          json: {
            id: "server-protocol-test-peer",
            role: "owner",
            lease_id: leaseId,
            lease_expires_at: new Date(Date.now() + 30_000).toISOString(),
          },
        };
      }
      if (path === "/claim-messages") {
        // Undelivered and out of its visibility window: claimable again.
        if (acked || (servedAt !== 0 && Date.now() - servedAt < visibilityMs)) {
          return { json: { messages: [] } };
        }
        servedAt = Date.now();
        claimsServed += 1;
        return { json: { messages: inbound } };
      }
      if (path === "/ack-messages") {
        acked = true;
        return {
          json: {
            ok: true,
            acked: Array.isArray(body.message_ids) ? body.message_ids.length : 0,
          },
        };
      }
    });

    await harness.waitForRequest("/register");
    await harness.waitForNotification("notifications/claude/channel", 1_250);

    // Sit on both messages across three visibility windows, the way a session
    // busy with a long turn does before it gets around to acking.
    await waitUntil(
      () => claimsServed >= 3,
      "three visibility-timeout re-claims",
      2_500,
    );

    const pushesOf = (id: number) =>
      harness.rpcMessages.filter(
        (message) =>
          message.method === "notifications/claude/channel" &&
          (message.params?.meta as JsonObject | undefined)?.message_id ===
            String(id),
      );
    expect(pushesOf(704)).toHaveLength(1);
    expect(pushesOf(705)).toHaveLength(1);
    expect(
      harness.rpcMessages.filter(
        (message) => message.method === "notifications/claude/channel",
      ),
    ).toHaveLength(2);

    // The late ack still lands, so the broker can finally retire the messages.
    await harness.callTool("ack_message", { message_ids: [704, 705] });
    await harness.waitForRequest("/ack-messages");
    const ack = harness.requestsFor("/ack-messages")[0]!.body;
    expect(ack.message_ids).toEqual([704, 705]);
    expectLease(
      ack,
      harness.requestsFor("/register")[0]!.body.instance_id as string,
      leaseId,
    );

    await harness.stop();
  });

  test("a standby whose parent dies during retry sleep cannot take ownership", async () => {
    let registrationCount = 0;
    const harness = await startClient(
      (path) => {
        if (path === "/register") {
          registrationCount += 1;
          if (registrationCount > 1) {
            return {
              json: {
                id: "server-protocol-test-peer",
                role: "owner",
                lease_id: "orphan-takeover-lease",
                lease_expires_at: new Date(Date.now() + 30_000).toISOString(),
              },
            };
          }
          return {
            json: {
              id: "server-protocol-test-peer",
              role: "standby",
              lease_id: null,
              lease_expires_at: new Date(Date.now() + 500).toISOString(),
            },
          };
        }
      },
      {
        CLAUDE_PEERS_REGISTER_RETRY_MS: "250",
      },
      true,
    );

    await harness.waitForRequest("/register");
    await harness.killParentOnly();
    await Bun.sleep(400);
    expect(harness.requestsFor("/register")).toHaveLength(1);
    expect(harness.requestsFor("/claim-messages")).toHaveLength(0);
    expect(harness.requestsFor("/unregister")).toHaveLength(0);
  });

  test("falls back to legacy polling when a broker omits role and lease", async () => {
    const harness = await startClient((path) => {
      if (path === "/register") {
        return { json: { id: "server-protocol-test-peer" } };
      }
    });

    await harness.waitForRequest("/register");
    await harness.waitForRequest("/poll-messages", 1, 1_250);

    const registration = harness.requestsFor("/register")[0]!.body;
    expect(registration.payload_version).toBe(2);
    expect(typeof registration.instance_id).toBe("string");
    expect((registration.instance_id as string).length).toBeGreaterThan(0);
    expect(harness.requestsFor("/claim-messages").length).toBe(0);
  });
});
