import { afterAll, beforeAll, describe, expect, test } from "bun:test";

const probe = Bun.serve({
  port: 0,
  fetch() {
    return new Response("");
  },
});
const PORT = probe.port;
probe.stop(true);

const DB = `/tmp/claude-peers-delivery-${process.pid}.db`;
const BASE = `http://127.0.0.1:${PORT}`;
const VISIBILITY_TIMEOUT_MS = 100;

let broker: ReturnType<typeof Bun.spawn> | null = null;

type JsonObject = Record<string, unknown>;

async function post(path: string, body: JsonObject) {
  const response = await fetch(BASE + path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return {
    status: response.status,
    json: (await response.json()) as JsonObject,
  };
}

async function waitForHealth() {
  for (let attempt = 0; attempt < 80; attempt++) {
    try {
      const response = await fetch(BASE + "/health");
      if (response.ok) return;
    } catch {
      // Broker has not started listening yet.
    }
    await Bun.sleep(25);
  }
  throw new Error("isolated broker did not start");
}

function registration(requestedId: string, instanceId: string, pid: number) {
  return {
    payload_version: 2,
    requested_id: requestedId,
    instance_id: instanceId,
    pid,
    cwd: `/tmp/${requestedId}`,
    git_root: null,
    tty: null,
    machine: "delivery-protocol-testbox",
    summary: "delivery protocol integration test",
  };
}

async function listPeer(id: string) {
  const response = await post("/list-peers", {
    scope: "fleet",
    cwd: "/tmp",
    git_root: null,
  });
  expect(response.status).toBe(200);
  if (!Array.isArray(response.json)) return undefined;
  return response.json.find(
    (peer): peer is JsonObject =>
      typeof peer === "object" && peer !== null && peer.id === id,
  );
}

beforeAll(async () => {
  for (const suffix of ["", "-wal", "-shm"]) {
    try {
      await Bun.file(DB + suffix).delete();
    } catch {
      // No prior isolated database file.
    }
  }

  broker = Bun.spawn(["bun", "broker.ts"], {
    cwd: import.meta.dir,
    env: {
      ...process.env,
      CLAUDE_PEERS_PORT: String(PORT),
      CLAUDE_PEERS_DB: DB,
      CLAUDE_PEERS_TOKEN: "",
      CLAUDE_PEERS_REQUIRE_AUTH: "",
      CLAUDE_PEERS_LEASE_TTL_MS: "500",
      CLAUDE_PEERS_VISIBILITY_TIMEOUT_MS: String(VISIBILITY_TIMEOUT_MS),
    },
    stdout: "ignore",
    stderr: "ignore",
  });

  await waitForHealth();
});

afterAll(async () => {
  broker?.kill();
  if (broker) await broker.exited;

  for (const suffix of ["", "-wal", "-shm"]) {
    try {
      await Bun.file(DB + suffix).delete();
    } catch {
      // Ignore already-removed SQLite artifacts.
    }
  }
});

describe("payload v2 runtime ownership", () => {
  test("the first instance owns the stable peer id and a duplicate becomes standby", async () => {
    const first = await post(
      "/register",
      registration("lease-owner-test", "instance-first", 51001),
    );
    expect(first.status).toBe(200);
    expect(first.json.id).toBe("lease-owner-test");
    expect(first.json.role).toBe("owner");
    expect(typeof first.json.lease_id).toBe("string");
    expect((first.json.lease_id as string).length).toBeGreaterThan(0);
    expect(typeof first.json.lease_expires_at).toBe("string");

    await Bun.sleep(20);
    const renewed = await post(
      "/register",
      {
        ...registration("lease-owner-test", "instance-first", 51001),
        lease_id: first.json.lease_id,
      },
    );
    expect(renewed.status).toBe(200);
    expect(renewed.json.role).toBe("owner");
    expect(renewed.json.lease_id).toBe(first.json.lease_id);
    expect(
      Date.parse(renewed.json.lease_expires_at as string),
    ).toBeGreaterThan(Date.parse(first.json.lease_expires_at as string));

    const second = await post(
      "/register",
      registration("lease-owner-test", "instance-second", 51002),
    );
    expect(second.status).toBe(200);
    expect(second.json.id).toBe("lease-owner-test");
    expect(second.json.role).toBe("standby");
    expect(second.json.lease_id ?? null).toBeNull();

    const visibleOwner = await listPeer("lease-owner-test");
    expect(visibleOwner?.pid).toBe(51001);
    expect(visibleOwner?.instance_id).toBeUndefined();

    for (const leaseId of [undefined, "wrong-owner-lease"]) {
      const stolenRenewal = await post("/register", {
        ...registration("lease-owner-test", "instance-first", 51099),
        ...(leaseId ? { lease_id: leaseId } : {}),
      });
      expect(stolenRenewal.status).toBe(409);
      expect(stolenRenewal.json.lease_id).toBeUndefined();
    }
    expect(Object.hasOwn(visibleOwner ?? {}, "lease_id")).toBe(false);

    const standbyClaim = await post("/claim-messages", {
      id: "lease-owner-test",
      instance_id: "instance-second",
      lease_id: "not-an-owner-lease",
    });
    expect(standbyClaim.status).toBe(409);

    const leasedPoll = await post("/poll-messages", { id: "lease-owner-test" });
    expect(leasedPoll.status).toBe(409);

    const legacyTakeover = await post("/register", {
      ...registration("lease-owner-test", "ignored-by-v1", 51003),
      payload_version: 1,
    });
    expect(legacyTakeover.status).toBe(409);
    expect((await listPeer("lease-owner-test"))?.pid).toBe(51001);
  });

  test("payload v2 rejects an invalid instance id instead of silently becoming v1", async () => {
    const invalid = await post("/register", {
      ...registration("lease-invalid-instance", "valid-instance", 51501),
      instance_id: " contains whitespace ",
    });
    expect(invalid.status).toBe(400);
    expect(await listPeer("lease-invalid-instance")).toBeUndefined();
  });

  test("a stale lease cannot heartbeat or unregister the current owner", async () => {
    const owner = await post(
      "/register",
      registration("lease-fence-test", "instance-owner", 52001),
    );
    expect(owner.status).toBe(200);
    expect(owner.json.role).toBe("owner");

    const before = await listPeer("lease-fence-test");
    const beforeLastSeen = before?.last_seen;
    await Bun.sleep(20);

    const staleHeartbeat = await post("/heartbeat", {
      id: "lease-fence-test",
      instance_id: "stale-instance",
      lease_id: "stale-lease",
    });
    expect(staleHeartbeat.status).toBe(409);

    const afterHeartbeat = await listPeer("lease-fence-test");
    expect(afterHeartbeat?.pid).toBe(52001);
    expect(afterHeartbeat?.last_seen).toBe(beforeLastSeen);

    const staleUnregister = await post("/unregister", {
      id: "lease-fence-test",
      instance_id: "stale-instance",
      lease_id: "stale-lease",
    });
    expect(staleUnregister.status).toBe(409);

    const afterUnregister = await listPeer("lease-fence-test");
    expect(afterUnregister?.pid).toBe(52001);
  });

  test("a standby takes ownership only after the current lease expires", async () => {
    const first = await post(
      "/register",
      registration("lease-takeover-test", "instance-original", 52501),
    );
    expect(first.status).toBe(200);
    expect(first.json.role).toBe("owner");

    const standby = await post(
      "/register",
      registration("lease-takeover-test", "instance-standby", 52502),
    );
    expect(standby.status).toBe(200);
    expect(standby.json.role).toBe("standby");

    await Bun.sleep(550);

    const takeover = await post(
      "/register",
      registration("lease-takeover-test", "instance-standby", 52502),
    );
    expect(takeover.status).toBe(200);
    expect(takeover.json.role).toBe("owner");
    expect(typeof takeover.json.lease_id).toBe("string");
    expect(takeover.json.lease_id).not.toBe(first.json.lease_id);

    const visibleOwner = await listPeer("lease-takeover-test");
    expect(visibleOwner?.pid).toBe(52502);

    const staleHeartbeat = await post("/heartbeat", {
      id: "lease-takeover-test",
      instance_id: "instance-original",
      lease_id: first.json.lease_id,
    });
    expect(staleHeartbeat.status).toBe(409);
  });

  test("an expired v2 lease permits a legacy rollback without exposing the old lease", async () => {
    const owner = await post(
      "/register",
      registration("lease-legacy-rollback", "instance-v2", 52601),
    );
    expect(owner.json.role).toBe("owner");
    await Bun.sleep(550);

    const legacy = await post("/register", {
      requested_id: "lease-legacy-rollback",
      pid: 52602,
      cwd: "/tmp/lease-legacy-rollback",
      git_root: null,
      tty: null,
      machine: "delivery-protocol-testbox",
      summary: "legacy rollback",
    });
    expect(legacy.status).toBe(200);
    expect(legacy.json).toEqual({ id: "lease-legacy-rollback" });

    const heartbeat = await post("/heartbeat", { id: "lease-legacy-rollback" });
    expect(heartbeat.status).toBe(200);
    expect(heartbeat.json.found).toBe(true);
  });
});

describe("leased message delivery", () => {
  test("an unacked claim is redelivered after visibility timeout and ack is final", async () => {
    const sender = await post("/register", {
      requested_id: "lease-sender-test",
      pid: 53001,
      cwd: "/tmp/lease-sender-test",
      git_root: null,
      tty: null,
      machine: "delivery-protocol-testbox",
      summary: "legacy sender",
    });
    expect(sender.status).toBe(200);

    const receiver = await post(
      "/register",
      registration("lease-delivery-test", "instance-receiver", 53002),
    );
    expect(receiver.status).toBe(200);
    expect(receiver.json.role).toBe("owner");
    const leaseId = receiver.json.lease_id;
    expect(typeof leaseId).toBe("string");

    const sent = await post("/send-message", {
      from_id: "lease-sender-test",
      to_id: "lease-delivery-test",
      text: "redeliver until explicitly acknowledged",
    });
    expect(sent.status).toBe(200);
    expect(sent.json.ok).toBe(true);

    const claimBody = {
      id: "lease-delivery-test",
      instance_id: "instance-receiver",
      lease_id: leaseId,
    };
    const firstClaim = await post("/claim-messages", claimBody);
    expect(firstClaim.status).toBe(200);
    const firstMessages = firstClaim.json.messages as Array<JsonObject>;
    expect(firstMessages).toHaveLength(1);
    expect(firstMessages[0]?.text).toBe("redeliver until explicitly acknowledged");
    const messageId = firstMessages[0]?.id;
    expect(typeof messageId).toBe("number");

    const hiddenClaim = await post("/claim-messages", claimBody);
    expect(hiddenClaim.status).toBe(200);
    expect(hiddenClaim.json.messages).toEqual([]);

    await Bun.sleep(VISIBILITY_TIMEOUT_MS + 50);
    const redelivery = await post("/claim-messages", claimBody);
    expect(redelivery.status).toBe(200);
    const redeliveredMessages = redelivery.json.messages as Array<JsonObject>;
    expect(redeliveredMessages.map((message) => message.id)).toEqual([messageId]);

    const ack = await post("/ack-messages", {
      ...claimBody,
      message_ids: [messageId],
    });
    expect(ack.status).toBe(200);
    expect(ack.json.ok).toBe(true);
    expect(ack.json.acked).toBe(1);

    await Bun.sleep(VISIBILITY_TIMEOUT_MS + 50);
    const afterAck = await post("/claim-messages", claimBody);
    expect(afterAck.status).toBe(200);
    expect(afterAck.json.messages).toEqual([]);
  });

  test("leased sender identity is fenced on mutation endpoints", async () => {
    const sender = await post(
      "/register",
      registration("lease-mutation-sender", "instance-mutation", 53501),
    );
    expect(sender.status).toBe(200);
    const receiver = await post("/register", {
      requested_id: "lease-mutation-receiver",
      pid: 53502,
      cwd: "/tmp/lease-mutation-receiver",
      git_root: null,
      tty: null,
      machine: "delivery-protocol-testbox",
      summary: "legacy receiver",
    });
    expect(receiver.status).toBe(200);

    const staleSummary = await post("/set-summary", {
      id: "lease-mutation-sender",
      summary: "must not apply",
      instance_id: "stale-instance",
      lease_id: "stale-lease",
    });
    expect(staleSummary.status).toBe(409);

    const spoofedSend = await post("/send-message", {
      from_id: "lease-mutation-sender",
      to_id: "lease-mutation-receiver",
      text: "must not enqueue",
    });
    expect(spoofedSend.status).toBe(409);

    const authorizedSend = await post("/send-message", {
      from_id: "lease-mutation-sender",
      to_id: "lease-mutation-receiver",
      text: "lease-authenticated send",
      instance_id: "instance-mutation",
      lease_id: sender.json.lease_id,
    });
    expect(authorizedSend.status).toBe(200);
    expect(authorizedSend.json.ok).toBe(true);

    const received = await post("/poll-messages", { id: "lease-mutation-receiver" });
    expect((received.json.messages as Array<JsonObject>).map((message) => message.text)).toEqual([
      "lease-authenticated send",
    ]);
  });
});

describe("legacy delivery compatibility", () => {
  test("payload v1 registration and destructive poll continue to work", async () => {
    const sender = await post("/register", {
      requested_id: "legacy-poll-sender",
      pid: 54001,
      cwd: "/tmp/legacy-poll-sender",
      git_root: null,
      tty: null,
      machine: "delivery-protocol-testbox",
      summary: "legacy sender",
    });
    const receiver = await post("/register", {
      requested_id: "legacy-poll-receiver",
      pid: 54002,
      cwd: "/tmp/legacy-poll-receiver",
      git_root: null,
      tty: null,
      machine: "delivery-protocol-testbox",
      summary: "legacy receiver",
    });
    expect(sender.status).toBe(200);
    expect(receiver.status).toBe(200);

    const sent = await post("/send-message", {
      from_id: "legacy-poll-sender",
      to_id: "legacy-poll-receiver",
      text: "legacy poll payload",
    });
    expect(sent.json.ok).toBe(true);

    const firstPoll = await post("/poll-messages", { id: "legacy-poll-receiver" });
    expect(firstPoll.status).toBe(200);
    const firstMessages = firstPoll.json.messages as Array<JsonObject>;
    expect(firstMessages).toHaveLength(1);
    expect(firstMessages[0]?.text).toBe("legacy poll payload");

    const secondPoll = await post("/poll-messages", { id: "legacy-poll-receiver" });
    expect(secondPoll.status).toBe(200);
    expect(secondPoll.json.messages).toEqual([]);
  });
});

// Two different reaping rules, and which one applies decides how long a dead
// session keeps being advertised. Measured 2026-09-17 against a real broker: a
// peer on the broker's own machine went in 8.0s (the 30s pid sweep), while the
// same peer claiming a different machine survived until its TTL — 89.8s with a
// 1-minute TTL, and 20 minutes on the default. Nothing here changes broker.ts;
// these pin the semantics the client-side lifecycle fix now leans on.
describe("stale peer reaping", () => {
  async function deadPid(): Promise<number> {
    const corpse = Bun.spawn(["bun", "-e", ""], { stdout: "ignore", stderr: "ignore" });
    await corpse.exited;
    return corpse.pid!;
  }

  function registrationOn(requestedId: string, machine: string, pid: number) {
    return { ...registration(requestedId, `${requestedId}-instance`, pid), machine };
  }

  async function listedIds(): Promise<string[]> {
    const response = await post("/list-peers", { scope: "fleet" });
    return ((response.json as unknown as Array<{ id: string }>) ?? []).map((p) => p.id);
  }

  test("a dead peer on the broker's own machine is reaped by the pid check", async () => {
    const pid = await deadPid();
    const registered = await post("/register", registrationOn("reap-local", require("os").hostname(), pid));
    expect(registered.status).toBe(200);

    // /list-peers runs cleanStalePeers, so no 30s wait is needed to observe it.
    expect(await listedIds()).not.toContain("reap-local");
  });

  test("a dead peer on another machine survives until its TTL", async () => {
    const pid = await deadPid();
    const registered = await post("/register", registrationOn("reap-remote", "some-other-box", pid));
    expect(registered.status).toBe(200);

    // Same dead pid, same sweep — only the machine differs. The broker cannot
    // check liveness across a machine boundary, so the row has to stay.
    expect(await listedIds()).toContain("reap-remote");
  });
});
