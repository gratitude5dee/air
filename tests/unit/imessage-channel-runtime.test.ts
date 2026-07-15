import { createHmac } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import {
  __setAirRuntimeForTests,
  createAirRuntime,
  drainDurableInbox,
  handleIMessageActionResult,
  handleIMessageInputRequested,
  handleIMessageMessageCompleted,
  handleIMessageSessionCompleted,
  handleIMessageTurnStarted,
  handleIMessageWebhook,
  type AirIMessageAdapter,
  type AirRuntime,
} from "../../lib/imessage-channel-runtime.js";
import { type VerifiedInboxInput } from "../../lib/durable-inbox.js";
import { outboxRecordKey } from "../../lib/durable-outbox.js";
import { loadAirEnvironment } from "../../lib/env.js";
import { acquireRedisLease, MemoryRedis } from "../../lib/redis.js";
import { deriveOpaqueId, hashSenderIdentifier, hashThreadIdentifier } from "../../lib/security.js";
import { AR_AGENT_CAPABILITY_BLURB } from "../../lib/messages.js";

const identitySecret = "runtime-test-identity";
const senderId = "+14155551234";
const chatGuid = "iMessage;-;+14155551234";
const queueKey = Buffer.alloc(32, 11).toString("base64");

class FakeAdapter implements AirIMessageAdapter {
  public readonly sent: Array<{ threadId: string; message: string }> = [];
  public readonly read: string[] = [];

  encodeThreadId(input: { readonly chatGuid: string; readonly phone?: string }): string {
    return `imessage:${input.phone ?? ""}:${input.chatGuid}`;
  }

  async postMessage(threadId: string, message: string): Promise<unknown> {
    this.sent.push({ threadId, message });
    return { id: `outbound-${this.sent.length}` };
  }

  async markRead(_threadId: string, messageId: string): Promise<void> {
    this.read.push(messageId);
  }

  async startTyping(): Promise<void> {}
}

function environment(allowed = hashSenderIdentifier(identitySecret, senderId)) {
  return loadAirEnvironment({
    NODE_ENV: "development",
    IMESSAGE_PROJECT_ID: "project-test",
    IMESSAGE_WEBHOOK_SECRET: "webhook-test-secret",
    REDIS_URL: "redis://fixture.invalid:6379",
    AIR_ID_HASH_SECRET: identitySecret,
    AIR_QUEUE_ENCRYPTION_KEY: queueKey,
    AIR_ALLOWED_SENDER_HASHES: allowed,
    CRON_SECRET: "cron-test-secret",
  });
}

function input(messageId: string, text: string, receivedAtMs = Date.now()): VerifiedInboxInput {
  return {
    projectId: "project-test",
    messageId,
    webhookId: `delivery-${messageId}`,
    senderHash: hashSenderIdentifier(identitySecret, senderId),
    threadHash: hashThreadIdentifier(identitySecret, chatGuid),
    contentType: "text",
    isGroup: false,
    receivedAtMs,
    envelope: {
      projectId: "project-test",
      messageId,
      webhookId: `delivery-${messageId}`,
      chatGuid,
      sendingLine: "+15555550123",
      senderId,
      direction: "inbound",
      content: { type: "text", text },
      isGroup: false,
      receivedAtMs,
    },
  };
}

function injectedRuntime() {
  const redis = new MemoryRedis();
  return createAirRuntime({ environment: environment(), redis });
}

function controlledRuntime(clock: () => number): { runtime: AirRuntime; redis: MemoryRedis } {
  const redis = new MemoryRedis(clock);
  return {
    runtime: createAirRuntime({ environment: environment(), redis, clock }),
    redis,
  };
}

function chatThread(adapter: FakeAdapter) {
  return {
    id: adapter.encodeThreadId({ chatGuid, phone: "+15555550123" }),
    post: async (message: string) => {
      adapter.sent.push({ threadId: "chat-sdk-thread", message });
      return { id: `chat-sdk-outbound-${adapter.sent.length}` };
    },
  };
}

async function activeAttemptToken(runtime: AirRuntime, redis: MemoryRedis, threadId: string): Promise<string> {
  const key = `air:active-turn:v1:${deriveOpaqueId(identitySecret, "chat-sdk-thread", threadId)}`;
  const raw = await redis.get(key);
  if (!raw) throw new Error("expected an active iMessage turn");
  const active = runtime.cipher.decrypt<{ attemptToken?: unknown }>(JSON.parse(raw), key);
  if (typeof active.attemptToken !== "string") throw new Error("active turn has no attempt token");
  return active.attemptToken;
}

function completedToolResult(turnId: string, rendered: string): unknown {
  return {
    status: "completed",
    turnId,
    result: {
      kind: "tool-result",
      toolName: "answer_music_question",
      output: {
        kind: "air.grounded-answer.v1",
        rendered,
      },
    },
  };
}

afterEach(() => {
  __setAirRuntimeForTests(undefined);
});

describe("durable iMessage transport", () => {
  it("hands off once, ignores raw model prose, and posts only the stored validated tool result", async () => {
    const runtime = injectedRuntime();
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    const persisted = await runtime.inbox.persistVerified(input("question-1", "How do labels develop artists?"));
    const handoffs: unknown[] = [];

    await drainDurableInbox({
      adapter,
      receive: async (turn) => {
        handoffs.push(turn);
        return { id: "session-1" };
      },
    });

    expect(handoffs).toHaveLength(1);
    const thread = {
      id: adapter.encodeThreadId({ chatGuid, phone: "+15555550123" }),
      post: async (message: string) => {
        adapter.sent.push({ threadId: "chat-sdk-thread", message });
        return { id: "chat-sdk-outbound" };
      },
    };
    await handleIMessageTurnStarted({
      event: { turnId: "turn-1" },
      eventChannel: { thread },
      sessionId: "session-1",
    });
    await handleIMessageActionResult({
      event: {
        status: "completed",
        turnId: "turn-1",
        result: {
          kind: "tool-result",
          toolName: "answer_music_question",
          output: {
            kind: "air.grounded-answer.v1",
            rendered: "Labels often combine development investment with release planning. [S1]",
          },
        },
      },
      eventChannel: { thread },
      sessionId: "session-1",
    });
    await handleIMessageMessageCompleted({
      event: { finishReason: "stop", turnId: "turn-1", message: "unsafe free-form model prose" },
      eventChannel: { thread },
      sessionId: "session-1",
    });

    expect(adapter.sent).toEqual([
      {
        threadId: "chat-sdk-thread",
        message: "Labels often combine development investment with release planning. [S1]",
      },
    ]);
    expect((await runtime.inbox.getRecord(persisted.record.immutable.recordKey))?.state).toBe("sent");
  });

  it("maps the typed citation-validation fallback to the fixed capability blurb", async () => {
    const runtime = injectedRuntime();
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    const thread = chatThread(adapter);
    await runtime.inbox.persistVerified(input("citation-fallback-1", "How should a release roll out?"));

    await drainDurableInbox({ adapter, receive: async () => ({ id: "citation-fallback-session" }) });
    await handleIMessageTurnStarted({
      event: { turnId: "citation-fallback-turn" },
      eventChannel: { thread },
      sessionId: "citation-fallback-session",
    });
    await handleIMessageActionResult({
      event: {
        status: "completed",
        turnId: "citation-fallback-turn",
        result: {
          kind: "tool-result",
          toolName: "answer_music_question",
          output: { kind: "air.capability-fallback.v1", reason: "citation_validation" },
        },
      },
      eventChannel: { thread },
      sessionId: "citation-fallback-session",
    });
    await handleIMessageMessageCompleted({
      event: { finishReason: "stop", turnId: "citation-fallback-turn" },
      eventChannel: { thread },
      sessionId: "citation-fallback-session",
    });

    expect(adapter.sent).toEqual([{ threadId: "chat-sdk-thread", message: AR_AGENT_CAPABILITY_BLURB }]);
  });

  it("handles a greeting without starting Eve or retrieval work", async () => {
    const runtime = injectedRuntime();
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    await runtime.inbox.persistVerified(input("greeting-1", "hello"));
    const handoffs: unknown[] = [];

    await drainDurableInbox({
      adapter,
      receive: async (turn) => {
        handoffs.push(turn);
        return { id: "greeting-session" };
      },
    });

    expect(handoffs).toEqual([]);
    expect(adapter.sent).toHaveLength(1);
    expect(adapter.sent[0]?.message).toMatch(/transcript-grounded/i);
  });

  it("drops reactions and unknown provider content without a reply or Eve handoff", async () => {
    const runtime = injectedRuntime();
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    const reactionInput = input("reaction-1", "");
    const persisted = await runtime.inbox.persistVerified({
      ...reactionInput,
      contentType: "reaction",
      envelope: {
        ...reactionInput.envelope,
        content: { type: "reaction", text: null },
      },
    });
    const handoffs: unknown[] = [];

    await drainDurableInbox({
      adapter,
      receive: async (turn) => {
        handoffs.push(turn);
        return { id: "reaction-session" };
      },
    });

    expect(handoffs).toEqual([]);
    expect(adapter.sent).toEqual([]);
    expect((await runtime.inbox.getRecord(persisted.record.immutable.recordKey))?.state)
      .toBe("dead_letter");
  });

  it("does not mark a non-allowlisted DM read before its one access notice", async () => {
    const redis = new MemoryRedis();
    const runtime = createAirRuntime({
      environment: environment(hashSenderIdentifier(identitySecret, "+14155559999")),
      redis,
    });
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    await runtime.inbox.persistVerified(input("denied-1", "Can you help with a rollout?"));

    await drainDurableInbox({
      adapter,
      receive: async () => {
        throw new Error("a denied sender must not start Eve");
      },
    });

    expect(adapter.read).toEqual([]);
    expect(adapter.sent).toHaveLength(1);
  });

  it("fences delayed session A callbacks after the inbox is recovered by session B", async () => {
    let now = 0;
    const { runtime } = controlledRuntime(() => now);
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    const thread = chatThread(adapter);
    const persisted = await runtime.inbox.persistVerified(input("session-fence-1", "How should a release roll out?", now));
    const sessions: string[] = [];
    const continuationKeys: string[] = [];

    await drainDurableInbox({
      adapter,
      receive: async (turn) => {
        const id = sessions.length === 0 ? "session-a" : "session-b";
        sessions.push(id);
        continuationKeys.push(turn.target.continuationKey);
        return { id };
      },
    });
    await handleIMessageTurnStarted({
      event: { turnId: "turn-a" },
      eventChannel: { thread },
      sessionId: "session-a",
    });

    // The worker that owned A disappears. Its durable processing lease and the
    // short-lived active map both expire before the recovery worker starts B.
    now += 75_001;
    await drainDurableInbox({
      adapter,
      receive: async (turn) => {
        const id = sessions.length === 0 ? "session-a" : "session-b";
        sessions.push(id);
        continuationKeys.push(turn.target.continuationKey);
        return { id };
      },
    });
    await handleIMessageTurnStarted({
      event: { turnId: "turn-b" },
      eventChannel: { thread },
      sessionId: "session-b",
    });

    expect(sessions).toEqual(["session-a", "session-b"]);
    expect(continuationKeys).toHaveLength(2);
    expect(continuationKeys[0]).toMatch(/^[a-f0-9]{64}$/);
    expect(continuationKeys[1]).toMatch(/^[a-f0-9]{64}$/);
    expect(continuationKeys[1]).not.toBe(continuationKeys[0]);
    await handleIMessageActionResult({
      event: completedToolResult("turn-a", "Stale answer that must never be delivered. [S1]"),
      eventChannel: { thread },
      sessionId: "session-a",
    });
    await handleIMessageMessageCompleted({
      event: { finishReason: "stop", turnId: "turn-a", message: "stale model prose" },
      eventChannel: { thread },
      sessionId: "session-a",
    });

    expect(adapter.sent).toEqual([]);
    expect(await runtime.outbox.getRecord(outboxRecordKey(persisted.record.immutable.correlationId))).toBeNull();
    expect((await runtime.inbox.getRecord(persisted.record.immutable.recordKey))?.state).toBe("session_started");

    await handleIMessageActionResult({
      event: completedToolResult("turn-b", "Recovered session B answer. [S1]"),
      eventChannel: { thread },
      sessionId: "session-b",
    });
    await handleIMessageMessageCompleted({
      event: { finishReason: "stop", turnId: "turn-b", message: "unsafe B model prose" },
      eventChannel: { thread },
      sessionId: "session-b",
    });

    expect(adapter.sent).toEqual([
      { threadId: "chat-sdk-thread", message: "Recovered session B answer. [S1]" },
    ]);
    expect((await runtime.inbox.getRecord(persisted.record.immutable.recordKey))?.state).toBe("sent");
  });

  it("keeps a handed-off Eve turn leased, renews it from lifecycle events, and frees its successor promptly", async () => {
    let now = 0;
    const { runtime, redis } = controlledRuntime(() => now);
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    const thread = chatThread(adapter);
    const first = await runtime.inbox.persistVerified(input("lease-handoff-a", "How should a rollout be sequenced?", now));
    now += 1;
    await runtime.inbox.persistVerified(input("lease-handoff-b", "What should happen next?", now));
    const sessions: string[] = [];

    await drainDurableInbox({
      adapter,
      receive: async () => {
        sessions.push("session-a");
        return { id: "session-a" };
      },
    });
    await handleIMessageTurnStarted({
      event: { turnId: "turn-a" },
      eventChannel: { thread },
      sessionId: "session-a",
    });
    const firstAttempt = await activeAttemptToken(runtime, redis, thread.id);
    const threadLeaseKey = `air:inbox:v1:lease:thread:${first.record.immutable.threadHash}`;
    expect(await acquireRedisLease(redis, threadLeaseKey, { ttlMs: 1_000, now: () => now })).toBeNull();

    // This arrives shortly before the original 75-second lease would expire.
    // The lifecycle callback must extend both the physical queue lock and the
    // encrypted active-map TTL, so a recovery scan cannot start another turn.
    now += 74_000;
    await handleIMessageActionResult({
      event: completedToolResult("turn-a", "A durable handoff remains ordered. [S1]"),
      eventChannel: { thread },
      sessionId: "session-a",
    });
    expect((await runtime.inbox.getRecord(first.record.immutable.recordKey))?.status.processingLeaseExpiresAtMs)
      .toBe(now + 75_000);
    const activeKey = `air:active-turn:v1:${deriveOpaqueId(identitySecret, "chat-sdk-thread", thread.id)}`;
    expect(await redis.pttl(activeKey)).toBe(75_000);

    now += 2_000;
    await drainDurableInbox({
      adapter,
      receive: async () => {
        sessions.push("unexpected-recovery");
        return { id: "unexpected-recovery" };
      },
    });
    expect(sessions).toEqual(["session-a"]);

    await handleIMessageMessageCompleted({
      event: { finishReason: "stop", turnId: "turn-a", message: "unsafe model prose" },
      eventChannel: { thread },
      sessionId: "session-a",
    });
    expect((await runtime.inbox.getRecord(first.record.immutable.recordKey))?.state).toBe("sent");
    expect(await runtime.inbox.heartbeatAttempt(first.record.immutable.recordKey, firstAttempt)).toBe(false);

    await drainDurableInbox({
      adapter,
      receive: async () => {
        sessions.push("session-b");
        return { id: "session-b" };
      },
    });
    expect(sessions).toEqual(["session-a", "session-b"]);
  });

  it("rebinds A-owned encrypted delivery artifacts during post-send crash recovery", async () => {
    let now = 0;
    const { runtime, redis } = controlledRuntime(() => now);
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    const thread = chatThread(adapter);
    const persisted = await runtime.inbox.persistVerified(input("sent-recovery-1", "What should the launch plan include?", now));

    await drainDurableInbox({
      adapter,
      receive: async () => ({ id: "session-a" }),
    });
    await handleIMessageTurnStarted({
      event: { turnId: "turn-a" },
      eventChannel: { thread },
      sessionId: "session-a",
    });
    await handleIMessageActionResult({
      event: completedToolResult("turn-a", "Durable answer awaiting send. [S1]"),
      eventChannel: { thread },
      sessionId: "session-a",
    });

    const firstAttemptToken = await activeAttemptToken(runtime, redis, thread.id);
    expect((await runtime.inbox.transition(persisted.record.immutable.recordKey, "response_ready", {
      expectedStates: ["session_started"],
      expectedAttemptToken: firstAttemptToken,
    }))?.state).toBe("response_ready");
    await runtime.outbox.persistValidatedResponse({
      correlationId: persisted.record.immutable.correlationId,
      renderedResponse: "Durable answer awaiting send. [S1]",
    });
    const providerAttempt = await runtime.outbox.beginSend(outboxRecordKey(persisted.record.immutable.correlationId));
    expect(providerAttempt).not.toBeNull();
    if (!providerAttempt) throw new Error("expected an outbound provider attempt");
    expect((await providerAttempt.markSent())?.state).toBe("sent");

    const targetKey = `air:delivery-target:v1:${persisted.record.immutable.correlationId}`;
    const resultKey = `air:validated-result:v1:${persisted.record.immutable.correlationId}`;
    expect(await redis.get(targetKey)).not.toBeNull();
    expect(await redis.get(resultKey)).not.toBeNull();

    // Simulate a crash after the provider's success was recorded but before the
    // inbox reached `sent`. Recovery receives a fresh B capability and must
    // reconcile without another provider call, then delete A's artifacts.
    now += 75_001;
    await drainDurableInbox({
      adapter,
      receive: async () => {
        throw new Error("a sent outbox must reconcile without starting Eve");
      },
    });

    expect(adapter.sent).toEqual([]);
    expect((await runtime.inbox.getRecord(persisted.record.immutable.recordKey))?.state).toBe("sent");
    expect((await runtime.outbox.getRecord(outboxRecordKey(persisted.record.immutable.correlationId)))?.state).toBe("sent");
    expect(await redis.get(targetKey)).toBeNull();
    expect(await redis.get(resultKey)).toBeNull();
  });

  it("accepts turn.started that arrives before receive returns its session", async () => {
    const runtime = injectedRuntime();
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    const thread = chatThread(adapter);
    const persisted = await runtime.inbox.persistVerified(input("receive-race-1", "How should a campaign be sequenced?"));
    let handoffs = 0;

    await drainDurableInbox({
      adapter,
      receive: async () => {
        handoffs += 1;
        await handleIMessageTurnStarted({
          event: { turnId: "turn-race" },
          eventChannel: { thread },
          sessionId: "session-race",
        });
        return { id: "session-race" };
      },
    });
    await handleIMessageActionResult({
      event: completedToolResult("turn-race", "Early start still produces this answer. [S1]"),
      eventChannel: { thread },
      sessionId: "session-race",
    });
    await handleIMessageMessageCompleted({
      event: { finishReason: "stop", turnId: "turn-race", message: "unsafe model prose" },
      eventChannel: { thread },
      sessionId: "session-race",
    });

    expect(handoffs).toBe(1);
    expect(adapter.sent).toEqual([
      { threadId: "chat-sdk-thread", message: "Early start still produces this answer. [S1]" },
    ]);
    expect((await runtime.inbox.getRecord(persisted.record.immutable.recordKey))?.state).toBe("sent");
  });

  it("does not let a session-only completion send a fallback before turn.started binds it", async () => {
    const runtime = injectedRuntime();
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    const thread = chatThread(adapter);
    const persisted = await runtime.inbox.persistVerified(input(
      "session-only-complete-1",
      "How should we plan the release?",
    ));

    await drainDurableInbox({
      adapter,
      receive: async () => ({ id: "session-before-turn" }),
    });
    await handleIMessageSessionCompleted({
      eventChannel: { thread },
      sessionId: "session-before-turn",
    });

    expect(adapter.sent).toEqual([]);
    expect((await runtime.inbox.getRecord(persisted.record.immutable.recordKey))?.state)
      .toBe("session_started");
  });

  it("delivers rephrase guidance without mutating a task-mode continuation", async () => {
    const runtime = injectedRuntime();
    __setAirRuntimeForTests(runtime);
    const adapter = new FakeAdapter();
    const thread = chatThread(adapter);
    const persisted = await runtime.inbox.persistVerified(input(
      "input-requested-1",
      "How should we organize the release?",
    ));

    await drainDurableInbox({
      adapter,
      receive: async () => ({ id: "input-requested-session" }),
    });
    await handleIMessageTurnStarted({
      event: { turnId: "input-requested-turn" },
      eventChannel: { thread },
      sessionId: "input-requested-session",
    });
    const continuationTokens: string[] = [];

    await expect(handleIMessageInputRequested({
      event: { turnId: "input-requested-turn" },
      eventChannel: {
        thread,
        setContinuationToken: (token) => continuationTokens.push(token),
      },
      sessionId: "input-requested-session",
    })).resolves.toBeUndefined();

    expect(continuationTokens).toEqual([]);
    expect(adapter.sent).toEqual([
      { threadId: "chat-sdk-thread", message: expect.stringMatching(/rephrase/i) },
    ]);
    expect((await runtime.inbox.getRecord(persisted.record.immutable.recordKey))?.state)
      .toBe("sent");
  });

  it("persists a signed public webhook before scheduling exactly one durable handoff", async () => {
    const oldSecret = process.env.IMESSAGE_WEBHOOK_SECRET;
    const oldProject = process.env.IMESSAGE_PROJECT_ID;
    process.env.IMESSAGE_WEBHOOK_SECRET = "webhook-test-secret";
    process.env.IMESSAGE_PROJECT_ID = "project-test";
    try {
      const runtime = injectedRuntime();
      __setAirRuntimeForTests(runtime);
      const adapter = new FakeAdapter();
      const payload = JSON.stringify({
        event: "messages",
        space: { id: chatGuid, phone: "+15555550123", type: "dm" },
        message: {
          id: "webhook-question-1",
          direction: "inbound",
          sender: { id: senderId },
          content: { type: "text", text: "What are release strategy themes?" },
        },
      });
      const timestamp = String(Math.floor(Date.now() / 1_000));
      const signature = `v0=${createHmac("sha256", "webhook-test-secret")
        .update(`v0:${timestamp}:${payload}`)
        .digest("hex")}`;
      const scheduled: Promise<unknown>[] = [];
      const handoffs: unknown[] = [];
      const options = {
        adapter,
        receive: async (turn: unknown) => {
          handoffs.push(turn);
          return { id: "webhook-session" };
        },
        waitUntil: (promise: Promise<unknown>) => scheduled.push(promise),
      };

      const first = await handleIMessageWebhook(
        new Request("https://air.example/webhooks/imessage", {
          method: "POST",
          headers: {
            "x-spectrum-signature": signature,
            "x-spectrum-timestamp": timestamp,
            "x-spectrum-webhook-id": "webhook-delivery-1",
          },
          body: payload,
        }),
        options,
      );
      const duplicate = await handleIMessageWebhook(
        new Request("https://air.example/webhooks/imessage", {
          method: "POST",
          headers: {
            "x-spectrum-signature": signature,
            "x-spectrum-timestamp": timestamp,
            "x-spectrum-webhook-id": "webhook-delivery-2",
          },
          body: payload,
        }),
        options,
      );
      await Promise.all(scheduled);

      expect(first.status).toBe(200);
      expect(duplicate.status).toBe(200);
      expect(scheduled).toHaveLength(1);
      expect(handoffs).toHaveLength(1);
    } finally {
      if (oldSecret === undefined) delete process.env.IMESSAGE_WEBHOOK_SECRET;
      else process.env.IMESSAGE_WEBHOOK_SECRET = oldSecret;
      if (oldProject === undefined) delete process.env.IMESSAGE_PROJECT_ID;
      else process.env.IMESSAGE_PROJECT_ID = oldProject;
    }
  });

  it("acknowledges a signed reaction without queueing, alertable failure, or a handoff", async () => {
    const oldSecret = process.env.IMESSAGE_WEBHOOK_SECRET;
    const oldProject = process.env.IMESSAGE_PROJECT_ID;
    process.env.IMESSAGE_WEBHOOK_SECRET = "webhook-test-secret";
    process.env.IMESSAGE_PROJECT_ID = "project-test";
    try {
      const runtime = injectedRuntime();
      __setAirRuntimeForTests(runtime);
      const adapter = new FakeAdapter();
      const payload = JSON.stringify({
        event: "messages",
        space: { id: chatGuid, phone: "+15555550123", type: "dm" },
        message: {
          id: "webhook-reaction-1",
          direction: "inbound",
          sender: { id: senderId },
          content: { type: "reaction", text: null },
        },
      });
      const timestamp = String(Math.floor(Date.now() / 1_000));
      const signature = `v0=${createHmac("sha256", "webhook-test-secret")
        .update(`v0:${timestamp}:${payload}`)
        .digest("hex")}`;
      const scheduled: Promise<unknown>[] = [];

      const response = await handleIMessageWebhook(
        new Request("https://air.example/webhooks/imessage", {
          method: "POST",
          headers: {
            "x-spectrum-signature": signature,
            "x-spectrum-timestamp": timestamp,
            "x-spectrum-webhook-id": "webhook-reaction-delivery-1",
          },
          body: payload,
        }),
        {
          adapter,
          receive: async () => {
            throw new Error("reactions must not start Eve");
          },
          waitUntil: (promise: Promise<unknown>) => scheduled.push(promise),
        },
      );

      expect(response.status).toBe(204);
      expect(scheduled).toEqual([]);
      expect(await runtime.inbox.listDue()).toEqual([]);
      expect(adapter.sent).toEqual([]);
    } finally {
      if (oldSecret === undefined) delete process.env.IMESSAGE_WEBHOOK_SECRET;
      else process.env.IMESSAGE_WEBHOOK_SECRET = oldSecret;
      if (oldProject === undefined) delete process.env.IMESSAGE_PROJECT_ID;
      else process.env.IMESSAGE_PROJECT_ID = oldProject;
    }
  });
});
