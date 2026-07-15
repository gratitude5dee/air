import { createHmac } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import {
  __setAirRuntimeForTests,
  createAirRuntime,
  drainDurableInbox,
  drainFromOperatorRoute,
  handleIMessageActionResult,
  handleIMessageMessageCompleted,
  handleIMessageTurnStarted,
  handleIMessageWebhook,
  type AirIMessageAdapter,
  type AirIMessageTurnInput,
  type AirRuntime,
} from "../../lib/imessage-channel-runtime.js";
import { inboxRecordKey, type VerifiedInboxInput } from "../../lib/durable-inbox.js";
import { loadAirEnvironment } from "../../lib/env.js";
import { MemoryRedis, PERSIST_INBOX_NEW_RECORD_LUA } from "../../lib/redis.js";
import { hashSenderIdentifier, hashThreadIdentifier } from "../../lib/security.js";

const projectId = "project-ingress-test";
const webhookSecret = "webhook-ingress-test-secret";
const cronSecret = "cron-ingress-test-secret";
const identitySecret = "ingress-test-identity-secret";
const queueKey = Buffer.alloc(32, 17).toString("base64");
const senderId = "+14155551234";
const chatGuid = "iMessage;-;+14155551234";
const sendingLine = "+15555550123";

class FakeAdapter implements AirIMessageAdapter {
  public readonly sent: Array<{ threadId: string; message: string }> = [];

  encodeThreadId(input: { readonly chatGuid: string; readonly phone?: string }): string {
    return input.phone
      ? `imessage:${input.chatGuid}~${input.phone}`
      : `imessage:${input.chatGuid}`;
  }

  async postMessage(threadId: string, message: string): Promise<unknown> {
    this.sent.push({ threadId, message });
    return { id: `outbound-${this.sent.length}` };
  }

  async markRead(): Promise<void> {}

  async startTyping(): Promise<void> {}
}

class RecordingRedis extends MemoryRedis {
  public readonly order: string[] = [];
  public inboxPublicationCalls = 0;

  override async eval(
    script: string,
    options: { keys: readonly string[]; arguments: readonly string[] },
  ): Promise<unknown> {
    if (script === PERSIST_INBOX_NEW_RECORD_LUA) {
      this.inboxPublicationCalls += 1;
      this.order.push("persist");
    }
    return super.eval(script, options);
  }
}

function environment() {
  return loadAirEnvironment({
    NODE_ENV: "development",
    IMESSAGE_PROJECT_ID: projectId,
    IMESSAGE_WEBHOOK_SECRET: webhookSecret,
    REDIS_URL: "redis://fixture.invalid:6379",
    AIR_ID_HASH_SECRET: identitySecret,
    AIR_QUEUE_ENCRYPTION_KEY: queueKey,
    AIR_ALLOWED_SENDER_HASHES: hashSenderIdentifier(identitySecret, senderId),
    CRON_SECRET: cronSecret,
  });
}

function runtimeWith(redis: MemoryRedis, clock?: () => number): AirRuntime {
  return createAirRuntime({
    environment: environment(),
    redis,
    ...(clock ? { clock } : {}),
  });
}

function signedPayload(
  messageId: string,
  text: string,
  options: { readonly omitSpaceType?: boolean } = {},
): string {
  const space = {
    id: chatGuid,
    phone: sendingLine,
    ...(options.omitSpaceType ? {} : { type: "dm" }),
  };
  return JSON.stringify({
    event: "messages",
    space,
    message: {
      id: messageId,
      direction: "inbound",
      sender: { id: senderId },
      content: { type: "text", text },
    },
  });
}

function spectrumSignature(rawBody: string, timestamp: string): string {
  return `v0=${createHmac("sha256", webhookSecret)
    .update(`v0:${timestamp}:${rawBody}`)
    .digest("hex")}`;
}

function signedWebhookRequest(
  rawBody: string,
  input: {
    readonly deliveryId: string;
    readonly timestamp?: string;
    readonly signature?: string;
  },
): Request {
  const timestamp = input.timestamp ?? String(Math.floor(Date.now() / 1_000));
  return new Request("https://air.example/webhooks/imessage", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-spectrum-signature": input.signature ?? spectrumSignature(rawBody, timestamp),
      "x-spectrum-timestamp": timestamp,
      "x-spectrum-webhook-id": input.deliveryId,
    },
    body: rawBody,
  });
}

function verifiedInboxInput(messageId: string, text: string, receivedAtMs: number): VerifiedInboxInput {
  return {
    projectId,
    messageId,
    webhookId: `delivery-${messageId}`,
    senderHash: hashSenderIdentifier(identitySecret, senderId),
    threadHash: hashThreadIdentifier(identitySecret, chatGuid),
    contentType: "text",
    isGroup: false,
    receivedAtMs,
    envelope: {
      projectId,
      messageId,
      webhookId: `delivery-${messageId}`,
      chatGuid,
      sendingLine,
      senderId,
      direction: "inbound",
      content: { type: "text", text },
      isGroup: false,
      receivedAtMs,
    },
  };
}

function chatThread(adapter: FakeAdapter) {
  return {
    id: adapter.encodeThreadId({ chatGuid, phone: sendingLine }),
    post: async (message: string) => {
      adapter.sent.push({ threadId: "chat-sdk-thread", message });
      return { id: `chat-sdk-outbound-${adapter.sent.length}` };
    },
  };
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

function deferred(): { readonly promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

function installIngressEnvironment(): () => void {
  const names = ["IMESSAGE_WEBHOOK_SECRET", "IMESSAGE_PROJECT_ID", "CRON_SECRET"] as const;
  const previous = names.map((name) => [name, process.env[name]] as const);
  process.env.IMESSAGE_WEBHOOK_SECRET = webhookSecret;
  process.env.IMESSAGE_PROJECT_ID = projectId;
  process.env.CRON_SECRET = cronSecret;
  return () => {
    for (const [name, value] of previous) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  };
}

afterEach(() => {
  __setAirRuntimeForTests(undefined);
});

describe.sequential("iMessage durable ingress integration", () => {
  it("commits an encrypted verified delivery before scheduling and acknowledging it", async () => {
    const restoreEnvironment = installIngressEnvironment();
    const redis = new RecordingRedis();
    const runtime = runtimeWith(redis);
    const adapter = new FakeAdapter();
    const scheduled: Promise<unknown>[] = [];
    const receiveGate = deferred();
    const question = "How should an A&R team sequence a label rollout?";
    const messageId = "durable-before-ack";
    __setAirRuntimeForTests(runtime);

    try {
      const response = await handleIMessageWebhook(
        signedWebhookRequest(signedPayload(messageId, question), { deliveryId: "delivery-before-ack" }),
        {
          adapter,
          receive: async () => {
            redis.order.push("receive");
            await receiveGate.promise;
            return { id: "durable-before-ack-session" };
          },
          waitUntil: (promise) => {
            redis.order.push("schedule");
            scheduled.push(promise);
          },
        },
      );
      redis.order.push("ack");

      const recordKey = inboxRecordKey(projectId, messageId);
      const rawRecord = await redis.get(recordKey);
      if (!rawRecord) throw new Error("expected durable encrypted inbox record");

      expect(response.status).toBe(200);
      expect(redis.inboxPublicationCalls).toBe(1);
      expect(redis.order.indexOf("persist")).toBeGreaterThanOrEqual(0);
      expect(redis.order.indexOf("schedule")).toBeGreaterThan(redis.order.indexOf("persist"));
      expect(redis.order.indexOf("ack")).toBeGreaterThan(redis.order.indexOf("schedule"));
      expect(scheduled).toHaveLength(1);
      expect(rawRecord).not.toContain(question);
      expect(rawRecord).not.toContain(senderId);
      expect((await runtime.inbox.getRecord(recordKey))?.immutable.messageId).toBe(messageId);
    } finally {
      receiveGate.resolve();
      await Promise.allSettled(scheduled);
      restoreEnvironment();
    }
  });

  it("accepts a signed direct message when Spectrum omits optional space.type", async () => {
    const restoreEnvironment = installIngressEnvironment();
    const redis = new RecordingRedis();
    const runtime = runtimeWith(redis);
    const adapter = new FakeAdapter();
    const scheduled: Promise<unknown>[] = [];
    const handoffs: AirIMessageTurnInput[] = [];
    __setAirRuntimeForTests(runtime);

    try {
      const response = await handleIMessageWebhook(
        signedWebhookRequest(
          signedPayload("missing-space-type", "How should a release roll out?", { omitSpaceType: true }),
          { deliveryId: "delivery-missing-space-type" },
        ),
        {
          adapter,
          receive: async (input) => {
            handoffs.push(input);
            return { id: "missing-space-type-session" };
          },
          waitUntil: (promise) => scheduled.push(promise),
        },
      );
      await Promise.all(scheduled);

      expect(response.status).toBe(200);
      expect(handoffs).toHaveLength(1);
      expect((await runtime.inbox.getRecord(inboxRecordKey(projectId, "missing-space-type")))?.immutable.isGroup)
        .toBe(false);
    } finally {
      await Promise.allSettled(scheduled);
      restoreEnvironment();
    }
  });

  it("turns concurrent duplicate deliveries into one handoff and one outbound response", async () => {
    const restoreEnvironment = installIngressEnvironment();
    const redis = new RecordingRedis();
    const runtime = runtimeWith(redis);
    const adapter = new FakeAdapter();
    const scheduled: Promise<unknown>[] = [];
    const handoffs: AirIMessageTurnInput[] = [];
    const messageId = "concurrent-duplicate";
    const question = "What makes a music launch plan credible?";
    __setAirRuntimeForTests(runtime);

    try {
      const payload = signedPayload(messageId, question);
      const options = {
        adapter,
        receive: async (input: AirIMessageTurnInput) => {
          handoffs.push(input);
          return { id: "concurrent-duplicate-session" };
        },
        waitUntil: (promise: Promise<unknown>) => scheduled.push(promise),
      };
      const [first, duplicate] = await Promise.all([
        handleIMessageWebhook(
          signedWebhookRequest(payload, { deliveryId: "concurrent-delivery-a" }),
          options,
        ),
        handleIMessageWebhook(
          signedWebhookRequest(payload, { deliveryId: "concurrent-delivery-b" }),
          options,
        ),
      ]);
      await Promise.all(scheduled);

      expect(first.status).toBe(200);
      expect(duplicate.status).toBe(200);
      expect(scheduled).toHaveLength(1);
      expect(handoffs).toHaveLength(1);
      expect(handoffs[0]?.message).toBe(question);
      const deliveryTargetId = handoffs[0]?.target.deliveryTargetId;
      expect(deliveryTargetId).toMatch(/^[a-f0-9]{64}$/);
      if (!deliveryTargetId) throw new Error("expected an opaque delivery target");
      // The durable worker has already encrypted the physical Chat SDK thread
      // in its delivery target. Cross-channel Eve hand-off contains only this
      // opaque correlation id.
      expect(JSON.stringify(handoffs[0]?.target)).not.toContain("chat-sdk-thread");

      const recordKey = inboxRecordKey(projectId, messageId);
      expect((await runtime.inbox.getRecord(recordKey))?.delivery.deliveryCount).toBe(2);

      const thread = {
        id: deliveryTargetId,
        post: async (message: string) => {
          adapter.sent.push({ threadId: "chat-sdk-thread", message });
        },
      };
      const eventChannel = { thread, state: { deliveryTargetId } };
      await handleIMessageTurnStarted({
        event: { turnId: "concurrent-duplicate-turn" },
        eventChannel,
        sessionId: "concurrent-duplicate-session",
      });
      await handleIMessageActionResult({
        event: completedToolResult(
          "concurrent-duplicate-turn",
          "A credible plan maps the release sequence to evidence. [S1]",
        ),
        eventChannel,
        sessionId: "concurrent-duplicate-session",
      });
      await handleIMessageMessageCompleted({
        event: { finishReason: "stop", turnId: "concurrent-duplicate-turn" },
        eventChannel,
        sessionId: "concurrent-duplicate-session",
      });

      expect(adapter.sent).toEqual([
        {
          threadId: "chat-sdk-thread",
          message: "A credible plan maps the release sequence to evidence. [S1]",
        },
      ]);
      expect((await runtime.inbox.getRecord(recordKey))?.state).toBe("sent");
    } finally {
      await Promise.allSettled(scheduled);
      restoreEnvironment();
    }
  });

  it("rejects invalid, stale, and malformed requests without publishing durable work", async () => {
    const restoreEnvironment = installIngressEnvironment();
    const redis = new RecordingRedis();
    const runtime = runtimeWith(redis);
    const adapter = new FakeAdapter();
    const scheduled: Promise<unknown>[] = [];
    const handoffs: AirIMessageTurnInput[] = [];
    __setAirRuntimeForTests(runtime);

    try {
      const validJson = signedPayload("rejected-message", "Should never reach the durable inbox.");
      const staleTimestamp = String(Math.floor(Date.now() / 1_000) - 301);
      const responses = await Promise.all([
        handleIMessageWebhook(
          signedWebhookRequest(validJson, {
            deliveryId: "invalid-signature",
            signature: "v0=definitely-not-a-valid-signature",
          }),
          {
            adapter,
            receive: async (input) => {
              handoffs.push(input);
              return { id: "unexpected-invalid-session" };
            },
            waitUntil: (promise) => scheduled.push(promise),
          },
        ),
        handleIMessageWebhook(
          signedWebhookRequest(validJson, {
            deliveryId: "stale-timestamp",
            timestamp: staleTimestamp,
          }),
          {
            adapter,
            receive: async (input) => {
              handoffs.push(input);
              return { id: "unexpected-stale-session" };
            },
            waitUntil: (promise) => scheduled.push(promise),
          },
        ),
        handleIMessageWebhook(
          signedWebhookRequest('{"event":', { deliveryId: "malformed-json" }),
          {
            adapter,
            receive: async (input) => {
              handoffs.push(input);
              return { id: "unexpected-malformed-session" };
            },
            waitUntil: (promise) => scheduled.push(promise),
          },
        ),
      ]);

      expect(responses.map((response) => response.status)).toEqual([401, 400, 400]);
      expect(redis.inboxPublicationCalls).toBe(0);
      expect(await runtime.inbox.listDue()).toEqual([]);
      expect(scheduled).toEqual([]);
      expect(handoffs).toEqual([]);
    } finally {
      await Promise.allSettled(scheduled);
      restoreEnvironment();
    }
  });

  it("keeps operator drain authenticated and recovers a retryable pre-session handoff", async () => {
    const restoreEnvironment = installIngressEnvironment();
    let now = 1_000;
    const redis = new RecordingRedis(() => now);
    const runtime = runtimeWith(redis, () => now);
    const adapter = new FakeAdapter();
    const messageId = "operator-recovery";
    __setAirRuntimeForTests(runtime);

    try {
      const persisted = await runtime.inbox.persistVerified(
        verifiedInboxInput(messageId, "What should recover after an interrupted handoff?", now),
      );
      let failedReceiveCalls = 0;
      await drainDurableInbox({
        adapter,
        receive: async () => {
          failedReceiveCalls += 1;
          throw new Error("simulated Eve handoff interruption");
        },
      });
      const retryable = await runtime.inbox.getRecord(persisted.record.immutable.recordKey);
      if (!retryable?.status.nextAttemptAtMs) throw new Error("expected a retryable inbox record");
      expect(retryable.state).toBe("failed_retryable");

      const unauthorized = await drainFromOperatorRoute(
        new Request("https://air.example/internal/drain-inbox", { method: "POST" }),
        {
          adapter,
          receive: async () => {
            throw new Error("unauthorized drain must not run");
          },
        },
      );
      expect(unauthorized.status).toBe(401);
      expect(failedReceiveCalls).toBe(1);
      expect((await runtime.inbox.getRecord(persisted.record.immutable.recordKey))?.state)
        .toBe("failed_retryable");

      now = retryable.status.nextAttemptAtMs;
      const recovered: AirIMessageTurnInput[] = [];
      const authorized = await drainFromOperatorRoute(
        new Request("https://air.example/internal/drain-inbox", {
          method: "POST",
          headers: { authorization: `Bearer ${cronSecret}` },
        }),
        {
          adapter,
          receive: async (input) => {
            recovered.push(input);
            return { id: "operator-recovery-session" };
          },
        },
      );

      expect(authorized.status).toBe(204);
      expect(recovered).toHaveLength(1);
      expect(recovered[0]?.message).toBe("What should recover after an interrupted handoff?");
      expect((await runtime.inbox.getRecord(persisted.record.immutable.recordKey))?.state)
        .toBe("session_started");
    } finally {
      restoreEnvironment();
    }
  });
});
