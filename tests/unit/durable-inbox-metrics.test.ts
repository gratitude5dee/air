import { afterEach, describe, expect, it, vi } from "vitest";
import { createQueueCipher } from "../../lib/crypto.js";
import {
  DurableInbox,
  type VerifiedInboxInput,
} from "../../lib/durable-inbox.js";
import {
  __setAirRuntimeForTests,
  createAirRuntime,
  drainDurableInbox,
  type AirIMessageAdapter,
} from "../../lib/imessage-channel-runtime.js";
import { loadAirEnvironment } from "../../lib/env.js";
import { redactLogFields } from "../../lib/logging.js";
import { MemoryRedis } from "../../lib/redis.js";
import { hashSenderIdentifier, hashThreadIdentifier } from "../../lib/security.js";

const identitySecret = "queue-metrics-test-identity";
const queueKey = Buffer.alloc(32, 23).toString("base64");

function input(options: {
  readonly messageId: string;
  readonly receivedAtMs: number;
  readonly chatGuid?: string;
}): VerifiedInboxInput {
  const senderId = "+14155551234";
  const chatGuid = options.chatGuid ?? "iMessage;-;+14155551234";
  return {
    projectId: "project-test",
    messageId: options.messageId,
    webhookId: `delivery-${options.messageId}`,
    senderHash: hashSenderIdentifier(identitySecret, senderId),
    threadHash: hashThreadIdentifier(identitySecret, chatGuid),
    contentType: "text",
    isGroup: false,
    receivedAtMs: options.receivedAtMs,
    envelope: {
      projectId: "project-test",
      messageId: options.messageId,
      webhookId: `delivery-${options.messageId}`,
      chatGuid,
      sendingLine: "+15555550123",
      senderId,
      direction: "inbound",
      content: { type: "text", text: "private queue fixture" },
      isGroup: false,
      receivedAtMs: options.receivedAtMs,
    },
  };
}

function createInbox(redis: MemoryRedis, clock: () => number): DurableInbox {
  return new DurableInbox({
    redis,
    cipher: createQueueCipher(queueKey),
    identityHashSecret: identitySecret,
    clock,
  });
}

function environment() {
  return loadAirEnvironment({
    NODE_ENV: "development",
    IMESSAGE_PROJECT_ID: "project-test",
    IMESSAGE_WEBHOOK_SECRET: "queue-metrics-webhook-secret",
    REDIS_URL: "redis://fixture.invalid:6379",
    AIR_ID_HASH_SECRET: identitySecret,
    AIR_QUEUE_ENCRYPTION_KEY: queueKey,
    AIR_ALLOWED_SENDER_HASHES: hashSenderIdentifier(identitySecret, "+14155551234"),
    CRON_SECRET: "queue-metrics-cron-secret",
  });
}

class FixtureAdapter implements AirIMessageAdapter {
  encodeThreadId({ chatGuid, phone }: { readonly chatGuid: string; readonly phone?: string }): string {
    return phone ? `imessage:${chatGuid}~${phone}` : `imessage:${chatGuid}`;
  }

  async postMessage(): Promise<unknown> {
    return { id: "fixture-outbound" };
  }
}

afterEach(() => {
  __setAirRuntimeForTests(undefined);
  vi.restoreAllMocks();
});

describe("durable inbox queue metrics", () => {
  it("keeps the oldest receipt age correct when retry scheduling changes and terminal records are removed", async () => {
    let now = 1_000;
    const redis = new MemoryRedis(() => now);
    const inbox = createInbox(redis, () => now);
    const oldest = await inbox.persistVerified(input({ messageId: "oldest", receivedAtMs: now }));
    now = 1_100;
    const newer = await inbox.persistVerified(input({
      messageId: "newer",
      receivedAtMs: now,
      chatGuid: "iMessage;-;+14155559999",
    }));
    now = 1_200;

    expect(await inbox.getQueueMetrics()).toEqual({ depth: 2, oldestInboxAgeMs: 200 });

    const retry = await inbox.beginProcessing(oldest.record.immutable.recordKey);
    if (!retry) throw new Error("expected oldest record processing lease");
    await retry.transition("failed_retryable", {
      failureCategory: "fixture_retry",
      nextAttemptAtMs: now + 60_000,
    });

    // The scheduling index now lists `newer` first. The received-at index
    // must continue to report the true oldest outstanding receipt.
    expect(await inbox.getQueueMetrics()).toEqual({ depth: 2, oldestInboxAgeMs: 200 });

    await inbox.transition(oldest.record.immutable.recordKey, "dead_letter", {
      expectedStates: ["failed_retryable"],
      failureCategory: "fixture_terminal",
    });
    expect(await inbox.getQueueMetrics()).toEqual({ depth: 1, oldestInboxAgeMs: 100 });

    const final = await inbox.beginProcessing(newer.record.immutable.recordKey);
    if (!final) throw new Error("expected newer record processing lease");
    await final.transition("dead_letter", { failureCategory: "fixture_terminal" });
    expect(await inbox.getQueueMetrics()).toEqual({ depth: 0, oldestInboxAgeMs: 0 });
  });

  it("uses a bounded legacy fallback when a pre-index queue has no received-at members", async () => {
    let now = 2_000;
    const redis = new MemoryRedis(() => now);
    const inbox = createInbox(redis, () => now);
    const oldest = await inbox.persistVerified(input({ messageId: "legacy-oldest", receivedAtMs: now }));
    now = 2_100;
    const newer = await inbox.persistVerified(input({
      messageId: "legacy-newer",
      receivedAtMs: now,
      chatGuid: "iMessage;-;+14155559999",
    }));
    now = 2_400;

    // Simulate records written before the received-at index existed. The
    // fallback sees only private record keys and returns no identifiers.
    await redis.zRem("air:inbox:v1:pending:received-at", oldest.record.immutable.recordKey);
    await redis.zRem("air:inbox:v1:pending:received-at", newer.record.immutable.recordKey);

    expect(await inbox.getQueueMetrics()).toEqual({ depth: 2, oldestInboxAgeMs: 400 });
  });

  it("emits the exact queue metric from the durable drain and keeps millisecond measurements redacted-safe", async () => {
    let now = 3_000;
    const redis = new MemoryRedis(() => now);
    const runtime = createAirRuntime({ environment: environment(), redis, clock: () => now });
    __setAirRuntimeForTests(runtime);
    await runtime.inbox.persistVerified(input({ messageId: "drain-oldest", receivedAtMs: 2_500 }));
    await runtime.inbox.persistVerified(input({ messageId: "drain-newer", receivedAtMs: 2_900 }));

    const output = vi.spyOn(console, "log").mockImplementation(() => undefined);
    await drainDurableInbox({
      adapter: new FixtureAdapter(),
      receive: async () => ({ id: "queue-metrics-session" }),
    });

    const scan = output.mock.calls
      .map(([line]) => JSON.parse(String(line)) as { event?: string; fields?: Record<string, unknown> })
      .find((event) => event.event === "imessage.inbox_scan");
    expect(scan?.fields).toMatchObject({
      dueCount: 1,
      queueDepth: 2,
      oldestInboxAgeMs: 500,
    });
    expect(redactLogFields({ oldestInboxAgeMs: 500 })).toEqual({ oldestInboxAgeMs: 500 });
  });
});
