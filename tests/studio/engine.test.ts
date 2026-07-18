import { describe, expect, it, vi } from "vitest";

import { createQueueCipher } from "../../lib/crypto.js";
import { MemoryRedis } from "../../lib/redis.js";
import { hashSenderIdentifier, hashThreadIdentifier } from "../../lib/security.js";
import type { StudioEnvironment, StudioReleasePhase } from "../../lib/studio/config.js";
import { drainStudioOutbox, drainStudioWithStats } from "../../lib/studio/engine.js";
import type { IMessageThreadReference, StudioIMessageTransport, TransportOutcome } from "../../lib/studio/imessage-transport.js";
import { getStudioWorkflow, isSinglePaidStepWorkflow } from "../../lib/studio/workflows.js";
import type { StudioRuntime } from "../../lib/studio/runtime.js";
import {
  StudioStore,
  type StudioAttachmentReference,
  type StudioInboundEnvelope,
  type StudioOutboxRecord,
} from "../../lib/studio/store.js";

const IDENTITY_SECRET = "studio-test-identity-secret";
const SENDER_IDENTIFIER = "+14155550123";
const CHAT_GUID = "iMessage;-;+14155550999";
const SENDING_LINE = "+14155550999";

function createDarkFixture(options: {
  readonly acceptNewJobs?: boolean;
  readonly allowProviderSubmit?: boolean;
  readonly deliveryEnabled?: boolean;
  readonly releasePhase?: StudioReleasePhase;
  readonly transportVerified?: boolean;
} = {}) {
  const now = 1_784_990_400_000;
  const cipher = createQueueCipher(Buffer.alloc(32, 23).toString("base64"));
  const senderHash = hashSenderIdentifier(IDENTITY_SECRET, SENDER_IDENTIFIER);
  const threadHash = hashThreadIdentifier(IDENTITY_SECRET, CHAT_GUID);
  const store = new StudioStore({
    redis: new MemoryRedis(() => now),
    cipher,
    identityHashSecret: IDENTITY_SECRET,
    clock: () => now,
  });
  const transport = {
    verified: options.transportVerified ?? false,
    showTyping: vi.fn(async (_thread: IMessageThreadReference): Promise<void> => undefined),
    stopTyping: vi.fn(async (_thread: IMessageThreadReference): Promise<void> => undefined),
    sendText: vi.fn(async (
      _thread: IMessageThreadReference,
      _text: string,
      _idempotencyKey: string,
    ): Promise<TransportOutcome> => ({ kind: "sent" })),
    sendAttachment: vi.fn(async (
      _thread: IMessageThreadReference,
      _attachment: StudioAttachmentReference,
      _idempotencyKey: string,
    ): Promise<TransportOutcome> => ({ kind: "sent" })),
    addLikeTapback: vi.fn(async (
      _thread: IMessageThreadReference,
      _messageId: string,
      _idempotencyKey: string,
    ): Promise<TransportOutcome> => ({ kind: "sent" })),
    fetchAttachment: vi.fn(async (
      _thread: IMessageThreadReference,
      _attachmentId: string,
    ): Promise<Uint8Array> => new Uint8Array()),
  } satisfies StudioIMessageTransport;
  const environment: StudioEnvironment = {
    deploymentMode: "preview",
    releasePhase: options.releasePhase ?? "dark",
    storageTransport: "rest",
    imessageProjectId: "test-project",
    imessageProjectSecret: "test-project-secret",
    imessageWebhookSecret: "test-webhook-secret",
    upstashRedisRestUrl: "https://redis.example.test",
    upstashRedisRestToken: "test-redis-token",
    idHashSecret: IDENTITY_SECRET,
    queueEncryptionKey: Buffer.alloc(32, 23).toString("base64"),
    allowedSenderHashes: [senderHash],
    cronSecret: "test-cron-secret",
    publicBaseUrl: "https://air.example.test",
    modelRegistryVersion: "2.1.0",
    dailyUserJobLimit: 3,
    activeJobLimit: 1,
    policy: {
      acceptNewJobs: options.acceptNewJobs ?? false,
      allowProviderSubmit: options.allowProviderSubmit ?? false,
      allowProviderRecovery: false,
      deliveryEnabled: options.deliveryEnabled ?? false,
    },
    policyExplicit: true,
    spendCaps: {
      perJobCents: 0,
      dailyCents: 0,
      canaryTotalCents: 0,
      canaryFalCents: 0,
    },
  };
  const runtime: StudioRuntime = {
    environment,
    cipher,
    store,
    transport,
    storageTransport: "rest",
    readiness: (_expectedRegistryVersion: string): readonly string[] => [],
    close: async (): Promise<void> => undefined,
  };

  return { runtime, store, transport, senderHash, threadHash };
}

async function persistInbound(
  fixture: ReturnType<typeof createDarkFixture>,
  messageId: string,
  text: string | null,
  attachments: readonly StudioAttachmentReference[] = [],
): Promise<string> {
  const receiptId = fixture.store.receiptId("test-project", messageId);
  const envelope: StudioInboundEnvelope = {
    chatGuid: CHAT_GUID,
    sendingLine: SENDING_LINE,
    senderIdentifier: SENDER_IDENTIFIER,
    messageId,
    text,
    attachments,
    replyTargetId: `reply-${messageId}`,
  };
  const persisted = await fixture.store.persistInbox({
    receiptId,
    senderHash: fixture.senderHash,
    threadHash: fixture.threadHash,
    envelope,
  });
  expect(persisted.created).toBe(true);
  return receiptId;
}

async function durableOutboxRecords(
  fixture: ReturnType<typeof createDarkFixture>,
  outboxIds: readonly string[],
): Promise<readonly StudioOutboxRecord[]> {
  const records = await Promise.all(outboxIds.map((outboxId) => fixture.store.getOutbox(outboxId)));
  if (records.some((record) => record === null)) throw new Error("expected durable outbox intent");
  return records as StudioOutboxRecord[];
}

describe("Air studio dark engine", () => {
  it("drains an allowlisted persisted creation into maintenance_rejected with onboarding and a durable response", async () => {
    const fixture = createDarkFixture();
    const createOutbox = vi.spyOn(fixture.store, "createOutbox");
    const admitJob = vi.spyOn(fixture.store, "admitJob");
    const receiptId = await persistInbound(fixture, "creation-001", "/imagine a moonlit glass terrarium");

    expect(fixture.runtime.environment.allowedSenderHashes).toEqual([fixture.senderHash]);
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });

    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({
      receiptId,
      state: "maintenance_rejected",
    });
    expect(admitJob).not.toHaveBeenCalled();
    expect(createOutbox).toHaveBeenCalledTimes(3);

    const outboxIds = createOutbox.mock.calls.map(([input]) => input.outboxId);
    const records = await durableOutboxRecords(fixture, outboxIds);
    expect(records.every((record) => record.state === "intent" && record.attempts === 0)).toBe(true);

    const texts = records.map((record) => fixture.store.decryptOutboxPayload(record).text);
    expect(texts).toEqual(expect.arrayContaining([
      expect.stringContaining("I’m Air by WZRD"),
      expect.stringContaining("Create with /imagine"),
      expect.stringContaining("maintenance mode"),
    ]));
    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
    expect(fixture.transport.sendAttachment).not.toHaveBeenCalled();
  });

  it("does not claim or invoke a verified transport while delivery is disabled", async () => {
    const fixture = createDarkFixture({ transportVerified: true });
    const outboxId = "delivery-disabled-intent";
    await fixture.store.createOutbox({
      outboxId,
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "text",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
        text: "This must stay durable until the delivery gate is explicitly enabled.",
      },
    });

    await expect(drainStudioOutbox(fixture.runtime)).resolves.toBe(0);
    await expect(fixture.store.getOutbox(outboxId)).resolves.toMatchObject({
      state: "intent",
      attempts: 0,
    });
    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.stopTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
    expect(fixture.transport.sendAttachment).not.toHaveBeenCalled();
    expect(fixture.transport.addLikeTapback).not.toHaveBeenCalled();
  });

  it("does not claim or invoke a verified transport in the dark phase even when delivery is enabled", async () => {
    const fixture = createDarkFixture({ transportVerified: true, deliveryEnabled: true });
    const outboxId = "dark-phase-delivery-intent";
    await fixture.store.createOutbox({
      outboxId,
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "text",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
        text: "The release phase must be an independent delivery gate.",
      },
    });

    await expect(drainStudioOutbox(fixture.runtime)).resolves.toBe(0);
    await expect(fixture.store.getOutbox(outboxId)).resolves.toMatchObject({ state: "intent", attempts: 0 });
    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
  });

  it("defines every launch workflow as exactly one provider-generation paid step", () => {
    for (const mode of ["imagine", "animate", "edit-image", "edit-video"] as const) {
      const workflow = getStudioWorkflow(mode);
      expect(isSinglePaidStepWorkflow(workflow)).toBe(true);
      expect(workflow.workflowId).toBe(`air:${mode}:v1`);
      expect(workflow.paidSteps).toEqual([{ stepId: "provider-generation", kind: "provider-generation" }]);
    }
  });

  it("never admits a creation in the dark phase even when admission and provider switches are true", async () => {
    // The release phase remains dark. Both switches are intentionally true to
    // prove that phase is an independent gate.
    const fixture = createDarkFixture({ acceptNewJobs: true, allowProviderSubmit: true });
    const admitJob = vi.spyOn(fixture.store, "admitJob");
    const creationReceipt = await persistInbound(fixture, "creation-002", "/imagine a cobalt paper lantern");
    const bareTextReceipt = await persistInbound(fixture, "bare-text-001", "make it feel more cinematic");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 2, outbox: 0 });

    await expect(fixture.store.readInbox(creationReceipt)).resolves.toMatchObject({ state: "maintenance_rejected" });
    await expect(fixture.store.readInbox(bareTextReceipt)).resolves.toMatchObject({ state: "processed" });
    expect(admitJob).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
    expect(fixture.transport.sendAttachment).not.toHaveBeenCalled();
  });
});
