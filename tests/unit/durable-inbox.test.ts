import { describe, expect, it } from "vitest";
import { createQueueCipher } from "../../lib/crypto.js";
import { DurableInbox } from "../../lib/durable-inbox.js";
import { MemoryRedis } from "../../lib/redis.js";
import { deriveCorrelationId, hashSenderIdentifier, hashThreadIdentifier } from "../../lib/security.js";

const identitySecret = "durable-inbox-test-identity";
const queueKey = Buffer.alloc(32, 23).toString("base64");

describe("durable inbox delivery recovery", () => {
  it("marks an expired static-message send as unknown rather than replaying it", async () => {
    let now = 1_700_000_000_000;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
      threadLeaseTtlMs: 1_000,
    });
    const projectId = "photon-project";
    const messageId = "photon-message";
    const senderId = "+14155550100";
    const chatGuid = "iMessage;-;+14155550100";
    const persisted = await inbox.persistVerified({
      projectId,
      messageId,
      senderHash: hashSenderIdentifier(identitySecret, senderId),
      threadHash: hashThreadIdentifier(identitySecret, `${chatGuid}\u0000+14155550000`),
      contentType: "text",
      isGroup: false,
      envelope: {
        projectId,
        messageId,
        chatGuid,
        sendingLine: "+14155550000",
        senderId,
        direction: "inbound",
        content: { type: "text", text: "hello" },
        isGroup: false,
        receivedAtMs: now,
      },
    });
    const lease = await inbox.beginProcessing(persisted.record.immutable.recordKey);
    if (!lease) throw new Error("expected inbox lease");
    await lease.transition("response_ready");
    await lease.transition("sending");
    await lease.release();

    now += 1_001;
    expect(await inbox.listDue()).toEqual([]);
    expect(await inbox.getRecord(persisted.record.immutable.recordKey)).toMatchObject({
      state: "send_unknown",
      status: { failureCategory: "photon_static_send_unknown" },
    });
  });

  it("keeps provider identifiers out of durable Redis metadata and keys", async () => {
    const redis = new MemoryRedis();
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
    });
    const projectId = "private-provider-project";
    const messageId = "provider-message-guid";
    const senderId = "+14155550101";
    const chatGuid = "iMessage;-;+14155550101";
    const correlationId = deriveCorrelationId(identitySecret, projectId, messageId);
    const persisted = await inbox.persistVerified({
      projectId,
      messageId,
      senderHash: hashSenderIdentifier(identitySecret, senderId),
      threadHash: hashThreadIdentifier(identitySecret, `${chatGuid}\u0000+14155550000`),
      contentType: "text",
      isGroup: false,
      envelope: {
        projectId,
        messageId,
        chatGuid,
        sendingLine: "+14155550000",
        senderId,
        direction: "inbound",
        content: { type: "text", text: "private prompt" },
        isGroup: false,
        receivedAtMs: Date.now(),
      },
    });

    expect(persisted.record.immutable.recordKey).toBe(`air:inbox:v1:record:${correlationId}`);
    const metadata = await redis.get(`${persisted.record.immutable.recordKey}:metadata`);
    expect(metadata).not.toContain(projectId);
    expect(metadata).not.toContain(messageId);
    expect(metadata).not.toContain(senderId);
    expect(metadata).not.toContain(chatGuid);
    expect(metadata).not.toContain("private prompt");
  });
});
