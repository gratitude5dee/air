import { describe, expect, it } from "vitest";
import { createQueueCipher } from "../../lib/crypto.js";
import { MemoryRedis } from "../../lib/redis.js";
import { deriveCorrelationId } from "../../lib/security.js";
import {
  VideoJobStore,
  VideoJobStoreError,
  videoJobActiveSenderKey,
  videoJobManualReviewIndexKey,
  videoJobRecordKey,
} from "../../lib/video-job-store.js";

const identitySecret = "video-job-store-test-secret";
const queueKey = Buffer.alloc(32, 41).toString("base64");
const deliveryTarget = Object.freeze({ chatGuid: "iMessage;-;+14155551234", phone: "+14155550000" });

function correlation(id = "message-1"): string {
  return deriveCorrelationId(identitySecret, "photon-project", id);
}

function sender(id = "sender-1"): string {
  return deriveCorrelationId(identitySecret, "video-sender", id);
}

function setup(options: { readonly payloadRetentionMs?: number; readonly metadataRetentionMs?: number } = {}) {
  let now = 1_700_000_000_000;
  const redis = new MemoryRedis(() => now);
  const store = new VideoJobStore({
    redis,
    cipher: createQueueCipher(queueKey),
    identityHashSecret: identitySecret,
    clock: () => now,
    workLeaseTtlMs: 1_000,
    ...options,
  });
  return {
    redis,
    store,
    now: () => now,
    advance: (milliseconds: number) => { now += milliseconds; },
  };
}

describe("durable video job store", () => {
  it("creates one encrypted job per inbound event and rejects a divergent duplicate", async () => {
    const { redis, store } = setup();
    const id = correlation();
    const senderHash = sender();
    const firstFrameIdentity = deriveCorrelationId(identitySecret, "video-frame", "attachment-1");
    const first = await store.createOrGet({
      correlationId: id,
      senderHash,
      delivery: deliveryTarget,
      prompt: "A tiny orange robot watering desert flowers",
      firstFrameUrl: "https://storage.googleapis.com/air/frame.png",
      firstFrameIdentity,
    });
    const second = await store.createOrGet({
      correlationId: id,
      senderHash,
      delivery: deliveryTarget,
      prompt: "A tiny orange robot watering desert flowers",
      // A retry can receive a fresh GMI URL; the attachment HMAC remains
      // stable, so it returns the original encrypted job rather than conflict.
      firstFrameUrl: "https://storage.googleapis.com/air/frame-retry.png",
      firstFrameIdentity,
    });

    expect(first).toMatchObject({ created: true, record: { state: "validated" } });
    expect(second).toMatchObject({ created: false, record: { state: "validated" } });
    expect(await store.getPayload(first.record.immutable.recordKey)).toMatchObject({
      prompt: "A tiny orange robot watering desert flowers",
      firstFrameUrl: "https://storage.googleapis.com/air/frame.png",
    });

    const metadata = await redis.get(`${first.record.immutable.recordKey}:metadata`);
    const payload = await redis.get(`${first.record.immutable.recordKey}:payload`);
    expect(metadata).not.toContain("orange robot");
    expect(metadata).not.toContain("storage.googleapis.com");
    expect(payload).not.toContain("orange robot");
    expect(payload).not.toContain("storage.googleapis.com");
    expect(await redis.get(videoJobActiveSenderKey(senderHash))).toBe(first.record.immutable.recordKey);
    await expect(store.createOrGet({
      correlationId: id,
      senderHash,
      delivery: deliveryTarget,
      prompt: "A different paid request",
    })).rejects.toMatchObject({ code: "conflict" });

    await expect(store.createOrGet({
      correlationId: correlation("missing-frame-identity"),
      senderHash: sender("missing-frame-identity"),
      delivery: deliveryTarget,
      prompt: "A valid prompt",
      firstFrameUrl: "https://storage.googleapis.com/air/frame.png",
    })).rejects.toMatchObject({ code: "invalid_input" });
  });

  it("persists encrypted image recovery checkpoints before a Zap submission", async () => {
    const { redis, store } = setup();
    const firstFrameIdentity = deriveCorrelationId(identitySecret, "video-frame", "recovery-attachment");
    const source = {
      messageId: "photon-message-recovery",
      attachment: {
        id: "photon-guid-recovery",
        name: "first-frame.jpg",
        mimeType: "image/jpeg",
        size: 123,
      },
    } as const;
    const created = await store.createOrGet({
      correlationId: correlation("image-recovery"),
      senderHash: sender("image-recovery"),
      delivery: deliveryTarget,
      prompt: "A bright kite over a canyon",
      firstFrameInput: source,
      firstFrameIdentity,
    });
    const recordKey = created.record.immutable.recordKey;

    expect(created.record.state).toBe("validated");
    const metadata = await redis.get(`${recordKey}:metadata`);
    const ciphertext = await redis.get(`${recordKey}:payload`);
    expect(metadata).not.toContain(source.messageId);
    expect(metadata).not.toContain(source.attachment.id);
    expect(ciphertext).not.toContain(source.messageId);
    expect(ciphertext).not.toContain(source.attachment.id);

    const interruptedUpload = await store.beginWork(recordKey);
    if (!interruptedUpload) throw new Error("expected image-upload work");
    expect(interruptedUpload.operation).toBe("upload");
    expect(interruptedUpload.record.state).toBe("image_uploading");
    await interruptedUpload.release();

    // Simulate a cold-start recovery after the worker acquired its upload
    // intent but before it could contact Zap/GMI.
    const recoveredUpload = await store.beginWork(recordKey);
    if (!recoveredUpload) throw new Error("expected recovered image-upload work");
    expect(recoveredUpload.operation).toBe("upload");
    await recoveredUpload.transition({
      expectedStates: ["image_uploading"],
      state: "zap_submitting",
      firstFrameUrl: "https://storage.googleapis.com/air/frame.jpg",
      clearFirstFrameInput: true,
    });
    const uploadedPayload = await store.getPayload(recordKey);
    expect(uploadedPayload).toMatchObject({ firstFrameUrl: "https://storage.googleapis.com/air/frame.jpg" });
    expect(uploadedPayload).not.toHaveProperty("firstFrameInput");
    await recoveredUpload.release();

    // A crash after image upload but before provider handoff resumes from the
    // named `zap_submitting` checkpoint and writes `submission_unknown` first.
    const submission = await store.beginWork(recordKey);
    if (!submission) throw new Error("expected recovered submission work");
    expect(submission.operation).toBe("submit");
    expect(submission.record.state).toBe("submission_unknown");
    await submission.release();
  });

  it("persists an unknown submission intent before Zap and reconciles it without automatic resubmission", async () => {
    const { redis, store } = setup();
    const created = await store.createOrGet({
      correlationId: correlation("ambiguous"),
      senderHash: sender("ambiguous"),
      delivery: deliveryTarget,
      prompt: "A city of glass at dawn",
    });
    const attempt = await store.beginWork(created.record.immutable.recordKey);
    if (!attempt) throw new Error("expected work");

    expect(attempt.operation).toBe("submit");
    expect(attempt.idempotencyKey).toBe(created.record.immutable.inboundEventHash);
    expect(attempt.record).toMatchObject({
      state: "submission_unknown",
      status: { failureCategory: "submission_intent" },
    });
    await attempt.release();

    expect(await store.listDue()).toEqual([]);
    expect(await store.beginWork(created.record.immutable.recordKey)).toBeNull();

    const reconciled = await store.transition(created.record.immutable.recordKey, {
      expectedStates: ["submission_unknown"],
      state: "queued",
      zapRunId: "run_123",
      nextAttemptAtMs: 1_700_000_000_100,
    });
    expect(reconciled).toMatchObject({ state: "queued" });
  });

  it("durably advances polling and guarded media delivery while preserving send_unknown separately", async () => {
    const { redis, store, advance } = setup();
    const created = await store.createOrGet({
      correlationId: correlation("flow"),
      senderHash: sender("flow"),
      delivery: deliveryTarget,
      prompt: "A biomechanical hummingbird flying through a cloud forest",
    });
    const submission = await store.beginWork(created.record.immutable.recordKey);
    if (!submission) throw new Error("expected submission work");
    const queued = await submission.transition({
      expectedStates: ["submission_unknown"],
      state: "queued",
      zapRunId: "run_123",
      nextAttemptAtMs: 1_700_000_000_500,
      progress: 0,
    });
    expect(queued).toMatchObject({ state: "queued", status: { zapRunIdHash: expect.stringMatching(/^[a-f0-9]{64}$/) } });
    await submission.release();

    expect(await store.listDue()).toEqual([]);
    advance(500);
    const [due] = await store.listDue();
    expect(due?.state).toBe("queued");

    const poll = await store.beginWork(due?.immutable.recordKey ?? "");
    if (!poll) throw new Error("expected poll work");
    expect(poll.operation).toBe("poll");
    const running = await poll.transition({
      expectedStates: ["queued"],
      state: "running",
      nextAttemptAtMs: 1_700_000_001_000,
      progress: 40,
    });
    expect(running?.state).toBe("running");
    const ready = await poll.transition({
      expectedStates: ["running"],
      state: "video_ready",
      progress: 100,
      video: {
        url: "https://blob.vercel-storage.com/air/generated.mp4",
        mimeType: "video/mp4",
      },
    });
    expect(ready).toMatchObject({ state: "video_ready", status: { videoUrlHash: expect.stringMatching(/^[a-f0-9]{64}$/) } });
    await poll.release();

    const [readyDue] = await store.listDue();
    expect(readyDue?.state).toBe("video_ready");
    const delivery = await store.beginWork(readyDue?.immutable.recordKey ?? "");
    if (!delivery) throw new Error("expected delivery work");
    expect(delivery.operation).toBe("send");
    expect(delivery.record.state).toBe("sending");
    expect(await redis.zRangeByScore(videoJobManualReviewIndexKey(), 0, Number.POSITIVE_INFINITY))
      .toEqual([created.record.immutable.recordKey]);
    const unknown = await delivery.transition({
      expectedStates: ["sending"],
      state: "send_unknown",
      failureCategory: "photon_send_timeout",
    });
    expect(unknown).toMatchObject({ state: "send_unknown", status: { failureCategory: "photon_send_timeout" } });
    await delivery.release();

    expect(await redis.zRangeByScore(videoJobManualReviewIndexKey(), 0, Number.POSITIVE_INFINITY))
      .toEqual([created.record.immutable.recordKey]);

    expect(await store.listDue()).toEqual([]);
    expect(await store.getPayload(created.record.immutable.recordKey)).toMatchObject({
      zapRunId: "run_123",
      video: { url: "https://blob.vercel-storage.com/air/generated.mp4", mimeType: "video/mp4" },
    });
    const delivered = await store.transition(created.record.immutable.recordKey, {
      expectedStates: ["send_unknown"],
      state: "delivered",
    });
    expect(delivered?.state).toBe("delivered");
    expect(await store.getPayload(created.record.immutable.recordKey)).toBeNull();
    expect(await redis.zRangeByScore(videoJobManualReviewIndexKey(), 0, Number.POSITIVE_INFINITY))
      .toEqual([]);
  });

  it("allows one active generation per pseudonymous sender and releases it when video is ready", async () => {
    const { redis, store } = setup();
    const senderHash = sender("active");
    const first = await store.createOrGet({
      correlationId: correlation("active-first"),
      senderHash,
      delivery: deliveryTarget,
      prompt: "A clockwork koi fish",
    });
    await expect(store.createOrGet({
      correlationId: correlation("active-second"),
      senderHash,
      delivery: deliveryTarget,
      prompt: "A luminous jellyfish",
    })).rejects.toEqual(new VideoJobStoreError("sender_busy"));

    const submission = await store.beginWork(first.record.immutable.recordKey);
    if (!submission) throw new Error("expected submission");
    await submission.transition({
      expectedStates: ["submission_unknown"],
      state: "queued",
      zapRunId: "run_active",
    });
    await submission.release();
    const poll = await store.beginWork(first.record.immutable.recordKey);
    if (!poll) throw new Error("expected poll");
    await poll.transition({
      expectedStates: ["queued"],
      state: "video_ready",
      video: { url: "https://blob.vercel-storage.com/air/active.mp4", mimeType: "video/mp4" },
    });
    await poll.release();

    expect(await redis.get(videoJobActiveSenderKey(senderHash))).toBeNull();
    await expect(store.createOrGet({
      correlationId: correlation("active-second"),
      senderHash,
      delivery: deliveryTarget,
      prompt: "A luminous jellyfish",
    })).resolves.toMatchObject({ created: true });
  });

  it("dead-letters a due job whose encrypted payload has expired", async () => {
    const { store, advance } = setup({ payloadRetentionMs: 1_000, metadataRetentionMs: 5_000 });
    const created = await store.createOrGet({
      correlationId: correlation("expiry"),
      senderHash: sender("expiry"),
      delivery: deliveryTarget,
      prompt: "A blue whale above the clouds",
    });
    advance(1_001);

    expect(await store.beginWork(created.record.immutable.recordKey)).toBeNull();
    expect(await store.get(created.record.immutable.recordKey)).toMatchObject({
      state: "dead_letter",
      status: { failureCategory: "payload_expired" },
    });
  });

  it("enforces the retry-attempt ceiling before issuing another external request", async () => {
    const { store } = setup();
    const created = await store.createOrGet({
      correlationId: correlation("attempts"),
      senderHash: sender("attempts"),
      delivery: deliveryTarget,
      prompt: "A fox made of rain",
    });
    for (let attemptNumber = 0; attemptNumber < 100; attemptNumber += 1) {
      const attempt = await store.beginWork(created.record.immutable.recordKey);
      if (!attempt) throw new Error(`missing attempt ${attemptNumber}`);
      expect(attempt.operation).toBe("submit");
      await attempt.transition({
        expectedStates: ["submission_unknown"],
        state: "retryable",
        failureCategory: "zap_preflight_retryable",
      });
      await attempt.release();
    }

    expect(await store.beginWork(created.record.immutable.recordKey)).toBeNull();
    expect(await store.get(created.record.immutable.recordKey)).toMatchObject({
      state: "dead_letter",
      status: { attempts: 100, failureCategory: "attempts_exhausted" },
    });
  });

  it("fails closed on a state change that would poll without a durable Zap run ID", async () => {
    const { store } = setup();
    const created = await store.createOrGet({
      correlationId: correlation("invalid"),
      senderHash: sender("invalid"),
      delivery: deliveryTarget,
      prompt: "Rain over a moonlit lake",
    });
    await expect(store.transition(created.record.immutable.recordKey, {
      expectedStates: ["submission_pending"],
      state: "queued",
    })).rejects.toEqual(new VideoJobStoreError("invalid_transition"));
    expect(videoJobRecordKey(correlation("invalid"))).toBe(created.record.immutable.recordKey);
  });
});
