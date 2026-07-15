import { timingSafeEqual } from "node:crypto";
import { imessage } from "@spectrum-ts/imessage";
import type { AdapterPostableMessage } from "chat";
import heicConvert from "heic-convert";
import {
  type InboxContentType,
  type InboxProcessingLease,
  type InboxRecord,
} from "./durable-inbox.js";
import { getAirRuntime, type AirRuntime } from "./imessage-channel-runtime.js";
import {
  AIR_VIDEO_ACCESS_NOTICE,
  AIR_VIDEO_ACTIVE_NOTICE,
  AIR_VIDEO_DM_ONLY_NOTICE,
  AIR_VIDEO_GENERATING_NOTICE,
  AIR_VIDEO_IMAGE_NOTICE,
  AIR_VIDEO_INVALID_PROMPT_NOTICE,
  AIR_VIDEO_RATE_LIMIT_NOTICE,
} from "./messages.js";
import { createSenderIdentity, deriveOpaqueId, hashThreadIdentifier, isSenderAllowed } from "./security.js";
import {
  decodeWebhookJson,
  normalizeSpectrumWebhook,
  readRawBodyLimited,
  SPECTRUM_SIGNATURE_HEADER,
  SPECTRUM_TIMESTAMP_HEADER,
  SPECTRUM_WEBHOOK_ID_HEADER,
  verifySpectrumSignature,
  WebhookInputError,
  type NormalizedIMessageAttachment,
} from "./spectrum-webhook.js";
import {
  VideoJobStore,
  VideoJobStoreError,
  videoJobActiveSenderKey,
  videoJobRecordKey,
  type VideoJobAttempt,
  type VideoJobDeliveryTarget,
  type VideoJobFirstFrameInput,
  type PersistedVideoJob,
  type VideoJobRecord,
} from "./video-job-store.js";
import { acquireRedisLease } from "./redis.js";
import {
  createZapVideoServiceClient,
  ZapVideoServiceError,
  type ZapVideoRun,
  type ZapVideoServiceClient,
} from "./zap-video-service.js";

/** The video worker never accepts media larger than what Photon can return safely. */
export const MAX_INPUT_IMAGE_BYTES = 10 * 1024 * 1024;
export const MAX_OUTPUT_VIDEO_BYTES = 25 * 1024 * 1024;
export const VIDEO_POLL_INTERVAL_MS = 12_000;
export const VIDEO_RETRY_DELAY_MS = 30_000;
export const VIDEO_WORKER_LIMIT = 16;
/**
 * Serializes admission through the active-job fence. It outlives Air's
 * function budget so a duplicate invocation cannot consume a second slot
 * while the first is still preparing its durable job.
 */
const VIDEO_ADMISSION_LEASE_TTL_MS = 75_000;

type VideoPostable = AdapterPostableMessage;

/** The narrow adapter surface that keeps the job worker independent of Eve. */
export interface AirVideoIMessageAdapter {
  app: unknown | null;
  encodeThreadId(input: { readonly chatGuid: string; readonly phone?: string }): string;
  fetchMessage(threadId: string, messageId: string): Promise<unknown>;
  postMessage(threadId: string, message: VideoPostable): Promise<unknown>;
}

export interface VideoWebhookRuntimeOptions {
  readonly adapter: AirVideoIMessageAdapter;
  readonly waitUntil: (promise: Promise<unknown>) => void;
  /**
   * The public route supplies this fail-closed deployment guard. It runs only
   * after the raw Spectrum signature has been verified, so an unsigned probe
   * cannot be turned into a framework-level configuration exception.
   */
  readonly assertRuntimeConfiguration: () => void;
}

export interface VideoDrainOptions {
  readonly adapter: AirVideoIMessageAdapter;
  readonly limit?: number | undefined;
  /** Internal route reuse avoids reconnecting Redis after authorization. */
  readonly runtime?: AirRuntime | undefined;
  /**
   * Recovery cron uses this to emit a redacted storage signal. It receives no
   * error details so ingress `waitUntil` drains remain privacy-safe and
   * best-effort.
   */
  readonly onStorageUnavailable?: (() => void) | undefined;
}

/** Successful operator authorization deliberately exposes only a live runtime. */
export interface AuthorizedVideoDrain {
  readonly runtime: AirRuntime;
}

interface SpectrumAttachmentReader {
  getAttachment(guid: string, phone?: string): Promise<{
    /** Spectrum opens a cancellable gRPC-backed Web stream for each call. */
    stream(): Promise<ReadableStream<Uint8Array>>;
    size?: number | undefined;
  } | undefined>;
}

interface PreparedImage {
  readonly bytes: Buffer;
  readonly mimeType: "image/jpeg" | "image/png";
}

/** Ephemeral bytes let the first post-ack worker avoid a second Photon read. */
interface PreparedFirstFrameHandoff {
  readonly record: VideoJobRecord;
  readonly image: PreparedImage;
}

class VideoWorkflowError extends Error {
  public constructor(
    readonly category: string,
    readonly retryable: boolean,
    readonly deterministicOutput = false,
  ) {
    super(category);
    this.name = "VideoWorkflowError";
  }
}

/**
 * Verify first, then persist only an encrypted, bounded envelope before the
 * Photon acknowledgement. No provider submission or attachment fetch occurs
 * in the webhook invocation.
 */
export async function handleIMessageVideoWebhook(
  request: Request,
  options: VideoWebhookRuntimeOptions,
): Promise<Response> {
  if (request.method !== "POST") return emptyResponse(405);

  let rawBody: Uint8Array;
  try {
    rawBody = await readRawBodyLimited(request);
  } catch (error) {
    return webhookInputResponse(error);
  }

  const verification = verifySpectrumSignature({
    rawBody,
    secret: process.env.IMESSAGE_WEBHOOK_SECRET,
    signature: request.headers.get(SPECTRUM_SIGNATURE_HEADER),
    timestamp: request.headers.get(SPECTRUM_TIMESTAMP_HEADER),
  });
  if (!verification.ok) {
    // These are a closed set of fixed categories.  They deliberately contain
    // no request headers, payload bytes, identifiers, or signature material,
    // but make a dashboard configuration issue distinguishable from a bad
    // signing secret in serverless production logs.
    console.warn(
      verification.reason === "missing_headers"
        ? "air_imessage_ingress_signature_headers_missing"
        : verification.reason === "stale_timestamp"
          ? "air_imessage_ingress_signature_timestamp_stale"
          : "air_imessage_ingress_signature_invalid",
    );
    return emptyResponse(verification.status);
  }

  try {
    options.assertRuntimeConfiguration();
  } catch {
    // Deployment configuration is unavailable or unsafe. Do not parse,
    // persist, or acknowledge a signed delivery in that state.
    // Emit only a fixed category: the error may carry secret configuration
    // details and this route must never log webhook contents.
    console.error("air_imessage_ingress_configuration_unavailable");
    return emptyResponse(503);
  }

  let runtime: AirRuntime;
  try {
    runtime = await getAirRuntime();
  } catch {
    // Connection and environment errors can contain credentials. Keep the
    // operational signal useful without disclosing the underlying error.
    console.error("air_imessage_ingress_runtime_unavailable");
    return emptyResponse(503);
  }

  try {
    const normalized = normalizeSpectrumWebhook({
      payload: decodeWebhookJson(rawBody),
      projectId: process.env.IMESSAGE_PROJECT_ID ?? "",
      webhookId: request.headers.get(SPECTRUM_WEBHOOK_ID_HEADER),
      receivedAt: Date.now(),
    });
    if (
      !normalized ||
      normalized.direction !== "inbound" ||
      !normalized.senderId ||
      normalized.spaceType === "unknown"
    ) {
      return emptyResponse(204);
    }

    // Reactions and other non-message events are signed deliveries too, but
    // they are never prompts. Do not enqueue them merely to send an invalid
    // prompt response back into the conversation.
    const contentType = inboxContentType(normalized.text, normalized.attachments, normalized.contentType);
    if (contentType !== "text" && contentType !== "attachment") return emptyResponse(204);

    const sender = createSenderIdentity(
      runtime.environment.idHashSecret as string,
      normalized.senderId,
    ).senderHash;
    const persisted = await runtime.inbox.persistVerified({
      projectId: normalized.projectId,
      messageId: normalized.messageId,
      webhookId: normalized.webhookId,
      senderHash: sender,
      // The same chat GUID can be routed through more than one local iMessage
      // line. Keep the ordering fence aligned with the concrete reply route.
      threadHash: hashThreadIdentifier(
        runtime.environment.idHashSecret as string,
        `${normalized.chatGuid}\u0000${normalized.phone ?? ""}`,
      ),
      contentType,
      isGroup: normalized.spaceType === "group",
      receivedAtMs: normalized.receivedAt,
      envelope: {
        projectId: normalized.projectId,
        messageId: normalized.messageId,
        webhookId: normalized.webhookId,
        chatGuid: normalized.chatGuid,
        sendingLine: normalized.phone ?? "",
        senderId: normalized.senderId,
        direction: "inbound",
        content: {
          type: contentType,
          text: normalized.text,
          attachments: normalized.attachments,
        },
        isGroup: normalized.spaceType === "group",
        receivedAtMs: normalized.receivedAt,
      },
    });

    if (persisted.created) {
      options.waitUntil(drainIMessageVideoJobs({ adapter: options.adapter }).catch(() => undefined));
    }
    return emptyResponse(200);
  } catch {
    // A non-2xx result makes Photon retry. The inbox key is idempotent, and
    // no raw provider response, prompt, or handle reaches the log stream.
    console.error("air_imessage_ingress_persistence_unavailable");
    return emptyResponse(503);
  }
}

/** The post-ack hook and minute cron share one encrypted inbox/job worker. */
export async function drainIMessageVideoJobs(options: VideoDrainOptions): Promise<void> {
  let runtime = options.runtime;
  if (!runtime) {
    try {
      runtime = await getAirRuntime();
    } catch {
      notifyStorageUnavailable(options);
      return;
    }
  }

  const limit = options.limit ?? VIDEO_WORKER_LIMIT;
  if (!Number.isInteger(limit) || limit < 1 || limit > 50) return;
  const jobs = createVideoJobStore(runtime);

  let inboxRecords: readonly InboxRecord[];
  try {
    inboxRecords = await runtime.inbox.listDue(limit);
  } catch {
    notifyStorageUnavailable(options);
    return;
  }
  for (const record of inboxRecords) {
    const prepared = await processInboxRecord(runtime, jobs, record, options.adapter).catch(() => undefined);
    if (prepared) {
      await processVideoJob(runtime, jobs, prepared.record, options.adapter, prepared.image).catch(() => undefined);
    }
  }

  let dueJobs: readonly VideoJobRecord[];
  try {
    dueJobs = await jobs.listDue(limit);
  } catch {
    notifyStorageUnavailable(options);
    return;
  }
  for (const record of dueJobs) {
    await processVideoJob(runtime, jobs, record, options.adapter).catch(() => undefined);
  }
}

/** Failure telemetry is optional and must never interrupt durable recovery. */
function notifyStorageUnavailable(options: VideoDrainOptions): void {
  try {
    options.onStorageUnavailable?.();
  } catch {
    // Observability must not make a background delivery fail.
  }
}

/**
 * Authorizes the manual recovery route without exposing queue state.  Vercel
 * Cron invokes the schedule directly; this remains an operator-only escape
 * hatch for safe recovery checks.
 */
export async function authorizeIMessageVideoDrain(
  request: Request,
): Promise<AuthorizedVideoDrain | Response> {
  if (request.method !== "POST") return emptyResponse(405);
  // Reject unauthenticated probes before asking the runtime to validate or
  // connect anything. This keeps a missing production configuration from
  // turning an unauthenticated recovery request into a framework-level 500.
  if (!isBearerAuthorized(request.headers.get("authorization"), process.env.CRON_SECRET?.trim())) {
    return emptyResponse(401);
  }
  let runtime: AirRuntime;
  try {
    runtime = await getAirRuntime();
  } catch {
    return emptyResponse(503);
  }
  if (!isBearerAuthorized(request.headers.get("authorization"), runtime.environment.cronSecret)) {
    return emptyResponse(401);
  }
  return Object.freeze({ runtime });
}

function createVideoJobStore(runtime: AirRuntime): VideoJobStore {
  return new VideoJobStore({
    redis: runtime.redis,
    cipher: runtime.cipher,
    identityHashSecret: runtime.environment.idHashSecret as string,
    clock: runtime.clock,
  });
}

/** Keep the admission mutex adjacent to the pseudonymous active-job fence. */
function videoAdmissionLeaseKey(senderHash: string): string {
  return `${videoJobActiveSenderKey(senderHash)}:admission`;
}

async function processInboxRecord(
  runtime: AirRuntime,
  jobs: VideoJobStore,
  record: InboxRecord,
  adapter: AirVideoIMessageAdapter,
): Promise<PreparedFirstFrameHandoff | undefined> {
  const lease = await runtime.inbox.beginProcessing(record.immutable.recordKey);
  if (!lease) return;
  try {
    const envelope = lease.envelope;
    // Spectrum's multi-line routing must be exact. Without the concrete
    // sending line, Air cannot safely fetch media or return a generation, so
    // terminate before any prompt, quota, or provider work is considered.
    if (!envelope.sendingLine) {
      await transitionInboxTerminal(lease, "message_line_missing");
      return;
    }
    const threadId = adapter.encodeThreadId({
      chatGuid: envelope.chatGuid,
      phone: envelope.sendingLine,
    });

    if (envelope.isGroup) {
      await sendStaticAndFinish(lease, adapter, threadId, AIR_VIDEO_DM_ONLY_NOTICE);
      return;
    }
    if (!isSenderAllowed(record.immutable.senderHash, runtime.environment.allowedSenderHashes)) {
      await sendStaticAndFinish(lease, adapter, threadId, AIR_VIDEO_ACCESS_NOTICE);
      return;
    }

    const prompt = envelope.content.text?.trim() ?? "";
    const attachments = envelope.content.attachments ?? [];
    if (!isValidPrompt(prompt)) {
      await sendStaticAndFinish(lease, adapter, threadId, AIR_VIDEO_INVALID_PROMPT_NOTICE);
      return;
    }
    if (attachments.length > 1) {
      await sendStaticAndFinish(lease, adapter, threadId, AIR_VIDEO_IMAGE_NOTICE);
      return;
    }
    if (attachments.length === 1 && !isPotentialImage(attachments[0])) {
      await sendStaticAndFinish(lease, adapter, threadId, AIR_VIDEO_IMAGE_NOTICE);
      return;
    }

    const correlationId = record.immutable.correlationId;
    // This lease and the active-job key make the admission sequence
    // (preflight -> quota -> durable job) single-writer per sender. Without
    // it, two concurrent receipts can both spend quota before createOrGet's
    // sender fence rejects the second record.
    const admission = await acquireRedisLease(
      runtime.redis,
      videoAdmissionLeaseKey(record.immutable.senderHash),
      { ttlMs: VIDEO_ADMISSION_LEASE_TTL_MS, now: runtime.clock },
    );
    if (!admission) {
      await transitionInboxRetryable(lease, "video_admission_busy", runtime.clock());
      return;
    }

    try {
      const activeRecordKey = await runtime.redis.get(videoJobActiveSenderKey(record.immutable.senderHash));
      // A retry of the receipt which created the active job is still allowed
      // through createOrGet; a distinct receipt gets the stable active notice.
      if (activeRecordKey && activeRecordKey !== videoJobRecordKey(correlationId)) {
        await sendStaticAndFinish(lease, adapter, threadId, AIR_VIDEO_ACTIVE_NOTICE);
        return;
      }

      let firstFrame: PreparedImage | undefined;
      if (attachments.length === 1) {
        try {
          firstFrame = await prepareFirstFrame({
            adapter,
            attachment: attachments[0] as NormalizedIMessageAttachment,
            messageId: envelope.messageId,
            phone: envelope.sendingLine || undefined,
            threadId,
          });
        } catch (error) {
          const workflow = workflowError(error, "image_transfer");
          if (workflow.retryable) {
            await transitionInboxRetryable(lease, workflow.category, runtime.clock());
          } else {
            await sendStaticAndFinish(lease, adapter, threadId, AIR_VIDEO_IMAGE_NOTICE);
          }
          return;
        }
      }

      // Refresh before the durable rate decision so a suspended invocation
      // cannot resume after another admission owner has taken the fence.
      if (!(await admission.refresh(VIDEO_ADMISSION_LEASE_TTL_MS))) {
        await transitionInboxRetryable(lease, "video_admission_lost", runtime.clock());
        return;
      }

      const rate = await runtime.limiter.reserveAtReceipt(
        record.immutable.senderHash,
        correlationId,
        record.immutable.receivedAtMs,
      );
      if (!rate.accepted) {
        if (rate.shouldNotify) {
          await sendStaticAndFinish(lease, adapter, threadId, AIR_VIDEO_RATE_LIMIT_NOTICE);
        } else {
          await transitionInboxTerminal(lease, "rate_notice_suppressed");
        }
        return;
      }

      if (!(await admission.refresh(VIDEO_ADMISSION_LEASE_TTL_MS))) {
        await transitionInboxRetryable(lease, "video_admission_lost", runtime.clock());
        return;
      }

      const firstFrameInput = attachments.length === 1
        ? toFirstFrameInput(envelope.messageId, attachments[0] as NormalizedIMessageAttachment)
        : undefined;
      const firstFrameIdentity = firstFrameInput === undefined
        ? undefined
        : deriveOpaqueId(
            runtime.environment.idHashSecret as string,
            "video-first-frame",
            firstFrameInput.attachment.id,
          );

      let persistedJob: PersistedVideoJob;
      try {
        persistedJob = await jobs.createOrGet({
          correlationId,
          senderHash: record.immutable.senderHash,
          prompt,
          ...(firstFrameInput === undefined ? {} : { firstFrameInput, firstFrameIdentity }),
          delivery: {
            chatGuid: envelope.chatGuid,
            phone: envelope.sendingLine,
          },
          createdAtMs: record.immutable.receivedAtMs,
        });
      } catch (error) {
        if (error instanceof VideoJobStoreError && error.code === "sender_busy") {
          await sendStaticAndFinish(lease, adapter, threadId, AIR_VIDEO_ACTIVE_NOTICE);
          return;
        }
        await transitionInboxRetryable(lease, "video_job_persist", runtime.clock());
        return;
      }

      // A durable validated input exists before this handoff. The job records
      // image-upload and Zap-submission intents before each external handoff;
      // the acknowledgement still waits for durable Zap acceptance.
      const handedOff = await lease.transition("handed_off");
      if (handedOff && firstFrame && persistedJob.created && persistedJob.record.state === "validated") {
        return Object.freeze({ record: persistedJob.record, image: firstFrame });
      }
    } finally {
      await admission.release().catch(() => undefined);
    }
  } catch {
    await transitionInboxRetryable(lease, "inbox_processing", runtime.clock());
  } finally {
    await lease.release();
  }
}

async function processVideoJob(
  runtime: AirRuntime,
  jobs: VideoJobStore,
  record: VideoJobRecord,
  adapter: AirVideoIMessageAdapter,
  preloadedImage?: PreparedImage | undefined,
): Promise<void> {
  const attempt = await jobs.beginWork(record.immutable.recordKey);
  if (!attempt || !attempt.payload) return;
  try {
    switch (attempt.operation) {
      case "upload":
        await uploadVideoFirstFrame(runtime, attempt, adapter, preloadedImage);
        return;
      case "submit":
        await submitVideoJob(runtime, attempt, adapter);
        return;
      case "poll":
        await pollVideoJob(runtime, attempt, adapter);
        return;
      case "send":
        await deliverVideoJob(runtime, attempt, adapter);
        return;
    }
  } finally {
    await attempt.release();
  }
}

/**
 * The raw Photon reference is encrypted in the job until a GMI-readable frame
 * URL has been durably recorded. An interruption in this state may repeat an
 * upload ticket/PUT, but can never reach a paid Seedance submission twice.
 */
async function uploadVideoFirstFrame(
  runtime: AirRuntime,
  attempt: VideoJobAttempt,
  adapter: AirVideoIMessageAdapter,
  preloadedImage?: PreparedImage | undefined,
): Promise<void> {
  const payload = attempt.payload;
  const firstFrameInput = payload?.firstFrameInput;
  if (!payload || !firstFrameInput || payload.firstFrameUrl !== undefined) {
    await attempt.transition({
      expectedStates: ["image_uploading"],
      state: "dead_letter",
      failureCategory: "first_frame_source_missing",
    });
    return;
  }

  try {
    let image = preloadedImage;
    if (!image) {
      const threadId = adapter.encodeThreadId({
        chatGuid: payload.delivery.chatGuid,
        ...(payload.delivery.phone === undefined ? {} : { phone: payload.delivery.phone }),
      });
      image = await prepareFirstFrame({
        adapter,
        attachment: firstFrameInput.attachment,
        messageId: firstFrameInput.messageId,
        phone: payload.delivery.phone,
        threadId,
      });
    }

    const firstFrameUrl = await uploadPreparedFirstFrame({
      image,
      correlationId: attempt.idempotencyKey,
      service: zapClient(runtime),
    });
    const submitting = await attempt.transition({
      expectedStates: ["image_uploading"],
      state: "zap_submitting",
      firstFrameUrl,
      clearFirstFrameInput: true,
    });
    if (!submitting) return;

    // The named checkpoint above is visible to recovery. Persist the stronger
    // no-retry intent before calling Zap in this same fast-path invocation.
    const intent = await attempt.transition({
      expectedStates: ["zap_submitting"],
      state: "submission_unknown",
      failureCategory: "submission_intent",
    });
    if (!intent) return;
    await submitVideoJob(runtime, attempt, adapter);
  } catch (error) {
    const workflow = workflowError(error, "image_transfer");
    if (workflow.retryable) {
      await attempt.transition({
        expectedStates: ["image_uploading"],
        state: "image_uploading",
        failureCategory: workflow.category,
        nextAttemptAtMs: runtime.clock() + VIDEO_RETRY_DELAY_MS,
      });
    } else {
      await attempt.transition({
        expectedStates: ["image_uploading"],
        state: "dead_letter",
        failureCategory: workflow.category,
      });
    }
  }
}

async function submitVideoJob(
  runtime: AirRuntime,
  attempt: VideoJobAttempt,
  adapter: AirVideoIMessageAdapter,
): Promise<void> {
  const payload = attempt.payload;
  if (!payload) return;
  let accepted: VideoJobRecord | null;
  try {
    const result = await zapClient(runtime).createRun({
      prompt: payload.prompt,
      ...(payload.firstFrameUrl === undefined ? {} : { firstFrameUrl: payload.firstFrameUrl }),
      idempotencyKey: attempt.idempotencyKey,
    });
    accepted = await transitionSubmissionResult(attempt, result, runtime.clock());
  } catch (error) {
    // An accepted-but-malformed response, timeout, transport fault, or 5xx
    // can all follow a paid provider submission. Preserve the durable intent
    // for operator/idempotency reconciliation rather than declaring it dead.
    if (isDefinitiveZapSubmissionRejection(error)) {
      await attempt.transition({
        expectedStates: ["submission_unknown"],
        state: "dead_letter",
        failureCategory: "zap_submission_rejected",
      });
    } else {
      await attempt.transition({
        expectedStates: ["submission_unknown"],
        state: "submission_unknown",
        failureCategory: "zap_submission_unknown",
      });
    }
    return;
  }
  await acknowledgeAcceptedZapRun(runtime, attempt, adapter, accepted, payload.delivery);
}

async function transitionSubmissionResult(
  attempt: VideoJobAttempt,
  result: ZapVideoRun,
  now: number,
): Promise<VideoJobRecord | null> {
  switch (result.status) {
    case "queued":
    case "running":
      return attempt.transition({
        expectedStates: ["submission_unknown"],
        state: result.status,
        zapRunId: result.runId,
        nextAttemptAtMs: now + VIDEO_POLL_INTERVAL_MS,
        ...(result.progress === undefined ? {} : { progress: result.progress }),
      });
    case "video_ready":
      if (!result.video) throw new VideoWorkflowError("zap_invalid_completion", false);
      return attempt.transition({
        expectedStates: ["submission_unknown"],
        state: "video_ready",
        zapRunId: result.runId,
        video: result.video,
        progress: 100,
      });
    case "retryable":
      return attempt.transition({
        expectedStates: ["submission_unknown"],
        state: "retryable",
        zapRunId: result.runId,
        failureCategory: "zap_retryable",
        nextAttemptAtMs: now + VIDEO_RETRY_DELAY_MS,
      });
    case "submission_unknown":
      return attempt.transition({
        expectedStates: ["submission_unknown"],
        state: "submission_unknown",
        zapRunId: result.runId,
        failureCategory: "zap_submission_unknown",
      });
    case "dead_letter":
      return attempt.transition({
        expectedStates: ["submission_unknown"],
        state: "dead_letter",
        failureCategory: "zap_dead_letter",
      });
  }
}

async function pollVideoJob(
  runtime: AirRuntime,
  attempt: VideoJobAttempt,
  adapter: AirVideoIMessageAdapter,
): Promise<void> {
  const payload = attempt.payload;
  if (!payload?.zapRunId) return;
  try {
    const result = await zapClient(runtime).getRun(payload.zapRunId);
    const expectedStates = [attempt.record.state];
    switch (result.status) {
      case "queued":
      case "running": {
        const accepted = await attempt.transition({
          expectedStates,
          state: result.status,
          nextAttemptAtMs: runtime.clock() + VIDEO_POLL_INTERVAL_MS,
          ...(result.progress === undefined ? {} : { progress: result.progress }),
        });
        await acknowledgeAcceptedZapRun(runtime, attempt, adapter, accepted, payload.delivery);
        return;
      }
      case "video_ready": {
        if (!result.video) throw new VideoWorkflowError("zap_invalid_completion", false);
        const accepted = await attempt.transition({
          expectedStates,
          state: "video_ready",
          video: result.video,
          progress: 100,
        });
        await acknowledgeAcceptedZapRun(runtime, attempt, adapter, accepted, payload.delivery);
        return;
      }
      case "retryable": {
        const accepted = await attempt.transition({
          expectedStates,
          state: "retryable",
          failureCategory: "zap_poll_retryable",
          nextAttemptAtMs: runtime.clock() + VIDEO_RETRY_DELAY_MS,
        });
        await acknowledgeAcceptedZapRun(runtime, attempt, adapter, accepted, payload.delivery);
        return;
      }
      case "submission_unknown":
        await attempt.transition({
          expectedStates,
          state: "submission_unknown",
          failureCategory: "zap_submission_unknown",
        });
        return;
      case "dead_letter":
        await attempt.transition({
          expectedStates,
          state: "dead_letter",
          failureCategory: "zap_dead_letter",
        });
    }
  } catch (error) {
    const workflow = workflowError(error, "zap_poll");
    // A malformed 200 cannot prove that Zap/provider work failed. Requeue its
    // existing durable run ID so a transient schema/proxy fault is recoverable.
    if (workflow.retryable || (error instanceof ZapVideoServiceError && error.code === "invalid_response")) {
      const accepted = await attempt.transition({
        expectedStates: [attempt.record.state],
        state: "retryable",
        failureCategory: "zap_poll_retryable",
        nextAttemptAtMs: runtime.clock() + VIDEO_RETRY_DELAY_MS,
      });
      await acknowledgeAcceptedZapRun(runtime, attempt, adapter, accepted, payload.delivery);
    } else {
      await attempt.transition({
        expectedStates: [attempt.record.state],
        state: "dead_letter",
        failureCategory: "zap_poll_rejected",
      });
    }
  }
}

/**
 * The initial local record is only a submission intent. Mark and send the
 * static acknowledgement only once a Zap run ID has been durably recorded in
 * an accepted state. The mark is persisted before the transport attempt so an
 * ambiguous Photon result is never automatically replayed.
 */
async function acknowledgeAcceptedZapRun(
  runtime: AirRuntime,
  attempt: VideoJobAttempt,
  adapter: AirVideoIMessageAdapter,
  accepted: VideoJobRecord | null,
  delivery: VideoJobDeliveryTarget,
): Promise<void> {
  try {
    if (
      !accepted ||
      accepted.status.acknowledgementAttemptedAtMs !== undefined ||
      accepted.status.zapRunIdHash === undefined ||
      !isAcceptedZapRunState(accepted.state)
    ) {
      return;
    }
    const current = attempt.record;
    // The returned record proves Zap acceptance was committed before any user
    // message intent is written. Do not acknowledge if this lease no longer
    // owns that exact revision.
    if (current.state !== accepted.state || current.stateVersion !== accepted.stateVersion) return;

    const nextAttemptAtMs = accepted.status.nextAttemptAtMs === undefined
      ? undefined
      : Math.max(accepted.status.nextAttemptAtMs, runtime.clock());
    const marked = await attempt.transition({
      expectedStates: [accepted.state],
      state: accepted.state,
      ...(nextAttemptAtMs === undefined ? {} : { nextAttemptAtMs }),
      ...(accepted.status.progress === undefined ? {} : { progress: accepted.status.progress }),
      ...(accepted.status.failureCategory === undefined
        ? {}
        : { failureCategory: accepted.status.failureCategory }),
      acknowledgementAttempted: true,
    });
    if (!marked) return;
    const threadId = adapter.encodeThreadId({
      chatGuid: delivery.chatGuid,
      ...(delivery.phone === undefined ? {} : { phone: delivery.phone }),
    });
    await adapter.postMessage(threadId, AIR_VIDEO_GENERATING_NOTICE);
  } catch {
    // Acknowledgement bookkeeping and transport are intentionally isolated
    // from provider-result handling. The paid run remains recoverable, and an
    // ambiguous Photon acknowledgement is never replayed automatically.
  }
}

function isAcceptedZapRunState(state: VideoJobRecord["state"]): boolean {
  return state === "queued" || state === "running" || state === "video_ready" || state === "retryable";
}

async function deliverVideoJob(
  runtime: AirRuntime,
  attempt: VideoJobAttempt,
  adapter: AirVideoIMessageAdapter,
): Promise<void> {
  const payload = attempt.payload;
  if (!payload?.video) return;
  const now = runtime.clock();
  if (payload.video.expiresAt && Date.parse(payload.video.expiresAt) <= now) {
    await attempt.transition({
      expectedStates: ["sending"],
      state: "dead_letter",
      failureCategory: "video_expired",
    });
    return;
  }

  let threadId: string;
  try {
    threadId = adapter.encodeThreadId({
      chatGuid: payload.delivery.chatGuid,
      ...(payload.delivery.phone ? { phone: payload.delivery.phone } : {}),
    });
  } catch {
    await returnVideoToDue(attempt, runtime.clock(), "thread_encoding_retryable");
    return;
  }

  try {
    const video = await downloadVideo(payload.video.url);
    try {
      await adapter.postMessage(threadId, {
        raw: "",
        files: [{ data: video, filename: "air-video.mp4", mimeType: "video/mp4" }],
      });
      await attempt.transition({ expectedStates: ["sending"], state: "delivered" });
    } catch {
      await attempt.transition({
        expectedStates: ["sending"],
        state: "send_unknown",
        failureCategory: "photon_video_send_unknown",
      });
    }
  } catch (error) {
    const workflow = workflowError(error, "video_download");
    if (workflow.deterministicOutput && isApprovedBlobUrl(payload.video.url)) {
      try {
        await adapter.postMessage(threadId, `Your video is ready, but it could not be attached. Temporary link: ${payload.video.url}`);
        await attempt.transition({ expectedStates: ["sending"], state: "delivered" });
      } catch {
        await attempt.transition({
          expectedStates: ["sending"],
          state: "send_unknown",
          failureCategory: "photon_fallback_send_unknown",
        });
      }
    } else if (workflow.retryable) {
      await returnVideoToDue(attempt, runtime.clock(), workflow.category);
    } else {
      await attempt.transition({
        expectedStates: ["sending"],
        state: "dead_letter",
        failureCategory: workflow.category,
      });
    }
  }
}

/** Only request-local validation or an explicit non-retryable 4xx proves no paid run was accepted. */
function isDefinitiveZapSubmissionRejection(error: unknown): boolean {
  if (!(error instanceof ZapVideoServiceError)) return false;
  if (error.code === "invalid_configuration" || error.code === "invalid_request") return true;
  return error.code === "unexpected_status" &&
    error.status !== undefined &&
    error.status >= 400 &&
    error.status < 500 &&
    error.status !== 408 &&
    error.status !== 429;
}

async function returnVideoToDue(attempt: VideoJobAttempt, now: number, category: string): Promise<void> {
  await attempt.transition({
    expectedStates: ["sending"],
    state: "video_ready",
    nextAttemptAtMs: now + VIDEO_RETRY_DELAY_MS,
    failureCategory: category,
  });
}

/** Read and validate provider media before a rate slot is reserved. */
async function prepareFirstFrame(input: {
  readonly adapter: AirVideoIMessageAdapter;
  readonly attachment: NormalizedIMessageAttachment;
  readonly messageId: string;
  readonly phone?: string | undefined;
  readonly threadId: string;
}): Promise<PreparedImage> {
  if (input.attachment.size !== null && input.attachment.size > MAX_INPUT_IMAGE_BYTES) {
    throw new VideoWorkflowError("image_too_large", false);
  }
  // Spectrum requires the sending line in multi-client mode. Retrying without
  // it cannot succeed deterministically and could leak an attachment lookup
  // across a line, so reject before opening the provider stream.
  if (!input.phone) throw new VideoWorkflowError("attachment_line_missing", false);
  const bytes = await readPhotonAttachment(input.adapter, input.threadId, input.messageId, input.attachment.id, input.phone);
  return prepareImage(bytes, input.attachment);
}

/** Upload only an already validated image after quota admission succeeds. */
async function uploadPreparedFirstFrame(input: {
  readonly image: PreparedImage;
  readonly correlationId: string;
  readonly service: ZapVideoServiceClient;
}): Promise<string> {
  const ticket = await input.service.createUpload({
    mimeType: input.image.mimeType,
    sizeBytes: input.image.bytes.byteLength,
    idempotencyKey: `${input.correlationId}.upload`,
  });
  await putSignedImage(ticket.uploadUrl, ticket.headers, input.image.bytes);
  return ticket.publicUrl;
}

async function readPhotonAttachment(
  adapter: AirVideoIMessageAdapter,
  threadId: string,
  messageId: string,
  guid: string,
  phone: string | undefined,
): Promise<Buffer> {
  try {
    // The adapter lazily builds its Spectrum instance during a message lookup.
    // Do not retain the looked-up message; the attachment GUID is all we keep.
    await adapter.fetchMessage(threadId, messageId);
  } catch {
    // A fetch cache miss can still have initialized the app. The attachment
    // lookup below is authoritative and gives a safe retryable failure if not.
  }
  if (!adapter.app) throw new VideoWorkflowError("attachment_transport", true);
  const platform = imessage as unknown as (app: unknown) => SpectrumAttachmentReader;
  let attachment: Awaited<ReturnType<SpectrumAttachmentReader["getAttachment"]>>;
  try {
    attachment = await platform(adapter.app).getAttachment(guid, phone);
  } catch {
    throw new VideoWorkflowError("attachment_transport", true);
  }
  if (!attachment) throw new VideoWorkflowError("attachment_missing", false);
  if (attachment.size !== undefined && (!Number.isSafeInteger(attachment.size) || attachment.size < 1 || attachment.size > MAX_INPUT_IMAGE_BYTES)) {
    throw new VideoWorkflowError("image_too_large", false);
  }
  try {
    return await readBoundedAttachmentStream(await attachment.stream());
  } catch (error) {
    if (error instanceof VideoWorkflowError) throw error;
    throw new VideoWorkflowError("attachment_read", true);
  }
}

/** Read and cancel a provider stream before an undeclared large file reaches memory. */
async function readBoundedAttachmentStream(stream: ReadableStream<Uint8Array>): Promise<Buffer> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      total += next.value.byteLength;
      if (total > MAX_INPUT_IMAGE_BYTES) {
        await reader.cancel().catch(() => undefined);
        throw new VideoWorkflowError("image_too_large", false);
      }
      chunks.push(next.value);
    }
  } catch (error) {
    if (error instanceof VideoWorkflowError) throw error;
    throw new VideoWorkflowError("attachment_read", true);
  } finally {
    reader.releaseLock();
  }
  if (total < 1) throw new VideoWorkflowError("invalid_image", false);
  return Buffer.concat(chunks.map((chunk) => Buffer.from(chunk)), total);
}

async function prepareImage(bytes: Buffer, attachment: NormalizedIMessageAttachment): Promise<PreparedImage> {
  const expected = expectedImageKind(attachment);
  if (!expected) throw new VideoWorkflowError("unsupported_image", false);
  const detected = detectImageKind(bytes);
  if (!detected || detected !== expected) throw new VideoWorkflowError("invalid_image", false);
  if (detected === "jpeg") return Object.freeze({ bytes, mimeType: "image/jpeg" });
  if (detected === "png") return Object.freeze({ bytes, mimeType: "image/png" });
  try {
    const jpeg = Buffer.from(await heicConvert({ buffer: bytes, format: "JPEG", quality: 0.9 }));
    if (jpeg.byteLength < 4 || jpeg.byteLength > MAX_INPUT_IMAGE_BYTES || detectImageKind(jpeg) !== "jpeg") {
      throw new VideoWorkflowError("invalid_image", false);
    }
    return Object.freeze({ bytes: jpeg, mimeType: "image/jpeg" });
  } catch (error) {
    if (error instanceof VideoWorkflowError) throw error;
    throw new VideoWorkflowError("invalid_image", false);
  }
}

async function putSignedImage(url: string, headers: Readonly<Record<string, string>>, bytes: Buffer): Promise<void> {
  if (!isApprovedGmiUploadUrl(url)) throw new VideoWorkflowError("image_upload_rejected", false);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30_000);
  try {
    const response = await fetch(url, {
      method: "PUT",
      headers,
      body: Uint8Array.from(bytes),
      redirect: "error",
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new VideoWorkflowError("image_upload", response.status >= 500 || response.status === 429);
    }
  } catch (error) {
    if (error instanceof VideoWorkflowError) throw error;
    throw new VideoWorkflowError("image_upload", true);
  } finally {
    clearTimeout(timeout);
  }
}

async function downloadVideo(url: string): Promise<Buffer> {
  if (!isApprovedBlobUrl(url)) throw new VideoWorkflowError("output_rejected", false, true);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30_000);
  try {
    const response = await fetch(url, { redirect: "error", signal: controller.signal });
    if (!response.ok) throw new VideoWorkflowError("video_download", response.status >= 500 || response.status === 429);
    const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
    if (!contentType.startsWith("video/mp4")) throw new VideoWorkflowError("output_rejected", false, true);
    const declared = response.headers.get("content-length");
    if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > MAX_OUTPUT_VIDEO_BYTES)) {
      throw new VideoWorkflowError("output_too_large", false, true);
    }
    const bytes = await readBoundedBody(response, MAX_OUTPUT_VIDEO_BYTES);
    if (!isMp4(bytes)) throw new VideoWorkflowError("output_rejected", false, true);
    return bytes;
  } catch (error) {
    if (error instanceof VideoWorkflowError) throw error;
    throw new VideoWorkflowError("video_download", true);
  } finally {
    clearTimeout(timeout);
  }
}

async function readBoundedBody(response: Response, maximum: number): Promise<Buffer> {
  if (!response.body) throw new VideoWorkflowError("video_download", true);
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      total += next.value.byteLength;
      if (total > maximum) {
        await reader.cancel().catch(() => undefined);
        throw new VideoWorkflowError("output_too_large", false, true);
      }
      chunks.push(next.value);
    }
  } catch (error) {
    if (error instanceof VideoWorkflowError) throw error;
    throw new VideoWorkflowError("video_download", true);
  } finally {
    reader.releaseLock();
  }
  return Buffer.concat(chunks.map((chunk) => Buffer.from(chunk)), total);
}

function expectedImageKind(attachment: NormalizedIMessageAttachment): "jpeg" | "png" | "heic" | undefined {
  const mime = attachment.mimeType?.toLowerCase().split(";", 1)[0]?.trim();
  if (mime === "image/jpeg") return "jpeg";
  if (mime === "image/png") return "png";
  if (mime === "image/heic" || mime === "image/heif") return "heic";
  const name = attachment.name?.toLowerCase();
  const extension = name ? name.slice(name.lastIndexOf(".") + 1) : undefined;
  if (extension === "jpg" || extension === "jpeg") return "jpeg";
  if (extension === "png") return "png";
  if (extension === "heic" || extension === "heif") return "heic";
  return undefined;
}

function isPotentialImage(attachment: NormalizedIMessageAttachment | undefined): boolean {
  return attachment !== undefined && expectedImageKind(attachment) !== undefined &&
    (attachment.size === null || attachment.size <= MAX_INPUT_IMAGE_BYTES);
}

/** Keep the Photon lookup reference only in the encrypted video-job payload. */
function toFirstFrameInput(
  messageId: string,
  attachment: NormalizedIMessageAttachment,
): VideoJobFirstFrameInput {
  return Object.freeze({
    messageId,
    attachment: Object.freeze({
      id: attachment.id,
      name: attachment.name,
      mimeType: attachment.mimeType,
      size: attachment.size,
    }),
  });
}

function detectImageKind(bytes: Buffer): "jpeg" | "png" | "heic" | undefined {
  if (bytes.length >= 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return "jpeg";
  if (bytes.length >= 8 && bytes.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))) return "png";
  if (bytes.length >= 12 && bytes.subarray(4, 8).toString("ascii") === "ftyp") {
    const brand = bytes.subarray(8, 12).toString("ascii").toLowerCase();
    if (["heic", "heix", "hevc", "hevx", "mif1", "msf1"].includes(brand)) return "heic";
  }
  return undefined;
}

function isMp4(bytes: Buffer): boolean {
  return bytes.length >= 12 && bytes.subarray(4, 8).toString("ascii") === "ftyp";
}

function isApprovedBlobUrl(value: string): boolean {
  try {
    const url = new URL(value);
    const host = url.hostname.toLowerCase();
    return url.protocol === "https:" && !url.username && !url.password && !url.port &&
      (host === "blob.vercel-storage.com" || host.endsWith(".blob.vercel-storage.com"));
  } catch {
    return false;
  }
}

/** GMI documents signed object URLs on Google Cloud Storage for Air uploads. */
function isApprovedGmiUploadUrl(value: string): boolean {
  try {
    const url = new URL(value);
    const host = url.hostname.toLowerCase();
    return url.protocol === "https:" && !url.username && !url.password && !url.port &&
      (host === "storage.googleapis.com" || host.endsWith(".storage.googleapis.com"));
  } catch {
    return false;
  }
}

function zapClient(runtime: AirRuntime): ZapVideoServiceClient {
  return createZapVideoServiceClient({
    serviceUrl: runtime.environment.zapAirServiceUrl as string,
    serviceToken: runtime.environment.zapAirServiceToken as string,
  });
}

function inboxContentType(
  text: string | null,
  attachments: readonly NormalizedIMessageAttachment[],
  providerContentType: string | null,
): InboxContentType {
  if (attachments.length > 0) return "attachment";
  if (typeof text === "string") return "text";
  return providerContentType === "reaction" ? "reaction" : "unknown";
}

function isValidPrompt(prompt: string): boolean {
  return prompt.length >= 1 && prompt.length <= 2_000 && !prompt.includes("\u0000");
}

function workflowError(error: unknown, fallback: string): VideoWorkflowError {
  if (error instanceof VideoWorkflowError) return error;
  if (error instanceof ZapVideoServiceError) {
    return new VideoWorkflowError(fallback, error.retryable);
  }
  return new VideoWorkflowError(fallback, true);
}

async function sendStaticAndFinish(
  lease: InboxProcessingLease,
  adapter: AirVideoIMessageAdapter,
  threadId: string,
  message: string,
): Promise<void> {
  const ready = await lease.transition("response_ready");
  if (!ready) return;
  const sending = await lease.transition("sending");
  if (!sending) return;
  try {
    await adapter.postMessage(threadId, message);
    await lease.transition("sent");
  } catch {
    await lease.transition("send_unknown", { failureCategory: "photon_static_send_unknown" });
  }
}

async function transitionInboxRetryable(
  lease: InboxProcessingLease,
  category: string,
  now: number,
): Promise<void> {
  await lease.transition("failed_retryable", {
    failureCategory: category,
    nextAttemptAtMs: now + VIDEO_RETRY_DELAY_MS,
  }).catch(() => undefined);
}

async function transitionInboxTerminal(lease: InboxProcessingLease, category: string): Promise<void> {
  await lease.transition("dead_letter", { failureCategory: category }).catch(() => undefined);
}

function webhookInputResponse(error: unknown): Response {
  if (error instanceof WebhookInputError) return emptyResponse(error.status);
  return emptyResponse(400);
}

function emptyResponse(status: number): Response {
  return new Response(null, { status });
}

function isBearerAuthorized(value: string | null, secret: string | undefined): boolean {
  if (!value || !secret) return false;
  const expected = Buffer.from(`Bearer ${secret}`, "utf8");
  const actual = Buffer.from(value, "utf8");
  return expected.byteLength === actual.byteLength && timingSafeEqual(expected, actual);
}
