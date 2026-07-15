import { randomBytes, timingSafeEqual } from "node:crypto";
import type { EncryptedPayload, QueueCipher } from "./crypto.js";
import { createQueueCipher } from "./crypto.js";
import {
  DurableInbox,
  type InboxContentType,
  type InboxProcessingLease,
  type InboxRecord,
  type InboxState,
} from "./durable-inbox.js";
import { DurableOutbox, outboxRecordKey, type OutboxState } from "./durable-outbox.js";
import { loadAirEnvironment, type AirEnvironment } from "./env.js";
import { createRedactedLogger } from "./logging.js";
import { RateLimiter } from "./rate-limit.js";
import {
  COMPARE_AND_DELETE_LUA,
  COMPARE_AND_SET_WITH_PX_LUA,
  connectRedisFromUrl,
  type RedisLike,
} from "./redis.js";
import { OperationDeadline } from "./upstream.js";
import {
  createSenderIdentity,
  deriveOpaqueId,
  hashThreadIdentifier,
  isSenderAllowed,
  type CorrelationId,
  type SenderHash,
  type ThreadHash,
} from "./security.js";
import {
  decodeWebhookJson,
  normalizeSpectrumWebhook,
  readRawBodyLimited,
  SPECTRUM_SIGNATURE_HEADER,
  SPECTRUM_TIMESTAMP_HEADER,
  SPECTRUM_WEBHOOK_ID_HEADER,
  verifySpectrumSignature,
  WebhookInputError,
  type NormalizedIMessageEnvelope,
} from "./spectrum-webhook.js";
import {
  AR_AGENT_ACCESS_NOTICE,
  AR_AGENT_CAPABILITY_BLURB,
  AR_AGENT_DM_ONLY_NOTICE,
  AR_AGENT_RATE_LIMIT_NOTICE,
  AR_AGENT_REPHRASE_GUIDANCE,
  AR_AGENT_RETRY_NOTICE,
  AR_AGENT_TEXT_ONLY_NOTICE,
  AR_AGENT_TOO_LONG_NOTICE,
} from "./messages.js";

/** The fixed Chat SDK adapter name used by the sole Air V1 transport. */
export const IMESSAGE_ADAPTER_NAME = "imessage" as const;

const ACTIVE_TURN_TTL_MS = 75_000;
const ENCRYPTED_TRANSIENT_TTL_SECONDS = 86_400;
const NOTICE_TTL_SECONDS = 7 * 24 * 60 * 60;
const DRAIN_LIMIT = 24;
const MAX_QUESTION_CHARS = 2_000;
/** Signed ingress has to acknowledge or fail closed before Photon retries. */
export const WEBHOOK_DURABILITY_BUDGET_MS = 900;

const logger = createRedactedLogger((event) => {
  // The logger sanitizes fields before they reach the Vercel log stream.
  console.log(JSON.stringify(event));
});

export interface AirIMessageAuth {
  readonly authenticator: "air-imessage";
  readonly principalId: string;
  readonly principalType: "user";
  /** Eve's cross-channel auth context accepts strings or string collections only. */
  readonly attributes: Readonly<Record<string, string | readonly string[]>>;
}

/** The only cross-channel hand-off accepted by the durable worker. */
export interface AirIMessageTurnInput {
  readonly auth: AirIMessageAuth;
  readonly message: string;
  readonly target: {
    readonly adapterName: typeof IMESSAGE_ADAPTER_NAME;
    readonly threadId: string;
    /** HMAC-derived and attempt-scoped; never the raw queue capability. */
    readonly continuationKey: string;
  };
}

export interface AirIMessageAdapter {
  encodeThreadId(input: { readonly chatGuid: string; readonly phone?: string }): string;
  postMessage(threadId: string, message: string): Promise<unknown>;
  markRead?(threadId: string, messageId: string): Promise<void>;
  startTyping?(threadId: string, status?: string): Promise<void>;
}

/** Minimal shape returned by Eve's cross-channel `receive()` hand-off. */
export interface AirIMessageSession {
  readonly id: string;
}

export interface AirChatThread {
  readonly id: string;
  post(message: string): Promise<unknown>;
}

export interface AirChatEventChannel {
  readonly thread: AirChatThread | null;
  /** Eve channel operation used to retire a parked HITL continuation. */
  setContinuationToken?(token: string): void;
}

export interface DurableDrainOptions {
  readonly adapter: AirIMessageAdapter;
  readonly receive: (input: AirIMessageTurnInput) => Promise<AirIMessageSession>;
  readonly limit?: number;
}

export interface WebhookRuntimeOptions extends DurableDrainOptions {
  readonly waitUntil: (promise: Promise<unknown>) => void;
}

type RuntimeDependencies = {
  readonly environment: AirEnvironment;
  readonly redis: RedisLike;
  /** Shared with deterministic queue tests; never derives time from input. */
  readonly clock: () => number;
  readonly cipher: QueueCipher;
  readonly inbox: DurableInbox;
  readonly outbox: DurableOutbox;
  readonly limiter: RateLimiter;
  /** Production-only immediate teardown of a hung Redis transport. */
  readonly invalidate?: () => void;
};

export type AirRuntime = RuntimeDependencies;

type ActiveTurn = {
  readonly version: 1;
  readonly recordKey: string;
  readonly correlationId: CorrelationId;
  readonly senderHash: SenderHash;
  readonly threadHash: ThreadHash;
  readonly threadIdHash: string;
  /** Raw worker capability; stored only in this encrypted, short-lived map. */
  readonly attemptToken: string;
  readonly startedAtMs: number;
  /** Bound only after cross-channel receive returns the exact Eve session. */
  readonly sessionId?: string;
  /** Prevents pre-receive lifecycle events from delivering a stale response. */
  readonly receiveBound: boolean;
  readonly turnId?: string;
};

type StoredValidatedResult = {
  readonly version: 1;
  readonly attemptToken: string;
  readonly renderedResponse: string;
};

type StoredDeliveryTarget = {
  readonly version: 1;
  readonly recordKey: string;
  readonly threadId: string;
  readonly attemptToken: string;
};

type SendTransport =
  | { readonly kind: "chat_thread"; readonly thread: AirChatThread }
  | { readonly kind: "adapter"; readonly adapter: AirIMessageAdapter }
  | { readonly kind: "unavailable" };

type NoticeClaim = {
  readonly key: string;
  readonly token: string;
};

let runtimeOverride: AirRuntime | undefined;
let runtimePromise: Promise<AirRuntime> | undefined;

/**
 * Test and integration-fixture constructor. Production uses the lazy Redis
 * connector below; it never creates a connection while Eve discovers modules.
 */
export function createAirRuntime(input: {
  readonly environment: AirEnvironment;
  readonly redis: RedisLike;
  readonly clock?: () => number;
  readonly invalidate?: () => void;
}): AirRuntime {
  const { environment, redis } = input;
  assertQueueEnvironment(environment);
  const clock = input.clock ?? Date.now;
  const cipher = createQueueCipher(environment.queueEncryptionKey as string);
  return Object.freeze({
    environment,
    redis,
    clock,
    cipher,
    inbox: new DurableInbox({
      redis,
      cipher,
      identityHashSecret: environment.idHashSecret as string,
      clock,
    }),
    outbox: new DurableOutbox({
      redis,
      cipher,
      identityHashSecret: environment.idHashSecret as string,
      clock,
    }),
    limiter: new RateLimiter(redis, { now: clock }),
    ...(input.invalidate ? { invalidate: input.invalidate } : {}),
  });
}

/** Isolated tests may inject MemoryRedis without teaching production code a fallback. */
export function __setAirRuntimeForTests(runtime: AirRuntime | undefined): void {
  runtimeOverride = runtime;
  runtimePromise = undefined;
}

async function getRuntime(): Promise<AirRuntime> {
  if (runtimeOverride) return runtimeOverride;
  if (!runtimePromise) {
    let pending: Promise<AirRuntime>;
    pending = (async () => {
      const environment = loadAirEnvironment();
      assertQueueEnvironment(environment);
      const connection = await connectRedisFromUrl(environment.redisUrl as string);
      let invalidated = false;
      const invalidate = () => {
        if (invalidated) return;
        invalidated = true;
        // The connected client uses no automatic reconnects. Discard only this
        // exact singleton so the next signed webhook makes a bounded fresh
        // connection instead of retaining a broken warm client.
        if (runtimePromise === pending) runtimePromise = undefined;
        connection.destroy();
      };
      connection.onOperationalError(invalidate);
      return createAirRuntime({ environment, redis: connection.redis, invalidate });
    })();
    runtimePromise = pending;
    // A transient Redis/DNS failure must not permanently poison a warm
    // serverless instance. Keep the successful singleton, but retry later.
    void pending.catch(() => {
      if (runtimePromise === pending) runtimePromise = undefined;
    });
  }
  return runtimePromise;
}

function assertQueueEnvironment(environment: AirEnvironment): void {
  if (
    !environment.imessageProjectId ||
    !environment.imessageWebhookSecret ||
    !environment.redisUrl ||
    !environment.idHashSecret ||
    !environment.queueEncryptionKey
  ) {
    throw new Error("AR Agent durable runtime configuration is incomplete");
  }
}

/**
 * Verify exactly one signed Photon delivery, persist its encrypted normalized
 * envelope first, then defer queue work. Nothing invokes the adapter webhook
 * handler on this public path.
 */
export async function handleIMessageWebhook(
  request: Request,
  options: WebhookRuntimeOptions,
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
    logger.warn("imessage.webhook_rejected", { category: verification.reason, status: verification.status });
    return emptyResponse(verification.status);
  }

  let normalized: NormalizedIMessageEnvelope | null;
  try {
    const payload = decodeWebhookJson(rawBody);
    normalized = normalizeSpectrumWebhook({
      payload,
      projectId: process.env.IMESSAGE_PROJECT_ID ?? "",
      webhookId: request.headers.get(SPECTRUM_WEBHOOK_ID_HEADER),
      receivedAt: Date.now(),
    });
  } catch (error) {
    return webhookInputResponse(error);
  }

  // Only inbound message events have application work. Authenticated outbound
  // echoes and other Photon event kinds are acknowledged without entering Redis.
  if (!normalized || normalized.direction !== "inbound") return emptyResponse(204);
  if (normalized.spaceType !== "dm" && normalized.spaceType !== "group") return emptyResponse(204);
  if (!normalized.senderId) return emptyResponse(204);
  const contentType = toInboxContentType(normalized.contentType);
  // Reactions and unknown event-shaped payloads never need recovery. Drop
  // them before Redis so ordinary iMessage activity does not create a
  // dead-letter/alert or a bot-to-bot reply loop. Attachments remain queued
  // for the product's one text-only guidance response.
  if (contentType === "reaction" || contentType === "unknown") {
    logger.info("imessage.webhook_ignored", { category: "unsupported_inbound_event" });
    return emptyResponse(204);
  }

  const durabilityDeadline = new OperationDeadline(WEBHOOK_DURABILITY_BUDGET_MS);
  let runtime: AirRuntime | undefined;
  try {
    runtime = await durabilityDeadline.run(getRuntime());
    const sender = createSenderIdentity(runtime.environment.idHashSecret as string, normalized.senderId);
    const threadHash = hashThreadIdentifier(
      runtime.environment.idHashSecret as string,
      normalized.chatGuid,
    );
    const persisted = await durabilityDeadline.run(runtime.inbox.persistVerified({
      projectId: normalized.projectId,
      messageId: normalized.messageId,
      webhookId: normalized.webhookId,
      senderHash: sender.senderHash,
      threadHash,
      contentType,
      isGroup: normalized.spaceType === "group",
      receivedAtMs: normalized.receivedAt,
      envelope: {
        projectId: normalized.projectId,
        messageId: normalized.messageId,
        webhookId: normalized.webhookId,
        chatGuid: normalized.chatGuid,
        // The official adapter only needs a sending line for cold-cache
        // replies. A blank value is intentional when Photon does not supply it.
        sendingLine: normalized.phone ?? "",
        senderId: normalized.senderId,
        direction: "inbound",
        content: { type: contentType, text: normalized.text },
        isGroup: normalized.spaceType === "group",
        receivedAtMs: normalized.receivedAt,
      },
    }));

    logger.info(persisted.created ? "imessage.webhook_accepted" : "imessage.webhook_duplicate", {
      correlationId: persisted.record.immutable.correlationId,
      deliveryCount: persisted.record.delivery.deliveryCount,
    });
    if (persisted.created) {
      options.waitUntil(
        drainDurableInbox({ adapter: options.adapter, receive: options.receive }).catch(() => {
          logger.error("imessage.drain_failed", { category: "post_ack_drain" });
        }),
      );
    }
    return emptyResponse(200);
  } catch (error) {
    // A late Redis EVAL may still commit after our deadline. Destroying this
    // client makes the current invocation fail closed; Photon retries are
    // idempotent against the durable inbox's atomic first-write key.
    runtime?.invalidate?.();
    logger.error("imessage.webhook_persist_failed", { category: classifyError(error) });
    return emptyResponse(503);
  } finally {
    durabilityDeadline.dispose();
  }
}

/** The recurring Eve schedule and the post-ack hook share this exact worker. */
export async function drainDurableInbox(options: DurableDrainOptions): Promise<void> {
  let runtime: AirRuntime;
  try {
    runtime = await getRuntime();
  } catch {
    logger.error("imessage.drain_unavailable", { category: "runtime_unavailable" });
    return;
  }

  // First drain only known pre-send retries. This avoids re-running retrieval
  // when a response already exists durably in the outbox.
  await drainOutbox(runtime, options.adapter, options.limit ?? DRAIN_LIMIT);

  let due: readonly InboxRecord[];
  try {
    due = await runtime.inbox.listDue(options.limit ?? DRAIN_LIMIT);
  } catch {
    logger.error("imessage.inbox_scan_failed", { category: "inbox_scan" });
    return;
  }
  logger.info("imessage.inbox_scan", {
    dueCount: due.length,
    oldestInboxAgeMs: due.reduce(
      (oldest, record) => Math.max(oldest, runtime.clock() - record.immutable.receivedAtMs),
      0,
    ),
  });

  for (const record of due) {
    try {
      await processInboxRecord(runtime, record, options);
    } catch {
      logger.error("imessage.inbox_record_failed", {
        correlationId: record.immutable.correlationId,
        category: "worker_record",
      });
    }
  }

  // A response may have become due while the loop processed static notices.
  await drainOutbox(runtime, options.adapter, options.limit ?? DRAIN_LIMIT);
}

async function processInboxRecord(
  runtime: AirRuntime,
  record: InboxRecord,
  options: DurableDrainOptions,
): Promise<void> {
  const lease = await runtime.inbox.beginProcessing(record.immutable.recordKey);
  if (!lease) return;

  const { envelope } = lease;
  const current = lease.record;
  const correlationId = current.immutable.correlationId;

  // Reactions and unrecognized provider event payloads are not messages Air
  // can answer.  They still enter the encrypted inbox for signed-delivery
  // idempotency, but must never trigger a bot reply (which could create a
  // reaction loop), a Chat SDK session, or any RAG/model spend. Attachments
  // deliberately take the separate text-only guidance path below.
  if (
    current.immutable.contentType === "reaction" ||
    current.immutable.contentType === "unknown"
  ) {
    await transitionTerminal(runtime, lease, "unsupported_inbound_event");
    await lease.release();
    return;
  }

  let threadId: string;
  let handoffOwnsLease = false;
  try {
    threadId = options.adapter.encodeThreadId({
      chatGuid: envelope.chatGuid,
      ...(envelope.sendingLine ? { phone: envelope.sendingLine } : {}),
    });
  } catch {
    await transitionRetryable(runtime, lease, "thread_encoding_failed");
    await lease.release();
    return;
  }

  try {
    // An interrupted known pre-send failure has an outbox response already.
    // Continue that response instead of re-running Eve or the RAG tool.
    const extantOutbox = await runtime.outbox.getRecord(outboxRecordKey(correlationId));
    if (extantOutbox) {
      await continueExistingOutbox(runtime, lease, current, threadId, options.adapter, extantOutbox.state);
      return;
    }

    if (envelope.isGroup) {
      const notice = await claimNotice(
        runtime,
        `${noticeKeyPrefix(runtime, "group")}:${current.immutable.threadHash}`,
      );
      if (!notice) {
        await transitionTerminal(runtime, lease, "group_notice_suppressed");
        return;
      }
      try {
        const queued = await queueStaticResponse(runtime, lease, current, threadId, options.adapter, AR_AGENT_DM_ONLY_NOTICE);
        if (!queued) await releaseNotice(runtime, notice);
      } catch {
        await releaseNotice(runtime, notice);
        throw new Error("group_notice_enqueue_failed");
      }
      return;
    }

    if (!isSenderAllowed(current.immutable.senderHash, runtime.environment.allowedSenderHashes)) {
      const notice = await claimNotice(
        runtime,
        `${noticeKeyPrefix(runtime, "access")}:${current.immutable.senderHash}`,
      );
      if (!notice) {
        await transitionTerminal(runtime, lease, "access_notice_suppressed");
        return;
      }
      try {
        const queued = await queueStaticResponse(runtime, lease, current, threadId, options.adapter, AR_AGENT_ACCESS_NOTICE);
        if (!queued) await releaseNotice(runtime, notice);
      } catch {
        await releaseNotice(runtime, notice);
        throw new Error("access_notice_enqueue_failed");
      }
      return;
    }

    // A silent repeat from a non-allowlisted sender must not visibly wake the
    // bot. For an authorized direct message, these are UX-only best efforts.
    await bestEffortInboundSignals(options.adapter, threadId, envelope.messageId);

    const text = envelope.content.type === "text" ? envelope.content.text?.trim() ?? "" : "";
    if (!text) {
      await queueStaticResponse(runtime, lease, current, threadId, options.adapter, AR_AGENT_TEXT_ONLY_NOTICE);
      return;
    }
    if (text.length > MAX_QUESTION_CHARS) {
      await queueStaticResponse(runtime, lease, current, threadId, options.adapter, AR_AGENT_TOO_LONG_NOTICE);
      return;
    }
    if (isCapabilityOnlyMessage(text)) {
      await queueStaticResponse(runtime, lease, current, threadId, options.adapter, AR_AGENT_CAPABILITY_BLURB);
      return;
    }

    const rate = await runtime.limiter.consume(current.immutable.senderHash, correlationId);
    if (!rate.accepted) {
      logger.warn("imessage.rate_limited", {
        correlationId,
        category: rate.reason ?? "unknown",
        retry: rate.retryAfterSeconds,
      });
      if (rate.shouldNotify) {
        await queueStaticResponse(runtime, lease, current, threadId, options.adapter, AR_AGENT_RATE_LIMIT_NOTICE);
      } else {
        await transitionTerminal(runtime, lease, "rate_notice_suppressed");
      }
      return;
    }

    const active: ActiveTurn = {
      version: 1,
      recordKey: current.immutable.recordKey,
      correlationId,
      senderHash: current.immutable.senderHash,
      threadHash: current.immutable.threadHash,
      threadIdHash: opaqueThreadId(runtime, threadId),
      attemptToken: lease.attemptToken,
      startedAtMs: runtime.clock(),
      receiveBound: false,
    };
    if (!(await storeActiveTurn(runtime, threadId, active))) {
      await transitionRetryable(runtime, lease, "active_thread_busy");
      return;
    }
    await storeDeliveryTarget(runtime, correlationId, {
      version: 1,
      recordKey: current.immutable.recordKey,
      threadId,
      attemptToken: lease.attemptToken,
    });
    const started = await lease.transition("session_started", { expectedStates: ["processing"] });
    if (!started || started.state !== "session_started") {
      await clearActiveTurn(runtime, threadId, current.immutable.recordKey, lease.attemptToken);
      await transitionRetryable(runtime, lease, "session_start_state_conflict");
      return;
    }
    if (!(await lease.heartbeat())) {
      await clearActiveTurn(runtime, threadId, current.immutable.recordKey, lease.attemptToken);
      await transitionRetryable(runtime, lease, "session_lease_heartbeat_failed");
      return;
    }

    try {
      const session = await options.receive({
        message: text,
        target: {
          adapterName: IMESSAGE_ADAPTER_NAME,
          threadId,
          continuationKey: attemptScopedContinuationKey(runtime, active),
        },
        auth: {
          authenticator: "air-imessage",
          principalId: current.immutable.senderHash,
          principalType: "user",
          attributes: {
            senderHash: current.immutable.senderHash,
            threadHash: current.immutable.threadHash,
          },
        },
      });
      const sessionId = sessionIdFromReceive(session);
      if (!sessionId || !(await bindActiveTurnSession(runtime, threadId, active, sessionId))) {
        await clearActiveTurn(runtime, threadId, current.immutable.recordKey, lease.attemptToken);
        await transitionRetryable(runtime, lease, "eve_session_bind_failed");
        return;
      }
      // The task is now owned by durable active-turn metadata rather than this
      // request. Lifecycle callbacks renew/release the attempt-derived Redis
      // lease, so a warm process is never required to preserve ordering.
      handoffOwnsLease = true;
      logger.info("imessage.session_handed_off", { correlationId });
    } catch {
      await clearActiveTurn(runtime, threadId, current.immutable.recordKey, lease.attemptToken);
      await transitionRetryable(runtime, lease, "eve_receive_failed");
    }
  } finally {
    if (!handoffOwnsLease) await lease.release();
  }
}

async function continueExistingOutbox(
  runtime: AirRuntime,
  lease: InboxProcessingLease,
  record: InboxRecord,
  threadId: string,
  adapter: AirIMessageAdapter,
  outboxState: OutboxState,
): Promise<void> {
  const correlationId = record.immutable.correlationId;
  const attemptToken = lease.attemptToken;
  if (outboxState === "sent") {
    // Rebind any pre-crash transient records before terminal cleanup. The
    // recovered B capability must own their conditional deletion, otherwise
    // A's encrypted artifacts remain until TTL despite a confirmed send.
    await storeDeliveryTarget(runtime, correlationId, {
      version: 1,
      recordKey: record.immutable.recordKey,
      threadId,
      attemptToken,
    });
    await rebindValidatedResult(runtime, correlationId, attemptToken);
    // The provider send completed before a crash, but the inbox did not reach
    // its terminal state. Reconcile through the legal state graph without a
    // second provider call.
    const ready = await ensureResponseReady(runtime, record.immutable.recordKey, attemptToken);
    if (!ready) return;
    const sending = await transitionInbox(
      runtime,
      record.immutable.recordKey,
      "sending",
      ["response_ready"],
      attemptToken,
    );
    if (!sending || sending.state !== "sending") return;
    const sent = await transitionInbox(
      runtime,
      record.immutable.recordKey,
      "sent",
      ["sending"],
      attemptToken,
    );
    if (sent?.state === "sent") {
      await cleanupDeliveredTurn(runtime, correlationId, threadId, record.immutable.recordKey, attemptToken);
    }
    return;
  }
  if (outboxState === "send_unknown" || outboxState === "dead_letter") {
    const terminal = await transitionInbox(
      runtime,
      record.immutable.recordKey,
      "dead_letter",
      ["processing", "response_ready", "sending"],
      attemptToken,
      "outbox_terminal",
    );
    if (terminal?.state === "dead_letter") {
      await clearActiveTurn(runtime, threadId, record.immutable.recordKey, attemptToken);
    }
    return;
  }
  if (outboxState === "sending") {
    await transitionRetryableRecord(runtime, record.immutable.recordKey, attemptToken, "outbox_sending");
    return;
  }
  await storeDeliveryTarget(runtime, correlationId, {
    version: 1,
    recordKey: record.immutable.recordKey,
    threadId,
    attemptToken,
  });
  await deliverOutbox(runtime, correlationId, {
    kind: "adapter",
    adapter,
  });
}

async function queueStaticResponse(
  runtime: AirRuntime,
  lease: InboxProcessingLease,
  record: InboxRecord,
  threadId: string,
  adapter: AirIMessageAdapter,
  response: string,
): Promise<boolean> {
  const correlationId = record.immutable.correlationId;
  const ready = await ensureResponseReady(runtime, record.immutable.recordKey, lease.attemptToken);
  if (!ready) return false;
  try {
    await storeDeliveryTarget(runtime, correlationId, {
      version: 1,
      recordKey: record.immutable.recordKey,
      threadId,
      attemptToken: lease.attemptToken,
    });
    await runtime.outbox.persistValidatedResponse({
      correlationId,
      renderedResponse: response,
    });
    // Once the outbox write succeeds, the notice is durable even if this
    // immediate delivery pass has a transient problem; cron will recover it.
    await deliverOutbox(runtime, correlationId, { kind: "adapter", adapter }).catch(() => undefined);
    return true;
  } catch {
    await transitionRetryable(runtime, lease, "static_response_persist_failed");
    return false;
  }
}

async function drainOutbox(
  runtime: AirRuntime,
  adapter: AirIMessageAdapter,
  limit: number,
): Promise<void> {
  let due;
  try {
    due = await runtime.outbox.listDue(limit);
  } catch {
    logger.error("imessage.outbox_scan_failed", { category: "outbox_scan" });
    return;
  }
  logger.info("imessage.outbox_scan", { dueCount: due.length });
  for (const record of due) {
    const target = await loadDeliveryTarget(runtime, record.immutable.correlationId);
    if (!target) {
      // No provider call occurred, so this is a safe bounded retry category.
      const attempt = await runtime.outbox.beginSend(record.immutable.recordKey);
      if (attempt) {
        try {
          await attempt.markPreSendFailure(
            "delivery_target_missing",
            nextBackoffMs(attempt.record.status.attempts, runtime.clock()),
          );
        } finally {
          await attempt.release();
        }
      }
      continue;
    }
    await deliverOutbox(runtime, record.immutable.correlationId, { kind: "adapter", adapter });
  }
}

/**
 * Store the first tool output encrypted. `message.completed` deliberately
 * reads this value rather than Eve's free-form model message.
 */
export async function handleIMessageActionResult(input: {
  readonly event: unknown;
  readonly eventChannel: AirChatEventChannel;
  readonly sessionId: string;
}): Promise<void> {
  const thread = input.eventChannel.thread;
  const turnId = eventTurnId(input.event);
  if (!thread || !turnId) return;
  let runtime: AirRuntime;
  try {
    runtime = await getRuntime();
    const active = await loadActiveTurn(runtime, thread.id, input.sessionId, turnId);
    if (!active || !(await renewActiveAttempt(runtime, thread.id, active))) return;
    const result = validatedAnswerFromActionEvent(input.event);
    if (!result) return;
    const stored = await storeValidatedResult(runtime, active.correlationId, {
      version: 1,
      attemptToken: active.attemptToken,
      renderedResponse: result.rendered,
    });
    if (stored) {
      logger.info("imessage.validated_tool_result", {
        correlationId: active.correlationId,
        intent: result.intent,
        sourceCount: result.sourceCount,
        citationRetries: result.citationRetries,
      });
    }
  } catch {
    logger.error("imessage.tool_result_store_failed", { category: "tool_result_store" });
  }
}

/** Bind a turn id to the active map so late framework events cannot target a new turn. */
export async function handleIMessageTurnStarted(input: {
  readonly event: unknown;
  readonly eventChannel: AirChatEventChannel;
  readonly sessionId: string;
}): Promise<void> {
  const thread = input.eventChannel.thread;
  const turnId = eventTurnId(input.event);
  if (!thread || !turnId) return;
  try {
    const runtime = await getRuntime();
    const active = await loadActiveTurnForStart(runtime, thread.id, input.sessionId);
    if (!active || (active.turnId && active.turnId !== turnId)) return;
    if (!(await renewActiveAttempt(runtime, thread.id, active))) return;
    const replaced = await replaceActiveTurn(runtime, thread.id, active, {
      ...active,
      sessionId: input.sessionId,
      turnId,
    });
    // Belt-and-suspenders protection for the lifecycle path: the receiver
    // already starts the session with this key, but no later event may retain a
    // thread-derived or prior-attempt continuation token.
    if (replaced) {
      input.eventChannel.setContinuationToken?.(attemptScopedContinuationKey(runtime, active));
    }
  } catch {
    logger.error("imessage.turn_bind_failed", { category: "turn_bind" });
  }
}

export async function handleIMessageMessageCompleted(input: {
  readonly event: unknown;
  readonly eventChannel: AirChatEventChannel;
  readonly sessionId: string;
}): Promise<void> {
  if (eventFinishReason(input.event) === "tool-calls") return;
  const thread = input.eventChannel.thread;
  const turnId = eventTurnId(input.event);
  if (!thread || !turnId) return;
  try {
    const runtime = await getRuntime();
    const active = await loadActiveTurn(runtime, thread.id, input.sessionId, turnId);
    if (!active || !(await renewActiveAttempt(runtime, thread.id, active))) return;
    const validated = await loadValidatedResult(runtime, active.correlationId, active.attemptToken);
    const response = validated?.renderedResponse ?? AR_AGENT_CAPABILITY_BLURB;
    await queueEventResponse(runtime, active, thread, response);
  } catch {
    logger.error("imessage.completion_failed", { category: "message_completion" });
  }
}

export async function handleIMessageTurnFailed(input: {
  readonly event: unknown;
  readonly eventChannel: AirChatEventChannel;
  readonly sessionId: string;
}): Promise<void> {
  const thread = input.eventChannel.thread;
  const turnId = eventTurnId(input.event);
  if (!thread || !turnId) return;
  try {
    const runtime = await getRuntime();
    const active = await loadActiveTurn(runtime, thread.id, input.sessionId, turnId);
    if (!active || !(await renewActiveAttempt(runtime, thread.id, active))) return;
    const validated = await loadValidatedResult(runtime, active.correlationId, active.attemptToken);
    await queueEventResponse(runtime, active, thread, validated?.renderedResponse ?? AR_AGENT_RETRY_NOTICE);
  } catch {
    logger.error("imessage.turn_failure_handler_failed", { category: "turn_failure" });
  }
}

export async function handleIMessageInputRequested(input: {
  readonly event: unknown;
  readonly eventChannel: AirChatEventChannel;
  readonly sessionId: string;
}): Promise<void> {
  const thread = input.eventChannel.thread;
  const turnId = eventTurnId(input.event);
  if (!thread || !turnId) return;
  try {
    const runtime = await getRuntime();
    const active = await loadActiveTurn(runtime, thread.id, input.sessionId, turnId);
    if (!active || !(await renewActiveAttempt(runtime, thread.id, active))) return;
    // Task-mode sessions have no requestInput capability. If a future
    // framework/tool path nevertheless reports this event, deliver the fixed
    // rephrase guidance and return normally; Eve's task workflow owns the
    // terminal session.failed boundary. Adapter handler exceptions are
    // intentionally swallowed by Eve and cannot be used as cancellation.
    await queueEventResponse(runtime, active, thread, AR_AGENT_REPHRASE_GUIDANCE);
  } catch {
    logger.error("imessage.input_request_handler_failed", { category: "input_request" });
  }
}

/** A fatal session error may arrive without a turn.failed event. */
export async function handleIMessageSessionFailed(input: {
  readonly event: unknown;
  readonly eventChannel: AirChatEventChannel;
}): Promise<void> {
  const thread = input.eventChannel.thread;
  const sessionId = eventSessionId(input.event);
  if (!thread || !sessionId) return;
  try {
    const runtime = await getRuntime();
    const active = await loadActiveTurn(runtime, thread.id, sessionId);
    // A prior session can finish while a recovered attempt is still in the
    // receive/start race. Do not let a session-only event send a fallback
    // until the exact turn id has bound this durable attempt.
    if (!active || !active.turnId || !(await renewActiveAttempt(runtime, thread.id, active))) return;
    const validated = await loadValidatedResult(runtime, active.correlationId, active.attemptToken);
    await queueEventResponse(runtime, active, thread, validated?.renderedResponse ?? AR_AGENT_RETRY_NOTICE);
  } catch {
    logger.error("imessage.session_failure_handler_failed", { category: "session_failure" });
  }
}

/**
 * A defensive terminal fallback for sessions that end without a usable
 * message.completed event. The durable outbox makes this idempotent with the
 * normal completion handler.
 */
export async function handleIMessageSessionCompleted(input: {
  readonly eventChannel: AirChatEventChannel;
  readonly sessionId: string;
}): Promise<void> {
  const thread = input.eventChannel.thread;
  if (!thread) return;
  try {
    const runtime = await getRuntime();
    const active = await loadActiveTurn(runtime, thread.id, input.sessionId);
    if (!active || !active.turnId || !(await renewActiveAttempt(runtime, thread.id, active))) return;
    const validated = await loadValidatedResult(runtime, active.correlationId, active.attemptToken);
    await queueEventResponse(runtime, active, thread, validated?.renderedResponse ?? AR_AGENT_CAPABILITY_BLURB);
  } catch {
    logger.error("imessage.session_completion_handler_failed", { category: "session_completion" });
  }
}

/**
 * Events that carry no user-visible output still count as proof that Eve is
 * actively advancing this exact fenced turn. They renew only the durable
 * attempt/active-map leases and deliberately never post or mutate inbox state.
 */
export async function handleIMessageProgressEvent(input: {
  readonly event: unknown;
  readonly eventChannel: AirChatEventChannel;
  readonly sessionId: string;
}): Promise<void> {
  const thread = input.eventChannel.thread;
  const turnId = eventTurnId(input.event);
  if (!thread || !turnId) return;
  try {
    const runtime = await getRuntime();
    const active = await loadActiveTurn(runtime, thread.id, input.sessionId, turnId);
    if (!active) return;
    await renewActiveAttempt(runtime, thread.id, active);
  } catch {
    logger.error("imessage.progress_heartbeat_failed", { category: "progress_heartbeat" });
  }
}

async function queueEventResponse(
  runtime: AirRuntime,
  active: ActiveTurn,
  thread: AirChatThread,
  response: string,
): Promise<void> {
  const ready = await ensureResponseReady(runtime, active.recordKey, active.attemptToken);
  if (!ready) return;
  const target = await loadDeliveryTarget(runtime, active.correlationId);
  if (
    !target ||
    target.recordKey !== active.recordKey ||
    target.attemptToken !== active.attemptToken ||
    target.threadId !== thread.id
  ) {
    await transitionRetryableRecord(runtime, active.recordKey, active.attemptToken, "delivery_target_missing");
    return;
  }
  // Several lifecycle callbacks can race at a terminal boundary. The durable
  // outbox is the response arbiter: whichever validated/fallback response is
  // committed first wins, and later callbacks only drain that exact record.
  try {
    await runtime.outbox.persistValidatedResponse({
      correlationId: active.correlationId,
      renderedResponse: response,
    });
  } catch {
    const existing = await runtime.outbox.getRecord(outboxRecordKey(active.correlationId));
    if (!existing) {
      await transitionRetryableRecord(runtime, active.recordKey, active.attemptToken, "terminal_response_persist_failed");
      return;
    }
  }
  logger.info("imessage.response_queued", {
    correlationId: active.correlationId,
    sessionDurationMs: Math.max(0, runtime.clock() - active.startedAtMs),
  });
  try {
    await deliverOutbox(runtime, active.correlationId, { kind: "chat_thread", thread });
  } catch {
    await transitionRetryableRecord(runtime, active.recordKey, active.attemptToken, "terminal_response_delivery_failed");
  }
}

async function deliverOutbox(
  runtime: AirRuntime,
  correlationId: CorrelationId,
  transport: SendTransport,
): Promise<void> {
  const target = await loadDeliveryTarget(runtime, correlationId);
  if (!target) return;
  const ready = await ensureResponseReady(runtime, target.recordKey, target.attemptToken);
  if (!ready) return;

  const attempt = await runtime.outbox.beginSend(outboxRecordKey(correlationId));
  if (!attempt) return;
  const sendStartedAtMs = runtime.clock();
  try {
    const sending = await transitionInbox(
      runtime,
      target.recordKey,
      "sending",
      ["response_ready"],
      target.attemptToken,
    );
    if (!sending || sending.state !== "sending") {
      await attempt.markPreSendFailure(
        "inbox_send_state_conflict",
        nextBackoffMs(attempt.record.status.attempts, runtime.clock()),
      );
      await transitionRetryableRecord(runtime, target.recordKey, target.attemptToken, "inbox_send_state_conflict");
      return;
    }

    if (transport.kind === "unavailable") {
      await attempt.markPreSendFailure(
        "chat_thread_unavailable",
        nextBackoffMs(attempt.record.status.attempts, runtime.clock()),
      );
      await transitionRetryableRecord(runtime, target.recordKey, target.attemptToken, "chat_thread_unavailable");
      return;
    }

    // A Chat SDK event always uses the rebuilt thread so the normal success
    // path remains `channel.thread.post`. Direct adapter sends are recovery or
    // pre-Eve static notices only and still use the official Photon adapter.
    if (transport.kind === "chat_thread") {
      await transport.thread.post(attempt.renderedResponse);
    } else {
      await transport.adapter.postMessage(target.threadId, attempt.renderedResponse);
    }
    await attempt.markSent();
    const sent = await transitionInbox(runtime, target.recordKey, "sent", ["sending"], target.attemptToken);
    if (sent?.state !== "sent") return;
    await cleanupDeliveredTurn(runtime, correlationId, target.threadId, target.recordKey, target.attemptToken);
    logger.info("imessage.response_sent", {
      correlationId,
      attempt: attempt.record.status.attempts,
      photonLatencyMs: Math.max(0, runtime.clock() - sendStartedAtMs),
    });
  } catch {
    // Any throw after an adapter/thread post is ambiguous: Photon has no
    // outbound idempotency key, so never turn this into an automatic retry.
    await attempt.markSendUnknown("photon_send_unknown").catch(() => undefined);
    await transitionInbox(
      runtime,
      target.recordKey,
      "send_unknown",
      ["sending"],
      target.attemptToken,
      "photon_send_unknown",
    ).catch(() => undefined);
    await clearActiveTurn(runtime, target.threadId, target.recordKey, target.attemptToken);
    logger.error("imessage.response_send_unknown", {
      correlationId,
      category: "photon_send_unknown",
      photonLatencyMs: Math.max(0, runtime.clock() - sendStartedAtMs),
    });
  } finally {
    await attempt.release();
  }
}

async function ensureResponseReady(
  runtime: AirRuntime,
  recordKey: string,
  attemptToken: string,
): Promise<InboxRecord | null> {
  const current = await runtime.inbox.getRecord(recordKey);
  if (!current) return null;
  if (current.state === "response_ready") return current;
  if (current.state !== "processing" && current.state !== "session_started") return null;
  return transitionInbox(
    runtime,
    recordKey,
    "response_ready",
    ["processing", "session_started"],
    attemptToken,
  );
}

async function transitionInbox(
  runtime: AirRuntime,
  recordKey: string,
  target: InboxState,
  expectedStates: readonly InboxState[],
  attemptToken: string,
  failureCategory?: string,
): Promise<InboxRecord | null> {
  try {
    const transitioned = await runtime.inbox.transition(recordKey, target, {
      expectedStates,
      expectedAttemptToken: attemptToken,
      ...(failureCategory ? { failureCategory } : {}),
    });
    if (target === "dead_letter" && transitioned?.state === "dead_letter") {
      logger.error("imessage.dead_letter", {
        correlationId: transitioned.immutable.correlationId,
        category: failureCategory ?? "unknown_failure",
        attempt: transitioned.status.attempts,
        inboxAgeMs: Math.max(0, runtime.clock() - transitioned.immutable.receivedAtMs),
      });
    }
    return transitioned;
  } catch {
    return null;
  }
}

async function transitionTerminal(
  runtime: AirRuntime,
  lease: InboxProcessingLease | null,
  category: string,
): Promise<void> {
  if (!lease) return;
  const transitioned = await lease.transition("dead_letter", {
    expectedStates: ["processing", "session_started", "response_ready", "sending"],
    failureCategory: category,
  }).catch(() => undefined);
  if (transitioned?.state === "dead_letter") {
    logger.error("imessage.dead_letter", {
      correlationId: transitioned.immutable.correlationId,
      category,
      attempt: transitioned.status.attempts,
      inboxAgeMs: Math.max(0, runtime.clock() - transitioned.immutable.receivedAtMs),
    });
  }
}

async function transitionRetryable(
  runtime: AirRuntime,
  lease: InboxProcessingLease | null,
  category: string,
): Promise<void> {
  if (!lease) return;
  const attempts = Math.max(1, lease.record.status.attempts);
  if (attempts >= 3) {
    await transitionTerminal(runtime, lease, `${category}_retries_exhausted`);
    return;
  }
  const transitioned = await lease.transition("failed_retryable", {
    expectedStates: ["processing", "session_started", "response_ready", "sending"],
    failureCategory: category,
    nextAttemptAtMs: nextBackoffMs(attempts, runtime.clock()),
  }).catch(() => undefined);
  if (transitioned?.state === "failed_retryable") {
    logger.warn("imessage.retry_scheduled", {
      correlationId: transitioned.immutable.correlationId,
      category,
      attempt: transitioned.status.attempts,
    });
  }
}

async function transitionRetryableRecord(
  runtime: AirRuntime,
  recordKey: string,
  attemptToken: string,
  category: string,
): Promise<void> {
  const current = await runtime.inbox.getRecord(recordKey);
  if (!current || current.state === "failed_retryable") return;
  if (current.status.attempts >= 3) {
    const transitioned = await runtime.inbox.transition(recordKey, "dead_letter", {
      expectedStates: ["processing", "session_started", "response_ready", "sending"],
      expectedAttemptToken: attemptToken,
      failureCategory: `${category}_retries_exhausted`,
    }).catch(() => undefined);
    if (transitioned?.state === "dead_letter") {
      logger.error("imessage.dead_letter", {
        correlationId: transitioned.immutable.correlationId,
        category: `${category}_retries_exhausted`,
        attempt: transitioned.status.attempts,
        inboxAgeMs: Math.max(0, runtime.clock() - transitioned.immutable.receivedAtMs),
      });
    }
    return;
  }
  const transitioned = await runtime.inbox.transition(recordKey, "failed_retryable", {
    expectedStates: ["processing", "session_started", "response_ready", "sending"],
    expectedAttemptToken: attemptToken,
    failureCategory: category,
    nextAttemptAtMs: nextBackoffMs(current.status.attempts, runtime.clock()),
  }).catch(() => undefined);
  if (transitioned?.state === "failed_retryable") {
    logger.warn("imessage.retry_scheduled", {
      correlationId: transitioned.immutable.correlationId,
      category,
      attempt: transitioned.status.attempts,
    });
  }
}

async function cleanupDeliveredTurn(
  runtime: AirRuntime,
  correlationId: CorrelationId,
  threadId: string,
  recordKey: string,
  attemptToken: string,
): Promise<void> {
  await Promise.all([
    clearValidatedResult(runtime, correlationId, attemptToken),
    clearDeliveryTarget(runtime, correlationId, attemptToken),
    clearActiveTurn(runtime, threadId, recordKey, attemptToken),
  ]);
}

async function bestEffortInboundSignals(
  adapter: AirIMessageAdapter,
  threadId: string,
  messageId: string,
): Promise<void> {
  await Promise.all([
    adapter.markRead?.(threadId, messageId).catch(() => undefined),
    adapter.startTyping?.(threadId, "Working...").catch(() => undefined),
  ]);
}

function toInboxContentType(contentType: string | null): InboxContentType {
  if (contentType === "text") return "text";
  if (contentType === "reaction") return "reaction";
  if (contentType === "attachment") return "attachment";
  return "unknown";
}

function isCapabilityOnlyMessage(value: string): boolean {
  const normalized = value.trim().toLowerCase().replace(/\s+/g, " ");
  if (/^(?:hi|hello|hey|yo|sup|good (?:morning|afternoon|evening))[!.? ]*$/.test(normalized)) {
    return true;
  }
  if (normalized.length > 180) return false;
  return /^(?:what can you do|what do you do|how can you help|help|who are you|capabilities)\??$/.test(normalized);
}

function nextBackoffMs(attempts: number, now = Date.now()): number {
  const boundedAttempt = Math.max(1, Math.min(3, attempts));
  return now + 1_000 * (2 ** (boundedAttempt - 1));
}

function activeTurnKey(runtime: AirRuntime, threadId: string): string {
  return `air:active-turn:v1:${opaqueThreadId(runtime, threadId)}`;
}

function opaqueThreadId(runtime: AirRuntime, threadId: string): string {
  return deriveOpaqueId(runtime.environment.idHashSecret as string, "chat-sdk-thread", threadId);
}

function attemptScopedContinuationKey(runtime: AirRuntime, active: ActiveTurn): string {
  return deriveOpaqueId(
    runtime.environment.idHashSecret as string,
    "eve-imessage-attempt",
    `${active.recordKey}:${active.attemptToken}`,
  );
}

function validatedResultKey(correlationId: CorrelationId): string {
  return `air:validated-result:v1:${correlationId}`;
}

function deliveryTargetKey(correlationId: CorrelationId): string {
  return `air:delivery-target:v1:${correlationId}`;
}

function noticeKeyPrefix(runtime: AirRuntime, kind: "access" | "group"): string {
  return `air:notice:v1:${kind}:${deriveOpaqueId(runtime.environment.idHashSecret as string, "notice", kind)}`;
}

async function claimNotice(runtime: AirRuntime, key: string): Promise<NoticeClaim | null> {
  const token = randomBytes(24).toString("base64url");
  const claimed = await runtime.redis.set(key, token, { nx: true, ex: NOTICE_TTL_SECONDS });
  return claimed === "OK" ? { key, token } : null;
}

async function releaseNotice(runtime: AirRuntime, claim: NoticeClaim): Promise<void> {
  await runtime.redis.eval(COMPARE_AND_DELETE_LUA, {
    keys: [claim.key],
    arguments: [claim.token],
  }).catch(() => undefined);
}

async function storeActiveTurn(runtime: AirRuntime, threadId: string, active: ActiveTurn): Promise<boolean> {
  const key = activeTurnKey(runtime, threadId);
  return (
    await runtime.redis.set(
      key,
      JSON.stringify(runtime.cipher.encrypt(active, key)),
      { nx: true, px: ACTIVE_TURN_TTL_MS },
    )
  ) === "OK";
}

async function bindActiveTurnSession(
  runtime: AirRuntime,
  threadId: string,
  active: ActiveTurn,
  sessionId: string,
): Promise<boolean> {
  if (!isValidSessionId(sessionId)) return false;
  // `receive()` returns after Eve starts the workflow, so a turn.started
  // callback may have already bound the same session/turn. Re-read by durable
  // attempt capability rather than treating that benign ordering as failure.
  const current = await loadActiveTurnCandidate(runtime, threadId);
  if (!current || !sameActiveAttempt(current, active)) return false;
  if (current.sessionId && current.sessionId !== sessionId) return false;
  if (current.receiveBound) return true;
  return replaceActiveTurn(runtime, threadId, current, {
    ...current,
    sessionId,
    receiveBound: true,
  });
}

/**
 * Refresh both pieces of cross-invocation ownership before a lifecycle event
 * can act: the physical per-thread lease fences queue execution, while the
 * encrypted active map fences Eve session/turn callbacks.
 */
async function renewActiveAttempt(
  runtime: AirRuntime,
  threadId: string,
  active: ActiveTurn,
): Promise<boolean> {
  if (!(await runtime.inbox.heartbeatAttempt(active.recordKey, active.attemptToken))) {
    return false;
  }
  return replaceActiveTurn(runtime, threadId, active, active);
}

/**
 * Atomically replaces an active turn only when the exact previous encrypted
 * record is still current. This fences delayed Eve events from an expired
 * worker so they cannot bind a recovered attempt on the same chat thread.
 */
async function replaceActiveTurn(
  runtime: AirRuntime,
  threadId: string,
  expected: ActiveTurn,
  next: ActiveTurn,
): Promise<boolean> {
  if (!sameActiveAttempt(expected, next)) return false;
  const key = activeTurnKey(runtime, threadId);
  const raw = await runtime.redis.get(key);
  const current = raw ? decryptStored<ActiveTurn>(runtime, raw, key) : null;
  if (!raw || !current || !sameActiveTurn(current, expected)) return false;
  const nextRaw = JSON.stringify(runtime.cipher.encrypt(next, key));
  const replaced = await runtime.redis.eval(COMPARE_AND_SET_WITH_PX_LUA, {
    keys: [key],
    arguments: [raw, nextRaw, String(ACTIVE_TURN_TTL_MS)],
  });
  return Number(replaced) === 1;
}

async function loadActiveTurn(
  runtime: AirRuntime,
  threadId: string,
  sessionId: string,
  turnId?: string,
): Promise<ActiveTurn | null> {
  if (!isValidSessionId(sessionId)) return null;
  const active = await loadActiveTurnCandidate(runtime, threadId);
  if (!active || active.sessionId !== sessionId || !active.receiveBound) return null;
  // Non-start events must have both identities already bound. Treating an
  // unbound turn as a wildcard would let a delayed prior session claim a
  // recovered record after its original lease expires.
  if (turnId !== undefined && active.turnId !== turnId) return null;
  return active;
}

/** Allows `turn.started` to pre-bind its session while receive() is resolving. */
async function loadActiveTurnForStart(
  runtime: AirRuntime,
  threadId: string,
  sessionId: string,
): Promise<ActiveTurn | null> {
  if (!isValidSessionId(sessionId)) return null;
  const active = await loadActiveTurnCandidate(runtime, threadId);
  if (!active) return null;
  if (active.sessionId !== undefined && active.sessionId !== sessionId) return null;
  return active;
}

async function loadActiveTurnCandidate(
  runtime: AirRuntime,
  threadId: string,
): Promise<ActiveTurn | null> {
  const key = activeTurnKey(runtime, threadId);
  const raw = await runtime.redis.get(key);
  const active = raw ? decryptStored<ActiveTurn>(runtime, raw, key) : null;
  return isValidActiveTurn(active, runtime, threadId) ? active : null;
}

async function clearActiveTurn(
  runtime: AirRuntime,
  threadId: string,
  recordKey: string,
  attemptToken: string,
): Promise<void> {
  const key = activeTurnKey(runtime, threadId);
  const raw = await runtime.redis.get(key);
  if (!raw) return;
  const active = decryptStored<ActiveTurn>(runtime, raw, key);
  if (!active || active.recordKey !== recordKey || active.attemptToken !== attemptToken) return;
  await runtime.redis.eval(COMPARE_AND_DELETE_LUA, { keys: [key], arguments: [raw] });
}

async function storeValidatedResult(
  runtime: AirRuntime,
  correlationId: CorrelationId,
  result: StoredValidatedResult,
): Promise<boolean> {
  const key = validatedResultKey(correlationId);
  const serialized = JSON.stringify(runtime.cipher.encrypt(result, key));
  const created = await runtime.redis.set(key, serialized, {
    nx: true,
    ex: ENCRYPTED_TRANSIENT_TTL_SECONDS,
  });
  if (created === "OK") return true;
  const raw = await runtime.redis.get(key);
  const existing = raw ? decryptStored<StoredValidatedResult>(runtime, raw, key) : null;
  if (!raw || !isValidStoredResult(existing)) {
    throw new Error("durable validated result is unavailable");
  }
  // Preserve the first valid tool output for the current attempt. A recovered
  // attempt can fence and replace a stale encrypted result atomically.
  if (existing.attemptToken === result.attemptToken) return true;
  const replaced = await runtime.redis.eval(COMPARE_AND_SET_WITH_PX_LUA, {
    keys: [key],
    arguments: [raw, serialized, String(ENCRYPTED_TRANSIENT_TTL_SECONDS * 1_000)],
  });
  return Number(replaced) === 1;
}

async function loadValidatedResult(
  runtime: AirRuntime,
  correlationId: CorrelationId,
  attemptToken: string,
): Promise<StoredValidatedResult | null> {
  const key = validatedResultKey(correlationId);
  const raw = await runtime.redis.get(key);
  const value = raw ? decryptStored<StoredValidatedResult>(runtime, raw, key) : null;
  return isValidStoredResult(value) && value.attemptToken === attemptToken ? value : null;
}

async function rebindValidatedResult(
  runtime: AirRuntime,
  correlationId: CorrelationId,
  attemptToken: string,
): Promise<void> {
  const key = validatedResultKey(correlationId);
  const raw = await runtime.redis.get(key);
  const existing = raw ? decryptStored<StoredValidatedResult>(runtime, raw, key) : null;
  if (!raw || !isValidStoredResult(existing) || existing.attemptToken === attemptToken) return;
  const nextRaw = JSON.stringify(runtime.cipher.encrypt({ ...existing, attemptToken }, key));
  await runtime.redis.eval(COMPARE_AND_SET_WITH_PX_LUA, {
    keys: [key],
    arguments: [raw, nextRaw, String(ENCRYPTED_TRANSIENT_TTL_SECONDS * 1_000)],
  }).catch(() => undefined);
}

async function storeDeliveryTarget(
  runtime: AirRuntime,
  correlationId: CorrelationId,
  target: StoredDeliveryTarget,
): Promise<void> {
  const key = deliveryTargetKey(correlationId);
  const serialized = JSON.stringify(runtime.cipher.encrypt(target, key));
  const created = await runtime.redis.set(key, serialized, {
    nx: true,
    ex: ENCRYPTED_TRANSIENT_TTL_SECONDS,
  });
  if (created === "OK") return;
  const raw = await runtime.redis.get(key);
  const existing = raw ? decryptStored<StoredDeliveryTarget>(runtime, raw, key) : null;
  if (!raw || !isValidDeliveryTarget(existing)) {
    throw new Error("durable delivery target is unavailable");
  }
  if (existing.attemptToken === target.attemptToken) {
    if (existing.recordKey !== target.recordKey || existing.threadId !== target.threadId) {
      throw new Error("durable delivery target does not match active attempt");
    }
    return;
  }
  // Retry/recovery has minted a new ownership capability for the same inbox
  // record. Fence-rebind the target so outbox recovery advances only it.
  const replaced = await runtime.redis.eval(COMPARE_AND_SET_WITH_PX_LUA, {
    keys: [key],
    arguments: [raw, serialized, String(ENCRYPTED_TRANSIENT_TTL_SECONDS * 1_000)],
  });
  if (Number(replaced) !== 1) throw new Error("durable delivery target rebind conflict");
}

async function loadDeliveryTarget(
  runtime: AirRuntime,
  correlationId: CorrelationId,
): Promise<StoredDeliveryTarget | null> {
  const key = deliveryTargetKey(correlationId);
  const raw = await runtime.redis.get(key);
  const target = raw ? decryptStored<StoredDeliveryTarget>(runtime, raw, key) : null;
  return isValidDeliveryTarget(target) ? target : null;
}

async function clearValidatedResult(
  runtime: AirRuntime,
  correlationId: CorrelationId,
  attemptToken: string,
): Promise<void> {
  const key = validatedResultKey(correlationId);
  const raw = await runtime.redis.get(key);
  const result = raw ? decryptStored<StoredValidatedResult>(runtime, raw, key) : null;
  if (!raw || !isValidStoredResult(result) || result.attemptToken !== attemptToken) return;
  await runtime.redis.eval(COMPARE_AND_DELETE_LUA, { keys: [key], arguments: [raw] });
}

async function clearDeliveryTarget(
  runtime: AirRuntime,
  correlationId: CorrelationId,
  attemptToken: string,
): Promise<void> {
  const key = deliveryTargetKey(correlationId);
  const raw = await runtime.redis.get(key);
  const target = raw ? decryptStored<StoredDeliveryTarget>(runtime, raw, key) : null;
  if (!raw || !isValidDeliveryTarget(target) || target.attemptToken !== attemptToken) return;
  await runtime.redis.eval(COMPARE_AND_DELETE_LUA, { keys: [key], arguments: [raw] });
}

function decryptStored<T>(runtime: AirRuntime, raw: string, key: string): T | null {
  try {
    return runtime.cipher.decrypt<T>(JSON.parse(raw) as EncryptedPayload, key);
  } catch {
    return null;
  }
}

function sessionIdFromReceive(value: AirIMessageSession): string | null {
  const candidate = (value as { id?: unknown } | null | undefined)?.id;
  return isValidSessionId(candidate) ? candidate : null;
}

function isValidActiveTurn(
  value: ActiveTurn | null,
  runtime: AirRuntime,
  threadId: string,
): value is ActiveTurn {
  return Boolean(
    value &&
      value.version === 1 &&
      value.threadIdHash === opaqueThreadId(runtime, threadId) &&
      typeof value.recordKey === "string" &&
      value.recordKey.length > 0 &&
      isAttemptToken(value.attemptToken) &&
      typeof value.receiveBound === "boolean" &&
      (value.sessionId === undefined || isValidSessionId(value.sessionId)) &&
      (value.turnId === undefined || isValidTurnId(value.turnId)),
  );
}

function sameActiveAttempt(left: ActiveTurn, right: ActiveTurn): boolean {
  return (
    left.recordKey === right.recordKey &&
    left.correlationId === right.correlationId &&
    left.threadIdHash === right.threadIdHash &&
    left.attemptToken === right.attemptToken &&
    left.startedAtMs === right.startedAtMs
  );
}

function sameActiveTurn(left: ActiveTurn, right: ActiveTurn): boolean {
  return (
    sameActiveAttempt(left, right) &&
    left.sessionId === right.sessionId &&
    left.turnId === right.turnId &&
    left.receiveBound === right.receiveBound
  );
}

function isValidStoredResult(value: StoredValidatedResult | null): value is StoredValidatedResult {
  return Boolean(
    value &&
      value.version === 1 &&
      isAttemptToken(value.attemptToken) &&
      isValidRenderedResponse(value.renderedResponse),
  );
}

function isValidDeliveryTarget(value: StoredDeliveryTarget | null): value is StoredDeliveryTarget {
  return Boolean(
    value &&
      value.version === 1 &&
      typeof value.recordKey === "string" &&
      value.recordKey.length > 0 &&
      typeof value.threadId === "string" &&
      value.threadId.length > 0 &&
      isAttemptToken(value.attemptToken),
  );
}

function isAttemptToken(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9_-]{32,128}$/.test(value);
}

function isValidSessionId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 512 && !value.includes("\u0000");
}

function isValidTurnId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 512 && !value.includes("\u0000");
}

type ValidatedActionAnswer = {
  readonly rendered: string;
  readonly intent?: "entity_lookup" | "thematic" | "comparative" | "aggregative";
  readonly sourceCount?: number;
  readonly citationRetries?: number;
};

function validatedAnswerFromActionEvent(event: unknown): ValidatedActionAnswer | null {
  const data = eventPayload(event);
  if (!data) return null;
  const details = data as { status?: unknown; result?: unknown };
  if (details.status !== "completed" || !details.result || typeof details.result !== "object") return null;
  const result = details.result as { kind?: unknown; toolName?: unknown; isError?: unknown; output?: unknown };
  if (result.kind !== "tool-result" || result.toolName !== "answer_music_question" || result.isError === true) {
    return null;
  }
  if (!result.output || typeof result.output !== "object") return null;
  const output = result.output as {
    kind?: unknown;
    rendered?: unknown;
    reason?: unknown;
    intent?: unknown;
    sourceCount?: unknown;
    citationRetries?: unknown;
  };
  // Citation-invalid model output is represented by a typed tool result with
  // no caller-controlled prose. Map it to the fixed static notice instead of
  // treating a deliberately rejected answer like a transient turn failure.
  if (output.kind === "air.capability-fallback.v1") {
    return output.reason === "citation_validation"
      ? Object.freeze({ rendered: AR_AGENT_CAPABILITY_BLURB })
      : null;
  }
  if (output.kind !== "air.grounded-answer.v1" || !isValidRenderedResponse(output.rendered)) return null;
  const answer: ValidatedActionAnswer = { rendered: output.rendered.trim() };
  if (
    output.intent === "entity_lookup" ||
    output.intent === "thematic" ||
    output.intent === "comparative" ||
    output.intent === "aggregative"
  ) {
    (answer as { intent?: ValidatedActionAnswer["intent"] }).intent = output.intent;
  }
  if (Number.isInteger(output.sourceCount) && (output.sourceCount as number) >= 0 && (output.sourceCount as number) <= 4) {
    (answer as { sourceCount?: number }).sourceCount = output.sourceCount as number;
  }
  if (Number.isInteger(output.citationRetries) && (output.citationRetries as number) >= 0 && (output.citationRetries as number) <= 3) {
    (answer as { citationRetries?: number }).citationRetries = output.citationRetries as number;
  }
  return Object.freeze(answer);
}

function isValidRenderedResponse(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0 && value.length <= 4_096;
}

function eventTurnId(event: unknown): string | undefined {
  const data = eventPayload(event);
  if (!data) return undefined;
  const turnId = (data as { turnId?: unknown }).turnId;
  return typeof turnId === "string" && turnId.length > 0 && turnId.length <= 512 ? turnId : undefined;
}

function eventSessionId(event: unknown): string | undefined {
  const data = eventPayload(event);
  if (!data) return undefined;
  const sessionId = (data as { sessionId?: unknown }).sessionId;
  return isValidSessionId(sessionId) ? sessionId : undefined;
}

function eventFinishReason(event: unknown): string | undefined {
  const data = eventPayload(event);
  if (!data) return undefined;
  const reason = (data as { finishReason?: unknown }).finishReason;
  return typeof reason === "string" ? reason : undefined;
}

/**
 * Eve channel handlers receive an event's `data` object, while isolated
 * fixtures often carry the enclosing stream event. Support both without ever
 * treating model text as a delivery candidate.
 */
function eventPayload(event: unknown): Record<string, unknown> | null {
  if (!event || typeof event !== "object") return null;
  const direct = event as Record<string, unknown>;
  if (direct.data && typeof direct.data === "object") {
    return direct.data as Record<string, unknown>;
  }
  return direct;
}

/** CRON_SECRET-protected manual recovery path. It never exposes queue contents. */
export async function drainFromOperatorRoute(
  request: Request,
  options: Omit<DurableDrainOptions, "limit">,
): Promise<Response> {
  if (request.method !== "POST") return emptyResponse(405);
  const secret = process.env.CRON_SECRET;
  if (!secret || !constantTimeAuthorizationMatches(request.headers.get("authorization"), secret)) {
    return emptyResponse(401);
  }
  await drainDurableInbox(options);
  return emptyResponse(204);
}

function constantTimeAuthorizationMatches(header: string | null, secret: string): boolean {
  const expected = Buffer.from(`Bearer ${secret}`, "utf8");
  const supplied = Buffer.from(header ?? "", "utf8");
  return expected.byteLength === supplied.byteLength && timingSafeEqual(expected, supplied);
}

/** A deliberately small public status surface; it reveals no route internals. */
export function publicStatusResponse(): Response {
  return jsonResponse({ name: "AR Agent", status: "transcript-grounded iMessage research" }, 200);
}

/** No upstream hostname, secret name, or configuration inventory is exposed here. */
export function healthResponse(): Response {
  let ok = true;
  try {
    assertQueueEnvironment(loadAirEnvironment());
  } catch {
    ok = false;
  }
  return jsonResponse({ ok, version: deploymentVersion() }, ok ? 200 : 503);
}

function deploymentVersion(): string {
  const candidate = process.env.VERCEL_GIT_COMMIT_SHA?.trim() || process.env.GIT_COMMIT_SHA?.trim() || "dev";
  return /^[a-zA-Z0-9._-]{1,100}$/.test(candidate) ? candidate : "unknown";
}

function jsonResponse(body: Record<string, unknown>, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}

function emptyResponse(status: number): Response {
  return new Response(null, { status, headers: { "cache-control": "no-store" } });
}

function webhookInputResponse(error: unknown): Response {
  if (error instanceof WebhookInputError) return emptyResponse(error.status);
  logger.warn("imessage.webhook_rejected", { category: "invalid_payload", status: 400 });
  return emptyResponse(400);
}

function classifyError(_error: unknown): string {
  // Intentionally avoid serializing external error messages: drivers and SDKs
  // may include secrets, hosts, identifiers, or provider payload fragments.
  return "operation_failed";
}
