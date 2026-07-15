import { createRedisState } from "@chat-adapter/state-redis";
import { createiMessageAdapter } from "@photon-ai/chat-adapter-imessage";
import { ConsoleLogger, type Lock, type QueueEntry, type StateAdapter } from "chat";
import { defineChannel, type Channel, type RouteDefinition } from "eve/channels";
import {
  chatSdkChannel,
  type ChatSdkInstrumentationMetadata,
} from "eve/channels/chat-sdk";
import {
  handleIMessageActionResult,
  handleIMessageInputRequested,
  handleIMessageMessageCompleted,
  handleIMessageProgressEvent,
  handleIMessageSessionCompleted,
  handleIMessageSessionFailed,
  handleIMessageTurnStarted,
  handleIMessageTurnFailed,
  resolveIMessageDeliveryThreadId,
  type AirChatEventChannel,
  type AirChatThread,
} from "../../lib/imessage-channel-runtime.js";
import { deriveOpaqueId } from "../../lib/security.js";

/** The only Chat SDK adapter configured for Air V1. */
export const IMESSAGE_ADAPTER_NAME = "imessage" as const;

/**
 * The Chat SDK bridge's generated adapter route is pinned to an unregistered
 * path. Photon must only be configured for the durable public ingress route;
 * this route remains solely to retain the official adapter's HMAC-verifying
 * webhook surface without creating an alternate Eve dispatch path.
 */
export const IMESSAGE_ADAPTER_INTERNAL_ROUTE = "/_internal/chat-sdk/imessage";

export const AR_AGENT_NAME = "AR Agent";

/**
 * Chat SDK, its Redis state adapter, and the Photon adapter can include raw
 * provider errors or physical thread ids in their default console logging.
 * Air emits its own allowlisted operational telemetry, so framework logging is
 * deliberately silent at every boundary rather than becoming a second log
 * pipeline that can bypass Air's redaction contract.
 */
export const AIR_CHAT_LOGGER = new ConsoleLogger("silent");

/**
 * The ingress channel constructs this after hashing the normalized sender
 * address. No raw phone number or email handle may be passed as `principalId`.
 */
export interface AirIMessageAuth {
  authenticator: "air-imessage";
  principalId: string;
  principalType: "user";
  attributes?: Record<string, string | readonly string[]>;
}

export interface ReceiveIMessageTurnInput {
  auth: AirIMessageAuth;
  message: string;
  deliveryTargetId: string;
}

/**
 * Each durable inbox attempt gets a distinct opaque continuation key. The
 * physical Chat SDK thread is intentionally separate from this key: a retry
 * for the same iMessage thread must start a fresh Eve task, never deliver into
 * a session parked by an expired worker.
 */
export interface AttemptScopedIMessageTarget {
  readonly adapterName: typeof IMESSAGE_ADAPTER_NAME;
  /** Correlation HMAC; the physical thread remains in encrypted Redis only. */
  readonly deliveryTargetId: string;
  readonly continuationKey: string;
}

/**
 * Eve durable state must be safe to serialize into Vercel Workflow storage.
 * A delivery target is only a correlation HMAC; its physical iMessage thread
 * remains in Air's separately encrypted Redis record.
 */
export interface OpaqueIMessageWorkflowState extends Record<string, unknown> {
  deliveryTargetId: string | null;
}

export type AttemptScopedIMessageChannel = Channel<
  OpaqueIMessageWorkflowState,
  AttemptScopedIMessageTarget,
  ChatSdkInstrumentationMetadata
>;

type OpaqueIMessageChannelContext = {
  readonly thread: AirChatThread | null;
  state: OpaqueIMessageWorkflowState;
};

const CONTINUATION_KEY_PATTERN = /^[a-f0-9]{64}$/;
const OPAQUE_DELIVERY_TARGET_PATTERN = /^[a-f0-9]{64}$/;
export const AR_AGENT_IMESSAGE_WORKFLOW_TITLE = "AR Agent iMessage";

const RUNTIME_ENVIRONMENT_NAMES = [
  "IMESSAGE_PROJECT_ID",
  "IMESSAGE_PROJECT_SECRET",
  "IMESSAGE_WEBHOOK_SECRET",
  "REDIS_URL",
  "AIR_ID_HASH_SECRET",
] as const;

const BUILD_PLACEHOLDER_REDIS_URL = "redis://air-build-placeholder.invalid:6379";

function environmentValue(name: string): string | undefined {
  const value = process.env[name]?.trim();
  return value || undefined;
}

function buildSafeAdapterValue(name: string): string {
  return environmentValue(name) ?? `air-build-placeholder-${name.toLowerCase()}`;
}

/**
 * Module discovery during `eve build` must not need production secrets. Every
 * public ingress and recovery entrypoint calls this before starting work, so a
 * deployed function with missing configuration fails before it can accept or
 * send a message. The placeholders only make static channel discovery safe.
 */
export function assertIMessageRuntimeConfiguration(): void {
  const missing = RUNTIME_ENVIRONMENT_NAMES.filter(
    (name) => environmentValue(name) === undefined,
  );

  if (missing.length > 0) {
    throw new Error(
      `AR Agent iMessage runtime configuration is incomplete (${missing.join(", ")}).`,
    );
  }
}

/**
 * The official Photon adapter remains the sole transport implementation. It
 * is exported for the durable ingress worker's `encodeThreadId`, best-effort
 * read receipts, and bounded pre-Eve notices; this module never calls its
 * webhook handler directly.
 */
export const imessageAdapter = createiMessageAdapter({
  projectId: buildSafeAdapterValue("IMESSAGE_PROJECT_ID"),
  projectSecret: buildSafeAdapterValue("IMESSAGE_PROJECT_SECRET"),
  webhookSecret: buildSafeAdapterValue("IMESSAGE_WEBHOOK_SECRET"),
  logger: AIR_CHAT_LOGGER,
});

const rawState = createRedisState({
  keyPrefix: "air:chat",
  url: environmentValue("REDIS_URL") ?? BUILD_PLACEHOLDER_REDIS_URL,
  logger: AIR_CHAT_LOGGER,
});

/**
 * The official adapter's thread id includes transport addressing information.
 * Chat SDK passes that id into its state adapter for subscriptions, locks,
 * queues, and cache keys. Keep it intact only at the transport boundary, then
 * pseudonymize every state-adapter key with Air's identity HMAC secret.
 */
export function createPrivacySafeChatState(
  state: StateAdapter,
  secret: () => string = requiredChatStateSecret,
): StateAdapter {
  const hash = (namespace: string, value: string): string =>
    deriveOpaqueId(secret(), namespace, value);
  const threadKey = (threadId: string): string => hash("chat-thread", threadId);
  const cacheKey = (key: string): string => hash("chat-key", key);

  return {
    connect: () => state.connect(),
    disconnect: () => state.disconnect(),
    subscribe: (threadId) => state.subscribe(threadKey(threadId)),
    unsubscribe: (threadId) => state.unsubscribe(threadKey(threadId)),
    isSubscribed: (threadId) => state.isSubscribed(threadKey(threadId)),
    acquireLock: (threadId, ttlMs) => state.acquireLock(threadKey(threadId), ttlMs),
    forceReleaseLock: (threadId) => state.forceReleaseLock(threadKey(threadId)),
    releaseLock: (lock: Lock) => state.releaseLock(lock),
    extendLock: (lock: Lock, ttlMs) => state.extendLock(lock, ttlMs),
    get: <T = unknown>(key: string) => state.get<T>(cacheKey(key)),
    set: <T = unknown>(key: string, value: T, ttlMs?: number) =>
      state.set<T>(cacheKey(key), value, ttlMs),
    setIfNotExists: (key: string, value: unknown, ttlMs?: number) =>
      state.setIfNotExists(cacheKey(key), value, ttlMs),
    delete: (key: string) => state.delete(cacheKey(key)),
    appendToList: (key: string, value: unknown, options?: { maxLength?: number; ttlMs?: number }) =>
      state.appendToList(cacheKey(key), value, options),
    enqueue: (threadId: string, entry: QueueEntry, maxSize: number) =>
      state.enqueue(threadKey(threadId), entry, maxSize),
    dequeue: (threadId: string) => state.dequeue(threadKey(threadId)),
    queueDepth: (threadId: string) => state.queueDepth(threadKey(threadId)),
    getList: <T = unknown>(key: string) => state.getList<T>(cacheKey(key)),
  };
}

function requiredChatStateSecret(): string {
  const secret = environmentValue("AIR_ID_HASH_SECRET");
  if (!secret) throw new Error("AIR_ID_HASH_SECRET is required for Chat SDK Redis state");
  return secret;
}

const state = createPrivacySafeChatState(rawState);

/**
 * Both the inert generated adapter route and Air's opaque workflow channel use
 * the same lifecycle policy. The handler accepts only the narrow runtime
 * facade so it cannot accidentally depend on Chat SDK's raw serialized state.
 */
const lifecycleEvents = {
  "turn.started": async (
    event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageTurnStarted({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
  "actions.requested": async (
    event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageProgressEvent({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
  "message.appended": async (
    event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageProgressEvent({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
  "reasoning.appended": async (
    event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageProgressEvent({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
  "reasoning.completed": async (
    event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageProgressEvent({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
  "action.result": async (
    event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageActionResult({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
  "input.requested": async (
    event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageInputRequested({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
  "message.completed": async (
    event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageMessageCompleted({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
  "turn.failed": async (
    event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageTurnFailed({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
  "turn.completed": async (
    event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageProgressEvent({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
  "session.failed": async (event: unknown, eventChannel: unknown) => handleIMessageSessionFailed({
    event,
    eventChannel: eventChannel as AirChatEventChannel,
  }),
  "session.completed": async (
    _event: unknown,
    eventChannel: unknown,
    ctx: { readonly session: { readonly id: string } },
  ) => handleIMessageSessionCompleted({
    eventChannel: eventChannel as AirChatEventChannel,
    sessionId: ctx.session.id,
  }),
};

/**
 * This is deliberately a transport bridge, not an inbound Chat SDK bot.
 *
 * Do not register `bot.onNewMention`, `bot.onDirectMessage`, or
 * `bot.onSubscribedMessage` here. Public traffic goes through Air's durable
 * ingress, which calls Eve's `receive(imessageChannel, input)` hand-off once
 * the encrypted inbox has accepted and serialized the delivery.
 */
const bridge = chatSdkChannel({
  adapters: { [IMESSAGE_ADAPTER_NAME]: imessageAdapter },
  logger: AIR_CHAT_LOGGER,
  // These handlers are inert on the pinned route because there are no bot
  // dispatch registrations. Keeping them explicit also prevents Chat SDK's
  // default free-form delivery behavior if that route is ever exercised.
  events: lifecycleEvents,
  routes: {
    [IMESSAGE_ADAPTER_NAME]: IMESSAGE_ADAPTER_INTERNAL_ROUTE,
  },
  state,
  streaming: false,
  userName: AR_AGENT_NAME,
});

export const { bot } = bridge;

/**
 * The generated Chat SDK channel's default state contains a SerializedThread,
 * which would put physical addressing into Eve workflow state. Air keeps the
 * official bridge for its adapter routes and Chat SDK thread abstraction, but
 * exposes a small, explicitly state-safe channel below for durable hand-offs.
 */
export function createOpaqueIMessageThread(
  deliveryTargetId: string,
  dependencies: {
    readonly resolveThreadId?: (targetId: string) => Promise<string | null>;
    readonly threadFromId?: (threadId: string) => AirChatThread;
  } = {},
): AirChatThread {
  const resolveThreadId = dependencies.resolveThreadId ?? resolveIMessageDeliveryThreadId;
  const threadFromId = dependencies.threadFromId
    ?? ((threadId: string) => bot.thread(threadId) as unknown as AirChatThread);
  return Object.freeze({
    id: deliveryTargetId,
    post: async (message: string) => {
      const threadId = await resolveThreadId(deliveryTargetId);
      if (!threadId) throw new Error("AR Agent delivery target is unavailable");
      return threadFromId(threadId).post(message);
    },
  });
}

/**
 * Projects only HMAC-derived operational metadata. A missing hash secret is
 * deliberately represented by nulls during module discovery instead of a
 * build-time placeholder that could make identities correlatable.
 */
export function privacySafeChatInstrumentationMetadata(
  state: OpaqueIMessageWorkflowState,
  secret: string | undefined = environmentValue("AIR_ID_HASH_SECRET"),
): ChatSdkInstrumentationMetadata {
  const deliveryTargetId = state.deliveryTargetId;
  const active = Boolean(deliveryTargetId && OPAQUE_DELIVERY_TARGET_PATTERN.test(deliveryTargetId));
  return {
    adapterName: active ? IMESSAGE_ADAPTER_NAME : null,
    channelId: null,
    isDM: active ? true : null,
    threadId: active && secret
      ? deriveOpaqueId(secret, "eve-imessage-workflow-target", deliveryTargetId as string)
      : null,
  };
}

export function createOpaqueIMessageChannelContext(
  state: OpaqueIMessageWorkflowState,
): OpaqueIMessageChannelContext {
  const deliveryTargetId = state.deliveryTargetId;
  return {
    state,
    thread: deliveryTargetId && OPAQUE_DELIVERY_TARGET_PATTERN.test(deliveryTargetId)
      ? createOpaqueIMessageThread(deliveryTargetId)
      : null,
  };
}

/**
 * The stock Chat SDK receiver keys sessions by a physical thread id and uses
 * conversation mode. Air's durable worker needs a per-attempt task instead.
 * This small, version-pinned composition retains Chat SDK state/context while
 * making continuation ownership explicit and unguessable.
 */
export function createAttemptScopedIMessageReceive(): NonNullable<AttemptScopedIMessageChannel["receive"]> {
  return async (input, { send: sendTurn }) => {
    const { adapterName, continuationKey, deliveryTargetId } = input.target;
    if (adapterName !== IMESSAGE_ADAPTER_NAME) {
      throw new Error("AR Agent only accepts the iMessage Chat SDK adapter.");
    }
    if (!CONTINUATION_KEY_PATTERN.test(continuationKey)) {
      throw new Error("AR Agent iMessage continuation key is invalid.");
    }
    if (!OPAQUE_DELIVERY_TARGET_PATTERN.test(deliveryTargetId)) {
      throw new Error("AR Agent iMessage delivery target is invalid.");
    }

    return sendTurn(input.message, {
      auth: input.auth,
      continuationToken: continuationKey,
      mode: "task",
      title: AR_AGENT_IMESSAGE_WORKFLOW_TITLE,
      state: { deliveryTargetId },
    });
  };
}

/**
 * Keep the generated adapter webhook at its pinned, unregistered internal
 * path. The official adapter still verifies HMAC there, but this module has no
 * message-dispatching bot handlers, so a valid adapter delivery cannot call
 * `send` or start an Eve session. Photon must point only to the durable public
 * ingress route, which is the sole configured dispatch path.
 */
export const channel = defineChannel({
  kindHint: "chat-sdk",
  state: { deliveryTargetId: null } as OpaqueIMessageWorkflowState,
  context: createOpaqueIMessageChannelContext,
  metadata: privacySafeChatInstrumentationMetadata,
  // The bridge creates these two official adapter routes. Reusing only their
  // route definitions preserves HMAC verification while avoiding the bridge's
  // raw SerializedThread workflow adapter state.
  routes: bridge.channel.routes as unknown as readonly RouteDefinition<OpaqueIMessageWorkflowState>[],
  events: lifecycleEvents,
  receive: createAttemptScopedIMessageReceive(),
}) as unknown as AttemptScopedIMessageChannel;

export default channel;
