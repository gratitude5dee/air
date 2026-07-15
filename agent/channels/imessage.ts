import { createRedisState } from "@chat-adapter/state-redis";
import { createiMessageAdapter } from "@photon-ai/chat-adapter-imessage";
import type { SerializedThread } from "chat";
import { GET, type Channel } from "eve/channels";
import {
  chatSdkChannel,
  type ChatSdkChannelState,
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
} from "../../lib/imessage-channel-runtime.js";

/** The only Chat SDK adapter configured for Air V1. */
export const IMESSAGE_ADAPTER_NAME = "imessage" as const;

/**
 * A defensive placeholder used while constructing the Chat SDK bridge. Air
 * strips bridge-owned webhook routes from the exported channel below: Photon
 * must have no alternate public path around the durable ingress.
 */
export const IMESSAGE_ADAPTER_INTERNAL_ROUTE = "/_internal/chat-sdk/imessage";

/**
 * This GET-only route exists solely so Eve compiles the channel's cross-channel
 * receiver. It is deliberately inert: an "internal" path is not an access
 * control boundary, so the handler always returns a non-cacheable 404 and
 * never calls Photon, Chat SDK, Eve send, or Eve receive.
 */
export const IMESSAGE_CHANNEL_REGISTRATION_ROUTE = "/_internal/air-imessage-channel";

export const AR_AGENT_NAME = "AR Agent";

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
  threadId: string;
}

/**
 * Each durable inbox attempt gets a distinct opaque continuation key. The
 * physical Chat SDK thread is intentionally separate from this key: a retry
 * for the same iMessage thread must start a fresh Eve task, never deliver into
 * a session parked by an expired worker.
 */
export interface AttemptScopedIMessageTarget {
  readonly adapterName: typeof IMESSAGE_ADAPTER_NAME;
  readonly threadId: string;
  readonly continuationKey: string;
}

export type AttemptScopedIMessageChannel = Channel<
  ChatSdkChannelState,
  AttemptScopedIMessageTarget,
  ChatSdkInstrumentationMetadata
>;

type ThreadSerializer = {
  readonly thread: (threadId: string) => { readonly toJSON: () => SerializedThread };
};

const CONTINUATION_KEY_PATTERN = /^[a-f0-9]{64}$/;

const RUNTIME_ENVIRONMENT_NAMES = [
  "IMESSAGE_PROJECT_ID",
  "IMESSAGE_PROJECT_SECRET",
  "IMESSAGE_WEBHOOK_SECRET",
  "REDIS_URL",
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
});

const state = createRedisState({
  keyPrefix: "air:chat",
  url: environmentValue("REDIS_URL") ?? BUILD_PLACEHOLDER_REDIS_URL,
});

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
  events: {
    // chatSdkChannel merges supplied handlers over its delivery defaults.
    // Explicitly neutralize every remaining default so this channel cannot
    // post model text, tool-progress text, or a framework error independently
    // of Air's controlled result/fallback handlers below.
    "turn.started": async (event, eventChannel, ctx) =>
      handleIMessageTurnStarted({ event, eventChannel, sessionId: ctx.session.id }),
    "actions.requested": async (event, eventChannel, ctx) =>
      handleIMessageProgressEvent({ event, eventChannel, sessionId: ctx.session.id }),
    "message.appended": async (event, eventChannel, ctx) =>
      handleIMessageProgressEvent({ event, eventChannel, sessionId: ctx.session.id }),
    "reasoning.appended": async (event, eventChannel, ctx) =>
      handleIMessageProgressEvent({ event, eventChannel, sessionId: ctx.session.id }),
    "reasoning.completed": async (event, eventChannel, ctx) =>
      handleIMessageProgressEvent({ event, eventChannel, sessionId: ctx.session.id }),
    "action.result": async (event, eventChannel, ctx) =>
      handleIMessageActionResult({ event, eventChannel, sessionId: ctx.session.id }),
    "input.requested": async (event, eventChannel, ctx) =>
      handleIMessageInputRequested({ event, eventChannel, sessionId: ctx.session.id }),
    "message.completed": async (event, eventChannel, ctx) =>
      handleIMessageMessageCompleted({ event, eventChannel, sessionId: ctx.session.id }),
    "turn.failed": async (event, eventChannel, ctx) =>
      handleIMessageTurnFailed({ event, eventChannel, sessionId: ctx.session.id }),
    "turn.completed": async (event, eventChannel, ctx) =>
      handleIMessageProgressEvent({ event, eventChannel, sessionId: ctx.session.id }),
    "session.failed": async (event, eventChannel) =>
      handleIMessageSessionFailed({ event, eventChannel }),
    "session.completed": async (_event, eventChannel, ctx) =>
      handleIMessageSessionCompleted({ eventChannel, sessionId: ctx.session.id }),
  },
  routes: {
    [IMESSAGE_ADAPTER_NAME]: IMESSAGE_ADAPTER_INTERNAL_ROUTE,
  },
  state,
  streaming: false,
  userName: AR_AGENT_NAME,
});

export const { bot, send } = bridge;

/**
 * The stock Chat SDK receiver keys sessions by a physical thread id and uses
 * conversation mode. Air's durable worker needs a per-attempt task instead.
 * This small, version-pinned composition retains Chat SDK state/context while
 * making continuation ownership explicit and unguessable.
 */
export function createAttemptScopedIMessageReceive(
  chat: ThreadSerializer,
): NonNullable<AttemptScopedIMessageChannel["receive"]> {
  return async (input, { send: sendTurn }) => {
    const { adapterName, continuationKey, threadId } = input.target;
    if (adapterName !== IMESSAGE_ADAPTER_NAME) {
      throw new Error("AR Agent only accepts the iMessage Chat SDK adapter.");
    }
    if (!CONTINUATION_KEY_PATTERN.test(continuationKey)) {
      throw new Error("AR Agent iMessage continuation key is invalid.");
    }

    const thread = chat.thread(threadId).toJSON();
    if (thread.adapterName !== IMESSAGE_ADAPTER_NAME || thread.id !== threadId) {
      throw new Error("AR Agent iMessage target does not resolve to its adapter thread.");
    }

    return sendTurn(input.message, {
      auth: input.auth,
      continuationToken: continuationKey,
      mode: "task",
      state: { thread },
    });
  };
}

/**
 * `chatSdkChannel()` creates adapter webhooks by default. Air needs its
 * channel adapter and cross-channel `receive` hook, but intentionally exposes
 * no adapter-generated routes: otherwise a valid Photon delivery could reach
 * `adapter.handleWebhook()` without first entering the encrypted inbox.
 *
 * Eve omits a channel with an empty route table from its compiled manifest,
 * which would also remove this channel as a cross-channel receive target.
 * The inert registration route below preserves the target while exposing no
 * Photon webhook or dispatch path. Its handler is safe even if reached.
 */
// Eve attaches a private instrumentation marker while resolving registered
// channels at runtime, so this wrapper must remain extensible. Treat it as
// immutable by module convention rather than Object.freeze().
export const channel = {
  ...bridge.channel,
  routes: [
    GET(IMESSAGE_CHANNEL_REGISTRATION_ROUTE, async () =>
      new Response(null, {
        status: 404,
        headers: { "cache-control": "no-store" },
      })),
  ],
  receive: createAttemptScopedIMessageReceive(bot),
} as unknown as AttemptScopedIMessageChannel;

export default channel;
