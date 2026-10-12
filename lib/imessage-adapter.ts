import type { AirVideoIMessageAdapter } from "./imessage-video-runtime.js";

type ChatAdapterModule = typeof import("@photon-ai/chat-adapter-imessage");
type RealIMessageAdapter = ReturnType<ChatAdapterModule["createiMessageAdapter"]> & {
  app: unknown | null;
};

/**
 * Air accepts signed Photon deliveries only through `agent/channels/ingress`.
 * This module exposes the official Spectrum adapter solely for attachment
 * retrieval and outbound iMessage posts; it declares no adapter webhook route.
 */
export const IMESSAGE_ADAPTER_NAME = "imessage" as const;

const RUNTIME_ENVIRONMENT_NAMES = [
  "IMESSAGE_PROJECT_ID",
  "IMESSAGE_PROJECT_SECRET",
  "IMESSAGE_WEBHOOK_SECRET",
  "REDIS_URL",
  "ZAP_AIR_SERVICE_URL",
  "ZAP_AIR_SERVICE_TOKEN",
  "AIR_ID_HASH_SECRET",
  "AIR_QUEUE_ENCRYPTION_KEY",
  "AIR_ALLOWED_SENDER_HASHES",
  "AIR_PUBLIC_BASE_URL",
  "CRON_SECRET",
] as const;

// Photon/Spectrum spans can include provider message metadata. Air deliberately
// has no redacting exporter, so deployment must fail rather than leak it to an
// ambient OTLP collector configured by the hosting environment.
const FORBIDDEN_OTEL_EXPORTER_ENVIRONMENT_NAMES = [
  "OTEL_TRACES_EXPORTER",
  "OTEL_LOGS_EXPORTER",
] as const;

function environmentValue(name: string): string | undefined {
  const value = process.env[name]?.trim();
  return value || undefined;
}

function buildSafeAdapterValue(name: string): string {
  return environmentValue(name) ?? `air-build-placeholder-${name.toLowerCase()}`;
}

/** Fail closed before an ingress or recovery worker can accept a message. */
export function assertIMessageRuntimeConfiguration(): void {
  // Workers deployments use Upstash REST instead of a TCP REDIS_URL.
  const redisConfigured =
    environmentValue("REDIS_URL") !== undefined ||
    (environmentValue("UPSTASH_REDIS_REST_URL") !== undefined &&
      environmentValue("UPSTASH_REDIS_REST_TOKEN") !== undefined);
  const missing = RUNTIME_ENVIRONMENT_NAMES.filter(
    (name) => name !== "REDIS_URL" || !redisConfigured,
  ).filter((name) => environmentValue(name) === undefined);
  if (missing.length > 0) {
    throw new Error(`Air iMessage video runtime configuration is incomplete (${missing.join(", ")}).`);
  }
  const exporters = [
    ...FORBIDDEN_OTEL_EXPORTER_ENVIRONMENT_NAMES.filter((name) => environmentValue(name) !== undefined),
    ...Object.keys(process.env).filter(
      (name) => name.startsWith("OTEL_EXPORTER_OTLP_") && environmentValue(name) !== undefined,
    ),
  ];
  if (exporters.length > 0) {
    throw new Error("Air forbids OTLP exporter configuration because Spectrum telemetry is not redacted.");
  }
}

const IMESSAGE_THREAD_PREFIX = "imessage:";
const IMESSAGE_LINE_SEPARATOR = "~";

let adapterPromise: Promise<RealIMessageAdapter> | undefined;

/**
 * The Photon adapter chain pulls gRPC/net/sqlite modules that cannot evaluate
 * inside a Cloudflare Worker, so the real adapter is built on first use inside
 * the media/delivery path. The webhook-verify path never touches it.
 */
function loadIMessageAdapter(): Promise<RealIMessageAdapter> {
  adapterPromise ??= (async () => {
    const [{ createiMessageAdapter }, { ConsoleLogger }] = await Promise.all([
      import("@photon-ai/chat-adapter-imessage"),
      import("chat"),
    ]);
    // Silence framework transport logging; Air emits no prompts or provider bodies.
    return createiMessageAdapter({
      projectId: buildSafeAdapterValue("IMESSAGE_PROJECT_ID"),
      projectSecret: buildSafeAdapterValue("IMESSAGE_PROJECT_SECRET"),
      webhookSecret: buildSafeAdapterValue("IMESSAGE_WEBHOOK_SECRET"),
      logger: new ConsoleLogger("silent"),
    }) as unknown as RealIMessageAdapter;
  })();
  return adapterPromise;
}

/**
 * Lazy `AirVideoIMessageAdapter`: `encodeThreadId` mirrors the adapter's
 * internal encoder (THREAD_PREFIX + LINE_SEP) so it stays synchronous; every
 * network-facing method defers the package load until first call. `app` is
 * null until the underlying adapter has initialized, matching the previous
 * semantics that gated attachments on a prior `fetchMessage`.
 */
class LazyIMessageAdapter implements AirVideoIMessageAdapter {
  private real: RealIMessageAdapter | undefined;

  get app(): unknown | null {
    return this.real?.app ?? null;
  }

  encodeThreadId(input: { readonly chatGuid: string; readonly phone?: string }): string {
    return input.phone
      ? `${IMESSAGE_THREAD_PREFIX}${input.chatGuid}${IMESSAGE_LINE_SEPARATOR}${input.phone}`
      : `${IMESSAGE_THREAD_PREFIX}${input.chatGuid}`;
  }

  async fetchMessage(threadId: string, messageId: string): Promise<unknown> {
    const adapter = await this.ensure();
    return adapter.fetchMessage(threadId, messageId);
  }

  async postMessage(
    threadId: string,
    message: Parameters<RealIMessageAdapter["postMessage"]>[1],
  ): Promise<unknown> {
    const adapter = await this.ensure();
    return adapter.postMessage(threadId, message);
  }

  private async ensure(): Promise<RealIMessageAdapter> {
    const adapter = await loadIMessageAdapter();
    this.real = adapter;
    return adapter;
  }
}

export const imessageAdapter: AirVideoIMessageAdapter = new LazyIMessageAdapter();
