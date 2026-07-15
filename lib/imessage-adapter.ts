import { ConsoleLogger } from "chat";
import { createiMessageAdapter } from "@photon-ai/chat-adapter-imessage";

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
  const missing = RUNTIME_ENVIRONMENT_NAMES.filter((name) => environmentValue(name) === undefined);
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

/** Silence framework transport logging; Air emits no prompts or provider bodies. */
export const AIR_CHAT_LOGGER = new ConsoleLogger("silent");

export const imessageAdapter = createiMessageAdapter({
  projectId: buildSafeAdapterValue("IMESSAGE_PROJECT_ID"),
  projectSecret: buildSafeAdapterValue("IMESSAGE_PROJECT_SECRET"),
  webhookSecret: buildSafeAdapterValue("IMESSAGE_WEBHOOK_SECRET"),
  logger: AIR_CHAT_LOGGER,
});
