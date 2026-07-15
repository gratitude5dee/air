import { decodeQueueEncryptionKey } from "./crypto.js";
import { parseAllowedSenderHashes, type SenderHash } from "./security.js";

export type EnvironmentSource = Readonly<Record<string, string | undefined>>;
export type DeploymentMode = "development" | "preview" | "production";

/** Runtime configuration for the private iMessage → Zap video service. */
export interface AirEnvironment {
  readonly mode: DeploymentMode;
  readonly imessageProjectId?: string | undefined;
  readonly imessageProjectSecret?: string | undefined;
  readonly imessageWebhookSecret?: string | undefined;
  readonly redisUrl?: string | undefined;
  /** Preferred HTTPS transport for Upstash in serverless production. */
  readonly upstashRedisRestUrl?: string | undefined;
  readonly upstashRedisRestToken?: string | undefined;
  readonly zapAirServiceUrl?: string | undefined;
  readonly zapAirServiceToken?: string | undefined;
  readonly idHashSecret?: string | undefined;
  readonly queueEncryptionKey?: string | undefined;
  readonly allowedSenderHashes: readonly SenderHash[];
  readonly cronSecret?: string | undefined;
  readonly publicBaseUrl: string;
}

export const PRODUCTION_REQUIRED_ENV = Object.freeze([
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
] as const);

/** Returns names only; no secret values ever leave this module. */
export function missingRequiredEnvironment(
  source: EnvironmentSource,
  names: readonly string[] = PRODUCTION_REQUIRED_ENV,
): string[] {
  return names.filter((name) => !source[name]?.trim());
}

export function loadAirEnvironment(
  source: EnvironmentSource = process.env,
  options: { mode?: DeploymentMode } = {},
): AirEnvironment {
  const mode = options.mode ?? inferDeploymentMode(source);
  if (mode === "production") {
    const missing = missingRequiredEnvironment(source);
    if (missing.length > 0) {
      throw new Error(`missing required production environment: ${missing.join(", ")}`);
    }
  }

  const queueEncryptionKey = optional(source.AIR_QUEUE_ENCRYPTION_KEY);
  if (queueEncryptionKey) decodeQueueEncryptionKey(queueEncryptionKey);
  const allowedSenderHashes = parseAllowedSenderHashes(source.AIR_ALLOWED_SENDER_HASHES);
  if (mode === "production" && allowedSenderHashes.length === 0) {
    throw new Error("AIR_ALLOWED_SENDER_HASHES must contain at least one approved sender hash");
  }

  const zapAirServiceUrl = optional(source.ZAP_AIR_SERVICE_URL);
  const zapAirServiceToken = optional(source.ZAP_AIR_SERVICE_TOKEN);
  const upstashRedisRestUrl = optional(source.UPSTASH_REDIS_REST_URL);
  const upstashRedisRestToken = optional(source.UPSTASH_REDIS_REST_TOKEN);
  if (upstashRedisRestUrl || upstashRedisRestToken) {
    if (!upstashRedisRestUrl || !upstashRedisRestToken) {
      throw new Error("UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN must be configured together");
    }
    validateUpstashRedisRestUrl(upstashRedisRestUrl);
    if (upstashRedisRestToken.length > 4_096 || /[\r\n]/.test(upstashRedisRestToken)) {
      throw new Error("UPSTASH_REDIS_REST_TOKEN is invalid");
    }
  }
  if (zapAirServiceUrl) validateZapAirServiceUrl(zapAirServiceUrl);
  if (zapAirServiceToken && (zapAirServiceToken.length > 4_096 || /[\r\n]/.test(zapAirServiceToken))) {
    throw new Error("ZAP_AIR_SERVICE_TOKEN is invalid");
  }

  return Object.freeze({
    mode,
    imessageProjectId: optional(source.IMESSAGE_PROJECT_ID),
    imessageProjectSecret: optional(source.IMESSAGE_PROJECT_SECRET),
    imessageWebhookSecret: optional(source.IMESSAGE_WEBHOOK_SECRET),
    redisUrl: optional(source.REDIS_URL),
    upstashRedisRestUrl,
    upstashRedisRestToken,
    zapAirServiceUrl,
    zapAirServiceToken,
    idHashSecret: optional(source.AIR_ID_HASH_SECRET),
    queueEncryptionKey,
    allowedSenderHashes,
    cronSecret: optional(source.CRON_SECRET),
    publicBaseUrl: optional(source.AIR_PUBLIC_BASE_URL) ?? "https://air.wzrd.tech",
  });
}

/** Allows loopback HTTP only for local test fixtures. */
function validateZapAirServiceUrl(value: string): void {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("ZAP_AIR_SERVICE_URL is invalid");
  }
  const loopback = ["localhost", "127.0.0.1", "::1", "[::1]"].includes(url.hostname.toLowerCase());
  if (
    (url.protocol !== "https:" && !(url.protocol === "http:" && loopback)) ||
    !url.hostname ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    (url.pathname !== "/" && url.pathname !== "")
  ) {
    throw new Error("ZAP_AIR_SERVICE_URL is invalid");
  }
}

function validateUpstashRedisRestUrl(value: string): void {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("UPSTASH_REDIS_REST_URL is invalid");
  }
  if (
    url.protocol !== "https:" ||
    !url.hostname ||
    url.username ||
    url.password ||
    (url.pathname !== "/" && url.pathname !== "") ||
    url.search ||
    url.hash
  ) {
    throw new Error("UPSTASH_REDIS_REST_URL is invalid");
  }
}

export function inferDeploymentMode(source: EnvironmentSource): DeploymentMode;
export function inferDeploymentMode(nodeEnvironment: string | undefined): DeploymentMode;
export function inferDeploymentMode(sourceOrNodeEnvironment: EnvironmentSource | string | undefined): DeploymentMode {
  const source = typeof sourceOrNodeEnvironment === "object" && sourceOrNodeEnvironment !== null
    ? sourceOrNodeEnvironment
    : undefined;
  const vercelEnvironment = source ? optional(source.VERCEL_ENV) : undefined;
  if (vercelEnvironment === "production") return "production";
  if (vercelEnvironment === "preview") return "preview";
  const nodeEnvironment = source ? optional(source.NODE_ENV) : sourceOrNodeEnvironment;
  if (nodeEnvironment === "production") return "production";
  if (nodeEnvironment === "preview") return "preview";
  return "development";
}

function optional(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed || undefined;
}
