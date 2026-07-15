import { decodeQueueEncryptionKey } from "./crypto.js";
import { parseAllowedSenderHashes, type SenderHash } from "./security.js";

export type EnvironmentSource = Readonly<Record<string, string | undefined>>;
export type DeploymentMode = "development" | "preview" | "production";

export interface AirEnvironment {
  readonly mode: DeploymentMode;
  readonly imessageProjectId?: string | undefined;
  readonly imessageProjectSecret?: string | undefined;
  readonly imessageWebhookSecret?: string | undefined;
  readonly redisUrl?: string | undefined;
  readonly mongoUri?: string | undefined;
  readonly mongoDatabase: string;
  readonly voyageApiKey?: string | undefined;
  readonly embedModel: string;
  readonly rerankModel: string;
  readonly generationModel: string;
  readonly idHashSecret?: string | undefined;
  readonly queueEncryptionKey?: string | undefined;
  readonly allowedSenderHashes: readonly SenderHash[];
  readonly cronSecret?: string | undefined;
  readonly publicBaseUrl: string;
  readonly mongoServerSelectionTimeoutMs: number;
  readonly mongoConnectTimeoutMs: number;
}

export const PRODUCTION_REQUIRED_ENV = Object.freeze([
  "IMESSAGE_PROJECT_ID",
  "IMESSAGE_PROJECT_SECRET",
  "IMESSAGE_WEBHOOK_SECRET",
  "REDIS_URL",
  "MONGODB_URI",
  "MONGODB_DB",
  "MONGODB_SERVER_SELECTION_TIMEOUT_MS",
  "MONGODB_CONNECT_TIMEOUT_MS",
  "VOYAGE_API_KEY",
  "EMBED_MODEL",
  "RERANK_MODEL",
  "GENERATION_MODEL",
  "AIR_ID_HASH_SECRET",
  "AIR_QUEUE_ENCRYPTION_KEY",
  "AIR_ALLOWED_SENDER_HASHES",
  "AIR_PUBLIC_BASE_URL",
  "CRON_SECRET",
] as const);

/** Returns missing variable names only; never include secret values in an error or log. */
export function missingRequiredEnvironment(
  source: EnvironmentSource,
  names: readonly string[] = PRODUCTION_REQUIRED_ENV,
): string[] {
  return names.filter((name) => !source[name]?.trim());
}

/**
 * Parse configuration without doing I/O. Production is intentionally strict:
 * there is no permissive startup when the allowlist, queue key, Redis, or
 * transport-verification secrets are absent.
 */
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

  return Object.freeze({
    mode,
    imessageProjectId: optional(source.IMESSAGE_PROJECT_ID),
    imessageProjectSecret: optional(source.IMESSAGE_PROJECT_SECRET),
    imessageWebhookSecret: optional(source.IMESSAGE_WEBHOOK_SECRET),
    redisUrl: optional(source.REDIS_URL),
    mongoUri: optional(source.MONGODB_URI),
    mongoDatabase: optional(source.MONGODB_DB) ?? "music_rag",
    voyageApiKey: optional(source.VOYAGE_API_KEY),
    embedModel: optional(source.EMBED_MODEL) ?? "voyage-4-large",
    rerankModel: optional(source.RERANK_MODEL) ?? "rerank-2.5",
    generationModel: optional(source.GENERATION_MODEL) ?? "google/gemini-3.5-flash",
    idHashSecret: optional(source.AIR_ID_HASH_SECRET),
    queueEncryptionKey,
    allowedSenderHashes,
    cronSecret: optional(source.CRON_SECRET),
    publicBaseUrl: optional(source.AIR_PUBLIC_BASE_URL) ?? "https://air.wzrd.tech",
    mongoServerSelectionTimeoutMs: parsePositiveInteger(
      source.MONGODB_SERVER_SELECTION_TIMEOUT_MS,
      8_000,
      "MONGODB_SERVER_SELECTION_TIMEOUT_MS",
    ),
    mongoConnectTimeoutMs: parsePositiveInteger(
      source.MONGODB_CONNECT_TIMEOUT_MS,
      8_000,
      "MONGODB_CONNECT_TIMEOUT_MS",
    ),
  });
}

export function inferDeploymentMode(source: EnvironmentSource): DeploymentMode;
/** Backward-compatible NODE_ENV-only form for callers outside the runtime. */
export function inferDeploymentMode(nodeEnvironment: string | undefined): DeploymentMode;
export function inferDeploymentMode(sourceOrNodeEnvironment: EnvironmentSource | string | undefined): DeploymentMode {
  const source = typeof sourceOrNodeEnvironment === "object" && sourceOrNodeEnvironment !== null
    ? sourceOrNodeEnvironment
    : undefined;
  const vercelEnvironment = source
    ? optional(source.VERCEL_ENV)
    : undefined;
  // Vercel sets NODE_ENV=production for previews too. Its deployment mode is
  // therefore the authoritative signal: a production deployment must never
  // silently use development defaults because NODE_ENV is absent or unusual.
  if (vercelEnvironment === "production") return "production";
  if (vercelEnvironment === "preview") return "preview";

  const nodeEnvironment = source
    ? optional(source.NODE_ENV)
    : sourceOrNodeEnvironment;
  if (nodeEnvironment === "production") return "production";
  if (nodeEnvironment === "preview") return "preview";
  return "development";
}

function optional(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed || undefined;
}

function parsePositiveInteger(
  value: string | undefined,
  fallback: number,
  name: string,
): number {
  if (!value?.trim()) return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}
