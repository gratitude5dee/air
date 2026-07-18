import { decodeQueueEncryptionKey } from "../crypto.js";
import { parseAllowedSenderHashes, type SenderHash } from "../security.js";

/**
 * Air's direct-studio configuration is deliberately parsed in one place.
 * Values never leave this module: callers receive capability booleans and
 * already-validated values only.
 */
export type StudioReleasePhase = "dark" | "canary" | "production";
export type StudioDeploymentMode = "development" | "preview" | "production";
export type StorageTransport = "rest" | "tcp" | "unconfigured";

export interface StudioSideEffectPolicy {
  readonly acceptNewJobs: boolean;
  readonly allowProviderSubmit: boolean;
  readonly allowProviderRecovery: boolean;
  readonly deliveryEnabled: boolean;
}

export interface StudioSpendCaps {
  /** Integer US cents. Keeping money integral prevents float-cap bypasses. */
  readonly perJobCents: number;
  readonly dailyCents: number;
  readonly canaryTotalCents: number;
  readonly canaryFalCents: number;
}

export interface StudioEnvironment {
  readonly deploymentMode: StudioDeploymentMode;
  readonly releasePhase: StudioReleasePhase;
  /** Monotonic global release epoch; dark deployments intentionally use zero. */
  readonly releaseGeneration: number;
  /** Shared-secret material for authenticated release-fence advancement. */
  readonly releaseFenceSecret?: string | undefined;
  readonly storageTransport: StorageTransport;
  readonly imessageProjectId?: string | undefined;
  readonly imessageProjectSecret?: string | undefined;
  readonly imessageWebhookSecret?: string | undefined;
  readonly redisUrl?: string | undefined;
  readonly upstashRedisRestUrl?: string | undefined;
  readonly upstashRedisRestToken?: string | undefined;
  readonly gmiApiKey?: string | undefined;
  readonly gmiOrganizationId?: string | undefined;
  readonly idHashSecret?: string | undefined;
  readonly queueEncryptionKey?: string | undefined;
  readonly allowedSenderHashes: readonly SenderHash[];
  readonly cronSecret?: string | undefined;
  readonly publicBaseUrl: string;
  readonly modelRegistryVersion?: string | undefined;
  readonly dailyUserJobLimit: number;
  readonly activeJobLimit: number;
  readonly canaryBudgetId?: string | undefined;
  readonly policy: StudioSideEffectPolicy;
  /** Every side-effect switch must be explicitly true or false in production. */
  readonly policyExplicit: boolean;
  readonly spendCaps: StudioSpendCaps;
}

/**
 * The capability checks below deliberately require both a non-dark release
 * phase and the matching explicit switch. A switch set to true must never turn
 * a dark deployment into a side-effecting deployment by itself.
 */
export type StudioSideEffectEnvironment = Pick<StudioEnvironment, "releasePhase" | "policy">;

export type EnvironmentSource = Readonly<Record<string, string | undefined>>;

const PROTECTED_CONFIGURATION_NAMES = Object.freeze([
  "IMESSAGE_PROJECT_ID",
  "IMESSAGE_PROJECT_SECRET",
  "IMESSAGE_WEBHOOK_SECRET",
  "AIR_ID_HASH_SECRET",
  "AIR_QUEUE_ENCRYPTION_KEY",
  "AIR_ALLOWED_SENDER_HASHES",
  "CRON_SECRET",
  "AIR_MODEL_REGISTRY_VERSION",
  "AIR_RELEASE_GENERATION",
  "AIR_RELEASE_FENCE_SECRET",
] as const);

/** Names only, for operators and tests. Never use this list in a response body. */
export const STUDIO_REQUIRED_CONFIGURATION_NAMES = PROTECTED_CONFIGURATION_NAMES;

export function loadStudioEnvironment(source: EnvironmentSource = process.env): StudioEnvironment {
  const deploymentMode = inferStudioDeploymentMode(source);
  const restUrl = optional(source.UPSTASH_REDIS_REST_URL);
  const restToken = optional(source.UPSTASH_REDIS_REST_TOKEN);
  if ((restUrl === undefined) !== (restToken === undefined)) {
    throw new Error("Upstash Redis REST credentials must be configured as a pair");
  }
  if (restUrl !== undefined) validateRestUrl(restUrl);
  if (restToken !== undefined) validateSecretLikeValue(restToken, "Upstash Redis REST token");

  const redisUrl = optional(source.REDIS_URL);
  // A legacy deployment may retain a TCP URL with credentials/options that
  // the V2 REST client neither parses nor uses. Validate it only when it is
  // the selected Studio transport; otherwise an unrelated rollback setting
  // must not prevent the explicit REST control plane from starting.
  if (redisUrl !== undefined && restUrl === undefined) validateRedisUrl(redisUrl);
  // V2 selects the explicit Upstash REST pair whenever it is present. The
  // retained REDIS_URL can belong to the legacy deployment kept for rollback;
  // treating it as a second V2 store would make a production migration fail
  // closed even though the new runtime never opens that TCP connection.
  const storageTransport: StorageTransport = restUrl !== undefined
    ? "rest"
    : redisUrl !== undefined
      ? "tcp"
      : "unconfigured";

  const queueEncryptionKey = optional(source.AIR_QUEUE_ENCRYPTION_KEY);
  if (queueEncryptionKey !== undefined) decodeQueueEncryptionKey(queueEncryptionKey);

  const allowedSenderHashes = parseAllowedSenderHashes(source.AIR_ALLOWED_SENDER_HASHES);
  const gmiApiKey = optional(source.GMI_API_KEY);
  if (gmiApiKey !== undefined) validateSecretLikeValue(gmiApiKey, "GMI API key");
  const gmiOrganizationId = optional(source.GMI_ORG_ID);
  if (gmiOrganizationId !== undefined) validateHeaderValue(gmiOrganizationId, "GMI organization ID");

  const policy = Object.freeze({
    acceptNewJobs: explicitBoolean(source.AIR_ACCEPT_NEW_JOBS),
    allowProviderSubmit: explicitBoolean(source.AIR_ALLOW_PROVIDER_SUBMIT),
    allowProviderRecovery: explicitBoolean(source.AIR_ALLOW_PROVIDER_RECOVERY),
    deliveryEnabled: explicitBoolean(source.AIR_DELIVERY_ENABLED),
  });
  const policyExplicit = hasExplicitBoolean(source.AIR_ACCEPT_NEW_JOBS)
    && hasExplicitBoolean(source.AIR_ALLOW_PROVIDER_SUBMIT)
    && hasExplicitBoolean(source.AIR_ALLOW_PROVIDER_RECOVERY)
    && hasExplicitBoolean(source.AIR_DELIVERY_ENABLED);
  const releasePhase = parseReleasePhase(source.AIR_RELEASE_PHASE);
  const spendCaps = Object.freeze({
    perJobCents: usdToCents(source.AIR_JOB_SPEND_CAP_USD),
    dailyCents: usdToCents(source.AIR_DAILY_SPEND_CAP_USD),
    canaryTotalCents: usdToCents(source.AIR_CANARY_TOTAL_CAP_USD),
    canaryFalCents: usdToCents(source.AIR_CANARY_FAL_CAP_USD),
  });
  const canaryBudgetId = optional(source.AIR_CANARY_BUDGET_ID);
  if (releasePhase === "canary") {
    if (!canaryBudgetId || spendCaps.canaryTotalCents < 1) {
      throw new Error("canary phase requires an approved budget and positive cap");
    }
  }
  const releaseGeneration = parseReleaseGeneration(source.AIR_RELEASE_GENERATION, releasePhase);
  const releaseFenceSecret = optional(source.AIR_RELEASE_FENCE_SECRET);
  if (releaseFenceSecret !== undefined) validateSecretLikeValue(releaseFenceSecret, "AIR_RELEASE_FENCE_SECRET");
  if (isReleasePhaseActive(releasePhase) && !releaseFenceSecret) {
    throw new Error("an active release phase requires AIR_RELEASE_FENCE_SECRET");
  }

  const environment = Object.freeze({
    deploymentMode,
    releasePhase,
    releaseGeneration,
    releaseFenceSecret,
    storageTransport,
    imessageProjectId: optional(source.IMESSAGE_PROJECT_ID),
    imessageProjectSecret: optional(source.IMESSAGE_PROJECT_SECRET),
    imessageWebhookSecret: optional(source.IMESSAGE_WEBHOOK_SECRET),
    redisUrl,
    upstashRedisRestUrl: restUrl,
    upstashRedisRestToken: restToken,
    gmiApiKey,
    gmiOrganizationId,
    idHashSecret: optional(source.AIR_ID_HASH_SECRET),
    queueEncryptionKey,
    allowedSenderHashes,
    cronSecret: optional(source.CRON_SECRET),
    publicBaseUrl: optional(source.AIR_PUBLIC_BASE_URL) ?? "https://air.wzrd.tech",
    modelRegistryVersion: optional(source.AIR_MODEL_REGISTRY_VERSION),
    dailyUserJobLimit: positiveInteger(source.AIR_DAILY_USER_JOB_LIMIT, 3, 100),
    activeJobLimit: positiveInteger(source.AIR_ACTIVE_JOB_LIMIT, 1, 1),
    canaryBudgetId,
    policy,
    policyExplicit,
    spendCaps,
  } satisfies StudioEnvironment);

  validatePublicBaseUrl(environment.publicBaseUrl);
  return environment;
}

/** Canary and production are the only explicitly active release phases. */
export function isStudioSideEffectPhaseActive(
  environment: Pick<StudioEnvironment, "releasePhase">,
): boolean {
  return isReleasePhaseActive(environment.releasePhase);
}

export function canAcceptStudioJobs(environment: StudioSideEffectEnvironment): boolean {
  return isStudioSideEffectPhaseActive(environment) && environment.policy.acceptNewJobs;
}

export function canSubmitStudioProvider(environment: StudioSideEffectEnvironment): boolean {
  return isStudioSideEffectPhaseActive(environment) && environment.policy.allowProviderSubmit;
}

/**
 * Polling, artifact fetch, and cancellation are provider side effects too.
 * Keeping this separate from submission preserves the credential-compromise
 * kill switch without letting a dark release recover provider work.
 */
export function canRecoverStudioProvider(environment: StudioSideEffectEnvironment): boolean {
  return isStudioSideEffectPhaseActive(environment) && environment.policy.allowProviderRecovery;
}

export function canDeliverStudioMessages(environment: StudioSideEffectEnvironment): boolean {
  return isStudioSideEffectPhaseActive(environment) && environment.policy.deliveryEnabled;
}

/**
 * A production readiness response is intentionally coarse: no secret names,
 * values, model IDs, sender counts, or provider configuration leak from it.
 */
export function studioReadinessProblems(
  environment: StudioEnvironment,
  expectedRegistryVersion: string,
): readonly string[] {
  const problems: string[] = [];
  if (!environment.imessageProjectId || !environment.imessageProjectSecret || !environment.imessageWebhookSecret) {
    problems.push("messaging_unavailable");
  }
  if (environment.storageTransport === "unconfigured") problems.push("storage_unavailable");
  if (!environment.idHashSecret || !environment.queueEncryptionKey || environment.allowedSenderHashes.length === 0) {
    problems.push("privacy_configuration_unavailable");
  }
  if (!environment.cronSecret) problems.push("operator_access_unavailable");
  if (environment.modelRegistryVersion !== expectedRegistryVersion) problems.push("registry_version_mismatch");
  if (isStudioSideEffectPhaseActive(environment)) {
    // `loadStudioEnvironment` rejects this configuration before a worker can
    // start. Keeping the readiness check protects manually constructed test
    // runtimes and future configuration sources as well.
    if (!Number.isSafeInteger(environment.releaseGeneration) || environment.releaseGeneration < 1) {
      problems.push("release_generation_unavailable");
    }
    if (!environment.releaseFenceSecret) problems.push("release_fence_unavailable");
  }
  if (environment.deploymentMode === "production") {
    if (environment.releasePhase !== "production" && environment.releasePhase !== "canary" && environment.releasePhase !== "dark") {
      problems.push("release_phase_invalid");
    }
    // Explicit flags are required in production. Missing values resolve false
    // above, but readiness must make that safe default visible to operators.
    if (!environment.policyExplicit) {
      problems.push("side_effect_policy_unset");
    }
  }
  return Object.freeze(problems);
}

export function inferStudioDeploymentMode(source: EnvironmentSource): StudioDeploymentMode {
  const vercel = optional(source.VERCEL_ENV);
  if (vercel === "production") return "production";
  if (vercel === "preview") return "preview";
  return optional(source.NODE_ENV) === "production" ? "production" : "development";
}

export function explicitBoolean(value: string | undefined): boolean {
  return value?.trim() === "true";
}

function hasExplicitBoolean(value: string | undefined): boolean {
  return value?.trim() === "true" || value?.trim() === "false";
}

function parseReleasePhase(value: string | undefined): StudioReleasePhase {
  switch (value?.trim()) {
    case "canary":
      return "canary";
    case "production":
      return "production";
    case "dark":
    case undefined:
    case "":
      return "dark";
    default:
      throw new Error("AIR_RELEASE_PHASE is invalid");
  }
}

function parseReleaseGeneration(value: string | undefined, releasePhase: StudioReleasePhase): number {
  const normalized = value?.trim();
  if (!normalized) {
    if (releasePhase === "dark") return 0;
    throw new Error("an active release phase requires a positive AIR_RELEASE_GENERATION");
  }
  if (!/^(?:0|[1-9]\d*)$/.test(normalized)) {
    throw new Error("AIR_RELEASE_GENERATION is invalid");
  }
  const parsed = Number(normalized);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error("AIR_RELEASE_GENERATION is invalid");
  }
  if (isReleasePhaseActive(releasePhase) && parsed < 1) {
    throw new Error("an active release phase requires a positive AIR_RELEASE_GENERATION");
  }
  return parsed;
}

function isReleasePhaseActive(releasePhase: StudioReleasePhase): boolean {
  return releasePhase === "canary" || releasePhase === "production";
}

/** Missing/invalid monetary limits intentionally become zero, never unlimited. */
export function usdToCents(value: string | undefined): number {
  const normalized = value?.trim();
  if (!normalized || !/^\d+(?:\.\d{1,2})?$/.test(normalized)) return 0;
  const [whole, fraction = ""] = normalized.split(".");
  const cents = Number(whole) * 100 + Number(`${fraction}00`.slice(0, 2));
  return Number.isSafeInteger(cents) && cents >= 0 && cents <= 100_000_000 ? cents : 0;
}

function positiveInteger(value: string | undefined, fallback: number, maximum: number): number {
  const normalized = value?.trim();
  if (!normalized || !/^\d+$/.test(normalized)) return fallback;
  const parsed = Number(normalized);
  return Number.isSafeInteger(parsed) && parsed >= 1 && parsed <= maximum ? parsed : fallback;
}

function optional(value: string | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized || undefined;
}

function validateSecretLikeValue(value: string, label: string): void {
  if (value.length > 4_096 || /[\r\n]/.test(value)) throw new Error(`${label} is invalid`);
}

function validateHeaderValue(value: string, label: string): void {
  if (value.length > 512 || /[\u0000\r\n]/.test(value)) throw new Error(`${label} is invalid`);
}

function validateRestUrl(value: string): void {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("UPSTASH_REDIS_REST_URL is invalid");
  }
  if (url.protocol !== "https:" || !url.hostname || url.username || url.password || url.search || url.hash) {
    throw new Error("UPSTASH_REDIS_REST_URL is invalid");
  }
}

function validateRedisUrl(value: string): void {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("REDIS_URL is invalid");
  }
  if ((url.protocol !== "redis:" && url.protocol !== "rediss:") || !url.hostname || url.username) {
    throw new Error("REDIS_URL is invalid");
  }
}

function validatePublicBaseUrl(value: string): void {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("AIR_PUBLIC_BASE_URL is invalid");
  }
  if (url.protocol !== "https:" || !url.hostname || url.username || url.password || url.search || url.hash) {
    throw new Error("AIR_PUBLIC_BASE_URL is invalid");
  }
}
