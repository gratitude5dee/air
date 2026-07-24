import {
  canAcceptStudioJobs,
  canDeliverStudioMessages,
  canRecoverPaidStudioProvider,
  canSubmitPaidStudioProvider,
  loadStudioEnvironment,
  studioReadinessProblems,
  type StudioEnvironment,
} from "./config.js";
import { AIR_MODEL_REGISTRY, AIR_MODEL_REGISTRY_VERSION, listEnabledModels } from "./model-registry.js";
import { getStudioDeliveryTransport, getStudioRuntime, type StudioRuntime } from "./runtime.js";
import type {
  StudioInboxFifoActivateResult,
  StudioInboxFifoBeginResult,
  StudioInboxFifoPrepareResult,
  StudioInboxFifoStatus,
  StudioReleaseFenceStatus,
} from "./store.js";
import { authorizeStudioDrain } from "./webhook-handler.js";

export type StudioDrain = (runtime: StudioRuntime) => Promise<void>;

export interface StudioInternalRouteOptions {
  readonly loadEnvironment?: (() => StudioEnvironment) | undefined;
  readonly getRuntime?: (() => Promise<StudioRuntime>) | undefined;
}

type StudioReadinessReleaseFence = "not_required" | "current" | "missing" | "mismatched" | "invalid" | "unreachable" | "unknown";
type StudioReadinessInboxFifo = "not_required" | "fifo" | "legacy" | "holding" | "invalid" | "unreachable" | "unknown";

/**
 * Vercel cron and operator recovery endpoint. Authorization happens before a
 * Redis connection or worker run, so an unauthenticated request cannot create
 * a queue side effect or use configuration failures as an oracle.
 */
export async function handleStudioDrain(
  request: Request,
  drain: StudioDrain,
  options: StudioInternalRouteOptions = {},
): Promise<Response> {
  // Only Vercel's Cron GET and the documented operator POST may make the
  // durable drain progress. A bearer credential must not turn an arbitrary
  // HTTP method into a side-effecting recovery entrypoint.
  if (request.method !== "GET" && request.method !== "POST") {
    return methodNotAllowedResponse("GET, POST");
  }
  const environment = loadAuthorizedEnvironment(request, options);
  if (environment === "unauthorized") return privateEmptyResponse(401);
  if (environment === "unavailable") return privateEmptyResponse(503);

  let runtime: StudioRuntime;
  try {
    runtime = await (options.getRuntime ?? getStudioRuntime)();
  } catch {
    return privateEmptyResponse(503);
  }

  try {
    await drain(runtime);
    // Vercel Cron invokes the configured route with GET. A successful manual
    // POST deliberately does not make the production Cron look healthy.
    if (request.method === "GET") await runtime.store.recordCronHeartbeat(environment.deploymentMode);
  } catch {
    return privateEmptyResponse(503);
  }

  return Response.json({ accepted: true }, { status: 202, headers: { "cache-control": "no-store" } });
}

/**
 * Protected, secret-safe readiness diagnostic. It verifies the configured
 * Redis path through the operational heartbeat key, but never contacts a
 * provider or reads any tenant record.
 */
export async function handleStudioReadiness(
  request: Request,
  options: StudioInternalRouteOptions = {},
): Promise<Response> {
  if (request.method !== "GET") {
    return new Response(null, {
      status: 405,
      headers: { allow: "GET", "cache-control": "no-store" },
    });
  }
  const environment = loadAuthorizedEnvironment(request, options);
  if (environment === "unauthorized") return privateEmptyResponse(401);
  if (environment === "unavailable") return privateEmptyResponse(503);

  const enabledModels = listEnabledModels(AIR_MODEL_REGISTRY).length;
  const problems = [...studioReadinessProblems(environment, AIR_MODEL_REGISTRY_VERSION)];
  // A delivery-only release is intentionally healthy with no provider or
  // selectable model. Requiring either before its policy is enabled would
  // misreport the safe, zero-provider iMessage smoke path as broken.
  const admissionConfigured = canAcceptStudioJobs(environment);
  const providerWorkEnabled = admissionConfigured
    || canSubmitPaidStudioProvider(environment)
    || canRecoverPaidStudioProvider(environment);
  const activeSideEffectsEnabled = providerWorkEnabled || canDeliverStudioMessages(environment);
  if (admissionConfigured && enabledModels === 0) problems.push("no_enabled_models");

  let redis: "reachable" | "unreachable" | "unknown" = "unknown";
  let cron: "fresh" | "stale" | "missing" | "invalid" | "not_scheduled" | "unreachable" | "unknown" = "unknown";
  let releaseFence: StudioReadinessReleaseFence = activeSideEffectsEnabled ? "unknown" : "not_required";
  let inboxFifo: StudioReadinessInboxFifo = admissionConfigured ? "unknown" : "not_required";
  if (!canProbeRedis(environment)) {
    problems.push("redis_reachability_unknown", "cron_freshness_unknown");
    if (activeSideEffectsEnabled) problems.push("release_fence_unavailable");
    if (admissionConfigured) problems.push("inbox_fifo_unavailable");
  } else {
    let runtime: StudioRuntime;
    try {
      runtime = await (options.getRuntime ?? getStudioRuntime)();
    } catch {
      redis = "unreachable";
      cron = "unreachable";
      releaseFence = activeSideEffectsEnabled ? "unreachable" : "not_required";
      inboxFifo = admissionConfigured ? "unreachable" : "not_required";
      problems.push("redis_unavailable", "cron_freshness_unknown");
      if (activeSideEffectsEnabled) problems.push("release_fence_unavailable");
      if (admissionConfigured) problems.push("inbox_fifo_unavailable");
      return readinessResponse(environment, enabledModels, redis, cron, releaseFence, inboxFifo, problems);
    }
    try {
      const freshness = await runtime.store.getCronFreshness(environment.deploymentMode);
      redis = "reachable";
      if (admissionConfigured) {
        try {
          const migration = await runtime.store.getInboxFifoMigrationStatus();
          if (migration.kind === "invalid") {
            inboxFifo = "invalid";
            problems.push("inbox_fifo_invalid");
          } else {
            inboxFifo = migration.phase;
            if (migration.phase !== "fifo") problems.push(`inbox_fifo_${migration.phase}`);
          }
        } catch {
          inboxFifo = "unreachable";
          problems.push("inbox_fifo_unavailable");
        }
      }
      // Inbound receipt persistence deliberately never mints an outbound
      // Photon token. An authenticated readiness probe, however, is the right
      // place to verify that a currently active delivery release can obtain
      // one without putting that failure in front of signed webhook durability.
      let transportVerified = runtime.transport.verified;
      if (!transportVerified && runtime.getDeliveryTransport !== undefined && canDeliverStudioMessages(environment)) {
        try {
          transportVerified = (await getStudioDeliveryTransport(runtime)).verified;
        } catch {
          transportVerified = false;
        }
      }
      if (!transportVerified && runtime.getDeliveryTransport === undefined) {
        problems.push("transport_unverified");
      } else if (!transportVerified && canDeliverStudioMessages(environment)) {
        problems.push("transport_unverified");
      }
      if (providerWorkEnabled && !runtime.provider.verified) problems.push("provider_unprovisioned");
      // Vercel only schedules Cron jobs on production deployments. Preview and
      // development readiness still verify Redis but must not claim a missed
      // scheduled invocation.
      if (environment.deploymentMode === "production") {
        cron = freshness;
        if (cron === "stale") problems.push("cron_stale");
        if (cron === "missing") problems.push("cron_missing");
        if (cron === "invalid") problems.push("cron_invalid");
      } else {
        cron = "not_scheduled";
      }

      if (activeSideEffectsEnabled) {
        try {
          releaseFence = releaseFenceReadinessStatus(
            await runtime.store.getReleaseFenceStatus(),
            environment,
          );
          if (releaseFence !== "current") problems.push(`release_fence_${releaseFence}`);
        } catch {
          releaseFence = "unreachable";
          problems.push("release_fence_unavailable");
        }
      }
    } catch {
      redis = "unreachable";
      cron = "unreachable";
      releaseFence = activeSideEffectsEnabled ? "unreachable" : "not_required";
      inboxFifo = admissionConfigured ? "unreachable" : "not_required";
      problems.push("redis_unavailable", "cron_freshness_unknown");
      if (activeSideEffectsEnabled) problems.push("release_fence_unavailable");
      if (admissionConfigured) problems.push("inbox_fifo_unavailable");
    }
  }

  return readinessResponse(environment, enabledModels, redis, cron, releaseFence, inboxFifo, problems);
}

/**
 * Authenticated compare-and-swap for the global release fence. This endpoint
 * deliberately returns no fence generation, configuration state, or error
 * detail: callers must already know the expected generation to advance it.
 */
export async function handleStudioReleaseFence(
  request: Request,
  options: StudioInternalRouteOptions = {},
): Promise<Response> {
  if (request.method !== "POST" && request.method !== "GET") return methodNotAllowedResponse("GET, POST");

  let environment: StudioEnvironment;
  try {
    environment = (options.loadEnvironment ?? loadStudioEnvironment)();
  } catch {
    return privateEmptyResponse(503);
  }
  // Reuse the constant-time bearer comparison used by the protected Cron
  // route, but authenticate against the independent release-fence secret.
  if (!authorizeStudioDrain(request, environment.releaseFenceSecret)) {
    return privateEmptyResponse(401);
  }

  const previousGeneration = request.method === "POST"
    ? parseReleaseFenceIfMatch(request.headers.get("if-match"))
    : undefined;
  if (request.method === "POST" && previousGeneration === undefined) return privateEmptyResponse(409);

  let runtime: StudioRuntime;
  try {
    runtime = await (options.getRuntime ?? getStudioRuntime)();
  } catch {
    return privateEmptyResponse(503);
  }

  if (request.method === "GET") {
    try {
      const status = await runtime.store.getReleaseFenceStatus();
      return Response.json(status, {
        status: status.kind === "invalid" ? 503 : 200,
        headers: { "cache-control": "no-store" },
      });
    } catch {
      return privateEmptyResponse(503);
    }
  }

  try {
    const outcome = await runtime.store.advanceReleaseFence(previousGeneration as number);
    if (outcome.kind === "advanced" || outcome.kind === "current") {
      return Response.json({ accepted: true }, { status: 202, headers: { "cache-control": "no-store" } });
    }
    return privateEmptyResponse(409);
  } catch {
    return privateEmptyResponse(503);
  }
}

/**
 * Protected, one-way operator control plane for the inbox FIFO migration.
 * The only mutation verbs are the reviewed legacy -> holding -> fifo steps;
 * this endpoint deliberately has no force, skip, reset, or arbitrary phase
 * setter. Its status and outcomes are bounded counters and enum values only.
 */
export async function handleStudioInboxFifoMigration(
  request: Request,
  options: StudioInternalRouteOptions = {},
): Promise<Response> {
  if (request.method !== "GET" && request.method !== "POST") return methodNotAllowedResponse("GET, POST");

  let environment: StudioEnvironment;
  try {
    environment = (options.loadEnvironment ?? loadStudioEnvironment)();
  } catch {
    return privateEmptyResponse(503);
  }
  // This is intentionally independent from the inbound HMAC and Cron bearer:
  // a delivery source or scheduled worker must not gain migration authority.
  if (!authorizeStudioDrain(request, environment.releaseFenceSecret)) return privateEmptyResponse(401);

  const action = request.method === "POST"
    ? parseInboxFifoMigrationAction(request.headers.get("x-air-fifo-migration-action"))
    : undefined;
  // Parse the exact action before opening Redis. In particular, a future
  // caller cannot smuggle a force/skip/reset operation through a broad JSON
  // command surface or use malformed input to cause a queue side effect.
  if (request.method === "POST" && action === undefined) return privateEmptyResponse(400);

  let runtime: StudioRuntime;
  try {
    runtime = await (options.getRuntime ?? getStudioRuntime)();
  } catch {
    return privateEmptyResponse(503);
  }

  try {
    if (request.method === "GET") {
      const status = await runtime.store.getInboxFifoMigrationStatus();
      return inboxFifoMigrationStatusResponse(status);
    }

    switch (action) {
      case "begin":
        return inboxFifoMigrationMutationResponse(await runtime.store.beginInboxFifoMigration());
      case "prepare":
        return inboxFifoMigrationMutationResponse(await runtime.store.prepareInboxFifoHoldingScopes());
      case "activate":
        return inboxFifoMigrationMutationResponse(await runtime.store.activateInboxFifoMigration());
      default:
        // `action` is checked above. Keep a fail-closed guard if this handler
        // is ever widened without updating the exact parser.
        return privateEmptyResponse(400);
    }
  } catch {
    return privateEmptyResponse(503);
  }
}

function readinessResponse(
  environment: StudioEnvironment,
  enabledModels: number,
  redis: "reachable" | "unreachable" | "unknown",
  cron: "fresh" | "stale" | "missing" | "invalid" | "not_scheduled" | "unreachable" | "unknown",
  releaseFence: StudioReadinessReleaseFence,
  inboxFifo: StudioReadinessInboxFifo,
  problems: readonly string[],
): Response {
  return Response.json(
    {
      ok: problems.length === 0,
      releasePhase: environment.releasePhase,
      registryVersion: AIR_MODEL_REGISTRY_VERSION,
      enabledModels,
      sideEffects: environment.policy,
      effectiveSideEffects: {
        acceptNewJobs: canAcceptStudioJobs(environment) && inboxFifo === "fifo",
        allowProviderSubmit: canSubmitPaidStudioProvider(environment),
        allowProviderRecovery: canRecoverPaidStudioProvider(environment),
        deliveryEnabled: canDeliverStudioMessages(environment),
      },
      redis,
      cron,
      releaseFence,
      inboxFifo,
      problems,
    },
    { status: problems.length === 0 ? 200 : 503, headers: { "cache-control": "no-store" } },
  );
}

/** Readiness exposes no fence generation or phase, only whether it matches this deployment. */
function releaseFenceReadinessStatus(
  status: StudioReleaseFenceStatus,
  environment: Pick<StudioEnvironment, "releaseGeneration" | "releasePhase">,
): Exclude<StudioReadinessReleaseFence, "not_required" | "unreachable" | "unknown"> {
  if (status.kind === "missing" || status.kind === "invalid") return status.kind;
  return status.stamp.generation === environment.releaseGeneration && status.stamp.phase === environment.releasePhase
    ? "current"
    : "mismatched";
}

function canProbeRedis(environment: StudioEnvironment): boolean {
  return environment.storageTransport !== "unconfigured"
    && Boolean(environment.idHashSecret && environment.queueEncryptionKey);
}

function loadAuthorizedEnvironment(
  request: Request,
  options: StudioInternalRouteOptions,
): StudioEnvironment | "unauthorized" | "unavailable" {
  let environment: StudioEnvironment;
  try {
    environment = (options.loadEnvironment ?? loadStudioEnvironment)();
  } catch {
    return "unavailable";
  }
  return authorizeStudioDrain(request, environment.cronSecret) ? environment : "unauthorized";
}

/** Accept only the literal CAS precondition; whitespace and HTTP-list syntax are invalid. */
function parseReleaseFenceIfMatch(value: string | null): number | undefined {
  if (value === "-1") return -1;
  if (!value || !/^(?:0|[1-9]\d*)$/.test(value)) return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}

/** Header values are literal control-plane verbs after normal HTTP OWS normalization. */
function parseInboxFifoMigrationAction(value: string | null): "begin" | "prepare" | "activate" | undefined {
  switch (value) {
    case "begin":
    case "prepare":
    case "activate":
      return value;
    default:
      return undefined;
  }
}

function inboxFifoMigrationStatusResponse(status: StudioInboxFifoStatus): Response {
  return Response.json(status, {
    status: status.kind === "current" ? 200 : 503,
    headers: { "cache-control": "no-store" },
  });
}

function inboxFifoMigrationMutationResponse(
  outcome: StudioInboxFifoBeginResult | StudioInboxFifoPrepareResult | StudioInboxFifoActivateResult,
): Response {
  const status = isAcceptedInboxFifoMigrationOutcome(outcome)
    ? 202
    : outcome.kind === "invalid"
      ? 503
      : 409;
  return Response.json(outcome, { status, headers: { "cache-control": "no-store" } });
}

function isAcceptedInboxFifoMigrationOutcome(
  outcome: StudioInboxFifoBeginResult | StudioInboxFifoPrepareResult | StudioInboxFifoActivateResult,
): boolean {
  switch (outcome.kind) {
    case "holding":
    case "already_holding":
    case "already_fifo":
    case "prepared":
    case "activated":
      return true;
    case "legacy_not_drained":
    case "blocked":
    case "not_holding":
    case "holding_not_prepared":
    case "invalid":
      return false;
  }
}

function methodNotAllowedResponse(allowed: string): Response {
  return new Response(null, {
    status: 405,
    headers: { allow: allowed, "cache-control": "no-store" },
  });
}

function privateEmptyResponse(status: 400 | 401 | 409 | 503): Response {
  return new Response(null, { status, headers: { "cache-control": "no-store" } });
}
