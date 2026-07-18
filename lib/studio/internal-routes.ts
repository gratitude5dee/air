import {
  canAcceptStudioJobs,
  canDeliverStudioMessages,
  canRecoverStudioProvider,
  canSubmitStudioProvider,
  loadStudioEnvironment,
  studioReadinessProblems,
  type StudioEnvironment,
} from "./config.js";
import { AIR_MODEL_REGISTRY, AIR_MODEL_REGISTRY_VERSION, listEnabledModels } from "./model-registry.js";
import { getStudioRuntime, type StudioRuntime } from "./runtime.js";
import { authorizeStudioDrain } from "./webhook-handler.js";

export type StudioDrain = (runtime: StudioRuntime) => Promise<void>;

export interface StudioInternalRouteOptions {
  readonly loadEnvironment?: (() => StudioEnvironment) | undefined;
  readonly getRuntime?: (() => Promise<StudioRuntime>) | undefined;
}

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
  const providerWorkEnabled = canAcceptStudioJobs(environment)
    || canSubmitStudioProvider(environment)
    || canRecoverStudioProvider(environment);
  if (canAcceptStudioJobs(environment) && enabledModels === 0) problems.push("no_enabled_models");

  let redis: "reachable" | "unreachable" | "unknown" = "unknown";
  let cron: "fresh" | "stale" | "missing" | "invalid" | "not_scheduled" | "unreachable" | "unknown" = "unknown";
  if (!canProbeRedis(environment)) {
    problems.push("redis_reachability_unknown", "cron_freshness_unknown");
  } else {
    let runtime: StudioRuntime;
    try {
      runtime = await (options.getRuntime ?? getStudioRuntime)();
    } catch {
      redis = "unreachable";
      cron = "unreachable";
      problems.push("redis_unavailable", "cron_freshness_unknown");
      return readinessResponse(environment, enabledModels, redis, cron, problems);
    }
    try {
      const freshness = await runtime.store.getCronFreshness(environment.deploymentMode);
      redis = "reachable";
      if (!runtime.transport.verified) problems.push("transport_unverified");
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
    } catch {
      redis = "unreachable";
      cron = "unreachable";
      problems.push("redis_unavailable", "cron_freshness_unknown");
    }
  }

  return readinessResponse(environment, enabledModels, redis, cron, problems);
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
  if (request.method !== "POST") return methodNotAllowedResponse("POST");

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

  const previousGeneration = parseReleaseFenceIfMatch(request.headers.get("if-match"));
  if (previousGeneration === undefined) return privateEmptyResponse(409);

  let runtime: StudioRuntime;
  try {
    runtime = await (options.getRuntime ?? getStudioRuntime)();
  } catch {
    return privateEmptyResponse(503);
  }

  try {
    const outcome = await runtime.store.advanceReleaseFence(previousGeneration);
    if (outcome.kind === "advanced" || outcome.kind === "current") {
      return Response.json({ accepted: true }, { status: 202, headers: { "cache-control": "no-store" } });
    }
    return privateEmptyResponse(409);
  } catch {
    return privateEmptyResponse(503);
  }
}

function readinessResponse(
  environment: StudioEnvironment,
  enabledModels: number,
  redis: "reachable" | "unreachable" | "unknown",
  cron: "fresh" | "stale" | "missing" | "invalid" | "not_scheduled" | "unreachable" | "unknown",
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
        acceptNewJobs: canAcceptStudioJobs(environment),
        allowProviderSubmit: canSubmitStudioProvider(environment),
        allowProviderRecovery: canRecoverStudioProvider(environment),
        deliveryEnabled: canDeliverStudioMessages(environment),
      },
      redis,
      cron,
      problems,
    },
    { status: problems.length === 0 ? 200 : 503, headers: { "cache-control": "no-store" } },
  );
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

function methodNotAllowedResponse(allowed: string): Response {
  return new Response(null, {
    status: 405,
    headers: { allow: allowed, "cache-control": "no-store" },
  });
}

function privateEmptyResponse(status: 401 | 409 | 503): Response {
  return new Response(null, { status, headers: { "cache-control": "no-store" } });
}
