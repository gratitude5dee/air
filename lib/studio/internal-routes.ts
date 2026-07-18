import { loadStudioEnvironment, studioReadinessProblems, type StudioEnvironment } from "./config.js";
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
  } catch {
    return privateEmptyResponse(503);
  }

  return Response.json({ accepted: true }, { status: 202, headers: { "cache-control": "no-store" } });
}

/**
 * Protected, secret-safe readiness diagnostic. It deliberately does not
 * connect to Redis or GMI: an operator needs to see dark-deploy blockers even
 * when the backing service is intentionally absent or disabled.
 */
export function handleStudioReadiness(
  request: Request,
  options: StudioInternalRouteOptions = {},
): Response {
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
  // The only bundled transport intentionally fails closed until the required
  // real-line Photon transport spike has passed.
  problems.push("transport_unverified");
  if (enabledModels === 0) problems.push("no_enabled_models");

  return Response.json(
    {
      ok: problems.length === 0,
      releasePhase: environment.releasePhase,
      registryVersion: AIR_MODEL_REGISTRY_VERSION,
      enabledModels,
      sideEffects: environment.policy,
      problems,
    },
    { status: problems.length === 0 ? 200 : 503, headers: { "cache-control": "no-store" } },
  );
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

function privateEmptyResponse(status: 401 | 503): Response {
  return new Response(null, { status, headers: { "cache-control": "no-store" } });
}
