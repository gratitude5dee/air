import { createQueueCipher, type QueueCipher } from "../crypto.js";
import { connectRedisFromUrl, type ConnectedRedis } from "../redis.js";
import {
  canAcceptStudioJobs,
  canRecoverPaidStudioProvider,
  canSubmitPaidStudioProvider,
  loadStudioEnvironment,
  studioReadinessProblems,
  type StudioEnvironment,
} from "./config.js";
import { UnverifiedPhotonTransport, type StudioIMessageTransport } from "./imessage-transport.js";
import { AIR_MODEL_REGISTRY, type AirModelRegistry } from "./model-registry.js";
import { assertNoPhotonTelemetryExporters } from "./photon-telemetry.js";
import { UnprovisionedStudioProvider, type StudioProviderPort } from "./provider-port.js";
import { StudioStore } from "./store.js";

export const AIR_STUDIO_VERSION = "2";

export interface StudioRuntime {
  readonly environment: StudioEnvironment;
  readonly cipher: QueueCipher;
  readonly store: StudioStore;
  readonly transport: StudioIMessageTransport;
  /** Never live by default; the executor rejects this unverified provider. */
  readonly provider: StudioProviderPort;
  /** Committed registry; tests may inject a reviewed fixture without mutating code defaults. */
  readonly registry?: AirModelRegistry | undefined;
  readonly storageTransport: "rest" | "tcp";
  readiness(expectedRegistryVersion: string): readonly string[];
  close(): Promise<void>;
}

let runtimePromise: Promise<StudioRuntime> | undefined;
let runtimeOverride: StudioRuntime | undefined;

/** Test-only injection. Production calls always connect to the configured existing Redis resource. */
export function __setStudioRuntimeForTests(runtime: StudioRuntime | undefined): void {
  runtimeOverride = runtime;
  runtimePromise = undefined;
}

export async function getStudioRuntime(): Promise<StudioRuntime> {
  if (runtimeOverride) return runtimeOverride;
  if (!runtimePromise) {
    runtimePromise = createStudioRuntime();
    void runtimePromise.catch(() => {
      runtimePromise = undefined;
    });
  }
  return runtimePromise;
}

export async function createStudioRuntime(
  environment: StudioEnvironment = loadStudioEnvironment(),
  transport?: StudioIMessageTransport,
  provider: StudioProviderPort = new UnprovisionedStudioProvider(),
): Promise<StudioRuntime> {
  if (!environment.idHashSecret || !environment.queueEncryptionKey || environment.storageTransport === "unconfigured") {
    throw new Error("studio runtime configuration is incomplete");
  }
  const configuredTransport = transport ?? await createConfiguredIMessageTransport(environment);
  const connection = await connectExistingRedis(environment);
  return runtimeFromConnectedRedis(environment, connection, configuredTransport, provider);
}

export function studioPublicStatusResponse(): Response {
  return Response.json(
    // This branded, dependency-free shape is an external compatibility
    // contract. The protected readiness route is the source of truth for the
    // current dark/active release state; do not overload this public product
    // description with deployment configuration.
    { name: "Air", status: "private allowlisted iMessage studio" },
    { headers: { "cache-control": "no-store", link: '</favicon.ico>; rel="icon"; type="image/x-icon"' } },
  );
}

export function studioHealthResponse(): Response {
  return Response.json(
    { ok: true, version: AIR_STUDIO_VERSION },
    { headers: { "cache-control": "no-store" } },
  );
}

async function connectExistingRedis(environment: StudioEnvironment): Promise<ConnectedRedis> {
  // connectRedisFromUrl ignores the TCP URL when the validated REST pair is
  // present. The placeholder is never parsed or used on that path.
  return connectRedisFromUrl(environment.redisUrl ?? "rediss://unused.invalid", {
    ...(environment.upstashRedisRestUrl && environment.upstashRedisRestToken
      ? { upstashRestUrl: environment.upstashRedisRestUrl, upstashRestToken: environment.upstashRedisRestToken }
      : {}),
  });
}

/**
 * Importing the Photon client is deferred until a real, fully configured
 * messaging runtime is requested. Dark previews with incomplete credentials
 * retain the dependency-free, fail-closed transport instead of initializing a
 * client that could emit unredacted telemetry.
 */
async function createConfiguredIMessageTransport(environment: StudioEnvironment): Promise<StudioIMessageTransport> {
  if (!environment.imessageProjectId || !environment.imessageProjectSecret || !environment.imessageWebhookSecret) {
    return new UnverifiedPhotonTransport();
  }
  // This module has no Photon imports, so unredacted telemetry is rejected
  // before any provider package can initialize against process-wide settings.
  assertNoPhotonTelemetryExporters();
  const { createPhotonIMessageTransport } = await import("./photon-transport.js");
  return createPhotonIMessageTransport({
    projectId: environment.imessageProjectId,
    projectSecret: environment.imessageProjectSecret,
    webhookSecret: environment.imessageWebhookSecret,
  });
}

function runtimeFromConnectedRedis(
  environment: StudioEnvironment,
  connection: ConnectedRedis,
  transport: StudioIMessageTransport,
  provider: StudioProviderPort,
): StudioRuntime {
  const cipher = createQueueCipher(environment.queueEncryptionKey as string);
  const store = new StudioStore({
    redis: connection.redis,
    cipher,
    identityHashSecret: environment.idHashSecret as string,
    releaseStamp: {
      generation: environment.releaseGeneration,
      phase: environment.releasePhase,
    },
  });
  return Object.freeze({
    environment,
    cipher,
    store,
    transport,
    provider,
    registry: AIR_MODEL_REGISTRY,
    storageTransport: environment.storageTransport === "rest" ? "rest" : "tcp",
    readiness(expectedRegistryVersion: string): readonly string[] {
      const problems = [...studioReadinessProblems(environment, expectedRegistryVersion)];
      if (!transport.verified) problems.push("transport_unverified");
      if (
        (canAcceptStudioJobs(environment) || canSubmitPaidStudioProvider(environment) || canRecoverPaidStudioProvider(environment))
        && !provider.verified
      ) problems.push("provider_unprovisioned");
      return Object.freeze(problems);
    },
    async close(): Promise<void> {
      await connection.close();
    },
  });
}
