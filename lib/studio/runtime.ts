import { createQueueCipher, type QueueCipher } from "../crypto.js";
import { connectRedisFromUrl, type ConnectedRedis } from "../redis.js";
import { loadStudioEnvironment, studioReadinessProblems, type StudioEnvironment } from "./config.js";
import { UnverifiedPhotonTransport, type StudioIMessageTransport } from "./imessage-transport.js";
import { StudioStore } from "./store.js";

export const AIR_STUDIO_VERSION = "2";

export interface StudioRuntime {
  readonly environment: StudioEnvironment;
  readonly cipher: QueueCipher;
  readonly store: StudioStore;
  readonly transport: StudioIMessageTransport;
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
  transport: StudioIMessageTransport = new UnverifiedPhotonTransport(),
): Promise<StudioRuntime> {
  if (!environment.idHashSecret || !environment.queueEncryptionKey || environment.storageTransport === "unconfigured") {
    throw new Error("studio runtime configuration is incomplete");
  }
  const connection = await connectExistingRedis(environment);
  return runtimeFromConnectedRedis(environment, connection, transport);
}

export function studioPublicStatusResponse(): Response {
  return Response.json(
    // This branded, dependency-free shape is an external compatibility
    // contract. The protected readiness route is the source of truth for the
    // current dark/active release state; do not overload this public product
    // description with deployment configuration.
    { name: "Air", status: "allowlisted iMessage image and video generation" },
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

function runtimeFromConnectedRedis(
  environment: StudioEnvironment,
  connection: ConnectedRedis,
  transport: StudioIMessageTransport,
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
    storageTransport: environment.storageTransport === "rest" ? "rest" : "tcp",
    readiness(expectedRegistryVersion: string): readonly string[] {
      const problems = [...studioReadinessProblems(environment, expectedRegistryVersion)];
      if (!transport.verified) problems.push("transport_unverified");
      return Object.freeze(problems);
    },
    async close(): Promise<void> {
      await connection.close();
    },
  });
}
