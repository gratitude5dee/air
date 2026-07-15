import { createQueueCipher, type QueueCipher } from "./crypto.js";
import { DurableInbox } from "./durable-inbox.js";
import { loadAirEnvironment, type AirEnvironment } from "./env.js";
import { RateLimiter } from "./rate-limit.js";
import { connectRedisFromUrl, type RedisLike } from "./redis.js";

/**
 * Shared, lazy runtime for Air's signed webhook and recovery cron.  It owns
 * only encrypted Redis state; Zap owns generation, provider credentials, and
 * media persistence.
 */
export interface AirRuntime {
  readonly environment: AirEnvironment;
  readonly redis: RedisLike;
  readonly clock: () => number;
  readonly cipher: QueueCipher;
  readonly inbox: DurableInbox;
  readonly limiter: RateLimiter;
  readonly invalidate?: (() => void) | undefined;
}

let runtimeOverride: AirRuntime | undefined;
let runtimePromise: Promise<AirRuntime> | undefined;

export function createAirRuntime(input: {
  readonly environment: AirEnvironment;
  readonly redis: RedisLike;
  readonly clock?: (() => number) | undefined;
  readonly invalidate?: (() => void) | undefined;
}): AirRuntime {
  assertQueueEnvironment(input.environment);
  const clock = input.clock ?? Date.now;
  const cipher = createQueueCipher(input.environment.queueEncryptionKey as string);
  return Object.freeze({
    environment: input.environment,
    redis: input.redis,
    clock,
    cipher,
    inbox: new DurableInbox({
      redis: input.redis,
      cipher,
      identityHashSecret: input.environment.idHashSecret as string,
      clock,
    }),
    limiter: new RateLimiter(input.redis, { now: clock }),
    ...(input.invalidate ? { invalidate: input.invalidate } : {}),
  });
}

/** Test-only injection; production always uses a connected Redis client. */
export function __setAirRuntimeForTests(runtime: AirRuntime | undefined): void {
  runtimeOverride = runtime;
  runtimePromise = undefined;
}

export async function getAirRuntime(): Promise<AirRuntime> {
  if (runtimeOverride) return runtimeOverride;
  if (!runtimePromise) {
    let pending: Promise<AirRuntime>;
    pending = (async () => {
      const environment = loadAirEnvironment();
      assertQueueEnvironment(environment);
      const connection = await connectRedisFromUrl(
        environment.redisUrl as string,
        environment.upstashRedisRestUrl && environment.upstashRedisRestToken
          ? {
            upstashRestUrl: environment.upstashRedisRestUrl,
            upstashRestToken: environment.upstashRedisRestToken,
          }
          : undefined,
      );
      let invalidated = false;
      const invalidate = () => {
        if (invalidated) return;
        invalidated = true;
        if (runtimePromise === pending) runtimePromise = undefined;
        connection.destroy();
      };
      connection.onOperationalError(invalidate);
      return createAirRuntime({ environment, redis: connection.redis, invalidate });
    })();
    runtimePromise = pending;
    void pending.catch(() => {
      if (runtimePromise === pending) runtimePromise = undefined;
    });
  }
  return runtimePromise;
}

export function assertQueueEnvironment(environment: AirEnvironment): void {
  if (
    !environment.imessageProjectId ||
    !environment.imessageProjectSecret ||
    !environment.imessageWebhookSecret ||
    !environment.redisUrl ||
    !environment.zapAirServiceUrl ||
    !environment.zapAirServiceToken ||
    !environment.idHashSecret ||
    !environment.queueEncryptionKey
  ) {
    throw new Error("Air video runtime configuration is incomplete");
  }
}

export function publicStatusResponse(): Response {
  return Response.json(
    {
      name: "Air",
      status: "allowlisted iMessage Seedance video generation",
    },
    { headers: { "cache-control": "no-store" } },
  );
}

export function healthResponse(): Response {
  return Response.json(
    { ok: true, version: "1" },
    { headers: { "cache-control": "no-store" } },
  );
}
