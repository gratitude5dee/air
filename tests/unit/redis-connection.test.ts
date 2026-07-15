import { describe, expect, it } from "vitest";
import {
  connectRedisFromUrl,
  createNodeRedisAdapter,
  createUpstashRedisAdapter,
  type UpstashRedisCompatibleClient,
  type UpstashRedisRestOptions,
} from "../../lib/redis.js";

function hangingClient() {
  let errorListener: ((error: Error) => void) | undefined;
  let destroyed = false;
  return {
    get isOpen() {
      return false;
    },
    async get(): Promise<null> { return null; },
    async set(): Promise<null> { return null; },
    async del(): Promise<number> { return 0; },
    async incr(): Promise<number> { return 0; },
    async expire(): Promise<boolean> { return false; },
    async pTTL(): Promise<number> { return -2; },
    async zAdd(): Promise<number> { return 0; },
    async zRangeByScore(): Promise<string[]> { return []; },
    async zCard(_key: string): Promise<number> { return 0; },
    async zRem(): Promise<number> { return 0; },
    async zRemRangeByScore(): Promise<number> { return 0; },
    async eval(): Promise<unknown> { return 0; },
    connect(): Promise<void> { return new Promise(() => undefined); },
    async close(): Promise<void> { return undefined; },
    destroy(): void { destroyed = true; },
    on(event: "error", listener: (error: Error) => void): void {
      if (event === "error") errorListener = listener;
    },
    get destroyed() { return destroyed; },
    emitError(): void { errorListener?.(new Error("transport failed")); },
  };
}

function upstashClient(overrides: Partial<UpstashRedisCompatibleClient> = {}): UpstashRedisCompatibleClient {
  return {
    async get<T = string>(): Promise<T | null> { return null; },
    async set<T = string>(): Promise<"OK" | T | null> { return "OK"; },
    async del(): Promise<number> { return 0; },
    async incr(): Promise<number> { return 0; },
    async expire(): Promise<number> { return 0; },
    async pttl(): Promise<number> { return -2; },
    async zadd<T = string>(): Promise<number> { return 0; },
    async zrange<T extends unknown[] = string[]>(): Promise<T> { return [] as unknown as T; },
    async zcard(): Promise<number> { return 0; },
    async zrem<T = string>(): Promise<number> { return 0; },
    async zremrangebyscore(): Promise<number> { return 0; },
    async eval<T = unknown>(): Promise<T> { return 0 as T; },
    ...overrides,
  };
}

describe("Redis production connection", () => {
  it("normalizes sorted-set cardinality from node-redis", async () => {
    const client = hangingClient();
    let observedKey: string | undefined;
    client.zCard = async (key: string) => {
      observedKey = key;
      return 7;
    };

    expect(await createNodeRedisAdapter(client).zCard("air:inbox:v1:pending")).toBe(7);
    expect(observedKey).toBe("air:inbox:v1:pending");
  });

  it("bounds cold connection setup and destroys a hanging client", async () => {
    const client = hangingClient();
    const startedAt = Date.now();
    await expect(connectRedisFromUrl("rediss://example.test:6379", {
      connectTimeoutMs: 5,
      clientFactory: (options) => {
        expect(options.socket).toEqual({ connectTimeout: 5, reconnectStrategy: false });
        return client;
      },
    })).rejects.toThrow("Redis connection failed");
    expect(Date.now() - startedAt).toBeLessThan(250);
    expect(client.destroyed).toBe(true);
  });

  it("contains node-redis error events and notifies a connected runtime", async () => {
    const client = {
      ...hangingClient(),
      get isOpen() { return true; },
      connect: async () => undefined,
    };
    const connected = await connectRedisFromUrl("redis://example.test:6379", {
      clientFactory: () => client,
    });
    let failures = 0;
    const unsubscribe = connected.onOperationalError(() => { failures += 1; });
    client.emitError();
    expect(failures).toBe(1);
    unsubscribe();
    client.emitError();
    expect(failures).toBe(1);
  });

  it("preserves opaque values and maps Upstash sorted-set bounds", async () => {
    let observedRange: readonly unknown[] | undefined;
    const client = upstashClient();
    client.get = async <T = string>(): Promise<T | null> =>
      '{"opaque":"queue-value"}' as unknown as T;
    client.zrange = async <T extends unknown[] = string[]>(
      key: string,
      min: number | "-inf" | "+inf",
      max: number | "-inf" | "+inf",
      options: { byScore: true; offset?: number; count?: number },
    ): Promise<T> => {
      observedRange = [key, min, max, options];
      return ["air:inbox:v1:item"] as unknown as T;
    };
    const redis = createUpstashRedisAdapter(client);

    expect(await redis.get("air:inbox:v1:item")).toBe('{"opaque":"queue-value"}');
    expect(await redis.zRangeByScore(
      "air:inbox:v1:pending",
      Number.NEGATIVE_INFINITY,
      Number.POSITIVE_INFINITY,
      { offset: 2, limit: 3 },
    )).toEqual(["air:inbox:v1:item"]);
    expect(observedRange).toEqual([
      "air:inbox:v1:pending",
      "-inf",
      "+inf",
      { byScore: true, offset: 2, count: 3 },
    ]);
  });

  it("uses one bounded HTTPS attempt when REST credentials are configured", async () => {
    let received: UpstashRedisRestOptions | undefined;
    const client = upstashClient();
    client.get = async <T = string>(): Promise<T | null> => "ready" as unknown as T;
    const connected = await connectRedisFromUrl("rediss://unused.example.test:6379", {
      upstashRestUrl: "https://example.upstash.io",
      upstashRestToken: "unit-test-rest-token",
      upstashClientFactory(options) {
        received = options;
        return client;
      },
    });

    expect(received).toMatchObject({
      url: "https://example.upstash.io",
      token: "unit-test-rest-token",
      retry: { retries: 0 },
      automaticDeserialization: false,
    });
    expect(typeof received?.signal).toBe("function");
    expect(received?.signal()).toBeInstanceOf(AbortSignal);
    expect(await connected.redis.get("air:ready")).toBe("ready");
  });

  it("rejects incomplete Upstash REST configuration before attempting TCP", async () => {
    await expect(connectRedisFromUrl("rediss://example.test:6379", {
      upstashRestUrl: "https://example.upstash.io",
    })).rejects.toThrow("Upstash Redis REST configuration is incomplete");
  });
});
