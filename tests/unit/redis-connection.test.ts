import { describe, expect, it } from "vitest";
import { connectRedisFromUrl, createNodeRedisAdapter } from "../../lib/redis.js";

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
});
