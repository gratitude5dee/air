import { afterEach, describe, expect, it, vi } from "vitest";
import { assertIMessageRuntimeConfiguration } from "../../lib/imessage-adapter.js";

const required = {
  IMESSAGE_PROJECT_ID: "project",
  IMESSAGE_PROJECT_SECRET: "secret",
  IMESSAGE_WEBHOOK_SECRET: "webhook-secret",
  REDIS_URL: "redis://fixture.invalid:6379",
  ZAP_AIR_SERVICE_URL: "https://zap.example.test",
  ZAP_AIR_SERVICE_TOKEN: "service-token",
  AIR_ID_HASH_SECRET: "identity-secret",
  AIR_QUEUE_ENCRYPTION_KEY: Buffer.alloc(32, 7).toString("base64"),
  AIR_ALLOWED_SENDER_HASHES: "a".repeat(64),
  AIR_PUBLIC_BASE_URL: "https://air.example.test",
  CRON_SECRET: "cron-secret",
};

afterEach(() => vi.unstubAllEnvs());

describe("iMessage runtime configuration", () => {
  it("fails closed when an unredacted OTLP exporter is configured", () => {
    for (const [name, value] of Object.entries(required)) vi.stubEnv(name, value);
    expect(assertIMessageRuntimeConfiguration).not.toThrow();

    vi.stubEnv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://collector.example.test/v1/traces");
    expect(assertIMessageRuntimeConfiguration).toThrow(/forbids OTLP exporter/i);
  });
});
