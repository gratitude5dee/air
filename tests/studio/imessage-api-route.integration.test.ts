import { createHmac } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";

import { createSenderIdentity } from "../../lib/security.js";
import handler, { config } from "../../api/webhooks/imessage.js";

const webhookSecret = "route-integration-webhook-secret";
const identitySecret = "route-integration-identity-secret";
const senderId = "+14155550123";
const queueKey = Buffer.alloc(32, 31).toString("base64");

afterEach(() => vi.unstubAllEnvs());

function configureWebhookEnvironment(): void {
  const values: Record<string, string | undefined> = {
    VERCEL_ENV: "preview",
    IMESSAGE_PROJECT_ID: "route-integration-project",
    IMESSAGE_PROJECT_SECRET: "route-integration-project-secret",
    IMESSAGE_WEBHOOK_SECRET: webhookSecret,
    REDIS_URL: "rediss://redis.example.test:6380",
    UPSTASH_REDIS_REST_URL: undefined,
    UPSTASH_REDIS_REST_TOKEN: undefined,
    AIR_ID_HASH_SECRET: identitySecret,
    AIR_QUEUE_ENCRYPTION_KEY: queueKey,
    AIR_ALLOWED_SENDER_HASHES: createSenderIdentity(identitySecret, senderId).senderHash,
    AIR_ACCEPT_NEW_JOBS: "false",
    AIR_ALLOW_PROVIDER_SUBMIT: "false",
    AIR_ALLOW_PROVIDER_RECOVERY: "false",
    AIR_DELIVERY_ENABLED: "false",
  };
  for (const [name, value] of Object.entries(values)) vi.stubEnv(name, value);
}

function signedNodeRequest(raw: string): IncomingMessage {
  const timestamp = String(Math.floor(Date.now() / 1_000));
  const signature = `v0=${createHmac("sha256", webhookSecret)
    .update(`v0:${timestamp}:${raw}`)
    .digest("hex")}`;
  const stream = Readable.from([Buffer.from(raw, "utf8")]);
  Object.assign(stream, {
    method: "POST",
    url: "/webhooks/imessage",
    headers: {
      host: "air.wzrd.tech",
      "content-length": String(Buffer.byteLength(raw, "utf8")),
      "content-type": "application/json",
      "x-spectrum-signature": signature,
      "x-spectrum-timestamp": timestamp,
      "x-spectrum-webhook-id": "route-exact-raw-001",
    },
  });
  return stream as unknown as IncomingMessage;
}

function nodeResponse(): ServerResponse {
  return {
    statusCode: 0,
    setHeader: vi.fn(),
    end: vi.fn(),
  } as unknown as ServerResponse;
}

describe("Vercel iMessage webhook route integration", () => {
  it("keeps the exact signed native bytes intact through the unmocked route and handler", async () => {
    configureWebhookEnvironment();
    // Intentional non-canonical whitespace and field order: parsing and
    // reserializing this body before HMAC verification would invalidate it.
    const raw = [
      "{",
      '  "message" : { "content" : { "text" : "line-less signed delivery", "type" : "text" }, "id" : "route-raw-001", "direction" : "inbound", "sender" : { "id" : "+14155550123" } },',
      '  "space" : { "type" : "dm", "id" : "any;-;+14155550123" },',
      '  "event" : "messages"',
      "}",
    ].join("\n");
    const response = nodeResponse();

    expect(config).toEqual({ api: { bodyParser: false } });
    await handler(signedNodeRequest(raw), response);

    // The message intentionally has no `space.phone`, so the real handler
    // recognizes it as a valid but unroutable signed delivery and returns an
    // acknowledgement without invoking the Redis runtime.
    expect(response.statusCode).toBe(200);
    expect((response.end as unknown as ReturnType<typeof vi.fn>)).toHaveBeenCalledTimes(1);
  });
});
