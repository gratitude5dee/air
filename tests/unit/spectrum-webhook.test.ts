import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  decodeWebhookJson,
  isEligibleInboundMessage,
  MAX_WEBHOOK_BODY_BYTES,
  normalizeSpectrumWebhook,
  readRawBodyLimited,
  verifySpectrumSignature,
  WebhookInputError,
} from "../../lib/spectrum-webhook.js";

const secret = "test-webhook-secret";

function signed(raw: string, timestamp = 1_700_000_000): string {
  return `v0=${createHmac("sha256", secret)
    .update(`v0:${timestamp}:${raw}`)
    .digest("hex")}`;
}

function payload(): Record<string, unknown> {
  const space = {
    id: "iMessage;-;+15555550123",
    phone: "+15555550124",
    type: "dm",
  };
  return {
    event: "messages",
    space,
    message: {
      id: "message-001",
      direction: "inbound",
      sender: { id: "+15555550123" },
      space,
      content: { type: "text", text: "How do labels develop artists?" },
    },
  };
}

describe("Spectrum webhook verifier", () => {
  it("uses untouched bytes with the official v0 signing shape", () => {
    const raw = JSON.stringify(payload());
    expect(
      verifySpectrumSignature({
        rawBody: Buffer.from(raw),
        secret,
        signature: signed(raw),
        timestamp: "1700000000",
        now: 1_700_000_050,
      }),
    ).toEqual({ ok: true });

    // Same parsed JSON, different bytes: must not validate.
    expect(
      verifySpectrumSignature({
        rawBody: Buffer.from(`${raw}\n`),
        secret,
        signature: signed(raw),
        timestamp: "1700000000",
        now: 1_700_000_050,
      }),
    ).toMatchObject({ ok: false, status: 401 });
  });

  it("rejects missing, stale, and malformed signature inputs", () => {
    const raw = Buffer.from("{}", "utf8");
    expect(
      verifySpectrumSignature({ rawBody: raw, secret, signature: null, timestamp: null }),
    ).toMatchObject({ ok: false, status: 400 });
    expect(
      verifySpectrumSignature({
        rawBody: raw,
        secret,
        signature: signed("{}", 1),
        timestamp: "1",
        now: 1_700_000_000,
      }),
    ).toMatchObject({ ok: false, status: 400 });
  });

  it("bounds body reads before JSON parsing", async () => {
    const tooLarge = "x".repeat(MAX_WEBHOOK_BODY_BYTES + 1);
    try {
      await readRawBodyLimited(
        new Request("https://air.example/webhooks/imessage", {
          method: "POST",
          body: tooLarge,
        }),
      );
      throw new Error("expected a body limit failure");
    } catch (error) {
      expect(error).toBeInstanceOf(WebhookInputError);
      expect((error as WebhookInputError).status).toBe(413);
    }
  });

  it("normalizes only the encrypted work envelope fields", async () => {
    const raw = JSON.stringify(payload());
    const bytes = await readRawBodyLimited(
      new Request("https://air.example/webhooks/imessage", { method: "POST", body: raw }),
    );
    const envelope = normalizeSpectrumWebhook({
      payload: decodeWebhookJson(bytes),
      projectId: "a26b448c-9bd6-4469-847a-84b78c1f7778",
      webhookId: "delivery-001",
      receivedAt: 1_700_000_000_000,
    });
    expect(envelope).toMatchObject({
      messageId: "message-001",
      chatGuid: "iMessage;-;+15555550123",
      spaceType: "dm",
      contentType: "text",
    });
    expect(envelope && isEligibleInboundMessage(envelope)).toBe(true);
  });
});
