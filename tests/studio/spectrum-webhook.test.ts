import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";

import {
  decodeWebhookJson,
  MAX_WEBHOOK_BODY_BYTES,
  normalizeSpectrumWebhook,
  readRawBodyLimited,
  verifySpectrumSignature,
  WebhookInputError,
} from "../../lib/spectrum-webhook.js";

const secret = "spectrum-core-test-secret";

function payload(): Record<string, unknown> {
  const space = { id: "iMessage;-;+15555550123", phone: "+15555550124", type: "dm" };
  return {
    event: "messages",
    space,
    message: {
      id: "message-001",
      direction: "inbound",
      sender: { id: "+15555550123" },
      space,
      content: { type: "text", text: "Make an ink wash animation." },
    },
  };
}

function signature(raw: string, timestamp: number): string {
  return `v0=${createHmac("sha256", secret).update(`v0:${timestamp}:${raw}`).digest("hex")}`;
}

describe("Spectrum webhook core guards", () => {
  it("verifies untouched raw bytes before JSON parsing", () => {
    const timestamp = 1_700_000_000;
    const raw = JSON.stringify(payload());
    expect(verifySpectrumSignature({
      rawBody: Buffer.from(raw), secret, signature: signature(raw, timestamp), timestamp: String(timestamp), now: timestamp,
    })).toEqual({ ok: true });
    expect(verifySpectrumSignature({
      rawBody: Buffer.from(`${raw}\n`), secret, signature: signature(raw, timestamp), timestamp: String(timestamp), now: timestamp,
    })).toMatchObject({ ok: false, status: 401 });
  });

  it("bounds body reads before parsing", async () => {
    await expect(readRawBodyLimited(new Request("https://air.example.test/webhooks/imessage", {
      method: "POST",
      body: "x".repeat(MAX_WEBHOOK_BODY_BYTES + 1),
    }))).rejects.toMatchObject({ name: WebhookInputError.name, status: 413 });
  });

  it("retains only bounded attachment references from nested content", () => {
    const grouped = payload();
    (grouped.message as { content: unknown }).content = {
      type: "group",
      items: [
        { content: { type: "text", text: "Use this as a reference." } },
        { content: { type: "attachment", guid: "attachment-guid", name: "ignored.png", mimeType: "image/png", size: 456 } },
      ],
    };
    const normalized = normalizeSpectrumWebhook({ payload: grouped, projectId: "project", webhookId: "delivery" });
    expect(normalized).toMatchObject({
      text: "Use this as a reference.",
      attachments: [{ id: "attachment-guid", mimeType: "image/png", size: 456 }],
    });
    expect(JSON.stringify(normalized)).not.toContain("sourceUrl");
  });

  it("unwraps a reply authored in a direct message and rejects contradictory chat metadata", () => {
    const replied = payload();
    (replied.message as { content: unknown }).content = {
      type: "reply",
      target: { id: "historical-message" },
      content: { type: "attachment", id: "reply-attachment", mimeType: "image/jpeg", size: 321 },
    };
    expect(normalizeSpectrumWebhook({ payload: replied, projectId: "project", webhookId: "delivery" }))
      .toMatchObject({ contentType: "reply", attachments: [{ id: "reply-attachment" }] });

    const contradictory = payload();
    (contradictory.space as { type: string }).type = "group";
    expect(() => normalizeSpectrumWebhook({ payload: contradictory, projectId: "project", webhookId: "delivery" }))
      .toThrow(WebhookInputError);
  });

  it("decodes only valid JSON after raw verification", () => {
    expect(() => decodeWebhookJson(Buffer.from("{invalid", "utf8"))).toThrow(WebhookInputError);
  });
});
