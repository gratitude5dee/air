import { createHmac } from "node:crypto";
import { createiMessageAdapter } from "@photon-ai/chat-adapter-imessage";
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
  it("shares the official adapter's raw HMAC, thread-id, and text fixture", async () => {
    const timestamp = Math.floor(Date.now() / 1_000);
    const raw = JSON.stringify(payload(), null, 2);
    const signature = signed(raw, timestamp);
    const adapter = createiMessageAdapter({
      projectId: "contract-project",
      projectSecret: "contract-project-secret",
      webhookSecret: secret,
    });
    const delivered: unknown[][] = [];
    (
      adapter as unknown as {
        chat: { processMessage(...args: unknown[]): void };
      }
    ).chat = {
      processMessage: (...args) => delivered.push(args),
    };

    expect(
      verifySpectrumSignature({
        rawBody: Buffer.from(raw, "utf8"),
        secret,
        signature,
        timestamp: String(timestamp),
        now: timestamp,
      }),
    ).toEqual({ ok: true });

    const normalized = normalizeSpectrumWebhook({
      payload: decodeWebhookJson(Buffer.from(raw, "utf8")),
      projectId: "contract-project",
      webhookId: "contract-delivery-001",
      receivedAt: timestamp * 1_000,
    });
    if (!normalized) throw new Error("expected a normalized messages event");

    const response = await adapter.handleWebhook(
      new Request("https://air.example/webhooks/imessage", {
        method: "POST",
        headers: {
          "x-spectrum-signature": signature,
          "x-spectrum-timestamp": String(timestamp),
        },
        body: raw,
      }),
    );

    expect(response.status).toBe(200);
    expect(delivered).toHaveLength(1);
    const [, threadId, message] = delivered[0] ?? [];
    expect(threadId).toBe("imessage:iMessage;-;+15555550123~+15555550124");
    expect(adapter.encodeThreadId({
      chatGuid: normalized.chatGuid,
      ...(normalized.phone ? { phone: normalized.phone } : {}),
    })).toBe(threadId);
    expect((message as { text?: unknown }).text).toBe(normalized.text);
  });

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

  it("retains a direct attachment's Photon GUID and bounded metadata, never bytes", () => {
    const image = payload();
    (image.message as { content: unknown }).content = {
      type: "attachment",
      id: "photon-attachment-guid-001",
      name: "first-frame.heic",
      mimeType: "image/heic",
      size: 1_234_567,
      // Cloud webhooks must never deliver these functions; this fixture also
      // verifies the normalizer only retains the safe reference fields.
      read: "not-a-function",
      stream: "not-a-function",
      sourceUrl: "https://provider.example/private-image",
    };

    const envelope = normalizeSpectrumWebhook({
      payload: image,
      projectId: "contract-project",
      webhookId: "attachment-delivery-001",
    });

    expect(envelope).toMatchObject({
      contentType: "attachment",
      text: null,
      attachments: [
        {
          id: "photon-attachment-guid-001",
          name: "first-frame.heic",
          mimeType: "image/heic",
          size: 1_234_567,
        },
      ],
    });
    expect(envelope?.attachments[0]).not.toHaveProperty("read");
    expect(envelope?.attachments[0]).not.toHaveProperty("stream");
    expect(envelope?.attachments[0]).not.toHaveProperty("sourceUrl");
  });

  it("extracts prompt text and every attachment from compact and nested group payloads", () => {
    const grouped = payload();
    (grouped.message as { content: unknown }).content = {
      type: "group",
      items: [
        {
          id: "nested-text-message",
          content: { type: "text", text: "Make this feel like a 1970s film." },
        },
        {
          id: "nested-image-message",
          content: {
            type: "attachment",
            // A few Photon payloads label the same attachment GUID `guid`.
            guid: "photon-attachment-guid-002",
            name: "reference.png",
            mimeType: "image/png",
            size: 456,
          },
        },
        {
          // Compact content-only group item, nested one level further.
          type: "group",
          items: [
            {
              type: "attachment",
              id: "photon-attachment-guid-003",
              name: "second-reference.jpg",
              mimeType: "image/jpeg",
              size: 789,
            },
          ],
        },
      ],
    };

    const envelope = normalizeSpectrumWebhook({
      payload: grouped,
      projectId: "contract-project",
      webhookId: "attachment-group-delivery-001",
    });

    expect(envelope).toMatchObject({
      contentType: "group",
      text: "Make this feel like a 1970s film.",
      attachments: [
        {
          id: "photon-attachment-guid-002",
          name: "reference.png",
          mimeType: "image/png",
          size: 456,
        },
        {
          id: "photon-attachment-guid-003",
          name: "second-reference.jpg",
          mimeType: "image/jpeg",
          size: 789,
        },
      ],
    });
    // The workflow can now reject this as a multi-attachment request instead
    // of silently losing the second attachment during normalization.
    expect(envelope?.attachments).toHaveLength(2);
  });

  it("recursively unwraps Spectrum reply content without retaining the reply target", () => {
    const replied = payload();
    (replied.message as { content: unknown }).content = {
      type: "reply",
      target: {
        id: "historical-message-id",
        content: { type: "text", text: "This must not become the prompt." },
      },
      content: {
        type: "group",
        items: [
          { content: { type: "text", text: "Animate this as an ink wash." } },
          {
            content: {
              type: "reply",
              target: { id: "another-historical-message" },
              content: {
                type: "attachment",
                id: "photon-reply-attachment-guid",
                name: "reference.jpg",
                mimeType: "image/jpeg",
                size: 321,
              },
            },
          },
        ],
      },
    };

    const envelope = normalizeSpectrumWebhook({
      payload: replied,
      projectId: "contract-project",
      webhookId: "reply-delivery-001",
    });

    expect(envelope).toMatchObject({
      contentType: "reply",
      text: "Animate this as an ink wash.",
      attachments: [
        {
          id: "photon-reply-attachment-guid",
          name: "reference.jpg",
          mimeType: "image/jpeg",
          size: 321,
        },
      ],
    });
  });

  it("rejects attachment content with no usable Photon GUID", () => {
    const malformed = payload();
    (malformed.message as { content: unknown }).content = {
      type: "attachment",
      name: "missing-guid.png",
      mimeType: "image/png",
      size: 42,
    };

    expect(() => normalizeSpectrumWebhook({
      payload: malformed,
      projectId: "contract-project",
      webhookId: "bad-attachment-delivery-001",
    })).toThrow(WebhookInputError);
  });

  it("infers an omitted Spectrum space.type from the official iMessage chat GUID", () => {
    const missingType = payload();
    delete (missingType.space as { type?: string }).type;
    const envelope = normalizeSpectrumWebhook({
      payload: missingType,
      projectId: "contract-project",
      webhookId: "delivery-without-space-type",
    });
    expect(envelope?.spaceType).toBe("dm");
    expect(envelope && isEligibleInboundMessage(envelope)).toBe(true);

    const groupWithoutType = payload();
    const groupSpace = groupWithoutType.space as { id: string; type?: string };
    groupSpace.id = "iMessage;+;group-chat-guid";
    delete groupSpace.type;
    expect(normalizeSpectrumWebhook({
      payload: groupWithoutType,
      projectId: "contract-project",
      webhookId: "group-delivery-without-space-type",
    })?.spaceType).toBe("group");

    const contradictoryType = payload();
    (contradictoryType.space as { type?: string }).type = "group";
    expect(() => normalizeSpectrumWebhook({
      payload: contradictoryType,
      projectId: "contract-project",
      webhookId: "contradictory-space-type",
    })).toThrow(WebhookInputError);
  });
});
