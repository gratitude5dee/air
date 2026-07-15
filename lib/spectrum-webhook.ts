import { createHmac, timingSafeEqual } from "node:crypto";
import { z } from "zod";

/** Spectrum Cloud's maximum accepted delivery body for Air V1. */
export const MAX_WEBHOOK_BODY_BYTES = 64 * 1024;
export const SPECTRUM_SIGNATURE_HEADER = "x-spectrum-signature";
export const SPECTRUM_TIMESTAMP_HEADER = "x-spectrum-timestamp";
export const SPECTRUM_WEBHOOK_ID_HEADER = "x-spectrum-webhook-id";
export const SPECTRUM_EVENT_HEADER = "x-spectrum-event";
export const SPECTRUM_MESSAGES_EVENT = "messages";
export const SPECTRUM_TIMESTAMP_TOLERANCE_SECONDS = 5 * 60;

export type WebhookVerification =
  | { ok: true }
  | { ok: false; status: 400 | 401; reason: "missing_headers" | "stale_timestamp" | "invalid_signature" };

export type NormalizedIMessageEnvelope = {
  readonly version: 1;
  readonly webhookId: string;
  readonly projectId: string;
  readonly messageId: string;
  readonly chatGuid: string;
  readonly phone: string | null;
  readonly senderId: string;
  readonly direction: "inbound" | "outbound";
  readonly spaceType: "dm" | "group" | "unknown";
  /** Only textual content is copied into the encrypted envelope. */
  readonly text: string | null;
  readonly contentType: string | null;
  readonly receivedAt: number;
};

const nonEmptyString = z.string().trim().min(1).max(4_096);

const webhookPayloadSchema = z.object({
  event: z.string(),
  space: z.object({
    id: nonEmptyString,
    phone: z.string().trim().min(1).max(320).optional(),
    type: z.enum(["dm", "group"]).optional(),
  }),
  message: z.object({
    id: nonEmptyString,
    direction: z.enum(["inbound", "outbound"]).optional(),
    sender: z.object({ id: nonEmptyString }).optional(),
    space: z
      .object({
        phone: z.string().trim().min(1).max(320).optional(),
      })
      .optional(),
    content: z.unknown(),
  }),
});

/** A request parser error that is safe to map onto an HTTP status. */
export class WebhookInputError extends Error {
  public constructor(
    readonly status: 400 | 413,
    message: "invalid_body" | "body_too_large" | "invalid_json" | "invalid_payload",
  ) {
    super(message);
    this.name = "WebhookInputError";
  }
}

/**
 * Read a Fetch Request exactly once, retaining only raw bytes.  It guards the
 * route before JSON parsing so a claimed Content-Length cannot bypass the
 * actual 64 KiB body limit.
 */
export async function readRawBodyLimited(
  request: Request,
  maxBytes = MAX_WEBHOOK_BODY_BYTES,
): Promise<Uint8Array> {
  const declaredLength = request.headers.get("content-length");
  if (declaredLength !== null) {
    if (!/^\d+$/.test(declaredLength)) {
      throw new WebhookInputError(400, "invalid_body");
    }
    if (Number(declaredLength) > maxBytes) {
      throw new WebhookInputError(413, "body_too_large");
    }
  }

  if (!request.body) return new Uint8Array();
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > maxBytes) {
        await reader.cancel().catch(() => undefined);
        throw new WebhookInputError(413, "body_too_large");
      }
      chunks.push(next.value);
    }
  } catch (error) {
    if (error instanceof WebhookInputError) throw error;
    throw new WebhookInputError(400, "invalid_body");
  } finally {
    reader.releaseLock();
  }

  const raw = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    raw.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return raw;
}

/**
 * Byte-for-byte compatible with Photon’s current Chat SDK adapter.  The
 * header is `v0=<hex>`, while the HMAC signing base is
 * `v0:{timestamp}:{raw UTF-8 bytes}`.
 */
export function verifySpectrumSignature(input: {
  readonly rawBody: Uint8Array;
  readonly secret: string | undefined;
  readonly signature: string | null;
  readonly timestamp: string | null;
  readonly now?: number;
}): WebhookVerification {
  if (!input.secret?.trim() || !input.signature || !input.timestamp) {
    return { ok: false, status: 400, reason: "missing_headers" };
  }
  if (!/^\d+(?:\.\d+)?$/.test(input.timestamp)) {
    return { ok: false, status: 400, reason: "stale_timestamp" };
  }

  const nowSeconds = input.now ?? Math.floor(Date.now() / 1_000);
  const age = Math.abs(nowSeconds - Number(input.timestamp));
  if (!Number.isFinite(age) || age > SPECTRUM_TIMESTAMP_TOLERANCE_SECONDS) {
    return { ok: false, status: 400, reason: "stale_timestamp" };
  }

  const prefix = Buffer.from(`v0:${input.timestamp}:`, "utf8");
  const signingBase = Buffer.concat([prefix, Buffer.from(input.rawBody)]);
  const expected = `v0=${createHmac("sha256", input.secret)
    .update(signingBase)
    .digest("hex")}`;
  const expectedBytes = Buffer.from(expected, "utf8");
  const suppliedBytes = Buffer.from(input.signature, "utf8");
  if (
    expectedBytes.byteLength !== suppliedBytes.byteLength ||
    !timingSafeEqual(expectedBytes, suppliedBytes)
  ) {
    return { ok: false, status: 401, reason: "invalid_signature" };
  }
  return { ok: true };
}

/** Decode strict UTF-8 only after a signature succeeds. */
export function decodeWebhookJson(rawBody: Uint8Array): unknown {
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(rawBody);
  } catch {
    throw new WebhookInputError(400, "invalid_json");
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new WebhookInputError(400, "invalid_json");
  }
}

/**
 * Drops unneeded provider fields before encryption. The caller supplies the
 * configured project ID because current Spectrum message payloads do not
 * include it reliably.
 */
export function normalizeSpectrumWebhook(input: {
  readonly payload: unknown;
  readonly projectId: string;
  readonly webhookId: string | null;
  readonly receivedAt?: number;
}): NormalizedIMessageEnvelope | null {
  const parsed = webhookPayloadSchema.safeParse(input.payload);
  if (!parsed.success) throw new WebhookInputError(400, "invalid_payload");
  const payload = parsed.data;
  if (payload.event !== SPECTRUM_MESSAGES_EVENT) return null;
  const webhookId = input.webhookId?.trim();
  if (!webhookId || webhookId.length > 512) {
    throw new WebhookInputError(400, "invalid_payload");
  }
  const projectId = input.projectId.trim();
  if (!projectId || projectId.length > 512) {
    // This is a deployment misconfiguration. Keep it generic for callers.
    throw new Error("IMESSAGE_PROJECT_ID is required");
  }

  const rawContent = payload.message.content;
  const content = rawContent && typeof rawContent === "object"
    ? (rawContent as Record<string, unknown>)
    : undefined;
  const contentType = typeof content?.type === "string" ? content.type.slice(0, 80) : null;
  const text = typeof content?.text === "string" ? content.text : null;
  const spaceType = resolveSpectrumSpaceType(payload.space.id, payload.space.type);

  return Object.freeze({
    version: 1,
    webhookId,
    projectId,
    messageId: payload.message.id,
    chatGuid: payload.space.id,
    phone: payload.space.phone ?? payload.message.space?.phone ?? null,
    senderId: payload.message.sender?.id ?? "",
    direction: payload.message.direction ?? "inbound",
    spaceType,
    text,
    contentType,
    receivedAt: input.receivedAt ?? Date.now(),
  });
}

/**
 * Spectrum documents `space.type` as optional. The official iMessage adapter
 * encodes direct-message and group chat GUIDs with distinct delimiters, so
 * recover that information only when the provider omitted the optional field.
 * A contradictory declared type is rejected rather than letting an attacker
 * steer an otherwise unambiguous direct/group policy decision.
 */
function resolveSpectrumSpaceType(
  chatGuid: string,
  declared: "dm" | "group" | undefined,
): "dm" | "group" | "unknown" {
  const inferred = chatGuid.includes(";-;")
    ? "dm"
    : chatGuid.includes(";+;")
      ? "group"
      : "unknown";
  if (declared && inferred !== "unknown" && declared !== inferred) {
    throw new WebhookInputError(400, "invalid_payload");
  }
  return declared ?? inferred;
}

/** Only inbound plain-text DMs may enter the Eve/RAG work path. */
export function isEligibleInboundMessage(envelope: NormalizedIMessageEnvelope): boolean {
  return (
    envelope.direction === "inbound" &&
    envelope.spaceType === "dm" &&
    envelope.contentType === "text" &&
    typeof envelope.text === "string" &&
    envelope.senderId.length > 0
  );
}
