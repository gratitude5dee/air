import { timingSafeEqual } from "node:crypto";
import { createSenderIdentity, hashThreadIdentifier, isSenderAllowed } from "../security.js";
import {
  decodeWebhookJson,
  hasActionableInboundContent,
  normalizeSpectrumWebhook,
  readRawBodyLimited,
  SPECTRUM_SIGNATURE_HEADER,
  SPECTRUM_TIMESTAMP_HEADER,
  SPECTRUM_WEBHOOK_ID_HEADER,
  verifySpectrumSignature,
} from "../spectrum-webhook.js";
import { loadStudioEnvironment } from "./config.js";
import type { StudioRuntime } from "./runtime.js";
import type { StudioInboundEnvelope } from "./store.js";

export interface WebhookHandlerOptions {
  readonly getRuntime?: (() => Promise<StudioRuntime>) | undefined;
}

/**
 * The only public Photon ingress. It never parses unverified JSON and only
 * returns 200 after a qualifying inbound receipt has been encrypted and made
 * durable. Non-actionable signed events are intentionally acknowledged with no
 * detail, preventing both retries and policy leakage.
 */
export async function handleStudioIMessageWebhook(
  request: Request,
  options: WebhookHandlerOptions = {},
): Promise<Response> {
  if (request.method !== "POST") return new Response(null, { status: 405 });
  let rawBody: Uint8Array;
  try {
    rawBody = await readRawBodyLimited(request);
  } catch (error) {
    const status = error instanceof Error && (error.message === "body_too_large") ? 413 : 400;
    return new Response(null, { status });
  }

  let environment;
  try {
    environment = loadStudioEnvironment();
  } catch {
    // Without a webhook secret we cannot validate a delivery. Do not parse or
    // acknowledge it, so the source can retry after configuration is repaired.
    return new Response(null, { status: 503 });
  }
  if (!environment.imessageWebhookSecret) return new Response(null, { status: 503 });
  const verification = verifySpectrumSignature({
    rawBody,
    secret: environment.imessageWebhookSecret,
    signature: request.headers.get(SPECTRUM_SIGNATURE_HEADER),
    timestamp: request.headers.get(SPECTRUM_TIMESTAMP_HEADER),
  });
  if (!verification.ok) return new Response(null, { status: verification.status });
  if (
    !environment.imessageProjectId
    || !environment.idHashSecret
    || !environment.queueEncryptionKey
    || environment.storageTransport === "unconfigured"
    || environment.allowedSenderHashes.length === 0
  ) {
    return new Response(null, { status: 503 });
  }

  let payload: unknown;
  try {
    payload = decodeWebhookJson(rawBody);
  } catch {
    return new Response(null, { status: 400 });
  }
  let normalized;
  try {
    normalized = normalizeSpectrumWebhook({
      payload,
      projectId: environment.imessageProjectId ?? "",
      webhookId: request.headers.get(SPECTRUM_WEBHOOK_ID_HEADER),
    });
  } catch {
    return new Response(null, { status: 400 });
  }
  if (!normalized) return new Response(null, { status: 200 });

  // Air is DM-only. Reactions, typing, membership changes, groups, echoes,
  // missing sender/line values, and malformed inputs never enter the provider
  // queue or trigger onboarding.
  if (
    normalized.direction !== "inbound"
    || normalized.spaceType !== "dm"
    || !normalized.senderId
    || !normalized.phone
    || !hasActionableInboundContent(normalized)
  ) {
    return new Response(null, { status: 200 });
  }
  let senderHash;
  let threadHash;
  try {
    senderHash = createSenderIdentity(environment.idHashSecret, normalized.senderId).senderHash;
    // Namespace the conversation scope by the configured Photon project as
    // well as chat and sending line. Shared Redis must never let identical
    // chat identifiers from another project join this pending-assembly fence.
    threadHash = hashThreadIdentifier(
      environment.idHashSecret,
      `${normalized.projectId}\u0000${normalized.chatGuid}\u0000${normalized.phone}`,
    );
  } catch {
    // The signed payload passed the provider schema, but its identifiers
    // cannot safely enter Air's bounded pseudonymous key space.
    return new Response(null, { status: 400 });
  }
  if (!isSenderAllowed(senderHash, environment.allowedSenderHashes)) {
    return new Response(null, { status: 200 });
  }

  let runtime: StudioRuntime;
  try {
    runtime = await resolveStudioRuntime(options);
  } catch {
    return new Response(null, { status: 503 });
  }
  const envelope: StudioInboundEnvelope = Object.freeze({
    chatGuid: normalized.chatGuid,
    sendingLine: normalized.phone,
    // The signed inbound message is the only reply target we can safely
    // resolve without a separate, spike-verified Photon history lookup.
    // Retaining it encrypted lets a future verified transport use native
    // replies while an unresolved target still degrades to a fresh send.
    replyTargetId: normalized.messageId,
    text: normalized.text,
    attachments: normalized.attachments.map((attachment) => Object.freeze({
      id: attachment.id,
      mimeType: attachment.mimeType,
      sizeBytes: attachment.size,
    })),
  });
  try {
    await runtime.store.persistInbox({
      receiptId: runtime.store.receiptId(normalized.projectId, normalized.messageId),
      senderHash,
      threadHash,
      envelope,
    });
  } catch {
    return new Response(null, { status: 503 });
  }
  return new Response(null, { status: 200 });
}

/**
 * Keep the Redis/runtime graph out of the public webhook's cold path. Invalid
 * signatures, non-actionable events, and non-allowlisted senders need only
 * the lightweight raw-HMAC boundary; they must not wait on serverless module
 * initialization intended for durable receipt persistence.
 */
async function resolveStudioRuntime(options: WebhookHandlerOptions): Promise<StudioRuntime> {
  if (options.getRuntime !== undefined) return await options.getRuntime();
  const { getStudioRuntime } = await import("./runtime.js");
  return await getStudioRuntime();
}

export function authorizeStudioDrain(request: Request, expectedSecret: string | undefined): boolean {
  if (!expectedSecret || request.method !== "GET" && request.method !== "POST") return false;
  const header = request.headers.get("authorization");
  const expected = `Bearer ${expectedSecret}`;
  if (!header || header.length !== expected.length) return false;
  return timingSafeEqual(Buffer.from(header), Buffer.from(expected));
}
