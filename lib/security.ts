import { createHmac, timingSafeEqual } from "node:crypto";

export type SenderHash = string & { readonly __brand: "AirSenderHash" };
export type ThreadHash = string & { readonly __brand: "AirThreadHash" };
export type CorrelationId = string & { readonly __brand: "AirCorrelationId" };

export interface SenderIdentity {
  readonly senderHash: SenderHash;
  /** Informational only; never contains the original address. */
  readonly kind: "phone" | "email" | "opaque";
}

const HEX_HASH = /^[a-f0-9]{64}$/;
const EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PHONEISH = /^[+0-9().\s-]+$/;

/**
 * Canonicalizes an iMessage sender before hashing. Phone numbers must already
 * carry a country prefix; guessing a default country would create collisions.
 */
export function normalizeSenderIdentifier(value: string): {
  value: string;
  kind: SenderIdentity["kind"];
} {
  const normalized = value.trim().normalize("NFKC");
  if (!normalized || normalized.length > 320) {
    throw new Error("sender identifier is invalid");
  }
  if (EMAIL.test(normalized)) {
    return { value: normalized.toLowerCase(), kind: "email" };
  }
  if (PHONEISH.test(normalized)) {
    return { value: normalizeE164(normalized), kind: "phone" };
  }
  // Some provider events use a stable opaque handle rather than a phone/email.
  // Canonicalize it without attempting to interpret it as a phone number.
  return { value: normalized.toLowerCase(), kind: "opaque" };
}

export function normalizeE164(value: string): string {
  const trimmed = value.trim();
  const digits = trimmed.replace(/\D/g, "");
  const canonical = trimmed.startsWith("+")
    ? `+${digits}`
    : trimmed.startsWith("00")
      ? `+${digits.slice(2)}`
      : "";
  if (!/^\+[1-9]\d{6,14}$/.test(canonical)) {
    throw new Error("phone sender must be canonical E.164");
  }
  return canonical;
}

export function hashSenderIdentifier(secret: string, identifier: string): SenderHash {
  const normalized = normalizeSenderIdentifier(identifier);
  return hmacHex(secret, `sender:v1:${normalized.value}`) as SenderHash;
}

export function hashThreadIdentifier(secret: string, threadIdentifier: string): ThreadHash {
  const normalized = threadIdentifier.trim().normalize("NFKC");
  if (!normalized || normalized.length > 1_024) {
    throw new Error("thread identifier is invalid");
  }
  return hmacHex(secret, `thread:v1:${normalized}`) as ThreadHash;
}

export function createSenderIdentity(secret: string, identifier: string): SenderIdentity {
  const normalized = normalizeSenderIdentifier(identifier);
  return Object.freeze({
    senderHash: hmacHex(secret, `sender:v1:${normalized.value}`) as SenderHash,
    kind: normalized.kind,
  });
}

/** Stable, pseudonymous IDs for records, delivery metadata, and logs. */
export function deriveCorrelationId(
  secret: string,
  projectId: string,
  messageId: string,
): CorrelationId {
  return hmacHex(secret, `correlation:v1:${projectId}\u0000${messageId}`) as CorrelationId;
}

export function deriveOpaqueId(secret: string, namespace: string, value: string): string {
  if (!namespace || namespace.includes("\u0000")) {
    throw new Error("invalid identifier namespace");
  }
  return hmacHex(secret, `${namespace}:v1:${value}`);
}

/**
 * Constant-time allowlist comparison. An empty allowlist intentionally denies
 * every sender so production fails closed until an operator adds a hash.
 */
export function isSenderAllowed(
  senderHash: string,
  allowedSenderHashes: Iterable<string>,
): boolean {
  if (!HEX_HASH.test(senderHash)) return false;
  const candidate = Buffer.from(senderHash, "hex");
  let allowed = false;
  let sawValidHash = false;
  for (const knownHash of allowedSenderHashes) {
    if (!HEX_HASH.test(knownHash)) continue;
    sawValidHash = true;
    // Do not short-circuit: comparisons take the same form for every entry.
    allowed = timingSafeEqual(candidate, Buffer.from(knownHash, "hex")) || allowed;
  }
  return sawValidHash && allowed;
}

export function parseAllowedSenderHashes(value: string | undefined): readonly SenderHash[] {
  if (!value?.trim()) return Object.freeze([]);
  const hashes = value
    .split(",")
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
  if (hashes.some((entry) => !HEX_HASH.test(entry))) {
    throw new Error("AIR_ALLOWED_SENDER_HASHES contains an invalid hash");
  }
  return Object.freeze([...new Set(hashes)] as SenderHash[]);
}

function hmacHex(secret: string, value: string): string {
  if (!secret || secret.trim().length < 1) {
    throw new Error("identity hash secret is required");
  }
  return createHmac("sha256", secret).update(value, "utf8").digest("hex");
}
