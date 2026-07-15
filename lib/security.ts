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
/** Keep an approved source link usable without letting corpus metadata bloat a DM. */
export const MAX_SOURCE_DEEP_LINK_CHARS = 320;
const MAX_SOURCE_DEEP_LINK_INPUT_CHARS = 4_096;
const YOUTUBE_VIDEO_ID_RE = /^[A-Za-z0-9_-]{3,64}$/;
const MAX_SOURCE_START_SECONDS = 7 * 24 * 60 * 60;
const YOUTUBE_WATCH_HOSTS = new Set(["youtube.com", "www.youtube.com"]);

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

/** Only the source links rendered by the app are allowed through this gate. */
export function validateAllowedSourceDeepLink(value: string | null | undefined): string | null {
  if (!value || value.length > MAX_SOURCE_DEEP_LINK_INPUT_CHARS) return null;
  try {
    const url = new URL(value);
    if (url.protocol !== "https:" || url.username || url.password || url.port || url.hash) return null;

    const host = url.hostname.toLowerCase();
    const parsed = host === "youtu.be"
      ? parseShortYouTubeLink(url)
      : YOUTUBE_WATCH_HOSTS.has(host)
        ? parseYouTubeWatchLink(url)
        : null;
    if (!parsed) return null;

    const canonical = parsed.short
      ? new URL(`https://youtu.be/${parsed.videoId}`)
      : new URL(`https://www.youtube.com/watch?v=${parsed.videoId}`);
    if (parsed.startSeconds !== undefined) canonical.searchParams.set("t", String(parsed.startSeconds));
    const rendered = canonical.toString();
    // URL parsing can expand Unicode/percent-encoded input. Enforce the cap on
    // the exact string that would be rendered into the iMessage transcript.
    return rendered.length <= MAX_SOURCE_DEEP_LINK_CHARS ? rendered : null;
  } catch {
    return null;
  }
}

type ParsedYouTubeVideo = {
  readonly videoId: string;
  readonly short: boolean;
  readonly startSeconds?: number;
};

function parseShortYouTubeLink(url: URL): ParsedYouTubeVideo | null {
  const segments = url.pathname.split("/").filter(Boolean);
  if (segments.length !== 1 || !YOUTUBE_VIDEO_ID_RE.test(segments[0] ?? "")) return null;
  const startSeconds = parseAllowedStart(url, new Set(["t", "start"]));
  if (startSeconds === null) return null;
  return {
    videoId: segments[0] as string,
    short: true,
    ...(startSeconds === undefined ? {} : { startSeconds }),
  };
}

function parseYouTubeWatchLink(url: URL): ParsedYouTubeVideo | null {
  let videoId: string | null = null;
  if (url.pathname === "/watch") {
    if (url.searchParams.getAll("v").length !== 1) return null;
    videoId = url.searchParams.get("v");
    const startSeconds = parseAllowedStart(url, new Set(["v", "t", "start"]));
    if (!videoId || !YOUTUBE_VIDEO_ID_RE.test(videoId) || startSeconds === null) return null;
    return {
      videoId,
      short: false,
      ...(startSeconds === undefined ? {} : { startSeconds }),
    };
  }

  const match = /^\/(?:shorts|embed)\/([A-Za-z0-9_-]{3,64})$/.exec(url.pathname);
  if (!match) return null;
  const startSeconds = parseAllowedStart(url, new Set(["t", "start"]));
  if (startSeconds === null) return null;
  return {
    videoId: match[1] as string,
    short: false,
    ...(startSeconds === undefined ? {} : { startSeconds }),
  };
}

/** Reject tracking/redirect query parameters rather than carrying them into a rendered link. */
function parseAllowedStart(url: URL, allowedKeys: ReadonlySet<string>): number | undefined | null {
  for (const key of url.searchParams.keys()) {
    if (!allowedKeys.has(key) || url.searchParams.getAll(key).length !== 1) return null;
  }
  const values = [url.searchParams.get("t"), url.searchParams.get("start")].filter(
    (candidate): candidate is string => candidate !== null,
  );
  if (values.length === 0) return undefined;
  if (values.length !== 1) return null;
  return parseStartSeconds(values[0] as string);
}

function parseStartSeconds(value: string): number | null {
  if (/^\d{1,7}$/.test(value)) {
    const seconds = Number(value);
    return Number.isSafeInteger(seconds) && seconds <= MAX_SOURCE_START_SECONDS ? seconds : null;
  }
  const parts = /^(?:(\d{1,3})h)?(?:(\d{1,3})m)?(?:(\d{1,3})s)?$/i.exec(value);
  if (!parts || !parts.slice(1).some(Boolean)) return null;
  const seconds =
    Number(parts[1] ?? 0) * 3_600 + Number(parts[2] ?? 0) * 60 + Number(parts[3] ?? 0);
  return Number.isSafeInteger(seconds) && seconds <= MAX_SOURCE_START_SECONDS ? seconds : null;
}

function hmacHex(secret: string, value: string): string {
  if (!secret || secret.trim().length < 1) {
    throw new Error("identity hash secret is required");
  }
  return createHmac("sha256", secret).update(value, "utf8").digest("hex");
}
