import { isIP } from "node:net";

import { STUDIO_PROVIDER_ARTIFACT_MAX_BYTES } from "./artifact-validation.js";

/**
 * The artifact download boundary shares the byte ceiling enforced by artifact
 * validation. A caller may select a smaller model-specific ceiling, never a
 * larger one.
 */
export const STUDIO_PROVIDER_ARTIFACT_FETCH_MAX_BYTES = STUDIO_PROVIDER_ARTIFACT_MAX_BYTES;
export const STUDIO_PROVIDER_ARTIFACT_FETCH_DEFAULT_TIMEOUT_MS = 12_000;

const MIN_TIMEOUT_MS = 250;
const MAX_TIMEOUT_MS = 60_000;
const MAX_ARTIFACT_URL_CHARS = 8_192;
const MAX_CONTENT_TYPE_CHARS = 256;
// GCS V4 permits a signed URL to become usable up to fifteen minutes before
// its active datetime. Accept that documented clock-skew window, but never
// mistake a later capability for one that can be used or persisted now.
const GCS_V4_EARLY_USE_WINDOW_MS = 15 * 60 * 1_000;
const GCS_V4_HMAC_SIGNATURE_HEX_CHARS = 64;
// RSA signatures are encoded as the full signature byte sequence in hex. The
// documented GCS example is far longer than an HMAC digest; accept reviewed
// common RSA key sizes without turning this URL parser into an unbounded
// carrier for provider-controlled query data.
const GCS_V4_RSA_SIGNATURE_MIN_HEX_CHARS = 64;
const GCS_V4_RSA_SIGNATURE_MAX_HEX_CHARS = 1_024;

/**
 * An immutable, registry-owned allowlist for one provider output location.
 * `host` is an exact, canonical DNS name—not a wildcard, URL, or host:port.
 * `pathPrefix` must name a non-root object namespace such as
 * `/gmi-generated-assets/air/`.
 *
 * `signed-url` is deliberately narrow: Air currently accepts only a complete
 * Google Cloud Storage V4 signed-query envelope. A provider-private result
 * cannot be fetched through this generic URL boundary because it would need a
 * provider-specific authenticated retrieval implementation instead of an
 * ambient or unauthenticated HTTP request.
 */
export interface StudioProviderArtifactFetchPolicy {
  readonly host: string;
  readonly pathPrefix: string;
  /** The committed model contract for this exact provider output location. */
  readonly access: "signed-url" | "provider-private";
}

/**
 * Timing metadata derived from a GCS V4 signed-query capability. It omits the
 * capability itself, credential, object path, and all provider response data
 * so callers can safely use it as a persistence/materialization gate.
 */
export interface StudioProviderArtifactReferenceLifetime {
  /** Absolute UTC expiry computed from X-Goog-Date plus X-Goog-Expires. */
  readonly expiresAtMs: number;
  /** Remaining lifetime at the caller-supplied (or current) wall clock. */
  readonly remainingMs: number;
}

/**
 * Performs the same exact policy and signed-query validation used by the
 * fetch boundary, while returning only non-secret timing metadata. A durable
 * lifecycle can require a full bounded read/upload window before it persists
 * or consumes a provider capability.
 */
export interface ValidateStudioProviderArtifactReferenceLifetimeInput {
  /** Provider-returned capability. It is never included in output or errors. */
  readonly url: string;
  /** Model-registry-owned exact output host and object path namespace. */
  readonly policy: StudioProviderArtifactFetchPolicy;
  /** A fail-closed lower bound for usable time after this validation. */
  readonly minimumRemainingMs: number;
  /** Test-only clock injection; production uses the current wall clock. */
  readonly nowMs?: number | undefined;
}

export interface FetchStudioProviderArtifactInput {
  /** Provider-returned URL. It is validated and deliberately never returned. */
  readonly url: string;
  /** Model-registry-owned exact output host and object path namespace. */
  readonly policy: StudioProviderArtifactFetchPolicy;
  /** Dependency injection for deterministic tests; production uses global fetch. */
  readonly fetch?: typeof globalThis.fetch | undefined;
  /** Whole-request deadline, including consumption of the returned stream. */
  readonly timeoutMs?: number | undefined;
  /** Optional model-specific ceiling. It can only make the hard cap smaller. */
  readonly maxBytes?: number | undefined;
}

/**
 * The only safe shape exposed from this module. It intentionally omits the
 * provider URL and response headers other than bounded metadata useful to the
 * byte validator.
 */
export interface StudioFetchedProviderArtifact {
  readonly stream: ReadableStream<Uint8Array>;
  /** A normalized content type when the response advertised a syntactically safe one. */
  readonly declaredMimeType?: string | undefined;
  /** The accepted response Content-Length, if supplied by the provider. */
  readonly contentLength?: number | undefined;
}

/**
 * Validates a provider retrieval capability without starting an HTTP request.
 * A split status-poll path uses this before it seals a private URL for later
 * encrypted persistence. The returned normalized URL remains caller-owned
 * sensitive material and must not be logged or placed in plaintext metadata.
 */
export function normalizeStudioProviderArtifactReference(input: {
  readonly url: string;
  readonly policy: StudioProviderArtifactFetchPolicy;
}): string {
  try {
    return normalizeArtifactUrl(input.url, normalizePolicy(input.policy));
  } catch (error) {
    if (error instanceof StudioProviderArtifactFetchError) throw error;
    fail("invalid_configuration");
  }
}

/**
 * Validates a signed provider artifact capability without exposing its URL.
 * The capability must already be live and retain at least the requested
 * conservative window. This is intentionally separate from fetching so a
 * store/lifecycle can fail closed before encrypted persistence or material
 * download starts.
 */
export function validateStudioProviderArtifactReferenceLifetime(
  input: ValidateStudioProviderArtifactReferenceLifetimeInput,
): StudioProviderArtifactReferenceLifetime {
  try {
    if (!input || typeof input !== "object" || Array.isArray(input)) fail("invalid_configuration");
    const policy = normalizePolicy(input.policy);
    const nowMs = normalizeWallClock(input.nowMs);
    const minimumRemainingMs = normalizeMinimumRemainingLifetime(input.minimumRemainingMs);
    return inspectArtifactReference(input.url, policy, nowMs, minimumRemainingMs).lifetime;
  } catch (error) {
    if (error instanceof StudioProviderArtifactFetchError) throw error;
    fail("invalid_configuration");
  }
}

export type StudioProviderArtifactFetchErrorCode =
  | "invalid_configuration"
  | "invalid_policy"
  | "invalid_url"
  | "invalid_signed_url"
  | "signed_url_not_yet_valid"
  | "signed_url_expired"
  | "signed_url_insufficient_lifetime"
  | "provider_private_transport_required"
  | "request_timeout"
  | "transport_failure"
  | "unexpected_status"
  | "invalid_response"
  | "content_length_invalid"
  | "content_length_too_large"
  | "response_body_unavailable"
  | "response_stream_failure"
  | "response_stream_too_large";

/**
 * This error is safe to surface in telemetry: it contains only a stable code.
 * It never contains the provider URL, a signed query string, response text,
 * or a native transport error.
 */
export class StudioProviderArtifactFetchError extends Error {
  public constructor(readonly code: StudioProviderArtifactFetchErrorCode) {
    super(`studio_provider_artifact_fetch_${code}`);
    this.name = "StudioProviderArtifactFetchError";
  }
}

interface NormalizedPolicy {
  readonly host: string;
  readonly pathPrefix: string;
  readonly access: "signed-url" | "provider-private";
}

interface NormalizedInput {
  readonly url: string;
  readonly fetch: typeof globalThis.fetch;
  readonly policy: NormalizedPolicy;
  readonly timeoutMs: number;
  readonly maxBytes: number;
}

interface InspectedArtifactReference {
  /** Internal-only normalized capability. It must never leave this module. */
  readonly normalizedUrl: string;
  /** Deliberately URL-free timing metadata available to callers. */
  readonly lifetime: StudioProviderArtifactReferenceLifetime;
}

interface ArtifactFetchDeadline {
  readonly signal: AbortSignal;
  readonly timeout: Promise<never>;
  readonly didExpire: () => boolean;
  readonly close: () => void;
}

class ArtifactFetchDeadlineExceeded extends Error {
  public constructor() {
    super("provider_artifact_fetch_deadline_exceeded");
    this.name = "ArtifactFetchDeadlineExceeded";
  }
}

/**
 * Fetches an already-authorized provider artifact without becoming a generic
 * URL fetcher. The output URL is fully checked before `fetch` is invoked;
 * redirects, ambient credentials, and over-limit bodies are rejected.
 *
 * The returned stream remains under the same deadline and byte ceiling. Feed
 * it to `validateStudioProviderArtifact` before any outbound attachment work.
 */
export async function fetchStudioProviderArtifact(
  input: FetchStudioProviderArtifactInput,
): Promise<StudioFetchedProviderArtifact> {
  let normalized: NormalizedInput;
  try {
    normalized = normalizeInput(input);
  } catch (error) {
    if (error instanceof StudioProviderArtifactFetchError) throw error;
    fail("invalid_configuration");
  }
  const deadline = createDeadline(normalized.timeoutMs);

  let response: Response;
  try {
    response = await raceWithDeadline(
      normalized.fetch(normalized.url, {
        method: "GET",
        redirect: "error",
        credentials: "omit",
        cache: "no-store",
        referrerPolicy: "no-referrer",
        signal: deadline.signal,
      }),
      deadline,
    );
  } catch (error) {
    deadline.close();
    if (deadline.didExpire() || error instanceof ArtifactFetchDeadlineExceeded) {
      fail("request_timeout");
    }
    fail("transport_failure");
  }

  if (!isResponseLike(response)) {
    deadline.close();
    fail("invalid_response");
  }
  if (deadline.didExpire()) {
    void cancelResponseBody(response);
    deadline.close();
    fail("request_timeout");
  }
  if (response.status !== 200) {
    void cancelResponseBody(response);
    deadline.close();
    fail("unexpected_status");
  }

  let contentLength: number | undefined;
  let declaredMimeType: string | undefined;
  try {
    contentLength = parseContentLength(response, normalized.maxBytes);
    declaredMimeType = parseDeclaredMimeType(response);
  } catch (error) {
    void cancelResponseBody(response);
    deadline.close();
    if (error instanceof StudioProviderArtifactFetchError) throw error;
    fail("invalid_response");
  }

  if (response.body === null) {
    deadline.close();
    fail("response_body_unavailable");
  }

  let reader: ReadableStreamDefaultReader<Uint8Array>;
  try {
    reader = response.body.getReader();
  } catch {
    void cancelResponseBody(response);
    deadline.close();
    fail("response_body_unavailable");
  }

  return Object.freeze({
    stream: createBoundedArtifactStream(reader, normalized.maxBytes, deadline),
    ...(declaredMimeType === undefined ? {} : { declaredMimeType }),
    ...(contentLength === undefined ? {} : { contentLength }),
  });
}

function normalizeInput(input: FetchStudioProviderArtifactInput): NormalizedInput {
  if (!input || typeof input !== "object" || Array.isArray(input)) fail("invalid_configuration");
  const fetch = input.fetch ?? globalThis.fetch;
  if (typeof fetch !== "function") fail("invalid_configuration");

  const policy = normalizePolicy(input.policy);
  const url = normalizeArtifactUrl(input.url, policy);
  const timeoutMs = normalizeTimeout(input.timeoutMs);
  const maxBytes = normalizeMaxBytes(input.maxBytes);
  return Object.freeze({ url, fetch, policy, timeoutMs, maxBytes });
}

function normalizePolicy(value: StudioProviderArtifactFetchPolicy): NormalizedPolicy {
  if (!value || typeof value !== "object" || Array.isArray(value)) fail("invalid_policy");
  const rawHost = value.host;
  const pathPrefix = value.pathPrefix;
  const access = value.access;
  if (
    typeof rawHost !== "string" ||
    typeof pathPrefix !== "string" ||
    (access !== "signed-url" && access !== "provider-private")
  ) {
    fail("invalid_policy");
  }

  let hostUrl: URL;
  try {
    hostUrl = new URL(`https://${rawHost}`);
  } catch {
    fail("invalid_policy");
  }
  const host = hostUrl.hostname.toLowerCase();
  if (
    rawHost !== host ||
    !isCanonicalDnsHost(host) ||
    hostUrl.protocol !== "https:" ||
    hostUrl.username ||
    hostUrl.password ||
    hostUrl.port ||
    hostUrl.pathname !== "/" ||
    hostUrl.search ||
    hostUrl.hash
  ) {
    fail("invalid_policy");
  }

  if (
    pathPrefix.length < 3 ||
    pathPrefix.length > 2_048 ||
    !pathPrefix.startsWith("/") ||
    !pathPrefix.endsWith("/") ||
    pathPrefix === "/" ||
    pathPrefix.includes("//") ||
    pathPrefix.includes("\\") ||
    pathPrefix.includes("?") ||
    pathPrefix.includes("#") ||
    pathPrefix.includes("%") ||
    /[\u0000\r\n]/.test(pathPrefix)
  ) {
    fail("invalid_policy");
  }
  const segments = pathPrefix.slice(1, -1).split("/");
  if (segments.length === 0 || segments.some((segment) => !/^[A-Za-z0-9._~-]+$/.test(segment) || segment === "." || segment === "..")) {
    fail("invalid_policy");
  }
  return Object.freeze({ host, pathPrefix, access });
}

function isCanonicalDnsHost(host: string): boolean {
  if (host.length < 4 || host.length > 253 || !host.includes(".") || host.endsWith(".")) return false;
  try {
    const normalizedHost = new URL(`https://${host}`).hostname;
    // Reject both ordinary IPv4 literals and legacy numeric forms that URL
    // normalizes to one before an allowlisted artifact fetch is attempted.
    if (normalizedHost !== host || isIP(normalizedHost) !== 0) return false;
  } catch {
    return false;
  }
  const labels = host.split(".");
  return labels.every((label) => (
    label.length > 0 &&
    label.length <= 63 &&
    /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label)
  ));
}

function normalizeArtifactUrl(value: string, policy: NormalizedPolicy): string {
  return inspectArtifactReference(value, policy, normalizeWallClock(undefined)).normalizedUrl;
}

/**
 * Parses and validates one capability at the policy boundary. The normalized
 * URL stays internal even when the caller only needs its safe timing facts.
 */
function inspectArtifactReference(
  value: string,
  policy: NormalizedPolicy,
  nowMs: number,
  minimumRemainingMs?: number,
): InspectedArtifactReference {
  // A provider-private result must be fetched by a dedicated provider adapter
  // that can prove its authentication contract. Never fall back to a generic
  // unauthenticated GET merely because an exact host/path was reviewed.
  if (policy.access === "provider-private") fail("provider_private_transport_required");

  if (
    typeof value !== "string" ||
    !value ||
    value.length > MAX_ARTIFACT_URL_CHARS ||
    /[\u0000\r\n]/.test(value)
  ) {
    fail("invalid_url");
  }

  let url: URL;
  try {
    url = new URL(value);
  } catch {
    fail("invalid_url");
  }
  if (
    hasUnsafeRawAuthority(value) ||
    url.protocol !== "https:" ||
    url.hostname.toLowerCase() !== policy.host ||
    url.username ||
    url.password ||
    url.port ||
    url.hash ||
    url.pathname.length <= policy.pathPrefix.length ||
    !url.pathname.startsWith(policy.pathPrefix) ||
    url.pathname.includes("//") ||
    /(?:^|\/)\.{1,2}(?:\/|$)/.test(url.pathname) ||
    /%2f|%5c|%2e/i.test(url.pathname)
  ) {
    fail("invalid_url");
  }
  const lifetime = inspectGoogleCloudStorageV4Signature(url, nowMs, minimumRemainingMs);
  if (lifetime === null) fail("invalid_signed_url");
  return Object.freeze({ normalizedUrl: url.toString(), lifetime });
}

/**
 * Require the complete GCS V4 query signature envelope, rather than treating
 * a provider's `is_public: false` field or a bare storage URL as proof that an
 * object is private. The storage service validates the actual signature; this
 * boundary ensures Air only attempts that authenticated retrieval shape.
 */
function inspectGoogleCloudStorageV4Signature(
  url: URL,
  nowMs: number,
  minimumRemainingMs: number | undefined,
): StudioProviderArtifactReferenceLifetime | null {
  const parameters = url.searchParams;
  const algorithm = exactlyOneQueryParameter(parameters, "X-Goog-Algorithm");
  const credential = exactlyOneQueryParameter(parameters, "X-Goog-Credential");
  const date = exactlyOneQueryParameter(parameters, "X-Goog-Date");
  const expires = exactlyOneQueryParameter(parameters, "X-Goog-Expires");
  const signedHeaders = exactlyOneQueryParameter(parameters, "X-Goog-SignedHeaders");
  const signature = exactlyOneQueryParameter(parameters, "X-Goog-Signature");

  if (
    (algorithm !== "GOOG4-RSA-SHA256" && algorithm !== "GOOG4-HMAC-SHA256") ||
    credential === null ||
    date === null ||
    expires === null ||
    signedHeaders === null ||
    signature === null
  ) {
    return null;
  }

  if (!isGoogleCloudStorageV4Credential(credential)) return null;
  if (!/^\d{8}T\d{6}Z$/u.test(date)) return null;
  if (!/^[1-9]\d{0,5}$/u.test(expires)) return null;
  const expiresSeconds = Number(expires);
  // GCS V4 signed URLs can be valid for no more than seven days.
  if (!Number.isSafeInteger(expiresSeconds) || expiresSeconds > 604_800) return null;
  if (!hasCanonicalSignedHeaders(signedHeaders) || !hasGoogleCloudStorageV4Signature(algorithm, signature)) {
    return null;
  }

  const signedAtMs = parseGoogleCloudStorageV4SigningTime(date);
  if (signedAtMs === null) return null;
  const expiresAtMs = signedAtMs + expiresSeconds * 1_000;
  if (!Number.isSafeInteger(expiresAtMs)) return null;

  // GCS V4 allows up to fifteen minutes of early use for ordinary signing
  // clock skew. Beyond that documented window, this worker cannot consume or
  // durably retain the capability yet.
  if (signedAtMs - nowMs > GCS_V4_EARLY_USE_WINDOW_MS) fail("signed_url_not_yet_valid");
  if (expiresAtMs <= nowMs) fail("signed_url_expired");

  const remainingMs = expiresAtMs - nowMs;
  if (!Number.isSafeInteger(remainingMs) || remainingMs < 1) fail("signed_url_expired");
  if (minimumRemainingMs !== undefined && remainingMs < minimumRemainingMs) {
    fail("signed_url_insufficient_lifetime");
  }
  return Object.freeze({ expiresAtMs, remainingMs });
}

/**
 * `Date.parse` accepts several non-canonical forms and normalizes invalid
 * calendar dates. GCS signs the compact UTC form, so reconstruct and compare
 * every component before using it in an authorization lifetime calculation.
 */
function parseGoogleCloudStorageV4SigningTime(value: string): number | null {
  const match = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/u.exec(value);
  if (match === null) return null;

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  if (
    !Number.isSafeInteger(year) || year < 1 ||
    !Number.isSafeInteger(month) || month < 1 || month > 12 ||
    !Number.isSafeInteger(day) || day < 1 || day > 31 ||
    !Number.isSafeInteger(hour) || hour > 23 ||
    !Number.isSafeInteger(minute) || minute > 59 ||
    !Number.isSafeInteger(second) || second > 59
  ) {
    return null;
  }

  // Start from epoch and use setUTCFullYear so years 0001-0099 do not receive
  // Date.UTC's legacy 1900 offset. The component comparison rejects leap-day
  // and month-end normalizations such as 20260229 or 20260431.
  const signedAt = new Date(0);
  signedAt.setUTCFullYear(year, month - 1, day);
  signedAt.setUTCHours(hour, minute, second, 0);
  const signedAtMs = signedAt.getTime();
  if (
    !Number.isSafeInteger(signedAtMs) ||
    signedAt.getUTCFullYear() !== year ||
    signedAt.getUTCMonth() !== month - 1 ||
    signedAt.getUTCDate() !== day ||
    signedAt.getUTCHours() !== hour ||
    signedAt.getUTCMinutes() !== minute ||
    signedAt.getUTCSeconds() !== second ||
    signedAt.getUTCMilliseconds() !== 0
  ) {
    return null;
  }
  return signedAtMs;
}

function exactlyOneQueryParameter(parameters: URLSearchParams, name: string): string | null {
  const values = parameters.getAll(name);
  return values.length === 1 ? values[0] ?? null : null;
}

function isGoogleCloudStorageV4Credential(value: string): boolean {
  // `<access-key>/<yyyymmdd>/<region>/storage/goog4_request` after URL
  // decoding. Keep every part bounded and printable before accepting it.
  return /^[A-Za-z0-9._-]{1,256}\/\d{8}\/[A-Za-z0-9-]{1,64}\/storage\/goog4_request$/u.test(value);
}

function hasGoogleCloudStorageV4Signature(algorithm: string, signature: string): boolean {
  if (algorithm === "GOOG4-HMAC-SHA256") {
    return new RegExp(`^[a-f0-9]{${GCS_V4_HMAC_SIGNATURE_HEX_CHARS}}$`, "u").test(signature);
  }
  return algorithm === "GOOG4-RSA-SHA256"
    && signature.length >= GCS_V4_RSA_SIGNATURE_MIN_HEX_CHARS
    && signature.length <= GCS_V4_RSA_SIGNATURE_MAX_HEX_CHARS
    && signature.length % 2 === 0
    && /^[a-f0-9]+$/u.test(signature);
}

function hasCanonicalSignedHeaders(value: string): boolean {
  if (!/^[a-z0-9-]+(?:;[a-z0-9-]+)*$/u.test(value)) return false;
  const names = value.split(";");
  return names.includes("host") && new Set(names).size === names.length;
}

/**
 * URL canonicalization removes an explicit default port (`:443`). Keep the
 * policy exact by rejecting an authority that supplied a port or user-info
 * before the WHATWG parser can erase that distinction. The policy only allows
 * DNS hosts, so a colon can never be legitimate authority syntax here.
 */
function hasUnsafeRawAuthority(value: string): boolean {
  const schemeEnd = value.indexOf("://");
  if (schemeEnd < 0) return true;
  const authorityStart = schemeEnd + 3;
  const pathStart = value.slice(authorityStart).search(/[/?#]/);
  const authority = pathStart < 0
    ? value.slice(authorityStart)
    : value.slice(authorityStart, authorityStart + pathStart);
  return authority.includes(":") || authority.includes("@");
}

function normalizeTimeout(value: number | undefined): number {
  const timeoutMs = value ?? STUDIO_PROVIDER_ARTIFACT_FETCH_DEFAULT_TIMEOUT_MS;
  if (!Number.isInteger(timeoutMs) || timeoutMs < MIN_TIMEOUT_MS || timeoutMs > MAX_TIMEOUT_MS) {
    fail("invalid_configuration");
  }
  return timeoutMs;
}

function normalizeWallClock(value: number | undefined): number {
  const nowMs = value ?? Date.now();
  if (!Number.isSafeInteger(nowMs) || nowMs < 0) fail("invalid_configuration");
  return nowMs;
}

function normalizeMinimumRemainingLifetime(value: number): number {
  if (!Number.isSafeInteger(value) || value < 1) fail("invalid_configuration");
  return value;
}

function normalizeMaxBytes(value: number | undefined): number {
  const maxBytes = value ?? STUDIO_PROVIDER_ARTIFACT_FETCH_MAX_BYTES;
  if (!Number.isInteger(maxBytes) || maxBytes < 1 || maxBytes > STUDIO_PROVIDER_ARTIFACT_FETCH_MAX_BYTES) {
    fail("invalid_configuration");
  }
  return maxBytes;
}

function parseContentLength(response: Response, maxBytes: number): number | undefined {
  const value = readHeader(response, "content-length");
  if (value === null) return undefined;
  if (!/^\d{1,8}$/.test(value)) fail("content_length_invalid");
  const contentLength = Number(value);
  if (!Number.isSafeInteger(contentLength)) fail("content_length_invalid");
  if (contentLength > maxBytes) fail("content_length_too_large");
  return contentLength;
}

function parseDeclaredMimeType(response: Response): string | undefined {
  const value = readHeader(response, "content-type");
  if (value === null || value.length > MAX_CONTENT_TYPE_CHARS || /[\u0000\r\n]/.test(value)) return undefined;
  const candidate = value.split(";", 1)[0]?.trim().toLowerCase();
  if (!candidate || !/^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/.test(candidate)) return undefined;
  return candidate;
}

function readHeader(response: Response, name: string): string | null {
  try {
    const value = response.headers.get(name);
    return typeof value === "string" ? value : null;
  } catch {
    fail("invalid_response");
  }
}

function createDeadline(timeoutMs: number): ArtifactFetchDeadline {
  const controller = new AbortController();
  let expired = false;
  let rejectTimeout: (reason: ArtifactFetchDeadlineExceeded) => void = () => undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    rejectTimeout = reject as (reason: ArtifactFetchDeadlineExceeded) => void;
  });
  const timer = setTimeout(() => {
    expired = true;
    controller.abort();
    rejectTimeout(new ArtifactFetchDeadlineExceeded());
  }, timeoutMs);
  let closed = false;
  return Object.freeze({
    signal: controller.signal,
    timeout,
    didExpire: () => expired,
    close: () => {
      if (closed) return;
      closed = true;
      clearTimeout(timer);
    },
  });
}

async function raceWithDeadline<T>(operation: Promise<T>, deadline: ArtifactFetchDeadline): Promise<T> {
  return Promise.race([operation, deadline.timeout]);
}

function createBoundedArtifactStream(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  maxBytes: number,
  deadline: ArtifactFetchDeadline,
): ReadableStream<Uint8Array> {
  let totalBytes = 0;
  let finished = false;

  const finish = (): void => {
    if (finished) return;
    finished = true;
    deadline.close();
    try {
      reader.releaseLock();
    } catch {
      // Release is best effort; do not expose provider stream implementation details.
    }
  };
  const cancelReader = async (): Promise<void> => {
    try {
      await reader.cancel();
    } catch {
      // The boundary deliberately never forwards a provider stream error.
    } finally {
      finish();
    }
  };
  const failStream = (
    controller: ReadableStreamDefaultController<Uint8Array>,
    code: Extract<StudioProviderArtifactFetchErrorCode, "request_timeout" | "response_stream_failure" | "response_stream_too_large">,
  ): void => {
    // Do not await a malicious stream's cancellation: a cancellation promise
    // must never extend Air's deadline or delay the safe error to the caller.
    void cancelReader();
    controller.error(new StudioProviderArtifactFetchError(code));
  };

  return new ReadableStream<Uint8Array>({
    async pull(controller): Promise<void> {
      if (finished) return;
      let next: ReadableStreamReadResult<Uint8Array>;
      try {
        next = await raceWithDeadline(reader.read(), deadline);
      } catch (error) {
        failStream(
          controller,
          deadline.didExpire() || error instanceof ArtifactFetchDeadlineExceeded
            ? "request_timeout"
            : "response_stream_failure",
        );
        return;
      }

      if (!next || typeof next !== "object" || typeof next.done !== "boolean") {
        failStream(controller, "response_stream_failure");
        return;
      }

      if (next.done) {
        finish();
        controller.close();
        return;
      }
      if (!(next.value instanceof Uint8Array)) {
        failStream(controller, "response_stream_failure");
        return;
      }

      totalBytes += next.value.byteLength;
      if (!Number.isSafeInteger(totalBytes) || totalBytes > maxBytes) {
        failStream(controller, "response_stream_too_large");
        return;
      }
      controller.enqueue(next.value);
    },
    async cancel(): Promise<void> {
      await cancelReader();
    },
  });
}

function isResponseLike(value: unknown): value is Response {
  if (!value || typeof value !== "object") return false;
  const response = value as Partial<Response>;
  return (
    typeof response.status === "number" &&
    Number.isInteger(response.status) &&
    response.status >= 100 &&
    response.status <= 599 &&
    !!response.headers &&
    typeof response.headers.get === "function"
  );
}

async function cancelResponseBody(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // A response body is untrusted and may already be locked or failed.
  }
}

function fail(code: StudioProviderArtifactFetchErrorCode): never {
  throw new StudioProviderArtifactFetchError(code);
}
