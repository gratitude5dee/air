import {
  canRecoverStudioProvider,
  canSubmitStudioProvider,
  type StudioSideEffectEnvironment,
} from "./config.js";

/**
 * Narrow, direct boundary for GMI's documented request-queue API.
 *
 * This module deliberately has no model registry, model-enablement, logging,
 * retry loop, or automatic network work.  Callers inject `fetch` in tests and
 * must make an explicit method call before any provider request is sent.
 */

export const GMI_REQUEST_QUEUE_ORIGIN = "https://console.gmicloud.ai";
export const GMI_REQUEST_QUEUE_PATH = "/api/v1/ie/requestqueue/apikey";
export const GMI_REQUEST_QUEUE_BASE_URL = `${GMI_REQUEST_QUEUE_ORIGIN}${GMI_REQUEST_QUEUE_PATH}`;

/** Provider JSON is bounded before parsing so a bad response cannot fill memory. */
export const MAX_GMI_RESPONSE_BYTES = 128 * 1024;
/** A data-URI-capable model may need a full, bounded input asset in its JSON payload. */
export const MAX_GMI_REQUEST_BYTES = 16 * 1024 * 1024;

const DEFAULT_TIMEOUT_MS = 12_000;
const MIN_TIMEOUT_MS = 250;
const MAX_TIMEOUT_MS = 60_000;
const MIN_RESPONSE_BYTES = 1_024;
const MAX_JSON_DEPTH = 32;
const MAX_JSON_OBJECT_ENTRIES = 2_048;
const MAX_JSON_ARRAY_ENTRIES = 2_048;
const MAX_JSON_KEY_CHARS = 512;
const MAX_JSON_STRING_CHARS = MAX_GMI_REQUEST_BYTES;
const MAX_URL_CHARS = 8_192;

/** A JSON-only value, used because provider schemas are discovered separately. */
export type GmiJsonValue =
  | null
  | boolean
  | number
  | string
  | readonly GmiJsonValue[]
  | GmiJsonObject;

export interface GmiJsonObject {
  readonly [key: string]: GmiJsonValue;
}

export type GmiRequestQueueErrorCode =
  | "invalid_configuration"
  | "invalid_request"
  | "provider_submission_disabled"
  | "provider_recovery_disabled"
  | "request_timeout"
  | "transport_failure"
  | "unexpected_status"
  | "invalid_response";

/**
 * Safe-to-log error shape.  Its message intentionally excludes a URL, API key,
 * organization ID, request body, response body, and native fetch error.
 */
export class GmiRequestQueueError extends Error {
  public constructor(
    readonly code: GmiRequestQueueErrorCode,
    readonly retryable: boolean,
    readonly httpStatus?: number,
  ) {
    super(`gmi_request_queue_${code}`);
    this.name = "GmiRequestQueueError";
  }
}

export interface GmiRequestQueueClientOptions {
  readonly apiKey: string;
  readonly organizationId?: string | undefined;
  /**
   * Required for every mutating request and provider-recovery request. Omit it
   * for read-only catalog discovery only; omission fails closed for all other
   * operations.
   */
  readonly sideEffectEnvironment?: StudioSideEffectEnvironment | undefined;
  readonly fetch?: typeof globalThis.fetch | undefined;
  readonly timeoutMs?: number | undefined;
  /** Primarily useful for deterministic overflow tests. Never raises the hard cap. */
  readonly maxResponseBytes?: number | undefined;
}

export interface GmiRequestQueueEnvironment {
  readonly GMI_API_KEY?: string | undefined;
  readonly GMI_ORG_ID?: string | undefined;
}

export type GmiUploadFileType = "jpeg" | "jpg" | "png" | "mp4" | "mp3" | "wav";

/**
 * A registry-owned, exact GCS location.  It is intentionally not a generic
 * "Google Storage" switch: each model must pin one host and object prefix.
 */
export interface GmiGoogleStoragePathPolicy {
  readonly host: string;
  /** A non-root, slash-terminated object-path prefix, for example `/bucket/air/`. */
  readonly pathPrefix: string;
}

export interface GmiCreateUploadUrlInput {
  readonly fileType: GmiUploadFileType;
  readonly storage: GmiGoogleStoragePathPolicy;
  /** When the documented model contract supports it, callers may only request private. */
  readonly isPublic?: false | undefined;
}

export interface GmiUploadUrl {
  readonly uploadUrl: string;
  readonly publicUrl: string;
}

export interface GmiSubmitRequestInput {
  readonly model: string;
  /** Model-specific schema is verified by the registry before it reaches this boundary. */
  readonly payload: GmiJsonObject;
}

export type GmiRequestStatus =
  | "dispatched"
  | "processing"
  | "success"
  | "failed"
  | "cancelled"
  | "unknown";

export interface GmiRequestObservation {
  readonly requestId: string;
  readonly status: GmiRequestStatus;
  /** Bounded parsed JSON for the model-specific artifact parser. Never log this value. */
  readonly response: GmiJsonObject;
}

export interface GmiAcceptedSubmission {
  readonly classification: "accepted";
  readonly requestId: string;
  readonly status: GmiRequestStatus;
}

export interface GmiDefinitiveRejection {
  readonly classification: "definitive_rejection";
  /** A non-429 4xx response is provider evidence that this request was rejected. */
  readonly httpStatus: number;
}

export type GmiAmbiguousSubmissionReason =
  | "timeout"
  | "transport_failure"
  | "malformed_response"
  | "rate_limited"
  | "server_error"
  | "unexpected_status";

export interface GmiAmbiguousSubmission {
  readonly classification: "ambiguous";
  readonly reason: GmiAmbiguousSubmissionReason;
  readonly httpStatus?: number | undefined;
}

/** Submission intentionally has no retryable fourth state. */
export type GmiSubmitResult =
  | GmiAcceptedSubmission
  | GmiDefinitiveRejection
  | GmiAmbiguousSubmission;

interface NormalizedStoragePolicy {
  readonly host: string;
  readonly pathPrefix: string;
}

interface JsonResponse {
  readonly status: number;
  readonly body: GmiJsonValue;
}

type FetchLike = typeof globalThis.fetch;

/**
 * Authenticated client for the fixed GMI request-queue origin.  There is no
 * origin override so a configuration typo cannot turn this into an SSRF client.
 */
export class GmiRequestQueueClient {
  private readonly apiKey: string;
  private readonly organizationId: string | undefined;
  private readonly sideEffectEnvironment: StudioSideEffectEnvironment | undefined;
  private readonly fetch: FetchLike;
  private readonly timeoutMs: number;
  private readonly maxResponseBytes: number;

  public constructor(options: GmiRequestQueueClientOptions) {
    this.apiKey = parseSecretHeader(options.apiKey);
    this.organizationId = options.organizationId === undefined
      ? undefined
      : parseOrganizationId(options.organizationId);
    this.sideEffectEnvironment = options.sideEffectEnvironment;
    this.fetch = options.fetch ?? globalThis.fetch;
    if (typeof this.fetch !== "function") {
      throw new GmiRequestQueueError("invalid_configuration", false);
    }
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    if (!Number.isInteger(this.timeoutMs) || this.timeoutMs < MIN_TIMEOUT_MS || this.timeoutMs > MAX_TIMEOUT_MS) {
      throw new GmiRequestQueueError("invalid_configuration", false);
    }
    this.maxResponseBytes = options.maxResponseBytes ?? MAX_GMI_RESPONSE_BYTES;
    if (
      !Number.isInteger(this.maxResponseBytes) ||
      this.maxResponseBytes < MIN_RESPONSE_BYTES ||
      this.maxResponseBytes > MAX_GMI_RESPONSE_BYTES
    ) {
      throw new GmiRequestQueueError("invalid_configuration", false);
    }
  }

  /** Read-only catalog discovery. This method does not enable a model. */
  public async listModels(): Promise<GmiJsonValue> {
    const response = await this.requestJson("/models", { method: "GET", expectedStatuses: [200] });
    return response.body;
  }

  /** Read-only schema/detail discovery for one exact provider model ID. */
  public async getModel(modelId: string): Promise<GmiJsonValue> {
    validateModelId(modelId);
    const response = await this.requestJson(`/models/${encodeURIComponent(modelId)}`, {
      method: "GET",
      expectedStatuses: [200],
    });
    return response.body;
  }

  /** Alias kept explicit for call sites that distinguish catalog from model detail. */
  public async getModelDetail(modelId: string): Promise<GmiJsonValue> {
    return this.getModel(modelId);
  }

  /**
   * Submit once.  A caller must persist its intent before this call.  GMI does
   * not document idempotent submission lookup, so every uncertain outcome is
   * deliberately returned as `ambiguous` rather than retried here.
   */
  public async submitRequest(input: GmiSubmitRequestInput): Promise<GmiSubmitResult> {
    this.requireProviderSubmission();
    validateSubmitInput(input);
    let response: Response;
    try {
      response = await this.request("/requests", {
        method: "POST",
        body: encodeBoundedJson({ model: input.model, payload: input.payload }),
      });
    } catch (error) {
      return classifySubmitError(error);
    }

    if (response.status >= 200 && response.status < 300) {
      try {
        const body = await readBoundedJson(response, this.maxResponseBytes);
        return parseAcceptedSubmission(body);
      } catch {
        // A 2xx response without a valid request_id cannot prove that GMI did
        // not create a paid job, so it must remain operator-reconciled.
        return ambiguousSubmission("malformed_response");
      }
    }

    discardResponseBody(response);
    if (response.status >= 400 && response.status < 500 && response.status !== 429) {
      return Object.freeze({ classification: "definitive_rejection", httpStatus: response.status });
    }
    if (response.status === 429) return ambiguousSubmission("rate_limited", response.status);
    if (response.status >= 500) return ambiguousSubmission("server_error", response.status);
    return ambiguousSubmission("unexpected_status", response.status);
  }

  /** Alias for consumers whose provider port calls the operation `submit`. */
  public async submit(input: GmiSubmitRequestInput): Promise<GmiSubmitResult> {
    return this.submitRequest(input);
  }

  /** Observe an already accepted request. Status retries are owned by the job worker. */
  public async getRequestStatus(requestId: string): Promise<GmiRequestObservation> {
    this.requireProviderRecovery();
    validateRequestId(requestId);
    const response = await this.requestJson(`/requests/${encodeURIComponent(requestId)}`, {
      method: "GET",
      expectedStatuses: [200],
    });
    return parseRequestObservation(response.body, requestId);
  }

  /** Alias for code that models the endpoint as a resource read. */
  public async getRequest(requestId: string): Promise<GmiRequestObservation> {
    return this.getRequestStatus(requestId);
  }

  /**
   * Ask GMI for a presigned direct-upload URL.  Both URLs are checked against
   * an exact registry-provided GCS host and object prefix before they escape.
   */
  public async createUploadUrl(input: GmiCreateUploadUrlInput): Promise<GmiUploadUrl> {
    this.requireProviderSubmission();
    validateUploadInput(input);
    const storage = normalizeStoragePolicy(input.storage, "invalid_request");
    const response = await this.requestJson("/upload-url", {
      method: "POST",
      expectedStatuses: [200, 201],
      body: encodeBoundedJson({
        file_type: input.fileType,
        ...(input.isPublic === undefined ? {} : { is_public: false }),
      }),
    });
    return parseUploadUrl(response.body, storage);
  }

  /** Alias for call sites that use the provider's endpoint terminology. */
  public async getUploadUrl(input: GmiCreateUploadUrlInput): Promise<GmiUploadUrl> {
    return this.createUploadUrl(input);
  }

  private requireProviderSubmission(): void {
    if (!this.sideEffectEnvironment || !canSubmitStudioProvider(this.sideEffectEnvironment)) {
      throw new GmiRequestQueueError("provider_submission_disabled", false);
    }
  }

  private requireProviderRecovery(): void {
    if (!this.sideEffectEnvironment || !canRecoverStudioProvider(this.sideEffectEnvironment)) {
      throw new GmiRequestQueueError("provider_recovery_disabled", false);
    }
  }

  private async requestJson(
    path: string,
    options: {
      readonly method: "GET" | "POST";
      readonly expectedStatuses: readonly number[];
      readonly body?: string | undefined;
    },
  ): Promise<JsonResponse> {
    const response = await this.request(path, options);
    if (!options.expectedStatuses.includes(response.status)) {
      discardResponseBody(response);
      throw new GmiRequestQueueError(
        "unexpected_status",
        response.status === 429 || response.status >= 500,
        response.status,
      );
    }
    return Object.freeze({
      status: response.status,
      body: await readBoundedJson(response, this.maxResponseBytes),
    });
  }

  private async request(
    path: string,
    options: { readonly method: "GET" | "POST"; readonly body?: string | undefined },
  ): Promise<Response> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await this.fetch(new URL(`${GMI_REQUEST_QUEUE_PATH}${path}`, GMI_REQUEST_QUEUE_ORIGIN), {
        method: options.method,
        headers: {
          accept: "application/json",
          authorization: `Bearer ${this.apiKey}`,
          ...(this.organizationId === undefined ? {} : { "x-organization-id": this.organizationId }),
          ...(options.body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(options.body === undefined ? {} : { body: options.body }),
        cache: "no-store",
        credentials: "omit",
        redirect: "error",
        signal: controller.signal,
      });
      if (!isResponseLike(response)) {
        throw new GmiRequestQueueError("invalid_response", false);
      }
      return response;
    } catch (error) {
      if (error instanceof GmiRequestQueueError) throw error;
      if (controller.signal.aborted) throw new GmiRequestQueueError("request_timeout", true);
      // Do not retain or rethrow a native fetch error: it can contain a URL or
      // provider diagnostic body in some runtimes.
      throw new GmiRequestQueueError("transport_failure", true);
    } finally {
      clearTimeout(timeout);
    }
  }
}

export function createGmiRequestQueueClient(options: GmiRequestQueueClientOptions): GmiRequestQueueClient {
  return new GmiRequestQueueClient(options);
}

/** Read only named environment values; no environment value is ever logged or returned. */
export function createGmiRequestQueueClientFromEnv(
  environment: GmiRequestQueueEnvironment = process.env,
  options: Omit<GmiRequestQueueClientOptions, "apiKey" | "organizationId"> = {},
): GmiRequestQueueClient {
  const apiKey = environment.GMI_API_KEY;
  if (typeof apiKey !== "string") throw new GmiRequestQueueError("invalid_configuration", false);
  const organizationId = optionalEnvironmentHeader(environment.GMI_ORG_ID);
  return createGmiRequestQueueClient({
    ...options,
    apiKey,
    ...(organizationId === undefined ? {} : { organizationId }),
  });
}

/**
 * Reusable host-and-prefix validator for GMI result URLs.  It rejects a bare
 * `storage.googleapis.com` allowlist as well as a URL that only matches it.
 */
export function validateGmiGoogleStorageUrl(
  value: string,
  policy: GmiGoogleStoragePathPolicy,
): string {
  return validateGoogleStorageUrl(value, normalizeStoragePolicy(policy, "invalid_request"), "invalid_response");
}

function parseSecretHeader(value: string): string {
  if (typeof value !== "string" || /[\u0000\r\n]/.test(value)) {
    throw new GmiRequestQueueError("invalid_configuration", false);
  }
  const token = value.trim();
  if (!token || token.length > 4_096) throw new GmiRequestQueueError("invalid_configuration", false);
  return token;
}

function parseOrganizationId(value: string): string {
  if (typeof value !== "string" || /[\u0000\r\n]/.test(value)) {
    throw new GmiRequestQueueError("invalid_configuration", false);
  }
  const id = value.trim();
  if (!id || id.length > 512) throw new GmiRequestQueueError("invalid_configuration", false);
  return id;
}

function optionalEnvironmentHeader(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string") throw new GmiRequestQueueError("invalid_configuration", false);
  if (value.trim() === "") return undefined;
  return parseOrganizationId(value);
}

function validateSubmitInput(input: GmiSubmitRequestInput): void {
  if (!input || typeof input !== "object" || Array.isArray(input)) invalidRequest();
  validateModelId(input.model);
  assertBoundedJsonObject(input.payload, "invalid_request");
}

function validateModelId(value: string): void {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,511}$/.test(value)) invalidRequest();
}

function validateRequestId(value: string): void {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$/.test(value)) invalidRequest();
}

function validateUploadInput(input: GmiCreateUploadUrlInput): void {
  if (!input || typeof input !== "object" || Array.isArray(input)) invalidRequest();
  if (
    input.fileType !== "jpeg" &&
    input.fileType !== "jpg" &&
    input.fileType !== "png" &&
    input.fileType !== "mp4" &&
    input.fileType !== "mp3" &&
    input.fileType !== "wav"
  ) {
    invalidRequest();
  }
  if (input.isPublic !== undefined && input.isPublic !== false) invalidRequest();
}

function normalizeStoragePolicy(
  value: GmiGoogleStoragePathPolicy,
  errorCode: "invalid_request" | "invalid_response",
): NormalizedStoragePolicy {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new GmiRequestQueueError(errorCode, false);
  const rawHost = value.host;
  const pathPrefix = value.pathPrefix;
  if (typeof rawHost !== "string" || typeof pathPrefix !== "string") {
    throw new GmiRequestQueueError(errorCode, false);
  }

  let hostUrl: URL;
  try {
    hostUrl = new URL(`https://${rawHost}`);
  } catch {
    throw new GmiRequestQueueError(errorCode, false);
  }
  const host = hostUrl.hostname.toLowerCase();
  if (
    !host ||
    hostUrl.protocol !== "https:" ||
    hostUrl.username ||
    hostUrl.password ||
    hostUrl.port ||
    hostUrl.pathname !== "/" ||
    hostUrl.search ||
    hostUrl.hash ||
    !isGoogleStorageHost(host)
  ) {
    throw new GmiRequestQueueError(errorCode, false);
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
    throw new GmiRequestQueueError(errorCode, false);
  }
  const segments = pathPrefix.slice(1, -1).split("/");
  if (segments.length === 0 || segments.some((segment) => !/^[A-Za-z0-9._~-]+$/.test(segment) || segment === "." || segment === "..")) {
    throw new GmiRequestQueueError(errorCode, false);
  }
  return Object.freeze({ host, pathPrefix });
}

function isGoogleStorageHost(host: string): boolean {
  return host === "storage.googleapis.com" || host.endsWith(".storage.googleapis.com");
}

function validateGoogleStorageUrl(
  value: string,
  policy: NormalizedStoragePolicy,
  errorCode: "invalid_request" | "invalid_response",
): string {
  if (typeof value !== "string" || !value || value.length > MAX_URL_CHARS || /[\u0000\r\n]/.test(value)) {
    throw new GmiRequestQueueError(errorCode, false);
  }
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new GmiRequestQueueError(errorCode, false);
  }
  if (
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
    throw new GmiRequestQueueError(errorCode, false);
  }
  return url.toString();
}

function encodeBoundedJson(value: GmiJsonObject): string {
  assertBoundedJsonObject(value, "invalid_request");
  let encoded: string;
  try {
    encoded = JSON.stringify(value);
  } catch {
    invalidRequest();
  }
  if (new TextEncoder().encode(encoded).byteLength > MAX_GMI_REQUEST_BYTES) invalidRequest();
  return encoded;
}

function assertBoundedJsonObject(
  value: unknown,
  errorCode: "invalid_request" | "invalid_response",
): asserts value is GmiJsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new GmiRequestQueueError(errorCode, false);
  }
  assertBoundedJsonValue(value, errorCode, 0, new WeakSet<object>());
}

function assertBoundedJsonValue(
  value: unknown,
  errorCode: "invalid_request" | "invalid_response",
  depth: number,
  ancestors: WeakSet<object>,
): asserts value is GmiJsonValue {
  if (depth > MAX_JSON_DEPTH) throw new GmiRequestQueueError(errorCode, false);
  if (value === null || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new GmiRequestQueueError(errorCode, false);
    return;
  }
  if (typeof value === "string") {
    if (value.length > MAX_JSON_STRING_CHARS) throw new GmiRequestQueueError(errorCode, false);
    return;
  }
  if (!value || typeof value !== "object") throw new GmiRequestQueueError(errorCode, false);
  if (ancestors.has(value)) throw new GmiRequestQueueError(errorCode, false);
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      if (value.length > MAX_JSON_ARRAY_ENTRIES) throw new GmiRequestQueueError(errorCode, false);
      for (const entry of value) assertBoundedJsonValue(entry, errorCode, depth + 1, ancestors);
      return;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new GmiRequestQueueError(errorCode, false);
    const entries = Object.entries(value);
    if (entries.length > MAX_JSON_OBJECT_ENTRIES) throw new GmiRequestQueueError(errorCode, false);
    for (const [key, entry] of entries) {
      if (key.length > MAX_JSON_KEY_CHARS) throw new GmiRequestQueueError(errorCode, false);
      assertBoundedJsonValue(entry, errorCode, depth + 1, ancestors);
    }
  } finally {
    ancestors.delete(value);
  }
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

async function readBoundedJson(response: Response, maxBytes: number): Promise<GmiJsonValue> {
  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  if (!contentType.includes("application/json")) {
    await cancelResponseBody(response);
    throw new GmiRequestQueueError("invalid_response", false, response.status);
  }
  const contentLength = response.headers.get("content-length");
  if (contentLength !== null) {
    if (!/^\d+$/.test(contentLength) || Number(contentLength) > maxBytes) {
      await cancelResponseBody(response);
      throw new GmiRequestQueueError("invalid_response", false, response.status);
    }
  }
  if (!response.body) throw new GmiRequestQueueError("invalid_response", false, response.status);

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.byteLength;
      if (size > maxBytes) {
        await reader.cancel().catch(() => undefined);
        throw new GmiRequestQueueError("invalid_response", false, response.status);
      }
      chunks.push(chunk.value);
    }
  } catch (error) {
    if (error instanceof GmiRequestQueueError) throw error;
    throw new GmiRequestQueueError("invalid_response", false, response.status);
  } finally {
    reader.releaseLock();
  }

  try {
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    const parsed: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    assertBoundedJsonValue(parsed, "invalid_response", 0, new WeakSet<object>());
    return freezeJson(parsed);
  } catch (error) {
    if (error instanceof GmiRequestQueueError) throw error;
    throw new GmiRequestQueueError("invalid_response", false, response.status);
  }
}

function parseAcceptedSubmission(value: GmiJsonValue): GmiAcceptedSubmission {
  const object = requiredJsonObject(value);
  const requestId = requiredProviderRequestId(object.request_id);
  const status = normalizeProviderStatus(object.status);
  return Object.freeze({ classification: "accepted", requestId, status });
}

function parseRequestObservation(value: GmiJsonValue, expectedRequestId: string): GmiRequestObservation {
  const response = requiredJsonObject(value);
  const requestId = requiredProviderRequestId(response.request_id);
  if (requestId !== expectedRequestId) throw new GmiRequestQueueError("invalid_response", false);
  const status = normalizeProviderStatus(response.status);
  return Object.freeze({ requestId, status, response });
}

function parseUploadUrl(value: GmiJsonValue, policy: NormalizedStoragePolicy): GmiUploadUrl {
  const object = requiredJsonObject(value);
  const uploadUrl = requiredString(object.upload_url);
  const publicUrl = requiredString(object.public_url);
  return Object.freeze({
    uploadUrl: validateGoogleStorageUrl(uploadUrl, policy, "invalid_response"),
    publicUrl: validateGoogleStorageUrl(publicUrl, policy, "invalid_response"),
  });
}

function requiredJsonObject(value: GmiJsonValue): GmiJsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new GmiRequestQueueError("invalid_response", false);
  }
  return value as GmiJsonObject;
}

function requiredProviderRequestId(value: GmiJsonValue | undefined): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$/.test(value)) {
    throw new GmiRequestQueueError("invalid_response", false);
  }
  return value;
}

function requiredString(value: GmiJsonValue | undefined): string {
  if (typeof value !== "string") throw new GmiRequestQueueError("invalid_response", false);
  return value;
}

function normalizeProviderStatus(value: GmiJsonValue | undefined): GmiRequestStatus {
  if (typeof value !== "string" || !value || value.length > 128 || /[\u0000\r\n]/.test(value)) {
    throw new GmiRequestQueueError("invalid_response", false);
  }
  switch (value.trim().toLowerCase()) {
    case "dispatched":
    case "queued":
      return "dispatched";
    case "processing":
      return "processing";
    case "success":
      return "success";
    case "failed":
      return "failed";
    case "cancelled":
    case "canceled":
      return "cancelled";
    default:
      return "unknown";
  }
}

function classifySubmitError(error: unknown): GmiAmbiguousSubmission {
  if (error instanceof GmiRequestQueueError) {
    if (error.code === "request_timeout") return ambiguousSubmission("timeout");
    if (error.code === "invalid_response") return ambiguousSubmission("malformed_response");
  }
  return ambiguousSubmission("transport_failure");
}

function ambiguousSubmission(
  reason: GmiAmbiguousSubmissionReason,
  httpStatus?: number,
): GmiAmbiguousSubmission {
  return Object.freeze({
    classification: "ambiguous",
    reason,
    ...(httpStatus === undefined ? {} : { httpStatus }),
  });
}

function freezeJson<T extends GmiJsonValue>(value: T): T {
  if (value && typeof value === "object") {
    for (const child of Array.isArray(value) ? value : Object.values(value)) freezeJson(child);
    Object.freeze(value);
  }
  return value;
}

function discardResponseBody(response: Response): void {
  void cancelResponseBody(response);
}

async function cancelResponseBody(response: Response): Promise<void> {
  await response.body?.cancel().catch(() => undefined);
}

function invalidRequest(): never {
  throw new GmiRequestQueueError("invalid_request", false);
}
