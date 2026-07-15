/**
 * Narrow, authenticated boundary between Air and Zap's private video service.
 *
 * This module deliberately does not know about iMessage, GMI, or Redis.  It
 * validates every value at the HTTP boundary, never logs a request or response,
 * and exposes only typed data for the durable job worker to persist encrypted.
 */

export const ZAP_AIR_SERVICE_PATH = "/api/services/air/v1";
export const MAX_ZAP_VIDEO_PROMPT_CHARS = 2_000;
export const MAX_ZAP_IMAGE_BYTES = 10 * 1024 * 1024;
export const MAX_ZAP_RESPONSE_BYTES = 64 * 1024;

export type ZapVideoServiceErrorCode =
  | "invalid_configuration"
  | "invalid_request"
  | "request_timeout"
  | "transport_failure"
  | "unexpected_status"
  | "invalid_response";

/**
 * Safe-to-log error shape.  It intentionally carries no URL, token, prompt,
 * response body, provider identifier, or native network error.
 */
export class ZapVideoServiceError extends Error {
  public constructor(
    readonly code: ZapVideoServiceErrorCode,
    readonly retryable: boolean,
    readonly status?: number,
  ) {
    super(`zap_video_service_${code}`);
    this.name = "ZapVideoServiceError";
  }
}

export interface ZapVideoServiceClientOptions {
  readonly serviceUrl: string;
  readonly serviceToken: string;
  readonly fetch?: typeof globalThis.fetch | undefined;
  readonly timeoutMs?: number | undefined;
}

export interface CreateZapImageUploadInput {
  readonly mimeType: "image/jpeg" | "image/png";
  readonly sizeBytes: number;
  /** Stable HMAC-derived key so image-ticket retries are safe to audit. */
  readonly idempotencyKey: string;
}

export interface ZapImageUpload {
  /** Presigned, short-lived HTTPS target. It is encrypted by the caller if retained. */
  readonly uploadUrl: string;
  /** Stable GMI-readable HTTPS URL supplied to the eventual Seedance run. */
  readonly publicUrl: string;
  /** Required request headers for the presigned direct upload. */
  readonly headers: Readonly<Record<string, string>>;
  /** Zap intentionally permits only a direct PUT; Air must not proxy image bytes. */
  readonly method: "PUT";
  /** Signed URL expiry, validated syntactically before it reaches a worker. */
  readonly expiresAt: string;
}

export interface CreateZapVideoRunInput {
  readonly prompt: string;
  readonly firstFrameUrl?: string | undefined;
  /** Stable opaque key derived from Air's inbound correlation id. */
  readonly idempotencyKey: string;
}

export type ZapVideoRunStatus =
  | "submission_unknown"
  | "queued"
  | "running"
  | "video_ready"
  | "retryable"
  | "dead_letter";

export interface ZapVideoFailure {
  readonly code?: string | undefined;
  readonly message: string;
}

export interface ZapCompletedVideo {
  readonly url: string;
  readonly mimeType: "video/mp4";
  readonly expiresAt?: string | undefined;
}

export interface ZapVideoRun {
  readonly runId: string;
  readonly status: ZapVideoRunStatus;
  readonly progress?: number | undefined;
  readonly error?: ZapVideoFailure | undefined;
  readonly video?: ZapCompletedVideo | undefined;
}

type FetchLike = typeof globalThis.fetch;

const DEFAULT_TIMEOUT_MS = 12_000;
const RUN_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$/;
const IDEMPOTENCY_KEY_PATTERN = /^[A-Za-z0-9._:-]{16,256}$/;
const SAFE_ERROR_CODE_PATTERN = /^[a-z0-9][a-z0-9_.:-]{0,127}$/i;

export class ZapVideoServiceClient {
  private readonly serviceOrigin: URL;
  private readonly serviceToken: string;
  private readonly fetch: FetchLike;
  private readonly timeoutMs: number;

  public constructor(options: ZapVideoServiceClientOptions) {
    this.serviceOrigin = parseServiceOrigin(options.serviceUrl);
    this.serviceToken = parseServiceToken(options.serviceToken);
    this.fetch = options.fetch ?? globalThis.fetch;
    if (typeof this.fetch !== "function") {
      throw new ZapVideoServiceError("invalid_configuration", false);
    }
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    if (!Number.isInteger(this.timeoutMs) || this.timeoutMs < 250 || this.timeoutMs > 60_000) {
      throw new ZapVideoServiceError("invalid_configuration", false);
    }
  }

  /** Reserve a GMI upload URL. Air uploads image bytes directly; it never relays them through Redis. */
  public async createUpload(input: CreateZapImageUploadInput): Promise<ZapImageUpload> {
    validateImageUploadInput(input);
    const response = await this.requestJson("/uploads", {
      method: "POST",
      body: {
        mimeType: input.mimeType,
        sizeBytes: input.sizeBytes,
      },
      idempotencyKey: input.idempotencyKey,
      expectedStatuses: [200, 201],
    });
    return parseZapImageUpload(response.body, input.mimeType);
  }

  /** Submit an idempotent Seedance request through Zap's service-owned provider queue. */
  public async createRun(input: CreateZapVideoRunInput): Promise<ZapVideoRun> {
    const prompt = normalizePrompt(input.prompt);
    const firstFrameUrl = input.firstFrameUrl === undefined
      ? undefined
      : normalizeGmiObjectUrl(input.firstFrameUrl, "invalid_request");
    validateIdempotencyKey(input.idempotencyKey);

    const response = await this.requestJson("/runs", {
      method: "POST",
      body: {
        prompt,
        ...(firstFrameUrl === undefined ? {} : { firstFrameUrl }),
      },
      idempotencyKey: input.idempotencyKey,
      expectedStatuses: [200, 202],
    });
    const parsed = parseZapVideoRun(response.body);
    // A fresh provider-side creation is always queued. A 200 means Zap replayed
    // the same opaque idempotency key and may report any later durable state.
    if (response.status === 202 && parsed.status !== "queued") invalidResponse();
    return parsed;
  }

  /** Read Zap's sanitized run projection. Caller decides retry/dead-letter policy. */
  public async getRun(runId: string): Promise<ZapVideoRun> {
    if (!RUN_ID_PATTERN.test(runId)) {
      throw new ZapVideoServiceError("invalid_request", false);
    }
    const response = await this.requestJson(`/runs/${encodeURIComponent(runId)}`, {
      method: "GET",
      expectedStatuses: [200],
    });
    const parsed = parseZapVideoRun(response.body);
    if (parsed.runId !== runId) {
      throw new ZapVideoServiceError("invalid_response", false);
    }
    return parsed;
  }

  private async requestJson(
    path: string,
    options: {
      readonly method: "GET" | "POST";
      readonly expectedStatuses: readonly number[];
      readonly body?: Record<string, unknown> | undefined;
      readonly idempotencyKey?: string | undefined;
    },
  ): Promise<{ readonly status: number; readonly body: unknown }> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    let response: Response;
    try {
      response = await this.fetch(new URL(`${ZAP_AIR_SERVICE_PATH}${path}`, this.serviceOrigin), {
        method: options.method,
        headers: {
          accept: "application/json",
          authorization: `Bearer ${this.serviceToken}`,
          ...(options.body === undefined ? {} : { "content-type": "application/json" }),
          ...(options.idempotencyKey === undefined ? {} : { "idempotency-key": options.idempotencyKey }),
        },
        ...(options.body === undefined ? {} : { body: JSON.stringify(options.body) }),
        signal: controller.signal,
      });
    } catch {
      if (controller.signal.aborted) {
        throw new ZapVideoServiceError("request_timeout", true);
      }
      throw new ZapVideoServiceError("transport_failure", true);
    } finally {
      clearTimeout(timeout);
    }

    if (!options.expectedStatuses.includes(response.status)) {
      throw new ZapVideoServiceError(
        "unexpected_status",
        response.status === 429 || response.status >= 500,
        response.status,
      );
    }
    return Object.freeze({ status: response.status, body: await readBoundedJson(response) });
  }
}

export function createZapVideoServiceClient(options: ZapVideoServiceClientOptions): ZapVideoServiceClient {
  return new ZapVideoServiceClient(options);
}

function parseServiceOrigin(value: string): URL {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new ZapVideoServiceError("invalid_configuration", false);
  }
  const localHttp = url.protocol === "http:" && isLoopbackHost(url.hostname);
  if (
    !(url.protocol === "https:" || localHttp) ||
    !url.hostname ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    (url.pathname !== "/" && url.pathname !== "")
  ) {
    throw new ZapVideoServiceError("invalid_configuration", false);
  }
  url.pathname = "/";
  return url;
}

function isLoopbackHost(hostname: string): boolean {
  const normalized = hostname.toLowerCase();
  // WHATWG URL retains brackets around an IPv6 hostname ("[::1]").
  return normalized === "localhost" || normalized === "127.0.0.1" || normalized === "::1" || normalized === "[::1]";
}

function parseServiceToken(value: string): string {
  if (typeof value !== "string" || /[\r\n]/.test(value)) {
    throw new ZapVideoServiceError("invalid_configuration", false);
  }
  const token = value?.trim();
  if (!token || token.length > 4_096) {
    throw new ZapVideoServiceError("invalid_configuration", false);
  }
  return token;
}

function validateImageUploadInput(input: CreateZapImageUploadInput): void {
  if (input.mimeType !== "image/jpeg" && input.mimeType !== "image/png") {
    throw new ZapVideoServiceError("invalid_request", false);
  }
  if (!Number.isInteger(input.sizeBytes) || input.sizeBytes < 1 || input.sizeBytes > MAX_ZAP_IMAGE_BYTES) {
    throw new ZapVideoServiceError("invalid_request", false);
  }
  validateIdempotencyKey(input.idempotencyKey);
}

function normalizePrompt(value: string): string {
  const prompt = value?.trim();
  if (!prompt || prompt.length > MAX_ZAP_VIDEO_PROMPT_CHARS || prompt.includes("\u0000")) {
    throw new ZapVideoServiceError("invalid_request", false);
  }
  return prompt;
}

function validateIdempotencyKey(value: string): void {
  if (!IDEMPOTENCY_KEY_PATTERN.test(value)) {
    throw new ZapVideoServiceError("invalid_request", false);
  }
}

function normalizeHttpsUrl(value: string, errorCode: ZapVideoServiceErrorCode): string {
  if (!value || value.length > 8_192) throw new ZapVideoServiceError(errorCode, false);
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new ZapVideoServiceError(errorCode, false);
  }
  if (url.protocol !== "https:" || !url.hostname || url.username || url.password || url.port) {
    throw new ZapVideoServiceError(errorCode, false);
  }
  return url.toString();
}

/** GMI documents Google Cloud Storage URLs for upload tickets and public frames. */
function normalizeGmiObjectUrl(value: string, errorCode: ZapVideoServiceErrorCode): string {
  const normalized = normalizeHttpsUrl(value, errorCode);
  const host = new URL(normalized).hostname.toLowerCase();
  if (host !== "storage.googleapis.com" && !host.endsWith(".storage.googleapis.com")) {
    throw new ZapVideoServiceError(errorCode, false);
  }
  return normalized;
}

/** Completed media must be the Zap-owned Vercel Blob object, never provider output. */
function normalizeBlobUrl(value: string, errorCode: ZapVideoServiceErrorCode): string {
  const normalized = normalizeHttpsUrl(value, errorCode);
  const host = new URL(normalized).hostname.toLowerCase();
  if (host !== "blob.vercel-storage.com" && !host.endsWith(".blob.vercel-storage.com")) {
    throw new ZapVideoServiceError(errorCode, false);
  }
  return normalized;
}

async function readBoundedJson(response: Response): Promise<unknown> {
  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  if (!contentType.includes("application/json")) {
    throw new ZapVideoServiceError("invalid_response", false, response.status);
  }
  if (!response.body) throw new ZapVideoServiceError("invalid_response", false, response.status);

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.byteLength;
      if (size > MAX_ZAP_RESPONSE_BYTES) {
        await reader.cancel().catch(() => undefined);
        throw new ZapVideoServiceError("invalid_response", false, response.status);
      }
      chunks.push(chunk.value);
    }
  } catch (error) {
    if (error instanceof ZapVideoServiceError) throw error;
    throw new ZapVideoServiceError("invalid_response", false, response.status);
  } finally {
    reader.releaseLock();
  }

  let text: string;
  try {
    const body = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    text = new TextDecoder("utf-8", { fatal: true }).decode(body);
    return JSON.parse(text) as unknown;
  } catch {
    throw new ZapVideoServiceError("invalid_response", false, response.status);
  }
}

function parseZapImageUpload(
  value: unknown,
  expectedMimeType: CreateZapImageUploadInput["mimeType"],
): ZapImageUpload {
  const object = objectRecord(value);
  const uploadUrl = requiredGmiObjectUrl(object.uploadUrl);
  const publicUrl = requiredGmiObjectUrl(object.publicUrl);
  const headers = parseUploadHeaders(object.headers);
  if (headers["content-type"]?.toLowerCase() !== expectedMimeType) invalidResponse();
  if (object.method !== "PUT") invalidResponse();
  const expiresAt = parseExpiresAt(object.expiresAt);
  return Object.freeze({
    uploadUrl,
    publicUrl,
    headers,
    method: "PUT",
    expiresAt,
  });
}

function parseZapVideoRun(value: unknown): ZapVideoRun {
  const object = objectRecord(value);
  const runId = requiredRunId(object.runId);
  const status = object.status;
  if (
    status !== "submission_unknown" &&
    status !== "queued" &&
    status !== "running" &&
    status !== "video_ready" &&
    status !== "retryable" &&
    status !== "dead_letter"
  ) {
    invalidResponse();
  }
  const progress = object.progress === undefined ? undefined : boundedProgress(object.progress);
  const error = object.error === undefined ? undefined : parseZapFailure(object.error);
  const video = object.video === undefined ? undefined : parseCompletedVideo(object.video);
  if (status === "video_ready" && video === undefined) invalidResponse();
  if (status !== "video_ready" && video !== undefined) invalidResponse();

  return Object.freeze({
    runId,
    status,
    ...(progress === undefined ? {} : { progress }),
    ...(error === undefined ? {} : { error }),
    ...(video === undefined ? {} : { video }),
  });
}

function parseUploadHeaders(value: unknown): Readonly<Record<string, string>> {
  const object = objectRecord(value);
  const entries: Array<readonly [string, string]> = [];
  const seen = new Set<string>();
  for (const [key, headerValue] of Object.entries(object)) {
    if (!/^[A-Za-z0-9-]{1,128}$/.test(key) || typeof headerValue !== "string" || !headerValue || headerValue.length > 4_096 || /[\r\n]/.test(headerValue)) {
      invalidResponse();
    }
    const normalizedKey = key.toLowerCase();
    // HTTP field names are case-insensitive; accepting duplicate spellings
    // would silently change a presigned upload's signed-header semantics.
    if (seen.has(normalizedKey) || entries.length >= 64) invalidResponse();
    seen.add(normalizedKey);
    entries.push([normalizedKey, headerValue]);
  }
  if (entries.length === 0) invalidResponse();
  return Object.freeze(Object.fromEntries(entries));
}

function parseZapFailure(value: unknown): ZapVideoFailure {
  const object = objectRecord(value);
  if (typeof object.message !== "string" || !object.message.trim() || object.message.length > 512 || /[\u0000\r\n]/.test(object.message)) {
    invalidResponse();
  }
  const code = object.code;
  if (code !== undefined && (typeof code !== "string" || !SAFE_ERROR_CODE_PATTERN.test(code))) {
    invalidResponse();
  }
  return Object.freeze({
    message: object.message.trim(),
    ...(code === undefined ? {} : { code }),
  });
}

function parseCompletedVideo(value: unknown): ZapCompletedVideo {
  const object = objectRecord(value);
  const url = requiredBlobUrl(object.url);
  if (object.mimeType !== "video/mp4") invalidResponse();
  const expiresAt = object.expiresAt === undefined ? undefined : parseExpiresAt(object.expiresAt);
  return Object.freeze({
    url,
    mimeType: "video/mp4",
    ...(expiresAt === undefined ? {} : { expiresAt }),
  });
}

function parseExpiresAt(value: unknown): string {
  if (typeof value !== "string" || value.length > 128 || !Number.isFinite(Date.parse(value))) {
    invalidResponse();
  }
  return value;
}

function boundedProgress(value: unknown): number {
  if (!Number.isInteger(value) || (value as number) < 0 || (value as number) > 100) invalidResponse();
  return value as number;
}

function requiredHttpsString(value: unknown): string {
  if (typeof value !== "string") invalidResponse();
  return normalizeHttpsUrl(value as string, "invalid_response");
}

function requiredGmiObjectUrl(value: unknown): string {
  if (typeof value !== "string") invalidResponse();
  return normalizeGmiObjectUrl(value as string, "invalid_response");
}

function requiredBlobUrl(value: unknown): string {
  if (typeof value !== "string") invalidResponse();
  return normalizeBlobUrl(value as string, "invalid_response");
}

function requiredRunId(value: unknown): string {
  if (typeof value !== "string" || !RUN_ID_PATTERN.test(value)) invalidResponse();
  return value as string;
}

function objectRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) invalidResponse();
  return value as Record<string, unknown>;
}

function invalidResponse(): never {
  throw new ZapVideoServiceError("invalid_response", false);
}
