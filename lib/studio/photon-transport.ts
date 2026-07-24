import { createClient } from "@photon-ai/advanced-imessage";
import { cloud } from "@spectrum-ts/core";

import {
  copyAndRevalidateStudioProviderArtifactForUpload,
  isStudioValidatedProviderArtifact,
  type StudioPreparedProviderArtifactUpload,
  type StudioValidatedProviderArtifact,
} from "./artifact-validation.js";
import type { StudioAttachmentReference, StudioPreparedOutput } from "./store.js";
import {
  isSafeTransportText,
  type IMessageThreadReference,
  type StudioPreparedOutputTransport,
  type StudioPrepareOutputOutcome,
  type StudioIMessageTransport,
  type StudioTransportOperationOptions,
  type TransportOutcome,
} from "./imessage-transport.js";
import { assertNoPhotonTelemetryExporters } from "./photon-telemetry.js";

const PHOTON_SHARED_LINE = "shared";
const PHOTON_SHARED_ADDRESS = "imessage.spectrum.photon.codes:443";
const TOKEN_EXPIRY_SAFETY_WINDOW_MS = 30_000;
/**
 * Keep every unary Photon RPC well inside Air's 90-second durable lease. A
 * timeout is intentionally still classified as an unknown physical outcome;
 * it only prevents a stalled SDK call from holding the worker indefinitely.
 */
const PHOTON_UNARY_RPC_TIMEOUT_MS = 20_000;
/** Token minting is a pre-send prerequisite and must never consume a Cron. */
export const PHOTON_TOKEN_ISSUE_TIMEOUT_MS = 10_000;
/** Typing is cosmetic, so its own RPC never gets a full message-send window. */
const PHOTON_TYPING_RPC_TIMEOUT_MS = 2_000;
const PHOTON_LIKE_TAPBACK = Object.freeze({ kind: "like" as const });

type PhotonTokenData = Awaited<ReturnType<typeof cloud.issueImessageTokens>>;
export type PhotonTokenIssuer = (projectId: string, projectSecret: string) => Promise<PhotonTokenData>;

interface PhotonReplyOptions {
  /** Native iMessage reply target, carried only from a validated inbound GUID. */
  readonly replyTo?: string;
}

interface PhotonTextSendOptions extends PhotonReplyOptions {
  readonly clientMessageId: string;
  readonly enableDataDetection: false;
  readonly enableLinkPreview: false;
}

interface PhotonAttachmentSendOptions extends PhotonReplyOptions {
  readonly clientMessageId: string;
}

interface PhotonReactionSendOptions {
  readonly clientMessageId: string;
}

/**
 * A deliberately tiny, injected provider boundary. Unlike Spectrum's
 * high-level `space.send`, this accepts Air's stable outbox key as
 * `clientMessageId`, so a later operator-approved replay cannot create a
 * second physical iMessage for the same durable intent.
 */
export interface PhotonIMessageClient {
  readonly chats: {
    setTyping(chatGuid: string, isTyping: boolean): Promise<void>;
  };
  readonly attachments: {
    upload(input: { readonly data: Uint8Array; readonly fileName: string }): Promise<{
      readonly attachment: { readonly guid: string };
    }>;
  };
  readonly messages: {
    sendText(
      chatGuid: string,
      text: string,
      options: PhotonTextSendOptions,
    ): Promise<unknown>;
    sendAttachment(
      chatGuid: string,
      attachmentGuid: string,
      options: PhotonAttachmentSendOptions,
    ): Promise<unknown>;
    setReaction(
      chatGuid: string,
      messageId: string,
      reaction: typeof PHOTON_LIKE_TAPBACK,
      isSet: true,
      options: PhotonReactionSendOptions,
    ): Promise<unknown>;
  };
}

export interface PhotonIMessagePort {
  /**
   * The caller always supplies the encrypted inbound sending line. Shared
   * Cloud tokens intentionally collapse that value to their one pooled client;
   * dedicated tokens retain the exact per-line lookup.
   */
  getClient(sendingLine: string, options?: StudioTransportOperationOptions): Promise<PhotonIMessageClient>;
}

export interface PhotonIMessageTransportOptions {
  readonly projectId: string;
  readonly projectSecret: string;
  /** Required even though the outbound client does not consume it directly. */
  readonly webhookSecret: string;
  /** Dependency injection keeps transport tests offline and deterministic. */
  readonly port: PhotonIMessagePort;
}

export interface CreatePhotonIMessageTransportOptions {
  readonly projectId: string;
  readonly projectSecret: string;
  readonly webhookSecret: string;
  readonly port?: PhotonIMessagePort | undefined;
  /** Limits first-use token minting to the current Cron invocation. */
  readonly deadlineAtMs?: number | undefined;
  /** Test-only injection; production uses Spectrum Cloud's reviewed binding. */
  readonly issueImessageTokens?: PhotonTokenIssuer | undefined;
  /** Test-only narrower bound; production uses `PHOTON_TOKEN_ISSUE_TIMEOUT_MS`. */
  readonly tokenIssueTimeoutMs?: number | undefined;
}

/**
 * Establishes the direct Photon/Spectrum Cloud session without subscribing to
 * inbox events. Inbound processing remains exclusively in Air's signed
 * webhook path. A verified transport means credentials and a sending line
 * were minted successfully; it does not send a message during construction.
 */
export async function createPhotonIMessageTransport(
  options: CreatePhotonIMessageTransportOptions,
): Promise<PhotonIMessageTransport> {
  assertRequiredCredential(options.projectId, "IMESSAGE_PROJECT_ID");
  assertRequiredCredential(options.projectSecret, "IMESSAGE_PROJECT_SECRET");
  assertRequiredCredential(options.webhookSecret, "IMESSAGE_WEBHOOK_SECRET");
  assertNoPhotonTelemetryExporters();
  const port = options.port ?? await CloudIMessagePort.create(options.projectId, options.projectSecret, {
    ...(options.deadlineAtMs === undefined ? {} : { deadlineAtMs: options.deadlineAtMs }),
    ...(options.issueImessageTokens === undefined ? {} : { issueImessageTokens: options.issueImessageTokens }),
    ...(options.tokenIssueTimeoutMs === undefined ? {} : { tokenIssueTimeoutMs: options.tokenIssueTimeoutMs }),
  });
  return new PhotonIMessageTransport({
    projectId: options.projectId,
    projectSecret: options.projectSecret,
    webhookSecret: options.webhookSecret,
    port,
  });
}

/**
 * Direct Cloud iMessage delivery. Air only accepts an explicitly validated
 * provider artifact at the output boundary; an inbound attachment reference
 * is a locator, never an outbound byte payload. Inbound media fetch remains
 * unavailable until it has its own reviewed retrieval path.
 */
export class PhotonIMessageTransport implements StudioIMessageTransport, StudioPreparedOutputTransport {
  public readonly verified = true;
  public readonly preparedOutputTransport = "photon" as const;
  private readonly port: PhotonIMessagePort;

  public constructor(options: PhotonIMessageTransportOptions) {
    assertRequiredCredential(options.projectId, "IMESSAGE_PROJECT_ID");
    assertRequiredCredential(options.projectSecret, "IMESSAGE_PROJECT_SECRET");
    assertRequiredCredential(options.webhookSecret, "IMESSAGE_WEBHOOK_SECRET");
    this.port = options.port;
  }

  public async showTyping(thread: IMessageThreadReference, options?: StudioTransportOperationOptions): Promise<void> {
    const client = await this.clientFor(thread, options);
    await runPhotonOperation(
      () => client.chats.setTyping(thread.chatGuid, true),
      PHOTON_TYPING_RPC_TIMEOUT_MS,
      options,
      true,
    );
  }

  public async stopTyping(thread: IMessageThreadReference, options?: StudioTransportOperationOptions): Promise<void> {
    const client = await this.clientFor(thread, options);
    await runPhotonOperation(
      () => client.chats.setTyping(thread.chatGuid, false),
      PHOTON_TYPING_RPC_TIMEOUT_MS,
      options,
      true,
    );
  }

  public async sendText(
    thread: IMessageThreadReference,
    text: string,
    idempotencyKey: string,
    options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome> {
    if (!isSafeTransportText(text)) {
      return Object.freeze({ kind: "failed", code: "invalid_text_payload" });
    }
    if (!isSafeThreadReference(thread)) {
      return Object.freeze({ kind: "failed", code: "invalid_thread_reference" });
    }
    if (!isSafeIdempotencyKey(idempotencyKey)) {
      return Object.freeze({ kind: "failed", code: "invalid_idempotency_key" });
    }

    let client: PhotonIMessageClient;
    try {
      client = await clientForPort(this.port, thread.sendingLine, options);
    } catch {
      // Client resolution/token minting happens before a physical message RPC,
      // so this is safely retryable by the durable outbox.
      return Object.freeze({ kind: "retryable_failure", code: "photon_client_unavailable" });
    }

    try {
      await runPhotonOperation(
        () => client.messages.sendText(thread.chatGuid, text, {
          clientMessageId: idempotencyKey,
          // Text-only onboarding/help must not trigger link previews or
          // data-detector enrichment outside the signed Air boundary.
          enableDataDetection: false,
          enableLinkPreview: false,
          ...replyOptionsFor(thread),
        }),
        PHOTON_UNARY_RPC_TIMEOUT_MS,
        options,
        true,
      );
      return Object.freeze({ kind: "sent" });
    } catch (error) {
      if (error instanceof PhotonPreflightDeadlineExceeded) {
        return Object.freeze({ kind: "retryable_failure", code: "photon_send_deadline_exhausted" });
      }
      // The RPC may have reached Photon. Air deliberately does not replay it
      // automatically; the persisted clientMessageId is retained for a
      // reviewed reconciliation path if an operator later needs one.
      return Object.freeze({ kind: "send_unknown", code: "photon_text_send_unknown" });
    }
  }

  /**
   * Uploads an Air-validated provider output then sends its Photon GUID.
   *
   * The broad parameter keeps this class structurally compatible with the
   * legacy transport interface while rejecting a `StudioAttachmentReference`
   * at runtime. That reference identifies an inbound attachment and has no
   * validated bytes, filename, or one-based provider output position.
   */
  public async sendAttachment(
    thread: IMessageThreadReference,
    attachment: StudioAttachmentReference | StudioValidatedProviderArtifact,
    idempotencyKey: string,
    options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome> {
    if (!isSafeThreadReference(thread)) {
      return Object.freeze({ kind: "failed", code: "invalid_thread_reference" });
    }
    if (!isSafeIdempotencyKey(idempotencyKey)) {
      return Object.freeze({ kind: "failed", code: "invalid_idempotency_key" });
    }
    if (!isStudioValidatedProviderArtifact(attachment)) {
      return Object.freeze({ kind: "failed", code: "invalid_outbound_artifact" });
    }

    const prepared = await this.prepareValidatedProviderArtifact(thread, attachment, options);
    if (prepared.kind !== "prepared") return prepared;

    const sent = await this.sendPreparedOutput(thread, prepared.output, idempotencyKey, options);
    if (sent.kind !== "retryable_failure") return sent;

    // The legacy method has nowhere durable to keep the newly uploaded GUID.
    // Reporting retryable here would cause its old outbox caller to invoke
    // this whole method again and upload a duplicate remote attachment. The
    // split capability above can safely retry this exact send after it has
    // persisted `prepared.output`; the compatibility bridge must fail closed.
    return Object.freeze({ kind: "send_unknown", code: "photon_attachment_send_unknown" });
  }

  /**
   * Performs only the Photon upload half of direct output delivery. The
   * resulting small GUID record is safe for a later encrypted durable outbox
   * record; it deliberately cannot contain provider bytes, URLs, or names.
   */
  public async prepareValidatedProviderArtifact(
    thread: IMessageThreadReference,
    attachment: StudioValidatedProviderArtifact,
    options?: StudioTransportOperationOptions,
  ): Promise<StudioPrepareOutputOutcome> {
    if (!isSafeThreadReference(thread)) {
      return Object.freeze({ kind: "failed", code: "invalid_thread_reference" });
    }
    if (!isStudioValidatedProviderArtifact(attachment)) {
      return Object.freeze({ kind: "failed", code: "invalid_outbound_artifact" });
    }

    let uploadArtifact: StudioPreparedProviderArtifactUpload;
    try {
      // This is the last byte boundary before Photon. It rejects forged
      // structures and uses a fresh revalidated copy rather than the public,
      // mutable `attachment.bytes` property.
      uploadArtifact = copyAndRevalidateStudioProviderArtifactForUpload(attachment);
    } catch {
      return Object.freeze({ kind: "failed", code: "invalid_outbound_artifact" });
    }

    let client: PhotonIMessageClient;
    try {
      client = await clientForPort(this.port, thread.sendingLine, options);
    } catch {
      return Object.freeze({ kind: "retryable_failure", code: "photon_client_unavailable" });
    }

    let attachmentGuid: string;
    try {
      const uploaded = await runPhotonOperation(
        () => client.attachments.upload({
          data: uploadArtifact.bytes,
          fileName: uploadArtifact.filename,
        }),
        PHOTON_UNARY_RPC_TIMEOUT_MS,
        options,
        true,
      );
      attachmentGuid = uploaded.attachment.guid;
    } catch (error) {
      if (error instanceof PhotonPreflightDeadlineExceeded) {
        return Object.freeze({ kind: "retryable_failure", code: "photon_upload_deadline_exhausted" });
      }
      // The Photon upload API has no clientMessageId option. It may have
      // persisted bytes even when the caller observes an error, so retries
      // would create a second remote artifact and are never automatic.
      return Object.freeze({ kind: "send_unknown", code: "photon_attachment_upload_unknown" });
    }

    if (!isSafePhotonOpaqueId(attachmentGuid)) {
      // Treat an unusable upload response as ambiguous too: the upload may
      // have succeeded, but Air cannot safely create a second copy to retry.
      return Object.freeze({ kind: "send_unknown", code: "photon_attachment_upload_unknown" });
    }

    return Object.freeze({
      kind: "prepared",
      output: Object.freeze({ transport: "photon" as const, attachmentGuid }),
    });
  }

  /**
   * Performs only the physical iMessage send half of output delivery. It
   * accepts no provider artifact or byte payload, and it repeats GUID
   * validation after any durable rehydration before handing it to Photon.
   */
  public async sendPreparedOutput(
    thread: IMessageThreadReference,
    output: StudioPreparedOutput,
    idempotencyKey: string,
    options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome> {
    if (!isSafeThreadReference(thread)) {
      return Object.freeze({ kind: "failed", code: "invalid_thread_reference" });
    }
    if (!isSafeIdempotencyKey(idempotencyKey)) {
      return Object.freeze({ kind: "failed", code: "invalid_idempotency_key" });
    }
    if (!isSafePreparedPhotonOutput(output)) {
      return Object.freeze({ kind: "failed", code: "invalid_prepared_photon_output" });
    }

    let client: PhotonIMessageClient;
    try {
      client = await clientForPort(this.port, thread.sendingLine, options);
    } catch {
      return Object.freeze({ kind: "retryable_failure", code: "photon_client_unavailable" });
    }

    try {
      await runPhotonOperation(
        () => client.messages.sendAttachment(thread.chatGuid, output.attachmentGuid, {
          clientMessageId: idempotencyKey,
          ...replyOptionsFor(thread),
        }),
        PHOTON_UNARY_RPC_TIMEOUT_MS,
        options,
        true,
      );
      return Object.freeze({ kind: "sent" });
    } catch (error) {
      if (error instanceof PhotonPreflightDeadlineExceeded) {
        return Object.freeze({ kind: "retryable_failure", code: "photon_send_deadline_exhausted" });
      }
      // This send may have reached Photon. Keep the durable clientMessageId
      // for explicit reconciliation instead of re-uploading or replaying.
      return Object.freeze({ kind: "send_unknown", code: "photon_attachment_send_unknown" });
    }
  }

  /** Adds the semantic iMessage thumbs-up tapback to one validated message. */
  public async addLikeTapback(
    thread: IMessageThreadReference,
    messageId: string,
    idempotencyKey: string,
    options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome> {
    if (!isSafeThreadReference(thread)) {
      return Object.freeze({ kind: "failed", code: "invalid_thread_reference" });
    }
    if (!isSafePhotonOpaqueId(messageId)) {
      return Object.freeze({ kind: "failed", code: "invalid_message_reference" });
    }
    if (!isSafeIdempotencyKey(idempotencyKey)) {
      return Object.freeze({ kind: "failed", code: "invalid_idempotency_key" });
    }

    let client: PhotonIMessageClient;
    try {
      client = await clientForPort(this.port, thread.sendingLine, options);
    } catch {
      return Object.freeze({ kind: "retryable_failure", code: "photon_client_unavailable" });
    }

    try {
      await runPhotonOperation(
        () => client.messages.setReaction(
          thread.chatGuid,
          messageId,
          PHOTON_LIKE_TAPBACK,
          true,
          { clientMessageId: idempotencyKey },
        ),
        PHOTON_UNARY_RPC_TIMEOUT_MS,
        options,
        true,
      );
      return Object.freeze({ kind: "sent" });
    } catch (error) {
      if (error instanceof PhotonPreflightDeadlineExceeded) {
        return Object.freeze({ kind: "retryable_failure", code: "photon_tapback_deadline_exhausted" });
      }
      return Object.freeze({ kind: "send_unknown", code: "photon_like_tapback_unknown" });
    }
  }

  /** Inbound attachment bytes stay unavailable until media validation is wired. */
  public async fetchAttachment(_thread: IMessageThreadReference, _attachmentId: string): Promise<Uint8Array> {
    throw new Error("attachment_transport_not_enabled");
  }

  private async clientFor(
    thread: IMessageThreadReference,
    options?: StudioTransportOperationOptions,
  ): Promise<PhotonIMessageClient> {
    if (!isSafeThreadReference(thread)) throw new Error("invalid_thread_reference");
    return await clientForPort(this.port, thread.sendingLine, options);
  }
}

/**
 * Cloud token renewal is request-driven rather than timer-driven so Vercel
 * functions do not retain a background resource. Each direct iMessage RPC
 * still uses Photon-issued credentials; all automatic RPC retry is disabled.
 */
class CloudIMessagePort implements PhotonIMessagePort {
  private tokenData: PhotonTokenData;
  private expiresAtMs: number;
  private readonly clientsByInstance = new Map<string, PhotonIMessageClient>();
  private clientsByLine = new Map<string, PhotonIMessageClient>();
  private refreshInFlight: Promise<void> | undefined;

  private constructor(
    private readonly projectId: string,
    private readonly projectSecret: string,
    private readonly issueImessageTokens: PhotonTokenIssuer,
    private readonly tokenIssueTimeoutMs: number,
    tokenData: PhotonTokenData,
  ) {
    this.tokenData = tokenData;
    this.expiresAtMs = expiryFromToken(tokenData);
    this.syncClients(tokenData);
  }

  public static async create(
    projectId: string,
    projectSecret: string,
    options: {
      readonly deadlineAtMs?: number | undefined;
      readonly issueImessageTokens?: PhotonTokenIssuer | undefined;
      readonly tokenIssueTimeoutMs?: number | undefined;
    } = {},
  ): Promise<CloudIMessagePort> {
    const issueImessageTokens = options.issueImessageTokens ?? cloud.issueImessageTokens;
    const tokenIssueTimeoutMs = normalizeTokenIssueTimeout(options.tokenIssueTimeoutMs);
    const tokenData = await issuePhotonTokens(
      issueImessageTokens,
      projectId,
      projectSecret,
      tokenIssueTimeoutMs,
      options,
    );
    return new CloudIMessagePort(projectId, projectSecret, issueImessageTokens, tokenIssueTimeoutMs, tokenData);
  }

  public async getClient(
    sendingLine: string,
    options?: StudioTransportOperationOptions,
  ): Promise<PhotonIMessageClient> {
    // Refresh and validate token lifetime before the SDK is handed a client.
    // The SDK invokes its token callback after its outbound RPC has been
    // admitted, so that callback must remain a read-only lookup: a late token
    // mint there would outlive the caller's durable deadline and make the
    // physical-send boundary ambiguous.
    await this.ensureFreshTokenForOutboundRpc(options);
    // A shared Cloud project has no dedicated sending-line token. The signed
    // webhook still gives Air the real line for encrypted scope/audit data,
    // but every shared inbound line must use the single pooled client. A
    // dedicated project remains exact-line-only so multi-line delivery cannot
    // silently cross a caller's recorded route.
    const client = this.clientsByLine.get(resolvePhotonIMessageClientRoute(this.tokenData.type, sendingLine));
    if (!client) throw new Error("photon_sending_line_unavailable");
    return client;
  }

  private async tokenFor(instanceId: string): Promise<string> {
    // This callback runs inside the SDK's outbound-RPC path. It must never
    // initiate network work: getClient has already minted a sufficiently
    // fresh token under the caller-owned deadline before that RPC is admitted.
    const current = this.tokenData;
    if (instanceId === PHOTON_SHARED_LINE) {
      if (current.type !== "shared") throw new Error("photon_client_mode_changed");
      return current.token;
    }
    if (current.type !== "dedicated") throw new Error("photon_client_mode_changed");
    const token = current.auth[instanceId];
    if (!token) throw new Error("photon_instance_token_unavailable");
    return token;
  }

  private async ensureFreshTokenForOutboundRpc(options?: StudioTransportOperationOptions): Promise<void> {
    await this.refreshIfNeeded(options);
    // A provider response with an unexpectedly short TTL must not be deferred
    // to the SDK callback. Reject it before a message/upload RPC begins so the
    // durable caller can retry safely with its own deadline.
    if (!this.hasSufficientTokenLifetime()) throw new Error("photon_token_ttl_too_short");
  }

  private async refreshIfNeeded(options?: StudioTransportOperationOptions): Promise<void> {
    if (this.hasSufficientTokenLifetime()) return;
    if (!this.refreshInFlight) {
      this.refreshInFlight = this.refresh(options).finally(() => {
        this.refreshInFlight = undefined;
      });
    }
    await this.refreshInFlight;
  }

  private hasSufficientTokenLifetime(): boolean {
    return Date.now() < this.expiresAtMs - TOKEN_EXPIRY_SAFETY_WINDOW_MS;
  }

  private async refresh(options?: StudioTransportOperationOptions): Promise<void> {
    const tokenData = await issuePhotonTokens(
      this.issueImessageTokens,
      this.projectId,
      this.projectSecret,
      this.tokenIssueTimeoutMs,
      options,
    );
    this.tokenData = tokenData;
    this.expiresAtMs = expiryFromToken(tokenData);
    this.syncClients(tokenData);
  }

  private syncClients(tokenData: PhotonTokenData): void {
    const nextByLine = new Map<string, PhotonIMessageClient>();
    if (tokenData.type === "shared") {
      nextByLine.set(PHOTON_SHARED_LINE, this.clientForInstance(PHOTON_SHARED_LINE, PHOTON_SHARED_ADDRESS));
      this.clientsByLine = nextByLine;
      return;
    }

    for (const instanceId of Object.keys(tokenData.auth)) {
      const line = tokenData.numbers[instanceId];
      if (!line) throw new Error("photon_instance_line_unavailable");
      nextByLine.set(line, this.clientForInstance(instanceId, `${instanceId}.imsg.photon.codes:443`));
    }
    this.clientsByLine = nextByLine;
  }

  private clientForInstance(instanceId: string, address: string): PhotonIMessageClient {
    const existing = this.clientsByInstance.get(instanceId);
    if (existing) return existing;
    // Keep the narrow Air facade structurally checked against the installed
    // Photon SDK. An unchecked cast here could silently drop the durable
    // `clientMessageId` contract after a dependency update.
    const client: PhotonIMessageClient = createClient({
      address,
      autoIdempotency: false,
      retry: false,
      tls: true,
      timeout: PHOTON_UNARY_RPC_TIMEOUT_MS,
      token: async () => this.tokenFor(instanceId),
    });
    this.clientsByInstance.set(instanceId, client);
    return client;
  }
}

/**
 * Resolving a client is a pre-send boundary.  When the remaining invocation
 * window expires here, no iMessage RPC has begun and the durable outbox can
 * safely retry it on the next Cron.
 */
async function clientForPort(
  port: PhotonIMessagePort,
  sendingLine: string,
  options?: StudioTransportOperationOptions,
): Promise<PhotonIMessageClient> {
  return options === undefined
    ? await port.getClient(sendingLine)
    : await port.getClient(sendingLine, options);
}

/** A timeout after the supplied operation starts is intentionally ambiguous. */
async function runPhotonOperation<T>(
  operation: () => Promise<T>,
  maximumTimeoutMs: number,
  options?: StudioTransportOperationOptions,
  requirePhysicalActionMargin = false,
): Promise<T> {
  const timeoutMs = boundedOperationTimeout(maximumTimeoutMs, options, requirePhysicalActionMargin);
  if (timeoutMs === null) throw new PhotonPreflightDeadlineExceeded();

  return await new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new PhotonOperationDeadlineExceeded()), timeoutMs);
    let operationPromise: Promise<T>;
    try {
      operationPromise = Promise.resolve().then(operation);
    } catch (error) {
      clearTimeout(timeout);
      reject(error);
      return;
    }
    void operationPromise.then(
      (value) => {
        clearTimeout(timeout);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timeout);
        reject(error);
      },
    );
  });
}

async function issuePhotonTokens(
  issueImessageTokens: PhotonTokenIssuer,
  projectId: string,
  projectSecret: string,
  tokenIssueTimeoutMs: number,
  options?: StudioTransportOperationOptions,
): Promise<PhotonTokenData> {
  return await runPhotonOperation(
    () => issueImessageTokens(projectId, projectSecret),
    tokenIssueTimeoutMs,
    options,
  );
}

function boundedOperationTimeout(
  maximumTimeoutMs: number,
  options?: StudioTransportOperationOptions,
  requirePhysicalActionMargin = false,
): number | null {
  if (!Number.isSafeInteger(maximumTimeoutMs) || maximumTimeoutMs < 1) return null;
  if (options?.deadlineAtMs === undefined) return maximumTimeoutMs;
  if (!Number.isSafeInteger(options.deadlineAtMs)) return null;
  const remaining = options.deadlineAtMs - Date.now();
  if (!Number.isSafeInteger(remaining) || remaining < 1) return null;
  if (requirePhysicalActionMargin) {
    const minimum = options.minimumPhysicalActionRemainingMs;
    if (minimum !== undefined && (!Number.isSafeInteger(minimum) || minimum < 1 || remaining < minimum)) return null;
  }
  return Math.min(maximumTimeoutMs, remaining);
}

function normalizeTokenIssueTimeout(value: number | undefined): number {
  if (value === undefined) return PHOTON_TOKEN_ISSUE_TIMEOUT_MS;
  if (!Number.isSafeInteger(value) || value < 1 || value > PHOTON_TOKEN_ISSUE_TIMEOUT_MS) {
    throw new Error("invalid_photon_token_issue_timeout");
  }
  return value;
}

class PhotonPreflightDeadlineExceeded extends Error {
  public constructor() {
    super("photon_preflight_deadline_exceeded");
  }
}

class PhotonOperationDeadlineExceeded extends Error {
  public constructor() {
    super("photon_operation_deadline_exceeded");
  }
}

function expiryFromToken(tokenData: PhotonTokenData): number {
  if (!Number.isFinite(tokenData.expiresIn) || tokenData.expiresIn <= 0 || tokenData.expiresIn > 86_400) {
    throw new Error("photon_token_expiry_invalid");
  }
  return Date.now() + tokenData.expiresIn * 1_000;
}

/**
 * A shared Photon token represents exactly one pooled sending capability, not
 * a phone-number-specific credential. Keep the original inbound line in the
 * encrypted Air thread reference, while selecting the pooled client only at
 * the direct SDK boundary. Dedicated client selection remains strict.
 */
export function resolvePhotonIMessageClientRoute(
  tokenMode: "shared" | "dedicated",
  sendingLine: string,
): string {
  if (!isSafeOpaqueValue(sendingLine, 320)) throw new Error("invalid_photon_sending_line");
  return tokenMode === "shared" ? PHOTON_SHARED_LINE : sendingLine;
}

function isSafeThreadReference(thread: IMessageThreadReference): boolean {
  // The direct Photon SDK accepts only the structured chat GUID it issues
  // (`any;-;…` for direct conversations or `any;+;…` for groups). Air stores
  // an inbound GUID verbatim for auditability, but must fail before its send
  // RPC rather than let an unrecognized legacy/provider-shaped value become
  // an ambiguous physical-delivery failure.
  return isSafePhotonChatGuid(thread.chatGuid) &&
    isSafeOpaqueValue(thread.sendingLine, 320) &&
    (thread.replyTargetId === undefined || isSafePhotonOpaqueId(thread.replyTargetId));
}

function isSafePhotonChatGuid(value: string): boolean {
  return isSafeOpaqueValue(value, 4_096) && /^any;[-+];.+$/u.test(value);
}

function isSafeIdempotencyKey(value: string): boolean {
  return /^[A-Za-z0-9][A-Za-z0-9_-]{0,255}$/.test(value);
}

function replyOptionsFor(thread: IMessageThreadReference): PhotonReplyOptions {
  return thread.replyTargetId === undefined ? Object.freeze({}) : Object.freeze({ replyTo: thread.replyTargetId });
}

/**
 * A prepared output has exactly one remote capability: a validated Photon
 * attachment GUID. Reject any provider bytes, locator, or unexpected shape
 * before a post-restart delivery path can invoke the direct SDK.
 */
function isSafePreparedPhotonOutput(value: unknown): value is StudioPreparedOutput {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  try {
    const output = value as Record<string, unknown>;
    const keys = Object.keys(output);
    return keys.length === 2 &&
      Object.hasOwn(output, "transport") &&
      Object.hasOwn(output, "attachmentGuid") &&
      output.transport === "photon" &&
      typeof output.attachmentGuid === "string" &&
      isSafePhotonOpaqueId(output.attachmentGuid);
  } catch {
    return false;
  }
}

function isSafePhotonOpaqueId(value: string): boolean {
  return isSafeOpaqueValue(value, 4_096);
}

function isSafeOpaqueValue(value: string, maximumLength: number): boolean {
  return value.length > 0 && value.length <= maximumLength && value.trim() === value && !/[\r\n\u0000]/u.test(value);
}

function assertRequiredCredential(value: string, name: string): void {
  if (!value.trim() || value.length > 4_096 || /[\r\n]/u.test(value)) {
    throw new Error(`${name} is invalid`);
  }
}
