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
const PHOTON_LIKE_TAPBACK = Object.freeze({ kind: "like" as const });

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
  getClient(sendingLine: string): Promise<PhotonIMessageClient>;
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
  const port = options.port ?? await CloudIMessagePort.create(options.projectId, options.projectSecret);
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

  public async showTyping(thread: IMessageThreadReference): Promise<void> {
    const client = await this.clientFor(thread);
    await client.chats.setTyping(thread.chatGuid, true);
  }

  public async stopTyping(thread: IMessageThreadReference): Promise<void> {
    const client = await this.clientFor(thread);
    await client.chats.setTyping(thread.chatGuid, false);
  }

  public async sendText(
    thread: IMessageThreadReference,
    text: string,
    idempotencyKey: string,
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
      client = await this.port.getClient(thread.sendingLine);
    } catch {
      // Client resolution/token minting happens before a physical message RPC,
      // so this is safely retryable by the durable outbox.
      return Object.freeze({ kind: "retryable_failure", code: "photon_client_unavailable" });
    }

    try {
      await client.messages.sendText(thread.chatGuid, text, {
        clientMessageId: idempotencyKey,
        // Text-only onboarding/help must not trigger link previews or
        // data-detector enrichment outside the signed Air boundary.
        enableDataDetection: false,
        enableLinkPreview: false,
        ...replyOptionsFor(thread),
      });
      return Object.freeze({ kind: "sent" });
    } catch {
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

    const prepared = await this.prepareValidatedProviderArtifact(thread, attachment);
    if (prepared.kind !== "prepared") return prepared;

    const sent = await this.sendPreparedOutput(thread, prepared.output, idempotencyKey);
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
      client = await this.port.getClient(thread.sendingLine);
    } catch {
      return Object.freeze({ kind: "retryable_failure", code: "photon_client_unavailable" });
    }

    let attachmentGuid: string;
    try {
      const uploaded = await client.attachments.upload({
        data: uploadArtifact.bytes,
        fileName: uploadArtifact.filename,
      });
      attachmentGuid = uploaded.attachment.guid;
    } catch {
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
      client = await this.port.getClient(thread.sendingLine);
    } catch {
      return Object.freeze({ kind: "retryable_failure", code: "photon_client_unavailable" });
    }

    try {
      await client.messages.sendAttachment(thread.chatGuid, output.attachmentGuid, {
        clientMessageId: idempotencyKey,
        ...replyOptionsFor(thread),
      });
      return Object.freeze({ kind: "sent" });
    } catch {
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
      client = await this.port.getClient(thread.sendingLine);
    } catch {
      return Object.freeze({ kind: "retryable_failure", code: "photon_client_unavailable" });
    }

    try {
      await client.messages.setReaction(
        thread.chatGuid,
        messageId,
        PHOTON_LIKE_TAPBACK,
        true,
        { clientMessageId: idempotencyKey },
      );
      return Object.freeze({ kind: "sent" });
    } catch {
      return Object.freeze({ kind: "send_unknown", code: "photon_like_tapback_unknown" });
    }
  }

  /** Inbound attachment bytes stay unavailable until media validation is wired. */
  public async fetchAttachment(_thread: IMessageThreadReference, _attachmentId: string): Promise<Uint8Array> {
    throw new Error("attachment_transport_not_enabled");
  }

  private async clientFor(thread: IMessageThreadReference): Promise<PhotonIMessageClient> {
    if (!isSafeThreadReference(thread)) throw new Error("invalid_thread_reference");
    return this.port.getClient(thread.sendingLine);
  }
}

/**
 * Cloud token renewal is request-driven rather than timer-driven so Vercel
 * functions do not retain a background resource. Each direct iMessage RPC
 * still uses Photon-issued credentials; all automatic RPC retry is disabled.
 */
class CloudIMessagePort implements PhotonIMessagePort {
  private tokenData: Awaited<ReturnType<typeof cloud.issueImessageTokens>>;
  private expiresAtMs: number;
  private readonly clientsByInstance = new Map<string, PhotonIMessageClient>();
  private clientsByLine = new Map<string, PhotonIMessageClient>();
  private refreshInFlight: Promise<void> | undefined;

  private constructor(
    private readonly projectId: string,
    private readonly projectSecret: string,
    tokenData: Awaited<ReturnType<typeof cloud.issueImessageTokens>>,
  ) {
    this.tokenData = tokenData;
    this.expiresAtMs = expiryFromToken(tokenData);
    this.syncClients(tokenData);
  }

  public static async create(projectId: string, projectSecret: string): Promise<CloudIMessagePort> {
    const tokenData = await cloud.issueImessageTokens(projectId, projectSecret);
    return new CloudIMessagePort(projectId, projectSecret, tokenData);
  }

  public async getClient(sendingLine: string): Promise<PhotonIMessageClient> {
    await this.refreshIfNeeded();
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
    await this.refreshIfNeeded();
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

  private async refreshIfNeeded(): Promise<void> {
    if (Date.now() < this.expiresAtMs - TOKEN_EXPIRY_SAFETY_WINDOW_MS) return;
    if (!this.refreshInFlight) {
      this.refreshInFlight = this.refresh().finally(() => {
        this.refreshInFlight = undefined;
      });
    }
    await this.refreshInFlight;
  }

  private async refresh(): Promise<void> {
    const tokenData = await cloud.issueImessageTokens(this.projectId, this.projectSecret);
    this.tokenData = tokenData;
    this.expiresAtMs = expiryFromToken(tokenData);
    this.syncClients(tokenData);
  }

  private syncClients(tokenData: Awaited<ReturnType<typeof cloud.issueImessageTokens>>): void {
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

function expiryFromToken(tokenData: Awaited<ReturnType<typeof cloud.issueImessageTokens>>): number {
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
