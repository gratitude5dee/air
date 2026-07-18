import { createClient } from "@photon-ai/advanced-imessage";
import { cloud } from "@spectrum-ts/core";

import type { StudioAttachmentReference } from "./store.js";
import {
  isSafeTransportText,
  type IMessageThreadReference,
  type StudioIMessageTransport,
  type TransportOutcome,
} from "./imessage-transport.js";
import { assertNoPhotonTelemetryExporters } from "./photon-telemetry.js";

const PHOTON_SHARED_LINE = "shared";
const PHOTON_SHARED_ADDRESS = "imessage.spectrum.photon.codes:443";
const TOKEN_EXPIRY_SAFETY_WINDOW_MS = 30_000;

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
  readonly messages: {
    sendText(
      chatGuid: string,
      text: string,
      options: { readonly clientMessageId: string; readonly enableDataDetection: false; readonly enableLinkPreview: false },
    ): Promise<unknown>;
  };
}

export interface PhotonIMessagePort {
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
 * Text-only, direct Cloud iMessage delivery. Attachments, tapbacks, native
 * replies, and inbound media fetch remain unavailable until they have their
 * own reviewed byte and idempotency paths.
 */
export class PhotonIMessageTransport implements StudioIMessageTransport {
  public readonly verified = true;
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
      });
      return Object.freeze({ kind: "sent" });
    } catch {
      // The RPC may have reached Photon. Air deliberately does not replay it
      // automatically; the persisted clientMessageId is retained for a
      // reviewed reconciliation path if an operator later needs one.
      return Object.freeze({ kind: "send_unknown", code: "photon_text_send_unknown" });
    }
  }

  /** Artifact delivery is intentionally unavailable until its byte path is reviewed. */
  public async sendAttachment(
    _thread: IMessageThreadReference,
    _attachment: StudioAttachmentReference,
    _idempotencyKey: string,
  ): Promise<TransportOutcome> {
    return Object.freeze({ kind: "failed", code: "attachment_delivery_not_enabled" });
  }

  /** Tapbacks remain disabled until a real-line reaction spike is documented. */
  public async addLikeTapback(
    _thread: IMessageThreadReference,
    _messageId: string,
    _idempotencyKey: string,
  ): Promise<TransportOutcome> {
    return Object.freeze({ kind: "failed", code: "tapback_delivery_not_enabled" });
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
    const client = this.clientsByLine.get(sendingLine);
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
    const client = createClient({
      address,
      autoIdempotency: false,
      retry: false,
      tls: true,
      token: async () => this.tokenFor(instanceId),
    }) as PhotonIMessageClient;
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

function isSafeThreadReference(thread: IMessageThreadReference): boolean {
  return isSafeOpaqueValue(thread.chatGuid, 4_096) && isSafeOpaqueValue(thread.sendingLine, 320);
}

function isSafeIdempotencyKey(value: string): boolean {
  return /^[A-Za-z0-9][A-Za-z0-9_-]{0,255}$/.test(value);
}

function isSafeOpaqueValue(value: string, maximumLength: number): boolean {
  return value.length > 0 && value.length <= maximumLength && value.trim() === value && !/[\r\n\u0000]/u.test(value);
}

function assertRequiredCredential(value: string, name: string): void {
  if (!value.trim() || value.length > 4_096 || /[\r\n]/u.test(value)) {
    throw new Error(`${name} is invalid`);
  }
}
