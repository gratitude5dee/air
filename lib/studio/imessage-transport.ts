import type { StudioAttachmentReference, StudioPreparedOutput } from "./store.js";
import type { StudioValidatedProviderArtifact } from "./artifact-validation.js";

/**
 * One physical iMessage side effect per method. The concrete Photon binding is
 * deliberately isolated here so a drain retry cannot accidentally turn a
 * text+attachment batch into ambiguous duplicate sends.
 */
export interface IMessageThreadReference {
  readonly chatGuid: string;
  readonly sendingLine: string;
  readonly replyTargetId?: string | undefined;
}

/**
 * A caller-owned absolute deadline for one outbound operation.  Keeping this
 * at the transport boundary lets the Cron worker share one Vercel invocation
 * budget without making a durable intent's retry policy depend on wall-clock
 * timing.  Implementations must treat an elapsed deadline before an RPC as a
 * pre-send failure; an RPC that has already begun remains ambiguous.
 */
export interface StudioTransportOperationOptions {
  readonly deadlineAtMs?: number | undefined;
  /**
   * Optional physical-RPC admission margin. A concrete transport must not
   * begin a message/upload RPC once less than this remains; it should instead
   * report a retryable pre-send outcome.
   */
  readonly minimumPhysicalActionRemainingMs?: number | undefined;
  /**
   * Larger admission reserve for a delivery-preparation flow which may first
   * mint a Photon token and only then begin an upload/send RPC.
   */
  readonly minimumDeliveryPreparationRemainingMs?: number | undefined;
}

export type TransportOutcome =
  | { readonly kind: "sent" }
  | { readonly kind: "retryable_failure"; readonly code: string }
  | { readonly kind: "failed"; readonly code: string }
  | { readonly kind: "send_unknown"; readonly code: string };

/**
 * Upload is a distinct physical operation with its own ambiguity boundary.
 * A successful result is not a sent iMessage: it only authorizes the caller
 * to durably record a validated Photon attachment GUID before a later send.
 */
export type StudioPrepareOutputOutcome =
  | { readonly kind: "prepared"; readonly output: StudioPreparedOutput }
  | { readonly kind: "retryable_failure"; readonly code: string }
  | { readonly kind: "failed"; readonly code: string }
  | { readonly kind: "send_unknown"; readonly code: string };

export interface StudioIMessageTransport {
  readonly verified: boolean;
  showTyping(thread: IMessageThreadReference, options?: StudioTransportOperationOptions): Promise<void>;
  stopTyping(thread: IMessageThreadReference, options?: StudioTransportOperationOptions): Promise<void>;
  sendText(
    thread: IMessageThreadReference,
    text: string,
    idempotencyKey: string,
    options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome>;
  sendAttachment(
    thread: IMessageThreadReference,
    attachment: StudioAttachmentReference,
    idempotencyKey: string,
    options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome>;
  addLikeTapback(
    thread: IMessageThreadReference,
    messageId: string,
    idempotencyKey: string,
    options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome>;
  fetchAttachment(thread: IMessageThreadReference, attachmentId: string): Promise<Uint8Array>;
}

/**
 * Optional capability for the direct Photon output path. It is deliberately
 * separate from the legacy transport so old/drain transports never receive
 * provider bytes or an unreviewed Photon attachment GUID by accident.
 */
export interface StudioPreparedOutputTransport {
  readonly preparedOutputTransport: "photon";
  prepareValidatedProviderArtifact(
    thread: IMessageThreadReference,
    artifact: StudioValidatedProviderArtifact,
    options?: StudioTransportOperationOptions,
  ): Promise<StudioPrepareOutputOutcome>;
  sendPreparedOutput(
    thread: IMessageThreadReference,
    output: StudioPreparedOutput,
    idempotencyKey: string,
    options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome>;
}

/**
 * Callers must explicitly opt into this narrow capability. Every legacy or
 * non-Photon transport remains on the fail-closed attachment path.
 */
export function isStudioPreparedOutputTransport(
  transport: StudioIMessageTransport,
): transport is StudioIMessageTransport & StudioPreparedOutputTransport {
  const candidate = transport as Partial<StudioPreparedOutputTransport>;
  return candidate.preparedOutputTransport === "photon" &&
    typeof candidate.prepareValidatedProviderArtifact === "function" &&
    typeof candidate.sendPreparedOutput === "function";
}

/**
 * Safe default for dark deployment. It blocks delivery instead of guessing a
 * Spectrum rehydration or attachment-send API that has not passed the required
 * real-line transport spike.
 */
export class UnverifiedPhotonTransport implements StudioIMessageTransport {
  public readonly verified = false;

  public async showTyping(_thread: IMessageThreadReference, _options?: StudioTransportOperationOptions): Promise<void> {}
  public async stopTyping(_thread: IMessageThreadReference, _options?: StudioTransportOperationOptions): Promise<void> {}

  public async sendText(
    _thread: IMessageThreadReference,
    _text: string,
    _idempotencyKey: string,
    _options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome> {
    return Object.freeze({ kind: "failed", code: "transport_not_verified" });
  }

  public async sendAttachment(
    _thread: IMessageThreadReference,
    _attachment: StudioAttachmentReference,
    _idempotencyKey: string,
    _options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome> {
    return Object.freeze({ kind: "failed", code: "transport_not_verified" });
  }

  public async addLikeTapback(
    _thread: IMessageThreadReference,
    _messageId: string,
    _idempotencyKey: string,
    _options?: StudioTransportOperationOptions,
  ): Promise<TransportOutcome> {
    return Object.freeze({ kind: "failed", code: "transport_not_verified" });
  }

  public async fetchAttachment(_thread: IMessageThreadReference, _attachmentId: string): Promise<Uint8Array> {
    throw new Error("transport_not_verified");
  }
}

export function isSafeTransportText(text: string): boolean {
  return text.length > 0 && text.length <= 4_000 && !/[\u0000\r]/.test(text);
}
