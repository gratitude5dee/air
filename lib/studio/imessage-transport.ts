import type { StudioAttachmentReference } from "./store.js";

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

export type TransportOutcome =
  | { readonly kind: "sent" }
  | { readonly kind: "retryable_failure"; readonly code: string }
  | { readonly kind: "failed"; readonly code: string }
  | { readonly kind: "send_unknown"; readonly code: string };

export interface StudioIMessageTransport {
  readonly verified: boolean;
  showTyping(thread: IMessageThreadReference): Promise<void>;
  stopTyping(thread: IMessageThreadReference): Promise<void>;
  sendText(thread: IMessageThreadReference, text: string, idempotencyKey: string): Promise<TransportOutcome>;
  sendAttachment(thread: IMessageThreadReference, attachment: StudioAttachmentReference, idempotencyKey: string): Promise<TransportOutcome>;
  addLikeTapback(thread: IMessageThreadReference, messageId: string, idempotencyKey: string): Promise<TransportOutcome>;
  fetchAttachment(thread: IMessageThreadReference, attachmentId: string): Promise<Uint8Array>;
}

/**
 * Safe default for dark deployment. It blocks delivery instead of guessing a
 * Spectrum rehydration or attachment-send API that has not passed the required
 * real-line transport spike.
 */
export class UnverifiedPhotonTransport implements StudioIMessageTransport {
  public readonly verified = false;

  public async showTyping(_thread: IMessageThreadReference): Promise<void> {}
  public async stopTyping(_thread: IMessageThreadReference): Promise<void> {}

  public async sendText(_thread: IMessageThreadReference, _text: string, _idempotencyKey: string): Promise<TransportOutcome> {
    return Object.freeze({ kind: "failed", code: "transport_not_verified" });
  }

  public async sendAttachment(_thread: IMessageThreadReference, _attachment: StudioAttachmentReference, _idempotencyKey: string): Promise<TransportOutcome> {
    return Object.freeze({ kind: "failed", code: "transport_not_verified" });
  }

  public async addLikeTapback(_thread: IMessageThreadReference, _messageId: string, _idempotencyKey: string): Promise<TransportOutcome> {
    return Object.freeze({ kind: "failed", code: "transport_not_verified" });
  }

  public async fetchAttachment(_thread: IMessageThreadReference, _attachmentId: string): Promise<Uint8Array> {
    throw new Error("transport_not_verified");
  }
}

export function isSafeTransportText(text: string): boolean {
  return text.length > 0 && text.length <= 4_000 && !/[\u0000\r]/.test(text);
}
