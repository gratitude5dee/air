import type {
  StudioAttachmentReference,
  StudioExecutionSnapshot,
  StudioJobRecord,
} from "./store.js";

/**
 * The only provider boundary Air's durable executor may use. It has no
 * catalog discovery, automatic retry, polling loop, or transport setup: those
 * each require their own reviewed integration evidence.
 */
export interface StudioProviderPort {
  /**
   * True only for an adapter that has passed the provider-specific conformance
   * review. The deployed runtime uses the false default below.
   */
  readonly verified: boolean;
  submit(input: StudioProviderSubmitInput): Promise<StudioProviderSubmitResult>;
}

/** User content is available only to the injected provider adapter at call time. */
export interface StudioProviderSubmitInput {
  readonly job: Pick<
    StudioJobRecord,
    "jobId" | "mode" | "modelSlug" | "provider" | "outputCount" | "durationSeconds" | "reservedCents"
  >;
  readonly snapshot: StudioExecutionSnapshot;
  /** Stable opaque provider-side idempotency/reconciliation key. */
  readonly submissionIntentId: string;
  /**
   * Provider-visible work only. Chat GUIDs, sending lines, reply targets, and
   * all delivery routing data remain inside Air's encrypted job payload.
   */
  readonly request: {
    readonly prompt: string | null;
    readonly input?: StudioAttachmentReference | undefined;
  };
}

/**
 * Submission has exactly three outcomes. There is deliberately no retryable
 * fourth state because a timeout or broken connection cannot prove the
 * provider did not receive the request.
 */
export type StudioProviderSubmitResult =
  | { readonly kind: "accepted"; readonly providerRequestId: string }
  | { readonly kind: "definitive_rejection" }
  | { readonly kind: "ambiguous" };

/**
 * Safe production default. It cannot reach GMI, fal, or any other provider
 * even if an environment toggle is misconfigured. A reviewed adapter must be
 * injected explicitly in a future release.
 */
export class UnprovisionedStudioProvider implements StudioProviderPort {
  public readonly verified = false;

  public async submit(_input: StudioProviderSubmitInput): Promise<StudioProviderSubmitResult> {
    return Object.freeze({ kind: "definitive_rejection" });
  }
}
