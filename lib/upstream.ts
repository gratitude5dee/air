/**
 * A bounded upstream call may safely be surfaced as a transient operational
 * failure. Its message is deliberately generic so callers never log provider
 * payloads or connection details.
 */
export class RetryableUpstreamError extends Error {
  public readonly retryable = true;

  public constructor(message: string) {
    super(message);
    this.name = "RetryableUpstreamError";
  }
}

export class UpstreamTimeoutError extends RetryableUpstreamError {
  public constructor(message: string) {
    super(message);
    this.name = "UpstreamTimeoutError";
  }
}

/**
 * One request-wide budget for a multi-stage RAG turn. Individual provider
 * timeouts prevent a single call from hanging; this deadline prevents a valid
 * comparative query plus citation repairs from cumulatively exceeding Vercel's
 * 60-second function limit.
 */
export class OperationDeadline {
  public readonly signal: AbortSignal;
  private readonly controller = new AbortController();
  private readonly deadlineAtMs: number;
  private readonly expired: Promise<never>;
  private timer: ReturnType<typeof setTimeout> | undefined;

  public constructor(
    budgetMs: number,
    private readonly now: () => number = Date.now,
  ) {
    if (!Number.isInteger(budgetMs) || budgetMs < 1) {
      throw new Error("operation deadline must be a positive integer");
    }
    this.deadlineAtMs = this.now() + budgetMs;
    this.signal = this.controller.signal;
    this.expired = new Promise<never>((_resolve, reject) => {
      this.timer = setTimeout(() => {
        const error = new UpstreamTimeoutError("RAG operation deadline exceeded");
        this.controller.abort(error);
        reject(error);
      }, budgetMs);
    });
  }

  remainingMs(): number {
    return Math.max(0, this.deadlineAtMs - this.now());
  }

  throwIfExpired(): void {
    if (this.signal.aborted || this.remainingMs() < 1) {
      throw new UpstreamTimeoutError("RAG operation deadline exceeded");
    }
  }

  /** Race even a non-cooperating dependency while retaining its rejection handler. */
  async run<T>(operation: Promise<T>): Promise<T> {
    this.throwIfExpired();
    return Promise.race([operation, this.expired]);
  }

  dispose(): void {
    if (this.timer !== undefined) clearTimeout(this.timer);
  }
}

export function isRetryableUpstreamError(error: unknown): error is RetryableUpstreamError {
  return error instanceof RetryableUpstreamError;
}
