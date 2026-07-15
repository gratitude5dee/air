import { inspect } from "node:util";
import { trace } from "../../node_modules/eve/dist/src/compiled/@opentelemetry/api/index.js";
import {
  createLogger,
  formatError,
  logError,
  recordErrorOnSpan,
} from "../../node_modules/eve/dist/src/internal/logging.js";
import { describe, expect, it, vi } from "vitest";
import { RetryableUpstreamError } from "../../lib/upstream.js";

const SECRET = "voyage-secret-must-not-reach-eve-observability";
const PRIVATE_QUESTION = "private iMessage question must not reach Eve observability";
const PRIVATE_TRANSCRIPT = "private transcript excerpt must not reach Eve observability";
const REDACTED_MESSAGE = "An internal runtime error occurred.";

type RecordedSpan = {
  readonly exceptions: unknown[];
  readonly events: Array<{ name: string; attributes: unknown }>;
  status: unknown;
  setStatus(status: unknown): void;
  recordException(exception: unknown): void;
  addEvent(name: string, attributes: unknown): void;
};

function makeProviderError(): RetryableUpstreamError {
  return Object.assign(
    new RetryableUpstreamError(`provider rejected: ${PRIVATE_QUESTION}`),
    {
      authorization: `Bearer ${SECRET}`,
      requestBody: { question: PRIVATE_QUESTION, transcript: PRIVATE_TRANSCRIPT },
      responseBody: `{"api_key":"${SECRET}","question":"${PRIVATE_QUESTION}"}`,
      cause: new Error(`nested provider failure: ${SECRET}`),
    },
  );
}

function makeSpan(): RecordedSpan {
  const span: RecordedSpan = {
    exceptions: [],
    events: [],
    status: undefined,
    setStatus(status) {
      this.status = status;
    },
    recordException(exception) {
      this.exceptions.push(exception);
    },
    addEvent(name, attributes) {
      this.events.push({ name, attributes });
    },
  };
  return span;
}

function expectNoProviderContent(value: unknown): void {
  const rendered = inspect(value, { depth: 10, showHidden: true });
  expect(rendered).not.toContain(SECRET);
  expect(rendered).not.toContain(PRIVATE_QUESTION);
  expect(rendered).not.toContain(PRIVATE_TRANSCRIPT);
}

describe("Eve internal logging redaction", () => {
  it("redacts a primary-model provider failure from console output and the active OTel span", () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const providerError = makeProviderError();
    const span = makeSpan();
    const logger = createLogger("harness.tool-loop");

    vi.spyOn(trace, "getActiveSpan").mockReturnValue(span as never);
    logError(logger, `tool-loop stream error: ${PRIVATE_QUESTION}`, providerError, {
      requestId: "test-request",
    });

    expect(consoleError).toHaveBeenCalledTimes(1);
    expect(consoleError.mock.calls[0]).toEqual([
      `[eve:harness.tool-loop] ${REDACTED_MESSAGE}`,
    ]);
    expectNoProviderContent(consoleError.mock.calls);

    expect(span.status).toEqual({ code: 2, message: REDACTED_MESSAGE });
    expect(span.exceptions).toEqual([
      {
        name: "EveRuntimeError",
        message: REDACTED_MESSAGE,
        stack: "error details redacted",
      },
    ]);
    expect(span.events).toEqual([]);
    expectNoProviderContent(span);
  });

  it("discards nested model-error details from error logs and active spans", () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const span = makeSpan();
    const logger = createLogger("harness.tool-loop");
    const providerError = makeProviderError();
    const nestedProviderDetails = {
      details: {
        responseBodySnippet: `{"api_key":"${SECRET}","transcript":"${PRIVATE_TRANSCRIPT}"}`,
        upstreamMessage: PRIVATE_QUESTION,
      },
      errorId: "evt_model_failure",
      sessionId: "session_safe",
    };
    const plaintextProviderError = {
      errorId: SECRET,
      message: PRIVATE_QUESTION,
      requestBody: { transcript: PRIVATE_TRANSCRIPT },
      responseBody: `Bearer ${SECRET}`,
    };

    vi.spyOn(trace, "getActiveSpan").mockReturnValue(span as never);
    logger.error(`model request failed: ${PRIVATE_QUESTION}`, { error: providerError });
    logger.error(`model request failed: ${PRIVATE_QUESTION}`, nestedProviderDetails);

    expect(consoleError.mock.calls).toEqual([
      [`[eve:harness.tool-loop] ${REDACTED_MESSAGE}`],
      [`[eve:harness.tool-loop] ${REDACTED_MESSAGE}`],
    ]);
    expect(span.events).toEqual([]);
    expect(span.status).toEqual({ code: 2, message: REDACTED_MESSAGE });
    expect(span.exceptions).toEqual([
      {
        name: "EveRuntimeError",
        message: REDACTED_MESSAGE,
        stack: "error details redacted",
      },
      {
        name: "EveRuntimeError",
        message: REDACTED_MESSAGE,
        stack: "error details redacted",
      },
    ]);
    expectNoProviderContent({ console: consoleError.mock.calls, span });

    recordErrorOnSpan(span as never, plaintextProviderError);
    expect(formatError(plaintextProviderError, "evt_redacted")).toEqual({
      errorId: "evt_redacted",
      name: "EveRuntimeError",
      message: REDACTED_MESSAGE,
      detail: "error details redacted",
    });
    expect(formatError(plaintextProviderError).errorId).not.toBe(SECRET);
    expect(span.exceptions).toHaveLength(3);
    expectNoProviderContent({ console: consoleError.mock.calls, span, formatted: formatError(providerError) });
  });
});
