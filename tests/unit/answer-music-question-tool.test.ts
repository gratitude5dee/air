import { describe, expect, it } from "vitest";
import {
  AirToolExecutionError,
  executeAnswerMusicQuestion,
} from "../../agent/tools/answer_music_question.js";
import { RetryableUpstreamError } from "../../lib/upstream.js";

describe("answer_music_question tool boundary", () => {
  it("replaces secret-bearing upstream errors with a minimal retryable Eve-safe error", async () => {
    const secret = "voyage-secret-should-never-reach-eve";
    const privateQuestion = "private iMessage question should never reach Eve logs";
    const upstream = Object.assign(new RetryableUpstreamError(`provider rejected ${privateQuestion}`), {
      authorization: `Bearer ${secret}`,
      requestBody: { question: privateQuestion, transcript: "private transcript excerpt" },
      responseBody: `{"api_key":"${secret}","question":"${privateQuestion}"}`,
      cause: new Error(`nested ${secret}`),
    });

    let thrown: unknown;
    try {
      await executeAnswerMusicQuestion(
        privateQuestion,
        async () => {
          throw upstream;
        },
      );
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(AirToolExecutionError);
    const safe = thrown as AirToolExecutionError;
    expect(safe).toMatchObject({
      name: "AirToolExecutionError",
      message: "AR Agent research is temporarily unavailable.",
      category: "upstream_timeout",
      isRetryable: true,
    });
    expect("cause" in safe).toBe(false);
    const observableBoundary = JSON.stringify({
      name: safe.name,
      message: safe.message,
      stack: safe.stack,
      ownFields: Object.fromEntries(Object.entries(safe)),
    });
    expect(observableBoundary).not.toContain(secret);
    expect(observableBoundary).not.toContain(privateQuestion);
    expect(observableBoundary).not.toContain("private transcript excerpt");
  });
});
