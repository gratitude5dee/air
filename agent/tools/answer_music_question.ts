import { z } from "zod";
import { defineTool } from "eve/tools";
import { MAX_RENDERED_RESPONSE_CHARS } from "../../lib/citations.js";
import { answerMusicQuestion, GroundingValidationError } from "../../lib/grounded-answer.js";
import { operationalLogger } from "../../lib/logging.js";
import { isRetryableUpstreamError } from "../../lib/upstream.js";

const inputSchema = z.object({
  question: z
    .string()
    .trim()
    .min(1)
    .max(2_000)
    .describe("The user's self-contained music-industry research question."),
});

const groundedAnswerOutputSchema = z.object({
  kind: z.literal("air.grounded-answer.v1"),
  /** The only model-derived content eligible for outbound iMessage delivery. */
  rendered: z.string().min(1).max(MAX_RENDERED_RESPONSE_CHARS),
  intent: z.enum(["entity_lookup", "thematic", "comparative", "aggregative"]),
  sourceCount: z.number().int().min(0).max(4),
  citationRetries: z.number().int().min(0).max(3),
});

/** Contains no model prose; the channel maps this typed signal to its fixed notice. */
const capabilityFallbackOutputSchema = z.object({
  kind: z.literal("air.capability-fallback.v1"),
  reason: z.literal("citation_validation"),
});

const outputSchema = z.discriminatedUnion("kind", [
  groundedAnswerOutputSchema,
  capabilityFallbackOutputSchema,
]);

export type AnswerMusicQuestionToolOutput = z.infer<typeof outputSchema>;

export type AirToolFailureCategory = "upstream_timeout" | "rag_execution";

/**
 * This is the only error shape permitted to leave the application tool
 * boundary. Eve logs tool errors verbatim, while provider errors can contain
 * request bodies, transcript excerpts, headers, and credentials.
 */
export class AirToolExecutionError extends Error {
  public readonly category: AirToolFailureCategory;
  /** Eve recognizes this safe marker when deciding whether to retry a task. */
  public readonly isRetryable: boolean;

  public constructor(category: AirToolFailureCategory) {
    super(
      category === "upstream_timeout"
        ? "AR Agent research is temporarily unavailable."
        : "AR Agent research could not be completed.",
    );
    this.name = "AirToolExecutionError";
    this.category = category;
    this.isRetryable = category === "upstream_timeout";
  }
}

/** Exported separately so the tool-error boundary can be tested without Eve. */
export async function executeAnswerMusicQuestion(
  question: string,
  executeGroundedAnswer: typeof answerMusicQuestion = answerMusicQuestion,
): Promise<AnswerMusicQuestionToolOutput> {
  const startedAtMs = Date.now();
  try {
    const grounded = await executeGroundedAnswer(question);
    const output = {
      kind: "air.grounded-answer.v1" as const,
      rendered: grounded.answer,
      intent: grounded.intent,
      sourceCount: grounded.sources.length,
      citationRetries: grounded.citationRetries,
    };
    operationalLogger.info("rag.answer_completed", {
      latencyMs: Date.now() - startedAtMs,
      intent: output.intent,
      sourceCount: output.sourceCount,
      citationRetries: output.citationRetries,
    });
    return output;
  } catch (error) {
    if (error instanceof GroundingValidationError) {
      operationalLogger.warn("rag.answer_validation_fallback", {
        latencyMs: Date.now() - startedAtMs,
        category: error.reason,
      });
      return { kind: "air.capability-fallback.v1", reason: "citation_validation" };
    }
    const category: AirToolFailureCategory = isRetryableUpstreamError(error)
      ? "upstream_timeout"
      : "rag_execution";
    operationalLogger.error("rag.answer_failed", {
      latencyMs: Date.now() - startedAtMs,
      category,
    });
    // Deliberately do not set `cause` or copy properties from `error`.
    throw new AirToolExecutionError(category);
  }
}

/**
 * Air's sole callable capability. Retrieval, generation, citation checking,
 * and source rendering all happen before this tool result reaches the channel.
 * Citation validation returns a typed, model-free fallback result. The channel
 * maps it to its fixed capability text; no rejected model prose crosses this
 * boundary. Transient upstream failures still throw and use the retry path.
 */
export const answerMusicQuestionTool = defineTool({
  description:
    "Answer a music-industry research question using only the MusicRAG transcript corpus and return a citation-validated iMessage response.",
  inputSchema,
  outputSchema,
  async execute({ question }): Promise<AnswerMusicQuestionToolOutput> {
    return executeAnswerMusicQuestion(question);
  },
});

export default answerMusicQuestionTool;
