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
    const startedAtMs = Date.now();
    try {
      const grounded = await answerMusicQuestion(question);
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
      operationalLogger.error("rag.answer_failed", {
        latencyMs: Date.now() - startedAtMs,
        category: isRetryableUpstreamError(error) ? "upstream_timeout" : "rag_execution",
      });
      throw error;
    }
  },
});

export default answerMusicQuestionTool;
