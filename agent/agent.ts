import { defineAgent } from "eve";

/**
 * A gateway model id is resolved by Eve through Vercel AI Gateway. The RAG
 * tool owns answer generation and sets its deterministic generation options.
 */
export const GENERATION_MODEL = "google/gemini-3.5-flash";

export default defineAgent({
  model: GENERATION_MODEL,
  // Eve 0.24.3's bundled model catalog has not yet learned this May 2026 AI
  // Gateway model. Google and Vercel document a 1,048,576-token context
  // window, so supply the supported build-time escape hatch rather than
  // changing the requested Gateway model or requiring network catalog access.
  modelContextWindowTokens: 1_048_576,
});
