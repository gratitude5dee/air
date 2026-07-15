import { defineAgent } from "eve";

/**
 * Eve supplies route discovery and the durable cron host only. Air never
 * starts an Eve model session: all accepted iMessages follow the private Zap
 * video workflow in `lib/imessage-video-runtime.ts`.
 */
export default defineAgent({
  model: "openai/gpt-4.1-mini",
  // Keep the build deterministic without resolving a remote model catalog.
  modelContextWindowTokens: 128_000,
});
