import { asNodeHandler } from "../../lib/studio/node-handler.js";
import { handleStudioIMessageWebhook } from "../../lib/studio/webhook-handler.js";

/**
 * Photon signs the exact request bytes. Vercel's default Node API body parser
 * would consume the stream before the raw-body verifier can read it, causing
 * a request to hang until the function deadline. Keep this route stream-only.
 */
export const config = {
  api: {
    bodyParser: false,
  },
} as const;

/**
 * The sole unauthenticated ingress; signature verification is in the handler.
 *
 * Keep this route receipt-only: it authenticates, persists, and acknowledges.
 * The minute Cron is the authoritative worker and owns all outbox/provider
 * activity, so a stalled downstream dependency cannot extend or time out an
 * inbound Photon webhook invocation.
 */
export default asNodeHandler((request) => handleStudioIMessageWebhook(request));
