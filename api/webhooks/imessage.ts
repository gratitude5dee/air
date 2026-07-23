import { asNodeHandler } from "../../lib/studio/node-handler.js";
import { handleStudioIMessageWebhook } from "../../lib/studio/webhook-handler.js";

/**
 * The sole unauthenticated ingress; signature verification is in the handler.
 *
 * Keep this route receipt-only: it authenticates, persists, and acknowledges.
 * The minute Cron is the authoritative worker and owns all outbox/provider
 * activity, so a stalled downstream dependency cannot extend or time out an
 * inbound Photon webhook invocation.
 */
export default asNodeHandler((request) => handleStudioIMessageWebhook(request));
