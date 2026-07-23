import { waitUntil } from "@vercel/functions";
import { drainStudio } from "../../lib/studio/engine.js";
import { asNodeHandler } from "../../lib/studio/node-handler.js";
import { handleStudioIMessageWebhook } from "../../lib/studio/webhook-handler.js";

/**
 * The sole unauthenticated ingress; signature verification is in the handler.
 *
 * Receipt persistence stays on the acknowledgement path. Once that succeeds,
 * the handler uses Vercel's Node-runtime `waitUntil` hook for the best-effort,
 * zero-cost drain instead of relying on a fire-and-forget promise that the
 * function could freeze after returning its 200 response. The cron drain
 * remains the authoritative recovery path if this bounded continuation fails.
 */
export default asNodeHandler((request) => handleStudioIMessageWebhook(request, {
  drain: drainStudio,
  waitUntil,
}));
