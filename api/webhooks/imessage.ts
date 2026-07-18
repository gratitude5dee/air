import { drainStudio } from "../../lib/studio/engine.js";
import { asNodeHandler } from "../../lib/studio/node-handler.js";
import { handleStudioIMessageWebhook } from "../../lib/studio/webhook-handler.js";

/** The sole unauthenticated ingress; signature verification is in the handler. */
export default asNodeHandler((request) => handleStudioIMessageWebhook(request, { drain: drainStudio }));
