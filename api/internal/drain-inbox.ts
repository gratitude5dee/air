import { drainStudio } from "../../lib/studio/engine.js";
import { handleStudioDrain } from "../../lib/studio/internal-routes.js";
import { asNodeHandler } from "../../lib/studio/node-handler.js";

/** Vercel's minute cron and manual recovery use the same bearer-protected path. */
export default asNodeHandler((request) => handleStudioDrain(request, drainStudio));
