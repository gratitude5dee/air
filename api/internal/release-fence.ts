import { handleStudioReleaseFence } from "../../lib/studio/internal-routes.js";
import { asNodeHandler } from "../../lib/studio/node-handler.js";

/** Explicit operator-only release-fence CAS; never invoked by Cron. */
export default asNodeHandler((request) => handleStudioReleaseFence(request));
