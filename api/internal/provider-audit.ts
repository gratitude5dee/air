import { handleStudioProviderAudit } from "../../lib/studio/provider-audit.js";
import { asNodeHandler } from "../../lib/studio/node-handler.js";

/** Manually invoked, bearer-protected, read-only GMI catalog/schema review. */
export default asNodeHandler((request) => handleStudioProviderAudit(request));
