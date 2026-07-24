import { handleStudioInboxFifoMigration } from "../../lib/studio/internal-routes.js";
import { asNodeHandler } from "../../lib/studio/node-handler.js";

/** Explicit operator-only, one-way inbox FIFO migration control plane. */
export default asNodeHandler((request) => handleStudioInboxFifoMigration(request));
