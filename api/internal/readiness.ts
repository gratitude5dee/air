import { handleStudioReadiness } from "../../lib/studio/internal-routes.js";
import { asNodeHandler } from "../../lib/studio/node-handler.js";

export default asNodeHandler((request) => handleStudioReadiness(request));
