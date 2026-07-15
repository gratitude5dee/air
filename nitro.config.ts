import { defineNitroConfig } from "nitro/config";

/**
 * Eve emits Vercel Build Output, not source files under `api/`. Nitro writes
 * this setting into the generated server function's `.vc-config.json`, where
 * Vercel's Build Output API applies the actual function duration.
 */
export default defineNitroConfig({
  vercel: {
    functions: {
      maxDuration: 60,
    },
  },
});
