import { defineNitroConfig } from "nitro/config";

/**
 * Eve emits Vercel Build Output, not source files under `api/`. Nitro writes
 * this setting into the generated server function's `.vc-config.json`, where
 * Vercel's Build Output API applies the actual function duration.
 */
export default defineNitroConfig({
  // Eve registers its framework home page after Nitro has loaded this project
  // config. Nitro preserves the first static method match, so this supported
  // application handler keeps GET / branded as AR Agent rather than exposing
  // Eve's package-name-derived landing page.
  handlers: [
    {
      route: "/",
      method: "GET",
      handler: "./lib/public-status-handler.ts",
    },
  ],
  vercel: {
    functions: {
      maxDuration: 60,
    },
  },
});
