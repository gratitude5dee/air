import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

describe("Air landing delivery", () => {
  it("serves the landing only to HTML clients while retaining the API root fallback", async () => {
    const config = JSON.parse(await readFile(new URL("../vercel.json", import.meta.url), "utf8")) as {
      rewrites: Array<Record<string, unknown>>;
    };
    const rootRewrites = config.rewrites.filter((rewrite) => rewrite.source === "/");

    expect(rootRewrites).toEqual([
      {
        source: "/",
        has: [{ type: "header", key: "accept", value: ".*text/html.*" }],
        destination: "/air-landing.html",
      },
      { source: "/", destination: "/api" },
    ]);
  });

  it("ships the supplied landing with the WZRD favicon, mobile-safe intro media, and launch video", async () => {
    const [landing, polish] = await Promise.all([
      readFile(new URL("../public/air-landing.html", import.meta.url), "utf8"),
      readFile(new URL("../public/air-polish.js", import.meta.url), "utf8"),
    ]);
    const favicon = await readFile(new URL("../public/wzrdtechlogo.png", import.meta.url));

    expect(landing).toContain("Send a text.<br>Get a film back.");
    expect(landing).toContain("prefers-reduced-motion: reduce");
    expect(landing).toContain("doc.title = 'Air by WZRD'");
    expect(landing).toContain("favicon.href = '/wzrdtechlogo.png'");
    expect(landing).toContain("polish.src = '/air-polish.js'");
    expect(polish).toContain("data-air-hero-mark");
    expect(polish).toContain("data-air-page-bubble");
    expect(polish).toContain('video.src = "/air-launch.mp4"');
    expect(polish).toContain("video.controls = true");
    expect(polish).toContain("video.preload = \"metadata\"");
    expect(polish).toContain("configureMobileIntroVideo");
    expect(polish).toContain("(max-width: 767px)");
    expect(polish).not.toContain("data-air-cursor-field");
    expect(polish).toContain('border-radius: 999px');
    expect(favicon.subarray(1, 4).toString("ascii")).toBe("PNG");
    const launchVideo = await readFile(new URL("../public/air-launch.mp4", import.meta.url));
    expect(launchVideo.subarray(4, 8).toString("ascii")).toBe("ftyp");
  });
});
