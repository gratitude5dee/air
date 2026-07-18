import { describe, expect, it } from "vitest";

import { studioHealthResponse, studioPublicStatusResponse } from "../../lib/studio/runtime.js";

describe("Air public runtime surface", () => {
  it("preserves the branded dependency-free public status contract", async () => {
    const response = studioPublicStatusResponse();

    expect(response.headers.get("cache-control")).toBe("no-store");
    await expect(response.json()).resolves.toEqual({
      name: "Air",
      status: "private allowlisted iMessage studio",
    });
  });

  it("keeps health dependency-free", async () => {
    const response = studioHealthResponse();

    expect(response.headers.get("cache-control")).toBe("no-store");
    await expect(response.json()).resolves.toEqual({ ok: true, version: "2" });
  });
});
