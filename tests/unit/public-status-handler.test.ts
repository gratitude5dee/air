import { describe, expect, it } from "vitest";
import nitroConfig from "../../nitro.config.js";
import publicStatusHandler from "../../lib/public-status-handler.js";

describe("public Air status route", () => {
  it("reserves GET / for the application-owned Nitro handler", () => {
    expect(nitroConfig.handlers).toContainEqual({
      route: "/",
      method: "GET",
      handler: "./lib/public-status-handler.ts",
    });
  });

  it("returns only the minimal branded status payload", async () => {
    const response = publicStatusHandler();

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("application/json");
    await expect(response.json()).resolves.toEqual({
      name: "Air",
      status: "allowlisted iMessage Seedance video generation",
    });
  });
});
