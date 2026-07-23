import { describe, expect, it, vi } from "vitest";

import {
  createLazyIMessageTransportResolver,
  studioHealthResponse,
  studioPublicStatusResponse,
} from "../../lib/studio/runtime.js";
import { UnverifiedPhotonTransport } from "../../lib/studio/imessage-transport.js";

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

  it("defers Photon transport creation and retries a failed token-mint boundary", async () => {
    const createTransport = vi
      .fn<() => Promise<UnverifiedPhotonTransport>>()
      .mockRejectedValueOnce(new Error("token-service-unavailable"))
      .mockResolvedValueOnce(new UnverifiedPhotonTransport());
    const resolveTransport = createLazyIMessageTransportResolver(createTransport);

    expect(createTransport).not.toHaveBeenCalled();
    await expect(resolveTransport()).rejects.toThrow("token-service-unavailable");
    expect(createTransport).toHaveBeenCalledTimes(1);

    const transport = await resolveTransport();
    expect(transport).toBeInstanceOf(UnverifiedPhotonTransport);
    expect(createTransport).toHaveBeenCalledTimes(2);
    await expect(resolveTransport()).resolves.toBe(transport);
    expect(createTransport).toHaveBeenCalledTimes(2);
  });
});
