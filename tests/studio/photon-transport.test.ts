import { describe, expect, it, vi } from "vitest";

import type { IMessageThreadReference } from "../../lib/studio/imessage-transport.js";
import { assertNoPhotonTelemetryExporters } from "../../lib/studio/photon-telemetry.js";
import {
  PhotonIMessageTransport,
  type PhotonIMessagePort,
} from "../../lib/studio/photon-transport.js";

const THREAD: IMessageThreadReference = Object.freeze({
  chatGuid: "iMessage;-;+15555550101",
  sendingLine: "shared",
});

function fixture(): {
  readonly transport: PhotonIMessageTransport;
  readonly port: { readonly getClient: ReturnType<typeof vi.fn> };
  readonly client: {
    readonly chats: { readonly setTyping: ReturnType<typeof vi.fn> };
    readonly messages: { readonly sendText: ReturnType<typeof vi.fn> };
  };
} {
  const client = {
    chats: { setTyping: vi.fn(async () => undefined) },
    messages: { sendText: vi.fn(async () => ({ guid: "sent-message" })) },
  };
  const port = { getClient: vi.fn(async () => client) };
  const transport = new PhotonIMessageTransport({
    projectId: "project",
    projectSecret: "project-secret",
    webhookSecret: "webhook-secret",
    port: port as unknown as PhotonIMessagePort,
  });
  return { transport, port, client };
}

describe("Photon iMessage text transport", () => {
  it("requires the signed inbound webhook credential before marking delivery verified", () => {
    const port = { getClient: vi.fn() } as unknown as PhotonIMessagePort;
    expect(() => new PhotonIMessageTransport({
      projectId: "project",
      projectSecret: "project-secret",
      webhookSecret: "",
      port,
    })).toThrow("IMESSAGE_WEBHOOK_SECRET is invalid");
  });

  it("rebuilds the stored sending line and carries Air's stable outbox key to Photon", async () => {
    const { transport, port, client } = fixture();

    await expect(transport.sendText(THREAD, "Air is ready.", "stable-outbox-key"))
      .resolves.toEqual({ kind: "sent" });

    expect(port.getClient).toHaveBeenCalledWith(THREAD.sendingLine);
    expect(client.messages.sendText).toHaveBeenCalledWith(THREAD.chatGuid, "Air is ready.", {
      clientMessageId: "stable-outbox-key",
      enableDataDetection: false,
      enableLinkPreview: false,
    });
  });

  it("maps a rejected physical send to manual reconciliation instead of replay", async () => {
    const { transport, client } = fixture();
    client.messages.sendText.mockRejectedValueOnce(new Error("network"));

    await expect(transport.sendText(THREAD, "Air is ready.", "stable-outbox-key"))
      .resolves.toEqual({ kind: "send_unknown", code: "photon_text_send_unknown" });
  });

  it("uses a retryable result only when a client cannot be resolved before a send RPC", async () => {
    const { transport, port, client } = fixture();
    port.getClient.mockRejectedValueOnce(new Error("token mint unavailable"));

    await expect(transport.sendText(THREAD, "Air is ready.", "stable-outbox-key"))
      .resolves.toEqual({ kind: "retryable_failure", code: "photon_client_unavailable" });
    expect(client.messages.sendText).not.toHaveBeenCalled();
  });

  it("does not pass invalid text, routes, or idempotency keys to Photon", async () => {
    const { transport, port, client } = fixture();

    await expect(transport.sendText(THREAD, "unsafe\rtext", "stable-outbox-key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_text_payload" });
    await expect(transport.sendText({ ...THREAD, chatGuid: "bad\nroute" }, "Air is ready.", "stable-outbox-key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_thread_reference" });
    await expect(transport.sendText(THREAD, "Air is ready.", "bad key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_idempotency_key" });

    expect(port.getClient).not.toHaveBeenCalled();
    expect(client.messages.sendText).not.toHaveBeenCalled();
  });

  it("uses Photon typing only as a best-effort display signal on the same stored route", async () => {
    const { transport, port, client } = fixture();

    await expect(transport.showTyping(THREAD)).resolves.toBeUndefined();
    await expect(transport.stopTyping(THREAD)).resolves.toBeUndefined();

    expect(port.getClient).toHaveBeenNthCalledWith(1, THREAD.sendingLine);
    expect(port.getClient).toHaveBeenNthCalledWith(2, THREAD.sendingLine);
    expect(client.chats.setTyping).toHaveBeenNthCalledWith(1, THREAD.chatGuid, true);
    expect(client.chats.setTyping).toHaveBeenNthCalledWith(2, THREAD.chatGuid, false);
  });

  it("keeps media, attachment fetch, and tapbacks disabled in the text-only release", async () => {
    const { transport, client } = fixture();

    await expect(transport.sendAttachment(THREAD, { id: "attachment", mimeType: "image/png", sizeBytes: 1 }, "key"))
      .resolves.toEqual({ kind: "failed", code: "attachment_delivery_not_enabled" });
    await expect(transport.addLikeTapback(THREAD, "message", "key"))
      .resolves.toEqual({ kind: "failed", code: "tapback_delivery_not_enabled" });
    await expect(transport.fetchAttachment(THREAD, "attachment"))
      .rejects.toThrow("attachment_transport_not_enabled");

    expect(client.messages.sendText).not.toHaveBeenCalled();
  });

  it("fails closed before Photon initialization when telemetry or logging is unsafe", () => {
    expect(() => assertNoPhotonTelemetryExporters({ OTEL_EXPORTER_OTLP_ENDPOINT: "https://telemetry.example.test" }))
      .toThrow("Photon telemetry exporters are not permitted for Air");
    expect(() => assertNoPhotonTelemetryExporters({ LOG_LEVEL: "info" }))
      .toThrow("Photon telemetry exporters are not permitted for Air");
    expect(() => assertNoPhotonTelemetryExporters({ LOG_LEVEL: "silent", OTEL_TRACES_EXPORTER: "none" })).not.toThrow();
  });
});
