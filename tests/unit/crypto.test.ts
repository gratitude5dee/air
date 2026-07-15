import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  createQueueCipher,
  decodeQueueEncryptionKey,
  decryptJson,
  encryptJson,
} from "../../lib/crypto.js";

const key = Buffer.alloc(32, 7).toString("base64");

describe("queue encryption", () => {
  it("uses a fresh AES-GCM nonce and round-trips JSON", () => {
    const cipher = createQueueCipher(key);
    const first = cipher.encrypt({ text: "private message" }, "record-1");
    const second = cipher.encrypt({ text: "private message" }, "record-1");

    expect(first.iv).not.toBe(second.iv);
    expect(cipher.decrypt<{ text: string }>(first, "record-1")).toEqual({
      text: "private message",
    });
  });

  it("rejects tampering, the wrong AAD, and malformed keys", () => {
    const cipher = createQueueCipher(key);
    const encrypted = cipher.encrypt({ text: "private message" }, "record-1");
    const replacement = encrypted.tag.startsWith("A") ? "B" : "A";
    const tampered = { ...encrypted, tag: `${replacement}${encrypted.tag.slice(1)}` };

    expect(() => cipher.decrypt(tampered, "record-1")).toThrow(/authenticated/);
    expect(() => cipher.decrypt(encrypted, "record-2")).toThrow(/authenticated/);
    expect(() => decodeQueueEncryptionKey("not a valid key")).toThrow(/base64/);
    expect(() => decodeQueueEncryptionKey(randomBytes(31).toString("base64"))).toThrow(/32 bytes/);
  });

  it("can use the lower-level API without exposing plaintext in the envelope", () => {
    const decoded = decodeQueueEncryptionKey(key);
    const encrypted = encryptJson(decoded, { secret: "do-not-log" }, "record-1");

    expect(JSON.stringify(encrypted)).not.toContain("do-not-log");
    expect(decryptJson<{ secret: string }>(decoded, encrypted, "record-1")).toEqual({
      secret: "do-not-log",
    });
  });
});
