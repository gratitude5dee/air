import {
  createCipheriv,
  createDecipheriv,
  randomBytes,
} from "node:crypto";

export const QUEUE_ENCRYPTION_ALGORITHM = "aes-256-gcm" as const;
export const QUEUE_ENCRYPTION_VERSION = 1 as const;

export interface EncryptedPayload {
  readonly version: typeof QUEUE_ENCRYPTION_VERSION;
  readonly algorithm: typeof QUEUE_ENCRYPTION_ALGORITHM;
  /** 96-bit random GCM nonce, base64url encoded. */
  readonly iv: string;
  readonly ciphertext: string;
  /** 128-bit GCM authentication tag, base64url encoded. */
  readonly tag: string;
}

/**
 * Decodes the base64-encoded 32-byte key required by AES-256-GCM.  This is
 * intentionally strict so an accidental password, truncated secret, or key
 * from another environment cannot silently weaken encryption.
 */
export function decodeQueueEncryptionKey(encoded: string): Buffer {
  if (encoded !== encoded.trim() || !/^[A-Za-z0-9+/_-]+={0,2}$/.test(encoded)) {
    throw new Error("AIR_QUEUE_ENCRYPTION_KEY must be a base64-encoded 32-byte key");
  }
  const normalized = encoded.replace(/-/g, "+").replace(/_/g, "/");
  const padding = "=".repeat((4 - (normalized.length % 4)) % 4);
  const key = Buffer.from(normalized + padding, "base64");
  if (key.byteLength !== 32) {
    throw new Error("AIR_QUEUE_ENCRYPTION_KEY must decode to exactly 32 bytes");
  }
  return key;
}

export function encryptBytes(
  key: Uint8Array,
  plaintext: Uint8Array,
  associatedData?: string,
): EncryptedPayload {
  assertEncryptionKey(key);
  const iv = randomBytes(12);
  const cipher = createCipheriv(QUEUE_ENCRYPTION_ALGORITHM, key, iv, {
    authTagLength: 16,
  });
  if (associatedData) cipher.setAAD(Buffer.from(associatedData, "utf8"));
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Object.freeze({
    version: QUEUE_ENCRYPTION_VERSION,
    algorithm: QUEUE_ENCRYPTION_ALGORITHM,
    iv: iv.toString("base64url"),
    ciphertext: ciphertext.toString("base64url"),
    tag: tag.toString("base64url"),
  });
}

export function decryptBytes(
  key: Uint8Array,
  payload: EncryptedPayload,
  associatedData?: string,
): Buffer {
  assertEncryptionKey(key);
  if (
    payload.version !== QUEUE_ENCRYPTION_VERSION ||
    payload.algorithm !== QUEUE_ENCRYPTION_ALGORITHM
  ) {
    throw new Error("unsupported encrypted payload version");
  }

  try {
    const iv = decodeBase64Url(payload.iv, 12);
    const tag = decodeBase64Url(payload.tag, 16);
    const ciphertext = decodeBase64Url(payload.ciphertext);
    const decipher = createDecipheriv(QUEUE_ENCRYPTION_ALGORITHM, key, iv, {
      authTagLength: 16,
    });
    if (associatedData) decipher.setAAD(Buffer.from(associatedData, "utf8"));
    decipher.setAuthTag(tag);
    return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  } catch {
    // Do not surface cipher, key, or payload details in a request log.
    throw new Error("encrypted payload could not be authenticated");
  }
}

export function encryptJson<T>(
  key: Uint8Array,
  value: T,
  associatedData?: string,
): EncryptedPayload {
  let serialized: string;
  try {
    serialized = JSON.stringify(value);
  } catch {
    throw new Error("value cannot be serialized for encrypted storage");
  }
  if (serialized === undefined) {
    throw new Error("value cannot be serialized for encrypted storage");
  }
  return encryptBytes(key, Buffer.from(serialized, "utf8"), associatedData);
}

export function decryptJson<T>(
  key: Uint8Array,
  payload: EncryptedPayload,
  associatedData?: string,
): T {
  const plaintext = decryptBytes(key, payload, associatedData);
  try {
    return JSON.parse(plaintext.toString("utf8")) as T;
  } catch {
    throw new Error("encrypted payload contains invalid JSON");
  }
}

export interface QueueCipher {
  encrypt<T>(value: T, associatedData?: string): EncryptedPayload;
  decrypt<T>(payload: EncryptedPayload, associatedData?: string): T;
}

/** Keeps the raw queue key confined to one closure. */
export function createQueueCipher(base64Key: string): QueueCipher {
  const key = decodeQueueEncryptionKey(base64Key);
  return Object.freeze({
    encrypt<T>(value: T, associatedData?: string): EncryptedPayload {
      return encryptJson(key, value, associatedData);
    },
    decrypt<T>(payload: EncryptedPayload, associatedData?: string): T {
      return decryptJson<T>(key, payload, associatedData);
    },
  });
}

function assertEncryptionKey(key: Uint8Array): void {
  if (key.byteLength !== 32) {
    throw new Error("AES-256-GCM requires a 32-byte key");
  }
}

function decodeBase64Url(value: string, expectedLength?: number): Buffer {
  if (!value || !/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new Error("invalid encoded payload field");
  }
  const decoded = Buffer.from(value, "base64url");
  if (expectedLength !== undefined && decoded.byteLength !== expectedLength) {
    throw new Error("invalid encoded payload field length");
  }
  return decoded;
}
