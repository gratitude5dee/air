import { createClient } from "redis";

const OUTBOX_PREFIX = "air:outbox:v1";
const SEND_UNKNOWN_INDEX = `${OUTBOX_PREFIX}:send_unknown`;
const MAX_LIMIT = 500;
const DEFAULT_LIMIT = 100;
const CORRELATION_ID = /^[a-f0-9]{64}$/;
const FAILURE_CATEGORY = /^[a-z0-9_.-]{1,80}$/i;
const RECORD_KEY = new RegExp(`^${escapeRegExp(OUTBOX_PREFIX)}:record:[a-f0-9]{64}$`);

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function parseLimit(argv) {
  if (argv.length === 0) return DEFAULT_LIMIT;
  if (argv.length !== 2 || argv[0] !== "--limit") throw new Error("invalid report arguments");
  const limit = Number(argv[1]);
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT) {
    throw new Error("invalid report limit");
  }
  return limit;
}

function metadataKey(recordKey) {
  return `${recordKey}:metadata`;
}

/** Extract only fields approved for operator output from a redacted metadata key. */
function reportRow(serialized, recordKey, now) {
  try {
    const record = JSON.parse(serialized);
    if (
      record?.schemaVersion !== 1 ||
      record?.state !== "send_unknown" ||
      Object.hasOwn(record, "encryptedResponse") ||
      record?.immutable?.recordKey !== recordKey ||
      !CORRELATION_ID.test(record?.immutable?.correlationId ?? "") ||
      !Number.isSafeInteger(record?.immutable?.metadataExpiresAtMs) ||
      record.immutable.metadataExpiresAtMs <= now ||
      !Number.isSafeInteger(record?.status?.updatedAtMs) ||
      !Number.isSafeInteger(record?.status?.attempts) ||
      record.status.attempts < 0
    ) {
      return null;
    }
    const category = typeof record.status.failureCategory === "string" &&
      FAILURE_CATEGORY.test(record.status.failureCategory)
      ? record.status.failureCategory.toLowerCase()
      : "unknown_failure";
    return {
      correlationId: record.immutable.correlationId,
      age: Math.max(0, now - record.status.updatedAtMs),
      attempt: record.status.attempts,
      errorCategory: category,
    };
  } catch {
    return null;
  }
}

async function main() {
  const redisUrl = process.env.REDIS_URL;
  if (!redisUrl) throw new Error("missing redis configuration");
  let parsedUrl;
  try {
    parsedUrl = new URL(redisUrl);
  } catch {
    throw new Error("invalid redis configuration");
  }
  if (parsedUrl.protocol !== "redis:" && parsedUrl.protocol !== "rediss:") {
    throw new Error("invalid redis configuration");
  }

  const limit = parseLimit(process.argv.slice(2));
  const client = createClient({ url: redisUrl });
  client.on("error", () => undefined);
  try {
    await client.connect();
    // This command is strictly metadata-only: it reads the index and
    // `<record>:metadata` keys, never the encrypted `<record>` payload key.
    const now = Date.now();
    const recordKeys = (await client.zRangeByScore(SEND_UNKNOWN_INDEX, now, "+inf", {
      LIMIT: { offset: 0, count: limit },
    }))
      .filter((recordKey) => RECORD_KEY.test(recordKey));
    if (recordKeys.length === 0) return;
    const metadata = await client.mGet(recordKeys.map(metadataKey));
    for (const [index, serialized] of metadata.entries()) {
      if (!serialized) continue;
      const recordKey = recordKeys[index];
      if (!recordKey) continue;
      const row = reportRow(serialized, recordKey, now);
      if (row) process.stdout.write(`${JSON.stringify(row)}\n`);
    }
  } finally {
    if (client.isOpen) await client.close();
  }
}

void main().catch(() => {
  // Never include a Redis URL, credential, record, or response in stderr.
  process.stderr.write("send-unknown report failed\n");
  process.exitCode = 1;
});
