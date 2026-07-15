import { createClient } from "redis";

const VIDEO_PREFIX = "air:video:v1";
const MANUAL_REVIEW_INDEX = `${VIDEO_PREFIX}:manual-review`;
const MAX_LIMIT = 500;
const DEFAULT_LIMIT = 100;
const CORRELATION_ID = /^[a-f0-9]{64}$/;
const FAILURE_CATEGORY = /^[a-z0-9_:-]{1,120}$/;
const RECORD_KEY = new RegExp(`^${escapeRegExp(VIDEO_PREFIX)}:record:[a-f0-9]{64}$`);

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

/** Extract only fields approved for operator output from redacted metadata. */
function reportRow(serialized, recordKey, now) {
  try {
    const record = JSON.parse(serialized);
    if (
      record?.schemaVersion !== 1 ||
      (record?.state !== "sending" && record?.state !== "send_unknown" && record?.state !== "submission_unknown") ||
      record?.immutable?.recordKey !== recordKey ||
      !CORRELATION_ID.test(record?.immutable?.inboundEventHash ?? "") ||
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
      correlationId: record.immutable.inboundEventHash,
      state: record.state,
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
    // This command is metadata-only: it reads the manual-review index and
    // record metadata, never the encrypted job payload.
    const now = Date.now();
    const recordKeys = (await client.zRangeByScore(MANUAL_REVIEW_INDEX, "-inf", "+inf", {
      LIMIT: { offset: 0, count: limit },
    })).filter((recordKey) => RECORD_KEY.test(recordKey));
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
  // Never include a Redis URL, credential, record, or payload in stderr.
  process.stderr.write("manual video review report failed\n");
  process.exitCode = 1;
});
