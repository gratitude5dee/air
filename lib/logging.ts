/**
 * Privacy-safe structured logging. The logger receives only operational
 * metadata; it never serializes incoming text, transcript snippets, handles,
 * webhook signatures, credentials, or raw provider payloads.
 */
export type LogLevel = "info" | "warn" | "error";
export type LogFields = Readonly<Record<string, unknown>>;

export interface RedactedLogEvent {
  readonly timestamp: string;
  readonly level: LogLevel;
  readonly event: string;
  readonly fields: Record<string, unknown>;
}

export interface RedactedLogger {
  log(level: LogLevel, event: string, fields?: LogFields): void;
  info(event: string, fields?: LogFields): void;
  warn(event: string, fields?: LogFields): void;
  error(event: string, fields?: LogFields): void;
}

/**
 * Shared production metrics/log stream. Every caller still passes only
 * explicitly safe operational fields through the redaction gate below.
 */
export const operationalLogger: RedactedLogger = createRedactedLogger((event) => {
  console.log(JSON.stringify(event));
});

const SENSITIVE_KEY = /(?:message|text|content|body|sender|phone|email|handle|snippet|payload|signature|secret|token|authorization|cookie|password|api[_-]?key|uri|url)/i;
/** Only application HMACs may be emitted as identifier-like values. */
const SAFE_HMAC_KEY = /^(?:correlationId|senderHash|threadHash|responseHash|webhookIdHash|deliveryHash|recordHash)$/i;
const SAFE_MEASUREMENT_KEY = /(?:count|duration|latency|attempt|retry|status|state|age|depth|length|bytes|version|intent|category)$/i;
const SAFE_STRING_KEY = /(?:status|state|intent|category|adapter|provider|operation|version|result)$/i;
const HMAC_HEX = /^[a-f0-9]{64}$/;

/**
 * Drops disallowed fields instead of replacing their values. This means a log
 * sink cannot accidentally reconstruct user content from a nested object.
 */
export function redactLogFields(fields: LogFields = {}): Record<string, unknown> {
  return sanitizeObject(fields, 0);
}

export function createRedactedLogger(
  sink: (event: RedactedLogEvent) => void,
  clock: () => Date = () => new Date(),
): RedactedLogger {
  const log = (level: LogLevel, event: string, fields: LogFields = {}): void => {
    if (!/^[a-z0-9_.:-]{1,100}$/i.test(event)) {
      throw new Error("log event name is invalid");
    }
    sink({
      timestamp: clock().toISOString(),
      level,
      event,
      fields: redactLogFields(fields),
    });
  };
  return Object.freeze({
    log,
    info: (event: string, fields?: LogFields) => log("info", event, fields),
    warn: (event: string, fields?: LogFields) => log("warn", event, fields),
    error: (event: string, fields?: LogFields) => log("error", event, fields),
  });
}

function sanitizeObject(value: LogFields, depth: number): Record<string, unknown> {
  if (depth > 3) return {};
  const output: Record<string, unknown> = {};
  for (const [key, candidate] of Object.entries(value)) {
    if (!isSafeLogKey(key)) continue;
    const sanitized = sanitizeValue(candidate, depth + 1, key);
    if (sanitized !== undefined) output[key] = sanitized;
  }
  return output;
}

function isSafeLogKey(key: string): boolean {
  // Sensitive field names win over generic "*Id" conventions. Raw provider
  // IDs and user identifiers must never reach logs merely because they look
  // operational; known HMAC pseudonyms are the sole exception.
  if (SENSITIVE_KEY.test(key)) return SAFE_HMAC_KEY.test(key);
  if (SAFE_HMAC_KEY.test(key) || SAFE_MEASUREMENT_KEY.test(key)) return true;
  return !SENSITIVE_KEY.test(key);
}

function sanitizeValue(value: unknown, depth: number, key: string): unknown {
  if (value === null || typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isFinite(value) ? value : undefined;
  if (typeof value === "string") {
    if (SAFE_HMAC_KEY.test(key)) return HMAC_HEX.test(value) ? value : undefined;
    return SAFE_STRING_KEY.test(key) && /^[a-zA-Z0-9_.:-]{1,256}$/.test(value)
      ? value
      : undefined;
  }
  if (Array.isArray(value)) {
    if (depth > 3 || value.length > 20) return undefined;
    const items = value.map((item) => sanitizeValue(item, depth + 1, key));
    return items.every((item) => item !== undefined) ? items : undefined;
  }
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return sanitizeObject(value as LogFields, depth + 1);
  }
  return undefined;
}
