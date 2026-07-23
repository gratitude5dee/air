import { createHash } from "node:crypto";

import { loadStudioEnvironment, type StudioEnvironment } from "./config.js";
import {
  GmiRequestQueueClient,
  type GmiJsonObject,
  type GmiJsonValue,
} from "./gmi-request-queue.js";
import { authorizeStudioDrain } from "./webhook-handler.js";

/** The only provider model this manual review endpoint is permitted to inspect. */
export const GMI_PROVIDER_AUDIT_MODEL_ID = "flux-kontext-pro";
/** Keep a manual response small even if a provider adds a huge schema. */
export const MAX_PROVIDER_AUDIT_PARAMETERS = 32;

export type StudioProviderAuditDeclaredType =
  | "string"
  | "number"
  | "integer"
  | "boolean"
  | "object"
  | "array"
  | "unknown";

/** A deliberately narrow, provider-text-free schema view for human review. */
export interface StudioProviderAuditParameter {
  readonly name: string;
  readonly declaredType: StudioProviderAuditDeclaredType;
  readonly required: boolean;
}

export interface StudioProviderAuditResponse {
  readonly modelId: typeof GMI_PROVIDER_AUDIT_MODEL_ID;
  readonly available: boolean;
  readonly catalogFingerprint: string;
  readonly schemaFingerprint: string;
  readonly parameters: readonly StudioProviderAuditParameter[];
  readonly pricing: "manual_console_required";
}

/** Read-only subset intentionally excluding submit, poll, upload, and cancel. */
export interface StudioProviderAuditClient {
  listModels(): Promise<GmiJsonValue>;
  getModelDetail(modelId: string): Promise<GmiJsonValue>;
}

export interface StudioProviderAuditOptions {
  readonly loadEnvironment?: (() => StudioEnvironment) | undefined;
  readonly createClient?: ((input: {
    readonly apiKey: string;
    readonly organizationId?: string | undefined;
  }) => StudioProviderAuditClient) | undefined;
}

/**
 * Performs only the two approved read-only GMI requests needed to review the
 * exact Flux model contract. This route intentionally cannot enable a model,
 * update the registry, submit a request, poll a job, or upload media.
 */
export async function handleStudioProviderAudit(
  request: Request,
  options: StudioProviderAuditOptions = {},
): Promise<Response> {
  if (request.method !== "GET") return methodNotAllowed();

  // Match the protected drain route: parse the environment only to obtain its
  // bearer credential, authenticate immediately, and do not construct a GMI
  // client or touch its key before that exact constant-time check succeeds.
  let environment: StudioEnvironment;
  try {
    environment = (options.loadEnvironment ?? loadStudioEnvironment)();
  } catch {
    return privateEmptyResponse(503);
  }
  if (!authorizeStudioDrain(request, environment.cronSecret)) return privateEmptyResponse(401);
  if (!environment.gmiApiKey) return privateEmptyResponse(503);

  let client: StudioProviderAuditClient;
  try {
    client = (options.createClient ?? createGmiProviderAuditClient)({
      apiKey: environment.gmiApiKey,
      ...(environment.gmiOrganizationId === undefined ? {} : { organizationId: environment.gmiOrganizationId }),
    });
  } catch {
    return privateEmptyResponse(503);
  }

  let catalog: GmiJsonValue;
  let detail: GmiJsonValue;
  try {
    // The intentional fixed call order makes the operator audit trace easy to
    // reason about. No other client method is available through this type.
    catalog = await client.listModels();
    detail = await client.getModelDetail(GMI_PROVIDER_AUDIT_MODEL_ID);
  } catch {
    return privateEmptyResponse(503);
  }

  return Response.json(sanitizeProviderAudit(catalog, detail), {
    headers: { "cache-control": "no-store" },
  });
}

export function sanitizeProviderAudit(
  catalog: GmiJsonValue,
  detail: GmiJsonValue,
): StudioProviderAuditResponse {
  return Object.freeze({
    modelId: GMI_PROVIDER_AUDIT_MODEL_ID,
    available: catalogContainsExactModel(catalog, GMI_PROVIDER_AUDIT_MODEL_ID),
    catalogFingerprint: sha256Fingerprint(catalog),
    schemaFingerprint: sha256Fingerprint(detail),
    parameters: Object.freeze(extractSanitizedParameters(detail)),
    // Public docs are not an authorization to use a price. The account's
    // console must still be reviewed manually before any registry update.
    pricing: "manual_console_required" as const,
  });
}

function createGmiProviderAuditClient(input: {
  readonly apiKey: string;
  readonly organizationId?: string | undefined;
}): StudioProviderAuditClient {
  return new GmiRequestQueueClient(input);
}

function catalogContainsExactModel(catalog: GmiJsonValue, modelId: string): boolean {
  const root = object(catalog);
  if (!root) return false;
  const candidates = [root.models, root.data, root.items];
  for (const candidate of candidates) {
    if (!Array.isArray(candidate)) continue;
    for (const entry of candidate) {
      if (entry === modelId) return true;
      const model = object(entry);
      if (!model) continue;
      // Only explicit identifier fields count. Descriptions and arbitrary
      // provider text are deliberately never searched or returned.
      if (model.id === modelId || model.model_id === modelId || model.model === modelId || model.slug === modelId) {
        return true;
      }
    }
  }
  return false;
}

function extractSanitizedParameters(detail: GmiJsonValue): StudioProviderAuditParameter[] {
  const root = object(detail);
  if (!root) return [];

  const candidates: readonly GmiJsonObject[] = [
    root,
    root.input_schema,
    root.inputSchema,
    root.request_schema,
    root.requestSchema,
    root.payload_schema,
    root.payloadSchema,
    root.schema,
  ].flatMap((candidate) => {
    const value = object(candidate);
    return value ? [value] : [];
  });

  for (const candidate of candidates) {
    const parameters = parametersFromCandidate(candidate);
    if (parameters.length > 0) return parameters;
  }
  return [];
}

function parametersFromCandidate(candidate: GmiJsonObject): StudioProviderAuditParameter[] {
  const required = requiredNames(candidate.required);
  const parameterArray = candidate.parameters;
  if (Array.isArray(parameterArray)) {
    const result: StudioProviderAuditParameter[] = [];
    for (const parameter of parameterArray) {
      const entry = object(parameter);
      if (!entry || typeof entry.name !== "string" || !isSafeParameterName(entry.name)) continue;
      result.push(frozenParameter({
        name: entry.name,
        declaredType: normalizedDeclaredType(entry.type),
        required: entry.required === true || required.has(entry.name),
      }));
    }
    return boundedUniqueParameters(result);
  }

  const mapping = object(candidate.parameters) ?? object(candidate.properties);
  if (!mapping) return [];
  const result: StudioProviderAuditParameter[] = [];
  for (const name of Object.keys(mapping).sort()) {
    if (!isSafeParameterName(name)) continue;
    const definition = object(mapping[name]);
    result.push(frozenParameter({
      name,
      declaredType: normalizedDeclaredType(definition?.type),
      required: definition?.required === true || required.has(name),
    }));
  }
  return boundedUniqueParameters(result);
}

function boundedUniqueParameters(
  parameters: readonly StudioProviderAuditParameter[],
): StudioProviderAuditParameter[] {
  const selected = new Map<string, StudioProviderAuditParameter>();
  for (const parameter of parameters) {
    if (!selected.has(parameter.name)) selected.set(parameter.name, parameter);
  }
  return [...selected.values()]
    .sort((left, right) => left.name.localeCompare(right.name))
    .slice(0, MAX_PROVIDER_AUDIT_PARAMETERS);
}

function requiredNames(value: GmiJsonValue | undefined): ReadonlySet<string> {
  if (!Array.isArray(value)) return new Set();
  return new Set(value.filter((name): name is string => typeof name === "string" && isSafeParameterName(name)));
}

function normalizedDeclaredType(value: GmiJsonValue | undefined): StudioProviderAuditDeclaredType {
  switch (value) {
    case "string":
    case "number":
    case "integer":
    case "boolean":
    case "object":
    case "array":
      return value;
    default:
      return "unknown";
  }
}

function isSafeParameterName(value: string): boolean {
  return /^[A-Za-z][A-Za-z0-9_.-]{0,63}$/u.test(value);
}

function frozenParameter(value: StudioProviderAuditParameter): StudioProviderAuditParameter {
  return Object.freeze(value);
}

function object(value: GmiJsonValue | undefined): GmiJsonObject | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as GmiJsonObject
    : undefined;
}

/** Stable key ordering means equivalent provider JSON produces the same evidence digest. */
function sha256Fingerprint(value: GmiJsonValue): string {
  return `sha256:${createHash("sha256").update(canonicalJson(value), "utf8").digest("hex")}`;
}

function canonicalJson(value: GmiJsonValue): string {
  if (value === null || typeof value === "boolean" || typeof value === "number" || typeof value === "string") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const objectValue = value as GmiJsonObject;
  return `{${Object.keys(objectValue).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(objectValue[key]!)}`).join(",")}}`;
}

function methodNotAllowed(): Response {
  return new Response(null, { status: 405, headers: { allow: "GET", "cache-control": "no-store" } });
}

function privateEmptyResponse(status: 401 | 503): Response {
  return new Response(null, { status, headers: { "cache-control": "no-store" } });
}
