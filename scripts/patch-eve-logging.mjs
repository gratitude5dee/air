import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Eve v0.24.3 logs provider error objects verbatim, including their inspected
 * request/response bodies, to both console and the active OTel span. Air
 * processes private iMessage content, so fail closed if Eve's internal shape
 * changes rather than silently deploying without this redaction boundary.
 */
const PATCH_ID = "air-eve-logging-redaction-v1";
const EXPECTED_EVE_VERSION = "0.24.3";
const projectRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const evePackagePath = join(projectRoot, "node_modules", "eve", "package.json");
const loggingPath = join(projectRoot, "node_modules", "eve", "dist", "src", "internal", "logging.js");

const replacements = [
  {
    name: "formatError",
    before:
      "function formatError(e,t){let n={errorId:t??createErrorId(),message:extractErrorMessage(e)},r=extractErrorName(e);return r!==void 0&&(n.name=r),n.detail=inspectError(e),n}",
    after:
      "function formatError(e,t){/* air-eve-logging-redaction-v1 */return{errorId:t??createErrorId(),name:`EveRuntimeError`,message:`An internal runtime error occurred.`,detail:`error details redacted`}}",
  },
  {
    name: "recordErrorOnSpan",
    before:
      "function recordErrorOnSpan(e,t){let n=t instanceof Error?t.message:getErrorMessage(t),i=t instanceof Error?t.name:`Error`;e.setStatus({code:SpanStatusCode.ERROR,message:n}),e.recordException({message:n,name:i,stack:inspectError(t)})}",
    after:
      "function recordErrorOnSpan(e,t){/* air-eve-logging-redaction-v1 */e.setStatus({code:SpanStatusCode.ERROR,message:`An internal runtime error occurred.`}),e.recordException({message:`An internal runtime error occurred.`,name:`EveRuntimeError`,stack:`error details redacted`})}",
  },
  {
    name: "write",
    before:
      "function write(e,t,n,r){if(LEVEL_SEVERITY[e]<resolveThreshold())return;let i=e===`error`?console.error:e===`warn`?console.warn:console.log,a=`[eve:${t}] ${n}`;if(r===void 0){i(a);return}i(a,renderFields(r))}",
    after:
      "function write(e,t,n,r){if(LEVEL_SEVERITY[e]<resolveThreshold())return;let i=e===`error`?console.error:e===`warn`?console.warn:console.log,a=`[eve:${t}] ${e===`error`?`An internal runtime error occurred.`:n}`;if(e===`error`||r===void 0){i(a);return}i(a,renderFields(r))}",
  },
  {
    name: "renderFields",
    before:
      "function renderFields(e){let t={};for(let[n,r]of Object.entries(e))if(r!==void 0){if(r instanceof Error){t[n]=formatError(r);continue}t[n]=r}return t}",
    after:
      "function renderFields(e){let t={};for(let[n,r]of Object.entries(e))if(r!==void 0){if(r instanceof Error||n===`error`){t[n]=formatError(r);continue}t[n]=r}return t}",
  },
  {
    name: "recordOnActiveSpan",
    before:
      "function recordOnActiveSpan(e,t){let n=trace.getActiveSpan();if(n===void 0)return;let a=t?.error;if(a instanceof Error){recordErrorOnSpan(n,a);return}if(isFormattedError(a)){n.setStatus({code:SpanStatusCode.ERROR,message:a.message}),n.recordException({message:a.message,name:typeof a.name==`string`?a.name:`Error`,stack:typeof a.detail==`string`?a.detail:void 0});return}n.addEvent(e,t?renderFields(t):void 0)}",
    after:
      "function recordOnActiveSpan(e,t){let n=trace.getActiveSpan();if(n===void 0)return;recordErrorOnSpan(n,t?.error)}",
  },
  {
    name: "inspectError",
    before:
      "function inspectError(t){return truncate(inspect(t,{breakLength:1/0,compact:!1,depth:10,maxStringLength:8192}),16384)}",
    after:
      "function inspectError(t){/* air-eve-logging-redaction-v1 */return`error details redacted`}",
  },
];

// This is the only previously emitted patch shape. Keeping its exact migration
// makes a local re-run safe without accepting arbitrary modified dependencies.
const legacyReplacements = [
  {
    name: "formatError legacy patch",
    before:
      "function formatError(e,t){/* air-eve-logging-redaction-v1 */return{errorId:t??extractErrorId(e)??createErrorId(),name:`EveRuntimeError`,message:`An internal runtime error occurred.`,detail:`error details redacted`}}",
    after: replacements[0].after,
  },
  {
    name: "write legacy patch",
    before:
      "function write(e,t,n,r){if(LEVEL_SEVERITY[e]<resolveThreshold())return;let i=e===`error`?console.error:e===`warn`?console.warn:console.log,a=`[eve:${t}] ${e===`error`&&r!==void 0&&Object.hasOwn(r,`error`)?`An internal runtime error occurred.`:n}`;if(r===void 0){i(a);return}i(a,renderFields(r))}",
    after: replacements[2].after,
  },
  {
    name: "write field-bearing redaction patch",
    before:
      "function write(e,t,n,r){if(LEVEL_SEVERITY[e]<resolveThreshold())return;let i=e===`error`?console.error:e===`warn`?console.warn:console.log,a=`[eve:${t}] ${e===`error`?`An internal runtime error occurred.`:n}`;if(r===void 0){i(a);return}i(a,renderFields(r))}",
    after: replacements[2].after,
  },
  {
    name: "active-span field-bearing redaction patch",
    before:
      "function recordOnActiveSpan(e,t){let n=trace.getActiveSpan();if(n===void 0)return;let a=t?.error;if(a!==void 0){recordErrorOnSpan(n,a);return}n.addEvent(e,t?renderFields(t):void 0)}",
    after: replacements[4].after,
  },
];

function occurrenceCount(source, fragment) {
  return source.split(fragment).length - 1;
}

function fail(message) {
  throw new Error(`[${PATCH_ID}] ${message}`);
}

function assertInstalledEveVersion() {
  let pkg;
  try {
    pkg = JSON.parse(readFileSync(evePackagePath, "utf8"));
  } catch (error) {
    fail(`could not read Eve package metadata at ${evePackagePath}: ${String(error)}`);
  }
  if (pkg.version !== EXPECTED_EVE_VERSION) {
    fail(`expected eve@${EXPECTED_EVE_VERSION}, found eve@${String(pkg.version)}. Review and update this patch before installing.`);
  }
}

function patchEveLogging() {
  assertInstalledEveVersion();

  let source;
  try {
    source = readFileSync(loggingPath, "utf8");
  } catch (error) {
    fail(`could not read Eve logging module at ${loggingPath}: ${String(error)}`);
  }

  let migrated = false;
  for (const { name, before, after } of legacyReplacements) {
    const count = occurrenceCount(source, before);
    if (count > 1) fail(`found ${count} ${name} implementations; refusing an ambiguous install.`);
    if (count === 1) {
      source = source.replace(before, after);
      migrated = true;
    }
  }

  const patchedCount = replacements.filter(({ after }) => source.includes(after)).length;
  const hasMarker = source.includes(`/* ${PATCH_ID} */`);
  if (patchedCount === replacements.length && hasMarker) {
    if (replacements.some(({ before }) => source.includes(before))) {
      fail("found both unredacted and patched logging implementations; refusing an ambiguous install.");
    }
    if (migrated) writeFileSync(loggingPath, source, "utf8");
    console.log(`[${PATCH_ID}] Eve logging redaction already applied.`);
    return;
  }
  if (patchedCount > 0 || hasMarker) {
    fail("found a partial or unknown Eve logging patch; refusing to continue.");
  }

  let patched = source;
  for (const { name, before, after } of replacements) {
    const count = occurrenceCount(patched, before);
    if (count !== 1) {
      fail(`expected exactly one Eve ${name} implementation, found ${count}. Review this patch before installing.`);
    }
    patched = patched.replace(before, after);
  }

  if (
    !patched.includes(`/* ${PATCH_ID} */`) ||
    replacements.some(({ after }) => !patched.includes(after)) ||
    replacements.some(({ before }) => patched.includes(before))
  ) {
    fail("post-patch verification failed; Eve logging was not modified safely.");
  }

  writeFileSync(loggingPath, patched, "utf8");
  console.log(`[${PATCH_ID}] Applied Eve logging redaction.`);
}

patchEveLogging();
