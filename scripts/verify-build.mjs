import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";

const ROOT = resolve(".");
const DRAIN_CRON = "* * * * *";
const EXPECTED_API_FILES = Object.freeze([
  "api/healthz.ts",
  "api/index.ts",
  "api/internal/drain-inbox.ts",
  "api/internal/readiness.ts",
  "api/webhooks/imessage.ts",
]);
const EXPECTED_REWRITES = Object.freeze([
  ["/", "/api"],
  ["/healthz", "/api/healthz"],
  ["/webhooks/imessage", "/api/webhooks/imessage"],
  ["/internal/drain-inbox", "/api/internal/drain-inbox"],
  ["/internal/readiness", "/api/internal/readiness"],
]);

/** Packages which would put the retired framework or an LLM runtime on Air's request path. */
const FORBIDDEN_MODULE_SEGMENTS = new Set([
  "eve",
  "zap",
  "ai",
  "chat",
  "openai",
  "anthropic",
  "langchain",
]);

async function readJson(path) {
  try {
    return JSON.parse(await readFile(resolve(ROOT, path), "utf8"));
  } catch (error) {
    throw new Error(`${path} is unavailable or invalid JSON: ${error instanceof Error ? error.message : "unknown error"}`);
  }
}

async function collectTypeScriptFiles(path) {
  let entries;
  try {
    entries = await readdir(resolve(ROOT, path), { withFileTypes: true });
  } catch {
    throw new Error(`${path} is unavailable`);
  }
  const files = [];
  for (const entry of entries) {
    const child = `${path}/${entry.name}`;
    if (entry.isDirectory()) {
      files.push(...await collectTypeScriptFiles(child));
    } else if (entry.isFile() && entry.name.endsWith(".ts")) {
      files.push(child);
    }
  }
  return files.sort();
}

function verifyVercelConfiguration(config) {
  if (config.installCommand !== "npm ci") {
    throw new Error("vercel.json must use reproducible npm ci installation");
  }
  if (config.buildCommand !== "npm run build") {
    throw new Error("vercel.json must use npm run build");
  }
  if (config?.functions?.["api/**/*.ts"]?.maxDuration !== 60) {
    throw new Error("every API function must declare maxDuration 60");
  }
  if (!Array.isArray(config.crons) || config.crons.length !== 1) {
    throw new Error("vercel.json must declare exactly one recovery cron");
  }
  const cron = config.crons[0];
  if (cron?.path !== "/internal/drain-inbox" || cron?.schedule !== DRAIN_CRON) {
    throw new Error(`the recovery cron must be /internal/drain-inbox at ${DRAIN_CRON}`);
  }
  if (!Array.isArray(config.rewrites) || config.rewrites.length !== EXPECTED_REWRITES.length) {
    throw new Error("vercel.json must expose exactly the Air route rewrites");
  }
  for (const [index, [source, destination]] of EXPECTED_REWRITES.entries()) {
    const rewrite = config.rewrites[index];
    if (rewrite?.source !== source || rewrite?.destination !== destination) {
      throw new Error(`rewrite ${index + 1} must map ${source} to ${destination}`);
    }
  }
}

function moduleSpecifiers(source) {
  const specifiers = [];
  const staticImport = /\b(?:import|export)\s+(?:type\s+)?(?:[\w*$\s,{}]+?\s+from\s+)?["']([^"']+)["']/gu;
  const dynamicImport = /\bimport\s*\(\s*["']([^"']+)["']\s*\)/gu;
  for (const pattern of [staticImport, dynamicImport]) {
    let match;
    while ((match = pattern.exec(source)) !== null) {
      const specifier = match[1];
      if (specifier !== undefined) specifiers.push(specifier);
    }
  }
  return specifiers;
}

function isForbiddenSpecifier(specifier) {
  const normalized = specifier.toLowerCase().replace(/^node:/u, "node/");
  return normalized
    .split(/[\\/@._-]+/u)
    .some((segment) => FORBIDDEN_MODULE_SEGMENTS.has(segment));
}

function verifyNoRetiredRuntimeDependencies(packageJson) {
  const dependencies = {
    ...(packageJson.dependencies ?? {}),
    ...(packageJson.devDependencies ?? {}),
    ...(packageJson.optionalDependencies ?? {}),
  };
  for (const name of Object.keys(dependencies)) {
    if (isForbiddenSpecifier(name)) {
      throw new Error(`package.json retains forbidden runtime dependency ${name}`);
    }
  }
}

async function verifyRouteSources() {
  const actualApiFiles = await collectTypeScriptFiles("api");
  if (actualApiFiles.length !== EXPECTED_API_FILES.length || actualApiFiles.some((file, index) => file !== EXPECTED_API_FILES[index])) {
    throw new Error("API surface differs from the approved five Air route files");
  }

  const sourceFiles = [...actualApiFiles, ...await collectTypeScriptFiles("lib/studio")];
  for (const file of sourceFiles) {
    const source = await readFile(resolve(ROOT, file), "utf8");
    for (const specifier of moduleSpecifiers(source)) {
      if (isForbiddenSpecifier(specifier)) {
        throw new Error(`${file} imports forbidden runtime module ${specifier}`);
      }
    }
    // Studio handlers process encrypted-but-sensitive user content. Logging
    // from this request path is prohibited until a separately reviewed,
    // redacting telemetry boundary exists.
    if (/\bconsole\.(?:log|info|warn|error|debug|trace)\s*\(/u.test(source)) {
      throw new Error(`${file} contains an unsafe request-path console logger`);
    }
  }

  const webhookSource = await readFile(resolve(ROOT, "api/webhooks/imessage.ts"), "utf8");
  if (!webhookSource.includes("handleStudioIMessageWebhook")) {
    throw new Error("the public webhook must use the signed Studio webhook handler");
  }
  const drainSource = await readFile(resolve(ROOT, "api/internal/drain-inbox.ts"), "utf8");
  if (!drainSource.includes("handleStudioDrain")) {
    throw new Error("the cron route must use the Studio authorization handler");
  }
}

async function verifyTypeScriptSurface() {
  const tsconfig = await readJson("tsconfig.json");
  const includes = Array.isArray(tsconfig.include) ? tsconfig.include : [];
  if (!includes.includes("api/**/*.ts") || !includes.includes("lib/studio/**/*.ts")) {
    throw new Error("tsconfig must typecheck the Vercel API shell and Studio modules");
  }
  if (includes.some((entry) => typeof entry === "string" && entry.split("/").includes("agent"))) {
    throw new Error("tsconfig must not compile retired agent runtime sources");
  }
}

async function main() {
  const [vercel, packageJson] = await Promise.all([
    readJson("vercel.json"),
    readJson("package.json"),
  ]);
  verifyVercelConfiguration(vercel);
  verifyNoRetiredRuntimeDependencies(packageJson);
  await Promise.all([verifyRouteSources(), verifyTypeScriptSurface()]);
  process.stdout.write("Air Studio Vercel surface verified.\n");
}

void main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : "Air Studio build verification failed"}\n`);
  process.exitCode = 1;
});
