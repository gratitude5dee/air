import { access, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const summaryPath = resolve(".eve/agent-summary.json");
const adapterInternalPath = "/_internal/chat-sdk/imessage";
const drainCron = "* * * * *";

/**
 * Eve's summary is the application-owned route registry. Keep it closed: the
 * Vercel Build Output contains framework plumbing (including the opaque cron
 * callback), but it must not gain another Air HTTP dispatch path.
 */
const expectedChannels = [
  // The official adapter receiver remains at this intentionally unregistered
  // path. Photon is configured only for Air's durable ingress; without bridge
  // dispatch handlers, a signed delivery here cannot start an Eve session.
  { name: "imessage", method: "GET", urlPath: adapterInternalPath },
  { name: "imessage", method: "POST", urlPath: adapterInternalPath },
  { name: "ingress", method: "GET", urlPath: "/healthz" },
  { name: "ingress", method: "POST", urlPath: "/webhooks/imessage" },
  { name: "ingress", method: "POST", urlPath: "/internal/drain-inbox" },
];

function routeKey(route) {
  return `${route?.name ?? "<unknown>"} ${route?.method ?? "<unknown>"} ${route?.urlPath ?? "<unknown>"}`;
}

function requiredArray(summary, field) {
  const value = summary?.[field];
  if (!Array.isArray(value)) {
    throw new Error(`compiled Eve build is missing its ${field} manifest array`);
  }
  return value;
}

function verifyNoDeclarations(summary, field) {
  const declarations = requiredArray(summary, field);
  if (declarations.length !== 0) {
    throw new Error(`compiled Eve build must not declare ${field}`);
  }
}

function verifySchedules(summary) {
  const schedules = requiredArray(summary, "schedules");
  if (schedules.length !== 1) {
    throw new Error("compiled Eve build must expose exactly one recovery schedule");
  }
  const schedule = schedules[0];
  if (
    schedule?.name !== "drain-inbox" ||
    schedule?.logicalPath !== "schedules/drain-inbox.ts" ||
    schedule?.cron !== drainCron
  ) {
    throw new Error(`compiled Eve recovery schedule must be drain-inbox at ${drainCron}`);
  }
}

function verifyChannelSurface(summary) {
  const channels = requiredArray(summary, "channels");
  if (channels.length !== expectedChannels.length) {
    throw new Error("compiled Eve build exposes an unexpected number of channel routes");
  }

  const expected = new Set(expectedChannels.map(routeKey));
  const actualCounts = new Map();
  for (const channel of channels) {
    const key = routeKey(channel);
    if (!expected.has(key)) {
      throw new Error(`compiled Eve build exposes an unexpected channel route: ${key}`);
    }
    actualCounts.set(key, (actualCounts.get(key) ?? 0) + 1);
  }
  for (const route of expectedChannels) {
    const key = routeKey(route);
    if (actualCounts.get(key) !== 1) {
      throw new Error(`compiled Eve build must register ${key} exactly once`);
    }
  }
}

async function verifyVercelFunctionDuration() {
  if (!process.env.VERCEL) return;
  const candidates = [
    ".vercel/output/functions/__server.func/.vc-config.json",
    // Eve uses this service-prefixed path only in co-deployed setups; accept
    // it so the assertion remains valid if this project is later composed.
    ".vercel/output/functions/eve/__server.func/.vc-config.json",
  ];
  let configPath;
  for (const candidate of candidates) {
    try {
      await access(candidate);
      configPath = candidate;
      break;
    } catch {
      // Try the alternate Eve Build Output layout.
    }
  }
  if (!configPath) throw new Error("Eve Vercel server function config is unavailable");
  const config = JSON.parse(await readFile(configPath, "utf8"));
  if (config?.maxDuration !== 60) {
    throw new Error("Eve Vercel server function duration is not 60 seconds");
  }
  if (config?.runtime !== "nodejs24.x") {
    throw new Error("Eve Vercel server function is not using the required Node.js 24 runtime");
  }
}

async function resolveVercelServerEntry() {
  const candidates = [
    ".vercel/output/functions/__server.func/index.mjs",
    // Keep this in lockstep with the co-deployed output layout accepted above.
    ".vercel/output/functions/eve/__server.func/index.mjs",
  ];
  for (const candidate of candidates) {
    try {
      await access(candidate);
      return resolve(candidate);
    } catch {
      // Try the alternate Eve Build Output layout.
    }
  }
  throw new Error("Eve Vercel server function entry is unavailable");
}

/**
 * The Eve framework also declares GET / for its package-name-derived landing
 * page. Air deliberately registers a supported Nitro handler first; execute
 * the actual Vercel function to prove that the public route remains branded
 * and does not regress to the framework home page.
 */
async function verifyVercelPublicStatusRoute() {
  if (!process.env.VERCEL) return;
  const entry = await resolveVercelServerEntry();
  const module = await import(pathToFileURL(entry).href);
  const handler = module?.default;
  if (!handler || typeof handler.fetch !== "function") {
    throw new Error("Eve Vercel server function does not expose a web fetch handler");
  }
  const response = await handler.fetch(new Request("https://air.invalid/"), {
    waitUntil: () => undefined,
  });
  if (response.status !== 200 || !response.headers.get("content-type")?.includes("application/json")) {
    throw new Error("public GET / route is not the Air status response");
  }
  const body = await response.json();
  if (
    !body ||
    body.name !== "AR Agent" ||
    body.status !== "transcript-grounded iMessage research"
  ) {
    throw new Error("public GET / route is not branded as AR Agent");
  }
}

async function verifyVercelCron() {
  if (!process.env.VERCEL) return;
  let output;
  try {
    output = JSON.parse(await readFile(".vercel/output/config.json", "utf8"));
  } catch {
    throw new Error("Vercel Build Output config is unavailable or invalid");
  }
  const crons = output?.crons;
  if (!Array.isArray(crons) || crons.length !== 1) {
    throw new Error("Vercel Build Output must expose exactly one cron job");
  }
  const cron = crons[0];
  if (
    cron?.schedule !== drainCron ||
    typeof cron?.path !== "string" ||
    !cron.path.startsWith("/eve/v1/cron/")
  ) {
    throw new Error(`Vercel Build Output cron must be the Eve drain at ${drainCron}`);
  }
}

async function main() {
  let summary;
  try {
    summary = JSON.parse(await readFile(summaryPath, "utf8"));
  } catch {
    throw new Error("Eve build summary is unavailable or invalid");
  }
  const tools = requiredArray(summary, "tools");

  const toolNames = tools.map((tool) => tool?.name).filter((name) => typeof name === "string");
  if (toolNames.length !== 1 || toolNames[0] !== "answer_music_question") {
    throw new Error("compiled Eve tool surface is not limited to answer_music_question");
  }

  verifyNoDeclarations(summary, "skills");
  verifyNoDeclarations(summary, "connections");
  verifyNoDeclarations(summary, "subagents");
  verifySchedules(summary);
  verifyChannelSurface(summary);

  await verifyVercelFunctionDuration();
  await verifyVercelCron();
  await verifyVercelPublicStatusRoute();

  process.stdout.write("Eve build surface verified.\n");
}

void main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : "Eve build verification failed"}\n`);
  process.exitCode = 1;
});
