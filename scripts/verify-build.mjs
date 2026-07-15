import { access, readFile } from "node:fs/promises";
import { resolve } from "node:path";

const summaryPath = resolve(".eve/agent-summary.json");
const registrationPath = "/_internal/air-imessage-channel";
const forbiddenAdapterPaths = new Set([
  "/_internal/chat-sdk/imessage",
  "/eve/v1/imessage",
]);

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
}

async function main() {
  let summary;
  try {
    summary = JSON.parse(await readFile(summaryPath, "utf8"));
  } catch {
    throw new Error("Eve build summary is unavailable or invalid");
  }
  const channels = Array.isArray(summary?.channels) ? summary.channels : [];
  const tools = Array.isArray(summary?.tools) ? summary.tools : [];

  const toolNames = tools.map((tool) => tool?.name).filter((name) => typeof name === "string");
  if (toolNames.length !== 1 || toolNames[0] !== "answer_music_question") {
    throw new Error("compiled Eve tool surface is not limited to answer_music_question");
  }

  const registrations = channels.filter((channel) =>
    channel?.name === "imessage" &&
    channel?.method === "GET" &&
    channel?.urlPath === registrationPath,
  );
  if (registrations.length !== 1) {
    throw new Error("attempt-scoped iMessage receiver is not registered exactly once");
  }

  const exposedAdapterRoute = channels.find((channel) =>
    forbiddenAdapterPaths.has(channel?.urlPath),
  );
  if (exposedAdapterRoute) {
    throw new Error("compiled build exposes a forbidden Chat SDK iMessage webhook route");
  }

  const unexpectedIMessageRoute = channels.find((channel) =>
    channel?.name === "imessage" &&
    channel?.urlPath !== registrationPath,
  );
  if (unexpectedIMessageRoute) {
    throw new Error("compiled build exposes an unexpected iMessage channel route");
  }

  await verifyVercelFunctionDuration();

  process.stdout.write("Eve build surface verified.\n");
}

void main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : "Eve build verification failed"}\n`);
  process.exitCode = 1;
});
