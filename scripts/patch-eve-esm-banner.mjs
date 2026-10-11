import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Eve v0.24.3's node-esm-compat banner emits `__filename`/`__dirname` shims
 * from `import.meta.url`. Cloudflare Workers set `import.meta.url` to
 * undefined in deployed modules, so the emitted `fileURLToPath` call throws
 * TypeError at worker startup (CF error 10021) before any route can run.
 * This patch makes the emitted shim tolerate a missing URL. Fail closed if
 * Eve's internal shape changes rather than silently deploying a broken
 * worker.
 */
const PATCH_ID = "air-eve-esm-banner-v1";
const EXPECTED_EVE_VERSION = "0.24.3";
const projectRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const evePackagePath = join(projectRoot, "node_modules", "eve", "package.json");
const bannerPath = join(
  projectRoot,
  "node_modules",
  "eve",
  "dist",
  "src",
  "internal",
  "node-esm-compat-banner.js",
);

const replacements = [
  {
    name: "filenameDeclaration",
    before: "declarationLine:`const __filename = __eveFileURLToPath(import.meta.url);`",
    after:
      'declarationLine:`const __filename = __eveFileURLToPath(import.meta.url || "file:///index.mjs");/* air-eve-esm-banner-v1 */`',
  },
  {
    name: "requireDeclaration",
    before: "declarationLine:`const require = __eveCreateRequire(import.meta.url);`",
    after:
      'declarationLine:`const require = __eveCreateRequire(import.meta.url || "file:///index.mjs");`',
  },
];

function fail(message) {
  console.error(`[${PATCH_ID}] ${message}`);
  process.exit(1);
}

function occurrenceCount(source, needle) {
  return source.split(needle).length - 1;
}

function patchEveEsmBanner() {
  const { version } = JSON.parse(readFileSync(evePackagePath, "utf8"));
  if (version !== EXPECTED_EVE_VERSION) {
    fail(`expected eve@${EXPECTED_EVE_VERSION}, found ${version}. Review this patch before upgrading.`);
  }

  const source = readFileSync(bannerPath, "utf8");

  const patchedCount = replacements.filter(({ after }) => source.includes(after)).length;
  const hasMarker = source.includes(PATCH_ID);
  if (patchedCount === replacements.length && hasMarker) {
    if (replacements.some(({ before }) => source.includes(before))) {
      fail("found both unpatched and patched esm banner implementations; refusing an ambiguous install.");
    }
    console.log(`[${PATCH_ID}] Eve esm banner patch already applied.`);
    return;
  }
  if (patchedCount > 0 || hasMarker) {
    fail("found a partial or unknown Eve esm banner patch; refusing to continue.");
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
    !patched.includes(PATCH_ID) ||
    replacements.some(({ after }) => !patched.includes(after)) ||
    replacements.some(({ before }) => patched.includes(before))
  ) {
    fail("post-patch verification failed; Eve esm banner was not modified safely.");
  }

  writeFileSync(bannerPath, patched, "utf8");
  console.log(`[${PATCH_ID}] Applied Eve esm banner patch.`);
}

patchEveEsmBanner();
