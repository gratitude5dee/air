import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";
import {
  meetsRecordedMusicRagBaseline,
  parseGoldenEvaluationJsonl,
  runGoldenEvaluation,
  summarizeGoldenEvaluation,
} from "../../lib/golden-eval.js";
import { runMusicRagRetrieval } from "../../lib/retrieval.js";

const fixturePath = fileURLToPath(new URL("../../eval/musicrag-golden.jsonl", import.meta.url));
const RUN_LIVE_GOLDEN_EVAL = process.env.RUN_LIVE_GOLDEN_EVAL === "1";
const liveTest = RUN_LIVE_GOLDEN_EVAL ? test : test.skip;

liveTest(
  "runs the supplied MusicRAG golden retrieval evaluation against approved live credentials",
  async () => {
    const missing = ["MONGODB_URI", "MONGODB_DB", "VOYAGE_API_KEY"].filter(
      (name) => !process.env[name]?.trim(),
    );
    if (missing.length > 0) {
      throw new Error(`Live golden evaluation requires ${missing.join(", ")}`);
    }

    const questions = parseGoldenEvaluationJsonl(await readFile(fixturePath, "utf8"));
    // Match the source agent evaluation: channel is coverage metadata, not an
    // injected filter. This exercises Air's actual query-planning path.
    const rows = await runGoldenEvaluation(questions, async (question) => {
      const result = await runMusicRagRetrieval(question.question);
      return result.docs;
    });
    const summary = summarizeGoldenEvaluation(rows);

    // Do not write a report: it avoids committing unverified metrics or corpus
    // details. The summary is sufficient for the credentialed operator log.
    process.stdout.write(`${JSON.stringify({ evaluation: "musicrag-golden", summary })}\n`);
    expect(rows).toHaveLength(40);
    expect(meetsRecordedMusicRagBaseline(summary)).toBe(true);
  },
  600_000,
);
