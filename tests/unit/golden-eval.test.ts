import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";
import {
  GOLDEN_EVALUATION_QUESTION_COUNT,
  GOLDEN_EVALUATION_TOP_K,
  MUSICRAG_GOLDEN_FIXTURE_SHA256,
  meetsRecordedMusicRagBaseline,
  mrrAt10,
  ndcgAt10,
  parseGoldenEvaluationJsonl,
  recallAt10,
  runGoldenEvaluation,
  summarizeGoldenEvaluation,
} from "../../lib/golden-eval.js";

const fixturePath = fileURLToPath(new URL("../../eval/musicrag-golden.jsonl", import.meta.url));

test("Air-owned fixture preserves the supplied 40-question, 12-channel MusicRAG coverage", async () => {
  const contents = await readFile(fixturePath, "utf8");
  const questions = parseGoldenEvaluationJsonl(contents);

  expect(createHash("sha256").update(contents).digest("hex")).toBe(MUSICRAG_GOLDEN_FIXTURE_SHA256);
  expect(questions).toHaveLength(GOLDEN_EVALUATION_QUESTION_COUNT);
  expect(new Set(questions.map((question) => question.channel))).toHaveLength(12);
  expect(questions[0]).toMatchObject({
    question: "How did Andrew Watt become a go-to producer for major artists?",
    expectedVideoIds: ["KIYLFqTweYs"],
  });
  expect(questions.at(-1)).toMatchObject({
    expectedVideoIds: ["VEdX9D_ZFTY"],
  });
});

test("MusicRAG metrics use unique expected episodes for recall and nDCG, but first rank for MRR", () => {
  const results = [
    { video_id: "wrong" },
    { video_id: "expected-a" },
    { video_id: "expected-a" },
    { video_id: "expected-b" },
  ];

  expect(recallAt10(results, ["expected-a", "expected-b"])).toBe(1);
  expect(mrrAt10(results, ["expected-a", "expected-b"])).toBe(0.5);
  expect(ndcgAt10(results, ["expected-a", "expected-b"])).toBeCloseTo(0.6509, 4);
  expect(GOLDEN_EVALUATION_TOP_K).toBe(10);
});

test("offline harness runs injected rankings sequentially and evaluates the aggregate threshold", async () => {
  const questions = parseGoldenEvaluationJsonl(await readFile(fixturePath, "utf8"));
  const calls: string[] = [];
  const rows = await runGoldenEvaluation(questions, async (question) => {
    calls.push(question.question);
    return question.expectedVideoIds.map((video_id) => ({ video_id }));
  });
  const summary = summarizeGoldenEvaluation(rows);

  expect(calls).toEqual(questions.map((question) => question.question));
  expect(summary).toEqual({ questions: 40, recallAt10: 1, mrrAt10: 1, ndcgAt10: 1 });
  expect(meetsRecordedMusicRagBaseline(summary)).toBe(true);
  expect(
    meetsRecordedMusicRagBaseline({
      questions: 40,
      recallAt10: 0.9624,
      mrrAt10: 0.955,
      ndcgAt10: 0.947,
    }),
  ).toBe(false);
});

test("parser rejects a shortened fixture rather than silently weakening the regression gate", () => {
  expect(() => parseGoldenEvaluationJsonl('{"q":"Only one","expected_video_ids":["x"],"channel":"Test"}\n')).toThrow(
    "expected exactly 40",
  );
});
