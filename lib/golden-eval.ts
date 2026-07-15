import type { CorpusDocument } from "./types.js";

/**
 * Provenance: metric definitions and fixture shape are ported from
 * `/Users/gratitud3/Downloads/musicindustry/musicRAG/musicrag/eval/run_eval.py`.
 * The immutable input fixture is documented in `eval/README.md`.
 */
export const GOLDEN_EVALUATION_QUESTION_COUNT = 40;
export const GOLDEN_EVALUATION_MIN_CHANNELS = 12;
export const GOLDEN_EVALUATION_TOP_K = 10;
/** SHA-256 of the verbatim source snapshot, asserted by the offline unit test. */
export const MUSICRAG_GOLDEN_FIXTURE_SHA256 =
  "b14df08a798394d46d1bfb3d602ec9eb0856581d36c02255ddeefe72b04c787c";

/**
 * Recorded in the Air execution goal from the supplied MusicRAG agent eval.
 * These values are thresholds, not a claim about an unrun Air deployment.
 */
export const RECORDED_MUSICRAG_AGENT_BASELINE = Object.freeze({
  recallAt10: 0.963,
  mrrAt10: 0.955,
  ndcgAt10: 0.947,
});

export type GoldenQuestion = Readonly<{
  question: string;
  expectedVideoIds: readonly string[];
  channel: string;
  notes?: string;
}>;

export type GoldenMetrics = Readonly<{
  recallAt10: number;
  mrrAt10: number;
  ndcgAt10: number;
}>;

export type GoldenEvaluationRow = Readonly<{
  question: GoldenQuestion;
  metrics: GoldenMetrics;
  topVideoIds: readonly string[];
}>;

export type GoldenEvaluationSummary = Readonly<{
  questions: number;
  recallAt10: number;
  mrrAt10: number;
  ndcgAt10: number;
}>;

export type GoldenRetriever = (
  question: GoldenQuestion,
) => Promise<readonly CorpusDocument[]> | readonly CorpusDocument[];

type GoldenJsonlRow = {
  q?: unknown;
  expected_video_ids?: unknown;
  channel?: unknown;
  notes?: unknown;
};

function metricNumber(value: number): number {
  return Math.round(value * 1_000) / 1_000;
}

function videoId(document: CorpusDocument): string | undefined {
  const value = document.video_id;
  return typeof value === "string" && value.trim() ? value : undefined;
}

function parseJsonlRow(line: string, lineNumber: number): GoldenQuestion {
  let parsed: GoldenJsonlRow;
  try {
    parsed = JSON.parse(line) as GoldenJsonlRow;
  } catch {
    throw new Error(`Golden evaluation line ${lineNumber} is not valid JSON`);
  }
  if (typeof parsed.q !== "string" || !parsed.q.trim()) {
    throw new Error(`Golden evaluation line ${lineNumber} must include a non-empty q`);
  }
  if (
    !Array.isArray(parsed.expected_video_ids) ||
    parsed.expected_video_ids.length === 0 ||
    parsed.expected_video_ids.some((id) => typeof id !== "string" || !id.trim())
  ) {
    throw new Error(
      `Golden evaluation line ${lineNumber} must include non-empty expected_video_ids`,
    );
  }
  if (typeof parsed.channel !== "string" || !parsed.channel.trim()) {
    throw new Error(`Golden evaluation line ${lineNumber} must include a non-empty channel`);
  }
  if (parsed.notes !== undefined && typeof parsed.notes !== "string") {
    throw new Error(`Golden evaluation line ${lineNumber} notes must be a string`);
  }
  return {
    question: parsed.q,
    expectedVideoIds: [...new Set(parsed.expected_video_ids)],
    channel: parsed.channel,
    ...(typeof parsed.notes === "string" ? { notes: parsed.notes } : {}),
  };
}

/** Parse and strictly pin the Air-owned copy of the supplied MusicRAG fixture. */
export function parseGoldenEvaluationJsonl(contents: string): GoldenQuestion[] {
  const rows = contents
    .split(/\r?\n/)
    .map((line, index) => ({ line: line.trim(), lineNumber: index + 1 }))
    .filter(({ line }) => line.length > 0)
    .map(({ line, lineNumber }) => parseJsonlRow(line, lineNumber));
  validateGoldenEvaluation(rows);
  return rows;
}

/** Prevent a quiet fixture reduction or loss of broad channel coverage. */
export function validateGoldenEvaluation(questions: readonly GoldenQuestion[]): void {
  if (questions.length !== GOLDEN_EVALUATION_QUESTION_COUNT) {
    throw new Error(
      `Golden evaluation has ${questions.length} questions; expected exactly ${GOLDEN_EVALUATION_QUESTION_COUNT}`,
    );
  }
  const questionTexts = new Set<string>();
  const channels = new Set<string>();
  for (const [index, question] of questions.entries()) {
    if (!question.question.trim()) {
      throw new Error(`Golden evaluation row ${index + 1} has an empty question`);
    }
    if (questionTexts.has(question.question)) {
      throw new Error(`Golden evaluation row ${index + 1} repeats a question`);
    }
    questionTexts.add(question.question);
    if (!question.channel.trim()) {
      throw new Error(`Golden evaluation row ${index + 1} has an empty channel`);
    }
    channels.add(question.channel);
    if (question.expectedVideoIds.length === 0 || question.expectedVideoIds.some((id) => !id.trim())) {
      throw new Error(`Golden evaluation row ${index + 1} has no expected video IDs`);
    }
  }
  if (channels.size < GOLDEN_EVALUATION_MIN_CHANNELS) {
    throw new Error(
      `Golden evaluation spans ${channels.size} channels; expected at least ${GOLDEN_EVALUATION_MIN_CHANNELS}`,
    );
  }
}

/** Recall is episode-level and therefore deduplicates repeated chunks. */
export function recallAt10(
  results: readonly CorpusDocument[],
  expectedVideoIds: readonly string[],
): number {
  const expected = new Set(expectedVideoIds);
  if (expected.size === 0) return 0;
  const found = new Set<string>();
  for (const document of results.slice(0, GOLDEN_EVALUATION_TOP_K)) {
    const id = videoId(document);
    if (id && expected.has(id)) found.add(id);
  }
  return found.size / expected.size;
}

/** MRR uses the first relevant ranked chunk, matching the supplied evaluator. */
export function mrrAt10(
  results: readonly CorpusDocument[],
  expectedVideoIds: readonly string[],
): number {
  const expected = new Set(expectedVideoIds);
  for (const [index, document] of results.slice(0, GOLDEN_EVALUATION_TOP_K).entries()) {
    const id = videoId(document);
    if (id && expected.has(id)) return 1 / (index + 1);
  }
  return 0;
}

/** nDCG gives each expected episode credit once, even if it has many chunks. */
export function ndcgAt10(
  results: readonly CorpusDocument[],
  expectedVideoIds: readonly string[],
): number {
  const expected = new Set(expectedVideoIds);
  if (expected.size === 0) return 0;
  let dcg = 0;
  const seen = new Set<string>();
  for (const [index, document] of results.slice(0, GOLDEN_EVALUATION_TOP_K).entries()) {
    const id = videoId(document);
    if (!id || !expected.has(id) || seen.has(id)) continue;
    dcg += 1 / Math.log2(index + 2);
    seen.add(id);
  }
  const idealHits = Math.min(expected.size, GOLDEN_EVALUATION_TOP_K);
  let ideal = 0;
  for (let rank = 1; rank <= idealHits; rank += 1) {
    ideal += 1 / Math.log2(rank + 1);
  }
  return ideal === 0 ? 0 : dcg / ideal;
}

export function evaluateGoldenRanking(
  question: GoldenQuestion,
  results: readonly CorpusDocument[],
): GoldenEvaluationRow {
  return {
    question,
    metrics: {
      recallAt10: recallAt10(results, question.expectedVideoIds),
      mrrAt10: mrrAt10(results, question.expectedVideoIds),
      ndcgAt10: ndcgAt10(results, question.expectedVideoIds),
    },
    topVideoIds: results
      .slice(0, GOLDEN_EVALUATION_TOP_K)
      .flatMap((document) => {
        const id = videoId(document);
        return id ? [id] : [];
      }),
  };
}

/** Sequential execution keeps the live corpus/Voyage run bounded and attributable. */
export async function runGoldenEvaluation(
  questions: readonly GoldenQuestion[],
  retrieve: GoldenRetriever,
): Promise<GoldenEvaluationRow[]> {
  validateGoldenEvaluation(questions);
  const rows: GoldenEvaluationRow[] = [];
  for (const question of questions) {
    const results = await retrieve(question);
    rows.push(evaluateGoldenRanking(question, results));
  }
  return rows;
}

export function summarizeGoldenEvaluation(
  rows: readonly GoldenEvaluationRow[],
): GoldenEvaluationSummary {
  if (rows.length === 0) {
    return { questions: 0, recallAt10: 0, mrrAt10: 0, ndcgAt10: 0 };
  }
  const total = rows.reduce(
    (sum, row) => ({
      recallAt10: sum.recallAt10 + row.metrics.recallAt10,
      mrrAt10: sum.mrrAt10 + row.metrics.mrrAt10,
      ndcgAt10: sum.ndcgAt10 + row.metrics.ndcgAt10,
    }),
    { recallAt10: 0, mrrAt10: 0, ndcgAt10: 0 },
  );
  return {
    questions: rows.length,
    recallAt10: total.recallAt10 / rows.length,
    mrrAt10: total.mrrAt10 / rows.length,
    ndcgAt10: total.ndcgAt10 / rows.length,
  };
}

/** The published threshold has three decimals, so compare at that precision. */
export function meetsRecordedMusicRagBaseline(summary: GoldenEvaluationSummary): boolean {
  return (
    metricNumber(summary.recallAt10) >= RECORDED_MUSICRAG_AGENT_BASELINE.recallAt10 &&
    metricNumber(summary.mrrAt10) >= RECORDED_MUSICRAG_AGENT_BASELINE.mrrAt10 &&
    metricNumber(summary.ndcgAt10) >= RECORDED_MUSICRAG_AGENT_BASELINE.ndcgAt10
  );
}
