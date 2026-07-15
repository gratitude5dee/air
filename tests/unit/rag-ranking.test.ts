import { expect, test } from "vitest";
import {
  diversifyByEpisode,
  fuseAndAggregate,
  uniqueEpisodeCount,
} from "../../lib/ranking.js";
import { limitContextBudget } from "../../lib/retrieval.js";

function document(videoId: string, index: number, rerank: number, rrf: number) {
  return {
    chunk_uid: `${videoId}:${index}`,
    video_id: videoId,
    chunk_index: index,
    rerank_score: rerank,
    rrf_score: rrf,
  };
}

test("episode diversification broadens a clustered source list before filling repeats", () => {
  const docs = [
    document("A", 0, 1, 1),
    document("A", 1, 0.99, 0.99),
    document("A", 2, 0.98, 0.98),
    document("B", 0, 0.9, 0.9),
    document("C", 0, 0.8, 0.8),
    document("D", 0, 0.7, 0.7),
    document("E", 0, 0.6, 0.6),
  ];
  const diversified = diversifyByEpisode(docs, 6, { maxPerEpisode: 2, minEpisodes: 5 });

  expect(diversified).toHaveLength(6);
  expect(uniqueEpisodeCount(diversified)).toBe(5);
  expect(diversified.filter((item) => item.video_id === "A")).toHaveLength(2);
});

test("rerank/RRF fusion uses the established 0.7/0.3 weighting", () => {
  const fused = fuseAndAggregate(
    [document("A", 0, 1, 0.1), document("B", 0, 0.5, 0.2)],
    false,
  );
  expect(fused[0]?.video_id).toBe("A");
  expect(Number(fused[0]?.combined_score)).toBeGreaterThan(Number(fused[1]?.combined_score));
});

test("final evidence context is capped at the configured token budget", () => {
  const docs = [
    { chunk_uid: "a", text: "a".repeat(24) }, // 6 tokens
    { chunk_uid: "b", text: "b".repeat(24) }, // 6 tokens
    { chunk_uid: "c", text: "c".repeat(24) }, // 6 tokens
  ];

  const bounded = limitContextBudget(docs, 12);
  expect(bounded.map((document) => document.chunk_uid)).toEqual(["a", "b"]);

  const oversized = limitContextBudget([{ chunk_uid: "large", text: "x".repeat(100) }], 12);
  expect(String(oversized[0]?.text)).toHaveLength(48);
});
