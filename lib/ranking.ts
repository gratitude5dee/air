import type { CorpusDocument } from "./types.js";

export const RERANK_WEIGHT = 0.7;
export const RRF_WEIGHT = 0.3;
export const EPISODE_DAMPING = 0.5;

function chunkKey(doc: CorpusDocument): string {
  return String(
    doc.chunk_uid ?? `${String(doc.video_id ?? "unknown")}:${String(doc.chunk_index ?? "unknown")}`,
  );
}

function episodeKey(doc: CorpusDocument): string {
  return String(doc.video_id ?? chunkKey(doc));
}

export function uniqueEpisodeCount(docs: CorpusDocument[]): number {
  return new Set(docs.map(episodeKey)).size;
}

export function normalizeScores(values: number[]): number[] {
  if (values.length === 0) return [];
  const low = Math.min(...values);
  const high = Math.max(...values);
  if (high - low < 1e-9) return values.map(() => 1);
  return values.map((value) => (value - low) / (high - low));
}

/**
 * Fuse reranker and RRF scores, then reward episodes with several strong
 * chunks without allowing one episode to dominate every result.
 */
export function fuseAndAggregate(
  docs: CorpusDocument[],
  episodeAware = true,
): CorpusDocument[] {
  if (docs.length === 0) return [];

  const rerank = normalizeScores(docs.map((doc) => Number(doc.rerank_score ?? 0)));
  const rrf = normalizeScores(docs.map((doc) => Number(doc.rrf_score ?? 0)));
  const scored = docs.map((doc, index) => ({
    ...doc,
    combined_score:
      RERANK_WEIGHT * (rerank[index] ?? 0) + RRF_WEIGHT * (rrf[index] ?? 0),
    video_score: 0,
  }));

  if (!episodeAware) {
    return [...scored].sort(
      (left, right) =>
        Number(right.combined_score) - Number(left.combined_score),
    );
  }

  const byVideo = new Map<string, number[]>();
  for (const doc of scored) {
    const key = episodeKey(doc);
    const scores = byVideo.get(key) ?? [];
    scores.push(Number(doc.combined_score));
    byVideo.set(key, scores);
  }

  const videoScores = new Map<string, number>();
  for (const [videoId, scores] of byVideo) {
    const top = Math.max(...scores);
    videoScores.set(
      videoId,
      top + EPISODE_DAMPING * (scores.reduce((sum, score) => sum + score, 0) - top),
    );
  }

  for (const doc of scored) {
    doc.video_score = videoScores.get(episodeKey(doc)) ?? Number(doc.combined_score);
  }

  return scored.sort(
    (left, right) =>
      Number(right.video_score) - Number(left.video_score) ||
      Number(right.combined_score) - Number(left.combined_score),
  );
}

export function diversifyByEpisode<T extends CorpusDocument>(
  docs: T[],
  limit: number,
  options: { maxPerEpisode?: number; minEpisodes?: number } = {},
): T[] {
  const maxPerEpisode = Math.max(1, options.maxPerEpisode ?? 2);
  const minEpisodes = Math.min(
    limit,
    Math.max(1, options.minEpisodes ?? Math.ceil(limit / 2)),
  );
  const selected: T[] = [];
  const selectedChunks = new Set<string>();
  const episodeCounts = new Map<string, number>();

  const add = (doc: T, perEpisodeCap: number): boolean => {
    if (selected.length >= limit) return false;
    const currentChunk = chunkKey(doc);
    if (selectedChunks.has(currentChunk)) return false;
    const currentEpisode = episodeKey(doc);
    const count = episodeCounts.get(currentEpisode) ?? 0;
    if (count >= perEpisodeCap) return false;

    selected.push(doc);
    selectedChunks.add(currentChunk);
    episodeCounts.set(currentEpisode, count + 1);
    return true;
  };

  // First pass prioritizes breadth, followed by the configured per-episode
  // cap, then a final fill pass if the corpus cannot meet that diversity goal.
  for (const doc of docs) {
    if (episodeCounts.size >= minEpisodes || selected.length >= limit) break;
    add(doc, 1);
  }
  for (const doc of docs) {
    if (selected.length >= limit) break;
    add(doc, maxPerEpisode);
  }
  for (const doc of docs) {
    if (selected.length >= limit) break;
    add(doc, Number.POSITIVE_INFINITY);
  }

  return selected;
}
