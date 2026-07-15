import { afterEach, expect, test, vi } from "vitest";
import {
  embedQuery,
  formatForRerank,
  MAX_RERANK_DOCUMENT_CHARS,
  MAX_RERANK_TOTAL_CHARS,
  RERANK_CANDIDATE_LIMIT,
  rerankCandidates,
  VOYAGE_DIMENSIONS,
} from "../../lib/voyage.js";
import type { CorpusDocument } from "../../lib/types.js";

function document(index: number, text = "Transcript evidence."): CorpusDocument {
  return {
    chunk_uid: `chunk-${index}`,
    video_id: `video-${index}`,
    title: `Episode ${index}`,
    channel: "MusicRAG",
    guests: ["Guest"],
    text,
  };
}

afterEach(() => {
  delete process.env.VOYAGE_API_KEY;
  delete process.env.VOYAGE_API_BASE_URL;
  delete process.env.EMBED_MODEL;
  delete process.env.RERANK_MODEL;
});

test("embedding requests carry an abort signal and reject a wrong Voyage dimension", async () => {
  process.env.VOYAGE_API_KEY = "test-key";
  const fetchMock = vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ data: [{ embedding: [1, 2, 3] }] }), { status: 200 }),
  );
  vi.stubGlobal("fetch", fetchMock);

  await expect(embedQuery("artist strategy")).rejects.toThrow(/3 dimensions; expected 1024/);
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const options = fetchMock.mock.calls[0]?.[1] as RequestInit | undefined;
  expect(options?.signal).toBeInstanceOf(AbortSignal);
});

test("rerank input is bounded per document and in aggregate", async () => {
  const oversized = document(1, "x".repeat(20_000));
  expect(formatForRerank(oversized).length).toBeLessThanOrEqual(MAX_RERANK_DOCUMENT_CHARS);

  process.env.VOYAGE_API_KEY = "test-key";
  const fetchMock = vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ results: [{ index: 0, relevance_score: 0.9 }] }), { status: 200 }),
  );
  vi.stubGlobal("fetch", fetchMock);
  await rerankCandidates("artist strategy", [oversized]);

  const options = fetchMock.mock.calls[0]?.[1] as RequestInit | undefined;
  const body = JSON.parse(String(options?.body)) as { documents: string[] };
  expect(body.documents).toHaveLength(1);
  expect(body.documents[0]?.length).toBeLessThanOrEqual(MAX_RERANK_DOCUMENT_CHARS);
  expect(body.documents.reduce((total, item) => total + item.length, 0))
    .toBeLessThanOrEqual(MAX_RERANK_TOTAL_CHARS);
});

test("rerank rejects more than the fixed candidate budget before an outbound request", async () => {
  const fetchMock = vi.fn();
  vi.stubGlobal("fetch", fetchMock);
  const docs = Array.from({ length: RERANK_CANDIDATE_LIMIT + 1 }, (_value, index) => document(index));

  await expect(rerankCandidates("artist strategy", docs)).rejects.toThrow(/candidate count exceeds/);
  expect(fetchMock).not.toHaveBeenCalled();
});

test("valid Voyage embeddings retain the required 1024 dimensions", async () => {
  process.env.VOYAGE_API_KEY = "test-key";
  const embedding = Array.from({ length: VOYAGE_DIMENSIONS }, (_value, index) => index);
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(new Response(JSON.stringify({ data: [{ embedding }] }), { status: 200 })),
  );

  await expect(embedQuery("artist strategy")).resolves.toEqual(embedding);
});
