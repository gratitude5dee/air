import type { Collection, Db } from "mongodb";
import { beforeEach, describe, expect, test, vi } from "vitest";
import type { CorpusDocument, Filters } from "../../lib/types.js";
import type { Vocabulary } from "../../lib/retrieval.js";

const voyage = vi.hoisted(() => ({
  embedQuery: vi.fn(),
}));

vi.mock("../../lib/voyage.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../lib/voyage.js")>();
  return { ...actual, embedQuery: voyage.embedQuery };
});

import {
  classifyIntent,
  compactFilters,
  fullTextSearch,
  retrieve,
  rrfFuse,
  toSource,
  vectorSearch,
} from "../../lib/retrieval.js";

function document(
  chunkUid: string,
  overrides: CorpusDocument = {},
): CorpusDocument {
  return {
    chunk_uid: chunkUid,
    video_id: `video-${chunkUid}`,
    title: `Episode ${chunkUid}`,
    channel: "MusicRAG",
    guests: [],
    topics: [],
    text: `Evidence from ${chunkUid}.`,
    chunk_index: 0,
    score: 0.5,
    ...overrides,
  };
}

function aggregateCollection(batches: CorpusDocument[][]): {
  collection: Collection<CorpusDocument>;
  aggregate: ReturnType<typeof vi.fn>;
} {
  const remaining = [...batches];
  const aggregate = vi.fn((_pipeline: unknown[]) => ({
    toArray: vi.fn().mockResolvedValue(remaining.shift() ?? []),
  }));
  return {
    collection: { aggregate } as unknown as Collection<CorpusDocument>,
    aggregate,
  };
}

function pipelineAt(
  aggregate: ReturnType<typeof vi.fn>,
  index = 0,
): Array<Record<string, unknown>> {
  return aggregate.mock.calls[index]?.[0] as Array<Record<string, unknown>>;
}

const vocabulary: Vocabulary = {
  guests: new Map([
    ["rick rubin", "Rick Rubin"],
    ["pharrell williams", "Pharrell Williams"],
    ["producer", "Producer"],
  ]),
  channels: new Map([["musicrag", "MusicRAG"]]),
  topics: new Map([
    ["artist development", "Artist Development"],
    ["producer", "Producer"],
  ]),
};

beforeEach(() => {
  voyage.embedQuery.mockReset();
});

describe("intent planning", () => {
  test("routes comparisons, aggregation, entity lookup, and generic role topics deterministically", () => {
    const comparisonQuestion = "Rick Rubin versus Pharrell Williams on artist development";
    const comparison = classifyIntent(comparisonQuestion, vocabulary);
    expect(comparison).toMatchObject({
      intent: "comparative",
      guests: ["Pharrell Williams", "Rick Rubin"],
      topics: ["Artist Development"],
      subqueries: [
        `Pharrell Williams: ${comparisonQuestion}`,
        `Rick Rubin: ${comparisonQuestion}`,
      ],
    });

    expect(classifyIntent("What common themes recur across episodes about artist development?", vocabulary))
      .toMatchObject({ intent: "aggregative", guests: [], topics: ["Artist Development"] });
    expect(classifyIntent("What did Rick Rubin say about artist development?", vocabulary))
      .toMatchObject({ intent: "entity_lookup", guests: ["Rick Rubin"] });
    expect(classifyIntent("How should a producer plan a release?", vocabulary))
      .toMatchObject({ intent: "thematic", guests: [], topics: ["Producer"] });
  });
});

describe("filter translation", () => {
  test("keeps compact single clauses and combines all selected filters, including Unix epoch", () => {
    expect(compactFilters()).toBeUndefined();
    expect(compactFilters({ channel: "MusicRAG" })).toEqual({ channel: "MusicRAG" });

    expect(compactFilters({
      channel: "MusicRAG",
      guest: "Rick Rubin",
      topic: "Artist Development",
      date_from_ts: 0,
      date_to_ts: 1_700_000_000,
    })).toEqual({
      $and: [
        { channel: "MusicRAG" },
        { guests: { $eq: "Rick Rubin" } },
        { topics: { $eq: "Artist Development" } },
        { upload_ts: { $gte: 0, $lte: 1_700_000_000 } },
      ],
    });
  });
});

describe("Atlas search stages", () => {
  test("uses the locked vector index, projection, filters, and bounded candidate count", async () => {
    const { collection, aggregate } = aggregateCollection([[document("vector-result")]]);
    const filters: Filters = { guest: "Rick Rubin", topic: "Artist Development" };

    await expect(vectorSearch(collection, [0.25, 0.75], filters, 600))
      .resolves.toEqual([document("vector-result")]);

    const pipeline = pipelineAt(aggregate);
    const stage = pipeline[0]?.$vectorSearch as Record<string, unknown>;
    expect(stage).toEqual({
      index: "vector_index",
      path: "embedding",
      queryVector: [0.25, 0.75],
      numCandidates: 10_000,
      limit: 600,
      filter: { $and: [{ guests: { $eq: "Rick Rubin" } }, { topics: { $eq: "Artist Development" } }] },
    });
    expect(pipeline[1]).toMatchObject({
      $project: {
        chunk_uid: 1,
        text: 1,
        score: { $meta: "vectorSearchScore" },
      },
    });

    const minimum = aggregateCollection([[]]);
    await vectorSearch(minimum.collection, [1], undefined, 1);
    expect((pipelineAt(minimum.aggregate)[0]?.$vectorSearch as Record<string, unknown>).numCandidates)
      .toBe(800);
  });

  test("uses the locked text index before matching filters and applying the limit", async () => {
    const { collection, aggregate } = aggregateCollection([[document("text-result")]]);
    const filters: Filters = { channel: "MusicRAG", date_from_ts: 100, date_to_ts: 200 };

    await expect(fullTextSearch(collection, "artist strategy", filters, 7))
      .resolves.toEqual([document("text-result")]);

    const pipeline = pipelineAt(aggregate);
    expect(pipeline[0]).toEqual({
      $search: {
        index: "text_index",
        text: { query: "artist strategy", path: ["text", "title", "guests", "topics"] },
      },
    });
    expect(pipeline[1]).toEqual({
      $match: { $and: [{ channel: "MusicRAG" }, { upload_ts: { $gte: 100, $lte: 200 } }] },
    });
    expect(pipeline[2]).toEqual({ $limit: 7 });
    expect(pipeline[3]).toMatchObject({
      $project: { chunk_uid: 1, text: 1, score: { $meta: "searchScore" } },
    });
  });
});

test("reciprocal-rank fusion preserves vector/text weights, signals, and selected-filter boosts", () => {
  const shared = document("shared", {
    guests: ["Rick Rubin"],
    topics: ["Artist Development"],
    score: 0.91,
  });
  const fused = rrfFuse(
    [shared, document("vector-only")],
    [shared, document("text-only")],
    { guest: "Rick Rubin", topic: "Artist Development" },
  );
  const merged = fused.find((item) => item.chunk_uid === "shared");

  expect(fused[0]?.chunk_uid).toBe("shared");
  expect(Number(merged?.rrf_score)).toBeCloseTo((1 + 0.85) / 61 + 0.02, 12);
  expect(merged?.signals).toEqual({
    vector: { rank: 1, score: 0.91 },
    text: { rank: 1, score: 0.91 },
  });
});

test("source normalization supplies safe defaults and a bounded transcript snippet", () => {
  const source = toSource({
    chunk_uid: "chunk-7",
    video_id: 7,
    guests: ["Rick Rubin", 42],
    text: "x".repeat(530),
    rerank_score: 0.9,
    rrf_score: 0.8,
    score: 0.7,
    chunk_index: 12,
  });

  expect(source).toMatchObject({
    chunk_uid: "chunk-7",
    title: "Untitled",
    channel: "Unknown channel",
    guests: ["Rick Rubin", "42"],
    video_id: "7",
    start_sec: null,
    end_sec: null,
    deep_link: null,
    score: 0.9,
    chunk_index: 12,
  });
  expect(source.snippet).toHaveLength(520);
  expect(source.snippet).toBe(`${"x".repeat(517)}...`);
});

test("retrieve embeds once and combines fake vector/text results without a provider credential", async () => {
  voyage.embedQuery.mockResolvedValue([0.1, 0.2]);
  const shared = document("shared", { guests: ["Rick Rubin"] });
  const { collection, aggregate } = aggregateCollection([
    [shared, document("vector-only")],
    [shared, document("text-only")],
  ]);
  const database = {
    collection: vi.fn(() => collection),
  } as unknown as Db;

  const docs = await retrieve("artist strategy", { guest: "Rick Rubin" }, 3, database);

  expect(voyage.embedQuery).toHaveBeenCalledOnce();
  expect(voyage.embedQuery).toHaveBeenCalledWith("artist strategy", {});
  expect(docs.map((item) => item.chunk_uid)).toEqual(["shared", "vector-only", "text-only"]);
  expect(aggregate).toHaveBeenCalledTimes(2);
  expect((pipelineAt(aggregate, 0)[0]?.$vectorSearch as Record<string, unknown>).index)
    .toBe("vector_index");
  expect((pipelineAt(aggregate, 1)[0]?.$search as Record<string, unknown>).index)
    .toBe("text_index");
});
