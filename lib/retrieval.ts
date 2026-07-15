import type { Collection, Db, Document } from "mongodb";
import { getDb } from "./mongo.js";
import {
  diversifyByEpisode,
  fuseAndAggregate,
  uniqueEpisodeCount,
} from "./ranking.js";
import type {
  AgentTraceEvent,
  CorpusDocument,
  Filters,
  Intent,
  QueryPlan,
  RetrievedSource,
} from "./types.js";
import { embedQuery, RERANK_CANDIDATE_LIMIT, rerankCandidates } from "./voyage.js";
import { operationalLogger } from "./logging.js";
import { UpstreamTimeoutError } from "./upstream.js";

/** Must stay aligned with the bounded Voyage reranker request. */
export const CANDIDATE_LIMIT = RERANK_CANDIDATE_LIMIT;
export const TOP_RANKED_LIMIT = 8;
export const FINAL_SOURCE_LIMIT = 10;
export const MAX_RETRIEVED_TOKENS = 8_000;

const MIN_NUM_CANDIDATES = 800;
const MAX_NUM_CANDIDATES = 10_000;
const EXPAND_SEEDS = 4;
const EXPAND_WINDOW = 1;

const SOURCE_PROJECTION = {
  _id: 0,
  chunk_uid: 1,
  video_id: 1,
  channel: 1,
  title: 1,
  text: 1,
  guests: 1,
  topics: 1,
  start_sec: 1,
  end_sec: 1,
  deep_link: 1,
  chunk_index: 1,
};

const EPISODE_PROJECTION = {
  _id: 0,
  video_id: 1,
  channel: 1,
  title: 1,
  guests: 1,
  topics: 1,
};

const COMPARISON_PATTERNS = [
  /\bvs\.?\b/,
  /\bversus\b/,
  /\bcompared?\b/,
  /\bdifference between\b/,
  /\bdiffer\b/,
  /\bcontrast\b/,
];
const AGGREGATION_PATTERNS = [
  /\bcommon (themes?|threads?|advice|lessons?|patterns?)\b/,
  /\bacross (the )?(episodes?|guests?|channels?|interviews?)\b/,
  /\bmost (guests?|people|producers?|managers?|artists?)\b/,
  /\bconsensus\b/,
  /\bevery(one|body)\b/,
  /\brecurring\b/,
];
const GENERIC_INDUSTRY_ROLE_TOPICS = new Set([
  "a&r",
  "a & r",
  "artist",
  "artists",
  "manager",
  "managers",
  "producer",
  "producers",
  "songwriter",
  "songwriters",
  "engineer",
  "engineers",
  "publisher",
  "publishers",
]);

export type Vocabulary = {
  guests: Map<string, string>;
  channels: Map<string, string>;
  topics: Map<string, string>;
};

export type MusicRagRetrieval = {
  plan: QueryPlan;
  docs: CorpusDocument[];
  trace: AgentTraceEvent[];
};

/** Request-wide cancellation propagated from the grounded-answer deadline. */
export interface RetrievalExecutionOptions {
  readonly signal?: AbortSignal | undefined;
}

let cachedVocabulary: Vocabulary | undefined;

function canonicalize(value: string): string {
  return value
    .replace(/[’‘`´]/g, "'")
    .replace(/[“”]/g, '"')
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function slugify(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function matchVocabulary(text: string, vocabulary: Map<string, string>): string[] {
  const input = canonicalize(text);
  const seen = new Set<string>();
  const matches: Array<{ name: string; length: number }> = [];
  for (const surface of [...vocabulary.keys()].sort((left, right) => right.length - left.length)) {
    const candidate = canonicalize(surface);
    if (candidate.length < 3) continue;
    const expression = new RegExp(`(?<!\\w)${escapeRegExp(candidate)}(?!\\w)`);
    if (!expression.test(input)) continue;
    const canonical = vocabulary.get(surface);
    if (canonical && !seen.has(canonical)) {
      seen.add(canonical);
      matches.push({ name: canonical, length: candidate.length });
    }
  }
  return matches.sort((left, right) => right.length - left.length).map((match) => match.name);
}

export async function loadVocabulary(db: Db): Promise<Vocabulary> {
  if (cachedVocabulary) return cachedVocabulary;

  const guests = new Map<string, string>();
  const topics = new Map<string, string>();
  const channels = new Map<string, string>();
  const entities = await db
    .collection<CorpusDocument>("entities")
    .find({}, { projection: { name: 1, type: 1 } })
    .toArray();
  for (const entity of entities) {
    const name = typeof entity.name === "string" ? entity.name : undefined;
    if (!name) continue;
    (entity.type === "guest" ? guests : topics).set(name.toLowerCase(), name);
  }
  const corpusChannels = await db
    .collection<CorpusDocument>("channels")
    .find({}, { projection: { channel: 1 } })
    .toArray();
  for (const item of corpusChannels) {
    const channel = typeof item.channel === "string" ? item.channel : undefined;
    if (channel) channels.set(channel.toLowerCase(), channel);
  }

  cachedVocabulary = { guests, channels, topics };
  return cachedVocabulary;
}

export function __resetVocabularyForTests(): void {
  cachedVocabulary = undefined;
}

export function classifyIntent(query: string, vocabulary: Vocabulary): QueryPlan {
  const text = ` ${canonicalize(query)} `;
  const topics = matchVocabulary(text, vocabulary.topics);
  const topicNames = new Set(topics.map(canonicalize));
  const guests = matchVocabulary(text, vocabulary.guests).filter((guest) => {
    const normalized = canonicalize(guest);
    return !topicNames.has(normalized) && !GENERIC_INDUSTRY_ROLE_TOPICS.has(normalized);
  });
  const channels = matchVocabulary(text, vocabulary.channels);
  const namedEntities = [...guests, ...channels];
  const isComparison = COMPARISON_PATTERNS.some((pattern) => pattern.test(text));
  const isAggregation = AGGREGATION_PATTERNS.some((pattern) => pattern.test(text));

  let intent: Intent = "thematic";
  let subqueries: string[] = [];
  let rationale = "no named guest; conceptual/topic query";
  if (isComparison && namedEntities.length >= 2) {
    intent = "comparative";
    subqueries = namedEntities.slice(0, 3).map((entity) => `${entity}: ${query}`);
    rationale = `comparison cue + ${namedEntities.length} named entities`;
  } else if (isAggregation && guests.length === 0) {
    intent = "aggregative";
    rationale = "aggregation cue without a single named guest";
  } else if (guests.length > 0) {
    intent = "entity_lookup";
    rationale = `named guest(s): ${guests.join(", ")}`;
  }

  return { intent, query, guests, channels, topics, subqueries, rationale };
}

/** Convert user-selected filters into both Atlas Search and vector filter form. */
export function compactFilters(filters?: Filters): Document | undefined {
  const clauses: Document[] = [];
  if (filters?.channel) clauses.push({ channel: filters.channel });
  if (filters?.guest) clauses.push({ guests: { $eq: filters.guest } });
  if (filters?.topic) clauses.push({ topics: { $eq: filters.topic } });
  if (filters?.date_from_ts || filters?.date_to_ts) {
    const range: Document = {};
    if (filters.date_from_ts) range.$gte = filters.date_from_ts;
    if (filters.date_to_ts) range.$lte = filters.date_to_ts;
    clauses.push({ upload_ts: range });
  }
  if (clauses.length === 0) return undefined;
  return clauses.length === 1 ? clauses[0] : { $and: clauses };
}

export async function vectorSearch(
  collection: Collection<CorpusDocument>,
  queryVector: number[],
  filters?: Filters,
  limit = CANDIDATE_LIMIT,
): Promise<CorpusDocument[]> {
  const numCandidates = Math.min(
    MAX_NUM_CANDIDATES,
    Math.max(MIN_NUM_CANDIDATES, limit * 20),
  );
  const stage: Document = {
    index: "vector_index",
    path: "embedding",
    queryVector,
    numCandidates,
    limit,
  };
  const filter = compactFilters(filters);
  if (filter) stage.filter = filter;

  const startedAtMs = Date.now();
  try {
    const result = await collection
      .aggregate<CorpusDocument>([
        { $vectorSearch: stage },
        { $project: { ...SOURCE_PROJECTION, score: { $meta: "vectorSearchScore" } } },
      ])
      .toArray();
    operationalLogger.info("atlas.search_completed", {
      operation: "vector_search",
      latencyMs: Date.now() - startedAtMs,
      resultCount: result.length,
    });
    return result;
  } catch (error) {
    operationalLogger.error("atlas.search_failed", {
      operation: "vector_search",
      latencyMs: Date.now() - startedAtMs,
      category: "atlas_vector_search",
    });
    throw error;
  }
}

export async function fullTextSearch(
  collection: Collection<CorpusDocument>,
  query: string,
  filters?: Filters,
  limit = CANDIDATE_LIMIT,
): Promise<CorpusDocument[]> {
  const pipeline: Document[] = [
    {
      $search: {
        index: "text_index",
        text: { query, path: ["text", "title", "guests", "topics"] },
      },
    },
  ];
  const filter = compactFilters(filters);
  if (filter) pipeline.push({ $match: filter });
  pipeline.push(
    { $limit: limit },
    { $project: { ...SOURCE_PROJECTION, score: { $meta: "searchScore" } } },
  );
  const startedAtMs = Date.now();
  try {
    const result = await collection.aggregate<CorpusDocument>(pipeline).toArray();
    operationalLogger.info("atlas.search_completed", {
      operation: "text_search",
      latencyMs: Date.now() - startedAtMs,
      resultCount: result.length,
    });
    return result;
  } catch (error) {
    operationalLogger.error("atlas.search_failed", {
      operation: "text_search",
      latencyMs: Date.now() - startedAtMs,
      category: "atlas_text_search",
    });
    throw error;
  }
}

/** Reciprocal-rank fusion preserves both semantic and literal retrieval wins. */
export function rrfFuse(
  vector: CorpusDocument[],
  text: CorpusDocument[],
  filters?: Filters,
): CorpusDocument[] {
  const fused = new Map<string, CorpusDocument>();
  for (const [name, results, weight] of [
    ["vector", vector, 1],
    ["text", text, 0.85],
  ] as const) {
    results.forEach((document, index) => {
      const key = String(document.chunk_uid ?? `${document.video_id}:${document.chunk_index}`);
      const existing = fused.get(key) ?? { ...document, rrf_score: 0, signals: {} };
      existing.rrf_score = Number(existing.rrf_score ?? 0) + weight * (1 / (60 + index + 1));
      (existing.signals as Record<string, unknown>)[name] = {
        rank: index + 1,
        score: document.score,
      };
      fused.set(key, existing);
    });
  }
  for (const document of fused.values()) {
    if (filters?.guest && Array.isArray(document.guests) && document.guests.includes(filters.guest)) {
      document.rrf_score = Number(document.rrf_score ?? 0) + 0.01;
    }
    if (filters?.topic && Array.isArray(document.topics) && document.topics.includes(filters.topic)) {
      document.rrf_score = Number(document.rrf_score ?? 0) + 0.01;
    }
  }
  return [...fused.values()].sort(
    (left, right) => Number(right.rrf_score ?? 0) - Number(left.rrf_score ?? 0),
  );
}

export function toSource(document: CorpusDocument): RetrievedSource {
  const text = String(document.text ?? "");
  return {
    ...(typeof document.chunk_uid === "string"
      ? { chunk_uid: document.chunk_uid }
      : {}),
    title: String(document.title ?? "Untitled"),
    channel: String(document.channel ?? "Unknown channel"),
    guests: Array.isArray(document.guests) ? document.guests.map(String) : [],
    video_id: String(document.video_id ?? ""),
    start_sec: typeof document.start_sec === "number" ? document.start_sec : null,
    end_sec: typeof document.end_sec === "number" ? document.end_sec : null,
    deep_link: typeof document.deep_link === "string" ? document.deep_link : null,
    snippet: text.length > 520 ? `${text.slice(0, 517).trim()}...` : text,
    score: Number(document.rerank_score ?? document.rrf_score ?? document.score ?? 0),
    chunk_index: typeof document.chunk_index === "number" ? document.chunk_index : null,
  };
}

export async function retrieve(
  query: string,
  filters?: Filters,
  limit = CANDIDATE_LIMIT,
  db?: Db,
  execution: RetrievalExecutionOptions = {},
): Promise<CorpusDocument[]> {
  throwIfAborted(execution);
  const database = db ?? (await getDb());
  const collection = database.collection<CorpusDocument>("chunks");
  const queryVector = await embedQuery(query, execution);
  throwIfAborted(execution);
  const [vector, text] = await Promise.all([
    vectorSearch(collection, queryVector, filters, limit),
    fullTextSearch(collection, query, filters, limit),
  ]);
  return rrfFuse(vector, text, filters).slice(0, limit);
}

function throwIfAborted(execution: RetrievalExecutionOptions): void {
  if (execution.signal?.aborted) {
    throw new UpstreamTimeoutError("RAG retrieval deadline exceeded");
  }
}

function docKey(document: CorpusDocument): string {
  return String(document.chunk_uid ?? `${document.video_id}:${document.chunk_index}`);
}

function dedupe(docs: CorpusDocument[]): CorpusDocument[] {
  const seen = new Set<string>();
  return docs.filter((document) => {
    const key = docKey(document);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function estimateTokenCount(text: string): number {
  return Math.ceil(text.length / 4);
}

/**
 * Retrieval may inspect more material while ranking, but the final evidence
 * set handed to answer construction must fit the ~8k-token context budget.
 * Preserve ranking order and retain a bounded prefix only when the strongest
 * single chunk itself exceeds the entire budget.
 */
export function limitContextBudget(
  docs: CorpusDocument[],
  maxTokens = MAX_RETRIEVED_TOKENS,
): CorpusDocument[] {
  if (!Number.isInteger(maxTokens) || maxTokens < 1) {
    throw new Error("context token budget must be a positive integer");
  }
  const selected: CorpusDocument[] = [];
  let remaining = maxTokens;
  for (const document of docs) {
    const text = String(document.text ?? "");
    const tokens = estimateTokenCount(text);
    if (tokens <= remaining) {
      selected.push(document);
      remaining -= tokens;
      continue;
    }
    if (selected.length === 0 && text) {
      // Four characters/token is the same conservative estimator used by the
      // source pipeline. Cloning avoids mutating Mongo driver objects.
      selected.push({ ...document, text: text.slice(0, maxTokens * 4) });
      remaining = 0;
    }
  }
  return selected;
}

function recordDocs(
  trace: AgentTraceEvent[],
  state: { retrievedTokens: number },
  docs: CorpusDocument[],
  label: string,
): void {
  state.retrievedTokens += docs.reduce(
    (total, document) => total + estimateTokenCount(String(document.text ?? "")),
    0,
  );
  trace.push({
    step: "search_transcripts",
    label,
    count: docs.length,
    token_count: state.retrievedTokens,
  });
}

async function findEpisodesByGuest(db: Db, guest: string, limit = 5): Promise<CorpusDocument[]> {
  const entity = await db
    .collection<CorpusDocument>("entities")
    .findOne({ type: "guest", slug: slugify(guest) });
  if (entity && Array.isArray(entity.episode_ids) && entity.episode_ids.length > 0) {
    return db
      .collection<CorpusDocument>("episodes")
      .find(
        { video_id: { $in: entity.episode_ids.map(String) } },
        { projection: EPISODE_PROJECTION },
      )
      .limit(limit)
      .toArray();
  }
  return db
    .collection<CorpusDocument>("episodes")
    .find(
      {
        $or: [
          { guests: guest },
          { title: { $regex: escapeRegExp(guest), $options: "i" } },
        ],
      },
      { projection: EPISODE_PROJECTION },
    )
    .limit(limit)
    .toArray();
}

async function episodeChunks(db: Db, videoId: string, limit = 8): Promise<CorpusDocument[]> {
  return db
    .collection<CorpusDocument>("chunks")
    .find({ video_id: videoId }, { projection: SOURCE_PROJECTION })
    .sort({ chunk_index: 1 })
    .limit(limit)
    .toArray();
}

function guestAligned(document: CorpusDocument, names: Set<string>): boolean {
  const guests = new Set(
    (Array.isArray(document.guests) ? document.guests : []).map((guest) =>
      String(guest).toLowerCase(),
    ),
  );
  const title = String(document.title ?? "").toLowerCase();
  return [...names].some((name) => guests.has(name) || title.includes(name));
}

function sufficient(plan: QueryPlan, docs: CorpusDocument[]): boolean {
  if (docs.length < 3) return false;
  if (plan.guests.length === 0) return true;
  const names = new Set(plan.guests.map((guest) => guest.toLowerCase()));
  return docs.slice(0, 5).some((document) => guestAligned(document, names));
}

async function routeEntity(
  db: Db,
  plan: QueryPlan,
  filters: Filters | undefined,
  execution: RetrievalExecutionOptions,
): Promise<CorpusDocument[]> {
  const videoIds: string[] = [];
  for (const guest of plan.guests.slice(0, 2)) {
    throwIfAborted(execution);
    let episodes = await findEpisodesByGuest(db, guest);
    if (plan.channels.length > 0) {
      const scoped = episodes.filter((episode) => plan.channels.includes(String(episode.channel)));
      if (scoped.length > 0) episodes = scoped;
    }
    for (const episode of episodes) {
      const videoId = typeof episode.video_id === "string" ? episode.video_id : undefined;
      if (videoId && !videoIds.includes(videoId)) videoIds.push(videoId);
    }
  }
  const seedCandidates: CorpusDocument[] = [];
  for (const videoId of videoIds.slice(0, 5)) {
    throwIfAborted(execution);
    seedCandidates.push(...(await episodeChunks(db, videoId)));
  }
  // Neighbor seeds and hybrid search share one fixed rerank budget. Without
  // this reservation an entity query can send 40 seed chunks plus 80 hybrid
  // chunks to Voyage, bypassing both the latency and spend guardrail.
  const boundedSeeds = dedupe(seedCandidates).slice(0, CANDIDATE_LIMIT);
  const remaining = CANDIDATE_LIMIT - boundedSeeds.length;
  const hybrid = remaining > 0
    ? await retrieve(plan.query, filters, remaining, db, execution)
    : [];
  const candidates = dedupe([...boundedSeeds, ...hybrid]).slice(0, CANDIDATE_LIMIT);
  return fuseAndAggregate(await rerankCandidates(plan.query, candidates, execution)).slice(
    0,
    TOP_RANKED_LIMIT,
  );
}

async function routeThematic(
  query: string,
  filters: Filters | undefined,
  db: Db,
  execution: RetrievalExecutionOptions,
): Promise<CorpusDocument[]> {
  const fused = await retrieve(query, filters, CANDIDATE_LIMIT, db, execution);
  const scored = await rerankCandidates(query, fused, execution);
  return diversifyByEpisode(fuseAndAggregate(scored, false), TOP_RANKED_LIMIT, {
    maxPerEpisode: 1,
    minEpisodes: TOP_RANKED_LIMIT,
  });
}

async function routeComparative(
  db: Db,
  plan: QueryPlan,
  filters: Filters | undefined,
  execution: RetrievalExecutionOptions,
): Promise<CorpusDocument[]> {
  const merged: CorpusDocument[] = [];
  const subqueries = plan.subqueries.length > 0 ? plan.subqueries : [plan.query];
  const perEntity = Math.max(2, Math.floor(TOP_RANKED_LIMIT / subqueries.length));
  for (const subquery of subqueries) {
    throwIfAborted(execution);
    const fused = await retrieve(
      subquery,
      filters,
      Math.max(40, Math.floor(CANDIDATE_LIMIT / 2)),
      db,
      execution,
    );
    merged.push(...fuseAndAggregate(await rerankCandidates(subquery, fused, execution)).slice(0, perEntity));
  }
  return dedupe(merged).slice(0, TOP_RANKED_LIMIT);
}

async function routeAggregative(
  query: string,
  filters: Filters | undefined,
  db: Db,
  execution: RetrievalExecutionOptions,
): Promise<CorpusDocument[]> {
  // Keep every intent on the locked 80-candidate retrieval budget. A broader
  // aggregation route gets its diversity from episode selection below, not a
  // hidden larger Atlas query.
  const fused = await retrieve(query, filters, CANDIDATE_LIMIT, db, execution);
  const ranked = fuseAndAggregate(await rerankCandidates(query, fused, execution), false);
  const seen = new Set<string>();
  return ranked
    .filter((document) => {
      const key = String(document.video_id ?? docKey(document));
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .slice(0, TOP_RANKED_LIMIT);
}

async function expandContext(
  db: Db,
  docs: CorpusDocument[],
  state: { retrievedTokens: number },
  trace: AgentTraceEvent[],
  execution: RetrievalExecutionOptions,
): Promise<CorpusDocument[]> {
  if (docs.length === 0 || state.retrievedTokens >= MAX_RETRIEVED_TOKENS) {
    trace.push({
      step: "expand_context",
      label: "skipped: context budget reached or no seed documents",
      token_count: state.retrievedTokens,
    });
    return docs;
  }

  const expanded = [...docs];
  const seen = new Set(docs.map(docKey));
  let remaining = MAX_RETRIEVED_TOKENS - state.retrievedTokens;
  let added = 0;
  for (const seed of docs.slice(0, EXPAND_SEEDS)) {
    throwIfAborted(execution);
    const videoId = typeof seed.video_id === "string" ? seed.video_id : undefined;
    const chunkIndex = typeof seed.chunk_index === "number" ? seed.chunk_index : undefined;
    if (!videoId || chunkIndex === undefined || remaining <= 0) continue;
    const neighbors = await db
      .collection<CorpusDocument>("chunks")
      .find(
        {
          video_id: videoId,
          chunk_index: {
            $gte: Math.max(0, chunkIndex - EXPAND_WINDOW),
            $lte: chunkIndex + EXPAND_WINDOW,
          },
        },
        { projection: SOURCE_PROJECTION },
      )
      .sort({ chunk_index: 1 })
      .toArray();
    for (const neighbor of neighbors) {
      const key = docKey(neighbor);
      if (seen.has(key)) continue;
      const tokens = estimateTokenCount(String(neighbor.text ?? ""));
      if (tokens > remaining) continue;
      seen.add(key);
      remaining -= tokens;
      added += 1;
      expanded.push({
        ...neighbor,
        rerank_score: Number(seed.rerank_score ?? 0) * 0.92,
        rrf_score: Number(seed.rrf_score ?? 0) * 0.92,
        expanded_from: docKey(seed),
      });
    }
  }
  state.retrievedTokens = MAX_RETRIEVED_TOKENS - remaining;
  trace.push({
    step: "expand_context",
    label: added > 0 ? `expanded ${added} neighboring chunks` : "no neighboring chunks added",
    count: added,
    token_count: state.retrievedTokens,
  });
  return fuseAndAggregate(dedupe(expanded)).slice(0, FINAL_SOURCE_LIMIT * 2);
}

function diversifyForIntent(intent: Intent, docs: CorpusDocument[]): CorpusDocument[] {
  const ranked = dedupe(docs);
  if (intent === "entity_lookup") {
    return diversifyByEpisode(ranked, FINAL_SOURCE_LIMIT, { maxPerEpisode: 4, minEpisodes: 2 });
  }
  if (intent === "comparative") {
    return diversifyByEpisode(ranked, FINAL_SOURCE_LIMIT, { maxPerEpisode: 2, minEpisodes: 4 });
  }
  return diversifyByEpisode(ranked, FINAL_SOURCE_LIMIT, { maxPerEpisode: 2, minEpisodes: 5 });
}

/**
 * Deterministic TypeScript port of MusicRAG's intent-routing retrieval path.
 * It performs no generation and never writes to the corpus database.
 */
export async function runMusicRagRetrieval(
  question: string,
  filters?: Filters,
  database?: Db,
  execution: RetrievalExecutionOptions = {},
): Promise<MusicRagRetrieval> {
  throwIfAborted(execution);
  const db = database ?? (await getDb());
  const vocabulary = await loadVocabulary(db);
  let plan = classifyIntent(question, vocabulary);
  const trace: AgentTraceEvent[] = [
    { step: "classify", label: plan.intent, detail: plan.rationale },
  ];
  const state = { retrievedTokens: 0 };

  const route = async (currentPlan: QueryPlan): Promise<CorpusDocument[]> => {
    throwIfAborted(execution);
    if (currentPlan.intent === "entity_lookup") return routeEntity(db, currentPlan, filters, execution);
    if (currentPlan.intent === "comparative") return routeComparative(db, currentPlan, filters, execution);
    if (currentPlan.intent === "aggregative") return routeAggregative(currentPlan.query, filters, db, execution);
    return routeThematic(currentPlan.query, filters, db, execution);
  };

  let docs = await route(plan);
  recordDocs(trace, state, docs, `${plan.intent} route`);
  if (!sufficient(plan, docs)) {
    trace.push({
      step: "grade_context",
      label: "insufficient",
      detail: "broadened to thematic retrieval",
      count: docs.length,
    });
    plan = {
      ...plan,
      intent: "thematic",
      guests: [],
      subqueries: [],
      rationale: "rewrite: broaden",
    };
    docs = await route(plan);
    recordDocs(trace, state, docs, "thematic fallback route");
  } else {
    trace.push({ step: "grade_context", label: "sufficient", count: docs.length });
  }

  throwIfAborted(execution);
  docs = limitContextBudget(
    diversifyForIntent(plan.intent, await expandContext(db, docs, state, trace, execution)),
  );
  const finalContextTokens = docs.reduce(
    (total, document) => total + estimateTokenCount(String(document.text ?? "")),
    0,
  );
  trace.push({
    step: "expand_context",
    label: `diversified to ${uniqueEpisodeCount(docs)} episodes`,
    count: docs.length,
    token_count: finalContextTokens,
  });
  trace.push({
    step: "synthesize",
    label: "ready for grounded answer",
    count: docs.length,
    token_count: finalContextTokens,
  });
  return { plan, docs, trace };
}
