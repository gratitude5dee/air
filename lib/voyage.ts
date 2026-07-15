import type { CorpusDocument } from "./types.js";
import { operationalLogger } from "./logging.js";
import { UpstreamTimeoutError } from "./upstream.js";

/**
 * Provenance: MusicRAG's Voyage embedding and reranking request contract,
 * ported from `/Users/gratitud3/Downloads/musicindustry/musicRAG/web/lib`.
 */

// MusicRAG routes Voyage-compatible embedding/reranking requests through this
// pinned endpoint. Do not make it an environment override: the request carries
// the Voyage bearer credential and user question content.
const VOYAGE_BASE_URL = "https://ai.mongodb.com/v1";
export const VOYAGE_DIMENSIONS = 1_024;
/** Leave room for MongoDB work and a bounded generation attempt in a 60s function. */
export const VOYAGE_REQUEST_TIMEOUT_MS = 8_000;
export const RERANK_CANDIDATE_LIMIT = 80;
export const MAX_RERANK_DOCUMENT_CHARS = 1_600;
export const MAX_RERANK_TOTAL_CHARS = RERANK_CANDIDATE_LIMIT * MAX_RERANK_DOCUMENT_CHARS;
const MAX_RERANK_TITLE_CHARS = 180;
const MAX_RERANK_GUESTS_CHARS = 180;
const MAX_RERANK_CHANNEL_CHARS = 120;

type EmbeddingResponse = {
  data?: Array<{ embedding?: number[] }>;
  embeddings?: number[][];
};

type RerankResponse = {
  data?: Array<{ index: number; relevance_score?: number; score?: number }>;
  results?: Array<{ index: number; relevance_score?: number; score?: number }>;
};

export interface VoyageRequestOptions {
  /** Request-wide deadline propagated from the durable tool execution. */
  readonly signal?: AbortSignal | undefined;
  /** A remaining budget can shorten, but never lengthen, the provider cap. */
  readonly timeoutMs?: number | undefined;
}

function voyageApiKey(): string {
  const key = process.env.VOYAGE_API_KEY?.trim();
  if (!key) throw new Error("VOYAGE_API_KEY is required for grounded retrieval");
  return key;
}

async function voyageFetch(
  path: string,
  body: unknown,
  options: VoyageRequestOptions = {},
): Promise<Response> {
  const timeoutMs = options.timeoutMs === undefined
    ? VOYAGE_REQUEST_TIMEOUT_MS
    : Math.min(VOYAGE_REQUEST_TIMEOUT_MS, options.timeoutMs);
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) {
    throw new UpstreamTimeoutError("Voyage request deadline exceeded");
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => {
    controller.abort(new UpstreamTimeoutError("Voyage request timed out"));
  }, timeoutMs);
  const signal = options.signal ? AbortSignal.any([controller.signal, options.signal]) : controller.signal;
  try {
    return await fetch(`${VOYAGE_BASE_URL}${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${voyageApiKey()}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      signal,
    });
  } catch (error) {
    if (controller.signal.aborted || options.signal?.aborted) {
      throw new UpstreamTimeoutError("Voyage request timed out");
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

export async function embedQuery(
  query: string,
  options: VoyageRequestOptions = {},
): Promise<number[]> {
  if (!query.trim()) throw new Error("A non-empty query is required for embedding");
  const startedAtMs = Date.now();

  try {
    const model = process.env.EMBED_MODEL?.trim() || "voyage-4-large";
    const response = await voyageFetch("/embeddings", {
      model,
      input: [query],
      input_type: "query",
    }, options);

    if (!response.ok) {
      throw new Error(`Voyage embedding request failed (${response.status})`);
    }

    const payload = (await response.json()) as EmbeddingResponse;
    const embedding = payload.data?.[0]?.embedding ?? payload.embeddings?.[0];
    if (!embedding || embedding.length !== VOYAGE_DIMENSIONS) {
      throw new Error(
        `Voyage embedding returned ${embedding?.length ?? 0} dimensions; expected ${VOYAGE_DIMENSIONS}`,
      );
    }
    operationalLogger.info("voyage.embedding_completed", {
      latencyMs: Date.now() - startedAtMs,
    });
    return embedding;
  } catch (error) {
    operationalLogger.error("voyage.embedding_failed", {
      latencyMs: Date.now() - startedAtMs,
      category: "embedding",
    });
    throw error;
  }
}

/** Give the cross-encoder episode identity as well as the transcript chunk. */
export function formatForRerank(doc: CorpusDocument): string {
  const title = compactRerankText(String(doc.title ?? "Untitled"), MAX_RERANK_TITLE_CHARS);
  const guests = compactRerankText(
    Array.isArray(doc.guests) ? doc.guests.map(String).join(", ") : "",
    MAX_RERANK_GUESTS_CHARS,
  );
  const channel = compactRerankText(String(doc.channel ?? ""), MAX_RERANK_CHANNEL_CHARS);
  let header = `[${title}`;
  if (guests) header += ` — ${guests}`;
  header += channel ? ` · ${channel}]` : "]";
  const availableTextChars = Math.max(0, MAX_RERANK_DOCUMENT_CHARS - header.length - 1);
  return `${header}\n${compactRerankText(String(doc.text ?? ""), availableTextChars)}`;
}

function compactRerankText(value: string, maxChars: number): string {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (maxChars <= 0) return "";
  return normalized.length <= maxChars ? normalized : `${normalized.slice(0, maxChars - 1).trimEnd()}…`;
}

function rankResults(payload: RerankResponse): Array<{ index: number; score: number }> {
  const results = payload.results ?? payload.data ?? [];
  return results.map((result) => ({
    index: result.index,
    score: Number(result.relevance_score ?? result.score ?? 0),
  }));
}

/**
 * Score all retrieval candidates. Unlike the source web UI's best-effort
 * fallback, production Air fails a reranker outage rather than silently
 * changing retrieval quality for a factual answer.
 */
export async function rerankCandidates(
  query: string,
  docs: CorpusDocument[],
  options: VoyageRequestOptions = {},
): Promise<CorpusDocument[]> {
  if (docs.length === 0) return [];
  if (docs.length > RERANK_CANDIDATE_LIMIT) {
    throw new Error(`Rerank candidate count exceeds ${RERANK_CANDIDATE_LIMIT}`);
  }
  const startedAtMs = Date.now();

  try {
    const documents = docs.map(formatForRerank);
    const totalChars = documents.reduce((total, document) => total + document.length, 0);
    if (totalChars > MAX_RERANK_TOTAL_CHARS) {
      throw new Error("Rerank candidate payload exceeds its bounded input budget");
    }
    const response = await voyageFetch("/rerank", {
      model: process.env.RERANK_MODEL?.trim() || "rerank-2.5",
      query,
      documents,
    }, options);
    if (!response.ok) {
      throw new Error(`Voyage rerank request failed (${response.status})`);
    }

    const ranked = rankResults((await response.json()) as RerankResponse);
    if (ranked.length === 0) {
      throw new Error("Voyage rerank returned no candidate scores");
    }

    const result = ranked.flatMap(({ index, score }) => {
      const document = docs[index];
      return document ? [{ ...document, rerank_score: score }] : [];
    });
    operationalLogger.info("voyage.rerank_completed", {
      latencyMs: Date.now() - startedAtMs,
      candidateCount: docs.length,
      resultCount: result.length,
    });
    return result;
  } catch (error) {
    operationalLogger.error("voyage.rerank_failed", {
      latencyMs: Date.now() - startedAtMs,
      candidateCount: docs.length,
      category: "rerank",
    });
    throw error;
  }
}

/** Retained for focused retrieval tests and non-agent callers. */
export async function rerank(
  query: string,
  docs: CorpusDocument[],
  topK = 8,
  options: VoyageRequestOptions = {},
): Promise<CorpusDocument[]> {
  const ranked = await rerankCandidates(query, docs, options);
  return ranked.slice(0, Math.min(topK, ranked.length));
}
