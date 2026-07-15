/**
 * Shared, deliberately small types for Air's read-only MusicRAG pipeline.
 *
 * The corpus can contain incomplete historical documents, so raw MongoDB
 * records remain untyped until `toSource()` normalizes them into `Source`.
 */

export type SourceId = `S${number}`;

export type Source = {
  id: SourceId;
  chunk_uid?: string;
  title: string;
  channel: string;
  guests: string[];
  video_id: string;
  start_sec: number | null;
  end_sec: number | null;
  deep_link: string | null;
  snippet: string;
  score: number;
  chunk_index: number | null;
};

/** A source before stable, per-answer `S#` IDs are assigned. */
export type RetrievedSource = Omit<Source, "id"> & { id?: SourceId };

export type Filters = {
  channel?: string;
  guest?: string;
  topic?: string;
  date_from_ts?: number;
  date_to_ts?: number;
};

export type Intent =
  | "entity_lookup"
  | "thematic"
  | "comparative"
  | "aggregative";

export type QueryPlan = {
  intent: Intent;
  query: string;
  guests: string[];
  channels: string[];
  topics: string[];
  subqueries: string[];
  rationale: string;
};

export type AgentTraceEvent = {
  step:
    | "classify"
    | "search_transcripts"
    | "expand_context"
    | "grade_context"
    | "synthesize"
    | "citation_validation";
  label: string;
  detail?: string;
  count?: number;
  token_count?: number;
};

export type GroundedAnswer = {
  /** Final iMessage text, including the programmatically rendered sources. */
  answer: string;
  /** Exactly the cited sources, in first-marker order. */
  sources: Source[];
  citedSourceIds: SourceId[];
  intent: Intent;
  /** Number of repair generations after the initial generation. */
  citationRetries: number;
};

/** Raw corpus records, kept broad at the database boundary. */
export type CorpusDocument = Record<string, unknown>;
