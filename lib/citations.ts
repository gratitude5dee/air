import type { Source, SourceId } from "./types.js";
import { validateAllowedSourceDeepLink } from "./security.js";

export const MAX_CITED_SOURCES = 4;
export const MAX_ANSWER_CHARS = 1_500;
/** The tool schema and outbound transport both refuse larger final payloads. */
export const MAX_RENDERED_RESPONSE_CHARS = 4_096;
const MAX_RENDERED_TITLE_CHARS = 160;
const MAX_RENDERED_CHANNEL_CHARS = 96;
const MAX_RENDERED_TIMESTAMP_SECONDS = 7 * 24 * 60 * 60;

const CITATION_MARKER_RE = /\[([^\]\r\n]+)\]/g;
const SOURCE_ID_RE = /^S[1-9]\d*$/;
const MARKDOWN_LINK_RE = /\[[^\]\r\n]*\]\(\s*[^)\s]+\s*\)/;
// A scheme must be immediately followed by a non-space character. This catches
// arbitrary custom URI schemes while leaving ordinary prose labels like
// "Note: evidence" alone.
const URI_SCHEME_RE = /\b[a-z][a-z0-9+.-]{0,31}:(?=\S)[^\s<>()\[\]]*/i;
const EMAIL_ADDRESS_RE = /\b[^\s@<>()\[\]]+@(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}\b/i;
const BARE_DOMAIN_RE = /\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}(?::\d{1,5})?(?:[/?#][^\s<>()\[\]]*)?/i;
const URI_SCHEME_GLOBAL_RE = /\b[a-z][a-z0-9+.-]{0,31}:(?=\S)[^\s<>()\[\]]*/gi;
const EMAIL_ADDRESS_GLOBAL_RE = /\b[^\s@<>()\[\]]+@(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}\b/gi;
const BARE_DOMAIN_GLOBAL_RE = /\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}(?::\d{1,5})?(?:[/?#][^\s<>()\[\]]*)?/gi;
const MODEL_SOURCES_HEADING_RE = /^\s*(?:#{1,6}\s*)?sources\s*:/im;

/** The only citation-free answer shape that may leave the grounding gate. */
export const EVIDENCE_NOT_FOUND_ANSWER =
  "I couldn't find enough transcript evidence to answer that.";

export type CitationValidation =
  | { ok: true; citedSourceIds: SourceId[] }
  | { ok: false; citedSourceIds: SourceId[]; correction: string };

type CitationMarker = {
  raw: string;
  ids: SourceId[];
  invalid: string[];
};

function parseMarker(raw: string): CitationMarker {
  const pieces = raw
    .split(/[;,]/)
    .map((piece) => piece.trim())
    .filter(Boolean);
  const ids: SourceId[] = [];
  const invalid: string[] = [];

  for (const piece of pieces) {
    if (SOURCE_ID_RE.test(piece)) {
      ids.push(piece as SourceId);
    } else if (/\bS\d+\b/i.test(piece)) {
      // Do not silently ignore malformed source-like markers such as
      // `[S1 S2]`; they are indistinguishable from a broken citation.
      invalid.push(piece);
    }
  }

  return { raw, ids, invalid };
}

function citationMarkers(answer: string): CitationMarker[] {
  return Array.from(answer.matchAll(CITATION_MARKER_RE), (match) =>
    parseMarker(match[1] ?? ""),
  );
}

/** Preserve first-marker order while removing duplicate source references. */
export function parseCitedSourceIds(answer: string): SourceId[] {
  const seen = new Set<SourceId>();
  const ids: SourceId[] = [];
  for (const marker of citationMarkers(answer)) {
    for (const id of marker.ids) {
      if (!seen.has(id)) {
        seen.add(id);
        ids.push(id);
      }
    }
  }
  return ids;
}

function removeCitationMarkers(text: string): string {
  return text.replace(CITATION_MARKER_RE, "").replace(/\s+/g, " ").trim();
}

export function isEvidenceNotFoundAnswer(answer: string): boolean {
  return answer.trim().replace(/\s+/g, " ").toLowerCase() === EVIDENCE_NOT_FOUND_ANSWER.toLowerCase();
}

function isHeading(line: string): boolean {
  return /^#{1,6}\s+/.test(line.trim());
}

function isBullet(line: string): boolean {
  return /^\s*(?:[-*+]|\d+[.)])\s+/.test(line);
}

function isSubstantive(unit: string): boolean {
  const plain = removeCitationMarkers(unit)
    .replace(/^\s*(?:[-*+]|\d+[.)])\s+/, "")
    .trim();
  if (!plain || isHeading(unit)) return false;
  if (/^sources\s*:?$/i.test(plain)) return false;
  // Values such as "2020" and "$3M" can be factual claims too. Do not let
  // the absence of two adjacent letters turn them into an uncited loophole.
  return true;
}

/**
 * Every ordinary sentence needs its own marker. A bullet is treated as one
 * factual unit so a final marker can cover its compact, multi-clause content.
 */
function factualUnits(answer: string): string[] {
  const units: string[] = [];
  for (const line of answer.split(/\r?\n/)) {
    if (!line.trim() || isHeading(line)) continue;
    if (isBullet(line)) {
      units.push(line);
      continue;
    }
    const sentences = line.match(/[^.!?]+(?:[.!?]+|$)/g) ?? [line];
    for (const sentence of sentences) {
      if (sentence.trim()) units.push(sentence);
    }
  }
  return units;
}

function containsLinkLikeText(value: string): boolean {
  return (
    URI_SCHEME_RE.test(value) ||
    EMAIL_ADDRESS_RE.test(value) ||
    BARE_DOMAIN_RE.test(value) ||
    MARKDOWN_LINK_RE.test(value)
  );
}

export function validateCitations(
  answer: string,
  sources: Source[],
  options: { maxAnswerChars?: number } = {},
): CitationValidation {
  const normalized = answer.trim();
  const citedSourceIds = parseCitedSourceIds(normalized);
  const maxAnswerChars = options.maxAnswerChars ?? MAX_ANSWER_CHARS;

  if (!normalized) {
    return {
      ok: false,
      citedSourceIds,
      correction: "Return a concise answer or a clear statement that the corpus lacks evidence.",
    };
  }
  if (normalized.length > maxAnswerChars) {
    return {
      ok: false,
      citedSourceIds,
      correction: `The answer is ${normalized.length} characters. Rewrite it to at most ${maxAnswerChars} characters before the sources section while preserving valid citation markers.`,
    };
  }
  if (containsLinkLikeText(normalized)) {
    return {
      ok: false,
      citedSourceIds,
      correction:
        "Do not include URLs, email addresses, bare domains, URI schemes, or Markdown links in the answer body. Use only source markers such as [S1].",
    };
  }
  if (MODEL_SOURCES_HEADING_RE.test(normalized) || /^\s*(?:#{1,6}\s*)?sources\s*:?\s*$/im.test(normalized)) {
    return {
      ok: false,
      citedSourceIds,
      correction: "Do not render a Sources section; Air renders cited sources programmatically.",
    };
  }

  const malformed = citationMarkers(normalized).flatMap((marker) => marker.invalid);
  if (malformed.length > 0) {
    return {
      ok: false,
      citedSourceIds,
      correction: `Use separate valid source markers only, for example [S1] [S2]. Invalid marker content: ${malformed.join(", ")}.`,
    };
  }

  const validSourceIds = new Set<SourceId>(sources.map((source) => source.id));
  const unknown = citedSourceIds.filter((id) => !validSourceIds.has(id));
  if (unknown.length > 0) {
    return {
      ok: false,
      citedSourceIds,
      correction: `Only cite retrieved source IDs. Unknown citation IDs: ${unknown.join(", ")}.`,
    };
  }
  if (citedSourceIds.length > MAX_CITED_SOURCES) {
    return {
      ok: false,
      citedSourceIds,
      correction: `Cite at most ${MAX_CITED_SOURCES} distinct sources. Keep only the strongest retrieved evidence.`,
    };
  }

  if (citedSourceIds.length === 0) {
    if (isEvidenceNotFoundAnswer(normalized)) {
      return { ok: true, citedSourceIds: [] };
    }
    return {
      ok: false,
      citedSourceIds: [],
      correction:
        `Every supported factual sentence or bullet needs a valid retrieved source marker such as [S1]. If the corpus lacks evidence, return exactly: "${EVIDENCE_NOT_FOUND_ANSWER}"`,
    };
  }

  const uncited = factualUnits(normalized).find(
    (unit) => isSubstantive(unit) && parseCitedSourceIds(unit).length === 0,
  );
  if (uncited) {
    return {
      ok: false,
      citedSourceIds,
      correction:
        "Every non-heading factual sentence or bullet must contain at least one valid source marker. Add a marker to the unsupported sentence or remove it.",
    };
  }

  return { ok: true, citedSourceIds };
}

export function assignSourceIds(sources: Array<Omit<Source, "id"> & { id?: SourceId }>): Source[] {
  return sources.map((source, index) => ({
    ...source,
    id: source.id ?? (`S${index + 1}` as SourceId),
  }));
}

export function isAllowedDeepLink(value: string | null | undefined): value is string {
  return validateAllowedSourceDeepLink(value) !== null;
}

export function secondsToMmss(seconds: number | null | undefined): string {
  if (
    seconds === null ||
    seconds === undefined ||
    !Number.isFinite(seconds) ||
    seconds < 0 ||
    seconds > MAX_RENDERED_TIMESTAMP_SECONDS
  ) {
    return "no timestamp";
  }
  const whole = Math.floor(seconds);
  return `${Math.floor(whole / 60)}:${String(whole % 60).padStart(2, "0")}`;
}

/**
 * Source metadata is untrusted corpus text. Keep each source on one compact
 * row and remove URL-looking fragments so the only tappable links Air emits
 * have passed the explicit deep-link allowlist below.
 */
function renderSourceLabel(value: string, maxChars: number, fallback: string): string {
  const normalized = value
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(URI_SCHEME_GLOBAL_RE, "link")
    .replace(EMAIL_ADDRESS_GLOBAL_RE, "link")
    .replace(BARE_DOMAIN_GLOBAL_RE, "link")
    .replace(/\s+/g, " ")
    .trim();
  const label = normalized || fallback;
  return label.length <= maxChars ? label : `${label.slice(0, maxChars - 1).trimEnd()}…`;
}

/** Return only cited sources, ordered by their first marker in the answer. */
export function selectCitedSources(sources: Source[], citedSourceIds: SourceId[]): Source[] {
  const byId = new Map(sources.map((source) => [source.id, source]));
  return citedSourceIds.map((id) => {
    const source = byId.get(id);
    if (!source) throw new Error(`Cannot render missing cited source ${id}`);
    return source;
  });
}

export function renderSourcesSection(sources: Source[]): string {
  if (sources.length === 0) return "";
  const lines = ["Sources:"];
  for (const source of sources) {
    const title = renderSourceLabel(source.title, MAX_RENDERED_TITLE_CHARS, "Untitled");
    const channel = renderSourceLabel(source.channel, MAX_RENDERED_CHANNEL_CHARS, "Unknown channel");
    lines.push(
      `[${source.id}] ${title} — ${channel}, ${secondsToMmss(source.start_sec)}`,
    );
    // Never turn an untrusted corpus value into an iMessage link unless it is
    // an HTTPS URL on one of the explicitly allowed YouTube hosts.
    const deepLink = validateAllowedSourceDeepLink(source.deep_link);
    if (deepLink) lines.push(deepLink);
  }
  return lines.join("\n");
}

export function appendRenderedSources(answer: string, citedSources: Source[]): string {
  const sources = renderSourcesSection(citedSources);
  const rendered = sources ? `${answer.trim()}\n\n${sources}` : answer.trim();
  if (rendered.length > MAX_RENDERED_RESPONSE_CHARS) {
    // This is a programmer/corpus-boundary invariant. Let the controlled
    // grounding path reject it rather than silently truncating citations.
    throw new Error("rendered grounded response exceeds the iMessage safety limit");
  }
  return rendered;
}
