// ── MusicBrainz API client ────────────────────────────────────────
//
// Minimal wrapper around the MusicBrainz Web Service v2. MB is
// permissive: no auth, no per-account tokens — they only ask for a
// well-formed User-Agent and rate limit at 1 req/sec across the whole
// caller. We enforce that rate limit here (single in-process mutex
// queue) so a burst of admin searches doesn't get 503-throttled.
//
// All responses are cached in the musicbrainz_cache table via the
// helpers exported from db.ts; this client only emits a raw fetch.

import { logApiRequest } from "./db.js";

const BASE_URL = "https://musicbrainz.org/ws/2";
// 1 req/sec global rate limit per MB's policy. We add a small slack
// (1.1s) so the burstiest path still stays under their token bucket.
const MIN_INTERVAL_MS = 1100;

let _lastRequestAt = 0;
let _queueTail: Promise<void> = Promise.resolve();

async function _rateLimit(): Promise<void> {
  // Chain onto whatever's queued so concurrent callers serialize.
  const slot = _queueTail.then(async () => {
    const now = Date.now();
    const wait = Math.max(0, _lastRequestAt + MIN_INTERVAL_MS - now);
    if (wait > 0) await new Promise(r => setTimeout(r, wait));
    _lastRequestAt = Date.now();
  });
  _queueTail = slot.catch(() => {});
  return slot;
}

export interface MbFetchOpts {
  /** Override the per-call User-Agent (otherwise the module default
   *  with the deployment contact URL is sent). */
  userAgent?: string;
  /** Hard ms cap for the upstream fetch. Defaults to 30s. */
  timeoutMs?: number;
}

const DEFAULT_UA = "SeaDisco/1.0 (+https://seadisco.com)";

// Low-level: hit MusicBrainz with a path + query map. Caller handles
// caching — this is just the rate-limited fetch with logging.
export async function mbFetch<T = any>(
  path: string,
  params: Record<string, string | number | undefined> = {},
  opts: MbFetchOpts = {},
): Promise<T> {
  const url = new URL(`${BASE_URL}${path.startsWith("/") ? path : "/" + path}`);
  // MB always wants fmt=json; force it so callers can't accidentally
  // ask for XML and break the cache shape.
  url.searchParams.set("fmt", "json");
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null || v === "") continue;
    url.searchParams.set(k, String(v));
  }
  await _rateLimit();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), opts.timeoutMs ?? 30_000);
  const headers: Record<string, string> = {
    "User-Agent": opts.userAgent || DEFAULT_UA,
    "Accept":     "application/json",
  };
  const start = Date.now();
  let response: Response;
  try {
    response = await fetch(url.toString(), { headers, signal: controller.signal });
  } catch (err: any) {
    const ms = Date.now() - start;
    logApiRequest({ service: "musicbrainz", endpoint: url.pathname, statusCode: 0, success: false, durationMs: ms, context: "client" }).catch(() => {});
    if (err?.name === "AbortError") throw new Error(`MusicBrainz timeout: ${url.pathname}`);
    throw err;
  } finally {
    clearTimeout(timer);
  }
  const ms = Date.now() - start;
  logApiRequest({ service: "musicbrainz", endpoint: url.pathname, statusCode: response.status, success: response.ok, durationMs: ms, context: "client" }).catch(() => {});
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`MusicBrainz ${response.status}: ${body.slice(0, 200)}`);
  }
  return response.json() as Promise<T>;
}

// MB entity types we expose. Restricting at this layer keeps the API
// surface honest — server endpoints validate against the same set.
export type MbEntity =
  | "artist" | "release" | "release-group" | "recording" | "work" | "label";

export const MB_ENTITIES: MbEntity[] = [
  "artist", "release", "release-group", "recording", "work", "label",
];

// Build a Lucene-flavoured query string from a structured filter map.
// MB's search endpoint takes `query=` with Lucene syntax — empty
// values are dropped and string fields get quoted to survive spaces.
// Numeric ranges (year) pass through verbatim so callers can pass
// "1965 TO 1972" if they want a span.
export function mbBuildLuceneQuery(parts: Record<string, string | number | undefined>): string {
  const out: string[] = [];
  for (const [k, v] of Object.entries(parts)) {
    if (v === undefined || v === null || v === "") continue;
    const s = String(v).trim();
    if (!s) continue;
    // Free-form 'q' just goes in raw; everything else becomes
    // field:value with quoting for safety.
    if (k === "q") {
      out.push(s);
      continue;
    }
    // Spaces / colons need quoting. Lucene quotes use double-quotes;
    // escape internal quotes by stripping.
    const safe = s.replace(/"/g, "");
    if (/\s|:/.test(safe)) out.push(`${k}:"${safe}"`);
    else                   out.push(`${k}:${safe}`);
  }
  return out.join(" AND ");
}
