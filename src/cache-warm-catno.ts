// ── Catalog-number cache-warm worker ──────────────────────────────
//
// Sibling worker to src/cache-warm.ts. Walks a small curated set of
// label + catalog-number ranges (the CATNO_SERIES seed list below),
// hitting Discogs's /database/search with catno=N&label=Label for
// each value in [cat_lo, cat_hi]. For every search result whose
// year is ≤ year_max (or has no year at all), the full release is
// fetched and cached. Cursor (current_catno) is persisted to
// cache_warm_catno_runs so a Railway restart resumes from where
// the dead worker left off.
//
// Use case: filling gaps the year/genre worker misses because of
// Discogs's 10k pagination cap. Pre-1955 race/old-time labels used
// tightly sequential catalog numbers — Excello 2000s, Paramount
// 12000s, Bluebird B-5000s, etc. — so walking the range guarantees
// every cataloged release gets a chance, regardless of how the
// year/genre facet would have surfaced (or buried) it.

import { DiscogsClient, OAuthCredentials } from "./discogs-client.js";
import {
  getCacheWarmCatnoRun,
  upsertCacheWarmCatnoRun,
  recordCacheWarmCatnoRunHit,
  recordCacheWarmCatnoRunSearched,
  recordCacheWarmCatnoRunError,
  bumpCacheWarmCatnoRunSkip,
  cacheRelease,
  getOAuthCredentials,
  getAppSetting,
  setAppSetting,
  getCachedReleaseIds,
} from "./db.js";

const ACTIVE_KEY = "cache_warm_catno_active_run";
async function _writeActiveRun(seriesKey: string): Promise<void> {
  try { await setAppSetting(ACTIVE_KEY, seriesKey); } catch {}
}
async function _clearActiveRun(): Promise<void> {
  try { await setAppSetting(ACTIVE_KEY, null); } catch {}
}
async function _readActiveRun(): Promise<string | null> {
  try { return (await getAppSetting(ACTIVE_KEY)) || null; } catch { return null; }
}

const REQ_INTERVAL_MS = 1000;

// ── Curated seed list ──────────────────────────────────────────────
// Hand-picked catalog series for labels with sequential numbering.
// Add new entries here (and they appear in the admin UI on next boot).
// `prefix` is optional — when set, the search uses "{prefix}{n}" as the
// catno value (e.g. "B-5000" for Bluebird). Most early labels used a
// bare numeric catno, so prefix can be left empty.
export interface CatnoSeries {
  key: string;       // unique slug, e.g. "excello:2000-2400"
  label: string;     // Discogs label string for the `label` search param
  prefix?: string;   // optional prefix joined to the number, e.g. "B-"
  lo: number;
  hi: number;
  yearMax: number;   // ignore (don't cache) results dated AFTER this
  notes?: string;    // freeform context shown in the admin UI
}
export const CATNO_SERIES: CatnoSeries[] = [
  {
    key: "excello:2000-2400",
    label: "Excello",
    lo: 2000, hi: 2400,
    yearMax: 1968,
    notes: "Excello 7-inch single 2000-series, Nashville swamp blues / R&B (Slim Harpo, Lightnin' Slim, Lazy Lester).",
  },
];

let _runningKey: string | null = null;
let _stopRequested: boolean    = false;
let _adminClerkIdForWorker: string | null = null;

export function initCacheWarmCatnoModule(adminClerkId: string): void {
  _adminClerkIdForWorker = adminClerkId || null;
  setTimeout(() => {
    _readActiveRun().then(active => {
      if (!active || _runningKey) return;
      console.log(`[cache-warm-catno] boot-resume: ${active}`);
      startCacheWarmCatnoRun(active).catch(err =>
        console.error("[cache-warm-catno] boot-resume failed:", err));
    }).catch(() => {});
  }, 5000);
}

export function isCacheWarmCatnoRunning(): boolean {
  return _runningKey != null;
}
export function getActiveCacheWarmCatnoKey(): string | null {
  return _runningKey;
}
export function requestCacheWarmCatnoStop(): void {
  if (_runningKey) _stopRequested = true;
  _clearActiveRun().catch(() => {});
}
export function forceClearCacheWarmCatnoRunning(): void {
  console.log(`[cache-warm-catno] FORCE clearing in-memory lock (was ${_runningKey})`);
  _runningKey = null;
  _stopRequested = false;
  _clearActiveRun().catch(() => {});
}

function _sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

function _isTransient(err: any): boolean {
  const msg = String(err?.message ?? err ?? "");
  if (/Discogs API error (5\d\d|429)/.test(msg)) return true;
  if (/timeout|abort/i.test(msg)) return true;
  if (/ECONN(RESET|REFUSED)|ENOTFOUND|EAI_AGAIN|fetch failed|socket hang up|network/i.test(msg)) return true;
  return false;
}

async function _withRetry<T>(label: string, fn: () => Promise<T>): Promise<T> {
  const delays = [2000, 4000];
  let lastErr: any;
  for (let attempt = 0; attempt < delays.length + 1; attempt++) {
    try {
      return await fn();
    } catch (err: any) {
      lastErr = err;
      if (!_isTransient(err) || attempt === delays.length) throw err;
      console.warn(`[cache-warm-catno] ${label} retry ${attempt + 1}/${delays.length} after ${err?.message ?? err}`);
      await _sleep(delays[attempt]);
    }
  }
  throw lastErr;
}

async function _adminClient(): Promise<DiscogsClient | null> {
  if (!_adminClerkIdForWorker) return null;
  const oauth = await getOAuthCredentials(_adminClerkIdForWorker);
  if (!oauth) return null;
  if (!process.env.DISCOGS_CONSUMER_KEY || !process.env.DISCOGS_CONSUMER_SECRET) return null;
  return new DiscogsClient({
    consumerKey:    process.env.DISCOGS_CONSUMER_KEY,
    consumerSecret: process.env.DISCOGS_CONSUMER_SECRET,
    accessToken:    oauth.accessToken,
    accessSecret:   oauth.accessSecret,
  } as OAuthCredentials);
}

function _findSeries(key: string): CatnoSeries | null {
  return CATNO_SERIES.find(s => s.key === key) ?? null;
}

// Loose label match — Discogs returns the label string verbatim from
// the release, which may include suffixes like " Records", "(2)", or
// the parent group name. We just need the label term to appear as a
// case-insensitive substring of any of the result's labels.
function _labelMatches(result: any, labelTerm: string): boolean {
  const term = labelTerm.toLowerCase();
  const labels = Array.isArray(result?.label) ? result.label : (result?.label ? [result.label] : []);
  for (const l of labels) {
    if (String(l).toLowerCase().includes(term)) return true;
  }
  return false;
}

export async function startCacheWarmCatnoRun(seriesKey: string, opts: { resetCursor?: boolean } = {}): Promise<{ ok: boolean; error?: string }> {
  if (_runningKey) return { ok: false, error: `Another catno run is in progress: ${_runningKey}` };
  const series = _findSeries(seriesKey);
  if (!series) return { ok: false, error: `Unknown series: ${seriesKey}` };
  const client = await _adminClient();
  if (!client) return { ok: false, error: "Admin Discogs OAuth not connected (or DISCOGS_CONSUMER_KEY/SECRET missing)" };

  _runningKey = series.key;
  _stopRequested = false;

  // Ensure the row exists with the configured shape, then read it back
  // to pick up the persisted cursor (if any). resetCursor wipes the
  // cursor only; counters are reset separately via the API endpoint.
  await upsertCacheWarmCatnoRun(series.key, {
    label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
  });
  const row = await getCacheWarmCatnoRun(series.key);
  let cursor = (opts.resetCursor || !row?.current_catno)
    ? series.lo
    : Number(row.current_catno);
  if (cursor > series.hi) cursor = series.lo;

  await upsertCacheWarmCatnoRun(series.key, {
    label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
  }, { current_catno: cursor, last_run_at: new Date() });

  await _writeActiveRun(series.key);

  console.log(`[cache-warm-catno] kicking worker for ${series.key} from catno=${cursor} (range ${series.lo}-${series.hi})`);
  (async () => {
    try {
      await _runWorker(client, series, cursor);
      console.log(`[cache-warm-catno] worker for ${series.key} exited cleanly`);
      await _clearActiveRun();
    } catch (err: any) {
      console.error(`[cache-warm-catno] worker for ${series.key} crashed:`, err?.stack || err);
      try {
        await recordCacheWarmCatnoRunError(series.key, `crash: ${err?.message ?? String(err)}`);
      } catch (e2) {
        console.error(`[cache-warm-catno] failed to record crash:`, e2);
      }
    } finally {
      console.log(`[cache-warm-catno] clearing in-memory lock for ${series.key}`);
      _runningKey = null;
      _stopRequested = false;
    }
  })().catch(err => console.error(`[cache-warm-catno] outer IIFE rejected:`, err));

  return { ok: true };
}

async function _runWorker(
  client: DiscogsClient,
  series: CatnoSeries,
  startCatno: number,
): Promise<void> {
  let n = startCatno;
  while (true) {
    if (_stopRequested) break;
    if (n > series.hi) break;

    const catnoStr = `${series.prefix ?? ""}${n}`;
    let searchRes: any;
    try {
      await _sleep(REQ_INTERVAL_MS);
      searchRes = await _withRetry(`search ${series.label} catno=${catnoStr}`, () =>
        client.search("", {
          type:    "release",
          label:   series.label,
          catno:   catnoStr,
          perPage: 5,
          page:    1,
        }),
      );
    } catch (err: any) {
      await recordCacheWarmCatnoRunError(series.key, `search ${catnoStr}: ${err?.message ?? String(err)}`);
      n += 1;
      await upsertCacheWarmCatnoRun(series.key, {
        label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
      }, { current_catno: n, last_run_at: new Date() });
      continue;
    }

    const results: any[] = Array.isArray(searchRes?.results) ? searchRes.results : [];
    await recordCacheWarmCatnoRunSearched(series.key, results.length);

    // Filter to results that actually match the label term AND fall
    // within the year cap (or have no year, which is common for pre-
    // war material — those still cache because Discogs doesn't know
    // the year, not because the release is necessarily out of scope).
    const candidates = results.filter(r => {
      if (!_labelMatches(r, series.label)) return false;
      const yr = parseInt(String(r?.year ?? ""), 10);
      if (Number.isFinite(yr) && yr > series.yearMax) return false;
      return true;
    });

    // Skip-counter bump for already-cached IDs in one shot — same
    // pattern as the genre/style worker.
    const ids: number[] = [];
    for (const r of candidates) {
      const id = Number(r?.id);
      if (Number.isFinite(id) && id > 0) ids.push(id);
    }
    let cachedIds: Set<number>;
    try { cachedIds = await getCachedReleaseIds(ids, "release"); }
    catch { cachedIds = new Set(); }
    const skipsThisN = ids.filter(id => cachedIds.has(id)).length;
    if (skipsThisN > 0) await bumpCacheWarmCatnoRunSkip(series.key, skipsThisN);

    for (const r of candidates) {
      if (_stopRequested) break;
      const id = Number(r?.id);
      if (!Number.isFinite(id) || id <= 0) continue;
      if (cachedIds.has(id)) continue;
      try {
        await _sleep(REQ_INTERVAL_MS);
        const full = await _withRetry(`release ${id}`, () => client.getRelease(id)) as any;
        // Final year guard against the full release record — search
        // results occasionally omit `year` even when the release has
        // one, so re-check before persisting.
        const fullYear = parseInt(String(full?.year ?? ""), 10);
        if (Number.isFinite(fullYear) && fullYear > series.yearMax) {
          continue;
        }
        await cacheRelease(id, "release", full as object);
        const title = String(full?.title ?? r?.title ?? "(untitled)");
        await recordCacheWarmCatnoRunHit(series.key, title, id);
      } catch (err: any) {
        await recordCacheWarmCatnoRunError(series.key, `release ${id}: ${err?.message ?? String(err)}`);
        await _sleep(REQ_INTERVAL_MS);
      }
    }
    if (_stopRequested) break;

    n += 1;
    await upsertCacheWarmCatnoRun(series.key, {
      label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
    }, { current_catno: n, last_run_at: new Date() });
  }
}
