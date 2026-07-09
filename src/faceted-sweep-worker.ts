// ── Year × facet sweep worker ────────────────────────────────────
//
// Broader-slicing companion to the year × genre × style sweep in
// cache-warm.ts. Sweeps /database/search along either
//   * year × format   (Shellac / 7" / LP / 10" / 12") — surfaces
//     pre-war 78s the genre sweep hits the 10k pagination cap on
//   * year × country  (US / UK / Jamaica / Nigeria / France / …)
//     — different slicing means different tail records are reachable
//
// Same singleflight pattern as the other bulk workers: build a
// (year, value) queue on start, walk it with a persisted cursor,
// resume on boot.

import { DiscogsClient, OAuthCredentials } from "./discogs-client.js";
import {
  cacheRelease,
  getOAuthCredentials,
  getAppSetting,
  setAppSetting,
  getCachedReleaseIds,
} from "./db.js";
import { retryTransient } from "./worker-retry.js";

const STATE_KEY  = "faceted_sweep_state";
const REQ_INTERVAL_MS = 1000;

export type FacetedMode = "format" | "country";

interface QueueItem { year: number; value: string; }
interface State {
  mode:       FacetedMode;
  queue:      QueueItem[];
  cursor:     number;
  startedAt:  string;
  hits:       number;
  skipped:    number;
  errors:     number;
  lastError?: string | null;
}

let _state:         State   | null = null;
let _running:       boolean        = false;
let _stopRequested: boolean        = false;
let _adminClerkId:  string  | null = null;

const _sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

async function _persist() {
  try { await setAppSetting(STATE_KEY, _state ? JSON.stringify(_state) : null); }
  catch (err) { console.error("[faceted-sweep] persist failed:", err); }
}
async function _load(): Promise<State | null> {
  try {
    const raw = await getAppSetting(STATE_KEY);
    if (!raw) return null;
    const p = JSON.parse(raw);
    return p && Array.isArray(p.queue) && (p.mode === "format" || p.mode === "country") ? p as State : null;
  } catch { return null; }
}

async function _adminClient(): Promise<DiscogsClient | null> {
  if (!_adminClerkId) return null;
  const oauth = await getOAuthCredentials(_adminClerkId);
  if (!oauth) return null;
  if (!process.env.DISCOGS_CONSUMER_KEY || !process.env.DISCOGS_CONSUMER_SECRET) return null;
  return new DiscogsClient({
    consumerKey:    process.env.DISCOGS_CONSUMER_KEY,
    consumerSecret: process.env.DISCOGS_CONSUMER_SECRET,
    accessToken:    oauth.accessToken,
    accessSecret:   oauth.accessSecret,
  } as OAuthCredentials);
}

export function isFacetedSweepRunning(): boolean { return _running; }

export function getFacetedSweepStatus() {
  if (!_state) return {
    running: false, mode: null as FacetedMode | null,
    total: 0, cursor: 0, hits: 0, skipped: 0, errors: 0,
    startedAt: null as string | null,
    currentSlot: null as QueueItem | null,
    lastError: null as string | null,
  };
  return {
    running:     _running,
    mode:        _state.mode,
    total:       _state.queue.length,
    cursor:      _state.cursor,
    hits:        _state.hits,
    skipped:     _state.skipped,
    errors:      _state.errors,
    startedAt:   _state.startedAt,
    currentSlot: _state.queue[_state.cursor] ?? null,
    lastError:   _state.lastError ?? null,
  };
}

export function requestFacetedSweepStop(): void {
  if (_running) _stopRequested = true;
}
export function forceClearFacetedSweep(): void {
  console.log(`[faceted-sweep] FORCE clear (was running=${_running})`);
  _running = false; _stopRequested = false; _state = null;
  _persist().catch(() => {});
}

// Default value sets — sane pre-1970 picks. Caller can override.
const DEFAULT_FORMATS   = ["Shellac", "7\"", "10\"", "12\"", "LP"];
const DEFAULT_COUNTRIES = ["US", "UK", "France", "Germany", "Italy", "Japan", "Jamaica", "Brazil", "Nigeria"];

export async function startFacetedSweep(opts: {
  mode:        FacetedMode;
  yearFrom?:   number;
  yearTo?:     number;
  values?:     string[];
  resetCursor?: boolean;
}): Promise<{ ok: boolean; error?: string; queued?: number }> {
  if (_running) return { ok: false, error: "Faceted sweep already running" };
  const client = await _adminClient();
  if (!client) return { ok: false, error: "Admin Discogs OAuth not connected" };
  if (opts.mode !== "format" && opts.mode !== "country") return { ok: false, error: "mode must be 'format' or 'country'" };
  const yearFrom = Number.isFinite(opts.yearFrom as number) ? Math.trunc(opts.yearFrom as number) : 1900;
  const yearTo   = Number.isFinite(opts.yearTo   as number) ? Math.trunc(opts.yearTo   as number) : 1970;
  const values = (Array.isArray(opts.values) && opts.values.length > 0)
    ? opts.values.map(String).filter(Boolean)
    : (opts.mode === "format" ? DEFAULT_FORMATS : DEFAULT_COUNTRIES);
  const queue: QueueItem[] = [];
  for (const value of values) {
    for (let year = yearFrom; year <= yearTo; year++) {
      queue.push({ year, value });
    }
  }
  if (queue.length === 0) return { ok: false, error: "empty queue (check yearFrom/yearTo and values)" };

  const persisted = opts.resetCursor ? null : await _load();
  const reusable = persisted
    && persisted.mode === opts.mode
    && persisted.queue.length === queue.length
    && persisted.queue.every((q, i) => q.year === queue[i].year && q.value === queue[i].value);
  _state = reusable
    ? persisted
    : {
        mode: opts.mode,
        queue,
        cursor:    0,
        startedAt: new Date().toISOString(),
        hits:      0,
        skipped:   0,
        errors:    0,
        lastError: null,
      };
  _running       = true;
  _stopRequested = false;
  await _persist();
  console.log(`[faceted-sweep] START mode=${opts.mode} slots=${_state.queue.length} cursor=${_state.cursor}`);
  _run(client).catch(err => console.error("[faceted-sweep] runner crashed:", err));
  return { ok: true, queued: _state.queue.length };
}

async function _run(client: DiscogsClient): Promise<void> {
  try {
    while (_state && _state.cursor < _state.queue.length && !_stopRequested) {
      const slot = _state.queue[_state.cursor];
      try { await _sweepSlot(client, _state.mode, slot); }
      catch (err: any) {
        _state.errors++;
        _state.lastError = `${_state.mode}=${slot.value} year=${slot.year}: ${err?.message ?? String(err)}`;
        console.warn(`[faceted-sweep] ${_state.lastError}`);
      }
      _state.cursor++;
      await _persist();
      if (_stopRequested) break;
    }
  } finally {
    const drained = _state && _state.cursor >= _state.queue.length;
    _running       = false;
    _stopRequested = false;
    if (drained) console.log(`[faceted-sweep] queue drained; hits=${_state?.hits} skipped=${_state?.skipped} errors=${_state?.errors}`);
    await _persist();
  }
}

async function _sweepSlot(client: DiscogsClient, mode: FacetedMode, slot: QueueItem): Promise<void> {
  let page = 1;
  const perPage = 100;
  const maxPages = 100;
  while (page <= maxPages) {
    if (_stopRequested) return;
    const opts: any = { type: "master", year: String(slot.year), page, perPage };
    if (mode === "format") opts.format = slot.value;
    else                    opts.country = slot.value;
    let payload: any;
    try {
      payload = await retryTransient(
        () => client.search("", opts),
        { label: `faceted-sweep ${mode}=${slot.value}/${slot.year}` },
      );
    } catch (err: any) {
      const msg = String(err?.message ?? err ?? "");
      if (/404/.test(msg)) return;
      throw err;
    }
    const results: any[] = Array.isArray(payload?.results) ? payload.results : [];
    if (results.length === 0) return;

    // Batch cache check across the whole page (all master ids).
    const inScope: number[] = [];
    for (const r of results) {
      const id   = Number(r?.id);
      const kind = String(r?.type ?? "").toLowerCase();
      if (!Number.isFinite(id) || id <= 0) continue;
      if (kind !== "master") continue;
      inScope.push(id);
    }
    if (inScope.length === 0) {
      if (results.length < perPage) return;
      page++;
      continue;
    }
    const cachedSet = await getCachedReleaseIds(inScope, "master");

    for (const id of inScope) {
      if (_stopRequested) return;
      if (cachedSet.has(id)) { _state!.skipped++; continue; }
      try {
        const full = await retryTransient(
          () => client.getMasterRelease(id),
          { label: `faceted-sweep master=${id}` },
        );
        await cacheRelease(id, "master", full as any, { warmOnly: true });
        _state!.hits++;
        await _persist();
      } catch (err: any) {
        _state!.errors++;
        _state!.lastError = `master=${id}: ${err?.message ?? String(err)}`;
        console.warn(`[faceted-sweep] ${_state!.lastError}`);
      }
      await _sleep(REQ_INTERVAL_MS);
    }
    if (results.length < perPage) return;
    page++;
  }
}

export function initFacetedSweepModule(adminClerkId: string): void {
  _adminClerkId = adminClerkId || null;
  setTimeout(async () => {
    if (_running) return;
    const persisted = await _load();
    if (!persisted) return;
    if (persisted.cursor >= persisted.queue.length) {
      _state = null; await _persist(); return;
    }
    const client = await _adminClient();
    if (!client) { console.warn(`[faceted-sweep] boot-resume: no admin client, skipping`); return; }
    _state         = persisted;
    _running       = true;
    _stopRequested = false;
    console.log(`[faceted-sweep] boot-resume mode=${persisted.mode} at ${persisted.cursor}/${persisted.queue.length}`);
    _run(client).catch(err => console.error("[faceted-sweep] resume crashed:", err));
  }, 16000);
}
