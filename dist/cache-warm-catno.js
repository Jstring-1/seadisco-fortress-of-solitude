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
import { DiscogsClient } from "./discogs-client.js";
import { getCacheWarmCatnoRun, upsertCacheWarmCatnoRun, recordCacheWarmCatnoRunHit, recordCacheWarmCatnoRunSearched, recordCacheWarmCatnoRunError, bumpCacheWarmCatnoRunSkip, cacheRelease, getOAuthCredentials, getAppSetting, setAppSetting, getCachedReleaseIds, } from "./db.js";
const ACTIVE_KEY = "cache_warm_catno_active_run";
async function _writeActiveRun(seriesKey) {
    try {
        await setAppSetting(ACTIVE_KEY, seriesKey);
    }
    catch { }
}
async function _clearActiveRun() {
    try {
        await setAppSetting(ACTIVE_KEY, null);
    }
    catch { }
}
async function _readActiveRun() {
    try {
        return (await getAppSetting(ACTIVE_KEY)) || null;
    }
    catch {
        return null;
    }
}
const REQ_INTERVAL_MS = 1000;
export const CATNO_SERIES = [
    {
        key: "excello:2000-2400",
        label: "Excello",
        lo: 2000, hi: 2400,
        yearMax: 1968,
        notes: "Excello 7-inch single 2000-series, Nashville swamp blues / R&B (Slim Harpo, Lightnin' Slim, Lazy Lester).",
    },
];
let _runningKey = null;
let _stopRequested = false;
let _adminClerkIdForWorker = null;
export function initCacheWarmCatnoModule(adminClerkId) {
    _adminClerkIdForWorker = adminClerkId || null;
    setTimeout(() => {
        _readActiveRun().then(active => {
            if (!active || _runningKey)
                return;
            console.log(`[cache-warm-catno] boot-resume: ${active}`);
            startCacheWarmCatnoRun(active).catch(err => console.error("[cache-warm-catno] boot-resume failed:", err));
        }).catch(() => { });
    }, 5000);
}
export function isCacheWarmCatnoRunning() {
    return _runningKey != null;
}
export function getActiveCacheWarmCatnoKey() {
    return _runningKey;
}
export function requestCacheWarmCatnoStop() {
    if (_runningKey)
        _stopRequested = true;
    _clearActiveRun().catch(() => { });
}
export function forceClearCacheWarmCatnoRunning() {
    console.log(`[cache-warm-catno] FORCE clearing in-memory lock (was ${_runningKey})`);
    _runningKey = null;
    _stopRequested = false;
    _clearActiveRun().catch(() => { });
}
function _sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}
function _isTransient(err) {
    const msg = String(err?.message ?? err ?? "");
    if (/Discogs API error (5\d\d|429)/.test(msg))
        return true;
    if (/timeout|abort/i.test(msg))
        return true;
    if (/ECONN(RESET|REFUSED)|ENOTFOUND|EAI_AGAIN|fetch failed|socket hang up|network/i.test(msg))
        return true;
    return false;
}
async function _withRetry(label, fn) {
    const delays = [2000, 4000];
    let lastErr;
    for (let attempt = 0; attempt < delays.length + 1; attempt++) {
        try {
            return await fn();
        }
        catch (err) {
            lastErr = err;
            if (!_isTransient(err) || attempt === delays.length)
                throw err;
            console.warn(`[cache-warm-catno] ${label} retry ${attempt + 1}/${delays.length} after ${err?.message ?? err}`);
            await _sleep(delays[attempt]);
        }
    }
    throw lastErr;
}
async function _adminClient() {
    if (!_adminClerkIdForWorker)
        return null;
    const oauth = await getOAuthCredentials(_adminClerkIdForWorker);
    if (!oauth)
        return null;
    if (!process.env.DISCOGS_CONSUMER_KEY || !process.env.DISCOGS_CONSUMER_SECRET)
        return null;
    return new DiscogsClient({
        consumerKey: process.env.DISCOGS_CONSUMER_KEY,
        consumerSecret: process.env.DISCOGS_CONSUMER_SECRET,
        accessToken: oauth.accessToken,
        accessSecret: oauth.accessSecret,
    });
}
function _findSeries(key) {
    return CATNO_SERIES.find(s => s.key === key) ?? null;
}
// Loose label match — Discogs returns the label string verbatim from
// the release, which may include suffixes like " Records", "(2)", or
// the parent group name. We just need the label term to appear as a
// case-insensitive substring of any of the result's labels.
function _labelMatches(result, labelTerm) {
    const term = labelTerm.toLowerCase();
    const labels = Array.isArray(result?.label) ? result.label : (result?.label ? [result.label] : []);
    for (const l of labels) {
        if (String(l).toLowerCase().includes(term))
            return true;
    }
    return false;
}
export async function startCacheWarmCatnoRun(seriesKey, opts = {}) {
    if (_runningKey)
        return { ok: false, error: `Another catno run is in progress: ${_runningKey}` };
    const series = _findSeries(seriesKey);
    if (!series)
        return { ok: false, error: `Unknown series: ${seriesKey}` };
    const client = await _adminClient();
    if (!client)
        return { ok: false, error: "Admin Discogs OAuth not connected (or DISCOGS_CONSUMER_KEY/SECRET missing)" };
    _runningKey = series.key;
    _stopRequested = false;
    await upsertCacheWarmCatnoRun(series.key, {
        label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
    });
    const row = await getCacheWarmCatnoRun(series.key);
    let phase = String(row?.phase ?? "catno");
    let catnoCursor = (opts.resetCursor || !row?.current_catno)
        ? series.lo
        : Number(row.current_catno);
    if (catnoCursor > series.hi && phase === "catno") {
        // Catno walk previously finished but phase didn't advance — bump it
        // so we don't re-walk the same range.
        phase = "label_sweep";
    }
    let sweepPage = (opts.resetCursor || !row?.label_sweep_page)
        ? 1
        : Number(row.label_sweep_page);
    if (opts.resetCursor) {
        phase = "catno";
        catnoCursor = series.lo;
        sweepPage = 1;
    }
    await upsertCacheWarmCatnoRun(series.key, {
        label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
    }, { current_catno: catnoCursor, phase, label_sweep_page: sweepPage, last_run_at: new Date() });
    await _writeActiveRun(series.key);
    console.log(`[cache-warm-catno] kicking catno-walk worker for ${series.key} from catno=${catnoCursor} (range ${series.lo}-${series.hi})`);
    (async () => {
        try {
            await _runCatnoPhase(client, series, catnoCursor);
            if (!_stopRequested) {
                // Catno phase done. We no longer auto-chain into the label
                // sweep — the curator kicks that explicitly with the
                // "Sweep label" button when they want the catalog tail too.
                await upsertCacheWarmCatnoRun(series.key, {
                    label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
                }, { phase: "catno_done", last_run_at: new Date() });
            }
            console.log(`[cache-warm-catno] catno-walk worker for ${series.key} exited cleanly`);
            await _clearActiveRun();
        }
        catch (err) {
            console.error(`[cache-warm-catno] worker for ${series.key} crashed:`, err?.stack || err);
            try {
                await recordCacheWarmCatnoRunError(series.key, `crash: ${err?.message ?? String(err)}`);
            }
            catch (e2) {
                console.error(`[cache-warm-catno] failed to record crash:`, e2);
            }
        }
        finally {
            console.log(`[cache-warm-catno] clearing in-memory lock for ${series.key}`);
            _runningKey = null;
            _stopRequested = false;
        }
    })().catch(err => console.error(`[cache-warm-catno] outer IIFE rejected:`, err));
    return { ok: true };
}
// Independent label-sweep run — paginated /database/search?label=X
// with no catno filter. Catches releases with catnos outside the
// configured [lo, hi] range, releases with no catno at all, and one-
// off variants the catno walk misses. Resumes from the persisted
// label_sweep_page unless resetCursor=true.
export async function startLabelSweepRun(seriesKey, opts = {}) {
    if (_runningKey)
        return { ok: false, error: `Another catno run is in progress: ${_runningKey}` };
    const series = _findSeries(seriesKey);
    if (!series)
        return { ok: false, error: `Unknown series: ${seriesKey}` };
    const client = await _adminClient();
    if (!client)
        return { ok: false, error: "Admin Discogs OAuth not connected (or DISCOGS_CONSUMER_KEY/SECRET missing)" };
    _runningKey = series.key;
    _stopRequested = false;
    await upsertCacheWarmCatnoRun(series.key, {
        label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
    });
    const row = await getCacheWarmCatnoRun(series.key);
    let sweepPage = (opts.resetCursor || !row?.label_sweep_page)
        ? 1
        : Number(row.label_sweep_page);
    if (!Number.isFinite(sweepPage) || sweepPage < 1)
        sweepPage = 1;
    await upsertCacheWarmCatnoRun(series.key, {
        label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
    }, { phase: "label_sweep", label_sweep_page: sweepPage, last_run_at: new Date() });
    await _writeActiveRun(series.key);
    console.log(`[cache-warm-catno] kicking label-sweep worker for ${series.key} from page=${sweepPage}`);
    (async () => {
        try {
            await _runLabelSweepPhase(client, series, sweepPage);
            if (!_stopRequested) {
                await upsertCacheWarmCatnoRun(series.key, {
                    label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
                }, { phase: "label_sweep_done", last_run_at: new Date() });
            }
            console.log(`[cache-warm-catno] label-sweep worker for ${series.key} exited cleanly`);
            await _clearActiveRun();
        }
        catch (err) {
            console.error(`[cache-warm-catno] label-sweep worker for ${series.key} crashed:`, err?.stack || err);
            try {
                await recordCacheWarmCatnoRunError(series.key, `crash: ${err?.message ?? String(err)}`);
            }
            catch (e2) {
                console.error(`[cache-warm-catno] failed to record crash:`, e2);
            }
        }
        finally {
            console.log(`[cache-warm-catno] clearing in-memory lock for ${series.key}`);
            _runningKey = null;
            _stopRequested = false;
        }
    })().catch(err => console.error(`[cache-warm-catno] label-sweep IIFE rejected:`, err));
    return { ok: true };
}
// ── Shared candidate processor ────────────────────────────────────
// Filter the search payload to results that actually carry the label
// term + fall within the year cap, cache the missing ones, bump skip
// counter for the rest. Returns the number of fresh hits cached so
// the caller can decide when to stop a paginated phase.
async function _processSearchResults(client, series, results) {
    await recordCacheWarmCatnoRunSearched(series.key, results.length);
    // Year filter dropped intentionally — every label-matched release is
    // cached regardless of year. The yearMax field on the seed entry is
    // kept for documentation only.
    const candidates = results.filter(r => _labelMatches(r, series.label));
    const ids = [];
    for (const r of candidates) {
        const id = Number(r?.id);
        if (Number.isFinite(id) && id > 0)
            ids.push(id);
    }
    let cachedIds;
    try {
        cachedIds = await getCachedReleaseIds(ids, "release");
    }
    catch {
        cachedIds = new Set();
    }
    const skipsThisN = ids.filter(id => cachedIds.has(id)).length;
    if (skipsThisN > 0)
        await bumpCacheWarmCatnoRunSkip(series.key, skipsThisN);
    let fresh = 0;
    for (const r of candidates) {
        if (_stopRequested)
            break;
        const id = Number(r?.id);
        if (!Number.isFinite(id) || id <= 0)
            continue;
        if (cachedIds.has(id))
            continue;
        try {
            await _sleep(REQ_INTERVAL_MS);
            const full = await _withRetry(`release ${id}`, () => client.getRelease(id));
            await cacheRelease(id, "release", full);
            const title = String(full?.title ?? r?.title ?? "(untitled)");
            await recordCacheWarmCatnoRunHit(series.key, title, id);
            fresh++;
        }
        catch (err) {
            await recordCacheWarmCatnoRunError(series.key, `release ${id}: ${err?.message ?? String(err)}`);
            await _sleep(REQ_INTERVAL_MS);
        }
    }
    return fresh;
}
// ── Phase 1: walk the configured catno range ─────────────────────
async function _runCatnoPhase(client, series, startCatno) {
    let n = startCatno;
    while (true) {
        if (_stopRequested)
            break;
        if (n > series.hi)
            break;
        const catnoStr = `${series.prefix ?? ""}${n}`;
        let searchRes;
        try {
            await _sleep(REQ_INTERVAL_MS);
            searchRes = await _withRetry(`search ${series.label} catno=${catnoStr}`, () => client.search("", {
                type: "release",
                label: series.label,
                catno: catnoStr,
                perPage: 5,
                page: 1,
            }));
        }
        catch (err) {
            await recordCacheWarmCatnoRunError(series.key, `search ${catnoStr}: ${err?.message ?? String(err)}`);
            n += 1;
            await upsertCacheWarmCatnoRun(series.key, {
                label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
            }, { current_catno: n, last_run_at: new Date() });
            continue;
        }
        const results = Array.isArray(searchRes?.results) ? searchRes.results : [];
        await _processSearchResults(client, series, results);
        if (_stopRequested)
            break;
        n += 1;
        await upsertCacheWarmCatnoRun(series.key, {
            label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
        }, { current_catno: n, last_run_at: new Date() });
    }
}
// ── Phase 2: paginate the whole label ────────────────────────────
// After the configured catno range is fully walked, sweep every
// release Discogs has tagged with this label. Catches releases with
// catnos outside the [lo, hi] range, releases with no catno at all,
// and one-off variants the sequential walk would miss. Year filter
// still applies. Discogs caps label search at 10000 results (~100
// pages × 100 per page); the worker stops when pagination exhausts
// or when an empty page comes back.
const LABEL_SWEEP_PER_PAGE = 100;
async function _runLabelSweepPhase(client, series, startPage) {
    let page = Math.max(1, startPage);
    while (true) {
        if (_stopRequested)
            break;
        let searchRes;
        try {
            await _sleep(REQ_INTERVAL_MS);
            searchRes = await _withRetry(`label-sweep ${series.label} p${page}`, () => client.search("", {
                type: "release",
                label: series.label,
                perPage: LABEL_SWEEP_PER_PAGE,
                page,
            }));
        }
        catch (err) {
            await recordCacheWarmCatnoRunError(series.key, `label-sweep p${page}: ${err?.message ?? String(err)}`);
            // Skip the bad page so we don't loop forever on a server-side glitch.
            page += 1;
            await upsertCacheWarmCatnoRun(series.key, {
                label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
            }, { label_sweep_page: page, last_run_at: new Date() });
            continue;
        }
        const results = Array.isArray(searchRes?.results) ? searchRes.results : [];
        if (!results.length)
            break; // pagination exhausted
        await _processSearchResults(client, series, results);
        if (_stopRequested)
            break;
        const totalPages = Number(searchRes?.pagination?.pages);
        const next = page + 1;
        page = next;
        await upsertCacheWarmCatnoRun(series.key, {
            label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
        }, { label_sweep_page: page, last_run_at: new Date() });
        if (Number.isFinite(totalPages) && next > totalPages)
            break;
    }
}
