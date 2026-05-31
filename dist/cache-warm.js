// ── Manual cache-warm worker ──────────────────────────────────────
//
// Replaces the prior nightly rotation cron. One run at a time,
// initiated by an admin clicking Start with a genre and (optional)
// style. Walks Discogs releases for that combo year-by-year, caches
// each missing one into release_cache. Persists cursor + counters
// in cache_warm_runs so a restart resumes from where it left off.
//
// Rate limit: 1.1s between Discogs calls (Discogs's authed 60/min).
// Single in-process worker enforced via module-level `_runningKey`
// guard.
import { DiscogsClient } from "./discogs-client.js";
import { getCacheWarmRun, upsertCacheWarmRun, recordCacheWarmRunHit, recordCacheWarmRunSkip, recordCacheWarmRunSearched, recordCacheWarmRunError, isReleaseCached, cacheRelease, getOAuthCredentials, } from "./db.js";
const PER_PAGE = 100;
const REQ_INTERVAL_MS = 1100;
// In-memory state only — survives process lifetime, resets on
// Railway restart. Mid-run restarts leave cursor in the DB so the
// admin can re-click Start with the same params and pick up.
let _runningKey = null; // `${genre}::${style}`
let _stopRequested = false;
let _adminClerkIdForWorker = null;
// Cache the in-flight run params so /status can return them without
// re-querying the DB on every tick.
let _activeParams = null;
export function initCacheWarmModule(adminClerkId) {
    _adminClerkIdForWorker = adminClerkId || null;
}
export function isCacheWarmRunning() {
    return _runningKey != null;
}
export function getActiveCacheWarmParams() {
    return _activeParams ? { ..._activeParams } : null;
}
export function requestCacheWarmStop() {
    if (_runningKey)
        _stopRequested = true;
}
// Admin override: force-clear the in-memory lock even if the worker
// is theoretically running. Use when the state is wedged — e.g. a
// crashed worker that didn't reach its finally clause, or a stale
// _runningKey from a hot-reload during development.
export function forceClearCacheWarmRunning() {
    console.log(`[cache-warm] FORCE clearing in-memory lock (was ${_runningKey})`);
    _runningKey = null;
    _stopRequested = false;
    _activeParams = null;
}
function _sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}
function _isTransientDiscogsError(err) {
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
            if (!_isTransientDiscogsError(err) || attempt === delays.length) {
                throw err;
            }
            console.warn(`[cache-warm] ${label} retry ${attempt + 1}/${delays.length} after ${err?.message ?? err}`);
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
// Public: kick off a manual run. Rejects if another run is in
// progress. fromYear defaults to whatever cursor was last persisted
// (or startYear if no row exists); toYear caps the walk.
export async function startCacheWarmRun(opts) {
    if (_runningKey) {
        return { ok: false, error: `Another run is in progress: ${_runningKey}` };
    }
    const genreKey = String(opts.genreKey || "").trim();
    if (!genreKey)
        return { ok: false, error: "genre required" };
    const styleKey = String(opts.styleKey || "").trim();
    const client = await _adminClient();
    if (!client)
        return { ok: false, error: "Admin Discogs OAuth not connected (or DISCOGS_CONSUMER_KEY/SECRET missing)" };
    const key = `${genreKey}::${styleKey}`;
    _runningKey = key;
    _stopRequested = false;
    // Ensure a row exists; honour resetCursor if asked.
    await upsertCacheWarmRun(genreKey, styleKey, {});
    const row = await getCacheWarmRun(genreKey, styleKey);
    const toYear = opts.toYear ?? new Date().getFullYear();
    let cursorYear = (opts.resetCursor || !row?.current_year)
        ? (opts.fromYear ?? 1900)
        : Number(row.current_year);
    let cursorPage = (opts.resetCursor || !row?.current_page)
        ? 1
        : Number(row.current_page);
    // If the persisted cursor is past the cap (a previous sweep
    // completed), restart from the user's from-year (or 1900) so a
    // fresh click on Start picks up new Discogs submissions in the
    // existing range — instead of silently doing nothing.
    if (cursorYear > toYear) {
        console.log(`[cache-warm] persisted cursor ${cursorYear} > toYear ${toYear}; auto-resetting to ${opts.fromYear ?? 1900}`);
        cursorYear = opts.fromYear ?? 1900;
        cursorPage = 1;
    }
    const fromYear = Math.min(cursorYear, toYear);
    _activeParams = {
        genreKey, styleKey,
        fromYear, toYear,
        startedAt: new Date().toISOString(),
    };
    await upsertCacheWarmRun(genreKey, styleKey, {
        current_year: cursorYear,
        current_page: cursorPage,
        last_run_at: new Date(),
    });
    // Fire-and-forget: caller doesn't wait. Worker tears down state on exit.
    console.log(`[cache-warm] kicking worker for ${key}, cursor=${cursorYear}/p${cursorPage}, range=${cursorYear}-${toYear}`);
    (async () => {
        try {
            await _runWorker(client, genreKey, styleKey, cursorYear, cursorPage, toYear);
            console.log(`[cache-warm] worker for ${key} exited cleanly`);
        }
        catch (err) {
            console.error(`[cache-warm] worker for ${key} crashed:`, err?.stack || err);
            // Best-effort error log; if THIS also throws (DB down), the
            // outer .catch(() => {}) below swallows it so finally still
            // clears the in-memory lock.
            try {
                await recordCacheWarmRunError(genreKey, styleKey, `crash: ${err?.message ?? String(err)}`);
            }
            catch (e2) {
                console.error(`[cache-warm] failed to record crash for ${key}:`, e2);
            }
        }
        finally {
            console.log(`[cache-warm] clearing in-memory lock for ${key}`);
            _runningKey = null;
            _stopRequested = false;
            _activeParams = null;
        }
    })().catch(err => console.error(`[cache-warm] outer IIFE rejected for ${key}:`, err));
    return { ok: true };
}
async function _runWorker(client, genreKey, styleKey, startYear, startPage, endYear) {
    let year = startYear;
    let page = startPage;
    while (true) {
        if (_stopRequested)
            break;
        if (year > endYear)
            break;
        let searchRes;
        try {
            searchRes = await _withRetry(`search ${genreKey}/${styleKey || "*"} ${year} p${page}`, () => client.search("", {
                type: "release",
                genre: genreKey,
                style: styleKey || undefined,
                year: String(year),
                page,
                perPage: PER_PAGE,
            }));
        }
        catch (err) {
            await recordCacheWarmRunError(genreKey, styleKey, `search ${year} p${page}: ${err?.message ?? String(err)}`);
            await _sleep(5000);
            continue;
        }
        const results = Array.isArray(searchRes?.results) ? searchRes.results : [];
        await recordCacheWarmRunSearched(genreKey, styleKey, results.length);
        if (!results.length) {
            // Empty year — advance.
            year += 1;
            page = 1;
            await upsertCacheWarmRun(genreKey, styleKey, {
                current_year: year, current_page: page, last_run_at: new Date(),
            });
            continue;
        }
        for (const r of results) {
            if (_stopRequested)
                break;
            const id = Number(r?.id);
            if (!Number.isFinite(id) || id <= 0)
                continue;
            if (await isReleaseCached(id, "release")) {
                await recordCacheWarmRunSkip(genreKey, styleKey);
                continue;
            }
            try {
                await _sleep(REQ_INTERVAL_MS);
                const full = await _withRetry(`release ${id}`, () => client.getRelease(id));
                await cacheRelease(id, "release", full);
                const title = String(full?.title ?? r?.title ?? "(untitled)");
                await recordCacheWarmRunHit(genreKey, styleKey, title, id);
            }
            catch (err) {
                await recordCacheWarmRunError(genreKey, styleKey, `release ${id}: ${err?.message ?? String(err)}`);
                await _sleep(REQ_INTERVAL_MS);
            }
        }
        if (_stopRequested)
            break;
        // Page advance / year roll. Clamp the persisted year at
        // endYear+1 max — beyond that and a future Start with the same
        // toYear would silently no-op. The auto-reset in
        // startCacheWarmRun handles the +1 case (cursor past end) by
        // rewinding to fromYear, so this is just a guard against
        // persisting nonsense values.
        const totalPages = Number(searchRes?.pagination?.pages);
        const nextPage = page + 1;
        if (Number.isFinite(totalPages) && nextPage > totalPages) {
            year += 1;
            page = 1;
        }
        else {
            page = nextPage;
        }
        await upsertCacheWarmRun(genreKey, styleKey, {
            current_year: Math.min(year, endYear + 1),
            current_page: page,
            last_run_at: new Date(),
        });
    }
}
