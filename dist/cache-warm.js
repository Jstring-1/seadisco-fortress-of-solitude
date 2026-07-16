// ── Manual cache-warm worker ──────────────────────────────────────
//
// Replaces the prior nightly rotation cron. One run at a time,
// initiated by an admin clicking Start with a genre and (optional)
// style. Walks Discogs releases for that combo year-by-year, caches
// each missing one into release_cache. Persists cursor + counters
// in cache_warm_runs so a restart resumes from where it left off.
//
// Rate limit: 1.0s between Discogs calls (Discogs's authed 60/min).
// Single in-process worker enforced via module-level `_runningKey`
// guard.
import { getAdminDiscogsClient } from "./discogs-client.js";
import { getCacheWarmRun, upsertCacheWarmRun, recordCacheWarmRunHit, recordCacheWarmRunSearched, recordCacheWarmRunError, getCachedReleaseIds, getDeadDiscogsIds, recordDeadDiscogsId, bumpCacheWarmRunSkip, cacheRelease, getAppSetting, setAppSetting, } from "./db.js";
// Persisted intent so a Railway restart can auto-resume the run.
// Set when startCacheWarmRun fires, cleared by Stop or natural
// completion. Crashes do NOT clear it — boot-resume picks up where
// the dead worker left off.
const ACTIVE_KEY = "cache_warm_active_run";
async function _writeActiveRun(params) {
    try {
        await setAppSetting(ACTIVE_KEY, JSON.stringify(params));
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
        const raw = await getAppSetting(ACTIVE_KEY);
        if (!raw)
            return null;
        return JSON.parse(raw);
    }
    catch {
        return null;
    }
}
const PER_PAGE = 100;
const REQ_INTERVAL_MS = 1000;
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
    // Boot-resume: if app_settings still has an active_run record
    // from before the restart, kick the worker again. Idempotent —
    // the persisted cursor in cache_warm_runs lets the worker pick
    // up where the dead one left off.
    setTimeout(() => {
        _readActiveRun().then(active => {
            if (!active || _runningKey)
                return;
            console.log(`[cache-warm] boot-resume: ${active.genreKey}::${active.styleKey || ""}`);
            startCacheWarmRun({
                genreKey: active.genreKey,
                styleKey: active.styleKey,
                fromYear: active.fromYear,
                toYear: active.toYear,
            }).catch(err => console.error("[cache-warm] boot-resume failed:", err));
        }).catch(() => { });
    }, 5000); // brief delay so DB pool is up
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
    // Clear the persisted resume intent so a restart doesn't bring
    // a stopped run back. Fire-and-forget — worker's finally clause
    // also clears it on natural exit; both paths are safe.
    _clearActiveRun().catch(() => { });
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
    // Also clear the resume intent — otherwise the next restart
    // would helpfully bring the wedged run back.
    _clearActiveRun().catch(() => { });
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
    const c = await getAdminDiscogsClient(_adminClerkIdForWorker);
    // Lowest rate-lane priority: yield to user-facing + scheduled traffic.
    return c ? c.withPriority("sweep") : null;
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
    // Persist the resume intent so a Railway restart auto-resumes
    // this combo from the persisted cursor. Cleared by Stop, natural
    // completion, or Force-clear.
    await _writeActiveRun({ genreKey, styleKey, fromYear: opts.fromYear, toYear: opts.toYear });
    // No-year sweeps (fromYear=0 toYear=0) must not touch the dated
    // cursor — the two sweep histories are kept independent so the
    // "no-year done" indicator and the dated resume point survive each
    // other.
    if (opts.fromYear === 0 && opts.toYear === 0) {
        await upsertCacheWarmRun(genreKey, styleKey, { last_run_at: new Date() });
    }
    else {
        await upsertCacheWarmRun(genreKey, styleKey, {
            current_year: cursorYear,
            current_page: cursorPage,
            last_run_at: new Date(),
        });
    }
    // Fire-and-forget: caller doesn't wait. Worker tears down state on exit.
    console.log(`[cache-warm] kicking worker for ${key}, cursor=${cursorYear}/p${cursorPage}, range=${cursorYear}-${toYear}${opts.alsoNoYear ? " + no-year follow-up" : ""}`);
    (async () => {
        try {
            await _runWorker(client, genreKey, styleKey, cursorYear, cursorPage, toYear);
            console.log(`[cache-warm] worker for ${key} exited cleanly`);
            // Chained no-year follow-up: only fires when the dated sweep
            // exited cleanly (not stopped). One click on "▶ 1900-1970" then
            // covers both the dated walk AND the long-tail no-year releases.
            if (opts.alsoNoYear && !_stopRequested) {
                console.log(`[cache-warm] chaining no-year sweep for ${key}`);
                await _runWorker(client, genreKey, styleKey, 0, 1, 0);
                console.log(`[cache-warm] no-year follow-up for ${key} exited cleanly`);
            }
            // Natural exit = clear the resume intent. Crashes leave it
            // set so the next boot will retry.
            await _clearActiveRun();
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
        // year === 0 is the "no-year sweep" mode — omit the year filter
        // so Discogs returns releases that have no year set. Year-filtered
        // sweeps (e.g. 1900-1970) skip these, so this catches the long
        // tail. Worker still walks pages within this single pseudo-year
        // then breaks once endYear (also 0) is exceeded.
        const noYearMode = year === 0;
        try {
            searchRes = await _withRetry(`search ${genreKey}/${styleKey || "*"} ${noYearMode ? "no-year" : year} p${page}`, () => 
            // Sweep MASTERS rather than individual releases. Each master
            // already aggregates pressings, year, primary artist, tracklist,
            // and cover — so for the app's wide-card / search / feed views
            // it's the unit we actually want cached. Sweeping releases
            // instead would pay the Discogs throttle for every pressing of
            // the same album (potentially dozens). Orphan releases — ones
            // with no parent master — are not surfaced by this search and
            // would need a separate type=release pass filtered to
            // master_id == null. Tracked as a possible future button.
            client.search("", {
                type: "master",
                genre: genreKey,
                style: styleKey || undefined,
                year: noYearMode ? undefined : String(year),
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
            if (noYearMode) {
                // No-year sweep finished a page set: stamp no_year_last_run_at
                // and leave the dated cursor (current_year / current_page)
                // untouched so a subsequent dated run resumes where it left off.
                await upsertCacheWarmRun(genreKey, styleKey, {
                    no_year_last_run_at: new Date(), last_run_at: new Date(),
                });
            }
            else {
                await upsertCacheWarmRun(genreKey, styleKey, {
                    current_year: year, current_page: page, last_run_at: new Date(),
                });
            }
            continue;
        }
        // One batch cache-check + one bulk skip-counter bump per page
        // instead of N round-trips. Re-runs over already-cached genres
        // used to spend ~2 DB calls per result (SELECT + UPDATE) on every
        // hit; this collapses each page's 100 hits to 2 queries total.
        // Genre/style master searches carry no pressing-level facet, so
        // Discogs returns genuine master rows (id === master_id) — but guard
        // by type anyway, and skip any row that isn't actually a master.
        const pageIds = [];
        for (const r of results) {
            const id = Number(r?.id);
            if (String(r?.type ?? "").toLowerCase() !== "master")
                continue;
            if (Number.isFinite(id) && id > 0)
                pageIds.push(id);
        }
        let cachedIds;
        let deadIds;
        try {
            [cachedIds, deadIds] = await Promise.all([
                getCachedReleaseIds(pageIds, "master"),
                getDeadDiscogsIds(pageIds, "master"),
            ]);
        }
        catch {
            cachedIds = new Set();
            deadIds = new Set();
        }
        const skipsThisPage = pageIds.filter(id => cachedIds.has(id) || deadIds.has(id)).length;
        if (skipsThisPage > 0)
            await bumpCacheWarmRunSkip(genreKey, styleKey, skipsThisPage);
        for (const r of results) {
            if (_stopRequested)
                break;
            const id = Number(r?.id);
            if (!Number.isFinite(id) || id <= 0)
                continue;
            if (String(r?.type ?? "").toLowerCase() !== "master")
                continue;
            // Skip already-cached and known-dead (previously-404'd) ids.
            if (cachedIds.has(id) || deadIds.has(id))
                continue;
            try {
                // Pacing is now enforced process-wide by discogsGate() inside
                // DiscogsClient — no local sleep, so a solo run rides the gate
                // at ~1/sec instead of gate + local sleep ≈ 2s/item.
                const full = await _withRetry(`master ${id}`, () => client.getMasterRelease(id));
                await cacheRelease(id, "master", full);
                const title = String(full?.title ?? r?.title ?? "(untitled)");
                await recordCacheWarmRunHit(genreKey, styleKey, title, id);
            }
            catch (err) {
                await recordCacheWarmRunError(genreKey, styleKey, `master ${id}: ${err?.message ?? String(err)}`);
                // Tombstone genuine 404s so future sweeps stop re-fetching them.
                if (/Discogs API error 404/.test(String(err?.message ?? err))) {
                    await recordDeadDiscogsId(id, "master").catch(() => { });
                }
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
        if (noYearMode) {
            // Stamp the no-year fields and bump pages-seen; leave the dated
            // cursor alone so the two sweep histories don't clobber each other.
            await upsertCacheWarmRun(genreKey, styleKey, {
                no_year_last_run_at: new Date(),
                no_year_pages_seen: page,
                last_run_at: new Date(),
            });
        }
        else {
            await upsertCacheWarmRun(genreKey, styleKey, {
                current_year: Math.min(year, endYear + 1),
                current_page: page,
                last_run_at: new Date(),
            });
        }
    }
}
