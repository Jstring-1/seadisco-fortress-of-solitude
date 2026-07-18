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
import { getAdminDiscogsClient } from "./discogs-client.js";
import { getCacheWarmCatnoRun, upsertCacheWarmCatnoRun, recordCacheWarmCatnoRunHit, recordCacheWarmCatnoRunSearched, recordCacheWarmCatnoRunError, bumpCacheWarmCatnoRunSkip, cacheRelease, getAppSetting, setAppSetting, getCachedReleaseIds, getDeadDiscogsIds, recordDeadDiscogsId, backfillCachedMasterLabel, } from "./db.js";
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
        labelId: 51225,
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
    setTimeout(async () => {
        if (_runningKey)
            return;
        try {
            const active = await _readActiveRun();
            if (!active)
                return;
            console.log(`[cache-warm-catno] boot-resume candidate: ${active}`);
            const row = await getCacheWarmCatnoRun(active);
            const phase = String(row?.phase ?? "catno");
            // Done states don't resume.
            if (phase === "catno_done" || phase === "label_sweep_done") {
                console.log(`[cache-warm-catno] boot-resume skipped — phase ${phase}`);
                await _clearActiveRun();
                return;
            }
            // Ad-hoc keys came from the Label directory's per-row sweep. The
            // labelId is in the key suffix; the row carries the label name.
            if (active.startsWith("adhoc:")) {
                const labelId = Number(active.slice("adhoc:".length));
                const labelName = String(row?.label ?? "");
                if (!Number.isFinite(labelId) || !labelName) {
                    console.warn(`[cache-warm-catno] adhoc boot-resume malformed (${active}); clearing`);
                    await _clearActiveRun();
                    return;
                }
                await startAdHocLabelSweep(labelId, labelName).catch(err => console.error("[cache-warm-catno] adhoc boot-resume failed:", err));
                return;
            }
            // Curated seeds — dispatch by phase.
            if (phase === "label_sweep_masters" || phase === "label_sweep_orphans") {
                await startLabelSweepRun(active).catch(err => console.error("[cache-warm-catno] label-sweep boot-resume failed:", err));
            }
            else {
                await startCacheWarmCatnoRun(active).catch(err => console.error("[cache-warm-catno] catno boot-resume failed:", err));
            }
        }
        catch (err) {
            console.error("[cache-warm-catno] boot-resume check failed:", err);
        }
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
    const c = await getAdminDiscogsClient(_adminClerkIdForWorker);
    // Lowest rate-lane priority: yield to user-facing + scheduled traffic.
    return c ? c.withPriority("sweep") : null;
}
function _findSeries(key) {
    return CATNO_SERIES.find(s => s.key === key) ?? null;
}
// label_id → ISO timestamp of the most recent completed sweep, if any.
// A "completed" sweep is a catno-runs row in phase label_sweep_done or
// catno_done. Ad-hoc keys (`adhoc:{labelId}`) map directly; curated
// series carry labelId inside CATNO_SERIES. Used by the label
// directory endpoint AND the bulk sweep worker to skip labels that
// were swept recently.
export async function buildLabelSweptMap() {
    const { listCacheWarmCatnoRuns } = await import("./db.js");
    const runs = await listCacheWarmCatnoRuns();
    const map = new Map();
    const donePhases = new Set(["label_sweep_done", "catno_done"]);
    for (const run of runs) {
        if (!donePhases.has(run.phase) || !run.last_run_at)
            continue;
        let labelId = null;
        if (String(run.series_key).startsWith("adhoc:")) {
            const n = Number(String(run.series_key).slice("adhoc:".length));
            if (Number.isFinite(n) && n > 0)
                labelId = n;
        }
        else {
            const series = CATNO_SERIES.find(s => s.key === run.series_key);
            if (series?.labelId)
                labelId = series.labelId;
        }
        if (labelId == null)
            continue;
        const ts = run.last_run_at instanceof Date
            ? run.last_run_at.toISOString()
            : String(run.last_run_at);
        const existing = map.get(labelId);
        if (!existing || ts > existing)
            map.set(labelId, ts);
    }
    return map;
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
    // The hi field is informational only — the walker keeps going past
    // it until the empty-streak detector catches the end of the catalog.
    let catnoCursor = (opts.resetCursor || !row?.current_catno)
        ? series.lo
        : Number(row.current_catno);
    let sweepPage = (opts.resetCursor || !row?.label_sweep_page)
        ? 1
        : Number(row.label_sweep_page);
    if (opts.resetCursor) {
        catnoCursor = series.lo;
        sweepPage = 1;
    }
    // Force phase back to catno — caller picked the "Catno walk" path,
    // so any prior "catno_done" / "label_sweep_*" state is irrelevant
    // for this run.
    const phase = "catno";
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
// Resolves where a (re)started label sweep should pick up: resume the
// masters or orphans pass at its persisted page, or start fresh at
// masters p1 (resetCursor, no prior run, or a prior run whose phase
// predates the masters/orphans split — e.g. "label_sweep_done" or the
// legacy single-phase "label_sweep").
function _resumeLabelSweepCursor(row, resetCursor) {
    if (!resetCursor && (row?.phase === "label_sweep_masters" || row?.phase === "label_sweep_orphans")) {
        const page = Number(row.label_sweep_page);
        return {
            subphase: row.phase === "label_sweep_orphans" ? "orphans" : "masters",
            page: Number.isFinite(page) && page >= 1 ? page : 1,
        };
    }
    return { subphase: "masters", page: 1 };
}
// Independent label-sweep run — paginated /database/search?label=X
// with no catno filter, in two passes: every master, then every
// orphan release (no parent master). Catches masters + one-off
// pressings the catno walk misses. Resumes from the persisted
// (subphase, label_sweep_page) unless resetCursor=true.
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
    const { subphase, page: sweepPage } = _resumeLabelSweepCursor(row, opts.resetCursor);
    await upsertCacheWarmCatnoRun(series.key, {
        label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
    }, { phase: subphase === "masters" ? "label_sweep_masters" : "label_sweep_orphans", label_sweep_page: sweepPage, last_run_at: new Date() });
    await _writeActiveRun(series.key);
    console.log(`[cache-warm-catno] kicking label-sweep worker for ${series.key} (${subphase}) from page=${sweepPage}`);
    (async () => {
        try {
            await _runLabelSweepPhase(client, series, sweepPage, subphase);
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
// Ad-hoc label sweep launched from the Labels admin grid. Same
// `_runLabelSweepPhase` as the curated `startLabelSweepRun`, but the
// CatnoSeries is built inline from (labelId, labelName) — no seed
// entry needed. State lives under `series_key = adhoc:{labelId}` so
// boot-resume can reconstruct everything from the row + key.
export async function startAdHocLabelSweep(labelId, labelName, opts = {}) {
    if (!Number.isFinite(labelId) || labelId <= 0)
        return { ok: false, error: "labelId required" };
    const safeName = String(labelName ?? "").trim();
    if (!safeName)
        return { ok: false, error: "labelName required" };
    if (_runningKey)
        return { ok: false, error: `Another catno run is in progress: ${_runningKey}` };
    const client = await _adminClient();
    if (!client)
        return { ok: false, error: "Admin Discogs OAuth not connected (or DISCOGS_CONSUMER_KEY/SECRET missing)" };
    const key = `adhoc:${labelId}`;
    const ephemeral = {
        key,
        label: safeName,
        labelId,
        lo: 0, hi: 0,
        yearMax: 9999,
        notes: "Ad-hoc sweep from Label directory",
    };
    _runningKey = key;
    _stopRequested = false;
    await upsertCacheWarmCatnoRun(key, {
        label: safeName, catLo: 0, catHi: 0, yearMax: 9999,
    });
    const row = await getCacheWarmCatnoRun(key);
    const { subphase, page: sweepPage } = _resumeLabelSweepCursor(row, opts.resetCursor);
    await upsertCacheWarmCatnoRun(key, {
        label: safeName, catLo: 0, catHi: 0, yearMax: 9999,
    }, { phase: subphase === "masters" ? "label_sweep_masters" : "label_sweep_orphans", label_sweep_page: sweepPage, last_run_at: new Date() });
    await _writeActiveRun(key);
    console.log(`[cache-warm-catno] ad-hoc sweep ${key} (${safeName}, ${subphase}) from page=${sweepPage}`);
    (async () => {
        try {
            await _runLabelSweepPhase(client, ephemeral, sweepPage, subphase);
            if (!_stopRequested) {
                await upsertCacheWarmCatnoRun(key, {
                    label: safeName, catLo: 0, catHi: 0, yearMax: 9999,
                }, { phase: "label_sweep_done", last_run_at: new Date() });
            }
            console.log(`[cache-warm-catno] ad-hoc sweep ${key} exited cleanly`);
            await _clearActiveRun();
        }
        catch (err) {
            console.error(`[cache-warm-catno] ad-hoc sweep ${key} crashed:`, err?.stack || err);
            try {
                await recordCacheWarmCatnoRunError(key, `crash: ${err?.message ?? String(err)}`);
            }
            catch (e2) {
                console.error(`[cache-warm-catno] failed to record crash:`, e2);
            }
        }
        finally {
            _runningKey = null;
            _stopRequested = false;
        }
    })().catch(err => console.error(`[cache-warm-catno] ad-hoc sweep IIFE rejected:`, err));
    return { ok: true, seriesKey: key };
}
// ── Shared candidate processor ────────────────────────────────────
// Filter the search payload to results that actually carry the label
// term, cache the missing ones, bump skip counter for the rest.
// Returns the number of label-matched results (regardless of whether
// they were new or already cached) so the caller can detect the end
// of a sequential catalog by counting consecutive empty catnos.
async function _processSearchResults(client, series, results, opts = {}) {
    await recordCacheWarmCatnoRunSearched(series.key, results.length);
    const mastersPlus = !!opts.mastersPlus;
    const orphansOnly = !!opts.orphansOnly;
    // Year filter dropped intentionally — every label-matched hit is
    // cached regardless of year. yearMax on the seed is documentation.
    const matched = results.filter(r => _labelMatches(r, series.label));
    // Stash the label-matched count so the catno walker can read it
    // without re-walking the list.
    _processSearchResults.lastMatchedCount = matched.length;
    const targets = [];
    const seen = new Set();
    for (const r of matched) {
        const kind = String(r?.type ?? "").toLowerCase();
        const rid = Number(r?.id);
        const mid = Number(r?.master_id) || 0;
        let tId, tType;
        if (kind === "master") {
            tId = rid;
            tType = "master";
        }
        else if (mid > 0) {
            if (orphansOnly)
                continue; // pressing w/ master — skip
            if (mastersPlus) {
                tId = mid;
                tType = "master";
            }
            else {
                tId = rid;
                tType = "release";
            }
        }
        else {
            tId = rid;
            tType = "release";
        } // orphan
        if (!Number.isFinite(tId) || tId <= 0)
            continue;
        const key = `${tType}:${tId}`;
        if (seen.has(key))
            continue;
        seen.add(key);
        targets.push({ id: tId, type: tType });
    }
    const masterIds = targets.filter(t => t.type === "master").map(t => t.id);
    const releaseIds = targets.filter(t => t.type === "release").map(t => t.id);
    let cachedM, deadM, cachedR, deadR;
    try {
        [cachedM, deadM, cachedR, deadR] = await Promise.all([
            getCachedReleaseIds(masterIds, "master"),
            getDeadDiscogsIds(masterIds, "master"),
            getCachedReleaseIds(releaseIds, "release"),
            getDeadDiscogsIds(releaseIds, "release"),
        ]);
    }
    catch {
        cachedM = new Set();
        deadM = new Set();
        cachedR = new Set();
        deadR = new Set();
    }
    const isSkip = (t) => t.type === "master"
        ? (cachedM.has(t.id) || deadM.has(t.id))
        : (cachedR.has(t.id) || deadR.has(t.id));
    const skipCount = targets.filter(isSkip).length;
    if (skipCount > 0)
        await bumpCacheWarmCatnoRunSkip(series.key, skipCount);
    // GET /masters/{id} carries no `labels` field, so stamp the label
    // onto already-cached masters so the label directory join works even
    // when the master was cached by another worker. Cheap — no API call.
    const skippedMasterIds = targets.filter(t => t.type === "master" && isSkip(t)).map(t => t.id);
    if (skippedMasterIds.length > 0) {
        try {
            await backfillCachedMasterLabel(skippedMasterIds, series.label);
        }
        catch (err) {
            console.error(`[cache-warm-catno] master label backfill failed for ${series.key}:`, err?.message ?? err);
        }
    }
    let fresh = 0;
    for (const t of targets) {
        if (_stopRequested)
            break;
        if (isSkip(t))
            continue;
        try {
            await _sleep(REQ_INTERVAL_MS);
            const full = t.type === "master"
                ? await _withRetry(`master ${t.id}`, () => client.getMasterRelease(t.id))
                : await _withRetry(`release ${t.id}`, () => client.getRelease(t.id));
            // /masters/{id} has no `labels` field — stamp one before caching.
            if (t.type === "master" && !Array.isArray(full?.labels))
                full.labels = [{ name: series.label }];
            // warmOnly: swept rows stay seen_at=NULL so the "never-viewed"
            // prune reflects sweep output (a human open flips seen_at later).
            await cacheRelease(t.id, t.type, full, { warmOnly: true });
            const title = String(full?.title ?? "(untitled)");
            await recordCacheWarmCatnoRunHit(series.key, title, t.id);
            fresh++;
        }
        catch (err) {
            await recordCacheWarmCatnoRunError(series.key, `${t.type} ${t.id}: ${err?.message ?? String(err)}`);
            // Tombstone genuine 404s so future sweeps stop re-fetching them.
            if (/Discogs API error 404/.test(String(err?.message ?? err))) {
                await recordDeadDiscogsId(t.id, t.type).catch(() => { });
            }
            await _sleep(REQ_INTERVAL_MS);
        }
    }
    return fresh;
}
// ── Phase 1: walk catnos from `lo` upward ────────────────────────
// The configured `hi` is treated as a soft target, not a hard cap.
// The walker keeps incrementing past `hi` and only stops when:
//   • _stopRequested (admin clicked Stop), OR
//   • CATNO_EMPTY_STREAK_BREAK consecutive catnos return zero
//     label-matched results (catalog has run out).
// The empty-streak detector uses the lastMatchedCount that
// _processSearchResults stashes for us — every catno whose search
// returns no row that actually carries the configured label bumps
// the streak counter; any label-matched hit resets it to zero.
const CATNO_EMPTY_STREAK_BREAK = 200;
async function _runCatnoPhase(client, series, startCatno) {
    let n = startCatno;
    let emptyStreak = 0;
    while (true) {
        if (_stopRequested)
            break;
        if (emptyStreak >= CATNO_EMPTY_STREAK_BREAK) {
            console.log(`[cache-warm-catno] catno walk for ${series.key}: ${CATNO_EMPTY_STREAK_BREAK} consecutive empty catnos at n=${n}, auto-stop`);
            break;
        }
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
        // masters+: each catno hit is a specific pressing; collapse it to
        // its master (or keep it if it's an orphan) so the walk never caches
        // a redundant pressing.
        await _processSearchResults(client, series, results, { mastersPlus: true });
        const matched = Number(_processSearchResults.lastMatchedCount) || 0;
        if (matched > 0)
            emptyStreak = 0;
        else
            emptyStreak += 1;
        if (_stopRequested)
            break;
        n += 1;
        await upsertCacheWarmCatnoRun(series.key, {
            label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
        }, { current_catno: n, last_run_at: new Date() });
    }
}
// ── Phase 2: masters + orphan releases for the whole label ───────
// After the configured catno range is fully walked, sweep the rest
// of the label in two passes:
//   1. "masters"  — /database/search?type=master&label=X. Each master
//      already aggregates every pressing under it, so this is the
//      unit we actually want for search/feed/wide-card views.
//   2. "orphans"  — /database/search?type=release&label=X, kept only
//      when the release carries no master_id. These are the pressings
//      Discogs never grouped under a master, so the masters pass
//      would otherwise miss them entirely. Releases that DO have a
//      master are skipped here — the masters pass already covers
//      them, and re-fetching every pressing is exactly what this
//      change is dropping.
// There's no /labels/{id}/releases equivalent for masters (that
// endpoint is release-only and doesn't expose master_id either), so
// both passes go through the fuzzy ?label=Name search and rely on
// _labelMatches to filter noise — same tradeoff the non-ID label
// search already made. Discogs caps label search at 10000 results
// (~100 pages × 100 per page); each pass stops when its pagination
// exhausts or an empty page comes back, then the sweep is done.
const LABEL_SWEEP_PER_PAGE = 100;
async function _runLabelSweepPhase(client, series, startPage, startSubphase = "masters") {
    let subphase = startSubphase;
    let page = Math.max(1, startPage);
    const persist = (patch) => upsertCacheWarmCatnoRun(series.key, {
        label: series.label, catLo: series.lo, catHi: series.hi, yearMax: series.yearMax,
    }, patch);
    const phaseName = () => subphase === "masters" ? "label_sweep_masters" : "label_sweep_orphans";
    while (true) {
        if (_stopRequested)
            break;
        const searchType = subphase === "masters" ? "master" : "release";
        let searchRes;
        try {
            await _sleep(REQ_INTERVAL_MS);
            searchRes = await _withRetry(`label-sweep ${subphase} ${series.label} p${page}`, () => client.search("", {
                type: searchType,
                label: series.label,
                perPage: LABEL_SWEEP_PER_PAGE,
                page,
            }));
        }
        catch (err) {
            await recordCacheWarmCatnoRunError(series.key, `label-sweep ${subphase} p${page}: ${err?.message ?? String(err)}`);
            // Skip the bad page so we don't loop forever on a server-side glitch.
            page += 1;
            await persist({ phase: phaseName(), label_sweep_page: page, last_run_at: new Date() });
            continue;
        }
        const results = Array.isArray(searchRes?.results) ? searchRes.results : [];
        if (!results.length) {
            if (subphase === "masters") {
                // Masters exhausted — hand off to the orphan-release pass.
                subphase = "orphans";
                page = 1;
                await persist({ phase: phaseName(), label_sweep_page: page, last_run_at: new Date() });
                continue;
            }
            break; // orphan pagination exhausted — sweep done
        }
        await _processSearchResults(client, series, results, { mastersPlus: true, orphansOnly: subphase === "orphans" });
        if (_stopRequested)
            break;
        const totalPages = Number(searchRes?.pagination?.pages);
        const next = page + 1;
        page = next;
        await persist({ phase: phaseName(), label_sweep_page: page, last_run_at: new Date() });
        if (Number.isFinite(totalPages) && next > totalPages) {
            if (subphase === "masters") {
                subphase = "orphans";
                page = 1;
                await persist({ phase: phaseName(), label_sweep_page: page, last_run_at: new Date() });
                continue;
            }
            break;
        }
    }
}
