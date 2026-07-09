// ── Master → versions walk worker ────────────────────────────────
//
// For every master already in our cache (masters_plus type='master'),
// hits /masters/{id}/versions and caches every listed pressing.
// Catches regional / promo / reissue pressings the by-label search
// misses under Discogs's 10k pagination cap. Yield per API call
// scales with how many pressings a master has (typically 3–20 for
// pre-1970 records).
//
// Filters to yearMax at the version level so a master's post-yearMax
// reissues don't get cached in a pre-1970-only run.
import { DiscogsClient } from "./discogs-client.js";
import { cacheRelease, getOAuthCredentials, getAppSetting, setAppSetting, getCachedReleaseIds, getPool, } from "./db.js";
import { retryTransient } from "./worker-retry.js";
const STATE_KEY = "master_versions_walk_state";
const REQ_INTERVAL_MS = 1000;
let _state = null;
let _running = false;
let _stopRequested = false;
let _adminClerkId = null;
const _sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function _persist() {
    try {
        await setAppSetting(STATE_KEY, _state ? JSON.stringify(_state) : null);
    }
    catch (err) {
        console.error("[master-versions-walk] persist failed:", err);
    }
}
async function _load() {
    try {
        const raw = await getAppSetting(STATE_KEY);
        if (!raw)
            return null;
        const p = JSON.parse(raw);
        return p && Array.isArray(p.masterIds) ? p : null;
    }
    catch {
        return null;
    }
}
async function _adminClient() {
    if (!_adminClerkId)
        return null;
    const oauth = await getOAuthCredentials(_adminClerkId);
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
export function isMasterVersionsWalkRunning() { return _running; }
export function getMasterVersionsWalkStatus() {
    if (!_state)
        return {
            running: false, total: 0, cursor: 0, hits: 0, skipped: 0, errors: 0,
            yearMax: null, startedAt: null,
            lastError: null,
        };
    return {
        running: _running,
        total: _state.masterIds.length,
        cursor: _state.cursor,
        hits: _state.hits,
        skipped: _state.skipped,
        errors: _state.errors,
        yearMax: _state.yearMax,
        startedAt: _state.startedAt,
        lastError: _state.lastError ?? null,
    };
}
export function requestMasterVersionsWalkStop() {
    if (_running)
        _stopRequested = true;
}
export function forceClearMasterVersionsWalk() {
    console.log(`[master-versions-walk] FORCE clear (was running=${_running}, cursor=${_state?.cursor ?? "?"})`);
    _running = false;
    _stopRequested = false;
    _state = null;
    _persist().catch(() => { });
}
// Pull master ids from the split schema when available (post-Phase 2
// this is the source of truth) and fall back to release_cache so the
// worker still runs pre-projection. Bounded to yearMax so we don't
// waste API calls on a post-1970 master whose versions all live
// outside the window.
async function _loadMasterIds(yearMax) {
    const pool = getPool();
    try {
        const r = await pool.query(`SELECT discogs_id FROM discogs_cache_masters_plus
        WHERE type = 'master'
          AND (year IS NULL OR year <= $1)
        ORDER BY discogs_id ASC`, [yearMax]);
        if (r.rows.length > 0)
            return r.rows.map(row => Number(row.discogs_id));
    }
    catch (err) {
        console.warn("[master-versions-walk] split load failed, falling back to release_cache:", err);
    }
    const r2 = await pool.query(`SELECT discogs_id FROM release_cache
      WHERE type = 'master'
        AND (COALESCE(NULLIF(data->>'year','')::int, 0) = 0
             OR NULLIF(data->>'year','')::int <= $1)
      ORDER BY discogs_id ASC`, [yearMax]);
    return r2.rows.map(row => Number(row.discogs_id));
}
export async function startMasterVersionsWalk(opts = {}) {
    if (_running)
        return { ok: false, error: "Master-versions walk already running" };
    const client = await _adminClient();
    if (!client)
        return { ok: false, error: "Admin Discogs OAuth not connected" };
    const yearMax = Number.isFinite(opts.yearMax) ? Math.trunc(opts.yearMax) : 1970;
    const ids = await _loadMasterIds(yearMax);
    if (ids.length === 0)
        return { ok: false, error: "No cached masters within the yearMax window" };
    const persisted = opts.resetCursor ? null : await _load();
    _state = persisted && persisted.masterIds.length === ids.length
        ? persisted
        : {
            masterIds: ids,
            cursor: 0,
            yearMax,
            startedAt: new Date().toISOString(),
            hits: 0,
            skipped: 0,
            errors: 0,
            lastError: null,
        };
    _state.yearMax = yearMax;
    _running = true;
    _stopRequested = false;
    await _persist();
    console.log(`[master-versions-walk] START ${_state.masterIds.length} masters, yearMax=${yearMax}, cursor=${_state.cursor}`);
    _run(client).catch(err => console.error("[master-versions-walk] runner crashed:", err));
    return { ok: true, queued: _state.masterIds.length };
}
async function _run(client) {
    try {
        while (_state && _state.cursor < _state.masterIds.length && !_stopRequested) {
            const masterId = _state.masterIds[_state.cursor];
            try {
                await _walkOne(client, masterId, _state.yearMax);
            }
            catch (err) {
                _state.errors++;
                _state.lastError = `master=${masterId}: ${err?.message ?? String(err)}`;
                console.warn(`[master-versions-walk] ${_state.lastError}`);
            }
            _state.cursor++;
            await _persist();
            if (_stopRequested)
                break;
        }
    }
    finally {
        const drained = _state && _state.cursor >= _state.masterIds.length;
        _running = false;
        _stopRequested = false;
        if (drained)
            console.log(`[master-versions-walk] queue drained; hits=${_state?.hits} skipped=${_state?.skipped} errors=${_state?.errors}`);
        await _persist();
    }
}
async function _walkOne(client, masterId, yearMax) {
    let page = 1;
    const perPage = 100;
    const maxPages = 100;
    while (page <= maxPages) {
        if (_stopRequested)
            return;
        let payload;
        try {
            payload = await retryTransient(() => client.getMasterVersions(masterId, { page, perPage }), { label: "master-versions getMasterVersions" });
        }
        catch (err) {
            const msg = String(err?.message ?? err ?? "");
            if (/404/.test(msg))
                return;
            throw err;
        }
        const versions = Array.isArray(payload?.versions) ? payload.versions : [];
        if (versions.length === 0)
            return;
        // Batch-check all in-scope release ids against the cache in one
        // DB round trip (releases live in pressings OR masters_plus, so
        // getCachedReleaseIds UNIONs both).
        const inScope = [];
        for (const v of versions) {
            const releaseId = Number(v?.id);
            if (!Number.isFinite(releaseId) || releaseId <= 0)
                continue;
            const yr = Number(v?.released ? String(v.released).slice(0, 4) : v?.year);
            if (Number.isFinite(yr) && yr > yearMax)
                continue;
            inScope.push(releaseId);
        }
        if (inScope.length === 0) {
            if (versions.length < perPage)
                return;
            page++;
            continue;
        }
        const cachedSet = await getCachedReleaseIds(inScope, "release");
        for (const releaseId of inScope) {
            if (_stopRequested)
                return;
            if (cachedSet.has(releaseId)) {
                _state.skipped++;
                continue;
            }
            try {
                const full = await retryTransient(() => client.getRelease(releaseId), { label: `master-versions release=${releaseId}` });
                await cacheRelease(releaseId, "release", full, { warmOnly: true });
                _state.hits++;
                await _persist();
            }
            catch (err) {
                _state.errors++;
                _state.lastError = `master=${masterId} release=${releaseId}: ${err?.message ?? String(err)}`;
                console.warn(`[master-versions-walk] ${_state.lastError}`);
            }
            await _sleep(REQ_INTERVAL_MS);
        }
        if (versions.length < perPage)
            return;
        page++;
    }
}
export function initMasterVersionsWalkModule(adminClerkId) {
    _adminClerkId = adminClerkId || null;
    setTimeout(async () => {
        if (_running)
            return;
        const persisted = await _load();
        if (!persisted)
            return;
        if (persisted.cursor >= persisted.masterIds.length) {
            _state = null;
            await _persist();
            return;
        }
        const client = await _adminClient();
        if (!client) {
            console.warn(`[master-versions-walk] boot-resume: no admin client, skipping`);
            return;
        }
        _state = persisted;
        _running = true;
        _stopRequested = false;
        console.log(`[master-versions-walk] boot-resume at ${persisted.cursor}/${persisted.masterIds.length} yearMax=${persisted.yearMax}`);
        _run(client).catch(err => console.error("[master-versions-walk] resume crashed:", err));
    }, 14000);
}
