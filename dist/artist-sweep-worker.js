// ── Artist masters+ sweep worker ─────────────────────────────────
//
// Queue runner that iterates every artist we know a Discogs ID for
// (currently sourced from blues_artists) and caches every master +
// orphan release credited to them up to the configured year cap.
// Uses /artists/{id}/releases which returns a mixed list of masters
// and releases with the artist credit — one endpoint covers both
// "everything by them" (masters) and orphan pressings (releases
// with no master_id).
//
// Yield per API call is high for the pre-1970 corpus (a curated
// blues/jazz/country artist set covers side-project releases, guest
// spots, and one-off singles that year × genre / label sweeps miss).
import { getAdminDiscogsClient } from "./discogs-client.js";
import { cacheRelease, getBluesArtistDiscogsIds, getAppSetting, setAppSetting, getCachedReleaseIds, } from "./db.js";
import { retryTransient } from "./worker-retry.js";
const STATE_KEY = "artist_sweep_state";
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
        console.error("[artist-sweep] persist failed:", err);
    }
}
async function _load() {
    try {
        const raw = await getAppSetting(STATE_KEY);
        if (!raw)
            return null;
        const p = JSON.parse(raw);
        return p && Array.isArray(p.artistIds) ? p : null;
    }
    catch {
        return null;
    }
}
async function _adminClient() {
    return getAdminDiscogsClient(_adminClerkId);
}
export function isArtistSweepRunning() { return _running; }
export function getArtistSweepStatus() {
    if (!_state)
        return {
            running: false, total: 0, cursor: 0, hits: 0, skipped: 0, errors: 0,
            yearMax: null, startedAt: null,
            lastError: null,
        };
    return {
        running: _running,
        total: _state.artistIds.length,
        cursor: _state.cursor,
        hits: _state.hits,
        skipped: _state.skipped,
        errors: _state.errors,
        yearMax: _state.yearMax,
        startedAt: _state.startedAt,
        lastError: _state.lastError ?? null,
    };
}
export function requestArtistSweepStop() {
    if (_running)
        _stopRequested = true;
}
export function forceClearArtistSweep() {
    console.log(`[artist-sweep] FORCE clear (was running=${_running}, cursor=${_state?.cursor ?? "?"})`);
    _running = false;
    _stopRequested = false;
    _state = null;
    _persist().catch(() => { });
}
export async function startArtistSweep(opts = {}) {
    if (_running)
        return { ok: false, error: "Artist sweep already running" };
    const client = await _adminClient();
    if (!client)
        return { ok: false, error: "Admin Discogs OAuth not connected" };
    const yearMax = Number.isFinite(opts.yearMax) ? Math.trunc(opts.yearMax) : 1970;
    const ids = await getBluesArtistDiscogsIds();
    if (ids.length === 0)
        return { ok: false, error: "No blues_artists rows have a Discogs ID" };
    const persisted = opts.resetCursor ? null : await _load();
    _state = persisted && persisted.artistIds.length === ids.length
        ? persisted
        : {
            artistIds: ids,
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
    console.log(`[artist-sweep] START ${_state.artistIds.length} artists, yearMax=${yearMax}, cursor=${_state.cursor}`);
    _run(client).catch(err => console.error("[artist-sweep] runner crashed:", err));
    return { ok: true, queued: _state.artistIds.length };
}
async function _run(client) {
    try {
        while (_state && _state.cursor < _state.artistIds.length && !_stopRequested) {
            const artistId = _state.artistIds[_state.cursor];
            try {
                await _sweepArtist(client, artistId, _state.yearMax);
            }
            catch (err) {
                _state.errors++;
                _state.lastError = `artist=${artistId}: ${err?.message ?? String(err)}`;
                console.warn(`[artist-sweep] ${_state.lastError}`);
            }
            _state.cursor++;
            await _persist();
            if (_stopRequested)
                break;
        }
    }
    finally {
        const drained = _state && _state.cursor >= _state.artistIds.length;
        _running = false;
        _stopRequested = false;
        if (drained)
            console.log(`[artist-sweep] queue drained; hits=${_state?.hits} skipped=${_state?.skipped} errors=${_state?.errors}`);
        await _persist();
    }
}
async function _sweepArtist(client, artistId, yearMax) {
    let page = 1;
    const perPage = 100;
    // Guard against Discogs's 10k pagination cap.
    const maxPages = 100;
    while (page <= maxPages) {
        if (_stopRequested)
            return;
        let payload;
        try {
            payload = await retryTransient(() => client.getArtistReleases(artistId, { page, perPage, sort: "year", sortOrder: "asc" }), { label: "artist-sweep getArtistReleases" });
        }
        catch (err) {
            const msg = String(err?.message ?? err ?? "");
            if (/404/.test(msg))
                return; // artist id no longer valid
            throw err;
        }
        const releases = Array.isArray(payload?.releases) ? payload.releases : [];
        if (releases.length === 0)
            return;
        // ── Batch skip check ──
        // Filter to valid in-scope candidates once, split by kind, and
        // call getCachedReleaseIds twice per page (instead of N times).
        // Cuts DB round trips from ~200 per page down to 2.
        const candidates = [];
        for (const r of releases) {
            const id = Number(r?.id);
            const kind = String(r?.type ?? "").toLowerCase();
            if (!Number.isFinite(id) || id <= 0)
                continue;
            if (kind !== "master" && kind !== "release")
                continue;
            // yearMax <= 0 means "all years" — cache every release + master
            // credited to the artist regardless of year.
            if (yearMax > 0) {
                const yr = Number(r?.year);
                if (Number.isFinite(yr) && yr > yearMax)
                    continue;
            }
            candidates.push({ id, kind: kind });
        }
        if (candidates.length === 0) {
            if (releases.length < perPage)
                return;
            page++;
            continue;
        }
        const masterIds = candidates.filter(c => c.kind === "master").map(c => c.id);
        const releaseIds = candidates.filter(c => c.kind === "release").map(c => c.id);
        const [cachedMasters, cachedReleases] = await Promise.all([
            masterIds.length ? getCachedReleaseIds(masterIds, "master") : Promise.resolve(new Set()),
            releaseIds.length ? getCachedReleaseIds(releaseIds, "release") : Promise.resolve(new Set()),
        ]);
        for (const { id, kind } of candidates) {
            if (_stopRequested)
                return;
            const alreadyCached = kind === "master" ? cachedMasters.has(id) : cachedReleases.has(id);
            if (alreadyCached) {
                _state.skipped++;
                continue;
            }
            try {
                const detailed = await retryTransient(() => kind === "master" ? client.getMasterRelease(id) : client.getRelease(id), { label: `artist-sweep ${kind}=${id}` });
                await cacheRelease(id, kind, detailed, { warmOnly: true });
                _state.hits++;
                await _persist();
            }
            catch (err) {
                _state.errors++;
                _state.lastError = `artist=${artistId} ${kind}=${id}: ${err?.message ?? String(err)}`;
                console.warn(`[artist-sweep] ${_state.lastError}`);
            }
            await _sleep(REQ_INTERVAL_MS);
        }
        if (releases.length < perPage)
            return;
        page++;
    }
}
export function initArtistSweepModule(adminClerkId) {
    _adminClerkId = adminClerkId || null;
    setTimeout(async () => {
        if (_running)
            return;
        const persisted = await _load();
        if (!persisted)
            return;
        if (persisted.cursor >= persisted.artistIds.length) {
            _state = null;
            await _persist();
            return;
        }
        const client = await _adminClient();
        if (!client) {
            console.warn(`[artist-sweep] boot-resume: no admin client, skipping`);
            return;
        }
        _state = persisted;
        _running = true;
        _stopRequested = false;
        console.log(`[artist-sweep] boot-resume at ${persisted.cursor}/${persisted.artistIds.length} yearMax=${persisted.yearMax}`);
        _run(client).catch(err => console.error("[artist-sweep] resume crashed:", err));
    }, 12000);
}
