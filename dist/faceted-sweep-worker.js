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
import { DiscogsClient } from "./discogs-client.js";
import { cacheRelease, getOAuthCredentials, getAppSetting, setAppSetting, getCachedReleaseIds, } from "./db.js";
import { retryTransient } from "./worker-retry.js";
const STATE_KEY = "faceted_sweep_state";
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
        console.error("[faceted-sweep] persist failed:", err);
    }
}
async function _load() {
    try {
        const raw = await getAppSetting(STATE_KEY);
        if (!raw)
            return null;
        const p = JSON.parse(raw);
        return p && Array.isArray(p.queue) && (p.mode === "format" || p.mode === "country") ? p : null;
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
export function isFacetedSweepRunning() { return _running; }
export function getFacetedSweepStatus() {
    if (!_state)
        return {
            running: false, mode: null,
            total: 0, cursor: 0, hits: 0, skipped: 0, errors: 0,
            startedAt: null,
            currentSlot: null,
            lastError: null,
        };
    return {
        running: _running,
        mode: _state.mode,
        total: _state.queue.length,
        cursor: _state.cursor,
        hits: _state.hits,
        skipped: _state.skipped,
        errors: _state.errors,
        startedAt: _state.startedAt,
        currentSlot: _state.queue[_state.cursor] ?? null,
        lastError: _state.lastError ?? null,
    };
}
export function requestFacetedSweepStop() {
    if (_running)
        _stopRequested = true;
}
export function forceClearFacetedSweep() {
    console.log(`[faceted-sweep] FORCE clear (was running=${_running})`);
    _running = false;
    _stopRequested = false;
    _state = null;
    _persist().catch(() => { });
}
// Default value sets — sane pre-1970 picks. Caller can override.
const DEFAULT_FORMATS = ["Shellac", "7\"", "10\"", "12\"", "LP"];
const DEFAULT_COUNTRIES = ["US", "UK", "France", "Germany", "Italy", "Japan", "Jamaica", "Brazil", "Nigeria"];
export async function startFacetedSweep(opts) {
    if (_running)
        return { ok: false, error: "Faceted sweep already running" };
    const client = await _adminClient();
    if (!client)
        return { ok: false, error: "Admin Discogs OAuth not connected" };
    if (opts.mode !== "format" && opts.mode !== "country")
        return { ok: false, error: "mode must be 'format' or 'country'" };
    const yearFrom = Number.isFinite(opts.yearFrom) ? Math.trunc(opts.yearFrom) : 1900;
    const yearTo = Number.isFinite(opts.yearTo) ? Math.trunc(opts.yearTo) : 1970;
    const values = (Array.isArray(opts.values) && opts.values.length > 0)
        ? opts.values.map(String).filter(Boolean)
        : (opts.mode === "format" ? DEFAULT_FORMATS : DEFAULT_COUNTRIES);
    const queue = [];
    for (const value of values) {
        for (let year = yearFrom; year <= yearTo; year++) {
            queue.push({ year, value });
        }
    }
    if (queue.length === 0)
        return { ok: false, error: "empty queue (check yearFrom/yearTo and values)" };
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
            cursor: 0,
            startedAt: new Date().toISOString(),
            hits: 0,
            skipped: 0,
            errors: 0,
            lastError: null,
        };
    _running = true;
    _stopRequested = false;
    await _persist();
    console.log(`[faceted-sweep] START mode=${opts.mode} slots=${_state.queue.length} cursor=${_state.cursor}`);
    _run(client).catch(err => console.error("[faceted-sweep] runner crashed:", err));
    return { ok: true, queued: _state.queue.length };
}
async function _run(client) {
    try {
        while (_state && _state.cursor < _state.queue.length && !_stopRequested) {
            const slot = _state.queue[_state.cursor];
            try {
                await _sweepSlot(client, _state.mode, slot);
            }
            catch (err) {
                _state.errors++;
                _state.lastError = `${_state.mode}=${slot.value} year=${slot.year}: ${err?.message ?? String(err)}`;
                console.warn(`[faceted-sweep] ${_state.lastError}`);
            }
            _state.cursor++;
            await _persist();
            if (_stopRequested)
                break;
        }
    }
    finally {
        const drained = _state && _state.cursor >= _state.queue.length;
        _running = false;
        _stopRequested = false;
        if (drained)
            console.log(`[faceted-sweep] queue drained; hits=${_state?.hits} skipped=${_state?.skipped} errors=${_state?.errors}`);
        await _persist();
    }
}
async function _sweepSlot(client, mode, slot) {
    let page = 1;
    const perPage = 100;
    const maxPages = 100;
    while (page <= maxPages) {
        if (_stopRequested)
            return;
        const opts = { type: "master", year: String(slot.year), page, perPage };
        if (mode === "format")
            opts.format = slot.value;
        else
            opts.country = slot.value;
        let payload;
        try {
            payload = await retryTransient(() => client.search("", opts), { label: `faceted-sweep ${mode}=${slot.value}/${slot.year}` });
        }
        catch (err) {
            const msg = String(err?.message ?? err ?? "");
            if (/404/.test(msg))
                return;
            throw err;
        }
        const results = Array.isArray(payload?.results) ? payload.results : [];
        if (results.length === 0)
            return;
        // Batch cache check across the whole page (all master ids).
        const inScope = [];
        for (const r of results) {
            const id = Number(r?.id);
            const kind = String(r?.type ?? "").toLowerCase();
            if (!Number.isFinite(id) || id <= 0)
                continue;
            if (kind !== "master")
                continue;
            inScope.push(id);
        }
        if (inScope.length === 0) {
            if (results.length < perPage)
                return;
            page++;
            continue;
        }
        const cachedSet = await getCachedReleaseIds(inScope, "master");
        for (const id of inScope) {
            if (_stopRequested)
                return;
            if (cachedSet.has(id)) {
                _state.skipped++;
                continue;
            }
            try {
                const full = await retryTransient(() => client.getMasterRelease(id), { label: `faceted-sweep master=${id}` });
                await cacheRelease(id, "master", full, { warmOnly: true });
                _state.hits++;
                await _persist();
            }
            catch (err) {
                _state.errors++;
                _state.lastError = `master=${id}: ${err?.message ?? String(err)}`;
                console.warn(`[faceted-sweep] ${_state.lastError}`);
            }
            await _sleep(REQ_INTERVAL_MS);
        }
        if (results.length < perPage)
            return;
        page++;
    }
}
export function initFacetedSweepModule(adminClerkId) {
    _adminClerkId = adminClerkId || null;
    setTimeout(async () => {
        if (_running)
            return;
        const persisted = await _load();
        if (!persisted)
            return;
        if (persisted.cursor >= persisted.queue.length) {
            _state = null;
            await _persist();
            return;
        }
        const client = await _adminClient();
        if (!client) {
            console.warn(`[faceted-sweep] boot-resume: no admin client, skipping`);
            return;
        }
        _state = persisted;
        _running = true;
        _stopRequested = false;
        console.log(`[faceted-sweep] boot-resume mode=${persisted.mode} at ${persisted.cursor}/${persisted.queue.length}`);
        _run(client).catch(err => console.error("[faceted-sweep] resume crashed:", err));
    }, 16000);
}
