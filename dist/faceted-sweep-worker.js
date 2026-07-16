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
import { getAdminDiscogsClient } from "./discogs-client.js";
import { cacheRelease, getAppSetting, setAppSetting, getCachedReleaseIds, getDeadDiscogsIds, recordDeadDiscogsId, } from "./db.js";
import { retryTransient } from "./worker-retry.js";
const STATE_KEY = "faceted_sweep_state";
let _state = null;
let _running = false;
let _stopRequested = false;
let _adminClerkId = null;
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
    return getAdminDiscogsClient(_adminClerkId);
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
    // Distinct values in the queue — used by the admin UI to restore
    // checkbox selections so a resumed sweep shows what it's actually
    // sweeping, not the checkbox defaults.
    const values = Array.from(new Set(_state.queue.map(q => q.value)));
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
        values,
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
        // Build the master-id worklist. A type=master search combined with
        // a pressing-level facet (format/country) makes Discogs hand back
        // PRESSING rows still tagged type:"master" but carrying the release
        // id — fetching that as a master 404s. So we trust r.master_id (the
        // real master) over r.id, falling back to r.id only for genuine
        // master rows that carry no master_id. Dedup because many pressings
        // collapse onto one master.
        const seen = new Set();
        const inScope = [];
        for (const r of results) {
            const kind = String(r?.type ?? "").toLowerCase();
            const mid = Number(r?.master_id);
            const rid = Number(r?.id);
            const masterId = Number.isFinite(mid) && mid > 0
                ? mid
                : (kind === "master" && Number.isFinite(rid) && rid > 0 ? rid : 0);
            if (masterId <= 0 || seen.has(masterId))
                continue;
            seen.add(masterId);
            inScope.push(masterId);
        }
        if (inScope.length === 0) {
            if (results.length < perPage)
                return;
            page++;
            continue;
        }
        // Skip both already-cached and known-dead ids so a mature sweep
        // doesn't re-spend its budget re-fetching either.
        const [cachedSet, deadSet] = await Promise.all([
            getCachedReleaseIds(inScope, "master"),
            getDeadDiscogsIds(inScope, "master"),
        ]);
        for (const id of inScope) {
            if (_stopRequested)
                return;
            if (cachedSet.has(id) || deadSet.has(id)) {
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
                // Tombstone genuine 404s so we never re-fetch this id again.
                if (/Discogs API error 404/.test(String(err?.message ?? err))) {
                    await recordDeadDiscogsId(id, "master").catch(() => { });
                }
            }
            // Pacing is enforced process-wide by discogsGate() in DiscogsClient;
            // no local sleep so the gate is the single, uniform pacer.
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
