// ── Master label backfill worker ─────────────────────────────────
//
// Discogs /masters/{id} carries no label, so masters cached by the genre
// and faceted sweeps had none — which leaves them under "Unknown label"
// in the Year-Label tab. This job fills them in:
//   1. Free pass: copy a label from whatever the cache already holds
//      (main release, any pressing, saved version list).
//   2. For the rest, fetch the master's main release (one Discogs call
//      each, lowest-priority lane) and stamp its first label + catno onto
//      the master row. The release itself is not cached.
// A main release with no label, or a 404, marks the master _labelTried so
// it isn't fetched again. Progress persists in app_settings and resumes on
// boot, like the other bulk workers.
import { getAdminDiscogsClient } from "./discogs-client.js";
import { getAppSetting, setAppSetting, countMastersNeedingLabel, listMastersNeedingLabel, markMasterLabelTried, stampMasterLabelsFromCache, stampCachedMasterLabels, } from "./db.js";
import { retryTransient } from "./worker-retry.js";
import { isApiKilled } from "./api-guard.js";
const STATE_KEY = "master_label_backfill_state";
const BATCH = 50;
let _state = null;
let _running = false;
let _stopRequested = false;
let _adminClerkId = null;
async function _persist() {
    try {
        await setAppSetting(STATE_KEY, _state ? JSON.stringify(_state) : null);
    }
    catch (err) {
        console.error("[master-labels] persist failed:", err);
    }
}
async function _load() {
    try {
        const raw = await getAppSetting(STATE_KEY);
        if (!raw)
            return null;
        const p = JSON.parse(raw);
        return p && typeof p.cursor === "number" ? p : null;
    }
    catch {
        return null;
    }
}
async function _adminClient() {
    const c = await getAdminDiscogsClient(_adminClerkId);
    return c ? c.withPriority("sweep") : null;
}
export function getMasterLabelBackfillStatus() {
    return {
        running: _running,
        startedAt: _state?.startedAt ?? null,
        total: _state?.total ?? 0,
        processed: _state?.processed ?? 0,
        fromCache: _state?.fromCache ?? 0,
        fetched: _state?.fetched ?? 0,
        noLabel: _state?.noLabel ?? 0,
        errors: _state?.errors ?? 0,
        lastError: _state?.lastError ?? null,
        done: !!_state?.done,
    };
}
export async function getMasterLabelBackfillOverview() {
    return { ...getMasterLabelBackfillStatus(), remaining: await countMastersNeedingLabel() };
}
export function requestMasterLabelBackfillStop() {
    if (_running)
        _stopRequested = true;
    if (_state) {
        _state.stopped = true;
        _persist().catch(() => { });
    }
}
export async function startMasterLabelBackfill() {
    if (_running)
        return { ok: false, error: "Already running" };
    const client = await _adminClient();
    if (!client)
        return { ok: false, error: "Admin Discogs OAuth not connected" };
    if (isApiKilled())
        return { ok: false, error: "API kill switch is on" };
    const persisted = await _load();
    _state = persisted && !persisted.done
        ? persisted
        : { startedAt: new Date().toISOString(), cursor: 0, total: 0, fromCache: 0, fetched: 0, noLabel: 0, errors: 0, processed: 0, lastError: null };
    _state.stopped = false;
    _running = true;
    _stopRequested = false;
    await _persist();
    _run(client, !persisted || !!persisted.done).catch(err => console.error("[master-labels] runner crashed:", err));
    return { ok: true };
}
async function _run(client, freshStart) {
    try {
        if (freshStart) {
            _state.fromCache = await stampMasterLabelsFromCache();
            console.log(`[master-labels] free pass stamped ${_state.fromCache} masters from cached data`);
        }
        _state.total = _state.processed + await countMastersNeedingLabel();
        await _persist();
        console.log(`[master-labels] START cursor=${_state.cursor} remaining=${_state.total - _state.processed}`);
        while (!_stopRequested) {
            const batch = await listMastersNeedingLabel(_state.cursor, BATCH);
            if (!batch.length) {
                _state.done = true;
                break;
            }
            for (const m of batch) {
                if (_stopRequested)
                    break;
                if (isApiKilled()) {
                    _state.lastError = "Paused: API kill switch is on";
                    _stopRequested = true;
                    break;
                }
                try {
                    if (m.mainRelease == null) {
                        await markMasterLabelTried(m.id);
                        _state.noLabel++;
                    }
                    else {
                        const rel = await retryTransient(() => client.getRelease(m.mainRelease), { label: `master-labels release=${m.mainRelease}` });
                        const lbl = Array.isArray(rel?.labels) ? rel.labels.find((l) => String(l?.name ?? "").trim()) : null;
                        if (lbl) {
                            await stampCachedMasterLabels([{ id: m.id, name: String(lbl.name).trim(), catno: String(lbl.catno ?? "").trim() }]);
                            _state.fetched++;
                        }
                        else {
                            await markMasterLabelTried(m.id);
                            _state.noLabel++;
                        }
                    }
                }
                catch (err) {
                    const msg = String(err?.message ?? err);
                    if (/Discogs API error 404/.test(msg)) {
                        await markMasterLabelTried(m.id).catch(() => { });
                        _state.noLabel++;
                    }
                    else {
                        // Left unmarked so a later run retries it.
                        _state.errors++;
                        _state.lastError = `master ${m.id}: ${msg}`;
                        console.warn(`[master-labels] ${_state.lastError}`);
                    }
                }
                _state.cursor = m.id;
                _state.processed++;
                if (_state.processed % 10 === 0)
                    await _persist();
                // Pacing: discogsGate() inside DiscogsClient.
            }
        }
    }
    finally {
        _running = false;
        _stopRequested = false;
        if (_state?.done)
            console.log(`[master-labels] done; cache=${_state.fromCache} fetched=${_state.fetched} noLabel=${_state.noLabel} errors=${_state.errors}`);
        await _persist();
    }
}
export function initMasterLabelBackfillModule(adminClerkId) {
    _adminClerkId = adminClerkId || null;
    setTimeout(async () => {
        if (_running)
            return;
        const persisted = await _load();
        if (!persisted || persisted.done || persisted.stopped) {
            _state = persisted;
            return;
        }
        const client = await _adminClient();
        if (!client || isApiKilled()) {
            _state = persisted;
            return;
        }
        _state = persisted;
        _running = true;
        _stopRequested = false;
        console.log(`[master-labels] boot-resume at master ${persisted.cursor}`);
        _run(client, false).catch(err => console.error("[master-labels] resume crashed:", err));
    }, 20000);
}
