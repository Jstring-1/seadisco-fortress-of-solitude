// ── Sublabel discovery worker ────────────────────────────────────
//
// For each cached label with a Discogs ID and pad rows ≥ threshold,
// fetch /labels/{id} and enqueue every sublabel[] entry as a
// placeholder external_discography row so it surfaces in the label
// directory as unswept. Follow up with the bulk masters+ sweep and
// each sublabel gets its own /labels/{id}/releases pass.
//
// Runs as a background worker — the previous inline endpoint
// hammered the request timeout because a full run against a few
// thousand labels at 1 req/sec is minutes, not seconds. Singleflight
// + persisted cursor + boot-resume like the other bulk workers.
import { DiscogsClient } from "./discogs-client.js";
import { listLabelDirectory, getOAuthCredentials, getAppSetting, setAppSetting, getPool, } from "./db.js";
const STATE_KEY = "sublabel_discovery_state";
const REQ_INTERVAL_MS = 1100;
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
        console.error("[sublabel-discovery] persist failed:", err);
    }
}
async function _load() {
    try {
        const raw = await getAppSetting(STATE_KEY);
        if (!raw)
            return null;
        const p = JSON.parse(raw);
        return p && Array.isArray(p.queue) ? p : null;
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
export function isSublabelDiscoveryRunning() { return _running; }
export function getSublabelDiscoveryStatus() {
    if (!_state)
        return {
            running: false, total: 0, cursor: 0,
            discovered: 0, inserted: 0, errors: 0,
            minCount: null,
            startedAt: null,
            lastError: null,
            currentLabel: null,
        };
    return {
        running: _running,
        total: _state.queue.length,
        cursor: _state.cursor,
        discovered: _state.discovered,
        inserted: _state.inserted,
        errors: _state.errors,
        minCount: _state.minCount,
        startedAt: _state.startedAt,
        lastError: _state.lastError ?? null,
        currentLabel: _state.queue[_state.cursor] ?? null,
    };
}
export function requestSublabelDiscoveryStop() {
    if (_running)
        _stopRequested = true;
}
export function forceClearSublabelDiscovery() {
    console.log(`[sublabel-discovery] FORCE clear (was running=${_running})`);
    _running = false;
    _stopRequested = false;
    _state = null;
    _persist().catch(() => { });
}
export async function startSublabelDiscovery(opts = {}) {
    if (_running)
        return { ok: false, error: "Sublabel discovery already running" };
    const client = await _adminClient();
    if (!client)
        return { ok: false, error: "Admin Discogs OAuth not connected" };
    const minCount = Math.max(1, Number(opts.minExternalCount ?? 1));
    const rows = await listLabelDirectory({ limit: 5000 });
    const queue = rows
        .filter(r => Number.isFinite(r.label_id) && (r.label_id ?? 0) > 0 && (r.external_count ?? 0) >= minCount)
        .map(r => ({ labelId: r.label_id, labelName: r.label_name }));
    if (queue.length === 0) {
        return { ok: false, error: `no labels with Discogs ID and ≥${minCount} pad rows` };
    }
    const persisted = opts.resetCursor ? null : await _load();
    const reusable = persisted
        && persisted.queue.length === queue.length
        && persisted.queue.every((q, i) => q.labelId === queue[i].labelId);
    _state = reusable
        ? persisted
        : {
            queue,
            cursor: 0,
            minCount,
            startedAt: new Date().toISOString(),
            discovered: 0,
            inserted: 0,
            errors: 0,
            lastError: null,
        };
    _state.minCount = minCount;
    _running = true;
    _stopRequested = false;
    await _persist();
    console.log(`[sublabel-discovery] START ${_state.queue.length} labels, minCount=${minCount}, cursor=${_state.cursor}`);
    _run(client).catch(err => console.error("[sublabel-discovery] runner crashed:", err));
    return { ok: true, queued: _state.queue.length };
}
async function _run(client) {
    try {
        while (_state && _state.cursor < _state.queue.length && !_stopRequested) {
            const item = _state.queue[_state.cursor];
            try {
                await _processLabel(client, item);
            }
            catch (err) {
                _state.errors++;
                _state.lastError = `${item.labelName} (${item.labelId}): ${err?.message ?? String(err)}`;
                console.warn(`[sublabel-discovery] ${_state.lastError}`);
            }
            _state.cursor++;
            await _persist();
            if (_stopRequested)
                break;
            await _sleep(REQ_INTERVAL_MS);
        }
    }
    finally {
        const drained = _state && _state.cursor >= _state.queue.length;
        _running = false;
        _stopRequested = false;
        if (drained)
            console.log(`[sublabel-discovery] drained; discovered=${_state?.discovered} inserted=${_state?.inserted} errors=${_state?.errors}`);
        await _persist();
    }
}
async function _processLabel(client, item) {
    const payload = await client.getLabel(item.labelId);
    const subs = Array.isArray(payload?.sublabels) ? payload.sublabels : [];
    if (subs.length === 0)
        return;
    for (const s of subs) {
        const subName = String(s?.name ?? "").trim();
        const subId = Number(s?.id);
        if (!subName)
            continue;
        _state.discovered++;
        try {
            // external_discography.catno is NOT NULL; empty string satisfies
            // the constraint and lets listLabelDirectory pick the sublabel
            // up via its ext CTE. Unique key is (label_name, catno, side,
            // source) so re-runs are idempotent.
            const r = await getPool().query(`INSERT INTO external_discography
           (source, label_name, label_id, catno)
         VALUES ('discogs:sublabels', $1, $2, '')
         ON CONFLICT DO NOTHING`, [subName, Number.isFinite(subId) ? subId : null]);
            _state.inserted += r.rowCount ?? 0;
        }
        catch (err) {
            _state.errors++;
            _state.lastError = `insert ${subName}: ${err?.message ?? String(err)}`;
            console.warn(`[sublabel-discovery] ${_state.lastError}`);
        }
    }
}
export function initSublabelDiscoveryModule(adminClerkId) {
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
            console.warn(`[sublabel-discovery] boot-resume: no admin client, skipping`);
            return;
        }
        _state = persisted;
        _running = true;
        _stopRequested = false;
        console.log(`[sublabel-discovery] boot-resume at ${persisted.cursor}/${persisted.queue.length}`);
        _run(client).catch(err => console.error("[sublabel-discovery] resume crashed:", err));
    }, 18000);
}
