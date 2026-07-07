// ── Bulk label masters+ sweep worker ─────────────────────────────
//
// Queue runner that iterates every label in the directory (top-down
// by external_discography row count) and fires an ad-hoc masters+
// sweep for each in turn. Individual sweeps are still done by the
// existing single-flight cache-warm-catno worker; this module just
// walks the queue and waits for each sweep to exit before starting
// the next. Cursor + queue are persisted so a Railway restart
// resumes cleanly.
//
// UX: user hits "Bulk masters+ sweep" in the Labels admin panel.
// Under the hood we build the queue once (labels with a Discogs
// id AND external_count >= min), then loop:
//   1. wait until catno worker is idle
//   2. startAdHocLabelSweep(labelId, labelName)
//   3. poll isCacheWarmCatnoRunning() until it flips back to false
//   4. advance cursor, persist
//   5. repeat until queue exhausted or stop requested
import { isCacheWarmCatnoRunning, startAdHocLabelSweep, requestCacheWarmCatnoStop, } from "./cache-warm-catno.js";
import { listLabelDirectory, getAppSetting, setAppSetting } from "./db.js";
const STATE_KEY = "bulk_label_sweep_state";
let _state = null;
let _running = false;
let _stopRequested = false;
let _currentLabel = null;
async function _persist() {
    try {
        await setAppSetting(STATE_KEY, _state ? JSON.stringify(_state) : null);
    }
    catch (err) {
        console.error("[bulk-label-sweep] persist failed:", err);
    }
}
async function _load() {
    try {
        const raw = await getAppSetting(STATE_KEY);
        if (!raw)
            return null;
        const parsed = JSON.parse(raw);
        if (!parsed || !Array.isArray(parsed.queue))
            return null;
        return parsed;
    }
    catch {
        return null;
    }
}
function _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
export function isBulkLabelSweepRunning() { return _running; }
export function getBulkLabelSweepStatus() {
    if (!_state) {
        return {
            running: false, total: 0, cursor: 0, completed: 0, errors: 0,
            currentLabel: null, startedAt: null, lastError: null, remaining: [],
        };
    }
    return {
        running: _running,
        total: _state.queue.length,
        cursor: _state.cursor,
        completed: _state.completed,
        errors: _state.errors,
        currentLabel: _currentLabel,
        startedAt: _state.startedAt,
        lastError: _state.lastError ?? null,
        remaining: _state.queue.slice(_state.cursor, _state.cursor + 5),
    };
}
// Signal the runner loop to break after the current inner sweep, and
// also stop that inner sweep so we don't wait out a long label just
// to unwind.
export function requestBulkLabelSweepStop() {
    if (!_running)
        return;
    _stopRequested = true;
    try {
        requestCacheWarmCatnoStop();
    }
    catch { }
}
// Nuke in-memory + persisted state. Only for stuck-runner recovery.
export function forceClearBulkLabelSweep() {
    console.log(`[bulk-label-sweep] FORCE clear (was running=${_running}, cursor=${_state?.cursor ?? "?"})`);
    _running = false;
    _stopRequested = false;
    _currentLabel = null;
    _state = null;
    _persist().catch(() => { });
}
export async function startBulkLabelSweep(opts = {}) {
    if (_running)
        return { ok: false, error: "Bulk sweep already running" };
    const minCount = Math.max(1, Number(opts.minExternalCount ?? 1));
    const rows = await listLabelDirectory({ limit: 5000 });
    const queue = rows
        .filter(r => Number.isFinite(r.label_id) && (r.label_id ?? 0) > 0 && r.external_count >= minCount)
        .sort((a, b) => (b.external_count - a.external_count))
        .map(r => ({
        labelId: r.label_id,
        labelName: r.label_name,
        externalCount: r.external_count,
    }));
    if (queue.length === 0) {
        return { ok: false, error: `No labels with a Discogs ID and ≥${minCount} pad rows` };
    }
    _state = {
        queue,
        cursor: 0,
        startedAt: new Date().toISOString(),
        completed: 0,
        errors: 0,
        lastError: null,
    };
    _running = true;
    _stopRequested = false;
    await _persist();
    console.log(`[bulk-label-sweep] START queue=${queue.length} top=${queue.slice(0, 3).map(q => `${q.labelName}(${q.externalCount})`).join(", ")}`);
    _run().catch(err => console.error("[bulk-label-sweep] runner crashed:", err));
    return { ok: true, queued: queue.length };
}
async function _run() {
    try {
        while (_state && _state.cursor < _state.queue.length) {
            if (_stopRequested) {
                console.log(`[bulk-label-sweep] stop requested at ${_state.cursor}/${_state.queue.length}`);
                break;
            }
            // Wait for catno worker slot to free up. Some other tab may
            // have started a curated sweep manually — just idle here.
            let waited = 0;
            while (isCacheWarmCatnoRunning() && !_stopRequested) {
                await _sleep(3000);
                waited += 3000;
                if (waited > 0 && waited % 60000 === 0) {
                    console.log(`[bulk-label-sweep] waiting ${waited / 1000}s for catno worker`);
                }
            }
            if (_stopRequested)
                break;
            const item = _state.queue[_state.cursor];
            _currentLabel = item;
            console.log(`[bulk-label-sweep] ${_state.cursor + 1}/${_state.queue.length} → ${item.labelName} (id=${item.labelId}, ext=${item.externalCount})`);
            let started;
            try {
                started = await startAdHocLabelSweep(item.labelId, item.labelName);
            }
            catch (err) {
                started = { ok: false, error: err?.message ?? String(err) };
            }
            if (!started.ok) {
                _state.errors++;
                _state.lastError = `${item.labelName}: ${started.error ?? "start failed"}`;
                console.warn(`[bulk-label-sweep] start failed: ${_state.lastError}`);
                _state.cursor++;
                _currentLabel = null;
                await _persist();
                await _sleep(2000);
                continue;
            }
            // Poll until the inner sweep finishes (or user stops).
            // startAdHocLabelSweep returns immediately; the actual sweep
            // runs in its own IIFE and clears _runningKey on exit.
            // Give it a beat to actually mark itself running before polling.
            await _sleep(1500);
            while (isCacheWarmCatnoRunning()) {
                await _sleep(5000);
            }
            _state.completed++;
            _state.cursor++;
            _currentLabel = null;
            await _persist();
            // Small breather between labels — avoids hammering Discogs
            // and gives the singleflight lock a moment to fully release.
            await _sleep(1000);
        }
    }
    catch (err) {
        console.error("[bulk-label-sweep] loop crashed:", err?.stack || err);
        if (_state) {
            _state.lastError = `runner crash: ${err?.message ?? String(err)}`;
            _state.errors++;
            await _persist();
        }
    }
    finally {
        const drained = !!_state && _state.cursor >= _state.queue.length;
        _running = false;
        _stopRequested = false;
        _currentLabel = null;
        if (drained) {
            console.log(`[bulk-label-sweep] queue drained; clearing state`);
            _state = null;
            await _persist();
        }
        else {
            // Stopped mid-queue: keep state so the operator can inspect.
            // A subsequent /start call will build a fresh queue.
            await _persist();
        }
    }
}
// Boot resume: if we crashed / redeployed while a queue was in
// flight, pick up where we left off. Delayed 8 s to let the catno
// module's own boot-resume run first (5 s) so the pointer to the
// currently-active sweep is consistent.
export function initBulkLabelSweepModule() {
    setTimeout(async () => {
        if (_running)
            return;
        const persisted = await _load();
        if (!persisted)
            return;
        if (persisted.cursor >= persisted.queue.length) {
            console.log(`[bulk-label-sweep] persisted state already drained; clearing`);
            _state = null;
            await _persist();
            return;
        }
        console.log(`[bulk-label-sweep] boot-resume at ${persisted.cursor}/${persisted.queue.length}`);
        _state = persisted;
        _running = true;
        _stopRequested = false;
        _run().catch(err => console.error("[bulk-label-sweep] resume crashed:", err));
    }, 8000);
}
