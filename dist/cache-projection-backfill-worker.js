// ── Split-cache projection backfill worker ────────────────────────
//
// One-shot chunked scan over release_cache that projects every
// release/master row into the new discogs_cache_masters_plus /
// discogs_cache_pressings + release_labels / release_artists /
// release_tags schema. Idempotent: re-running is safe (each row is
// DELETE-then-INSERT keyed by (discogs_id, bucket) inside
// writeProjectedCache).
//
// Singleflight; stop/force-clear compatible; boot-resume via
// app_settings so a Railway restart picks up where it left off.
import { getAppSetting, setAppSetting, writeProjectedCacheBatch, readReleaseCacheBatchForProjection, getProjectedCacheStats, } from "./db.js";
const STATE_KEY = "cache_projection_backfill_state";
// Batch size tuned for the batched writer: ~8 round trips per batch
// regardless of size, so we can push a lot of rows per network hop.
// Kept below the read helper's 1000-row cap to stay conservative on
// TOAST + WAL amplification.
const BATCH_SIZE = 500;
const INTER_BATCH_MS = 50;
let _state = null;
let _running = false;
let _stopRequested = false;
function _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
async function _persist() {
    try {
        await setAppSetting(STATE_KEY, _state ? JSON.stringify(_state) : null);
    }
    catch { }
}
async function _load() {
    try {
        const raw = await getAppSetting(STATE_KEY);
        if (!raw)
            return null;
        return JSON.parse(raw);
    }
    catch {
        return null;
    }
}
export function isCacheProjectionBackfillRunning() { return _running; }
export function getCacheProjectionBackfillStatus() {
    if (!_state) {
        return { running: false, cursorId: 0, processed: 0, errors: 0, lastError: null, startedAt: null, total: null };
    }
    return {
        running: _running,
        cursorId: _state.cursorId,
        processed: _state.processed,
        errors: _state.errors,
        lastError: _state.lastError ?? null,
        startedAt: _state.startedAt,
        total: _state.releaseCacheProjectable,
    };
}
export function requestCacheProjectionBackfillStop() {
    if (!_running)
        return;
    _stopRequested = true;
}
export function forceClearCacheProjectionBackfill() {
    console.log(`[cache-project-backfill] FORCE clear (was running=${_running}, cursor=${_state?.cursorId ?? "?"})`);
    _running = false;
    _stopRequested = false;
    _state = null;
    _persist().catch(() => { });
}
export async function startCacheProjectionBackfill(opts = {}) {
    if (_running)
        return { ok: false, error: "Backfill already running" };
    const persisted = opts.resetCursor ? null : await _load();
    const stats = await getProjectedCacheStats();
    _state = persisted && !opts.resetCursor
        ? {
            ...persisted,
            releaseCacheProjectable: stats.releaseCacheProjectable,
            batchSize: persisted.batchSize || BATCH_SIZE,
        }
        : {
            cursorId: 0,
            startedAt: new Date().toISOString(),
            processed: 0,
            errors: 0,
            lastError: null,
            batchSize: BATCH_SIZE,
            releaseCacheProjectable: stats.releaseCacheProjectable,
        };
    _running = true;
    _stopRequested = false;
    await _persist();
    console.log(`[cache-project-backfill] START total=${stats.releaseCacheProjectable} cursorId=${_state.cursorId} processed=${_state.processed}`);
    _run().catch(err => console.error("[cache-project-backfill] runner crashed:", err));
    return { ok: true, total: stats.releaseCacheProjectable };
}
async function _run() {
    try {
        while (_state && !_stopRequested) {
            const batch = await readReleaseCacheBatchForProjection(_state.cursorId, _state.batchSize);
            if (batch.length === 0) {
                console.log(`[cache-project-backfill] queue drained at ${_state.processed} processed`);
                break;
            }
            const result = await writeProjectedCacheBatch(batch);
            _state.processed += result.ok;
            _state.errors += result.err;
            if (result.lastError)
                _state.lastError = result.lastError;
            // Cursor advances to the max discogs_id in the batch even when
            // some rows failed — the per-row fallback inside the batched
            // writer already retried them once; skipping avoids the loop
            // getting stuck on a poison row.
            let maxIdInBatch = _state.cursorId;
            for (const row of batch) {
                if (row.discogs_id > maxIdInBatch)
                    maxIdInBatch = row.discogs_id;
            }
            _state.cursorId = maxIdInBatch;
            await _persist();
            if (_stopRequested)
                break;
            await _sleep(INTER_BATCH_MS);
        }
    }
    catch (err) {
        console.error("[cache-project-backfill] loop crashed:", err?.stack || err);
        if (_state) {
            _state.errors++;
            _state.lastError = `loop crash: ${err?.message ?? String(err)}`;
            await _persist();
        }
    }
    finally {
        const drained = !!_state && !_stopRequested;
        _running = false;
        _stopRequested = false;
        if (drained) {
            console.log(`[cache-project-backfill] complete; leaving state so operator can inspect final counts`);
            // Leave state for the UI; a fresh /start rebuilds it. Reader
            // will just see cursorId near max and processed near total.
            await _persist();
        }
        else {
            await _persist();
        }
    }
}
export function initCacheProjectionBackfillModule() {
    setTimeout(async () => {
        if (_running)
            return;
        const persisted = await _load();
        if (!persisted)
            return;
        // Only auto-resume if the previous run wasn't already at the end.
        // Absent a persisted "done" flag we don't know for sure, but a
        // fresh /start from the UI is cheap so we auto-resume only if the
        // cursor is clearly in-progress (cursorId > 0 and processed > 0).
        if (persisted.cursorId <= 0 && persisted.processed <= 0)
            return;
        console.log(`[cache-project-backfill] boot-resume at cursorId=${persisted.cursorId} processed=${persisted.processed}`);
        _state = persisted;
        _running = true;
        _stopRequested = false;
        _run().catch(err => console.error("[cache-project-backfill] resume crashed:", err));
    }, 10000);
}
