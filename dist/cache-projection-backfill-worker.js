// ── Split-cache projection backfill worker — RETIRED ──────────────────
//
// The V2 split cache (discogs_cache_masters_plus / discogs_cache_pressings
// + release_labels / release_artists / release_tags) was dropped — see
// initDb in db.ts. This worker used to project release_cache into it.
//
// Every entry point is now inert so nothing writes to the (non-existent)
// split tables, and init CLEARS any persisted resume-state so a Railway
// restart can never relaunch the old backfill against the dropped tables.
// The exports are kept as no-op stubs only so the (now-unwired) admin
// projection routes + the /api/admin/workers status aggregate still link.
import { setAppSetting } from "./db.js";
const STATE_KEY = "cache_projection_backfill_state";
function _clearPersistedState() {
    // Wipe the boot-resume marker so nothing tries to restart projection.
    setAppSetting(STATE_KEY, null).catch(() => { });
}
export function isCacheProjectionBackfillRunning() { return false; }
export function getCacheProjectionBackfillStatus() {
    return { running: false, cursorId: 0, processed: 0, errors: 0, lastError: null, startedAt: null, total: null };
}
export function requestCacheProjectionBackfillStop() {
    _clearPersistedState();
}
export function forceClearCacheProjectionBackfill() {
    _clearPersistedState();
}
export async function startCacheProjectionBackfill(_opts = {}) {
    // Split cache retired — refuse to start.
    return { ok: false, error: "Split cache retired — projection disabled." };
}
export function initCacheProjectionBackfillModule() {
    // Was: setTimeout auto-resume from persisted state. Now: clear that
    // state so a restart can't relaunch the backfill.
    _clearPersistedState();
}
