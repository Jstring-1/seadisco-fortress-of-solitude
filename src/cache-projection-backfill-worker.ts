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

function _clearPersistedState(): void {
  // Wipe the boot-resume marker so nothing tries to restart projection.
  setAppSetting(STATE_KEY, null).catch(() => {});
}

export function isCacheProjectionBackfillRunning(): boolean { return false; }

export function getCacheProjectionBackfillStatus(): {
  running:   boolean;
  cursorId:  number;
  processed: number;
  errors:    number;
  lastError: string | null;
  startedAt: string | null;
  total:     number | null;
} {
  return { running: false, cursorId: 0, processed: 0, errors: 0, lastError: null, startedAt: null, total: null };
}

export function requestCacheProjectionBackfillStop(): void {
  _clearPersistedState();
}

export function forceClearCacheProjectionBackfill(): void {
  _clearPersistedState();
}

export async function startCacheProjectionBackfill(
  _opts: { resetCursor?: boolean } = {}
): Promise<{ ok: boolean; error?: string; total?: number }> {
  // Split cache retired — refuse to start.
  return { ok: false, error: "Split cache retired — projection disabled." };
}

export function initCacheProjectionBackfillModule(): void {
  // Was: setTimeout auto-resume from persisted state. Now: clear that
  // state so a restart can't relaunch the backfill.
  _clearPersistedState();
}
