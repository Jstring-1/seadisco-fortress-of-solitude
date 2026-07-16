// ── Label upstream stats worker ──────────────────────────────────
//
// For each label in external_discography that has a Discogs id and
// either has no cached upstream total OR whose stored total is older
// than the stale window, calls /labels/{id}/releases?per_page=1 once
// and records `pagination.items` into label_upstream_stats. That
// single-page call reveals Discogs's total release count for the
// label without pulling any actual releases — cheap way to see how
// big each label is before deciding to sweep it.
//
// Same singleflight pattern as the other coverage workers: build
// the queue on start, walk it with a persisted cursor, resume on
// boot.

import { DiscogsClient, getAdminDiscogsClient } from "./discogs-client.js";
import {
  getOAuthCredentials,
  getAppSetting,
  setAppSetting,
  listLabelIdsNeedingUpstreamRefresh,
  setLabelUpstreamTotal,
} from "./db.js";
import { retryTransient } from "./worker-retry.js";

const STATE_KEY       = "label_upstream_stats_state";
const REQ_INTERVAL_MS = 1000;

interface QueueItem { labelId: number; labelName: string; externalCount: number; }
interface State {
  queue:          QueueItem[];
  cursor:         number;
  staleAfterDays: number;
  startedAt:      string;
  filled:         number;   // rows with a real total written
  empty:          number;   // rows with total = null (404 / no releases)
  errors:         number;
  lastError?:     string | null;
}

let _state:         State   | null = null;
let _running:       boolean        = false;
let _stopRequested: boolean        = false;
let _adminClerkId:  string  | null = null;

const _sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

async function _persist() {
  try { await setAppSetting(STATE_KEY, _state ? JSON.stringify(_state) : null); }
  catch (err) { console.error("[label-upstream-stats] persist failed:", err); }
}
async function _load(): Promise<State | null> {
  try {
    const raw = await getAppSetting(STATE_KEY);
    if (!raw) return null;
    const p = JSON.parse(raw);
    return p && Array.isArray(p.queue) ? p as State : null;
  } catch { return null; }
}

async function _adminClient(): Promise<DiscogsClient | null> {
  const c = await getAdminDiscogsClient(_adminClerkId);
  // Scheduled background job: above the cache sweeps, below realtime.
  return c ? c.withPriority("scheduled") : null;
}

export function isLabelUpstreamStatsRunning(): boolean { return _running; }

export function getLabelUpstreamStatsStatus() {
  if (!_state) return {
    running: false, total: 0, cursor: 0,
    filled: 0, empty: 0, errors: 0,
    staleAfterDays: null as number | null,
    startedAt: null as string | null,
    lastError: null as string | null,
    currentLabel: null as QueueItem | null,
  };
  return {
    running:        _running,
    total:          _state.queue.length,
    cursor:         _state.cursor,
    filled:         _state.filled,
    empty:          _state.empty,
    errors:         _state.errors,
    staleAfterDays: _state.staleAfterDays,
    startedAt:      _state.startedAt,
    lastError:      _state.lastError ?? null,
    currentLabel:   _state.queue[_state.cursor] ?? null,
  };
}

export function requestLabelUpstreamStatsStop(): void {
  if (_running) _stopRequested = true;
}
export function forceClearLabelUpstreamStats(): void {
  console.log(`[label-upstream-stats] FORCE clear (was running=${_running})`);
  _running = false; _stopRequested = false; _state = null;
  _persist().catch(() => {});
}

export async function startLabelUpstreamStats(opts: {
  staleAfterDays?: number;
  resetCursor?:    boolean;
} = {}): Promise<{ ok: boolean; error?: string; queued?: number }> {
  if (_running) return { ok: false, error: "Label upstream stats already running" };
  const client = await _adminClient();
  if (!client) return { ok: false, error: "Admin Discogs OAuth not connected" };
  const staleAfterDays = Number.isFinite(opts.staleAfterDays as number) && (opts.staleAfterDays as number) >= 0
    ? Number(opts.staleAfterDays)
    : 30;
  const rows = await listLabelIdsNeedingUpstreamRefresh({ staleAfterDays, limit: 20000 });
  const queue: QueueItem[] = rows.map(r => ({
    labelId:       r.label_id,
    labelName:     r.label_name,
    externalCount: r.external_count,
  }));
  if (queue.length === 0) {
    return { ok: false, error: `no labels need refresh (staleAfterDays=${staleAfterDays})` };
  }

  // Resume when the persisted queue matches this one exactly. Any
  // drift (new label appeared, one got its upstream filled in via
  // another mechanism) and we rebuild — otherwise the cursor points
  // into a stale queue.
  const persisted = opts.resetCursor ? null : await _load();
  const reusable = persisted
    && persisted.queue.length === queue.length
    && persisted.queue.every((q, i) => q.labelId === queue[i].labelId);
  _state = reusable
    ? persisted
    : {
        queue,
        cursor:         0,
        staleAfterDays,
        startedAt:      new Date().toISOString(),
        filled:         0,
        empty:          0,
        errors:         0,
        lastError:      null,
      };
  _state.staleAfterDays = staleAfterDays;
  _running       = true;
  _stopRequested = false;
  await _persist();
  console.log(`[label-upstream-stats] START ${_state.queue.length} labels, staleAfterDays=${staleAfterDays}, cursor=${_state.cursor}`);
  _run(client).catch(err => console.error("[label-upstream-stats] runner crashed:", err));
  return { ok: true, queued: _state.queue.length };
}

async function _run(client: DiscogsClient): Promise<void> {
  try {
    while (_state && _state.cursor < _state.queue.length && !_stopRequested) {
      const item = _state.queue[_state.cursor];
      try { await _processLabel(client, item); }
      catch (err: any) {
        _state.errors++;
        _state.lastError = `${item.labelName} (${item.labelId}): ${err?.message ?? String(err)}`;
        console.warn(`[label-upstream-stats] ${_state.lastError}`);
      }
      _state.cursor++;
      await _persist();
      if (_stopRequested) break;
      await _sleep(REQ_INTERVAL_MS);
    }
  } finally {
    const drained = _state && _state.cursor >= _state.queue.length;
    _running       = false;
    _stopRequested = false;
    if (drained) console.log(`[label-upstream-stats] drained; filled=${_state?.filled} empty=${_state?.empty} errors=${_state?.errors}`);
    await _persist();
  }
}

async function _processLabel(client: DiscogsClient, item: QueueItem): Promise<void> {
  // Single 1-item page fetch. Discogs returns `pagination.items` = the
  // total across all pages, which is what we want. A 404 means the
  // label id was deleted upstream — record null and move on so we
  // don't keep re-fetching.
  let payload: any;
  try {
    payload = await retryTransient(
      () => client.getLabelReleases(item.labelId, { page: 1, perPage: 1 }),
      { label: `label-upstream-stats getLabelReleases=${item.labelId}` },
    );
  } catch (err: any) {
    const msg = String(err?.message ?? err ?? "");
    if (/404/.test(msg)) {
      await setLabelUpstreamTotal(item.labelId, null);
      _state!.empty++;
      return;
    }
    throw err;
  }
  const total = Number(payload?.pagination?.items);
  if (Number.isFinite(total) && total >= 0) {
    await setLabelUpstreamTotal(item.labelId, total);
    if (total === 0) _state!.empty++;
    else             _state!.filled++;
  } else {
    // Unexpected shape — record null and move on. Keeps the cursor
    // advancing rather than getting stuck on a single label.
    await setLabelUpstreamTotal(item.labelId, null);
    _state!.empty++;
  }
}

export function initLabelUpstreamStatsModule(adminClerkId: string): void {
  _adminClerkId = adminClerkId || null;
  setTimeout(async () => {
    if (_running) return;
    const persisted = await _load();
    if (!persisted) return;
    if (persisted.cursor >= persisted.queue.length) {
      _state = null; await _persist(); return;
    }
    const client = await _adminClient();
    if (!client) { console.warn(`[label-upstream-stats] boot-resume: no admin client, skipping`); return; }
    _state         = persisted;
    _running       = true;
    _stopRequested = false;
    console.log(`[label-upstream-stats] boot-resume at ${persisted.cursor}/${persisted.queue.length}`);
    _run(client).catch(err => console.error("[label-upstream-stats] resume crashed:", err));
  }, 20000);
}
