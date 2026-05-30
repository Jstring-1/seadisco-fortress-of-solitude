// ── Background cron: warm release_cache by Discogs genre, rotated ──
//
// One genre per night, rotating through the active set in
// genre_cache_warm_state (currently: Blues / Folk, World, & Country /
// Jazz / Reggae / Latin). Each genre tracks its own cursor + counters
// so progress is independent — a slow Discogs week for jazz doesn't
// hold back blues.
//
// Time window: 1am-6am Pacific (America/Los_Angeles). Outside the
// window the worker idles. A per-genre manual_override flag lets the
// admin kick a specific genre's worker immediately from the UI.
//
// State is persisted in genre_cache_warm_state — every iteration
// updates the cursor + counters so a Railway restart resumes from
// the next release without re-fetching anything already cached.
//
// Rate limit: 1.1s between Discogs calls (Discogs's authed limit is
// 60 req/min). Single in-process worker per genre enforced via the
// row's running claim plus an in-memory guard.

import { DiscogsClient, OAuthCredentials } from "./discogs-client.js";
import {
  listAllGenreCacheWarmStates,
  getGenreCacheWarmState,
  updateGenreCacheWarmState,
  tryClaimGenreCacheWarmRun,
  releaseGenreCacheWarmRun,
  releaseAllStaleGenreCacheWarmRuns,
  recordGenreCacheWarmHit,
  recordGenreCacheWarmSkip,
  recordGenreCacheWarmSearched,
  recordGenreCacheWarmError,
  resetGenreCacheWarmCycle,
  isReleaseCached,
  cacheRelease,
  getOAuthCredentials,
} from "./db.js";

const TZ = "America/Los_Angeles";
const WINDOW_START_HOUR = 1;   // inclusive
const WINDOW_END_HOUR   = 6;   // exclusive
const PER_PAGE          = 100; // Discogs max
const REQ_INTERVAL_MS   = 1100;

const _runningGenres = new Set<string>();   // in-process double-launch guard
const _stopRequested = new Set<string>();   // signal from POST /stop

function _inWindowPacific(): boolean {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: TZ,
    hour: "numeric",
    hour12: false,
  });
  const hour = parseInt(fmt.format(new Date()), 10);
  if (!Number.isFinite(hour)) return false;
  return hour >= WINDOW_START_HOUR && hour < WINDOW_END_HOUR;
}

// Day-of-year in Pacific. Used to pick today's genre — divides ~365
// days evenly across the rotation so each genre fires once every
// activeGenres.length days.
function _dayOfYearPacific(): number {
  const now = new Date();
  // Compute components in Pacific TZ.
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: TZ,
    year: "numeric", month: "2-digit", day: "2-digit",
  }).formatToParts(now);
  const get = (t: string) => parseInt(parts.find(p => p.type === t)?.value || "0", 10);
  const y = get("year"), m = get("month"), d = get("day");
  // Days-from-Jan-1: build a UTC date and diff. Sidestepping local TZ
  // entirely since we already know the Pacific calendar date.
  const jan1Utc = Date.UTC(y, 0, 1);
  const todayUtc = Date.UTC(y, m - 1, d);
  return Math.floor((todayUtc - jan1Utc) / (24 * 3600 * 1000));
}

async function _adminClient(adminClerkId: string): Promise<DiscogsClient | null> {
  const oauth = await getOAuthCredentials(adminClerkId);
  if (!oauth) return null;
  if (!process.env.DISCOGS_CONSUMER_KEY || !process.env.DISCOGS_CONSUMER_SECRET) {
    return null;
  }
  return new DiscogsClient({
    consumerKey:    process.env.DISCOGS_CONSUMER_KEY,
    consumerSecret: process.env.DISCOGS_CONSUMER_SECRET,
    accessToken:    oauth.accessToken,
    accessSecret:   oauth.accessSecret,
  } as OAuthCredentials);
}

function _sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

// Public: signal the in-flight worker for a genre (or all genres
// when key is undefined) to wind down. Idempotent.
export function requestGenreCacheWarmStop(genreKey?: string): void {
  if (genreKey) {
    _stopRequested.add(genreKey);
  } else {
    for (const g of _runningGenres) _stopRequested.add(g);
  }
}

// Pick today's genre from the rotation, considering only enabled
// rows. Returns null when no genre is eligible (e.g. all disabled).
// manual_override-enabled rows take precedence and run regardless
// of the day-of-year cycle so an admin "Start now" on jazz at 3am
// runs jazz, not whatever the day picks.
export function pickTodaysGenre(rows: any[]): any | null {
  const active = rows.filter(r => r.enabled);
  if (!active.length) return null;
  const override = active.find(r => r.manual_override);
  if (override) return override;
  active.sort((a, b) => a.rotation_order - b.rotation_order);
  const idx = _dayOfYearPacific() % active.length;
  return active[idx] ?? null;
}

async function _runWorkerForGenre(
  genreKey: string,
  adminClerkId: string,
): Promise<void> {
  const client = await _adminClient(adminClerkId);
  if (!client) {
    await recordGenreCacheWarmError(
      genreKey,
      "admin Discogs OAuth credentials missing - connect the admin user's Discogs account",
    );
    await releaseGenreCacheWarmRun(genreKey);
    _runningGenres.delete(genreKey);
    return;
  }
  _stopRequested.delete(genreKey);
  try {
    while (true) {
      if (_stopRequested.has(genreKey)) break;
      const state = await getGenreCacheWarmState(genreKey);
      if (!state) break;
      if (!state.enabled && !state.manual_override) break;
      if (!state.manual_override && !_inWindowPacific()) break;

      const year = Number(state.current_year);
      const page = Number(state.current_page);
      const endYear = Number(state.end_year);

      let searchRes: any;
      try {
        searchRes = await client.search("", {
          type: "release",
          genre: genreKey,
          year: String(year),
          page,
          perPage: PER_PAGE,
        });
      } catch (err: any) {
        await recordGenreCacheWarmError(
          genreKey,
          `search ${year} p${page}: ${err?.message ?? String(err)}`,
        );
        await _sleep(5000);
        await updateGenreCacheWarmState(genreKey, { last_tick_at: new Date() });
        continue;
      }

      const results: any[] = Array.isArray(searchRes?.results) ? searchRes.results : [];
      await recordGenreCacheWarmSearched(genreKey, results.length);

      if (!results.length) {
        // Year exhausted - advance.
        const nextYear = year + 1;
        if (nextYear > endYear) {
          await resetGenreCacheWarmCycle(genreKey);
        } else {
          await updateGenreCacheWarmState(genreKey, {
            current_year: nextYear,
            current_page: 1,
            last_tick_at: new Date(),
          });
        }
        continue;
      }

      for (const r of results) {
        if (_stopRequested.has(genreKey)) break;
        const live = await getGenreCacheWarmState(genreKey);
        if (!live?.enabled && !live?.manual_override) { _stopRequested.add(genreKey); break; }
        if (!live?.manual_override && !_inWindowPacific()) { _stopRequested.add(genreKey); break; }

        const id = Number(r?.id);
        if (!Number.isFinite(id) || id <= 0) continue;

        if (await isReleaseCached(id, "release")) {
          await recordGenreCacheWarmSkip(genreKey);
          continue;
        }
        try {
          await _sleep(REQ_INTERVAL_MS);
          const full = await client.getRelease(id) as any;
          await cacheRelease(id, "release", full as object);
          const title = String(full?.title ?? r?.title ?? "(untitled)");
          await recordGenreCacheWarmHit(genreKey, title, id);
        } catch (err: any) {
          await recordGenreCacheWarmError(
            genreKey,
            `release ${id}: ${err?.message ?? String(err)}`,
          );
          await _sleep(REQ_INTERVAL_MS);
        }
      }
      if (_stopRequested.has(genreKey)) break;

      const pagination = searchRes?.pagination;
      const totalPages = Number(pagination?.pages);
      const nextPage = page + 1;
      if (Number.isFinite(totalPages) && nextPage > totalPages) {
        const nextYear = year + 1;
        if (nextYear > endYear) {
          await resetGenreCacheWarmCycle(genreKey);
        } else {
          await updateGenreCacheWarmState(genreKey, {
            current_year: nextYear,
            current_page: 1,
            last_tick_at: new Date(),
          });
        }
      } else {
        await updateGenreCacheWarmState(genreKey, {
          current_page: nextPage,
          last_tick_at: new Date(),
        });
      }
    }
  } finally {
    await updateGenreCacheWarmState(genreKey, { manual_override: false });
    await releaseGenreCacheWarmRun(genreKey);
    _runningGenres.delete(genreKey);
    _stopRequested.delete(genreKey);
  }
}

// One scheduler-tick body, shared by the auto setInterval and the
// "Start now" endpoint so a manual kick doesn't wait up to 60s for
// the next minute boundary. When forceGenreKey is passed, the
// rotation pick is bypassed — used by the Blues "Start now" button
// so blues runs *now*, not whichever genre the day-of-year points at.
async function _tickOnce(
  adminClerkId: string,
  forceGenreKey?: string,
): Promise<void> {
  try {
    const rows = await listAllGenreCacheWarmStates();
    if (!rows.length) return;
    const target = forceGenreKey
      ? rows.find(r => r.genre_key === forceGenreKey) ?? null
      : pickTodaysGenre(rows);
    if (!target) return;
    if (_runningGenres.has(target.genre_key)) return;
    const wantToRun =
      (target.manual_override === true) ||
      (target.enabled === true && _inWindowPacific());
    if (!wantToRun) return;
    if (target.running) {
      const startedMs = target.started_at ? new Date(target.started_at).getTime() : 0;
      if (Date.now() - startedMs > 10 * 60 * 1000) {
        await releaseGenreCacheWarmRun(target.genre_key);
      } else {
        return;
      }
    }
    const claimed = await tryClaimGenreCacheWarmRun(target.genre_key);
    if (!claimed) return;
    _runningGenres.add(target.genre_key);
    _runWorkerForGenre(target.genre_key, adminClerkId).catch(async err => {
      console.error("[genre-cache-warm] worker crashed:", err);
      await recordGenreCacheWarmError(
        target.genre_key,
        `worker crash: ${err?.message ?? String(err)}`,
      );
      await releaseGenreCacheWarmRun(target.genre_key);
      _runningGenres.delete(target.genre_key);
    });
  } catch (err) {
    console.error("[genre-cache-warm] tick error:", err);
  }
}

// Remember the adminClerkId so kickGenreCacheWarmNow() doesn't
// need every route to re-thread it through.
let _adminClerkIdForScheduler: string | null = null;

// Public: kick the worker for a specific genre immediately. The
// admin Start endpoint calls this so the user sees RUNNING flip
// within milliseconds instead of waiting up to 60s for the next
// setInterval tick. Idempotent — re-entry is gated by both the in-
// memory _runningGenres set and the DB run-lock.
export async function kickGenreCacheWarmNow(genreKey: string): Promise<void> {
  if (!_adminClerkIdForScheduler) return;
  await _tickOnce(_adminClerkIdForScheduler, genreKey);
}

// Scheduler - called from app startup. Ticks every minute, picks
// today's genre, decides whether to launch a worker for it.
export function startGenreCacheWarmScheduler(adminClerkId: string): void {
  if (!adminClerkId) {
    console.warn("[genre-cache-warm] no ADMIN_CLERK_ID set - scheduler will not run");
    return;
  }
  _adminClerkIdForScheduler = adminClerkId;
  // At boot: clear any stale running locks from a crashed restart.
  releaseAllStaleGenreCacheWarmRuns(10).catch(() => {});

  const tick = () => _tickOnce(adminClerkId);
  setTimeout(tick, 15_000);
  setInterval(tick, 60_000);
}
