// sync-fresh-releases.ts
// Fetches fresh releases from ListenBrainz, saves to DB.
// On first run (DB sparse): fetches 28 days to backfill.
// On subsequent runs: fetches 1 day incrementally.
// Called on server startup and every 6 hours thereafter.

import pg from "pg";
const { Pool } = pg;
import { upsertFreshRelease, pruneFreshReleases, logApiRequest } from "./db.js";

const LB_API  = "https://api.listenbrainz.org/1/explore/fresh-releases/";
const CAA_URL = (mbid: string) => `https://coverartarchive.org/release/${mbid}/front-250`;
const UA      = "SeaDisco/1.0 (https://seadisco.com)";
const INTERVAL_MS   = 6 * 60 * 60 * 1000; // 6 hours
const DELAY_MS      = 350;                 // ~3 req/sec to CAA
const BACKFILL_DAYS = 90;                  // 3 months on first run
const NORMAL_DAYS   = 1;
const SPARSE_THRESHOLD = 1500;            // below this → treat as sparse, do backfill

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function countFreshReleases(): Promise<number> {
  const connStr = process.env.APP_DB_URL;
  if (!connStr) return 999; // no DB, skip backfill
  const pool = new Pool({ connectionString: connStr, ssl: { rejectUnauthorized: false } });
  try {
    const r = await pool.query("SELECT COUNT(*)::int AS n FROM fresh_releases");
    return r.rows[0]?.n ?? 0;
  } catch { return 0; }
  finally { await pool.end(); }
}

async function getExistingMbids(): Promise<Set<string>> {
  const connStr = process.env.APP_DB_URL;
  if (!connStr) return new Set();
  const pool = new Pool({ connectionString: connStr, ssl: { rejectUnauthorized: false } });
  try {
    const r = await pool.query("SELECT release_mbid FROM fresh_releases WHERE cover_url IS NOT NULL");
    return new Set(r.rows.map(row => row.release_mbid));
  } catch { return new Set(); }
  finally { await pool.end(); }
}

async function fetchListenBrainz(days: number): Promise<any[]> {
  const url = `${LB_API}?days=${days}&sort=release_date&past=true&future=false`;
  const start = Date.now();
  const r = await fetch(url, { headers: { "User-Agent": UA } });
  logApiRequest({ service: "listenbrainz", endpoint: url, statusCode: r.status, success: r.ok, durationMs: Date.now() - start, context: `${days}d fresh` }).catch(() => {});
  if (!r.ok) throw new Error(`ListenBrainz HTTP ${r.status}`);
  const data = await r.json() as any;
  return data.payload?.releases ?? data.releases ?? [];
}

async function checkCoverArt(caaReleaseMbid: string): Promise<string | null> {
  if (!caaReleaseMbid) return null;
  try {
    const start = Date.now();
    const r = await fetch(CAA_URL(caaReleaseMbid), {
      method: "HEAD",
      headers: { "User-Agent": UA },
      redirect: "follow",
    });
    logApiRequest({ service: "coverartarchive", endpoint: CAA_URL(caaReleaseMbid), method: "HEAD", statusCode: r.status, success: r.ok, durationMs: Date.now() - start }).catch(() => {});
    if (r.ok) return CAA_URL(caaReleaseMbid);
  } catch { /* no art */ }
  return null;
}

export async function runFreshSync(): Promise<void> {
  console.log("[fresh-sync] starting", new Date().toISOString());
  try {
    const count = await countFreshReleases();
    const days  = count < SPARSE_THRESHOLD ? BACKFILL_DAYS : NORMAL_DAYS;
    console.log(`[fresh-sync] DB has ${count} records — fetching ${days} day(s) from ListenBrainz`);

    const releases = await fetchListenBrainz(days);
    const existingMbids = await getExistingMbids();
    console.log(`[fresh-sync] fetched ${releases.length} releases, ${existingMbids.size} already have cover art`);

    let saved = 0, skipped = 0, reused = 0;

    for (const rel of releases) {
      const mbid       = rel.release_mbid as string;
      const caaRelMbid = (rel.caa_release_mbid ?? mbid) as string;

      if (!mbid) { skipped++; continue; }

      // Skip CAA check if we already have this release with cover art
      if (existingMbids.has(mbid)) { reused++; continue; }

      const coverUrl = await checkCoverArt(caaRelMbid);
      if (!coverUrl) { skipped++; continue; }

      await upsertFreshRelease({
        release_mbid:       mbid,
        release_name:       rel.release_name                   ?? null,
        artist_credit_name: rel.artist_credit_name             ?? null,
        release_date:       rel.release_date                   ?? null,
        primary_type:       rel.release_group_primary_type     ?? null,
        secondary_type:     rel.release_group_secondary_type   ?? null,
        tags:               (rel.release_tags ?? []).slice(0, 10),
        caa_id:             rel.caa_id                         ?? null,
        caa_release_mbid:   caaRelMbid,
        cover_url:          coverUrl,
        release_group_mbid: rel.release_group_mbid             ?? null,
        artist_mbids:       rel.artist_mbids                   ?? [],
      });
      saved++;
      await sleep(DELAY_MS);
    }

    const pruned = await pruneFreshReleases();
    console.log(`[fresh-sync] done — saved: ${saved}, skipped: ${skipped}, reused: ${reused}, pruned: ${pruned}`);
  } catch (err) {
    console.error("[fresh-sync] error:", err);
  }
}

export function startFreshSyncSchedule(): void {
  // Every 6 hours aligned to :30
  const msTo30 = msUntilMinute(30);
  console.log(`[fresh-sync] Next fetch at :30 (in ${Math.round(msTo30 / 60000)}min), then every 6h`);
  setTimeout(() => {
    runFreshSync();
    setInterval(() => runFreshSync(), INTERVAL_MS);
  }, msTo30);
}

function msUntilMinute(minute: number): number {
  const now = new Date();
  const target = new Date(now);
  target.setMinutes(minute, 0, 0);
  if (target.getTime() <= now.getTime()) target.setHours(target.getHours() + 1);
  return target.getTime() - now.getTime();
}
