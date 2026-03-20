// sync-fresh-releases.ts
// Fetches fresh releases from ListenBrainz, saves to DB.
// On first run (DB sparse): fetches 28 days to backfill.
// On subsequent runs: fetches 1 day incrementally.
// Called on server startup and every 6 hours thereafter.

import pg from "pg";
const { Pool } = pg;
import { upsertFreshRelease, pruneFreshReleases } from "./db.js";

const LB_API  = "https://api.listenbrainz.org/1/explore/fresh-releases/";
const CAA_URL = (mbid: string) => `https://coverartarchive.org/release/${mbid}/front-250`;
const UA      = "SeaDisco/1.0 (https://seadisco.com)";
const INTERVAL_MS   = 6 * 60 * 60 * 1000; // 6 hours
const DELAY_MS      = 350;                 // ~3 req/sec to CAA
const BACKFILL_DAYS = 28;
const NORMAL_DAYS   = 1;
const SPARSE_THRESHOLD = 400;             // below this → treat as sparse, do backfill

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

async function fetchListenBrainz(days: number): Promise<any[]> {
  const url = `${LB_API}?days=${days}&sort=release_date&past=true&future=false`;
  const r = await fetch(url, { headers: { "User-Agent": UA } });
  if (!r.ok) throw new Error(`ListenBrainz HTTP ${r.status}`);
  const data = await r.json() as any;
  return data.payload?.releases ?? data.releases ?? [];
}

async function checkCoverArt(caaReleaseMbid: string): Promise<string | null> {
  if (!caaReleaseMbid) return null;
  try {
    const r = await fetch(CAA_URL(caaReleaseMbid), {
      method: "HEAD",
      headers: { "User-Agent": UA },
      redirect: "follow",
    });
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
    console.log(`[fresh-sync] fetched ${releases.length} releases`);

    let saved = 0, skipped = 0;

    for (const rel of releases) {
      const mbid       = rel.release_mbid as string;
      const caaRelMbid = (rel.caa_release_mbid ?? mbid) as string;

      if (!mbid) { skipped++; continue; }

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
      });
      saved++;
      await sleep(DELAY_MS);
    }

    const pruned = await pruneFreshReleases();
    console.log(`[fresh-sync] done — saved: ${saved}, skipped: ${skipped}, pruned: ${pruned}`);
  } catch (err) {
    console.error("[fresh-sync] error:", err);
  }
}

export function startFreshSyncSchedule(): void {
  // Run once immediately (with a short delay so DB is ready)
  setTimeout(() => runFreshSync(), 15_000);
  // Then every 6 hours
  setInterval(() => runFreshSync(), INTERVAL_MS);
  console.log("[fresh-sync] scheduled every 6 hours");
}
