#!/usr/bin/env node
// sync-fresh-releases.js
// Fetches fresh releases from ListenBrainz (last 1 day),
// keeps only those with Cover Art Archive artwork, saves to DB.
// Prunes records older than 14 days.
// Run every 6 hours via Railway cron or: node scripts/sync-fresh-releases.js

import pg from "pg";
const { Pool } = pg;

const LB_API  = "https://api.listenbrainz.org/1/explore/fresh-releases/";
const CAA_URL = (mbid) => `https://coverartarchive.org/release/${mbid}/front-250`;
const UA      = "SeaDisco/1.0 (https://seadisco.com)";

const pool = new Pool({
  connectionString: process.env.APP_DB_URL,
  ssl: { rejectUnauthorized: false },
});

async function ensureTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS fresh_releases (
      id                  SERIAL PRIMARY KEY,
      release_mbid        TEXT UNIQUE NOT NULL,
      release_name        TEXT,
      artist_credit_name  TEXT,
      release_date        DATE,
      primary_type        TEXT,
      secondary_type      TEXT,
      tags                TEXT[],
      caa_id              BIGINT,
      caa_release_mbid    TEXT,
      cover_url           TEXT,
      fetched_at          TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

async function fetchListenBrainz() {
  const url = `${LB_API}?days=1&sort=release_date&past=true&future=false`;
  const r = await fetch(url, { headers: { "User-Agent": UA } });
  if (!r.ok) throw new Error(`ListenBrainz HTTP ${r.status}`);
  const data = await r.json();
  return data.payload?.releases ?? data.releases ?? [];
}

// Check if cover art exists without downloading it (HEAD request)
async function checkCoverArt(caaReleaseMbid) {
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

async function upsert(r) {
  await pool.query(
    `INSERT INTO fresh_releases
       (release_mbid, release_name, artist_credit_name, release_date,
        primary_type, secondary_type, tags, caa_id, caa_release_mbid, cover_url, fetched_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
     ON CONFLICT (release_mbid) DO UPDATE SET
       release_name       = $2,
       artist_credit_name = $3,
       release_date       = $4,
       primary_type       = $5,
       secondary_type     = $6,
       tags               = $7,
       caa_id             = $8,
       caa_release_mbid   = $9,
       cover_url          = $10,
       fetched_at         = NOW()`,
    [r.release_mbid, r.release_name, r.artist_credit_name, r.release_date,
     r.primary_type, r.secondary_type, r.tags, r.caa_id, r.caa_release_mbid, r.cover_url]
  );
}

async function prune() {
  const res = await pool.query(
    `DELETE FROM fresh_releases WHERE fetched_at < NOW() - INTERVAL '14 days'`
  );
  return res.rowCount ?? 0;
}

// Simple rate limiter — wait ms between calls
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function main() {
  console.log("=== sync-fresh-releases starting ===", new Date().toISOString());
  await ensureTable();

  const releases = await fetchListenBrainz();
  console.log(`Fetched ${releases.length} releases from ListenBrainz`);

  let saved = 0, skipped = 0;

  for (const rel of releases) {
    const mbid        = rel.release_mbid;
    const caaRelMbid  = rel.caa_release_mbid ?? mbid;
    const caaId       = rel.caa_id ?? null;

    if (!mbid) { skipped++; continue; }

    // Only save if cover art is available
    const coverUrl = await checkCoverArt(caaRelMbid);
    if (!coverUrl) { skipped++; continue; }

    await upsert({
      release_mbid:       mbid,
      release_name:       rel.release_name        ?? null,
      artist_credit_name: rel.artist_credit_name  ?? null,
      release_date:       rel.release_date         ?? null,
      primary_type:       rel.release_group_primary_type   ?? null,
      secondary_type:     rel.release_group_secondary_type ?? null,
      tags:               (rel.release_tags ?? []).slice(0, 10),
      caa_id:             caaId,
      caa_release_mbid:   caaRelMbid,
      cover_url:          coverUrl,
    });
    saved++;
    // Gentle rate limit: ~3 req/sec to Cover Art Archive
    await sleep(350);
  }

  const pruned = await prune();
  console.log(`Done. Saved: ${saved}, Skipped (no art): ${skipped}, Pruned (old): ${pruned}`);
  await pool.end();
}

main().catch(err => {
  console.error("sync-fresh-releases failed:", err);
  process.exit(1);
});
