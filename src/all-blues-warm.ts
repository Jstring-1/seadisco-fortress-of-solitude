// ── All Blues background worker ───────────────────────────────────
//
// Scans cached releases (release_cache) in the 1900-1970 window,
// collects every Discogs artist ID referenced by those releases,
// then fetches each artist's /artists/:id profile from Discogs at
// the standard 1.1s rate. Parses [aNNNNN] artist-mention bbcode out
// of the profile prose and stores edges in all_blues_links with a
// best-effort `kind` inferred from the surrounding keywords
// (family / spouse / mentor / band / alias / mention).
//
// Resumable: cursor lives in all_blues_artist_queue (pending rows)
// + all_blues_warm_state.running. Admin clicks Start, can Stop.
// Boot-resume rehydrates a crashed run from the persisted intent.

import { DiscogsClient, OAuthCredentials } from "./discogs-client.js";
import {
  getPool,
  getOAuthCredentials,
  getAppSetting,
  setAppSetting,
} from "./db.js";

const ACTIVE_KEY = "all_blues_warm_active";
const REQ_INTERVAL_MS = 1100;
const DEFAULT_FROM = 1900;
const DEFAULT_TO   = 1970;

let _running = false;
let _stopRequested = false;
let _adminClerkId: string | null = null;
let _activeParams: { fromYear: number; toYear: number; startedAt: string } | null = null;

function _sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

export function isAllBluesRunning() { return _running; }
export function getAllBluesActiveParams() { return _activeParams ? { ..._activeParams } : null; }
export function requestAllBluesStop() {
  if (_running) _stopRequested = true;
  setAppSetting(ACTIVE_KEY, null).catch(() => {});
}
export function forceClearAllBluesRunning() {
  _running = false;
  _stopRequested = false;
  _activeParams = null;
  setAppSetting(ACTIVE_KEY, null).catch(() => {});
}

export function initAllBluesModule(adminClerkId: string) {
  _adminClerkId = adminClerkId || null;
  setTimeout(() => {
    getAppSetting(ACTIVE_KEY).then(raw => {
      if (!raw || _running) return;
      try {
        const p = JSON.parse(raw);
        console.log(`[all-blues] boot-resume: ${p.fromYear}-${p.toYear}`);
        startAllBluesRun({ fromYear: p.fromYear, toYear: p.toYear }).catch(err =>
          console.error("[all-blues] boot-resume failed:", err));
      } catch { /* ignore */ }
    }).catch(() => {});
  }, 5000);
}

async function _adminClient(): Promise<DiscogsClient | null> {
  if (!_adminClerkId) return null;
  const oauth = await getOAuthCredentials(_adminClerkId);
  if (!oauth) return null;
  if (!process.env.DISCOGS_CONSUMER_KEY || !process.env.DISCOGS_CONSUMER_SECRET) return null;
  return new DiscogsClient({
    consumerKey:    process.env.DISCOGS_CONSUMER_KEY,
    consumerSecret: process.env.DISCOGS_CONSUMER_SECRET,
    accessToken:    oauth.accessToken,
    accessSecret:   oauth.accessSecret,
  } as OAuthCredentials);
}

function _isTransientDiscogsError(err: any): boolean {
  const msg = String(err?.message ?? err ?? "");
  if (/Discogs API error (5\d\d|429)/.test(msg)) return true;
  if (/timeout|abort/i.test(msg)) return true;
  if (/ECONN(RESET|REFUSED)|ENOTFOUND|EAI_AGAIN|fetch failed|socket hang up|network/i.test(msg)) return true;
  return false;
}

async function _withRetry<T>(label: string, fn: () => Promise<T>): Promise<T> {
  const delays = [2000, 4000];
  let lastErr: any;
  for (let attempt = 0; attempt < delays.length + 1; attempt++) {
    try { return await fn(); }
    catch (err: any) {
      lastErr = err;
      if (!_isTransientDiscogsError(err) || attempt === delays.length) throw err;
      console.warn(`[all-blues] ${label} retry ${attempt + 1}/${delays.length}: ${err?.message ?? err}`);
      await _sleep(delays[attempt]);
    }
  }
  throw lastErr;
}

// ── Public API ────────────────────────────────────────────────────

export async function startAllBluesRun(opts: {
  fromYear?: number;
  toYear?: number;
  resetQueue?: boolean;
}): Promise<{ ok: boolean; error?: string }> {
  if (_running) return { ok: false, error: "All Blues worker already running" };
  const fromYear = opts.fromYear ?? DEFAULT_FROM;
  const toYear   = opts.toYear   ?? DEFAULT_TO;
  const client = await _adminClient();
  if (!client) return { ok: false, error: "Admin Discogs OAuth not connected (or DISCOGS_CONSUMER_KEY/SECRET missing)" };

  _running = true;
  _stopRequested = false;
  _activeParams = { fromYear, toYear, startedAt: new Date().toISOString() };
  await setAppSetting(ACTIVE_KEY, JSON.stringify({ fromYear, toYear }));

  await getPool().query(
    `UPDATE all_blues_warm_state SET running=true, phase='collect', from_year=$1, to_year=$2,
       started_at=NOW(), last_tick_at=NOW(), last_error=NULL WHERE id=1`,
    [fromYear, toYear],
  );

  if (opts.resetQueue) {
    // Full reset: clear queue + edges. We keep discogs_artist_cache
    // because the Discogs payloads are still valid — re-parsing them
    // on the next fetch costs zero API calls. Counters reset too so
    // the stats panel reflects the fresh run.
    await getPool().query(`DELETE FROM all_blues_artist_queue`);
    await getPool().query(`DELETE FROM all_blues_links`);
    await getPool().query(
      `UPDATE all_blues_warm_state SET artists_queued=0, artists_fetched=0,
         artists_errored=0, links_inserted=0, last_error=NULL WHERE id=1`,
    );
  }

  (async () => {
    try {
      await _runCollect(fromYear, toYear);
      if (!_stopRequested) await _runReleaseNotes(fromYear, toYear);
      if (!_stopRequested) await _runFetch(client);
      console.log("[all-blues] worker exited cleanly");
      await setAppSetting(ACTIVE_KEY, null);
    } catch (err: any) {
      console.error("[all-blues] worker crashed:", err?.stack || err);
      try {
        await getPool().query(
          `UPDATE all_blues_warm_state SET last_error=$1 WHERE id=1`,
          [`crash: ${err?.message ?? String(err)}`],
        );
      } catch {}
    } finally {
      _running = false;
      _stopRequested = false;
      _activeParams = null;
      try { await getPool().query(`UPDATE all_blues_warm_state SET running=false, phase='idle' WHERE id=1`); } catch {}
    }
  })().catch(err => console.error("[all-blues] outer IIFE rejected:", err));

  return { ok: true };
}

// ── Phase 1: collect blues seed artists from cached releases ──────
// One bulk SQL pass. Walks release_cache for releases that:
//   • have a year in the requested window (default 1900-1970)
//   • list 'Blues' in their data->'genres' array
// For each such release, we grab every artist in data->'artists' (the
// primary credit) and add them as a seed with seed_year = the earliest
// year of any blues release they appear on. extraartists are NOT seeds
// — they include sidemen / producers / guest spots that often span
// genres and would pull jazz/folk contemporaries into the network.
// They can still appear as edge targets if they're also a primary on
// some other blues release.
async function _runCollect(fromYear: number, toYear: number): Promise<void> {
  console.log(`[all-blues] collect phase: years ${fromYear}-${toYear} (Blues genre only)`);
  const sql = `
    WITH blues_releases AS (
      SELECT rc.data, (rc.data->>'year')::int AS yr
        FROM release_cache rc
       WHERE rc.type = 'release'
         AND (rc.data->>'year') ~ '^[0-9]+$'
         AND (rc.data->>'year')::int BETWEEN $1 AND $2
         AND EXISTS (
           SELECT 1
             FROM jsonb_array_elements_text(COALESCE(rc.data->'genres', '[]'::jsonb)) AS g
            WHERE g = 'Blues'
         )
    ),
    artist_refs AS (
      SELECT (a->>'id')::int AS aid,
             MIN(yr) AS first_year,
             -- earliest non-empty name we see for this artist across
             -- their cached blues releases. Gives us a label to show
             -- on the graph immediately, before the Discogs API fetch
             -- phase has had a chance to write the canonical name.
             (array_agg(a->>'name' ORDER BY yr) FILTER (WHERE COALESCE(a->>'name','') <> ''))[1] AS name
        FROM blues_releases
        CROSS JOIN LATERAL jsonb_array_elements(
          COALESCE(blues_releases.data->'artists', '[]'::jsonb)
        ) AS a
       WHERE (a->>'id') ~ '^[0-9]+$'
         AND (a->>'id')::int > 0
       GROUP BY (a->>'id')::int
    )
    INSERT INTO all_blues_artist_queue (discogs_id, seed_year, name)
    SELECT aid, first_year, name FROM artist_refs
    ON CONFLICT (discogs_id) DO UPDATE
      SET seed_year = LEAST(
            COALESCE(all_blues_artist_queue.seed_year, EXCLUDED.seed_year),
            EXCLUDED.seed_year
          ),
          name = COALESCE(all_blues_artist_queue.name, EXCLUDED.name)
  `;
  const result = await getPool().query(sql, [fromYear, toYear]);
  const queued = result.rowCount ?? 0;

  // ── Manual seed import from Blues Archive ──────────────────────
  // Every blues_artists row with a discogs_id becomes a seed too, so
  // manually curated artists who don't happen to appear on a cached
  // Blues release in the year window still show up on the network.
  // seed_year uses first_recording_year (or 1900 as a sortable
  // sentinel) so the queue ordering still walks oldest first.
  const manualSeeds = await getPool().query(`
    INSERT INTO all_blues_artist_queue (discogs_id, seed_year, name)
    SELECT discogs_id,
           COALESCE(first_recording_year, 1900),
           name
      FROM blues_artists
     WHERE discogs_id IS NOT NULL
    ON CONFLICT (discogs_id) DO UPDATE
      SET name      = COALESCE(all_blues_artist_queue.name, EXCLUDED.name),
          seed_year = LEAST(
            COALESCE(all_blues_artist_queue.seed_year, EXCLUDED.seed_year),
            EXCLUDED.seed_year
          )
  `);

  // ── Manual edge import from blues_artist_links ─────────────────
  // Join on discogs_id (both endpoints must be Discogs-linked). Kind
  // names mostly match the Constellations schema; pseudonym→alias
  // because the two are semantically the same thing. excerpt gets a
  // sentinel so the user can tell at-a-glance these came from the
  // archive vs. an auto-parsed mention.
  const manualLinks = await getPool().query(`
    INSERT INTO all_blues_links (src_id, dst_id, kind, excerpt)
    SELECT lo.discogs_id,
           hi.discogs_id,
           CASE link.kind
             WHEN 'pseudonym' THEN 'alias'
             ELSE link.kind
           END,
           'From Blues Archive (manually curated)'
      FROM blues_artist_links link
      JOIN blues_artists lo ON lo.id = link.lo_id AND lo.discogs_id IS NOT NULL
      JOIN blues_artists hi ON hi.id = link.hi_id AND hi.discogs_id IS NOT NULL
     WHERE lo.discogs_id <> hi.discogs_id
    ON CONFLICT (src_id, dst_id, kind) DO NOTHING
  `);

  await getPool().query(
    `UPDATE all_blues_warm_state SET artists_queued = artists_queued + $1,
       links_inserted = links_inserted + $2,
       phase='fetch', last_tick_at=NOW() WHERE id=1`,
    [queued + (manualSeeds.rowCount ?? 0), manualLinks.rowCount ?? 0],
  );
  console.log(`[all-blues] collect: ${queued} from release_cache + ${manualSeeds.rowCount ?? 0} manual seeds + ${manualLinks.rowCount ?? 0} manual edges`);
}

// ── Phase 2: fetch each pending artist, parse mentions ────────────

async function _runFetch(client: DiscogsClient): Promise<void> {
  console.log("[all-blues] fetch phase: draining queue");
  while (true) {
    if (_stopRequested) break;
    // Oldest blues seeds first. seed_year IS NULL means a row left
    // over from the pre-blues-filter codepath — process those last
    // (they may not even be blues; the edge gate below will drop
    // any mentions they emit to non-seeds).
    const next = await getPool().query(
      `SELECT discogs_id FROM all_blues_artist_queue
        WHERE status='pending'
        ORDER BY seed_year ASC NULLS LAST, discogs_id ASC
        LIMIT 1`,
    );
    if (!next.rows.length) {
      console.log("[all-blues] fetch: queue drained");
      break;
    }
    const discogsId: number = next.rows[0].discogs_id;
    try {
      // Skip if already cached recently
      const cached = await getPool().query(
        `SELECT 1 FROM discogs_artist_cache WHERE discogs_id=$1`,
        [discogsId],
      );
      let data: any;
      if (cached.rows.length) {
        const r = await getPool().query(
          `SELECT data FROM discogs_artist_cache WHERE discogs_id=$1`,
          [discogsId],
        );
        data = r.rows[0].data;
      } else {
        await _sleep(REQ_INTERVAL_MS);
        data = await _withRetry(`artist ${discogsId}`, () => client.getArtist(discogsId));
        await getPool().query(
          `INSERT INTO discogs_artist_cache (discogs_id, name, profile, data)
             VALUES ($1, $2, $3, $4)
           ON CONFLICT (discogs_id) DO UPDATE SET name=EXCLUDED.name, profile=EXCLUDED.profile,
             data=EXCLUDED.data, cached_at=NOW()`,
          [discogsId, data?.name ?? null, data?.profile ?? null, data],
        );
      }
      const profile: string = typeof data?.profile === "string" ? data.profile : "";
      let inserted = 0;
      if (profile) {
        const edges = _extractMentions(discogsId, profile);
        for (const e of edges) {
          try {
            // Edge gate: only insert if dst is itself a blues seed
            // (seed_year IS NOT NULL). This keeps the network inside
            // the blues universe — mentions to Dizzy Gillespie etc.
            // are dropped because he isn't a primary artist on any
            // cached blues release.
            const r = await getPool().query(
              `INSERT INTO all_blues_links (src_id, dst_id, kind, excerpt)
               SELECT $1, $2, $3, $4
                WHERE EXISTS (
                  SELECT 1 FROM all_blues_artist_queue
                   WHERE discogs_id=$2 AND seed_year IS NOT NULL
                )
               ON CONFLICT (src_id, dst_id, kind) DO NOTHING`,
              [e.src, e.dst, e.kind, e.excerpt],
            );
            if (r.rowCount) inserted++;
          } catch (linkErr) {
            // Skip any rows that violate CHECK (src <> dst) etc.
          }
        }
      }
      await getPool().query(
        `UPDATE all_blues_artist_queue SET status='done', fetched_at=NOW() WHERE discogs_id=$1`,
        [discogsId],
      );
      await getPool().query(
        `UPDATE all_blues_warm_state SET artists_fetched = artists_fetched + 1,
           links_inserted = links_inserted + $1, last_tick_at=NOW() WHERE id=1`,
        [inserted],
      );
    } catch (err: any) {
      const msg = err?.message ?? String(err);
      await getPool().query(
        `UPDATE all_blues_artist_queue SET status='error', error=$2, fetched_at=NOW() WHERE discogs_id=$1`,
        [discogsId, msg.slice(0, 500)],
      );
      await getPool().query(
        `UPDATE all_blues_warm_state SET artists_errored = artists_errored + 1,
           last_error=$1, last_tick_at=NOW() WHERE id=1`,
        [`artist ${discogsId}: ${msg.slice(0, 200)}`],
      );
      console.warn(`[all-blues] artist ${discogsId} failed: ${msg.slice(0, 200)}`);
      await _sleep(REQ_INTERVAL_MS);
    }
  }
}

// ── Mention extraction ─────────────────────────────────────────────
// Discogs profiles use [a123456] (or [a=name]) BBCode for artist
// references. We grab ~80 chars on either side as a "window" and
// classify the edge kind from keywords in that window.
//
// Priority (first match wins):
//   spouse  — wife/husband/married/spouse
//   family  — father/mother/son/daughter/brother/sister/uncle/aunt/
//             cousin/niece/nephew/family/relative
//   mentor  — mentor/taught/student of/teacher/tutored/learned from
//   band    — band/group/joined/member of/played with/formed/sideman
//   alias   — alias/aka/a.k.a./pseudonym/also known
//   mention — fallback

const KIND_PATTERNS: Array<[string, RegExp]> = [
  ["spouse", /\b(wife|husband|married|spouse)\b/i],
  ["family", /\b(father|mother|son|daughter|brother|sister|uncle|aunt|cousin|niece|nephew|family|relative|grandfather|grandmother|stepfather|stepmother)\b/i],
  ["mentor", /\b(mentor|taught|student of|teacher|tutored|learned from|protege|protégé|apprentice)\b/i],
  ["band",   /\b(band|group|joined|member of|played with|formed|sideman|backed|toured with|bandmate)\b/i],
  ["alias",  /\b(alias|aka|a\.k\.a\.|pseudonym|also known|recorded as)\b/i],
];

// Generic mention scanner — returns { dst, kind, excerpt } for every
// [aNNNNN] in the text. Caller picks a src and turns these into edges.
function _scanMentionTargets(text: string): Array<{ dst: number; kind: string; excerpt: string }> {
  const re = /\[a(?:=)?(\d+)(?:\|[^\]]*)?\]/gi;
  const out: Array<{ dst: number; kind: string; excerpt: string }> = [];
  const seenDst = new Set<number>();
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const dst = parseInt(m[1], 10);
    if (!Number.isFinite(dst) || dst <= 0) continue;
    if (seenDst.has(dst)) continue;
    seenDst.add(dst);
    const start = Math.max(0, m.index - 80);
    const end   = Math.min(text.length, m.index + m[0].length + 80);
    const window = text.slice(start, end);
    let kind = "mention";
    for (const [k, re2] of KIND_PATTERNS) {
      if (re2.test(window)) { kind = k; break; }
    }
    const excerpt = window.replace(/\s+/g, " ").trim().slice(0, 200);
    out.push({ dst, kind, excerpt });
  }
  return out;
}

function _extractMentions(srcId: number, profile: string): Array<{ src: number; dst: number; kind: string; excerpt: string | null }> {
  return _scanMentionTargets(profile)
    .filter(m => m.dst !== srcId)
    .map(m => ({ src: srcId, dst: m.dst, kind: m.kind, excerpt: m.excerpt }));
}

// ── Phase 1.5: scan blues release/master notes for mentions ───────
// Discogs release & master records carry `notes` text with the same
// BBCode-ish [aNNNNN] artist refs as the artist profile prose. Liner
// notes are gold for collaboration: "with [a123] on harmonica" etc.
// For each cached blues release/master in the year window:
//   • src artists = data->'artists' (the release's primary credits)
//   • dst artists = every [aNNNNN] found in data->'notes'
//   • emit one edge per (src, dst, kind) tuple, gated to blues seeds.
// No Discogs API calls — pure cache scan.
async function _runReleaseNotes(fromYear: number, toYear: number): Promise<void> {
  console.log(`[all-blues] release-notes phase: scanning blues release/master notes (${fromYear}-${toYear})`);
  await getPool().query(
    `UPDATE all_blues_warm_state SET phase='release-notes', last_tick_at=NOW() WHERE id=1`,
  );
  const r = await getPool().query(
    `SELECT rc.discogs_id, rc.type, rc.data
       FROM release_cache rc
      WHERE rc.type IN ('release', 'master')
        AND (rc.data->>'year') ~ '^[0-9]+$'
        AND (rc.data->>'year')::int BETWEEN $1 AND $2
        AND EXISTS (
          SELECT 1
            FROM jsonb_array_elements_text(COALESCE(rc.data->'genres', '[]'::jsonb)) AS g
           WHERE g = 'Blues'
        )
        AND COALESCE(TRIM(rc.data->>'notes'), '') <> ''`,
    [fromYear, toYear],
  );
  const total = r.rows.length;
  let releasesScanned = 0;
  let edgesInserted = 0;
  let edgesSinceLastTick = 0;
  let lastTickAt = Date.now();
  const TICK_EVERY_MS = 2000; // flush progress to DB at most every 2s
  // Initial tick — peg last_tick_at so the panel reflects the phase
  // having started even before the first insert lands.
  await getPool().query(
    `UPDATE all_blues_warm_state SET last_tick_at=NOW() WHERE id=1`,
  );
  console.log(`[all-blues] release-notes: ${total} releases to scan`);
  for (const row of r.rows) {
    if (_stopRequested) break;
    const data: any = row.data || {};
    const notes: string = String(data.notes ?? "");
    if (!notes.trim()) continue;
    const primaries: any[] = Array.isArray(data.artists) ? data.artists : [];
    const srcIds: number[] = [];
    for (const a of primaries) {
      const id = Number(a?.id);
      if (Number.isFinite(id) && id > 0) srcIds.push(id);
    }
    if (!srcIds.length) continue;
    const mentions = _scanMentionTargets(notes);
    if (!mentions.length) continue;
    releasesScanned++;
    const releaseId = Number(row.discogs_id);
    for (const src of srcIds) {
      for (const mention of mentions) {
        if (mention.dst === src) continue;
        try {
          const ins = await getPool().query(
            `INSERT INTO all_blues_links (src_id, dst_id, kind, excerpt, release_ids)
             SELECT $1, $2, $3, $4, ARRAY[$5::int]
              WHERE EXISTS (
                SELECT 1 FROM all_blues_artist_queue
                 WHERE discogs_id=$1 AND seed_year IS NOT NULL
              )
                AND EXISTS (
                SELECT 1 FROM all_blues_artist_queue
                 WHERE discogs_id=$2 AND seed_year IS NOT NULL
              )
             ON CONFLICT (src_id, dst_id, kind) DO UPDATE SET
               release_ids = ARRAY(
                 SELECT DISTINCT u FROM unnest(
                   all_blues_links.release_ids || EXCLUDED.release_ids
                 ) AS u
               )`,
            [src, mention.dst, mention.kind, mention.excerpt, releaseId],
          );
          if (ins.rowCount) { edgesInserted++; edgesSinceLastTick++; }
        } catch { /* CHECK violations etc — skip */ }
      }
    }
    // Periodic progress flush — pegs last_tick_at + bumps the rolling
    // links_inserted counter so the admin panel reflects progress
    // without waiting for the whole phase to finish. Throttled to
    // every TICK_EVERY_MS so we don't double the DB load just to
    // refresh a counter.
    if (Date.now() - lastTickAt >= TICK_EVERY_MS) {
      await getPool().query(
        `UPDATE all_blues_warm_state SET last_tick_at=NOW(),
           links_inserted = links_inserted + $1 WHERE id=1`,
        [edgesSinceLastTick],
      );
      edgesSinceLastTick = 0;
      lastTickAt = Date.now();
    }
  }
  // Flush any remaining un-ticked edges.
  await getPool().query(
    `UPDATE all_blues_warm_state SET links_inserted = links_inserted + $1,
       last_tick_at=NOW() WHERE id=1`,
    [edgesSinceLastTick],
  );
  console.log(`[all-blues] release-notes: scanned ${releasesScanned}/${total} releases/masters, inserted ${edgesInserted} edges`);
}
