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
    await getPool().query(`DELETE FROM all_blues_artist_queue`);
  }

  (async () => {
    try {
      await _runCollect(fromYear, toYear);
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

// ── Phase 1: collect artist IDs from cached releases ──────────────
// One bulk SQL pass. release_cache.data has Discogs's release JSON,
// where `artists` + `extraartists` are arrays of {id, name, ...}.
// We pull every id (>0) where the release's `year` falls in the
// requested window (defaults 1900-1970). ON CONFLICT keeps the
// queue idempotent so re-running just adds anything new.
async function _runCollect(fromYear: number, toYear: number): Promise<void> {
  console.log(`[all-blues] collect phase: years ${fromYear}-${toYear}`);
  const sql = `
    WITH ids AS (
      SELECT DISTINCT (a->>'id')::int AS aid
        FROM release_cache rc
        CROSS JOIN LATERAL jsonb_array_elements(
          COALESCE(rc.data->'artists',      '[]'::jsonb)
          ||
          COALESCE(rc.data->'extraartists', '[]'::jsonb)
        ) AS a
       WHERE (rc.data->>'year') ~ '^[0-9]+$'
         AND (rc.data->>'year')::int BETWEEN $1 AND $2
         AND (a->>'id') ~ '^[0-9]+$'
         AND (a->>'id')::int > 0
    )
    INSERT INTO all_blues_artist_queue (discogs_id)
    SELECT aid FROM ids
    ON CONFLICT (discogs_id) DO NOTHING
  `;
  const result = await getPool().query(sql, [fromYear, toYear]);
  const queued = result.rowCount ?? 0;
  // Also backfill: any artist referenced by an existing link but not
  // yet in our cache (so the graph stops showing "Artist NNNNNN" for
  // nodes parsed out of earlier passes' mentions). Idempotent — only
  // queues IDs we haven't already processed.
  const backfill = await getPool().query(`
    INSERT INTO all_blues_artist_queue (discogs_id)
    SELECT DISTINCT dst_id FROM all_blues_links
     WHERE dst_id NOT IN (SELECT discogs_id FROM discogs_artist_cache)
       AND dst_id NOT IN (SELECT discogs_id FROM all_blues_artist_queue)
    ON CONFLICT (discogs_id) DO NOTHING
  `);
  const backfilled = backfill.rowCount ?? 0;
  await getPool().query(
    `UPDATE all_blues_warm_state SET artists_queued = artists_queued + $1,
       phase='fetch', last_tick_at=NOW() WHERE id=1`,
    [queued + backfilled],
  );
  console.log(`[all-blues] collect: ${queued} from release_cache + ${backfilled} mention-backfill artists queued`);
}

// ── Phase 2: fetch each pending artist, parse mentions ────────────

async function _runFetch(client: DiscogsClient): Promise<void> {
  console.log("[all-blues] fetch phase: draining queue");
  while (true) {
    if (_stopRequested) break;
    const next = await getPool().query(
      `SELECT discogs_id FROM all_blues_artist_queue
        WHERE status='pending' ORDER BY discogs_id LIMIT 1`,
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
            const r = await getPool().query(
              `INSERT INTO all_blues_links (src_id, dst_id, kind, excerpt)
                 VALUES ($1, $2, $3, $4)
               ON CONFLICT (src_id, dst_id, kind) DO NOTHING`,
              [e.src, e.dst, e.kind, e.excerpt],
            );
            if (r.rowCount) inserted++;
          } catch (linkErr) {
            // Skip any rows that violate CHECK (src <> dst) etc.
          }
        }
        // Enqueue every mentioned artist we haven't seen so a future
        // pass fetches its profile + name. Without this, mentions that
        // point to artists not in any cached release stay nameless on
        // the graph ("Artist 274192"). Idempotent — ON CONFLICT skips
        // ones we already have.
        const mentionedIds = [...new Set(edges.map(e => e.dst))];
        if (mentionedIds.length) {
          const q = await getPool().query(
            `INSERT INTO all_blues_artist_queue (discogs_id)
             SELECT UNNEST($1::int[])
             ON CONFLICT (discogs_id) DO NOTHING`,
            [mentionedIds],
          );
          if (q.rowCount) {
            await getPool().query(
              `UPDATE all_blues_warm_state SET artists_queued = artists_queued + $1 WHERE id=1`,
              [q.rowCount],
            );
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

function _extractMentions(srcId: number, profile: string): Array<{ src: number; dst: number; kind: string; excerpt: string | null }> {
  // [aNNNNN] and [a=NNNNN] and [a123|Display Name]
  const re = /\[a(?:=)?(\d+)(?:\|[^\]]*)?\]/gi;
  const out: Array<{ src: number; dst: number; kind: string; excerpt: string | null }> = [];
  const seenPair = new Set<string>();
  let m: RegExpExecArray | null;
  while ((m = re.exec(profile)) !== null) {
    const dst = parseInt(m[1], 10);
    if (!Number.isFinite(dst) || dst <= 0 || dst === srcId) continue;
    const start = Math.max(0, m.index - 80);
    const end   = Math.min(profile.length, m.index + m[0].length + 80);
    const window = profile.slice(start, end);
    let kind = "mention";
    for (const [k, re2] of KIND_PATTERNS) {
      if (re2.test(window)) { kind = k; break; }
    }
    const key = `${dst}::${kind}`;
    if (seenPair.has(key)) continue;
    seenPair.add(key);
    // Excerpt: trim whitespace, single-line, max 200 chars
    const excerpt = window.replace(/\s+/g, " ").trim().slice(0, 200);
    out.push({ src: srcId, dst, kind, excerpt });
  }
  return out;
}
