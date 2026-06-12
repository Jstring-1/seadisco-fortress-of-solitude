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
    // Restart-from-beginning semantics (NOT a wipe):
    //   • Every queue row gets flipped back to status='pending' so the
    //     fetch phase will re-process every seed from the top.
    //   • Edges stay PUT — new ones land via ON CONFLICT DO NOTHING /
    //     DO UPDATE, existing ones survive. The graph keeps building.
    //   • discogs_artist_cache stays intact (re-parses for free).
    //   • Counters reset so the stats panel reflects this run.
    // For a true wipe, use the Delete buttons in the admin panel.
    await getPool().query(
      `UPDATE all_blues_artist_queue
          SET status='pending', fetched_at=NULL, error=NULL`,
    );
    const queueSize = await getPool().query(`SELECT COUNT(*)::int AS n FROM all_blues_artist_queue`);
    const linksNow  = await getPool().query(`SELECT COUNT(*)::int AS n FROM all_blues_links`);
    await getPool().query(
      `UPDATE all_blues_warm_state SET artists_queued=$1, artists_fetched=0,
         artists_errored=0, links_inserted=$2, last_error=NULL WHERE id=1`,
      [queueSize.rows[0]?.n ?? 0, linksNow.rows[0]?.n ?? 0],
    );
  }

  (async () => {
    try {
      await _runCollect(fromYear, toYear);
      // Build the seed-name → id map AFTER collect so it covers every
      // primary credit harvested from release_cache AND every
      // blues_artists manual seed. Used by both subsequent phases to
      // resolve [a=Display Name] literal-name refs that have no
      // attached Discogs id.
      const nameToId = await _buildNameToIdMap();
      if (!_stopRequested) await _runReleaseNotes(fromYear, toYear, nameToId);
      if (!_stopRequested) await _runFetch(client, nameToId);
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
    primary_refs AS (
      -- Every artist listed in data.artists (the release's primary
      -- credits) — singers, bandleaders, the named-up-front folks.
      SELECT (a->>'id')::int AS aid,
             br.yr,
             a->>'name' AS name
        FROM blues_releases br
        CROSS JOIN LATERAL jsonb_array_elements(
          COALESCE(br.data->'artists', '[]'::jsonb)
        ) AS a
       WHERE (a->>'id') ~ '^[0-9]+$'
         AND (a->>'id')::int > 0
    ),
    sideman_refs AS (
      -- Every extraartist credited with a musical-performance role.
      -- Catches session players, sidemen, accompanists who only show
      -- up in extraartists, never as primaries. The role filter
      -- excludes producer / engineer / liner-notes / photography /
      -- design — those aren't musical collaboration.
      SELECT (a->>'id')::int AS aid,
             br.yr,
             a->>'name' AS name
        FROM blues_releases br
        CROSS JOIN LATERAL jsonb_array_elements(
          COALESCE(br.data->'extraartists', '[]'::jsonb)
        ) AS a
       WHERE (a->>'id') ~ '^[0-9]+$'
         AND (a->>'id')::int > 0
         AND (a->>'role') ~* '\\b(guitar|piano|harmonica|vocals?|voice|bass|drums|sax|saxophone|fiddle|violin|mandolin|banjo|harp|organ|trumpet|cornet|clarinet|trombone|percussion|harmony vocals?|backing vocals?|accompani(?:ed|ment|st)|kazoo|jug|washboard|tambourine|tuba|ukulele|accordion)\\b'
    ),
    artist_refs AS (
      SELECT aid,
             MIN(yr) AS first_year,
             -- earliest non-empty name we see for this artist across
             -- the union of primary + sideman appearances.
             (array_agg(name ORDER BY yr) FILTER (WHERE COALESCE(name,'') <> ''))[1] AS name
        FROM (
          SELECT aid, yr, name FROM primary_refs
          UNION ALL
          SELECT aid, yr, name FROM sideman_refs
        ) AS combined
       GROUP BY aid
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

async function _runFetch(client: DiscogsClient, nameToId: Map<string, number>): Promise<void> {
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
        const mentions = _scanMentions(profile, nameToId).filter(m => m.dst !== discogsId);
        for (const mention of mentions) {
          for (const kind of mention.kinds) {
            try {
              // Edge gate: only insert if dst is itself a blues seed
              // (seed_year IS NOT NULL). One INSERT per kind so
              // multi-tag mentions ("his wife and bandmate [a123]")
              // yield two distinct rows that the popup can surface
              // separately.
              const r = await getPool().query(
                `INSERT INTO all_blues_links (src_id, dst_id, kind, excerpt)
                 SELECT $1, $2, $3, $4
                  WHERE EXISTS (
                    SELECT 1 FROM all_blues_artist_queue
                     WHERE discogs_id=$2 AND seed_year IS NOT NULL
                  )
                 ON CONFLICT (src_id, dst_id, kind) DO NOTHING`,
                [discogsId, mention.dst, kind, mention.excerpt],
              );
              if (r.rowCount) inserted++;
            } catch (linkErr) {
              // Skip any rows that violate CHECK (src <> dst) etc.
            }
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
// Two ref forms surface in Discogs prose:
//   numeric: [a123456], [a=123], [a123|Display Name], [a=123|Display]
//   literal: [a=Display Name], [a=Display Name (3)]
// The literal form is used when the prose author didn't link to a
// Discogs id. We match literals against the harvested seed-name map
// (queue.name lowercased) so any name we know becomes an edge target.
//
// Each mention is scored against EVERY kind pattern, not just the
// first match — "his wife and bandmate [a123]" emits both spouse and
// band edges. If no pattern matches we fall back to 'mention'.

const KIND_PATTERNS: Array<[string, RegExp]> = [
  ["spouse", /\b(wife|husband|married|spouse|romantic partner|lover|fianc[eé]e?|widow|widower)\b/i],
  ["family", /\b(father|mother|son|daughter|brother|sister|uncle|aunt|cousin|niece|nephew|family|relative|grandfather|grandmother|stepfather|stepmother|child|parent|sibling|kin\b|sister\-in\-law|brother\-in\-law)\b/i],
  ["mentor", /\b(mentor|mentored|taught|student of|teacher|tutored|learned from|proteg[eé]|apprentice|influenced by|inspired by|trained by|schooled|coached|under the tutelage)\b/i],
  ["band",   /\b(band|group|joined|member of|played with|formed|sideman|backed|toured with|bandmate|collaborat(?:ed|or)|recorded with|duet|accompan(?:ied|ist)|played alongside|jammed|gigged|sang with)\b/i],
  ["alias",  /\b(alias|aka|a\.k\.a\.|pseudonym|also known|recorded as|performing as|stage name|using the name)\b/i],
  ["traveled", /\b(traveled with|travelled with|travelling with|traveling with|on the road with|tour(?:ed|ing) with|hoboed with|rambled with)\b/i],
];

// Pull a sentence-bounded context window around `idx` so kind-keyword
// matches don't bleed across unrelated sentences. Hard caps at ±300
// chars in either direction so a missing punctuation mark doesn't
// grab a whole bio. Falls back to the bounds when no sentence
// terminator is found within the cap.
function _sentenceWindow(text: string, idx: number, len: number): string {
  const CAP = 300;
  const lo = Math.max(0, idx - CAP);
  const hi = Math.min(text.length, idx + len + CAP);
  // Walk back from the match to find the previous . ! ? — accept \n
  // as a sentence boundary too (multi-paragraph bios).
  let start = lo;
  for (let i = idx - 1; i >= lo; i--) {
    const ch = text[i];
    if (ch === "." || ch === "!" || ch === "?" || ch === "\n") {
      start = i + 1;
      break;
    }
  }
  // Walk forward from the end of the match to find the next .!?\n.
  let end = hi;
  for (let i = idx + len; i < hi; i++) {
    const ch = text[i];
    if (ch === "." || ch === "!" || ch === "?" || ch === "\n") {
      end = i + 1;
      break;
    }
  }
  return text.slice(start, end).replace(/\s+/g, " ").trim();
}

// Classify a window against every KIND_PATTERN, returning every kind
// that matches. Fallback to ["mention"] if nothing keyword-hits so
// every edge has at least one kind tag.
function _classifyKinds(window: string): string[] {
  const out: string[] = [];
  for (const [k, re] of KIND_PATTERNS) {
    if (re.test(window)) out.push(k);
  }
  return out.length ? out : ["mention"];
}

// One-shot lookup of every seed with a known name, returning a
// case-insensitive name → discogs_id map. Used to resolve [a=Display
// Name] literal-name refs against the seed set.
async function _buildNameToIdMap(): Promise<Map<string, number>> {
  const map = new Map<string, number>();
  const r = await getPool().query(
    `SELECT q.discogs_id, COALESCE(c.name, q.name) AS name
       FROM all_blues_artist_queue q
       LEFT JOIN discogs_artist_cache c USING (discogs_id)
      WHERE q.seed_year IS NOT NULL AND COALESCE(c.name, q.name) IS NOT NULL`,
  );
  for (const row of r.rows) {
    const k = String(row.name).toLowerCase().trim();
    if (k) map.set(k, row.discogs_id);
  }
  console.log(`[all-blues] name→id map built: ${map.size} seeds`);
  return map;
}

// Musician-role keywords used to promote an extraartist credit into
// a band edge. Anyone credited with one of these roles on a primary's
// release is treated as a bandmate (provided they're also a blues
// seed). Roles outside this list (producer, engineer, photography,
// liner notes, etc.) are not — they don't imply musical collaboration.
const MUSICIAN_ROLE_RE = /\b(guitar|piano|harmonica|vocals?|voice|bass|drums|sax|saxophone|fiddle|violin|mandolin|banjo|harp|organ|trumpet|cornet|clarinet|trombone|percussion|harmony vocals?|backing vocals?|accompani(?:ed|ment|st)|kazoo|jug|washboard|tambourine|tuba|ukulele|accordion)\b/i;

// Scans `text` for both numeric and literal-name artist refs, returning
// one entry per dst with every kind keyword that matches its sentence-
// bounded context. nameToId resolves [a=Display Name] forms against
// the harvested seed set; literals that don't match any seed are
// dropped (we have no way to attribute them).
function _scanMentions(
  text: string,
  nameToId: Map<string, number>,
): Array<{ dst: number; kinds: string[]; excerpt: string }> {
  const out: Array<{ dst: number; kinds: string[]; excerpt: string }> = [];
  const seenDst = new Set<number>();
  const recordHit = (dst: number, idx: number, matchLen: number) => {
    if (!Number.isFinite(dst) || dst <= 0 || seenDst.has(dst)) return;
    seenDst.add(dst);
    const window = _sentenceWindow(text, idx, matchLen);
    const kinds = _classifyKinds(window);
    // Cap the stored excerpt at 600 chars — long enough to carry the
    // full surrounding sentence(s) the _sentenceWindow grabbed, but
    // still bounded so a pathological notes blob doesn't bloat the
    // links table. The popup renders the excerpt as-is.
    out.push({ dst, kinds, excerpt: window.slice(0, 600) });
  };
  // Numeric forms: [aNNN], [a=NNN], [aNNN|Display], [a=NNN|Display]
  const NUM_RE = /\[a(?:=)?(\d+)(?:\|[^\]]*)?\]/gi;
  let m: RegExpExecArray | null;
  while ((m = NUM_RE.exec(text)) !== null) {
    recordHit(parseInt(m[1], 10), m.index, m[0].length);
  }
  // Name forms: [a=Display Name], [a=Display Name (3)]. The negative
  // lookahead on \d skips numeric values already caught above.
  const NAME_RE = /\[a=([^\d\]][^\]|]*)(?:\|[^\]]*)?\]/gi;
  while ((m = NAME_RE.exec(text)) !== null) {
    const raw = m[1].trim().replace(/\s*\(\d+\)\s*$/, "");
    const dst = nameToId.get(raw.toLowerCase());
    if (!dst) continue;
    recordHit(dst, m.index, m[0].length);
  }
  return out;
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
async function _runReleaseNotes(fromYear: number, toYear: number, nameToId: Map<string, number>): Promise<void> {
  console.log(`[all-blues] release-notes phase: scanning blues releases (${fromYear}-${toYear})`);
  await getPool().query(
    `UPDATE all_blues_warm_state SET phase='release-notes', last_tick_at=NOW() WHERE id=1`,
  );
  // Drop the notes-only filter — we now scan every blues release for
  // both prose mentions AND extraartists musician credits. Releases
  // with no notes still contribute via the credit pass.
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
        )`,
    [fromYear, toYear],
  );
  const total = r.rows.length;
  let releasesScanned = 0;
  let edgesInserted = 0;
  let edgesSinceLastTick = 0;
  let lastTickAt = Date.now();
  const TICK_EVERY_MS = 2000;
  await getPool().query(
    `UPDATE all_blues_warm_state SET last_tick_at=NOW() WHERE id=1`,
  );
  console.log(`[all-blues] release-notes: ${total} releases to scan`);
  // Helper: insert one seed-gated edge with release_id appended,
  // dedupe-merging release_ids on conflict. Returns 1 if a row was
  // inserted, 0 if it conflicted/was skipped.
  const insertEdge = async (src: number, dst: number, kind: string, excerpt: string, releaseId: number) => {
    if (src === dst) return 0;
    try {
      const ins = await getPool().query(
        `INSERT INTO all_blues_links (src_id, dst_id, kind, excerpt, release_ids)
         SELECT $1, $2, $3, $4, ARRAY[$5::int]
          WHERE EXISTS (SELECT 1 FROM all_blues_artist_queue
                         WHERE discogs_id=$1 AND seed_year IS NOT NULL)
            AND EXISTS (SELECT 1 FROM all_blues_artist_queue
                         WHERE discogs_id=$2 AND seed_year IS NOT NULL)
         ON CONFLICT (src_id, dst_id, kind) DO UPDATE SET
           release_ids = ARRAY(
             SELECT DISTINCT u FROM unnest(
               all_blues_links.release_ids || EXCLUDED.release_ids
             ) AS u
           )`,
        [src, dst, kind, excerpt, releaseId],
      );
      return ins.rowCount ?? 0;
    } catch { return 0; }
  };
  for (const row of r.rows) {
    if (_stopRequested) break;
    const data: any = row.data || {};
    const primaries: any[] = Array.isArray(data.artists) ? data.artists : [];
    const srcIds: number[] = [];
    for (const a of primaries) {
      const id = Number(a?.id);
      if (Number.isFinite(id) && id > 0) srcIds.push(id);
    }
    if (!srcIds.length) continue;
    const releaseId = Number(row.discogs_id);
    let touched = false;
    // ── (a) prose-notes scan ────────────────────────────────────
    const notes: string = String(data.notes ?? "");
    if (notes.trim()) {
      const mentions = _scanMentions(notes, nameToId);
      for (const mention of mentions) {
        for (const src of srcIds) {
          for (const kind of mention.kinds) {
            const n = await insertEdge(src, mention.dst, kind, mention.excerpt, releaseId);
            if (n) { edgesInserted += n; edgesSinceLastTick += n; touched = true; }
          }
        }
      }
    }
    // ── (b) extraartists role scan ──────────────────────────────
    // Every extraartist credited with a musician role on this release
    // gets a band edge from each primary credit. Producer / liner-
    // notes / photography roles are skipped — only musical contrib.
    const extras: any[] = Array.isArray(data.extraartists) ? data.extraartists : [];
    for (const ea of extras) {
      const eaId = Number(ea?.id);
      const role = String(ea?.role ?? "");
      if (!Number.isFinite(eaId) || eaId <= 0) continue;
      if (!MUSICIAN_ROLE_RE.test(role)) continue;
      const excerpt = `Credited as: ${role}`.slice(0, 600);
      for (const src of srcIds) {
        const n = await insertEdge(src, eaId, "band", excerpt, releaseId);
        if (n) { edgesInserted += n; edgesSinceLastTick += n; touched = true; }
      }
    }
    if (touched) releasesScanned++;
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
  await getPool().query(
    `UPDATE all_blues_warm_state SET links_inserted = links_inserted + $1,
       last_tick_at=NOW() WHERE id=1`,
    [edgesSinceLastTick],
  );
  console.log(`[all-blues] release-notes: scanned ${releasesScanned}/${total} releases (prose + credits), inserted ${edgesInserted} edges`);
}
