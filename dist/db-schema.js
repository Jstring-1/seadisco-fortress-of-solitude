// ── Database schema & migrations ──────────────────────────────────────
// Extracted verbatim from db.ts: the entire initDb() bootstrap — ~70
// CREATE TABLE statements plus idempotent ALTER/DROP migrations and a few
// seed passes. Split out purely for manageability (db.ts had grown past
// 12k lines); the SQL and control flow are unchanged. Runs once at boot
// from search-api.ts.
//
// Depends only on getPool + the two app-setting helpers, imported from
// db.ts. This is a circular import (db.ts re-exports initDb from here),
// but it is runtime-safe: these symbols are only referenced INSIDE
// initDb(), which runs long after both modules have finished loading.
import { getPool, getAppSetting, setAppSetting } from "./db.js";
// Minimal CSV parser tuned for the keysandpositions/tunings.csv shape:
// header row + comma-separated fields with optional double-quoted
// values that may themselves contain commas. Quoted-quote escaping is
// "" → ". Newlines inside quoted fields aren't expected for this
// dataset, so the line splitter is naive. Good enough for our seed
// data; replace with a real parser if the source ever grows arbitrary.
function _parseTuningsCsv(raw) {
    const lines = raw.replace(/\r\n/g, "\n").split("\n");
    if (lines.length < 2)
        return [];
    // The file may start with one or more "noise" rows (the export sometimes
    // adds an empty first line of just commas) before the real header.
    // Find the header row by looking for "Artist" and "Title" tokens.
    let headerIdx = -1;
    let cols = [];
    for (let i = 0; i < Math.min(5, lines.length); i++) {
        const cells = _splitCsvLine(lines[i]).map(c => c.trim().toLowerCase());
        if (cells.includes("artist") && cells.includes("title")) {
            headerIdx = i;
            cols = cells;
            break;
        }
    }
    if (headerIdx < 0)
        return [];
    // Map header names → cell indices so the parser doesn't break if the
    // column order shifts again. Both the original layout
    //   Artist,Track,Title,Position,Pitch,Notes
    // and the current one
    //   #,Artist,Title,Position,Pitch,Notes
    // are accepted without code changes.
    const idx = (name) => cols.indexOf(name);
    const iArtist = idx("artist");
    const iTitle = idx("title");
    const iPosition = idx("position");
    const iPitch = idx("pitch");
    const iNotes = idx("notes");
    // "Track" column is named differently across exports. The "#" column
    // in the newer file is a row index that effectively serves as the
    // track number per artist; treat both as the track identifier.
    let iTrack = idx("track");
    if (iTrack < 0)
        iTrack = idx("#");
    const rows = [];
    for (let i = headerIdx + 1; i < lines.length; i++) {
        const line = lines[i];
        if (!line || !line.trim())
            continue;
        const cells = _splitCsvLine(line);
        const artist = iArtist >= 0 ? (cells[iArtist] ?? "").trim() : "";
        const title = iTitle >= 0 ? (cells[iTitle] ?? "").trim() : "";
        // Skip noise rows (commas only / no artist). A row without an
        // artist is meaningless for our schema.
        if (!artist && !title)
            continue;
        rows.push({
            artist,
            track: iTrack >= 0 ? (cells[iTrack] ?? "").trim() : "",
            title,
            position: iPosition >= 0 ? (cells[iPosition] ?? "").trim() : "",
            pitch: iPitch >= 0 ? (cells[iPitch] ?? "").trim() : "",
            notes: iNotes >= 0 ? (cells[iNotes] ?? "").trim() : "",
        });
    }
    return rows;
}
function _splitCsvLine(line) {
    const out = [];
    let cur = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (inQuotes) {
            if (ch === '"' && line[i + 1] === '"') {
                cur += '"';
                i++;
            }
            else if (ch === '"') {
                inQuotes = false;
            }
            else {
                cur += ch;
            }
        }
        else {
            if (ch === '"') {
                inQuotes = true;
            }
            else if (ch === ",") {
                out.push(cur);
                cur = "";
            }
            else {
                cur += ch;
            }
        }
    }
    out.push(cur);
    return out;
}
export async function initDb() {
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_tokens (
      clerk_user_id TEXT PRIMARY KEY,
      discogs_token TEXT NOT NULL,
      created_at    TIMESTAMPTZ DEFAULT NOW(),
      updated_at    TIMESTAMPTZ DEFAULT NOW()
    )
  `);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS feedback (
      id            SERIAL PRIMARY KEY,
      clerk_user_id TEXT NOT NULL,
      user_email    TEXT NOT NULL,
      message       TEXT NOT NULL,
      created_at    TIMESTAMPTZ DEFAULT NOW()
    )
  `);
    // Collection / wantlist columns on user_tokens
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS discogs_username TEXT`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS collection_synced_at TIMESTAMP`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS wantlist_synced_at TIMESTAMP`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS default_add_folder_id INTEGER DEFAULT 1`);
    // Folder support for collection items
    await getPool().query(`ALTER TABLE user_collection ADD COLUMN IF NOT EXISTS folder_id INTEGER DEFAULT 0`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_collection_folders (
      id            SERIAL PRIMARY KEY,
      clerk_user_id TEXT NOT NULL,
      folder_id     INTEGER NOT NULL,
      folder_name   TEXT NOT NULL,
      item_count    INTEGER DEFAULT 0,
      UNIQUE(clerk_user_id, folder_id)
    )
  `);
    // Extra collection fields — rating, notes, instance_id
    // NOTE: the CREATE TABLE IF NOT EXISTS user_collection runs further down, so
    // we wrap these ALTERs in IF EXISTS to avoid errors on a truly fresh install.
    // On fresh installs, the CREATE TABLE below will include these columns once
    // we also run the migration after that CREATE (see below).
    await getPool().query(`ALTER TABLE IF EXISTS user_collection ADD COLUMN IF NOT EXISTS rating INTEGER DEFAULT 0`);
    await getPool().query(`ALTER TABLE IF EXISTS user_collection ADD COLUMN IF NOT EXISTS instance_id INTEGER`);
    await getPool().query(`ALTER TABLE IF EXISTS user_collection ADD COLUMN IF NOT EXISTS notes JSONB`);
    // Extra wantlist fields — rating, notes
    await getPool().query(`ALTER TABLE user_wantlist ADD COLUMN IF NOT EXISTS rating INTEGER DEFAULT 0`);
    await getPool().query(`ALTER TABLE user_wantlist ADD COLUMN IF NOT EXISTS notes JSONB`);
    // Background sync progress tracking
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS sync_status TEXT DEFAULT 'idle'`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS sync_progress INTEGER DEFAULT 0`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS sync_total INTEGER DEFAULT 0`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS sync_error TEXT`);
    // OAuth 1.0a credential storage
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS auth_method TEXT DEFAULT 'pat'`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS oauth_access_token TEXT`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS oauth_access_secret TEXT`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS oauth_connected_at TIMESTAMPTZ`);
    // Discogs profile cache
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS discogs_user_id INTEGER`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS discogs_avatar_url TEXT`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS discogs_profile_data JSONB`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS discogs_curr_abbr TEXT`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS profile_synced_at TIMESTAMP`);
    // Activity tracking + hibernate
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS last_active_at TIMESTAMPTZ DEFAULT NOW()`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS hibernated_at TIMESTAMPTZ`);
    // Temporary table for OAuth request tokens (handshake flow)
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS oauth_request_tokens (
      token           TEXT PRIMARY KEY,
      token_secret    TEXT NOT NULL,
      clerk_user_id   TEXT NOT NULL,
      csrf_state      TEXT,
      created_at      TIMESTAMPTZ DEFAULT NOW()
    )
  `);
    await getPool().query(`ALTER TABLE oauth_request_tokens ADD COLUMN IF NOT EXISTS csrf_state TEXT`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_collection (
      id                 SERIAL PRIMARY KEY,
      clerk_user_id      TEXT NOT NULL,
      discogs_release_id INTEGER NOT NULL,
      data               JSONB NOT NULL,
      added_at           TIMESTAMP,
      synced_at          TIMESTAMP DEFAULT NOW()
    )
  `);
    // Ensure required columns exist (covers fresh installs where the earlier
    // IF EXISTS block was a no-op because the table didn't yet exist)
    await getPool().query(`ALTER TABLE user_collection ADD COLUMN IF NOT EXISTS folder_id INTEGER DEFAULT 0`);
    await getPool().query(`ALTER TABLE user_collection ADD COLUMN IF NOT EXISTS rating INTEGER DEFAULT 0`);
    await getPool().query(`ALTER TABLE user_collection ADD COLUMN IF NOT EXISTS instance_id INTEGER`);
    await getPool().query(`ALTER TABLE user_collection ADD COLUMN IF NOT EXISTS notes JSONB`);
    // Migration: switch user_collection uniqueness from (user, release_id) to
    // (user, instance_id) so users can store multiple copies of the same release.
    // Backfill NULL instance_ids with a synthetic negative value derived from
    // release_id (guaranteed unique per-user under the legacy constraint).
    try {
        await getPool().query(`UPDATE user_collection SET instance_id = -discogs_release_id WHERE instance_id IS NULL`);
        await getPool().query(`ALTER TABLE user_collection DROP CONSTRAINT IF EXISTS user_collection_clerk_user_id_discogs_release_id_key`);
        await getPool().query(`ALTER TABLE user_collection ADD CONSTRAINT user_collection_user_instance_key UNIQUE (clerk_user_id, instance_id)`);
    }
    catch (e) {
        // Constraint may already exist — ignore
    }
    // instance_id used to be INTEGER (int4, max 2,147,483,647). Discogs'
    // monotonically-increasing collection-instance IDs crossed that
    // boundary in May 2026 — first symptom was "value … is out of range
    // for type integer" on sync for users with newly-added items.
    // ALTER to BIGINT (int8) so the column can hold all current and
    // future IDs. Idempotent: ALTER TYPE is a no-op when the column is
    // already bigint.
    await getPool().query(`ALTER TABLE user_collection ALTER COLUMN instance_id TYPE BIGINT`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_wantlist (
      id                 SERIAL PRIMARY KEY,
      clerk_user_id      TEXT NOT NULL,
      discogs_release_id INTEGER NOT NULL,
      data               JSONB NOT NULL,
      added_at           TIMESTAMP,
      synced_at          TIMESTAMP DEFAULT NOW(),
      UNIQUE(clerk_user_id, discogs_release_id)
    )
  `);
    // ── User inventory (marketplace listings) ────────────────────────────────
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_inventory (
      id                 SERIAL PRIMARY KEY,
      clerk_user_id      TEXT NOT NULL,
      listing_id         BIGINT NOT NULL,
      discogs_release_id INTEGER,
      data               JSONB NOT NULL,
      status             TEXT DEFAULT 'For Sale',
      price_value        NUMERIC(10,2),
      price_currency     TEXT DEFAULT 'USD',
      condition          TEXT,
      sleeve_condition   TEXT,
      posted_at          TIMESTAMP,
      synced_at          TIMESTAMP DEFAULT NOW(),
      UNIQUE(clerk_user_id, listing_id)
    )
  `);
    await getPool().query(`ALTER TABLE user_inventory ALTER COLUMN listing_id TYPE BIGINT`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS inventory_synced_at TIMESTAMP`);
    // ── User lists (curated Discogs lists) ───────────────────────────────────
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_lists (
      id              SERIAL PRIMARY KEY,
      clerk_user_id   TEXT NOT NULL,
      list_id         INTEGER NOT NULL,
      name            TEXT,
      description     TEXT,
      item_count      INTEGER DEFAULT 0,
      is_public       BOOLEAN DEFAULT true,
      data            JSONB,
      synced_at       TIMESTAMP DEFAULT NOW(),
      UNIQUE(clerk_user_id, list_id)
    )
  `);
    // ── User list items (items inside each Discogs list) ─────────────────────
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_list_items (
      id              SERIAL PRIMARY KEY,
      clerk_user_id   TEXT NOT NULL,
      list_id         INTEGER NOT NULL,
      discogs_id      INTEGER NOT NULL,
      entity_type     TEXT DEFAULT 'release',
      comment         TEXT,
      data            JSONB,
      synced_at       TIMESTAMP DEFAULT NOW(),
      UNIQUE(clerk_user_id, list_id, discogs_id)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_list_items_user_idx ON user_list_items (clerk_user_id)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_list_items_release_idx ON user_list_items (clerk_user_id, discogs_id)`);
    // Composite indexes for hot lookup paths.
    // user_collection's primary unique key is (clerk_user_id, instance_id) so
    // lookups by (clerk_user_id, discogs_release_id) — used by the badge /
    // instance fetchers — need their own index.
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_collection_user_release_idx ON user_collection (clerk_user_id, discogs_release_id)`);
    // user_inventory's unique key is (clerk_user_id, listing_id); lookups by
    // (clerk_user_id, discogs_release_id) (getInventoryListingIdsByRelease)
    // would otherwise scan.
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_inventory_user_release_idx ON user_inventory (clerk_user_id, discogs_release_id)`);
    // ── Recent views (cross-device Recent strip on the search page) ─────────
    // Stores the last N releases/masters the user opened in the modal, so the
    // Recent strip survives browser clears and syncs between devices. Capped
    // to RECENT_VIEWS_MAX rows per user via a trim after each upsert.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_recent_views (
      clerk_user_id TEXT        NOT NULL,
      discogs_id    INTEGER     NOT NULL,
      entity_type   TEXT        NOT NULL DEFAULT 'release',
      data          JSONB       NOT NULL,
      opened_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, discogs_id, entity_type)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_recent_views_user_time_idx ON user_recent_views (clerk_user_id, opened_at DESC)`);
    // ── Library of Congress audio saves (LOC view) ───────────────────────────
    // User's saved LOC audio items so they can build a personal listening
    // list without going back through LOC search each time.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_loc_saves (
      clerk_user_id TEXT        NOT NULL,
      loc_id        TEXT        NOT NULL,   -- LOC's item URL (stable unique id)
      title         TEXT,
      stream_url    TEXT,                   -- primary playable audio URL
      data          JSONB       NOT NULL,   -- full card snapshot
      saved_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, loc_id)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_loc_saves_user_time_idx ON user_loc_saves (clerk_user_id, saved_at DESC)`);
    // ── Archive.org item saves (Archive view) ────────────────────────────────
    // Mirrors user_loc_saves — admins can bookmark items from the archive.org
    // collection (Aadam Jacobs live-show recordings) and revisit them in a
    // dedicated "Saved" tab on the archive page.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_archive_saves (
      clerk_user_id TEXT        NOT NULL,
      archive_id    TEXT        NOT NULL,   -- archive.org item identifier (slug)
      title         TEXT,
      stream_url    TEXT,                   -- primary playable audio URL (mp3 or hls)
      data          JSONB       NOT NULL,   -- full card snapshot (date/desc/itemUrl)
      saved_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, archive_id)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_archive_saves_user_time_idx ON user_archive_saves (clerk_user_id, saved_at DESC)`);
    // ── YouTube video saves (YouTube SPA "Saved" tab) ────────────────────────
    // Mirrors the LOC / Archive saves shape. Video ID is the canonical
    // Discogs-independent identifier; title / channel / thumbnail / data
    // are a snapshot for the Saved tab card render so re-running the
    // YouTube search isn't required to display the user's library.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_youtube_saves (
      clerk_user_id TEXT        NOT NULL,
      video_id      TEXT        NOT NULL,   -- YouTube videoId (11 chars)
      title         TEXT,
      channel       TEXT,
      thumbnail     TEXT,
      data          JSONB       NOT NULL,
      saved_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, video_id)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_youtube_saves_user_time_idx ON user_youtube_saves (clerk_user_id, saved_at DESC)`);
    // ── Crowd-sourced YouTube overrides for tracks Discogs missed ─────────────
    // When a Discogs release/master tracklist has a track with no `videos[]`
    // entry, signed-in users can suggest a YouTube video for it. The first
    // submission wins (no approval queue, by design — admin can delete).
    // Scope is master-level by default so a fix to one pressing surfaces
    // across every release of the same master; per-release overrides are
    // also supported by writing release_type='release' rows. The lookup
    // does master first, falls back to release.
    //
    // PK on (release_id, release_type, track_position) gives us "first
    // submission wins" via INSERT ... ON CONFLICT DO NOTHING.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS track_youtube_overrides (
      release_id     TEXT        NOT NULL,   -- Discogs master OR release id (string for forward-compat)
      release_type   TEXT        NOT NULL,   -- "master" | "release"
      track_position TEXT        NOT NULL,   -- e.g. "A1", "1", "2-4"; whatever Discogs returns
      track_title    TEXT,                   -- snapshot at submission time (for the admin tab)
      video_id       TEXT        NOT NULL,   -- 11-char YouTube videoId
      video_title    TEXT,                   -- snapshot at submission time
      submitted_by   TEXT        NOT NULL,   -- clerk_user_id of submitter
      submitted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (release_id, release_type, track_position)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS track_youtube_overrides_submitted_idx ON track_youtube_overrides (submitted_at DESC)`);
    // mode: 'gap' (crowd-sourced fill for tracks Discogs missed — the
    // original behavior), 'replace' (admin-set; wins over the Discogs
    // videos[] match on the client), 'block' (admin hid a wrong Discogs
    // match; video_id is '' and the track renders as missing). A user
    // suggestion landing on a 'block' slot upgrades it to 'replace' so
    // the new video also beats the known-wrong Discogs match.
    await getPool().query(`ALTER TABLE track_youtube_overrides ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'gap'`);
    // ── Unavailable / broken YouTube videos ─────────────────────────────────
    // When the IFrame Player fires onError 100/101/150 (video removed,
    // embed disabled, or region-blocked) we record the videoId here.
    // After report_count crosses a threshold (2) the status flips to
    // 'unavailable' and the renderer treats every album track that
    // references that video as "missing" — counted in the heading and
    // contributable via the album-suggest popup.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS youtube_video_unavailable (
      video_id          TEXT PRIMARY KEY,
      status            TEXT NOT NULL DEFAULT 'flagged',
      report_count      INTEGER NOT NULL DEFAULT 1,
      first_reported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_reported_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      sample_user_id    TEXT,
      sample_error_code INTEGER
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS yt_video_unavailable_status_idx ON youtube_video_unavailable (status)`);
    // ── YT-match review queue (admin tab, v1) ──────────────────────────
    // Background worker walks earliest-year Blues masters and proposes
    // YouTube videos for tracks that have no override yet. v1 puts
    // every candidate into this queue for human review — no auto-accept.
    // Approving copies the row into track_youtube_overrides; rejecting
    // leaves a tombstone so the same video isn't re-proposed next run.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS track_yt_review_queue (
      id                          SERIAL PRIMARY KEY,
      master_id                   BIGINT NOT NULL,
      track_position              TEXT NOT NULL,
      track_title                 TEXT NOT NULL,
      track_artist                TEXT,
      master_year                 INTEGER,
      master_cover_url            TEXT,
      candidate_video_id          TEXT NOT NULL,
      candidate_title             TEXT,
      candidate_channel_title     TEXT,
      candidate_channel_id        TEXT,
      candidate_duration_seconds  INTEGER,
      candidate_thumbnail_url     TEXT,
      candidate_published_at      TIMESTAMPTZ,
      title_score                 REAL,
      duration_ok                 BOOLEAN,
      status                      TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','approved','rejected','skipped','superseded')),
      reviewed_at                 TIMESTAMPTZ,
      reviewed_by                 TEXT,
      created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS track_yt_review_queue_status_idx ON track_yt_review_queue (status, created_at)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS track_yt_review_queue_master_idx ON track_yt_review_queue (master_id, track_position)`);
    await getPool().query(`CREATE UNIQUE INDEX IF NOT EXISTS track_yt_review_queue_uniq_idx ON track_yt_review_queue (master_id, track_position, candidate_video_id)`);
    // v2 auto-approve: a candidate on an "Artist - Topic" channel whose
    // artist, title AND duration all match exactly is pinned without
    // human review. auto_reason records WHY a row was (or wasn't) taken
    // automatically so the decision is auditable after the fact.
    await getPool().query(`ALTER TABLE track_yt_review_queue ADD COLUMN IF NOT EXISTS is_topic_channel BOOLEAN`);
    await getPool().query(`ALTER TABLE track_yt_review_queue ADD COLUMN IF NOT EXISTS auto_reason TEXT`);
    await getPool().query(`ALTER TABLE track_yt_review_queue ADD COLUMN IF NOT EXISTS track_duration_seconds INTEGER`);
    // One-time cleanup (idempotent): an early auto-approve bug left
    // 'pending' straggler candidates for tracks that were already
    // auto-pinned — they showed up in the review queue tagged
    // 'track_already_auto_approved'. Collapse any pending row whose
    // (master, track) already has an approved row to 'superseded' so the
    // queue only ever shows genuinely undecided tracks.
    await getPool().query(`
    UPDATE track_yt_review_queue p
       SET status = 'superseded', reviewed_at = NOW(), reviewed_by = 'auto'
     WHERE p.status = 'pending'
       AND EXISTS (
         SELECT 1 FROM track_yt_review_queue a
          WHERE a.master_id = p.master_id
            AND a.track_position = p.track_position
            AND a.status = 'approved'
       )
  `);
    // Single-row state for the YT-review worker. id is pinned to 1 so
    // upserts and reads stay trivial.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS track_yt_review_state (
      id                INT PRIMARY KEY DEFAULT 1,
      running           BOOLEAN NOT NULL DEFAULT false,
      cursor_year       INT,
      cursor_master_id  BIGINT,
      cursor_track_pos  TEXT,
      total_searched    INT NOT NULL DEFAULT 0,
      total_queued      INT NOT NULL DEFAULT 0,
      total_skipped     INT NOT NULL DEFAULT 0,
      total_errors      INT NOT NULL DEFAULT 0,
      last_run_at       TIMESTAMPTZ,
      last_error        TEXT,
      message           TEXT
    )
  `);
    await getPool().query(`INSERT INTO track_yt_review_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING`);
    await getPool().query(`ALTER TABLE track_yt_review_state ADD COLUMN IF NOT EXISTS quota_date TEXT`);
    await getPool().query(`ALTER TABLE track_yt_review_state ADD COLUMN IF NOT EXISTS quota_worker_searches INT NOT NULL DEFAULT 0`);
    await getPool().query(`ALTER TABLE track_yt_review_state ADD COLUMN IF NOT EXISTS quota_project_units INT NOT NULL DEFAULT 0`);
    await getPool().query(`ALTER TABLE track_yt_review_state ADD COLUMN IF NOT EXISTS total_auto_approved INT NOT NULL DEFAULT 0`);
    // Which slice of Blues masters the walk is currently on. 'strict' =
    // sole-genre Blues; 'loose' = Blues among multiple genres. The worker
    // walks strict first, then loose, then cycles — see _runYtReviewWorker.
    await getPool().query(`ALTER TABLE track_yt_review_state ADD COLUMN IF NOT EXISTS walk_tier TEXT NOT NULL DEFAULT 'strict'`);
    // Per (master, track) search log so the worker can skip what it
    // already tried, and the admin can later trigger "retry tracks
    // that got 0 candidates" without re-walking every other track.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS track_yt_review_searched (
      master_id        BIGINT NOT NULL,
      track_position   TEXT NOT NULL,
      last_searched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      candidate_count  INT NOT NULL DEFAULT 0,
      source           TEXT NOT NULL DEFAULT 'album',
      PRIMARY KEY (master_id, track_position)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS track_yt_review_searched_at_idx ON track_yt_review_searched (last_searched_at)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS track_yt_review_searched_empty_idx ON track_yt_review_searched (candidate_count) WHERE candidate_count = 0`);
    // Persisted worker error log. One row per upstream failure so the
    // admin can drill into the Errors tile and see the exact reason
    // (HTTP 403 quotaExceeded, throw: ECONNRESET, etc.) without having
    // to dig through Railway logs.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS track_yt_review_errors (
      id          SERIAL PRIMARY KEY,
      ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      master_id   BIGINT,
      query       TEXT,
      reason      TEXT NOT NULL
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS track_yt_review_errors_ts_idx ON track_yt_review_errors (ts DESC)`);
    // Learned per-channel trust. Some curator channels (e.g. "Traveler
    // Into the Blue") post correct transfers consistently enough that a
    // title+artist match on them is as reliable as an official Topic
    // upload. Rather than hard-coding a list, derive it from the
    // admin's OWN approve/reject history — see refreshDerivedChannelTrust.
    //   source='derived' rows are recomputed from that history.
    //   source='manual'  rows are the admin's explicit trust/block and
    //                    are never touched by a refresh.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS yt_channel_trust (
      channel_id    TEXT PRIMARY KEY,
      channel_title TEXT,
      state         TEXT NOT NULL CHECK (state IN ('trusted','blocked')),
      source        TEXT NOT NULL DEFAULT 'derived' CHECK (source IN ('derived','manual')),
      approvals     INT NOT NULL DEFAULT 0,
      rejections    INT NOT NULL DEFAULT 0,
      updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS yt_channel_trust_state_idx ON yt_channel_trust (state)`);
    // Hard channel ban. Distinct from yt_channel_trust 'blocked' (which
    // only means "never auto-approve"): a banned channel is filtered out
    // of YouTube results ENTIRELY — live album-popup search AND the
    // background yt-review worker. Keyed on the YouTube channel id.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS yt_channel_bans (
      channel_id    TEXT PRIMARY KEY,
      channel_title TEXT,
      reason        TEXT,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    // ── YouTube search cache (DB-backed, survives Railway restarts) ─────────
    // The in-memory _ytSearchCache in search-api.ts gets wiped on every
    // deploy. With YT quota at 100 calls/day project-wide, even a few
    // deploys can burn the whole day's budget on otherwise-cached queries.
    // Mirror the same cache to a row here so a query that landed yesterday
    // is still free today.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS youtube_search_cache (
      cache_key  TEXT PRIMARY KEY,
      body       JSONB NOT NULL,
      cached_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS youtube_search_cache_age_idx ON youtube_search_cache (cached_at)`);
    // ── Discogs search cache ────────────────────────────────────────────
    // /search was a straight pass-through: every repeat query, every
    // page-back, every "load more" re-fetch paid the full ~1s rate-gate
    // wait plus upstream latency. Catalogue data barely moves, so a short
    // TTL buys a lot. Keyed on the full normalised param set, NOT per
    // user — the response depends only on the query, and sharing it
    // across users is the whole point.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS discogs_search_cache (
      cache_key  TEXT PRIMARY KEY,
      body       JSONB NOT NULL,
      cached_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS discogs_search_cache_age_idx ON discogs_search_cache (cached_at)`);
    // ── archive.org search cache ─────────────────────────────────────────
    // Long-TTL cache for archive.org's advancedsearch responses. Each
    // (q, page, rows) tuple gets its own row; once cached, repeat queries
    // serve from here instead of hitting archive.org. 90-day TTL since
    // archive search results are essentially stable for that horizon.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS archive_search_cache (
      cache_key  TEXT PRIMARY KEY,
      body       JSONB NOT NULL,
      cached_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS archive_search_cache_age_idx ON archive_search_cache (cached_at)`);
    // ── Project Gutenberg book cache (DB-as-cache, no separate proxy layer) ──
    // First-ever read of a book fetches the HTML body from gutenberg.org,
    // sanitizes, and stores it here. Every subsequent read serves directly
    // from this row — no upstream hop, no in-memory cache to manage.
    // Postgres TOAST handles the large `html` column transparently; a 2MB
    // book is just a 2MB row. Metadata (title, authors, etc.) is duplicated
    // out of the JSONB blob into top-level columns to keep saved-list /
    // search-display queries fast without re-parsing JSON.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS gutenberg_books (
      book_id          INTEGER     PRIMARY KEY,    -- Gutenberg etext id
      title            TEXT,
      authors          JSONB,                      -- [{name, birth_year, death_year}, ...]
      languages        JSONB,                      -- ["en", ...]
      subjects         JSONB,                      -- ["topic", ...]
      html             TEXT,                       -- sanitized body
      plain_text       TEXT,                       -- optional, populated on first read
      byte_size        INTEGER,                    -- length(html), for stats
      metadata         JSONB,                      -- raw Gutendex row (forward-compat)
      fetched_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_accessed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS gutenberg_books_accessed_idx ON gutenberg_books (last_accessed_at DESC)`);
    // Per-user saved Gutenberg books — the "Library" tab on the
    // Gutenberg view. Just the membership pointer; rendered metadata
    // comes from gutenberg_books (joined on book_id).
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS gutenberg_saved (
      clerk_user_id TEXT        NOT NULL,
      book_id       INTEGER     NOT NULL,
      saved_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, book_id)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS gutenberg_saved_user_time_idx ON gutenberg_saved (clerk_user_id, saved_at DESC)`);
    // Per-user, per-book bookmarks. position_pct is the 0–100% scroll
    // position; position_anchor is an optional element id ("p123",
    // "chapter-iii", etc.) that the reader can jump to directly when
    // the HTML body provides stable anchors. label is user-supplied
    // (or auto-derived from nearby heading text on save).
    //
    // Special row: bookmark_kind='auto' is the auto-resume position,
    // one per (user, book). 'manual' rows are user-pinned bookmarks,
    // many per (user, book). Composite unique constraint enforces the
    // singleton on auto.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS gutenberg_bookmarks (
      id              SERIAL      PRIMARY KEY,
      clerk_user_id   TEXT        NOT NULL,
      book_id         INTEGER     NOT NULL,
      bookmark_kind   TEXT        NOT NULL DEFAULT 'manual',  -- 'manual' | 'auto'
      position_pct    REAL        NOT NULL DEFAULT 0,
      position_anchor TEXT,
      label           TEXT,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS gutenberg_bookmarks_user_book_idx ON gutenberg_bookmarks (clerk_user_id, book_id, created_at DESC)`);
    await getPool().query(`CREATE UNIQUE INDEX IF NOT EXISTS gutenberg_bookmarks_auto_unique ON gutenberg_bookmarks (clerk_user_id, book_id) WHERE bookmark_kind = 'auto'`);
    // Admin-curated annotations linking book positions to Discogs
    // entities. Shared (not per-user): one annotation set everyone
    // sees. Two-way surface — the reader shows them in the sidebar
    // and as inline anchor markers; the artist/album/label popups
    // query the same table to surface "📖 Mentioned in books".
    // entity_id is nullable so name-only links (artist not in Discogs)
    // still work — those don't surface on the reverse side but still
    // render in the book.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS gutenberg_annotations (
      id              SERIAL      PRIMARY KEY,
      book_id         INTEGER     NOT NULL,
      position_pct    REAL        NOT NULL DEFAULT 0,
      position_anchor TEXT,
      entity_type     TEXT        NOT NULL,    -- 'artist'|'release'|'master'|'label'
      entity_id       BIGINT,                   -- Discogs id, nullable
      entity_name     TEXT        NOT NULL,
      snippet         TEXT,                     -- short quoted excerpt for the reverse-side card
      label           TEXT,                     -- admin context note
      created_by      TEXT        NOT NULL,     -- clerk_user_id of admin who created it
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    // Lookup by book (reader side): all annotations for the open book.
    await getPool().query(`CREATE INDEX IF NOT EXISTS gutenberg_annotations_book_idx ON gutenberg_annotations (book_id, position_pct ASC)`);
    // Lookup by entity (artist/album popup side): name-based fallback
    // when entity_id is null, id-based when known.
    await getPool().query(`CREATE INDEX IF NOT EXISTS gutenberg_annotations_entity_id_idx ON gutenberg_annotations (entity_type, entity_id) WHERE entity_id IS NOT NULL`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS gutenberg_annotations_entity_name_idx ON gutenberg_annotations (entity_type, lower(entity_name))`);
    // ── Per-user "banished" suggestions ──────────────────────────────────────
    // When a user dismisses a personal-suggestion card with the × button,
    // the (id,type) is recorded here and the background generator skips it
    // on every subsequent run. Banishments are permanent unless the user
    // clears them (no UI for that yet — manual DB op).
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_suggestion_dismissals (
      clerk_user_id TEXT        NOT NULL,
      discogs_id    INTEGER     NOT NULL,
      entity_type   TEXT        NOT NULL,    -- 'master' | 'release'
      dismissed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, discogs_id, entity_type)
    )
  `);
    // ── Per-user personal suggestions (background-generated) ─────────────────
    // A scheduled task computes a fresh batch of master/release suggestions
    // for each user once an hour: albums in the user's favorite genre/style
    // bands, recorded around the user's most-represented years, that the
    // user doesn't already own and that lack embedded YouTube videos. The
    // job overwrites the user's row each pass (no append history).
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_personal_suggestions (
      clerk_user_id TEXT        NOT NULL,
      discogs_id    INTEGER     NOT NULL,
      entity_type   TEXT        NOT NULL,    -- 'master' | 'release'
      score         REAL        NOT NULL DEFAULT 0,  -- ranking heuristic
      data          JSONB       NOT NULL,    -- card snapshot for render
      generated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, discogs_id, entity_type)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_personal_suggestions_user_idx ON user_personal_suggestions (clerk_user_id, generated_at DESC)`);
    // ── Per-user taste profile (Feed soft bias) ──────────────────────────
    // Cached top genres + styles per user, derived from their collection.
    // Used by the Feed sampler to nudge (not filter) toward cards matching
    // the user's existing taste. Recomputed lazily when computed_at is
    // older than 24h; collection edits don't force an immediate refresh.
    //
    // genre_scores / style_scores are JSONB maps from name → normalized
    // weight in [0, 1] (sum ≤ 1 per map). Feed uses them as a per-card
    // multiplier so a card matching your #1 genre gets a bigger boost
    // than one matching #10. top_genres / top_styles arrays are kept
    // for backwards compatibility with the flat-bias query path.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_taste_profile (
      clerk_user_id TEXT PRIMARY KEY,
      top_genres    TEXT[] NOT NULL DEFAULT '{}',
      top_styles    TEXT[] NOT NULL DEFAULT '{}',
      computed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`ALTER TABLE user_taste_profile ADD COLUMN IF NOT EXISTS genre_scores JSONB NOT NULL DEFAULT '{}'::jsonb`);
    await getPool().query(`ALTER TABLE user_taste_profile ADD COLUMN IF NOT EXISTS style_scores JSONB NOT NULL DEFAULT '{}'::jsonb`);
    // ── Catalog-tab pool cache (Feed/Rare/Dig/Active/Played) ─────────────
    // Heavy SQL (TABLESAMPLE+scoring, multi-genre EXISTS, GROUP BY across
    // all users) takes seconds per request. The pool worker runs each
    // mode's full query every ~2h and writes the top N (id, type, score)
    // tuples here; per-request the endpoint just samples this small
    // table. Score is the weight the per-request sampler reads for the
    // -LN(R)/score reservoir (raw open count, want count, etc.).
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS feed_cache_pool (
      mode         TEXT        NOT NULL,
      discogs_id   INTEGER     NOT NULL,
      entity_type  TEXT        NOT NULL,
      score        REAL        NOT NULL DEFAULT 1.0,
      refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (mode, discogs_id, entity_type)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS feed_cache_pool_mode_idx ON feed_cache_pool (mode, refreshed_at DESC)`);
    // ── Site-wide app settings (admin-controlled) ────────────────────────────
    // Simple key/value store for global config (theme, feature flags, etc.).
    // Currently used for the site-wide theme: admin picks a theme on /admin
    // and it applies to every visitor.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS app_settings (
      key        TEXT PRIMARY KEY,
      value      TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    // ── Per-user preferences ──────────────────────────────────────────────
    // Cross-device user prefs (currently: { offlineEnabled }). Stored as a
    // JSONB blob per user so we can extend with new keys without schema
    // migrations. Read on every page load to surface a one-time "cache on
    // this device too?" prompt when offlineEnabled is true server-side
    // but the device hasn't been opted in locally.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_preferences (
      clerk_user_id TEXT PRIMARY KEY,
      prefs         JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    // ── Wikipedia article saves (Wikipedia SPA "Saved" tab) ──────────────────
    // Mirrors user_loc_saves so users can bookmark articles without bouncing
    // through search again. Title is the canonical Wikipedia title (used to
    // re-fetch the article on click); the snippet/thumbnail/url fields are
    // a snapshot for the saved-tab card render.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_wiki_saves (
      clerk_user_id TEXT        NOT NULL,
      wiki_title    TEXT        NOT NULL,   -- canonical Wikipedia title
      wiki_url      TEXT,                   -- en.wikipedia.org/wiki/<Title>
      snippet       TEXT,                   -- HTML-stripped first paragraph
      thumbnail     TEXT,                   -- thumbnail image URL if any
      data          JSONB       NOT NULL,   -- additional snapshot fields
      saved_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, wiki_title)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_wiki_saves_user_time_idx ON user_wiki_saves (clerk_user_id, saved_at DESC)`);
    // ── Chronicling America (historic newspapers) saves ───────────────────
    // chronam_id is the canonical relative path returned by the API:
    // "/lccn/<lccn>/<date>/ed-X/seq-N/". Stable + unique per page; pairs
    // with a fixed URL prefix on render.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_chronam_saves (
      clerk_user_id TEXT        NOT NULL,
      chronam_id    TEXT        NOT NULL,
      paper_title   TEXT,
      issue_date    TEXT,
      snippet       TEXT,
      thumbnail     TEXT,
      data          JSONB       NOT NULL,
      saved_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, chronam_id)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_chronam_saves_user_time_idx ON user_chronam_saves (clerk_user_id, saved_at DESC)`);
    // Persistent search cache for Chronicling America. loc.gov's search
    // API is slow (10–20s common) — a shared DB cache means the first
    // user's wait warms it for everyone, and the cache survives Railway
    // restarts (vs the in-memory LRU which doesn't).
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS chronam_search_cache (
      cache_key TEXT        PRIMARY KEY,
      data      JSONB       NOT NULL,
      cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS chronam_search_cache_at_idx ON chronam_search_cache (cached_at)`);
    // ── User play queue (cross-source: LOC + YouTube) ───────────────────────
    // Items are ordered by `position` (1-indexed). Source is "loc" or "yt"
    // and `data` JSONB carries everything needed to play without a Discogs
    // round-trip: title, artist, image, plus engine-specific fields
    // (streamUrl/streamType for LOC; videoId/durationSec for YT).
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_play_queue (
      clerk_user_id TEXT        NOT NULL,
      position      INTEGER     NOT NULL,
      source        TEXT        NOT NULL,
      external_id   TEXT        NOT NULL,
      data          JSONB       NOT NULL,
      added_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, position)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_play_queue_user_idx ON user_play_queue (clerk_user_id, position)`);
    // ── User playlists ──────────────────────────────────────────────────────
    // Saved snapshots of a user's queue. Public-readable by id (so
    // playlists are shareable via /?pl=<id> URLs) but only the owner
    // can rename/delete. Items mirror the user_play_queue shape so
    // loading a playlist into the queue is a copy-paste job.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_playlists (
      id            SERIAL PRIMARY KEY,
      clerk_user_id TEXT        NOT NULL,
      name          TEXT        NOT NULL,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_playlists_user_idx ON user_playlists (clerk_user_id, updated_at DESC)`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_playlist_items (
      playlist_id   INTEGER     NOT NULL REFERENCES user_playlists(id) ON DELETE CASCADE,
      position      INTEGER     NOT NULL,
      source        TEXT        NOT NULL,
      external_id   TEXT        NOT NULL,
      data          JSONB       NOT NULL,
      PRIMARY KEY (playlist_id, position)
    )
  `);
    // ── User orders (marketplace buy/sell history) ──────────────────────────
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_orders (
      id              SERIAL PRIMARY KEY,
      clerk_user_id   TEXT NOT NULL,
      order_id        TEXT NOT NULL,
      status          TEXT,
      buyer_username  TEXT,
      seller_username TEXT,
      total_value     NUMERIC(10,2),
      total_currency  TEXT DEFAULT 'USD',
      item_count      INTEGER DEFAULT 0,
      created_at      TIMESTAMPTZ,
      data            JSONB,
      synced_at       TIMESTAMP DEFAULT NOW(),
      UNIQUE(clerk_user_id, order_id)
    )
  `);
    // Discogs order IDs are strings like "username-NNN"; widen if existing column is numeric
    await getPool().query(`ALTER TABLE user_orders ALTER COLUMN order_id TYPE TEXT`);
    await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS orders_synced_at TIMESTAMP`);
    await getPool().query(`ALTER TABLE user_orders ADD COLUMN IF NOT EXISTS viewed_at TIMESTAMPTZ`);
    // ── Order messages (per-order thread, fetched on demand) ─────────────────
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_order_messages (
      id            SERIAL PRIMARY KEY,
      clerk_user_id TEXT NOT NULL,
      order_id      TEXT NOT NULL,
      message_order INTEGER NOT NULL,
      subject       TEXT,
      message       TEXT,
      from_user     TEXT,
      ts            TIMESTAMPTZ,
      data          JSONB,
      synced_at     TIMESTAMP DEFAULT NOW(),
      UNIQUE(clerk_user_id, order_id, message_order)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_order_messages_order_idx ON user_order_messages (clerk_user_id, order_id)`);
    // ── API request log (errors + successes for all external API calls) ────
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS api_request_log (
      id            SERIAL PRIMARY KEY,
      service       TEXT NOT NULL,
      endpoint      TEXT NOT NULL,
      method        TEXT DEFAULT 'GET',
      status_code   INTEGER,
      success       BOOLEAN NOT NULL,
      duration_ms   INTEGER,
      error_message TEXT,
      context       TEXT,
      created_at    TIMESTAMPTZ DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS api_request_log_created_idx ON api_request_log (created_at DESC)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS api_request_log_service_idx ON api_request_log (service, created_at DESC)`);
    // ── Release cache (full Discogs release/master detail saved on user click) ─
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS release_cache (
      discogs_id      INTEGER NOT NULL,
      type            TEXT NOT NULL DEFAULT 'release',
      data            JSONB NOT NULL,
      cached_at       TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(discogs_id, type)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_cache_id_type_idx ON release_cache (discogs_id, type)`);
    // Powers the admin "Cache write rate" card: range counts by cached_at
    // (last 1h / 24h / 7d) + the hourly GROUP BY use this btree instead of
    // sequential-scanning the whole table on every 30s poll.
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_cache_cached_at_idx ON release_cache (cached_at)`);
    // Negative cache — Discogs ids that returned 404 (deleted / merged,
    // or mis-typed by the search index: a type=master search combined
    // with a pressing-level facet like format/country/label hands back
    // PRESSING ids still tagged "master", which then 404 on /masters/{id}).
    // Recorded here so a mature sweep stops re-spending its ~1/sec Discogs
    // budget re-fetching the same known-dead ids on every pass.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS discogs_dead_ids (
      discogs_id  BIGINT      NOT NULL,
      type        TEXT        NOT NULL,
      first_seen  TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (discogs_id, type)
    )
  `);
    // GIN indexes for the Constellations artist popup + collect SQL.
    // Without these, looking up every release credited to a given
    // discogs_id was a full table scan that expanded each row's
    // artists JSONB array. With the gin indexes we can use jsonb
    // containment (@>) and key-exists (?) which both leverage the
    // index for sub-millisecond lookups. Safe to add — idempotent
    // and only affects query plans, not data.
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_cache_data_artists_gin ON release_cache USING gin ((data->'artists'))`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_cache_data_extra_gin ON release_cache USING gin ((data->'extraartists'))`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_cache_data_genres_gin ON release_cache USING gin ((data->'genres'))`);
    // MusicBrainz removed entirely — drop its cache + saves tables on boot
    // (idempotent; no-ops once already gone). The blues_artists.musicbrainz_mbid
    // column is dropped just after that table is (re)ensured, below.
    await getPool().query(`DROP TABLE IF EXISTS musicbrainz_cache`);
    await getPool().query(`DROP TABLE IF EXISTS musicbrainz_saves`);
    // seen_at: NULL = pre-warmed-only (cache-warm job pulled it but no
    // human has opened the modal yet). Set to NOW() on the first user
    // click. Feed queries filter WHERE seen_at IS NOT NULL so warmed-
    // but-unviewed entries don't pollute the public feed pool.
    //
    // Wrapped in a DO block so the backfill ONLY runs the first time
    // the column is added — every prior row was written via the user-
    // click path (no warm code existed yet), so all of them are
    // legitimately "engaged" and get stamped. After that initial pass,
    // NULL seen_at means "still warm-only" and we leave them alone.
    await getPool().query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = current_schema()
           AND table_name = 'release_cache'
           AND column_name = 'seen_at'
      ) THEN
        ALTER TABLE release_cache ADD COLUMN seen_at TIMESTAMPTZ;
        UPDATE release_cache SET seen_at = cached_at;
      END IF;
    END $$;
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_cache_seen_at_idx ON release_cache (seen_at) WHERE seen_at IS NOT NULL`);
    // GIN index on the genres array inside data — powers the Cache
    // panel's per-genre "in cache: N" counts and any future "browse
    // cached blues" features. Without it those COUNT(*) queries
    // sequential-scan the whole table; at 100k+ rows that's slow.
    // Uses default jsonb_ops opclass — required for the ? (text-
    // existence) operator the COUNT queries use. (jsonb_path_ops is
    // smaller but only supports @>, which doesn't apply here.)
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_cache_data_genres_idx
       ON release_cache USING GIN ((data->'genres'))`);
    // ── Retired: split cache (V2) ──────────────────────────────────────
    // release_cache (V1) is the single source of truth; the split schema
    // (discogs_cache_masters_plus / discogs_cache_pressings + the
    // release_labels / release_artists / release_tags side tables) was
    // dual-written but never read (reader flag stayed off), so it was pure
    // duplicate storage. Dropped here to reclaim it. isSplitCacheReaderEnabled()
    // is hard-wired false and the dual-write + projection worker are gone,
    // so nothing writes or reads these any more. Idempotent — no-ops once
    // already dropped. CASCADE covers the SERIAL sequences.
    await getPool().query(`DROP TABLE IF EXISTS discogs_cache_masters_plus CASCADE`);
    await getPool().query(`DROP TABLE IF EXISTS discogs_cache_pressings   CASCADE`);
    await getPool().query(`DROP TABLE IF EXISTS release_labels            CASCADE`);
    await getPool().query(`DROP TABLE IF EXISTS release_artists           CASCADE`);
    await getPool().query(`DROP TABLE IF EXISTS release_tags              CASCADE`);
    // ── Cache-fetch queue ───────────────────────────────────────────────
    // Generic backlog of "fetch this album from Discogs and cache it".
    // Multiple sources enqueue (suggestion generator, future hover-to-
    // prefetch, LOC backfill, crowd submissions, …) and a single rate-
    // limited worker drains it during the overnight window. Dedupe is
    // enforced by the unique (entity_type, discogs_id) constraint —
    // re-enqueueing a popular album just bumps its priority/source if
    // higher, no row spam.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS cache_fetch_queue (
      id            SERIAL      PRIMARY KEY,
      entity_type   TEXT        NOT NULL,    -- 'master' | 'release'
      discogs_id    INTEGER     NOT NULL,
      source        TEXT        NOT NULL DEFAULT 'unknown',
      priority      INTEGER     NOT NULL DEFAULT 0,
      requested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      attempts      INTEGER     NOT NULL DEFAULT 0,
      last_error    TEXT,
      UNIQUE(entity_type, discogs_id)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS cache_fetch_queue_drain_idx ON cache_fetch_queue (priority DESC, requested_at ASC)`);
    // ── Background-job run history (admin audit) ─────────────────────────
    // Durable, exact record of every scheduled-job invocation. In-memory
    // counters reset on redeploy; this survives so the admin panel can
    // show real last-run time + outcome.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS job_runs (
      id          SERIAL      PRIMARY KEY,
      job_name    TEXT        NOT NULL,
      status      TEXT        NOT NULL DEFAULT 'running',  -- 'running' | 'ok' | 'error'
      started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      ended_at    TIMESTAMPTZ,
      items       INTEGER     NOT NULL DEFAULT 0,
      errors      INTEGER     NOT NULL DEFAULT 0,
      detail      TEXT
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS job_runs_name_time_idx ON job_runs (job_name, started_at DESC)`);
    // ── Behavior events (admin "behavior stats" panel) ───────────────────
    // Two narrow append-only tables. Album-click counts already live in
    // user_recent_views and favorite counts in user_favorites, so we
    // only need new tables for the events that aren't otherwise logged
    // per-user: Discogs main-page searches and media-player track plays.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_search_events (
      id            SERIAL      PRIMARY KEY,
      clerk_user_id TEXT        NOT NULL,
      query         TEXT        NOT NULL DEFAULT '',
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_search_events_user_time_idx ON user_search_events (clerk_user_id, created_at DESC)`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_play_events (
      id            SERIAL      PRIMARY KEY,
      clerk_user_id TEXT        NOT NULL,
      source        TEXT        NOT NULL,    -- 'yt' | 'loc' | 'archive'
      external_id   TEXT        NOT NULL,
      title         TEXT,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_play_events_user_time_idx ON user_play_events (clerk_user_id, created_at DESC)`);
    // Discogs identity for play-derived taste suggestions. Nullable +
    // additive: LOC/Archive plays (and legacy rows) leave these NULL and
    // simply don't feed Discogs taste tuples. release_id/master_id let
    // the suggestions job resolve genre/style/year via release_cache;
    // play_meta is an optional client-sent snapshot ({genres,styles,year})
    // used directly when present so a play counts even before the release
    // is cached.
    await getPool().query(`ALTER TABLE user_play_events ADD COLUMN IF NOT EXISTS release_type TEXT`);
    await getPool().query(`ALTER TABLE user_play_events ADD COLUMN IF NOT EXISTS release_id   INTEGER`);
    await getPool().query(`ALTER TABLE user_play_events ADD COLUMN IF NOT EXISTS master_id    INTEGER`);
    await getPool().query(`ALTER TABLE user_play_events ADD COLUMN IF NOT EXISTS play_meta    JSONB`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_play_events_taste_idx ON user_play_events (clerk_user_id, created_at DESC) WHERE release_id IS NOT NULL`);
    // ── Phase 4: Price intelligence tables ──────────────────────────────────
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS price_cache (
      discogs_release_id  INTEGER NOT NULL,
      lowest_price        NUMERIC(10,2),
      median_price        NUMERIC(10,2),
      highest_price       NUMERIC(10,2),
      num_for_sale        INTEGER DEFAULT 0,
      currency            TEXT DEFAULT 'USD',
      fetched_at          TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(discogs_release_id, currency)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS price_cache_release_idx ON price_cache (discogs_release_id)`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS price_history (
      id                  SERIAL PRIMARY KEY,
      discogs_release_id  INTEGER NOT NULL,
      lowest_price        NUMERIC(10,2),
      median_price        NUMERIC(10,2),
      highest_price       NUMERIC(10,2),
      num_for_sale        INTEGER DEFAULT 0,
      currency            TEXT DEFAULT 'USD',
      recorded_at         TIMESTAMPTZ DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS price_history_release_date_idx ON price_history (discogs_release_id, recorded_at DESC)`);
    // Drop legacy alert tables that were planned but never wired up.
    // No reads, no writes anywhere in the code; safe to remove. FK
    // dependency: triggered_alerts → price_alerts, so child first.
    await getPool().query(`DROP TABLE IF EXISTS triggered_alerts`);
    await getPool().query(`DROP TABLE IF EXISTS price_alerts`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS saved_searches (
      id                  SERIAL PRIMARY KEY,
      clerk_user_id       TEXT NOT NULL,
      view                TEXT NOT NULL,
      label               TEXT NOT NULL,
      params              JSONB NOT NULL DEFAULT '{}',
      created_at          TIMESTAMPTZ DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS saved_searches_user_idx ON saved_searches (clerk_user_id, view)`);
    // ── Favorites ────────────────────────────────────────────────────────────
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_favorites (
      id                 SERIAL PRIMARY KEY,
      clerk_user_id      TEXT NOT NULL,
      discogs_id         INTEGER NOT NULL,
      entity_type        TEXT NOT NULL,
      data               JSONB NOT NULL,
      created_at         TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(clerk_user_id, discogs_id, entity_type)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS user_favorites_user_idx ON user_favorites (clerk_user_id, created_at DESC)`);
    // The legacy ai_recommendations table is no longer created. To drop the
    // existing data on a deployed instance, run manually:
    //   DROP TABLE IF EXISTS ai_recommendations CASCADE;
    // ── Pre-1930 blues artists database (admin-curated) ───────────────────
    // Seeded from Wikidata SPARQL; enriched manually + via future jobs from
    // Wikipedia, Discogs, YouTube. The wikidata_qid is the
    // canonical key — re-running the seeder upserts on it.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_artists (
      id                       SERIAL PRIMARY KEY,
      wikidata_qid             TEXT UNIQUE,
      discogs_id               INTEGER UNIQUE,
      name                     TEXT NOT NULL,
      aliases                  JSONB DEFAULT '[]'::jsonb,
      birth_date               TEXT,
      birth_place              TEXT,
      death_date               TEXT,
      death_place              TEXT,
      death_cause              TEXT,
      hometown_region          TEXT,
      first_recording_year     INTEGER,
      first_recording_title    TEXT,
      last_recording_year      INTEGER,
      last_recording_title     TEXT,
      associated_labels        JSONB DEFAULT '[]'::jsonb,
      styles                   JSONB DEFAULT '[]'::jsonb,
      instruments              JSONB DEFAULT '[]'::jsonb,
      songs_authored           JSONB DEFAULT '[]'::jsonb,
      collaborators            JSONB DEFAULT '[]'::jsonb,
      photo_url                TEXT,
      wikipedia_suffix         TEXT,
      youtube_urls             JSONB DEFAULT '[]'::jsonb,
      notes                    TEXT,
      enrichment_status        JSONB DEFAULT '{}'::jsonb,
      date_added               TIMESTAMPTZ DEFAULT NOW(),
      updated_at               TIMESTAMPTZ DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_artists_name_idx ON blues_artists (lower(name))`);
    // MusicBrainz removed — drop the now-unused mbid column (idempotent).
    await getPool().query(`ALTER TABLE blues_artists DROP COLUMN IF EXISTS musicbrainz_mbid`);
    // Phase-1.5 add-on: per-artist list of Discogs master/release IDs
    // (Masters+) discovered by the year-walk seeder. JSONB array of
    // { id, type:"master"|"release", title, year, label }.
    await getPool().query(`
    ALTER TABLE blues_artists
    ADD COLUMN IF NOT EXISTS discogs_releases JSONB DEFAULT '[]'::jsonb
  `);
    // Discogs artist /artists/:id payload includes a `urls` array — store
    // it for later cross-reference (often holds Wikipedia, AllMusic,
    // SecondHandSongs links etc.).
    await getPool().query(`
    ALTER TABLE blues_artists
    ADD COLUMN IF NOT EXISTS external_urls JSONB DEFAULT '[]'::jsonb
  `);
    // Count of cached MASTERS in release_cache where genres = ['Blues']
    // (exactly one genre, that genre is Blues) AND the artist appears as
    // a primary credit. Used to distinguish "this artist has an actual
    // blues album in our cache" from "this artist was added manually /
    // from lyrics / etc.".
    await getPool().query(`
    ALTER TABLE blues_artists
    ADD COLUMN IF NOT EXISTS seed_strict_count INT NOT NULL DEFAULT 0
  `);
    // Idempotent cleanup: remove Discogs placeholder artists that aren't
    // real people (Various=194, Unknown Artist=355). The strict-Blues pad
    // also excludes these going forward.
    await getPool().query(`DELETE FROM blues_artists WHERE discogs_id IN (194, 355)`);
    // ── Blues lyrics (scraped from weeniecampbell.com wiki, admin-only) ───
    // Source: weeniecampbell.com/wiki, Category:Lyrics (and subcategories).
    // page_title is the canonical wiki page title (unique per source host).
    // tuning extracted from page body via regex (Open D, Spanish, etc.)
    // so we can filter by it in the admin view.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_lyrics (
      id          SERIAL PRIMARY KEY,
      source_host TEXT        NOT NULL DEFAULT 'weeniecampbell.com',
      page_title  TEXT        NOT NULL,
      page_url    TEXT        NOT NULL,
      artist      TEXT,
      tuning      TEXT,
      wikitext    TEXT,
      plaintext   TEXT,
      scraped_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(source_host, page_title)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_lyrics_tuning_idx ON blues_lyrics (tuning)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_lyrics_artist_idx ON blues_lyrics (artist)`);
    // ── Phase: schema-level merge of lyrics with the blues_artists DB ───
    // artist_id is the canonical join. The free-text `artist` column is
    // retained as a fallback display value and to seed the FK on import,
    // but lookups everywhere should prefer artist_id when present.
    // ON DELETE SET NULL — deleting an artist orphans the lyric rather
    // than cascading destruction.
    await getPool().query(`
    ALTER TABLE blues_lyrics
    ADD COLUMN IF NOT EXISTS artist_id BIGINT REFERENCES blues_artists(id) ON DELETE SET NULL
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_lyrics_artist_id_idx ON blues_lyrics (artist_id)`);
    // Lyric-level pinning to a specific Discogs release / master. Lets
    // the per-track 📜 affordance in album modals know precisely which
    // lyric belongs to which release, instead of guessing by title +
    // artist alone.
    await getPool().query(`ALTER TABLE blues_lyrics ADD COLUMN IF NOT EXISTS discogs_release_id BIGINT`);
    await getPool().query(`ALTER TABLE blues_lyrics ADD COLUMN IF NOT EXISTS discogs_master_id BIGINT`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_lyrics_discogs_release_idx ON blues_lyrics (discogs_release_id)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_lyrics_discogs_master_idx  ON blues_lyrics (discogs_master_id)`);
    // updated_at on blues_lyrics — recent-edits feed needs it; existing
    // rows get NOW() once, then a trigger keeps it fresh.
    await getPool().query(`ALTER TABLE blues_lyrics ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`);
    // Shared trigger function that bumps updated_at to NOW() on every
    // row update. Applied to both blues_lyrics and blues_artists so the
    // recent-edits feed stays accurate without app-level wiring at every
    // mutation site. CREATE OR REPLACE so re-running the migration is
    // idempotent.
    await getPool().query(`
    CREATE OR REPLACE FUNCTION _blues_set_updated_at() RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = NOW();
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `);
    // DROP+CREATE pattern because CREATE TRIGGER doesn't have IF NOT
    // EXISTS in Postgres < 14 and would error on re-run otherwise.
    await getPool().query(`DROP TRIGGER IF EXISTS blues_lyrics_set_updated_at  ON blues_lyrics`);
    await getPool().query(`DROP TRIGGER IF EXISTS blues_artists_set_updated_at ON blues_artists`);
    await getPool().query(`
    CREATE TRIGGER blues_lyrics_set_updated_at
      BEFORE UPDATE ON blues_lyrics
      FOR EACH ROW EXECUTE FUNCTION _blues_set_updated_at();
  `);
    await getPool().query(`
    CREATE TRIGGER blues_artists_set_updated_at
      BEFORE UPDATE ON blues_artists
      FOR EACH ROW EXECUTE FUNCTION _blues_set_updated_at();
  `);
    // ── Manual cache-warm runs (per genre+style combo) ────────────────
    // The earlier nightly-rotation cron has been removed; runs are now
    // driven by an admin clicking Start with a genre and (optional)
    // style. This table holds one row per (genre, style) combination
    // the admin has ever run, tracking the cursor and cumulative
    // counters. style_key='' means "all-of-genre" (no style filter).
    // PK enforces single row per combo.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS cache_warm_runs (
      genre_key            TEXT NOT NULL,
      style_key            TEXT NOT NULL DEFAULT '',
      current_year         INT,
      current_page         INT NOT NULL DEFAULT 1,
      total_searched       INT NOT NULL DEFAULT 0,
      total_cached         INT NOT NULL DEFAULT 0,
      total_skipped        INT NOT NULL DEFAULT 0,
      total_errors         INT NOT NULL DEFAULT 0,
      last_run_at          TIMESTAMPTZ,
      last_cached_at       TIMESTAMPTZ,
      no_year_last_run_at  TIMESTAMPTZ,
      no_year_pages_seen   INT NOT NULL DEFAULT 0,
      recent_cached        JSONB NOT NULL DEFAULT '[]'::jsonb,
      recent_errors        JSONB NOT NULL DEFAULT '[]'::jsonb,
      PRIMARY KEY (genre_key, style_key)
    )
  `);
    // Migration for already-deployed envs that pre-date the no-year sweep
    // tracking. Independent from current_year / current_page so the dated
    // and no-year cursors don't clobber each other.
    await getPool().query(`ALTER TABLE cache_warm_runs ADD COLUMN IF NOT EXISTS no_year_last_run_at TIMESTAMPTZ`);
    await getPool().query(`ALTER TABLE cache_warm_runs ADD COLUMN IF NOT EXISTS no_year_pages_seen INT NOT NULL DEFAULT 0`);
    // One-time backfill: the pre-indicator no-year worker would land
    // its cursor at current_year=1 / current_page=1 (cursorYear=0 →
    // Math.min(year, endYear+1) → 1 after the first empty-page advance).
    // Stamp the no-year indicator for those rows and clear the bogus
    // dated cursor so the per-combo grid stops reading "1·p1" as a
    // dated position. Idempotent via the IS NULL gate on
    // no_year_last_run_at — runs once per row, never again.
    await getPool().query(`
    UPDATE cache_warm_runs
       SET no_year_last_run_at = COALESCE(last_run_at, NOW()),
           current_year        = NULL,
           current_page        = 1
     WHERE current_year = 1
       AND no_year_last_run_at IS NULL
  `);
    // ── Genre cache-warm cron state ──────────────────────────────────
    // One row per Discogs genre in the rotation. The nightly worker
    // picks today's genre by (dayOfYear % active.length) over rows
    // ordered by rotation_order, then walks that genre's cursor year
    // by year. Each genre has its own cursor + counters so progress
    // on each is independent. Idempotent: row inserts use ON CONFLICT.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS genre_cache_warm_state (
      genre_key           TEXT PRIMARY KEY,
      rotation_order      INT  NOT NULL,
      enabled             BOOLEAN NOT NULL DEFAULT true,
      manual_override     BOOLEAN NOT NULL DEFAULT false,
      start_year          INT NOT NULL DEFAULT 1900,
      -- end_year is a floor for the cron cap; the worker dynamically
      -- bumps it to max(stored, currentYear) so the sweep always
      -- extends through "this year" without yearly maintenance.
      end_year            INT NOT NULL DEFAULT 2100,
      current_year        INT NOT NULL DEFAULT 1900,
      current_page        INT NOT NULL DEFAULT 1,
      running             BOOLEAN NOT NULL DEFAULT false,
      started_at          TIMESTAMPTZ,
      last_tick_at        TIMESTAMPTZ,
      last_cached_at      TIMESTAMPTZ,
      lifetime_searched   INT NOT NULL DEFAULT 0,
      lifetime_cached     INT NOT NULL DEFAULT 0,
      lifetime_skipped    INT NOT NULL DEFAULT 0,
      lifetime_errors     INT NOT NULL DEFAULT 0,
      cycle_searched      INT NOT NULL DEFAULT 0,
      cycle_cached        INT NOT NULL DEFAULT 0,
      cycle_skipped       INT NOT NULL DEFAULT 0,
      cycle_started_at    TIMESTAMPTZ DEFAULT NOW(),
      cycle_count         INT NOT NULL DEFAULT 0,
      recent_errors       JSONB NOT NULL DEFAULT '[]'::jsonb,
      recent_cached       JSONB NOT NULL DEFAULT '[]'::jsonb
    )
  `);
    // Seed the rotation. ON CONFLICT preserves any admin-edited values
    // (rotation_order, start/end years, enabled, etc.) across re-runs.
    // Genre keys must match Discogs's exact genre strings — "Folk,
    // World, & Country" is one genre (not three), so commas + ampersand
    // are intentional. First five participate in the auto rotation;
    // the other ten get inserted disabled so the admin can manually
    // warm them via Start without them slotting into the nightly cycle.
    const _ROTATION = [
        [1, "Blues"],
        [2, "Folk, World, & Country"],
        [3, "Jazz"],
        [4, "Reggae"],
        [5, "Latin"],
    ];
    const _MANUAL_ONLY = [
        [10, "Rock"],
        [11, "Electronic"],
        [12, "Funk / Soul"],
        [13, "Pop"],
        [14, "Hip Hop"],
        [15, "Classical"],
        [16, "Stage & Screen"],
        [17, "Brass & Military"],
        [18, "Children's"],
        [19, "Non-Music"],
    ];
    for (const [order, genre] of _ROTATION) {
        await getPool().query(`INSERT INTO genre_cache_warm_state (genre_key, rotation_order)
       VALUES ($1, $2)
       ON CONFLICT (genre_key) DO NOTHING`, [genre, order]);
    }
    for (const [order, genre] of _MANUAL_ONLY) {
        await getPool().query(`INSERT INTO genre_cache_warm_state (genre_key, rotation_order, enabled)
       VALUES ($1, $2, false)
       ON CONFLICT (genre_key) DO NOTHING`, [genre, order]);
    }
    // One-time migration: rows seeded with end_year=1960 from the
    // earlier schema get bumped to 2100 so the worker walks all the
    // way through the modern era. Idempotent — no-op once the value
    // has been changed by hand or by a previous run.
    await getPool().query(`UPDATE genre_cache_warm_state SET end_year = 2100 WHERE end_year = 1960`);
    // ── Catalog-number cache-warm runs ───────────────────────────────
    // Sibling table to cache_warm_runs but keyed by label+catno range
    // instead of genre/style. The catno worker (src/cache-warm-catno.ts)
    // walks an inclusive [cat_lo, cat_hi] range for the given label,
    // hitting Discogs's /database/search with catno=N&label=Label, and
    // caches every matching release whose year is ≤ year_max (or has no
    // year). Cursor (current_catno) survives restarts so the worker
    // resumes from where it left off.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS cache_warm_catno_runs (
      series_key       TEXT PRIMARY KEY,
      label            TEXT NOT NULL,
      cat_lo           INT  NOT NULL,
      cat_hi           INT  NOT NULL,
      year_max         INT,
      current_catno    INT,
      total_searched   INT NOT NULL DEFAULT 0,
      total_cached     INT NOT NULL DEFAULT 0,
      total_skipped    INT NOT NULL DEFAULT 0,
      total_errors     INT NOT NULL DEFAULT 0,
      last_run_at      TIMESTAMPTZ,
      last_cached_at   TIMESTAMPTZ,
      recent_cached    JSONB NOT NULL DEFAULT '[]'::jsonb,
      recent_errors    JSONB NOT NULL DEFAULT '[]'::jsonb,
      created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    // Two-phase walker: after the catno range exhausts, transition to a
    // label-only sweep for every master + orphan release under the
    // label. `phase` is 'catno' | 'catno_done' | 'label_sweep_masters' |
    // 'label_sweep_orphans' | 'label_sweep_done'; `label_sweep_page`
    // tracks the current page within whichever label-sweep sub-pass is
    // active.
    await getPool().query(`ALTER TABLE cache_warm_catno_runs ADD COLUMN IF NOT EXISTS phase TEXT NOT NULL DEFAULT 'catno'`);
    await getPool().query(`ALTER TABLE cache_warm_catno_runs ADD COLUMN IF NOT EXISTS label_sweep_page INT`);
    // ── External discography rows ────────────────────────────────────
    // Canonical label-catalog data sourced from outside Discogs
    // (curated xlsx files, fan sites like wirz.de, etc.). The labels
    // carousel surfaces these as thin "stub" cards for catnos that
    // release_cache has no entry for — fills the gaps without
    // polluting release_cache with non-Discogs payloads.
    //
    // UNIQUE on (label_name, catno, side, source) so the same catno
    // can be present from multiple sources (we keep both and dedupe
    // visually by source priority on the read side).
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS external_discography (
      id           SERIAL PRIMARY KEY,
      label_name   TEXT NOT NULL,
      label_id     INT,
      catno        TEXT NOT NULL,
      catno_sort   NUMERIC,
      side         TEXT,
      artist       TEXT,
      title        TEXT,
      year         INT,
      matrix       TEXT,
      xref         TEXT,
      loc          TEXT,
      composer     TEXT,
      notes        TEXT,
      source       TEXT NOT NULL,
      data         JSONB,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (label_name, catno, side, source)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS idx_external_disc_label_sort ON external_discography(label_name, catno_sort)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS idx_external_disc_label_year ON external_discography(label_name, year)`);
    // ── Label upstream stats ────────────────────────────────────────
    // Discogs's total release count per label, fetched from
    // /labels/{id}/releases?per_page=1 (a single API call reveals the
    // `pagination.items` total). Lets the label directory show
    // priorities before sweeping. Fetched by label-upstream-stats-worker
    // and refreshed on a rolling window.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS label_upstream_stats (
      label_id       INTEGER PRIMARY KEY,
      total_releases INTEGER,
      fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS idx_label_upstream_fetched ON label_upstream_stats(fetched_at)`);
    // ── Label aliases ────────────────────────────────────────────────
    // Group multiple Discogs label IDs under one canonical for display
    // in the Label directory. Handles the "Excello / Excello (2) /
    // Excello Records" split-ID case and simple rename / reissue
    // successions. One row per alias — PK on alias_label_id ensures a
    // given Discogs ID can only be an alias of one canonical. Data
    // itself is left untouched; the collapse happens at read time.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS label_aliases (
      alias_label_id      INT PRIMARY KEY,
      canonical_label_id  INT NOT NULL,
      reason              TEXT,
      created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (alias_label_id <> canonical_label_id)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS idx_label_aliases_canonical ON label_aliases(canonical_label_id)`);
    // ── Year backfill removed ────────────────────────────────────────
    // The feature (guessing missing years by catalog number) was pulled.
    // One-time cleanup: strip every backfilled ("guessed") year — each
    // was tagged with data._year_backfilled_from and only ever filled a
    // row that had NO year, so removing the year key + marker restores
    // the correct year-less state. Then drop the audit table. Gated on an
    // app_setting so it runs exactly once.
    if (await getAppSetting("year_backfill_removed") !== "1") {
        await getPool().query(`UPDATE release_cache SET data = (data - 'year') - '_year_backfilled_from' WHERE data ? '_year_backfilled_from'`);
        // (Split-cache V2 UPDATEs removed — those tables were dropped above.)
        await getPool().query(`DROP TABLE IF EXISTS year_backfill_log`);
        await setAppSetting("year_backfill_removed", "1");
        console.log("[init] year-backfill feature removed; guessed years stripped");
    }
    // ── Pseudonym / band-member links between blues_artists rows ─────
    // Symmetric junction table — the same row covers both directions of
    // a link. We normalise (a_id, b_id) to (lo, hi) so a single row per
    // pair is enforced by the PK. `kind` is what kind of connection it
    // is: 'pseudonym' = same person under different recording name,
    // 'band' = played together in any group / sideman capacity. The
    // editor adds/removes; the artist popup renders chips on either side.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_artist_links (
      lo_id      INTEGER NOT NULL REFERENCES blues_artists(id) ON DELETE CASCADE,
      hi_id      INTEGER NOT NULL REFERENCES blues_artists(id) ON DELETE CASCADE,
      kind       TEXT    NOT NULL DEFAULT 'pseudonym',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (lo_id, hi_id),
      CHECK (lo_id < hi_id),
      CHECK (kind IN ('pseudonym', 'band'))
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_artist_links_hi_idx ON blues_artist_links (hi_id)`);
    // Allow multiple kinds per pair (Family AND Band, etc). PK widens
    // from (lo, hi) to (lo, hi, kind). Idempotent: only swaps when the
    // current PK is the old narrow shape. Existing rows survive — the
    // values are unique on (lo,hi,kind) by definition once we drop the
    // narrower constraint.
    await getPool().query(`
    DO $$
    DECLARE
      pk_cols text;
    BEGIN
      SELECT string_agg(att.attname, ',' ORDER BY att.attnum)
        INTO pk_cols
        FROM pg_constraint con
        JOIN pg_class      cls ON cls.oid = con.conrelid
        JOIN pg_attribute  att ON att.attrelid = cls.oid
                              AND att.attnum = ANY(con.conkey)
       WHERE cls.relname = 'blues_artist_links'
         AND con.contype = 'p';
      IF pk_cols = 'lo_id,hi_id' THEN
        ALTER TABLE blues_artist_links DROP CONSTRAINT blues_artist_links_pkey;
        ALTER TABLE blues_artist_links ADD PRIMARY KEY (lo_id, hi_id, kind);
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'blues_artist_links PK widening skipped: %', SQLERRM;
    END $$;
  `);
    // Expand the kind CHECK to include the broader relationship types
    // used by the Connections tab. We look up EVERY CHECK constraint on
    // the table whose definition references `kind` and drop them all —
    // the inline CHECK in CREATE TABLE gets an auto-generated name on
    // older deploys that doesn't necessarily match our preferred name,
    // so a blind DROP IF EXISTS on one name would leave the legacy
    // constraint in place alongside the new permissive one (and CHECK
    // constraints AND together — the strict one wins, blocking every
    // new kind from inserting). Idempotent and safe to run on every boot.
    await getPool().query(`
    DO $$
    DECLARE
      r record;
    BEGIN
      FOR r IN
        SELECT con.conname
          FROM pg_constraint con
          JOIN pg_class    cls ON cls.oid = con.conrelid
         WHERE cls.relname = 'blues_artist_links'
           AND con.contype = 'c'
           AND pg_get_constraintdef(con.oid) ILIKE '%kind%'
      LOOP
        EXECUTE format('ALTER TABLE blues_artist_links DROP CONSTRAINT %I', r.conname);
      END LOOP;
      ALTER TABLE blues_artist_links ADD CONSTRAINT blues_artist_links_kind_check
        CHECK (kind IN ('pseudonym', 'band', 'spouse', 'traveled', 'mentor', 'family'));
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'blues_artist_links kind CHECK migration skipped: %', SQLERRM;
    END $$;
  `);
    // ── All Blues: artist-profile cache + inferred link graph ─────────
    // Independent of blues_artists / blues_artist_links. Populated by a
    // background worker that walks release_cache (1900-1970), collects
    // every Discogs artist ID it sees, then fetches each artist's
    // /artists/:id profile and parses [aNNNNN] mentions out of the
    // profile prose. Edge kind is inferred from nearby keywords
    // (family / spouse / mentor / band / alias / mention).
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS discogs_artist_cache (
      discogs_id INTEGER PRIMARY KEY,
      name       TEXT,
      profile    TEXT,
      data       JSONB NOT NULL,
      cached_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS discogs_artist_cache_cached_at_idx ON discogs_artist_cache (cached_at DESC)`);
    // All Blues / Constellations feature removed — drop its tables (idempotent).
    await getPool().query(`DROP TABLE IF EXISTS all_blues_links`);
    await getPool().query(`DROP TABLE IF EXISTS all_blues_artist_queue`);
    await getPool().query(`DROP TABLE IF EXISTS all_blues_warm_state`);
    // ── Scrape ban list ──────────────────────────────────────────────
    // When the curator deletes an artist (or a single lyric) they can
    // mark it BANNED so the wiki rescrape doesn't immediately put it
    // back. Two kinds:
    //   'title'  — exact page_title; the discovery list filters these
    //              out before any fetch happens
    //   'artist' — case-insensitive artist name; we still fetch the
    //              page (we need the extracted-artist field), but the
    //              loop skips the upsert when the artist matches
    // Unique on (kind, lower-cased value) so adding an existing ban is
    // an idempotent no-op.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_lyrics_bans (
      id         SERIAL PRIMARY KEY,
      kind       TEXT NOT NULL CHECK (kind IN ('title', 'artist')),
      value      TEXT NOT NULL,
      reason     TEXT,
      banned_by  TEXT,
      banned_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE UNIQUE INDEX IF NOT EXISTS blues_lyrics_bans_uniq_idx
       ON blues_lyrics_bans (kind, LOWER(TRIM(value)))`);
    // Allow 'body_hash' as a third ban kind — fingerprints the exact
    // plaintext of a deleted lyric so a re-upload of the same body gets
    // skipped on rescrape, while a real edit (different text, same
    // title) comes through. Same pattern as the blues_artist_links
    // migration: drop every existing CHECK that references `kind`, then
    // add the permissive one.
    await getPool().query(`
    DO $$
    DECLARE
      r record;
    BEGIN
      FOR r IN
        SELECT con.conname
          FROM pg_constraint con
          JOIN pg_class    cls ON cls.oid = con.conrelid
         WHERE cls.relname = 'blues_lyrics_bans'
           AND con.contype = 'c'
           AND pg_get_constraintdef(con.oid) ILIKE '%kind%'
      LOOP
        EXECUTE format('ALTER TABLE blues_lyrics_bans DROP CONSTRAINT %I', r.conname);
      END LOOP;
      ALTER TABLE blues_lyrics_bans ADD CONSTRAINT blues_lyrics_bans_kind_check
        CHECK (kind IN ('title', 'artist', 'body_hash'));
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'blues_lyrics_bans kind CHECK migration skipped: %', SQLERRM;
    END $$;
  `);
    // ── Tunings grid (read-only) ───────────────────────────────────────
    // Per-track tuning + pitch table seeded from src/data/tunings.csv
    // (Weeniecampbell "keys and positions" research) the first time
    // this migration runs. Source-of-truth lives in the CSV — re-import
    // by truncating + re-seeding. Schema mirrors the CSV: artist, track,
    // title, position, pitch, notes.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_tunings_grid (
      id        SERIAL PRIMARY KEY,
      artist    TEXT NOT NULL,
      track     TEXT,
      title     TEXT NOT NULL,
      position  TEXT,
      pitch     TEXT,
      notes     TEXT
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_tunings_grid_artist_idx ON blues_tunings_grid (artist)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_tunings_grid_title_idx ON blues_tunings_grid (title)`);
    // Seed from the bundled CSV iff the table is empty OR the seed
    // version has changed. Bump _TUNINGS_SEED_VERSION whenever the CSV
    // is replaced so deployed environments truncate + re-seed instead
    // of clinging to the previous content. The version is stored in
    // app_settings under "tunings_seed_version".
    const _TUNINGS_SEED_VERSION = "2"; // bump on every CSV swap
    try {
        const countR = await getPool().query(`SELECT COUNT(*)::int AS n FROM blues_tunings_grid`);
        const existing = countR.rows[0]?.n ?? 0;
        let storedVersion = null;
        try {
            const vr = await getPool().query(`SELECT value FROM app_settings WHERE key = 'tunings_seed_version'`);
            storedVersion = vr.rows[0]?.value ?? null;
        }
        catch { /* app_settings might not exist on a brand-new DB; fall through */ }
        const needsReseed = existing === 0 || storedVersion !== _TUNINGS_SEED_VERSION;
        if (needsReseed) {
            if (existing > 0) {
                await getPool().query(`TRUNCATE blues_tunings_grid RESTART IDENTITY`);
                console.log(`[init] tunings seed version changed (${storedVersion} → ${_TUNINGS_SEED_VERSION}); truncated ${existing} rows`);
            }
            // Lazy require to avoid pulling fs into hot init paths when the
            // table is already populated. Path resolution: dist/db.js sits
            // at <repo>/dist/db.js so the CSV at <repo>/src/data/tunings.csv
            // is reached via "../src/data/tunings.csv" from the compiled file.
            const fs = await import("fs");
            const path = await import("path");
            const url = await import("url");
            // ESM-safe __dirname equivalent. dist/db.js → __dirname = dist/
            const here = path.dirname(url.fileURLToPath(import.meta.url));
            const candidates = [
                path.join(here, "..", "src", "data", "tunings.csv"),
                path.join(here, "..", "..", "src", "data", "tunings.csv"),
                path.join(process.cwd(), "src", "data", "tunings.csv"),
            ];
            let csvPath = "";
            for (const c of candidates) {
                if (fs.existsSync(c)) {
                    csvPath = c;
                    break;
                }
            }
            if (csvPath) {
                const raw = fs.readFileSync(csvPath, "utf8");
                const rows = _parseTuningsCsv(raw);
                if (rows.length) {
                    // Batch-insert via UNNEST so 1k+ rows go in one query.
                    await getPool().query(`INSERT INTO blues_tunings_grid (artist, track, title, position, pitch, notes)
             SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])`, [
                        rows.map(r => r.artist),
                        rows.map(r => r.track),
                        rows.map(r => r.title),
                        rows.map(r => r.position),
                        rows.map(r => r.pitch),
                        rows.map(r => r.notes),
                    ]);
                    console.log(`[init] seeded blues_tunings_grid with ${rows.length} rows from ${csvPath}`);
                    // Stamp the seed version so subsequent boots skip the
                    // re-seed unless the constant is bumped again.
                    try {
                        await getPool().query(`INSERT INTO app_settings (key, value)
               VALUES ('tunings_seed_version', $1)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, [_TUNINGS_SEED_VERSION]);
                    }
                    catch (e) {
                        console.warn("[init] tunings_seed_version stamp failed:", e);
                    }
                }
            }
            else {
                console.log("[init] tunings.csv not found in any candidate path; blues_tunings_grid left empty");
            }
        }
    }
    catch (e) {
        console.warn("[init] tunings seed failed (table left as-is):", e);
    }
    // One-shot backfill of artist_id for any rows missing it. Matches on
    // case-insensitive trim equality — same key the merge / import code
    // paths have always used. Idempotent: once populated, the WHERE
    // artist_id IS NULL clause is a no-op.
    await getPool().query(`
    UPDATE blues_lyrics l
       SET artist_id = a.id
      FROM blues_artists a
     WHERE l.artist_id IS NULL
       AND l.artist IS NOT NULL
       AND LOWER(TRIM(l.artist)) = LOWER(a.name)
  `);
    // Tuning normalization: bluesman slang and the modern canonical name
    // are the same physical tuning. Merge so the dropdown / breakdown
    // doesn't split equivalent rows.
    //   "Open G (Spanish)"     → "Open G"
    //   "Open D (Vestapol)"    → "Open D"
    //   "Cross Note"           → "Open Em (Cross Note)"
    // Idempotent (rerunning is a no-op once values are normalized).
    await getPool().query(`UPDATE blues_lyrics SET tuning = 'Open G' WHERE tuning = 'Open G (Spanish)'`);
    await getPool().query(`UPDATE blues_lyrics SET tuning = 'Open D' WHERE tuning = 'Open D (Vestapol)'`);
    await getPool().query(`UPDATE blues_lyrics SET tuning = 'Open Em (Cross Note)' WHERE tuning = 'Cross Note'`);
    // Allow the same page_title to appear under different artists. The
    // original UNIQUE(source_host, page_title) blocked manual adds of
    // covers (e.g. Robert Johnson's "Crossroads" and Eric Clapton's
    // "Crossroads"), even though they're different songs by different
    // performers. Drop the old constraint and replace with a partial
    // unique index that includes a normalized artist so the scraper's
    // re-run upsert still de-dupes, but a different artist with the
    // same title is allowed.
    //
    // The expression COALESCE(LOWER(TRIM(artist)), '') keeps NULL
    // artists in their own bucket (so a second NULL-artist scrape of
    // the same page still upserts cleanly).
    await getPool().query(`ALTER TABLE blues_lyrics DROP CONSTRAINT IF EXISTS blues_lyrics_source_host_page_title_key`);
    await getPool().query(`
    CREATE UNIQUE INDEX IF NOT EXISTS blues_lyrics_dedup_idx
      ON blues_lyrics (source_host, page_title, (COALESCE(LOWER(TRIM(artist)), '')))
  `);
    // ── Lyric first_release_year (chronological sort) ────────────────────
    // Year the song was first recorded/released. Resolved cheaply by
    // matching the lyric's page_title against blues_artists.discogs_releases
    // titles for the linked artist; falls back to manual entry. NULL until
    // resolved. Source enum lets the curator audit where each value came
    // from (e.g. tighten matches that were 'artist_releases' guesses).
    await getPool().query(`ALTER TABLE blues_lyrics ADD COLUMN IF NOT EXISTS first_release_year INTEGER`);
    await getPool().query(`ALTER TABLE blues_lyrics ADD COLUMN IF NOT EXISTS first_release_source TEXT`);
    await getPool().query(`ALTER TABLE blues_lyrics ADD COLUMN IF NOT EXISTS first_release_checked_at TIMESTAMPTZ`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_lyrics_first_release_year_idx ON blues_lyrics (first_release_year)`);
    // ── One-time scrub: strip the weeniecampbell.com footer ("Go to
    // [the] original forum thread") and any trailing junk that follows.
    // Regex tolerates a missing "the", flexible inter-word whitespace,
    // and optional trailing punctuation. (?is) = case-insensitive + dot
    // matches newlines, so the match consumes through end-of-text. \s*
    // before the marker also takes a trailing blank line so the cleaned
    // body doesn't end in whitespace. Idempotent — after the first run
    // no rows match the WHERE clause, so subsequent boots no-op.
    await getPool().query(`
    UPDATE blues_lyrics
       SET plaintext = regexp_replace(plaintext, '(?is)\\s*Go\\s+to\\s+(the\\s+)?original\\s+for[ua]m\\s+thread.*$', '')
     WHERE plaintext ~* 'original\\s+for[ua]m\\s+thread'
  `);
    // ── Blues Words lexicon (Stephen Calt-style dictionary) ─────────────
    // Headword → definition + one or more song-lyric citations. Seeded
    // by scripts/parse-blueswords.py → scripts/blueswords-*.json, ingested
    // via /api/admin/blues-words/ingest. Per-headword updated_at lets the
    // admin edit OCR-noisy entries inline. Citations have a stable
    // position so the admin UI can preserve ordering across edits.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_words (
      headword       TEXT PRIMARY KEY,
      definition     TEXT NOT NULL DEFAULT '',
      source_volume  TEXT,
      source_pages   INTEGER[],
      created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_words_letter_idx ON blues_words (LEFT(LOWER(headword), 1))`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_word_citations (
      id          SERIAL PRIMARY KEY,
      headword    TEXT NOT NULL REFERENCES blues_words(headword) ON DELETE CASCADE,
      position    INTEGER NOT NULL DEFAULT 1,
      quote       TEXT,
      artist      TEXT,
      song_title  TEXT,
      year        INTEGER,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_word_citations_headword_idx ON blues_word_citations (headword, position)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_word_citations_artist_idx ON blues_word_citations (LOWER(artist))`);
    // One-time wipe: the initial blues_words ingest came from an OCR
    // pass that produced too much noise to be useful. Schema is kept so
    // a fresh ingest from cleaner scans can refill the tables later;
    // app_settings flag keeps this from re-wiping a future re-import.
    try {
        const wiped = await getAppSetting("blues_words_wiped_2026_06_23");
        if (!wiped) {
            await getPool().query(`TRUNCATE blues_word_citations, blues_words CASCADE`);
            await setAppSetting("blues_words_wiped_2026_06_23", new Date().toISOString());
            console.log("[migrate] wiped blues_words + blues_word_citations (one-time, OCR ingest was unreliable)");
        }
    }
    catch (err) {
        console.warn("[migrate] blues_words wipe failed:", err);
    }
    // ── Lyric favorites + Setlists (admin curator tools) ─────────────────
    // Per-user favorites: (clerk_user_id, lyric_id) PK so a single user
    // can't double-favorite the same lyric. ON DELETE CASCADE on the
    // lyric FK so deleting a lyric cleans up its favorite rows.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_lyric_favorites (
      clerk_user_id TEXT       NOT NULL,
      lyric_id      INTEGER    NOT NULL REFERENCES blues_lyrics(id) ON DELETE CASCADE,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (clerk_user_id, lyric_id)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_lyric_favorites_user_idx ON blues_lyric_favorites (clerk_user_id)`);
    // Named setlists. One per name per user; deleting cascades to items.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_setlists (
      id            SERIAL PRIMARY KEY,
      clerk_user_id TEXT       NOT NULL,
      name          TEXT       NOT NULL,
      notes         TEXT,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_setlists_user_idx ON blues_setlists (clerk_user_id)`);
    await getPool().query(`CREATE UNIQUE INDEX IF NOT EXISTS blues_setlists_user_name_idx ON blues_setlists (clerk_user_id, LOWER(TRIM(name)))`);
    await getPool().query(`DROP TRIGGER IF EXISTS blues_setlists_set_updated_at ON blues_setlists`);
    await getPool().query(`
    CREATE TRIGGER blues_setlists_set_updated_at
      BEFORE UPDATE ON blues_setlists
      FOR EACH ROW EXECUTE FUNCTION _blues_set_updated_at();
  `);
    // Setlist items (lyrics in order). sort_order is a plain int; gaps
    // are fine, ties tie-break by lyric_id. Per-item note for things like
    // "open with this" or alternate tuning reminders.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_setlist_items (
      setlist_id INTEGER NOT NULL REFERENCES blues_setlists(id) ON DELETE CASCADE,
      lyric_id   INTEGER NOT NULL REFERENCES blues_lyrics(id)   ON DELETE CASCADE,
      sort_order INTEGER NOT NULL DEFAULT 0,
      note       TEXT,
      added_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (setlist_id, lyric_id)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS blues_setlist_items_setlist_idx ON blues_setlist_items (setlist_id, sort_order)`);
}
