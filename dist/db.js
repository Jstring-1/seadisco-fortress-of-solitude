import pg from "pg";
const { Pool } = pg;
import { expandWithSynonyms } from "./classical-synonyms.js";
let pool = null;
// Exported so feature modules (gutenberg endpoints in search-api.ts, etc.)
// can run ad-hoc queries without forcing every new feature to land a
// pile of helper functions in this file. Existing helpers stay in place
// for stable hot paths.
export function getPool() {
    if (!pool) {
        const connStr = process.env.APP_DB_URL;
        if (!connStr)
            throw new Error("APP_DB_URL not set");
        // Explicit pool sizing. max bumped down to 10 (from 20) because
        // Railway's Postgres connection cap is shared with sync workers,
        // admin tooling, the Railway dashboard, etc. — and a crash-loop
        // can pile up enough sockets in TIME_WAIT to push us past the
        // cap (Postgres SQLSTATE 53300 "sorry, too many clients already"
        // at boot, exactly what we just hit). 10 leaves plenty of
        // headroom even with multiple instances temporarily overlapping.
        // Overridable via env so we can raise it after upgrading the DB
        // tier or lower it further on smaller plans.
        const max = Number(process.env.APP_DB_POOL_MAX ?? 10);
        const min = Number(process.env.APP_DB_POOL_MIN ?? 2);
        const idle = Number(process.env.APP_DB_POOL_IDLE_MS ?? 30000);
        // 5s was too aggressive on Railway — when its Postgres is under
        // load or recovering from a restart, the first connect can take
        // 8–12s before the pool warms up. 15s gives a more forgiving
        // window so transient slowness doesn't surface as a flood of
        // user-visible 500s.
        const connTimeout = Number(process.env.APP_DB_POOL_CONN_MS ?? 15000);
        // statement_timeout enforced at connection startup via the libpq
        // `options` parameter. Previously set via pool.on("connect")
        // running an async SET — that race-conditioned with the pool
        // handing the client to a caller (triggering the
        // "client.query() ... already executing a query" deprecation
        // warning) and didn't guarantee the timeout was in place before
        // the first user query ran. Setting it through `options` applies
        // before the client is exposed.
        const stmtTimeoutMs = Number(process.env.APP_DB_STATEMENT_TIMEOUT_MS ?? 30000);
        pool = new Pool({
            connectionString: connStr,
            ssl: process.env.DB_CA_CERT
                ? { rejectUnauthorized: true, ca: process.env.DB_CA_CERT }
                : { rejectUnauthorized: false },
            max,
            min,
            idleTimeoutMillis: idle,
            connectionTimeoutMillis: connTimeout,
            options: `-c statement_timeout=${stmtTimeoutMs}`,
        });
        pool.on("error", (err) => {
            // Idle-client errors aren't fatal — log so we notice if Railway
            // is killing connections in the background.
            console.warn("[db pool] idle client error:", err?.message ?? err);
        });
    }
    return pool;
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
    // ── MusicBrainz cache ──────────────────────────────────────────────
    // Mirrors the release_cache shape, but keyed by (entity_type, key).
    // For entity lookups, `key` is the MBID. For search responses, it's
    // a sha256 of the query+params so identical searches collapse onto
    // one row. JSONB blob holds the upstream MB response verbatim.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS musicbrainz_cache (
      entity_type TEXT NOT NULL,
      key         TEXT NOT NULL,
      data        JSONB NOT NULL,
      cached_at   TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(entity_type, key)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS musicbrainz_cache_type_key_idx ON musicbrainz_cache (entity_type, key)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS musicbrainz_cache_cached_at_idx ON musicbrainz_cache (cached_at DESC)`);
    // ── MusicBrainz saves (per-user ★ bookmarks for the Saved tab) ────
    // entity_type is the MB type (artist / release / release-group /
    // recording / work / label); mbid is the canonical UUID. `meta`
    // carries a small snapshot (name, sort-name, disambiguation,
    // country, life-span / date) so the Saved list can render rows
    // without hitting the cache on first paint.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS musicbrainz_saves (
      id              SERIAL PRIMARY KEY,
      clerk_user_id   TEXT NOT NULL,
      entity_type     TEXT NOT NULL,
      mbid            TEXT NOT NULL,
      name            TEXT NOT NULL,
      meta            JSONB,
      saved_at        TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(clerk_user_id, entity_type, mbid)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS musicbrainz_saves_user_idx ON musicbrainz_saves (clerk_user_id, saved_at DESC)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS musicbrainz_saves_user_type_idx ON musicbrainz_saves (clerk_user_id, entity_type, saved_at DESC)`);
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
    // ── Split release_cache into masters+ / pressings + side tables ────
    // Migration target for the JSONB-heavy release_cache. Same source of
    // truth (data JSONB) but split by usage pattern:
    //   • discogs_cache_masters_plus  — masters AND orphan releases
    //     (releases without a master_id). This is what feed / cards /
    //     browse hit ~90% of the time, so it stays smaller and hotter.
    //   • discogs_cache_pressings     — releases WITH a master_id
    //     (specific pressings). Only read when a modal drills into one.
    // Both tables promote the four scalars we query on constantly
    // (year, country, master_id, primary_format) into indexed columns so
    // no `data->>'year'` JSON traversal is needed at read time. Full
    // Discogs blob stays in `data` for the modal / player.
    //
    // artist + master-versions cache types stay in the old release_cache
    // — they're small lookup caches with a different query pattern.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS discogs_cache_masters_plus (
      discogs_id     INTEGER      NOT NULL,
      type           TEXT         NOT NULL,     -- 'master' | 'release' (orphan only)
      year           SMALLINT,
      country        TEXT,
      primary_format TEXT,
      data           JSONB        NOT NULL,
      cached_at      TIMESTAMPTZ  DEFAULT NOW(),
      seen_at        TIMESTAMPTZ,
      UNIQUE (discogs_id, type)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS dcmp_year_idx      ON discogs_cache_masters_plus (year)      WHERE year IS NOT NULL`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS dcmp_country_idx   ON discogs_cache_masters_plus (country)   WHERE country IS NOT NULL`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS dcmp_seen_at_idx   ON discogs_cache_masters_plus (seen_at DESC) WHERE seen_at IS NOT NULL`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS dcmp_cached_at_idx ON discogs_cache_masters_plus (cached_at DESC)`);
    // Promoted scalar columns for the Feed / catalog samplers. Adding
    // them as nullable ints so pre-existing rows stay valid until the
    // reprojection pass fills them in; each is INT (not SMALLINT) since
    // community.want for very popular releases exceeds SMALLINT max.
    await getPool().query(`ALTER TABLE discogs_cache_masters_plus ADD COLUMN IF NOT EXISTS community_want  INT`);
    await getPool().query(`ALTER TABLE discogs_cache_masters_plus ADD COLUMN IF NOT EXISTS community_have  INT`);
    await getPool().query(`ALTER TABLE discogs_cache_masters_plus ADD COLUMN IF NOT EXISTS num_for_sale    INT`);
    await getPool().query(`ALTER TABLE discogs_cache_masters_plus ADD COLUMN IF NOT EXISTS videos_count    SMALLINT`);
    await getPool().query(`ALTER TABLE discogs_cache_masters_plus ADD COLUMN IF NOT EXISTS tracks_count    SMALLINT`);
    // Partial index tuned to the Rare-tab predicate (want>20 AND have<10
    // AND num_for_sale=0). Small — most rows don't qualify — but it
    // lets the Rare sampler skip a full table scan.
    await getPool().query(`CREATE INDEX IF NOT EXISTS dcmp_rare_idx
       ON discogs_cache_masters_plus (year, community_want DESC)
       WHERE community_want IS NOT NULL AND community_want > 20
         AND community_have IS NOT NULL AND community_have < 10
         AND num_for_sale = 0`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS discogs_cache_pressings (
      discogs_id     INTEGER      PRIMARY KEY,  -- always type='release'
      master_id      INTEGER      NOT NULL,
      year           SMALLINT,
      country        TEXT,
      primary_format TEXT,
      data           JSONB        NOT NULL,
      cached_at      TIMESTAMPTZ  DEFAULT NOW(),
      seen_at        TIMESTAMPTZ
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS dcp_master_id_idx  ON discogs_cache_pressings (master_id)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS dcp_year_idx       ON discogs_cache_pressings (year)     WHERE year IS NOT NULL`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS dcp_seen_at_idx    ON discogs_cache_pressings (seen_at DESC) WHERE seen_at IS NOT NULL`);
    // Same promoted-scalar shape as masters_plus (see comment there).
    await getPool().query(`ALTER TABLE discogs_cache_pressings ADD COLUMN IF NOT EXISTS community_want  INT`);
    await getPool().query(`ALTER TABLE discogs_cache_pressings ADD COLUMN IF NOT EXISTS community_have  INT`);
    await getPool().query(`ALTER TABLE discogs_cache_pressings ADD COLUMN IF NOT EXISTS num_for_sale    INT`);
    await getPool().query(`ALTER TABLE discogs_cache_pressings ADD COLUMN IF NOT EXISTS videos_count    SMALLINT`);
    await getPool().query(`ALTER TABLE discogs_cache_pressings ADD COLUMN IF NOT EXISTS tracks_count    SMALLINT`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS dcp_rare_idx
       ON discogs_cache_pressings (year, community_want DESC)
       WHERE community_want IS NOT NULL AND community_want > 20
         AND community_have IS NOT NULL AND community_have < 10
         AND num_for_sale = 0`);
    // Side tables. `bucket` is a small int denoting where the parent row
    // lives: 0 = master, 1 = orphan (release without master_id), 2 =
    // pressing (release with master_id). Baked in so aggregations don't
    // need to join back to the primary tables just to know which bucket
    // a row is in.
    //
    // Dedup is handled by the writer (DELETE-then-INSERT per (discogs_id,
    // bucket)) so we don't need a natural-key PRIMARY KEY here — a plain
    // SERIAL keeps schema simple and avoids expression-key limitations.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS release_labels (
      id          BIGSERIAL PRIMARY KEY,
      discogs_id  INTEGER   NOT NULL,
      bucket      SMALLINT  NOT NULL,
      label_id    INTEGER,
      label_name  TEXT      NOT NULL,
      catno       TEXT
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_labels_by_release_idx ON release_labels (discogs_id, bucket)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_labels_by_label_idx   ON release_labels (label_id) WHERE label_id IS NOT NULL`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_labels_name_lower_idx ON release_labels (LOWER(label_name))`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS release_artists (
      id          BIGSERIAL PRIMARY KEY,
      discogs_id  INTEGER   NOT NULL,
      bucket      SMALLINT  NOT NULL,
      artist_id   INTEGER   NOT NULL,
      role        TEXT      NOT NULL           -- 'main' | 'extra'
    )
  `);
    // Add name inline (mirrors release_labels.label_name) so the
    // cache-analytics V2 reader can filter + facet on artist name
    // without joining an out-of-band artist name table. Populated by
    // the projector; old rows written before this column stay NULL
    // until the backfill reprojects them.
    await getPool().query(`ALTER TABLE release_artists ADD COLUMN IF NOT EXISTS name TEXT`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_artists_by_release_idx ON release_artists (discogs_id, bucket)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_artists_by_artist_idx  ON release_artists (artist_id, role)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_artists_name_lower_idx ON release_artists (LOWER(name)) WHERE name IS NOT NULL`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS release_tags (
      id          BIGSERIAL PRIMARY KEY,
      discogs_id  INTEGER   NOT NULL,
      bucket      SMALLINT  NOT NULL,
      kind        TEXT      NOT NULL,          -- 'genre' | 'style' | 'format'
      value       TEXT      NOT NULL
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_tags_by_release_idx ON release_tags (discogs_id, bucket)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS release_tags_by_value_idx   ON release_tags (kind, value)`);
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
    // MusicBrainz, Wikipedia, Discogs, YouTube. The wikidata_qid is the
    // canonical key — re-running the seeder upserts on it.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS blues_artists (
      id                       SERIAL PRIMARY KEY,
      wikidata_qid             TEXT UNIQUE,
      musicbrainz_mbid         TEXT UNIQUE,
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
    // ── Year backfill audit log ──────────────────────────────────────
    // Every year written to release_cache.data->>'year' by the backfill
    // pass is logged here so the curator can roll back a whole batch if
    // a source turns out to be wrong. batch_id groups one Apply run;
    // rolled_back_at / rolled_back_batch_id flip when a row is reversed.
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS year_backfill_log (
      id                    SERIAL PRIMARY KEY,
      batch_id              UUID NOT NULL,
      discogs_id            BIGINT NOT NULL,
      type                  TEXT NOT NULL,
      old_year              INT,
      new_year              INT NOT NULL,
      donor_ref             TEXT NOT NULL,
      donor_source          TEXT,
      label_name            TEXT,
      catno                 TEXT,
      applied_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      rolled_back_at        TIMESTAMPTZ,
      rolled_back_batch_id  UUID,
      UNIQUE (batch_id, discogs_id, type)
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS idx_year_backfill_log_batch ON year_backfill_log(batch_id)`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS idx_year_backfill_log_target ON year_backfill_log(discogs_id, type)`);
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
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS all_blues_links (
      src_id     INTEGER NOT NULL,
      dst_id     INTEGER NOT NULL,
      kind       TEXT    NOT NULL DEFAULT 'mention',
      excerpt    TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (src_id, dst_id, kind),
      CHECK (src_id <> dst_id),
      CHECK (kind IN ('family', 'spouse', 'mentor', 'band', 'alias', 'mention'))
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS all_blues_links_dst_idx ON all_blues_links (dst_id)`);
    // release_ids: which release/master IDs surfaced this edge (release-
    // notes phase only — profile-prose mentions have no associated release
    // and stay as empty arrays). Each insert appends + dedupes so an edge
    // accumulates every blues release where the pair was credited together.
    await getPool().query(`ALTER TABLE all_blues_links ADD COLUMN IF NOT EXISTS release_ids INT[] NOT NULL DEFAULT '{}'`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS all_blues_artist_queue (
      discogs_id INTEGER PRIMARY KEY,
      status     TEXT NOT NULL DEFAULT 'pending',
      added_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      fetched_at TIMESTAMPTZ,
      error      TEXT
    )
  `);
    await getPool().query(`CREATE INDEX IF NOT EXISTS all_blues_artist_queue_status_idx ON all_blues_artist_queue (status)`);
    // seed_year: earliest year of a Blues-genre release in release_cache
    // that referenced this artist as a primary. Drives the worker's
    // oldest-first ordering and acts as a "this is a real blues seed"
    // flag — mention-follow targets without a seed_year are excluded
    // from the graph so the network stays inside the blues universe.
    await getPool().query(`ALTER TABLE all_blues_artist_queue ADD COLUMN IF NOT EXISTS seed_year INT`);
    await getPool().query(`CREATE INDEX IF NOT EXISTS all_blues_artist_queue_seed_year_idx ON all_blues_artist_queue (seed_year)`);
    // name: harvested at collect time from release_cache.data.artists[]
    // so the graph has labels the moment the worker finishes collect,
    // without waiting for the rate-limited Discogs profile fetch phase.
    // Overwritten by the actual fetched profile name on conflict — until
    // then this is what the public network view shows.
    await getPool().query(`ALTER TABLE all_blues_artist_queue ADD COLUMN IF NOT EXISTS name TEXT`);
    // Cached graph positions: admin runs fcose once in the browser,
    // posts the resulting x/y per node back, and everyone gets a
    // "preset" layout next load = instant. New seeds added by later
    // worker runs get NULL positions and trigger an fcose re-fit only
    // on demand (admin button); public viewers always use whatever's
    // cached, falling back to (0,0) for unpositioned nodes.
    await getPool().query(`ALTER TABLE all_blues_artist_queue ADD COLUMN IF NOT EXISTS pos_x DOUBLE PRECISION`);
    await getPool().query(`ALTER TABLE all_blues_artist_queue ADD COLUMN IF NOT EXISTS pos_y DOUBLE PRECISION`);
    await getPool().query(`
    CREATE TABLE IF NOT EXISTS all_blues_warm_state (
      id              INT PRIMARY KEY DEFAULT 1,
      running         BOOLEAN NOT NULL DEFAULT false,
      phase           TEXT NOT NULL DEFAULT 'idle',
      from_year       INT NOT NULL DEFAULT 1900,
      to_year         INT NOT NULL DEFAULT 1970,
      started_at      TIMESTAMPTZ,
      last_tick_at    TIMESTAMPTZ,
      artists_queued  INT NOT NULL DEFAULT 0,
      artists_fetched INT NOT NULL DEFAULT 0,
      artists_errored INT NOT NULL DEFAULT 0,
      links_inserted  INT NOT NULL DEFAULT 0,
      last_error      TEXT,
      CHECK (id = 1)
    )
  `);
    await getPool().query(`INSERT INTO all_blues_warm_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING`);
    // Migration: extend the all_blues_links kind CHECK to include
    // 'traveled' so we can import the matching kind from the manually
    // curated blues_artist_links table. Same idempotent pattern as the
    // blues_artist_links CHECK widening above — find every CHECK on
    // all_blues_links that references "kind", drop it, then re-add the
    // permissive one. Safe to re-run on every boot.
    await getPool().query(`
    DO $$
    DECLARE r record;
    BEGIN
      FOR r IN
        SELECT con.conname
          FROM pg_constraint con
          JOIN pg_class    cls ON cls.oid = con.conrelid
         WHERE cls.relname = 'all_blues_links'
           AND con.contype = 'c'
           AND pg_get_constraintdef(con.oid) ILIKE '%kind%'
      LOOP
        EXECUTE format('ALTER TABLE all_blues_links DROP CONSTRAINT %I', r.conname);
      END LOOP;
      ALTER TABLE all_blues_links ADD CONSTRAINT all_blues_links_kind_check
        CHECK (kind IN ('family','spouse','mentor','band','alias','mention','traveled'));
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'all_blues_links kind CHECK migration skipped: %', SQLERRM;
    END $$;
  `);
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
// ── Blues lyrics: re-link orphans to existing blues_artists rows ──
// More aggressive than the boot-time backfill: also matches when one
// side has a Discogs " (N)" disambiguator and the other doesn't, and
// collapses internal whitespace. Returns the number of rows linked.
// Idempotent. Triggered by the admin "Re-link orphans" button so the
// user can sweep up near-misses without restarting the server.
export async function relinkOrphanLyricsToArtists() {
    const r = await getPool().query(`
    UPDATE blues_lyrics l
       SET artist_id = a.id
      FROM blues_artists a
     WHERE l.artist_id IS NULL
       AND l.artist IS NOT NULL
       AND (
         -- Pass 1: exact lowercase trim match (cheap).
         LOWER(TRIM(l.artist)) = LOWER(a.name)
         OR
         -- Pass 2: drop the " (N)" Discogs disambiguator from EITHER
         -- side before comparing. Catches "Tommy Tucker" lyric vs
         -- "Tommy Tucker (3)" artist row and vice versa.
         REGEXP_REPLACE(LOWER(TRIM(l.artist)), '\\s*\\(\\d+\\)\\s*$', '')
           =
         REGEXP_REPLACE(LOWER(a.name),         '\\s*\\(\\d+\\)\\s*$', '')
       )
  `);
    return r.rowCount ?? 0;
}
// ── Blues lyrics: bulk normalize empty tuning → "Standard" ─────────
// On a blues-lyrics wiki an unmentioned tuning is overwhelmingly
// standard. Backend for the admin-triggered button on the archive
// list. Returns the rowcount so the UI can report it. Idempotent
// (NULL set shrinks to zero after first run).
export async function normalizeEmptyTuningsToStandard() {
    const r = await getPool().query(`UPDATE blues_lyrics SET tuning = 'Standard' WHERE tuning IS NULL OR tuning = ''`);
    return r.rowCount ?? 0;
}
// ── Blues lyrics: get-or-create artist by name ─────────────────────
// Backs the "+ Create as new" affordance on the lyric editor's Artist
// input. Looks up by case-insensitive name first to avoid duplicates,
// otherwise inserts a fresh row with enrichment_status flagged so it
// can be distinguished from imported/seeded rows.
export async function getOrCreateBluesArtistByName(rawName) {
    const name = String(rawName || "").replace(/\s+/g, " ").trim();
    if (!name)
        throw new Error("name required");
    if (name.length > 200)
        throw new Error("name too long");
    const existing = await getPool().query(`SELECT id, name FROM blues_artists WHERE LOWER(name) = LOWER($1) LIMIT 1`, [name]);
    if (existing.rows.length) {
        return { id: existing.rows[0].id, name: existing.rows[0].name, created: false };
    }
    const inserted = await getPool().query(`INSERT INTO blues_artists (name, enrichment_status)
     VALUES ($1, '{"source":"manual_editor_create"}'::jsonb)
     RETURNING id, name`, [name]);
    return { id: inserted.rows[0].id, name: inserted.rows[0].name, created: true };
}
// ── Blues DB helpers (admin-only) ──────────────────────────────────────
// Field whitelist used by upsert/update so the admin UI can't smuggle
// arbitrary columns. Keep this list aligned with the table schema above.
const _BLUES_FIELDS = [
    "wikidata_qid", "musicbrainz_mbid", "discogs_id", "name",
    "aliases", "birth_date", "birth_place", "death_date", "death_place",
    "death_cause", "hometown_region",
    "first_recording_year", "first_recording_title",
    "last_recording_year", "last_recording_title",
    "associated_labels", "styles", "instruments",
    "songs_authored", "collaborators",
    "photo_url", "wikipedia_suffix", "youtube_urls", "notes",
    "enrichment_status", "discogs_releases", "external_urls",
];
const _BLUES_JSONB_FIELDS = new Set([
    "aliases", "associated_labels", "styles", "instruments",
    "songs_authored", "collaborators", "youtube_urls",
    "enrichment_status", "discogs_releases", "external_urls",
]);
const _BLUES_INT_FIELDS = new Set([
    "discogs_id", "first_recording_year", "last_recording_year",
]);
function _coerceBluesValue(field, value) {
    if (value === undefined || value === null || value === "")
        return null;
    if (_BLUES_JSONB_FIELDS.has(field)) {
        if (Array.isArray(value) || typeof value === "object")
            return JSON.stringify(value);
        if (typeof value === "string") {
            // Tolerate comma-separated input from the admin UI for array fields.
            try {
                JSON.parse(value);
                return value;
            }
            catch {
                const arr = value.split(",").map(s => s.trim()).filter(Boolean);
                return JSON.stringify(arr);
            }
        }
        return JSON.stringify(value);
    }
    if (_BLUES_INT_FIELDS.has(field)) {
        const n = parseInt(String(value), 10);
        return Number.isFinite(n) ? n : null;
    }
    return String(value);
}
// SQL fragment that computes the earliest release year for an artist:
// the smaller of first_recording_year (filled by MB enrichment) and the
// minimum year inside the discogs_releases JSONB array (filled by the
// Discogs seed). Used for both the display column and the sort key.
const _EARLIEST_REL_SQL = `
  LEAST(
    first_recording_year,
    (SELECT min((rel->>'year')::int)
       FROM jsonb_array_elements(coalesce(discogs_releases, '[]'::jsonb)) rel
       WHERE rel ? 'year' AND (rel->>'year') ~ '^[0-9]+$')
  )
`;
// Whitelist of sortable columns → SQL fragments. Anything outside this
// map is rejected so the admin form can't smuggle arbitrary SQL.
const _BLUES_SORT_COLUMNS = {
    name: "lower(name)",
    birth_date: "birth_date",
    death_date: "death_date",
    birth_place: "lower(coalesce(birth_place,''))",
    hometown_region: "lower(coalesce(hometown_region,''))",
    styles: "lower(coalesce(styles->>0,''))",
    wikidata_qid: "wikidata_qid",
    discogs_id: "discogs_id",
    release_count: "jsonb_array_length(coalesce(discogs_releases, '[]'::jsonb))",
    earliest_release: _EARLIEST_REL_SQL,
    first_recording_year: "first_recording_year",
    date_added: "date_added",
    updated_at: "updated_at",
};
export async function listBluesArtists(opts = {}) {
    const { search, limit = 50, offset = 0, sort = "name", order = "asc" } = opts;
    const args = [];
    let where = "";
    if (search?.trim()) {
        args.push(`%${search.trim().toLowerCase()}%`);
        where = `WHERE lower(name) LIKE $1 OR lower(coalesce(birth_place,'')) LIKE $1 OR lower(coalesce(hometown_region,'')) LIKE $1`;
    }
    const countSql = `SELECT count(*)::int AS total FROM blues_artists ${where}`;
    const totalRes = await getPool().query(countSql, args);
    const total = totalRes.rows[0]?.total ?? 0;
    // Resolve sort against the whitelist; fall back to name if unknown.
    const sortFrag = _BLUES_SORT_COLUMNS[sort] ?? _BLUES_SORT_COLUMNS["name"];
    const dir = String(order).toLowerCase() === "desc" ? "DESC" : "ASC";
    // NULLS LAST so empty values don't dominate the top of asc sorts.
    args.push(limit, offset);
    const sql = `
    SELECT *,
           ${_EARLIEST_REL_SQL} AS earliest_release_year
    FROM blues_artists
    ${where}
    ORDER BY ${sortFrag} ${dir} NULLS LAST, lower(name) ASC
    LIMIT $${args.length - 1} OFFSET $${args.length}
  `;
    const r = await getPool().query(sql, args);
    return { rows: r.rows, total };
}
/** Return every discogs_id currently in blues_artists. Used by the
 *  admin-only "+" icon next to artist names in album popups so we can
 *  hide the icon for artists already in the DB. */
export async function getBluesArtistDiscogsIds() {
    const r = await getPool().query(`SELECT discogs_id FROM blues_artists WHERE discogs_id IS NOT NULL ORDER BY discogs_id`);
    return r.rows.map(row => row.discogs_id);
}
/** Return both the discogs_ids and the names of every row in
 *  blues_artists. Cards parse artist NAMES from result titles and
 *  don't have a Discogs ID locally, so we cache names too for the
 *  "already in DB?" check on card-level "+" buttons. */
export async function getBluesArtistIdentifiers() {
    const r = await getPool().query(`SELECT discogs_id, name FROM blues_artists`);
    const ids = [];
    const names = [];
    for (const row of r.rows) {
        if (row.discogs_id)
            ids.push(row.discogs_id);
        if (row.name)
            names.push(row.name);
    }
    return { ids, names };
}
/** Wipe the entire blues_artists table. Admin-only — there's no
 *  per-row history so this is irreversible. */
export async function deleteAllBluesArtists() {
    const r = await getPool().query(`DELETE FROM blues_artists`);
    return r.rowCount ?? 0;
}
export async function getBluesArtist(id) {
    const r = await getPool().query(`SELECT * FROM blues_artists WHERE id = $1`, [id]);
    return r.rows[0] ?? null;
}
export async function deleteBluesArtist(id) {
    await getPool().query(`DELETE FROM blues_artists WHERE id = $1`, [id]);
}
// Cascade-delete the artist AND every lyric tied to them. Matches
// lyrics by FK (canonical) OR by case-insensitive name (legacy rows
// whose artist_id hasn't been backfilled). Single transaction so a
// partial failure rolls back the whole thing. Returns counts so the
// UI can report exactly how many lyrics went with the artist.
// ── Tunings grid helpers ─────────────────────────────────────────────
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
const _TUNINGS_SORT_COLS = {
    artist: "artist", track: "track", title: "title",
    position: "position", pitch: "pitch",
};
export async function listBluesTunings(opts = {}) {
    const where = [];
    const params = [];
    if (opts.q) {
        params.push(`%${opts.q}%`);
        const p = `$${params.length}`;
        where.push(`(artist ILIKE ${p} OR title ILIKE ${p} OR position ILIKE ${p} OR pitch ILIKE ${p} OR notes ILIKE ${p})`);
    }
    if (opts.artist) {
        params.push(opts.artist);
        where.push(`artist = $${params.length}`);
    }
    if (opts.position) {
        params.push(opts.position);
        where.push(`position = $${params.length}`);
    }
    if (opts.pitch) {
        params.push(opts.pitch);
        where.push(`pitch = $${params.length}`);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const sortCol = _TUNINGS_SORT_COLS[String(opts.sort ?? "")] || "artist";
    const order = opts.order === "desc" ? "DESC" : "ASC";
    // Title sort uses a derived "display title": for collaborative-session
    // rows the title column is blank and the real track name is stashed in
    // notes as "With <collaborator>:: <track>" — same logic the client
    // applies for display. Without this, every blank-title row sorts to
    // the top of the page in title-asc order even though their VISIBLE
    // titles are anything but "" — which is what the user saw.
    const titleExpr = `COALESCE(NULLIF(title, ''),
    CASE WHEN notes ~ '::' THEN trim(regexp_replace(notes, '^.*?::\\s*', ''))
         ELSE notes END,
    '')`;
    const sortExpr = sortCol === "title" ? titleExpr : sortCol;
    const orderSql = sortCol === "artist"
        ? `ORDER BY artist ${order}, ${titleExpr} ASC`
        : `ORDER BY ${sortExpr} ${order} NULLS LAST, artist ASC, ${titleExpr} ASC`;
    const totalR = await getPool().query(`SELECT COUNT(*)::int AS n FROM blues_tunings_grid ${whereSql}`, params);
    const total = totalR.rows[0]?.n ?? 0;
    const limit = Math.max(1, Math.min(500, opts.limit ?? 200));
    const offset = Math.max(0, opts.offset ?? 0);
    params.push(limit, offset);
    const rowsR = await getPool().query(`SELECT id, artist, track, title, position, pitch, notes
       FROM blues_tunings_grid ${whereSql}
       ${orderSql}
       LIMIT $${params.length - 1} OFFSET $${params.length}`, params);
    return { rows: rowsR.rows, total };
}
/** Bulk-set the `position` column on a set of blues_tunings_grid
 *  rows. Empty / null position clears the column. Returns the rows
 *  actually updated. */
export async function bulkUpdateTuningPosition(ids, position) {
    const cleanIds = Array.from(new Set((ids || [])
        .map((v) => Number(v))
        .filter((n) => Number.isFinite(n) && n > 0)));
    if (!cleanIds.length)
        return 0;
    const normalized = (typeof position === "string" ? position.trim().slice(0, 80) : null);
    const r = await getPool().query(`UPDATE blues_tunings_grid SET position = $1 WHERE id = ANY($2::int[])`, [normalized || null, cleanIds]);
    return r.rowCount ?? 0;
}
/** Bulk-delete blues_tunings_grid rows. */
export async function bulkDeleteTunings(ids) {
    const cleanIds = Array.from(new Set((ids || [])
        .map((v) => Number(v))
        .filter((n) => Number.isFinite(n) && n > 0)));
    if (!cleanIds.length)
        return 0;
    const r = await getPool().query(`DELETE FROM blues_tunings_grid WHERE id = ANY($1::int[])`, [cleanIds]);
    return r.rowCount ?? 0;
}
/** Just the ids matching the current tunings filter (same shape as
 *  listBluesTunings). Capped at 10k for the bulk editor's "Select all
 *  matching" link. */
export async function listBluesTuningIdsMatching(opts) {
    const CAP = 10000;
    const out = await listBluesTunings({ ...opts, limit: CAP + 1, offset: 0 });
    const ids = (out.rows || []).slice(0, CAP).map((r) => Number(r.id));
    return { ids, capped: (out.rows?.length ?? 0) > CAP };
}
export async function getBluesTuningsFacets() {
    const r = await getPool().query(`
    SELECT
      ARRAY(SELECT DISTINCT artist   FROM blues_tunings_grid WHERE artist   IS NOT NULL AND artist   <> '' ORDER BY artist)   AS artists,
      ARRAY(SELECT DISTINCT position FROM blues_tunings_grid WHERE position IS NOT NULL AND position <> '' ORDER BY position) AS positions,
      ARRAY(SELECT DISTINCT pitch    FROM blues_tunings_grid WHERE pitch    IS NOT NULL AND pitch    <> '' ORDER BY pitch)    AS pitches
  `);
    const row = r.rows[0] ?? {};
    return {
        artists: Array.isArray(row.artists) ? row.artists : [],
        positions: Array.isArray(row.positions) ? row.positions : [],
        pitches: Array.isArray(row.pitches) ? row.pitches : [],
    };
}
// ── Lyrics ban list helpers ──────────────────────────────────────────
// blues_lyrics_bans backs the "delete and don't re-add" workflow:
// scrape consults these sets so already-banned titles never get fetched
// and banned-artist pages never get upserted even if the discovery
// phase finds them.
export async function addBluesLyricsBans(rows) {
    if (!rows.length)
        return 0;
    let inserted = 0;
    for (const row of rows) {
        const v = String(row.value || "").trim();
        if (!v)
            continue;
        // Pre-check via the same lower-cased trim used by the unique
        // index so we don't have to wrestle with ON CONFLICT's expression-
        // index inference (which is finicky). The unique index still
        // catches concurrent inserts — we just swallow that error.
        const exists = await getPool().query(`SELECT 1 FROM blues_lyrics_bans
        WHERE kind = $1
          AND LOWER(TRIM(value)) = LOWER(TRIM($2))
        LIMIT 1`, [row.kind, v]);
        if (exists.rows.length)
            continue;
        try {
            await getPool().query(`INSERT INTO blues_lyrics_bans (kind, value, reason, banned_by)
         VALUES ($1, $2, $3, $4)`, [row.kind, v, row.reason ?? null, row.bannedBy ?? null]);
            inserted++;
        }
        catch (e) {
            // Unique-violation: someone raced us. Treat as already-banned.
            if (e?.code !== "23505")
                throw e;
        }
    }
    return inserted;
}
export async function removeBluesLyricsBan(id) {
    const r = await getPool().query(`DELETE FROM blues_lyrics_bans WHERE id = $1`, [id]);
    return (r.rowCount ?? 0) > 0;
}
export async function listBluesLyricsBans(opts = {}) {
    const where = [];
    const params = [];
    if (opts.kind) {
        params.push(opts.kind);
        where.push(`kind = $${params.length}`);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const totalR = await getPool().query(`SELECT COUNT(*)::int AS n FROM blues_lyrics_bans ${whereSql}`, params);
    const limit = Math.max(1, Math.min(500, opts.limit ?? 200));
    const offset = Math.max(0, opts.offset ?? 0);
    params.push(limit, offset);
    const rowsR = await getPool().query(`SELECT id, kind, value, reason, banned_by, banned_at
       FROM blues_lyrics_bans ${whereSql}
       ORDER BY banned_at DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`, params);
    return { rows: rowsR.rows, total: totalR.rows[0]?.n ?? 0 };
}
// Sets for the scrape loop — cheap full-table scans (the ban list is
// expected to stay small, < a few hundred rows for normal use). Values
// are lower-cased+trimmed so the in-memory contains() match is fast
// regardless of how the scraper capitalises the extracted artist.
export async function getBannedLyricTitleSet() {
    const r = await getPool().query(`SELECT TRIM(value) AS value FROM blues_lyrics_bans WHERE kind = 'title'`);
    return new Set(r.rows.map((row) => String(row.value)));
}
export async function getBannedLyricArtistSet() {
    const r = await getPool().query(`SELECT LOWER(TRIM(value)) AS value FROM blues_lyrics_bans WHERE kind = 'artist'`);
    return new Set(r.rows.map((row) => String(row.value)));
}
// Body-hash bans — fingerprint the exact plaintext of a deleted
// lyric so a re-upload of the same body gets skipped on rescrape
// without blocking real edits (a single character change produces a
// different hash). Values are SHA-256 hex digests; the lookup is
// case-sensitive (hex is normalized lowercase at write-time).
export async function getBannedLyricBodyHashSet() {
    const r = await getPool().query(`SELECT LOWER(TRIM(value)) AS value FROM blues_lyrics_bans WHERE kind = 'body_hash'`);
    return new Set(r.rows.map((row) => String(row.value)));
}
export async function deleteBluesArtistAndLyrics(id, opts = {}) {
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        const ar = await client.query(`SELECT id, name FROM blues_artists WHERE id = $1 FOR UPDATE`, [id]);
        if (!ar.rows.length) {
            await client.query("ROLLBACK");
            throw new Error("artist not found");
        }
        const name = String(ar.rows[0].name || "");
        // Capture page titles BEFORE we delete so we can ban them by
        // exact match later. Union both deletion sets: FK-linked +
        // legacy name-matched orphans.
        const titlesR = await client.query(`SELECT page_title FROM blues_lyrics
        WHERE artist_id = $1
           OR (artist_id IS NULL
               AND LOWER(TRIM(COALESCE(artist, ''))) = LOWER(TRIM($2)))`, [id, name]);
        const titles = titlesR.rows.map((r) => String(r.page_title)).filter(Boolean);
        // 1. Drop lyrics linked by FK.
        const r1 = await client.query(`DELETE FROM blues_lyrics WHERE artist_id = $1`, [id]);
        // 2. Drop lyrics that name-match but never got FK-linked (legacy).
        const r2 = name
            ? await client.query(`DELETE FROM blues_lyrics
            WHERE artist_id IS NULL
              AND LOWER(TRIM(COALESCE(artist, ''))) = LOWER(TRIM($1))`, [name])
            : { rowCount: 0 };
        // 3. Drop the artist.
        await client.query(`DELETE FROM blues_artists WHERE id = $1`, [id]);
        await client.query("COMMIT");
        let bansAdded = 0;
        if (opts.ban) {
            // After the transaction so a ban-insert race can't roll back
            // the deletion. Banning is best-effort — duplicates are fine.
            const banRows = [];
            if (name)
                banRows.push({ kind: "artist", value: name, reason: opts.banReason ?? null, bannedBy: opts.banBy ?? null });
            for (const t of titles)
                banRows.push({ kind: "title", value: t, reason: opts.banReason ?? null, bannedBy: opts.banBy ?? null });
            try {
                bansAdded = await addBluesLyricsBans(banRows);
            }
            catch (e) {
                console.warn("[deleteBluesArtistAndLyrics] ban insert failed:", e);
            }
        }
        return {
            ok: true,
            artistName: name,
            lyricsDeleted: (r1.rowCount ?? 0) + (r2.rowCount ?? 0),
            bansAdded,
        };
    }
    catch (e) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        throw e;
    }
    finally {
        client.release();
    }
}
// ── Lyric first_release_year resolver: cheap path ────────────────────
// For every lyric whose linked artist has a discogs_releases entry
// whose title matches the lyric's page_title, set first_release_year
// to the minimum year among Main-role matches.
//
// Pre-WWII 78 rpm releases on Discogs are titled "A-Side / B-Side"
// (e.g. "Crossroads Blues / Walking Blues"), and titles diverge from
// the lyric wiki on apostrophes, parens, capitalization. So the
// matcher tries (in order of trust):
//   1. Exact normalized match — strip everything but [a-z0-9] from
//      both sides. Catches apostrophe / punctuation differences.
//   2. Either side of an A/B "X / Y" release title (split on slash)
//      via the same normalization.
// MIN(year) over all matching entries → assigns the earliest known
// release year. Idempotent — only touches rows where the value would
// actually change. force flag re-resolves rows whose year already
// came from this path (in case the artist's release list shifted).
export async function resolveLyricFirstReleaseYearsCheap(opts = {}) {
    const guard = opts.force
        ? ""
        : `AND (l.first_release_year IS NULL OR l.first_release_source = 'artist_releases')`;
    // norm() collapses to [a-z0-9] only so "Don't You Lie to Me" ==
    // "Dont You Lie To Me" == "don't_you_lie_to_me" — see inline SQL.
    const r = await getPool().query(`
    WITH cand AS (
      SELECT l.id AS lyric_id,
             MIN((rel->>'year')::int) AS year
        FROM blues_lyrics l
        JOIN blues_artists a ON a.id = l.artist_id,
             jsonb_array_elements(COALESCE(a.discogs_releases, '[]'::jsonb)) AS rel
       WHERE (rel->>'year') ~ '^[0-9]+$'
         AND ((rel->>'role') IS NULL OR (rel->>'role') = '' OR (rel->>'role') = 'Main')
         AND (
           -- Full release title matches lyric (normalized: parens
           -- content stripped, then everything but [a-z0-9])
           LOWER(regexp_replace(regexp_replace(COALESCE(rel->>'title',''), '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g'))
             = LOWER(regexp_replace(regexp_replace(l.page_title, '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g'))
           OR
           -- A-side of "A / B" matches lyric
           LOWER(regexp_replace(regexp_replace(COALESCE(split_part(rel->>'title','/',1),''), '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g'))
             = LOWER(regexp_replace(regexp_replace(l.page_title, '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g'))
           OR
           -- B-side of "A / B" matches lyric
           LOWER(regexp_replace(regexp_replace(COALESCE(split_part(rel->>'title','/',2),''), '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g'))
             = LOWER(regexp_replace(regexp_replace(l.page_title, '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g'))
         )
         AND length(regexp_replace(regexp_replace(l.page_title, '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g')) >= 3
         ${guard}
       GROUP BY l.id
    )
    UPDATE blues_lyrics l
       SET first_release_year       = c.year,
           first_release_source     = 'artist_releases',
           first_release_checked_at = NOW()
      FROM cand c
     WHERE c.lyric_id = l.id
       AND (l.first_release_year IS DISTINCT FROM c.year OR l.first_release_source IS DISTINCT FROM 'artist_releases')
  `);
    return { updated: r.rowCount ?? 0 };
}
// ── Lyric first_release_year resolver: release_cache path ────────────
// Richer sibling of resolveLyricFirstReleaseYearsCheap. That one only
// consults each linked artist's own discogs_releases JSONB (small, and
// needs artist_id). This one scans the big release_cache — where the
// masters+ sweeps have cached tens of thousands of early-blues releases
// and masters — matching a lyric to any cached release whose title AND
// one of whose credited artists match. MIN(year) over matches becomes
// first_release_year. Zero Discogs API calls; one sequential scan of
// release_cache, hash-joined to the (small) set of year-less lyrics.
//
// Matching is deliberately conservative to avoid stamping a wrong year:
//   • title: normalized (parens stripped, then [a-z0-9] only) equality,
//     OR either side of a "A / B" 78rpm coupling.
//   • artist: normalized equality against ANY name in the release's
//     data->'artists' array. Requiring an artist match is what keeps a
//     common title ("Crossroads") from grabbing a different act's year.
// Lyrics with a blank artist can't satisfy the artist test, so they're
// left for manual entry (surface them via the "No artist" grid filter).
export async function resolveLyricFirstReleaseYearsFromCache(opts = {}) {
    const guard = opts.force
        ? ""
        : `AND (l.first_release_year IS NULL OR l.first_release_source IN ('artist_releases', 'release_cache'))`;
    // norm() = lower, drop parenthetical asides, keep [a-z0-9] only — so
    // "Don't You Lie to Me" == "Dont You Lie To Me". Applied identically
    // to lyric titles, cache titles, and both artist name sides.
    const r = await getPool().query(`
    WITH lyr AS (
      SELECT l.id,
             LOWER(regexp_replace(regexp_replace(l.page_title, '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g')) AS ntitle,
             LOWER(regexp_replace(COALESCE(l.artist, ''), '[^a-zA-Z0-9]', '', 'g'))                              AS nartist
        FROM blues_lyrics l
       WHERE (l.first_release_year IS NULL OR $1::bool)
         AND l.artist IS NOT NULL AND LENGTH(TRIM(l.artist)) > 0
         AND LENGTH(regexp_replace(regexp_replace(l.page_title, '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g')) >= 3
    ),
    cand AS (
      SELECT lyr.id AS lyric_id, MIN((rc.data->>'year')::int) AS year
        FROM release_cache rc
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(rc.data->'artists', '[]'::jsonb)) AS art
        JOIN lyr ON (
          lyr.ntitle = LOWER(regexp_replace(regexp_replace(COALESCE(rc.data->>'title',''), '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g'))
          OR lyr.ntitle = LOWER(regexp_replace(regexp_replace(COALESCE(split_part(rc.data->>'title','/',1),''), '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g'))
          OR lyr.ntitle = LOWER(regexp_replace(regexp_replace(COALESCE(split_part(rc.data->>'title','/',2),''), '\\([^)]*\\)', '', 'g'), '[^a-zA-Z0-9]', '', 'g'))
        )
       WHERE (rc.data->>'year') ~ '^[0-9]+$'
         AND (rc.data->>'year')::int >= 1850
         AND lyr.nartist = LOWER(regexp_replace(COALESCE(art->>'name',''), '[^a-zA-Z0-9]', '', 'g'))
       GROUP BY lyr.id
    )
    UPDATE blues_lyrics l
       SET first_release_year       = c.year,
           first_release_source     = 'release_cache',
           first_release_checked_at = NOW()
      FROM cand c
     WHERE c.lyric_id = l.id
       ${guard}
       AND (l.first_release_year IS DISTINCT FROM c.year OR l.first_release_source IS DISTINCT FROM 'release_cache')
  `, [!!opts.force]);
    return { updated: r.rowCount ?? 0 };
}
// Return blues_lyrics ids missing first_release_year — feed for the
// (future) Discogs-search worker. Capped to keep payloads sane.
export async function getLyricsMissingFirstReleaseYear(limit = 1000) {
    const r = await getPool().query(`SELECT id, page_title, artist, artist_id
       FROM blues_lyrics
      WHERE first_release_year IS NULL
      ORDER BY id ASC
      LIMIT $1`, [Math.max(1, Math.min(5000, limit))]);
    return r.rows;
}
// ── Lyric favorites ──────────────────────────────────────────────────
export async function listLyricFavoriteIds(userId) {
    const r = await getPool().query(`SELECT lyric_id FROM blues_lyric_favorites WHERE clerk_user_id = $1 ORDER BY created_at DESC`, [userId]);
    return r.rows.map(row => Number(row.lyric_id));
}
export async function listLyricFavoritesWithDetails(userId) {
    const r = await getPool().query(`SELECT l.id, l.page_title, l.artist, l.artist_id, l.tuning,
            l.discogs_release_id, l.discogs_master_id,
            substring(l.plaintext, 1, 240) AS snippet,
            f.created_at AS favorited_at
       FROM blues_lyric_favorites f
       JOIN blues_lyrics l ON l.id = f.lyric_id
      WHERE f.clerk_user_id = $1
      ORDER BY f.created_at DESC`, [userId]);
    return r.rows;
}
export async function addLyricFavorite(userId, lyricId) {
    const r = await getPool().query(`INSERT INTO blues_lyric_favorites (clerk_user_id, lyric_id) VALUES ($1, $2)
     ON CONFLICT DO NOTHING`, [userId, lyricId]);
    return (r.rowCount ?? 0) > 0;
}
export async function removeLyricFavorite(userId, lyricId) {
    const r = await getPool().query(`DELETE FROM blues_lyric_favorites WHERE clerk_user_id = $1 AND lyric_id = $2`, [userId, lyricId]);
    return (r.rowCount ?? 0) > 0;
}
// ── Setlists ─────────────────────────────────────────────────────────
export async function listSetlists(userId) {
    const r = await getPool().query(`SELECT s.id, s.name, s.notes, s.created_at, s.updated_at,
            (SELECT COUNT(*)::int FROM blues_setlist_items si WHERE si.setlist_id = s.id) AS item_count
       FROM blues_setlists s
      WHERE s.clerk_user_id = $1
      ORDER BY s.updated_at DESC`, [userId]);
    return r.rows;
}
export async function getSetlist(userId, id) {
    const sr = await getPool().query(`SELECT id, name, notes, created_at, updated_at FROM blues_setlists
      WHERE id = $1 AND clerk_user_id = $2`, [id, userId]);
    if (!sr.rows.length)
        return null;
    const items = await getPool().query(`SELECT si.lyric_id, si.sort_order, si.note, si.added_at,
            l.page_title, l.artist, l.artist_id, l.tuning,
            l.discogs_release_id, l.discogs_master_id,
            substring(l.plaintext, 1, 240) AS snippet
       FROM blues_setlist_items si
       JOIN blues_lyrics l ON l.id = si.lyric_id
      WHERE si.setlist_id = $1
      ORDER BY si.sort_order ASC, si.lyric_id ASC`, [id]);
    return { ...sr.rows[0], items: items.rows };
}
export async function createSetlist(userId, name, notes = null) {
    const trimmed = name.trim();
    if (!trimmed)
        throw new Error("name required");
    const r = await getPool().query(`INSERT INTO blues_setlists (clerk_user_id, name, notes) VALUES ($1, $2, $3) RETURNING id`, [userId, trimmed, notes]);
    return Number(r.rows[0].id);
}
export async function updateSetlist(userId, id, patch) {
    const sets = [];
    const params = [];
    if (patch.name != null) {
        params.push(patch.name.trim());
        sets.push(`name = $${params.length}`);
    }
    if (patch.notes !== undefined) {
        params.push(patch.notes);
        sets.push(`notes = $${params.length}`);
    }
    if (!sets.length)
        return false;
    params.push(userId, id);
    const r = await getPool().query(`UPDATE blues_setlists SET ${sets.join(", ")} WHERE clerk_user_id = $${params.length - 1} AND id = $${params.length}`, params);
    return (r.rowCount ?? 0) > 0;
}
export async function deleteSetlist(userId, id) {
    const r = await getPool().query(`DELETE FROM blues_setlists WHERE id = $1 AND clerk_user_id = $2`, [id, userId]);
    return (r.rowCount ?? 0) > 0;
}
export async function addSetlistItem(userId, setlistId, lyricId, note = null) {
    // Verify setlist ownership before write so a malicious caller can't
    // append items to someone else's setlist via id-guessing.
    const own = await getPool().query(`SELECT 1 FROM blues_setlists WHERE id = $1 AND clerk_user_id = $2`, [setlistId, userId]);
    if (!own.rows.length)
        throw new Error("setlist not found");
    // sort_order defaults to current max + 1 so new items land at the end.
    const r = await getPool().query(`INSERT INTO blues_setlist_items (setlist_id, lyric_id, sort_order, note)
       SELECT $1, $2, COALESCE(MAX(sort_order), 0) + 1, $3
         FROM blues_setlist_items WHERE setlist_id = $1
     ON CONFLICT DO NOTHING`, [setlistId, lyricId, note]);
    await getPool().query(`UPDATE blues_setlists SET updated_at = NOW() WHERE id = $1`, [setlistId]);
    return (r.rowCount ?? 0) > 0;
}
export async function removeSetlistItem(userId, setlistId, lyricId) {
    const own = await getPool().query(`SELECT 1 FROM blues_setlists WHERE id = $1 AND clerk_user_id = $2`, [setlistId, userId]);
    if (!own.rows.length)
        throw new Error("setlist not found");
    const r = await getPool().query(`DELETE FROM blues_setlist_items WHERE setlist_id = $1 AND lyric_id = $2`, [setlistId, lyricId]);
    await getPool().query(`UPDATE blues_setlists SET updated_at = NOW() WHERE id = $1`, [setlistId]);
    return (r.rowCount ?? 0) > 0;
}
export async function reorderSetlistItems(userId, setlistId, items) {
    const own = await getPool().query(`SELECT 1 FROM blues_setlists WHERE id = $1 AND clerk_user_id = $2`, [setlistId, userId]);
    if (!own.rows.length)
        throw new Error("setlist not found");
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        for (const it of items) {
            await client.query(`UPDATE blues_setlist_items SET sort_order = $1 WHERE setlist_id = $2 AND lyric_id = $3`, [Number(it.sort_order) || 0, setlistId, Number(it.lyricId)]);
        }
        await client.query(`UPDATE blues_setlists SET updated_at = NOW() WHERE id = $1`, [setlistId]);
        await client.query("COMMIT");
    }
    catch (e) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        throw e;
    }
    finally {
        client.release();
    }
}
/** Upsert a Wikidata-sourced blues artist row.
 *
 *  Match order:
 *    1. existing row with same wikidata_qid  (re-running the seed)
 *    2. existing row with same discogs_id    (Discogs seed already
 *       created it — this is how the Wikidata seed merges its
 *       bio data into Discogs-keyed rows)
 *    3. else INSERT new row
 *
 *  When merging into an existing row we use COALESCE semantics: blank
 *  existing fields get filled, non-blank existing fields stay put so
 *  manual edits and earlier richer data aren't trampled. */
export async function upsertBluesArtistByQid(record) {
    if (!record.wikidata_qid || !record.name) {
        throw new Error("upsertBluesArtistByQid requires wikidata_qid and name");
    }
    // Look up existing row by either key.
    const existing = await getPool().query(`SELECT id, wikidata_qid, discogs_id FROM blues_artists
     WHERE wikidata_qid = $1
        OR ($2::int IS NOT NULL AND discogs_id = $2::int)
     ORDER BY (wikidata_qid = $1) DESC
     LIMIT 1`, [record.wikidata_qid, record.discogs_id ?? null]);
    if (existing.rows.length) {
        const row = existing.rows[0];
        const matchedBy = row.wikidata_qid === record.wikidata_qid ? "qid" : "discogs_id";
        // Build a COALESCE-style UPDATE so non-null existing values win.
        // JSONB array fields use a different rule: prefer the existing
        // array if it has content; otherwise take the new one.
        const sets = [];
        const vals = [];
        for (const f of _BLUES_FIELDS) {
            if (!(f in record))
                continue;
            vals.push(_coerceBluesValue(f, record[f]));
            const ph = `$${vals.length}`;
            if (_BLUES_JSONB_FIELDS.has(f)) {
                // Keep existing JSONB unless it's NULL / empty array.
                sets.push(`${f} = CASE
          WHEN ${f} IS NULL OR jsonb_typeof(${f}) = 'null' OR ${f} = '[]'::jsonb
            THEN ${ph}::jsonb
          ELSE ${f}
        END`);
            }
            else {
                // Keep existing scalar unless it's NULL or empty string.
                sets.push(`${f} = COALESCE(NULLIF(${f}, ''), ${ph})`);
            }
        }
        vals.push(new Date());
        sets.push(`updated_at = $${vals.length}`);
        vals.push(row.id);
        await getPool().query(`UPDATE blues_artists SET ${sets.join(", ")} WHERE id = $${vals.length}`, vals);
        return { id: row.id, merged: true, createdNew: false, matchedBy };
    }
    // No existing row — straight insert.
    const cols = [];
    const vals = [];
    const ph = [];
    for (const f of _BLUES_FIELDS) {
        if (!(f in record))
            continue;
        cols.push(f);
        vals.push(_coerceBluesValue(f, record[f]));
        ph.push(`$${vals.length}`);
    }
    const ins = await getPool().query(`INSERT INTO blues_artists (${cols.join(", ")}) VALUES (${ph.join(", ")}) RETURNING id`, vals);
    return { id: ins.rows[0].id, merged: false, createdNew: true, matchedBy: null };
}
/** Upsert from the Discogs seeder. Keys on discogs_id. Merges
 *  discogs_releases by release id (union), and only fills name/labels/
 *  styles when the existing row has them blank — never trampling
 *  manually-edited values or richer Wikidata-sourced data. */
export async function upsertBluesArtistByDiscogsId(record) {
    if (!record.discogs_id || !record.name) {
        throw new Error("upsertBluesArtistByDiscogsId requires discogs_id and name");
    }
    const existing = await getPool().query(`SELECT id, name, discogs_releases, associated_labels, styles, enrichment_status FROM blues_artists WHERE discogs_id = $1`, [record.discogs_id]);
    const newRels = record.discogs_releases ?? [];
    if (existing.rows.length) {
        const row = existing.rows[0];
        const oldRels = Array.isArray(row.discogs_releases) ? row.discogs_releases : [];
        const seen = new Set(oldRels.map((r) => `${r.type}:${r.id}`));
        const merged = [...oldRels];
        let added = 0;
        for (const r of newRels) {
            const k = `${r.type}:${r.id}`;
            if (!seen.has(k)) {
                merged.push(r);
                seen.add(k);
                added++;
            }
        }
        // Fill blank labels/styles, leave populated alone.
        const oldLabels = Array.isArray(row.associated_labels) ? row.associated_labels : [];
        const oldStyles = Array.isArray(row.styles) ? row.styles : [];
        const newLabels = oldLabels.length ? oldLabels : (record.associated_labels ?? []);
        const newStyles = oldStyles.length ? oldStyles : (record.styles ?? []);
        const status = (row.enrichment_status && typeof row.enrichment_status === "object")
            ? row.enrichment_status : {};
        await getPool().query(`UPDATE blues_artists
         SET discogs_releases  = $1::jsonb,
             associated_labels = $2::jsonb,
             styles            = $3::jsonb,
             enrichment_status = $4::jsonb,
             updated_at        = NOW()
       WHERE id = $5`, [JSON.stringify(merged), JSON.stringify(newLabels), JSON.stringify(newStyles),
            JSON.stringify({ ...status, discogs_seed: 1 }), row.id]);
        return { id: row.id, created: false, mergedCount: added };
    }
    // No existing row — fresh insert. Other fields default to blank/empty
    // so a later Wikidata/MB/Wiki enrichment can fill them.
    const ins = await getPool().query(`INSERT INTO blues_artists
       (discogs_id, name, discogs_releases, associated_labels, styles, enrichment_status)
     VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb)
     RETURNING id`, [
        record.discogs_id,
        record.name,
        JSON.stringify(newRels),
        JSON.stringify(record.associated_labels ?? []),
        JSON.stringify(record.styles ?? []),
        JSON.stringify({ discogs_seed: 1 }),
    ]);
    return { id: ins.rows[0].id, created: true, mergedCount: newRels.length };
}
export async function insertBluesArtist(record) {
    if (!record.name)
        throw new Error("insertBluesArtist requires name");
    const cols = [];
    const vals = [];
    const ph = [];
    for (const f of _BLUES_FIELDS) {
        if (!(f in record))
            continue;
        cols.push(f);
        vals.push(_coerceBluesValue(f, record[f]));
        ph.push(`$${vals.length}`);
    }
    const sql = `INSERT INTO blues_artists (${cols.join(", ")}) VALUES (${ph.join(", ")}) RETURNING id`;
    const r = await getPool().query(sql, vals);
    return r.rows[0].id;
}
export async function updateBluesArtist(id, record) {
    const sets = [];
    const vals = [];
    for (const f of _BLUES_FIELDS) {
        if (!(f in record))
            continue;
        vals.push(_coerceBluesValue(f, record[f]));
        sets.push(`${f} = $${vals.length}`);
    }
    if (!sets.length)
        return;
    vals.push(new Date());
    sets.push(`updated_at = $${vals.length}`);
    vals.push(id);
    await getPool().query(`UPDATE blues_artists SET ${sets.join(", ")} WHERE id = $${vals.length}`, vals);
}
export async function getBluesStats() {
    const r = await getPool().query(`
    SELECT
      count(*)::int AS total,
      max(date_added)  AS last_seed,
      max(updated_at)  AS last_update
    FROM blues_artists
  `);
    return {
        total: r.rows[0]?.total ?? 0,
        lastSeed: r.rows[0]?.last_seed ?? null,
        lastUpdate: r.rows[0]?.last_update ?? null,
    };
}
// Upsert a batch of entries; for each headword, REPLACES its citation
// list with the supplied ones (re-ingesting a volume is the expected
// path). Returns counts. Uses UNNEST batch inserts so a 100+ entry
// volume lands in 3 queries instead of 500+.
export async function ingestBluesWords(entries) {
    if (!entries.length)
        return { upserted: 0, citations: 0 };
    const norm = entries
        .map(e => ({
        ...e,
        headword: String(e.headword || "").trim().toLowerCase(),
    }))
        .filter(e => e.headword);
    if (!norm.length)
        return { upserted: 0, citations: 0 };
    // Build a JSONB payload — handles the int[] source_pages column
    // cleanly (UNNEST can't carry nested arrays as per-row values).
    const wordsPayload = norm.map(e => ({
        headword: e.headword,
        definition: String(e.definition ?? ""),
        source_volume: e.source_volume ?? null,
        source_pages: (e.source_pages && e.source_pages.length) ? e.source_pages : null,
    }));
    const citsPayload = [];
    for (const e of norm) {
        const list = (e.citations ?? []).filter(c => c && (c.artist || c.song_title || c.quote));
        list.forEach((c, i) => {
            citsPayload.push({
                headword: e.headword,
                position: c.position ?? i + 1,
                quote: c.quote ?? null,
                artist: c.artist ?? null,
                song_title: c.song_title ?? null,
                year: Number.isFinite(c.year) ? c.year : null,
            });
        });
    }
    const heads = norm.map(e => e.headword);
    const pool = getPool();
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        await client.query(`INSERT INTO blues_words (headword, definition, source_volume, source_pages, updated_at)
         SELECT x.headword, x.definition, x.source_volume, x.source_pages, NOW()
           FROM jsonb_to_recordset($1::jsonb) AS x(
             headword      text,
             definition    text,
             source_volume text,
             source_pages  int[]
           )
        ON CONFLICT (headword) DO UPDATE SET
          definition    = EXCLUDED.definition,
          source_volume = COALESCE(EXCLUDED.source_volume, blues_words.source_volume),
          source_pages  = COALESCE(EXCLUDED.source_pages, blues_words.source_pages),
          updated_at    = NOW()`, [JSON.stringify(wordsPayload)]);
        await client.query(`DELETE FROM blues_word_citations WHERE headword = ANY($1::text[])`, [heads]);
        if (citsPayload.length) {
            await client.query(`INSERT INTO blues_word_citations (headword, position, quote, artist, song_title, year)
           SELECT x.headword, x.position, x.quote, x.artist, x.song_title, x.year
             FROM jsonb_to_recordset($1::jsonb) AS x(
               headword   text,
               position   int,
               quote      text,
               artist     text,
               song_title text,
               year       int
             )`, [JSON.stringify(citsPayload)]);
        }
        await client.query("COMMIT");
    }
    catch (e) {
        await client.query("ROLLBACK");
        throw e;
    }
    finally {
        client.release();
    }
    return { upserted: norm.length, citations: citsPayload.length };
}
// Public list/search. q matches headword + definition + citation quote/artist/song_title.
// letter filters by first letter of headword. Returns entries with citations nested.
export async function listBluesWords(opts = {}) {
    const limit = Math.min(Math.max(opts.limit ?? 100, 1), 500);
    const offset = Math.max(opts.offset ?? 0, 0);
    const where = [];
    const args = [];
    const q = (opts.q ?? "").trim();
    if (q) {
        args.push(`%${q.toLowerCase()}%`);
        const p = `$${args.length}`;
        where.push(`(LOWER(w.headword) LIKE ${p} OR LOWER(w.definition) LIKE ${p}
        OR EXISTS (SELECT 1 FROM blues_word_citations c
                    WHERE c.headword = w.headword
                      AND (LOWER(c.quote) LIKE ${p}
                           OR LOWER(c.artist) LIKE ${p}
                           OR LOWER(c.song_title) LIKE ${p})))`);
    }
    const letter = (opts.letter ?? "").trim().toLowerCase();
    if (letter && /^[a-z]$/.test(letter)) {
        args.push(letter);
        where.push(`LEFT(LOWER(w.headword), 1) = $${args.length}`);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const totalR = await getPool().query(`SELECT COUNT(*)::int AS n FROM blues_words w ${whereSql}`, args);
    const total = totalR.rows[0]?.n ?? 0;
    args.push(limit);
    args.push(offset);
    const rowsR = await getPool().query(`SELECT w.headword, w.definition, w.source_volume, w.source_pages, w.updated_at,
            COALESCE(
              (SELECT json_agg(
                 json_build_object(
                   'position', c.position,
                   'quote',    c.quote,
                   'artist',   c.artist,
                   'song',     c.song_title,
                   'year',     c.year
                 ) ORDER BY c.position
               )
               FROM blues_word_citations c
               WHERE c.headword = w.headword),
              '[]'::json
            ) AS citations
       FROM blues_words w
       ${whereSql}
       ORDER BY w.headword ASC
       LIMIT $${args.length - 1} OFFSET $${args.length}`, args);
    return { rows: rowsR.rows, total };
}
export async function getBluesWordLetterCounts() {
    const r = await getPool().query(`SELECT LEFT(LOWER(headword), 1) AS letter, COUNT(*)::int AS n
       FROM blues_words
       GROUP BY 1
       ORDER BY 1`);
    const out = {};
    for (const row of r.rows)
        out[row.letter] = row.n;
    return out;
}
export async function updateBluesWord(headword, patch) {
    const sets = [];
    const args = [];
    if (patch.definition !== undefined) {
        args.push(patch.definition);
        sets.push(`definition = $${args.length}`);
    }
    if (patch.source_volume !== undefined) {
        args.push(patch.source_volume);
        sets.push(`source_volume = $${args.length}`);
    }
    if (patch.source_pages !== undefined) {
        args.push(patch.source_pages);
        sets.push(`source_pages = $${args.length}`);
    }
    if (!sets.length)
        return false;
    args.push(headword.toLowerCase());
    const r = await getPool().query(`UPDATE blues_words SET ${sets.join(", ")}, updated_at = NOW()
      WHERE headword = $${args.length}`, args);
    return (r.rowCount ?? 0) > 0;
}
// Full save from the admin editor: optionally renames the headword,
// replaces definition/source fields, and replaces the citation list.
// Returns the (possibly new) headword on success.
export async function saveBluesWordEntry(originalHeadword, patch) {
    const oldHead = String(originalHeadword || "").trim().toLowerCase();
    const newHead = String(patch.headword || "").trim().toLowerCase();
    if (!oldHead || !newHead)
        return null;
    const renamed = oldHead !== newHead;
    const pool = getPool();
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        // Make sure the old row exists; if not, bail (use ingest for new).
        const existsR = await client.query(`SELECT 1 FROM blues_words WHERE headword = $1`, [oldHead]);
        if (!existsR.rowCount) {
            await client.query("ROLLBACK");
            return null;
        }
        if (renamed) {
            // Block accidental overwrite of a different existing entry.
            const collideR = await client.query(`SELECT 1 FROM blues_words WHERE headword = $1`, [newHead]);
            if (collideR.rowCount) {
                await client.query("ROLLBACK");
                throw new Error(`Headword "${newHead}" already exists`);
            }
            // INSERT new row with patch fields, repoint citations, drop old.
            await client.query(`INSERT INTO blues_words (headword, definition, source_volume, source_pages, created_at, updated_at)
         SELECT $1, $2, $3, $4, created_at, NOW()
           FROM blues_words WHERE headword = $5`, [
                newHead,
                patch.definition ?? "",
                patch.source_volume ?? null,
                patch.source_pages && patch.source_pages.length ? patch.source_pages : null,
                oldHead,
            ]);
            await client.query(`UPDATE blues_word_citations SET headword = $1 WHERE headword = $2`, [newHead, oldHead]);
            await client.query(`DELETE FROM blues_words WHERE headword = $1`, [oldHead]);
        }
        else {
            await client.query(`UPDATE blues_words
            SET definition    = $2,
                source_volume = $3,
                source_pages  = $4,
                updated_at    = NOW()
          WHERE headword = $1`, [
                newHead,
                patch.definition ?? "",
                patch.source_volume ?? null,
                patch.source_pages && patch.source_pages.length ? patch.source_pages : null,
            ]);
        }
        // Full replace of citation list
        await client.query(`DELETE FROM blues_word_citations WHERE headword = $1`, [newHead]);
        const cits = (patch.citations ?? []).filter(c => c && (c.artist || c.song_title || c.quote));
        if (cits.length) {
            const payload = cits.map((c, i) => ({
                headword: newHead,
                position: c.position ?? i + 1,
                quote: c.quote ?? null,
                artist: c.artist ?? null,
                song_title: c.song_title ?? null,
                year: Number.isFinite(c.year) ? c.year : null,
            }));
            await client.query(`INSERT INTO blues_word_citations (headword, position, quote, artist, song_title, year)
           SELECT x.headword, x.position, x.quote, x.artist, x.song_title, x.year
             FROM jsonb_to_recordset($1::jsonb) AS x(
               headword text, position int, quote text, artist text, song_title text, year int
             )`, [JSON.stringify(payload)]);
        }
        await client.query("COMMIT");
        return { headword: newHead, renamed };
    }
    catch (e) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        throw e;
    }
    finally {
        client.release();
    }
}
export async function deleteBluesWord(headword) {
    const r = await getPool().query(`DELETE FROM blues_words WHERE headword = $1`, [headword.toLowerCase()]);
    return (r.rowCount ?? 0) > 0;
}
// ── Invite-only purge: nuke all per-user data for non-admin clerk_user_ids ──
//
// Used when SeaDisco is locked down to a single admin (or invite-only mode).
// Pass the admin's clerk_user_id to keep their rows intact and wipe everyone
// else from every per-user table. Returns row counts per table.
export async function purgeNonAdminUserData(adminClerkId) {
    if (!adminClerkId)
        throw new Error("adminClerkId required");
    // Per-user tables, ordered with FK-children first.
    const tables = [
        "user_order_messages",
        "user_orders",
        "user_list_items",
        "user_lists",
        "user_inventory",
        "user_wantlist",
        "user_collection",
        "user_collection_folders",
        "user_favorites",
        "user_recent_views",
        "user_loc_saves",
        "user_archive_saves",
        "user_youtube_saves",
        "user_wiki_saves",
        "user_play_queue",
        "user_preferences",
        "saved_searches",
        "feedback",
        "oauth_request_tokens",
        "user_tokens", // delete last so foreign references (if any) are gone
    ];
    const counts = {};
    const pool = getPool();
    for (const table of tables) {
        try {
            const r = await pool.query(`DELETE FROM ${table} WHERE clerk_user_id <> $1`, [adminClerkId]);
            counts[table] = r.rowCount ?? 0;
        }
        catch (e) {
            // Table might not exist on a fresh install — record and continue
            counts[table] = -1;
            console.warn(`[purgeNonAdminUserData] ${table}: ${e?.message ?? e}`);
        }
    }
    return counts;
}
// ── Saved searches ──────────────────────────────────────────────────────
export async function getSavedSearches(clerkUserId, view) {
    const sql = view
        ? `SELECT id, view, label, params, created_at as "createdAt" FROM saved_searches WHERE clerk_user_id = $1 AND view = $2 ORDER BY created_at DESC`
        : `SELECT id, view, label, params, created_at as "createdAt" FROM saved_searches WHERE clerk_user_id = $1 ORDER BY created_at DESC`;
    const r = await getPool().query(sql, view ? [clerkUserId, view] : [clerkUserId]);
    return r.rows;
}
export async function saveSavedSearch(clerkUserId, view, label, params) {
    const r = await getPool().query(`INSERT INTO saved_searches (clerk_user_id, view, label, params) VALUES ($1, $2, $3, $4) RETURNING id`, [clerkUserId, view, label, JSON.stringify(params)]);
    return r.rows[0].id;
}
export async function deleteSavedSearch(clerkUserId, id) {
    await getPool().query(`DELETE FROM saved_searches WHERE id = $1 AND clerk_user_id = $2`, [id, clerkUserId]);
}
// ── Random records (all sources combined) ─────────────────────────────────
export async function getRandomRecords(clerkUserId, limit = 192) {
    // Union all sources, deduplicate by release ID, randomize
    const r = await getPool().query(`WITH all_records AS (
      SELECT DISTINCT ON (rid) rid, src, data FROM (
        SELECT discogs_release_id AS rid, 'collection' AS src, data FROM user_collection WHERE clerk_user_id = $1
        UNION ALL
        SELECT discogs_release_id AS rid, 'wantlist' AS src, data FROM user_wantlist WHERE clerk_user_id = $1
        UNION ALL
        SELECT discogs_id AS rid, 'favorites' AS src, data FROM user_favorites WHERE clerk_user_id = $1 AND entity_type = 'release'
        UNION ALL
        SELECT discogs_release_id AS rid, 'inventory' AS src, data FROM user_inventory WHERE clerk_user_id = $1 AND discogs_release_id IS NOT NULL
        UNION ALL
        SELECT discogs_id AS rid, 'list' AS src, data FROM user_list_items WHERE clerk_user_id = $1 AND entity_type = 'release'
      ) combined WHERE rid IS NOT NULL
      ORDER BY rid, src  -- DISTINCT ON needs ORDER BY on the same column
    )
    SELECT rid, src, data FROM all_records ORDER BY RANDOM() LIMIT $2`, [clerkUserId, limit]);
    return r.rows;
}
// ── Favorites ──────────────────────────────────────────────────────────────
export async function getFavoriteIds(clerkUserId) {
    const r = await getPool().query("SELECT discogs_id, entity_type FROM user_favorites WHERE clerk_user_id = $1", [clerkUserId]);
    return r.rows;
}
export async function getFavorites(clerkUserId, limit = 100, offset = 0) {
    const r = await getPool().query("SELECT discogs_id, entity_type, data, created_at FROM user_favorites WHERE clerk_user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3", [clerkUserId, limit, offset]);
    return r.rows;
}
export async function addFavorite(clerkUserId, discogsId, entityType, data) {
    await getPool().query(`INSERT INTO user_favorites (clerk_user_id, discogs_id, entity_type, data)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (clerk_user_id, discogs_id, entity_type) DO UPDATE SET data = $4`, [clerkUserId, discogsId, entityType, JSON.stringify(data)]);
}
export async function removeFavorite(clerkUserId, discogsId, entityType) {
    await getPool().query("DELETE FROM user_favorites WHERE clerk_user_id = $1 AND discogs_id = $2 AND entity_type = $3", [clerkUserId, discogsId, entityType]);
}
export async function getAllFavoriteCounts() {
    const r = await getPool().query("SELECT clerk_user_id, COUNT(*)::int AS count FROM user_favorites GROUP BY clerk_user_id");
    const map = new Map();
    for (const row of r.rows)
        map.set(row.clerk_user_id, row.count);
    return map;
}
export async function getAllUsersSyncStatus() {
    const r = await getPool().query(`SELECT ut.clerk_user_id, ut.discogs_username, ut.collection_synced_at, ut.wantlist_synced_at,
            ut.sync_status, ut.sync_progress, ut.sync_total, ut.sync_error,
            COALESCE(ut.auth_method, 'none') AS auth_method,
            (ut.discogs_token IS NOT NULL AND ut.discogs_token != '' AND ut.discogs_token != '__oauth__') AS has_pat,
            (ut.oauth_access_token IS NOT NULL AND ut.oauth_access_token != '') AS has_oauth
     FROM user_tokens ut
     WHERE ut.discogs_username IS NOT NULL
     ORDER BY ut.discogs_username`);
    return r.rows.map(row => ({
        clerkUserId: row.clerk_user_id,
        username: row.discogs_username,
        collectionSyncedAt: row.collection_synced_at ?? null,
        wantlistSyncedAt: row.wantlist_synced_at ?? null,
        syncStatus: row.sync_status ?? "idle",
        syncProgress: row.sync_progress ?? 0,
        syncTotal: row.sync_total ?? 0,
        syncError: row.sync_error ?? null,
        authMethod: row.auth_method ?? "none",
        hasPat: row.has_pat ?? false,
        hasOAuth: row.has_oauth ?? false,
    }));
}
export async function getPriceStats() {
    const r = await getPool().query(`
    SELECT
      (SELECT COUNT(*) FROM price_cache) AS cache_count,
      (SELECT COUNT(*) FROM price_history) AS history_rows
  `);
    const row = r.rows[0];
    return {
        cacheCount: parseInt(row.cache_count) || 0,
        historyRows: parseInt(row.history_rows) || 0,
    };
}
export async function getAllUsersForSync() {
    const r = await getPool().query(`SELECT clerk_user_id, discogs_token, discogs_username, collection_synced_at, wantlist_synced_at
     FROM user_tokens
     WHERE discogs_token IS NOT NULL AND discogs_username IS NOT NULL`);
    return r.rows.map(row => ({
        clerkUserId: row.clerk_user_id,
        token: row.discogs_token,
        username: row.discogs_username,
        collectionSyncedAt: row.collection_synced_at ?? null,
        wantlistSyncedAt: row.wantlist_synced_at ?? null,
    }));
}
export async function getUserCount() {
    const r = await getPool().query("SELECT COUNT(*)::int AS cnt FROM user_tokens");
    return r.rows[0]?.cnt ?? 0;
}
export async function getActiveUserCount() {
    const r = await getPool().query("SELECT COUNT(*)::int AS cnt FROM user_tokens WHERE hibernated_at IS NULL");
    return r.rows[0]?.cnt ?? 0;
}
export async function touchUserActivity(clerkUserId) {
    await getPool().query("UPDATE user_tokens SET last_active_at = NOW() WHERE clerk_user_id = $1", [clerkUserId]);
}
export async function isUserHibernated(clerkUserId) {
    const r = await getPool().query("SELECT hibernated_at FROM user_tokens WHERE clerk_user_id = $1", [clerkUserId]);
    return r.rows[0]?.hibernated_at != null;
}
export async function reactivateUser(clerkUserId) {
    await getPool().query("UPDATE user_tokens SET hibernated_at = NULL, last_active_at = NOW() WHERE clerk_user_id = $1", [clerkUserId]);
}
export async function hibernateInactiveUsers() {
    const r = await getPool().query(`UPDATE user_tokens
     SET hibernated_at = NOW()
     WHERE hibernated_at IS NULL
       AND last_active_at < NOW() - INTERVAL '90 days'
     RETURNING clerk_user_id`);
    return r.rowCount ?? 0;
}
export async function getUserToken(clerkUserId) {
    const r = await getPool().query("SELECT discogs_token FROM user_tokens WHERE clerk_user_id = $1", [clerkUserId]);
    return r.rows[0]?.discogs_token ?? null;
}
export async function setUserToken(clerkUserId, token) {
    await getPool().query(`INSERT INTO user_tokens (clerk_user_id, discogs_token, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (clerk_user_id)
     DO UPDATE SET discogs_token = $2, updated_at = NOW()`, [clerkUserId, token]);
}
export async function deleteUserToken(clerkUserId) {
    await getPool().query("DELETE FROM user_tokens WHERE clerk_user_id = $1", [clerkUserId]);
}
export async function saveFeedback(clerkUserId, userEmail, message) {
    await getPool().query(`INSERT INTO feedback (clerk_user_id, user_email, message) VALUES ($1, $2, $3)`, [clerkUserId, userEmail, message]);
}
export async function getFeedback() {
    const r = await getPool().query(`SELECT id, user_email, message, created_at FROM feedback ORDER BY created_at DESC`);
    return r.rows;
}
export async function deleteFeedback(id) {
    await getPool().query(`DELETE FROM feedback WHERE id = $1`, [id]);
}
export async function deleteUserData(clerkUserId) {
    const tables = [
        "user_favorites",
        "user_recent_views",
        "user_loc_saves",
        "user_archive_saves",
        "user_youtube_saves",
        "user_wiki_saves",
        "user_play_queue",
        "user_preferences",
        "saved_searches",
        "feedback",
        "user_order_messages",
        "user_orders",
        "user_list_items",
        "user_lists",
        "user_inventory",
        "user_collection_folders",
        "user_collection",
        "user_wantlist",
        "oauth_request_tokens",
        "user_tokens", // last — other tables may reference it
    ];
    for (const table of tables) {
        try {
            await getPool().query(`DELETE FROM ${table} WHERE clerk_user_id = $1`, [clerkUserId]);
        }
        catch { /* table may not exist on fresh install */ }
    }
}
export async function getClerkUserIdByUsername(discogsUsername) {
    const r = await getPool().query("SELECT clerk_user_id FROM user_tokens WHERE discogs_username = $1", [discogsUsername]);
    return r.rows[0]?.clerk_user_id ?? null;
}
export async function getDiscogsUsername(clerkUserId) {
    const r = await getPool().query("SELECT discogs_username FROM user_tokens WHERE clerk_user_id = $1", [clerkUserId]);
    return r.rows[0]?.discogs_username ?? null;
}
export async function setDiscogsUsername(clerkUserId, username) {
    await getPool().query(`UPDATE user_tokens SET discogs_username = $2 WHERE clerk_user_id = $1`, [clerkUserId, username]);
}
// ── OAuth request token helpers (temporary during handshake) ──────────────
export async function storeOAuthRequestToken(token, tokenSecret, clerkUserId, csrfState) {
    await getPool().query(`INSERT INTO oauth_request_tokens (token, token_secret, clerk_user_id, csrf_state) VALUES ($1, $2, $3, $4)
     ON CONFLICT (token) DO UPDATE SET token_secret = $2, clerk_user_id = $3, csrf_state = $4, created_at = NOW()`, [token, tokenSecret, clerkUserId, csrfState ?? null]);
}
export async function getOAuthRequestToken(token) {
    const r = await getPool().query(`SELECT token_secret, clerk_user_id, csrf_state FROM oauth_request_tokens WHERE token = $1`, [token]);
    if (!r.rows[0])
        return null;
    return { tokenSecret: r.rows[0].token_secret, clerkUserId: r.rows[0].clerk_user_id, csrfState: r.rows[0].csrf_state ?? null };
}
export async function deleteOAuthRequestToken(token) {
    await getPool().query(`DELETE FROM oauth_request_tokens WHERE token = $1`, [token]);
}
export async function pruneOAuthRequestTokens() {
    await getPool().query(`DELETE FROM oauth_request_tokens WHERE created_at < NOW() - INTERVAL '15 minutes'`);
}
// ── OAuth credential storage ─────────────────────────────────────────────
export async function setOAuthCredentials(clerkUserId, accessToken, accessSecret) {
    await getPool().query(`UPDATE user_tokens SET oauth_access_token = $2, oauth_access_secret = $3, auth_method = 'oauth', oauth_connected_at = NOW() WHERE clerk_user_id = $1`, [clerkUserId, accessToken, accessSecret]);
}
export async function getOAuthCredentials(clerkUserId) {
    const r = await getPool().query(`SELECT oauth_access_token, oauth_access_secret FROM user_tokens WHERE clerk_user_id = $1 AND auth_method = 'oauth'`, [clerkUserId]);
    if (!r.rows[0]?.oauth_access_token)
        return null;
    return { accessToken: r.rows[0].oauth_access_token, accessSecret: r.rows[0].oauth_access_secret };
}
export async function clearOAuthCredentials(clerkUserId) {
    // Clear OAuth columns and also null out the __oauth__ placeholder token
    await getPool().query(`UPDATE user_tokens
     SET oauth_access_token = NULL, oauth_access_secret = NULL, auth_method = 'pat', oauth_connected_at = NULL,
         discogs_token = CASE WHEN discogs_token = '__oauth__' THEN NULL ELSE discogs_token END
     WHERE clerk_user_id = $1`, [clerkUserId]);
}
export async function getAuthMethod(clerkUserId) {
    const r = await getPool().query(`SELECT auth_method FROM user_tokens WHERE clerk_user_id = $1`, [clerkUserId]);
    return r.rows[0]?.auth_method ?? "pat";
}
// ── Discogs profile cache ────────────────────────────────────────────────
export async function setDiscogsProfile(clerkUserId, userId, avatarUrl, profileData) {
    const currAbbr = profileData?.curr_abbr ?? null;
    await getPool().query(`UPDATE user_tokens
        SET discogs_user_id = $2,
            discogs_avatar_url = $3,
            discogs_profile_data = $4,
            discogs_curr_abbr = COALESCE($5, discogs_curr_abbr),
            profile_synced_at = NOW()
      WHERE clerk_user_id = $1`, [clerkUserId, userId, avatarUrl, JSON.stringify(profileData), currAbbr]);
}
export async function getDiscogsProfile(clerkUserId) {
    const r = await getPool().query(`SELECT discogs_username, discogs_user_id, discogs_avatar_url, discogs_profile_data,
            auth_method, oauth_connected_at, discogs_curr_abbr, profile_synced_at
       FROM user_tokens WHERE clerk_user_id = $1`, [clerkUserId]);
    const row = r.rows[0];
    if (!row)
        return { username: null, userId: null, avatarUrl: null, profileData: null, authMethod: "pat", currAbbr: null, profileSyncedAt: null };
    return {
        username: row.discogs_username,
        userId: row.discogs_user_id,
        avatarUrl: row.discogs_avatar_url,
        profileData: row.discogs_profile_data,
        authMethod: row.auth_method ?? "pat",
        currAbbr: row.discogs_curr_abbr,
        profileSyncedAt: row.profile_synced_at,
    };
}
export async function updateSyncProgress(clerkUserId, status, progress, total, error) {
    await getPool().query(`UPDATE user_tokens SET sync_status = $2, sync_progress = $3, sync_total = $4, sync_error = $5 WHERE clerk_user_id = $1`, [clerkUserId, status, progress, total, error ?? null]);
}
export async function resetAllSyncingStatuses() {
    const r = await getPool().query(`UPDATE user_tokens SET sync_status = 'stopped', sync_error = 'Stopped by admin' WHERE sync_status = 'syncing' RETURNING clerk_user_id`);
    return r.rowCount ?? 0;
}
export async function getSyncStatus(clerkUserId) {
    const r = await getPool().query("SELECT collection_synced_at, wantlist_synced_at, sync_status, sync_progress, sync_total, sync_error FROM user_tokens WHERE clerk_user_id = $1", [clerkUserId]);
    return {
        collectionSyncedAt: r.rows[0]?.collection_synced_at ?? null,
        wantlistSyncedAt: r.rows[0]?.wantlist_synced_at ?? null,
        syncStatus: r.rows[0]?.sync_status ?? "idle",
        syncProgress: r.rows[0]?.sync_progress ?? 0,
        syncTotal: r.rows[0]?.sync_total ?? 0,
        syncError: r.rows[0]?.sync_error ?? null,
    };
}
// Get the most recent added_at date for a user's collection (for incremental sync)
export async function getLatestCollectionAddedAt(clerkUserId) {
    const r = await getPool().query(`SELECT MAX(added_at) AS latest FROM user_collection WHERE clerk_user_id = $1`, [clerkUserId]);
    return r.rows[0]?.latest ?? null;
}
// Get the most recent added_at date for a user's wantlist (for incremental sync)
export async function getLatestWantlistAddedAt(clerkUserId) {
    const r = await getPool().query(`SELECT MAX(added_at) AS latest FROM user_wantlist WHERE clerk_user_id = $1`, [clerkUserId]);
    return r.rows[0]?.latest ?? null;
}
export async function upsertCollectionItems(clerkUserId, items) {
    if (!items.length)
        return;
    // Deduplicate by instance_id within the batch. Items without an instance_id get a
    // synthetic negative id derived from the release_id so they still conflict-check
    // correctly against the (clerk_user_id, instance_id) unique constraint.
    const deduped = new Map();
    for (const item of items) {
        const key = item.instanceId ?? -item.id;
        deduped.set(key, item);
    }
    const ids = [];
    const dataArr = [];
    const addedArr = [];
    const folderArr = [];
    const ratingArr = [];
    const instanceArr = [];
    const notesArr = [];
    for (const [key, item] of deduped) {
        ids.push(item.id);
        dataArr.push(JSON.stringify(item.data));
        addedArr.push(item.addedAt ?? null);
        folderArr.push(item.folderId ?? 0);
        ratingArr.push(item.rating ?? 0);
        instanceArr.push(key);
        notesArr.push(item.notes ? JSON.stringify(item.notes) : null);
    }
    // instance_id is BIGINT (Discogs IDs crossed int4 in May 2026) — the
    // unnest cast for column 7 must be bigint[], otherwise Postgres
    // narrows each element to int4 BEFORE the column accepts it and
    // throws "value … is out of range for type integer". release_id /
    // folder_id / rating stay int — release IDs are ~30M and folder /
    // rating values are tiny.
    await getPool().query(`INSERT INTO user_collection (clerk_user_id, discogs_release_id, data, added_at, synced_at, folder_id, rating, instance_id, notes)
     SELECT $1, unnest($2::int[]), unnest($3::jsonb[]), unnest($4::timestamptz[]), NOW(), unnest($5::int[]), unnest($6::int[]), unnest($7::bigint[]), unnest($8::jsonb[])
     ON CONFLICT (clerk_user_id, instance_id)
     DO UPDATE SET data = EXCLUDED.data, added_at = EXCLUDED.added_at, synced_at = NOW(),
                   folder_id = EXCLUDED.folder_id, rating = EXCLUDED.rating,
                   discogs_release_id = EXCLUDED.discogs_release_id, notes = EXCLUDED.notes`, [clerkUserId, ids, dataArr, addedArr, folderArr, ratingArr, instanceArr, notesArr]);
}
export async function upsertCollectionFolders(clerkUserId, folders) {
    // Clear old folders and re-insert
    await getPool().query(`DELETE FROM user_collection_folders WHERE clerk_user_id = $1`, [clerkUserId]);
    for (const f of folders) {
        await getPool().query(`INSERT INTO user_collection_folders (clerk_user_id, folder_id, folder_name, item_count)
       VALUES ($1, $2, $3, $4)`, [clerkUserId, f.id, f.name, f.count]);
    }
}
export async function renameCollectionFolder(clerkUserId, folderId, newName) {
    await getPool().query(`UPDATE user_collection_folders SET folder_name = $3 WHERE clerk_user_id = $1 AND folder_id = $2`, [clerkUserId, folderId, newName]);
}
export async function deleteCollectionFolder(clerkUserId, folderId) {
    await getPool().query(`DELETE FROM user_collection_folders WHERE clerk_user_id = $1 AND folder_id = $2`, [clerkUserId, folderId]);
    // If the user's default-add folder pointed at this folder, reset it to Uncategorized (1)
    await getPool().query(`UPDATE user_tokens SET default_add_folder_id = 1
      WHERE clerk_user_id = $1 AND default_add_folder_id = $2`, [clerkUserId, folderId]);
}
/** Bulk reassign every collection item in one folder to another (local only). */
export async function moveAllCollectionItemsBetweenFolders(clerkUserId, fromFolderId, toFolderId) {
    const r = await getPool().query(`UPDATE user_collection SET folder_id = $3 WHERE clerk_user_id = $1 AND folder_id = $2 RETURNING 1`, [clerkUserId, fromFolderId, toFolderId]);
    return r.rowCount ?? 0;
}
/** Return every (releaseId, instanceId, folderId) tuple for a user's items in a specific folder. */
export async function getFolderContents(clerkUserId, folderId) {
    const r = await getPool().query(`SELECT discogs_release_id, instance_id FROM user_collection WHERE clerk_user_id = $1 AND folder_id = $2`, [clerkUserId, folderId]);
    return r.rows.map(row => ({
        releaseId: row.discogs_release_id,
        instanceId: row.instance_id != null && row.instance_id > 0 ? row.instance_id : null,
    }));
}
export async function getCollectionFolderList(clerkUserId) {
    // Join against user_collection for a live count so after local rename/move
    // operations the folder pill counts stay accurate without waiting for sync.
    const r = await getPool().query(`SELECT f.folder_id, f.folder_name,
            COALESCE((SELECT COUNT(*)::int FROM user_collection uc
                      WHERE uc.clerk_user_id = f.clerk_user_id AND uc.folder_id = f.folder_id), 0) AS live_count
       FROM user_collection_folders f
      WHERE f.clerk_user_id = $1
      ORDER BY f.folder_name ASC`, [clerkUserId]);
    return r.rows.map(row => ({ folderId: row.folder_id, name: row.folder_name, count: row.live_count }));
}
export async function upsertWantlistItems(clerkUserId, items) {
    if (!items.length)
        return;
    // Deduplicate by release ID within batch
    const deduped = new Map();
    for (const item of items)
        deduped.set(item.id, item);
    const unique = [...deduped.values()];
    const ids = [];
    const dataArr = [];
    const addedArr = [];
    const ratingArr = [];
    const notesArr = [];
    for (const item of unique) {
        ids.push(item.id);
        dataArr.push(JSON.stringify(item.data));
        addedArr.push(item.addedAt ?? null);
        ratingArr.push(item.rating ?? 0);
        notesArr.push(item.notes ? JSON.stringify(item.notes) : null);
    }
    await getPool().query(`INSERT INTO user_wantlist (clerk_user_id, discogs_release_id, data, added_at, synced_at, rating, notes)
     SELECT $1, unnest($2::int[]), unnest($3::jsonb[]), unnest($4::timestamptz[]), NOW(), unnest($5::int[]), unnest($6::jsonb[])
     ON CONFLICT (clerk_user_id, discogs_release_id)
     DO UPDATE SET data = EXCLUDED.data, added_at = EXCLUDED.added_at, synced_at = NOW(),
                   rating = EXCLUDED.rating, notes = EXCLUDED.notes`, [clerkUserId, ids, dataArr, addedArr, ratingArr, notesArr]);
}
// ── Phase 2: Collection/Wantlist action helpers ──────────────────────────
export async function deleteCollectionItem(clerkUserId, releaseId, instanceId) {
    if (instanceId !== undefined && instanceId !== null) {
        await getPool().query(`DELETE FROM user_collection WHERE clerk_user_id = $1 AND discogs_release_id = $2 AND instance_id = $3`, [clerkUserId, releaseId, instanceId]);
    }
    else {
        // No instance_id given — remove all instances of this release (legacy callers)
        await getPool().query(`DELETE FROM user_collection WHERE clerk_user_id = $1 AND discogs_release_id = $2`, [clerkUserId, releaseId]);
    }
}
export async function deleteWantlistItem(clerkUserId, releaseId) {
    await getPool().query(`DELETE FROM user_wantlist WHERE clerk_user_id = $1 AND discogs_release_id = $2`, [clerkUserId, releaseId]);
}
/** Remove local wantlist items that no longer exist in Discogs after a full sync */
export async function pruneWantlistItems(clerkUserId, keepIds) {
    if (!keepIds.length)
        return 0;
    const r = await getPool().query(`DELETE FROM user_wantlist WHERE clerk_user_id = $1 AND discogs_release_id != ALL($2::int[]) RETURNING 1`, [clerkUserId, keepIds]);
    return r.rowCount ?? 0;
}
/** Remove local collection items (by instance_id) that no longer exist in Discogs after a full sync */
export async function pruneCollectionItems(clerkUserId, keepInstanceIds) {
    if (!keepInstanceIds.length)
        return 0;
    // Cast to bigint[] — instance_id is bigint as of the May 2026
    // migration above (Discogs IDs crossed int4 max). An int[] cast
    // would re-overflow here even with the column widened.
    const r = await getPool().query(`DELETE FROM user_collection WHERE clerk_user_id = $1 AND instance_id IS NOT NULL AND instance_id != ALL($2::bigint[]) RETURNING 1`, [clerkUserId, keepInstanceIds]);
    return r.rowCount ?? 0;
}
export async function updateCollectionRating(clerkUserId, releaseId, rating, instanceId) {
    if (instanceId !== undefined && instanceId !== null) {
        await getPool().query(`UPDATE user_collection SET rating = $4 WHERE clerk_user_id = $1 AND discogs_release_id = $2 AND instance_id = $3`, [clerkUserId, releaseId, instanceId, rating]);
    }
    else {
        await getPool().query(`UPDATE user_collection SET rating = $3 WHERE clerk_user_id = $1 AND discogs_release_id = $2`, [clerkUserId, releaseId, rating]);
    }
}
export async function updateCollectionFolder(clerkUserId, releaseId, folderId, instanceId) {
    if (instanceId !== undefined && instanceId !== null) {
        await getPool().query(`UPDATE user_collection SET folder_id = $4 WHERE clerk_user_id = $1 AND discogs_release_id = $2 AND instance_id = $3`, [clerkUserId, releaseId, instanceId, folderId]);
    }
    else {
        await getPool().query(`UPDATE user_collection SET folder_id = $3 WHERE clerk_user_id = $1 AND discogs_release_id = $2`, [clerkUserId, releaseId, folderId]);
    }
}
/** Return the first stored instance for a release (legacy single-instance helper). */
export async function getCollectionInstance(clerkUserId, releaseId) {
    const r = await getPool().query(`SELECT instance_id, folder_id, rating, notes FROM user_collection WHERE clerk_user_id = $1 AND discogs_release_id = $2 ORDER BY instance_id ASC LIMIT 1`, [clerkUserId, releaseId]);
    if (!r.rows[0])
        return null;
    const instId = r.rows[0].instance_id;
    // Hide synthetic negative instance_ids (used as placeholders for legacy rows)
    return {
        instanceId: instId != null && instId > 0 ? instId : null,
        folderId: r.rows[0].folder_id ?? 0,
        rating: r.rows[0].rating ?? 0,
        notes: r.rows[0].notes ?? [],
    };
}
/** Return every stored instance of a release in the user's collection. */
export async function getCollectionInstances(clerkUserId, releaseId) {
    const r = await getPool().query(`SELECT instance_id, folder_id, rating, notes, added_at
       FROM user_collection
      WHERE clerk_user_id = $1 AND discogs_release_id = $2
      ORDER BY added_at ASC NULLS LAST, instance_id ASC`, [clerkUserId, releaseId]);
    return r.rows.map(row => ({
        instanceId: row.instance_id != null && row.instance_id > 0 ? row.instance_id : null,
        folderId: row.folder_id ?? 0,
        rating: row.rating ?? 0,
        notes: row.notes ?? [],
        addedAt: row.added_at ?? null,
    }));
}
export async function updateCollectionNotes(clerkUserId, releaseId, notes, instanceId) {
    if (instanceId !== undefined && instanceId !== null) {
        await getPool().query(`UPDATE user_collection SET notes = $4 WHERE clerk_user_id = $1 AND discogs_release_id = $2 AND instance_id = $3`, [clerkUserId, releaseId, instanceId, JSON.stringify(notes)]);
    }
    else {
        await getPool().query(`UPDATE user_collection SET notes = $3 WHERE clerk_user_id = $1 AND discogs_release_id = $2`, [clerkUserId, releaseId, JSON.stringify(notes)]);
    }
}
export async function updateWantlistNotes(clerkUserId, releaseId, notes) {
    await getPool().query(`UPDATE user_wantlist SET notes = $3 WHERE clerk_user_id = $1 AND discogs_release_id = $2`, [clerkUserId, releaseId, JSON.stringify(notes)]);
}
export async function getWantlistItem(clerkUserId, releaseId) {
    const r = await getPool().query(`SELECT rating, notes FROM user_wantlist WHERE clerk_user_id = $1 AND discogs_release_id = $2`, [clerkUserId, releaseId]);
    if (!r.rows.length)
        return null;
    return { rating: r.rows[0].rating ?? 0, notes: r.rows[0].notes ?? [] };
}
// ── Recent views (cross-device Recent strip) ────────────────────────────
// Per-user cap for the Recent strip. Matches the _HISTORY_MAX constant in
// web/modal.js so the frontend and backend truncate at the same length.
// 576 = 12 pages of 48 cards under the load-more pager.
const RECENT_VIEWS_MAX = 576;
export async function upsertRecentView(clerkUserId, discogsId, entityType, data) {
    const pool = getPool();
    await pool.query(`INSERT INTO user_recent_views (clerk_user_id, discogs_id, entity_type, data, opened_at)
     VALUES ($1, $2, $3, $4, NOW())
     ON CONFLICT (clerk_user_id, discogs_id, entity_type)
     DO UPDATE SET data = EXCLUDED.data, opened_at = NOW()`, [clerkUserId, discogsId, entityType, JSON.stringify(data)]);
    // Trim to last RECENT_VIEWS_MAX rows for this user. Cheap — the index on
    // (clerk_user_id, opened_at DESC) makes the subquery a quick range scan.
    await pool.query(`DELETE FROM user_recent_views
     WHERE clerk_user_id = $1
       AND (discogs_id, entity_type) NOT IN (
         SELECT discogs_id, entity_type FROM user_recent_views
         WHERE clerk_user_id = $1
         ORDER BY opened_at DESC
         LIMIT ${RECENT_VIEWS_MAX}
       )`, [clerkUserId]);
}
export async function getRecentViews(clerkUserId, limit = RECENT_VIEWS_MAX) {
    const capped = Math.min(Math.max(1, limit), RECENT_VIEWS_MAX);
    const r = await getPool().query(`SELECT discogs_id, entity_type, data, opened_at
     FROM user_recent_views
     WHERE clerk_user_id = $1
     ORDER BY opened_at DESC
     LIMIT $2`, [clerkUserId, capped]);
    return r.rows.map(row => ({
        id: row.discogs_id,
        type: row.entity_type,
        data: row.data ?? {},
        openedAt: row.opened_at,
    }));
}
export async function deleteRecentView(clerkUserId, discogsId, entityType) {
    if (entityType) {
        await getPool().query(`DELETE FROM user_recent_views WHERE clerk_user_id = $1 AND discogs_id = $2 AND entity_type = $3`, [clerkUserId, discogsId, entityType]);
    }
    else {
        await getPool().query(`DELETE FROM user_recent_views WHERE clerk_user_id = $1 AND discogs_id = $2`, [clerkUserId, discogsId]);
    }
}
export async function clearRecentViews(clerkUserId) {
    await getPool().query(`DELETE FROM user_recent_views WHERE clerk_user_id = $1`, [clerkUserId]);
}
// ── LOC audio saves ──────────────────────────────────────────────────────
// Hard cap on how many LOC items a single user can keep saved. Protects
// the table from unbounded growth if someone mass-stars the entire
// National Jukebox. At 1000 items the total JSONB is still well under
// 10 MB per user.
const LOC_SAVES_MAX_PER_USER = 1000;
export async function saveLocItem(clerkUserId, locId, title, streamUrl, data) {
    const pool = getPool();
    await pool.query(`INSERT INTO user_loc_saves (clerk_user_id, loc_id, title, stream_url, data, saved_at)
     VALUES ($1, $2, $3, $4, $5, NOW())
     ON CONFLICT (clerk_user_id, loc_id)
     DO UPDATE SET title = EXCLUDED.title, stream_url = EXCLUDED.stream_url,
                   data = EXCLUDED.data, saved_at = NOW()`, [clerkUserId, locId, title, streamUrl, JSON.stringify(data)]);
    // Trim oldest rows if the user is above the cap. Uses the user+time
    // index so the subquery is a cheap range scan.
    await pool.query(`DELETE FROM user_loc_saves
     WHERE clerk_user_id = $1
       AND loc_id NOT IN (
         SELECT loc_id FROM user_loc_saves
         WHERE clerk_user_id = $1
         ORDER BY saved_at DESC
         LIMIT ${LOC_SAVES_MAX_PER_USER}
       )`, [clerkUserId]);
}
export async function getLocSaves(clerkUserId, limit = 500) {
    const capped = Math.min(Math.max(1, limit), 1000);
    const r = await getPool().query(`SELECT loc_id, title, stream_url, data, saved_at
     FROM user_loc_saves
     WHERE clerk_user_id = $1
     ORDER BY saved_at DESC
     LIMIT $2`, [clerkUserId, capped]);
    return r.rows.map(row => ({
        locId: row.loc_id,
        title: row.title,
        streamUrl: row.stream_url,
        data: row.data ?? {},
        savedAt: row.saved_at,
    }));
}
export async function deleteLocSave(clerkUserId, locId) {
    await getPool().query(`DELETE FROM user_loc_saves WHERE clerk_user_id = $1 AND loc_id = $2`, [clerkUserId, locId]);
}
export async function getLocSaveIds(clerkUserId) {
    const r = await getPool().query(`SELECT loc_id FROM user_loc_saves WHERE clerk_user_id = $1`, [clerkUserId]);
    return r.rows.map(row => row.loc_id);
}
// ── Archive.org item saves ────────────────────────────────────────────
// Same shape and cap as LOC saves; just a different table. Archive is
// admin-only on the wire — only admin users can hit the saves
// endpoints — but the DB layer doesn't enforce that (the API does).
const ARCHIVE_SAVES_MAX_PER_USER = 1000;
export async function saveArchiveItem(clerkUserId, archiveId, title, streamUrl, data) {
    const pool = getPool();
    await pool.query(`INSERT INTO user_archive_saves (clerk_user_id, archive_id, title, stream_url, data, saved_at)
     VALUES ($1, $2, $3, $4, $5, NOW())
     ON CONFLICT (clerk_user_id, archive_id)
     DO UPDATE SET title = EXCLUDED.title, stream_url = EXCLUDED.stream_url,
                   data = EXCLUDED.data, saved_at = NOW()`, [clerkUserId, archiveId, title, streamUrl, JSON.stringify(data)]);
    await pool.query(`DELETE FROM user_archive_saves
     WHERE clerk_user_id = $1
       AND archive_id NOT IN (
         SELECT archive_id FROM user_archive_saves
         WHERE clerk_user_id = $1
         ORDER BY saved_at DESC
         LIMIT ${ARCHIVE_SAVES_MAX_PER_USER}
       )`, [clerkUserId]);
}
export async function getArchiveSaves(clerkUserId, limit = 500) {
    const capped = Math.min(Math.max(1, limit), 1000);
    const r = await getPool().query(`SELECT archive_id, title, stream_url, data, saved_at
     FROM user_archive_saves
     WHERE clerk_user_id = $1
     ORDER BY saved_at DESC
     LIMIT $2`, [clerkUserId, capped]);
    return r.rows.map(row => ({
        archiveId: row.archive_id,
        title: row.title,
        streamUrl: row.stream_url,
        data: row.data ?? {},
        savedAt: row.saved_at,
    }));
}
export async function deleteArchiveSave(clerkUserId, archiveId) {
    await getPool().query(`DELETE FROM user_archive_saves WHERE clerk_user_id = $1 AND archive_id = $2`, [clerkUserId, archiveId]);
}
export async function getArchiveSaveIds(clerkUserId) {
    const r = await getPool().query(`SELECT archive_id FROM user_archive_saves WHERE clerk_user_id = $1`, [clerkUserId]);
    return r.rows.map(row => row.archive_id);
}
// ── YouTube video saves ───────────────────────────────────────────────
const YOUTUBE_SAVES_MAX_PER_USER = 1000;
export async function saveYoutubeVideo(clerkUserId, videoId, title, channel, thumbnail, data) {
    const pool = getPool();
    await pool.query(`INSERT INTO user_youtube_saves (clerk_user_id, video_id, title, channel, thumbnail, data, saved_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())
     ON CONFLICT (clerk_user_id, video_id)
     DO UPDATE SET title = EXCLUDED.title, channel = EXCLUDED.channel,
                   thumbnail = EXCLUDED.thumbnail, data = EXCLUDED.data, saved_at = NOW()`, [clerkUserId, videoId, title, channel, thumbnail, JSON.stringify(data)]);
    await pool.query(`DELETE FROM user_youtube_saves
     WHERE clerk_user_id = $1
       AND video_id NOT IN (
         SELECT video_id FROM user_youtube_saves
         WHERE clerk_user_id = $1
         ORDER BY saved_at DESC
         LIMIT ${YOUTUBE_SAVES_MAX_PER_USER}
       )`, [clerkUserId]);
}
export async function getYoutubeSaves(clerkUserId, limit = 500) {
    const capped = Math.min(Math.max(1, limit), 1000);
    const r = await getPool().query(`SELECT video_id, title, channel, thumbnail, data, saved_at
     FROM user_youtube_saves
     WHERE clerk_user_id = $1
     ORDER BY saved_at DESC
     LIMIT $2`, [clerkUserId, capped]);
    return r.rows.map(row => ({
        videoId: row.video_id,
        title: row.title,
        channel: row.channel,
        thumbnail: row.thumbnail,
        data: row.data ?? {},
        savedAt: row.saved_at,
    }));
}
export async function deleteYoutubeSave(clerkUserId, videoId) {
    await getPool().query(`DELETE FROM user_youtube_saves WHERE clerk_user_id = $1 AND video_id = $2`, [clerkUserId, videoId]);
}
export async function getYoutubeSaveIds(clerkUserId) {
    const r = await getPool().query(`SELECT video_id FROM user_youtube_saves WHERE clerk_user_id = $1`, [clerkUserId]);
    return r.rows.map(row => row.video_id);
}
// ── Wikipedia article saves ───────────────────────────────────────────
// Mirrors the LOC saves API. The Wikipedia title is the natural primary
// key (canonical, stable). Cap matches LOC's so a user can't accumulate
// more than 1000 saved articles.
const WIKI_SAVES_MAX_PER_USER = 1000;
export async function saveWikiArticle(clerkUserId, title, url, snippet, thumbnail, data) {
    const pool = getPool();
    await pool.query(`INSERT INTO user_wiki_saves (clerk_user_id, wiki_title, wiki_url, snippet, thumbnail, data, saved_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())
     ON CONFLICT (clerk_user_id, wiki_title)
     DO UPDATE SET wiki_url = EXCLUDED.wiki_url, snippet = EXCLUDED.snippet,
                   thumbnail = EXCLUDED.thumbnail, data = EXCLUDED.data, saved_at = NOW()`, [clerkUserId, title, url, snippet, thumbnail, JSON.stringify(data)]);
    await pool.query(`DELETE FROM user_wiki_saves
     WHERE clerk_user_id = $1
       AND wiki_title NOT IN (
         SELECT wiki_title FROM user_wiki_saves
         WHERE clerk_user_id = $1
         ORDER BY saved_at DESC
         LIMIT ${WIKI_SAVES_MAX_PER_USER}
       )`, [clerkUserId]);
}
export async function getWikiSaves(clerkUserId, limit = 500) {
    const capped = Math.min(Math.max(1, limit), 1000);
    const r = await getPool().query(`SELECT wiki_title, wiki_url, snippet, thumbnail, data, saved_at
     FROM user_wiki_saves
     WHERE clerk_user_id = $1
     ORDER BY saved_at DESC
     LIMIT $2`, [clerkUserId, capped]);
    return r.rows.map(row => ({
        title: row.wiki_title,
        url: row.wiki_url,
        snippet: row.snippet,
        thumbnail: row.thumbnail,
        data: row.data ?? {},
        savedAt: row.saved_at,
    }));
}
export async function deleteWikiSave(clerkUserId, title) {
    await getPool().query(`DELETE FROM user_wiki_saves WHERE clerk_user_id = $1 AND wiki_title = $2`, [clerkUserId, title]);
}
export async function getWikiSaveIds(clerkUserId) {
    const r = await getPool().query(`SELECT wiki_title FROM user_wiki_saves WHERE clerk_user_id = $1`, [clerkUserId]);
    return r.rows.map(row => row.wiki_title);
}
// ── Chronicling America (historic newspapers) saves ─────────────────────
// Mirrors the wiki saves API. chronam_id is the canonical relative path
// returned by the LOC API ("/lccn/<lccn>/<date>/ed-X/seq-N/"). Cap per
// user matches wiki/loc/archive saves at 1000.
const CHRONAM_SAVES_MAX_PER_USER = 1000;
export async function saveChronAmItem(clerkUserId, chronamId, paperTitle, issueDate, snippet, thumbnail, data) {
    const pool = getPool();
    await pool.query(`INSERT INTO user_chronam_saves (clerk_user_id, chronam_id, paper_title, issue_date, snippet, thumbnail, data, saved_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
     ON CONFLICT (clerk_user_id, chronam_id)
     DO UPDATE SET paper_title = EXCLUDED.paper_title, issue_date = EXCLUDED.issue_date,
                   snippet = EXCLUDED.snippet, thumbnail = EXCLUDED.thumbnail,
                   data = EXCLUDED.data, saved_at = NOW()`, [clerkUserId, chronamId, paperTitle, issueDate, snippet, thumbnail, JSON.stringify(data)]);
    await pool.query(`DELETE FROM user_chronam_saves
      WHERE clerk_user_id = $1
        AND chronam_id NOT IN (
          SELECT chronam_id FROM user_chronam_saves
           WHERE clerk_user_id = $1
           ORDER BY saved_at DESC
           LIMIT ${CHRONAM_SAVES_MAX_PER_USER}
        )`, [clerkUserId]);
}
export async function getChronAmSaves(clerkUserId, limit = 500) {
    const capped = Math.min(Math.max(1, limit), 1000);
    const r = await getPool().query(`SELECT chronam_id, paper_title, issue_date, snippet, thumbnail, data, saved_at
       FROM user_chronam_saves
      WHERE clerk_user_id = $1
      ORDER BY saved_at DESC
      LIMIT $2`, [clerkUserId, capped]);
    return r.rows.map(row => ({
        id: row.chronam_id,
        paperTitle: row.paper_title,
        issueDate: row.issue_date,
        snippet: row.snippet,
        thumbnail: row.thumbnail,
        data: row.data ?? {},
        savedAt: row.saved_at,
    }));
}
export async function deleteChronAmSave(clerkUserId, chronamId) {
    await getPool().query(`DELETE FROM user_chronam_saves WHERE clerk_user_id = $1 AND chronam_id = $2`, [clerkUserId, chronamId]);
}
export async function getChronAmSaveIds(clerkUserId) {
    const r = await getPool().query(`SELECT chronam_id FROM user_chronam_saves WHERE clerk_user_id = $1`, [clerkUserId]);
    return r.rows.map(row => row.chronam_id);
}
// Persistent search cache. Historic newspaper data effectively never
// changes, so a 30-day TTL is conservative — older rows are simply
// treated as expired and a fresh upstream call refreshes them.
const CHRONAM_SEARCH_CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000;
export async function getChronAmSearchCache(cacheKey) {
    try {
        const r = await getPool().query(`SELECT data, cached_at FROM chronam_search_cache WHERE cache_key = $1`, [cacheKey]);
        if (!r.rows.length)
            return null;
        const at = new Date(r.rows[0].cached_at).getTime();
        if (Date.now() - at > CHRONAM_SEARCH_CACHE_TTL_MS)
            return null;
        return r.rows[0].data;
    }
    catch {
        return null;
    }
}
// Stale fallback — returns ANY cached row for a key regardless of TTL.
// Used by the chronam search endpoint when the loc.gov upstream times
// out or errors: serving slightly-old historic-newspaper results is
// way better than returning a 504 to the user. Returns { data, cachedAt }
// so the endpoint can surface "served stale, last refreshed N ago".
export async function getChronAmSearchCacheStale(cacheKey) {
    try {
        const r = await getPool().query(`SELECT data, cached_at FROM chronam_search_cache WHERE cache_key = $1`, [cacheKey]);
        if (!r.rows.length)
            return null;
        return { data: r.rows[0].data, cachedAt: new Date(r.rows[0].cached_at) };
    }
    catch {
        return null;
    }
}
export async function setChronAmSearchCache(cacheKey, data) {
    try {
        await getPool().query(`INSERT INTO chronam_search_cache (cache_key, data, cached_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (cache_key)
       DO UPDATE SET data = EXCLUDED.data, cached_at = NOW()`, [cacheKey, JSON.stringify(data)]);
    }
    catch { /* best-effort cache write */ }
}
// ── Play queue (cross-source: LOC + YouTube) ─────────────────────────
const PLAY_QUEUE_MAX = 500;
export async function getPlayQueue(clerkUserId) {
    const r = await getPool().query(`SELECT position, source, external_id, data
     FROM user_play_queue
     WHERE clerk_user_id = $1
     ORDER BY position ASC`, [clerkUserId]);
    return r.rows.map(row => ({
        position: row.position,
        source: row.source,
        externalId: row.external_id,
        data: row.data ?? {},
    }));
}
// Add items to the queue. mode "next" inserts at the head (positions
// 1..N) and shifts existing items down by N. mode "append" puts them
// at the tail (MAX(position)+1..). Caps total at 500; head trims when
// exceeded. Both modes are transactional so concurrent adds stay
// monotonic. Default mode is "next" — single-track ➕ buttons want
// "I want this NOW" semantics; album bulk-adds pass mode "append".
export async function appendPlayQueue(clerkUserId, items, opts) {
    if (!items.length)
        return { added: 0, firstPosition: 0 };
    const mode = opts?.mode === "append" ? "append" : "next";
    const pool = getPool();
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        // Serialize all multi-statement queue writes for this user. Both
        // this and reorderPlayQueue do DELETE-then-INSERT bursts on the
        // same (clerk_user_id, position) rows; run concurrently they can
        // deadlock and Postgres aborts one — surfacing to the client as an
        // intermittent "reorder failed" that snaps back. The advisory lock
        // (namespaced with a constant so it can't collide with other
        // advisory locks) makes them queue instead. Auto-released on
        // COMMIT/ROLLBACK.
        await client.query(`SELECT pg_advisory_xact_lock(hashtext($1), 424242)`, [clerkUserId]);
        // De-dupe: re-adding a track that's already in the queue is a MOVE,
        // not a stack. Delete any existing rows whose external_id matches an
        // incoming item BEFORE positioning the new copies. Without this the
        // server kept both copies; the client render-dedup hid the old one
        // (lowest position wins) but it stayed live in the queue, so
        // play/next nav still hit the stale copy — the queue appeared to
        // "jump back" to where the track already was. The anon localStorage
        // path already de-dupes this way; this brings the server in line.
        const incomingExtIds = Array.from(new Set(items.map(it => String(it.externalId))));
        if (incomingExtIds.length) {
            await client.query(`DELETE FROM user_play_queue WHERE clerk_user_id = $1 AND external_id = ANY($2::text[])`, [clerkUserId, incomingExtIds]);
        }
        let startPos;
        if (mode === "append") {
            const r = await client.query(`SELECT COALESCE(MAX(position), 0) AS maxp FROM user_play_queue WHERE clerk_user_id = $1`, [clerkUserId]);
            startPos = (r.rows[0]?.maxp ?? 0) + 1;
        }
        else {
            // "next": shift all existing positions down by items.length so
            // the new items can occupy 1..N. The naive
            //   UPDATE ... SET position = position + $2
            // hits a unique-constraint violation in PostgreSQL because the
            // PK (clerk_user_id, position) is checked per-row mid-statement
            // — going from [1, 2, 3] → [3, 4, 5] tries to write 3 while 3
            // still exists. Workaround: two passes through a negative
            // intermediate range that can't collide with valid positive
            // positions:
            //   pass 1: position → -position - 1   ([1,2,3] → [-2,-3,-4])
            //   pass 2: position → -position - 1 + N ([-2,-3,-4] → [N+1,…])
            // Final: existing rows shifted down by N, slots 1..N free.
            await client.query(`UPDATE user_play_queue SET position = -position - 1 WHERE clerk_user_id = $1`, [clerkUserId]);
            await client.query(`UPDATE user_play_queue SET position = -position - 1 + $2 WHERE clerk_user_id = $1`, [clerkUserId, items.length]);
            startPos = 1;
        }
        const values = [];
        const placeholders = [];
        items.forEach((it, i) => {
            const p = startPos + i;
            placeholders.push(`($1, $${values.length + 2}, $${values.length + 3}, $${values.length + 4}, $${values.length + 5})`);
            values.push(p, it.source, it.externalId, JSON.stringify(it.data ?? {}));
        });
        await client.query(`INSERT INTO user_play_queue (clerk_user_id, position, source, external_id, data) VALUES ${placeholders.join(", ")}
       ON CONFLICT (clerk_user_id, position) DO UPDATE SET source = EXCLUDED.source, external_id = EXCLUDED.external_id, data = EXCLUDED.data, added_at = NOW()`, [clerkUserId, ...values]);
        // Trim the cap (drops oldest tail entries beyond PLAY_QUEUE_MAX).
        await client.query(`DELETE FROM user_play_queue
       WHERE clerk_user_id = $1
         AND position NOT IN (
           SELECT position FROM user_play_queue
           WHERE clerk_user_id = $1
           ORDER BY position ASC
           LIMIT ${PLAY_QUEUE_MAX}
         )`, [clerkUserId]);
        await client.query("COMMIT");
        return { added: items.length, firstPosition: startPos };
    }
    catch (err) {
        await client.query("ROLLBACK");
        throw err;
    }
    finally {
        client.release();
    }
}
export async function removeFromPlayQueue(clerkUserId, position) {
    await getPool().query(`DELETE FROM user_play_queue WHERE clerk_user_id = $1 AND position = $2`, [clerkUserId, position]);
}
export async function clearPlayQueue(clerkUserId) {
    const r = await getPool().query(`DELETE FROM user_play_queue WHERE clerk_user_id = $1`, [clerkUserId]);
    return r.rowCount ?? 0;
}
// Reorder by sending the full ordered list of positions. Server rewrites
// positions in a transaction so concurrent updates stay consistent.
export async function reorderPlayQueue(clerkUserId, orderedPositions) {
    if (!orderedPositions.length)
        return;
    const pool = getPool();
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        // Same per-user serialization as appendPlayQueue (see note there) —
        // prevents reorder/add deadlocks that bubbled up as a flaky
        // "reorder failed" + snap-back.
        await client.query(`SELECT pg_advisory_xact_lock(hashtext($1), 424242)`, [clerkUserId]);
        const r = await client.query(`SELECT position, source, external_id, data FROM user_play_queue WHERE clerk_user_id = $1`, [clerkUserId]);
        const byPos = new Map();
        r.rows.forEach(row => byPos.set(row.position, row));
        await client.query(`DELETE FROM user_play_queue WHERE clerk_user_id = $1`, [clerkUserId]);
        let newPos = 1;
        for (const oldPos of orderedPositions) {
            const row = byPos.get(oldPos);
            if (!row)
                continue;
            await client.query(`INSERT INTO user_play_queue (clerk_user_id, position, source, external_id, data) VALUES ($1, $2, $3, $4, $5)`, [clerkUserId, newPos, row.source, row.external_id, JSON.stringify(row.data ?? {})]);
            newPos++;
        }
        await client.query("COMMIT");
    }
    catch (err) {
        await client.query("ROLLBACK");
        throw err;
    }
    finally {
        client.release();
    }
}
// ── User playlists ─────────────────────────────────────────────────────
const PLAYLIST_NAME_MAX = 80;
const PLAYLIST_ITEMS_MAX = 500; // same cap as the live queue
const PLAYLISTS_PER_USER = 100; // soft cap per user
// Create a playlist owned by clerkUserId from a snapshot of items.
// Caller passes the items in display order. Returns the new id.
export async function createPlaylist(clerkUserId, name, items) {
    const trimmedName = (name || "").trim().slice(0, PLAYLIST_NAME_MAX) || "Untitled playlist";
    const capped = items.slice(0, PLAYLIST_ITEMS_MAX);
    const pool = getPool();
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        // Soft cap: refuse new playlists past PLAYLISTS_PER_USER. The
        // CHECK is a count, not a delete-the-oldest sweep — we'd rather
        // tell the user "too many playlists, delete some" than silently
        // garbage-collect things they made.
        const countRow = await client.query(`SELECT COUNT(*)::int AS n FROM user_playlists WHERE clerk_user_id = $1`, [clerkUserId]);
        if ((countRow.rows[0]?.n ?? 0) >= PLAYLISTS_PER_USER) {
            throw new Error(`Playlist limit reached (${PLAYLISTS_PER_USER}). Delete one before saving another.`);
        }
        const r = await client.query(`INSERT INTO user_playlists (clerk_user_id, name) VALUES ($1, $2) RETURNING id`, [clerkUserId, trimmedName]);
        const id = r.rows[0].id;
        if (capped.length) {
            const values = [];
            const placeholders = [];
            capped.forEach((it, i) => {
                const base = values.length;
                placeholders.push(`($1, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
                values.push(i + 1, it.source, it.externalId, JSON.stringify(it.data ?? {}));
            });
            await client.query(`INSERT INTO user_playlist_items (playlist_id, position, source, external_id, data) VALUES ${placeholders.join(", ")}`, [id, ...values]);
        }
        await client.query("COMMIT");
        return id;
    }
    catch (err) {
        await client.query("ROLLBACK");
        throw err;
    }
    finally {
        client.release();
    }
}
// List one user's playlists — name + counts, no item bodies (the
// drawer picker just needs labels).
export async function listPlaylists(clerkUserId) {
    const r = await getPool().query(`SELECT p.id, p.name, p.created_at, p.updated_at,
            COALESCE(c.n, 0) AS item_count
       FROM user_playlists p
       LEFT JOIN (
         SELECT playlist_id, COUNT(*)::int AS n
           FROM user_playlist_items
          GROUP BY playlist_id
       ) c ON c.playlist_id = p.id
      WHERE p.clerk_user_id = $1
      ORDER BY p.updated_at DESC`, [clerkUserId]);
    return r.rows;
}
// Public: fetch a playlist by id including all items in order. Returns
// null if not found. Owner clerk_user_id is included so the caller can
// gate edits — read-side gating is owner-agnostic by design (shareable).
export async function getPlaylist(id) {
    const head = await getPool().query(`SELECT id, name, clerk_user_id, created_at, updated_at
       FROM user_playlists WHERE id = $1`, [id]);
    if (!head.rows.length)
        return null;
    const itemsRows = await getPool().query(`SELECT position, source, external_id, data
       FROM user_playlist_items
      WHERE playlist_id = $1
      ORDER BY position ASC`, [id]);
    return {
        id: head.rows[0].id,
        name: head.rows[0].name,
        clerk_user_id: head.rows[0].clerk_user_id,
        created_at: head.rows[0].created_at,
        updated_at: head.rows[0].updated_at,
        items: itemsRows.rows.map(r => ({
            position: r.position,
            source: r.source,
            externalId: r.external_id,
            data: r.data ?? {},
        })),
    };
}
// Owner-only rename. Returns true if a row was updated.
export async function renamePlaylist(id, clerkUserId, name) {
    const trimmed = (name || "").trim().slice(0, PLAYLIST_NAME_MAX);
    if (!trimmed)
        return false;
    const r = await getPool().query(`UPDATE user_playlists SET name = $3, updated_at = NOW()
      WHERE id = $1 AND clerk_user_id = $2`, [id, clerkUserId, trimmed]);
    return (r.rowCount ?? 0) > 0;
}
// Owner-only delete. ON DELETE CASCADE on the items table sweeps
// the children automatically.
export async function deletePlaylist(id, clerkUserId) {
    const r = await getPool().query(`DELETE FROM user_playlists WHERE id = $1 AND clerk_user_id = $2`, [id, clerkUserId]);
    return (r.rowCount ?? 0) > 0;
}
// Owner-only items replace. Used by the "overwrite this playlist"
// flow on save: keep the same id (and share-URL), wipe items, write
// the new ones, bump updated_at. Optionally renames the playlist
// in the same transaction. Returns false if the playlist isn't
// the caller's.
export async function replacePlaylistItems(id, clerkUserId, items, newName) {
    const capped = items.slice(0, PLAYLIST_ITEMS_MAX);
    const pool = getPool();
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        // Owner check + optional rename. UPDATE with RETURNING gives us
        // both ownership confirmation AND the row id in one round-trip.
        const ownerCheck = newName
            ? await client.query(`UPDATE user_playlists SET name = $3, updated_at = NOW()
            WHERE id = $1 AND clerk_user_id = $2
            RETURNING id`, [id, clerkUserId, newName.trim().slice(0, PLAYLIST_NAME_MAX) || "Untitled playlist"])
            : await client.query(`UPDATE user_playlists SET updated_at = NOW()
            WHERE id = $1 AND clerk_user_id = $2
            RETURNING id`, [id, clerkUserId]);
        if (!ownerCheck.rows.length) {
            await client.query("ROLLBACK");
            return false;
        }
        await client.query(`DELETE FROM user_playlist_items WHERE playlist_id = $1`, [id]);
        if (capped.length) {
            const values = [];
            const placeholders = [];
            capped.forEach((it, i) => {
                const base = values.length;
                placeholders.push(`($1, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
                values.push(i + 1, it.source, it.externalId, JSON.stringify(it.data ?? {}));
            });
            await client.query(`INSERT INTO user_playlist_items (playlist_id, position, source, external_id, data) VALUES ${placeholders.join(", ")}`, [id, ...values]);
        }
        await client.query("COMMIT");
        return true;
    }
    catch (e) {
        await client.query("ROLLBACK").catch(() => { });
        throw e;
    }
    finally {
        client.release();
    }
}
// ── Phase 4: Price intelligence DB functions ─────────────────────────────
export async function upsertPriceCache(releaseId, lowest, median, highest, numForSale, currency = "USD") {
    await getPool().query(`INSERT INTO price_cache (discogs_release_id, lowest_price, median_price, highest_price, num_for_sale, currency, fetched_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())
     ON CONFLICT (discogs_release_id, currency)
     DO UPDATE SET lowest_price = $2, median_price = $3, highest_price = $4, num_for_sale = $5, fetched_at = NOW()`, [releaseId, lowest, median, highest, numForSale, currency]);
}
export async function appendPriceHistory(releaseId, lowest, median, highest, numForSale, currency = "USD") {
    // Only one entry per release per day
    const existing = await getPool().query(`SELECT id FROM price_history WHERE discogs_release_id = $1 AND currency = $2 AND recorded_at > NOW() - INTERVAL '20 hours'`, [releaseId, currency]);
    if (existing.rows.length > 0)
        return;
    await getPool().query(`INSERT INTO price_history (discogs_release_id, lowest_price, median_price, highest_price, num_for_sale, currency)
     VALUES ($1, $2, $3, $4, $5, $6)`, [releaseId, lowest, median, highest, numForSale, currency]);
}
export async function getPriceCache(releaseId, currency = "USD") {
    const r = await getPool().query(`SELECT lowest_price, median_price, highest_price, num_for_sale, fetched_at FROM price_cache WHERE discogs_release_id = $1 AND currency = $2`, [releaseId, currency]);
    if (!r.rows[0])
        return null;
    return { lowest: r.rows[0].lowest_price, median: r.rows[0].median_price, highest: r.rows[0].highest_price, numForSale: r.rows[0].num_for_sale, fetchedAt: r.rows[0].fetched_at };
}
export async function getPriceHistory(releaseId, currency = "USD", days = 90) {
    const r = await getPool().query(`SELECT median_price, lowest_price, highest_price, recorded_at FROM price_history
     WHERE discogs_release_id = $1 AND currency = $2 AND recorded_at > NOW() - make_interval(days => $3)
     ORDER BY recorded_at ASC`, [releaseId, currency, days]);
    return r.rows.map(row => ({ median: row.median_price, lowest: row.lowest_price, highest: row.highest_price, recordedAt: row.recorded_at }));
}
export async function getStaleReleaseIds(limit = 100) {
    // Get unique release IDs from all collections where price is stale (>24h) or missing
    const r = await getPool().query(`SELECT uc.discogs_release_id, MIN(pc.fetched_at) AS oldest
     FROM user_collection uc
     LEFT JOIN price_cache pc ON pc.discogs_release_id = uc.discogs_release_id
     WHERE pc.fetched_at IS NULL OR pc.fetched_at < NOW() - INTERVAL '24 hours'
     GROUP BY uc.discogs_release_id
     ORDER BY oldest ASC NULLS FIRST
     LIMIT $1`, [limit]);
    return r.rows.map(row => row.discogs_release_id);
}
export async function prunePriceHistory() {
    // Keep max 1 year of history
    await getPool().query(`DELETE FROM price_history WHERE recorded_at < NOW() - INTERVAL '365 days'`);
}
function cwOrderBy(sort) {
    switch (sort) {
        case "title": return `ORDER BY LOWER(data->>'title') ASC, LOWER(data->'artists'->0->>'name') ASC`;
        case "year": return `ORDER BY (data->>'year') DESC NULLS LAST, LOWER(data->'artists'->0->>'name') ASC`;
        case "year_asc": return `ORDER BY (data->>'year') ASC NULLS LAST, LOWER(data->'artists'->0->>'name') ASC`;
        case "added": return `ORDER BY added_at DESC NULLS LAST, id DESC`;
        case "added_asc": return `ORDER BY added_at ASC NULLS LAST, id ASC`;
        case "rating": return `ORDER BY rating DESC NULLS LAST, LOWER(data->'artists'->0->>'name') ASC`;
        default: return `ORDER BY LOWER(data->'artists'->0->>'name') ASC, LOWER(data->>'title') ASC`;
    }
}
// Parse a filter value with operators:  + (AND),  | (OR),  - prefix (NOT)
// e.g. "miles davis + john coltrane" → both must match
// e.g. "-verve" → must NOT match verve
// e.g. "miles davis | john coltrane" → either matches
// e.g. "blue note + -verve | columbia" → (blue note AND NOT verve) OR columbia
function parseFilterExpr(value, column, startIdx) {
    const orBranches = value.split(/\s*\|\s*/);
    const orClauses = [];
    const params = [];
    let idx = startIdx;
    for (const branch of orBranches) {
        const terms = branch.split(/\s*\+\s*/);
        const andClauses = [];
        for (let term of terms) {
            term = term.trim();
            if (!term)
                continue;
            if (term.startsWith("-") && term.length > 1) {
                // NOT: exclude this term
                andClauses.push(`${column} NOT ILIKE $${idx}`);
                params.push(`%${term.slice(1).trim()}%`);
            }
            else {
                andClauses.push(`${column} ILIKE $${idx}`);
                params.push(`%${term}%`);
            }
            idx++;
        }
        if (andClauses.length) {
            orClauses.push(andClauses.length === 1 ? andClauses[0] : `(${andClauses.join(" AND ")})`);
        }
    }
    const clause = orClauses.length === 0 ? ""
        : orClauses.length === 1 ? orClauses[0]
            : `(${orClauses.join(" OR ")})`;
    return { clause, params, nextIdx: idx };
}
// Like parseFilterExpr but expands classical music synonyms on positive terms
function parseFilterExprWithSynonyms(value, column, startIdx) {
    const orBranches = value.split(/\s*\|\s*/);
    const orClauses = [];
    const params = [];
    const synonymsApplied = [];
    let idx = startIdx;
    for (const branch of orBranches) {
        const terms = branch.split(/\s*\+\s*/);
        const andClauses = [];
        for (let term of terms) {
            term = term.trim();
            if (!term)
                continue;
            if (term.startsWith("-") && term.length > 1) {
                // NOT: exclude this term — do NOT expand synonyms for negations
                andClauses.push(`${column} NOT ILIKE $${idx}`);
                params.push(`%${term.slice(1).trim()}%`);
                idx++;
            }
            else {
                // Positive term: expand with synonyms
                const { variants, applied } = expandWithSynonyms(term);
                const likeClauses = [`${column} ILIKE $${idx}`];
                params.push(`%${term}%`);
                idx++;
                for (const variant of variants) {
                    likeClauses.push(`${column} ILIKE $${idx}`);
                    params.push(`%${variant}%`);
                    idx++;
                }
                if (applied.length)
                    synonymsApplied.push(...applied);
                andClauses.push(likeClauses.length === 1 ? likeClauses[0] : `(${likeClauses.join(" OR ")})`);
            }
        }
        if (andClauses.length) {
            orClauses.push(andClauses.length === 1 ? andClauses[0] : `(${andClauses.join(" AND ")})`);
        }
    }
    const clause = orClauses.length === 0 ? ""
        : orClauses.length === 1 ? orClauses[0]
            : `(${orClauses.join(" OR ")})`;
    return { clause, params, nextIdx: idx, synonymsApplied };
}
function buildCwWhere(filters, startIdx) {
    const clauses = [];
    const allParams = [];
    const allSynonyms = [];
    let idx = startIdx;
    const useSynonyms = filters.synonyms !== false;
    // Fields eligible for synonym expansion (q and release/title)
    const synonymFields = new Set(["data::text", "data->>'title'"]);
    const fields = [
        [filters.q, "data::text"],
        [filters.artist, "(data->'artists')::text"],
        [filters.release, "data->>'title'"],
        [filters.label, "(data->'labels')::text"],
        [filters.year, "(data->>'year')::text"],
        [filters.genre, "(data->'genres')::text"],
        [filters.style, "(data->'styles')::text"],
        [filters.format, "(data->'formats')::text"],
    ];
    for (const [value, column] of fields) {
        if (!value)
            continue;
        if (useSynonyms && synonymFields.has(column)) {
            const { clause, params, nextIdx, synonymsApplied } = parseFilterExprWithSynonyms(value, column, idx);
            if (clause) {
                clauses.push(clause);
                allParams.push(...params);
                allSynonyms.push(...synonymsApplied);
                idx = nextIdx;
            }
        }
        else {
            const { clause, params, nextIdx } = parseFilterExpr(value, column, idx);
            if (clause) {
                clauses.push(clause);
                allParams.push(...params);
                idx = nextIdx;
            }
        }
    }
    // Folder filter (exact match on integer column)
    if (filters.folderId !== undefined && filters.folderId > 0) {
        clauses.push(`folder_id = $${idx}`);
        allParams.push(filters.folderId);
        idx++;
    }
    // Rating filter
    if (filters.ratingUnrated) {
        clauses.push(`(rating IS NULL OR rating = 0)`);
    }
    else if (filters.ratingMin && filters.ratingMin >= 1 && filters.ratingMin <= 5) {
        clauses.push(`rating >= $${idx}`);
        allParams.push(filters.ratingMin);
        idx++;
    }
    // Type filter: "master" = has master_id, "release" = no master_id (standalone release)
    if (filters.type === "master") {
        clauses.push(`(data->>'master_id') IS NOT NULL AND (data->>'master_id')::int > 0`);
    }
    else if (filters.type === "release") {
        clauses.push(`((data->>'master_id') IS NULL OR (data->>'master_id')::int = 0)`);
    }
    // Notes text search
    if (filters.notes) {
        clauses.push(`notes::text ILIKE $${idx}`);
        allParams.push(`%${filters.notes}%`);
        idx++;
    }
    // Deduplicate synonym descriptions
    const uniqueSynonyms = [...new Set(allSynonyms)];
    return { clause: clauses.length ? " AND " + clauses.join(" AND ") : "", params: allParams, synonymsApplied: uniqueSynonyms };
}
export async function getCollectionPage(clerkUserId, page, perPage, filters) {
    const offset = (page - 1) * perPage;
    const { clause: dataClause, params: dataFilterParams, synonymsApplied } = buildCwWhere(filters ?? {}, 4);
    const { clause: countClause, params: countFilterParams } = buildCwWhere(filters ?? {}, 2);
    const orderBy = cwOrderBy(filters?.sort);
    // A release may have multiple instances (multiple copies owned). Collapse to one
    // card per release in the grid via DISTINCT ON, picking the highest-rated/most-
    // recently-added instance as representative. Also surface a per-release instance
    // count so the UI can show "×N copies" badges.
    const [dataR, countR] = await Promise.all([
        getPool().query(`SELECT data, rating, notes, instance_count FROM (
         SELECT DISTINCT ON (discogs_release_id)
                discogs_release_id,
                data,
                rating,
                notes,
                added_at,
                id,
                COUNT(*) OVER (PARTITION BY discogs_release_id) AS instance_count
           FROM user_collection
          WHERE clerk_user_id = $1${dataClause}
          ORDER BY discogs_release_id, rating DESC NULLS LAST, added_at DESC NULLS LAST, id DESC
       ) sub
       ${orderBy}
       LIMIT $2 OFFSET $3`, [clerkUserId, perPage, offset, ...dataFilterParams]),
        getPool().query(`SELECT COUNT(DISTINCT discogs_release_id)::int AS total FROM user_collection WHERE clerk_user_id = $1${countClause}`, [clerkUserId, ...countFilterParams]),
    ]);
    return {
        items: dataR.rows.map(r => ({
            ...r.data,
            _rating: r.rating ?? 0,
            _notes: r.notes ?? [],
            _instanceCount: r.instance_count ?? 1,
        })),
        total: countR.rows[0]?.total ?? 0,
        synonymsApplied: synonymsApplied.length ? synonymsApplied : undefined,
    };
}
export async function getAllCollectionItems(clerkUserId) {
    const r = await getPool().query(`SELECT data, folder_id FROM user_collection WHERE clerk_user_id = $1
     ORDER BY LOWER(data->'artists'->0->>'name') ASC, LOWER(data->>'title') ASC`, [clerkUserId]);
    return r.rows;
}
export async function getAllWantlistItems(clerkUserId) {
    const r = await getPool().query(`SELECT data FROM user_wantlist WHERE clerk_user_id = $1
     ORDER BY LOWER(data->'artists'->0->>'name') ASC, LOWER(data->>'title') ASC`, [clerkUserId]);
    return r.rows;
}
export async function getWantlistPage(clerkUserId, page, perPage, filters) {
    const offset = (page - 1) * perPage;
    const { clause: dataClause, params: dataFilterParams, synonymsApplied } = buildCwWhere(filters ?? {}, 4);
    const { clause: countClause, params: countFilterParams } = buildCwWhere(filters ?? {}, 2);
    const orderBy = cwOrderBy(filters?.sort);
    const [dataR, countR] = await Promise.all([
        getPool().query(`SELECT data, rating, notes FROM user_wantlist WHERE clerk_user_id = $1${dataClause}
       ${orderBy}
       LIMIT $2 OFFSET $3`, [clerkUserId, perPage, offset, ...dataFilterParams]),
        getPool().query(`SELECT COUNT(*)::int AS total FROM user_wantlist WHERE clerk_user_id = $1${countClause}`, [clerkUserId, ...countFilterParams]),
    ]);
    return {
        items: dataR.rows.map(r => ({ ...r.data, _rating: r.rating ?? 0, _notes: r.notes ?? [] })),
        total: countR.rows[0]?.total ?? 0,
        synonymsApplied: synonymsApplied.length ? synonymsApplied : undefined,
    };
}
export async function getCollectionFacets(clerkUserId, genre) {
    const stylesQuery = genre
        ? `SELECT DISTINCT s AS name FROM user_collection, jsonb_array_elements_text(data->'styles') AS s WHERE clerk_user_id = $1 AND (data->'genres')::text ILIKE $2 ORDER BY s`
        : `SELECT DISTINCT s AS name FROM user_collection, jsonb_array_elements_text(data->'styles') AS s WHERE clerk_user_id = $1 ORDER BY s`;
    const stylesParams = genre ? [clerkUserId, `%${genre}%`] : [clerkUserId];
    const [genresR, stylesR] = await Promise.all([
        getPool().query(`SELECT DISTINCT g AS name FROM user_collection, jsonb_array_elements_text(data->'genres') AS g WHERE clerk_user_id = $1 ORDER BY g`, [clerkUserId]),
        getPool().query(stylesQuery, stylesParams),
    ]);
    return { genres: genresR.rows.map(r => r.name), styles: stylesR.rows.map(r => r.name) };
}
export async function getWantlistFacets(clerkUserId, genre) {
    const stylesQuery = genre
        ? `SELECT DISTINCT s AS name FROM user_wantlist, jsonb_array_elements_text(data->'styles') AS s WHERE clerk_user_id = $1 AND (data->'genres')::text ILIKE $2 ORDER BY s`
        : `SELECT DISTINCT s AS name FROM user_wantlist, jsonb_array_elements_text(data->'styles') AS s WHERE clerk_user_id = $1 ORDER BY s`;
    const stylesParams = genre ? [clerkUserId, `%${genre}%`] : [clerkUserId];
    const [genresR, stylesR] = await Promise.all([
        getPool().query(`SELECT DISTINCT g AS name FROM user_wantlist, jsonb_array_elements_text(data->'genres') AS g WHERE clerk_user_id = $1 ORDER BY g`, [clerkUserId]),
        getPool().query(stylesQuery, stylesParams),
    ]);
    return { genres: genresR.rows.map(r => r.name), styles: stylesR.rows.map(r => r.name) };
}
export async function getCollectionIds(clerkUserId) {
    const r = await getPool().query("SELECT DISTINCT discogs_release_id FROM user_collection WHERE clerk_user_id = $1", [clerkUserId]);
    return r.rows.map(row => row.discogs_release_id);
}
/**
 * Returns a map of discogs_release_id → instance_count for releases the user owns
 * more than one copy of. Used to render the "(N)" badge on card thumbnails.
 */
export async function getCollectionMultiInstanceCounts(clerkUserId) {
    const r = await getPool().query(`SELECT discogs_release_id, COUNT(*)::int AS n
       FROM user_collection
      WHERE clerk_user_id = $1
      GROUP BY discogs_release_id
     HAVING COUNT(*) > 1`, [clerkUserId]);
    const out = {};
    for (const row of r.rows)
        out[row.discogs_release_id] = row.n;
    return out;
}
export async function getDefaultAddFolderId(clerkUserId) {
    const r = await getPool().query(`SELECT default_add_folder_id FROM user_tokens WHERE clerk_user_id = $1`, [clerkUserId]);
    const v = r.rows[0]?.default_add_folder_id;
    return Number.isFinite(Number(v)) && Number(v) > 0 ? Number(v) : 1;
}
export async function setDefaultAddFolderId(clerkUserId, folderId) {
    const fid = Number(folderId);
    if (!Number.isFinite(fid) || fid < 1)
        throw new Error("Invalid folder id");
    await getPool().query(`UPDATE user_tokens SET default_add_folder_id = $2 WHERE clerk_user_id = $1`, [clerkUserId, fid]);
}
export async function getWantedSample(limit = 24, excludeIds = []) {
    // Distribute evenly across users: each user contributes at most ceil(limit/userCount) items
    const uc = await getPool().query(`SELECT COUNT(DISTINCT clerk_user_id)::int AS n FROM user_wantlist`);
    const userCount = Math.max(uc.rows[0]?.n ?? 1, 1);
    const perUser = Math.ceil(limit / userCount);
    const excludeClause = excludeIds.length
        ? `AND discogs_release_id != ALL($3)`
        : "";
    const params = [perUser, limit];
    if (excludeIds.length)
        params.push(excludeIds);
    const r = await getPool().query(`WITH ranked AS (
       SELECT data, discogs_release_id,
              ROW_NUMBER() OVER (PARTITION BY clerk_user_id ORDER BY RANDOM()) AS rn
       FROM user_wantlist
       WHERE 1=1 ${excludeClause}
     )
     SELECT data FROM ranked
     WHERE rn <= $1
     ORDER BY RANDOM()
     LIMIT $2`, params);
    return r.rows.map(row => row.data);
}
export async function getWantedItems() {
    const r = await getPool().query(`SELECT clerk_user_id, discogs_release_id, data FROM user_wantlist`);
    // Group by user
    const byUser = new Map();
    for (const row of r.rows) {
        if (!byUser.has(row.clerk_user_id))
            byUser.set(row.clerk_user_id, []);
        byUser.get(row.clerk_user_id).push({ id: row.discogs_release_id, data: row.data });
    }
    // Fisher-Yates shuffle helper
    const shuffle = (arr) => {
        for (let i = arr.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [arr[i], arr[j]] = [arr[j], arr[i]];
        }
        return arr;
    };
    // Shuffle each user's list independently
    const userLists = Array.from(byUser.values()).map(items => shuffle(items));
    // Round-robin interleave, deduping by release_id
    const seen = new Set();
    const result = [];
    const maxLen = Math.max(...userLists.map(l => l.length));
    outer: for (let i = 0; i < maxLen; i++) {
        for (const list of userLists) {
            if (i < list.length && !seen.has(list[i].id)) {
                seen.add(list[i].id);
                result.push(list[i].data);
                if (result.length >= 500)
                    break outer;
            }
        }
    }
    return result;
}
export async function getWantlistIds(clerkUserId) {
    const r = await getPool().query("SELECT discogs_release_id FROM user_wantlist WHERE clerk_user_id = $1", [clerkUserId]);
    return r.rows.map(row => row.discogs_release_id);
}
/**
 * Returns master_id → distinct release count for the user's collection.
 * Used to render the "N" inside C/W dot indicators on master-type
 * search cards so the user can see at a glance "you already own 2
 * pressings of this master". Releases without a master_id (orphans)
 * are skipped. Multiple instances of the same release count as 1.
 */
export async function getCollectionMasterCounts(clerkUserId) {
    const r = await getPool().query(`SELECT (data->>'master_id')::int AS master_id,
            COUNT(DISTINCT discogs_release_id)::int AS n
       FROM user_collection
      WHERE clerk_user_id = $1
        AND data ? 'master_id'
        AND data->>'master_id' IS NOT NULL
        AND data->>'master_id' <> '0'
        AND data->>'master_id' <> ''
      GROUP BY (data->>'master_id')::int`, [clerkUserId]);
    const out = {};
    for (const row of r.rows) {
        if (row.master_id)
            out[row.master_id] = row.n;
    }
    return out;
}
/** Same as getCollectionMasterCounts but for the wantlist. */
export async function getWantlistMasterCounts(clerkUserId) {
    const r = await getPool().query(`SELECT (data->>'master_id')::int AS master_id,
            COUNT(DISTINCT discogs_release_id)::int AS n
       FROM user_wantlist
      WHERE clerk_user_id = $1
        AND data ? 'master_id'
        AND data->>'master_id' IS NOT NULL
        AND data->>'master_id' <> '0'
        AND data->>'master_id' <> ''
      GROUP BY (data->>'master_id')::int`, [clerkUserId]);
    const out = {};
    for (const row of r.rows) {
        if (row.master_id)
            out[row.master_id] = row.n;
    }
    return out;
}
export async function updateCollectionSyncedAt(clerkUserId) {
    await getPool().query("UPDATE user_tokens SET collection_synced_at = NOW() WHERE clerk_user_id = $1", [clerkUserId]);
}
export async function updateWantlistSyncedAt(clerkUserId) {
    await getPool().query("UPDATE user_tokens SET wantlist_synced_at = NOW() WHERE clerk_user_id = $1", [clerkUserId]);
}
// ── Inventory (marketplace listings) ──────────────────────────────────────
export async function upsertInventoryItems(clerkUserId, items) {
    if (!items.length)
        return;
    // Dedupe by listingId within the batch (keep last occurrence)
    const deduped = [...new Map(items.map(i => [i.listingId, i])).values()];
    const CHUNK = 50;
    for (let i = 0; i < deduped.length; i += CHUNK) {
        const chunk = deduped.slice(i, i + CHUNK);
        const values = [];
        const params = [];
        let idx = 1;
        for (const item of chunk) {
            values.push(`($${idx}, $${idx + 1}, $${idx + 2}, $${idx + 3}, $${idx + 4}, $${idx + 5}, $${idx + 6}, $${idx + 7}, $${idx + 8}, $${idx + 9}, NOW())`);
            params.push(clerkUserId, item.listingId, item.releaseId ?? null, JSON.stringify(item.data), item.status ?? "For Sale", item.priceValue ?? null, item.priceCurrency ?? "USD", item.condition ?? null, item.sleeveCondition ?? null, item.postedAt ?? null);
            idx += 10;
        }
        await getPool().query(`INSERT INTO user_inventory (clerk_user_id, listing_id, discogs_release_id, data, status, price_value, price_currency, condition, sleeve_condition, posted_at, synced_at)
       VALUES ${values.join(", ")}
       ON CONFLICT (clerk_user_id, listing_id)
       DO UPDATE SET data = EXCLUDED.data, status = EXCLUDED.status, price_value = EXCLUDED.price_value, price_currency = EXCLUDED.price_currency, condition = EXCLUDED.condition, sleeve_condition = EXCLUDED.sleeve_condition, posted_at = EXCLUDED.posted_at, synced_at = NOW()`, params);
    }
}
export async function updateInventorySyncedAt(clerkUserId) {
    await getPool().query("UPDATE user_tokens SET inventory_synced_at = NOW() WHERE clerk_user_id = $1", [clerkUserId]);
}
export async function getInventoryCount(clerkUserId) {
    const r = await getPool().query("SELECT COUNT(*)::int AS cnt FROM user_inventory WHERE clerk_user_id = $1", [clerkUserId]);
    return r.rows[0]?.cnt ?? 0;
}
export async function getInventoryPage(clerkUserId, page = 1, perPage = 24, filters) {
    const conditions = ["clerk_user_id = $1"];
    const params = [clerkUserId];
    let idx = 2;
    let synonymsApplied = [];
    if (filters?.q) {
        const useSyn = filters.synonyms !== false;
        if (useSyn) {
            const { variants, applied } = expandWithSynonyms(filters.q);
            const likeClauses = [`data::text ILIKE $${idx}`];
            params.push(`%${filters.q}%`);
            idx++;
            for (const v of variants) {
                likeClauses.push(`data::text ILIKE $${idx}`);
                params.push(`%${v}%`);
                idx++;
            }
            conditions.push(`(${likeClauses.join(" OR ")})`);
            synonymsApplied = applied;
        }
        else {
            conditions.push(`(data::text ILIKE $${idx})`);
            params.push(`%${filters.q}%`);
            idx++;
        }
    }
    if (filters?.status) {
        conditions.push(`status = $${idx}`);
        params.push(filters.status);
        idx++;
    }
    const where = conditions.join(" AND ");
    const countR = await getPool().query(`SELECT COUNT(*)::int AS cnt FROM user_inventory WHERE ${where}`, params);
    const total = countR.rows[0]?.cnt ?? 0;
    const offset = (page - 1) * perPage;
    params.push(perPage, offset);
    const r = await getPool().query(`SELECT listing_id, discogs_release_id, data, status, price_value, price_currency, condition, sleeve_condition, posted_at
     FROM user_inventory WHERE ${where} ORDER BY posted_at DESC NULLS LAST LIMIT $${idx} OFFSET $${idx + 1}`, params);
    return { items: r.rows, total, synonymsApplied: synonymsApplied.length ? synonymsApplied : undefined };
}
export async function getUserListsList(clerkUserId) {
    const r = await getPool().query(`SELECT ul.list_id, ul.name, ul.description,
            COUNT(uli.id)::int AS item_count,
            ul.is_public, ul.synced_at
     FROM user_lists ul
     LEFT JOIN user_list_items uli
       ON ul.clerk_user_id = uli.clerk_user_id AND ul.list_id = uli.list_id
     WHERE ul.clerk_user_id = $1
     GROUP BY ul.list_id, ul.name, ul.description, ul.is_public, ul.synced_at
     ORDER BY ul.name`, [clerkUserId]);
    return r.rows;
}
// ── Lists ────────────────────────────────────────────────────────────────
export async function upsertUserLists(clerkUserId, lists) {
    for (const list of lists) {
        await getPool().query(`INSERT INTO user_lists (clerk_user_id, list_id, name, description, item_count, is_public, data, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
       ON CONFLICT (clerk_user_id, list_id)
       DO UPDATE SET name = $3, description = $4, item_count = $5, is_public = $6, data = $7, synced_at = NOW()`, [clerkUserId, list.listId, list.name, list.description ?? null, list.itemCount ?? 0, list.isPublic ?? true, list.data ? JSON.stringify(list.data) : null]);
    }
}
// ── List items ──────────────────────────────────────────────────────────
export async function upsertListItems(clerkUserId, listId, items) {
    if (!items.length)
        return;
    // Remove old items for this list, then insert fresh
    await getPool().query(`DELETE FROM user_list_items WHERE clerk_user_id = $1 AND list_id = $2`, [clerkUserId, listId]);
    for (const item of items) {
        await getPool().query(`INSERT INTO user_list_items (clerk_user_id, list_id, discogs_id, entity_type, comment, data, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW())
       ON CONFLICT (clerk_user_id, list_id, discogs_id) DO UPDATE SET entity_type = $4, comment = $5, data = $6, synced_at = NOW()`, [clerkUserId, listId, item.discogsId, item.entityType ?? "release", item.comment ?? null, item.data ? JSON.stringify(item.data) : null]);
    }
}
export async function getListItems(clerkUserId, listId) {
    const r = await getPool().query(`SELECT discogs_id, entity_type, comment, data FROM user_list_items WHERE clerk_user_id = $1 AND list_id = $2 ORDER BY id`, [clerkUserId, listId]);
    return r.rows;
}
/** Returns { discogsId → [{ listId, listName }] } for badge rendering */
export async function getListMembership(clerkUserId) {
    const r = await getPool().query(`SELECT li.discogs_id, li.list_id, l.name
     FROM user_list_items li
     JOIN user_lists l ON l.clerk_user_id = li.clerk_user_id AND l.list_id = li.list_id
     WHERE li.clerk_user_id = $1`, [clerkUserId]);
    const map = {};
    for (const row of r.rows) {
        if (!map[row.discogs_id])
            map[row.discogs_id] = [];
        map[row.discogs_id].push({ listId: row.list_id, listName: row.name });
    }
    return map;
}
export async function getInventoryIds(clerkUserId) {
    const r = await getPool().query("SELECT DISTINCT discogs_release_id FROM user_inventory WHERE clerk_user_id = $1 AND discogs_release_id IS NOT NULL", [clerkUserId]);
    return r.rows.map(row => row.discogs_release_id);
}
/**
 * Returns a map of `releaseId → [InventoryListingSummary, ...]` for all of
 * the user's cached marketplace listings. Used to hydrate the release-modal
 * "Listed" tooltip and the inventory-link badges on cards without needing
 * an extra round-trip.
 */
export async function getInventoryListingIdsByRelease(clerkUserId) {
    const r = await getPool().query(`SELECT discogs_release_id, listing_id, status, price_value, price_currency,
            condition, sleeve_condition, data, posted_at
     FROM user_inventory
     WHERE clerk_user_id = $1 AND discogs_release_id IS NOT NULL
     ORDER BY posted_at DESC NULLS LAST`, [clerkUserId]);
    const map = {};
    for (const row of r.rows) {
        const rid = Number(row.discogs_release_id);
        if (!map[rid])
            map[rid] = [];
        // Pull comments and posted date out of the full listing JSON. The JSON
        // blob is refreshed on every sync/write so it's the most reliable source,
        // whereas the dedicated `posted_at` column can be stale if an earlier
        // sync pass persisted a NULL or a touch-timestamp by mistake.
        let comments = null;
        let postedFromJson = null;
        try {
            const d = row.data;
            if (d) {
                const obj = typeof d === "string" ? JSON.parse(d) : d;
                comments = obj?.comments ?? null;
                if (obj?.posted)
                    postedFromJson = String(obj.posted);
            }
        }
        catch { }
        const postedIso = postedFromJson
            ? new Date(postedFromJson).toISOString()
            : (row.posted_at ? new Date(row.posted_at).toISOString() : null);
        map[rid].push({
            id: Number(row.listing_id),
            status: row.status ?? null,
            price: row.price_value != null ? Number(row.price_value) : null,
            currency: row.price_currency ?? null,
            condition: row.condition ?? null,
            sleeve: row.sleeve_condition ?? null,
            comments,
            posted_at: postedIso,
        });
    }
    return map;
}
export async function getInventoryItem(clerkUserId, listingId) {
    const r = await getPool().query(`SELECT listing_id, discogs_release_id, data, status, price_value, price_currency, condition, sleeve_condition, posted_at, synced_at
     FROM user_inventory WHERE clerk_user_id = $1 AND listing_id = $2`, [clerkUserId, listingId]);
    return r.rows[0] ?? null;
}
export async function deleteInventoryItem(clerkUserId, listingId) {
    await getPool().query(`DELETE FROM user_inventory WHERE clerk_user_id = $1 AND listing_id = $2`, [clerkUserId, listingId]);
}
export async function getListItemStats(clerkUserId) {
    const r = await getPool().query(`SELECT COUNT(*)::int AS total_items, COUNT(DISTINCT list_id)::int AS lists_with_items FROM user_list_items WHERE clerk_user_id = $1`, [clerkUserId]);
    return { totalItems: r.rows[0]?.total_items ?? 0, listsWithItems: r.rows[0]?.lists_with_items ?? 0 };
}
// ── Orders (seller-side marketplace orders) ───────────────────────────────
export async function upsertUserOrders(clerkUserId, orders) {
    for (const order of orders) {
        await getPool().query(`INSERT INTO user_orders (clerk_user_id, order_id, status, buyer_username, seller_username, total_value, total_currency, item_count, created_at, data, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
       ON CONFLICT (clerk_user_id, order_id)
       DO UPDATE SET status = $3, buyer_username = $4, seller_username = $5, total_value = $6, total_currency = $7, item_count = $8, created_at = $9, data = $10, synced_at = NOW()`, [clerkUserId, order.orderId, order.status ?? null, order.buyerUsername ?? null, order.sellerUsername ?? null, order.totalValue ?? null, order.totalCurrency ?? "USD", order.itemCount ?? 0, order.createdAt ?? null, order.data ? JSON.stringify(order.data) : null]);
    }
}
export async function updateOrdersSyncedAt(clerkUserId) {
    await getPool().query("UPDATE user_tokens SET orders_synced_at = NOW() WHERE clerk_user_id = $1", [clerkUserId]);
}
export async function getOrdersCount(clerkUserId) {
    const r = await getPool().query("SELECT COUNT(*)::int AS cnt FROM user_orders WHERE clerk_user_id = $1", [clerkUserId]);
    return r.rows[0]?.cnt ?? 0;
}
export async function getUserOrdersPage(clerkUserId, page = 1, perPage = 25, filters) {
    const conditions = ["clerk_user_id = $1"];
    const params = [clerkUserId];
    let idx = 2;
    if (filters?.status) {
        conditions.push(`status = $${idx}`);
        params.push(filters.status);
        idx++;
    }
    if (filters?.q) {
        conditions.push(`(buyer_username ILIKE $${idx} OR order_id ILIKE $${idx} OR data::text ILIKE $${idx})`);
        params.push(`%${filters.q}%`);
        idx++;
    }
    const where = conditions.join(" AND ");
    const countR = await getPool().query(`SELECT COUNT(*)::int AS cnt FROM user_orders WHERE ${where}`, params);
    const total = countR.rows[0]?.cnt ?? 0;
    const offset = (page - 1) * perPage;
    params.push(perPage, offset);
    const r = await getPool().query(`SELECT order_id, status, buyer_username, seller_username, total_value, total_currency, item_count, created_at, data, synced_at, viewed_at,
            CASE
              WHEN (data->>'last_activity') IS NULL THEN false
              WHEN viewed_at IS NULL THEN true
              WHEN (data->>'last_activity')::timestamptz > viewed_at THEN true
              ELSE false
            END AS has_new
     FROM user_orders WHERE ${where} ORDER BY created_at DESC NULLS LAST LIMIT $${idx} OFFSET $${idx + 1}`, params);
    return { items: r.rows, total };
}
export async function getUserOrder(clerkUserId, orderId) {
    const r = await getPool().query(`SELECT order_id, status, buyer_username, seller_username, total_value, total_currency, item_count, created_at, data, synced_at, viewed_at
     FROM user_orders WHERE clerk_user_id = $1 AND order_id = $2`, [clerkUserId, orderId]);
    return r.rows[0] ?? null;
}
export async function markOrderViewed(clerkUserId, orderId) {
    await getPool().query(`UPDATE user_orders SET viewed_at = NOW() WHERE clerk_user_id = $1 AND order_id = $2`, [clerkUserId, orderId]);
}
export async function getUnreadOrdersCount(clerkUserId) {
    const r = await getPool().query(`SELECT COUNT(*)::int AS cnt FROM user_orders
      WHERE clerk_user_id = $1
        AND (data->>'last_activity') IS NOT NULL
        AND (viewed_at IS NULL OR (data->>'last_activity')::timestamptz > viewed_at)`, [clerkUserId]);
    return r.rows[0]?.cnt ?? 0;
}
export async function upsertOrderMessages(clerkUserId, orderId, messages) {
    if (!messages.length)
        return;
    // Replace the whole thread for a given order (simpler than merging)
    await getPool().query(`DELETE FROM user_order_messages WHERE clerk_user_id = $1 AND order_id = $2`, [clerkUserId, orderId]);
    for (const m of messages) {
        await getPool().query(`INSERT INTO user_order_messages (clerk_user_id, order_id, message_order, subject, message, from_user, ts, data, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
       ON CONFLICT (clerk_user_id, order_id, message_order)
       DO UPDATE SET subject = $4, message = $5, from_user = $6, ts = $7, data = $8, synced_at = NOW()`, [clerkUserId, orderId, m.order, m.subject ?? null, m.message ?? null, m.fromUser ?? null, m.ts ?? null, m.data ? JSON.stringify(m.data) : null]);
    }
}
export async function getOrderMessages(clerkUserId, orderId) {
    const r = await getPool().query(`SELECT message_order, subject, message, from_user, ts, data
     FROM user_order_messages WHERE clerk_user_id = $1 AND order_id = $2 ORDER BY message_order ASC`, [clerkUserId, orderId]);
    return r.rows;
}
// ── Auto-prune stale data ─────────────────────────────────────────────────
export async function pruneAllStaleData() {
    const interval30d = `NOW() - INTERVAL '30 days'`;
    // User collection older than 30 days
    const col = await getPool().query(`DELETE FROM user_collection WHERE synced_at < ${interval30d}`);
    // User wantlist older than 30 days
    const wl = await getPool().query(`DELETE FROM user_wantlist WHERE synced_at < ${interval30d}`);
    // user_collection_folders has no synced_at column (and was never going
    // to — folders are user-scoped metadata, lifecycle tied to the user).
    // The old query here was erroring daily in the Postgres logs. Folder
    // rows are tiny and already removed via the user-deletion cascade, so
    // dropping the prune step entirely. fld stays at 0 so the return-shape
    // is unchanged.
    const fld = { rowCount: 0 };
    // User inventory older than 30 days
    const inv = await getPool().query(`DELETE FROM user_inventory WHERE synced_at < ${interval30d}`);
    // User list items older than 30 days
    const lsti = await getPool().query(`DELETE FROM user_list_items WHERE synced_at < ${interval30d}`);
    // User lists older than 30 days
    const lst = await getPool().query(`DELETE FROM user_lists WHERE synced_at < ${interval30d}`);
    // User orders older than 30 days
    const ord = await getPool().query(`DELETE FROM user_orders WHERE synced_at < ${interval30d}`);
    return {
        collection: col.rowCount ?? 0,
        wantlist: wl.rowCount ?? 0,
        folders: fld.rowCount ?? 0,
        inventory: inv.rowCount ?? 0,
        listItems: lsti.rowCount ?? 0,
        lists: lst.rowCount ?? 0,
        orders: ord.rowCount ?? 0,
    };
}
// ── API request logging ──────────────────────────────────────────────────
export async function logApiRequest(opts) {
    try {
        await getPool().query(`INSERT INTO api_request_log (service, endpoint, method, status_code, success, duration_ms, error_message, context)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, [opts.service, opts.endpoint, opts.method ?? "GET", opts.statusCode ?? null, opts.success, opts.durationMs ?? null, opts.errorMessage ?? null, opts.context ?? null]);
    }
    catch {
        // Don't let logging failures break the app
    }
    // Auto-prune: keep only last 10,000 rows
    try {
        await getPool().query(`DELETE FROM api_request_log WHERE id NOT IN (SELECT id FROM api_request_log ORDER BY created_at DESC LIMIT 10000)`);
    }
    catch { }
}
export async function getApiRequestLog(opts) {
    const params = [];
    let where = "WHERE 1=1";
    const hours = opts?.hours ?? 24;
    params.push(hours);
    where += ` AND created_at > NOW() - INTERVAL '1 hour' * $${params.length}`;
    if (opts?.service) {
        params.push(opts.service);
        where += ` AND service = $${params.length}`;
    }
    if (opts?.successOnly)
        where += " AND success = true";
    if (opts?.errorsOnly)
        where += " AND success = false";
    if (opts?.scheduledOnly) {
        // Match requests originating from scheduled/cron jobs. Services that
        // are only called from scheduled jobs (rss, youtube, listenbrainz) plus
        // specific context markers for other scheduled tasks.
        where += ` AND (
      service IN ('rss', 'youtube', 'listenbrainz')
      OR context LIKE 'scheduled %'
      OR context = 'profile-refresh'
      OR context = 'price-update'
      OR context LIKE 'extras: %'
    )`;
    }
    const r = await getPool().query(`SELECT * FROM api_request_log ${where} ORDER BY created_at DESC`, params);
    return { items: r.rows, total: r.rows.length };
}
// ── User collection/wantlist stats (admin) ────────────────────────────────
export async function getUserCollectionStats() {
    const pool = getPool();
    // Per-user stats
    const perUser = await pool.query(`
    SELECT
      u.clerk_user_id,
      u.discogs_username AS username,
      COALESCE(c.coll_count, 0)::int AS collection_count,
      COALESCE(w.want_count, 0)::int AS wantlist_count,
      COALESCE(i.inv_count, 0)::int AS inventory_count,
      COALESCE(l.list_count, 0)::int AS list_count,
      COALESCE(li.list_item_count, 0)::int AS list_item_count,
      COALESCE(o.orders_count, 0)::int AS orders_count,
      c.oldest_added AS coll_oldest,
      c.newest_added AS coll_newest,
      c.top_genres,
      c.top_styles
    FROM user_tokens u
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS coll_count,
             MIN(added_at) AS oldest_added,
             MAX(added_at) AS newest_added,
             (SELECT array_agg(g ORDER BY cnt DESC) FROM (
               SELECT g, COUNT(*)::int AS cnt
               FROM user_collection uc2,
                    jsonb_array_elements_text(uc2.data->'genres') AS g
               WHERE uc2.clerk_user_id = u.clerk_user_id
               GROUP BY g ORDER BY cnt DESC LIMIT 5
             ) sub) AS top_genres,
             (SELECT array_agg(s ORDER BY cnt DESC) FROM (
               SELECT s, COUNT(*)::int AS cnt
               FROM user_collection uc3,
                    jsonb_array_elements_text(uc3.data->'styles') AS s
               WHERE uc3.clerk_user_id = u.clerk_user_id
               GROUP BY s ORDER BY cnt DESC LIMIT 5
             ) sub2) AS top_styles
      FROM user_collection uc
      WHERE uc.clerk_user_id = u.clerk_user_id
    ) c ON true
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS want_count
      FROM user_wantlist uw
      WHERE uw.clerk_user_id = u.clerk_user_id
    ) w ON true
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS inv_count
      FROM user_inventory ui
      WHERE ui.clerk_user_id = u.clerk_user_id
    ) i ON true
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS list_count
      FROM user_lists ul
      WHERE ul.clerk_user_id = u.clerk_user_id
    ) l ON true
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS list_item_count
      FROM user_list_items uli
      WHERE uli.clerk_user_id = u.clerk_user_id
    ) li ON true
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS orders_count
      FROM user_orders uo
      WHERE uo.clerk_user_id = u.clerk_user_id
    ) o ON true
    WHERE u.discogs_username IS NOT NULL
    ORDER BY c.coll_count DESC NULLS LAST
  `);
    // Global totals
    const globalQ = await pool.query(`
    SELECT
      (SELECT COUNT(*)::int FROM user_collection) AS total_collection,
      (SELECT COUNT(*)::int FROM user_wantlist) AS total_wantlist,
      (SELECT COUNT(*)::int FROM user_inventory) AS total_inventory,
      (SELECT COUNT(*)::int FROM user_orders) AS total_orders,
      (SELECT COUNT(*)::int FROM user_list_items) AS total_list_items,
      (SELECT COUNT(DISTINCT discogs_release_id)::int FROM user_collection) AS unique_releases,
      (SELECT COUNT(DISTINCT discogs_release_id)::int FROM user_wantlist) AS unique_wants
  `);
    return { users: perUser.rows, global: globalQ.rows[0] };
}
/** Get a cached entry from DB. Returns null if not cached or if the
 *  entry is older than `maxAgeSeconds` (cache miss vs stale-eviction
 *  collapsed to a single null return — caller decides whether to
 *  refetch). When `maxAgeSeconds` is undefined the entry is returned
 *  regardless of age (caller can then decide based on cached_at). */
export async function getCachedRelease(discogsId, type, maxAgeSeconds) {
    // V2 dispatch — masters and releases live in the split schema when
    // the flag's on. artist / master-versions stay on release_cache
    // (infra caches, not projected).
    if ((type === "release" || type === "master") && await isSplitCacheReaderEnabled()) {
        return _getCachedReleaseV2(discogsId, type, maxAgeSeconds);
    }
    const r = await getPool().query(`SELECT data, cached_at FROM release_cache WHERE discogs_id = $1 AND type = $2`, [discogsId, type]);
    const row = r.rows[0];
    if (!row)
        return null;
    if (typeof maxAgeSeconds === "number" && row.cached_at) {
        const ageMs = Date.now() - new Date(row.cached_at).getTime();
        if (ageMs > maxAgeSeconds * 1000)
            return null;
    }
    if (type === "release" || type === "master") {
        getPool().query(`UPDATE release_cache SET seen_at = NOW()
        WHERE discogs_id = $1 AND type = $2 AND seen_at IS NULL`, [discogsId, type]).catch(() => { });
    }
    return row.data ?? null;
}
// Split-schema getCachedRelease. Masters live in masters_plus only.
// Releases: try pressings first (the common case — most releases have
// a master_id), fall back to masters_plus (orphan releases with no
// master_id). seen_at bookkeeping targets whichever table the row
// came from.
async function _getCachedReleaseV2(discogsId, type, maxAgeSeconds) {
    const pool = getPool();
    if (type === "master") {
        const r = await pool.query(`SELECT data, cached_at FROM discogs_cache_masters_plus
        WHERE discogs_id = $1 AND type = 'master' LIMIT 1`, [discogsId]);
        const row = r.rows[0];
        if (!row)
            return null;
        if (typeof maxAgeSeconds === "number" && row.cached_at) {
            const ageMs = Date.now() - new Date(row.cached_at).getTime();
            if (ageMs > maxAgeSeconds * 1000)
                return null;
        }
        pool.query(`UPDATE discogs_cache_masters_plus SET seen_at = NOW()
        WHERE discogs_id = $1 AND type = 'master' AND seen_at IS NULL`, [discogsId]).catch(() => { });
        return row.data ?? null;
    }
    // type === "release"
    const rp = await pool.query(`SELECT data, cached_at FROM discogs_cache_pressings
      WHERE discogs_id = $1 LIMIT 1`, [discogsId]);
    if (rp.rows[0]) {
        const row = rp.rows[0];
        if (typeof maxAgeSeconds === "number" && row.cached_at) {
            const ageMs = Date.now() - new Date(row.cached_at).getTime();
            if (ageMs > maxAgeSeconds * 1000)
                return null;
        }
        pool.query(`UPDATE discogs_cache_pressings SET seen_at = NOW()
        WHERE discogs_id = $1 AND seen_at IS NULL`, [discogsId]).catch(() => { });
        return row.data ?? null;
    }
    const rm = await pool.query(`SELECT data, cached_at FROM discogs_cache_masters_plus
      WHERE discogs_id = $1 AND type = 'release' LIMIT 1`, [discogsId]);
    if (!rm.rows[0])
        return null;
    const row = rm.rows[0];
    if (typeof maxAgeSeconds === "number" && row.cached_at) {
        const ageMs = Date.now() - new Date(row.cached_at).getTime();
        if (ageMs > maxAgeSeconds * 1000)
            return null;
    }
    pool.query(`UPDATE discogs_cache_masters_plus SET seen_at = NOW()
      WHERE discogs_id = $1 AND type = 'release' AND seen_at IS NULL`, [discogsId]).catch(() => { });
    return row.data ?? null;
}
/** Save a metadata response to cache. Overwrites if already present.
 *  Default behaviour stamps seen_at = NOW() — i.e. this write came
 *  from a user click (or any path where surfacing in the feed is
 *  appropriate). Pass `warmOnly: true` for the cache-warm job so
 *  the row stays out of the feed until a user actually opens it. */
// ── MusicBrainz cache helpers ──────────────────────────────────────
// (Mirror cacheRelease / getCacheEnrichmentBatch but for MB rows.)
export async function mbCacheGet(entityType, key) {
    try {
        const r = await getPool().query(`SELECT data FROM musicbrainz_cache WHERE entity_type = $1 AND key = $2 LIMIT 1`, [entityType, key]);
        return r.rows[0]?.data ?? null;
    }
    catch {
        return null;
    }
}
export async function mbCacheSet(entityType, key, data) {
    await getPool().query(`INSERT INTO musicbrainz_cache (entity_type, key, data, cached_at)
     VALUES ($1, $2, $3, NOW())
     ON CONFLICT (entity_type, key)
     DO UPDATE SET data = EXCLUDED.data, cached_at = NOW()`, [entityType, key, JSON.stringify(data)]);
}
// MB Saves CRUD — per-user star/bookmark list for the MB view's
// Saved tab. listMbSaves returns rows newest-first; filter by
// entity_type optionally so the UI can group/segment without a
// client-side filter pass.
export async function listMbSaves(clerkUserId, entityType) {
    const params = [clerkUserId];
    let where = "clerk_user_id = $1";
    if (entityType) {
        params.push(entityType);
        where += ` AND entity_type = $${params.length}`;
    }
    const r = await getPool().query(`SELECT id, entity_type, mbid, name, meta, saved_at
       FROM musicbrainz_saves
      WHERE ${where}
      ORDER BY saved_at DESC`, params);
    return r.rows;
}
export async function listMbSaveIds(clerkUserId) {
    // Compact key list ("artist:<mbid>") for the client's "am I
    // already saved?" lookup. The UI uses a Set so star state
    // renders synchronously on a fresh search render.
    const r = await getPool().query(`SELECT entity_type, mbid FROM musicbrainz_saves WHERE clerk_user_id = $1`, [clerkUserId]);
    return r.rows.map((row) => `${row.entity_type}:${row.mbid}`);
}
export async function addMbSave(clerkUserId, entityType, mbid, name, meta) {
    const r = await getPool().query(`INSERT INTO musicbrainz_saves (clerk_user_id, entity_type, mbid, name, meta)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (clerk_user_id, entity_type, mbid) DO NOTHING
     RETURNING id`, [clerkUserId, entityType, mbid, name, meta ? JSON.stringify(meta) : null]);
    return (r.rowCount ?? 0) > 0;
}
export async function removeMbSave(clerkUserId, entityType, mbid) {
    const r = await getPool().query(`DELETE FROM musicbrainz_saves WHERE clerk_user_id = $1 AND entity_type = $2 AND mbid = $3`, [clerkUserId, entityType, mbid]);
    return (r.rowCount ?? 0) > 0;
}
export async function cacheRelease(discogsId, type, data, opts) {
    const warmOnly = !!opts?.warmOnly;
    if (warmOnly) {
        // Don't downgrade an existing engaged row back to warm-only —
        // COALESCE keeps any prior seen_at intact, only sets it if the
        // current value is NULL (which for a fresh row means leaving it
        // NULL, i.e. warm-only).
        await getPool().query(`INSERT INTO release_cache (discogs_id, type, data, cached_at, seen_at)
       VALUES ($1, $2, $3, NOW(), NULL)
       ON CONFLICT (discogs_id, type)
       DO UPDATE SET data = EXCLUDED.data, cached_at = NOW()`, [discogsId, type, JSON.stringify(data)]);
    }
    else {
        await getPool().query(`INSERT INTO release_cache (discogs_id, type, data, cached_at, seen_at)
       VALUES ($1, $2, $3, NOW(), NOW())
       ON CONFLICT (discogs_id, type)
       DO UPDATE SET data = EXCLUDED.data, cached_at = NOW(),
                     seen_at = COALESCE(release_cache.seen_at, NOW())`, [discogsId, type, JSON.stringify(data)]);
    }
    // Dual-write into the new split schema. Fire-and-forget from the
    // caller's perspective — a projection failure MUST NOT break the
    // main cache write, since the old release_cache is still the
    // source of truth until we flip readers over.
    try {
        await writeProjectedCache(discogsId, type, data, { warmOnly });
    }
    catch (err) {
        console.error(`[cache-project] dual-write failed id=${discogsId} type=${type}:`, err?.message ?? err);
    }
}
function _projCoerceInt(v) {
    if (v == null)
        return null;
    const n = typeof v === "number" ? v : Number(String(v).trim());
    return Number.isFinite(n) && n !== 0 ? Math.trunc(n) : null;
}
function _projCoerceStr(v) {
    if (v == null)
        return null;
    const s = String(v).trim();
    return s ? s : null;
}
export function projectReleaseData(type, data) {
    // artist + master-versions caches don't belong in the split — skip.
    if (type !== "release" && type !== "master")
        return null;
    if (!data || typeof data !== "object")
        return null;
    const masterId = type === "release" ? _projCoerceInt(data.master_id) : null;
    const bucket = type === "master" ? "master" : (masterId ? "pressing" : "orphan");
    const bucketNum = bucket === "master" ? 0 : bucket === "orphan" ? 1 : 2;
    const year = _projCoerceInt(data.year);
    const country = _projCoerceStr(data.country);
    const rawFormats = Array.isArray(data.formats) ? data.formats : [];
    const primaryFormat = _projCoerceStr(rawFormats[0]?.name);
    // community.want / have — coerce '' → null; anything else int.
    // num_for_sale is at the top level.
    // videos_count, tracks_count are just array lengths (0 when missing).
    const community = data.community ?? {};
    const communityWant = _projCoerceInt(community.want) ?? 0;
    const communityHave = _projCoerceInt(community.have) ?? 0;
    const numForSale = _projCoerceInt(data.num_for_sale) ?? 0;
    const videosCount = Array.isArray(data.videos) ? data.videos.length : 0;
    const tracksCount = Array.isArray(data.tracklist) ? data.tracklist.length : 0;
    const labels = [];
    if (Array.isArray(data.labels)) {
        for (const l of data.labels) {
            const name = _projCoerceStr(l?.name);
            if (!name)
                continue;
            labels.push({
                id: _projCoerceInt(l?.id),
                name,
                catno: _projCoerceStr(l?.catno),
            });
        }
    }
    const artists = [];
    if (Array.isArray(data.artists)) {
        for (const a of data.artists) {
            const id = _projCoerceInt(a?.id);
            if (id != null)
                artists.push({ id, name: _projCoerceStr(a?.name), role: "main" });
        }
    }
    if (Array.isArray(data.extraartists)) {
        for (const a of data.extraartists) {
            const id = _projCoerceInt(a?.id);
            if (id != null)
                artists.push({ id, name: _projCoerceStr(a?.name), role: "extra" });
        }
    }
    const tags = [];
    if (Array.isArray(data.genres)) {
        for (const g of data.genres) {
            const v = _projCoerceStr(g);
            if (v)
                tags.push({ kind: "genre", value: v });
        }
    }
    if (Array.isArray(data.styles)) {
        for (const s of data.styles) {
            const v = _projCoerceStr(s);
            if (v)
                tags.push({ kind: "style", value: v });
        }
    }
    for (const f of rawFormats) {
        const v = _projCoerceStr(f?.name);
        if (v)
            tags.push({ kind: "format", value: v });
    }
    return {
        bucket, bucketNum, masterId, year, country, primaryFormat,
        communityWant, communityHave, numForSale, videosCount, tracksCount,
        labels, artists, tags,
    };
}
// Write a single release/master into the split schema. Runs inside
// one transaction so a failure leaves nothing half-written; caller
// decides whether to swallow or propagate.
export async function writeProjectedCache(discogsId, type, data, opts = {}) {
    const proj = projectReleaseData(type, data);
    if (!proj)
        return;
    const warmOnly = !!opts.warmOnly;
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        if (proj.bucket === "pressing") {
            await client.query(`INSERT INTO discogs_cache_pressings
           (discogs_id, master_id, year, country, primary_format,
            community_want, community_have, num_for_sale, videos_count, tracks_count,
            data, cached_at, seen_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), ${warmOnly ? "NULL" : "NOW()"})
         ON CONFLICT (discogs_id) DO UPDATE SET
           master_id      = EXCLUDED.master_id,
           year           = EXCLUDED.year,
           country        = EXCLUDED.country,
           primary_format = EXCLUDED.primary_format,
           community_want = EXCLUDED.community_want,
           community_have = EXCLUDED.community_have,
           num_for_sale   = EXCLUDED.num_for_sale,
           videos_count   = EXCLUDED.videos_count,
           tracks_count   = EXCLUDED.tracks_count,
           data           = EXCLUDED.data,
           cached_at      = NOW(),
           seen_at        = ${warmOnly
                ? "discogs_cache_pressings.seen_at"
                : "COALESCE(discogs_cache_pressings.seen_at, NOW())"}`, [discogsId, proj.masterId, proj.year, proj.country, proj.primaryFormat,
                proj.communityWant, proj.communityHave, proj.numForSale, proj.videosCount, proj.tracksCount,
                JSON.stringify(data)]);
        }
        else {
            await client.query(`INSERT INTO discogs_cache_masters_plus
           (discogs_id, type, year, country, primary_format,
            community_want, community_have, num_for_sale, videos_count, tracks_count,
            data, cached_at, seen_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), ${warmOnly ? "NULL" : "NOW()"})
         ON CONFLICT (discogs_id, type) DO UPDATE SET
           year           = EXCLUDED.year,
           country        = EXCLUDED.country,
           primary_format = EXCLUDED.primary_format,
           community_want = EXCLUDED.community_want,
           community_have = EXCLUDED.community_have,
           num_for_sale   = EXCLUDED.num_for_sale,
           videos_count   = EXCLUDED.videos_count,
           tracks_count   = EXCLUDED.tracks_count,
           data           = EXCLUDED.data,
           cached_at      = NOW(),
           seen_at        = ${warmOnly
                ? "discogs_cache_masters_plus.seen_at"
                : "COALESCE(discogs_cache_masters_plus.seen_at, NOW())"}`, [discogsId, type, proj.year, proj.country, proj.primaryFormat,
                proj.communityWant, proj.communityHave, proj.numForSale, proj.videosCount, proj.tracksCount,
                JSON.stringify(data)]);
        }
        // Refresh side tables: DELETE-then-INSERT keyed by (discogs_id,
        // bucket). Cheap because both indexes are on that pair.
        await client.query(`DELETE FROM release_labels  WHERE discogs_id = $1 AND bucket = $2`, [discogsId, proj.bucketNum]);
        await client.query(`DELETE FROM release_artists WHERE discogs_id = $1 AND bucket = $2`, [discogsId, proj.bucketNum]);
        await client.query(`DELETE FROM release_tags    WHERE discogs_id = $1 AND bucket = $2`, [discogsId, proj.bucketNum]);
        if (proj.labels.length > 0) {
            const params = [];
            const rows = [];
            for (const l of proj.labels) {
                params.push(discogsId, proj.bucketNum, l.id, l.name, l.catno);
                const b = params.length;
                rows.push(`($${b - 4}, $${b - 3}, $${b - 2}, $${b - 1}, $${b})`);
            }
            await client.query(`INSERT INTO release_labels (discogs_id, bucket, label_id, label_name, catno) VALUES ${rows.join(",")}`, params);
        }
        if (proj.artists.length > 0) {
            // Dedup within input — Discogs sometimes lists the same artist
            // as both main + extra, or duplicate ids in extraartists.
            const seen = new Set();
            const params = [];
            const rows = [];
            for (const a of proj.artists) {
                const k = `${a.id}:${a.role}`;
                if (seen.has(k))
                    continue;
                seen.add(k);
                params.push(discogsId, proj.bucketNum, a.id, a.role, a.name);
                const b = params.length;
                rows.push(`($${b - 4}, $${b - 3}, $${b - 2}, $${b - 1}, $${b})`);
            }
            if (rows.length > 0) {
                await client.query(`INSERT INTO release_artists (discogs_id, bucket, artist_id, role, name) VALUES ${rows.join(",")}`, params);
            }
        }
        if (proj.tags.length > 0) {
            const seen = new Set();
            const params = [];
            const rows = [];
            for (const t of proj.tags) {
                const k = `${t.kind}:${t.value}`;
                if (seen.has(k))
                    continue;
                seen.add(k);
                params.push(discogsId, proj.bucketNum, t.kind, t.value);
                const b = params.length;
                rows.push(`($${b - 3}, $${b - 2}, $${b - 1}, $${b})`);
            }
            if (rows.length > 0) {
                await client.query(`INSERT INTO release_tags (discogs_id, bucket, kind, value) VALUES ${rows.join(",")}`, params);
            }
        }
        await client.query("COMMIT");
    }
    catch (err) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        throw err;
    }
    finally {
        client.release();
    }
}
export async function writeProjectedCacheBatch(batch) {
    if (batch.length === 0)
        return { ok: 0, err: 0, lastError: null };
    // Project all rows up front. Non-projectable rows (artist / master-
    // versions in the input) are silently dropped.
    const rows = [];
    for (const r of batch) {
        const proj = projectReleaseData(r.type, r.data);
        if (proj)
            rows.push({ ...r, proj });
    }
    if (rows.length === 0)
        return { ok: 0, err: 0, lastError: null };
    const mastersPlus = rows.filter(r => r.proj.bucket !== "pressing");
    const pressings = rows.filter(r => r.proj.bucket === "pressing");
    // Build side-table row lists with per-parent dedup so a single
    // Discogs blob repeating an artist/tag doesn't multiply the row.
    const labelRows = [];
    const artistRows = [];
    const tagRows = [];
    for (const r of rows) {
        for (const l of r.proj.labels) {
            labelRows.push([r.discogs_id, r.proj.bucketNum, l.id, l.name, l.catno]);
        }
        const artistSeen = new Set();
        for (const a of r.proj.artists) {
            const k = `${a.id}:${a.role}`;
            if (artistSeen.has(k))
                continue;
            artistSeen.add(k);
            artistRows.push([r.discogs_id, r.proj.bucketNum, a.id, a.role, a.name]);
        }
        const tagSeen = new Set();
        for (const t of r.proj.tags) {
            const k = `${t.kind}:${t.value}`;
            if (tagSeen.has(k))
                continue;
            tagSeen.add(k);
            tagRows.push([r.discogs_id, r.proj.bucketNum, t.kind, t.value]);
        }
    }
    // Chunk big INSERT VALUES to stay under Postgres's 65,535-parameter
    // limit. Signature k = params per row; safe chunk = floor(60000/k).
    const chunkedInsert = async (client, sql, rowsArr, paramsPerRow) => {
        if (rowsArr.length === 0)
            return;
        const maxRows = Math.max(1, Math.floor(60000 / paramsPerRow));
        for (let i = 0; i < rowsArr.length; i += maxRows) {
            const slice = rowsArr.slice(i, i + maxRows);
            const params = [];
            const placeholders = [];
            for (const row of slice) {
                const ph = [];
                for (const v of row) {
                    params.push(v);
                    ph.push(`$${params.length}`);
                }
                placeholders.push(`(${ph.join(",")})`);
            }
            await client.query(sql(placeholders.join(",")), params);
        }
    };
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        if (mastersPlus.length > 0) {
            const rowsArr = mastersPlus.map(r => [
                r.discogs_id, r.type, r.proj.year, r.proj.country, r.proj.primaryFormat,
                r.proj.communityWant, r.proj.communityHave, r.proj.numForSale,
                r.proj.videosCount, r.proj.tracksCount,
                JSON.stringify(r.data),
            ]);
            // Explicit casts on the SELECT list — Postgres infers every
            // VALUES column as text otherwise, and pg driver serializes
            // number/null placeholders as text too, so the raw insert fails
            // with "expression is of type text" against the int column.
            await chunkedInsert(client, ph => `INSERT INTO discogs_cache_masters_plus
                 (discogs_id, type, year, country, primary_format,
                  community_want, community_have, num_for_sale, videos_count, tracks_count,
                  data, cached_at, seen_at)
               SELECT v.discogs_id::int,
                      v.type::text,
                      v.year::smallint,
                      v.country::text,
                      v.primary_format::text,
                      v.community_want::int,
                      v.community_have::int,
                      v.num_for_sale::int,
                      v.videos_count::smallint,
                      v.tracks_count::smallint,
                      v.data::jsonb,
                      NOW(), NOW()
                 FROM (VALUES ${ph}) AS v(discogs_id, type, year, country, primary_format,
                                          community_want, community_have, num_for_sale,
                                          videos_count, tracks_count, data)
               ON CONFLICT (discogs_id, type) DO UPDATE SET
                 year           = EXCLUDED.year,
                 country        = EXCLUDED.country,
                 primary_format = EXCLUDED.primary_format,
                 community_want = EXCLUDED.community_want,
                 community_have = EXCLUDED.community_have,
                 num_for_sale   = EXCLUDED.num_for_sale,
                 videos_count   = EXCLUDED.videos_count,
                 tracks_count   = EXCLUDED.tracks_count,
                 data           = EXCLUDED.data,
                 cached_at      = NOW(),
                 seen_at        = COALESCE(discogs_cache_masters_plus.seen_at, NOW())`, rowsArr, 11);
        }
        if (pressings.length > 0) {
            const rowsArr = pressings.map(r => [
                r.discogs_id, r.proj.masterId, r.proj.year, r.proj.country, r.proj.primaryFormat,
                r.proj.communityWant, r.proj.communityHave, r.proj.numForSale,
                r.proj.videosCount, r.proj.tracksCount,
                JSON.stringify(r.data),
            ]);
            await chunkedInsert(client, ph => `INSERT INTO discogs_cache_pressings
                 (discogs_id, master_id, year, country, primary_format,
                  community_want, community_have, num_for_sale, videos_count, tracks_count,
                  data, cached_at, seen_at)
               SELECT v.discogs_id::int,
                      v.master_id::int,
                      v.year::smallint,
                      v.country::text,
                      v.primary_format::text,
                      v.community_want::int,
                      v.community_have::int,
                      v.num_for_sale::int,
                      v.videos_count::smallint,
                      v.tracks_count::smallint,
                      v.data::jsonb,
                      NOW(), NOW()
                 FROM (VALUES ${ph}) AS v(discogs_id, master_id, year, country, primary_format,
                                          community_want, community_have, num_for_sale,
                                          videos_count, tracks_count, data)
               ON CONFLICT (discogs_id) DO UPDATE SET
                 master_id      = EXCLUDED.master_id,
                 year           = EXCLUDED.year,
                 country        = EXCLUDED.country,
                 primary_format = EXCLUDED.primary_format,
                 community_want = EXCLUDED.community_want,
                 community_have = EXCLUDED.community_have,
                 num_for_sale   = EXCLUDED.num_for_sale,
                 videos_count   = EXCLUDED.videos_count,
                 tracks_count   = EXCLUDED.tracks_count,
                 data           = EXCLUDED.data,
                 cached_at      = NOW(),
                 seen_at        = COALESCE(discogs_cache_pressings.seen_at, NOW())`, rowsArr, 11);
        }
        // Delete side-table rows for every (discogs_id, bucket) pair we're
        // about to (re)insert. `= ANY($1::int[])` avoids the parameter
        // explosion an IN (...) list would cause; small (discogs_id, bucket)
        // index handles the seek.
        for (const bucketNum of [0, 1, 2]) {
            const ids = rows.filter(r => r.proj.bucketNum === bucketNum).map(r => r.discogs_id);
            if (ids.length === 0)
                continue;
            await client.query(`DELETE FROM release_labels  WHERE bucket = $1 AND discogs_id = ANY($2::int[])`, [bucketNum, ids]);
            await client.query(`DELETE FROM release_artists WHERE bucket = $1 AND discogs_id = ANY($2::int[])`, [bucketNum, ids]);
            await client.query(`DELETE FROM release_tags    WHERE bucket = $1 AND discogs_id = ANY($2::int[])`, [bucketNum, ids]);
        }
        await chunkedInsert(client, ph => `INSERT INTO release_labels  (discogs_id, bucket, label_id, label_name, catno) VALUES ${ph}`, labelRows, 5);
        await chunkedInsert(client, ph => `INSERT INTO release_artists (discogs_id, bucket, artist_id, role, name)        VALUES ${ph}`, artistRows, 5);
        await chunkedInsert(client, ph => `INSERT INTO release_tags    (discogs_id, bucket, kind, value)                  VALUES ${ph}`, tagRows, 4);
        await client.query("COMMIT");
        return { ok: rows.length, err: 0, lastError: null };
    }
    catch (err) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        console.warn(`[cache-project] batch of ${rows.length} failed, falling back to per-row: ${err?.message ?? err}`);
        // Per-row fallback — a poison row shouldn't sink the batch. This
        // still uses the tx-per-row writeProjectedCache so the failure
        // stays isolated.
        let ok = 0, errCount = 0, lastError = null;
        for (const r of rows) {
            try {
                await writeProjectedCache(r.discogs_id, r.type, r.data);
                ok++;
            }
            catch (e) {
                errCount++;
                lastError = `id=${r.discogs_id} type=${r.type}: ${e?.message ?? String(e)}`;
            }
        }
        return { ok, err: errCount, lastError };
    }
    finally {
        client.release();
    }
}
// Reports rough sizes of both sides of the split — used by the
// backfill worker UI to show progress vs total.
export async function getProjectedCacheStats() {
    const [r1, r2, r3, r4, r5, r6, r7] = await Promise.all([
        getPool().query(`SELECT COUNT(*)::bigint AS c FROM release_cache`),
        getPool().query(`SELECT COUNT(*)::bigint AS c FROM release_cache WHERE type IN ('master','release')`),
        getPool().query(`SELECT COUNT(*)::bigint AS c FROM discogs_cache_masters_plus`),
        getPool().query(`SELECT COUNT(*)::bigint AS c FROM discogs_cache_pressings`),
        getPool().query(`SELECT COUNT(*)::bigint AS c FROM release_labels`),
        getPool().query(`SELECT COUNT(*)::bigint AS c FROM release_artists`),
        getPool().query(`SELECT COUNT(*)::bigint AS c FROM release_tags`),
    ]);
    return {
        releaseCacheTotal: Number(r1.rows[0]?.c ?? 0),
        releaseCacheProjectable: Number(r2.rows[0]?.c ?? 0),
        mastersPlusRows: Number(r3.rows[0]?.c ?? 0),
        pressingsRows: Number(r4.rows[0]?.c ?? 0),
        labelsRows: Number(r5.rows[0]?.c ?? 0),
        artistsRows: Number(r6.rows[0]?.c ?? 0),
        tagsRows: Number(r7.rows[0]?.c ?? 0),
    };
}
// Streaming batch reader for the backfill worker. Returns the next
// `limit` rows from release_cache with discogs_id > cursorId, only
// for projectable types (release/master).
export async function readReleaseCacheBatchForProjection(cursorId, limit) {
    const r = await getPool().query(`SELECT discogs_id, type, data
       FROM release_cache
      WHERE type IN ('release','master')
        AND discogs_id > $1
      ORDER BY discogs_id ASC
      LIMIT $2`, [cursorId, Math.max(1, Math.min(1000, limit))]);
    return r.rows.map(row => ({
        discogs_id: Number(row.discogs_id),
        type: row.type,
        data: row.data,
    }));
}
/** Prune stale cache entries — masters/artists older than 30 days,
 *  releases older than 7 days. Run nightly via a scheduled task or
 *  on-demand from the admin DB Stats panel. Returns rows deleted from
 *  release_cache (not the sum across all tables — that's what the
 *  caller expects). Split-schema mirrors run in the same transaction. */
export async function pruneStaleReleaseCache() {
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        const r = await client.query(`DELETE FROM release_cache
         WHERE (type IN ('master','artist') AND cached_at < NOW() - INTERVAL '30 days')
            OR (type = 'release' AND cached_at < NOW() - INTERVAL '7 days')`);
        // Split-schema mirror. Same TTL semantics; masters live in
        // masters_plus (both masters proper and orphan releases), and
        // pressings holds every release with a master_id. Side tables
        // cascade via (discogs_id, bucket) with a subquery per bucket.
        const del = await client.query(`WITH stale_master_bucket AS (
         DELETE FROM discogs_cache_masters_plus
          WHERE (type = 'master'  AND cached_at < NOW() - INTERVAL '30 days')
             OR (type = 'release' AND cached_at < NOW() - INTERVAL '7 days')
         RETURNING discogs_id,
                   CASE WHEN type = 'master' THEN 0::smallint ELSE 1::smallint END AS bucket
       ),
       stale_pressing_bucket AS (
         DELETE FROM discogs_cache_pressings
          WHERE cached_at < NOW() - INTERVAL '7 days'
         RETURNING discogs_id, 2::smallint AS bucket
       ),
       all_stale AS (
         SELECT discogs_id, bucket FROM stale_master_bucket
         UNION ALL
         SELECT discogs_id, bucket FROM stale_pressing_bucket
       )
       SELECT COUNT(*)::int AS n FROM all_stale`);
        const staleCount = Number(del.rows[0]?.n ?? 0);
        if (staleCount > 0) {
            // Side-table cleanup — matches the buckets that were just
            // deleted. No JOIN needed since the primary-side row is gone
            // (dangling references would only be a problem if we joined
            // FROM release_labels/artists/tags, which we don't).
            // Practically we can just leave the side rows dangling until
            // the next projection touches them, but a targeted DELETE keeps
            // storage tight. Cheap enough given ~5k stale rows at once.
            await client.query(`DELETE FROM release_labels rl
           WHERE NOT EXISTS (
             SELECT 1 FROM discogs_cache_masters_plus m
              WHERE m.discogs_id = rl.discogs_id
                AND ((rl.bucket = 0 AND m.type = 'master') OR (rl.bucket = 1 AND m.type = 'release'))
           )
             AND NOT EXISTS (
             SELECT 1 FROM discogs_cache_pressings p
              WHERE p.discogs_id = rl.discogs_id AND rl.bucket = 2
           )`);
            await client.query(`DELETE FROM release_artists ra
           WHERE NOT EXISTS (
             SELECT 1 FROM discogs_cache_masters_plus m
              WHERE m.discogs_id = ra.discogs_id
                AND ((ra.bucket = 0 AND m.type = 'master') OR (ra.bucket = 1 AND m.type = 'release'))
           )
             AND NOT EXISTS (
             SELECT 1 FROM discogs_cache_pressings p
              WHERE p.discogs_id = ra.discogs_id AND ra.bucket = 2
           )`);
            await client.query(`DELETE FROM release_tags rt
           WHERE NOT EXISTS (
             SELECT 1 FROM discogs_cache_masters_plus m
              WHERE m.discogs_id = rt.discogs_id
                AND ((rt.bucket = 0 AND m.type = 'master') OR (rt.bucket = 1 AND m.type = 'release'))
           )
             AND NOT EXISTS (
             SELECT 1 FROM discogs_cache_pressings p
              WHERE p.discogs_id = rt.discogs_id AND rt.bucket = 2
           )`);
        }
        await client.query("COMMIT");
        return r.rowCount ?? 0;
    }
    catch (err) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        throw err;
    }
    finally {
        client.release();
    }
}
export async function getApiRequestStats(hours = 24) {
    const r = await getPool().query(`
    SELECT service,
           COUNT(*)::int AS total_requests,
           COUNT(*) FILTER (WHERE success)::int AS successes,
           COUNT(*) FILTER (WHERE NOT success)::int AS failures,
           ROUND(AVG(duration_ms))::int AS avg_duration_ms,
           MAX(created_at) AS last_request_at
    FROM api_request_log
    WHERE created_at > NOW() - INTERVAL '1 hour' * $1
    GROUP BY service
    ORDER BY total_requests DESC
  `, [hours]);
    return r.rows;
}
// Per-service API health for the admin "Active APIs" popup: volume,
// success rate, p50/p95 latency, error count, and the most recent
// error message + time — all within the requested window.
export async function getApiHealth(hours = 24) {
    const r = await getPool().query(`
    SELECT e1.service,
           COUNT(*)::int AS total,
           COUNT(*) FILTER (WHERE e1.success)::int      AS successes,
           COUNT(*) FILTER (WHERE NOT e1.success)::int  AS failures,
           PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY e1.duration_ms)::int AS p50_ms,
           PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY e1.duration_ms)::int AS p95_ms,
           (SELECT e2.error_message FROM api_request_log e2
             WHERE e2.service = e1.service AND NOT e2.success
               AND e2.created_at > NOW() - INTERVAL '1 hour' * $1
             ORDER BY e2.created_at DESC LIMIT 1) AS last_error,
           (SELECT e3.created_at FROM api_request_log e3
             WHERE e3.service = e1.service AND NOT e3.success
               AND e3.created_at > NOW() - INTERVAL '1 hour' * $1
             ORDER BY e3.created_at DESC LIMIT 1) AS last_error_at
      FROM api_request_log e1
     WHERE e1.created_at > NOW() - INTERVAL '1 hour' * $1
     GROUP BY e1.service
     ORDER BY total DESC
  `, [hours]);
    return r.rows;
}
// Site-wide KPI bundle for the admin Overview + Users summary boxes.
// One round-trip; all counts derive from existing tables (user_tokens
// for accounts/activity, the event tables for plays/searches/opens).
export async function getAdminOverview() {
    const r = await getPool().query(`
    SELECT
      (SELECT COUNT(*)::int FROM user_tokens) AS total_users,
      (SELECT COUNT(*)::int FROM user_tokens WHERE created_at > NOW() - INTERVAL '7 days')  AS new_users_7d,
      (SELECT COUNT(*)::int FROM user_tokens WHERE created_at > NOW() - INTERVAL '30 days') AS new_users_30d,
      (SELECT COUNT(*)::int FROM user_tokens WHERE last_active_at > NOW() - INTERVAL '1 day')   AS dau,
      (SELECT COUNT(*)::int FROM user_tokens WHERE last_active_at > NOW() - INTERVAL '7 days')  AS wau,
      (SELECT COUNT(*)::int FROM user_tokens WHERE last_active_at > NOW() - INTERVAL '30 days') AS mau,
      (SELECT COUNT(*)::int FROM user_play_events   WHERE created_at > NOW() - INTERVAL '1 day')  AS plays_24h,
      (SELECT COUNT(*)::int FROM user_play_events   WHERE created_at > NOW() - INTERVAL '7 days') AS plays_7d,
      (SELECT COUNT(*)::int FROM user_search_events WHERE created_at > NOW() - INTERVAL '1 day')  AS searches_24h,
      (SELECT COUNT(*)::int FROM user_search_events WHERE created_at > NOW() - INTERVAL '7 days') AS searches_7d,
      (SELECT COUNT(*)::int FROM user_recent_views  WHERE opened_at > NOW() - INTERVAL '1 day')  AS album_opens_24h,
      (SELECT COUNT(*)::int FROM user_recent_views  WHERE opened_at > NOW() - INTERVAL '7 days') AS album_opens_7d
  `);
    const x = r.rows[0] || {};
    return {
        totalUsers: x.total_users ?? 0,
        newUsers7d: x.new_users_7d ?? 0,
        newUsers30d: x.new_users_30d ?? 0,
        dau: x.dau ?? 0, wau: x.wau ?? 0, mau: x.mau ?? 0,
        plays24h: x.plays_24h ?? 0, plays7d: x.plays_7d ?? 0,
        searches24h: x.searches_24h ?? 0, searches7d: x.searches_7d ?? 0,
        albumOpens24h: x.album_opens_24h ?? 0, albumOpens7d: x.album_opens_7d ?? 0,
    };
}
// Media-player + playlist/queue usage for the admin dashboard.
// All from existing tables: user_play_events (plays), user_playlists /
// user_playlist_items (saved playlists), user_play_queue (live queues).
export async function getMediaStats(topLimit = 10) {
    const pool = getPool();
    const lim = Math.max(1, Math.min(100, Math.floor(topLimit) || 10));
    const [agg, bySource, topTitles, playlists, queue] = await Promise.all([
        pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 day')::int   AS plays_24h,
        COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days')::int  AS plays_7d,
        COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '30 days')::int AS plays_30d,
        COUNT(DISTINCT clerk_user_id) FILTER (WHERE created_at > NOW() - INTERVAL '30 days')::int AS listeners_30d
      FROM user_play_events`),
        pool.query(`
      SELECT source, COUNT(*)::int AS n
      FROM user_play_events
      WHERE created_at > NOW() - INTERVAL '7 days'
      GROUP BY source ORDER BY n DESC`),
        pool.query(`
      SELECT COALESCE(NULLIF(title,''), '(untitled)') AS title, source,
             MAX(external_id) AS external_id, COUNT(*)::int AS n
      FROM user_play_events
      WHERE created_at > NOW() - INTERVAL '30 days'
        AND NOT (source = 'yt' AND NULLIF(title,'') IS NULL)
      GROUP BY 1, 2 ORDER BY n DESC LIMIT ${lim}`),
        pool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM user_playlists) AS total_playlists,
        (SELECT COUNT(*)::int FROM user_playlist_items) AS total_items,
        (SELECT COUNT(DISTINCT clerk_user_id)::int FROM user_playlists) AS users_with_playlists`),
        pool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM user_play_queue) AS queue_rows,
        (SELECT COUNT(DISTINCT clerk_user_id)::int FROM user_play_queue) AS users_with_queue`),
    ]);
    const a = agg.rows[0] || {};
    const pl = playlists.rows[0] || {};
    const q = queue.rows[0] || {};
    const totalPlaylists = pl.total_playlists ?? 0;
    return {
        plays24h: a.plays_24h ?? 0,
        plays7d: a.plays_7d ?? 0,
        plays30d: a.plays_30d ?? 0,
        listeners30d: a.listeners_30d ?? 0,
        bySource7d: bySource.rows, // [{source,n}]
        topTitles30d: topTitles.rows, // [{title,source,n}]
        totalPlaylists,
        totalPlaylistItems: pl.total_items ?? 0,
        avgPlaylistLen: totalPlaylists ? Math.round(((pl.total_items ?? 0) / totalPlaylists) * 10) / 10 : 0,
        usersWithPlaylists: pl.users_with_playlists ?? 0,
        queueRows: q.queue_rows ?? 0,
        usersWithQueue: q.users_with_queue ?? 0,
    };
}
// Discogs request volume in rolling windows — a headroom proxy. There
// is no instrumented Discogs limiter (calls are paced ad-hoc and rely
// on Discogs's own 60/min OAuth ceiling), so we approximate pressure
// from api_request_log.
export async function getDiscogsRateWindow() {
    const r = await getPool().query(`
    SELECT
      COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '60 seconds')::int AS last_minute,
      COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 day')::int      AS last_24h
    FROM api_request_log WHERE service = 'discogs'`);
    const x = r.rows[0] || {};
    return { lastMinute: x.last_minute ?? 0, last24h: x.last_24h ?? 0 };
}
// Durable background-job "last activity" timestamps. In-memory job
// state resets on every process restart (Railway redeploys often), so
// the admin panel was reporting "not run yet" for jobs that had in
// fact run. These derive last-run from persisted side-effects:
//   - daily suggestions  → newest user_personal_suggestions.generated_at
//   - cache-warm worker   → newest still-unviewed release_cache row
//     (warm-only rows are written with seen_at NULL until a human opens
//      the modal), which tracks recent warm activity.
export async function getJobHealth() {
    const pool = getPool();
    const [s, c] = await Promise.all([
        pool.query(`SELECT MAX(generated_at) AS t FROM user_personal_suggestions`),
        pool.query(`SELECT MAX(cached_at) AS t FROM release_cache WHERE seen_at IS NULL`),
    ]);
    const iso = (v) => v ? new Date(v).toISOString() : null;
    return {
        suggestionsLastAt: iso(s.rows[0]?.t),
        cacheWarmLastAt: iso(c.rows[0]?.t),
    };
}
// ── Background-job run tracking ──────────────────────────────────────
// Wrap a scheduled job: startJobRun() at the top, finishJobRun() in a
// finally. Best-effort — a logging failure must never break the job,
// so callers should .catch() these.
export async function startJobRun(jobName) {
    try {
        const r = await getPool().query(`INSERT INTO job_runs (job_name, status) VALUES ($1, 'running') RETURNING id`, [jobName]);
        return r.rows[0]?.id ?? null;
    }
    catch {
        return null;
    }
}
export async function finishJobRun(id, opts) {
    if (id == null)
        return;
    try {
        await getPool().query(`UPDATE job_runs
          SET status = $2, ended_at = NOW(),
              items = $3, errors = $4, detail = $5
        WHERE id = $1`, [id, opts.status, opts.items ?? 0, opts.errors ?? 0,
            (opts.detail ?? "").slice(0, 500) || null]);
        // Keep the table bounded: retain the most recent 100 runs per job.
        await getPool().query(`DELETE FROM job_runs jr
        WHERE jr.job_name = (SELECT job_name FROM job_runs WHERE id = $1)
          AND jr.id NOT IN (
            SELECT id FROM job_runs
             WHERE job_name = (SELECT job_name FROM job_runs WHERE id = $1)
             ORDER BY started_at DESC LIMIT 100
          )`, [id]);
    }
    catch { /* best-effort */ }
}
// Latest run per job (for the admin Active-APIs popup).
export async function getJobLastRuns() {
    try {
        const r = await getPool().query(`
      SELECT DISTINCT ON (job_name)
             job_name, status, started_at, ended_at, items, errors, detail
        FROM job_runs
       ORDER BY job_name, started_at DESC
    `);
        return r.rows;
    }
    catch {
        return [];
    }
}
// Full recent history for one job (audit drill-down).
export async function getRecentJobRuns(jobName, limit = 25) {
    try {
        const r = await getPool().query(`SELECT id, job_name, status, started_at, ended_at, items, errors, detail
         FROM job_runs WHERE job_name = $1
        ORDER BY started_at DESC LIMIT $2`, [jobName, Math.max(1, Math.min(200, limit))]);
        return r.rows;
    }
    catch {
        return [];
    }
}
// ── Table row counts (admin dashboard) ───────────────────────────────────
export async function getTableRowCounts() {
    const tables = [
        'user_tokens', 'user_collection', 'user_collection_folders', 'user_wantlist',
        'user_inventory', 'user_lists', 'user_list_items', 'user_orders', 'user_order_messages',
        'user_favorites', 'user_recent_views', 'user_loc_saves', 'user_archive_saves',
        'user_youtube_saves', 'user_wiki_saves', 'user_play_queue', 'saved_searches', 'feedback',
        'release_cache', 'price_cache', 'price_history',
        'blues_artists', 'api_request_log', 'oauth_request_tokens',
        'app_settings',
    ];
    const counts = await Promise.all(tables.map(async (t) => {
        try {
            const r = await getPool().query(`SELECT COUNT(*)::int AS cnt FROM ${t}`);
            return { table: t, rows: r.rows[0]?.cnt ?? 0 };
        }
        catch {
            return { table: t, rows: -1 };
        }
    }));
    return counts;
}
// ── App settings (key/value) ─────────────────────────────────────────────
export async function getAppSetting(key) {
    try {
        const r = await getPool().query(`SELECT value FROM app_settings WHERE key = $1 LIMIT 1`, [key]);
        return r.rows[0]?.value ?? null;
    }
    catch {
        return null;
    }
}
export async function setAppSetting(key, value) {
    await getPool().query(`INSERT INTO app_settings (key, value, updated_at) VALUES ($1, $2, NOW())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`, [key, value]);
}
// ── User preferences (cross-device) ───────────────────────────────────
// Returns the prefs JSON for a user, or {} if no row. Never throws —
// missing rows or DB hiccups are treated as "no prefs set."
export async function getUserPrefs(clerkUserId) {
    try {
        const r = await getPool().query(`SELECT prefs FROM user_preferences WHERE clerk_user_id = $1 LIMIT 1`, [clerkUserId]);
        const row = r.rows[0]?.prefs;
        return row && typeof row === "object" ? row : {};
    }
    catch {
        return {};
    }
}
// Merge-update a user's prefs. Pass partial keys; existing keys not in
// the patch are preserved. Returns the post-merge prefs.
export async function setUserPrefs(clerkUserId, patch) {
    const current = await getUserPrefs(clerkUserId);
    const merged = { ...current, ...patch };
    await getPool().query(`INSERT INTO user_preferences (clerk_user_id, prefs, updated_at)
     VALUES ($1, $2::jsonb, NOW())
     ON CONFLICT (clerk_user_id) DO UPDATE SET prefs = EXCLUDED.prefs, updated_at = NOW()`, [clerkUserId, JSON.stringify(merged)]);
    return merged;
}
// Look up overrides for a given master/release. Returns rows for both
// the master scope and (if releaseId is provided) the matching release
// scope; caller picks per-position with release-scope winning.
export async function getTrackYtOverrides(masterId, releaseId) {
    const params = [];
    const ors = [];
    if (masterId != null && String(masterId).length) {
        params.push(String(masterId));
        ors.push(`(release_type = 'master' AND release_id = $${params.length})`);
    }
    if (releaseId != null && String(releaseId).length) {
        params.push(String(releaseId));
        ors.push(`(release_type = 'release' AND release_id = $${params.length})`);
    }
    if (ors.length === 0)
        return [];
    try {
        // LEFT JOIN against unavailable list so we can exclude rows whose
        // video has been flagged "unavailable" by enough users — the
        // override served to clients only contains videos that are likely
        // playable. Flagged-but-not-yet-unavailable rows still serve.
        const r = await getPool().query(`SELECT o.release_id, o.release_type, o.track_position, o.track_title,
              o.video_id, o.video_title, o.submitted_by, o.submitted_at
         FROM track_youtube_overrides o
         LEFT JOIN youtube_video_unavailable u
           ON u.video_id = o.video_id AND u.status = 'unavailable'
        WHERE (${ors.join(" OR ")})
          AND u.video_id IS NULL`, params);
        return r.rows;
    }
    catch {
        return [];
    }
}
// First submission wins. Returns true if inserted, false if a row
// already existed (ON CONFLICT DO NOTHING).
export async function suggestTrackYtOverride(args) {
    const r = await getPool().query(`INSERT INTO track_youtube_overrides
       (release_id, release_type, track_position, track_title, video_id, video_title, submitted_by)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (release_id, release_type, track_position) DO NOTHING
     RETURNING release_id`, [
        String(args.releaseId),
        args.releaseType,
        args.trackPosition,
        args.trackTitle ?? null,
        args.videoId,
        args.videoTitle ?? null,
        args.submittedBy,
    ]);
    return r.rowCount === 1;
}
// Batch insert: all-or-mostly-nothing wrapper around the singleton
// suggest path. The album-level "Find missing tracks" popup hands us
// a list of (releaseId, releaseType, trackPosition, videoId, …)
// tuples and we insert them in one transaction. First-submission-wins
// semantics still apply per-row via ON CONFLICT DO NOTHING — if some
// rows are already taken, they're silently skipped while the rest
// land. Returns counts of inserts vs skips so the UI can report
// "saved 4, 1 already taken".
export async function suggestTrackYtOverridesBatch(items) {
    if (!items.length)
        return { inserted: 0, skipped: 0 };
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        let inserted = 0;
        for (const it of items) {
            const r = await client.query(`INSERT INTO track_youtube_overrides
           (release_id, release_type, track_position, track_title, video_id, video_title, submitted_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (release_id, release_type, track_position) DO NOTHING
         RETURNING release_id`, [
                String(it.releaseId),
                it.releaseType,
                it.trackPosition,
                it.trackTitle ?? null,
                it.videoId,
                it.videoTitle ?? null,
                it.submittedBy,
            ]);
            if (r.rowCount === 1)
                inserted++;
        }
        await client.query("COMMIT");
        return { inserted, skipped: items.length - inserted };
    }
    catch (e) {
        await client.query("ROLLBACK").catch(() => { });
        throw e;
    }
    finally {
        client.release();
    }
}
// ── YT review queue helpers (v1: human-reviewed background matcher) ──
export async function insertReviewCandidate(args) {
    const r = await getPool().query(`INSERT INTO track_yt_review_queue
       (master_id, track_position, track_title, track_artist, master_year, master_cover_url,
        candidate_video_id, candidate_title, candidate_channel_title, candidate_channel_id,
        candidate_duration_seconds, candidate_thumbnail_url, candidate_published_at,
        title_score, duration_ok)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
     ON CONFLICT (master_id, track_position, candidate_video_id) DO NOTHING
     RETURNING id`, [
        Number(args.masterId), args.trackPosition, args.trackTitle, args.trackArtist ?? null,
        args.masterYear ?? null, args.masterCoverUrl ?? null,
        args.candidateVideoId, args.candidateTitle ?? null, args.candidateChannelTitle ?? null,
        args.candidateChannelId ?? null, args.candidateDurationSeconds ?? null,
        args.candidateThumbnailUrl ?? null,
        args.candidatePublishedAt ?? null,
        args.titleScore ?? null, args.durationOk ?? null,
    ]);
    return (r.rowCount ?? 0) > 0;
}
// Mark a (master, track) pair as searched (regardless of outcome) so
// the next worker pass skips it. candidate_count tracks how many
// videos auto-matched to this track from the album-level search.
export async function logTrackSearched(masterId, trackPosition, candidateCount, source = "album") {
    await getPool().query(`INSERT INTO track_yt_review_searched (master_id, track_position, candidate_count, source)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (master_id, track_position)
     DO UPDATE SET last_searched_at = NOW(), candidate_count = EXCLUDED.candidate_count, source = EXCLUDED.source`, [Number(masterId), trackPosition, Math.max(0, candidateCount | 0), source]);
}
export async function isTrackAlreadySearched(masterId, trackPosition) {
    const r = await getPool().query(`SELECT 1 FROM track_yt_review_searched WHERE master_id = $1 AND track_position = $2 LIMIT 1`, [Number(masterId), trackPosition]);
    return (r.rowCount ?? 0) > 0;
}
// Clear searched-log rows that yielded 0 candidates so the worker
// will retry them on its next pass. Optional master_id scopes the
// clear to a single album.
export async function clearEmptySearchedRows(masterId) {
    const r = masterId == null
        ? await getPool().query(`DELETE FROM track_yt_review_searched WHERE candidate_count = 0`)
        : await getPool().query(`DELETE FROM track_yt_review_searched WHERE candidate_count = 0 AND master_id = $1`, [Number(masterId)]);
    return r.rowCount ?? 0;
}
// Has this (master, track) ever been considered? Used by the worker to
// skip tracks that already have a pending OR resolved (approved /
// rejected / skipped) candidate so a single track only enqueues once.
export async function reviewQueueHasEntry(masterId, trackPosition) {
    const r = await getPool().query(`SELECT 1 FROM track_yt_review_queue WHERE master_id = $1 AND track_position = $2 LIMIT 1`, [Number(masterId), trackPosition]);
    return (r.rowCount ?? 0) > 0;
}
export async function listReviewQueue(opts = {}) {
    const status = opts.status || "pending";
    const limit = Math.max(1, Math.min(200, opts.limit ?? 50));
    const offset = Math.max(0, opts.offset ?? 0);
    const totalR = await getPool().query(`SELECT COUNT(*)::int AS n FROM track_yt_review_queue WHERE status = $1`, [status]);
    const r = await getPool().query(`SELECT * FROM track_yt_review_queue
      WHERE status = $1
      ORDER BY master_year ASC NULLS LAST, master_id ASC, track_position ASC,
               title_score DESC NULLS LAST, id ASC
      LIMIT $2 OFFSET $3`, [status, limit, offset]);
    return { rows: r.rows, total: totalR.rows[0]?.n ?? 0 };
}
export async function getReviewQueueCounts() {
    const r = await getPool().query(`SELECT status, COUNT(DISTINCT (master_id, track_position))::int AS n
       FROM track_yt_review_queue
      GROUP BY status`);
    const out = { pending: 0, approved: 0, rejected: 0, skipped: 0, total: 0 };
    for (const row of r.rows) {
        const s = String(row.status);
        if (s in out)
            out[s] = Number(row.n);
        out.total += Number(row.n);
    }
    return out;
}
export async function reviewQueueDecide(id, action, reviewer) {
    const status = action === "approve" ? "approved" : action === "reject" ? "rejected" : "skipped";
    const upd = await getPool().query(`UPDATE track_yt_review_queue
        SET status = $2, reviewed_at = NOW(), reviewed_by = $3
      WHERE id = $1 AND status = 'pending'
    RETURNING master_id, track_position, track_title, candidate_video_id, candidate_title`, [id, status, reviewer]);
    if ((upd.rowCount ?? 0) === 0)
        return { ok: false };
    const row = upd.rows[0];
    // Approve also marks any OTHER pending candidates for the same
    // (master, track) as 'superseded' so the queue collapses to one
    // resolved row per track.
    if (action === "approve") {
        await getPool().query(`UPDATE track_yt_review_queue
          SET status = 'superseded', reviewed_at = NOW(), reviewed_by = $3
        WHERE master_id = $1 AND track_position = $2 AND id <> $4 AND status = 'pending'`, [Number(row.master_id), String(row.track_position), reviewer, id]);
    }
    return {
        ok: true,
        videoId: row.candidate_video_id,
        masterId: Number(row.master_id),
        trackPosition: String(row.track_position),
        trackTitle: String(row.track_title || ""),
    };
}
export async function reviewQueueDeleteApproval(id, reviewer) {
    const row = (await getPool().query(`SELECT master_id, track_position, status FROM track_yt_review_queue WHERE id = $1`, [id])).rows[0];
    if (!row || row.status !== "approved")
        return { ok: false };
    const masterId = Number(row.master_id);
    const trackPosition = String(row.track_position);
    await deleteTrackYtOverride(masterId, "master", trackPosition);
    await getPool().query(`UPDATE track_yt_review_queue
        SET status = 'rejected', reviewed_at = NOW(), reviewed_by = $2
      WHERE id = $1`, [id, reviewer]);
    return { ok: true, masterId, trackPosition };
}
export async function getReviewState() {
    const r = await getPool().query(`SELECT * FROM track_yt_review_state WHERE id = 1`);
    return r.rows[0] ?? null;
}
export async function updateReviewState(patch) {
    const allowed = new Set([
        "running", "cursor_year", "cursor_master_id", "cursor_track_pos",
        "total_searched", "total_queued", "total_skipped", "total_errors",
        "last_run_at", "last_error", "message",
    ]);
    const cols = [];
    const vals = [];
    for (const [k, v] of Object.entries(patch)) {
        if (!allowed.has(k))
            continue;
        cols.push(k);
        vals.push(v);
    }
    if (!cols.length)
        return;
    const setSql = cols.map((c, i) => `${c} = $${i + 1}`).join(", ");
    await getPool().query(`UPDATE track_yt_review_state SET ${setSql} WHERE id = 1`, vals);
}
export async function logReviewError(masterId, query, reason) {
    await getPool().query(`INSERT INTO track_yt_review_errors (master_id, query, reason) VALUES ($1, $2, $3)`, [masterId, query, reason.slice(0, 500)]);
}
export async function listReviewErrors(limit = 100) {
    const r = await getPool().query(`SELECT id, ts, master_id, query, reason
       FROM track_yt_review_errors
      ORDER BY ts DESC
      LIMIT $1`, [Math.max(1, Math.min(500, limit))]);
    return r.rows;
}
export async function bumpReviewCounter(field, by = 1) {
    await getPool().query(`UPDATE track_yt_review_state SET ${field} = ${field} + $1, last_run_at = NOW() WHERE id = 1`, [by]);
}
// Persisted daily-quota counters. quota_date is the UTC YYYY-MM-DD the
// counters apply to; if today's UTC date differs, the counters reset.
// Reads return the post-reset values so callers can gate on them.
function _utcDateString(d = new Date()) {
    return d.toISOString().slice(0, 10);
}
export async function getReviewQuotaToday() {
    const today = _utcDateString();
    const r = await getPool().query(`SELECT quota_date, quota_worker_searches AS w, quota_project_units AS p
       FROM track_yt_review_state WHERE id = 1`);
    const row = r.rows[0];
    if (!row || row.quota_date !== today) {
        await getPool().query(`UPDATE track_yt_review_state
          SET quota_date = $1, quota_worker_searches = 0, quota_project_units = 0
        WHERE id = 1`, [today]);
        return { workerSearches: 0, projectUnits: 0 };
    }
    return { workerSearches: Number(row.w) || 0, projectUnits: Number(row.p) || 0 };
}
export async function bumpReviewQuota(workerSearches, projectUnits) {
    const today = _utcDateString();
    await getPool().query(`UPDATE track_yt_review_state
        SET quota_date = $1,
            quota_worker_searches = CASE WHEN quota_date = $1 THEN quota_worker_searches + $2 ELSE $2 END,
            quota_project_units   = CASE WHEN quota_date = $1 THEN quota_project_units   + $3 ELSE $3 END
      WHERE id = 1`, [today, workerSearches, projectUnits]);
}
// Finds the next earliest STRICT-Blues master (year ASC) past the
// cursor. "Strict" = genres array is exactly ['Blues'] (length 1) —
// a master tagged ['Blues','Rock'] is skipped. No-year masters are
// also skipped for now per the v1.1 spec; revisit when the dated
// catalog is exhausted. Returns null when no more matches exist past
// the cursor.
export async function getNextBluesMasterAfter(cursorYear, cursorMasterId) {
    const r = await getPool().query(`SELECT rc.discogs_id AS master_id,
            (rc.data->>'year')::int AS year,
            rc.data
       FROM release_cache rc
      WHERE rc.type = 'master'
        AND jsonb_typeof(rc.data->'genres') = 'array'
        AND jsonb_array_length(rc.data->'genres') = 1
        AND rc.data->'genres' ? 'Blues'
        AND rc.data->>'year' ~ '^[0-9]+$'
        AND (rc.data->>'year')::int > 0
        AND (rc.data->>'year')::int >= COALESCE($1, 0)
        AND ((rc.data->>'year')::int > COALESCE($1, 0)
             OR rc.discogs_id > COALESCE($2, 0))
      ORDER BY (rc.data->>'year')::int ASC, rc.discogs_id ASC
      LIMIT 1`, [cursorYear, cursorMasterId]);
    return r.rows[0] ?? null;
}
// Admin: delete a single override (called from the album popup or the
// admin tab). Returns true if a row was removed.
export async function deleteTrackYtOverride(releaseId, releaseType, trackPosition) {
    const r = await getPool().query(`DELETE FROM track_youtube_overrides
      WHERE release_id = $1 AND release_type = $2 AND track_position = $3`, [String(releaseId), releaseType, trackPosition]);
    return (r.rowCount ?? 0) > 0;
}
// Batch lookup: for a list of {id,type} items, return whether each
// has videos available (either from Discogs's cached videos[] array
// or from a crowd-sourced track_youtube_overrides row). Returns
// hasVideos: null when the item isn't in the cache yet — caller treats
// null as "unknown" and skips badging that card.
//
// Used by the "Hard to Find" search mode to mark cards that lack any
// embedded YT, so users can see at a glance where contributions help.
export async function getVideoStatusBatch(items) {
    if (!items.length)
        return [];
    const masterIds = items.filter(i => i.type === "master").map(i => i.id);
    const releaseIds = items.filter(i => i.type === "release").map(i => i.id);
    const cacheRows = [];
    if (masterIds.length) {
        const r = await getPool().query(`SELECT discogs_id AS id, 'master'::text AS type,
              (jsonb_typeof(data->'videos') = 'array' AND jsonb_array_length(data->'videos') > 0) AS has_videos
         FROM release_cache
        WHERE type = 'master' AND discogs_id = ANY($1::int[])`, [masterIds]);
        cacheRows.push(...r.rows);
    }
    if (releaseIds.length) {
        const r = await getPool().query(`SELECT discogs_id AS id, 'release'::text AS type,
              (jsonb_typeof(data->'videos') = 'array' AND jsonb_array_length(data->'videos') > 0) AS has_videos
         FROM release_cache
        WHERE type = 'release' AND discogs_id = ANY($1::int[])`, [releaseIds]);
        cacheRows.push(...r.rows);
    }
    // Cross-check crowd-sourced overrides — a master with no Discogs
    // videos but with user-contributed overrides shouldn't still read as
    // "needs videos" in search.
    const allIds = items.map(i => String(i.id));
    let overrideRows = [];
    if (allIds.length) {
        const r = await getPool().query(`SELECT release_id AS id, release_type AS type
         FROM track_youtube_overrides
        WHERE release_id = ANY($1::text[])`, [allIds]);
        overrideRows = r.rows;
    }
    const overrideSet = new Set(overrideRows.map(r => `${r.type}:${r.id}`));
    const cacheMap = new Map();
    for (const r of cacheRows)
        cacheMap.set(`${r.type}:${r.id}`, r.has_videos);
    return items.map(it => {
        const key = `${it.type}:${it.id}`;
        const cacheVal = cacheMap.get(key);
        const hasOverride = overrideSet.has(key);
        if (cacheVal == null) {
            // Not in cache — mark unknown unless an override exists (which
            // means the master was loaded enough to attract a contribution).
            return { id: it.id, type: it.type, hasVideos: hasOverride ? true : null };
        }
        return { id: it.id, type: it.type, hasVideos: cacheVal || hasOverride };
    });
}
// Albums the CURRENT user has personally submitted YouTube overrides
// for. Distinct by (release_id, release_type), joined with the
// release_cache snapshot so we can render cards without further
// Discogs round-trips. Used by the "Submitted" home-strip tab so
// each signed-in user sees their own contribution history rather
// than the global community feed.
export async function getUserSubmittedAlbums(clerkUserId, limit = 96) {
    try {
        const r = await getPool().query(`WITH counts AS (
         SELECT release_id, release_type,
                COUNT(*)::int     AS n,
                MIN(submitted_at) AS first_at,
                MAX(submitted_at) AS last_at
           FROM track_youtube_overrides
          WHERE submitted_by = $1
          GROUP BY release_id, release_type
       )
       SELECT c.release_id     AS id,
              c.release_type   AS type,
              c.n              AS contribution_count,
              c.first_at       AS first_contributed_at,
              c.last_at        AS last_contributed_at,
              rc.data          AS data
         FROM counts c
         LEFT JOIN release_cache rc
           ON rc.discogs_id = c.release_id::int
          AND rc.type       = c.release_type
        ORDER BY c.last_at DESC
        LIMIT $2`, [clerkUserId, Math.max(1, Math.min(500, limit))]);
        return r.rows;
    }
    catch {
        return [];
    }
}
// Pull masters/releases that have at least one crowd-sourced YouTube
// override, joined with release_cache so each row carries the card
// snapshot the home strip needs (orphan rows are skipped). Order is
// caller-selectable: "most" (default), "fewest", or "recent" — the
// last sorts by the most recently submitted override timestamp.
export async function getMostContributedAlbums(limit = 48, order = "most") {
    let orderBy;
    switch (order) {
        case "fewest":
            orderBy = "c.n ASC, c.release_id ASC";
            break;
        case "recent":
            orderBy = "c.last_at DESC, c.release_id DESC";
            break;
        case "most":
        default:
            orderBy = "c.n DESC, c.release_id DESC";
            break;
    }
    try {
        const useV2 = await isSplitCacheReaderEnabled();
        const cteAndJoin = useV2 ? `
      WITH counts AS (
        SELECT release_id, release_type,
               COUNT(*)::int AS n,
               MAX(submitted_at) AS last_at
          FROM track_youtube_overrides
         GROUP BY release_id, release_type
      ),
      cache_all AS (
        SELECT m.discogs_id, m.type, m.data FROM discogs_cache_masters_plus m
        UNION ALL
        SELECT p.discogs_id, 'release'::text AS type, p.data FROM discogs_cache_pressings p
      )
      SELECT c.release_id AS id, c.release_type AS type,
             c.n AS contribution_count, c.last_at AS last_contributed_at,
             ca.data AS data
        FROM counts c
        JOIN cache_all ca
          ON ca.discogs_id = c.release_id::int
         AND ca.type       = c.release_type
       ORDER BY ${orderBy.replace(/c\.release_id/g, "c.release_id")}
       LIMIT $1` : `
      WITH counts AS (
        SELECT release_id, release_type,
               COUNT(*)::int   AS n,
               MAX(submitted_at) AS last_at
          FROM track_youtube_overrides
         GROUP BY release_id, release_type
      )
      SELECT c.release_id   AS id,
             c.release_type AS type,
             c.n            AS contribution_count,
             c.last_at      AS last_contributed_at,
             rc.data        AS data
        FROM counts c
        JOIN release_cache rc
          ON rc.discogs_id = c.release_id::int
         AND rc.type       = c.release_type
       ORDER BY ${orderBy}
       LIMIT $1`;
        const r = await getPool().query(cteAndJoin, [Math.max(1, Math.min(200, limit))]);
        return r.rows;
    }
    catch {
        return [];
    }
}
// Active-tab sampler. Returns vinyl releases ranked by how many
// times anyone on the site has opened them in the last 90 days.
// Same exponential-weighted reservoir pattern as Feed so the same
// top-3 don't dominate every page — higher open counts surface
// more often but every counted row remains reachable.
export async function getFeedActiveAlbums(limit = 48, excludeIds) {
    try {
        const cap = Math.max(1, Math.min(200, limit));
        const exclude = Array.isArray(excludeIds) ? excludeIds.slice(0, 500) : [];
        const excludeIdsArr = exclude.map(e => Number(e.id));
        const excludeTypeArr = exclude.map(e => String(e.type));
        const params = [cap];
        if (await isSplitCacheReaderEnabled()) {
            let excludeClauseV2 = "";
            if (exclude.length) {
                params.push(excludeIdsArr);
                const idIdx = params.length;
                params.push(excludeTypeArr);
                const tyIdx = params.length;
                excludeClauseV2 = `AND NOT (ca.discogs_id = ANY($${idIdx}::int[]) AND ca.type = ANY($${tyIdx}::text[]))`;
            }
            // Vinyl filter dropped in V2: masters carry no data.formats at
            // all in Discogs's payload, so keeping the filter would silently
            // exclude every master row (same problem the Played fix solved).
            // Active means "opened lots recently"; if a master is what got
            // opened, it should surface.
            const sql = `
        WITH counts AS (
          SELECT urv.discogs_id, urv.entity_type, COUNT(*)::int AS n
            FROM user_recent_views urv
           WHERE urv.opened_at >= NOW() - INTERVAL '90 days'
           GROUP BY urv.discogs_id, urv.entity_type
        ),
        cache_all AS (
          SELECT m.discogs_id, m.type, m.data, m.cached_at, m.seen_at
            FROM discogs_cache_masters_plus m
          UNION ALL
          SELECT p.discogs_id, 'release'::text AS type, p.data, p.cached_at, p.seen_at
            FROM discogs_cache_pressings p
        )
        SELECT ca.discogs_id AS id, ca.type, ca.data, ca.cached_at
          FROM counts c
          JOIN cache_all ca
            ON ca.discogs_id = c.discogs_id
           AND ca.type       = c.entity_type
         WHERE ca.seen_at IS NOT NULL
           ${excludeClauseV2}
         ORDER BY -LN(RANDOM() + 1e-12) / GREATEST(LN(1 + c.n), 1)
         LIMIT $1`;
            const r = await getPool().query(sql, params);
            return r.rows;
        }
        let excludeClause = "";
        if (exclude.length) {
            params.push(excludeIdsArr);
            const idIdx = params.length;
            params.push(excludeTypeArr);
            const tyIdx = params.length;
            excludeClause = `AND NOT (rc.discogs_id = ANY($${idIdx}::int[]) AND rc.type = ANY($${tyIdx}::text[]))`;
        }
        const sql = `
      WITH counts AS (
        SELECT urv.discogs_id, urv.entity_type, COUNT(*)::int AS n
          FROM user_recent_views urv
         WHERE urv.opened_at >= NOW() - INTERVAL '90 days'
         GROUP BY urv.discogs_id, urv.entity_type
      )
      SELECT rc.discogs_id AS id, rc.type, rc.data, rc.cached_at
        FROM counts c
        JOIN release_cache rc
          ON rc.discogs_id = c.discogs_id
         AND rc.type       = c.entity_type
       WHERE rc.seen_at IS NOT NULL
         AND rc.data->'formats' @> '[{"name":"Vinyl"}]'::jsonb
         ${excludeClause}
       ORDER BY -LN(RANDOM() + 1e-12) / GREATEST(LN(1 + c.n), 1)
       LIMIT $1`;
        const r = await getPool().query(sql, params);
        return r.rows;
    }
    catch (e) {
        console.error("[getFeedActiveAlbums]", e?.message ?? e);
        return [];
    }
}
// Played-tab sampler. Driven by user_play_events (source='yt'). Two
// meaningful shifts from the earlier version:
//   * Rolls each play up under its master when a master_id is set,
//     otherwise under the release. Aligns with how the mini-player
//     actually logs — most listens are against the master, so
//     attributing to the specific pressing was fragmenting a single
//     album's playcount across multiple release rows AND filtering
//     out master-typed plays entirely at the release_cache join.
//   * Vinyl-only filter removed. Masters carry no data.formats at
//     all in Discogs's payload, so the old filter silently dropped
//     every master-typed play. Better to surface the album whatever
//     its format cluster is; the Feed / Rare tabs still enforce
//     Vinyl-only when the user wants that lens.
// Ordering stays the same weighted-random draw (higher play count
// = more likely to surface, but every counted row is reachable).
export async function getFeedPlayedAlbums(limit = 48, excludeIds) {
    try {
        const cap = Math.max(1, Math.min(200, limit));
        const exclude = Array.isArray(excludeIds) ? excludeIds.slice(0, 500) : [];
        const excludeIdsArr = exclude.map(e => Number(e.id));
        const excludeTypeArr = exclude.map(e => String(e.type));
        const params = [cap];
        let excludeClause = "";
        if (exclude.length) {
            params.push(excludeIdsArr);
            const idIdx = params.length;
            params.push(excludeTypeArr);
            const tyIdx = params.length;
            excludeClause = `AND NOT (rc.discogs_id = ANY($${idIdx}::int[]) AND rc.type = ANY($${tyIdx}::text[]))`;
        }
        const useV2 = await isSplitCacheReaderEnabled();
        const excludeClauseV2 = excludeClause.replace(/rc\./g, "ca.");
        const sql = useV2 ? `
      WITH counts AS (
        SELECT
          CASE WHEN upe.master_id IS NOT NULL THEN upe.master_id
               ELSE upe.release_id END                 AS discogs_id,
          CASE WHEN upe.master_id IS NOT NULL THEN 'master'
               ELSE 'release' END                       AS entity_type,
          COUNT(*)::int                                AS n
          FROM user_play_events upe
         WHERE upe.source = 'yt'
           AND upe.created_at >= NOW() - INTERVAL '90 days'
           AND (upe.release_id IS NOT NULL OR upe.master_id IS NOT NULL)
         GROUP BY 1, 2
      ),
      cache_all AS (
        SELECT m.discogs_id, m.type, m.data, m.cached_at, m.seen_at
          FROM discogs_cache_masters_plus m
        UNION ALL
        SELECT p.discogs_id, 'release'::text AS type, p.data, p.cached_at, p.seen_at
          FROM discogs_cache_pressings p
      )
      SELECT ca.discogs_id AS id, ca.type, ca.data, ca.cached_at
        FROM counts c
        JOIN cache_all ca
          ON ca.discogs_id = c.discogs_id
         AND ca.type       = c.entity_type
       WHERE ca.seen_at IS NOT NULL
         ${excludeClauseV2}
       ORDER BY -LN(RANDOM() + 1e-12) / GREATEST(LN(1 + c.n), 1)
       LIMIT $1` : `
      WITH counts AS (
        SELECT
          CASE WHEN upe.master_id IS NOT NULL THEN upe.master_id
               ELSE upe.release_id END                 AS discogs_id,
          CASE WHEN upe.master_id IS NOT NULL THEN 'master'
               ELSE 'release' END                       AS entity_type,
          COUNT(*)::int                                AS n
          FROM user_play_events upe
         WHERE upe.source = 'yt'
           AND upe.created_at >= NOW() - INTERVAL '90 days'
           AND (upe.release_id IS NOT NULL OR upe.master_id IS NOT NULL)
         GROUP BY 1, 2
      )
      SELECT rc.discogs_id AS id, rc.type, rc.data, rc.cached_at
        FROM counts c
        JOIN release_cache rc
          ON rc.discogs_id = c.discogs_id
         AND rc.type       = c.entity_type
       WHERE rc.seen_at IS NOT NULL
         ${excludeClause}
       ORDER BY -LN(RANDOM() + 1e-12) / GREATEST(LN(1 + c.n), 1)
       LIMIT $1`;
        const r = await getPool().query(sql, params);
        return r.rows;
    }
    catch (e) {
        console.error("[getFeedPlayedAlbums]", e?.message ?? e);
        return [];
    }
}
// Dig-tab sampler. Returns vinyl releases that NO ONE on the site
// has opened yet — i.e. zero rows in user_recent_views for the
// (discogs_id, entity_type) pair. Pure long-tail discovery: by
// definition these cards have never surfaced in anyone's Recent.
// Same TABLESAMPLE strategy as Feed so the anti-join doesn't have
// to scan the entire release_cache.
export async function getFeedDigAlbums(limit = 48, excludeIds) {
    try {
        const cap = Math.max(1, Math.min(200, limit));
        const exclude = Array.isArray(excludeIds) ? excludeIds.slice(0, 500) : [];
        const excludeIdsArr = exclude.map(e => Number(e.id));
        const excludeTypeArr = exclude.map(e => String(e.type));
        const params = [cap];
        let excludeClause = "";
        if (exclude.length) {
            params.push(excludeIdsArr);
            const idIdx = params.length;
            params.push(excludeTypeArr);
            const tyIdx = params.length;
            excludeClause = `AND NOT (rc.discogs_id = ANY($${idIdx}::int[]) AND rc.type = ANY($${tyIdx}::text[]))`;
        }
        if (await isSplitCacheReaderEnabled()) {
            const excludeClauseV2 = excludeClause.replace(/rc\./g, "sub.");
            // TABLESAMPLE runs per-table, so we sample masters_plus and
            // pressings independently and UNION. Vinyl filter dropped for
            // the same reason as Played/Active — masters carry no formats.
            const sql = `
        WITH sampled AS (
          SELECT m.discogs_id, m.type, m.data, m.cached_at, m.seen_at
            FROM discogs_cache_masters_plus m TABLESAMPLE SYSTEM (10)
          UNION ALL
          SELECT p.discogs_id, 'release'::text AS type, p.data, p.cached_at, p.seen_at
            FROM discogs_cache_pressings p TABLESAMPLE SYSTEM (10)
        )
        SELECT sub.discogs_id AS id, sub.type, sub.data, sub.cached_at
          FROM sampled sub
         WHERE sub.seen_at IS NOT NULL
           AND NOT EXISTS (
             SELECT 1
               FROM user_recent_views urv
              WHERE urv.discogs_id  = sub.discogs_id
                AND urv.entity_type = sub.type
           )
           ${excludeClauseV2}
         ORDER BY RANDOM()
         LIMIT $1`;
            const r = await getPool().query(sql, params);
            return r.rows;
        }
        const sql = `
      SELECT rc.discogs_id AS id, rc.type, rc.data, rc.cached_at
        FROM release_cache rc TABLESAMPLE SYSTEM (10)
       WHERE rc.type IN ('master','release')
         AND rc.seen_at IS NOT NULL
         AND rc.data->'formats' @> '[{"name":"Vinyl"}]'::jsonb
         AND NOT EXISTS (
           SELECT 1
             FROM user_recent_views urv
            WHERE urv.discogs_id  = rc.discogs_id
              AND urv.entity_type = rc.type
         )
         ${excludeClause}
       ORDER BY RANDOM()
       LIMIT $1`;
        const r = await getPool().query(sql, params);
        return r.rows;
    }
    catch (e) {
        console.error("[getFeedDigAlbums]", e?.message ?? e);
        return [];
    }
}
// Rare-tab sampler. Returns releases that satisfy ALL three:
//   - have <= 3   (basically nobody owns it)
//   - want >= 20  (but lots of people want it)
//   - year falls inside the "first two decades" window of at least
//     one of the release's genres. Windows are hardcoded below from
//     conventional genre-origin dates.
//
// When `genre` is supplied, the query restricts strictly to that
// genre and applies only its year window — so a "Blues" filter only
// returns Blues records from 1900-1925, with no other genres mixed
// in.
//
// ORDER BY is a want-weighted reservoir sample, not pure random:
// `-LN(R) / GREATEST(LN(1+want), 1)`. Higher-want rows tend to
// surface first while still leaving every match reachable.
const RARE_GENRE_WINDOWS = {
    "Blues": [1900, 1925],
    "Folk, World, & Country": [1900, 1925],
    "Jazz": [1917, 1937],
    "Classical": [1900, 1920],
    "Stage & Screen": [1900, 1925],
    "Latin": [1930, 1955],
    "Rock": [1955, 1975],
    "Pop": [1950, 1970],
    "Reggae": [1960, 1980],
    "Funk / Soul": [1960, 1980],
    "Brass & Military": [1900, 1925],
    "Electronic": [1970, 1990],
    "Hip Hop": [1979, 1999],
};
export async function getFeedRareAlbums(limit = 48, excludeIds, genre, opts = {}) {
    const reader = opts.forceReader ??
        ((await isSplitCacheReaderEnabled()) ? "v2" : "v1");
    return reader === "v2"
        ? _getFeedRareAlbumsV2(limit, excludeIds, genre)
        : _getFeedRareAlbumsV1(limit, excludeIds, genre);
}
async function _getFeedRareAlbumsV1(limit, excludeIds, genre) {
    try {
        const cap = Math.max(1, Math.min(200, limit));
        const exclude = Array.isArray(excludeIds) ? excludeIds.slice(0, 500) : [];
        const excludeIdsArr = exclude.map(e => Number(e.id));
        const excludeTypeArr = exclude.map(e => String(e.type));
        const params = [cap];
        let excludeClause = "";
        if (exclude.length) {
            params.push(excludeIdsArr);
            const idIdx = params.length;
            params.push(excludeTypeArr);
            const tyIdx = params.length;
            excludeClause = `AND NOT (rc.discogs_id = ANY($${idIdx}::int[]) AND rc.type = ANY($${tyIdx}::text[]))`;
        }
        // Strict-genre path: when the caller picks a specific genre, the
        // query is much simpler — filter strictly on that genre + its
        // single year window, no per-row EXISTS over multi-genre joins.
        const strictWindow = genre ? RARE_GENRE_WINDOWS[genre] : null;
        if (genre && strictWindow) {
            params.push(genre);
            const gIdx = params.length;
            const sql = `
        SELECT rc.discogs_id AS id, rc.type, rc.data, rc.cached_at
          FROM release_cache rc
         WHERE rc.type IN ('master','release')
           AND rc.seen_at IS NOT NULL
           AND rc.data->'genres' ? $${gIdx}
           AND rc.data->'formats' @> '[{"name":"Vinyl"}]'::jsonb
           AND COALESCE(NULLIF(rc.data->>'year','')::int, 0) BETWEEN ${strictWindow[0]} AND ${strictWindow[1]}
           AND COALESCE(NULLIF(rc.data->'community'->>'have','')::int, 0) < 10
           AND COALESCE(NULLIF(rc.data->'community'->>'want','')::int, 0) > 20
           AND COALESCE(NULLIF(rc.data->>'num_for_sale','')::int, 0) = 0
           ${excludeClause}
         ORDER BY -LN(RANDOM() + 1e-12) / GREATEST(
           LN(1 + COALESCE(NULLIF(rc.data->'community'->>'want','')::int, 0)),
           1
         )
         LIMIT $1`;
            const r = await getPool().query(sql, params);
            return r.rows;
        }
        // GIN index on (data->'genres') lets Postgres use ?| to narrow
        // candidates BEFORE we touch community.have / community.want /
        // year. The year_int cap is a coarse pre-filter (1900-1999 spans
        // every per-genre window), letting the EXISTS check work on a
        // tiny set.
        const sql = `
      WITH genre_window(genre, from_year, to_year) AS (
        VALUES
          ('Blues',                   1900, 1925),
          ('Folk, World, & Country',  1900, 1925),
          ('Jazz',                    1917, 1937),
          ('Classical',               1900, 1920),
          ('Stage & Screen',          1900, 1925),
          ('Latin',                   1930, 1955),
          ('Rock',                    1955, 1975),
          ('Pop',                     1950, 1970),
          ('Reggae',                  1960, 1980),
          ('Funk / Soul',             1960, 1980),
          ('Brass & Military',        1900, 1925),
          ('Electronic',              1970, 1990),
          ('Hip Hop',                 1979, 1999)
      ),
      candidates AS (
        SELECT rc.discogs_id AS id, rc.type, rc.data, rc.cached_at,
               COALESCE(NULLIF(rc.data->>'year','')::int, 0) AS year_int,
               COALESCE(NULLIF(rc.data->'community'->>'want','')::int, 0) AS want_int
          FROM release_cache rc
         WHERE rc.type IN ('master','release')
           AND rc.seen_at IS NOT NULL
           AND rc.data->'genres' ?| ARRAY[
               'Blues','Folk, World, & Country','Jazz','Classical',
               'Stage & Screen','Latin','Rock','Pop','Reggae',
               'Funk / Soul','Brass & Military','Electronic','Hip Hop'
             ]
           AND rc.data->'formats' @> '[{"name":"Vinyl"}]'::jsonb
           AND COALESCE(NULLIF(rc.data->>'year','')::int, 0) BETWEEN 1900 AND 1999
           AND COALESCE(NULLIF(rc.data->'community'->>'have','')::int, 0) < 10
           AND COALESCE(NULLIF(rc.data->'community'->>'want','')::int, 0) > 20
           AND COALESCE(NULLIF(rc.data->>'num_for_sale','')::int, 0) = 0
           ${excludeClause}
         LIMIT 5000
      )
      SELECT c.id, c.type, c.data, c.cached_at
        FROM candidates c
       WHERE EXISTS (
         SELECT 1
           FROM jsonb_array_elements_text(c.data->'genres') AS g(name)
           JOIN genre_window gw ON gw.genre = g.name
          WHERE c.year_int BETWEEN gw.from_year AND gw.to_year
       )
       ORDER BY -LN(RANDOM() + 1e-12) / GREATEST(LN(1 + c.want_int), 1)
       LIMIT $1`;
        const r = await getPool().query(sql, params);
        return r.rows;
    }
    catch (e) {
        console.error("[getFeedRareAlbums]", e?.message ?? e);
        return [];
    }
}
// V2 reader — hits the promoted community_want / community_have /
// num_for_sale columns instead of jsonb->>'community'->>'want' etc.
// The Rare filter (want>20 AND have<10 AND num_for_sale=0) rides the
// dcmp_rare_idx / dcp_rare_idx partial indexes, so the candidate
// pool is pre-filtered before any per-row work. Genre / format
// checks stay on the release_tags side table.
async function _getFeedRareAlbumsV2(limit, excludeIds, genre) {
    try {
        const cap = Math.max(1, Math.min(200, limit));
        const exclude = Array.isArray(excludeIds) ? excludeIds.slice(0, 500) : [];
        const excludeIdsArr = exclude.map(e => Number(e.id));
        const excludeTypeArr = exclude.map(e => String(e.type));
        const params = [cap];
        let excludeClause = "";
        if (exclude.length) {
            params.push(excludeIdsArr);
            const idIdx = params.length;
            params.push(excludeTypeArr);
            const tyIdx = params.length;
            excludeClause = `AND NOT (ca.discogs_id = ANY($${idIdx}::int[]) AND ca.type = ANY($${tyIdx}::text[]))`;
        }
        // Vinyl match via release_tags with kind='format' — matches the
        // V1 semantics (data->'formats' @> [{name:Vinyl}]) exactly since
        // release_tags stores every format entry, not just the primary.
        const vinylExists = `EXISTS (
      SELECT 1 FROM release_tags rt
       WHERE rt.discogs_id = ca.discogs_id AND rt.bucket = ca.bucket
         AND rt.kind = 'format' AND rt.value = 'Vinyl')`;
        const strictWindow = genre ? RARE_GENRE_WINDOWS[genre] : null;
        if (genre && strictWindow) {
            params.push(genre);
            const gIdx = params.length;
            const sql = `
        WITH cache_all AS (
          SELECT m.discogs_id, m.type, m.year, m.data, m.cached_at, m.seen_at,
                 m.community_want, m.community_have, m.num_for_sale,
                 CASE WHEN m.type = 'master' THEN 0::smallint ELSE 1::smallint END AS bucket
            FROM discogs_cache_masters_plus m
          UNION ALL
          SELECT p.discogs_id, 'release'::text AS type, p.year, p.data, p.cached_at, p.seen_at,
                 p.community_want, p.community_have, p.num_for_sale,
                 2::smallint AS bucket
            FROM discogs_cache_pressings p
        )
        SELECT ca.discogs_id AS id, ca.type, ca.data, ca.cached_at
          FROM cache_all ca
         WHERE ca.seen_at IS NOT NULL
           AND ca.year BETWEEN ${strictWindow[0]} AND ${strictWindow[1]}
           AND ca.community_want IS NOT NULL AND ca.community_want > 20
           AND ca.community_have IS NOT NULL AND ca.community_have < 10
           AND ca.num_for_sale = 0
           AND ${vinylExists}
           AND EXISTS (SELECT 1 FROM release_tags rt
                        WHERE rt.discogs_id = ca.discogs_id AND rt.bucket = ca.bucket
                          AND rt.kind = 'genre' AND rt.value = $${gIdx})
           ${excludeClause}
         ORDER BY -LN(RANDOM() + 1e-12) / GREATEST(LN(1 + ca.community_want), 1)
         LIMIT $1`;
            const r = await getPool().query(sql, params);
            return r.rows;
        }
        // Multi-genre path: use the same fixed year-window table but as
        // a JOIN to release_tags (kind='genre') so the year check applies
        // per row's actual genre membership. Coarse pre-filter on year
        // BETWEEN 1900 AND 1999 to shrink the candidate pool before the
        // per-genre EXISTS runs.
        const sql = `
      WITH genre_window(genre, from_year, to_year) AS (
        VALUES
          ('Blues',                   1900, 1925),
          ('Folk, World, & Country',  1900, 1925),
          ('Jazz',                    1917, 1937),
          ('Classical',               1900, 1920),
          ('Stage & Screen',          1900, 1925),
          ('Latin',                   1930, 1955),
          ('Rock',                    1955, 1975),
          ('Pop',                     1950, 1970),
          ('Reggae',                  1960, 1980),
          ('Funk / Soul',             1960, 1980),
          ('Brass & Military',        1900, 1925),
          ('Electronic',              1970, 1990),
          ('Hip Hop',                 1979, 1999)
      ),
      cache_all AS (
        SELECT m.discogs_id, m.type, m.year, m.data, m.cached_at, m.seen_at,
               m.community_want, m.community_have, m.num_for_sale,
               CASE WHEN m.type = 'master' THEN 0::smallint ELSE 1::smallint END AS bucket
          FROM discogs_cache_masters_plus m
        UNION ALL
        SELECT p.discogs_id, 'release'::text AS type, p.year, p.data, p.cached_at, p.seen_at,
               p.community_want, p.community_have, p.num_for_sale,
               2::smallint AS bucket
          FROM discogs_cache_pressings p
      ),
      candidates AS (
        SELECT ca.discogs_id AS id, ca.type, ca.data, ca.cached_at, ca.bucket, ca.year,
               ca.community_want AS want_int
          FROM cache_all ca
         WHERE ca.seen_at IS NOT NULL
           AND ca.year BETWEEN 1900 AND 1999
           AND ca.community_want IS NOT NULL AND ca.community_want > 20
           AND ca.community_have IS NOT NULL AND ca.community_have < 10
           AND ca.num_for_sale = 0
           AND ${vinylExists}
           ${excludeClause}
         LIMIT 5000
      )
      SELECT c.id, c.type, c.data, c.cached_at
        FROM candidates c
       WHERE EXISTS (
         SELECT 1
           FROM release_tags rt
           JOIN genre_window gw ON gw.genre = rt.value
          WHERE rt.discogs_id = c.id AND rt.bucket = c.bucket
            AND rt.kind = 'genre'
            AND c.year BETWEEN gw.from_year AND gw.to_year
       )
       ORDER BY -LN(RANDOM() + 1e-12) / GREATEST(LN(1 + c.want_int), 1)
       LIMIT $1`;
        const r = await getPool().query(sql, params);
        return r.rows;
    }
    catch (e) {
        console.error("[getFeedRareAlbums V2]", e?.message ?? e);
        return [];
    }
}
// Per-user taste profile, fetched-or-computed. Returns the cached row
// when it's <24h old; otherwise re-aggregates from user_collection
// (top 10 genres + top 20 styles by collection count) and upserts.
// Anons / users with empty profiles get null — callers should treat
// that as "no bias."
//
// Each map is normalized so the sum is 1.0 (or 0 when the user has
// no collection). The Feed sampler reads these weights directly so
// stronger preferences get a bigger boost than weak ones.
export async function getOrComputeUserTasteProfile(clerkUserId) {
    if (!clerkUserId)
        return null;
    const pool = getPool();
    try {
        const existing = await pool.query(`SELECT top_genres, top_styles, genre_scores, style_scores, computed_at
         FROM user_taste_profile
        WHERE clerk_user_id = $1`, [clerkUserId]);
        const row = existing.rows[0];
        const fresh = row && (Date.now() - new Date(row.computed_at).getTime()) < 24 * 3600 * 1000;
        const hasScores = row && row.genre_scores && Object.keys(row.genre_scores).length > 0;
        if (fresh && hasScores) {
            return {
                topGenres: row.top_genres || [],
                topStyles: row.top_styles || [],
                genreScores: row.genre_scores || {},
                styleScores: row.style_scores || {},
            };
        }
        // Recompute from collection. Both queries use jsonb_array_elements_text
        // over data->'genres' / 'styles' — the same shape used elsewhere
        // (see getCollectionFacets in db.ts).
        const g = await pool.query(`SELECT g, COUNT(*)::int AS n
         FROM user_collection, jsonb_array_elements_text(data->'genres') AS g
        WHERE clerk_user_id = $1
        GROUP BY g
        ORDER BY n DESC
        LIMIT 10`, [clerkUserId]);
        const s = await pool.query(`SELECT s, COUNT(*)::int AS n
         FROM user_collection, jsonb_array_elements_text(data->'styles') AS s
        WHERE clerk_user_id = $1
        GROUP BY s
        ORDER BY n DESC
        LIMIT 20`, [clerkUserId]);
        const gRows = g.rows.filter(r => r.g);
        const sRows = s.rows.filter(r => r.s);
        const gTotal = gRows.reduce((acc, r) => acc + Number(r.n || 0), 0);
        const sTotal = sRows.reduce((acc, r) => acc + Number(r.n || 0), 0);
        const topGenres = gRows.map(r => String(r.g));
        const topStyles = sRows.map(r => String(r.s));
        const genreScores = {};
        const styleScores = {};
        if (gTotal > 0)
            for (const r of gRows)
                genreScores[String(r.g)] = Number(r.n) / gTotal;
        if (sTotal > 0)
            for (const r of sRows)
                styleScores[String(r.s)] = Number(r.n) / sTotal;
        await pool.query(`INSERT INTO user_taste_profile (clerk_user_id, top_genres, top_styles, genre_scores, style_scores, computed_at)
       VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, NOW())
       ON CONFLICT (clerk_user_id) DO UPDATE
         SET top_genres   = EXCLUDED.top_genres,
             top_styles   = EXCLUDED.top_styles,
             genre_scores = EXCLUDED.genre_scores,
             style_scores = EXCLUDED.style_scores,
             computed_at  = NOW()`, [clerkUserId, topGenres, topStyles, JSON.stringify(genreScores), JSON.stringify(styleScores)]);
        return { topGenres, topStyles, genreScores, styleScores };
    }
    catch (e) {
        console.error("[getOrComputeUserTasteProfile]", e?.message ?? e);
        return null;
    }
}
// Random sample of cached albums for the public Feed strip — anon
// visitors see this as their home view (signed-in users get it as the
// "Feed" tab in the Recent/Suggestions/Submitted/Feed strip). All
// rows already paid for via the release_cache; no upstream Discogs
// hit. Defaults to masters (richer card data, broader scope) but
// callers can opt for either via `type`.
//
// `tasteProfile` is an optional soft bias: when supplied with
// genre/style scores (normalized to sum 1 per map), each card's score
// is multiplied by `1 + 1.0×(sum of matched genre weights) +
// 0.5×(sum of matched style weights)` — so a card matching your #1
// genre gets a bigger boost than one matching your #10. Anons or
// users with empty profiles pass null and the sampler runs unbiased.
export async function getFeedRandomAlbums(limit = 48, type = "any", excludeIds, tasteProfile) {
    try {
        const cap = Math.max(1, Math.min(200, limit));
        const exclude = Array.isArray(excludeIds) ? excludeIds.slice(0, 500) : [];
        const excludeIdsArr = exclude.map(e => Number(e.id));
        const excludeTypeArr = exclude.map(e => String(e.type));
        const params = [cap];
        let where = "";
        if (type !== "any") {
            params.push(type);
            where += `WHERE rc.type = $${params.length} `;
        }
        else {
            where += `WHERE rc.type IN ('master','release') `;
        }
        where += "AND rc.seen_at IS NOT NULL ";
        if (exclude.length) {
            params.push(excludeIdsArr);
            const idIdx = params.length;
            params.push(excludeTypeArr);
            const tyIdx = params.length;
            where += `AND NOT (rc.discogs_id = ANY($${idIdx}::int[]) AND rc.type = ANY($${tyIdx}::text[])) `;
        }
        // Soft taste bias — when the caller passed a profile with score
        // maps, push them as JSONB params. The scored CTE multiplies each
        // card's score by 1 + 1.0×(sum of matched genre weights) +
        // 0.5×(sum of matched style weights) so stronger preferences get
        // a bigger boost. Empty objects disable the bias without changing
        // the query plan.
        const gScores = tasteProfile?.genreScores ?? {};
        const sScores = tasteProfile?.styleScores ?? {};
        params.push(JSON.stringify(gScores));
        const gjIdx = params.length;
        params.push(JSON.stringify(sScores));
        const sjIdx = params.length;
        // ── Weighted-random feed sample ───────────────────────────────
        // Each candidate gets a composite score from four normalized
        // signals; the final ORDER BY is an exponential-weighted reservoir
        // sample (`-LN(R) / score`) so high-score rows surface more often
        // without being deterministic. Every row remains reachable.
        //
        //   yt_score       — Discogs videos[] count / tracklist length,
        //                    clamped to [0,1].
        //   scarcity_score — log-scaled community.want / GREATEST(have, 1).
        //                    High when lots of collectors want the record
        //                    but few own it ("rare and desired"). Replaces
        //                    raw want_score because raw want is dominated
        //                    by very popular (and very COMMON) releases.
        //   sale_score     — log-scaled num_for_sale from price_cache.
        //                    Joined only for release rows. Small weight —
        //                    Feed isn't a marketplace surface.
        //
        // Weights 0.35 yt / 0.55 scarcity / 0.10 sale, multiplied by a
        // 1.5× taste bonus when the card's genres/styles overlap the
        // user's profile (no overlap or empty profile = ×1.0).
        //
        // Perf: scoring the entire release_cache + reservoir-sorting it
        // hits statement_timeout once the cache grows past a few hundred
        // thousand rows. TABLESAMPLE SYSTEM(5) cuts the candidate pool to
        // ~5% of pages BEFORE the JSON extractions and LNs run, so the
        // scoring/sort phase only handles a small subset. Each request
        // re-samples (no `REPEATABLE`), so callers still see fresh cards.
        // The cost is that any single response only covers a slice of the
        // cache — Load More re-samples to fill in more.
        const useV2 = await isSplitCacheReaderEnabled();
        // Shared scored + ORDER BY tail. Reads tl_count / vd_count as
        // plain ints so both V1 (jsonb_array_length) and V2 (promoted
        // scalar) provide the same shape.
        const scoredTail = `
      scored AS (
        SELECT id, type, data, cached_at,
               CASE
                 WHEN tl_count > 0
                 THEN LEAST(1.0, vd_count::float / tl_count::float)
                 ELSE 0
               END AS yt_score,
               LN(1 + want_int::float / GREATEST(have_int, 1)) AS scarcity_score,
               LN(1 + sale_int) AS sale_score,
               CASE
                 WHEN $${gjIdx}::jsonb = '{}'::jsonb
                  AND $${sjIdx}::jsonb = '{}'::jsonb
                 THEN 1.0
                 ELSE 1.0
                      + 1.0 * COALESCE((
                          SELECT SUM(($${gjIdx}::jsonb->>gname.value)::float)
                            FROM jsonb_array_elements_text(
                              CASE WHEN jsonb_typeof(gs) = 'array' THEN gs ELSE '[]'::jsonb END
                            ) AS gname(value)
                           WHERE $${gjIdx}::jsonb ? gname.value
                        ), 0)
                      + 0.5 * COALESCE((
                          SELECT SUM(($${sjIdx}::jsonb->>sname.value)::float)
                            FROM jsonb_array_elements_text(
                              CASE WHEN jsonb_typeof(sts) = 'array' THEN sts ELSE '[]'::jsonb END
                            ) AS sname(value)
                           WHERE $${sjIdx}::jsonb ? sname.value
                        ), 0)
               END AS taste_mult
          FROM raw
      )
      SELECT id, type, data, cached_at
        FROM scored
       ORDER BY -LN(RANDOM() + 1e-12) / GREATEST(
         taste_mult * (
           0.35 * yt_score
         + 0.55 * (scarcity_score / 3.0)
         + 0.10 * (sale_score / 5.0)
         ),
         0.05
       )
       LIMIT $1`;
        let sql;
        if (useV2) {
            // TABLESAMPLE on each split table, UNION, then dress with the
            // promoted community/videos/tracks scalars (no JSON scans).
            // Vinyl filter dropped for parity with the other V2 samplers —
            // masters carry no formats so keeping it drops every master.
            const typeGate = type === "any"
                ? "s.type IN ('master','release')"
                : `s.type = '${type}'`;
            const excludeClauseV2 = exclude.length
                ? `AND NOT (s.discogs_id = ANY($${gjIdx - 2}::int[]) AND s.type = ANY($${gjIdx - 1}::text[]))`
                : "";
            sql = `
        WITH sampled AS (
          SELECT m.discogs_id, m.type, m.data, m.cached_at, m.seen_at,
                 m.community_want, m.community_have,
                 m.videos_count, m.tracks_count
            FROM discogs_cache_masters_plus m TABLESAMPLE SYSTEM (5)
          UNION ALL
          SELECT p.discogs_id, 'release'::text AS type, p.data, p.cached_at, p.seen_at,
                 p.community_want, p.community_have,
                 p.videos_count, p.tracks_count
            FROM discogs_cache_pressings p TABLESAMPLE SYSTEM (5)
        ),
        raw AS (
          SELECT s.discogs_id AS id, s.type, s.data, s.cached_at,
                 COALESCE(s.community_want, 0)::int      AS want_int,
                 COALESCE(s.community_have, 0)::int      AS have_int,
                 COALESCE(pc.num_for_sale, 0)::int       AS sale_int,
                 COALESCE(s.tracks_count, 0)::int        AS tl_count,
                 COALESCE(s.videos_count, 0)::int        AS vd_count,
                 s.data->'genres'                        AS gs,
                 s.data->'styles'                        AS sts
            FROM sampled s
            LEFT JOIN price_cache pc
              ON pc.discogs_release_id = s.discogs_id
             AND s.type = 'release'
           WHERE s.seen_at IS NOT NULL
             AND ${typeGate}
             ${excludeClauseV2}
        ),
        ${scoredTail}`;
        }
        else {
            sql = `
        WITH raw AS (
          SELECT rc.discogs_id AS id, rc.type, rc.data, rc.cached_at,
                 COALESCE(NULLIF(rc.data->'community'->>'want','')::int, 0) AS want_int,
                 COALESCE(NULLIF(rc.data->'community'->>'have','')::int, 0) AS have_int,
                 COALESCE(pc.num_for_sale, 0)                               AS sale_int,
                 jsonb_array_length(CASE WHEN jsonb_typeof(rc.data->'tracklist') = 'array' THEN rc.data->'tracklist' ELSE '[]'::jsonb END) AS tl_count,
                 jsonb_array_length(CASE WHEN jsonb_typeof(rc.data->'videos')    = 'array' THEN rc.data->'videos'    ELSE '[]'::jsonb END) AS vd_count,
                 rc.data->'genres'                                          AS gs,
                 rc.data->'styles'                                          AS sts
            FROM release_cache rc TABLESAMPLE SYSTEM (5)
            LEFT JOIN price_cache pc
              ON pc.discogs_release_id = rc.discogs_id
             AND rc.type = 'release'
            ${where}
             AND rc.data->'formats' @> '[{"name":"Vinyl"}]'::jsonb
        ),
        ${scoredTail}`;
        }
        const r = await getPool().query(sql, params);
        return r.rows;
    }
    catch (e) {
        console.error("[getFeedRandomAlbums]", e?.message ?? e);
        return [];
    }
}
const _FEED_POOL_TARGET = 500;
// Read the cached pool for `mode` and return up to `limit` cards.
// excludeIds suppresses cards the client already showed. The JOIN to
// release_cache hydrates the snapshot the endpoint needs.
export async function getFeedPoolItems(mode, limit = 48, excludeIds) {
    try {
        const cap = Math.max(1, Math.min(200, limit));
        const exclude = Array.isArray(excludeIds) ? excludeIds.slice(0, 500) : [];
        const params = [mode, cap];
        let excludeClause = "";
        if (exclude.length) {
            params.push(exclude.map(e => Number(e.id)));
            const idIdx = params.length;
            params.push(exclude.map(e => String(e.type)));
            const tyIdx = params.length;
            excludeClause = `AND NOT (rc.discogs_id = ANY($${idIdx}::int[]) AND rc.type = ANY($${tyIdx}::text[]))`;
        }
        const sql = `
      SELECT rc.discogs_id AS id, rc.type, rc.data, rc.cached_at
        FROM feed_cache_pool fcp
        JOIN release_cache rc
          ON rc.discogs_id = fcp.discogs_id
         AND rc.type       = fcp.entity_type
       WHERE fcp.mode = $1
         AND rc.seen_at IS NOT NULL
         ${excludeClause}
       ORDER BY RANDOM()
       LIMIT $2`;
        const r = await getPool().query(sql, params);
        return r.rows;
    }
    catch (e) {
        console.error("[getFeedPoolItems]", e?.message ?? e);
        return [];
    }
}
// Returns the refresh age of each pool. Used by the scheduler so it
// can decide whether a particular mode is stale.
export async function getFeedPoolFreshness() {
    const out = {};
    try {
        const r = await getPool().query(`SELECT mode, COUNT(*)::int AS n, MAX(refreshed_at) AS refreshed_at
         FROM feed_cache_pool GROUP BY mode`);
        for (const row of r.rows) {
            out[row.mode] = { count: Number(row.n) || 0, refreshedAt: row.refreshed_at ?? null };
        }
    }
    catch (e) {
        console.error("[getFeedPoolFreshness]", e?.message ?? e);
    }
    return out;
}
// Run the slow per-mode query at POOL_TARGET size and replace the
// stored pool for that mode. Called by the scheduler every ~2h and
// also on-demand from the admin button. Wraps in a transaction so
// readers never see a half-populated mode.
export async function refreshFeedPool(mode) {
    let rows = [];
    switch (mode) {
        case "feed":
            rows = await getFeedRandomAlbums(_FEED_POOL_TARGET, "any", []);
            break;
        case "rare":
            rows = await getFeedRareAlbums(_FEED_POOL_TARGET, [], null);
            break;
        case "dig":
            rows = await getFeedDigAlbums(_FEED_POOL_TARGET, []);
            break;
        case "active":
            rows = await getFeedActiveAlbums(_FEED_POOL_TARGET, []);
            break;
        case "played":
            rows = await getFeedPlayedAlbums(_FEED_POOL_TARGET, []);
            break;
    }
    const ids = rows.map(r => Number(r.id)).filter(Number.isFinite);
    const types = rows.map(r => String(r.type));
    if (!ids.length)
        return { inserted: 0 };
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        await client.query(`DELETE FROM feed_cache_pool WHERE mode = $1`, [mode]);
        await client.query(`INSERT INTO feed_cache_pool (mode, discogs_id, entity_type, score, refreshed_at)
         SELECT $1, x.id, x.ty, 1.0, NOW()
           FROM unnest($2::int[], $3::text[]) AS x(id, ty)
       ON CONFLICT (mode, discogs_id, entity_type) DO UPDATE
         SET score = EXCLUDED.score, refreshed_at = NOW()`, [mode, ids, types]);
        await client.query("COMMIT");
        console.log(`[refreshFeedPool] ${mode}: ${ids.length} rows`);
        return { inserted: ids.length };
    }
    catch (e) {
        await client.query("ROLLBACK");
        console.error(`[refreshFeedPool] ${mode}:`, e?.message ?? e);
        return { inserted: 0 };
    }
    finally {
        client.release();
    }
}
// Batched lookup of track_youtube_overrides for a list of
// (release_id, release_type) pairs. Used by the wide-card enrichment
// path so each card's tracks (and the special ALBUM full-album slot)
// can render play / queue buttons keyed off user-contributed overrides
// in addition to Discogs's own videos[]. Excludes rows whose video
// has been flagged "unavailable" via the LEFT JOIN against
// youtube_video_unavailable, mirroring getTrackYtOverrides.
export async function getTrackYtOverridesBatch(pairs) {
    if (!Array.isArray(pairs) || !pairs.length)
        return [];
    const capped = pairs.slice(0, 200).filter(p => Number.isFinite(Number(p.id)) && (p.type === "master" || p.type === "release"));
    if (!capped.length)
        return [];
    try {
        const ids = capped.map(p => String(p.id));
        const types = capped.map(p => String(p.type));
        const r = await getPool().query(`SELECT o.release_id, o.release_type, o.track_position, o.video_id, o.video_title
         FROM track_youtube_overrides o
         LEFT JOIN youtube_video_unavailable u
           ON u.video_id = o.video_id AND u.status = 'unavailable'
        WHERE (o.release_id, o.release_type) IN (
          SELECT * FROM unnest($1::text[], $2::text[])
        )
          AND u.video_id IS NULL`, [ids, types]);
        return r.rows;
    }
    catch {
        return [];
    }
}
// Batched lookup of release_cache rows for a list of (id, type)
// pairs. Used by /api/cards/enrich to backfill the wide-card image
// strip + inline tracklist on any card surface (favorites, search
// results, collection, etc.) without requiring each endpoint to
// JOIN with release_cache itself. Returns only rows that exist —
// cache misses are silently dropped.
export async function getCacheEnrichmentBatch(pairs, opts = {}) {
    if (!Array.isArray(pairs) || !pairs.length)
        return [];
    const reader = opts.forceReader ??
        ((await isSplitCacheReaderEnabled()) ? "v2" : "v1");
    return reader === "v2"
        ? _getCacheEnrichmentBatchV2(pairs)
        : _getCacheEnrichmentBatchV1(pairs);
}
async function _getCacheEnrichmentBatchV1(pairs) {
    // Cap input size to keep the query bounded; at 200 pairs the
    // query fingerprint is still small (~3KB JSON) and the unnest
    // cost is negligible.
    const capped = pairs.slice(0, 200).filter(p => Number.isFinite(Number(p.id)) && (p.type === "master" || p.type === "release"));
    if (!capped.length)
        return [];
    try {
        const ids = capped.map(p => Number(p.id));
        const types = capped.map(p => String(p.type));
        const r = await getPool().query(`SELECT discogs_id AS id, type, data
         FROM release_cache
        WHERE (discogs_id, type) IN (
          SELECT * FROM unnest($1::int[], $2::text[])
        )`, [ids, types]);
        const hits = r.rows;
        // Cross-type fallback: for any (id, 'master') pair we missed on,
        // try to find ANY cached release whose data->>'master_id' matches
        // the master id, and surface its tracklist/images under the master
        // key.
        const hitKeys = new Set(hits.map(h => `${h.type}:${h.id}`));
        const missedMasterIds = capped
            .filter(p => p.type === "master" && !hitKeys.has(`master:${p.id}`))
            .map(p => Number(p.id));
        if (missedMasterIds.length) {
            try {
                const fb = await getPool().query(`SELECT DISTINCT ON ((data->>'master_id')::bigint)
                  (data->>'master_id')::bigint AS master_id,
                  data
             FROM release_cache
            WHERE type = 'release'
              AND (data->>'master_id') IS NOT NULL
              AND (data->>'master_id')::bigint = ANY($1::bigint[])
            ORDER BY (data->>'master_id')::bigint, discogs_id ASC`, [missedMasterIds]);
                for (const row of fb.rows) {
                    const mid = Number(row.master_id);
                    if (!Number.isFinite(mid) || mid <= 0)
                        continue;
                    hits.push({ id: mid, type: "master", data: row.data });
                }
            }
            catch { /* fallback is best-effort */ }
        }
        return hits;
    }
    catch {
        return [];
    }
}
// V2 reader — routes reads by type to the right split table and
// uses the promoted master_id INT column for the master-miss
// fallback instead of a JSONB extraction. discogs_cache_pressings
// carries master_id as an indexed INT column, so the fallback is a
// straight index seek. Masters + orphan releases both live in
// masters_plus keyed by (discogs_id, type) — same shape as the old
// primary lookup.
async function _getCacheEnrichmentBatchV2(pairs) {
    const capped = pairs.slice(0, 200).filter(p => Number.isFinite(Number(p.id)) && (p.type === "master" || p.type === "release"));
    if (!capped.length)
        return [];
    try {
        const masterIds = capped.filter(p => p.type === "master").map(p => Number(p.id));
        const releaseIds = capped.filter(p => p.type === "release").map(p => Number(p.id));
        const hits = [];
        // Masters live only in masters_plus.
        if (masterIds.length) {
            const r = await getPool().query(`SELECT discogs_id AS id, type, data
           FROM discogs_cache_masters_plus
          WHERE type = 'master' AND discogs_id = ANY($1::int[])`, [masterIds]);
            hits.push(...r.rows);
        }
        // Releases can be in either table — orphans live in masters_plus
        // (type='release'), pressings live in pressings.
        if (releaseIds.length) {
            const [a, b] = await Promise.all([
                getPool().query(`SELECT discogs_id AS id, type, data
             FROM discogs_cache_masters_plus
            WHERE type = 'release' AND discogs_id = ANY($1::int[])`, [releaseIds]),
                getPool().query(`SELECT discogs_id AS id, 'release'::text AS type, data
             FROM discogs_cache_pressings
            WHERE discogs_id = ANY($1::int[])`, [releaseIds]),
            ]);
            hits.push(...a.rows, ...b.rows);
        }
        // Master-miss fallback: any (id, 'master') that wasn't found gets
        // a representative pressing surfaced under the master key. Old
        // path did DISTINCT ON with a JSON extraction; new path is a
        // straight WHERE master_id = ANY($1) on an indexed INT column.
        const hitKeys = new Set(hits.map(h => `${h.type}:${h.id}`));
        const missedMasterIds = masterIds.filter(id => !hitKeys.has(`master:${id}`));
        if (missedMasterIds.length) {
            try {
                const fb = await getPool().query(`SELECT DISTINCT ON (master_id) master_id, data
             FROM discogs_cache_pressings
            WHERE master_id = ANY($1::int[])
            ORDER BY master_id, discogs_id ASC`, [missedMasterIds]);
                for (const row of fb.rows) {
                    const mid = Number(row.master_id);
                    if (!Number.isFinite(mid) || mid <= 0)
                        continue;
                    hits.push({ id: mid, type: "master", data: row.data });
                }
            }
            catch { /* fallback is best-effort */ }
        }
        return hits;
    }
    catch {
        return [];
    }
}
// AI-search exclusion list: a compact set of "Artist - Title" lines
// pulled from the user's collection + wantlist so the recommendation
// prompt can tell Claude what to avoid. Capped so the prompt stays
// within token budget — at 200 lines × ~50 chars = ~10KB which is
// fine. UNION-distinct keeps a single-line entry per album the user
// has in either list.
export async function getAiExclusionTitles(clerkUserId, limit = 200) {
    try {
        const r = await getPool().query(`SELECT title FROM (
         SELECT DISTINCT NULLIF(data->>'title', '') AS title
           FROM user_collection
          WHERE clerk_user_id = $1
         UNION
         SELECT DISTINCT NULLIF(data->>'title', '') AS title
           FROM user_wantlist
          WHERE clerk_user_id = $1
       ) sub
       WHERE title IS NOT NULL
       LIMIT $2`, [clerkUserId, Math.max(1, Math.min(500, limit))]);
        return r.rows.map(row => String(row.title || "")).filter(Boolean);
    }
    catch {
        return [];
    }
}
// ── Personal suggestions: taste profile + library dedup + persistence ──
//
// Taste tuples power the per-user background suggestions job. We
// expand the user's favorites + plays + collection + wantlist by
// (genre, style, year) and rank by a WEIGHTED frequency:
//   favorites 3x · plays 2x · collection 1x · wantlist 1x
// Collections accumulate noise (gifts, completionism, flips, dupes) so
// they're a weak taste proxy; an explicit favorite is the strongest
// signal, and what the user actually listens to (plays, recency-
// windowed + master/day-deduped) sits just under it. The job then
// queries Discogs for masters matching each tuple. Wide net by design.
//
// Play rows carry only source + external_id, so genre/style/year come
// from either the client-sent snapshot (play_meta) or, failing that,
// a release_cache lookup keyed by the logged Discogs release/master id.
// LOC/Archive plays have no Discogs id → release_id NULL → excluded.
// Engagement check for the suggestions job — answers "is it worth
// spending Discogs API budget on this user's feed?". Returns the
// signals needed for the scheduler to skip:
//   - hibernated:        user_tokens.hibernated_at IS NOT NULL
//                        (6mo inactive, already flagged)
//   - hasSuggestions:    user has any rows in user_personal_suggestions
//   - oldestSuggDays:    age in days of the user's oldest current
//                        suggestion (proxies "how long has the feed
//                        existed for them"); 0 if none.
//   - recentClicks:      count of the user's saved suggestions opened
//                        in user_recent_views in the last 30 days.
//                        Zero ⇒ they don't engage with the feed.
// One round-trip, all-in-one query.
export async function getUserSuggestionEngagement(clerkUserId) {
    try {
        const r = await getPool().query(`SELECT
         (SELECT hibernated_at IS NOT NULL FROM user_tokens WHERE clerk_user_id = $1) AS hibernated,
         (SELECT COUNT(*)::int FROM user_personal_suggestions WHERE clerk_user_id = $1) AS sugg_count,
         COALESCE(
           EXTRACT(EPOCH FROM (NOW() - (SELECT MIN(generated_at) FROM user_personal_suggestions WHERE clerk_user_id = $1))) / 86400.0,
           0
         )::float AS oldest_sugg_days,
         (
           SELECT COUNT(*)::int FROM user_personal_suggestions ps
            JOIN user_recent_views rv
              ON rv.clerk_user_id = ps.clerk_user_id
             AND rv.discogs_id    = ps.discogs_id
             AND rv.entity_type   = ps.entity_type
            WHERE ps.clerk_user_id = $1
              AND rv.opened_at > NOW() - INTERVAL '30 days'
         ) AS recent_clicks`, [clerkUserId]);
        const row = r.rows[0] || {};
        return {
            hibernated: !!row.hibernated,
            hasSuggestions: Number(row.sugg_count) > 0,
            oldestSuggDays: Number(row.oldest_sugg_days) || 0,
            recentClicks: Number(row.recent_clicks) || 0,
        };
    }
    catch {
        return { hibernated: false, hasSuggestions: false, oldestSuggDays: 0, recentClicks: 0 };
    }
}
// Cheap fingerprint of a user's taste-source state. The suggestions
// job compares the current signature to the one stored from its last
// successful run; if nothing has changed (no new plays, no new
// favorites, no collection/wantlist deltas), the run early-exits. Lets
// us run frequently without burning Discogs API budget on idle users.
export async function getUserTasteSignature(clerkUserId) {
    try {
        const r = await getPool().query(`SELECT
         COALESCE(EXTRACT(EPOCH FROM (SELECT MAX(created_at) FROM user_play_events WHERE clerk_user_id = $1))::bigint, 0) AS p,
         COALESCE(EXTRACT(EPOCH FROM (SELECT MAX(created_at) FROM user_favorites   WHERE clerk_user_id = $1))::bigint, 0) AS f,
         (SELECT COUNT(*)::int FROM user_collection WHERE clerk_user_id = $1) AS c,
         (SELECT COUNT(*)::int FROM user_wantlist   WHERE clerk_user_id = $1) AS w`, [clerkUserId]);
        const row = r.rows[0] || {};
        return `${row.p ?? 0}|${row.f ?? 0}|${row.c ?? 0}|${row.w ?? 0}`;
    }
    catch {
        return "";
    }
}
export async function getUserTasteTuples(clerkUserId, limit = 9) {
    try {
        const r = await getPool().query(`WITH plays AS (
         -- Play-derived taste: one row per (master/release, day) in the
         -- last 90 days so a track on repeat can't dominate. Metadata is
         -- the client snapshot when present, else resolved from
         -- release_cache via the logged Discogs id + type.
         SELECT DISTINCT ON (mkey, d) data
           FROM (
             SELECT
               COALESCE(NULLIF(pe.master_id, 0), pe.release_id) AS mkey,
               (pe.created_at)::date                            AS d,
               COALESCE(pe.play_meta, rc.data)                  AS data
             FROM user_play_events pe
             LEFT JOIN release_cache rc
               ON rc.discogs_id = pe.release_id
              AND rc.type       = COALESCE(pe.release_type, 'release')
             WHERE pe.clerk_user_id = $1
               AND pe.release_id IS NOT NULL
               AND pe.created_at > NOW() - INTERVAL '90 days'
           ) q
          WHERE q.data IS NOT NULL
          ORDER BY mkey, d
       ),
       src AS (
         SELECT data, 3 AS w FROM user_favorites  WHERE clerk_user_id = $1
         UNION ALL
         SELECT data, 2 AS w FROM plays
         UNION ALL
         SELECT data, 1 AS w FROM user_collection WHERE clerk_user_id = $1
         UNION ALL
         SELECT data, 1 AS w FROM user_wantlist   WHERE clerk_user_id = $1
       ),
       expanded AS (
         SELECT
           g.value AS genre,
           s.value AS style,
           NULLIF(src.data->>'year','')::int AS year,
           src.w   AS w
         FROM src,
              jsonb_array_elements_text(src.data->'genres') AS g(value),
              jsonb_array_elements_text(src.data->'styles') AS s(value)
       ),
       counts AS (
         SELECT genre, style, year, SUM(w)::int AS n
         FROM expanded
         WHERE year IS NOT NULL AND year > 1900 AND year < 2030
         GROUP BY genre, style, year
       )
       SELECT genre, style, year, n
         FROM counts
        ORDER BY n DESC, year DESC
        LIMIT $2`, [clerkUserId, Math.max(1, Math.min(50, limit))]);
        return r.rows;
    }
    catch {
        return [];
    }
}
// Set of master/release ids the user already has anywhere — used so
// the suggestions job doesn't surface stuff the user owns/wants. We
// pull master_ids when present (most releases carry them), plus the
// release ids themselves so a release-typed suggestion that matches
// is also dropped.
export async function getUserLibraryMasterIds(clerkUserId) {
    const out = new Set();
    try {
        const r = await getPool().query(`SELECT DISTINCT (data->>'master_id')::int AS m
         FROM (
           SELECT data FROM user_collection WHERE clerk_user_id = $1
           UNION ALL
           SELECT data FROM user_wantlist   WHERE clerk_user_id = $1
           UNION ALL
           SELECT data FROM user_inventory  WHERE clerk_user_id = $1
         ) s
        WHERE NULLIF(s.data->>'master_id','') IS NOT NULL`, [clerkUserId]);
        for (const row of r.rows) {
            const m = Number(row.m);
            if (Number.isFinite(m) && m > 0)
                out.add(m);
        }
    }
    catch { /* best-effort */ }
    return out;
}
// Diff-based merge: keep existing rows, add only the candidates the
// user hasn't seen yet. Returns the list of newly-added (id, type)
// pairs so the caller can enqueue cache-warm fetches just for the
// genuinely new ones (no point re-fetching what already cached).
//
// Old behaviour (replaceUserPersonalSuggestions) wipe-and-replace
// burned a full Discogs API budget every hour even when 95% of
// candidates overlapped with the previous run. mergeUserPersonalSuggestions
// keeps stable rows (no UI shuffle for users) and only writes the
// genuinely new finds.
//
// `excludeKeys` is the union of dismissals + recently-clicked +
// owned-library masters: candidates that match are skipped at insert
// time, AND existing rows matching those keys are deleted in the
// same transaction so already-clicked items get suppressed even on
// passes where they're not in the new candidate list.
export async function mergeUserPersonalSuggestions(clerkUserId, items, opts) {
    const exclude = opts?.excludeKeys ?? new Set();
    const maxRows = Math.max(50, Math.min(2000, opts?.maxRows ?? 1000));
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        // Suppress excluded keys (e.g. recently-clicked) from any existing
        // rows. They might have been added in a previous pass before the
        // user opened them.
        if (exclude.size) {
            const arr = Array.from(exclude);
            const ids = arr.map(k => Number(k.split(":")[1])).filter(n => Number.isFinite(n));
            const types = arr.map(k => k.split(":")[0]);
            if (ids.length) {
                await client.query(`DELETE FROM user_personal_suggestions
            WHERE clerk_user_id = $1
              AND (discogs_id, entity_type) IN (
                SELECT * FROM unnest($2::int[], $3::text[])
              )`, [clerkUserId, ids, types]);
            }
        }
        // Pull existing keys so we can tell new rows from updates.
        const existingRows = await client.query(`SELECT discogs_id, entity_type FROM user_personal_suggestions WHERE clerk_user_id = $1`, [clerkUserId]);
        const existing = new Set(existingRows.rows.map(r => `${r.entity_type}:${r.discogs_id}`));
        const added = [];
        let suppressed = 0;
        for (const it of items) {
            const key = `${it.type}:${it.id}`;
            if (exclude.has(key)) {
                suppressed++;
                continue;
            }
            const isNew = !existing.has(key);
            await client.query(`INSERT INTO user_personal_suggestions
           (clerk_user_id, discogs_id, entity_type, score, data, generated_at)
         VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
         ON CONFLICT (clerk_user_id, discogs_id, entity_type) DO UPDATE
           SET score = EXCLUDED.score, data = EXCLUDED.data`, // generated_at intentionally NOT touched on conflict
            [clerkUserId, it.id, it.type, it.score, JSON.stringify(it.data ?? {})]);
            if (isNew)
                added.push({ discogs_id: it.id, entity_type: it.type });
        }
        // Cap row count: drop the lowest-score rows past maxRows so the
        // table doesn't grow unbounded over many merges.
        await client.query(`DELETE FROM user_personal_suggestions
        WHERE clerk_user_id = $1
          AND (discogs_id, entity_type) NOT IN (
            SELECT discogs_id, entity_type FROM user_personal_suggestions
             WHERE clerk_user_id = $1
             ORDER BY score DESC, generated_at DESC
             LIMIT $2
          )`, [clerkUserId, maxRows]);
        await client.query("COMMIT");
        return { added, suppressed };
    }
    catch (e) {
        await client.query("ROLLBACK").catch(() => { });
        throw e;
    }
    finally {
        client.release();
    }
}
// Wipe + insert: the job overwrites the user's previous batch each
// run so the row count never grows beyond N per user. data is the
// card snapshot so the UI can render without further Discogs calls.
export async function replaceUserPersonalSuggestions(clerkUserId, items) {
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        await client.query(`DELETE FROM user_personal_suggestions WHERE clerk_user_id = $1`, [clerkUserId]);
        for (const it of items) {
            await client.query(`INSERT INTO user_personal_suggestions
           (clerk_user_id, discogs_id, entity_type, score, data, generated_at)
         VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
         ON CONFLICT (clerk_user_id, discogs_id, entity_type) DO UPDATE
           SET score = EXCLUDED.score, data = EXCLUDED.data, generated_at = NOW()`, [clerkUserId, it.id, it.type, it.score, JSON.stringify(it.data ?? {})]);
        }
        await client.query("COMMIT");
    }
    catch (e) {
        await client.query("ROLLBACK").catch(() => { });
        throw e;
    }
    finally {
        client.release();
    }
}
// Dismissals ("banish this suggestion forever") — record per-user so
// the generator skips them and any saved row is wiped immediately.
export async function dismissPersonalSuggestion(clerkUserId, discogsId, entityType) {
    await getPool().query(`INSERT INTO user_suggestion_dismissals (clerk_user_id, discogs_id, entity_type)
     VALUES ($1, $2, $3)
     ON CONFLICT (clerk_user_id, discogs_id, entity_type) DO NOTHING`, [clerkUserId, discogsId, entityType]);
    // Also remove from the saved batch so the card disappears on next render.
    await getPool().query(`DELETE FROM user_personal_suggestions
       WHERE clerk_user_id = $1 AND discogs_id = $2 AND entity_type = $3`, [clerkUserId, discogsId, entityType]);
}
// Returns set of "type:id" strings the user has banished so the
// generator can skip them in O(1). `withinDays` constrains to recent
// dismissals only — caller passes e.g. 90 to let older dismissals
// expire so the pool can thaw over time. Default unbounded preserves
// existing behaviour for any other caller.
export async function getDismissedSuggestionKeys(clerkUserId, withinDays) {
    const out = new Set();
    try {
        const params = [clerkUserId];
        let where = "clerk_user_id = $1";
        if (Number.isFinite(withinDays) && withinDays > 0) {
            const d = Math.max(1, Math.min(3650, Math.trunc(withinDays)));
            // Inline the integer (already clamped) — Postgres can't bind
            // intervals through parameters cleanly.
            where += ` AND dismissed_at > NOW() - INTERVAL '${d} days'`;
        }
        const r = await getPool().query(`SELECT discogs_id, entity_type FROM user_suggestion_dismissals WHERE ${where}`, params);
        for (const row of r.rows)
            out.add(`${row.entity_type}:${row.discogs_id}`);
    }
    catch { /* best effort */ }
    return out;
}
export async function getUserPersonalSuggestions(clerkUserId, limit = 1000) {
    try {
        const r = await getPool().query(`SELECT discogs_id, entity_type, score, data, generated_at
         FROM user_personal_suggestions
        WHERE clerk_user_id = $1
        ORDER BY score DESC, generated_at DESC
        LIMIT $2`, [clerkUserId, Math.max(1, Math.min(1000, limit))]);
        return r.rows;
    }
    catch {
        return [];
    }
}
// ── cache_fetch_queue helpers ─────────────────────────────────────
//
// Single chokepoint for "I want this album cached but I don't want
// to wait on a Discogs fetch." Anyone who wants an album cached
// drops a row in via enqueueCacheFetches; the rate-limited worker
// drains it. Dedupe is automatic via the unique constraint on
// (entity_type, discogs_id).
export async function enqueueCacheFetches(refs, source = "unknown", priority = 0) {
    if (!Array.isArray(refs) || !refs.length)
        return 0;
    // Skip rows already in release_cache — no point queueing a fetch
    // for something we already have. Doing this filter in SQL beats
    // round-tripping each id individually.
    const types = refs.map(r => r.entity_type);
    const ids = refs.map(r => Number(r.discogs_id));
    // INSERT ... SELECT ... LEFT JOIN release_cache so already-cached
    // rows skip insertion. ON CONFLICT bumps priority+source if the
    // new request is higher priority — useful for "user just clicked
    // this from search results, prioritize the prefetch."
    const r = await getPool().query(`INSERT INTO cache_fetch_queue (entity_type, discogs_id, source, priority)
     SELECT u.entity_type, u.discogs_id, $3, $4
       FROM unnest($1::text[], $2::int[]) AS u(entity_type, discogs_id)
       LEFT JOIN release_cache rc
         ON rc.discogs_id = u.discogs_id AND rc.type = u.entity_type
      WHERE rc.discogs_id IS NULL
     ON CONFLICT (entity_type, discogs_id) DO UPDATE
       SET priority = GREATEST(cache_fetch_queue.priority, EXCLUDED.priority),
           source   = CASE WHEN EXCLUDED.priority > cache_fetch_queue.priority
                            THEN EXCLUDED.source
                            ELSE cache_fetch_queue.source END`, [types, ids, source, priority]);
    return r.rowCount ?? 0;
}
// Pull the next batch of items to fetch. Highest priority first,
// oldest-requested-first within priority. Caller should fetch each
// and call markCacheFetchSucceeded / markCacheFetchFailed.
export async function dequeueCacheFetches(limit = 50) {
    const r = await getPool().query(`SELECT id, entity_type, discogs_id, source, attempts
       FROM cache_fetch_queue
      ORDER BY priority DESC, requested_at ASC
      LIMIT $1`, [Math.max(1, Math.min(1000, limit))]);
    return r.rows;
}
export async function markCacheFetchSucceeded(id) {
    await getPool().query(`DELETE FROM cache_fetch_queue WHERE id = $1`, [id]);
}
const _CACHE_FETCH_MAX_ATTEMPTS = 5;
// Increment attempts; drop the row once it's failed too many times
// so the queue doesn't accumulate permanent stuck entries (e.g.
// deleted Discogs IDs that always 404). Returns true if the row
// was kept, false if dropped.
export async function markCacheFetchFailed(id, error) {
    const r = await getPool().query(`UPDATE cache_fetch_queue
        SET attempts   = attempts + 1,
            last_error = $2
      WHERE id = $1
      RETURNING attempts`, [id, error.slice(0, 500)]);
    const attempts = r.rows[0]?.attempts ?? 0;
    if (attempts >= _CACHE_FETCH_MAX_ATTEMPTS) {
        await getPool().query(`DELETE FROM cache_fetch_queue WHERE id = $1`, [id]);
        return false;
    }
    return true;
}
export async function getCacheFetchQueueStats() {
    const totalRow = await getPool().query(`SELECT COUNT(*)::int AS total, MIN(requested_at) AS oldest_requested_at
       FROM cache_fetch_queue`);
    const sourceRows = await getPool().query(`SELECT source, COUNT(*)::int AS n
       FROM cache_fetch_queue
      GROUP BY source ORDER BY n DESC`);
    return {
        total: totalRow.rows[0]?.total ?? 0,
        oldest_requested_at: totalRow.rows[0]?.oldest_requested_at ?? null,
        by_source: sourceRows.rows,
    };
}
// ── Behavior-event logging ─────────────────────────────────────────
// Append-only writes — best-effort, never block a real request on
// the log write. Caller passes through `.catch(() => {})`.
export async function logUserSearch(clerkUserId, query) {
    if (!clerkUserId)
        return;
    await getPool().query(`INSERT INTO user_search_events (clerk_user_id, query) VALUES ($1, $2)`, [clerkUserId, (query || "").slice(0, 200)]);
}
export async function logUserPlay(clerkUserId, source, externalId, title, ident) {
    if (!clerkUserId || !externalId)
        return;
    const relType = ident?.releaseType === "release" || ident?.releaseType === "master"
        ? ident.releaseType : null;
    const relId = Number.isFinite(ident?.releaseId) && ident.releaseId > 0
        ? Math.trunc(ident.releaseId) : null;
    const masId = Number.isFinite(ident?.masterId) && ident.masterId > 0
        ? Math.trunc(ident.masterId) : null;
    // Keep only the three fields the taste expansion needs; drop anything
    // else the client may have sent so the JSONB stays small.
    let meta = null;
    if (ident?.meta && typeof ident.meta === "object") {
        const g = Array.isArray(ident.meta.genres) ? ident.meta.genres.slice(0, 12).map(String) : [];
        const s = Array.isArray(ident.meta.styles) ? ident.meta.styles.slice(0, 24).map(String) : [];
        const y = Number.isFinite(ident.meta.year) ? Number(ident.meta.year) : null;
        if (g.length || s.length || y)
            meta = { genres: g, styles: s, year: y };
    }
    await getPool().query(`INSERT INTO user_play_events
       (clerk_user_id, source, external_id, title, release_type, release_id, master_id, play_meta)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, [
        clerkUserId,
        source,
        String(externalId).slice(0, 100),
        (title || "").slice(0, 200) || null,
        relType,
        relId,
        masId,
        meta ? JSON.stringify(meta) : null,
    ]);
}
// Per-user behavior summary for the admin panel. Aggregates across
// existing tables (favorites, recent_views, suggestions) and the new
// event tables (searches, plays). Window is configurable; default is
// "all time" + last 30 days side by side.
export async function getUserBehaviorStats() {
    // Single query with LEFT JOINs on per-user counts. Building the
    // counts as subqueries keeps each row self-contained — no risk of
    // double-counting due to JOIN cardinality.
    // Use user_tokens as the user list — every signed-in user with a
    // Discogs connection has a row there. Pre-OAuth users (Clerk
    // account but no Discogs link) won't appear, but they also can't
    // search / favorite / play anything yet, so the omission is fine.
    // Union in any clerk_user_ids that DO have activity but no
    // user_tokens row, just in case (defensive — shouldn't happen in
    // practice but cheap to include).
    const r = await getPool().query(`
    WITH activity_users AS (
      -- Every clerk_user_id that has touched ANY tracked surface,
      -- not just user_tokens. Demo accounts authenticate via Clerk
      -- but route Discogs calls through the admin's OAuth — they
      -- never write a user_tokens row, so they were invisible here.
      -- Including every source table means anyone with at least
      -- one favorite / suggestion / view / play / search shows up.
      SELECT clerk_user_id FROM user_favorites
      UNION SELECT clerk_user_id FROM user_personal_suggestions
      UNION SELECT clerk_user_id FROM user_recent_views
      UNION SELECT clerk_user_id FROM user_play_events
      UNION SELECT clerk_user_id FROM user_search_events
    ), all_users AS (
      SELECT clerk_user_id, discogs_username, last_active_at, created_at FROM user_tokens
      UNION
      SELECT DISTINCT a.clerk_user_id, NULL::text, NULL::timestamptz, NULL::timestamptz
        FROM activity_users a
       WHERE NOT EXISTS (SELECT 1 FROM user_tokens t WHERE t.clerk_user_id = a.clerk_user_id)
    )
    SELECT u.clerk_user_id,
           u.discogs_username     AS username,
           COALESCE(f.n, 0)       AS favorites,
           COALESCE(s.n, 0)       AS suggestions_pool,
           COALESCE(sf.n, 0)      AS suggestions_favorited,
           COALESCE(rv.n_total, 0) AS album_clicks_total,
           COALESCE(rv.n_30d, 0)   AS album_clicks_30d,
           COALESCE(pe.n_total, 0) AS player_plays_total,
           COALESCE(pe.n_30d, 0)   AS player_plays_30d,
           COALESCE(se.n_total, 0) AS searches_total,
           COALESCE(se.n_30d, 0)   AS searches_30d,
           u.last_active_at        AS last_active,
           u.created_at            AS signed_up_at
      FROM all_users u
      LEFT JOIN (SELECT clerk_user_id, COUNT(*)::int AS n FROM user_favorites GROUP BY clerk_user_id) f
             ON f.clerk_user_id = u.clerk_user_id
      LEFT JOIN (SELECT clerk_user_id, COUNT(*)::int AS n FROM user_personal_suggestions GROUP BY clerk_user_id) s
             ON s.clerk_user_id = u.clerk_user_id
      LEFT JOIN (
        SELECT ps.clerk_user_id, COUNT(*)::int AS n
          FROM user_personal_suggestions ps
          JOIN user_favorites uf
            ON uf.clerk_user_id = ps.clerk_user_id
           AND uf.discogs_id    = ps.discogs_id
           AND uf.entity_type   = ps.entity_type
         GROUP BY ps.clerk_user_id
      ) sf ON sf.clerk_user_id = u.clerk_user_id
      LEFT JOIN (
        SELECT clerk_user_id,
               COUNT(*)::int AS n_total,
               COUNT(*) FILTER (WHERE opened_at >= NOW() - INTERVAL '30 days')::int AS n_30d
          FROM user_recent_views
         GROUP BY clerk_user_id
      ) rv ON rv.clerk_user_id = u.clerk_user_id
      LEFT JOIN (
        SELECT clerk_user_id,
               COUNT(*)::int AS n_total,
               COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days')::int AS n_30d
          FROM user_play_events
         GROUP BY clerk_user_id
      ) pe ON pe.clerk_user_id = u.clerk_user_id
      LEFT JOIN (
        SELECT clerk_user_id,
               COUNT(*)::int AS n_total,
               COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days')::int AS n_30d
          FROM user_search_events
         GROUP BY clerk_user_id
      ) se ON se.clerk_user_id = u.clerk_user_id
      ORDER BY u.last_active_at DESC NULLS LAST
  `);
    return r.rows;
}
// "Recently clicked" — return the set of "type:id" keys the user has
// opened in the last N days, pulled from recent_views. Used by the
// suggestions generator to soft-suppress albums the user already
// looked at: they don't get auto-dismissed (they may want to revisit)
// but the generator avoids re-suggesting them while interest is
// fresh, keeping the suggestions slot occupied by something new.
export async function getRecentlyClickedSuggestionKeys(clerkUserId, days = 30) {
    const out = new Set();
    try {
        const r = await getPool().query(`SELECT discogs_id, entity_type
         FROM user_recent_views
        WHERE clerk_user_id = $1
          AND opened_at >= NOW() - INTERVAL '${Math.max(1, Math.min(365, days))} days'
          AND entity_type IN ('master','release')`, [clerkUserId]);
        for (const row of r.rows) {
            out.add(`${row.entity_type}:${row.discogs_id}`);
        }
    }
    catch { /* table may not exist on cold install — best effort */ }
    return out;
}
// Suggestions cache-warm: list every (discogs_id, entity_type) pair
// that appears in user_personal_suggestions but is NOT yet in
// release_cache. The nightly cache-warm job uses this to know what
// to fetch from Discogs. Deduped across all users (one fetch covers
// every user who has the same suggestion). Sorted oldest-suggestion-
// first so newer suggestions wait their turn — fairness across users.
export async function getUncachedSuggestionRefs(limit = 5000) {
    const r = await getPool().query(`SELECT s.discogs_id, s.entity_type
       FROM (
         SELECT discogs_id, entity_type, MIN(generated_at) AS first_seen
           FROM user_personal_suggestions
           WHERE entity_type IN ('master', 'release')
           GROUP BY discogs_id, entity_type
       ) s
       LEFT JOIN release_cache rc
         ON rc.discogs_id = s.discogs_id AND rc.type = s.entity_type
      WHERE rc.discogs_id IS NULL
      ORDER BY s.first_seen ASC
      LIMIT $1`, [Math.max(1, Math.min(50000, limit))]);
    return r.rows;
}
// ── Unavailable YouTube videos ──────────────────────────────────────
// Threshold above which a flagged videoId graduates to "unavailable"
// and gets filtered out of every album popup site-wide. 2 reports is
// enough to suppress one-off network/region anomalies while not
// requiring a long stream of bad clicks before action.
const _YT_UNAVAILABLE_THRESHOLD = 2;
// Record a single "video failed to play" report. Increments the
// counter, refreshes last_reported_at, and flips status to
// 'unavailable' once the threshold is reached. Returns the post-
// update row so the caller can decide whether to act on the new
// status.
export async function reportYoutubeVideoUnavailable(videoId, reporterUserId, errorCode) {
    if (!/^[A-Za-z0-9_-]{11}$/.test(videoId)) {
        return { status: "invalid", report_count: 0 };
    }
    const r = await getPool().query(`INSERT INTO youtube_video_unavailable
       (video_id, status, report_count, sample_user_id, sample_error_code)
     VALUES ($1, 'flagged', 1, $2, $3)
     ON CONFLICT (video_id) DO UPDATE
       SET report_count     = youtube_video_unavailable.report_count + 1,
           last_reported_at = NOW(),
           status           = CASE
             WHEN youtube_video_unavailable.report_count + 1 >= $4
             THEN 'unavailable'
             ELSE youtube_video_unavailable.status
           END,
           -- Keep the first reporter's id + code for diagnostics.
           sample_user_id    = COALESCE(youtube_video_unavailable.sample_user_id, $2),
           sample_error_code = COALESCE(youtube_video_unavailable.sample_error_code, $3)
     RETURNING status, report_count`, [videoId, reporterUserId, errorCode, _YT_UNAVAILABLE_THRESHOLD]);
    return r.rows[0] ?? { status: "unknown", report_count: 0 };
}
// Set of videoIds whose status is 'unavailable' (above threshold).
// Used by the renderer to filter out broken videos from album popups
// so users see them as "missing" and can submit replacements.
export async function getUnavailableYoutubeVideoIds() {
    const out = new Set();
    try {
        const r = await getPool().query(`SELECT video_id FROM youtube_video_unavailable WHERE status = 'unavailable'`);
        for (const row of r.rows)
            out.add(row.video_id);
    }
    catch { /* best-effort */ }
    return out;
}
// Admin: list every flagged + unavailable entry for the admin tab.
// Newest reports first so admin sees recent issues at the top.
export async function listYoutubeVideoUnavailable(limit = 500) {
    try {
        // LEFT JOIN release_cache so the admin panel can show the album
        // title + first artist as the clickable link text — hitting the
        // release/master JSON blob for a single field per row is fine at
        // the 500-row cap; no need to fan out through the split schema.
        const r = await getPool().query(`SELECT u.video_id, u.status, u.report_count,
              u.first_reported_at, u.last_reported_at,
              u.sample_user_id, u.sample_error_code,
              ov.release_type, ov.release_id,
              ov.track_title, ov.track_position,
              rc.data->>'title'                                AS album_title,
              COALESCE(rc.data->'artists'->0->>'name', '')     AS album_artist
         FROM youtube_video_unavailable u
         LEFT JOIN LATERAL (
           SELECT release_type, release_id, track_title, track_position
             FROM track_youtube_overrides o
            WHERE o.video_id = u.video_id
            ORDER BY o.submitted_at DESC
            LIMIT 1
         ) ov ON true
         LEFT JOIN release_cache rc
           ON rc.discogs_id = ov.release_id::int
          AND rc.type       = ov.release_type
        ORDER BY u.last_reported_at DESC
        LIMIT $1`, [Math.max(1, Math.min(2000, limit))]);
        return r.rows;
    }
    catch {
        return [];
    }
}
// Admin: clear a single videoId from the unavailable list (e.g. when
// a video came back online). Removes the row entirely so a future
// report starts fresh from count 1.
export async function clearYoutubeVideoUnavailable(videoId) {
    try {
        const r = await getPool().query(`DELETE FROM youtube_video_unavailable WHERE video_id = $1`, [videoId]);
        return (r.rowCount ?? 0) > 0;
    }
    catch {
        return false;
    }
}
// ── YouTube search cache (DB-backed) ────────────────────────────────
// In-memory cache in search-api.ts evaporates on every Railway
// restart. Mirror the same query results to a durable row so we
// don't rebuild the cache from quota-paid hits each deploy.
export async function getYoutubeSearchCache(cacheKey, maxAgeSeconds) {
    try {
        const r = await getPool().query(`SELECT body, cached_at FROM youtube_search_cache WHERE cache_key = $1 LIMIT 1`, [cacheKey]);
        const row = r.rows[0];
        if (!row)
            return null;
        const ageMs = Date.now() - new Date(row.cached_at).getTime();
        if (ageMs > maxAgeSeconds * 1000)
            return null;
        return row.body ?? null;
    }
    catch {
        return null;
    }
}
// Last time we cached a result for this query, regardless of TTL.
// Used to surface a "last searched <time> ago" hover hint on the
// external-YouTube fallback link when the popup couldn't fetch
// fresh results (quota error, etc.).
export async function getYoutubeSearchCacheTimestamp(cacheKey) {
    try {
        const r = await getPool().query(`SELECT cached_at FROM youtube_search_cache WHERE cache_key = $1 LIMIT 1`, [cacheKey]);
        const ca = r.rows[0]?.cached_at;
        return ca ? new Date(ca) : null;
    }
    catch {
        return null;
    }
}
export async function setYoutubeSearchCache(cacheKey, body) {
    try {
        await getPool().query(`INSERT INTO youtube_search_cache (cache_key, body, cached_at)
       VALUES ($1, $2::jsonb, NOW())
       ON CONFLICT (cache_key) DO UPDATE SET body = EXCLUDED.body, cached_at = NOW()`, [cacheKey, JSON.stringify(body)]);
    }
    catch { /* cache is best-effort */ }
}
// Periodic prune of stale rows so the table stays bounded. Anything
// older than 7 days is dropped — well past the 24h read TTL.
export async function pruneYoutubeSearchCache() {
    try {
        const r = await getPool().query(`DELETE FROM youtube_search_cache WHERE cached_at < NOW() - INTERVAL '7 days'`);
        return r.rowCount ?? 0;
    }
    catch {
        return 0;
    }
}
// ── archive.org search cache helpers ───────────────────────────────
// Mirrors the youtube_search_cache helpers above. Long-lived 90-day
// TTL — archive.org's catalog is extremely stable, and we want to
// avoid burning any goodwill with their public search API. Cache key
// is a normalized "q|page|rows" string built by the route handler.
export async function getArchiveSearchCache(cacheKey, maxAgeSeconds) {
    try {
        const r = await getPool().query(`SELECT body, cached_at FROM archive_search_cache WHERE cache_key = $1 LIMIT 1`, [cacheKey]);
        const row = r.rows[0];
        if (!row)
            return null;
        const ageMs = Date.now() - new Date(row.cached_at).getTime();
        if (ageMs > maxAgeSeconds * 1000)
            return null;
        return row.body ?? null;
    }
    catch {
        return null;
    }
}
export async function setArchiveSearchCache(cacheKey, body) {
    try {
        await getPool().query(`INSERT INTO archive_search_cache (cache_key, body, cached_at)
       VALUES ($1, $2::jsonb, NOW())
       ON CONFLICT (cache_key) DO UPDATE SET body = EXCLUDED.body, cached_at = NOW()`, [cacheKey, JSON.stringify(body)]);
    }
    catch { /* cache is best-effort */ }
}
// Drop rows past the 90-day TTL. Wired to the existing prune
// scheduler (or run manually). Returns # of rows deleted.
export async function pruneArchiveSearchCache() {
    try {
        const r = await getPool().query(`DELETE FROM archive_search_cache WHERE cached_at < NOW() - INTERVAL '90 days'`);
        return r.rowCount ?? 0;
    }
    catch {
        return 0;
    }
}
// Admin DB tab: per-table summary popup data. Pure introspection
// against information_schema + pg_indexes + pg_total_relation_size —
// no application data leaves this function. Caller is responsible for
// whitelisting the table name (it's interpolated for pg_total_relation_size
// since that's a function call, not a query target — but is also
// passed as a parameter to information_schema queries so even an
// injection slip would only affect the size query).
export async function getDbAdminTableSummary(tableName) {
    // Defensive: refuse any name that's not a-z/0-9/underscore even
    // though callers should already whitelist via getTableRowCounts.
    if (!/^[a-z_][a-z0-9_]*$/i.test(tableName)) {
        throw new Error("Invalid table name");
    }
    const [colsR, idxR, cntR, sizeR] = await Promise.all([
        getPool().query(`SELECT column_name AS name,
              data_type   AS type,
              is_nullable = 'YES' AS nullable,
              column_default AS "default"
         FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = $1
        ORDER BY ordinal_position`, [tableName]),
        getPool().query(`SELECT indexname AS name, indexdef AS definition
         FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = $1
        ORDER BY indexname`, [tableName]),
        getPool().query(`SELECT COUNT(*)::int AS n FROM "${tableName}"`),
        getPool().query(`SELECT pg_total_relation_size('public."${tableName}"') AS bytes`),
    ]);
    return {
        table: tableName,
        rowCount: cntR.rows[0]?.n ?? 0,
        totalSizeBytes: Number(sizeR.rows[0]?.bytes) || 0,
        columns: colsR.rows,
        indexes: idxR.rows,
    };
}
// Admin Suggestions tab: per-user counts + last-generated timestamp
// for the background personal-suggestions job. Used to verify the
// hourly run is healthy without dumping every row.
export async function getPersonalSuggestionsStats() {
    try {
        const r = await getPool().query(`
      SELECT clerk_user_id AS "clerkUserId",
             COUNT(*)::int AS count,
             MAX(generated_at) AS "lastGeneratedAt"
        FROM user_personal_suggestions
       GROUP BY clerk_user_id
       ORDER BY MAX(generated_at) DESC NULLS LAST
    `);
        return r.rows;
    }
    catch {
        return [];
    }
}
// Admin: list every override for the audit/admin tab. Newest first.
export async function listAllTrackYtOverrides(limit = 500) {
    try {
        const r = await getPool().query(`SELECT release_id, release_type, track_position, track_title,
              video_id, video_title, submitted_by, submitted_at
         FROM track_youtube_overrides
        ORDER BY submitted_at DESC
        LIMIT $1`, [limit]);
        return r.rows;
    }
    catch {
        return [];
    }
}
// ── Lyrics helpers (scraped from weeniecampbell.com) ─────────────────
// upsertLyric is keyed on (source_host, page_title) so a re-scrape
// updates existing rows instead of duplicating.
export async function upsertLyric(record) {
    const host = record.sourceHost || "weeniecampbell.com";
    // ON CONFLICT target matches the partial unique index defined in
    // initDb (source_host, page_title, COALESCE(LOWER(TRIM(artist)), '')).
    // Re-scraping the same page still upserts (artist matches itself),
    // but a manual add with a different artist on the same title goes
    // through as a fresh INSERT.
    await getPool().query(`INSERT INTO blues_lyrics (source_host, page_title, page_url, artist, tuning, wikitext, plaintext, scraped_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
     ON CONFLICT (source_host, page_title, (COALESCE(LOWER(TRIM(artist)), '')))
     DO UPDATE SET page_url = EXCLUDED.page_url,
                   tuning = EXCLUDED.tuning,
                   wikitext = EXCLUDED.wikitext,
                   plaintext = EXCLUDED.plaintext,
                   scraped_at = NOW()`, [host, record.pageTitle, record.pageUrl, record.artist ?? null, record.tuning ?? null, record.wikitext ?? null, record.plaintext ?? null]);
}
// Manual lyric insert — used by the admin "+ Add lyric" affordance.
// Returns the new row. page_title + artist together are unique per
// source_host, so adding a duplicate (same title + same artist) on
// the same source throws unique_violation (23505) which the caller
// surfaces as 409.
export async function createLyric(record) {
    const host = record.sourceHost || "manual";
    const title = String(record.pageTitle || "").trim();
    if (!title)
        throw new Error("page_title required");
    // Auto-resolve artist_id from name when not explicitly provided.
    let artistId = record.artistId ?? null;
    if (artistId == null && record.artist) {
        const r = await getPool().query(`SELECT id FROM blues_artists WHERE LOWER(name) = LOWER(TRIM($1)) LIMIT 1`, [record.artist]);
        if (r.rows.length)
            artistId = r.rows[0].id;
    }
    const fy = record.firstReleaseYear != null && Number.isFinite(Number(record.firstReleaseYear))
        ? Number(record.firstReleaseYear) : null;
    const ins = await getPool().query(`INSERT INTO blues_lyrics
       (source_host, page_title, page_url, artist, artist_id, tuning,
        wikitext, plaintext, discogs_release_id, discogs_master_id,
        first_release_year, first_release_source, first_release_checked_at,
        scraped_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
             $11, CASE WHEN $11::int IS NULL THEN NULL ELSE 'manual' END,
             CASE WHEN $11::int IS NULL THEN NULL ELSE NOW() END,
             NOW())
     RETURNING *`, [host, title, record.pageUrl ?? null, record.artist ?? null, artistId,
        record.tuning ?? null, record.wikitext ?? null, record.plaintext ?? null,
        record.discogsReleaseId ?? null, record.discogsMasterId ?? null,
        fy]);
    return ins.rows[0];
}
// Set of (source_host, page_title) keys we've already scraped — so a
// resumed scrape skips them. Returns lowercased titles in a Set for
// fast probing.
export async function getLyricTitlesAlreadyScraped(sourceHost = "weeniecampbell.com") {
    try {
        const r = await getPool().query(`SELECT page_title FROM blues_lyrics WHERE source_host = $1`, [sourceHost]);
        return new Set(r.rows.map((row) => String(row.page_title)));
    }
    catch {
        return new Set();
    }
}
export async function getLyricById(id) {
    const r = await getPool().query(`SELECT * FROM blues_lyrics WHERE id = $1`, [id]);
    return r.rows[0] || null;
}
// Whitelist of columns allowed for the sort= URL param. Anything else
// (or unset) falls back to page_title. Keeps the SQL injection-safe
// while letting the client header click drive ORDER BY.
const _LYRICS_SORT_COLS = {
    page_title: "page_title",
    artist: "artist",
    tuning: "tuning",
    scraped_at: "scraped_at",
    updated_at: "updated_at",
    first_release_year: "first_release_year",
};
// Expand a lyric search query into its phonetic / colloquial
// equivalents so a single typed phrase catches all the ways blues
// lyrics actually render the same thought. Bidirectional: typing
// "want to" also matches "wanna" and vice versa. Capped at 32
// variants so a pathological query can't blow up the OR list.
//
//   "want to" → ["want to", "wanna"]
//   "wanna"   → ["wanna", "want to"]
//   "one and one" → ["one and one", "1 and one", "one & one",
//                     "one and 1", "1 & 1", "1 and 1", …]
//   "morning" → ["morning", "mornin'"]
//   "mornin'" → ["mornin'", "morning"]
function _escRe(s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
function expandLyricSearchVariants(q) {
    if (!q)
        return [q];
    const out = new Set([q]);
    // Bidirectional substring rewrites. Each pair is applied both ways
    // against every variant in the BFS queue until we hit the cap.
    const pairs = [
        // Reduced auxiliaries / common slurs
        ["want to", "wanna"],
        ["going to", "gonna"],
        ["got to", "gotta"],
        ["have to", "hafta"],
        ["has to", "hasta"],
        ["had to", "hadda"],
        ["ought to", "oughta"],
        ["used to", "useta"],
        ["kind of", "kinda"],
        ["sort of", "sorta"],
        ["lot of", "lotta"],
        ["out of", "outta"],
        ["because", "'cause"],
        ["because", "cause"],
        ["them", "'em"],
        ["and", "&"],
        // Number words ↔ digits
        ["zero", "0"], ["one", "1"], ["two", "2"], ["three", "3"],
        ["four", "4"], ["five", "5"], ["six", "6"], ["seven", "7"],
        ["eight", "8"], ["nine", "9"], ["ten", "10"],
        // Common contractions seen in transcribed blues lyrics
        ["i am", "i'm"],
        ["you are", "you're"],
        ["he is", "he's"],
        ["she is", "she's"],
        ["it is", "it's"],
        ["we are", "we're"],
        ["they are", "they're"],
        ["do not", "don't"],
        ["does not", "doesn't"],
        ["did not", "didn't"],
        ["will not", "won't"],
        ["can not", "can't"],
        ["cannot", "can't"],
        ["should not", "shouldn't"],
        ["would not", "wouldn't"],
        ["could not", "couldn't"],
        ["is not", "ain't"],
        ["are not", "ain't"],
        ["am not", "ain't"],
    ];
    const queue = [q];
    const CAP = 32;
    while (queue.length && out.size < CAP) {
        const s = queue.shift();
        const lower = s.toLowerCase();
        for (const [a, b] of pairs) {
            if (out.size >= CAP)
                break;
            if (lower.includes(a)) {
                const v = s.replace(new RegExp(_escRe(a), "gi"), b);
                if (!out.has(v)) {
                    out.add(v);
                    queue.push(v);
                }
            }
            if (lower.includes(b)) {
                const v = s.replace(new RegExp(_escRe(b), "gi"), a);
                if (!out.has(v)) {
                    out.add(v);
                    queue.push(v);
                }
            }
        }
    }
    // Dropped-g ending: morning ↔ mornin'. Applied after the pair pass
    // so contractions don't accidentally collide. Word-boundary only on
    // the right so we match the suffix at end-of-word, not anywhere.
    for (const s of [...out]) {
        if (out.size >= CAP * 2)
            break;
        if (/ing\b/i.test(s)) {
            out.add(s.replace(/ing\b/gi, "in'"));
            out.add(s.replace(/ing\b/gi, "in")); // sometimes apostrophe is dropped too
        }
        if (/in'\b/i.test(s)) {
            out.add(s.replace(/in'\b/gi, "ing"));
        }
    }
    return [...out];
}
export async function listLyrics(opts) {
    const where = [];
    const params = [];
    if (opts.search) {
        // Parse the query for && (AND) and || (OR) operators. Standard
        // precedence: || binds looser than &&, so "a && b || c" means
        // "(a AND b) OR c". Each leaf term is then expanded to colloquial
        // variants (want to ↔ wanna, one ↔ 1, mornin' ↔ morning, etc.)
        // and matched ILIKE against page_title / artist / plaintext.
        //   "wanna"            → single-term: variant expansion only
        //   "love && money"    → must match both terms
        //   "love || hate"     → match either term
        //   "love && me || you"→ (love AND me) OR (you)
        const orGroups = String(opts.search)
            .split(/\s*\|\|\s*/)
            .map(s => s.trim())
            .filter(Boolean)
            .map(part => part
            .split(/\s*&&\s*/)
            .map(s => s.trim())
            .filter(Boolean));
        const orClauses = [];
        for (const group of orGroups) {
            if (!group.length)
                continue;
            const andClauses = [];
            for (const term of group) {
                const variants = expandLyricSearchVariants(term);
                const subClauses = [];
                for (const v of variants) {
                    params.push(`%${v}%`);
                    const p = `$${params.length}`;
                    subClauses.push(`(page_title ILIKE ${p} OR artist ILIKE ${p} OR plaintext ILIKE ${p})`);
                }
                andClauses.push(`(${subClauses.join(" OR ")})`);
            }
            orClauses.push(`(${andClauses.join(" AND ")})`);
        }
        if (orClauses.length)
            where.push(`(${orClauses.join(" OR ")})`);
    }
    if (opts.tuning) {
        // Special sentinel: "(unspecified)" filters rows where no tuning
        // was extracted from the page body. On a blues-lyrics wiki, an
        // unmentioned tuning effectively means standard, so this is the
        // catch-all for "standard tuning" pages that don't say so
        // explicitly.
        if (opts.tuning === "(unspecified)") {
            where.push(`tuning IS NULL`);
        }
        else {
            params.push(opts.tuning);
            where.push(`tuning = $${params.length}`);
        }
    }
    if (opts.tuningLike) {
        // Case-insensitive substring filter on tuning. Used by the bulk
        // editor so the curator can corral inconsistent variants ("open
        // d", "Open D", "OPEN D") under one selection before mass-editing.
        params.push(`%${opts.tuningLike}%`);
        where.push(`tuning ILIKE $${params.length}`);
    }
    if (opts.artist) {
        params.push(opts.artist);
        where.push(`artist = $${params.length}`);
    }
    if (opts.unmatchedOnly) {
        where.push(`artist_id IS NULL`);
    }
    if (opts.unpinnedOnly) {
        where.push(`discogs_release_id IS NULL AND discogs_master_id IS NULL`);
    }
    if (opts.emptyOnly) {
        where.push(`(plaintext IS NULL OR LENGTH(TRIM(plaintext)) = 0)`);
    }
    if (opts.noArtistOnly) {
        where.push(`(artist IS NULL OR LENGTH(TRIM(artist)) = 0)`);
    }
    if (opts.pinnedOnly) {
        where.push(`(discogs_release_id IS NOT NULL OR discogs_master_id IS NOT NULL)`);
    }
    if (opts.titleHasPunct) {
        where.push(`(page_title LIKE '%-%' OR page_title LIKE '%(%')`);
    }
    if (opts.noYearOnly) {
        where.push(`first_release_year IS NULL`);
    }
    if (opts.favoritesOnly && opts.favoriteUserId) {
        params.push(opts.favoriteUserId);
        where.push(`id IN (SELECT lyric_id FROM blues_lyric_favorites WHERE clerk_user_id = $${params.length})`);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const limit = Math.max(1, Math.min(500, opts.limit ?? 100));
    const offset = Math.max(0, opts.offset ?? 0);
    // Sort column: whitelist-mapped to defeat SQL injection. NULLs LAST
    // so empty-tuning rows don't dominate a "Tuning ASC" sort — the
    // most-common-by-far value otherwise crowds the visible page out.
    const sortCol = _LYRICS_SORT_COLS[String(opts.sort ?? "")] || "page_title";
    const order = opts.order === "desc" ? "DESC" : "ASC";
    // Secondary sort by page_title for stable ordering within ties.
    const orderSql = sortCol === "page_title"
        ? `ORDER BY page_title ${order}`
        : `ORDER BY ${sortCol} ${order} NULLS LAST, page_title ASC`;
    const totalRow = await getPool().query(`SELECT COUNT(*)::int AS n FROM blues_lyrics ${whereSql}`, params);
    const total = totalRow.rows[0]?.n ?? 0;
    params.push(limit, offset);
    const rowsRes = await getPool().query(`SELECT id, page_title, page_url, artist, artist_id, tuning,
            discogs_release_id, discogs_master_id,
            first_release_year, first_release_source,
            scraped_at, updated_at,
            substring(plaintext, 1, 240) AS snippet
       FROM blues_lyrics ${whereSql}
       ${orderSql}
       LIMIT $${params.length - 1} OFFSET $${params.length}`, params);
    return { rows: rowsRes.rows, total };
}
export async function getLyricTunings() {
    const r = await getPool().query(`SELECT tuning, COUNT(*)::int AS n
       FROM blues_lyrics
      WHERE tuning IS NOT NULL AND tuning <> ''
      GROUP BY tuning
      ORDER BY n DESC, tuning ASC`);
    // Append a virtual (unspecified) entry counting rows where no
    // tuning was extracted — almost all of these are standard tuning
    // on a blues-lyrics wiki, so the dropdown surfaces it as a usable
    // filter. Pinned to the bottom of the list regardless of count.
    const nullR = await getPool().query(`SELECT COUNT(*)::int AS n FROM blues_lyrics WHERE tuning IS NULL OR tuning = ''`);
    const nullCount = nullR.rows[0]?.n ?? 0;
    const rows = r.rows;
    if (nullCount > 0)
        rows.push({ tuning: "(unspecified)", n: nullCount });
    return rows;
}
export async function getLyricCount() {
    const r = await getPool().query(`SELECT COUNT(*)::int AS n FROM blues_lyrics`);
    return r.rows[0]?.n ?? 0;
}
// ── Blues archive (unified artist + lyrics + releases view) ──────────
// Powers the admin Discovery sub-view that combines blues_artists,
// blues_lyrics, and the JSONB releases array into a single per-artist
// page. Matching of lyrics → artist is case-insensitive on name.
// Delete blues_artists rows added in the last 24 hours — cleanup for
// the (now-removed) strict pad button which had no year filter and
// pulled in modern artists. User confirmed they haven't manually
// added an artist in over a week, so date_added alone is a precise
// signal for "pad-inserted" rows. Returns { removed, names } for UI.
export async function pruneBluesArtistsRecent24h() {
    const r = await getPool().query(`
    DELETE FROM blues_artists
     WHERE date_added >= NOW() - INTERVAL '24 hours'
    RETURNING name
  `);
    return {
        removed: r.rowCount ?? 0,
        names: (r.rows ?? []).slice(0, 50).map((row) => row.name),
    };
}
// Bulk-insert blues_artists for every artist who appears as a primary
// credit on at least one strictly-Blues master in release_cache whose
// EARLIEST such master is in or before 1950. Tags inserts with
// enrichment_status.source = "strict_pad_pre1950" so future cleanup
// can target them precisely. Existing rows have their seed_strict_count
// refreshed but their other fields are untouched.
export async function padBluesArtistsStrictPre1950() {
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        await client.query(`
      CREATE TEMP TABLE _pad_pre1950 ON COMMIT DROP AS
      SELECT (a->>'id')::int AS aid,
             MIN(a->>'name') AS name,
             COUNT(*)::int   AS strict_count,
             MIN((rc.data->>'year')::int) AS first_year
        FROM release_cache rc
        CROSS JOIN LATERAL jsonb_array_elements(
          COALESCE(rc.data->'artists', '[]'::jsonb)
        ) AS a
       WHERE rc.type = 'master'
         AND jsonb_typeof(rc.data->'genres') = 'array'
         AND jsonb_array_length(rc.data->'genres') = 1
         AND rc.data->'genres' ? 'Blues'
         AND (a->>'id') ~ '^[0-9]+$'
         AND (a->>'id')::int > 0
         AND (a->>'id')::int NOT IN (194, 355)
         AND rc.data->>'year' ~ '^[0-9]+$'
         AND (rc.data->>'year')::int > 0
       GROUP BY (a->>'id')::int
      HAVING MIN((rc.data->>'year')::int) <= 1950
    `);
        const scannedR = await client.query(`SELECT COUNT(*)::int AS n FROM _pad_pre1950`);
        const scanned = scannedR.rows[0]?.n ?? 0;
        if (!scanned) {
            await client.query("COMMIT");
            return { scanned: 0, inserted: 0, refreshed: 0 };
        }
        const refreshedR = await client.query(`
      UPDATE blues_artists ba
         SET seed_strict_count = c.strict_count,
             updated_at        = NOW()
        FROM _pad_pre1950 c
       WHERE ba.discogs_id = c.aid
    `);
        const refreshed = refreshedR.rowCount ?? 0;
        const insertedR = await client.query(`
      INSERT INTO blues_artists (discogs_id, name, seed_strict_count, enrichment_status)
      SELECT c.aid,
             LEFT(COALESCE(NULLIF(TRIM(c.name), ''), 'Artist ' || c.aid), 200),
             c.strict_count,
             '{"source":"strict_pad_pre1950"}'::jsonb
        FROM _pad_pre1950 c
       WHERE NOT EXISTS (SELECT 1 FROM blues_artists ba WHERE ba.discogs_id = c.aid)
      ON CONFLICT (discogs_id) DO UPDATE
        SET seed_strict_count = EXCLUDED.seed_strict_count,
            updated_at        = NOW()
    `);
        const inserted = insertedR.rowCount ?? 0;
        await client.query("COMMIT");
        return { scanned, inserted, refreshed };
    }
    catch (err) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        throw err;
    }
    finally {
        client.release();
    }
}
// Walks distinct lyrics.artist values, finds those that don't already
// exist as a blues_artists.name (case-insensitive), and inserts each
// as a minimal new row (name only). Returns { added, total, existing }
// so the UI can report what happened.
export async function importLyricsArtistsToBluesDb(validate) {
    // Distinct non-null lyric-artist names. Trim + dedupe in JS so we
    // can match against the (lowercased) existing set in one pass.
    const rawNamesR = await getPool().query(`SELECT DISTINCT TRIM(artist) AS artist
       FROM blues_lyrics
      WHERE artist IS NOT NULL AND TRIM(artist) <> ''`);
    const rawCandidates = rawNamesR.rows
        .map(r => r.artist.replace(/\s+/g, " ").trim())
        .filter(s => s.length >= 2 && s.length <= 200);
    // Validator pass — applied at IMPORT time as well as extraction so
    // stale bad values left in blues_lyrics.artist (from old scrapes
    // before the validator was added) don't leak into blues_artists.
    // Without this, "1934 version" / "alternate take" / catalog
    // numbers etc. would still get imported as artists.
    const candidates = [];
    let rejected = 0;
    for (const c of rawCandidates) {
        if (validate && !validate(c)) {
            rejected++;
            continue;
        }
        candidates.push(c);
    }
    const existingR = await getPool().query(`SELECT LOWER(name) AS lname FROM blues_artists`);
    const existing = new Set(existingR.rows.map(r => r.lname));
    // De-dupe locally + filter against the existing set so the bulk
    // INSERT below only carries names that genuinely need adding.
    const toAdd = [];
    const localSeen = new Set();
    let already = 0;
    for (const name of candidates) {
        const key = name.toLowerCase();
        if (existing.has(key)) {
            already++;
            continue;
        }
        if (localSeen.has(key))
            continue;
        localSeen.add(key);
        toAdd.push(name);
    }
    // Bulk INSERT via unnest — one round-trip regardless of size.
    // Previous version did one INSERT per name which 60s-timed-out
    // once the candidate set got into the low thousands.
    let added = 0;
    if (toAdd.length) {
        const r = await getPool().query(`INSERT INTO blues_artists (name, enrichment_status)
       SELECT n, '{"source":"lyrics_import"}'::jsonb FROM unnest($1::text[]) AS n
       RETURNING id`, [toAdd]);
        added = r.rowCount ?? 0;
    }
    // Wire up the FK on any orphan lyrics whose artist string now
    // matches a freshly-imported (or previously-existing) artist row.
    // This is what makes the import the "merge step" — without it, the
    // new artist rows would exist but their lyrics would still be
    // joined only by string match.
    await getPool().query(`
    UPDATE blues_lyrics l
       SET artist_id = a.id
      FROM blues_artists a
     WHERE l.artist_id IS NULL
       AND l.artist IS NOT NULL
       AND LOWER(TRIM(l.artist)) = LOWER(a.name)
  `);
    return { added, total: rawCandidates.length, existing: already, rejected };
}
// Paginated artist list with aggregated counts. Joins lyrics on
// case-insensitive name match (no alias support yet — keeps the SQL
// fast; we can add aliases JSONB matching later). Releases come from
// the existing discogs_releases JSONB column.
// Whitelist of sort columns. Maps the URL-safe key to a SQL fragment
// (some are computed expressions, not bare column refs).
const _BLUES_ARCHIVE_SORT_COLS = {
    name: "a.name",
    discogs_id: "a.discogs_id",
    releases_count: "COALESCE(jsonb_array_length(a.discogs_releases), 0)",
    lyrics_count: `(SELECT COUNT(*)::int FROM blues_lyrics l
                     WHERE l.artist_id = a.id
                        OR (l.artist_id IS NULL
                            AND LOWER(TRIM(l.artist)) = LOWER(a.name)))`,
    // Year of first release — LEAST(MB first_recording_year,
    // MIN(discogs_releases[*].year)). _EARLIEST_REL_SQL references
    // unqualified columns, so we substitute the a. alias inline.
    first_release_year: _EARLIEST_REL_SQL
        .replace(/\bfirst_recording_year\b/g, "a.first_recording_year")
        .replace(/\bdiscogs_releases\b/g, "a.discogs_releases"),
    // Has-photo: 1 when photo_url is set + non-empty, 0 otherwise.
    // Sorting DESC puts rows WITH a photo first (find-by-eye),
    // ASC puts MISSING rows first (curation queue).
    // Count of cached masters in release_cache where genres=['Blues']
    // exactly AND this artist is a primary credit. Populated by the
    // pre-1950 strict pad button; DESC surfaces artists with the most
    // strictly-Blues releases first.
    strict_count: "a.seed_strict_count",
    has_photo: "CASE WHEN COALESCE(a.photo_url, '') <> '' THEN 1 ELSE 0 END",
    // Has-wiki: same idea for wikipedia_suffix. ASC surfaces rows still
    // missing a Wikipedia link so the curator can knock them out.
    has_wiki: "CASE WHEN COALESCE(a.wikipedia_suffix, '') <> '' THEN 1 ELSE 0 END",
};
export async function listBluesArchive(opts = {}) {
    const params = [];
    const where = [];
    if (opts.search) {
        // Pure-digit input matches Discogs ID exactly. Free-text matches
        // BOTH the name (fast index on lower(name)) and the notes column
        // (linear scan, but ~1k row table — fine). A 4-digit input also
        // ILIKE's name + notes so a numeric fragment like "1923" finds
        // "...died 1923..." in a bio.
        const raw = opts.search.trim();
        params.push(`%${raw}%`);
        const likePh = `$${params.length}`;
        const nameOrNotes = `(a.name ILIKE ${likePh} OR a.notes ILIKE ${likePh})`;
        if (/^\d+$/.test(raw)) {
            params.push(parseInt(raw, 10));
            where.push(`(${nameOrNotes} OR a.discogs_id = $${params.length})`);
        }
        else {
            where.push(nameOrNotes);
        }
    }
    // Category filter — translates to a HAS_LYRICS / HAS_RELEASES
    // combo. Built as a correlated EXISTS so the index on
    // blues_lyrics.artist_id stays useful; the lyrics-by-name fallback
    // covers legacy rows whose artist_id hasn't been backfilled yet.
    if (opts.category) {
        const HAS_LYRICS = `EXISTS (
      SELECT 1 FROM blues_lyrics l
       WHERE l.artist_id = a.id
          OR (l.artist_id IS NULL AND LOWER(TRIM(l.artist)) = LOWER(a.name))
    )`;
        const HAS_RELEASES = `COALESCE(jsonb_array_length(a.discogs_releases), 0) > 0`;
        if (opts.category === "with_both")
            where.push(`${HAS_LYRICS} AND ${HAS_RELEASES}`);
        else if (opts.category === "with_lyrics_only")
            where.push(`${HAS_LYRICS} AND NOT ${HAS_RELEASES}`);
        else if (opts.category === "with_releases_only")
            where.push(`NOT ${HAS_LYRICS} AND ${HAS_RELEASES}`);
        else if (opts.category === "empty")
            where.push(`NOT ${HAS_LYRICS} AND NOT ${HAS_RELEASES}`);
    }
    if (opts.noWiki) {
        where.push(`(a.wikipedia_suffix IS NULL OR a.wikipedia_suffix = '')`);
    }
    if (opts.noDiscogsId) {
        where.push(`a.discogs_id IS NULL`);
    }
    if (opts.hasStrict) {
        where.push(`a.seed_strict_count > 0`);
    }
    if (opts.noStrict) {
        where.push(`COALESCE(a.seed_strict_count, 0) = 0`);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const limit = Math.max(1, Math.min(500, opts.limit ?? 100));
    const offset = Math.max(0, opts.offset ?? 0);
    // Whitelist sort column + direction. Secondary sort by name keeps
    // ordering stable within ties (e.g. two artists with same lyrics
    // count). NULLS LAST on numeric/id columns so empty discogs_id
    // rows fall to the bottom instead of crowding the top.
    const sortCol = _BLUES_ARCHIVE_SORT_COLS[String(opts.sort ?? "")] || "a.name";
    const order = opts.order === "desc" ? "DESC" : "ASC";
    const orderSql = sortCol === "a.name"
        ? `ORDER BY a.name ${order}`
        : `ORDER BY ${sortCol} ${order} NULLS LAST, a.name ASC`;
    const totalR = await getPool().query(`SELECT COUNT(*)::int AS n FROM blues_artists a ${whereSql}`, params);
    const total = totalR.rows[0]?.n ?? 0;
    params.push(limit, offset);
    const r = await getPool().query(`SELECT a.id,
            a.name,
            a.birth_date,
            a.death_date,
            a.photo_url,
            a.discogs_id,
            a.wikipedia_suffix,
            COALESCE(a.seed_strict_count, 0) AS seed_strict_count,
            COALESCE(jsonb_array_length(a.discogs_releases), 0) AS releases_count,
            -- lyrics_count: prefer FK count (canonical) but fall back
            -- to the legacy name-match count for any unmigrated rows
            -- so the number doesn't read 0 before backfill completes.
            (SELECT COUNT(*)::int FROM blues_lyrics l
              WHERE l.artist_id = a.id
                 OR (l.artist_id IS NULL
                     AND LOWER(TRIM(l.artist)) = LOWER(a.name))) AS lyrics_count,
            -- first_release_year: smaller of MB first_recording_year
            -- and MIN(discogs_releases[*].year). Drives the new "Year"
            -- column + its sort key.
            ${_BLUES_ARCHIVE_SORT_COLS.first_release_year} AS first_release_year
       FROM blues_artists a
       ${whereSql}
       ${orderSql}
       LIMIT $${params.length - 1} OFFSET $${params.length}`, params);
    return { rows: r.rows, total };
}
// ── Blues Archive: flat releases list ─────────────────────────────────
// Unnests every blues_artists.discogs_releases JSONB array into a flat
// per-release row joined back to the source artist. Powers the Releases
// sub-tab on the Discovery Blues Archive view so a curator can browse,
// filter, and sort the catalog without bouncing through artist popups.
const _BLUES_REL_SORT_COLS = {
    // Display title comes from the JSONB blob.
    title: "lower(coalesce(rel->>'title',''))",
    // Cast year text to int so 1928 < 1932 sorts numerically.
    year: "NULLIF(rel->>'year','')::int",
    artist: "lower(a.name)",
    type: "lower(coalesce(rel->>'type',''))",
    role: "lower(coalesce(rel->>'role',''))",
};
export async function listBluesArchiveReleases(opts = {}) {
    const params = [];
    const where = [];
    if (opts.search?.trim()) {
        const q = opts.search.trim();
        params.push(`%${q}%`);
        // Match title, artist name — and if the query is a bare 4-digit
        // year, also match release year. Keeps the existing dedicated
        // Year input useful (exact int) while letting the curator type
        // '1928' into the main search and find what they expect.
        if (/^\d{4}$/.test(q)) {
            params.push(parseInt(q, 10));
            where.push(`(rel->>'title' ILIKE $${params.length - 1} OR a.name ILIKE $${params.length - 1} OR NULLIF(rel->>'year','')::int = $${params.length})`);
        }
        else {
            where.push(`(rel->>'title' ILIKE $${params.length} OR a.name ILIKE $${params.length})`);
        }
    }
    if (opts.artist?.trim()) {
        params.push(`%${opts.artist.trim()}%`);
        where.push(`a.name ILIKE $${params.length}`);
    }
    if (opts.year != null && Number.isFinite(opts.year)) {
        params.push(opts.year);
        where.push(`NULLIF(rel->>'year','')::int = $${params.length}`);
    }
    if (opts.type?.trim()) {
        params.push(opts.type.trim().toLowerCase());
        where.push(`LOWER(COALESCE(rel->>'type','')) = $${params.length}`);
    }
    if (opts.role?.trim()) {
        params.push(opts.role.trim().toLowerCase());
        where.push(`LOWER(COALESCE(rel->>'role','')) = $${params.length}`);
    }
    // Drop rows whose JSONB entry doesn't have a usable id — they can't
    // be linked out to discogs.com so they're effectively noise.
    where.push(`(rel ? 'id') AND (rel->>'id') ~ '^[0-9]+$'`);
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const limit = Math.max(1, Math.min(500, opts.limit ?? 100));
    const offset = Math.max(0, opts.offset ?? 0);
    const sortCol = _BLUES_REL_SORT_COLS[String(opts.sort ?? "")] || _BLUES_REL_SORT_COLS.year;
    const order = opts.order === "asc" ? "ASC" : "DESC";
    // Tie-break by artist then title so identical sort keys stay stable.
    const orderSql = `ORDER BY ${sortCol} ${order} NULLS LAST, lower(a.name) ASC, lower(coalesce(rel->>'title','')) ASC`;
    // Count query stays narrow (no release_cache join needed). The
    // data query LEFT JOINs release_cache so each row can carry a
    // small cover thumb when one is cached; rows without a cached
    // release just come back with cover_thumb=null and the UI shows
    // a placeholder.
    const fromCountSql = `FROM blues_artists a, jsonb_array_elements(coalesce(a.discogs_releases, '[]'::jsonb)) AS rel`;
    const fromDataSql = `
    FROM blues_artists a
    CROSS JOIN LATERAL jsonb_array_elements(coalesce(a.discogs_releases, '[]'::jsonb)) AS rel
    LEFT JOIN release_cache rc
      ON rc.discogs_id = NULLIF(rel->>'id','')::int
     AND rc.type       = COALESCE(NULLIF(rel->>'type',''), 'release')
  `;
    const totalR = await getPool().query(`SELECT COUNT(*)::int AS n ${fromCountSql} ${whereSql}`, params);
    const total = totalR.rows[0]?.n ?? 0;
    params.push(limit, offset);
    const r = await getPool().query(`SELECT a.id   AS artist_id,
            a.name AS artist_name,
            a.photo_url,
            a.discogs_id AS artist_discogs_id,
            (rel->>'id')::bigint                    AS release_id,
            COALESCE(rel->>'type', 'release')       AS release_type,
            COALESCE(rel->>'title','')              AS release_title,
            NULLIF(rel->>'year','')::int            AS release_year,
            rel->'label'                            AS release_label,
            rel->>'role'                            AS role,
            COALESCE(
              rc.data->'images'->0->>'thumb',
              rc.data->'images'->0->>'uri150',
              rc.data->'images'->0->>'uri'
            )                                       AS cover_thumb
       ${fromDataSql}
       ${whereSql}
       ${orderSql}
       LIMIT $${params.length - 1} OFFSET $${params.length}`, params);
    return { rows: r.rows, total };
}
// Full artist record + matched lyrics + releases sorted oldest→newest.
// Lyrics match by artist_id (canonical FK) OR — for legacy rows not
// yet backfilled — by case-insensitive name. The name fallback keeps
// the artist popup useful even before the import button has been run.
export async function getBluesArchiveArtist(id) {
    const ar = await getPool().query(`SELECT * FROM blues_artists WHERE id = $1`, [id]);
    if (!ar.rows.length)
        return null;
    const a = ar.rows[0];
    const lr = await getPool().query(`SELECT id, page_title, page_url, tuning, scraped_at, updated_at,
            artist_id, discogs_release_id, discogs_master_id,
            first_release_year, first_release_source,
            substring(plaintext, 1, 240) AS snippet
       FROM blues_lyrics
      WHERE artist_id = $1
         OR (artist_id IS NULL AND LOWER(TRIM(artist)) = LOWER($2))
      ORDER BY page_title ASC`, [id, a.name]);
    // Tuning breakdown — per-artist counts, descending. Powers the
    // little chip strip above the lyrics table on the artist popup.
    const tr = await getPool().query(`SELECT COALESCE(NULLIF(tuning, ''), '(unspecified)') AS tuning,
            COUNT(*)::int AS n
       FROM blues_lyrics
      WHERE artist_id = $1
         OR (artist_id IS NULL AND LOWER(TRIM(artist)) = LOWER($2))
      GROUP BY 1
      ORDER BY n DESC, tuning ASC`, [id, a.name]);
    // Static tunings grid (from src/data/tunings.csv) for this artist.
    // Match by case-insensitive trimmed name. Annotate each CSV row with
    // the matching blues_lyrics row id (if any) so the popup can flag
    // "this CSV title isn't in our lyrics table yet" — useful curation
    // signal: either the lyric wasn't scraped, or it's filed under a
    // slightly different page_title that needs reconciling.
    const gr = await getPool().query(`SELECT g.id, g.title, g.position, g.pitch, g.notes,
            (SELECT l.id FROM blues_lyrics l
              WHERE (l.artist_id = $1
                     OR (l.artist_id IS NULL AND LOWER(TRIM(l.artist)) = LOWER($2)))
                AND LOWER(TRIM(l.page_title)) = LOWER(TRIM(g.title))
              LIMIT 1) AS lyric_id
       FROM blues_tunings_grid g
      WHERE LOWER(TRIM(g.artist)) = LOWER(TRIM($2))
      ORDER BY g.title ASC, g.id ASC`, [id, a.name]);
    // Sort the JSONB releases array oldest→newest (NULL years last).
    const releases = Array.isArray(a.discogs_releases) ? a.discogs_releases.slice() : [];
    releases.sort((x, y) => {
        const xy = Number(x?.year) || 9999;
        const yy = Number(y?.year) || 9999;
        if (xy !== yy)
            return xy - yy;
        return String(x?.title ?? "").localeCompare(String(y?.title ?? ""));
    });
    // Linked artists — symmetric junction. Either lo_id or hi_id can
    // be us; the other side is the linked row. Returns name + photo +
    // discogs_id so the popup can render a clickable chip with art.
    const lk = await getPool().query(`SELECT
        CASE WHEN l.lo_id = $1 THEN l.hi_id ELSE l.lo_id END AS id,
        l.kind,
        b.name,
        b.photo_url,
        b.discogs_id
       FROM blues_artist_links l
       JOIN blues_artists b
         ON b.id = CASE WHEN l.lo_id = $1 THEN l.hi_id ELSE l.lo_id END
      WHERE l.lo_id = $1 OR l.hi_id = $1
      ORDER BY l.kind ASC, lower(b.name) ASC`, [id]);
    return { ...a, lyrics: lr.rows, tunings: tr.rows, gridTunings: gr.rows, releases, links: lk.rows };
}
const _CACHED_BLUES_SORT = {
    year: "bc.release_year",
    title: "lower(bc.release_title)",
    artist: "lower(bc.artist_name)",
    artist_count: "ac.n", // user's "least releases" sort
    cached_at: "bc.cached_at",
};
export async function listCachedBluesReleases(opts = {}) {
    const params = [];
    // The base CTE is shared between count + data. Filters apply to
    // both. Free-text matches title OR artist via ILIKE.
    const where = [];
    if (opts.q?.trim()) {
        params.push(`%${opts.q.trim()}%`);
        where.push(`(bc.release_title ILIKE $${params.length} OR bc.artist_name ILIKE $${params.length})`);
    }
    if (opts.yearFrom != null && Number.isFinite(opts.yearFrom)) {
        params.push(opts.yearFrom);
        where.push(`bc.release_year >= $${params.length}`);
    }
    if (opts.yearTo != null && Number.isFinite(opts.yearTo)) {
        params.push(opts.yearTo);
        where.push(`bc.release_year <= $${params.length}`);
    }
    if (opts.country?.trim()) {
        params.push(`%${opts.country.trim()}%`);
        where.push(`bc.country ILIKE $${params.length}`);
    }
    if (opts.format?.trim()) {
        params.push(`%${opts.format.trim()}%`);
        where.push(`bc.format_name ILIKE $${params.length}`);
    }
    if (opts.label?.trim()) {
        params.push(`%${opts.label.trim()}%`);
        where.push(`bc.label_name ILIKE $${params.length}`);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const random = opts.sort === "random";
    const sortCol = _CACHED_BLUES_SORT[String(opts.sort ?? "")] || _CACHED_BLUES_SORT.cached_at;
    const dir = opts.order === "asc" ? "ASC" : "DESC";
    // For artist_count asc (least-first), nulls are obvious noise —
    // push them last so the curator sees real artists first.
    // bc.discogs_id is the final tiebreaker so LIMIT/OFFSET pagination
    // is stable: without a unique key in the ORDER BY, ties on the
    // primary sort + release_year + title would let PostgreSQL shuffle
    // those rows between page 1 and page 2, causing "Next page doesn't
    // respect my sort" symptoms.
    const orderSql = random
        ? "ORDER BY random()"
        : `ORDER BY ${sortCol} ${dir} NULLS LAST, bc.release_year ASC NULLS LAST, lower(bc.release_title) ASC, bc.discogs_id ASC`;
    const limit = Math.max(1, Math.min(200, opts.limit ?? 60));
    const offset = Math.max(0, opts.offset ?? 0);
    const cteSql = `
    WITH blues_cache AS (
      SELECT
        discogs_id,
        type,
        data,
        cached_at,
        COALESCE(NULLIF(data->'artists'->0->>'name', ''), 'Unknown') AS artist_name,
        data->>'title'                                  AS release_title,
        NULLIF(data->>'year', '')::int                  AS release_year,
        data->>'country'                                AS country,
        COALESCE(
          data->'images'->0->>'thumb',
          data->'images'->0->>'uri150',
          data->'images'->0->>'uri'
        )                                               AS cover_thumb,
        data->'formats'->0->>'name'                     AS format_name,
        data->'labels'->0->>'name'                      AS label_name
      FROM release_cache
      WHERE type = 'release'
        AND data->'genres' ? 'Blues'
    ),
    artist_counts AS (
      SELECT artist_name, COUNT(*)::int AS n
      FROM blues_cache
      GROUP BY artist_name
    )
  `;
    const totalR = await getPool().query(`${cteSql}
     SELECT COUNT(*)::int AS n
       FROM blues_cache bc
       LEFT JOIN artist_counts ac ON ac.artist_name = bc.artist_name
     ${whereSql}`, params);
    const total = totalR.rows[0]?.n ?? 0;
    params.push(limit, offset);
    const r = await getPool().query(`${cteSql}
     SELECT
       bc.discogs_id           AS release_id,
       bc.type                 AS release_type,
       bc.release_title        AS title,
       bc.release_year         AS year,
       bc.artist_name          AS artist,
       bc.country              AS country,
       bc.format_name          AS format,
       bc.label_name           AS label,
       bc.cover_thumb          AS cover_thumb,
       bc.cached_at            AS cached_at,
       ac.n                    AS artist_release_count
     FROM blues_cache bc
     LEFT JOIN artist_counts ac ON ac.artist_name = bc.artist_name
     ${whereSql}
     ${orderSql}
     LIMIT $${params.length - 1} OFFSET $${params.length}`, params);
    return { rows: r.rows, total };
}
// ── blues_artist_links helpers ─────────────────────────────────────
// Symmetric (lo,hi) storage — we normalise the pair before write so
// the PK enforces single-row-per-pair regardless of click direction.
// Find rows worth linking to this artist — used by the editor's
// References panel. Pulls candidates from FIVE sources, all in one
// query so the curator sees one merged list:
//
//   1. aliases     — other row's `aliases` jsonb mentions this name
//   2. collaborators — other row's `collaborators` jsonb mentions this name
//   3. notes       — other row's `notes` text mentions this name
//   4. shared band — both this row AND the other row list the same band
//                    name in their collaborators (a group both played in)
//   5. discogs members — when this row IS a band/group, surface every
//                        Discogs-supplied member (kind='member') that
//                        exists as a blues_artists row by name match
//
// Excludes the target row itself and any rows already linked to it.
// `matched` is a string[] of source labels so the panel can show WHY
// each row surfaced.
export async function findBluesArtistReferences(id) {
    const target = await getPool().query(`SELECT name, collaborators FROM blues_artists WHERE id = $1`, [id]);
    if (!target.rows.length)
        return [];
    const name = (target.rows[0].name || "").trim();
    if (!name)
        return [];
    const n = name.toLowerCase();
    // Derive the two name-set parameters for sources 4 and 5.
    //   sharedBandNames: every band/group name this row recorded a
    //     collaborator entry for. Used to find OTHER rows that share
    //     one of those band names → likely bandmates.
    //   memberNames: every person this row's Discogs record listed as a
    //     member (kind='member'). Used when THIS row is a group: those
    //     are its line-up, surface any that exist as blues_artists rows.
    const collabs = Array.isArray(target.rows[0].collaborators) ? target.rows[0].collaborators : [];
    const sharedBandNames = [];
    const memberNames = [];
    for (const c of collabs) {
        if (typeof c === "string") {
            // Plain-string collaborators are ambiguous (could be a person OR
            // a band). Treat them as potential bands for source 4 — the
            // curator can ignore false matches. They don't drive source 5
            // because there's no member/group distinction.
            sharedBandNames.push(c);
        }
        else if (c && typeof c === "object") {
            const cn = String(c.name || "").trim();
            if (!cn)
                continue;
            if (c.kind === "group")
                sharedBandNames.push(cn);
            else if (c.kind === "member")
                memberNames.push(cn);
            else
                sharedBandNames.push(cn);
        }
    }
    const sharedBandsLc = [...new Set(sharedBandNames.map(s => s.trim().toLowerCase()).filter(Boolean))];
    const membersLc = [...new Set(memberNames.map(s => s.trim().toLowerCase()).filter(Boolean))];
    const r = await getPool().query(`WITH t AS (SELECT $2::text AS n),
          shared_bands AS (SELECT unnest($3::text[]) AS bn),
          members AS (SELECT unnest($4::text[]) AS mn)
     SELECT
       a.id, a.name,
       EXISTS (
         SELECT 1 FROM jsonb_array_elements_text(coalesce(a.aliases, '[]'::jsonb)) v
         WHERE lower(trim(v)) = (SELECT n FROM t)
       ) AS in_aliases,
       EXISTS (
         SELECT 1 FROM jsonb_array_elements(coalesce(a.collaborators, '[]'::jsonb)) v
         WHERE lower(trim(coalesce(v->>'name', v#>>'{}', ''))) = (SELECT n FROM t)
       ) AS in_collabs,
       (a.notes IS NOT NULL AND position(lower($2) in lower(a.notes)) > 0) AS in_notes,
       -- Shared band names this row has in common with the target.
       -- Aggregated into an array so the UI can show "shared band: X, Y".
       (
         SELECT array_agg(DISTINCT sb.bn)
           FROM shared_bands sb
          WHERE EXISTS (
            SELECT 1 FROM jsonb_array_elements(coalesce(a.collaborators, '[]'::jsonb)) v
             WHERE lower(trim(coalesce(v->>'name', v#>>'{}', ''))) = sb.bn
          )
       ) AS shared_bands,
       -- This row IS one of the bands the target lists in collaborators.
       -- (Tampa Red has "Hokum Boys" in collaborators; this row's name
       -- is "Hokum Boys" → surface so user can link as band.)
       (
         SELECT sb.bn FROM shared_bands sb
          WHERE lower(trim(a.name)) = sb.bn
          LIMIT 1
       ) AS is_band,
       -- This row matches a Discogs 'member' name on the target (the
       -- target is a group; this row is one of its members).
       EXISTS (
         SELECT 1 FROM members m
          WHERE lower(trim(a.name)) = m.mn
       ) AS via_member
       FROM blues_artists a
      WHERE a.id <> $1
        AND NOT EXISTS (
          SELECT 1 FROM blues_artist_links l
           WHERE (l.lo_id = $1 AND l.hi_id = a.id)
              OR (l.lo_id = a.id AND l.hi_id = $1)
        )
        AND (
          EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(coalesce(a.aliases, '[]'::jsonb)) v
            WHERE lower(trim(v)) = (SELECT n FROM t)
          )
          OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(coalesce(a.collaborators, '[]'::jsonb)) v
            WHERE lower(trim(coalesce(v->>'name', v#>>'{}', ''))) = (SELECT n FROM t)
          )
          OR (a.notes IS NOT NULL AND position(lower($2) in lower(a.notes)) > 0)
          OR EXISTS (
            SELECT 1
              FROM shared_bands sb
              JOIN jsonb_array_elements(coalesce(a.collaborators, '[]'::jsonb)) v ON true
             WHERE lower(trim(coalesce(v->>'name', v#>>'{}', ''))) = sb.bn
          )
          OR EXISTS (
            SELECT 1 FROM shared_bands sb WHERE lower(trim(a.name)) = sb.bn
          )
          OR EXISTS (
            SELECT 1 FROM members m WHERE lower(trim(a.name)) = m.mn
          )
        )
      ORDER BY lower(a.name) ASC
      LIMIT 100`, [id, n, sharedBandsLc, membersLc]);
    return r.rows.map(row => {
        const matched = [];
        if (row.in_aliases)
            matched.push("aliases");
        if (row.in_collabs)
            matched.push("collaborators");
        if (row.in_notes)
            matched.push("notes");
        if (row.shared_bands && row.shared_bands.length) {
            // Keep the band names short — first 2 + ellipsis if more.
            const list = row.shared_bands.slice(0, 2).join(", ");
            const more = row.shared_bands.length > 2 ? ` (+${row.shared_bands.length - 2})` : "";
            matched.push(`shared band: ${list}${more}`);
        }
        if (row.is_band)
            matched.push(`is the band "${row.is_band}"`);
        if (row.via_member)
            matched.push("Discogs member");
        return { id: row.id, name: row.name, matched };
    });
}
export const BLUES_ARTIST_LINK_KINDS = [
    "pseudonym", "band", "spouse", "traveled", "mentor", "family",
];
export async function addBluesArtistLink(aId, bId, kind) {
    if (!Number.isFinite(aId) || !Number.isFinite(bId) || aId === bId) {
        throw new Error("Invalid artist ids");
    }
    if (!BLUES_ARTIST_LINK_KINDS.includes(kind)) {
        throw new Error("Invalid kind");
    }
    const lo = Math.min(aId, bId);
    const hi = Math.max(aId, bId);
    // PK is (lo, hi, kind) so multiple kinds per pair coexist (Family +
    // Band, etc). ON CONFLICT DO NOTHING makes this idempotent — clicking
    // Band twice on the same pair is a no-op rather than an error.
    await getPool().query(`INSERT INTO blues_artist_links (lo_id, hi_id, kind)
     VALUES ($1, $2, $3)
     ON CONFLICT (lo_id, hi_id, kind) DO NOTHING`, [lo, hi, kind]);
}
// Snapshot of the entire connection graph for the Connections viz.
// Nodes carry just the bits the client needs to draw (id, name, photo)
// and a `degree` field so the viz can size nodes by connectedness.
// Isolated artists (zero links) are excluded — they'd add noise to the
// graph without informing any relationship.
export async function listBluesConnectionsGraph() {
    const er = await getPool().query(`SELECT lo_id, hi_id, kind FROM blues_artist_links ORDER BY lo_id, hi_id`);
    const edges = er.rows;
    if (!edges.length)
        return { nodes: [], edges: [] };
    const ids = new Set();
    for (const e of edges) {
        ids.add(e.lo_id);
        ids.add(e.hi_id);
    }
    const nr = await getPool().query(`SELECT a.id, a.name, a.photo_url,
            (SELECT COUNT(*)::int FROM blues_artist_links l
              WHERE l.lo_id = a.id OR l.hi_id = a.id) AS degree
       FROM blues_artists a
      WHERE a.id = ANY($1::int[])
      ORDER BY lower(a.name) ASC`, [[...ids]]);
    return { nodes: nr.rows, edges };
}
export async function removeBluesArtistLink(aId, bId, kind) {
    if (!Number.isFinite(aId) || !Number.isFinite(bId))
        return;
    const lo = Math.min(aId, bId);
    const hi = Math.max(aId, bId);
    if (kind) {
        // Scoped delete — pulls just one of multiple kinds on the same pair.
        // Caller's responsibility to ensure `kind` is a valid value; the
        // CHECK constraint on the table catches anything else.
        await getPool().query(`DELETE FROM blues_artist_links WHERE lo_id = $1 AND hi_id = $2 AND kind = $3`, [lo, hi, kind]);
    }
    else {
        // No kind → wipe every link between the pair.
        await getPool().query(`DELETE FROM blues_artist_links WHERE lo_id = $1 AND hi_id = $2`, [lo, hi]);
    }
}
export async function listBluesArtistLinks(aId) {
    const r = await getPool().query(`SELECT
        CASE WHEN l.lo_id = $1 THEN l.hi_id ELSE l.lo_id END AS id,
        l.kind,
        b.name,
        b.discogs_id
       FROM blues_artist_links l
       JOIN blues_artists b
         ON b.id = CASE WHEN l.lo_id = $1 THEN l.hi_id ELSE l.lo_id END
      WHERE l.lo_id = $1 OR l.hi_id = $1
      ORDER BY l.kind ASC, lower(b.name) ASC`, [aId]);
    return r.rows;
}
// ── Genre cache-warm cron state helpers ──────────────────────────
// Per-genre rows; every helper takes a genre_key. The scheduler picks
// today's genre via listAllGenreCacheWarmStates() + (dayOfYear %
// activeCount) and works only that row's cursor. Per-genre enable
// flag so individual genres can be paused without touching the others.
export async function listAllGenreCacheWarmStates() {
    const r = await getPool().query(`SELECT * FROM genre_cache_warm_state ORDER BY rotation_order ASC, genre_key ASC`);
    return r.rows;
}
export async function getGenreCacheWarmState(genreKey) {
    const r = await getPool().query(`SELECT * FROM genre_cache_warm_state WHERE genre_key = $1`, [genreKey]);
    return r.rows[0] || null;
}
export async function updateGenreCacheWarmState(genreKey, patch) {
    const allowed = new Set([
        "rotation_order", "enabled", "manual_override",
        "start_year", "end_year",
        "current_year", "current_page",
        "running", "started_at", "last_tick_at", "last_cached_at",
        "lifetime_searched", "lifetime_cached", "lifetime_skipped", "lifetime_errors",
        "cycle_searched", "cycle_cached", "cycle_skipped",
        "cycle_started_at", "cycle_count",
        "recent_errors", "recent_cached",
    ]);
    const sets = [];
    const vals = [];
    for (const [k, v] of Object.entries(patch)) {
        if (!allowed.has(k))
            continue;
        vals.push((k === "recent_errors" || k === "recent_cached") && v != null
            ? JSON.stringify(v)
            : v);
        sets.push(`${k} = $${vals.length}`);
    }
    if (!sets.length)
        return;
    vals.push(genreKey);
    await getPool().query(`UPDATE genre_cache_warm_state SET ${sets.join(", ")} WHERE genre_key = $${vals.length}`, vals);
}
// Atomic "claim a run" for one genre. Flips running false→true so
// only one worker can be active per genre even if two scheduler
// ticks race. Returns true if we got the lock.
export async function tryClaimGenreCacheWarmRun(genreKey) {
    const r = await getPool().query(`UPDATE genre_cache_warm_state
        SET running = true,
            started_at = NOW(),
            last_tick_at = NOW()
      WHERE genre_key = $1 AND running = false
      RETURNING 1`, [genreKey]);
    return (r.rowCount ?? 0) > 0;
}
export async function releaseGenreCacheWarmRun(genreKey) {
    await getPool().query(`UPDATE genre_cache_warm_state SET running = false, last_tick_at = NOW() WHERE genre_key = $1`, [genreKey]);
}
// Mass release lock for stale-lock recovery at scheduler boot.
export async function releaseAllStaleGenreCacheWarmRuns(staleMinutes = 10) {
    const r = await getPool().query(`UPDATE genre_cache_warm_state
        SET running = false
      WHERE running = true
        AND (started_at IS NULL OR started_at < NOW() - ($1 || ' minutes')::interval)
      RETURNING genre_key`, [String(staleMinutes)]);
    return r.rowCount ?? 0;
}
export async function recordGenreCacheWarmHit(genreKey, title, releaseId) {
    await getPool().query(`UPDATE genre_cache_warm_state
        SET lifetime_cached = lifetime_cached + 1,
            cycle_cached    = cycle_cached + 1,
            last_cached_at  = NOW(),
            recent_cached   = (
              jsonb_build_array(jsonb_build_object('id', $3::bigint, 'title', $2::text, 'at', NOW()))
              || (recent_cached - 9)
            )
      WHERE genre_key = $1`, [genreKey, title, releaseId]);
}
export async function recordGenreCacheWarmSkip(genreKey) {
    await getPool().query(`UPDATE genre_cache_warm_state
        SET lifetime_skipped = lifetime_skipped + 1,
            cycle_skipped    = cycle_skipped + 1
      WHERE genre_key = $1`, [genreKey]);
}
export async function recordGenreCacheWarmSearched(genreKey, n) {
    await getPool().query(`UPDATE genre_cache_warm_state
        SET lifetime_searched = lifetime_searched + $2,
            cycle_searched    = cycle_searched + $2
      WHERE genre_key = $1`, [genreKey, n]);
}
export async function recordGenreCacheWarmError(genreKey, msg) {
    await getPool().query(`UPDATE genre_cache_warm_state
        SET lifetime_errors = lifetime_errors + 1,
            recent_errors   = (
              jsonb_build_array(jsonb_build_object('msg', $2::text, 'at', NOW()))
              || (recent_errors - 9)
            )
      WHERE genre_key = $1`, [genreKey, msg.slice(0, 500)]);
}
export async function resetGenreCacheWarmCycle(genreKey) {
    await getPool().query(`UPDATE genre_cache_warm_state
        SET current_year     = start_year,
            current_page     = 1,
            cycle_searched   = 0,
            cycle_cached     = 0,
            cycle_skipped    = 0,
            cycle_started_at = NOW(),
            cycle_count      = cycle_count + 1
      WHERE genre_key = $1`, [genreKey]);
}
// ── Manual cache-warm run helpers ─────────────────────────────────
// One row per (genre, style) combo. Helpers below are designed for
// the single-active-run pattern: the worker reads its row, updates
// the cursor + counters, and persists every page so a restart can
// resume from current_year / current_page.
export async function listCacheWarmRuns() {
    const r = await getPool().query(`SELECT * FROM cache_warm_runs ORDER BY lower(genre_key) ASC, lower(style_key) ASC`);
    return r.rows;
}
export async function getCacheWarmRun(genreKey, styleKey) {
    const r = await getPool().query(`SELECT * FROM cache_warm_runs WHERE genre_key = $1 AND style_key = $2`, [genreKey, styleKey || ""]);
    return r.rows[0] || null;
}
export async function upsertCacheWarmRun(genreKey, styleKey, patch) {
    const allowed = new Set([
        "current_year", "current_page",
        "total_searched", "total_cached", "total_skipped", "total_errors",
        "last_run_at", "last_cached_at",
        "no_year_last_run_at", "no_year_pages_seen",
        "recent_cached", "recent_errors",
    ]);
    const cols = [];
    const vals = [];
    for (const [k, v] of Object.entries(patch)) {
        if (!allowed.has(k))
            continue;
        vals.push((k === "recent_cached" || k === "recent_errors") && v != null ? JSON.stringify(v) : v);
        cols.push(k);
    }
    if (!cols.length) {
        // Just ensure the row exists.
        await getPool().query(`INSERT INTO cache_warm_runs (genre_key, style_key) VALUES ($1, $2)
        ON CONFLICT (genre_key, style_key) DO NOTHING`, [genreKey, styleKey || ""]);
        return;
    }
    const setSql = cols.map((c, i) => `${c} = $${i + 3}`).join(", ");
    const insertCols = ["genre_key", "style_key", ...cols].join(", ");
    const insertVals = ["$1", "$2", ...cols.map((_, i) => `$${i + 3}`)].join(", ");
    await getPool().query(`INSERT INTO cache_warm_runs (${insertCols})
     VALUES (${insertVals})
     ON CONFLICT (genre_key, style_key) DO UPDATE SET ${setSql}`, [genreKey, styleKey || "", ...vals]);
}
export async function recordCacheWarmRunHit(genreKey, styleKey, title, releaseId) {
    await getPool().query(`UPDATE cache_warm_runs
        SET total_cached   = total_cached + 1,
            last_cached_at = NOW(),
            recent_cached  = (
              jsonb_build_array(jsonb_build_object('id', $4::bigint, 'title', $3::text, 'at', NOW()))
              || (recent_cached - 9)
            )
      WHERE genre_key = $1 AND style_key = $2`, [genreKey, styleKey || "", title, releaseId]);
}
export async function recordCacheWarmRunSkip(genreKey, styleKey) {
    await getPool().query(`UPDATE cache_warm_runs SET total_skipped = total_skipped + 1
      WHERE genre_key = $1 AND style_key = $2`, [genreKey, styleKey || ""]);
}
// Bulk variant — one UPDATE for a whole page's worth of cache hits
// instead of N round-trips. No-op when n <= 0.
export async function bumpCacheWarmRunSkip(genreKey, styleKey, n) {
    if (!Number.isFinite(n) || n <= 0)
        return;
    await getPool().query(`UPDATE cache_warm_runs SET total_skipped = total_skipped + $3
      WHERE genre_key = $1 AND style_key = $2`, [genreKey, styleKey || "", n]);
}
export async function recordCacheWarmRunSearched(genreKey, styleKey, n) {
    await getPool().query(`UPDATE cache_warm_runs SET total_searched = total_searched + $3
      WHERE genre_key = $1 AND style_key = $2`, [genreKey, styleKey || "", n]);
}
export async function recordCacheWarmRunError(genreKey, styleKey, msg) {
    await getPool().query(`UPDATE cache_warm_runs
        SET total_errors  = total_errors + 1,
            recent_errors = (
              jsonb_build_array(jsonb_build_object('msg', $3::text, 'at', NOW()))
              || (recent_errors - 9)
            )
      WHERE genre_key = $1 AND style_key = $2`, [genreKey, styleKey || "", msg.slice(0, 500)]);
}
// ── Catalog-number cache-warm helpers ──────────────────────────────
// Sibling helpers to the cache_warm_runs set above, keyed by
// series_key (e.g. "excello:2000-2400") instead of (genre, style).
// One row per defined catalog series; cursor + counters survive
// restarts via the cache_warm_catno_runs table.
export async function getCacheWarmCatnoRun(seriesKey) {
    const r = await getPool().query(`SELECT * FROM cache_warm_catno_runs WHERE series_key = $1`, [seriesKey]);
    return r.rows[0] || null;
}
export async function listCacheWarmCatnoRuns() {
    const r = await getPool().query(`SELECT * FROM cache_warm_catno_runs ORDER BY label ASC, cat_lo ASC`);
    return r.rows || [];
}
export async function upsertCacheWarmCatnoRun(seriesKey, seed, patch = {}) {
    const allowed = new Set([
        "current_catno",
        "phase", "label_sweep_page",
        "total_searched", "total_cached", "total_skipped", "total_errors",
        "last_run_at", "last_cached_at",
        "recent_cached", "recent_errors",
    ]);
    const cols = [];
    const vals = [];
    for (const [k, v] of Object.entries(patch)) {
        if (!allowed.has(k))
            continue;
        vals.push((k === "recent_cached" || k === "recent_errors") && v != null ? JSON.stringify(v) : v);
        cols.push(k);
    }
    // The base UPSERT always carries the seed fields so the row is
    // guaranteed to exist with the configured range / year_max before
    // any subsequent patch hits it.
    const baseCols = ["series_key", "label", "cat_lo", "cat_hi", "year_max", ...cols];
    const baseVals = [seriesKey, seed.label, seed.catLo, seed.catHi, seed.yearMax, ...vals];
    const placeholders = baseCols.map((_, i) => `$${i + 1}`).join(", ");
    const updateSet = cols.length
        ? cols.map((c, i) => `${c} = $${i + 6}`).join(", ")
        : `label = EXCLUDED.label, cat_lo = EXCLUDED.cat_lo, cat_hi = EXCLUDED.cat_hi, year_max = EXCLUDED.year_max`;
    await getPool().query(`INSERT INTO cache_warm_catno_runs (${baseCols.join(", ")})
     VALUES (${placeholders})
     ON CONFLICT (series_key) DO UPDATE SET ${updateSet}`, baseVals);
}
export async function recordCacheWarmCatnoRunHit(seriesKey, title, releaseId) {
    await getPool().query(`UPDATE cache_warm_catno_runs
        SET total_cached   = total_cached + 1,
            last_cached_at = NOW(),
            recent_cached  = (
              jsonb_build_array(jsonb_build_object('id', $3::bigint, 'title', $2::text, 'at', NOW()))
              || (recent_cached - 9)
            )
      WHERE series_key = $1`, [seriesKey, title, releaseId]);
}
export async function bumpCacheWarmCatnoRunSkip(seriesKey, n) {
    if (!Number.isFinite(n) || n <= 0)
        return;
    await getPool().query(`UPDATE cache_warm_catno_runs SET total_skipped = total_skipped + $2
      WHERE series_key = $1`, [seriesKey, n]);
}
export async function recordCacheWarmCatnoRunSearched(seriesKey, n) {
    if (!Number.isFinite(n) || n <= 0)
        return;
    await getPool().query(`UPDATE cache_warm_catno_runs SET total_searched = total_searched + $2
      WHERE series_key = $1`, [seriesKey, n]);
}
export async function recordCacheWarmCatnoRunError(seriesKey, msg) {
    await getPool().query(`UPDATE cache_warm_catno_runs
        SET total_errors  = total_errors + 1,
            recent_errors = (
              jsonb_build_array(jsonb_build_object('msg', $2::text, 'at', NOW()))
              || (recent_errors - 9)
            )
      WHERE series_key = $1`, [seriesKey, msg.slice(0, 500)]);
}
export async function resetCacheWarmCatnoRun(seriesKey) {
    await getPool().query(`UPDATE cache_warm_catno_runs
        SET current_catno    = NULL,
            phase            = 'catno',
            label_sweep_page = NULL,
            total_searched   = 0,
            total_cached     = 0,
            total_skipped    = 0,
            total_errors     = 0,
            recent_cached    = '[]'::jsonb,
            recent_errors    = '[]'::jsonb
      WHERE series_key = $1`, [seriesKey]);
}
// Defensive sanitation for label names — strips HTML tags, decodes
// the handful of entities that commonly leak from upstream scrapes,
// collapses whitespace (including raw newlines), and truncates to a
// reasonable display length. Source for the bug: the abrams-labels.json
// seed file was extracted from Abrams's index page which embeds HTML
// markup in some label names (e.g. "LAKESIDE (USA)</font>...<a href=…>"),
// and `seed.name` becomes external_discography.label_name verbatim.
export function sanitizeLabelName(s) {
    if (s == null)
        return "";
    return String(s)
        .replace(/<[^>]+>/g, "")
        .replace(/&nbsp;/g, " ")
        .replace(/&amp;/g, "&")
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'")
        .replace(/&#?\w+;/g, "")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 200);
}
function _deriveCatnoSort(catno) {
    if (!catno)
        return null;
    const m = String(catno).match(/(\d{1,9})/);
    if (!m)
        return null;
    const n = Number(m[1]);
    return Number.isFinite(n) ? n : null;
}
// Batch upsert with multi-row VALUES — one network round-trip per
// 500-row chunk instead of one per row. The original per-row loop
// was ~100s per page on huge catalogs (Aladdin 3000 = 921 rows,
// Bluebird/Decca race series push thousands), turning the Abrams
// scrape into a multi-hour grind. Chunking at 500 stays well under
// Postgres's 65535-parameter limit (500 × 15 = 7500).
//
// Conflicts on (label_name, catno, side, source) update every other
// field — handy for re-running a parser after fixing a row.
const _BULK_EXT_DISC_CHUNK = 500;
export async function bulkInsertExternalDiscography(rows) {
    if (!rows.length)
        return { inserted: 0 };
    const client = await getPool().connect();
    let inserted = 0;
    try {
        await client.query("BEGIN");
        for (let off = 0; off < rows.length; off += _BULK_EXT_DISC_CHUNK) {
            const chunk = rows.slice(off, off + _BULK_EXT_DISC_CHUNK);
            const valueClauses = [];
            const args = [];
            for (const r of chunk) {
                const sort = r.catno_sort ?? _deriveCatnoSort(r.catno);
                const base = args.length;
                valueClauses.push(`($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},$${base + 11},$${base + 12},$${base + 13},$${base + 14},$${base + 15})`);
                args.push(sanitizeLabelName(r.label_name), r.label_id ?? null, r.catno, sort, r.side ?? null, r.artist ?? null, r.title ?? null, r.year ?? null, r.matrix ?? null, r.xref ?? null, r.loc ?? null, r.composer ?? null, r.notes ?? null, r.source, r.data ? JSON.stringify(r.data) : null);
            }
            await client.query(`INSERT INTO external_discography
           (label_name, label_id, catno, catno_sort, side,
            artist, title, year, matrix, xref, loc, composer, notes,
            source, data)
         VALUES ${valueClauses.join(",")}
         ON CONFLICT (label_name, catno, side, source) DO UPDATE SET
           label_id   = EXCLUDED.label_id,
           catno_sort = EXCLUDED.catno_sort,
           artist     = EXCLUDED.artist,
           title      = EXCLUDED.title,
           year       = EXCLUDED.year,
           matrix     = EXCLUDED.matrix,
           xref       = EXCLUDED.xref,
           loc        = EXCLUDED.loc,
           composer   = EXCLUDED.composer,
           notes      = EXCLUDED.notes,
           data       = EXCLUDED.data`, args);
            inserted += chunk.length;
        }
        await client.query("COMMIT");
    }
    catch (err) {
        await client.query("ROLLBACK");
        throw err;
    }
    finally {
        client.release();
    }
    return { inserted };
}
export async function listExternalDiscographyForLabel(opts) {
    const args = [opts.label];
    const where = [`label_name = $1`];
    if (Number.isFinite(opts.yearFrom)) {
        args.push(opts.yearFrom);
        where.push(`COALESCE(year, 0) >= $${args.length}`);
    }
    if (Number.isFinite(opts.yearTo)) {
        args.push(opts.yearTo);
        where.push(`COALESCE(year, 9999) <= $${args.length}`);
    }
    const r = await getPool().query(`SELECT id, label_name, label_id, catno, catno_sort, side,
            artist, title, year, matrix, xref, loc, composer, notes,
            source, data
       FROM external_discography
      WHERE ${where.join(" AND ")}
      ORDER BY catno_sort ASC NULLS LAST, catno ASC, side ASC NULLS FIRST`, args);
    return r.rows;
}
export async function countExternalDiscographyByLabel() {
    const r = await getPool().query(`SELECT label_name, COUNT(*)::int AS n
       FROM external_discography
      GROUP BY label_name
      ORDER BY n DESC`);
    return r.rows;
}
// Delete external rows whose (label_name, catno_sort) is already
// represented in release_cache — once a Discogs payload covers the
// catalog number, the external stub is redundant. Matching is by
// EXACT label name (release_cache `data->'labels'[i].name` ILIKE
// label_name) plus the leading-numeric component of the cache row's
// catno (which strips off the same prefixes the carousel sort
// normalises out, but only the digits we actually need to match).
// Pass label=undefined to purge across every label at once.
export async function purgeExternalDiscographyCovered(opts = {}) {
    const args = [];
    let labelFilter = "";
    if (opts.label) {
        args.push(opts.label);
        labelFilter = `AND ed.label_name = $${args.length}`;
    }
    // Approach: materialise one row per (label_name_lc, catno_sort)
    // pair that EXISTS in release_cache, then DELETE external rows
    // whose pair matches. The previous correlated-EXISTS form did the
    // jsonb_array_elements lateral scan per ed row and timed out / 500'd
    // on production-size data (150k × 150k).
    const r = await getPool().query(`WITH cache_label_keys AS (
       SELECT DISTINCT
         LOWER(lbl->>'name')                                                    AS label_lc,
         NULLIF((REGEXP_MATCH(COALESCE(lbl->>'catno',''), '(\\d+)'))[1],'')::numeric AS catno_sort
       FROM release_cache rc,
            jsonb_array_elements(COALESCE(rc.data->'labels','[]'::jsonb)) lbl
       WHERE COALESCE(lbl->>'name','')  <> ''
         AND COALESCE(lbl->>'catno','') <> ''
     )
     DELETE FROM external_discography ed
      WHERE ed.catno_sort IS NOT NULL
        ${labelFilter}
        AND EXISTS (
          SELECT 1 FROM cache_label_keys ck
           WHERE ck.label_lc   = LOWER(ed.label_name)
             AND ck.catno_sort = ed.catno_sort
        )`, args);
    return { deleted: r.rowCount ?? 0 };
}
// One-shot retroactive cleanup for rows already inserted with dirty
// label_name (the abrams-labels.json bug). Walks every distinct
// label_name that has HTML markers / runs of whitespace, computes
// the sanitized canonical, then for each row UPDATEs to the new name
// — DELETEing rows whose post-clean (label_name, catno, side, source)
// collides with an existing row (the keeper). Returns how many rows
// were renamed and how many duplicates were dropped.
export async function cleanDirtyExternalDiscographyLabelNames() {
    const dirty = await getPool().query(`SELECT DISTINCT label_name
       FROM external_discography
      WHERE label_name ~ '<[^>]+>|\\s\\s|\n|\r|\t'`);
    const renames = [];
    let renamed = 0;
    let mergedDuplicates = 0;
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        for (const r of dirty.rows) {
            const oldName = String(r.label_name);
            const newName = sanitizeLabelName(oldName);
            if (!newName || newName === oldName)
                continue;
            // Delete rows on NEW name that would collide with rows on OLD
            // name (same catno+side+source) — the OLD row will be renamed
            // and is the keeper.
            const dropRes = await client.query(`DELETE FROM external_discography ed_new
           USING external_discography ed_old
          WHERE ed_old.label_name = $1
            AND ed_new.label_name = $2
            AND ed_new.id <> ed_old.id
            AND ed_new.catno  = ed_old.catno
            AND COALESCE(ed_new.side,'') = COALESCE(ed_old.side,'')
            AND ed_new.source = ed_old.source`, [oldName, newName]);
            mergedDuplicates += dropRes.rowCount ?? 0;
            const updRes = await client.query(`UPDATE external_discography SET label_name = $2 WHERE label_name = $1`, [oldName, newName]);
            renamed += updRes.rowCount ?? 0;
            renames.push({ from: oldName.slice(0, 80), to: newName.slice(0, 80), rows: updRes.rowCount ?? 0 });
        }
        await client.query("COMMIT");
    }
    catch (err) {
        await client.query("ROLLBACK");
        throw err;
    }
    finally {
        client.release();
    }
    return { renamed, mergedDuplicates, renames };
}
// Merge one external_discography label_name into another. Typical
// case: the scraper recorded "Excello" but the canonical Discogs
// name (and label_id) live under "Excello Records", so the directory
// shows them as two separate rows even though they're the same label.
// Rename FROM → TO so the FULL OUTER JOIN in listLabelDirectory
// coalesces the cache + external counts onto one row.
//
// Steps inside one txn:
//   1. Pick a final label_id = MAX across rows on FROM ∪ TO (prefer
//      non-null; ties broken by latter winning, but COALESCE keeps
//      whichever exists).
//   2. Delete any TO rows that would collide (same catno+side+source)
//      with a FROM row after the rename — FROM wins (it's the user's
//      canonical pick to keep, since they're moving everything to TO).
//      Actually: we keep TO's preexisting rows when they exist, drop
//      FROM rows that collide. Reason: TO is the user-picked target,
//      so its established data is more likely the intended one.
//      ... wait, the OPPOSITE is simpler: rename FROM → TO and drop
//      pre-existing TO rows that collide. Then every FROM row becomes
//      a TO row. Consistent and explained.
//   3. UPDATE FROM rows to label_name = TO and label_id = final_id.
//   4. Backfill label_id on any straggling rows on TO that were null.
export async function mergeExternalLabel(fromName, toName) {
    const cleanFrom = sanitizeLabelName(fromName);
    const cleanTo = sanitizeLabelName(toName);
    if (!cleanFrom)
        throw new Error("from label required");
    if (!cleanTo)
        throw new Error("to label required");
    if (cleanFrom === cleanTo)
        throw new Error("from and to are the same label");
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        // Determine the final label_id — prefer TO's, fall back to FROM's.
        const idQ = await client.query(`SELECT
         (SELECT MAX(label_id) FROM external_discography WHERE label_name = $1) AS to_id,
         (SELECT MAX(label_id) FROM external_discography WHERE label_name = $2) AS from_id`, [cleanTo, cleanFrom]);
        const toId = idQ.rows[0]?.to_id;
        const fromId = idQ.rows[0]?.from_id;
        const finalId = toId != null ? Number(toId)
            : fromId != null ? Number(fromId)
                : null;
        // Drop pre-existing TO rows whose (catno, side, source) collides
        // with a FROM row — FROM rows will replace them after the rename.
        const dropQ = await client.query(`DELETE FROM external_discography ed_to
         USING external_discography ed_from
        WHERE ed_to.label_name   = $1
          AND ed_from.label_name = $2
          AND ed_to.id <> ed_from.id
          AND ed_to.catno  = ed_from.catno
          AND COALESCE(ed_to.side,'')   = COALESCE(ed_from.side,'')
          AND ed_to.source = ed_from.source`, [cleanTo, cleanFrom]);
        const mergedDuplicates = dropQ.rowCount ?? 0;
        // Rename FROM → TO with the final id.
        const renQ = await client.query(`UPDATE external_discography
          SET label_name = $1,
              label_id   = COALESCE($3::int, label_id)
        WHERE label_name = $2`, [cleanTo, cleanFrom, finalId]);
        const renamed = renQ.rowCount ?? 0;
        // Backfill label_id on any pre-existing TO rows that still have null.
        if (finalId != null) {
            await client.query(`UPDATE external_discography
            SET label_id = $2
          WHERE label_name = $1 AND label_id IS NULL`, [cleanTo, finalId]);
        }
        await client.query("COMMIT");
        return { renamed, mergedDuplicates, finalLabelId: finalId };
    }
    catch (err) {
        await client.query("ROLLBACK");
        throw err;
    }
    finally {
        client.release();
    }
}
// ── Label upstream stats helpers ─────────────────────────────────
// Bulk lookup: label_id → { total, fetchedAtISO }. Used by the label
// directory endpoint to enrich each row.
export async function getLabelUpstreamStatsMap() {
    const r = await getPool().query(`SELECT label_id, total_releases, fetched_at FROM label_upstream_stats`);
    const map = new Map();
    for (const row of r.rows) {
        map.set(Number(row.label_id), {
            total: row.total_releases == null ? null : Number(row.total_releases),
            fetchedAt: row.fetched_at instanceof Date ? row.fetched_at.toISOString() : String(row.fetched_at),
        });
    }
    return map;
}
// Upsert one label's upstream total. `null` is valid (means Discogs
// returned 404 or the label has no releases — we still record the
// fetched_at so we don't keep retrying immediately).
export async function setLabelUpstreamTotal(labelId, total) {
    await getPool().query(`INSERT INTO label_upstream_stats (label_id, total_releases, fetched_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (label_id) DO UPDATE
       SET total_releases = EXCLUDED.total_releases,
           fetched_at     = NOW()`, [labelId, total]);
}
// Return the ordered list of label_ids from external_discography that
// need a fresh upstream fetch: missing entirely OR older than
// staleCutoff. Sorted by external_count DESC so the highest-value
// labels get counted first. Excludes label_ids we've already tried
// within a short "retry cool-down" so a permanently-404ing label
// doesn't get re-fetched every drain.
export async function listLabelIdsNeedingUpstreamRefresh(opts = {}) {
    const staleDays = Math.max(0, Number(opts.staleAfterDays ?? 30));
    const limit = Math.max(1, Math.min(50000, Number(opts.limit ?? 20000)));
    const r = await getPool().query(`WITH ext AS (
       SELECT label_id, label_name, COUNT(*)::int AS external_count
         FROM external_discography
        WHERE label_id IS NOT NULL AND label_id > 0
        GROUP BY label_id, label_name
     )
     SELECT e.label_id, e.label_name, e.external_count
       FROM ext e
       LEFT JOIN label_upstream_stats s ON s.label_id = e.label_id
      WHERE s.label_id IS NULL
         OR s.fetched_at < NOW() - ($1 || ' days')::interval
      ORDER BY e.external_count DESC NULLS LAST, e.label_id ASC
      LIMIT $2`, [String(staleDays), limit]);
    return r.rows.map(row => ({
        label_id: Number(row.label_id),
        label_name: String(row.label_name),
        external_count: Number(row.external_count ?? 0),
    }));
}
// ── Split-cache reader feature flag ───────────────────────────────
// Off by default. When on (per app_settings.use_split_cache_readers),
// admin panels read from the new split schema; when off they still
// read from release_cache. Set via the toggle in the Cache tab after
// the projection backfill has drained.
const SPLIT_READERS_KEY = "use_split_cache_readers";
export async function isSplitCacheReaderEnabled() {
    try {
        const raw = await getAppSetting(SPLIT_READERS_KEY);
        return raw === "1" || raw === "true";
    }
    catch {
        return false;
    }
}
export async function setSplitCacheReaderEnabled(v) {
    await setAppSetting(SPLIT_READERS_KEY, v ? "1" : "0");
}
export async function listLabelDirectory(opts = {}) {
    const limit = Math.max(1, Math.min(50000, opts.limit ?? 1000));
    const reader = opts.forceReader ??
        ((await isSplitCacheReaderEnabled()) ? "v2" : "v1");
    const rawRows = reader === "v2"
        ? await _listLabelDirectoryV2Raw(opts.search, limit)
        : await _listLabelDirectoryV1Raw(opts.search, limit);
    return _collapseLabelDirectoryAliases(rawRows);
}
// Old JSONB-unrolling reader. Kept until every Phase 2 reader has
// been verified against the split schema and readers are switched
// over globally.
async function _listLabelDirectoryV1Raw(search, limit) {
    const args = [];
    let extraWhere = "";
    if (search) {
        args.push(`%${search}%`);
        extraWhere = `WHERE label_name ILIKE $${args.length}`;
    }
    const r = await getPool().query(`WITH ext AS (
       SELECT
         label_name,
         COUNT(*)::int           AS external_count,
         MAX(label_id)           AS label_id,
         ARRAY_AGG(DISTINCT source ORDER BY source) AS sources
         FROM external_discography
        GROUP BY label_name
     ),
     cache_lbl AS (
       SELECT
         LOWER(lbl->>'name')        AS label_name_lc,
         MIN(lbl->>'name')          AS label_name_display,
         COUNT(*) FILTER (WHERE rc.type = 'release')::int AS cache_releases,
         COUNT(*) FILTER (WHERE rc.type = 'master' )::int AS cache_masters,
         MAX(NULLIF(lbl->>'id','')::int)                 AS label_id_cache
       FROM release_cache rc,
            jsonb_array_elements(COALESCE(rc.data->'labels','[]'::jsonb)) lbl
       WHERE COALESCE(lbl->>'name','') <> ''
       GROUP BY LOWER(lbl->>'name')
     ),
     joined AS (
       SELECT
         COALESCE(ext.label_name,  cache_lbl.label_name_display) AS label_name,
         COALESCE(ext.label_id,    cache_lbl.label_id_cache)      AS label_id,
         COALESCE(ext.external_count, 0) AS external_count,
         COALESCE(cache_lbl.cache_releases, 0) AS cache_releases,
         COALESCE(cache_lbl.cache_masters,  0) AS cache_masters,
         COALESCE(ext.sources,     ARRAY[]::text[])              AS sources
       FROM ext
       FULL OUTER JOIN cache_lbl ON LOWER(ext.label_name) = cache_lbl.label_name_lc
     )
     SELECT * FROM joined
      ${extraWhere}
      ORDER BY external_count DESC NULLS LAST,
               cache_releases DESC NULLS LAST,
               label_name ASC
      LIMIT ${limit}`, args);
    return r.rows.map(row => ({
        label_name: String(row.label_name ?? ""),
        label_id: row.label_id != null ? Number(row.label_id) : null,
        external_count: Number(row.external_count ?? 0),
        cache_releases: Number(row.cache_releases ?? 0),
        cache_masters: Number(row.cache_masters ?? 0),
        sources: Array.isArray(row.sources) ? row.sources : [],
    }));
}
// New reader — same shape, same output, but hits the projected side
// table release_labels + normal GROUP BY instead of the LATERAL JSONB
// scan. Bucket 0 = master, buckets 1/2 = release (orphan/pressing).
async function _listLabelDirectoryV2Raw(search, limit) {
    const args = [];
    let extraWhere = "";
    if (search) {
        args.push(`%${search}%`);
        extraWhere = `WHERE label_name ILIKE $${args.length}`;
    }
    const r = await getPool().query(`WITH ext AS (
       SELECT
         label_name,
         COUNT(*)::int           AS external_count,
         MAX(label_id)           AS label_id,
         ARRAY_AGG(DISTINCT source ORDER BY source) AS sources
         FROM external_discography
        GROUP BY label_name
     ),
     cache_lbl AS (
       SELECT
         LOWER(rl.label_name)        AS label_name_lc,
         MIN(rl.label_name)          AS label_name_display,
         COUNT(*) FILTER (WHERE rl.bucket IN (1,2))::int AS cache_releases,
         COUNT(*) FILTER (WHERE rl.bucket = 0)::int      AS cache_masters,
         MAX(rl.label_id)                                AS label_id_cache
       FROM release_labels rl
       WHERE rl.label_name IS NOT NULL AND rl.label_name <> ''
       GROUP BY LOWER(rl.label_name)
     ),
     joined AS (
       SELECT
         COALESCE(ext.label_name,  cache_lbl.label_name_display) AS label_name,
         COALESCE(ext.label_id,    cache_lbl.label_id_cache)      AS label_id,
         COALESCE(ext.external_count, 0) AS external_count,
         COALESCE(cache_lbl.cache_releases, 0) AS cache_releases,
         COALESCE(cache_lbl.cache_masters,  0) AS cache_masters,
         COALESCE(ext.sources,     ARRAY[]::text[])              AS sources
       FROM ext
       FULL OUTER JOIN cache_lbl ON LOWER(ext.label_name) = cache_lbl.label_name_lc
     )
     SELECT * FROM joined
      ${extraWhere}
      ORDER BY external_count DESC NULLS LAST,
               cache_releases DESC NULLS LAST,
               label_name ASC
      LIMIT ${limit}`, args);
    return r.rows.map(row => ({
        label_name: String(row.label_name ?? ""),
        label_id: row.label_id != null ? Number(row.label_id) : null,
        external_count: Number(row.external_count ?? 0),
        cache_releases: Number(row.cache_releases ?? 0),
        cache_masters: Number(row.cache_masters ?? 0),
        sources: Array.isArray(row.sources) ? row.sources : [],
    }));
}
async function _collapseLabelDirectoryAliases(rawRows) {
    // Fold aliases into their canonical. Alias table is small (dozens
    // of rows at most, probably) so we do the resolution in memory
    // rather than baking it into the CTE.
    const aliasQ = await getPool().query(`SELECT alias_label_id, canonical_label_id, reason FROM label_aliases`);
    if (aliasQ.rows.length === 0)
        return rawRows;
    const aliasMap = new Map();
    for (const a of aliasQ.rows) {
        aliasMap.set(Number(a.alias_label_id), {
            canonical: Number(a.canonical_label_id),
            reason: a.reason ?? null,
        });
    }
    const groups = new Map();
    const passthrough = [];
    for (const row of rawRows) {
        const id = row.label_id;
        if (id == null) {
            passthrough.push(row);
            continue;
        }
        const mapped = aliasMap.get(id);
        const canonicalId = mapped ? mapped.canonical : id;
        let g = groups.get(canonicalId);
        if (!g) {
            g = { canonicalId, rows: [] };
            groups.set(canonicalId, g);
        }
        g.rows.push(row);
    }
    const merged = [...passthrough];
    for (const g of groups.values()) {
        if (g.rows.length === 1 && g.rows[0].label_id === g.canonicalId) {
            merged.push(g.rows[0]);
            continue;
        }
        const canonicalRow = g.rows.find(r => r.label_id === g.canonicalId) ?? g.rows[0];
        const aliasRows = g.rows.filter(r => r !== canonicalRow);
        merged.push({
            ...canonicalRow,
            external_count: g.rows.reduce((a, r) => a + r.external_count, 0),
            cache_releases: g.rows.reduce((a, r) => a + r.cache_releases, 0),
            cache_masters: g.rows.reduce((a, r) => a + r.cache_masters, 0),
            sources: Array.from(new Set(g.rows.flatMap(r => r.sources))),
            aliases: aliasRows.map(r => ({
                label_id: r.label_id ?? 0,
                label_name: r.label_name,
                reason: r.label_id != null ? (aliasMap.get(r.label_id)?.reason ?? null) : null,
            })),
        });
    }
    merged.sort((a, b) => (b.external_count - a.external_count));
    return merged;
}
// ── Label alias mutators ────────────────────────────────────────
export async function addLabelAlias(aliasLabelId, canonicalLabelId, reason) {
    if (!Number.isFinite(aliasLabelId) || aliasLabelId <= 0)
        throw new Error("aliasLabelId required");
    if (!Number.isFinite(canonicalLabelId) || canonicalLabelId <= 0)
        throw new Error("canonicalLabelId required");
    if (aliasLabelId === canonicalLabelId)
        throw new Error("alias and canonical are the same id");
    // Guard against alias → alias → canonical chains: if the target is
    // itself an alias, resolve to its canonical first.
    const chain = await getPool().query(`SELECT canonical_label_id FROM label_aliases WHERE alias_label_id = $1`, [canonicalLabelId]);
    const finalCanonical = chain.rows[0]?.canonical_label_id
        ? Number(chain.rows[0].canonical_label_id)
        : canonicalLabelId;
    // And: if this id was already someone else's canonical, that whole
    // group would become orphaned. Flip them all to the new canonical.
    await getPool().query(`UPDATE label_aliases SET canonical_label_id = $1 WHERE canonical_label_id = $2`, [finalCanonical, aliasLabelId]);
    await getPool().query(`INSERT INTO label_aliases (alias_label_id, canonical_label_id, reason)
     VALUES ($1, $2, $3)
     ON CONFLICT (alias_label_id) DO UPDATE
       SET canonical_label_id = EXCLUDED.canonical_label_id,
           reason             = EXCLUDED.reason`, [aliasLabelId, finalCanonical, reason ?? null]);
}
export async function removeLabelAlias(aliasLabelId) {
    if (!Number.isFinite(aliasLabelId))
        throw new Error("aliasLabelId required");
    const r = await getPool().query(`DELETE FROM label_aliases WHERE alias_label_id = $1`, [aliasLabelId]);
    return { removed: (r.rowCount ?? 0) > 0 };
}
// Bulk-set a Discogs label_id for every external_discography row
// matching the given label_name. Idempotent and cheap (UPDATE on an
// indexed column). Returns rowCount.
export async function setLabelDirectoryId(labelName, labelId) {
    if (!labelName)
        throw new Error("labelName required");
    const r = await getPool().query(`UPDATE external_discography SET label_id = $1 WHERE label_name = $2`, [labelId, labelName]);
    return { updated: r.rowCount ?? 0 };
}
export async function computeCacheAnalytics(f, opts = {}) {
    const reader = opts.forceReader ??
        ((await isSplitCacheReaderEnabled()) ? "v2" : "v1");
    return reader === "v2"
        ? _computeCacheAnalyticsV2(f)
        : _computeCacheAnalyticsV1(f);
}
async function _computeCacheAnalyticsV1(f) {
    const args = [];
    const where = [];
    const push = (v) => { args.push(v); return `$${args.length}`; };
    if (f.label) {
        // Match any row whose labels[] contains an entry with this name
        // (case-insensitive substring — the carousel/directory work by
        // exact name so we mirror that convention for exact matches, but
        // let a curator paste "Excello" and catch "Excello Records" too).
        where.push(`EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(rc.data->'labels','[]'::jsonb)) lbl
                        WHERE LOWER(lbl->>'name') LIKE LOWER(${push('%' + f.label + '%')}))`);
    }
    if (f.artist) {
        where.push(`EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(rc.data->'artists','[]'::jsonb)) art
                        WHERE LOWER(art->>'name') LIKE LOWER(${push('%' + f.artist + '%')}))`);
    }
    if (f.genre) {
        where.push(`rc.data->'genres' @> to_jsonb(ARRAY[${push(f.genre)}])`);
    }
    if (f.style) {
        where.push(`rc.data->'styles' @> to_jsonb(ARRAY[${push(f.style)}])`);
    }
    if (f.country) {
        where.push(`rc.data->>'country' = ${push(f.country)}`);
    }
    if (Number.isFinite(f.yearFrom)) {
        where.push(`COALESCE(NULLIF(rc.data->>'year','')::int, 0) >= ${push(f.yearFrom)}`);
    }
    if (Number.isFinite(f.yearTo)) {
        where.push(`COALESCE(NULLIF(rc.data->>'year','')::int, 9999) <= ${push(f.yearTo)}`);
    }
    if (f.type === "release" || f.type === "master") {
        where.push(`rc.type = ${push(f.type)}`);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    // AS MATERIALIZED forces Postgres to compute filtered once instead
    // of inlining the CTE into every downstream aggregation.
    const q = `
    WITH filtered AS MATERIALIZED (
      SELECT rc.discogs_id, rc.type, rc.data
        FROM release_cache rc
        ${whereSql}
    ),
    total AS (SELECT COUNT(*)::bigint AS n FROM filtered),
    genre_facets AS (
      SELECT g AS name, COUNT(*)::int AS n
        FROM filtered,
             jsonb_array_elements_text(COALESCE(data->'genres','[]'::jsonb)) g
        WHERE g <> ''
        GROUP BY g ORDER BY n DESC LIMIT 20
    ),
    style_facets AS (
      SELECT s AS name, COUNT(*)::int AS n
        FROM filtered,
             jsonb_array_elements_text(COALESCE(data->'styles','[]'::jsonb)) s
        WHERE s <> ''
        GROUP BY s ORDER BY n DESC LIMIT 20
    ),
    label_facets AS (
      SELECT lbl->>'name' AS name, COUNT(*)::int AS n
        FROM filtered,
             jsonb_array_elements(COALESCE(data->'labels','[]'::jsonb)) lbl
        WHERE COALESCE(lbl->>'name','') <> ''
        GROUP BY 1 ORDER BY n DESC LIMIT 20
    ),
    artist_facets AS (
      SELECT art->>'name' AS name, COUNT(*)::int AS n
        FROM filtered,
             jsonb_array_elements(COALESCE(data->'artists','[]'::jsonb)) art
        WHERE COALESCE(art->>'name','') <> ''
        GROUP BY 1 ORDER BY n DESC LIMIT 20
    ),
    country_facets AS (
      SELECT data->>'country' AS name, COUNT(*)::int AS n
        FROM filtered
        WHERE COALESCE(data->>'country','') <> ''
        GROUP BY 1 ORDER BY n DESC LIMIT 20
    ),
    decade_facets AS (
      SELECT ((NULLIF(data->>'year','')::int / 10) * 10) AS decade, COUNT(*)::int AS n
        FROM filtered
        WHERE NULLIF(data->>'year','')::int > 0
        GROUP BY 1 ORDER BY 1 ASC
    ),
    sample AS (
      SELECT discogs_id, type,
             data->>'title'                       AS title,
             COALESCE(data->'artists'->0->>'name','') AS artist,
             COALESCE(data->'labels'->0->>'name','')  AS label,
             NULLIF(data->>'year','')::int         AS year
        FROM filtered
        ORDER BY NULLIF(data->>'year','')::int ASC NULLS LAST, discogs_id ASC
        LIMIT 20
    )
    SELECT jsonb_build_object(
      'totalCount', (SELECT n FROM total),
      'facets', jsonb_build_object(
        'genres',    COALESCE((SELECT jsonb_agg(jsonb_build_object('name', name, 'count', n) ORDER BY n DESC) FROM genre_facets),    '[]'::jsonb),
        'styles',    COALESCE((SELECT jsonb_agg(jsonb_build_object('name', name, 'count', n) ORDER BY n DESC) FROM style_facets),    '[]'::jsonb),
        'labels',    COALESCE((SELECT jsonb_agg(jsonb_build_object('name', name, 'count', n) ORDER BY n DESC) FROM label_facets),    '[]'::jsonb),
        'artists',   COALESCE((SELECT jsonb_agg(jsonb_build_object('name', name, 'count', n) ORDER BY n DESC) FROM artist_facets),   '[]'::jsonb),
        'countries', COALESCE((SELECT jsonb_agg(jsonb_build_object('name', name, 'count', n) ORDER BY n DESC) FROM country_facets),  '[]'::jsonb),
        'decades',   COALESCE((SELECT jsonb_agg(jsonb_build_object('decade', decade, 'count', n) ORDER BY decade ASC) FROM decade_facets), '[]'::jsonb)
      ),
      'sample', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'id', discogs_id, 'type', type, 'title', title,
        'artist', artist, 'label', label, 'year', year
      )) FROM sample), '[]'::jsonb)
    ) AS payload
  `;
    const r = await getPool().query(q, args);
    const payload = r.rows[0]?.payload ?? {};
    return {
        totalCount: Number(payload.totalCount ?? 0),
        facets: {
            genres: payload.facets?.genres ?? [],
            styles: payload.facets?.styles ?? [],
            labels: payload.facets?.labels ?? [],
            artists: payload.facets?.artists ?? [],
            countries: payload.facets?.countries ?? [],
            decades: payload.facets?.decades ?? [],
        },
        sample: payload.sample ?? [],
    };
}
// V2 reader — reads from the split schema. cache_all UNIONs
// discogs_cache_masters_plus + discogs_cache_pressings so every
// filter that goes through scalar columns (year/country/type) is a
// straight column lookup, and every filter that goes through
// side-table content (label name / artist name / genre / style) is
// a plain indexed EXISTS instead of a LATERAL jsonb_array_elements.
// bucket is materialised on the union so side-table joins can key
// on (discogs_id, bucket) — same shape as the writer uses.
async function _computeCacheAnalyticsV2(f) {
    const args = [];
    const where = [];
    const push = (v) => { args.push(v); return `$${args.length}`; };
    if (f.label) {
        where.push(`EXISTS (SELECT 1 FROM release_labels rl
                        WHERE rl.discogs_id = ca.discogs_id AND rl.bucket = ca.bucket
                          AND LOWER(rl.label_name) LIKE LOWER(${push('%' + f.label + '%')}))`);
    }
    if (f.artist) {
        where.push(`EXISTS (SELECT 1 FROM release_artists ra
                        WHERE ra.discogs_id = ca.discogs_id AND ra.bucket = ca.bucket
                          AND ra.name IS NOT NULL
                          AND LOWER(ra.name) LIKE LOWER(${push('%' + f.artist + '%')}))`);
    }
    if (f.genre) {
        where.push(`EXISTS (SELECT 1 FROM release_tags rt
                        WHERE rt.discogs_id = ca.discogs_id AND rt.bucket = ca.bucket
                          AND rt.kind = 'genre' AND rt.value = ${push(f.genre)})`);
    }
    if (f.style) {
        where.push(`EXISTS (SELECT 1 FROM release_tags rt
                        WHERE rt.discogs_id = ca.discogs_id AND rt.bucket = ca.bucket
                          AND rt.kind = 'style' AND rt.value = ${push(f.style)})`);
    }
    if (f.country) {
        where.push(`ca.country = ${push(f.country)}`);
    }
    if (Number.isFinite(f.yearFrom)) {
        where.push(`COALESCE(ca.year, 0)::int >= ${push(f.yearFrom)}`);
    }
    if (Number.isFinite(f.yearTo)) {
        where.push(`COALESCE(ca.year, 9999)::int <= ${push(f.yearTo)}`);
    }
    if (f.type === "release" || f.type === "master") {
        where.push(`ca.type = ${push(f.type)}`);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const q = `
    WITH cache_all AS (
      SELECT m.discogs_id,
             m.type,
             m.year,
             m.country,
             m.data,
             CASE WHEN m.type = 'master' THEN 0::smallint ELSE 1::smallint END AS bucket
        FROM discogs_cache_masters_plus m
      UNION ALL
      SELECT p.discogs_id,
             'release'::text AS type,
             p.year,
             p.country,
             p.data,
             2::smallint AS bucket
        FROM discogs_cache_pressings p
    ),
    filtered AS MATERIALIZED (
      SELECT ca.discogs_id, ca.type, ca.bucket, ca.year, ca.country, ca.data
        FROM cache_all ca
        ${whereSql}
    ),
    total AS (SELECT COUNT(*)::bigint AS n FROM filtered),
    genre_facets AS (
      SELECT rt.value AS name, COUNT(*)::int AS n
        FROM filtered f
        JOIN release_tags rt
          ON rt.discogs_id = f.discogs_id AND rt.bucket = f.bucket
        WHERE rt.kind = 'genre'
        GROUP BY rt.value ORDER BY n DESC LIMIT 20
    ),
    style_facets AS (
      SELECT rt.value AS name, COUNT(*)::int AS n
        FROM filtered f
        JOIN release_tags rt
          ON rt.discogs_id = f.discogs_id AND rt.bucket = f.bucket
        WHERE rt.kind = 'style'
        GROUP BY rt.value ORDER BY n DESC LIMIT 20
    ),
    label_facets AS (
      SELECT rl.label_name AS name, COUNT(*)::int AS n
        FROM filtered f
        JOIN release_labels rl
          ON rl.discogs_id = f.discogs_id AND rl.bucket = f.bucket
        GROUP BY rl.label_name ORDER BY n DESC LIMIT 20
    ),
    artist_facets AS (
      SELECT ra.name, COUNT(*)::int AS n
        FROM filtered f
        JOIN release_artists ra
          ON ra.discogs_id = f.discogs_id AND ra.bucket = f.bucket
        WHERE ra.name IS NOT NULL
        GROUP BY ra.name ORDER BY n DESC LIMIT 20
    ),
    country_facets AS (
      SELECT country AS name, COUNT(*)::int AS n
        FROM filtered
        WHERE country IS NOT NULL AND country <> ''
        GROUP BY 1 ORDER BY n DESC LIMIT 20
    ),
    decade_facets AS (
      SELECT ((year::int / 10) * 10) AS decade, COUNT(*)::int AS n
        FROM filtered
        WHERE year IS NOT NULL AND year > 0
        GROUP BY 1 ORDER BY 1 ASC
    ),
    sample AS (
      SELECT discogs_id, type,
             data->>'title'                              AS title,
             COALESCE(data->'artists'->0->>'name','')    AS artist,
             COALESCE(data->'labels'->0->>'name','')     AS label,
             year::int                                   AS year_out
        FROM filtered
        ORDER BY year ASC NULLS LAST, discogs_id ASC
        LIMIT 20
    )
    SELECT jsonb_build_object(
      'totalCount', (SELECT n FROM total),
      'facets', jsonb_build_object(
        'genres',    COALESCE((SELECT jsonb_agg(jsonb_build_object('name', name, 'count', n) ORDER BY n DESC) FROM genre_facets),    '[]'::jsonb),
        'styles',    COALESCE((SELECT jsonb_agg(jsonb_build_object('name', name, 'count', n) ORDER BY n DESC) FROM style_facets),    '[]'::jsonb),
        'labels',    COALESCE((SELECT jsonb_agg(jsonb_build_object('name', name, 'count', n) ORDER BY n DESC) FROM label_facets),    '[]'::jsonb),
        'artists',   COALESCE((SELECT jsonb_agg(jsonb_build_object('name', name, 'count', n) ORDER BY n DESC) FROM artist_facets),   '[]'::jsonb),
        'countries', COALESCE((SELECT jsonb_agg(jsonb_build_object('name', name, 'count', n) ORDER BY n DESC) FROM country_facets),  '[]'::jsonb),
        'decades',   COALESCE((SELECT jsonb_agg(jsonb_build_object('decade', decade, 'count', n) ORDER BY decade ASC) FROM decade_facets), '[]'::jsonb)
      ),
      'sample', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'id', discogs_id, 'type', type, 'title', title,
        'artist', artist, 'label', label, 'year', year_out
      )) FROM sample), '[]'::jsonb)
    ) AS payload
  `;
    const r = await getPool().query(q, args);
    const payload = r.rows[0]?.payload ?? {};
    return {
        totalCount: Number(payload.totalCount ?? 0),
        facets: {
            genres: payload.facets?.genres ?? [],
            styles: payload.facets?.styles ?? [],
            labels: payload.facets?.labels ?? [],
            artists: payload.facets?.artists ?? [],
            countries: payload.facets?.countries ?? [],
            decades: payload.facets?.decades ?? [],
        },
        sample: payload.sample ?? [],
    };
}
// ── Year backfill (label + catno → missing year) ─────────────────
//
// Donor pool unions:
//   * external_discography rows (research-grade, preferred)
//   * release_cache rows that already have a year (Discogs siblings)
// keyed on (LOWER(label_name), catno_sort) — the leading numeric
// component of the catalog number, same normalisation external rows
// already store.
//
// Phase 1 (release): for each unknown-year release_cache row, find a
//   donor and assign earliest year (external beats cache; ties broken
//   by earliest year).
// Phase 2 (master):  for each unknown-year master, year = MIN over
//   versions in cache. Runs AFTER phase 1 so phase-1 lifts can propagate
//   up.
//
// Every change is logged to year_backfill_log keyed by batch_id so
// rollbackYearBackfillBatch() can reverse a whole pass.
const _YEAR_BACKFILL_DONOR_CTE = `
  WITH donors_raw AS (
    -- External (research) donors
    SELECT
      LOWER(label_name) AS label_name,
      catno_sort,
      year,
      'external:' || id::text AS donor_ref,
      source AS donor_source,
      0 AS priority
    FROM external_discography
    WHERE year IS NOT NULL AND catno_sort IS NOT NULL

    UNION ALL

    -- Discogs cache siblings: any cache row with a known year, keyed
    -- by its label+catno entries.
    SELECT
      LOWER(lbl->>'name')                                                 AS label_name,
      NULLIF((REGEXP_MATCH(COALESCE(lbl->>'catno',''), '(\\d+)'))[1],'')::numeric AS catno_sort,
      NULLIF(rc.data->>'year','')::int                                   AS year,
      'release_cache:' || rc.discogs_id::text                            AS donor_ref,
      'release_cache'                                                    AS donor_source,
      1 AS priority
      FROM release_cache rc,
           jsonb_array_elements(COALESCE(rc.data->'labels','[]'::jsonb)) lbl
     WHERE COALESCE(NULLIF(rc.data->>'year','')::int, 0) > 0
       AND COALESCE(lbl->>'name','') <> ''
       AND COALESCE(lbl->>'catno','') <> ''
  ),
  donors AS (
    SELECT DISTINCT ON (label_name, catno_sort)
      label_name, catno_sort, year, donor_ref, donor_source
    FROM donors_raw
    WHERE label_name IS NOT NULL AND catno_sort IS NOT NULL
    ORDER BY label_name, catno_sort, priority ASC, year ASC
  ),
  target_label_pairs AS (
    SELECT
      rc.discogs_id,
      rc.type,
      LOWER(lbl->>'name')                                                AS label_name,
      NULLIF((REGEXP_MATCH(COALESCE(lbl->>'catno',''), '(\\d+)'))[1],'')::numeric AS catno_sort,
      lbl->>'name'  AS label_name_raw,
      lbl->>'catno' AS catno_raw
    FROM release_cache rc,
         jsonb_array_elements(COALESCE(rc.data->'labels','[]'::jsonb)) lbl
    WHERE COALESCE(NULLIF(rc.data->>'year','')::int, 0) = 0
      AND COALESCE(lbl->>'name','')  <> ''
      AND COALESCE(lbl->>'catno','') <> ''
  ),
  matches AS (
    SELECT DISTINCT ON (t.discogs_id, t.type)
      t.discogs_id, t.type,
      d.year       AS new_year,
      d.donor_ref,
      d.donor_source,
      t.label_name_raw AS label_name,
      t.catno_raw      AS catno
    FROM target_label_pairs t
    JOIN donors d
      ON d.label_name = t.label_name
     AND d.catno_sort = t.catno_sort
    ORDER BY t.discogs_id, t.type, d.year ASC
  )
`;
export async function previewYearBackfill() {
    const phase1Q = await getPool().query(`${_YEAR_BACKFILL_DONOR_CTE}
     SELECT donor_source, COUNT(*)::int AS n FROM matches GROUP BY donor_source ORDER BY n DESC`);
    const totalQ = await getPool().query(`${_YEAR_BACKFILL_DONOR_CTE}
     SELECT COUNT(*)::int AS n FROM matches`);
    const sampleQ = await getPool().query(`${_YEAR_BACKFILL_DONOR_CTE}
     SELECT discogs_id, type, new_year, label_name, catno, donor_source, donor_ref
       FROM matches ORDER BY new_year ASC, discogs_id ASC LIMIT 20`);
    // Phase 2 estimate: masters with no year whose versions DO have years.
    const phase2Q = await getPool().query(`SELECT COUNT(*)::int AS n
       FROM release_cache m
      WHERE m.type = 'master'
        AND COALESCE(NULLIF(m.data->>'year','')::int, 0) = 0
        AND EXISTS (
          SELECT 1 FROM release_cache v
           WHERE v.type = 'release'
             AND COALESCE(NULLIF(v.data->>'master_id','')::bigint, 0) = m.discogs_id
             AND NULLIF(v.data->>'year','')::int > 0
        )`);
    return {
        phase1Release: Number(totalQ.rows[0]?.n ?? 0),
        phase2Master: Number(phase2Q.rows[0]?.n ?? 0),
        bySource: phase1Q.rows.map(r => ({ donor_source: String(r.donor_source), n: Number(r.n) })),
        sample: sampleQ.rows.map(r => ({
            discogs_id: Number(r.discogs_id),
            type: String(r.type),
            new_year: Number(r.new_year),
            label_name: String(r.label_name ?? ""),
            catno: String(r.catno ?? ""),
            donor_source: String(r.donor_source),
            donor_ref: String(r.donor_ref),
        })),
    };
}
export async function applyYearBackfill() {
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        // Generate one batch id for this whole apply pass.
        const batchRow = await client.query(`SELECT gen_random_uuid() AS id`);
        const batchId = String(batchRow.rows[0].id);
        // Phase 1 — bulk. One INSERT logs every match that's still
        // year-less at commit time; one UPDATE lifts every row that got
        // logged. Replaces the earlier per-row loop that ran 2 statements
        // × 150k rows and timed out (the 500 the user was seeing).
        const phase1LogQ = await client.query(`${_YEAR_BACKFILL_DONOR_CTE}
       INSERT INTO year_backfill_log
         (batch_id, discogs_id, type, old_year, new_year, donor_ref, donor_source, label_name, catno)
       SELECT $1::uuid, m.discogs_id, m.type,
              NULLIF(rc.data->>'year','')::int,
              m.new_year, m.donor_ref, m.donor_source, m.label_name, m.catno
         FROM matches m
         JOIN release_cache rc
           ON rc.discogs_id = m.discogs_id AND rc.type = m.type
        WHERE COALESCE(NULLIF(rc.data->>'year','')::int, 0) = 0
       ON CONFLICT (batch_id, discogs_id, type) DO NOTHING`, [batchId]);
        const phase1Logged = phase1LogQ.rowCount ?? 0;
        let phase1Updated = 0;
        if (phase1Logged > 0) {
            const upd = await client.query(`UPDATE release_cache rc
            SET data = jsonb_set(
                         jsonb_set(rc.data, '{year}', to_jsonb(l.new_year::int)),
                         '{_year_backfilled_from}', to_jsonb(l.donor_ref::text)
                       )
           FROM year_backfill_log l
          WHERE l.batch_id = $1::uuid
            AND rc.discogs_id = l.discogs_id
            AND rc.type = l.type
            AND COALESCE(NULLIF(rc.data->>'year','')::int, 0) = 0`, [batchId]);
            phase1Updated = upd.rowCount ?? 0;
            // Propagate into the split schema. A Phase-1 release lives in
            // masters_plus (type='release') when it's an orphan, in
            // pressings when it has a master_id — hit both, filtered by
            // discogs_id + type in the log. Postgres just skips the table
            // that doesn't have the row.
            await client.query(`UPDATE discogs_cache_masters_plus mp
            SET year = l.new_year::smallint,
                data = jsonb_set(
                         jsonb_set(mp.data, '{year}', to_jsonb(l.new_year::int)),
                         '{_year_backfilled_from}', to_jsonb(l.donor_ref::text)
                       )
           FROM year_backfill_log l
          WHERE l.batch_id = $1::uuid
            AND mp.discogs_id = l.discogs_id
            AND mp.type       = l.type
            AND (mp.year IS NULL OR mp.year = 0)`, [batchId]);
            await client.query(`UPDATE discogs_cache_pressings p
            SET year = l.new_year::smallint,
                data = jsonb_set(
                         jsonb_set(p.data, '{year}', to_jsonb(l.new_year::int)),
                         '{_year_backfilled_from}', to_jsonb(l.donor_ref::text)
                       )
           FROM year_backfill_log l
          WHERE l.batch_id = $1::uuid
            AND l.type = 'release'
            AND p.discogs_id = l.discogs_id
            AND (p.year IS NULL OR p.year = 0)`, [batchId]);
        }
        // Phase 2 — bulk. Same shape: log + UPDATE against the log.
        // Master row's year = MIN of its known-year versions in cache.
        const phase2LogQ = await client.query(`INSERT INTO year_backfill_log
         (batch_id, discogs_id, type, old_year, new_year, donor_ref, donor_source)
       SELECT $1::uuid, m.discogs_id, 'master', NULL, agg.new_year,
              'aggregate:versions', 'aggregate:versions'
         FROM release_cache m
         JOIN (
           SELECT COALESCE(NULLIF(v.data->>'master_id','')::bigint, 0) AS master_id,
                  MIN(NULLIF(v.data->>'year','')::int) AS new_year
             FROM release_cache v
            WHERE v.type = 'release'
              AND NULLIF(v.data->>'year','')::int > 0
              AND COALESCE(NULLIF(v.data->>'master_id','')::bigint, 0) > 0
            GROUP BY 1
         ) agg ON agg.master_id = m.discogs_id
        WHERE m.type = 'master'
          AND COALESCE(NULLIF(m.data->>'year','')::int, 0) = 0
          AND agg.new_year IS NOT NULL
       ON CONFLICT (batch_id, discogs_id, type) DO NOTHING`, [batchId]);
        const phase2Logged = phase2LogQ.rowCount ?? 0;
        let phase2Updated = 0;
        if (phase2Logged > 0) {
            const upd = await client.query(`UPDATE release_cache rc
            SET data = jsonb_set(
                         jsonb_set(rc.data, '{year}', to_jsonb(l.new_year::int)),
                         '{_year_backfilled_from}', to_jsonb('aggregate:versions'::text)
                       )
           FROM year_backfill_log l
          WHERE l.batch_id = $1::uuid
            AND l.type = 'master'
            AND rc.discogs_id = l.discogs_id
            AND rc.type = 'master'
            AND COALESCE(NULLIF(rc.data->>'year','')::int, 0) = 0`, [batchId]);
            phase2Updated = upd.rowCount ?? 0;
            // Masters live only in discogs_cache_masters_plus with
            // type='master'. Same shape update.
            await client.query(`UPDATE discogs_cache_masters_plus mp
            SET year = l.new_year::smallint,
                data = jsonb_set(
                         jsonb_set(mp.data, '{year}', to_jsonb(l.new_year::int)),
                         '{_year_backfilled_from}', to_jsonb('aggregate:versions'::text)
                       )
           FROM year_backfill_log l
          WHERE l.batch_id = $1::uuid
            AND l.type = 'master'
            AND mp.discogs_id = l.discogs_id
            AND mp.type = 'master'
            AND (mp.year IS NULL OR mp.year = 0)`, [batchId]);
        }
        const sourceQ = await client.query(`SELECT donor_source, COUNT(*)::int AS n
         FROM year_backfill_log
        WHERE batch_id = $1
        GROUP BY donor_source
        ORDER BY n DESC`, [batchId]);
        await client.query("COMMIT");
        return {
            batchId,
            phase1Updated,
            phase2Updated,
            bySource: sourceQ.rows.map(r => ({ donor_source: String(r.donor_source), n: Number(r.n) })),
        };
    }
    catch (err) {
        await client.query("ROLLBACK");
        throw err;
    }
    finally {
        client.release();
    }
}
export async function listYearBackfillBatches(limit = 20) {
    const r = await getPool().query(`SELECT batch_id::text                              AS batch_id,
            MIN(applied_at)                             AS applied_at,
            COUNT(*)::int                               AS total,
            COUNT(*) FILTER (WHERE rolled_back_at IS NOT NULL)::int AS reverted,
            jsonb_agg(jsonb_build_object('donor_source', donor_source))
                                                        AS sources_raw
       FROM year_backfill_log
      GROUP BY batch_id
      ORDER BY MIN(applied_at) DESC
      LIMIT $1`, [limit]);
    return r.rows.map(row => {
        const counts = new Map();
        for (const s of row.sources_raw) {
            const k = s?.donor_source ?? "(unknown)";
            counts.set(k, (counts.get(k) ?? 0) + 1);
        }
        return {
            batch_id: String(row.batch_id),
            applied_at: row.applied_at?.toISOString?.() ?? String(row.applied_at),
            total: Number(row.total),
            reverted: Number(row.reverted),
            by_source: Array.from(counts.entries())
                .map(([donor_source, n]) => ({ donor_source, n }))
                .sort((a, b) => b.n - a.n),
        };
    });
}
// Reverse one batch: for every log entry not yet rolled back, if the
// cache row's current year still equals our new_year, restore old_year
// (removing the key if old_year was null). Rows whose year has drifted
// since we wrote it are left alone — flagged in `drifted`.
export async function rollbackYearBackfillBatch(batchId) {
    if (!/^[0-9a-f-]{36}$/i.test(batchId))
        throw new Error("bad batch_id");
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        const alreadyPreQ = await client.query(`SELECT COUNT(*)::int AS n FROM year_backfill_log
        WHERE batch_id = $1::uuid AND rolled_back_at IS NOT NULL`, [batchId]);
        const alreadyReverted = Number(alreadyPreQ.rows[0]?.n ?? 0);
        // Bulk rewind. Only rows still un-reverted count.
        // A row is "drifted" if the cache's current year no longer matches
        // what we wrote — leave those alone and record a count.
        const driftQ = await client.query(`SELECT COUNT(*)::int AS n
         FROM year_backfill_log l
         JOIN release_cache rc
           ON rc.discogs_id = l.discogs_id AND rc.type = l.type
        WHERE l.batch_id = $1::uuid
          AND l.rolled_back_at IS NULL
          AND NULLIF(rc.data->>'year','')::int IS DISTINCT FROM l.new_year`, [batchId]);
        const drifted = Number(driftQ.rows[0]?.n ?? 0);
        // Restore rows whose current year still equals new_year:
        //   * old_year IS NULL → strip the year key entirely
        //   * old_year IS NOT NULL → set year back to old_year
        // Both cases also strip the _year_backfilled_from provenance marker.
        const restoreNullQ = await client.query(`UPDATE release_cache rc
          SET data = (rc.data - 'year') - '_year_backfilled_from'
         FROM year_backfill_log l
        WHERE l.batch_id = $1::uuid
          AND l.rolled_back_at IS NULL
          AND l.old_year IS NULL
          AND rc.discogs_id = l.discogs_id
          AND rc.type = l.type
          AND NULLIF(rc.data->>'year','')::int IS NOT DISTINCT FROM l.new_year`, [batchId]);
        const restoreValQ = await client.query(`UPDATE release_cache rc
          SET data = jsonb_set(rc.data, '{year}', to_jsonb(l.old_year::int)) - '_year_backfilled_from'
         FROM year_backfill_log l
        WHERE l.batch_id = $1::uuid
          AND l.rolled_back_at IS NULL
          AND l.old_year IS NOT NULL
          AND rc.discogs_id = l.discogs_id
          AND rc.type = l.type
          AND NULLIF(rc.data->>'year','')::int IS NOT DISTINCT FROM l.new_year`, [batchId]);
        const reverted = (restoreNullQ.rowCount ?? 0) + (restoreValQ.rowCount ?? 0);
        // Mirror the reversal into the split schema. Same NULL-vs-value
        // branch shape; drift check stays keyed on release_cache above so
        // a split-only drift can't stall the rollback.
        await client.query(`UPDATE discogs_cache_masters_plus mp
          SET year = NULL,
              data = (mp.data - 'year') - '_year_backfilled_from'
         FROM year_backfill_log l
        WHERE l.batch_id = $1::uuid
          AND l.old_year IS NULL
          AND mp.discogs_id = l.discogs_id
          AND mp.type = l.type
          AND (mp.year IS NOT DISTINCT FROM l.new_year::smallint)`, [batchId]);
        await client.query(`UPDATE discogs_cache_masters_plus mp
          SET year = l.old_year::smallint,
              data = jsonb_set(mp.data, '{year}', to_jsonb(l.old_year::int)) - '_year_backfilled_from'
         FROM year_backfill_log l
        WHERE l.batch_id = $1::uuid
          AND l.old_year IS NOT NULL
          AND mp.discogs_id = l.discogs_id
          AND mp.type = l.type
          AND (mp.year IS NOT DISTINCT FROM l.new_year::smallint)`, [batchId]);
        await client.query(`UPDATE discogs_cache_pressings p
          SET year = NULL,
              data = (p.data - 'year') - '_year_backfilled_from'
         FROM year_backfill_log l
        WHERE l.batch_id = $1::uuid
          AND l.old_year IS NULL
          AND l.type = 'release'
          AND p.discogs_id = l.discogs_id
          AND (p.year IS NOT DISTINCT FROM l.new_year::smallint)`, [batchId]);
        await client.query(`UPDATE discogs_cache_pressings p
          SET year = l.old_year::smallint,
              data = jsonb_set(p.data, '{year}', to_jsonb(l.old_year::int)) - '_year_backfilled_from'
         FROM year_backfill_log l
        WHERE l.batch_id = $1::uuid
          AND l.old_year IS NOT NULL
          AND l.type = 'release'
          AND p.discogs_id = l.discogs_id
          AND (p.year IS NOT DISTINCT FROM l.new_year::smallint)`, [batchId]);
        const rollbackBatchRow = await client.query(`SELECT gen_random_uuid() AS id`);
        const rollbackBatchId = String(rollbackBatchRow.rows[0].id);
        // Mark every previously-un-reverted row as rolled back — even the
        // drifted ones. Their state can't be undone anyway; treating them
        // as "handled" prevents the same rollback firing on them again.
        await client.query(`UPDATE year_backfill_log
          SET rolled_back_at = NOW(),
              rolled_back_batch_id = $2::uuid
        WHERE batch_id = $1::uuid
          AND rolled_back_at IS NULL`, [batchId, rollbackBatchId]);
        await client.query("COMMIT");
        return { batchId, reverted, alreadyReverted, drifted };
    }
    catch (err) {
        await client.query("ROLLBACK");
        throw err;
    }
    finally {
        client.release();
    }
}
export async function resetCacheWarmRun(genreKey, styleKey) {
    await getPool().query(`UPDATE cache_warm_runs
        SET current_year    = NULL,
            current_page    = 1,
            total_searched  = 0,
            total_cached    = 0,
            total_skipped   = 0,
            total_errors    = 0,
            recent_cached   = '[]'::jsonb,
            recent_errors   = '[]'::jsonb
      WHERE genre_key = $1 AND style_key = $2`, [genreKey, styleKey || ""]);
}
export async function deleteCacheWarmRun(genreKey, styleKey) {
    await getPool().query(`DELETE FROM cache_warm_runs WHERE genre_key = $1 AND style_key = $2`, [genreKey, styleKey || ""]);
}
// Existence check the worker uses before fetching a release. We only
// pay the Discogs RTT if it's not already cached. (kind defaults to
// 'release' to match how cacheRelease stores its rows.)
export async function isReleaseCached(discogsId, type = "release") {
    if (await isSplitCacheReaderEnabled()) {
        const pool = getPool();
        if (type === "master") {
            const r = await pool.query(`SELECT 1 FROM discogs_cache_masters_plus
          WHERE discogs_id = $1 AND type = 'master' LIMIT 1`, [discogsId]);
            return (r.rowCount ?? 0) > 0;
        }
        // release — either bucket
        const r = await pool.query(`SELECT 1 FROM discogs_cache_pressings WHERE discogs_id = $1
       UNION ALL
       SELECT 1 FROM discogs_cache_masters_plus
        WHERE discogs_id = $1 AND type = 'release'
       LIMIT 1`, [discogsId]);
        return (r.rowCount ?? 0) > 0;
    }
    const r = await getPool().query(`SELECT 1 FROM release_cache WHERE discogs_id = $1 AND type = $2 LIMIT 1`, [discogsId, type]);
    return (r.rowCount ?? 0) > 0;
}
// Batch variant — one query for up to several hundred ids. Returns a
// Set of the ids that are already cached so callers can fast-path
// cache hits without N round-trips. Used by the cache-warm worker to
// skip throttling on a full page of already-cached releases.
export async function getCachedReleaseIds(ids, type = "release") {
    if (!ids.length)
        return new Set();
    if (await isSplitCacheReaderEnabled()) {
        const pool = getPool();
        let q;
        if (type === "master") {
            q = `SELECT discogs_id FROM discogs_cache_masters_plus
            WHERE type = 'master' AND discogs_id = ANY($1::int[])`;
        }
        else {
            // Release rows live in either table — UNION covers both buckets.
            q = `SELECT discogs_id FROM discogs_cache_pressings
            WHERE discogs_id = ANY($1::int[])
           UNION
           SELECT discogs_id FROM discogs_cache_masters_plus
            WHERE type = 'release' AND discogs_id = ANY($1::int[])`;
        }
        const r = await pool.query(q, [ids]);
        return new Set((r.rows ?? []).map((row) => Number(row.discogs_id)));
    }
    const r = await getPool().query(`SELECT discogs_id FROM release_cache WHERE type = $1 AND discogs_id = ANY($2::int[])`, [type, ids]);
    return new Set((r.rows ?? []).map((row) => Number(row.discogs_id)));
}
// GET /masters/{id} carries no `labels` field (labels are a
// release-level concept — a master can span pressings on several
// labels), so a cached master row can never be found by the label
// directory's `data->labels` join no matter how it was cached. The
// label sweep knows which label it matched a master against, so it
// stamps a synthetic single-entry `labels` array onto the cached row
// purely so that join can find it — this doesn't touch any other
// field and never overwrites a non-empty `labels` already present.
export async function backfillCachedMasterLabel(ids, labelName) {
    if (!ids.length || !labelName)
        return 0;
    const labelsJson = JSON.stringify([{ name: labelName }]);
    const r = await getPool().query(`UPDATE release_cache
        SET data = jsonb_set(data, '{labels}', $2::jsonb, true)
      WHERE type = 'master' AND discogs_id = ANY($1::int[])
        AND jsonb_array_length(COALESCE(data->'labels', '[]'::jsonb)) = 0`, [ids, labelsJson]);
    // Mirror the stamp into the split schema so the label directory /
    // analytics see the same label attribution the mini-player would
    // read out of release_cache. Two touches: patch data->labels on
    // masters_plus (masters live there), and INSERT a row into
    // release_labels so the aggregate GROUP BY picks it up. We only
    // insert when the release_labels side is empty for this master, so
    // repeat calls (idempotent) don't multiply the row.
    const pool = getPool();
    try {
        await pool.query(`UPDATE discogs_cache_masters_plus
          SET data = jsonb_set(data, '{labels}', $2::jsonb, true)
        WHERE type = 'master' AND discogs_id = ANY($1::int[])
          AND jsonb_array_length(COALESCE(data->'labels', '[]'::jsonb)) = 0`, [ids, labelsJson]);
        await pool.query(`INSERT INTO release_labels (discogs_id, bucket, label_id, label_name, catno)
       SELECT id, 0::smallint, NULL, $2, NULL
         FROM unnest($1::int[]) AS id
        WHERE NOT EXISTS (
          SELECT 1 FROM release_labels rl
           WHERE rl.discogs_id = id AND rl.bucket = 0
        )`, [ids, labelName]);
    }
    catch (err) {
        console.error("[backfillCachedMasterLabel split mirror]", err?.message ?? err);
    }
    return r.rowCount ?? 0;
}
// Patch a single blues_lyrics row's tuning + artist fields. Both are
// optional — only the keys you pass get updated. Returns the row
// after the write so the client can repaint without a second fetch.
// Patch a single blues_lyrics row's editable fields. Only the keys
// present in `patch` get touched. When `artist` changes, artist_id is
// re-resolved against blues_artists by case-insensitive name match;
// a `null` artist clears artist_id too. Caller can also pass
// `artist_id` directly (e.g. promote-to-artist flow) to short-circuit
// the lookup.
export async function updateLyricFields(id, patch) {
    const sets = [];
    const params = [];
    if ("tuning" in patch) {
        params.push(patch.tuning === "" ? null : patch.tuning ?? null);
        sets.push(`tuning = $${params.length}`);
    }
    if ("artist" in patch) {
        const name = patch.artist === "" ? null : patch.artist ?? null;
        params.push(name);
        sets.push(`artist = $${params.length}`);
        // Re-resolve artist_id from the new name unless the caller has
        // also supplied an explicit artist_id (handled below). Null name
        // → null FK. Postgres-side LOWER(TRIM(...)) keeps the lookup
        // matching the import/merge convention.
        if (!("artist_id" in patch)) {
            if (name) {
                params.push(name);
                sets.push(`artist_id = (SELECT id FROM blues_artists WHERE LOWER(name) = LOWER(TRIM($${params.length})) LIMIT 1)`);
            }
            else {
                sets.push(`artist_id = NULL`);
            }
        }
    }
    if ("artist_id" in patch) {
        if (patch.artist_id === null) {
            sets.push(`artist_id = NULL`);
        }
        else {
            params.push(Number(patch.artist_id));
            sets.push(`artist_id = $${params.length}`);
        }
    }
    if ("page_title" in patch && typeof patch.page_title === "string" && patch.page_title) {
        params.push(patch.page_title);
        sets.push(`page_title = $${params.length}`);
    }
    if ("discogs_release_id" in patch) {
        if (patch.discogs_release_id == null) {
            sets.push(`discogs_release_id = NULL`);
        }
        else {
            params.push(Number(patch.discogs_release_id));
            sets.push(`discogs_release_id = $${params.length}`);
        }
    }
    if ("discogs_master_id" in patch) {
        if (patch.discogs_master_id == null) {
            sets.push(`discogs_master_id = NULL`);
        }
        else {
            params.push(Number(patch.discogs_master_id));
            sets.push(`discogs_master_id = $${params.length}`);
        }
    }
    if ("first_release_year" in patch) {
        if (patch.first_release_year == null) {
            sets.push(`first_release_year = NULL, first_release_source = NULL`);
        }
        else {
            params.push(Number(patch.first_release_year));
            // source='manual' so the curator can audit which years were
            // hand-entered vs derived. Resolver respects this — won't
            // overwrite manual entries unless force=1.
            sets.push(`first_release_year = $${params.length}, first_release_source = 'manual', first_release_checked_at = NOW()`);
        }
    }
    if ("plaintext" in patch) {
        if (patch.plaintext == null) {
            sets.push(`plaintext = NULL`);
        }
        else {
            params.push(String(patch.plaintext));
            sets.push(`plaintext = $${params.length}`);
        }
    }
    if (!sets.length)
        return await getLyricById(id);
    params.push(id);
    await getPool().query(`UPDATE blues_lyrics SET ${sets.join(", ")} WHERE id = $${params.length}`, params);
    return await getLyricById(id);
}
/** Bulk-delete blues_lyrics rows. Hard delete — matches the per-row
 *  DELETE /api/admin/lyrics/:id behaviour and the constraints on
 *  blues_lyrics already cascade their dependent rows (favorites,
 *  artist links) on delete. Returns the count actually removed. */
export async function bulkDeleteLyrics(ids) {
    const cleanIds = Array.from(new Set((ids || [])
        .map((v) => Number(v))
        .filter((n) => Number.isFinite(n) && n > 0)));
    if (!cleanIds.length)
        return 0;
    const r = await getPool().query(`DELETE FROM blues_lyrics WHERE id = ANY($1::int[])`, [cleanIds]);
    return r.rowCount ?? 0;
}
/** Bulk-set the `tuning` column on a set of blues_lyrics rows. Used by
 *  the admin Lyrics bulk-edit bar to corral inconsistent tuning text
 *  ("open d", "Open D maj.", "OPEN D") under one canonical value in a
 *  single round-trip. Empty / null tuning clears the column. Returns
 *  the number of rows actually updated. */
export async function bulkUpdateLyricTuning(ids, tuning) {
    const cleanIds = Array.from(new Set((ids || [])
        .map((v) => Number(v))
        .filter((n) => Number.isFinite(n) && n > 0)));
    if (!cleanIds.length)
        return 0;
    const normalized = (typeof tuning === "string" ? tuning.trim().slice(0, 80) : null);
    const r = await getPool().query(`UPDATE blues_lyrics SET tuning = $1 WHERE id = ANY($2::int[])`, [normalized || null, cleanIds]);
    return r.rowCount ?? 0;
}
/** Return just the lyric ids that match a filter (same params as
 *  listLyrics's WHERE shape). Used by "Select all matching" so the
 *  client can fan a single click out to every row that hits the
 *  current filter — without paginating through them. Hard-capped at
 *  10k ids per call so a curator-glitch can't accidentally fetch the
 *  whole table. */
export async function listLyricIdsMatching(opts) {
    // Reuse listLyrics so the WHERE / param wiring stays in lock-step
    // with the actual listing endpoint. Ask for the cap+1 so we know
    // whether the result hit the limit.
    const CAP = 10000;
    const out = await listLyrics({
        ...opts,
        limit: CAP + 1,
        offset: 0,
    });
    const ids = (out.rows || []).slice(0, CAP).map((r) => Number(r.id));
    return { ids, capped: (out.rows?.length ?? 0) > CAP };
}
// Merge two blues_artists rows. `intoId` is the survivor; `fromId`
// gets deleted after its data is migrated:
//   - lyrics whose artist matched the source row by name are
//     reassigned to the target row's name (case-insensitive match)
//   - discogs_releases JSONB arrays are concatenated (deduped by
//     id + type so a manually-curated target doesn't grow stale
//     duplicates)
//   - other blues_artists columns are left as the target's — if
//     the target lacks data the source had, the admin can re-run
//     "Get all info from Discogs" afterward to re-enrich
// Wrapped in a transaction so a mid-merge crash doesn't leave the
// DB in a half-merged state.
export async function mergeBluesArtists(fromId, intoId) {
    if (fromId === intoId)
        throw new Error("source and target are the same row");
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        const fr = await client.query(`SELECT id, name, discogs_releases FROM blues_artists WHERE id = $1 FOR UPDATE`, [fromId]);
        const tr = await client.query(`SELECT id, name, discogs_releases FROM blues_artists WHERE id = $1 FOR UPDATE`, [intoId]);
        if (!fr.rows.length || !tr.rows.length) {
            await client.query("ROLLBACK");
            throw new Error("one of the artists doesn't exist");
        }
        const from = fr.rows[0];
        const into = tr.rows[0];
        // Reassign lyrics. Two-pass: first update by canonical FK (the
        // common case once backfill has run), then mop up any unmigrated
        // legacy rows that only join by name. Both passes also update the
        // display `artist` string so the UI shows the target's name on
        // every affected row even when a lyric was previously joined only
        // by string.
        const lr1 = await client.query(`UPDATE blues_lyrics
          SET artist_id = $1, artist = $2
        WHERE artist_id = $3`, [into.id, into.name, from.id]);
        const lr2 = await client.query(`UPDATE blues_lyrics
          SET artist_id = $1, artist = $2
        WHERE artist_id IS NULL
          AND LOWER(TRIM(artist)) = LOWER($3)`, [into.id, into.name, from.name]);
        const lyricsReassigned = (lr1.rowCount ?? 0) + (lr2.rowCount ?? 0);
        // Merge discogs_releases JSONB. Dedupe by id+type.
        const fromRels = Array.isArray(from.discogs_releases) ? from.discogs_releases : [];
        const intoRels = Array.isArray(into.discogs_releases) ? into.discogs_releases : [];
        const seen = new Set(intoRels.map((r) => `${r?.type ?? "release"}:${r?.id}`));
        let releasesAdded = 0;
        for (const r of fromRels) {
            const key = `${r?.type ?? "release"}:${r?.id}`;
            if (seen.has(key))
                continue;
            intoRels.push(r);
            seen.add(key);
            releasesAdded++;
        }
        if (releasesAdded) {
            await client.query(`UPDATE blues_artists SET discogs_releases = $1::jsonb WHERE id = $2`, [JSON.stringify(intoRels), intoId]);
        }
        // Drop the source row.
        await client.query(`DELETE FROM blues_artists WHERE id = $1`, [fromId]);
        await client.query("COMMIT");
        return {
            ok: true,
            fromName: from.name,
            intoName: into.name,
            lyricsReassigned,
            releasesAdded,
        };
    }
    catch (e) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        throw e;
    }
    finally {
        client.release();
    }
}
// ── Blues Archive aggregate helpers (admin) ──────────────────────────
// Top-of-page stats strip. One pass per metric — the table is small
// enough that this is fine; index on artist_id + tuning keeps each
// query milliseconds.
export async function getBluesArchiveStats() {
    const r = await getPool().query(`
    WITH a AS (
      SELECT id,
             COALESCE(jsonb_array_length(discogs_releases), 0) > 0 AS has_releases,
             EXISTS (SELECT 1 FROM blues_lyrics l
                      WHERE l.artist_id = blues_artists.id
                         OR (l.artist_id IS NULL AND LOWER(TRIM(l.artist)) = LOWER(blues_artists.name)))
               AS has_lyrics
        FROM blues_artists
    )
    SELECT
      (SELECT COUNT(*)::int FROM blues_artists)                                     AS artists_total,
      (SELECT COUNT(*)::int FROM a WHERE has_lyrics)                                AS artists_with_lyrics,
      (SELECT COUNT(*)::int FROM a WHERE has_releases)                              AS artists_with_releases,
      (SELECT COUNT(*)::int FROM a WHERE has_lyrics AND has_releases)               AS artists_with_both,
      (SELECT COUNT(*)::int FROM a WHERE NOT has_lyrics AND NOT has_releases)       AS artists_empty,
      (SELECT COUNT(*)::int FROM blues_lyrics)                                      AS lyrics_total,
      (SELECT COUNT(*)::int FROM blues_lyrics WHERE artist_id IS NULL)              AS lyrics_orphan,
      (SELECT COUNT(*)::int FROM blues_lyrics WHERE tuning IS NULL OR tuning = '') AS lyrics_missing_tuning
  `);
    return r.rows[0];
}
// Recent edits feed across blues_artists + blues_lyrics, newest first.
// Each row carries `kind` so the UI can label it ("artist" vs "lyric")
// and link appropriately.
export async function getRecentBluesEdits(limit = 20) {
    const lim = Math.max(1, Math.min(200, limit));
    const r = await getPool().query(`
    (SELECT 'artist'::text AS kind, id, name AS title, NULL::text AS artist_name,
            updated_at FROM blues_artists)
    UNION ALL
    (SELECT 'lyric'::text  AS kind, l.id, l.page_title AS title,
            COALESCE(a.name, l.artist) AS artist_name, l.updated_at
       FROM blues_lyrics l
       LEFT JOIN blues_artists a ON a.id = l.artist_id)
    ORDER BY updated_at DESC
    LIMIT $1
  `, [lim]);
    return r.rows;
}
// Bulk reassign lyrics to a target artist. Caller supplies either a
// source artist id (FK match) or a free-text artist string (matches
// LOWER(TRIM(artist)) for unmigrated rows). Both can be passed
// together — the union is reassigned. Single transaction so partial
// failures don't half-rewrite.
export async function reassignLyrics(opts) {
    const toId = Number(opts.toArtistId);
    if (!Number.isFinite(toId))
        throw new Error("toArtistId required");
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        const tr = await client.query(`SELECT id, name FROM blues_artists WHERE id = $1 FOR UPDATE`, [toId]);
        if (!tr.rows.length) {
            await client.query("ROLLBACK");
            throw new Error("target artist not found");
        }
        const to = tr.rows[0];
        let reassigned = 0;
        if (opts.fromArtistId != null) {
            const r = await client.query(`UPDATE blues_lyrics SET artist_id = $1, artist = $2 WHERE artist_id = $3`, [to.id, to.name, Number(opts.fromArtistId)]);
            reassigned += r.rowCount ?? 0;
        }
        if (opts.fromArtistName) {
            const r = await client.query(`UPDATE blues_lyrics SET artist_id = $1, artist = $2
          WHERE LOWER(TRIM(COALESCE(artist, ''))) = LOWER(TRIM($3))
            AND (artist_id IS NULL OR artist_id <> $1)`, [to.id, to.name, opts.fromArtistName]);
            reassigned += r.rowCount ?? 0;
        }
        await client.query("COMMIT");
        return { ok: true, toName: to.name, reassigned };
    }
    catch (e) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        throw e;
    }
    finally {
        client.release();
    }
}
// Promote an orphan lyric (no artist_id) to a brand-new blues_artists
// row using its current `artist` string as the name. Also retro-links
// every other orphan lyric with the same name so a single click
// rescues the whole batch. Transaction.
export async function promoteOrphanLyricToArtist(lyricId) {
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        const lr = await client.query(`SELECT id, artist FROM blues_lyrics WHERE id = $1 FOR UPDATE`, [lyricId]);
        if (!lr.rows.length) {
            await client.query("ROLLBACK");
            throw new Error("lyric not found");
        }
        const name = String(lr.rows[0].artist || "").trim();
        if (!name) {
            await client.query("ROLLBACK");
            throw new Error("lyric has no artist name to promote");
        }
        // If a row already exists with this name (case-insensitive), reuse
        // it instead of creating a duplicate.
        let artistId;
        const existR = await client.query(`SELECT id FROM blues_artists WHERE LOWER(name) = LOWER($1) LIMIT 1`, [name]);
        if (existR.rows.length) {
            artistId = existR.rows[0].id;
        }
        else {
            const insR = await client.query(`INSERT INTO blues_artists (name, enrichment_status)
         VALUES ($1, '{"source":"promote_from_orphan_lyric"}'::jsonb)
         RETURNING id`, [name]);
            artistId = insR.rows[0].id;
        }
        const upR = await client.query(`UPDATE blues_lyrics
          SET artist_id = $1, artist = $2
        WHERE artist_id IS NULL
          AND LOWER(TRIM(COALESCE(artist, ''))) = LOWER($3)`, [artistId, name, name]);
        await client.query("COMMIT");
        return { ok: true, artistId, artistName: name, lyricsLinked: upR.rowCount ?? 0 };
    }
    catch (e) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        throw e;
    }
    finally {
        client.release();
    }
}
export async function runReadonlyQuery(sql, opts = {}) {
    const maxRows = Math.max(1, Math.min(200_000, Math.trunc(opts.maxRows ?? 1000)));
    const timeoutMs = Math.max(1000, Math.min(60_000, Math.trunc(opts.timeoutMs ?? 15_000)));
    const client = await getPool().connect();
    const t0 = Date.now();
    let inTxn = false;
    try {
        await client.query("START TRANSACTION READ ONLY");
        inTxn = true;
        // statement_timeout takes a bare integer as milliseconds. timeoutMs is
        // a clamped integer, so this interpolation is injection-safe.
        await client.query(`SET LOCAL statement_timeout = ${timeoutMs}`);
        // The user SQL is embedded in the cursor declaration. It's validated
        // upstream as a single SELECT/WITH statement, and the READ ONLY txn
        // makes writes impossible regardless — arbitrary reads are the point.
        await client.query(`DECLARE _sdq NO SCROLL CURSOR FOR ${sql}`);
        const res = await client.query({ text: `FETCH FORWARD ${maxRows + 1} FROM _sdq`, rowMode: "array" });
        const columns = (res.fields ?? []).map(f => f.name);
        const all = (res.rows ?? []);
        const truncated = all.length > maxRows;
        const rows = truncated ? all.slice(0, maxRows) : all;
        return { columns, rows, rowCount: rows.length, truncated, elapsedMs: Date.now() - t0 };
    }
    finally {
        if (inTxn) {
            try {
                await client.query("ROLLBACK");
            }
            catch { /* ignore */ }
        }
        client.release();
    }
}
// Tables the Query tab advertises in its schema cheat-sheet and feeds to
// the NL→SQL prompt. Read-only queries can still SELECT from anything
// (per the user's "everything" scope), but we only surface the cache /
// blues / label / lyrics tables here — user-account + OAuth tables stay
// out of the advertised surface and the AI context.
const _QUERYABLE_TABLES = [
    "release_cache", "discogs_cache_masters_plus", "discogs_cache_pressings",
    "release_labels", "release_artists", "release_tags",
    "blues_artists", "blues_lyrics", "blues_tunings_grid",
    "blues_words", "blues_word_citations",
    "external_discography", "all_blues_links", "musicbrainz_cache",
];
export async function getQueryableSchema() {
    const r = await getPool().query(`SELECT table_name, column_name, data_type
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = ANY($1::text[])
      ORDER BY table_name, ordinal_position`, [_QUERYABLE_TABLES]);
    const out = {};
    for (const row of r.rows) {
        (out[row.table_name] ||= []).push({ column: row.column_name, type: row.data_type });
    }
    return out;
}
// ── Bulk artist delete + filter-match ids (Blues Archive) ────────────
// Batched sibling of deleteBluesArtistAndLyrics for the checkbox-driven
// "Delete selected" action. One transaction: optionally cascade every
// lyric linked to the selected artists (FK-linked + legacy name-matched
// orphans), then drop the artist rows. withLyrics=false leaves lyrics
// in place (their artist_id nulls out via ON DELETE SET NULL) — matches
// the single-delete endpoint's default.
export async function bulkDeleteBluesArtists(ids, opts = {}) {
    const clean = Array.from(new Set((ids || []).map(n => Number(n)).filter(n => Number.isFinite(n) && n > 0)));
    if (!clean.length)
        return { deleted: 0, lyricsDeleted: 0 };
    const client = await getPool().connect();
    try {
        await client.query("BEGIN");
        let lyricsDeleted = 0;
        if (opts.withLyrics) {
            // Grab the names up front so the legacy name-match orphan sweep
            // still works after the artist rows are gone.
            const namesR = await client.query(`SELECT name FROM blues_artists WHERE id = ANY($1::int[])`, [clean]);
            const names = namesR.rows
                .map((r) => String(r.name || "").trim().toLowerCase())
                .filter(Boolean);
            const r1 = await client.query(`DELETE FROM blues_lyrics WHERE artist_id = ANY($1::int[])`, [clean]);
            lyricsDeleted += r1.rowCount ?? 0;
            if (names.length) {
                const r2 = await client.query(`DELETE FROM blues_lyrics
            WHERE artist_id IS NULL
              AND LOWER(TRIM(COALESCE(artist, ''))) = ANY($1::text[])`, [names]);
                lyricsDeleted += r2.rowCount ?? 0;
            }
        }
        const rd = await client.query(`DELETE FROM blues_artists WHERE id = ANY($1::int[])`, [clean]);
        await client.query("COMMIT");
        return { deleted: rd.rowCount ?? 0, lyricsDeleted };
    }
    catch (e) {
        try {
            await client.query("ROLLBACK");
        }
        catch { }
        throw e;
    }
    finally {
        client.release();
    }
}
// Every blues_artists id matching the same filters the Blues Archive
// list endpoint uses. Powers "Select all matching" on the bulk-delete
// bar so a filtered set can be actioned beyond the current page. Capped
// at 10k; `capped` signals the caller that matches beyond the cap exist.
// WHERE clauses mirror listBluesArchive — keep the two in sync.
export async function listBluesArtistIdsMatching(opts = {}) {
    const cap = Math.max(1, Math.min(10_000, Math.trunc(opts.limit ?? 10_000)));
    const params = [];
    const where = [];
    if (opts.search) {
        const raw = opts.search.trim();
        params.push(`%${raw}%`);
        const likePh = `$${params.length}`;
        const nameOrNotes = `(a.name ILIKE ${likePh} OR a.notes ILIKE ${likePh})`;
        if (/^\d+$/.test(raw)) {
            params.push(parseInt(raw, 10));
            where.push(`(${nameOrNotes} OR a.discogs_id = $${params.length})`);
        }
        else {
            where.push(nameOrNotes);
        }
    }
    if (opts.category) {
        const HAS_LYRICS = `EXISTS (
      SELECT 1 FROM blues_lyrics l
       WHERE l.artist_id = a.id
          OR (l.artist_id IS NULL AND LOWER(TRIM(l.artist)) = LOWER(a.name))
    )`;
        const HAS_RELEASES = `COALESCE(jsonb_array_length(a.discogs_releases), 0) > 0`;
        if (opts.category === "with_both")
            where.push(`${HAS_LYRICS} AND ${HAS_RELEASES}`);
        else if (opts.category === "with_lyrics_only")
            where.push(`${HAS_LYRICS} AND NOT ${HAS_RELEASES}`);
        else if (opts.category === "with_releases_only")
            where.push(`NOT ${HAS_LYRICS} AND ${HAS_RELEASES}`);
        else if (opts.category === "empty")
            where.push(`NOT ${HAS_LYRICS} AND NOT ${HAS_RELEASES}`);
    }
    if (opts.noWiki)
        where.push(`(a.wikipedia_suffix IS NULL OR a.wikipedia_suffix = '')`);
    if (opts.noDiscogsId)
        where.push(`a.discogs_id IS NULL`);
    if (opts.hasStrict)
        where.push(`a.seed_strict_count > 0`);
    if (opts.noStrict)
        where.push(`COALESCE(a.seed_strict_count, 0) = 0`);
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    params.push(cap + 1);
    const r = await getPool().query(`SELECT a.id FROM blues_artists a ${whereSql} ORDER BY a.id LIMIT $${params.length}`, params);
    const all = r.rows.map((x) => Number(x.id));
    const capped = all.length > cap;
    return { ids: capped ? all.slice(0, cap) : all, capped };
}
