import pg from "pg";
const { Pool } = pg;
import { expandWithSynonyms } from "./classical-synonyms.js";

let pool: InstanceType<typeof Pool> | null = null;

// Exported so feature modules (gutenberg endpoints in search-api.ts, etc.)
// can run ad-hoc queries without forcing every new feature to land a
// pile of helper functions in this file. Existing helpers stay in place
// for stable hot paths.
export function getPool() {
  if (!pool) {
    const connStr = process.env.APP_DB_URL;
    if (!connStr) throw new Error("APP_DB_URL not set");
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
    await getPool().query(
      `UPDATE user_collection SET instance_id = -discogs_release_id WHERE instance_id IS NULL`
    );
    await getPool().query(
      `ALTER TABLE user_collection DROP CONSTRAINT IF EXISTS user_collection_clerk_user_id_discogs_release_id_key`
    );
    await getPool().query(
      `ALTER TABLE user_collection ADD CONSTRAINT user_collection_user_instance_key UNIQUE (clerk_user_id, instance_id)`
    );
  } catch (e) {
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
  // are intentional. 5-night rotation as requested.
  for (const [order, genre] of [
    [1, "Blues"],
    [2, "Folk, World, & Country"],
    [3, "Jazz"],
    [4, "Reggae"],
    [5, "Latin"],
  ] as [number, string][]) {
    await getPool().query(
      `INSERT INTO genre_cache_warm_state (genre_key, rotation_order)
       VALUES ($1, $2)
       ON CONFLICT (genre_key) DO NOTHING`,
      [genre, order],
    );
  }
  // One-time migration: rows seeded with end_year=1960 from the
  // earlier schema get bumped to 2100 so the worker walks all the
  // way through the modern era. Idempotent — no-op once the value
  // has been changed by hand or by a previous run.
  await getPool().query(
    `UPDATE genre_cache_warm_state SET end_year = 2100 WHERE end_year = 1960`,
  );

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
export async function relinkOrphanLyricsToArtists(): Promise<number> {
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
export async function normalizeEmptyTuningsToStandard(): Promise<number> {
  const r = await getPool().query(
    `UPDATE blues_lyrics SET tuning = 'Standard' WHERE tuning IS NULL OR tuning = ''`,
  );
  return r.rowCount ?? 0;
}

// ── Blues lyrics: get-or-create artist by name ─────────────────────
// Backs the "+ Create as new" affordance on the lyric editor's Artist
// input. Looks up by case-insensitive name first to avoid duplicates,
// otherwise inserts a fresh row with enrichment_status flagged so it
// can be distinguished from imported/seeded rows.
export async function getOrCreateBluesArtistByName(rawName: string): Promise<{
  id: number;
  name: string;
  created: boolean;
}> {
  const name = String(rawName || "").replace(/\s+/g, " ").trim();
  if (!name) throw new Error("name required");
  if (name.length > 200) throw new Error("name too long");
  const existing = await getPool().query(
    `SELECT id, name FROM blues_artists WHERE LOWER(name) = LOWER($1) LIMIT 1`,
    [name],
  );
  if (existing.rows.length) {
    return { id: existing.rows[0].id, name: existing.rows[0].name, created: false };
  }
  const inserted = await getPool().query(
    `INSERT INTO blues_artists (name, enrichment_status)
     VALUES ($1, '{"source":"manual_editor_create"}'::jsonb)
     RETURNING id, name`,
    [name],
  );
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
  "last_recording_year",  "last_recording_title",
  "associated_labels", "styles", "instruments",
  "songs_authored", "collaborators",
  "photo_url", "wikipedia_suffix", "youtube_urls", "notes",
  "enrichment_status", "discogs_releases", "external_urls",
] as const;
const _BLUES_JSONB_FIELDS = new Set([
  "aliases", "associated_labels", "styles", "instruments",
  "songs_authored", "collaborators", "youtube_urls",
  "enrichment_status", "discogs_releases", "external_urls",
]);
const _BLUES_INT_FIELDS = new Set([
  "discogs_id", "first_recording_year", "last_recording_year",
]);

function _coerceBluesValue(field: string, value: any): any {
  if (value === undefined || value === null || value === "") return null;
  if (_BLUES_JSONB_FIELDS.has(field)) {
    if (Array.isArray(value) || typeof value === "object") return JSON.stringify(value);
    if (typeof value === "string") {
      // Tolerate comma-separated input from the admin UI for array fields.
      try { JSON.parse(value); return value; }
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
const _BLUES_SORT_COLUMNS: Record<string, string> = {
  name:           "lower(name)",
  birth_date:     "birth_date",
  death_date:     "death_date",
  birth_place:    "lower(coalesce(birth_place,''))",
  hometown_region:"lower(coalesce(hometown_region,''))",
  styles:         "lower(coalesce(styles->>0,''))",
  wikidata_qid:   "wikidata_qid",
  discogs_id:     "discogs_id",
  release_count:  "jsonb_array_length(coalesce(discogs_releases, '[]'::jsonb))",
  earliest_release: _EARLIEST_REL_SQL,
  first_recording_year: "first_recording_year",
  date_added:     "date_added",
  updated_at:     "updated_at",
};

export async function listBluesArtists(opts: { search?: string; limit?: number; offset?: number; sort?: string; order?: string } = {}): Promise<{ rows: any[]; total: number }> {
  const { search, limit = 50, offset = 0, sort = "name", order = "asc" } = opts;
  const args: any[] = [];
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
export async function getBluesArtistDiscogsIds(): Promise<number[]> {
  const r = await getPool().query(`SELECT discogs_id FROM blues_artists WHERE discogs_id IS NOT NULL ORDER BY discogs_id`);
  return r.rows.map(row => row.discogs_id);
}

/** Return both the discogs_ids and the names of every row in
 *  blues_artists. Cards parse artist NAMES from result titles and
 *  don't have a Discogs ID locally, so we cache names too for the
 *  "already in DB?" check on card-level "+" buttons. */
export async function getBluesArtistIdentifiers(): Promise<{ ids: number[]; names: string[] }> {
  const r = await getPool().query(`SELECT discogs_id, name FROM blues_artists`);
  const ids: number[] = [];
  const names: string[] = [];
  for (const row of r.rows) {
    if (row.discogs_id) ids.push(row.discogs_id);
    if (row.name) names.push(row.name);
  }
  return { ids, names };
}

/** Wipe the entire blues_artists table. Admin-only — there's no
 *  per-row history so this is irreversible. */
export async function deleteAllBluesArtists(): Promise<number> {
  const r = await getPool().query(`DELETE FROM blues_artists`);
  return r.rowCount ?? 0;
}

export async function getBluesArtist(id: number): Promise<any | null> {
  const r = await getPool().query(`SELECT * FROM blues_artists WHERE id = $1`, [id]);
  return r.rows[0] ?? null;
}

export async function deleteBluesArtist(id: number): Promise<void> {
  await getPool().query(`DELETE FROM blues_artists WHERE id = $1`, [id]);
}

// Cascade-delete the artist AND every lyric tied to them. Matches
// lyrics by FK (canonical) OR by case-insensitive name (legacy rows
// whose artist_id hasn't been backfilled). Single transaction so a
// partial failure rolls back the whole thing. Returns counts so the
// UI can report exactly how many lyrics went with the artist.
export async function deleteBluesArtistAndLyrics(id: number): Promise<{
  ok: boolean;
  artistName: string;
  lyricsDeleted: number;
}> {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    const ar = await client.query(
      `SELECT id, name FROM blues_artists WHERE id = $1 FOR UPDATE`,
      [id],
    );
    if (!ar.rows.length) { await client.query("ROLLBACK"); throw new Error("artist not found"); }
    const name = String(ar.rows[0].name || "");
    // 1. Drop lyrics linked by FK.
    const r1 = await client.query(
      `DELETE FROM blues_lyrics WHERE artist_id = $1`,
      [id],
    );
    // 2. Drop lyrics that name-match but never got FK-linked (legacy).
    const r2 = name
      ? await client.query(
          `DELETE FROM blues_lyrics
            WHERE artist_id IS NULL
              AND LOWER(TRIM(COALESCE(artist, ''))) = LOWER(TRIM($1))`,
          [name],
        )
      : { rowCount: 0 } as any;
    // 3. Drop the artist.
    await client.query(`DELETE FROM blues_artists WHERE id = $1`, [id]);
    await client.query("COMMIT");
    return {
      ok: true,
      artistName: name,
      lyricsDeleted: (r1.rowCount ?? 0) + (r2.rowCount ?? 0),
    };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
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
export async function resolveLyricFirstReleaseYearsCheap(opts: { force?: boolean } = {}): Promise<{ updated: number }> {
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

// Return blues_lyrics ids missing first_release_year — feed for the
// (future) Discogs-search worker. Capped to keep payloads sane.
export async function getLyricsMissingFirstReleaseYear(limit = 1000): Promise<Array<{ id: number; page_title: string; artist: string | null; artist_id: number | null }>> {
  const r = await getPool().query(
    `SELECT id, page_title, artist, artist_id
       FROM blues_lyrics
      WHERE first_release_year IS NULL
      ORDER BY id ASC
      LIMIT $1`,
    [Math.max(1, Math.min(5000, limit))],
  );
  return r.rows;
}

// ── Lyric favorites ──────────────────────────────────────────────────
export async function listLyricFavoriteIds(userId: string): Promise<number[]> {
  const r = await getPool().query(
    `SELECT lyric_id FROM blues_lyric_favorites WHERE clerk_user_id = $1 ORDER BY created_at DESC`,
    [userId],
  );
  return r.rows.map(row => Number(row.lyric_id));
}
export async function listLyricFavoritesWithDetails(userId: string): Promise<any[]> {
  const r = await getPool().query(
    `SELECT l.id, l.page_title, l.artist, l.artist_id, l.tuning,
            l.discogs_release_id, l.discogs_master_id,
            substring(l.plaintext, 1, 240) AS snippet,
            f.created_at AS favorited_at
       FROM blues_lyric_favorites f
       JOIN blues_lyrics l ON l.id = f.lyric_id
      WHERE f.clerk_user_id = $1
      ORDER BY f.created_at DESC`,
    [userId],
  );
  return r.rows;
}
export async function addLyricFavorite(userId: string, lyricId: number): Promise<boolean> {
  const r = await getPool().query(
    `INSERT INTO blues_lyric_favorites (clerk_user_id, lyric_id) VALUES ($1, $2)
     ON CONFLICT DO NOTHING`,
    [userId, lyricId],
  );
  return (r.rowCount ?? 0) > 0;
}
export async function removeLyricFavorite(userId: string, lyricId: number): Promise<boolean> {
  const r = await getPool().query(
    `DELETE FROM blues_lyric_favorites WHERE clerk_user_id = $1 AND lyric_id = $2`,
    [userId, lyricId],
  );
  return (r.rowCount ?? 0) > 0;
}

// ── Setlists ─────────────────────────────────────────────────────────
export async function listSetlists(userId: string): Promise<any[]> {
  const r = await getPool().query(
    `SELECT s.id, s.name, s.notes, s.created_at, s.updated_at,
            (SELECT COUNT(*)::int FROM blues_setlist_items si WHERE si.setlist_id = s.id) AS item_count
       FROM blues_setlists s
      WHERE s.clerk_user_id = $1
      ORDER BY s.updated_at DESC`,
    [userId],
  );
  return r.rows;
}
export async function getSetlist(userId: string, id: number): Promise<any | null> {
  const sr = await getPool().query(
    `SELECT id, name, notes, created_at, updated_at FROM blues_setlists
      WHERE id = $1 AND clerk_user_id = $2`,
    [id, userId],
  );
  if (!sr.rows.length) return null;
  const items = await getPool().query(
    `SELECT si.lyric_id, si.sort_order, si.note, si.added_at,
            l.page_title, l.artist, l.artist_id, l.tuning,
            l.discogs_release_id, l.discogs_master_id,
            substring(l.plaintext, 1, 240) AS snippet
       FROM blues_setlist_items si
       JOIN blues_lyrics l ON l.id = si.lyric_id
      WHERE si.setlist_id = $1
      ORDER BY si.sort_order ASC, si.lyric_id ASC`,
    [id],
  );
  return { ...sr.rows[0], items: items.rows };
}
export async function createSetlist(userId: string, name: string, notes: string | null = null): Promise<number> {
  const trimmed = name.trim();
  if (!trimmed) throw new Error("name required");
  const r = await getPool().query(
    `INSERT INTO blues_setlists (clerk_user_id, name, notes) VALUES ($1, $2, $3) RETURNING id`,
    [userId, trimmed, notes],
  );
  return Number(r.rows[0].id);
}
export async function updateSetlist(userId: string, id: number, patch: { name?: string; notes?: string | null }): Promise<boolean> {
  const sets: string[] = [];
  const params: any[] = [];
  if (patch.name != null)  { params.push(patch.name.trim());  sets.push(`name = $${params.length}`); }
  if (patch.notes !== undefined) { params.push(patch.notes); sets.push(`notes = $${params.length}`); }
  if (!sets.length) return false;
  params.push(userId, id);
  const r = await getPool().query(
    `UPDATE blues_setlists SET ${sets.join(", ")} WHERE clerk_user_id = $${params.length - 1} AND id = $${params.length}`,
    params,
  );
  return (r.rowCount ?? 0) > 0;
}
export async function deleteSetlist(userId: string, id: number): Promise<boolean> {
  const r = await getPool().query(
    `DELETE FROM blues_setlists WHERE id = $1 AND clerk_user_id = $2`,
    [id, userId],
  );
  return (r.rowCount ?? 0) > 0;
}
export async function addSetlistItem(userId: string, setlistId: number, lyricId: number, note: string | null = null): Promise<boolean> {
  // Verify setlist ownership before write so a malicious caller can't
  // append items to someone else's setlist via id-guessing.
  const own = await getPool().query(
    `SELECT 1 FROM blues_setlists WHERE id = $1 AND clerk_user_id = $2`,
    [setlistId, userId],
  );
  if (!own.rows.length) throw new Error("setlist not found");
  // sort_order defaults to current max + 1 so new items land at the end.
  const r = await getPool().query(
    `INSERT INTO blues_setlist_items (setlist_id, lyric_id, sort_order, note)
       SELECT $1, $2, COALESCE(MAX(sort_order), 0) + 1, $3
         FROM blues_setlist_items WHERE setlist_id = $1
     ON CONFLICT DO NOTHING`,
    [setlistId, lyricId, note],
  );
  await getPool().query(`UPDATE blues_setlists SET updated_at = NOW() WHERE id = $1`, [setlistId]);
  return (r.rowCount ?? 0) > 0;
}
export async function removeSetlistItem(userId: string, setlistId: number, lyricId: number): Promise<boolean> {
  const own = await getPool().query(
    `SELECT 1 FROM blues_setlists WHERE id = $1 AND clerk_user_id = $2`,
    [setlistId, userId],
  );
  if (!own.rows.length) throw new Error("setlist not found");
  const r = await getPool().query(
    `DELETE FROM blues_setlist_items WHERE setlist_id = $1 AND lyric_id = $2`,
    [setlistId, lyricId],
  );
  await getPool().query(`UPDATE blues_setlists SET updated_at = NOW() WHERE id = $1`, [setlistId]);
  return (r.rowCount ?? 0) > 0;
}
export async function reorderSetlistItems(userId: string, setlistId: number, items: Array<{ lyricId: number; sort_order: number }>): Promise<void> {
  const own = await getPool().query(
    `SELECT 1 FROM blues_setlists WHERE id = $1 AND clerk_user_id = $2`,
    [setlistId, userId],
  );
  if (!own.rows.length) throw new Error("setlist not found");
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    for (const it of items) {
      await client.query(
        `UPDATE blues_setlist_items SET sort_order = $1 WHERE setlist_id = $2 AND lyric_id = $3`,
        [Number(it.sort_order) || 0, setlistId, Number(it.lyricId)],
      );
    }
    await client.query(`UPDATE blues_setlists SET updated_at = NOW() WHERE id = $1`, [setlistId]);
    await client.query("COMMIT");
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
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
export async function upsertBluesArtistByQid(record: Record<string, any>): Promise<{ id: number; merged: boolean; createdNew: boolean; matchedBy: "qid" | "discogs_id" | null }> {
  if (!record.wikidata_qid || !record.name) {
    throw new Error("upsertBluesArtistByQid requires wikidata_qid and name");
  }
  // Look up existing row by either key.
  const existing = await getPool().query(
    `SELECT id, wikidata_qid, discogs_id FROM blues_artists
     WHERE wikidata_qid = $1
        OR ($2::int IS NOT NULL AND discogs_id = $2::int)
     ORDER BY (wikidata_qid = $1) DESC
     LIMIT 1`,
    [record.wikidata_qid, record.discogs_id ?? null],
  );
  if (existing.rows.length) {
    const row = existing.rows[0];
    const matchedBy: "qid" | "discogs_id" =
      row.wikidata_qid === record.wikidata_qid ? "qid" : "discogs_id";
    // Build a COALESCE-style UPDATE so non-null existing values win.
    // JSONB array fields use a different rule: prefer the existing
    // array if it has content; otherwise take the new one.
    const sets: string[] = [];
    const vals: any[] = [];
    for (const f of _BLUES_FIELDS) {
      if (!(f in record)) continue;
      vals.push(_coerceBluesValue(f, (record as any)[f]));
      const ph = `$${vals.length}`;
      if (_BLUES_JSONB_FIELDS.has(f)) {
        // Keep existing JSONB unless it's NULL / empty array.
        sets.push(`${f} = CASE
          WHEN ${f} IS NULL OR jsonb_typeof(${f}) = 'null' OR ${f} = '[]'::jsonb
            THEN ${ph}::jsonb
          ELSE ${f}
        END`);
      } else {
        // Keep existing scalar unless it's NULL or empty string.
        sets.push(`${f} = COALESCE(NULLIF(${f}, ''), ${ph})`);
      }
    }
    vals.push(new Date()); sets.push(`updated_at = $${vals.length}`);
    vals.push(row.id);
    await getPool().query(
      `UPDATE blues_artists SET ${sets.join(", ")} WHERE id = $${vals.length}`,
      vals,
    );
    return { id: row.id, merged: true, createdNew: false, matchedBy };
  }
  // No existing row — straight insert.
  const cols: string[] = [];
  const vals: any[] = [];
  const ph: string[] = [];
  for (const f of _BLUES_FIELDS) {
    if (!(f in record)) continue;
    cols.push(f);
    vals.push(_coerceBluesValue(f, (record as any)[f]));
    ph.push(`$${vals.length}`);
  }
  const ins = await getPool().query(
    `INSERT INTO blues_artists (${cols.join(", ")}) VALUES (${ph.join(", ")}) RETURNING id`,
    vals,
  );
  return { id: ins.rows[0].id, merged: false, createdNew: true, matchedBy: null };
}

/** Upsert from the Discogs seeder. Keys on discogs_id. Merges
 *  discogs_releases by release id (union), and only fills name/labels/
 *  styles when the existing row has them blank — never trampling
 *  manually-edited values or richer Wikidata-sourced data. */
export async function upsertBluesArtistByDiscogsId(record: {
  discogs_id: number;
  name: string;
  discogs_releases?: Array<{ id: number; type: string; title?: string; year?: number; label?: string }>;
  associated_labels?: string[];
  styles?: string[];
}): Promise<{ id: number; created: boolean; mergedCount: number }> {
  if (!record.discogs_id || !record.name) {
    throw new Error("upsertBluesArtistByDiscogsId requires discogs_id and name");
  }
  const existing = await getPool().query(
    `SELECT id, name, discogs_releases, associated_labels, styles, enrichment_status FROM blues_artists WHERE discogs_id = $1`,
    [record.discogs_id],
  );
  const newRels = record.discogs_releases ?? [];
  if (existing.rows.length) {
    const row = existing.rows[0];
    const oldRels = Array.isArray(row.discogs_releases) ? row.discogs_releases : [];
    const seen = new Set(oldRels.map((r: any) => `${r.type}:${r.id}`));
    const merged = [...oldRels];
    let added = 0;
    for (const r of newRels) {
      const k = `${r.type}:${r.id}`;
      if (!seen.has(k)) { merged.push(r); seen.add(k); added++; }
    }
    // Fill blank labels/styles, leave populated alone.
    const oldLabels = Array.isArray(row.associated_labels) ? row.associated_labels : [];
    const oldStyles = Array.isArray(row.styles) ? row.styles : [];
    const newLabels = oldLabels.length ? oldLabels : (record.associated_labels ?? []);
    const newStyles = oldStyles.length ? oldStyles : (record.styles ?? []);
    const status = (row.enrichment_status && typeof row.enrichment_status === "object")
      ? row.enrichment_status : {};
    await getPool().query(
      `UPDATE blues_artists
         SET discogs_releases  = $1::jsonb,
             associated_labels = $2::jsonb,
             styles            = $3::jsonb,
             enrichment_status = $4::jsonb,
             updated_at        = NOW()
       WHERE id = $5`,
      [JSON.stringify(merged), JSON.stringify(newLabels), JSON.stringify(newStyles),
       JSON.stringify({ ...status, discogs_seed: 1 }), row.id],
    );
    return { id: row.id, created: false, mergedCount: added };
  }
  // No existing row — fresh insert. Other fields default to blank/empty
  // so a later Wikidata/MB/Wiki enrichment can fill them.
  const ins = await getPool().query(
    `INSERT INTO blues_artists
       (discogs_id, name, discogs_releases, associated_labels, styles, enrichment_status)
     VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb)
     RETURNING id`,
    [
      record.discogs_id,
      record.name,
      JSON.stringify(newRels),
      JSON.stringify(record.associated_labels ?? []),
      JSON.stringify(record.styles ?? []),
      JSON.stringify({ discogs_seed: 1 }),
    ],
  );
  return { id: ins.rows[0].id, created: true, mergedCount: newRels.length };
}

export async function insertBluesArtist(record: Record<string, any>): Promise<number> {
  if (!record.name) throw new Error("insertBluesArtist requires name");
  const cols: string[] = [];
  const vals: any[] = [];
  const ph: string[] = [];
  for (const f of _BLUES_FIELDS) {
    if (!(f in record)) continue;
    cols.push(f);
    vals.push(_coerceBluesValue(f, (record as any)[f]));
    ph.push(`$${vals.length}`);
  }
  const sql = `INSERT INTO blues_artists (${cols.join(", ")}) VALUES (${ph.join(", ")}) RETURNING id`;
  const r = await getPool().query(sql, vals);
  return r.rows[0].id;
}

export async function updateBluesArtist(id: number, record: Record<string, any>): Promise<void> {
  const sets: string[] = [];
  const vals: any[] = [];
  for (const f of _BLUES_FIELDS) {
    if (!(f in record)) continue;
    vals.push(_coerceBluesValue(f, (record as any)[f]));
    sets.push(`${f} = $${vals.length}`);
  }
  if (!sets.length) return;
  vals.push(new Date()); sets.push(`updated_at = $${vals.length}`);
  vals.push(id);
  await getPool().query(
    `UPDATE blues_artists SET ${sets.join(", ")} WHERE id = $${vals.length}`,
    vals,
  );
}

export async function getBluesStats(): Promise<{ total: number; lastSeed: string | null; lastUpdate: string | null }> {
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

// ── Invite-only purge: nuke all per-user data for non-admin clerk_user_ids ──
//
// Used when SeaDisco is locked down to a single admin (or invite-only mode).
// Pass the admin's clerk_user_id to keep their rows intact and wipe everyone
// else from every per-user table. Returns row counts per table.
export async function purgeNonAdminUserData(adminClerkId: string): Promise<Record<string, number>> {
  if (!adminClerkId) throw new Error("adminClerkId required");
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
  const counts: Record<string, number> = {};
  const pool = getPool();
  for (const table of tables) {
    try {
      const r = await pool.query(
        `DELETE FROM ${table} WHERE clerk_user_id <> $1`,
        [adminClerkId]
      );
      counts[table] = r.rowCount ?? 0;
    } catch (e: any) {
      // Table might not exist on a fresh install — record and continue
      counts[table] = -1;
      console.warn(`[purgeNonAdminUserData] ${table}: ${e?.message ?? e}`);
    }
  }
  return counts;
}

// ── Saved searches ──────────────────────────────────────────────────────

export async function getSavedSearches(clerkUserId: string, view?: string): Promise<any[]> {
  const sql = view
    ? `SELECT id, view, label, params, created_at as "createdAt" FROM saved_searches WHERE clerk_user_id = $1 AND view = $2 ORDER BY created_at DESC`
    : `SELECT id, view, label, params, created_at as "createdAt" FROM saved_searches WHERE clerk_user_id = $1 ORDER BY created_at DESC`;
  const r = await getPool().query(sql, view ? [clerkUserId, view] : [clerkUserId]);
  return r.rows;
}

export async function saveSavedSearch(clerkUserId: string, view: string, label: string, params: Record<string, any>): Promise<number> {
  const r = await getPool().query(
    `INSERT INTO saved_searches (clerk_user_id, view, label, params) VALUES ($1, $2, $3, $4) RETURNING id`,
    [clerkUserId, view, label, JSON.stringify(params)]
  );
  return r.rows[0].id;
}

export async function deleteSavedSearch(clerkUserId: string, id: number): Promise<void> {
  await getPool().query(`DELETE FROM saved_searches WHERE id = $1 AND clerk_user_id = $2`, [id, clerkUserId]);
}

// ── Random records (all sources combined) ─────────────────────────────────

export async function getRandomRecords(clerkUserId: string, limit: number = 192): Promise<any[]> {
  // Union all sources, deduplicate by release ID, randomize
  const r = await getPool().query(
    `WITH all_records AS (
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
    SELECT rid, src, data FROM all_records ORDER BY RANDOM() LIMIT $2`,
    [clerkUserId, limit]
  );
  return r.rows;
}

// ── Favorites ──────────────────────────────────────────────────────────────

export async function getFavoriteIds(clerkUserId: string): Promise<Array<{ discogs_id: number; entity_type: string }>> {
  const r = await getPool().query(
    "SELECT discogs_id, entity_type FROM user_favorites WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return r.rows;
}

export async function getFavorites(clerkUserId: string, limit: number = 100, offset: number = 0): Promise<any[]> {
  const r = await getPool().query(
    "SELECT discogs_id, entity_type, data, created_at FROM user_favorites WHERE clerk_user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
    [clerkUserId, limit, offset]
  );
  return r.rows;
}

export async function addFavorite(clerkUserId: string, discogsId: number, entityType: string, data: object): Promise<void> {
  await getPool().query(
    `INSERT INTO user_favorites (clerk_user_id, discogs_id, entity_type, data)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (clerk_user_id, discogs_id, entity_type) DO UPDATE SET data = $4`,
    [clerkUserId, discogsId, entityType, JSON.stringify(data)]
  );
}

export async function removeFavorite(clerkUserId: string, discogsId: number, entityType: string): Promise<void> {
  await getPool().query(
    "DELETE FROM user_favorites WHERE clerk_user_id = $1 AND discogs_id = $2 AND entity_type = $3",
    [clerkUserId, discogsId, entityType]
  );
}

export async function getAllFavoriteCounts(): Promise<Map<string, number>> {
  const r = await getPool().query(
    "SELECT clerk_user_id, COUNT(*)::int AS count FROM user_favorites GROUP BY clerk_user_id"
  );
  const map = new Map<string, number>();
  for (const row of r.rows) map.set(row.clerk_user_id, row.count);
  return map;
}

export async function getAllUsersSyncStatus(): Promise<Array<{
  clerkUserId: string;
  username: string;
  collectionSyncedAt: Date | null;
  wantlistSyncedAt: Date | null;
  syncStatus: string;
  syncProgress: number;
  syncTotal: number;
  syncError: string | null;
  authMethod: string;
  hasPat: boolean;
  hasOAuth: boolean;
}>> {
  const r = await getPool().query(
    `SELECT ut.clerk_user_id, ut.discogs_username, ut.collection_synced_at, ut.wantlist_synced_at,
            ut.sync_status, ut.sync_progress, ut.sync_total, ut.sync_error,
            COALESCE(ut.auth_method, 'none') AS auth_method,
            (ut.discogs_token IS NOT NULL AND ut.discogs_token != '' AND ut.discogs_token != '__oauth__') AS has_pat,
            (ut.oauth_access_token IS NOT NULL AND ut.oauth_access_token != '') AS has_oauth
     FROM user_tokens ut
     WHERE ut.discogs_username IS NOT NULL
     ORDER BY ut.discogs_username`
  );
  return r.rows.map(row => ({
    clerkUserId:        row.clerk_user_id,
    username:           row.discogs_username,
    collectionSyncedAt: row.collection_synced_at ?? null,
    wantlistSyncedAt:   row.wantlist_synced_at   ?? null,
    syncStatus:         row.sync_status          ?? "idle",
    syncProgress:       row.sync_progress        ?? 0,
    syncTotal:          row.sync_total           ?? 0,
    syncError:          row.sync_error           ?? null,
    authMethod:         row.auth_method          ?? "none",
    hasPat:             row.has_pat              ?? false,
    hasOAuth:           row.has_oauth            ?? false,
  }));
}

export async function getPriceStats(): Promise<{
  cacheCount: number;
  historyRows: number;
}> {
  const r = await getPool().query(`
    SELECT
      (SELECT COUNT(*) FROM price_cache) AS cache_count,
      (SELECT COUNT(*) FROM price_history) AS history_rows
  `);
  const row = r.rows[0];
  return {
    cacheCount:        parseInt(row.cache_count) || 0,
    historyRows:       parseInt(row.history_rows) || 0,
  };
}

export async function getAllUsersForSync(): Promise<Array<{ clerkUserId: string; token: string; username: string; collectionSyncedAt: Date | null; wantlistSyncedAt: Date | null }>> {
  const r = await getPool().query(
    `SELECT clerk_user_id, discogs_token, discogs_username, collection_synced_at, wantlist_synced_at
     FROM user_tokens
     WHERE discogs_token IS NOT NULL AND discogs_username IS NOT NULL`
  );
  return r.rows.map(row => ({
    clerkUserId:       row.clerk_user_id,
    token:             row.discogs_token,
    username:          row.discogs_username,
    collectionSyncedAt: row.collection_synced_at ?? null,
    wantlistSyncedAt:   row.wantlist_synced_at   ?? null,
  }));
}

export async function getUserCount(): Promise<number> {
  const r = await getPool().query("SELECT COUNT(*)::int AS cnt FROM user_tokens");
  return r.rows[0]?.cnt ?? 0;
}

export async function getActiveUserCount(): Promise<number> {
  const r = await getPool().query("SELECT COUNT(*)::int AS cnt FROM user_tokens WHERE hibernated_at IS NULL");
  return r.rows[0]?.cnt ?? 0;
}

export async function touchUserActivity(clerkUserId: string): Promise<void> {
  await getPool().query(
    "UPDATE user_tokens SET last_active_at = NOW() WHERE clerk_user_id = $1",
    [clerkUserId]
  );
}

export async function isUserHibernated(clerkUserId: string): Promise<boolean> {
  const r = await getPool().query(
    "SELECT hibernated_at FROM user_tokens WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return r.rows[0]?.hibernated_at != null;
}

export async function reactivateUser(clerkUserId: string): Promise<void> {
  await getPool().query(
    "UPDATE user_tokens SET hibernated_at = NULL, last_active_at = NOW() WHERE clerk_user_id = $1",
    [clerkUserId]
  );
}

export async function hibernateInactiveUsers(): Promise<number> {
  const r = await getPool().query(
    `UPDATE user_tokens
     SET hibernated_at = NOW()
     WHERE hibernated_at IS NULL
       AND last_active_at < NOW() - INTERVAL '6 months'
     RETURNING clerk_user_id`
  );
  return r.rowCount ?? 0;
}

export async function getUserToken(clerkUserId: string): Promise<string | null> {
  const r = await getPool().query(
    "SELECT discogs_token FROM user_tokens WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return r.rows[0]?.discogs_token ?? null;
}

export async function setUserToken(clerkUserId: string, token: string): Promise<void> {
  await getPool().query(
    `INSERT INTO user_tokens (clerk_user_id, discogs_token, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (clerk_user_id)
     DO UPDATE SET discogs_token = $2, updated_at = NOW()`,
    [clerkUserId, token]
  );
}

export async function deleteUserToken(clerkUserId: string): Promise<void> {
  await getPool().query(
    "DELETE FROM user_tokens WHERE clerk_user_id = $1",
    [clerkUserId]
  );
}


export async function saveFeedback(clerkUserId: string, userEmail: string, message: string): Promise<void> {
  await getPool().query(
    `INSERT INTO feedback (clerk_user_id, user_email, message) VALUES ($1, $2, $3)`,
    [clerkUserId, userEmail, message]
  );
}

export async function getFeedback(): Promise<Array<{ id: number; user_email: string; message: string; created_at: string }>> {
  const r = await getPool().query(
    `SELECT id, user_email, message, created_at FROM feedback ORDER BY created_at DESC`
  );
  return r.rows;
}

export async function deleteFeedback(id: number): Promise<void> {
  await getPool().query(`DELETE FROM feedback WHERE id = $1`, [id]);
}

export async function deleteUserData(clerkUserId: string): Promise<void> {
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
    "user_tokens",         // last — other tables may reference it
  ];
  for (const table of tables) {
    try {
      await getPool().query(`DELETE FROM ${table} WHERE clerk_user_id = $1`, [clerkUserId]);
    } catch { /* table may not exist on fresh install */ }
  }
}

export async function getClerkUserIdByUsername(discogsUsername: string): Promise<string | null> {
  const r = await getPool().query(
    "SELECT clerk_user_id FROM user_tokens WHERE discogs_username = $1",
    [discogsUsername]
  );
  return r.rows[0]?.clerk_user_id ?? null;
}

export async function getDiscogsUsername(clerkUserId: string): Promise<string | null> {
  const r = await getPool().query(
    "SELECT discogs_username FROM user_tokens WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return r.rows[0]?.discogs_username ?? null;
}

export async function setDiscogsUsername(clerkUserId: string, username: string): Promise<void> {
  await getPool().query(
    `UPDATE user_tokens SET discogs_username = $2 WHERE clerk_user_id = $1`,
    [clerkUserId, username]
  );
}

// ── OAuth request token helpers (temporary during handshake) ──────────────
export async function storeOAuthRequestToken(token: string, tokenSecret: string, clerkUserId: string, csrfState?: string): Promise<void> {
  await getPool().query(
    `INSERT INTO oauth_request_tokens (token, token_secret, clerk_user_id, csrf_state) VALUES ($1, $2, $3, $4)
     ON CONFLICT (token) DO UPDATE SET token_secret = $2, clerk_user_id = $3, csrf_state = $4, created_at = NOW()`,
    [token, tokenSecret, clerkUserId, csrfState ?? null]
  );
}

export async function getOAuthRequestToken(token: string): Promise<{ tokenSecret: string; clerkUserId: string; csrfState: string | null } | null> {
  const r = await getPool().query(
    `SELECT token_secret, clerk_user_id, csrf_state FROM oauth_request_tokens WHERE token = $1`,
    [token]
  );
  if (!r.rows[0]) return null;
  return { tokenSecret: r.rows[0].token_secret, clerkUserId: r.rows[0].clerk_user_id, csrfState: r.rows[0].csrf_state ?? null };
}

export async function deleteOAuthRequestToken(token: string): Promise<void> {
  await getPool().query(`DELETE FROM oauth_request_tokens WHERE token = $1`, [token]);
}

export async function pruneOAuthRequestTokens(): Promise<void> {
  await getPool().query(`DELETE FROM oauth_request_tokens WHERE created_at < NOW() - INTERVAL '15 minutes'`);
}

// ── OAuth credential storage ─────────────────────────────────────────────
export async function setOAuthCredentials(clerkUserId: string, accessToken: string, accessSecret: string): Promise<void> {
  await getPool().query(
    `UPDATE user_tokens SET oauth_access_token = $2, oauth_access_secret = $3, auth_method = 'oauth', oauth_connected_at = NOW() WHERE clerk_user_id = $1`,
    [clerkUserId, accessToken, accessSecret]
  );
}

export async function getOAuthCredentials(clerkUserId: string): Promise<{ accessToken: string; accessSecret: string } | null> {
  const r = await getPool().query(
    `SELECT oauth_access_token, oauth_access_secret FROM user_tokens WHERE clerk_user_id = $1 AND auth_method = 'oauth'`,
    [clerkUserId]
  );
  if (!r.rows[0]?.oauth_access_token) return null;
  return { accessToken: r.rows[0].oauth_access_token, accessSecret: r.rows[0].oauth_access_secret };
}

export async function clearOAuthCredentials(clerkUserId: string): Promise<void> {
  // Clear OAuth columns and also null out the __oauth__ placeholder token
  await getPool().query(
    `UPDATE user_tokens
     SET oauth_access_token = NULL, oauth_access_secret = NULL, auth_method = 'pat', oauth_connected_at = NULL,
         discogs_token = CASE WHEN discogs_token = '__oauth__' THEN NULL ELSE discogs_token END
     WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
}

export async function getAuthMethod(clerkUserId: string): Promise<string> {
  const r = await getPool().query(
    `SELECT auth_method FROM user_tokens WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  return r.rows[0]?.auth_method ?? "pat";
}

// ── Discogs profile cache ────────────────────────────────────────────────
export async function setDiscogsProfile(
  clerkUserId: string,
  userId: number,
  avatarUrl: string,
  profileData: object & { curr_abbr?: string }
): Promise<void> {
  const currAbbr = (profileData as any)?.curr_abbr ?? null;
  await getPool().query(
    `UPDATE user_tokens
        SET discogs_user_id = $2,
            discogs_avatar_url = $3,
            discogs_profile_data = $4,
            discogs_curr_abbr = COALESCE($5, discogs_curr_abbr),
            profile_synced_at = NOW()
      WHERE clerk_user_id = $1`,
    [clerkUserId, userId, avatarUrl, JSON.stringify(profileData), currAbbr]
  );
}

export async function getDiscogsProfile(clerkUserId: string): Promise<{
  username: string | null;
  userId: number | null;
  avatarUrl: string | null;
  profileData: any;
  authMethod: string;
  currAbbr: string | null;
  profileSyncedAt: Date | null;
}> {
  const r = await getPool().query(
    `SELECT discogs_username, discogs_user_id, discogs_avatar_url, discogs_profile_data,
            auth_method, oauth_connected_at, discogs_curr_abbr, profile_synced_at
       FROM user_tokens WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  const row = r.rows[0];
  if (!row) return { username: null, userId: null, avatarUrl: null, profileData: null, authMethod: "pat", currAbbr: null, profileSyncedAt: null };
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

export async function updateSyncProgress(clerkUserId: string, status: string, progress: number, total: number, error?: string): Promise<void> {
  await getPool().query(
    `UPDATE user_tokens SET sync_status = $2, sync_progress = $3, sync_total = $4, sync_error = $5 WHERE clerk_user_id = $1`,
    [clerkUserId, status, progress, total, error ?? null]
  );
}

export async function resetAllSyncingStatuses(): Promise<number> {
  const r = await getPool().query(
    `UPDATE user_tokens SET sync_status = 'stopped', sync_error = 'Stopped by admin' WHERE sync_status = 'syncing' RETURNING clerk_user_id`
  );
  return r.rowCount ?? 0;
}

export async function getSyncStatus(clerkUserId: string): Promise<{ collectionSyncedAt: Date | null; wantlistSyncedAt: Date | null; syncStatus: string; syncProgress: number; syncTotal: number; syncError: string | null }> {
  const r = await getPool().query(
    "SELECT collection_synced_at, wantlist_synced_at, sync_status, sync_progress, sync_total, sync_error FROM user_tokens WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return {
    collectionSyncedAt: r.rows[0]?.collection_synced_at ?? null,
    wantlistSyncedAt:   r.rows[0]?.wantlist_synced_at   ?? null,
    syncStatus:         r.rows[0]?.sync_status           ?? "idle",
    syncProgress:       r.rows[0]?.sync_progress         ?? 0,
    syncTotal:          r.rows[0]?.sync_total             ?? 0,
    syncError:          r.rows[0]?.sync_error             ?? null,
  };
}

// Get the most recent added_at date for a user's collection (for incremental sync)
export async function getLatestCollectionAddedAt(clerkUserId: string): Promise<Date | null> {
  const r = await getPool().query(
    `SELECT MAX(added_at) AS latest FROM user_collection WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  return r.rows[0]?.latest ?? null;
}

// Get the most recent added_at date for a user's wantlist (for incremental sync)
export async function getLatestWantlistAddedAt(clerkUserId: string): Promise<Date | null> {
  const r = await getPool().query(
    `SELECT MAX(added_at) AS latest FROM user_wantlist WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  return r.rows[0]?.latest ?? null;
}

export async function upsertCollectionItems(
  clerkUserId: string,
  items: Array<{ id: number; data: object; addedAt?: Date; folderId?: number; rating?: number; instanceId?: number; notes?: any[] }>
): Promise<void> {
  if (!items.length) return;
  // Deduplicate by instance_id within the batch. Items without an instance_id get a
  // synthetic negative id derived from the release_id so they still conflict-check
  // correctly against the (clerk_user_id, instance_id) unique constraint.
  const deduped = new Map<number, typeof items[0]>();
  for (const item of items) {
    const key = item.instanceId ?? -item.id;
    deduped.set(key, item);
  }
  const ids:        number[]       = [];
  const dataArr:    string[]       = [];
  const addedArr:   (Date | null)[] = [];
  const folderArr:  number[]       = [];
  const ratingArr:  number[]       = [];
  const instanceArr:number[]       = [];
  const notesArr:   (string|null)[]= [];
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
  await getPool().query(
    `INSERT INTO user_collection (clerk_user_id, discogs_release_id, data, added_at, synced_at, folder_id, rating, instance_id, notes)
     SELECT $1, unnest($2::int[]), unnest($3::jsonb[]), unnest($4::timestamptz[]), NOW(), unnest($5::int[]), unnest($6::int[]), unnest($7::bigint[]), unnest($8::jsonb[])
     ON CONFLICT (clerk_user_id, instance_id)
     DO UPDATE SET data = EXCLUDED.data, added_at = EXCLUDED.added_at, synced_at = NOW(),
                   folder_id = EXCLUDED.folder_id, rating = EXCLUDED.rating,
                   discogs_release_id = EXCLUDED.discogs_release_id, notes = EXCLUDED.notes`,
    [clerkUserId, ids, dataArr, addedArr, folderArr, ratingArr, instanceArr, notesArr]
  );
}

export async function upsertCollectionFolders(
  clerkUserId: string,
  folders: Array<{ id: number; name: string; count: number }>
): Promise<void> {
  // Clear old folders and re-insert
  await getPool().query(`DELETE FROM user_collection_folders WHERE clerk_user_id = $1`, [clerkUserId]);
  for (const f of folders) {
    await getPool().query(
      `INSERT INTO user_collection_folders (clerk_user_id, folder_id, folder_name, item_count)
       VALUES ($1, $2, $3, $4)`,
      [clerkUserId, f.id, f.name, f.count]
    );
  }
}

export async function renameCollectionFolder(
  clerkUserId: string,
  folderId: number,
  newName: string
): Promise<void> {
  await getPool().query(
    `UPDATE user_collection_folders SET folder_name = $3 WHERE clerk_user_id = $1 AND folder_id = $2`,
    [clerkUserId, folderId, newName]
  );
}

export async function deleteCollectionFolder(
  clerkUserId: string,
  folderId: number
): Promise<void> {
  await getPool().query(
    `DELETE FROM user_collection_folders WHERE clerk_user_id = $1 AND folder_id = $2`,
    [clerkUserId, folderId]
  );
  // If the user's default-add folder pointed at this folder, reset it to Uncategorized (1)
  await getPool().query(
    `UPDATE user_tokens SET default_add_folder_id = 1
      WHERE clerk_user_id = $1 AND default_add_folder_id = $2`,
    [clerkUserId, folderId]
  );
}

/** Bulk reassign every collection item in one folder to another (local only). */
export async function moveAllCollectionItemsBetweenFolders(
  clerkUserId: string,
  fromFolderId: number,
  toFolderId: number
): Promise<number> {
  const r = await getPool().query(
    `UPDATE user_collection SET folder_id = $3 WHERE clerk_user_id = $1 AND folder_id = $2 RETURNING 1`,
    [clerkUserId, fromFolderId, toFolderId]
  );
  return r.rowCount ?? 0;
}

/** Return every (releaseId, instanceId, folderId) tuple for a user's items in a specific folder. */
export async function getFolderContents(
  clerkUserId: string,
  folderId: number
): Promise<Array<{ releaseId: number; instanceId: number | null }>> {
  const r = await getPool().query(
    `SELECT discogs_release_id, instance_id FROM user_collection WHERE clerk_user_id = $1 AND folder_id = $2`,
    [clerkUserId, folderId]
  );
  return r.rows.map(row => ({
    releaseId: row.discogs_release_id,
    instanceId: row.instance_id != null && row.instance_id > 0 ? row.instance_id : null,
  }));
}

export async function getCollectionFolderList(
  clerkUserId: string
): Promise<Array<{ folderId: number; name: string; count: number }>> {
  // Join against user_collection for a live count so after local rename/move
  // operations the folder pill counts stay accurate without waiting for sync.
  const r = await getPool().query(
    `SELECT f.folder_id, f.folder_name,
            COALESCE((SELECT COUNT(*)::int FROM user_collection uc
                      WHERE uc.clerk_user_id = f.clerk_user_id AND uc.folder_id = f.folder_id), 0) AS live_count
       FROM user_collection_folders f
      WHERE f.clerk_user_id = $1
      ORDER BY f.folder_name ASC`,
    [clerkUserId]
  );
  return r.rows.map(row => ({ folderId: row.folder_id, name: row.folder_name, count: row.live_count }));
}

export async function upsertWantlistItems(
  clerkUserId: string,
  items: Array<{ id: number; data: object; addedAt?: Date; rating?: number; notes?: any[] }>
): Promise<void> {
  if (!items.length) return;
  // Deduplicate by release ID within batch
  const deduped = new Map<number, typeof items[0]>();
  for (const item of items) deduped.set(item.id, item);
  const unique = [...deduped.values()];
  const ids:      number[]        = [];
  const dataArr:  string[]        = [];
  const addedArr: (Date | null)[] = [];
  const ratingArr:number[]        = [];
  const notesArr: (string|null)[] = [];
  for (const item of unique) {
    ids.push(item.id);
    dataArr.push(JSON.stringify(item.data));
    addedArr.push(item.addedAt ?? null);
    ratingArr.push(item.rating ?? 0);
    notesArr.push(item.notes ? JSON.stringify(item.notes) : null);
  }
  await getPool().query(
    `INSERT INTO user_wantlist (clerk_user_id, discogs_release_id, data, added_at, synced_at, rating, notes)
     SELECT $1, unnest($2::int[]), unnest($3::jsonb[]), unnest($4::timestamptz[]), NOW(), unnest($5::int[]), unnest($6::jsonb[])
     ON CONFLICT (clerk_user_id, discogs_release_id)
     DO UPDATE SET data = EXCLUDED.data, added_at = EXCLUDED.added_at, synced_at = NOW(),
                   rating = EXCLUDED.rating, notes = EXCLUDED.notes`,
    [clerkUserId, ids, dataArr, addedArr, ratingArr, notesArr]
  );
}

// ── Phase 2: Collection/Wantlist action helpers ──────────────────────────

export async function deleteCollectionItem(
  clerkUserId: string,
  releaseId: number,
  instanceId?: number
): Promise<void> {
  if (instanceId !== undefined && instanceId !== null) {
    await getPool().query(
      `DELETE FROM user_collection WHERE clerk_user_id = $1 AND discogs_release_id = $2 AND instance_id = $3`,
      [clerkUserId, releaseId, instanceId]
    );
  } else {
    // No instance_id given — remove all instances of this release (legacy callers)
    await getPool().query(
      `DELETE FROM user_collection WHERE clerk_user_id = $1 AND discogs_release_id = $2`,
      [clerkUserId, releaseId]
    );
  }
}

export async function deleteWantlistItem(clerkUserId: string, releaseId: number): Promise<void> {
  await getPool().query(`DELETE FROM user_wantlist WHERE clerk_user_id = $1 AND discogs_release_id = $2`, [clerkUserId, releaseId]);
}

/** Remove local wantlist items that no longer exist in Discogs after a full sync */
export async function pruneWantlistItems(clerkUserId: string, keepIds: number[]): Promise<number> {
  if (!keepIds.length) return 0;
  const r = await getPool().query(
    `DELETE FROM user_wantlist WHERE clerk_user_id = $1 AND discogs_release_id != ALL($2::int[]) RETURNING 1`,
    [clerkUserId, keepIds]
  );
  return r.rowCount ?? 0;
}

/** Remove local collection items (by instance_id) that no longer exist in Discogs after a full sync */
export async function pruneCollectionItems(clerkUserId: string, keepInstanceIds: number[]): Promise<number> {
  if (!keepInstanceIds.length) return 0;
  // Cast to bigint[] — instance_id is bigint as of the May 2026
  // migration above (Discogs IDs crossed int4 max). An int[] cast
  // would re-overflow here even with the column widened.
  const r = await getPool().query(
    `DELETE FROM user_collection WHERE clerk_user_id = $1 AND instance_id IS NOT NULL AND instance_id != ALL($2::bigint[]) RETURNING 1`,
    [clerkUserId, keepInstanceIds]
  );
  return r.rowCount ?? 0;
}

export async function updateCollectionRating(
  clerkUserId: string,
  releaseId: number,
  rating: number,
  instanceId?: number
): Promise<void> {
  if (instanceId !== undefined && instanceId !== null) {
    await getPool().query(
      `UPDATE user_collection SET rating = $4 WHERE clerk_user_id = $1 AND discogs_release_id = $2 AND instance_id = $3`,
      [clerkUserId, releaseId, instanceId, rating]
    );
  } else {
    await getPool().query(
      `UPDATE user_collection SET rating = $3 WHERE clerk_user_id = $1 AND discogs_release_id = $2`,
      [clerkUserId, releaseId, rating]
    );
  }
}

export async function updateCollectionFolder(
  clerkUserId: string,
  releaseId: number,
  folderId: number,
  instanceId?: number
): Promise<void> {
  if (instanceId !== undefined && instanceId !== null) {
    await getPool().query(
      `UPDATE user_collection SET folder_id = $4 WHERE clerk_user_id = $1 AND discogs_release_id = $2 AND instance_id = $3`,
      [clerkUserId, releaseId, instanceId, folderId]
    );
  } else {
    await getPool().query(
      `UPDATE user_collection SET folder_id = $3 WHERE clerk_user_id = $1 AND discogs_release_id = $2`,
      [clerkUserId, releaseId, folderId]
    );
  }
}

/** Return the first stored instance for a release (legacy single-instance helper). */
export async function getCollectionInstance(clerkUserId: string, releaseId: number): Promise<{ instanceId: number | null; folderId: number; rating: number; notes: any[] } | null> {
  const r = await getPool().query(
    `SELECT instance_id, folder_id, rating, notes FROM user_collection WHERE clerk_user_id = $1 AND discogs_release_id = $2 ORDER BY instance_id ASC LIMIT 1`,
    [clerkUserId, releaseId]
  );
  if (!r.rows[0]) return null;
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
export async function getCollectionInstances(
  clerkUserId: string,
  releaseId: number
): Promise<Array<{ instanceId: number | null; folderId: number; rating: number; notes: any[]; addedAt: Date | null }>> {
  const r = await getPool().query(
    `SELECT instance_id, folder_id, rating, notes, added_at
       FROM user_collection
      WHERE clerk_user_id = $1 AND discogs_release_id = $2
      ORDER BY added_at ASC NULLS LAST, instance_id ASC`,
    [clerkUserId, releaseId]
  );
  return r.rows.map(row => ({
    instanceId: row.instance_id != null && row.instance_id > 0 ? row.instance_id : null,
    folderId: row.folder_id ?? 0,
    rating: row.rating ?? 0,
    notes: row.notes ?? [],
    addedAt: row.added_at ?? null,
  }));
}

export async function updateCollectionNotes(
  clerkUserId: string,
  releaseId: number,
  notes: any[],
  instanceId?: number
): Promise<void> {
  if (instanceId !== undefined && instanceId !== null) {
    await getPool().query(
      `UPDATE user_collection SET notes = $4 WHERE clerk_user_id = $1 AND discogs_release_id = $2 AND instance_id = $3`,
      [clerkUserId, releaseId, instanceId, JSON.stringify(notes)]
    );
  } else {
    await getPool().query(
      `UPDATE user_collection SET notes = $3 WHERE clerk_user_id = $1 AND discogs_release_id = $2`,
      [clerkUserId, releaseId, JSON.stringify(notes)]
    );
  }
}

export async function updateWantlistNotes(
  clerkUserId: string,
  releaseId: number,
  notes: any[]
): Promise<void> {
  await getPool().query(
    `UPDATE user_wantlist SET notes = $3 WHERE clerk_user_id = $1 AND discogs_release_id = $2`,
    [clerkUserId, releaseId, JSON.stringify(notes)]
  );
}

export async function getWantlistItem(
  clerkUserId: string,
  releaseId: number
): Promise<{ rating: number; notes: any[] } | null> {
  const r = await getPool().query(
    `SELECT rating, notes FROM user_wantlist WHERE clerk_user_id = $1 AND discogs_release_id = $2`,
    [clerkUserId, releaseId]
  );
  if (!r.rows.length) return null;
  return { rating: r.rows[0].rating ?? 0, notes: r.rows[0].notes ?? [] };
}

// ── Recent views (cross-device Recent strip) ────────────────────────────

// Per-user cap for the Recent strip. Matches the _HISTORY_MAX constant in
// web/modal.js so the frontend and backend truncate at the same length.
// 576 = 12 pages of 48 cards under the load-more pager.
const RECENT_VIEWS_MAX = 576;

export async function upsertRecentView(
  clerkUserId: string,
  discogsId: number,
  entityType: string,
  data: object
): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO user_recent_views (clerk_user_id, discogs_id, entity_type, data, opened_at)
     VALUES ($1, $2, $3, $4, NOW())
     ON CONFLICT (clerk_user_id, discogs_id, entity_type)
     DO UPDATE SET data = EXCLUDED.data, opened_at = NOW()`,
    [clerkUserId, discogsId, entityType, JSON.stringify(data)]
  );
  // Trim to last RECENT_VIEWS_MAX rows for this user. Cheap — the index on
  // (clerk_user_id, opened_at DESC) makes the subquery a quick range scan.
  await pool.query(
    `DELETE FROM user_recent_views
     WHERE clerk_user_id = $1
       AND (discogs_id, entity_type) NOT IN (
         SELECT discogs_id, entity_type FROM user_recent_views
         WHERE clerk_user_id = $1
         ORDER BY opened_at DESC
         LIMIT ${RECENT_VIEWS_MAX}
       )`,
    [clerkUserId]
  );
}

export async function getRecentViews(
  clerkUserId: string,
  limit: number = RECENT_VIEWS_MAX
): Promise<Array<{ id: number; type: string; data: any; openedAt: string }>> {
  const capped = Math.min(Math.max(1, limit), RECENT_VIEWS_MAX);
  const r = await getPool().query(
    `SELECT discogs_id, entity_type, data, opened_at
     FROM user_recent_views
     WHERE clerk_user_id = $1
     ORDER BY opened_at DESC
     LIMIT $2`,
    [clerkUserId, capped]
  );
  return r.rows.map(row => ({
    id: row.discogs_id,
    type: row.entity_type,
    data: row.data ?? {},
    openedAt: row.opened_at,
  }));
}

export async function deleteRecentView(
  clerkUserId: string,
  discogsId: number,
  entityType?: string
): Promise<void> {
  if (entityType) {
    await getPool().query(
      `DELETE FROM user_recent_views WHERE clerk_user_id = $1 AND discogs_id = $2 AND entity_type = $3`,
      [clerkUserId, discogsId, entityType]
    );
  } else {
    await getPool().query(
      `DELETE FROM user_recent_views WHERE clerk_user_id = $1 AND discogs_id = $2`,
      [clerkUserId, discogsId]
    );
  }
}

export async function clearRecentViews(clerkUserId: string): Promise<void> {
  await getPool().query(
    `DELETE FROM user_recent_views WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
}

// ── LOC audio saves ──────────────────────────────────────────────────────

// Hard cap on how many LOC items a single user can keep saved. Protects
// the table from unbounded growth if someone mass-stars the entire
// National Jukebox. At 1000 items the total JSONB is still well under
// 10 MB per user.
const LOC_SAVES_MAX_PER_USER = 1000;

export async function saveLocItem(
  clerkUserId: string,
  locId: string,
  title: string | null,
  streamUrl: string | null,
  data: object
): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO user_loc_saves (clerk_user_id, loc_id, title, stream_url, data, saved_at)
     VALUES ($1, $2, $3, $4, $5, NOW())
     ON CONFLICT (clerk_user_id, loc_id)
     DO UPDATE SET title = EXCLUDED.title, stream_url = EXCLUDED.stream_url,
                   data = EXCLUDED.data, saved_at = NOW()`,
    [clerkUserId, locId, title, streamUrl, JSON.stringify(data)]
  );
  // Trim oldest rows if the user is above the cap. Uses the user+time
  // index so the subquery is a cheap range scan.
  await pool.query(
    `DELETE FROM user_loc_saves
     WHERE clerk_user_id = $1
       AND loc_id NOT IN (
         SELECT loc_id FROM user_loc_saves
         WHERE clerk_user_id = $1
         ORDER BY saved_at DESC
         LIMIT ${LOC_SAVES_MAX_PER_USER}
       )`,
    [clerkUserId]
  );
}

export async function getLocSaves(
  clerkUserId: string,
  limit: number = 500
): Promise<Array<{ locId: string; title: string | null; streamUrl: string | null; data: any; savedAt: string }>> {
  const capped = Math.min(Math.max(1, limit), 1000);
  const r = await getPool().query(
    `SELECT loc_id, title, stream_url, data, saved_at
     FROM user_loc_saves
     WHERE clerk_user_id = $1
     ORDER BY saved_at DESC
     LIMIT $2`,
    [clerkUserId, capped]
  );
  return r.rows.map(row => ({
    locId: row.loc_id,
    title: row.title,
    streamUrl: row.stream_url,
    data: row.data ?? {},
    savedAt: row.saved_at,
  }));
}

export async function deleteLocSave(clerkUserId: string, locId: string): Promise<void> {
  await getPool().query(
    `DELETE FROM user_loc_saves WHERE clerk_user_id = $1 AND loc_id = $2`,
    [clerkUserId, locId]
  );
}

export async function getLocSaveIds(clerkUserId: string): Promise<string[]> {
  const r = await getPool().query(
    `SELECT loc_id FROM user_loc_saves WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  return r.rows.map(row => row.loc_id);
}

// ── Archive.org item saves ────────────────────────────────────────────
// Same shape and cap as LOC saves; just a different table. Archive is
// admin-only on the wire — only admin users can hit the saves
// endpoints — but the DB layer doesn't enforce that (the API does).
const ARCHIVE_SAVES_MAX_PER_USER = 1000;

export async function saveArchiveItem(
  clerkUserId: string,
  archiveId: string,
  title: string | null,
  streamUrl: string | null,
  data: object
): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO user_archive_saves (clerk_user_id, archive_id, title, stream_url, data, saved_at)
     VALUES ($1, $2, $3, $4, $5, NOW())
     ON CONFLICT (clerk_user_id, archive_id)
     DO UPDATE SET title = EXCLUDED.title, stream_url = EXCLUDED.stream_url,
                   data = EXCLUDED.data, saved_at = NOW()`,
    [clerkUserId, archiveId, title, streamUrl, JSON.stringify(data)]
  );
  await pool.query(
    `DELETE FROM user_archive_saves
     WHERE clerk_user_id = $1
       AND archive_id NOT IN (
         SELECT archive_id FROM user_archive_saves
         WHERE clerk_user_id = $1
         ORDER BY saved_at DESC
         LIMIT ${ARCHIVE_SAVES_MAX_PER_USER}
       )`,
    [clerkUserId]
  );
}

export async function getArchiveSaves(
  clerkUserId: string,
  limit: number = 500
): Promise<Array<{ archiveId: string; title: string | null; streamUrl: string | null; data: any; savedAt: string }>> {
  const capped = Math.min(Math.max(1, limit), 1000);
  const r = await getPool().query(
    `SELECT archive_id, title, stream_url, data, saved_at
     FROM user_archive_saves
     WHERE clerk_user_id = $1
     ORDER BY saved_at DESC
     LIMIT $2`,
    [clerkUserId, capped]
  );
  return r.rows.map(row => ({
    archiveId: row.archive_id,
    title: row.title,
    streamUrl: row.stream_url,
    data: row.data ?? {},
    savedAt: row.saved_at,
  }));
}

export async function deleteArchiveSave(clerkUserId: string, archiveId: string): Promise<void> {
  await getPool().query(
    `DELETE FROM user_archive_saves WHERE clerk_user_id = $1 AND archive_id = $2`,
    [clerkUserId, archiveId]
  );
}

export async function getArchiveSaveIds(clerkUserId: string): Promise<string[]> {
  const r = await getPool().query(
    `SELECT archive_id FROM user_archive_saves WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  return r.rows.map(row => row.archive_id);
}

// ── YouTube video saves ───────────────────────────────────────────────
const YOUTUBE_SAVES_MAX_PER_USER = 1000;

export async function saveYoutubeVideo(
  clerkUserId: string,
  videoId: string,
  title: string | null,
  channel: string | null,
  thumbnail: string | null,
  data: object
): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO user_youtube_saves (clerk_user_id, video_id, title, channel, thumbnail, data, saved_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())
     ON CONFLICT (clerk_user_id, video_id)
     DO UPDATE SET title = EXCLUDED.title, channel = EXCLUDED.channel,
                   thumbnail = EXCLUDED.thumbnail, data = EXCLUDED.data, saved_at = NOW()`,
    [clerkUserId, videoId, title, channel, thumbnail, JSON.stringify(data)]
  );
  await pool.query(
    `DELETE FROM user_youtube_saves
     WHERE clerk_user_id = $1
       AND video_id NOT IN (
         SELECT video_id FROM user_youtube_saves
         WHERE clerk_user_id = $1
         ORDER BY saved_at DESC
         LIMIT ${YOUTUBE_SAVES_MAX_PER_USER}
       )`,
    [clerkUserId]
  );
}

export async function getYoutubeSaves(
  clerkUserId: string,
  limit: number = 500
): Promise<Array<{ videoId: string; title: string | null; channel: string | null; thumbnail: string | null; data: any; savedAt: string }>> {
  const capped = Math.min(Math.max(1, limit), 1000);
  const r = await getPool().query(
    `SELECT video_id, title, channel, thumbnail, data, saved_at
     FROM user_youtube_saves
     WHERE clerk_user_id = $1
     ORDER BY saved_at DESC
     LIMIT $2`,
    [clerkUserId, capped]
  );
  return r.rows.map(row => ({
    videoId:   row.video_id,
    title:     row.title,
    channel:   row.channel,
    thumbnail: row.thumbnail,
    data:      row.data ?? {},
    savedAt:   row.saved_at,
  }));
}

export async function deleteYoutubeSave(clerkUserId: string, videoId: string): Promise<void> {
  await getPool().query(
    `DELETE FROM user_youtube_saves WHERE clerk_user_id = $1 AND video_id = $2`,
    [clerkUserId, videoId]
  );
}

export async function getYoutubeSaveIds(clerkUserId: string): Promise<string[]> {
  const r = await getPool().query(
    `SELECT video_id FROM user_youtube_saves WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  return r.rows.map(row => row.video_id);
}

// ── Wikipedia article saves ───────────────────────────────────────────
// Mirrors the LOC saves API. The Wikipedia title is the natural primary
// key (canonical, stable). Cap matches LOC's so a user can't accumulate
// more than 1000 saved articles.
const WIKI_SAVES_MAX_PER_USER = 1000;

export async function saveWikiArticle(
  clerkUserId: string,
  title: string,
  url: string | null,
  snippet: string | null,
  thumbnail: string | null,
  data: object
): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO user_wiki_saves (clerk_user_id, wiki_title, wiki_url, snippet, thumbnail, data, saved_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())
     ON CONFLICT (clerk_user_id, wiki_title)
     DO UPDATE SET wiki_url = EXCLUDED.wiki_url, snippet = EXCLUDED.snippet,
                   thumbnail = EXCLUDED.thumbnail, data = EXCLUDED.data, saved_at = NOW()`,
    [clerkUserId, title, url, snippet, thumbnail, JSON.stringify(data)]
  );
  await pool.query(
    `DELETE FROM user_wiki_saves
     WHERE clerk_user_id = $1
       AND wiki_title NOT IN (
         SELECT wiki_title FROM user_wiki_saves
         WHERE clerk_user_id = $1
         ORDER BY saved_at DESC
         LIMIT ${WIKI_SAVES_MAX_PER_USER}
       )`,
    [clerkUserId]
  );
}

export async function getWikiSaves(
  clerkUserId: string,
  limit: number = 500
): Promise<Array<{ title: string; url: string | null; snippet: string | null; thumbnail: string | null; data: any; savedAt: string }>> {
  const capped = Math.min(Math.max(1, limit), 1000);
  const r = await getPool().query(
    `SELECT wiki_title, wiki_url, snippet, thumbnail, data, saved_at
     FROM user_wiki_saves
     WHERE clerk_user_id = $1
     ORDER BY saved_at DESC
     LIMIT $2`,
    [clerkUserId, capped]
  );
  return r.rows.map(row => ({
    title: row.wiki_title,
    url: row.wiki_url,
    snippet: row.snippet,
    thumbnail: row.thumbnail,
    data: row.data ?? {},
    savedAt: row.saved_at,
  }));
}

export async function deleteWikiSave(clerkUserId: string, title: string): Promise<void> {
  await getPool().query(
    `DELETE FROM user_wiki_saves WHERE clerk_user_id = $1 AND wiki_title = $2`,
    [clerkUserId, title]
  );
}

export async function getWikiSaveIds(clerkUserId: string): Promise<string[]> {
  const r = await getPool().query(
    `SELECT wiki_title FROM user_wiki_saves WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  return r.rows.map(row => row.wiki_title);
}

// ── Chronicling America (historic newspapers) saves ─────────────────────
// Mirrors the wiki saves API. chronam_id is the canonical relative path
// returned by the LOC API ("/lccn/<lccn>/<date>/ed-X/seq-N/"). Cap per
// user matches wiki/loc/archive saves at 1000.
const CHRONAM_SAVES_MAX_PER_USER = 1000;

export async function saveChronAmItem(
  clerkUserId: string,
  chronamId: string,
  paperTitle: string | null,
  issueDate: string | null,
  snippet: string | null,
  thumbnail: string | null,
  data: object
): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO user_chronam_saves (clerk_user_id, chronam_id, paper_title, issue_date, snippet, thumbnail, data, saved_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
     ON CONFLICT (clerk_user_id, chronam_id)
     DO UPDATE SET paper_title = EXCLUDED.paper_title, issue_date = EXCLUDED.issue_date,
                   snippet = EXCLUDED.snippet, thumbnail = EXCLUDED.thumbnail,
                   data = EXCLUDED.data, saved_at = NOW()`,
    [clerkUserId, chronamId, paperTitle, issueDate, snippet, thumbnail, JSON.stringify(data)]
  );
  await pool.query(
    `DELETE FROM user_chronam_saves
      WHERE clerk_user_id = $1
        AND chronam_id NOT IN (
          SELECT chronam_id FROM user_chronam_saves
           WHERE clerk_user_id = $1
           ORDER BY saved_at DESC
           LIMIT ${CHRONAM_SAVES_MAX_PER_USER}
        )`,
    [clerkUserId]
  );
}

export async function getChronAmSaves(
  clerkUserId: string,
  limit: number = 500,
): Promise<Array<{ id: string; paperTitle: string | null; issueDate: string | null; snippet: string | null; thumbnail: string | null; data: any; savedAt: string }>> {
  const capped = Math.min(Math.max(1, limit), 1000);
  const r = await getPool().query(
    `SELECT chronam_id, paper_title, issue_date, snippet, thumbnail, data, saved_at
       FROM user_chronam_saves
      WHERE clerk_user_id = $1
      ORDER BY saved_at DESC
      LIMIT $2`,
    [clerkUserId, capped]
  );
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

export async function deleteChronAmSave(clerkUserId: string, chronamId: string): Promise<void> {
  await getPool().query(
    `DELETE FROM user_chronam_saves WHERE clerk_user_id = $1 AND chronam_id = $2`,
    [clerkUserId, chronamId]
  );
}

export async function getChronAmSaveIds(clerkUserId: string): Promise<string[]> {
  const r = await getPool().query(
    `SELECT chronam_id FROM user_chronam_saves WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  return r.rows.map(row => row.chronam_id);
}

// Persistent search cache. Historic newspaper data effectively never
// changes, so a 30-day TTL is conservative — older rows are simply
// treated as expired and a fresh upstream call refreshes them.
const CHRONAM_SEARCH_CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000;

export async function getChronAmSearchCache(cacheKey: string): Promise<any | null> {
  try {
    const r = await getPool().query(
      `SELECT data, cached_at FROM chronam_search_cache WHERE cache_key = $1`,
      [cacheKey],
    );
    if (!r.rows.length) return null;
    const at = new Date(r.rows[0].cached_at).getTime();
    if (Date.now() - at > CHRONAM_SEARCH_CACHE_TTL_MS) return null;
    return r.rows[0].data;
  } catch { return null; }
}

// Stale fallback — returns ANY cached row for a key regardless of TTL.
// Used by the chronam search endpoint when the loc.gov upstream times
// out or errors: serving slightly-old historic-newspaper results is
// way better than returning a 504 to the user. Returns { data, cachedAt }
// so the endpoint can surface "served stale, last refreshed N ago".
export async function getChronAmSearchCacheStale(cacheKey: string):
  Promise<{ data: any; cachedAt: Date } | null> {
  try {
    const r = await getPool().query(
      `SELECT data, cached_at FROM chronam_search_cache WHERE cache_key = $1`,
      [cacheKey],
    );
    if (!r.rows.length) return null;
    return { data: r.rows[0].data, cachedAt: new Date(r.rows[0].cached_at) };
  } catch { return null; }
}

export async function setChronAmSearchCache(cacheKey: string, data: any): Promise<void> {
  try {
    await getPool().query(
      `INSERT INTO chronam_search_cache (cache_key, data, cached_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (cache_key)
       DO UPDATE SET data = EXCLUDED.data, cached_at = NOW()`,
      [cacheKey, JSON.stringify(data)],
    );
  } catch { /* best-effort cache write */ }
}

// ── Play queue (cross-source: LOC + YouTube) ─────────────────────────
const PLAY_QUEUE_MAX = 500;

export type QueueItem = {
  source: "loc" | "yt";
  externalId: string;            // loc URL OR YouTube videoId
  data: any;                     // title, artist, image, streamUrl/streamType OR durationSec etc.
};

export async function getPlayQueue(clerkUserId: string): Promise<Array<QueueItem & { position: number }>> {
  const r = await getPool().query(
    `SELECT position, source, external_id, data
     FROM user_play_queue
     WHERE clerk_user_id = $1
     ORDER BY position ASC`,
    [clerkUserId]
  );
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
export async function appendPlayQueue(
  clerkUserId: string,
  items: QueueItem[],
  opts?: { mode?: "next" | "append" },
): Promise<{ added: number; firstPosition: number }> {
  if (!items.length) return { added: 0, firstPosition: 0 };
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
      await client.query(
        `DELETE FROM user_play_queue WHERE clerk_user_id = $1 AND external_id = ANY($2::text[])`,
        [clerkUserId, incomingExtIds]
      );
    }
    let startPos: number;
    if (mode === "append") {
      const r = await client.query(
        `SELECT COALESCE(MAX(position), 0) AS maxp FROM user_play_queue WHERE clerk_user_id = $1`,
        [clerkUserId]
      );
      startPos = (r.rows[0]?.maxp ?? 0) + 1;
    } else {
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
      await client.query(
        `UPDATE user_play_queue SET position = -position - 1 WHERE clerk_user_id = $1`,
        [clerkUserId]
      );
      await client.query(
        `UPDATE user_play_queue SET position = -position - 1 + $2 WHERE clerk_user_id = $1`,
        [clerkUserId, items.length]
      );
      startPos = 1;
    }
    const values: any[] = [];
    const placeholders: string[] = [];
    items.forEach((it, i) => {
      const p = startPos + i;
      placeholders.push(`($1, $${values.length + 2}, $${values.length + 3}, $${values.length + 4}, $${values.length + 5})`);
      values.push(p, it.source, it.externalId, JSON.stringify(it.data ?? {}));
    });
    await client.query(
      `INSERT INTO user_play_queue (clerk_user_id, position, source, external_id, data) VALUES ${placeholders.join(", ")}
       ON CONFLICT (clerk_user_id, position) DO UPDATE SET source = EXCLUDED.source, external_id = EXCLUDED.external_id, data = EXCLUDED.data, added_at = NOW()`,
      [clerkUserId, ...values]
    );
    // Trim the cap (drops oldest tail entries beyond PLAY_QUEUE_MAX).
    await client.query(
      `DELETE FROM user_play_queue
       WHERE clerk_user_id = $1
         AND position NOT IN (
           SELECT position FROM user_play_queue
           WHERE clerk_user_id = $1
           ORDER BY position ASC
           LIMIT ${PLAY_QUEUE_MAX}
         )`,
      [clerkUserId]
    );
    await client.query("COMMIT");
    return { added: items.length, firstPosition: startPos };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function removeFromPlayQueue(clerkUserId: string, position: number): Promise<void> {
  await getPool().query(
    `DELETE FROM user_play_queue WHERE clerk_user_id = $1 AND position = $2`,
    [clerkUserId, position]
  );
}

export async function clearPlayQueue(clerkUserId: string): Promise<number> {
  const r = await getPool().query(
    `DELETE FROM user_play_queue WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  return r.rowCount ?? 0;
}

// Reorder by sending the full ordered list of positions. Server rewrites
// positions in a transaction so concurrent updates stay consistent.
export async function reorderPlayQueue(clerkUserId: string, orderedPositions: number[]): Promise<void> {
  if (!orderedPositions.length) return;
  const pool = getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Same per-user serialization as appendPlayQueue (see note there) —
    // prevents reorder/add deadlocks that bubbled up as a flaky
    // "reorder failed" + snap-back.
    await client.query(`SELECT pg_advisory_xact_lock(hashtext($1), 424242)`, [clerkUserId]);
    const r = await client.query(
      `SELECT position, source, external_id, data FROM user_play_queue WHERE clerk_user_id = $1`,
      [clerkUserId]
    );
    const byPos = new Map<number, any>();
    r.rows.forEach(row => byPos.set(row.position, row));
    await client.query(`DELETE FROM user_play_queue WHERE clerk_user_id = $1`, [clerkUserId]);
    let newPos = 1;
    for (const oldPos of orderedPositions) {
      const row = byPos.get(oldPos);
      if (!row) continue;
      await client.query(
        `INSERT INTO user_play_queue (clerk_user_id, position, source, external_id, data) VALUES ($1, $2, $3, $4, $5)`,
        [clerkUserId, newPos, row.source, row.external_id, JSON.stringify(row.data ?? {})]
      );
      newPos++;
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

// ── User playlists ─────────────────────────────────────────────────────

const PLAYLIST_NAME_MAX  = 80;
const PLAYLIST_ITEMS_MAX = 500;       // same cap as the live queue
const PLAYLISTS_PER_USER = 100;       // soft cap per user

// Create a playlist owned by clerkUserId from a snapshot of items.
// Caller passes the items in display order. Returns the new id.
export async function createPlaylist(
  clerkUserId: string,
  name: string,
  items: QueueItem[],
): Promise<number> {
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
    const countRow = await client.query(
      `SELECT COUNT(*)::int AS n FROM user_playlists WHERE clerk_user_id = $1`,
      [clerkUserId],
    );
    if ((countRow.rows[0]?.n ?? 0) >= PLAYLISTS_PER_USER) {
      throw new Error(`Playlist limit reached (${PLAYLISTS_PER_USER}). Delete one before saving another.`);
    }
    const r = await client.query(
      `INSERT INTO user_playlists (clerk_user_id, name) VALUES ($1, $2) RETURNING id`,
      [clerkUserId, trimmedName],
    );
    const id = r.rows[0].id as number;
    if (capped.length) {
      const values: any[] = [];
      const placeholders: string[] = [];
      capped.forEach((it, i) => {
        const base = values.length;
        placeholders.push(`($1, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
        values.push(i + 1, it.source, it.externalId, JSON.stringify(it.data ?? {}));
      });
      await client.query(
        `INSERT INTO user_playlist_items (playlist_id, position, source, external_id, data) VALUES ${placeholders.join(", ")}`,
        [id, ...values],
      );
    }
    await client.query("COMMIT");
    return id;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

// List one user's playlists — name + counts, no item bodies (the
// drawer picker just needs labels).
export async function listPlaylists(clerkUserId: string): Promise<Array<{
  id: number; name: string; created_at: string; updated_at: string; item_count: number;
}>> {
  const r = await getPool().query(
    `SELECT p.id, p.name, p.created_at, p.updated_at,
            COALESCE(c.n, 0) AS item_count
       FROM user_playlists p
       LEFT JOIN (
         SELECT playlist_id, COUNT(*)::int AS n
           FROM user_playlist_items
          GROUP BY playlist_id
       ) c ON c.playlist_id = p.id
      WHERE p.clerk_user_id = $1
      ORDER BY p.updated_at DESC`,
    [clerkUserId],
  );
  return r.rows;
}

// Public: fetch a playlist by id including all items in order. Returns
// null if not found. Owner clerk_user_id is included so the caller can
// gate edits — read-side gating is owner-agnostic by design (shareable).
export async function getPlaylist(id: number): Promise<{
  id: number; name: string; clerk_user_id: string; created_at: string; updated_at: string;
  items: Array<QueueItem & { position: number }>;
} | null> {
  const head = await getPool().query(
    `SELECT id, name, clerk_user_id, created_at, updated_at
       FROM user_playlists WHERE id = $1`,
    [id],
  );
  if (!head.rows.length) return null;
  const itemsRows = await getPool().query(
    `SELECT position, source, external_id, data
       FROM user_playlist_items
      WHERE playlist_id = $1
      ORDER BY position ASC`,
    [id],
  );
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
export async function renamePlaylist(
  id: number, clerkUserId: string, name: string,
): Promise<boolean> {
  const trimmed = (name || "").trim().slice(0, PLAYLIST_NAME_MAX);
  if (!trimmed) return false;
  const r = await getPool().query(
    `UPDATE user_playlists SET name = $3, updated_at = NOW()
      WHERE id = $1 AND clerk_user_id = $2`,
    [id, clerkUserId, trimmed],
  );
  return (r.rowCount ?? 0) > 0;
}

// Owner-only delete. ON DELETE CASCADE on the items table sweeps
// the children automatically.
export async function deletePlaylist(id: number, clerkUserId: string): Promise<boolean> {
  const r = await getPool().query(
    `DELETE FROM user_playlists WHERE id = $1 AND clerk_user_id = $2`,
    [id, clerkUserId],
  );
  return (r.rowCount ?? 0) > 0;
}

// Owner-only items replace. Used by the "overwrite this playlist"
// flow on save: keep the same id (and share-URL), wipe items, write
// the new ones, bump updated_at. Optionally renames the playlist
// in the same transaction. Returns false if the playlist isn't
// the caller's.
export async function replacePlaylistItems(
  id: number,
  clerkUserId: string,
  items: QueueItem[],
  newName?: string,
): Promise<boolean> {
  const capped = items.slice(0, PLAYLIST_ITEMS_MAX);
  const pool = getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Owner check + optional rename. UPDATE with RETURNING gives us
    // both ownership confirmation AND the row id in one round-trip.
    const ownerCheck = newName
      ? await client.query(
          `UPDATE user_playlists SET name = $3, updated_at = NOW()
            WHERE id = $1 AND clerk_user_id = $2
            RETURNING id`,
          [id, clerkUserId, newName.trim().slice(0, PLAYLIST_NAME_MAX) || "Untitled playlist"],
        )
      : await client.query(
          `UPDATE user_playlists SET updated_at = NOW()
            WHERE id = $1 AND clerk_user_id = $2
            RETURNING id`,
          [id, clerkUserId],
        );
    if (!ownerCheck.rows.length) {
      await client.query("ROLLBACK");
      return false;
    }
    await client.query(`DELETE FROM user_playlist_items WHERE playlist_id = $1`, [id]);
    if (capped.length) {
      const values: any[] = [];
      const placeholders: string[] = [];
      capped.forEach((it, i) => {
        const base = values.length;
        placeholders.push(`($1, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
        values.push(i + 1, it.source, it.externalId, JSON.stringify(it.data ?? {}));
      });
      await client.query(
        `INSERT INTO user_playlist_items (playlist_id, position, source, external_id, data) VALUES ${placeholders.join(", ")}`,
        [id, ...values],
      );
    }
    await client.query("COMMIT");
    return true;
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

// ── Phase 4: Price intelligence DB functions ─────────────────────────────

export async function upsertPriceCache(releaseId: number, lowest: number | null, median: number | null, highest: number | null, numForSale: number, currency: string = "USD"): Promise<void> {
  await getPool().query(
    `INSERT INTO price_cache (discogs_release_id, lowest_price, median_price, highest_price, num_for_sale, currency, fetched_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())
     ON CONFLICT (discogs_release_id, currency)
     DO UPDATE SET lowest_price = $2, median_price = $3, highest_price = $4, num_for_sale = $5, fetched_at = NOW()`,
    [releaseId, lowest, median, highest, numForSale, currency]
  );
}

export async function appendPriceHistory(releaseId: number, lowest: number | null, median: number | null, highest: number | null, numForSale: number, currency: string = "USD"): Promise<void> {
  // Only one entry per release per day
  const existing = await getPool().query(
    `SELECT id FROM price_history WHERE discogs_release_id = $1 AND currency = $2 AND recorded_at > NOW() - INTERVAL '20 hours'`,
    [releaseId, currency]
  );
  if (existing.rows.length > 0) return;
  await getPool().query(
    `INSERT INTO price_history (discogs_release_id, lowest_price, median_price, highest_price, num_for_sale, currency)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [releaseId, lowest, median, highest, numForSale, currency]
  );
}

export async function getPriceCache(releaseId: number, currency: string = "USD"): Promise<{ lowest: number | null; median: number | null; highest: number | null; numForSale: number; fetchedAt: Date | null } | null> {
  const r = await getPool().query(
    `SELECT lowest_price, median_price, highest_price, num_for_sale, fetched_at FROM price_cache WHERE discogs_release_id = $1 AND currency = $2`,
    [releaseId, currency]
  );
  if (!r.rows[0]) return null;
  return { lowest: r.rows[0].lowest_price, median: r.rows[0].median_price, highest: r.rows[0].highest_price, numForSale: r.rows[0].num_for_sale, fetchedAt: r.rows[0].fetched_at };
}

export async function getPriceHistory(releaseId: number, currency: string = "USD", days: number = 90): Promise<Array<{ median: number; lowest: number; highest: number; recordedAt: Date }>> {
  const r = await getPool().query(
    `SELECT median_price, lowest_price, highest_price, recorded_at FROM price_history
     WHERE discogs_release_id = $1 AND currency = $2 AND recorded_at > NOW() - make_interval(days => $3)
     ORDER BY recorded_at ASC`,
    [releaseId, currency, days]
  );
  return r.rows.map(row => ({ median: row.median_price, lowest: row.lowest_price, highest: row.highest_price, recordedAt: row.recorded_at }));
}

export async function getStaleReleaseIds(limit: number = 100): Promise<number[]> {
  // Get unique release IDs from all collections where price is stale (>24h) or missing
  const r = await getPool().query(
    `SELECT uc.discogs_release_id, MIN(pc.fetched_at) AS oldest
     FROM user_collection uc
     LEFT JOIN price_cache pc ON pc.discogs_release_id = uc.discogs_release_id
     WHERE pc.fetched_at IS NULL OR pc.fetched_at < NOW() - INTERVAL '24 hours'
     GROUP BY uc.discogs_release_id
     ORDER BY oldest ASC NULLS FIRST
     LIMIT $1`,
    [limit]
  );
  return r.rows.map(row => row.discogs_release_id);
}


export async function prunePriceHistory(): Promise<void> {
  // Keep max 1 year of history
  await getPool().query(`DELETE FROM price_history WHERE recorded_at < NOW() - INTERVAL '365 days'`);
}

export interface CwSearchFilters {
  q?: string;
  artist?: string;
  release?: string;
  label?: string;
  year?: string;
  genre?: string;
  style?: string;
  format?: string;
  type?: string;       // "master" = has master_id, "release" = no master_id
  folderId?: number;
  ratingMin?: number;  // 1-5 for "N stars+", 0 for unrated only
  ratingUnrated?: boolean; // true = show only unrated
  notes?: string;      // text search across notes JSONB
  sort?: string;
  synonyms?: boolean;  // expand classical music synonyms (default true)
}

function cwOrderBy(sort?: string): string {
  switch (sort) {
    case "title":   return `ORDER BY LOWER(data->>'title') ASC, LOWER(data->'artists'->0->>'name') ASC`;
    case "year":    return `ORDER BY (data->>'year') DESC NULLS LAST, LOWER(data->'artists'->0->>'name') ASC`;
    case "year_asc":return `ORDER BY (data->>'year') ASC NULLS LAST, LOWER(data->'artists'->0->>'name') ASC`;
    case "added":     return `ORDER BY added_at DESC NULLS LAST, id DESC`;
    case "added_asc": return `ORDER BY added_at ASC NULLS LAST, id ASC`;
    case "rating":  return `ORDER BY rating DESC NULLS LAST, LOWER(data->'artists'->0->>'name') ASC`;
    default:        return `ORDER BY LOWER(data->'artists'->0->>'name') ASC, LOWER(data->>'title') ASC`;
  }
}

// Parse a filter value with operators:  + (AND),  | (OR),  - prefix (NOT)
// e.g. "miles davis + john coltrane" → both must match
// e.g. "-verve" → must NOT match verve
// e.g. "miles davis | john coltrane" → either matches
// e.g. "blue note + -verve | columbia" → (blue note AND NOT verve) OR columbia
function parseFilterExpr(value: string, column: string, startIdx: number): { clause: string; params: any[]; nextIdx: number } {
  const orBranches = value.split(/\s*\|\s*/);
  const orClauses: string[] = [];
  const params: any[] = [];
  let idx = startIdx;

  for (const branch of orBranches) {
    const terms = branch.split(/\s*\+\s*/);
    const andClauses: string[] = [];
    for (let term of terms) {
      term = term.trim();
      if (!term) continue;
      if (term.startsWith("-") && term.length > 1) {
        // NOT: exclude this term
        andClauses.push(`${column} NOT ILIKE $${idx}`);
        params.push(`%${term.slice(1).trim()}%`);
      } else {
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
function parseFilterExprWithSynonyms(
  value: string, column: string, startIdx: number
): { clause: string; params: any[]; nextIdx: number; synonymsApplied: string[] } {
  const orBranches = value.split(/\s*\|\s*/);
  const orClauses: string[] = [];
  const params: any[] = [];
  const synonymsApplied: string[] = [];
  let idx = startIdx;

  for (const branch of orBranches) {
    const terms = branch.split(/\s*\+\s*/);
    const andClauses: string[] = [];
    for (let term of terms) {
      term = term.trim();
      if (!term) continue;
      if (term.startsWith("-") && term.length > 1) {
        // NOT: exclude this term — do NOT expand synonyms for negations
        andClauses.push(`${column} NOT ILIKE $${idx}`);
        params.push(`%${term.slice(1).trim()}%`);
        idx++;
      } else {
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
        if (applied.length) synonymsApplied.push(...applied);
        andClauses.push(
          likeClauses.length === 1 ? likeClauses[0] : `(${likeClauses.join(" OR ")})`
        );
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

function buildCwWhere(filters: CwSearchFilters, startIdx: number): { clause: string; params: any[]; synonymsApplied: string[] } {
  const clauses: string[] = [];
  const allParams: any[] = [];
  const allSynonyms: string[] = [];
  let idx = startIdx;
  const useSynonyms = filters.synonyms !== false;

  // Fields eligible for synonym expansion (q and release/title)
  const synonymFields = new Set(["data::text", "data->>'title'"]);

  const fields: Array<[string | undefined, string]> = [
    [filters.q,       "data::text"],
    [filters.artist,  "(data->'artists')::text"],
    [filters.release, "data->>'title'"],
    [filters.label,   "(data->'labels')::text"],
    [filters.year,    "(data->>'year')::text"],
    [filters.genre,   "(data->'genres')::text"],
    [filters.style,   "(data->'styles')::text"],
    [filters.format,  "(data->'formats')::text"],
  ];

  for (const [value, column] of fields) {
    if (!value) continue;
    if (useSynonyms && synonymFields.has(column)) {
      const { clause, params, nextIdx, synonymsApplied } = parseFilterExprWithSynonyms(value, column, idx);
      if (clause) {
        clauses.push(clause);
        allParams.push(...params);
        allSynonyms.push(...synonymsApplied);
        idx = nextIdx;
      }
    } else {
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
  } else if (filters.ratingMin && filters.ratingMin >= 1 && filters.ratingMin <= 5) {
    clauses.push(`rating >= $${idx}`);
    allParams.push(filters.ratingMin);
    idx++;
  }

  // Type filter: "master" = has master_id, "release" = no master_id (standalone release)
  if (filters.type === "master") {
    clauses.push(`(data->>'master_id') IS NOT NULL AND (data->>'master_id')::int > 0`);
  } else if (filters.type === "release") {
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

export async function getCollectionPage(
  clerkUserId: string,
  page: number,
  perPage: number,
  filters?: CwSearchFilters
): Promise<{ items: any[]; total: number; synonymsApplied?: string[] }> {
  const offset = (page - 1) * perPage;
  const { clause: dataClause, params: dataFilterParams, synonymsApplied } = buildCwWhere(filters ?? {}, 4);
  const { clause: countClause, params: countFilterParams } = buildCwWhere(filters ?? {}, 2);
  const orderBy = cwOrderBy(filters?.sort);
  // A release may have multiple instances (multiple copies owned). Collapse to one
  // card per release in the grid via DISTINCT ON, picking the highest-rated/most-
  // recently-added instance as representative. Also surface a per-release instance
  // count so the UI can show "×N copies" badges.
  const [dataR, countR] = await Promise.all([
    getPool().query(
      `SELECT data, rating, notes, instance_count FROM (
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
       LIMIT $2 OFFSET $3`,
      [clerkUserId, perPage, offset, ...dataFilterParams]
    ),
    getPool().query(
      `SELECT COUNT(DISTINCT discogs_release_id)::int AS total FROM user_collection WHERE clerk_user_id = $1${countClause}`,
      [clerkUserId, ...countFilterParams]
    ),
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

export async function getAllCollectionItems(clerkUserId: string): Promise<any[]> {
  const r = await getPool().query(
    `SELECT data, folder_id FROM user_collection WHERE clerk_user_id = $1
     ORDER BY LOWER(data->'artists'->0->>'name') ASC, LOWER(data->>'title') ASC`,
    [clerkUserId]
  );
  return r.rows;
}

export async function getAllWantlistItems(clerkUserId: string): Promise<any[]> {
  const r = await getPool().query(
    `SELECT data FROM user_wantlist WHERE clerk_user_id = $1
     ORDER BY LOWER(data->'artists'->0->>'name') ASC, LOWER(data->>'title') ASC`,
    [clerkUserId]
  );
  return r.rows;
}

export async function getWantlistPage(
  clerkUserId: string,
  page: number,
  perPage: number,
  filters?: CwSearchFilters
): Promise<{ items: any[]; total: number; synonymsApplied?: string[] }> {
  const offset = (page - 1) * perPage;
  const { clause: dataClause, params: dataFilterParams, synonymsApplied } = buildCwWhere(filters ?? {}, 4);
  const { clause: countClause, params: countFilterParams } = buildCwWhere(filters ?? {}, 2);
  const orderBy = cwOrderBy(filters?.sort);
  const [dataR, countR] = await Promise.all([
    getPool().query(
      `SELECT data, rating, notes FROM user_wantlist WHERE clerk_user_id = $1${dataClause}
       ${orderBy}
       LIMIT $2 OFFSET $3`,
      [clerkUserId, perPage, offset, ...dataFilterParams]
    ),
    getPool().query(
      `SELECT COUNT(*)::int AS total FROM user_wantlist WHERE clerk_user_id = $1${countClause}`,
      [clerkUserId, ...countFilterParams]
    ),
  ]);
  return {
    items: dataR.rows.map(r => ({ ...r.data, _rating: r.rating ?? 0, _notes: r.notes ?? [] })),
    total: countR.rows[0]?.total ?? 0,
    synonymsApplied: synonymsApplied.length ? synonymsApplied : undefined,
  };
}

export async function getCollectionFacets(clerkUserId: string, genre?: string): Promise<{ genres: string[]; styles: string[] }> {
  const stylesQuery = genre
    ? `SELECT DISTINCT s AS name FROM user_collection, jsonb_array_elements_text(data->'styles') AS s WHERE clerk_user_id = $1 AND (data->'genres')::text ILIKE $2 ORDER BY s`
    : `SELECT DISTINCT s AS name FROM user_collection, jsonb_array_elements_text(data->'styles') AS s WHERE clerk_user_id = $1 ORDER BY s`;
  const stylesParams = genre ? [clerkUserId, `%${genre}%`] : [clerkUserId];
  const [genresR, stylesR] = await Promise.all([
    getPool().query(
      `SELECT DISTINCT g AS name FROM user_collection, jsonb_array_elements_text(data->'genres') AS g WHERE clerk_user_id = $1 ORDER BY g`,
      [clerkUserId]
    ),
    getPool().query(stylesQuery, stylesParams),
  ]);
  return { genres: genresR.rows.map(r => r.name), styles: stylesR.rows.map(r => r.name) };
}

export async function getWantlistFacets(clerkUserId: string, genre?: string): Promise<{ genres: string[]; styles: string[] }> {
  const stylesQuery = genre
    ? `SELECT DISTINCT s AS name FROM user_wantlist, jsonb_array_elements_text(data->'styles') AS s WHERE clerk_user_id = $1 AND (data->'genres')::text ILIKE $2 ORDER BY s`
    : `SELECT DISTINCT s AS name FROM user_wantlist, jsonb_array_elements_text(data->'styles') AS s WHERE clerk_user_id = $1 ORDER BY s`;
  const stylesParams = genre ? [clerkUserId, `%${genre}%`] : [clerkUserId];
  const [genresR, stylesR] = await Promise.all([
    getPool().query(
      `SELECT DISTINCT g AS name FROM user_wantlist, jsonb_array_elements_text(data->'genres') AS g WHERE clerk_user_id = $1 ORDER BY g`,
      [clerkUserId]
    ),
    getPool().query(stylesQuery, stylesParams),
  ]);
  return { genres: genresR.rows.map(r => r.name), styles: stylesR.rows.map(r => r.name) };
}

export async function getCollectionIds(clerkUserId: string): Promise<number[]> {
  const r = await getPool().query(
    "SELECT DISTINCT discogs_release_id FROM user_collection WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return r.rows.map(row => row.discogs_release_id);
}

/**
 * Returns a map of discogs_release_id → instance_count for releases the user owns
 * more than one copy of. Used to render the "(N)" badge on card thumbnails.
 */
export async function getCollectionMultiInstanceCounts(
  clerkUserId: string
): Promise<Record<number, number>> {
  const r = await getPool().query(
    `SELECT discogs_release_id, COUNT(*)::int AS n
       FROM user_collection
      WHERE clerk_user_id = $1
      GROUP BY discogs_release_id
     HAVING COUNT(*) > 1`,
    [clerkUserId]
  );
  const out: Record<number, number> = {};
  for (const row of r.rows) out[row.discogs_release_id] = row.n;
  return out;
}

export async function getDefaultAddFolderId(clerkUserId: string): Promise<number> {
  const r = await getPool().query(
    `SELECT default_add_folder_id FROM user_tokens WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  const v = r.rows[0]?.default_add_folder_id;
  return Number.isFinite(Number(v)) && Number(v) > 0 ? Number(v) : 1;
}

export async function setDefaultAddFolderId(clerkUserId: string, folderId: number): Promise<void> {
  const fid = Number(folderId);
  if (!Number.isFinite(fid) || fid < 1) throw new Error("Invalid folder id");
  await getPool().query(
    `UPDATE user_tokens SET default_add_folder_id = $2 WHERE clerk_user_id = $1`,
    [clerkUserId, fid]
  );
}

export async function getWantedSample(limit: number = 24, excludeIds: number[] = []): Promise<object[]> {
  // Distribute evenly across users: each user contributes at most ceil(limit/userCount) items
  const uc = await getPool().query(`SELECT COUNT(DISTINCT clerk_user_id)::int AS n FROM user_wantlist`);
  const userCount = Math.max(uc.rows[0]?.n ?? 1, 1);
  const perUser = Math.ceil(limit / userCount);
  const excludeClause = excludeIds.length
    ? `AND discogs_release_id != ALL($3)`
    : "";
  const params: any[] = [perUser, limit];
  if (excludeIds.length) params.push(excludeIds);
  const r = await getPool().query(
    `WITH ranked AS (
       SELECT data, discogs_release_id,
              ROW_NUMBER() OVER (PARTITION BY clerk_user_id ORDER BY RANDOM()) AS rn
       FROM user_wantlist
       WHERE 1=1 ${excludeClause}
     )
     SELECT data FROM ranked
     WHERE rn <= $1
     ORDER BY RANDOM()
     LIMIT $2`,
    params
  );
  return r.rows.map(row => row.data);
}

export async function getWantedItems(): Promise<object[]> {
  const r = await getPool().query(
    `SELECT clerk_user_id, discogs_release_id, data FROM user_wantlist`
  );

  // Group by user
  const byUser = new Map<string, Array<{ id: number; data: object }>>();
  for (const row of r.rows) {
    if (!byUser.has(row.clerk_user_id)) byUser.set(row.clerk_user_id, []);
    byUser.get(row.clerk_user_id)!.push({ id: row.discogs_release_id, data: row.data });
  }

  // Fisher-Yates shuffle helper
  const shuffle = <T>(arr: T[]): T[] => {
    for (let i = arr.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
  };

  // Shuffle each user's list independently
  const userLists = Array.from(byUser.values()).map(items => shuffle(items));

  // Round-robin interleave, deduping by release_id
  const seen = new Set<number>();
  const result: object[] = [];
  const maxLen = Math.max(...userLists.map(l => l.length));
  outer: for (let i = 0; i < maxLen; i++) {
    for (const list of userLists) {
      if (i < list.length && !seen.has(list[i].id)) {
        seen.add(list[i].id);
        result.push(list[i].data);
        if (result.length >= 500) break outer;
      }
    }
  }
  return result;
}

export async function getWantlistIds(clerkUserId: string): Promise<number[]> {
  const r = await getPool().query(
    "SELECT discogs_release_id FROM user_wantlist WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return r.rows.map(row => row.discogs_release_id);
}

/**
 * Returns master_id → distinct release count for the user's collection.
 * Used to render the "N" inside C/W dot indicators on master-type
 * search cards so the user can see at a glance "you already own 2
 * pressings of this master". Releases without a master_id (orphans)
 * are skipped. Multiple instances of the same release count as 1.
 */
export async function getCollectionMasterCounts(
  clerkUserId: string
): Promise<Record<number, number>> {
  const r = await getPool().query(
    `SELECT (data->>'master_id')::int AS master_id,
            COUNT(DISTINCT discogs_release_id)::int AS n
       FROM user_collection
      WHERE clerk_user_id = $1
        AND data ? 'master_id'
        AND data->>'master_id' IS NOT NULL
        AND data->>'master_id' <> '0'
        AND data->>'master_id' <> ''
      GROUP BY (data->>'master_id')::int`,
    [clerkUserId]
  );
  const out: Record<number, number> = {};
  for (const row of r.rows) {
    if (row.master_id) out[row.master_id] = row.n;
  }
  return out;
}

/** Same as getCollectionMasterCounts but for the wantlist. */
export async function getWantlistMasterCounts(
  clerkUserId: string
): Promise<Record<number, number>> {
  const r = await getPool().query(
    `SELECT (data->>'master_id')::int AS master_id,
            COUNT(DISTINCT discogs_release_id)::int AS n
       FROM user_wantlist
      WHERE clerk_user_id = $1
        AND data ? 'master_id'
        AND data->>'master_id' IS NOT NULL
        AND data->>'master_id' <> '0'
        AND data->>'master_id' <> ''
      GROUP BY (data->>'master_id')::int`,
    [clerkUserId]
  );
  const out: Record<number, number> = {};
  for (const row of r.rows) {
    if (row.master_id) out[row.master_id] = row.n;
  }
  return out;
}

export async function updateCollectionSyncedAt(clerkUserId: string): Promise<void> {
  await getPool().query(
    "UPDATE user_tokens SET collection_synced_at = NOW() WHERE clerk_user_id = $1",
    [clerkUserId]
  );
}

export async function updateWantlistSyncedAt(clerkUserId: string): Promise<void> {
  await getPool().query(
    "UPDATE user_tokens SET wantlist_synced_at = NOW() WHERE clerk_user_id = $1",
    [clerkUserId]
  );
}

// ── Inventory (marketplace listings) ──────────────────────────────────────

export async function upsertInventoryItems(
  clerkUserId: string,
  items: Array<{ listingId: number; releaseId?: number; data: object; status?: string; priceValue?: number; priceCurrency?: string; condition?: string; sleeveCondition?: string; postedAt?: Date }>
): Promise<void> {
  if (!items.length) return;
  // Dedupe by listingId within the batch (keep last occurrence)
  const deduped = [...new Map(items.map(i => [i.listingId, i])).values()];
  const CHUNK = 50;
  for (let i = 0; i < deduped.length; i += CHUNK) {
    const chunk = deduped.slice(i, i + CHUNK);
    const values: string[] = [];
    const params: any[] = [];
    let idx = 1;
    for (const item of chunk) {
      values.push(`($${idx}, $${idx+1}, $${idx+2}, $${idx+3}, $${idx+4}, $${idx+5}, $${idx+6}, $${idx+7}, $${idx+8}, $${idx+9}, NOW())`);
      params.push(clerkUserId, item.listingId, item.releaseId ?? null, JSON.stringify(item.data), item.status ?? "For Sale", item.priceValue ?? null, item.priceCurrency ?? "USD", item.condition ?? null, item.sleeveCondition ?? null, item.postedAt ?? null);
      idx += 10;
    }
    await getPool().query(
      `INSERT INTO user_inventory (clerk_user_id, listing_id, discogs_release_id, data, status, price_value, price_currency, condition, sleeve_condition, posted_at, synced_at)
       VALUES ${values.join(", ")}
       ON CONFLICT (clerk_user_id, listing_id)
       DO UPDATE SET data = EXCLUDED.data, status = EXCLUDED.status, price_value = EXCLUDED.price_value, price_currency = EXCLUDED.price_currency, condition = EXCLUDED.condition, sleeve_condition = EXCLUDED.sleeve_condition, posted_at = EXCLUDED.posted_at, synced_at = NOW()`,
      params
    );
  }
}

export async function updateInventorySyncedAt(clerkUserId: string): Promise<void> {
  await getPool().query(
    "UPDATE user_tokens SET inventory_synced_at = NOW() WHERE clerk_user_id = $1",
    [clerkUserId]
  );
}

export async function getInventoryCount(clerkUserId: string): Promise<number> {
  const r = await getPool().query(
    "SELECT COUNT(*)::int AS cnt FROM user_inventory WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return r.rows[0]?.cnt ?? 0;
}

export async function getInventoryPage(
  clerkUserId: string, page = 1, perPage = 24, filters?: Record<string, any>
): Promise<{ items: any[]; total: number; synonymsApplied?: string[] }> {
  const conditions = ["clerk_user_id = $1"];
  const params: any[] = [clerkUserId];
  let idx = 2;
  let synonymsApplied: string[] = [];
  if (filters?.q) {
    const useSyn = filters.synonyms !== false;
    if (useSyn) {
      const { variants, applied } = expandWithSynonyms(filters.q);
      const likeClauses = [`data::text ILIKE $${idx}`];
      params.push(`%${filters.q}%`); idx++;
      for (const v of variants) {
        likeClauses.push(`data::text ILIKE $${idx}`);
        params.push(`%${v}%`); idx++;
      }
      conditions.push(`(${likeClauses.join(" OR ")})`);
      synonymsApplied = applied;
    } else {
      conditions.push(`(data::text ILIKE $${idx})`);
      params.push(`%${filters.q}%`); idx++;
    }
  }
  if (filters?.status) {
    conditions.push(`status = $${idx}`);
    params.push(filters.status); idx++;
  }
  const where = conditions.join(" AND ");
  const countR = await getPool().query(`SELECT COUNT(*)::int AS cnt FROM user_inventory WHERE ${where}`, params);
  const total = countR.rows[0]?.cnt ?? 0;
  const offset = (page - 1) * perPage;
  params.push(perPage, offset);
  const r = await getPool().query(
    `SELECT listing_id, discogs_release_id, data, status, price_value, price_currency, condition, sleeve_condition, posted_at
     FROM user_inventory WHERE ${where} ORDER BY posted_at DESC NULLS LAST LIMIT $${idx} OFFSET $${idx + 1}`, params
  );
  return { items: r.rows, total, synonymsApplied: synonymsApplied.length ? synonymsApplied : undefined };
}

export async function getUserListsList(clerkUserId: string): Promise<any[]> {
  const r = await getPool().query(
    `SELECT ul.list_id, ul.name, ul.description,
            COUNT(uli.id)::int AS item_count,
            ul.is_public, ul.synced_at
     FROM user_lists ul
     LEFT JOIN user_list_items uli
       ON ul.clerk_user_id = uli.clerk_user_id AND ul.list_id = uli.list_id
     WHERE ul.clerk_user_id = $1
     GROUP BY ul.list_id, ul.name, ul.description, ul.is_public, ul.synced_at
     ORDER BY ul.name`,
    [clerkUserId]
  );
  return r.rows;
}

// ── Lists ────────────────────────────────────────────────────────────────

export async function upsertUserLists(
  clerkUserId: string,
  lists: Array<{ listId: number; name: string; description?: string; itemCount?: number; isPublic?: boolean; data?: object }>
): Promise<void> {
  for (const list of lists) {
    await getPool().query(
      `INSERT INTO user_lists (clerk_user_id, list_id, name, description, item_count, is_public, data, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
       ON CONFLICT (clerk_user_id, list_id)
       DO UPDATE SET name = $3, description = $4, item_count = $5, is_public = $6, data = $7, synced_at = NOW()`,
      [clerkUserId, list.listId, list.name, list.description ?? null, list.itemCount ?? 0, list.isPublic ?? true, list.data ? JSON.stringify(list.data) : null]
    );
  }
}

// ── List items ──────────────────────────────────────────────────────────

export async function upsertListItems(
  clerkUserId: string,
  listId: number,
  items: Array<{ discogsId: number; entityType?: string; comment?: string; data?: object }>
): Promise<void> {
  if (!items.length) return;
  // Remove old items for this list, then insert fresh
  await getPool().query(
    `DELETE FROM user_list_items WHERE clerk_user_id = $1 AND list_id = $2`,
    [clerkUserId, listId]
  );
  for (const item of items) {
    await getPool().query(
      `INSERT INTO user_list_items (clerk_user_id, list_id, discogs_id, entity_type, comment, data, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW())
       ON CONFLICT (clerk_user_id, list_id, discogs_id) DO UPDATE SET entity_type = $4, comment = $5, data = $6, synced_at = NOW()`,
      [clerkUserId, listId, item.discogsId, item.entityType ?? "release", item.comment ?? null, item.data ? JSON.stringify(item.data) : null]
    );
  }
}

export async function getListItems(clerkUserId: string, listId: number): Promise<any[]> {
  const r = await getPool().query(
    `SELECT discogs_id, entity_type, comment, data FROM user_list_items WHERE clerk_user_id = $1 AND list_id = $2 ORDER BY id`,
    [clerkUserId, listId]
  );
  return r.rows;
}

/** Returns { discogsId → [{ listId, listName }] } for badge rendering */
export async function getListMembership(clerkUserId: string): Promise<Record<number, Array<{ listId: number; listName: string }>>> {
  const r = await getPool().query(
    `SELECT li.discogs_id, li.list_id, l.name
     FROM user_list_items li
     JOIN user_lists l ON l.clerk_user_id = li.clerk_user_id AND l.list_id = li.list_id
     WHERE li.clerk_user_id = $1`,
    [clerkUserId]
  );
  const map: Record<number, Array<{ listId: number; listName: string }>> = {};
  for (const row of r.rows) {
    if (!map[row.discogs_id]) map[row.discogs_id] = [];
    map[row.discogs_id].push({ listId: row.list_id, listName: row.name });
  }
  return map;
}

export async function getInventoryIds(clerkUserId: string): Promise<number[]> {
  const r = await getPool().query(
    "SELECT DISTINCT discogs_release_id FROM user_inventory WHERE clerk_user_id = $1 AND discogs_release_id IS NOT NULL",
    [clerkUserId]
  );
  return r.rows.map(row => row.discogs_release_id);
}

/** Returns { releaseId → [listingId, ...] } for modal/card linking. */
export interface InventoryListingSummary {
  id: number;
  status: string | null;
  price: number | null;
  currency: string | null;
  condition: string | null;
  sleeve: string | null;
  comments: string | null;
  posted_at: string | null;
}

/**
 * Returns a map of `releaseId → [InventoryListingSummary, ...]` for all of
 * the user's cached marketplace listings. Used to hydrate the release-modal
 * "Listed" tooltip and the inventory-link badges on cards without needing
 * an extra round-trip.
 */
export async function getInventoryListingIdsByRelease(
  clerkUserId: string
): Promise<Record<number, InventoryListingSummary[]>> {
  const r = await getPool().query(
    `SELECT discogs_release_id, listing_id, status, price_value, price_currency,
            condition, sleeve_condition, data, posted_at
     FROM user_inventory
     WHERE clerk_user_id = $1 AND discogs_release_id IS NOT NULL
     ORDER BY posted_at DESC NULLS LAST`,
    [clerkUserId]
  );
  const map: Record<number, InventoryListingSummary[]> = {};
  for (const row of r.rows) {
    const rid = Number(row.discogs_release_id);
    if (!map[rid]) map[rid] = [];
    // Pull comments and posted date out of the full listing JSON. The JSON
    // blob is refreshed on every sync/write so it's the most reliable source,
    // whereas the dedicated `posted_at` column can be stale if an earlier
    // sync pass persisted a NULL or a touch-timestamp by mistake.
    let comments: string | null = null;
    let postedFromJson: string | null = null;
    try {
      const d = row.data;
      if (d) {
        const obj = typeof d === "string" ? JSON.parse(d) : d;
        comments = obj?.comments ?? null;
        if (obj?.posted) postedFromJson = String(obj.posted);
      }
    } catch {}
    const postedIso = postedFromJson
      ? new Date(postedFromJson).toISOString()
      : (row.posted_at ? new Date(row.posted_at).toISOString() : null);
    map[rid].push({
      id:        Number(row.listing_id),
      status:    row.status ?? null,
      price:     row.price_value != null ? Number(row.price_value) : null,
      currency:  row.price_currency ?? null,
      condition: row.condition ?? null,
      sleeve:    row.sleeve_condition ?? null,
      comments,
      posted_at: postedIso,
    });
  }
  return map;
}

export async function getInventoryItem(clerkUserId: string, listingId: number): Promise<any | null> {
  const r = await getPool().query(
    `SELECT listing_id, discogs_release_id, data, status, price_value, price_currency, condition, sleeve_condition, posted_at, synced_at
     FROM user_inventory WHERE clerk_user_id = $1 AND listing_id = $2`,
    [clerkUserId, listingId]
  );
  return r.rows[0] ?? null;
}

export async function deleteInventoryItem(clerkUserId: string, listingId: number): Promise<void> {
  await getPool().query(
    `DELETE FROM user_inventory WHERE clerk_user_id = $1 AND listing_id = $2`,
    [clerkUserId, listingId]
  );
}

export async function getListItemStats(clerkUserId: string): Promise<{ totalItems: number; listsWithItems: number }> {
  const r = await getPool().query(
    `SELECT COUNT(*)::int AS total_items, COUNT(DISTINCT list_id)::int AS lists_with_items FROM user_list_items WHERE clerk_user_id = $1`,
    [clerkUserId]
  );
  return { totalItems: r.rows[0]?.total_items ?? 0, listsWithItems: r.rows[0]?.lists_with_items ?? 0 };
}

// ── Orders (seller-side marketplace orders) ───────────────────────────────

export async function upsertUserOrders(
  clerkUserId: string,
  orders: Array<{ orderId: string; status?: string; buyerUsername?: string; sellerUsername?: string; totalValue?: number; totalCurrency?: string; itemCount?: number; createdAt?: Date; data?: object }>
): Promise<void> {
  for (const order of orders) {
    await getPool().query(
      `INSERT INTO user_orders (clerk_user_id, order_id, status, buyer_username, seller_username, total_value, total_currency, item_count, created_at, data, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
       ON CONFLICT (clerk_user_id, order_id)
       DO UPDATE SET status = $3, buyer_username = $4, seller_username = $5, total_value = $6, total_currency = $7, item_count = $8, created_at = $9, data = $10, synced_at = NOW()`,
      [clerkUserId, order.orderId, order.status ?? null, order.buyerUsername ?? null, order.sellerUsername ?? null, order.totalValue ?? null, order.totalCurrency ?? "USD", order.itemCount ?? 0, order.createdAt ?? null, order.data ? JSON.stringify(order.data) : null]
    );
  }
}

export async function updateOrdersSyncedAt(clerkUserId: string): Promise<void> {
  await getPool().query(
    "UPDATE user_tokens SET orders_synced_at = NOW() WHERE clerk_user_id = $1",
    [clerkUserId]
  );
}

export async function getOrdersCount(clerkUserId: string): Promise<number> {
  const r = await getPool().query(
    "SELECT COUNT(*)::int AS cnt FROM user_orders WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return r.rows[0]?.cnt ?? 0;
}

export async function getUserOrdersPage(
  clerkUserId: string, page = 1, perPage = 25, filters?: Record<string, any>
): Promise<{ items: any[]; total: number }> {
  const conditions = ["clerk_user_id = $1"];
  const params: any[] = [clerkUserId];
  let idx = 2;
  if (filters?.status) {
    conditions.push(`status = $${idx}`);
    params.push(filters.status); idx++;
  }
  if (filters?.q) {
    conditions.push(`(buyer_username ILIKE $${idx} OR order_id ILIKE $${idx} OR data::text ILIKE $${idx})`);
    params.push(`%${filters.q}%`); idx++;
  }
  const where = conditions.join(" AND ");
  const countR = await getPool().query(`SELECT COUNT(*)::int AS cnt FROM user_orders WHERE ${where}`, params);
  const total = countR.rows[0]?.cnt ?? 0;
  const offset = (page - 1) * perPage;
  params.push(perPage, offset);
  const r = await getPool().query(
    `SELECT order_id, status, buyer_username, seller_username, total_value, total_currency, item_count, created_at, data, synced_at, viewed_at,
            CASE
              WHEN (data->>'last_activity') IS NULL THEN false
              WHEN viewed_at IS NULL THEN true
              WHEN (data->>'last_activity')::timestamptz > viewed_at THEN true
              ELSE false
            END AS has_new
     FROM user_orders WHERE ${where} ORDER BY created_at DESC NULLS LAST LIMIT $${idx} OFFSET $${idx + 1}`,
    params
  );
  return { items: r.rows, total };
}

export async function getUserOrder(clerkUserId: string, orderId: string): Promise<any | null> {
  const r = await getPool().query(
    `SELECT order_id, status, buyer_username, seller_username, total_value, total_currency, item_count, created_at, data, synced_at, viewed_at
     FROM user_orders WHERE clerk_user_id = $1 AND order_id = $2`,
    [clerkUserId, orderId]
  );
  return r.rows[0] ?? null;
}

export async function markOrderViewed(clerkUserId: string, orderId: string): Promise<void> {
  await getPool().query(
    `UPDATE user_orders SET viewed_at = NOW() WHERE clerk_user_id = $1 AND order_id = $2`,
    [clerkUserId, orderId]
  );
}

export async function getUnreadOrdersCount(clerkUserId: string): Promise<number> {
  const r = await getPool().query(
    `SELECT COUNT(*)::int AS cnt FROM user_orders
      WHERE clerk_user_id = $1
        AND (data->>'last_activity') IS NOT NULL
        AND (viewed_at IS NULL OR (data->>'last_activity')::timestamptz > viewed_at)`,
    [clerkUserId]
  );
  return r.rows[0]?.cnt ?? 0;
}

export async function upsertOrderMessages(
  clerkUserId: string,
  orderId: string,
  messages: Array<{ order: number; subject?: string; message?: string; fromUser?: string; ts?: Date; data?: object }>
): Promise<void> {
  if (!messages.length) return;
  // Replace the whole thread for a given order (simpler than merging)
  await getPool().query(
    `DELETE FROM user_order_messages WHERE clerk_user_id = $1 AND order_id = $2`,
    [clerkUserId, orderId]
  );
  for (const m of messages) {
    await getPool().query(
      `INSERT INTO user_order_messages (clerk_user_id, order_id, message_order, subject, message, from_user, ts, data, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
       ON CONFLICT (clerk_user_id, order_id, message_order)
       DO UPDATE SET subject = $4, message = $5, from_user = $6, ts = $7, data = $8, synced_at = NOW()`,
      [clerkUserId, orderId, m.order, m.subject ?? null, m.message ?? null, m.fromUser ?? null, m.ts ?? null, m.data ? JSON.stringify(m.data) : null]
    );
  }
}

export async function getOrderMessages(clerkUserId: string, orderId: string): Promise<any[]> {
  const r = await getPool().query(
    `SELECT message_order, subject, message, from_user, ts, data
     FROM user_order_messages WHERE clerk_user_id = $1 AND order_id = $2 ORDER BY message_order ASC`,
    [clerkUserId, orderId]
  );
  return r.rows;
}

// ── Auto-prune stale data ─────────────────────────────────────────────────
export async function pruneAllStaleData(): Promise<{
  collection: number; wantlist: number; folders: number;
  inventory: number; listItems: number; lists: number; orders: number;
}> {
  const interval30d = `NOW() - INTERVAL '30 days'`;

  // User collection older than 30 days
  const col = await getPool().query(
    `DELETE FROM user_collection WHERE synced_at < ${interval30d}`
  );
  // User wantlist older than 30 days
  const wl = await getPool().query(
    `DELETE FROM user_wantlist WHERE synced_at < ${interval30d}`
  );
  // user_collection_folders has no synced_at column (and was never going
  // to — folders are user-scoped metadata, lifecycle tied to the user).
  // The old query here was erroring daily in the Postgres logs. Folder
  // rows are tiny and already removed via the user-deletion cascade, so
  // dropping the prune step entirely. fld stays at 0 so the return-shape
  // is unchanged.
  const fld = { rowCount: 0 } as { rowCount: number };
  // User inventory older than 30 days
  const inv = await getPool().query(
    `DELETE FROM user_inventory WHERE synced_at < ${interval30d}`
  );
  // User list items older than 30 days
  const lsti = await getPool().query(
    `DELETE FROM user_list_items WHERE synced_at < ${interval30d}`
  );
  // User lists older than 30 days
  const lst = await getPool().query(
    `DELETE FROM user_lists WHERE synced_at < ${interval30d}`
  );
  // User orders older than 30 days
  const ord = await getPool().query(
    `DELETE FROM user_orders WHERE synced_at < ${interval30d}`
  );
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
export async function logApiRequest(opts: {
  service: string;
  endpoint: string;
  method?: string;
  statusCode?: number;
  success: boolean;
  durationMs?: number;
  errorMessage?: string;
  context?: string;
}): Promise<void> {
  try {
    await getPool().query(
      `INSERT INTO api_request_log (service, endpoint, method, status_code, success, duration_ms, error_message, context)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [opts.service, opts.endpoint, opts.method ?? "GET", opts.statusCode ?? null, opts.success, opts.durationMs ?? null, opts.errorMessage ?? null, opts.context ?? null]
    );
  } catch {
    // Don't let logging failures break the app
  }
  // Auto-prune: keep only last 10,000 rows
  try {
    await getPool().query(
      `DELETE FROM api_request_log WHERE id NOT IN (SELECT id FROM api_request_log ORDER BY created_at DESC LIMIT 10000)`
    );
  } catch {}
}

export async function getApiRequestLog(opts?: { service?: string; successOnly?: boolean; errorsOnly?: boolean; scheduledOnly?: boolean; hours?: number }): Promise<{ items: any[]; total: number }> {
  const params: any[] = [];
  let where = "WHERE 1=1";
  const hours = opts?.hours ?? 24;
  params.push(hours);
  where += ` AND created_at > NOW() - INTERVAL '1 hour' * $${params.length}`;
  if (opts?.service) {
    params.push(opts.service);
    where += ` AND service = $${params.length}`;
  }
  if (opts?.successOnly) where += " AND success = true";
  if (opts?.errorsOnly) where += " AND success = false";
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

  const r = await getPool().query(
    `SELECT * FROM api_request_log ${where} ORDER BY created_at DESC`,
    params
  );
  return { items: r.rows, total: r.rows.length };
}

// ── User collection/wantlist stats (admin) ────────────────────────────────
export async function getUserCollectionStats(): Promise<{ users: any[]; global: any }> {
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

// ── Release / master / artist cache ───────────────────────────────────────
//
// Generic Discogs metadata cache. The `release_cache` table name is kept
// for backwards compat but now also stores 'artist' rows alongside
// 'release' and 'master'. Each call site picks an appropriate TTL via
// `maxAgeSeconds` — masters/artists are stable so 7 days is fine;
// releases get 1 day since their metadata can be edited.

export type DiscogsCacheType = "release" | "master" | "artist" | "master-versions";

/** Get a cached entry from DB. Returns null if not cached or if the
 *  entry is older than `maxAgeSeconds` (cache miss vs stale-eviction
 *  collapsed to a single null return — caller decides whether to
 *  refetch). When `maxAgeSeconds` is undefined the entry is returned
 *  regardless of age (caller can then decide based on cached_at). */
export async function getCachedRelease(
  discogsId: number,
  type: DiscogsCacheType,
  maxAgeSeconds?: number,
): Promise<any | null> {
  const r = await getPool().query(
    `SELECT data, cached_at FROM release_cache WHERE discogs_id = $1 AND type = $2`,
    [discogsId, type]
  );
  const row = r.rows[0];
  if (!row) return null;
  if (typeof maxAgeSeconds === "number" && row.cached_at) {
    const ageMs = Date.now() - new Date(row.cached_at).getTime();
    if (ageMs > maxAgeSeconds * 1000) return null;
  }
  // Promote on first read: a row whose seen_at is NULL was pre-warmed
  // by the overnight job and hasn't yet been touched by a real user.
  // Reaching it via getCachedRelease almost always means a user just
  // opened it (modal / version / suggestion click), so stamp seen_at
  // now so the feed pool starts including it. Fire-and-forget — no
  // need to block the response on the bookkeeping. Bare release/
  // master types only; master-versions / artist are infra caches that
  // don't need promotion since they don't appear in the feed anyway.
  if (type === "release" || type === "master") {
    getPool().query(
      `UPDATE release_cache SET seen_at = NOW()
        WHERE discogs_id = $1 AND type = $2 AND seen_at IS NULL`,
      [discogsId, type],
    ).catch(() => {});
  }
  return row.data ?? null;
}

/** Save a metadata response to cache. Overwrites if already present.
 *  Default behaviour stamps seen_at = NOW() — i.e. this write came
 *  from a user click (or any path where surfacing in the feed is
 *  appropriate). Pass `warmOnly: true` for the cache-warm job so
 *  the row stays out of the feed until a user actually opens it. */
export async function cacheRelease(
  discogsId: number,
  type: DiscogsCacheType,
  data: object,
  opts?: { warmOnly?: boolean },
): Promise<void> {
  const warmOnly = !!opts?.warmOnly;
  if (warmOnly) {
    // Don't downgrade an existing engaged row back to warm-only —
    // COALESCE keeps any prior seen_at intact, only sets it if the
    // current value is NULL (which for a fresh row means leaving it
    // NULL, i.e. warm-only).
    await getPool().query(
      `INSERT INTO release_cache (discogs_id, type, data, cached_at, seen_at)
       VALUES ($1, $2, $3, NOW(), NULL)
       ON CONFLICT (discogs_id, type)
       DO UPDATE SET data = EXCLUDED.data, cached_at = NOW()`,
      [discogsId, type, JSON.stringify(data)]
    );
  } else {
    await getPool().query(
      `INSERT INTO release_cache (discogs_id, type, data, cached_at, seen_at)
       VALUES ($1, $2, $3, NOW(), NOW())
       ON CONFLICT (discogs_id, type)
       DO UPDATE SET data = EXCLUDED.data, cached_at = NOW(),
                     seen_at = COALESCE(release_cache.seen_at, NOW())`,
      [discogsId, type, JSON.stringify(data)]
    );
  }
}

/** Prune stale cache entries — masters/artists older than 30 days,
 *  releases older than 7 days. Run nightly via a scheduled task or
 *  on-demand from the admin DB Stats panel. Returns rows deleted. */
export async function pruneStaleReleaseCache(): Promise<number> {
  const r = await getPool().query(
    `DELETE FROM release_cache
       WHERE (type IN ('master','artist') AND cached_at < NOW() - INTERVAL '30 days')
          OR (type = 'release' AND cached_at < NOW() - INTERVAL '7 days')`
  );
  return r.rowCount ?? 0;
}

export async function getApiRequestStats(hours: number = 24): Promise<any[]> {
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
export async function getApiHealth(hours: number = 24): Promise<any[]> {
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
export async function getAdminOverview(): Promise<any> {
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
export async function getMediaStats(topLimit = 10): Promise<any> {
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
    bySource7d: bySource.rows,                       // [{source,n}]
    topTitles30d: topTitles.rows,                    // [{title,source,n}]
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
export async function getDiscogsRateWindow(): Promise<{ lastMinute: number; last24h: number }> {
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
export async function getJobHealth(): Promise<{ suggestionsLastAt: string | null; cacheWarmLastAt: string | null }> {
  const pool = getPool();
  const [s, c] = await Promise.all([
    pool.query(`SELECT MAX(generated_at) AS t FROM user_personal_suggestions`),
    pool.query(`SELECT MAX(cached_at) AS t FROM release_cache WHERE seen_at IS NULL`),
  ]);
  const iso = (v: any) => v ? new Date(v).toISOString() : null;
  return {
    suggestionsLastAt: iso(s.rows[0]?.t),
    cacheWarmLastAt: iso(c.rows[0]?.t),
  };
}

// ── Background-job run tracking ──────────────────────────────────────
// Wrap a scheduled job: startJobRun() at the top, finishJobRun() in a
// finally. Best-effort — a logging failure must never break the job,
// so callers should .catch() these.
export async function startJobRun(jobName: string): Promise<number | null> {
  try {
    const r = await getPool().query(
      `INSERT INTO job_runs (job_name, status) VALUES ($1, 'running') RETURNING id`,
      [jobName]
    );
    return r.rows[0]?.id ?? null;
  } catch { return null; }
}
export async function finishJobRun(
  id: number | null,
  opts: { status: "ok" | "error"; items?: number; errors?: number; detail?: string }
): Promise<void> {
  if (id == null) return;
  try {
    await getPool().query(
      `UPDATE job_runs
          SET status = $2, ended_at = NOW(),
              items = $3, errors = $4, detail = $5
        WHERE id = $1`,
      [id, opts.status, opts.items ?? 0, opts.errors ?? 0,
       (opts.detail ?? "").slice(0, 500) || null]
    );
    // Keep the table bounded: retain the most recent 100 runs per job.
    await getPool().query(
      `DELETE FROM job_runs jr
        WHERE jr.job_name = (SELECT job_name FROM job_runs WHERE id = $1)
          AND jr.id NOT IN (
            SELECT id FROM job_runs
             WHERE job_name = (SELECT job_name FROM job_runs WHERE id = $1)
             ORDER BY started_at DESC LIMIT 100
          )`,
      [id]
    );
  } catch { /* best-effort */ }
}
// Latest run per job (for the admin Active-APIs popup).
export async function getJobLastRuns(): Promise<Array<{
  job_name: string; status: string; started_at: string;
  ended_at: string | null; items: number; errors: number; detail: string | null;
}>> {
  try {
    const r = await getPool().query(`
      SELECT DISTINCT ON (job_name)
             job_name, status, started_at, ended_at, items, errors, detail
        FROM job_runs
       ORDER BY job_name, started_at DESC
    `);
    return r.rows;
  } catch { return []; }
}
// Full recent history for one job (audit drill-down).
export async function getRecentJobRuns(jobName: string, limit = 25): Promise<any[]> {
  try {
    const r = await getPool().query(
      `SELECT id, job_name, status, started_at, ended_at, items, errors, detail
         FROM job_runs WHERE job_name = $1
        ORDER BY started_at DESC LIMIT $2`,
      [jobName, Math.max(1, Math.min(200, limit))]
    );
    return r.rows;
  } catch { return []; }
}

// ── Table row counts (admin dashboard) ───────────────────────────────────
export async function getTableRowCounts(): Promise<Array<{ table: string; rows: number }>> {
  const tables = [
    'user_tokens', 'user_collection', 'user_collection_folders', 'user_wantlist',
    'user_inventory', 'user_lists', 'user_list_items', 'user_orders', 'user_order_messages',
    'user_favorites', 'user_recent_views', 'user_loc_saves', 'user_archive_saves',
    'user_youtube_saves', 'user_wiki_saves', 'user_play_queue', 'saved_searches', 'feedback',
    'release_cache', 'price_cache', 'price_history',
    'blues_artists', 'api_request_log', 'oauth_request_tokens',
    'app_settings',
  ];
  const counts = await Promise.all(
    tables.map(async (t) => {
      try {
        const r = await getPool().query(`SELECT COUNT(*)::int AS cnt FROM ${t}`);
        return { table: t, rows: r.rows[0]?.cnt ?? 0 };
      } catch {
        return { table: t, rows: -1 };
      }
    })
  );
  return counts;
}

// ── App settings (key/value) ─────────────────────────────────────────────
export async function getAppSetting(key: string): Promise<string | null> {
  try {
    const r = await getPool().query(
      `SELECT value FROM app_settings WHERE key = $1 LIMIT 1`,
      [key]
    );
    return r.rows[0]?.value ?? null;
  } catch { return null; }
}

export async function setAppSetting(key: string, value: string | null): Promise<void> {
  await getPool().query(
    `INSERT INTO app_settings (key, value, updated_at) VALUES ($1, $2, NOW())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
    [key, value]
  );
}

// ── User preferences (cross-device) ───────────────────────────────────
// Returns the prefs JSON for a user, or {} if no row. Never throws —
// missing rows or DB hiccups are treated as "no prefs set."
export async function getUserPrefs(clerkUserId: string): Promise<Record<string, any>> {
  try {
    const r = await getPool().query(
      `SELECT prefs FROM user_preferences WHERE clerk_user_id = $1 LIMIT 1`,
      [clerkUserId]
    );
    const row = r.rows[0]?.prefs;
    return row && typeof row === "object" ? row : {};
  } catch { return {}; }
}

// Merge-update a user's prefs. Pass partial keys; existing keys not in
// the patch are preserved. Returns the post-merge prefs.
export async function setUserPrefs(clerkUserId: string, patch: Record<string, any>): Promise<Record<string, any>> {
  const current = await getUserPrefs(clerkUserId);
  const merged = { ...current, ...patch };
  await getPool().query(
    `INSERT INTO user_preferences (clerk_user_id, prefs, updated_at)
     VALUES ($1, $2::jsonb, NOW())
     ON CONFLICT (clerk_user_id) DO UPDATE SET prefs = EXCLUDED.prefs, updated_at = NOW()`,
    [clerkUserId, JSON.stringify(merged)]
  );
  return merged;
}

// ── Track YouTube overrides (crowd-sourced gap fill) ──────────────────
// All IDs stored as strings (release/master IDs come in as numbers from
// Discogs but TEXT keeps the column shape stable if we ever stash a
// non-numeric source).

export type TrackYtOverride = {
  release_id: string;
  release_type: "master" | "release";
  track_position: string;
  track_title: string | null;
  video_id: string;
  video_title: string | null;
  submitted_by: string;
  submitted_at: Date;
};

// Look up overrides for a given master/release. Returns rows for both
// the master scope and (if releaseId is provided) the matching release
// scope; caller picks per-position with release-scope winning.
export async function getTrackYtOverrides(
  masterId: string | number | null,
  releaseId: string | number | null
): Promise<TrackYtOverride[]> {
  const params: string[] = [];
  const ors: string[] = [];
  if (masterId != null && String(masterId).length) {
    params.push(String(masterId));
    ors.push(`(release_type = 'master' AND release_id = $${params.length})`);
  }
  if (releaseId != null && String(releaseId).length) {
    params.push(String(releaseId));
    ors.push(`(release_type = 'release' AND release_id = $${params.length})`);
  }
  if (ors.length === 0) return [];
  try {
    // LEFT JOIN against unavailable list so we can exclude rows whose
    // video has been flagged "unavailable" by enough users — the
    // override served to clients only contains videos that are likely
    // playable. Flagged-but-not-yet-unavailable rows still serve.
    const r = await getPool().query(
      `SELECT o.release_id, o.release_type, o.track_position, o.track_title,
              o.video_id, o.video_title, o.submitted_by, o.submitted_at
         FROM track_youtube_overrides o
         LEFT JOIN youtube_video_unavailable u
           ON u.video_id = o.video_id AND u.status = 'unavailable'
        WHERE (${ors.join(" OR ")})
          AND u.video_id IS NULL`,
      params
    );
    return r.rows as TrackYtOverride[];
  } catch { return []; }
}

// First submission wins. Returns true if inserted, false if a row
// already existed (ON CONFLICT DO NOTHING).
export async function suggestTrackYtOverride(args: {
  releaseId: string | number;
  releaseType: "master" | "release";
  trackPosition: string;
  trackTitle?: string | null;
  videoId: string;
  videoTitle?: string | null;
  submittedBy: string;
}): Promise<boolean> {
  const r = await getPool().query(
    `INSERT INTO track_youtube_overrides
       (release_id, release_type, track_position, track_title, video_id, video_title, submitted_by)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (release_id, release_type, track_position) DO NOTHING
     RETURNING release_id`,
    [
      String(args.releaseId),
      args.releaseType,
      args.trackPosition,
      args.trackTitle ?? null,
      args.videoId,
      args.videoTitle ?? null,
      args.submittedBy,
    ]
  );
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
export async function suggestTrackYtOverridesBatch(
  items: Array<{
    releaseId: string | number;
    releaseType: "master" | "release";
    trackPosition: string;
    trackTitle?: string | null;
    videoId: string;
    videoTitle?: string | null;
    submittedBy: string;
  }>
): Promise<{ inserted: number; skipped: number }> {
  if (!items.length) return { inserted: 0, skipped: 0 };
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    let inserted = 0;
    for (const it of items) {
      const r = await client.query(
        `INSERT INTO track_youtube_overrides
           (release_id, release_type, track_position, track_title, video_id, video_title, submitted_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (release_id, release_type, track_position) DO NOTHING
         RETURNING release_id`,
        [
          String(it.releaseId),
          it.releaseType,
          it.trackPosition,
          it.trackTitle ?? null,
          it.videoId,
          it.videoTitle ?? null,
          it.submittedBy,
        ]
      );
      if (r.rowCount === 1) inserted++;
    }
    await client.query("COMMIT");
    return { inserted, skipped: items.length - inserted };
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

// Admin: delete a single override (called from the album popup or the
// admin tab). Returns true if a row was removed.
export async function deleteTrackYtOverride(
  releaseId: string | number,
  releaseType: "master" | "release",
  trackPosition: string
): Promise<boolean> {
  const r = await getPool().query(
    `DELETE FROM track_youtube_overrides
      WHERE release_id = $1 AND release_type = $2 AND track_position = $3`,
    [String(releaseId), releaseType, trackPosition]
  );
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
export async function getVideoStatusBatch(
  items: Array<{ id: number; type: "release" | "master" }>
): Promise<Array<{ id: number; type: "release" | "master"; hasVideos: boolean | null }>> {
  if (!items.length) return [];
  const masterIds = items.filter(i => i.type === "master").map(i => i.id);
  const releaseIds = items.filter(i => i.type === "release").map(i => i.id);

  const cacheRows: Array<{ id: number; type: string; has_videos: boolean }> = [];
  if (masterIds.length) {
    const r = await getPool().query(
      `SELECT discogs_id AS id, 'master'::text AS type,
              (jsonb_typeof(data->'videos') = 'array' AND jsonb_array_length(data->'videos') > 0) AS has_videos
         FROM release_cache
        WHERE type = 'master' AND discogs_id = ANY($1::int[])`,
      [masterIds]
    );
    cacheRows.push(...(r.rows as any));
  }
  if (releaseIds.length) {
    const r = await getPool().query(
      `SELECT discogs_id AS id, 'release'::text AS type,
              (jsonb_typeof(data->'videos') = 'array' AND jsonb_array_length(data->'videos') > 0) AS has_videos
         FROM release_cache
        WHERE type = 'release' AND discogs_id = ANY($1::int[])`,
      [releaseIds]
    );
    cacheRows.push(...(r.rows as any));
  }

  // Cross-check crowd-sourced overrides — a master with no Discogs
  // videos but with user-contributed overrides shouldn't still read as
  // "needs videos" in search.
  const allIds = items.map(i => String(i.id));
  let overrideRows: Array<{ id: string; type: string }> = [];
  if (allIds.length) {
    const r = await getPool().query(
      `SELECT release_id AS id, release_type AS type
         FROM track_youtube_overrides
        WHERE release_id = ANY($1::text[])`,
      [allIds]
    );
    overrideRows = r.rows as any;
  }
  const overrideSet = new Set(overrideRows.map(r => `${r.type}:${r.id}`));

  const cacheMap = new Map<string, boolean>();
  for (const r of cacheRows) cacheMap.set(`${r.type}:${r.id}`, r.has_videos);

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
export async function getUserSubmittedAlbums(
  clerkUserId: string,
  limit = 96
): Promise<any[]> {
  try {
    const r = await getPool().query(
      `WITH counts AS (
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
        LIMIT $2`,
      [clerkUserId, Math.max(1, Math.min(500, limit))]
    );
    return r.rows;
  } catch { return []; }
}

// Pull masters/releases that have at least one crowd-sourced YouTube
// override, joined with release_cache so each row carries the card
// snapshot the home strip needs (orphan rows are skipped). Order is
// caller-selectable: "most" (default), "fewest", or "recent" — the
// last sorts by the most recently submitted override timestamp.
export async function getMostContributedAlbums(
  limit = 48,
  order: "most" | "fewest" | "recent" = "most"
): Promise<any[]> {
  let orderBy: string;
  switch (order) {
    case "fewest": orderBy = "c.n ASC, c.release_id ASC"; break;
    case "recent": orderBy = "c.last_at DESC, c.release_id DESC"; break;
    case "most":
    default:       orderBy = "c.n DESC, c.release_id DESC"; break;
  }
  try {
    const r = await getPool().query(
      `WITH counts AS (
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
        LIMIT $1`,
      [Math.max(1, Math.min(200, limit))]
    );
    return r.rows;
  } catch { return []; }
}

// Random sample of cached albums for the public Feed strip — anon
// visitors see this as their home view (signed-in users get it as the
// "Feed" tab in the Recent/Suggestions/Submitted/Feed strip). All
// rows already paid for via the release_cache; no upstream Discogs
// hit. Defaults to masters (richer card data, broader scope) but
// callers can opt for either via `type`.
export async function getFeedRandomAlbums(
  limit = 48,
  type: "master" | "release" | "any" = "any",
  excludeIds?: Array<{ id: number; type: string }>
): Promise<any[]> {
  try {
    const cap = Math.max(1, Math.min(200, limit));
    // Exclusion filter — used by Load More so already-shown rows
    // don't repeat in the next page. Encoded as parallel arrays of
    // (discogs_id, type) so we can NOT (id, type) IN (...) match.
    const exclude = Array.isArray(excludeIds) ? excludeIds.slice(0, 500) : [];
    const excludeIdsArr  = exclude.map(e => Number(e.id));
    const excludeTypeArr = exclude.map(e => String(e.type));
    const params: any[] = [cap];
    let where = "";
    if (type !== "any") {
      params.push(type);
      where += `WHERE type = $${params.length} `;
    } else {
      // "any" means "any album", NOT every row in release_cache. The
      // table also stores master-versions (pressing-list payloads) and
      // artist (artist profile cache) which are infrastructure caches,
      // not displayable albums — surfacing them as feed cards produces
      // empty placeholders like "master-versions 645422 / NO IMAGE".
      // Restrict to the two real album types.
      where += `WHERE type IN ('master','release') `;
    }
    // Hide pre-warmed-but-unviewed rows. The overnight cache-warm job
    // pulls thousands of suggested albums into release_cache; without
    // this filter they'd flood the public feed even though no human
    // has interacted with them. seen_at stamps on first user click,
    // so the feed only surfaces albums someone actually engaged with.
    where += "AND seen_at IS NOT NULL ";
    if (exclude.length) {
      params.push(excludeIdsArr);
      const idIdx = params.length;
      params.push(excludeTypeArr);
      const tyIdx = params.length;
      where += where ? "AND " : "WHERE ";
      where += `NOT (discogs_id = ANY($${idIdx}::int[]) AND type = ANY($${tyIdx}::text[])) `;
    }
    const sql = `SELECT discogs_id AS id, type, data, cached_at
                   FROM release_cache
                   ${where}
                  ORDER BY RANDOM() LIMIT $1`;
    const r = await getPool().query(sql, params);
    return r.rows;
  } catch { return []; }
}

// Batched lookup of track_youtube_overrides for a list of
// (release_id, release_type) pairs. Used by the wide-card enrichment
// path so each card's tracks (and the special ALBUM full-album slot)
// can render play / queue buttons keyed off user-contributed overrides
// in addition to Discogs's own videos[]. Excludes rows whose video
// has been flagged "unavailable" via the LEFT JOIN against
// youtube_video_unavailable, mirroring getTrackYtOverrides.
export async function getTrackYtOverridesBatch(
  pairs: Array<{ id: number; type: string }>
): Promise<Array<{ release_id: string; release_type: string; track_position: string; video_id: string; video_title: string | null }>> {
  if (!Array.isArray(pairs) || !pairs.length) return [];
  const capped = pairs.slice(0, 200).filter(p => Number.isFinite(Number(p.id)) && (p.type === "master" || p.type === "release"));
  if (!capped.length) return [];
  try {
    const ids = capped.map(p => String(p.id));
    const types = capped.map(p => String(p.type));
    const r = await getPool().query(
      `SELECT o.release_id, o.release_type, o.track_position, o.video_id, o.video_title
         FROM track_youtube_overrides o
         LEFT JOIN youtube_video_unavailable u
           ON u.video_id = o.video_id AND u.status = 'unavailable'
        WHERE (o.release_id, o.release_type) IN (
          SELECT * FROM unnest($1::text[], $2::text[])
        )
          AND u.video_id IS NULL`,
      [ids, types]
    );
    return r.rows;
  } catch { return []; }
}

// Batched lookup of release_cache rows for a list of (id, type)
// pairs. Used by /api/cards/enrich to backfill the wide-card image
// strip + inline tracklist on any card surface (favorites, search
// results, collection, etc.) without requiring each endpoint to
// JOIN with release_cache itself. Returns only rows that exist —
// cache misses are silently dropped.
export async function getCacheEnrichmentBatch(
  pairs: Array<{ id: number; type: string }>
): Promise<Array<{ id: number; type: string; data: any }>> {
  if (!Array.isArray(pairs) || !pairs.length) return [];
  // Cap input size to keep the query bounded; at 200 pairs the
  // query fingerprint is still small (~3KB JSON) and the unnest
  // cost is negligible.
  const capped = pairs.slice(0, 200).filter(p => Number.isFinite(Number(p.id)) && (p.type === "master" || p.type === "release"));
  if (!capped.length) return [];
  try {
    const ids = capped.map(p => Number(p.id));
    const types = capped.map(p => String(p.type));
    const r = await getPool().query(
      `SELECT discogs_id AS id, type, data
         FROM release_cache
        WHERE (discogs_id, type) IN (
          SELECT * FROM unnest($1::int[], $2::text[])
        )`,
      [ids, types]
    );
    return r.rows;
  } catch { return []; }
}

// AI-search exclusion list: a compact set of "Artist - Title" lines
// pulled from the user's collection + wantlist so the recommendation
// prompt can tell Claude what to avoid. Capped so the prompt stays
// within token budget — at 200 lines × ~50 chars = ~10KB which is
// fine. UNION-distinct keeps a single-line entry per album the user
// has in either list.
export async function getAiExclusionTitles(
  clerkUserId: string,
  limit = 200
): Promise<string[]> {
  try {
    const r = await getPool().query(
      `SELECT title FROM (
         SELECT DISTINCT NULLIF(data->>'title', '') AS title
           FROM user_collection
          WHERE clerk_user_id = $1
         UNION
         SELECT DISTINCT NULLIF(data->>'title', '') AS title
           FROM user_wantlist
          WHERE clerk_user_id = $1
       ) sub
       WHERE title IS NOT NULL
       LIMIT $2`,
      [clerkUserId, Math.max(1, Math.min(500, limit))]
    );
    return r.rows.map(row => String(row.title || "")).filter(Boolean);
  } catch { return []; }
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
export async function getUserSuggestionEngagement(clerkUserId: string): Promise<{
  hibernated: boolean;
  hasSuggestions: boolean;
  oldestSuggDays: number;
  recentClicks: number;
}> {
  try {
    const r = await getPool().query(
      `SELECT
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
         ) AS recent_clicks`,
      [clerkUserId],
    );
    const row = r.rows[0] || {};
    return {
      hibernated:     !!row.hibernated,
      hasSuggestions: Number(row.sugg_count) > 0,
      oldestSuggDays: Number(row.oldest_sugg_days) || 0,
      recentClicks:   Number(row.recent_clicks)    || 0,
    };
  } catch {
    return { hibernated: false, hasSuggestions: false, oldestSuggDays: 0, recentClicks: 0 };
  }
}

// Cheap fingerprint of a user's taste-source state. The suggestions
// job compares the current signature to the one stored from its last
// successful run; if nothing has changed (no new plays, no new
// favorites, no collection/wantlist deltas), the run early-exits. Lets
// us run frequently without burning Discogs API budget on idle users.
export async function getUserTasteSignature(clerkUserId: string): Promise<string> {
  try {
    const r = await getPool().query(
      `SELECT
         COALESCE(EXTRACT(EPOCH FROM (SELECT MAX(created_at) FROM user_play_events WHERE clerk_user_id = $1))::bigint, 0) AS p,
         COALESCE(EXTRACT(EPOCH FROM (SELECT MAX(created_at) FROM user_favorites   WHERE clerk_user_id = $1))::bigint, 0) AS f,
         (SELECT COUNT(*)::int FROM user_collection WHERE clerk_user_id = $1) AS c,
         (SELECT COUNT(*)::int FROM user_wantlist   WHERE clerk_user_id = $1) AS w`,
      [clerkUserId],
    );
    const row = r.rows[0] || {};
    return `${row.p ?? 0}|${row.f ?? 0}|${row.c ?? 0}|${row.w ?? 0}`;
  } catch { return ""; }
}

export async function getUserTasteTuples(
  clerkUserId: string,
  limit = 9
): Promise<Array<{ genre: string; style: string; year: number; n: number }>> {
  try {
    const r = await getPool().query(
      `WITH plays AS (
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
        LIMIT $2`,
      [clerkUserId, Math.max(1, Math.min(50, limit))]
    );
    return r.rows;
  } catch { return []; }
}

// Set of master/release ids the user already has anywhere — used so
// the suggestions job doesn't surface stuff the user owns/wants. We
// pull master_ids when present (most releases carry them), plus the
// release ids themselves so a release-typed suggestion that matches
// is also dropped.
export async function getUserLibraryMasterIds(clerkUserId: string): Promise<Set<number>> {
  const out = new Set<number>();
  try {
    const r = await getPool().query(
      `SELECT DISTINCT (data->>'master_id')::int AS m
         FROM (
           SELECT data FROM user_collection WHERE clerk_user_id = $1
           UNION ALL
           SELECT data FROM user_wantlist   WHERE clerk_user_id = $1
           UNION ALL
           SELECT data FROM user_inventory  WHERE clerk_user_id = $1
         ) s
        WHERE NULLIF(s.data->>'master_id','') IS NOT NULL`,
      [clerkUserId]
    );
    for (const row of r.rows) {
      const m = Number(row.m);
      if (Number.isFinite(m) && m > 0) out.add(m);
    }
  } catch { /* best-effort */ }
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
export async function mergeUserPersonalSuggestions(
  clerkUserId: string,
  items: Array<{ id: number; type: "master" | "release"; score: number; data: any }>,
  opts?: { excludeKeys?: Set<string>; maxRows?: number },
): Promise<{ added: Array<{ discogs_id: number; entity_type: "master" | "release" }>; suppressed: number }> {
  const exclude = opts?.excludeKeys ?? new Set<string>();
  const maxRows = Math.max(50, Math.min(2000, opts?.maxRows ?? 1000));
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    // Suppress excluded keys (e.g. recently-clicked) from any existing
    // rows. They might have been added in a previous pass before the
    // user opened them.
    if (exclude.size) {
      const arr = Array.from(exclude);
      const ids   = arr.map(k => Number(k.split(":")[1])).filter(n => Number.isFinite(n));
      const types = arr.map(k => k.split(":")[0]);
      if (ids.length) {
        await client.query(
          `DELETE FROM user_personal_suggestions
            WHERE clerk_user_id = $1
              AND (discogs_id, entity_type) IN (
                SELECT * FROM unnest($2::int[], $3::text[])
              )`,
          [clerkUserId, ids, types],
        );
      }
    }
    // Pull existing keys so we can tell new rows from updates.
    const existingRows = await client.query(
      `SELECT discogs_id, entity_type FROM user_personal_suggestions WHERE clerk_user_id = $1`,
      [clerkUserId],
    );
    const existing = new Set<string>(existingRows.rows.map(r => `${r.entity_type}:${r.discogs_id}`));
    const added: Array<{ discogs_id: number; entity_type: "master" | "release" }> = [];
    let suppressed = 0;
    for (const it of items) {
      const key = `${it.type}:${it.id}`;
      if (exclude.has(key)) { suppressed++; continue; }
      const isNew = !existing.has(key);
      await client.query(
        `INSERT INTO user_personal_suggestions
           (clerk_user_id, discogs_id, entity_type, score, data, generated_at)
         VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
         ON CONFLICT (clerk_user_id, discogs_id, entity_type) DO UPDATE
           SET score = EXCLUDED.score, data = EXCLUDED.data`,  // generated_at intentionally NOT touched on conflict
        [clerkUserId, it.id, it.type, it.score, JSON.stringify(it.data ?? {})],
      );
      if (isNew) added.push({ discogs_id: it.id, entity_type: it.type });
    }
    // Cap row count: drop the lowest-score rows past maxRows so the
    // table doesn't grow unbounded over many merges.
    await client.query(
      `DELETE FROM user_personal_suggestions
        WHERE clerk_user_id = $1
          AND (discogs_id, entity_type) NOT IN (
            SELECT discogs_id, entity_type FROM user_personal_suggestions
             WHERE clerk_user_id = $1
             ORDER BY score DESC, generated_at DESC
             LIMIT $2
          )`,
      [clerkUserId, maxRows],
    );
    await client.query("COMMIT");
    return { added, suppressed };
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

// Wipe + insert: the job overwrites the user's previous batch each
// run so the row count never grows beyond N per user. data is the
// card snapshot so the UI can render without further Discogs calls.
export async function replaceUserPersonalSuggestions(
  clerkUserId: string,
  items: Array<{ id: number; type: "master" | "release"; score: number; data: any }>
): Promise<void> {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `DELETE FROM user_personal_suggestions WHERE clerk_user_id = $1`,
      [clerkUserId]
    );
    for (const it of items) {
      await client.query(
        `INSERT INTO user_personal_suggestions
           (clerk_user_id, discogs_id, entity_type, score, data, generated_at)
         VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
         ON CONFLICT (clerk_user_id, discogs_id, entity_type) DO UPDATE
           SET score = EXCLUDED.score, data = EXCLUDED.data, generated_at = NOW()`,
        [clerkUserId, it.id, it.type, it.score, JSON.stringify(it.data ?? {})]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

// Dismissals ("banish this suggestion forever") — record per-user so
// the generator skips them and any saved row is wiped immediately.
export async function dismissPersonalSuggestion(
  clerkUserId: string,
  discogsId: number,
  entityType: "master" | "release"
): Promise<void> {
  await getPool().query(
    `INSERT INTO user_suggestion_dismissals (clerk_user_id, discogs_id, entity_type)
     VALUES ($1, $2, $3)
     ON CONFLICT (clerk_user_id, discogs_id, entity_type) DO NOTHING`,
    [clerkUserId, discogsId, entityType]
  );
  // Also remove from the saved batch so the card disappears on next render.
  await getPool().query(
    `DELETE FROM user_personal_suggestions
       WHERE clerk_user_id = $1 AND discogs_id = $2 AND entity_type = $3`,
    [clerkUserId, discogsId, entityType]
  );
}

// Returns set of "type:id" strings the user has banished so the
// generator can skip them in O(1). `withinDays` constrains to recent
// dismissals only — caller passes e.g. 90 to let older dismissals
// expire so the pool can thaw over time. Default unbounded preserves
// existing behaviour for any other caller.
export async function getDismissedSuggestionKeys(
  clerkUserId: string,
  withinDays?: number,
): Promise<Set<string>> {
  const out = new Set<string>();
  try {
    const params: any[] = [clerkUserId];
    let where = "clerk_user_id = $1";
    if (Number.isFinite(withinDays as number) && (withinDays as number) > 0) {
      const d = Math.max(1, Math.min(3650, Math.trunc(withinDays as number)));
      // Inline the integer (already clamped) — Postgres can't bind
      // intervals through parameters cleanly.
      where += ` AND dismissed_at > NOW() - INTERVAL '${d} days'`;
    }
    const r = await getPool().query(
      `SELECT discogs_id, entity_type FROM user_suggestion_dismissals WHERE ${where}`,
      params,
    );
    for (const row of r.rows) out.add(`${row.entity_type}:${row.discogs_id}`);
  } catch { /* best effort */ }
  return out;
}

export async function getUserPersonalSuggestions(
  clerkUserId: string,
  limit = 1000
): Promise<any[]> {
  try {
    const r = await getPool().query(
      `SELECT discogs_id, entity_type, score, data, generated_at
         FROM user_personal_suggestions
        WHERE clerk_user_id = $1
        ORDER BY score DESC, generated_at DESC
        LIMIT $2`,
      [clerkUserId, Math.max(1, Math.min(1000, limit))]
    );
    return r.rows;
  } catch { return []; }
}

// ── cache_fetch_queue helpers ─────────────────────────────────────
//
// Single chokepoint for "I want this album cached but I don't want
// to wait on a Discogs fetch." Anyone who wants an album cached
// drops a row in via enqueueCacheFetches; the rate-limited worker
// drains it. Dedupe is automatic via the unique constraint on
// (entity_type, discogs_id).

export async function enqueueCacheFetches(
  refs: Array<{ entity_type: "master" | "release"; discogs_id: number }>,
  source: string = "unknown",
  priority: number = 0,
): Promise<number> {
  if (!Array.isArray(refs) || !refs.length) return 0;
  // Skip rows already in release_cache — no point queueing a fetch
  // for something we already have. Doing this filter in SQL beats
  // round-tripping each id individually.
  const types = refs.map(r => r.entity_type);
  const ids   = refs.map(r => Number(r.discogs_id));
  // INSERT ... SELECT ... LEFT JOIN release_cache so already-cached
  // rows skip insertion. ON CONFLICT bumps priority+source if the
  // new request is higher priority — useful for "user just clicked
  // this from search results, prioritize the prefetch."
  const r = await getPool().query(
    `INSERT INTO cache_fetch_queue (entity_type, discogs_id, source, priority)
     SELECT u.entity_type, u.discogs_id, $3, $4
       FROM unnest($1::text[], $2::int[]) AS u(entity_type, discogs_id)
       LEFT JOIN release_cache rc
         ON rc.discogs_id = u.discogs_id AND rc.type = u.entity_type
      WHERE rc.discogs_id IS NULL
     ON CONFLICT (entity_type, discogs_id) DO UPDATE
       SET priority = GREATEST(cache_fetch_queue.priority, EXCLUDED.priority),
           source   = CASE WHEN EXCLUDED.priority > cache_fetch_queue.priority
                            THEN EXCLUDED.source
                            ELSE cache_fetch_queue.source END`,
    [types, ids, source, priority],
  );
  return r.rowCount ?? 0;
}

// Pull the next batch of items to fetch. Highest priority first,
// oldest-requested-first within priority. Caller should fetch each
// and call markCacheFetchSucceeded / markCacheFetchFailed.
export async function dequeueCacheFetches(limit = 50): Promise<Array<{
  id: number; entity_type: "master" | "release"; discogs_id: number; source: string; attempts: number;
}>> {
  const r = await getPool().query(
    `SELECT id, entity_type, discogs_id, source, attempts
       FROM cache_fetch_queue
      ORDER BY priority DESC, requested_at ASC
      LIMIT $1`,
    [Math.max(1, Math.min(1000, limit))],
  );
  return r.rows;
}

export async function markCacheFetchSucceeded(id: number): Promise<void> {
  await getPool().query(`DELETE FROM cache_fetch_queue WHERE id = $1`, [id]);
}

const _CACHE_FETCH_MAX_ATTEMPTS = 5;

// Increment attempts; drop the row once it's failed too many times
// so the queue doesn't accumulate permanent stuck entries (e.g.
// deleted Discogs IDs that always 404). Returns true if the row
// was kept, false if dropped.
export async function markCacheFetchFailed(id: number, error: string): Promise<boolean> {
  const r = await getPool().query(
    `UPDATE cache_fetch_queue
        SET attempts   = attempts + 1,
            last_error = $2
      WHERE id = $1
      RETURNING attempts`,
    [id, error.slice(0, 500)],
  );
  const attempts = r.rows[0]?.attempts ?? 0;
  if (attempts >= _CACHE_FETCH_MAX_ATTEMPTS) {
    await getPool().query(`DELETE FROM cache_fetch_queue WHERE id = $1`, [id]);
    return false;
  }
  return true;
}

export async function getCacheFetchQueueStats(): Promise<{
  total: number; oldest_requested_at: string | null;
  by_source: Array<{ source: string; n: number }>;
}> {
  const totalRow = await getPool().query(
    `SELECT COUNT(*)::int AS total, MIN(requested_at) AS oldest_requested_at
       FROM cache_fetch_queue`,
  );
  const sourceRows = await getPool().query(
    `SELECT source, COUNT(*)::int AS n
       FROM cache_fetch_queue
      GROUP BY source ORDER BY n DESC`,
  );
  return {
    total: totalRow.rows[0]?.total ?? 0,
    oldest_requested_at: totalRow.rows[0]?.oldest_requested_at ?? null,
    by_source: sourceRows.rows,
  };
}

// ── Behavior-event logging ─────────────────────────────────────────
// Append-only writes — best-effort, never block a real request on
// the log write. Caller passes through `.catch(() => {})`.

export async function logUserSearch(clerkUserId: string, query: string): Promise<void> {
  if (!clerkUserId) return;
  await getPool().query(
    `INSERT INTO user_search_events (clerk_user_id, query) VALUES ($1, $2)`,
    [clerkUserId, (query || "").slice(0, 200)],
  );
}

export async function logUserPlay(
  clerkUserId: string,
  source: "yt" | "loc" | "archive",
  externalId: string,
  title?: string,
  ident?: {
    releaseType?: "release" | "master" | null;
    releaseId?: number | null;
    masterId?: number | null;
    // Optional client snapshot. Stored verbatim and read by the taste
    // query when present; only genres/styles/year are actually used.
    meta?: { genres?: string[]; styles?: string[]; year?: number | null } | null;
  },
): Promise<void> {
  if (!clerkUserId || !externalId) return;
  const relType = ident?.releaseType === "release" || ident?.releaseType === "master"
    ? ident.releaseType : null;
  const relId  = Number.isFinite(ident?.releaseId as number) && (ident!.releaseId as number) > 0
    ? Math.trunc(ident!.releaseId as number) : null;
  const masId  = Number.isFinite(ident?.masterId as number) && (ident!.masterId as number) > 0
    ? Math.trunc(ident!.masterId as number) : null;
  // Keep only the three fields the taste expansion needs; drop anything
  // else the client may have sent so the JSONB stays small.
  let meta: { genres: string[]; styles: string[]; year: number | null } | null = null;
  if (ident?.meta && typeof ident.meta === "object") {
    const g = Array.isArray(ident.meta.genres) ? ident.meta.genres.slice(0, 12).map(String) : [];
    const s = Array.isArray(ident.meta.styles) ? ident.meta.styles.slice(0, 24).map(String) : [];
    const y = Number.isFinite(ident.meta.year as number) ? Number(ident.meta.year) : null;
    if (g.length || s.length || y) meta = { genres: g, styles: s, year: y };
  }
  await getPool().query(
    `INSERT INTO user_play_events
       (clerk_user_id, source, external_id, title, release_type, release_id, master_id, play_meta)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [
      clerkUserId,
      source,
      String(externalId).slice(0, 100),
      (title || "").slice(0, 200) || null,
      relType,
      relId,
      masId,
      meta ? JSON.stringify(meta) : null,
    ],
  );
}

// Per-user behavior summary for the admin panel. Aggregates across
// existing tables (favorites, recent_views, suggestions) and the new
// event tables (searches, plays). Window is configurable; default is
// "all time" + last 30 days side by side.
export async function getUserBehaviorStats(): Promise<Array<{
  clerk_user_id: string;
  username: string | null;
  favorites:               number;
  suggestions_pool:        number;
  suggestions_favorited:   number;
  album_clicks_total:      number;
  album_clicks_30d:        number;
  player_plays_total:      number;
  player_plays_30d:        number;
  searches_total:          number;
  searches_30d:            number;
  last_active:             string | null;
}>> {
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
      SELECT clerk_user_id, discogs_username, last_active_at FROM user_tokens
      UNION
      SELECT DISTINCT a.clerk_user_id, NULL::text, NULL::timestamptz
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
           u.last_active_at        AS last_active
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
export async function getRecentlyClickedSuggestionKeys(
  clerkUserId: string, days = 30,
): Promise<Set<string>> {
  const out = new Set<string>();
  try {
    const r = await getPool().query(
      `SELECT discogs_id, entity_type
         FROM user_recent_views
        WHERE clerk_user_id = $1
          AND opened_at >= NOW() - INTERVAL '${Math.max(1, Math.min(365, days))} days'
          AND entity_type IN ('master','release')`,
      [clerkUserId],
    );
    for (const row of r.rows) {
      out.add(`${row.entity_type}:${row.discogs_id}`);
    }
  } catch { /* table may not exist on cold install — best effort */ }
  return out;
}

// Suggestions cache-warm: list every (discogs_id, entity_type) pair
// that appears in user_personal_suggestions but is NOT yet in
// release_cache. The nightly cache-warm job uses this to know what
// to fetch from Discogs. Deduped across all users (one fetch covers
// every user who has the same suggestion). Sorted oldest-suggestion-
// first so newer suggestions wait their turn — fairness across users.
export async function getUncachedSuggestionRefs(
  limit = 5000,
): Promise<Array<{ discogs_id: number; entity_type: "master" | "release" }>> {
  const r = await getPool().query(
    `SELECT s.discogs_id, s.entity_type
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
      LIMIT $1`,
    [Math.max(1, Math.min(50000, limit))],
  );
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
export async function reportYoutubeVideoUnavailable(
  videoId: string,
  reporterUserId: string | null,
  errorCode: number | null
): Promise<{ status: string; report_count: number }> {
  if (!/^[A-Za-z0-9_-]{11}$/.test(videoId)) {
    return { status: "invalid", report_count: 0 };
  }
  const r = await getPool().query(
    `INSERT INTO youtube_video_unavailable
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
     RETURNING status, report_count`,
    [videoId, reporterUserId, errorCode, _YT_UNAVAILABLE_THRESHOLD]
  );
  return r.rows[0] ?? { status: "unknown", report_count: 0 };
}

// Set of videoIds whose status is 'unavailable' (above threshold).
// Used by the renderer to filter out broken videos from album popups
// so users see them as "missing" and can submit replacements.
export async function getUnavailableYoutubeVideoIds(): Promise<Set<string>> {
  const out = new Set<string>();
  try {
    const r = await getPool().query(
      `SELECT video_id FROM youtube_video_unavailable WHERE status = 'unavailable'`
    );
    for (const row of r.rows) out.add(row.video_id);
  } catch { /* best-effort */ }
  return out;
}

// Admin: list every flagged + unavailable entry for the admin tab.
// Newest reports first so admin sees recent issues at the top.
export async function listYoutubeVideoUnavailable(limit = 500): Promise<any[]> {
  try {
    const r = await getPool().query(
      `SELECT u.video_id, u.status, u.report_count,
              u.first_reported_at, u.last_reported_at,
              u.sample_user_id, u.sample_error_code,
              ov.release_type, ov.release_id,
              ov.track_title, ov.track_position
         FROM youtube_video_unavailable u
         LEFT JOIN LATERAL (
           SELECT release_type, release_id, track_title, track_position
             FROM track_youtube_overrides o
            WHERE o.video_id = u.video_id
            ORDER BY o.submitted_at DESC
            LIMIT 1
         ) ov ON true
        ORDER BY u.last_reported_at DESC
        LIMIT $1`,
      [Math.max(1, Math.min(2000, limit))]
    );
    return r.rows;
  } catch { return []; }
}

// Admin: clear a single videoId from the unavailable list (e.g. when
// a video came back online). Removes the row entirely so a future
// report starts fresh from count 1.
export async function clearYoutubeVideoUnavailable(videoId: string): Promise<boolean> {
  try {
    const r = await getPool().query(
      `DELETE FROM youtube_video_unavailable WHERE video_id = $1`,
      [videoId]
    );
    return (r.rowCount ?? 0) > 0;
  } catch { return false; }
}

// ── YouTube search cache (DB-backed) ────────────────────────────────
// In-memory cache in search-api.ts evaporates on every Railway
// restart. Mirror the same query results to a durable row so we
// don't rebuild the cache from quota-paid hits each deploy.

export async function getYoutubeSearchCache(cacheKey: string, maxAgeSeconds: number): Promise<any | null> {
  try {
    const r = await getPool().query(
      `SELECT body, cached_at FROM youtube_search_cache WHERE cache_key = $1 LIMIT 1`,
      [cacheKey]
    );
    const row = r.rows[0];
    if (!row) return null;
    const ageMs = Date.now() - new Date(row.cached_at).getTime();
    if (ageMs > maxAgeSeconds * 1000) return null;
    return row.body ?? null;
  } catch { return null; }
}

// Last time we cached a result for this query, regardless of TTL.
// Used to surface a "last searched <time> ago" hover hint on the
// external-YouTube fallback link when the popup couldn't fetch
// fresh results (quota error, etc.).
export async function getYoutubeSearchCacheTimestamp(cacheKey: string): Promise<Date | null> {
  try {
    const r = await getPool().query(
      `SELECT cached_at FROM youtube_search_cache WHERE cache_key = $1 LIMIT 1`,
      [cacheKey]
    );
    const ca = r.rows[0]?.cached_at;
    return ca ? new Date(ca) : null;
  } catch { return null; }
}

export async function setYoutubeSearchCache(cacheKey: string, body: any): Promise<void> {
  try {
    await getPool().query(
      `INSERT INTO youtube_search_cache (cache_key, body, cached_at)
       VALUES ($1, $2::jsonb, NOW())
       ON CONFLICT (cache_key) DO UPDATE SET body = EXCLUDED.body, cached_at = NOW()`,
      [cacheKey, JSON.stringify(body)]
    );
  } catch { /* cache is best-effort */ }
}

// Periodic prune of stale rows so the table stays bounded. Anything
// older than 7 days is dropped — well past the 24h read TTL.
export async function pruneYoutubeSearchCache(): Promise<number> {
  try {
    const r = await getPool().query(
      `DELETE FROM youtube_search_cache WHERE cached_at < NOW() - INTERVAL '7 days'`
    );
    return r.rowCount ?? 0;
  } catch { return 0; }
}

// ── archive.org search cache helpers ───────────────────────────────
// Mirrors the youtube_search_cache helpers above. Long-lived 90-day
// TTL — archive.org's catalog is extremely stable, and we want to
// avoid burning any goodwill with their public search API. Cache key
// is a normalized "q|page|rows" string built by the route handler.

export async function getArchiveSearchCache(cacheKey: string, maxAgeSeconds: number): Promise<any | null> {
  try {
    const r = await getPool().query(
      `SELECT body, cached_at FROM archive_search_cache WHERE cache_key = $1 LIMIT 1`,
      [cacheKey]
    );
    const row = r.rows[0];
    if (!row) return null;
    const ageMs = Date.now() - new Date(row.cached_at).getTime();
    if (ageMs > maxAgeSeconds * 1000) return null;
    return row.body ?? null;
  } catch { return null; }
}

export async function setArchiveSearchCache(cacheKey: string, body: any): Promise<void> {
  try {
    await getPool().query(
      `INSERT INTO archive_search_cache (cache_key, body, cached_at)
       VALUES ($1, $2::jsonb, NOW())
       ON CONFLICT (cache_key) DO UPDATE SET body = EXCLUDED.body, cached_at = NOW()`,
      [cacheKey, JSON.stringify(body)]
    );
  } catch { /* cache is best-effort */ }
}

// Drop rows past the 90-day TTL. Wired to the existing prune
// scheduler (or run manually). Returns # of rows deleted.
export async function pruneArchiveSearchCache(): Promise<number> {
  try {
    const r = await getPool().query(
      `DELETE FROM archive_search_cache WHERE cached_at < NOW() - INTERVAL '90 days'`
    );
    return r.rowCount ?? 0;
  } catch { return 0; }
}

// Admin DB tab: per-table summary popup data. Pure introspection
// against information_schema + pg_indexes + pg_total_relation_size —
// no application data leaves this function. Caller is responsible for
// whitelisting the table name (it's interpolated for pg_total_relation_size
// since that's a function call, not a query target — but is also
// passed as a parameter to information_schema queries so even an
// injection slip would only affect the size query).
export async function getDbAdminTableSummary(tableName: string): Promise<{
  table: string;
  rowCount: number;
  totalSizeBytes: number;
  columns: Array<{ name: string; type: string; nullable: boolean; default: string | null }>;
  indexes: Array<{ name: string; definition: string }>;
}> {
  // Defensive: refuse any name that's not a-z/0-9/underscore even
  // though callers should already whitelist via getTableRowCounts.
  if (!/^[a-z_][a-z0-9_]*$/i.test(tableName)) {
    throw new Error("Invalid table name");
  }
  const [colsR, idxR, cntR, sizeR] = await Promise.all([
    getPool().query(
      `SELECT column_name AS name,
              data_type   AS type,
              is_nullable = 'YES' AS nullable,
              column_default AS "default"
         FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = $1
        ORDER BY ordinal_position`,
      [tableName]
    ),
    getPool().query(
      `SELECT indexname AS name, indexdef AS definition
         FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = $1
        ORDER BY indexname`,
      [tableName]
    ),
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
export async function getPersonalSuggestionsStats(): Promise<Array<{
  clerkUserId: string;
  count: number;
  lastGeneratedAt: Date | null;
}>> {
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
  } catch { return []; }
}

// Admin: list every override for the audit/admin tab. Newest first.
export async function listAllTrackYtOverrides(limit = 500): Promise<TrackYtOverride[]> {
  try {
    const r = await getPool().query(
      `SELECT release_id, release_type, track_position, track_title,
              video_id, video_title, submitted_by, submitted_at
         FROM track_youtube_overrides
        ORDER BY submitted_at DESC
        LIMIT $1`,
      [limit]
    );
    return r.rows as TrackYtOverride[];
  } catch { return []; }
}


// ── Lyrics helpers (scraped from weeniecampbell.com) ─────────────────
// upsertLyric is keyed on (source_host, page_title) so a re-scrape
// updates existing rows instead of duplicating.
export async function upsertLyric(record: {
  pageTitle: string;
  pageUrl: string;
  artist?: string | null;
  tuning?: string | null;
  wikitext?: string | null;
  plaintext?: string | null;
  sourceHost?: string;
}): Promise<void> {
  const host = record.sourceHost || "weeniecampbell.com";
  // ON CONFLICT target matches the partial unique index defined in
  // initDb (source_host, page_title, COALESCE(LOWER(TRIM(artist)), '')).
  // Re-scraping the same page still upserts (artist matches itself),
  // but a manual add with a different artist on the same title goes
  // through as a fresh INSERT.
  await getPool().query(
    `INSERT INTO blues_lyrics (source_host, page_title, page_url, artist, tuning, wikitext, plaintext, scraped_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
     ON CONFLICT (source_host, page_title, (COALESCE(LOWER(TRIM(artist)), '')))
     DO UPDATE SET page_url = EXCLUDED.page_url,
                   tuning = EXCLUDED.tuning,
                   wikitext = EXCLUDED.wikitext,
                   plaintext = EXCLUDED.plaintext,
                   scraped_at = NOW()`,
    [host, record.pageTitle, record.pageUrl, record.artist ?? null, record.tuning ?? null, record.wikitext ?? null, record.plaintext ?? null],
  );
}

// Manual lyric insert — used by the admin "+ Add lyric" affordance.
// Returns the new row. page_title + artist together are unique per
// source_host, so adding a duplicate (same title + same artist) on
// the same source throws unique_violation (23505) which the caller
// surfaces as 409.
export async function createLyric(record: {
  pageTitle: string;
  pageUrl?: string | null;
  artist?: string | null;
  tuning?: string | null;
  plaintext?: string | null;
  wikitext?: string | null;
  sourceHost?: string;
  artistId?: number | null;
  discogsReleaseId?: number | null;
  discogsMasterId?: number | null;
  firstReleaseYear?: number | null;
}): Promise<any> {
  const host = record.sourceHost || "manual";
  const title = String(record.pageTitle || "").trim();
  if (!title) throw new Error("page_title required");
  // Auto-resolve artist_id from name when not explicitly provided.
  let artistId = record.artistId ?? null;
  if (artistId == null && record.artist) {
    const r = await getPool().query(
      `SELECT id FROM blues_artists WHERE LOWER(name) = LOWER(TRIM($1)) LIMIT 1`,
      [record.artist],
    );
    if (r.rows.length) artistId = r.rows[0].id;
  }
  const fy = record.firstReleaseYear != null && Number.isFinite(Number(record.firstReleaseYear))
    ? Number(record.firstReleaseYear) : null;
  const ins = await getPool().query(
    `INSERT INTO blues_lyrics
       (source_host, page_title, page_url, artist, artist_id, tuning,
        wikitext, plaintext, discogs_release_id, discogs_master_id,
        first_release_year, first_release_source, first_release_checked_at,
        scraped_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
             $11, CASE WHEN $11::int IS NULL THEN NULL ELSE 'manual' END,
             CASE WHEN $11::int IS NULL THEN NULL ELSE NOW() END,
             NOW())
     RETURNING *`,
    [host, title, record.pageUrl ?? null, record.artist ?? null, artistId,
     record.tuning ?? null, record.wikitext ?? null, record.plaintext ?? null,
     record.discogsReleaseId ?? null, record.discogsMasterId ?? null,
     fy],
  );
  return ins.rows[0];
}

// Set of (source_host, page_title) keys we've already scraped — so a
// resumed scrape skips them. Returns lowercased titles in a Set for
// fast probing.
export async function getLyricTitlesAlreadyScraped(sourceHost = "weeniecampbell.com"): Promise<Set<string>> {
  try {
    const r = await getPool().query(
      `SELECT page_title FROM blues_lyrics WHERE source_host = $1`,
      [sourceHost],
    );
    return new Set(r.rows.map((row: any) => String(row.page_title)));
  } catch { return new Set(); }
}

export async function getLyricById(id: number): Promise<any | null> {
  const r = await getPool().query(`SELECT * FROM blues_lyrics WHERE id = $1`, [id]);
  return r.rows[0] || null;
}

// Whitelist of columns allowed for the sort= URL param. Anything else
// (or unset) falls back to page_title. Keeps the SQL injection-safe
// while letting the client header click drive ORDER BY.
const _LYRICS_SORT_COLS: Record<string, string> = {
  page_title:         "page_title",
  artist:             "artist",
  tuning:             "tuning",
  scraped_at:         "scraped_at",
  updated_at:         "updated_at",
  first_release_year: "first_release_year",
};

export async function listLyrics(opts: {
  search?: string;
  tuning?: string;
  artist?: string;
  unmatchedOnly?: boolean;
  /** True → only return rows where BOTH discogs_release_id and
   *  discogs_master_id are NULL (no release pin at all). Lets the
   *  curator find lyrics that still need a Discogs release linked. */
  unpinnedOnly?: boolean;
  /** True → only return rows whose plaintext body is empty / NULL.
   *  Used to surface lyric titles awaiting their text body so the
   *  curator can paste it in via the editor. */
  emptyOnly?: boolean;
  sort?: string;
  order?: "asc" | "desc";
  limit?: number;
  offset?: number;
}): Promise<{ rows: any[]; total: number }> {
  const where: string[] = [];
  const params: any[] = [];
  if (opts.search) {
    params.push(`%${opts.search}%`);
    where.push(`(page_title ILIKE $${params.length} OR artist ILIKE $${params.length} OR plaintext ILIKE $${params.length})`);
  }
  if (opts.tuning) {
    // Special sentinel: "(unspecified)" filters rows where no tuning
    // was extracted from the page body. On a blues-lyrics wiki, an
    // unmentioned tuning effectively means standard, so this is the
    // catch-all for "standard tuning" pages that don't say so
    // explicitly.
    if (opts.tuning === "(unspecified)") {
      where.push(`tuning IS NULL`);
    } else {
      params.push(opts.tuning);
      where.push(`tuning = $${params.length}`);
    }
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
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const limit = Math.max(1, Math.min(500, opts.limit ?? 100));
  const offset = Math.max(0, opts.offset ?? 0);
  // Sort column: whitelist-mapped to defeat SQL injection. NULLs LAST
  // so empty-tuning rows don't dominate a "Tuning ASC" sort — the
  // most-common-by-far value otherwise crowds the visible page out.
  const sortCol = _LYRICS_SORT_COLS[String(opts.sort ?? "")] || "page_title";
  const order   = opts.order === "desc" ? "DESC" : "ASC";
  // Secondary sort by page_title for stable ordering within ties.
  const orderSql = sortCol === "page_title"
    ? `ORDER BY page_title ${order}`
    : `ORDER BY ${sortCol} ${order} NULLS LAST, page_title ASC`;
  const totalRow = await getPool().query(`SELECT COUNT(*)::int AS n FROM blues_lyrics ${whereSql}`, params);
  const total = totalRow.rows[0]?.n ?? 0;
  params.push(limit, offset);
  const rowsRes = await getPool().query(
    `SELECT id, page_title, page_url, artist, artist_id, tuning,
            discogs_release_id, discogs_master_id,
            first_release_year, first_release_source,
            scraped_at, updated_at,
            substring(plaintext, 1, 240) AS snippet
       FROM blues_lyrics ${whereSql}
       ${orderSql}
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
    params,
  );
  return { rows: rowsRes.rows, total };
}

export async function getLyricTunings(): Promise<Array<{ tuning: string; n: number }>> {
  const r = await getPool().query(
    `SELECT tuning, COUNT(*)::int AS n
       FROM blues_lyrics
      WHERE tuning IS NOT NULL AND tuning <> ''
      GROUP BY tuning
      ORDER BY n DESC, tuning ASC`,
  );
  // Append a virtual (unspecified) entry counting rows where no
  // tuning was extracted — almost all of these are standard tuning
  // on a blues-lyrics wiki, so the dropdown surfaces it as a usable
  // filter. Pinned to the bottom of the list regardless of count.
  const nullR = await getPool().query(
    `SELECT COUNT(*)::int AS n FROM blues_lyrics WHERE tuning IS NULL OR tuning = ''`,
  );
  const nullCount = nullR.rows[0]?.n ?? 0;
  const rows = r.rows as Array<{ tuning: string; n: number }>;
  if (nullCount > 0) rows.push({ tuning: "(unspecified)", n: nullCount });
  return rows;
}

export async function getLyricCount(): Promise<number> {
  const r = await getPool().query(`SELECT COUNT(*)::int AS n FROM blues_lyrics`);
  return r.rows[0]?.n ?? 0;
}



// ── Blues archive (unified artist + lyrics + releases view) ──────────
// Powers the admin Discovery sub-view that combines blues_artists,
// blues_lyrics, and the JSONB releases array into a single per-artist
// page. Matching of lyrics → artist is case-insensitive on name.

// Walks distinct lyrics.artist values, finds those that don't already
// exist as a blues_artists.name (case-insensitive), and inserts each
// as a minimal new row (name only). Returns { added, total, existing }
// so the UI can report what happened.
export async function importLyricsArtistsToBluesDb(
  validate?: (name: string) => boolean,
): Promise<{ added: number; total: number; existing: number; rejected: number }> {
  // Distinct non-null lyric-artist names. Trim + dedupe in JS so we
  // can match against the (lowercased) existing set in one pass.
  const rawNamesR = await getPool().query(
    `SELECT DISTINCT TRIM(artist) AS artist
       FROM blues_lyrics
      WHERE artist IS NOT NULL AND TRIM(artist) <> ''`,
  );
  const rawCandidates = (rawNamesR.rows as Array<{ artist: string }>)
    .map(r => r.artist.replace(/\s+/g, " ").trim())
    .filter(s => s.length >= 2 && s.length <= 200);
  // Validator pass — applied at IMPORT time as well as extraction so
  // stale bad values left in blues_lyrics.artist (from old scrapes
  // before the validator was added) don't leak into blues_artists.
  // Without this, "1934 version" / "alternate take" / catalog
  // numbers etc. would still get imported as artists.
  const candidates: string[] = [];
  let rejected = 0;
  for (const c of rawCandidates) {
    if (validate && !validate(c)) { rejected++; continue; }
    candidates.push(c);
  }
  const existingR = await getPool().query(`SELECT LOWER(name) AS lname FROM blues_artists`);
  const existing = new Set((existingR.rows as Array<{ lname: string }>).map(r => r.lname));
  // De-dupe locally + filter against the existing set so the bulk
  // INSERT below only carries names that genuinely need adding.
  const toAdd: string[] = [];
  const localSeen = new Set<string>();
  let already = 0;
  for (const name of candidates) {
    const key = name.toLowerCase();
    if (existing.has(key)) { already++; continue; }
    if (localSeen.has(key)) continue;
    localSeen.add(key);
    toAdd.push(name);
  }
  // Bulk INSERT via unnest — one round-trip regardless of size.
  // Previous version did one INSERT per name which 60s-timed-out
  // once the candidate set got into the low thousands.
  let added = 0;
  if (toAdd.length) {
    const r = await getPool().query(
      `INSERT INTO blues_artists (name, enrichment_status)
       SELECT n, '{"source":"lyrics_import"}'::jsonb FROM unnest($1::text[]) AS n
       RETURNING id`,
      [toAdd],
    );
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
const _BLUES_ARCHIVE_SORT_COLS: Record<string, string> = {
  name:           "a.name",
  discogs_id:     "a.discogs_id",
  releases_count: "COALESCE(jsonb_array_length(a.discogs_releases), 0)",
  lyrics_count:   `(SELECT COUNT(*)::int FROM blues_lyrics l
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
  has_photo: "CASE WHEN COALESCE(a.photo_url, '') <> '' THEN 1 ELSE 0 END",
};

export async function listBluesArchive(opts: {
  search?: string;
  sort?: string;
  order?: "asc" | "desc";
  // Optional category filter — used by the stats-strip chips.
  //   "with_both"          → artists with lyrics AND releases
  //   "with_lyrics_only"   → lyrics but no releases
  //   "with_releases_only" → releases but no lyrics
  //   "empty"              → neither
  category?: "with_both" | "with_lyrics_only" | "with_releases_only" | "empty";
  limit?: number;
  offset?: number;
} = {}): Promise<{ rows: Array<any>; total: number }> {
  const params: any[] = [];
  const where: string[] = [];
  if (opts.search) {
    params.push(`%${opts.search}%`);
    where.push(`(a.name ILIKE $${params.length})`);
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
    if      (opts.category === "with_both")          where.push(`${HAS_LYRICS} AND ${HAS_RELEASES}`);
    else if (opts.category === "with_lyrics_only")   where.push(`${HAS_LYRICS} AND NOT ${HAS_RELEASES}`);
    else if (opts.category === "with_releases_only") where.push(`NOT ${HAS_LYRICS} AND ${HAS_RELEASES}`);
    else if (opts.category === "empty")              where.push(`NOT ${HAS_LYRICS} AND NOT ${HAS_RELEASES}`);
  }
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const limit = Math.max(1, Math.min(500, opts.limit ?? 100));
  const offset = Math.max(0, opts.offset ?? 0);
  // Whitelist sort column + direction. Secondary sort by name keeps
  // ordering stable within ties (e.g. two artists with same lyrics
  // count). NULLS LAST on numeric/id columns so empty discogs_id
  // rows fall to the bottom instead of crowding the top.
  const sortCol = _BLUES_ARCHIVE_SORT_COLS[String(opts.sort ?? "")] || "a.name";
  const order   = opts.order === "desc" ? "DESC" : "ASC";
  const orderSql = sortCol === "a.name"
    ? `ORDER BY a.name ${order}`
    : `ORDER BY ${sortCol} ${order} NULLS LAST, a.name ASC`;
  const totalR = await getPool().query(
    `SELECT COUNT(*)::int AS n FROM blues_artists a ${whereSql}`,
    params,
  );
  const total = totalR.rows[0]?.n ?? 0;
  params.push(limit, offset);
  const r = await getPool().query(
    `SELECT a.id,
            a.name,
            a.birth_date,
            a.death_date,
            a.photo_url,
            a.discogs_id,
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
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
    params,
  );
  return { rows: r.rows, total };
}

// ── Blues Archive: flat releases list ─────────────────────────────────
// Unnests every blues_artists.discogs_releases JSONB array into a flat
// per-release row joined back to the source artist. Powers the Releases
// sub-tab on the Discovery Blues Archive view so a curator can browse,
// filter, and sort the catalog without bouncing through artist popups.
const _BLUES_REL_SORT_COLS: Record<string, string> = {
  // Display title comes from the JSONB blob.
  title:        "lower(coalesce(rel->>'title',''))",
  // Cast year text to int so 1928 < 1932 sorts numerically.
  year:         "NULLIF(rel->>'year','')::int",
  artist:       "lower(a.name)",
  type:         "lower(coalesce(rel->>'type',''))",
  role:         "lower(coalesce(rel->>'role',''))",
};
export async function listBluesArchiveReleases(opts: {
  search?: string;
  artist?: string;
  year?: number | null;
  type?: string;
  role?: string;
  sort?: string;
  order?: "asc" | "desc";
  limit?: number;
  offset?: number;
} = {}): Promise<{ rows: any[]; total: number }> {
  const params: any[] = [];
  const where: string[] = [];
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
    } else {
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
  const order   = opts.order === "asc" ? "ASC" : "DESC";
  // Tie-break by artist then title so identical sort keys stay stable.
  const orderSql = `ORDER BY ${sortCol} ${order} NULLS LAST, lower(a.name) ASC, lower(coalesce(rel->>'title','')) ASC`;
  const fromSql = `FROM blues_artists a, jsonb_array_elements(coalesce(a.discogs_releases, '[]'::jsonb)) AS rel`;
  const totalR = await getPool().query(`SELECT COUNT(*)::int AS n ${fromSql} ${whereSql}`, params);
  const total = totalR.rows[0]?.n ?? 0;
  params.push(limit, offset);
  const r = await getPool().query(
    `SELECT a.id   AS artist_id,
            a.name AS artist_name,
            a.photo_url,
            a.discogs_id AS artist_discogs_id,
            (rel->>'id')::bigint                    AS release_id,
            COALESCE(rel->>'type', 'release')       AS release_type,
            COALESCE(rel->>'title','')              AS release_title,
            NULLIF(rel->>'year','')::int            AS release_year,
            rel->'label'                            AS release_label,
            rel->>'role'                            AS role
       ${fromSql}
       ${whereSql}
       ${orderSql}
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
    params,
  );
  return { rows: r.rows, total };
}

// Full artist record + matched lyrics + releases sorted oldest→newest.
// Lyrics match by artist_id (canonical FK) OR — for legacy rows not
// yet backfilled — by case-insensitive name. The name fallback keeps
// the artist popup useful even before the import button has been run.
export async function getBluesArchiveArtist(id: number): Promise<any | null> {
  const ar = await getPool().query(`SELECT * FROM blues_artists WHERE id = $1`, [id]);
  if (!ar.rows.length) return null;
  const a = ar.rows[0];
  const lr = await getPool().query(
    `SELECT id, page_title, page_url, tuning, scraped_at, updated_at,
            artist_id, discogs_release_id, discogs_master_id,
            first_release_year, first_release_source,
            substring(plaintext, 1, 240) AS snippet
       FROM blues_lyrics
      WHERE artist_id = $1
         OR (artist_id IS NULL AND LOWER(TRIM(artist)) = LOWER($2))
      ORDER BY page_title ASC`,
    [id, a.name],
  );
  // Tuning breakdown — per-artist counts, descending. Powers the
  // little chip strip above the lyrics table on the artist popup.
  const tr = await getPool().query(
    `SELECT COALESCE(NULLIF(tuning, ''), '(unspecified)') AS tuning,
            COUNT(*)::int AS n
       FROM blues_lyrics
      WHERE artist_id = $1
         OR (artist_id IS NULL AND LOWER(TRIM(artist)) = LOWER($2))
      GROUP BY 1
      ORDER BY n DESC, tuning ASC`,
    [id, a.name],
  );
  // Sort the JSONB releases array oldest→newest (NULL years last).
  const releases = Array.isArray(a.discogs_releases) ? a.discogs_releases.slice() : [];
  releases.sort((x: any, y: any) => {
    const xy = Number(x?.year) || 9999;
    const yy = Number(y?.year) || 9999;
    if (xy !== yy) return xy - yy;
    return String(x?.title ?? "").localeCompare(String(y?.title ?? ""));
  });
  // Linked artists — symmetric junction. Either lo_id or hi_id can
  // be us; the other side is the linked row. Returns name + photo +
  // discogs_id so the popup can render a clickable chip with art.
  const lk = await getPool().query(
    `SELECT
        CASE WHEN l.lo_id = $1 THEN l.hi_id ELSE l.lo_id END AS id,
        l.kind,
        b.name,
        b.photo_url,
        b.discogs_id
       FROM blues_artist_links l
       JOIN blues_artists b
         ON b.id = CASE WHEN l.lo_id = $1 THEN l.hi_id ELSE l.lo_id END
      WHERE l.lo_id = $1 OR l.hi_id = $1
      ORDER BY l.kind ASC, lower(b.name) ASC`,
    [id],
  );
  return { ...a, lyrics: lr.rows, tunings: tr.rows, releases, links: lk.rows };
}

// ── blues_artist_links helpers ─────────────────────────────────────
// Symmetric (lo,hi) storage — we normalise the pair before write so
// the PK enforces single-row-per-pair regardless of click direction.
export async function addBluesArtistLink(
  aId: number,
  bId: number,
  kind: "pseudonym" | "band",
): Promise<void> {
  if (!Number.isFinite(aId) || !Number.isFinite(bId) || aId === bId) {
    throw new Error("Invalid artist ids");
  }
  if (kind !== "pseudonym" && kind !== "band") {
    throw new Error("Invalid kind");
  }
  const lo = Math.min(aId, bId);
  const hi = Math.max(aId, bId);
  // ON CONFLICT updates the kind — lets the user re-categorise an
  // existing link (e.g. pseudonym → band) without having to delete +
  // re-add it.
  await getPool().query(
    `INSERT INTO blues_artist_links (lo_id, hi_id, kind)
     VALUES ($1, $2, $3)
     ON CONFLICT (lo_id, hi_id) DO UPDATE SET kind = EXCLUDED.kind`,
    [lo, hi, kind],
  );
}

export async function removeBluesArtistLink(aId: number, bId: number): Promise<void> {
  if (!Number.isFinite(aId) || !Number.isFinite(bId)) return;
  const lo = Math.min(aId, bId);
  const hi = Math.max(aId, bId);
  await getPool().query(
    `DELETE FROM blues_artist_links WHERE lo_id = $1 AND hi_id = $2`,
    [lo, hi],
  );
}

export async function listBluesArtistLinks(aId: number): Promise<any[]> {
  const r = await getPool().query(
    `SELECT
        CASE WHEN l.lo_id = $1 THEN l.hi_id ELSE l.lo_id END AS id,
        l.kind,
        b.name,
        b.discogs_id
       FROM blues_artist_links l
       JOIN blues_artists b
         ON b.id = CASE WHEN l.lo_id = $1 THEN l.hi_id ELSE l.lo_id END
      WHERE l.lo_id = $1 OR l.hi_id = $1
      ORDER BY l.kind ASC, lower(b.name) ASC`,
    [aId],
  );
  return r.rows;
}

// ── Genre cache-warm cron state helpers ──────────────────────────
// Per-genre rows; every helper takes a genre_key. The scheduler picks
// today's genre via listAllGenreCacheWarmStates() + (dayOfYear %
// activeCount) and works only that row's cursor. Per-genre enable
// flag so individual genres can be paused without touching the others.
export async function listAllGenreCacheWarmStates(): Promise<any[]> {
  const r = await getPool().query(
    `SELECT * FROM genre_cache_warm_state ORDER BY rotation_order ASC, genre_key ASC`,
  );
  return r.rows;
}

export async function getGenreCacheWarmState(genreKey: string): Promise<any> {
  const r = await getPool().query(
    `SELECT * FROM genre_cache_warm_state WHERE genre_key = $1`,
    [genreKey],
  );
  return r.rows[0] || null;
}

export async function updateGenreCacheWarmState(
  genreKey: string,
  patch: Record<string, any>,
): Promise<void> {
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
  const sets: string[] = [];
  const vals: any[] = [];
  for (const [k, v] of Object.entries(patch)) {
    if (!allowed.has(k)) continue;
    vals.push(
      (k === "recent_errors" || k === "recent_cached") && v != null
        ? JSON.stringify(v)
        : v,
    );
    sets.push(`${k} = $${vals.length}`);
  }
  if (!sets.length) return;
  vals.push(genreKey);
  await getPool().query(
    `UPDATE genre_cache_warm_state SET ${sets.join(", ")} WHERE genre_key = $${vals.length}`,
    vals,
  );
}

// Atomic "claim a run" for one genre. Flips running false→true so
// only one worker can be active per genre even if two scheduler
// ticks race. Returns true if we got the lock.
export async function tryClaimGenreCacheWarmRun(genreKey: string): Promise<boolean> {
  const r = await getPool().query(
    `UPDATE genre_cache_warm_state
        SET running = true,
            started_at = NOW(),
            last_tick_at = NOW()
      WHERE genre_key = $1 AND running = false
      RETURNING 1`,
    [genreKey],
  );
  return (r.rowCount ?? 0) > 0;
}

export async function releaseGenreCacheWarmRun(genreKey: string): Promise<void> {
  await getPool().query(
    `UPDATE genre_cache_warm_state SET running = false, last_tick_at = NOW() WHERE genre_key = $1`,
    [genreKey],
  );
}

// Mass release lock for stale-lock recovery at scheduler boot.
export async function releaseAllStaleGenreCacheWarmRuns(staleMinutes: number = 10): Promise<number> {
  const r = await getPool().query(
    `UPDATE genre_cache_warm_state
        SET running = false
      WHERE running = true
        AND (started_at IS NULL OR started_at < NOW() - ($1 || ' minutes')::interval)
      RETURNING genre_key`,
    [String(staleMinutes)],
  );
  return r.rowCount ?? 0;
}

export async function recordGenreCacheWarmHit(
  genreKey: string,
  title: string,
  releaseId: number,
): Promise<void> {
  await getPool().query(
    `UPDATE genre_cache_warm_state
        SET lifetime_cached = lifetime_cached + 1,
            cycle_cached    = cycle_cached + 1,
            last_cached_at  = NOW(),
            recent_cached   = (
              jsonb_build_array(jsonb_build_object('id', $3::bigint, 'title', $2::text, 'at', NOW()))
              || (recent_cached - 9)
            )
      WHERE genre_key = $1`,
    [genreKey, title, releaseId],
  );
}

export async function recordGenreCacheWarmSkip(genreKey: string): Promise<void> {
  await getPool().query(
    `UPDATE genre_cache_warm_state
        SET lifetime_skipped = lifetime_skipped + 1,
            cycle_skipped    = cycle_skipped + 1
      WHERE genre_key = $1`,
    [genreKey],
  );
}

export async function recordGenreCacheWarmSearched(genreKey: string, n: number): Promise<void> {
  await getPool().query(
    `UPDATE genre_cache_warm_state
        SET lifetime_searched = lifetime_searched + $2,
            cycle_searched    = cycle_searched + $2
      WHERE genre_key = $1`,
    [genreKey, n],
  );
}

export async function recordGenreCacheWarmError(genreKey: string, msg: string): Promise<void> {
  await getPool().query(
    `UPDATE genre_cache_warm_state
        SET lifetime_errors = lifetime_errors + 1,
            recent_errors   = (
              jsonb_build_array(jsonb_build_object('msg', $2::text, 'at', NOW()))
              || (recent_errors - 9)
            )
      WHERE genre_key = $1`,
    [genreKey, msg.slice(0, 500)],
  );
}

export async function resetGenreCacheWarmCycle(genreKey: string): Promise<void> {
  await getPool().query(
    `UPDATE genre_cache_warm_state
        SET current_year     = start_year,
            current_page     = 1,
            cycle_searched   = 0,
            cycle_cached     = 0,
            cycle_skipped    = 0,
            cycle_started_at = NOW(),
            cycle_count      = cycle_count + 1
      WHERE genre_key = $1`,
    [genreKey],
  );
}

// Existence check the worker uses before fetching a release. We only
// pay the Discogs RTT if it's not already cached. (kind defaults to
// 'release' to match how cacheRelease stores its rows.)
export async function isReleaseCached(
  discogsId: number,
  type: "release" | "master" = "release",
): Promise<boolean> {
  const r = await getPool().query(
    `SELECT 1 FROM release_cache WHERE discogs_id = $1 AND type = $2 LIMIT 1`,
    [discogsId, type],
  );
  return (r.rowCount ?? 0) > 0;
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
export async function updateLyricFields(id: number, patch: {
  tuning?: string | null;
  artist?: string | null;
  page_title?: string;
  artist_id?: number | null;
  discogs_release_id?: number | null;
  discogs_master_id?: number | null;
  first_release_year?: number | null;
}): Promise<any | null> {
  const sets: string[] = [];
  const params: any[] = [];
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
      } else {
        sets.push(`artist_id = NULL`);
      }
    }
  }
  if ("artist_id" in patch) {
    if (patch.artist_id === null) {
      sets.push(`artist_id = NULL`);
    } else {
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
    } else {
      params.push(Number(patch.discogs_release_id));
      sets.push(`discogs_release_id = $${params.length}`);
    }
  }
  if ("discogs_master_id" in patch) {
    if (patch.discogs_master_id == null) {
      sets.push(`discogs_master_id = NULL`);
    } else {
      params.push(Number(patch.discogs_master_id));
      sets.push(`discogs_master_id = $${params.length}`);
    }
  }
  if ("first_release_year" in patch) {
    if (patch.first_release_year == null) {
      sets.push(`first_release_year = NULL, first_release_source = NULL`);
    } else {
      params.push(Number(patch.first_release_year));
      // source='manual' so the curator can audit which years were
      // hand-entered vs derived. Resolver respects this — won't
      // overwrite manual entries unless force=1.
      sets.push(`first_release_year = $${params.length}, first_release_source = 'manual', first_release_checked_at = NOW()`);
    }
  }
  if (!sets.length) return await getLyricById(id);
  params.push(id);
  await getPool().query(
    `UPDATE blues_lyrics SET ${sets.join(", ")} WHERE id = $${params.length}`,
    params,
  );
  return await getLyricById(id);
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
export async function mergeBluesArtists(fromId: number, intoId: number): Promise<{
  ok: boolean;
  fromName: string;
  intoName: string;
  lyricsReassigned: number;
  releasesAdded: number;
}> {
  if (fromId === intoId) throw new Error("source and target are the same row");
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
    const lr1 = await client.query(
      `UPDATE blues_lyrics
          SET artist_id = $1, artist = $2
        WHERE artist_id = $3`,
      [into.id, into.name, from.id],
    );
    const lr2 = await client.query(
      `UPDATE blues_lyrics
          SET artist_id = $1, artist = $2
        WHERE artist_id IS NULL
          AND LOWER(TRIM(artist)) = LOWER($3)`,
      [into.id, into.name, from.name],
    );
    const lyricsReassigned = (lr1.rowCount ?? 0) + (lr2.rowCount ?? 0);
    // Merge discogs_releases JSONB. Dedupe by id+type.
    const fromRels = Array.isArray(from.discogs_releases) ? from.discogs_releases : [];
    const intoRels = Array.isArray(into.discogs_releases) ? into.discogs_releases : [];
    const seen = new Set(intoRels.map((r: any) => `${r?.type ?? "release"}:${r?.id}`));
    let releasesAdded = 0;
    for (const r of fromRels) {
      const key = `${r?.type ?? "release"}:${r?.id}`;
      if (seen.has(key)) continue;
      intoRels.push(r);
      seen.add(key);
      releasesAdded++;
    }
    if (releasesAdded) {
      await client.query(
        `UPDATE blues_artists SET discogs_releases = $1::jsonb WHERE id = $2`,
        [JSON.stringify(intoRels), intoId],
      );
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
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
}

// ── Blues Archive aggregate helpers (admin) ──────────────────────────

// Top-of-page stats strip. One pass per metric — the table is small
// enough that this is fine; index on artist_id + tuning keeps each
// query milliseconds.
export async function getBluesArchiveStats(): Promise<{
  artists_total: number;
  artists_with_lyrics: number;
  artists_with_releases: number;
  artists_with_both: number;
  artists_empty: number;
  lyrics_total: number;
  lyrics_orphan: number;
  lyrics_missing_tuning: number;
}> {
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
  return r.rows[0] as any;
}

// Recent edits feed across blues_artists + blues_lyrics, newest first.
// Each row carries `kind` so the UI can label it ("artist" vs "lyric")
// and link appropriately.
export async function getRecentBluesEdits(limit: number = 20): Promise<Array<any>> {
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
export async function reassignLyrics(opts: {
  fromArtistId?: number | null;
  fromArtistName?: string | null;
  toArtistId: number;
}): Promise<{ ok: boolean; toName: string; reassigned: number }> {
  const toId = Number(opts.toArtistId);
  if (!Number.isFinite(toId)) throw new Error("toArtistId required");
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
      const r = await client.query(
        `UPDATE blues_lyrics SET artist_id = $1, artist = $2 WHERE artist_id = $3`,
        [to.id, to.name, Number(opts.fromArtistId)],
      );
      reassigned += r.rowCount ?? 0;
    }
    if (opts.fromArtistName) {
      const r = await client.query(
        `UPDATE blues_lyrics SET artist_id = $1, artist = $2
          WHERE LOWER(TRIM(COALESCE(artist, ''))) = LOWER(TRIM($3))
            AND (artist_id IS NULL OR artist_id <> $1)`,
        [to.id, to.name, opts.fromArtistName],
      );
      reassigned += r.rowCount ?? 0;
    }
    await client.query("COMMIT");
    return { ok: true, toName: to.name, reassigned };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
}

// Promote an orphan lyric (no artist_id) to a brand-new blues_artists
// row using its current `artist` string as the name. Also retro-links
// every other orphan lyric with the same name so a single click
// rescues the whole batch. Transaction.
export async function promoteOrphanLyricToArtist(lyricId: number): Promise<{
  ok: boolean;
  artistId: number;
  artistName: string;
  lyricsLinked: number;
}> {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    const lr = await client.query(
      `SELECT id, artist FROM blues_lyrics WHERE id = $1 FOR UPDATE`,
      [lyricId],
    );
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
    let artistId: number;
    const existR = await client.query(
      `SELECT id FROM blues_artists WHERE LOWER(name) = LOWER($1) LIMIT 1`,
      [name],
    );
    if (existR.rows.length) {
      artistId = existR.rows[0].id;
    } else {
      const insR = await client.query(
        `INSERT INTO blues_artists (name, enrichment_status)
         VALUES ($1, '{"source":"promote_from_orphan_lyric"}'::jsonb)
         RETURNING id`,
        [name],
      );
      artistId = insR.rows[0].id;
    }
    const upR = await client.query(
      `UPDATE blues_lyrics
          SET artist_id = $1, artist = $2
        WHERE artist_id IS NULL
          AND LOWER(TRIM(COALESCE(artist, ''))) = LOWER($3)`,
      [artistId, name, name],
    );
    await client.query("COMMIT");
    return { ok: true, artistId, artistName: name, lyricsLinked: upR.rowCount ?? 0 };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
}
