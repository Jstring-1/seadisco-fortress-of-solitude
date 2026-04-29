import pg from "pg";
const { Pool } = pg;
import { expandWithSynonyms } from "./classical-synonyms.js";

let pool: InstanceType<typeof Pool> | null = null;

function getPool() {
  if (!pool) {
    const connStr = process.env.APP_DB_URL;
    if (!connStr) throw new Error("APP_DB_URL not set");
    // Explicit pool sizing. Defaults (max:10, no idleTimeoutMillis)
    // were fine at small scale but caused queue buildup during
    // sync bursts (every signed-in user can hit /api/user/* in
    // parallel for collection / wantlist / inventory / lists).
    // Overridable via env so we can tune without a redeploy.
    const max = Number(process.env.APP_DB_POOL_MAX ?? 20);
    const min = Number(process.env.APP_DB_POOL_MIN ?? 2);
    const idle = Number(process.env.APP_DB_POOL_IDLE_MS ?? 30000);
    const connTimeout = Number(process.env.APP_DB_POOL_CONN_MS ?? 5000);
    pool = new Pool({
      connectionString: connStr,
      ssl: process.env.DB_CA_CERT
        ? { rejectUnauthorized: true, ca: process.env.DB_CA_CERT }
        : { rejectUnauthorized: false },
      max,
      min,
      idleTimeoutMillis: idle,
      connectionTimeoutMillis: connTimeout,
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
  await getPool().query(
    `INSERT INTO user_collection (clerk_user_id, discogs_release_id, data, added_at, synced_at, folder_id, rating, instance_id, notes)
     SELECT $1, unnest($2::int[]), unnest($3::jsonb[]), unnest($4::timestamptz[]), NOW(), unnest($5::int[]), unnest($6::int[]), unnest($7::int[]), unnest($8::jsonb[])
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
  const r = await getPool().query(
    `DELETE FROM user_collection WHERE clerk_user_id = $1 AND instance_id IS NOT NULL AND instance_id != ALL($2::int[]) RETURNING 1`,
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
  // User collection folders older than 30 days
  const fld = await getPool().query(
    `DELETE FROM user_collection_folders WHERE synced_at < ${interval30d}`
  );
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
  return row.data ?? null;
}

/** Save a metadata response to cache. Overwrites if already present. */
export async function cacheRelease(discogsId: number, type: DiscogsCacheType, data: object): Promise<void> {
  await getPool().query(
    `INSERT INTO release_cache (discogs_id, type, data, cached_at)
     VALUES ($1, $2, $3, NOW())
     ON CONFLICT (discogs_id, type)
     DO UPDATE SET data = EXCLUDED.data, cached_at = NOW()`,
    [discogsId, type, JSON.stringify(data)]
  );
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
    const r = await getPool().query(
      `SELECT release_id, release_type, track_position, track_title,
              video_id, video_title, submitted_by, submitted_at
         FROM track_youtube_overrides
        WHERE ${ors.join(" OR ")}`,
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

// ── Personal suggestions: taste profile + library dedup + persistence ──
//
// Taste tuples power the per-user background suggestions job. We
// expand the user's collection + wantlist by (genre, style, year)
// and rank by frequency: the top tuples are what the user listens to
// the most. The job then queries Discogs for masters matching each
// tuple. Wide net by design — top 9 covers a few different bands
// of the user's taste rather than just their #1 obsession.

export async function getUserTasteTuples(
  clerkUserId: string,
  limit = 9
): Promise<Array<{ genre: string; style: string; year: number; n: number }>> {
  try {
    const r = await getPool().query(
      `WITH src AS (
         SELECT data FROM user_collection WHERE clerk_user_id = $1
         UNION ALL
         SELECT data FROM user_wantlist  WHERE clerk_user_id = $1
       ),
       expanded AS (
         SELECT
           g.value AS genre,
           s.value AS style,
           NULLIF(src.data->>'year','')::int AS year
         FROM src,
              jsonb_array_elements_text(src.data->'genres') AS g(value),
              jsonb_array_elements_text(src.data->'styles') AS s(value)
       ),
       counts AS (
         SELECT genre, style, year, COUNT(*)::int AS n
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
// generator can skip them in O(1).
export async function getDismissedSuggestionKeys(clerkUserId: string): Promise<Set<string>> {
  const out = new Set<string>();
  try {
    const r = await getPool().query(
      `SELECT discogs_id, entity_type FROM user_suggestion_dismissals WHERE clerk_user_id = $1`,
      [clerkUserId]
    );
    for (const row of r.rows) out.add(`${row.entity_type}:${row.discogs_id}`);
  } catch { /* best effort */ }
  return out;
}

export async function getUserPersonalSuggestions(
  clerkUserId: string,
  limit = 48
): Promise<any[]> {
  try {
    const r = await getPool().query(
      `SELECT discogs_id, entity_type, score, data, generated_at
         FROM user_personal_suggestions
        WHERE clerk_user_id = $1
        ORDER BY score DESC, generated_at DESC
        LIMIT $2`,
      [clerkUserId, Math.max(1, Math.min(96, limit))]
    );
    return r.rows;
  } catch { return []; }
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
