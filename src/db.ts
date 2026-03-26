import pg from "pg";
const { Pool } = pg;

let pool: InstanceType<typeof Pool> | null = null;

function getPool() {
  if (!pool) {
    const connStr = process.env.APP_DB_URL;
    if (!connStr) throw new Error("APP_DB_URL not set");
    pool = new Pool({ connectionString: connStr, ssl: { rejectUnauthorized: false } });
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
    CREATE TABLE IF NOT EXISTS search_history (
      id            SERIAL PRIMARY KEY,
      clerk_user_id TEXT NOT NULL,
      params        JSONB NOT NULL,
      searched_at   TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await getPool().query(`
    CREATE INDEX IF NOT EXISTS search_history_user_idx
    ON search_history (clerk_user_id, searched_at DESC)
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
  await getPool().query(`ALTER TABLE user_collection ADD COLUMN IF NOT EXISTS rating INTEGER DEFAULT 0`);
  await getPool().query(`ALTER TABLE user_collection ADD COLUMN IF NOT EXISTS instance_id INTEGER`);
  await getPool().query(`ALTER TABLE user_collection ADD COLUMN IF NOT EXISTS notes JSONB`);
  // Extra wantlist fields — rating, notes
  await getPool().query(`ALTER TABLE user_wantlist ADD COLUMN IF NOT EXISTS rating INTEGER DEFAULT 0`);
  await getPool().query(`ALTER TABLE user_wantlist ADD COLUMN IF NOT EXISTS notes JSONB`);
  // Background sync progress tracking
  await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS sync_status TEXT DEFAULT 'idle'`);
  await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS sync_progress INTEGER DEFAULT 0`);
  await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS sync_total INTEGER DEFAULT 0`);
  await getPool().query(`ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS sync_error TEXT`);
  await getPool().query(`
    CREATE TABLE IF NOT EXISTS user_collection (
      id                 SERIAL PRIMARY KEY,
      clerk_user_id      TEXT NOT NULL,
      discogs_release_id INTEGER NOT NULL,
      data               JSONB NOT NULL,
      added_at           TIMESTAMP,
      synced_at          TIMESTAMP DEFAULT NOW(),
      UNIQUE(clerk_user_id, discogs_release_id)
    )
  `);
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
  // Anonymous interest signals — survives account deletion
  await getPool().query(`
    CREATE TABLE IF NOT EXISTS interest_signals (
      id                 SERIAL PRIMARY KEY,
      discogs_release_id INTEGER NOT NULL,
      source             TEXT NOT NULL,
      artists            TEXT[],
      labels             TEXT[],
      genres             TEXT[],
      styles             TEXT[],
      year               INTEGER,
      recorded_at        TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(discogs_release_id, source)
    )
  `);
  await getPool().query(`
    CREATE INDEX IF NOT EXISTS interest_signals_artists_idx ON interest_signals USING GIN (artists)
  `);
  await getPool().query(`
    CREATE INDEX IF NOT EXISTS interest_signals_genres_idx ON interest_signals USING GIN (genres)
  `);

  await getPool().query(`
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
      release_group_mbid  TEXT,
      artist_mbids        TEXT[],
      fetched_at          TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  // Migration: add columns if missing on existing tables
  await getPool().query(`ALTER TABLE fresh_releases ADD COLUMN IF NOT EXISTS release_group_mbid TEXT`);
  await getPool().query(`ALTER TABLE fresh_releases ADD COLUMN IF NOT EXISTS artist_mbids TEXT[]`);

  // ── Gear listings (eBay vintage electronics) ────────────────────────────
  await getPool().query(`
    CREATE TABLE IF NOT EXISTS gear_listings (
      item_id         TEXT PRIMARY KEY,
      title           TEXT NOT NULL,
      price           NUMERIC(10,2) NOT NULL,
      currency        TEXT DEFAULT 'USD',
      condition       TEXT,
      image_url       TEXT,
      item_url        TEXT,
      location_city   TEXT,
      location_state  TEXT,
      location_country TEXT,
      seller_username TEXT,
      seller_feedback INTEGER,
      buying_options  TEXT[],
      bid_count       INTEGER DEFAULT 0,
      categories      TEXT[],
      category_names  TEXT[],
      item_end_date   TIMESTAMPTZ,
      detail_html     TEXT,
      all_images      TEXT[],
      item_specifics  JSONB,
      thumbnail_url   TEXT,
      raw_summary     JSONB,
      fetched_at      TIMESTAMPTZ DEFAULT NOW(),
      detailed_at     TIMESTAMPTZ,
      expired         BOOLEAN DEFAULT false
    )
  `);
  await getPool().query(`
    CREATE INDEX IF NOT EXISTS gear_listings_bids_price_idx ON gear_listings (bid_count DESC, price DESC) WHERE NOT expired
  `);
  // ── Feed articles (RSS + YouTube) ──────────────────────────────────────
  await getPool().query(`
    CREATE TABLE IF NOT EXISTS feed_articles (
      id              SERIAL PRIMARY KEY,
      source          TEXT NOT NULL,
      source_url      TEXT UNIQUE NOT NULL,
      title           TEXT NOT NULL,
      summary         TEXT,
      image_url       TEXT,
      author          TEXT,
      category        TEXT DEFAULT 'news',
      content_type    TEXT DEFAULT 'article',
      published_at    TIMESTAMPTZ,
      fetched_at      TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await getPool().query(`
    CREATE INDEX IF NOT EXISTS feed_articles_published_idx ON feed_articles (published_at DESC)
  `);
  await getPool().query(`
    CREATE TABLE IF NOT EXISTS gear_fetch_log (
      id          SERIAL PRIMARY KEY,
      fetch_type  TEXT NOT NULL,
      item_count  INTEGER DEFAULT 0,
      error       TEXT,
      started_at  TIMESTAMPTZ DEFAULT NOW(),
      finished_at TIMESTAMPTZ
    )
  `);
  // ── Live events (Ticketmaster upcoming) ─────────────────────────────────
  await getPool().query(`
    CREATE TABLE IF NOT EXISTS live_events (
      id          SERIAL PRIMARY KEY,
      event_name  TEXT NOT NULL,
      artist      TEXT,
      event_date  TEXT,
      event_time  TEXT,
      venue       TEXT,
      venue_id    TEXT,
      city        TEXT,
      region      TEXT,
      country     TEXT,
      url         TEXT,
      fetched_at  TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(url)
    )
  `);
}

export async function saveSearch(clerkUserId: string, params: Record<string, string>): Promise<void> {
  // Skip if identical params were saved in the last 5 minutes (prevents double-saves)
  const recent = await getPool().query(
    `SELECT 1 FROM search_history
     WHERE clerk_user_id = $1 AND params = $2
       AND searched_at > NOW() - INTERVAL '5 minutes'
     LIMIT 1`,
    [clerkUserId, JSON.stringify(params)]
  );
  if (recent.rows.length) return;

  await getPool().query(
    `INSERT INTO search_history (clerk_user_id, params) VALUES ($1, $2)`,
    [clerkUserId, JSON.stringify(params)]
  );
  // Keep only the most recent 500 searches per user
  await getPool().query(
    `DELETE FROM search_history
     WHERE clerk_user_id = $1
       AND id NOT IN (
         SELECT id FROM search_history
         WHERE clerk_user_id = $1
         ORDER BY searched_at DESC
         LIMIT 500
       )`,
    [clerkUserId]
  );
}

export async function markSearchBio(clerkUserId: string): Promise<void> {
  // Add b=y to the params of the most recent search
  await getPool().query(
    `UPDATE search_history SET params = params || '{"b":"y"}'::jsonb
     WHERE id = (
       SELECT id FROM search_history
       WHERE clerk_user_id = $1
       ORDER BY searched_at DESC
       LIMIT 1
     )`,
    [clerkUserId]
  );
}

export async function deleteSearch(clerkUserId: string, params: Record<string, string>): Promise<void> {
  await getPool().query(
    "DELETE FROM search_history WHERE clerk_user_id = $1 AND params = $2",
    [clerkUserId, JSON.stringify(params)]
  );
}

export async function clearSearchHistory(clerkUserId: string): Promise<void> {
  await getPool().query("DELETE FROM search_history WHERE clerk_user_id = $1", [clerkUserId]);
}

export async function deleteSearchGlobal(params: Record<string, string>): Promise<void> {
  await getPool().query(
    "DELETE FROM search_history WHERE params = $1",
    [JSON.stringify(params)]
  );
}

export async function deleteSearchById(id: number): Promise<void> {
  await getPool().query("DELETE FROM search_history WHERE id = $1", [id]);
}

export async function getSearchHistory(clerkUserId: string, limit = 50): Promise<Array<{ params: Record<string, string>; searched_at: string }>> {
  const r = await getPool().query(
    `SELECT params, MAX(searched_at) AS searched_at
     FROM search_history
     WHERE clerk_user_id = $1 AND NOT (params ? '_type')
     GROUP BY params
     ORDER BY MAX(searched_at) DESC
     LIMIT $2`,
    [clerkUserId, limit]
  );
  return r.rows;
}

export async function getAllUsersSyncStatus(): Promise<Array<{
  username: string;
  collectionSyncedAt: Date | null;
  wantlistSyncedAt: Date | null;
  syncStatus: string;
  syncProgress: number;
  syncTotal: number;
  syncError: string | null;
}>> {
  const r = await getPool().query(
    `SELECT discogs_username, collection_synced_at, wantlist_synced_at,
            sync_status, sync_progress, sync_total, sync_error
     FROM user_tokens
     WHERE discogs_username IS NOT NULL
     ORDER BY discogs_username`
  );
  return r.rows.map(row => ({
    username:           row.discogs_username,
    collectionSyncedAt: row.collection_synced_at ?? null,
    wantlistSyncedAt:   row.wantlist_synced_at   ?? null,
    syncStatus:         row.sync_status          ?? "idle",
    syncProgress:       row.sync_progress        ?? 0,
    syncTotal:          row.sync_total           ?? 0,
    syncError:          row.sync_error           ?? null,
  }));
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

export async function getRecentSearches(limit = 300): Promise<Array<{ params: Record<string, string>; searched_at: string }>> {
  // Grab the latest `limit` unique searches from logged-in users only, then randomise in the API layer
  const r = await getPool().query(
    `SELECT params, searched_at FROM (
       SELECT DISTINCT ON (params) params, searched_at
       FROM search_history
       WHERE NOT (params ? '_type')
         AND clerk_user_id <> 'anon'
       ORDER BY params, searched_at DESC
     ) sub
     ORDER BY searched_at DESC
     LIMIT $1`,
    [limit]
  );
  return r.rows;
}

export async function getRecentLiveSearches(limit = 200): Promise<Array<{ params: Record<string, string>; searched_at: string }>> {
  const r = await getPool().query(
    `SELECT params, searched_at FROM (
       SELECT DISTINCT ON (params) params, searched_at
       FROM search_history
       WHERE params->>'_type' = 'live'
         AND clerk_user_id <> 'anon'
       ORDER BY params, searched_at DESC
     ) sub
     ORDER BY searched_at DESC
     LIMIT $1`,
    [limit]
  );
  return r.rows;
}

export async function dumpSearchHistory(): Promise<any[]> {
  const r = await getPool().query(
    `SELECT id, clerk_user_id, params, searched_at FROM search_history ORDER BY searched_at DESC`
  );
  return r.rows;
}

export async function truncateSearchHistory(): Promise<number> {
  const r = await getPool().query(`DELETE FROM search_history`);
  return r.rowCount ?? 0;
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
  await getPool().query("DELETE FROM user_tokens    WHERE clerk_user_id = $1", [clerkUserId]);
  await getPool().query("DELETE FROM search_history WHERE clerk_user_id = $1", [clerkUserId]);
  await getPool().query("DELETE FROM user_collection WHERE clerk_user_id = $1", [clerkUserId]);
  await getPool().query("DELETE FROM user_wantlist   WHERE clerk_user_id = $1", [clerkUserId]);
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
  for (const item of items) {
    await getPool().query(
      `INSERT INTO user_collection (clerk_user_id, discogs_release_id, data, added_at, synced_at, folder_id, rating, instance_id, notes)
       VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7, $8)
       ON CONFLICT (clerk_user_id, discogs_release_id)
       DO UPDATE SET data = $3, added_at = $4, synced_at = NOW(), folder_id = $5, rating = $6, instance_id = $7, notes = $8`,
      [clerkUserId, item.id, JSON.stringify(item.data), item.addedAt ?? null, item.folderId ?? 0, item.rating ?? 0, item.instanceId ?? null, item.notes ? JSON.stringify(item.notes) : null]
    );
  }
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

export async function getCollectionFolderList(
  clerkUserId: string
): Promise<Array<{ folderId: number; name: string; count: number }>> {
  const r = await getPool().query(
    `SELECT folder_id, folder_name, item_count FROM user_collection_folders
     WHERE clerk_user_id = $1 ORDER BY folder_name ASC`,
    [clerkUserId]
  );
  return r.rows.map(row => ({ folderId: row.folder_id, name: row.folder_name, count: row.item_count }));
}

export async function upsertWantlistItems(
  clerkUserId: string,
  items: Array<{ id: number; data: object; addedAt?: Date; rating?: number; notes?: any[] }>
): Promise<void> {
  for (const item of items) {
    await getPool().query(
      `INSERT INTO user_wantlist (clerk_user_id, discogs_release_id, data, added_at, synced_at, rating, notes)
       VALUES ($1, $2, $3, $4, NOW(), $5, $6)
       ON CONFLICT (clerk_user_id, discogs_release_id)
       DO UPDATE SET data = $3, added_at = $4, synced_at = NOW(), rating = $5, notes = $6`,
      [clerkUserId, item.id, JSON.stringify(item.data), item.addedAt ?? null, item.rating ?? 0, item.notes ? JSON.stringify(item.notes) : null]
    );
  }
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
  folderId?: number;
  ratingMin?: number;  // 1-5 for "N stars+", 0 for unrated only
  ratingUnrated?: boolean; // true = show only unrated
  notes?: string;      // text search across notes JSONB
  sort?: string;
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

function buildCwWhere(filters: CwSearchFilters, startIdx: number): { clause: string; params: any[] } {
  const clauses: string[] = [];
  const allParams: any[] = [];
  let idx = startIdx;

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
    const { clause, params, nextIdx } = parseFilterExpr(value, column, idx);
    if (clause) {
      clauses.push(clause);
      allParams.push(...params);
      idx = nextIdx;
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

  // Notes text search
  if (filters.notes) {
    clauses.push(`notes::text ILIKE $${idx}`);
    allParams.push(`%${filters.notes}%`);
    idx++;
  }

  return { clause: clauses.length ? " AND " + clauses.join(" AND ") : "", params: allParams };
}

export async function getCollectionPage(
  clerkUserId: string,
  page: number,
  perPage: number,
  filters?: CwSearchFilters
): Promise<{ items: any[]; total: number }> {
  const offset = (page - 1) * perPage;
  const { clause: dataClause, params: dataFilterParams } = buildCwWhere(filters ?? {}, 4);
  const { clause: countClause, params: countFilterParams } = buildCwWhere(filters ?? {}, 2);
  const orderBy = cwOrderBy(filters?.sort);
  const [dataR, countR] = await Promise.all([
    getPool().query(
      `SELECT data, rating, notes FROM user_collection WHERE clerk_user_id = $1${dataClause}
       ${orderBy}
       LIMIT $2 OFFSET $3`,
      [clerkUserId, perPage, offset, ...dataFilterParams]
    ),
    getPool().query(
      `SELECT COUNT(*)::int AS total FROM user_collection WHERE clerk_user_id = $1${countClause}`,
      [clerkUserId, ...countFilterParams]
    ),
  ]);
  return { items: dataR.rows.map(r => ({ ...r.data, _rating: r.rating ?? 0, _notes: r.notes ?? [] })), total: countR.rows[0]?.total ?? 0 };
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
): Promise<{ items: any[]; total: number }> {
  const offset = (page - 1) * perPage;
  const { clause: dataClause, params: dataFilterParams } = buildCwWhere(filters ?? {}, 4);
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
  return { items: dataR.rows.map(r => ({ ...r.data, _rating: r.rating ?? 0, _notes: r.notes ?? [] })), total: countR.rows[0]?.total ?? 0 };
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
    "SELECT discogs_release_id FROM user_collection WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return r.rows.map(row => row.discogs_release_id);
}

export async function getWantedSample(limit: number = 24): Promise<object[]> {
  const r = await getPool().query(
    `SELECT data FROM user_wantlist ORDER BY RANDOM() LIMIT $1`, [limit]
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

export async function upsertFreshRelease(r: {
  release_mbid: string;
  release_name: string;
  artist_credit_name: string;
  release_date: string | null;
  primary_type: string | null;
  secondary_type: string | null;
  tags: string[];
  caa_id: number | null;
  caa_release_mbid: string | null;
  cover_url: string | null;
  release_group_mbid: string | null;
  artist_mbids: string[];
}): Promise<void> {
  await getPool().query(
    `INSERT INTO fresh_releases
       (release_mbid, release_name, artist_credit_name, release_date, primary_type, secondary_type, tags, caa_id, caa_release_mbid, cover_url, release_group_mbid, artist_mbids, fetched_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
     ON CONFLICT (release_mbid)
     DO UPDATE SET
       release_name       = $2,
       artist_credit_name = $3,
       release_date       = $4,
       primary_type       = $5,
       secondary_type     = $6,
       tags               = $7,
       caa_id             = $8,
       caa_release_mbid   = $9,
       cover_url          = $10,
       release_group_mbid = $11,
       artist_mbids       = $12,
       fetched_at         = NOW()`,
    [r.release_mbid, r.release_name, r.artist_credit_name, r.release_date,
     r.primary_type, r.secondary_type, r.tags, r.caa_id, r.caa_release_mbid, r.cover_url,
     r.release_group_mbid, r.artist_mbids]
  );
}

export async function getFreshStats(): Promise<{ count: number; oldest: string | null; newest: string | null; tagCount: number }> {
  const r = await getPool().query(
    `SELECT COUNT(*)::int AS count,
            MIN(release_date) AS oldest,
            MAX(release_date) AS newest,
            (SELECT COUNT(DISTINCT t)::int FROM fresh_releases, unnest(tags) AS t) AS tag_count
     FROM fresh_releases`
  );
  return {
    count:    r.rows[0]?.count     ?? 0,
    oldest:   r.rows[0]?.oldest    ?? null,
    newest:   r.rows[0]?.newest    ?? null,
    tagCount: r.rows[0]?.tag_count ?? 0,
  };
}

export async function pruneFreshReleases(): Promise<number> {
  const r = await getPool().query(
    `DELETE FROM fresh_releases WHERE fetched_at < NOW() - INTERVAL '3 months'`
  );
  return r.rowCount ?? 0;
}

export async function getFreshReleases(limit = 150): Promise<any[]> {
  // Random sample from last 3 months — all loaded at once for client-side filtering
  const r = await getPool().query(
    `SELECT release_mbid, release_name, artist_credit_name, release_date,
            primary_type, secondary_type, tags, caa_release_mbid, cover_url,
            release_group_mbid, artist_mbids
     FROM fresh_releases
     WHERE fetched_at > NOW() - INTERVAL '3 months'
     ORDER BY RANDOM()
     LIMIT $1`,
    [limit]
  );
  return r.rows;
}

export async function searchFreshReleases(query: string, limit = 200): Promise<any[]> {
  const pattern = `%${query}%`;
  const r = await getPool().query(
    `SELECT release_mbid, release_name, artist_credit_name, release_date,
            primary_type, secondary_type, tags, caa_release_mbid, cover_url,
            release_group_mbid, artist_mbids
     FROM fresh_releases
     WHERE fetched_at > NOW() - INTERVAL '3 months'
       AND (
         artist_credit_name ILIKE $1
         OR release_name ILIKE $1
         OR EXISTS (SELECT 1 FROM unnest(tags) AS t WHERE t ILIKE $1)
       )
     ORDER BY release_date DESC NULLS LAST
     LIMIT $2`,
    [pattern, limit]
  );
  return r.rows;
}

export async function getFreshTopTags(limit = 24): Promise<Array<{ tag: string; cnt: number }>> {
  // Random selection from all tags that appear on at least 1 release in the last 14 days.
  const r = await getPool().query(
    `SELECT unnest(tags) AS tag, COUNT(*)::int AS cnt
     FROM fresh_releases
     WHERE fetched_at > NOW() - INTERVAL '28 days'
     GROUP BY tag
     ORDER BY RANDOM()
     LIMIT $1`,
    [limit]
  );
  return r.rows;
}

// ── Interest signals (anonymous, survives account deletion) ──

export async function recordInterestSignals(
  items: Array<{ id: number; data: any }>,
  source: "collection" | "wantlist"
): Promise<void> {
  for (const item of items) {
    const d = item.data;
    if (!d) continue;
    const artists: string[] = (d.artists ?? []).map((a: any) => a.name).filter(Boolean);
    const labels:  string[] = (d.labels  ?? []).map((l: any) => l.name).filter(Boolean);
    const genres:  string[] = d.genres ?? [];
    const styles:  string[] = d.styles ?? [];
    const year:    number | null = d.year || null;
    await getPool().query(
      `INSERT INTO interest_signals (discogs_release_id, source, artists, labels, genres, styles, year)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (discogs_release_id, source)
       DO UPDATE SET artists = $3, labels = $4, genres = $5, styles = $6, year = $7, recorded_at = NOW()`,
      [item.id, source, artists, labels, genres, styles, year]
    );
  }
}

export async function getInterestStats(): Promise<{
  totalReleases: number;
  topArtists: Array<{ name: string; cnt: number }>;
  topLabels:  Array<{ name: string; cnt: number }>;
  topGenres:  Array<{ name: string; cnt: number }>;
  topStyles:  Array<{ name: string; cnt: number }>;
}> {
  const [totalR, artistsR, labelsR, genresR, stylesR] = await Promise.all([
    getPool().query("SELECT COUNT(*)::int AS cnt FROM interest_signals"),
    getPool().query(
      `SELECT unnest(artists) AS name, COUNT(*)::int AS cnt
       FROM interest_signals GROUP BY name ORDER BY cnt DESC LIMIT 50`
    ),
    getPool().query(
      `SELECT unnest(labels) AS name, COUNT(*)::int AS cnt
       FROM interest_signals GROUP BY name ORDER BY cnt DESC LIMIT 50`
    ),
    getPool().query(
      `SELECT unnest(genres) AS name, COUNT(*)::int AS cnt
       FROM interest_signals GROUP BY name ORDER BY cnt DESC LIMIT 30`
    ),
    getPool().query(
      `SELECT unnest(styles) AS name, COUNT(*)::int AS cnt
       FROM interest_signals GROUP BY name ORDER BY cnt DESC LIMIT 50`
    ),
  ]);
  return {
    totalReleases: totalR.rows[0]?.cnt ?? 0,
    topArtists: artistsR.rows,
    topLabels:  labelsR.rows,
    topGenres:  genresR.rows,
    topStyles:  stylesR.rows,
  };
}

export async function backfillInterestSignals(): Promise<{ collection: number; wantlist: number }> {
  let collCount = 0;
  let wantCount = 0;
  const coll = await getPool().query("SELECT discogs_release_id, data FROM user_collection");
  for (const row of coll.rows) {
    const artists: string[] = (row.data?.artists ?? []).map((a: any) => a.name).filter(Boolean);
    const labels:  string[] = (row.data?.labels  ?? []).map((l: any) => l.name).filter(Boolean);
    const genres:  string[] = row.data?.genres ?? [];
    const styles:  string[] = row.data?.styles ?? [];
    const year:    number | null = row.data?.year || null;
    await getPool().query(
      `INSERT INTO interest_signals (discogs_release_id, source, artists, labels, genres, styles, year)
       VALUES ($1, 'collection', $2, $3, $4, $5, $6)
       ON CONFLICT (discogs_release_id, source) DO NOTHING`,
      [row.discogs_release_id, artists, labels, genres, styles, year]
    );
    collCount++;
  }
  const want = await getPool().query("SELECT discogs_release_id, data FROM user_wantlist");
  for (const row of want.rows) {
    const artists: string[] = (row.data?.artists ?? []).map((a: any) => a.name).filter(Boolean);
    const labels:  string[] = (row.data?.labels  ?? []).map((l: any) => l.name).filter(Boolean);
    const genres:  string[] = row.data?.genres ?? [];
    const styles:  string[] = row.data?.styles ?? [];
    const year:    number | null = row.data?.year || null;
    await getPool().query(
      `INSERT INTO interest_signals (discogs_release_id, source, artists, labels, genres, styles, year)
       VALUES ($1, 'wantlist', $2, $3, $4, $5, $6)
       ON CONFLICT (discogs_release_id, source) DO NOTHING`,
      [row.discogs_release_id, artists, labels, genres, styles, year]
    );
    wantCount++;
  }
  return { collection: collCount, wantlist: wantCount };
}

// ── Gear listings (eBay) ────────────────────────────────────────────────

export async function upsertGearListings(items: Array<{
  itemId: string; title: string; price: number; currency: string;
  condition?: string; imageUrl?: string; itemUrl?: string;
  locationCity?: string; locationState?: string; locationCountry?: string;
  sellerUsername?: string; sellerFeedback?: number;
  buyingOptions?: string[]; bidCount?: number;
  categories?: string[]; categoryNames?: string[];
  itemEndDate?: string; thumbnailUrl?: string; rawSummary?: object;
}>): Promise<number> {
  let count = 0;
  for (const item of items) {
    await getPool().query(
      `INSERT INTO gear_listings (item_id, title, price, currency, condition, image_url, item_url,
        location_city, location_state, location_country, seller_username, seller_feedback,
        buying_options, bid_count, categories, category_names, item_end_date, thumbnail_url, raw_summary, fetched_at, expired)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,NOW(),false)
       ON CONFLICT (item_id) DO UPDATE SET
         title=$2, price=$3, currency=$4, condition=$5, image_url=$6, item_url=$7,
         location_city=$8, location_state=$9, location_country=$10, seller_username=$11,
         seller_feedback=$12, buying_options=$13, bid_count=$14, categories=$15,
         category_names=$16, item_end_date=$17, thumbnail_url=$18, raw_summary=$19,
         fetched_at=NOW(), expired=false`,
      [item.itemId, item.title, item.price, item.currency,
       item.condition ?? null, item.imageUrl ?? null, item.itemUrl ?? null,
       item.locationCity ?? null, item.locationState ?? null, item.locationCountry ?? null,
       item.sellerUsername ?? null, item.sellerFeedback ?? null,
       item.buyingOptions ?? [], item.bidCount ?? 0,
       item.categories ?? [], item.categoryNames ?? [],
       item.itemEndDate ?? null, item.thumbnailUrl ?? null,
       JSON.stringify(item.rawSummary ?? {})]
    );
    count++;
  }
  return count;
}

export async function updateGearDetail(itemId: string, detailHtml: string, allImages: string[], itemSpecifics: object): Promise<void> {
  await getPool().query(
    `UPDATE gear_listings SET detail_html=$2, all_images=$3, item_specifics=$4, detailed_at=NOW() WHERE item_id=$1`,
    [itemId, detailHtml, allImages, JSON.stringify(itemSpecifics)]
  );
}

export async function getGearNeedingDetail(limit: number = 20): Promise<Array<{ itemId: string; price: number }>> {
  const r = await getPool().query(
    `SELECT item_id, price FROM gear_listings
     WHERE detailed_at IS NULL AND NOT expired
     ORDER BY bid_count DESC, price DESC LIMIT $1`,
    [limit]
  );
  return r.rows.map(row => ({ itemId: row.item_id, price: row.price }));
}

export async function getGearListings(minPrice: number = 0, limit: number = 200, offset: number = 0, sort: string = "bids", q: string = ""): Promise<{ items: any[]; total: number }> {
  const params: any[] = [minPrice];
  let where = `WHERE price >= $1 AND NOT expired
    AND (condition IS NULL OR condition NOT ILIKE '%for parts%')
    AND (item_end_date IS NULL OR item_end_date > NOW())`;
  if (q.trim()) {
    params.push(`%${q.trim()}%`);
    where += ` AND title ILIKE $${params.length}`;
  }
  const countR = await getPool().query(
    `SELECT COUNT(*)::int AS cnt FROM gear_listings ${where}`,
    params
  );
  const total = countR.rows[0]?.cnt ?? 0;
  const orderMap: Record<string, string> = {
    bids: "bid_count DESC, price DESC",
    price_desc: "price DESC",
    price_asc: "price ASC",
    ending: "item_end_date ASC NULLS LAST",
    newest: "fetched_at DESC",
  };
  const orderBy = orderMap[sort] ?? orderMap.bids;
  const r = await getPool().query(
    `SELECT item_id, title, price, currency, condition, image_url, item_url,
       location_city, location_state, location_country,
       seller_username, seller_feedback, buying_options, bid_count,
       categories, category_names, item_end_date,
       detail_html, all_images, item_specifics, thumbnail_url,
       fetched_at, detailed_at
     FROM gear_listings
     ${where}
     ORDER BY ${orderBy}
     LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
    [...params, limit, offset]
  );
  return { items: r.rows, total };
}

export async function markExpiredGearListings(): Promise<number> {
  const r = await getPool().query(
    `UPDATE gear_listings SET expired = true
     WHERE NOT expired AND fetched_at < NOW() - INTERVAL '3 days'`
  );
  return r.rowCount ?? 0;
}

// ── Auto-prune stale data ─────────────────────────────────────────────────
export async function pruneAllStaleData(): Promise<{ interest: number; fresh: number; gear: number; gearLog: number; liveEvents: number }> {
  // Interest signals older than 6 months
  const i = await getPool().query(
    `DELETE FROM interest_signals WHERE recorded_at < NOW() - INTERVAL '6 months'`
  );
  // Fresh releases older than 6 months
  const f = await getPool().query(
    `DELETE FROM fresh_releases WHERE fetched_at < NOW() - INTERVAL '6 months'`
  );
  // Expired gear listings (no longer live auctions)
  const g = await getPool().query(
    `DELETE FROM gear_listings WHERE expired = true`
  );
  // Gear fetch log older than 30 days
  const gl = await getPool().query(
    `DELETE FROM gear_fetch_log WHERE started_at < NOW() - INTERVAL '30 days'`
  );
  // Past live events
  const le = await getPool().query(
    `DELETE FROM live_events WHERE event_date ~ '^\\d{4}-\\d{2}-\\d{2}' AND event_date::date < CURRENT_DATE`
  );
  return {
    interest: i.rowCount ?? 0,
    fresh: f.rowCount ?? 0,
    gear: g.rowCount ?? 0,
    gearLog: gl.rowCount ?? 0,
    liveEvents: le.rowCount ?? 0,
  };
}

// ── Feed articles ─────────────────────────────────────────────────────────
export async function upsertFeedArticle(article: {
  source: string; sourceUrl: string; title: string; summary?: string;
  imageUrl?: string; author?: string; category?: string;
  contentType?: string; publishedAt?: string;
}): Promise<void> {
  await getPool().query(
    `INSERT INTO feed_articles (source, source_url, title, summary, image_url, author, category, content_type, published_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     ON CONFLICT (source_url) DO UPDATE SET
       title = EXCLUDED.title,
       summary = EXCLUDED.summary,
       image_url = COALESCE(EXCLUDED.image_url, feed_articles.image_url),
       author = COALESCE(EXCLUDED.author, feed_articles.author),
       category = EXCLUDED.category,
       published_at = COALESCE(EXCLUDED.published_at, feed_articles.published_at)`,
    [article.source, article.sourceUrl, article.title, article.summary ?? null,
     article.imageUrl ?? null, article.author ?? null, article.category ?? "news",
     article.contentType ?? "article", article.publishedAt ?? null]
  );
}

export async function getFeedArticles(opts: {
  category?: string; limit?: number; offset?: number; q?: string;
}): Promise<{ items: any[]; total: number }> {
  const params: any[] = [];
  let where = "WHERE 1=1";
  if (opts.category && opts.category !== "all") {
    params.push(opts.category);
    where += ` AND category = $${params.length}`;
  }
  if (opts.q?.trim()) {
    params.push(`%${opts.q.trim()}%`);
    where += ` AND (title ILIKE $${params.length} OR summary ILIKE $${params.length} OR source ILIKE $${params.length})`;
  }
  const countR = await getPool().query(`SELECT COUNT(*)::int AS cnt FROM feed_articles ${where}`, params);
  const total = countR.rows[0]?.cnt ?? 0;
  const limit = opts.limit ?? 50;
  const offset = opts.offset ?? 0;
  const r = await getPool().query(
    `SELECT * FROM feed_articles ${where}
     ORDER BY published_at DESC NULLS LAST
     LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
    [...params, limit, offset]
  );
  return { items: r.rows, total };
}

export async function pruneFeedArticles(daysOld: number = 90): Promise<number> {
  const r = await getPool().query(
    `DELETE FROM feed_articles WHERE published_at < NOW() - INTERVAL '1 day' * $1`,
    [daysOld]
  );
  return r.rowCount ?? 0;
}

export async function getGearStats(): Promise<{ total: number; detailed: number; lastFetch: string | null }> {
  const r = await getPool().query(
    `SELECT COUNT(*)::int AS total,
       COUNT(detailed_at)::int AS detailed,
       MAX(fetched_at) AS last_fetch
     FROM gear_listings WHERE NOT expired`
  );
  return {
    total: r.rows[0]?.total ?? 0,
    detailed: r.rows[0]?.detailed ?? 0,
    lastFetch: r.rows[0]?.last_fetch ?? null,
  };
}

export async function logGearFetch(fetchType: string, itemCount: number, error?: string): Promise<void> {
  await getPool().query(
    `INSERT INTO gear_fetch_log (fetch_type, item_count, error, finished_at)
     VALUES ($1, $2, $3, NOW())`,
    [fetchType, itemCount, error ?? null]
  );
}

// ── Live events (Ticketmaster upcoming) ─────────────────────────────────
export async function upsertLiveEvents(events: Array<{
  name: string; artist: string; date: string; time: string;
  venue: string; venueId: string; city: string; region: string;
  country: string; url: string;
}>): Promise<number> {
  let count = 0;
  for (const ev of events) {
    await getPool().query(
      `INSERT INTO live_events (event_name, artist, event_date, event_time, venue, venue_id, city, region, country, url, fetched_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
       ON CONFLICT (url) DO UPDATE SET
         event_name = EXCLUDED.event_name,
         artist = EXCLUDED.artist,
         event_date = EXCLUDED.event_date,
         event_time = EXCLUDED.event_time,
         venue = EXCLUDED.venue,
         venue_id = EXCLUDED.venue_id,
         city = EXCLUDED.city,
         region = EXCLUDED.region,
         country = EXCLUDED.country,
         fetched_at = NOW()`,
      [ev.name, ev.artist, ev.date, ev.time, ev.venue, ev.venueId, ev.city, ev.region, ev.country, ev.url]
    );
    count++;
  }
  return count;
}

export async function getLiveEvents(limit: number = 30): Promise<object[]> {
  const r = await getPool().query(
    `SELECT event_name AS name, artist, event_date AS date, event_time AS time,
            venue, venue_id AS "venueId", city, region, country, url
     FROM live_events
     WHERE event_date ~ '^\\d{4}-\\d{2}-\\d{2}' AND event_date::date >= CURRENT_DATE
     ORDER BY event_date ASC, event_time ASC
     LIMIT $1`,
    [limit]
  );
  return r.rows;
}

export async function pruneLiveEvents(): Promise<number> {
  // Remove events that have already passed
  const r = await getPool().query(
    `DELETE FROM live_events WHERE event_date ~ '^\\d{4}-\\d{2}-\\d{2}' AND event_date::date < CURRENT_DATE`
  );
  return r.rowCount ?? 0;
}
