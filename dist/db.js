import pg from "pg";
const { Pool } = pg;
let pool = null;
function getPool() {
    if (!pool) {
        const connStr = process.env.APP_DB_URL;
        if (!connStr)
            throw new Error("APP_DB_URL not set");
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
}
export async function saveSearch(clerkUserId, params) {
    // Skip if identical params were saved in the last 5 minutes (prevents double-saves)
    const recent = await getPool().query(`SELECT 1 FROM search_history
     WHERE clerk_user_id = $1 AND params = $2
       AND searched_at > NOW() - INTERVAL '5 minutes'
     LIMIT 1`, [clerkUserId, JSON.stringify(params)]);
    if (recent.rows.length)
        return;
    await getPool().query(`INSERT INTO search_history (clerk_user_id, params) VALUES ($1, $2)`, [clerkUserId, JSON.stringify(params)]);
    // Keep only the most recent 500 searches per user
    await getPool().query(`DELETE FROM search_history
     WHERE clerk_user_id = $1
       AND id NOT IN (
         SELECT id FROM search_history
         WHERE clerk_user_id = $1
         ORDER BY searched_at DESC
         LIMIT 500
       )`, [clerkUserId]);
}
export async function markSearchBio(clerkUserId) {
    // Add b=y to the params of the most recent search
    await getPool().query(`UPDATE search_history SET params = params || '{"b":"y"}'::jsonb
     WHERE id = (
       SELECT id FROM search_history
       WHERE clerk_user_id = $1
       ORDER BY searched_at DESC
       LIMIT 1
     )`, [clerkUserId]);
}
export async function deleteSearch(clerkUserId, params) {
    await getPool().query("DELETE FROM search_history WHERE clerk_user_id = $1 AND params = $2", [clerkUserId, JSON.stringify(params)]);
}
export async function clearSearchHistory(clerkUserId) {
    await getPool().query("DELETE FROM search_history WHERE clerk_user_id = $1", [clerkUserId]);
}
export async function deleteSearchGlobal(params) {
    await getPool().query("DELETE FROM search_history WHERE params = $1", [JSON.stringify(params)]);
}
export async function deleteSearchById(id) {
    await getPool().query("DELETE FROM search_history WHERE id = $1", [id]);
}
export async function getSearchHistory(clerkUserId, limit = 50) {
    const r = await getPool().query(`SELECT params, MAX(searched_at) AS searched_at
     FROM search_history
     WHERE clerk_user_id = $1 AND NOT (params ? '_type')
     GROUP BY params
     ORDER BY MAX(searched_at) DESC
     LIMIT $2`, [clerkUserId, limit]);
    return r.rows;
}
export async function getAllUsersSyncStatus() {
    const r = await getPool().query(`SELECT discogs_username, collection_synced_at, wantlist_synced_at,
            sync_status, sync_progress, sync_total, sync_error
     FROM user_tokens
     WHERE discogs_username IS NOT NULL
     ORDER BY discogs_username`);
    return r.rows.map(row => ({
        username: row.discogs_username,
        collectionSyncedAt: row.collection_synced_at ?? null,
        wantlistSyncedAt: row.wantlist_synced_at ?? null,
        syncStatus: row.sync_status ?? "idle",
        syncProgress: row.sync_progress ?? 0,
        syncTotal: row.sync_total ?? 0,
        syncError: row.sync_error ?? null,
    }));
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
export async function getRecentSearches(limit = 300) {
    // Grab the latest `limit` unique searches, then randomise in the API layer
    const r = await getPool().query(`SELECT params, searched_at FROM (
       SELECT DISTINCT ON (params) params, searched_at
       FROM search_history
       WHERE NOT (params ? '_type')
       ORDER BY params, searched_at DESC
     ) sub
     ORDER BY searched_at DESC
     LIMIT $1`, [limit]);
    return r.rows;
}
export async function getRecentLiveSearches(limit = 200) {
    const r = await getPool().query(`SELECT params, searched_at FROM (
       SELECT DISTINCT ON (params) params, searched_at
       FROM search_history
       WHERE params->>'_type' = 'live'
       ORDER BY params, searched_at DESC
     ) sub
     ORDER BY searched_at DESC
     LIMIT $1`, [limit]);
    return r.rows;
}
export async function dumpSearchHistory() {
    const r = await getPool().query(`SELECT id, clerk_user_id, params, searched_at FROM search_history ORDER BY searched_at DESC`);
    return r.rows;
}
export async function truncateSearchHistory() {
    const r = await getPool().query(`DELETE FROM search_history`);
    return r.rowCount ?? 0;
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
    await getPool().query("DELETE FROM user_tokens    WHERE clerk_user_id = $1", [clerkUserId]);
    await getPool().query("DELETE FROM search_history WHERE clerk_user_id = $1", [clerkUserId]);
    await getPool().query("DELETE FROM user_collection WHERE clerk_user_id = $1", [clerkUserId]);
    await getPool().query("DELETE FROM user_wantlist   WHERE clerk_user_id = $1", [clerkUserId]);
}
export async function getDiscogsUsername(clerkUserId) {
    const r = await getPool().query("SELECT discogs_username FROM user_tokens WHERE clerk_user_id = $1", [clerkUserId]);
    return r.rows[0]?.discogs_username ?? null;
}
export async function setDiscogsUsername(clerkUserId, username) {
    await getPool().query(`UPDATE user_tokens SET discogs_username = $2 WHERE clerk_user_id = $1`, [clerkUserId, username]);
}
export async function updateSyncProgress(clerkUserId, status, progress, total, error) {
    await getPool().query(`UPDATE user_tokens SET sync_status = $2, sync_progress = $3, sync_total = $4, sync_error = $5 WHERE clerk_user_id = $1`, [clerkUserId, status, progress, total, error ?? null]);
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
export async function upsertCollectionItems(clerkUserId, items) {
    for (const item of items) {
        await getPool().query(`INSERT INTO user_collection (clerk_user_id, discogs_release_id, data, added_at, synced_at, folder_id)
       VALUES ($1, $2, $3, $4, NOW(), $5)
       ON CONFLICT (clerk_user_id, discogs_release_id)
       DO UPDATE SET data = $3, added_at = $4, synced_at = NOW(), folder_id = $5`, [clerkUserId, item.id, JSON.stringify(item.data), item.addedAt ?? null, item.folderId ?? 0]);
    }
}
export async function upsertCollectionFolders(clerkUserId, folders) {
    // Clear old folders and re-insert
    await getPool().query(`DELETE FROM user_collection_folders WHERE clerk_user_id = $1`, [clerkUserId]);
    for (const f of folders) {
        await getPool().query(`INSERT INTO user_collection_folders (clerk_user_id, folder_id, folder_name, item_count)
       VALUES ($1, $2, $3, $4)`, [clerkUserId, f.id, f.name, f.count]);
    }
}
export async function getCollectionFolderList(clerkUserId) {
    const r = await getPool().query(`SELECT folder_id, folder_name, item_count FROM user_collection_folders
     WHERE clerk_user_id = $1 ORDER BY folder_name ASC`, [clerkUserId]);
    return r.rows.map(row => ({ folderId: row.folder_id, name: row.folder_name, count: row.item_count }));
}
export async function upsertWantlistItems(clerkUserId, items) {
    for (const item of items) {
        await getPool().query(`INSERT INTO user_wantlist (clerk_user_id, discogs_release_id, data, added_at, synced_at)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (clerk_user_id, discogs_release_id)
       DO UPDATE SET data = $3, added_at = $4, synced_at = NOW()`, [clerkUserId, item.id, JSON.stringify(item.data), item.addedAt ?? null]);
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
function buildCwWhere(filters, startIdx) {
    const clauses = [];
    const allParams = [];
    let idx = startIdx;
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
    return { clause: clauses.length ? " AND " + clauses.join(" AND ") : "", params: allParams };
}
export async function getCollectionPage(clerkUserId, page, perPage, filters) {
    const offset = (page - 1) * perPage;
    const { clause: dataClause, params: dataFilterParams } = buildCwWhere(filters ?? {}, 4);
    const { clause: countClause, params: countFilterParams } = buildCwWhere(filters ?? {}, 2);
    const [dataR, countR] = await Promise.all([
        getPool().query(`SELECT data FROM user_collection WHERE clerk_user_id = $1${dataClause}
       ORDER BY LOWER(data->'artists'->0->>'name') ASC, LOWER(data->>'title') ASC
       LIMIT $2 OFFSET $3`, [clerkUserId, perPage, offset, ...dataFilterParams]),
        getPool().query(`SELECT COUNT(*)::int AS total FROM user_collection WHERE clerk_user_id = $1${countClause}`, [clerkUserId, ...countFilterParams]),
    ]);
    return { items: dataR.rows.map(r => r.data), total: countR.rows[0]?.total ?? 0 };
}
export async function getWantlistPage(clerkUserId, page, perPage, filters) {
    const offset = (page - 1) * perPage;
    const { clause: dataClause, params: dataFilterParams } = buildCwWhere(filters ?? {}, 4);
    const { clause: countClause, params: countFilterParams } = buildCwWhere(filters ?? {}, 2);
    const [dataR, countR] = await Promise.all([
        getPool().query(`SELECT data FROM user_wantlist WHERE clerk_user_id = $1${dataClause}
       ORDER BY LOWER(data->'artists'->0->>'name') ASC, LOWER(data->>'title') ASC
       LIMIT $2 OFFSET $3`, [clerkUserId, perPage, offset, ...dataFilterParams]),
        getPool().query(`SELECT COUNT(*)::int AS total FROM user_wantlist WHERE clerk_user_id = $1${countClause}`, [clerkUserId, ...countFilterParams]),
    ]);
    return { items: dataR.rows.map(r => r.data), total: countR.rows[0]?.total ?? 0 };
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
    const r = await getPool().query("SELECT discogs_release_id FROM user_collection WHERE clerk_user_id = $1", [clerkUserId]);
    return r.rows.map(row => row.discogs_release_id);
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
export async function updateCollectionSyncedAt(clerkUserId) {
    await getPool().query("UPDATE user_tokens SET collection_synced_at = NOW() WHERE clerk_user_id = $1", [clerkUserId]);
}
export async function updateWantlistSyncedAt(clerkUserId) {
    await getPool().query("UPDATE user_tokens SET wantlist_synced_at = NOW() WHERE clerk_user_id = $1", [clerkUserId]);
}
export async function upsertFreshRelease(r) {
    await getPool().query(`INSERT INTO fresh_releases
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
       fetched_at         = NOW()`, [r.release_mbid, r.release_name, r.artist_credit_name, r.release_date,
        r.primary_type, r.secondary_type, r.tags, r.caa_id, r.caa_release_mbid, r.cover_url,
        r.release_group_mbid, r.artist_mbids]);
}
export async function getFreshStats() {
    const r = await getPool().query(`SELECT COUNT(*)::int AS count,
            MIN(release_date) AS oldest,
            MAX(release_date) AS newest,
            (SELECT COUNT(DISTINCT t)::int FROM fresh_releases, unnest(tags) AS t) AS tag_count
     FROM fresh_releases`);
    return {
        count: r.rows[0]?.count ?? 0,
        oldest: r.rows[0]?.oldest ?? null,
        newest: r.rows[0]?.newest ?? null,
        tagCount: r.rows[0]?.tag_count ?? 0,
    };
}
export async function pruneFreshReleases() {
    const r = await getPool().query(`DELETE FROM fresh_releases WHERE fetched_at < NOW() - INTERVAL '3 months'`);
    return r.rowCount ?? 0;
}
export async function getFreshReleases(limit = 150) {
    // Random sample from last 3 months — all loaded at once for client-side filtering
    const r = await getPool().query(`SELECT release_mbid, release_name, artist_credit_name, release_date,
            primary_type, secondary_type, tags, caa_release_mbid, cover_url,
            release_group_mbid, artist_mbids
     FROM fresh_releases
     WHERE fetched_at > NOW() - INTERVAL '3 months'
     ORDER BY RANDOM()
     LIMIT $1`, [limit]);
    return r.rows;
}
export async function searchFreshReleases(query, limit = 200) {
    const pattern = `%${query}%`;
    const r = await getPool().query(`SELECT release_mbid, release_name, artist_credit_name, release_date,
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
     LIMIT $2`, [pattern, limit]);
    return r.rows;
}
export async function getFreshTopTags(limit = 24) {
    // Random selection from all tags that appear on at least 1 release in the last 14 days.
    const r = await getPool().query(`SELECT unnest(tags) AS tag, COUNT(*)::int AS cnt
     FROM fresh_releases
     WHERE fetched_at > NOW() - INTERVAL '28 days'
     GROUP BY tag
     ORDER BY RANDOM()
     LIMIT $1`, [limit]);
    return r.rows;
}
// ── Interest signals (anonymous, survives account deletion) ──
export async function recordInterestSignals(items, source) {
    for (const item of items) {
        const d = item.data;
        if (!d)
            continue;
        const artists = (d.artists ?? []).map((a) => a.name).filter(Boolean);
        const labels = (d.labels ?? []).map((l) => l.name).filter(Boolean);
        const genres = d.genres ?? [];
        const styles = d.styles ?? [];
        const year = d.year || null;
        await getPool().query(`INSERT INTO interest_signals (discogs_release_id, source, artists, labels, genres, styles, year)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (discogs_release_id, source)
       DO UPDATE SET artists = $3, labels = $4, genres = $5, styles = $6, year = $7, recorded_at = NOW()`, [item.id, source, artists, labels, genres, styles, year]);
    }
}
export async function getInterestStats() {
    const [totalR, artistsR, labelsR, genresR, stylesR] = await Promise.all([
        getPool().query("SELECT COUNT(*)::int AS cnt FROM interest_signals"),
        getPool().query(`SELECT unnest(artists) AS name, COUNT(*)::int AS cnt
       FROM interest_signals GROUP BY name ORDER BY cnt DESC LIMIT 50`),
        getPool().query(`SELECT unnest(labels) AS name, COUNT(*)::int AS cnt
       FROM interest_signals GROUP BY name ORDER BY cnt DESC LIMIT 50`),
        getPool().query(`SELECT unnest(genres) AS name, COUNT(*)::int AS cnt
       FROM interest_signals GROUP BY name ORDER BY cnt DESC LIMIT 30`),
        getPool().query(`SELECT unnest(styles) AS name, COUNT(*)::int AS cnt
       FROM interest_signals GROUP BY name ORDER BY cnt DESC LIMIT 50`),
    ]);
    return {
        totalReleases: totalR.rows[0]?.cnt ?? 0,
        topArtists: artistsR.rows,
        topLabels: labelsR.rows,
        topGenres: genresR.rows,
        topStyles: stylesR.rows,
    };
}
export async function backfillInterestSignals() {
    let collCount = 0;
    let wantCount = 0;
    const coll = await getPool().query("SELECT discogs_release_id, data FROM user_collection");
    for (const row of coll.rows) {
        const artists = (row.data?.artists ?? []).map((a) => a.name).filter(Boolean);
        const labels = (row.data?.labels ?? []).map((l) => l.name).filter(Boolean);
        const genres = row.data?.genres ?? [];
        const styles = row.data?.styles ?? [];
        const year = row.data?.year || null;
        await getPool().query(`INSERT INTO interest_signals (discogs_release_id, source, artists, labels, genres, styles, year)
       VALUES ($1, 'collection', $2, $3, $4, $5, $6)
       ON CONFLICT (discogs_release_id, source) DO NOTHING`, [row.discogs_release_id, artists, labels, genres, styles, year]);
        collCount++;
    }
    const want = await getPool().query("SELECT discogs_release_id, data FROM user_wantlist");
    for (const row of want.rows) {
        const artists = (row.data?.artists ?? []).map((a) => a.name).filter(Boolean);
        const labels = (row.data?.labels ?? []).map((l) => l.name).filter(Boolean);
        const genres = row.data?.genres ?? [];
        const styles = row.data?.styles ?? [];
        const year = row.data?.year || null;
        await getPool().query(`INSERT INTO interest_signals (discogs_release_id, source, artists, labels, genres, styles, year)
       VALUES ($1, 'wantlist', $2, $3, $4, $5, $6)
       ON CONFLICT (discogs_release_id, source) DO NOTHING`, [row.discogs_release_id, artists, labels, genres, styles, year]);
        wantCount++;
    }
    return { collection: collCount, wantlist: wantCount };
}
