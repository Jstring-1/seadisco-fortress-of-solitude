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
      fetched_at          TIMESTAMPTZ DEFAULT NOW()
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
  // Keep only the most recent 200 searches per user
  await getPool().query(
    `DELETE FROM search_history
     WHERE clerk_user_id = $1
       AND id NOT IN (
         SELECT id FROM search_history
         WHERE clerk_user_id = $1
         ORDER BY searched_at DESC
         LIMIT 200
       )`,
    [clerkUserId]
  );
}

export async function getSearchHistory(clerkUserId: string, limit = 50): Promise<Array<{ params: Record<string, string>; searched_at: string }>> {
  const r = await getPool().query(
    `SELECT params, MAX(searched_at) AS searched_at
     FROM search_history
     WHERE clerk_user_id = $1
     GROUP BY params
     ORDER BY MAX(searched_at) DESC
     LIMIT $2`,
    [clerkUserId, limit]
  );
  return r.rows;
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

export async function getRecentSearches(limit = 20): Promise<Array<{ params: Record<string, string>; searched_at: string }>> {
  const r = await getPool().query(
    `SELECT params, MAX(searched_at) AS searched_at
     FROM search_history
     GROUP BY params
     ORDER BY MAX(searched_at) DESC
     LIMIT $1`,
    [limit]
  );
  return r.rows;
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

export async function getSyncStatus(clerkUserId: string): Promise<{ collectionSyncedAt: Date | null; wantlistSyncedAt: Date | null }> {
  const r = await getPool().query(
    "SELECT collection_synced_at, wantlist_synced_at FROM user_tokens WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return {
    collectionSyncedAt: r.rows[0]?.collection_synced_at ?? null,
    wantlistSyncedAt:   r.rows[0]?.wantlist_synced_at   ?? null,
  };
}

export async function upsertCollectionItems(
  clerkUserId: string,
  items: Array<{ id: number; data: object; addedAt?: Date }>
): Promise<void> {
  for (const item of items) {
    await getPool().query(
      `INSERT INTO user_collection (clerk_user_id, discogs_release_id, data, added_at, synced_at)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (clerk_user_id, discogs_release_id)
       DO UPDATE SET data = $3, added_at = $4, synced_at = NOW()`,
      [clerkUserId, item.id, JSON.stringify(item.data), item.addedAt ?? null]
    );
  }
}

export async function upsertWantlistItems(
  clerkUserId: string,
  items: Array<{ id: number; data: object; addedAt?: Date }>
): Promise<void> {
  for (const item of items) {
    await getPool().query(
      `INSERT INTO user_wantlist (clerk_user_id, discogs_release_id, data, added_at, synced_at)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (clerk_user_id, discogs_release_id)
       DO UPDATE SET data = $3, added_at = $4, synced_at = NOW()`,
      [clerkUserId, item.id, JSON.stringify(item.data), item.addedAt ?? null]
    );
  }
}

export async function getCollectionPage(
  clerkUserId: string,
  page: number,
  perPage: number
): Promise<{ items: any[]; total: number }> {
  const offset = (page - 1) * perPage;
  const [dataR, countR] = await Promise.all([
    getPool().query(
      `SELECT data FROM user_collection WHERE clerk_user_id = $1
       ORDER BY added_at DESC NULLS LAST, id DESC
       LIMIT $2 OFFSET $3`,
      [clerkUserId, perPage, offset]
    ),
    getPool().query(
      "SELECT COUNT(*)::int AS total FROM user_collection WHERE clerk_user_id = $1",
      [clerkUserId]
    ),
  ]);
  return { items: dataR.rows.map(r => r.data), total: countR.rows[0]?.total ?? 0 };
}

export async function getWantlistPage(
  clerkUserId: string,
  page: number,
  perPage: number
): Promise<{ items: any[]; total: number }> {
  const offset = (page - 1) * perPage;
  const [dataR, countR] = await Promise.all([
    getPool().query(
      `SELECT data FROM user_wantlist WHERE clerk_user_id = $1
       ORDER BY added_at DESC NULLS LAST, id DESC
       LIMIT $2 OFFSET $3`,
      [clerkUserId, perPage, offset]
    ),
    getPool().query(
      "SELECT COUNT(*)::int AS total FROM user_wantlist WHERE clerk_user_id = $1",
      [clerkUserId]
    ),
  ]);
  return { items: dataR.rows.map(r => r.data), total: countR.rows[0]?.total ?? 0 };
}

export async function getCollectionIds(clerkUserId: string): Promise<number[]> {
  const r = await getPool().query(
    "SELECT discogs_release_id FROM user_collection WHERE clerk_user_id = $1",
    [clerkUserId]
  );
  return r.rows.map(row => row.discogs_release_id);
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
}): Promise<void> {
  await getPool().query(
    `INSERT INTO fresh_releases
       (release_mbid, release_name, artist_credit_name, release_date, primary_type, secondary_type, tags, caa_id, caa_release_mbid, cover_url, fetched_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
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
       fetched_at         = NOW()`,
    [r.release_mbid, r.release_name, r.artist_credit_name, r.release_date,
     r.primary_type, r.secondary_type, r.tags, r.caa_id, r.caa_release_mbid, r.cover_url]
  );
}

export async function pruneFreshReleases(): Promise<number> {
  const r = await getPool().query(
    `DELETE FROM fresh_releases WHERE fetched_at < NOW() - INTERVAL '14 days'`
  );
  return r.rowCount ?? 0;
}

export async function getFreshReleases(limit = 48): Promise<any[]> {
  // Random sample from last 14 days, all genres
  const r = await getPool().query(
    `SELECT release_mbid, release_name, artist_credit_name, release_date,
            primary_type, secondary_type, tags, caa_release_mbid, cover_url
     FROM fresh_releases
     WHERE fetched_at > NOW() - INTERVAL '14 days'
     ORDER BY RANDOM()
     LIMIT $1`,
    [limit]
  );
  return r.rows;
}

export async function getFreshReleasesByTag(tag: string, limit = 48): Promise<any[]> {
  // Most recent releases with a specific tag, all genres
  const r = await getPool().query(
    `SELECT release_mbid, release_name, artist_credit_name, release_date,
            primary_type, secondary_type, tags, caa_release_mbid, cover_url
     FROM fresh_releases
     WHERE fetched_at > NOW() - INTERVAL '14 days'
       AND $2 = ANY(tags)
     ORDER BY release_date DESC NULLS LAST, fetched_at DESC
     LIMIT $1`,
    [limit, tag]
  );
  return r.rows;
}

export async function getFreshTopTags(limit = 12): Promise<Array<{ tag: string; cnt: number }>> {
  const r = await getPool().query(
    `SELECT unnest(tags) AS tag, COUNT(*)::int AS cnt
     FROM fresh_releases
     WHERE fetched_at > NOW() - INTERVAL '14 days'
     GROUP BY tag
     ORDER BY cnt DESC
     LIMIT $1`,
    [limit]
  );
  return r.rows;
}
