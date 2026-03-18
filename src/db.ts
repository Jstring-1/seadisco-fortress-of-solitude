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
}

export async function saveSearch(clerkUserId: string, params: Record<string, string>): Promise<void> {
  await getPool().query(
    `INSERT INTO search_history (clerk_user_id, params) VALUES ($1, $2)`,
    [clerkUserId, JSON.stringify(params)]
  );
  // Keep only the most recent 50 searches per user
  await getPool().query(
    `DELETE FROM search_history
     WHERE clerk_user_id = $1
       AND id NOT IN (
         SELECT id FROM search_history
         WHERE clerk_user_id = $1
         ORDER BY searched_at DESC
         LIMIT 50
       )`,
    [clerkUserId]
  );
}

export async function getSearchHistory(clerkUserId: string, limit = 25): Promise<Array<{ params: Record<string, string>; searched_at: string }>> {
  const r = await getPool().query(
    `SELECT params, searched_at FROM search_history
     WHERE clerk_user_id = $1
     ORDER BY searched_at DESC LIMIT $2`,
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
