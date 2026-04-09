// One-shot script: nukes all per-user data EXCEPT the admin's rows.
//
// Usage:
//   APP_DB_URL="postgresql://..." ADMIN_CLERK_ID="user_xxx" node scripts/purge-non-admin-users.js
//
// Add --dry-run to preview row counts without deleting.
//
// This wipes every per-user table for everyone whose clerk_user_id is not
// the admin. Use this when locking SeaDisco down to invite-only mode.

import pg from "pg";

const { Pool } = pg;

const dryRun = process.argv.includes("--dry-run");
const adminId = process.env.ADMIN_CLERK_ID ?? "";
const dbUrl   = process.env.APP_DB_URL ?? "";

if (!dbUrl) {
  console.error("ERROR: APP_DB_URL env var not set");
  process.exit(1);
}
if (!adminId) {
  console.error("ERROR: ADMIN_CLERK_ID env var not set");
  process.exit(1);
}

const pool = new Pool({ connectionString: dbUrl, ssl: { rejectUnauthorized: false } });

// Per-user tables, ordered with FK-children first (triggered_alerts → price_alerts).
// user_tokens last so it doesn't break any references mid-run.
const tables = [
  "triggered_alerts",
  "price_alerts",
  "user_order_messages",
  "user_orders",
  "user_list_items",
  "user_lists",
  "user_inventory",
  "user_wantlist",
  "user_collection",
  "user_collection_folders",
  "user_favorites",
  "saved_searches",
  "feedback",
  "oauth_request_tokens",
  "user_tokens",
];

async function main() {
  console.log(`Mode:        ${dryRun ? "DRY RUN (no deletes)" : "LIVE (will delete)"}`);
  console.log(`Admin ID:    ${adminId}`);
  console.log(`Database:    ${dbUrl.replace(/:[^:@]*@/, ":***@")}`);
  console.log("");

  let total = 0;
  for (const table of tables) {
    try {
      if (dryRun) {
        const r = await pool.query(
          `SELECT COUNT(*)::int AS n FROM ${table} WHERE clerk_user_id <> $1`,
          [adminId]
        );
        const n = r.rows[0]?.n ?? 0;
        console.log(`  ${table.padEnd(28)} ${String(n).padStart(8)} rows would be deleted`);
        total += n;
      } else {
        const r = await pool.query(
          `DELETE FROM ${table} WHERE clerk_user_id <> $1`,
          [adminId]
        );
        const n = r.rowCount ?? 0;
        console.log(`  ${table.padEnd(28)} ${String(n).padStart(8)} rows deleted`);
        total += n;
      }
    } catch (e) {
      console.warn(`  ${table.padEnd(28)} SKIP (${e.message})`);
    }
  }

  console.log("");
  console.log(`Total: ${total} rows ${dryRun ? "would be" : ""} deleted.`);

  await pool.end();
}

main().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
