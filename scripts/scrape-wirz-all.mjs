// Iterate scripts/wirz-labels.json and scrape every label page on
// wirz.de, emitting one combined JSON file ready for batched ingest.
//
//   node scripts/scrape-wirz-all.mjs [--out=scratch/wirz-all.json]
//                                    [--delay=2000]
//                                    [--limit=5]   (debug)
//
// The output is an array of per-label payloads, each shaped like the
// admin import endpoint expects:
//   { label, labelId, source, rows: [{ catno, side, artist, title, year, … }] }
//
// Run separately, then POST each entry to the import endpoint:
//   node -e "JSON.parse(fs.readFileSync('wirz-all.json')).forEach(p =>
//     fetch('http://localhost:5000/api/admin/external-discography/import',
//       { method: 'POST', headers: { 'content-type': 'application/json',
//         cookie: '__session=…' }, body: JSON.stringify(p) }))"
//
// Polite scrape: configurable delay (default 2s), real User-Agent,
// resumable (skips labels already present in --out if it exists).

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SEEDS_PATH = path.join(__dirname, "wirz-labels.json");

const argv = Object.fromEntries(
  process.argv.slice(2).map(a => a.split("=", 2)).map(([k, v]) => [k.replace(/^--/, ""), v ?? true]),
);
const OUT_PATH = argv.out ?? path.join(__dirname, "..", "scratch", "wirz-all.json");
const DELAY_MS = Number(argv.delay ?? 2000);
const LIMIT    = argv.limit ? Number(argv.limit) : null;

const seeds = JSON.parse(fs.readFileSync(SEEDS_PATH, "utf8"));

// Resume support: if OUT_PATH already exists, skip labels we already
// fetched. Lets a re-run pick up where a crash / Ctrl-C stopped.
let acc = [];
if (fs.existsSync(OUT_PATH)) {
  try {
    acc = JSON.parse(fs.readFileSync(OUT_PATH, "utf8"));
    console.error(`[scrape] resuming from ${acc.length} previously-fetched labels`);
  } catch {}
}
const done = new Set(acc.map(p => p.label));

fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });

let nFetched = 0;
for (const seed of seeds) {
  if (LIMIT && nFetched >= LIMIT) break;
  if (done.has(seed.name)) {
    console.error(`[scrape:skip] ${seed.name} (already in ${OUT_PATH})`);
    continue;
  }

  await new Promise(r => setTimeout(r, DELAY_MS));
  console.error(`[scrape] ${seed.slug} → ${seed.name}`);
  const payload = await runParser(seed);
  if (payload && Array.isArray(payload.rows) && payload.rows.length > 0) {
    acc.push(payload);
    fs.writeFileSync(OUT_PATH, JSON.stringify(acc, null, 2));
  } else {
    console.error(`[scrape:empty] ${seed.slug} produced no rows`);
  }
  nFetched += 1;
}

console.error(`[scrape] done — ${acc.length} labels, ${acc.reduce((n, p) => n + p.rows.length, 0)} rows in ${OUT_PATH}`);

function runParser(seed) {
  return new Promise((resolve) => {
    const args = [
      path.join(__dirname, "parse-wirz-label.mjs"),
      seed.slug,
      seed.name,
    ];
    if (seed.labelId) args.push(String(seed.labelId));
    const child = spawn("node", args);
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => { stdout += d.toString(); });
    child.stderr.on("data", (d) => { stderr += d.toString(); });
    child.on("close", () => {
      if (stderr) process.stderr.write(stderr);
      try { resolve(JSON.parse(stdout)); }
      catch { resolve(null); }
    });
  });
}
