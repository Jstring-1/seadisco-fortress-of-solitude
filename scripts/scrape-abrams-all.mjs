// Iterate scripts/abrams-labels.json and scrape every 78discography.com
// label-series page, emitting one combined JSON file ready for batched
// ingest.
//
//   node scripts/scrape-abrams-all.mjs [--out=scratch/abrams-all.json]
//                                      [--delay=2000]
//                                      [--limit=5]   (debug)
//
// Resumable: re-runs skip labels already saved to --out. Polite scrape
// with configurable delay and a real User-Agent.
//
// Output is an array of per-label-series payloads, each shaped like the
// admin import endpoint expects:
//   { label, labelId, source: "78discography.com", rows: [...] }

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SEEDS_PATH = path.join(__dirname, "abrams-labels.json");

const argv = Object.fromEntries(
  process.argv.slice(2).map(a => a.split("=", 2)).map(([k, v]) => [k.replace(/^--/, ""), v ?? true]),
);
const OUT_PATH = argv.out ?? path.join(__dirname, "..", "scratch", "abrams-all.json");
const DELAY_MS = Number(argv.delay ?? 2000);
const LIMIT    = argv.limit ? Number(argv.limit) : null;

const seeds = JSON.parse(fs.readFileSync(SEEDS_PATH, "utf8"));

let acc = [];
if (fs.existsSync(OUT_PATH)) {
  try {
    acc = JSON.parse(fs.readFileSync(OUT_PATH, "utf8"));
    console.error(`[scrape] resuming from ${acc.length} previously-fetched pages`);
  } catch {}
}
// Resume key is the slug (unique per page) — not the label name (which
// repeats across multi-series labels like Aladdin/Bluebird).
const done = new Set(acc.map(p => p._slug));

fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });

let nFetched = 0;
for (const seed of seeds) {
  if (LIMIT && nFetched >= LIMIT) break;
  if (done.has(seed.slug)) {
    console.error(`[scrape:skip] ${seed.slug} (${seed.name}) — already in ${OUT_PATH}`);
    continue;
  }

  await new Promise(r => setTimeout(r, DELAY_MS));
  console.error(`[scrape] ${seed.slug} → ${seed.name}`);
  const payload = await runParser(seed);
  if (payload && Array.isArray(payload.rows) && payload.rows.length > 0) {
    payload._slug = seed.slug;            // resume key
    acc.push(payload);
    fs.writeFileSync(OUT_PATH, JSON.stringify(acc, null, 2));
  } else {
    console.error(`[scrape:empty] ${seed.slug} produced no rows`);
  }
  nFetched += 1;
}

console.error(`[scrape] done — ${acc.length} pages, ${acc.reduce((n, p) => n + p.rows.length, 0)} rows in ${OUT_PATH}`);

function runParser(seed) {
  return new Promise((resolve) => {
    const args = [
      path.join(__dirname, "parse-abrams-label.mjs"),
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
