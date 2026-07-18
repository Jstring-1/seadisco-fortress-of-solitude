#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────
// Discogs monthly data-dump loader.
//
// Streams a Discogs XML dump (releases / masters / artists / labels),
// transforms each record into the same API-shaped JSON blob the site's
// live cache stores, and upserts it into `release_cache` (type =
// 'release' | 'master' | 'artist' | 'label'). Constant memory — the gz
// is streamed through a SAX parser, one record built at a time.
//
// After loading, run the admin "Project into split cache" backfill so
// masters/pressings/side-tables get populated (this script writes only
// release_cache; the app's projector handles the split from there).
//
// Usage:
//   node scripts/load-discogs-dump.mjs --type releases \
//        --file /path/discogs_YYYYMMDD_releases.xml.gz [options]
//
// Options:
//   --type   releases|masters|artists|labels   (required)
//   --file   path to .xml.gz or .xml           (required)
//   --dry-run           parse + transform, print the first record's JSON,
//                       write NOTHING. Use to eyeball the mapping first.
//   --limit N           stop after N *kept* records (sizing / spot-check)
//   --since-year YYYY    releases/masters: skip records whose year < YYYY
//   --genres "A,B,C"     releases/masters: keep only if a genre matches
//                       (case-insensitive; records with no genre are
//                       dropped when this filter is set)
//   --batch N            upsert batch size (default 800)
//   --skip-existing      skip ids already present in release_cache for
//                       this type (resumable re-runs; costs one lookup
//                       per batch)
//   --warm              mark rows engaged (seen_at = NOW). Default is
//                       warm-only (seen_at NULL) so a mass import doesn't
//                       flood the public feed.
//   --progress N         log every N scanned records (default 50000)
//
// Env: DATABASE_URL (required), DB_CA_CERT (optional, mirrors the app).
//
// NOTE: Discogs strips image URLs from the dumps — dump-loaded records
// have empty <image> uris, so cards/modals show no cover art until the
// live API refetches them. Everything else (tracklist, videos, credits,
// year, community counts) is present.
// ─────────────────────────────────────────────────────────────────────

import fs from "node:fs";
import zlib from "node:zlib";
import sax from "sax";
import pg from "pg";

// ── args ─────────────────────────────────────────────────────────────
function parseArgs(argv) {
  const out = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const key = a.slice(2);
      const next = argv[i + 1];
      if (next === undefined || next.startsWith("--")) { out[key] = true; }
      else { out[key] = next; i++; }
    } else out._.push(a);
  }
  return out;
}
const args = parseArgs(process.argv.slice(2));

const TYPE = String(args.type || "").toLowerCase();
const RECORD_OF_TYPE = {
  releases: { tag: "release", cacheType: "release" },
  masters:  { tag: "master",  cacheType: "master"  },
  artists:  { tag: "artist",  cacheType: "artist"  },
  labels:   { tag: "label",   cacheType: "label"   },
};
const cfg = RECORD_OF_TYPE[TYPE];
if (!cfg) {
  console.error("--type must be one of: releases, masters, artists, labels");
  process.exit(1);
}
const FILE = args.file;
if (!FILE || args.file === true) { console.error("--file is required"); process.exit(1); }
if (!fs.existsSync(FILE)) { console.error(`file not found: ${FILE}`); process.exit(1); }

const DRY_RUN      = !!args["dry-run"];
const LIMIT        = args.limit ? Number(args.limit) : 0;
const SINCE_YEAR   = args["since-year"] ? Number(args["since-year"]) : 0;
const GENRES       = args.genres && args.genres !== true
  ? String(args.genres).split(",").map(s => s.trim().toLowerCase()).filter(Boolean)
  : [];
const BATCH        = args.batch ? Math.max(50, Number(args.batch)) : 800;
const SKIP_EXISTING = !!args["skip-existing"];
const WARM         = !!args.warm;
const PROGRESS     = args.progress ? Number(args.progress) : 50000;

// ── transforms: dump XML tree → API-shape JSON ───────────────────────
// The SAX layer below hands each transform a generic tree node:
//   { name, attrs:{}, children:[node], text:"" }
// with helpers attached (see makeNode). Transforms return the `data`
// blob plus the numeric id used as the cache key.

const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : undefined; };
// Parse a Discogs `released` string ("1969", "1969-00-00", "1969-05-00")
// into a plain 4-digit year int, or 0 if absent/garbage.
function yearFromReleased(s) {
  if (!s) return 0;
  const m = String(s).match(/(\d{4})/);
  const y = m ? Number(m[1]) : 0;
  return (y >= 1000 && y <= 2100) ? y : 0;
}

function xf_release(node) {
  const id = num(node.attrs.id);
  const data = { id, type: "release", status: node.attrs.status || undefined };
  data.title = node.child("title");
  data.country = node.child("country");
  const released = node.child("released");
  if (released) data.released = released;
  data.year = yearFromReleased(released);
  data.notes = node.child("notes") || undefined;
  data.data_quality = node.child("data_quality") || undefined;

  data.artists = node.listUnder("artists", "artist").map(a => ({
    id: num(a.child("id")), name: a.child("name"),
    anv: a.child("anv") || undefined, join: a.child("join") || undefined,
    role: a.child("role") || undefined, tracks: a.child("tracks") || undefined,
  }));
  data.extraartists = node.listUnder("extraartists", "artist").map(a => ({
    id: num(a.child("id")), name: a.child("name"),
    anv: a.child("anv") || undefined, join: a.child("join") || undefined,
    role: a.child("role") || undefined, tracks: a.child("tracks") || undefined,
  }));
  data.labels = node.listUnder("labels", "label").map(l => ({
    id: num(l.attrs.id), name: l.attrs.name, catno: l.attrs.catno,
    entity_type_name: l.attrs.entity_type_name || undefined,
  }));
  data.companies = node.listUnder("companies", "company").map(c => ({
    id: num(c.child("id")), name: c.child("name"),
    catno: c.child("catno") || undefined,
    entity_type: c.child("entity_type") || undefined,
    entity_type_name: c.child("entity_type_name") || undefined,
  }));
  data.formats = node.listUnder("formats", "format").map(f => ({
    name: f.attrs.name, qty: f.attrs.qty, text: f.attrs.text || undefined,
    descriptions: f.listUnder("descriptions", "description").map(d => d.text).filter(Boolean),
  }));
  data.genres = node.listUnder("genres", "genre").map(g => g.text).filter(Boolean);
  data.styles = node.listUnder("styles", "style").map(s => s.text).filter(Boolean);
  data.identifiers = node.listUnder("identifiers", "identifier").map(i => ({
    type: i.attrs.type, value: i.attrs.value, description: i.attrs.description || undefined,
  }));
  data.videos = node.listUnder("videos", "video").map(v => ({
    uri: v.attrs.src, duration: num(v.attrs.duration),
    embed: v.attrs.embed === "true" || undefined,
    title: v.child("title") || undefined, description: v.child("description") || undefined,
  }));
  data.tracklist = node.listUnder("tracklist", "track").map(t => {
    const tr = {
      position: t.child("position") || "", title: t.child("title") || "",
      duration: t.child("duration") || "",
    };
    const ea = t.listUnder("extraartists", "artist");
    if (ea.length) tr.extraartists = ea.map(a => ({ id: num(a.child("id")), name: a.child("name"), role: a.child("role") || undefined }));
    const sub = t.listUnder("sub_tracks", "track");
    if (sub.length) tr.sub_tracks = sub.map(s => ({ position: s.child("position") || "", title: s.child("title") || "", duration: s.child("duration") || "" }));
    return tr;
  });

  const mid = node.first("master_id");
  if (mid) { data.master_id = num(mid.text); if (mid.attrs.is_main_release === "true") data.main_release = true; }

  const comm = node.first("community");
  if (comm) {
    const rating = comm.first("rating");
    data.community = {
      have: num(comm.child("have")), want: num(comm.child("want")),
      // <rating count="20" average="4.24"/> — attributes, not elements.
      rating: rating ? { count: num(rating.attrs.count), average: num(rating.attrs.average) } : undefined,
    };
  }
  data.images = node.listUnder("images", "image").map(im => ({
    type: im.attrs.type, uri: im.attrs.uri || "", uri150: im.attrs.uri150 || "",
    width: num(im.attrs.width), height: num(im.attrs.height),
  }));
  return { id, data };
}

function xf_master(node) {
  const id = num(node.attrs.id);
  const data = { id, type: "master" };
  data.title = node.child("title");
  data.year = num(node.child("year")) || 0;
  data.main_release = num(node.child("main_release"));
  data.data_quality = node.child("data_quality") || undefined;
  data.notes = node.child("notes") || undefined;
  data.artists = node.listUnder("artists", "artist").map(a => ({
    id: num(a.child("id")), name: a.child("name"),
    anv: a.child("anv") || undefined, join: a.child("join") || undefined,
    role: a.child("role") || undefined,
  }));
  data.genres = node.listUnder("genres", "genre").map(g => g.text).filter(Boolean);
  data.styles = node.listUnder("styles", "style").map(s => s.text).filter(Boolean);
  data.videos = node.listUnder("videos", "video").map(v => ({
    uri: v.attrs.src, duration: num(v.attrs.duration),
    embed: v.attrs.embed === "true" || undefined,
    title: v.child("title") || undefined, description: v.child("description") || undefined,
  }));
  data.images = node.listUnder("images", "image").map(im => ({
    type: im.attrs.type, uri: im.attrs.uri || "", uri150: im.attrs.uri150 || "",
    width: num(im.attrs.width), height: num(im.attrs.height),
  }));
  return { id, data };
}

function xf_artist(node) {
  const id = num(node.child("id"));
  const data = { id, name: node.child("name") };
  data.realname = node.child("realname") || undefined;
  data.profile = node.child("profile") || undefined;
  data.data_quality = node.child("data_quality") || undefined;
  data.urls = node.listUnder("urls", "url").map(u => u.text).filter(Boolean);
  data.namevariations = node.listUnder("namevariations", "name").map(n => n.text).filter(Boolean);
  data.aliases = node.listUnder("aliases", "name").map(n => ({ id: num(n.attrs.id), name: n.text }));
  data.groups = node.listUnder("groups", "name").map(n => ({ id: num(n.attrs.id), name: n.text }));
  data.members = node.listUnder("members", "name").map(n => ({ id: num(n.attrs.id), name: n.text }));
  data.images = node.listUnder("images", "image").map(im => ({
    type: im.attrs.type, uri: im.attrs.uri || "", uri150: im.attrs.uri150 || "",
    width: num(im.attrs.width), height: num(im.attrs.height),
  }));
  return { id, data };
}

function xf_label(node) {
  const id = num(node.child("id"));
  const data = { id, name: node.child("name") };
  data.profile = node.child("profile") || undefined;
  data.contact_info = node.child("contactinfo") || undefined;
  data.data_quality = node.child("data_quality") || undefined;
  data.urls = node.listUnder("urls", "url").map(u => u.text).filter(Boolean);
  data.sublabels = node.listUnder("sublabels", "label").map(l => ({ id: num(l.attrs.id), name: l.text }));
  // sax lowercases tag names, so <parentLabel> arrives as 'parentlabel'.
  const parent = node.first("parentlabel");
  if (parent) data.parent_label = { id: num(parent.attrs.id), name: parent.text };
  data.images = node.listUnder("images", "image").map(im => ({
    type: im.attrs.type, uri: im.attrs.uri || "", uri150: im.attrs.uri150 || "",
    width: num(im.attrs.width), height: num(im.attrs.height),
  }));
  return { id, data };
}

const TRANSFORM = { release: xf_release, master: xf_master, artist: xf_artist, label: xf_label };
const xf = TRANSFORM[cfg.cacheType];

// ── genre / year filter (releases + masters only) ────────────────────
function keepRecord(data) {
  if (SINCE_YEAR && cfg.cacheType !== "artist" && cfg.cacheType !== "label") {
    if (!data.year || data.year < SINCE_YEAR) return false;
  }
  if (GENRES.length) {
    const g = Array.isArray(data.genres) ? data.genres.map(x => String(x).toLowerCase()) : [];
    if (!g.some(x => GENRES.includes(x))) return false;
  }
  return true;
}

// ── generic tree node with query helpers ─────────────────────────────
function makeNode(name, attrs) {
  return {
    name, attrs: attrs || {}, children: [], text: "",
    // direct child element by name → its text (trimmed), or "" if absent
    child(n) { const c = this.children.find(k => k.name === n); return c ? c.text.trim() : ""; },
    // first direct child element by name → node, or null
    first(n) { return this.children.find(k => k.name === n) || null; },
    // items of a wrapper: node.listUnder("artists","artist") returns the
    // <artist> nodes inside the (first) <artists> wrapper, or [] if the
    // wrapper is absent. No flatten fallback — a bare match would let a
    // sibling like the artist's own <name> leak into groups/members.
    listUnder(wrapper, item) {
      const w = this.children.find(k => k.name === wrapper);
      return w ? w.children.filter(k => k.name === item) : [];
    },
  };
}

// ── pg pool ──────────────────────────────────────────────────────────
const connStr = process.env.DATABASE_URL;
if (!connStr && !DRY_RUN) { console.error("DATABASE_URL is required (unless --dry-run)"); process.exit(1); }
const pool = DRY_RUN ? null : new pg.Pool({
  connectionString: connStr,
  ssl: process.env.DB_CA_CERT ? { rejectUnauthorized: true, ca: process.env.DB_CA_CERT } : { rejectUnauthorized: false },
  max: 4,
});

async function existingIds(ids) {
  if (!SKIP_EXISTING || !pool || !ids.length) return new Set();
  const r = await pool.query(
    `SELECT discogs_id FROM release_cache WHERE type = $1 AND discogs_id = ANY($2::int[])`,
    [cfg.cacheType, ids]
  );
  return new Set(r.rows.map(x => x.discogs_id));
}

// Batched upsert. Multi-row VALUES with ON CONFLICT DO UPDATE. seen_at
// stays NULL unless --warm so a mass import doesn't flood the feed.
async function flush(batch) {
  if (!pool || !batch.length) return 0;
  let rows = batch;
  if (SKIP_EXISTING) {
    const have = await existingIds(batch.map(b => b.id));
    rows = batch.filter(b => !have.has(b.id));
    if (!rows.length) return 0;
  }
  const vals = [];
  const params = [];
  let p = 1;
  for (const b of rows) {
    vals.push(`($${p++}, $${p++}, $${p++}, NOW(), ${WARM ? "NOW()" : "NULL"})`);
    params.push(b.id, cfg.cacheType, JSON.stringify(b.data));
  }
  const seenClause = WARM
    ? "seen_at = COALESCE(release_cache.seen_at, NOW())"
    : "seen_at = release_cache.seen_at";
  await pool.query(
    `INSERT INTO release_cache (discogs_id, type, data, cached_at, seen_at)
     VALUES ${vals.join(",")}
     ON CONFLICT (discogs_id, type)
     DO UPDATE SET data = EXCLUDED.data, cached_at = NOW(), ${seenClause}`,
    params
  );
  return rows.length;
}

// ── stream + parse ───────────────────────────────────────────────────
async function main() {
  const t0 = Date.now();
  // lowercase:true — non-strict sax otherwise UPPERCASES tag + attr
  // names, which would break every element/attribute lookup below.
  const strict = false;
  const parser = sax.createStream(strict, { trim: false, position: false, lowercase: true });

  const stack = [];        // open element nodes; stack[0] is the record root once we descend
  let depth = 0;           // element depth (root dump element = 0)
  let recordNode = null;   // the current record's root node (depth === 1)

  let scanned = 0, kept = 0, written = 0;
  let batch = [];
  let firstPrinted = false;
  let stopped = false;

  // Backpressure: pause the stream while an async flush runs.
  const stream = fs.createReadStream(FILE);
  const input = /\.gz$/i.test(FILE) ? stream.pipe(zlib.createGunzip()) : stream;

  const done = new Promise((resolve, reject) => {
    parser.on("error", (e) => reject(e));
    parser.on("end", () => resolve());

    parser.on("opentag", (t) => {
      if (stopped) return;
      const node = makeNode(t.name, t.attributes);
      if (depth === 1 && t.name === cfg.tag) recordNode = node;
      if (stack.length) stack[stack.length - 1].children.push(node);
      stack.push(node);
      depth++;
    });
    parser.on("text", (s) => { if (!stopped && stack.length) stack[stack.length - 1].text += s; });
    parser.on("cdata", (s) => { if (!stopped && stack.length) stack[stack.length - 1].text += s; });

    parser.on("closetag", async (name) => {
      if (stopped) return;
      const node = stack.pop();
      depth--;
      if (depth === 1 && recordNode && node === recordNode) {
        // a full record just closed
        const rn = recordNode;
        recordNode = null;
        // drop it from the root's children so memory doesn't accumulate
        if (stack.length) stack[stack.length - 1].children.length = 0;
        scanned++;
        if (PROGRESS && scanned % PROGRESS === 0) {
          const rate = Math.round(scanned / ((Date.now() - t0) / 1000));
          console.log(`  scanned ${scanned.toLocaleString()} · kept ${kept.toLocaleString()} · written ${written.toLocaleString()} · ${rate}/s`);
        }
        let rec;
        try { rec = xf(rn); } catch (e) { return; }
        if (!rec || !rec.id) return;
        if (!keepRecord(rec.data)) return;
        kept++;

        if (DRY_RUN) {
          if (!firstPrinted) { console.log(JSON.stringify(rec.data, null, 2)); firstPrinted = true; }
          if (LIMIT && kept >= LIMIT) { stopped = true; input.destroy(); resolve(); }
          return;
        }

        batch.push(rec);
        if (batch.length >= BATCH) {
          const b = batch; batch = [];
          input.pause();
          try { written += await flush(b); }
          catch (e) { reject(e); return; }
          input.resume();
        }
        if (LIMIT && kept >= LIMIT) { stopped = true; input.destroy(); resolve(); }
      }
    });

    input.on("error", reject);
    input.pipe(parser);
  });

  await done;
  if (!DRY_RUN && batch.length) written += await flush(batch);

  const secs = ((Date.now() - t0) / 1000).toFixed(0);
  console.log(`\nDone. scanned ${scanned.toLocaleString()} · kept ${kept.toLocaleString()} · written ${written.toLocaleString()} · ${secs}s`);
  if (DRY_RUN) console.log("(dry-run — nothing written)");
  else console.log(`Next: run the admin "Project into split cache" backfill to populate masters/pressings from these ${cfg.cacheType} rows.`);
  if (pool) await pool.end();
}

main().catch(async (e) => {
  console.error("\nFATAL:", e?.message ?? e);
  try { if (pool) await pool.end(); } catch {}
  process.exit(1);
});
