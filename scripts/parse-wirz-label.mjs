// Scrape a single label page on Stefan Wirz's blues discography site
// (www.wirz.de/music/<slug>.htm) and emit JSON in the shape expected
// by POST /api/admin/external-discography/import.
//
//   node scripts/parse-wirz-label.mjs <slug> <label-name> [discogs-label-id]
//
// Wirz pages share a common HTML row format:
//   <tr><td align=center><FONT SIZE="-1">CATNO<br>(YEAR)</FONT></td>
//   <td><b>BOLD_TEXT</b>…
// The bold text format varies by label:
//   - LP series usually:  "Artist: Title" or just "Title" (compilations)
//   - Singles series:     just artist (with track titles in italics later)
// We capture whatever's in bold as `_bold` and attempt a permissive
// artist/title split — accepting that per-label cleanup will tidy it
// up later (the user explicitly punted on label-matching correctness).
//
// LPs have no A/B "side" in the catalog sense — emit side=null.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const slug    = process.argv[2];
const labelName = process.argv[3];
const labelId   = process.argv[4] ? Number(process.argv[4]) : null;
if (!slug || !labelName) {
  console.error("usage: node parse-wirz-label.mjs <slug> <label-name> [discogs-label-id]");
  process.exit(1);
}

const URL = `https://www.wirz.de/music/${slug}.htm`;
const UA  = "SeaDisco-archival/1.0 (+https://seadisco.com, kylejester@gmail.com)";

const res = await fetch(URL, { headers: { "User-Agent": UA } });
if (!res.ok) {
  console.error(`fetch failed: ${URL} → ${res.status}`);
  process.exit(2);
}
const html = await res.text();

// Match each row's catno + year + bold cell. Slash-catnos (e.g. "8015/16"
// for double-LPs) are preserved verbatim — sort logic falls back on the
// leading digits.
const rowRe = /<tr>\s*<td[^>]*align=center[^>]*>\s*<FONT[^>]*>(\d{2,5}(?:\/\d+)?)<br>\s*\((n\.d\.|\d{4}(?:\?|)|19\?\?|197\?|198\?|\?)\)<\/FONT>\s*<\/td>\s*<td[^>]*>\s*<b>([^<]+)<\/b>/gi;

const rows = [];
let m;
while ((m = rowRe.exec(html)) !== null) {
  const catnoRaw = m[1].trim();
  const yearRaw  = m[2].trim();
  const bold     = m[3]
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&nbsp;/g, " ")
    .trim();

  // Permissive split: prefer "Artist: Title" form; falls back to bold
  // text as title with artist=null when no colon.
  let artist = null;
  let title  = bold;
  const colon = bold.indexOf(": ");
  if (colon > 0 && colon < 60) {
    artist = bold.slice(0, colon).trim();
    title  = bold.slice(colon + 2).trim();
  } else if (/^(various|the excello story|montreux|swamp blues|the real blues|the original american)/i.test(bold)) {
    artist = "Various";
  }
  const year = /^\d{4}$/.test(yearRaw) ? Number(yearRaw) : null;
  rows.push({
    catno: catnoRaw,
    side:  null,
    artist,
    title,
    year,
    matrix:   null,
    xref:     null,
    loc:      null,
    composer: null,
    notes:    `_bold=${bold}`,
  });
}

const out = {
  label:   labelName,
  labelId: Number.isFinite(labelId) ? labelId : null,
  source:  "wirz.de",
  rows,
};

process.stdout.write(JSON.stringify(out, null, 2));
process.stderr.write(`[wirz:${slug}] parsed ${rows.length} entries\n`);
