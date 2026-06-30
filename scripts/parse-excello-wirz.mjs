// Scrape Stefan Wirz's illustrated Excello LP (8000-series) discography
// at https://www.wirz.de/music/excello.htm and emit JSON in the shape
// expected by POST /api/admin/external-discography/import.
//
//   node scripts/parse-excello-wirz.mjs > excello-wirz.json
//
// Each LP is a <tr> with:
//   <td align=center><FONT SIZE="-1">CATNO<br>(YEAR)</FONT></td>
//   <td><b>ARTIST: TITLE</b>...
// Compilations omit the "Artist:" prefix.
//
// LPs are single-sided in the catalog sense — no A/B split per single.
// We emit side=null so the carousel dedupe matches the (label, catno)
// pair without confusion.

const URL = "https://www.wirz.de/music/excello.htm";

const html = await (await fetch(URL)).text();

// Match each row's catno + year + bold title cell. The HTML is loose
// (mixed case tags, varying spacing) so we keep the regex permissive.
const rowRe = /<tr>\s*<td[^>]*align=center[^>]*>\s*<FONT[^>]*>(\d{4}(?:\/\d+)?)<br>\s*\((n\.d\.|\d{4})\)<\/FONT>\s*<\/td>\s*<td[^>]*>\s*<b>([^<]+)<\/b>/gi;

const rows = [];
let m;
while ((m = rowRe.exec(html)) !== null) {
  const catnoRaw = m[1].trim();
  const yearRaw  = m[2].trim();
  const titleCell = m[3].replace(/&amp;/g, "&").replace(/&quot;/g, '"').trim();
  let artist = null;
  let title  = titleCell;
  // "Artist: Title" → split on first ": "
  const colon = titleCell.indexOf(": ");
  if (colon > 0 && colon < 60) {
    artist = titleCell.slice(0, colon).trim();
    title  = titleCell.slice(colon + 2).trim();
  } else if (/^(various|tunes to|the excello story|montreux|swamp blues|the real blues|the original american)/i.test(titleCell)) {
    artist = "Various";
  }
  const year = /^\d{4}$/.test(yearRaw) ? Number(yearRaw) : null;
  rows.push({
    catno: catnoRaw,
    side:  null,
    artist,
    title,
    year,
    matrix: null,
    xref:   null,
    loc:    null,
    composer: null,
    notes:    null,
  });
}

const out = {
  label:   "Excello",
  labelId: 51225,
  source:  "wirz.de",
  rows,
};

process.stdout.write(JSON.stringify(out, null, 2));
process.stderr.write(`parsed ${rows.length} LP entries from wirz.de\n`);
