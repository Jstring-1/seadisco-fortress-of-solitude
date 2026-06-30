// Walk the 78discography.com index page, extract every label-series
// page link, and pair each one with the label name from context (either
// the link text itself or the most recent green-coloured label header).
//
//   node scripts/discover-abrams-labels.mjs > scripts/abrams-labels.json
//
// The index uses this rough structure:
//   <center><b><font color="#009900"><font size="+1">LABEL_NAME (...)</font></font></b></center>
//   <center><a href="SeriesA.htm">100</a></center>
//   <center><a href="SeriesB.htm">2000</a></center>
//   ...
// or for single-series labels:
//   <center><b><font size="+1"><a href="X.htm">LABEL_NAME (USA)</a></font></b></center>
//
// We normalise the name (strip trailing "(USA)", date ranges, etc.) so
// it has a chance of matching what Discogs calls the label.

const URL = "https://www.78discography.com/";
const UA  = "SeaDisco-archival/1.0 (+https://seadisco.com, kylejester@gmail.com)";

const res = await fetch(URL, { headers: { "User-Agent": UA } });
if (!res.ok) {
  console.error(`fetch failed: ${URL} → ${res.status}`);
  process.exit(2);
}
const html = await res.text();

function normaliseLabel(s) {
  // Strip trailing parentheticals like "(USA)", "(1945 - 1958)",
  // "(Canadian Race 1920s)", or " - 78rpm". Title-case if all caps.
  let t = String(s ?? "").trim();
  t = t.replace(/\s*\([^)]*\)\s*$/, "").trim();
  t = t.replace(/&amp;/g, "&").replace(/&nbsp;/g, " ").trim();
  // Title-case all-caps
  if (t && t === t.toUpperCase() && /[A-Z]/.test(t)) {
    t = t.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
  }
  return t;
}

// Build a flat sequence of tokens — either "header" or "link" — in
// document order, so we can pair links to the nearest preceding header.
// The header regex specifically looks for the green colour (#009900)
// which is the discriminator for "this is a multi-series label header"
// — keeps us from matching the unrelated bold-bigfont blocks used for
// site headings and call-to-action text. Tolerates an inserted
// <a name="..."> anchor between <center> and <b> (some headers have one
// to support intra-page jumps).
const tokens = [];
const headerRe = /<center>[\s\S]{0,200}?<font[^>]*color="?#?009900"?[^>]*>[\s\S]{0,200}?<font[^>]*size="\+\d"[^>]*>([\s\S]+?)<\/font>[\s\S]{0,200}?<\/center>/gi;
const linkRe   = /<a\s+href="([A-Z][A-Za-z0-9]+\.html?)"[^>]*>([^<]+)<\/a>/g;

let hm, lm;
const events = [];
while ((hm = headerRe.exec(html)) !== null) {
  events.push({ pos: hm.index, kind: "header", text: hm[1] });
}
while ((lm = linkRe.exec(html)) !== null) {
  events.push({ pos: lm.index, kind: "link", href: lm[1], text: lm[2] });
}
events.sort((a, b) => a.pos - b.pos);

const seen = new Set();
const seeds = [];
let currentLabel = null;

for (const ev of events) {
  if (ev.kind === "header") {
    currentLabel = normaliseLabel(ev.text);
    continue;
  }
  // skip non-label hrefs (helpout, mailto, external sites)
  if (/^(helpout|index)\b/i.test(ev.href)) continue;
  if (seen.has(ev.href)) continue;
  seen.add(ev.href);
  const slug = ev.href.replace(/\.html?$/, "");
  let name;
  // If the link text looks like a label name (mostly letters, has caps),
  // and the current "label context" isn't already a closer match, use
  // the link text. Pattern: short alphabetical, e.g. "ABBEY (USA)".
  const linkLooksLikeLabel = /^[A-Z][A-Z0-9\-' .&]{2,}/.test(ev.text.trim());
  if (linkLooksLikeLabel) {
    name = normaliseLabel(ev.text);
  } else if (currentLabel) {
    name = currentLabel;
  } else {
    // Fallback: derive from slug — last resort, often imperfect.
    name = slug.replace(/(\d+)$/, "").replace(/([A-Z])/g, " $1").trim();
  }
  seeds.push({ slug, name });
}

process.stdout.write(JSON.stringify(seeds, null, 2));
process.stderr.write(`discovered ${seeds.length} label-series pages\n`);
