// Parse the Praguefrank-style Excello 2000-series singles xlsx into
// the shape expected by POST /api/admin/external-discography/import.
//
//   node scripts/parse-excello-xlsx.mjs <path-to-xlsx> > excello.json
//
// Rows alternate A-side / B-side per catalog number — the source file
// has no side column, so we infer it from the first/second occurrence
// of each catno.
//
// Columns (header row 1 — labels match the source file):
//   A: Cat#   B: Artist   C: Title   D: MX   E: Xref
//   F: Loc    G: Date     H: Comp
//
// Output JSON shape:
//   { label: "Excello", labelId: 51225, source: "excello-xlsx-praguefrank",
//     rows: [{ catno, side, artist, title, year, matrix, xref, loc, composer }, …] }

import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";

const XLSX_PATH = process.argv[2];
if (!XLSX_PATH) {
  console.error("usage: node parse-excello-xlsx.mjs <path-to-xlsx>");
  process.exit(1);
}
if (!fs.existsSync(XLSX_PATH)) {
  console.error(`file not found: ${XLSX_PATH}`);
  process.exit(1);
}

// Use 7zip / unzip / Node's built-in is harder — shell out to `unzip`
// (present on Git Bash on Windows + every dev box).
const TMPDIR = fs.mkdtempSync(path.join(process.env.TEMP || "/tmp", "excelloxlsx-"));
try {
  execSync(`unzip -o "${XLSX_PATH}" -d "${TMPDIR}"`, { stdio: "ignore" });
} catch (err) {
  console.error("unzip failed — is unzip in PATH?", err.message);
  process.exit(1);
}

const ss = fs.readFileSync(path.join(TMPDIR, "xl/sharedStrings.xml"), "utf8");
const strings = [...ss.matchAll(/<t[^>]*>([\s\S]*?)<\/t>/g)].map(m =>
  m[1]
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'"),
);

const sh = fs.readFileSync(path.join(TMPDIR, "xl/worksheets/sheet1.xml"), "utf8");
const rowMatches = [...sh.matchAll(/<row[^>]*>([\s\S]*?)<\/row>/g)].map(m => m[1]);

// Two-phase parse: pull each <c ...>...</c> cell, then parse its
// attributes independently. Avoids brittle attribute-order regexes —
// in the wild, `t=` often appears AFTER `r=` or `s=` and the previous
// regex silently missed those (treating them as raw numerics).
function parseRow(rowXml) {
  const out = {};
  for (const cellMatch of rowXml.matchAll(/<c\b([^>]*)>([\s\S]*?)<\/c>/g)) {
    const attrs = cellMatch[1];
    const inner = cellMatch[2];
    const rMatch = attrs.match(/\br="([A-Z]+)\d+"/);
    if (!rMatch) continue;
    const col  = rMatch[1];
    const tMatch = attrs.match(/\bt="([^"]+)"/);
    const type = tMatch ? tMatch[1] : "n";
    const vMatch       = inner.match(/<v>([\s\S]*?)<\/v>/);
    const inlineVMatch = inner.match(/<is>[\s\S]*?<t[^>]*>([\s\S]*?)<\/t>[\s\S]*?<\/is>/);
    let val;
    if (inlineVMatch)                       val = inlineVMatch[1];
    else if (type === "s" && vMatch)        val = strings[Number(vMatch[1])];
    else if (vMatch)                        val = vMatch[1];
    else                                    val = undefined;
    out[col] = val;
  }
  return out;
}

const allRows = rowMatches.map(parseRow);
// Drop header row (col A = "Cat#" literal string)
const dataRows = allRows.filter(r => r.A && /^\d+/.test(String(r.A)));

const sideCounter = new Map(); // catno → next side letter
function nextSide(catno) {
  const n = (sideCounter.get(catno) ?? 0) + 1;
  sideCounter.set(catno, n);
  // A, B, C… most 7-inch singles cap at 2 (A + B). Multi-side EPs rare.
  return String.fromCharCode(64 + n);
}

const rows = dataRows.map(r => ({
  catno:    String(r.A || "").trim(),
  side:     nextSide(String(r.A || "").trim()),
  artist:   r.B ? String(r.B).trim() : null,
  title:    r.C ? String(r.C).trim() : null,
  matrix:   r.D && r.D !== "-" ? String(r.D).trim() : null,
  xref:     r.E && r.E !== "-" ? String(r.E).trim() : null,
  loc:      r.F ? String(r.F).trim() : null,
  year:     r.G && /^\d{4}$/.test(String(r.G)) ? Number(r.G) : null,
  composer: r.H && r.H !== "-" ? String(r.H).trim() : null,
}));

const out = {
  label:   "Excello",
  labelId: 51225,
  source:  "excello-xlsx-praguefrank",
  rows,
};

process.stdout.write(JSON.stringify(out, null, 2));
process.stderr.write(`parsed ${rows.length} rows across ${sideCounter.size} catnos\n`);

// Best-effort cleanup
try { fs.rmSync(TMPDIR, { recursive: true, force: true }); } catch {}
