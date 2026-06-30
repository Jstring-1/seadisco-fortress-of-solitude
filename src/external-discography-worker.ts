// ── External discography worker ──────────────────────────────────
//
// Background scraper for non-Discogs label discographies:
//   - wirz.de (~130 blues LP series)
//   - 78discography.com (~655 78-rpm label-series pages)
//
// Reads the same seed JSON files the CLI scrapers use
// (scripts/wirz-labels.json, scripts/abrams-labels.json) so the
// source-of-truth label list is single. Walks each seed, fetches the
// label page, parses the catalog rows, and upserts them into
// external_discography.
//
// Singleflight: one worker may run at a time. Persistent cursor in
// app_settings so a Railway restart resumes from the last completed
// seed. Stop / purge are admin-triggered.

import fs from "node:fs";
import path from "node:path";
import {
  bulkInsertExternalDiscography,
  getAppSetting,
  setAppSetting,
  type ExternalDiscographyRow,
} from "./db.js";

interface Seed { slug: string; name: string; labelId?: number; }
type Source = "wirz" | "abrams";

const REQ_INTERVAL_MS = 2000;     // polite scrape rate
const UA = "SeaDisco-archival/1.0 (+https://seadisco.com)";
const ACTIVE_KEY = "external_disc_worker_active";

// ── In-memory worker state ──────────────────────────────────────
let _running        = false;
let _stopRequested  = false;
let _currentSource: Source | null = null;
let _currentIndex   = 0;
let _totalSeeds     = 0;
let _startedAt: Date | null = null;
const _recentInserted: Array<{ source: Source; label: string; rows: number; at: Date }> = [];
const _recentErrors:   Array<{ source: Source; slug: string; msg: string; at: Date }>    = [];

function _sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

function _seedPath(source: Source): string {
  // process.cwd() is the project root when running `node dist/search-api.js`
  // from Railway, and the seed JSONs are committed in scripts/.
  return path.join(process.cwd(), "scripts",
    source === "wirz" ? "wirz-labels.json" : "abrams-labels.json");
}

function _loadSeeds(source: Source): Seed[] {
  return JSON.parse(fs.readFileSync(_seedPath(source), "utf8"));
}

// ── Pure parsers (ports of the CLI .mjs equivalents) ────────────

function _decodeText(s: string): string {
  return String(s ?? "")
    .replace(/&amp;/g,  "&")
    .replace(/&lt;/g,   "<")
    .replace(/&gt;/g,   ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/<[^>]+>/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function _dashToNull(v: string | null | undefined): string | null {
  if (!v || v === "-" || v === "?" || v === "n/a") return null;
  return v;
}

function _parseYearFromAbramsDate(dateStr: string): number | null {
  if (!dateStr) return null;
  const m = dateStr.match(/(\d{1,2})\/(\d{1,2})\/(\d{2}|\d{4})/);
  if (!m) {
    const y = dateStr.match(/\b(19\d{2}|20\d{2})\b/);
    return y ? Number(y[1]) : null;
  }
  const y = Number(m[3]);
  if (y < 100) return y < 30 ? 2000 + y : 1900 + y;
  return y;
}

function parseWirzLabel(html: string, seed: Seed): Partial<ExternalDiscographyRow>[] {
  // <tr><td align=center><FONT SIZE="-1">CATNO<br>(YEAR)</FONT></td>
  // <td><b>BOLD_TEXT</b>…
  const rowRe = /<tr>\s*<td[^>]*align=center[^>]*>\s*<FONT[^>]*>(\d{2,5}(?:\/\d+)?)<br>\s*\((n\.d\.|\d{4}(?:\?|)|19\?\?|197\?|198\?|\?)\)<\/FONT>\s*<\/td>\s*<td[^>]*>\s*<b>([^<]+)<\/b>/gi;
  const rows: Partial<ExternalDiscographyRow>[] = [];
  let m: RegExpExecArray | null;
  while ((m = rowRe.exec(html)) !== null) {
    const catnoRaw = m[1].trim();
    const yearRaw  = m[2].trim();
    const bold     = m[3].replace(/&amp;/g, "&").replace(/&quot;/g, '"').replace(/&nbsp;/g, " ").trim();
    let artist: string | null = null;
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
      notes: `_bold=${bold}`,
    });
  }
  return rows;
}

function parseAbramsLabel(html: string, _seed: Seed): Partial<ExternalDiscographyRow>[] {
  const trRe = /<tr>([\s\S]*?)<\/tr>/gi;
  const sideCounter = new Map<string, number>();
  const nextSide = (catno: string): string => {
    const n = (sideCounter.get(catno) ?? 0) + 1;
    sideCounter.set(catno, n);
    return String.fromCharCode(64 + n);
  };
  const rows: Partial<ExternalDiscographyRow>[] = [];
  let trMatch: RegExpExecArray | null;
  while ((trMatch = trRe.exec(html)) !== null) {
    const inner = trMatch[1];
    const tdRe = /<td[^>]*>([\s\S]*?)<\/td>/gi;
    const cells: string[] = [];
    let tdMatch: RegExpExecArray | null;
    while ((tdMatch = tdRe.exec(inner)) !== null) {
      cells.push(_decodeText(tdMatch[1]));
    }
    if (cells.length !== 8) continue;
    if (/^(label|cat#?|catalog)\b:?$/i.test(cells[0])) continue;
    const catno = cells[0];
    if (!/^\d/.test(catno)) continue;
    rows.push({
      catno,
      side:     nextSide(catno),
      artist:   _dashToNull(cells[1]),
      title:    _dashToNull(cells[2]),
      matrix:   _dashToNull(cells[3]),
      xref:     _dashToNull(cells[4]),
      loc:      _dashToNull(cells[5]),
      year:     _parseYearFromAbramsDate(cells[6]),
      composer: _dashToNull(cells[7]),
    });
  }
  return rows;
}

// ── Public API ────────────────────────────────────────────────

export function initExternalDiscographyWorkerModule(): void {
  // Boot-resume: 5s after process start, check whether a run was
  // active when we shut down and pick it up where we left off.
  setTimeout(async () => {
    if (_running) return;
    try {
      const raw = await getAppSetting(ACTIVE_KEY);
      if (!raw) return;
      const state = JSON.parse(raw);
      if (!state?.source) return;
      console.log(`[external-disc-worker] boot-resume: ${state.source} at cursor ${state.cursor ?? 0}`);
      startExternalDiscographyRun(state.source, { resumeFromIndex: state.cursor ?? 0 }).catch(err =>
        console.error("[external-disc-worker] boot-resume failed:", err));
    } catch (err) {
      console.error("[external-disc-worker] boot-resume check failed:", err);
    }
  }, 5000);
}

export function isExternalDiscographyRunning(): boolean {
  return _running;
}

export function requestExternalDiscographyStop(): void {
  if (_running) _stopRequested = true;
}

export function getExternalDiscographyStatus(): object {
  return {
    running:         _running,
    source:          _currentSource,
    cursor:          _currentIndex,
    total:           _totalSeeds,
    startedAt:       _startedAt,
    recentInserted:  _recentInserted.slice(0, 12),
    recentErrors:    _recentErrors.slice(0, 12),
  };
}

export async function startExternalDiscographyRun(
  source: Source,
  opts: { resumeFromIndex?: number } = {},
): Promise<{ ok: boolean; error?: string }> {
  if (_running) return { ok: false, error: `Already running: ${_currentSource}` };
  if (source !== "wirz" && source !== "abrams") {
    return { ok: false, error: `Unknown source: ${source}` };
  }
  let seeds: Seed[];
  try {
    seeds = _loadSeeds(source);
  } catch (err: any) {
    return { ok: false, error: `Failed to load seeds: ${err?.message ?? String(err)}` };
  }

  _running        = true;
  _stopRequested  = false;
  _currentSource  = source;
  _currentIndex   = opts.resumeFromIndex ?? 0;
  _totalSeeds     = seeds.length;
  _startedAt      = new Date();
  _recentInserted.length = 0;
  _recentErrors.length   = 0;

  await setAppSetting(ACTIVE_KEY, JSON.stringify({ source, cursor: _currentIndex }));

  (async () => {
    try {
      for (let i = _currentIndex; i < seeds.length; i++) {
        if (_stopRequested) {
          console.log(`[external-disc-worker] stop requested at i=${i}`);
          break;
        }
        _currentIndex = i;
        const seed = seeds[i];
        try {
          await _sleep(REQ_INTERVAL_MS);
          const url = source === "wirz"
            ? `https://www.wirz.de/music/${seed.slug}.htm`
            : `https://www.78discography.com/${seed.slug}.htm`;
          const res = await fetch(url, { headers: { "User-Agent": UA } });
          if (!res.ok) {
            _recordError(source, seed.slug, `HTTP ${res.status}`);
            continue;
          }
          const html = await res.text();
          const parsed = source === "wirz"
            ? parseWirzLabel(html, seed)
            : parseAbramsLabel(html, seed);
          if (parsed.length === 0) continue;
          const payload: ExternalDiscographyRow[] = parsed.map(r => ({
            ...r,
            label_name: seed.name,
            label_id:   seed.labelId ?? null,
            source:     source === "wirz" ? "wirz.de" : "78discography.com",
            catno:      r.catno!,
          }));
          const result = await bulkInsertExternalDiscography(payload);
          _recordInserted(source, seed.name, result.inserted);
        } catch (err: any) {
          _recordError(source, seed.slug, err?.message ?? String(err));
        }
        await setAppSetting(ACTIVE_KEY, JSON.stringify({ source, cursor: i + 1 }));
      }
      console.log(`[external-disc-worker] ${source} run finished at i=${_currentIndex}`);
    } catch (err: any) {
      console.error("[external-disc-worker] worker crashed:", err?.stack || err);
    } finally {
      _running        = false;
      _stopRequested  = false;
      _currentSource  = null;
      _currentIndex   = 0;
      _totalSeeds     = 0;
      _startedAt      = null;
      try { await setAppSetting(ACTIVE_KEY, null); } catch {}
    }
  })().catch(err => console.error("[external-disc-worker] outer IIFE rejected:", err));

  return { ok: true };
}

function _recordInserted(source: Source, label: string, rows: number): void {
  _recentInserted.unshift({ source, label, rows, at: new Date() });
  if (_recentInserted.length > 30) _recentInserted.length = 30;
}
function _recordError(source: Source, slug: string, msg: string): void {
  _recentErrors.unshift({ source, slug, msg: msg.slice(0, 200), at: new Date() });
  if (_recentErrors.length > 30) _recentErrors.length = 30;
  console.warn(`[external-disc-worker] ${source} ${slug}: ${msg}`);
}

// ── Server-side xlsx parser ──────────────────────────────────────
// Reads a Praguefrank-style Excello xlsx (or anything with the same
// 8-column schema) and emits ExternalDiscographyRow[] ready for
// bulkInsertExternalDiscography. Used by the admin upload endpoint.
//
// Mirrors scripts/parse-excello-xlsx.mjs so the same input produces
// the same rows.

import AdmZip from "adm-zip";

export function parseExcelloXlsxBuffer(buf: Buffer): Partial<ExternalDiscographyRow>[] {
  const zip = new AdmZip(buf);
  const ssXml = zip.getEntry("xl/sharedStrings.xml")?.getData().toString("utf8") ?? "";
  const shXml = zip.getEntry("xl/worksheets/sheet1.xml")?.getData().toString("utf8") ?? "";
  if (!shXml) throw new Error("xl/worksheets/sheet1.xml missing");

  const strings: string[] = [];
  const ssRe = /<t[^>]*>([\s\S]*?)<\/t>/g;
  let m: RegExpExecArray | null;
  while ((m = ssRe.exec(ssXml)) !== null) {
    strings.push(m[1]
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g,  "<")
      .replace(/&gt;/g,  ">")
      .replace(/&quot;/g, '"')
      .replace(/&apos;/g, "'"));
  }

  const rowRe = /<row[^>]*>([\s\S]*?)<\/row>/g;
  const out: Partial<ExternalDiscographyRow>[] = [];
  const sideCounter = new Map<string, number>();
  const nextSide = (cn: string): string => {
    const n = (sideCounter.get(cn) ?? 0) + 1;
    sideCounter.set(cn, n);
    return String.fromCharCode(64 + n);
  };

  let rm: RegExpExecArray | null;
  while ((rm = rowRe.exec(shXml)) !== null) {
    const rowXml = rm[1];
    const cells: Record<string, string | undefined> = {};
    const cellRe = /<c\b([^>]*)>([\s\S]*?)<\/c>/g;
    let cm: RegExpExecArray | null;
    while ((cm = cellRe.exec(rowXml)) !== null) {
      const attrs = cm[1];
      const inner = cm[2];
      const rMatch = attrs.match(/\br="([A-Z]+)\d+"/);
      if (!rMatch) continue;
      const col = rMatch[1];
      const tMatch = attrs.match(/\bt="([^"]+)"/);
      const type = tMatch ? tMatch[1] : "n";
      const vMatch       = inner.match(/<v>([\s\S]*?)<\/v>/);
      const inlineVMatch = inner.match(/<is>[\s\S]*?<t[^>]*>([\s\S]*?)<\/t>[\s\S]*?<\/is>/);
      let val: string | undefined;
      if (inlineVMatch)                       val = inlineVMatch[1];
      else if (type === "s" && vMatch)        val = strings[Number(vMatch[1])];
      else if (vMatch)                        val = vMatch[1];
      cells[col] = val;
    }
    const a = cells.A;
    if (!a || !/^\d+/.test(String(a))) continue;     // skip header & blanks
    const catno = String(a).trim();
    out.push({
      catno,
      side:     nextSide(catno),
      artist:   cells.B ? String(cells.B).trim() : null,
      title:    cells.C ? String(cells.C).trim() : null,
      matrix:   cells.D && cells.D !== "-" ? String(cells.D).trim() : null,
      xref:     cells.E && cells.E !== "-" ? String(cells.E).trim() : null,
      loc:      cells.F ? String(cells.F).trim() : null,
      year:     cells.G && /^\d{4}$/.test(String(cells.G)) ? Number(cells.G) : null,
      composer: cells.H && cells.H !== "-" ? String(cells.H).trim() : null,
    });
  }
  return out;
}
