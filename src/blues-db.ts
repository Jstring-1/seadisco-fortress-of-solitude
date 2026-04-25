// ── Pre-1930 blues artists DB seeder + enricher ───────────────────────
//
// Phase 1 (seed): pull the seed list from Wikidata via SPARQL and upsert
// into the blues_artists table. Each artist is keyed by its Q-number so
// re-running the seed is idempotent and only fills empty fields without
// clobbering any manual admin edits stored alongside.
//
// Phase 2 (enrich/MB): for every row that has a musicbrainz_mbid, query
// MusicBrainz for that artist's release list and write back the earliest
// + latest release year + title. Also merges in any new aliases MB knows
// that Wikidata didn't.
//
// Wikidata SPARQL endpoint:   https://query.wikidata.org/sparql
// MusicBrainz endpoint:       https://musicbrainz.org/ws/2/  (JSON, 1 req/s)
// Required header on both:    User-Agent (per Wikimedia/MetaBrainz policy)

import { upsertBluesArtistByQid, upsertBluesArtistByDiscogsId, listBluesArtists, updateBluesArtist } from "./db.js";
import { DiscogsClient } from "./discogs-client.js";

const WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql";
const WIKIDATA_UA = "SeaDisco/1.0 (+https://seadisco.com; vinyl discovery app)";

// Genre Q-numbers the seeder treats as "blues" for matching.
// Q9759 = blues, plus the major sub-genres common to pre-1930 artists.
// Query stays fast even with the union because of GENRE prop indexing.
const BLUES_GENRE_QIDS = [
  "Q9759",      // blues
  "Q1191988",   // country blues
  "Q1097732",   // delta blues
  "Q5128515",   // classic female blues
  "Q1130560",   // piedmont blues
  "Q828224",    // electric blues  (rare pre-1930 but cheap to include)
  "Q1058355",   // ragtime          (overlaps the era)
];

// Cut-off year. The criterion is "released or recorded music before this".
const BEFORE_YEAR = 1930;

function _buildSeedSparql(): string {
  const valuesClause = BLUES_GENRE_QIDS.map(q => `wd:${q}`).join(" ");
  // The criterion is recordings/releases pre-1930. Wikidata coverage of
  // 1920s recordings is uneven, so we union three signals that all imply
  // the artist was active in the era:
  //   1. Has a musical work (P175 → ?artist) with publication date P577
  //      before BEFORE_YEAR — strongest, "they actually released a record".
  //   2. Has a documented work-period start (P2031) before BEFORE_YEAR —
  //      "career began before 1930".
  //   3. Born on/before (BEFORE_YEAR - 18) AND in a blues genre — proxy
  //      that catches artists whose Wikidata entry has a birth date but
  //      no recording metadata; the admin can delete false positives.
  // GROUP_CONCAT keeps multi-valued fields (aliases, instruments,
  // hometowns, labels) on a single row per artist. The SERVICE block
  // resolves Q-IDs to English labels for any wd:* item we reference.
  const birthCutoff = BEFORE_YEAR - 18; // 1912 — youngest plausible recorder
  return `
SELECT DISTINCT
  ?artist ?artistLabel
  (GROUP_CONCAT(DISTINCT ?aliasLbl;        separator="|") AS ?aliasesStr)
  ?birth ?birthPlaceLabel ?birthAdminLabel ?birthCountryLabel
  ?death ?deathPlaceLabel ?deathAdminLabel ?deathCountryLabel ?causeLabel
  (GROUP_CONCAT(DISTINCT ?genreLabel;      separator="|") AS ?genresStr)
  (GROUP_CONCAT(DISTINCT ?instrumentLabel; separator="|") AS ?instrumentsStr)
  (GROUP_CONCAT(DISTINCT ?labelLabel;      separator="|") AS ?labelsStr)
  (GROUP_CONCAT(DISTINCT ?hometownLabel;   separator="|") AS ?hometownsStr)
  (GROUP_CONCAT(DISTINCT ?hometownAdminLabel;   separator="|") AS ?hometownAdminStr)
  (GROUP_CONCAT(DISTINCT ?hometownCountryLabel; separator="|") AS ?hometownCountryStr)
  ?image ?wikipediaArticle ?mbid ?discogsId
WHERE {
  VALUES ?genre { ${valuesClause} }
  ?artist wdt:P136 ?genre .
  {
    # Path 1: a recording credited to this artist, published before 1930.
    ?work wdt:P175 ?artist ;
          wdt:P577 ?pubDate .
    FILTER(YEAR(?pubDate) < ${BEFORE_YEAR})
  } UNION {
    # Path 2: career start (work period) before 1930.
    ?artist wdt:P2031 ?wsDate .
    FILTER(YEAR(?wsDate) < ${BEFORE_YEAR})
  } UNION {
    # Path 3: birth ≤ 1912 (= 1930 minus 18). Proxy for "could plausibly
    # have recorded before 1930" when Wikidata lacks recording metadata.
    ?artist wdt:P569 ?bornDate .
    FILTER(YEAR(?bornDate) <= ${birthCutoff})
  }
  OPTIONAL { ?artist wdt:P569 ?birth .       }
  OPTIONAL { ?artist wdt:P570 ?death .       }
  OPTIONAL {
    ?artist wdt:P19  ?birthPlace .
    # Walk up the admin chain to find a U.S. state (Q35657) ancestor.
    # For US cities this lands at the state; for non-US, no match.
    OPTIONAL {
      ?birthPlace wdt:P131* ?birthAdmin .
      ?birthAdmin wdt:P31 wd:Q35657 .
    }
    # Fallback: country, used only when there's no state-tagged ancestor.
    OPTIONAL { ?birthPlace wdt:P17 ?birthCountry . }
  }
  OPTIONAL {
    ?artist wdt:P20  ?deathPlace .
    OPTIONAL {
      ?deathPlace wdt:P131* ?deathAdmin .
      ?deathAdmin wdt:P31 wd:Q35657 .
    }
    OPTIONAL { ?deathPlace wdt:P17 ?deathCountry . }
  }
  OPTIONAL { ?artist wdt:P509 ?cause .       }
  OPTIONAL { ?artist wdt:P136 ?genreFilled . }
  OPTIONAL { ?artist wdt:P1303 ?instrument . }
  OPTIONAL { ?artist wdt:P264  ?label .      }
  OPTIONAL {
    ?artist wdt:P551  ?hometown .
    OPTIONAL {
      ?hometown wdt:P131* ?hometownAdmin .
      ?hometownAdmin wdt:P31 wd:Q35657 .
    }
    OPTIONAL { ?hometown wdt:P17 ?hometownCountry . }
  }
  OPTIONAL { ?artist wdt:P18   ?image .      }
  OPTIONAL { ?artist wdt:P434  ?mbid .       }
  OPTIONAL { ?artist wdt:P1953 ?discogsId .  }
  OPTIONAL { ?artist skos:altLabel ?aliasLbl FILTER(LANG(?aliasLbl)="en") }
  OPTIONAL {
    ?wikipediaArticle schema:about ?artist ;
                      schema:isPartOf <https://en.wikipedia.org/> .
  }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en" .
    ?artist             rdfs:label    ?artistLabel .
    ?birthPlace         rdfs:label    ?birthPlaceLabel .
    ?birthAdmin         rdfs:label    ?birthAdminLabel .
    ?birthCountry       rdfs:label    ?birthCountryLabel .
    ?deathPlace         rdfs:label    ?deathPlaceLabel .
    ?deathAdmin         rdfs:label    ?deathAdminLabel .
    ?deathCountry       rdfs:label    ?deathCountryLabel .
    ?cause              rdfs:label    ?causeLabel .
    ?genreFilled        rdfs:label    ?genreLabel .
    ?instrument         rdfs:label    ?instrumentLabel .
    ?label              rdfs:label    ?labelLabel .
    ?hometown           rdfs:label    ?hometownLabel .
    ?hometownAdmin      rdfs:label    ?hometownAdminLabel .
    ?hometownCountry    rdfs:label    ?hometownCountryLabel .
  }
}
GROUP BY ?artist ?artistLabel
         ?birth ?birthPlaceLabel ?birthAdminLabel ?birthCountryLabel
         ?death ?deathPlaceLabel ?deathAdminLabel ?deathCountryLabel
         ?causeLabel
         ?image ?wikipediaArticle ?mbid ?discogsId
ORDER BY ?artistLabel
`.trim();
}

interface SparqlBinding {
  artist:               { value: string };
  artistLabel?:         { value: string };
  aliasesStr?:          { value: string };
  birth?:               { value: string };
  birthPlaceLabel?:     { value: string };
  birthAdminLabel?:     { value: string };
  birthCountryLabel?:   { value: string };
  death?:               { value: string };
  deathPlaceLabel?:     { value: string };
  deathAdminLabel?:     { value: string };
  deathCountryLabel?:   { value: string };
  causeLabel?:          { value: string };
  genresStr?:           { value: string };
  instrumentsStr?:      { value: string };
  labelsStr?:           { value: string };
  hometownsStr?:        { value: string };
  hometownAdminStr?:    { value: string };
  hometownCountryStr?:  { value: string };
  image?:               { value: string };
  wikipediaArticle?:    { value: string };
  mbid?:                { value: string };
  discogsId?:           { value: string };
}

/** Format "City, Region". Prefers a U.S. state when one was found via
 *  the P131* walk; falls back to country otherwise. Avoids redundancy
 *  when the city label already contains the region. */
function _joinPlace(city?: string | null, state?: string | null, country?: string | null): string | null {
  const c = (city ?? "").trim();
  const s = (state ?? "").trim();
  const co = (country ?? "").trim();
  if (!c && !s && !co) return null;
  // State takes precedence — that's what the user asked for.
  const region = s || co;
  if (!region) return c || null;
  if (!c) return region;
  if (c.toLowerCase() === region.toLowerCase()) return c;
  if (c.toLowerCase().includes(region.toLowerCase())) return c;
  return `${c}, ${region}`;
}

function _qidFromUri(uri: string): string {
  const i = uri.lastIndexOf("/");
  return i >= 0 ? uri.slice(i + 1) : uri;
}

function _wikiSuffixFromUrl(url?: string): string | null {
  if (!url) return null;
  // e.g. https://en.wikipedia.org/wiki/Charley_Patton → /wiki/Charley_Patton
  try {
    const u = new URL(url);
    return u.pathname.startsWith("/wiki/") ? u.pathname : null;
  } catch { return null; }
}

function _splitPipe(s?: string): string[] {
  if (!s) return [];
  return Array.from(new Set(
    s.split("|").map(x => x.trim()).filter(Boolean)
  ));
}

function _firstPart(s?: string): string | null {
  const arr = _splitPipe(s);
  return arr.length ? arr[0] : null;
}

function _yearOnly(iso?: string): string | null {
  if (!iso) return null;
  // Wikidata returns 1899-04-29T00:00:00Z (or shorter for some entries).
  return iso.slice(0, 10);
}

export interface SeedResult {
  fetched: number;
  upserted: number;
  created?: number;          // brand-new rows
  mergedByQid?: number;      // matched existing row via wikidata_qid
  mergedByDiscogs?: number;  // matched existing Discogs-seeded row via discogs_id
  errors: Array<{ qid: string; message: string }>;
  durationMs: number;
}

/**
 * Run the Wikidata seed. Idempotent — re-running upserts on QID and
 * preserves any manually-edited fields the admin has filled in.
 */
export async function seedBluesArtistsFromWikidata(): Promise<SeedResult> {
  const start = Date.now();
  const sparql = _buildSeedSparql();
  const url = `${WIKIDATA_ENDPOINT}?query=${encodeURIComponent(sparql)}&format=json`;
  const r = await fetch(url, {
    headers: {
      "User-Agent": WIKIDATA_UA,
      "Accept": "application/sparql-results+json",
    },
  });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`Wikidata SPARQL HTTP ${r.status}: ${body.slice(0, 300)}`);
  }
  const data = await r.json() as { results?: { bindings?: SparqlBinding[] } };
  const bindings = data?.results?.bindings ?? [];

  const errors: SeedResult["errors"] = [];
  let upserted = 0, created = 0, mergedByQid = 0, mergedByDiscogs = 0;

  for (const b of bindings) {
    const qid = _qidFromUri(b.artist.value);
    const name = b.artistLabel?.value?.trim();
    if (!name) continue;
    const hometownCity    = _firstPart(b.hometownsStr?.value);
    const hometownAdmin   = _firstPart(b.hometownAdminStr?.value);
    const hometownCountry = _firstPart(b.hometownCountryStr?.value);
    const record = {
      wikidata_qid: qid,
      musicbrainz_mbid: b.mbid?.value || null,
      discogs_id: b.discogsId?.value ? parseInt(b.discogsId.value, 10) : null,
      name,
      aliases: _splitPipe(b.aliasesStr?.value),
      birth_date:  _yearOnly(b.birth?.value),
      birth_place: _joinPlace(b.birthPlaceLabel?.value, b.birthAdminLabel?.value, b.birthCountryLabel?.value),
      death_date:  _yearOnly(b.death?.value),
      death_place: _joinPlace(b.deathPlaceLabel?.value, b.deathAdminLabel?.value, b.deathCountryLabel?.value),
      death_cause: b.causeLabel?.value || null,
      hometown_region: _joinPlace(hometownCity, hometownAdmin, hometownCountry),
      styles:      _splitPipe(b.genresStr?.value),
      instruments: _splitPipe(b.instrumentsStr?.value),
      associated_labels: _splitPipe(b.labelsStr?.value),
      photo_url: b.image?.value || null,
      wikipedia_suffix: _wikiSuffixFromUrl(b.wikipediaArticle?.value),
      enrichment_status: { wikidata: 1 },
    };
    try {
      const out = await upsertBluesArtistByQid(record);
      upserted++;
      if (out.createdNew) created++;
      else if (out.matchedBy === "qid") mergedByQid++;
      else if (out.matchedBy === "discogs_id") mergedByDiscogs++;
    } catch (err: any) {
      errors.push({ qid, message: err?.message ?? String(err) });
    }
  }

  return {
    fetched: bindings.length,
    upserted,
    created,
    mergedByQid,
    mergedByDiscogs,
    errors,
    durationMs: Date.now() - start,
  } as SeedResult & { created: number; mergedByQid: number; mergedByDiscogs: number };
}

// ─────────────────────────────────────────────────────────────────────────
// Phase 2: MusicBrainz enrichment
// ─────────────────────────────────────────────────────────────────────────

const MB_ENDPOINT = "https://musicbrainz.org/ws/2";
const MB_UA = "SeaDisco/1.0 ( https://seadisco.com )";
const MB_RATE_LIMIT_MS = 1100;   // 1 req/s strict + safety margin
const MB_RELEASE_PAGE = 100;     // MB max per page
const MB_RELEASE_PAGE_CAP = 5;   // hard cap on pages per artist (500 releases)

interface MbRelease {
  id: string;
  title: string;
  date?: string;
  status?: string;
}

interface MbArtist {
  id: string;
  name: string;
  aliases?: Array<{ name: string; "sort-name"?: string }>;
  tags?: Array<{ name: string; count?: number }>;
}

function _yearFromMbDate(d?: string): number | null {
  if (!d) return null;
  const m = d.match(/^(\d{4})/);
  if (!m) return null;
  const y = parseInt(m[1], 10);
  return Number.isFinite(y) && y >= 1800 && y <= 2100 ? y : null;
}

async function _mbFetch(path: string): Promise<any> {
  const url = `${MB_ENDPOINT}${path}${path.includes("?") ? "&" : "?"}fmt=json`;
  const r = await fetch(url, {
    headers: { "User-Agent": MB_UA, "Accept": "application/json" },
  });
  if (r.status === 503) {
    // MB rate limit / maintenance — wait then retry once.
    await new Promise(res => setTimeout(res, 2500));
    const r2 = await fetch(url, {
      headers: { "User-Agent": MB_UA, "Accept": "application/json" },
    });
    if (!r2.ok) throw new Error(`MB ${r2.status}: ${path}`);
    return r2.json();
  }
  if (!r.ok) throw new Error(`MB ${r.status}: ${path}`);
  return r.json();
}

/**
 * Fetch all releases for an MBID, paging up to MB_RELEASE_PAGE_CAP pages.
 * Skips bootleg / pseudo-release / promotion statuses for first/last
 * picks but keeps them in the count.
 */
async function _fetchAllMbReleases(mbid: string): Promise<MbRelease[]> {
  const all: MbRelease[] = [];
  for (let page = 0; page < MB_RELEASE_PAGE_CAP; page++) {
    const offset = page * MB_RELEASE_PAGE;
    const data = await _mbFetch(`/release?artist=${encodeURIComponent(mbid)}&limit=${MB_RELEASE_PAGE}&offset=${offset}`);
    const releases = (data?.releases ?? []) as MbRelease[];
    all.push(...releases);
    if (releases.length < MB_RELEASE_PAGE) break;
    // Pace ourselves between pages to stay under MB's 1 req/s limit.
    await new Promise(res => setTimeout(res, MB_RATE_LIMIT_MS));
  }
  return all;
}

async function _fetchMbArtistMeta(mbid: string): Promise<MbArtist | null> {
  try {
    const data = await _mbFetch(`/artist/${encodeURIComponent(mbid)}?inc=aliases+tags`);
    return data ?? null;
  } catch { return null; }
}

/** Resolve an artist NAME to a MusicBrainz MBID via the search API.
 *  Returns the top hit if its name closely matches the input — otherwise
 *  null, so we don't pin the wrong "John Lewis" to our row. Used by the
 *  MB enricher when a row lacks an MBID (Discogs-only seeds). */
async function _resolveMbidByName(name: string): Promise<string | null> {
  const trimmed = String(name || "").trim();
  if (!trimmed) return null;
  try {
    const data = await _mbFetch(`/artist?query=${encodeURIComponent(trimmed)}&limit=3`);
    const candidates = (data?.artists ?? []) as Array<{ id: string; name: string; score?: number }>;
    if (!candidates.length) return null;
    // Take the top hit only if MB's confidence score is high (>= 90)
    // AND the name matches case-insensitively after trimming. Stops
    // "John Lewis" the bluesman from getting linked to the jazz pianist.
    const top = candidates[0];
    const sameName = top.name.trim().toLowerCase() === trimmed.toLowerCase();
    const highScore = (top.score ?? 0) >= 90;
    return (sameName && highScore) ? top.id : null;
  } catch { return null; }
}

interface EnrichedRow {
  first_recording_year:  number | null;
  first_recording_title: string | null;
  last_recording_year:   number | null;
  last_recording_title:  string | null;
  aliasesAdded: string[];
  releaseCount: number;
}

/** Enrich a single artist by MBID. Pure: returns the patch, doesn't write.
 *  deathYear (when known) clamps the "last" pick so a posthumous reissue
 *  in 2025 doesn't masquerade as the artist's last recording. */
async function _enrichOneFromMb(mbid: string, existingAliases: string[] = [], deathYear: number | null = null): Promise<EnrichedRow> {
  // Releases are the strongest signal for first/last recording year.
  const releases = await _fetchAllMbReleases(mbid);
  // Filter out bootleg / pseudo-release / promotion when picking first/last,
  // but only if there's at least one official release. Otherwise fall back
  // to whatever's available so we still get something.
  const PRIMARY_STATUS = new Set(["Official", undefined, ""]);
  const officialOnly = releases.filter(r => PRIMARY_STATUS.has(r.status as any));
  const pool = officialOnly.length ? officialOnly : releases;
  const dated = pool
    .map(r => ({ r, year: _yearFromMbDate(r.date) }))
    .filter(x => x.year != null) as Array<{ r: MbRelease; year: number }>;
  let firstYear: number | null = null;
  let firstTitle: string | null = null;
  let lastYear: number | null = null;
  let lastTitle: string | null = null;
  if (dated.length) {
    const asc = [...dated].sort((a, b) => a.year - b.year);
    firstYear = asc[0].year;
    firstTitle = asc[0].r.title || null;
    // Clamp "last" to the artist's death year + 1-year grace (a release
    // can hit the market shortly after death without being posthumous).
    // If we have no death year, take the global max as before.
    const ceiling = deathYear ? deathYear + 1 : Infinity;
    const inLifetime = asc.filter(x => x.year <= ceiling);
    const pickFromLast = inLifetime.length ? inLifetime : asc;
    const last = pickFromLast[pickFromLast.length - 1];
    lastYear = last.year;
    lastTitle = last.r.title || null;
  }
  // Aliases — pace before the second call.
  await new Promise(res => setTimeout(res, MB_RATE_LIMIT_MS));
  const meta = await _fetchMbArtistMeta(mbid);
  const mbAliases = (meta?.aliases ?? [])
    .map(a => a.name?.trim()).filter((s): s is string => !!s);
  const lowerExisting = new Set(existingAliases.map(s => s.toLowerCase()));
  const aliasesAdded = mbAliases.filter(a => !lowerExisting.has(a.toLowerCase()));
  return {
    first_recording_year:  firstYear,
    first_recording_title: firstTitle,
    last_recording_year:   lastYear,
    last_recording_title:  lastTitle,
    aliasesAdded,
    releaseCount: releases.length,
  };
}

export interface MbEnrichResult {
  attempted: number;
  enriched: number;
  skipped: number;            // no MBID
  errors: Array<{ id: number; mbid?: string; message: string }>;
  durationMs: number;
}

export interface MbEnrichProgress {
  total: number;        // unknown ahead of time, populated after first page query
  processed: number;
  enriched: number;
  skipped: number;
  errors: number;
  currentName?: string;
}

/**
 * Walk every blues_artists row that has an MBID and enrich first/last
 * recording fields + aliases. Rate-limited to MusicBrainz's 1 req/s
 * policy; for ~177 artists this takes ~6–10 minutes.
 *
 * If `idFilter` is given, only processes that one row (used by the
 * per-row "Enrich" button in the editor).
 *
 * Pass `onProgress` to receive periodic updates — used by the
 * background-job runner to surface progress to the admin UI.
 */
export async function enrichBluesFromMusicBrainz(opts: {
  idFilter?: number; limit?: number;
  onProgress?: (p: MbEnrichProgress) => void;
} = {}): Promise<MbEnrichResult> {
  const start = Date.now();
  const errors: MbEnrichResult["errors"] = [];
  let attempted = 0, enriched = 0, skipped = 0;

  // Pull rows in name order, page through them so we don't keep an
  // unbounded array in memory if the table grows. We need an upfront
  // total count so the progress UI has a denominator — easy: count rows
  // matching the filter in one query.
  const PAGE = 200;
  let offset = 0;
  let processed = 0;
  const totalRowsRes = await listBluesArtists({ limit: 1, offset: 0 });
  const total = opts.idFilter ? 1 : (opts.limit ?? totalRowsRes.total);
  const reportProgress = (currentName?: string) => {
    if (!opts.onProgress) return;
    try {
      opts.onProgress({
        total,
        processed,
        enriched,
        skipped,
        errors: errors.length,
        currentName,
      });
    } catch { /* never let a progress callback abort the run */ }
  };
  reportProgress();
  outer: while (true) {
    const { rows } = await listBluesArtists({ limit: PAGE, offset });
    if (!rows.length) break;
    for (const row of rows) {
      if (opts.idFilter && row.id !== opts.idFilter) continue;
      attempted++;
      reportProgress(row.name);
      // Fallback: look up MBID by name if missing. Persist on success so
      // future passes (and the editor) can use it. If we can't resolve a
      // confident match, skip — too risky to bind the wrong MBID.
      let mbid = row.musicbrainz_mbid;
      if (!mbid) {
        mbid = await _resolveMbidByName(row.name);
        if (mbid) {
          try { await updateBluesArtist(row.id, { musicbrainz_mbid: mbid }); } catch {}
          await new Promise(res => setTimeout(res, MB_RATE_LIMIT_MS));
        }
      }
      if (!mbid) { skipped++; continue; }
      try {
        const deathYear = row.death_date ? _yearFromMbDate(row.death_date) : null;
        const patch = await _enrichOneFromMb(mbid, row.aliases ?? [], deathYear);
        const update: Record<string, any> = {};
        if (patch.first_recording_year)  update.first_recording_year  = patch.first_recording_year;
        if (patch.first_recording_title) update.first_recording_title = patch.first_recording_title;
        if (patch.last_recording_year)   update.last_recording_year   = patch.last_recording_year;
        if (patch.last_recording_title)  update.last_recording_title  = patch.last_recording_title;
        if (patch.aliasesAdded.length) {
          update.aliases = [...(row.aliases ?? []), ...patch.aliasesAdded];
        }
        // Mark enrichment so we can skip in future passes if desired.
        const status = (row.enrichment_status && typeof row.enrichment_status === "object")
          ? row.enrichment_status : {};
        update.enrichment_status = { ...status, mb: 1, mb_releases: patch.releaseCount };
        if (Object.keys(update).length) {
          await updateBluesArtist(row.id, update);
        }
        enriched++;
      } catch (err: any) {
        errors.push({ id: row.id, mbid: mbid || row.musicbrainz_mbid, message: err?.message ?? String(err) });
      }
      processed++;
      if (opts.limit && processed >= opts.limit) break outer;
      // Periodic progress every artist — cheap.
      reportProgress();
      // Rate-limit between artists to stay polite.
      await new Promise(res => setTimeout(res, MB_RATE_LIMIT_MS));
    }
    if (opts.idFilter) break;
    offset += PAGE;
  }
  reportProgress();

  return {
    attempted,
    enriched,
    skipped,
    errors,
    durationMs: Date.now() - start,
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Phase 3a: Wikipedia notes (free-text bio paragraph)
// ─────────────────────────────────────────────────────────────────────────

const WIKI_RATE_LIMIT_MS = 250;  // 4/s — Wikipedia is liberal but we stay polite
const WIKI_API = "https://en.wikipedia.org/w/api.php";

export interface GenericEnrichResult {
  attempted: number;
  enriched: number;
  skipped: number;
  errors: Array<{ id: number; message: string }>;
  durationMs: number;
}

function _titleFromWikiSuffix(suffix?: string | null): string | null {
  if (!suffix) return null;
  // Suffix is e.g. "/wiki/Charley_Patton". Strip the prefix and the
  // section anchor; URL-decode percent-encoded chars.
  const m = suffix.match(/\/wiki\/([^#?]+)/);
  if (!m) return null;
  try { return decodeURIComponent(m[1]).replace(/_/g, " "); }
  catch { return m[1].replace(/_/g, " "); }
}

/** Resolve an artist NAME to a Wikipedia article title via opensearch.
 *  Returns null if no clean match — opensearch is loose and can return
 *  near-misses, so we sanity-check the title before persisting. */
async function _resolveWikiTitleByName(name: string): Promise<string | null> {
  const trimmed = String(name || "").trim();
  if (!trimmed) return null;
  try {
    const url = `${WIKI_API}?action=opensearch&format=json&limit=3&search=${encodeURIComponent(trimmed)}`;
    const r = await fetch(url, {
      headers: {
        "User-Agent": "SeaDisco/1.0 (+https://seadisco.com; vinyl discovery app)",
        "Accept": "application/json",
      },
    });
    if (!r.ok) return null;
    const data = await r.json() as any;
    // opensearch shape: [query, [titles], [descriptions], [urls]]
    const titles = Array.isArray(data) && Array.isArray(data[1]) ? data[1] as string[] : [];
    if (!titles.length) return null;
    // Accept the top hit only if its name shares the first token with
    // our query (case-insensitive). Stops "Charley" matching "Charles
    // Dickens" or similar drift.
    const first = titles[0];
    const firstWord = (s: string) => s.split(/\s+/)[0].toLowerCase();
    return firstWord(first) === firstWord(trimmed) ? first : null;
  } catch { return null; }
}

async function _fetchWikipediaIntro(title: string): Promise<string | null> {
  const params = [
    "action=query", "format=json", "prop=extracts",
    "exintro=1", "explaintext=1", "redirects=1",
    `titles=${encodeURIComponent(title)}`,
  ].join("&");
  const r = await fetch(`${WIKI_API}?${params}`, {
    headers: {
      "User-Agent": "SeaDisco/1.0 (+https://seadisco.com; vinyl discovery app)",
      "Accept": "application/json",
    },
  });
  if (!r.ok) throw new Error(`Wikipedia ${r.status}`);
  const data = await r.json() as any;
  const pages = data?.query?.pages ?? {};
  const first = Object.values(pages)[0] as any;
  const text = first?.extract?.trim();
  return text || null;
}

export interface GenericEnrichProgress {
  total: number;
  processed: number;
  enriched: number;
  skipped: number;
  errors: number;
  currentName?: string;
}

/** Fill the `notes` field from Wikipedia's lead-section plain text for
 *  any row that has a `wikipedia_suffix` and no notes yet. Skips rows
 *  whose notes already contain content so we don't trample manual edits.
 */
export async function enrichBluesFromWikipedia(opts: {
  idFilter?: number; limit?: number; force?: boolean;
  onProgress?: (p: GenericEnrichProgress) => void;
} = {}): Promise<GenericEnrichResult> {
  const start = Date.now();
  const errors: GenericEnrichResult["errors"] = [];
  let attempted = 0, enriched = 0, skipped = 0;
  const PAGE = 200;
  let offset = 0;
  let processed = 0;
  const totalRowsRes = await listBluesArtists({ limit: 1, offset: 0 });
  const total = opts.idFilter ? 1 : (opts.limit ?? totalRowsRes.total);
  const reportProgress = (currentName?: string) => {
    if (!opts.onProgress) return;
    try { opts.onProgress({ total, processed, enriched, skipped, errors: errors.length, currentName }); }
    catch { /* swallow */ }
  };
  reportProgress();
  outer: while (true) {
    const { rows } = await listBluesArtists({ limit: PAGE, offset });
    if (!rows.length) break;
    for (const row of rows) {
      if (opts.idFilter && row.id !== opts.idFilter) continue;
      attempted++;
      reportProgress(row.name);
      // Fallback: opensearch the artist name if we don't already have a
      // wiki suffix. Persist on success so the editor and future runs
      // can use it. Pace the extra call.
      let title = _titleFromWikiSuffix(row.wikipedia_suffix);
      if (!title) {
        const found = await _resolveWikiTitleByName(row.name);
        if (found) {
          title = found;
          const newSuffix = `/wiki/${found.replace(/ /g, "_")}`;
          try { await updateBluesArtist(row.id, { wikipedia_suffix: newSuffix }); } catch {}
          await new Promise(res => setTimeout(res, WIKI_RATE_LIMIT_MS));
        }
      }
      if (!title) { skipped++; continue; }
      // Don't trample existing notes unless explicitly told to.
      if (!opts.force && row.notes?.trim()) { skipped++; continue; }
      try {
        const text = await _fetchWikipediaIntro(title);
        if (text) {
          const status = (row.enrichment_status && typeof row.enrichment_status === "object")
            ? row.enrichment_status : {};
          await updateBluesArtist(row.id, {
            notes: text,
            enrichment_status: { ...status, wiki: 1 },
          });
          enriched++;
        } else {
          skipped++;
        }
      } catch (err: any) {
        errors.push({ id: row.id, message: err?.message ?? String(err) });
      }
      processed++;
      reportProgress();
      if (opts.limit && processed >= opts.limit) break outer;
      await new Promise(res => setTimeout(res, WIKI_RATE_LIMIT_MS));
    }
    if (opts.idFilter) break;
    offset += PAGE;
  }
  reportProgress();
  return { attempted, enriched, skipped, errors, durationMs: Date.now() - start };
}

// ─────────────────────────────────────────────────────────────────────────
// Phase 3b: Discogs ID confirmation
// ─────────────────────────────────────────────────────────────────────────

const DISCOGS_RATE_LIMIT_MS = 1100; // 60/min for authenticated traffic

// ─────────────────────────────────────────────────────────────────────────
// Phase 4: full Discogs artist enrichment — pulls /artists/:id for every
// row in blues_artists with a discogs_id and stores everything useful:
// the bio (profile), realname, aliases, namevariations, members, groups,
// images and the external URLs array (Wikipedia, AllMusic, etc.).
// ─────────────────────────────────────────────────────────────────────────

/** Fetch the full Discogs artist record and merge its data into the
 *  blues_artists row. Idempotent — array fields union, scalar fields
 *  only fill blanks. The bio (Discogs `profile`) overwrites notes
 *  unless `force === false` is set. */
async function _enrichOneFromDiscogsArtist(
  client: DiscogsClient,
  row: any,
): Promise<{ patch: Record<string, any>; raw: any }> {
  const data: any = await (client as any).get(`/artists/${row.discogs_id}`);
  // Stay polite between the two calls per artist.
  await new Promise(res => setTimeout(res, DISCOGS_RATE_LIMIT_MS));
  // Pull the first 100 releases ascending so we can compute first/last
  // recording year + title and store the master/release ID list.
  let releasesPayload: any = null;
  try {
    releasesPayload = await (client as any).get(
      `/artists/${row.discogs_id}/releases`,
      { sort: "year", sort_order: "asc", per_page: "100" },
    );
  } catch {
    // Releases endpoint can 404 on tiny / wiped artist records — keep
    // going so the rest of the metadata still lands.
    releasesPayload = null;
  }
  const patch: Record<string, any> = {};

  // Bio. Discogs uses [b]/[i]/[url=…] BBCode-ish markup; we keep it
  // raw (the existing notes field is a free-text string already).
  if (typeof data.profile === "string" && data.profile.trim()) {
    patch.notes = data.profile.trim();
  }

  // Aliases come from three Discogs sources: the explicit `aliases`
  // array, the `namevariations` strings, and `realname`. Union them
  // with what we already have and dedupe case-insensitively.
  const existingAliases = Array.isArray(row.aliases) ? row.aliases : [];
  const merged: string[] = [];
  const seen = new Set<string>();
  const push = (s: any) => {
    if (typeof s !== "string") return;
    const t = s.trim();
    if (!t) return;
    const k = t.toLowerCase();
    if (seen.has(k)) return;
    seen.add(k);
    merged.push(t);
  };
  for (const a of existingAliases) push(a);
  for (const a of (data.aliases ?? [])) push(a?.name);
  for (const n of (data.namevariations ?? [])) push(n);
  push(data.realname);
  if (merged.length !== existingAliases.length) patch.aliases = merged;

  // Collaborators: members (for groups) + groups (for solo artists).
  const existingCollabs = Array.isArray(row.collaborators) ? row.collaborators : [];
  const collabSeen = new Set(existingCollabs.map((c: any) =>
    typeof c === "string" ? c.toLowerCase() : (c?.name ?? "").toLowerCase()
  ));
  const collabs = [...existingCollabs];
  for (const m of (data.members ?? [])) {
    const k = (m?.name ?? "").toLowerCase();
    if (k && !collabSeen.has(k)) {
      collabs.push({ name: m.name, discogs_id: m.id ?? null, kind: "member" });
      collabSeen.add(k);
    }
  }
  for (const g of (data.groups ?? [])) {
    const k = (g?.name ?? "").toLowerCase();
    if (k && !collabSeen.has(k)) {
      collabs.push({ name: g.name, discogs_id: g.id ?? null, kind: "group" });
      collabSeen.add(k);
    }
  }
  if (collabs.length !== existingCollabs.length) patch.collaborators = collabs;

  // First image → photo_url if blank. Discogs returns an array sorted
  // by primary first; the `uri` field is the full-size CDN URL.
  if (!row.photo_url && Array.isArray(data.images) && data.images[0]?.uri) {
    patch.photo_url = data.images[0].uri;
  }

  // External URLs — store the full array so we can render them later.
  // Wikipedia / AllMusic / SecondHandSongs / Wikipedia foreign-language
  // links, etc. all show up here.
  const urls = (data.urls ?? []).filter((u: any) => typeof u === "string" && u.trim());
  if (urls.length) {
    const existingUrls = Array.isArray(row.external_urls) ? row.external_urls : [];
    const urlSet = new Set([...existingUrls, ...urls]);
    if (urlSet.size !== existingUrls.length) patch.external_urls = Array.from(urlSet);
  }

  // Releases — first/last recording year + title + a deduplicated
  // discogs_releases array. Discogs results may include role="Main",
  // "TrackAppearance", "Producer", etc.; we only treat Main credits as
  // the artist's own recordings for first/last year purposes.
  const rawReleases: any[] = Array.isArray(releasesPayload?.releases)
    ? releasesPayload.releases : [];
  const dated = rawReleases
    .map(r => ({
      id: r.id,
      type: (r.type ?? "release") as string,
      title: typeof r.title === "string" ? r.title : "",
      year: typeof r.year === "number" && r.year > 0 ? r.year : null,
      label: r.label ?? null,
      role: r.role ?? null,
    }))
    .filter(r => r.year != null) as Array<{ id: number; type: string; title: string; year: number; label: any; role: string | null }>;
  // Sorted ascending so first is earliest. (We asked Discogs for asc but
  // re-sort defensively in case the page slice isn't perfectly ordered.)
  dated.sort((a, b) => a.year - b.year);
  const main = dated.filter(r => !r.role || r.role === "Main");
  const firstPick = main[0] ?? dated[0];
  if (firstPick) {
    patch.first_recording_year  = firstPick.year;
    patch.first_recording_title = firstPick.title || null;
  }
  // Death-year clamp so a posthumous reissue doesn't masquerade as the
  // artist's last recording. Mirrors the MB enricher behaviour.
  const deathYear = (() => {
    const m = String(row.death_date ?? "").match(/^(\d{4})/);
    return m ? parseInt(m[1], 10) : null;
  })();
  const ceiling = deathYear ? deathYear + 1 : Infinity;
  const inLifetimeMain = main.filter(r => r.year <= ceiling);
  const lastPick = (inLifetimeMain.length ? inLifetimeMain : main).slice(-1)[0]
                ?? (dated.length ? dated[dated.length - 1] : null);
  if (lastPick) {
    patch.last_recording_year  = lastPick.year;
    patch.last_recording_title = lastPick.title || null;
  }
  // Merge fetched releases into discogs_releases by (type:id) so the
  // existing per-row JSONB array gains entries we didn't have before
  // without duplicating ones we already stored from earlier passes.
  if (rawReleases.length) {
    const existing = Array.isArray(row.discogs_releases) ? row.discogs_releases : [];
    const seen = new Set(existing.map((r: any) => `${r.type}:${r.id}`));
    const merged = [...existing];
    for (const r of rawReleases) {
      const k = `${r.type ?? "release"}:${r.id}`;
      if (seen.has(k)) continue;
      seen.add(k);
      merged.push({
        id: r.id,
        type: (r.type ?? "release") as string,
        title: typeof r.title === "string" ? r.title : "",
        year: typeof r.year === "number" ? r.year : undefined,
        label: r.label ?? undefined,
        role: r.role ?? undefined,
      });
    }
    if (merged.length !== existing.length) patch.discogs_releases = merged;
  }

  // Mark this enrichment so we can skip-already-done in future passes.
  const status = (row.enrichment_status && typeof row.enrichment_status === "object")
    ? row.enrichment_status : {};
  patch.enrichment_status = { ...status, discogs_full: 1, discogs_full_at: new Date().toISOString() };

  return { patch, raw: data };
}

/** Walk every row that has a discogs_id and pull the full artist
 *  record. Rate-limited 1.1s/req. ~3-5 min for 200 rows. */
export async function enrichBluesFromDiscogsArtists(
  client: DiscogsClient,
  opts: {
    idFilter?: number; limit?: number;
    onProgress?: (p: GenericEnrichProgress) => void;
  } = {},
): Promise<GenericEnrichResult> {
  const start = Date.now();
  const errors: GenericEnrichResult["errors"] = [];
  let attempted = 0, enriched = 0, skipped = 0;
  const PAGE = 200;
  let offset = 0;
  let processed = 0;
  const totalRowsRes = await listBluesArtists({ limit: 1, offset: 0 });
  const total = opts.idFilter ? 1 : (opts.limit ?? totalRowsRes.total);
  const reportProgress = (currentName?: string) => {
    if (!opts.onProgress) return;
    try { opts.onProgress({ total, processed, enriched, skipped, errors: errors.length, currentName }); }
    catch { /* swallow */ }
  };
  reportProgress();
  outer: while (true) {
    const { rows } = await listBluesArtists({ limit: PAGE, offset });
    if (!rows.length) break;
    for (const row of rows) {
      if (opts.idFilter && row.id !== opts.idFilter) continue;
      attempted++;
      reportProgress(row.name);
      if (!row.discogs_id) { skipped++; continue; }
      try {
        const { patch } = await _enrichOneFromDiscogsArtist(client, row);
        if (Object.keys(patch).length) {
          await updateBluesArtist(row.id, patch);
        }
        enriched++;
      } catch (err: any) {
        errors.push({ id: row.id, message: err?.message ?? String(err) });
      }
      processed++;
      reportProgress();
      if (opts.limit && processed >= opts.limit) break outer;
      await new Promise(res => setTimeout(res, DISCOGS_RATE_LIMIT_MS));
    }
    if (opts.idFilter) break;
    offset += PAGE;
  }
  reportProgress();
  return { attempted, enriched, skipped, errors, durationMs: Date.now() - start };
}

/** Search Discogs for an artist by name, return the top hit's ID.
 *  Uses an admin-supplied DiscogsClient (PAT or OAuth). */
async function _searchDiscogsArtist(client: DiscogsClient, name: string): Promise<number | null> {
  // Use any to dodge the generic typing on the existing client.
  const data: any = await (client as any).get("/database/search", { q: name, type: "artist", per_page: "5" });
  const top = data?.results?.[0];
  return top?.id ?? null;
}

/** Fill `discogs_id` for any row missing one, by name lookup. Idempotent —
 *  rows with an existing discogs_id are skipped. Requires a Discogs
 *  client (admin's PAT/OAuth) since the search endpoint needs auth. */
export async function enrichBluesFromDiscogs(client: DiscogsClient, opts: {
  idFilter?: number; limit?: number;
  onProgress?: (p: GenericEnrichProgress) => void;
} = {}): Promise<GenericEnrichResult> {
  const start = Date.now();
  const errors: GenericEnrichResult["errors"] = [];
  let attempted = 0, enriched = 0, skipped = 0;
  const PAGE = 200;
  let offset = 0;
  let processed = 0;
  const totalRowsRes = await listBluesArtists({ limit: 1, offset: 0 });
  const total = opts.idFilter ? 1 : (opts.limit ?? totalRowsRes.total);
  const reportProgress = (currentName?: string) => {
    if (!opts.onProgress) return;
    try { opts.onProgress({ total, processed, enriched, skipped, errors: errors.length, currentName }); }
    catch { /* swallow */ }
  };
  reportProgress();
  outer: while (true) {
    const { rows } = await listBluesArtists({ limit: PAGE, offset });
    if (!rows.length) break;
    for (const row of rows) {
      if (opts.idFilter && row.id !== opts.idFilter) continue;
      attempted++;
      reportProgress(row.name);
      if (row.discogs_id) { skipped++; continue; }
      try {
        const id = await _searchDiscogsArtist(client, row.name);
        if (id) {
          const status = (row.enrichment_status && typeof row.enrichment_status === "object")
            ? row.enrichment_status : {};
          await updateBluesArtist(row.id, {
            discogs_id: id,
            enrichment_status: { ...status, discogs: 1 },
          });
          enriched++;
        } else {
          skipped++;
        }
      } catch (err: any) {
        errors.push({ id: row.id, message: err?.message ?? String(err) });
      }
      processed++;
      reportProgress();
      if (opts.limit && processed >= opts.limit) break outer;
      await new Promise(res => setTimeout(res, DISCOGS_RATE_LIMIT_MS));
    }
    if (opts.idFilter) break;
    offset += PAGE;
  }
  reportProgress();
  return { attempted, enriched, skipped, errors, durationMs: Date.now() - start };
}

// ─────────────────────────────────────────────────────────────────────────
// Phase 3c: YouTube top tracks (per-row only — quota-expensive)
// ─────────────────────────────────────────────────────────────────────────
//
// YouTube Data API v3 search costs 100 quota units per call. Default
// daily quota is 10,000. Bulk-running across 177 artists would burn
// the entire day's budget in seconds, so we only expose this as a
// per-row "Find on YouTube" enrichment from the editor.

const YT_API = "https://www.googleapis.com/youtube/v3";

export async function enrichBluesArtistFromYouTube(id: number, apiKey: string): Promise<{ added: string[] } | { error: string }> {
  if (!apiKey) return { error: "YOUTUBE_API_KEY not configured on the server" };
  const { rows } = await listBluesArtists({ limit: 1 });
  // Use list search with idFilter to grab the row we want.
  const targetRow = await (async () => {
    let off = 0;
    while (true) {
      const { rows: page } = await listBluesArtists({ limit: 200, offset: off });
      if (!page.length) return null;
      const hit = page.find(r => r.id === id);
      if (hit) return hit;
      off += 200;
    }
  })();
  if (!targetRow) return { error: "Row not found" };
  // Build a concise query: artist name + first known recording title (if
  // available) to avoid pulling unrelated channels with the same surname.
  const parts = [`"${targetRow.name}"`];
  if (targetRow.first_recording_title) parts.push(`"${targetRow.first_recording_title}"`);
  const q = parts.join(" ");
  const url = `${YT_API}/search?part=snippet&maxResults=3&type=video&q=${encodeURIComponent(q)}&key=${apiKey}`;
  const r = await fetch(url, { headers: { "Accept": "application/json" } });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    return { error: `YouTube ${r.status}: ${body.slice(0, 200)}` };
  }
  const data = await r.json() as any;
  const items = (data?.items ?? []) as Array<{ id?: { videoId?: string } }>;
  const newUrls = items
    .map(it => it.id?.videoId)
    .filter((v): v is string => !!v)
    .map(vid => `https://www.youtube.com/watch?v=${vid}`);
  if (!newUrls.length) return { added: [] };
  const existing = Array.isArray(targetRow.youtube_urls) ? targetRow.youtube_urls : [];
  const merged = Array.from(new Set([...existing, ...newUrls]));
  const status = (targetRow.enrichment_status && typeof targetRow.enrichment_status === "object")
    ? targetRow.enrichment_status : {};
  await updateBluesArtist(targetRow.id, {
    youtube_urls: merged,
    enrichment_status: { ...status, yt: 1 },
  });
  return { added: newUrls };
}

// ─────────────────────────────────────────────────────────────────────────
// Phase 1.5: Discogs year-walk seed
// ─────────────────────────────────────────────────────────────────────────
//
// Walk genre=Blues releases year by year (descending), pull every artist
// off the search results, and upsert by Discogs artist ID. Captures the
// per-artist list of Discogs master/release IDs (Masters+) we found
// them on so the row records exactly which 1923-1930 sides surfaced.
//
// Auth: admin's PAT/OAuth, 60 req/min limit, paced at 1.1s/request.
// Strict Blues-only filter — the previous multi-genre sweep (Folk,
// World & Country / Jazz with style refinements) pulled in too much
// non-blues noise. If the admin wants to widen the net later, this
// list is the one knob to turn.

const DISCOGS_SEED_RATE_MS = 1100;
const DISCOGS_PER_PAGE = 100;
const DISCOGS_MAX_PAGES_PER_YEAR = 25;

const DISCOGS_SEED_GENRES: Array<{ genre: string; style: string }> = [
  { genre: "Blues", style: "" },
];


interface DiscogsSearchResult {
  id: number;
  title: string;
  year?: string;
  master_id?: number;
  type: string;
  format?: string[];
  label?: string[];
  country?: string;
  genre?: string[];
  style?: string[];
}

/** Parse "Artist - Title" and return just the artist string. */
function _artistFromSearchTitle(t: string): string | null {
  if (!t) return null;
  const i = t.indexOf(" - ");
  if (i <= 0) return null;
  return t.slice(0, i).trim() || null;
}

/** Strip Discogs disambiguator suffixes like "(2)" so we dedupe homonyms. */
function _normaliseDiscogsName(s: string): string {
  return s.replace(/\s*\(\d+\)\s*$/, "").trim();
}

export interface DiscogsSeedResult {
  yearsScanned: number[];
  rowsScanned: number;
  uniqueArtists: number;
  artistsCreated: number;
  artistsMerged: number;
  releasesAdded: number;
  errors: Array<{ year: number; message: string }>;
  durationMs: number;
}

export interface DiscogsSeedProgress {
  phase: "scanning" | "upserting" | "done";
  yearsTotal: number;
  yearsScannedSoFar: number;
  rowsScannedSoFar: number;
  uniqueArtistsSoFar: number;
  upsertsTotal: number;
  upsertsDone: number;
}

/** Year-walk seed. Walks endYear..startYear descending, sweeps the
 *  configured genre/style filters, accumulates artist→releases pairs,
 *  resolves each artist name to a Discogs ID, then upserts.
 *
 *  Pass `onProgress` to receive periodic updates — used by the
 *  background-job runner to surface progress to the admin UI. */
export async function seedBluesArtistsFromDiscogs(
  client: DiscogsClient,
  opts: {
    startYear?: number; endYear?: number;
    perPage?: number; maxPages?: number; debug?: boolean;
    onProgress?: (p: DiscogsSeedProgress) => void;
  } = {},
): Promise<DiscogsSeedResult> {
  // Walk 1900..1930 by default. Years before ~1920 will return very
  // few hits but cost nothing to scan (one empty search call each),
  // and they catch the rare wax-cylinder / very-early 78 outliers.
  const startYear = opts.startYear ?? 1900;
  const endYear   = opts.endYear   ?? 1930;
  const perPage   = opts.perPage   ?? DISCOGS_PER_PAGE;
  const maxPages  = opts.maxPages  ?? DISCOGS_MAX_PAGES_PER_YEAR;
  const debug     = !!opts.debug;
  const start = Date.now();
  const errors: DiscogsSeedResult["errors"] = [];

  const years: number[] = [];
  for (let y = endYear; y >= startYear; y--) years.push(y);

  type Bucket = {
    releases: Array<{ id: number; type: string; title: string; year?: number; label?: string }>;
    styles: Set<string>;
    labels: Set<string>;
  };
  const byName = new Map<string, Bucket>();
  let rowsScanned = 0;
  let yearsScannedSoFar = 0;
  const reportProgress = (phase: DiscogsSeedProgress["phase"], upsertsDone: number, upsertsTotal: number) => {
    if (!opts.onProgress) return;
    try {
      opts.onProgress({
        phase,
        yearsTotal: years.length,
        yearsScannedSoFar,
        rowsScannedSoFar: rowsScanned,
        uniqueArtistsSoFar: byName.size,
        upsertsTotal,
        upsertsDone,
      });
    } catch { /* never let a progress callback abort the seed */ }
  };

  for (const year of years) {
    for (const filter of DISCOGS_SEED_GENRES) {
      for (let page = 1; page <= maxPages; page++) {
        const params: Record<string, string> = {
          type: "master",
          year: String(year),
          per_page: String(perPage),
          page: String(page),
          genre: filter.genre,
        };
        if (filter.style) params.style = filter.style;
        let data: any;
        try {
          data = await (client as any).get("/database/search", params);
        } catch (err: any) {
          errors.push({ year, message: `${filter.genre}/${filter.style || "*"} p${page}: ${err?.message ?? String(err)}` });
          break;
        }
        const results: DiscogsSearchResult[] = data?.results ?? [];
        rowsScanned += results.length;
        if (debug) console.log(`[discogs seed] ${year} ${filter.genre}/${filter.style || "*"} p${page} -> ${results.length} rows`);
        for (const r of results) {
          // Require Blues to be the PRIMARY genre on the master (first
          // entry in the genre array). Discogs returns matches if Blues
          // appears anywhere in the genre list, which lets comps and
          // Pop/Jazz reissues with a secondary Blues tag through. Strict
          // primary-only knocks Sophie Tucker / Al Jolson-style false
          // positives out without over-filtering one-off legit artists.
          const genres = Array.isArray(r.genre) ? r.genre : [];
          if (genres[0] !== "Blues") continue;
          const credit = _artistFromSearchTitle(r.title);
          if (!credit) continue;
          const lc = credit.toLowerCase();
          if (lc.includes("various") || lc === "unknown artist" || lc === "no artist") continue;
          if (/[,&/]/.test(credit) && credit.split(/[,&/]/).length > 2) continue;
          const key = _normaliseDiscogsName(credit);
          if (!byName.has(key)) byName.set(key, { releases: [], styles: new Set(), labels: new Set() });
          const b = byName.get(key)!;
          const id = r.id;
          const type = (r.type ?? "master") as string;
          const yr = r.year ? parseInt(r.year, 10) : year;
          const labelStr = Array.isArray(r.label) ? r.label[0] : (r.label as any) ?? "";
          const idx = r.title.indexOf(" - ");
          const titleOnly = idx > 0 ? r.title.slice(idx + 3) : r.title;
          b.releases.push({ id, type, title: titleOnly, year: yr || undefined, label: labelStr || undefined });
          for (const s of (r.style ?? [])) b.styles.add(s);
          if (labelStr) b.labels.add(labelStr);
        }
        if (results.length < perPage) break;
        await new Promise(res => setTimeout(res, DISCOGS_SEED_RATE_MS));
      }
    }
    yearsScannedSoFar++;
    reportProgress("scanning", 0, byName.size);
  }

  let artistsCreated = 0, artistsMerged = 0, releasesAdded = 0;
  let upsertsDone = 0;
  reportProgress("upserting", 0, byName.size);
  for (const [name, bucket] of byName.entries()) {
    let artistId: number | null = null;
    try {
      const search: any = await (client as any).get("/database/search", {
        q: name, type: "artist", per_page: "1",
      });
      artistId = search?.results?.[0]?.id ?? null;
    } catch (err: any) {
      errors.push({ year: -1, message: `artist lookup "${name}": ${err?.message ?? String(err)}` });
    }
    await new Promise(res => setTimeout(res, DISCOGS_SEED_RATE_MS));
    if (!artistId) continue;
    try {
      const out = await upsertBluesArtistByDiscogsId({
        discogs_id: artistId,
        name,
        discogs_releases: bucket.releases,
        styles: Array.from(bucket.styles),
        associated_labels: Array.from(bucket.labels),
      });
      if (out.created) artistsCreated++; else artistsMerged++;
      releasesAdded += out.mergedCount;
      upsertsDone++;
      // Cheap report — every 10 upserts is plenty for a UI poll loop.
      if (upsertsDone % 10 === 0) reportProgress("upserting", upsertsDone, byName.size);
    } catch (err: any) {
      errors.push({ year: -1, message: `upsert "${name}": ${err?.message ?? String(err)}` });
    }
  }
  reportProgress("done", upsertsDone, byName.size);

  return {
    yearsScanned: years,
    rowsScanned,
    uniqueArtists: byName.size,
    artistsCreated,
    artistsMerged,
    releasesAdded,
    errors,
    durationMs: Date.now() - start,
  };
}
