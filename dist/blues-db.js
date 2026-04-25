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
import { upsertBluesArtistByQid, listBluesArtists, updateBluesArtist } from "./db.js";
const WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql";
const WIKIDATA_UA = "SeaDisco/1.0 (+https://seadisco.com; vinyl discovery app)";
// Genre Q-numbers the seeder treats as "blues" for matching.
// Q9759 = blues, plus the major sub-genres common to pre-1930 artists.
// Query stays fast even with the union because of GENRE prop indexing.
const BLUES_GENRE_QIDS = [
    "Q9759", // blues
    "Q1191988", // country blues
    "Q1097732", // delta blues
    "Q5128515", // classic female blues
    "Q1130560", // piedmont blues
    "Q828224", // electric blues  (rare pre-1930 but cheap to include)
    "Q1058355", // ragtime          (overlaps the era)
];
// Cut-off year. The criterion is "released or recorded music before this".
const BEFORE_YEAR = 1930;
function _buildSeedSparql() {
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
  ?birth ?birthPlaceLabel
  ?death ?deathPlaceLabel ?causeLabel
  (GROUP_CONCAT(DISTINCT ?genreLabel;      separator="|") AS ?genresStr)
  (GROUP_CONCAT(DISTINCT ?instrumentLabel; separator="|") AS ?instrumentsStr)
  (GROUP_CONCAT(DISTINCT ?labelLabel;      separator="|") AS ?labelsStr)
  (GROUP_CONCAT(DISTINCT ?hometownLabel;   separator="|") AS ?hometownsStr)
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
  OPTIONAL { ?artist wdt:P19  ?birthPlace .  }
  OPTIONAL { ?artist wdt:P20  ?deathPlace .  }
  OPTIONAL { ?artist wdt:P509 ?cause .       }
  OPTIONAL { ?artist wdt:P136 ?genreFilled . }
  OPTIONAL { ?artist wdt:P1303 ?instrument . }
  OPTIONAL { ?artist wdt:P264  ?label .      }
  OPTIONAL { ?artist wdt:P551  ?hometown .   }
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
    ?deathPlace         rdfs:label    ?deathPlaceLabel .
    ?cause              rdfs:label    ?causeLabel .
    ?genreFilled        rdfs:label    ?genreLabel .
    ?instrument         rdfs:label    ?instrumentLabel .
    ?label              rdfs:label    ?labelLabel .
    ?hometown           rdfs:label    ?hometownLabel .
  }
}
GROUP BY ?artist ?artistLabel ?birth ?birthPlaceLabel
         ?death ?deathPlaceLabel ?causeLabel
         ?image ?wikipediaArticle ?mbid ?discogsId
ORDER BY ?artistLabel
`.trim();
}
function _qidFromUri(uri) {
    const i = uri.lastIndexOf("/");
    return i >= 0 ? uri.slice(i + 1) : uri;
}
function _wikiSuffixFromUrl(url) {
    if (!url)
        return null;
    // e.g. https://en.wikipedia.org/wiki/Charley_Patton → /wiki/Charley_Patton
    try {
        const u = new URL(url);
        return u.pathname.startsWith("/wiki/") ? u.pathname : null;
    }
    catch {
        return null;
    }
}
function _splitPipe(s) {
    if (!s)
        return [];
    return Array.from(new Set(s.split("|").map(x => x.trim()).filter(Boolean)));
}
function _firstPart(s) {
    const arr = _splitPipe(s);
    return arr.length ? arr[0] : null;
}
function _yearOnly(iso) {
    if (!iso)
        return null;
    // Wikidata returns 1899-04-29T00:00:00Z (or shorter for some entries).
    return iso.slice(0, 10);
}
/**
 * Run the Wikidata seed. Idempotent — re-running upserts on QID and
 * preserves any manually-edited fields the admin has filled in.
 */
export async function seedBluesArtistsFromWikidata() {
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
    const data = await r.json();
    const bindings = data?.results?.bindings ?? [];
    const errors = [];
    let upserted = 0;
    for (const b of bindings) {
        const qid = _qidFromUri(b.artist.value);
        const name = b.artistLabel?.value?.trim();
        if (!name)
            continue;
        const record = {
            wikidata_qid: qid,
            musicbrainz_mbid: b.mbid?.value || null,
            discogs_id: b.discogsId?.value ? parseInt(b.discogsId.value, 10) : null,
            name,
            aliases: _splitPipe(b.aliasesStr?.value),
            birth_date: _yearOnly(b.birth?.value),
            birth_place: b.birthPlaceLabel?.value || null,
            death_date: _yearOnly(b.death?.value),
            death_place: b.deathPlaceLabel?.value || null,
            death_cause: b.causeLabel?.value || null,
            hometown_region: _firstPart(b.hometownsStr?.value),
            styles: _splitPipe(b.genresStr?.value),
            instruments: _splitPipe(b.instrumentsStr?.value),
            associated_labels: _splitPipe(b.labelsStr?.value),
            photo_url: b.image?.value || null,
            wikipedia_suffix: _wikiSuffixFromUrl(b.wikipediaArticle?.value),
            enrichment_status: { wikidata: 1 },
        };
        try {
            await upsertBluesArtistByQid(record);
            upserted++;
        }
        catch (err) {
            errors.push({ qid, message: err?.message ?? String(err) });
        }
    }
    return {
        fetched: bindings.length,
        upserted,
        errors,
        durationMs: Date.now() - start,
    };
}
// ─────────────────────────────────────────────────────────────────────────
// Phase 2: MusicBrainz enrichment
// ─────────────────────────────────────────────────────────────────────────
const MB_ENDPOINT = "https://musicbrainz.org/ws/2";
const MB_UA = "SeaDisco/1.0 ( https://seadisco.com )";
const MB_RATE_LIMIT_MS = 1100; // 1 req/s strict + safety margin
const MB_RELEASE_PAGE = 100; // MB max per page
const MB_RELEASE_PAGE_CAP = 5; // hard cap on pages per artist (500 releases)
function _yearFromMbDate(d) {
    if (!d)
        return null;
    const m = d.match(/^(\d{4})/);
    if (!m)
        return null;
    const y = parseInt(m[1], 10);
    return Number.isFinite(y) && y >= 1800 && y <= 2100 ? y : null;
}
async function _mbFetch(path) {
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
        if (!r2.ok)
            throw new Error(`MB ${r2.status}: ${path}`);
        return r2.json();
    }
    if (!r.ok)
        throw new Error(`MB ${r.status}: ${path}`);
    return r.json();
}
/**
 * Fetch all releases for an MBID, paging up to MB_RELEASE_PAGE_CAP pages.
 * Skips bootleg / pseudo-release / promotion statuses for first/last
 * picks but keeps them in the count.
 */
async function _fetchAllMbReleases(mbid) {
    const all = [];
    for (let page = 0; page < MB_RELEASE_PAGE_CAP; page++) {
        const offset = page * MB_RELEASE_PAGE;
        const data = await _mbFetch(`/release?artist=${encodeURIComponent(mbid)}&limit=${MB_RELEASE_PAGE}&offset=${offset}`);
        const releases = (data?.releases ?? []);
        all.push(...releases);
        if (releases.length < MB_RELEASE_PAGE)
            break;
        // Pace ourselves between pages to stay under MB's 1 req/s limit.
        await new Promise(res => setTimeout(res, MB_RATE_LIMIT_MS));
    }
    return all;
}
async function _fetchMbArtistMeta(mbid) {
    try {
        const data = await _mbFetch(`/artist/${encodeURIComponent(mbid)}?inc=aliases+tags`);
        return data ?? null;
    }
    catch {
        return null;
    }
}
/** Enrich a single artist by MBID. Pure: returns the patch, doesn't write.
 *  deathYear (when known) clamps the "last" pick so a posthumous reissue
 *  in 2025 doesn't masquerade as the artist's last recording. */
async function _enrichOneFromMb(mbid, existingAliases = [], deathYear = null) {
    // Releases are the strongest signal for first/last recording year.
    const releases = await _fetchAllMbReleases(mbid);
    // Filter out bootleg / pseudo-release / promotion when picking first/last,
    // but only if there's at least one official release. Otherwise fall back
    // to whatever's available so we still get something.
    const PRIMARY_STATUS = new Set(["Official", undefined, ""]);
    const officialOnly = releases.filter(r => PRIMARY_STATUS.has(r.status));
    const pool = officialOnly.length ? officialOnly : releases;
    const dated = pool
        .map(r => ({ r, year: _yearFromMbDate(r.date) }))
        .filter(x => x.year != null);
    let firstYear = null;
    let firstTitle = null;
    let lastYear = null;
    let lastTitle = null;
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
        .map(a => a.name?.trim()).filter((s) => !!s);
    const lowerExisting = new Set(existingAliases.map(s => s.toLowerCase()));
    const aliasesAdded = mbAliases.filter(a => !lowerExisting.has(a.toLowerCase()));
    return {
        first_recording_year: firstYear,
        first_recording_title: firstTitle,
        last_recording_year: lastYear,
        last_recording_title: lastTitle,
        aliasesAdded,
        releaseCount: releases.length,
    };
}
/**
 * Walk every blues_artists row that has an MBID and enrich first/last
 * recording fields + aliases. Rate-limited to MusicBrainz's 1 req/s
 * policy; for ~177 artists this takes ~6–10 minutes.
 *
 * If `idFilter` is given, only processes that one row (used by the
 * per-row "Enrich" button in the editor).
 */
export async function enrichBluesFromMusicBrainz(opts = {}) {
    const start = Date.now();
    const errors = [];
    let attempted = 0, enriched = 0, skipped = 0;
    // Pull rows in name order, page through them so we don't keep an
    // unbounded array in memory if the table grows.
    const PAGE = 200;
    let offset = 0;
    let processed = 0;
    outer: while (true) {
        const { rows } = await listBluesArtists({ limit: PAGE, offset });
        if (!rows.length)
            break;
        for (const row of rows) {
            if (opts.idFilter && row.id !== opts.idFilter)
                continue;
            attempted++;
            if (!row.musicbrainz_mbid) {
                skipped++;
                continue;
            }
            try {
                const deathYear = row.death_date ? _yearFromMbDate(row.death_date) : null;
                const patch = await _enrichOneFromMb(row.musicbrainz_mbid, row.aliases ?? [], deathYear);
                const update = {};
                if (patch.first_recording_year)
                    update.first_recording_year = patch.first_recording_year;
                if (patch.first_recording_title)
                    update.first_recording_title = patch.first_recording_title;
                if (patch.last_recording_year)
                    update.last_recording_year = patch.last_recording_year;
                if (patch.last_recording_title)
                    update.last_recording_title = patch.last_recording_title;
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
            }
            catch (err) {
                errors.push({ id: row.id, mbid: row.musicbrainz_mbid, message: err?.message ?? String(err) });
            }
            processed++;
            if (opts.limit && processed >= opts.limit)
                break outer;
            // Rate-limit between artists to stay polite.
            await new Promise(res => setTimeout(res, MB_RATE_LIMIT_MS));
        }
        if (opts.idFilter)
            break;
        offset += PAGE;
    }
    return {
        attempted,
        enriched,
        skipped,
        errors,
        durationMs: Date.now() - start,
    };
}
