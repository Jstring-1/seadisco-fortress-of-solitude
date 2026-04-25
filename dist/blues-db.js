// ── Pre-1930 blues artists DB seeder ───────────────────────────────────
//
// Phase 1: pull the seed list from Wikidata via SPARQL and upsert into the
// blues_artists table. Each artist is keyed by its Q-number so re-running
// the seed is idempotent and only fills empty fields without clobbering
// any manual admin edits stored alongside.
//
// Wikidata SPARQL endpoint:   https://query.wikidata.org/sparql
// Required header:            User-Agent (per Wikimedia policy)
// Default rate limit:         5 concurrent / 60s of CPU time per query
import { upsertBluesArtistByQid } from "./db.js";
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
