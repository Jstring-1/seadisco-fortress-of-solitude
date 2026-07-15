// ── Lyric first-release-year resolver (Discogs search) ────────────────
//
// The Blues Archive's artist/enrichment subsystem was retired; all that
// remains here is the one still-live worker: it walks lyrics with no
// first_release_year and resolves each by searching Discogs for the
// earliest release/master matching the lyric's title (+ artist string).
// Backs the "Resolve via Discogs" button on the Blues Archive Lyrics tab.
import { getLyricsMissingFirstReleaseYear, getPool } from "./db.js";
const DISCOGS_RATE_LIMIT_MS = 1100; // 60/min for authenticated traffic
const _NORMALIZE_TITLE = (s) => String(s || "").toLowerCase().replace(/\([^)]*\)/g, "").replace(/[^a-z0-9]/g, "");
export async function resolveLyricFirstReleaseYearsDiscogs(client, opts = {}) {
    const start = Date.now();
    const errors = [];
    let resolved = 0, notFound = 0, processed = 0;
    const rows = await getLyricsMissingFirstReleaseYear(opts.limit ?? 5000);
    const total = rows.length;
    const reportProgress = (currentTitle) => {
        if (!opts.onProgress)
            return;
        try {
            opts.onProgress({
                total, processed, resolved, notFound,
                errors: errors.length,
                recentErrors: errors.slice(-20),
                currentTitle,
            });
        }
        catch { }
    };
    reportProgress();
    for (const row of rows) {
        if (opts.shouldStop?.())
            break;
        const title = (row.page_title || "").trim();
        const artist = (row.artist || "").trim();
        if (!title) {
            processed++;
            continue;
        }
        reportProgress(title);
        const normTarget = _NORMALIZE_TITLE(title);
        if (normTarget.length < 3) {
            processed++;
            notFound++;
            reportProgress();
            continue;
        }
        let bestYear = null;
        try {
            // Search release first — most pre-WWII blues lyrics resolve here.
            // Discogs's `q=` does fuzzy matching, but we still post-filter
            // results by normalized title so we don't grab look-alikes.
            const q = artist ? `${artist} ${title}` : title;
            const data = await client.get("/database/search", {
                q, type: "release", per_page: "25",
            });
            const results = Array.isArray(data?.results) ? data.results : [];
            for (const r of results) {
                const t = String(r?.title || "");
                // Discogs returns titles like "Artist - Track" — strip the
                // leading "<artist> - " segment when present.
                const trackOnly = t.includes(" - ") ? t.split(" - ").slice(1).join(" - ") : t;
                const candidates = [trackOnly, t];
                if (trackOnly.includes("/")) {
                    for (const p of trackOnly.split("/"))
                        candidates.push(p);
                }
                if (!candidates.some(c => _NORMALIZE_TITLE(c) === normTarget))
                    continue;
                const y = Number(r?.year);
                if (Number.isFinite(y) && y >= 1850 && (bestYear == null || y < bestYear))
                    bestYear = y;
            }
            // Be polite between the two requests we may make per lyric.
            await new Promise(res => setTimeout(res, DISCOGS_RATE_LIMIT_MS));
            // If release search came up empty, fall back to master search —
            // some songs only appear as master entries on Discogs.
            if (bestYear == null) {
                if (opts.shouldStop?.())
                    break;
                const md = await client.get("/database/search", {
                    q, type: "master", per_page: "25",
                });
                const mresults = Array.isArray(md?.results) ? md.results : [];
                for (const r of mresults) {
                    const t = String(r?.title || "");
                    const trackOnly = t.includes(" - ") ? t.split(" - ").slice(1).join(" - ") : t;
                    const candidates = [trackOnly, t];
                    if (trackOnly.includes("/")) {
                        for (const p of trackOnly.split("/"))
                            candidates.push(p);
                    }
                    if (!candidates.some(c => _NORMALIZE_TITLE(c) === normTarget))
                        continue;
                    const y = Number(r?.year);
                    if (Number.isFinite(y) && y >= 1850 && (bestYear == null || y < bestYear))
                        bestYear = y;
                }
                await new Promise(res => setTimeout(res, DISCOGS_RATE_LIMIT_MS));
            }
            if (bestYear != null) {
                await getPool().query(`UPDATE blues_lyrics
              SET first_release_year       = $1,
                  first_release_source     = 'discogs_search',
                  first_release_checked_at = NOW()
            WHERE id = $2
              AND (first_release_year IS NULL OR first_release_source IN ('artist_releases','discogs_search'))`, [bestYear, row.id]);
                resolved++;
            }
            else {
                // Stamp nothing — we only fetch rows where first_release_year IS
                // NULL, so a no-result row simply gets retried on the next pass.
                notFound++;
            }
        }
        catch (err) {
            errors.push({ id: row.id, title, message: err?.message ?? String(err) });
        }
        processed++;
        reportProgress();
    }
    reportProgress();
    return { attempted: processed, resolved, notFound, errors, durationMs: Date.now() - start };
}
