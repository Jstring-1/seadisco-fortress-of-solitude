import express from "express";
import { fileURLToPath } from "url";
import path from "path";
import { DiscogsClient } from "./discogs-client.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const token = process.env.DISCOGS_TOKEN;
if (!token) {
  console.error("Error: DISCOGS_TOKEN environment variable is required.");
  process.exit(1);
}

const anthropicKey     = process.env.ANTHROPIC_API_KEY     ?? "";
const ticketmasterKey  = process.env.TICKETMASTER_API_KEY  ?? "";

const discogs = new DiscogsClient(token);
const app = express();

// Serve static files from web/ (logo, etc.)
app.use(express.static(path.join(__dirname, "../web")));

// Allow any webpage to call this API
app.use((_req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  next();
});

function stripArtistSuffix(name: string | undefined): string | undefined {
  return name ? name.replace(/\s*\(\d+\)$/, "").trim() : undefined;
}

// GET /search?q=pink+floyd&type=master&year=1973&page=1&per_page=10
app.get("/search", async (req, res) => {
  const rawQ   = (req.query.q as string) ?? "";
  const artist = stripArtistSuffix(req.query.artist as string | undefined);
  // Discogs requires a non-empty q; fall back to the artist filter if q is blank
  const q = rawQ || artist || "";

  try {
    const results = await discogs.search(q, {
      type: req.query.type as "release" | "master" | "artist" | "label" | undefined,
      artist,
      releaseTitle: req.query.release_title as string | undefined,
      label: req.query.label as string | undefined,
      year: req.query.year as string | undefined,
      genre: req.query.genre as string | undefined,
      style: req.query.style as string | undefined,
      sort: req.query.sort as string | undefined,
      sortOrder: req.query.sort_order as "asc" | "desc" | undefined,
      page: req.query.page ? parseInt(req.query.page as string) : 1,
      perPage: req.query.per_page ? parseInt(req.query.per_page as string) : 12,
    });
    res.json(results);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Discogs API error" });
  }
});

// GET /release/:id
app.get("/release/:id", async (req, res) => {
  try {
    const result = await discogs.getRelease(req.params.id);
    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Discogs API error" });
  }
});

// GET /master/:id
app.get("/master/:id", async (req, res) => {
  try {
    const result = await discogs.getMasterRelease(req.params.id);
    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Discogs API error" });
  }
});

// GET /artist/:id
app.get("/artist/:id", async (req, res) => {
  try {
    const result = await discogs.getArtist(req.params.id);
    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Discogs API error" });
  }
});

const MB_UA = "DiscogsMCPSearch/1.0 ( search@sideman.pro )";

// Helper: fetch a Wikipedia extract by article title
async function fetchWikiSummary(title: string): Promise<{ extract: string; displayTitle: string } | null> {
  try {
    const r = await fetch(
      `https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(title)}`,
      { headers: { "User-Agent": MB_UA } }
    );
    if (!r.ok) return null;
    const d = await r.json() as any;
    if (d.type === "standard" && d.extract) return { extract: d.extract, displayTitle: d.title ?? title };
  } catch { /* fall through */ }
  return null;
}

// Helper: search Wikipedia and return best-matching article summary
async function searchWiki(query: string, name: string): Promise<{ extract: string; displayTitle: string } | null> {
  try {
    const r = await fetch(
      `https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch=${encodeURIComponent(query)}&format=json&utf8=1&srlimit=5`,
      { headers: { "User-Agent": MB_UA } }
    );
    const d = await r.json() as any;
    const hits: any[] = d?.query?.search ?? [];
    if (!hits.length) return null;

    const norm = (s: string) => s.toLowerCase().replace(/[^a-z0-9\s]/g, "").trim();
    const target = norm(name);
    // Prefer exact title match, otherwise use top result
    const best = hits.find(h => norm(h.title) === target) ?? hits[0];
    return fetchWikiSummary(best.title);
  } catch { return null; }
}

// GET /artist-bio?name=Miles+Davis[&id=123456] — Discogs bio
// If `id` is supplied the artist is fetched directly (no ambiguous name search).
app.get("/artist-bio", async (req, res) => {
  const nameRaw = req.query.name as string;
  const idParam = req.query.id ? parseInt(req.query.id as string, 10) : null;

  if (!nameRaw || !nameRaw.trim()) {
    res.status(400).json({ error: "Missing required query parameter: name" });
    return;
  }

  const mapNames = (arr: any[]) =>
    (arr ?? []).filter(x => x?.name)
               .map(x => ({ name: x.name as string, active: x.active, id: x.id as number | undefined }));

  // ── Fast path: direct lookup by Discogs ID ──────────────────────────────
  if (idParam) {
    try {
      const artist = await discogs.getArtist(idParam) as any;
      let profile: string | null = artist?.profile ?? null;
      if (profile) profile = await resolveDiscogsIds(profile);
      res.json({
        profile,
        name: artist?.name ?? nameRaw,
        alternatives: [],
        wikiExtract: null,
        members: mapNames(artist?.members ?? []),
        groups:  mapNames(artist?.groups  ?? []),
        aliases: mapNames(artist?.aliases ?? []),
      });
    } catch (err) {
      console.error(err);
      res.json({ profile: null });
    }
    return;
  }

  // ── Slow path: name search + best-match heuristics ─────────────────────
  // Strip suffix for the Discogs search query, but keep original for exact matching
  const nameForSearch = nameRaw.replace(/\s*\(\d+\)$/, "").trim();
  const nameForMatch  = nameRaw.trim();

  try {
    // Fetch Discogs candidates and Wikipedia in parallel
    const pAll = await Promise.all([
      discogs.search(nameForSearch, { type: "artist", perPage: 20 }),
      fetchWikiSummary(nameForSearch).then(r => r ?? searchWiki(`${nameForSearch} musician`, nameForSearch)),
    ]);
    const discogsResults = pAll[0] as any;
    const wikiResult     = pAll[1] as any;

    const candidates: any[] = discogsResults?.results ?? [];

    const norm = (s: string) => s.toLowerCase().replace(/[^a-z0-9\s]/g, "").trim();
    // Match against the full name (including suffix) so "Snail Mail (2)" finds the right entry
    const searchNorm = norm(nameForMatch);
    let best = candidates.find(a => norm(a.title) === searchNorm)
            ?? candidates.find(a => {
                 const an = norm(a.title);
                 return an.startsWith(norm(nameForSearch)) || norm(nameForSearch).startsWith(an);
               })
            ?? candidates[0];

    if (!best?.id) { res.json({ profile: null, name: nameForMatch }); return; }

    let artist = await discogs.getArtist(best.id) as any;
    let profile: string | null = artist?.profile ?? null;

    // If best match has no profile, check remaining candidates in parallel
    // but only accept a fallback whose name has word overlap with the search
    if (!profile && candidates.length > 1) {
      const sigWords = (s: string) => new Set(
        s.toLowerCase().replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter(w => w.length > 3)
      );
      const searchWords = sigWords(nameForSearch);
      const nameMatches = (title: string) => [...sigWords(title)].some(w => searchWords.has(w));

      const rest = candidates.filter(c => c.id !== best.id && nameMatches(c.title ?? ""));
      const restArtists = await Promise.all(
        rest.map(c => (discogs.getArtist(c.id) as Promise<any>).catch(() => null))
      );
      const idx = restArtists.findIndex(a => a?.profile);
      if (idx >= 0) {
        artist = restArtists[idx];
        best   = rest[idx];
        profile = artist.profile;
      }
    }

    if (profile) profile = await resolveDiscogsIds(profile);

    const alternatives = candidates
      .filter(a => a.id !== best.id && a.title)
      .slice(0, 19)
      .map(a => ({ name: a.title as string, id: a.id as number }));

    res.json({
      profile,
      name: artist?.name ?? nameForMatch,
      alternatives,
      wikiExtract: wikiResult?.extract ?? null,
      members: mapNames(artist?.members ?? []),
      groups:  mapNames(artist?.groups  ?? []),
      aliases: mapNames(artist?.aliases ?? []),
    });
  } catch (err) {
    console.error(err);
    res.json({ profile: null });
  }
});

// Helper: resolve Discogs ID tags in a profile string
async function resolveDiscogsIds(profile: string): Promise<string> {
  const idPattern = /\[([rma])=?(\d+)\]/g;
  const matches: { tag: string; type: string; id: string }[] = [];
  const seen = new Set<string>();
  let m;
  while ((m = idPattern.exec(profile)) !== null) {
    if (!seen.has(m[0])) {
      seen.add(m[0]);
      matches.push({ tag: m[0], type: m[1], id: m[2] });
    }
  }
  const resolved = await Promise.all(matches.map(async ({ tag, type, id }) => {
    try {
      let displayName = "";
      if (type === "r") {
        const r = await discogs.getRelease(id) as any;
        displayName = r?.title ?? "";
      } else if (type === "m") {
        const r = await discogs.getMasterRelease(id) as any;
        displayName = r?.title ?? "";
      } else {
        const r = await discogs.getArtist(id) as any;
        // Wrap in [a=Name] so the frontend can render it as a clickable link
        displayName = r?.name ? `[a=${r.name}]` : "";
      }
      return { tag, displayName };
    } catch {
      return { tag, displayName: "" };
    }
  }));
  for (const { tag, displayName } of resolved) {
    profile = profile.split(tag).join(displayName);
  }
  return profile;
}

// GET /label-bio?name=Blue+Note — Discogs label profile
app.get("/label-bio", async (req, res) => {
  const name = req.query.name as string;
  if (!name || !name.trim()) {
    res.status(400).json({ error: "Missing required query parameter: name" });
    return;
  }

  try {
    const results = await discogs.search(name, { type: "label", perPage: 1 }) as any;
    const first = results?.results?.[0];
    if (!first?.id) { res.json({ profile: null, name }); return; }
    const label = await discogs.getLabel(first.id) as any;
    let profile: string | null = label?.profile ?? null;
    if (profile) profile = await resolveDiscogsIds(profile);
    res.json({ profile, name: label?.name ?? name });
  } catch (err) {
    console.error(err);
    res.json({ profile: null });
  }
});

// GET /genre-info?genre=Jazz — returns a factual AI-generated genre description
app.get("/genre-info", async (req, res) => {
  const genre = req.query.genre as string;
  if (!genre || !genre.trim()) {
    res.status(400).json({ error: "Missing required query parameter: genre" });
    return;
  }
  if (!anthropicKey) { res.json({ profile: null }); return; }

  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": anthropicKey,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 220,
        messages: [{
          role: "user",
          content: `Write 2–3 sentences describing "${genre}" as a music genre. State only well-established, verifiable facts: its geographic or cultural origins, defining musical characteristics, and time period it emerged. Do not name specific artists or albums. Do not speculate.`,
        }],
      }),
    });
    const data = await response.json() as any;
    const profile = data?.content?.[0]?.text?.trim() ?? null;
    res.json({ profile, name: genre });
  } catch (err) {
    console.error(err);
    res.json({ profile: null });
  }
});

// GET /upcoming-shows?artist=Dead+Milkmen — returns upcoming Ticketmaster events
app.get("/upcoming-shows", async (req, res) => {
  const artist = req.query.artist as string;
  if (!artist || !artist.trim()) {
    res.status(400).json({ error: "Missing required query parameter: artist" });
    return;
  }
  if (!ticketmasterKey) { res.json({ shows: [] }); return; }

  try {
    const url = new URL("https://app.ticketmaster.com/discovery/v2/events.json");
    url.searchParams.set("apikey", ticketmasterKey);
    url.searchParams.set("keyword", artist);
    url.searchParams.set("classificationName", "music");
    url.searchParams.set("size", "10");
    url.searchParams.set("sort", "date,asc");

    const r = await fetch(url.toString());
    const data = await r.json() as any;
    const events = data?._embedded?.events ?? [];

    const tributeTerms = [
      'tribute', 'salute to', 'celebrating', 'the music of', 'songs of',
      'in memory', 'memorial', 'legacy of', 'a night of', 'versus', ' vs ',
      'experience', 'performed by', 'celebration of',
    ];

    const shows = events
      .filter((e: any) => {
        const name = (e.name ?? "").toLowerCase();
        return !tributeTerms.some(t => name.includes(t));
      })
      .map((e: any) => ({
        name:    e.name ?? "",
        date:    e.dates?.start?.localDate ?? "",
        time:    e.dates?.start?.localTime ?? "",
        venue:   e._embedded?.venues?.[0]?.name ?? "",
        city:    e._embedded?.venues?.[0]?.city?.name ?? "",
        country: e._embedded?.venues?.[0]?.country?.name ?? "",
        url:     e.url ?? "",
      }));
    res.json({ shows });
  } catch (err) {
    console.error(err);
    res.json({ shows: [] });
  }
});

// GET /marketplace-stats/:id?type=release|master
app.get("/marketplace-stats/:id", async (req, res) => {
  const { id } = req.params;
  const type = (req.query.type as string) ?? "release";

  try {
    let releaseId = id;
    if (type === "master") {
      const master = await discogs.getMasterRelease(id) as any;
      releaseId = String(master?.main_release ?? id);
    }

    const statsRes = await fetch(
      `https://api.discogs.com/marketplace/stats/${releaseId}?curr_abbr=USD`,
      { headers: { "Authorization": `Discogs token=${token}`, "User-Agent": MB_UA } }
    );
    const stats = await statsRes.json() as any;
    res.json({
      numForSale:  stats?.num_for_sale ?? 0,
      lowestPrice: stats?.lowest_price?.value ?? null,
      currency:    stats?.lowest_price?.currency ?? "USD",
      releaseId,
    });
  } catch (err) {
    console.error(err);
    res.json({ numForSale: 0, lowestPrice: null });
  }
});

const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3001;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Discogs search API listening on port ${PORT}`);
});
