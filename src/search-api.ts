import express from "express";
import { DiscogsClient } from "./discogs-client.js";

const token = process.env.DISCOGS_TOKEN;
if (!token) {
  console.error("Error: DISCOGS_TOKEN environment variable is required.");
  process.exit(1);
}

const anthropicKey     = process.env.ANTHROPIC_API_KEY     ?? "";
const ticketmasterKey  = process.env.TICKETMASTER_API_KEY  ?? "";

const discogs = new DiscogsClient(token);
const app = express();

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
  const q = (req.query.q as string) ?? "";

  try {
    const results = await discogs.search(q, {
      type: req.query.type as "release" | "master" | "artist" | "label" | undefined,
      artist: stripArtistSuffix(req.query.artist as string | undefined),
      releaseTitle: req.query.release_title as string | undefined,
      label: req.query.label as string | undefined,
      year: req.query.year as string | undefined,
      genre: req.query.genre as string | undefined,
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

// GET /artist-bio?name=Miles+Davis — fetches bio via MusicBrainz → Wikipedia, falls back to Discogs
app.get("/artist-bio", async (req, res) => {
  const nameRaw = req.query.name as string;
  if (!nameRaw || !nameRaw.trim()) {
    res.status(400).json({ error: "Missing required query parameter: name" });
    return;
  }
  const name = nameRaw.replace(/\s*\(\d+\)$/, "").trim();

  try {
    // 1. Search MusicBrainz for the artist (get top 5 for disambiguation)
    const mbSearchUrl = `https://musicbrainz.org/ws/2/artist/?query=artist:${encodeURIComponent(name)}&fmt=json&limit=5`;
    const mbSearchRes = await fetch(mbSearchUrl, { headers: { "User-Agent": MB_UA } });
    const mbSearchData = await mbSearchRes.json() as any;
    const mbArtists = mbSearchData?.artists ?? [];
    const mbArtist  = mbArtists[0];
    const mbid      = mbArtist?.id;
    const mbName    = mbArtist?.name ?? name;

    // Collect alternatives: other results with the same or similar name
    const alternatives = mbArtists.slice(1).map((a: any) => ({
      name:           a.name ?? "",
      disambiguation: a.disambiguation ?? "",
    })).filter((a: any) => a.name);

    if (mbid) {
      // 2. Get URL relations to find Wikipedia link
      const mbArtistUrl = `https://musicbrainz.org/ws/2/artist/${mbid}?inc=url-rels&fmt=json`;
      const mbArtistRes = await fetch(mbArtistUrl, { headers: { "User-Agent": MB_UA } });
      const mbArtistData = await mbArtistRes.json() as any;

      const wikiRel = (mbArtistData?.relations ?? []).find((r: any) =>
        r.url?.resource?.includes("en.wikipedia.org/wiki/")
      );

      if (wikiRel) {
        // 3. Fetch Wikipedia summary
        const wikiTitle = decodeURIComponent(wikiRel.url.resource.split("/wiki/")[1]);
        const wikiRes = await fetch(
          `https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(wikiTitle)}`
        );
        const wikiData = await wikiRes.json() as any;
        const profile  = wikiData?.extract ?? null;
        if (profile) { res.json({ profile, name: mbName, alternatives }); return; }
      }
    }

    // Fallback: Discogs profile
    const results = await discogs.search(name, { type: "artist", perPage: 1 }) as any;
    const first = results?.results?.[0];
    if (!first?.id) { res.json({ profile: null, name: mbName ?? name }); return; }
    const artist = await discogs.getArtist(first.id) as any;
    let profile: string | null = artist?.profile ?? null;
    if (profile) profile = await resolveDiscogsIds(profile);
    res.json({ profile, name: artist?.name ?? mbName ?? name, alternatives });
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
        displayName = r?.name ?? "";
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

// GET /label-bio?name=Blue+Note — searches for label, returns their Discogs profile
app.get("/label-bio", async (req, res) => {
  const name = req.query.name as string;
  if (!name || !name.trim()) {
    res.status(400).json({ error: "Missing required query parameter: name" });
    return;
  }

  try {
    const results = await discogs.search(name, { type: "label", perPage: 1 }) as any;
    const first = results?.results?.[0];
    if (!first?.id) { res.json({ profile: null }); return; }

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
    const shows = events.map((e: any) => ({
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

const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3001;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Discogs search API listening on port ${PORT}`);
});
