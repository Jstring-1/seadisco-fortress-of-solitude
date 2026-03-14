import express from "express";
import { DiscogsClient } from "./discogs-client.js";

const token = process.env.DISCOGS_TOKEN;
if (!token) {
  console.error("Error: DISCOGS_TOKEN environment variable is required.");
  process.exit(1);
}

const discogs = new DiscogsClient(token);
const app = express();

// Allow any webpage to call this API
app.use((_req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  next();
});

// GET /search?q=pink+floyd&type=master&year=1973&page=1&per_page=10
app.get("/search", async (req, res) => {
  const q = (req.query.q as string) ?? "";

  try {
    const results = await discogs.search(q, {
      type: req.query.type as "release" | "master" | "artist" | "label" | undefined,
      artist: req.query.artist as string | undefined,
      releaseTitle: req.query.release_title as string | undefined,
      label: req.query.label as string | undefined,
      year: req.query.year as string | undefined,
      genre: req.query.genre as string | undefined,
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

// GET /artist-bio?name=Miles+Davis — searches for artist, returns their Discogs profile
app.get("/artist-bio", async (req, res) => {
  const name = req.query.name as string;
  if (!name || !name.trim()) {
    res.status(400).json({ error: "Missing required query parameter: name" });
    return;
  }

  try {
    const results = await discogs.search(name, { type: "artist", perPage: 1 }) as any;
    const first = results?.results?.[0];
    if (!first?.id) { res.json({ profile: null }); return; }

    const artist = await discogs.getArtist(first.id) as any;
    let profile: string | null = artist?.profile ?? null;

    if (profile) {
      // Find all ID-only references: [r=123], [m=123], [a123], [a=123]
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

      // Resolve all IDs in parallel
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
    }

    res.json({ profile, name: artist?.name ?? name });
  } catch (err) {
    console.error(err);
    res.json({ profile: null });
  }
});

const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3001;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Discogs search API listening on port ${PORT}`);
});
