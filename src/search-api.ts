import express from "express";
import Anthropic from "@anthropic-ai/sdk";
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
  const q = req.query.q as string;
  if (!q || !q.trim()) {
    res.status(400).json({ error: "Missing required query parameter: q" });
    return;
  }

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

// POST /parse  body: { q: "tell me about the history of ska music" }
app.post("/parse", express.json(), async (req, res) => {
  const q = req.body?.q as string;
  if (!q || !q.trim()) {
    res.status(400).json({ error: "Missing required body field: q" });
    return;
  }

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    // Fall back to passing the raw query unchanged
    res.json({ q, type: null, artist: null, release_title: null, year: null, label: null, genre: null });
    return;
  }

  try {
    const anthropic = new Anthropic({ apiKey });
    const message = await anthropic.messages.create({
      model: "claude-haiku-4-5",
      max_tokens: 300,
      messages: [{
        role: "user",
        content: `You are a Discogs search query parser. Given a natural language music query, extract structured search parameters and return ONLY a valid JSON object — no explanation, no markdown, just raw JSON.

Fields to extract (use null if not applicable):
- q: main search keyword(s) — the core topic (artist name, genre, era, etc.)
- type: one of "release", "master", "artist", "label", or null (use "release" for history/discography queries, "artist" for biography queries, null for general)
- artist: specific artist name if mentioned, else null
- release_title: specific album/release title if mentioned, else null
- year: specific year or null
- label: specific record label if mentioned, else null
- genre: Discogs genre if inferable (e.g. "Reggae", "Jazz", "Rock", "Electronic", "Classical", "Hip Hop", "Blues", "Folk, World, & Country", "Pop"), else null

Query: "${q.replace(/"/g, '\\"')}"`,
      }],
    });

    const raw = message.content[0].type === "text" ? message.content[0].text.trim() : "{}";
    // Strip any accidental markdown fencing
    const cleaned = raw.replace(/^```json?\s*/i, "").replace(/\s*```$/, "").trim();
    const parsed = JSON.parse(cleaned);
    res.json(parsed);
  } catch (err) {
    console.error(err);
    // On failure, return the raw query as-is
    res.json({ q, type: null, artist: null, release_title: null, year: null, label: null, genre: null });
  }
});

// GET /blurb?q=ska+music
app.get("/blurb", async (req, res) => {
  const q = req.query.q as string;
  if (!q || !q.trim()) {
    res.status(400).json({ error: "Missing required query parameter: q" });
    return;
  }

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    res.status(503).json({ blurb: null });
    return;
  }

  try {
    const anthropic = new Anthropic({ apiKey });
    const message = await anthropic.messages.create({
      model: "claude-haiku-4-5",
      max_tokens: 200,
      messages: [{
        role: "user",
        content: `Write 2-3 sentences about "${q}" in the context of music, artists, and vinyl records. Be informative and engaging. Plain text only, no headers or formatting.`,
      }],
    });
    const blurb = message.content[0].type === "text" ? message.content[0].text : null;
    res.json({ blurb });
  } catch (err) {
    console.error(err);
    res.json({ blurb: null });
  }
});

const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3001;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Discogs search API listening on port ${PORT}`);
});
