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
