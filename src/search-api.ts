import express from "express";
import { fileURLToPath } from "url";
import path from "path";
import { DiscogsClient } from "./discogs-client.js";
import { initDb, getUserToken, setUserToken, deleteUserToken, deleteUserData, saveSearch, getSearchHistory, getRecentSearches, saveFeedback, getFeedback, deleteFeedback, getDiscogsUsername, setDiscogsUsername, getSyncStatus, upsertCollectionItems, upsertWantlistItems, getCollectionPage, getWantlistPage, getCollectionIds, getWantlistIds, updateCollectionSyncedAt, updateWantlistSyncedAt, getFreshReleases, getFreshReleasesByTag, getFreshTopTags } from "./db.js";
import { startFreshSyncSchedule } from "./sync-fresh-releases.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const sharedToken     = process.env.DISCOGS_TOKEN ?? "";
const anthropicKey    = process.env.ANTHROPIC_API_KEY    ?? "";
// Publishable key sent to frontend via /api/config
const authPk      = process.env.AUTH_PK ?? "";
// Set REQUIRE_AUTH=true to require users to sign in and provide their own Discogs token
const requireAuth = process.env.REQUIRE_AUTH === "true";

// Shared Discogs client (used as fallback when user has no personal token)
const discogs = sharedToken ? new DiscogsClient(sharedToken) : null;

// ── IP rate limiter for unauthenticated (shared-token) searches ───────────
const UNAUTH_LIMIT     = 5;
const LIMIT_WINDOW_MS  = 24 * 60 * 60 * 1000; // 24 hours
const ipCounts = new Map<string, { count: number; resetAt: number }>();

function clientIp(req: express.Request): string {
  const fwd = req.headers["x-forwarded-for"] as string | undefined;
  return (fwd ? fwd.split(",")[0] : req.ip ?? "unknown").replace(/^::ffff:/, "").trim();
}

function checkRateLimit(ip: string): { allowed: boolean; remaining: number } {
  const now = Date.now();
  const entry = ipCounts.get(ip);
  if (!entry || now > entry.resetAt) {
    ipCounts.set(ip, { count: 1, resetAt: now + LIMIT_WINDOW_MS });
    return { allowed: true, remaining: UNAUTH_LIMIT - 1 };
  }
  if (entry.count >= UNAUTH_LIMIT) return { allowed: false, remaining: 0 };
  entry.count++;
  return { allowed: true, remaining: UNAUTH_LIMIT - entry.count };
}

// Prune expired entries hourly to prevent memory growth
setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of ipCounts) if (now > entry.resetAt) ipCounts.delete(ip);
}, 60 * 60 * 1000);

// Decode Clerk session JWT from Authorization header (payload only — no pkg needed)
function getClerkUserId(req: express.Request): string | null {
  const auth = req.headers.authorization as string | undefined;
  if (!auth?.startsWith("Bearer ")) return null;
  try {
    const b64 = auth.slice(7).split(".")[1];
    const { sub } = JSON.parse(Buffer.from(b64, "base64").toString());
    return sub ?? null;
  } catch { return null; }
}

// Resolve Discogs token: user token → (if auth not required) shared token → null
async function getTokenForRequest(req: express.Request, allowFallback = false): Promise<string | null> {
  const userId = getClerkUserId(req);
  if (userId) {
    const userToken = await getUserToken(userId);
    if (userToken) return userToken;
  }
  // Fall back to shared token when auth is disabled OR when explicitly allowed (bio endpoints)
  if (!requireAuth || allowFallback) return sharedToken || null;
  return null;
}

async function getDiscogsForRequest(req: express.Request, allowFallback = false): Promise<DiscogsClient | null> {
  const t = await getTokenForRequest(req, allowFallback);
  if (!t) return null;
  if (t === sharedToken && discogs) return discogs;
  return new DiscogsClient(t);
}

// Boot DB if a connection string is configured
if (process.env.APP_DB_URL) {
  initDb().catch(err => console.error("DB init failed:", err));
}

const app = express();
app.set("trust proxy", true); // respect X-Forwarded-For from Railway's proxy

// Serve static files from web/ (logo, etc.)
app.use(express.static(path.join(__dirname, "../web"), { extensions: ["html"] }));

// Allow any webpage to call this API
app.use((_req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (_req.method === "OPTIONS") { res.sendStatus(204); return; }
  next();
});

// ── Auth / account endpoints ──────────────────────────────────────────────

// GET /api/config — public config for the frontend
app.get("/api/config", (_req, res) => {
  res.json({ clerkPublishableKey: authPk, authEnabled: requireAuth });
});

// GET /api/user/token — returns whether the user has a token saved
app.get("/api/user/token", async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const t = await getUserToken(userId);
  res.json({ hasToken: !!t, masked: t ? `****${t.slice(-4)}` : null });
});

// POST /api/user/token — save user's Discogs personal access token
app.post("/api/user/token", express.json(), async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const { token } = req.body ?? {};
  if (!token || typeof token !== "string" || token.trim().length < 8) {
    res.status(400).json({ error: "Invalid token" }); return;
  }
  await setUserToken(userId, token.trim());
  // Fetch Discogs username from /oauth/identity using the user's token
  try {
    const identRes = await fetch("https://api.discogs.com/oauth/identity", {
      headers: { "Authorization": `Discogs token=${token.trim()}`, "User-Agent": "SeaDisco/1.0" }
    });
    if (identRes.ok) {
      const ident = await identRes.json() as { username?: string };
      if (ident.username) await setDiscogsUsername(userId, ident.username);
    }
  } catch {}
  res.json({ ok: true });
});

// DELETE /api/user/token — remove user's saved token
app.delete("/api/user/token", async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  await deleteUserToken(userId);
  res.json({ ok: true });
});

// DELETE /api/user/account — wipe all user data from our DB (Clerk deletion handled client-side)
app.delete("/api/user/account", async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  await deleteUserData(userId);
  res.json({ ok: true });
});

// POST /api/user/sync — sync collection and/or wantlist from Discogs
app.post("/api/user/sync", express.json(), async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }

  const { type = "both" } = req.body ?? {};
  const syncCollection = type === "collection" || type === "both";
  const syncWantlist   = type === "wantlist"   || type === "both";

  // Check cooldown: skip individual types if synced within last hour
  const syncStatus = await getSyncStatus(userId);
  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
  const collectionRecent = syncCollection && !!syncStatus.collectionSyncedAt && syncStatus.collectionSyncedAt > oneHourAgo;
  const wantlistRecent   = syncWantlist   && !!syncStatus.wantlistSyncedAt   && syncStatus.wantlistSyncedAt   > oneHourAgo;
  if (collectionRecent && wantlistRecent) {
    res.json({ ok: true, skipped: true, reason: "Recently synced" });
    return;
  }

  const token = await getUserToken(userId);
  if (!token) { res.status(400).json({ error: "No Discogs token found" }); return; }

  let username = await getDiscogsUsername(userId);
  if (!username) {
    // Auto-fetch username from Discogs identity endpoint (token saved before this feature)
    try {
      const identRes = await fetch("https://api.discogs.com/oauth/identity", {
        headers: { "Authorization": `Discogs token=${token}`, "User-Agent": "SeaDisco/1.0" }
      });
      if (identRes.ok) {
        const ident = await identRes.json() as { username?: string };
        if (ident.username) {
          await setDiscogsUsername(userId, ident.username);
          username = ident.username;
        }
      }
    } catch {}
  }
  if (!username) { res.status(400).json({ error: "Could not determine Discogs username" }); return; }

  const headers = { "Authorization": `Discogs token=${token}`, "User-Agent": "SeaDisco/1.0" };
  const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

  let collectionCount = 0;
  let wantlistCount   = 0;

  try {
    if (syncCollection && !collectionRecent) {
      for (let page = 1; page <= 10; page++) {
        if (page > 1) await delay(1000);
        const r = await fetch(
          `https://api.discogs.com/users/${encodeURIComponent(username)}/collection/folders/0/releases?per_page=100&page=${page}`,
          { headers }
        );
        if (!r.ok) break;
        const data = await r.json() as any;
        const releases: any[] = data.releases ?? [];
        if (!releases.length) break;
        const items = releases.map((item: any) => ({
          id:      item.basic_information?.id as number,
          data:    item.basic_information as object,
          addedAt: item.date_added ? new Date(item.date_added) : undefined,
        })).filter(i => i.id);
        await upsertCollectionItems(userId, items);
        collectionCount += items.length;
        if (releases.length < 100) break;
      }
      await updateCollectionSyncedAt(userId);
    }

    if (syncWantlist && !wantlistRecent) {
      for (let page = 1; page <= 10; page++) {
        if (page > 1) await delay(1000);
        const r = await fetch(
          `https://api.discogs.com/users/${encodeURIComponent(username)}/wants?per_page=100&page=${page}`,
          { headers }
        );
        if (!r.ok) break;
        const data = await r.json() as any;
        const wants: any[] = data.wants ?? [];
        if (!wants.length) break;
        const items = wants.map((item: any) => ({
          id:      item.id as number,
          data:    item.basic_information as object,
          addedAt: item.date_added ? new Date(item.date_added) : undefined,
        })).filter(i => i.id);
        await upsertWantlistItems(userId, items);
        wantlistCount += items.length;
        if (wants.length < 100) break;
      }
      await updateWantlistSyncedAt(userId);
    }

    res.json({ ok: true, collectionCount, wantlistCount });
  } catch (err) {
    console.error("Sync error:", err);
    res.status(500).json({ error: "Sync failed" });
  }
});

// GET /api/user/collection — paginated cached collection
app.get("/api/user/collection", async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const page    = parseInt(req.query.page    as string) || 1;
  const perPage = parseInt(req.query.per_page as string) || 25;
  const { items, total } = await getCollectionPage(userId, page, perPage);
  res.json({ items, total, page, pages: Math.ceil(total / perPage) });
});

// GET /api/user/wantlist — paginated cached wantlist
app.get("/api/user/wantlist", async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const page    = parseInt(req.query.page    as string) || 1;
  const perPage = parseInt(req.query.per_page as string) || 25;
  const { items, total } = await getWantlistPage(userId, page, perPage);
  res.json({ items, total, page, pages: Math.ceil(total / perPage) });
});

// GET /api/user/discogs-ids — collection and wantlist IDs for badge rendering
app.get("/api/user/discogs-ids", async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const [collectionIds, wantlistIds] = await Promise.all([
    getCollectionIds(userId),
    getWantlistIds(userId),
  ]);
  res.json({ collectionIds, wantlistIds });
});

// GET /api/user/sync-status — last sync timestamps + Discogs username
app.get("/api/user/sync-status", async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const [syncStatus, username] = await Promise.all([
    getSyncStatus(userId),
    getDiscogsUsername(userId),
  ]);
  res.json({
    collectionSyncedAt: syncStatus.collectionSyncedAt,
    wantlistSyncedAt:   syncStatus.wantlistSyncedAt,
    discogsUsername:    username,
  });
});

function stripArtistSuffix(name: string | undefined): string | undefined {
  return name ? name.replace(/\s*\(\d+\)$/, "").trim() : undefined;
}

// GET /api/user/history — recent searches for the logged-in user
app.get("/api/user/history", async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const history = await getSearchHistory(userId);
  res.json({ history });
});

// POST /api/feedback — save feedback from signed-in user
app.post("/api/feedback", express.json(), async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const { message, userEmail } = req.body;
  if (!message?.trim()) { res.status(400).json({ error: "Message required" }); return; }
  await saveFeedback(userId, userEmail ?? "", message.trim());
  res.json({ ok: true });
});

// GET /api/admin/feedback — inbox, only for admin user
app.get("/api/admin/feedback", async (req, res) => {
  const userId = getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const items = await getFeedback();
  res.json({ items });
});

// DELETE /api/admin/feedback/:id — delete a feedback item, admin only
app.delete("/api/admin/feedback/:id", async (req, res) => {
  const userId = getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  await deleteFeedback(parseInt(req.params.id));
  res.json({ ok: true });
});

// POST /api/ai-search — Claude music recommendations
app.post("/api/ai-search", express.json(), async (req, res) => {
  const userId = getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "no_token" }); return; }
  if (!anthropicKey) { res.status(503).json({ error: "AI not configured" }); return; }

  const q = (req.body.q as string ?? "").trim();
  if (!q) { res.status(400).json({ error: "Query required" }); return; }

  const prompt = `You are a music expert specializing in vinyl records, rare and world music.
The user is searching for: "${q}"

Return a JSON array of 6-8 music recommendations (artists or albums/releases) that best match this query.
For each item include:
- name: artist name, or "Album Title by Artist"
- type: "artist" or "release"
- description: one compelling sentence explaining why this fits
- discogsParams: object with relevant Discogs search fields only (choose from: q, artist, label, genre, style, year). year must be a single 4-digit year, never a range.

Return ONLY a valid JSON array, no markdown, no explanation.`;

  try {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": anthropicKey,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 1024,
        messages: [{ role: "user", content: prompt }],
      }),
    });
    const data = await r.json() as any;
    if (!r.ok) {
      console.error("Anthropic API error:", data);
      res.status(502).json({ error: `AI error: ${data.error?.message ?? r.status}` });
      return;
    }
    let text = (data.content?.[0]?.text ?? "[]").trim();
    // Strip markdown code fences if present
    text = text.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "");
    const recommendations = JSON.parse(text);
    res.json({ recommendations });
  } catch (err) {
    console.error("AI search error:", err);
    res.status(500).json({ error: "AI search failed: " + (err as Error).message });
  }
});

// GET /api/recent-searches — anonymous global feed
app.get("/api/recent-searches", async (_req, res) => {
  if (!process.env.APP_DB_URL) { res.json({ searches: [] }); return; }
  try {
    const raw = await getRecentSearches(500);
    // Deduplicate by normalised content: lowercase q/artist/label/release/genre/style/year
    // Searches differing only in format, type, or sort are treated as the same
    const seen = new Set<string>();
    const searches = raw.filter(({ params }) => {
      const sig = [params.q, params.artist, params.release_title, params.label, params.genre, params.style, params.year]
        .map(v => (v ?? "").toLowerCase().trim())
        .join("|");
      if (!sig.replace(/\|/g, "").trim() || seen.has(sig)) return false;
      seen.add(sig);
      return true;
    });
    // Shuffle and return 20 random entries
    for (let i = searches.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [searches[i], searches[j]] = [searches[j], searches[i]];
    }
    res.json({ searches: searches.slice(0, 20) });
  } catch { res.json({ searches: [] }); }
});

// GET /search?q=pink+floyd&type=master&year=1973&page=1&per_page=10
app.get("/search", async (req, res) => {
  const rawQ   = (req.query.q as string) ?? "";
  const artist = stripArtistSuffix(req.query.artist as string | undefined);
  const q = rawQ || artist || "";

  const userId = getClerkUserId(req);
  const userToken = userId ? await getUserToken(userId) : null;
  const usingSharedToken = !userToken;

  // Rate-limit unauthenticated users — allow 5 free searches/day via shared token
  if (usingSharedToken) {
    if (!sharedToken) {
      res.status(401).json({ error: "no_token", message: "Sign in and add your Discogs API token to search." });
      return;
    }
    const ip = clientIp(req);
    const { allowed, remaining } = checkRateLimit(ip);
    if (!allowed) {
      res.status(429).json({
        error: "rate_limited",
        message: `Free searches used up for today. Sign in and add your own Discogs token for unlimited searches.`,
      });
      return;
    }
    res.setHeader("X-RateLimit-Remaining", remaining);
  }

  // allowFallback=true for unauthenticated users — they passed the rate limit check above
  const dc = await getDiscogsForRequest(req, usingSharedToken);
  if (!dc) {
    res.status(401).json({ error: "no_token", message: "Sign in and add your Discogs API token to search." });
    return;
  }

  try {
    const results = await dc.search(q, {
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

    // Record search history for logged-in users (fire-and-forget, page 1 only)
    const userId = getClerkUserId(req);
    const isFirstPage = !req.query.page || req.query.page === "1";
    if (userId && isFirstPage) {
      const artistParam = req.query.artist ? String(req.query.artist) : "";
      const p: Record<string, string> = {};
      // Skip q if it duplicates artist (case-insensitive); skip default Vinyl format
      if (rawQ && rawQ.toLowerCase() !== artistParam.toLowerCase()) p.q = rawQ;
      if (artistParam)             p.artist        = artistParam;
      if (req.query.release_title) p.release_title = String(req.query.release_title);
      if (req.query.label)         p.label         = String(req.query.label);
      if (req.query.year)          p.year          = String(req.query.year);
      if (req.query.genre)         p.genre         = String(req.query.genre);
      if (req.query.style)         p.style         = String(req.query.style);
      const fmt = req.query.format ? String(req.query.format) : "";
      if (fmt && fmt !== "Vinyl")  p.format        = fmt;
      if (req.query.type)          p.type          = String(req.query.type);
      if (req.query.sort)          p.sort          = String(req.query.sort);
      const hasResults = (results as any)?.results?.length > 0;
      if (Object.keys(p).length && hasResults) saveSearch(userId, p).catch(() => {});
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Discogs API error" });
  }
});

// GET /release/:id
app.get("/release/:id", async (req, res) => {
  const dc = await getDiscogsForRequest(req, true);
  if (!dc) { res.status(503).json({ error: "No Discogs token configured" }); return; }
  try {
    const result = await dc.getRelease(req.params.id);
    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Discogs API error" });
  }
});

// GET /master/:id
app.get("/master/:id", async (req, res) => {
  const dc = await getDiscogsForRequest(req, true);
  if (!dc) { res.status(503).json({ error: "No Discogs token configured" }); return; }
  try {
    const result = await dc.getMasterRelease(req.params.id);
    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Discogs API error" });
  }
});

// GET /artist/:id
app.get("/artist/:id", async (req, res) => {
  const dc = await getDiscogsForRequest(req, true);
  if (!dc) { res.status(503).json({ error: "No Discogs token configured" }); return; }
  try {
    const result = await dc.getArtist(req.params.id);
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

  const dc = await getDiscogsForRequest(req, true);
  if (!dc) { res.json({ profile: null }); return; }

  const mapNames = (arr: any[]) =>
    (arr ?? []).filter(x => x?.name)
               .map(x => ({ name: x.name as string, active: x.active, id: x.id as number | undefined }));

  // ── Fast path: direct lookup by Discogs ID ──────────────────────────────
  if (idParam) {
    try {
      const nameForWiki = nameRaw.replace(/\s*\(\d+\)$/, "").trim();
      const [artist, wikiResult] = await Promise.all([
        dc.getArtist(idParam) as Promise<any>,
        fetchWikiSummary(nameForWiki).then(r => r ?? searchWiki(`${nameForWiki} musician`, nameForWiki)),
      ]);
      let profile: string | null = artist?.profile ?? null;
      if (profile) profile = await resolveDiscogsIds(profile, dc);
      res.json({
        profile,
        name: artist?.name ?? nameRaw,
        alternatives: [],
        wikiExtract: wikiResult?.extract ?? null,
        members:        mapNames(artist?.members ?? []),
        groups:         mapNames(artist?.groups  ?? []),
        aliases:        mapNames(artist?.aliases ?? []),
        namevariations: (artist?.namevariations ?? []).filter(Boolean),
        urls:           (artist?.urls ?? []).filter(Boolean),
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
      dc.search(nameForSearch, { type: "artist", perPage: 20 }),
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

    let artist = await dc.getArtist(best.id) as any;
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
        rest.map(c => (dc.getArtist(c.id) as Promise<any>).catch(() => null))
      );
      const idx = restArtists.findIndex(a => a?.profile);
      if (idx >= 0) {
        artist = restArtists[idx];
        best   = rest[idx];
        profile = artist.profile;
      }
    }

    if (profile) profile = await resolveDiscogsIds(profile, dc);

    const alternatives = candidates
      .filter(a => a.id !== best.id && a.title)
      .slice(0, 19)
      .map(a => ({ name: a.title as string, id: a.id as number }));

    res.json({
      profile,
      name: artist?.name ?? nameForMatch,
      discogsId: best.id ?? null,
      alternatives,
      wikiExtract: wikiResult?.extract ?? null,
      members:        mapNames(artist?.members ?? []),
      groups:         mapNames(artist?.groups  ?? []),
      aliases:        mapNames(artist?.aliases ?? []),
      namevariations: (artist?.namevariations ?? []).filter(Boolean),
      urls:           (artist?.urls ?? []).filter(Boolean),
    });
  } catch (err) {
    console.error(err);
    res.json({ profile: null });
  }
});

// Helper: resolve Discogs ID tags in a profile string
async function resolveDiscogsIds(profile: string, dc: DiscogsClient = discogs!): Promise<string> {
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
        const r = await dc.getRelease(id) as any;
        displayName = r?.title ?? "";
      } else if (type === "m") {
        const r = await dc.getMasterRelease(id) as any;
        displayName = r?.title ?? "";
      } else {
        const r = await dc.getArtist(id) as any;
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

  const dc = await getDiscogsForRequest(req, true);
  if (!dc) { res.json({ profile: null }); return; }

  try {
    const results = await dc.search(name, { type: "label", perPage: 1 }) as any;
    const first = results?.results?.[0];
    if (!first?.id) { res.json({ profile: null, name }); return; }
    const label = await dc.getLabel(first.id) as any;
    let profile: string | null = label?.profile ?? null;
    if (profile) profile = await resolveDiscogsIds(profile, dc);
    res.json({
      profile,
      name:        label?.name ?? name,
      urls:        (label?.urls ?? []).filter(Boolean),
      parentLabel: label?.parent_label?.name
                     ? { name: label.parent_label.name, id: label.parent_label.id }
                     : null,
      sublabels:   (label?.sublabels ?? []).filter((x: any) => x?.name)
                     .map((x: any) => ({ name: x.name as string, id: x.id as number })),
    });
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

// GET /marketplace-stats/:id?type=release|master
app.get("/marketplace-stats/:id", async (req, res) => {
  const { id } = req.params;
  const type = (req.query.type as string) ?? "release";
  const dc = await getDiscogsForRequest(req, true);
  const reqToken = await getTokenForRequest(req);
  if (!dc || !reqToken) { res.json({ numForSale: 0, lowestPrice: null }); return; }

  try {
    let releaseId = id;
    if (type === "master") {
      const master = await dc.getMasterRelease(id) as any;
      releaseId = String(master?.main_release ?? id);
    }

    const statsRes = await fetch(
      `https://api.discogs.com/marketplace/stats/${releaseId}?curr_abbr=USD`,
      { headers: { "Authorization": `Discogs token=${reqToken}`, "User-Agent": MB_UA } }
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

// GET /master-versions/:id — all pressings/versions of a master release
app.get("/master-versions/:id", async (req, res) => {
  const { id } = req.params;
  const reqToken = await getTokenForRequest(req);
  if (!reqToken) { res.json({ versions: [] }); return; }
  try {
    const r = await fetch(
      `https://api.discogs.com/masters/${id}/versions?per_page=100&sort=released&sort_order=asc`,
      { headers: { "Authorization": `Discogs token=${reqToken}`, "User-Agent": MB_UA } }
    );
    const data = await r.json() as any;
    const versions = (data.versions ?? []).map((v: any) => ({
      id:      v.id,
      title:   v.title,
      label:   v.label,
      catno:   v.catno,
      country: v.country,
      year:    v.released,
      format:  v.format,
      url:     v.resource_url ? `https://www.discogs.com/release/${v.id}` : null,
    }));
    res.json({ versions });
  } catch (err) {
    console.error(err);
    res.json({ versions: [] });
  }
});

// GET /api/fresh-releases[?tag=folk] — releases + top tags
app.get("/api/fresh-releases", async (req, res) => {
  try {
    const tag = req.query.tag ? String(req.query.tag) : "";
    const [releases, topTags] = await Promise.all([
      tag ? getFreshReleasesByTag(tag, 48) : getFreshReleases(48),
      getFreshTopTags(12),
    ]);
    res.json({ releases, topTags });
  } catch (err) {
    console.error("fresh-releases error:", err);
    res.json({ releases: [], topTags: [] });
  }
});

const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3001;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Discogs search API listening on port ${PORT}`);
  if (process.env.APP_DB_URL) startFreshSyncSchedule();
});
