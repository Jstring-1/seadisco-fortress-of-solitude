import express from "express";
import compression from "compression";
import { fileURLToPath } from "url";
import path from "path";
import { DiscogsClient } from "./discogs-client.js";
import { initDb, getAllUsersForSync, getUserToken, setUserToken, deleteUserToken, deleteUserData, saveSearch, markSearchBio, getSearchHistory, deleteSearch, clearSearchHistory, deleteSearchGlobal, deleteSearchById, getRecentSearches, dumpSearchHistory, truncateSearchHistory, saveFeedback, getFeedback, deleteFeedback, getDiscogsUsername, setDiscogsUsername, getSyncStatus, updateSyncProgress, upsertCollectionItems, upsertWantlistItems, getCollectionPage, getWantlistPage, getCollectionIds, getWantlistIds, getCollectionFacets, getWantlistFacets, updateCollectionSyncedAt, updateWantlistSyncedAt, getFreshReleases, getFreshReleasesByTag, getFreshTopTags, recordInterestSignals, getInterestStats, backfillInterestSignals } from "./db.js";
import { startFreshSyncSchedule } from "./sync-fresh-releases.js";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const sharedToken = process.env.DISCOGS_TOKEN ?? "";
const anthropicKey = process.env.ANTHROPIC_API_KEY ?? "";
// Publishable key sent to frontend via /api/config
const authPk = process.env.AUTH_PK ?? "";
// Set REQUIRE_AUTH=true to require users to sign in and provide their own Discogs token
const requireAuth = process.env.REQUIRE_AUTH === "true";
// Shared Discogs client (used as fallback when user has no personal token)
const discogs = sharedToken ? new DiscogsClient(sharedToken) : null;
// ── IP rate limiter for unauthenticated (shared-token) searches ───────────
const UNAUTH_LIMIT = 5;
const LIMIT_WINDOW_MS = 24 * 60 * 60 * 1000; // 24 hours
const ipCounts = new Map();
// IPs that bypass the rate limit and auth requirement entirely
const IP_WHITELIST = new Set([
    "172.59.131.156",
]);
function clientIp(req) {
    const fwd = req.headers["x-forwarded-for"];
    return (fwd ? fwd.split(",")[0] : req.ip ?? "unknown").replace(/^::ffff:/, "").trim();
}
function checkRateLimit(ip) {
    if (IP_WHITELIST.has(ip))
        return { allowed: true, remaining: UNAUTH_LIMIT };
    const now = Date.now();
    const entry = ipCounts.get(ip);
    if (!entry || now > entry.resetAt) {
        ipCounts.set(ip, { count: 1, resetAt: now + LIMIT_WINDOW_MS });
        return { allowed: true, remaining: UNAUTH_LIMIT - 1 };
    }
    if (entry.count >= UNAUTH_LIMIT)
        return { allowed: false, remaining: 0 };
    entry.count++;
    return { allowed: true, remaining: UNAUTH_LIMIT - entry.count };
}
// Prune expired entries hourly to prevent memory growth
setInterval(() => {
    const now = Date.now();
    for (const [ip, entry] of ipCounts)
        if (now > entry.resetAt)
            ipCounts.delete(ip);
}, 60 * 60 * 1000);
// Decode Clerk session JWT from Authorization header (payload only — no pkg needed)
function getClerkUserId(req) {
    const auth = req.headers.authorization;
    if (!auth?.startsWith("Bearer "))
        return null;
    try {
        const b64 = auth.slice(7).split(".")[1];
        const { sub } = JSON.parse(Buffer.from(b64, "base64").toString());
        return sub ?? null;
    }
    catch {
        return null;
    }
}
// Resolve Discogs token: user token → (if auth not required) shared token → null
async function getTokenForRequest(req, allowFallback = false) {
    const userId = getClerkUserId(req);
    if (userId) {
        const userToken = await getUserToken(userId);
        if (userToken)
            return userToken;
    }
    // Fall back to shared token when auth is disabled OR when explicitly allowed (bio endpoints)
    if (!requireAuth || allowFallback)
        return sharedToken || null;
    return null;
}
async function getDiscogsForRequest(req, allowFallback = false) {
    const t = await getTokenForRequest(req, allowFallback);
    if (!t)
        return null;
    if (t === sharedToken && discogs)
        return discogs;
    return new DiscogsClient(t);
}
// Boot DB if a connection string is configured
if (process.env.APP_DB_URL) {
    initDb().catch(err => console.error("DB init failed:", err));
}
const app = express();
app.set("trust proxy", true); // respect X-Forwarded-For from Railway's proxy
// Gzip/brotli compression for all responses
app.use(compression());
// Cache headers for static assets (versioned files get long cache, HTML short)
app.use(express.static(path.join(__dirname, "../web"), {
    extensions: ["html"],
    setHeaders(res, filePath) {
        if (/\.(js|css|webp|png|ico|woff2?)$/i.test(filePath)) {
            res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
        }
        else if (/\.html$/i.test(filePath)) {
            res.setHeader("Cache-Control", "no-cache, must-revalidate");
        }
    },
}));
// Allow any webpage to call this API
app.use((_req, res, next) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (_req.method === "OPTIONS") {
        res.sendStatus(204);
        return;
    }
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
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const t = await getUserToken(userId);
    res.json({ hasToken: !!t, masked: t ? `****${t.slice(-4)}` : null });
});
// POST /api/user/token — save user's Discogs personal access token
app.post("/api/user/token", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const { token } = req.body ?? {};
    if (!token || typeof token !== "string" || token.trim().length < 8) {
        res.status(400).json({ error: "Invalid token" });
        return;
    }
    await setUserToken(userId, token.trim());
    // Fetch Discogs username from /oauth/identity using the user's token
    try {
        const identRes = await fetch("https://api.discogs.com/oauth/identity", {
            headers: { "Authorization": `Discogs token=${token.trim()}`, "User-Agent": "SeaDisco/1.0" }
        });
        if (identRes.ok) {
            const ident = await identRes.json();
            if (ident.username)
                await setDiscogsUsername(userId, ident.username);
        }
    }
    catch { }
    res.json({ ok: true });
});
// DELETE /api/user/token — remove user's saved token
app.delete("/api/user/token", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    await deleteUserToken(userId);
    res.json({ ok: true });
});
// DELETE /api/user/account — wipe all user data from our DB (Clerk deletion handled client-side)
app.delete("/api/user/account", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    await deleteUserData(userId);
    res.json({ ok: true });
});
// Background sync worker — runs detached from the HTTP request
async function runBackgroundSync(userId, token, username, syncCollection, syncWantlist) {
    const headers = { "Authorization": `Discogs token=${token}`, "User-Agent": "SeaDisco/1.0" };
    const delay = (ms) => new Promise(r => setTimeout(r, ms));
    let totalSynced = 0;
    try {
        // First, get total counts from Discogs to estimate total
        let estimatedTotal = 0;
        if (syncCollection) {
            try {
                const r = await fetch(`https://api.discogs.com/users/${encodeURIComponent(username)}/collection/folders/0/releases?per_page=1&page=1`, { headers });
                if (r.ok) {
                    const d = await r.json();
                    estimatedTotal += d.pagination?.items ?? 0;
                }
                await delay(500);
            }
            catch { }
        }
        if (syncWantlist) {
            try {
                const r = await fetch(`https://api.discogs.com/users/${encodeURIComponent(username)}/wants?per_page=1&page=1`, { headers });
                if (r.ok) {
                    const d = await r.json();
                    estimatedTotal += d.pagination?.items ?? 0;
                }
                await delay(500);
            }
            catch { }
        }
        await updateSyncProgress(userId, "syncing", 0, estimatedTotal);
        if (syncCollection) {
            for (let page = 1;; page++) {
                if (page > 1)
                    await delay(2000); // 2s pacing — leaves headroom for user searches
                const r = await fetch(`https://api.discogs.com/users/${encodeURIComponent(username)}/collection/folders/0/releases?per_page=100&page=${page}`, { headers });
                if (!r.ok)
                    break;
                const data = await r.json();
                const releases = data.releases ?? [];
                if (!releases.length)
                    break;
                const items = releases.map((item) => ({
                    id: item.basic_information?.id,
                    data: item.basic_information,
                    addedAt: item.date_added ? new Date(item.date_added) : undefined,
                })).filter(i => i.id);
                await upsertCollectionItems(userId, items);
                await recordInterestSignals(items, "collection");
                totalSynced += items.length;
                await updateSyncProgress(userId, "syncing", totalSynced, estimatedTotal);
                if (releases.length < 100)
                    break;
            }
            await updateCollectionSyncedAt(userId);
        }
        if (syncWantlist) {
            for (let page = 1;; page++) {
                if (page > 1)
                    await delay(2000);
                const r = await fetch(`https://api.discogs.com/users/${encodeURIComponent(username)}/wants?per_page=100&page=${page}`, { headers });
                if (!r.ok)
                    break;
                const data = await r.json();
                const wants = data.wants ?? [];
                if (!wants.length)
                    break;
                const items = wants.map((item) => ({
                    id: item.id,
                    data: item.basic_information,
                    addedAt: item.date_added ? new Date(item.date_added) : undefined,
                })).filter(i => i.id);
                await upsertWantlistItems(userId, items);
                await recordInterestSignals(items, "wantlist");
                totalSynced += items.length;
                await updateSyncProgress(userId, "syncing", totalSynced, estimatedTotal);
                if (wants.length < 100)
                    break;
            }
            await updateWantlistSyncedAt(userId);
        }
        await updateSyncProgress(userId, "complete", totalSynced, estimatedTotal);
        console.log(`Background sync complete for ${username}: ${totalSynced} items`);
    }
    catch (err) {
        console.error("Background sync error:", err);
        await updateSyncProgress(userId, "error", totalSynced, 0, String(err));
    }
}
// POST /api/user/sync — kick off background sync of collection and/or wantlist
app.post("/api/user/sync", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const { type = "both" } = req.body ?? {};
    const syncCollection = type === "collection" || type === "both";
    const syncWantlist = type === "wantlist" || type === "both";
    // Check cooldown: skip individual types if synced within last hour
    const syncStatus = await getSyncStatus(userId);
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    const collectionRecent = syncCollection && !!syncStatus.collectionSyncedAt && syncStatus.collectionSyncedAt > oneHourAgo;
    const wantlistRecent = syncWantlist && !!syncStatus.wantlistSyncedAt && syncStatus.wantlistSyncedAt > oneHourAgo;
    if (collectionRecent && wantlistRecent) {
        res.json({ ok: true, skipped: true, reason: "Recently synced" });
        return;
    }
    // If already syncing, don't start another
    if (syncStatus.syncStatus === "syncing") {
        res.json({ ok: true, syncing: true, progress: syncStatus.syncProgress, total: syncStatus.syncTotal });
        return;
    }
    const token = await getUserToken(userId);
    if (!token) {
        res.status(400).json({ error: "No Discogs token found" });
        return;
    }
    let username = await getDiscogsUsername(userId);
    if (!username) {
        try {
            const identRes = await fetch("https://api.discogs.com/oauth/identity", {
                headers: { "Authorization": `Discogs token=${token}`, "User-Agent": "SeaDisco/1.0" }
            });
            if (identRes.ok) {
                const ident = await identRes.json();
                if (ident.username) {
                    await setDiscogsUsername(userId, ident.username);
                    username = ident.username;
                }
            }
        }
        catch { }
    }
    if (!username) {
        res.status(400).json({ error: "Could not determine Discogs username" });
        return;
    }
    // Respond immediately, sync runs in background
    res.json({ ok: true, started: true });
    // Fire and forget — runs in background
    runBackgroundSync(userId, token, username, syncCollection && !collectionRecent, syncWantlist && !wantlistRecent).catch(err => {
        console.error("Background sync uncaught error:", err);
    });
});
// GET /api/user/collection — paginated cached collection (with optional filters)
app.get("/api/user/collection", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const page = parseInt(req.query.page) || 1;
    const perPage = parseInt(req.query.per_page) || 25;
    const filters = {};
    for (const key of ["q", "artist", "release", "label", "year", "genre", "style", "format"]) {
        const v = (req.query[key] ?? "").trim();
        if (v)
            filters[key] = v;
    }
    const { items, total } = await getCollectionPage(userId, page, perPage, Object.keys(filters).length ? filters : undefined);
    res.json({ items, total, page, pages: Math.ceil(total / perPage) });
});
// GET /api/user/wantlist — paginated cached wantlist (with optional filters)
app.get("/api/user/wantlist", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const page = parseInt(req.query.page) || 1;
    const perPage = parseInt(req.query.per_page) || 25;
    const filters = {};
    for (const key of ["q", "artist", "release", "label", "year", "genre", "style", "format"]) {
        const v = (req.query[key] ?? "").trim();
        if (v)
            filters[key] = v;
    }
    const { items, total } = await getWantlistPage(userId, page, perPage, Object.keys(filters).length ? filters : undefined);
    res.json({ items, total, page, pages: Math.ceil(total / perPage) });
});
// GET /api/user/facets — distinct genres and styles from collection or wantlist
app.get("/api/user/facets", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const type = req.query.type ?? "collection";
    const genre = req.query.genre || undefined;
    const facets = type === "wantlist" ? await getWantlistFacets(userId, genre) : await getCollectionFacets(userId, genre);
    res.json(facets);
});
// GET /api/user/discogs-ids — collection and wantlist IDs for badge rendering
app.get("/api/user/discogs-ids", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const [collectionIds, wantlistIds] = await Promise.all([
        getCollectionIds(userId),
        getWantlistIds(userId),
    ]);
    res.json({ collectionIds, wantlistIds });
});
// GET /api/user/sync-status — last sync timestamps + Discogs username
app.get("/api/user/sync-status", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const [syncStatus, username] = await Promise.all([
        getSyncStatus(userId),
        getDiscogsUsername(userId),
    ]);
    res.json({
        collectionSyncedAt: syncStatus.collectionSyncedAt,
        wantlistSyncedAt: syncStatus.wantlistSyncedAt,
        discogsUsername: username,
        syncStatus: syncStatus.syncStatus,
        syncProgress: syncStatus.syncProgress,
        syncTotal: syncStatus.syncTotal,
        syncError: syncStatus.syncError,
    });
});
function stripArtistSuffix(name) {
    return name ? name.replace(/\s*\(\d+\)$/, "").trim() : undefined;
}
// GET /api/user/history — recent searches for the logged-in user
app.get("/api/user/history", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const raw = await getSearchHistory(userId);
    const history = raw.map((s) => ({ ...s, params: normalizeParams(s.params) }));
    res.json({ history });
});
// DELETE /api/user/search — delete one saved search by params
app.delete("/api/user/search", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const { params } = req.body ?? {};
    if (!params) {
        res.status(400).json({ error: "Missing params" });
        return;
    }
    await deleteSearch(userId, params);
    res.json({ ok: true });
});
// DELETE /api/user/searches — clear all saved searches for the user
app.delete("/api/user/searches", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    await clearSearchHistory(userId);
    res.json({ ok: true });
});
// POST /api/feedback — save feedback from signed-in user
app.post("/api/feedback", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const { message, userEmail } = req.body;
    if (!message?.trim()) {
        res.status(400).json({ error: "Message required" });
        return;
    }
    await saveFeedback(userId, userEmail ?? "", message.trim());
    res.json({ ok: true });
});
// GET /api/admin/feedback — inbox, only for admin user
app.get("/api/admin/feedback", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const items = await getFeedback();
    res.json({ items });
});
// DELETE /api/admin/feedback/:id — delete a feedback item, admin only
app.delete("/api/admin/feedback/:id", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    await deleteFeedback(parseInt(req.params.id));
    res.json({ ok: true });
});
// GET /api/admin/searches — full search history, admin only
app.get("/api/admin/searches", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const raw = await dumpSearchHistory();
    const searches = raw.map(s => ({ ...s, params: normalizeParams(s.params) }));
    res.json({ searches });
});
// GET /api/admin/search-dump — full search history export, admin only
app.get("/api/admin/search-dump", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const rows = await dumpSearchHistory();
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Content-Disposition", "attachment; filename=search-history.json");
    res.json({ count: rows.length, searches: rows });
});
// DELETE /api/admin/search-all — wipe entire search history, admin only
app.delete("/api/admin/search-all", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const deleted = await truncateSearchHistory();
    res.json({ ok: true, deleted });
});
// DELETE /api/admin/search — delete a search by params across all users, admin only
app.delete("/api/admin/search", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const { params } = req.body ?? {};
    if (!params) {
        res.status(400).json({ error: "Missing params" });
        return;
    }
    await deleteSearchGlobal(params);
    res.json({ ok: true });
});
// DELETE /api/admin/search/:id — delete a single search row by ID, admin only
app.delete("/api/admin/search/:id", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const id = parseInt(req.params.id);
    if (!id) {
        res.status(400).json({ error: "Invalid id" });
        return;
    }
    await deleteSearchById(id);
    res.json({ ok: true });
});
// POST /api/admin/backfill-interests — one-time backfill from existing collection/wantlist data
app.post("/api/admin/backfill-interests", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    try {
        const counts = await backfillInterestSignals();
        res.json({ ok: true, ...counts });
    }
    catch (err) {
        console.error("Backfill error:", err);
        res.status(500).json({ error: "Backfill failed" });
    }
});
// POST /api/admin/sync-all — trigger background sync for all users with tokens, admin only
app.post("/api/admin/sync-all", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const users = await getAllUsersForSync();
    res.json({ ok: true, queued: users.length });
    // Run syncs sequentially so server load and Discogs API stay manageable
    (async () => {
        for (const user of users) {
            try {
                await runBackgroundSync(user.clerkUserId, user.token, user.username, true, true);
            }
            catch (err) {
                console.error(`Sync-all error for ${user.username}:`, err);
            }
        }
    })();
});
// GET /api/admin/interests — interest signal stats, admin only
app.get("/api/admin/interests", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const stats = await getInterestStats();
    res.json(stats);
});
// POST /api/ai-search — Claude music recommendations
app.post("/api/ai-search", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "no_token" });
        return;
    }
    if (!anthropicKey) {
        res.status(503).json({ error: "AI not configured" });
        return;
    }
    const q = (req.body.q ?? "").trim();
    if (!q) {
        res.status(400).json({ error: "Query required" });
        return;
    }
    const prompt = `You are a music expert specializing in vinyl records, rare and world music.
The user is searching for: "${q}"

Return a JSON object with two fields:
- blurb: a single sentence (max 30 words) that contextualizes these recommendations — what connects them, what era or sound they represent. Conversational, no filler phrases.
- items: an array of 12 music recommendations (artists or albums/releases) that best match this query.

Each item in the items array must include:
- name: artist name, or "Album Title by Artist"
- type: "artist" or "release"
- description: one compelling sentence explaining why this fits
- discogsParams: object with relevant Discogs search fields only (choose from: q, artist, label, genre, style, year). year must be a single 4-digit year, never a range.

Return ONLY a valid JSON object, no markdown, no explanation.`;
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
                max_tokens: 1700,
                messages: [{ role: "user", content: prompt }],
            }),
        });
        const data = await r.json();
        if (!r.ok) {
            console.error("Anthropic API error:", data);
            res.status(502).json({ error: `AI error: ${data.error?.message ?? r.status}` });
            return;
        }
        let text = (data.content?.[0]?.text ?? "{}").trim();
        // Strip markdown code fences if present
        text = text.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "");
        const parsed = JSON.parse(text);
        // Support both new { blurb, items } format and legacy bare array
        const recommendations = Array.isArray(parsed) ? parsed : (parsed.items ?? []);
        const blurb = Array.isArray(parsed) ? "" : (parsed.blurb ?? "");
        res.json({ recommendations, blurb });
    }
    catch (err) {
        console.error("AI search error:", err);
        res.status(500).json({ error: "AI search failed: " + err.message });
    }
});
// POST /api/result-quality — Claude gives a short phrase on result relevance
app.post("/api/result-quality", express.json(), async (req, res) => {
    if (!anthropicKey) {
        res.json({ phrase: null });
        return;
    }
    const { query, titles } = req.body ?? {};
    if (!query || !Array.isArray(titles) || !titles.length) {
        res.json({ phrase: null });
        return;
    }
    const titleList = titles.slice(0, 6).map((t, i) => `${i + 1}. ${t}`).join("\n");
    const prompt = `A user searched Discogs for: "${query}"

Top results returned:
${titleList}

In 4–7 words, give a single honest phrase describing how well these results match the query. Be direct, like a librarian — e.g. "Strong match", "Partial match, try narrowing", "Loose results, refine your search", "Exact artist found", "Mixed bag, add more filters". No punctuation at the end. Return ONLY the phrase, nothing else.`;
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
                max_tokens: 24,
                messages: [{ role: "user", content: prompt }],
            }),
        });
        const data = await r.json();
        if (!r.ok) {
            console.error("[result-quality] Anthropic API error:", JSON.stringify(data));
            res.json({ phrase: null });
            return;
        }
        const phrase = (data.content?.[0]?.text ?? "").trim().replace(/[.!?]+$/, "") || null;
        console.log("[result-quality] phrase:", phrase);
        res.json({ phrase });
    }
    catch (err) {
        console.error("[result-quality] fetch error:", err);
        res.json({ phrase: null });
    }
});
// POST /api/user/mb — mark most recent search as having a bio
app.post("/api/user/mb", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "not signed in" });
        return;
    }
    try {
        await markSearchBio(userId);
        res.json({ ok: true });
    }
    catch {
        res.status(500).json({ error: "failed" });
    }
});
// GET /api/recent-searches — anonymous global feed
// Normalize old full-name param keys to single-letter keys
function normalizeParams(p) {
    const keyMap = {
        artist: "a", release_title: "r", label: "l", year: "y",
        genre: "g", style: "s", format: "f", type: "t", sort: "o",
    };
    const out = {};
    for (const [k, v] of Object.entries(p)) {
        if (v)
            out[keyMap[k] ?? k] = v;
    }
    return out;
}
app.get("/api/recent-searches", async (_req, res) => {
    if (!process.env.APP_DB_URL) {
        res.json({ searches: [] });
        return;
    }
    try {
        const raw = await getRecentSearches(500);
        // Normalize all params to single-letter keys (handles old entries with full names)
        const normalized = raw.map(s => ({ ...s, params: normalizeParams(s.params) }));
        // Deduplicate by normalised content: lowercase q/artist/label/release/genre/style/year
        // Searches differing only in format, type, or sort are treated as the same
        const seen = new Set();
        const searches = normalized.filter(({ params }) => {
            const sig = [params.q, params.a, params.r, params.l, params.g, params.s, params.y]
                .map(v => (v ?? "").toLowerCase().trim())
                .join("|");
            if (!sig.replace(/\|/g, "").trim() || seen.has(sig))
                return false;
            seen.add(sig);
            return true;
        });
        // DB already randomises; shuffle again for extra variety, return 48
        for (let i = searches.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [searches[i], searches[j]] = [searches[j], searches[i]];
        }
        res.json({ searches: searches.slice(0, 48) });
    }
    catch {
        res.json({ searches: [] });
    }
});
// GET /search?q=pink+floyd&type=master&year=1973&page=1&per_page=10
app.get("/search", async (req, res) => {
    const rawQ = req.query.q ?? "";
    const artist = stripArtistSuffix(req.query.artist);
    const rawLabel = req.query.label ?? "";
    const rawRelease = req.query.release_title ?? "";
    // Each field is sent as its own dedicated Discogs param — no promotion to q
    const q = rawQ;
    const searchArtist = artist || undefined;
    const searchLabel = rawLabel || undefined;
    const searchRelease = rawRelease || undefined;
    const ip = clientIp(req);
    const whitelisted = IP_WHITELIST.has(ip);
    const userId = getClerkUserId(req);
    const userToken = userId ? await getUserToken(userId) : null;
    const usingSharedToken = !userToken;
    // Rate-limit unauthenticated users — allow 5 free searches/day via shared token
    if (usingSharedToken && !whitelisted) {
        if (!sharedToken) {
            res.status(401).json({ error: "no_token", message: "Sign in and add your Discogs API token to search." });
            return;
        }
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
    // allowFallback=true for unauthenticated/whitelisted users — they passed the rate limit check above
    const dc = await getDiscogsForRequest(req, usingSharedToken || whitelisted);
    if (!dc) {
        res.status(401).json({ error: "no_token", message: "Sign in and add your Discogs API token to search." });
        return;
    }
    try {
        const results = await dc.search(q, {
            type: req.query.type,
            artist: searchArtist,
            releaseTitle: searchRelease,
            label: searchLabel,
            year: req.query.year,
            genre: req.query.genre,
            style: req.query.style,
            sort: req.query.sort,
            sortOrder: req.query.sort_order,
            page: req.query.page ? parseInt(req.query.page) : 1,
            perPage: req.query.per_page ? parseInt(req.query.per_page) : 12,
        });
        res.json(results);
        // Record search history for logged-in users (fire-and-forget, page 1 only)
        const userId = getClerkUserId(req);
        const isFirstPage = !req.query.page || req.query.page === "1";
        if (userId && isFirstPage) {
            const artistParam = req.query.artist ? String(req.query.artist) : "";
            const p = {};
            // Single-letter keys: q a r l y g s f t o
            if (rawQ && rawQ.toLowerCase() !== artistParam.toLowerCase())
                p.q = rawQ;
            if (artistParam)
                p.a = artistParam;
            if (req.query.release_title)
                p.r = String(req.query.release_title);
            if (req.query.label)
                p.l = String(req.query.label);
            if (req.query.year)
                p.y = String(req.query.year);
            if (req.query.genre)
                p.g = String(req.query.genre);
            if (req.query.style)
                p.s = String(req.query.style);
            const fmt = req.query.format ? String(req.query.format) : "";
            if (fmt && fmt !== "Vinyl")
                p.f = fmt;
            if (req.query.type)
                p.t = String(req.query.type);
            if (req.query.sort) {
                const sortOrder = req.query.sort_order ? `:${String(req.query.sort_order)}` : "";
                p.o = `${String(req.query.sort)}${sortOrder}`;
            }
            const hasResults = results?.results?.length > 0;
            // Only save when there are meaningful search terms (not just type/sort/format)
            const hasMeaningful = p.q || p.a || p.r || p.l || p.g || p.s || p.y;
            if (hasMeaningful && hasResults)
                saveSearch(userId, p).catch(() => { });
        }
    }
    catch (err) {
        console.error(err);
        res.status(500).json({ error: "Discogs API error" });
    }
});
// GET /release/:id
app.get("/release/:id", async (req, res) => {
    const dc = await getDiscogsForRequest(req, true);
    if (!dc) {
        res.status(503).json({ error: "No Discogs token configured" });
        return;
    }
    try {
        const result = await dc.getRelease(req.params.id);
        res.json(result);
    }
    catch (err) {
        console.error(err);
        res.status(500).json({ error: "Discogs API error" });
    }
});
// GET /master/:id
app.get("/master/:id", async (req, res) => {
    const dc = await getDiscogsForRequest(req, true);
    if (!dc) {
        res.status(503).json({ error: "No Discogs token configured" });
        return;
    }
    try {
        const result = await dc.getMasterRelease(req.params.id);
        res.json(result);
    }
    catch (err) {
        console.error(err);
        res.status(500).json({ error: "Discogs API error" });
    }
});
// GET /artist/:id
app.get("/artist/:id", async (req, res) => {
    const dc = await getDiscogsForRequest(req, true);
    if (!dc) {
        res.status(503).json({ error: "No Discogs token configured" });
        return;
    }
    try {
        const result = await dc.getArtist(req.params.id);
        res.json(result);
    }
    catch (err) {
        console.error(err);
        res.status(500).json({ error: "Discogs API error" });
    }
});
const MB_UA = "DiscogsMCPSearch/1.0 ( search@sideman.pro )";
// GET /artist-bio?name=Miles+Davis[&id=123456] — Discogs bio
// If `id` is supplied the artist is fetched directly (no ambiguous name search).
app.get("/artist-bio", async (req, res) => {
    const nameRaw = req.query.name;
    const idParam = req.query.id ? parseInt(req.query.id, 10) : null;
    if (!nameRaw || !nameRaw.trim()) {
        res.status(400).json({ error: "Missing required query parameter: name" });
        return;
    }
    const dc = await getDiscogsForRequest(req, true);
    if (!dc) {
        res.json({ profile: null });
        return;
    }
    const mapNames = (arr) => (arr ?? []).filter(x => x?.name)
        .map(x => ({ name: x.name, active: x.active, id: x.id }));
    // ── Fast path: direct lookup by Discogs ID ──────────────────────────────
    if (idParam) {
        try {
            const artist = await dc.getArtist(idParam);
            let profile = artist?.profile ?? null;
            if (profile)
                profile = await resolveDiscogsIds(profile, dc);
            res.json({
                profile,
                name: artist?.name ?? nameRaw,
                alternatives: [],
                members: mapNames(artist?.members ?? []),
                groups: mapNames(artist?.groups ?? []),
                aliases: mapNames(artist?.aliases ?? []),
                namevariations: (artist?.namevariations ?? []).filter(Boolean),
                urls: (artist?.urls ?? []).filter(Boolean),
            });
        }
        catch (err) {
            console.error(err);
            res.json({ profile: null });
        }
        return;
    }
    // ── Slow path: name search + best-match heuristics ─────────────────────
    // Strip suffix for the Discogs search query, but keep original for exact matching
    const nameForSearch = nameRaw.replace(/\s*\(\d+\)$/, "").trim();
    const nameForMatch = nameRaw.trim();
    try {
        const discogsResults = await dc.search(nameForSearch, { type: "artist", perPage: 20 });
        const candidates = discogsResults?.results ?? [];
        const norm = (s) => s.toLowerCase().replace(/[^a-z0-9\s]/g, "").trim();
        // Match against the full name (including suffix) so "Snail Mail (2)" finds the right entry
        const searchNorm = norm(nameForMatch);
        let best = candidates.find(a => norm(a.title) === searchNorm)
            ?? candidates.find(a => {
                const an = norm(a.title);
                return an.startsWith(norm(nameForSearch)) || norm(nameForSearch).startsWith(an);
            })
            ?? candidates[0];
        if (!best?.id) {
            res.json({ profile: null, name: nameForMatch });
            return;
        }
        let artist = await dc.getArtist(best.id);
        let profile = artist?.profile ?? null;
        // If best match has no profile, check remaining candidates in parallel
        // but only accept a fallback whose name has word overlap with the search
        if (!profile && candidates.length > 1) {
            const sigWords = (s) => new Set(s.toLowerCase().replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter(w => w.length > 3));
            const searchWords = sigWords(nameForSearch);
            const nameMatches = (title) => [...sigWords(title)].some(w => searchWords.has(w));
            const rest = candidates.filter(c => c.id !== best.id && nameMatches(c.title ?? ""));
            const restArtists = await Promise.all(rest.map(c => dc.getArtist(c.id).catch(() => null)));
            const idx = restArtists.findIndex(a => a?.profile);
            if (idx >= 0) {
                artist = restArtists[idx];
                best = rest[idx];
                profile = artist.profile;
            }
        }
        if (profile)
            profile = await resolveDiscogsIds(profile, dc);
        const alternatives = candidates
            .filter(a => a.id !== best.id && a.title)
            .slice(0, 19)
            .map(a => ({ name: a.title, id: a.id }));
        res.json({
            profile,
            name: artist?.name ?? nameForMatch,
            discogsId: best.id ?? null,
            alternatives,
            members: mapNames(artist?.members ?? []),
            groups: mapNames(artist?.groups ?? []),
            aliases: mapNames(artist?.aliases ?? []),
            namevariations: (artist?.namevariations ?? []).filter(Boolean),
            urls: (artist?.urls ?? []).filter(Boolean),
        });
    }
    catch (err) {
        console.error(err);
        res.json({ profile: null });
    }
});
// Helper: resolve Discogs ID tags in a profile string
async function resolveDiscogsIds(profile, dc = discogs) {
    const idPattern = /\[([rmal])=?(\d+)\]/g;
    const matches = [];
    const seen = new Set();
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
                const r = await dc.getRelease(id);
                displayName = r?.title ?? "";
            }
            else if (type === "m") {
                const r = await dc.getMasterRelease(id);
                displayName = r?.title ?? "";
            }
            else if (type === "l") {
                const r = await dc.getLabel(id);
                displayName = r?.name ?? "";
            }
            else {
                const r = await dc.getArtist(id);
                // Wrap in [a=Name] so the frontend can render it as a clickable link
                displayName = r?.name ? `[a=${r.name}]` : "";
            }
            return { tag, displayName };
        }
        catch {
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
    const name = req.query.name;
    if (!name || !name.trim()) {
        res.status(400).json({ error: "Missing required query parameter: name" });
        return;
    }
    const dc = await getDiscogsForRequest(req, true);
    if (!dc) {
        res.json({ profile: null });
        return;
    }
    try {
        const results = await dc.search(name, { type: "label", perPage: 1 });
        const first = results?.results?.[0];
        if (!first?.id) {
            res.json({ profile: null, name });
            return;
        }
        const label = await dc.getLabel(first.id);
        let profile = label?.profile ?? null;
        if (profile)
            profile = await resolveDiscogsIds(profile, dc);
        res.json({
            profile,
            name: label?.name ?? name,
            urls: (label?.urls ?? []).filter(Boolean),
            parentLabel: label?.parent_label?.name
                ? { name: label.parent_label.name, id: label.parent_label.id }
                : null,
            sublabels: (label?.sublabels ?? []).filter((x) => x?.name)
                .map((x) => ({ name: x.name, id: x.id })),
        });
    }
    catch (err) {
        console.error(err);
        res.json({ profile: null });
    }
});
// GET /genre-info?genre=Jazz — returns a factual AI-generated genre description
app.get("/genre-info", async (req, res) => {
    const genre = req.query.genre;
    if (!genre || !genre.trim()) {
        res.status(400).json({ error: "Missing required query parameter: genre" });
        return;
    }
    if (!anthropicKey) {
        res.json({ profile: null });
        return;
    }
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
        const data = await response.json();
        const profile = data?.content?.[0]?.text?.trim() ?? null;
        res.json({ profile, name: genre });
    }
    catch (err) {
        console.error(err);
        res.json({ profile: null });
    }
});
// GET /marketplace-stats/:id?type=release|master
app.get("/marketplace-stats/:id", async (req, res) => {
    const { id } = req.params;
    const type = req.query.type ?? "release";
    const dc = await getDiscogsForRequest(req, true);
    const reqToken = await getTokenForRequest(req);
    if (!dc || !reqToken) {
        res.json({ numForSale: 0, lowestPrice: null });
        return;
    }
    try {
        let releaseId = id;
        if (type === "master") {
            const master = await dc.getMasterRelease(id);
            releaseId = String(master?.main_release ?? id);
        }
        const statsRes = await fetch(`https://api.discogs.com/marketplace/stats/${releaseId}?curr_abbr=USD`, { headers: { "Authorization": `Discogs token=${reqToken}`, "User-Agent": MB_UA } });
        const stats = await statsRes.json();
        res.json({
            numForSale: stats?.num_for_sale ?? 0,
            lowestPrice: stats?.lowest_price?.value ?? null,
            currency: stats?.lowest_price?.currency ?? "USD",
            releaseId,
        });
    }
    catch (err) {
        console.error(err);
        res.json({ numForSale: 0, lowestPrice: null });
    }
});
// GET /master-versions/:id — all pressings/versions of a master release
app.get("/master-versions/:id", async (req, res) => {
    const { id } = req.params;
    const reqToken = await getTokenForRequest(req);
    if (!reqToken) {
        res.json({ versions: [] });
        return;
    }
    try {
        const r = await fetch(`https://api.discogs.com/masters/${id}/versions?per_page=100&sort=released&sort_order=asc`, { headers: { "Authorization": `Discogs token=${reqToken}`, "User-Agent": MB_UA } });
        const data = await r.json();
        const versions = (data.versions ?? []).map((v) => ({
            id: v.id,
            title: v.title,
            label: v.label,
            catno: v.catno,
            country: v.country,
            year: v.released,
            format: v.format,
            url: v.resource_url ? `https://www.discogs.com/release/${v.id}` : null,
        }));
        res.json({ versions });
    }
    catch (err) {
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
            getFreshTopTags(24),
        ]);
        res.json({ releases, topTags });
    }
    catch (err) {
        console.error("fresh-releases error:", err);
        res.json({ releases: [], topTags: [] });
    }
});
const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3001;
app.listen(PORT, "0.0.0.0", () => {
    console.log(`Discogs search API listening on port ${PORT}`);
    if (process.env.APP_DB_URL)
        startFreshSyncSchedule();
});
