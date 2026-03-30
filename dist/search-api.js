import express from "express";
import compression from "compression";
import crypto from "crypto";
import { fileURLToPath } from "url";
import path from "path";
import { DiscogsClient } from "./discogs-client.js";
import { initDb, getAllUsersForSync, getAllUsersSyncStatus, getUserToken, setUserToken, deleteUserToken, deleteUserData, saveSearch, markSearchBio, getSearchHistory, deleteSearch, clearSearchHistory, deleteSearchGlobal, deleteSearchById, getRecentSearches, getRecentLiveSearches, dumpSearchHistory, truncateSearchHistory, saveFeedback, getFeedback, deleteFeedback, getDiscogsUsername, setDiscogsUsername, getSyncStatus, updateSyncProgress, upsertCollectionItems, upsertCollectionFolders, upsertWantlistItems, getCollectionPage, getWantlistPage, getAllCollectionItems, getAllWantlistItems, getCollectionIds, getWantlistIds, getCollectionFacets, getWantlistFacets, getCollectionFolderList, updateCollectionSyncedAt, updateWantlistSyncedAt, getFreshReleases, searchFreshReleases, getFreshStats, recordInterestSignals, getInterestStats, backfillInterestSignals, getWantedItems, getWantedSample, upsertGearListings, updateGearDetail, getGearNeedingDetail, getGearListings, markExpiredGearListings, getGearStats, logGearFetch, resetAllSyncingStatuses, upsertFeedArticle, getFeedArticles, pruneFeedArticles, pruneAllStaleData, upsertLiveEvents, getLiveEvents, pruneLiveEvents, getLocationByIp, upsertLocation, rebuildUserTasteProfile, getUserTasteProfile, getPersonalizedFreshReleases, getPersonalizedFeedArticles, upsertInventoryItems, updateInventorySyncedAt, upsertUserLists, getInventoryPage, getUserListsList, getExistingYouTubeUrls, logApiRequest, getApiRequestLog, getApiRequestStats, getUserCollectionStats, getCachedRelease, cacheRelease } from "./db.js";
import { startFreshSyncSchedule } from "./sync-fresh-releases.js";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const sharedToken = process.env.DISCOGS_TOKEN ?? "";
const anthropicKey = process.env.ANTHROPIC_API_KEY ?? "";
// Publishable key sent to frontend via /api/config
const authPk = process.env.AUTH_PK ?? "";
// Set REQUIRE_AUTH=true to require users to sign in and provide their own Discogs token
const requireAuth = process.env.REQUIRE_AUTH === "true";
// Concert API keys
const ticketmasterKey = process.env.TICKETMASTER_API_KEY ?? "";
const bandsintownAppId = "seadisco"; // Bandsintown just needs an app identifier
// YouTube API key
const youtubeApiKey = process.env.YOUTUBE_API_KEY ?? "";
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
// ── Global API kill switch ──────────────────────────────────────────────
let _apiKillSwitch = false;
// ── Logged fetch: wraps fetch() and logs the request to api_request_log ──
async function loggedFetch(service, url, init) {
    if (_apiKillSwitch) {
        const cleanUrl = url.replace(/token=[^&]+/g, "token=***").replace(/key=[^&]+/g, "key=***").replace(/apikey=[^&]+/g, "apikey=***");
        logApiRequest({ service, endpoint: cleanUrl, method: init?.method ?? "GET", statusCode: 0, success: false, durationMs: 0, errorMessage: "BLOCKED — API kill switch active", context: init?.context }).catch(() => { });
        throw new Error("API kill switch is active — all outgoing requests blocked");
    }
    const start = Date.now();
    const method = init?.method ?? "GET";
    const context = init?.context;
    // Strip context from init before passing to real fetch
    const { context: _ctx, ...fetchInit } = (init ?? {});
    // Strip query params with tokens for safety
    const cleanUrl = url.replace(/token=[^&]+/g, "token=***").replace(/key=[^&]+/g, "key=***").replace(/apikey=[^&]+/g, "apikey=***");
    try {
        const r = await fetch(url, fetchInit);
        const ms = Date.now() - start;
        // Fire-and-forget log
        logApiRequest({ service, endpoint: cleanUrl, method, statusCode: r.status, success: r.ok, durationMs: ms, errorMessage: r.ok ? undefined : `HTTP ${r.status}`, context }).catch(() => { });
        return r;
    }
    catch (err) {
        const ms = Date.now() - start;
        logApiRequest({ service, endpoint: cleanUrl, method, statusCode: 0, success: false, durationMs: ms, errorMessage: err?.message ?? String(err), context }).catch(() => { });
        throw err;
    }
}
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
// ── IP geolocation resolver (3-layer cache: memory → DB → ip-api.com) ───
const locationCache = new Map();
async function resolveLocation(req) {
    const ip = clientIp(req);
    if (ip === "unknown" || ip === "127.0.0.1" || ip === "::1")
        return null;
    // 1. In-memory cache (1 hour)
    const cached = locationCache.get(ip);
    if (cached && Date.now() - cached.ts < 3600_000) {
        return { lat: cached.lat, lon: cached.lon, city: cached.city, region: cached.region, country: cached.country };
    }
    // 2. DB cache (7 days)
    try {
        const dbRow = await getLocationByIp(ip);
        if (dbRow) {
            const loc = { lat: dbRow.latitude, lon: dbRow.longitude, city: dbRow.city, region: dbRow.region, country: dbRow.country };
            locationCache.set(ip, { ...loc, ts: Date.now() });
            return loc;
        }
    }
    catch { }
    // 3. ip-api.com (free tier, HTTP only, 45 req/min)
    try {
        const r = await loggedFetch("ip-api", `http://ip-api.com/json/${ip}?fields=status,lat,lon,city,regionName,country`, { signal: AbortSignal.timeout(5000) });
        const data = await r.json();
        if (data.status !== "success")
            return null;
        const loc = { lat: data.lat, lon: data.lon, city: data.city, region: data.regionName, country: data.country };
        locationCache.set(ip, { ...loc, ts: Date.now() });
        // Persist to DB (fire-and-forget)
        const userId = getClerkUserId(req);
        upsertLocation(ip, userId, loc.lat, loc.lon, loc.city, loc.region, loc.country).catch(() => { });
        return loc;
    }
    catch {
        return null;
    }
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
    res.setHeader("Cache-Control", "public, max-age=600"); // 10 min
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
        const identRes = await loggedFetch("discogs", "https://api.discogs.com/oauth/identity", {
            headers: { "Authorization": `Discogs token=${token.trim()}`, "User-Agent": "SeaDisco/1.0" },
            context: "save-token identity check",
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
// Abort flag for stopping all syncs
let _syncAbort = false;
const SYNC_STALL_TIMEOUT = 5 * 60 * 1000; // 5 minutes with no progress = stalled
// Background sync worker — runs detached from the HTTP request
async function runBackgroundSync(userId, token, username, syncCollection, syncWantlist) {
    console.log(`Sync ${username}: starting full sync (collection=${syncCollection}, wantlist=${syncWantlist})`);
    const headers = { "Authorization": `Discogs token=${token}`, "User-Agent": "SeaDisco/1.0" };
    const delay = (ms) => new Promise(r => setTimeout(r, ms));
    let totalSynced = 0;
    let lastProgressAt = Date.now(); // tracks last time progress was made
    // Timeout guard — checks every 60s if sync has stalled
    let _syncDone = false;
    let _thisSyncAbort = false; // per-sync abort flag (set by stall guard)
    const stallGuard = setInterval(async () => {
        if (_syncDone) {
            clearInterval(stallGuard);
            return;
        }
        const stalledFor = Date.now() - lastProgressAt;
        if (stalledFor >= SYNC_STALL_TIMEOUT) {
            console.error(`Sync ${username}: STALLED — no progress for ${Math.round(stalledFor / 60000)}min, auto-aborting`);
            _thisSyncAbort = true;
            clearInterval(stallGuard);
            try {
                await updateSyncProgress(userId, "error", totalSynced, 0, `Stalled — no progress for ${Math.round(stalledFor / 60000)} minutes`);
            }
            catch { }
        }
    }, 60_000);
    // Fetch with retry — up to 5 attempts with exponential backoff, respects Discogs rate limit headers
    async function fetchWithRetry(url, retries = 5) {
        const backoffs = [15000, 30000, 60000, 90000, 120000];
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                const r = await loggedFetch("discogs", url, { headers, signal: AbortSignal.timeout(30000), context: `sync ${username}` });
                if (r.ok) {
                    // Check remaining rate limit — if low, pause proactively
                    const remaining = parseInt(r.headers.get("x-discogs-ratelimit-remaining") ?? "10");
                    if (remaining <= 1) {
                        console.log(`Sync ${username}: rate limit nearly exhausted (${remaining} left), pausing 30s`);
                        await delay(30000);
                    }
                    else if (remaining <= 5) {
                        await delay(3000);
                    }
                    return r;
                }
                if (r.status === 429 || r.status >= 500) {
                    const waitMs = backoffs[Math.min(attempt - 1, backoffs.length - 1)];
                    console.warn(`Sync ${username}: HTTP ${r.status} on attempt ${attempt}/${retries}, retrying in ${waitMs / 1000}s`);
                    if (attempt < retries)
                        await delay(waitMs);
                    else
                        throw new Error(`HTTP ${r.status} after ${retries} attempts`);
                }
                else {
                    throw new Error(`HTTP ${r.status}`); // 4xx non-retryable
                }
            }
            catch (err) {
                if (attempt === retries)
                    throw err;
                const waitMs = backoffs[Math.min(attempt - 1, backoffs.length - 1)];
                console.warn(`Sync ${username}: fetch error attempt ${attempt}/${retries}:`, err);
                await delay(waitMs);
            }
        }
        throw new Error("fetchWithRetry exhausted");
    }
    try {
        // First, get total counts from Discogs to estimate total
        let estimatedTotal = 0;
        if (syncCollection) {
            try {
                const r = await fetchWithRetry(`https://api.discogs.com/users/${encodeURIComponent(username)}/collection/folders/0/releases?per_page=1&page=1`);
                const d = await r.json();
                estimatedTotal += d.pagination?.items ?? 0;
                lastProgressAt = Date.now();
                await delay(500);
            }
            catch { }
        }
        if (syncWantlist) {
            try {
                const r = await fetchWithRetry(`https://api.discogs.com/users/${encodeURIComponent(username)}/wants?per_page=1&page=1`);
                const d = await r.json();
                estimatedTotal += d.pagination?.items ?? 0;
                lastProgressAt = Date.now();
                await delay(500);
            }
            catch { }
        }
        console.log(`Sync ${username}: estimated total = ${estimatedTotal}`);
        await updateSyncProgress(userId, "syncing", 0, estimatedTotal);
        if (syncCollection) {
            for (let page = 1;; page++) {
                if (_syncAbort || _thisSyncAbort) {
                    console.log(`Sync ${username}: aborted`);
                    await updateSyncProgress(userId, "error", totalSynced, 0, "Aborted");
                    _syncDone = true;
                    return;
                }
                if (page > 1)
                    await delay(1200); // 1.2s pacing — Discogs allows 60/min
                const r = await fetchWithRetry(`https://api.discogs.com/users/${encodeURIComponent(username)}/collection/folders/0/releases?per_page=500&page=${page}&sort=added&sort_order=desc`);
                const data = await r.json();
                const releases = data.releases ?? [];
                if (!releases.length)
                    break;
                const items = releases.map((item) => ({
                    id: item.basic_information?.id,
                    data: item.basic_information,
                    addedAt: item.date_added ? new Date(item.date_added) : undefined,
                    folderId: item.folder_id ?? 0,
                    rating: item.rating ?? 0,
                    instanceId: item.instance_id ?? undefined,
                    notes: item.notes ?? undefined,
                })).filter(i => i.id);
                await upsertCollectionItems(userId, items);
                await recordInterestSignals(items, "collection");
                totalSynced += items.length;
                lastProgressAt = Date.now();
                await updateSyncProgress(userId, "syncing", totalSynced, estimatedTotal);
                if (releases.length < 500)
                    break;
            }
            await updateCollectionSyncedAt(userId);
            // Sync folder list
            try {
                await delay(1000);
                const fr = await fetchWithRetry(`https://api.discogs.com/users/${encodeURIComponent(username)}/collection/folders`);
                const fd = await fr.json();
                const folders = (fd.folders ?? [])
                    .filter((f) => f.id !== 0) // skip the virtual "All" folder
                    .map((f) => ({ id: f.id, name: f.name, count: f.count }));
                if (folders.length)
                    await upsertCollectionFolders(userId, folders);
                lastProgressAt = Date.now();
            }
            catch { /* folder sync optional */ }
        }
        if (syncWantlist) {
            for (let page = 1;; page++) {
                if (_syncAbort || _thisSyncAbort) {
                    console.log(`Sync ${username}: aborted`);
                    await updateSyncProgress(userId, "error", totalSynced, 0, "Aborted");
                    _syncDone = true;
                    return;
                }
                if (page > 1)
                    await delay(1200);
                const r = await fetchWithRetry(`https://api.discogs.com/users/${encodeURIComponent(username)}/wants?per_page=500&page=${page}&sort=added&sort_order=desc`);
                const data = await r.json();
                const wants = data.wants ?? [];
                if (!wants.length)
                    break;
                const items = wants.map((item) => ({
                    id: item.id,
                    data: item.basic_information,
                    addedAt: item.date_added ? new Date(item.date_added) : undefined,
                    rating: item.rating ?? 0,
                    notes: item.notes ?? undefined,
                })).filter(i => i.id);
                await upsertWantlistItems(userId, items);
                await recordInterestSignals(items, "wantlist");
                totalSynced += items.length;
                lastProgressAt = Date.now();
                await updateSyncProgress(userId, "syncing", totalSynced, estimatedTotal);
                if (wants.length < 500)
                    break;
            }
            await updateWantlistSyncedAt(userId);
        }
        await updateSyncProgress(userId, "complete", totalSynced, estimatedTotal);
        console.log(`Full sync complete for ${username}: ${totalSynced} items`);
        // Rebuild taste profile after successful sync
        await rebuildUserTasteProfile(userId).catch(err => console.error(`Taste profile rebuild error for ${userId}:`, err));
    }
    catch (err) {
        console.error(`Background sync error for ${username}:`, err);
        await updateSyncProgress(userId, "error", totalSynced, 0, String(err));
    }
    finally {
        _syncDone = true;
        clearInterval(stallGuard);
    }
}
// POST /api/user/sync — kick off background sync of collection, wantlist, inventory & lists
app.post("/api/user/sync", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const { type = "both" } = req.body ?? {};
    const syncCollection = type === "collection" || type === "both";
    const syncWantlist = type === "wantlist" || type === "both";
    // Check cooldown: skip if synced within last 5 minutes
    const syncStatus = await getSyncStatus(userId);
    const cooldownAgo = new Date(Date.now() - 5 * 60 * 1000);
    const collectionRecent = syncCollection && !!syncStatus.collectionSyncedAt && syncStatus.collectionSyncedAt > cooldownAgo;
    const wantlistRecent = syncWantlist && !!syncStatus.wantlistSyncedAt && syncStatus.wantlistSyncedAt > cooldownAgo;
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
            const identRes = await loggedFetch("discogs", "https://api.discogs.com/oauth/identity", {
                headers: { "Authorization": `Discogs token=${token}`, "User-Agent": "SeaDisco/1.0" },
                context: "sync identity check",
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
    // Fire and forget — runs in background (full sync for user-initiated)
    (async () => {
        try {
            await runBackgroundSync(userId, token, username, syncCollection && !collectionRecent, syncWantlist && !wantlistRecent);
            // Also sync inventory & lists after collection/wantlist
            await syncUserExtras(userId, username, token);
            console.log(`[user-sync] ${username}: extras sync complete`);
        }
        catch (err) {
            console.error("Background sync uncaught error:", err);
        }
    })();
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
    const folderId = parseInt(req.query.folderId ?? "", 10);
    if (folderId > 0)
        filters.folderId = folderId;
    const ratingParam = (req.query.rating ?? "").trim();
    if (ratingParam === "unrated")
        filters.ratingUnrated = true;
    else if (ratingParam) {
        const rm = parseInt(ratingParam, 10);
        if (rm >= 1 && rm <= 5)
            filters.ratingMin = rm;
    }
    const notesParam = (req.query.notes ?? "").trim();
    if (notesParam)
        filters.notes = notesParam;
    const sort = (req.query.sort ?? "").trim();
    if (sort)
        filters.sort = sort;
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
    const ratingParam = (req.query.rating ?? "").trim();
    if (ratingParam === "unrated")
        filters.ratingUnrated = true;
    else if (ratingParam) {
        const rm = parseInt(ratingParam, 10);
        if (rm >= 1 && rm <= 5)
            filters.ratingMin = rm;
    }
    const notesParam = (req.query.notes ?? "").trim();
    if (notesParam)
        filters.notes = notesParam;
    const sort = (req.query.sort ?? "").trim();
    if (sort)
        filters.sort = sort;
    const { items, total } = await getWantlistPage(userId, page, perPage, Object.keys(filters).length ? filters : undefined);
    res.json({ items, total, page, pages: Math.ceil(total / perPage) });
});
// GET /api/user/inventory — paginated inventory listings
app.get("/api/user/inventory", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const page = parseInt(req.query.page) || 1;
    const perPage = parseInt(req.query.per_page) || 24;
    const filters = {};
    const q = (req.query.q ?? "").trim();
    if (q)
        filters.q = q;
    const { items, total } = await getInventoryPage(userId, page, perPage, Object.keys(filters).length ? filters : undefined);
    res.json({ items, total, page, pages: Math.ceil(total / perPage) });
});
// GET /api/user/lists — user's Discogs lists
app.get("/api/user/lists", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const lists = await getUserListsList(userId);
    res.json({ lists });
});
// GET /api/live/upcoming — serve upcoming events from DB
app.get("/api/live/upcoming", async (_req, res) => {
    try {
        res.setHeader("Cache-Control", "no-store");
        const events = await getLiveEvents(50);
        res.json({ events });
    }
    catch {
        res.json({ events: [] });
    }
});
// Helper: extract full event data from a Ticketmaster event object
function mapTmEvent(ev) {
    const venue = ev._embedded?.venues?.[0];
    const price = ev.priceRanges?.[0];
    const venueUrl = venue?.externalLinks?.homepage?.[0]?.url
        ?? venue?.url ?? "";
    const segment = ev.classifications?.[0]?.segment?.name?.toLowerCase() ?? "";
    return {
        name: ev.name ?? "",
        artist: ev._embedded?.attractions?.[0]?.name ?? "",
        date: ev.dates?.start?.localDate ?? "",
        time: ev.dates?.start?.localTime ?? "",
        venue: venue?.name ?? "",
        venueId: venue?.id ?? "",
        venueUrl,
        city: venue?.city?.name ?? "",
        region: venue?.state?.name ?? venue?.state?.stateCode ?? "",
        country: venue?.country?.countryCode ?? "",
        url: ev.url ?? "",
        imageUrl: ev.images?.find((i) => i.ratio === "16_9" && i.width >= 500)?.url
            ?? ev.images?.[0]?.url ?? "",
        priceMin: price?.min ?? undefined,
        priceMax: price?.max ?? undefined,
        currency: price?.currency ?? undefined,
        status: ev.dates?.status?.code ?? "",
        segment,
        source: "ticketmaster",
    };
}
/** Filter out non-music events (comedy, sports, etc.) that leak through TM's classificationName filter */
function isMusicEvent(ev) {
    const seg = ev.segment ?? "";
    return !seg || seg === "music";
}
// GET /api/live/nearby — geo-targeted events based on user IP
app.get("/api/live/nearby", async (req, res) => {
    // Allow client to pass cached lat/lon to skip IP lookup
    let lat = parseFloat(req.query.lat);
    let lon = parseFloat(req.query.lon);
    let city = req.query.city || "";
    let region = req.query.region || "";
    if (isNaN(lat) || isNaN(lon)) {
        const loc = await resolveLocation(req);
        if (!loc) {
            res.json({ events: [], location: null });
            return;
        }
        lat = loc.lat;
        lon = loc.lon;
        city = loc.city;
        region = loc.region;
    }
    if (!ticketmasterKey) {
        res.json({ events: [], location: { lat, lon, city, region } });
        return;
    }
    // If logged in, add genre keyword from taste profile for more relevant results
    const userId = getClerkUserId(req);
    let genreKeyword = "";
    if (userId) {
        const profile = await getUserTasteProfile(userId).catch(() => null);
        if (profile?.genre_keywords?.length) {
            // Use top genre as keyword hint (Ticketmaster supports this)
            genreKeyword = profile.genre_keywords[0];
        }
    }
    try {
        const params = new URLSearchParams({
            latlong: `${lat},${lon}`,
            radius: "50",
            unit: "miles",
            classificationName: "music",
            size: "50",
            sort: "date,asc",
            apikey: ticketmasterKey,
        });
        if (genreKeyword)
            params.set("keyword", genreKeyword);
        const tmRes = await loggedFetch("ticketmaster", `https://app.ticketmaster.com/discovery/v2/events.json?${params}`, { signal: AbortSignal.timeout(10000), context: "personalized events" });
        if (!tmRes.ok) {
            res.json({ events: [], location: { lat, lon, city, region } });
            return;
        }
        const tmData = await tmRes.json();
        const events = (tmData._embedded?.events ?? []).map(mapTmEvent).filter(isMusicEvent);
        res.setHeader("Cache-Control", "no-store");
        res.json({ events, location: { lat, lon, city, region } });
    }
    catch {
        res.json({ events: [], location: { lat, lon, city, region } });
    }
});
// Background: fetch upcoming events from Ticketmaster and store in DB
async function fetchUpcomingEvents() {
    if (!ticketmasterKey)
        return 0;
    try {
        const url = `https://app.ticketmaster.com/discovery/v2/events.json?apikey=${ticketmasterKey}&classificationName=music&size=50&sort=date,asc&countryCode=US`;
        const r = await loggedFetch("ticketmaster", url, { signal: AbortSignal.timeout(30000), context: "scheduled fetch" });
        if (!r.ok)
            return 0;
        const data = await r.json();
        const events = (data._embedded?.events ?? []).map(mapTmEvent).filter(isMusicEvent);
        const count = await upsertLiveEvents(events);
        await pruneLiveEvents();
        console.log(`[live-events] Fetched ${count} upcoming events, pruned past events`);
        return count;
    }
    catch (e) {
        console.error("[live-events] Fetch error:", e);
        return 0;
    }
}
function startLiveEventsSchedule() {
    // Every 6 hours starting at 3:40 AM Pacific
    const ms = msUntilPacific(3, 40, 6);
    console.log(`[live-events] Next fetch in ${Math.round(ms / 60000)}min, then every 6h`);
    setTimeout(() => {
        fetchUpcomingEvents();
        setInterval(() => fetchUpcomingEvents(), 6 * 60 * 60 * 1000);
    }, ms);
}
// GET /api/wanted-sample — small public sample for Find page filler
app.get("/api/wanted-sample", async (req, res) => {
    try {
        res.setHeader("Cache-Control", "public, max-age=60");
        const excludeStr = req.query.exclude || "";
        const excludeIds = excludeStr ? excludeStr.split(",").map(Number).filter(n => !isNaN(n)) : [];
        const items = await getWantedSample(16, excludeIds);
        res.json({ items });
    }
    catch (e) {
        res.status(500).json({ error: String(e) });
    }
});
// GET /api/wanted — all community wantlist items, deduped and shuffled (requires login)
app.get("/api/wanted", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    try {
        res.setHeader("Cache-Control", "private, max-age=300"); // 5 min, auth-gated
        const items = await getWantedItems();
        res.json({ items });
    }
    catch (e) {
        res.status(500).json({ error: String(e) });
    }
});
// GET /api/user/collection/export — download collection as CSV
app.get("/api/user/collection/export", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    try {
        const [rows, folders] = await Promise.all([
            getAllCollectionItems(userId),
            getCollectionFolderList(userId),
        ]);
        const folderMap = new Map(folders.map(f => [f.folderId, f.name]));
        const csv = buildCsv(rows, folderMap);
        res.setHeader("Content-Type", "text/csv; charset=utf-8");
        res.setHeader("Content-Disposition", "attachment; filename=seadisco-collection.csv");
        res.send(csv);
    }
    catch (e) {
        res.status(500).json({ error: "Export failed" });
    }
});
// GET /api/user/wantlist/export — download wantlist as CSV
app.get("/api/user/wantlist/export", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    try {
        const rows = await getAllWantlistItems(userId);
        const csv = buildCsv(rows, null);
        res.setHeader("Content-Type", "text/csv; charset=utf-8");
        res.setHeader("Content-Disposition", "attachment; filename=seadisco-wantlist.csv");
        res.send(csv);
    }
    catch (e) {
        res.status(500).json({ error: "Export failed" });
    }
});
function buildCsv(rows, folderMap) {
    const headers = ["Artist", "Title", "Label", "Cat#", "Year", "Format", "Genre", "Style", "Country"];
    if (folderMap)
        headers.push("Folder");
    const escCsv = (s) => {
        if (!s)
            return "";
        if (s.includes('"') || s.includes(",") || s.includes("\n"))
            return '"' + s.replace(/"/g, '""') + '"';
        return s;
    };
    const lines = [headers.join(",")];
    for (const row of rows) {
        const d = row.data;
        const artist = (d.artists ?? []).map((a) => a.name).join(", ");
        const title = d.title ?? "";
        const label = (d.labels ?? []).map((l) => l.name).join(", ");
        const catno = (d.labels ?? [])[0]?.catno ?? "";
        const year = String(d.year ?? "");
        const format = (d.formats ?? []).map((f) => [f.name, ...(f.descriptions ?? [])].filter(Boolean).join(" ")).join("; ");
        const genre = (d.genres ?? []).join(", ");
        const style = (d.styles ?? []).join(", ");
        const country = d.country ?? "";
        const cols = [artist, title, label, catno, year, format, genre, style, country];
        if (folderMap) {
            const fid = row.folder_id ?? 0;
            cols.push(folderMap.get(fid) ?? (fid === 0 ? "Uncategorized" : String(fid)));
        }
        lines.push(cols.map(escCsv).join(","));
    }
    return lines.join("\n");
}
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
// GET /api/user/folders — collection folder list
app.get("/api/user/folders", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Unauthorized" });
        return;
    }
    const folders = await getCollectionFolderList(userId);
    res.json({ folders });
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
    const searches = raw.map(s => ({
        ...s,
        params: s.params?._type === "live" ? s.params : normalizeParams(s.params),
    }));
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
// GET /api/admin/sync-status — per-user sync status + fresh releases stats, admin only
app.get("/api/admin/sync-status", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const [users, freshStats] = await Promise.all([getAllUsersSyncStatus(), getFreshStats()]);
    res.json({ users, freshStats });
});
// POST /api/admin/sync-all — trigger FULL background sync for all users, admin only
app.post("/api/admin/sync-all", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    _syncAbort = false; // clear any previous abort
    const users = await getAllUsersForSync();
    res.json({ ok: true, queued: users.length, mode: "full" });
    // Run syncs sequentially so server load and Discogs API stay manageable
    (async () => {
        for (const user of users) {
            if (_syncAbort) {
                console.log("Sync-all: aborted, skipping remaining users");
                break;
            }
            try {
                await runBackgroundSync(user.clerkUserId, user.token, user.username, true, true);
            }
            catch (err) {
                console.error(`Sync-all error for ${user.username}:`, err);
            }
        }
    })();
});
// POST /api/admin/sync-user — trigger background sync for a single user, admin only
app.post("/api/admin/sync-user", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const { username } = req.body;
    if (!username) {
        res.status(400).json({ error: "username required" });
        return;
    }
    const users = await getAllUsersForSync();
    const user = users.find(u => u.username === username);
    if (!user) {
        res.status(404).json({ error: "User not found" });
        return;
    }
    _syncAbort = false;
    res.json({ ok: true, username, mode: "full" });
    (async () => {
        try {
            await runBackgroundSync(user.clerkUserId, user.token, user.username, true, true);
        }
        catch (err) {
            console.error(`Sync-user error for ${user.username}:`, err);
        }
    })();
});
// POST /api/admin/sync-stop — abort all running syncs and reset statuses
app.post("/api/admin/sync-stop", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    _syncAbort = true;
    const count = await resetAllSyncingStatuses();
    console.log(`Admin: sync abort requested, ${count} syncing statuses reset`);
    res.json({ ok: true, message: `All syncs stopped — ${count} reset.` });
});
// POST /api/admin/api-kill — toggle global API kill switch
app.post("/api/admin/api-kill", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const { enabled } = req.body ?? {};
    _apiKillSwitch = enabled !== undefined ? !!enabled : !_apiKillSwitch;
    console.log(`Admin: API kill switch ${_apiKillSwitch ? "ENABLED — all outgoing requests blocked" : "DISABLED — requests flowing"}`);
    res.json({ ok: true, killSwitch: _apiKillSwitch });
});
// GET /api/admin/api-kill — check kill switch status
app.get("/api/admin/api-kill", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    res.json({ killSwitch: _apiKillSwitch });
});
// GET /api/admin/collection-stats — per-user and global collection/wantlist stats
app.get("/api/admin/collection-stats", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    try {
        const stats = await getUserCollectionStats();
        res.json(stats);
    }
    catch (err) {
        res.status(500).json({ error: String(err) });
    }
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
        const r = await loggedFetch("anthropic", "https://api.anthropic.com/v1/messages", {
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
            context: "ai-search suggestions",
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
        const r = await loggedFetch("anthropic", "https://api.anthropic.com/v1/messages", {
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
            context: "result-quality rating",
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
// Save live (concert) searches
app.post("/api/user/live-search", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    // Allow anonymous saves too — use "anon" as placeholder
    const uid = userId || "anon";
    const params = req.body?.params;
    if (!params || typeof params !== "object") {
        res.json({ ok: true });
        return;
    }
    try {
        // Tag as live search so we can distinguish from Discogs searches
        await saveSearch(uid, { ...params, _type: "live" });
        res.json({ ok: true });
    }
    catch {
        res.json({ ok: true });
    }
});
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
    res.setHeader("Cache-Control", "public, max-age=120"); // 2 min
    if (!process.env.APP_DB_URL) {
        res.json({ searches: [] });
        return;
    }
    try {
        const raw = await getRecentSearches(300);
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
        // Shuffle the latest 300 and return 48 random pills
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
// GET /api/recent-live-searches — recent concert searches for Live tab pill cloud
app.get("/api/recent-live-searches", async (_req, res) => {
    res.setHeader("Cache-Control", "public, max-age=120");
    if (!process.env.APP_DB_URL) {
        res.json({ searches: [] });
        return;
    }
    try {
        const raw = await getRecentLiveSearches(200);
        // Dedupe by content signature
        const seen = new Set();
        const searches = raw.filter(({ params }) => {
            const sig = [params.artist, params.city, params.genre]
                .map(v => (v ?? "").toLowerCase().trim())
                .join("|");
            if (!sig.replace(/\|/g, "").trim() || seen.has(sig))
                return false;
            seen.add(sig);
            return true;
        });
        // Shuffle and return 48
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
        // Record search history (fire-and-forget, page 1 only)
        const userId = getClerkUserId(req) || "anon";
        const isFirstPage = !req.query.page || req.query.page === "1";
        if (isFirstPage) {
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
    const id = parseInt(req.params.id, 10);
    const dc = await getDiscogsForRequest(req, true);
    if (!dc) {
        res.status(503).json({ error: "No Discogs token configured" });
        return;
    }
    try {
        const result = await dc.getRelease(req.params.id);
        // Always save fresh data to cache
        cacheRelease(id, "release", result).catch(() => { });
        res.json(result);
    }
    catch (err) {
        console.error(err);
        res.status(500).json({ error: "Discogs API error" });
    }
});
// GET /master/:id
app.get("/master/:id", async (req, res) => {
    const id = parseInt(req.params.id, 10);
    const dc = await getDiscogsForRequest(req, true);
    if (!dc) {
        res.status(503).json({ error: "No Discogs token configured" });
        return;
    }
    try {
        const result = await dc.getMasterRelease(req.params.id);
        // Always save fresh data to cache
        cacheRelease(id, "master", result).catch(() => { });
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
    res.setHeader("Cache-Control", "public, max-age=3600"); // 1 hour
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
    res.setHeader("Cache-Control", "public, max-age=3600"); // 1 hour
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
    res.setHeader("Cache-Control", "public, max-age=86400"); // 24 hours
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
        const response = await loggedFetch("anthropic", "https://api.anthropic.com/v1/messages", {
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
            context: "genre profile",
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
    res.setHeader("Cache-Control", "public, max-age=300"); // 5 min
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
            // Try cache first for the master lookup
            const cachedMaster = await getCachedRelease(parseInt(id, 10), "master").catch(() => null);
            const master = cachedMaster ?? await dc.getMasterRelease(id);
            if (!cachedMaster && master)
                cacheRelease(parseInt(id, 10), "master", master).catch(() => { });
            releaseId = String(master?.main_release ?? id);
        }
        const statsRes = await loggedFetch("discogs", `https://api.discogs.com/marketplace/stats/${releaseId}?curr_abbr=USD`, { headers: { "Authorization": `Discogs token=${reqToken}`, "User-Agent": MB_UA }, context: "marketplace stats" });
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
        const r = await loggedFetch("discogs", `https://api.discogs.com/masters/${id}/versions?per_page=100&sort=released&sort_order=asc`, { headers: { "Authorization": `Discogs token=${reqToken}`, "User-Agent": MB_UA }, context: "master versions" });
        const data = await r.json();
        const versions = (data.versions ?? []).map((v) => ({
            id: v.id,
            title: v.title,
            label: v.label,
            catno: v.catno,
            country: v.country,
            year: v.released,
            format: v.format,
            majorFormats: v.major_formats ?? [],
            url: v.resource_url ? `https://www.discogs.com/release/${v.id}` : null,
        }));
        res.json({ versions });
    }
    catch (err) {
        console.error(err);
        res.json({ versions: [] });
    }
});
// GET /api/fresh-releases — 150 random releases from last 3 months, client-side filtered
app.get("/api/fresh-releases", async (req, res) => {
    try {
        const userId = getClerkUserId(req);
        if (userId) {
            res.setHeader("Cache-Control", "private, max-age=300");
            const releases = await getPersonalizedFreshReleases(userId, 150);
            res.json({ releases, personalized: true });
        }
        else {
            res.setHeader("Cache-Control", "public, max-age=300");
            const releases = await getFreshReleases(150);
            res.json({ releases });
        }
    }
    catch (err) {
        console.error("fresh-releases error:", err);
        res.json({ releases: [] });
    }
});
// GET /api/fresh-releases/search?q=... — search full 3-month DB by artist/release/tag
app.get("/api/fresh-releases/search", async (req, res) => {
    try {
        res.setHeader("Cache-Control", "public, max-age=120"); // 2 min
        const q = String(req.query.q ?? "").trim();
        if (!q) {
            res.json({ releases: [] });
            return;
        }
        const releases = await searchFreshReleases(q, 200);
        res.json({ releases });
    }
    catch (err) {
        console.error("fresh-releases search error:", err);
        res.json({ releases: [] });
    }
});
// ── Live tab: flexible concert search (artist, city/zip, genre) ───────────
app.get("/api/concerts/search", async (req, res) => {
    res.setHeader("Cache-Control", "no-cache"); // disable until stable
    const artist = (req.query.artist ?? "").trim();
    const city = (req.query.city ?? "").trim();
    const genre = (req.query.genre ?? "").trim();
    const page = parseInt(req.query.page ?? "0", 10) || 0;
    if (!artist && !city && !genre) {
        res.json({ events: [], artistImage: null });
        return;
    }
    const events = [];
    let artistImage = null;
    let tmTotalPages = 0;
    // ── Ticketmaster ──
    if (ticketmasterKey) {
        try {
            const params = new URLSearchParams({
                classificationName: genre || "music",
                size: "200",
                page: String(page),
                sort: "date,asc",
                apikey: ticketmasterKey,
            });
            if (artist)
                params.set("keyword", artist);
            if (city) {
                if (/^\d{5}$/.test(city)) {
                    params.set("postalCode", city);
                    params.set("radius", "75");
                    params.set("unit", "miles");
                    params.set("countryCode", "US");
                }
                else {
                    params.set("city", city);
                }
            }
            const tmUrl = `https://app.ticketmaster.com/discovery/v2/events.json?${params}`;
            console.log("Live TM URL:", tmUrl.replace(ticketmasterKey, "***"));
            const tmRes = await loggedFetch("ticketmaster", tmUrl, { context: "live search" });
            const tmBody = await tmRes.text();
            if (tmRes.ok) {
                try {
                    const tmData = JSON.parse(tmBody);
                    tmTotalPages = tmData.page?.totalPages ?? 0;
                    for (const ev of (tmData._embedded?.events ?? [])) {
                        const attractions = ev._embedded?.attractions ?? [];
                        let eventArtist = attractions[0]?.name ?? "";
                        if (artist) {
                            const artistLower = artist.toLowerCase();
                            const matched = attractions.find((a) => (a.name ?? "").toLowerCase().includes(artistLower) ||
                                artistLower.includes((a.name ?? "").toLowerCase()));
                            if (!matched && !(ev.name ?? "").toLowerCase().includes(artistLower))
                                continue;
                            if (matched)
                                eventArtist = matched.name;
                        }
                        const mapped = mapTmEvent(ev);
                        if (!isMusicEvent(mapped))
                            continue;
                        events.push({
                            ...mapped,
                            artist: eventArtist || ev.name?.split(/\s[-–—:]\s/)?.[0] || "",
                        });
                    }
                }
                catch { /* parse error */ }
            }
        }
        catch (err) {
            console.error("Live TM error:", err);
        }
    }
    // ── Bandsintown (only when artist is specified) ──
    if (artist) {
        try {
            const [artRes, evtRes] = await Promise.all([
                fetch(`https://rest.bandsintown.com/artists/${encodeURIComponent(artist)}?app_id=${bandsintownAppId}`),
                fetch(`https://rest.bandsintown.com/artists/${encodeURIComponent(artist)}/events?app_id=${bandsintownAppId}&date=upcoming`),
            ]);
            if (artRes.ok) {
                try {
                    const artData = await artRes.json();
                    const bitName = (artData.name ?? "").toLowerCase().trim();
                    const searchName = artist.toLowerCase().trim();
                    if (bitName === searchName || bitName.includes(searchName) || searchName.includes(bitName)) {
                        const img = artData.image_url || artData.thumb_url || null;
                        if (img && !img.includes("default") && !img.includes("placeholder")) {
                            artistImage = img;
                        }
                    }
                }
                catch { /* ignore */ }
            }
            if (evtRes.ok) {
                try {
                    const bitBody = await evtRes.text();
                    const bitData = JSON.parse(bitBody);
                    if (Array.isArray(bitData)) {
                        for (const ev of bitData) {
                            const evCity = ev.venue?.city ?? "";
                            const evRegion = ev.venue?.region ?? "";
                            if (city && !/^\d{5}$/.test(city)) {
                                const cityLower = city.toLowerCase();
                                if (!evCity.toLowerCase().includes(cityLower) && !evRegion.toLowerCase().includes(cityLower))
                                    continue;
                            }
                            events.push({
                                artist: artist,
                                name: ev.title ?? `${artist} live`,
                                date: (ev.datetime ?? "").slice(0, 10),
                                time: (ev.datetime ?? "").slice(11, 16),
                                venue: ev.venue?.name ?? "",
                                venueId: "",
                                city: evCity,
                                region: evRegion,
                                country: ev.venue?.country ?? "",
                                url: ev.url ?? "",
                                source: "bandsintown",
                            });
                        }
                    }
                }
                catch { /* parse error */ }
            }
        }
        catch (err) {
            console.error("Live BIT error:", err);
        }
    }
    const seen = new Set();
    const deduped = [];
    for (const ev of events) {
        const key = `${ev.date}|${ev.venue}`.toLowerCase();
        if (seen.has(key))
            continue;
        seen.add(key);
        deduped.push(ev);
    }
    deduped.sort((a, b) => a.date.localeCompare(b.date));
    const hasMore = page + 1 < tmTotalPages;
    res.json({ events: deduped, artistImage, page, hasMore });
});
// GET /api/concerts/venue/:venueId — all upcoming events at a Ticketmaster venue
app.get("/api/concerts/venue/:venueId", async (req, res) => {
    res.setHeader("Cache-Control", "public, max-age=900");
    const venueId = req.params.venueId;
    if (!venueId || !ticketmasterKey) {
        res.json({ events: [], venueName: "" });
        return;
    }
    try {
        const tmUrl = `https://app.ticketmaster.com/discovery/v2/events.json?venueId=${encodeURIComponent(venueId)}&classificationName=music&size=200&sort=date,asc&apikey=${ticketmasterKey}`;
        const tmRes = await loggedFetch("ticketmaster", tmUrl, { context: "venue events" });
        if (!tmRes.ok) {
            res.json({ events: [], venueName: "" });
            return;
        }
        const tmData = await tmRes.json();
        const events = [];
        let venueName = "";
        for (const ev of (tmData._embedded?.events ?? [])) {
            const venue = ev._embedded?.venues?.[0];
            if (!venueName && venue?.name)
                venueName = venue.name;
            const mapped = mapTmEvent(ev);
            if (!isMusicEvent(mapped))
                continue;
            events.push({
                ...mapped,
                venueId: mapped.venueId || venueId,
            });
        }
        const location = events[0] ? [events[0].city, events[0].region, events[0].country].filter(Boolean).join(", ") : "";
        res.json({ events, venueName, location });
    }
    catch (err) {
        console.error("Venue events error:", err);
        res.json({ events: [], venueName: "" });
    }
});
// ── Concert info (Ticketmaster + Bandsintown) ─────────────────────────────
app.get("/api/concerts/:artist", async (req, res) => {
    res.setHeader("Cache-Control", "public, max-age=900"); // 15 min
    const artist = decodeURIComponent(req.params.artist).trim();
    if (!artist) {
        res.json({ events: [] });
        return;
    }
    const events = [];
    // Ticketmaster Discovery API
    if (ticketmasterKey) {
        try {
            const tmUrl = `https://app.ticketmaster.com/discovery/v2/events.json?keyword=${encodeURIComponent(artist)}&classificationName=music&size=20&sort=date,asc&apikey=${ticketmasterKey}`;
            const tmRes = await loggedFetch("ticketmaster", tmUrl, { context: `artist: ${artist}` });
            const tmBody = await tmRes.text();
            if (tmRes.ok) {
                try {
                    const tmData = JSON.parse(tmBody);
                    for (const ev of (tmData._embedded?.events ?? [])) {
                        const mapped = mapTmEvent(ev);
                        if (isMusicEvent(mapped))
                            events.push(mapped);
                    }
                }
                catch { /* parse error */ }
            }
            else {
                console.error(`Ticketmaster ${tmRes.status}:`, tmBody.slice(0, 300));
            }
        }
        catch (err) {
            console.error("Ticketmaster fetch error:", err);
        }
    }
    else {
        console.log("Ticketmaster skipped — no TICKETMASTER_API_KEY env var");
    }
    // Bandsintown API
    try {
        const bitUrl = `https://rest.bandsintown.com/artists/${encodeURIComponent(artist)}/events?app_id=${bandsintownAppId}&date=upcoming`;
        const bitRes = await loggedFetch("bandsintown", bitUrl, { context: `artist: ${artist}` });
        const bitBody = await bitRes.text();
        if (bitRes.ok) {
            try {
                const bitData = JSON.parse(bitBody);
                if (Array.isArray(bitData)) {
                    for (const ev of bitData) {
                        events.push({
                            name: ev.title ?? `${artist} live`,
                            date: (ev.datetime ?? "").slice(0, 10),
                            time: (ev.datetime ?? "").slice(11, 16),
                            venue: ev.venue?.name ?? "",
                            city: ev.venue?.city ?? "",
                            region: ev.venue?.region ?? "",
                            country: ev.venue?.country ?? "",
                            url: ev.url ?? "",
                            source: "bandsintown",
                        });
                    }
                }
            }
            catch { /* parse error */ }
        }
        else {
            console.error(`Bandsintown ${bitRes.status}:`, bitBody.slice(0, 300));
        }
    }
    catch (err) {
        console.error("Bandsintown fetch error:", err);
    }
    // Dedupe by date+venue (prefer ticketmaster if duplicate)
    const seen = new Set();
    const deduped = [];
    for (const ev of events) {
        const key = `${ev.date}|${ev.venue}`.toLowerCase();
        if (seen.has(key))
            continue;
        seen.add(key);
        deduped.push(ev);
    }
    // Sort by date ascending
    deduped.sort((a, b) => a.date.localeCompare(b.date));
    console.log(`Concerts for "${artist}": ${events.length} raw, ${deduped.length} deduped`);
    res.json({ artist, events: deduped });
});
// ── eBay Gear integration ─────────────────────────────────────────────────
const ebayClientId = process.env.EBAY_CLIENT_ID ?? "";
const ebayClientSecret = process.env.EBAY_CLIENT_SECRET ?? "";
let ebayAccessToken = "";
let ebayTokenExpiry = 0;
async function getEbayToken() {
    if (ebayAccessToken && Date.now() < ebayTokenExpiry - 60000)
        return ebayAccessToken;
    if (!ebayClientId || !ebayClientSecret)
        throw new Error("eBay credentials not configured");
    const creds = Buffer.from(`${ebayClientId}:${ebayClientSecret}`).toString("base64");
    const r = await loggedFetch("ebay", "https://api.ebay.com/identity/v1/oauth2/token", {
        method: "POST",
        headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": `Basic ${creds}`,
        },
        body: "grant_type=client_credentials&scope=https%3A%2F%2Fapi.ebay.com%2Foauth%2Fapi_scope",
        context: "oauth token",
    });
    if (!r.ok)
        throw new Error(`eBay OAuth failed: ${r.status}`);
    const data = await r.json();
    ebayAccessToken = data.access_token;
    ebayTokenExpiry = Date.now() + data.expires_in * 1000;
    console.log("eBay OAuth token refreshed");
    return ebayAccessToken;
}
const GEAR_SEARCH_QUERIES = [
    "vintage receiver",
    "vintage amplifier",
    "vintage turntable",
    "vintage speakers hifi",
    "vintage tape deck reel",
    "hifi separates amplifier",
    "tube amplifier audio",
    "vintage preamp audio",
];
async function fetchEbayGearListings() {
    if (!ebayClientId || !ebayClientSecret) {
        console.log("eBay gear fetch skipped — no credentials");
        return 0;
    }
    console.log("Starting eBay gear fetch…");
    let totalUpserted = 0;
    try {
        const token = await getEbayToken();
        for (const query of GEAR_SEARCH_QUERIES) {
            try {
                // Auctions only, sorted by ending soonest (most active)
                const url = `https://api.ebay.com/buy/browse/v1/item_summary/search?q=${encodeURIComponent(query)}&limit=200&sort=endingSoonest&filter=price:[50..],priceCurrency:USD,buyingOptions:{AUCTION}`;
                const r = await loggedFetch("ebay", url, {
                    headers: { "Authorization": `Bearer ${token}`, "X-EBAY-C-MARKETPLACE-ID": "EBAY_US" },
                    context: `gear search: ${query}`,
                });
                if (!r.ok) {
                    console.error(`eBay search "${query}" failed: ${r.status}`);
                    continue;
                }
                const data = await r.json();
                const summaries = data.itemSummaries ?? [];
                console.log(`eBay "${query}": ${summaries.length} results`);
                const items = summaries.map((s) => ({
                    itemId: s.itemId,
                    title: s.title ?? "",
                    price: parseFloat(s.currentBidPrice?.value ?? s.price?.value ?? "0"),
                    currency: s.currentBidPrice?.currency ?? s.price?.currency ?? "USD",
                    condition: s.condition ?? s.conditionId ?? "",
                    imageUrl: s.image?.imageUrl ?? "",
                    itemUrl: s.itemWebUrl ?? "",
                    locationCity: s.itemLocation?.city ?? "",
                    locationState: s.itemLocation?.stateOrProvince ?? "",
                    locationCountry: s.itemLocation?.country ?? "",
                    sellerUsername: s.seller?.username ?? "",
                    sellerFeedback: s.seller?.feedbackScore ?? 0,
                    buyingOptions: s.buyingOptions ?? [],
                    bidCount: s.bidCount ?? 0,
                    categories: (s.categories ?? []).map((c) => c.categoryId),
                    categoryNames: (s.categories ?? []).map((c) => c.categoryName),
                    itemEndDate: s.itemEndDate ?? null,
                    thumbnailUrl: (s.thumbnailImages ?? [])[0]?.imageUrl ?? "",
                    rawSummary: s,
                }));
                const count = await upsertGearListings(items);
                totalUpserted += count;
                // Pace requests to avoid rate limiting
                await new Promise(r => setTimeout(r, 1000));
            }
            catch (err) {
                console.error(`eBay search "${query}" error:`, err);
            }
        }
        // Mark old listings as expired
        const expired = await markExpiredGearListings();
        if (expired)
            console.log(`Marked ${expired} gear listings as expired`);
        await logGearFetch("browse_search", totalUpserted);
        console.log(`eBay gear fetch complete: ${totalUpserted} items upserted`);
    }
    catch (err) {
        console.error("eBay gear fetch failed:", err);
        await logGearFetch("browse_search", totalUpserted, String(err));
    }
    return totalUpserted;
}
async function fetchGearDetails() {
    if (!ebayClientId || !ebayClientSecret)
        return 0;
    let detailed = 0;
    try {
        const token = await getEbayToken();
        const items = await getGearNeedingDetail(50);
        if (!items.length)
            return 0;
        console.log(`Fetching details for ${items.length} gear listings…`);
        for (const item of items) {
            try {
                const r = await loggedFetch("ebay", `https://api.ebay.com/buy/browse/v1/item/${item.itemId}`, {
                    headers: { "Authorization": `Bearer ${token}`, "X-EBAY-C-MARKETPLACE-ID": "EBAY_US" },
                    context: `gear detail: ${item.itemId}`,
                });
                if (!r.ok) {
                    console.error(`eBay getItem ${item.itemId} failed: ${r.status}`);
                    continue;
                }
                const d = await r.json();
                const detailHtml = d.description ?? "";
                const allImages = (d.additionalImages ?? []).map((img) => img.imageUrl).filter(Boolean);
                if (d.image?.imageUrl)
                    allImages.unshift(d.image.imageUrl);
                const specifics = {};
                for (const nv of (d.localizedAspects ?? [])) {
                    if (nv.name && nv.value)
                        specifics[nv.name] = nv.value;
                }
                await updateGearDetail(item.itemId, detailHtml, allImages, specifics);
                detailed++;
                // ~17 seconds between calls to stay well within 5000/day
                await new Promise(r => setTimeout(r, 2000));
            }
            catch (err) {
                console.error(`eBay detail ${item.itemId} error:`, err);
            }
        }
        await logGearFetch("item_detail", detailed);
        console.log(`eBay detail fetch complete: ${detailed} items detailed`);
    }
    catch (err) {
        console.error("eBay detail fetch failed:", err);
        await logGearFetch("item_detail", detailed, String(err));
    }
    return detailed;
}
// Schedule: gear search at :55, detail worker at :25 (30min offset)
function startGearSchedule() {
    if (!ebayClientId || !ebayClientSecret) {
        console.log("eBay gear schedule not started — no credentials");
        return;
    }
    // Hourly gear search at :50 past the hour (anchored to 4:50 AM Pacific)
    const msSearch = msUntilPacific(4, 50, 1);
    console.log(`[gear] Next search in ${Math.round(msSearch / 60000)}min, then every 1h`);
    setTimeout(() => {
        fetchEbayGearListings();
        setInterval(() => fetchEbayGearListings(), 60 * 60 * 1000);
    }, msSearch);
    // Detail worker every 30min at :55 past the hour (anchored to 4:55 AM Pacific)
    const msDetail = msUntilPacific(4, 55, 1);
    console.log(`[gear-detail] Next detail fetch in ${Math.round(msDetail / 60000)}min, then every 30min`);
    setTimeout(() => {
        fetchGearDetails();
        setInterval(() => fetchGearDetails(), 30 * 60 * 1000);
    }, msDetail);
}
// GET /api/gear — public gear listings
app.get("/api/gear", async (_req, res) => {
    try {
        const minPrice = parseFloat(_req.query.min_price) || 0;
        const sort = _req.query.sort || "bids";
        const q = _req.query.q || "";
        const limit = Math.min(parseInt(_req.query.limit) || 200, 500);
        const offset = parseInt(_req.query.offset) || 0;
        res.setHeader("Cache-Control", "public, max-age=300"); // 5 min
        const { items, total } = await getGearListings(minPrice, limit, offset, sort, q);
        res.json({ items, total });
    }
    catch (e) {
        res.status(500).json({ error: String(e) });
    }
});
// GET /api/gear/stats — admin stats
app.get("/api/gear/stats", async (_req, res) => {
    try {
        const stats = await getGearStats();
        res.json(stats);
    }
    catch (e) {
        res.status(500).json({ error: String(e) });
    }
});
// POST /api/admin/gear/fetch — manual trigger for admin
app.post("/api/admin/gear/fetch", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    res.json({ ok: true, started: true });
    fetchEbayGearListings().then(() => fetchGearDetails());
});
// ── eBay Marketplace Account Deletion Notification (compliance) ──────────
const EBAY_VERIFICATION_TOKEN = "seadisco2026accountdeletiontoken";
app.get("/api/ebay/deletion", (req, res) => {
    // eBay sends a GET with challenge_code to verify the endpoint
    const challengeCode = req.query.challenge_code;
    if (!challengeCode)
        return res.status(400).json({ error: "missing challenge_code" });
    const hash = crypto.createHash("sha256");
    hash.update(challengeCode);
    hash.update(EBAY_VERIFICATION_TOKEN);
    hash.update("https://discogs-mcp-server-production-c794.up.railway.app/api/ebay/deletion");
    const responseHash = hash.digest("hex");
    res.json({ challengeResponse: responseHash });
});
app.post("/api/ebay/deletion", (_req, res) => {
    // eBay sends POST for actual deletion notifications — just acknowledge
    res.status(200).json({ ok: true });
});
// ── Feed: RSS + YouTube ─────────────────────────────────────────────────
const RSS_FEEDS = [
    { name: "Pitchfork", url: "https://pitchfork.com/feed/feed-album-reviews/rss", category: "reviews" },
    { name: "Pitchfork News", url: "https://pitchfork.com/feed/feed-news/rss", category: "news" },
    { name: "Bandcamp Daily", url: "https://daily.bandcamp.com/feed", category: "reviews" },
    { name: "Stereogum", url: "https://www.stereogum.com/feed/", category: "news" },
    { name: "Aquarium Drunkard", url: "https://aquariumdrunkard.com/feed/", category: "news" },
    { name: "The Quietus", url: "https://thequietus.com/feed", category: "reviews" },
    { name: "BrooklynVegan", url: "https://www.brooklynvegan.com/feed/", category: "news" },
];
const YOUTUBE_CHANNELS = [
    // Vinyl & collecting
    { name: "Vinyl Eyezz", channelId: "UCYkG_jJ2L0clu-_fqFfWGbQ" },
    { name: "The Vinyl Guide", channelId: "UCdYYSNnFPIdMuiAaq0pSpTA" },
    // Gear & audiophile
    { name: "Techmoan", channelId: "UC5I2hjZYiW9gZPVkvzM8_Cw", category: "gear" },
    { name: "Analog Planet", channelId: "UCXnnKXr8oSTfZ7Y3R8CGAUQ", category: "gear" },
    // Live sessions & performances
    { name: "KEXP", channelId: "UC3I2GFN_F8WudD_2jUZbojA" },
    { name: "Tiny Desk (NPR)", channelId: "UC4eYXhJI4-7wSWc8UNRwD4A" },
    { name: "COLORS", channelId: "UC2Qw1dzXDBAZPwS7zm37g8g" },
    { name: "Audiotree", channelId: "UCelEMf7HHJgUy-6MJPClcXA" },
    // Reviews
    { name: "The Needle Drop", channelId: "UCt7fwAhXDy3oNFTAzF2o8Pw", category: "reviews" },
    { name: "Deep Cuts", channelId: "UCVBp4LmZjEBpzB-6VsFU4cQ", category: "reviews" },
];
// Keyword searches for fresh music content
const YOUTUBE_SEARCHES = [
    { query: "official music video 2026", category: "video" },
    { query: "new album review 2026", category: "reviews" },
    { query: "vinyl unboxing haul 2026", category: "video" },
];
function decodeHtmlEntities(str) {
    return str
        .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
        .replace(/&#x([0-9a-fA-F]+);/g, (_, n) => String.fromCharCode(parseInt(n, 16)))
        .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
        .replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&nbsp;/g, " ")
        .replace(/&mdash;/g, "—").replace(/&ndash;/g, "–").replace(/&lsquo;/g, "'")
        .replace(/&rsquo;/g, "'").replace(/&ldquo;/g, "\u201C").replace(/&rdquo;/g, "\u201D")
        .replace(/&hellip;/g, "…");
}
function extractFromXml(xml, tag) {
    const re = new RegExp(`<${tag}[^>]*>(?:<!\\[CDATA\\[)?([\\s\\S]*?)(?:\\]\\]>)?</${tag}>`);
    const m = xml.match(re);
    return m ? m[1].trim() : "";
}
function extractAttr(xml, tag, attr) {
    const re = new RegExp(`<${tag}[^>]*${attr}=["']([^"']*)["']`);
    const m = xml.match(re);
    return m ? m[1] : "";
}
function extractImage(itemXml) {
    // Try media:content, media:thumbnail, enclosure, then img in description
    let img = extractAttr(itemXml, "media:content", "url") ||
        extractAttr(itemXml, "media:thumbnail", "url") ||
        extractAttr(itemXml, "enclosure", "url");
    if (!img) {
        const desc = extractFromXml(itemXml, "description") + extractFromXml(itemXml, "content:encoded");
        const imgMatch = desc.match(/<img[^>]+src=["']([^"']+)["']/);
        if (imgMatch)
            img = imgMatch[1];
    }
    return img;
}
async function fetchRssFeeds() {
    let total = 0;
    for (const feed of RSS_FEEDS) {
        try {
            const r = await loggedFetch("rss", feed.url, {
                headers: { "User-Agent": "SeaDisco/1.0 (music feed aggregator)" },
                signal: AbortSignal.timeout(15000),
                context: feed.name,
            });
            if (!r.ok) {
                console.warn(`RSS ${feed.name}: HTTP ${r.status}`);
                continue;
            }
            const xml = await r.text();
            // Split items
            const items = xml.split(/<item[\s>]/).slice(1);
            let count = 0;
            for (const itemXml of items.slice(0, 20)) { // Max 20 per feed
                try {
                    const title = decodeHtmlEntities(extractFromXml(itemXml, "title"));
                    const link = extractFromXml(itemXml, "link") || extractFromXml(itemXml, "guid");
                    if (!title || !link)
                        continue;
                    let summary = decodeHtmlEntities(extractFromXml(itemXml, "description").replace(/<[^>]+>/g, "")).trim();
                    if (summary.length > 300)
                        summary = summary.slice(0, 297) + "…";
                    const imageUrl = extractImage(itemXml);
                    const author = decodeHtmlEntities(extractFromXml(itemXml, "dc:creator") || extractFromXml(itemXml, "author") || "");
                    const pubDate = extractFromXml(itemXml, "pubDate") ||
                        extractFromXml(itemXml, "dc:date") || "";
                    const publishedAt = pubDate ? new Date(pubDate).toISOString() : null;
                    await upsertFeedArticle({
                        source: feed.name,
                        sourceUrl: link,
                        title,
                        summary: summary || undefined,
                        imageUrl: imageUrl || undefined,
                        author: author || undefined,
                        category: feed.category,
                        contentType: "article",
                        publishedAt: publishedAt ?? undefined,
                    });
                    count++;
                }
                catch (e) { /* skip bad item */ }
            }
            total += count;
            console.log(`RSS ${feed.name}: ${count} articles`);
        }
        catch (err) {
            console.warn(`RSS ${feed.name} failed:`, err);
        }
    }
    return total;
}
async function fetchYouTubeVideos() {
    if (!youtubeApiKey) {
        console.log("YouTube feed skip — no API key");
        return 0;
    }
    let total = 0;
    // Load existing YouTube video URLs to skip channels with no new content
    let existingUrls = new Set();
    try {
        existingUrls = await getExistingYouTubeUrls();
    }
    catch { }
    console.log(`[youtube] ${existingUrls.size} existing videos in DB`);
    // Fetch from specific channels — use RSS feed (free, no quota) to check for new videos first
    let skipped = 0;
    for (const ch of YOUTUBE_CHANNELS) {
        try {
            // Check channel RSS feed (free, no quota) for latest video ID
            const rssUrl = `https://www.youtube.com/feeds/videos.xml?channel_id=${ch.channelId}`;
            let hasNew = true;
            try {
                const rssR = await fetch(rssUrl, { signal: AbortSignal.timeout(5000), headers: { "User-Agent": "SeaDisco/1.0" } });
                if (rssR.ok) {
                    const xml = await rssR.text();
                    // Extract first video ID from RSS
                    const vidMatch = xml.match(/<yt:videoId>([^<]+)<\/yt:videoId>/);
                    if (vidMatch) {
                        const latestUrl = `https://www.youtube.com/watch?v=${vidMatch[1]}`;
                        if (existingUrls.has(latestUrl)) {
                            hasNew = false;
                        }
                    }
                }
            }
            catch { }
            if (!hasNew) {
                skipped++;
                continue;
            }
            const url = `https://www.googleapis.com/youtube/v3/search?key=${youtubeApiKey}&channelId=${ch.channelId}&part=snippet&order=date&maxResults=5&type=video`;
            const r = await loggedFetch("youtube", url, { signal: AbortSignal.timeout(10000), context: `channel: ${ch.name}` });
            if (!r.ok) {
                console.warn(`YouTube ${ch.name}: HTTP ${r.status}`);
                continue;
            }
            const data = await r.json();
            let count = 0;
            for (const item of data.items ?? []) {
                const videoId = item.id?.videoId;
                if (!videoId)
                    continue;
                const snippet = item.snippet;
                await upsertFeedArticle({
                    source: ch.name,
                    sourceUrl: `https://www.youtube.com/watch?v=${videoId}`,
                    title: decodeHtmlEntities(snippet.title ?? ""),
                    summary: snippet.description?.slice(0, 300) ?? "",
                    imageUrl: snippet.thumbnails?.high?.url ?? snippet.thumbnails?.medium?.url ?? "",
                    author: snippet.channelTitle ?? ch.name,
                    category: ch.category ?? "video",
                    contentType: "video",
                    publishedAt: snippet.publishedAt ?? undefined,
                });
                count++;
            }
            total += count;
            console.log(`YouTube ${ch.name}: ${count} videos`);
        }
        catch (err) {
            console.warn(`YouTube ${ch.name} failed:`, err);
        }
    }
    if (skipped)
        console.log(`[youtube] skipped ${skipped}/${YOUTUBE_CHANNELS.length} channels (no new videos)`);
    // Keyword searches for fresh music content (last 7 days)
    const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    for (const search of YOUTUBE_SEARCHES) {
        try {
            const url = `https://www.googleapis.com/youtube/v3/search?key=${youtubeApiKey}&q=${encodeURIComponent(search.query)}&part=snippet&order=date&maxResults=10&type=video&publishedAfter=${weekAgo}&videoCategoryId=10`;
            const r = await loggedFetch("youtube", url, { signal: AbortSignal.timeout(10000), context: `search: ${search.query}` });
            if (!r.ok) {
                console.warn(`YouTube search "${search.query}": HTTP ${r.status}`);
                continue;
            }
            const data = await r.json();
            let count = 0;
            for (const item of data.items ?? []) {
                const videoId = item.id?.videoId;
                if (!videoId)
                    continue;
                const snippet = item.snippet;
                await upsertFeedArticle({
                    source: snippet.channelTitle ?? "YouTube",
                    sourceUrl: `https://www.youtube.com/watch?v=${videoId}`,
                    title: decodeHtmlEntities(snippet.title ?? ""),
                    summary: snippet.description?.slice(0, 300) ?? "",
                    imageUrl: snippet.thumbnails?.high?.url ?? snippet.thumbnails?.medium?.url ?? "",
                    author: snippet.channelTitle ?? "",
                    category: search.category,
                    contentType: "video",
                    publishedAt: snippet.publishedAt ?? undefined,
                });
                count++;
            }
            total += count;
            console.log(`YouTube search "${search.query}": ${count} videos`);
        }
        catch (err) {
            console.warn(`YouTube search "${search.query}" failed:`, err);
        }
    }
    return total;
}
async function fetchAllFeedContent() {
    console.log("Starting feed content fetch…");
    const articles = await fetchRssFeeds();
    const videos = await fetchYouTubeVideos();
    const pruned = await pruneFeedArticles(90);
    const stale = await pruneAllStaleData();
    console.log(`Feed fetch complete: ${articles} articles, ${videos} videos, ${pruned} feed pruned`);
    const staleTotal = Object.values(stale).reduce((a, b) => a + b, 0);
    if (staleTotal > 0) {
        console.log(`Pruned stale data: ${JSON.stringify(stale)}`);
    }
}
function startFeedSchedule() {
    // Every 6 hours starting at 2:30 AM Pacific (YouTube quota: 13 calls × 100 = 1300 units × 4/day = 5200 < 10K limit)
    const ms = msUntilPacific(2, 30, 6);
    console.log(`[feed] Next fetch in ${Math.round(ms / 60000)}min, then every 6h`);
    setTimeout(() => {
        fetchAllFeedContent();
        setInterval(() => fetchAllFeedContent(), 6 * 60 * 60 * 1000);
    }, ms);
}
// GET /api/feed — personalized for logged-in users, public for anonymous
app.get("/api/feed", async (req, res) => {
    try {
        const category = req.query.category || "all";
        const q = req.query.q || "";
        const limit = Math.min(parseInt(req.query.limit) || 50, 200);
        const offset = parseInt(req.query.offset) || 0;
        const userId = getClerkUserId(req);
        if (userId) {
            try {
                const { items, total } = await getPersonalizedFeedArticles(userId, { category, limit, offset, q });
                res.setHeader("Cache-Control", items.length ? "private, max-age=300" : "no-cache");
                res.json({ items, total, personalized: true });
            }
            catch (personalErr) {
                console.error("Personalized feed failed, falling back to generic:", personalErr);
                const { items, total } = await getFeedArticles({ category, limit, offset, q });
                res.setHeader("Cache-Control", items.length ? "public, max-age=300" : "no-cache");
                res.json({ items, total });
            }
        }
        else {
            const { items, total } = await getFeedArticles({ category, limit, offset, q });
            res.setHeader("Cache-Control", items.length ? "public, max-age=300" : "no-cache");
            res.json({ items, total });
        }
    }
    catch (e) {
        res.status(500).json({ error: String(e) });
    }
});
// POST /api/admin/feed/fetch — manual trigger for admin
app.post("/api/admin/feed/fetch", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    res.json({ ok: true, started: true });
    fetchAllFeedContent();
});
// POST /api/admin/live/fetch — manual trigger for admin
app.post("/api/admin/live/fetch", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const count = await fetchUpcomingEvents();
    res.json({ ok: true, count });
});
// POST /api/admin/extras/fetch — manual trigger for inventory/lists sync
app.post("/api/admin/extras/fetch", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    res.json({ ok: true, message: "Extras sync started" });
    (async () => {
        const users = await getAllUsersForSync();
        for (const user of users) {
            try {
                const result = await syncUserExtras(user.clerkUserId, user.username, user.token);
                console.log(`[admin-extras] ${user.username}: ${result.inventory} inventory, ${result.lists} lists`);
                await sleep(30000); // 30s between users
            }
            catch (err) {
                console.error(`[admin-extras] Error syncing ${user.username}:`, err);
            }
        }
        console.log("[admin-extras] Complete");
    })();
});
// GET /api/user/taste-profile — user's own taste profile
app.get("/api/user/taste-profile", async (req, res) => {
    const userId = getClerkUserId(req);
    if (!userId) {
        res.status(401).json({ error: "Not authenticated" });
        return;
    }
    const profile = await getUserTasteProfile(userId);
    res.json({ profile });
});
// POST /api/admin/rebuild-taste — rebuild taste profile for all users
app.post("/api/admin/rebuild-taste", express.json(), async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const users = await getAllUsersForSync();
    let rebuilt = 0;
    const errors = [];
    for (const u of users) {
        try {
            const profile = await rebuildUserTasteProfile(u.clerkUserId);
            if (profile)
                rebuilt++;
            else
                errors.push(`${u.username}: no collection data`);
        }
        catch (err) {
            errors.push(`${u.username}: ${String(err).slice(0, 100)}`);
        }
    }
    res.json({ ok: true, rebuilt, total: users.length, errors });
});
// GET /api/admin/api-log — view API request log (last 24h by default)
app.get("/api/admin/api-log", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const service = req.query.service;
    const errorsOnly = req.query.errors === "true";
    const hours = Math.min(parseInt(req.query.hours) || 24, 168); // max 7 days
    const result = await getApiRequestLog({ service: service || undefined, errorsOnly, hours });
    res.json(result);
});
// GET /api/admin/api-stats — 24h summary by service
app.get("/api/admin/api-stats", async (req, res) => {
    const userId = getClerkUserId(req);
    const adminId = process.env.ADMIN_CLERK_ID ?? "";
    if (!userId || !adminId || userId !== adminId) {
        res.status(403).json({ error: "Forbidden" });
        return;
    }
    const hours = Math.min(parseInt(req.query.hours) || 24, 168);
    const stats = await getApiRequestStats(hours);
    res.json({ stats });
});
// Helper: ms until the next occurrence of HH:MM Pacific, repeating every intervalH hours
function msUntilPacific(hour, minute, intervalH) {
    const now = new Date();
    const pacificStr = now.toLocaleString("en-US", { timeZone: "America/Los_Angeles" });
    const pacific = new Date(pacificStr);
    // Try each upcoming slot today and tomorrow
    const base = new Date(pacific);
    base.setHours(hour, minute, 0, 0);
    // Rewind to the most recent slot at or before now, then step forward
    while (base.getTime() > pacific.getTime())
        base.setTime(base.getTime() - intervalH * 3600000);
    // Step forward to the next future slot
    while (base.getTime() <= pacific.getTime())
        base.setTime(base.getTime() + intervalH * 3600000);
    return base.getTime() - pacific.getTime();
}
const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3001;
// Scheduled sync: collection/wantlist every 6 hours starting at midnight Pacific
function startDailySyncSchedule() {
    async function runScheduledSync() {
        console.log(`[sync-schedule] Starting full sync for all users`);
        _syncAbort = false;
        const users = await getAllUsersForSync();
        for (const user of users) {
            if (_syncAbort) {
                console.log("[sync-schedule] Aborted");
                break;
            }
            try {
                console.log(`[sync-schedule] Full syncing ${user.username}...`);
                await runBackgroundSync(user.clerkUserId, user.token, user.username, true, true);
                // 30s pause between users to let rate limits settle
                await new Promise(r => setTimeout(r, 30000));
            }
            catch (err) {
                console.error(`[sync-schedule] Error syncing ${user.username}:`, err);
            }
        }
        console.log(`[sync-schedule] Full sync complete`);
    }
    function schedule() {
        const ms = msUntilPacific(0, 0, 6);
        const hours = Math.round(ms / 3600000 * 10) / 10;
        console.log(`[sync-schedule] Next full sync in ${hours}h (midnight Pacific, every 6h)`);
        setTimeout(async () => {
            await runScheduledSync();
            schedule();
        }, ms);
    }
    schedule();
}
// ── Inventory / Lists / Orders sync (5 AM Pacific daily) ──────────────────
async function syncUserExtras(userId, username, token) {
    const headers = { Authorization: `Discogs token=${token}`, "User-Agent": "SeaDisco/1.0" };
    // Simple fetch with retry for extras sync
    async function extrasFetch(url, retries = 3) {
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                const r = await loggedFetch("discogs", url, { headers, signal: AbortSignal.timeout(15000), context: `extras: ${username}` });
                if (r.ok || r.status === 401 || r.status === 403)
                    return r;
                if (r.status === 429 || r.status >= 500) {
                    if (attempt < retries) {
                        await sleep(15000 * attempt);
                        continue;
                    }
                    throw new Error(`HTTP ${r.status} after ${retries} attempts`);
                }
                throw new Error(`HTTP ${r.status}`);
            }
            catch (err) {
                if (attempt >= retries)
                    throw err;
                await sleep(10000 * attempt);
            }
        }
        throw new Error("unreachable");
    }
    let inventory = 0, lists = 0;
    // Inventory (paginated) — Discogs returns 401 if user isn't a seller
    try {
        for (let page = 1;; page++) {
            if (page > 1)
                await sleep(1200);
            const r = await extrasFetch(`https://api.discogs.com/users/${encodeURIComponent(username)}/inventory?per_page=100&page=${page}&sort=listed&sort_order=desc`);
            if (r.status === 401 || r.status === 403) {
                console.log(`[extras] ${username}: no inventory (${r.status})`);
                break;
            }
            const data = await r.json();
            const listings = data.listings ?? [];
            if (!listings.length)
                break;
            const items = listings.map((l) => ({
                listingId: l.id,
                releaseId: l.release?.id ?? undefined,
                data: l,
                status: l.status ?? "For Sale",
                priceValue: parseFloat(l.price?.value) || undefined,
                priceCurrency: l.price?.currency ?? "USD",
                condition: l.condition ?? undefined,
                sleeveCondition: l.sleeve_condition ?? undefined,
                postedAt: l.posted ? new Date(l.posted) : undefined,
            }));
            await upsertInventoryItems(userId, items);
            inventory += items.length;
            if (listings.length < 100)
                break;
        }
        await updateInventorySyncedAt(userId);
        console.log(`[extras] ${username}: ${inventory} inventory listings synced`);
    }
    catch (err) {
        console.error(`[extras] ${username} inventory error:`, err);
    }
    // Lists (not paginated) — Discogs returns 401 if user has no public lists
    try {
        await sleep(1200);
        const r = await extrasFetch(`https://api.discogs.com/users/${encodeURIComponent(username)}/lists?per_page=100`);
        if (r.status === 401 || r.status === 403) {
            console.log(`[extras] ${username}: no lists (${r.status})`);
        }
        const data = r.ok ? await r.json() : { lists: [] };
        const userLists = data.lists ?? [];
        if (userLists.length) {
            const items = userLists.map((l) => ({
                listId: l.id,
                name: l.name ?? "",
                description: l.description ?? undefined,
                itemCount: l.item_count ?? 0,
                isPublic: l.public !== false,
                data: l,
            }));
            await upsertUserLists(userId, items);
            lists = items.length;
        }
        console.log(`[extras] ${username}: ${lists} lists synced`);
    }
    catch (err) {
        console.error(`[extras] ${username} lists error:`, err);
    }
    return { inventory, lists };
}
function startExtrasSyncSchedule() {
    function schedule() {
        const ms = msUntilPacific(0, 15, 6);
        const hours = Math.round(ms / 3600000 * 10) / 10;
        console.log(`[extras-sync] Next extras sync in ${hours}h (12:15 AM Pacific, every 6h)`);
        setTimeout(async () => {
            console.log("[extras-sync] Starting inventory/lists sync for all users");
            const users = await getAllUsersForSync();
            for (const user of users) {
                try {
                    await syncUserExtras(user.clerkUserId, user.username, user.token);
                    await sleep(30000); // 30s between users
                }
                catch (err) {
                    console.error(`[extras-sync] Error syncing ${user.username}:`, err);
                }
            }
            console.log("[extras-sync] Complete");
            schedule();
        }, ms);
    }
    schedule();
}
app.listen(PORT, "0.0.0.0", () => {
    console.log(`Discogs search API listening on port ${PORT}`);
    if (process.env.APP_DB_URL) {
        startFreshSyncSchedule();
        startGearSchedule();
        startFeedSchedule();
        startDailySyncSchedule();
        startExtrasSyncSchedule();
        startLiveEventsSchedule();
    }
});
