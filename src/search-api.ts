import express from "express";
import compression from "compression";
import cookieParser from "cookie-parser";
import crypto from "crypto";
import fs from "fs";
import { createRemoteJWKSet, jwtVerify } from "jose";
import { fileURLToPath } from "url";
import path from "path";
import { DiscogsClient, signOAuthRequest } from "./discogs-client.js";
import { initDb, getAllUsersForSync, getAllUsersSyncStatus, getUserCount, getActiveUserCount, touchUserActivity, isUserHibernated, reactivateUser, hibernateInactiveUsers, getUserToken, setUserToken, deleteUserToken, deleteUserData, saveFeedback, getFeedback, deleteFeedback, getDiscogsUsername, getClerkUserIdByUsername, setDiscogsUsername, getSyncStatus, updateSyncProgress, upsertCollectionItems, upsertCollectionFolders, upsertWantlistItems, getCollectionPage, getWantlistPage, getAllCollectionItems, getAllWantlistItems, getCollectionIds, getWantlistIds, getCollectionFacets, getWantlistFacets, getCollectionFolderList, updateCollectionSyncedAt, updateWantlistSyncedAt, getWantedItems, resetAllSyncingStatuses, pruneAllStaleData, upsertInventoryItems, updateInventorySyncedAt, upsertUserLists, getInventoryPage, getUserListsList, logApiRequest, getApiRequestLog, getApiRequestStats, getUserCollectionStats, getCachedRelease, cacheRelease, storeOAuthRequestToken, getOAuthRequestToken, deleteOAuthRequestToken, pruneOAuthRequestTokens, setOAuthCredentials, getOAuthCredentials, clearOAuthCredentials, setDiscogsProfile, getDiscogsProfile, deleteCollectionItem, deleteWantlistItem, updateCollectionRating, updateCollectionFolder, getCollectionInstance, getCollectionInstances, getCollectionMultiInstanceCounts, getCollectionMasterCounts, getWantlistMasterCounts, updateCollectionNotes, updateWantlistNotes, getWantlistItem, upsertRecentView, getRecentViews, deleteRecentView, clearRecentViews, saveLocItem, getLocSaves, deleteLocSave, getLocSaveIds, renameCollectionFolder, deleteCollectionFolder, moveAllCollectionItemsBetweenFolders, getFolderContents, upsertPriceCache, appendPriceHistory, getSavedSearches, saveSavedSearch, deleteSavedSearch, pruneWantlistItems, pruneCollectionItems, getFavoriteIds, getFavorites, addFavorite, removeFavorite, getAllFavoriteCounts, upsertListItems, getListItems, getListMembership, getInventoryIds, getListItemStats, getRandomRecords, getDefaultAddFolderId, setDefaultAddFolderId, getInventoryItem, deleteInventoryItem, getInventoryListingIdsByRelease, upsertUserOrders, updateOrdersSyncedAt, getOrdersCount, getUserOrdersPage, getUserOrder, upsertOrderMessages, getOrderMessages, markOrderViewed, getUnreadOrdersCount, getTableRowCounts, purgeNonAdminUserData, listBluesArtists, getBluesArtist, deleteBluesArtist, insertBluesArtist, updateBluesArtist, getBluesStats, deleteAllBluesArtists } from "./db.js";
import { seedBluesArtistsFromWikidata, seedBluesArtistsFromDiscogs, enrichBluesFromMusicBrainz, enrichBluesFromWikipedia, enrichBluesFromDiscogs, enrichBluesArtistFromYouTube } from "./blues-db.js";


const __dirname = path.dirname(fileURLToPath(import.meta.url));

const anthropicKey    = process.env.ANTHROPIC_API_KEY    ?? "";
// Discogs OAuth 1.0a consumer credentials (register at discogs.com/settings/developers)
const discogsConsumerKey    = process.env.DISCOGS_CONSUMER_KEY    ?? "";
const discogsConsumerSecret = process.env.DISCOGS_CONSUMER_SECRET ?? "";
// Publishable key sent to frontend via /api/config
const authPk      = process.env.AUTH_PK ?? "";
// Cached at boot — hot-path functions (requireAdmin + 20-odd admin routes)
// previously re-read this env var per request.
const ADMIN_CLERK_ID = process.env.ADMIN_CLERK_ID ?? "";
// SeaDisco is invite-only — Clerk waitlist gates all sign-ups. Every API
// endpoint that touches user data or external services requires a valid
// Clerk session via requireUser(). The admin tab is additionally gated by
// ADMIN_CLERK_ID. There is no shared/anonymous Discogs token fallback.


const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

// Shape the subset of Discogs profile fields we persist, so all three
// setDiscogsProfile call sites stay in sync and every field the dashboard
// needs (including curr_abbr for default listing currency and the seller
// rating stars) is captured.
function _extractDiscogsProfile(profile: any): any {
  return {
    username:            profile.username,
    name:                profile.name,
    registered:          profile.registered,
    home_page:           profile.home_page,
    profile:             profile.profile,
    location:            profile.location,
    curr_abbr:           profile.curr_abbr,
    num_collection:      profile.num_collection,
    num_wantlist:        profile.num_wantlist,
    num_lists:           profile.num_lists,
    num_for_sale:        profile.num_for_sale,
    num_pending:         profile.num_pending,
    releases_rated:      profile.releases_rated,
    rating_avg:          profile.rating_avg,
    seller_rating:       profile.seller_rating,
    seller_num_ratings:  profile.seller_num_ratings,
    seller_rating_stars: profile.seller_rating_stars,
    buyer_rating:        profile.buyer_rating,
    buyer_num_ratings:   profile.buyer_num_ratings,
    buyer_rating_stars:  profile.buyer_rating_stars,
    releases_contributed: profile.releases_contributed,
    rank:                profile.rank,
  };
}

// ── Global API kill switch ──────────────────────────────────────────────
const MAX_USERS         = 25;
let _apiKillSwitch = false;

// ── Token-bucket rate limiter (shared across all callers) ──────────────
//
// Used by the LOC proxy so a stampede of clicks from any signed-in user
// can't take the whole site's LOC IP allowance down. Acquire blocks until
// a slot is free, or rejects with `rate_limit_queue_full` if too many
// callers are already waiting (so the client gets a fast 503 rather than
// an indefinite hang).
class RateLimiter {
  private slots: number[] = [];           // Timestamps of recent completions, within windowMs
  private queue: Array<() => void> = [];  // Resolvers waiting for a slot
  private draining = false;
  constructor(
    private readonly max: number,
    private readonly windowMs: number,
    private readonly maxQueueDepth: number,
    public readonly label: string,
  ) {}
  async acquire(): Promise<void> {
    this.prune();
    if (this.slots.length < this.max) {
      this.slots.push(Date.now());
      return;
    }
    if (this.queue.length >= this.maxQueueDepth) {
      const e = new Error("rate_limit_queue_full");
      (e as any).code = "rate_limit_queue_full";
      throw e;
    }
    await new Promise<void>(resolve => {
      this.queue.push(resolve);
      this.scheduleDrain();
    });
  }
  /** Current limiter state — for the admin stats panel. */
  getStats(): { inWindow: number; queued: number; max: number; windowMs: number; maxQueueDepth: number } {
    this.prune();
    return {
      inWindow: this.slots.length,
      queued: this.queue.length,
      max: this.max,
      windowMs: this.windowMs,
      maxQueueDepth: this.maxQueueDepth,
    };
  }
  private prune() {
    const cutoff = Date.now() - this.windowMs;
    while (this.slots.length && this.slots[0] < cutoff) this.slots.shift();
  }
  private scheduleDrain() {
    if (this.draining) return;
    this.draining = true;
    const tryDrain = () => {
      this.prune();
      while (this.queue.length && this.slots.length < this.max) {
        const next = this.queue.shift()!;
        this.slots.push(Date.now());
        next();
      }
      if (this.queue.length) {
        // Next opening = when the oldest slot falls out of the window
        const now = Date.now();
        const wait = Math.max(20, this.windowMs - (now - this.slots[0]) + 5);
        setTimeout(tryDrain, wait);
      } else {
        this.draining = false;
      }
    };
    setTimeout(tryDrain, 20);
  }
}

// LOC doesn't publish a hard rate limit but the docs strongly discourage
// hammering — 20 requests/minute across all users is safely under any
// reasonable threshold, and we cache every response for 6 hours so repeat
// searches are free. Queue caps at 30 pending so worst-case tail latency
// is bounded (queue full → fast 503 → client governor backs off).
const locLimiter = new RateLimiter(20, 60_000, 30, "loc");

// Simple in-memory LRU-ish cache for LOC search responses. Key is the
// canonicalized query string; value is the raw body + timestamp.
const _locCache = new Map<string, { ts: number; body: any }>();
const LOC_CACHE_TTL_MS = 6 * 60 * 60 * 1000;  // 6 hours
const LOC_CACHE_MAX = 200;
function _locCacheGet(key: string): any | null {
  const entry = _locCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.ts > LOC_CACHE_TTL_MS) { _locCache.delete(key); return null; }
  // Refresh LRU order
  _locCache.delete(key);
  _locCache.set(key, entry);
  return entry.body;
}
function _locCacheSet(key: string, body: any) {
  if (_locCache.size >= LOC_CACHE_MAX) {
    // Drop oldest
    const firstKey = _locCache.keys().next().value;
    if (firstKey !== undefined) _locCache.delete(firstKey);
  }
  _locCache.set(key, { ts: Date.now(), body });
}

// Runtime LOC stats surfaced by GET /api/admin/loc-stats. Cumulative
// counters since last process restart, plus a rolling 24h timestamp
// deque so the admin dashboard can show requests per hour / per day.
const _locStats = {
  cacheHits:      0,
  cacheMisses:    0,
  failures:       0,
  rateLimitHits:  0,          // how many times we hit 503/rate_limit_queue_full
  lastRequestAt:  null as number | null,
  lastFailureAt:  null as number | null,
  lastFailureMsg: "" as string,
  recentTs:       [] as number[],  // timestamps of every proxy hit in last 24h
};
const LOC_STATS_WINDOW_MS = 24 * 60 * 60 * 1000;
function _locStatsPrune() {
  const cutoff = Date.now() - LOC_STATS_WINDOW_MS;
  while (_locStats.recentTs.length && _locStats.recentTs[0] < cutoff) _locStats.recentTs.shift();
}
function _locStatsRecord(kind: "hit" | "miss" | "failure" | "ratelimit", errorMsg?: string) {
  const now = Date.now();
  _locStats.lastRequestAt = now;
  _locStats.recentTs.push(now);
  if (kind === "hit")       _locStats.cacheHits++;
  else if (kind === "miss") _locStats.cacheMisses++;
  else if (kind === "failure") {
    _locStats.failures++;
    _locStats.lastFailureAt = now;
    _locStats.lastFailureMsg = String(errorMsg ?? "").slice(0, 240);
  } else if (kind === "ratelimit") {
    _locStats.rateLimitHits++;
    _locStats.lastFailureAt = now;
    _locStats.lastFailureMsg = "rate limit queue full";
  }
  _locStatsPrune();
}

// ── Logged fetch: wraps fetch() and logs the request to api_request_log ──
async function loggedFetch(service: string, url: string, init?: RequestInit & { context?: string }): Promise<Response> {
  if (_apiKillSwitch) {
    const cleanUrl = url.replace(/token=[^&]+/g, "token=***").replace(/key=[^&]+/g, "key=***").replace(/apikey=[^&]+/g, "apikey=***");
    logApiRequest({ service, endpoint: cleanUrl, method: init?.method ?? "GET", statusCode: 0, success: false, durationMs: 0, errorMessage: "BLOCKED — API kill switch active", context: init?.context }).catch(() => {});
    throw new Error("API kill switch is active — all outgoing requests blocked");
  }
  const start = Date.now();
  const method = init?.method ?? "GET";
  const context = init?.context;
  // Strip context from init before passing to real fetch
  const { context: _ctx, ...fetchInit } = (init ?? {}) as any;
  // Strip query params with tokens for safety
  const cleanUrl = url.replace(/token=[^&]+/g, "token=***").replace(/key=[^&]+/g, "key=***").replace(/apikey=[^&]+/g, "apikey=***");
  try {
    const r = await fetch(url, fetchInit);
    const ms = Date.now() - start;
    // Fire-and-forget log
    logApiRequest({ service, endpoint: cleanUrl, method, statusCode: r.status, success: r.ok, durationMs: ms, errorMessage: r.ok ? undefined : `HTTP ${r.status}`, context }).catch(() => {});
    return r;
  } catch (err: any) {
    const ms = Date.now() - start;
    logApiRequest({ service, endpoint: cleanUrl, method, statusCode: 0, success: false, durationMs: ms, errorMessage: err?.message ?? String(err), context }).catch(() => {});
    throw err;
  }
}

// ── Clerk JWT verification via JWKS ──────────────────────────────────────
// Derive Clerk issuer URL from AUTH_PK (publishable key) so no extra env var is needed
const clerkIssuer = (() => {
  const pk = process.env.AUTH_PK ?? "";
  if (!pk) return "";
  try {
    const domain = Buffer.from(pk.replace(/^pk_(test|live)_/, ""), "base64").toString().replace(/\$$/, "");
    return `https://${domain}`;
  } catch { return ""; }
})();
const JWKS = clerkIssuer
  ? createRemoteJWKSet(new URL(`${clerkIssuer}/.well-known/jwks.json`))
  : null;

async function getClerkUserId(req: express.Request): Promise<string | null> {
  const auth = req.headers.authorization as string | undefined;
  if (!auth?.startsWith("Bearer ")) return null;
  if (JWKS) {
    try {
      const { payload } = await jwtVerify(auth.slice(7), JWKS);
      return (payload.sub as string) ?? null;
    } catch { return null; }
  }
  // Fallback: decode without verification (only when CLERK_ISSUER_URL not set)
  try {
    const b64 = auth.slice(7).split(".")[1];
    const { sub } = JSON.parse(Buffer.from(b64, "base64").toString());
    return sub ?? null;
  } catch { return null; }
}

/** Resolve a Discogs token for an authenticated request. OAuth → PAT → null.
 *  No shared-token fallback — invite-only sites require every caller to have
 *  brought their own auth. */
async function getTokenForRequest(req: express.Request): Promise<string | null> {
  const userId = await getClerkUserId(req);
  if (!userId) return null;
  const userToken = await getUserToken(userId);
  if (userToken && userToken !== "__oauth__") return userToken;
  return null;
}

async function getDiscogsForRequest(req: express.Request): Promise<DiscogsClient | null> {
  const userId = await getClerkUserId(req);
  if (!userId) return null;
  // Check if user has OAuth credentials first
  if (discogsConsumerKey) {
    const oauth = await getOAuthCredentials(userId);
    if (oauth) {
      return new DiscogsClient({
        consumerKey: discogsConsumerKey,
        consumerSecret: discogsConsumerSecret,
        accessToken: oauth.accessToken,
        accessSecret: oauth.accessSecret,
      });
    }
  }
  // Fall back to PAT flow
  const t = await getTokenForRequest(req);
  if (!t) return null;
  return new DiscogsClient(t);
}

/** Gate helper: returns the Clerk userId for a signed-in caller, or null
 *  after sending a 401 response. Use at the top of every endpoint that
 *  requires a logged-in user.
 *
 *    const userId = await requireUser(req, res);
 *    if (!userId) return;
 *
 *  SeaDisco is invite-only via Clerk's waitlist — Clerk is the sole
 *  gatekeeper of who has an account. Any valid Clerk session is allowed. */
async function requireUser(req: express.Request, res: express.Response): Promise<string | null> {
  const userId = await getClerkUserId(req);
  if (!userId) {
    res.status(401).json({ error: "auth_required", message: "Sign in to use SeaDisco." });
    return null;
  }
  return userId;
}

/** Admin gate — same shape as requireUser but additionally checks
 *  ADMIN_CLERK_ID. Returns 401 for missing/expired auth (so apiFetch
 *  can retry with a fresh token) and 403 for valid auth that's not
 *  the admin user. */
async function requireAdmin(req: express.Request, res: express.Response): Promise<string | null> {
  const userId = await getClerkUserId(req);
  if (!userId) {
    res.status(401).json({ error: "auth_required" });
    return null;
  }
  if (!ADMIN_CLERK_ID || userId !== ADMIN_CLERK_ID) {
    res.status(403).json({ error: "forbidden" });
    return null;
  }
  return userId;
}

/** Build a DiscogsClient for a userId (outside of an HTTP request context).
 *  Checks OAuth first, then PAT. Returns null if user has no valid auth. */
async function getDiscogsClientForUser(userId: string): Promise<DiscogsClient | null> {
  if (discogsConsumerKey) {
    const oauth = await getOAuthCredentials(userId);
    if (oauth) {
      return new DiscogsClient({
        consumerKey: discogsConsumerKey,
        consumerSecret: discogsConsumerSecret,
        accessToken: oauth.accessToken,
        accessSecret: oauth.accessSecret,
      });
    }
  }
  const token = await getUserToken(userId);
  if (token && token !== "__oauth__") return new DiscogsClient(token);
  return null;
}

// Boot DB if a connection string is configured
if (process.env.APP_DB_URL) {
  initDb().catch(err => console.error("DB init failed:", err));
}

const app = express();
app.set("trust proxy", 1); // trust exactly 1 hop (Railway's reverse proxy)

// Gzip/brotli compression for all responses
app.use(compression());

// Redirect old /account URL to SPA view so existing links/bookmarks still work
app.get("/account", (req, res) => {
  const qs = req.url.includes("?") ? "&" + req.url.split("?")[1] : "";
  res.redirect(301, `/?v=account${qs}`);
});

// ── HTML template cache with Clerk script preload injection ───────────────
// Reads index.html/admin.html at startup and substitutes
// <!--CLERK_SCRIPT_INJECT--> with a preloaded <script async> tag for
// clerk-js. This lets the Clerk bundle start downloading before shared.js
// even parses, saving ~300–500ms on cold page loads.
const _htmlCache = new Map<string, string>();
function _buildClerkInject(): string {
  if (!authPk) return "";
  try {
    const host = Buffer.from(authPk.replace(/^pk_(test|live)_/, ""), "base64")
      .toString("utf8")
      .replace(/\$+$/, "");
    if (!host || !/^[a-z0-9.-]+$/i.test(host)) return "";
    return `<script async crossorigin="anonymous" data-clerk-publishable-key="${authPk}" src="https://${host}/npm/@clerk/clerk-js@latest/dist/clerk.browser.js" onload="window._clerkScriptReady=true"></script>`;
  } catch { return ""; }
}
const _clerkInject = _buildClerkInject();
function _loadHtmlTemplated(relPath: string): string | null {
  const cached = _htmlCache.get(relPath);
  if (cached) return cached;
  try {
    const full = path.join(__dirname, "../web", relPath);
    let html = fs.readFileSync(full, "utf8");
    html = html.replace(/<!--CLERK_SCRIPT_INJECT-->/g, _clerkInject);
    _htmlCache.set(relPath, html);
    return html;
  } catch { return null; }
}
function _sendHtml(res: express.Response, relPath: string) {
  const html = _loadHtmlTemplated(relPath);
  if (!html) return false;
  res.setHeader("Cache-Control", "no-cache, must-revalidate");
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(html);
  return true;
}

// Serve the main HTML pages with Clerk script inlined in <head>.
// Must come BEFORE express.static so the static handler doesn't intercept.
app.get("/", (_req, res, next) => { if (!_sendHtml(res, "index.html")) next(); });
app.get("/index.html", (_req, res, next) => { if (!_sendHtml(res, "index.html")) next(); });
// Legacy /account.html URL: the standalone account page was merged into the
// SPA (see account.js). Redirect old bookmarks to the SPA account view.
app.get("/account.html", (_req, res) => { res.redirect(301, "/?v=account"); });
app.get("/admin.html", (_req, res, next) => { if (!_sendHtml(res, "admin.html")) next(); });

// Cache headers for static assets (versioned files get long cache, HTML short)
app.use(express.static(path.join(__dirname, "../web"), {
  extensions: ["html"],
  setHeaders(res, filePath) {
    if (/\.(js|css|webp|png|ico|woff2?)$/i.test(filePath)) {
      res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
    } else if (/\.html$/i.test(filePath)) {
      res.setHeader("Cache-Control", "no-cache, must-revalidate");
    }
  },
}));

// Cookie parser (for OAuth CSRF state)
app.use(cookieParser());

// CORS — restrict to known origins
const ALLOWED_ORIGINS = new Set([
  "https://seadisco.com",
  "https://www.seadisco.com",
]);
if (process.env.NODE_ENV !== "production") {
  ALLOWED_ORIGINS.add("http://localhost:3000");
  ALLOWED_ORIGINS.add("http://localhost:5173");
}
app.use((req, res, next) => {
  const origin = req.headers.origin as string | undefined;
  if (origin && ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") { res.sendStatus(204); return; }
  next();
});

// Security headers
app.use((_req, res, next) => {
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
  next();
});

// ── Auth / account endpoints ──────────────────────────────────────────────

// GET /api/config — public config for the frontend (always invite-only)
app.get("/api/config", (_req, res) => {
  res.setHeader("Cache-Control", "public, max-age=600"); // 10 min
  res.json({ clerkPublishableKey: authPk, authEnabled: true });
});

// GET /api/me — returns whether the current Clerk session is the admin.
// Used by the footer to reveal the Admin link only for the admin user.
app.get("/api/me", async (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  const userId = await getClerkUserId(req);
  if (!userId) { res.json({ signedIn: false, isAdmin: false }); return; }
  const adminId = ADMIN_CLERK_ID;
  res.json({ signedIn: true, isAdmin: !!adminId && userId === adminId });
});

// GET /api/user-count — admin-only (cap is internal, never advertised)
app.get("/api/user-count", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  try {
    const count = await getActiveUserCount();
    res.json({ count, limit: MAX_USERS });
  } catch { res.json({ count: 0, limit: MAX_USERS }); }
});

// GET /api/user/token — returns whether the user has a token saved + auth method
app.get("/api/user/token", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }

  // Check if this user is hibernated
  const hibernated = await isUserHibernated(userId);
  if (hibernated) {
    const activeCount = await getActiveUserCount();
    if (activeCount >= MAX_USERS) {
      res.status(403).json({ error: "hibernated", message: `Your account is hibernated due to inactivity. All ${MAX_USERS} spots are currently full. Please try again later.` });
      return;
    }
    // There's room — reactivate automatically
    await reactivateUser(userId);
  }

  // Touch activity
  const existingToken = await getUserToken(userId);
  if (existingToken) touchUserActivity(userId).catch(() => {});

  const [t, oauthCreds] = await Promise.all([
    getUserToken(userId),
    getOAuthCredentials(userId),
  ]);
  const hasPat = !!t && t !== "__oauth__";
  const hasOAuth = !!oauthCreds;
  res.json({
    hasToken: hasPat || hasOAuth,
    masked: hasPat ? `****${t!.slice(-4)}` : null,
    authMethod: hasOAuth ? "oauth" : (hasPat ? "pat" : null),
    oauthEnabled: !!discogsConsumerKey,
  });
});

// POST /api/user/token — save user's Discogs personal access token
app.post("/api/user/token", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const { token } = req.body ?? {};
  if (!token || typeof token !== "string" || token.trim().length < 8) {
    res.status(400).json({ error: "Invalid token" }); return;
  }
  // Check user cap for new users
  const existingToken = await getUserToken(userId);
  if (!existingToken) {
    const count = await getActiveUserCount();
    if (count >= MAX_USERS) {
      res.status(403).json({ error: `User limit reached (${MAX_USERS}). New registrations are currently closed.` });
      return;
    }
  }
  await setUserToken(userId, token.trim());
  // Fetch Discogs username and profile using the user's token
  try {
    const identRes = await loggedFetch("discogs", "https://api.discogs.com/oauth/identity", {
      headers: { "Authorization": `Discogs token=${token.trim()}`, "User-Agent": "SeaDisco/1.0" },
      context: "save-token identity check",
    });
    if (identRes.ok) {
      const ident = await identRes.json() as { username?: string; id?: number };
      if (ident.username) {
        await setDiscogsUsername(userId, ident.username);
        // Also fetch and cache the full profile
        try {
          const profileRes = await loggedFetch("discogs", `https://api.discogs.com/users/${encodeURIComponent(ident.username)}`, {
            headers: { "Authorization": `Discogs token=${token.trim()}`, "User-Agent": "SeaDisco/1.0" },
            context: "save-token profile fetch",
          });
          if (profileRes.ok) {
            const profile = await profileRes.json() as any;
            await setDiscogsProfile(userId, profile.id ?? ident.id ?? 0, profile.avatar_url ?? "", _extractDiscogsProfile(profile));
          }
        } catch {}
      }
    }
  } catch {}
  res.json({ ok: true });
});

// DELETE /api/user/token — remove user's saved token
app.delete("/api/user/token", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  await deleteUserToken(userId);
  res.json({ ok: true });
});

// DELETE /api/user/account — wipe all user data from our DB (Clerk deletion handled client-side)
app.delete("/api/user/account", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  await deleteUserData(userId);
  res.json({ ok: true });
});

// ── OAuth 1.0a endpoints ─────────────────────────────────────────────────

// GET /api/auth/discogs/start — initiate OAuth flow (requires Clerk auth + clerk_user_id in query)
app.get("/api/auth/discogs/start", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  if (!discogsConsumerKey || !discogsConsumerSecret) {
    res.status(500).json({ error: "OAuth not configured" }); return;
  }
  try {
    // Step 1: Get request token from Discogs
    const requestTokenUrl = "https://api.discogs.com/oauth/request_token";
    const proto = req.get("x-forwarded-proto") || req.protocol;
    const callbackUrl = `${proto}://${req.get("host")}/api/auth/discogs/callback`;
    const authHeader = signOAuthRequest("POST", requestTokenUrl, discogsConsumerKey, discogsConsumerSecret, undefined, undefined, undefined, callbackUrl);
    const rtRes = await loggedFetch("discogs", requestTokenUrl, {
      method: "POST",
      headers: {
        "Authorization": authHeader,
        "User-Agent": "SeaDisco/1.0",
      },
      context: "oauth-request-token",
    });
    if (!rtRes.ok) {
      console.error("OAuth request token failed:", rtRes.status);
      res.status(500).json({ error: "Failed to start OAuth flow" }); return;
    }
    const body = await rtRes.text();
    const params = new URLSearchParams(body);
    const oauthToken = params.get("oauth_token") ?? "";
    const oauthTokenSecret = params.get("oauth_token_secret") ?? "";
    if (!oauthToken || !oauthTokenSecret) {
      res.status(500).json({ error: "Invalid response from Discogs" }); return;
    }
    // Store request token with CSRF state
    const csrfState = crypto.randomBytes(24).toString("hex");
    await storeOAuthRequestToken(oauthToken, oauthTokenSecret, userId, csrfState);
    res.cookie("oauth_state", csrfState, { httpOnly: true, secure: true, sameSite: "lax", maxAge: 600_000 });
    // Return the authorize URL for the frontend to redirect to
    res.json({ authorizeUrl: `https://www.discogs.com/oauth/authorize?oauth_token=${oauthToken}` });
  } catch (e) {
    console.error("OAuth start error:", (e as Error)?.message);
    res.status(500).json({ error: "OAuth flow failed" });
  }
});

// GET /api/auth/discogs/callback — Discogs redirects here after user authorizes
app.get("/api/auth/discogs/callback", async (req, res) => {
  const oauthToken = req.query.oauth_token as string;
  const oauthVerifier = req.query.oauth_verifier as string;
  if (!oauthToken || !oauthVerifier) {
    res.status(400).send("Missing OAuth parameters. <a href='/?v=account'>Return to account</a>"); return;
  }
  try {
    // Look up the request token secret and clerk user ID
    const stored = await getOAuthRequestToken(oauthToken);
    if (!stored) {
      res.status(400).send("OAuth session expired. <a href='/?v=account'>Try again</a>"); return;
    }
    // Validate CSRF state cookie
    const cookieState = (req.cookies as Record<string, string>)?.oauth_state;
    if (stored.csrfState && (!cookieState || cookieState !== stored.csrfState)) {
      res.status(403).send("State mismatch — possible CSRF. <a href='/?v=account'>Try again</a>"); return;
    }
    res.clearCookie("oauth_state");
    // Step 3: Exchange for access token
    const accessTokenUrl = "https://api.discogs.com/oauth/access_token";
    const authHeader = signOAuthRequest("POST", accessTokenUrl, discogsConsumerKey, discogsConsumerSecret, oauthToken, stored.tokenSecret, oauthVerifier);
    const atRes = await loggedFetch("discogs", accessTokenUrl, {
      method: "POST",
      headers: {
        "Authorization": authHeader,
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "SeaDisco/1.0",
      },
      context: "oauth-access-token",
    });
    if (!atRes.ok) {
      console.error("OAuth access token failed:", atRes.status);
      res.status(500).send("Failed to complete OAuth. <a href='/?v=account'>Try again</a>"); return;
    }
    const body = await atRes.text();
    const params = new URLSearchParams(body);
    const accessToken = params.get("oauth_token") ?? "";
    const accessSecret = params.get("oauth_token_secret") ?? "";
    if (!accessToken || !accessSecret) {
      res.status(500).send("Invalid access token response. <a href='/?v=account'>Try again</a>"); return;
    }
    // Clean up request token
    await deleteOAuthRequestToken(oauthToken);

    // Ensure the user has a row in user_tokens (they might not have saved a PAT yet)
    const existingToken = await getUserToken(stored.clerkUserId);
    if (!existingToken) {
      // Check user cap for new users
      const userCount = await getActiveUserCount();
      if (userCount >= MAX_USERS) {
        res.status(403).send(`User limit reached (${MAX_USERS}). New registrations are currently closed. <a href="/">Home</a>`);
        return;
      }
      // Create a placeholder row so OAuth columns have somewhere to live
      await setUserToken(stored.clerkUserId, "__oauth__");
    }

    // Store OAuth credentials
    await setOAuthCredentials(stored.clerkUserId, accessToken, accessSecret);

    // Fetch identity and profile using the new OAuth credentials
    const oauthClient = new DiscogsClient({
      consumerKey: discogsConsumerKey,
      consumerSecret: discogsConsumerSecret,
      accessToken,
      accessSecret,
    });
    try {
      const identUrl = "https://api.discogs.com/oauth/identity";
      const identRes = await loggedFetch("discogs", identUrl, {
        headers: oauthClient.buildHeaders("GET", identUrl),
        context: "oauth-identity",
      });
      if (identRes.ok) {
        const ident = await identRes.json() as { username?: string; id?: number };
        if (ident.username) {
          await setDiscogsUsername(stored.clerkUserId, ident.username);
          // Fetch full profile
          const profileUrl = `https://api.discogs.com/users/${encodeURIComponent(ident.username)}`;
          const profileRes = await loggedFetch("discogs", profileUrl, {
            headers: oauthClient.buildHeaders("GET", profileUrl),
            context: "oauth-profile",
          });
          if (profileRes.ok) {
            const profile = await profileRes.json() as any;
            await setDiscogsProfile(stored.clerkUserId, profile.id ?? ident.id ?? 0, profile.avatar_url ?? "", _extractDiscogsProfile(profile));
          }
        }
      }
    } catch (e) {
      console.error("OAuth profile fetch error:", (e as Error)?.message);
    }

    // Redirect back to account view in SPA
    res.redirect("/?v=account&oauth=success");
  } catch (e) {
    console.error("OAuth callback error:", (e as Error)?.message);
    res.status(500).send("OAuth error. <a href='/?v=account'>Try again</a>");
  }
});

// DELETE /api/auth/discogs/disconnect — remove OAuth credentials (keep PAT if present)
app.delete("/api/auth/discogs/disconnect", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  await clearOAuthCredentials(userId);
  res.json({ ok: true });
});

// GET /api/user/profile — returns cached Discogs profile. Lazily refreshes
// from Discogs when the cache is older than 1 hour (or absent) so stats
// like num_collection / num_for_sale stay fresh enough for the account
// dashboard without us having to sync on every pageview.
app.get("/api/user/profile", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  let profile = await getDiscogsProfile(userId);
  const stale = !profile.profileSyncedAt || (Date.now() - new Date(profile.profileSyncedAt).getTime() > 60 * 60 * 1000);
  if (stale && profile.username) {
    try {
      const client = await getDiscogsForRequest(req);
      if (client) {
        const url = `https://api.discogs.com/users/${encodeURIComponent(profile.username)}`;
        const r = await loggedFetch("discogs", url, { headers: client.buildHeaders("GET", url), context: "profile-lazy-refresh" });
        if (r.ok) {
          const fresh = await r.json() as any;
          await setDiscogsProfile(userId, fresh.id ?? profile.userId ?? 0, fresh.avatar_url ?? profile.avatarUrl ?? "", _extractDiscogsProfile(fresh));
          profile = await getDiscogsProfile(userId);
        }
      }
    } catch {}
  }
  res.json(profile);
});

// POST /api/user/profile/refresh — re-fetch profile from Discogs
app.post("/api/user/profile/refresh", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const username = await getDiscogsUsername(userId);
  if (!username) { res.status(400).json({ error: "No Discogs username" }); return; }
  try {
    const profileUrl = `https://api.discogs.com/users/${encodeURIComponent(username)}`;
    const profileClient = await getDiscogsForRequest(req);
    if (!profileClient) { res.status(400).json({ error: "No Discogs credentials" }); return; }
    const profileRes = await loggedFetch("discogs", profileUrl, {
      headers: profileClient.buildHeaders("GET", profileUrl),
      context: "profile-refresh",
    });
    if (!profileRes.ok) { res.status(500).json({ error: "Failed to fetch profile" }); return; }
    const profile = await profileRes.json() as any;
    await setDiscogsProfile(userId, profile.id ?? 0, profile.avatar_url ?? "", _extractDiscogsProfile(profile));
    const updated = await getDiscogsProfile(userId);
    res.json(updated);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// Prune expired OAuth request tokens every 10 minutes
setInterval(() => { pruneOAuthRequestTokens().catch(() => {}); }, 10 * 60 * 1000);


// Abort flag for stopping all syncs
let _syncAbort = false;
const SYNC_STALL_TIMEOUT = 5 * 60 * 1000; // 5 minutes with no progress = stalled

// Background sync worker — runs detached from the HTTP request
async function runBackgroundSync(userId: string, client: DiscogsClient, username: string, syncCollection: boolean, syncWantlist: boolean) {
  console.log(`Sync ${username}: starting full sync (collection=${syncCollection}, wantlist=${syncWantlist})`);
  // Build headers per-request via the client (handles both PAT and OAuth signing)
  const getHeaders = (url: string) => client.buildHeaders("GET", url);
  const delay = (ms: number) => new Promise(r => setTimeout(r, ms));
  let totalSynced = 0;
  let lastProgressAt = Date.now(); // tracks last time progress was made

  // Timeout guard — checks every 60s if sync has stalled
  let _syncDone = false;
  let _thisSyncAbort = false; // per-sync abort flag (set by stall guard)
  const stallGuard = setInterval(async () => {
    if (_syncDone) { clearInterval(stallGuard); return; }
    const stalledFor = Date.now() - lastProgressAt;
    if (stalledFor >= SYNC_STALL_TIMEOUT) {
      console.error(`Sync ${username}: STALLED — no progress for ${Math.round(stalledFor / 60000)}min, auto-aborting`);
      _thisSyncAbort = true;
      clearInterval(stallGuard);
      try {
        await updateSyncProgress(userId, "error", totalSynced, 0, `Stalled — no progress for ${Math.round(stalledFor / 60000)} minutes`);
      } catch {}
    }
  }, 60_000);

  // Fetch with retry — up to 5 attempts with exponential backoff, respects Discogs rate limit headers
  async function fetchWithRetry(url: string, retries = 5): Promise<Response> {
    const backoffs = [15000, 30000, 60000, 90000, 120000];
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const r = await loggedFetch("discogs", url, { headers: getHeaders(url), signal: AbortSignal.timeout(30000), context: `sync ${username}` });
        if (r.ok) {
          // Check remaining rate limit — if low, pause proactively
          const remaining = parseInt(r.headers.get("x-discogs-ratelimit-remaining") ?? "10");
          if (remaining <= 1) {
            console.log(`Sync ${username}: rate limit nearly exhausted (${remaining} left), pausing 30s`);
            await delay(30000);
          } else if (remaining <= 5) {
            await delay(3000);
          }
          return r;
        }
        if (r.status === 429 || r.status >= 500) {
          const waitMs = backoffs[Math.min(attempt - 1, backoffs.length - 1)];
          console.warn(`Sync ${username}: HTTP ${r.status} on attempt ${attempt}/${retries}, retrying in ${waitMs / 1000}s`);
          if (attempt < retries) await delay(waitMs);
          else throw new Error(`HTTP ${r.status} after ${retries} attempts`);
        } else {
          throw new Error(`HTTP ${r.status}`); // 4xx non-retryable
        }
      } catch (err) {
        if (attempt === retries) throw err;
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
        const d = await r.json() as any;
        estimatedTotal += d.pagination?.items ?? 0;
        lastProgressAt = Date.now();
        await delay(500);
      } catch {}
    }
    if (syncWantlist) {
      try {
        const r = await fetchWithRetry(`https://api.discogs.com/users/${encodeURIComponent(username)}/wants?per_page=1&page=1`);
        const d = await r.json() as any;
        estimatedTotal += d.pagination?.items ?? 0;
        lastProgressAt = Date.now();
        await delay(500);
      } catch {}
    }

    console.log(`Sync ${username}: estimated total = ${estimatedTotal}`);
    await updateSyncProgress(userId, "syncing", 0, estimatedTotal);

    if (syncCollection) {
      const allInstanceIds: number[] = [];
      for (let page = 1; ; page++) {
        if (_syncAbort || _thisSyncAbort) { console.log(`Sync ${username}: aborted`); await updateSyncProgress(userId, "error", totalSynced, 0, "Aborted"); _syncDone = true; return; }
        if (page > 1) await delay(1200); // 1.2s pacing — Discogs allows 60/min
        const r = await fetchWithRetry(
          `https://api.discogs.com/users/${encodeURIComponent(username)}/collection/folders/0/releases?per_page=500&page=${page}&sort=added&sort_order=desc`
        );
        const data = await r.json() as any;
        const releases: any[] = data.releases ?? [];
        if (!releases.length) break;
        const items = releases.map((item: any) => ({
          id:         item.basic_information?.id as number,
          data:       item.basic_information as object,
          addedAt:    item.date_added ? new Date(item.date_added) : undefined,
          folderId:   item.folder_id ?? 0,
          rating:     item.rating ?? 0,
          instanceId: item.instance_id ?? undefined,
          notes:      item.notes ?? undefined,
        })).filter(i => i.id);

        await upsertCollectionItems(userId, items);
        // Track every instance key we saw (synthetic -releaseId for items missing instance_id)
        for (const i of items) allInstanceIds.push(i.instanceId ?? -i.id);
        totalSynced += items.length;
        lastProgressAt = Date.now();
        await updateSyncProgress(userId, "syncing", totalSynced, estimatedTotal);
        if (releases.length < 500) break;
      }
      // Remove local instances that are no longer in Discogs collection
      if (allInstanceIds.length > 0) {
        const pruned = await pruneCollectionItems(userId, allInstanceIds);
        if (pruned > 0) console.log(`Sync ${username}: pruned ${pruned} stale collection instances`);
      }
      await updateCollectionSyncedAt(userId);

      // Sync folder list
      try {
        await delay(1000);
        const fr = await fetchWithRetry(`https://api.discogs.com/users/${encodeURIComponent(username)}/collection/folders`);
        const fd = await fr.json() as any;
        const folders = (fd.folders ?? [])
          .filter((f: any) => f.id !== 0) // skip the virtual "All" folder
          .map((f: any) => ({ id: f.id as number, name: f.name as string, count: f.count as number }));
        if (folders.length) await upsertCollectionFolders(userId, folders);
        lastProgressAt = Date.now();
      } catch { /* folder sync optional */ }
    }

    if (syncWantlist) {
      const allWantlistIds: number[] = [];
      for (let page = 1; ; page++) {
        if (_syncAbort || _thisSyncAbort) { console.log(`Sync ${username}: aborted`); await updateSyncProgress(userId, "error", totalSynced, 0, "Aborted"); _syncDone = true; return; }
        if (page > 1) await delay(1200);
        const r = await fetchWithRetry(
          `https://api.discogs.com/users/${encodeURIComponent(username)}/wants?per_page=500&page=${page}&sort=added&sort_order=desc`
        );
        const data = await r.json() as any;
        const wants: any[] = data.wants ?? [];
        if (!wants.length) break;
        const items = wants.map((item: any) => ({
          id:      item.id as number,
          data:    item.basic_information as object,
          addedAt: item.date_added ? new Date(item.date_added) : undefined,
          rating:  item.rating ?? 0,
          notes:   item.notes ?? undefined,
        })).filter(i => i.id);

        await upsertWantlistItems(userId, items);
        allWantlistIds.push(...items.map(i => i.id));
        totalSynced += items.length;
        lastProgressAt = Date.now();
        await updateSyncProgress(userId, "syncing", totalSynced, estimatedTotal);
        if (wants.length < 500) break;
      }
      // Remove local items that are no longer in Discogs wantlist
      if (allWantlistIds.length > 0) {
        const pruned = await pruneWantlistItems(userId, allWantlistIds);
        if (pruned > 0) console.log(`Sync ${username}: pruned ${pruned} stale wantlist items`);
      }
      await updateWantlistSyncedAt(userId);
    }

    await updateSyncProgress(userId, "complete", totalSynced, estimatedTotal);
    console.log(`Full sync complete for ${username}: ${totalSynced} items`);
  } catch (err) {
    console.error(`Background sync error for ${username}:`, err);
    await updateSyncProgress(userId, "error", totalSynced, 0, String(err));
  } finally {
    _syncDone = true;
    clearInterval(stallGuard);
  }
}

// POST /api/user/sync — kick off background sync of collection, wantlist, inventory & lists
app.post("/api/user/sync", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }

  const { type = "both" } = req.body ?? {};
  const syncCollection = type === "collection" || type === "both";
  const syncWantlist   = type === "wantlist"   || type === "both";

  // Check cooldown: skip if synced within last 5 minutes
  const syncStatus = await getSyncStatus(userId);
  const cooldownAgo = new Date(Date.now() - 5 * 60 * 1000);
  const collectionRecent = syncCollection && !!syncStatus.collectionSyncedAt && syncStatus.collectionSyncedAt > cooldownAgo;
  const wantlistRecent   = syncWantlist   && !!syncStatus.wantlistSyncedAt   && syncStatus.wantlistSyncedAt   > cooldownAgo;
  if (collectionRecent && wantlistRecent) {
    res.json({ ok: true, skipped: true, reason: "Recently synced" });
    return;
  }

  // If already syncing, don't start another
  if (syncStatus.syncStatus === "syncing") {
    res.json({ ok: true, syncing: true, progress: syncStatus.syncProgress, total: syncStatus.syncTotal });
    return;
  }

  const discogsClient = await getDiscogsForRequest(req);
  if (!discogsClient) { res.status(400).json({ error: "No Discogs credentials found" }); return; }

  let username = await getDiscogsUsername(userId);
  if (!username) {
    try {
      const identUrl = "https://api.discogs.com/oauth/identity";
      const identRes = await loggedFetch("discogs", identUrl, {
        headers: discogsClient.buildHeaders("GET", identUrl),
        context: "sync identity check",
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

  // Respond immediately, sync runs in background
  res.json({ ok: true, started: true });

  // Fire and forget — runs in background (full sync for user-initiated)
  (async () => {
    try {
      await runBackgroundSync(userId, discogsClient, username, syncCollection && !collectionRecent, syncWantlist && !wantlistRecent);
      // Also sync inventory & lists after collection/wantlist
      await syncUserExtras(userId, username, discogsClient);
      console.log(`[user-sync] ${username}: extras sync complete`);
    } catch (err) {
      console.error("Background sync uncaught error:", err);
    }
  })();
});

// GET /api/user/collection — paginated cached collection (with optional filters)
app.get("/api/user/collection", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const page    = parseInt(req.query.page    as string) || 1;
  const perPage = parseInt(req.query.per_page as string) || 25;
  const filters: Record<string, any> = {};
  for (const key of ["q", "artist", "release", "label", "year", "genre", "style", "format", "type"]) {
    const v = (req.query[key] as string ?? "").trim();
    if (v) filters[key] = v;
  }
  const folderId = parseInt(req.query.folderId as string ?? "", 10);
  if (folderId > 0) filters.folderId = folderId;
  const ratingParam = (req.query.rating as string ?? "").trim();
  if (ratingParam === "unrated") filters.ratingUnrated = true;
  else if (ratingParam) { const rm = parseInt(ratingParam, 10); if (rm >= 1 && rm <= 5) filters.ratingMin = rm; }
  const notesParam = (req.query.notes as string ?? "").trim();
  if (notesParam) filters.notes = notesParam;
  const sort = (req.query.sort as string ?? "").trim();
  if (sort) filters.sort = sort;
  if (req.query.synonyms === "false") filters.synonyms = false;
  const { items, total, synonymsApplied } = await getCollectionPage(userId, page, perPage, Object.keys(filters).length ? filters : undefined);
  res.json({ items, total, page, pages: Math.ceil(total / perPage), synonymsApplied });
});

// GET /api/user/wantlist — paginated cached wantlist (with optional filters)
app.get("/api/user/wantlist", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const page    = parseInt(req.query.page    as string) || 1;
  const perPage = parseInt(req.query.per_page as string) || 25;
  const filters: Record<string, any> = {};
  for (const key of ["q", "artist", "release", "label", "year", "genre", "style", "format", "type"]) {
    const v = (req.query[key] as string ?? "").trim();
    if (v) filters[key] = v;
  }
  const ratingParam = (req.query.rating as string ?? "").trim();
  if (ratingParam === "unrated") filters.ratingUnrated = true;
  else if (ratingParam) { const rm = parseInt(ratingParam, 10); if (rm >= 1 && rm <= 5) filters.ratingMin = rm; }
  const notesParam = (req.query.notes as string ?? "").trim();
  if (notesParam) filters.notes = notesParam;
  const sort = (req.query.sort as string ?? "").trim();
  if (sort) filters.sort = sort;
  if (req.query.synonyms === "false") filters.synonyms = false;
  const { items, total, synonymsApplied } = await getWantlistPage(userId, page, perPage, Object.keys(filters).length ? filters : undefined);
  res.json({ items, total, page, pages: Math.ceil(total / perPage), synonymsApplied });
});

// GET /api/user/inventory — paginated inventory listings
app.get("/api/user/inventory", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const page    = parseInt(req.query.page    as string) || 1;
  const perPage = parseInt(req.query.per_page as string) || 24;
  const filters: Record<string, any> = {};
  const q = (req.query.q as string ?? "").trim();
  if (q) filters.q = q;
  const status = (req.query.status as string ?? "").trim();
  if (status) filters.status = status;
  if (req.query.synonyms === "false") filters.synonyms = false;
  const { items, total, synonymsApplied } = await getInventoryPage(userId, page, perPage, Object.keys(filters).length ? filters : undefined);
  res.json({ items, total, page, pages: Math.ceil(total / perPage), synonymsApplied });
});

// ── Inventory (marketplace) management: create / edit / delete ────────────
// Helper: fetch a single listing from Discogs and mirror into user_inventory.
async function refreshInventoryListing(
  userId: string, client: DiscogsClient, listingId: number
): Promise<any | null> {
  const url = `https://api.discogs.com/marketplace/listings/${listingId}`;
  const r = await loggedFetch("discogs", url, { method: "GET", headers: client.buildHeaders("GET", url), context: "inventory-refresh" });
  if (!r.ok) return null;
  const data = await r.json() as any;
  const releaseId = data?.release?.id ? Number(data.release.id) : undefined;
  const priceValue = data?.price?.value != null ? Number(data.price.value) : undefined;
  const priceCurrency = data?.price?.currency ?? "USD";
  const postedAt = data?.posted ? new Date(data.posted) : undefined;
  await upsertInventoryItems(userId, [{
    listingId,
    releaseId,
    data,
    status: data?.status ?? "For Sale",
    priceValue,
    priceCurrency,
    condition: data?.condition ?? undefined,
    sleeveCondition: data?.sleeve_condition ?? undefined,
    postedAt,
  }]);
  return data;
}

// Build the JSON body Discogs expects for create/edit listing.
function buildListingBody(body: any): { ok: true; payload: any } | { ok: false; error: string } {
  const required = ["releaseId", "condition", "sleeveCondition", "price", "status"];
  for (const k of required) if (body?.[k] == null || body?.[k] === "") return { ok: false, error: `${k} required` };
  const payload: any = {
    release_id:       Number(body.releaseId),
    condition:        String(body.condition),
    sleeve_condition: String(body.sleeveCondition),
    price:            Number(body.price),
    status:           String(body.status),
  };
  // Treat empty strings as "unset" so we don't send 0 for numeric optional fields.
  const has = (v: any) => v != null && v !== "";
  if (has(body.comments))       payload.comments        = String(body.comments);
  if (body.allowOffers != null) payload.allow_offers    = !!body.allowOffers;
  if (has(body.externalId))     payload.external_id     = String(body.externalId);
  if (has(body.location))       payload.location        = String(body.location);
  if (has(body.weight)) {
    payload.weight = body.weight === "auto" ? "auto" : Number(body.weight);
    if (typeof payload.weight === "number" && !Number.isFinite(payload.weight)) delete payload.weight;
  }
  if (has(body.formatQuantity)) {
    const n = Number(body.formatQuantity);
    if (Number.isFinite(n) && n > 0) payload.format_quantity = n;
  }
  if (!Number.isFinite(payload.release_id) || payload.release_id < 1) return { ok: false, error: "Invalid releaseId" };
  if (!Number.isFinite(payload.price) || payload.price < 0) return { ok: false, error: "Invalid price" };
  return { ok: true, payload };
}

// POST /api/user/inventory/create — create a new marketplace listing
app.post("/api/user/inventory/create", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const built = buildListingBody(req.body);
  if (!built.ok) { res.status(400).json({ error: built.error }); return; }
  try {
    const url = "https://api.discogs.com/marketplace/listings";
    const r = await loggedFetch("discogs", url, {
      method: "POST",
      headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" },
      body: JSON.stringify(built.payload),
      context: "inventory-create",
    });
    if (!r.ok) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    const created = await r.json() as any;
    const listingId = Number(created?.listing_id);
    if (!listingId) { res.status(502).json({ error: "Discogs create did not return a listing_id" }); return; }
    // Discogs create returns only the ID + URI; fetch the full listing for the cache.
    await _sleep(DISCOGS_CALL_DELAY_MS);
    const full = await refreshInventoryListing(ctx.userId, ctx.client, listingId);
    res.json({ ok: true, listingId, item: full });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/inventory/refresh — sync only inventory, respond when done.
// MUST be registered before the POST /:listingId route because Express matches
// the first pattern that fits — otherwise "refresh" would be parsed as a listingId.
app.post("/api/user/inventory/refresh", async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  try {
    const count = await syncInventoryOnly(ctx.userId, ctx.username, ctx.client);
    res.json({ ok: true, count });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/inventory/:listingId — edit an existing listing (Discogs uses POST)
app.post("/api/user/inventory/:listingId", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const listingId = Number(req.params.listingId);
  if (!Number.isFinite(listingId) || listingId < 1) { res.status(400).json({ error: "Invalid listingId" }); return; }
  const built = buildListingBody(req.body);
  if (!built.ok) { res.status(400).json({ error: built.error }); return; }
  try {
    const url = `https://api.discogs.com/marketplace/listings/${listingId}`;
    const r = await loggedFetch("discogs", url, {
      method: "POST",
      headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" },
      body: JSON.stringify(built.payload),
      context: "inventory-edit",
    });
    if (!r.ok && r.status !== 204) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    await _sleep(DISCOGS_CALL_DELAY_MS);
    const full = await refreshInventoryListing(ctx.userId, ctx.client, listingId);
    res.json({ ok: true, item: full });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// DELETE /api/user/inventory/:listingId — remove a listing
app.delete("/api/user/inventory/:listingId", async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const listingId = Number(req.params.listingId);
  if (!Number.isFinite(listingId) || listingId < 1) { res.status(400).json({ error: "Invalid listingId" }); return; }
  try {
    const url = `https://api.discogs.com/marketplace/listings/${listingId}`;
    const r = await loggedFetch("discogs", url, {
      method: "DELETE",
      headers: ctx.client.buildHeaders("DELETE", url),
      context: "inventory-delete",
    });
    // Treat 404 as "already gone" — the listing was deleted on Discogs
    // (manually, expired, or already removed in a prior call) but our
    // local cache still had a stale row. Sync local state and report
    // success so the UI can move on instead of leaving a phantom row.
    let alreadyGone = false;
    if (r.status === 404) {
      alreadyGone = true;
    } else if (!r.ok && r.status !== 204) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    await deleteInventoryItem(ctx.userId, listingId);
    res.json({ ok: true, alreadyGone });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/inventory/:listingId — single listing detail for the edit modal
app.get("/api/user/inventory/:listingId", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const listingId = Number(req.params.listingId);
  if (!Number.isFinite(listingId) || listingId < 1) { res.status(400).json({ error: "Invalid listingId" }); return; }
  try {
    let row = await getInventoryItem(userId, listingId);
    // If missing or older than 5 minutes, refresh from Discogs
    const stale = !row || !row.synced_at || (Date.now() - new Date(row.synced_at).getTime() > 5 * 60 * 1000);
    if (stale) {
      const client = await getDiscogsClientForUser(userId);
      if (client) {
        await refreshInventoryListing(userId, client, listingId);
        row = await getInventoryItem(userId, listingId);
      }
    }
    if (!row) { res.status(404).json({ error: "Listing not found" }); return; }
    res.json({ item: row });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/inventory/price-suggestions/:releaseId — Discogs price suggestions proxy
app.get("/api/user/inventory/price-suggestions/:releaseId", async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const releaseId = Number(req.params.releaseId);
  if (!Number.isFinite(releaseId) || releaseId < 1) { res.status(400).json({ error: "Invalid releaseId" }); return; }
  try {
    const url = `https://api.discogs.com/marketplace/price_suggestions/${releaseId}`;
    const r = await loggedFetch("discogs", url, { method: "GET", headers: ctx.client.buildHeaders("GET", url), context: "price-suggestions" });
    if (!r.ok) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    const data = await r.json() as any;
    res.json({ suggestions: data });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// ── Seller orders endpoints ───────────────────────────────────────────────
// Helper: refresh a single order from Discogs and upsert into local cache.
async function refreshOrder(userId: string, client: DiscogsClient, orderId: string) {
  const url = `https://api.discogs.com/marketplace/orders/${encodeURIComponent(orderId)}`;
  const r = await loggedFetch("discogs", url, { method: "GET", headers: client.buildHeaders("GET", url), context: "order-refresh" });
  if (!r.ok) return null;
  const o = await r.json() as any;
  await upsertUserOrders(userId, [{
    orderId:       String(o.id),
    status:        o.status ?? undefined,
    buyerUsername: o.buyer?.username ?? undefined,
    itemCount:    Array.isArray(o.items) ? o.items.length : undefined,
    totalValue:    parseFloat(o.total?.value) || undefined,
    totalCurrency: o.total?.currency ?? undefined,
    createdAt:     o.created ? new Date(o.created) : undefined,
    data:          o as object,
  }]);
  return o;
}

// Shared: single-phase sync for just the orders list. Runs inline and
// responds when finished (bounded: Discogs orders are small compared to
// collection). Reused by POST /api/user/orders/refresh.
async function syncOrdersOnly(userId: string, username: string, client: DiscogsClient): Promise<number> {
  let total = 0;
  for (let page = 1; ; page++) {
    if (page > 1) await sleep(DISCOGS_CALL_DELAY_MS);
    const url = `https://api.discogs.com/marketplace/orders?per_page=100&page=${page}&sort=last_activity&sort_order=desc`;
    const r = await loggedFetch("discogs", url, { headers: client.buildHeaders("GET", url), context: `orders-refresh: ${username}` });
    if (r.status === 401 || r.status === 403 || r.status === 404) break;
    if (!r.ok) throw new Error(`Discogs ${r.status}`);
    const data = await r.json() as any;
    const rows: any[] = data.orders ?? [];
    if (!rows.length) break;
    const mapped = rows.map((o: any) => ({
      orderId:       String(o.id),
      status:        o.status ?? undefined,
      buyerUsername: o.buyer?.username ?? undefined,
      itemCount:     Array.isArray(o.items) ? o.items.length : undefined,
      totalValue:    parseFloat(o.total?.value) || undefined,
      totalCurrency: o.total?.currency ?? undefined,
      createdAt:     o.created ? new Date(o.created) : undefined,
      data:          o as object,
    }));
    await upsertUserOrders(userId, mapped);
    total += mapped.length;
    if (rows.length < 100) break;
  }
  await updateOrdersSyncedAt(userId);
  return total;
}

// Shared: single-phase sync for just the inventory list.
async function syncInventoryOnly(userId: string, username: string, client: DiscogsClient): Promise<number> {
  let total = 0;
  for (let page = 1; ; page++) {
    if (page > 1) await sleep(DISCOGS_CALL_DELAY_MS);
    const url = `https://api.discogs.com/users/${encodeURIComponent(username)}/inventory?per_page=100&page=${page}&sort=listed&sort_order=desc`;
    const r = await loggedFetch("discogs", url, { headers: client.buildHeaders("GET", url), context: `inventory-refresh: ${username}` });
    if (r.status === 401 || r.status === 403) break;
    if (!r.ok) throw new Error(`Discogs ${r.status}`);
    const data = await r.json() as any;
    const listings: any[] = data.listings ?? [];
    if (!listings.length) break;
    const items = listings.map((l: any) => ({
      listingId:       l.id as number,
      releaseId:       l.release?.id ?? undefined,
      data:            l as object,
      status:          l.status ?? "For Sale",
      priceValue:      parseFloat(l.price?.value) || undefined,
      priceCurrency:   l.price?.currency ?? "USD",
      condition:       l.condition ?? undefined,
      sleeveCondition: l.sleeve_condition ?? undefined,
      postedAt:        l.posted ? new Date(l.posted) : undefined,
    }));
    await upsertInventoryItems(userId, items);
    total += items.length;
    if (listings.length < 100) break;
  }
  await updateInventorySyncedAt(userId);
  return total;
}

// POST /api/user/orders/refresh — sync only orders, respond when done
app.post("/api/user/orders/refresh", async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  try {
    const count = await syncOrdersOnly(ctx.userId, ctx.username, ctx.client);
    res.json({ ok: true, count });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/orders — paginated seller orders from local cache
app.get("/api/user/orders", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const page = Math.max(1, Number(req.query.page) || 1);
  const perPage = Math.min(200, Math.max(1, Number(req.query.per_page) || 20));
  const status = typeof req.query.status === "string" ? req.query.status : undefined;
  const q = typeof req.query.q === "string" ? req.query.q : undefined;
  try {
    const result = await getUserOrdersPage(userId, page, perPage, { status, q });
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/orders/count — lightweight visibility check for Account UI
app.get("/api/user/orders/count", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const count = await getOrdersCount(userId);
    res.json({ count });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/orders/unread-count — orders where last_activity > viewed_at
app.get("/api/user/orders/unread-count", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const count = await getUnreadOrdersCount(userId);
    res.json({ count });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/orders/:orderId/view — mark order as viewed (clears unread)
app.post("/api/user/orders/:orderId/view", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    await markOrderViewed(userId, String(req.params.orderId));
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/orders/:orderId — single order (refreshes from Discogs if stale)
app.get("/api/user/orders/:orderId", async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const orderId = String(req.params.orderId);
  try {
    const row = await getUserOrder(ctx.userId, orderId);
    const stale = !row || (row.synced_at && Date.now() - new Date(row.synced_at).getTime() > 5 * 60 * 1000);
    if (stale) {
      const fresh = await refreshOrder(ctx.userId, ctx.client, orderId);
      if (!fresh && !row) { res.status(404).json({ error: "Order not found" }); return; }
    }
    const final = await getUserOrder(ctx.userId, orderId);
    // Opening the order counts as a view — clears the unread indicator.
    await markOrderViewed(ctx.userId, orderId);
    res.json({ item: final });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/orders/:orderId/messages — fetch message thread (always fresh)
app.get("/api/user/orders/:orderId/messages", async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const orderId = String(req.params.orderId);
  try {
    const url = `https://api.discogs.com/marketplace/orders/${encodeURIComponent(orderId)}/messages`;
    const r = await loggedFetch("discogs", url, { method: "GET", headers: ctx.client.buildHeaders("GET", url), context: "order-messages" });
    if (!r.ok) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` });
      return;
    }
    const data = await r.json() as any;
    const messages: any[] = data.messages ?? [];
    const mapped = messages.map((m: any, i: number) => ({
      order: i,
      subject:      m.subject ?? undefined,
      message:      m.message ?? undefined,
      fromUser:     m.from?.username ?? m.from_user ?? undefined,
      ts:           m.timestamp ? new Date(m.timestamp) : undefined,
      data:         m as object,
    }));
    await upsertOrderMessages(ctx.userId, orderId, mapped);
    res.json({ messages: await getOrderMessages(ctx.userId, orderId) });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/orders/:orderId/status — update order status / shipping
app.post("/api/user/orders/:orderId/status", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const orderId = String(req.params.orderId);
  const body = req.body || {};
  const payload: any = {};
  if (body.status) payload.status = String(body.status);
  if (body.shipping != null) payload.shipping = Number(body.shipping);
  if (!payload.status && payload.shipping == null) {
    res.status(400).json({ error: "status or shipping required" }); return;
  }
  try {
    const url = `https://api.discogs.com/marketplace/orders/${encodeURIComponent(orderId)}`;
    const r = await loggedFetch("discogs", url, {
      method: "POST",
      headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      context: "order-update",
    });
    if (!r.ok) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    await sleep(DISCOGS_CALL_DELAY_MS);
    const fresh = await refreshOrder(ctx.userId, ctx.client, orderId);
    res.json({ ok: true, item: fresh });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/orders/:orderId/messages — send a message on the order
app.post("/api/user/orders/:orderId/messages", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const orderId = String(req.params.orderId);
  const body = req.body || {};
  if (!body.message) { res.status(400).json({ error: "message required" }); return; }
  const payload: any = { message: String(body.message) };
  if (body.subject) payload.subject = String(body.subject);
  if (body.status) payload.status = String(body.status);
  try {
    const url = `https://api.discogs.com/marketplace/orders/${encodeURIComponent(orderId)}/messages`;
    const r = await loggedFetch("discogs", url, {
      method: "POST",
      headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      context: "order-message",
    });
    if (!r.ok) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    // Re-fetch the full thread so local cache stays authoritative
    await sleep(DISCOGS_CALL_DELAY_MS);
    const mUrl = `https://api.discogs.com/marketplace/orders/${encodeURIComponent(orderId)}/messages`;
    const mr = await loggedFetch("discogs", mUrl, { method: "GET", headers: ctx.client.buildHeaders("GET", mUrl), context: "order-messages" });
    if (mr.ok) {
      const data = await mr.json() as any;
      const messages: any[] = data.messages ?? [];
      const mapped = messages.map((m: any, i: number) => ({
        order: i,
        subject:      m.subject ?? undefined,
        message:      m.message ?? undefined,
        fromUser:     m.from?.username ?? m.from_user ?? undefined,
        ts:           m.timestamp ? new Date(m.timestamp) : undefined,
        data:         m as object,
      }));
      await upsertOrderMessages(ctx.userId, orderId, mapped);
    }
    res.json({ ok: true, messages: await getOrderMessages(ctx.userId, orderId) });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/lists — user's Discogs lists
app.get("/api/user/lists", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const lists = await getUserListsList(userId);
  res.json({ lists });
});

// GET /api/user/lists/:id/items — items inside a specific list
app.get("/api/user/lists/:id/items", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const listId = parseInt(req.params.id, 10);
    if (isNaN(listId)) { res.status(400).json({ error: "Invalid list ID" }); return; }
    const items = await getListItems(userId, listId);
    res.json({ items });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/wanted — all community wantlist items, deduped and shuffled (requires login)
app.get("/api/wanted", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    res.setHeader("Cache-Control", "private, max-age=300"); // 5 min, auth-gated
    const items = await getWantedItems();
    res.json({ items });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/collection/export — download collection as CSV
app.get("/api/user/collection/export", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
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
  } catch (e) {
    res.status(500).json({ error: "Export failed" });
  }
});

// GET /api/user/wantlist/export — download wantlist as CSV
app.get("/api/user/wantlist/export", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const rows = await getAllWantlistItems(userId);
    const csv = buildCsv(rows, null);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", "attachment; filename=seadisco-wantlist.csv");
    res.send(csv);
  } catch (e) {
    res.status(500).json({ error: "Export failed" });
  }
});

function buildCsv(rows: any[], folderMap: Map<number, string> | null): string {
  const headers = ["Artist", "Title", "Label", "Cat#", "Year", "Format", "Genre", "Style", "Country"];
  if (folderMap) headers.push("Folder");
  const escCsv = (s: string) => {
    if (!s) return "";
    if (s.includes('"') || s.includes(",") || s.includes("\n")) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  };
  const lines = [headers.join(",")];
  for (const row of rows) {
    const d = row.data;
    const artist = (d.artists ?? []).map((a: any) => a.name).join(", ");
    const title  = d.title ?? "";
    const label  = (d.labels ?? []).map((l: any) => l.name).join(", ");
    const catno  = (d.labels ?? [])[0]?.catno ?? "";
    const year   = String(d.year ?? "");
    const format = (d.formats ?? []).map((f: any) => [f.name, ...(f.descriptions ?? [])].filter(Boolean).join(" ")).join("; ");
    const genre  = (d.genres ?? []).join(", ");
    const style  = (d.styles ?? []).join(", ");
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

// ── Phase 2: Collection / Wantlist actions ───────────────────────────────

// Helper: get Discogs username + authenticated client for the current user.
// Supports both OAuth and PAT auth by delegating to getDiscogsClientForUser.
async function requireUsernameAndToken(req: express.Request, res: express.Response): Promise<{ userId: string; username: string; client: DiscogsClient } | null> {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return null; }
  // Username lookup and client construction are independent DB reads — run in parallel.
  const [username, client] = await Promise.all([
    getDiscogsUsername(userId),
    getDiscogsClientForUser(userId),
  ]);
  if (!username) { res.status(400).json({ error: "No Discogs username — connect your account first" }); return null; }
  if (!client) { res.status(400).json({ error: "No Discogs credentials — connect your account first" }); return null; }
  return { userId, username, client };
}

// POST /api/user/collection/add — add release to collection
app.post("/api/user/collection/add", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { releaseId } = req.body ?? {};
  if (!releaseId) { res.status(400).json({ error: "releaseId required" }); return; }
  // Resolve folder: body.folderId wins; otherwise fall back to the user's default.
  let folderId = Number(req.body?.folderId);
  if (!Number.isFinite(folderId) || folderId < 1) {
    folderId = await getDefaultAddFolderId(ctx.userId);
  }
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/folders/${folderId}/releases/${releaseId}`;
    const r = await loggedFetch("discogs", url, { method: "POST", headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" }, context: "collection-add" });
    if (!r.ok) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    const data = await r.json() as any;
    // Update local DB
    await upsertCollectionItems(ctx.userId, [{
      id: releaseId,
      data: data.basic_information ?? {},
      addedAt: new Date(),
      folderId,
      rating: 0,
      instanceId: data.instance_id ?? null,
      notes: [],
    }]);
    // Look up folder name for the client-side toast
    let folderName: string | null = null;
    try {
      if (folderId === 1) folderName = "Uncategorized";
      else {
        const folders = await getCollectionFolderList(ctx.userId);
        folderName = folders.find((f: any) => Number(f.folderId) === folderId)?.name ?? null;
      }
    } catch {}
    res.json({ ok: true, instanceId: data.instance_id ?? null, folderId, folderName });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/settings/default-folder — returns the user's default add-to-collection folder id
app.get("/api/user/settings/default-folder", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const folderId = await getDefaultAddFolderId(userId);
    res.json({ folderId });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// PUT /api/user/settings/default-folder — update the user's default add-to-collection folder id
app.put("/api/user/settings/default-folder", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const folderId = Number(req.body?.folderId);
  if (!Number.isFinite(folderId) || folderId < 1) { res.status(400).json({ error: "folderId required (>=1)" }); return; }
  try {
    await setDefaultAddFolderId(userId, folderId);
    res.json({ ok: true, folderId });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/collection/remove — remove release from collection
app.post("/api/user/collection/remove", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { releaseId, instanceId, folderId = 1 } = req.body ?? {};
  if (!releaseId || !instanceId) { res.status(400).json({ error: "releaseId and instanceId required" }); return; }
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/folders/${folderId}/releases/${releaseId}/instances/${instanceId}`;
    const r = await loggedFetch("discogs", url, { method: "DELETE", headers: ctx.client.buildHeaders("DELETE", url), context: "collection-remove" });
    if (!r.ok && r.status !== 204) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    // Remove just this instance from local DB
    await deleteCollectionItem(ctx.userId, releaseId, instanceId);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/wantlist/add — add release to wantlist
app.post("/api/user/wantlist/add", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { releaseId, notes = "" } = req.body ?? {};
  if (!releaseId) { res.status(400).json({ error: "releaseId required" }); return; }
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/wants/${releaseId}`;
    const r = await loggedFetch("discogs", url, { method: "PUT", headers: { ...ctx.client.buildHeaders("PUT", url), "Content-Type": "application/json" }, body: notes ? JSON.stringify({ notes }) : undefined, context: "wantlist-add" });
    if (!r.ok) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    const data = await r.json() as any;
    // Update local DB
    await upsertWantlistItems(ctx.userId, [{
      id: releaseId,
      data: data.basic_information ?? {},
      addedAt: new Date(),
      rating: data.rating ?? 0,
      notes: data.notes ? [{ field_id: 0, value: data.notes }] : [],
    }]);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/wantlist/remove — remove release from wantlist
app.post("/api/user/wantlist/remove", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { releaseId } = req.body ?? {};
  if (!releaseId) { res.status(400).json({ error: "releaseId required" }); return; }
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/wants/${releaseId}`;
    const r = await loggedFetch("discogs", url, { method: "DELETE", headers: ctx.client.buildHeaders("DELETE", url), context: "wantlist-remove" });
    if (!r.ok && r.status !== 204) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    await deleteWantlistItem(ctx.userId, releaseId);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/collection/rating — set rating on a collection item
app.post("/api/user/collection/rating", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { releaseId, instanceId, folderId = 1, rating } = req.body ?? {};
  if (!releaseId || !instanceId || rating == null) { res.status(400).json({ error: "releaseId, instanceId, and rating required" }); return; }
  if (rating < 0 || rating > 5) { res.status(400).json({ error: "Rating must be 0-5" }); return; }
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/folders/${folderId}/releases/${releaseId}/instances/${instanceId}`;
    const r = await loggedFetch("discogs", url, { method: "POST", headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" }, body: JSON.stringify({ rating }), context: "collection-rating" });
    if (!r.ok && r.status !== 204) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    // Update local DB (instance-scoped)
    await updateCollectionRating(ctx.userId, releaseId, rating, instanceId);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/folders/create — create a new collection folder
app.post("/api/user/folders/create", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { name } = req.body ?? {};
  if (!name?.trim()) { res.status(400).json({ error: "Folder name required" }); return; }
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/folders`;
    const r = await loggedFetch("discogs", url, { method: "POST", headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" }, body: JSON.stringify({ name: name.trim() }), context: "folder-create" });
    if (!r.ok) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    const data = await r.json() as any;
    // Update local folder list
    await upsertCollectionFolders(ctx.userId, [{ id: data.id, name: data.name, count: 0 }]);
    res.json({ ok: true, folderId: data.id, name: data.name });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// Helper: throttle loop for Discogs API calls (~55/min to stay under the 60/min limit)
const DISCOGS_CALL_DELAY_MS = 1100;
const _sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

// POST /api/user/folders/rename — rename an existing collection folder
app.post("/api/user/folders/rename", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { folderId, name } = req.body ?? {};
  if (folderId == null || !name?.trim()) { res.status(400).json({ error: "folderId and name required" }); return; }
  const fid = Number(folderId);
  if (fid === 0 || fid === 1) { res.status(400).json({ error: "Cannot rename the built-in 'All' or 'Uncategorized' folders" }); return; }
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/folders/${fid}`;
    const r = await loggedFetch("discogs", url, {
      method: "POST",
      headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" },
      body: JSON.stringify({ name: name.trim() }),
      context: "folder-rename",
    });
    if (!r.ok && r.status !== 200 && r.status !== 204) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    await renameCollectionFolder(ctx.userId, fid, name.trim());
    res.json({ ok: true, folderId: fid, name: name.trim() });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/folders/delete — delete an empty collection folder
// If force=true and the folder has items, we first move every item to folder 1
// (Uncategorized) so Discogs will permit the delete.
app.post("/api/user/folders/delete", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { folderId, force } = req.body ?? {};
  if (folderId == null) { res.status(400).json({ error: "folderId required" }); return; }
  const fid = Number(folderId);
  if (fid === 0 || fid === 1) { res.status(400).json({ error: "Cannot delete the built-in 'All' or 'Uncategorized' folders" }); return; }
  try {
    // If forcing and the folder has contents, move them to Uncategorized first
    if (force) {
      const contents = await getFolderContents(ctx.userId, fid);
      // Safety cap: bulk delete through the API is slow (Discogs 60/min limit). For
      // very large folders, ask the user to move items manually or use the Discogs
      // website directly.
      if (contents.length > 150) {
        res.status(400).json({ error: `This folder contains ${contents.length} items. For folders this large, please move items in smaller batches or use the Discogs website.` });
        return;
      }
      for (let i = 0; i < contents.length; i++) {
        const item = contents[i];
        if (!item.instanceId) continue; // skip synthetic legacy rows (shouldn't happen post-migration)
        if (i > 0) await _sleep(DISCOGS_CALL_DELAY_MS);
        const moveUrl = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/folders/${fid}/releases/${item.releaseId}/instances/${item.instanceId}`;
        const mr = await loggedFetch("discogs", moveUrl, {
          method: "POST",
          headers: { ...ctx.client.buildHeaders("POST", moveUrl), "Content-Type": "application/json" },
          body: JSON.stringify({ folder_id: 1 }),
          context: "folder-delete-move",
        });
        if (!mr.ok && mr.status !== 204) {
          const text = await mr.text();
          // Persist whatever moves we already made locally so the state stays consistent
          await moveAllCollectionItemsBetweenFolders(ctx.userId, fid, 1);
          res.status(mr.status).json({ error: `Failed to move item during folder delete: ${text}` }); return;
        }
      }
      await moveAllCollectionItemsBetweenFolders(ctx.userId, fid, 1);
    }

    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/folders/${fid}`;
    const r = await loggedFetch("discogs", url, {
      method: "DELETE",
      headers: ctx.client.buildHeaders("DELETE", url),
      context: "folder-delete",
    });
    if (!r.ok && r.status !== 204) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    await deleteCollectionFolder(ctx.userId, fid);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/collection/move — move item to different folder
app.post("/api/user/collection/move", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { releaseId, instanceId, fromFolderId, toFolderId } = req.body ?? {};
  if (!releaseId || !instanceId || fromFolderId == null || toFolderId == null) {
    res.status(400).json({ error: "releaseId, instanceId, fromFolderId, toFolderId required" }); return;
  }
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/folders/${fromFolderId}/releases/${releaseId}/instances/${instanceId}`;
    const r = await loggedFetch("discogs", url, { method: "POST", headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" }, body: JSON.stringify({ folder_id: toFolderId }), context: "collection-move" });
    if (!r.ok && r.status !== 204) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    await updateCollectionFolder(ctx.userId, releaseId, toFolderId, instanceId);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/collection/notes — update notes on a collection item
app.post("/api/user/collection/notes", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { releaseId, instanceId, folderId = 1, fieldId, value } = req.body ?? {};
  if (!releaseId || !instanceId || fieldId == null) { res.status(400).json({ error: "releaseId, instanceId, fieldId required" }); return; }
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/folders/${folderId}/releases/${releaseId}/instances/${instanceId}/fields/${fieldId}`;
    const r = await loggedFetch("discogs", url, { method: "POST", headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" }, body: JSON.stringify({ value: value ?? "" }), context: "collection-notes" });
    if (!r.ok && r.status !== 204) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    // Update local DB notes — merge into existing JSONB notes array (instance-scoped)
    const instance = await getCollectionInstance(ctx.userId, releaseId);
    const currentNotes = instance?.notes ?? [];
    const noteIdx = currentNotes.findIndex((n: any) => n.field_id === fieldId);
    if (noteIdx >= 0) {
      currentNotes[noteIdx].value = value ?? "";
    } else {
      currentNotes.push({ field_id: fieldId, value: value ?? "" });
    }
    await updateCollectionNotes(ctx.userId, releaseId, currentNotes, instanceId);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/collection/fields — get user's custom field definitions
app.get("/api/user/collection/fields", async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/fields`;
    const r = await loggedFetch("discogs", url, { headers: ctx.client.buildHeaders("GET", url), context: "collection-fields" });
    if (!r.ok) { res.status(r.status).json({ error: "Failed to fetch fields" }); return; }
    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/wantlist/notes — update notes on a wantlist item
// Discogs stores a single free-text `notes` string on each wantlist item.
// We mirror the collection notes shape locally as [{ field_id: 0, value }].
app.post("/api/user/wantlist/notes", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { releaseId, value } = req.body ?? {};
  if (!releaseId) { res.status(400).json({ error: "releaseId required" }); return; }
  const text = String(value ?? "");
  try {
    const url = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/wants/${releaseId}`;
    // Preserve existing rating — POST overwrites the whole wantlist item.
    const existing = await getWantlistItem(ctx.userId, Number(releaseId));
    const body: any = { notes: text };
    if (existing?.rating) body.rating = existing.rating;
    const r = await loggedFetch("discogs", url, {
      method: "POST",
      headers: { ...ctx.client.buildHeaders("POST", url), "Content-Type": "application/json" },
      body: JSON.stringify(body),
      context: "wantlist-notes",
    });
    if (!r.ok && r.status !== 204) {
      const txt = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${txt}` }); return;
    }
    const newNotes = text ? [{ field_id: 0, value: text }] : [];
    await updateWantlistNotes(ctx.userId, Number(releaseId), newNotes);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/wantlist/item — fetch local wantlist rating & notes for a release
app.get("/api/user/wantlist/item", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const releaseId = Number(req.query.releaseId);
  if (!releaseId) { res.status(400).json({ error: "releaseId required" }); return; }
  try {
    const item = await getWantlistItem(userId, releaseId);
    if (!item) { res.json({ found: false }); return; }
    res.json({ found: true, rating: item.rating, notes: item.notes });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// ── Library of Congress audio search + saves ───────────────────────────
//
// ADMIN-ONLY feature. The LOC view is a personal listening workspace
// for the site owner — not a tenant-facing tool. Every endpoint below
// is gated with requireAdmin so even a signed-in non-admin who guesses
// the URL gets a 403.
//
// GET /api/loc/search is a rate-limited, cached proxy to loc.gov's JSON
// search API. The frontend never calls loc.gov directly so (a) LOC-side
// rate limits apply to the site as a whole rather than each user, and
// (b) LOC responses are cached in memory for LOC_CACHE_TTL_MS so a user
// paginating back and forth repeatedly hits the cache instead of LOC.
//
// The save endpoints back the "Saved" tab inside the LOC view — a
// durable, cross-device list the user curates by hitting the ★ on any
// result card.

// Whitelist of query params we forward to loc.gov. Everything else is
// dropped so malformed / abusive params can't sneak through.
const LOC_QUERY_PARAMS = new Set([
  "q", "c", "sp", "fo", "at", "sb",
  "contributor", "subject", "location", "language",
  "start_date", "end_date", "dates",
  "original_format", "online_format", "partof",
  "playable",  // SeaDisco-only: filter out results with no extractable stream
]);
// LOC sort values we accept. Maps our UI key → LOC's `sb` param value.
const LOC_SORT_MAP: Record<string, string> = {
  "relevance": "",               // default — LOC sorts by relevance when sb is absent
  "date-asc":  "date",           // oldest first
  "date-desc": "date_desc",      // newest first
  "title":     "title_s",        // A–Z by title
};

// Build a LOC URL from the validated query params. We always target the
// /audio/ endpoint since SeaDisco only surfaces audio.
function _buildLocSearchUrl(req: express.Request): { url: string; cacheKey: string } {
  const params = new URLSearchParams();
  params.set("fo", "json");
  // Cap per-page at 100 (LOC allows up to 1000 but we don't need it)
  const perPage = Math.min(Math.max(1, Number(req.query.c) || 100), 100);
  params.set("c", String(perPage));
  const page = Math.max(1, Number(req.query.sp) || 1);
  if (page > 1) params.set("sp", String(page));
  const q = typeof req.query.q === "string" ? req.query.q.trim() : "";
  if (q) params.set("q", q);

  // Facet filters — build the `fa` param as pipe-delimited key:value pairs
  const facets: string[] = [];
  // Always restrict to online-playable audio so we don't surface items
  // the user can't actually listen to.
  facets.push("online-format:audio");
  const facetMap: Record<string, string> = {
    contributor: "contributor",
    subject: "subject",
    location: "location",
    language: "language",
    partof: "partof",
  };
  for (const [qp, facetKey] of Object.entries(facetMap)) {
    const v = typeof req.query[qp] === "string" ? (req.query[qp] as string).trim() : "";
    if (v) facets.push(`${facetKey}:${v.toLowerCase()}`);
  }
  if (facets.length) params.set("fa", facets.join("|"));

  // Date range — LOC uses `dates=YYYY/YYYY`
  const startDate = typeof req.query.start_date === "string" ? req.query.start_date.trim() : "";
  const endDate   = typeof req.query.end_date   === "string" ? req.query.end_date.trim()   : "";
  if (/^\d{4}$/.test(startDate) || /^\d{4}$/.test(endDate)) {
    const s = /^\d{4}$/.test(startDate) ? startDate : "1800";
    const e = /^\d{4}$/.test(endDate)   ? endDate   : String(new Date().getFullYear());
    params.set("dates", `${s}/${e}`);
  }

  // Sort — map our UI key to LOC's `sb` param
  const sortKey = typeof req.query.sort === "string" ? req.query.sort.trim() : "";
  const sbValue = LOC_SORT_MAP[sortKey];
  if (sbValue) params.set("sb", sbValue);

  const url = `https://www.loc.gov/audio/?${params.toString()}`;
  // Cache key: deterministic on sorted params
  const cacheKey = [...params.entries()].sort((a, b) => a[0].localeCompare(b[0])).map(([k, v]) => `${k}=${v}`).join("&");
  return { url, cacheKey };
}

// Normalize a LOC result row into the compact card shape the frontend
// renders. Defensive — LOC responses vary across collections.
function _normalizeLocResult(r: any): any {
  if (!r || typeof r !== "object") return null;
  const id = typeof r.id === "string" ? r.id : (typeof r.url === "string" ? r.url : "");
  if (!id) return null;
  const title = typeof r.title === "string" ? r.title : "";
  const contributors = Array.isArray(r.contributor_primary) && r.contributor_primary.length
    ? r.contributor_primary
    : (Array.isArray(r.contributor) ? r.contributor.slice(0, 3) : []);
  const date = typeof r.date === "string" ? r.date : (Array.isArray(r.dates) ? r.dates[0] : "");
  const year = typeof date === "string" && date.length >= 4 ? date.slice(0, 4) : "";
  // Image coverage — LOC stores thumbnails in several places depending on
  // the collection. Try each in order and skip LOC's generic type-icon
  // SVG (`/static/images/original-format/...svg`), which is not a real
  // cover and just clutters the grid.
  const _pickImage = (): string => {
    const isUseful = (u: string) => !!u && !/\/static\/images\/original-format\//i.test(u);
    const clean = (u: string) => String(u).split("#")[0];
    // 1. Top-level image_url array (most common for Jukebox)
    if (Array.isArray(r.image_url)) {
      for (const u of r.image_url) if (typeof u === "string" && isUseful(u)) return clean(u);
    } else if (typeof r.image_url === "string" && isUseful(r.image_url)) {
      return clean(r.image_url);
    }
    // 2. item.image_url nested (some collections bury it under `item`)
    const itemImage = (r.item && typeof r.item === "object") ? r.item.image_url : null;
    if (Array.isArray(itemImage)) {
      for (const u of itemImage) if (typeof u === "string" && isUseful(u)) return clean(u);
    } else if (typeof itemImage === "string" && isUseful(itemImage)) {
      return clean(itemImage);
    }
    // 3. resources[].poster (video items have a poster frame)
    if (Array.isArray(r.resources)) {
      for (const res of r.resources) {
        if (!res || typeof res !== "object") continue;
        if (typeof res.poster === "string" && isUseful(res.poster)) {
          // poster URLs sometimes start with // — add https:
          return res.poster.startsWith("//") ? "https:" + clean(res.poster) : clean(res.poster);
        }
      }
      // 4. resources[].image (but only if it's a real image, not the SVG icon)
      for (const res of r.resources) {
        if (!res || typeof res !== "object") continue;
        if (typeof res.image === "string" && isUseful(res.image)) {
          return res.image.startsWith("//") ? "https:" + clean(res.image) : clean(res.image);
        }
      }
    }
    return "";
  };
  const image = _pickImage();
  // Audio URLs live in many different fields depending on the collection:
  //   - National Jukebox:              resources[].media         (mp3)
  //   - NAVCC / main catalog / AFC:    resources[].audio         (mp3)
  //   - Podcasts / AFC Folklife:       item.mp3_url              (mp3)
  //   - Video performances:            resources[].video         (mp4 — audio track plays in <audio>)
  //   - Some newer items:              resources[].stream        (HLS .m3u8)
  //   - Streamed video:                resources[].video_stream  (HLS)
  //
  // Items can contain multiple tracks — e.g. the Gerry Mulligan
  // autobiography has 31 audio resources in one item. We collect every
  // playable track into `tracks[]` in their original order.
  const itemObj = r.item && typeof r.item === "object" ? r.item : {};
  const tracks: Array<{ url: string; title: string; streamType: string; order: number; duration?: string }> = [];
  if (Array.isArray(r.resources)) {
    for (let i = 0; i < r.resources.length; i++) {
      const res = r.resources[i];
      if (!res || typeof res !== "object") continue;
      // Pick the best playable URL from this resource. Prefer direct
      // mp3/mp4 over HLS streams (native playback vs hls.js).
      const url = (typeof res.media === "string"        && res.media)        ? res.media
                : (typeof res.audio === "string"        && res.audio)        ? res.audio
                : (typeof res.video === "string"        && res.video)        ? res.video
                : (typeof res.stream === "string"       && res.stream)       ? res.stream
                : (typeof res.video_stream === "string" && res.video_stream) ? res.video_stream
                : "";
      if (!url) continue;
      const isHls = /\.m3u8($|\?)/i.test(url);
      const caption = typeof res.caption === "string" ? res.caption.trim()
                    : typeof res.resource_label === "string" ? res.resource_label.trim()
                    : "";
      // Duration as HH:MM:SS when LOC gives us seconds.
      let duration = "";
      const rawDur = res.duration;
      if (typeof rawDur === "number" && rawDur > 0) {
        const h = Math.floor(rawDur / 3600);
        const m = Math.floor((rawDur % 3600) / 60);
        const s = Math.floor(rawDur % 60);
        duration = h > 0
          ? `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
          : `${m}:${String(s).padStart(2, "0")}`;
      } else if (typeof rawDur === "string" && rawDur) {
        duration = rawDur;
      }
      tracks.push({
        url,
        title: caption,
        streamType: isHls ? "hls" : "mp3",
        order: typeof res.order === "number" ? res.order : i,
        ...(duration ? { duration } : {}),
      });
    }
  }
  // Podcast fallback — item.mp3_url is where Folklife / podcast items
  // keep their audio URL, with no resources[] entry.
  if (!tracks.length && typeof itemObj.mp3_url === "string" && itemObj.mp3_url) {
    tracks.push({
      url: itemObj.mp3_url,
      title: "",
      streamType: "mp3",
      order: 0,
    });
  }
  // Sort by order so multi-track items play in LP sequence
  tracks.sort((a, b) => a.order - b.order);
  const streamUrl  = tracks[0]?.url ?? "";
  const streamType = tracks[0]?.streamType ?? "";
  const item = itemObj;
  // Rights advisory — LOC's own note about what you can / can't do with
  // the recording. Critical for attribution and any downstream reuse.
  const rights = typeof item.rights_advisory === "string" ? item.rights_advisory
               : Array.isArray(item.rights_advisory) ? item.rights_advisory.join(" ")
               : typeof item.rights === "string" ? item.rights
               : Array.isArray(item.rights) ? item.rights.join(" ")
               : "";
  // Collection(s) the item belongs to — used for the credit line.
  // `partof` can be an array of strings or an array of objects with a
  // `title` property depending on the collection. Normalize to strings.
  const partof = Array.isArray(r.partof)
    ? r.partof.map((p: any) => typeof p === "string" ? p : (p?.title ?? "")).filter(Boolean)
    : [];
  // Helper: strip HTML tags and decode a few common entities so LOC's
  // HTML-flavored description / notes / article fields render as plain
  // text in the popup instead of leaking visible markup.
  const stripHtml = (s: string): string => {
    if (!s) return "";
    return String(s)
      // Convert block-level tags to paragraph breaks so the stripped
      // text keeps its structure
      .replace(/<\s*\/?(p|br|div|li|h[1-6]|tr)\s*\/?\s*>/gi, "\n")
      // Drop every other tag entirely
      .replace(/<[^>]*>/g, "")
      // Decode common entities
      .replace(/&nbsp;/g, " ")
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/&apos;/g, "'")
      // Collapse excess whitespace
      .replace(/[ \t]+/g, " ")
      .replace(/\n{3,}/g, "\n\n")
      .trim();
  };
  // Helper: coerce a "maybe-string, maybe-array, maybe-missing" field
  // into a trimmed string array for display. Dedupes and drops blanks.
  // Applies HTML stripping so LOC markup doesn't leak to the UI.
  const toArr = (v: any): string[] => {
    if (!v) return [];
    const arr = Array.isArray(v) ? v : [v];
    const out: string[] = [];
    const seen = new Set<string>();
    for (const x of arr) {
      const raw = typeof x === "string" ? x : (x?.title ?? x?.name ?? "");
      const s = stripHtml(raw);
      if (s && !seen.has(s)) { seen.add(s); out.push(s); }
    }
    return out;
  };

  // Subjects — different from genres; often topic-level tags like
  // "Blues (Music)" or "Folk songs--United States".
  const subjects = toArr(item.subjects).length ? toArr(item.subjects) : toArr(r.subject);

  return {
    id,
    title,
    contributors,
    date: String(date || ""),
    year,
    image,
    streamUrl,
    streamType,
    tracks,
    trackCount: tracks.length,
    label: item.recording_label ?? "",
    location: item.recording_location ?? (Array.isArray(r.location) ? r.location[0] : ""),
    summary: stripHtml(item.summary ?? ""),
    audioType: item.audio_type ?? "",
    genres: toArr(r.subject_genre).length ? toArr(r.subject_genre) : toArr(item.genre),
    language: Array.isArray(r.language) ? r.language[0] : (typeof item.language === "string" ? item.language : ""),
    rights,
    partof,
    url: typeof r.url === "string" ? r.url : id,

    // ── Extended metadata (rendered in the info popup) ──────────────
    subjects,
    otherTitles:         toArr(r.other_title),
    description:         toArr(r.description),
    notes:               toArr(item.notes),
    medium:              typeof item.medium === "string" ? item.medium
                         : Array.isArray(item.medium) ? item.medium.join(" · ") : "",
    mediaSize:           typeof item.media_size === "string" ? item.media_size : "",
    callNumber:          typeof item.call_number === "string" ? item.call_number
                         : Array.isArray(item.call_number) ? item.call_number[0]
                         : (typeof r.shelf_id === "string" ? r.shelf_id : ""),
    catalogNumber:       item.recording_catalog_number ?? "",
    matrixNumber:        item.recording_matrix_number ?? "",
    takeNumber:          item.recording_take_number ?? "",
    repository:          typeof item.recording_repository === "string" ? item.recording_repository
                         : (typeof item.repository === "string" ? item.repository
                         :  Array.isArray(item.repository) ? item.repository.join(" · ") : ""),
    createdPublished:    Array.isArray(item.created_published) ? item.created_published.join(" · ")
                         : (typeof item.created_published === "string" ? item.created_published : ""),
    format:              Array.isArray(item.format) ? item.format.join(" · ")
                         : (typeof item.format === "string" ? item.format : ""),
    itemType:            Array.isArray(r.type) ? r.type.join(" · ")
                         : (typeof r.type === "string" ? r.type : ""),

    // ── Podcast-specific fields (AFC Folklife etc.) ────────────────
    article:             stripHtml(typeof item.article === "string" ? item.article : ""),
    speakers:            toArr(item.speakers),
    runningTime:         typeof item.running_time === "string" ? item.running_time
                         : (typeof item.running_time === "number" ? String(item.running_time) : ""),
    podcastSeries:       typeof item.podcast_series === "string" ? item.podcast_series
                         : Array.isArray(item.podcast_series) ? item.podcast_series[0] : "",

    accessRestricted:    r.access_restricted === true,
  };
}

// GET /api/loc/search — proxy with rate limiting and response caching
app.get("/api/loc/search", async (req, res) => {
  // Admin-only: the LOC view is a personal workspace for the site owner.
  const userId = await requireAdmin(req, res);
  if (!userId) return;

  const { url, cacheKey } = _buildLocSearchUrl(req);

  // Cache hit → return immediately, no LOC round trip, no rate-limit slot
  const cached = _locCacheGet(cacheKey);
  if (cached) {
    _locStatsRecord("hit");
    res.setHeader("X-SeaDisco-Cache", "hit");
    res.json(cached);
    return;
  }

  // Cache miss → acquire a rate-limit slot (may queue or 503)
  try {
    await locLimiter.acquire();
  } catch (e: any) {
    if (e?.code === "rate_limit_queue_full") {
      _locStatsRecord("ratelimit");
      res.status(503).json({ error: "rate_limited", message: "LOC API is busy, try again in a moment." });
      return;
    }
    _locStatsRecord("failure", e?.message);
    res.status(500).json({ error: String(e?.message ?? e) });
    return;
  }

  try {
    const r = await loggedFetch("loc", url, {
      headers: { "User-Agent": "SeaDisco/1.0 (+https://seadisco.com)", "Accept": "application/json" },
      context: "loc-search",
    });
    if (r.status === 429) {
      _locStatsRecord("ratelimit");
      res.status(503).json({ error: "rate_limited", message: "LOC is currently rate-limiting — try again shortly." });
      return;
    }
    if (!r.ok) {
      _locStatsRecord("failure", `HTTP ${r.status}`);
      res.status(502).json({ error: `LOC HTTP ${r.status}` });
      return;
    }
    const ct = r.headers.get("content-type") ?? "";
    if (!ct.includes("json")) {
      // LOC sometimes serves an HTML CAPTCHA page on heavy load
      _locStatsRecord("failure", "non-JSON (CAPTCHA?)");
      res.status(502).json({ error: "LOC returned non-JSON (possible CAPTCHA). Try again later." });
      return;
    }
    const body = await r.json() as any;
    const allNormalized = Array.isArray(body?.results) ? body.results.map(_normalizeLocResult).filter(Boolean) : [];
    // Playable-only filter: when ?playable=1 (or not explicitly 0), drop
    // items that have no extractable stream URL. LOC sometimes returns
    // audio-format items where the mp3 lives behind a separate resource
    // page we can't discover without another round-trip — hiding them
    // here keeps the grid honest about what we can actually play.
    const playableOnly = req.query.playable !== "0" && req.query.playable !== "false";
    const results = playableOnly
      ? allNormalized.filter((r: any) => r && typeof r.streamUrl === "string" && r.streamUrl.length > 0)
      : allNormalized;
    const hiddenCount = allNormalized.length - results.length;
    const pagination = body?.pagination && typeof body.pagination === "object" ? {
      current: body.pagination.current ?? 1,
      perpage: body.pagination.perpage ?? 100,
      total:   body.pagination.of ?? body.pagination.total ?? 0,
      from:    body.pagination.from ?? 0,
      to:      body.pagination.to ?? 0,
      hasNext: !!body.pagination.next,
      hasPrev: !!body.pagination.previous,
      hiddenCount,
      playableOnly,
    } : { current: 1, perpage: 100, total: results.length, from: 1, to: results.length, hasNext: false, hasPrev: false, hiddenCount, playableOnly };

    const payload = { results, pagination };
    _locCacheSet(cacheKey, payload);
    _locStatsRecord("miss");
    res.setHeader("X-SeaDisco-Cache", "miss");
    res.json(payload);
  } catch (e: any) {
    _locStatsRecord("failure", e?.message);
    res.status(500).json({ error: String(e?.message ?? e) });
  }
});

// GET /api/user/loc-saves — list the current user's saved LOC items
app.get("/api/user/loc-saves", async (req, res) => {
  const userId = await requireAdmin(req, res);
  if (!userId) return;
  try {
    const items = await getLocSaves(userId);
    res.json({ items });
  } catch (e: any) {
    res.status(500).json({ error: String(e?.message ?? e) });
  }
});

// GET /api/user/loc-saves/ids — lightweight list of saved IDs (for toggling star state)
app.get("/api/user/loc-saves/ids", async (req, res) => {
  const userId = await requireAdmin(req, res);
  if (!userId) return;
  try {
    const ids = await getLocSaveIds(userId);
    res.json({ ids });
  } catch (e: any) {
    res.status(500).json({ error: String(e?.message ?? e) });
  }
});

// POST /api/user/loc-saves — save an item
// Body: { locId, title, streamUrl, data }
app.post("/api/user/loc-saves", express.json({ limit: "64kb" }), async (req, res) => {
  const userId = await requireAdmin(req, res);
  if (!userId) return;
  const { locId, title = null, streamUrl = null, data = {} } = req.body ?? {};
  if (typeof locId !== "string" || !locId) {
    res.status(400).json({ error: "locId required" }); return;
  }
  // Cap payload size (belt-and-braces; express.json already enforces limit)
  try {
    await saveLocItem(userId, locId, title ? String(title).slice(0, 500) : null,
                      streamUrl ? String(streamUrl).slice(0, 2000) : null,
                      data && typeof data === "object" ? data : {});
    res.json({ ok: true });
  } catch (e: any) {
    res.status(500).json({ error: String(e?.message ?? e) });
  }
});

// DELETE /api/user/loc-saves — remove a single save
// Body: { locId }
// (DELETE with a URL param was avoided because LOC IDs are full URLs with slashes)
app.delete("/api/user/loc-saves", express.json(), async (req, res) => {
  const userId = await requireAdmin(req, res);
  if (!userId) return;
  const locId = typeof req.body?.locId === "string"
    ? req.body.locId
    : (typeof req.query?.locId === "string" ? req.query.locId : "");
  if (!locId) { res.status(400).json({ error: "locId required" }); return; }
  try {
    await deleteLocSave(userId, locId);
    res.json({ ok: true });
  } catch (e: any) {
    res.status(500).json({ error: String(e?.message ?? e) });
  }
});

// ── Recent views (cross-device Recent strip) ────────────────────────────
//
// The frontend still writes localStorage immediately for instant UI, then
// mirrors the same entry to the server here. On first search-view render
// the GET below hydrates whichever device has the stalest local cache.

// GET /api/user/recent — ordered list of the user's last opened releases
app.get("/api/user/recent", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const limit = Number(req.query.limit) || 576;
  try {
    const rows = await getRecentViews(userId, limit);
    res.json({ items: rows });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/recent — upsert a single recent view
// Body: { id, type, data }
app.post("/api/user/recent", express.json({ limit: "64kb" }), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const { id, type = "release", data } = req.body ?? {};
  const discogsId = Number(id);
  if (!Number.isFinite(discogsId) || discogsId <= 0) {
    res.status(400).json({ error: "id required" }); return;
  }
  const entity = String(type || "release").toLowerCase();
  if (!["release", "master"].includes(entity)) {
    res.status(400).json({ error: "type must be release or master" }); return;
  }
  try {
    await upsertRecentView(userId, discogsId, entity, data && typeof data === "object" ? data : {});
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// DELETE /api/user/recent/:id — drop one entry
app.delete("/api/user/recent/:id", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const discogsId = Number(req.params.id);
  if (!discogsId) { res.status(400).json({ error: "Invalid id" }); return; }
  const type = typeof req.query.type === "string" ? req.query.type : undefined;
  try {
    await deleteRecentView(userId, discogsId, type);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// DELETE /api/user/recent — clear all entries for the current user
app.delete("/api/user/recent", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    await clearRecentViews(userId);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/collection/instance — get instance info for a release in the user's collection
app.get("/api/user/collection/instance", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const releaseId = Number(req.query.releaseId);
  if (!releaseId) { res.status(400).json({ error: "releaseId required" }); return; }
  try {
    const instance = await getCollectionInstance(userId, releaseId);
    if (!instance) { res.json({ found: false }); return; }
    res.json({ found: true, instance_id: instance.instanceId, folder_id: instance.folderId, rating: instance.rating, notes: instance.notes });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/collection/instances — list every stored instance of a release
app.get("/api/user/collection/instances", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const releaseId = Number(req.query.releaseId);
  if (!releaseId) { res.status(400).json({ error: "releaseId required" }); return; }
  try {
    const instances = await getCollectionInstances(userId, releaseId);
    res.json({
      count: instances.length,
      instances: instances.map(i => ({
        instance_id: i.instanceId,
        folder_id:   i.folderId,
        rating:      i.rating,
        notes:       i.notes,
        added_at:    i.addedAt,
      })),
    });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// ── Phase 4: Price Intelligence & Alerts ─────────────────────────────────

// GET /api/price-history/:releaseId — price history for sparklines
// ── Saved searches ──────────────────────────────────────────────────────

// GET /api/user/saved-searches?view=search — list saved searches
app.get("/api/user/saved-searches", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const view = (req.query.view as string) || undefined;
    const searches = await getSavedSearches(userId, view);
    res.json({ searches });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/saved-searches — save a search
app.post("/api/user/saved-searches", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const { view, label, params } = req.body ?? {};
  if (!view || !label) { res.status(400).json({ error: "view and label required" }); return; }
  try {
    const id = await saveSavedSearch(userId, view, label.slice(0, 200), params ?? {});
    res.json({ ok: true, id });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// DELETE /api/user/saved-searches/:id — delete a saved search
app.delete("/api/user/saved-searches/:id", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    await deleteSavedSearch(userId, parseInt(req.params.id));
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/facets — distinct genres and styles from collection or wantlist
app.get("/api/user/facets", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const type = (req.query.type as string) ?? "collection";
    const genre = (req.query.genre as string) || undefined;
    const facets = type === "wantlist" ? await getWantlistFacets(userId, genre) : await getCollectionFacets(userId, genre);
    res.json(facets);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/folders — collection folder list
app.get("/api/user/folders", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const folders = await getCollectionFolderList(userId);
    res.json({ folders });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/discogs-ids — collection and wantlist IDs for badge rendering
app.get("/api/user/discogs-ids", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const [collectionIds, wantlistIds, favoriteIds, inventoryIds, inventoryListingIds, listMembership, collectionInstanceCounts, collectionMasterCounts, wantlistMasterCounts, defaultAddFolderId, profile] = await Promise.all([
      getCollectionIds(userId),
      getWantlistIds(userId),
      getFavoriteIds(userId),
      getInventoryIds(userId),
      getInventoryListingIdsByRelease(userId),
      getListMembership(userId),
      getCollectionMultiInstanceCounts(userId),
      getCollectionMasterCounts(userId),
      getWantlistMasterCounts(userId),
      getDefaultAddFolderId(userId),
      getDiscogsProfile(userId),
    ]);
    res.json({ collectionIds, wantlistIds, favoriteIds, inventoryIds, inventoryListingIds, listMembership, collectionInstanceCounts, collectionMasterCounts, wantlistMasterCounts, defaultAddFolderId, currency: profile.currAbbr || "USD" });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/favorites — full data for rendering favorite cards
app.get("/api/user/favorites", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 96, 200);
    const offset = parseInt(req.query.offset as string) || 0;
    const items = await getFavorites(userId, limit, offset);
    res.json({ items });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/random-records — random selection from all user data
app.get("/api/user/random-records", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 192, 300);
    const rows = await getRandomRecords(userId, limit);
    res.json({ items: rows });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/favorites/add
app.post("/api/user/favorites/add", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const { discogsId, entityType, data } = req.body ?? {};
  if (!discogsId || !entityType) { res.status(400).json({ error: "Missing discogsId or entityType" }); return; }
  try {
    await addFavorite(userId, discogsId, entityType, data ?? {});
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/user/favorites/remove
app.post("/api/user/favorites/remove", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const { discogsId, entityType } = req.body ?? {};
  if (!discogsId || !entityType) { res.status(400).json({ error: "Missing discogsId or entityType" }); return; }
  try {
    await removeFavorite(userId, discogsId, entityType);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/user/sync-status — last sync timestamps + Discogs username + profile
app.get("/api/user/sync-status", async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const [syncStatus, username, profile] = await Promise.all([
    getSyncStatus(userId),
    getDiscogsUsername(userId),
    getDiscogsProfile(userId),
  ]);
  res.json({
    collectionSyncedAt: syncStatus.collectionSyncedAt,
    wantlistSyncedAt:   syncStatus.wantlistSyncedAt,
    discogsUsername:     username,
    syncStatus:          syncStatus.syncStatus,
    syncProgress:        syncStatus.syncProgress,
    syncTotal:           syncStatus.syncTotal,
    syncError:           syncStatus.syncError,
    authMethod:          profile.authMethod,
    avatarUrl:           profile.avatarUrl,
    profileData:         profile.profileData,
  });
});

function stripArtistSuffix(name: string | undefined): string | undefined {
  return name ? name.replace(/\s*\(\d+\)$/, "").trim() : undefined;
}

// POST /api/feedback — save feedback from signed-in user
app.post("/api/feedback", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }
  const { message, userEmail } = req.body;
  if (!message?.trim()) { res.status(400).json({ error: "Message required" }); return; }
  await saveFeedback(userId, userEmail ?? "", message.trim());
  res.json({ ok: true });
});

// ── Admin rate limiter ──────────────────────────────────────────────────
const adminRateCounts = new Map<string, { count: number; resetAt: number }>();
function _clientIp(req: express.Request): string {
  const xff = (req.headers["x-forwarded-for"] as string ?? "").split(",")[0].trim();
  return (xff || (req.ip ?? "unknown")).replace(/^::ffff:/, "").trim();
}
app.use("/api/admin", (req, res, next) => {
  const ip = _clientIp(req);
  const now = Date.now();
  const entry = adminRateCounts.get(ip);
  if (!entry || now > entry.resetAt) {
    adminRateCounts.set(ip, { count: 1, resetAt: now + 60_000 });
    return next();
  }
  if (entry.count >= 30) { res.status(429).json({ error: "Rate limited" }); return; }
  entry.count++;
  next();
});

// GET /api/admin/feedback — inbox, only for admin user
app.get("/api/admin/feedback", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const items = await getFeedback();
  res.json({ items });
});

// DELETE /api/admin/feedback/:id — delete a feedback item, admin only
app.delete("/api/admin/feedback/:id", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  await deleteFeedback(parseInt(req.params.id));
  res.json({ ok: true });
});

// GET /api/admin/sync-status — per-user sync status, admin only
app.get("/api/admin/sync-status", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const [users, favCounts] = await Promise.all([getAllUsersSyncStatus(), getAllFavoriteCounts()]);

  // Check Clerk sessions for accurate last-activity timestamps
  const clerkSecret = process.env.CLERK_SECRET_KEY ?? "";
  const lastActiveMap = new Map<string, number>(); // clerkUserId → most recent session activity ms
  if (clerkSecret) {
    try {
      // Query sessions for each user — check active first, fall back to recent ended/expired
      for (const u of users) {
        if (!u.clerkUserId) continue;
        try {
          let latest = 0;
          // Try active sessions first (most accurate for current users)
          for (const status of ["active", "ended", "expired"] as const) {
            const resp = await fetch(
              `https://api.clerk.com/v1/sessions?user_id=${u.clerkUserId}&status=${status}&limit=5`,
              { headers: { Authorization: `Bearer ${clerkSecret}` } }
            );
            if (!resp.ok) continue;
            const sessions = await resp.json() as Array<{ last_active_at: number; created_at: number }>;
            for (const s of sessions) {
              const ts = s.last_active_at > 1e12 ? s.last_active_at : s.last_active_at * 1000;
              if (ts > latest) latest = ts;
            }
            if (latest > 0) break; // found activity, no need to check lower-priority statuses
          }
          if (latest > 0) lastActiveMap.set(u.clerkUserId, latest);
        } catch { /* skip user */ }
      }
    } catch { /* ignore — activity data is best-effort */ }
  }

  const oneDayAgoMs = Date.now() - 24 * 60 * 60 * 1000;
  const enriched = users.map(u => {
    const lastActiveAt = lastActiveMap.get(u.clerkUserId) ?? null;
    const favoriteCount = favCounts.get(u.clerkUserId) ?? 0;
    return { ...u, online: lastActiveAt ? lastActiveAt > oneDayAgoMs : false, lastActiveAt, favoriteCount };
  });
  res.json({ users: enriched });
});

// POST /api/admin/sync-all — trigger FULL background sync for all users, admin only
app.post("/api/admin/sync-all", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  _syncAbort = false; // clear any previous abort
  const users = await getAllUsersForSync();
  res.json({ ok: true, queued: users.length, mode: "full" });
  // Run syncs sequentially so server load and Discogs API stay manageable
  (async () => {
    for (const user of users) {
      if (_syncAbort) { console.log("Sync-all: aborted, skipping remaining users"); break; }
      try {
        const userClient = await getDiscogsClientForUser(user.clerkUserId);
        if (!userClient) { console.warn(`Sync-all: no auth for ${user.username}, skipping`); continue; }
        await runBackgroundSync(user.clerkUserId, userClient, user.username, true, true);
      } catch (err) {
        console.error(`Sync-all error for ${user.username}:`, err);
      }
    }
  })();
});

// POST /api/admin/sync-user — trigger background sync for a single user, admin only
app.post("/api/admin/sync-user", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const { username } = req.body as { username: string };
  if (!username) { res.status(400).json({ error: "username required" }); return; }
  const users = await getAllUsersForSync();
  const user = users.find(u => u.username === username);
  if (!user) { res.status(404).json({ error: "User not found" }); return; }
  _syncAbort = false;
  const userClient = await getDiscogsClientForUser(user.clerkUserId);
  if (!userClient) { res.status(400).json({ error: "No valid auth for user" }); return; }
  res.json({ ok: true, username, mode: "full" });
  (async () => {
    try {
      await runBackgroundSync(user.clerkUserId, userClient, user.username, true, true);
    } catch (err) {
      console.error(`Sync-user error for ${user.username}:`, err);
    }
  })();
});

// POST /api/admin/sync-stop — abort all running syncs and reset statuses
app.post("/api/admin/sync-stop", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  _syncAbort = true;
  const count = await resetAllSyncingStatuses();
  console.log(`Admin: sync abort requested, ${count} syncing statuses reset`);
  res.json({ ok: true, message: `All syncs stopped — ${count} reset.` });
});

// POST /api/admin/api-kill — toggle global API kill switch
app.post("/api/admin/api-kill", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const { enabled } = req.body ?? {};
  _apiKillSwitch = enabled !== undefined ? !!enabled : !_apiKillSwitch;
  console.log(`Admin: API kill switch ${_apiKillSwitch ? "ENABLED — all outgoing requests blocked" : "DISABLED — requests flowing"}`);
  res.json({ ok: true, killSwitch: _apiKillSwitch });
});

// GET /api/admin/api-kill — check kill switch status
app.get("/api/admin/api-kill", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  res.json({ killSwitch: _apiKillSwitch });
});

// POST /api/admin/revoke-sessions — log out all Clerk users except admin
app.post("/api/admin/revoke-sessions", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const clerkSecret = process.env.CLERK_SECRET_KEY ?? "";
  if (!clerkSecret) { res.status(500).json({ error: "CLERK_SECRET_KEY not configured" }); return; }
  try {
    // Fetch all Clerk users (paginated, up to 500)
    let revoked = 0;
    let offset = 0;
    const limit = 100;
    while (true) {
      const usersResp = await fetch(`https://api.clerk.com/v1/users?limit=${limit}&offset=${offset}`, {
        headers: { Authorization: `Bearer ${clerkSecret}` },
      });
      if (!usersResp.ok) { res.status(502).json({ error: `Clerk API error: ${usersResp.status}` }); return; }
      const users = await usersResp.json() as Array<{ id: string }>;
      if (users.length === 0) break;
      for (const u of users) {
        if (u.id === adminId) continue;
        await fetch(`https://api.clerk.com/v1/users/${u.id}/sessions/revoke`, {
          method: "POST",
          headers: { Authorization: `Bearer ${clerkSecret}` },
        });
        revoked++;
      }
      if (users.length < limit) break;
      offset += limit;
    }
    console.log(`Admin: revoked sessions for ${revoked} user(s)`);
    res.json({ ok: true, revokedUsers: revoked });
  } catch (err) {
    console.error("revoke-sessions error:", err);
    res.status(500).json({ error: String(err) });
  }
});

// GET /api/admin/loc-stats — LOC proxy stats for the admin dashboard
app.get("/api/admin/loc-stats", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  _locStatsPrune();
  const now = Date.now();
  const oneHourAgo = now - 60 * 60 * 1000;
  const tenMinAgo  = now - 10 * 60 * 1000;
  const requestsLastHour = _locStats.recentTs.filter(t => t >= oneHourAgo).length;
  const requestsLastDay  = _locStats.recentTs.length;
  const requestsLast10m  = _locStats.recentTs.filter(t => t >= tenMinAgo).length;
  const totalLookups = _locStats.cacheHits + _locStats.cacheMisses;
  const hitRatePct = totalLookups > 0 ? Math.round((_locStats.cacheHits / totalLookups) * 1000) / 10 : 0;
  res.json({
    cacheHits:       _locStats.cacheHits,
    cacheMisses:     _locStats.cacheMisses,
    hitRatePct,
    cacheEntries:    _locCache.size,
    cacheMax:        LOC_CACHE_MAX,
    cacheTtlMs:      LOC_CACHE_TTL_MS,
    failures:        _locStats.failures,
    rateLimitHits:   _locStats.rateLimitHits,
    lastRequestAt:   _locStats.lastRequestAt,
    lastFailureAt:   _locStats.lastFailureAt,
    lastFailureMsg:  _locStats.lastFailureMsg,
    requestsLast10m,
    requestsLastHour,
    requestsLastDay,
    rateLimiter:     locLimiter.getStats(),
  });
});

// POST /api/admin/loc-cache/clear — wipe the in-memory LOC response cache
app.post("/api/admin/loc-cache/clear", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  const cleared = _locCache.size;
  _locCache.clear();
  res.json({ ok: true, cleared });
});

// POST /api/admin/purge-non-admin-users — wipe all per-user data EXCEPT the admin
// Body: { confirm: "PURGE" }
app.post("/api/admin/purge-non-admin-users", express.json(), async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  if ((req.body?.confirm ?? "") !== "PURGE") {
    res.status(400).json({ error: "confirm_required", message: "Body must include confirm: \"PURGE\"" });
    return;
  }
  const adminId = ADMIN_CLERK_ID;
  try {
    const counts = await purgeNonAdminUserData(adminId);
    const total = Object.values(counts).reduce((a, b) => a + (b > 0 ? b : 0), 0);
    console.log(`[admin] purged non-admin user data: ${total} rows total`, counts);
    res.json({ ok: true, total, counts });
  } catch (err: any) {
    console.error("purge-non-admin-users error:", err);
    res.status(500).json({ error: String(err?.message ?? err) });
  }
});

// GET /api/admin/collection-stats — per-user and global collection/wantlist stats
app.get("/api/admin/collection-stats", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  try {
    const stats = await getUserCollectionStats();
    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

// GET /api/admin/user-items — view any user's collection or wantlist, admin only
app.get("/api/admin/user-items", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const username = (req.query.username as string ?? "").trim();
  const tab = (req.query.tab as string ?? "collection");
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const perPage = Math.min(100, Math.max(1, parseInt(req.query.per_page as string) || 50));
  if (!username) { res.status(400).json({ error: "username required" }); return; }
  try {
    const clerkUserId = await getClerkUserIdByUsername(username);
    if (!clerkUserId) { res.status(404).json({ error: "User not found" }); return; }
    const result = tab === "wantlist"
      ? await getWantlistPage(clerkUserId, page, perPage)
      : await getCollectionPage(clerkUserId, page, perPage);
    res.json({ ...result, page, pages: Math.ceil(result.total / perPage) });
  } catch (err) { res.status(500).json({ error: String(err) }); }
});

// GET /api/admin/user-favorites — view any user's favorites, admin only
app.get("/api/admin/user-favorites", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const username = (req.query.username as string ?? "").trim();
  if (!username) { res.status(400).json({ error: "username required" }); return; }
  try {
    const clerkUserId = await getClerkUserIdByUsername(username);
    if (!clerkUserId) { res.status(404).json({ error: "User not found" }); return; }
    const items = await getFavorites(clerkUserId, 100);
    res.json({ items, total: items.length });
  } catch (err) { res.status(500).json({ error: String(err) }); }
});

// ── Blues DB admin endpoints ─────────────────────────────────────────────
// Curated database of pre-1930 blues artists. Admin-only. Phase 1 seeds
// from Wikidata; admin can add / edit / delete rows manually for obscure
// artists not on Wikidata or to correct mistakes.

app.get("/api/admin/blues/stats", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  try { res.json(await getBluesStats()); }
  catch (err) { console.error("[blues stats]", err); res.status(500).json({ error: String(err) }); }
});

app.get("/api/admin/blues/list", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  const search = (req.query.search as string ?? "").trim();
  // Admin tab now defaults to "show everything" (all=1 -> 100,000 cap).
  // Explicit limit query param still works for callers that want paging.
  const wantAll = req.query.all === "1" || req.query.all === "true";
  const limit = wantAll
    ? 100000
    : Math.min(2000, Math.max(1, parseInt(String(req.query.limit ?? "50"), 10) || 50));
  const offset = Math.max(0, parseInt(String(req.query.offset ?? "0"), 10) || 0);
  const sort = (req.query.sort as string ?? "name").trim();
  const order = (req.query.order as string ?? "asc").trim();
  try {
    const out = await listBluesArtists({ search, limit, offset, sort, order });
    res.json({ ...out, limit, offset, sort, order });
  } catch (err) { console.error("[blues list]", err); res.status(500).json({ error: String(err) }); }
});

// CSV export of every row in blues_artists. Admin-only. Streams a
// content-type-flagged response so browsers download the file directly.
app.get("/api/admin/blues/export.csv", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  try {
    // Pull everything in one shot. Whitelist-safe sort.
    const sort = (req.query.sort as string ?? "name").trim();
    const order = (req.query.order as string ?? "asc").trim();
    const { rows } = await listBluesArtists({ limit: 100000, offset: 0, sort, order });
    // Columns chosen to be readable in a spreadsheet without losing
    // the JSON-array structures completely (we serialise arrays as
    // pipe-separated strings; complex objects get JSON-encoded).
    const cols = [
      "id", "name", "aliases",
      "birth_date", "birth_place", "death_date", "death_place", "death_cause",
      "hometown_region",
      "first_recording_year", "first_recording_title",
      "last_recording_year",  "last_recording_title",
      "earliest_release_year",
      "styles", "instruments", "associated_labels", "songs_authored", "collaborators",
      "discogs_id", "musicbrainz_mbid", "wikidata_qid", "wikipedia_suffix",
      "photo_url", "youtube_urls",
      "discogs_releases",
      "notes", "enrichment_status",
      "date_added", "updated_at",
    ];
    const csvCell = (v: any): string => {
      if (v == null) return "";
      if (Array.isArray(v)) {
        // Pipe-separated for plain string arrays; JSON for object arrays.
        const isComplex = v.length > 0 && typeof v[0] === "object" && v[0] !== null;
        v = isComplex ? JSON.stringify(v) : v.join(" | ");
      } else if (typeof v === "object") {
        v = JSON.stringify(v);
      }
      const s = String(v);
      // Quote if the value contains comma, quote, newline, or starts
      // with a leading equals/at/plus/minus (Excel formula-injection guard).
      if (/[",\n\r]/.test(s) || /^[=@+\-]/.test(s)) {
        return `"${s.replace(/"/g, '""')}"`;
      }
      return s;
    };
    const header = cols.join(",");
    const body = rows.map(r => cols.map(c => csvCell(r[c])).join(",")).join("\n");
    const csv = "﻿" + header + "\n" + body + "\n"; // BOM for Excel UTF-8
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="seadisco-blues-${new Date().toISOString().slice(0,10)}.csv"`);
    res.send(csv);
  } catch (err: any) {
    console.error("[blues export.csv]", err);
    res.status(500).json({ error: err?.message ?? String(err) });
  }
});

// Delete every row in blues_artists. Admin-only. Used to wipe a noisy
// seed (e.g. the Wikidata pass that smuggled non-blues artists in via
// loose genre tags) before re-running.
app.delete("/api/admin/blues/all", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  // Belt-and-braces: caller must echo a confirm token in the body so
  // a stray DELETE call can't nuke the table.
  if ((req.query.confirm ?? req.body?.confirm) !== "DELETE ALL") {
    res.status(400).json({ error: "missing confirm token (expected: DELETE ALL)" });
    return;
  }
  try {
    const n = await deleteAllBluesArtists();
    res.json({ ok: true, deleted: n });
  } catch (err: any) {
    console.error("[blues delete all]", err);
    res.status(500).json({ error: err?.message ?? String(err) });
  }
});

app.get("/api/admin/blues/:id", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  const id = parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) { res.status(400).json({ error: "bad id" }); return; }
  try {
    const row = await getBluesArtist(id);
    if (!row) { res.status(404).json({ error: "not found" }); return; }
    res.json(row);
  } catch (err) { console.error("[blues get]", err); res.status(500).json({ error: String(err) }); }
});

app.post("/api/admin/blues", express.json({ limit: "200kb" }), async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  const body = req.body ?? {};
  if (!body.name?.trim()) { res.status(400).json({ error: "name required" }); return; }
  try {
    const id = await insertBluesArtist(body);
    res.json({ ok: true, id });
  } catch (err: any) {
    console.error("[blues insert]", err);
    res.status(500).json({ error: err?.message ?? String(err) });
  }
});

app.put("/api/admin/blues/:id", express.json({ limit: "200kb" }), async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  const id = parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) { res.status(400).json({ error: "bad id" }); return; }
  try {
    await updateBluesArtist(id, req.body ?? {});
    res.json({ ok: true });
  } catch (err: any) {
    console.error("[blues update]", err);
    res.status(500).json({ error: err?.message ?? String(err) });
  }
});

app.delete("/api/admin/blues/:id", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  const id = parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) { res.status(400).json({ error: "bad id" }); return; }
  try {
    await deleteBluesArtist(id);
    res.json({ ok: true });
  } catch (err) { console.error("[blues delete]", err); res.status(500).json({ error: String(err) }); }
});

// Trigger the Wikidata seed. Returns a summary of fetched/upserted/errors.
// Idempotent — re-running just refreshes the Wikidata-sourced fields and
// leaves manual edits the admin made elsewhere alone.
app.post("/api/admin/blues/seed", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  try {
    const result = await seedBluesArtistsFromWikidata();
    res.json({ ok: true, ...result });
  } catch (err: any) {
    console.error("[blues seed]", err);
    res.status(500).json({ error: err?.message ?? String(err) });
  }
});

// Phase 2: MusicBrainz enrichment. Walks rows with an MBID and writes
// first/last recording year + title + any new aliases. Rate-limited
// internally to MB's 1 req/s policy.
//
//   POST /api/admin/blues/enrich-mb           — bulk, all rows
//   POST /api/admin/blues/enrich-mb?id=N      — single row
//   POST /api/admin/blues/enrich-mb?limit=10  — first N rows only (testing)
//
// 10-minute response timeout because the bulk pass can take a while.
// MB enrich. Single-row (?id=N) runs inline — fast. Bulk runs as a
// background job since the full pass takes ~10 min and exceeds the
// edge proxy's idle timeout.

interface BluesMbJobState {
  status: "idle" | "running" | "done" | "error";
  startedAt: string | null;
  endedAt: string | null;
  progress: import("./blues-db.js").MbEnrichProgress | null;
  result: import("./blues-db.js").MbEnrichResult | null;
  error: string | null;
}
let _bluesMbJob: BluesMbJobState = {
  status: "idle", startedAt: null, endedAt: null,
  progress: null, result: null, error: null,
};

app.post("/api/admin/blues/enrich-mb", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  const idRaw = req.query.id as string | undefined;
  const limitRaw = req.query.limit as string | undefined;
  const idFilter = idRaw ? parseInt(idRaw, 10) : undefined;
  const limit = limitRaw ? parseInt(limitRaw, 10) : undefined;

  // Per-row enrich is short — keep it synchronous so the editor button
  // gets the result back in one round-trip.
  if (Number.isFinite(idFilter as number)) {
    try {
      const result = await enrichBluesFromMusicBrainz({ idFilter });
      res.json({ ok: true, ...result });
    } catch (err: any) {
      console.error("[blues enrich-mb single]", err);
      res.status(500).json({ error: err?.message ?? String(err) });
    }
    return;
  }

  // Bulk path → background job + 202.
  if (_bluesMbJob.status === "running") {
    res.status(409).json({
      error: "MB enrichment already running",
      startedAt: _bluesMbJob.startedAt,
      progress: _bluesMbJob.progress,
    });
    return;
  }
  _bluesMbJob = {
    status: "running",
    startedAt: new Date().toISOString(),
    endedAt: null,
    progress: null,
    result: null,
    error: null,
  };
  enrichBluesFromMusicBrainz({
    limit: Number.isFinite(limit as number) ? limit : undefined,
    onProgress: (p) => { _bluesMbJob.progress = p; },
  })
    .then(result => {
      _bluesMbJob.status = "done";
      _bluesMbJob.result = result;
      _bluesMbJob.endedAt = new Date().toISOString();
    })
    .catch(err => {
      _bluesMbJob.status = "error";
      _bluesMbJob.error = err?.message ?? String(err);
      _bluesMbJob.endedAt = new Date().toISOString();
      console.error("[blues enrich-mb bulk]", err);
    });
  res.status(202).json({ ok: true, started: true, startedAt: _bluesMbJob.startedAt });
});

app.get("/api/admin/blues/enrich-mb/status", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  res.json(_bluesMbJob);
});

// Phase 1.5 — Discogs year-walk seed (background job).
//
//   POST /api/admin/blues/seed-discogs[?startYear=1900&endYear=1930&maxPages=25]
//        → 202 immediately if the job started, 409 if one's already running
//   GET  /api/admin/blues/seed-discogs/status
//        → { status: "idle"|"running"|"done"|"error", progress, result, error }
//
// The seed walks Discogs paginated search results paced 1.1s/req and runs
// for ~10 minutes — well past Railway's edge proxy timeout. We run it as
// a background promise so the HTTP request can return immediately, and
// the admin UI polls the status endpoint to display progress.

interface BluesSeedJobState {
  status: "idle" | "running" | "done" | "error";
  startedAt: string | null;
  endedAt: string | null;
  progress: import("./blues-db.js").DiscogsSeedProgress | null;
  result: import("./blues-db.js").DiscogsSeedResult | null;
  error: string | null;
}
let _bluesDiscogsJob: BluesSeedJobState = {
  status: "idle", startedAt: null, endedAt: null,
  progress: null, result: null, error: null,
};

app.post("/api/admin/blues/seed-discogs", async (req, res) => {
  const adminId = await requireAdmin(req, res);
  if (!adminId) return;
  if (_bluesDiscogsJob.status === "running") {
    res.status(409).json({
      error: "Seed already running",
      startedAt: _bluesDiscogsJob.startedAt,
      progress: _bluesDiscogsJob.progress,
    });
    return;
  }
  const client = await getDiscogsClientForUser(adminId);
  if (!client) {
    res.status(400).json({ error: "Admin has no Discogs token configured. Connect Discogs on the Account page first." });
    return;
  }
  const startYear = parseInt(String(req.query.startYear ?? "1900"), 10);
  const endYear   = parseInt(String(req.query.endYear   ?? "1930"), 10);
  const maxPages  = parseInt(String(req.query.maxPages  ?? "25"),   10);

  // Reset job state and kick off — explicitly NOT awaited.
  _bluesDiscogsJob = {
    status: "running",
    startedAt: new Date().toISOString(),
    endedAt: null,
    progress: null,
    result: null,
    error: null,
  };
  seedBluesArtistsFromDiscogs(client, {
    startYear, endYear, maxPages,
    onProgress: (p) => { _bluesDiscogsJob.progress = p; },
  })
    .then(result => {
      _bluesDiscogsJob.status = "done";
      _bluesDiscogsJob.result = result;
      _bluesDiscogsJob.endedAt = new Date().toISOString();
    })
    .catch(err => {
      _bluesDiscogsJob.status = "error";
      _bluesDiscogsJob.error = err?.message ?? String(err);
      _bluesDiscogsJob.endedAt = new Date().toISOString();
      console.error("[blues seed-discogs]", err);
    });

  // 202 Accepted — work is in flight, frontend should poll status.
  res.status(202).json({ ok: true, started: true, startedAt: _bluesDiscogsJob.startedAt });
});

app.get("/api/admin/blues/seed-discogs/status", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  res.json(_bluesDiscogsJob);
});

// Phase 3a — Wikipedia notes (lead paragraph). Single-row inline,
// bulk via background job (mirrors the MB/Discogs-seed pattern).
interface BluesGenericJobState {
  status: "idle" | "running" | "done" | "error";
  startedAt: string | null;
  endedAt: string | null;
  progress: import("./blues-db.js").GenericEnrichProgress | null;
  result: import("./blues-db.js").GenericEnrichResult | null;
  error: string | null;
}
const _emptyJob = (): BluesGenericJobState => ({
  status: "idle", startedAt: null, endedAt: null,
  progress: null, result: null, error: null,
});
let _bluesWikiJob:    BluesGenericJobState = _emptyJob();
let _bluesDiscogsIdJob: BluesGenericJobState = _emptyJob();

app.post("/api/admin/blues/enrich-wiki", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  const idRaw = req.query.id as string | undefined;
  const limitRaw = req.query.limit as string | undefined;
  const force = req.query.force === "1" || req.query.force === "true";
  const idFilter = idRaw ? parseInt(idRaw, 10) : undefined;
  const limit = limitRaw ? parseInt(limitRaw, 10) : undefined;
  if (Number.isFinite(idFilter as number)) {
    try {
      const result = await enrichBluesFromWikipedia({ idFilter, force });
      res.json({ ok: true, ...result });
    } catch (err: any) {
      console.error("[blues enrich-wiki single]", err);
      res.status(500).json({ error: err?.message ?? String(err) });
    }
    return;
  }
  if (_bluesWikiJob.status === "running") {
    res.status(409).json({ error: "Wiki enrichment already running", startedAt: _bluesWikiJob.startedAt, progress: _bluesWikiJob.progress });
    return;
  }
  _bluesWikiJob = { ..._emptyJob(), status: "running", startedAt: new Date().toISOString() };
  enrichBluesFromWikipedia({
    limit: Number.isFinite(limit as number) ? limit : undefined,
    force,
    onProgress: (p) => { _bluesWikiJob.progress = p; },
  })
    .then(result => {
      _bluesWikiJob.status = "done";
      _bluesWikiJob.result = result;
      _bluesWikiJob.endedAt = new Date().toISOString();
    })
    .catch(err => {
      _bluesWikiJob.status = "error";
      _bluesWikiJob.error = err?.message ?? String(err);
      _bluesWikiJob.endedAt = new Date().toISOString();
      console.error("[blues enrich-wiki bulk]", err);
    });
  res.status(202).json({ ok: true, started: true, startedAt: _bluesWikiJob.startedAt });
});

app.get("/api/admin/blues/enrich-wiki/status", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  res.json(_bluesWikiJob);
});

// Phase 3b — Discogs ID confirmation. Same single-inline / bulk-job split.
app.post("/api/admin/blues/enrich-discogs", async (req, res) => {
  const adminId = await requireAdmin(req, res);
  if (!adminId) return;
  const client = await getDiscogsClientForUser(adminId);
  if (!client) {
    res.status(400).json({ error: "Admin has no Discogs token configured. Connect Discogs on the Account page first." });
    return;
  }
  const idRaw = req.query.id as string | undefined;
  const limitRaw = req.query.limit as string | undefined;
  const idFilter = idRaw ? parseInt(idRaw, 10) : undefined;
  const limit = limitRaw ? parseInt(limitRaw, 10) : undefined;
  if (Number.isFinite(idFilter as number)) {
    try {
      const result = await enrichBluesFromDiscogs(client, { idFilter });
      res.json({ ok: true, ...result });
    } catch (err: any) {
      console.error("[blues enrich-discogs single]", err);
      res.status(500).json({ error: err?.message ?? String(err) });
    }
    return;
  }
  if (_bluesDiscogsIdJob.status === "running") {
    res.status(409).json({ error: "Discogs ID enrichment already running", startedAt: _bluesDiscogsIdJob.startedAt, progress: _bluesDiscogsIdJob.progress });
    return;
  }
  _bluesDiscogsIdJob = { ..._emptyJob(), status: "running", startedAt: new Date().toISOString() };
  enrichBluesFromDiscogs(client, {
    limit: Number.isFinite(limit as number) ? limit : undefined,
    onProgress: (p) => { _bluesDiscogsIdJob.progress = p; },
  })
    .then(result => {
      _bluesDiscogsIdJob.status = "done";
      _bluesDiscogsIdJob.result = result;
      _bluesDiscogsIdJob.endedAt = new Date().toISOString();
    })
    .catch(err => {
      _bluesDiscogsIdJob.status = "error";
      _bluesDiscogsIdJob.error = err?.message ?? String(err);
      _bluesDiscogsIdJob.endedAt = new Date().toISOString();
      console.error("[blues enrich-discogs bulk]", err);
    });
  res.status(202).json({ ok: true, started: true, startedAt: _bluesDiscogsIdJob.startedAt });
});

app.get("/api/admin/blues/enrich-discogs/status", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  res.json(_bluesDiscogsIdJob);
});

// Phase 3c — YouTube top tracks. Per-row only because each call costs
// 100 quota units (default daily quota is 10000).
//   POST /api/admin/blues/enrich-yt/:id
app.post("/api/admin/blues/enrich-yt/:id", async (req, res) => {
  if (!await requireAdmin(req, res)) return;
  const id = parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) { res.status(400).json({ error: "bad id" }); return; }
  const apiKey = process.env.YOUTUBE_API_KEY ?? "";
  if (!apiKey) {
    res.status(503).json({ error: "YOUTUBE_API_KEY not configured on the server" });
    return;
  }
  try {
    const result = await enrichBluesArtistFromYouTube(id, apiKey);
    if ("error" in result) {
      res.status(502).json(result);
      return;
    }
    res.json({ ok: true, added: result.added });
  } catch (err: any) {
    console.error("[blues enrich-yt]", err);
    res.status(500).json({ error: err?.message ?? String(err) });
  }
});

// POST /api/ai-search — Claude music recommendations
app.post("/api/ai-search", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "no_token" }); return; }
  if (!anthropicKey) { res.status(503).json({ error: "AI not configured" }); return; }

  const q = (req.body.q as string ?? "").trim();
  if (!q) { res.status(400).json({ error: "Query required" }); return; }

  const prompt = `You are a deep-catalog music expert — a record-shop digger, not a radio programmer.
The user is searching for: "${q}"

Your job: surface LESSER-KNOWN, scene-accurate recommendations. Treat this like a crate-digging session, not a "best of" list.

Anchoring rules:
1. First, silently identify the specific scene, era, region, and sub-genre of the query (e.g. "Hi-Heel Sneakers" by Tommy Tucker = mid-1960s post-war US electric blues / R&B 12-bar). Stay inside that pocket.
2. STRONGLY AVOID household names and crossover hits. No Beatles, Stones, Big Bopper, Elvis, Dylan, Hendrix, Marley, Miles Davis, etc., unless the query is literally about them.
3. Prefer regional contemporaries, B-sides, single-album acts, label-mates, session players who cut their own records, and obscure scene peers.
4. If the query is a specific song/release, lean into other artists who recorded that exact style/era — not generic "similar artists".
5. Mix artist and release types. Releases should be specific records, not greatest-hits comps.

Return a JSON object with two fields:
- blurb: one sentence (max 30 words) naming the scene/era/sound these picks share — concrete, no filler.
- items: array of 12 recommendations.

Each item must include:
- name: artist name, or "Album Title by Artist"
- type: "artist" or "release"
- artist: the artist or band name (always present, even for releases)
- album: the album/release title (only for type "release", omit for "artist")
- label: a record label most associated with this entity if relevant (optional)
- description: one sentence — name the scene/era and why this is a deeper cut than the obvious answer
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
        // 12 items × structured JSON with richer descriptions blew past 1700;
        // 2800 leaves headroom so the JSON never truncates mid-array.
        max_tokens: 2800,
        messages: [{ role: "user", content: prompt }],
      }),
      context: "ai-search suggestions",
    });
    const data = await r.json() as any;
    if (!r.ok) {
      console.error("Anthropic API error:", data);
      res.status(502).json({ error: `AI error: ${data.error?.message ?? r.status}` });
      return;
    }
    let text = (data.content?.[0]?.text ?? "{}").trim();
    // Strip markdown code fences if present
    text = text.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "");
    let parsed: any;
    try {
      parsed = JSON.parse(text);
    } catch (parseErr) {
      // Defensive: if the model truncated mid-array (max_tokens hit, network
      // hiccup, etc.), salvage the well-formed prefix instead of returning
      // a generic 500. Trim back to the last complete `},` then close the
      // array+object brackets.
      console.warn("[ai-search] JSON parse failed, attempting salvage. stop_reason=", data.stop_reason);
      const lastClosed = text.lastIndexOf("},");
      if (lastClosed > 0) {
        const salvaged = text.slice(0, lastClosed + 1) + "]}";
        try { parsed = JSON.parse(salvaged); } catch { /* fall through */ }
      }
      if (!parsed) {
        res.status(502).json({ error: "AI returned malformed JSON", stop_reason: data.stop_reason });
        return;
      }
    }
    // Support both new { blurb, items } format and legacy bare array
    const recommendations = Array.isArray(parsed) ? parsed : (parsed.items ?? []);
    const blurb: string = Array.isArray(parsed) ? "" : (parsed.blurb ?? "");
    res.json({ recommendations, blurb });
  } catch (err) {
    console.error("AI search error:", err);
    res.status(500).json({ error: "AI search failed: " + (err as Error).message });
  }
});

// GET /api/wikipedia/search?q=X[&limit=N&offset=N] — list-style search for
// the wiki SPA page. Returns matching articles with title + HTML snippet
// (Wikipedia's <span class="searchmatch"> highlight markup preserved).
// Supports paging via offset so the frontend can implement "Load more".
app.get("/api/wikipedia/search", async (req, res) => {
  if (!await requireUser(req, res)) return;
  const q = ((req.query.q as string) ?? "").trim();
  if (!q) { res.status(400).json({ error: "Missing q" }); return; }
  // Clamp limit to Wikipedia's 50/page ceiling and a sensible default of 20.
  const limit = Math.min(50, Math.max(1, parseInt(String(req.query.limit ?? "20"), 10) || 20));
  const offset = Math.max(0, parseInt(String(req.query.offset ?? "0"), 10) || 0);
  const wikiHeaders = {
    "User-Agent": "SeaDisco/1.0 (+https://seadisco.com; vinyl discovery app)",
    "Accept": "application/json",
  };
  try {
    const params = [
      "action=query", "format=json", "list=search",
      `srlimit=${limit}`, `sroffset=${offset}`,
      "srprop=snippet",
      `srsearch=${encodeURIComponent(q)}`,
    ].join("&");
    const url = `https://en.wikipedia.org/w/api.php?${params}`;
    const r = await loggedFetch("wikipedia", url, { context: "wiki search list", headers: wikiHeaders });
    if (!r.ok) { console.error("[wikipedia/search] HTTP", r.status); res.status(502).json({ error: "Wikipedia search failed", status: r.status }); return; }
    const data = await r.json() as any;
    const rows = (data?.query?.search ?? []) as Array<{ title: string; snippet: string }>;
    const results = rows.map(s => ({
      title: s.title,
      snippet: s.snippet ?? "",
      url: `https://en.wikipedia.org/wiki/${encodeURIComponent((s.title || "").replace(/ /g, "_"))}`,
    }));
    const totalhits = data?.query?.searchinfo?.totalhits ?? null;
    const nextOffset = data?.continue?.sroffset ?? null;
    res.json({ results, offset, limit, totalhits, nextOffset });
  } catch (err) {
    console.error("[wikipedia/search] error:", err);
    res.status(500).json({ error: "Wikipedia search failed" });
  }
});

// GET /api/wikipedia/lookup?q=X[&full=1] — fetch a Wikipedia article for the
// in-app popup. Default returns only the lead section (fast); pass full=1 to
// load the entire article body so users can read it without leaving SeaDisco.
app.get("/api/wikipedia/lookup", async (req, res) => {
  if (!await requireUser(req, res)) return;
  const q = ((req.query.q as string) ?? "").trim();
  if (!q) { res.status(400).json({ error: "Missing q" }); return; }
  const full = req.query.full === "1" || req.query.full === "true";
  // Wikipedia API requires a descriptive User-Agent per their policy:
  // https://meta.wikimedia.org/wiki/User-Agent_policy
  // Without one, requests can be 403'd or rate-limited aggressively.
  const wikiHeaders = {
    "User-Agent": "SeaDisco/1.0 (+https://seadisco.com; vinyl discovery app)",
    "Accept": "application/json",
  };
  try {
    // Step 1: opensearch to find canonical title
    const searchUrl = `https://en.wikipedia.org/w/api.php?action=opensearch&format=json&limit=1&search=${encodeURIComponent(q)}`;
    const sr = await loggedFetch("wikipedia", searchUrl, { context: "wiki opensearch", headers: wikiHeaders });
    if (!sr.ok) { console.error("[wikipedia] opensearch HTTP", sr.status); res.status(502).json({ error: "Wikipedia search failed", status: sr.status }); return; }
    const sdata = await sr.json() as any;
    const title = Array.isArray(sdata) && Array.isArray(sdata[1]) && sdata[1][0] ? sdata[1][0] : null;
    if (!title) { res.json({ found: false }); return; }
    const pageUrl = (Array.isArray(sdata[3]) && sdata[3][0]) ? sdata[3][0] : `https://en.wikipedia.org/wiki/${encodeURIComponent(title.replace(/ /g, "_"))}`;
    // Step 2: fetch the article body. The TextExtracts API gives clean
    // paragraph HTML (no infoboxes/refs/edit-links). exintro=1 → lead only;
    // omit it to get the full article body. Always pull the thumbnail too.
    const extractParams = [
      "action=query",
      "format=json",
      "prop=extracts|pageimages",
      ...(full ? [] : ["exintro=1"]),
      "piprop=thumbnail",
      "pithumbsize=200",
      "redirects=1",
      `titles=${encodeURIComponent(title)}`,
    ].join("&");
    const extractUrl = `https://en.wikipedia.org/w/api.php?${extractParams}`;
    const er = await loggedFetch("wikipedia", extractUrl, { context: full ? "wiki extract full" : "wiki extract", headers: wikiHeaders });
    if (!er.ok) { console.error("[wikipedia] extract HTTP", er.status); res.status(502).json({ error: "Wikipedia extract failed", status: er.status }); return; }
    const edata = await er.json() as any;
    const pages = edata?.query?.pages ?? {};
    const firstKey = Object.keys(pages)[0];
    const page = firstKey ? pages[firstKey] : null;
    const html = page?.extract ?? "";
    const thumbnail = page?.thumbnail?.source ?? null;
    const finalTitle = page?.title ?? title;
    res.json({ found: !!html, title: finalTitle, url: pageUrl, html, thumbnail, full });
  } catch (err) {
    console.error("[wikipedia/lookup] error:", err);
    res.status(500).json({ error: "Wikipedia lookup failed" });
  }
});

// POST /api/result-quality — Claude gives a short phrase on result relevance
app.post("/api/result-quality", express.json(), async (req, res) => {
  if (!await requireUser(req, res)) return;
  if (!anthropicKey) { res.json({ phrase: null }); return; }
  const { query, titles } = req.body ?? {};
  if (!query || !Array.isArray(titles) || !titles.length) { res.json({ phrase: null }); return; }

  const titleList = (titles as string[]).slice(0, 6).map((t, i) => `${i + 1}. ${t}`).join("\n");
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
    const data = await r.json() as any;
    if (!r.ok) {
      console.error("[result-quality] Anthropic API error:", JSON.stringify(data));
      res.json({ phrase: null });
      return;
    }
    const phrase = (data.content?.[0]?.text ?? "").trim().replace(/[.!?]+$/, "") || null;
    console.log("[result-quality] phrase:", phrase);
    res.json({ phrase });
  } catch (err) {
    console.error("[result-quality] fetch error:", err);
    res.json({ phrase: null });
  }
});

// GET /search?q=pink+floyd&type=master&year=1973&page=1&per_page=10
app.get("/search", async (req, res) => {
  const userId = await requireUser(req, res);
  if (!userId) return;

  const rawQ   = (req.query.q as string) ?? "";
  const artist = stripArtistSuffix(req.query.artist as string | undefined);
  const rawLabel = (req.query.label as string) ?? "";
  const rawRelease = (req.query.release_title as string) ?? "";

  // Each field is sent as its own dedicated Discogs param — no promotion to q
  const q = rawQ;
  const searchArtist: string | undefined = artist || undefined;
  const searchLabel: string | undefined = rawLabel || undefined;
  const searchRelease: string | undefined = rawRelease || undefined;

  // Touch activity for the signed-in user
  touchUserActivity(userId).catch(() => {});

  const dc = await getDiscogsForRequest(req);
  if (!dc) {
    res.status(401).json({ error: "no_token", message: "Connect your Discogs account in Account settings to search." });
    return;
  }

  try {
    const results = await dc.search(q, {
      type: req.query.type as "release" | "master" | "artist" | "label" | undefined,
      artist: searchArtist,
      releaseTitle: searchRelease,
      label: searchLabel,
      year: req.query.year as string | undefined,
      genre: req.query.genre as string | undefined,
      style: req.query.style as string | undefined,
      format: req.query.format as string | undefined,
      country: req.query.country as string | undefined,
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
  if (!await requireUser(req, res)) return;
  const id = parseInt(req.params.id, 10);
  const dc = await getDiscogsForRequest(req);
  if (!dc) { res.status(503).json({ error: "No Discogs token configured" }); return; }
  try {
    const result = await dc.getRelease(req.params.id);
    // Always save fresh data to cache
    cacheRelease(id, "release", result as object).catch(() => {});
    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Discogs API error" });
  }
});

// GET /master/:id
app.get("/master/:id", async (req, res) => {
  if (!await requireUser(req, res)) return;
  const id = parseInt(req.params.id, 10);
  const dc = await getDiscogsForRequest(req);
  if (!dc) { res.status(503).json({ error: "No Discogs token configured" }); return; }
  try {
    const result = await dc.getMasterRelease(req.params.id);
    // Always save fresh data to cache
    cacheRelease(id, "master", result as object).catch(() => {});
    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Discogs API error" });
  }
});

// GET /artist/:id
app.get("/artist/:id", async (req, res) => {
  if (!await requireUser(req, res)) return;
  const dc = await getDiscogsForRequest(req);
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

// GET /artist-bio?name=Miles+Davis[&id=123456] — Discogs bio
// If `id` is supplied the artist is fetched directly (no ambiguous name search).
app.get("/artist-bio", async (req, res) => {
  if (!await requireUser(req, res)) return;
  res.setHeader("Cache-Control", "private, max-age=3600"); // 1 hour
  const nameRaw = req.query.name as string;
  const idParam = req.query.id ? parseInt(req.query.id as string, 10) : null;

  if (!nameRaw || !nameRaw.trim()) {
    res.status(400).json({ error: "Missing required query parameter: name" });
    return;
  }

  const dc = await getDiscogsForRequest(req);
  if (!dc) { res.json({ profile: null }); return; }

  const mapNames = (arr: any[]) =>
    (arr ?? []).filter(x => x?.name)
               .map(x => ({ name: x.name as string, active: x.active, id: x.id as number | undefined }));

  // ── Fast path: direct lookup by Discogs ID ──────────────────────────────
  if (idParam) {
    try {
      const artist = await dc.getArtist(idParam) as any;
      let profile: string | null = artist?.profile ?? null;
      if (profile) profile = await resolveDiscogsIds(profile, dc);
      res.json({
        profile,
        name: artist?.name ?? nameRaw,
        alternatives: [],
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
    const discogsResults = await dc.search(nameForSearch, { type: "artist", perPage: 20 }) as any;

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
async function resolveDiscogsIds(profile: string, dc: DiscogsClient): Promise<string> {
  const idPattern = /\[([rmal])=?(\d+)\]/g;
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
      } else if (type === "l") {
        const r = await dc.getLabel(id) as any;
        displayName = r?.name ?? "";
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
  if (!await requireUser(req, res)) return;
  res.setHeader("Cache-Control", "private, max-age=3600"); // 1 hour
  const name = req.query.name as string;
  if (!name || !name.trim()) {
    res.status(400).json({ error: "Missing required query parameter: name" });
    return;
  }

  const dc = await getDiscogsForRequest(req);
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
  if (!await requireUser(req, res)) return;
  res.setHeader("Cache-Control", "private, max-age=86400"); // 24 hours
  const genre = req.query.genre as string;
  if (!genre || !genre.trim()) {
    res.status(400).json({ error: "Missing required query parameter: genre" });
    return;
  }
  if (!anthropicKey) { res.json({ profile: null }); return; }

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
  if (!await requireUser(req, res)) return;
  res.setHeader("Cache-Control", "private, max-age=300"); // 5 min
  const { id } = req.params;
  const type = (req.query.type as string) ?? "release";
  const dc = await getDiscogsForRequest(req);
  if (!dc) { res.json({ numForSale: 0, lowestPrice: null }); return; }

  try {
    let releaseId = id;
    if (type === "master") {
      // Try cache first for the master lookup
      const cachedMaster = await getCachedRelease(parseInt(id, 10), "master").catch(() => null);
      const master = cachedMaster ?? await dc.getMasterRelease(id) as any;
      if (!cachedMaster && master) cacheRelease(parseInt(id, 10), "master", master).catch(() => {});
      releaseId = String(master?.main_release ?? id);
    }

    const stats = await dc.getMarketplaceStats(releaseId, "USD") as any;
    // Cache price data opportunistically
    const lowest = stats?.lowest_price?.value ?? null;
    const median = stats?.median_price?.value ?? null;
    const highest = stats?.highest_price?.value ?? null;
    const numForSale = stats?.num_for_sale ?? 0;
    if (lowest != null || median != null) {
      upsertPriceCache(parseInt(String(releaseId), 10), lowest, median, highest, numForSale).catch(() => {});
      appendPriceHistory(parseInt(String(releaseId), 10), lowest, median, highest, numForSale).catch(() => {});
    }
    res.json({
      numForSale,
      lowestPrice: lowest,
      medianPrice: median,
      highestPrice: highest,
      currency:    stats?.lowest_price?.currency ?? "USD",
      releaseId,
    });
  } catch (err) {
    console.error(err);
    res.json({ numForSale: 0, lowestPrice: null });
  }
});

// GET /price-suggestions/:id — condition-based price estimates for a release
app.get("/api/price-suggestions/:id", async (req, res) => {
  if (!await requireUser(req, res)) return;
  res.setHeader("Cache-Control", "private, max-age=300");
  const { id } = req.params;
  const dc = await getDiscogsForRequest(req);
  if (!dc) { res.status(503).json({ error: "No Discogs client" }); return; }
  try {
    const data = await dc.getPriceSuggestions(id);
    res.json(data);
  } catch (err: any) {
    console.error("price-suggestions error:", err?.message ?? err);
    res.status(500).json({ error: "Failed to fetch price suggestions" });
  }
});

// GET /master-versions/:id — all pressings/versions of a master release.
// Discogs paginates at 100 per page; popular masters can have several
// hundred pressings, so walk pages until the response runs out (with a
// safety cap to avoid runaway loops).
app.get("/master-versions/:id", async (req, res) => {
  if (!await requireUser(req, res)) return;
  const { id } = req.params;
  const dc = await getDiscogsForRequest(req);
  if (!dc) { res.json({ versions: [] }); return; }
  try {
    const PER_PAGE = 100;
    const MAX_PAGES = 10;  // 1,000 versions ceiling — well above any real master
    const collected: any[] = [];
    let page = 1;
    while (page <= MAX_PAGES) {
      const data = await dc.getMasterVersions(id, { page, perPage: PER_PAGE, sort: "released", sortOrder: "asc" }) as any;
      const chunk = data?.versions ?? [];
      collected.push(...chunk);
      const totalPages = data?.pagination?.pages ?? 1;
      if (page >= totalPages || chunk.length < PER_PAGE) break;
      page++;
    }
    const versions = collected.map((v: any) => ({
      id:           v.id,
      title:        v.title,
      label:        v.label,
      catno:        v.catno,
      country:      v.country,
      year:         v.released,
      format:       v.format,
      majorFormats: v.major_formats ?? [],
      url:          v.resource_url ? `https://www.discogs.com/release/${v.id}` : null,
    }));
    res.json({ versions });
  } catch (err: any) {
    console.error(`[master-versions/${id}] Error:`, err?.message ?? err);
    res.status(500).json({ error: err?.message ?? "Failed to load versions", versions: [] });
  }
});

// GET /series-releases/:id — all releases in a Discogs series (series are label-type entities)
app.get("/series-releases/:id", async (req, res) => {
  if (!await requireUser(req, res)) return;
  const { id } = req.params;
  const dc = await getDiscogsForRequest(req);
  if (!dc) { res.json({ releases: [], name: "" }); return; }
  try {
    res.setHeader("Cache-Control", "private, max-age=3600");
    // Fetch series info + first page of releases in parallel
    const [labelData, relData] = await Promise.all([
      dc.getLabel(id) as Promise<any>,
      dc.getLabelReleases(id, { perPage: 100 }) as Promise<any>,
    ]);
    const seriesName = labelData?.name ?? `Series ${id}`;
    const releases = (relData?.releases ?? []).map((r: any) => ({
      id:      r.id,
      title:   r.title ?? "",
      artist:  r.artist ?? "",
      catno:   r.catno ?? "",
      year:    r.year ?? 0,
      format:  r.format ?? "",
      thumb:   r.thumb ?? "",
      country: r.country ?? "",
    }));
    res.json({ name: seriesName, releases, total: relData?.pagination?.items ?? releases.length });
  } catch (err: any) {
    console.error(`[series-releases/${id}] Error:`, err?.message ?? err);
    res.status(500).json({ error: err?.message ?? "Failed to load series", releases: [], name: "" });
  }
});

// POST /api/admin/extras/fetch — manual trigger for inventory/lists sync
app.post("/api/admin/extras/fetch", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  res.json({ ok: true, message: "Extras sync started" });
  (async () => {
    const users = await getAllUsersForSync();
    for (const user of users) {
      try {
        const userClient = await getDiscogsClientForUser(user.clerkUserId);
        if (!userClient) { console.warn(`[admin-extras] no auth for ${user.username}, skipping`); continue; }
        const result = await syncUserExtras(user.clerkUserId, user.username, userClient);
        console.log(`[admin-extras] ${user.username}: ${result.inventory} inventory, ${result.lists} lists, ${result.orders ?? 0} orders`);
        await sleep(30000); // 30s between users
      } catch (err) {
        console.error(`[admin-extras] Error syncing ${user.username}:`, err);
      }
    }
    console.log("[admin-extras] Complete");
  })();
});

// GET /api/admin/api-log — view API request log (last 24h by default)
app.get("/api/admin/api-log", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const service = req.query.service as string | undefined;
  const errorsOnly = req.query.errors === "true";
  const scheduledOnly = req.query.scheduled === "true";
  const hours = Math.min(parseInt(req.query.hours as string) || 24, 168); // max 7 days
  const result = await getApiRequestLog({ service: service || undefined, errorsOnly, scheduledOnly, hours });
  res.json(result);
});

// GET /api/admin/api-stats — 24h summary by service
app.get("/api/admin/api-stats", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const hours = Math.min(parseInt(req.query.hours as string) || 24, 168);
  const stats = await getApiRequestStats(hours);
  res.json({ stats });
});

// GET /api/admin/db-stats — row counts for all tables
app.get("/api/admin/db-stats", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = ADMIN_CLERK_ID;
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  try {
    const tables = await getTableRowCounts();
    const totalRows = tables.reduce((sum, t) => sum + (t.rows > 0 ? t.rows : 0), 0);
    res.json({ tables, totalRows });
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

// Helper: ms until the next occurrence of HH:MM Pacific, repeating every intervalH hours
function msUntilPacific(hour: number, minute: number, intervalH: number): number {
  const now = new Date();
  const pacificStr = now.toLocaleString("en-US", { timeZone: "America/Los_Angeles" });
  const pacific = new Date(pacificStr);
  // Try each upcoming slot today and tomorrow
  const base = new Date(pacific);
  base.setHours(hour, minute, 0, 0);
  // Rewind to the most recent slot at or before now, then step forward
  while (base.getTime() > pacific.getTime()) base.setTime(base.getTime() - intervalH * 3600000);
  // Step forward to the next future slot
  while (base.getTime() <= pacific.getTime()) base.setTime(base.getTime() + intervalH * 3600000);
  return base.getTime() - pacific.getTime();
}

const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3001;
// Scheduled sync: collection/wantlist — frequency based on user activity
// Active (< 7 days): every 6 hours (every sync run)
// Inactive 7–14 days: once daily (every 4th run)
// Inactive 14–30 days: every 3 days (every 12th run)
// Inactive 30+ days: weekly (every 28th run)
let _syncRunCount = 0;

function startDailySyncSchedule() {
  async function runScheduledSync() {
    _syncRunCount++;
    console.log(`[sync-schedule] Starting sync run #${_syncRunCount}`);
    _syncAbort = false;
    const users = await getAllUsersForSync();

    // Fetch Clerk user activity data to determine sync tiers
    const clerkSecret = process.env.CLERK_SECRET_KEY ?? "";
    const lastActiveMap = new Map<string, number>(); // clerkUserId → timestamp ms
    if (clerkSecret) {
      try {
        let offset = 0;
        while (true) {
          const resp = await fetch(`https://api.clerk.com/v1/users?limit=100&offset=${offset}`, {
            headers: { Authorization: `Bearer ${clerkSecret}` },
          });
          if (!resp.ok) break;
          const clerkUsers = await resp.json() as Array<{ id: string; last_active_at: number | null }>;
          if (!clerkUsers.length) break;
          for (const u of clerkUsers) {
            if (u.last_active_at) {
              const ts = u.last_active_at > 1e12 ? u.last_active_at : u.last_active_at * 1000;
              lastActiveMap.set(u.id, ts);
            }
          }
          if (clerkUsers.length < 100) break;
          offset += 100;
        }
      } catch { /* proceed without activity data — sync everyone */ }
    }

    const now = Date.now();
    const DAY = 86400000;
    let synced = 0, skipped = 0;

    for (const user of users) {
      if (_syncAbort) { console.log("[sync-schedule] Aborted"); break; }

      // Determine sync frequency based on last activity
      const lastActive = lastActiveMap.get(user.clerkUserId);
      const daysInactive = lastActive ? (now - lastActive) / DAY : 0;
      let shouldSync = true;

      if (daysInactive > 90) {
        // 3+ months inactive: revoke sessions and skip sync entirely
        if (clerkSecret) {
          try {
            await fetch(`https://api.clerk.com/v1/users/${user.clerkUserId}/sessions/revoke`, {
              method: "POST",
              headers: { Authorization: `Bearer ${clerkSecret}` },
            });
            console.log(`[sync-schedule] ${user.username} inactive ${Math.round(daysInactive)}d — sessions revoked, sync skipped`);
          } catch { /* ignore revoke errors */ }
        }
        skipped++;
        continue;
      } else if (daysInactive > 30) {
        shouldSync = _syncRunCount % 28 === 0; // weekly
      } else if (daysInactive > 14) {
        shouldSync = _syncRunCount % 12 === 0; // every 3 days
      } else if (daysInactive > 7) {
        shouldSync = _syncRunCount % 4 === 0;  // daily
      }
      // else: active users sync every run (every 6h)

      if (!shouldSync) {
        skipped++;
        continue;
      }

      try {
        const userClient = await getDiscogsClientForUser(user.clerkUserId);
        if (!userClient) { console.warn(`[sync-schedule] no auth for ${user.username}, skipping`); continue; }
        const tier = daysInactive > 30 ? "weekly" : daysInactive > 14 ? "3-day" : daysInactive > 7 ? "daily" : "active";
        console.log(`[sync-schedule] Syncing ${user.username} (${tier}, ${Math.round(daysInactive)}d inactive)`);
        await runBackgroundSync(user.clerkUserId, userClient, user.username, true, true);
        synced++;
        await new Promise(r => setTimeout(r, 30000));
      } catch (err) {
        console.error(`[sync-schedule] Error syncing ${user.username}:`, err);
      }
    }
    console.log(`[sync-schedule] Run #${_syncRunCount} complete: ${synced} synced, ${skipped} skipped`);

    // Prune stale data and hibernate inactive users (previously in feed schedule)
    try {
      const stale = await pruneAllStaleData();
      const staleTotal = Object.values(stale).reduce((a, b) => a + b, 0);
      if (staleTotal > 0) console.log(`[sync-schedule] Pruned stale data: ${JSON.stringify(stale)}`);
      const hibernated = await hibernateInactiveUsers().catch(() => 0);
      if (hibernated) console.log(`[sync-schedule] ${hibernated} users hibernated`);
    } catch (e) { console.error("[sync-schedule] prune/hibernate error:", e); }
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
async function syncUserExtras(userId: string, username: string, client: DiscogsClient): Promise<{ inventory: number; lists: number; orders: number }> {
  const getHeaders = (url: string) => client.buildHeaders("GET", url);

  // Simple fetch with retry for extras sync
  async function extrasFetch(url: string, retries = 3): Promise<Response> {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const r = await loggedFetch("discogs", url, { headers: getHeaders(url), signal: AbortSignal.timeout(15000), context: `extras: ${username}` });
        if (r.ok || r.status === 401 || r.status === 403) return r;
        if (r.status === 429 || r.status >= 500) {
          if (attempt < retries) { await sleep(15000 * attempt); continue; }
          throw new Error(`HTTP ${r.status} after ${retries} attempts`);
        }
        throw new Error(`HTTP ${r.status}`);
      } catch (err: any) {
        if (attempt >= retries) throw err;
        await sleep(10000 * attempt);
      }
    }
    throw new Error("unreachable");
  }

  let inventory = 0, lists = 0, orders = 0;

  // Inventory (paginated) — Discogs returns 401 if user isn't a seller
  try {
    for (let page = 1; ; page++) {
      if (page > 1) await sleep(1200);
      const r = await extrasFetch(
        `https://api.discogs.com/users/${encodeURIComponent(username)}/inventory?per_page=100&page=${page}&sort=listed&sort_order=desc`,
      );
      if (r.status === 401 || r.status === 403) { console.log(`[extras] ${username}: no inventory (${r.status})`); break; }
      const data = await r.json() as any;
      const listings: any[] = data.listings ?? [];
      if (!listings.length) break;
      const items = listings.map((l: any) => ({
        listingId:      l.id as number,
        releaseId:      l.release?.id ?? undefined,
        data:           l as object,
        status:         l.status ?? "For Sale",
        priceValue:     parseFloat(l.price?.value) || undefined,
        priceCurrency:  l.price?.currency ?? "USD",
        condition:      l.condition ?? undefined,
        sleeveCondition: l.sleeve_condition ?? undefined,
        postedAt:       l.posted ? new Date(l.posted) : undefined,
      }));
      await upsertInventoryItems(userId, items);
      inventory += items.length;
      if (listings.length < 100) break;
    }
    await updateInventorySyncedAt(userId);
    console.log(`[extras] ${username}: ${inventory} inventory listings synced`);
  } catch (err) {
    console.error(`[extras] ${username} inventory error:`, err);
  }

  // Lists (not paginated) — Discogs returns 401 if user has no public lists
  try {
    await sleep(1200);
    const r = await extrasFetch(
      `https://api.discogs.com/users/${encodeURIComponent(username)}/lists?per_page=100`,
    );
    if (r.status === 401 || r.status === 403) { console.log(`[extras] ${username}: no lists (${r.status})`); }
    const data = r.ok ? (await r.json() as any) : { lists: [] };
    const userLists: any[] = data.lists ?? [];
    if (userLists.length) {
      const items = userLists.map((l: any) => ({
        listId:      l.id as number,
        name:        l.name ?? "",
        description: l.description ?? undefined,
        itemCount:   l.item_count ?? 0,
        isPublic:    l.public !== false,
        data:        l as object,
      }));
      await upsertUserLists(userId, items);
      lists = items.length;
    }
    console.log(`[extras] ${username}: ${lists} lists synced`);

    // Fetch items for each list (Discogs API: GET /lists/{id})
    let totalListItems = 0;
    for (const list of userLists) {
      try {
        await sleep(1200);
        const lr = await extrasFetch(`https://api.discogs.com/lists/${list.id}`);
        if (!lr.ok) { console.log(`[extras] ${username}: list ${list.id} items fetch ${lr.status}`); continue; }
        const listData = await lr.json() as any;
        const listItems: any[] = listData.items ?? [];
        if (listItems.length) {
          const parsed = listItems.map((item: any) => ({
            discogsId:  item.id as number,
            entityType: item.type ?? "release",
            comment:    item.comment ?? undefined,
            data:       item as object,
          }));
          await upsertListItems(userId, list.id, parsed);
          totalListItems += parsed.length;
        }
      } catch (err) {
        console.error(`[extras] ${username} list ${list.id} items error:`, err);
      }
    }
    console.log(`[extras] ${username}: ${totalListItems} list items synced across ${lists} lists`);
  } catch (err) {
    console.error(`[extras] ${username} lists error:`, err);
  }

  // Orders (seller side only). Discogs returns 401/403/404 if the user has
  // never sold or lacks marketplace permission — log and skip quietly.
  try {
    for (let page = 1; ; page++) {
      await sleep(1200);
      const r = await extrasFetch(
        `https://api.discogs.com/marketplace/orders?per_page=100&page=${page}&sort=last_activity&sort_order=desc`,
      );
      if (r.status === 401 || r.status === 403 || r.status === 404) {
        console.log(`[extras] ${username}: no seller orders (${r.status})`);
        break;
      }
      if (!r.ok) break;
      const data = await r.json() as any;
      const rows: any[] = data.orders ?? [];
      if (!rows.length) break;
      const mapped = rows.map((o: any) => ({
        orderId:       String(o.id),
        status:        o.status ?? undefined,
        buyerUsername: o.buyer?.username ?? undefined,
        itemCount:    Array.isArray(o.items) ? o.items.length : undefined,
        totalValue:    parseFloat(o.total?.value) || undefined,
        totalCurrency: o.total?.currency ?? undefined,
        createdAt:     o.created ? new Date(o.created) : undefined,
        data:          o as object,
      }));
      await upsertUserOrders(userId, mapped);
      orders += mapped.length;
      if (rows.length < 100) break;
    }
    await updateOrdersSyncedAt(userId);
    console.log(`[extras] ${username}: ${orders} seller orders synced`);
  } catch (err) {
    console.error(`[extras] ${username} orders error:`, err);
  }

  return { inventory, lists, orders };
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
          const userClient = await getDiscogsClientForUser(user.clerkUserId);
          if (!userClient) { console.warn(`[extras-sync] no auth for ${user.username}, skipping`); continue; }
          await syncUserExtras(user.clerkUserId, user.username, userClient);
          await sleep(30000); // 30s between users
        } catch (err) {
          console.error(`[extras-sync] Error syncing ${user.username}:`, err);
        }
      }
      console.log("[extras-sync] Complete");
      schedule();
    }, ms);
  }
  schedule();
}

app.listen(PORT, "0.0.0.0", async () => {
  console.log(`Discogs search API listening on port ${PORT}`);
  if (process.env.APP_DB_URL) {
    // Reset any syncs orphaned by a server restart
    try {
      const stuck = await resetAllSyncingStatuses();
      if (stuck > 0) console.log(`Startup: reset ${stuck} orphaned syncing status(es)`);
    } catch (e) { console.error("Startup: failed to reset stuck syncs:", e); }

    startDailySyncSchedule();
    startExtrasSyncSchedule();
  }
});
