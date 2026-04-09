import express from "express";
import compression from "compression";
import cookieParser from "cookie-parser";
import crypto from "crypto";
import fs from "fs";
import { createRemoteJWKSet, jwtVerify } from "jose";
import { fileURLToPath } from "url";
import path from "path";
import { DiscogsClient, signOAuthRequest } from "./discogs-client.js";
import { initDb, getAllUsersForSync, getAllUsersSyncStatus, getUserCount, getActiveUserCount, touchUserActivity, isUserHibernated, reactivateUser, hibernateInactiveUsers, getUserToken, setUserToken, deleteUserToken, deleteUserData, saveFeedback, getFeedback, deleteFeedback, getDiscogsUsername, getClerkUserIdByUsername, setDiscogsUsername, getSyncStatus, updateSyncProgress, upsertCollectionItems, upsertCollectionFolders, upsertWantlistItems, getCollectionPage, getWantlistPage, getAllCollectionItems, getAllWantlistItems, getCollectionIds, getWantlistIds, getCollectionFacets, getWantlistFacets, getCollectionFolderList, updateCollectionSyncedAt, updateWantlistSyncedAt, getFreshReleases, searchFreshReleases, getFreshStats, getWantedItems, upsertGearListings, getGearListings, markExpiredGearListings, getGearStats, logGearFetch, upsertVinylListings, getVinylListings, markExpiredVinylListings, getVinylStats, logVinylFetch, resetAllSyncingStatuses, upsertFeedArticle, getFeedArticles, pruneFeedArticles, pruneAllStaleData, upsertLiveEvents, getLiveEvents, pruneLiveEvents, upsertInventoryItems, updateInventorySyncedAt, upsertUserLists, getInventoryPage, getUserListsList, getExistingYouTubeUrls, logApiRequest, getApiRequestLog, getApiRequestStats, getUserCollectionStats, getCachedRelease, cacheRelease, storeOAuthRequestToken, getOAuthRequestToken, deleteOAuthRequestToken, pruneOAuthRequestTokens, setOAuthCredentials, getOAuthCredentials, clearOAuthCredentials, setDiscogsProfile, getDiscogsProfile, deleteCollectionItem, deleteWantlistItem, updateCollectionRating, updateCollectionFolder, getCollectionInstance, getCollectionInstances, getCollectionMultiInstanceCounts, updateCollectionNotes, renameCollectionFolder, deleteCollectionFolder, moveAllCollectionItemsBetweenFolders, getFolderContents, upsertPriceCache, appendPriceHistory, getPriceCache, getPriceHistory, getStaleReleaseIds, prunePriceHistory, getPriceStats, getSavedSearches, saveSavedSearch, deleteSavedSearch, pruneWantlistItems, pruneCollectionItems, getFavoriteIds, getFavorites, getRandomPublicFavorites, addFavorite, removeFavorite, getAllFavoriteCounts, upsertListItems, getListItems, getListMembership, getInventoryIds, getListItemStats, getRandomRecords, getDefaultAddFolderId, setDefaultAddFolderId, getInventoryItem, deleteInventoryItem, getInventoryListingIdsByRelease, upsertUserOrders, updateOrdersSyncedAt, getOrdersCount, getUserOrdersPage, getUserOrder, upsertOrderMessages, getOrderMessages, markOrderViewed, getUnreadOrdersCount, getEbayRateCount, incrementEbayClickCount, pruneEbaySearchCache, getTableRowCounts, updateListingFromDetail } from "./db.js";
import { startFreshSyncSchedule, runFreshSync } from "./sync-fresh-releases.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const sharedToken     = process.env.DISCOGS_TOKEN ?? "";
const anthropicKey    = process.env.ANTHROPIC_API_KEY    ?? "";
// Discogs OAuth 1.0a consumer credentials (register at discogs.com/settings/developers)
const discogsConsumerKey    = process.env.DISCOGS_CONSUMER_KEY    ?? "";
const discogsConsumerSecret = process.env.DISCOGS_CONSUMER_SECRET ?? "";
// Publishable key sent to frontend via /api/config
const authPk      = process.env.AUTH_PK ?? "";
// Set REQUIRE_AUTH=true to require users to sign in and provide their own Discogs token
const requireAuth = process.env.REQUIRE_AUTH === "true";

// Concert API keys
const ticketmasterKey = process.env.TICKETMASTER_API_KEY ?? "";
const bandsintownAppId = "seadisco"; // Bandsintown just needs an app identifier

// YouTube API key
const youtubeApiKey = process.env.YOUTUBE_API_KEY ?? "";

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
let _apiKillSwitch = false;
let _lastPriceUpdate: Date | null = null;

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

// Shared Discogs client (used as fallback when user has no personal token)
const discogs = sharedToken ? new DiscogsClient(sharedToken) : null;

// ── IP rate limiter for unauthenticated (shared-token) searches ───────────
const UNAUTH_LIMIT     = 5;
const LIMIT_WINDOW_MS  = 24 * 60 * 60 * 1000; // 24 hours
const ipCounts = new Map<string, { count: number; resetAt: number }>();

// IPs that bypass the rate limit and auth requirement entirely
const IP_WHITELIST = new Set<string>([
]);

function clientIp(req: express.Request): string {
  // Railway sets X-Forwarded-For with the real client IP as the first entry
  const xff = (req.headers["x-forwarded-for"] as string ?? "").split(",")[0].trim();
  const ip = xff || (req.ip ?? "unknown").replace(/^::ffff:/, "").trim();
  return ip;
}


function checkRateLimit(ip: string): { allowed: boolean; remaining: number } {
  if (IP_WHITELIST.has(ip)) return { allowed: true, remaining: UNAUTH_LIMIT };
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

// Resolve Discogs token: check OAuth first → PAT → shared token → null
async function getTokenForRequest(req: express.Request, allowFallback = false): Promise<string | null> {
  const userId = await getClerkUserId(req);
  if (userId) {
    const userToken = await getUserToken(userId);
    if (userToken && userToken !== "__oauth__") return userToken;
  }
  // Fall back to shared token when auth is disabled OR when explicitly allowed (bio endpoints)
  if (!requireAuth || allowFallback) return sharedToken || null;
  return null;
}

async function getDiscogsForRequest(req: express.Request, allowFallback = false): Promise<DiscogsClient | null> {
  const userId = await getClerkUserId(req);
  // Check if user has OAuth credentials first
  if (userId && discogsConsumerKey) {
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
  const t = await getTokenForRequest(req, allowFallback);
  if (!t) return null;
  if (t === sharedToken && discogs) return discogs;
  return new DiscogsClient(t);
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
// Reads index.html/account.html/admin.html at startup and substitutes
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
app.get("/account.html", (_req, res, next) => { if (!_sendHtml(res, "account.html")) next(); });
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

// GET /api/config — public config for the frontend
app.get("/api/config", (_req, res) => {
  res.setHeader("Cache-Control", "public, max-age=600"); // 10 min
  res.json({ clerkPublishableKey: authPk, authEnabled: requireAuth });
});

// GET /api/user-count — public, returns active user count + limit
app.get("/api/user-count", async (_req, res) => {
  try {
    res.setHeader("Cache-Control", "public, max-age=60");
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
setInterval(() => { pruneEbaySearchCache().catch(() => {}); }, 60 * 60 * 1000);

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
    if (!r.ok && r.status !== 204) {
      const text = await r.text();
      res.status(r.status).json({ error: `Discogs error: ${text}` }); return;
    }
    await deleteInventoryItem(ctx.userId, listingId);
    res.json({ ok: true });
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

// GET /api/live/upcoming — serve upcoming events from DB
app.get("/api/live/upcoming", async (_req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    const events = await getLiveEvents(200);
    res.json({ events });
  } catch {
    res.json({ events: [] });
  }
});

// Helper: extract full event data from a Ticketmaster event object
function mapTmEvent(ev: any) {
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
    imageUrl: ev.images?.find((i: any) => i.ratio === "16_9" && i.width >= 500)?.url
      ?? ev.images?.[0]?.url ?? "",
    priceMin: price?.min ?? undefined,
    priceMax: price?.max ?? undefined,
    currency: price?.currency ?? undefined,
    status: ev.dates?.status?.code ?? "",
    segment,
    source: "ticketmaster" as const,
  };
}

/** Filter out non-music events (comedy, sports, etc.) that leak through TM's classificationName filter */
function isMusicEvent(ev: { segment?: string }): boolean {
  const seg = ev.segment ?? "";
  return !seg || seg === "music";
}

// GET /api/live/nearby — geo-targeted events based on user IP
app.get("/api/live/nearby", async (req, res) => {
  // Allow client to pass cached lat/lon to skip IP lookup
  let lat = parseFloat(req.query.lat as string);
  let lon = parseFloat(req.query.lon as string);
  let city = (req.query.city as string) || "";
  let region = (req.query.region as string) || "";

  if (isNaN(lat) || isNaN(lon)) {
    res.json({ events: [], location: null });
    return;
  }

  if (!ticketmasterKey) { res.json({ events: [], location: { lat, lon, city, region } }); return; }

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
    const tmRes = await loggedFetch("ticketmaster", `https://app.ticketmaster.com/discovery/v2/events.json?${params}`, { signal: AbortSignal.timeout(10000), context: "nearby events" });
    if (!tmRes.ok) { res.json({ events: [], location: { lat, lon, city, region } }); return; }
    const tmData = await tmRes.json() as any;
    const events = (tmData._embedded?.events ?? []).map(mapTmEvent).filter(isMusicEvent);
    res.setHeader("Cache-Control", "no-store");
    res.json({ events, location: { lat, lon, city, region } });
  } catch {
    res.json({ events: [], location: { lat, lon, city, region } });
  }
});

// California metro areas to fetch concerts for
const LIVE_METROS = [
  { name: "San Francisco", lat: 37.7749, lon: -122.4194 },
  { name: "Los Angeles",   lat: 34.0522, lon: -118.2437 },
  { name: "Sacramento",    lat: 38.5816, lon: -121.4944 },
  { name: "San Diego",     lat: 32.7157, lon: -117.1611 },
  { name: "Ventura",       lat: 34.2746, lon: -119.2290 },
];

// Background: fetch upcoming events from Ticketmaster for all metros and store in DB
async function fetchUpcomingEvents(): Promise<number> {
  if (!ticketmasterKey) return 0;
  let totalCount = 0;
  for (const metro of LIVE_METROS) {
    try {
      // Ticketmaster allows up to 200 per page; fetch multiple pages to get broad coverage
      let page = 0;
      let fetched = 0;
      while (page < 3) { // up to 3 pages (600 events) per metro
        const params = new URLSearchParams({
          apikey: ticketmasterKey,
          classificationName: "music",
          latlong: `${metro.lat},${metro.lon}`,
          radius: "100",
          unit: "miles",
          size: "200",
          page: String(page),
          sort: "date,asc",
        });
        const r = await loggedFetch("ticketmaster", `https://app.ticketmaster.com/discovery/v2/events.json?${params}`, {
          signal: AbortSignal.timeout(30000),
          context: `scheduled fetch ${metro.name}`,
        });
        if (!r.ok) break;
        const data = await r.json() as any;
        const events = (data._embedded?.events ?? []).map(mapTmEvent).filter(isMusicEvent);
        if (!events.length) break;
        const count = await upsertLiveEvents(events);
        fetched += count;
        const totalPages = data.page?.totalPages ?? 1;
        page++;
        if (page >= totalPages) break;
        await sleep(250); // rate-limit courtesy
      }
      totalCount += fetched;
      console.log(`[live-events] ${metro.name}: ${fetched} events`);
      await sleep(500); // pause between metros
    } catch (e) {
      console.error(`[live-events] ${metro.name} fetch error:`, (e as Error)?.message);
    }
  }
  await pruneLiveEvents();
  console.log(`[live-events] Total: ${totalCount} events upserted, past events pruned`);
  return totalCount;
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
  const username = await getDiscogsUsername(userId);
  if (!username) { res.status(400).json({ error: "No Discogs username — connect your account first" }); return null; }
  const client = await getDiscogsClientForUser(userId);
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

// POST /api/user/folders/move-all — move every item from one folder to another
app.post("/api/user/folders/move-all", express.json(), async (req, res) => {
  const ctx = await requireUsernameAndToken(req, res);
  if (!ctx) return;
  const { fromFolderId, toFolderId } = req.body ?? {};
  if (fromFolderId == null || toFolderId == null) { res.status(400).json({ error: "fromFolderId and toFolderId required" }); return; }
  const from = Number(fromFolderId);
  const to   = Number(toFolderId);
  if (from === 0) { res.status(400).json({ error: "Cannot move items out of the virtual 'All' folder" }); return; }
  if (from === to) { res.json({ ok: true, moved: 0 }); return; }
  try {
    const contents = await getFolderContents(ctx.userId, from);
    if (contents.length > 150) {
      res.status(400).json({ error: `This folder contains ${contents.length} items. For folders this large, please move items in smaller batches.` });
      return;
    }
    let moved = 0;
    for (let i = 0; i < contents.length; i++) {
      const item = contents[i];
      if (!item.instanceId) continue;
      if (i > 0) await _sleep(DISCOGS_CALL_DELAY_MS);
      const moveUrl = `https://api.discogs.com/users/${encodeURIComponent(ctx.username)}/collection/folders/${from}/releases/${item.releaseId}/instances/${item.instanceId}`;
      const mr = await loggedFetch("discogs", moveUrl, {
        method: "POST",
        headers: { ...ctx.client.buildHeaders("POST", moveUrl), "Content-Type": "application/json" },
        body: JSON.stringify({ folder_id: to }),
        context: "folder-move-all",
      });
      if (mr.ok || mr.status === 204) moved++;
    }
    await moveAllCollectionItemsBetweenFolders(ctx.userId, from, to);
    res.json({ ok: true, moved });
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
app.get("/api/price-history/:releaseId", async (req, res) => {
  const releaseId = parseInt(req.params.releaseId);
  if (!releaseId) { res.status(400).json({ error: "Invalid releaseId" }); return; }
  const days = Math.min(365, parseInt(req.query.days as string) || 90);
  try {
    const history = await getPriceHistory(releaseId, "USD", days);
    res.json({ history });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/price/:releaseId — current price from cache
app.get("/api/price/:releaseId", async (req, res) => {
  const releaseId = parseInt(req.params.releaseId);
  if (!releaseId) { res.status(400).json({ error: "Invalid releaseId" }); return; }
  try {
    const price = await getPriceCache(releaseId);
    res.json(price ?? { lowest: null, median: null, highest: null, numForSale: 0, fetchedAt: null });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

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

// ── Background price updater ─────────────────────────────────────────────
async function runPriceUpdate() {
  if (_apiKillSwitch) return;
  _lastPriceUpdate = new Date();
  console.log("[price-update] Starting background price update…");
  try {
    const allIds = await getStaleReleaseIds(200);
    if (!allIds.length) { console.log("[price-update] No releases to update"); return; }
    console.log(`[price-update] Updating ${allIds.length} releases`);

    let updated = 0;
    for (const releaseId of allIds) {
      if (_apiKillSwitch) break;
      try {
        const url = `https://api.discogs.com/marketplace/stats/${releaseId}?curr_abbr=USD`;
        const headers: Record<string, string> = { "User-Agent": "SeaDisco/1.0" };
        if (sharedToken) headers["Authorization"] = `Discogs token=${sharedToken}`;
        const r = await loggedFetch("discogs", url, { headers, context: "price-update" });
        if (r.ok) {
          const data = await r.json() as any;
          const lowest = data.lowest_price?.value ?? null;
          const median = data.median_price?.value ?? null;
          const highest = data.highest_price?.value ?? null;
          const numForSale = data.num_for_sale ?? 0;
          await upsertPriceCache(releaseId, lowest, median, highest, numForSale);
          await appendPriceHistory(releaseId, lowest, median, highest, numForSale);
          updated++;
        } else if (r.status === 429) {
          console.log("[price-update] Rate limited, pausing 60s");
          await sleep(60000);
        }
        // Pace at ~1 req/sec to stay well within rate limits
        await sleep(1100);
      } catch (e) {
        console.error(`[price-update] Error for ${releaseId}:`, e);
      }
    }
    console.log(`[price-update] Done, updated ${updated}/${allIds.length} releases`);
  } catch (e) {
    console.error("[price-update] Error:", e);
  }
}

function startPriceUpdateSchedule() {
  // Run every 6 hours, starting 30 min after boot
  const SIX_HOURS = 6 * 60 * 60 * 1000;
  setTimeout(() => {
    runPriceUpdate();
    setInterval(runPriceUpdate, SIX_HOURS);
  }, 30 * 60 * 1000);
  // Also prune old price history daily
  setInterval(() => { prunePriceHistory().catch(() => {}); }, 24 * 60 * 60 * 1000);
}

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
    const [collectionIds, wantlistIds, favoriteIds, inventoryIds, inventoryListingIds, listMembership, collectionInstanceCounts, defaultAddFolderId, profile] = await Promise.all([
      getCollectionIds(userId),
      getWantlistIds(userId),
      getFavoriteIds(userId),
      getInventoryIds(userId),
      getInventoryListingIdsByRelease(userId),
      getListMembership(userId),
      getCollectionMultiInstanceCounts(userId),
      getDefaultAddFolderId(userId),
      getDiscogsProfile(userId),
    ]);
    res.json({ collectionIds, wantlistIds, favoriteIds, inventoryIds, inventoryListingIds, listMembership, collectionInstanceCounts, defaultAddFolderId, currency: profile.currAbbr || "USD" });
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

// GET /api/public/featured-records — owner's random records for logged-out landing
app.get("/api/public/featured-records", async (req, res) => {
  const ownerId = process.env.ADMIN_CLERK_ID ?? "";
  if (!ownerId) { res.json({ items: [] }); return; }
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 192, 300);
    const rows = await getRandomRecords(ownerId, limit);
    res.json({ items: rows });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/public/featured-favorites — random favorites from all users for logged-out landing page
app.get("/api/public/featured-favorites", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 48, 200);
    const items = await getRandomPublicFavorites(limit);
    res.json({ items });
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
app.use("/api/admin", (req, res, next) => {
  const ip = clientIp(req);
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
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const items = await getFeedback();
  res.json({ items });
});

// DELETE /api/admin/feedback/:id — delete a feedback item, admin only
app.delete("/api/admin/feedback/:id", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  await deleteFeedback(parseInt(req.params.id));
  res.json({ ok: true });
});

// GET /api/admin/sync-status — per-user sync status + fresh releases stats, admin only
app.get("/api/admin/sync-status", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const [users, freshStats, favCounts] = await Promise.all([getAllUsersSyncStatus(), getFreshStats(), getAllFavoriteCounts()]);

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
  res.json({ users: enriched, freshStats });
});

// POST /api/admin/sync-all — trigger FULL background sync for all users, admin only
app.post("/api/admin/sync-all", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
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
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
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
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  _syncAbort = true;
  const count = await resetAllSyncingStatuses();
  console.log(`Admin: sync abort requested, ${count} syncing statuses reset`);
  res.json({ ok: true, message: `All syncs stopped — ${count} reset.` });
});

// POST /api/admin/api-kill — toggle global API kill switch
app.post("/api/admin/api-kill", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const { enabled } = req.body ?? {};
  _apiKillSwitch = enabled !== undefined ? !!enabled : !_apiKillSwitch;
  console.log(`Admin: API kill switch ${_apiKillSwitch ? "ENABLED — all outgoing requests blocked" : "DISABLED — requests flowing"}`);
  res.json({ ok: true, killSwitch: _apiKillSwitch });
});

// GET /api/admin/api-kill — check kill switch status
app.get("/api/admin/api-kill", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  res.json({ killSwitch: _apiKillSwitch });
});

// POST /api/admin/revoke-sessions — log out all Clerk users except admin
app.post("/api/admin/revoke-sessions", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
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

// GET /api/admin/collection-stats — per-user and global collection/wantlist stats
app.get("/api/admin/collection-stats", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
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
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
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
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
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

// POST /api/ai-search — Claude music recommendations
app.post("/api/ai-search", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  if (!userId) { res.status(401).json({ error: "no_token" }); return; }
  if (!anthropicKey) { res.status(503).json({ error: "AI not configured" }); return; }

  const q = (req.body.q as string ?? "").trim();
  if (!q) { res.status(400).json({ error: "Query required" }); return; }

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
    const data = await r.json() as any;
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
    const blurb: string = Array.isArray(parsed) ? "" : (parsed.blurb ?? "");
    res.json({ recommendations, blurb });
  } catch (err) {
    console.error("AI search error:", err);
    res.status(500).json({ error: "AI search failed: " + (err as Error).message });
  }
});

// POST /api/result-quality — Claude gives a short phrase on result relevance
app.post("/api/result-quality", express.json(), async (req, res) => {
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
  const rawQ   = (req.query.q as string) ?? "";
  const artist = stripArtistSuffix(req.query.artist as string | undefined);
  const rawLabel = (req.query.label as string) ?? "";
  const rawRelease = (req.query.release_title as string) ?? "";

  // Each field is sent as its own dedicated Discogs param — no promotion to q
  const q = rawQ;
  const searchArtist: string | undefined = artist || undefined;
  const searchLabel: string | undefined = rawLabel || undefined;
  const searchRelease: string | undefined = rawRelease || undefined;

  const ip = clientIp(req);
  const whitelisted = IP_WHITELIST.has(ip);

  const userId = await getClerkUserId(req);
  const hasAuthHeader = !!req.headers.authorization?.startsWith("Bearer ");

  // If the user sent a token but it failed verification, return 401 so the
  // frontend can refresh the session and retry (instead of falling into the
  // rate limiter and showing "free searches used up" to a signed-in user)
  if (!userId && hasAuthHeader) {
    res.status(401).json({ error: "session_expired", message: "Session expired. Please refresh the page." });
    return;
  }

  const userToken = userId ? await getUserToken(userId) : null;
  const usingSharedToken = !userToken;

  // Touch activity for authenticated users
  if (userId && userToken) touchUserActivity(userId).catch(() => {});

  // Rate-limit unauthenticated users — allow 5 free searches/day via shared token
  // Only count page 1 — pagination ("load more") shouldn't burn a search
  const page = parseInt(req.query.page as string) || 1;
  if (usingSharedToken && !whitelisted) {
    if (!sharedToken) {
      res.status(401).json({ error: "no_token", message: "Sign in and add your Discogs API token to search." });
      return;
    }
    if (page <= 1) {
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
  }

  // allowFallback=true for unauthenticated/whitelisted users — they passed the rate limit check above
  const dc = await getDiscogsForRequest(req, usingSharedToken || whitelisted);
  if (!dc) {
    res.status(401).json({ error: "no_token", message: "Sign in and add your Discogs API token to search." });
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
  const id = parseInt(req.params.id, 10);
  const dc = await getDiscogsForRequest(req, true);
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
  const id = parseInt(req.params.id, 10);
  const dc = await getDiscogsForRequest(req, true);
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

// GET /artist-bio?name=Miles+Davis[&id=123456] — Discogs bio
// If `id` is supplied the artist is fetched directly (no ambiguous name search).
app.get("/artist-bio", async (req, res) => {
  res.setHeader("Cache-Control", "public, max-age=3600"); // 1 hour
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
async function resolveDiscogsIds(profile: string, dc: DiscogsClient = discogs!): Promise<string> {
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
  res.setHeader("Cache-Control", "public, max-age=3600"); // 1 hour
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
  res.setHeader("Cache-Control", "public, max-age=86400"); // 24 hours
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
  res.setHeader("Cache-Control", "public, max-age=300"); // 5 min
  const { id } = req.params;
  const type = (req.query.type as string) ?? "release";
  const dc = await getDiscogsForRequest(req, true);
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
  res.setHeader("Cache-Control", "public, max-age=300");
  const { id } = req.params;
  const dc = await getDiscogsForRequest(req, true);
  if (!dc) { res.status(503).json({ error: "No Discogs client" }); return; }
  try {
    const data = await dc.getPriceSuggestions(id);
    res.json(data);
  } catch (err: any) {
    console.error("price-suggestions error:", err?.message ?? err);
    res.status(500).json({ error: "Failed to fetch price suggestions" });
  }
});

// GET /master-versions/:id — all pressings/versions of a master release
app.get("/master-versions/:id", async (req, res) => {
  const { id } = req.params;
  const dc = await getDiscogsForRequest(req, true);
  if (!dc) { res.json({ versions: [] }); return; }
  try {
    const data = await dc.getMasterVersions(id, { perPage: 100, sort: "released", sortOrder: "asc" }) as any;
    const versions = (data.versions ?? []).map((v: any) => ({
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
  const { id } = req.params;
  const dc = await getDiscogsForRequest(req, true);
  if (!dc) { res.json({ releases: [], name: "" }); return; }
  try {
    res.setHeader("Cache-Control", "public, max-age=3600");
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

// GET /api/fresh-releases — 150 random releases from last 3 months
app.get("/api/fresh-releases", async (_req, res) => {
  try {
    res.setHeader("Cache-Control", "public, max-age=300");
    const releases = await getFreshReleases(150);
    res.json({ releases });
  } catch (err) {
    console.error("fresh-releases error:", err);
    res.json({ releases: [] });
  }
});

// GET /api/fresh-releases/search?q=... — search full 3-month DB by artist/release/tag
app.get("/api/fresh-releases/search", async (req, res) => {
  try {
    res.setHeader("Cache-Control", "public, max-age=120"); // 2 min
    const q = String(req.query.q ?? "").trim();
    if (!q) { res.json({ releases: [] }); return; }
    const releases = await searchFreshReleases(q, 200);
    res.json({ releases });
  } catch (err) {
    console.error("fresh-releases search error:", err);
    res.json({ releases: [] });
  }
});

// ── Live tab: flexible concert search (artist, city/zip, genre) ───────────
app.get("/api/concerts/search", async (req, res) => {
  res.setHeader("Cache-Control", "no-cache"); // disable until stable
  const artist = (req.query.artist as string ?? "").trim();
  const city   = (req.query.city   as string ?? "").trim();
  const genre  = (req.query.genre  as string ?? "").trim();
  const page   = parseInt(req.query.page as string ?? "0", 10) || 0;
  if (!artist && !city && !genre) { res.json({ events: [], artistImage: null }); return; }

  interface LiveEvent {
    artist: string;
    name: string;
    date: string;
    time: string;
    venue: string;
    venueId: string;
    city: string;
    region: string;
    country: string;
    url: string;
    source: string;
  }

  const events: LiveEvent[] = [];
  let artistImage: string | null = null;
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
      if (artist) params.set("keyword", artist);
      if (city) {
        if (/^\d{5}$/.test(city)) {
          params.set("postalCode", city);
          params.set("radius", "75");
          params.set("unit", "miles");
          params.set("countryCode", "US");
        } else {
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
              const matched = attractions.find((a: any) =>
                (a.name ?? "").toLowerCase().includes(artistLower) ||
                artistLower.includes((a.name ?? "").toLowerCase())
              );
              if (!matched && !(ev.name ?? "").toLowerCase().includes(artistLower)) continue;
              if (matched) eventArtist = matched.name;
            }
            const mapped = mapTmEvent(ev);
            if (!isMusicEvent(mapped)) continue;
            events.push({
              ...mapped,
              artist: eventArtist || ev.name?.split(/\s[-–—:]\s/)?.[0] || "",
            });
          }
        } catch { /* parse error */ }
      }
    } catch (err) { console.error("Live TM error:", err); }
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
          const artData = await artRes.json() as any;
          const bitName = (artData.name ?? "").toLowerCase().trim();
          const searchName = artist.toLowerCase().trim();
          if (bitName === searchName || bitName.includes(searchName) || searchName.includes(bitName)) {
            const img = artData.image_url || artData.thumb_url || null;
            if (img && !img.includes("default") && !img.includes("placeholder")) {
              artistImage = img;
            }
          }
        } catch { /* ignore */ }
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
                if (!evCity.toLowerCase().includes(cityLower) && !evRegion.toLowerCase().includes(cityLower)) continue;
              }
              events.push({
                artist:  artist,
                name:    ev.title ?? `${artist} live`,
                date:    (ev.datetime ?? "").slice(0, 10),
                time:    (ev.datetime ?? "").slice(11, 16),
                venue:   ev.venue?.name ?? "",
                venueId: "",
                city:    evCity,
                region:  evRegion,
                country: ev.venue?.country ?? "",
                url:     ev.url ?? "",
                source:  "bandsintown",
              });
            }
          }
        } catch { /* parse error */ }
      }
    } catch (err) { console.error("Live BIT error:", err); }
  }

  const seen = new Set<string>();
  const deduped: LiveEvent[] = [];
  for (const ev of events) {
    const key = `${ev.date}|${ev.venue}`.toLowerCase();
    if (seen.has(key)) continue;
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
  if (!venueId || !ticketmasterKey) { res.json({ events: [], venueName: "" }); return; }
  try {
    const tmUrl = `https://app.ticketmaster.com/discovery/v2/events.json?venueId=${encodeURIComponent(venueId)}&classificationName=music&size=200&sort=date,asc&apikey=${ticketmasterKey}`;
    const tmRes = await loggedFetch("ticketmaster", tmUrl, { context: "venue events" });
    if (!tmRes.ok) { res.json({ events: [], venueName: "" }); return; }
    const tmData = await tmRes.json() as any;
    const events: any[] = [];
    let venueName = "";
    for (const ev of (tmData._embedded?.events ?? [])) {
      const venue = ev._embedded?.venues?.[0];
      if (!venueName && venue?.name) venueName = venue.name;
      const mapped = mapTmEvent(ev);
      if (!isMusicEvent(mapped)) continue;
      events.push({
        ...mapped,
        venueId: mapped.venueId || venueId,
      });
    }
    const location = events[0] ? [events[0].city, events[0].region, events[0].country].filter(Boolean).join(", ") : "";
    res.json({ events, venueName, location });
  } catch (err) {
    console.error("Venue events error:", err);
    res.json({ events: [], venueName: "" });
  }
});

// ── Concert info (Ticketmaster + Bandsintown) ─────────────────────────────
app.get("/api/concerts/:artist", async (req, res) => {
  res.setHeader("Cache-Control", "public, max-age=900"); // 15 min
  const artist = decodeURIComponent(req.params.artist).trim();
  if (!artist) { res.json({ events: [] }); return; }

  interface ConcertEvent {
    name: string;
    date: string;        // ISO date
    time: string;        // display time or ""
    venue: string;
    city: string;
    region: string;
    country: string;
    url: string;
    source: string;      // "ticketmaster" | "bandsintown"
  }

  const events: ConcertEvent[] = [];

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
            if (isMusicEvent(mapped)) events.push(mapped);
          }
        } catch { /* parse error */ }
      } else {
        console.error(`Ticketmaster ${tmRes.status}:`, tmBody.slice(0, 300));
      }
    } catch (err) { console.error("Ticketmaster fetch error:", err); }
  } else {
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
              name:    ev.title ?? `${artist} live`,
              date:    (ev.datetime ?? "").slice(0, 10),
              time:    (ev.datetime ?? "").slice(11, 16),
              venue:   ev.venue?.name ?? "",
              city:    ev.venue?.city ?? "",
              region:  ev.venue?.region ?? "",
              country: ev.venue?.country ?? "",
              url:     ev.url ?? "",
              source:  "bandsintown",
            });
          }
        }
      } catch { /* parse error */ }
    } else {
      console.error(`Bandsintown ${bitRes.status}:`, bitBody.slice(0, 300));
    }
  } catch (err) { console.error("Bandsintown fetch error:", err); }

  // Dedupe by date+venue (prefer ticketmaster if duplicate)
  const seen = new Set<string>();
  const deduped: ConcertEvent[] = [];
  for (const ev of events) {
    const key = `${ev.date}|${ev.venue}`.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(ev);
  }
  // Sort by date ascending
  deduped.sort((a, b) => a.date.localeCompare(b.date));

  console.log(`Concerts for "${artist}": ${events.length} raw, ${deduped.length} deduped`);
  res.json({ artist, events: deduped });
});

// ── eBay Gear integration ─────────────────────────────────────────────────
const ebayClientId     = process.env.EBAY_CLIENT_ID ?? "";
const ebayClientSecret = process.env.EBAY_CLIENT_SECRET ?? "";
const ebayAffiliateCampaignId = process.env.EBAY_AFFILIATE_CAMPAIGN_ID ?? "";
let ebayAccessToken    = "";
let ebayTokenExpiry    = 0;
// Daily eBay Browse quota: 5,000 calls.
// Reserved by scheduled sweeps: vinyl ~900/day (9×2×50) + gear ~900/day (6×3×50) ≈ 1,800.
// Remaining ~3,200 is the user-triggered popup budget.
const EBAY_USER_LIMIT = 3000;
const MAX_USERS         = 25;

async function getEbayToken(): Promise<string> {
  if (ebayAccessToken && Date.now() < ebayTokenExpiry - 60000) return ebayAccessToken;
  if (!ebayClientId || !ebayClientSecret) throw new Error("eBay credentials not configured");
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
  if (!r.ok) throw new Error(`eBay OAuth failed: ${r.status}`);
  const data = await r.json() as { access_token: string; expires_in: number };
  ebayAccessToken = data.access_token;
  ebayTokenExpiry = Date.now() + data.expires_in * 1000;
  console.log("eBay OAuth token refreshed");
  return ebayAccessToken;
}

const GEAR_SEARCH_QUERIES = [
  "vintage turntable",
  "vintage record player",
  "vintage speakers",
  "vintage receiver",
  "vintage amp",
  "vintage preamp",
];

const VINYL_KEYWORD_QUERIES = [
  "rare",
  "first pressing",
  "original",
  "first press",
  "bootleg",
  "promo",
  "promotional",
  "white label",
];

async function fetchEbayGearListings(): Promise<number> {
  if (!ebayClientId || !ebayClientSecret) {
    console.log("eBay gear fetch skipped — no credentials");
    return 0;
  }
  console.log("Starting eBay gear fetch…");
  let totalUpserted = 0;

  // Fixed-price (Buy It Now) only, ≥$50. Sort omitted → eBay default (bestMatch).
  const baseFilter = `price:[50..],priceCurrency:USD,buyingOptions:{FIXED_PRICE}`;
  // Paginate 3 pages deep per query: 6 queries × 3 pages = 18 calls per sweep (~900/day)
  const PAGES = 3;
  const LIMIT = 200;

  try {
    const token = await getEbayToken();
    const headers = { "Authorization": `Bearer ${token}`, "X-EBAY-C-MARKETPLACE-ID": "EBAY_US" };

    for (const query of GEAR_SEARCH_QUERIES) {
      for (let page = 0; page < PAGES; page++) {
        const offset = page * LIMIT;
        try {
          await new Promise(r => setTimeout(r, 1000)); // pace
          // category_ids=175673 = Vintage Electronics (Vintage Audio & Video)
          const url = `https://api.ebay.com/buy/browse/v1/item_summary/search?q=${encodeURIComponent(query)}&category_ids=175673&limit=${LIMIT}&offset=${offset}&filter=${baseFilter}`;
          const r = await loggedFetch("ebay", url, { headers, context: `gear ${query} p${page}` });
          if (!r.ok) { console.error(`eBay gear "${query}" p${page} failed: ${r.status}`); continue; }
          const data = await r.json() as any;
          const summaries: any[] = data.itemSummaries ?? [];
          console.log(`eBay gear "${query}" p${page} (offset ${offset}): ${summaries.length} results`);

          if (summaries.length) {
            const items = summaries.map((s: any) => ({
              itemId:          s.itemId,
              title:           s.title ?? "",
              price:           parseFloat(s.currentBidPrice?.value ?? s.price?.value ?? "0"),
              currency:        s.currentBidPrice?.currency ?? s.price?.currency ?? "USD",
              condition:       s.condition ?? s.conditionId ?? "",
              imageUrl:        s.image?.imageUrl ?? "",
              itemUrl:         s.itemWebUrl ?? "",
              locationCity:    s.itemLocation?.city ?? "",
              locationState:   s.itemLocation?.stateOrProvince ?? "",
              locationCountry: s.itemLocation?.country ?? "",
              sellerUsername:  s.seller?.username ?? "",
              sellerFeedback:  s.seller?.feedbackScore ?? 0,
              buyingOptions:   s.buyingOptions ?? [],
              bidCount:        s.bidCount ?? 0,
              categories:      (s.categories ?? []).map((c: any) => c.categoryId),
              categoryNames:   (s.categories ?? []).map((c: any) => c.categoryName),
              itemEndDate:     s.itemEndDate ?? null,
              thumbnailUrl:    (s.thumbnailImages ?? [])[0]?.imageUrl ?? "",
              rawSummary:      s,
            }));
            totalUpserted += await upsertGearListings(items);
          }

          // Short-circuit if we got fewer than a full page
          if (summaries.length < LIMIT) break;
        } catch (err) {
          console.error(`eBay gear "${query}" p${page} error:`, err);
        }
      }
    }
    // Mark old listings as expired
    const expired = await markExpiredGearListings();
    if (expired) console.log(`Marked ${expired} gear listings as expired`);

    await logGearFetch("browse_search", totalUpserted);
    console.log(`eBay gear fetch complete: ${totalUpserted} items upserted (up to 20 calls)`);
  } catch (err) {
    console.error("eBay gear fetch failed:", err);
    await logGearFetch("browse_search", totalUpserted, String(err));
  }
  return totalUpserted;
}

// Schedule: gear fetch every 29 minutes, staggered from vinyl.
// Budget: 4 queries × 5 pages = 20 calls/sweep × ~50 sweeps/day ≈ 1,000 calls/day
function startGearSchedule() {
  if (!ebayClientId || !ebayClientSecret) {
    console.log("eBay gear schedule not started — no credentials");
    return;
  }
  // Gear fetch at :34 past (offset from vinyl's :20), every 29 min
  const msSearch = msUntilPacific(6, 34, 1);
  console.log(`[gear] Next fetch in ${Math.round(msSearch / 60000)}min, then every 29min`);
  setTimeout(() => {
    fetchEbayGearListings();
    setInterval(() => fetchEbayGearListings(), 29 * 60 * 1000);
  }, msSearch);
}

// GET /api/gear — public gear listings
app.get("/api/gear", async (_req, res) => {
  try {
    const minPrice = parseFloat(_req.query.min_price as string) || 0;
    const sort     = (_req.query.sort as string) || "price_desc";
    const q        = (_req.query.q as string) || "";
    const limit    = Math.min(parseInt(_req.query.limit as string) || 200, 500);
    const offset   = parseInt(_req.query.offset as string) || 0;
    res.setHeader("Cache-Control", "public, max-age=60");
    const { items, total } = await getGearListings(minPrice, limit, offset, sort, q);
    res.json({ items, total });
    // Fire-and-forget: refresh prices/bids in background (throttled to 1 call per 5 min)
    refreshGearPrices(sort).catch(() => {});
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/gear/stats — admin stats
app.get("/api/gear/stats", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  try {
    const stats = await getGearStats();
    res.json(stats);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/admin/gear/fetch — manual trigger for admin
app.post("/api/admin/gear/fetch", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  res.json({ ok: true, started: true });
  fetchEbayGearListings();
});

// ── Vinyl LP listings (eBay 12" records) ────────────────────────────────

async function fetchEbayVinylListings(): Promise<number> {
  if (!ebayClientId || !ebayClientSecret) {
    console.log("eBay vinyl fetch skipped — no credentials");
    return 0;
  }
  console.log("Starting eBay vinyl fetch…");
  let totalUpserted = 0;

  const NOT_12_RE = /\b(7["″''"]|7 inch|45 ?rpm|\b45\b|10["″''"]|10 inch|pic sleeve)\b/i;

  const mapSummaries = (summaries: any[]) => {
    const filtered = summaries.filter((s: any) => !NOT_12_RE.test(s.title ?? ""));
    if (filtered.length < summaries.length) {
      console.log(`  filtered ${summaries.length - filtered.length} non-12" items`);
    }
    return filtered.map((s: any) => ({
      itemId:          s.itemId,
      title:           s.title ?? "",
      price:           parseFloat(s.currentBidPrice?.value ?? s.price?.value ?? "0"),
      currency:        s.currentBidPrice?.currency ?? s.price?.currency ?? "USD",
      condition:       s.condition ?? s.conditionId ?? "",
      imageUrl:        s.image?.imageUrl ?? "",
      itemUrl:         s.itemWebUrl ?? "",
      locationCity:    s.itemLocation?.city ?? "",
      locationState:   s.itemLocation?.stateOrProvince ?? "",
      locationCountry: s.itemLocation?.country ?? "",
      sellerUsername:  s.seller?.username ?? "",
      sellerFeedback:  s.seller?.feedbackScore ?? 0,
      buyingOptions:   s.buyingOptions ?? [],
      bidCount:        s.bidCount ?? 0,
      categories:      (s.categories ?? []).map((c: any) => c.categoryId),
      categoryNames:   (s.categories ?? []).map((c: any) => c.categoryName),
      itemEndDate:     s.itemEndDate ?? null,
      thumbnailUrl:    (s.thumbnailImages ?? [])[0]?.imageUrl ?? "",
      rawSummary:      s,
    }));
  };

  try {
    const token = await getEbayToken();
    const headers = { "Authorization": `Bearer ${token}`, "X-EBAY-C-MARKETPLACE-ID": "EBAY_US" };
    // Fixed-price (Buy It Now) only, ≥$10. Sort omitted → eBay default (bestMatch).
    const baseFilter = `price:[10..],priceCurrency:USD,buyingOptions:{FIXED_PRICE}`;
    const aspectFilter = `aspect_filter=categoryId:176985,Record%20Size:12%22`;

    // Build query list: category-wide (no q=) + keyword queries
    const queries: Array<{ label: string; qParam: string }> = [
      { label: "category", qParam: "" },
      ...VINYL_KEYWORD_QUERIES.map(k => ({ label: k, qParam: `q=${encodeURIComponent(k)}&` })),
    ];

    // Paginate 2 pages deep per query (offset 0, 200)
    // Total: 9 queries × 2 pages = 18 calls per sweep (~900/day)
    const PAGES = 2;
    const LIMIT = 200;

    for (const { label, qParam } of queries) {
      for (let page = 0; page < PAGES; page++) {
        const offset = page * LIMIT;
        try {
          if (page > 0 || label !== "category") {
            await new Promise(r => setTimeout(r, 1000)); // pace
          }
          const url = `https://api.ebay.com/buy/browse/v1/item_summary/search?${qParam}category_ids=176985&limit=${LIMIT}&offset=${offset}&filter=${baseFilter}&${aspectFilter}`;
          const r = await loggedFetch("ebay", url, { headers, context: `vinyl ${label} p${page}` });
          if (!r.ok) { console.error(`eBay vinyl "${label}" p${page} failed: ${r.status}`); continue; }
          const data = await r.json() as any;
          const summaries: any[] = data.itemSummaries ?? [];
          console.log(`eBay vinyl ${label} p${page} (offset ${offset}): ${summaries.length} results`);
          if (summaries.length) totalUpserted += await upsertVinylListings(mapSummaries(summaries));
          // Short-circuit if we got fewer than a full page (no more results)
          if (summaries.length < LIMIT) break;
        } catch (err) { console.error(`eBay vinyl "${label}" p${page} error:`, err); }
      }
    }

    // Mark ended listings as expired
    const expired = await markExpiredVinylListings();
    if (expired) console.log(`Marked ${expired} vinyl listings as expired`);

    await logVinylFetch("browse_search", totalUpserted);
    console.log(`eBay vinyl fetch complete: ${totalUpserted} items upserted (up to 20 calls)`);
  } catch (err) {
    console.error("eBay vinyl fetch failed:", err);
    await logVinylFetch("browse_search", totalUpserted, String(err));
  }
  return totalUpserted;
}

// Schedule: vinyl fetch every 29 minutes.
// Budget: 4 queries × 5 pages = 20 calls/sweep × ~50 sweeps/day ≈ 1,000 calls/day
function startVinylSchedule() {
  if (!ebayClientId || !ebayClientSecret) {
    console.log("eBay vinyl schedule not started — no credentials");
    return;
  }
  const msSearch = msUntilPacific(6, 20, 1);
  console.log(`[vinyl] Next fetch in ${Math.round(msSearch / 60000)}min, then every 29min`);
  setTimeout(() => {
    fetchEbayVinylListings();
    setInterval(() => fetchEbayVinylListings(), 29 * 60 * 1000);
  }, msSearch);
}

// ── Page-load refresh disabled ──────────────────────────────────────────
// With fixed-price-only listings, prices barely change. The scheduled
// background fetch (every 29 min) keeps the DB fresh enough, and disabling
// page-load refresh preserves the full popup budget.
async function refreshVinylPrices(_sort: string): Promise<void> { /* no-op */ }
async function refreshGearPrices(_sort: string): Promise<void> { /* no-op */ }

// GET /api/vinyl — public vinyl listings
app.get("/api/vinyl", async (_req, res) => {
  try {
    const minPrice = parseFloat(_req.query.min_price as string) || 0;
    const sort     = (_req.query.sort as string) || "price_desc";
    const q        = (_req.query.q as string) || "";
    const limit    = Math.min(parseInt(_req.query.limit as string) || 200, 500);
    const offset   = parseInt(_req.query.offset as string) || 0;
    res.setHeader("Cache-Control", "public, max-age=60");
    const { items, total } = await getVinylListings(minPrice, limit, offset, sort, q);
    res.json({ items, total });
    // Fire-and-forget: refresh prices/bids in background (throttled to 1 call per 5 min)
    refreshVinylPrices(sort).catch(() => {});
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/vinyl/stats — admin stats
app.get("/api/vinyl/stats", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  try {
    const stats = await getVinylStats();
    res.json(stats);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/admin/vinyl/fetch — manual trigger for admin
app.post("/api/admin/vinyl/fetch", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  res.json({ ok: true, started: true });
  fetchEbayVinylListings();
});

// ── eBay live search ─────────────────────────────────────────────────────

function nextPacificMidnightIso(): string {
  // Get current time in Pacific, find next midnight, convert to UTC ISO
  const nowPacific = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
  const tomorrow = new Date(nowPacific);
  tomorrow.setDate(tomorrow.getDate() + 1);
  tomorrow.setHours(0, 0, 0, 0);
  // Offset back to UTC: Pacific is UTC-7 (PDT) or UTC-8 (PST)
  const jan = new Date(tomorrow.getFullYear(), 0, 1);
  const jul = new Date(tomorrow.getFullYear(), 6, 1);
  const stdOff = Math.max(jan.getTimezoneOffset(), jul.getTimezoneOffset());
  const isDST = nowPacific.getTimezoneOffset() < stdOff;
  // Pacific offset: -7 (PDT) or -8 (PST)
  const pacificOffsetHours = isDST ? 7 : 8;
  const utcMidnight = new Date(tomorrow.getTime() + pacificOffsetHours * 60 * 60 * 1000);
  return utcMidnight.toISOString();
}

// ── Item-detail in-memory cache ────────────────────────────────────────
// Keyed on itemId, 5-minute TTL. Cuts popup calls dramatically when
// multiple users click the same item in quick succession.
const ITEM_DETAIL_TTL_MS = 5 * 60 * 1000;
const ITEM_DETAIL_CACHE_MAX = 500;
const _itemDetailCache = new Map<string, { expiresAt: number; payload: any }>();

function _getCachedItemDetail(itemId: string): any | null {
  const hit = _itemDetailCache.get(itemId);
  if (!hit) return null;
  if (hit.expiresAt < Date.now()) {
    _itemDetailCache.delete(itemId);
    return null;
  }
  return hit.payload;
}

function _setCachedItemDetail(itemId: string, payload: any): void {
  // Simple LRU trim: if at capacity, drop the oldest entry
  if (_itemDetailCache.size >= ITEM_DETAIL_CACHE_MAX) {
    const firstKey = _itemDetailCache.keys().next().value;
    if (firstKey) _itemDetailCache.delete(firstKey);
  }
  _itemDetailCache.set(itemId, { expiresAt: Date.now() + ITEM_DETAIL_TTL_MS, payload });
}

// GET /api/ebay/item/:itemId — fetch full item details (description, images, specs)
// Public — no auth required so vinyl/gear popup detail works for all visitors
app.get("/api/ebay/item/:itemId", async (req, res) => {

  const itemId = req.params.itemId;
  if (!itemId || !/^v1\|/.test(itemId)) return res.status(400).json({ error: "Invalid item ID" });
  if (!ebayClientId || !ebayClientSecret) return res.status(503).json({ error: "eBay not available" });

  const resetsAt = nextPacificMidnightIso();

  // In-memory cache (5 min TTL) — short-circuit before touching eBay quota
  const cached = _getCachedItemDetail(itemId);
  if (cached) {
    const { count } = await getEbayRateCount();
    return res.json({
      ...cached,
      cached: true,
      rateLimit: { remaining: Math.max(0, EBAY_USER_LIMIT - count), limit: EBAY_USER_LIMIT, resetsAt },
    });
  }

  try {
    const { count } = await getEbayRateCount();
    if (count >= EBAY_USER_LIMIT) {
      return res.status(429).json({
        error: "Daily eBay request limit reached",
        rateLimit: { remaining: 0, limit: EBAY_USER_LIMIT, resetsAt },
      });
    }

    const newCount = await incrementEbayClickCount();
    const token = await getEbayToken();
    const headers: Record<string, string> = {
      "Authorization": `Bearer ${token}`,
      "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    };
    if (ebayAffiliateCampaignId) {
      headers["X-EBAY-C-ENDUSERCTX"] = `affiliateCampaignId=${ebayAffiliateCampaignId}`;
    }

    const r = await loggedFetch("ebay", `https://api.ebay.com/buy/browse/v1/item/${encodeURIComponent(itemId)}`, {
      headers,
      context: `item detail: ${itemId}`,
    });

    if (!r.ok) {
      return res.status(r.status === 404 ? 404 : 502).json({ error: `eBay returned ${r.status}` });
    }

    const d = await r.json() as any;
    const allImages = (d.additionalImages ?? []).map((img: any) => img.imageUrl).filter(Boolean);
    if (d.image?.imageUrl) allImages.unshift(d.image.imageUrl);
    const specifics: Record<string, string> = {};
    for (const nv of (d.localizedAspects ?? [])) {
      if (nv.name && nv.value) specifics[nv.name] = nv.value;
    }

    const detailPrice = parseFloat(d.currentBidPrice?.value ?? d.price?.value ?? "0");
    const detailCurrency = d.currentBidPrice?.currency ?? d.price?.currency ?? "USD";
    const detailCondition = d.condition ?? d.conditionDescription ?? "";
    const detailSeller = d.seller?.username ?? "";
    const detailSellerFeedback = d.seller?.feedbackScore ?? 0;
    const detailSellerFeedbackPercent = d.seller?.feedbackPercentage ?? "";
    const detailBidCount = d.bidCount ?? 0;
    const detailEndDate = d.itemEndDate ?? "";

    // Shipping — first shipping option if present
    const firstShip = Array.isArray(d.shippingOptions) ? d.shippingOptions[0] : null;
    const shipCostObj = firstShip?.shippingCost;
    const shipCostValue = parseFloat(shipCostObj?.value ?? "");
    const shippingFree = firstShip ? (shipCostValue === 0 || firstShip.shippingCostType === "FREE") : false;
    const shippingCost = Number.isFinite(shipCostValue) ? shipCostValue : 0;
    const shippingCurrency = shipCostObj?.currency ?? detailCurrency;

    // Return terms
    const returnsAccepted = d.returnTerms?.returnsAccepted ?? null;
    const returnPeriodVal = d.returnTerms?.returnPeriod?.value;
    const returnPeriodUnit = (d.returnTerms?.returnPeriod?.unit ?? "").toLowerCase();
    const returnPeriod = returnPeriodVal
      ? `${returnPeriodVal} ${returnPeriodUnit}${returnPeriodVal > 1 && !returnPeriodUnit.endsWith("s") ? "s" : ""}`
      : "";

    // Stock — availability estimates
    const firstAvail = Array.isArray(d.estimatedAvailabilities) ? d.estimatedAvailabilities[0] : null;
    const quantityAvailable = firstAvail?.estimatedAvailableQuantity ?? null;
    const quantitySold = firstAvail?.estimatedSoldQuantity ?? null;

    // Top-rated seller badge
    const topRatedBuyingExperience = d.topRatedBuyingExperience === true;

    // Category path (pipe-delimited from eBay; frontend turns into breadcrumb)
    const categoryPath = d.categoryPath ?? "";

    // Listed on — item creation date
    const itemCreationDate = d.itemCreationDate ?? "";

    // Subtitle if present
    const subtitle = d.subtitle ?? "";

    const payload = {
      description: d.description ?? "",
      allImages,
      specifics,
      bidCount: detailBidCount,
      itemUrl: d.itemWebUrl ?? "",
      condition: detailCondition,
      conditionDescription: d.conditionDescription ?? "",
      seller: detailSeller,
      sellerFeedback: detailSellerFeedback,
      sellerFeedbackPercent: detailSellerFeedbackPercent,
      price: detailPrice,
      currency: detailCurrency,
      location: [d.itemLocation?.city, d.itemLocation?.stateOrProvince, d.itemLocation?.country].filter(Boolean).join(", "),
      itemEndDate: detailEndDate,
      // Enriched popup fields
      shippingFree,
      shippingCost,
      shippingCurrency,
      returnsAccepted,
      returnPeriod,
      quantityAvailable,
      quantitySold,
      topRatedBuyingExperience,
      categoryPath,
      itemCreationDate,
      subtitle,
    };
    _setCachedItemDetail(itemId, payload);

    res.json({
      ...payload,
      cached: false,
      rateLimit: { remaining: Math.max(0, EBAY_USER_LIMIT - newCount), limit: EBAY_USER_LIMIT, resetsAt },
    });

    // Fire-and-forget: write fresh price/bids/detail back to DB
    updateListingFromDetail(itemId, {
      price: detailPrice, currency: detailCurrency, bidCount: detailBidCount,
      condition: detailCondition, description: d.description ?? "",
      allImages, specifics, seller: detailSeller,
      sellerFeedback: detailSellerFeedback, sellerFeedbackPercent: detailSellerFeedbackPercent,
      itemEndDate: detailEndDate,
    }).catch(() => {});
  } catch (e) {
    console.error("eBay item detail error:", e);
    res.status(500).json({ error: String(e) });
  }
});

// ── eBay Marketplace Account Deletion Notification (compliance) ──────────
const EBAY_VERIFICATION_TOKEN = "seadisco2026accountdeletiontoken";

app.get("/api/ebay/deletion", (req, res) => {
  // eBay sends a GET with challenge_code to verify the endpoint
  const challengeCode = req.query.challenge_code as string;
  if (!challengeCode) return res.status(400).json({ error: "missing challenge_code" });

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
const RSS_FEEDS: Array<{ name: string; url: string; category: string }> = [
  // Record collecting & vinyl culture
  { name: "Long Live Vinyl", url: "https://longlivevinyl.net/feed/", category: "news" },
  // Audiophile & gear
  { name: "What Hi-Fi?", url: "https://www.whathifi.com/feeds.xml", category: "gear" },
  { name: "Stereophile", url: "https://www.stereophile.com/rss.xml", category: "gear" },
  { name: "Head-Fi", url: "https://www.head-fi.org/forums/-/index.rss", category: "gear" },
  // Music reviews & news
  { name: "Pitchfork", url: "https://pitchfork.com/feed/feed-album-reviews/rss", category: "reviews" },
  { name: "Bandcamp Daily", url: "https://daily.bandcamp.com/feed", category: "reviews" },
  { name: "Aquarium Drunkard", url: "https://aquariumdrunkard.com/feed/", category: "news" },
  { name: "BrooklynVegan", url: "https://www.brooklynvegan.com/feed/", category: "news" },
  { name: "Stereogum", url: "https://www.stereogum.com/feed/", category: "news" },
];

const YOUTUBE_CHANNELS: Array<{ name: string; channelId: string; category?: string }> = [
  // Vinyl collecting & crate digging
  { name: "Vinyl Eyezz", channelId: "UCYkG_jJ2L0clu-_fqFfWGbQ" },
  { name: "The Vinyl Guide", channelId: "UCdYYSNnFPIdMuiAaq0pSpTA" },
  { name: "Vinyl Rewind", channelId: "UCpGhOGf56KAiQ9wBbNNUWzA" },
  { name: "My Analog Journal", channelId: "UC97UUhIqCmqm1JuXY6swfKA" },
  { name: "In Groove", channelId: "UCDXYKoEiWmFCYpEReNr4XFA" },
  // Gear & audiophile
  { name: "Techmoan", channelId: "UC5I2hjZYiW9gZPVkvzM8_Cw", category: "gear" },
  { name: "Analog Planet", channelId: "UCXnnKXr8oSTfZ7Y3R8CGAUQ", category: "gear" },
  // Live sessions
  { name: "KEXP", channelId: "UC3I2GFN_F8WudD_2jUZbojA" },
  { name: "Tiny Desk (NPR)", channelId: "UC4eYXhJI4-7wSWc8UNRwD4A" },
  // Reviews
  { name: "The Needle Drop", channelId: "UCt7fwAhXDy3oNFTAzF2o8Pw", category: "reviews" },
  { name: "Deep Cuts", channelId: "UCVBp4LmZjEBpzB-6VsFU4cQ", category: "reviews" },
];

// Keyword searches — vinyl & collecting focused
const YOUTUBE_SEARCHES: Array<{ query: string; category: string }> = [
  { query: "vinyl record collection 2026", category: "video" },
  { query: "vinyl unboxing haul 2026", category: "video" },
  { query: "crate digging vinyl records", category: "video" },
  { query: "new vinyl reissue 2026", category: "news" },
];

function decodeHtmlEntities(str: string): string {
  return str
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, n) => String.fromCharCode(parseInt(n, 16)))
    .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&nbsp;/g, " ")
    .replace(/&mdash;/g, "—").replace(/&ndash;/g, "–").replace(/&lsquo;/g, "'")
    .replace(/&rsquo;/g, "'").replace(/&ldquo;/g, "\u201C").replace(/&rdquo;/g, "\u201D")
    .replace(/&hellip;/g, "…");
}

function extractFromXml(xml: string, tag: string): string {
  const re = new RegExp(`<${tag}[^>]*>(?:<!\\[CDATA\\[)?([\\s\\S]*?)(?:\\]\\]>)?</${tag}>`);
  const m = xml.match(re);
  return m ? m[1].trim() : "";
}

function extractAttr(xml: string, tag: string, attr: string): string {
  const re = new RegExp(`<${tag}[^>]*${attr}=["']([^"']*)["']`);
  const m = xml.match(re);
  return m ? m[1] : "";
}

function extractImage(itemXml: string): string {
  // Try media:content, media:thumbnail, enclosure, then img in description
  let img = extractAttr(itemXml, "media:content", "url") ||
            extractAttr(itemXml, "media:thumbnail", "url") ||
            extractAttr(itemXml, "enclosure", "url");
  if (!img) {
    const desc = extractFromXml(itemXml, "description") + extractFromXml(itemXml, "content:encoded");
    const imgMatch = desc.match(/<img[^>]+src=["']([^"']+)["']/);
    if (imgMatch) img = imgMatch[1];
  }
  return img;
}

async function fetchRssFeeds(): Promise<number> {
  let total = 0;
  for (const feed of RSS_FEEDS) {
    try {
      const r = await loggedFetch("rss", feed.url, {
        headers: { "User-Agent": "SeaDisco/1.0 (music feed aggregator)" },
        signal: AbortSignal.timeout(15000),
        context: feed.name,
      });
      if (!r.ok) { console.warn(`RSS ${feed.name}: HTTP ${r.status}`); continue; }
      const xml = await r.text();

      // Split items
      const items = xml.split(/<item[\s>]/).slice(1);
      let count = 0;
      for (const itemXml of items.slice(0, 20)) { // Max 20 per feed
        try {
          const title = decodeHtmlEntities(extractFromXml(itemXml, "title"));
          const link = extractFromXml(itemXml, "link") || extractFromXml(itemXml, "guid");
          if (!title || !link) continue;

          let summary = decodeHtmlEntities(
            extractFromXml(itemXml, "description").replace(/<[^>]+>/g, "")
          ).trim();
          if (summary.length > 300) summary = summary.slice(0, 297) + "…";

          const imageUrl = extractImage(itemXml);

          const author = decodeHtmlEntities(
            extractFromXml(itemXml, "dc:creator") || extractFromXml(itemXml, "author") || ""
          );

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
        } catch (e) { /* skip bad item */ }
      }
      total += count;
      console.log(`RSS ${feed.name}: ${count} articles`);
    } catch (err) {
      console.warn(`RSS ${feed.name} failed:`, err);
    }
  }
  return total;
}

async function fetchYouTubeVideos(): Promise<number> {
  if (!youtubeApiKey) { console.log("YouTube feed skip — no API key"); return 0; }
  let total = 0;

  // Load existing YouTube video URLs to skip channels with no new content
  let existingUrls: Set<string> = new Set();
  try {
    existingUrls = await getExistingYouTubeUrls();
  } catch {}
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
            if (existingUrls.has(latestUrl)) { hasNew = false; }
          }
        }
      } catch {}
      if (!hasNew) { skipped++; continue; }

      const url = `https://www.googleapis.com/youtube/v3/search?key=${youtubeApiKey}&channelId=${ch.channelId}&part=snippet&order=date&maxResults=5&type=video`;
      const r = await loggedFetch("youtube", url, { signal: AbortSignal.timeout(10000), context: `channel: ${ch.name}` });
      if (!r.ok) { console.warn(`YouTube ${ch.name}: HTTP ${r.status}`); continue; }
      const data = await r.json() as any;
      let count = 0;
      for (const item of data.items ?? []) {
        const videoId = item.id?.videoId;
        if (!videoId) continue;
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
    } catch (err) {
      console.warn(`YouTube ${ch.name} failed:`, err);
    }
  }
  if (skipped) console.log(`[youtube] skipped ${skipped}/${YOUTUBE_CHANNELS.length} channels (no new videos)`);

  // Keyword searches for fresh music content (last 7 days)
  const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  for (const search of YOUTUBE_SEARCHES) {
    try {
      const url = `https://www.googleapis.com/youtube/v3/search?key=${youtubeApiKey}&q=${encodeURIComponent(search.query)}&part=snippet&order=date&maxResults=10&type=video&publishedAfter=${weekAgo}&videoCategoryId=10`;
      const r = await loggedFetch("youtube", url, { signal: AbortSignal.timeout(10000), context: `search: ${search.query}` });
      if (!r.ok) { console.warn(`YouTube search "${search.query}": HTTP ${r.status}`); continue; }
      const data = await r.json() as any;
      let count = 0;
      for (const item of data.items ?? []) {
        const videoId = item.id?.videoId;
        if (!videoId) continue;
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
    } catch (err) {
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
  const hibernated = await hibernateInactiveUsers().catch(() => 0);
  console.log(`Feed fetch complete: ${articles} articles, ${videos} videos, ${pruned} feed pruned${hibernated ? `, ${hibernated} users hibernated` : ""}`);
  const staleTotal = Object.values(stale).reduce((a, b) => a + b, 0);
  if (staleTotal > 0) {
    console.log(`Pruned stale data: ${JSON.stringify(stale)}`);
  }
}

function startFeedSchedule() {
  // Every 6 hours starting at 2:30 AM Pacific (YouTube quota: 15 calls × 100 = 1500 units × 4/day = 6000 < 10K limit)
  const ms = msUntilPacific(2, 30, 6);
  console.log(`[feed] Next fetch in ${Math.round(ms / 60000)}min, then every 6h`);
  setTimeout(() => {
    fetchAllFeedContent();
    setInterval(() => fetchAllFeedContent(), 6 * 60 * 60 * 1000);
  }, ms);
}

// GET /api/feed — public feed articles
app.get("/api/feed", async (req, res) => {
  try {
    const category = (req.query.category as string) || "all";
    const q = (req.query.q as string) || "";
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const offset = parseInt(req.query.offset as string) || 0;
    const { items, total } = await getFeedArticles({ category, limit, offset, q });
    res.setHeader("Cache-Control", items.length ? "public, max-age=300" : "no-cache");
    res.json({ items, total });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// POST /api/admin/drops/fetch — manual trigger for admin
app.post("/api/admin/drops/fetch", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  res.json({ ok: true, started: true });
  runFreshSync();
});

// POST /api/admin/feed/fetch — manual trigger for admin
app.post("/api/admin/feed/fetch", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  res.json({ ok: true, started: true });
  fetchAllFeedContent();
});

// POST /api/admin/live/fetch — manual trigger for admin
app.post("/api/admin/live/fetch", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const count = await fetchUpcomingEvents();
  res.json({ ok: true, count });
});

// POST /api/admin/extras/fetch — manual trigger for inventory/lists sync
app.post("/api/admin/extras/fetch", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
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
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
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
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  const hours = Math.min(parseInt(req.query.hours as string) || 24, 168);
  const stats = await getApiRequestStats(hours);
  res.json({ stats });
});

// GET /api/admin/price-stats — price tracking stats, admin only
app.get("/api/admin/price-stats", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  try {
    const stats = await getPriceStats();
    res.json({ ...stats, lastPriceUpdate: _lastPriceUpdate });
  } catch (err) {
    console.error("[admin] price-stats error:", err);
    res.status(500).json({ error: "Failed to get price stats" });
  }
});

// POST /api/admin/price-update — trigger manual price update, admin only
app.post("/api/admin/price-update", express.json(), async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
  if (!userId || !adminId || userId !== adminId) { res.status(403).json({ error: "Forbidden" }); return; }
  res.json({ ok: true, message: "Price update started" });
  runPriceUpdate();
});

// GET /api/admin/db-stats — row counts for all tables
app.get("/api/admin/db-stats", async (req, res) => {
  const userId = await getClerkUserId(req);
  const adminId = process.env.ADMIN_CLERK_ID ?? "";
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

    startFreshSyncSchedule();
    startGearSchedule();
    startVinylSchedule();
    startFeedSchedule();
    startDailySyncSchedule();
    startExtrasSyncSchedule();
    startLiveEventsSchedule();
    // startPriceUpdateSchedule(); // disabled — not using pricing
  }
});
