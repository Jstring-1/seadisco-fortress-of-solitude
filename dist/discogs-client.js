import crypto from "crypto";
import { logApiRequest, getOAuthCredentials } from "./db.js";
const BASE_URL = "https://api.discogs.com";
// ── Per-key, priority Discogs request scheduler ───────────────────
// Discogs enforces its ~60/min authed limit PER CREDENTIAL, so we keep
// one independent rate lane per api key (PAT token / OAuth access
// token). A user's sync + marketplace traffic on their own token never
// competes with the admin sweeps on the admin token — each lane paces
// itself at ~1/sec against its own budget.
//
// Within a lane, three priority tiers decide who gets the next slot:
//   realtime  — user-facing: live search, album-modal fetches, a user
//               triggering a sync or a marketplace action. Highest.
//   scheduled — background-but-timely jobs: scheduled sync/refresh,
//               suggestions, upstream-stats. Yields to realtime.
//   sweep     — the bulk cache crawlers (cache-warm, faceted, catno).
//               Lowest: yields to everything, resumes when the lane is
//               otherwise idle.
// A higher tier that arrives mid-wait is dispatched before any waiting
// lower-tier caller, so sweeps never make a user wait behind the crawl.
//
// Pacing is measured from the previous DISPATCH (not gap-after-fetch),
// so a request's own latency is absorbed into the interval and a lone
// caller lands near the true 1/sec ceiling. Tunable via
// DISCOGS_MIN_INTERVAL_MS.
const DISCOGS_MIN_INTERVAL_MS = Math.max(0, Number(process.env.DISCOGS_MIN_INTERVAL_MS) || 1050);
const _PRIORITY_ORDER = ["realtime", "scheduled", "sweep"];
const _gateLanes = new Map();
function _laneFor(keyId) {
    let lane = _gateLanes.get(keyId);
    if (!lane) {
        lane = { lastDispatch: 0, queues: { realtime: [], scheduled: [], sweep: [] }, pumping: false };
        _gateLanes.set(keyId, lane);
    }
    return lane;
}
function _nextWaiter(lane) {
    for (const p of _PRIORITY_ORDER) {
        const q = lane.queues[p];
        if (q.length)
            return q.shift();
    }
    return undefined;
}
function _hasWaiter(lane) {
    return _PRIORITY_ORDER.some(p => lane.queues[p].length > 0);
}
function _pump(lane) {
    if (lane.pumping)
        return;
    lane.pumping = true;
    const tick = () => {
        if (!_hasWaiter(lane)) {
            lane.pumping = false;
            return;
        }
        const wait = Math.max(0, lane.lastDispatch + DISCOGS_MIN_INTERVAL_MS - Date.now());
        setTimeout(() => {
            // Re-pick the highest-priority waiter AT dispatch time — a realtime
            // call that arrived during the wait jumps ahead of a queued sweep.
            const resolve = _nextWaiter(lane);
            lane.lastDispatch = Date.now();
            if (resolve)
                resolve();
            tick();
        }, wait);
    };
    tick();
}
// Acquire a rate slot on `keyId`'s lane at the given priority. Resolves
// when the caller may dispatch its Discogs request.
export function discogsGate(keyId, priority = "realtime") {
    const lane = _laneFor(keyId || "anon");
    return new Promise(resolve => {
        lane.queues[priority].push(resolve);
        _pump(lane);
    });
}
// Derive a stable per-credential lane key from an Authorization header
// (so raw fetch() callers that build their own headers land on the same
// lane as this.get()). PAT: "Discogs token=XXX"; OAuth 1.0a:
// "OAuth …,oauth_token=\"YYY\",…". Falls back to a hash of the whole
// header, then "anon". Never returns the raw secret.
export function discogsKeyFromAuthHeader(auth) {
    if (!auth)
        return "anon";
    const pat = auth.match(/Discogs\s+token=([^,\s]+)/i);
    if (pat)
        return "pat:" + _shortHash(pat[1]);
    const oauth = auth.match(/oauth_token="?([^",\s]+)"?/i);
    if (oauth)
        return "oauth:" + _shortHash(oauth[1]);
    return "hdr:" + _shortHash(auth);
}
function _shortHash(s) {
    return crypto.createHash("sha1").update(s).digest("hex").slice(0, 12);
}
export class DiscogsClient {
    token;
    oauth;
    appName;
    // Rate-lane priority for every request this client makes. Default
    // realtime (interactive); background workers downgrade to "sweep" or
    // "scheduled" so they yield to user-facing traffic on the same lane.
    priority = "realtime";
    constructor(tokenOrOAuth, appName = "SeaDisco/1.0 (+https://seadisco.com)") {
        this.appName = appName;
        if (typeof tokenOrOAuth === "string") {
            this.token = tokenOrOAuth;
            this.oauth = null;
        }
        else {
            this.token = null;
            this.oauth = tokenOrOAuth;
        }
    }
    /** Fluent priority setter — `client.withPriority("sweep")`. */
    withPriority(p) { this.priority = p; return this; }
    /** Stable per-credential lane key (never the raw secret). MUST match
     *  discogsKeyFromAuthHeader so get() and raw fetch() land on one lane. */
    _gateKey() {
        if (this.token)
            return "pat:" + _shortHash(this.token);
        if (this.oauth?.accessToken)
            return "oauth:" + _shortHash(this.oauth.accessToken);
        return "anon";
    }
    /** Acquire a rate slot on this client's lane — for raw fetch() callers
     *  (sync, marketplace) that build their own headers and bypass get(). */
    async gate(priority = this.priority) {
        await discogsGate(this._gateKey(), priority);
    }
    /** Build authorization headers for an external URL — either PAT or OAuth 1.0a signed.
     *  Use this when you need to make raw fetch() calls with proper auth (e.g. sync). */
    buildHeaders(method, url) {
        return this.getAuthHeaders(method, url);
    }
    /** Build authorization headers — either PAT or OAuth 1.0a signed */
    getAuthHeaders(method, url) {
        const common = {
            "User-Agent": this.appName,
            "Accept": "application/vnd.discogs.v2.discogs+json",
        };
        if (this.token) {
            return { ...common, Authorization: `Discogs token=${this.token}` };
        }
        if (this.oauth) {
            return { ...common, Authorization: signOAuth(method, url, this.oauth) };
        }
        return common;
    }
    async get(path, params) {
        const url = new URL(`${BASE_URL}${path}`);
        if (params) {
            for (const [key, value] of Object.entries(params)) {
                if (value !== undefined && value !== "") {
                    url.searchParams.set(key, value);
                }
            }
        }
        const fullUrl = url.toString();
        const headers = this.getAuthHeaders("GET", fullUrl);
        // Wait for a rate slot on this credential's lane at this client's
        // priority before dispatching (see discogsGate).
        await discogsGate(this._gateKey(), this.priority);
        // 30 second hard timeout. Prevents a stalled Discogs socket from
        // locking any caller (sync, background cache-warm worker, search
        // endpoints) indefinitely. AbortController fires the abort signal
        // when the timer expires; fetch then rejects with an AbortError
        // that propagates to the caller as a clear timeout message.
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 30_000);
        const start = Date.now();
        let response;
        try {
            response = await fetch(fullUrl, { headers, signal: controller.signal });
        }
        catch (err) {
            const ms = Date.now() - start;
            const cleanPath = path.replace(/token=[^&]+/g, "token=***");
            logApiRequest({ service: "discogs", endpoint: `${BASE_URL}${cleanPath}`, statusCode: 0, success: false, durationMs: ms, context: "client" }).catch(() => { });
            if (err?.name === "AbortError") {
                throw new Error(`Discogs API timeout after 30s: ${cleanPath}`);
            }
            throw err;
        }
        finally {
            clearTimeout(timer);
        }
        const ms = Date.now() - start;
        const cleanPath = path.replace(/token=[^&]+/g, "token=***");
        logApiRequest({ service: "discogs", endpoint: `${BASE_URL}${cleanPath}`, statusCode: response.status, success: response.ok, durationMs: ms, context: "client" }).catch(() => { });
        if (!response.ok) {
            const text = await response.text();
            throw new Error(`Discogs API error ${response.status}: ${text}`);
        }
        return response.json();
    }
    async search(query = "", options = {}) {
        const params = {};
        if (query)
            params.q = query;
        if (options.type)
            params.type = options.type;
        if (options.artist)
            params.artist = options.artist;
        if (options.releaseTitle)
            params.release_title = options.releaseTitle;
        if (options.label)
            params.label = options.label;
        if (options.year)
            params.year = options.year;
        if (options.genre)
            params.genre = options.genre;
        if (options.style)
            params.style = options.style;
        if (options.format)
            params.format = options.format;
        if (options.country)
            params.country = options.country;
        if (options.barcode)
            params.barcode = options.barcode;
        if (options.catno)
            params.catno = options.catno;
        if (options.sort)
            params.sort = options.sort;
        if (options.sortOrder)
            params.sort_order = options.sortOrder;
        params.page = String(options.page ?? 1);
        params.per_page = String(options.perPage ?? 10);
        return this.get("/database/search", params);
    }
    async getRelease(releaseId) {
        return this.get(`/releases/${releaseId}`);
    }
    async getMasterRelease(masterId) {
        return this.get(`/masters/${masterId}`);
    }
    async getMasterVersions(masterId, options = {}) {
        const params = {};
        params.page = String(options.page ?? 1);
        params.per_page = String(options.perPage ?? 10);
        if (options.format)
            params.format = options.format;
        if (options.label)
            params.label = options.label;
        if (options.country)
            params.country = options.country;
        if (options.sort)
            params.sort = options.sort;
        if (options.sortOrder)
            params.sort_order = options.sortOrder;
        return this.get(`/masters/${masterId}/versions`, params);
    }
    async getArtist(artistId) {
        return this.get(`/artists/${artistId}`);
    }
    async getArtistReleases(artistId, options = {}) {
        const params = {};
        if (options.sort)
            params.sort = options.sort;
        if (options.sortOrder)
            params.sort_order = options.sortOrder;
        params.page = String(options.page ?? 1);
        params.per_page = String(options.perPage ?? 10);
        return this.get(`/artists/${artistId}/releases`, params);
    }
    async getLabel(labelId) {
        return this.get(`/labels/${labelId}`);
    }
    async getLabelReleases(labelId, options = {}) {
        const params = {
            page: String(options.page ?? 1),
            per_page: String(options.perPage ?? 10),
        };
        if (options.sort)
            params.sort = options.sort;
        if (options.sortOrder)
            params.sort_order = options.sortOrder;
        return this.get(`/labels/${labelId}/releases`, params);
    }
    async getMarketplaceStats(releaseId, currency) {
        const params = {};
        if (currency)
            params.curr_abbr = currency;
        return this.get(`/marketplace/stats/${releaseId}`, params);
    }
    async getPriceSuggestions(releaseId) {
        return this.get(`/marketplace/price_suggestions/${releaseId}`);
    }
}
// ── OAuth 1.0a signing ───────────────────────────────────────────────────
function percentEncode(str) {
    return encodeURIComponent(str).replace(/[!'()*]/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}
export function signOAuth(method, url, creds, extraParams) {
    const nonce = crypto.randomBytes(16).toString("hex");
    const timestamp = Math.floor(Date.now() / 1000).toString();
    const oauthParams = {
        oauth_consumer_key: creds.consumerKey,
        oauth_nonce: nonce,
        oauth_signature_method: "HMAC-SHA1",
        oauth_timestamp: timestamp,
        oauth_token: creds.accessToken,
        oauth_version: "1.0",
        ...extraParams,
    };
    // Parse URL and combine all params
    const parsed = new URL(url);
    const allParams = { ...oauthParams };
    parsed.searchParams.forEach((v, k) => { allParams[k] = v; });
    // Sort and encode
    const paramString = Object.keys(allParams).sort()
        .map(k => `${percentEncode(k)}=${percentEncode(allParams[k])}`)
        .join("&");
    const baseUrl = `${parsed.origin}${parsed.pathname}`;
    const baseString = `${method.toUpperCase()}&${percentEncode(baseUrl)}&${percentEncode(paramString)}`;
    const signingKey = `${percentEncode(creds.consumerSecret)}&${percentEncode(creds.accessSecret)}`;
    const signature = crypto.createHmac("sha1", signingKey).update(baseString).digest("base64");
    oauthParams["oauth_signature"] = signature;
    const header = "OAuth " + Object.keys(oauthParams).sort()
        .map(k => `${percentEncode(k)}="${percentEncode(oauthParams[k])}"`)
        .join(", ");
    return header;
}
/** Sign an OAuth request for the initial token exchange (no access token yet) */
export function signOAuthRequest(method, url, consumerKey, consumerSecret, token, tokenSecret, verifier, callback) {
    const nonce = crypto.randomBytes(16).toString("hex");
    const timestamp = Math.floor(Date.now() / 1000).toString();
    const oauthParams = {
        oauth_consumer_key: consumerKey,
        oauth_nonce: nonce,
        oauth_signature_method: "HMAC-SHA1",
        oauth_timestamp: timestamp,
        oauth_version: "1.0",
    };
    if (token)
        oauthParams["oauth_token"] = token;
    if (verifier)
        oauthParams["oauth_verifier"] = verifier;
    if (callback)
        oauthParams["oauth_callback"] = callback;
    const parsed = new URL(url);
    const allParams = { ...oauthParams };
    parsed.searchParams.forEach((v, k) => { allParams[k] = v; });
    const paramString = Object.keys(allParams).sort()
        .map(k => `${percentEncode(k)}=${percentEncode(allParams[k])}`)
        .join("&");
    const baseUrl = `${parsed.origin}${parsed.pathname}`;
    const baseString = `${method.toUpperCase()}&${percentEncode(baseUrl)}&${percentEncode(paramString)}`;
    const signingKey = `${percentEncode(consumerSecret)}&${percentEncode(tokenSecret ?? "")}`;
    const signature = crypto.createHmac("sha1", signingKey).update(baseString).digest("base64");
    oauthParams["oauth_signature"] = signature;
    return "OAuth " + Object.keys(oauthParams).sort()
        .map(k => `${percentEncode(k)}="${percentEncode(oauthParams[k])}"`)
        .join(", ");
}
// Build an OAuth DiscogsClient for the given admin/user clerk id, or
// null if we can't (no clerk id, no stored OAuth tokens, or the app's
// consumer key/secret env vars aren't configured). Every background
// worker used to inline an identical `_adminClient()` — this is the
// single shared source of truth so the auth shape can't drift between
// them.
export async function getAdminDiscogsClient(clerkId) {
    if (!clerkId)
        return null;
    const oauth = await getOAuthCredentials(clerkId);
    if (!oauth)
        return null;
    if (!process.env.DISCOGS_CONSUMER_KEY || !process.env.DISCOGS_CONSUMER_SECRET)
        return null;
    return new DiscogsClient({
        consumerKey: process.env.DISCOGS_CONSUMER_KEY,
        consumerSecret: process.env.DISCOGS_CONSUMER_SECRET,
        accessToken: oauth.accessToken,
        accessSecret: oauth.accessSecret,
    });
}
