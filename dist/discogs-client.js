import crypto from "crypto";
import { logApiRequest } from "./db.js";
const BASE_URL = "https://api.discogs.com";
export class DiscogsClient {
    token;
    oauth;
    appName;
    constructor(tokenOrOAuth, appName = "SeaDisco/1.0") {
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
        const start = Date.now();
        const response = await fetch(fullUrl, { headers });
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
