import crypto from "crypto";
import { logApiRequest, getOAuthCredentials } from "./db.js";

const BASE_URL = "https://api.discogs.com";

// ── Process-wide Discogs request scheduler ────────────────────────
// Every DiscogsClient.get() passes through this gate before firing, so
// the WHOLE process shares one authed 60/min budget — the background
// sweeps (cache-warm, label-bulk, faceted, catno), the collection sync,
// and live user searches all draw from the same pace instead of each
// sleeping independently and collectively blowing past the limit into
// 429 backoff.
//
// It also fixes the "sleep-then-fetch" spacing bug: pacing is measured
// from the previous DISPATCH, not "gap after the last fetch finished",
// so a request's own network latency is absorbed into the interval
// rather than added on top. A lone sweep therefore lands close to the
// true 1/sec ceiling instead of ~1 per 1.5s.
//
// Implemented as a serial promise chain: each acquirer awaits the prior
// dispatch, then waits out any remaining interval. Depth ≈ number of
// concurrently-active callers (each awaits its slot AND its fetch before
// looping), so no single caller floods the queue — an interactive call
// waits behind at most a handful of in-flight sweep slots (~1-2s), not
// the crawler's entire backlog. Tunable via DISCOGS_MIN_INTERVAL_MS.
const DISCOGS_MIN_INTERVAL_MS = Math.max(0, Number(process.env.DISCOGS_MIN_INTERVAL_MS) || 1050);
let _discogsGateChain: Promise<void> = Promise.resolve();
let _discogsLastDispatch = 0;

export function discogsGate(): Promise<void> {
  const prev = _discogsGateChain;
  _discogsGateChain = (async () => {
    try { await prev; } catch { /* never let one waiter's rejection wedge the chain */ }
    const wait = _discogsLastDispatch + DISCOGS_MIN_INTERVAL_MS - Date.now();
    if (wait > 0) await new Promise(r => setTimeout(r, wait));
    _discogsLastDispatch = Date.now();
  })();
  return _discogsGateChain;
}

export interface OAuthCredentials {
  consumerKey: string;
  consumerSecret: string;
  accessToken: string;
  accessSecret: string;
}

export class DiscogsClient {
  private token: string | null;
  private oauth: OAuthCredentials | null;
  private appName: string;

  constructor(tokenOrOAuth: string | OAuthCredentials, appName = "SeaDisco/1.0 (+https://seadisco.com)") {
    this.appName = appName;
    if (typeof tokenOrOAuth === "string") {
      this.token = tokenOrOAuth;
      this.oauth = null;
    } else {
      this.token = null;
      this.oauth = tokenOrOAuth;
    }
  }

  /** Build authorization headers for an external URL — either PAT or OAuth 1.0a signed.
   *  Use this when you need to make raw fetch() calls with proper auth (e.g. sync). */
  public buildHeaders(method: string, url: string): Record<string, string> {
    return this.getAuthHeaders(method, url);
  }

  /** Build authorization headers — either PAT or OAuth 1.0a signed */
  private getAuthHeaders(method: string, url: string): Record<string, string> {
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

  private async get<T>(path: string, params?: Record<string, string>): Promise<T> {
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

    // Wait for a process-wide rate-limit slot before dispatching, so
    // all Discogs traffic shares one 60/min budget (see discogsGate).
    await discogsGate();

    // 30 second hard timeout. Prevents a stalled Discogs socket from
    // locking any caller (sync, background cache-warm worker, search
    // endpoints) indefinitely. AbortController fires the abort signal
    // when the timer expires; fetch then rejects with an AbortError
    // that propagates to the caller as a clear timeout message.
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 30_000);
    const start = Date.now();
    let response: Response;
    try {
      response = await fetch(fullUrl, { headers, signal: controller.signal });
    } catch (err: any) {
      const ms = Date.now() - start;
      const cleanPath = path.replace(/token=[^&]+/g, "token=***");
      logApiRequest({ service: "discogs", endpoint: `${BASE_URL}${cleanPath}`, statusCode: 0, success: false, durationMs: ms, context: "client" }).catch(() => {});
      if (err?.name === "AbortError") {
        throw new Error(`Discogs API timeout after 30s: ${cleanPath}`);
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
    const ms = Date.now() - start;
    const cleanPath = path.replace(/token=[^&]+/g, "token=***");
    logApiRequest({ service: "discogs", endpoint: `${BASE_URL}${cleanPath}`, statusCode: response.status, success: response.ok, durationMs: ms, context: "client" }).catch(() => {});

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Discogs API error ${response.status}: ${text}`);
    }

    return response.json() as Promise<T>;
  }

  async search(query: string = "", options: {
    type?: "release" | "master" | "artist" | "label";
    artist?: string;
    releaseTitle?: string;
    label?: string;
    year?: string;
    genre?: string;
    style?: string;
    format?: string;
    country?: string;
    barcode?: string;
    catno?: string;
    sort?: string;
    sortOrder?: "asc" | "desc";
    page?: number;
    perPage?: number;
  } = {}) {
    const params: Record<string, string> = {};
    if (query) params.q = query;
    if (options.type) params.type = options.type;
    if (options.artist) params.artist = options.artist;
    if (options.releaseTitle) params.release_title = options.releaseTitle;
    if (options.label) params.label = options.label;
    if (options.year) params.year = options.year;
    if (options.genre) params.genre = options.genre;
    if (options.style) params.style = options.style;
    if (options.format) params.format = options.format;
    if (options.country) params.country = options.country;
    if (options.barcode) params.barcode = options.barcode;
    if (options.catno) params.catno = options.catno;
    if (options.sort) params.sort = options.sort;
    if (options.sortOrder) params.sort_order = options.sortOrder;
    params.page = String(options.page ?? 1);
    params.per_page = String(options.perPage ?? 10);
    return this.get("/database/search", params);
  }

  async getRelease(releaseId: number | string) {
    return this.get(`/releases/${releaseId}`);
  }

  async getMasterRelease(masterId: number | string) {
    return this.get(`/masters/${masterId}`);
  }

  async getMasterVersions(masterId: number | string, options: {
    page?: number;
    perPage?: number;
    format?: string;
    label?: string;
    country?: string;
    sort?: string;
    sortOrder?: "asc" | "desc";
  } = {}) {
    const params: Record<string, string> = {};
    params.page = String(options.page ?? 1);
    params.per_page = String(options.perPage ?? 10);
    if (options.format) params.format = options.format;
    if (options.label) params.label = options.label;
    if (options.country) params.country = options.country;
    if (options.sort) params.sort = options.sort;
    if (options.sortOrder) params.sort_order = options.sortOrder;
    return this.get(`/masters/${masterId}/versions`, params);
  }

  async getArtist(artistId: number | string) {
    return this.get(`/artists/${artistId}`);
  }

  async getArtistReleases(artistId: number | string, options: {
    sort?: "year" | "title" | "format";
    sortOrder?: "asc" | "desc";
    page?: number;
    perPage?: number;
  } = {}) {
    const params: Record<string, string> = {};
    if (options.sort) params.sort = options.sort;
    if (options.sortOrder) params.sort_order = options.sortOrder;
    params.page = String(options.page ?? 1);
    params.per_page = String(options.perPage ?? 10);
    return this.get(`/artists/${artistId}/releases`, params);
  }

  async getLabel(labelId: number | string) {
    return this.get(`/labels/${labelId}`);
  }

  async getLabelReleases(labelId: number | string, options: {
    page?: number;
    perPage?: number;
    // Discogs supports sort=year|title|format for this endpoint.
    // Anything else is silently ignored upstream — caller is
    // responsible for sending one of those.
    sort?: string;
    sortOrder?: "asc" | "desc";
  } = {}) {
    const params: Record<string, string> = {
      page: String(options.page ?? 1),
      per_page: String(options.perPage ?? 10),
    };
    if (options.sort)      params.sort       = options.sort;
    if (options.sortOrder) params.sort_order = options.sortOrder;
    return this.get(`/labels/${labelId}/releases`, params);
  }

  async getMarketplaceStats(releaseId: number | string, currency?: string) {
    const params: Record<string, string> = {};
    if (currency) params.curr_abbr = currency;
    return this.get(`/marketplace/stats/${releaseId}`, params);
  }

  async getPriceSuggestions(releaseId: number | string) {
    return this.get(`/marketplace/price_suggestions/${releaseId}`);
  }
}

// ── OAuth 1.0a signing ───────────────────────────────────────────────────

function percentEncode(str: string): string {
  return encodeURIComponent(str).replace(/[!'()*]/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

export function signOAuth(method: string, url: string, creds: OAuthCredentials, extraParams?: Record<string, string>): string {
  const nonce = crypto.randomBytes(16).toString("hex");
  const timestamp = Math.floor(Date.now() / 1000).toString();

  const oauthParams: Record<string, string> = {
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
  const allParams: Record<string, string> = { ...oauthParams };
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
export function signOAuthRequest(method: string, url: string, consumerKey: string, consumerSecret: string, token?: string, tokenSecret?: string, verifier?: string, callback?: string): string {
  const nonce = crypto.randomBytes(16).toString("hex");
  const timestamp = Math.floor(Date.now() / 1000).toString();

  const oauthParams: Record<string, string> = {
    oauth_consumer_key: consumerKey,
    oauth_nonce: nonce,
    oauth_signature_method: "HMAC-SHA1",
    oauth_timestamp: timestamp,
    oauth_version: "1.0",
  };
  if (token) oauthParams["oauth_token"] = token;
  if (verifier) oauthParams["oauth_verifier"] = verifier;
  if (callback) oauthParams["oauth_callback"] = callback;

  const parsed = new URL(url);
  const allParams: Record<string, string> = { ...oauthParams };
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
export async function getAdminDiscogsClient(clerkId: string | null | undefined): Promise<DiscogsClient | null> {
  if (!clerkId) return null;
  const oauth = await getOAuthCredentials(clerkId);
  if (!oauth) return null;
  if (!process.env.DISCOGS_CONSUMER_KEY || !process.env.DISCOGS_CONSUMER_SECRET) return null;
  return new DiscogsClient({
    consumerKey:    process.env.DISCOGS_CONSUMER_KEY,
    consumerSecret: process.env.DISCOGS_CONSUMER_SECRET,
    accessToken:    oauth.accessToken,
    accessSecret:   oauth.accessSecret,
  } as OAuthCredentials);
}
