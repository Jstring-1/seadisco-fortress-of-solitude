const BASE_URL = "https://api.discogs.com";
export class DiscogsClient {
    headers;
    constructor(token, appName = "discogs-mcp/1.0") {
        this.headers = {
            Authorization: `Discogs token=${token}`,
            "User-Agent": appName,
            Accept: "application/vnd.discogs.v2.discogs+json",
        };
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
        const response = await fetch(url.toString(), { headers: this.headers });
        if (!response.ok) {
            const text = await response.text();
            throw new Error(`Discogs API error ${response.status}: ${text}`);
        }
        return response.json();
    }
    async search(query, options = {}) {
        const params = { q: query };
        if (options.type)
            params.type = options.type;
        if (options.artist)
            params.artist = options.artist;
        if (options.label)
            params.label = options.label;
        if (options.year)
            params.year = options.year;
        if (options.genre)
            params.genre = options.genre;
        if (options.style)
            params.style = options.style;
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
