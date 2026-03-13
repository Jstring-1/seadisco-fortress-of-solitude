const BASE_URL = "https://api.discogs.com";

export class DiscogsClient {
  private headers: Record<string, string>;

  constructor(token: string, appName = "discogs-mcp/1.0") {
    this.headers = {
      Authorization: `Discogs token=${token}`,
      "User-Agent": appName,
      Accept: "application/vnd.discogs.v2.discogs+json",
    };
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

    const response = await fetch(url.toString(), { headers: this.headers });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Discogs API error ${response.status}: ${text}`);
    }

    return response.json() as Promise<T>;
  }

  async search(query: string, options: {
    type?: "release" | "master" | "artist" | "label";
    artist?: string;
    label?: string;
    year?: string;
    genre?: string;
    style?: string;
    page?: number;
    perPage?: number;
  } = {}) {
    const params: Record<string, string> = { q: query };
    if (options.type) params.type = options.type;
    if (options.artist) params.artist = options.artist;
    if (options.label) params.label = options.label;
    if (options.year) params.year = options.year;
    if (options.genre) params.genre = options.genre;
    if (options.style) params.style = options.style;
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
  } = {}) {
    const params: Record<string, string> = {
      page: String(options.page ?? 1),
      per_page: String(options.perPage ?? 10),
    };
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
