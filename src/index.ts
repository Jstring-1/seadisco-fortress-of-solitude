import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";import { z } from "zod";
import { DiscogsClient } from "./discogs-client.js";

const token = process.env.DISCOGS_TOKEN;
if (!token) {
  console.error("Error: DISCOGS_TOKEN environment variable is required.");
  process.exit(1);
}

const discogs = new DiscogsClient(token);
const server = new McpServer({
  name: "discogs-mcp",
  version: "1.0.0",
});

// ── Search ────────────────────────────────────────────────────────────────────

server.tool(
  "search_discogs",
  "Search the Discogs database for releases, masters, artists, or labels.",
  {
    query: z.string().describe("Search query string"),
    type: z
      .enum(["release", "master", "artist", "label"])
      .optional()
      .describe("Limit results to this type. Omit to search all types."),
    artist: z.string().optional().describe("Filter by artist name"),
    label: z.string().optional().describe("Filter by label name"),
    year: z.string().optional().describe("Filter by release year, e.g. '1975'"),
    genre: z.string().optional().describe("Filter by genre, e.g. 'Rock'"),
    style: z.string().optional().describe("Filter by style, e.g. 'Psychedelic Rock'"),
    page: z.number().int().min(1).optional().default(1).describe("Page number"),
    per_page: z
      .number()
      .int()
      .min(1)
      .max(100)
      .optional()
      .default(10)
      .describe("Results per page (max 100)"),
  },
  async ({ query, type, artist, label, year, genre, style, page, per_page }) => {
    const results = await discogs.search(query, {
      type,
      artist,
      label,
      year,
      genre,
      style,
      page,
      perPage: per_page,
    });
    return { content: [{ type: "text", text: JSON.stringify(results, null, 2) }] };
  }
);

// ── Release ───────────────────────────────────────────────────────────────────

server.tool(
  "get_release",
  "Get full details for a specific Discogs release by its release ID.",
  {
    release_id: z
      .union([z.number().int(), z.string()])
      .describe("Discogs release ID (numeric)"),
  },
  async ({ release_id }) => {
    const result = await discogs.getRelease(release_id);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

// ── Master Release ────────────────────────────────────────────────────────────

server.tool(
  "get_master_release",
  "Get details for a master release (represents the definitive version of a release). Includes all known versions/pressings.",
  {
    master_id: z
      .union([z.number().int(), z.string()])
      .describe("Discogs master release ID"),
  },
  async ({ master_id }) => {
    const result = await discogs.getMasterRelease(master_id);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.tool(
  "get_master_versions",
  "List all known pressings/versions of a master release.",
  {
    master_id: z
      .union([z.number().int(), z.string()])
      .describe("Discogs master release ID"),
    format: z.string().optional().describe("Filter by format, e.g. 'Vinyl'"),
    label: z.string().optional().describe("Filter by label name"),
    country: z.string().optional().describe("Filter by country, e.g. 'US'"),
    sort: z
      .enum(["released", "title", "format", "label", "catno", "country"])
      .optional()
      .describe("Sort field"),
    sort_order: z.enum(["asc", "desc"]).optional().describe("Sort order"),
    page: z.number().int().min(1).optional().default(1),
    per_page: z.number().int().min(1).max(100).optional().default(10),
  },
  async ({ master_id, format, label, country, sort, sort_order, page, per_page }) => {
    const result = await discogs.getMasterVersions(master_id, {
      format,
      label,
      country,
      sort,
      sortOrder: sort_order,
      page,
      perPage: per_page,
    });
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

// ── Artist ────────────────────────────────────────────────────────────────────

server.tool(
  "get_artist",
  "Get details for a Discogs artist by their artist ID.",
  {
    artist_id: z
      .union([z.number().int(), z.string()])
      .describe("Discogs artist ID"),
  },
  async ({ artist_id }) => {
    const result = await discogs.getArtist(artist_id);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.tool(
  "get_artist_releases",
  "List releases associated with a Discogs artist.",
  {
    artist_id: z
      .union([z.number().int(), z.string()])
      .describe("Discogs artist ID"),
    sort: z
      .enum(["year", "title", "format"])
      .optional()
      .describe("Sort field"),
    sort_order: z.enum(["asc", "desc"]).optional().describe("Sort order"),
    page: z.number().int().min(1).optional().default(1),
    per_page: z.number().int().min(1).max(100).optional().default(10),
  },
  async ({ artist_id, sort, sort_order, page, per_page }) => {
    const result = await discogs.getArtistReleases(artist_id, {
      sort,
      sortOrder: sort_order,
      page,
      perPage: per_page,
    });
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

// ── Label ─────────────────────────────────────────────────────────────────────

server.tool(
  "get_label",
  "Get details for a record label by its Discogs label ID.",
  {
    label_id: z
      .union([z.number().int(), z.string()])
      .describe("Discogs label ID"),
  },
  async ({ label_id }) => {
    const result = await discogs.getLabel(label_id);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.tool(
  "get_label_releases",
  "List releases published on a Discogs label.",
  {
    label_id: z
      .union([z.number().int(), z.string()])
      .describe("Discogs label ID"),
    page: z.number().int().min(1).optional().default(1),
    per_page: z.number().int().min(1).max(100).optional().default(10),
  },
  async ({ label_id, page, per_page }) => {
    const result = await discogs.getLabelReleases(label_id, {
      page,
      perPage: per_page,
    });
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

// ── Marketplace / Pricing ─────────────────────────────────────────────────────

server.tool(
  "get_marketplace_stats",
  "Get current Discogs marketplace statistics for a release: lowest price, median price, number currently for sale, and blocked-from-sale status.",
  {
    release_id: z
      .union([z.number().int(), z.string()])
      .describe("Discogs release ID"),
    currency: z
      .string()
      .optional()
      .describe(
        "Currency code for prices, e.g. 'USD', 'EUR', 'GBP'. Defaults to your account currency."
      ),
  },
  async ({ release_id, currency }) => {
    const result = await discogs.getMarketplaceStats(release_id, currency);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.tool(
  "get_price_suggestions",
  "Get Discogs suggested price ranges for each vinyl/media condition (Mint, Near Mint, Very Good Plus, etc.) for a release, based on recent sales history.",
  {
    release_id: z
      .union([z.number().int(), z.string()])
      .describe("Discogs release ID"),
  },
  async ({ release_id }) => {
    const result = await discogs.getPriceSuggestions(release_id);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

// ── Start ─────────────────────────────────────────────────────────────────────

import express from "express";
const app = express();
app.use(express.json());
const transport = new StreamableHTTPServerTransport({ path: "/mcp" });
app.post("/mcp", (req, res) => transport.handleRequest(req, res));
app.get("/mcp", (req, res) => transport.handleRequest(req, res));
await server.connect(transport);
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`MCP server listening on port ${PORT}`));
