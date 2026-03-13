import { randomUUID } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { mcpAuthRouter } from "@modelcontextprotocol/sdk/server/auth/router.js";
import { requireBearerAuth } from "@modelcontextprotocol/sdk/server/auth/middleware/bearerAuth.js";
import { z } from "zod";
import { DiscogsClient } from "./discogs-client.js";
// ── OAuth provider ────────────────────────────────────────────────────────────
class SimpleClientsStore {
    clients = new Map();
    async getClient(clientId) {
        return this.clients.get(clientId);
    }
    async registerClient(metadata) {
        const client = {
            ...metadata,
            client_id: randomUUID(),
            client_id_issued_at: Math.floor(Date.now() / 1000),
        };
        this.clients.set(client.client_id, client);
        return client;
    }
}
class SimpleOAuthProvider {
    clientsStore = new SimpleClientsStore();
    codes = new Map();
    tokens = new Map();
    async authorize(client, params, res) {
        const code = randomUUID();
        this.codes.set(code, { client, params });
        const redirectUrl = new URL(params.redirectUri);
        redirectUrl.searchParams.set("code", code);
        if (params.state)
            redirectUrl.searchParams.set("state", params.state);
        res.redirect(redirectUrl.toString());
    }
    async challengeForAuthorizationCode(_client, code) {
        const data = this.codes.get(code);
        if (!data)
            throw new Error("Invalid authorization code");
        return data.params.codeChallenge;
    }
    async exchangeAuthorizationCode(client, code) {
        const data = this.codes.get(code);
        if (!data)
            throw new Error("Invalid authorization code");
        if (data.client.client_id !== client.client_id)
            throw new Error("Code not issued to this client");
        this.codes.delete(code);
        const token = randomUUID();
        const expiresAt = Math.floor(Date.now() / 1000) + 86400; // 24 hours
        this.tokens.set(token, {
            token,
            clientId: client.client_id,
            scopes: data.params.scopes ?? [],
            expiresAt,
        });
        const result = {
            access_token: token,
            token_type: "bearer",
            expires_in: 86400,
        };
        return result;
    }
    async exchangeRefreshToken() {
        throw new Error("Refresh tokens not supported");
    }
    async verifyAccessToken(token) {
        const info = this.tokens.get(token);
        if (!info || info.expiresAt < Math.floor(Date.now() / 1000)) {
            throw new Error("Invalid or expired token");
        }
        return info;
    }
    async revokeToken(_client, req) {
        this.tokens.delete(req.token);
    }
}
// ── MCP tool registration ─────────────────────────────────────────────────────
function buildMcpServer() {
    const token = process.env.DISCOGS_TOKEN;
    if (!token) {
        console.error("Error: DISCOGS_TOKEN environment variable is required.");
        process.exit(1);
    }
    const discogs = new DiscogsClient(token);
    const server = new McpServer({ name: "discogs-mcp", version: "1.0.0" });
    server.tool("search_discogs", "Search the Discogs database for releases, masters, artists, or labels.", {
        query: z.string().describe("Search query string"),
        type: z.enum(["release", "master", "artist", "label"]).optional().describe("Limit results to this type. Omit to search all types."),
        artist: z.string().optional().describe("Filter by artist name"),
        label: z.string().optional().describe("Filter by label name"),
        year: z.string().optional().describe("Filter by release year, e.g. '1975'"),
        genre: z.string().optional().describe("Filter by genre, e.g. 'Rock'"),
        style: z.string().optional().describe("Filter by style, e.g. 'Psychedelic Rock'"),
        page: z.number().int().min(1).optional().default(1).describe("Page number"),
        per_page: z.number().int().min(1).max(100).optional().default(10).describe("Results per page (max 100)"),
    }, async ({ query, type, artist, label, year, genre, style, page, per_page }) => {
        const results = await discogs.search(query, { type, artist, label, year, genre, style, page, perPage: per_page });
        return { content: [{ type: "text", text: JSON.stringify(results, null, 2) }] };
    });
    server.tool("get_release", "Get full details for a specific Discogs release by its release ID.", { release_id: z.union([z.number().int(), z.string()]).describe("Discogs release ID (numeric)") }, async ({ release_id }) => {
        const result = await discogs.getRelease(release_id);
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    });
    server.tool("get_master_release", "Get details for a master release (represents the definitive version of a release). Includes all known versions/pressings.", { master_id: z.union([z.number().int(), z.string()]).describe("Discogs master release ID") }, async ({ master_id }) => {
        const result = await discogs.getMasterRelease(master_id);
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    });
    server.tool("get_master_versions", "List all known pressings/versions of a master release.", {
        master_id: z.union([z.number().int(), z.string()]).describe("Discogs master release ID"),
        format: z.string().optional().describe("Filter by format, e.g. 'Vinyl'"),
        label: z.string().optional().describe("Filter by label name"),
        country: z.string().optional().describe("Filter by country, e.g. 'US'"),
        sort: z.enum(["released", "title", "format", "label", "catno", "country"]).optional().describe("Sort field"),
        sort_order: z.enum(["asc", "desc"]).optional().describe("Sort order"),
        page: z.number().int().min(1).optional().default(1),
        per_page: z.number().int().min(1).max(100).optional().default(10),
    }, async ({ master_id, format, label, country, sort, sort_order, page, per_page }) => {
        const result = await discogs.getMasterVersions(master_id, { format, label, country, sort, sortOrder: sort_order, page, perPage: per_page });
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    });
    server.tool("get_artist", "Get details for a Discogs artist by their artist ID.", { artist_id: z.union([z.number().int(), z.string()]).describe("Discogs artist ID") }, async ({ artist_id }) => {
        const result = await discogs.getArtist(artist_id);
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    });
    server.tool("get_artist_releases", "List releases associated with a Discogs artist.", {
        artist_id: z.union([z.number().int(), z.string()]).describe("Discogs artist ID"),
        sort: z.enum(["year", "title", "format"]).optional().describe("Sort field"),
        sort_order: z.enum(["asc", "desc"]).optional().describe("Sort order"),
        page: z.number().int().min(1).optional().default(1),
        per_page: z.number().int().min(1).max(100).optional().default(10),
    }, async ({ artist_id, sort, sort_order, page, per_page }) => {
        const result = await discogs.getArtistReleases(artist_id, { sort, sortOrder: sort_order, page, perPage: per_page });
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    });
    server.tool("get_label", "Get details for a record label by its Discogs label ID.", { label_id: z.union([z.number().int(), z.string()]).describe("Discogs label ID") }, async ({ label_id }) => {
        const result = await discogs.getLabel(label_id);
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    });
    server.tool("get_label_releases", "List releases published on a Discogs label.", {
        label_id: z.union([z.number().int(), z.string()]).describe("Discogs label ID"),
        page: z.number().int().min(1).optional().default(1),
        per_page: z.number().int().min(1).max(100).optional().default(10),
    }, async ({ label_id, page, per_page }) => {
        const result = await discogs.getLabelReleases(label_id, { page, perPage: per_page });
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    });
    server.tool("get_marketplace_stats", "Get current Discogs marketplace statistics for a release: lowest price, median price, number currently for sale, and blocked-from-sale status.", {
        release_id: z.union([z.number().int(), z.string()]).describe("Discogs release ID"),
        currency: z.string().optional().describe("Currency code for prices, e.g. 'USD', 'EUR', 'GBP'. Defaults to your account currency."),
    }, async ({ release_id, currency }) => {
        const result = await discogs.getMarketplaceStats(release_id, currency);
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    });
    server.tool("get_price_suggestions", "Get Discogs suggested price ranges for each vinyl/media condition (Mint, Near Mint, Very Good Plus, etc.) for a release, based on recent sales history.", { release_id: z.union([z.number().int(), z.string()]).describe("Discogs release ID") }, async ({ release_id }) => {
        const result = await discogs.getPriceSuggestions(release_id);
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    });
    return server;
}
// ── Express app ───────────────────────────────────────────────────────────────
const serverUrl = process.env.MCP_SERVER_URL;
if (!serverUrl) {
    console.error("Error: MCP_SERVER_URL environment variable is required (e.g. https://your-app.railway.app)");
    process.exit(1);
}
const issuerUrl = new URL(serverUrl);
const oauthProvider = new SimpleOAuthProvider();
const app = createMcpExpressApp({ host: "0.0.0.0" });
// OAuth endpoints
app.use(mcpAuthRouter({
    provider: oauthProvider,
    issuerUrl,
    scopesSupported: ["mcp:tools"],
    resourceName: "Discogs MCP Server",
}));
// MCP endpoint
const authMiddleware = requireBearerAuth({ verifier: oauthProvider });
app.post("/mcp", authMiddleware, async (req, res) => {
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    const server = buildMcpServer();
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
    res.on("close", () => {
        transport.close();
        server.close();
    });
});
app.get("/mcp", (_req, res) => {
    res.status(405).json({ jsonrpc: "2.0", error: { code: -32000, message: "Method not allowed." }, id: null });
});
app.delete("/mcp", (_req, res) => {
    res.status(405).json({ jsonrpc: "2.0", error: { code: -32000, message: "Method not allowed." }, id: null });
});
const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3000;
app.listen(PORT, "0.0.0.0", () => {
    console.log(`Discogs MCP server listening on port ${PORT}`);
    console.log(`MCP endpoint: ${serverUrl}/mcp`);
});
