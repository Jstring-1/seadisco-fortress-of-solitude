# SeaDisco

An invite-only music-discovery web app built on top of the Discogs API. Search and browse releases, manage a Discogs collection / wantlist / inventory / seller orders in-place, get AI-powered search recommendations via Claude, and listen to public-domain audio from the Library of Congress.

**Production:** https://seadisco.com (Railway, auto-deploys from `main`)

## Stack

- **Backend:** Node 20+ / Express 4 / TypeScript (ESM)
- **DB:** PostgreSQL via `pg` pool (Railway)
- **Auth:** Clerk (waitlist-gated sign-up)
- **Discogs:** OAuth 1.0a (full marketplace access) + Personal Access Token (read-only fallback)
- **LOC:** `loc.gov` JSON API (admin-only view, rate-limited proxy)
- **AI:** `@anthropic-ai/sdk` (search recommendations)
- **Frontend:** Vanilla JS SPA in `web/`, no build step, cache-busted via `?v=YYYYMMDD{letter}` query params

## Build & run

```bash
npm install
npm run build     # tsc → dist/
npm start         # node dist/search-api.js (port 3001)
npm run dev       # tsc --watch
```

## Environment variables

Copy `.env.example` to `.env` and fill in:

| Var | What |
|---|---|
| `APP_DB_URL` | Postgres connection string (Railway provides this) |
| `AUTH_PK` | Clerk publishable key |
| `CLERK_SECRET_KEY` | Clerk secret key (server-side JWT verification) |
| `ADMIN_CLERK_ID` | Clerk user id of the admin — gates `/admin` and the LOC view |
| `ANTHROPIC_API_KEY` | For AI search recommendations |
| `DISCOGS_CONSUMER_KEY` | OAuth 1.0a consumer key (from discogs.com/settings/developers) |
| `DISCOGS_CONSUMER_SECRET` | OAuth 1.0a consumer secret |

Without these the server will boot in degraded mode — AI search, admin dashboard, and Discogs OAuth will be disabled.

## Project layout

```
src/                TypeScript source (server + DB)
  search-api.ts       Express app, all routes
  db.ts               Postgres helpers and migrations
  discogs-client.ts   OAuth 1.0a client wrapper
  classical-synonyms.ts  Search query expansion lookup
dist/               Compiled JS — committed so Railway runs from here
web/                Static frontend served by Express
  index.html          SPA shell
  admin.html          Admin dashboard (gated by ADMIN_CLERK_ID)
  shared.js           Header/footer, Clerk bootstrap, apiFetch wrapper
  app.js              SPA bootstrap and routing
  collection.js       switchView + collection/wantlist/inventory tabs
  search.js           Discogs search UI
  modal.js            Record detail modal
  loc.js              Library of Congress view (admin-only)
  account.js          Account view
  orders.js           Seller orders
  inventory-editor.js Marketplace listing CRUD
  utils.js            Misc helpers
  style.css           All styles
.claude/
  launch.json         Preview server config for Claude Code
CLAUDE.md           Session-handoff doc for new Claude Code sessions
```

## See also

`CLAUDE.md` has the live project context used when resuming work with Claude Code, including the current cache-bust version, deployment notes, known issues, and coding conventions.
