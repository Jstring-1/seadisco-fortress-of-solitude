# SeaDisco — project context for Claude Code

> **Repo nickname:** "SeaDisco" or "seadisco-fortress-of-solitude" — same project. The repo was renamed from `discogs-mcp-server` after it grew past its MCP-server origins. The active product is **SeaDisco**, a music-discovery web app for vinyl collectors. Local working directory still uses the legacy name.

## Where things live

- **Repo root:** `C:\Users\KJ-NoJesteringStudio\GitHub\discogs-mcp-server` (Windows path; bash sees it as `/c/Users/KJ-NoJesteringStudio/GitHub/discogs-mcp-server`). Local directory still uses the legacy name; remote is `seadisco-fortress-of-solitude`.
- **Git remote:** `https://github.com/Jstring-1/seadisco-fortress-of-solitude` (branch: `main`)
- **Deployment:** Railway, auto-deploys from `main`
- **Production URL:** https://seadisco.com

## Stack

- **Backend:** Node 20+ / Express 4 / TypeScript (ESM, `"type": "module"`)
- **DB:** PostgreSQL via `pg` pool (Railway)
- **Auth:** Clerk (JWT bearer tokens via `getClerkUserId`, modal sign-in via `openSignInModal()`, waitlist via `openSignUpModal()`)
- **Discogs:** OAuth 1.0a — read + write (collection, wantlist, marketplace inventory, lists, ratings, folders, orders)
- **Frontend:** Vanilla JS SPA, no build step on the web/ side. Scripts loaded with `defer` and cache-busted via `?v=YYYYMMDD.HHMM`
- **AI:** `@anthropic-ai/sdk` for AI search recommendations
- **Media:** YouTube IFrame Player + LOC `<audio>` element + Archive.org streams. Cross-source unified play queue.

## Directory layout

```
src/                  TypeScript source (server + DB)
  search-api.ts         Express app, all routes — main entry point (~7000 lines)
  db.ts                 PostgreSQL helpers (~4500 lines)
  discogs-client.ts     OAuth signing + Discogs API wrapper
  blues-db.ts           Curated Blues artist catalog (admin-managed)
  ...
dist/                 Compiled JS (committed — Railway runs from here)
web/                  Static frontend served by Express
  index.html            SPA shell (search / records / companion views / popups)
  admin.html            Admin dashboard (gated by ADMIN_CLERK_ID)
  shared.js             Header/footer renderers, Clerk bootstrap, body-scroll
                        lock, card-mode toggle, card enrichment, lookup popup
  app.js                SPA bootstrap, applyAuthState, splash visibility
  collection.js         switchView + record tabs (collection / wantlist /
                        favorites / inventory / lists)
  search.js             Discogs search UI + renderCard + home-strip tabs
  modal.js              Album / version / series / lightbox popups,
                        tracklist, entity-lookup popup, openVideo handler
  account.js            Account view (Discogs OAuth connect, sync, prefs)
  orders.js             Seller orders panel
  inventory-editor.js   Marketplace listing CRUD
  loc.js                LOC view (search + Saved tab + audio playback)
  archive.js            Archive.org view (collection browse + audio)
  youtube.js            /?v=youtube view + suggest popup + paste-URL form
  queue.js              Cross-source play queue (LOC + YT) — server +
                        localStorage backed
  utils.js              Misc helpers
  style.css             All styles (~9300 lines)
  sw.js                 Service worker (offline caching)
  offline.js            Offline-mode boot path
scripts/              Maintenance scripts (rare use)
```

## Build & run

```bash
npm run build       # tsc → dist/
npm start           # node dist/search-api.js (port 3001 by default)
npm run dev         # tsc --watch
```

For Claude Code preview, use `preview_start` with name `"SeaDisco Web Server"` (defined in `.claude/launch.json`). Server runs on port 3001.

## Cache-bust version

Every CSS/JS asset in `index.html` and `admin.html` is loaded with `?v=YYYYMMDD.HHMM`. The build version is also displayed under the logo via `SITE_VERSION` in `web/shared.js`. **Bump in all four files** when changing frontend:

```bash
cd /c/Users/KJ-NoJesteringStudio/GitHub/discogs-mcp-server && \
  sed -i 's/20260507\.1225/20260507.1300/g' web/index.html web/sw.js web/admin.html web/shared.js
```

A PreToolUse hook auto-bumps the cache-bust on tool runs that touch `web/`, scooping the change into a separate `chore: bump cache-bust` commit. Expected behavior — don't fight it.

The current build is the trailing pattern in those files. Use `date "+%Y%m%d.%H%M"` for the new value (always increases monotonically).

## Auth tiers

The app has four access tiers, all gated server-side first then mirrored in client UI:

1. **Anon (signed-out)** — see `body.sd-anon` class.
   - Splash banner at top (slim, "Join the waitlist" / "Sign in").
   - Search form visible but `/search` requires sign-in (returns 401 / "Sign in to search Discogs" empty state).
   - Home strip shows only **Feed** tab (random sample from `release_cache`, public). Other tabs (Recent / Suggestions / Submitted) are visible but greyed out (`rr-tab-disabled`); click pops sign-in modal.
   - LOC / Wikipedia / Archive views fully open (per-IP throttled). Save buttons hidden via CSS (`body.sd-anon`). Entity-lookup popup LOC/Archive/Wiki tabs also open for anons.
   - YouTube view + submission flow: blocked.
   - Queue: works, persisted to localStorage (`sd_anon_queue_v1`, capped 200 items).

2. **Signed-in user** — `window._clerk?.user` truthy.
   - Full UI minus admin / demo gates.
   - Records tabs (collection / wantlist / favorites / inventory / lists) enabled regardless of Discogs OAuth status. Empty state offers "Connect your Discogs account" when OAuth missing.
   - YouTube submission popup + paste-URL form open to all signed-in users. **Auto-search (search.list) suspended** for non-admin/demo pending Google quota approval — they paste URLs manually instead. Per-track 🎵 button + album-level missing-tracks search button visible for any signed-in user.

3. **Demo allowlist** — Clerk user IDs in `DEMO_CLERK_IDS` env-var (comma-separated).
   - Used for the Google API Quota review demo account.
   - Bypasses `MAX_USERS` cap on signup + hibernated-user reactivation.
   - Same UX as admin minus admin-only mutations (✕ delete on track-yt overrides, etc.).
   - YT view + submission flow + paste-URL form open.
   - `getDiscogsForRequest` falls back to admin's OAuth credentials for demo users without their own (read-only flows: search, release/master fetches).
   - Server check: `isDemoUser(userId)`. Client check: `window._sdIsDemo` (from `/api/me`).

4. **Admin** — single user matching `ADMIN_CLERK_ID` env-var.
   - All of demo's privileges plus admin-only routes (`/admin`, `/api/admin/*`, ✕ delete buttons, blues-DB curation).
   - Server check: `requireAdmin(req, res)`. Client check: `window._isAdmin`.

Helper: `_sdHasYtAccess()` returns true for admin OR demo OR (signed-in && `YT_OPEN_TO_USERS`). Used to gate **auto-search** specifically. The submission popup itself + paste-URL form are gated on `_clerk?.user`. Server: `requireYtAccess` enforces auto-search; `requireUser` is enough for video-info/suggest endpoints.

## Major feature areas

### Cross-source play queue (`web/queue.js`)
- One queue spans LOC audio + YouTube videos.
- Server-backed for signed-in users (`play_queue` table + `/api/user/play-queue`); localStorage-backed for anons.
- `_queueOnExternalPlay` hook fires from `_locPlay` and `openVideo` to keep the bar in sync.
- Mini-player at bottom of page: persistent across navigation, shows the playing track. Dismissal persists via `localStorage.sd_player_bar_hidden`; cleared on next `openVideo` / `_locPlay`.
- "Disc icon" reopens the album for the playing track via `_playerReleaseType` / `_playerReleaseId` globals (with fallback to current queue entry's data).
- Repeat modes: off / all / one — **single source of truth** is `_queueGetRepeat()` / `_queueCycleRepeat()` (persisted to `localStorage.sd_queue_repeat`). Modal's `toggleRepeat` and mini-bar repeat button both delegate; YT player's `onVideoEnded` reads only this. Don't reintroduce a separate `_ytRepeat`.
- Consume-on-play toggle: ✂ button removes each track from the queue after playing.

### YouTube submission flow (`web/youtube.js` + `modal.js`)
- Crowd-sourced YT video matches for tracks Discogs is missing audio for.
- Album-level "🎵 N missing" link in tracklist heading → opens YT search popup. Admin/demo: auto-fires search and shows per-result dropdowns. Other signed-in users: paste-URL flow only ("Search YouTube directly and paste any video URL"), no search.list spend.
- Search query: `"<artist>" "<album>"` plus `-live` (suppressed when album title contains "live"). No `album` keyword. `maxResults=50` per call.
- Per-track 🎵 button (next to ▶ ＋) opens the popup focused on that track.
- Paste-URL form: paste YT URL/ID, pick track, stages without `search.list` (1-unit `videos.list` for title only).
- "Full album" pseudo-track at sentinel position `"ALBUM"` — a single video for the whole album. Renders as the first row, **excluded** from missing-tracks count and from in-popup unavailable pruning.
- Unavailable tracks (YT video flagged unavailable) are removed from the popup's tracklist immediately and added to in-session `window._sdYtUnavailable` Set.
- Override storage: `track_youtube_overrides` table (master/release scope, position keyed). First-submission-wins via `ON CONFLICT DO NOTHING`.
- Admin-only delete via ✕ next to override badges.

### YouTube API quota management
- DB-backed cache (`youtube_search_cache` table, 7-day TTL) survives Railway restarts.
- In-memory cache layered on top (faster, capped at 500 entries).
- Normalized cache keys: lowercase + collapsed whitespace.
- In-flight request coalescing: duplicate same-key requests share the response.
- Per-user daily cap (200 search.list calls/day) for signed-in users.
- Project soft-cap (~9000/day) before hitting Google's 10K hard limit.
- Duration enrichment: 1-unit `videos.list?part=contentDetails` per search, batched up to 50 IDs. Backfilled lazily on stale cached bodies.
- Quota dashboard: `/api/admin/youtube-quota`.

### Home-strip tabs (`web/search.js`)
Four modes — `Recent / Suggestions / Submitted / Feed`:
- **Recent** — albums viewed locally (localStorage history). `loadRandomRecords` uses a sequence counter (`_randomLoadSeq` + `_stillCurrent()` checks at every await) so a stale tab's results can't overwrite the current tab's render on fast switches.
- **Suggestions** — per-user background-generated feed. Hourly job (`_runPersonalSuggestionsForAllUsers`) walks each user's library taste tuples, queries Discogs for matches, saves up to 1000. The Suggestions filter scopes server-side across **all** of the user's stored suggestions (capped 1000), not just the visible page. Dismiss → server-side banish (`user_suggestion_dismissals`).
- **Submitted** — current user's YT submissions (distinct by master/release).
- **Feed** — random sample from `release_cache` (public, anon-accessible). Load More fetches another random page with already-shown rows excluded.

URL state: `?strip=recent|suggestions|submitted|feed` reflected via `replaceState`. The `?strip=submitted` deep-link is parked on `window._sdHomeStripPendingPin` until auth resolves (admin/demo flag), then `applyAuthState` replays it. Logo click resets `_sdHomeStripMode` so the Suggestions view doesn't stick after navigating home.

### Card display modes (`renderCard` in search.js)
- **Compact** (default) — 160px grid, line-clamped title + artist, click anywhere on card opens album modal.
- **Wide** — `body.card-mode-wide`, 360px grid, cover + body side-by-side, full text, image strip + tracklist + Full Album row + per-track ▶ ＋ buttons. Click only the cover image to open modal; clicks elsewhere fall to internal handlers (entity lookup, play, queue).
- Toggle: `▦ Wide` button on search controls bar. Persisted in `sd_card_mode` localStorage. Site-wide.

### Card enrichment (`_sdEnrichWideCards` in shared.js)
- Every release/master card across the site carries `data-card-id` / `data-card-type` attrs.
- A debounced helper (250ms) batches visible cards' (id, type) pairs and POSTs `/api/cards/enrich`.
- Server returns slim `{ images, tracklist, videos, overrides }` from `release_cache` + `track_youtube_overrides`.
- Client patches matching cards in place: image strip, inline tracklist, per-track play/queue buttons.
- Runs in **both** compact and wide modes (data is fetched but compact CSS hides the strip + tracklist).
- MutationObserver on `<body>` re-fires the sweep whenever new cards appear (Load More, view switches, page changes).
- Cache-missed cards (no `release_cache` row) get a ↻ Re-fetch button via `_sdMarkSparseCards` — click forces `/release|/master/:id?nocache=1` and patches the card with fresh data.

### Cache architecture
- `release_cache` table: 1-year TTL on read (`_RELEASE_CACHE_TTL_S` etc. in search-api.ts). Rows live indefinitely until explicit prune.
- `youtube_search_cache` table: 7-day TTL on read.
- Per-route in-memory caches in front of DB (LOC, Archive, YT video-info, etc.).
- All read paths follow: in-memory → DB → upstream.

### Companion views
- **LOC** (`web/loc.js`) — Library of Congress public-domain audio. Anon-throttled at 5/min/IP.
- **Archive** (`web/archive.js`) — archive.org live tapes. Anon-throttled at 5/min/IP.
- **Wikipedia** (popup-only via `modal.js`) — anon at 50/hr/IP.
- **YouTube** (`web/youtube.js`) — admin/demo only. Search + Saved tab + per-track + album-mode submit popups + paste-URL form.

All client paths handle 429 with friendly per-source messages (loc.js, archive.js, modal.js).

### Body scroll lock
- `_sdLockBodyScroll(id)` / `_sdUnlockBodyScroll(id)` in shared.js — counter-based, nested-popup-aware.
- Wired into every overlay: modal, version, series, lightbox, youtube-popup, inventory-editor, folder-manager, alts-popup.
- CSS: `body.modal-open { overflow: hidden; overscroll-behavior: none }`.

### Unified entity-lookup popup
- `entityLookupLinkHtml(scope, label, opts)` in shared.js builds a `<span role="button">` (was `<a>` until cards started nesting it; HTML5 auto-closes outer `<a>` on nested anchor). Pass `opts.entityId` (Discogs artist/label ID) when known — it's stashed on `data-lk-id` and forwarded to `_lookupSearchSeaDisco(scope, label, entityId)`.
- Click opens a small popup with: SeaDisco search / Collection / Wikipedia / LOC / YouTube / Copy to clipboard.
- Used in album modal track titles, artist names, label names, and wide-card artist + title.
- In compact-mode cards, the lookup link is `pointer-events: none` so the outer card click reaches the modal-open handler.

### Disambig-aware artist/label search via Discogs ID
Discogs `/database/search?artist=` does substring matching and chokes on `(N)` disambiguators in parens — so e.g. searching "John Lee (26)" used to surface only John Lee Hooker.
- When the lookup popup knows the entity ID (plumbed via `entityLookupLinkHtml({ entityId })`), it pins `window.currentArtistId` / `window.currentLabelId` and `doSearch` routes through `/artist-releases?id=N` (or `/label-releases?id=N`) instead of `/search`.
- Server endpoints (`src/search-api.ts`) reshape `getArtistReleases` / `getLabelReleases` into the same shape `/search` returns. Artist endpoint accepts `type=master|release|all`, `sort=year|title|format`, `sort_order`, `role=main|all`. Default popup-driven sort remains `year:asc`, masters+ collapsed.
- `/search` artist path also strips a trailing `(N)`, queries with the base name, and post-filters results to titles starting with `<base> <suffix> -` so direct text searches degrade gracefully. `/artist-bio` slow path requires strict exact-disambig match (no startsWith fallback) to avoid serving the wrong artist's bio.
- Total-count pagination: clamp `pagination.items` with `Math.max(rawReported, items.length)` — Discogs sometimes underreports.

## Conventions

- **Defer scripts:** All `<script>` tags use `defer`. Inline render calls must be wrapped in a `DOMContentLoaded` listener.
- **TDZ trap:** When adding `let`/`const` declarations to shared.js, watch boot-time call ordering. The pattern that bit us: a top-level `if (document.readyState !== "loading") { fn() }` block calling a function that references a `let` declared further down. Move boot blocks AFTER all the helpers they call.
- **Nested anchors:** Don't put `<a>` inside the card's outer `<a>`. Use `<span role="button" tabindex="0">` with onclick. HTML5 auto-closes the outer anchor on nested-anchor parse, splitting cards across grid cells.
- **Commit style:** Imperative subject under 70 chars. Body explains *why*. Always include `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>` trailer.
- **Push behavior:** Don't push without explicit "push" from the user. Wait for confirmation.
- **CRLF warnings on commit:** Expected — Windows checkout, harmless. Don't try to fix.
- **No emojis** in code/comments unless the user explicitly asks.
- **No new READMEs / docs** unless explicitly requested.

## Env vars (Railway)

| Var | Purpose |
|---|---|
| `DATABASE_URL` | Postgres connection |
| `AUTH_PK` | Clerk publishable key (used to derive issuer URL) |
| `ADMIN_CLERK_ID` | Single admin user — gates `/admin` + admin routes |
| `DEMO_CLERK_IDS` | Comma-separated demo user IDs (Google API quota review) |
| `YT_OPEN_TO_USERS` | Set to `1` to open YT features to all signed-in users (default off, admin+demo only) |
| `DISCOGS_CONSUMER_KEY` / `DISCOGS_CONSUMER_SECRET` | OAuth 1.0a app credentials |
| `YOUTUBE_API_KEY` | YouTube Data API v3 (Application restrictions: None) |
| `ANTHROPIC_API_KEY` | Claude SDK (AI search) |
| `MAX_USERS` | User cap (default 25) — admin + demo bypass |

## User preferences

- **Terse responses.** Lead with the answer or action. Skip preamble. No trailing summaries unless asked.
- **Ask before push.** Always wait for explicit "push".
- **Don't expand scope.** Bug fixes don't get surrounding cleanup. Stick to what was asked.
- **No emojis** in code unless requested.
- **No unsolicited docs.** This file was explicitly requested.

## Recent feature areas (2026-04 → 2026-05)

Major work since the original CLAUDE.md (2026-04-09):

- Companion views (LOC, Archive, Wikipedia, YouTube) + saves
- Cross-source play queue + persistent mini-player (with localStorage-persisted dismissal)
- YouTube submission flow + Full Album pseudo-track + paste-URL form, opened to all signed-in users (auto-search still admin/demo only)
- YouTube quota guards + DB-backed cache + admin quota dashboard, `maxResults=50`, `-live` filter
- Personal suggestions (hourly background job, per-user, server-scoped filter)
- Home-strip Recent / Suggestions / Submitted / Feed (with stale-load cancellation, post-auth pin replay)
- Anon Mode (cache browse, LOC/Wiki/Archive fully open including in lookup popup, no Discogs)
- Demo allowlist for Google API quota review
- Wide-card display mode + sitewide enrichment + sparse refresh button (no track duration in wide cards; head label always right-aligned)
- Body scroll lock infrastructure
- Album image strip viewer + inline tracklist + per-track play/queue/🎵 buttons
- 1-year `release_cache` TTL
- Disambig-aware artist/label search via `/artist-releases` and `/label-releases` ID endpoints; bio strict-match for `(N)` artists
- Repeat-mode unification (single source via `_queueGetRepeat`, no more dual-state drift)
- Unavailable-track immediate prune + Full Album excluded from missing count
- Total-items clamp (`Math.max(rawReported, items.length)`) for accurate result counts
- Various bug fixes: queue delete races, Full Album disc icon, navbar grey-out for signed-in, TDZ blanking the navbar, nested-anchor card splits

## Verification

For frontend changes, use `preview_start` (config in `.claude/launch.json` — name `"SeaDisco Web Server"`, port `3001`) and visually verify with `preview_screenshot` / `preview_snapshot`. Don't ask the user to test manually unless backend-only.

For backend changes, run `npm run build` first so `dist/` is current — Railway runs from `dist/`.
