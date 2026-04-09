# SeaDisco — project context for Claude Code

> **Repo nickname:** "SeaDisco" or "discogs-mcp-server" — same project. The repo name is historical (it started as a Discogs MCP server) but the active product is **SeaDisco**, an invite-only music-discovery web app.

## Where things live

- **Repo root:** `C:\Users\KJ-NoJesteringStudio\GitHub\discogs-mcp-server` (Windows path; bash sees it as `/c/Users/KJ-NoJesteringStudio/GitHub/discogs-mcp-server`)
- **Git remote:** `https://github.com/Jstring-1/discogs-mcp-server` (branch: `main`)
- **Deployment:** Railway, auto-deploys from `main`
- **Production URL:** https://seadisco.com

## Stack

- **Backend:** Node 20+ / Express 4 / TypeScript (ESM, `"type": "module"`)
- **DB:** PostgreSQL via `pg` pool (Railway)
- **Auth:** Clerk (JWT bearer tokens, `openSignIn()` modal, `mountWaitlist()` / `mountSignUp()`)
- **Discogs:** OAuth 1.0a (full marketplace access) + Personal Access Token (read-only fallback)
- **Frontend:** Vanilla JS SPA, no build step on the web/ side. Scripts loaded with `defer` and cache-busted via `?v=YYYYMMDD{letter}`
- **AI:** `@anthropic-ai/sdk` for AI search recommendations

## Directory layout

```
src/                TypeScript source (server + DB)
  search-api.ts       Express app, all routes, the main entry point
  db.ts               PostgreSQL helpers
  *.ts                Misc helpers (oauth, sync, ai, etc.)
dist/               Compiled JS (committed — Railway runs from here)
web/                Static frontend served by Express
  index.html          SPA shell (search, records, info, privacy, terms, account)
  admin.html          Admin dashboard (gated by ADMIN_CLERK_ID)
  shared.js           Header/footer renderers, Clerk bootstrap, openSignInModal
  app.js              SPA bootstrap, applyAuthState, splash visibility
  collection.js       switchView, record tabs, showRecordSignIn
  search.js           Discogs search UI
  account.js          Account view
  orders.js           Seller orders
  modal.js            Record detail modal
  inventory-editor.js Marketplace listing CRUD
  utils.js            Misc helpers
  style.css           All styles
scripts/            Maintenance scripts (rare use)
.claude/
  launch.json         Preview server config — name: "SeaDisco Web Server", port 3001
```

## Build & run

```bash
npm run build       # tsc → dist/
npm start           # node dist/search-api.js (port 3001 by default)
npm run dev         # tsc --watch
```

For Claude Code preview, use `preview_start` with name `"SeaDisco Web Server"` (defined in `.claude/launch.json`). Server runs on port 3001.

## Conventions

- **Cache-busting:** Every CSS/JS asset in `web/index.html` and `web/admin.html` is loaded with `?v=YYYYMMDD{letter}`. The build version is also displayed under the logo via `SITE_VERSION` in `web/shared.js` (`renderSharedHeader`). When you change a frontend file, bump the version in **all three places**: `index.html`, `admin.html`, and `SITE_VERSION` in `shared.js`. Use a one-liner like:
  ```bash
  cd /c/Users/KJ-NoJesteringStudio/GitHub/discogs-mcp-server && sed -i 's/v=20260409d/v=20260409e/g' web/index.html web/admin.html
  ```
  Then edit `SITE_VERSION` in `shared.js` to match.
- **Defer scripts:** All `<script>` tags in `index.html` and `admin.html` use `defer`. Inline `<script>renderSharedHeader()</script>` calls must be wrapped in `document.addEventListener("DOMContentLoaded", ...)` because deferred scripts haven't executed yet during HTML parse.
- **Commit style:** Imperative subject under 70 chars. Body explains *why*, not *what*. Always include the `Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>` trailer.
- **Push behavior:** Don't push without an explicit "push" from the user. After committing, wait for confirmation.
- **CRLF warnings on commit:** Expected — Windows checkout, harmless. Don't try to fix.

## Auth model (invite-only)

The site is **invite-only**:

- Signed-out users see a splash (`#splash-section` in `index.html`) with a Clerk waitlist widget mounted in `#splash-waitlist-mount`.
- The splash also has an "Already approved? Sign in" link that opens Clerk's sign-in modal **in-page** via `openSignInModal()` in `web/shared.js`. We do NOT use Clerk's hosted sign-in pages — every entry point goes through the modal so users never leave seadisco.com.
- Navbar record tabs (Collection/Wantlist/Lists/Inventory/Favorites) and the Account tab also call `openSignInModal()` when clicked while signed-out.
- Footer record links and the footer Account link were updated (commit `605a5e9`) to mirror the navbar — they also pop the modal when signed-out.
- Clerk's built-in "Have an account? Sign in" footer inside the mounted waitlist widget is hidden via a CSS rule in `web/style.css`:
  ```css
  #splash-waitlist-mount .cl-footer,
  #splash-waitlist-mount .cl-footerAction { display: none !important; }
  ```
  Clerk's `appearance.elements` API uses inline styles that lose specificity battles, so a real stylesheet rule with `!important` is the reliable approach.
- Admin access is gated server-side by `ADMIN_CLERK_ID` env var. The admin link in the footer is hidden until `/api/me` confirms the current user is admin.

## Known issues / pending work

### Discogs OAuth features — partially implemented
An audit on 2026-04-09 found these gaps. Backend exists but frontend UI is missing:

| Feature | Backend | Frontend UI | Status |
|---|---|---|---|
| Ratings (1–5 stars) | ✓ | ✓ | **DONE** |
| Collection folders CRUD | ✓ | ✓ | **DONE** |
| Collection custom fields | ✓ (`POST /api/user/collection/:id/fields/:fieldId`) | ✗ | Backend-only — no UI to view/edit field values |
| Collection notes (free-text) | ✓ (filter UI exists) | ✗ (edit UI missing) | Users can filter by notes but can't edit them in the modal |
| Wantlist notes | ✓ (add accepts `notes` param) | ✗ (frontend never sends, no edit endpoint) | Backend half-built |

User said "I will consider the rest" on 2026-04-09 — these are deferred until they choose to prioritize.

## User preferences

- **Terse responses.** Lead with the answer or action. Skip preamble. No trailing summaries unless asked.
- **Ask before push.** Always wait for explicit "push" before `git push`.
- **Don't auto-fix beyond scope.** Bug fixes don't need surrounding cleanup. Stick to what was asked.
- **No emojis** unless explicitly requested.
- **No new docs/READMEs** unless explicitly requested. (This CLAUDE.md was explicitly requested as a session-handoff doc.)

## Recent commits worth knowing about

```
605a5e9  Route footer record/account links through sign-in modal when signed out
a8f9753  Fix splash sign-in leaks, prune orphan files, defer scripts
350c724  Bump cache-bust to 20260409a
3895507  Treat Discogs 404 on inventory delete as already-gone
f5d71dd  Use Clerk modal for sign-in, center splash widget, tidy copy
c83fa73  Lock down site to invite-only with Clerk waitlist splash
```

The lockdown work (`c83fa73` onward) is the most recent feature push — it converted the site from open-access to invite-only.

## Verification

Always verify previewable changes with `preview_start` → `preview_snapshot` / `preview_screenshot`. Don't ask the user to test manually. The launch config is already in `.claude/launch.json` — name: `"SeaDisco Web Server"`, port `3001`.

For backend changes, run `npm run build` first so `dist/` is current.
