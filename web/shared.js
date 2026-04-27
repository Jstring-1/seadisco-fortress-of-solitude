// ── Shared utilities for all pages (index, account, admin) ──────────────

// ── Relative time formatting ─────────────────────────────────────────────
// Single helper covers both "syncedAt" displays (use fallback "never")
// and generic "ago" labels (default em-dash). Accepts ms-numbers or
// any Date-parsable value. Unifies what used to be fmtTime + fmtRelativeTime.
function fmtTime(ts, fallback = "\u2014") {
  if (!ts) return fallback;
  const ms = Date.now() - (typeof ts === "number" ? ts : new Date(ts).getTime());
  const mins = Math.floor(ms / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months}mo ago`;
  return `${Math.floor(days / 365)}y ago`;
}
// Back-compat alias \u2014 admin UI calls fmtRelativeTime in a few places.
const fmtRelativeTime = fmtTime;

// \u2500\u2500 localStorage JSON helpers \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
// Parse a JSON value out of localStorage, falling back to `defaultVal`
// on missing key, parse errors, or sandboxed/disabled storage. Replaces
// the repeated `JSON.parse(localStorage.getItem(key) || "{}")` pattern
// in 4+ files. (Audit #6.)
function getStorageJSON(key, defaultVal) {
  try {
    const raw = localStorage.getItem(key);
    if (raw == null) return defaultVal;
    const parsed = JSON.parse(raw);
    return parsed == null ? defaultVal : parsed;
  } catch {
    return defaultVal;
  }
}
// Set a JSON value into localStorage, swallowing quota/disabled errors.
function setStorageJSON(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); return true; }
  catch { return false; }
}

// ── Mobile nav toggle ────────────────────────────────────────────────────
function toggleMobileNav() {
  document.getElementById("main-nav-tabs")?.classList.toggle("mobile-open");
}
document.addEventListener("click", e => {
  if (!e.target.closest("#main-nav-tabs") && !e.target.closest("#nav-hamburger")) {
    document.getElementById("main-nav-tabs")?.classList.remove("mobile-open");
  }
});

// ── Search param normalization ───────────────────────────────────────────
function normP(raw) {
  const m = { artist:"a", release_title:"r", label:"l", year:"y", genre:"g", style:"s", format:"f", type:"t", sort:"o" };
  const o = {};
  for (const [k, v] of Object.entries(raw)) { if (v) o[m[k] ?? k] = v; }
  return o;
}

function searchLabel(raw) {
  const p = normP(raw);
  const parts = [];
  if (p.q && (!p.a || p.q.toLowerCase() !== p.a.toLowerCase())) parts.push(p.q);
  if (p.a) parts.push(p.a);
  if (p.r) parts.push(p.r);
  if (p.l) parts.push(`${p.l} label`);
  if (p.g) parts.push(p.g);
  if (p.s) parts.push(p.s);
  if (p.f && p.f !== "Vinyl") parts.push(p.f);
  if (p.y) parts.push(p.y);
  return parts.join(" \u00b7 ") || "Search";
}

function paramsToUrl(raw) {
  const p = normP(raw);
  const u = new URLSearchParams();
  if (p.q) u.set("q",  p.q);
  if (p.a) u.set("ar", p.a);
  if (p.r) u.set("re", p.r);
  if (p.y) u.set("yr", p.y);
  if (p.l) u.set("lb", p.l);
  if (p.g) u.set("gn", p.g);
  if (p.s) u.set("st", p.s);
  if (p.f) u.set("fm", p.f);
  if (p.t) u.set("rt", p.t);
  if (p.o) u.set("sr", p.o);
  if (p.b) u.set("b",  p.b);
  return "/?" + u.toString();
}

// ── API base URL (empty for same-origin) ────────────────────────────────
const API = "";

// ── Auth-aware fetch wrapper ────────────────────────────────────────────
async function apiFetch(url, options = {}) {
  const headers = { ...(options.headers ?? {}) };
  try {
    const t = await getSessionToken();
    if (t) headers["Authorization"] = `Bearer ${t}`;
  } catch { /* not signed in */ }
  const res = await fetch(url, { ...options, headers });
  // On 401, force-refresh the token once and retry
  if (res.status === 401 && window._clerk?.session) {
    try {
      _cachedToken = null; _cachedTokenAt = 0;
      const t2 = await getSessionToken();
      if (t2) {
        headers["Authorization"] = `Bearer ${t2}`;
        return fetch(url, { ...options, headers });
      }
    } catch { /* give up */ }
  }
  return res;
}

// ── HTML escape ─────────────────────────────────────────────────────────
function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// ── Shared Clerk bootstrap ───────────────────────────────────────────────
// Loads Clerk JS and returns the Clerk instance. Pages handle post-auth UI.

// Cached token + timestamp for getSessionToken()
let _cachedToken = null;
let _cachedTokenAt = 0;

// Reliable token getter — uses cache for <50s, otherwise refreshes.
// All pages should call this instead of clerk.session?.getToken() directly.
async function getSessionToken() {
  const c = window._clerk || window.Clerk;
  if (!c?.user || !c?.session) return null;

  // Clerk JWTs expire after ~60s; refresh if cache is >50s old
  if (_cachedToken && (Date.now() - _cachedTokenAt) < 50000) {
    return _cachedToken;
  }

  // Try to get a fresh token, retry up to 3 times with 300ms gaps
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const t = await c.session.getToken();
      if (t) {
        _cachedToken = t;
        _cachedTokenAt = Date.now();
        return t;
      }
    } catch { /* ignore */ }
    if (attempt < 2) await new Promise(r => setTimeout(r, 300));
  }
  // Return stale cache as last resort (server will reject if truly expired)
  return _cachedToken;
}

async function loadClerkInstance() {
  // Fast path: the server inlines a preloaded <script async> tag for
  // clerk-js in <head> (via CLERK_SCRIPT_INJECT), so on most page loads
  // window.Clerk is already defined or will appear within a few ms.
  // Skip the /api/config round-trip + dynamic <script> creation.
  let c = window.Clerk;
  if (!c) {
    // Wait briefly for the preloaded async script to finish downloading
    for (let i = 0; i < 40 && !window.Clerk; i++) {
      await new Promise(r => setTimeout(r, 25));
    }
    c = window.Clerk;
  }

  // Fallback: no preloaded script (e.g. dev, or template injection missing) —
  // load Clerk the old dynamic way
  if (!c) {
    let pk = "";
    const cached = getStorageJSON("_clerkCfg", null);
    if (cached?.pk && (Date.now() - (cached.ts || 0)) < 3600000) pk = cached.pk;
    if (!pk) {
      const cfg = await fetch("/api/config").then(r => r.json()).catch(() => ({}));
      pk = cfg.clerkPublishableKey || "";
      if (pk) setStorageJSON("_clerkCfg", { pk, ts: Date.now() });
    }
    if (!pk) return null;

    const frontendApi = atob(pk.replace(/^pk_(test|live)_/, "")).replace(/\$$/, "");
    await new Promise((resolve, reject) => {
      const s = document.createElement("script");
      s.src = `https://${frontendApi}/npm/@clerk/clerk-js@latest/dist/clerk.browser.js`;
      s.setAttribute("data-clerk-publishable-key", pk);
      s.setAttribute("crossorigin", "anonymous");
      s.onload = resolve; s.onerror = reject;
      document.head.appendChild(s);
    });

    // Poll for window.Clerk to be defined (script may take time to initialize)
    c = await new Promise((resolve) => {
      if (window.Clerk) { resolve(window.Clerk); return; }
      let tries = 0;
      const iv = setInterval(() => {
        tries++;
        if (window.Clerk) { clearInterval(iv); resolve(window.Clerk); }
        else if (tries > 60) { clearInterval(iv); resolve(null); } // 3s timeout
      }, 50);
    });
    if (!c) return null;
  }

  // Pass localization here — clerk-js applies it globally to every widget
  // mounted afterward. Per-component `localization` on mountSignUp /
  // openSignIn is silently ignored by the vanilla-JS SDK.
  await c.load({ localization: SEADISCO_CLERK_LOCALIZATION });

  // After c.load() resolves, Clerk has either hydrated the session or
  // confirmed there isn't one. Only poll briefly if the state is still
  // ambiguous (user === undefined). Previously we polled up to 3s on every
  // load, which dominated page TTI — drop that to ~500ms max.
  if (c.user === null) {
    // Confirmed signed-out — no need to poll
    return c;
  }
  if (c.user && c.session) {
    // Signed-in — warm the token cache but don't block more than 400ms
    try {
      const t = await Promise.race([
        c.session.getToken(),
        new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 400)),
      ]);
      if (t) { _cachedToken = t; _cachedTokenAt = Date.now(); }
    } catch { /* token will be refreshed on first apiFetch */ }
    return c;
  }
  // Ambiguous state (user === undefined) — short poll, 500ms max
  for (let i = 0; i < 5; i++) {
    await new Promise(r => setTimeout(r, 100));
    if (c.user === null) return c; // signed out
    if (c.user && c.session) {
      try {
        const t = await c.session.getToken();
        if (t) { _cachedToken = t; _cachedTokenAt = Date.now(); }
      } catch {}
      return c;
    }
  }
  return c;
}

// ── Shared auth initializer ─────────────────────────────────────────────
// One function for all pages. Callbacks:
//   onSignedIn(clerk)  — user is authenticated, build page
//   onSignedOut(clerk) — no user, show sign-in / public view
//   onError(msg)       — Clerk failed to load
//   onReady(clerk)     — always fires last (for resolving auth-ready promises)
async function initAuth({ onSignedIn, onSignedOut, onError, onReady } = {}) {
  try {
    const clerk = await loadClerkInstance();
    if (!clerk) {
      onError?.("Auth not configured");
      onReady?.(null);
      return null;
    }
    window._clerk = clerk;

    let _wasSignedIn = !!clerk.user;

    // Resolve onReady IMMEDIATELY once the user state is known, so
    // authReadyPromise unblocks and the page can render. Fire onSignedIn /
    // onSignedOut in the background — their async work (token checks,
    // loadDiscogsIds, etc.) shouldn't gate initial page rendering.
    onReady?.(clerk);

    if (clerk.user) {
      Promise.resolve(onSignedIn?.(clerk)).catch(err => console.error("onSignedIn error:", err));
    } else {
      Promise.resolve(onSignedOut?.(clerk)).catch(err => console.error("onSignedOut error:", err));
    }

    // Listen for GENUINE auth state changes only.
    // Clerk's addListener fires transiently with null user during hydration,
    // which would flash "access denied" on already-authenticated pages.
    // Only fire callbacks when the state actually changes.
    clerk.addListener(({ user }) => {
      if (user && !_wasSignedIn) {
        _wasSignedIn = true;
        onSignedIn?.(clerk);
      } else if (!user && _wasSignedIn) {
        _wasSignedIn = false;
        onSignedOut?.(clerk);
      }
    });

    return clerk;
  } catch (err) {
    onError?.(err.message ?? String(err));
    onReady?.(null);
    return null;
  }
}

// ── Shared Clerk theme + sign-in modal helper ───────────────────────────
// Dark amber theme matching the SeaDisco palette. Used by the splash
// waitlist mount and the openSignInModal() helper so every Clerk widget
// looks consistent.
const SEADISCO_CLERK_APPEARANCE = {
  variables: {
    colorBackground:      "#15120e",
    colorInputBackground: "#0e0c08",
    colorInputText:       "#e8dcc8",
    colorText:            "#e8dcc8",
    colorTextSecondary:   "#a89880",
    colorPrimary:         "#ff6b35",
    colorDanger:          "#e05050",
    colorNeutral:         "#a89880",
    borderRadius:         "6px",
    fontFamily:           "system-ui, -apple-system, sans-serif",
  },
  elements: {
    card:             "background:#15120e; border:1px solid #2e2518; box-shadow:none;",
    headerTitle:      "color:#e8dcc8;",
    headerSubtitle:   "color:#8a7d6b;",
    formFieldLabel:   "color:#8a7d6b;",
    formFieldInput:   "background:#0e0c08; border:1px solid #2e2518; color:#e8dcc8;",
    footerActionLink: "color:#ff6b35;",
    // Note: Clerk's footer is hidden inside #splash-waitlist-mount via a
    // real CSS rule in style.css (.cl-footer display:none). The appearance
    // API's inline style approach doesn't beat Clerk's own CSS specificity.
  },
};

// Public mode — registration is open. Localization just tunes the
// default Clerk copy to SeaDisco wording. Keys match Clerk's default
// structure so any unspecified strings fall back to English defaults.
// Clerk-js applies localization from Clerk.load() — see loadClerkInstance().
const SEADISCO_CLERK_LOCALIZATION = {
  signIn: {
    start: {
      title:      "Sign in to SeaDisco",
      subtitle:   "Welcome back",
      actionText: "Don't have an account?",
      actionLink: "Sign up",
    },
  },
  signUp: {
    start: {
      title:      "Create your SeaDisco account",
      subtitle:   "Sign up to sync your Discogs collection, wantlist, and favorites.",
      actionText: "Already have an account?",
      actionLink: "Sign in",
    },
  },
};

// Open Clerk's sign-in modal overlay (no view change). If the user is
// already signed in, route to the Account view instead. Falls back to
// the legacy account view if Clerk's modal API is unavailable.
async function openSignInModal() {
  try {
    const c = window._clerk || await loadClerkInstance();
    if (!c) {
      // Auth not configured — fall back to account view if SPA, else /account
      if (typeof switchView === "function") switchView("account");
      else location.href = "/?v=account";
      return;
    }
    if (c.user) {
      if (typeof switchView === "function") switchView("account");
      else location.href = "/?v=account";
      return;
    }
    if (typeof c.openSignIn === "function") {
      // Localization is applied globally via Clerk.load() — see
      // loadClerkInstance(). Per-component localization is ignored by the
      // vanilla-JS SDK.
      c.openSignIn({
        appearance: SEADISCO_CLERK_APPEARANCE,
        afterSignInUrl: location.pathname + location.search,
        afterSignUpUrl: location.pathname + location.search,
      });
    } else {
      // Older Clerk build without modal support — fall back
      if (typeof switchView === "function") switchView("account");
      else location.href = "/?v=account";
    }
  } catch (e) {
    console.error("[openSignInModal] failed:", e);
    if (typeof switchView === "function") switchView("account");
    else location.href = "/?v=account";
  }
}

// ── Inline nav icons (line-art vinyl set; uses currentColor) ────────────
// 24×24 viewBox; SVGs have no fixed width/height so the .nav-icon
// container's CSS sizing wins. fill="none" + stroke="currentColor" so
// each theme tints them via the nav tab's color.
const _SD_NAV_ICONS = {
  // Magnifier over a vinyl record
  search: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><circle cx="10" cy="10" r="6.5"/><circle cx="10" cy="10" r="3.2"/><circle cx="10" cy="10" r="0.7" fill="currentColor"/><path d="m15 15 5 5"/></svg>`,
  // Two stacked vinyl records (3/4 view) for Collection
  collection: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><circle cx="10" cy="12" r="7"/><circle cx="10" cy="12" r="2.5"/><circle cx="10" cy="12" r="0.6" fill="currentColor"/><path d="M16 6.5c2.5 1.2 4 3.6 4 6.5s-1.5 5.3-4 6.5"/><path d="M14 5.2c.7-.1 1.4-.2 2-.2"/></svg>`,
  // Vinyl with a small ribbon/banner across the top for Wantlist
  wantlist: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="13" r="6.5"/><circle cx="12" cy="13" r="2.4"/><circle cx="12" cy="13" r="0.6" fill="currentColor"/><path d="M9 3h6v5l-3-2-3 2z"/></svg>`,
  // Small vinyl plus three horizontal lines (track listing) for Lists
  lists: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><circle cx="6" cy="6" r="2.5"/><circle cx="6" cy="6" r="0.6" fill="currentColor"/><path d="M11 6h9"/><path d="M3 12h17"/><path d="M3 18h17"/></svg>`,
  // Crate with vinyl tops poking out for Inventory
  inventory: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="9" r="3.5"/><circle cx="9" cy="9" r="0.6" fill="currentColor"/><circle cx="15" cy="9.5" r="3"/><circle cx="15" cy="9.5" r="0.6" fill="currentColor"/><path d="M3 14h18v6H3z"/><path d="M3 17h18"/></svg>`,
  // Vinyl with a heart in the center for Favorites
  favorites: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="13" r="7"/><path d="M12 16.2c-1-.7-3.2-2-3.2-3.7 0-1 .8-1.8 1.8-1.8.7 0 1.1.4 1.4.8.3-.4.7-.8 1.4-.8 1 0 1.8.8 1.8 1.8 0 1.7-2.2 3-3.2 3.7Z"/></svg>`,
  // Person silhouette with two small vinyl circles as headphones for Account
  account: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><circle cx="6" cy="9" r="2.5"/><circle cx="6" cy="9" r="0.6" fill="currentColor"/><circle cx="18" cy="9" r="2.5"/><circle cx="18" cy="9" r="0.6" fill="currentColor"/><path d="M6 9c0-3.3 2.7-6 6-6s6 2.7 6 6"/><path d="M5 21c1-3.5 4-5 7-5s6 1.5 7 5"/></svg>`,
};

// ── Shared header injection ──────────────────────────────────────────────
function renderSharedHeader(opts) {
  const isSPA = opts?.spa;
  const active = opts?.active || "";
  const hideRecords = opts?.hideRecords;
  // Opt-in icon nav: each tab renders as [icon][hover-label] instead of
  // bare text. Used for the admin page first while we evaluate fit.
  const iconNav = !!opts?.iconNav;

  // Wrap a label in icon+hover-label markup when iconNav is on; otherwise
  // return the bare label text. The icon key matches a _SD_NAV_ICONS slot.
  const labelMarkup = (label, iconKey) => {
    if (!iconNav) return label;
    const icon = _SD_NAV_ICONS[iconKey] || "";
    return `<span class="nav-icon" aria-hidden="true">${icon}</span><span class="nav-label">${label}</span>`;
  };
  const navTabClass = iconNav ? "nav-tab-top icon-nav" : "nav-tab-top";

  // Nav tab helper — single row: Search, record tabs, Account
  const tab = (label, view, iconKey) => {
    if (isSPA) {
      const cls = view === active ? ' active' : '';
      return `<button class="${navTabClass}${cls}" data-view="${view}" onclick="switchView('${view}')" title="${label}">${labelMarkup(label, iconKey)}</button>`;
    }
    const href = view === "search" ? "/" : `/?v=${view}`;
    const activeCls = view === active ? ' active' : '';
    return `<a class="${navTabClass}${activeCls}" href="${href}" title="${label}">${labelMarkup(label, iconKey)}</a>`;
  };

  // Record tab — starts disabled until signed in
  const recTab = (label, rtab, iconKey) => {
    if (isSPA) {
      return `<button class="${navTabClass} nav-rec-disabled" data-rtab="${rtab}" onclick="showRecordSignIn('${rtab}')" title="${label}">${labelMarkup(label, iconKey)}</button>`;
    }
    return `<a class="${navTabClass}" href="/?v=${rtab}" data-rtab="${rtab}" title="${label}">${labelMarkup(label, iconKey)}</a>`;
  };

  // Auth tab removed from the navbar — the footer "Account" link
  // (rendered by renderSharedFooter) covers both signed-in and
  // signed-out paths. applyAuthState in app.js still tries to update
  // a #nav-auth-tab element if present and silently no-ops otherwise,
  // so no follow-up cleanup is required.

  const header = document.getElementById("site-header");
  if (!header) return;
  // Site build/version tag shown as tiny grey text under the logo. Updated
  // whenever the cache-bust version is bumped so the user can eyeball whether
  // they're on the latest build without digging into devtools.
  const SITE_VERSION = "build 20260426ah";
  header.innerHTML = `
    <div class="header-logo-wrap">
      <a href="${isSPA ? 'javascript:void(0)' : '/'}" ${isSPA ? 'onclick="if(typeof goHome===\'function\'){goHome();return false;}"' : ''} class="header-logo text-logo"><span class="logo-hi">SEA</span><span class="logo-lo">rch</span><span class="logo-gap"></span><span class="logo-hi">DISCO</span><span class="logo-lo">gs</span></a>
      <div class="header-version" title="Current build">${SITE_VERSION}</div>
    </div>
    ${isSPA ? '<h1 class="sr-only">SeaDisco — Music Discovery Platform: Search &amp; Collection</h1>' : ''}
    <nav id="main-nav">
      <button id="nav-hamburger" onclick="toggleMobileNav()" aria-label="Open navigation">
        <span></span><span></span><span></span>
      </button>
      <div id="nav-tabs-wrap">
        <div id="main-nav-tabs">
          <div class="nav-row nav-row-top" id="nav-row-records">
            ${tab("Search", "search", "search")}
            ${recTab("Collection", "collection", "collection")}
            ${recTab("Wantlist", "wantlist", "wantlist")}
            ${recTab("Lists", "lists", "lists")}
            ${recTab("Inventory", "inventory", "inventory")}
            ${recTab("Favorites", "favorites", "favorites")}
          </div>
        </div>
      </div>
    </nav>`;

  // On non-SPA pages (account, admin), update auth tab once Clerk resolves
  if (!isSPA) {
    loadClerkInstance().then(c => {
      if (c?.user) {
        const el = document.getElementById("nav-auth-tab");
        if (el) {
          el.title = "Account";
          // Same iconNav-safe pattern as applyAuthState — preserve the
          // SVG markup by updating only the label span.
          const labelSpan = el.querySelector(".nav-label");
          if (labelSpan) labelSpan.textContent = "Account";
          else el.textContent = "Account";
        }
      }
    }).catch(() => {});
  }
}

// ── Shared footer injection ──────────────────────────────────────────────
// Build a /?v=… href that preserves the user's current query params so a
// click on (say) "Wikipedia" while a search query is on the URL keeps
// the q=… intact. Drops view-local transient params that don't make
// sense in the new view (kept in sync with VIEW_LOCAL_PARAMS in
// switchView).
function _seaDiscoBuildViewHref(view) {
  let qs;
  try { qs = new URLSearchParams(location.search); } catch { qs = new URLSearchParams(); }
  ["tab", "li", "lp", "nocache"].forEach(k => qs.delete(k));
  if (view === "search") {
    qs.delete("v");
    const tail = qs.toString();
    return tail ? `/?${tail}` : "/";
  }
  qs.set("v", view);
  return `/?${qs.toString()}`;
}

// Walk every footer link tagged with data-sd-view and rewrite its href
// to reflect the CURRENT location.search. Click handlers already work
// correctly in SPA mode (switchView reads location.search fresh), but
// middle-click / right-click → copy-link / Open-in-new-tab use the
// href attribute directly — so we keep that attribute in sync with
// every history change.
function _updateFooterHrefs() {
  const footer = document.querySelector("footer");
  if (!footer) return;
  footer.querySelectorAll("a[data-sd-view]").forEach(a => {
    const v = a.dataset.sdView;
    if (v) a.href = _seaDiscoBuildViewHref(v);
  });
}
// Hook history.pushState / replaceState + popstate so any URL change
// from anywhere in the SPA propagates to the footer link hrefs. Patch
// is idempotent — only applied once per page load even if
// renderSharedFooter is called multiple times.
function _seaDiscoInstallFooterHrefSync() {
  if (window._sdFooterHrefSyncInstalled) return;
  window._sdFooterHrefSyncInstalled = true;
  const origPush    = history.pushState;
  const origReplace = history.replaceState;
  history.pushState = function (...args) {
    const r = origPush.apply(this, args);
    try { _updateFooterHrefs(); } catch {}
    return r;
  };
  history.replaceState = function (...args) {
    const r = origReplace.apply(this, args);
    try { _updateFooterHrefs(); } catch {}
    return r;
  };
  window.addEventListener("popstate", () => { try { _updateFooterHrefs(); } catch {} });
}

function renderSharedFooter(opts) {
  const isSPA = opts?.spa;
  // data-sd-view marks the link for the live href-sync system below.
  const link = (label, view) => {
    const href = _seaDiscoBuildViewHref(view);
    if (isSPA) return `<a href="${href}" data-sd-view="${view}" onclick="event.preventDefault();switchView('${view}');return false">${label}</a>`;
    return `<a href="${href}" data-sd-view="${view}">${label}</a>`;
  };

  // Records-tab links: in SPA mode, route through switchView('records') with the
  // matching sub-tab. Outside the SPA, fall back to a query-string deep link.
  // When signed out, mirror the navbar record-tab behavior: pop the in-page
  // sign-in modal instead of trying to load a records view that requires auth.
  const recLink = (label, tab) => {
    const href = _seaDiscoBuildViewHref(tab);
    if (isSPA) {
      return `<a href="${href}" data-sd-view="${tab}" onclick="event.preventDefault();if(!window._clerk?.user){openSignInModal();return false}_cwTab='${tab}';switchView('records');return false">${label}</a>`;
    }
    return `<a href="${href}" data-sd-view="${tab}">${label}</a>`;
  };

  const footer = document.querySelector("footer");
  if (!footer) return;
  footer.innerHTML = `
    <div class="footer-grid">
      <div class="footer-col">
        <h4>Browse</h4>
        ${link("Search", "search")}
        ${recLink("Collection", "collection")}
        ${recLink("Wantlist", "wantlist")}
        ${recLink("Inventory", "inventory")}
        ${recLink("Lists", "lists")}
        ${recLink("Favorites", "favorites")}
      </div>
      <div class="footer-col">
        <h4>SeaDisco</h4>
        ${isSPA
          ? `<a href="${_seaDiscoBuildViewHref("account")}" data-sd-view="account" onclick="event.preventDefault();openSignInModal();return false;">Account</a>`
          : `<a href="${_seaDiscoBuildViewHref("account")}" data-sd-view="account">Account</a>`}
        ${link("Info", "info")}
        ${link("Privacy Policy", "privacy")}
        ${link("Terms of Service", "terms")}
        ${link("LOC",       "loc")}
        ${link("Wikipedia", "wiki")}
        ${link("Archive",   "archive")}
        <a id="footer-admin-link" href="/admin" style="display:none">Admin</a>
      </div>
    </div>
    <div style="color:#555;font-style:italic;margin-bottom:0.3rem">DISCLAIMER: AI be funky sometimes</div>
    <div>Powered by <a href="https://www.discogs.com" target="_blank" rel="noopener" style="color:var(--muted);text-decoration:none">Discogs</a> and <a href="https://www.anthropic.com" target="_blank" rel="noopener" style="color:var(--muted);text-decoration:none">Claude</a></div>
    <div style="margin-top:0.3rem">&copy; 2026 SeaDisco &nbsp;&middot;&nbsp; Music data courtesy of Discogs API &nbsp;&middot;&nbsp; Not affiliated with Discogs &nbsp;&middot;&nbsp; Jimmy Witherfork Strikes Again</div>`;

  // Wire the live href-sync system so footer link hrefs always reflect
  // the current location.search. Idempotent — only patches history once.
  _seaDiscoInstallFooterHrefSync();
  _updateFooterHrefs();

  // Reveal admin-only footer links (Admin + LOC) when /api/me confirms the
  // current Clerk session is the admin user. /api/me returns { signedIn,
  // isAdmin } based on the server-side ADMIN_CLERK_ID env var, so it's not
  // spoofable from the client. Failures are silent (links stay hidden).
  (async () => {
    try {
      // Wait for Clerk so apiFetch can attach the bearer token. loadClerkInstance
      // is idempotent and returns the cached instance after first call.
      const c = await loadClerkInstance();
      // Wikipedia and LOC footer links stay hidden for non-admins —
      // both surfaces (search, lookup, saves, and the icon affordances
      // sprinkled through modals/cards) are admin-only. The /api/me
      // probe below reveals them only when isAdmin is true.
      const res = await apiFetch("/api/me");
      if (!res.ok) return;
      const data = await res.json();
      // _serverIsAdmin reflects the actual server-confirmed admin status;
      // _isAdmin is the EFFECTIVE flag every consumer reads — which we
      // override to false when the admin has opted into "view as user"
      // mode via /admin. The footer Admin link still uses _serverIsAdmin
      // so the admin can always reach /admin to flip the toggle back.
      window._serverIsAdmin = !!data?.isAdmin;
      let viewAsUser = false;
      try { viewAsUser = localStorage.getItem("sd-admin-as-user") === "1"; } catch {}
      window._adminViewAsUser = viewAsUser && window._serverIsAdmin;
      window._isAdmin = window._serverIsAdmin && !viewAsUser;
      if (window._serverIsAdmin) {
        const adminA = document.getElementById("footer-admin-link");
        if (adminA) adminA.style.display = "";
        // When viewing-as-user, drop a small fixed chip in the corner
        // so the admin can see they're impersonating + restore with
        // one click. Hidden in any other state. Only injected once.
        if (window._adminViewAsUser && !document.getElementById("admin-as-user-chip")) {
          const chip = document.createElement("button");
          chip.id = "admin-as-user-chip";
          chip.type = "button";
          chip.title = "Restore admin view";
          chip.textContent = "Viewing as user · restore";
          chip.onclick = () => {
            try { localStorage.removeItem("sd-admin-as-user"); } catch {}
            location.reload();
          };
          document.body.appendChild(chip);
        }
      }
      if (window._isAdmin) {
        const wikiA = document.getElementById("footer-wiki-link");
        if (wikiA) wikiA.style.display = "";
        const locA = document.getElementById("footer-loc-link");
        if (locA) locA.style.display = "";
        const archA = document.getElementById("footer-archive-link");
        if (archA) archA.style.display = "";
        // Pre-load the discogs_ids AND names already in the
        // blues_artists table so the admin "+ add to Blues DB" icon
        // (popup AND card) can hide itself for artists already in.
        // Cards only know the artist name (parsed from result title),
        // so we cache names alongside ids for that lookup path.
        try {
          const idsRes = await apiFetch("/api/admin/blues/ids");
          if (idsRes.ok) {
            const j = await idsRes.json();
            window._adminBluesIds   = new Set((j.ids   ?? []).map(Number));
            window._adminBluesNames = new Set((j.names ?? []).map(s => String(s).trim().toLowerCase()));
          }
        } catch { /* non-fatal */ }
      }
    } catch { /* hidden by default — fine */ }
  })();
}

// ── Unified entity-lookup popup ──────────────────────────────────────────
// One small floating menu replaces the cluster of W / 🏛 / 📺 icons that
// used to hang next to every track-title or artist-name link. The text
// itself is now the trigger: click it, pick a search target. Play (▶)
// and Queue (➕) stay as separate inline icons because they're actions,
// not lookups, and burying them behind a click would slow common use.
//
// Public API:
//   entityLookupLinkHtml(scope, label, opts)  — inline anchor markup
//   openLookupPopup(ev, scope, label, ctx)    — programmatic open
//   _handleLookupClick(el, ev)                — event delegate for anchors
//
// scope: "track" | "artist"
// ctx:   { trackArtist?: string }   (for tracks, used to scope YT / LOC)

let _lookupPopupEl = null;
let _lookupOutsideHandler = null;

function _closeLookupPopup() {
  if (_lookupPopupEl) { _lookupPopupEl.remove(); _lookupPopupEl = null; }
  if (_lookupOutsideHandler) {
    document.removeEventListener("mousedown", _lookupOutsideHandler, true);
    _lookupOutsideHandler = null;
  }
}

// Build the anchor HTML for a clickable entity-text link. Embeds scope
// + label (+ optional trackArtist) as data-* so a single global click
// handler can reconstruct the popup without each render site having to
// emit a custom inline JS literal.
function entityLookupLinkHtml(scope, label, opts = {}) {
  if (!label) return "";
  const safeLabel  = escHtml(label);
  const artistAttr = opts.trackArtist ? ` data-lk-artist="${escHtml(opts.trackArtist)}"` : "";
  const titleAttr  = opts.title       ? ` title="${escHtml(opts.title)}"`              : "";
  const cls = ["entity-lookup-link", opts.className || ""].filter(Boolean).join(" ");
  return `<a href="#" class="${cls}" data-lk-scope="${escHtml(scope)}" data-lk-label="${safeLabel}"${artistAttr} onclick="event.preventDefault();event.stopPropagation();_handleLookupClick(this,event);return false"${titleAttr}>${safeLabel}</a>`;
}

function _handleLookupClick(el, ev) {
  const scope = el.dataset.lkScope || "track";
  const label = el.dataset.lkLabel || "";
  const ctx   = { trackArtist: el.dataset.lkArtist || "" };
  openLookupPopup(ev, scope, label, ctx);
}

// "Search SeaDisco" handler — preserves the previous text-click behavior
// (general SeaDisco search). Closes any open modal/popup first so the
// user lands on the search results page cleanly.
function _lookupSearchSeaDisco(scope, label) {
  if (typeof closeModal === "function") { try { closeModal(); } catch {} }
  if (typeof _locCloseInfoPopup === "function") { try { _locCloseInfoPopup(); } catch {} }
  if (typeof clearForm === "function") { try { clearForm(); } catch {} }
  if (typeof switchView === "function") { try { switchView("search"); } catch {} }
  setTimeout(() => {
    if (scope === "artist") {
      const artistEl = document.getElementById("f-artist");
      if (artistEl) artistEl.value = label;
      if (typeof toggleAdvanced === "function") { try { toggleAdvanced(true); } catch {} }
    } else {
      const qEl = document.getElementById("query");
      if (qEl) qEl.value = label;
    }
    if (typeof doSearch === "function") doSearch(1);
  }, 30);
}

// Render and position the popup. Wikipedia and LOC are now open to
// anonymous callers (per-IP rate-limited server-side), so all buttons
// show for everyone.
function openLookupPopup(ev, scope, label, ctx) {
  _closeLookupPopup();
  if (!label) return;
  const trackArtist = ctx?.trackArtist || "";

  // Build the YouTube search query: for tracks, include artist for
  // disambiguation; for artists, the bare name is the right query.
  const ytQ = scope === "track" && trackArtist
    ? `"${trackArtist}" "${label}"`
    : `"${label}"`;
  const ytUrl = `https://www.youtube.com/results?search_query=${encodeURIComponent(ytQ)}`;
  // Discogs.com fallback link — useful when the user wants the upstream
  // record page (e.g. for label info we don't surface). Always shown
  // because Discogs is non-admin.
  const dcQ = scope === "artist" ? label : (trackArtist ? `${trackArtist} ${label}` : label);
  const dcUrl = `https://www.discogs.com/search?q=${encodeURIComponent(dcQ)}&type=all`;

  const buttons = [];
  buttons.push({ key: "sd",    icon: "🔎", text: "Search SeaDisco" });
  buttons.push({ key: "coll",  icon: "⌕",  text: scope === "artist" ? "Search my collection" : "Search my records" });
  buttons.push({ key: "yt",    icon: "▶",  text: "YouTube",  url: ytUrl });
  buttons.push({ key: "dc",    icon: "◎",  text: "Discogs.com", url: dcUrl });
  buttons.push({ key: "wiki",  icon: "W",  text: "Wikipedia" });
  buttons.push({ key: "loc",   icon: "🏛", text: "Library of Congress" });

  const wrap = document.createElement("div");
  wrap.className = "lookup-popup";
  wrap.innerHTML = `
    <div class="lookup-popup-head" title="${escHtml(label)}">${escHtml(label)}</div>
    <div class="lookup-popup-list">
      ${buttons.map((b, i) => b.url
        ? `<a href="${escHtml(b.url)}" target="_blank" rel="noopener" class="lookup-popup-btn" data-i="${i}"><span class="lookup-popup-icon">${b.icon}</span>${escHtml(b.text)}</a>`
        : `<button type="button" class="lookup-popup-btn" data-i="${i}"><span class="lookup-popup-icon">${b.icon}</span>${escHtml(b.text)}</button>`
      ).join("")}
    </div>
  `;
  document.body.appendChild(wrap);
  _lookupPopupEl = wrap;

  // Position near the click; clamp so the popup stays in-viewport.
  const popupW = 220;
  const popupH = 32 * buttons.length + 36;
  const x = (ev?.clientX ?? window.innerWidth / 2);
  const y = (ev?.clientY ?? window.innerHeight / 2);
  const left = Math.min(window.innerWidth  - popupW - 8, Math.max(8, x));
  const top  = Math.min(window.innerHeight - popupH - 8, Math.max(8, y + 6));
  wrap.style.position = "fixed";
  wrap.style.left = `${left}px`;
  wrap.style.top  = `${top}px`;
  wrap.style.width = `${popupW}px`;

  // Wire button actions
  wrap.querySelectorAll(".lookup-popup-btn").forEach(el => {
    const i = +el.dataset.i;
    const b = buttons[i];
    if (b.url) {
      // Plain anchor — let it navigate; just dismiss the popup after.
      el.addEventListener("click", () => setTimeout(_closeLookupPopup, 30));
      return;
    }
    el.addEventListener("click", e => {
      e.preventDefault();
      _closeLookupPopup();
      try {
        if (b.key === "sd")    _lookupSearchSeaDisco(scope, label);
        else if (b.key === "coll") {
          if (typeof searchCollectionFor === "function") {
            searchCollectionFor(scope === "artist" ? "cw-artist" : "cw-query", label);
          }
        }
        else if (b.key === "wiki") {
          // Quote phrase for exact-match Wikipedia search; tracks add
          // " song" so song-itself articles outrank artist hits.
          const q = scope === "track" ? `"${label}" song` : `"${label}"`;
          if (typeof openWikiPopup === "function") openWikiPopup(q);
        }
        else if (b.key === "loc") {
          if (scope === "track" && typeof locTrackSearch === "function") {
            locTrackSearch(e, label, trackArtist || "");
          } else if (typeof _locSearchByName === "function") {
            _locSearchByName(label);
          }
        }
      } catch (err) { console.error("lookup action failed:", err); }
    });
  });

  // Dismiss on outside click (deferred so the originating click doesn't
  // immediately close the popup we just opened).
  setTimeout(() => {
    _lookupOutsideHandler = (e) => {
      if (!_lookupPopupEl) return;
      if (_lookupPopupEl.contains(e.target)) return;
      _closeLookupPopup();
    };
    document.addEventListener("mousedown", _lookupOutsideHandler, true);
  }, 0);
}

// Expose globals
window.entityLookupLinkHtml = entityLookupLinkHtml;
window.openLookupPopup      = openLookupPopup;
window._handleLookupClick   = _handleLookupClick;
window._closeLookupPopup    = _closeLookupPopup;
