// ── Shared utilities for all pages (index, account, admin) ──────────────

// ── Relative time formatting ─────────────────────────────────────────────
function fmtTime(ts) {
  if (!ts) return "never";
  const diff = Math.round((Date.now() - new Date(ts)) / 60000);
  if (diff < 1) return "just now";
  if (diff < 60) return `${diff}m ago`;
  const h = Math.round(diff / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

function fmtRelativeTime(ts) {
  if (!ts) return "\u2014";
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
  if (raw._type === "live") {
    const parts = [];
    if (raw.artist) parts.push(raw.artist);
    if (raw.city)   parts.push(raw.city);
    if (raw.genre)  parts.push(raw.genre);
    return "\ud83c\udfa4 " + (parts.join(" \u00b7 ") || "Live search");
  }
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
    try {
      const cached = JSON.parse(localStorage.getItem("_clerkCfg") || "{}");
      if (cached.pk && (Date.now() - (cached.ts || 0)) < 3600000) pk = cached.pk;
    } catch {}
    if (!pk) {
      const cfg = await fetch("/api/config").then(r => r.json()).catch(() => ({}));
      pk = cfg.clerkPublishableKey || "";
      if (pk) try { localStorage.setItem("_clerkCfg", JSON.stringify({ pk, ts: Date.now() })); } catch {}
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

  await c.load();

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

// ── Shared header injection ──────────────────────────────────────────────
function renderSharedHeader(opts) {
  const isSPA = opts?.spa;
  const active = opts?.active || "";
  const hideRecords = opts?.hideRecords;

  // Top-row tab (discover pages + auth)
  const tab = (label, view) => {
    if (isSPA) {
      const cls = view === active ? ' active' : '';
      return `<button class="nav-tab-top${cls}" data-view="${view}" onclick="switchView('${view}')">${label}</button>`;
    }
    const urlView = view === "buy" ? "vinyl" : view;
    const href = view === "search" ? "/" : `/?v=${urlView}`;
    const cls = view === active ? ' class="nav-tab-top active"' : ' class="nav-tab-top"';
    return `<a${cls} href="${href}">${label}</a>`;
  };

  // Bottom-row tab — plain text, hover color matches the badge dot for that tab.
  const colors = { collection: "#6ddf70", wantlist: "#f0c95c", lists: "#a0ccf0", inventory: "#cda0f5", favorites: "#ff6b35" };
  const recTab = (label, rtab) => {
    const displayLabel = label;
    if (isSPA) {
      return `<button class="nav-tab-bot nav-rec-disabled" data-rtab="${rtab}" onclick="showRecordSignIn('${rtab}')">${displayLabel}</button>`;
    }
    return `<a class="nav-tab-bot" href="/?v=${rtab}" data-rtab="${rtab}">${displayLabel}</a>`;
  };

  // Auth tab (top row, rightmost) — uses <button> in SPA like all other tabs
  const authTab = isSPA
    ? `<button class="nav-tab-top nav-auth-tab" data-view="account" onclick="switchView('account')" id="nav-auth-tab">Sign In</button>`
    : `<a class="nav-tab-top nav-auth-tab" href="/?v=account" id="nav-auth-tab">Sign In</a>`;

  const header = document.getElementById("site-header");
  if (!header) return;
  // Site build/version tag shown as tiny grey text under the logo. Updated
  // whenever the cache-bust version is bumped so the user can eyeball whether
  // they're on the latest build without digging into devtools.
  const SITE_VERSION = "build 20260408f";
  header.innerHTML = `
    <div class="header-logo-wrap">
      <a href="${isSPA ? 'https://seadisco.com' : '/'}" class="header-logo text-logo"><span class="logo-hi">SEA</span><span class="logo-lo">rch</span><span class="logo-gap"></span><span class="logo-hi">DISCO</span><span class="logo-lo">gs</span></a>
      <div class="header-version" title="Current build">${SITE_VERSION}</div>
    </div>
    ${isSPA ? '<h1 class="sr-only">SeaDisco — Music Discovery Platform: Search, News, Concerts, Gear &amp; Collection</h1>' : ''}
    <nav id="main-nav">
      <button id="nav-hamburger" onclick="toggleMobileNav()" aria-label="Open navigation">
        <span></span><span></span><span></span>
      </button>
      <div id="nav-tabs-wrap">
        <div id="main-nav-tabs">
          <div class="nav-row nav-row-top">
            ${tab("Search", "search")}
            ${tab("Drops", "drops")}
            ${tab("Feed", "feed")}
            ${tab("Live", "live")}
            ${tab("Vinyl", "buy")}
            ${tab("Gear", "gear")}
            ${authTab}
          </div>
          <div class="nav-row nav-row-bot" id="nav-row-records">
            ${recTab("Collection", "collection")}
            ${recTab("Wantlist", "wantlist")}
            ${recTab("Lists", "lists")}
            ${recTab("Inventory", "inventory")}
            ${recTab("Favorites", "favorites")}
          </div>
        </div>
      </div>
    </nav>`;

  // On non-SPA pages (account, admin), update auth tab once Clerk resolves
  if (!isSPA) {
    loadClerkInstance().then(c => {
      if (c?.user) {
        const el = document.getElementById("nav-auth-tab");
        if (el) el.textContent = "Account";
      }
    }).catch(() => {});
  }
}

// ── Shared footer injection ──────────────────────────────────────────────
function renderSharedFooter(opts) {
  const isSPA = opts?.spa;
  const link = (label, view) => {
    if (isSPA) return `<a href="javascript:void(0)" onclick="switchView('${view}')">${label}</a>`;
    const urlView = view === "buy" ? "vinyl" : view;
    const href = view === "search" ? "/" : `/?v=${urlView}`;
    return `<a href="${href}">${label}</a>`;
  };

  const footer = document.querySelector("footer");
  if (!footer) return;
  footer.innerHTML = `
    <div class="footer-grid">
      <div class="footer-col">
        <h4>Discover</h4>
        ${link("Search", "search")}
        ${link("Drops", "drops")}
        ${link("Feed", "feed")}
        ${link("Live", "live")}
        ${link("Vinyl", "buy")}
        ${link("Gear", "gear")}
      </div>
      <div class="footer-col">
        <h4>Your Music</h4>
        ${isSPA
          ? `<a href="/?v=account" onclick="var t=document.querySelector('#nav-row-records .nav-tab-bot:not(.nav-rec-disabled)');if(t){event.preventDefault();_cwTab='collection';switchView('records')}">Collection</a>
             <a href="/?v=account" onclick="var t=document.querySelector('#nav-row-records .nav-tab-bot:not(.nav-rec-disabled)');if(t){event.preventDefault();_cwTab='wantlist';switchView('records')}">Wantlist</a>
             <a href="/?v=account" onclick="var t=document.querySelector('#nav-row-records .nav-tab-bot:not(.nav-rec-disabled)');if(t){event.preventDefault();_cwTab='inventory';switchView('records')}">Inventory</a>
             <a href="/?v=account" onclick="var t=document.querySelector('#nav-row-records .nav-tab-bot:not(.nav-rec-disabled)');if(t){event.preventDefault();_cwTab='lists';switchView('records')}">Lists</a>
             <a href="/?v=account" onclick="var t=document.querySelector('#nav-row-records .nav-tab-bot:not(.nav-rec-disabled)');if(t){event.preventDefault();_cwTab='favorites';switchView('records')}">Favorites</a>`
          : `<a href="/?v=account">Collection</a>
             <a href="/?v=account">Wantlist</a>
             <a href="/?v=account">Inventory</a>
             <a href="/?v=account">Lists</a>
             <a href="/?v=account">Favorites</a>`}
        ${isSPA ? `<a href="/?v=account" onclick="switchView('account');return false;">Account</a>` : `<a href="/?v=account">Account</a>`}
      </div>
      <div class="footer-col">
        <h4>About</h4>
        ${link("Info", "info")}
        ${link("Privacy Policy", "privacy")}
        ${link("Terms of Service", "terms")}
      </div>
    </div>
    <div style="color:#555;font-style:italic;margin-bottom:0.3rem">DISCLAIMER: AI be funky sometimes</div>
    <div>Powered by <a href="https://www.discogs.com" target="_blank" rel="noopener" style="color:var(--muted);text-decoration:none">Discogs</a>, <a href="https://www.anthropic.com" target="_blank" rel="noopener" style="color:var(--muted);text-decoration:none">Claude</a>, and <a href="https://listenbrainz.org" target="_blank" rel="noopener" style="color:var(--muted);text-decoration:none">ListenBrainz</a></div>
    <div style="margin-top:0.3rem">&copy; 2026 SeaDisco <span id="footer-user-count" style="color:#555;font-size:0.8em"></span> &nbsp;&middot;&nbsp; Music data courtesy of Discogs API &nbsp;&middot;&nbsp; Not affiliated with Discogs &nbsp;&middot;&nbsp; Jimmy Witherfork Strikes Again</div>`;
  // Fetch user count for footer
  fetch("/api/user-count").then(r => r.json()).then(d => {
    const el = document.getElementById("footer-user-count");
    if (el && d.count != null) el.textContent = `${d.count}/${d.limit}`;
  }).catch(() => {});
}
