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

// ── Shared Clerk bootstrap ───────────────────────────────────────────────
// Loads Clerk JS and returns the Clerk instance. Pages handle post-auth UI.
async function loadClerkInstance() {
  const cfg = await fetch("/api/config").then(r => r.json()).catch(() => ({}));
  const pk = cfg.clerkPublishableKey;
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
  const c = await new Promise((resolve) => {
    if (window.Clerk) { resolve(window.Clerk); return; }
    let tries = 0;
    const iv = setInterval(() => {
      tries++;
      if (window.Clerk) { clearInterval(iv); resolve(window.Clerk); }
      else if (tries > 100) { clearInterval(iv); resolve(null); } // 5s timeout
    }, 50);
  });
  if (!c) return null;
  await c.load();

  // Clerk.load() resolves before session hydration on many page loads.
  // Poll for a valid token (proof that auth is fully ready) for up to 5s.
  // If no token materializes, the user is either not signed in or Clerk is slow.
  for (let i = 0; i < 25; i++) {
    try {
      if (c.user && c.session) {
        const t = await c.session.getToken();
        if (t) break; // fully ready
      } else if (c.user === null && i >= 5) {
        break; // confirmed not signed in (after 1s grace)
      }
    } catch { /* ignore */ }
    await new Promise(r => setTimeout(r, 200));
  }

  return c;
}

// ── Shared header injection ──────────────────────────────────────────────
function renderSharedHeader(opts) {
  const isSPA = opts?.spa;
  const active = opts?.active || "";
  const hideRecords = opts?.hideRecords;
  const tab = (label, view) => {
    if (hideRecords && view === "records") {
      return `<a class="main-nav-tab" href="/?view=records">My Records</a>`;
    }
    if (isSPA) {
      const cls = view === active ? ' active' : '';
      if (view === 'records') {
        return `<a class="main-nav-tab nav-disabled" href="/account" data-view="${view}">SIGN UP/IN</a>`;
      }
      return `<button class="main-nav-tab${cls}" data-view="${view}" onclick="switchView('${view}')">${label}</button>`;
    }
    const href = view === "search" ? "/" : `/?view=${view}`;
    const cls = view === active ? ' class="main-nav-tab active"' : ' class="main-nav-tab"';
    return `<a${cls} href="${href}">${label}</a>`;
  };

  const header = document.getElementById("site-header");
  if (!header) return;
  header.innerHTML = `
    <a href="${isSPA ? 'https://seadisco.com' : '/'}" class="header-logo text-logo"><span class="logo-hi">SEA</span><span class="logo-lo">rch</span><span class="logo-gap"></span><span class="logo-hi">DISCO</span><span class="logo-lo">gs</span></a>
    ${isSPA ? '<h1 class="sr-only">SeaDisco — Music Discovery Platform: Search, News, Concerts, Gear &amp; Collection</h1>' : ''}
    <nav id="main-nav">
      <button id="nav-hamburger" onclick="toggleMobileNav()" aria-label="Open navigation">
        <span></span><span></span><span></span>
      </button>
      <div id="nav-tabs-wrap">
        <div id="main-nav-tabs">
          ${tab("Search", "search")}
          ${tab("Drops", "drops")}
          ${tab("Feed", "feed")}
          ${tab("Live", "live")}
          ${tab("Vinyl", "buy")}
          ${tab("Gear", "gear")}
          ${tab("My Records", "records")}
        </div>
        ${isSPA ? '<div id="nav-auth-popup">Sign up and add your Discogs API token for unlimited searches</div>' : ''}
      </div>
    </nav>`;
}

// ── Shared footer injection ──────────────────────────────────────────────
function renderSharedFooter(opts) {
  const isSPA = opts?.spa;
  const link = (label, view) => {
    if (isSPA) return `<a href="javascript:void(0)" onclick="switchView('${view}')">${label}</a>`;
    const href = view === "search" ? "/" : `/?view=${view}`;
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
          ? `<a href="/account" onclick="var t=document.querySelector('#main-nav-tabs [data-view=records]');if(t&&!t.classList.contains('nav-disabled')){event.preventDefault();_cwTab='collection';switchView('records')}">Collection</a>
             <a href="/account" onclick="var t=document.querySelector('#main-nav-tabs [data-view=records]');if(t&&!t.classList.contains('nav-disabled')){event.preventDefault();_cwTab='wantlist';switchView('records')}">Wantlist</a>
             <a href="/account" onclick="var t=document.querySelector('#main-nav-tabs [data-view=records]');if(t&&!t.classList.contains('nav-disabled')){event.preventDefault();_cwTab='inventory';switchView('records')}">Inventory</a>
             <a href="/account" onclick="var t=document.querySelector('#main-nav-tabs [data-view=records]');if(t&&!t.classList.contains('nav-disabled')){event.preventDefault();_cwTab='lists';switchView('records')}">Lists</a>`
          : `<a href="/account">Collection</a>
             <a href="/account">Wantlist</a>
             <a href="/account">Inventory</a>
             <a href="/account">Lists</a>`}
        <a href="/account">Account</a>
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
    <div style="margin-top:0.3rem">&copy; 2026 SeaDisco &nbsp;&middot;&nbsp; Music data courtesy of Discogs API &nbsp;&middot;&nbsp; Not affiliated with Discogs &nbsp;&middot;&nbsp; Jimmy Witherfork Strikes Again</div>`;
}
