// ── SeaDisco app.js — init, routing, auth ────────────────────────────────
// Module load order (in index.html):
//   1. utils.js    — config, helpers, escHtml, bio markup, relations
//   2. search.js   — doSearch, AI search, cards, pagination, artist nav
//   3. collection.js — switchView, collection/wantlist/wants, sync, nav
//   4. modal.js    — album popup, concerts, lightbox, video, bio, versions
//   5. drops.js    — fresh releases, tag cloud, genre filter, drop card popup
//   6. app.js      — auth, URL restore, event wiring (this file)

// Auth-ready promise
let _authReady;
const authReadyPromise = new Promise(res => { _authReady = res; });

// ── Restore from URL on page load ────────────────────────────────────────
(async function () {
  const p = new URLSearchParams(location.search);
  const view = p.get("view");
  if (view === "drops" || view === "live" || view === "info") {
    switchView(view, true);
    // Restore live search from shared URL
    if (view === "live" && (p.get("la") || p.get("lc") || p.get("lg"))) {
      const la = document.getElementById("live-artist");
      const lc = document.getElementById("live-city");
      const lg = document.getElementById("live-genre");
      if (la) la.value = p.get("la") || "";
      if (lc) lc.value = p.get("lc") || "";
      if (lg) lg.value = p.get("lg") || "";
      doLiveSearch();
    }
  } else if (view === "collection" || view === "wantlist" || view === "wanted") {
    await authReadyPromise;
    switchView(view, true);
  } else if (p.toString()) {
    restoreFromParams(p);
    await authReadyPromise;
    doSearch(parseInt(p.get("pg") ?? "1"), true);
  }
  const ctArtist = p.get("ct");
  if (ctArtist) openConcertPopup(null, ctArtist);

  const deferLoad = (fn) => typeof requestIdleCallback === "function" ? requestIdleCallback(fn) : setTimeout(fn, 200);
  deferLoad(() => loadRecentFeed());
  deferLoad(() => loadFreshReleases());
})();

// ── Browser back / forward ───────────────────────────────────────────────
window.addEventListener("popstate", () => {
  const p = new URLSearchParams(location.search);
  const view = p.get("view");
  if (view === "drops" || view === "live" || view === "collection" || view === "wantlist" || view === "info" || view === "wanted") {
    switchView(view, true);
  } else {
    switchView("search", true);
    restoreFromParams(p);
    if (p.toString()) {
      doSearch(parseInt(p.get("pg") ?? "1"), true);
    } else {
      document.getElementById("results").innerHTML = "";
      document.getElementById("blurb").style.display = "none";
      document.getElementById("artist-alts").innerHTML = "";
      document.getElementById("status").textContent = "";
      document.getElementById("pagination").style.display = "none";
      document.getElementById("search-desc").textContent = "";
      document.getElementById("search-returned").textContent = "";
      document.getElementById("search-ai-summary").textContent = "";
      document.getElementById("search-info-block").style.display = "none";
    }
  }
});

// ── Submit on Enter ──────────────────────────────────────────────────────
["query", "f-artist", "f-release", "f-year", "f-label"].forEach(id => {
  document.getElementById(id).addEventListener("keydown", e => {
    if (e.key === "Enter") doSearch(1);
  });
});

// ── Grey out advanced toggle when AI mode selected ───────────────────────
document.querySelectorAll('input[name="result-type"]').forEach(radio => {
  radio.addEventListener("change", () => {
    const isAi = document.querySelector('input[name="result-type"]:checked')?.value === "ai";
    if (isAi) {
      const panel = document.getElementById("advanced-panel");
      const arrow = document.getElementById("advanced-arrow");
      panel.dataset.open = "false";
      arrow.textContent = "▶";
    }
    const toggleBtn = document.getElementById("advanced-toggle");
    if (toggleBtn) {
      toggleBtn.style.opacity  = isAi ? "0.35" : "";
      toggleBtn.style.cursor   = isAi ? "default" : "";
    }
  });
});

// ── Clerk auth init ──────────────────────────────────────────────────────
(async function initAuth() {
  try {
    const cfg = await fetch("/api/config").then(r => r.json()).catch(() => ({}));
    const pk = cfg.clerkPublishableKey;
    if (!pk) { _authReady(); return; }

    const frontendApi = atob(pk.replace(/^pk_(test|live)_/, "")).replace(/\$$/, "");
    await new Promise((resolve, reject) => {
      const s = document.createElement("script");
      s.src = `https://${frontendApi}/npm/@clerk/clerk-js@latest/dist/clerk.browser.js`;
      s.setAttribute("data-clerk-publishable-key", pk);
      s.setAttribute("crossorigin", "anonymous");
      s.onload = resolve; s.onerror = reject;
      document.head.appendChild(s);
    });

    await new Promise(r => setTimeout(r, 50));
    window._clerk = window.Clerk;
    if (!window._clerk) { _authReady(); return; }
    await window._clerk.load();

    const navBtn = document.getElementById("nav-auth-btn");
    if (navBtn) {
      if (window._clerk.user) {
        navBtn.textContent = "Account";
        navBtn.classList.remove("nav-signup-btn");
        const popup = document.getElementById("nav-auth-popup");
        if (popup) popup.remove();
      } else {
        navBtn.textContent = "Sign Up";
        navBtn.classList.add("nav-signup-btn");
      }
    }

    if (window._clerk.user) {
      addNavTab("wanted");
      try {
        const tokenCheck = await apiFetch("/api/user/token");
        if (tokenCheck.ok) {
          const tokenData = await tokenCheck.json();
          if (tokenData.hasToken) {
            addNavTab("collection");
            addNavTab("wantlist");
            await loadDiscogsIds();
          }
        }
      } catch { /* collection tabs optional */ }
    }
  } catch { /* auth unavailable — site works fine without it */ }
  finally { _authReady(); }
})();
