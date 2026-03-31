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
  if (view === "drops" || view === "live" || view === "buy" || view === "gear" || view === "feed" || view === "info" || view === "privacy" || view === "terms") {
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
  } else if (view === "records" || view === "collection" || view === "wantlist" || view === "wanted") {
    await authReadyPromise;
    // Map old collection/wantlist URLs to records, restore sub-tab
    const mappedView = view === "collection" || view === "wantlist" ? "records" : view;
    if (mappedView === "records") {
      const tab = p.get("tab") || (view === "wantlist" ? "wantlist" : "collection");
      _cwTab = tab;
      const sort = p.get("sort");
      if (sort) { const el = document.getElementById("cw-sort"); if (el) el.value = sort; }
    }
    switchView(mappedView, true);
  } else if (p.get("q") || p.get("ar") || p.get("re") || p.get("yr") || p.get("lb") || p.get("gn")) {
    restoreFromParams(p);
    await authReadyPromise;
    doSearch(parseInt(p.get("pg") ?? "1"), true);
  }
  // Open album popup from URL (works even without a search query)
  const openParam = p.get("op");
  if (openParam && !document.getElementById("modal-overlay")?.classList.contains("open")) {
    const colon = openParam.indexOf(":");
    if (colon > 0) {
      const pType = openParam.slice(0, colon);
      const pId   = openParam.slice(colon + 1);
      const pUrl  = `https://www.discogs.com/${pType}/${pId}`;
      // Small delay ensures DOM + Clerk are settled before opening
      setTimeout(() => {
        try {
          openModal(null, pId, pType, pUrl);
        } catch (e) { console.error("[op] openModal error:", e); }
      }, 150);
      const versionParam = p.get("vr");
      if (versionParam) setTimeout(() => openVersionPopup(null, versionParam), 1400);
      const videoParam = p.get("vd");
      if (videoParam) setTimeout(() => openVideo(null, `https://www.youtube.com/watch?v=${videoParam}`), 1400);
    }
  }
  const ctArtist = p.get("ct");
  if (ctArtist) openConcertPopup(null, ctArtist);

  const deferLoad = (fn) => typeof requestIdleCallback === "function" ? requestIdleCallback(fn) : setTimeout(fn, 200);
  deferLoad(() => loadWantedSample());
  deferLoad(() => loadFreshReleases());
})();

// ── Browser back / forward ───────────────────────────────────────────────
window.addEventListener("popstate", () => {
  const p = new URLSearchParams(location.search);
  const view = p.get("view");
  // Map old collection/wantlist URLs to records, restore sub-tab
  if (view === "collection" || view === "wantlist") {
    _cwTab = view === "wantlist" ? "wantlist" : "collection";
    const sort = p.get("sort");
    if (sort) { const el = document.getElementById("cw-sort"); if (el) el.value = sort; }
    switchView("records", true); return;
  }
  if (view === "drops" || view === "live" || view === "buy" || view === "gear" || view === "feed" || view === "records" || view === "info" || view === "privacy" || view === "terms" || view === "wanted") {
    if (view === "records") {
      _cwTab = p.get("tab") || "collection";
      const sort = p.get("sort");
      if (sort) { const el = document.getElementById("cw-sort"); if (el) el.value = sort; }
    }
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

// ── Service Worker registration ──────────────────────────────────────────
if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("/sw.js").catch(() => {});
}

// ── Saved search UI init (after auth ready) ─────────────────────────────
authReadyPromise.then(() => {
  if (!window._clerk?.user) return;

  // Main search — next to search button
  const searchRow = document.querySelector(".search-row");
  if (searchRow) {
    buildSavedSearchUI("search",
      () => {
        const params = {};
        const q = document.getElementById("query")?.value?.trim();
        if (q) params.q = q;
        const type = document.querySelector('input[name="result-type"]:checked')?.value;
        if (type) params.type = type;
        const sort = document.getElementById("f-sort")?.value;
        if (sort) params.sort = sort;
        for (const f of ["artist","release","label","year","genre","style","format"]) {
          const v = document.getElementById(`f-${f}`)?.value?.trim();
          if (v) params[f] = v;
        }
        return params;
      },
      (p) => {
        const q = document.getElementById("query");
        if (q) q.value = p.q || "";
        if (p.type) {
          const radio = document.querySelector(`input[name="result-type"][value="${p.type}"]`);
          if (radio) radio.checked = true;
        }
        const sort = document.getElementById("f-sort");
        if (sort && p.sort) sort.value = p.sort;
        for (const f of ["artist","release","label","year","genre","style","format"]) {
          const el = document.getElementById(`f-${f}`);
          if (el) el.value = p[f] || "";
        }
        doSearch(1);
      },
      searchRow
    );
  }

  // Drops search
  const dropsBar = document.querySelector(".fresh-tag-footer");
  if (dropsBar) {
    buildSavedSearchUI("drops",
      () => {
        const q = document.getElementById("fresh-tag-input")?.value?.trim();
        return q ? { q } : {};
      },
      (p) => {
        const el = document.getElementById("fresh-tag-input");
        if (el) { el.value = p.q || ""; el.dispatchEvent(new Event("input")); }
      },
      dropsBar
    );
  }

  // Live search
  const liveForm = document.querySelector(".live-search-row");
  if (liveForm) {
    buildSavedSearchUI("live",
      () => {
        const params = {};
        const a = document.getElementById("live-artist")?.value?.trim();
        const c = document.getElementById("live-city")?.value?.trim();
        const g = document.getElementById("live-genre")?.value;
        if (a) params.artist = a;
        if (c) params.city = c;
        if (g) params.genre = g;
        return params;
      },
      (p) => {
        const a = document.getElementById("live-artist");
        const c = document.getElementById("live-city");
        const g = document.getElementById("live-genre");
        if (a) a.value = p.artist || "";
        if (c) c.value = p.city || "";
        if (g) g.value = p.genre || "";
        doLiveSearch();
      },
      liveForm
    );
  }

  // Buy (Vinyl) filter
  const buyBar = document.querySelector(".buy-right-controls");
  if (buyBar) {
    buildSavedSearchUI("buy",
      () => {
        const q = document.querySelector(".buy-search-field")?.value?.trim();
        return q ? { q } : {};
      },
      (p) => {
        const el = document.querySelector(".buy-search-field");
        if (el) { el.value = p.q || ""; el.dispatchEvent(new Event("input")); }
      },
      buyBar
    );
  }

  // Gear filter
  const gearBar = document.querySelector(".gear-right-controls");
  if (gearBar) {
    buildSavedSearchUI("gear",
      () => {
        const q = document.querySelector(".gear-search-field")?.value?.trim();
        return q ? { q } : {};
      },
      (p) => {
        const el = document.querySelector(".gear-search-field");
        if (el) { el.value = p.q || ""; el.dispatchEvent(new Event("input")); }
      },
      gearBar
    );
  }

  // Feed filter
  const feedBar = document.querySelector(".feed-right-controls");
  if (feedBar) {
    buildSavedSearchUI("feed",
      () => {
        const q = document.querySelector(".feed-search-field")?.value?.trim();
        return q ? { q } : {};
      },
      (p) => {
        const el = document.querySelector(".feed-search-field");
        if (el) { el.value = p.q || ""; el.dispatchEvent(new Event("input")); }
      },
      feedBar
    );
  }

  // Collection/Wantlist search — attach to the search-row (same position as main search)
  const cwSearchRow = document.getElementById("cw-query")?.closest(".search-row");
  if (cwSearchRow) {
    buildSavedSearchUI("records",
      () => {
        const params = {};
        const q = document.getElementById("cw-query")?.value?.trim();
        if (q) params.q = q;
        for (const f of ["artist","release","label","year","format","notes"]) {
          const v = document.getElementById(`cw-${f}`)?.value?.trim();
          if (v) params[f] = v;
        }
        const genre = document.getElementById("cw-genre")?.value;
        if (genre) params.genre = genre;
        const style = document.getElementById("cw-style")?.value;
        if (style) params.style = style;
        const rating = document.getElementById("cw-rating")?.value;
        if (rating) params.rating = rating;
        return params;
      },
      (p) => {
        const q = document.getElementById("cw-query");
        if (q) q.value = p.q || "";
        for (const f of ["artist","release","label","year","format","notes"]) {
          const el = document.getElementById(`cw-${f}`);
          if (el) el.value = p[f] || "";
        }
        const genre = document.getElementById("cw-genre");
        if (genre && p.genre) genre.value = p.genre;
        const style = document.getElementById("cw-style");
        if (style && p.style) style.value = p.style;
        const rating = document.getElementById("cw-rating");
        if (rating && p.rating) rating.value = p.rating;
        if (typeof doCwSearch === "function") doCwSearch(1);
      },
      cwControls
    );
  }
});
