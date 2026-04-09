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

// ── URL param helpers — read "v" with fallback to old "view" ─────────────
function _getView(p) {
  const v = p.get("v") || p.get("view") || "";
  // Map URL names to internal view names
  // Flattened record tabs: v=collection|wantlist|lists|inventory|favorites → records
  if (["collection","wantlist","lists","inventory","favorites"].includes(v)) return "records:" + v;
  return v;
}
function _getPage(p) { return parseInt(p.get("p") || p.get("pg") || "1"); }
function _hasSearch(p) { return p.get("q") || p.get("a") || p.get("ar") || p.get("e") || p.get("re") || p.get("y") || p.get("yr") || p.get("l") || p.get("lb") || p.get("g") || p.get("gn"); }

// ── Restore from URL on page load ────────────────────────────────────────
(async function () {
  const p = new URLSearchParams(location.search);
  const rawView = _getView(p);

  // Handle flattened record tabs (records:collection, records:wantlist, etc.)
  if (rawView.startsWith("records:")) {
    const tab = rawView.split(":")[1];
    await authReadyPromise;
    if (!window._clerk?.user) { showToast("Sign in to view your " + tab, "error"); switchView("account", true); }
    else {
      _cwTab = tab;
      const sort = p.get("s") || p.get("sort");
      if (sort) { const el = document.getElementById("cw-sort"); if (el) el.value = sort; }
      switchView("records", true);
    }
  } else if (rawView === "account") {
    switchView("account", true);
  } else if (rawView === "info" || rawView === "privacy" || rawView === "terms") {
    switchView(rawView, true);
  } else if (rawView === "records" || rawView === "wanted") {
    await authReadyPromise;
    if (!window._clerk?.user) { showToast("Sign in to view your records", "error"); switchView("account", true); }
    else {
      if (rawView === "records") {
        const tab = p.get("tab") || "collection";
        _cwTab = tab;
        const sort = p.get("s") || p.get("sort");
        if (sort) { const el = document.getElementById("cw-sort"); if (el) el.value = sort; }
      }
      switchView(rawView, true);
    }
  } else if (_hasSearch(p)) {
    restoreFromParams(p);
    await authReadyPromise;
    doSearch(_getPage(p), true);
  }
  // Open album popup from URL (works even without a search query)
  // vp = video's source popup (fallback if op was cleared when modal closed during playback)
  const openParam = p.get("op") || p.get("vp");
  if (openParam && !document.getElementById("modal-overlay")?.classList.contains("open")) {
    const colon = openParam.indexOf(":");
    if (colon > 0) {
      const pType = openParam.slice(0, colon);
      const pId   = openParam.slice(colon + 1);
      const pUrl  = `https://www.discogs.com/${pType}/${pId}`;
      // Wait for discogs IDs to load (for badge dots) with a timeout for
      // signed-out users where loadDiscogsIds is never called.
      const idsOrTimeout = Promise.race([
        window._discogsIdsReady,
        new Promise(r => setTimeout(r, 3000)),
      ]);
      const versionParam = p.get("vr");
      idsOrTimeout.then(() => {
        try {
          openModal(null, pId, pType, pUrl);
        } catch (e) { console.error("[op] openModal error:", e); }
        // Open version popup after modal has had time to load
        if (versionParam) setTimeout(() => openVersionPopup(null, versionParam), 1400);
      });
    }
  }
  // Resume video from URL — wait for tracklist if a popup is also opening
  const videoParam = p.get("vd");
  if (videoParam) {
    const playUrl = `https://www.youtube.com/watch?v=${videoParam}`;
    if (openParam) {
      // Poll for tracklist (popup still loading), up to 8s then play anyway
      let waited = 0;
      const poll = setInterval(() => {
        waited += 200;
        if (document.querySelector(".track-link[data-video]") || waited >= 8000) {
          clearInterval(poll);
          openVideo(null, playUrl);
        }
      }, 200);
    } else {
      setTimeout(() => openVideo(null, playUrl), 300);
    }
  }
  const ctArtist = p.get("ct");
  if (ctArtist) openConcertPopup(null, ctArtist);

  // Invite-only mode: signed-out users see only the splash on the home view,
  // so we no longer pre-load community records here. Signed-in users still
  // get their own random records via loadDiscogsIds → loadRandomRecords.
})();

// ── Browser back / forward ───────────────────────────────────────────────
window.addEventListener("popstate", () => {
  const p = new URLSearchParams(location.search);
  const rawView = _getView(p);

  // Flattened record tabs
  if (rawView.startsWith("records:")) {
    _cwTab = rawView.split(":")[1];
    const sort = p.get("s") || p.get("sort");
    if (sort) { const el = document.getElementById("cw-sort"); if (el) el.value = sort; }
    switchView("records", true); return;
  }
  if (rawView === "records" || rawView === "info" || rawView === "privacy" || rawView === "terms" || rawView === "wanted" || rawView === "account") {
    if (rawView === "records") {
      _cwTab = p.get("tab") || "collection";
      const sort = p.get("s") || p.get("sort");
      if (sort) { const el = document.getElementById("cw-sort"); if (el) el.value = sort; }
    }
    switchView(rawView, true);
  } else {
    switchView("search", true);
    restoreFromParams(p);
    if (p.toString()) {
      doSearch(_getPage(p), true);
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
function _applySplashVisibility(clerk) {
  // Invite-only mode: when there's no Clerk session, hide the search UI
  // entirely and show the waitlist splash inline. Info / privacy / terms
  // pages remain reachable via the header nav.
  const splash = document.getElementById("splash-section");
  const form   = document.getElementById("main-search-form");
  const results = document.getElementById("results");
  const random = document.getElementById("random-records");
  const pagination = document.getElementById("pagination");
  const searchInfo = document.getElementById("search-info-block");
  if (!splash) return;

  if (clerk.user) {
    splash.style.display = "none";
    if (form) form.style.display = "";
  } else {
    if (form) form.style.display = "none";
    if (results) results.innerHTML = "";
    if (random) random.style.display = "none";
    if (pagination) pagination.style.display = "none";
    if (searchInfo) searchInfo.style.display = "none";
    splash.style.display = "block";
    // Mount Clerk waitlist (preferred) or fall back to sign-up widget.
    // The waitlist widget handles its own copy; the sign-up fallback
    // borrows the SeaDisco localization so it reads "Join the waitlist"
    // rather than Clerk's default "Create your account".
    const mount = document.getElementById("splash-waitlist-mount");
    if (mount && !mount.dataset.mounted) {
      mount.dataset.mounted = "1";
      const appearance = (typeof SEADISCO_CLERK_APPEARANCE !== "undefined")
        ? SEADISCO_CLERK_APPEARANCE
        : undefined;
      const localization = (typeof SEADISCO_CLERK_LOCALIZATION !== "undefined")
        ? SEADISCO_CLERK_LOCALIZATION
        : undefined;
      try {
        if (typeof clerk.mountWaitlist === "function") {
          clerk.mountWaitlist(mount, { appearance, localization });
        } else if (typeof clerk.mountSignUp === "function") {
          clerk.mountSignUp(mount, { appearance, localization });
        }
      } catch (e) {
        console.error("[splash] Clerk mount failed:", e);
      }
    }
  }
}

async function applyAuthState(clerk) {
  // The header auth tab uses id="nav-auth-tab" (set in shared.js renderSharedHeader).
  // It always invokes openSignInModal() — that helper opens the Clerk
  // sign-in modal when signed-out and routes to /account when signed-in.
  const navBtn = document.getElementById("nav-auth-tab");
  if (navBtn) {
    if (clerk.user) {
      navBtn.textContent = "Account";
      navBtn.classList.remove("nav-signup-btn");
    } else {
      navBtn.textContent = "Sign In";
      navBtn.classList.add("nav-signup-btn");
    }
  }

  _applySplashVisibility(clerk);

  if (clerk.user) {
    // Notify account.js so the account view can update if it's active
    if (typeof handleSignedIn === "function" && document.getElementById("account-view")?.style.display !== "none") {
      handleSignedIn(clerk);
    }
    addNavTab("wanted");
    // Fire token check in the background — it only reveals the
    // collection/wantlist nav tabs and does not block anything visual.
    // Waiting on it adds ~50-100ms to TTI for no user-visible benefit.
    apiFetch("/api/user/token")
      .then(async (tokenCheck) => {
        if (!tokenCheck.ok) return;
        const tokenData = await tokenCheck.json();
        if (tokenData.hasToken) {
          addNavTab("collection");
          addNavTab("wantlist");
        }
      })
      .catch(() => { /* token check optional */ });
    // Load favorite IDs + collection/wantlist IDs for all signed-in users
    await loadDiscogsIds();                   // calls loadRandomRecords inside
  } else {
    // Signed-out: resolve the IDs promise immediately so URL modals don't wait
    if (window._resolveDiscogsIds) window._resolveDiscogsIds();
  }
}

initAuth({
  onSignedIn: applyAuthState,
  onSignedOut: applyAuthState,
  onReady: () => _authReady(),
});

// Service worker removed — no sw.js exists

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
        const hasAdvanced = ["artist","release","label","year","genre","style","format"].some(f => p[f]);
        if (hasAdvanced) toggleAdvanced(true);
        for (const f of ["artist","release","label","year","genre","style","format"]) {
          const el = document.getElementById(`f-${f}`);
          if (el) el.value = p[f] || "";
        }
        doSearch(1);
      },
      searchRow
    );
  }

  // Collection/Wantlist search — attach to the search-row (same position as main search)
  const cwSearchRow = document.getElementById("records-wrap")?.querySelector(".search-row");
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
        const hasAdvanced = ["artist","release","label","year","format","notes"].some(f => p[f]) || p.genre || p.style || p.rating;
        if (hasAdvanced && typeof toggleCwAdvanced === "function") toggleCwAdvanced(true);
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
      cwSearchRow
    );
  }
});
