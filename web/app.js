// ── SeaDisco app.js — init, routing, auth ────────────────────────────────
// Module load order (in index.html):
//   1. utils.js    — config, helpers, escHtml, bio markup, relations
//   2. search.js   — doSearch, AI search, cards, pagination, artist nav
//   3. collection.js — switchView, collection/wantlist/wants, sync, nav
//   4. modal.js    — album popup, lightbox, video, bio, versions
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
  } else if (rawView === "info" || rawView === "privacy" || rawView === "terms" || rawView === "wiki") {
    switchView(rawView, true);
  } else if (rawView === "loc") {
    await authReadyPromise;
    if (!window._clerk?.user) { showToast("Sign in to browse LOC", "error"); switchView("account", true); }
    else switchView("loc", true);  // initLocView() does the admin check
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

  // ── Restore stacked popups from URL ────────────────────────────────────
  // Load order is tuned for perceived speed: warm up the YouTube API
  // immediately so audio can start as soon as a tracklist arrives, then
  // open the topmost popup first so the user sees what they expect, then
  // open underlying popups in parallel so deeper context is ready when
  // they close the topmost. Stacking order (top→bottom):
  //   wk (wiki) > vr (release) > op (master/release) > vd (video bar)
  const wkParam      = p.get("wk");
  const versionParam = p.get("vr");
  const openParam    = p.get("op") || p.get("vp"); // vp = video parent popup fallback
  const videoParam   = p.get("vd");

  // 1) Pre-warm YouTube API so playback starts the instant we have a URL.
  if (videoParam && typeof ensureYTAPI === "function") { try { ensureYTAPI(); } catch {} }

  // 2) Topmost: wiki popup (independent fetch, no DOM dependencies).
  // Must wait for authReadyPromise — apiFetch needs the Clerk Bearer token
  // attached or /api/wikipedia/lookup returns 401 (auth_required) and the
  // popup shows "Wikipedia lookup failed". Open the empty overlay
  // synchronously so the user sees something immediately, then await auth
  // before the network call inside openWikiPopup.
  if (wkParam && typeof openWikiPopup === "function") {
    const wikiOverlay = document.getElementById("wiki-overlay");
    if (wikiOverlay) wikiOverlay.classList.add("open");
    authReadyPromise.then(() => {
      try { openWikiPopup(wkParam); } catch {}
    });
  }

  // 3) Release popup (vr) is the visible top of the modal stack — open
  //    immediately, in parallel with the underlying master modal below.
  //    openVersionPopup fetches its own data, so it does NOT depend on the
  //    master modal having loaded.
  if (versionParam && typeof openVersionPopup === "function") {
    setTimeout(() => { try { openVersionPopup(null, versionParam); } catch {} }, 0);
  }

  // 4) Underlying master/release modal — kicked off in parallel with vr.
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
      idsOrTimeout.then(() => {
        try { openModal(null, pId, pType, pUrl); } catch {}
      });
    }
  }

  // 5) Video bar — start playing as soon as a tracklist exists so the queue
  //    is correct. We poll quickly (already 200ms cadence). YT API was
  //    pre-warmed above so the first frame loads fast.
  if (videoParam) {
    const playUrl = `https://www.youtube.com/watch?v=${videoParam}`;
    if (openParam || versionParam) {
      let waited = 0;
      const poll = setInterval(() => {
        waited += 200;
        if (document.querySelector(".track-link[data-video]") || waited >= 8000) {
          clearInterval(poll);
          try { openVideo(null, playUrl); } catch {}
        }
      }, 200);
    } else {
      setTimeout(() => { try { openVideo(null, playUrl); } catch {} }, 300);
    }
  }

  // 6) Restore AI search panel if shared.
  const aiParam = p.get("ai");
  if (aiParam && typeof doAiSearch === "function") {
    await authReadyPromise;
    setTimeout(() => { try { doAiSearch(aiParam); } catch {} }, 0);
  }

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
  if (rawView === "records" || rawView === "info" || rawView === "privacy" || rawView === "terms" || rawView === "wanted" || rawView === "account" || rawView === "loc" || rawView === "wiki") {
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

// ── Per-field × clear button ────────────────────────────────────────────
// Wrap each text input in a relative span and append a small × button
// that appears only when the input has text. Click clears the input,
// dispatches an `input` event so any listeners (e.g. populateStyles)
// still fire, and refocuses the field.
function _attachInputClearButtons(ids) {
  ids.forEach(id => {
    const input = document.getElementById(id);
    if (!input || input.dataset.hasClearBtn === "1") return;
    input.dataset.hasClearBtn = "1";
    let wrap = input.parentElement;
    if (!wrap || !wrap.classList?.contains("input-clear-wrap")) {
      const span = document.createElement("span");
      span.className = "input-clear-wrap";
      input.parentNode.insertBefore(span, input);
      span.appendChild(input);
      wrap = span;
    }
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "input-clear-btn";
    btn.title = "Clear field";
    btn.tabIndex = -1;
    btn.textContent = "×";
    wrap.appendChild(btn);
    const update = () => wrap.classList.toggle("has-text", !!input.value);
    update();
    input.addEventListener("input", update);
    btn.addEventListener("click", (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      input.value = "";
      input.dispatchEvent(new Event("input", { bubbles: true }));
      input.focus();
    });
  });
}
_attachInputClearButtons([
  // Main + advanced search form
  "query", "f-artist", "f-release", "f-year", "f-label", "f-country",
  // Collection / wantlist search panel
  "cw-query", "cw-artist", "cw-release", "cw-year", "cw-label", "cw-notes",
]);

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
// Public mode: site is visible to everyone. Search works without
// signing in; sync / collection / wantlist / favorites still require
// auth, gated server-side. The old splash-section is only shown if
// it's still in the DOM AND nothing else is hiding it.
function _applySplashVisibility(_clerk) {
  const splash = document.getElementById("splash-section");
  const form   = document.getElementById("main-search-form");
  if (splash) splash.style.display = "none";
  if (form)   form.style.display   = "";
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
