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

// Fetch /api/me once and cache the admin flag on window. Used by URL
// routing (wiki/loc are admin-only) so we don't briefly render the
// page before shared.js's footer probe resolves. Returns the cached
// boolean if already known.
async function _ensureAdminFlag() {
  if (typeof window._isAdmin === "boolean") return window._isAdmin;
  try {
    const r = await apiFetch("/api/me");
    if (!r.ok) { window._isAdmin = false; return false; }
    const j = await r.json();
    window._isAdmin = !!j?.isAdmin;
    // Populate the per-account demo flag + broad YT-open toggle here
    // too, so URL-direct nav to /?v=youtube sees the right access
    // state without waiting for shared.js's separate /api/me probe.
    window._sdIsDemo = !!j?.isDemo;
    window._sdYtOpen = !!j?.ytOpen;
    return window._isAdmin;
  } catch {
    window._isAdmin = false;
    return false;
  }
}

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
  } else if (rawView === "wiki" || rawView === "loc" || rawView === "archive" || rawView === "youtube") {
    // Companion views (Wikipedia / LOC / Archive / YouTube) are now
    // signed-in only. Anon visitors hitting these URLs go through
    // switchView's _sdGateSignedInView gate, which pops the sign-in
    // modal and falls back to the search splash.
    await authReadyPromise;
    // YouTube is additionally admin-only right now (quota constraint).
    // Wait for the admin flag before routing — without this an admin
    // hitting /?v=youtube directly would race the /api/me probe and
    // get bounced back to /?v=search by the gate.
    if (rawView === "youtube") await _ensureAdminFlag();
    switchView(rawView, true);
  } else if (rawView === "picks") {
    // Legacy /?v=picks bookmark — Submitted Tracks moved into the
    // home-strip Recent / Suggestions / Submitted toggle. Land on
    // search and switch to the Submitted tab in-session (no
    // persistence, since the strip mode resets to Recent on every
    // page load by design).
    switchView("search", true);
    if (typeof window._sdSwitchHomeStripTab === "function") {
      window._sdSwitchHomeStripTab("submitted");
    }
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
  } else {
    // Bare /  (or any URL with no recognized view param): render the
    // default search view so loadRandomRecords fires and the
    // "Suggested" / "Recent" strip populates. Without this, anon
    // users on a hard refresh saw an empty home page until they
    // clicked the logo (which routes through goHome → switchView).
    switchView("search", true);
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
  // op = "open popup" — explicit. vp = "video parent" — set
  // automatically by setVideoUrl as the queue advances so the disc
  // icon knows the playing track's album. We deliberately do NOT
  // fall back to vp here: URLs that only carry vp would otherwise
  // auto-pop the album for the playing track on every reload, even
  // when the user never asked for that popup. Sharing an album +
  // track together is done from the popup itself, which sets op=.
  const openParam    = p.get("op");
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

  // 5) Video bar — URL ?vd= takes precedence over saved-queue
  //    auto-surface. Play immediately so the user gets the track they
  //    asked for without a flash of the prior queue's idle bar. The
  //    earlier "wait up to 8s for the tracklist to mount" gate was
  //    only useful for the per-album _videoQueue scoping; with the
  //    cross-source queue carrying its own metadata the wait isn't
  //    worth the perceptible delay (and during that wait queue.js's
  //    idle-bar timer fired first, briefly showing the saved queue
  //    head — wrong precedence).
  if (videoParam) {
    const playUrl = `https://www.youtube.com/watch?v=${videoParam}`;
    setTimeout(() => { try { openVideo(null, playUrl); } catch {} }, 0);
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

// ── Global × clear-button auto-attach ────────────────────────────────────
// Beyond the explicit ID list above, attach a × clear button to every
// text-like input on the site (LOC form, account page, popup forms,
// dynamically-rendered fields, etc.). Opt-out by setting `data-no-clear`
// on the input or any ancestor — used to skip Clerk-mounted widgets
// where the wrapper <span> would collide with their internal layout.
function _attachClearToOneInput(input) {
  if (!input || input.dataset.hasClearBtn === "1") return;
  if (input.dataset.noClear === "1") return;
  if (input.closest("[data-no-clear]")) return;
  // Clerk's mounted forms have their own field styling — skip everything
  // in their root containers to avoid breaking the sign-in/up UI.
  if (input.closest(".cl-rootBox, .cl-component, .cl-card, .cl-modalContent, .cl-userButtonPopover")) return;
  // Datalist-only inputs and hidden inputs aren't worth clearing.
  if (input.type === "hidden") return;
  if (!input.offsetParent && getComputedStyle(input).display === "none") {
    // Defer — input might be in a hidden tab; observer will catch it on display
    return;
  }
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
}

function _attachClearButtonsGlobally(root) {
  const scope = root || document;
  if (scope.querySelectorAll) {
    const sel = 'input[type="text"], input[type="search"], input[type="email"], input[type="url"], input[type="tel"], input:not([type])';
    scope.querySelectorAll(sel).forEach(_attachClearToOneInput);
  }
}

// Initial sweep, then observe the DOM for inputs that get rendered
// later (LOC form, account dashboard, modal popups, etc.).
_attachClearButtonsGlobally(document);
const _clearFieldObserver = new MutationObserver((mutations) => {
  for (const m of mutations) {
    for (const node of m.addedNodes) {
      if (!node || node.nodeType !== 1) continue;
      if (node.tagName === "INPUT") _attachClearToOneInput(node);
      else if (node.querySelectorAll) _attachClearButtonsGlobally(node);
    }
  }
});
_clearFieldObserver.observe(document.body, { childList: true, subtree: true });

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
// Search remains publicly accessible (anon visitors can search Discogs
// and play YouTube previews). The home strip (Recent / Suggestions /
// Submitted) and the anon splash panel are mutually exclusive: the
// strip shows for signed-in users who have library data; the splash
// panel pitches the waitlist + OAuth feature set to everyone else.
function _applySplashVisibility(clerk) {
  const splash    = document.getElementById("splash-section");
  const form      = document.getElementById("main-search-form");
  const anonSplash = document.getElementById("anon-splash");
  const stripWrap  = document.getElementById("random-records");
  const resultsEl  = document.getElementById("results");
  const signedIn = !!clerk?.user;
  if (splash) splash.style.display = "none"; // legacy splash, never used now
  // Anon visitors get the same surfaces signed-in users do — search
  // bar, results grid, home strip — so they can preview the catalog
  // (the Feed tab in the home strip is anon-accessible). The
  // anon-splash node is now a thin signup banner above everything
  // rather than a full takeover. The body class lets CSS shrink the
  // splash node (and any other anon-only nudges) to a slim banner.
  document.body.classList.toggle("sd-anon", !signedIn);
  if (form)       form.style.display      = "";
  if (resultsEl)  resultsEl.style.display = "";
  if (anonSplash) anonSplash.style.display = signedIn ? "none" : "";
  if (stripWrap)  stripWrap.style.display  = "";
}

async function applyAuthState(clerk) {
  // The header auth tab uses id="nav-auth-tab" (set in shared.js renderSharedHeader).
  // It always invokes openSignInModal() — that helper opens the Clerk
  // sign-in modal when signed-out and routes to /account when signed-in.
  const navBtn = document.getElementById("nav-auth-tab");
  if (navBtn) {
    const newLabel = clerk.user ? "Account" : "Sign In";
    navBtn.title = newLabel;
    // When iconNav is on the tab contains [icon SVG][label span] — set
    // textContent on the whole tab would nuke the icon. Update only
    // the .nav-label span if it exists; otherwise fall back to text.
    const labelSpan = navBtn.querySelector(".nav-label");
    if (labelSpan) labelSpan.textContent = newLabel;
    else navBtn.textContent = newLabel;
    if (clerk.user) navBtn.classList.remove("nav-signup-btn");
    else navBtn.classList.add("nav-signup-btn");
  }

  _applySplashVisibility(clerk);

  if (clerk.user) {
    // Notify account.js so the account view can update if it's active
    if (typeof handleSignedIn === "function" && document.getElementById("account-view")?.style.display !== "none") {
      handleSignedIn(clerk);
    }
    addNavTab("wanted");
    // Enable record tabs for ANY signed-in user. The records view's
    // own empty-state handles the "no Discogs OAuth connected yet"
    // case with a "Connect your Discogs account" CTA — better UX
    // than leaving the tabs in their disabled state where every
    // click bounces to the account page.
    addNavTab("collection");
    addNavTab("wantlist");
    // Load favorite IDs + collection/wantlist IDs for all signed-in users
    await loadDiscogsIds();                   // calls loadRandomRecords inside
  } else {
    // Signed-out: resolve the IDs promise immediately so URL modals don't wait
    if (window._resolveDiscogsIds) window._resolveDiscogsIds();
  }
}

// Offline-mode boot path — set by the inline script in index.html's
// <head> based on localStorage flag + navigator.onLine. When true,
// Clerk can't validate the session (its API is unreachable), so we
// skip auth entirely and stub a "signed-in offline" state. The
// apiFetch wrapper in offline.js falls back to IDB for the user's
// library endpoints, so collection / wantlist / inventory / lists
// keep working. Mutations + search + AI gracefully fail (network
// errors). Real auth resumes the next time the user is online.
if (window._sdOfflineMode) {
  window._clerk = { user: { id: "offline-user" }, session: null };
  // Resolve auth-related promises so URL-driven popup restoration
  // doesn't hang waiting for a signed-in state we'll never get.
  if (window._resolveDiscogsIds) window._resolveDiscogsIds();
  _authReady();
  // Hide the splash / sign-in CTA on the home view since "sign in"
  // can't actually do anything offline.
  document.documentElement.classList.add("sd-offline-mode");
  if (typeof _applySplashVisibility === "function") {
    try { _applySplashVisibility(window._clerk); } catch {}
  }
  // Wire the navbar AFTER renderSharedHeader has painted it — that
  // happens at DOMContentLoaded. Two things to override:
  //  - records tabs (Collection / Wantlist / Favorites / Inventory /
  //    Lists) start with onclick="showRecordSignIn(...)" which routes
  //    through openSignInModal → switchView("account"). With our
  //    stubbed Clerk that path lands on a half-broken Account view
  //    no matter which tab the user clicked.
  //  - the auth tab's openSignInModal call also lands on Account
  //    when _clerk.user is truthy. We want it to go directly there
  //    too, but at least relabeled and with no detour through Clerk.
  document.addEventListener("DOMContentLoaded", () => {
    // addNavTab("collection") enables the whole records row in one
    // shot — iterates every #nav-row-records .nav-tab-top[data-rtab]
    // and rebinds onclick to set _cwTab + switchView("records").
    // Without this, the default onclick is showRecordSignIn() which
    // routes to openSignInModal → switchView("account"), so every
    // navbar click in offline mode lands on a half-broken Account
    // view regardless of which tab was actually clicked.
    try { addNavTab("collection"); } catch {}
    // Auth tab: relabel + bypass openSignInModal (which would also
    // hop us to Account view via Clerk's user-route fallback). Go
    // straight to the account view so the user can find the offline
    // toggle to turn it off if they want.
    const navBtn = document.getElementById("nav-auth-tab");
    if (navBtn) {
      const labelSpan = navBtn.querySelector(".nav-label");
      const txt = "Offline";
      if (labelSpan) labelSpan.textContent = txt; else navBtn.textContent = txt;
      navBtn.title = "You're offline — using cached library";
      navBtn.onclick = (e) => { e.preventDefault?.(); switchView("account"); };
    }
  });
} else {
  initAuth({
    onSignedIn: applyAuthState,
    onSignedOut: applyAuthState,
    onReady: () => _authReady(),
  });
}

// Prefetch the unavailable-list cache at boot so the first album
// popup's tracklist is rendered with the correct play/missing state
// — without this, the very first popup of a session may briefly
// show ▶ on a track whose video has been globally flagged broken.
// Called via a tiny delay so it doesn't compete with the critical-
// path auth/sync fetches.
setTimeout(() => {
  if (typeof window._sdEnsureYtUnavailableLoaded === "function") {
    window._sdEnsureYtUnavailableLoaded();
  }
}, 1500);

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
