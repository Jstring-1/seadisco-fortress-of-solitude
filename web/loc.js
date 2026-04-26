// ── LOC (Library of Congress) view ──────────────────────────────────────
//
// This file powers the /?v=loc SPA view:
//   - A search form with every filter the loc.gov JSON API supports in
//     practice (q, contributor, subject, location, language, partof,
//     date range).
//   - A results grid with per-card ▶ play and ★ save buttons.
//   - A "Saved" tab backed by /api/user/loc-saves, cross-device synced.
//   - A fixed-bottom audio bar (#loc-audio-bar) using a native <audio>
//     element. HLS streams are handled by lazy-loading hls.js from CDN
//     the first time an HLS result is played.
//   - A client-side governor that prevents rapid-fire submits / page
//     clicks from overwhelming the backend's /api/loc/search rate limit.

// ── State ────────────────────────────────────────────────────────────────

// Current view-internal tab: "search" or "saved"
let _locTab = "search";
// Last search params (so pagination / re-renders use the same filters)
let _locLastQuery = null;
// Last raw response (cached client-side so flipping between Search / Saved
// and back doesn't re-hit the server)
let _locLastResponse = null;
// Server-side saved state: set of locIds the current user has starred.
// Loaded once per page load; mutated locally as the user stars/unstars.
let _locSavedIds = null;
// Last loaded Saved tab items (for the Saved grid render)
let _locSavedItems = null;
// Current playing audio item (for the audio bar)
let _locNowPlaying = null;
// Playback queue for multi-track LOC items — enables auto-advance when
// one track ends. Shape: { items: Track[], index: number, itemId: string,
// itemTitle: string, itemImage: string }
let _locQueue = null;
// Monotonic counter used to serialize rapid _locPlay() calls so the
// most recent call wins. Fixes AbortError when switching tracks fast.
let _locPlayToken = 0;
// Saved-tab filter state
let _locSavedFilter = "";
let _locSavedSort = "recent";  // recent | title | year-asc | year-desc
let _locSavedFilterDebounce = null;

// hls.js is lazy-loaded on first HLS play to save ~100KB for users who
// never hit an HLS-only stream.
let _hlsPromise = null;
function _ensureHlsLoaded() {
  if (window.Hls) return Promise.resolve(window.Hls);
  if (_hlsPromise) return _hlsPromise;
  _hlsPromise = new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = "https://cdn.jsdelivr.net/npm/hls.js@1/dist/hls.min.js";
    s.async = true;
    s.onload = () => resolve(window.Hls);
    s.onerror = () => { _hlsPromise = null; reject(new Error("hls.js failed to load")); };
    document.head.appendChild(s);
  });
  return _hlsPromise;
}

// ── Client-side governor ────────────────────────────────────────────────
//
// Independent of the server's token bucket — this protects the user from
// themselves. Collapses rapid submit clicks / pagination clicks into one
// in-flight request, enforces a minimum gap between successive calls,
// and short-circuits obvious duplicates against a tiny last-N cache.

const LOC_MIN_GAP_MS     = 400;   // Floor between consecutive network calls
const LOC_MAX_INFLIGHT   = 1;     // Only one LOC search in flight at once
const LOC_CLIENT_CACHE_N = 12;    // Last N query strings → response bodies

let _locInflight  = 0;             // Number of in-flight LOC requests
let _locLastCall  = 0;             // Timestamp of the last completed request
let _locPending   = null;          // Promise the next caller should await if in-flight
const _locClientCache = [];        // LRU-ish list of { key, body }

function _locClientCacheGet(key) {
  const idx = _locClientCache.findIndex(e => e.key === key);
  if (idx < 0) return null;
  const [entry] = _locClientCache.splice(idx, 1);
  _locClientCache.push(entry);
  return entry.body;
}
function _locClientCacheSet(key, body) {
  _locClientCache.push({ key, body });
  while (_locClientCache.length > LOC_CLIENT_CACHE_N) _locClientCache.shift();
}

// Canonicalize a search-params object into a stable cache key.
function _locKey(params) {
  const entries = Object.entries(params || {})
    .filter(([, v]) => v !== "" && v != null)
    .sort(([a], [b]) => a.localeCompare(b));
  return entries.map(([k, v]) => `${k}=${v}`).join("&");
}

// Governed LOC search call. Rejects with Error("busy") if too many calls
// are already queued (caller should show a soft "busy" indicator).
async function _locFetchSearch(params) {
  const key = _locKey(params);

  // 1. Hit the client cache first (free, instant).
  const cached = _locClientCacheGet(key);
  if (cached) return cached;

  // 2. If there's already an in-flight call for this exact key, reuse it.
  if (_locPending && _locPending.key === key) return _locPending.promise;

  // 3. Enforce the cap.
  if (_locInflight >= LOC_MAX_INFLIGHT) {
    // Drop rapid-fire: wait for the current one to finish, then recompute.
    if (_locPending) {
      try { await _locPending.promise; } catch { /* ignore */ }
    }
  }

  // 4. Enforce minimum gap between calls.
  const now = Date.now();
  const wait = Math.max(0, LOC_MIN_GAP_MS - (now - _locLastCall));
  if (wait > 0) await new Promise(r => setTimeout(r, wait));

  // 5. Fire the request.
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params || {})) {
    if (v !== "" && v != null) qs.set(k, String(v));
  }
  _locInflight++;
  const promise = (async () => {
    try {
      const r = await apiFetch(`/api/loc/search?${qs.toString()}`);
      if (r.status === 503) {
        const body = await r.json().catch(() => ({}));
        throw new Error(body?.message || "LOC is busy — try again in a moment.");
      }
      if (!r.ok) {
        throw new Error(`Search failed (${r.status})`);
      }
      const body = await r.json();
      _locClientCacheSet(key, body);
      return body;
    } finally {
      _locInflight--;
      _locLastCall = Date.now();
      if (_locPending?.key === key) _locPending = null;
    }
  })();
  _locPending = { key, promise };
  return promise;
}

// ── View mount ──────────────────────────────────────────────────────────

async function initLocView() {
  const root = document.getElementById("loc-view");
  if (!root) return;

  // Auth gate: any signed-in user can browse LOC. Backend endpoints use
  // requireUser, so this is just a UX guard — the server is the source
  // of truth.
  if (!window._clerk?.user) {
    root.dataset.mounted = "1";
    root.innerHTML = `
      <div class="loc-empty" style="padding:3rem 1rem">
        <div style="font-size:1rem;color:var(--text);margin-bottom:0.5rem">Sign in to browse LOC.</div>
        <div style="font-size:0.82rem">The Library of Congress search is available to any signed-in SeaDisco account.</div>
      </div>
    `;
    return;
  }

  if (root.dataset.mounted === "1") {
    // Already rendered — re-sync from URL params (back/forward navigation
    // and shared deep-links land here on the second mount).
    _locApplyUrlFromAddress();
    return;
  }
  root.dataset.mounted = "1";
  root.innerHTML = _locRenderShell();

  // Wire the search form
  const form = document.getElementById("loc-search-form");
  if (form) {
    form.addEventListener("submit", (ev) => {
      ev.preventDefault();
      _locRunSearchFromForm({ resetPage: true });
    });
    // Hitting Enter in any text input submits the form — that's native
    // behavior, no extra wiring needed.
  }

  // Load saved IDs once so the ★ state is correct on first render
  _locLoadSavedIds();
  // Mount the shared saved-search dropdown next to the submit button.
  // Reuses buildSavedSearchUI() from utils.js so LOC bookmarks look and
  // feel identical to the main search and collection bookmarks.
  if (typeof buildSavedSearchUI === "function") {
    const topRow = document.getElementById("loc-form-row-top");
    if (topRow && !topRow.querySelector(".saved-search-wrap")) {
      buildSavedSearchUI(
        "loc",
        () => _locReadFormParams(),  // getParams
        (params) => {                // apply
          const setVal = (elId, key) => {
            const el = document.getElementById(elId);
            if (el) el.value = params[key] ?? "";
          };
          setVal("loc-q",           "q");
          setVal("loc-contributor", "contributor");
          setVal("loc-subject",     "subject");
          setVal("loc-location",    "location");
          setVal("loc-language",    "language");
          setVal("loc-partof",      "partof");
          setVal("loc-start-date",  "start_date");
          setVal("loc-end-date",    "end_date");
          setVal("loc-sort",        "sort");
          setVal("loc-perpage",     "c");
          // Restore the playable checkbox (default ON if the saved param is missing)
          const playableCb = document.getElementById("loc-playable");
          if (playableCb) playableCb.checked = params.playable !== "0" && params.playable !== "false";
          _locSwitchTab("search");
          _locRunSearchFromForm({ resetPage: true });
        },
        topRow,
      );
    }
  }

  // Honor any deep-link / shared URL: pre-fill the form, switch tab,
  // and auto-run a search if the URL carries any criteria.
  _locApplyUrlFromAddress();
}

// Restore the LOC view from the current address bar — used both on
// first mount (deep link / share) and re-mount (back/forward).
function _locApplyUrlFromAddress() {
  const p = new URLSearchParams(location.search);
  const tab = p.get("tab") === "saved" ? "saved" : "search";
  const hasCriteria = _locApplyUrlParamsToForm(p);
  // Set sp from URL so Load more knows where to continue from
  if (p.get("sp")) {
    // Stash on the form's hidden state via _locLastQuery (used by Load more)
    // — we set it here even before the search runs so a deep-link to page 3
    // would technically request page 3 as the first call. Most users will
    // land on page 1, so this is just plumbing.
  }
  // Don't push another history entry — we're consuming the existing one.
  _locSwitchTab(tab, { pushUrl: false });
  if (tab === "search" && hasCriteria) {
    _locRunSearchFromForm({ resetPage: !p.get("sp"), pushUrl: false });
  }
  // Deep-link: ?li=<loc URL> reopens the info popup; ?lp=<loc URL>
  // resumes playback in the bar. Both can be present (e.g. a user
  // shares a link with the popup open AND a track playing).
  const li = p.get("li");
  const lp = p.get("lp");
  if (li || lp) {
    _locResumeFromUrl(li, lp);
  }
}

async function _locResumeFromUrl(li, lp) {
  // Resolve any ids that aren't already cached. Both li and lp may
  // refer to the same item, so coalesce duplicate fetches.
  const needed = new Set();
  if (li && !_locItemCache.has(li)) needed.add(li);
  if (lp && !_locItemCache.has(lp)) needed.add(lp);
  for (const id of needed) {
    try { await _locFetchLookup(id); } catch { /* keep going */ }
  }
  if (lp && _locItemCache.has(lp)) {
    const item = _locItemCache.get(lp);
    if (item?.streamUrl) {
      // Best-effort autoplay — modern browsers may block until the
      // user interacts. The bar still appears with the right title
      // and a tap on ▶ resumes the right track.
      try { _locPlay(item); } catch {}
    }
  }
  if (li && _locItemCache.has(li)) {
    try { _locOpenInfoPopup(li); } catch {}
  }
}

function _locRenderShell() {
  return `
    <div class="loc-header">
      <h2 class="loc-title">Library of Congress</h2>
      <p class="loc-sub">Search digitized audio from the Library of Congress. Audio and metadata courtesy of the <a href="https://www.loc.gov" target="_blank" rel="noopener" class="loc-attribution-link">Library of Congress</a>. Rights vary per item — see the info popup for each recording's <em>rights_advisory</em> note before reusing any clip.</p>
      <div class="loc-tabs">
        <button type="button" class="loc-tab loc-tab-search active" onclick="_locSwitchTab('search')">Search</button>
        <button type="button" class="loc-tab loc-tab-saved" onclick="_locSwitchTab('saved')">Saved</button>
      </div>
    </div>

    <div class="loc-panel loc-panel-search">
      <form id="loc-search-form" class="loc-form" autocomplete="off">
        <div class="loc-form-row" id="loc-form-row-top">
          <input type="text" id="loc-q" placeholder="Keyword (title, subject, or any text)" />
          <button type="submit" class="loc-submit" id="loc-submit-btn">Search</button>
          <label class="loc-playable-btn" title="Playable only — hide results with no audio stream">
            <input type="checkbox" id="loc-playable" checked onchange="if(_locLastQuery)_locRunSearchFromForm({resetPage:true})" />
            <span class="loc-playable-icon">♪</span>
          </label>
          <!-- buildSavedSearchUI injects the bookmark dropdown here -->
        </div>
        <div class="loc-form-grid">
          <label><span>Contributor</span><input type="text" id="loc-contributor" placeholder="e.g. whiteman, paul" /></label>
          <label><span>Subject / genre</span><input type="text" id="loc-subject" placeholder="e.g. blues" /></label>
          <label><span>Location</span><input type="text" id="loc-location" placeholder="e.g. new york" /></label>
          <label><span>Language</span><input type="text" id="loc-language" placeholder="e.g. english" /></label>
          <label><span>Collection</span><input type="text" id="loc-partof" placeholder="e.g. national jukebox" /></label>
          <label><span>Year from</span><input type="text" id="loc-start-date" placeholder="1900" inputmode="numeric" maxlength="4" /></label>
          <label><span>Year to</span><input type="text" id="loc-end-date" placeholder="1930" inputmode="numeric" maxlength="4" /></label>
          <label class="loc-form-split"><span>Sort &middot; Per page</span>
            <div class="loc-form-split-row">
              <select id="loc-sort" onchange="if(_locLastQuery)_locRunSearchFromForm({resetPage:true})">
                <option value="relevance" selected>Relevance</option>
                <option value="date-desc">Year (newest)</option>
                <option value="date-asc">Year (oldest)</option>
                <option value="title">Title A–Z</option>
                <option value="artist">Artist A–Z (page)</option>
                <option value="artist-desc">Artist Z–A (page)</option>
              </select>
              <select id="loc-perpage" class="loc-perpage-select">
                <option value="25">25</option>
                <option value="50">50</option>
                <option value="100" selected>100</option>
              </select>
            </div>
          </label>
        </div>
      </form>
      <div id="loc-status" class="loc-status"></div>
      <div id="loc-results" class="card-grid loc-results"></div>
      <div id="loc-pagination" class="loc-pagination"></div>
    </div>

    <div class="loc-panel loc-panel-saved" style="display:none">
      <div class="loc-saved-head">
        <div class="loc-saved-title">Your saved LOC audio <span class="loc-saved-count">(<span id="loc-saved-count">0</span>)</span></div>
        <button type="button" class="loc-refresh-btn" onclick="_locLoadSaved()" title="Refresh saved list">↻</button>
      </div>
      <div class="loc-saved-toolbar">
        <input type="text" id="loc-saved-filter" class="loc-saved-filter-input" placeholder="Filter title, artist, label…" oninput="_locOnSavedFilterInput(this)" />
        <select id="loc-saved-sort" class="loc-saved-sort" onchange="_locOnSavedSortChange(this)">
          <option value="recent">Recently saved</option>
          <option value="title">Title A–Z</option>
          <option value="year-asc">Year (oldest first)</option>
          <option value="year-desc">Year (newest first)</option>
        </select>
      </div>
      <div id="loc-saved-list" class="card-grid loc-results"></div>
    </div>
  `;
}

function _locSwitchTab(tab, { pushUrl = true } = {}) {
  _locTab = tab === "saved" ? "saved" : "search";
  document.querySelectorAll(".loc-tab").forEach(btn => btn.classList.remove("active"));
  document.querySelector(`.loc-tab-${_locTab}`)?.classList.add("active");
  const searchPanel = document.querySelector(".loc-panel-search");
  const savedPanel  = document.querySelector(".loc-panel-saved");
  if (searchPanel) searchPanel.style.display = _locTab === "search" ? "" : "none";
  if (savedPanel)  savedPanel.style.display  = _locTab === "saved"  ? "" : "none";
  if (_locTab === "saved") _locLoadSaved();
  // Reflect tab in the address bar so /?v=loc&tab=saved is shareable.
  if (pushUrl && typeof history?.pushState === "function") {
    const qs = new URLSearchParams(location.search);
    qs.set("v", "loc");
    if (_locTab === "saved") qs.set("tab", "saved"); else qs.delete("tab");
    const next = "/?" + qs.toString();
    if (location.pathname + location.search !== next) {
      history.pushState({}, "", next);
    }
  }
}

// ── Search execution ────────────────────────────────────────────────────

function _locReadFormParams() {
  const val = (id) => (document.getElementById(id)?.value ?? "").trim();
  const playable = document.getElementById("loc-playable")?.checked !== false;
  return {
    q:           val("loc-q"),
    contributor: val("loc-contributor"),
    subject:     val("loc-subject"),
    location:    val("loc-location"),
    language:    val("loc-language"),
    partof:      val("loc-partof"),
    start_date:  val("loc-start-date"),
    end_date:    val("loc-end-date"),
    sort:        val("loc-sort") || "relevance",
    c:           val("loc-perpage") || "100",
    playable:    playable ? "1" : "0",
  };
}

async function _locRunSearchFromForm({ resetPage = true, pushUrl = true } = {}) {
  const params = _locReadFormParams();
  if (resetPage) params.sp = "1";
  else if (_locLastQuery?.sp) params.sp = _locLastQuery.sp;
  if (pushUrl) _locPushUrlState(params);
  return _locRunSearch(params);
}

// Mirror the form's current state into the address bar so deep-links
// and the browser back button restore the same view. We strip defaults
// (relevance / 100 per page / playable on / sp=1) for shorter URLs.
function _locPushUrlState(params) {
  const qs = new URLSearchParams({ v: "loc" });
  const skip = (k, v) => (
    v == null || v === "" ||
    (k === "sort"     && v === "relevance") ||
    (k === "c"        && v === "100")       ||
    (k === "playable" && v === "1")         ||
    (k === "sp"       && (v === "1" || v === 1))
  );
  for (const [k, v] of Object.entries(params || {})) {
    if (!skip(k, v)) qs.set(k, String(v));
  }
  // Preserve tab=saved if the user is on the saved tab
  if (_locTab === "saved") qs.set("tab", "saved");
  const next = "/?" + qs.toString();
  if (location.pathname + location.search !== next) {
    history.pushState({}, "", next);
  }
}

// Read deep-link / back-button URL params and apply to the form.
// Returns true if any meaningful search criteria are present.
function _locApplyUrlParamsToForm(p) {
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.value = v ?? ""; };
  set("loc-q",           p.get("q"));
  set("loc-contributor", p.get("contributor"));
  set("loc-subject",     p.get("subject"));
  set("loc-location",    p.get("location"));
  set("loc-language",    p.get("language"));
  set("loc-partof",      p.get("partof"));
  set("loc-start-date",  p.get("start_date"));
  set("loc-end-date",    p.get("end_date"));
  set("loc-sort",        p.get("sort") || "relevance");
  set("loc-perpage",     p.get("c")    || "100");
  const cb = document.getElementById("loc-playable");
  if (cb) cb.checked = p.get("playable") !== "0";
  return !!(p.get("q") || p.get("contributor") || p.get("subject") ||
            p.get("location") || p.get("language") || p.get("partof") ||
            p.get("start_date") || p.get("end_date"));
}

async function _locRunSearch(params, { append = false } = {}) {
  const statusEl = document.getElementById("loc-status");
  const grid     = document.getElementById("loc-results");
  const pag      = document.getElementById("loc-pagination");
  const submitBtn = document.getElementById("loc-submit-btn");
  if (!grid) return;

  // Require something — either a keyword or at least one filter
  const hasCriteria = !!(params.q || params.contributor || params.subject ||
                         params.location || params.language || params.partof ||
                         params.start_date || params.end_date);
  if (!hasCriteria) {
    statusEl.textContent = "Enter a keyword or a filter to search.";
    grid.innerHTML = "";
    pag.innerHTML = "";
    return;
  }

  _locLastQuery = params;
  if (!append) {
    statusEl.textContent = "Searching LOC…";
    grid.innerHTML = `<div class="loc-skeleton"></div>`;
    pag.innerHTML = "";
  } else {
    // Disable the in-place Load more button while the next page is fetched
    const lm = pag.querySelector(".load-more-btn");
    if (lm) { lm.disabled = true; lm.classList.add("loading"); lm.textContent = "Loading…"; }
  }
  if (submitBtn) submitBtn.disabled = true;

  try {
    const body = await _locFetchSearch(params);
    _locLastResponse = body;
    let results = Array.isArray(body?.results) ? body.results : [];
    if (!results.length) {
      if (append) {
        // Page returned empty — just hide the load-more and update status.
        const lm = pag.querySelector(".load-more-btn");
        if (lm) lm.remove();
        return;
      }
      statusEl.textContent = "No results.";
      grid.innerHTML = "";
      return;
    }
    // Hidden-count hint for the status line when playable filter is on
    const hiddenCount = body.pagination?.hiddenCount ?? 0;
    const playableOn  = !!body.pagination?.playableOnly;
    const hiddenHint = hiddenCount > 0 ? ` (${hiddenCount} hidden — no stream)` : "";

    // Client-side artist sort — LOC's `sb` API only supports relevance,
    // date, and title, so artist-order is a local reshuffle of whatever
    // the backend returned for this page. Label in the status line so
    // the user knows it's page-scoped, not a global sort across pages.
    let sortHint = "";
    if (params.sort === "artist" || params.sort === "artist-desc") {
      const key = (r) => {
        const c = Array.isArray(r.contributors) && r.contributors.length ? r.contributors[0] : "";
        // LOC contributor strings are already "last, first" — fine for sort.
        return String(c || "").toLowerCase();
      };
      results = results.slice().sort((a, b) => {
        const ka = key(a), kb = key(b);
        if (!ka && !kb) return 0;
        if (!ka) return 1;   // empties last
        if (!kb) return -1;
        return params.sort === "artist-desc" ? kb.localeCompare(ka) : ka.localeCompare(kb);
      });
      sortHint = " · sorted by artist (this page)";
    }
    // Status line — when the playable filter is active the LOC
    // from-to-of-total numbers don't match what's on screen (LOC counts
    // pre-filter; we show post-filter). Switch to a page-based summary
    // so the count is honest. When the filter is off, show LOC's own
    // "1-100 of 1501" phrasing since it matches exactly.
    let statusText;
    if (body.pagination) {
      const p = body.pagination;
      if (playableOn && hiddenCount > 0) {
        const perPage = Number(p.perpage) || 100;
        const totalPages = Math.max(1, Math.ceil((Number(p.total) || 0) / perPage));
        statusText = `Page ${p.current} of ${totalPages} · ${results.length} playable${hiddenHint}`;
      } else {
        statusText = `${p.from}-${p.to} of ${p.total} results${hiddenHint}`;
      }
    } else {
      statusText = `${results.length} results${hiddenHint}`;
    }
    statusEl.textContent = statusText + sortHint;
    const cardsHtml = results.map(_locRenderCard).join("");
    if (append) {
      // Append mode: insert the new cards before any in-grid placeholder
      // (none in practice) and re-render the load-more footer.
      grid.insertAdjacentHTML("beforeend", cardsHtml);
    } else {
      grid.innerHTML = cardsHtml;
    }
    _locRenderPagination(body.pagination);
    _locUpdatePlayingCard();  // re-apply .is-playing after grid re-render
  } catch (e) {
    statusEl.textContent = e?.message || "Search failed.";
    if (!append) grid.innerHTML = "";
    else {
      const lm = pag.querySelector(".load-more-btn");
      if (lm) { lm.disabled = false; lm.classList.remove("loading"); lm.textContent = "Load more results"; }
    }
  } finally {
    if (submitBtn) submitBtn.disabled = false;
  }
}

// Render the load-more footer matching the rest of the site (search /
// wiki / collection all use the .load-more-wrap + .load-more-btn pair).
// The button hands off to _locLoadMore which appends the next page in
// place; the URL stays on the original search so the deep-link is the
// initial query.
function _locRenderPagination(p) {
  const el = document.getElementById("loc-pagination");
  if (!el || !p) return;
  if (!p.hasNext) { el.innerHTML = ""; return; }
  el.innerHTML = `
    <div class="load-more-wrap">
      <button type="button" class="load-more-btn" onclick="_locLoadMore()">Load more results</button>
    </div>
  `;
}

async function _locLoadMore() {
  if (!_locLastQuery) return;
  const nextSp = String(Math.max(1, (Number(_locLastQuery.sp) || 1) + 1));
  const next = { ..._locLastQuery, sp: nextSp };
  await _locRunSearch(next, { append: true });
}

// ── Result card renderer ────────────────────────────────────────────────

// Item cache by LOC id — lets the info popup look up the full item
// without re-fetching. Populated in _locRunSearch and _locLoadSaved.
const _locItemCache = new Map();

function _locRenderCard(item, opts) {
  if (!item || !item.id) return "";
  const savedTab = !!(opts && opts.savedTab);
  const saved = !!_locSavedIds?.has(item.id);
  const canPlay = !!item.streamUrl;
  _locItemCache.set(item.id, item);

  const contributor = Array.isArray(item.contributors) && item.contributors.length
    ? item.contributors.join(", ")
    : "";
  const label   = item.label   || "";
  const location = item.location || "";
  const metaParts = [item.year, location].filter(Boolean);

  const esc = (v) => escHtml(String(v ?? ""));
  const idAttr  = esc(item.id);
  const titleSafe = esc(item.title || "Untitled");
  const artistSafe = esc(contributor);
  const labelSafe  = esc(label);
  const metaSafe   = metaParts.map(esc).join(" · ");
  const thumb = item.image
    ? `<img src="${esc(item.image)}" alt="${titleSafe}" loading="lazy"/>`
    : `<div class="thumb-placeholder">♪</div>`;

  // Mirror the main search page's card layout so LOC results look like
  // any other SeaDisco card. Click opens the info popup (not an external
  // link) so the user can read the summary before playing.
  //
  // Top-right badge strip is a subset — only the ★ save toggle (or trash
  // in the Saved tab). The play button is overlaid on the thumb so it's
  // discoverable without opening the popup.
  const actionBadge = savedTab
    ? `<span class="card-badge loc-remove-badge" onclick="event.preventDefault();event.stopPropagation();_locRemoveSavedFromCard(this)" title="Remove from Saved">🗑</span>`
    : `<span class="card-badge loc-save-badge${saved ? " is-saved" : ""}" onclick="event.preventDefault();event.stopPropagation();_locToggleSaveFromCard(this)" title="${saved ? "Remove from Saved" : "Save to your list"}">${saved ? "★" : "☆"}</span>`;
  const playOverlay = canPlay
    ? `<span class="loc-thumb-play" onclick="event.preventDefault();event.stopPropagation();_locPlayFromCard(this)" title="Play">▶</span>`
    : "";

  return `
    <a class="card card-type-loc card-animate" href="#" title="${titleSafe}" data-loc-id="${idAttr}" data-title="${titleSafe}" data-stream="${esc(item.streamUrl || "")}" data-stream-type="${esc(item.streamType || "")}" data-image="${esc(item.image || "")}" onclick="event.preventDefault();_locOpenInfoPopup('${idAttr.replace(/'/g, "\\'")}')">
      <div class="card-thumb-wrap">
        ${thumb}
        ${playOverlay}
        <div class="card-thumb-badges">${actionBadge}</div>
      </div>
      <div class="card-body">
        ${artistSafe ? `<div class="card-artist">${artistSafe}</div>` : ""}
        <div class="card-title">${titleSafe}</div>
        <div class="card-bottom">
          ${labelSafe ? `<div class="card-sub">${labelSafe}</div>` : ""}
          <div class="card-meta">${metaSafe}</div>
        </div>
      </div>
    </a>
  `;
}

// ── LOC info popup ──────────────────────────────────────────────────────
//
// Click any LOC card → full details panel with contributors, date,
// location, label, genres, summary, and big Play / Save / Open-on-LOC
// buttons. The popup reuses the existing shared modal overlay pattern
// (see #loc-info-overlay in index.html).
async function _locOpenInfoPopup(locId) {
  if (!locId) return;
  const overlay = document.getElementById("loc-info-overlay");
  const body    = document.getElementById("loc-info-body");
  if (!overlay || !body) return;
  // Reflect in URL so the popup is shareable. Receiver lands on
  // /?v=loc&li=<id>, _locApplyUrlFromAddress runs the resolver, and
  // the popup reopens with the same item.
  _locPushPopupUrlState(locId);
  let item = _locItemCache.get(locId);
  if (!item) {
    // Show a brief loading state, then fetch the item from the lookup proxy
    overlay.classList.add("open");
    body.innerHTML = `<div class="loc-empty">Loading…</div>`;
    try {
      item = await _locFetchLookup(locId);
    } catch { /* fall through */ }
    if (!item) {
      body.innerHTML = `<div class="loc-empty">Item no longer in cache and lookup failed.</div>`;
      return;
    }
  }

  const esc = (v) => escHtml(String(v ?? ""));
  const canPlay = !!item.streamUrl;
  const saved = !!_locSavedIds?.has(item.id);

  const contributors = Array.isArray(item.contributors) ? item.contributors : [];
  const genres       = Array.isArray(item.genres)       ? item.genres       : [];
  const subjects     = Array.isArray(item.subjects)     ? item.subjects     : [];
  const partof       = Array.isArray(item.partof)       ? item.partof       : [];
  const otherTitles  = Array.isArray(item.otherTitles)  ? item.otherTitles  : [];
  const description  = Array.isArray(item.description)  ? item.description  : [];
  const notes        = Array.isArray(item.notes)        ? item.notes        : [];
  const speakers     = Array.isArray(item.speakers)     ? item.speakers     : [];
  // Best-guess "primary collection" for the credit line. We pick the
  // most specific-looking entry (skip the generic wrapper collections
  // like "catalog" or "recorded sound research center" if we can).
  const primaryCollection = (() => {
    if (!partof.length) return "";
    const preferred = partof.find(p => /jukebox|american folklife|gerry mulligan|occupational folklife|folklife|mulligan|music division/i.test(p));
    if (preferred) return preferred;
    return partof[0];
  })();
  // Contributors rendered separately in the header with clickable
  // search magnifiers (see below), so omit from the meta grid to
  // avoid duplication.
  const metaRows = [
    ["Year / date",    item.date || item.year || ""],
    ["Label",          item.label || ""],
    ["Catalog #",      item.catalogNumber || ""],
    ["Matrix #",       item.matrixNumber || ""],
    ["Take #",         item.takeNumber || ""],
    ["Call number",    item.callNumber || ""],
    ["Location",       item.location || ""],
    ["Recording repository", item.repository || ""],
    ["Medium",         item.medium || ""],
    ["Media size",     item.mediaSize || ""],
    ["Format",         item.format || ""],
    ["Type",           item.itemType || ""],
    ["Running time",   item.runningTime || ""],
    ["Audio type",     item.audioType || ""],
    ["Language",       item.language || ""],
    ["Genres",         genres.join(", ")],
    ["Subjects",       subjects.slice(0, 8).join(", ")],
    ["Other titles",   otherTitles.join(", ")],
    ["Podcast series", item.podcastSeries || ""],
    ["Collection",     partof.slice(0, 3).join(" · ")],
    ["Published",      item.createdPublished || ""],
    ["Rights",         item.rights || ""],
    ["Stream format",  item.streamType || (canPlay ? "mp3" : "—")],
  ].filter(([, v]) => v);

  const metaHtml = metaRows.map(([k, v]) => `
    <div class="loc-info-row">
      <span class="loc-info-key">${esc(k)}</span>
      <span class="loc-info-val">${esc(v)}</span>
    </div>
  `).join("");

  const summaryHtml = item.summary
    ? `<div class="loc-info-summary">${esc(item.summary)}</div>`
    : "";

  // Tracklist — items with 2+ audio files get a numbered, clickable list.
  // Single-track items fall back to the header Play button.
  const tracks = Array.isArray(item.tracks) ? item.tracks : [];
  const trackListBlock = tracks.length >= 2
    ? `<div class="loc-info-section">
        <div class="loc-info-section-title">Tracks (${tracks.length})</div>
        <ol class="loc-tracklist">${tracks.map((t, i) => {
          const label = esc(t.title || `Track ${i + 1}`);
          const dur = t.duration ? `<span class="loc-track-dur">${esc(t.duration)}</span>` : "";
          return `<li class="loc-track-row">
            <button type="button" class="loc-track-play" onclick="_locPlayTrack('${esc(item.id).replace(/'/g, "\\'")}',${i})" title="Play this track">▶</button>
            <span class="loc-track-num">${i + 1}.</span>
            <span class="loc-track-title">${label}</span>
            ${dur}
          </li>`;
        }).join("")}</ol>
      </div>`
    : "";

  // Free-form text blocks — description (from r.description), notes
  // (from item.notes, usually release catalog info), speakers (podcast
  // contributors), and the full article (podcast transcript or blurb).
  const descBlock = description.length
    ? `<div class="loc-info-section">
        <div class="loc-info-section-title">Description</div>
        <ul class="loc-info-list">${description.map(d => `<li>${esc(d)}</li>`).join("")}</ul>
      </div>`
    : "";
  const notesBlock = notes.length
    ? `<div class="loc-info-section">
        <div class="loc-info-section-title">Notes</div>
        <ul class="loc-info-list">${notes.map(n => `<li>${esc(n)}</li>`).join("")}</ul>
      </div>`
    : "";
  // Speakers: match the main-search album credits style — name is a
  // link (clicks search LOC by contributor), followed by two compact
  // magnifying-glass icons: 🔎 Discogs (main search), 🔍 Collection.
  // The LOC glass is omitted because the name itself already searches
  // LOC — it would be redundant. Entries are comma-separated inline
  // like credits, not stacked.
  const speakersBlock = speakers.length
    ? `<div class="loc-info-section">
        <div class="loc-info-section-title">Speakers / participants</div>
        <div class="loc-speaker-credits">${speakers.map(s => {
          const n = esc(s);
          const jsName = n.replace(/'/g, "\\'");
          return `<a href="#" class="credit-name loc-credit-name" onclick="event.preventDefault();_locSearchByName('${jsName}')" title="Search LOC for ${n}">${n}</a><a href="#" class="album-title-search loc-credit-discogs" onclick="event.preventDefault();_locSearchDiscogsByName('${jsName}')" title="Search Discogs for ${n}">⌕</a><a href="#" class="album-title-search loc-credit-collection" onclick="event.preventDefault();_locSearchCollectionByName('${jsName}')" title="Search your collection for ${n}">⌕</a>`;
        }).join('<span class="credit-sep"> · </span>')}</div>
      </div>`
    : "";
  // Article body — podcast transcript / blurb. Shown inside a scrollable
  // block so long transcripts don't blow the popup height.
  const articleBlock = item.article
    ? `<div class="loc-info-section">
        <div class="loc-info-section-title">Article / transcript</div>
        <div class="loc-info-article">${esc(item.article)}</div>
      </div>`
    : "";

  const imgTag = item.image
    ? `<img class="loc-info-thumb" src="${esc(item.image)}" alt=""/>`
    : `<div class="loc-info-thumb loc-info-thumb-ph">♪</div>`;

  const playBtn = canPlay
    ? `<button type="button" class="loc-info-btn loc-info-btn-play" onclick="_locPlayFromInfo('${esc(item.id).replace(/'/g, "\\'")}')">▶ Play</button>`
    : `<button type="button" class="loc-info-btn loc-info-btn-play is-disabled" disabled title="No playable stream">▶ No stream</button>`;
  const queueBtn = canPlay
    ? `<button type="button" class="loc-info-btn loc-info-btn-queue" onclick="_locQueueFromInfo('${esc(item.id).replace(/'/g, "\\'")}')" title="Add to play queue">＋ Queue</button>`
    : "";
  const saveBtn = `<button type="button" class="loc-info-btn loc-info-btn-save${saved ? " is-saved" : ""}" onclick="_locToggleSaveFromInfo('${esc(item.id).replace(/'/g, "\\'")}')">${saved ? "★ Saved" : "☆ Save"}</button>`;
  const locLink = `<a class="loc-info-btn loc-info-btn-loc" href="${esc(item.url || item.id)}" target="_blank" rel="noopener">Open on loc.gov ↗</a>`;

  // Credit line — "Library of Congress, [Collection name]." format from
  // LOC's own attribution guidance. Shown as a muted footer so any future
  // redistribution has the right citation on hand.
  const creditLine = primaryCollection
    ? `Library of Congress, ${primaryCollection.replace(/\b\w/g, (c) => c.toUpperCase())}.`
    : `Library of Congress.`;

  // Build clickable title + artist line with search magnifiers.
  // Title click runs a new LOC keyword search; the ⌕ icons also search
  // Discogs and the Collection. Contributors are split so each name
  // gets its own clickable link + magnifiers (same pattern as the
  // album-credits in the main search modal).
  const titleRaw = item.title || "Untitled";
  const titleJs  = esc(titleRaw).replace(/'/g, "\\'");
  const titleEl  = `
    <div class="loc-info-title">
      <a href="#" class="loc-title-link" onclick="event.preventDefault();_locSearchByKeyword('${titleJs}')" title="Search LOC for this title">${esc(titleRaw)}</a>
      <a href="#" class="album-title-search loc-credit-discogs" onclick="event.preventDefault();_locSearchDiscogsByName('${titleJs}')" title="Search Discogs for this title">⌕</a>
      <a href="#" class="album-title-search loc-credit-collection" onclick="event.preventDefault();_locSearchCollectionByName('${titleJs}')" title="Search your collection for this title">⌕</a>
    </div>`;
  const artistEl = contributors.length
    ? `<div class="loc-info-artist">${contributors.map(c => {
        const n = esc(c);
        const jsN = n.replace(/'/g, "\\'");
        return `<a href="#" class="credit-name loc-credit-name" onclick="event.preventDefault();_locSearchByName('${jsN}')" title="Search LOC for ${n}">${n}</a><a href="#" class="album-title-search loc-credit-discogs" onclick="event.preventDefault();_locSearchDiscogsByName('${jsN}')" title="Search Discogs for ${n}">⌕</a><a href="#" class="album-title-search loc-credit-collection" onclick="event.preventDefault();_locSearchCollectionByName('${jsN}')" title="Search your collection for ${n}">⌕</a>`;
      }).join('<span class="credit-sep"> · </span>')}</div>`
    : "";

  body.innerHTML = `
    <div class="loc-info-head">
      ${imgTag}
      <div class="loc-info-head-text">
        ${titleEl}
        ${artistEl}
        <div class="loc-info-actions">${playBtn}${queueBtn}${saveBtn}${locLink}</div>
      </div>
    </div>
    ${summaryHtml}
    ${trackListBlock}
    ${speakersBlock}
    ${descBlock}
    ${notesBlock}
    ${articleBlock}
    <div class="loc-info-meta">${metaHtml}</div>
    <div class="loc-info-credit">
      <div class="loc-info-credit-label">Credit</div>
      <div class="loc-info-credit-text">${esc(creditLine)}</div>
    </div>
  `;
  overlay.classList.add("open");
}

function _locCloseInfoPopup() {
  document.getElementById("loc-info-overlay")?.classList.remove("open");
  // Strip the popup param from the URL so the back-button doesn't
  // re-open it; keep `lp=` if a track is currently playing.
  _locPushPopupUrlState(null);
}

// ── URL state for shareable popup + playback ─────────────────────────
// `li=<full loc URL>` → reopen the info popup on landing.
// `lp=<full loc URL>` → resume playback on landing (best-effort; mobile
//   browsers may require a tap before audio actually plays).
// Both params are kept in sync via small helpers so the address bar
// always reflects the current popup + bar state.
function _locPushPopupUrlState(locIdOrNull) {
  if (typeof history?.replaceState !== "function") return;
  const qs = new URLSearchParams(location.search);
  qs.set("v", "loc");
  if (locIdOrNull) qs.set("li", locIdOrNull); else qs.delete("li");
  const next = "/?" + qs.toString();
  if (location.pathname + location.search !== next) {
    history.replaceState({}, "", next);
  }
}
function _locPushPlayUrlState(locIdOrNull) {
  if (typeof history?.replaceState !== "function") return;
  const qs = new URLSearchParams(location.search);
  qs.set("v", "loc");
  if (locIdOrNull) qs.set("lp", locIdOrNull); else qs.delete("lp");
  const next = "/?" + qs.toString();
  if (location.pathname + location.search !== next) {
    history.replaceState({}, "", next);
  }
}

// Fetch a single LOC item by id via the server proxy. Adds to the
// item cache. Returns the normalized item, or null on failure.
async function _locFetchLookup(locId) {
  if (!locId) return null;
  try {
    const r = await apiFetch(`/api/loc/lookup?id=${encodeURIComponent(locId)}`);
    if (!r.ok) return null;
    const j = await r.json();
    const item = j?.item;
    if (item && item.id) {
      _locItemCache.set(item.id, item);
      return item;
    }
  } catch { /* fall through */ }
  return null;
}

// Add the LOC item from the info popup to the cross-source play queue.
// Falls back to a server lookup if the item isn't in the local cache —
// covers the case where the popup is opened from a deep-link or saved
// tab and queueAddLoc fires before the cache is populated.
async function _locQueueFromInfo(locId) {
  let item = _locItemCache.get(locId);
  if (!item && typeof _locFetchLookup === "function") {
    try { item = await _locFetchLookup(locId); } catch {}
  }
  if (!item) {
    if (typeof showToast === "function") showToast("Could not load item", "error");
    return;
  }
  if (typeof queueAddLoc === "function") queueAddLoc(item);
  else if (typeof showToast === "function") showToast("Queue not available", "error");
}
window._locQueueFromInfo = _locQueueFromInfo;

function _locPlayFromInfo(locId) {
  const item = _locItemCache.get(locId);
  if (!item) return;
  // Multi-track items start a queue at track 0 so auto-advance works.
  if (Array.isArray(item.tracks) && item.tracks.length >= 2) {
    _locStartQueue(item, 0);
  } else if (item.streamUrl) {
    _locQueue = null;
    _locUpdateQueueButtons();
    _locPlay(item);
  }
}

// Play a specific track from a multi-track LOC item. Starts a queue at
// the clicked index so subsequent tracks auto-advance.
function _locPlayTrack(locId, trackIndex) {
  const item = _locItemCache.get(locId);
  if (!item || !Array.isArray(item.tracks)) return;
  _locStartQueue(item, trackIndex);
}

// Build a queue from a multi-track item and start playing at `index`.
function _locStartQueue(item, index) {
  const tracks = Array.isArray(item.tracks) ? item.tracks : [];
  if (!tracks.length) return;
  const safeIdx = Math.max(0, Math.min(index, tracks.length - 1));
  _locQueue = {
    items: tracks,
    index: safeIdx,
    itemId: item.id,
    itemTitle: item.title || "Untitled",
    itemImage: item.image || "",
  };
  _locPlayQueueCurrent();
}

// Play the current queue index — called by _locStartQueue, prev, next,
// and the onended auto-advance handler.
function _locPlayQueueCurrent() {
  if (!_locQueue) return;
  const track = _locQueue.items[_locQueue.index];
  if (!track?.url) return;
  const total = _locQueue.items.length;
  const pos   = _locQueue.index + 1;
  const trackTitle = track.title
    ? `${_locQueue.itemTitle} — ${track.title}`
    : `${_locQueue.itemTitle} (${pos}/${total})`;
  _locPlay({
    id: `${_locQueue.itemId}#${_locQueue.index}`,
    title: trackTitle,
    streamUrl: track.url,
    streamType: track.streamType || "mp3",
    image: _locQueue.itemImage,
  });
  _locUpdateQueueButtons();
}

function _locPlayNextInQueue() {
  if (!_locQueue) return;
  if (_locQueue.index + 1 >= _locQueue.items.length) return;
  _locQueue.index++;
  _locPlayQueueCurrent();
}

function _locPlayPrevInQueue() {
  if (!_locQueue) return;
  if (_locQueue.index <= 0) return;
  _locQueue.index--;
  _locPlayQueueCurrent();
}

function _locUpdateQueueButtons() {
  // Unified bar — prev/next live on the .mini-player controls and are
  // dispatched via playerPrev / playerNext when LOC is the engine.
  const prev = document.getElementById("mini-prev");
  const next = document.getElementById("mini-next");
  if (prev && next) {
    const hasQueue = !!_locQueue && _locQueue.items.length >= 2;
    prev.disabled = !hasQueue || _locQueue.index <= 0;
    next.disabled = !hasQueue || _locQueue.index + 1 >= _locQueue.items.length;
  }
  // Info / save buttons — enabled whenever something is playing
  const info = document.getElementById("mini-loc-info");
  const save = document.getElementById("mini-loc-save");
  const baseId = _locCurrentBarItemId();
  if (info) info.disabled = !baseId;
  if (save) {
    save.disabled = !baseId;
    const isSaved = !!(baseId && _locSavedIds?.has(baseId));
    save.classList.toggle("is-saved", isSaved);
    save.textContent = isSaved ? "★" : "☆";
    save.title = isSaved ? "Remove from Saved" : "Save this item";
  }
  _locUpdatePlayingCard();
}

// Figure out which LOC item is currently playing in the bar. Multi-track
// plays use a synthetic id "{itemId}#{trackIndex}" so we strip the tail,
// but queue state is the authoritative source when it's active.
function _locCurrentBarItemId() {
  if (_locQueue && _locQueue.itemId) return _locQueue.itemId;
  if (_locNowPlaying && typeof _locNowPlaying.id === "string") {
    const hash = _locNowPlaying.id.indexOf("#");
    return hash >= 0 ? _locNowPlaying.id.slice(0, hash) : _locNowPlaying.id;
  }
  return "";
}

// Mark the card whose LOC id matches the currently-playing item with
// .is-playing so the user can see where they are in the grid. Also
// clears the class from every other card. Called whenever play state
// changes (via _locUpdateQueueButtons).
function _locUpdatePlayingCard() {
  const baseId = _locCurrentBarItemId();
  // Remove from anything that isn't the current item
  document.querySelectorAll(".card.is-playing").forEach(el => {
    if (el.dataset.locId !== baseId) el.classList.remove("is-playing");
  });
  if (!baseId) return;
  // Add to every card matching the current item (could appear in both
  // Search and Saved grids if they're rendered simultaneously)
  document.querySelectorAll(`.card[data-loc-id="${CSS.escape(baseId)}"]`).forEach(el => {
    el.classList.add("is-playing");
  });
}

// Bar button: open the info popup for whatever is playing.
function _locOpenFromBar() {
  const id = _locCurrentBarItemId();
  if (!id) return;
  _locOpenInfoPopup(id);
}

// Bar button: toggle save for whatever is playing.
async function _locToggleSaveFromBar() {
  const id = _locCurrentBarItemId();
  if (!id) return;
  // Reuse the popup save path — it already handles the full item cache
  // lookup, payload construction, and state sync across card + popup.
  await _locToggleSaveFromInfo(id);
  _locUpdateQueueButtons();
}

// Speaker / participant name → new LOC search by contributor.
function _locSearchByName(name) {
  if (!name) return;
  _locCloseInfoPopup();
  // Swap to the Search tab and populate the form
  _locSwitchTab("search");
  const contributorEl = document.getElementById("loc-contributor");
  const qEl           = document.getElementById("loc-q");
  if (contributorEl) contributorEl.value = name;
  if (qEl) qEl.value = "";
  _locRunSearchFromForm({ resetPage: true });
}

// Title / keyword → new LOC search by `q`. Clears other filters so
// it doesn't inherit the previous form state (which would be confusing
// when you're chasing a specific title).
function _locSearchByKeyword(q) {
  if (!q) return;
  _locCloseInfoPopup();
  _locSwitchTab("search");
  const fields = ["loc-q", "loc-contributor", "loc-subject", "loc-location", "loc-language", "loc-partof", "loc-start-date", "loc-end-date"];
  fields.forEach(id => { const el = document.getElementById(id); if (el) el.value = ""; });
  const qEl = document.getElementById("loc-q");
  if (qEl) qEl.value = q;
  _locRunSearchFromForm({ resetPage: true });
}

// Name → main Discogs search view. Uses the existing SPA search flow.
function _locSearchDiscogsByName(name) {
  if (!name) return;
  _locCloseInfoPopup();
  if (typeof switchView !== "function") { location.href = "/?q=" + encodeURIComponent(name); return; }
  switchView("search");
  // Populate the main search input and submit
  setTimeout(() => {
    const input = document.getElementById("query") || document.querySelector("#main-search-form input[name='q']") || document.querySelector("#main-search-form input[type='text']");
    if (input) {
      input.value = name;
      input.form?.requestSubmit?.() || (typeof doSearch === "function" && doSearch(1));
    } else if (typeof doSearch === "function") {
      doSearch(1);
    }
  }, 50);
}

// Name → Collection filter view. Routes through records:collection with
// the cw-query set to the name.
function _locSearchCollectionByName(name) {
  if (!name) return;
  _locCloseInfoPopup();
  if (typeof switchView !== "function") { location.href = "/?v=collection&q=" + encodeURIComponent(name); return; }
  if (typeof window !== "undefined") window._cwTab = "collection";
  switchView("records");
  setTimeout(() => {
    const cwInput = document.getElementById("cw-query");
    if (cwInput) {
      cwInput.value = name;
      cwInput.dispatchEvent(new Event("input", { bubbles: true }));
    }
  }, 80);
}

async function _locToggleSaveFromInfo(locId) {
  const item = _locItemCache.get(locId);
  if (!item) return;
  // Reuse the toggle logic by constructing a temporary fake card and
  // driving the existing handler. Simpler: inline the save/unsave here.
  const saving = !_locSavedIds?.has(locId);
  if (!_locSavedIds) _locSavedIds = new Set();
  if (saving) _locSavedIds.add(locId); else _locSavedIds.delete(locId);
  try {
    if (saving) {
      const r = await apiFetch("/api/user/loc-saves", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          locId,
          title: item.title || "",
          streamUrl: item.streamUrl || "",
          // Spread the full cached item so every extended field is
          // persisted — same pattern as the card-path save.
          data: { ...item, id: locId },
        }),
      });
      if (!r.ok) throw new Error(`save failed (${r.status})`);
      showToast?.("Saved to LOC list");
    } else {
      const r = await apiFetch("/api/user/loc-saves", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ locId }),
      });
      if (!r.ok) throw new Error(`remove failed (${r.status})`);
      showToast?.("Removed from Saved");
      if (_locSavedItems) _locSavedItems = _locSavedItems.filter(i => i.id !== locId);
      if (_locTab === "saved") _locRenderSavedGrid();
    }
    // Re-render the popup so the save button + card star reflect new state
    _locOpenInfoPopup(locId);
    // Also re-sync any on-page card for this id
    document.querySelectorAll(`.card[data-loc-id="${CSS.escape(locId)}"] .loc-save-badge`).forEach(el => {
      el.classList.toggle("is-saved", saving);
      el.textContent = saving ? "★" : "☆";
      el.title = saving ? "Remove from Saved" : "Save to your list";
    });
  } catch (e) {
    if (saving) _locSavedIds.delete(locId); else _locSavedIds.add(locId);
    showToast?.(e?.message || "Action failed", "error");
  }
}

// Harvest data-* attributes from a card element into the shape the audio
// bar / save API expects.
function _locReadCard(el) {
  if (!el) return null;
  return {
    id:         el.dataset.locId,
    title:      el.dataset.title,
    streamUrl:  el.dataset.stream,
    streamType: el.dataset.streamType,
    image:      el.dataset.image,
  };
}

async function _locToggleSaveFromCard(btn) {
  const card = btn?.closest(".loc-card, .card");
  if (!card) return;
  const locId = card.dataset.locId;
  if (!locId) return;
  const saving = !btn.classList.contains("is-saved");
  // Optimistic toggle
  btn.classList.toggle("is-saved", saving);
  btn.textContent = saving ? "★" : "☆";
  btn.title = saving ? "Remove from Saved" : "Save to your list";
  if (!_locSavedIds) _locSavedIds = new Set();
  if (saving) _locSavedIds.add(locId); else _locSavedIds.delete(locId);
  try {
    if (saving) {
      // Prefer the full cached item (has contributors, rights, partof,
      // etc.) — fall back to just the card data attrs if cache is cold.
      // We spread the full object so every extended field (subjects,
      // description, notes, speakers, medium, call number, etc.) is
      // carried into the Saved record automatically.
      const full = _locItemCache.get(locId) || {};
      const payload = {
        locId,
        title: card.dataset.title || full.title || "",
        streamUrl: card.dataset.stream || full.streamUrl || "",
        data: {
          ...full,
          id: locId,
          title: card.dataset.title || full.title || "",
          streamUrl: card.dataset.stream || full.streamUrl || "",
          streamType: card.dataset.streamType || full.streamType || "",
          image: card.dataset.image || full.image || "",
          url: full.url || locId,
        },
      };
      const r = await apiFetch("/api/user/loc-saves", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!r.ok) throw new Error(`save failed (${r.status})`);
      showToast?.("Saved to LOC list");
    } else {
      const r = await apiFetch("/api/user/loc-saves", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ locId }),
      });
      if (!r.ok) throw new Error(`remove failed (${r.status})`);
      showToast?.("Removed from Saved");
      // Keep the in-memory Saved list in sync so the Saved tab stays
      // consistent when it's next shown (or re-rendered now).
      if (_locSavedItems) _locSavedItems = _locSavedItems.filter(i => i.id !== locId);
      if (_locTab === "saved") _locRenderSavedGrid();
    }
  } catch (e) {
    // Revert optimistic toggle
    btn.classList.toggle("is-saved", !saving);
    btn.textContent = !saving ? "★" : "☆";
    if (saving) _locSavedIds.delete(locId); else _locSavedIds.add(locId);
    showToast?.(e?.message || "Action failed", "error");
  }
}

// ── Saved tab ───────────────────────────────────────────────────────────

async function _locLoadSavedIds() {
  try {
    const r = await apiFetch("/api/user/loc-saves/ids");
    if (!r.ok) return;
    const body = await r.json();
    _locSavedIds = new Set(Array.isArray(body?.ids) ? body.ids : []);
    // Re-sync star state on any already-rendered cards
    document.querySelectorAll(".loc-card").forEach(card => {
      const id = card.dataset.locId;
      const btn = card.querySelector(".loc-save-btn");
      if (!id || !btn) return;
      const on = _locSavedIds.has(id);
      btn.classList.toggle("is-saved", on);
      btn.textContent = on ? "★" : "☆";
    });
  } catch { /* silent */ }
}

async function _locLoadSaved() {
  const grid = document.getElementById("loc-saved-list");
  if (!grid) return;
  grid.innerHTML = `<div class="loc-skeleton"></div>`;
  try {
    const r = await apiFetch("/api/user/loc-saves");
    if (!r.ok) throw new Error(`load failed (${r.status})`);
    const body = await r.json();
    const items = Array.isArray(body?.items) ? body.items : [];
    _locSavedItems = items.map(row => ({
      ...(row.data || {}),
      id:        row.locId,
      title:     row.title || row.data?.title || "",
      streamUrl: row.streamUrl || row.data?.streamUrl || "",
      _savedAt:  row.savedAt ? new Date(row.savedAt).getTime() : 0,
    }));
    // Ensure the in-memory saved-ID set matches
    if (!_locSavedIds) _locSavedIds = new Set();
    for (const it of _locSavedItems) _locSavedIds.add(it.id);
    _locRenderSavedGrid();
  } catch (e) {
    grid.innerHTML = `<div class="loc-empty">Could not load saved items: ${escHtml(e?.message || "unknown error")}</div>`;
  }
}

// Render the saved grid from the in-memory _locSavedItems list using the
// current filter + sort state. Safe to call whenever filter/sort changes
// without re-fetching from the server.
function _locRenderSavedGrid() {
  const grid = document.getElementById("loc-saved-list");
  const countEl = document.getElementById("loc-saved-count");
  if (!grid) return;
  const items = Array.isArray(_locSavedItems) ? _locSavedItems : [];
  if (countEl) countEl.textContent = String(items.length);

  if (!items.length) {
    grid.innerHTML = `<div class="loc-empty">No saves yet. Tap the ☆ on any result to build your list.</div>`;
    return;
  }

  // Filter
  const q = (_locSavedFilter || "").toLowerCase().trim();
  let filtered = items;
  if (q) {
    filtered = items.filter(it => {
      const hay = [
        it.title,
        Array.isArray(it.contributors) ? it.contributors.join(" ") : "",
        it.label,
        it.location,
        Array.isArray(it.genres) ? it.genres.join(" ") : "",
        it.year,
      ].map(v => String(v ?? "").toLowerCase()).join(" | ");
      return hay.includes(q);
    });
  }

  // Sort — make a copy so we don't mutate the master list order
  const sorted = filtered.slice();
  switch (_locSavedSort) {
    case "title":
      sorted.sort((a, b) => String(a.title || "").localeCompare(String(b.title || "")));
      break;
    case "year-asc":
      sorted.sort((a, b) => (parseInt(a.year) || 9999) - (parseInt(b.year) || 9999));
      break;
    case "year-desc":
      sorted.sort((a, b) => (parseInt(b.year) || 0) - (parseInt(a.year) || 0));
      break;
    case "recent":
    default:
      sorted.sort((a, b) => (b._savedAt || 0) - (a._savedAt || 0));
      break;
  }

  if (!sorted.length) {
    grid.innerHTML = `<div class="loc-empty">No saved items match "${escHtml(q)}".</div>`;
    return;
  }
  grid.innerHTML = sorted.map(it => _locRenderCard(it, { savedTab: true })).join("");
  _locUpdatePlayingCard();  // highlight the card currently in the bar
}

function _locOnSavedFilterInput(el) {
  // Debounce keystrokes so typing fast doesn't thrash the DOM
  clearTimeout(_locSavedFilterDebounce);
  _locSavedFilterDebounce = setTimeout(() => {
    _locSavedFilter = el.value || "";
    _locRenderSavedGrid();
  }, 160);
}

function _locOnSavedSortChange(el) {
  _locSavedSort = el.value || "recent";
  _locRenderSavedGrid();
}

// Explicit trash action from the Saved tab — asks for confirmation,
// then deletes the item from the server and re-renders.
async function _locRemoveSavedFromCard(btn) {
  const card = btn?.closest(".loc-card");
  if (!card) return;
  const locId = card.dataset.locId;
  const title = card.dataset.title || "this item";
  if (!locId) return;
  if (!confirm(`Remove "${title}" from your saved list?`)) return;
  btn.disabled = true;
  try {
    const r = await apiFetch("/api/user/loc-saves", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ locId }),
    });
    if (!r.ok) throw new Error(`remove failed (${r.status})`);
    // Update state + re-render
    if (_locSavedIds) _locSavedIds.delete(locId);
    if (_locSavedItems) _locSavedItems = _locSavedItems.filter(it => it.id !== locId);
    _locRenderSavedGrid();
    showToast?.("Removed from Saved");
  } catch (e) {
    btn.disabled = false;
    showToast?.(e?.message || "Remove failed", "error");
  }
}

// ── Audio bar ───────────────────────────────────────────────────────────

async function _locPlay(item) {
  if (!item?.streamUrl) return;
  // Unified persistent bar — was a separate #loc-audio-bar element;
  // now LOC playback shares the .mini-player chrome with YouTube and
  // dispatches via window._currentEngine.
  const bar     = document.getElementById("mini-player");
  const audio   = document.getElementById("loc-audio");
  const titleEl = document.getElementById("mini-player-title");
  if (!bar || !audio) return;

  // Each _locPlay call gets a monotonic token. If a newer call arrives
  // while this one is still async-setting-up (fetching hls.js, etc.),
  // this call aborts so the newest wins. Fixes the "playback fails when
  // I click another track mid-play" AbortError race.
  const myToken = ++_locPlayToken;
  const isCurrent = () => myToken === _locPlayToken;

  // Stop any YouTube playback so we don't double-play
  try { if (typeof closeVideo === "function") closeVideo(); } catch {}

  // Attach auto-advance + play/pause-icon handlers once per page load.
  if (!audio._locEndedBound) {
    audio.addEventListener("ended", _locOnTrackEnded);
    audio.addEventListener("play",  _locUpdatePlayPauseBtn);
    audio.addEventListener("pause", _locUpdatePlayPauseBtn);
    audio.addEventListener("ended", _locUpdatePlayPauseBtn);
    audio._locEndedBound = true;
  }

  _locNowPlaying = item;
  if (titleEl) titleEl.textContent = item.title || "Playing…";
  bar.classList.add("open");
  if (typeof _setPlayerEngine === "function") _setPlayerEngine("loc");
  document.body.classList.add("player-open");
  // Reflect now-playing in the URL — share/copy the link and the
  // recipient lands with the same track queued in the bar.
  _locPushPlayUrlState(item.id);
  _locUpdateQueueButtons();

  // Tear down any prior hls.js instance cleanly
  if (audio._hls) {
    try { audio._hls.destroy(); } catch {}
    audio._hls = null;
  }
  // Pausing before switching src tells the browser "this is intentional,
  // don't reject the in-flight play promise with AbortError". We swallow
  // the pre-existing play promise separately below.
  try { audio.pause(); } catch {}

  const isHls = item.streamType === "hls" || /\.m3u8(\?|$)/i.test(item.streamUrl);
  if (isHls) {
    if (audio.canPlayType("application/vnd.apple.mpegurl")) {
      audio.src = item.streamUrl;
    } else {
      try {
        const Hls = await _ensureHlsLoaded();
        // A newer play() may have fired while hls.js was loading. Bail.
        if (!isCurrent()) return;
        if (Hls.isSupported()) {
          const hls = new Hls();
          hls.loadSource(item.streamUrl);
          hls.attachMedia(audio);
          audio._hls = hls;
        } else {
          showToast?.("This browser can't play HLS streams.", "error");
          return;
        }
      } catch (e) {
        showToast?.("Failed to load HLS player.", "error");
        return;
      }
    }
  } else {
    audio.src = item.streamUrl;
  }

  // Bail if a newer play() has arrived during the async src assignment.
  if (!isCurrent()) return;

  // play() returns a Promise that rejects with AbortError if the element
  // is paused/re-sourced before it resolves. That's expected during rapid
  // switching — we only surface NON-abort errors.
  try {
    await audio.play();
  } catch (err) {
    if (err?.name !== "AbortError") {
      showToast?.("Playback failed: " + (err?.message || "unknown"), "error");
    }
  }
}

// Toggle native pause/play from the custom play-pause button so the LOC
// bar can hide the browser's default <audio controls> chrome and still
// expose play/pause in a way that matches the YouTube mini-player.
function _locTogglePause() {
  const audio = document.getElementById("loc-audio");
  if (!audio || !audio.src) return;
  if (audio.paused) {
    audio.play().catch(() => {});
  } else {
    audio.pause();
  }
}

// Update the ⏸/▶ icon on the play-pause button based on the current
// audio state. Wired to the audio element's play/pause/ended events
// once per page (lazy-bound on first _locPlay).
function _locUpdatePlayPauseBtn() {
  // Unified bar — same playpause button is used for both engines.
  // Only update its icon when LOC is the active engine; let YT manage
  // its own icon updates while it's active.
  if (window._currentEngine && window._currentEngine !== "loc") return;
  const btn = document.getElementById("mini-playpause");
  const audio = document.getElementById("loc-audio");
  if (!btn || !audio) return;
  const playing = !audio.paused && !audio.ended;
  btn.innerHTML = playing ? "&#9208;" : "&#9654;";
  btn.title = playing ? "Pause" : "Play";
}

// Toggle the expanded scrubber panel — delegates to the unified
// mini-player expand toggle so both engines use the same control.
function _locToggleExpand() {
  if (typeof toggleMiniPlayer === "function") toggleMiniPlayer();
}

// Auto-advance: when the current track ends, the cross-source queue
// takes precedence. If there's an item in the user's saved queue,
// hand off to it (LOC or YouTube source). Otherwise fall back to the
// internal multi-track LOC queue (for albums with multiple tracks
// inside a single LOC item).
async function _locOnTrackEnded() {
  if (typeof _queuePlayNext === "function") {
    const handled = await _queuePlayNext();
    if (handled) return;
  }
  if (!_locQueue) return;
  if (_locQueue.index + 1 >= _locQueue.items.length) {
    _locUpdateQueueButtons();
    return;
  }
  _locQueue.index++;
  _locPlayQueueCurrent();
}

// Clicking a card's ▶ overlay on a multi-track item should start the
// full queue, not just track 1 + end. Update this path too.
function _locPlayFromCard(btn) {
  const card = btn?.closest(".loc-card, .card");
  if (!card) return;
  const locId = card.dataset.locId;
  const item = locId ? _locItemCache.get(locId) : null;
  if (item && Array.isArray(item.tracks) && item.tracks.length >= 2) {
    _locStartQueue(item, 0);
    return;
  }
  const readItem = _locReadCard(card);
  if (!readItem?.streamUrl) return;
  _locQueue = null;
  _locUpdateQueueButtons();
  _locPlay(readItem);
}

function _locClosePlayer() {
  const bar = document.getElementById("mini-player");
  const audio = document.getElementById("loc-audio");
  if (audio) {
    audio.pause();
    audio.removeAttribute("src");
    audio.load();
    if (audio._hls) {
      try { audio._hls.destroy(); } catch {}
      audio._hls = null;
    }
  }
  // Only hide the bar when LOC is the active engine — if YT is playing
  // we're just stopping the LOC handoff, not the bar itself.
  if (bar && window._currentEngine === "loc") {
    bar.classList.remove("open", "expanded");
    document.body.classList.remove("player-open");
    if (typeof _setPlayerEngine === "function") _setPlayerEngine(null);
  }
  _locNowPlaying = null;
  _locQueue = null;
  _locUpdateQueueButtons();
  // Drop the now-playing param from the URL so the shareable link
  // doesn't keep pointing at a track the user has stopped.
  _locPushPlayUrlState(null);
}

// Close the info popup with Escape
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") _locCloseInfoPopup();
});

// Expose globals for inline onclick handlers
window.initLocView              = initLocView;
window._locSwitchTab            = _locSwitchTab;
window._locPlay                 = _locPlay;
window._locPlayFromCard         = _locPlayFromCard;
window._locToggleSaveFromCard   = _locToggleSaveFromCard;
window._locRemoveSavedFromCard  = _locRemoveSavedFromCard;
window._locOnSavedFilterInput   = _locOnSavedFilterInput;
window._locOnSavedSortChange    = _locOnSavedSortChange;
window._locLoadMore             = _locLoadMore;
window._locLoadSaved            = _locLoadSaved;
window._locClosePlayer          = _locClosePlayer;
window._locTogglePause          = _locTogglePause;
window._locToggleExpand         = _locToggleExpand;
window._locRunSearchFromForm    = _locRunSearchFromForm;
window._locOpenInfoPopup        = _locOpenInfoPopup;
window._locCloseInfoPopup       = _locCloseInfoPopup;
window._locPlayFromInfo         = _locPlayFromInfo;
window._locPlayTrack            = _locPlayTrack;
window._locPlayNextInQueue      = _locPlayNextInQueue;
window._locPlayPrevInQueue      = _locPlayPrevInQueue;
window._locOpenFromBar          = _locOpenFromBar;
window._locToggleSaveFromBar    = _locToggleSaveFromBar;
window._locToggleSaveFromInfo   = _locToggleSaveFromInfo;
window._locSearchByName         = _locSearchByName;
window._locSearchByKeyword      = _locSearchByKeyword;
window._locSearchDiscogsByName  = _locSearchDiscogsByName;
window._locSearchCollectionByName = _locSearchCollectionByName;
