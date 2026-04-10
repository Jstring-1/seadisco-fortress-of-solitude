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

  // Admin-only feature. shared.js sets window._isAdmin = true after
  // /api/me confirms the current session. If that hasn't resolved yet,
  // hit /api/me directly before rendering so a deep-link (?v=loc) can't
  // briefly show the LOC UI to non-admins. Backend endpoints are also
  // gated with requireAdmin — this is a layered defense.
  if (!window._isAdmin) {
    try {
      const r = await apiFetch("/api/me");
      if (r.ok) {
        const data = await r.json();
        if (data?.isAdmin) window._isAdmin = true;
      }
    } catch { /* fall through to deny */ }
  }
  if (!window._isAdmin) {
    root.dataset.mounted = "1";
    root.innerHTML = `
      <div class="loc-empty" style="padding:3rem 1rem">
        <div style="font-size:1rem;color:var(--text);margin-bottom:0.5rem">LOC is currently admin-only.</div>
        <div style="font-size:0.82rem">This section is a personal workspace and isn't open to other accounts.</div>
      </div>
    `;
    return;
  }

  if (root.dataset.mounted === "1") {
    // Already rendered — just make sure the tab state is correct
    _locSwitchTab(_locTab);
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

  // If the user linked straight to a saved tab (?v=loc&tab=saved), honor it
  const urlTab = new URLSearchParams(location.search).get("tab");
  _locSwitchTab(urlTab === "saved" ? "saved" : "search");
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
        <div class="loc-form-row">
          <input type="text" id="loc-q" placeholder="Keyword (title, subject, or any text)" />
          <button type="submit" class="loc-submit" id="loc-submit-btn">Search</button>
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

function _locSwitchTab(tab) {
  _locTab = tab === "saved" ? "saved" : "search";
  document.querySelectorAll(".loc-tab").forEach(btn => btn.classList.remove("active"));
  document.querySelector(`.loc-tab-${_locTab}`)?.classList.add("active");
  const searchPanel = document.querySelector(".loc-panel-search");
  const savedPanel  = document.querySelector(".loc-panel-saved");
  if (searchPanel) searchPanel.style.display = _locTab === "search" ? "" : "none";
  if (savedPanel)  savedPanel.style.display  = _locTab === "saved"  ? "" : "none";
  if (_locTab === "saved") _locLoadSaved();
}

// ── Search execution ────────────────────────────────────────────────────

function _locReadFormParams() {
  const val = (id) => (document.getElementById(id)?.value ?? "").trim();
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
  };
}

async function _locRunSearchFromForm({ resetPage = true } = {}) {
  const params = _locReadFormParams();
  if (resetPage) params.sp = "1";
  else if (_locLastQuery?.sp) params.sp = _locLastQuery.sp;
  return _locRunSearch(params);
}

async function _locRunSearch(params) {
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
  statusEl.textContent = "Searching LOC…";
  grid.innerHTML = `<div class="loc-skeleton"></div>`;
  pag.innerHTML = "";
  if (submitBtn) submitBtn.disabled = true;

  try {
    const body = await _locFetchSearch(params);
    _locLastResponse = body;
    const results = Array.isArray(body?.results) ? body.results : [];
    if (!results.length) {
      statusEl.textContent = "No results.";
      grid.innerHTML = "";
      return;
    }
    statusEl.textContent = body.pagination
      ? `${body.pagination.from}-${body.pagination.to} of ${body.pagination.total} results`
      : `${results.length} results`;
    grid.innerHTML = results.map(_locRenderCard).join("");
    _locRenderPagination(body.pagination);
  } catch (e) {
    statusEl.textContent = e?.message || "Search failed.";
    grid.innerHTML = "";
  } finally {
    if (submitBtn) submitBtn.disabled = false;
  }
}

function _locRenderPagination(p) {
  const el = document.getElementById("loc-pagination");
  if (!el || !p) return;
  const current = Number(p.current) || 1;
  const perpage = Number(p.perpage) || 100;
  const total   = Number(p.total)   || 0;
  const totalPages = Math.max(1, Math.ceil(total / perpage));
  if (totalPages <= 1) { el.innerHTML = ""; return; }
  const prevDis = !p.hasPrev ? "disabled" : "";
  const nextDis = !p.hasNext ? "disabled" : "";
  el.innerHTML = `
    <button type="button" class="loc-page-btn" ${prevDis} onclick="_locGotoPage(${current - 1})">← Prev</button>
    <span class="loc-page-info">Page ${current} / ${totalPages}</span>
    <button type="button" class="loc-page-btn" ${nextDis} onclick="_locGotoPage(${current + 1})">Next →</button>
  `;
}

function _locGotoPage(n) {
  if (!_locLastQuery) return;
  const next = { ..._locLastQuery, sp: String(Math.max(1, Number(n) || 1)) };
  _locRunSearch(next);
  // Scroll results into view
  document.getElementById("loc-results")?.scrollIntoView({ behavior: "smooth", block: "start" });
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
function _locOpenInfoPopup(locId) {
  if (!locId) return;
  const overlay = document.getElementById("loc-info-overlay");
  const body    = document.getElementById("loc-info-body");
  if (!overlay || !body) return;
  const item = _locItemCache.get(locId);
  if (!item) {
    body.innerHTML = `<div class="loc-empty">Item no longer in cache.</div>`;
    overlay.classList.add("open");
    return;
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
  const metaRows = [
    ["Contributor(s)", contributors.join(", ")],
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

  // Free-form text blocks — description (from r.description), notes
  // (from item.notes, usually release catalog info), speakers (podcast
  // contributors as a pseudo-tracklist), and the full article (podcast
  // transcript or blurb).
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
  const speakersBlock = speakers.length
    ? `<div class="loc-info-section">
        <div class="loc-info-section-title">Speakers / participants</div>
        <ul class="loc-info-list loc-info-list-inline">${speakers.map(s => `<li>${esc(s)}</li>`).join("")}</ul>
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
  const saveBtn = `<button type="button" class="loc-info-btn loc-info-btn-save${saved ? " is-saved" : ""}" onclick="_locToggleSaveFromInfo('${esc(item.id).replace(/'/g, "\\'")}')">${saved ? "★ Saved" : "☆ Save"}</button>`;
  const locLink = `<a class="loc-info-btn loc-info-btn-loc" href="${esc(item.url || item.id)}" target="_blank" rel="noopener">Open on loc.gov ↗</a>`;

  // Credit line — "Library of Congress, [Collection name]." format from
  // LOC's own attribution guidance. Shown as a muted footer so any future
  // redistribution has the right citation on hand.
  const creditLine = primaryCollection
    ? `Library of Congress, ${primaryCollection.replace(/\b\w/g, (c) => c.toUpperCase())}.`
    : `Library of Congress.`;

  body.innerHTML = `
    <div class="loc-info-head">
      ${imgTag}
      <div class="loc-info-head-text">
        <div class="loc-info-title">${esc(item.title || "Untitled")}</div>
        ${contributors.length ? `<div class="loc-info-artist">${esc(contributors.join(", "))}</div>` : ""}
        <div class="loc-info-actions">${playBtn}${saveBtn}${locLink}</div>
      </div>
    </div>
    ${summaryHtml}
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
}

function _locPlayFromInfo(locId) {
  const item = _locItemCache.get(locId);
  if (item?.streamUrl) _locPlay(item);
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

function _locPlayFromCard(btn) {
  const card = btn?.closest(".loc-card");
  const item = _locReadCard(card);
  if (!item?.streamUrl) return;
  _locPlay(item);
}

async function _locToggleSaveFromCard(btn) {
  const card = btn?.closest(".loc-card");
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
  const bar = document.getElementById("loc-audio-bar");
  const audio = document.getElementById("loc-audio");
  const titleEl = document.getElementById("loc-audio-title");
  if (!bar || !audio) return;

  // Stop any YouTube playback so we don't double-play
  try { if (typeof closeVideo === "function") closeVideo(); } catch {}

  _locNowPlaying = item;
  titleEl.textContent = item.title || "Playing…";
  bar.classList.add("is-visible");

  // Tear down any prior hls.js instance
  if (audio._hls) {
    try { audio._hls.destroy(); } catch {}
    audio._hls = null;
  }
  audio.pause();
  audio.removeAttribute("src");
  audio.load();

  const isHls = item.streamType === "hls" || /\.m3u8(\?|$)/i.test(item.streamUrl);
  if (isHls) {
    // Safari can play HLS natively; everywhere else needs hls.js
    if (audio.canPlayType("application/vnd.apple.mpegurl")) {
      audio.src = item.streamUrl;
    } else {
      try {
        const Hls = await _ensureHlsLoaded();
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
  audio.play().catch(err => {
    showToast?.("Playback failed: " + (err?.message || "unknown"), "error");
  });
}

function _locClosePlayer() {
  const bar = document.getElementById("loc-audio-bar");
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
  if (bar) bar.classList.remove("is-visible");
  _locNowPlaying = null;
}

// Close the info popup with Escape
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") _locCloseInfoPopup();
});

// Expose globals for inline onclick handlers
window.initLocView              = initLocView;
window._locSwitchTab            = _locSwitchTab;
window._locPlayFromCard         = _locPlayFromCard;
window._locToggleSaveFromCard   = _locToggleSaveFromCard;
window._locRemoveSavedFromCard  = _locRemoveSavedFromCard;
window._locOnSavedFilterInput   = _locOnSavedFilterInput;
window._locOnSavedSortChange    = _locOnSavedSortChange;
window._locGotoPage             = _locGotoPage;
window._locLoadSaved            = _locLoadSaved;
window._locClosePlayer          = _locClosePlayer;
window._locRunSearchFromForm    = _locRunSearchFromForm;
window._locOpenInfoPopup        = _locOpenInfoPopup;
window._locCloseInfoPopup       = _locCloseInfoPopup;
window._locPlayFromInfo         = _locPlayFromInfo;
window._locToggleSaveFromInfo   = _locToggleSaveFromInfo;
