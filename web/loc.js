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

function initLocView() {
  const root = document.getElementById("loc-view");
  if (!root) return;
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
      <p class="loc-sub">Search digitized audio from the Library of Congress (National Jukebox and more). All content is public-domain or royalty-free.</p>
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
          <label><span>Per page</span>
            <select id="loc-perpage">
              <option value="25">25</option>
              <option value="50">50</option>
              <option value="100" selected>100</option>
            </select>
          </label>
        </div>
      </form>
      <div id="loc-status" class="loc-status"></div>
      <div id="loc-results" class="loc-results"></div>
      <div id="loc-pagination" class="loc-pagination"></div>
    </div>

    <div class="loc-panel loc-panel-saved" style="display:none">
      <div class="loc-saved-head">
        <div class="loc-saved-title">Your saved LOC audio</div>
        <button type="button" class="loc-refresh-btn" onclick="_locLoadSaved()" title="Refresh saved list">↻</button>
      </div>
      <div id="loc-saved-list" class="loc-results"></div>
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

function _locRenderCard(item) {
  if (!item || !item.id) return "";
  const saved = !!_locSavedIds?.has(item.id);
  const canPlay = !!item.streamUrl;
  const contributor = Array.isArray(item.contributors) && item.contributors.length
    ? item.contributors.join(", ")
    : "";
  const subtitle = [contributor, item.year].filter(Boolean).join(" · ");
  const metaLine = [item.label, item.location, item.audioType].filter(Boolean).join(" · ");
  const genreLine = Array.isArray(item.genres) && item.genres.length
    ? item.genres.slice(0, 4).join(", ")
    : "";
  // Safely escape every interpolated value
  const esc = (v) => escHtml(String(v ?? ""));
  const idAttr = esc(item.id);
  const titleSafe = esc(item.title || "Untitled");
  const subSafe   = esc(subtitle);
  const metaSafe  = esc(metaLine);
  const genreSafe = esc(genreLine);
  const summarySafe = esc(item.summary || "");
  const urlSafe = esc(item.url || item.id);
  const imgTag = item.image
    ? `<img class="loc-card-thumb" src="${esc(item.image)}" alt="" loading="lazy"/>`
    : `<div class="loc-card-thumb loc-card-thumb-ph"></div>`;
  const playBtn = canPlay
    ? `<button type="button" class="loc-play-btn" onclick="_locPlayFromCard(this)" title="Play"><span class="loc-play-icon">▶</span></button>`
    : `<button type="button" class="loc-play-btn is-disabled" disabled title="No playable stream for this item">▶</button>`;
  const saveBtn = `<button type="button" class="loc-save-btn${saved ? " is-saved" : ""}" onclick="_locToggleSaveFromCard(this)" title="${saved ? "Remove from Saved" : "Save to your list"}">${saved ? "★" : "☆"}</button>`;
  return `
    <div class="loc-card" data-loc-id="${idAttr}" data-title="${titleSafe}" data-stream="${esc(item.streamUrl || "")}" data-stream-type="${esc(item.streamType || "")}" data-image="${esc(item.image || "")}">
      ${imgTag}
      <div class="loc-card-body">
        <a class="loc-card-title" href="${urlSafe}" target="_blank" rel="noopener" title="Open on loc.gov">${titleSafe}</a>
        ${subSafe ? `<div class="loc-card-sub">${subSafe}</div>` : ""}
        ${metaSafe ? `<div class="loc-card-meta">${metaSafe}</div>` : ""}
        ${genreSafe ? `<div class="loc-card-genre">${genreSafe}</div>` : ""}
        ${summarySafe ? `<div class="loc-card-summary">${summarySafe}</div>` : ""}
      </div>
      <div class="loc-card-actions">
        ${playBtn}
        ${saveBtn}
      </div>
    </div>
  `;
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
      const payload = {
        locId,
        title: card.dataset.title || "",
        streamUrl: card.dataset.stream || "",
        data: {
          id: locId,
          title: card.dataset.title || "",
          streamUrl: card.dataset.stream || "",
          streamType: card.dataset.streamType || "",
          image: card.dataset.image || "",
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
      // If the Saved tab is currently showing this card, drop it from the DOM too
      if (_locTab === "saved") {
        document.querySelectorAll(`.loc-panel-saved .loc-card[data-loc-id="${CSS.escape(locId)}"]`).forEach(el => el.remove());
        if (_locSavedItems) _locSavedItems = _locSavedItems.filter(i => i.id !== locId);
      }
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
    }));
    // Ensure the in-memory saved-ID set matches
    if (!_locSavedIds) _locSavedIds = new Set();
    for (const it of _locSavedItems) _locSavedIds.add(it.id);
    if (!_locSavedItems.length) {
      grid.innerHTML = `<div class="loc-empty">No saves yet. Tap the ☆ on any result to build your list.</div>`;
      return;
    }
    grid.innerHTML = _locSavedItems.map(_locRenderCard).join("");
  } catch (e) {
    grid.innerHTML = `<div class="loc-empty">Could not load saved items: ${escHtml(e?.message || "unknown error")}</div>`;
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

// Expose globals for inline onclick handlers
window.initLocView         = initLocView;
window._locSwitchTab       = _locSwitchTab;
window._locPlayFromCard    = _locPlayFromCard;
window._locToggleSaveFromCard = _locToggleSaveFromCard;
window._locGotoPage        = _locGotoPage;
window._locLoadSaved       = _locLoadSaved;
window._locClosePlayer     = _locClosePlayer;
