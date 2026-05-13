// ── Archive.org page ─────────────────────────────────────────────────────
//
// Three tabs:
//   Search  — general archive.org search filtered to MP3-playable items
//             (server proxies advancedsearch.php with mediatype:audio +
//              format MP3 constraints, 90-day DB cache per query)
//   Curated — Aadam Jacobs live-show collection (the original page;
//             still on /api/archive/aadamjacobs, filterable + sortable)
//   Saved   — the user's bookmarked items (★ toggle on each row)
// ▶ Play loads into the unified mini-player via the LOC audio engine
// (archive.org serves direct mp3 URLs the same way LOC does); ➕ Queue
// inserts into the cross-source play queue; ★ toggles save state on the
// server.

// Curated-tab state (kept identical to the pre-refactor names so the
// existing render path doesn't churn). _archiveCuratedSlug tracks
// which collection from the dropdown is currently selected — the
// dropdown options come from /api/archive/curated/list and selecting
// one swaps _archiveList.
let _archiveList = null;
let _archiveLoading = false;
let _archiveFilter = "";
// Default sort: oldest show first. Curated collections are live-show
// archives where chronological order is the natural way to browse —
// title sort would have looked alphabetical with no real signal.
let _archiveSort = "date-asc"; // title-asc | title-desc | date-desc | date-asc
let _archiveShown = 0;          // how many filtered+sorted rows currently rendered
const _ARCHIVE_PAGE = 48;       // matches the Recent strip page size
let _archiveCuratedSlug = "aadamjacobs";  // active curated collection slug
let _archiveCuratedList = null;            // [{slug, title}, ...] from server
// Per-slug cache of loaded item arrays, keyed by slug. Lets the user
// switch between collections without re-fetching ones they've
// already seen this session. LRU-capped because each entry can hold
// 50-200 items × 1-2KB.
const _archiveCuratedCache = new Map();
const _ARCHIVE_CURATED_CACHE_MAX = 20;
function _archiveCuratedCachePut(slug, list) {
  if (!slug) return;
  if (_archiveCuratedCache.has(slug)) _archiveCuratedCache.delete(slug);
  _archiveCuratedCache.set(slug, list);
  if (_archiveCuratedCache.size > _ARCHIVE_CURATED_CACHE_MAX) {
    const it = _archiveCuratedCache.keys();
    const drop = _archiveCuratedCache.size - _ARCHIVE_CURATED_CACHE_MAX;
    for (let i = 0; i < drop; i++) {
      const k = it.next().value;
      if (k === undefined) break;
      _archiveCuratedCache.delete(k);
    }
  }
}

// ── Search-tab state ────────────────────────────────────────────────
let _archiveSearchQuery   = "";       // last-submitted query text
let _archiveSearchResults = null;     // Array of normalized result items
let _archiveSearchPage    = 1;
let _archiveSearchRows    = 48;
let _archiveSearchNumFound = 0;
let _archiveSearchLoading  = false;
// Filter state — submitted with each search and reflected in the URL.
let _archiveSearchSort     = "popularity"; // popularity|newest|oldest|showNewest|showOldest|titleAsc|titleDesc|rated
let _archiveSearchCreator  = "";
let _archiveSearchSubject  = "";
let _archiveSearchCollection = "";
let _archiveSearchYearFrom = "";
let _archiveSearchYearTo   = "";
// Category dropdown — flat list, no sub-headings. Picking "music"
// (the default) searches the union of all music-y collections
// (audio_music + etree + 78rpm + opensource_audio) so the user
// doesn't have to think about which sub-bucket their target sits in.
// Other categories map 1:1 to a single collection slug.
let _archiveSearchCategory = "music";
const _ARCHIVE_CATEGORY_OPTIONS = [
  ["music",             "Music"],
  // Audiobooks-or-poetry is the union of librivoxaudio (LibriVox
  // recordings, the largest free-audiobook archive) AND
  // audio_bookspoetry (a broader bucket with non-LibriVox
  // recordings + poetry readings). The comma-list goes through the
  // same multi-collection OR path the music union uses.
  ["librivoxaudio,audio_bookspoetry", "Audiobooks & poetry"],
  ["radioprograms",     "Old-time radio programs"],
  ["audio_religion",    "Religion"],
  ["audio_news",        "News & public affairs"],
  ["audio_tech",        "Computers, technology, science"],
  ["audio_foreign",     "Non-English audio"],
  ["audio_podcast",     "Podcasts"],
  ["all",               "All audio"],
];

// ── Tab + saves state ────────────────────────────────────────────────
let _archiveTab = "search";        // "search" | "curated" | "saved"
let _archiveSavedIds = null;       // Set<string> of saved archive identifiers
let _archiveSavedItems = null;     // Array of saved items (loaded on Saved tab open)
let _archiveSavedLoading = false;
let _archiveSavedFilter = "";
let _archiveSavedSort = "recent";  // recent | title | date-asc | date-desc

async function initArchiveView(forceRefresh = false) {
  const root = document.getElementById("archive-view");
  if (!root) return;
  // Archive page is open to all callers (search is rate-limited per
  // IP; curated reads from the DB cache only). Saving requires
  // sign-in: _archiveLoadSavedIds 401s for anons and falls back to
  // an empty Set, which just hides the ★ as already-saved (the click
  // handler will still try and surface a "sign in" toast).
  if (window._clerk?.user && _archiveSavedIds == null) _archiveLoadSavedIds();

  // Honor ?tab=...&q=...&col=... in the URL so search/saved-list/
  // curated-collection links are shareable. Default lands on the
  // Search tab with an empty input.
  try {
    const qs = new URLSearchParams(location.search);
    const t = qs.get("tab");
    if (t === "saved") _archiveTab = "saved";
    else if (t === "curated" || t === "browse") _archiveTab = "curated";
    else _archiveTab = "search";
    const q = qs.get("q");
    if (q && _archiveTab === "search") _archiveSearchQuery = q;
    const col = qs.get("col");
    if (col) _archiveCuratedSlug = col;
    // Filter params for the search tab. Each is read once at boot
    // and pushed back into the URL on every submit so deep links
    // round-trip cleanly.
    if (_archiveTab === "search") {
      _archiveSearchSort       = qs.get("sort")       || "popularity";
      _archiveSearchCreator    = qs.get("creator")    || "";
      _archiveSearchSubject    = qs.get("subject")    || "";
      _archiveSearchCollection = qs.get("col2")       || "";
      _archiveSearchYearFrom   = qs.get("yf")         || "";
      _archiveSearchYearTo     = qs.get("yt")         || "";
      _archiveSearchCategory   = qs.get("cat")        || "music";
    }
  } catch {}

  // Kick off the dropdown population fetch in parallel with the rest
  // of init. The first render uses the static fallback ("Aadam Jacobs"
  // only) and re-renders once the full list lands.
  _archiveLoadCuratedList().catch(() => {});

  // Render the page shell + tabs first so the user always sees the
  // search input + tab strip even if the data fetches haven't returned.
  _renderArchiveList();

  // Curated data: only fetch when the curated tab is the active one
  // (or when forceRefresh is set). Mirrors the pattern in
  // _archiveSwitchTab where switching INTO curated triggers the load.
  if (_archiveTab === "curated") {
    _archiveLoadCurated(forceRefresh).catch(() => {});
  }
  // Search tab with a pre-filled q: kick off the search automatically
  // so a shared URL lands on the result grid, not the empty state.
  if (_archiveTab === "search" && _archiveSearchQuery) {
    _archiveDoSearch().catch(() => {});
  }
}

// Fetch the dropdown options once per page. Cached on the module
// scope so subsequent tab switches don't re-fetch. Re-renders the
// list panel once the items land so the dropdown gets populated.
async function _archiveLoadCuratedList() {
  if (_archiveCuratedList) return _archiveCuratedList;
  try {
    const r = await apiFetch("/api/archive/curated/list");
    if (!r.ok) throw new Error("HTTP " + r.status);
    const j = await r.json();
    _archiveCuratedList = Array.isArray(j?.items) ? j.items : [];
  } catch {
    // Fallback: just the legacy default. Better than an empty dropdown.
    _archiveCuratedList = [{ slug: "aadamjacobs", title: "Aadam Jacobs" }];
  }
  // Re-render the curated panel if it's the active tab so the
  // dropdown picks up the freshly-loaded options.
  if (_archiveTab === "curated") _renderArchiveList();
  return _archiveCuratedList;
}

// Fetch a curated collection by slug. Cached per-slug so switching
// between collections doesn't re-fetch ones already loaded this
// session. Idempotent: bails if already loaded or in flight unless
// forceRefresh is true.
async function _archiveLoadCurated(forceRefresh = false, slug = null) {
  const targetSlug = slug || _archiveCuratedSlug;
  // Pull from the per-slug cache first.
  if (!forceRefresh && _archiveCuratedCache.has(targetSlug)) {
    _archiveList = _archiveCuratedCache.get(targetSlug);
    _archiveCuratedSlug = targetSlug;
    _renderArchiveList();
    return;
  }
  if (_archiveLoading) return;
  _archiveLoading = true;
  _archiveCuratedSlug = targetSlug;
  const rowsEl = document.getElementById("archive-rows");
  if (rowsEl) rowsEl.innerHTML = `<div class="loc-empty">Loading collection — first load may take a few seconds.</div>`;
  try {
    // Legacy path: aadamjacobs uses the original endpoint (and gets
    // its weekly cron refresh). Other slugs go through the generic
    // /api/archive/curated/:slug endpoint.
    const base = targetSlug === "aadamjacobs"
      ? "/api/archive/aadamjacobs"
      : `/api/archive/curated/${encodeURIComponent(targetSlug)}`;
    const url = forceRefresh ? `${base}?nocache=1` : base;
    const r = await apiFetch(url);
    if (r.status === 429) {
      const body = await r.json().catch(() => ({}));
      if (rowsEl) rowsEl.innerHTML = `<div class="loc-empty">${escHtml(body?.message || "Too many Archive requests from your network — wait a minute and try again.")}</div>`;
      return;
    }
    if (!r.ok) {
      if (rowsEl) rowsEl.innerHTML = `<div class="loc-empty">Could not load archive collection (HTTP ${r.status}).</div>`;
      return;
    }
    const j = await r.json();
    const list = Array.isArray(j?.items) ? j.items : [];
    _archiveList = list;
    _archiveCuratedCachePut(targetSlug, list);
    _renderArchiveList();
  } catch (err) {
    if (rowsEl) rowsEl.innerHTML = `<div class="loc-empty">Could not load archive collection.</div>`;
  } finally {
    _archiveLoading = false;
  }
}

// Dropdown change handler — switch the active curated collection.
// Kicks off the load (cache hit or fetch) and pushes the new slug
// into the URL so the link is shareable.
function _archiveCuratedSelect(sel) {
  const slug = sel?.value || "aadamjacobs";
  _archiveCuratedSlug = slug;
  _archiveFilter = "";       // reset filter when switching collections
  _archiveShown  = 0;
  if (typeof history?.pushState === "function") {
    const qs = new URLSearchParams(location.search);
    qs.set("v", "archive");
    qs.set("tab", "curated");
    qs.set("col", slug);
    const next = "/?" + qs.toString();
    if (location.pathname + location.search !== next) {
      history.pushState({}, "", next);
    }
  }
  _archiveLoadCurated(false, slug).catch(() => {});
}
window._archiveCuratedSelect = _archiveCuratedSelect;

// archiveRefresh kept for backwards-compat only — the page no longer
// renders a refresh button, but admin.html's "Get archive data"
// button still calls into the admin endpoint, which writes the cache
// in the background. The page will show fresh data on next reload.
function archiveRefresh() {
  _archiveList = null;
  initArchiveView(true);
}

// ── Saved-state cache ─────────────────────────────────────────────────
async function _archiveLoadSavedIds() {
  try {
    const r = await apiFetch("/api/user/archive-saves/ids");
    if (!r.ok) { _archiveSavedIds = new Set(); return; }
    const j = await r.json();
    _archiveSavedIds = new Set(Array.isArray(j?.ids) ? j.ids : []);
    // If rows are already on screen, refresh the star badges.
    document.querySelectorAll(".archive-row[data-id]").forEach(el => {
      const id = el.dataset.id;
      const star = el.querySelector(".archive-save-btn");
      if (star) {
        const saved = _archiveSavedIds.has(id);
        star.classList.toggle("is-saved", saved);
        star.textContent = saved ? "★" : "☆";
      }
    });
  } catch {
    _archiveSavedIds = new Set();
  }
}

async function _archiveLoadSaved() {
  if (_archiveSavedLoading) return;
  _archiveSavedLoading = true;
  const listEl = document.getElementById("archive-saved-rows");
  if (listEl) listEl.innerHTML = `<div class="loc-empty">Loading…</div>`;
  try {
    const r = await apiFetch("/api/user/archive-saves");
    if (!r.ok) {
      if (listEl) listEl.innerHTML = `<div class="loc-empty">Could not load saved items.</div>`;
      return;
    }
    const j = await r.json();
    // Each row from the server has shape { archiveId, title, streamUrl, data, savedAt }.
    // Normalize back into the same row shape `_archiveRowHtml` expects so
    // we can render with the same template.
    _archiveSavedItems = (Array.isArray(j?.items) ? j.items : []).map(s => ({
      identifier: s.archiveId,
      title: s.title || s.data?.title || s.archiveId,
      date: s.data?.date || "",
      description: s.data?.description || "",
      streamUrl: s.streamUrl || s.data?.streamUrl || "",
      itemUrl: s.data?.itemUrl || `https://archive.org/details/${encodeURIComponent(s.archiveId)}`,
      _savedAt: s.savedAt,
    }));
    _archiveRenderSavedRows();
  } catch {
    if (listEl) listEl.innerHTML = `<div class="loc-empty">Could not load saved items.</div>`;
  } finally {
    _archiveSavedLoading = false;
  }
}

function _archiveSavedFilterSort() {
  if (!Array.isArray(_archiveSavedItems)) return [];
  const q = _archiveSavedFilter.trim().toLowerCase();
  const filtered = _archiveSavedItems
    .map((it, i) => ({ it, _origIdx: i }))
    .filter(({ it }) => {
      if (!q) return true;
      const hay = [it.title, it.date, it.description].filter(Boolean).join(" ").toLowerCase();
      return hay.includes(q);
    });
  const cmp = {
    "recent":     (a, b) => String(b.it._savedAt || "").localeCompare(String(a.it._savedAt || "")),
    "title":      (a, b) => String(a.it.title || "").localeCompare(String(b.it.title || ""), undefined, { sensitivity: "base" }),
    "date-desc":  (a, b) => String(b.it.date  || "").localeCompare(String(a.it.date  || "")),
    "date-asc":   (a, b) => String(a.it.date  || "").localeCompare(String(b.it.date  || "")),
  }[_archiveSavedSort] || ((a, b) => a._origIdx - b._origIdx);
  filtered.sort(cmp);
  return filtered;
}

function _archiveRenderSavedRows() {
  const rowsEl = document.getElementById("archive-saved-rows");
  const countEl = document.getElementById("archive-saved-count");
  if (!rowsEl) return;
  const view = _archiveSavedFilterSort();
  if (countEl) countEl.textContent = `${view.length}`;
  if (!_archiveSavedItems?.length) {
    rowsEl.innerHTML = `<div class="loc-empty">No saved items yet. Click ★ on any row in Browse to save it.</div>`;
    return;
  }
  if (!view.length) {
    rowsEl.innerHTML = `<div class="loc-empty">No saved items match.</div>`;
    return;
  }
  // Render every saved row — saved lists are typically small (capped at
  // 1000 server-side, expected dozens). No pagination needed.
  rowsEl.innerHTML = view.map(({ it }) => _archiveRowHtml(it, /*idx*/ -1, { savedView: true })).join("");
  if (typeof _locUpdatePlayingCard === "function") _locUpdatePlayingCard();
}

function _archiveOnSavedFilterInput(input) {
  _archiveSavedFilter = input.value || "";
  _archiveRenderSavedRows();
}
function _archiveOnSavedSortChange(select) {
  _archiveSavedSort = select.value || "recent";
  _archiveRenderSavedRows();
}

// ── Save toggle ──────────────────────────────────────────────────────
// Public probe used by the player-bar save button to decide which
// glyph to render (★ vs ☆). Defined as a function rather than direct
// Set access so the bar can do `window._archiveSavedIdsHas?.(id)`
// without crashing if archive.js hasn't loaded yet.
window._archiveSavedIdsHas = (id) => !!_archiveSavedIds?.has(id);

// Save / unsave by archive identifier (no DOM button). Used by the
// mini-player bar's save toggle, which only has the playing item's
// id. Mirrors the body of archiveToggleSave but adapts the save
// payload to be data-driven rather than read from a row element.
// Returns the new saved state (true = saved, false = unsaved).
async function _archiveToggleSaveById(id) {
  if (!id) return null;
  if (!window._clerk?.user) {
    if (typeof showToast === "function") showToast("Sign in to save items", "error");
    return null;
  }
  if (!_archiveSavedIds) _archiveSavedIds = new Set();
  const saving = !_archiveSavedIds.has(id);
  // Try to find the item in any in-memory list. If not found (e.g. the
  // user shared a queue link with an archive item that's not loaded
  // anywhere on this page), fetch metadata via /api/archive/item/:id
  // so the save row gets a usable title / date.
  let src = (_archiveList          || []).find(x => x.identifier === id)
         || (_archiveSearchResults || []).find(x => x.identifier === id)
         || (_archiveSavedItems    || []).find(x => x.identifier === id);
  if (!src && saving) {
    try {
      const meta = await _archiveResolveMeta(id);
      if (meta) {
        src = {
          identifier:  id,
          title:       meta.title || meta.identifier || id,
          date:        meta.date || "",
          description: meta.description || "",
          itemUrl:     `https://archive.org/details/${encodeURIComponent(id)}`,
          streamUrl:   meta.audioFiles?.[0]?.streamUrl || "",
        };
      }
    } catch { /* fall through with empty src */ }
  }
  // Optimistic toggle (no DOM elements to update here — the bar's
  // _locUpdateQueueButtons is called by its own handler after we return).
  _archiveSavedIds[saving ? "add" : "delete"](id);
  try {
    if (saving) {
      const r = await apiFetch("/api/user/archive-saves", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          archiveId: id,
          title: src?.title || "",
          streamUrl: src?.streamUrl || "",
          data: src ? {
            title: src.title || "",
            date: src.date || "",
            description: src.description || "",
            itemUrl: src.itemUrl || "",
            streamUrl: src.streamUrl || "",
          } : {},
        }),
      });
      if (!r.ok) throw new Error(`save failed (${r.status})`);
      showToast?.("Saved");
    } else {
      const r = await apiFetch("/api/user/archive-saves", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ archiveId: id }),
      });
      if (!r.ok) throw new Error(`remove failed (${r.status})`);
      showToast?.("Removed from Saved");
      if (_archiveSavedItems) _archiveSavedItems = _archiveSavedItems.filter(i => i.identifier !== id);
      if (_archiveTab === "saved") _archiveRenderSavedRows();
    }
    // Reflect the new state on any visible ★ button for this id —
    // covers all three tabs simultaneously since rows in the inactive
    // tabs stay in the DOM (just display:none).
    document.querySelectorAll(`.archive-row[data-id="${CSS.escape(id)}"] .archive-save-btn`).forEach(btn => {
      btn.classList.toggle("is-saved", saving);
      btn.textContent = saving ? "★" : "☆";
      btn.title = saving ? "Remove from Saved" : "Save to your list";
    });
    return saving;
  } catch (e) {
    _archiveSavedIds[saving ? "delete" : "add"](id);
    showToast?.(e?.message || "Action failed", "error");
    return null;
  }
}
window._archiveToggleSaveById = _archiveToggleSaveById;

async function archiveToggleSave(btn) {
  const row = btn?.closest(".archive-row");
  const id = row?.dataset.id;
  if (!id) return;
  // Saving requires sign-in (the endpoint is requireUser-gated). Surface
  // a friendlier message than a "save failed (401)" toast.
  if (!window._clerk?.user) {
    if (typeof showToast === "function") showToast("Sign in to save items", "error");
    return;
  }
  if (!_archiveSavedIds) _archiveSavedIds = new Set();
  const saving = !_archiveSavedIds.has(id);
  // Optimistic toggle
  _archiveSavedIds[saving ? "add" : "delete"](id);
  btn.classList.toggle("is-saved", saving);
  btn.textContent = saving ? "★" : "☆";
  btn.title = saving ? "Remove from Saved" : "Save to your list";
  // Find the source item — could be in curated list, search results,
  // or the saved list. Try all three so save works from any tab; the
  // row data-id is enough to look up by identifier.
  const src = (_archiveList           || []).find(x => x.identifier === id)
           || (_archiveSearchResults  || []).find(x => x.identifier === id)
           || (_archiveSavedItems     || []).find(x => x.identifier === id);
  try {
    if (saving) {
      const r = await apiFetch("/api/user/archive-saves", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          archiveId: id,
          title: src?.title || "",
          streamUrl: src?.streamUrl || "",
          data: src ? {
            title: src.title || "",
            date: src.date || "",
            description: src.description || "",
            itemUrl: src.itemUrl || "",
            streamUrl: src.streamUrl || "",
          } : {},
        }),
      });
      if (!r.ok) throw new Error(`save failed (${r.status})`);
      showToast?.("Saved");
    } else {
      const r = await apiFetch("/api/user/archive-saves", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ archiveId: id }),
      });
      if (!r.ok) throw new Error(`remove failed (${r.status})`);
      showToast?.("Removed from Saved");
      // Drop from in-memory saved list and re-render the saved tab if open
      if (_archiveSavedItems) _archiveSavedItems = _archiveSavedItems.filter(i => i.identifier !== id);
      if (_archiveTab === "saved") _archiveRenderSavedRows();
    }
  } catch (e) {
    // Revert optimistic toggle on failure
    _archiveSavedIds[saving ? "delete" : "add"](id);
    btn.classList.toggle("is-saved", !saving);
    btn.textContent = !saving ? "★" : "☆";
    showToast?.(e?.message || "Action failed", "error");
  }
}

// ── Browse-tab rendering ─────────────────────────────────────────────
// Apply current filter + sort to a copy of _archiveList. Pure — does
// not mutate the source array, so the original ordering survives sort
// changes. Items keep their original index in `_origIdx` so play/queue
// handlers can still address them by position in _archiveList.
function _archiveFilterSort() {
  if (!Array.isArray(_archiveList)) return [];
  const q = _archiveFilter.trim().toLowerCase();
  const filtered = _archiveList
    .map((it, i) => ({ it, _origIdx: i }))
    .filter(({ it }) => {
      if (!q) return true;
      const hay = [it.title, it.date, it.description].filter(Boolean).join(" ").toLowerCase();
      return hay.includes(q);
    });
  const cmp = {
    "title-asc":  (a, b) => String(a.it.title || "").localeCompare(String(b.it.title || ""), undefined, { sensitivity: "base" }),
    "title-desc": (a, b) => String(b.it.title || "").localeCompare(String(a.it.title || ""), undefined, { sensitivity: "base" }),
    "date-desc":  (a, b) => String(b.it.date  || "").localeCompare(String(a.it.date  || "")),
    "date-asc":   (a, b) => String(a.it.date  || "").localeCompare(String(b.it.date  || "")),
  }[_archiveSort] || ((a, b) => a._origIdx - b._origIdx);
  filtered.sort(cmp);
  return filtered;
}

// Re-render only the row list (keeps the meta-bar input/select state
// intact while filtering/sorting). Called from oninput / onchange on
// the controls so we don't lose focus on the filter input.
// Pagination: render up to _archiveShown items, with a "Load more"
// button below. _archiveShown resets to one page on filter/sort change.
function _archiveRenderRowsOnly(opts) {
  const rowsEl = document.getElementById("archive-rows");
  if (!rowsEl) { _renderArchiveList(); return; }
  const view = _archiveFilterSort();
  if (!view.length) {
    rowsEl.innerHTML = `<div class="loc-empty">No items match.</div>`;
    _updateArchiveCount(0);
    return;
  }
  if (!opts?.append) _archiveShown = Math.min(_ARCHIVE_PAGE, view.length);
  const slice = view.slice(0, _archiveShown);
  const html = slice.map(({ it, _origIdx }) => _archiveRowHtml(it, _origIdx)).join("");
  const remaining = view.length - _archiveShown;
  const loadMore = remaining > 0
    ? `<div class="archive-load-more-wrap"><button type="button" class="archive-load-more" onclick="archiveLoadMore()">Load ${Math.min(_ARCHIVE_PAGE, remaining)} more (${remaining} left)</button></div>`
    : "";
  rowsEl.innerHTML = html + loadMore;
  _updateArchiveCount(view.length);
  // Reapply the now-playing mark to any freshly-rendered row whose
  // identifier matches the LOC bar's current item — filter / sort /
  // load-more all swap row markup, which would otherwise drop the
  // existing .is-playing class.
  if (typeof _locUpdatePlayingCard === "function") _locUpdatePlayingCard();
}

function _updateArchiveCount(filtered) {
  const countEl = document.getElementById("archive-count");
  if (!countEl || !_archiveList) return;
  const total = _archiveList.length;
  if (filtered === total) {
    countEl.textContent = `${_archiveShown} of ${total}`;
  } else {
    countEl.textContent = `${_archiveShown} of ${filtered} (${total} total)`;
  }
}

function archiveLoadMore() {
  _archiveShown += _ARCHIVE_PAGE;
  // Pass append: true so _archiveRenderRowsOnly doesn't reset
  // _archiveShown back to one page. The function still re-renders
  // the entire visible block from index 0 — the flag only controls
  // the count reset, not the render strategy.
  _archiveRenderRowsOnly({ append: true });
}

// Build the row HTML. Used for both Browse rows (idx is the position in
// _archiveList — wired into archivePlayItem/archiveQueueItem) and Saved
// rows (idx is -1; play/queue read the row's data-* attributes
// directly via archivePlaySaved / archiveQueueSaved).
function _archiveRowHtml(it, i, opts = {}) {
  const safeTitle = escHtml(it.title || it.identifier);
  const safeDate  = escHtml(it.date || "");
  // Row description shows a truncated plaintext preview — strip any
  // embedded HTML before slicing so we don't leave a half-open tag.
  const cleanDesc = _archiveCleanDesc(it.description || "");
  const safeDesc  = escHtml(cleanDesc.slice(0, 280));
  const safeId    = escHtml(it.identifier);
  // Cache no longer pre-resolves stream URLs (refresh used to take
  // 5+ min walking per-item metadata). We optimistically render Play /
  // Queue buttons enabled for every row; the click handler resolves
  // the stream on demand via /api/archive/item/:id and shows a toast
  // if the item turns out to have no audio.
  const playable  = true;
  const isSaved   = !!_archiveSavedIds?.has(it.identifier);
  const saveBtn = `<button type="button" class="archive-btn archive-save-btn${isSaved ? " is-saved" : ""}" onclick="archiveToggleSave(this)" title="${isSaved ? "Remove from Saved" : "Save to your list"}">${isSaved ? "★" : "☆"}</button>`;
  // Saved-view rows source from the row's data-* (since `i` is
  // meaningless without _archiveList). Search-view rows have their
  // own array so they get an index-based handler that reads from
  // _archiveSearchResults instead of _archiveList. Default (curated)
  // uses the original index path.
  const playHandler  = opts.savedView  ? `archivePlaySavedFromRow(this)`
                     : opts.searchView ? `archivePlaySearchItem(${i})`
                     :                   `archivePlayItem(${i})`;
  const queueHandler = opts.savedView  ? `archiveQueueSavedFromRow(this)`
                     : opts.searchView ? `archiveQueueSearchItem(${i})`
                     :                   `archiveQueueItem(${i})`;
  const playBtn = playable
    ? `<button type="button" class="archive-btn archive-btn-play" onclick="${playHandler}" title="Play in the bar">▶ Play</button>`
    : `<button type="button" class="archive-btn archive-btn-play is-disabled" disabled>▶ No stream</button>`;
  const queueBtn = playable
    ? `<button type="button" class="archive-btn archive-btn-queue" onclick="${queueHandler}" title="Add to play queue">＋ Queue</button>`
    : "";
  // External "Open on archive.org" link removed per request — the
  // info popup ("⓵") and the in-app player cover the same ground
  // without leaving the site.
  const linkBtn = "";
  // Stash the data needed for play/queue from the saved view onto the
  // row itself so the saved-row handlers don't need a separate cache.
  const dataAttrs = opts.savedView
    ? ` data-stream="${escHtml(it.streamUrl || "")}" data-title="${escHtml(it.title || "")}" data-date="${escHtml(it.date || "")}"`
    : "";
  // Clicking the title/main area opens the rich info popup. The
  // actions column (▶ / ＋ / ★ / Open) keeps its own click handlers
  // and event-bubbling stops at .archive-row-actions to avoid
  // triggering the popup as a side effect.
  // Creator is only present on search-result rows (the upstream
  // advancedsearch payload includes it); curated/saved rows leave it
  // blank. Surface it as a small line above the date when set so
  // search results read with author context.
  const safeCreator = it.creator ? escHtml(String(it.creator).slice(0, 120)) : "";
  // Subject chips (search-tab only — curated rows skip this since
  // every item shares the collection's subject). Cap at 3 to avoid
  // a chip wall on items tagged into many collections.
  const subjectChipsHtml = (Array.isArray(it.subject) && it.subject.length)
    ? `<div class="archive-row-chips">${it.subject.slice(0, 3).map(s =>
         `<span class="archive-row-chip">${escHtml(String(s))}</span>`
       ).join("")}</div>`
    : "";
  // Rating + reviews. avgRating is 0-5; only show if at least one
  // review exists so we don't surface a stale "★ 0 (0)" on every row.
  const ratingHtml = (typeof it.avgRating === "number" && (it.numReviews || 0) > 0)
    ? `<span class="archive-row-rating" title="${escHtml(String(it.numReviews))} review${it.numReviews === 1 ? "" : "s"}">★ ${it.avgRating.toFixed(1)}<span class="archive-row-rating-count"> · ${it.numReviews}</span></span>`
    : "";
  // Collection badge — first non-meta collection from the array
  // (skip system tags like "audio", "podcasts", which are present
  // on every row and not informative). Slug-form is what archive
  // returns, so we display it as-is without trying to resolve to
  // a friendlier name.
  const _COLLECTION_HIDE = new Set(["audio", "audio_podcast", "audio_music"]);
  const visibleCollection = (Array.isArray(it.collection) ? it.collection : [])
    .find(c => c && !_COLLECTION_HIDE.has(String(c).toLowerCase()));
  const collectionBadgeHtml = visibleCollection
    ? `<span class="archive-row-collection" title="archive.org collection">${escHtml(String(visibleCollection))}</span>`
    : "";
  return `
    <div class="archive-row" data-id="${safeId}"${dataAttrs}>
      <div class="archive-row-main" onclick="_archiveOpenInfoPopup('${safeId.replace(/'/g, "\\'")}')" style="cursor:pointer">
        <div class="archive-row-title">${safeTitle}</div>
        ${safeCreator ? `<div class="archive-row-creator">${safeCreator}</div>` : ""}
        ${(safeDate || ratingHtml || collectionBadgeHtml) ? `<div class="archive-row-meta-row">
          ${safeDate ? `<span class="archive-row-date">${safeDate}</span>` : ""}
          ${ratingHtml}
          ${collectionBadgeHtml}
        </div>` : ""}
        ${subjectChipsHtml}
        ${safeDesc ? `<div class="archive-row-desc">${safeDesc}${cleanDesc.length > 280 ? "…" : ""}</div>` : ""}
      </div>
      <div class="archive-row-actions">${saveBtn}${playBtn}${queueBtn}${linkBtn}</div>
    </div>
  `;
}

function _archiveOnFilterInput(input) {
  _archiveFilter = input.value || "";
  _archiveRenderRowsOnly();
}
function _archiveOnSortChange(select) {
  _archiveSort = select.value || "title-asc";
  _archiveRenderRowsOnly();
}

// ── Top-level page render (tabs + the active panel) ──────────────────
function _renderArchiveList() {
  const listEl = document.getElementById("archive-list");
  if (!listEl) return;
  const filterVal = escHtml(_archiveFilter);
  const sortOpts = [
    ["title-asc",  "Title A → Z"],
    ["title-desc", "Title Z → A"],
    ["date-desc",  "Date (newest)"],
    ["date-asc",   "Date (oldest)"],
  ].map(([v, label]) => `<option value="${v}"${v === _archiveSort ? " selected" : ""}>${label}</option>`).join("");
  const savedSortOpts = [
    ["recent",    "Recently saved"],
    ["title",     "Title A → Z"],
    ["date-desc", "Date (newest)"],
    ["date-asc",  "Date (oldest)"],
  ].map(([v, label]) => `<option value="${v}"${v === _archiveSavedSort ? " selected" : ""}>${label}</option>`).join("");
  const savedFilterVal = escHtml(_archiveSavedFilter);
  // Three-tab strip. Inactive panels stay in the DOM hidden with
  // display:none so input/filter state persists across switches.
  const showSearch  = _archiveTab === "search";
  const showCurated = _archiveTab === "curated";
  const showSaved   = _archiveTab === "saved";
  const searchQ     = escHtml(_archiveSearchQuery || "");
  const curatedTotal = _archiveList?.length || 0;
  // Search results meta: "N results" or "Loading…" or empty.
  const searchMeta = _archiveSearchLoading
    ? "Searching…"
    : (_archiveSearchResults
        ? `${_archiveSearchNumFound.toLocaleString()} result${_archiveSearchNumFound === 1 ? "" : "s"}${
            _archiveSearchResults.length < _archiveSearchNumFound
              ? ` · showing ${_archiveSearchResults.length}` : ""
          }`
        : "");
  listEl.innerHTML = `
    <div class="loc-tabs archive-tabs">
      <button type="button" class="loc-tab archive-tab archive-tab-search${showSearch ? " active" : ""}" onclick="_archiveSwitchTab('search')">Search</button>
      <button type="button" class="loc-tab archive-tab archive-tab-curated${showCurated ? " active" : ""}" onclick="_archiveSwitchTab('curated')">Curated</button>
      <button type="button" class="loc-tab archive-tab archive-tab-saved${showSaved ? " active" : ""}" onclick="_archiveSwitchTab('saved')">Saved</button>
    </div>

    <div class="archive-panel archive-panel-search" style="display:${showSearch ? "" : "none"}">
      <form class="loc-form archive-search-form" onsubmit="event.preventDefault();_archiveOnSearchSubmit(this)">
        <div class="loc-form-row">
          <input type="search" id="archive-q" name="q" placeholder="Search audio on archive.org" autocomplete="off" value="${searchQ}" />
          <button type="submit" class="loc-submit">Search</button>
        </div>
        <div class="loc-form-grid">
          <label><span>Creator</span><input type="text" id="archive-creator" placeholder="e.g. Grateful Dead" value="${escHtml(_archiveSearchCreator)}" /></label>
          <label><span>Subject / genre</span><input type="text" id="archive-subject" placeholder="e.g. Jazz, Live, Folk" value="${escHtml(_archiveSearchSubject)}" /></label>
          <label><span>Collection</span><input type="text" id="archive-collection" placeholder="e.g. etree, audio_music" value="${escHtml(_archiveSearchCollection)}" /></label>
          <label><span>Category</span>
            <select id="archive-category">
              ${_ARCHIVE_CATEGORY_OPTIONS.map(([val, label]) =>
                `<option value="${escHtml(val)}"${val === _archiveSearchCategory ? " selected" : ""}>${escHtml(label)}</option>`
              ).join("")}
            </select>
          </label>
          <label><span>Year from</span><input type="text" id="archive-year-from" placeholder="yyyy" inputmode="numeric" maxlength="4" value="${escHtml(_archiveSearchYearFrom)}" /></label>
          <label><span>Year to</span><input type="text" id="archive-year-to" placeholder="yyyy" inputmode="numeric" maxlength="4" value="${escHtml(_archiveSearchYearTo)}" /></label>
          <label class="loc-form-split"><span>Sort</span>
            <select id="archive-sort-select">
              <option value="popularity"${_archiveSearchSort === "popularity" ? " selected" : ""}>Most popular</option>
              <option value="newest"${_archiveSearchSort === "newest" ? " selected" : ""}>Newest upload</option>
              <option value="oldest"${_archiveSearchSort === "oldest" ? " selected" : ""}>Oldest upload</option>
              <option value="showNewest"${_archiveSearchSort === "showNewest" ? " selected" : ""}>Show date (newest)</option>
              <option value="showOldest"${_archiveSearchSort === "showOldest" ? " selected" : ""}>Show date (oldest)</option>
              <option value="titleAsc"${_archiveSearchSort === "titleAsc" ? " selected" : ""}>Title A → Z</option>
              <option value="titleDesc"${_archiveSearchSort === "titleDesc" ? " selected" : ""}>Title Z → A</option>
              <option value="rated"${_archiveSearchSort === "rated" ? " selected" : ""}>Highest rated</option>
            </select>
          </label>
        </div>
      </form>
      <div class="archive-meta-bar archive-meta-bar-search">
        <span class="archive-meta-count" id="archive-search-count">${searchMeta}</span>
      </div>
      <div id="archive-search-rows">${
        _archiveSearchResults && !_archiveSearchResults.length && _archiveSearchQuery
          ? `<div class="loc-empty">No playable audio matches for "${escHtml(_archiveSearchQuery)}". Try a broader query.</div>`
          : _archiveSearchResults
            ? ""  // populated below by _archiveRenderSearchRows()
            : (_archiveSearchQuery
                ? `<div class="loc-empty">Searching…</div>`
                : "")  // empty state — no placeholder blurb
      }</div>
    </div>

    <div class="archive-panel archive-panel-curated" style="display:${showCurated ? "" : "none"}">
      <div class="archive-curated-picker">
        <label class="archive-curated-picker-label" for="archive-curated-select">Collection</label>
        <select id="archive-curated-select" class="archive-curated-select" onchange="_archiveCuratedSelect(this)">${
          (_archiveCuratedList || [{ slug: "aadamjacobs", title: "Aadam Jacobs" }])
            .map(c => `<option value="${escHtml(c.slug)}"${c.slug === _archiveCuratedSlug ? " selected" : ""}>${escHtml(c.title)}</option>`)
            .join("")
        }</select>
      </div>
      <div class="archive-meta-bar">
        <input type="text" class="archive-filter" placeholder="Filter title, date, description…" value="${filterVal}" oninput="_archiveOnFilterInput(this)" />
        <select class="archive-sort" onchange="_archiveOnSortChange(this)">${sortOpts}</select>
        <span class="archive-meta-count" id="archive-count">${
          _archiveList ? `${curatedTotal} item${curatedTotal === 1 ? "" : "s"}` : "Loading…"
        }</span>
      </div>
      <div id="archive-rows">${
        _archiveList && !_archiveList.length ? `<div class="loc-empty">No items in this collection.</div>` : ""
      }</div>
    </div>

    <div class="archive-panel archive-panel-saved" style="display:${showSaved ? "" : "none"}">
      <div class="archive-meta-bar">
        <input type="text" class="archive-filter" placeholder="Filter saved…" value="${savedFilterVal}" oninput="_archiveOnSavedFilterInput(this)" />
        <select class="archive-sort" onchange="_archiveOnSavedSortChange(this)">${savedSortOpts}</select>
        <span class="archive-meta-count"><span id="archive-saved-count">0</span> saved</span>
      </div>
      <div id="archive-saved-rows"><div class="loc-empty">Loading…</div></div>
    </div>
  `;
  // Populate the active panel's row content.
  if (showSearch && _archiveSearchResults?.length) {
    _archiveRenderSearchRows();
  }
  if (showCurated && _archiveList?.length) {
    _archiveRenderRowsOnly();
  }
  if (showSaved) {
    _archiveLoadSaved();
  }
  // Mount the bookmark dropdown into the search-row whenever the
  // search panel is visible. Idempotent — guards on the existing
  // .saved-search-wrap so the form re-render in this function
  // doesn't keep re-mounting it.
  if (showSearch && typeof buildSavedSearchUI === "function" && window._clerk?.user) {
    const formRow = document.querySelector(".archive-panel-search .loc-form-row");
    if (formRow && !formRow.querySelector(".saved-search-wrap")) {
      buildSavedSearchUI(
        "archive",
        () => {
          const out = {};
          const get = (id, key) => {
            const v = document.getElementById(id)?.value?.trim();
            if (v) out[key || id] = v;
          };
          get("archive-q",          "q");
          get("archive-creator",    "creator");
          get("archive-subject",    "subject");
          get("archive-collection", "collection");
          get("archive-year-from",  "yearFrom");
          get("archive-year-to",    "yearTo");
          const cat = document.getElementById("archive-category")?.value;
          if (cat && cat !== "music") out.cat = cat;
          const sort = document.getElementById("archive-sort-select")?.value;
          if (sort && sort !== "popularity") out.sort = sort;
          return out;
        },
        (params) => {
          const setVal = (id, key) => {
            const el = document.getElementById(id);
            if (el) el.value = params[key] ?? "";
          };
          setVal("archive-q",          "q");
          setVal("archive-creator",    "creator");
          setVal("archive-subject",    "subject");
          setVal("archive-collection", "collection");
          setVal("archive-year-from",  "yearFrom");
          setVal("archive-year-to",    "yearTo");
          const catEl = document.getElementById("archive-category");
          if (catEl) catEl.value = params.cat || "music";
          const sortEl = document.getElementById("archive-sort-select");
          if (sortEl) sortEl.value = params.sort || "popularity";
          // Submit the form so state + URL update through the same
          // path the user-driven submit uses.
          const form = document.querySelector(".archive-panel-search .archive-search-form");
          if (form && typeof _archiveOnSearchSubmit === "function") {
            _archiveOnSearchSubmit(form);
          }
        },
        formRow,
      );
    }
  }
}

// ── Search tab handlers ─────────────────────────────────────────────
// Submit handler for the form. Stashes the query, kicks off the
// fetch, re-renders. URL gets ?q= so a shared link reproduces the
// search.
function _archiveOnSearchSubmit(form) {
  // Read every visible filter from the form. Falls back to empty
  // strings if the inputs aren't present (filter panel collapsed).
  const get = (id) => String(document.getElementById(id)?.value ?? "").trim();
  _archiveSearchQuery       = get("archive-q");
  _archiveSearchCreator     = get("archive-creator");
  _archiveSearchSubject     = get("archive-subject");
  _archiveSearchCollection  = get("archive-collection");
  _archiveSearchYearFrom    = get("archive-year-from");
  _archiveSearchYearTo      = get("archive-year-to");
  const sortSel = document.getElementById("archive-sort-select");
  if (sortSel) _archiveSearchSort = sortSel.value || "popularity";
  const catSel = document.getElementById("archive-category");
  if (catSel) _archiveSearchCategory = catSel.value || "music";
  _archiveSearchPage = 1;
  // Reflect every populated filter in the URL so a shared link
  // round-trips the same search.
  if (typeof history?.pushState === "function") {
    const qs = new URLSearchParams(location.search);
    qs.set("v", "archive");
    qs.set("tab", "search");
    const setOrDel = (key, val) => {
      if (val) qs.set(key, val); else qs.delete(key);
    };
    setOrDel("q",       _archiveSearchQuery);
    setOrDel("creator", _archiveSearchCreator);
    setOrDel("subject", _archiveSearchSubject);
    setOrDel("col2",    _archiveSearchCollection);
    setOrDel("yf",      _archiveSearchYearFrom);
    setOrDel("yt",      _archiveSearchYearTo);
    if (_archiveSearchSort && _archiveSearchSort !== "popularity") qs.set("sort", _archiveSearchSort);
    else qs.delete("sort");
    if (_archiveSearchCategory && _archiveSearchCategory !== "music") qs.set("cat", _archiveSearchCategory);
    else qs.delete("cat");
    qs.delete("ep");  // legacy param from the old Exclude-Podcasts checkbox
    const next = "/?" + qs.toString();
    if (location.pathname + location.search !== next) {
      history.pushState({}, "", next);
    }
  }
  // Empty form → clear results. Category counts as a filter when
  // it narrows below "all" — see _archiveDoSearch for the matching
  // gate. Without this, picking "All music" + a sort and submitting
  // would silently no-op because none of the freeform inputs are
  // set.
  const _categoryNarrows = _archiveSearchCategory && _archiveSearchCategory !== "all";
  const anyFilter = !!(_archiveSearchQuery || _archiveSearchCreator || _archiveSearchSubject ||
                       _archiveSearchCollection || _archiveSearchYearFrom || _archiveSearchYearTo ||
                       _categoryNarrows);
  if (!anyFilter) {
    _archiveSearchResults = null;
    _archiveSearchNumFound = 0;
    _renderArchiveList();
    return;
  }
  // Auto-save every populated input into the per-field history so
  // the focus-dropdown surfaces past values on the next visit.
  if (typeof saveSearchHistory === "function") {
    try { saveSearchHistory("archive"); } catch {}
  }
  _archiveDoSearch().catch(() => {});
}
window._archiveOnSearchSubmit = _archiveOnSearchSubmit;

// (Filter-panel toggle retired — filters are always visible now.)

async function _archiveDoSearch() {
  // A search is meaningful as long as ANY filter is set — q is no
  // longer required by itself. Category dropdown counts: anything
  // other than "all" maps to a real collection constraint, so a
  // category-only search ("All music" + Sort=highest rated, nothing
  // else) is valid.
  const _categoryNarrows = _archiveSearchCategory && _archiveSearchCategory !== "all";
  const anyFilter = !!(_archiveSearchQuery
    || _archiveSearchCreator
    || _archiveSearchSubject
    || _archiveSearchCollection
    || _archiveSearchYearFrom
    || _archiveSearchYearTo
    || _categoryNarrows);
  if (!anyFilter) return;
  if (_archiveSearchLoading) return;
  _archiveSearchLoading = true;
  // Show the loading state without losing the previous results — the
  // input + tab strip stay in place.
  _renderArchiveList();
  try {
    const u = new URL("/api/archive/search", location.origin);
    if (_archiveSearchQuery)      u.searchParams.set("q", _archiveSearchQuery);
    if (_archiveSearchCreator)    u.searchParams.set("creator", _archiveSearchCreator);
    if (_archiveSearchSubject)    u.searchParams.set("subject", _archiveSearchSubject);
    if (_archiveSearchCollection) u.searchParams.set("collection", _archiveSearchCollection);
    if (_archiveSearchYearFrom)   u.searchParams.set("yearFrom", _archiveSearchYearFrom);
    if (_archiveSearchYearTo)     u.searchParams.set("yearTo", _archiveSearchYearTo);
    u.searchParams.set("sort", _archiveSearchSort);
    // Category dropdown → server params:
    //   "music"  → union of music-y collections
    //              (audio_music + etree + 78rpm + opensource_audio).
    //              Sent as a comma-separated `collection=` and the
    //              server ORs them. Tighter than "everything except
    //              podcasts" — only items in real music collections.
    //   "all"    → no extra constraint (every audio item).
    //   anything else → constrain to that single collection.
    const cat = _archiveSearchCategory || "music";
    if (cat === "music") {
      u.searchParams.set("collection", "audio_music,etree,78rpm,opensource_audio");
      u.searchParams.set("excludePodcasts", "0");  // music collections inherently exclude podcasts
    } else if (cat === "all") {
      u.searchParams.set("excludePodcasts", "0");
    } else {
      u.searchParams.set("excludePodcasts", "0");
      u.searchParams.set("collection", cat);
    }
    u.searchParams.set("page", String(_archiveSearchPage));
    u.searchParams.set("rows", String(_archiveSearchRows));
    const r = await apiFetch(u.pathname + u.search);
    if (r.status === 429) {
      const body = await r.json().catch(() => ({}));
      _archiveSearchResults = [];
      _archiveSearchNumFound = 0;
      if (typeof showToast === "function") {
        showToast(body?.message || "Too many archive searches — wait a minute.", "error");
      }
      return;
    }
    if (!r.ok) {
      _archiveSearchResults = [];
      _archiveSearchNumFound = 0;
      return;
    }
    const j = await r.json();
    _archiveSearchResults  = Array.isArray(j?.items) ? j.items : [];
    _archiveSearchNumFound = Number(j?.numFound || 0);
  } catch {
    _archiveSearchResults = [];
    _archiveSearchNumFound = 0;
  } finally {
    _archiveSearchLoading = false;
    _renderArchiveList();
  }
}

// Render search results into #archive-search-rows. Keeps the row
// markup identical to curated/saved (same _archiveRowHtml), but
// passes searchView:true so the play / queue handlers route to the
// search-results array instead of the curated _archiveList.
function _archiveRenderSearchRows() {
  const rowsEl = document.getElementById("archive-search-rows");
  if (!rowsEl) return;
  const items = _archiveSearchResults || [];
  if (!items.length) {
    rowsEl.innerHTML = `<div class="loc-empty">No playable audio matches.</div>`;
    return;
  }
  rowsEl.innerHTML = items.map((it, i) => _archiveRowHtml(it, i, { searchView: true })).join("");
  if (typeof _locUpdatePlayingCard === "function") _locUpdatePlayingCard();
}
window._archiveRenderSearchRows = _archiveRenderSearchRows;

// Search-row Play / Queue handlers — mirror archivePlayItem /
// archiveQueueItem but read from _archiveSearchResults so a row
// click resolves the right item even with multiple lists in scope.
async function archivePlaySearchItem(idx) {
  const it = _archiveSearchResults?.[idx];
  if (!it?.identifier) return;
  const meta = await _archiveResolveMeta(it.identifier);
  const items = _archiveItemsFromMeta(meta);
  if (!items.length) {
    if (typeof showToast === "function") showToast("This item has no playable audio", "error");
    return;
  }
  if (typeof queueAddAlbumOrPlay === "function") {
    await queueAddAlbumOrPlay(items, { mode: "play" });
  }
}
window.archivePlaySearchItem = archivePlaySearchItem;

async function archiveQueueSearchItem(idx) {
  const it = _archiveSearchResults?.[idx];
  if (!it?.identifier) return;
  const meta = await _archiveResolveMeta(it.identifier);
  const items = _archiveItemsFromMeta(meta);
  if (!items.length) {
    if (typeof showToast === "function") showToast("This item has no playable audio", "error");
    return;
  }
  if (typeof queueAddAlbumOrPlay === "function") {
    await queueAddAlbumOrPlay(items, { mode: "append" });
  }
}
window.archiveQueueSearchItem = archiveQueueSearchItem;

function _archiveSwitchTab(tab, { pushUrl = true } = {}) {
  // Normalize: legacy "browse" param maps to the renamed "curated" tab.
  if (tab === "browse") tab = "curated";
  _archiveTab = (tab === "saved" || tab === "curated") ? tab : "search";
  // Toggle tab button + panel visibility without re-rendering everything
  // (preserves filter/sort state in all panels).
  document.querySelectorAll(".archive-tab").forEach(b => b.classList.remove("active"));
  document.querySelector(`.archive-tab-${_archiveTab}`)?.classList.add("active");
  const search  = document.querySelector(".archive-panel-search");
  const curated = document.querySelector(".archive-panel-curated");
  const saved   = document.querySelector(".archive-panel-saved");
  if (search)  search.style.display  = _archiveTab === "search"  ? "" : "none";
  if (curated) curated.style.display = _archiveTab === "curated" ? "" : "none";
  if (saved)   saved.style.display   = _archiveTab === "saved"   ? "" : "none";
  // Lazy load whichever tab needs data.
  if (_archiveTab === "curated" && !_archiveList?.length) {
    _archiveLoadCurated().catch(() => {});
  }
  if (_archiveTab === "saved" && _archiveSavedItems == null) {
    _archiveLoadSaved();
  }
  if (_archiveTab === "search") {
    // Focus the input on tab activation so the user can start typing
    // immediately. Skip if there's already a query loaded — they may
    // be reviewing results.
    if (!_archiveSearchQuery) {
      setTimeout(() => document.getElementById("archive-q")?.focus(), 0);
    }
  }
  // Reflect in URL so each tab is shareable. Search tab also persists
  // its q param.
  if (pushUrl && typeof history?.pushState === "function") {
    const qs = new URLSearchParams(location.search);
    qs.set("v", "archive");
    if (_archiveTab === "search") {
      qs.set("tab", "search");
      if (_archiveSearchQuery) qs.set("q", _archiveSearchQuery); else qs.delete("q");
    } else if (_archiveTab === "saved") {
      qs.set("tab", "saved");
      qs.delete("q");
    } else {
      qs.set("tab", "curated");
      qs.delete("q");
    }
    const next = "/?" + qs.toString();
    if (location.pathname + location.search !== next) {
      history.pushState({}, "", next);
    }
  }
}

// Convert an archive item into a LOC-shaped object so _locPlay can play
// it through the existing engine. archive.org and LOC both serve direct
// mp3 streams — same code path, different metadata source.
function _archiveItemToLoc(it) {
  // Pull contributor(s) from the upstream item if present; the search-
  // tab payload includes `creator`, the curated payload doesn't.
  const cr = it.creator
    ? (Array.isArray(it.creator) ? it.creator : [it.creator])
    : [];
  return {
    id:           it.identifier,                       // unique key; not a URL but unique enough
    title:        it.title || it.identifier,
    streamUrl:    it.streamUrl,
    streamType:   "mp3",
    contributors: cr,
    image:        "",
    year:         (it.date || "").slice(0, 4),
  };
}

// Resolve full archive item metadata (incl. all audio files) via the
// existing /api/archive/item/:id endpoint, which has its own 24h
// memory cache. Used by row Play / Queue buttons to enumerate every
// track in a show, not just the primary stream — matches the popup's
// "Play album" semantics so a row click ≡ a popup ▶ click.
async function _archiveResolveMeta(identifier) {
  if (!identifier) return null;
  if (_archiveInfoCache.has(identifier)) return _archiveInfoCache.get(identifier);
  try {
    const r = await apiFetch(`/api/archive/item/${encodeURIComponent(identifier)}`);
    if (!r.ok) return null;
    const j = await r.json();
    _archiveInfoCachePut(identifier, j);
    return j;
  } catch { return null; }
}

// Build cross-source queue items for every audio file in an archive
// show. Each track gets a stable per-file externalId so dedup-by-id
// in queueAddAlbumOrPlay catches "already in queue" correctly.
function _archiveItemsFromMeta(d) {
  if (!Array.isArray(d?.audioFiles)) return [];
  // Resolve the show-level artist from the metadata. The page used to
  // default to "Aadam Jacobs" since it was a one-collection viewer;
  // now the search tab can surface anyone, so fall back to the
  // upstream `creator` (string OR string[]) before any hardcoded name.
  const showArtist =
    (Array.isArray(d.creator) ? d.creator[0] : d.creator) ||
    d.uploader || d.identifier || "";
  return d.audioFiles.map(f => ({
    source: "loc",
    externalId: `${d.identifier}#${f.name}`,
    data: {
      title:      f.title || f.name.replace(/\.[^.]+$/, ""),
      artist:     showArtist,
      streamUrl:  f.streamUrl,
      streamType: "mp3",
      image:      d.coverUrl || "",
      year:       (d.date || "").slice(0, 4),
    },
  }));
}

async function archivePlayItem(idx) {
  const it = _archiveList?.[idx];
  if (!it?.identifier) return;
  const meta = await _archiveResolveMeta(it.identifier);
  const items = _archiveItemsFromMeta(meta);
  if (!items.length) {
    if (typeof showToast === "function") showToast("This item has no playable audio", "error");
    return;
  }
  if (typeof queueAddAlbumOrPlay === "function") {
    await queueAddAlbumOrPlay(items, { mode: "play" });
  }
}

async function archiveQueueItem(idx) {
  const it = _archiveList?.[idx];
  if (!it?.identifier) return;
  const meta = await _archiveResolveMeta(it.identifier);
  const items = _archiveItemsFromMeta(meta);
  if (!items.length) {
    if (typeof showToast === "function") showToast("This item has no playable audio", "error");
    return;
  }
  if (typeof queueAddAlbumOrPlay === "function") {
    await queueAddAlbumOrPlay(items, { mode: "append" });
  }
}

// Saved-row variants — the row knows its own data via data-* attrs
// since the saved tab's source array is _archiveSavedItems, not
// _archiveList, and a numeric index would be ambiguous after filter/
// sort. Reads identifier + stream + title from the row dataset.
function _archiveItemFromRow(rowEl) {
  if (!rowEl) return null;
  return {
    identifier: rowEl.dataset.id,
    streamUrl:  rowEl.dataset.stream || "",
    title:      rowEl.dataset.title  || rowEl.dataset.id,
    date:       rowEl.dataset.date   || "",
  };
}

async function archivePlaySavedFromRow(btn) {
  const row = btn?.closest(".archive-row");
  const it = _archiveItemFromRow(row);
  if (!it?.identifier) return;
  const meta = await _archiveResolveMeta(it.identifier);
  const items = _archiveItemsFromMeta(meta);
  if (!items.length) {
    if (typeof showToast === "function") showToast("This item has no playable audio", "error");
    return;
  }
  if (typeof queueAddAlbumOrPlay === "function") {
    await queueAddAlbumOrPlay(items, { mode: "play" });
  }
}

async function archiveQueueSavedFromRow(btn) {
  const row = btn?.closest(".archive-row");
  const it = _archiveItemFromRow(row);
  if (!it?.identifier) return;
  const meta = await _archiveResolveMeta(it.identifier);
  const items = _archiveItemsFromMeta(meta);
  if (!items.length) {
    if (typeof showToast === "function") showToast("This item has no playable audio", "error");
    return;
  }
  if (typeof queueAddAlbumOrPlay === "function") {
    await queueAddAlbumOrPlay(items, { mode: "append" });
  }
}

// ── Archive item info popup ──────────────────────────────────────────
// Click a row's title (or the new ⓘ button) to open a rich Discogs-
// style popup with creator / date / description / per-track play
// buttons / subjects / license / cover art / archive.org link. Server
// fetches /metadata/{id} and curates the response (1d cache).
let _archiveInfoCache = new Map(); // identifier → curated metadata
let _archiveInfoCurrentId = null;
// Curated metadata blobs are 5-30KB each (tracklist, description, file
// list). After a session of poking through 100 items this would have
// been 1-3MB pinned. LRU cap at 100 keeps memory bounded.
const _ARCHIVE_INFO_CACHE_MAX = 100;
function _archiveInfoCachePut(id, data) {
  if (!id) return;
  if (_archiveInfoCache.has(id)) _archiveInfoCache.delete(id);
  _archiveInfoCache.set(id, data);
  if (_archiveInfoCache.size > _ARCHIVE_INFO_CACHE_MAX) {
    const it = _archiveInfoCache.keys();
    const drop = _archiveInfoCache.size - Math.floor(_ARCHIVE_INFO_CACHE_MAX * 0.75);
    for (let i = 0; i < drop; i++) {
      const k = it.next().value;
      if (k === undefined) break;
      _archiveInfoCache.delete(k);
    }
  }
}

async function _archiveOpenInfoPopup(identifier) {
  if (!identifier) return;
  const overlay = document.getElementById("archive-info-overlay");
  const body    = document.getElementById("archive-info-body");
  if (!overlay || !body) return;
  _archiveInfoCurrentId = identifier;
  overlay.classList.add("open");
  body.innerHTML = `<div class="loc-empty">Loading…</div>`;
  let data = _archiveInfoCache.get(identifier);
  if (!data) {
    try {
      const r = await apiFetch(`/api/archive/item/${encodeURIComponent(identifier)}`);
      if (r.status === 429) {
        const errBody = await r.json().catch(() => ({}));
        body.innerHTML = `<div class="loc-empty">${escHtml(errBody?.message || "Too many Archive requests from your network — try again in a minute.")}</div>`;
        return;
      }
      if (!r.ok) {
        body.innerHTML = `<div class="loc-empty">Could not load item details (HTTP ${r.status}).</div>`;
        return;
      }
      data = await r.json();
      _archiveInfoCachePut(identifier, data);
    } catch {
      body.innerHTML = `<div class="loc-empty">Could not load item details.</div>`;
      return;
    }
  }
  // If the user navigated/closed before the fetch landed, drop this render.
  if (_archiveInfoCurrentId !== identifier) return;
  body.innerHTML = _archiveInfoPopupHtml(data);
}

function _archiveCloseInfoPopup() {
  const overlay = document.getElementById("archive-info-overlay");
  if (overlay) overlay.classList.remove("open");
  _archiveInfoCurrentId = null;
}

// archive.org descriptions often contain raw HTML — <div> for line
// breaks, <br>, &nbsp; entities, etc. Render those as plain text with
// real newlines so the popup doesn't display literal `<div>` markup.
function _archiveCleanDesc(html) {
  if (!html) return "";
  const withBreaks = String(html)
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(?:div|p|li|h[1-6])>/gi, "\n")
    .replace(/<(?:div|p|li|h[1-6])[^>]*>/gi, "")
    .replace(/<[^>]+>/g, "");
  // Decode common entities so &amp; etc. don't show as literal text.
  const decoded = withBreaks
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'");
  return decoded.replace(/\n{3,}/g, "\n\n").trim();
}

// Format archive.org duration strings — they're sometimes "MM:SS",
// sometimes raw seconds like "245.32". Normalize to "M:SS" or "H:MM:SS".
function _archiveFmtDuration(s) {
  if (!s) return "";
  if (/^\d+:\d+/.test(s)) return s; // already formatted
  const num = parseFloat(s);
  if (!Number.isFinite(num) || num <= 0) return "";
  const total = Math.floor(num);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const sec = total % 60;
  if (h) return `${h}:${m < 10 ? "0" : ""}${m}:${sec < 10 ? "0" : ""}${sec}`;
  return `${m}:${sec < 10 ? "0" : ""}${sec}`;
}

function _archiveInfoPopupHtml(d) {
  const esc = (v) => escHtml(String(v ?? ""));
  const isSaved = !!_archiveSavedIds?.has(d.identifier);
  const playable = !!d.primaryStreamUrl;

  // Header: cover image + title block (creator / date / venue / runtime).
  // Falls back to a placeholder div if the image 404s — archive.org's
  // services/img endpoint serves a generic icon for items with no
  // explicit thumbnail, but we keep an onerror guard anyway.
  const coverHtml = d.coverUrl
    ? `<img class="archive-info-cover" src="${esc(d.coverUrl)}" alt="" onerror="this.style.display='none'">`
    : `<div class="archive-info-cover archive-info-cover-empty">♪</div>`;

  const creatorLine = (d.creator?.length)
    ? `<div class="archive-info-creator">${d.creator.map(c => entityLookupLinkHtml("artist", c, { title: `Lookup options for ${c}` })).join(", ")}</div>`
    : "";

  const metaBits = [];
  if (d.date)     metaBits.push(esc(d.date));
  if (d.venue)    metaBits.push(esc(d.venue));
  if (d.coverage) metaBits.push(esc(d.coverage));
  if (d.runtime)  metaBits.push(esc(d.runtime));
  const metaLine = metaBits.length
    ? `<div class="archive-info-meta-line">${metaBits.join('<span class="archive-info-meta-sep"> · </span>')}</div>`
    : "";

  // Action buttons row — Play (top-of-item), Queue All, Save, Open on
  // archive.org. Mirrors the LOC popup's action bar.
  const playBtn = playable
    ? `<button type="button" class="archive-btn archive-btn-play" onclick="_archiveInfoPlayPrimary('${esc(d.identifier).replace(/'/g, "\\'")}')">▶ Play</button>`
    : `<button type="button" class="archive-btn archive-btn-play is-disabled" disabled>▶ No stream</button>`;
  const queueBtn = playable
    ? `<button type="button" class="archive-btn archive-btn-queue" onclick="_archiveInfoQueueAll('${esc(d.identifier).replace(/'/g, "\\'")}')">＋ Queue all tracks</button>`
    : "";
  const saveBtn = `<button type="button" class="archive-btn archive-save-btn${isSaved ? " is-saved" : ""}" onclick="_archiveInfoToggleSave(this, '${esc(d.identifier).replace(/'/g, "\\'")}')">${isSaved ? "★ Saved" : "☆ Save"}</button>`;
  const linkBtn = `<a class="archive-btn archive-btn-link" href="${esc(d.itemUrl)}" target="_blank" rel="noopener">Open on archive.org ↗</a>`;

  // Description — strip embedded HTML markup, escape, then convert
  // newlines to <br>. archive.org item descriptions frequently contain
  // raw <div>/<br>/&entities; in their `description` field.
  const descText = _archiveCleanDesc(d.description);
  const descHtml = descText
    ? `<div class="archive-info-desc">${escHtml(descText).replace(/\n/g, "<br>")}</div>`
    : "";

  // Audio file list — each row gets a ▶ play button + a ➕ queue.
  // Hidden when there's nothing playable.
  const filesHtml = d.audioFiles?.length
    ? `<div class="archive-info-files">
        <div class="archive-info-section-head">${d.audioFiles.length} audio file${d.audioFiles.length === 1 ? "" : "s"}</div>
        ${d.audioFiles.map((f, i) => {
          const fname = esc(f.name).replace(/\.(mp3|m4a|flac|ogg|wav)$/i, "");
          const dur = _archiveFmtDuration(f.length);
          const trackLabel = f.title || fname;
          return `<div class="archive-info-file-row">
            <button class="archive-info-file-play" data-i="${i}" onclick="_archiveInfoPlayFile('${esc(d.identifier).replace(/'/g, "\\'")}', ${i})" title="Play this file">▶</button>
            <button class="archive-info-file-queue" data-i="${i}" onclick="_archiveInfoQueueFile('${esc(d.identifier).replace(/'/g, "\\'")}', ${i})" title="Add to queue">＋</button>
            <span class="archive-info-file-title">${esc(trackLabel)}</span>
            ${dur ? `<span class="archive-info-file-dur">${dur}</span>` : ""}
            ${f.format ? `<span class="archive-info-file-fmt">${esc(f.format)}</span>` : ""}
          </div>`;
        }).join("")}
      </div>`
    : "";

  // Detail grid — subjects, language, license, taper, source, etc.
  const detailRows = [];
  if (d.subject?.length)  detailRows.push(["Subjects",  d.subject.slice(0, 30).map(s => `<a class="archive-info-tag" href="https://archive.org/search.php?query=subject:%22${encodeURIComponent(s)}%22" target="_blank" rel="noopener">${esc(s)}</a>`).join(" ")]);
  if (d.language?.length) detailRows.push(["Language",  d.language.map(esc).join(", ")]);
  if (d.taper)            detailRows.push(["Taper",     esc(d.taper)]);
  if (d.source)           detailRows.push(["Source",    esc(d.source)]);
  if (d.uploader)         detailRows.push(["Uploaded by", `<a href="https://archive.org/details/@${encodeURIComponent(d.uploader)}" target="_blank" rel="noopener">${esc(d.uploader)}</a>`]);
  if (d.addeddate)        detailRows.push(["Added",     esc(d.addeddate.slice(0, 10))]);
  if (d.licenseurl)       detailRows.push(["License",   `<a href="${esc(d.licenseurl)}" target="_blank" rel="noopener">${esc(d.licenseurl.replace(/^https?:\/\//, ""))}</a>`]);
  if (d.collection?.length) {
    const colls = d.collection.slice(0, 8).map(c => `<a class="archive-info-tag" href="https://archive.org/details/${encodeURIComponent(c)}" target="_blank" rel="noopener">${esc(c)}</a>`).join(" ");
    detailRows.push(["Collections", colls]);
  }
  const detailGridHtml = detailRows.length
    ? `<div class="archive-info-detail-grid">${detailRows.map(([label, val]) => `<div class="archive-info-detail-label">${esc(label)}</div><div class="archive-info-detail-val">${val}</div>`).join("")}</div>`
    : "";

  // Reviews / ratings (when present).
  const ratingHtml = (d.avgRating || d.reviews)
    ? `<div class="archive-info-rating">${d.avgRating ? `★ ${parseFloat(d.avgRating).toFixed(2)}` : ""}${d.reviews ? ` <span style="color:#666">(${d.reviews} review${d.reviews === 1 ? "" : "s"})</span>` : ""}</div>`
    : "";

  return `
    <div class="archive-info-head">
      ${coverHtml}
      <div class="archive-info-head-text">
        <h2 class="archive-info-title">${esc(d.title)}</h2>
        ${creatorLine}
        ${metaLine}
        ${ratingHtml}
        <div class="archive-info-actions">${playBtn}${queueBtn}${saveBtn}${linkBtn}</div>
      </div>
    </div>
    ${descHtml}
    ${filesHtml}
    ${detailGridHtml}
  `;
}

// Action handlers used by the popup's inline onclicks.
function _archiveInfoLookup(id) {
  return _archiveInfoCache.get(id);
}
// Build the queue-shaped item list for an archive item. Shared by
// the ▶ Play and ＋ Queue handlers so dedup-by-externalId in
// queueAddAlbumOrPlay sees consistent ids.
function _archiveItemsForAlbum(id, d) {
  return d.audioFiles.map(f => ({
    source: "loc",
    externalId: `${id}#${f.name}`,
    data: {
      title:      f.title || f.name.replace(/\.[^.]+$/, ""),
      artist:     d.creator?.[0] || "Aadam Jacobs",
      streamUrl:  f.streamUrl,
      streamType: "mp3",
      image:      d.coverUrl || "",
      year:       (d.date || "").slice(0, 4),
    },
  }));
}
// "▶ Play album" — if not yet in queue, queue all tracks at head and
// start playing the first; if already queued, jump to the first track
// in place (no duplicate insert).
async function _archiveInfoPlayPrimary(id) {
  const d = _archiveInfoLookup(id);
  if (!d?.audioFiles?.length) {
    if (typeof showToast === "function") showToast("This item has no playable audio", "error");
    return;
  }
  const items = _archiveItemsForAlbum(id, d);
  if (typeof queueAddAlbumOrPlay === "function") await queueAddAlbumOrPlay(items, { mode: "play" });
}
// "＋ Queue all tracks" — append the album to the queue tail without
// interrupting current playback. No-op (with toast) if already queued.
async function _archiveInfoQueueAll(id) {
  const d = _archiveInfoLookup(id);
  if (!d?.audioFiles?.length) return;
  const items = _archiveItemsForAlbum(id, d);
  if (typeof queueAddAlbumOrPlay === "function") await queueAddAlbumOrPlay(items, { mode: "append" });
}
function _archiveInfoPlayFile(id, idx) {
  const d = _archiveInfoLookup(id);
  const f = d?.audioFiles?.[idx];
  if (!f?.streamUrl) return;
  if (typeof _locPlay === "function") _locPlay({
    id: `${id}#${f.name}`,
    title: f.title || f.name,
    streamUrl: f.streamUrl,
    streamType: "mp3",
    contributors: d.creator?.length ? d.creator : [],
    image: d.coverUrl || "",
    year: (d.date || "").slice(0, 4),
  });
}
function _archiveInfoQueueFile(id, idx) {
  const d = _archiveInfoLookup(id);
  const f = d?.audioFiles?.[idx];
  if (!f?.streamUrl) return;
  // ＋ button → tail of queue (consistent with YT / LOC ＋). ▶ is
  // the "play now" affordance and goes through _archiveInfoPlayFile.
  if (typeof queueAddLoc === "function") queueAddLoc({
    id: `${id}#${f.name}`,
    title: f.title || f.name,
    streamUrl: f.streamUrl,
    streamType: "mp3",
    contributors: d.creator?.length ? d.creator : [],
    image: d.coverUrl || "",
    year: (d.date || "").slice(0, 4),
  }, { mode: "append" });
}
async function _archiveInfoToggleSave(btn, id) {
  // Reuse the same archiveToggleSave path, but the popup's button isn't
  // inside an .archive-row — emulate the row dataset by stamping data-id
  // on the button itself for the closest('.archive-row') lookup. Easiest:
  // create a synthetic row wrapper for the toggle call and revert text.
  if (!window._clerk?.user) {
    if (typeof showToast === "function") showToast("Sign in to save items", "error");
    return;
  }
  if (!_archiveSavedIds) _archiveSavedIds = new Set();
  const saving = !_archiveSavedIds.has(id);
  _archiveSavedIds[saving ? "add" : "delete"](id);
  btn.classList.toggle("is-saved", saving);
  btn.textContent = saving ? "★ Saved" : "☆ Save";
  const d = _archiveInfoLookup(id);
  try {
    if (saving) {
      const r = await apiFetch("/api/user/archive-saves", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          archiveId: id, title: d?.title || "", streamUrl: d?.primaryStreamUrl || "",
          data: d ? { title: d.title, date: d.date, description: d.description, itemUrl: d.itemUrl, streamUrl: d.primaryStreamUrl, image: d.coverUrl } : {},
        }),
      });
      if (!r.ok) throw new Error(`save failed (${r.status})`);
      showToast?.("Saved");
    } else {
      const r = await apiFetch("/api/user/archive-saves", {
        method: "DELETE", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ archiveId: id }),
      });
      if (!r.ok) throw new Error(`remove failed (${r.status})`);
      showToast?.("Removed from Saved");
      if (_archiveSavedItems) _archiveSavedItems = _archiveSavedItems.filter(i => i.identifier !== id);
      if (_archiveTab === "saved") _archiveRenderSavedRows();
    }
    // Sync any in-list ★ button for this id.
    document.querySelectorAll(`.archive-row[data-id="${CSS.escape(id)}"] .archive-save-btn`).forEach(el => {
      el.classList.toggle("is-saved", saving);
      el.textContent = saving ? "★" : "☆";
    });
  } catch (e) {
    _archiveSavedIds[saving ? "delete" : "add"](id);
    btn.classList.toggle("is-saved", !saving);
    btn.textContent = !saving ? "★ Saved" : "☆ Save";
    showToast?.(e?.message || "Action failed", "error");
  }
}

// Globals for inline onclicks in the rendered list
window.initArchiveView          = initArchiveView;
window.archivePlayItem          = archivePlayItem;
window.archiveQueueItem         = archiveQueueItem;
window.archivePlaySavedFromRow  = archivePlaySavedFromRow;
window.archiveQueueSavedFromRow = archiveQueueSavedFromRow;
window.archiveToggleSave        = archiveToggleSave;
window.archiveRefresh           = archiveRefresh;
window.archiveLoadMore          = archiveLoadMore;
window._archiveOnFilterInput      = _archiveOnFilterInput;
window._archiveOnSortChange       = _archiveOnSortChange;
window._archiveOnSavedFilterInput = _archiveOnSavedFilterInput;
window._archiveOnSavedSortChange  = _archiveOnSavedSortChange;
window._archiveSwitchTab          = _archiveSwitchTab;
window._archiveOpenInfoPopup      = _archiveOpenInfoPopup;
window._archiveCloseInfoPopup     = _archiveCloseInfoPopup;
window._archiveInfoPlayPrimary    = _archiveInfoPlayPrimary;
window._archiveInfoQueueAll       = _archiveInfoQueueAll;
window._archiveInfoPlayFile       = _archiveInfoPlayFile;
window._archiveInfoQueueFile      = _archiveInfoQueueFile;
window._archiveInfoToggleSave     = _archiveInfoToggleSave;
