// ── Archive.org curated collection (admin-only) ──────────────────────────
//
// Renders the Aadam Jacobs live-show collection as a flat list of cards.
// Two tabs:
//   Browse — the full collection, filterable + sortable + paginated
//   Saved  — the user's bookmarked items (★ toggle on each row)
// ▶ Play loads into the unified mini-player via the LOC audio engine
// (archive.org serves direct mp3 URLs the same way LOC does); ➕ Queue
// inserts into the cross-source play queue; ★ toggles save state on the
// server (admin-only endpoint).

let _archiveList = null;
let _archiveLoading = false;
let _archiveFilter = "";
let _archiveSort = "title-asc"; // title-asc | title-desc | date-desc | date-asc
let _archiveShown = 0;          // how many filtered+sorted rows currently rendered
const _ARCHIVE_PAGE = 48;       // matches the Recent strip page size

// ── Tab + saves state ────────────────────────────────────────────────
let _archiveTab = "browse";        // "browse" | "saved"
let _archiveSavedIds = null;       // Set<string> of saved archive identifiers
let _archiveSavedItems = null;     // Array of saved items (loaded on Saved tab open)
let _archiveSavedLoading = false;
let _archiveSavedFilter = "";
let _archiveSavedSort = "recent";  // recent | title | date-asc | date-desc

async function initArchiveView(forceRefresh = false) {
  const root = document.getElementById("archive-view");
  if (!root) return;
  // Admin gate — server enforces; this is the UX guard.
  if (!window._isAdmin) {
    root.innerHTML = `<div class="loc-empty" style="padding:3rem 1rem">
      <div style="font-size:1rem;color:var(--text);margin-bottom:0.5rem">Archive page is admin-only.</div>
    </div>`;
    return;
  }
  // Load saved IDs in the background once per session so the ★ badge on
  // every row reflects current state without a per-row API round-trip.
  if (_archiveSavedIds == null) _archiveLoadSavedIds();

  // Honor ?tab=saved in the URL so saved-list links are shareable.
  try {
    const qs = new URLSearchParams(location.search);
    const t = qs.get("tab");
    if (t === "saved") _archiveTab = "saved";
  } catch {}

  // If we already have items in memory and nothing's forcing a refresh,
  // re-render and return immediately. The server cache is effectively
  // permanent (5-year TTL) so once loaded the data is stable.
  if (_archiveList?.length && !forceRefresh) {
    _renderArchiveList();
    return;
  }
  if (_archiveLoading) return;
  _archiveLoading = true;
  const listEl = document.getElementById("archive-list");
  if (listEl) listEl.innerHTML = `<div class="loc-empty">Loading collection — first load may take a few seconds.</div>`;
  try {
    const url = forceRefresh ? "/api/archive/aadamjacobs?nocache=1" : "/api/archive/aadamjacobs";
    const r = await apiFetch(url);
    if (!r.ok) {
      if (listEl) listEl.innerHTML = `<div class="loc-empty">Could not load archive collection (HTTP ${r.status}).</div>`;
      return;
    }
    const j = await r.json();
    _archiveList = Array.isArray(j?.items) ? j.items : [];
    _renderArchiveList();
  } catch (err) {
    if (listEl) listEl.innerHTML = `<div class="loc-empty">Could not load archive collection.</div>`;
  } finally {
    _archiveLoading = false;
  }
}

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
async function archiveToggleSave(btn) {
  const row = btn?.closest(".archive-row");
  const id = row?.dataset.id;
  if (!id) return;
  if (!_archiveSavedIds) _archiveSavedIds = new Set();
  const saving = !_archiveSavedIds.has(id);
  // Optimistic toggle
  _archiveSavedIds[saving ? "add" : "delete"](id);
  btn.classList.toggle("is-saved", saving);
  btn.textContent = saving ? "★" : "☆";
  btn.title = saving ? "Remove from Saved" : "Save to your list";
  // Find the source item — could be in main list (browse) or saved list.
  // We try both; the row data-id is enough to look up by identifier.
  const src = (_archiveList || []).find(x => x.identifier === id)
           || (_archiveSavedItems || []).find(x => x.identifier === id);
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
  _archiveRenderRowsOnly({ append: false }); // re-render entire visible block
}

// Build the row HTML. Used for both Browse rows (idx is the position in
// _archiveList — wired into archivePlayItem/archiveQueueItem) and Saved
// rows (idx is -1; play/queue read the row's data-* attributes
// directly via archivePlaySaved / archiveQueueSaved).
function _archiveRowHtml(it, i, opts = {}) {
  const safeTitle = escHtml(it.title || it.identifier);
  const safeDate  = escHtml(it.date || "");
  const safeDesc  = escHtml(String(it.description || "").slice(0, 280));
  const safeId    = escHtml(it.identifier);
  const playable  = !!it.streamUrl;
  const isSaved   = !!_archiveSavedIds?.has(it.identifier);
  const saveBtn = `<button type="button" class="archive-btn archive-save-btn${isSaved ? " is-saved" : ""}" onclick="archiveToggleSave(this)" title="${isSaved ? "Remove from Saved" : "Save to your list"}">${isSaved ? "★" : "☆"}</button>`;
  // Saved-view rows get distinct play/queue handlers that source from
  // the row's data-* (since `i` is meaningless without _archiveList).
  const playHandler  = opts.savedView ? `archivePlaySavedFromRow(this)`  : `archivePlayItem(${i})`;
  const queueHandler = opts.savedView ? `archiveQueueSavedFromRow(this)` : `archiveQueueItem(${i})`;
  const playBtn = playable
    ? `<button type="button" class="archive-btn archive-btn-play" onclick="${playHandler}" title="Play in the bar">▶ Play</button>`
    : `<button type="button" class="archive-btn archive-btn-play is-disabled" disabled>▶ No stream</button>`;
  const queueBtn = playable
    ? `<button type="button" class="archive-btn archive-btn-queue" onclick="${queueHandler}" title="Add to play queue">＋ Queue</button>`
    : "";
  const linkBtn = `<a class="archive-btn archive-btn-link" href="${escHtml(it.itemUrl || ("https://archive.org/details/" + encodeURIComponent(it.identifier)))}" target="_blank" rel="noopener">Open on archive.org ↗</a>`;
  // Stash the data needed for play/queue from the saved view onto the
  // row itself so the saved-row handlers don't need a separate cache.
  const dataAttrs = opts.savedView
    ? ` data-stream="${escHtml(it.streamUrl || "")}" data-title="${escHtml(it.title || "")}" data-date="${escHtml(it.date || "")}"`
    : "";
  return `
    <div class="archive-row" data-id="${safeId}"${dataAttrs}>
      <div class="archive-row-main">
        <div class="archive-row-title">${safeTitle}</div>
        ${safeDate ? `<div class="archive-row-date">${safeDate}</div>` : ""}
        ${safeDesc ? `<div class="archive-row-desc">${safeDesc}${it.description && it.description.length > 280 ? "…" : ""}</div>` : ""}
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
  if (!_archiveList?.length) {
    listEl.innerHTML = `<div class="loc-empty">No items in this collection.</div>`;
    return;
  }
  const total = _archiveList.length;
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
  // Tabs + both panels (the inactive one is hidden with display:none).
  // Using inline display rather than separate render-on-tab functions
  // keeps the controls' input state during tab switches.
  const showBrowse = _archiveTab === "browse";
  listEl.innerHTML = `
    <div class="archive-tabs">
      <button type="button" class="archive-tab archive-tab-browse${showBrowse ? " active" : ""}" onclick="_archiveSwitchTab('browse')">Browse</button>
      <button type="button" class="archive-tab archive-tab-saved${!showBrowse ? " active" : ""}" onclick="_archiveSwitchTab('saved')">Saved</button>
    </div>
    <div class="archive-panel archive-panel-browse" style="display:${showBrowse ? "" : "none"}">
      <div class="archive-meta-bar">
        <input type="search" class="archive-filter" placeholder="Filter title, date, description…" value="${filterVal}" oninput="_archiveOnFilterInput(this)" />
        <select class="archive-sort" onchange="_archiveOnSortChange(this)">${sortOpts}</select>
        <span class="archive-meta-count" id="archive-count">${total} item${total === 1 ? "" : "s"}</span>
      </div>
      <div id="archive-rows"></div>
    </div>
    <div class="archive-panel archive-panel-saved" style="display:${showBrowse ? "none" : ""}">
      <div class="archive-meta-bar">
        <input type="search" class="archive-filter" placeholder="Filter saved…" value="${savedFilterVal}" oninput="_archiveOnSavedFilterInput(this)" />
        <select class="archive-sort" onchange="_archiveOnSavedSortChange(this)">${savedSortOpts}</select>
        <span class="archive-meta-count"><span id="archive-saved-count">0</span> saved</span>
      </div>
      <div id="archive-saved-rows"><div class="loc-empty">Loading…</div></div>
    </div>
  `;
  if (showBrowse) {
    _archiveRenderRowsOnly();
  } else {
    _archiveLoadSaved();
  }
}

function _archiveSwitchTab(tab, { pushUrl = true } = {}) {
  _archiveTab = tab === "saved" ? "saved" : "browse";
  // Toggle tab button + panel visibility without re-rendering everything
  // (preserves filter/sort state in both panels).
  document.querySelectorAll(".archive-tab").forEach(b => b.classList.remove("active"));
  document.querySelector(`.archive-tab-${_archiveTab}`)?.classList.add("active");
  const browse = document.querySelector(".archive-panel-browse");
  const saved  = document.querySelector(".archive-panel-saved");
  if (browse) browse.style.display = _archiveTab === "browse" ? "" : "none";
  if (saved)  saved.style.display  = _archiveTab === "saved"  ? "" : "none";
  if (_archiveTab === "saved" && _archiveSavedItems == null) _archiveLoadSaved();
  // Reflect in URL so /?v=archive&tab=saved is shareable.
  if (pushUrl && typeof history?.pushState === "function") {
    const qs = new URLSearchParams(location.search);
    qs.set("v", "archive");
    if (_archiveTab === "saved") qs.set("tab", "saved"); else qs.delete("tab");
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
  return {
    id:           it.identifier,                       // unique key; not a URL but unique enough
    title:        it.title || it.identifier,
    streamUrl:    it.streamUrl,
    streamType:   "mp3",
    contributors: ["Aadam Jacobs"],
    image:        "",
    year:         (it.date || "").slice(0, 4),
  };
}

function archivePlayItem(idx) {
  const it = _archiveList?.[idx];
  if (!it?.streamUrl) return;
  if (typeof _locPlay === "function") _locPlay(_archiveItemToLoc(it));
}

function archiveQueueItem(idx) {
  const it = _archiveList?.[idx];
  if (!it?.streamUrl) return;
  if (typeof queueAddLoc === "function") queueAddLoc(_archiveItemToLoc(it));
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

function archivePlaySavedFromRow(btn) {
  const row = btn?.closest(".archive-row");
  const it = _archiveItemFromRow(row);
  if (!it?.streamUrl) return;
  if (typeof _locPlay === "function") _locPlay(_archiveItemToLoc(it));
}

function archiveQueueSavedFromRow(btn) {
  const row = btn?.closest(".archive-row");
  const it = _archiveItemFromRow(row);
  if (!it?.streamUrl) return;
  if (typeof queueAddLoc === "function") queueAddLoc(_archiveItemToLoc(it));
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
