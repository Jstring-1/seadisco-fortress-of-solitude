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
  // Archive page is now open to all callers (the GET endpoint reads
  // from our DB cache only, no upstream call). Saving requires
  // sign-in: _archiveLoadSavedIds 401s for anons and falls back to
  // an empty Set, which just hides the ★ as already-saved (the click
  // handler will still try and surface a "sign in" toast).
  if (window._clerk?.user && _archiveSavedIds == null) _archiveLoadSavedIds();

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
  _archiveRenderRowsOnly({ append: false }); // re-render entire visible block
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
  return `
    <div class="archive-row" data-id="${safeId}"${dataAttrs}>
      <div class="archive-row-main" onclick="_archiveOpenInfoPopup('${safeId.replace(/'/g, "\\'")}')" style="cursor:pointer">
        <div class="archive-row-title">${safeTitle}</div>
        ${safeDate ? `<div class="archive-row-date">${safeDate}</div>` : ""}
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
        <input type="text" class="archive-filter" placeholder="Filter title, date, description…" value="${filterVal}" oninput="_archiveOnFilterInput(this)" />
        <select class="archive-sort" onchange="_archiveOnSortChange(this)">${sortOpts}</select>
        <span class="archive-meta-count" id="archive-count">${total} item${total === 1 ? "" : "s"}</span>
      </div>
      <div id="archive-rows"></div>
    </div>
    <div class="archive-panel archive-panel-saved" style="display:${showBrowse ? "none" : ""}">
      <div class="archive-meta-bar">
        <input type="text" class="archive-filter" placeholder="Filter saved…" value="${savedFilterVal}" oninput="_archiveOnSavedFilterInput(this)" />
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
    _archiveInfoCache.set(identifier, j);
    return j;
  } catch { return null; }
}

// Build cross-source queue items for every audio file in an archive
// show. Each track gets a stable per-file externalId so dedup-by-id
// in queueAddAlbumOrPlay catches "already in queue" correctly.
function _archiveItemsFromMeta(d) {
  if (!Array.isArray(d?.audioFiles)) return [];
  return d.audioFiles.map(f => ({
    source: "loc",
    externalId: `${d.identifier}#${f.name}`,
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
      if (!r.ok) {
        body.innerHTML = `<div class="loc-empty">Could not load item details (HTTP ${r.status}).</div>`;
        return;
      }
      data = await r.json();
      _archiveInfoCache.set(identifier, data);
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
    contributors: d.creator?.length ? d.creator : ["Aadam Jacobs"],
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
    contributors: d.creator?.length ? d.creator : ["Aadam Jacobs"],
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
