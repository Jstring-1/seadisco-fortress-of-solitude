// ── Archive.org curated collection (admin-only) ──────────────────────────
//
// Renders the Aadam Jacobs live-show collection as a flat list of cards.
// No search, no filters — just the curated list. ▶ Play loads into the
// unified mini-player via the LOC audio engine (archive.org serves
// direct mp3 URLs the same way LOC does); ➕ Queue inserts into the
// cross-source play queue.

let _archiveList = null;
let _archiveLoading = false;
let _archiveFilter = "";
let _archiveSort = "title-asc"; // title-asc | title-desc | date-desc | date-asc
let _archiveShown = 0;          // how many filtered+sorted rows currently rendered
const _ARCHIVE_PAGE = 48;       // matches the Recent strip page size

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

function _archiveRowHtml(it, i) {
  const safeTitle = escHtml(it.title || it.identifier);
  const safeDate  = escHtml(it.date || "");
  const safeDesc  = escHtml(String(it.description || "").slice(0, 280));
  const safeId    = escHtml(it.identifier);
  const playable  = !!it.streamUrl;
  const playBtn = playable
    ? `<button type="button" class="archive-btn archive-btn-play" onclick="archivePlayItem(${i})" title="Play in the bar">▶ Play</button>`
    : `<button type="button" class="archive-btn archive-btn-play is-disabled" disabled>▶ No stream</button>`;
  const queueBtn = playable
    ? `<button type="button" class="archive-btn archive-btn-queue" onclick="archiveQueueItem(${i})" title="Add to play queue">＋ Queue</button>`
    : "";
  const linkBtn = `<a class="archive-btn archive-btn-link" href="${escHtml(it.itemUrl)}" target="_blank" rel="noopener">Open on archive.org ↗</a>`;
  return `
    <div class="archive-row" data-id="${safeId}">
      <div class="archive-row-main">
        <div class="archive-row-title">${safeTitle}</div>
        ${safeDate ? `<div class="archive-row-date">${safeDate}</div>` : ""}
        ${safeDesc ? `<div class="archive-row-desc">${safeDesc}${it.description && it.description.length > 280 ? "…" : ""}</div>` : ""}
      </div>
      <div class="archive-row-actions">${playBtn}${queueBtn}${linkBtn}</div>
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
  // Refresh button removed — the cache is auto-updated weekly server-
  // side and admin can trigger a manual refresh from /admin if needed.
  const controls = `<div class="archive-meta-bar">
    <input type="search" class="archive-filter" placeholder="Filter title, date, description…" value="${filterVal}" oninput="_archiveOnFilterInput(this)" />
    <select class="archive-sort" onchange="_archiveOnSortChange(this)">${sortOpts}</select>
    <span class="archive-meta-count" id="archive-count">${total} item${total === 1 ? "" : "s"}</span>
  </div>
  <div id="archive-rows"></div>`;
  listEl.innerHTML = controls;
  _archiveRenderRowsOnly();
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

// Globals for inline onclicks in the rendered list
window.initArchiveView      = initArchiveView;
window.archivePlayItem      = archivePlayItem;
window.archiveQueueItem     = archiveQueueItem;
window.archiveRefresh       = archiveRefresh;
window.archiveLoadMore      = archiveLoadMore;
window._archiveOnFilterInput = _archiveOnFilterInput;
window._archiveOnSortChange  = _archiveOnSortChange;
