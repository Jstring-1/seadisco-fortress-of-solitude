// ── Archive.org curated collection (admin-only) ──────────────────────────
//
// Renders the Aadam Jacobs live-show collection as a flat list of cards.
// No search, no filters — just the curated list. ▶ Play loads into the
// unified mini-player via the LOC audio engine (archive.org serves
// direct mp3 URLs the same way LOC does); ➕ Queue inserts into the
// cross-source play queue.

let _archiveList = null;
let _archiveLoading = false;

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

function archiveRefresh() {
  _archiveList = null;
  initArchiveView(true);
}

function _renderArchiveList() {
  const listEl = document.getElementById("archive-list");
  if (!listEl) return;
  if (!_archiveList?.length) {
    listEl.innerHTML = `<div class="loc-empty">No items in this collection.</div>`;
    return;
  }
  const refreshBar = `<div class="archive-meta-bar">
    <span class="archive-meta-count">${_archiveList.length} item${_archiveList.length === 1 ? "" : "s"}</span>
    <button type="button" class="archive-refresh" onclick="archiveRefresh()" title="Re-fetch from archive.org">↻ Refresh</button>
  </div>`;
  listEl.innerHTML = refreshBar + _archiveList.map((it, i) => {
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
  }).join("");
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
window.initArchiveView   = initArchiveView;
window.archivePlayItem   = archivePlayItem;
window.archiveQueueItem  = archiveQueueItem;
window.archiveRefresh    = archiveRefresh;
