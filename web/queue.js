// ── Cross-source play queue (LOC + YouTube) ─────────────────────────────
//
// Persists across logins via /api/user/play-queue. Items carry everything
// needed to play without a Discogs round-trip:
//
//   { source: "loc" | "yt",
//     externalId: string,        // LOC URL or YouTube videoId
//     data: { title, artist, image, ...engineSpecific } }
//
// LOC engine-specific data: { streamUrl, streamType ("mp3" | "hls"), duration }
// YT engine-specific data:  { durationSec, albumTitle, albumArtist }
//
// The bar's prev/next dispatchers (in modal.js) call into _queuePlayNext
// FIRST. Only when the queue is empty do they fall back to the per-engine
// internal next-track logic (LOC's multi-track queue, YT's _videoQueue).

let _queue = null;          // last fetched [{ position, source, externalId, data }, ...]
let _queueLoading = false;
let _queueDrawerEl = null;
let _sortableLoaded = null; // Promise once we begin lazy-loading Sortable.js
let _drawerSortable = null; // Sortable instance currently bound

// ── Fetch / cache ───────────────────────────────────────────────────
async function _queueLoad(force = false) {
  if (_queue && !force) return _queue;
  if (_queueLoading) return _queue ?? [];
  _queueLoading = true;
  try {
    const r = await apiFetch("/api/user/play-queue");
    if (!r.ok) { _queue = []; return _queue; }
    const j = await r.json();
    _queue = Array.isArray(j?.items) ? j.items : [];
    return _queue;
  } catch {
    _queue = _queue ?? [];
    return _queue;
  } finally {
    _queueLoading = false;
  }
}

// ── Add to queue ────────────────────────────────────────────────────
// Public entry-point used by the ➕ buttons site-wide. Returns true on
// success, false on failure (toast already shown).
async function queueAdd(items) {
  const arr = Array.isArray(items) ? items : [items];
  if (!arr.length) return false;
  if (!window._clerk?.user) {
    if (typeof showToast === "function") showToast("Sign in to use the queue", "error");
    return false;
  }
  try {
    const r = await apiFetch("/api/user/play-queue", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items: arr }),
    });
    if (!r.ok) {
      if (typeof showToast === "function") showToast("Could not add to queue", "error");
      return false;
    }
    _queue = null; // invalidate cache; next read refetches
    if (typeof showToast === "function") {
      showToast(arr.length === 1 ? "Added to queue" : `Added ${arr.length} to queue`);
    }
    // If the drawer is open, refresh it
    if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
    return true;
  } catch {
    if (typeof showToast === "function") showToast("Could not add to queue", "error");
    return false;
  }
}

// Convenience: build a queue item from a LOC card / info-popup item.
function queueAddLoc(locItem) {
  if (!locItem?.id) return false;
  if (!locItem.streamUrl) {
    if (typeof showToast === "function") showToast("Track has no playable stream", "error");
    return false;
  }
  return queueAdd([{
    source: "loc",
    externalId: locItem.id,
    data: {
      title: locItem.title || "Untitled",
      artist: Array.isArray(locItem.contributors) ? locItem.contributors.join(", ") : "",
      image: locItem.image || "",
      streamUrl: locItem.streamUrl,
      streamType: locItem.streamType || "",
      year: locItem.year || "",
    },
  }]);
}

// Convenience: build a queue item from a YouTube videoId + track meta.
// The YT player resolves the URL from the videoId.
function queueAddYt(videoId, meta) {
  if (!videoId) return false;
  return queueAdd([{
    source: "yt",
    externalId: String(videoId),
    data: {
      title: meta?.title || "",
      artist: meta?.artist || "",
      albumTitle: meta?.albumTitle || "",
      image: meta?.image || "",
      durationSec: meta?.durationSec || null,
    },
  }]);
}

// ── Queue navigation hooks ──────────────────────────────────────────
// Returns the next queue item and removes it from the local cache; the
// server still has it (we don't delete on play, only on user-remove or
// clear). Use `peek` if you need the next without consuming.
async function _queueShiftNext() {
  await _queueLoad();
  if (!_queue?.length) return null;
  // The "next" item is whatever has the lowest position (head of queue).
  return _queue[0] ?? null;
}

// Mark an item as played and remove from server queue. Called after
// the item starts playing successfully via _queuePlayNext.
async function _queueConsume(position) {
  try {
    await apiFetch("/api/user/play-queue", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ position }),
    });
  } catch { /* ignore — server-side will drift */ }
  if (_queue) _queue = _queue.filter(it => it.position !== position);
  if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
}

// Auto-advance entry point — called by both the LOC track-ended handler
// AND the YT onStateChange handler when the current track ends. If the
// queue has items, play the head; otherwise return false so the caller
// falls back to its own internal next-track logic.
async function _queuePlayNext() {
  const next = await _queueShiftNext();
  if (!next) return false;
  await _queueConsume(next.position);
  if (next.source === "loc") {
    // Synthesize a LOC-style item so _locPlay can take it as-is.
    const item = {
      id: next.externalId,
      title: next.data?.title || "",
      streamUrl: next.data?.streamUrl || "",
      streamType: next.data?.streamType || "",
      contributors: next.data?.artist ? [next.data.artist] : [],
      image: next.data?.image || "",
      year: next.data?.year || "",
    };
    if (typeof _locPlay === "function") _locPlay(item);
    return true;
  }
  if (next.source === "yt") {
    const url = `https://www.youtube.com/watch?v=${encodeURIComponent(next.externalId)}`;
    if (typeof openVideo === "function") openVideo(null, url);
    return true;
  }
  return false;
}

// ── Drawer UI ───────────────────────────────────────────────────────
function _ensureQueueDrawer() {
  if (_queueDrawerEl) return _queueDrawerEl;
  const wrap = document.createElement("div");
  wrap.id = "queue-drawer";
  wrap.className = "queue-drawer";
  wrap.innerHTML = `
    <div class="queue-drawer-head">
      <span class="queue-drawer-title">Up Next</span>
      <span class="queue-drawer-count" id="queue-drawer-count"></span>
      <button type="button" class="queue-drawer-clear" onclick="queueClear()" title="Clear queue">Clear</button>
      <button type="button" class="queue-drawer-close" onclick="queueToggleDrawer()" title="Close">×</button>
    </div>
    <div class="queue-drawer-list" id="queue-drawer-list"></div>
  `;
  document.body.appendChild(wrap);
  _queueDrawerEl = wrap;
  return wrap;
}

function queueToggleDrawer() {
  const wrap = _ensureQueueDrawer();
  const willOpen = !wrap.classList.contains("open");
  wrap.classList.toggle("open", willOpen);
  if (willOpen) {
    _queue = null; // refresh from server on open
    _renderQueueDrawer();
    _ensureSortable().catch(() => {}); // lazy-load drag library
  }
}

async function _renderQueueDrawer() {
  const wrap = _ensureQueueDrawer();
  const listEl = wrap.querySelector("#queue-drawer-list");
  const countEl = wrap.querySelector("#queue-drawer-count");
  if (!listEl) return;
  listEl.innerHTML = `<div class="queue-empty">Loading…</div>`;
  await _queueLoad(true);
  if (countEl) countEl.textContent = _queue?.length ? `${_queue.length} item${_queue.length === 1 ? "" : "s"}` : "";
  if (!_queue?.length) {
    listEl.innerHTML = `<div class="queue-empty">Queue empty. Click ➕ on tracks or LOC items to add them.</div>`;
    return;
  }
  listEl.innerHTML = _queue.map(it => {
    const safeTitle  = escHtml(it.data?.title || "Untitled");
    const safeArtist = escHtml(it.data?.artist || "");
    const sourceIcon = it.source === "loc" ? "♪" : "▶";
    const sourceTitle = it.source === "loc" ? "Library of Congress" : "YouTube";
    return `
      <div class="queue-row" data-position="${it.position}">
        <span class="queue-row-handle" title="Drag to reorder">⋮⋮</span>
        <span class="queue-row-source" title="${sourceTitle}">${sourceIcon}</span>
        <button class="queue-row-play" onclick="queueJumpTo(${it.position})" title="Play this now">
          <span class="queue-row-title">${safeTitle}</span>
          ${safeArtist ? `<span class="queue-row-artist">${safeArtist}</span>` : ""}
        </button>
        <button class="queue-row-remove" onclick="queueRemove(${it.position})" title="Remove from queue">×</button>
      </div>
    `;
  }).join("");
  _bindSortable();
}

// Lazy-load Sortable.js from CDN the first time the drawer opens.
function _ensureSortable() {
  if (window.Sortable) return Promise.resolve(window.Sortable);
  if (_sortableLoaded) return _sortableLoaded;
  _sortableLoaded = new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = "https://cdn.jsdelivr.net/npm/sortablejs@1.15.2/Sortable.min.js";
    s.async = true;
    s.onload = () => resolve(window.Sortable);
    s.onerror = () => { _sortableLoaded = null; reject(new Error("Sortable load failed")); };
    document.head.appendChild(s);
  });
  return _sortableLoaded;
}

async function _bindSortable() {
  const listEl = _queueDrawerEl?.querySelector("#queue-drawer-list");
  if (!listEl) return;
  if (_drawerSortable) { try { _drawerSortable.destroy(); } catch {} _drawerSortable = null; }
  try {
    const Sortable = await _ensureSortable();
    if (!Sortable) return;
    _drawerSortable = Sortable.create(listEl, {
      handle: ".queue-row-handle",
      animation: 140,
      onEnd: async () => {
        const positions = Array.from(listEl.querySelectorAll(".queue-row"))
          .map(el => parseInt(el.dataset.position, 10))
          .filter(n => Number.isFinite(n));
        try {
          await apiFetch("/api/user/play-queue/reorder", {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ positions }),
          });
          _queue = null;
          _renderQueueDrawer();
        } catch {
          if (typeof showToast === "function") showToast("Reorder failed", "error");
        }
      },
    });
  } catch { /* drag is optional — list still works without it */ }
}

// ── Public actions ──────────────────────────────────────────────────
async function queueRemove(position) {
  try {
    await apiFetch("/api/user/play-queue", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ position }),
    });
    _queue = null;
    _renderQueueDrawer();
  } catch {
    if (typeof showToast === "function") showToast("Could not remove from queue", "error");
  }
}

async function queueClear() {
  if (!confirm("Clear the entire queue?")) return;
  try {
    await apiFetch("/api/user/play-queue", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ clear: true }),
    });
    _queue = [];
    _renderQueueDrawer();
  } catch {
    if (typeof showToast === "function") showToast("Could not clear queue", "error");
  }
}

// Click on a queue row jumps directly to that item (consumes preceding
// items from the queue). Items before the chosen one are dropped server-
// side too.
async function queueJumpTo(position) {
  await _queueLoad(true);
  if (!_queue?.length) return;
  const target = _queue.find(it => it.position === position);
  if (!target) return;
  // Consume everything up to and including the chosen item
  const toDrop = _queue.filter(it => it.position <= position).map(it => it.position);
  for (const p of toDrop) {
    try {
      await apiFetch("/api/user/play-queue", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ position: p }),
      });
    } catch {}
  }
  _queue = null;
  if (target.source === "loc") {
    const item = {
      id: target.externalId,
      title: target.data?.title || "",
      streamUrl: target.data?.streamUrl || "",
      streamType: target.data?.streamType || "",
      contributors: target.data?.artist ? [target.data.artist] : [],
      image: target.data?.image || "",
    };
    if (typeof _locPlay === "function") _locPlay(item);
  } else if (target.source === "yt") {
    const url = `https://www.youtube.com/watch?v=${encodeURIComponent(target.externalId)}`;
    if (typeof openVideo === "function") openVideo(null, url);
  }
  if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
}

// ── Tiny "+ queue" icon helpers used by site-wide adders ────────────
// Returns inline HTML for a small ➕ button. `kind` is "loc" or "yt"
// for tooltip clarity; click handler is attached separately by callers.
function queueAddIconHtml(kind = "yt") {
  const tip = kind === "loc" ? "Add to queue" : "Add to queue (YouTube)";
  return `<a href="#" class="queue-add-icon" title="${tip}" onclick="event.preventDefault();event.stopPropagation();return false">＋</a>`;
}

// ── Globals ─────────────────────────────────────────────────────────
window.queueAdd            = queueAdd;
window.queueAddLoc         = queueAddLoc;
window.queueAddYt          = queueAddYt;
window.queueRemove         = queueRemove;
window.queueClear          = queueClear;
window.queueJumpTo         = queueJumpTo;
window.queueToggleDrawer   = queueToggleDrawer;
window._queuePlayNext      = _queuePlayNext;
