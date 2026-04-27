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

// ── Repeat state ────────────────────────────────────────────────────
// Three modes:
//   "off": queue plays through, then stops
//   "one": current track replays forever (engine-side seek+play; we
//          don't actually consume from the queue)
//   "all": when the queue drains, items consumed during this session
//          are re-queued (server-side append) so the cycle continues
// Persisted in localStorage so the user's preference survives reloads.
const _REPEAT_KEY = "sd_queue_repeat";
const _REPEAT_VALID = new Set(["off", "one", "all"]);
let _queueRepeat = (() => {
  try {
    const v = localStorage.getItem(_REPEAT_KEY);
    return _REPEAT_VALID.has(v) ? v : "off";
  } catch { return "off"; }
})();
// Items consumed during this session (used for repeat-all refill).
// Each entry is { source, externalId, data } — the same shape queueAdd
// expects, minus position which the server reassigns.
let _repeatHistory = [];
let _repeatRefillInFlight = false;

// Position of the item currently playing FROM THE QUEUE (or null if the
// active playback didn't start from the queue, e.g. user clicked ▶ on
// an album track directly). Used to:
//   1. Render a "▶ Now playing" badge on that row in the drawer
//   2. Defer consuming the item until the track actually ends
//      (was previously consumed at play-start, which removed it from
//       the drawer immediately — no visual feedback that "this is what's
//       on now")
let _queueCurrentPosition = null;

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
// Public entry-point used by the ➕ buttons site-wide. mode "next"
// (default) inserts at the head — single-track ➕ wants "play this
// next" semantics. mode "append" adds to the tail — used by
// queueAddAlbum so the tracks play in order after the existing queue.
async function queueAdd(items, opts) {
  const arr = Array.isArray(items) ? items : [items];
  if (!arr.length) return false;
  if (!window._clerk?.user) {
    if (typeof showToast === "function") showToast("Sign in to use the queue", "error");
    return false;
  }
  const mode = opts?.mode === "append" ? "append" : "next";
  try {
    const r = await apiFetch("/api/user/play-queue", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items: arr, mode }),
    });
    if (!r.ok) {
      if (typeof showToast === "function") showToast("Could not add to queue", "error");
      return false;
    }
    _queue = null; // invalidate cache; next read refetches
    // New items invalidate the user's earlier "I closed the idle bar"
    // suppression — they likely want the bar back so they can press ▶.
    _queueIdleClosed = false;
    if (typeof showToast === "function") {
      const verb = mode === "next" ? "Playing next" : "Queued";
      const count = arr.length === 1 ? "" : ` (${arr.length})`;
      showToast(`${verb}${count}`);
    }
    // Refetch in the background so the bar's prev/next state can
    // reflect the new queue immediately without waiting for the user
    // to open the drawer.
    _queueLoad(true).then(() => _refreshPlayerNavButtons()).catch(() => {});
    if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
    return true;
  } catch {
    if (typeof showToast === "function") showToast("Could not add to queue", "error");
    return false;
  }
}

// Convenience: build a queue item from a LOC card / info-popup item.
function queueAddLoc(locItem, opts) {
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
  }], opts);
}

// Convenience: build a queue item from a YouTube videoId + track meta.
// The YT player resolves the URL from the videoId.
function queueAddYt(videoId, meta, opts) {
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
  }], opts);
}

// Bulk-add every YT-matched track from an album popup tracklist. Reads
// the per-row data attributes that renderModal writes (data-yt-url,
// data-track, data-artist, data-album). Always uses mode "append" so
// the album plays in order after whatever's already queued.
async function queueAddAlbum(btn) {
  // Find the nearest tracklist container so we only grab THIS popup's
  // tracks, not any other tracklist that might be in the DOM.
  const scope = btn?.closest("#album-info, #version-info, .tracklist") || document;
  const rows  = scope.querySelectorAll(".queue-add-icon[data-yt-url]");
  if (!rows.length) {
    if (typeof showToast === "function") showToast("No playable tracks on this album", "error");
    return false;
  }
  const items = [];
  rows.forEach(el => {
    const url = el.dataset.ytUrl || "";
    const id  = (typeof extractYouTubeId === "function") ? extractYouTubeId(url) : "";
    if (!id) return;
    items.push({
      source: "yt",
      externalId: id,
      data: {
        title:      el.dataset.track  || "",
        artist:     el.dataset.artist || "",
        albumTitle: el.dataset.album  || "",
      },
    });
  });
  if (!items.length) {
    if (typeof showToast === "function") showToast("No playable tracks on this album", "error");
    return false;
  }
  // Visually mark each row as queued
  rows.forEach(el => el.classList.add("queued"));
  return queueAdd(items, { mode: "append" });
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

// Mark an item as played and remove from server queue. Called when a
// previously-playing item ends (via _queuePlayNext) or when the user
// removes/jumps. Updates local cache synchronously so the very next
// _queueShiftNext sees the new head, then fires the DELETE in the
// background — server drift is harmless (worst case: a re-load shows
// the consumed row briefly before the next consume cleans it).
async function _queueConsume(position) {
  if (_queue) _queue = _queue.filter(it => it.position !== position);
  _refreshPlayerNavButtons();
  if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
  try {
    await apiFetch("/api/user/play-queue", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ position }),
    });
  } catch { /* ignore — server-side will drift */ }
}

// Auto-advance entry point — called by both the LOC track-ended handler
// AND the YT onStateChange handler when the current track ends. With
// the new "queue is a playlist" model, items aren't consumed on play;
// we just advance _queueCurrentPosition through the queue. Removal
// only happens via the explicit × button (queueRemove). Repeat-all
// wraps to the first item; repeat-one is handled by the engine's
// own track-ended handler (seek+play, never reaches here).
async function _queuePlayNext() {
  await _queueLoad();
  if (!_queue?.length) {
    _queueCurrentPosition = null;
    _refreshPlayerNavButtons();
    if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
    return false;
  }
  const currentIdx = _queueCurrentPosition != null
    ? _queue.findIndex(it => it.position === _queueCurrentPosition)
    : -1;
  let next = currentIdx >= 0 && currentIdx + 1 < _queue.length
    ? _queue[currentIdx + 1]
    : (currentIdx < 0 ? _queue[0] : null);
  // Repeat-all: wrap to the first item.
  if (!next && _queueRepeat === "all" && _queue.length) {
    next = _queue[0];
  }
  if (!next) {
    // End of queue, no repeat: clear playing mark but keep items.
    _queueCurrentPosition = null;
    _refreshPlayerNavButtons();
    if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
    return false;
  }
  return _queuePlayItem(next);
}

// Internal: play a queue entry by dispatching to its engine and then
// re-applying the now-playing mark. Used by _queuePlayNext, queueJumpTo,
// and queuePlayHead. Sets _queueDispatching so the external-play hook
// fired by _locPlay/openVideo doesn't re-insert the same item.
let _queueDispatching = false;
async function _queuePlayItem(entry) {
  const playItem = entry.source === "loc"
    ? {
        id:           entry.externalId,
        title:        entry.data?.title || "",
        streamUrl:    entry.data?.streamUrl || "",
        streamType:   entry.data?.streamType || "",
        contributors: entry.data?.artist ? [entry.data.artist] : [],
        image:        entry.data?.image || "",
        year:         entry.data?.year || "",
      }
    : null;
  _queueDispatching = true;
  try {
    if (entry.source === "loc") {
      if (typeof _locPlay === "function") _locPlay(playItem);
    } else if (entry.source === "yt") {
      const url = `https://www.youtube.com/watch?v=${encodeURIComponent(entry.externalId)}`;
      if (typeof openVideo === "function") openVideo(null, url);
    } else {
      return false;
    }
  } finally {
    // Engine dispatchers are sync (they kick off async work but return
    // immediately). Clear the flag on the next tick so any synchronous
    // _queueOnExternalPlay hook is suppressed but later track-end-driven
    // calls aren't.
    setTimeout(() => { _queueDispatching = false; }, 0);
  }
  _queueCurrentPosition = entry.position;
  if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
  _refreshPlayerNavButtons();
  return true;
}

// Called from _locPlay and openVideo at the start of every play call.
// If the play is "external" (i.e. the user clicked ▶ on a card outside
// the queue) AND the queue has items, we want the new track to push
// to the head of the queue and become the now-playing item — that
// way the queue continues from there when the new track ends instead
// of being abandoned. Returns true if the caller should suppress its
// normal play (we'll handle it via queue insertion); false otherwise.
//
// Without item info we can only clear the now-playing mark — that's
// the fall-through path used when the engine's own internal queue is
// driving (e.g. _videoQueueIndex auto-advance inside the YT modal,
// where we don't want to re-queue the next album track).
function _queueOnExternalPlay(itemPayload) {
  // Re-entry guard: when _queuePlayItem dispatches a play call, the
  // engine's own _queueOnExternalPlay hook fires inside the same tick.
  // Skip — _queuePlayItem is already managing currentPosition + queue
  // state, and we don't want to re-insert the item we just picked.
  if (_queueDispatching) return false;
  // No item payload → just clear the mark (legacy callers)
  if (!itemPayload) {
    if (_queueCurrentPosition != null) {
      _queueCurrentPosition = null;
      if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
    }
    return false;
  }
  // Item payload + non-empty queue → insert at head, mark as playing,
  // let the calling _locPlay/openVideo continue with playback (the
  // server-side mode:"next" insert is async but the local engine call
  // doesn't depend on it; we sync up _queueCurrentPosition once the
  // insert lands).
  _queueLoad().then(async () => {
    if (!_queue?.length) {
      _queueCurrentPosition = null;
      if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
      return;
    }
    // Insert at head (mode "next" shifts existing positions down).
    try {
      const r = await apiFetch("/api/user/play-queue", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items: [itemPayload], mode: "next" }),
      });
      if (!r.ok) { _queueCurrentPosition = null; return; }
    } catch { _queueCurrentPosition = null; return; }
    _queue = null;
    await _queueLoad(true);
    // The newly-inserted item is the new head (lowest position).
    if (_queue?.length) _queueCurrentPosition = _queue[0].position;
    if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
    _refreshPlayerNavButtons();
  }).catch(() => {});
  return false; // don't suppress; engine still plays directly
}

// ── Repeat toggle ────────────────────────────────────────────────────
// Cycles off → all → one → off. Returns the new mode so callers can
// update their UI without a separate read.
function _queueCycleRepeat() {
  const order = ["off", "all", "one"];
  const idx = order.indexOf(_queueRepeat);
  _queueRepeat = order[(idx + 1) % order.length];
  try { localStorage.setItem(_REPEAT_KEY, _queueRepeat); } catch {}
  _renderRepeatBtn();
  if (typeof showToast === "function") {
    const msg = _queueRepeat === "off" ? "Repeat off"
              : _queueRepeat === "one" ? "Repeat one"
              :                          "Repeat all";
    showToast(msg);
  }
  return _queueRepeat;
}

function _queueGetRepeat() { return _queueRepeat; }

// Update the drawer's repeat button — distinct icon AND distinct color
// per state. Uses monochrome glyphs (NOT emoji like 🔁/🔂) because CSS
// `color` doesn't affect emoji rendering on most platforms — that's why
// the previous off/all states both looked blue. Safe to call when the
// drawer hasn't been built yet (no-ops).
function _renderRepeatBtn() {
  const btn = _queueDrawerEl?.querySelector(".queue-drawer-repeat");
  if (!btn) return;
  // Three distinct shapes:
  //   off → "→" (linear arrow, doesn't suggest looping)
  //   all → "↻" (full loop)
  //   one → "↻" + a small "1" badge in superscript
  btn.innerHTML = _queueRepeat === "off" ? "→"
                : _queueRepeat === "all" ? "↻"
                :                          '↻<span class="queue-repeat-1">1</span>';
  btn.classList.remove("repeat-off", "repeat-all", "repeat-one");
  btn.classList.add(`repeat-${_queueRepeat}`);
  btn.title = _queueRepeat === "off" ? "Repeat: off (click to cycle)"
            : _queueRepeat === "all" ? "Repeat: all (click to cycle)"
            :                          "Repeat: one (click to cycle)";
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
      <button type="button" class="queue-drawer-repeat repeat-off" onclick="_queueCycleRepeat()" title="Repeat: off (click to cycle)">→</button>
      <button type="button" class="queue-drawer-clear" onclick="queueClear()" title="Clear queue">Clear</button>
      <button type="button" class="queue-drawer-close" onclick="queueToggleDrawer()" title="Close">×</button>
    </div>
    <div class="queue-drawer-list" id="queue-drawer-list"></div>
  `;
  document.body.appendChild(wrap);
  _queueDrawerEl = wrap;
  _renderRepeatBtn();
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
    listEl.innerHTML = `<div class="queue-empty">Queue empty. Click ➕ on tracks, LOC items, or archive items to add them.</div>`;
    return;
  }
  listEl.innerHTML = _queue.map(it => {
    const safeTitle  = escHtml(it.data?.title || "Untitled");
    const safeArtist = escHtml(it.data?.artist || "");
    const isPlaying  = it.position === _queueCurrentPosition;
    // Source badge (small colored dot in the thumb corner) tells LOC
    // vs YT apart at a glance; the row's `now playing` mark is a
    // separate visual on top.
    const sourceTitle = it.source === "loc" ? "Library of Congress" : "YouTube";
    // Pick the thumb URL: archive/LOC items snapshot data.image at
    // queue-add; YT items fall back to YouTube's auto-thumbnail
    // (i.ytimg.com is CDN-hosted and respects no-referrer).
    let thumbUrl = it.data?.image || "";
    if (!thumbUrl && it.source === "yt") {
      thumbUrl = `https://i.ytimg.com/vi/${encodeURIComponent(it.externalId)}/mqdefault.jpg`;
    }
    const thumbHtml = thumbUrl
      ? `<img class="queue-row-thumb" src="${escHtml(thumbUrl)}" loading="lazy" alt="" onerror="this.classList.add('thumb-broken')">`
      : `<span class="queue-row-thumb queue-row-thumb-empty">${it.source === "loc" ? "♪" : "▶"}</span>`;
    return `
      <div class="queue-row${isPlaying ? " is-playing" : ""}" data-position="${it.position}">
        <span class="queue-row-handle" title="Drag to reorder">⋮⋮</span>
        <span class="queue-row-thumb-wrap" title="${sourceTitle}">
          ${thumbHtml}
          <span class="queue-row-source-badge queue-row-source-${it.source}"></span>
          ${isPlaying ? `<span class="queue-row-eq" aria-hidden="true"><i></i><i></i><i></i></span>` : ""}
        </span>
        <button class="queue-row-play" onclick="queueJumpTo(${it.position})" title="${isPlaying ? "Currently playing" : "Play this now"}">
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
  const wasPlaying = _queueCurrentPosition === position;
  try {
    await apiFetch("/api/user/play-queue", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ position }),
    });
    if (wasPlaying) _queueCurrentPosition = null;
    _queue = null;
    await _queueLoad(true);
    _refreshPlayerNavButtons();
    _renderQueueDrawer();
    // If the removed row was the now-playing one, audio keeps playing
    // (the engine still owns its stream); the queue just no longer
    // claims this position. The next track-end will pick the new
    // first-after-current via _queuePlayNext.
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
    _queueCurrentPosition = null;
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
  if (_queueCurrentPosition === position) return; // already playing
  // No consumption — items only leave the queue via × (queueRemove)
  // or Clear (queueClear). Just play the target and update the mark.
  await _queuePlayItem(target);
}

// Public "play queue" action — used by the mini-player ▶ when nothing
// is loaded yet but items are queued, and by the drawer's ▶ Play head
// button. Plays the queue head if no current position; otherwise resumes
// the currently-marked position. Idempotent.
async function queuePlayHead() {
  await _queueLoad(true);
  if (!_queue?.length) {
    if (typeof showToast === "function") showToast("Queue is empty", "error");
    return false;
  }
  const target = _queueCurrentPosition != null
    ? (_queue.find(it => it.position === _queueCurrentPosition) ?? _queue[0])
    : _queue[0];
  return _queuePlayItem(target);
}

// Public "play queue" action — used by the mini-player ▶ when nothing
// is loaded yet but items are queued, and by the drawer's ▶ Play head
// button. Picks the head and starts playing it; reuses _queuePlayNext
// but ensures we don't accidentally consume something before any
// ── Tiny "+ queue" icon helpers used by site-wide adders ────────────
// Returns inline HTML for a small ➕ button. `kind` is "loc" or "yt"
// for tooltip clarity; click handler is attached separately by callers.
function queueAddIconHtml(kind = "yt") {
  const tip = kind === "loc" ? "Add to queue" : "Add to queue (YouTube)";
  return `<a href="#" class="queue-add-icon" title="${tip}" onclick="event.preventDefault();event.stopPropagation();return false">＋</a>`;
}

// Sync "is there a next item" check used by the player nav buttons
// across both engines. Reads the locally-cached queue (not the server)
// so it returns instantly. Initial reads return false until the cache
// is hydrated; queueAdd/Remove/Clear all invalidate the cache and
// re-fire button state via _refreshPlayerNavButtons below.
// Returns true if there's an item AFTER the currently-playing position
// in the queue (the Next button moves through the playlist; it
// doesn't navigate back to items before the current position).
// Repeat-all makes Next always available so users can wrap manually.
function _queueHasNext() {
  if (!Array.isArray(_queue) || _queue.length === 0) return false;
  if (_queueCurrentPosition == null) return true;
  if (_queueRepeat === "all") return true;
  const idx = _queue.findIndex(it => it.position === _queueCurrentPosition);
  return idx >= 0 && idx + 1 < _queue.length;
}

// True if the queue has at least one item that ISN'T currently
// playing — used by the mini-player to decide whether to surface
// itself as "queue-ready" when no audio is loaded yet.
function _queueHasPlayable() {
  return Array.isArray(_queue) && _queue.length > 0;
}

// Whenever the queue mutates we invalidate the cache, then refresh the
// player's prev/next buttons so the UI reflects the new state without
// requiring the user to reopen the drawer or wait for a track-end.
function _refreshPlayerNavButtons() {
  if (typeof _locUpdateQueueButtons === "function") {
    try { _locUpdateQueueButtons(); } catch {}
  }
  if (typeof updateVideoNavButtons === "function") {
    try { updateVideoNavButtons(); } catch {}
  }
  _queueRefreshIdleBar();
}

// Idle-queue bar surface: show the persistent mini-player even when no
// audio is loaded, so long as the queue has items. The bar's ▶ button
// starts the queue head — that's how users discover playback when they
// queue items without ever pressing play first. Hidden when an engine
// becomes active (real playback takes over the bar) and when the queue
// drains. Honors the user's explicit close — once they hit × on the
// bar in idle state, we don't re-open it until the queue mutates.
let _queueIdleClosed = false;
function _queueRefreshIdleBar() {
  const bar = document.getElementById("mini-player");
  if (!bar) return;
  const engineActive = !!window._currentEngine;
  const hasItems = Array.isArray(_queue) && _queue.length > 0;
  if (engineActive) {
    bar.classList.remove("idle-queue");
    return;
  }
  if (!hasItems || _queueIdleClosed) {
    bar.classList.remove("idle-queue");
    if (!engineActive) {
      bar.classList.remove("open", "expanded");
      document.body.classList.remove("player-open", "expanded-mini");
    }
    return;
  }
  // Surface the bar in idle-queue mode. Title shows the head item +
  // queue count; clicking ▶ runs queuePlayHead().
  bar.classList.add("open", "idle-queue");
  document.body.classList.add("player-open");
  const head = _queue[0];
  const titleEl = document.getElementById("mini-player-title");
  if (titleEl) {
    const t = escHtml(head.data?.title || "Queued");
    const a = head.data?.artist ? ` · ${escHtml(head.data.artist)}` : "";
    const more = _queue.length > 1 ? ` <span class="mini-idle-count">(+${_queue.length - 1} queued)</span>` : "";
    titleEl.innerHTML = `${t}${a}${more}`;
  }
  const sourceEl = document.getElementById("mini-player-source-icon");
  if (sourceEl) sourceEl.textContent = head.source === "loc" ? "♪" : "▶";
  const statusEl = document.getElementById("mini-player-status");
  if (statusEl) statusEl.textContent = "ready · click ▶";
}
// User closing the bar in idle-queue state suppresses auto-show until
// they queue something new. Wired from the existing × button via
// playerClose's "if engine null and idle, set this flag".
function _queueMarkIdleClosed() { _queueIdleClosed = true; }
function _queueClearIdleClosed() { _queueIdleClosed = false; }

// ── Globals ─────────────────────────────────────────────────────────
window._queueHasNext = _queueHasNext;
window._queueHasPlayable = _queueHasPlayable;
window._queueOnExternalPlay = _queueOnExternalPlay;
window._queueGetCurrentPosition = () => _queueCurrentPosition;
window._queueGetRepeat = _queueGetRepeat;
window._queueCycleRepeat = _queueCycleRepeat;
window.queuePlayHead = queuePlayHead;
window._queueRefreshIdleBar = _queueRefreshIdleBar;
window._queueMarkIdleClosed = _queueMarkIdleClosed;
window._queueClearIdleClosed = _queueClearIdleClosed;
window._refreshPlayerNavButtons = _refreshPlayerNavButtons;
// On script load: if the user has queued items from a previous
// session, surface the idle bar once the queue cache hydrates. We
// kick the load asynchronously so the page renders first, then the
// _refreshPlayerNavButtons in the load callback brings up the bar.
document.addEventListener("DOMContentLoaded", () => {
  setTimeout(() => {
    _queueLoad(true).then(() => _refreshPlayerNavButtons()).catch(() => {});
  }, 600);
});
window.queueAdd            = queueAdd;
window.queueAddLoc         = queueAddLoc;
window.queueAddYt          = queueAddYt;
window.queueAddAlbum       = queueAddAlbum;
window.queueRemove         = queueRemove;
window.queueClear          = queueClear;
window.queueJumpTo         = queueJumpTo;
window.queueToggleDrawer   = queueToggleDrawer;
window._queuePlayNext      = _queuePlayNext;
