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
// Snapshot of the currently-playing item's externalId. Position-based
// matching breaks during the brief window when an optimistic insert
// hasn't been reconciled with a concurrent _queueLoad (URL-based
// playback at page boot is the common trigger). Falling back to
// externalId match keeps the indicator stable across that window.
let _queuePlayingExternalId = null;

// ── Fetch / cache ───────────────────────────────────────────────────
async function _queueLoad(force = false) {
  if (_queue && !force) return _queue;
  if (_queueLoading) return _queue ?? [];
  // Signed-out users have no server queue. Don't fire a request that
  // would 401, and don't clobber a local-only "queue of one" that
  // _queueOnExternalPlay may have populated for the playing track.
  if (!window._clerk?.user) {
    _queue = Array.isArray(_queue) ? _queue : [];
    return _queue;
  }
  _queueLoading = true;
  try {
    const r = await apiFetch("/api/user/play-queue");
    if (!r.ok) { _queue = []; return _queue; }
    const j = await r.json();
    const items = Array.isArray(j?.items) ? j.items : [];
    // Keep ALL server rows here (no dedup). Earlier we deduped at load
    // time to avoid duplicate rows in the drawer, but that hid the
    // dupes from queueRemove — the user would click × and only one
    // dupe got deleted, so the row reappeared on next reload until
    // every dupe was clicked individually. Now dedup happens only at
    // render (see _renderQueueDrawer) and queueRemove deletes every
    // matching position.
    _queue = items;
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
      // Surface the server's actual error so we can diagnose instead
      // of always showing the generic "Could not add to queue".
      const errBody = await r.text().catch(() => "");
      console.warn("[queue] POST /api/user/play-queue failed:", r.status, errBody);
      if (typeof showToast === "function") {
        const msg = r.status === 401 ? "Sign in to use the queue"
                  : r.status === 400 ? `Queue rejected: ${errBody.slice(0, 100)}`
                  : `Could not add to queue (HTTP ${r.status})`;
        showToast(msg, "error");
      }
      return false;
    }
    _queue = null; // invalidate cache; next read refetches
    if (typeof showToast === "function") {
      const verb = mode === "next" ? "Playing next" : "Queued";
      const count = arr.length === 1 ? "" : ` (${arr.length})`;
      showToast(`${verb}${count}`);
    }
    // Serialize the refresh: previously this ran two concurrent
    // _queueLoad(true) calls (one for nav buttons, one inside
    // _renderQueueDrawer). The second call hit the _queueLoading
    // guard and returned `[]`, so the drawer rendered an empty list
    // even though the server had the new items. Single load now,
    // shared across both consumers.
    if (_queueDrawerEl?.classList.contains("open")) {
      await _renderQueueDrawer(); // does its own _queueLoad(true)
    } else {
      await _queueLoad(true);
    }
    _refreshPlayerNavButtons();
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
      // Release context: kept on the queue entry so queue auto-
      // advance can hand it to openVideo via _queueDispatchYtMeta
      // and keep the disc icon pointing at the right album.
      releaseType: meta?.releaseType || "",
      releaseId:   meta?.releaseId   || "",
    },
  }], opts);
}

// Bulk-add every YT-matched track from an album popup tracklist.
// Routes through queueAddAlbumOrPlay so it stays consistent with the
// Play-album button: if the album is already queued, no-op (toast);
// otherwise append all tracks to the tail.
async function queueAddAlbum(btn) {
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
        title:       el.dataset.track       || "",
        artist:      el.dataset.artist      || "",
        albumTitle:  el.dataset.album       || "",
        // Release context so the disc icon stays accurate when the
        // queue auto-advances through these tracks later.
        releaseType: el.dataset.releaseType || "",
        releaseId:   el.dataset.releaseId   || "",
      },
    });
  });
  if (!items.length) {
    if (typeof showToast === "function") showToast("No playable tracks on this album", "error");
    return false;
  }
  rows.forEach(el => el.classList.add("queued"));
  return queueAddAlbumOrPlay(items, { mode: "append" });
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
  console.debug("[_queuePlayNext] start", {
    queueLen: _queue?.length ?? 0,
    currentPos: _queueCurrentPosition,
    currentExt: _queuePlayingExternalId,
    repeat: _queueRepeat,
  });
  if (!_queue?.length) {
    _queueCurrentPosition = null;
    _queuePlayingExternalId = null;
    _refreshPlayerNavButtons();
    if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
    return false;
  }
  // Position-first lookup is the normal path; externalId fallback
  // catches the race where the optimistic-insert reconcile lagged
  // and _queueCurrentPosition still holds the synthetic fractional
  // value while _queue has the real server position. Without this
  // fallback findIndex returns -1 and the `currentIdx < 0 ? _queue[0]`
  // path replays the head — which is exactly the just-finished track
  // — instead of advancing.
  let currentIdx = _queueCurrentPosition != null
    ? _queue.findIndex(it => it.position === _queueCurrentPosition)
    : -1;
  if (currentIdx < 0 && _queuePlayingExternalId != null) {
    currentIdx = _queue.findIndex(it => String(it.externalId) === String(_queuePlayingExternalId));
  }
  let next = currentIdx >= 0 && currentIdx + 1 < _queue.length
    ? _queue[currentIdx + 1]
    : null;
  // Repeat-all: wrap to the first item.
  if (!next && _queueRepeat === "all" && _queue.length) {
    next = _queue[0];
  }
  if (!next) {
    // End of queue, no repeat: clear playing mark but keep items.
    _queueCurrentPosition = null;
    _queuePlayingExternalId = null;
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
  console.debug("[_queuePlayItem]", {
    source: entry.source,
    externalId: entry.externalId,
    title: entry.data?.title,
    releaseType: entry.data?.releaseType || "(missing)",
    releaseId:   entry.data?.releaseId   || "(missing)",
  });
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
      // Hand the queue's title/artist/album to openVideo via a
      // module-level handoff slot. Without this, openVideo's DOM
      // scrape (looking for .track-link rows in album popups)
      // picks up a wrong track or nothing — the bar's title
      // would show the wrong song. Cleared on consume.
      window._queueDispatchYtMeta = {
        track:       entry.data?.title       || "",
        album:       entry.data?.albumTitle  || "",
        artist:      entry.data?.artist      || "",
        // Release context: openVideo uses these to set _playerReleaseId
        // so the mini-player's disc icon stays linked across queue
        // auto-advances even when the original album modal is closed.
        releaseType: entry.data?.releaseType || "",
        releaseId:   entry.data?.releaseId   || "",
      };
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
  _queuePlayingExternalId = entry.externalId ?? null;
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
      _queuePlayingExternalId = null;
      if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
    }
    return false;
  }
  // No-metadata fast path — used by the URL bootstrap (?vd=…) where
  // openVideo has no clicked .track-link to scrape, and the queue-meta
  // handoff slot is empty. Inserting an "Untitled" entry was creating
  // duplicate queue rows (one Untitled + one with the real metadata
  // from a prior session) AND making two rows light up as "playing"
  // because the externalId predicate matched both. Instead, just mark
  // the externalId as playing — if the track is already in the queue
  // (server-side), the existing row lights up; if it's not, no row
  // lights up and that's fine, the bar still shows what's playing.
  // Signed-out users can't have a server-backed queue, but we still
  // want the playing track to surface in the drawer ("queue of one").
  // Maintain a local-only _queue for anon: replace contents with the
  // single playing item; ＋ buttons elsewhere still 401 / show a
  // "Sign in" toast as before.
  const isAnon = !window._clerk?.user;
  if (isAnon) {
    const data = itemPayload.data || {};
    const optimistic = {
      position: 1,
      source: itemPayload.source,
      externalId: itemPayload.externalId,
      data: {
        title: data.title || `Track ${itemPayload.externalId}`,
        artist: data.artist || "",
        albumTitle: data.albumTitle || "",
        image: data.image || "",
      },
    };
    _queue = [optimistic];
    _queueCurrentPosition = 1;
    _queuePlayingExternalId = String(itemPayload.externalId);
    if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
    _refreshPlayerNavButtons();
    return false;
  }
  const data = itemPayload.data || {};
  const hasMeta = !!(data.title || data.artist || data.albumTitle);
  if (!hasMeta) {
    _queuePlayingExternalId = String(itemPayload.externalId);
    // If queue is already loaded, point _queueCurrentPosition at the
    // existing row (if any) so prev/next nav works. Otherwise lazy-load
    // and patch up once the data lands.
    const apply = () => {
      const ex = Array.isArray(_queue)
        ? _queue.find(it => String(it.externalId) === String(itemPayload.externalId))
        : null;
      _queueCurrentPosition = ex ? ex.position : null;
      if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
      _refreshPlayerNavButtons();
    };
    if (Array.isArray(_queue)) {
      apply();
    } else {
      _queueLoad().then(() => {
        // Race-safe: only apply if the played externalId is still us.
        if (String(_queuePlayingExternalId) === String(itemPayload.externalId)) apply();
      }).catch(() => {});
    }
    return false;
  }
  // If the played item is already in the queue, MOVE it to the head
  // rather than leaving it at its current position with a different
  // marker. Every play re-orders the queue with the played track at
  // the top — that's the consistent mental model: "the bar shows
  // what's playing, the queue shows what's next, the played track
  // is always at the top."
  if (Array.isArray(_queue)) {
    const existing = _queue.find(it => String(it.externalId) === String(itemPayload.externalId));
    if (existing) {
      const oldPosition = existing.position;
      // Inherit any richer metadata from the existing queue entry
      // before we drop it. URL bootstraps (?vd=) call openVideo with
      // empty data because there's no clicked track row to scrape;
      // the existing queue entry might have full release context
      // (releaseType / releaseId etc.) saved from when it was added.
      // Without this merge, the optimistic-insert below would
      // overwrite that with empty strings and the disc icon would
      // hide for the rest of the session.
      const existingData = existing.data || {};
      const newData = itemPayload.data || {};
      itemPayload.data = {
        ...existingData,
        ...Object.fromEntries(
          Object.entries(newData).filter(([_, v]) => v !== "" && v != null)
        ),
      };
      // Push the merged data into the queue-dispatch meta slot so
      // openVideo's existing queueMeta path picks up the release
      // context for THIS play (without it, openVideo's release
      // resolver runs before the optimistic insert lands and finds
      // nothing).
      window._queueDispatchYtMeta = {
        track:       itemPayload.data.title       || "",
        album:       itemPayload.data.albumTitle  || "",
        artist:      itemPayload.data.artist      || "",
        releaseType: itemPayload.data.releaseType || "",
        releaseId:   itemPayload.data.releaseId   || "",
      };
      // Remove from local cache; the upcoming optimistic-insert path
      // will prepend a fresh copy at the head.
      _queue = _queue.filter(it => it.position !== oldPosition);
      // Drop the old server row in the background so the queue
      // doesn't end up with two copies after reconcile.
      apiFetch("/api/user/play-queue", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ position: oldPosition }),
      }).catch(() => {});
      // Fall through to the optimistic-insert path below.
    }
  }
  // SYNCHRONOUSLY update local state before the engine call returns
  // control: optimistically insert the new item at a synthetic position
  // lower than the current minimum and mark it as playing. Any
  // track-end event that fires DURING the async server insert (e.g. a
  // stale "ended" from the YT player as it swaps video) reads this
  // updated state — without it, _queuePlayNext would advance from the
  // old _queueCurrentPosition and replay the previously-playing track.
  // Empty queue also gets the insert: every play becomes a queue head
  // so the user always has a "back" anchor.
  const existing = Array.isArray(_queue) ? _queue : [];
  const minPos = existing.reduce((m, it) => Math.min(m, Number(it.position) || 0), Infinity);
  // Fractional position can't collide with any server-assigned integer;
  // gets reconciled to a real position after the server POST + refetch.
  const tempPos = (Number.isFinite(minPos) ? minPos : 1) - 0.5;
  const optimistic = { position: tempPos, source: itemPayload.source, externalId: itemPayload.externalId, data: itemPayload.data || {} };
  _queue = [optimistic, ...existing];
  _queueCurrentPosition = tempPos;
  _queuePlayingExternalId = String(itemPayload.externalId);
  if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
  _refreshPlayerNavButtons();
  // Now fire the server insert in the background and reconcile our
  // synthetic position to the real server-assigned one.
  (async () => {
    try {
      const r = await apiFetch("/api/user/play-queue", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items: [itemPayload], mode: "next" }),
      });
      if (!r.ok) {
        // Silent failure was masking real issues — the optimistic
        // insert appears to work locally, but a fresh _queueLoad
        // from server later wipes it. Log + toast so the user knows
        // their play didn't make it into the persisted queue.
        const errBody = await r.text().catch(() => "");
        console.warn("[queue] external-play insert POST failed:", r.status, errBody);
        if (r.status === 401) {
          if (typeof showToast === "function") showToast("Sign in to keep tracks in your queue", "error");
        } else {
          if (typeof showToast === "function") showToast(`Couldn't add to queue (HTTP ${r.status})`, "error");
        }
        return;
      }
      _queue = null;
      await _queueLoad(true);
      // After reconcile, find the item by externalId — race-safe
      // even if a concurrent _queueLoad reordered the list.
      if (_queue?.length) {
        // Race-safe: only update position if we're still the active
        // playing externalId. If the user clicked another track during
        // the round trip, _queuePlayingExternalId has moved on and
        // we don't want to clobber its position with this stale one.
        if (String(_queuePlayingExternalId) === String(itemPayload.externalId)) {
          const head = _queue.find(it => String(it.externalId) === String(itemPayload.externalId)) ?? _queue[0];
          if (head) _queueCurrentPosition = head.position;
        }
      }
      if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
      _refreshPlayerNavButtons();
    } catch (e) {
      // Network failure (offline, DNS, etc.) — the local optimistic
      // insert already happened so the queue drawer looks right, but
      // the row never made it to the server and a fresh tab won't see
      // it. Surface a soft warning so the user knows their queue is
      // session-only until reconnection.
      console.warn("[queue] external-play insert threw:", e);
      if (typeof showToast === "function") {
        showToast("Couldn't save queue — local only until you reconnect", "error");
      }
    }
  })();
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
      <button type="button" class="queue-drawer-close" onclick="queueToggleDrawer()" title="Close">&#9660;</button>
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
  // Only flash the "Loading…" placeholder on a cold drawer open
  // (when no rows are rendered yet). On subsequent re-renders (after
  // add / remove / play-next), keep the existing list visible until
  // the new data is in hand — feels much smoother than blanking the
  // whole drawer for each update. Scroll position is also preserved
  // by capturing scrollTop before the swap and restoring after.
  const hadRows = !!listEl.querySelector(".queue-row");
  if (!hadRows) {
    listEl.innerHTML = `<div class="queue-empty">Loading…</div>`;
  }
  const prevScroll = listEl.scrollTop;
  // Use the cached _queue when available — queueRemove / queueAdd /
  // optimistic-insert paths set the local snapshot to the truth they
  // want the user to see, then fire the server call in the background.
  // A force-reload here would race the DELETE/POST and re-show the row
  // the user just removed (or hide one they just added). _queueLoad(false)
  // returns the cache when populated, fetches only when null (cold drawer
  // open clears _queue first via queueToggleDrawer).
  await _queueLoad(false);
  if (!_queue?.length) {
    if (countEl) countEl.textContent = "";
    listEl.innerHTML = `<div class="queue-empty">Queue empty. Click ➕ on tracks, LOC items, or archive items to add them.</div>`;
    return;
  }
  // Render-time dedup: server can have multiple rows for the same
  // externalId (e.g., a re-played track inserted before the prior
  // DELETE landed). Show only the lowest-position copy in the drawer
  // so the user sees a single row per track, but keep _queue itself
  // un-deduped so queueRemove can sweep every server-side dupe in
  // one click.
  const seen = new Set();
  const visible = [];
  for (const it of _queue) {
    const key = String(it.externalId ?? "");
    if (key && seen.has(key)) continue;
    if (key) seen.add(key);
    visible.push(it);
  }
  if (countEl) countEl.textContent = `${visible.length} item${visible.length === 1 ? "" : "s"}`;
  listEl.innerHTML = visible.map(it => {
    const safeTitle  = escHtml(it.data?.title || "Untitled");
    const safeArtist = escHtml(it.data?.artist || "");
    // Position-match is the primary check; externalId is a race-safe
    // fallback for the brief window between an optimistic insert and
    // the server reconcile (otherwise URL-based playback at boot
    // would leave the indicator off until the POST round-trips).
    const isPlaying  = (it.position === _queueCurrentPosition)
      || (_queuePlayingExternalId && String(it.externalId) === _queuePlayingExternalId);
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
      ? `<img class="queue-row-thumb" src="${escHtml(thumbUrl)}" loading="lazy" width="40" height="40" decoding="async" alt="" onerror="this.classList.add('thumb-broken')">`
      : `<span class="queue-row-thumb queue-row-thumb-empty">${it.source === "loc" ? "♪" : "▶"}</span>`;
    return `
      <div class="queue-row${isPlaying ? " is-playing" : ""}" data-position="${it.position}">
        <span class="queue-row-handle" title="Drag to reorder">⋮⋮</span>
        <span class="queue-row-thumb-wrap" title="${sourceTitle}">
          ${thumbHtml}
          <span class="queue-row-source-badge queue-row-source-${it.source}"></span>
          ${isPlaying ? `<span class="queue-row-eq" aria-hidden="true"><i></i><i></i><i></i></span>` : ""}
        </span>
        <button class="queue-row-play" onclick="queueJumpTo(null,'${escHtml(String(it.externalId)).replace(/'/g, "\\'")}')" title="${isPlaying ? "Currently playing" : "Play this now"}">
          <span class="queue-row-title">${safeTitle}</span>
          ${safeArtist ? `<span class="queue-row-artist">${safeArtist}</span>` : ""}
        </button>
        <!-- Remove targets the row by externalId rather than position.
             Positions can shift under us when other tracks are inserted
             via "next" mode (the server shifts everything down by N to
             make room). Position captured here at render time would
             then point at a different row by the time the click fires.
             externalId is stable across position shifts. -->
        <button class="queue-row-remove" onclick="queueRemove(null,'${escHtml(String(it.externalId)).replace(/'/g, "\\'")}')" title="Remove from queue">×</button>
      </div>
    `;
  }).join("");
  // Restore scroll position so an add/remove doesn't yank the user
  // back to the top of a long queue.
  if (prevScroll > 0) listEl.scrollTop = prevScroll;
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
// queueRemove can be called with either:
//   queueRemove(position)              — legacy, position-based
//   queueRemove(null, externalId)      — new, externalId-based (stable
//                                        across position shifts caused
//                                        by concurrent "next"-mode
//                                        inserts on other rows).
// We resolve to the live position from `_queue` at click time so the
// DELETE always targets the row the user actually clicked, even if the
// queue was reshuffled between render and click.
async function queueRemove(position, externalId) {
  // Resolve the set of server rows to drop. When called with an
  // externalId, sweep EVERY row matching it — the server can have
  // dupes (re-played track inserted before the prior DELETE landed)
  // and a single click should remove all of them, not just one.
  // Falls back to a single-position delete for legacy callers.
  let positionsToDrop = [];
  if (externalId != null && Array.isArray(_queue)) {
    positionsToDrop = _queue
      .filter(it => String(it.externalId) === String(externalId))
      .map(it => it.position);
  } else if (position != null) {
    positionsToDrop = [position];
  }
  if (!positionsToDrop.length) return;

  // Was any of the swept rows the currently-playing one?
  const wasPlaying = positionsToDrop.includes(_queueCurrentPosition)
    || (externalId != null && _queuePlayingExternalId && String(externalId) === String(_queuePlayingExternalId));
  // Capture next-to-play BEFORE the local mutation. Walk FORWARD from
  // the playing row's index — _queue.find() returned the first
  // non-removed row in queue order, which is queue[0] = the queue's
  // beginning, not the row immediately after the one we're killing.
  // That made removing a mid-queue playing track restart playback
  // from the top.
  let advanceTo = null;
  if (wasPlaying && Array.isArray(_queue)) {
    const removingExt = externalId != null ? String(externalId) : null;
    // Locate the playing row's index. Prefer position match, fall
    // back to externalId so optimistic-insert fractional positions
    // still resolve.
    let curIdx = _queue.findIndex(it => it.position === _queueCurrentPosition);
    if (curIdx < 0 && _queuePlayingExternalId != null) {
      curIdx = _queue.findIndex(it => String(it.externalId) === String(_queuePlayingExternalId));
    }
    // Walk forward from there; skip every position we're removing
    // and (if removing by externalId) every row sharing that id.
    if (curIdx >= 0) {
      for (let i = curIdx + 1; i < _queue.length; i++) {
        const it = _queue[i];
        if (positionsToDrop.includes(it.position)) continue;
        if (removingExt && String(it.externalId) === removingExt) continue;
        advanceTo = it;
        break;
      }
    }
    // Wrap around for repeat-all when nothing follows.
    if (!advanceTo && _queueRepeat === "all" && _queue.length) {
      for (const it of _queue) {
        if (positionsToDrop.includes(it.position)) continue;
        if (removingExt && String(it.externalId) === removingExt) continue;
        advanceTo = it;
        break;
      }
    }
  }
  // Local state update — instant UI feedback.
  if (Array.isArray(_queue)) {
    _queue = _queue.filter(it => !positionsToDrop.includes(it.position));
  }
  if (wasPlaying) { _queueCurrentPosition = null; _queuePlayingExternalId = null; }
  _refreshPlayerNavButtons();
  _renderQueueDrawer();
  // Auto-advance synchronously off the local-state copy.
  if (wasPlaying) {
    const fresh = advanceTo ? (_queue || []).find(it => it.position === advanceTo.position) : null;
    if (fresh) {
      await _queuePlayItem(fresh);
    } else if (typeof playerClose === "function") {
      playerClose();
    }
  }
  // Fire one DELETE per server-side row in the background. Parallel
  // is fine — the server endpoint is idempotent per (user, position).
  for (const p of positionsToDrop) {
    apiFetch("/api/user/play-queue", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ position: p }),
    }).then(r => {
      if (!r.ok) console.warn("[queue] DELETE failed:", r.status);
    }).catch(e => console.warn("[queue] DELETE threw:", e));
  }
}

async function queueClear() {
  // Destructive — wipes every queued item server-side and stops
  // playback. Other destructive actions (sign-out, account delete,
  // offline-cache wipe) all confirm; this used to fire on the bare
  // click. Add a guard so a stray tap doesn't dump a 30-track queue.
  const count = Array.isArray(_queue) ? _queue.length : 0;
  if (count > 0 && !confirm(`Clear all ${count} queued track${count === 1 ? "" : "s"}? This stops playback and can't be undone.`)) {
    return;
  }
  try {
    await apiFetch("/api/user/play-queue", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ clear: true }),
    });
    _queue = [];
    _queueCurrentPosition = null;
    _queuePlayingExternalId = null;
    // Stop whatever's playing — clearing the queue is a "stop everything"
    // gesture. playerClose dispatches to the active engine (LOC or YT)
    // and tears the bar down; safe to call when nothing is playing.
    if (typeof playerClose === "function") {
      try { playerClose(); } catch {}
    }
    // Collapse the drawer — there's nothing left to manage and the
    // empty-state placeholder isn't useful enough to keep it open.
    if (_queueDrawerEl) _queueDrawerEl.classList.remove("open");
    _renderQueueDrawer();
  } catch {
    if (typeof showToast === "function") showToast("Could not clear queue", "error");
  }
}

// Click on a queue row jumps directly to that item (consumes preceding
// items from the queue). Items before the chosen one are dropped server-
// side too.
// queueJumpTo can be called with either:
//   queueJumpTo(position)           — legacy, position-based
//   queueJumpTo(null, externalId)   — externalId-based (stable across
//                                     position shifts from concurrent
//                                     "next"-mode inserts).
async function queueJumpTo(position, externalId) {
  await _queueLoad(true);
  if (!_queue?.length) return;
  let target = null;
  if (externalId != null) {
    target = _queue.find(it => String(it.externalId) === String(externalId)) || null;
  }
  if (!target && position != null) {
    target = _queue.find(it => it.position === position) || null;
  }
  if (!target) return;
  if (_queueCurrentPosition === target.position) return; // already playing
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
// ── Album-level helpers (Play album / Queue album) ─────────────────
// Single source of truth for the two album-level actions across the
// Discogs modal, archive popup, and any future surface. Both check
// whether the album is already represented in the queue (by its first
// track's externalId — every track in a given album shares an
// identifying prefix, so the first-track lookup is a reliable proxy)
// and behave smartly:
//
//   "play"   → if already queued, queueJumpTo the first track in
//              place; otherwise queueAdd(items, "next") + queuePlayHead
//              (insert at head, play first; existing queue continues
//              after this album ends).
//   "append" → if already queued, no-op (with toast); otherwise
//              queueAdd(items, "append") (add to tail without
//              interrupting playback).
//
// `items` is an array of queue-shaped objects: { source, externalId, data }.
async function queueAddAlbumOrPlay(items, opts) {
  if (!Array.isArray(items) || !items.length) return false;
  if (!window._clerk?.user) {
    if (typeof showToast === "function") showToast("Sign in to use the queue", "error");
    return false;
  }
  const mode = opts?.mode === "append" ? "append" : "play";
  await _queueLoad(true);
  const firstId = String(items[0].externalId ?? "");
  const existing = (_queue || []).find(it => String(it.externalId) === firstId);
  if (existing) {
    if (mode === "play") {
      // Album already in queue — jump to the first track in place.
      await queueJumpTo(existing.position);
      return true;
    }
    // mode "append" — already queued, no-op.
    if (typeof showToast === "function") showToast("Already in queue");
    return false;
  }
  if (mode === "play") {
    await queueAdd(items, { mode: "next" });
    await queuePlayHead();
    return true;
  }
  await queueAdd(items, { mode: "append" });
  return true;
}

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

// Mirror of _queueHasNext for the Prev button. True if we can step
// backward through the playlist (currentPosition is past the head, or
// repeat-all is on so we wrap to the tail).
function _queueHasPrev() {
  if (!Array.isArray(_queue) || _queue.length === 0) return false;
  if (_queueCurrentPosition == null) return false;
  if (_queueRepeat === "all") return true;
  const idx = _queue.findIndex(it => it.position === _queueCurrentPosition);
  return idx > 0;
}

// Step the queue backward — used by the Prev button on the mini-player
// when the cross-source queue is the active playback source. Returns
// true if a prior item was found (and is now playing); false if there's
// nothing to step back to (caller falls back to engine-internal prev).
async function _queuePlayPrev() {
  await _queueLoad();
  if (!_queue?.length || _queueCurrentPosition == null) return false;
  const idx = _queue.findIndex(it => it.position === _queueCurrentPosition);
  if (idx < 0) return false;
  let prev = idx > 0 ? _queue[idx - 1] : null;
  // Repeat-all: wrap to the last item.
  if (!prev && _queueRepeat === "all" && _queue.length) {
    prev = _queue[_queue.length - 1];
  }
  if (!prev) return false;
  return _queuePlayItem(prev);
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
// queue items without ever pressing play first. Auto-hidden when both
// the engine is idle AND the queue is empty (the bar has no manual
// close button anymore — the queue's × button is the only way to
// drop items).
function _queueRefreshIdleBar() {
  const bar = document.getElementById("mini-player");
  if (!bar) return;
  const engineActive = !!window._currentEngine;
  const hasItems = Array.isArray(_queue) && _queue.length > 0;
  if (engineActive) {
    bar.classList.remove("idle-queue");
    return;
  }
  if (!hasItems) {
    // Truly nothing to show — hide the bar.
    bar.classList.remove("idle-queue", "open", "expanded");
    document.body.classList.remove("player-open", "expanded-mini");
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
  // Default playpause icon is ⏸; in idle-queue mode no audio is loaded
  // so showing pause is misleading. Switch to ▶ — clicking it kicks
  // off queuePlayHead via the existing playerTogglePause guard.
  const ppBtn = document.getElementById("mini-playpause");
  if (ppBtn) {
    ppBtn.innerHTML = "&#9654;";  // ▶
    ppBtn.title = "Play queue";
  }
}

// ── Globals ─────────────────────────────────────────────────────────
window._queueHasNext = _queueHasNext;
window._queueHasPrev = _queueHasPrev;
window._queuePlayPrev = _queuePlayPrev;
window._queueHasPlayable = _queueHasPlayable;
window._queueOnExternalPlay = _queueOnExternalPlay;
window._queueGetCurrentPosition = () => _queueCurrentPosition;
// Stop button on the media bar uses this to "forget" what was playing
// without clearing the queue itself. Re-renders so the now-playing
// row indicator clears immediately, and re-runs the idle-bar logic so
// the bar resurfaces in queue-head idle mode if items remain.
window._queueClearPlayingMark = () => {
  _queueCurrentPosition = null;
  _queuePlayingExternalId = null;
  if (_queueDrawerEl?.classList.contains("open")) _renderQueueDrawer();
  _refreshPlayerNavButtons();
  setTimeout(() => { try { _queueRefreshIdleBar(); } catch {} }, 0);
};
window._queueGetRepeat = _queueGetRepeat;
window._queueCycleRepeat = _queueCycleRepeat;
window.queuePlayHead = queuePlayHead;
window.queueAddAlbumOrPlay = queueAddAlbumOrPlay;
window._queueRefreshIdleBar = _queueRefreshIdleBar;
window._refreshPlayerNavButtons = _refreshPlayerNavButtons;
// On script load: if the user has queued items from a previous
// session, surface the idle bar once the queue cache hydrates. We
// kick the load asynchronously so the page renders first, then the
// _refreshPlayerNavButtons in the load callback brings up the bar.
//
// SKIP this when the URL is requesting media playback (?vd=…) —
// app.js will fire openVideo for that URL within the same tick, so
// the engine becomes "yt" and _queueRefreshIdleBar would correctly
// no-op. The race used to land the idle bar with the saved queue
// head briefly visible before the URL track took over; URL-driven
// media should take precedence with no flicker.
document.addEventListener("DOMContentLoaded", () => {
  let urlPlayingSomething = false;
  try {
    const p = new URLSearchParams(location.search);
    urlPlayingSomething = !!(p.get("vd") || p.get("ld")); // ?vd= YouTube, ?ld= LOC
  } catch {}
  setTimeout(() => {
    _queueLoad(true).then(() => {
      // Idle-bar surface is harmless when an engine is already
      // active (it early-returns), but avoid the timing-window
      // flicker by skipping it entirely when the URL was already
      // asking for media to play.
      if (urlPlayingSomething) return;
      _refreshPlayerNavButtons();
    }).catch(() => {});
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
