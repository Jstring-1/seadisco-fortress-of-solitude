// ── Shared utilities for all pages (index, account, admin) ──────────────

// ── Lazy module loader ──────────────────────────────────────────────────
// Used to defer non-critical view code (archive, youtube, inventory
// editor) until the user actually navigates into that view. Each path
// is loaded at most once — repeat calls return the original promise so
// callers can `await _sdLoadModule(...)` freely.
//
// The version param matches the cache-bust version we use in the static
// <script> tags. Inlined here too so we don't have to thread it through
// every call site.
window._sdLoadedModules  = window._sdLoadedModules  || {};
window._sdModulePromises = window._sdModulePromises || {};
function _sdLoadModule(srcPath) {
  if (window._sdLoadedModules[srcPath]) return Promise.resolve();
  if (window._sdModulePromises[srcPath]) return window._sdModulePromises[srcPath];
  const v = window._SD_LAZY_VERSION || "";
  window._sdModulePromises[srcPath] = new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = srcPath + (v ? `?v=${encodeURIComponent(v)}` : "");
    s.async = false; // preserve order if multiple are loaded back-to-back
    s.onload  = () => { window._sdLoadedModules[srcPath] = true; resolve(); };
    s.onerror = () => reject(new Error("Failed to load " + srcPath));
    document.head.appendChild(s);
  });
  return window._sdModulePromises[srcPath];
}
window._sdLoadModule = _sdLoadModule;

// Stubs for entry points exported by lazy-loaded modules. These get
// called from inline onclick handlers built by other (eager) modules.
// The stub loads the real script and re-dispatches the call once the
// module's own assignment to window.openYoutubePopup / etc. wins.
// Tracked via a flag so we don't load + re-call recursively if the
// real module forgets to override.
function _sdLazyStub(modulePath, fnName) {
  return function (...args) {
    _sdLoadModule(modulePath).then(() => {
      const fn = window[fnName];
      // Only re-dispatch if the module actually replaced the stub.
      if (typeof fn === "function" && fn !== window["_sdStub_" + fnName]) {
        fn.apply(null, args);
      }
    }).catch(err => console.warn("[lazy] " + modulePath + " load failed:", err));
  };
}
if (typeof window.openYoutubePopup !== "function") {
  const stub = _sdLazyStub("/youtube.js", "openYoutubePopup");
  window._sdStub_openYoutubePopup = stub;
  window.openYoutubePopup = stub;
}
if (typeof window.openInventoryEditor !== "function") {
  const stub = _sdLazyStub("/inventory-editor.js", "openInventoryEditor");
  window._sdStub_openInventoryEditor = stub;
  window.openInventoryEditor = stub;
}

// ── Relative time formatting ─────────────────────────────────────────────
// Single helper covers both "syncedAt" displays (use fallback "never")
// and generic "ago" labels (default em-dash). Accepts ms-numbers or
// any Date-parsable value. Unifies what used to be fmtTime + fmtRelativeTime.
function fmtTime(ts, fallback = "\u2014") {
  if (!ts) return fallback;
  const ms = Date.now() - (typeof ts === "number" ? ts : new Date(ts).getTime());
  const mins = Math.floor(ms / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months}mo ago`;
  return `${Math.floor(days / 365)}y ago`;
}
// Back-compat alias \u2014 admin UI calls fmtRelativeTime in a few places.
const fmtRelativeTime = fmtTime;

// \u2500\u2500 YouTube search-meta hover hint \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
// Lives here (eagerly loaded with shared.js) so hover handlers on
// inline-rendered modal markup work even when youtube.js hasn't been
// pulled in yet. Used by the album-suggest "\ud83c\udfb5 N missing" link to show
// "last searched X ago" in the title attribute on first hover.
//
// Render an ISO timestamp as "5 minutes ago", "2 hours ago", "3 days ago",
// etc. Returns "" if the input can't be parsed.
function _ytFormatRelativeTime(iso) {
  if (!iso) return "";
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return "";
  const diffMs = Date.now() - t;
  if (diffMs < 0) return "just now";
  const sec = Math.floor(diffMs / 1000);
  if (sec < 60) return `${sec} second${sec === 1 ? "" : "s"} ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min} minute${min === 1 ? "" : "s"} ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr} hour${hr === 1 ? "" : "s"} ago`;
  const day = Math.floor(hr / 24);
  if (day < 30) return `${day} day${day === 1 ? "" : "s"} ago`;
  const mo = Math.floor(day / 30);
  if (mo < 12) return `${mo} month${mo === 1 ? "" : "s"} ago`;
  const yr = Math.floor(day / 365);
  return `${yr} year${yr === 1 ? "" : "s"} ago`;
}
window._ytFormatRelativeTime = _ytFormatRelativeTime;

// Hover enrichment for any YT-submission affordance carrying
// data-yt-q="<query>". On first hover, fire one /api/youtube/search-meta
// lookup (no quota cost \u2014 pure cache read) and update the title
// attribute with " \u00b7 last searched X ago" / " \u00b7 not searched yet".
// Subsequent hovers no-op via data-yt-meta-fetched flag.
async function _ytEnrichLastSearched(el) {
  if (!el || el.dataset.ytMetaFetched === "1") return;
  const q = el.dataset.ytQ || "";
  if (!q) return;
  el.dataset.ytMetaFetched = "1";   // optimistic \u2014 block re-fetches even if this errors
  if (typeof apiFetch !== "function") return;
  try {
    const r = await apiFetch(`/api/youtube/search-meta?q=${encodeURIComponent(q)}`);
    if (!r.ok) return;
    const j = await r.json();
    const baseTitle = el.dataset.ytTitleBase || el.title || "";
    if (!el.dataset.ytTitleBase) el.dataset.ytTitleBase = baseTitle;
    const suffix = j.lastSearchedAt
      ? ` \u00b7 last searched ${_ytFormatRelativeTime(j.lastSearchedAt)}`
      : " \u00b7 not searched yet";
    el.title = baseTitle + suffix;
  } catch { /* leave title as-is */ }
}
window._ytEnrichLastSearched = _ytEnrichLastSearched;

// \u2500\u2500 localStorage JSON helpers \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
// Parse a JSON value out of localStorage, falling back to `defaultVal`
// on missing key, parse errors, or sandboxed/disabled storage. Replaces
// the repeated `JSON.parse(localStorage.getItem(key) || "{}")` pattern
// in 4+ files. (Audit #6.)
function getStorageJSON(key, defaultVal) {
  try {
    const raw = localStorage.getItem(key);
    if (raw == null) return defaultVal;
    const parsed = JSON.parse(raw);
    return parsed == null ? defaultVal : parsed;
  } catch {
    return defaultVal;
  }
}
// Set a JSON value into localStorage, swallowing quota/disabled errors.
function setStorageJSON(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); return true; }
  catch { return false; }
}

// ── Body scroll lock for popups ─────────────────────────────────────────
// When any major popup (album modal, version overlay, lightbox, YouTube
// popup, inventory editor, …) is open, the page underneath must NOT
// scroll — only the popup's own content. body.modal-open already wires
// `overflow: hidden`; the trick is to add/remove that class correctly
// across nested popups (e.g. open YT popup ON TOP of an album popup
// then close just the YT popup — the album popup is still open and
// must keep the lock).
//
// Counter-based: every popup calls _sdLockBodyScroll(uniqueId) on open
// and _sdUnlockBodyScroll(uniqueId) on close. The class only comes off
// when every owner has released. Idempotent — repeat lock-by-same-id
// is a no-op (Set semantics).
window._sdScrollLockOwners = window._sdScrollLockOwners || new Set();
function _sdLockBodyScroll(id) {
  if (!id) return;
  window._sdScrollLockOwners.add(String(id));
  document.body.classList.add("modal-open");
}
function _sdUnlockBodyScroll(id) {
  if (!id) return;
  window._sdScrollLockOwners.delete(String(id));
  if (window._sdScrollLockOwners.size === 0) {
    document.body.classList.remove("modal-open");
  }
}
window._sdLockBodyScroll = _sdLockBodyScroll;
window._sdUnlockBodyScroll = _sdUnlockBodyScroll;

// ── Card display mode (compact / wide) ────────────────────────────
// User-toggleable via the ▦ Wide button on the search bar. Persisted
// in localStorage so the choice sticks across reloads and tabs. Wide
// mode roughly doubles per-card width so all artist + title text
// shows in full without clamping. Compact mode is the default.
const _SD_CARD_MODE_KEY = "sd_card_mode";
function _sdGetCardMode() {
  try { return localStorage.getItem(_SD_CARD_MODE_KEY) === "wide" ? "wide" : "compact"; }
  catch { return "compact"; }
}
function _sdApplyCardMode() {
  const mode = _sdGetCardMode();
  document.body.classList.toggle("card-mode-wide", mode === "wide");
  // Reflect state across EVERY toggle button on the page — the search
  // controls bar has one (#card-mode-toggle) and each records-view
  // controls row has its own (#cw-card-mode-toggle). Additional
  // surfaces can opt in just by adding the .card-mode-toggle-btn
  // class. Inline styles override the buttons' own inline `style=`
  // attributes (which set the bordered-pill base look).
  document.querySelectorAll(".card-mode-toggle-btn").forEach(btn => {
    btn.classList.toggle("is-on", mode === "wide");
    btn.style.color       = mode === "wide" ? "var(--accent)" : "var(--muted)";
    btn.style.borderColor = mode === "wide" ? "var(--accent)" : "var(--border)";
    btn.title = mode === "wide"
      ? "Wide card mode is ON — click for compact"
      : "Wide card mode (shows full title + artist) — click to enable";
  });
}
function _sdToggleCardMode() {
  const next = _sdGetCardMode() === "wide" ? "compact" : "wide";
  try { localStorage.setItem(_SD_CARD_MODE_KEY, next); } catch {}
  _sdApplyCardMode();
  // Toggling on enriches every visible release/master card with
  // cached image-strip + tracklist data so favorites, collection,
  // search results etc. all get the wide-card extras.
  _sdScheduleCardEnrich();
}
window._sdToggleCardMode = _sdToggleCardMode;
window._sdApplyCardMode = _sdApplyCardMode;

// ── Wide-card enrichment ────────────────────────────────────────
// Cards on most surfaces (favorites, collection, search results,
// recent strip, etc.) ship with just thumbnail + summary fields. In
// wide mode we want each card to also show a tiny image strip + an
// inline tracklist, both pulled from the per-album cache the user
// has already populated by clicking around. _sdEnrichWideCards
// scans the visible DOM, batches { id, type } pairs the server
// hasn't yet enriched, fires /api/cards/enrich, and patches the
// matching cards in place. Idempotent — already-enriched cards are
// skipped via a data-card-enriched attribute.
const _sdEnrichInflight = new Map();
let _sdEnrichDebounce = null;
function _sdScheduleCardEnrich() {
  if (_sdEnrichDebounce) clearTimeout(_sdEnrichDebounce);
  _sdEnrichDebounce = setTimeout(() => {
    _sdEnrichDebounce = null;
    _sdEnrichWideCards().catch(() => {}).finally(() => {
      // After each enrichment pass, mark whichever cards are still
      // sparse (cache-missed) so the user sees a ↻ refresh button.
      try { _sdMarkSparseCards(); } catch {}
    });
  }, 250);
}
window._sdScheduleCardEnrich = _sdScheduleCardEnrich;

async function _sdEnrichWideCards() {
  // Runs in BOTH compact and wide mode now — search results, recent,
  // suggestions, every surface — so we have tracks + images cached
  // and ready when the user toggles to wide. Compact CSS hides the
  // injected strip + tracklist (display:none) so layout there is
  // unchanged; the data just rides along.
  const candidates = document.querySelectorAll(
    ".card[data-card-id][data-card-type]:not([data-card-enriched])"
  );
  if (!candidates.length) return;
  const uniq = new Map();
  candidates.forEach(el => {
    const id = el.dataset.cardId;
    const type = el.dataset.cardType;
    if (!id || !type) return;
    const key = `${type}:${id}`;
    if (!uniq.has(key)) uniq.set(key, { id: Number(id), type });
  });
  if (!uniq.size) return;
  const items = [];
  for (const [key, val] of uniq.entries()) {
    if (_sdEnrichInflight.has(key)) continue;
    items.push(val);
  }
  if (!items.length) return;
  for (const it of items) _sdEnrichInflight.set(`${it.type}:${it.id}`, true);
  const batches = [];
  for (let i = 0; i < items.length; i += 200) batches.push(items.slice(i, i + 200));
  for (const batch of batches) {
    try {
      const r = await fetch("/api/cards/enrich", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items: batch }),
        cache: "no-store",
      });
      if (!r.ok) continue;
      const j = await r.json();
      const rows = Array.isArray(j?.items) ? j.items : [];
      for (const row of rows) _sdInjectEnrichmentIntoCards(row);
    } catch { /* leave un-enriched — cards still render fine */ }
    finally {
      for (const it of batch) _sdEnrichInflight.delete(`${it.type}:${it.id}`);
    }
  }
}
window._sdEnrichWideCards = _sdEnrichWideCards;

// Outer-card click handler. In compact mode the whole card opens the
// album modal (existing behaviour). In wide mode only clicks ON the
// main cover image trigger the modal — clicks on the body, tracklist,
// thumb strip, entity links, etc. are no-ops at this level so their
// own handlers (play, queue, search-by-entity) can run without the
// modal stealing focus.
function _sdCardOuterClick(event, id, type, url) {
  // Always cancel the href="#" navigation regardless of wide/compact —
  // the card link uses # as an anchor sentinel.
  if (event && event.preventDefault) event.preventDefault();
  if (document.body.classList.contains("card-mode-wide")) {
    const card = event.currentTarget;
    // Accept either the main cover image OR the placeholder div as
    // the clickable cover — items without artwork render a
    // .thumb-placeholder in the same slot, and previously the click
    // filter only matched <img>, so cover-less wide cards were
    // unclickable.
    const main = card?.querySelector(".card-thumb-wrap > img:first-of-type")
              || card?.querySelector(".card-thumb-wrap > .thumb-placeholder");
    const onMain = main && (event.target === main || main.contains(event.target));
    if (!onMain) return;
  }
  if (typeof openModal === "function") openModal(event, id, type, url);
}
window._sdCardOuterClick = _sdCardOuterClick;

// "Play all" / "Queue all" — walks the tracklist DOM of the card the
// button lives in, collects every resolved per-track YT URL (skipping
// disabled rows and the Full Album sentinel), and routes through the
// queue. mode="play" does next-insert + playHead so playback starts on
// the first track immediately; mode="append" tacks them onto the tail.
// Anon users hit queueAdd's localStorage path, so this works without
// sign-in. Distinct from the Full Album row above which plays a single
// concatenated video — this one stitches the individual track videos.
async function _sdQueueAlbumTracks(btn, mode) {
  const card = btn?.closest(".card");
  if (!card) return;
  const rows = card.querySelectorAll(".card-tracklist-rows > li:not(.card-track-fullalbum)");
  if (!rows.length) return;
  const items = [];
  rows.forEach(li => {
    const playEl = li.querySelector(".card-track-play.track-link");
    if (!playEl) return; // disabled row — no YT URL
    const url = playEl.dataset.video || "";
    const videoId = (typeof extractYouTubeId === "function") ? extractYouTubeId(url) : "";
    if (!videoId) return;
    items.push({
      source: "yt",
      externalId: String(videoId),
      data: {
        title:       playEl.dataset.track   || "",
        artist:      playEl.dataset.artist  || "",
        albumTitle:  playEl.dataset.album   || "",
        ytUrl:       url,
        releaseType: playEl.dataset.releaseType || "",
        releaseId:   playEl.dataset.releaseId   || "",
      },
    });
  });
  if (!items.length) {
    if (typeof showToast === "function") showToast("No playable tracks on this card", "error");
    return;
  }
  // Signed-in path can use queueAddAlbumOrPlay (handles the "already
  // queued → jump in place" case). Anons fall through to plain
  // queueAdd which writes to localStorage.
  try {
    if (window._clerk?.user && typeof queueAddAlbumOrPlay === "function") {
      await queueAddAlbumOrPlay(items, { mode: mode === "play" ? "play" : "append" });
      return;
    }
    if (typeof queueAdd === "function") {
      if (mode === "play") {
        await queueAdd(items, { mode: "next" });
        if (typeof queuePlayHead === "function") {
          try { await queuePlayHead(); } catch {}
        }
      } else {
        await queueAdd(items, { mode: "append" });
      }
    }
  } catch (e) {
    console.warn("[_sdQueueAlbumTracks] failed", e);
    if (typeof showToast === "function") showToast("Could not queue album tracks", "error");
  }
}
window._sdQueueAlbumTracks = _sdQueueAlbumTracks;

// Entity click on a card — pop a fresh search keyed off the artist /
// label name. event.stopPropagation keeps the outer card's modal-open
// click from also firing. Routes through the existing search form so
// the user lands on a normal results grid.
function _sdSearchEntityFromCard(scope, name) {
  const v = String(name || "").trim();
  if (!v) return;
  // Switch to the search view + populate the field + run.
  if (typeof switchView === "function") {
    try { switchView("search", true); } catch {}
  }
  try {
    const fld = document.getElementById(scope === "label" ? "f-label" : "f-artist");
    if (fld) fld.value = v;
    if (typeof toggleAdvanced === "function") toggleAdvanced(true);
  } catch {}
  if (typeof doSearch === "function") {
    try { doSearch(1); } catch {}
  }
}
window._sdSearchEntityFromCard = _sdSearchEntityFromCard;

// Match a track title against Discogs's videos array — same loose
// substring rule the modal's findVideo uses. Returns the YT URL on
// hit, "" on miss.
function _sdMatchTrackVideo(trackTitle, videos) {
  if (!trackTitle || !Array.isArray(videos) || !videos.length) return "";
  const tl = String(trackTitle).toLowerCase();
  for (const v of videos) {
    const vt = String(v?.title ?? "").toLowerCase();
    if (vt && (vt.includes(tl) || tl.includes(vt))) return v.uri || "";
  }
  return "";
}

// Sparse-card detection + ↻ refresh button. A card is "sparse" when
// the underlying release_cache row is missing — the Submitted /
// Favorites / Collection feed returned a fallback record with just
// the discogs id and type. Visual symptoms: title shows as
// "master 187632", no artist, no cover. We surface a small ↻ button
// so the user can force a fresh fetch (which populates release_cache)
// and the next render comes back full. Library-wide.
function _sdIsSparseCard(card) {
  if (!card) return false;
  const id = card.dataset.cardId;
  const type = card.dataset.cardType;
  if (!id || !type) return false;
  const titleEl = card.querySelector(".card-title");
  const titleText = (titleEl?.textContent || "").trim();
  const sparseTitle = !titleText || titleText === `${type} ${id}` || titleText === id;
  const hasImg = !!card.querySelector(".card-thumb-wrap > img:first-of-type");
  const hasArtist = !!card.querySelector(".card-artist");
  return sparseTitle && !hasImg && !hasArtist;
}

async function _sdRefreshSparseCard(btn) {
  const card = btn?.closest(".card");
  if (!card) return;
  const id = card.dataset.cardId;
  const type = card.dataset.cardType;
  if (!id || !type) return;
  const oldText = btn.textContent;
  btn.disabled = true;
  btn.textContent = "…";
  try {
    // Hit the existing /release/:id or /master/:id endpoint with
    // nocache=1 so the upstream Discogs fetch runs and the result
    // gets persisted to release_cache. Server-side already requires
    // a Discogs OAuth client (own creds for signed-in users, admin
    // fallback for demo/anon).
    const r = await apiFetch(`/${type}/${id}?nocache=1`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    // The response is a full Discogs record. Patch the visible
    // card with whatever fields we can scrape. Easiest: re-fire
    // enrichment which now finds the freshly-cached row.
    card.removeAttribute("data-card-enriched");
    if (typeof _sdScheduleCardEnrich === "function") _sdScheduleCardEnrich();
    // Update the visible title + artist immediately from the
    // response so the card stops looking sparse before enrichment
    // lands (~250ms later).
    const j = await r.json();
    const titleEl = card.querySelector(".card-title");
    const bodyEl  = card.querySelector(".card-body");
    if (j?.title && titleEl) titleEl.textContent = j.title;
    if (j?.artists && Array.isArray(j.artists) && j.artists.length && bodyEl) {
      const artistName = j.artists.map(a => a?.name).filter(Boolean).join(", ");
      // Insert/replace .card-artist if it didn't exist before.
      let artEl = card.querySelector(".card-artist");
      if (!artEl) {
        artEl = document.createElement("div");
        artEl.className = "card-artist";
        bodyEl.insertBefore(artEl, bodyEl.firstChild);
      }
      artEl.textContent = artistName;
    }
    // Replace the placeholder thumb with the cover, if any.
    const cover = (Array.isArray(j?.images) && j.images[0]?.uri) ? j.images[0].uri : "";
    if (cover) {
      const wrap = card.querySelector(".card-thumb-wrap");
      const placeholder = wrap?.querySelector(".thumb-placeholder");
      if (placeholder) {
        const img = document.createElement("img");
        img.src = cover;
        img.loading = "lazy";
        img.decoding = "async";
        img.width = 300;
        img.height = 300;
        placeholder.replaceWith(img);
      } else {
        const existingImg = wrap?.querySelector("img:first-of-type:not(.card-images-thumb)");
        if (existingImg) existingImg.src = cover;
      }
    }
    // Drop the sparse marker + the button itself; the card is now full.
    card.classList.remove("card-sparse");
    btn.remove();
  } catch (e) {
    console.warn("[card refresh] failed:", e);
    btn.disabled = false;
    btn.textContent = oldText;
    if (typeof showToast === "function") showToast("Couldn't refresh from Discogs", "error");
  }
}
window._sdRefreshSparseCard = _sdRefreshSparseCard;

// Scan visible cards, mark sparse ones, inject the ↻ button. Runs
// after every render and after each enrichment pass (since enrichment
// may have populated some cards, leaving only the truly cache-missed
// ones still sparse). Idempotent — uses card-sparse class as marker.
function _sdMarkSparseCards() {
  const cards = document.querySelectorAll(".card[data-card-id][data-card-type]");
  cards.forEach(card => {
    const sparse = _sdIsSparseCard(card);
    if (sparse && !card.classList.contains("card-sparse")) {
      card.classList.add("card-sparse");
      // Inject ↻ button into the body if missing.
      const body = card.querySelector(".card-body");
      if (body && !body.querySelector(".card-refresh-btn")) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "card-refresh-btn";
        btn.textContent = "↻ Re-fetch from Discogs";
        btn.title = "This album's metadata is stale or missing — click to fetch fresh data";
        btn.onclick = (ev) => {
          ev.preventDefault();
          ev.stopPropagation();
          _sdRefreshSparseCard(btn);
        };
        body.appendChild(btn);
      }
    } else if (!sparse && card.classList.contains("card-sparse")) {
      card.classList.remove("card-sparse");
      card.querySelector(".card-refresh-btn")?.remove();
    }
  });
}
window._sdMarkSparseCards = _sdMarkSparseCards;

function _sdInjectEnrichmentIntoCards(row) {
  if (!row || row.id == null || !row.type) return;
  const sel = `.card[data-card-id="${CSS.escape(String(row.id))}"][data-card-type="${CSS.escape(String(row.type))}"]`;
  const cards = document.querySelectorAll(sel);
  if (!cards.length) return;
  const escAttr = (s) => String(s ?? "")
    .replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
  const escText = (s) => String(s ?? "")
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const images = Array.isArray(row.images) ? row.images.filter(Boolean).slice(0, 12) : [];
  const tracks = Array.isArray(row.tracklist) ? row.tracklist.filter(t => t?.title).slice(0, 30) : [];
  const videos = Array.isArray(row.videos) ? row.videos : [];
  const overrides = (row.overrides && typeof row.overrides === "object") ? row.overrides : {};
  // Resolve a YT URL per track: override (by position) wins, else
  // Discogs videos by fuzzy title match.
  const trackUrls = tracks.map(t => {
    const ovId = overrides[String(t.position)];
    if (ovId) return `https://www.youtube.com/watch?v=${ovId}`;
    return _sdMatchTrackVideo(t.title, videos);
  });
  // Full-album special slot — backed by an override at position "ALBUM".
  const fullAlbumId  = overrides["ALBUM"] || "";
  const fullAlbumUrl = fullAlbumId ? `https://www.youtube.com/watch?v=${fullAlbumId}` : "";
  cards.forEach(card => {
    if (card.dataset.cardEnriched === "1") return;
    card.dataset.cardEnriched = "1";
    const cardArtist  = card.querySelector(".card-artist")?.textContent?.trim() || "";
    const cardTitle   = card.querySelector(".card-title")?.textContent?.trim()  || "";
    const releaseType = card.dataset.cardType || "";
    const releaseId   = card.dataset.cardId   || "";
    if (images.length > 1) {
      const wrap = card.querySelector(".card-thumb-wrap");
      if (wrap && !wrap.querySelector(".card-images-strip")) {
        const stripHtml = `<div class="card-images-strip">${images.map((u, i) =>
          `<img class="card-images-thumb${i === 0 ? " is-active" : ""}" src="${escAttr(u)}" alt="thumb ${i + 1}" loading="lazy" decoding="async" onclick="event.preventDefault();event.stopPropagation();_sdSwapCardCover(this,'${escAttr(u)}')" />`
        ).join("")}</div>`;
        wrap.insertAdjacentHTML("beforeend", stripHtml);
        // Full-size stack of additional images, rendered below the
        // thumb-picker. Skips the first image (already shown as the
        // main cover) and lets the card's overflow:hidden + fixed
        // height clip the bottom — naturally cuts off when the body
        // (tracklist) sets the card's height. Hidden in compact mode
        // by CSS (only `.card-mode-wide .card-images-stack` is shown).
        const stackHtml = `<div class="card-images-stack">${images.slice(1).map((u, i) =>
          `<img class="card-images-stack-img" src="${escAttr(u)}" alt="image ${i + 2}" loading="lazy" decoding="async" onclick="event.preventDefault();event.stopPropagation();_sdSwapCardCover(this,'${escAttr(u)}')" />`
        ).join("")}</div>`;
        wrap.insertAdjacentHTML("beforeend", stackHtml);
      }
    }
    if (tracks.length || fullAlbumUrl) {
      const body = card.querySelector(".card-body");
      if (body && !body.querySelector(".card-tracklist")) {
        // data-* attrs match the modal's track-link / queue-add-icon
        // contract so openVideo and _trackQueueAdd consume them as-is.
        // Title is wrapped in entityLookupLinkHtml so clicking it opens
        // the same SeaDisco / Wikipedia / Copy popup the modal uses.
        const trackRow = (t, idx) => {
          const url = trackUrls[idx];
          const titleHtml = (typeof entityLookupLinkHtml === "function" && t.title)
            ? entityLookupLinkHtml("track", t.title, { className: "card-track-title-link", trackArtist: cardArtist, title: `Lookup options for "${t.title}"` })
            : escText(t.title || "");
          // <span> not <a> for the same nested-anchor reason —
          // openVideo / _trackQueueAdd read by class + dataset, no
          // anchor semantics needed.
          const playBtn = url
            ? `<span role="button" tabindex="0" class="card-track-play card-track-play-active track-link" data-video="${escAttr(url)}" data-track="${escAttr(t.title || "")}" data-album="${escAttr(cardTitle)}" data-artist="${escAttr(cardArtist)}" data-release-type="${escAttr(releaseType)}" data-release-id="${escAttr(releaseId)}" onclick="event.preventDefault();event.stopPropagation();openVideo(event,'${escAttr(url).replace(/'/g, "\\'")}')" title="Play this track">▶</span>`
            : `<span class="card-track-play card-track-disabled" aria-hidden="true">▶</span>`;
          const queueBtn = url
            ? `<span role="button" tabindex="0" class="card-track-queue queue-add-icon" data-yt-url="${escAttr(url)}" data-track="${escAttr(t.title || "")}" data-album="${escAttr(cardTitle)}" data-artist="${escAttr(cardArtist)}" data-release-type="${escAttr(releaseType)}" data-release-id="${escAttr(releaseId)}" onclick="event.preventDefault();event.stopPropagation();_trackQueueAdd(this);return false" title="Add to queue">＋</span>`
            : `<span class="card-track-queue card-track-disabled" aria-hidden="true">＋</span>`;
          // Actions column moved to the LEFT of pos so the play/queue
          // affordances are the first thing the eye lands on. Grid
          // template in style.css matches: actions | pos | title | dur.
          return `<li>
            <span class="card-track-actions">${playBtn}${queueBtn}</span>
            <span class="card-track-pos">${escText(t.position || "")}</span>
            <span class="card-track-title">${titleHtml}</span>
            ${t.duration ? `<span class="card-track-dur">${escText(t.duration)}</span>` : ""}
          </li>`;
        };
        const fullAlbumRow = fullAlbumUrl
          ? `<li class="card-track-fullalbum">
              <span class="card-track-actions">
                <span role="button" tabindex="0" class="card-track-play card-track-play-active track-link" data-video="${escAttr(fullAlbumUrl)}" data-track="Full album" data-album="${escAttr(cardTitle)}" data-artist="${escAttr(cardArtist)}" data-release-type="${escAttr(releaseType)}" data-release-id="${escAttr(releaseId)}" onclick="event.preventDefault();event.stopPropagation();openVideo(event,'${escAttr(fullAlbumUrl).replace(/'/g, "\\'")}')" title="Play full album">▶</span>
                <span role="button" tabindex="0" class="card-track-queue queue-add-icon" data-fullalbum="1" data-yt-url="${escAttr(fullAlbumUrl)}" data-track="Full album" data-album="${escAttr(cardTitle)}" data-artist="${escAttr(cardArtist)}" data-release-type="${escAttr(releaseType)}" data-release-id="${escAttr(releaseId)}" onclick="event.preventDefault();event.stopPropagation();_trackQueueAdd(this);return false" title="Queue full album">＋</span>
              </span>
              <span class="card-track-pos">★</span>
              <span class="card-track-title">Full album</span>
            </li>`
          : "";
        // "Play all tracks" / "Queue all tracks" — only meaningful if
        // at least one track has a resolved YT URL. They build a batch
        // of queueAddYt-shaped items from the resolved trackUrls and
        // call _sdQueueAlbumTracks (declared below). Distinct from the
        // Full album single-video row above: this stitches the
        // individual per-track videos together, which is the closer
        // analogue of "play the album" when no full-album upload
        // exists. A tiny visual divider separates these two affordances.
        const playableCount = trackUrls.filter(Boolean).length;
        const headActions = playableCount > 0
          ? `<span class="card-tracklist-head-actions">
              <span role="button" tabindex="0" class="card-tracklist-playall card-track-play-active" data-card-id="${escAttr(releaseId)}" data-card-type="${escAttr(releaseType)}" onclick="event.preventDefault();event.stopPropagation();_sdQueueAlbumTracks(this,'play');return false" title="Play all tracks (queues every available track)">▶</span>
              <span role="button" tabindex="0" class="card-tracklist-queueall" data-card-id="${escAttr(releaseId)}" data-card-type="${escAttr(releaseType)}" onclick="event.preventDefault();event.stopPropagation();_sdQueueAlbumTracks(this,'append');return false" title="Queue all tracks (append every available track)">＋</span>
            </span>`
          : "";
        // Head row: ALL buttons on the left, count label on the right
        // showing "(playable / total) tracks". When no tracks have
        // resolved YT URLs the buttons are hidden but the count still
        // renders so the user knows how many tracks the album has.
        const headLabel = `<span class="card-tracklist-head-label" title="${playableCount} of ${tracks.length} tracks have a YouTube match">(${playableCount}/${tracks.length}) track${tracks.length === 1 ? "" : "s"}${fullAlbumUrl ? " · full album" : ""}</span>`;
        const tlHtml = `<div class="card-tracklist">
          <div class="card-tracklist-head">${headActions}${headLabel}</div>
          <ol class="card-tracklist-rows">${fullAlbumRow}${tracks.map((t, i) => trackRow(t, i)).join("")}</ol>
        </div>`;
        body.insertAdjacentHTML("beforeend", tlHtml);
      }
    }
  });
}

// MutationObserver — any new card rendered into a .card-grid (search
// Load More, page change, view switch) triggers a debounced enrichment
// pass. Skip outright in compact mode (the helper itself early-exits
// too, but skipping the timer churn keeps the page snappier).
if (typeof MutationObserver !== "undefined" && typeof document !== "undefined") {
  const startObserver = () => {
    if (document.body.__sdEnrichBodyObs) return;
    let _sparseDebounce = null;
    const obs = new MutationObserver(() => {
      // Sparse-card marking runs in BOTH compact and wide modes —
      // the ↻ button isn't a wide-only affordance, it's a "stale
      // cache" rescue.
      if (_sparseDebounce) clearTimeout(_sparseDebounce);
      _sparseDebounce = setTimeout(() => {
        _sparseDebounce = null;
        try { _sdMarkSparseCards(); } catch {}
      }, 200);
      // Enrichment runs site-wide regardless of card mode now. In
      // compact mode the cached track + image data is fetched and
      // injected, but CSS keeps it hidden (display:none) so the
      // visible compact card looks unchanged. Toggling to wide
      // immediately reveals the already-cached data with no fetch.
      _sdScheduleCardEnrich();
    });
    obs.observe(document.body, { childList: true, subtree: true });
    document.body.__sdEnrichBodyObs = obs;
  };
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", startObserver);
  } else {
    startObserver();
  }
}

// Card-mode boot — moved here AFTER the wide-card enrichment helpers
// are declared so the synchronous _sdScheduleCardEnrich() call below
// can resolve _sdEnrichDebounce / _sdEnrichInflight (declared just
// above). When this lived BEFORE those declarations, the function
// body hit the TDZ on every fresh load and the script aborted —
// which left renderSharedHeader's _SD_NAV_ICONS uninitialized too,
// blanking the navbar.
if (typeof document !== "undefined") {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => { _sdApplyCardMode(); _sdScheduleCardEnrich(); });
  } else {
    _sdApplyCardMode();
    _sdScheduleCardEnrich();
  }
}

// True when the current user is allowed into the YouTube submission
// flow + standalone /?v=youtube view. Three paths:
//   - admin (ADMIN_CLERK_ID match): always passes.
//   - demo (DEMO_CLERK_IDS allowlist): per-account access for the
//     Google API quota reviewer. Same UX as admin minus admin-only
//     mutations (✕ delete buttons stay gated on _isAdmin).
//   - YT_OPEN_TO_USERS env-var: broad signed-in access. Off by default.
// Consumed by every YT-feature gate so a single env-var flip changes
// behaviour everywhere.
window._sdHasYtAccess = function () {
  if (window._isAdmin) return true;
  if (window._sdIsDemo) return true;
  return !!(window._clerk?.user) && !!window._sdYtOpen;
};

// ── Mobile nav toggle ────────────────────────────────────────────────────
function toggleMobileNav() {
  document.getElementById("main-nav-tabs")?.classList.toggle("mobile-open");
}
document.addEventListener("click", e => {
  if (!e.target.closest("#main-nav-tabs") && !e.target.closest("#nav-hamburger")) {
    document.getElementById("main-nav-tabs")?.classList.remove("mobile-open");
  }
});

// ── Search param normalization ───────────────────────────────────────────
function normP(raw) {
  const m = { artist:"a", release_title:"r", label:"l", year:"y", genre:"g", style:"s", format:"f", type:"t", sort:"o" };
  const o = {};
  for (const [k, v] of Object.entries(raw)) { if (v) o[m[k] ?? k] = v; }
  return o;
}

function searchLabel(raw) {
  const p = normP(raw);
  const parts = [];
  if (p.q && (!p.a || p.q.toLowerCase() !== p.a.toLowerCase())) parts.push(p.q);
  if (p.a) parts.push(p.a);
  if (p.r) parts.push(p.r);
  if (p.l) parts.push(`${p.l} label`);
  if (p.g) parts.push(p.g);
  if (p.s) parts.push(p.s);
  if (p.f && p.f !== "Vinyl") parts.push(p.f);
  if (p.y) parts.push(p.y);
  return parts.join(" \u00b7 ") || "Search";
}

function paramsToUrl(raw) {
  const p = normP(raw);
  const u = new URLSearchParams();
  if (p.q) u.set("q",  p.q);
  if (p.a) u.set("ar", p.a);
  if (p.r) u.set("re", p.r);
  if (p.y) u.set("yr", p.y);
  if (p.l) u.set("lb", p.l);
  if (p.g) u.set("gn", p.g);
  if (p.s) u.set("st", p.s);
  if (p.f) u.set("fm", p.f);
  if (p.t) u.set("rt", p.t);
  if (p.o) u.set("sr", p.o);
  if (p.b) u.set("b",  p.b);
  return "/?" + u.toString();
}

// ── API base URL (empty for same-origin) ────────────────────────────────
const API = "";

// ── Auth-aware fetch wrapper ────────────────────────────────────────────
async function apiFetch(url, options = {}) {
  const headers = { ...(options.headers ?? {}) };
  try {
    const t = await getSessionToken();
    if (t) headers["Authorization"] = `Bearer ${t}`;
  } catch { /* not signed in */ }
  const res = await fetch(url, { ...options, headers });
  // On 401, force-refresh the token once and retry
  if (res.status === 401 && window._clerk?.session) {
    try {
      _cachedToken = null; _cachedTokenAt = 0;
      const t2 = await getSessionToken();
      if (t2) {
        headers["Authorization"] = `Bearer ${t2}`;
        return fetch(url, { ...options, headers });
      }
    } catch { /* give up */ }
  }
  return res;
}

// ── HTML escape ─────────────────────────────────────────────────────────
function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// ── Shared Clerk bootstrap ───────────────────────────────────────────────
// Loads Clerk JS and returns the Clerk instance. Pages handle post-auth UI.

// Cached token + timestamp for getSessionToken()
let _cachedToken = null;
let _cachedTokenAt = 0;

// Reliable token getter — uses cache for <50s, otherwise refreshes.
// All pages should call this instead of clerk.session?.getToken() directly.
async function getSessionToken() {
  const c = window._clerk || window.Clerk;
  if (!c?.user || !c?.session) return null;

  // Clerk JWTs expire after ~60s; refresh if cache is >50s old
  if (_cachedToken && (Date.now() - _cachedTokenAt) < 50000) {
    return _cachedToken;
  }

  // Try to get a fresh token, retry up to 3 times with 300ms gaps
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const t = await c.session.getToken();
      if (t) {
        _cachedToken = t;
        _cachedTokenAt = Date.now();
        return t;
      }
    } catch { /* ignore */ }
    if (attempt < 2) await new Promise(r => setTimeout(r, 300));
  }
  // Return stale cache as last resort (server will reject if truly expired)
  return _cachedToken;
}

async function loadClerkInstance() {
  // Fast path: the server inlines a preloaded <script async> tag for
  // clerk-js in <head> (via CLERK_SCRIPT_INJECT), so on most page loads
  // window.Clerk is already defined or will appear within a few ms.
  // Skip the /api/config round-trip + dynamic <script> creation.
  let c = window.Clerk;
  if (!c) {
    // Wait briefly for the preloaded async script to finish downloading
    for (let i = 0; i < 40 && !window.Clerk; i++) {
      await new Promise(r => setTimeout(r, 25));
    }
    c = window.Clerk;
  }

  // Fallback: no preloaded script (e.g. dev, or template injection missing) —
  // load Clerk the old dynamic way
  if (!c) {
    let pk = "";
    const cached = getStorageJSON("_clerkCfg", null);
    if (cached?.pk && (Date.now() - (cached.ts || 0)) < 3600000) pk = cached.pk;
    if (!pk) {
      const cfg = await fetch("/api/config").then(r => r.json()).catch(() => ({}));
      pk = cfg.clerkPublishableKey || "";
      if (pk) setStorageJSON("_clerkCfg", { pk, ts: Date.now() });
    }
    if (!pk) return null;

    const frontendApi = atob(pk.replace(/^pk_(test|live)_/, "")).replace(/\$$/, "");
    await new Promise((resolve, reject) => {
      const s = document.createElement("script");
      s.src = `https://${frontendApi}/npm/@clerk/clerk-js@latest/dist/clerk.browser.js`;
      s.setAttribute("data-clerk-publishable-key", pk);
      s.setAttribute("crossorigin", "anonymous");
      s.onload = resolve; s.onerror = reject;
      document.head.appendChild(s);
    });

    // Poll for window.Clerk to be defined (script may take time to initialize)
    c = await new Promise((resolve) => {
      if (window.Clerk) { resolve(window.Clerk); return; }
      let tries = 0;
      const iv = setInterval(() => {
        tries++;
        if (window.Clerk) { clearInterval(iv); resolve(window.Clerk); }
        else if (tries > 60) { clearInterval(iv); resolve(null); } // 3s timeout
      }, 50);
    });
    if (!c) return null;
  }

  // Pass localization here — clerk-js applies it globally to every widget
  // mounted afterward. Per-component `localization` on mountSignUp /
  // openSignIn is silently ignored by the vanilla-JS SDK.
  await c.load({ localization: SEADISCO_CLERK_LOCALIZATION });

  // After c.load() resolves, Clerk has either hydrated the session or
  // confirmed there isn't one. Only poll briefly if the state is still
  // ambiguous (user === undefined). Previously we polled up to 3s on every
  // load, which dominated page TTI — drop that to ~500ms max.
  if (c.user === null) {
    // Confirmed signed-out — no need to poll
    return c;
  }
  if (c.user && c.session) {
    // Signed-in — warm the token cache but don't block more than 400ms
    try {
      const t = await Promise.race([
        c.session.getToken(),
        new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 400)),
      ]);
      if (t) { _cachedToken = t; _cachedTokenAt = Date.now(); }
    } catch { /* token will be refreshed on first apiFetch */ }
    return c;
  }
  // Ambiguous state (user === undefined) — short poll, 500ms max
  for (let i = 0; i < 5; i++) {
    await new Promise(r => setTimeout(r, 100));
    if (c.user === null) return c; // signed out
    if (c.user && c.session) {
      try {
        const t = await c.session.getToken();
        if (t) { _cachedToken = t; _cachedTokenAt = Date.now(); }
      } catch {}
      return c;
    }
  }
  return c;
}

// ── Shared auth initializer ─────────────────────────────────────────────
// One function for all pages. Callbacks:
//   onSignedIn(clerk)  — user is authenticated, build page
//   onSignedOut(clerk) — no user, show sign-in / public view
//   onError(msg)       — Clerk failed to load
//   onReady(clerk)     — always fires last (for resolving auth-ready promises)
async function initAuth({ onSignedIn, onSignedOut, onError, onReady } = {}) {
  try {
    const clerk = await loadClerkInstance();
    if (!clerk) {
      onError?.("Auth not configured");
      onReady?.(null);
      return null;
    }
    window._clerk = clerk;

    let _wasSignedIn = !!clerk.user;

    // Resolve onReady IMMEDIATELY once the user state is known, so
    // authReadyPromise unblocks and the page can render. Fire onSignedIn /
    // onSignedOut in the background — their async work (token checks,
    // loadDiscogsIds, etc.) shouldn't gate initial page rendering.
    onReady?.(clerk);

    if (clerk.user) {
      Promise.resolve(onSignedIn?.(clerk)).catch(err => console.error("onSignedIn error:", err));
    } else {
      Promise.resolve(onSignedOut?.(clerk)).catch(err => console.error("onSignedOut error:", err));
    }

    // Listen for GENUINE auth state changes only.
    // Clerk's addListener fires transiently with null user during hydration,
    // which would flash "access denied" on already-authenticated pages.
    // Only fire callbacks when the state actually changes.
    clerk.addListener(({ user }) => {
      if (user && !_wasSignedIn) {
        _wasSignedIn = true;
        onSignedIn?.(clerk);
      } else if (!user && _wasSignedIn) {
        _wasSignedIn = false;
        onSignedOut?.(clerk);
      }
    });

    return clerk;
  } catch (err) {
    onError?.(err.message ?? String(err));
    onReady?.(null);
    return null;
  }
}

// ── Shared Clerk theme + sign-in modal helper ───────────────────────────
// Built dynamically from the active CSS theme variables so the sign-in
// / sign-up modal matches whichever site theme admin has set. Re-read
// at modal-open time (themes can change between visits).
function _seaDiscoBuildClerkAppearance() {
  const css = (name, fallback) => {
    try {
      const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
      return v || fallback;
    } catch { return fallback; }
  };
  const bg      = css("--bg",      "#15120e");
  const surface = css("--surface", "#0e0c08");
  const text    = css("--text",    "#e8dcc8");
  const muted   = css("--muted",   "#a89880");
  const accent  = css("--accent",  "#ff6b35");
  const border  = css("--border",  "#2e2518");
  // Card border uses a fixed mid-grey rather than --border because
  // some themes set --border close to --bg, which makes the modal
  // blend into the page. A subtle but visible grey edge separates
  // the popup from whatever's behind it on every theme.
  const cardBorder = "rgba(255, 255, 255, 0.18)";
  return {
    variables: {
      colorBackground:      bg,
      colorInputBackground: surface,
      colorInputText:       text,
      colorText:            text,
      colorTextSecondary:   muted,
      colorPrimary:         accent,
      colorDanger:          "#e05050",
      colorNeutral:         muted,
      borderRadius:         "6px",
      fontFamily:           "system-ui, -apple-system, sans-serif",
    },
    elements: {
      card:             `background:${bg}; border:1px solid ${cardBorder}; box-shadow:0 10px 40px rgba(0,0,0,0.7);`,
      headerTitle:      `color:${text};`,
      headerSubtitle:   `color:${muted};`,
      formFieldLabel:   `color:${muted};`,
      formFieldInput:   `background:${surface}; border:1px solid ${border}; color:${text};`,
      footerActionLink: `color:${accent};`,
      socialButtonsBlockButton: `background:${surface}; border:1px solid ${border}; color:${text};`,
      // Primary "Continue" / "Sign in" button — fixed white text on
      // accent background. The previous `color:${bg}` was blending
      // in on themes where --bg was close to --accent in luminance.
      formButtonPrimary: `background:${accent}; color:#fff; border:none; font-weight:600;`,
      // Clerk's footer "Secured by" is hidden via .cl-footer in style.css.
    },
  };
}
// Backwards-compat constant — some sites read this directly. Build
// once at script load using whatever theme is applied at that moment.
const SEADISCO_CLERK_APPEARANCE = _seaDiscoBuildClerkAppearance();

// Public mode — registration is open. Localization just tunes the
// default Clerk copy to SeaDisco wording. Keys match Clerk's default
// structure so any unspecified strings fall back to English defaults.
// Clerk-js applies localization from Clerk.load() — see loadClerkInstance().
const SEADISCO_CLERK_LOCALIZATION = {
  signIn: {
    start: {
      title:      "Sign in to SeaDisco",
      subtitle:   "Welcome back",
      actionText: "Don't have an account?",
      actionLink: "Sign up",
    },
  },
  signUp: {
    start: {
      title:      "Create your SeaDisco account",
      subtitle:   "Sign up to sync your Discogs collection, wantlist, and favorites.",
      actionText: "Already have an account?",
      actionLink: "Sign in",
    },
  },
};

// Open Clerk's sign-in modal overlay (no view change). If the user is
// already signed in, route to the Account view instead. Falls back to
// the legacy account view if Clerk's modal API is unavailable.
async function openSignInModal() {
  try {
    const c = window._clerk || await loadClerkInstance();
    if (!c) {
      // Auth not configured — fall back to account view if SPA, else /account
      if (typeof switchView === "function") switchView("account");
      else location.href = "/?v=account";
      return;
    }
    if (c.user) {
      if (typeof switchView === "function") switchView("account");
      else location.href = "/?v=account";
      return;
    }
    if (typeof c.openSignIn === "function") {
      // Localization is applied globally via Clerk.load() — see
      // loadClerkInstance(). Per-component localization is ignored by the
      // vanilla-JS SDK.
      c.openSignIn({
        // Rebuild from current theme at open time so a theme switch
        // mid-session is reflected the next time the modal opens.
        appearance: _seaDiscoBuildClerkAppearance(),
        afterSignInUrl: location.pathname + location.search,
        afterSignUpUrl: location.pathname + location.search,
      });
    } else {
      // Older Clerk build without modal support — fall back
      if (typeof switchView === "function") switchView("account");
      else location.href = "/?v=account";
    }
  } catch (e) {
    console.error("[openSignInModal] failed:", e);
    if (typeof switchView === "function") switchView("account");
    else location.href = "/?v=account";
  }
}

// Open Clerk's sign-UP modal directly — what the anon splash's
// "Sign up" button calls. Landing on the sign-IN tab first (the
// regular openSignInModal) was confusing since the visitor doesn't
// have an account yet to sign in to.
async function openSignUpModal() {
  try {
    const c = window._clerk || await loadClerkInstance();
    if (!c) {
      if (typeof switchView === "function") switchView("account");
      else location.href = "/?v=account";
      return;
    }
    if (c.user) {
      if (typeof switchView === "function") switchView("account");
      else location.href = "/?v=account";
      return;
    }
    if (typeof c.openSignUp === "function") {
      c.openSignUp({
        appearance: _seaDiscoBuildClerkAppearance(),
        afterSignInUrl: location.pathname + location.search,
        afterSignUpUrl: location.pathname + location.search,
      });
    } else if (typeof c.openSignIn === "function") {
      // Older Clerk builds: fall back to sign-in modal; user can
      // tap the sign-up tab manually.
      c.openSignIn({
        appearance: _seaDiscoBuildClerkAppearance(),
        afterSignInUrl: location.pathname + location.search,
        afterSignUpUrl: location.pathname + location.search,
      });
    } else {
      if (typeof switchView === "function") switchView("account");
      else location.href = "/?v=account";
    }
  } catch (e) {
    console.error("[openSignUpModal] failed:", e);
    if (typeof switchView === "function") switchView("account");
    else location.href = "/?v=account";
  }
}

// ── Inline nav icons (line-art vinyl set; uses currentColor) ────────────
// 24×24 viewBox; SVGs have no fixed width/height so the .nav-icon
// container's CSS sizing wins. fill="none" + stroke="currentColor" so
// each theme tints them via the nav tab's color.
const _SD_NAV_ICONS = {
  // Magnifier over a vinyl record
  search: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><circle cx="10" cy="10" r="6.5"/><circle cx="10" cy="10" r="3.2"/><circle cx="10" cy="10" r="0.7" fill="currentColor"/><path d="m15 15 5 5"/></svg>`,
  // Two stacked vinyl records (3/4 view) for Collection
  collection: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><circle cx="10" cy="12" r="7"/><circle cx="10" cy="12" r="2.5"/><circle cx="10" cy="12" r="0.6" fill="currentColor"/><path d="M16 6.5c2.5 1.2 4 3.6 4 6.5s-1.5 5.3-4 6.5"/><path d="M14 5.2c.7-.1 1.4-.2 2-.2"/></svg>`,
  // Vinyl with a small ribbon/banner across the top for Wantlist
  wantlist: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="13" r="6.5"/><circle cx="12" cy="13" r="2.4"/><circle cx="12" cy="13" r="0.6" fill="currentColor"/><path d="M9 3h6v5l-3-2-3 2z"/></svg>`,
  // Small vinyl plus three horizontal lines (track listing) for Lists
  lists: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><circle cx="6" cy="6" r="2.5"/><circle cx="6" cy="6" r="0.6" fill="currentColor"/><path d="M11 6h9"/><path d="M3 12h17"/><path d="M3 18h17"/></svg>`,
  // Crate with vinyl tops poking out for Inventory
  inventory: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="9" r="3.5"/><circle cx="9" cy="9" r="0.6" fill="currentColor"/><circle cx="15" cy="9.5" r="3"/><circle cx="15" cy="9.5" r="0.6" fill="currentColor"/><path d="M3 14h18v6H3z"/><path d="M3 17h18"/></svg>`,
  // Vinyl with a heart in the center for Favorites
  favorites: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="13" r="7"/><path d="M12 16.2c-1-.7-3.2-2-3.2-3.7 0-1 .8-1.8 1.8-1.8.7 0 1.1.4 1.4.8.3-.4.7-.8 1.4-.8 1 0 1.8.8 1.8 1.8 0 1.7-2.2 3-3.2 3.7Z"/></svg>`,
  // Person silhouette with two small vinyl circles as headphones for Account
  account: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><circle cx="6" cy="9" r="2.5"/><circle cx="6" cy="9" r="0.6" fill="currentColor"/><circle cx="18" cy="9" r="2.5"/><circle cx="18" cy="9" r="0.6" fill="currentColor"/><path d="M6 9c0-3.3 2.7-6 6-6s6 2.7 6 6"/><path d="M5 21c1-3.5 4-5 7-5s6 1.5 7 5"/></svg>`,
  // Two-eighth-notes pair for the "Picks" tab — community-contributed
  // YouTube videos for tracks Discogs missed. Tinted purple via the
  // tab's color rule so it reads as a distinct contribution surface.
  picks: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><path d="M9 17V5l11-2v12"/><circle cx="6.5" cy="17" r="2.5" fill="currentColor"/><circle cx="17.5" cy="15" r="2.5" fill="currentColor"/></svg>`,
  // Question mark in a circle for the "Discover" tab — LOC /
  // Wikipedia / Archive / YouTube external-source group. Reads as
  // "look it up." Plain circle outline with no inner grooves so it
  // doesn't echo the vinyl-themed icons (search magnifier,
  // collection, wantlist, favorites all share those grooves).
  discover: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M9.5 9.5c0-2 1-3 2.5-3s2.5 1 2.5 2.5-1 2.2-2.5 3v1.5"/><circle cx="12" cy="16.5" r="0.7" fill="currentColor"/></svg>`,
};
// Expose icons globally so card badges (and any future surfaces) can
// reuse the same line-art set without duplicating SVG markup.
window._sdNavIconSvg = function (key) { return _SD_NAV_ICONS[key] || ""; };

// ── Shared header injection ──────────────────────────────────────────────
function renderSharedHeader(opts) {
  const isSPA = opts?.spa;
  const active = opts?.active || "";
  // Opt-in icon nav: each tab renders as [icon][hover-label] instead of
  // bare text. Used for the admin page first while we evaluate fit.
  const iconNav = !!opts?.iconNav;

  // Wrap a label in icon+hover-label markup when iconNav is on; otherwise
  // return the bare label text. The icon key matches a _SD_NAV_ICONS slot.
  const labelMarkup = (label, iconKey) => {
    if (!iconNav) return label;
    const icon = _SD_NAV_ICONS[iconKey] || "";
    return `<span class="nav-icon" aria-hidden="true">${icon}</span><span class="nav-label">${label}</span>`;
  };
  const navTabClass = iconNav ? "nav-tab-top icon-nav" : "nav-tab-top";

  // Nav tab helper — single row: Search, record tabs, Account
  const tab = (label, view, iconKey) => {
    if (isSPA) {
      const cls = view === active ? ' active' : '';
      return `<button class="${navTabClass}${cls}" data-view="${view}" onclick="switchView('${view}')" title="${label}">${labelMarkup(label, iconKey)}</button>`;
    }
    const href = view === "search" ? "/" : `/?v=${view}`;
    const activeCls = view === active ? ' active' : '';
    return `<a class="${navTabClass}${activeCls}" href="${href}" title="${label}">${labelMarkup(label, iconKey)}</a>`;
  };

  // Discover tab — covers the LOC / Wikipedia / Archive / YouTube
  // group. Default click lands on LOC (most-used external source);
  // the in-page sub-nav strip handles cross-tab switching once
  // you're on one. data-view is set to "discover" so syncDiscoverTabActive
  // (defined below) can flip the active class when any of the four
  // sub-views is active.
  const _DISCOVER_VIEWS = new Set(["loc", "wiki", "archive", "youtube"]);
  const discoverTab = (label, iconKey) => {
    const isActive = _DISCOVER_VIEWS.has(active);
    const activeCls = isActive ? ' active' : '';
    if (isSPA) {
      return `<button class="${navTabClass}${activeCls}" data-view="discover" onclick="switchView('loc')" title="${label}">${labelMarkup(label, iconKey)}</button>`;
    }
    return `<a class="${navTabClass}${activeCls}" href="/?v=loc" data-view="discover" title="${label}">${labelMarkup(label, iconKey)}</a>`;
  };

  // Record tab — starts disabled until signed in. `startEmpty` adds
  // the nav-rec-empty class on first paint so tabs that only matter
  // when the user has data (Inventory, Lists) are HIDDEN until
  // _updateEmptyRecordTabs flips the class off after sync. Without
  // this, the navbar pops Inventory/Lists into existence on initial
  // render then yanks them back when the empty-counts check runs —
  // the visible "jump" the user sees on load.
  const recTab = (label, rtab, iconKey, startEmpty) => {
    const emptyCls = startEmpty ? " nav-rec-empty" : "";
    if (isSPA) {
      return `<button class="${navTabClass} nav-rec-disabled${emptyCls}" data-rtab="${rtab}" onclick="showRecordSignIn('${rtab}')" title="${label}">${labelMarkup(label, iconKey)}</button>`;
    }
    return `<a class="${navTabClass}${emptyCls}" href="/?v=${rtab}" data-rtab="${rtab}" title="${label}">${labelMarkup(label, iconKey)}</a>`;
  };

  // Auth tab removed from the navbar — the footer "Account" link
  // (rendered by renderSharedFooter) covers both signed-in and
  // signed-out paths. applyAuthState in app.js still tries to update
  // a #nav-auth-tab element if present and silently no-ops otherwise,
  // so no follow-up cleanup is required.

  const header = document.getElementById("site-header");
  if (!header) return;
  // Site build/version tag shown as tiny grey text under the logo. Updated
  // whenever the cache-bust version is bumped so the user can eyeball whether
  // they're on the latest build without digging into devtools.
  const SITE_VERSION = "build 20260505.1642";
  header.innerHTML = `
    <div class="header-logo-wrap">
      <a href="${isSPA ? 'javascript:void(0)' : '/'}" ${isSPA ? 'onclick="if(typeof goHome===\'function\'){goHome();return false;}"' : ''} class="header-logo text-logo"><span class="logo-hi">SEA</span><span class="logo-lo">rch</span><span class="logo-gap"></span><span class="logo-hi">DISCO</span><span class="logo-lo">gs</span></a>
      <div class="header-version" title="Current build">${SITE_VERSION}</div>
    </div>
    ${isSPA ? '<h1 class="sr-only">SeaDisco — Music Discovery Platform: Search &amp; Collection</h1>' : ''}
    <nav id="main-nav">
      <button id="nav-hamburger" onclick="toggleMobileNav()" aria-label="Open navigation">
        <span></span><span></span><span></span>
      </button>
      <div id="nav-tabs-wrap">
        <div id="main-nav-tabs">
          <div class="nav-row nav-row-top" id="nav-row-records">
            ${tab("Search", "search", "search")}
            ${recTab("Favorites", "favorites", "favorites")}
            ${recTab("Collection", "collection", "collection")}
            ${recTab("Wantlist", "wantlist", "wantlist")}
            ${recTab("Inventory", "inventory", "inventory", true)}
            ${recTab("Lists", "lists", "lists", true)}
            ${discoverTab("Discover", "discover")}
          </div>
        </div>
      </div>
    </nav>`;

  // On non-SPA pages (account, admin), update auth tab once Clerk resolves
  if (!isSPA) {
    loadClerkInstance().then(c => {
      if (c?.user) {
        const el = document.getElementById("nav-auth-tab");
        if (el) {
          el.title = "Account";
          // Same iconNav-safe pattern as applyAuthState — preserve the
          // SVG markup by updating only the label span.
          const labelSpan = el.querySelector(".nav-label");
          if (labelSpan) labelSpan.textContent = "Account";
          else el.textContent = "Account";
        }
      }
    }).catch(() => {});
  }
  // Re-apply the card-mode state now that the navbar (and its
  // #nav-card-mode-toggle button) is in the DOM. The boot block above
  // ran at DOMContentLoaded too, but renderSharedHeader's handler may
  // have run after it, leaving the freshly-painted button without its
  // is-on class. Idempotent — only re-toggles classes/styles.
  if (typeof _sdApplyCardMode === "function") {
    try { _sdApplyCardMode(); } catch {}
  }
}

// ── Shared footer injection ──────────────────────────────────────────────
// Build a /?v=… href that preserves the user's current query params so a
// click on (say) "Wikipedia" while a search query is on the URL keeps
// the q=… intact. Drops view-local transient params that don't make
// sense in the new view (kept in sync with VIEW_LOCAL_PARAMS in
// switchView).
function _seaDiscoBuildViewHref(view) {
  let qs;
  try { qs = new URLSearchParams(location.search); } catch { qs = new URLSearchParams(); }
  ["tab", "li", "lp", "nocache"].forEach(k => qs.delete(k));
  if (view === "search") {
    qs.delete("v");
    const tail = qs.toString();
    return tail ? `/?${tail}` : "/";
  }
  qs.set("v", view);
  return `/?${qs.toString()}`;
}

// Walk every footer link tagged with data-sd-view and rewrite its href
// to reflect the CURRENT location.search. Click handlers already work
// correctly in SPA mode (switchView reads location.search fresh), but
// middle-click / right-click → copy-link / Open-in-new-tab use the
// href attribute directly — so we keep that attribute in sync with
// every history change.
function _updateFooterHrefs() {
  const footer = document.querySelector("footer");
  if (!footer) return;
  footer.querySelectorAll("a[data-sd-view]").forEach(a => {
    const v = a.dataset.sdView;
    if (v) a.href = _seaDiscoBuildViewHref(v);
  });
}
// Hook history.pushState / replaceState + popstate so any URL change
// from anywhere in the SPA propagates to the footer link hrefs. Patch
// is idempotent — only applied once per page load even if
// renderSharedFooter is called multiple times.
function _seaDiscoInstallFooterHrefSync() {
  if (window._sdFooterHrefSyncInstalled) return;
  window._sdFooterHrefSyncInstalled = true;
  const origPush    = history.pushState;
  const origReplace = history.replaceState;
  history.pushState = function (...args) {
    const r = origPush.apply(this, args);
    try { _updateFooterHrefs(); } catch {}
    return r;
  };
  history.replaceState = function (...args) {
    const r = origReplace.apply(this, args);
    try { _updateFooterHrefs(); } catch {}
    return r;
  };
  window.addEventListener("popstate", () => { try { _updateFooterHrefs(); } catch {} });
}

function renderSharedFooter(opts) {
  const isSPA = opts?.spa;
  // Hover hints for every footer link — describes what the destination
  // does so the labels (which are necessarily terse for the layout)
  // aren't the only signal.
  const HINTS = {
    search:     "Search Discogs releases, masters, artists, labels, AI",
    collection: "Your Discogs collection (synced)",
    wantlist:   "Your Discogs wantlist (synced)",
    favorites:  "Albums, artists, and labels you've favorited",
    inventory:  "Your seller listings on Discogs",
    lists:      "Your Discogs lists",
    loc:        "Search Library of Congress audio (free, public-domain)",
    wiki:       "Search Wikipedia in-app — save articles to read later",
    archive:    "Live concert recordings from archive.org",
    youtube:    "Search YouTube in-app — play in the mini-bar, save videos",
    account:    "Sign in / manage your account",
    info:       "About SeaDisco",
    privacy:    "Privacy policy",
    terms:      "Terms of service",
    // Home-strip tabs (footer column shortcut)
    recent:      "Albums you've recently viewed",
    suggestions: "Personalized suggestions from your taste profile",
    submitted:   "Albums you've contributed YouTube videos for",
    feed:        "Random sample from the catalog cache — explore freely",
  };
  // data-sd-view marks the link for the live href-sync system below.
  // `idAttr` is used for admin-only gated links so the post-/api/me
  // visibility flip can find them (see the wiki/loc/archive/youtube
  // reveal block further down).
  const link = (label, view, idAttr) => {
    const href = _seaDiscoBuildViewHref(view);
    const tip = HINTS[view] || "";
    const id = idAttr ? ` id="${idAttr}"` : "";
    if (isSPA) return `<a${id} href="${href}" data-sd-view="${view}" title="${escHtml(tip)}" onclick="event.preventDefault();switchView('${view}');return false">${label}</a>`;
    return `<a${id} href="${href}" data-sd-view="${view}" title="${escHtml(tip)}">${label}</a>`;
  };

  // Records-tab links: in SPA mode, route through switchView('records') with the
  // matching sub-tab. Outside the SPA, fall back to a query-string deep link.
  // When signed out, mirror the navbar record-tab behavior: pop the in-page
  // sign-in modal instead of trying to load a records view that requires auth.
  // `startEmpty` mirrors the navbar's same flag — Inventory and Lists ship
  // with nav-rec-empty so they're hidden until _updateEmptyRecordTabs flips
  // them on, avoiding the footer flash for users with no inventory/lists.
  const recLink = (label, tab, startEmpty) => {
    const href = _seaDiscoBuildViewHref(tab);
    const tip = HINTS[tab] || "";
    const cls = startEmpty ? ' class="nav-rec-empty"' : "";
    if (isSPA) {
      return `<a${cls} href="${href}" data-sd-view="${tab}" title="${escHtml(tip)}" onclick="event.preventDefault();if(!window._clerk?.user){openSignInModal();return false}_cwTab='${tab}';switchView('records');return false">${label}</a>`;
    }
    return `<a${cls} href="${href}" data-sd-view="${tab}" title="${escHtml(tip)}">${label}</a>`;
  };

  // Home-strip tab footer link — drops the user on the search/home
  // view with the named strip mode active. recent is the default so
  // it omits the ?strip= param; the rest deep-link via ?strip=<mode>
  // (the search page picks that up on load via _sdInitialHomeStripMode).
  // SPA path uses the in-page switcher when the search view is
  // already mounted — no full reload, no flash.
  const stripLink = (label, mode) => {
    const tip = HINTS[mode] || "";
    const href = mode === "recent" ? "/" : `/?strip=${mode}`;
    if (isSPA) {
      return `<a href="${href}" title="${escHtml(tip)}" onclick="event.preventDefault();switchView('search');setTimeout(()=>{if(typeof _sdSwitchHomeStripTab==='function')_sdSwitchHomeStripTab('${mode}');},0);return false">${label}</a>`;
    }
    return `<a href="${href}" title="${escHtml(tip)}">${label}</a>`;
  };

  const footer = document.querySelector("footer");
  if (!footer) return;
  footer.innerHTML = `
    <div class="footer-grid">
      <div class="footer-col">
        ${link("Search", "search")}
        ${recLink("Collection", "collection")}
        ${recLink("Wantlist", "wantlist")}
        ${recLink("Favorites", "favorites")}
        ${recLink("Inventory", "inventory", true)}
        ${recLink("Lists", "lists", true)}
      </div>
      <div class="footer-col">
        ${stripLink("Recent",      "recent")}
        ${stripLink("Suggestions", "suggestions")}
        ${(window._isAdmin || window._sdIsDemo) ? stripLink("Submitted", "submitted") : ""}
        ${stripLink("Feed",        "feed")}
      </div>
      <div class="footer-col">
        ${link("LOC",       "loc")}
        ${link("Wikipedia", "wiki")}
        ${link("Archive",   "archive")}
        <a id="footer-youtube-link" href="${_seaDiscoBuildViewHref("youtube")}" data-sd-view="youtube" title="${escHtml(HINTS.youtube)}" style="display:none"${isSPA ? ` onclick="event.preventDefault();switchView('youtube');return false"` : ""}>YouTube</a>
      </div>
      <div class="footer-col">
        ${isSPA
          ? `<a href="${_seaDiscoBuildViewHref("account")}" data-sd-view="account" title="${escHtml(HINTS.account)}" onclick="event.preventDefault();openSignInModal();return false;">Account</a>`
          : `<a href="${_seaDiscoBuildViewHref("account")}" data-sd-view="account" title="${escHtml(HINTS.account)}">Account</a>`}
        ${link("Info", "info")}
        ${link("Privacy Policy", "privacy")}
        ${link("Terms of Service", "terms")}
        <a id="footer-admin-link" href="/admin" title="Admin dashboard" style="display:none">Admin</a>
      </div>
    </div>
    <div style="color:#555;font-style:italic;margin-bottom:0.3rem">DISCLAIMER: AI be funky sometimes</div>
    <div><a href="#" onclick="_seaDiscoOpenJimmy(event);return false;" style="color:inherit;text-decoration:none;cursor:pointer" title="Jimmy Witherfork">Jimmy Witherfork Strikes Again</a></div>
    <div style="margin-top:0.3rem">&copy; 2026 SeaDisco</div>`;

  // Wire the live href-sync system so footer link hrefs always reflect
  // the current location.search. Idempotent — only patches history once.
  _seaDiscoInstallFooterHrefSync();

  // Easter-egg popup for the footer's "Jimmy Witherfork Strikes Again"
  // line — opens a small overlay with a single link to SlantFinder.pro.
  if (typeof window._seaDiscoOpenJimmy !== "function") {
    window._seaDiscoOpenJimmy = function (ev) {
      if (ev) { ev.preventDefault?.(); ev.stopPropagation?.(); }
      // Toggle: clicking again closes the popup.
      const existing = document.getElementById("sd-jimmy-popup");
      if (existing) { existing.remove(); return; }
      const el = document.createElement("div");
      el.id = "sd-jimmy-popup";
      el.innerHTML = `
        <a href="https://slantfinder.pro" target="_blank" rel="noopener" id="sd-jimmy-link">SlantFinder.pro ↗</a>
      `;
      document.body.appendChild(el);
      // Position near the click; clamp to viewport.
      const popupW = 200, popupH = 44;
      const x = (ev?.clientX ?? window.innerWidth / 2) - popupW / 2;
      const y = (ev?.clientY ?? window.innerHeight - 60) - popupH - 8;
      el.style.left = `${Math.min(window.innerWidth  - popupW - 8, Math.max(8, x))}px`;
      el.style.top  = `${Math.min(window.innerHeight - popupH - 8, Math.max(8, y))}px`;
      // Dismiss on outside click (deferred so the originating click
      // doesn't immediately close the popup we just opened).
      setTimeout(() => {
        const handler = (e) => {
          if (!document.getElementById("sd-jimmy-popup")) return;
          if (e.target.closest("#sd-jimmy-popup")) return;
          el.remove();
          document.removeEventListener("mousedown", handler, true);
        };
        document.addEventListener("mousedown", handler, true);
      }, 0);
    };
  }
  _updateFooterHrefs();

  // Reveal admin-only footer links (Admin + LOC) when /api/me confirms the
  // current Clerk session is the admin user. /api/me returns { signedIn,
  // isAdmin } based on the server-side ADMIN_CLERK_ID env var, so it's not
  // spoofable from the client. Failures are silent (links stay hidden).
  (async () => {
    try {
      // Wait for Clerk so apiFetch can attach the bearer token. loadClerkInstance
      // is idempotent and returns the cached instance after first call.
      const c = await loadClerkInstance();
      // Wikipedia and LOC footer links stay hidden for non-admins —
      // both surfaces (search, lookup, saves, and the icon affordances
      // sprinkled through modals/cards) are admin-only. The /api/me
      // probe below reveals them only when isAdmin is true.
      const res = await apiFetch("/api/me");
      if (!res.ok) return;
      const data = await res.json();
      // _serverIsAdmin reflects the actual server-confirmed admin status;
      // _isAdmin is the EFFECTIVE flag every consumer reads — which we
      // override to false when the admin has opted into "view as user"
      // mode via /admin. The footer Admin link still uses _serverIsAdmin
      // so the admin can always reach /admin to flip the toggle back.
      window._serverIsAdmin = !!data?.isAdmin;
      let viewAsUser = false;
      try { viewAsUser = localStorage.getItem("sd-admin-as-user") === "1"; } catch {}
      window._adminViewAsUser = viewAsUser && window._serverIsAdmin;
      window._isAdmin = window._serverIsAdmin && !viewAsUser;
      // Per-account demo allowlist (DEMO_CLERK_IDS env-var) — the
      // Google API quota reviewer's account gets YT-feature access
      // without being admin. Mutations stay gated on _isAdmin.
      window._sdIsDemo = !!data?.isDemo;
      // Broad YT_OPEN_TO_USERS toggle — off by default. Off means
      // signed-in non-admin/demo users see the standard splash.
      window._sdYtOpen = !!data?.ytOpen;
      if (window._serverIsAdmin) {
        const adminA = document.getElementById("footer-admin-link");
        if (adminA) adminA.style.display = "";
        // When viewing-as-user, drop a small fixed chip in the corner
        // so the admin can see they're impersonating + restore with
        // one click. Hidden in any other state. Only injected once.
        if (window._adminViewAsUser && !document.getElementById("admin-as-user-chip")) {
          const chip = document.createElement("button");
          chip.id = "admin-as-user-chip";
          chip.type = "button";
          chip.title = "Restore admin view";
          chip.textContent = "Viewing as user · restore";
          chip.onclick = () => {
            try { localStorage.removeItem("sd-admin-as-user"); } catch {}
            location.reload();
          };
          document.body.appendChild(chip);
        }
      }
      if (window._isAdmin) {
        const wikiA = document.getElementById("footer-wiki-link");
        if (wikiA) wikiA.style.display = "";
        const locA = document.getElementById("footer-loc-link");
        if (locA) locA.style.display = "";
        const archA = document.getElementById("footer-archive-link");
        if (archA) archA.style.display = "";
      }
      // YouTube footer link reveals for admin, demo accounts (per-
      // user DEMO_CLERK_IDS allowlist), or when the broad
      // YT_OPEN_TO_USERS env-var is set. Default: admin-only.
      if (typeof window._sdHasYtAccess === "function" ? window._sdHasYtAccess() : window._isAdmin) {
        const ytA = document.getElementById("footer-youtube-link");
        if (ytA) ytA.style.display = "";
      }
      if (window._isAdmin) {
        // Pre-load the discogs_ids AND names already in the
        // blues_artists table so the admin "+ add to Blues DB" icon
        // (popup AND card) can hide itself for artists already in.
        // Cards only know the artist name (parsed from result title),
        // so we cache names alongside ids for that lookup path.
        try {
          const idsRes = await apiFetch("/api/admin/blues/ids");
          if (idsRes.ok) {
            const j = await idsRes.json();
            window._adminBluesIds   = new Set((j.ids   ?? []).map(Number));
            window._adminBluesNames = new Set((j.names ?? []).map(s => String(s).trim().toLowerCase()));
          }
        } catch { /* non-fatal */ }
      }
    } catch { /* hidden by default — fine */ }
  })();
}

// ── Unified entity-lookup popup ──────────────────────────────────────────
// One small floating menu replaces the cluster of W / 🏛 / 📺 icons that
// used to hang next to every track-title or artist-name link. The text
// itself is now the trigger: click it, pick a search target. Play (▶)
// and Queue (➕) stay as separate inline icons because they're actions,
// not lookups, and burying them behind a click would slow common use.
//
// Public API:
//   entityLookupLinkHtml(scope, label, opts)  — inline anchor markup
//   openLookupPopup(ev, scope, label, ctx)    — programmatic open
//   _handleLookupClick(el, ev)                — event delegate for anchors
//
// scope: "track" | "artist"
// ctx:   { trackArtist?: string }   (for tracks, used to scope YT / LOC)

let _lookupPopupEl = null;
let _lookupOutsideHandler = null;

function _closeLookupPopup() {
  if (_lookupPopupEl) { _lookupPopupEl.remove(); _lookupPopupEl = null; }
  if (_lookupOutsideHandler) {
    document.removeEventListener("mousedown", _lookupOutsideHandler, true);
    _lookupOutsideHandler = null;
  }
}

// Build the anchor HTML for a clickable entity-text link. Embeds scope
// + label (+ optional trackArtist) as data-* so a single global click
// handler can reconstruct the popup without each render site having to
// emit a custom inline JS literal.
function entityLookupLinkHtml(scope, label, opts = {}) {
  if (!label) return "";
  const safeLabel  = escHtml(label);
  const artistAttr = opts.trackArtist ? ` data-lk-artist="${escHtml(opts.trackArtist)}"` : "";
  const titleAttr  = opts.title       ? ` title="${escHtml(opts.title)}"`              : "";
  const cls = ["entity-lookup-link", opts.className || ""].filter(Boolean).join(" ");
  // <span> not <a> — these markups can be embedded inside card <a>
  // wrappers, and HTML5 auto-closes the outer anchor when a nested
  // anchor is encountered, which broke wide-card layout (each card
  // split across multiple grid cells). Click semantics are unchanged
  // because the onclick handler does the work.
  return `<span class="${cls}" data-lk-scope="${escHtml(scope)}" data-lk-label="${safeLabel}"${artistAttr} role="button" tabindex="0" onclick="event.preventDefault();event.stopPropagation();_handleLookupClick(this,event);return false"${titleAttr}>${safeLabel}</span>`;
}

function _handleLookupClick(el, ev) {
  const scope = el.dataset.lkScope || "track";
  const label = el.dataset.lkLabel || "";
  const ctx   = { trackArtist: el.dataset.lkArtist || "" };
  openLookupPopup(ev, scope, label, ctx);
}

// "Search SeaDisco" handler — drops the user on the main search page
// with the right field populated for the scope:
//   track   → main query field (free-text)
//   artist  → f-artist field, advanced panel open
//   release → main query field
//   label   → f-label field, advanced panel open
function _lookupSearchSeaDisco(scope, label) {
  if (typeof closeModal === "function") { try { closeModal(); } catch {} }
  if (typeof _locCloseInfoPopup === "function") { try { _locCloseInfoPopup(); } catch {} }
  if (typeof clearForm === "function") { try { clearForm(); } catch {} }
  if (typeof switchView === "function") { try { switchView("search"); } catch {} }
  setTimeout(() => {
    if (scope === "artist") {
      const el = document.getElementById("f-artist");
      if (el) el.value = label;
      if (typeof toggleAdvanced === "function") { try { toggleAdvanced(true); } catch {} }
    } else if (scope === "label") {
      const el = document.getElementById("f-label");
      if (el) el.value = label;
      if (typeof toggleAdvanced === "function") { try { toggleAdvanced(true); } catch {} }
    } else {
      // track / release / catno / unknown — generic free-text query
      const qEl = document.getElementById("query");
      if (qEl) qEl.value = label;
    }
    // Default every popup-driven SeaDisco search to Masters+ result
    // type and oldest-first ordering — surfaces the canonical groupings
    // and the earliest / original pressings at the top, which is
    // almost always what the user wants when they jump from a credit
    // / track / artist / label / catno link.
    const masterPlusRadio = document.querySelector('input[name="result-type"][value="master+"]');
    if (masterPlusRadio) masterPlusRadio.checked = true;
    const sortEl = document.getElementById("f-sort");
    if (sortEl) sortEl.value = "year:asc";
    if (typeof doSearch === "function") doSearch(1);
  }, 30);
}

// Render and position the popup. Wikipedia and LOC are now open to
// anonymous callers (per-IP rate-limited server-side), so all buttons
// show for everyone. Buttons are ordered:
//   1. In-app actions (SeaDisco / collection / Wikipedia / LOC)
//   2. Visual separator
//   3. External links (YouTube, Discogs.com) with a ↗ indicator
function openLookupPopup(ev, scope, label, ctx) {
  _closeLookupPopup();
  if (!label) return;
  const trackArtist = ctx?.trackArtist || "";

  // Build the YouTube search query — scope-aware for disambiguation.
  // We send users to the in-app YouTube view (results play in the
  // mini-player, can be queued, can be ★-saved) rather than the
  // external youtube.com/results page.
  const ytQ = scope === "track" && trackArtist
    ? `"${trackArtist}" "${label}"`
    : `"${label}"`;
  const ytInAppHref = `/?v=youtube&yq=${encodeURIComponent(ytQ)}`;
  // Discogs.com fallback link.
  const dcQ = scope === "artist" || scope === "label"
    ? label
    : (trackArtist ? `${trackArtist} ${label}` : label);
  const dcUrl = `https://www.discogs.com/search?q=${encodeURIComponent(dcQ)}&type=all`;

  // In-app group — primary actions first (search across our integrated
  // sources), then "Copy to clipboard" as a quiet fallback at the bottom.
  // Copy used to be at the top; demoted because it's the least-clicked
  // option in everyday use and the visual primary slot is better spent
  // on the most-used SeaDisco / Wikipedia / YouTube entries.
  const internal = [];
  // SeaDisco / collection use the same line-art SVGs as the navbar so
  // the popup feels like an extension of the nav. Wrapped in a span
  // tagged `lookup-popup-icon-svg` so CSS forces white stroke (the
  // navbar tints them per-tab; here we want a plain white icon).
  internal.push({ key: "sd",   icon: `<span class="lookup-popup-icon-svg">${_SD_NAV_ICONS.search || "🔎"}</span>`, text: "Search SeaDisco" });
  // "Search my collection" only for scopes that can actually be saved
  // to a Discogs collection (releases only — labels and tracks can't
  // be saved as entities, just searched against). Labels are dropped
  // entirely per request; track/release/artist still get the option.
  if (scope !== "label" && scope !== "catno") {
    internal.push({ key: "coll", icon: `<span class="lookup-popup-icon-svg">${_SD_NAV_ICONS.collection || "⌕"}</span>`, text:
      scope === "artist"  ? "Search my collection" :
                            "Search my records" });
  }
  // Wikipedia / LOC don't make sense for catalog numbers.
  if (scope !== "catno") {
    internal.push({ key: "wiki", icon: "W",  text: "Wikipedia" });
  }
  // LOC for tracks / artists only — release / label / catno scopes
  // don't map cleanly to LOC's catalog model.
  if (scope === "track" || scope === "artist") {
    internal.push({ key: "loc", icon: "🏛", text: "Library of Congress" });
  }
  // Archive.org for tracks / artists / releases. Catno / label scopes
  // skip — Archive's index isn't well-suited to those.
  if (scope === "track" || scope === "artist" || scope === "release") {
    internal.push({ key: "archive", icon: "📼", text: "Archive.org" });
  }

  // YouTube is now an IN-APP search — popup overlay so users don't
  // lose context when initiated from an album / version modal.
  if (scope !== "catno") {
    internal.push({ key: "ytapp", icon: "▶", text: "YouTube", _ytQ: ytQ });
  }
  // Copy to clipboard sits last — universal across every scope.
  internal.push({ key: "copy", icon: "⎘", text: "Copy to clipboard" });
  // External Discogs.com link removed — internal SeaDisco search
  // covers the same ground without leaving the site.
  const external = [];

  // Combine for index addressing of action buttons (keeps indices
  // stable so the click delegate can resolve any clicked button).
  const buttons = [...internal, ...external];

  const wrap = document.createElement("div");
  wrap.className = "lookup-popup";
  wrap.innerHTML = `
    <div class="lookup-popup-head" title="${escHtml(label)}">${escHtml(label)}</div>
    <div class="lookup-popup-list">
      ${internal.map((b, i) => `<button type="button" class="lookup-popup-btn" data-i="${i}"><span class="lookup-popup-icon">${b.icon}</span>${escHtml(b.text)}</button>`).join("")}
      ${external.length ? `<div class="lookup-popup-sep" aria-hidden="true"></div>` : ""}
      ${external.map((b, idx) => {
        const i = internal.length + idx;
        return `<a href="${escHtml(b.url)}" target="_blank" rel="noopener" class="lookup-popup-btn lookup-popup-external" data-i="${i}"><span class="lookup-popup-icon">${b.icon}</span>${escHtml(b.text)}<span class="lookup-popup-ext-indicator" aria-hidden="true">↗</span></a>`;
      }).join("")}
    </div>
  `;
  document.body.appendChild(wrap);
  _lookupPopupEl = wrap;

  // Position near the click; clamp so the popup stays in-viewport.
  const popupW = 220;
  const popupH = 32 * buttons.length + 36;
  const x = (ev?.clientX ?? window.innerWidth / 2);
  const y = (ev?.clientY ?? window.innerHeight / 2);
  const left = Math.min(window.innerWidth  - popupW - 8, Math.max(8, x));
  const top  = Math.min(window.innerHeight - popupH - 8, Math.max(8, y + 6));
  wrap.style.position = "fixed";
  wrap.style.left = `${left}px`;
  wrap.style.top  = `${top}px`;
  wrap.style.width = `${popupW}px`;

  // Wire button actions
  wrap.querySelectorAll(".lookup-popup-btn").forEach(el => {
    const i = +el.dataset.i;
    const b = buttons[i];
    if (b.url) {
      // Plain anchor — let it navigate; just dismiss the popup after.
      el.addEventListener("click", () => setTimeout(_closeLookupPopup, 30));
      return;
    }
    el.addEventListener("click", e => {
      e.preventDefault();
      _closeLookupPopup();
      try {
        if (b.key === "copy") {
          // Universal copy-to-clipboard. Always copy ONLY the literal
          // `label` (the entity text the popup was opened for) — never
          // grab DOM textContent or any neighboring text. Reports of
          // copy pulling in tracklists / artist context were caused
          // by the page's text selection state leaking through some
          // browsers' clipboard write path; defensively snapshot the
          // string and trim whitespace before writing so what lands
          // in the user's clipboard is exactly what's shown in the
          // popup's heading. Shows the snippet in the toast for
          // visibility.
          const toCopy = String(label || "").trim();
          const tryToast = (msg, type) => {
            if (typeof showToast === "function") showToast(msg, type);
          };
          (async () => {
            try {
              if (navigator.clipboard?.writeText) {
                await navigator.clipboard.writeText(toCopy);
              } else {
                const ta = document.createElement("textarea");
                ta.value = toCopy;
                ta.style.position = "fixed";
                ta.style.left = "-1000px";
                document.body.appendChild(ta);
                ta.select();
                document.execCommand("copy");
                ta.remove();
              }
              const preview = toCopy.length > 40 ? toCopy.slice(0, 40) + "…" : toCopy;
              tryToast(`Copied: ${preview}`);
            } catch {
              tryToast("Could not copy", "error");
            }
          })();
        }
        else if (b.key === "sd")    _lookupSearchSeaDisco(scope, label);
        else if (b.key === "coll") {
          if (typeof searchCollectionFor === "function") {
            const cwField =
              scope === "artist"  ? "cw-artist"  :
              scope === "release" ? "cw-release" :
                                    "cw-query";
            searchCollectionFor(cwField, label);
          }
        }
        else if (b.key === "ytapp") {
          // Open the YouTube search results in a popup overlay so the
          // album / version modal underneath stays open. Standalone
          // searches (footer link or "Full page ↗" inside the popup)
          // still go to /?v=youtube.
          //
          // Force-load youtube.js if it hasn't been pulled yet — the
          // global window.openYoutubePopup might still be the lazy
          // stub at click time, which would re-dispatch correctly but
          // adds a roundtrip. Explicit load + check is more robust
          // and lets us tell the user something useful if the load
          // genuinely fails.
          const q = b._ytQ || label;
          console.debug("[ytapp click]", { q, hasLoader: typeof window._sdLoadModule === "function", isStub: window.openYoutubePopup === window._sdStub_openYoutubePopup });
          const tryOpen = () => {
            const fn = window.openYoutubePopup;
            if (typeof fn === "function" && fn !== window._sdStub_openYoutubePopup) {
              fn(q);
              return true;
            }
            return false;
          };
          if (tryOpen()) return;
          if (typeof window._sdLoadModule === "function") {
            window._sdLoadModule("/youtube.js")
              .then(() => {
                if (!tryOpen()) {
                  console.warn("[ytapp] youtube.js loaded but openYoutubePopup still missing");
                  if (typeof showToast === "function") showToast("Couldn't open YouTube search", "error");
                }
              })
              .catch(err => {
                console.warn("[ytapp] youtube.js load failed:", err);
                if (typeof showToast === "function") showToast("Couldn't load YouTube view", "error");
              });
          } else if (typeof switchView === "function") {
            // Fallback if the lazy loader itself isn't around.
            try { switchView("youtube"); } catch {}
            setTimeout(() => {
              const qInput = document.getElementById("youtube-view-q");
              if (qInput) qInput.value = q;
              if (typeof runYoutubeSearch === "function") runYoutubeSearch(q);
            }, 30);
          }
        }
        else if (b.key === "wiki") {
          // Quote phrase for exact-match Wikipedia search. Append a
          // hint term so the right kind of article surfaces:
          //   track   → "X" song
          //   release → "X" album
          //   label   → "X" record label
          //   artist  → "X" (no hint — Wikipedia's own ranking handles it)
          const hint =
            scope === "track"   ? "song" :
            scope === "release" ? "album" :
            scope === "label"   ? "record label" :
                                  "";
          const q = hint ? `"${label}" ${hint}` : `"${label}"`;
          if (typeof openWikiPopup === "function") openWikiPopup(q);
        }
        else if (b.key === "loc") {
          // Open in a popup overlay so the album / version modal
          // underneath stays put. Track scope quotes both track +
          // artist for an exact-phrase combo; broader scopes just
          // quote the label.
          const locQ = scope === "track" && trackArtist
            ? `"${label}" "${trackArtist}"`
            : `"${label}"`;
          if (typeof openLocPopup === "function") openLocPopup(locQ);
        }
        else if (b.key === "archive") {
          // Same popup-overlay pattern. archive.org's search supports
          // the same exact-phrase quoted-string convention LOC does;
          // wrap in quotes so cross-tagged unrelated items don't
          // dominate the result list.
          const archiveQ = scope === "track" && trackArtist
            ? `"${label}" "${trackArtist}"`
            : `"${label}"`;
          if (typeof openArchivePopup === "function") openArchivePopup(archiveQ);
        }
      } catch (err) { console.error("lookup action failed:", err); }
    });
  });

  // Dismiss on outside click (deferred so the originating click doesn't
  // immediately close the popup we just opened).
  setTimeout(() => {
    _lookupOutsideHandler = (e) => {
      if (!_lookupPopupEl) return;
      if (_lookupPopupEl.contains(e.target)) return;
      _closeLookupPopup();
    };
    document.addEventListener("mousedown", _lookupOutsideHandler, true);
  }, 0);
}

// Expose globals
window.entityLookupLinkHtml = entityLookupLinkHtml;
window.openLookupPopup      = openLookupPopup;
window._handleLookupClick   = _handleLookupClick;
window._closeLookupPopup    = _closeLookupPopup;
