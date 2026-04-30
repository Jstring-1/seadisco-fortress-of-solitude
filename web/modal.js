// ── Modal ─────────────────────────────────────────────────────────────────
// Guard SPA-only functions so modal.js works on account/admin pages too
if (typeof selectAltArtist === "undefined") window.selectAltArtist = () => {};
if (typeof searchArtistFromModal === "undefined") window.searchArtistFromModal = () => {};
if (typeof toggleAdvanced === "undefined") window.toggleAdvanced = () => {};
if (typeof doSearch === "undefined") window.doSearch = () => {};
if (typeof switchView === "undefined") window.switchView = () => {};
if (typeof doCwSearch === "undefined") window.doCwSearch = () => {};
if (typeof toggleCwAdvanced === "undefined") window.toggleCwAdvanced = () => {};

// Search user's collection from modal — navigates to My Records with a filter
// Uses a pending search mechanism: switchRecordsTab clears filters when
// a pending search is present, then applies the requested field value.
function searchCollectionFor(field, value) {
  closeModal();
  window._pendingCwSearch = { field, value };
  if (typeof switchView === "function") switchView("records");
}
// ── Visited cards — dim releases the user has already opened ────────────
const _visitedKey = "sd_visited";
let _visited = new Set(getStorageJSON(_visitedKey, []));
function _markVisited(id) {
  const key = String(id);
  if (_visited.has(key)) return;
  _visited.add(key);
  // Keep set bounded to last 500
  if (_visited.size > 500) {
    const arr = [..._visited];
    _visited = new Set(arr.slice(arr.length - 500));
  }
  localStorage.setItem(_visitedKey, JSON.stringify([..._visited]));
  // Mark all cards and version links for this ID
  document.querySelectorAll(`.card[onclick*="'${key}'"]`).forEach(el => el.classList.add("card-visited"));
  document.querySelectorAll(`.catno-link[onclick*="${key}"]`).forEach(el => el.classList.add("link-visited"));
}

// ── Recent history — richer record of opened releases for the Recent feed ──
//
// Storage strategy: localStorage is a write-through cache for instant
// render. Every modal open also fires a fire-and-forget POST to
// /api/user/recent so the same list is available on another device (see
// _hydrateHistoryFromServer() in search.js, which runs once per page load).
const _historyKey = "sd_history";
const _HISTORY_MAX = 576;
function _recordHistory(id, type) {
  if (!id || !type) return;
  // Don't record history for anon users — they should always see the
  // "Suggested" strip (admin's curated favorites), not their own
  // browse trail. The trail accumulates in localStorage and would
  // otherwise replace Suggested on the home page.
  if (!window._clerk?.user) return;
  let item;
  try {
    const entry = (typeof itemCache !== "undefined" ? itemCache.get(String(id)) : null);
    // Store a compact card-shaped snapshot so the Recent feed can render
    // without re-fetching. Fall back to minimal data if not in cache.
    item = entry ? {
      id,
      type,
      title: entry.title || "",
      cover_image: entry.cover_image || entry.thumb || "",
      uri: entry.uri || `/${type}/${id}`,
      label: entry.label ?? [],
      format: entry.format ?? [],
      genre: entry.genre ?? [],
      year: entry.year || "",
      country: entry.country || "",
      catno: entry.catno || "",
    } : { id, type, title: "", cover_image: "", uri: `/${type}/${id}` };
    const raw = localStorage.getItem(_historyKey);
    let hist = raw ? JSON.parse(raw) : [];
    // Remove any existing entry for this id (bubble to front)
    hist = hist.filter(h => String(h.id) !== String(id));
    hist.unshift({ ...item, _openedAt: Date.now() });
    if (hist.length > _HISTORY_MAX) hist = hist.slice(0, _HISTORY_MAX);
    localStorage.setItem(_historyKey, JSON.stringify(hist));
    // Notify any listeners (e.g. the Recent strip on the search page)
    window.dispatchEvent(new CustomEvent("sd-history-change"));
  } catch { /* storage quota or parse error — silently ignore */ }

  // Mirror to server for cross-device sync. Fire-and-forget: any error is
  // swallowed because the local cache already succeeded.
  if (item && window._clerk?.user && typeof apiFetch === "function") {
    try {
      apiFetch("/api/user/recent", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id, type, data: item }),
      }).catch(() => {});
    } catch {}
  }
}
/** Apply visited state to all currently rendered cards and version links */
function applyVisitedCards() {
  if (!_visited.size) return;
  document.querySelectorAll(".card[onclick]").forEach(el => {
    const m = el.getAttribute("onclick")?.match(/openModal\(event,'(\d+)'/);
    if (m && _visited.has(m[1])) el.classList.add("card-visited");
  });
  document.querySelectorAll(".catno-link[onclick]").forEach(el => {
    const m = el.getAttribute("onclick")?.match(/openVersionPopup\(event,(\d+)\)/);
    if (m && _visited.has(m[1])) el.classList.add("link-visited");
  });
}

// ── Version-list dot toggles (collection / wantlist / favorites) ──────────
async function mvToggleCol(dot, id) {
  const was = window._collectionIds?.has(id);
  if (!window._collectionIds) window._collectionIds = new Set();
  if (!window._clerk?.user) { showToast("Sign in to manage your collection", "error"); return; }
  if (was) window._collectionIds.delete(id); else window._collectionIds.add(id);
  dot.style.background = was ? "" : "#6ddf70";
  dot.title = was ? "Add to collection" : "In collection — click to remove";
  dot.classList.toggle("active", !was);
  refreshCardBadges?.(id);
  try {
    const r = await apiFetch(was ? "/api/user/collection/remove" : "/api/user/collection/add", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(was ? { releaseId: id, instanceId: null, folderId: 1 } : { releaseId: id }),
    }).then(r => r.json());
    if (!r.ok && r.error) throw new Error(r.error);
    showToast(was ? "Removed from collection" : "Added to collection");
    const modalBtn = document.getElementById("modal-col-btn");
    if (modalBtn) { modalBtn.classList.toggle("in-collection", !was); modalBtn.innerHTML = was ? "Collection" : "Collected"; }
  } catch (e) {
    if (was) window._collectionIds.add(id); else window._collectionIds.delete(id);
    dot.style.background = was ? "#6ddf70" : "";
    dot.title = was ? "In collection — click to remove" : "Add to collection";
    dot.classList.toggle("active", was);
    refreshCardBadges?.(id);
    showToast(e.message || "Failed to update collection", "error");
  }
}

async function mvToggleWant(dot, id) {
  const was = window._wantlistIds?.has(id);
  if (!window._wantlistIds) window._wantlistIds = new Set();
  if (!window._clerk?.user) { showToast("Sign in to manage your wantlist", "error"); return; }
  if (was) window._wantlistIds.delete(id); else window._wantlistIds.add(id);
  dot.style.background = was ? "" : "#f0c95c";
  dot.title = was ? "Add to wantlist" : "In wantlist — click to remove";
  dot.classList.toggle("active", !was);
  refreshCardBadges?.(id);
  try {
    const r = await apiFetch(was ? "/api/user/wantlist/remove" : "/api/user/wantlist/add", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ releaseId: id }),
    }).then(r => r.json());
    if (!r.ok && r.error) throw new Error(r.error);
    showToast(was ? "Removed from wantlist" : "Added to wantlist");
    const modalBtn = document.getElementById("modal-want-btn");
    if (modalBtn) { modalBtn.classList.toggle("in-wantlist", !was); modalBtn.innerHTML = was ? "Want" : "Wanted"; }
  } catch (e) {
    if (was) window._wantlistIds.add(id); else window._wantlistIds.delete(id);
    dot.style.background = was ? "#f0c95c" : "";
    dot.title = was ? "In wantlist — click to remove" : "Add to wantlist";
    dot.classList.toggle("active", was);
    refreshCardBadges?.(id);
    showToast(e.message || "Failed to update wantlist", "error");
  }
}

function mvToggleFav(dot, id) {
  const key = `release:${id}`;
  const was = window._favoriteKeys?.has(key);
  if (!window._favoriteKeys) window._favoriteKeys = new Set();
  if (was) window._favoriteKeys.delete(key); else window._favoriteKeys.add(key);
  dot.style.background = was ? "" : "#ff6b35";
  dot.title = was ? "Add to favorites" : "Favorited — click to remove";
  dot.classList.toggle("active", !was);
  refreshCardBadges?.(id);
  const endpoint = was ? "/api/user/favorites/remove" : "/api/user/favorites/add";
  const body = was
    ? { discogsId: id, entityType: "release" }
    : { discogsId: id, entityType: "release", data: { id, type: "release", title: "", uri: `/release/${id}` } };
  apiFetch(endpoint, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
    .then(r => { if (!r.ok) throw new Error(); showToast(was ? "Removed from favorites" : "Added to favorites"); })
    .catch(() => {
      if (was) window._favoriteKeys.add(key); else window._favoriteKeys.delete(key);
      dot.style.background = was ? "#ff6b35" : "";
      dot.title = was ? "Favorited — click to remove" : "Add to favorites";
      dot.classList.toggle("active", was);
      refreshCardBadges?.(id);
      showToast("Failed to update favorite", "error");
    });
}

function openModal(event, id, type, discogsUrl) {
  if (event) event.preventDefault();
  _markVisited(id);
  _recordHistory(id, type);
  const u = new URL(window.location.href);
  u.searchParams.set("op", `${type}:${id}`);
  history.replaceState({}, "", u.toString());
  const overlay = document.getElementById("modal-overlay");
  document.getElementById("album-info").innerHTML = "";
  document.getElementById("modal-loading").style.display = "block";
  overlay.classList.add("open");
  _sdLockBodyScroll("modal");

  const cachedItem = (typeof itemCache !== 'undefined' ? itemCache.get(String(id)) : null) ?? { type, id };
  const endpoint = type === "master" ? "master" : "release";
  Promise.all([
    apiFetch(`${API}/${endpoint}/${id}`).then(async r => {
      // Anon users get 401 on cache-miss for /release|/master. Don't
      // surface a partial-empty modal: hand back a synthesized record
      // built from cachedItem so renderAlbumInfo has SOMETHING to
      // render (cover, title, artist, year, format), plus a
      // _signInForMore flag so the modal can show a CTA instead of
      // letting fields silently fall to empty strings.
      if (!r.ok) {
        const reason = r.status === 401 ? "auth" : "error";
        return { _signInForMore: reason, _httpStatus: r.status, ...cachedItem };
      }
      return r.json();
    }),
    apiFetch(`${API}/marketplace-stats/${id}?type=${type}`).then(r => r.ok ? r.json() : null).catch(() => null),
  ])
    .then(([d, stats]) => {
      document.getElementById("modal-loading").style.display = "none";
      renderAlbumInfo(d, cachedItem, discogsUrl, stats);
    })
    .catch(() => {
      document.getElementById("modal-loading").textContent = "Failed to load.";
    });
}

function closeModal() {
  document.getElementById("modal-overlay").classList.remove("open");
  // Counter-based scroll lock — version/series/youtube/etc. popups
  // each hold their own lock and only the last release drops the
  // body.modal-open class. No more conditional checks needed here.
  _sdUnlockBodyScroll("modal");
  const u = new URL(window.location.href);
  u.searchParams.delete("op");
  history.replaceState({}, "", u.toString());
}

// ── Tracklist collapse/expand ──────────────────────────────────────────────
function toggleTracklist(headingEl) {
  const body = headingEl.closest(".tracklist-header").nextElementSibling;
  if (!body) return;
  const isOpen = body.style.display !== "none";
  body.style.display = isOpen ? "none" : "";
  const arrow = headingEl.querySelector(".tracklist-arrow");
  if (arrow) arrow.textContent = isOpen ? "▶" : "▼";
  localStorage.setItem("tracklist-open", isOpen ? "false" : "true");
}

function filterTracks(input) {
  const q = input.value.toLowerCase().trim();
  const body = input.closest(".tracklist-header").nextElementSibling;
  if (!body) return;
  // Ensure tracklist is expanded when filtering
  if (q && body.style.display === "none") {
    body.style.display = "";
    const arrow = input.closest(".tracklist-header").querySelector(".tracklist-arrow");
    if (arrow) arrow.textContent = "▼";
  }
  body.querySelectorAll(".track").forEach(row => {
    const title = row.querySelector(".track-title")?.textContent.toLowerCase() ?? "";
    row.style.display = !q || title.includes(q) ? "" : "none";
  });
}

function filterCredits(input) {
  const q = input.value.toLowerCase().trim();
  const body = input.closest(".credits-header").nextElementSibling;
  if (!body) return;
  body.querySelectorAll(".credit-item").forEach(item => {
    const text = item.textContent.toLowerCase();
    const show = !q || text.includes(q);
    item.style.display = show ? "" : "none";
    // Hide the separator after hidden items
    const sep = item.nextElementSibling;
    if (sep?.classList.contains("credit-sep")) sep.style.display = show ? "" : "none";
  });
  // Clean up leading separators (first visible item shouldn't have a sep before it)
  const items = body.querySelectorAll(".credit-item, .credit-sep");
  let needSep = false;
  items.forEach(el => {
    if (el.classList.contains("credit-item")) {
      if (el.style.display !== "none") needSep = true;
    } else if (el.classList.contains("credit-sep")) {
      // Show sep only between two visible items
      const next = el.nextElementSibling;
      el.style.display = (needSep && next?.classList.contains("credit-item") && next.style.display !== "none") ? "" : "none";
    }
  });
}

// ── Image lightbox / carousel ─────────────────────────────────────────────
let _lbImages = [], _lbIdx = 0;

function openLightbox(images, startIdx) {
  _lbImages = images;
  _lbIdx = startIdx ?? 0;
  _renderLightbox();
  document.getElementById("lightbox-overlay").classList.add("open");
  _sdLockBodyScroll("lightbox");
  document.addEventListener("keydown", _lbKey);
}

function closeLightbox() {
  document.getElementById("lightbox-overlay").classList.remove("open");
  _sdUnlockBodyScroll("lightbox");
  document.removeEventListener("keydown", _lbKey);
}

function lightboxStep(e, dir) {
  e.stopPropagation();
  _lbIdx = Math.max(0, Math.min(_lbImages.length - 1, _lbIdx + dir));
  _renderLightbox();
}

// ── Touch swipe for lightbox ──────────────────────────────────────────────
(function initLightboxSwipe() {
  const overlay = document.getElementById("lightbox-overlay");
  if (!overlay) return;
  let startX = 0, startY = 0, tracking = false;
  overlay.addEventListener("touchstart", e => {
    if (e.touches.length === 1) {
      startX = e.touches[0].clientX;
      startY = e.touches[0].clientY;
      tracking = true;
    }
  }, { passive: true });
  overlay.addEventListener("touchend", e => {
    if (!tracking) return;
    tracking = false;
    const dx = e.changedTouches[0].clientX - startX;
    const dy = e.changedTouches[0].clientY - startY;
    if (Math.abs(dx) < 40 || Math.abs(dy) > Math.abs(dx)) return; // too short or vertical
    if (dx < 0 && _lbIdx < _lbImages.length - 1) { _lbIdx++; _renderLightbox(); }
    if (dx > 0 && _lbIdx > 0) { _lbIdx--; _renderLightbox(); }
  }, { passive: true });
})();

function _lbKey(e) {
  if (e.key === "ArrowLeft")  { _lbIdx = Math.max(0, _lbIdx - 1); _renderLightbox(); }
  if (e.key === "ArrowRight") { _lbIdx = Math.min(_lbImages.length - 1, _lbIdx + 1); _renderLightbox(); }
  if (e.key === "Escape")     closeLightbox();
}

function _renderLightbox() {
  document.getElementById("lightbox-img").src = _lbImages[_lbIdx] ?? "";
  document.getElementById("lightbox-counter").textContent =
    _lbImages.length > 1 ? `${_lbIdx + 1} / ${_lbImages.length}` : "";
  document.getElementById("lightbox-prev").disabled = _lbIdx === 0;
  document.getElementById("lightbox-next").disabled = _lbIdx === _lbImages.length - 1;
  const single = _lbImages.length <= 1;
  document.getElementById("lightbox-prev").style.display = single ? "none" : "";
  document.getElementById("lightbox-next").style.display = single ? "none" : "";
}

(function () {
  const overlay = document.getElementById("modal-overlay");
  if (!overlay) return; // admin.html doesn't render this element
  overlay.addEventListener("click", e => {
    if (e.target === overlay) closeModal();
  });
})();

// ── Version popup ─────────────────────────────────────────────────────────
async function openVersionPopup(event, releaseId) {
  if (event) event.preventDefault();
  _markVisited(releaseId);
  const overlay = document.getElementById("version-overlay");
  const info    = document.getElementById("version-info");
  const loading = document.getElementById("version-loading");
  info.innerHTML = "";
  loading.style.display = "block";
  overlay.classList.add("open");
  _sdLockBodyScroll("version");
  const u = new URL(window.location.href);
  u.searchParams.set("vr", releaseId);
  history.replaceState({}, "", u.toString());
  try {
    const discogsUrl = `https://www.discogs.com/release/${releaseId}`;
    const [d, stats] = await Promise.all([
      apiFetch(`${API}/release/${releaseId}`).then(r => r.json()),
      apiFetch(`${API}/marketplace-stats/${releaseId}?type=release`).then(r => r.json()).catch(() => null),
    ]);
    loading.style.display = "none";
    const fakeResult = { type: "release", id: releaseId, title: d.title ?? "", cover_image: d.images?.[0]?.uri ?? "", format: [], country: d.country ?? "", year: d.year ?? "" };
    renderAlbumInfo(d, fakeResult, discogsUrl, stats, "version-info");
  } catch(e) {
    loading.style.display = "none";
    info.innerHTML = `<div style="padding:1rem;color:var(--muted)">Failed to load release details.</div>`;
  }
}

function closeVersionPopup() {
  document.getElementById("version-overlay").classList.remove("open");
  _sdUnlockBodyScroll("version");
  const u = new URL(window.location.href);
  u.searchParams.delete("vr");
  history.replaceState({}, "", u.toString());
}

// admin.html loads modal.js too but doesn't have the version-overlay
// element. Guard the listener attach so the missing element doesn't
// throw a TypeError at module load and abort the rest of modal.js.
(function () {
  const overlay = document.getElementById("version-overlay");
  if (!overlay) return;
  overlay.addEventListener("click", e => {
    if (e.target === overlay) closeVersionPopup();
  });
})();

// ── Bio full popup ────────────────────────────────────────────────────────
function openBioFull(event) {
  if (event) event.preventDefault();
  const u = new URL(window.location.href);
  u.searchParams.set("bi", "1");
  history.replaceState({}, "", u.toString());
  const {
    name, text, discogsId = null,
    members = [], groups = [], aliases = [],
    namevariations = [], urls = [],
    parentLabel = null, sublabels = [],
    alternatives = [],
  } = window._currentBio ?? {};
  document.getElementById("bio-full-name").textContent = name ?? "";
  let html = renderBioMarkup(text ?? "");
  if (alternatives.length > 0) {
    const altLinks = alternatives.map(a =>
      `<a href="#" class="bio-artist-link modal-internal-link" onclick="selectAltArtist(event,this);closeBioFull()" data-alt-name="${escHtml(a.name)}"${a.id ? ` data-alt-id="${a.id}"` : ""} title="Search for ${escHtml(a.name)}" style="color:var(--accent)">${escHtml(a.name)}</a>`
    ).join('<span style="color:#555;margin:0 0.3em">·</span>');
    html += `<div style="font-size:0.78rem;margin-top:0.7rem;line-height:1.6"><span style="color:#777;margin-right:0.4em">Also:</span>${altLinks}</div>`;
  }
  const relLinks = renderArtistRelations(members, groups, aliases, namevariations, urls, parentLabel, sublabels);
  if (relLinks) html += relLinks;
  if (discogsId) {
    html += `<div style="margin-top:1.1rem"><a href="https://www.discogs.com/artist/${discogsId}" target="_blank" rel="noopener" title="Open artist page on Discogs.com" style="font-size:0.75rem;color:#666;text-decoration:none">View profile on Discogs ↗</a></div>`;
  }
  document.getElementById("bio-full-text").innerHTML = html;
  document.getElementById("bio-full-overlay").classList.add("open");
}

function closeBioFull() {
  document.getElementById("bio-full-overlay").classList.remove("open");
  const u = new URL(window.location.href);
  u.searchParams.delete("bi");
  history.replaceState({}, "", u.toString());
}

(function () {
  const overlay = document.getElementById("bio-full-overlay");
  if (!overlay) return;
  overlay.addEventListener("click", e => {
    if (e.target === overlay) closeBioFull();
  });
})();

// ── Wikipedia popup ──────────────────────────────────────────────────────────
// Opens a stacked popup over any underlying modal/version popup. Music keeps
// playing. Closing returns to the underlying popup intact. In-article wiki
// links are rewritten so they open the same internal popup instead of leaving.
// Wiki popup is now SEARCH-FIRST. Every entry point (W icon, in-article
// wiki link, disambig list, manual call) lands on a list of matching
// articles; clicking a result loads that article in the SAME popup.
// This avoids fuzzy auto-jumps like "Bill Wax" → Bill Gates.
//
//   openWikiPopup(query)         — show search results for `query`
//   openWikiArticle(title, src)  — load that article inline; if `src` is
//                                  given, render a "← Back" button that
//                                  re-opens the search for `src`.
async function openWikiPopup(query) {
  const q = String(query || "").trim();
  if (!q) return;
  const overlay = document.getElementById("wiki-overlay");
  const loading = document.getElementById("wiki-loading");
  const content = document.getElementById("wiki-content");
  if (!overlay) return;
  overlay.classList.add("open");
  loading.style.display = "";
  content.innerHTML = "";
  // Reflect in URL so the popup is shareable. wk= holds the search query.
  try {
    const u = new URL(window.location.href);
    u.searchParams.set("wk", q);
    history.replaceState({}, "", u.toString());
  } catch {}
  try {
    await _renderWikiPopupSearch(q, content);
  } finally {
    loading.style.display = "none";
  }
}

// Strip operator double-quotes that the W icons add for exact-phrase
// search so the heading reads naturally even if the entity name itself
// contains quote marks (e.g. `'Baby Face' Willette`).
function _wikiHeadingDisplay(q) {
  let s = String(q || "").trim();
  if (s.length >= 2 && s.startsWith('"') && s.endsWith('"')) {
    s = s.slice(1, -1);
  }
  return s;
}

// Render the search-results list inside the wiki popup. Used by both
// openWikiPopup() and the "← Back to results" button on article view.
async function _renderWikiPopupSearch(q, contentEl) {
  const display = _wikiHeadingDisplay(q);
  contentEl.innerHTML = `
    <div class="wiki-header">
      <h2 style="margin:0 0 0.3rem 0">Wikipedia: ${escHtml(display)}</h2>
      <div class="wiki-popup-subnote">Click a title to open the article here.</div>
    </div>
    <div class="wiki-popup-results wiki-results-list"><div class="wiki-results-loading">Searching Wikipedia for <em>${escHtml(display)}</em>…</div></div>`;
  const listEl = contentEl.querySelector(".wiki-popup-results");
  try {
    const r = await apiFetch(`/api/wikipedia/search?q=${encodeURIComponent(q)}&limit=15&offset=0`);
    if (!r.ok) {
      listEl.innerHTML = `<div class="wiki-results-error">Wikipedia search failed.</div>`;
      return;
    }
    const data = await r.json();
    const rows = Array.isArray(data?.results) ? data.results : [];
    if (!rows.length) {
      listEl.innerHTML = `<div class="wiki-results-empty">No matches for <em>${escHtml(q)}</em>.</div>`;
      return;
    }
    const safeQ = String(q).replace(/'/g, "\\'");
    // Make sure ★ state is loaded so the saved buttons render correctly
    await _wikiLoadSavedIds();
    listEl.innerHTML = rows.map(rec => {
      const safeTitle = String(rec.title || "").replace(/'/g, "\\'");
      return `
        <div class="wiki-result">
          <div class="wiki-result-head">
            <a href="#" class="wiki-result-title" onclick="event.preventDefault();openWikiArticle('${escHtml(safeTitle)}','${escHtml(safeQ)}')" title="Open in popup">${escHtml(rec.title || "")}</a>
            ${_wikiSaveBtnHtml(rec.title || "")}
          </div>
          <div class="wiki-result-snippet">${_sanitizeWikiSnippet(rec.snippet || "")}…</div>
        </div>`;
    }).join("");
  } catch (err) {
    listEl.innerHTML = `<div class="wiki-results-error">Wikipedia search failed.</div>`;
  }
}

// Load a specific Wikipedia article into the popup. Internal article
// wiki links and disambig list rows call this directly (those are
// exact-title links, no need to bounce through search).
async function openWikiArticle(title, sourceQuery) {
  const t = String(title || "").trim();
  if (!t) return;
  const overlay = document.getElementById("wiki-overlay");
  const loading = document.getElementById("wiki-loading");
  const content = document.getElementById("wiki-content");
  if (!overlay) return;
  overlay.classList.add("open");
  loading.style.display = "";
  content.innerHTML = "";
  // Reflect in URL: wk = the article title (so a shared link opens the
  // popup with search results for that exact title — Wikipedia's first
  // hit will be that article, plus alternatives if disambiguation).
  try {
    const u = new URL(window.location.href);
    u.searchParams.set("wk", t);
    history.replaceState({}, "", u.toString());
  } catch {}
  try {
    const r = await apiFetch(`/api/wikipedia/lookup?q=${encodeURIComponent(t)}&full=1`);
    if (!r.ok) {
      content.innerHTML = `<div style="padding:1rem;color:var(--muted)">Wikipedia lookup failed.</div>`;
      return;
    }
    const data = await r.json();
    if (!data.found) {
      // Fall back to a fresh search for the title — same popup.
      await _renderWikiPopupSearch(t, content);
      return;
    }
    const thumb = data.thumbnail ? `<img src="${escHtml(data.thumbnail)}" alt="" style="float:right;max-width:140px;margin:0 0 0.5rem 1rem;border-radius:4px">` : "";
    const safeSrc = String(sourceQuery || "").replace(/'/g, "\\'");
    const backDisplay = _wikiHeadingDisplay(sourceQuery || "");
    const backBtn = sourceQuery
      ? `<button type="button" class="wiki-back-btn" onclick="openWikiPopup('${escHtml(safeSrc)}')">← Back to "${escHtml(backDisplay)}" results</button>`
      : "";
    // Render ★ next to the article heading so the user can save the
    // article they're currently reading without bouncing back to a list.
    await _wikiLoadSavedIds();
    const saveBtnHtml = _wikiSaveBtnHtml(data.title || "");
    content.innerHTML = `
      <div class="wiki-header">
        ${backBtn}
        <div class="wiki-article-title-row">
          <h2 style="margin:0.3rem 0 0.4rem 0">${escHtml(data.title)}</h2>
          ${saveBtnHtml}
        </div>
      </div>
      <div class="wiki-extract">${thumb}${data.html}</div>`;
    _applyWikiArticleRewrites(content, sourceQuery || data.title);
  } catch (err) {
    content.innerHTML = `<div style="padding:1rem;color:var(--muted)">Error: ${escHtml(err.message || String(err))}</div>`;
  } finally {
    loading.style.display = "none";
  }
}

// Apply blocked-section markers, internal-wiki-link rewrites, disambig
// list links, and bold→Discogs rewrites to a freshly rendered article.
// Extracted from openWikiPopup so openWikiArticle can reuse it.
function _applyWikiArticleRewrites(content, sourceQuery) {
  // Mark "External links" / "References" / "Notes" / "Further reading"
  // / "See also" / "Bibliography" / "Sources" / "Citations" sections so
  // the bold→Discogs and disambiguation rewriters skip them — those
  // sections are reference-style content, not article body.
  const _blockedHeadings = new Set([
    "external links", "references", "notes", "bibliography",
    "sources", "further reading", "see also", "citations", "footnotes",
  ]);
  content.querySelectorAll(".wiki-extract").forEach(extract => {
    let blocked = false;
    Array.from(extract.children).forEach(child => {
      if (/^H[1-6]$/.test(child.tagName)) {
        const txt = (child.textContent || "").trim().toLowerCase();
        const norm = txt.replace(/\s*\[edit\]\s*$/, "").trim();
        blocked = _blockedHeadings.has(norm);
        if (blocked) child.classList.add("wiki-no-rewrite");
      } else if (blocked) {
        child.classList.add("wiki-no-rewrite");
      }
    });
  });
  // Internal-wiki <a href> links — exact article titles, so jump straight
  // to that article (skip the search-results step).
  content.querySelectorAll(".wiki-extract a[href]").forEach(a => {
    const href = a.getAttribute("href") || "";
    let title = "";
    if (href.startsWith("/wiki/")) title = decodeURIComponent(href.slice(6).split("#")[0]);
    else if (/^https?:\/\/[^/]*wikipedia\.org\/wiki\//.test(href)) {
      title = decodeURIComponent(href.split("/wiki/")[1].split("#")[0]);
    }
    if (title) {
      const t = title.replace(/_/g, " ");
      a.setAttribute("href", "#");
      a.removeAttribute("target");
      a.addEventListener("click", (ev) => {
        ev.preventDefault();
        openWikiArticle(t, sourceQuery);
      });
    } else {
      a.setAttribute("target", "_blank");
      a.setAttribute("rel", "noopener");
    }
  });
  // Disambiguation list items — leading "Name (qualifier)" is an exact
  // article title, so go straight to the article.
  content.querySelectorAll(".wiki-extract li").forEach(li => {
    if (li.closest(".wiki-no-rewrite")) return;
    if (li.querySelector("a")) return;
    const raw = (li.textContent || "").trim();
    if (!raw || raw.length < 3) return;
    const nameMatch = raw.match(
      /^([A-Z][\w'\-.]*(?:\s+(?:[A-Z][\w'\-.]*|of|the|de|von|van|der|du|da|y|and|&)){0,6})/
    );
    if (!nameMatch) return;
    let prefix = nameMatch[1].trim();
    let consumed = nameMatch[0].length;
    const after = raw.slice(consumed);
    const qualMatch = after.match(/^\s*\(([^)]+)\)/);
    if (qualMatch) {
      const qual = qualMatch[1].trim();
      const isDate = /\b\d{3,4}\b/.test(qual);
      if (!isDate && qual.length <= 60) {
        prefix = `${prefix} (${qual})`;
        consumed += qualMatch[0].length;
      }
    }
    if (prefix.length < 3 || prefix.length > 80) return;
    const remainder = raw.slice(consumed);
    const a = document.createElement("a");
    a.href = "#";
    a.className = "wiki-disambig-link";
    a.textContent = prefix;
    a.title = `Open Wikipedia: ${prefix}`;
    a.addEventListener("click", (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      try { openWikiArticle(prefix, sourceQuery); } catch {}
    });
    li.textContent = "";
    li.appendChild(a);
    if (remainder) li.appendChild(document.createTextNode(remainder));
  });
  // Bold proper nouns → Discogs search (closes popup + runs main search).
  content.querySelectorAll(".wiki-extract b").forEach(b => {
    if (b.closest(".wiki-no-rewrite")) return;
    const text = (b.textContent || "").trim();
    if (!text || text.length < 2 || text.length > 80) return;
    const a = document.createElement("a");
    a.href = "#";
    a.className = "wiki-bold-search";
    a.title = `Search SeaDisco for "${text}"`;
    a.textContent = text;
    a.addEventListener("click", (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      try { closeWikiPopup(); } catch {}
      try {
        if (typeof switchView === "function") switchView("search", true);
        if (typeof clearForm === "function") clearForm();
        const qInput = document.getElementById("query");
        if (qInput) qInput.value = text;
        if (typeof doSearch === "function") doSearch(1);
      } catch {}
    });
    b.parentNode?.replaceChild(a, b);
  });
}

function closeWikiPopup() {
  const overlay = document.getElementById("wiki-overlay");
  if (overlay) overlay.classList.remove("open");
  try {
    const u = new URL(window.location.href);
    u.searchParams.delete("wk");
    history.replaceState({}, "", u.toString());
  } catch {}
}

document.getElementById("wiki-overlay")?.addEventListener("click", e => {
  if (e.target === document.getElementById("wiki-overlay")) closeWikiPopup();
});

// localStorage-backed recent-searches list for the wiki SPA page.
// Keeps last 8 unique queries (most recent first) and rehydrates the
// <datalist> so the input gets a native autocomplete dropdown — same
// idea as a browser history-style suggestion list.
const _WIKI_RECENT_KEY = "sd_wiki_recent";
const _WIKI_RECENT_MAX = 8;
function _readWikiRecents() {
  try {
    const raw = localStorage.getItem(_WIKI_RECENT_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.filter(s => typeof s === "string" && s.trim()) : [];
  } catch { return []; }
}
function _writeWikiRecents(arr) {
  try { localStorage.setItem(_WIKI_RECENT_KEY, JSON.stringify(arr.slice(0, _WIKI_RECENT_MAX))); } catch {}
}
function _renderWikiRecentDatalist() {
  const dl = document.getElementById("wiki-recent-list");
  if (!dl) return;
  const recents = _readWikiRecents();
  dl.innerHTML = recents.map(s => `<option value="${escHtml(s)}"></option>`).join("");
}
function _pushWikiRecent(q) {
  const trimmed = String(q || "").trim();
  if (!trimmed) return;
  const cur = _readWikiRecents().filter(s => s.toLowerCase() !== trimmed.toLowerCase());
  cur.unshift(trimmed);
  _writeWikiRecents(cur);
  _renderWikiRecentDatalist();
}

// Wikipedia SPA page (/?v=wiki) — list-style search. Renders matching
// articles with snippets right on the page; clicking a row opens that
// specific article in the popup, where in-article links work as usual.
// 20 rows per page; "Load more" appends the next page in place.
const _WIKI_PAGE_SIZE = 20;

function _wikiResultRowHtml(rec) {
  const safeTitle = String(rec.title || "").replace(/'/g, "\\'");
  // Open the article popup directly — the SPA results list already shows
  // the same matches we'd render in an intermediate search popup, so an
  // extra hop just duplicates what the user is looking at. Pass an empty
  // sourceQuery so the article view doesn't render a "Back to results"
  // button (closing the popup returns the user to the SPA list anyway).
  return `
    <div class="wiki-result">
      <div class="wiki-result-head">
        <a href="#" class="wiki-result-title" onclick="event.preventDefault();openWikiArticle('${escHtml(safeTitle)}','')" title="Open article">${escHtml(rec.title || "")}</a>
        ${_wikiSaveBtnHtml(rec.title || "")}
      </div>
      <div class="wiki-result-snippet">${_sanitizeWikiSnippet(rec.snippet || "")}…</div>
    </div>`;
}

// Wikipedia returns search snippets containing <span class="searchmatch">
// highlights. We escape everything else (so an attacker can't inject
// scripts via a poisoned wiki page title), then re-allow ONLY that
// known-safe highlight span. Anything else is rendered as text.
function _sanitizeWikiSnippet(raw) {
  const escaped = escHtml(String(raw));
  // After escaping, real highlights look like
  //   &lt;span class=&quot;searchmatch&quot;&gt;…&lt;/span&gt;
  // Re-render those exact tags (and the matching closing tag) only.
  return escaped
    .replace(/&lt;span class=&quot;searchmatch&quot;&gt;/g, '<span class="searchmatch">')
    .replace(/&lt;\/span&gt;/g, '</span>');
}

// ── Saved-articles state ────────────────────────────────────────────
// _wikiSavedTitles is a Set of canonical titles the current user has
// starred. Loaded once per page from /api/user/wiki-saves/ids and
// mutated locally on every toggle so ★ state is instant.
let _wikiSavedTitles = null;
let _wikiSavedItems  = null;
let _wikiTab         = "search";

async function _wikiLoadSavedIds() {
  if (_wikiSavedTitles) return _wikiSavedTitles;
  try {
    const r = await apiFetch("/api/user/wiki-saves/ids");
    if (!r.ok) { _wikiSavedTitles = new Set(); return _wikiSavedTitles; }
    const j = await r.json();
    _wikiSavedTitles = new Set(Array.isArray(j?.ids) ? j.ids : []);
  } catch {
    _wikiSavedTitles = new Set();
  }
  return _wikiSavedTitles;
}

function _wikiSaveBtnHtml(title) {
  if (!title) return "";
  const safe = String(title).replace(/'/g, "\\'");
  const isSaved = !!(_wikiSavedTitles && _wikiSavedTitles.has(title));
  const cls = isSaved ? "wiki-save-btn is-saved" : "wiki-save-btn";
  const tip = isSaved ? "Remove from saved" : "Save article for later";
  return `<button type="button" class="${cls}" data-wiki-title="${escHtml(title)}" onclick="event.preventDefault();event.stopPropagation();_wikiToggleSave('${escHtml(safe)}', this)" title="${tip}">★</button>`;
}

// Update every ★ button in the DOM that targets this title so they
// stay in sync (a row's ★, the article-header ★, the saved-tab card,
// etc. all live independently).
function _wikiSyncSaveButtons(title) {
  const isSaved = !!(_wikiSavedTitles && _wikiSavedTitles.has(title));
  document.querySelectorAll(`.wiki-save-btn[data-wiki-title="${CSS.escape(title)}"]`).forEach(b => {
    b.classList.toggle("is-saved", isSaved);
    b.title = isSaved ? "Remove from saved" : "Save article for later";
  });
}

async function _wikiToggleSave(title, btn) {
  const t = String(title || "").trim();
  if (!t) return;
  await _wikiLoadSavedIds();
  const isSaved = _wikiSavedTitles.has(t);
  // Optimistic toggle so the ★ flips instantly even on slow connections
  if (isSaved) _wikiSavedTitles.delete(t); else _wikiSavedTitles.add(t);
  _wikiSyncSaveButtons(t);
  try {
    if (isSaved) {
      const r = await apiFetch("/api/user/wiki-saves", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: t }),
      });
      if (!r.ok) throw new Error("Delete failed");
    } else {
      const row = btn?.closest(".wiki-result");
      const snippetEl = row?.querySelector(".wiki-result-snippet");
      const snippet = snippetEl ? snippetEl.textContent.trim().slice(0, 500) : null;
      const url = `https://en.wikipedia.org/wiki/${encodeURIComponent(t.replace(/ /g, "_"))}`;
      const r = await apiFetch("/api/user/wiki-saves", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: t, url, snippet, thumbnail: null, data: {} }),
      });
      if (!r.ok) throw new Error("Save failed");
    }
    _wikiSavedItems = null;
    if (_wikiTab === "saved") _wikiRenderSavedTab();
    if (typeof showToast === "function") {
      showToast(isSaved ? "Removed from saved" : "Saved");
    }
  } catch (err) {
    if (isSaved) _wikiSavedTitles.add(t); else _wikiSavedTitles.delete(t);
    _wikiSyncSaveButtons(t);
    if (typeof showToast === "function") {
      showToast(isSaved ? "Could not remove save" : "Could not save", "error");
    }
  }
}

// ── Tab switching + saved-tab render ─────────────────────────────────
function _wikiSwitchTab(tab, { pushUrl = true } = {}) {
  _wikiTab = tab === "saved" ? "saved" : "search";
  document.querySelectorAll("#wiki-view .loc-tab").forEach(btn => btn.classList.remove("active"));
  document.querySelector(`#wiki-view .loc-tab-${_wikiTab}`)?.classList.add("active");
  const searchPanel = document.querySelector(".wiki-panel-search");
  const savedPanel  = document.querySelector(".wiki-panel-saved");
  if (searchPanel) searchPanel.style.display = _wikiTab === "search" ? "" : "none";
  if (savedPanel)  savedPanel.style.display  = _wikiTab === "saved"  ? "" : "none";
  if (_wikiTab === "saved") _wikiRenderSavedTab();
  if (pushUrl && typeof history?.pushState === "function") {
    const qs = new URLSearchParams(location.search);
    qs.set("v", "wiki");
    if (_wikiTab === "saved") qs.set("tab", "saved"); else qs.delete("tab");
    const next = "/?" + qs.toString();
    if (location.pathname + location.search !== next) {
      history.pushState({}, "", next);
    }
  }
}

async function _wikiRenderSavedTab() {
  const el = document.getElementById("wiki-saved-results");
  if (!el) return;
  if (_wikiSavedItems) {
    el.innerHTML = _wikiRenderSavedListHtml(_wikiSavedItems);
    return;
  }
  el.innerHTML = `<div class="wiki-results-loading">Loading saved articles…</div>`;
  try {
    const r = await apiFetch("/api/user/wiki-saves");
    if (!r.ok) {
      el.innerHTML = `<div class="wiki-results-error">Could not load saved articles.</div>`;
      return;
    }
    const j = await r.json();
    _wikiSavedItems = Array.isArray(j?.items) ? j.items : [];
    _wikiSavedTitles = new Set(_wikiSavedItems.map(it => it.title));
    el.innerHTML = _wikiRenderSavedListHtml(_wikiSavedItems);
  } catch {
    el.innerHTML = `<div class="wiki-results-error">Could not load saved articles.</div>`;
  }
}

function _wikiRenderSavedListHtml(items) {
  if (!items.length) {
    return `<div class="wiki-results-empty">No saved articles yet. Click ★ on any search result to save it for later.</div>`;
  }
  return `<div class="wiki-results-rows">${items.map(it => {
    const safeTitle = String(it.title || "").replace(/'/g, "\\'");
    const snippetHtml = it.snippet
      ? `<div class="wiki-result-snippet">${escHtml(String(it.snippet))}</div>`
      : "";
    return `
      <div class="wiki-result">
        <div class="wiki-result-head">
          <a href="#" class="wiki-result-title" onclick="event.preventDefault();openWikiArticle('${escHtml(safeTitle)}','')" title="Open article">${escHtml(it.title || "")}</a>
          ${_wikiSaveBtnHtml(it.title || "")}
        </div>
        ${snippetHtml}
      </div>`;
  }).join("")}</div>`;
}

// Expose tab switcher + toggle to inline onclick handlers in index.html
window._wikiSwitchTab = _wikiSwitchTab;
window._wikiToggleSave = _wikiToggleSave;

async function runWikiPageSearch(query) {
  const q = String(query || "").trim();
  const resultsEl = document.getElementById("wiki-view-results");
  if (!resultsEl) return;
  if (!q) { resultsEl.innerHTML = ""; return; }
  resultsEl.innerHTML = `<div class="wiki-results-loading">Searching Wikipedia for <em>${escHtml(q)}</em>…</div>`;
  // Track the current query on the container so the load-more button
  // knows what to extend, and reset paging state for the new query.
  resultsEl.dataset.query = q;
  resultsEl.dataset.offset = "0";
  // Save to recents + refresh the datalist for native autocomplete.
  _pushWikiRecent(q);
  // Reflect the query in the URL. pushState (vs replaceState) so each
  // search becomes a back-button entry — typing a new search and then
  // hitting back returns to the previous wiki search list.
  try {
    const u = new URL(window.location.href);
    const prev = u.searchParams.get("wq") || "";
    u.searchParams.set("wq", q);
    if (prev !== q) {
      history.pushState({}, "", u.toString());
    } else {
      history.replaceState({}, "", u.toString());
    }
  } catch {}
  try {
    const r = await apiFetch(`/api/wikipedia/search?q=${encodeURIComponent(q)}&limit=${_WIKI_PAGE_SIZE}&offset=0`);
    if (!r.ok) {
      resultsEl.innerHTML = `<div class="wiki-results-error">Wikipedia search failed.</div>`;
      return;
    }
    const data = await r.json();
    const rows = Array.isArray(data?.results) ? data.results : [];
    if (!rows.length) {
      resultsEl.innerHTML = `<div class="wiki-results-empty">No matches for <em>${escHtml(q)}</em>.</div>`;
      return;
    }
    // Wikipedia's snippet field already contains <span class="searchmatch">
    // highlight markup; render verbatim so query terms are emphasised.
    resultsEl.innerHTML =
      `<div class="wiki-results-rows">${rows.map(_wikiResultRowHtml).join("")}</div>` +
      _renderWikiLoadMoreFooter(rows.length, data.totalhits, data.nextOffset);
    resultsEl.dataset.offset = String(data.nextOffset ?? rows.length);
  } catch (err) {
    resultsEl.innerHTML = `<div class="wiki-results-error">Wikipedia search failed: ${escHtml(err.message || String(err))}</div>`;
  }
}

// Footer rendered below the results list — shows "X of Y results" plus a
// "Load more" button when there are still rows on Wikipedia's side.
function _renderWikiLoadMoreFooter(loadedCount, totalhits, nextOffset) {
  const total = (typeof totalhits === "number") ? totalhits : null;
  const hasMore = nextOffset != null && (total == null || loadedCount < total);
  const counter = total != null
    ? `<span class="wiki-results-counter">${loadedCount} of ${total.toLocaleString()} results</span>`
    : `<span class="wiki-results-counter">${loadedCount} results</span>`;
  const btn = hasMore
    ? `<button type="button" class="wiki-load-more" onclick="loadMoreWikiResults()">Load more</button>`
    : `<span class="wiki-results-end">— end of results —</span>`;
  return `<div class="wiki-results-footer">${counter}${btn}</div>`;
}

async function loadMoreWikiResults() {
  const resultsEl = document.getElementById("wiki-view-results");
  if (!resultsEl) return;
  const q = resultsEl.dataset.query || "";
  const offset = parseInt(resultsEl.dataset.offset || "0", 10) || 0;
  if (!q) return;
  const rowsContainer = resultsEl.querySelector(".wiki-results-rows");
  const footer = resultsEl.querySelector(".wiki-results-footer");
  // Disable the button while loading so users don't double-click.
  const btn = footer?.querySelector(".wiki-load-more");
  if (btn) { btn.disabled = true; btn.textContent = "Loading…"; }
  try {
    const r = await apiFetch(`/api/wikipedia/search?q=${encodeURIComponent(q)}&limit=${_WIKI_PAGE_SIZE}&offset=${offset}`);
    if (!r.ok) {
      if (btn) { btn.disabled = false; btn.textContent = "Load more"; }
      return;
    }
    const data = await r.json();
    const rows = Array.isArray(data?.results) ? data.results : [];
    if (rowsContainer && rows.length) {
      rowsContainer.insertAdjacentHTML("beforeend", rows.map(_wikiResultRowHtml).join(""));
    }
    const loadedCount = (rowsContainer?.children.length) ?? rows.length;
    const newFooter = _renderWikiLoadMoreFooter(loadedCount, data.totalhits, data.nextOffset);
    if (footer) footer.outerHTML = newFooter;
    resultsEl.dataset.offset = String(data.nextOffset ?? (offset + rows.length));
  } catch {
    if (btn) { btn.disabled = false; btn.textContent = "Load more"; }
  }
}

// Esc closes wiki popup if open (without touching underlying modals)
document.addEventListener("keydown", e => {
  if (e.key === "Escape") {
    const w = document.getElementById("wiki-overlay");
    if (w && w.classList.contains("open")) {
      e.stopPropagation();
      closeWikiPopup();
    }
  }
}, true);

// Admin-only "+" icon — used to add Discogs artists to the Blues DB
// inline. Disabled per admin request: blues-DB curation now happens
// manually via the /admin Blues panel rather than from search results
// / album popup credit lines. Kept as a no-op so the call sites
// don't need to be deleted (and so it can be flipped back on later
// without touching every caller). _bluesAddArtist below is also kept
// since it's still wired to the admin panel's own add flow.
function bluesAddIcon(_discogsId, _name) { return ""; }

async function _bluesAddArtist(discogsId, name, anchor) {
  try {
    const r = await apiFetch("/api/admin/blues/add-by-discogs-id", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ discogs_id: discogsId, name }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      showToast?.("Add failed: " + (err.error ?? r.status), "error");
      return;
    }
    if (window._adminBluesIds) window._adminBluesIds.add(Number(discogsId));
    if (window._adminBluesNames && name) window._adminBluesNames.add(String(name).trim().toLowerCase());
    // Remove every "+ add" link for this id from the DOM so the popup
    // updates in place (multiple may exist if the artist appears in
    // both the album-artist row and a credit row).
    document.querySelectorAll(`.blues-add-icon[data-blues-id="${discogsId}"]`).forEach(a => a.remove());
    showToast?.("Added to Blues DB");
  } catch (e) {
    showToast?.("Add failed: " + e, "error");
  }
}

// Card-level + adder — cards only have the artist NAME parsed from the
// result title (no Discogs ID locally). The server resolves the name
// to an ID via /database/search?type=artist and upserts.
async function _bluesAddArtistByName(name, anchor) {
  try {
    const r = await apiFetch("/api/admin/blues/add-by-name", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      showToast?.("Add failed: " + (err.error ?? r.status), "error");
      return;
    }
    const j = await r.json();
    if (window._adminBluesIds && j.discogs_id) window._adminBluesIds.add(Number(j.discogs_id));
    if (window._adminBluesNames) {
      if (name) window._adminBluesNames.add(String(name).trim().toLowerCase());
      if (j.name) window._adminBluesNames.add(String(j.name).trim().toLowerCase());
    }
    // Remove every card "+ add" link for this name from the DOM. Match
    // case-insensitively since cards may have the disambiguator and
    // the canonical form may not.
    const lc = String(name).trim().toLowerCase();
    document.querySelectorAll(".card-blues-add").forEach(a => {
      const dn = (a.getAttribute("data-blues-name") ?? "").trim().toLowerCase();
      if (dn === lc) a.remove();
    });
    showToast?.("Added to Blues DB");
  } catch (e) {
    showToast?.("Add failed: " + e, "error");
  }
}

// Small "W" icon to render after a magnifying glass — opens the wiki popup
// for the given query. Wraps the term in double-quotes so Wikipedia's
// CirrusSearch treats it as an exact phrase — fixes fuzzy mismatches
// like "Bill Wax" → Bill Gates by forcing a literal phrase match.
// Pass extraTerms (unquoted) for context like "record label" so the
// quoted phrase isn't the entire query.
function wikiIcon(query, label = "", extraTerms = "") {
  if (!query) return "";
  // Wikipedia is now open to all callers (anons rate-limited per IP);
  // no client-side gate needed.
  const phrase = String(query).trim();
  if (!phrase) return "";
  const composed = extraTerms
    ? `"${phrase}" ${String(extraTerms).trim()}`
    : `"${phrase}"`;
  const q = composed.replace(/'/g, "\\'");
  const lab = label || query;
  // No leading space — wiki-icon's small left margin in CSS provides
  // just enough breathing room from the preceding ⌕ glass.
  return `<a href="#" class="wiki-icon" onclick="event.preventDefault();openWikiPopup('${escHtml(q)}')" title="Wikipedia: ${escHtml(lab)}">W</a>`;
}

// ── Per-track Library of Congress lookup ───────────────────────────────
// Renders a small 🏛 icon next to a track title. Clicking it queries
// loc.gov for a public-domain audio match against `"track" "artist"`.
// Behavior:
//   0 results → small "No LOC match" toast under the icon.
//   1 result  → start playback in the existing LOC bottom bar.
//   2+ results → small floating list anchored to the icon; click a row
//                to play it. Click outside to dismiss.
function locIcon(trackTitle, artistName) {
  if (!trackTitle) return "";
  // LOC is now open to all callers (anons throttled at 5/min per IP);
  // no client-side gate needed.
  const t = String(trackTitle).replace(/'/g, "\\'");
  const a = String(artistName || "").replace(/'/g, "\\'");
  return ` <a href="#" class="track-loc-icon" onclick="locTrackSearch(event, '${escHtml(t)}', '${escHtml(a)}', this)" title="Search Library of Congress for &quot;${escHtml(trackTitle)}&quot; (public-domain recordings)">🏛</a>`;
}

let _locTrackPopupEl = null;
let _locTrackOutsideClickHandler = null;

function _closeLocTrackPopup() {
  if (_locTrackPopupEl) { _locTrackPopupEl.remove(); _locTrackPopupEl = null; }
  if (_locTrackOutsideClickHandler) {
    document.removeEventListener("mousedown", _locTrackOutsideClickHandler, true);
    _locTrackOutsideClickHandler = null;
  }
}

function _locTrackToast(anchor, msg) {
  _closeLocTrackPopup();
  const tip = document.createElement("div");
  tip.className = "track-loc-toast";
  tip.textContent = msg;
  document.body.appendChild(tip);
  const r = anchor.getBoundingClientRect();
  tip.style.position = "fixed";
  tip.style.left = `${Math.min(window.innerWidth - 220, Math.max(8, r.left))}px`;
  tip.style.top  = `${r.bottom + 6}px`;
  setTimeout(() => { tip.classList.add("fade-out"); }, 1600);
  setTimeout(() => { tip.remove(); }, 2200);
}

function _renderLocTrackPopup(anchor, items) {
  _closeLocTrackPopup();
  const wrap = document.createElement("div");
  wrap.className = "track-loc-popup";
  wrap.innerHTML = `
    <div class="track-loc-popup-header">Library of Congress · ${items.length} match${items.length === 1 ? "" : "es"}</div>
    <div class="track-loc-popup-list">
      ${items.slice(0, 10).map((it, i) => {
        const contributor = Array.isArray(it.contributors) && it.contributors.length ? it.contributors.join(", ") : "";
        const yr = it.year ? ` · ${escHtml(String(it.year))}` : "";
        return `<a href="#" class="track-loc-popup-row" data-i="${i}" onclick="event.preventDefault();_locPlayFromTrackPopup(${i})">
          <div class="track-loc-popup-title">${escHtml(it.title || "Untitled")}</div>
          <div class="track-loc-popup-meta">${escHtml(contributor)}${yr}</div>
        </a>`;
      }).join("")}
    </div>
  `;
  document.body.appendChild(wrap);
  // Stash the items on the popup element so the row click handler can read them
  wrap._locItems = items.slice(0, 10);
  _locTrackPopupEl = wrap;

  // Position below the anchor; clamp to viewport.
  const r = anchor.getBoundingClientRect();
  const popupW = 320;
  const left = Math.min(window.innerWidth - popupW - 8, Math.max(8, r.left));
  wrap.style.position = "fixed";
  wrap.style.left = `${left}px`;
  wrap.style.top  = `${r.bottom + 6}px`;
  wrap.style.width = `${popupW}px`;

  // Dismiss on outside click
  _locTrackOutsideClickHandler = (ev) => {
    if (!_locTrackPopupEl) return;
    if (_locTrackPopupEl.contains(ev.target)) return;
    if (ev.target === anchor) return;
    _closeLocTrackPopup();
  };
  document.addEventListener("mousedown", _locTrackOutsideClickHandler, true);
}

function _locPlayFromTrackPopup(idx) {
  if (!_locTrackPopupEl?._locItems) return;
  const item = _locTrackPopupEl._locItems[idx];
  if (!item) return;
  _closeLocTrackPopup();
  if (typeof _locPlay === "function") _locPlay(item);
}

async function locTrackSearch(ev, trackTitle, artistName, btn) {
  ev.preventDefault();
  ev.stopPropagation();
  if (btn?.dataset.locBusy === "1") return;
  if (btn) {
    btn.dataset.locBusy = "1";
    btn.classList.add("is-loading");
  }
  // Compose `"track" "artist"` so LOC's full-text index needs both phrases.
  // If we have no artist, fall back to just the track.
  const qParts = [];
  if (trackTitle) qParts.push(`"${trackTitle}"`);
  if (artistName) qParts.push(`"${artistName}"`);
  const q = qParts.join(" ");
  try {
    const r = await apiFetch(`/api/loc/search?q=${encodeURIComponent(q)}&playable=1&c=20`);
    if (!r.ok) {
      _locTrackToast(btn, r.status === 401 ? "Sign in to search LOC" : "LOC search failed");
      return;
    }
    const body = await r.json();
    const items = (Array.isArray(body?.results) ? body.results : [])
      .filter(it => it && typeof it.streamUrl === "string" && it.streamUrl);
    if (!items.length) {
      _locTrackToast(btn, "No public-domain LOC match");
      return;
    }
    if (items.length === 1) {
      _closeLocTrackPopup();
      if (typeof _locPlay === "function") _locPlay(items[0]);
      return;
    }
    _renderLocTrackPopup(btn, items);
  } catch (err) {
    _locTrackToast(btn, "LOC search failed");
  } finally {
    if (btn) {
      btn.dataset.locBusy = "0";
      btn.classList.remove("is-loading");
    }
  }
}

window.locTrackSearch          = locTrackSearch;
window._locPlayFromTrackPopup  = _locPlayFromTrackPopup;

// ── Video popup ────────────────────────────────────────────────────────────
let ytPlayer = null;
let _ytLoading = false;
let _ytSession = 0;        // incremented on each openVideo, checked by async callbacks
let _ytPollId  = null;     // polling interval for YT API readiness
// Repeat modes: "off" → "album" → "one" → "off"
let _ytRepeat = "off";

function toggleRepeat() {
  _ytRepeat = _ytRepeat === "off" ? "album" : _ytRepeat === "album" ? "one" : "off";
  const labels  = { off: "Repeat: off", album: "Repeat: album", one: "Repeat: one" };
  const colors  = { off: "#666", album: "#4caf50", one: "var(--accent)" };
  // Mini bar icons: off = plain arrow, album = circled arrows, one = circled 1
  const miniIcons = { off: "↻", album: "⟳ ALL", one: "⟳ 1" };
  // Expanded nav labels
  const navLabels = { off: "↻ repeat", album: "⟳ repeat all", one: "⟳ repeat 1" };
  document.querySelectorAll(".repeat-btn").forEach(btn => {
    btn.style.color = colors[_ytRepeat];
    btn.title = labels[_ytRepeat];
    if (btn.closest("#video-nav")) btn.innerHTML = navLabels[_ytRepeat];
    else btn.innerHTML = miniIcons[_ytRepeat];
  });
}
window.onYouTubeIframeAPIReady = function() { window._ytAPIReady = true; };

// Highlight the currently playing track in any open popup tracklist
function highlightPlayingTrack() {
  document.querySelectorAll(".track-link.now-playing").forEach(el => el.classList.remove("now-playing"));
  const queue = window._videoQueue ?? [];
  const idx = window._videoQueueIndex ?? -1;
  const currentUrl = queue[idx];
  if (!currentUrl) return;
  // Prefer highlighting the exact row at the queue's index inside the
  // container the queue was built from. This avoids the "wrong track turns
  // orange" problem when fuzzy title matching gives two adjacent tracks the
  // same YouTube URL — in that case URL-only matching would always light up
  // the first occurrence, even if the user clicked the second.
  const ownerId = window._videoQueueContainerId || "";
  const owner = ownerId ? document.getElementById(ownerId) : null;
  if (owner) {
    const ownerTracks = owner.querySelectorAll(".track-link[data-video]");
    const target = ownerTracks[idx];
    if (target && target.dataset.video === currentUrl) {
      target.classList.add("now-playing");
      return;
    }
  }
  // Fallback: highlight the first URL match per popup container (covers
  // cases where the queue's owner popup is no longer in the DOM).
  const seen = new Set();
  document.querySelectorAll(".track-link[data-video]").forEach(el => {
    if (el.dataset.video !== currentUrl) return;
    const container = el.closest("#album-info, #version-info") || document;
    const key = container.id || "_root";
    if (seen.has(key)) return;
    seen.add(key);
    el.classList.add("now-playing");
  });
}

function ensureYTAPI() {
  if (window._ytAPIReady || _ytLoading) return;
  _ytLoading = true;
  const s = document.createElement("script");
  s.src = "https://www.youtube.com/iframe_api";
  document.head.appendChild(s);
}
// Preload YT API at page boot so the first click on a track ▶
// finds a ready API and creates the player synchronously inside the
// click handler — avoids the autoplay-blocked first-play that
// happens when the API has to download mid-click.
document.addEventListener("DOMContentLoaded", () => { try { ensureYTAPI(); } catch {} });

function setVideoUrl(id) {
  const u = new URL(window.location.href);
  u.searchParams.set("vd", id);
  // Sync ?vp= ("video parent popup") to the playing track's release.
  // Set just before this call by openVideo (window._playerReleaseType/Id),
  // so as the queue advances ?vp= points to the *current* track's
  // album — not whatever popup happened to be open. Sharing the URL
  // mid-queue gives the recipient the same track + album combo the
  // sender is hearing. If the new track has no known release, drop
  // ?vp= so a stale prior value doesn't linger.
  const rType = window._playerReleaseType;
  const rId   = window._playerReleaseId;
  if (rType && rId) {
    u.searchParams.set("vp", `${rType}:${rId}`);
  } else {
    u.searchParams.delete("vp");
  }
  history.replaceState({}, "", u.toString());
}

let _ytLoadTimer = null;
let _ytHasPlayed = false;  // true once the current video reaches "playing" state
let _ytVideoToken = 0;     // increments per video load — guards against stale per-video callbacks
function loadYTVideo(id) {
  _ytHasPlayed = false;
  window._ytRetried = false;  // reset retry flag for new video
  _ytVideoToken++;
  const vtoken = _ytVideoToken;
  console.debug("[loadYTVideo]", { id, hasPlayer: !!ytPlayer, apiReady: !!window._ytAPIReady });
  updatePlayerStatus("loading");
  // Stall watchdog: if the video hasn't reached the playing state by
  // 5s, treat it as unavailable and skip. We check _ytHasPlayed
  // instead of the status-text string because that flag is set once
  // and only by the YT state=1 callback — the previous text-based
  // check could miss intermediate states like "ended" / "error" if
  // the YT player swallowed them. 5s is the sweet spot: any healthy
  // video starts within a couple of seconds; anything taking longer
  // is probably broken (deleted / geo-blocked / embed-disabled / no
  // network). Was 8s; that felt stalled.
  if (_ytLoadTimer) clearTimeout(_ytLoadTimer);
  _ytLoadTimer = setTimeout(() => {
    if (vtoken !== _ytVideoToken) return;  // a different video was loaded since
    if (_ytHasPlayed) return;              // it actually started
    // Distinguish "actually unavailable" from "autoplay-blocked".
    // YT.PlayerState 5 (cued) means the iframe loaded the video
    // but the browser's autoplay policy refused to start it — most
    // common on first-visit URL bootstraps where the user hasn't
    // interacted with the domain yet. Treating that as "unavailable"
    // and pruning was wrong (the track is fine; the user just needs
    // to click play). State -1 (unstarted) is the same situation
    // before YT has even reported a state.
    let state = -1;
    try { state = ytPlayer?.getPlayerState?.() ?? -1; } catch {}
    if (state === 5 || state === -1) {
      updatePlayerStatus("paused");
      if (typeof showToast === "function") {
        showToast("Tap ▶ to start playback", "info", 4000);
      }
      return;
    }
    updatePlayerStatus("unavailable");
    _ytPruneUnavailable(id, /*advance=*/true);
  }, 5000);
  if (ytPlayer && typeof ytPlayer.loadVideoById === "function") {
    ytPlayer.loadVideoById(id);
    return;
  }
  // Wait for YT API so we always get onStateChange events (needed for repeat/auto-advance)
  if (window._ytAPIReady && typeof YT !== "undefined") {
    _createYTPlayer(id);
  } else {
    // Poll briefly for API readiness, then create player
    const pollSession = _ytSession;
    let attempts = 0;
    if (_ytPollId) clearInterval(_ytPollId);
    _ytPollId = setInterval(() => {
      attempts++;
      if (pollSession !== _ytSession) { clearInterval(_ytPollId); _ytPollId = null; return; }
      if (window._ytAPIReady && typeof YT !== "undefined") {
        clearInterval(_ytPollId); _ytPollId = null;
        _createYTPlayer(id);
      } else if (attempts > 40) {
        // Fallback after ~4s if API never loads
        clearInterval(_ytPollId); _ytPollId = null;
        document.getElementById("video-player").innerHTML =
          `<iframe src="https://www.youtube.com/embed/${id}?autoplay=1" style="width:100%;height:100%;border:none" allow="autoplay;encrypted-media" allowfullscreen></iframe>`;
      }
    }, 100);
  }
}

function updatePlayerStatus(state, errorCode) {
  // Don't clear _ytLoadTimer here. It used to be cleared on any
  // non-loading/non-buffering state to "shortcut" the watchdog, but
  // that disarmed the safety net for failure modes where the player
  // briefly hits "paused" / "ended" without ever reaching "playing"
  // (autoplay-blocked, region-locked-without-error, embed-disabled
  // silent failures). The watchdog body already gates on
  // _ytHasPlayed so it's a no-op when the video genuinely played;
  // letting it always run to 5 s ensures stuck loads still get
  // pruned. closeVideo() still clears it on real teardown.
  const el = document.getElementById("mini-player-status");
  if (!el) return;
  const map = {
    loading:     { text: "loading…",    cls: "status-loading" },
    buffering:   { text: "buffering…",  cls: "status-loading" },
    playing:     { text: "▶ playing",   cls: "status-playing" },
    paused:      { text: "⏸ paused",   cls: "status-paused"  },
    ended:       { text: "ended",       cls: "status-ended"   },
    unavailable: { text: "⚠ unavailable", cls: "status-error" },
    error:       { text: "⚠ error",     cls: "status-error"   },
  };
  const info = map[state] ?? { text: "", cls: "" };
  el.textContent = info.text;
  el.className = "mini-player-status " + info.cls;
  syncPlayPauseBtn(state);
}

// Drop a YouTube videoId from the cross-source queue (every position
// matching the id) when the player can't load it — deleted video,
// geo-blocked, embed-disabled, or stuck at "loading…" for 8 s. With
// `advance` true, we kick off auto-advance afterward so the user
// doesn't sit on a dead row. Toast lets them know what happened.
//
// Two advance paths:
//   - If the dead track is the currently-playing row, queueRemove
//     internally calls _queuePlayItem on the next track. That's
//     the happy path.
//   - If queueRemove can't infer "playing" (e.g. cross-source mark
//     drifted), we fall back to playNextVideo() so the player still
//     moves forward.
function _ytPruneUnavailable(videoId, advance) {
  if (!videoId) return;
  if (typeof showToast === "function") {
    showToast("Track unavailable — skipping", "error");
  }
  // Was the dead track sitting in the cross-source queue? If yes,
  // queueRemove(null, externalId) sweeps every matching position AND
  // auto-advances to the next queue row when the removed one was
  // the playing row. If no — the play was a one-off (URL share,
  // album modal click without queueing), and we fall back to
  // playNextVideo so the per-album _videoQueue still advances.
  const queueHadIt =
    Array.isArray(window._queue) /* not exposed, fall through */ ? false :
    (typeof window._queueGetCurrentPosition === "function" &&
     typeof queueRemove === "function");
  let queueRemoveTriggered = false;
  try {
    if (typeof queueRemove === "function") {
      queueRemove(null, String(videoId));
      queueRemoveTriggered = true;
    }
  } catch (e) {
    console.warn("[yt-prune] queueRemove threw:", e);
  }
  if (!advance) return;
  // If queueRemove handled the advance (dead row was playing), we'd
  // double-advance by also calling playNextVideo. Detect this by
  // checking whether the engine has switched away from yt OR the
  // ytPlayer is now loading a different video. Crude but effective:
  // wait 1500ms for queueRemove's async _queuePlayItem to dispatch,
  // then only fall through to playNextVideo if nothing else fired.
  const tokenAtSchedule = _ytVideoToken;
  setTimeout(() => {
    // If _ytVideoToken changed, something else (queueRemove's
    // auto-advance) already started loading a new video — leave it
    // alone. Same if engine flipped to LOC.
    if (_ytVideoToken !== tokenAtSchedule) return;
    if (window._currentEngine !== "yt") return;
    if (typeof playNextVideo === "function") playNextVideo();
  }, 1500);
}

function _createYTPlayer(id) {
  const session = _ytSession;
  // Don't reassign vtoken inside callbacks — capture once at creation
  // so onStateChange / onError can compare against the live token
  // without their captured value getting out from under them.
  const vtoken = _ytVideoToken;
  console.debug("[_createYTPlayer]", { id, session, vtoken });
  document.getElementById("video-player").innerHTML = "";
  ytPlayer = new YT.Player("video-player", {
    height: "100%", width: "100%", videoId: id,
    playerVars: { autoplay: 1, rel: 0, playsinline: 1 },
    events: {
      onStateChange: function(e) {
        if (session !== _ytSession) return;   // player was destroyed/recreated
        // YT.PlayerState: -1=unstarted, 0=ended, 1=playing, 2=paused, 3=buffering, 5=cued
        if (e.data === 1) {
          _ytHasPlayed = true; updatePlayerStatus("playing"); window._ytRetried = false;
          // Begin polling for YT progress (timeupdate-equivalent — the
          // iframe API doesn't fire one) so the mini-progress strip
          // animates without us hand-rolling rAF.
          if (typeof _startYtProgressLoop === "function") _startYtProgressLoop();
          // If no track meta (e.g. loaded from URL param), grab title from YT player
          const meta = (window._videoQueueMeta ?? [])[window._videoQueueIndex ?? 0];
          let msTitle = meta?.track || "";
          let msAlbum = meta?.album || "";
          let msArtist = meta?.artist || "";
          if (!meta || (!meta.track && !meta.album && !meta.artist)) {
            try {
              const vd = ytPlayer.getVideoData?.();
              if (vd?.title) {
                const titleEl = document.getElementById("mini-player-title");
                if (titleEl) titleEl.innerHTML = `<span class="vt-track">${escHtml(vd.title)}</span>`;
                if (!msTitle) msTitle = vd.title;
                if (!msArtist) msArtist = vd.author || "";
              }
            } catch {}
          }
          // Push to the OS media session so the lock-screen / Bluetooth
          // controls show track info + cover art.
          if (typeof _mediaSessionUpdate === "function") {
            _mediaSessionUpdate({
              title:  msTitle || "YouTube",
              artist: msArtist,
              album:  msAlbum,
              artwork: id ? `https://i.ytimg.com/vi/${encodeURIComponent(id)}/hqdefault.jpg` : "",
            });
          }
        }
        else if (e.data === 2) {
          updatePlayerStatus("paused");
          // Stop polling while paused — saves a few wakes/sec; fill
          // resumes from the last known position when play resumes.
          if (typeof _stopYtProgressLoop === "function") _stopYtProgressLoop();
        }
        else if (e.data === 3) updatePlayerStatus("buffering");
        else if (e.data === 0) {
          // Guard against late "ended" events from a previous video on a reused player
          if (vtoken !== _ytVideoToken) return;
          // "Ended" before "playing" ever fired = silent failure (the
          // video never actually started — usually region-blocked or
          // embed-disabled in a way that doesn't trigger onError).
          // Treat the same as an unavailable error: prune the dead
          // track from the queue + advance, so we don't leave it
          // sitting in the queue to retry forever.
          if (!_ytHasPlayed) {
            updatePlayerStatus("unavailable");
            _ytPruneUnavailable(id, /*advance=*/true);
            if (typeof _stopYtProgressLoop === "function") _stopYtProgressLoop();
            return;
          }
          // Don't double-fire if the tail-watch in _updateMiniProgress
          // already fired ended for this token.
          if (_ytEndFiredToken === _ytVideoToken) {
            updatePlayerStatus("ended");
            if (typeof _stopYtProgressLoop === "function") _stopYtProgressLoop();
            return;
          }
          _ytEndFiredToken = _ytVideoToken;
          updatePlayerStatus("ended"); onVideoEnded();
          if (typeof _stopYtProgressLoop === "function") _stopYtProgressLoop();
        }
        else if (e.data === 5) updatePlayerStatus("loading");
        // (vtoken is now const — captured at player creation; stale
        // events from a destroyed/recreated player are filtered out
        // by the session check above.)
      },
      onError: function(e) {
        if (session !== _ytSession) return;   // player was destroyed/recreated
        // Guard against errors from a previous video on a reused player
        if (vtoken !== _ytVideoToken) return;
        // Ignore errors if the video was already playing (late rights checks, transient issues)
        if (_ytHasPlayed) return;
        // Error codes: 2=invalid id, 5=HTML5 error, 100=not found, 101/150=embedding disabled
        const code = e?.data;
        if (code === 100 || code === 101 || code === 150) {
          updatePlayerStatus("unavailable");
          // Report this video to the server's unavailable-tracker so
          // future popups can pre-filter it out of tracklists. After
          // a small threshold of distinct reports, the server flips
          // status to 'unavailable' and broken videos vanish across
          // the site so users can submit replacements via the
          // album-suggest popup. Signed-in only — anon reports
          // would be too easy to spam.
          if (window._clerk?.user && typeof apiFetch === "function") {
            try {
              apiFetch("/api/youtube/report-unavailable", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ videoId: id, errorCode: code }),
              }).catch(() => {});
            } catch {}
          }
          // Removed from queue here so the user doesn't keep hitting
          // the same dead track on every cycle. Advance happens
          // either through queueRemove's wasPlaying auto-advance
          // (if the dead track was the playing row) or via
          // playNextVideo as a fallback.
          _ytPruneUnavailable(id, /*advance=*/true);
        } else if (code === 5 && !window._ytRetried) {
          // HTML5 error can be transient — retry once
          window._ytRetried = true;
          const retryId = id;  // capture current video id for retry
          const retryToken = vtoken;
          updatePlayerStatus("buffering");
          setTimeout(() => {
            if (retryToken !== _ytVideoToken) return;  // user moved on
            if (ytPlayer && typeof ytPlayer.loadVideoById === "function") ytPlayer.loadVideoById(retryId);
          }, 1000);
        } else {
          window._ytRetried = false;
          updatePlayerStatus("error");
          // Generic errors (code 2 = invalid id, code 5 after retry,
          // or anything else): treat as unavailable, prune + advance.
          _ytPruneUnavailable(id, /*advance=*/true);
        }
      },
      onReady: function() {
        // onReady fires when iframe is ready, not when video plays — don't set "playing" yet
        if (session === _ytSession) updatePlayerStatus("loading");
      }
    }
  });
}

function updateVideoNavButtons() {
  const queue = window._videoQueue ?? [];
  const idx   = window._videoQueueIndex ?? 0;
  // Update both expanded nav and mini bar buttons. NEXT is enabled if
  // either the per-album _videoQueue has more or the cross-source
  // queue has any item — same dual-source check the LOC engine uses.
  const prevBtn  = document.getElementById("video-prev");
  const nextBtn  = document.getElementById("video-next");
  const miniPrev = document.getElementById("mini-prev");
  const miniNext = document.getElementById("mini-next");
  const titleEl  = document.getElementById("mini-player-title");
  const ytHasNext = idx < queue.length - 1;
  const xqHasNext = (typeof _queueHasNext === "function") ? _queueHasNext() : false;
  const xqHasPrev = (typeof window._queueHasPrev === "function") ? window._queueHasPrev() : false;
  const ytHasPrev = idx > 0;
  const nextDisabled = !(ytHasNext || xqHasNext);
  const prevDisabled = !(ytHasPrev || xqHasPrev);
  if (prevBtn)  prevBtn.disabled  = prevDisabled;
  if (nextBtn)  nextBtn.disabled  = nextDisabled;
  if (miniPrev) miniPrev.disabled = prevDisabled;
  if (miniNext) miniNext.disabled = nextDisabled;
  if (titleEl) {
    const meta = (window._videoQueueMeta ?? [])[idx];
    if (meta && (meta.track || meta.album || meta.artist)) {
      const parts = [];
      if (meta.track)  parts.push(`<span class="vt-track">${escHtml(meta.track)}</span>`);
      if (meta.album)  parts.push(`<span>${escHtml(meta.album)}</span>`);
      if (meta.artist) parts.push(`<span>${escHtml(meta.artist)}</span>`);
      titleEl.innerHTML = parts.join(`<span class="vt-sep">·</span>`);
    } else {
      // No meta — try to get title from YT player, otherwise leave as-is (onStateChange will fill it)
      try {
        const vd = ytPlayer?.getVideoData?.();
        if (vd?.title) titleEl.innerHTML = `<span class="vt-track">${escHtml(vd.title)}</span>`;
      } catch {}
    }
  }
  // Show/hide album + share buttons. Engine-aware: YT needs a release
  // ID, LOC needs a loaded item. See openPlayerRelease for the
  // engine-aware click dispatch. Class-based via CSS rule
  // .mini-player.has-release.
  const mp = document.getElementById("mini-player");
  if (mp) {
    const hasOpenable = (window._currentEngine === "loc" && !!window._locNowPlaying)
                     || (!!window._playerReleaseId);
    mp.classList.toggle("has-release", hasOpenable);
  }
  console.debug("[updateVideoNavButtons]", {
    playerReleaseId: window._playerReleaseId,
    engine:          window._currentEngine,
    discIconVisible: !!window._playerReleaseId || (window._currentEngine === "loc" && !!window._locNowPlaying),
  });
  highlightPlayingTrack();
}

function toggleMiniPlayer() {
  const mp = document.getElementById("mini-player");
  if (mp) mp.classList.toggle("expanded");
}

// ── Unified player dispatchers ─────────────────────────────────────────
// The persistent bar hosts both the YouTube iframe engine and the LOC
// <audio> engine. window._currentEngine ("yt" | "loc" | null) tracks
// which one is active; the controls cluster routes through these
// wrappers so prev/play-pause/next/close hit the right engine.
window._currentEngine = window._currentEngine || null;

// Set the engine on the bar so CSS can swap which expanded panel +
// source-specific buttons are shown. Pass null to clear.
function _setPlayerEngine(name) {
  window._currentEngine = name;
  const mp = document.getElementById("mini-player");
  if (!mp) return;
  mp.classList.remove("engine-yt", "engine-loc");
  if (name === "yt")  mp.classList.add("engine-yt");
  if (name === "loc") mp.classList.add("engine-loc");
  // Whenever an engine becomes active, drop idle-queue immediately —
  // otherwise the progress strip stays hidden, the play button keeps
  // routing through queuePlayHead instead of toggling actual audio,
  // and engine-source-only buttons (LOC save/info, YT share/album)
  // stay hidden because their show-rules don't match. Clearing the
  // engine (name=null) hands control back to _queueRefreshIdleBar.
  if (name) {
    mp.classList.remove("idle-queue");
    // Clear the idle-queue text artifacts ("ready · click ▶") and
    // reset the playpause icon to its default ⏸ — the engine's own
    // state handlers (LOC's _locUpdatePlayPauseBtn / YT's
    // updatePlayerStatus) will swap in the correct icon as soon as
    // playback state arrives.
    const statusEl = document.getElementById("mini-player-status");
    if (statusEl) statusEl.textContent = "";
    const ppBtn = document.getElementById("mini-playpause");
    if (ppBtn) {
      ppBtn.innerHTML = "&#9208;"; // ⏸
      ppBtn.title = "Pause";
    }
  }
  // Sync the disc-icon-visibility class. The icon's click handler
  // now dispatches by engine, so the visibility rule is "do we have
  // SOMETHING to open for the current track?":
  //   YT  → _playerReleaseId is set
  //   LOC → an item is loaded (always — _locPlay sets _locNowPlaying
  //         before _setPlayerEngine, so the LOC info popup is always
  //         openable while LOC is the active engine)
  // updateVideoNavButtons re-syncs YT separately on every YT play.
  const hasOpenable = (name === "loc" && !!window._locNowPlaying)
                   || (!!window._playerReleaseId);
  mp.classList.toggle("has-release", hasOpenable);
  console.debug("[setPlayerEngine]", {
    engine: name,
    locNowPlaying: !!window._locNowPlaying,
    playerReleaseId: window._playerReleaseId,
    discIconVisible: hasOpenable,
  });
  // Source icon next to the title
  const icon = document.getElementById("mini-player-source-icon");
  if (icon) icon.textContent = name === "loc" ? "♪" : (name === "yt" ? "▶" : "");
  // When the engine clears (audio stopped), let the idle-queue logic
  // re-surface the bar if there are still items queued.
  if (name == null && typeof window._queueRefreshIdleBar === "function") {
    setTimeout(() => window._queueRefreshIdleBar(), 0);
  }
}

function playerTogglePause() {
  const bar = document.getElementById("mini-player");
  const isIdle = bar?.classList.contains("idle-queue");
  const engine = window._currentEngine;
  // Diagnostic — helps confirm which branch fires when playback
  // misbehaves. Logged at debug level so it's only in the console.
  console.debug("[playerTogglePause]", { isIdle, engine, hasYt: !!ytPlayer });
  // Idle-queue mode: bar is showing because the queue has items but
  // nothing is loaded yet. ▶ kicks off playback from the queue head.
  if (isIdle) {
    if (typeof queuePlayHead === "function") queuePlayHead();
    return;
  }
  if (engine === "loc") {
    const a = document.getElementById("loc-audio");
    if (!a || !a.src) {
      // Edge case: engine is "loc" but no src loaded. Fall back to
      // queuePlayHead so the click still does something useful.
      if (typeof queuePlayHead === "function") queuePlayHead();
      return;
    }
    if (a.paused) a.play().catch(() => {}); else a.pause();
    return;
  }
  if (engine === "yt") {
    // YT player can be null briefly if the API was still loading
    // when openVideo was first called. If we have a queue with a
    // current position, kick that off so the click isn't a no-op.
    // Note: ytPlayer is module-scoped, NOT window.ytPlayer (which is
    // always undefined). Earlier check used window.ytPlayer and was
    // ALWAYS falling through to queuePlayHead → loadVideoById →
    // restart. That was the entire pause-restarts-the-track bug.
    if (!ytPlayer || typeof ytPlayer.getPlayerState !== "function") {
      if (typeof queuePlayHead === "function") queuePlayHead();
      return;
    }
    if (typeof toggleVideoPause === "function") toggleVideoPause();
    return;
  }
  // No engine active and not idle — this shouldn't happen, but if
  // it does, try to recover by playing the queue head.
  if (typeof queuePlayHead === "function") queuePlayHead();
}

// ── Click/drag-to-seek progress strip ───────────────────────────────────
// Single dispatcher reads the current engine and asks it for currentTime
// + duration; same code path drives both LOC <audio> playback and the
// YouTube IFrame Player. _ytProgressTimer is a setInterval kept active
// only while a YT video is playing — see playYTVideo / onPlayerStateChange.
let _ytProgressTimer = null;
let _miniDragging = false;
let _miniDragFraction = 0;

function _formatProgressTime(s) {
  if (!Number.isFinite(s) || s < 0) return "0:00";
  const total = Math.floor(s);
  const m = Math.floor(total / 60);
  const sec = total % 60;
  return `${m}:${sec < 10 ? "0" : ""}${sec}`;
}

// Read currentTime + duration from whichever engine is active.
// Returns { current, duration } in seconds, or null if unknown.
function _playerReadProgress() {
  if (window._currentEngine === "loc") {
    const a = document.getElementById("loc-audio");
    if (!a || !a.duration || !Number.isFinite(a.duration)) return null;
    return { current: a.currentTime || 0, duration: a.duration };
  }
  if (window._currentEngine === "yt" && typeof ytPlayer !== "undefined" && ytPlayer) {
    try {
      const dur = ytPlayer.getDuration?.() ?? 0;
      const cur = ytPlayer.getCurrentTime?.() ?? 0;
      if (!Number.isFinite(dur) || dur <= 0) return null;
      return { current: cur, duration: dur };
    } catch { return null; }
  }
  return null;
}

function _playerSeekToFraction(f) {
  const p = _playerReadProgress();
  if (!p) return;
  const target = Math.max(0, Math.min(p.duration, f * p.duration));
  if (window._currentEngine === "loc") {
    const a = document.getElementById("loc-audio");
    try { if (a) a.currentTime = target; } catch {}
    return;
  }
  if (window._currentEngine === "yt" && typeof ytPlayer !== "undefined" && ytPlayer) {
    try { ytPlayer.seekTo?.(target, true); } catch {}
  }
}

// Refresh the visible fill + time labels from current engine state.
// Called from LOC's timeupdate event AND from a YT-polling interval.
// While the user is mid-drag, the fill follows the drag fraction
// instead of the engine's currentTime so the UI doesn't fight them.
function _updateMiniProgress() {
  const fill   = document.getElementById("mini-progress-fill");
  const knob   = document.getElementById("mini-progress-knob");
  const cur    = document.getElementById("mini-progress-current");
  const tot    = document.getElementById("mini-progress-total");
  const player = document.getElementById("mini-player");
  if (!fill || !player) return;
  const p = _playerReadProgress();
  if (!p) {
    player.classList.remove("has-duration");
    fill.style.width = "0%";
    if (knob) knob.style.left = "0%";
    if (cur) cur.textContent = "0:00";
    if (tot) tot.textContent = "0:00";
    return;
  }
  player.classList.add("has-duration");
  const fraction = _miniDragging
    ? _miniDragFraction
    : (p.duration > 0 ? p.current / p.duration : 0);
  const pct = Math.max(0, Math.min(1, fraction)) * 100;
  fill.style.width = `${pct}%`;
  if (knob) knob.style.left = `${pct}%`;
  if (cur) cur.textContent = _formatProgressTime(_miniDragging ? fraction * p.duration : p.current);
  if (tot) tot.textContent = _formatProgressTime(p.duration);
  // Mirror position to the OS media session so the lock-screen
  // scrubber stays in sync with playback. Throttled implicitly by
  // the existing 500 ms YT poll / LOC timeupdate cadence.
  if (typeof _mediaSessionUpdatePosition === "function") {
    _mediaSessionUpdatePosition();
  }
  // Fallback end detection: YouTube's onStateChange sometimes never
  // fires state=0 (ended), so onVideoEnded never runs and the queue
  // doesn't auto-advance. If the YT engine's currentTime has reached
  // duration - 0.5s while a track is loaded, fire ended manually.
  // Guarded by _ytEndFiredToken so we don't double-fire when the real
  // event eventually does arrive.
  if (window._currentEngine === "yt" && !_miniDragging && _ytHasPlayed
      && p.duration > 0 && (p.duration - p.current) <= 0.5
      && _ytEndFiredToken !== _ytVideoToken) {
    _ytEndFiredToken = _ytVideoToken;
    console.debug("[yt-tail] firing manual onVideoEnded", { current: p.current, duration: p.duration });
    try { onVideoEnded(); } catch {}
  }
}
// Tracks the most-recent _ytVideoToken we've manually fired ended for,
// so the tail-watch in _updateMiniProgress can't fire twice for the
// same track. Reset implicitly by token comparisons.
let _ytEndFiredToken = -1;

// Pointer-driven drag scrubbing. Tap = single click → instant seek.
// Drag = update visual fraction live, fire the actual seek on pointerup
// so audio doesn't jitter as the user moves.
function _miniProgressFractionAt(ev) {
  const strip = document.getElementById("mini-progress");
  if (!strip) return 0;
  const r = strip.getBoundingClientRect();
  return Math.max(0, Math.min(1, (ev.clientX - r.left) / r.width));
}
function _onMiniProgressDown(ev) {
  if (ev.button != null && ev.button !== 0) return; // left-click / touch only
  const strip = document.getElementById("mini-progress");
  if (!strip) return;
  _miniDragging = true;
  _miniDragFraction = _miniProgressFractionAt(ev);
  strip.classList.add("is-dragging");
  _updateMiniProgress();
  try { strip.setPointerCapture?.(ev.pointerId); } catch {}
  ev.preventDefault();
}
function _onMiniProgressMove(ev) {
  if (!_miniDragging) return;
  _miniDragFraction = _miniProgressFractionAt(ev);
  _updateMiniProgress();
}
function _onMiniProgressUp(ev) {
  if (!_miniDragging) return;
  _miniDragging = false;
  const strip = document.getElementById("mini-progress");
  if (strip) strip.classList.remove("is-dragging");
  _playerSeekToFraction(_miniDragFraction);
  _updateMiniProgress();
}

// Wire pointer events once on first script load. The strip is in the
// DOM from page load (hidden via CSS until duration is known) so we can
// attach now. Falls back to clicking via the existing onclick handler
// for browsers without PointerEvents (rare; caps the regression).
function _bindMiniProgress() {
  const strip = document.getElementById("mini-progress");
  if (!strip || strip.dataset.bound === "1") return;
  strip.dataset.bound = "1";
  strip.addEventListener("pointerdown", _onMiniProgressDown);
  strip.addEventListener("pointermove", _onMiniProgressMove);
  strip.addEventListener("pointerup",   _onMiniProgressUp);
  strip.addEventListener("pointercancel", _onMiniProgressUp);
  // Hook LOC's audio events so the bar refreshes without a polling
  // loop while it's the active engine. YT uses _startYtProgressLoop
  // because its iframe API doesn't fire timeupdate events.
  const a = document.getElementById("loc-audio");
  if (a && !a.dataset.miniBound) {
    a.dataset.miniBound = "1";
    a.addEventListener("timeupdate",     _updateMiniProgress);
    a.addEventListener("loadedmetadata", _updateMiniProgress);
    a.addEventListener("durationchange", _updateMiniProgress);
    a.addEventListener("seeked",         _updateMiniProgress);
    a.addEventListener("ended",          _updateMiniProgress);
  }
}
document.addEventListener("DOMContentLoaded", _bindMiniProgress);
// In case scripts load late after DOMContentLoaded already fired:
if (document.readyState !== "loading") _bindMiniProgress();

// YT-side polling. Started from onPlayerReady / onPlayerStateChange when
// playback begins; stopped when playback ends or the bar closes.
function _startYtProgressLoop() {
  if (_ytProgressTimer) return;
  _ytProgressTimer = setInterval(_updateMiniProgress, 500);
  _updateMiniProgress();
}
function _stopYtProgressLoop() {
  if (_ytProgressTimer) { clearInterval(_ytProgressTimer); _ytProgressTimer = null; }
}

// Click handler kept on the strip for non-pointer fallbacks. With
// pointer events bound above, click is largely redundant — but it
// lets the inline onclick="" attribute (if anything still triggers it)
// behave correctly. No-ops while a drag is in progress.
function playerSeekFrom(ev) {
  if (_miniDragging) return;
  const f = _miniProgressFractionAt(ev);
  _playerSeekToFraction(f);
}

window._updateMiniProgress   = _updateMiniProgress;
window._startYtProgressLoop  = _startYtProgressLoop;
window._stopYtProgressLoop   = _stopYtProgressLoop;
window.playerSeekFrom        = playerSeekFrom;
function playerPrev() {
  // Cross-source queue takes precedence over engine-internal prev:
  // a user-curated queue should drive both directions of navigation.
  if (typeof _queuePlayPrev === "function") {
    Promise.resolve(_queuePlayPrev()).then(handled => {
      if (handled) return;
      // Fall through to engine-internal prev (LOC's per-item track
      // queue or YT's _videoQueue) when the cross-source queue can't
      // step back any further.
      if (window._currentEngine === "loc" && typeof _locPlayPrevInQueue === "function") _locPlayPrevInQueue();
      else if (typeof videoPrev === "function") videoPrev();
    }).catch(() => {
      if (window._currentEngine === "loc" && typeof _locPlayPrevInQueue === "function") _locPlayPrevInQueue();
      else if (typeof videoPrev === "function") videoPrev();
    });
    return;
  }
  if (window._currentEngine === "loc") {
    if (typeof _locPlayPrevInQueue === "function") _locPlayPrevInQueue();
    return;
  }
  if (typeof videoPrev === "function") videoPrev();
}
function playerNext() {
  if (window._currentEngine === "loc") {
    if (typeof _locPlayNextInQueue === "function") _locPlayNextInQueue();
    return;
  }
  if (typeof playNextVideo === "function") playNextVideo();
}
// Bar has no manual close button anymore — it auto-hides when both
// the engine is idle and the queue is empty (see _queueRefreshIdleBar).
// playerClose is kept on window because Clear (and the engine-shutdown
// paths) still call it programmatically to drop active playback.
function playerClose() {
  if (window._currentEngine === "loc") {
    if (typeof _locClosePlayer === "function") _locClosePlayer();
    return;
  }
  if (typeof closeVideo === "function") closeVideo();
}

// ⏹ button on the media bar — stop whatever's playing and forget the
// current track + progress. The queue itself is preserved (use Clear
// in the drawer to wipe the queue). After this, the bar returns to
// idle-queue mode showing the head item if any remain, or hides if
// the queue is empty. Distinct from playerClose() which only stops
// the engine but leaves _queueCurrentPosition pointing at the row
// that was playing — the next ▶ would resume mid-queue rather than
// restart from the head.
function playerStop() {
  // Tear down whichever engine is active.
  if (typeof playerClose === "function") {
    try { playerClose(); } catch {}
  }
  // Drop the now-playing mark + position so the queue forgets what
  // was playing. _queueClearPlayingMark also re-runs the idle-bar
  // logic on the next tick, so the bar either shows the queue head
  // or hides if the queue is empty.
  if (typeof window._queueClearPlayingMark === "function") {
    try { window._queueClearPlayingMark(); } catch {}
  }
}
window.playerStop = playerStop;
// Bar info-area click. For YT: open the album modal if known. For LOC:
// open the info popup for the playing item. Falls back to expand toggle.
function playerInfoClick() {
  if (window._currentEngine === "loc") {
    if (typeof _locOpenFromBar === "function") { _locOpenFromBar(); return; }
  }
  toggleMiniPlayer();
}

// ➕ on a track row → resolve the YouTube videoId and add to the queue.
// The data-yt-url attribute carries the matched watch URL written into
// the row by renderModal at popup time.
function _trackQueueAdd(el) {
  const url = el?.dataset?.ytUrl || "";
  const videoId = (typeof extractYouTubeId === "function") ? extractYouTubeId(url) : "";
  if (!videoId) return;
  const meta = {
    title:       el.dataset.track   || "",
    artist:      el.dataset.artist  || "",
    albumTitle:  el.dataset.album   || "",
    // Release context: lets the disc-icon ("Open this album") in the
    // mini-player keep working even when this track is reached via
    // queue auto-advance (no DOM scrape, no ?op= URL param).
    releaseType: el.dataset.releaseType || "",
    releaseId:   el.dataset.releaseId   || "",
  };
  console.debug("[_trackQueueAdd]", {
    videoId,
    title: meta.title,
    releaseType: meta.releaseType || "(empty)",
    releaseId:   meta.releaseId   || "(empty)",
  });
  if (typeof queueAddYt === "function") {
    // ＋ button = add to the tail of the queue. The "play now"
    // affordance is the ▶ button, which goes through openVideo and
    // inserts at head via queueAddAlbumOrPlay's "play" mode.
    queueAddYt(videoId, meta, { mode: "append" });
    el.classList.add("queued");
    el.title = "Already added — click queue button to view";
  }
}
window._trackQueueAdd = _trackQueueAdd;

// ── Unavailable YouTube videos (broken / removed / embed-disabled) ──
//
// Set of videoIds the server has flagged as unavailable. Populated
// once per page session via /api/youtube/unavailable-list and
// consulted by findVideo so broken videos are treated as missing —
// album popups then count those tracks toward the "🎵 N missing"
// heading and the user can submit a replacement via the album-
// suggest popup. Refreshes after a TTL or on demand.
window._sdYtUnavailable = window._sdYtUnavailable || new Set();
let _sdYtUnavailableFetchedAt = 0;
const _SD_YT_UNAVAILABLE_TTL_MS = 5 * 60 * 1000;

async function _sdEnsureYtUnavailableLoaded() {
  if (Date.now() - _sdYtUnavailableFetchedAt < _SD_YT_UNAVAILABLE_TTL_MS) return;
  try {
    const r = await fetch("/api/youtube/unavailable-list", { cache: "no-store" });
    if (!r.ok) return;
    const j = await r.json();
    const ids = Array.isArray(j?.videoIds) ? j.videoIds : [];
    window._sdYtUnavailable = new Set(ids);
    _sdYtUnavailableFetchedAt = Date.now();
  } catch { /* best-effort */ }
}
window._sdEnsureYtUnavailableLoaded = _sdEnsureYtUnavailableLoaded;

// Pull a YouTube videoId out of a watch URL or short URL — same
// helper as extractYouTubeId, inlined here to avoid an import cycle
// between modal.js and queue.js. 11 chars, alphanumeric + _ -.
function _sdYtIdFromUrl(url) {
  if (!url) return "";
  // youtu.be/ID or watch?v=ID or embed/ID
  const m = String(url).match(/(?:v=|youtu\.be\/|embed\/)([A-Za-z0-9_-]{11})/);
  return m ? m[1] : "";
}

// ── Crowd-sourced track YouTube overrides ────────────────────────────
//
// Cache keyed by "${type}:${id}" of the popup that triggered the
// fetch. Master popups query master scope only; release popups query
// both master scope (via d.master_id) and release scope, with release
// winning at the same position.
const _trackYtOverridesCache = new Map();

function _trackYtCacheKey(masterId, releaseId, isMaster) {
  if (isMaster) return `master:${masterId || releaseId || ""}`;
  return `release:${releaseId || ""}`;
}

// Synchronous cache read used by renderAlbumInfo's first-pass render.
// Returns a Map(position -> override) or null if not yet fetched.
function _trackYtReadCache(masterId, releaseId, isMaster) {
  const k = _trackYtCacheKey(masterId, releaseId, isMaster);
  return _trackYtOverridesCache.get(k)?.byPosition || null;
}

async function _trackYtFetchOverrides(masterId, releaseId, isMaster) {
  const k = _trackYtCacheKey(masterId, releaseId, isMaster);
  const params = [];
  // For master popups, the master ID IS the popup's releaseId
  // (the popup IS the master). For release popups, masterId is the
  // optional d.master_id. Without this fallback, master popups sent
  // no params and silently returned an empty override map — which
  // is why some popups didn't display their submitted tracks.
  const effectiveMasterId = isMaster ? (masterId || releaseId) : masterId;
  if (effectiveMasterId) params.push(`master_id=${encodeURIComponent(effectiveMasterId)}`);
  // For release popups we still want the master-scope rows — use
  // d.master_id when available, else fall back to release_id only.
  if (!isMaster && releaseId) params.push(`release_id=${encodeURIComponent(releaseId)}`);
  if (!params.length) {
    const empty = new Map();
    _trackYtOverridesCache.set(k, { byPosition: empty, fetchedAt: Date.now() });
    return empty;
  }
  try {
    const r = await fetch(`/api/track-yt/for-release?${params.join("&")}`);
    if (!r.ok) {
      const empty = new Map();
      _trackYtOverridesCache.set(k, { byPosition: empty, fetchedAt: Date.now() });
      return empty;
    }
    const j = await r.json();
    const byPosition = new Map();
    // Master rows first, release rows second so release wins on overlap.
    for (const o of (j.overrides || [])) {
      if (o.release_type === "master") byPosition.set(String(o.track_position), o);
    }
    for (const o of (j.overrides || [])) {
      if (o.release_type === "release") byPosition.set(String(o.track_position), o);
    }
    _trackYtOverridesCache.set(k, { byPosition, fetchedAt: Date.now() });
    return byPosition;
  } catch {
    const empty = new Map();
    _trackYtOverridesCache.set(k, { byPosition: empty, fetchedAt: Date.now() });
    return empty;
  }
}

// Hook called from renderAlbumInfo after the popup HTML is in the DOM.
// Fetches overrides and re-renders affected tracklist rows so newly
// available overrides surface without requiring a popup re-open.
async function _trackYtKickFetchAndApply(targetId, masterId, releaseId, isMaster) {
  const k = _trackYtCacheKey(masterId, releaseId, isMaster);
  const cached = _trackYtOverridesCache.get(k);
  // Always refetch on popup open — overrides are rare-write, but a
  // user who just submitted one expects to see it on the next open.
  // The fetch is cheap (small JSON, server-side).
  await _trackYtFetchOverrides(masterId, releaseId, isMaster);
  _trackYtApplyToDom(targetId, masterId, releaseId, isMaster);
}
window._trackYtKickFetchAndApply = _trackYtKickFetchAndApply;

// Patch tracklist rows in-place using the latest cached overrides.
// Idempotent: rebuilds the override-driven affordances each call so a
// post-suggest re-apply correctly reflects the new state.
function _trackYtApplyToDom(targetId, masterId, releaseId, isMaster) {
  const root = document.getElementById(targetId) || document;
  const rows = root.querySelectorAll(".album-tracklist .track[data-pos]");
  if (!rows.length) return;
  const map = _trackYtReadCache(masterId, releaseId, isMaster) || new Map();
  rows.forEach(row => {
    const pos = row.dataset.pos || "";
    const titleCell = row.querySelector(".track-title");
    if (!titleCell) return;
    // Strip prior override-driven affordances we own — leaves the
    // Discogs-supplied play/queue alone.
    titleCell.querySelectorAll(".track-yt-override-badge, .track-yt-admin-delete, .track-yt-suggest").forEach(el => el.remove());
    // If row has a Discogs-supplied play link (a .track-link with
    // data-video set), the playCell is already correct and we don't
    // need to touch it. We only inject when the row has NO play cell
    // content at all.
    const playCell = row.querySelector(".track-play-cell");
    const hasPlay = !!(playCell && playCell.querySelector(".track-link"));
    const ov = map.get(String(pos));
    if (ov && !hasPlay) {
      // Inject ▶ + ＋ using the override videoId.
      const url = `https://www.youtube.com/watch?v=${ov.video_id}`;
      // The Full Album pseudo-row is rendered hidden for non-admins
      // when no override exists at boot. If an override has now landed,
      // reveal the row.
      if (row.classList.contains("track-fullalbum") && row.style.display === "none") {
        row.style.display = "";
      }
      const titleLink = titleCell.querySelector(".track-title-link");
      const trackTitle = titleLink?.textContent || ov.track_title || "";
      // The popup root is #album-info / #version-info — there's no
      // .album-popup wrapper, so the previous closest() selector was
      // returning null and trackArtist was always blank. Use the
      // root we already have via `root` (the targetId).
      const trackArtist = root?.querySelector?.(".album-artist")?.textContent?.split(",")[0]?.trim() || "";
      const albumTitle  = root?.querySelector?.("h2")?.textContent?.trim() || "";
      const entityType  = isMaster ? "master" : "release";
      const playHtml = `<a class="track-play-btn track-link" href="#" data-video="${url}" data-track="${escHtml(trackTitle)}" data-album="${escHtml(albumTitle)}" data-artist="${escHtml(trackArtist)}" data-release-type="${entityType}" data-release-id="${escHtml(String(releaseId || ""))}" onclick="openVideo(event,'${url}')" title="Play this track">▶</a>`;
      if (playCell) playCell.innerHTML = playHtml;
      // Append ＋ queue button at the end of the title cell, before
      // any badges/credits. Just put it at the very end — credits
      // come later via the credits row layout.
      // Mark the Full Album row's queue-add icon so the album-level
      // bulk-queue (queueAddAlbum) skips it — without this attr, post-
      // render override injection would let "Queue album" pick up the
      // full-album video alongside per-track ones.
      const isFullAlbumRow = String(pos) === "ALBUM";
      const fullAlbumAttr = isFullAlbumRow ? ' data-fullalbum="1"' : "";
      const queueAddHtml = ` <a href="#" class="queue-add-icon"${fullAlbumAttr} data-yt-url="${url}" data-track="${escHtml(trackTitle)}" data-album="${escHtml(albumTitle)}" data-artist="${escHtml(trackArtist)}" data-release-type="${entityType}" data-release-id="${escHtml(String(releaseId || ""))}" onclick="event.preventDefault();_trackQueueAdd(this);return false" title="${isFullAlbumRow ? "Add full album to play queue" : "Add to play queue"}">＋</a>`;
      // Avoid double-inserting: only add if not already there.
      if (!titleCell.querySelector(`.queue-add-icon[data-yt-url="${url}"]`)) {
        // Insert before any .track-credits subtree so credits stay last.
        const credits = titleCell.querySelector(".track-credits");
        const tmpl = document.createElement("template");
        tmpl.innerHTML = queueAddHtml.trim();
        if (credits) credits.before(tmpl.content);
        else titleCell.appendChild(tmpl.content);
      }
      row.dataset.ytOverride = "1";
    }
    // Now decorate the row's title with the badge / admin delete /
    // suggest button based on current state.
    const stillNoPlay = !(row.querySelector(".track-play-cell .track-link"));
    const isOverride = !!ov && !!row.querySelector(".track-play-cell .track-link") && row.dataset.ytOverride === "1";
    const credits = titleCell.querySelector(".track-credits");
    const decorate = (html) => {
      if (!html) return;
      const tmpl = document.createElement("template");
      tmpl.innerHTML = html.trim();
      if (credits) credits.before(tmpl.content);
      else titleCell.appendChild(tmpl.content);
    };
    if (isOverride) {
      decorate(` <span class="track-yt-override-badge" title="User-suggested YouTube video">🎵</span>`);
      if (window._isAdmin) {
        decorate(` <a href="#" class="track-yt-admin-delete" data-pos="${escHtml(pos)}" onclick="event.preventDefault();_trackYtAdminDelete(this);return false" title="Admin: remove this user-suggested video">✕</a>`);
      }
    }
    // Per-row suggest button retired — handled by the album-level
    // "🎵 N missing" link in the tracklist heading instead.
  });
  // Refresh the heading "🎵 N missing" link to reflect the post-patch
  // state. Without this the count is stuck at the render-time value
  // (which counted overrides as missing because the cache was empty
  // before the fetch landed).
  _trackYtRefreshHeadingMissingCount(root);
}
window._trackYtApplyToDom = _trackYtApplyToDom;

// Recount missing tracks from the DOM (rows with no .track-link in
// .track-play-cell, i.e. no playable URL even after the override
// patch) and update the heading link's text + visibility. If 0
// missing remain, the link disappears entirely.
function _trackYtRefreshHeadingMissingCount(root) {
  if (!root) return;
  const rows = root.querySelectorAll(".album-tracklist .track[data-pos]");
  let missing = 0;
  rows.forEach(row => {
    if (!row.querySelector(".track-play-cell .track-link")) missing++;
  });
  const link = root.querySelector(".tracklist-find-missing");
  if (!link) return;
  // Admin-only while quota is constrained — keep the link hidden for
  // non-admins regardless of missing count.
  if (missing < 1 || !window._clerk?.user || !window._isAdmin) {
    link.style.display = "none";
    return;
  }
  link.style.display = "";
  link.textContent = `🎵 ${missing} missing`;
}
window._trackYtRefreshHeadingMissingCount = _trackYtRefreshHeadingMissingCount;

// Click handler for the per-row 🎵 suggest affordance. Stashes the
// track context on window so youtube.js's popup can pick it up + show
// "✓ Suggest" buttons on each result, then opens the popup with a
// pre-filled query.
function _trackYtOpenSuggest(el) {
  if (!window._clerk?.user) {
    if (typeof showToast === "function") showToast("Sign in to suggest videos", "info");
    return;
  }
  // Admin-only while YouTube quota is constrained.
  if (!window._isAdmin) {
    if (typeof showToast === "function") showToast("YouTube submissions are admin-only right now (quota request pending).", "info");
    return;
  }
  // Walk up to the popup root to find the masterId / releaseId.
  // The album popup body has #album-info or #version-info as the
  // closest container; the type+id come from the popup share button's
  // dataset, but easier: re-read from the typeBadge text "Master: 12345"
  // or "Release: 67890". Falling back to the URL ?op=type:id if absent.
  const popup = el.closest(".album-popup, .modal-content, #album-info, #version-info") || document;
  const badge = popup.querySelector(".album-type-badge");
  let scopeType = "master";
  let scopeId = "";
  if (badge) {
    const m = /^(Master|Release|Version)\s*:\s*(\d+)/.exec(badge.textContent || "");
    if (m) {
      scopeType = m[1].toLowerCase() === "master" ? "master" : "release";
      scopeId = m[2];
    }
  }
  if (!scopeId) {
    // URL fallback
    try {
      const u = new URL(location.href);
      const op = u.searchParams.get("op");
      if (op) {
        const [t, id] = op.split(":");
        if (t && id) { scopeType = t; scopeId = id; }
      }
    } catch {}
  }
  const trackTitle  = el.dataset.track  || "";
  const trackArtist = el.dataset.artist || "";
  const trackAlbum  = el.dataset.album  || "";
  const trackPos    = el.dataset.pos    || "";
  if (!scopeId || !trackPos || !trackTitle) {
    if (typeof showToast === "function") showToast("Could not identify track context", "error");
    return;
  }
  // Extra context for the post-submit re-fetch: passing masterId
  // separately so a release-scope override still gets refreshed
  // alongside the existing master-scope rows (single-key cache).
  const popupRoot = el.closest("#album-info, #version-info");
  const masterId  = popupRoot?.dataset?.masterId || "";
  // Default to master scope when a master_id is known, even from a
  // release popup. The intent is "submissions lift to all pressings"
  // unless the user explicitly wants this submission to apply only
  // to a specific pressing. Track-position keys match between master
  // and release in most simple cases ("1", "2", "3"); for vinyl
  // ("A1", "B2") the release scope is still appropriate, so we keep
  // the release-scope path for popups WITHOUT a master_id.
  let submitScopeType = scopeType;
  let submitScopeId   = scopeId;
  if (scopeType === "release" && masterId) {
    submitScopeType = "master";
    submitScopeId   = masterId;
  }
  window._sdSuggestForTrack = {
    releaseType: submitScopeType,
    releaseId:   submitScopeId,
    masterId,
    trackPosition: trackPos,
    trackTitle,
    trackArtist,
    trackAlbum,
    // Where to re-apply the DOM patch after a successful suggestion —
    // the popup target id (#album-info or #version-info).
    targetId: popupRoot?.id || "album-info",
  };
  const q = [trackArtist ? `"${trackArtist}"` : "", trackTitle ? `"${trackTitle}"` : "", trackAlbum]
    .filter(Boolean).join(" ");
  if (typeof window.openYoutubePopup === "function") {
    window.openYoutubePopup(q);
  } else if (typeof window._sdLoadModule === "function") {
    window._sdLoadModule("/youtube.js").then(() => {
      window.openYoutubePopup?.(q);
    });
  }
}
window._trackYtOpenSuggest = _trackYtOpenSuggest;

// Album-level "Find missing tracks" — opens a YouTube popup with one
// search across the whole album, then lets the user assign returned
// videos to specific missing tracks via dropdowns. One search.list
// call instead of N (where N = number of missing tracks).
//
// Triggered by the 🎵 N missing affordance in the tracklist heading.
// Builds the missing-tracks payload from the popup's tracklist DOM
// (any .track row that has data-pos but no .track-link in its
// .track-play-cell), stashes it on window for youtube.js to consume,
// then opens the YT popup with q = "Artist" "Album".
async function _trackYtOpenAlbumSuggest(el) {
  if (!window._clerk?.user) {
    if (typeof showToast === "function") showToast("Sign in to suggest videos", "info");
    return;
  }
  // Admin-only while YouTube quota is constrained.
  if (!window._isAdmin) {
    if (typeof showToast === "function") showToast("YouTube submissions are admin-only right now (quota request pending).", "info");
    return;
  }
  const popupRoot = el.closest("#album-info, #version-info");
  if (!popupRoot) return;
  const releaseId   = popupRoot.dataset?.releaseId  || "";
  const masterId    = popupRoot.dataset?.masterId   || "";
  const isMaster    = popupRoot.dataset?.entityType === "master";
  const albumTitle  = popupRoot.querySelector("h2")?.textContent?.trim() || "";
  // First artist from the album-artist line (mirrors per-track flow).
  const albumArtist = popupRoot.querySelector(".album-artist")?.textContent?.split(",")[0]?.trim() || "";
  // Ensure the override DOM patch has run before we walk the rows —
  // otherwise tracks with already-saved overrides would still show
  // as "missing" if the user clicked the heading link before the
  // initial cache fetch landed. await is cheap when cache is hot.
  if (typeof window._trackYtKickFetchAndApply === "function") {
    try {
      await window._trackYtKickFetchAndApply(
        popupRoot.id || "album-info",
        masterId,
        isMaster ? "" : releaseId,
        isMaster
      );
    } catch {}
  }
  // Walk every tracklist row, collect the ones with no playable URL.
  const rows = popupRoot.querySelectorAll(".album-tracklist .track[data-pos]");
  const missing = [];
  rows.forEach(row => {
    const pos = row.dataset.pos || "";
    const hasPlay = !!row.querySelector(".track-play-cell .track-link");
    if (hasPlay) return;
    const titleEl = row.querySelector(".track-title-link");
    const trackTitle = titleEl?.textContent?.trim() || "";
    if (!pos || !trackTitle) return;
    missing.push({ position: pos, title: trackTitle });
  });
  if (!missing.length) {
    if (typeof showToast === "function") showToast("No missing tracks on this album", "info");
    return;
  }
  // Default scope: master if known, else release. Same rule as the
  // per-track flow so a contribution lifts to all pressings.
  const submitScopeType = (isMaster || masterId) ? "master" : "release";
  const submitScopeId   = submitScopeType === "master" ? (masterId || releaseId) : releaseId;
  if (!submitScopeId) {
    if (typeof showToast === "function") showToast("Could not identify album scope", "error");
    return;
  }
  window._sdSuggestAlbumContext = {
    releaseType:  submitScopeType,
    releaseId:    submitScopeId,
    masterId,
    albumTitle,
    albumArtist,
    tracks:       missing,
    targetId:     popupRoot.id || "album-info",
  };
  // Clear the per-track context so the popup knows it's in album mode.
  window._sdSuggestForTrack = null;
  const q = [albumArtist ? `"${albumArtist}"` : "", albumTitle ? `"${albumTitle}"` : ""]
    .filter(Boolean).join(" ");
  if (typeof window.openYoutubePopup === "function") {
    window.openYoutubePopup(q);
  } else if (typeof window._sdLoadModule === "function") {
    window._sdLoadModule("/youtube.js").then(() => {
      window.openYoutubePopup?.(q);
    });
  }
}
window._trackYtOpenAlbumSuggest = _trackYtOpenAlbumSuggest;

// Admin: delete a track override. Confirms, hits the admin endpoint
// using the override's ACTUAL scope (master or release) — not the
// popup's scope. This matters when a master popup is showing a
// release-scope override (or vice versa) — admin needs the delete to
// land regardless of which view they clicked from.
async function _trackYtAdminDelete(el) {
  if (!window._isAdmin) return;
  const pos = el.dataset.pos || "";
  const row = el.closest(".track");
  const popup = el.closest(".album-popup, .modal-content, #album-info, #version-info") || document;
  if (!pos) return;
  const popupRoot = popup.id ? popup : document.getElementById("album-info") || document.getElementById("version-info");
  const popupReleaseId = popupRoot?.dataset?.releaseId || "";
  const popupMasterId  = popupRoot?.dataset?.masterId  || "";
  const popupIsMaster  = popupRoot?.dataset?.entityType === "master";

  // Look up the override's ACTUAL scope from the in-memory cache. The
  // map merged master + release rows under the same position key, so
  // its release_id / release_type tell us where the row really lives.
  let overrideScopeType = popupIsMaster ? "master" : "release";
  let overrideScopeId   = popupIsMaster ? popupMasterId || popupReleaseId : popupReleaseId;
  // Try to find the cached override entry — check master cache key
  // first, then release cache key.
  const masterCacheKey  = `master:${popupMasterId || (popupIsMaster ? popupReleaseId : "")}`;
  const releaseCacheKey = `release:${popupReleaseId}`;
  const masterMap  = _trackYtOverridesCache.get(masterCacheKey)?.byPosition;
  const releaseMap = _trackYtOverridesCache.get(releaseCacheKey)?.byPosition;
  const ov = (releaseMap && releaseMap.get(String(pos))) || (masterMap && masterMap.get(String(pos))) || null;
  if (ov && ov.release_type && ov.release_id) {
    overrideScopeType = ov.release_type;
    overrideScopeId   = String(ov.release_id);
  }
  if (!overrideScopeId) return;

  if (!confirm(`Remove the user-suggested YouTube video for track "${row?.querySelector('.track-title-link')?.textContent || pos}"?`)) return;
  try {
    const r = await apiFetch("/api/admin/track-yt", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        releaseId:     overrideScopeId,
        releaseType:   overrideScopeType,
        trackPosition: pos,
      }),
    });
    if (!r.ok) throw new Error(`delete failed (${r.status})`);
    // Wipe the cache and re-fetch using the popup's full scope so the
    // re-apply repopulates from both master + release rows.
    _trackYtOverridesCache.clear();
    const targetId = popupRoot?.id || popup.id || "album-info";
    await _trackYtKickFetchAndApply(targetId, popupMasterId, popupIsMaster ? "" : popupReleaseId, popupIsMaster);
    if (typeof showToast === "function") showToast("Override removed", "info");
  } catch (e) {
    if (typeof showToast === "function") showToast(e?.message || "Could not remove override", "error");
  }
}
window._trackYtAdminDelete = _trackYtAdminDelete;

// ▶ in the tracklist heading: play the first playable track immediately
// AND queue the rest into the cross-source play queue (in append mode
// so they play after whatever's already queued). Clears the per-album
// _videoQueue afterward so auto-advance only runs through the
// cross-source queue (otherwise both would advance and tracks would
// double-up).
// Discogs album modal: ▶ Play album. Routes through the shared
// queueAddAlbumOrPlay so the behavior matches the archive popup —
// already-queued albums jump in place, otherwise queueAdd at head +
// play first. Existing queue continues after.
async function playAlbumAndQueue(triggerEl, _firstUrl) {
  const scope = triggerEl?.closest("#album-info, #version-info, .tracklist") || document;
  const rows  = scope.querySelectorAll(".queue-add-icon[data-yt-url]");
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
    el.classList.add("queued");
  });
  if (!items.length) return;
  if (typeof queueAddAlbumOrPlay === "function") {
    await queueAddAlbumOrPlay(items, { mode: "play" });
    // Drop the per-album _videoQueue so the cross-source queue is the
    // sole driver of auto-advance — otherwise playNextVideo would
    // advance via both, duplicating tracks.
    window._videoQueue = [];
    window._videoQueueIndex = 0;
  }
}
window.playAlbumAndQueue = playAlbumAndQueue;

function openVideo(event, url) {
  if (event) { event.preventDefault(); event.stopPropagation(); }
  // Playback needs a network — YouTube streams from their CDN, LOC
  // streams from loc.gov / archive.org. While offline the iframe
  // can't fetch the new video and silently keeps showing whichever
  // track was loaded last (causing the "queue updates but old track
  // keeps playing" symptom). Refuse the click cleanly with a toast
  // so the user knows what's going on.
  if (!navigator.onLine) {
    if (typeof showToast === "function") showToast("Playback needs a connection", "error");
    return;
  }
  // Cross-source queue interrupt hook. If the queue has items, the
  // new track gets pushed to head + marked playing so the queue
  // continues from there when this track ends. We gather title /
  // artist / album from the clicked track row's data-* attrs (set
  // by the album modal renderer); falls back to URL-only if there
  // aren't any (programmatic openVideo calls from the URL bootstrap).
  if (typeof window._queueOnExternalPlay === "function") {
    const videoId = (typeof extractYouTubeId === "function") ? extractYouTubeId(url) : "";
    if (videoId) {
      const trackEl = event?.target?.closest?.(".track-link");
      window._queueOnExternalPlay({
        source: "yt",
        externalId: videoId,
        data: {
          title:       trackEl?.dataset?.track       || "",
          artist:      trackEl?.dataset?.artist      || "",
          albumTitle:  trackEl?.dataset?.album       || "",
          // Release context flows into the queue entry's data so that
          // when the queue auto-advances later (no DOM context, no
          // ?op= URL), _queuePlayItem can hand it back via the queue
          // dispatch meta and the disc icon ("Open this album") still
          // links to the right release.
          releaseType: trackEl?.dataset?.releaseType || "",
          releaseId:   trackEl?.dataset?.releaseId   || "",
        },
      });
    } else {
      window._queueOnExternalPlay();
    }
  }
  // Starting YouTube playback should stop the LOC audio bar so we
  // don't play both simultaneously. _locPlay already does the reverse.
  try { if (typeof _locClosePlayer === "function") _locClosePlayer(); } catch {}
  ensureYTAPI();
  const id = extractYouTubeId(url);
  if (!id) { window.open(url, "_blank", "noopener"); return; }
  // Scope queue to the popup container the clicked track belongs to,
  // so we don't mix tracks from different albums.
  //
  // Queue-driven plays (cross-source queue advancing to a YT item)
  // pass meta via window._queueDispatchYtMeta because there's no
  // .track-link DOM to scrape — without this hook the title showed
  // the wrong track (whatever was at index 0 of any open album
  // popup's track list).
  const clickedEl = event?.target?.closest?.(".track-link") || event?.target;
  // Read AND clear the queue-meta handoff slot up front. Reading then
  // deleting unconditionally prevents leaks: if a second openVideo
  // fires while a previous queue dispatch's meta is still set, we
  // don't apply yesterday's title to today's video.
  const queueMeta = window._queueDispatchYtMeta;
  delete window._queueDispatchYtMeta;
  // trackLinks + container are set in the DOM-scrape branch and
  // consumed below (clickedIdx lookup, _videoQueueContainerId).
  // Hoist to function scope so the queue-driven branch (where they
  // stay at their defaults) doesn't trigger ReferenceError.
  let trackLinks = [];
  let container = null;
  if (queueMeta) {
    window._videoQueue     = [url];
    window._videoQueueMeta = [queueMeta];
  } else {
    container = clickedEl?.closest?.("#album-info, #version-info")
      || (document.getElementById("version-overlay")?.classList.contains("open") ? document.getElementById("version-info") : null)
      || (document.getElementById("modal-overlay")?.classList.contains("open") ? document.getElementById("album-info") : null)
      || document;
    trackLinks = [...container.querySelectorAll(".track-link[data-video]")];
    window._videoQueue      = trackLinks.map(a => a.dataset.video);
    window._videoQueueMeta  = trackLinks.map(a => ({
      track:  a.dataset.track  || "",
      album:  a.dataset.album  || "",
      artist: a.dataset.artist || "",
    }));
  }
  // Use the clicked element's position in the list (not indexOf, which fails with duplicate URLs)
  const clickedTrack = event?.target?.closest?.(".track-link");
  const clickedIdx = clickedTrack ? trackLinks.indexOf(clickedTrack) : -1;
  window._videoQueueIndex = clickedIdx >= 0 ? clickedIdx : window._videoQueue.indexOf(url);
  if (window._videoQueueIndex === -1) window._videoQueueIndex = 0;
  // Remember which container's tracklist owns the queue so highlightPlayingTrack
  // can light up the exact clicked row even when several rows share the same
  // URL (findVideo() can fuzzy-match neighbouring tracks to the same video) or
  // when both an album and version popup are open at once.
  window._videoQueueContainerId = container?.id || "";
  // Remember the release this track belongs to so the disc icon
  // ("Open this album") can reopen it. Priority order:
  //   1. Queue dispatch meta — auto-advance from the cross-source queue
  //   2. Clicked track row's data-release-* attrs — most reliable
  //   3. Current ?op= URL param — works when modal is still open
  //
  // If the call has clear context (queue dispatch OR a real click) we
  // OVERWRITE _playerReleaseId — including clearing it when the new
  // track lacks release context, so the disc icon doesn't keep pointing
  // at the previous album as the queue advances through tracks. We
  // only fall back to "leave previous untouched" for the bootstrap
  // case (no event, no queueMeta) where preserving the prior album
  // link is preferable to losing it.
  // Each openVideo is a fresh track — wipe the cached release ID so
  // the disc icon's click handler does a fresh search if we can't
  // populate it from explicit context. If we DO have context
  // (queueMeta / track-link / ?op= URL), set it and skip the search.
  window._playerReleaseType = null;
  window._playerReleaseId   = null;
  window._playerReleaseUrl  = null;
  let _rType = "", _rId = "";
  if (queueMeta?.releaseType && queueMeta?.releaseId) {
    _rType = queueMeta.releaseType; _rId = String(queueMeta.releaseId);
  } else if (clickedEl?.dataset?.releaseType && clickedEl?.dataset?.releaseId) {
    _rType = clickedEl.dataset.releaseType; _rId = String(clickedEl.dataset.releaseId);
  } else {
    // Auto-heal for queue rows that were saved before release-context
    // existed: scan any currently-open album popup for a track-link
    // whose data-video matches the URL we're loading. If found,
    // borrow its release attrs and ALSO write them back to the queue
    // entry's data so subsequent plays carry the info. This means
    // "open the album modal once and play through the queue" backfills
    // every old row in that album.
    try {
      const matches = document.querySelectorAll(`.track-link[data-video="${url.replace(/"/g, '\\"')}"][data-release-id]`);
      for (const m of matches) {
        if (m.dataset.releaseType && m.dataset.releaseId) {
          _rType = m.dataset.releaseType;
          _rId   = String(m.dataset.releaseId);
          // Backfill the queue row server-side so next time we dispatch
          // this track we don't have to scan the DOM. Fire-and-forget;
          // failure just leaves the queue row stale (today's behaviour).
          if (typeof window._queueBackfillReleaseInfo === "function") {
            try {
              window._queueBackfillReleaseInfo(id, {
                releaseType: _rType,
                releaseId:   _rId,
              });
            } catch {}
          }
          break;
        }
      }
    } catch {}
  }
  if (!_rType && !_rId && !window._sdFirstOpenVideoDone) {
    // URL fallback ONLY on the very first openVideo of the page —
    // the bootstrap path where ?vp= / ?op= were set in the same
    // share-link as ?vd= and genuinely refer to the playing track.
    // Subsequent calls (queue auto-advance, modal-driven plays
    // after the user opened other albums) must NOT consult these
    // URL params — the user may have opened a different album in
    // the modal since, in which case ?op= / ?vp= now point to
    // something unrelated to the playing track.
    const params  = new URLSearchParams(location.search);
    const opParam = params.get("op") || params.get("vp");
    if (opParam && opParam.includes(":")) {
      _rType = opParam.slice(0, opParam.indexOf(":"));
      _rId   = opParam.slice(opParam.indexOf(":") + 1);
    }
  }
  window._sdFirstOpenVideoDone = true;
  if (_rType && _rId) {
    window._playerReleaseType = _rType;
    window._playerReleaseId   = _rId;
    window._playerReleaseUrl  = `https://www.discogs.com/${_rType}/${_rId}`;
  }
  setVideoUrl(id);
  const mp = document.getElementById("mini-player");
  mp.classList.add("open");
  _setPlayerEngine("yt");
  document.body.classList.add("player-open");
  loadYTVideo(id);
  updateVideoNavButtons();
}

function videoPrev() {
  const queue = window._videoQueue ?? [];
  let prev = (window._videoQueueIndex ?? 0) - 1;
  while (prev >= 0) {
    const id = extractYouTubeId(queue[prev]);
    if (id) {
      window._videoQueueIndex = prev;
      setVideoUrl(id);
      loadYTVideo(id);
      updateVideoNavButtons();
      return;
    }
    prev--;
  }
}

function playNextVideo() {
  // Cross-source queue takes precedence over the per-album internal
  // _videoQueue: a user-curated queue should drive auto-advance, falling
  // back to the album's track list only when the queue is empty.
  if (typeof _queuePlayNext === "function") {
    Promise.resolve(_queuePlayNext()).then(handled => {
      if (handled) return;
      _playNextVideoInternal();
    }).catch(() => _playNextVideoInternal());
    return;
  }
  _playNextVideoInternal();
}
function _playNextVideoInternal() {
  const queue = window._videoQueue ?? [];
  let next = (window._videoQueueIndex ?? -1) + 1;
  while (next < queue.length) {
    const id = extractYouTubeId(queue[next]);
    if (id) {
      window._videoQueueIndex = next;
      setVideoUrl(id);
      loadYTVideo(id);
      updateVideoNavButtons();
      return;
    }
    next++;
  }
  // No more tracks in queue
  updatePlayerStatus("ended");
  updateVideoNavButtons();
}

function onVideoEnded() {
  console.debug("[onVideoEnded]", {
    queueRepeat: typeof window._queueGetRepeat === "function" ? window._queueGetRepeat() : "n/a",
    ytRepeat: _ytRepeat,
    videoQueueIdx: window._videoQueueIndex,
    videoQueueLen: (window._videoQueue ?? []).length,
  });
  // Cross-source repeat-one (toggled in the queue drawer) takes
  // precedence over the per-album _ytRepeat setting — it's the
  // user's most recent intent. Replay without advancing the queue.
  if (typeof window._queueGetRepeat === "function" && window._queueGetRepeat() === "one" && ytPlayer) {
    try { ytPlayer.seekTo(0); ytPlayer.playVideo(); } catch {}
    return;
  }
  if (_ytRepeat === "one" && ytPlayer) {
    ytPlayer.seekTo(0);
    ytPlayer.playVideo();
    return;
  }
  if (_ytRepeat === "album") {
    const queue = window._videoQueue ?? [];
    let next = (window._videoQueueIndex ?? -1) + 1;
    // Try next track; if at end, loop back to first
    while (next < queue.length) {
      const id = extractYouTubeId(queue[next]);
      if (id) { window._videoQueueIndex = next; setVideoUrl(id); loadYTVideo(id); updateVideoNavButtons(); return; }
      next++;
    }
    // Wrap to beginning
    for (let i = 0; i < queue.length; i++) {
      const id = extractYouTubeId(queue[i]);
      if (id) { window._videoQueueIndex = i; setVideoUrl(id); loadYTVideo(id); updateVideoNavButtons(); return; }
    }
    return;
  }
  playNextVideo();
}

// Toggle YouTube playback from the mini-player bar. Exposed on window so the
// inline onclick in index.html can find it. Mirrors the YT IFrame API states:
// 1=playing → pause, anything else → play (covers paused/buffering/cued/ended).
function toggleVideoPause() {
  if (!ytPlayer || typeof ytPlayer.getPlayerState !== "function") return;
  let state = -1;
  try { state = ytPlayer.getPlayerState(); } catch {}
  console.debug("[toggleVideoPause]", { state, hasPlayed: _ytHasPlayed });
  // YT.PlayerState: -1=unstarted, 0=ended, 1=playing, 2=paused,
  // 3=buffering, 5=cued.
  try {
    if (state === 1 || state === 3) {
      ytPlayer.pauseVideo();
      return;
    }
    if (state === 2) {
      ytPlayer.playVideo();
      return;
    }
    // States -1 (unstarted) and 5 (cued):
    //   - If the video has never played, this is autoplay-blocked
    //     (browser refused to start without a user gesture). The
    //     click IS the gesture — start playback.
    //   - If the video HAS played and we're now in -1/5/0, calling
    //     playVideo() would restart from frame zero (the "pause
    //     click restarts the track" bug). No-op in that case.
    if (!_ytHasPlayed && (state === -1 || state === 5)) {
      ytPlayer.playVideo();
    }
  } catch {}
}

// Reflect the current play/pause state on the mini-player toggle button so
// the icon and tooltip stay in sync with what the YouTube player is doing.
function syncPlayPauseBtn(state) {
  const btn = document.getElementById("mini-playpause");
  if (!btn) return;
  const isPlaying = state === "playing" || state === "buffering";
  btn.textContent = isPlaying ? "\u23F8" : "\u25B6";
  btn.title = isPlaying ? "Pause" : "Play";
  // Mirror the play/pause state to the OS media session so the
  // lock-screen / notification / Bluetooth-headset controls show
  // the right icon and fire the right action when tapped.
  if ("mediaSession" in navigator) {
    try {
      navigator.mediaSession.playbackState =
        state === "playing"   ? "playing"
      : state === "buffering" ? "playing"
      : state === "paused"    ? "paused"
      :                         "none";
    } catch {}
  }
}

// ── Media Session ──────────────────────────────────────────────────────
// Hooks the OS-level media controls (Android lock-screen / notification,
// macOS Now Playing, Bluetooth-headset prev/play/next, in-car infotainment).
// Only fires for browsers that implement the API; degrades silently
// elsewhere. Called from both engines whenever a track changes:
//   - YT engine: on state=1 in onStateChange (_createYTPlayer)
//   - LOC engine: on _locPlay start (loc.js)
// Action handlers route to the existing playerTogglePause / playerPrev /
// playerNext / playerStop entry points so the OS controls hit the same
// code paths as the in-page buttons.
function _mediaSessionUpdate(meta) {
  if (!("mediaSession" in navigator)) return;
  try {
    const title  = meta?.title  || "Unknown track";
    const artist = meta?.artist || "";
    const album  = meta?.album  || "";
    const art    = meta?.artwork
      ? [
          { src: meta.artwork, sizes: "96x96",   type: "image/jpeg" },
          { src: meta.artwork, sizes: "192x192", type: "image/jpeg" },
          { src: meta.artwork, sizes: "256x256", type: "image/jpeg" },
          { src: meta.artwork, sizes: "384x384", type: "image/jpeg" },
          { src: meta.artwork, sizes: "512x512", type: "image/jpeg" },
        ]
      : [];
    navigator.mediaSession.metadata = new MediaMetadata({
      title, artist, album, artwork: art,
    });
  } catch (e) {
    console.warn("[mediaSession] metadata set failed:", e);
  }
}
function _mediaSessionBindActions() {
  if (!("mediaSession" in navigator)) return;
  if (window._sdMediaSessionBound) return;
  window._sdMediaSessionBound = true;
  const safe = (fn) => { try { fn?.(); } catch (e) { console.warn("[mediaSession]", e); } };
  try {
    navigator.mediaSession.setActionHandler("play",           () => safe(window.playerTogglePause));
    navigator.mediaSession.setActionHandler("pause",          () => safe(window.playerTogglePause));
    navigator.mediaSession.setActionHandler("previoustrack",  () => safe(window.playerPrev));
    navigator.mediaSession.setActionHandler("nexttrack",      () => safe(window.playerNext));
    navigator.mediaSession.setActionHandler("stop",           () => safe(window.playerStop));
    navigator.mediaSession.setActionHandler("seekbackward", (details) => {
      _seekRelativeSec(-(details?.seekOffset || 10));
    });
    navigator.mediaSession.setActionHandler("seekforward", (details) => {
      _seekRelativeSec(+(details?.seekOffset || 10));
    });
    navigator.mediaSession.setActionHandler("seekto", (details) => {
      if (typeof details?.seekTime !== "number") return;
      const p = _playerReadProgress?.();
      if (!p?.duration) return;
      _playerSeekToFraction?.(details.seekTime / p.duration);
    });
  } catch (e) {
    console.warn("[mediaSession] action bind failed:", e);
  }
}
function _seekRelativeSec(deltaSec) {
  const p = _playerReadProgress?.();
  if (!p?.duration) return;
  const target = Math.max(0, Math.min(p.duration, (p.current || 0) + deltaSec));
  _playerSeekToFraction?.(target / p.duration);
}
// Push the current scrub position to the media session so lock-screen
// scrubbers and Bluetooth seek controls work. Called from the
// progress-update loop (_updateMiniProgress) for both engines.
function _mediaSessionUpdatePosition() {
  if (!("mediaSession" in navigator) || !navigator.mediaSession.setPositionState) return;
  const p = _playerReadProgress?.();
  if (!p?.duration || !Number.isFinite(p.duration)) return;
  try {
    navigator.mediaSession.setPositionState({
      duration: p.duration,
      position: Math.max(0, Math.min(p.duration, p.current || 0)),
      playbackRate: 1,
    });
  } catch {}
}
// Bind action handlers as soon as the script loads (idempotent).
_mediaSessionBindActions();
window._mediaSessionUpdate         = _mediaSessionUpdate;
window._mediaSessionBindActions    = _mediaSessionBindActions;
window._mediaSessionUpdatePosition = _mediaSessionUpdatePosition;

// Disc-icon click: open the source-of-truth page for the currently-
// playing track. Engine-aware:
//   YT  → Discogs release/master modal (needs _playerReleaseId)
//   LOC → Library of Congress info popup (needs _locNowPlaying)
// The icon is ALWAYS visible while an engine is active — but dimmed
// (opacity 0.35) when we don't have the info to open anything. A
// click on the dimmed icon shows a brief toast explaining why.
function openPlayerRelease() {
  if (window._currentEngine === "loc") {
    if (typeof _locOpenFromBar === "function") {
      try { _locOpenFromBar(); return; } catch {}
    }
    if (typeof showToast === "function") {
      showToast("No info available for this track", "info", 2500);
    }
    return;
  }
  let rType = window._playerReleaseType;
  let rId   = window._playerReleaseId;
  let rUrl  = window._playerReleaseUrl;
  // Fallback: if the player-release globals got dropped (e.g. mid-
  // queue race or a stale optimistic insert), try the currently-
  // playing queue entry's data — every queue row carries the
  // releaseType/releaseId from when it was added. This keeps the disc
  // icon working for full-album / queue-managed tracks even when the
  // direct openVideo path's setters were stomped or never ran.
  if ((!rType || !rId) && typeof window._queueGetCurrentEntry === "function") {
    const cur = window._queueGetCurrentEntry();
    if (cur?.data?.releaseType && cur?.data?.releaseId) {
      rType = cur.data.releaseType;
      rId   = String(cur.data.releaseId);
      rUrl  = `https://www.discogs.com/${rType}/${rId}`;
      // Heal the player-release globals so subsequent disc-icon
      // clicks don't re-do the lookup.
      window._playerReleaseType = rType;
      window._playerReleaseId   = rId;
      window._playerReleaseUrl  = rUrl;
    }
  }
  if (rType && rId) {
    openModal(null, rId, rType, rUrl);
    return;
  }
  // YT track without a known Discogs release. Tell the user instead
  // of silently doing nothing — the dim icon already telegraphs the
  // "no link" state visually.
  if (typeof showToast === "function") {
    showToast("Album info not available for this track", "info", 2500);
  }
}
window.openPlayerRelease = openPlayerRelease;

function sharePlayerUrl() {
  const u = new URL(window.location.origin);
  const cur = new URLSearchParams(location.search);
  // Include open popup if one is showing
  const op = cur.get("op");
  if (op) u.searchParams.set("op", op);
  const vd = cur.get("vd");
  const vp = cur.get("vp");
  if (vd) u.searchParams.set("vd", vd);
  if (vp) u.searchParams.set("vp", vp);
  navigator.clipboard.writeText(u.toString())
    .then(() => showToast("Share link copied"))
    .catch(() => showToast("Could not copy link", "error"));
}

function closeVideo() {
  _ytSession++;                             // invalidate any pending callbacks
  if (_ytPollId) { clearInterval(_ytPollId); _ytPollId = null; }
  if (_ytLoadTimer) { clearTimeout(_ytLoadTimer); _ytLoadTimer = null; }
  if (typeof _stopYtProgressLoop === "function") _stopYtProgressLoop();
  // Reset the progress strip so it doesn't linger at the closed video's
  // last position when a new track loads (LOC's loadedmetadata event
  // already covers re-init for that engine).
  if (typeof _updateMiniProgress === "function") {
    setTimeout(_updateMiniProgress, 0);
  }
  const mp = document.getElementById("mini-player");
  mp.classList.remove("open", "expanded");
  // Only clear the engine if we're actually closing a YT session (not
  // mid-handoff to LOC). _setPlayerEngine(null) leaves the bar generic.
  if (window._currentEngine === "yt") _setPlayerEngine(null);
  document.body.classList.remove("player-open");
  if (ytPlayer && typeof ytPlayer.stopVideo === "function") {
    ytPlayer.stopVideo();
    ytPlayer.destroy();
    ytPlayer = null;
  }
  document.getElementById("video-player").innerHTML = "";
  const titleEl = document.getElementById("mini-player-title");
  if (titleEl) titleEl.textContent = "Not playing";
  updatePlayerStatus("");
  const u = new URL(window.location.href);
  u.searchParams.delete("vd");
  u.searchParams.delete("vp");
  history.replaceState({}, "", u.toString());
  // Hide album + share buttons via the class-based flag
  const mpClose = document.getElementById("mini-player");
  if (mpClose) mpClose.classList.remove("has-release");
  // Clear playing track highlights
  document.querySelectorAll(".track-link.now-playing").forEach(el => el.classList.remove("now-playing"));
  window._playerReleaseType = null;
  window._playerReleaseId = null;
  window._playerReleaseUrl = null;
  window._videoQueue = [];
  window._videoQueueMeta = [];
  window._videoQueueIndex = -1;
}

function extractYouTubeId(url) {
  try {
    const u = new URL(url);
    if (u.hostname === "youtu.be") return u.pathname.slice(1);
    return u.searchParams.get("v");
  } catch { return null; }
}

// video-overlay was replaced by mini-player — no click-to-close needed

document.addEventListener("keydown", e => {
  if (e.key === "Escape") {
    closeVideo();
    closeModal();
    closeBioFull();
  }
});

// ── eBay search link helper ──────────────────────────────────────────────
// Builds a small colored eBay logo link that searches eBay Music for artist/title/catno.
// `standalone` = true when rendered alone (no leading margin on the link).
// The Wikipedia dropdown renders immediately after, sharing the same
// artist/title + an optional label arg for the third Wikipedia search option.
function renderEbayLink(artist, title, catno, standalone = false, label = "") {
  const q = [artist, title, catno].filter(Boolean).join(" ").trim();
  if (!q) return "";
  const url = `https://www.ebay.com/sch/i.html?_nkw=${encodeURIComponent(q)}&_sacat=11233`;
  const ml = standalone ? "" : "margin-left:0.5rem;";
  return `<a href="${url}" target="_blank" rel="noopener nofollow" title="Search eBay Music for: ${escHtml(q)}" style="text-decoration:none;${ml}font-size:0.72rem;font-weight:900;font-family:'Helvetica Neue',Arial,Helvetica,sans-serif;letter-spacing:-0.04em;font-style:italic;vertical-align:baseline"><span style="color:#e53238">e</span><span style="color:#0064d2">b</span><span style="color:#f5af02">a</span><span style="color:#86b817">y</span><span style="color:#666;font-weight:400;font-style:normal;margin-left:0.1em">↗</span></a>`;
}

// ── Album info panel ──────────────────────────────────────────────────────
function renderAlbumInfo(d, searchResult, discogsUrl = "", stats = null, targetId = "album-info") {
  const el = document.getElementById(targetId);

  const rawTitle = d.title ?? searchResult.title ?? "";
  const title    = rawTitle.includes(" - ") && !d.title
                   ? rawTitle.slice(rawTitle.indexOf(" - ") + 3) : rawTitle;
  // Discogs appends " (N)" to artist/label names when multiple entities
  // share a name. In the album popup we KEEP the disambiguator in the
  // displayed text and pass it through to the search links so clicking
  // "Tommy Tucker (3)" finds that specific Tommy Tucker rather than
  // matching every artist with that base name. The card grid still
  // strips for cleaner thumbnails — see search.js renderCard.
  const artistNames = (d.artists ?? []).map(a => a.name);
  const artists  = artistNames.length ? artistNames
                   : ((searchResult.title ?? "").split(" - ")[0] ? [(searchResult.title ?? "").split(" - ")[0]] : []);
  // Same data with Discogs IDs preserved — used so the admin "+ add to
  // Blues DB" icon next to each artist name has the ID it needs.
  const artistEntries = (d.artists ?? []).length
    ? (d.artists ?? []).map(a => ({ id: a.id, name: a.name }))
    : artists.map(n => ({ id: null, name: n }));
  const year     = d.year ?? searchResult.year ?? "";
  const labelNames = (d.labels ?? []).map(l => l.name).slice(0, 2);
  if (!labelNames.length) labelNames.push(...(searchResult.label ?? []).slice(0, 2));
  const labels   = labelNames.join(", ");
  const genres   = [...(d.genres ?? []), ...(d.styles ?? [])].slice(0, 4).join(" · ");
  const country  = d.country ?? searchResult.country ?? "";
  const allImages = (d.images ?? []).map(i => i.uri).filter(Boolean);
  if (allImages.length === 0 && searchResult.cover_image) allImages.push(searchResult.cover_image);
  const img      = allImages[0] ?? "";
  const released    = d.released_formatted ?? d.released ?? "";
  const formats     = (d.formats ?? []).map(f =>
    [f.name, ...(f.descriptions ?? [])].filter(Boolean).join(" · ")
  ).join("; ") || (searchResult.format ?? []).join(" · ");
  const creditItems = (d.extraartists ?? [])
    .map(a => {
      // Display + search both keep the original disambiguated name
      // (e.g. "Smith (4)") so popup search links resolve the exact
      // contributor instead of merging with same-named artists.
      // Credit name uses the unified lookup popup (SeaDisco / collection
      // / YouTube / Wiki / LOC). The blues-DB add icon stays inline as
      // an admin mutation, not a search.
      const nameEl = entityLookupLinkHtml("artist", a.name, { className: "credit-name", title: `Lookup options for ${a.name}` });
      const searchIcon = bluesAddIcon(a.id, a.name);
      return `<span class="credit-item">${nameEl}${a.role ? ` <span class="credit-role">(${escHtml(a.role)})</span>` : ""}${searchIcon}</span>`;
    });
  const notes       = d.notes ? stripDiscogsMarkup(d.notes) : "";
  const catno     = (d.labels ?? [])[0]?.catno ?? "";
  const releaseId = d.id ?? searchResult.id ?? "";
  const typeName = targetId === "version-info"
    ? "Version"
    : searchResult.type === "master" ? "Master" : searchResult.type === "release" ? "Release" : "";
  const typeLabel = typeName && releaseId ? `${typeName}: ${releaseId}` : typeName;

  // Store rich card-shaped data for favorites (popup may not have itemCache entry)
  const entityType = targetId === "version-info" ? "release" : (searchResult.type || "release");
  if (!window._popupCardData) window._popupCardData = {};
  window._popupCardData[String(releaseId)] = {
    id: releaseId,
    type: entityType,
    title: artists.length ? `${artists.join(", ")} - ${title}` : title,
    cover_image: img,
    uri: searchResult.uri || `/${entityType}/${releaseId}`,
    year: String(year),
    country: country,
    genre: [...(d.genres ?? [])].slice(0, 2),
    label: labelNames.slice(0, 2),
    format: (d.formats ?? []).map(f => f.name).slice(0, 3),
    catno: catno,
  };

  const videoMap = new Map();
  for (const v of (d.videos ?? [])) {
    if (v.title && v.uri) videoMap.set(v.title.toLowerCase(), v.uri);
  }
  // Crowd-sourced overrides — keyed by track position. Read from the
  // module-level cache (populated by _trackYtFetchOverrides). On the
  // first popup open for an album the cache is cold so this returns
  // an empty map; the deferred DOM patch in _trackYtApplyToDom then
  // injects the affordances once the fetch lands.
  const _overrideMap = _trackYtReadCache(d.master_id, releaseId, searchResult.type === "master") || new Map();
  function findVideo(trackTitle, trackPosition) {
    const unavailable = window._sdYtUnavailable;
    // 1) Discogs-provided videos win — they're the canonical source.
    //    Skip any whose videoId is in the global unavailable set so
    //    the track reads as "missing" and contributes to the album-
    //    suggest count + popup.
    const tl = trackTitle.toLowerCase();
    for (const [vt, uri] of videoMap) {
      if (vt.includes(tl) || tl.includes(vt)) {
        if (unavailable && unavailable.size) {
          const vid = _sdYtIdFromUrl(uri);
          if (vid && unavailable.has(vid)) continue;
        }
        return uri;
      }
    }
    // 2) Gap-fill via crowd-sourced overrides keyed by track position.
    //    The server-side query already excludes overrides whose video
    //    is unavailable, but check defensively in case the cache here
    //    is stale relative to the unavailable set.
    if (trackPosition) {
      const ov = _overrideMap.get(String(trackPosition));
      if (ov && ov.video_id && (!unavailable || !unavailable.has(ov.video_id))) {
        return `https://www.youtube.com/watch?v=${ov.video_id}`;
      }
    }
    return null;
  }

  const identifierTypes = ["Barcode","Matrix / Runout","ASIN","Catalog Number","Label Code"];
  const identifierGroups = {};
  for (const i of (d.identifiers ?? [])) {
    if (i.value && identifierTypes.includes(i.type)) {
      identifierGroups[i.type] = identifierGroups[i.type]
        ? identifierGroups[i.type] + ", " + i.value
        : i.value;
    }
  }

  // Extract special identifier rows for repositioning in the detail grid
  const labelCodeRow = identifierGroups["Label Code"]
    ? `<span class="detail-label">Label Code</span><span>${escHtml(identifierGroups["Label Code"])}</span>` : "";
  const matrixRow = identifierGroups["Matrix / Runout"]
    ? (() => { const val = identifierGroups["Matrix / Runout"]; return `<span class="detail-label">Matrix / Runout</span><span class="matrix-runout" style="color:#7ec87e;cursor:pointer" onclick="navigator.clipboard.writeText('${escHtml(val.replace(/'/g, "\\'"))}');this.dataset.copied='true';setTimeout(()=>this.dataset.copied='',1200)" title="Click to copy">${escHtml(val)}</span>`; })() : "";

  // Remaining identifiers (exclude Label Code and Matrix / Runout — placed separately)
  const otherIdentifierTypes = ["Barcode","ASIN","Catalog Number"];
  const identifierRows = otherIdentifierTypes
    .filter(t => identifierGroups[t])
    .map(t => {
      if (t === "Catalog Number") {
        const vals = identifierGroups[t].split(", ");
        const linked = vals.map(v => {
          const esc = escHtml(v.replace(/'/g, "\\'"));
          return `<a href="#" class="modal-internal-link catno-link" onclick="event.preventDefault();closeModal();document.getElementById('query').value='${esc}';toggleAdvanced(false);document.querySelector('input[name=\\'result-type\\'][value=\\'\\']').checked=true;doSearch(1)" title="Search for this catalog number">${escHtml(v)}</a> <a href="#" class="catno-collection-search" onclick="event.preventDefault();searchCollectionFor('cw-query','${esc}')" title="Search your collection for ${escHtml(v)}">⌕</a>`;
        }).join(", ");
        return `<span class="detail-label">${escHtml(t)}</span><span>${linked}</span>`;
      }
      return `<span class="detail-label">${escHtml(t)}</span><span>${escHtml(identifierGroups[t])}</span>`;
    })
    .join("");

  const companyTypes = ["Pressed By","Manufactured By","Mastered At","Recorded At","Mixed At","Distributed By","Licensed From","Phonographic Copyright (p)"];
  const companyGroups = {};
  for (const c of (d.companies ?? [])) {
    const t = c.entity_type_name;
    if (companyTypes.includes(t)) {
      if (!companyGroups[t]) companyGroups[t] = [];
      companyGroups[t].push(c.name);
    }
  }
  const companyRows = companyTypes
    .filter(t => companyGroups[t])
    .map(t => `<span class="detail-label">${escHtml(t)}</span><span>${escHtml(companyGroups[t].join(", "))}</span>`)
    .join("");

  const seriesLinks = (d.series ?? [])
    .filter(s => s.name)
    .map(s => {
      const label = s.catno ? `${s.name} (${s.catno})` : s.name;
      return `<a href="#" class="modal-internal-link" onclick="event.preventDefault();openSeriesBrowser(${s.id},'${escHtml(s.name.replace(/'/g, "\\'"))}')" title="Browse series: ${escHtml(s.name)}">${escHtml(label)}</a>`;
    }).join(", ");

  const isMaster = searchResult.type === "master";
  const catnoEsc = catno.replace(/'/g, "\\'");
  const detailRows = [
    labelNames.length ? `<span class="detail-label">Label</span><span>${labelNames.map(n => entityLookupLinkHtml("label", n, { className: "modal-internal-link", title: `Lookup options for ${n}` })).join(", ")}</span>` : "",
    (labels && labelCodeRow) ? labelCodeRow : "",
    (!isMaster && catno) ? `<span class="detail-label">Cat#</span><span><a href="#" class="modal-internal-link catno-link" onclick="event.preventDefault();closeModal();clearForm();document.getElementById('query').value='${escHtml(catnoEsc)}';doSearch(1)" title="Search for this catalog number">${escHtml(catno)}</a> <a href="#" class="catno-collection-search" onclick="event.preventDefault();searchCollectionFor('cw-query','${escHtml(catnoEsc)}')" title="Search your collection for ${escHtml(catno)}">⌕</a></span>` : "",
    (!isMaster && formats) ? `<span class="detail-label">Format</span><span>${escHtml(formats)}</span>` : "",
    year    ? `<span class="detail-label">Year</span><span>${escHtml(String(year))}</span>` : "",
    country ? `<span class="detail-label">Country</span><span>${escHtml(country)}</span>` : "",
    seriesLinks ? `<span class="detail-label">Series</span><span>${seriesLinks}</span>` : "",
    companyRows,
    identifierRows,
    matrixRow,
    genres  ? `<span class="detail-label">Genre</span><span>${escHtml(genres)}</span>`   : "",
  ].filter(Boolean).join("");

  const tracks = (d.tracklist ?? []).filter(t => t.type_ !== "heading");
  // Pre-compute playable count + first-playable URL so we can show
  // them in the Tracklist heading. A track is "playable" when
  // findVideo() returns a YouTube URL for its title.
  const playableUrls = tracks.map(t => findVideo(t.title || "", t.position || "")).filter(Boolean);
  const playableCount = playableUrls.length;
  const firstPlayableUrl = playableUrls[0] || "";
  // Tracks with a title but no playable URL are candidates for the
  // album-level "🎵 N missing" affordance. Threshold is ≥1 since
  // we removed the per-track suggest button — this is the only path
  // for users to contribute. The count shown here is the render-time
  // value (cache may not be populated yet); _trackYtApplyToDom
  // recounts after the override patch lands so the heading reflects
  // the true post-patch state.
  // Full-album pseudo-track: a single video that contains the entire
  // album. Stored as an override at position "ALBUM" via the existing
  // track_youtube_overrides table — same flow, sentinel position.
  // When an override exists, the row appears for everyone; when there's
  // no override, the row appears for admins (so they can suggest one).
  const fullAlbumOverride = _overrideMap.get("ALBUM");
  const fullAlbumUrl = (fullAlbumOverride && fullAlbumOverride.video_id)
    ? `https://www.youtube.com/watch?v=${fullAlbumOverride.video_id}`
    : "";
  // Show the full-album row whenever we have an override OR the user is
  // admin (and signed-in non-admin gates are already in place for the
  // album-suggest entry points). _renderFullAlbumRow is generated
  // alongside the normal track rows below.
  const fullAlbumRowVisible = !!fullAlbumUrl || !!window._isAdmin;
  // Count Full Album as "missing" only when admin (so the heading
  // "🎵 N missing" link naturally rolls in the option) AND no override
  // exists. Non-admins don't see the album-suggest entry point at all.
  const fullAlbumIsMissing = window._isAdmin && !fullAlbumUrl;
  const missingCount = tracks.filter(t => t.title && !findVideo(t.title || "", t.position || "")).length
    + (fullAlbumIsMissing ? 1 : 0);
  // Admin-only while we're throttling YouTube quota — every album-mode
  // search.list call costs 100 units and runs against the constrained
  // 10k/day project quota. Reconsider when Google approves the
  // increased-quota request.
  const albumFindMissingLink = (missingCount >= 1 && window._clerk?.user && window._isAdmin)
    ? ` <a href="#" class="tracklist-find-missing" onclick="event.preventDefault();event.stopPropagation();_trackYtOpenAlbumSuggest(this);return false" title="Search YouTube once for the whole album and assign videos to all missing tracks at once">🎵 ${missingCount} missing</a>`
    : "";
  const playableMeta = playableCount
    ? `<span class="tracklist-playable">(${playableCount}${firstPlayableUrl ? ` <a href="#" class="tracklist-play-all" onclick="event.preventDefault();event.stopPropagation();playAlbumAndQueue(this,'${firstPlayableUrl.replace(/'/g, "\\'")}')" title="Play the first track and queue the rest of the album">▶</a>` : ""}${playableCount >= 1 ? ` <a href="#" class="tracklist-queue-album" onclick="event.preventDefault();event.stopPropagation();queueAddAlbum(this)" title="Add all playable tracks to the bottom of your queue">＋</a>` : ""}${albumFindMissingLink})</span>`
    : (albumFindMissingLink ? `<span class="tracklist-playable">(${albumFindMissingLink})</span>` : "");
  const tracklistOpen = localStorage.getItem("tracklist-open") !== "false";
  const trackHTML = tracks.length ? `
    <div class="album-tracklist">
      <div class="tracklist-header">
        <div class="tracklist-heading tracklist-toggle" onclick="toggleTracklist(this)" title="Click to collapse/expand tracklist"><span class="tracklist-arrow">${tracklistOpen ? "▼" : "▶"}</span> Tracklist ${playableMeta}</div>
        <input type="text" class="tracklist-filter" placeholder="filter tracks…" oninput="filterTracks(this)" />
      </div>
      <div class="tracklist-body"${tracklistOpen ? "" : ' style="display:none"'}>
      ${(() => {
        // Full Album pseudo-row. Mirrors the regular track row layout so
        // _trackYtApplyToDom (which walks .track[data-pos] generically)
        // patches in ▶ ＋ when an override lands. Always rendered so the
        // override-fetch reconcile can reveal it on cold-cache boots —
        // the row is visually hidden for non-admins without an override
        // and unhidden by _trackYtApplyToDom once the override lands.
        const trackArtistFA = artists.length ? artists[0] : "";
        const entityType = isMaster ? "master" : "release";
        const playCellFA = fullAlbumUrl
          ? `<a class="track-play-btn track-link" href="#" data-video="${escHtml(fullAlbumUrl)}" data-track="Full album" data-album="${escHtml(title)}" data-artist="${escHtml(trackArtistFA)}" data-release-type="${escHtml(entityType)}" data-release-id="${escHtml(String(releaseId || ""))}" onclick="openVideo(event,'${fullAlbumUrl.replace(/'/g, "\\'")}')" title="Play the full album">▶</a>`
          : "";
        // data-fullalbum="1" marks this queue-add icon so queueAddAlbum
        // (the bulk-queue scan over .queue-add-icon[data-yt-url]) skips
        // it — the full-album video shouldn't be queued alongside per-
        // track ones via "Play album" / "Queue album". The single ＋
        // button on this row still works for an individual queue add.
        const queueAddFA = fullAlbumUrl
          ? ` <a href="#" class="queue-add-icon" data-fullalbum="1" data-yt-url="${escHtml(fullAlbumUrl)}" data-track="Full album" data-album="${escHtml(title)}" data-artist="${escHtml(trackArtistFA)}" data-release-type="${escHtml(entityType)}" data-release-id="${escHtml(String(releaseId || ""))}" onclick="event.preventDefault();_trackQueueAdd(this);return false" title="Add full album to play queue">＋</a>`
          : "";
        const overrideBadgeFA = fullAlbumUrl
          ? ` <span class="track-yt-override-badge" title="User-suggested full-album video">🎵</span>${
              window._isAdmin
                ? ` <a href="#" class="track-yt-admin-delete" data-pos="ALBUM" onclick="event.preventDefault();_trackYtAdminDelete(this);return false" title="Admin: remove this user-suggested video">✕</a>`
                : ""
            }`
          : "";
        // Hide the row by default when no override AND not admin —
        // _trackYtApplyToDom flips this off when an override lands so
        // non-admins on cold-cache boots still get the row revealed
        // once the override fetch reconciles.
        const hideStyle = (!fullAlbumUrl && !window._isAdmin) ? ' style="display:none"' : "";
        return `<div class="track track-fullalbum" data-pos="ALBUM"${fullAlbumUrl ? ' data-yt-override="1"' : ""}${hideStyle}>
          <span class="track-play-cell">${playCellFA}</span>
          <span class="track-pos">Full</span>
          <span class="track-title"><span class="track-title-link">Full album</span>${queueAddFA}${overrideBadgeFA}</span>
        </div>`;
      })()}
      ${tracks.map(t => {
        const trackPos = t.position || "";
        const url = findVideo(t.title || "", trackPos);
        // Was this URL provided by a crowd-sourced override? (vs. Discogs)
        // Used to mark the row so the badge / admin-delete affordance
        // renders. Only true when no Discogs video matched.
        const overrideRow = url && _overrideMap.has(String(trackPos)) && !(() => {
          const tl = (t.title || "").toLowerCase();
          for (const [vt] of videoMap) { if (vt.includes(tl) || tl.includes(vt)) return true; }
          return false;
        })();
        const trackArtist = artists.length ? artists[0] : "";
        // YouTube search supports double-quoted exact-phrase matching too —
        // quote artist + track so search results match the literal strings
        // rather than a bag-of-words. Album title stays unquoted as light
        // context for cases where the track title is generic.
        const _ytArtistQ = trackArtist ? `"${trackArtist}"` : "";
        const _ytTrackQ  = t.title ? `"${t.title}"` : "";
        const _ytAlbumQ  = title || "";
        const ytQuery = encodeURIComponent(
          [_ytArtistQ, _ytTrackQ, _ytAlbumQ].filter(Boolean).join(" ")
        );
        const trackSearchQ = ('"' + (t.title || '').trim() + '"').replace(/'/g, "\\'");
        // Play column: reserved width so rows align even with no playable URL.
        // Only the ▶ play button lives here now (no orange circle); the
        // external YouTube-search fallback was moved to the end of the
        // title cell, after the wiki W icon.
        const playCell = url
          ? `<a class="track-play-btn track-link" href="#" data-video="${escHtml(url)}" data-track="${escHtml(t.title || "")}" data-album="${escHtml(title)}" data-artist="${escHtml(trackArtist)}" data-release-type="${escHtml(entityType || "")}" data-release-id="${escHtml(String(releaseId || ""))}" onclick="openVideo(event,'${url.replace(/'/g, "\\'")}')" title="Play this track">▶</a>`
          : "";
        // Track title now opens the unified lookup popup (SeaDisco /
        // collection / YouTube / Wikipedia / LOC) instead of going
        // straight to a Discogs search. Reduces inline icon clutter —
        // the W / 🏛 / 📺 icons that used to follow the title are now
        // options in that popup.
        const titleLink = t.title
          ? entityLookupLinkHtml("track", t.title, { className: "track-title-link", trackArtist, title: `Lookup options for "${t.title}"` })
          : "";
        // searchIcon / wikiW / locL / ytSearchEnd were retired here —
        // those affordances live in the lookup popup now.
        const searchIcon = "";
        const wikiW = "";
        const locL = "";
        const ytSearchEnd = "";
        // ➕ → add this YouTube-matched track to the cross-source play
        // queue. Only renders when the row has a confirmed YT URL match
        // (otherwise there's nothing to queue from this row).
        const queueAdd = url
          ? ` <a href="#" class="queue-add-icon" data-yt-url="${escHtml(url)}" data-track="${escHtml(t.title || "")}" data-album="${escHtml(title)}" data-artist="${escHtml(trackArtist)}" data-release-type="${escHtml(entityType || "")}" data-release-id="${escHtml(String(releaseId || ""))}" onclick="event.preventDefault();_trackQueueAdd(this);return false" title="Add to play queue">＋</a>`
          : "";
        // Crowd-sourced override badge (shown only when this row's URL
        // came from track_youtube_overrides, not Discogs's videos[]).
        // The 🎵 indicates "user-contributed". Admins get a tiny ✕
        // delete affordance next to it.
        const overrideBadge = overrideRow
          ? ` <span class="track-yt-override-badge" title="User-suggested YouTube video">🎵</span>${
              window._isAdmin
                ? ` <a href="#" class="track-yt-admin-delete" data-pos="${escHtml(trackPos)}" onclick="event.preventDefault();_trackYtAdminDelete(this);return false" title="Admin: remove this user-suggested video">✕</a>`
                : ""
            }`
          : "";
        // Per-row "🎵 Suggest a video" affordance retired — replaced
        // by the album-level "🎵 N missing" link in the tracklist
        // heading, which opens a single YT search and lets the user
        // assign multiple tracks at once.
        const suggestBtn = "";
        const trackCredits = (t.extraartists ?? []).length
          ? `<div class="track-credits">${t.extraartists.map(a => {
              const nameEl = entityLookupLinkHtml("artist", a.name, { className: "credit-name", title: `Lookup options for ${a.name}` });
              const credSearchIcon = `${bluesAddIcon(a.id, a.name)}`;
              // Role parentheses come right after the name; the blues-DB
              // admin mutation (if any) trails. The \u2315 / W / \ud83c\udfdb / YT
              // icons that used to live here are now in the lookup
              // popup that opens when the credit name is clicked.
              return `${nameEl}${a.role ? ` <span class="credit-role">(${escHtml(a.role)})</span>` : ""}${credSearchIcon}`;
            }).join('<span class="credit-sep"> · </span>')}</div>`
          : "";
        return `<div class="track" data-pos="${escHtml(trackPos)}"${overrideRow ? ' data-yt-override="1"' : ""}>
          <span class="track-play-cell">${playCell}</span>
          <span class="track-pos">${escHtml(t.position || "")}</span>
          <span class="track-title">${titleLink}${searchIcon}${wikiW}${locL}${ytSearchEnd}${queueAdd}${overrideBadge}${suggestBtn}${trackCredits}</span>
          ${t.duration ? `<span class="track-dur">${escHtml(t.duration)}</span>` : ""}
        </div>`;
      }).join("")}
      </div>
    </div>` : "";

  const creditsHTML = creditItems.length ? `
    <div class="album-credits">
      <div class="credits-header">
        <div class="tracklist-heading">Credits</div>
        ${creditItems.length > 4 ? `<input type="text" class="tracklist-filter" placeholder="filter credits…" oninput="filterCredits(this)" />` : ""}
      </div>
      <div class="credits-body">${creditItems.join('<span class="credit-sep"> · </span>')}</div>
    </div>` : "";

  const metaRows = [
    creditsHTML,
    notes    ? `<div class="album-notes"><div class="tracklist-heading" style="margin-top:0.5rem">Notes</div>${escHtml(notes)}</div>` : "",
  ].filter(Boolean).join("");

  el.innerHTML = `
    <div class="album-header">
      ${img ? `<div class="album-cover-wrap">
        <img class="album-cover" src="${img}" alt="${escHtml(title)}" loading="lazy"
             onclick="openLightbox(${escHtml(JSON.stringify(allImages))},0)"
             title="${allImages.length > 1 ? `View ${allImages.length} photos` : 'View photo'}" />
        ${allImages.length > 1 ? `<div class="album-thumb-strip">${allImages.slice(1).map((u, i) =>
          `<img src="${escHtml(u)}" loading="lazy" class="album-thumb" onclick="openLightbox(${escHtml(JSON.stringify(allImages))},${i + 1})" onerror="this.style.display='none'" title="Photo ${i + 2} of ${allImages.length}">`
        ).join("")}</div>` : ""}
      </div>`
             : `<div class="album-cover-placeholder">♪</div>`}
      <div class="album-meta">
        ${typeLabel ? `<div style="display:flex;align-items:center;gap:0.4rem;margin-bottom:0.3rem"><div class="album-type-badge" style="cursor:pointer;user-select:none" onclick="navigator.clipboard.writeText('${escHtml(String(releaseId))}');this.dataset.copied='true';setTimeout(()=>this.dataset.copied='',1200)" title="Click to copy ID">${escHtml(typeLabel)}</div><button class="popup-share-inline" onclick="sharePopup(this)" title="Copy share link">share</button></div>` : ""}
        ${d._signInForMore ? `<div style="font-size:0.75rem;color:var(--muted);background:rgba(255,255,255,0.04);border-left:2px solid var(--accent);padding:0.4rem 0.6rem;border-radius:4px;margin-bottom:0.5rem">Sign in to load full release details (tracklist, credits, marketplace).</div>` : ""}
        <h2>${entityLookupLinkHtml("release", title, { className: "modal-title-link", title: `Lookup options for "${title}"` })}</h2>
        ${artistEntries.length ? `<div class="album-artist">${artistEntries.map(({ id: aId, name: n }) => `${entityLookupLinkHtml("artist", n, { className: "modal-artist-link", title: `Lookup options for ${n}` })}${bluesAddIcon(aId, n)}`).join(", ")}</div>` : ""}
        ${detailRows ? `<div class="album-detail-grid">${detailRows}</div>` : ""}
        ${(() => {
          const r = d.community?.rating;
          const have = d.community?.have;
          const want = d.community?.want;
          const parts = [];
          if (r?.count > 0) parts.push(`★ ${parseFloat(r.average).toFixed(2)} <span style="color:#555">(${r.count.toLocaleString()} ratings)</span>`);
          if (have || want) parts.push(`${(have ?? 0).toLocaleString()} have · ${(want ?? 0).toLocaleString()} want`);
          return parts.length ? `<div style="font-size:0.72rem;color:#888;margin-top:0.35rem">${parts.join('<span style="color:#444;margin:0 0.35em">·</span>')}</div>` : "";
        })()}
        ${(!isMaster && d.master_id) ? `<div style="margin-top:0.4rem"><a href="#" class="modal-internal-link" onclick="event.preventDefault();closeModal();setTimeout(()=>openModal(null,${d.master_id},'master','https://www.discogs.com/master/${d.master_id}'),100)" title="View all pressings of this release" style="font-size:0.75rem;color:#7eb8da;text-decoration:none">Master/Versions</a></div>` : ""}
        ${discogsUrl ? `<a href="${discogsUrl}" target="_blank" rel="noopener" title="Open this release on Discogs.com" style="font-size:0.75rem;color:#888;text-decoration:none;margin-top:0.25rem;display:inline-block">View on Discogs ↗</a>` : ""}
        ${stats?.numForSale > 0 && (stats?.lowestPrice != null || stats?.medianPrice != null || stats?.highestPrice != null)
          ? (() => {
              const low = stats.lowestPrice != null ? parseFloat(stats.lowestPrice).toFixed(2) : null;
              const med = stats.medianPrice != null ? parseFloat(stats.medianPrice).toFixed(2) : null;
              const high = stats.highestPrice != null ? parseFloat(stats.highestPrice).toFixed(2) : null;
              // For masters, link to listings across ALL pressings (master_id) so the
               // user lands on the full marketplace view for the work, not the listings
               // for the single main_release that the stats endpoint resolved to (which
               // can be confusing when the user has their own listing on that pressing).
               const sellUrl = isMaster
                 ? `https://www.discogs.com/sell/list?master_id=${escHtml(String(searchResult.id))}`
                 : `https://www.discogs.com/sell/list?release_id=${escHtml(String(stats.releaseId))}`;
              const count = escHtml(String(stats.numForSale));
              const dash = `<span style="color:#555;margin:0 0.15rem"> ── </span>`;
              const parts = [];
              if (low) parts.push(`<span style="color:var(--accent)">$${low}</span>`);
              if (med) parts.push(`<span style="color:#999">$${med}</span>`);
              if (high) parts.push(`<span style="color:#777">$${high}</span>`);
              const priceBar = parts.length === 1
                ? `from ${parts[0]}`
                : parts.join(dash);
              const estId = `price-est-${escHtml(String(stats.releaseId))}`;
              return `<div style="font-size:0.75rem;margin-top:0.2rem">
                <a href="${sellUrl}" target="_blank" rel="noopener" title="Browse ${count} listings on Discogs marketplace" style="color:#888;text-decoration:none">(${count}) :: ${priceBar} ↗</a>
                ${!isMaster ? `<a href="#" onclick="event.preventDefault();loadPriceEstimates('${escHtml(String(stats.releaseId))}','${estId}')" style="color:#555;text-decoration:none;margin-left:0.4rem;font-size:0.7rem" title="Show estimated prices by condition">(est)</a>${renderEbayLink(artists[0], title, catno, false, labelNames[0])}<div id="${estId}"></div>` : renderEbayLink(artists[0], title, catno, false, labelNames[0])}
              </div>`;
            })()
          : (stats?.numForSale === 0
              ? `<div style="font-size:0.75rem;color:#555;margin-top:0.2rem">Not currently available on Discogs marketplace${renderEbayLink(artists[0], title, catno, false, labelNames[0])}</div>`
              : (artists.length || title ? `<div style="font-size:0.75rem;margin-top:0.2rem">${renderEbayLink(artists[0], title, catno, true, labelNames[0])}</div>` : ""))
        }
        ${releaseId ? renderActionsImmediate(Number(releaseId), isMaster ? "master" : "release") : ""}
      </div>
    </div>
    ${trackHTML}
    ${metaRows ? `<div class="album-extra">${metaRows}</div>` : ""}
    ${isMaster ? `<div id="master-versions-list" style="padding:0.75rem;font-size:0.78rem;color:var(--muted)">Loading pressings…</div>` : ""}`;

  if (isMaster) loadMasterVersions(null, searchResult.id);
  // Fetch instance data in background (for rating stars + instanceId)
  if (!isMaster && releaseId && window._collectionIds?.has(Number(releaseId))) loadModalInstanceData(Number(releaseId));
  // Highlight currently playing track if a video is active
  highlightPlayingTrack();
  // Stash scope on the popup root so post-suggest re-fetch can read
  // master_id without re-deriving from the rendered HTML.
  el.dataset.releaseId = String(releaseId || "");
  el.dataset.masterId  = String(d.master_id || (isMaster ? releaseId : "") || "");
  el.dataset.entityType = isMaster ? "master" : "release";
  // Crowd-sourced YouTube overrides — kick the fetch + DOM patch.
  // Anything new lands on the next tick; if there's nothing to apply,
  // this is a no-op. Anonymous viewers still get override-driven play
  // links (the GET endpoint is public).
  // Refresh the unavailable-list cache too — the override+findVideo
  // paths consult it. After it lands, re-apply the DOM patch so any
  // newly-flagged broken videos are filtered out and the missing
  // count updates.
  if (releaseId && tracks.length) {
    Promise.all([
      _sdEnsureYtUnavailableLoaded(),
      _trackYtKickFetchAndApply(targetId, d.master_id, releaseId, isMaster),
    ]).then(() => {
      // Second apply — first run used a possibly-empty unavailable
      // set; this one runs after both fetches finish. Idempotent.
      _trackYtApplyToDom(targetId, d.master_id, releaseId, isMaster);
    }).catch(() => {});
  }
}

// ── Modal action buttons (collection/wantlist/rating) ────────────────────

async function loadPriceEstimates(releaseId, targetId) {
  const el = document.getElementById(targetId);
  if (!el) return;
  // Toggle off if already showing
  if (el.innerHTML) { el.innerHTML = ""; return; }
  el.innerHTML = `<span style="color:#555;font-size:0.7rem">Loading…</span>`;
  try {
    const r = await apiFetch(`/api/price-suggestions/${releaseId}`);
    if (!r.ok) throw new Error();
    const data = await r.json();
    // Conditions from best → worst, with short labels and gradient colors (orange → blue)
    const grades = [
      { key: "Good (G)",                 label: "G",   color: "#3a8596" },
      { key: "Good Plus (G+)",           label: "G+",  color: "#58867c" },
      { key: "Very Good (VG)",           label: "VG",  color: "#7c8766" },
      { key: "Very Good Plus (VG+)",     label: "VG+", color: "#a08850" },
      { key: "Near Mint (NM or M-)",     label: "NM",  color: "#c4893f" },
      { key: "Mint (M)",                 label: "M",   color: "#e08a3a" },
    ];
    const rows = grades
      .filter(g => data[g.key]?.value != null)
      .map(g => {
        const val = parseFloat(data[g.key].value).toFixed(2);
        return `<span style="color:${g.color};font-weight:600">${g.label}</span>&nbsp;<span style="color:#aaa">$${val}</span>`;
      });
    el.innerHTML = rows.length
      ? `<div style="font-size:0.72rem;margin-top:0.25rem;display:flex;flex-wrap:wrap;gap:0.15rem 0.6rem">${rows.map(r => `<span>${r}</span>`).join("")}</div>`
      : `<span style="color:#555;font-size:0.7rem">No estimates available</span>`;
  } catch {
    el.innerHTML = `<span style="color:#555;font-size:0.7rem">Estimates unavailable</span>`;
  }
}

// Render buttons immediately from local Sets (no network call)
function renderActionsImmediate(rid, entityType = "release") {
  const inCol = window._collectionIds?.has(rid);
  const inWant = window._wantlistIds?.has(rid);
  const favKey = `${entityType}:${rid}`;
  const isFav = window._favoriteKeys?.has(favKey);
  const favBtn = `<button class="modal-act-btn ${isFav ? 'is-favorite' : ''}" id="modal-fav-btn" onclick="toggleFavoriteFromModal(${rid},'${entityType}')" title="${isFav ? 'Remove from favorites' : 'Add to favorites'}">
      ${isFav ? 'Favorited' : 'Favorite'}
    </button>`;
  if (entityType !== "release") {
    // Master/artist/label — only show favorite button
    return `<div id="modal-actions" class="modal-actions" data-release-id="${rid}" data-entity-type="${entityType}">${favBtn}</div>`;
  }
  const invListings = (window._inventoryListingIds && window._inventoryListingIds[rid]) || [];
  const hasListing = invListings.length > 0;
  // When the user already has listings for this release, the "copies for sale"
  // panel below handles editing — no button in the action row. Only offer the
  // Sell action for releases the user hasn't yet listed.
  const sellBtn = hasListing
    ? ""
    : `<button class="modal-act-btn" id="modal-sell-btn" onclick="openInventoryEditor({mode:'create',releaseId:${rid}})" title="Create a marketplace listing for this release">Sell</button>`;
  return `<div id="modal-actions" class="modal-actions" data-release-id="${rid}" data-entity-type="${entityType}">
    <button class="modal-act-btn ${inCol ? 'in-collection' : ''}" id="modal-col-btn" onclick="toggleCollection(${rid})" title="${inCol ? 'Remove from collection' : 'Add to collection'}">
      ${inCol ? 'Collected' : 'Collection'}
    </button>
    <button class="modal-act-btn ${inWant ? 'in-wantlist' : ''}" id="modal-want-btn" onclick="toggleWantlist(${rid})" title="${inWant ? 'Remove from wantlist' : 'Add to wantlist'}">
      ${inWant ? 'Wanted' : 'Want'}
    </button>
    ${sellBtn}
    ${favBtn}
    ${inCol ? '<span class="modal-rating" id="modal-rating" style="opacity:0.4">☆☆☆☆☆</span>' : ''}
  </div>`;
}

// Fetch instance data in background and upgrade the rating stars.
// Also fetches all instances (multi-copy support) and renders a per-copy
// summary when the user owns more than one instance of this release.
//
// If window._modalActiveInstanceId is set (from the (N) popover on a card),
// the modal's rating stars / folder / remove button are scoped to that
// specific instance instead of the default primary one. The hint is
// consumed once and cleared.
async function loadModalInstanceData(releaseId) {
  const el = document.getElementById("modal-actions");
  if (!el) return;
  const rid = Number(releaseId);
  let instanceId = null, folderId = 1, currentRating = 0;
  let allInstances = [];
  try {
    if (window._clerk?.user) {
      const [singleRes, allRes] = await Promise.all([
        apiFetch(`/api/user/collection/instance?releaseId=${rid}`).then(r => r.ok ? r.json() : null).catch(() => null),
        apiFetch(`/api/user/collection/instances?releaseId=${rid}`).then(r => r.ok ? r.json() : null).catch(() => null),
      ]);
      if (singleRes?.found) {
        instanceId = singleRes.instance_id;
        folderId = singleRes.folder_id ?? 1;
        currentRating = singleRes.rating ?? 0;
      }
      if (Array.isArray(allRes?.instances)) allInstances = allRes.instances;

      // If the user clicked a specific instance in the (N) popover, prefer it.
      const hint = Number(window._modalActiveInstanceId ?? 0);
      if (hint) {
        const match = allInstances.find(i => Number(i.instance_id) === hint);
        if (match) {
          instanceId = match.instance_id;
          folderId = match.folder_id ?? 1;
          currentRating = match.rating ?? 0;
        }
        window._modalActiveInstanceId = null; // consume hint
      }
    }
  } catch {}
  el.dataset.instanceId = instanceId ?? "";
  el.dataset.folderId = folderId;
  // Upgrade rating stars with actual data
  const ratingEl = document.getElementById("modal-rating");
  if (ratingEl) {
    ratingEl.innerHTML = renderStars(currentRating, rid);
    ratingEl.dataset.rating = currentRating;
    ratingEl.style.opacity = "";
  }
  await renderMultiInstancePanel(rid, allInstances, instanceId);
  renderNotesPanel(rid);
}

// Open the (N) popover listing every instance the user owns of this release.
// Clicking a row opens the standard album modal pre-scoped to that instance.
async function openInstancesPopover(event, releaseId) {
  event?.preventDefault?.();
  event?.stopPropagation?.();
  closeInstancesPopover();
  const rid = Number(releaseId);
  const anchor = event?.currentTarget || event?.target;
  if (!anchor) return;

  // Fetch instances
  let instances = [];
  try {
    if (!window._clerk?.user) { showToast?.("Sign in to view your copies", "error"); return; }
    const data = await apiFetch(`/api/user/collection/instances?releaseId=${rid}`).then(r => r.ok ? r.json() : null).catch(() => null);
    if (Array.isArray(data?.instances)) instances = data.instances;
  } catch {}
  if (!instances.length) return;

  // Look up folder names (lazy-load cache if needed)
  await ensureCollectionFoldersLoaded();
  const folderMap = new Map([[0, "All"], [1, "Uncategorized"]]);
  try {
    if (Array.isArray(window._collectionFolders)) {
      for (const f of window._collectionFolders) folderMap.set(Number(f.folderId ?? f.id), f.name);
    }
  } catch {}

  // Find the card's discogs URL so the modal can link back
  const card = anchor.closest("a[onclick]");
  let discogsUrl = "#";
  const match = card?.getAttribute("onclick")?.match(/openModal\(event,['"]?\d+['"]?,\s*'\w+',\s*'([^']+)'/);
  if (match) discogsUrl = match[1];

  const rows = instances.map((inst, idx) => {
    const folderName = folderMap.get(Number(inst.folder_id)) || `Folder ${inst.folder_id}`;
    const rating = inst.rating > 0 ? "★".repeat(inst.rating) + "☆".repeat(5 - inst.rating) : "unrated";
    const added = inst.added_at ? new Date(inst.added_at).toLocaleDateString() : "";
    const instId = Number(inst.instance_id ?? 0);
    return `<li class="instance-popover-row" data-instance-id="${instId}">
      <span class="instance-popover-folder">${escHtml(folderName)}</span>
      <span class="instance-popover-rating">${rating}</span>
      ${added ? `<span class="instance-popover-added">${added}</span>` : ""}
    </li>`;
  }).join("");

  const pop = document.createElement("div");
  pop.id = "instance-popover";
  pop.className = "instance-popover";
  pop.innerHTML = `
    <div class="instance-popover-header">${instances.length} ${instances.length === 1 ? "copy" : "copies"} — click to open</div>
    <ul class="instance-popover-list">${rows}</ul>
  `;
  document.body.appendChild(pop);

  // Position near the anchor (below, aligned left)
  const rect = anchor.getBoundingClientRect();
  const top = rect.bottom + window.scrollY + 6;
  const left = rect.left + window.scrollX;
  pop.style.top = `${top}px`;
  pop.style.left = `${left}px`;

  // Clamp to viewport
  requestAnimationFrame(() => {
    const pr = pop.getBoundingClientRect();
    if (pr.right > window.innerWidth - 8) {
      pop.style.left = `${window.scrollX + window.innerWidth - pr.width - 8}px`;
    }
  });

  // Click rows → open modal scoped to that instance
  pop.querySelectorAll(".instance-popover-row").forEach(row => {
    row.addEventListener("click", (ev) => {
      ev.stopPropagation();
      const instId = Number(row.dataset.instanceId) || 0;
      window._modalActiveInstanceId = instId;
      closeInstancesPopover();
      if (typeof openModal === "function") openModal(null, rid, "release", discogsUrl);
    });
  });

  // Dismiss on outside click / escape
  setTimeout(() => {
    const outsideClickHandler = (ev) => {
      const pop = document.getElementById("instance-popover");
      if (!pop) {
        document.removeEventListener("click", outsideClickHandler, true);
        return;
      }
      if (!pop.contains(ev.target)) {
        document.removeEventListener("click", outsideClickHandler, true);
        closeInstancesPopover();
      }
    };
    document.addEventListener("click", outsideClickHandler, true);
    window._instancePopoverOutsideHandler = outsideClickHandler;
  }, 0);
}

function closeInstancesPopover() {
  const existing = document.getElementById("instance-popover");
  if (existing) existing.remove();
  if (window._instancePopoverOutsideHandler) {
    document.removeEventListener("click", window._instancePopoverOutsideHandler, true);
    window._instancePopoverOutsideHandler = null;
  }
}
document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeInstancesPopover(); });

// Render a "×N copies" panel when the user owns more than one instance of a release.
// Make sure window._collectionFolders is populated so folder IDs can be
// resolved to human-readable names. Safe to call repeatedly; only fetches
// once unless the cache is empty.
async function ensureCollectionFoldersLoaded() {
  if (Array.isArray(window._collectionFolders) && window._collectionFolders.length) return;
  try {
    const r = await apiFetch("/api/user/folders");
    if (r.ok) {
      const d = await r.json();
      window._collectionFolders = Array.isArray(d.folders) ? d.folders : [];
    }
  } catch {}
}

// Clicking a row switches the modal's active instance (rating stars, remove
// button, folder, notes all become scoped to that copy).
// Render a single row inside the "copies for sale" list on the modal.
function renderSaleListingRow(l) {
  if (!l || typeof l !== "object") return "";
  const id = Number(l.id ?? 0);
  const status = l.status || "";
  const statusClass = status === "For Sale" ? "sale-status-live"
    : status === "Draft" ? "sale-status-draft"
    : status === "Expired" ? "sale-status-expired"
    : "sale-status-other";
  const price = (l.price != null && l.currency)
    ? `${l.currency} ${Number(l.price).toFixed(2)}`
    : (l.price != null ? Number(l.price).toFixed(2) : "—");
  const cond = l.condition || "—";
  const sleeve = l.sleeve || "—";
  const comments = l.comments ? String(l.comments) : "";
  const posted = l.posted_at ? new Date(l.posted_at).toLocaleDateString() : "";
  return `<li class="modal-sale-row">
    <div class="modal-sale-row-top">
      <span class="modal-sale-price">${escHtml(price)}</span>
      <span class="modal-sale-status ${statusClass}">${escHtml(status || "—")}</span>
      ${posted ? `<span class="modal-sale-date">${escHtml(posted)}</span>` : ""}
      <span style="flex:1"></span>
      <button type="button" class="modal-sale-edit" onclick="openInventoryEditor({mode:'edit',listingId:${id}})" title="Edit this listing">Edit</button>
    </div>
    <div class="modal-sale-row-cond">
      <span><strong>Media:</strong> ${escHtml(cond)}</span>
      <span><strong>Sleeve:</strong> ${escHtml(sleeve)}</span>
    </div>
    ${comments ? `<div class="modal-sale-row-notes">${escHtml(comments)}</div>` : ""}
  </li>`;
}

function toggleSaleListingDetails(btn) {
  const list = document.getElementById("modal-sale-list");
  if (!list) return;
  // Don't rely on the `hidden` attribute here — the list's base display is
  // `flex`, which would override `[hidden]` unless marked !important. Use a
  // class instead so the open/closed state is explicit.
  const isOpen = list.classList.contains("is-open");
  list.classList.toggle("is-open", !isOpen);
  btn.setAttribute("aria-expanded", String(!isOpen));
  btn.textContent = isOpen ? "Show details" : "Hide details";
}

async function renderMultiInstancePanel(releaseId, instances, activeInstanceId) {
  await ensureCollectionFoldersLoaded();
  const existing = document.getElementById("modal-instances-panel");
  if (existing) existing.remove();
  instances = Array.isArray(instances) ? instances : [];
  const saleListings = (window._inventoryListingIds && window._inventoryListingIds[Number(releaseId)]) || [];
  // Render the panel if the user owns any copy OR has the release listed for sale.
  if (instances.length < 1 && saleListings.length < 1) return;
  const actionsEl = document.getElementById("modal-actions");
  if (!actionsEl) return;
  const activeId = Number(activeInstanceId ?? 0);
  const count = instances.length;
  const multi = count >= 2;

  // Look up folder names from window._collectionFolders (if available)
  const folderMap = new Map();
  try {
    if (Array.isArray(window._collectionFolders)) {
      for (const f of window._collectionFolders) folderMap.set(Number(f.folderId ?? f.id), f.name);
    }
  } catch {}
  folderMap.set(0, "All");
  folderMap.set(1, "Uncategorized");

  const rows = multi ? instances.map(inst => {
    const fid = Number(inst.folder_id);
    const folderName = folderMap.get(fid) || `Folder ${fid}`;
    const rating = inst.rating > 0 ? "★".repeat(inst.rating) + "☆".repeat(5 - inst.rating) : "unrated";
    const added = inst.added_at ? new Date(inst.added_at).toLocaleDateString() : "";
    const notesStr = Array.isArray(inst.notes) && inst.notes.length
      ? inst.notes.filter(n => n?.value).map(n => n.value).join(" · ")
      : "";
    const instId = Number(inst.instance_id ?? 0);
    const isActive = instId && instId === activeId;
    return `<li class="modal-instance-row${isActive ? " is-active" : ""}" data-instance-id="${instId}" title="Click to make this copy active">
      <button type="button" class="modal-instance-folder modal-folder-chip" data-instance-id="${instId}" data-folder-id="${fid}" title="Click to move this copy to a different folder">${escHtml(folderName)}</button>
      <span class="modal-instance-rating">${rating}</span>
      ${added ? `<span class="modal-instance-added">${added}</span>` : ""}
      ${notesStr ? `<span class="modal-instance-notes">${escHtml(notesStr)}</span>` : ""}
    </li>`;
  }).join("") : "";

  // Single-copy header needs a folder chip too
  let singleFolderChip = "";
  if (!multi && instances[0]) {
    const inst = instances[0];
    const fid = Number(inst.folder_id);
    const folderName = folderMap.get(fid) || `Folder ${fid}`;
    const instId = Number(inst.instance_id ?? 0);
    singleFolderChip = ` in <button type="button" class="modal-folder-chip modal-folder-chip-inline" data-instance-id="${instId}" data-folder-id="${fid}" title="Click to move this copy to a different folder">${escHtml(folderName)}</button>`;
  }

  const panel = document.createElement("div");
  panel.id = "modal-instances-panel";
  panel.className = "modal-instances-panel";

  // Collection header block — only render if the user actually owns copies.
  let collectionBlock = "";
  if (count > 0) {
    const headerText = multi
      ? `You own <strong>${count}</strong> copies of this release`
      : `You own <strong>1</strong> copy of this release${singleFolderChip}`;
    const hint = multi ? `<span class="modal-instances-hint">— click a copy to edit it</span>` : "";
    collectionBlock = `
      <div class="modal-instances-header">
        <span class="modal-instances-title">${headerText} ${hint}</span>
        <button type="button" class="modal-add-copy-btn" onclick="openAddCopyFolderPicker(${Number(releaseId)})" title="Add another copy of this release to a folder">+ Add another copy</button>
      </div>
      ${multi ? `<ul class="modal-instances-list">${rows}</ul>` : ""}
    `;
  }

  // Sale-listings block — shown if the user has at least one marketplace
  // listing for this release, whether or not they also own a collection copy.
  let saleBlock = "";
  if (saleListings.length > 0) {
    const saleCount = saleListings.length;
    const saleHeader = saleCount === 1
      ? `You have <strong>1</strong> copy for sale`
      : `You have <strong>${saleCount}</strong> copies for sale`;
    saleBlock = `
      <div class="modal-sale-header">
        <span class="modal-sale-title">${saleHeader}</span>
        <button type="button" class="modal-sale-toggle" id="modal-sale-toggle" aria-expanded="false" onclick="toggleSaleListingDetails(this)">Show details</button>
      </div>
      <ul class="modal-sale-list" id="modal-sale-list">
        ${saleListings.map(l => renderSaleListingRow(l)).join("")}
      </ul>
    `;
  }

  panel.innerHTML = collectionBlock + saleBlock;
  actionsEl.insertAdjacentElement("afterend", panel);

  // Folder chips → open move picker for that specific instance
  panel.querySelectorAll(".modal-folder-chip").forEach(chip => {
    chip.addEventListener("click", (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      const instId = Number(chip.dataset.instanceId) || 0;
      const fromFolderId = Number(chip.dataset.folderId);
      if (!instId || !Number.isFinite(fromFolderId)) return;
      if (typeof openQuickFolderPicker === "function") {
        openQuickFolderPicker(Number(releaseId), instId, fromFolderId);
      }
    });
  });

  // Row click → switch the active instance without closing the modal.
  // (Folder chip clicks are captured above with stopPropagation so they don't trigger this.)
  panel.querySelectorAll(".modal-instance-row").forEach(row => {
    row.addEventListener("click", () => {
      const instId = Number(row.dataset.instanceId) || 0;
      if (!instId || instId === activeId) return;
      window._modalActiveInstanceId = instId;
      loadModalInstanceData(releaseId);
    });
  });
}

// ── Collection custom-field definitions cache ───────────────────────────
//
// Discogs lets each user define custom collection fields (the built-in
// "Notes" field is itself one of them) with type `textarea` or `dropdown`.
// Fetch them once per session and reuse.
async function ensureCollectionFieldsLoaded() {
  if (Array.isArray(window._collectionFieldDefs)) return window._collectionFieldDefs;
  if (window._collectionFieldDefsPromise) return window._collectionFieldDefsPromise;
  window._collectionFieldDefsPromise = (async () => {
    try {
      const r = await apiFetch("/api/user/collection/fields");
      if (!r.ok) { window._collectionFieldDefs = []; return []; }
      const data = await r.json();
      const fields = Array.isArray(data?.fields) ? data.fields : [];
      fields.sort((a, b) => (a.position ?? 0) - (b.position ?? 0));
      window._collectionFieldDefs = fields;
      return fields;
    } catch {
      window._collectionFieldDefs = [];
      return [];
    } finally {
      window._collectionFieldDefsPromise = null;
    }
  })();
  return window._collectionFieldDefsPromise;
}

// Render the notes/fields editor panel below #modal-instances-panel (or
// below #modal-actions when no instances panel exists). Shows:
//   - Collection fields editor scoped to the active instance, if the user
//     owns a copy of this release.
//   - Wantlist notes editor, if the release is in the user's wantlist.
// Both blocks can coexist.
// Read/write the global collapsed state for the notes panel. Stored
// in localStorage so toggling on one popup persists to every future
// popup in every tab until the user flips it again.
function _notesPanelIsCollapsed() {
  try { return localStorage.getItem("sd-notes-collapsed") === "1"; } catch { return false; }
}
function _setNotesPanelCollapsed(collapsed) {
  try { localStorage.setItem("sd-notes-collapsed", collapsed ? "1" : "0"); } catch {}
}
function toggleNotesPanel() {
  const panel = document.getElementById("modal-notes-panel");
  if (!panel) return;
  const nowCollapsed = !panel.classList.contains("is-collapsed");
  panel.classList.toggle("is-collapsed", nowCollapsed);
  _setNotesPanelCollapsed(nowCollapsed);
  const chev = panel.querySelector(".modal-notes-chev");
  if (chev) chev.textContent = nowCollapsed ? "▸" : "▾";
}

async function renderNotesPanel(releaseId) {
  const rid = Number(releaseId);
  if (!rid) return;
  // Remove any existing editor (we re-render on instance switches / toggles)
  const existing = document.getElementById("modal-notes-panel");
  if (existing) existing.remove();

  const actionsEl = document.getElementById("modal-actions");
  if (!actionsEl || actionsEl.dataset.entityType && actionsEl.dataset.entityType !== "release") return;

  const inCol = window._collectionIds?.has(rid);
  const inWant = window._wantlistIds?.has(rid);
  if (!inCol && !inWant) return;

  const collapsed = _notesPanelIsCollapsed();
  const panel = document.createElement("div");
  panel.id = "modal-notes-panel";
  panel.className = "modal-notes-panel" + (collapsed ? " is-collapsed" : "");
  panel.innerHTML = `<div class="modal-notes-loading">Loading notes…</div>`;

  const anchor = document.getElementById("modal-instances-panel") || actionsEl;
  anchor.insertAdjacentElement("afterend", panel);

  // Fetch the pieces in parallel.
  const instanceId = Number(actionsEl.dataset.instanceId) || null;
  const folderId = Number(actionsEl.dataset.folderId) || 1;

  const tasks = [];
  if (inCol) {
    tasks.push(ensureCollectionFieldsLoaded());
    tasks.push(
      instanceId
        ? apiFetch(`/api/user/collection/instances?releaseId=${rid}`).then(r => r.ok ? r.json() : null).catch(() => null)
        : apiFetch(`/api/user/collection/instance?releaseId=${rid}`).then(r => r.ok ? r.json() : null).catch(() => null)
    );
  } else {
    tasks.push(Promise.resolve([]));
    tasks.push(Promise.resolve(null));
  }
  if (inWant) {
    tasks.push(apiFetch(`/api/user/wantlist/item?releaseId=${rid}`).then(r => r.ok ? r.json() : null).catch(() => null));
  } else {
    tasks.push(Promise.resolve(null));
  }

  const [fieldDefs, colData, wantData] = await Promise.all(tasks);

  // Resolve notes for the active collection instance.
  let instanceNotes = [];
  if (inCol && colData) {
    if (Array.isArray(colData.instances)) {
      const match = instanceId
        ? colData.instances.find(i => Number(i.instance_id) === instanceId)
        : colData.instances[0];
      instanceNotes = Array.isArray(match?.notes) ? match.notes : [];
    } else if (colData.found) {
      instanceNotes = Array.isArray(colData.notes) ? colData.notes : [];
    }
  }
  const notesByField = new Map();
  for (const n of instanceNotes) {
    if (n && n.field_id != null) notesByField.set(Number(n.field_id), n.value ?? "");
  }

  let html = "";

  // Collection block
  if (inCol) {
    const fields = Array.isArray(fieldDefs) ? fieldDefs : [];
    if (fields.length === 0) {
      html += `<div class="modal-notes-block"><div class="modal-notes-empty">No collection fields defined on Discogs.</div></div>`;
    } else {
      const rows = fields.map(f => {
        const fid = Number(f.id);
        const cur = notesByField.get(fid) ?? "";
        const label = escHtml(f.name || `Field ${fid}`);
        if (f.type === "dropdown" && Array.isArray(f.options)) {
          const opts = ['<option value=""></option>']
            .concat(f.options.map(o => `<option value="${escHtml(o)}"${o === cur ? " selected" : ""}>${escHtml(o)}</option>`))
            .join("");
          return `<label class="modal-notes-row">
            <span class="modal-notes-label">${label}</span>
            <select class="modal-notes-input" data-field-id="${fid}" data-initial="${escHtml(cur)}" onchange="saveCollectionField(event,${rid},${fid})">${opts}</select>
          </label>`;
        }
        const isTextarea = f.type === "textarea" || (cur && cur.length > 40);
        const input = isTextarea
          ? `<textarea class="modal-notes-input" rows="2" data-field-id="${fid}" data-initial="${escHtml(cur)}" onblur="saveCollectionField(event,${rid},${fid})" onkeydown="handleNotesKey(event,${rid},${fid},'collection')">${escHtml(cur)}</textarea>`
          : `<input type="text" class="modal-notes-input" data-field-id="${fid}" data-initial="${escHtml(cur)}" value="${escHtml(cur)}" onblur="saveCollectionField(event,${rid},${fid})" onkeydown="handleNotesKey(event,${rid},${fid},'collection')" />`;
        return `<label class="modal-notes-row">
          <span class="modal-notes-label">${label}</span>
          ${input}
        </label>`;
      }).join("");
      html += `<div class="modal-notes-block">
        <div class="modal-notes-title">Collection fields${instanceId ? ` <span class="modal-notes-hint">(this copy)</span>` : ""}</div>
        ${rows}
      </div>`;
    }
  }

  // Wantlist block
  if (inWant) {
    const wantNotes = Array.isArray(wantData?.notes) ? wantData.notes : [];
    const cur = wantNotes.find(n => n && n.field_id === 0)?.value ?? "";
    html += `<div class="modal-notes-block">
      <div class="modal-notes-title">Wantlist notes</div>
      <label class="modal-notes-row">
        <textarea class="modal-notes-input" rows="2" data-initial="${escHtml(cur)}" placeholder="Notes visible only to you" onblur="saveWantlistNotes(event,${rid})" onkeydown="handleNotesKey(event,${rid},0,'wantlist')">${escHtml(cur)}</textarea>
      </label>
    </div>`;
  }

  // Wrap the blocks in a collapsible body. The header is always
  // visible and flips between ▾ (open) and ▸ (collapsed); clicking it
  // toggles the body AND persists the choice site-wide.
  const bodyHtml = html || `<div class="modal-notes-empty">No notes available.</div>`;
  const headerLabel = (inCol && inWant) ? "Notes & fields"
                    : inCol              ? "Collection fields"
                    :                      "Wantlist notes";
  panel.innerHTML = `
    <button type="button" class="modal-notes-header" onclick="toggleNotesPanel()" title="Click to collapse or expand">
      <span class="modal-notes-chev">${collapsed ? "▸" : "▾"}</span>
      <span class="modal-notes-header-label">${escHtml(headerLabel)}</span>
    </button>
    <div class="modal-notes-body">${bodyHtml}</div>
  `;
}

// Enter (without Shift) commits the edit by blurring the field, which
// triggers the onblur save handler. Esc reverts to the initial value.
function handleNotesKey(event, releaseId, fieldId, mode) {
  if (event.key === "Enter" && !event.shiftKey && event.target.tagName !== "TEXTAREA") {
    event.preventDefault();
    event.target.blur();
  } else if (event.key === "Enter" && !event.shiftKey && event.ctrlKey) {
    event.preventDefault();
    event.target.blur();
  } else if (event.key === "Escape") {
    event.preventDefault();
    const initial = event.target.dataset.initial ?? "";
    event.target.value = initial;
    event.target.blur();
  }
}

async function saveCollectionField(event, releaseId, fieldId) {
  const el = event?.target;
  if (!el) return;
  const value = String(el.value ?? "");
  const initial = el.dataset.initial ?? "";
  if (value === initial) return; // no-op
  const actionsEl = document.getElementById("modal-actions");
  const instanceId = Number(actionsEl?.dataset.instanceId) || null;
  const folderId = Number(actionsEl?.dataset.folderId) || 1;
  if (!instanceId) { showToast?.("Add this release to your collection first", "error"); return; }
  el.disabled = true;
  try {
    const r = await apiFetch("/api/user/collection/notes", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ releaseId: Number(releaseId), instanceId, folderId, fieldId: Number(fieldId), value }),
    }).then(r => r.json());
    if (r?.ok) {
      el.dataset.initial = value;
      showToast?.("Saved");
    } else {
      el.value = initial;
      showToast?.(r?.error || "Failed to save", "error");
    }
  } catch {
    el.value = initial;
    showToast?.("Failed to save", "error");
  } finally {
    el.disabled = false;
  }
}

async function saveWantlistNotes(event, releaseId) {
  const el = event?.target;
  if (!el) return;
  const value = String(el.value ?? "");
  const initial = el.dataset.initial ?? "";
  if (value === initial) return;
  el.disabled = true;
  try {
    const r = await apiFetch("/api/user/wantlist/notes", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ releaseId: Number(releaseId), value }),
    }).then(r => r.json());
    if (r?.ok) {
      el.dataset.initial = value;
      showToast?.("Saved");
    } else {
      el.value = initial;
      showToast?.(r?.error || "Failed to save", "error");
    }
  } catch {
    el.value = initial;
    showToast?.("Failed to save", "error");
  } finally {
    el.disabled = false;
  }
}

// Full re-render of action buttons (used after toggle actions)
// context: "version-info" for version popup, otherwise main modal
function loadModalActions(releaseId, context) {
  const container = context ? document.getElementById(context) : null;
  const el = container ? container.querySelector(".modal-actions") : document.getElementById("modal-actions");
  if (!el) return;
  const rid = Number(releaseId);
  const entityType = el.dataset.entityType || "release";
  const inCol = window._collectionIds?.has(rid);
  const inWant = window._wantlistIds?.has(rid);
  const favKey = `${entityType}:${rid}`;
  const isFav = window._favoriteKeys?.has(favKey);
  const favBtn = `<button class="modal-act-btn ${isFav ? 'is-favorite' : ''}" id="modal-fav-btn" onclick="toggleFavoriteFromModal(${rid},'${entityType}')" title="${isFav ? 'Remove from favorites' : 'Add to favorites'}">
      ${isFav ? 'Favorited' : 'Favorite'}
    </button>`;

  if (entityType !== "release") {
    el.innerHTML = favBtn;
  } else {
    const invListings = (window._inventoryListingIds && window._inventoryListingIds[rid]) || [];
    const hasListing = invListings.length > 0;
    // No button in the action row when listings exist — the "copies for sale"
    // panel below handles editing and details.
    const sellBtn = hasListing
      ? ""
      : `<button class="modal-act-btn" id="modal-sell-btn" onclick="openInventoryEditor({mode:'create',releaseId:${rid}})" title="Create a marketplace listing for this release">Sell</button>`;
    el.innerHTML = `
      <button class="modal-act-btn ${inCol ? 'in-collection' : ''}" id="modal-col-btn" onclick="toggleCollection(${rid})" title="${inCol ? 'Remove from collection' : 'Add to collection'}">
        ${inCol ? 'Collected' : 'Collection'}
      </button>
      <button class="modal-act-btn ${inWant ? 'in-wantlist' : ''}" id="modal-want-btn" onclick="toggleWantlist(${rid})" title="${inWant ? 'Remove from wantlist' : 'Add to wantlist'}">
        ${inWant ? 'Wanted' : 'Want'}
      </button>
      ${sellBtn}
      ${favBtn}
      ${inCol ? '<span class="modal-rating" id="modal-rating" style="opacity:0.4">☆☆☆☆☆</span>' : ''}
    `;
  }
  el.style.display = "";
  // If in collection, fetch instance data for rating; if not in collection
  // but listed for sale, still render the panel so sale info is visible.
  if (entityType === "release") {
    const invListings = (window._inventoryListingIds && window._inventoryListingIds[rid]) || [];
    if (inCol) loadModalInstanceData(rid);
    else if (invListings.length) renderMultiInstancePanel(rid, [], null);
    // Wantlist-only: no instance data, but still render the notes panel
    // so the user can edit wantlist notes.
    if (!inCol && inWant) renderNotesPanel(rid);
  }
}

function renderStars(rating, releaseId) {
  let html = '';
  for (let i = 1; i <= 5; i++) {
    const active = i <= rating;
    html += `<span class="modal-star ${active ? 'active' : ''}" onclick="setRating(event,${releaseId},${i})" onmouseover="previewStars(this,${i})" onmouseout="resetStars(this)" title="Rate ${i} out of 5">${active ? '★' : '☆'}</span>`;
  }
  return html;
}

function previewStars(el, n) {
  const container = el.parentElement;
  container.querySelectorAll('.modal-star').forEach((s, i) => {
    s.textContent = i < n ? '★' : '☆';
    s.classList.toggle('preview', i < n);
  });
}

function resetStars(el) {
  const container = el.parentElement;
  const current = Number(container.dataset.rating ?? 0);
  container.querySelectorAll('.modal-star').forEach((s, i) => {
    s.textContent = i < current ? '★' : '☆';
    s.classList.remove('preview');
    s.classList.toggle('active', i < current);
  });
}

async function toggleCollection(releaseId) {
  const btn = document.getElementById("modal-col-btn");
  if (btn) btn.disabled = true;
  const inCol = window._collectionIds?.has(releaseId);
  try {
    if (!window._clerk?.user) { showToast("Sign in to manage your collection", "error"); return; }

    // Optimistic update
    if (inCol) {
      btn.innerHTML = '+ Collection';
      btn.classList.remove('in-collection');
      window._collectionIds?.delete(releaseId);
    } else {
      btn.innerHTML = '✓ Collected';
      btn.classList.add('in-collection');
      window._collectionIds?.add(releaseId);
    }
    refreshCardBadges(releaseId);

    const actionsEl = document.getElementById("modal-actions");
    const endpoint = inCol ? "/api/user/collection/remove" : "/api/user/collection/add";
    const body = inCol
      ? { releaseId, instanceId: Number(actionsEl?.dataset.instanceId) || null, folderId: Number(actionsEl?.dataset.folderId) || 1 }
      : { releaseId };

    const r = await apiFetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).then(r => r.json());

    if (!r.ok && r.error) {
      // Revert
      if (inCol) { window._collectionIds?.add(releaseId); } else { window._collectionIds?.delete(releaseId); }
      showToast(r.error, "error");
      loadModalActions(releaseId);
      return;
    }

    if (inCol) {
      showToast("Removed from collection");
    } else {
      const landedFolderId = Number(r.folderId) || 1;
      const folderName = r.folderName || (window._collectionFolders || []).find(f => Number(f.folderId) === landedFolderId)?.name || "Uncategorized";
      const instanceId = Number(r.instanceId) || null;
      if (instanceId && typeof showToastWithAction === "function") {
        showToastWithAction(
          `Added to ${folderName}`,
          "Move…",
          () => openQuickFolderPicker?.(releaseId, instanceId, landedFolderId),
          { type: "success", duration: 6000 }
        );
      } else {
        showToast(`Added to ${folderName}`, "success");
      }
    }
    // Refresh action row to update rating stars, instance info
    loadModalActions(releaseId);
    // Update badges on cards in the background
    refreshCardBadges(releaseId);
  } catch (e) {
    if (inCol) { window._collectionIds?.add(releaseId); } else { window._collectionIds?.delete(releaseId); }
    showToast("Action failed — try again", "error");
    loadModalActions(releaseId);
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function toggleWantlist(releaseId) {
  const btn = document.getElementById("modal-want-btn");
  if (btn) btn.disabled = true;
  const inWant = window._wantlistIds?.has(releaseId);
  try {
    if (!window._clerk?.user) { showToast("Sign in to manage your wantlist", "error"); return; }

    // Optimistic update
    if (inWant) {
      btn.innerHTML = '<span class="want-icon">🤞</span> Want';
      btn.classList.remove('in-wantlist');
      window._wantlistIds?.delete(releaseId);
    } else {
      btn.innerHTML = '<span class="want-icon active">🤞</span> Wanted';
      btn.classList.add('in-wantlist');
      window._wantlistIds?.add(releaseId);
    }
    refreshCardBadges(releaseId);

    const endpoint = inWant ? "/api/user/wantlist/remove" : "/api/user/wantlist/add";
    const r = await apiFetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ releaseId }),
    }).then(r => r.json());

    if (!r.ok && r.error) {
      if (inWant) { window._wantlistIds?.add(releaseId); } else { window._wantlistIds?.delete(releaseId); }
      showToast(r.error, "error");
      loadModalActions(releaseId);
      return;
    }

    showToast(inWant ? "Removed from wantlist" : "Added to wantlist");
    refreshCardBadges(releaseId);
    loadModalActions(releaseId);
  } catch (e) {
    if (inWant) { window._wantlistIds?.add(releaseId); } else { window._wantlistIds?.delete(releaseId); }
    showToast("Action failed — try again", "error");
    loadModalActions(releaseId);
  } finally {
    if (btn) btn.disabled = false;
  }
}

let _ratingDebounce = null;
async function setRating(event, releaseId, rating) {
  event.stopPropagation();
  const container = document.getElementById("modal-rating");
  if (!container) return;
  // Update stars immediately
  container.dataset.rating = rating;
  container.querySelectorAll('.modal-star').forEach((s, i) => {
    s.textContent = i < rating ? '★' : '☆';
    s.classList.toggle('active', i < rating);
  });

  // Debounce the API call
  clearTimeout(_ratingDebounce);
  _ratingDebounce = setTimeout(async () => {
    try {
      if (!window._clerk?.user) return;
      const actionsEl = document.getElementById("modal-actions");
      const r = await apiFetch("/api/user/collection/rating", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          releaseId,
          instanceId: Number(actionsEl?.dataset.instanceId) || null,
          folderId: Number(actionsEl?.dataset.folderId) || 1,
          rating,
        }),
      }).then(r => r.json());
      if (r.ok) showToast(`Rated ${rating}/5`);
    } catch { showToast("Failed to save rating", "error"); }
  }, 500);
}

// ── Price sparkline + alert UI in modal ─────────────────────────────────


function refreshCardBadges(releaseId) {
  // Re-render badges on any visible card with this release ID
  document.querySelectorAll(`.card-thumb-badges`).forEach(el => {
    const card = el.closest('a[onclick]');
    if (!card) return;
    // Match openModal cards (releases/masters)
    const modalMatch = card.getAttribute('onclick')?.match(/openModal\(event,['"]?(\d+)['"]?,\s*'(\w+)'/);
    // Match searchByEntity cards (artists/labels)
    const entityMatch = !modalMatch ? card.dataset.entityId : null;
    const entityType = !modalMatch ? card.dataset.entityType : null;

    let id, type;
    if (modalMatch && Number(modalMatch[1]) === releaseId) {
      id = releaseId; type = modalMatch[2] || "release";
    } else if (entityMatch && Number(entityMatch) === releaseId) {
      id = releaseId; type = entityType || "artist";
    } else return;

    // Use the SAME nav-icon SVG markup the initial card render (search.js)
    // uses — without this, toggling a badge replaced the icons with the
    // legacy single-letter glyphs ("C", "W", "F" etc.). The pink "F"
    // bug after clicking the favorite badge was the most visible
    // symptom; collection/wantlist icons reverted to letters too.
    const navIcon = (typeof window._sdNavIconSvg === "function") ? window._sdNavIconSvg : (() => "");
    let badges = "";
    const userHasLists = window._listMembership && Object.keys(window._listMembership).length > 0;
    const userHasInventory = (window._inventoryIds?.size ?? 0) > 0;
    if (type === "release") {
      const inCol = window._collectionIds?.has(id);
      const inWant = window._wantlistIds?.has(id);
      badges += `<span class="card-badge badge-collection${inCol ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleCollectionFromCard(this,${id})" title="${inCol ? "Remove from collection" : "Add to collection"}">${navIcon("collection")}</span>`;
      badges += `<span class="card-badge badge-wantlist${inWant ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleWantlistFromCard(this,${id})" title="${inWant ? "Remove from wantlist" : "Add to wantlist"}">${navIcon("wantlist")}</span>`;
    } else if (type === "master") {
      const colCount = Number(window._collectionMasterCounts?.[id]) || 0;
      const wantCount = Number(window._wantlistMasterCounts?.[id]) || 0;
      const colActive = colCount > 0;
      const wantActive = wantCount > 0;
      const colTitle = colActive
        ? `${colCount} ${colCount === 1 ? "version" : "versions"} of this master in your collection — click to view pressings`
        : "Open to add a version to collection";
      const wantTitle = wantActive
        ? `${wantCount} ${wantCount === 1 ? "version" : "versions"} of this master in your wantlist — click to view pressings`
        : "Open to add a version to wantlist";
      const colSup  = colCount  >= 2 ? `<sup class="card-badge-count">${colCount}</sup>`  : "";
      const wantSup = wantCount >= 2 ? `<sup class="card-badge-count">${wantCount}</sup>` : "";
      badges += `<span class="card-badge badge-collection${colActive ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();openModal(event,'${id}','master','')" title="${colTitle}">${navIcon("collection")}${colSup}</span>`;
      badges += `<span class="card-badge badge-wantlist${wantActive ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();openModal(event,'${id}','master','')" title="${wantTitle}">${navIcon("wantlist")}${wantSup}</span>`;
    }
    const favKey = `${type}:${id}`;
    const isFav = window._favoriteKeys?.has(favKey);
    badges += `<span class="card-badge badge-favorite${isFav ? " is-favorite" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleFavoriteFromCard(this,${id},'${type}')" title="${isFav ? "Remove from favorites" : "Add to favorites"}">${navIcon("favorites")}</span>`;
    if (type === "release") {
      if (userHasInventory) {
        const inInv = window._inventoryIds?.has(id);
        const iTitle = inInv ? "In your inventory" : "Not in your inventory";
        badges += `<span class="card-badge badge-inventory${inInv ? " is-active" : ""}" title="${iTitle}">${navIcon("inventory")}</span>`;
      }
      if (userHasLists) {
        const lists = window._listMembership?.[id];
        const inList = !!(lists && lists.length);
        const names = inList ? lists.map(l => l.listName).join(", ") : "";
        const lTitle = inList ? `In list: ${escHtml(names)}` : "Not in any of your lists";
        badges += `<span class="card-badge badge-list${inList ? " is-active" : ""}" title="${lTitle}">${navIcon("lists")}</span>`;
      }
    }
    el.innerHTML = badges;
  });
}

function toggleFavoriteFromModal(discogsId, entityType) {
  const key = `${entityType}:${discogsId}`;
  const wasFav = window._favoriteKeys?.has(key);
  if (!window._favoriteKeys) window._favoriteKeys = new Set();

  // Detect which popup this came from (version overlay or main modal)
  const inVersion = document.getElementById("version-overlay")?.classList.contains("open");
  const context = inVersion ? "version-info" : null;

  // Optimistic toggle
  if (wasFav) window._favoriteKeys.delete(key); else window._favoriteKeys.add(key);
  loadModalActions(discogsId, context);
  refreshCardBadges(discogsId);

  // Build card data — use _popupCardData (built in renderAlbumInfo with full API data)
  const popupData = window._popupCardData?.[String(discogsId)];
  const cached = (typeof itemCache !== "undefined") ? itemCache.get(String(discogsId)) : null;
  // Prefer popupCardData (always rich), then itemCache if it has detail fields
  const cardData = popupData
    || (cached && (cached.label?.length || cached.format?.length) ? cached : null)
    || (() => {
      const prefix = inVersion ? "#version-info" : "#album-info";
      const modalTitle = document.querySelector(`${prefix} .album-meta h2`);
      const modalArtist = document.querySelector(`${prefix} .album-artist`);
      const modalImg = document.querySelector(`${prefix} .album-cover`);
      return {
        id: discogsId, type: entityType,
        title: [modalArtist?.textContent?.replace(/⌕/g,"").trim(), modalTitle?.textContent?.replace(/⌕/g,"").trim()].filter(Boolean).join(" - "),
        cover_image: modalImg?.src || "", uri: `/${entityType}/${discogsId}`,
      };
    })();

  const endpoint = wasFav ? "/api/user/favorites/remove" : "/api/user/favorites/add";
  const body = wasFav ? { discogsId, entityType } : { discogsId, entityType, data: cardData };
  apiFetch(endpoint, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
    .then(r => { if (!r.ok) throw new Error(); const n = window._favoriteKeys?.size ?? 0; showToast(wasFav ? "Removed from favorites" : `Added to favorites (${n})`); })
    .catch(() => {
      if (wasFav) window._favoriteKeys.add(key); else window._favoriteKeys.delete(key);
      loadModalActions(discogsId, context);
      refreshCardBadges(discogsId);
      showToast("Failed to update favorite", "error");
    });
}

let _masterVersions = [];
let _mvFormatFilter = "";
let _mvCountryFilter = "";

const _MV_MEDIA = new Set(["Vinyl","CD","Cassette","DVD","Blu-ray","File","Box Set","Lathe Cut","Flexi-disc","Shellac","8-Track Cartridge","Reel-To-Reel","MiniDisc","SACD","Betamax","VHS"]);
function _mvGetMedium(v) {
  const parts = (v.format ?? "").split(",").map(s => s.trim());
  return parts.find(p => _MV_MEDIA.has(p)) || (v.majorFormats ?? []).find(f => _MV_MEDIA.has(f)) || parts[0] || "";
}
function _mvGetDisplayFormat(v) {
  const fmt = (v.format ?? "").trim();
  const medium = (v.majorFormats ?? []).find(f => _MV_MEDIA.has(f));
  if (!fmt) return medium || "—";
  if (!medium || fmt.split(",").map(s => s.trim()).includes(medium)) return fmt;
  return `${medium}, ${fmt}`;
}

function setMvFormatFilter(f) { _mvFormatFilter = f; localStorage.setItem("mv-format-filter", f); renderMasterVersions(); }
function setMvCountryFilter(c) { _mvCountryFilter = c; localStorage.setItem("mv-country-filter", c); renderMasterVersions(); }

function renderMasterVersions() {
  const list = document.getElementById("master-versions-list");
  if (!list) return;

  let filtered = _masterVersions;
  if (_mvFormatFilter) filtered = filtered.filter(v => _mvGetMedium(v) === _mvFormatFilter);
  if (_mvCountryFilter) filtered = filtered.filter(v => (v.country || "") === _mvCountryFilter);

  list.querySelectorAll(".mv-format-pill").forEach(p => {
    p.style.background = p.dataset.filter === (_mvFormatFilter ?? "") ? "var(--accent)" : "#2a2a2a";
    p.style.color      = p.dataset.filter === (_mvFormatFilter ?? "") ? "#000" : "var(--fg)";
  });
  list.querySelectorAll(".mv-country-pill").forEach(p => {
    p.style.background = p.dataset.filter === (_mvCountryFilter ?? "") ? "var(--accent)" : "#2a2a2a";
    p.style.color      = p.dataset.filter === (_mvCountryFilter ?? "") ? "#000" : "var(--fg)";
  });

  const grid = list.querySelector(".mv-grid");
  if (!grid) return;
  if (!filtered.length) { grid.innerHTML = `<span style="color:var(--muted);grid-column:1/-1">No pressings match this filter.</span>`; return; }
  grid.innerHTML = filtered.map(v => {
    const inCol  = window._collectionIds?.has(v.id);
    const inWant = window._wantlistIds?.has(v.id);
    const inList = window._listMembership?.[v.id]?.length > 0;
    const inInv  = window._inventoryIds?.has(v.id);
    const isFav  = window._favoriteKeys?.has(`release:${v.id}`);
    const listNames = inList ? (window._listMembership[v.id].map(l => l.name || l.title).filter(Boolean).join(", ")) : "";
    // Dots use the same SVG nav-icons as the navbar tabs and tint to
    // the matching tab colors (collection green, wantlist yellow,
    // favorites pink, lists light blue, inventory purple). Inactive
    // dots stay dimmed but still show the icon for clarity. Lists +
    // inventory dots are always present (no longer conditional) so
    // the row layout stays stable across versions.
    const navIcon = (typeof window._sdNavIconSvg === "function") ? window._sdNavIconSvg : (() => "");
    const badge = `<span class="mv-dots">` +
      `<span class="mv-dot${inCol ? ' active' : ''}" style="${inCol ? 'color:#6ddf70' : ''}" onclick="event.preventDefault();event.stopPropagation();mvToggleCol(this,${v.id})" title="${inCol ? 'In collection — click to remove' : 'Add to collection'}">${navIcon("collection")}</span>` +
      `<span class="mv-dot${inWant ? ' active' : ''}" style="${inWant ? 'color:#f0c95c' : ''}" onclick="event.preventDefault();event.stopPropagation();mvToggleWant(this,${v.id})" title="${inWant ? 'In wantlist — click to remove' : 'Add to wantlist'}">${navIcon("wantlist")}</span>` +
      `<span class="mv-dot${isFav ? ' active' : ''}" style="${isFav ? 'color:#ff7eb6' : ''}" onclick="event.preventDefault();event.stopPropagation();mvToggleFav(this,${v.id})" title="${isFav ? 'Favorited — click to remove' : 'Add to favorites'}">${navIcon("favorites")}</span>` +
      `<span class="mv-dot${inList ? ' active' : ''}" style="${inList ? 'color:#a0ccf0' : ''}" title="${escHtml(inList ? (listNames ? `In your list${window._listMembership[v.id].length > 1 ? "s" : ""}: ${listNames}` : "In one of your lists") : "Not in any of your lists")}">${navIcon("lists")}</span>` +
      `<span class="mv-dot${inInv ? ' active' : ''}" style="${inInv ? 'color:#cda0f5' : ''}" title="${inInv ? 'In your inventory' : 'Not in your inventory'}">${navIcon("inventory")}</span>` +
      `</span>`;
    const fmtText = _mvGetDisplayFormat(v);
    const fmtCell = inCol
      ? `<span title="Click to view your copy / change folder"><a href="#" class="modal-internal-link mv-format-owned" onclick="event.preventDefault();event.stopPropagation();openInstancesPopover(event,${v.id})" style="color:#7ec87e">${escHtml(fmtText)}</a></span>`
      : `<span style="color:#888" title="${escHtml(fmtText)}">${escHtml(fmtText)}</span>`;
    return `
      <span style="color:#888">${escHtml(!v.year || v.year === "0" ? "?" : String(v.year))}</span>
      <span style="color:#aaa">${escHtml(v.country || "?")}</span>
      ${fmtCell}
      ${badge}
      <span title="${escHtml(v.catno || "")}">${v.catno && v.catno !== "—" ? `<a href="#" class="modal-internal-link catno-link" onclick="openVersionPopup(event,${v.id})" title="Open this release">${escHtml(v.catno)}</a>` : `<span style="color:#7ec87e">—</span>`}</span>
      <span title="${escHtml(v.label ?? v.title ?? "")}">${(v.label) ? `<a href="#" class="modal-internal-link" onclick="event.preventDefault();closeModal();clearForm();document.getElementById('f-label').value='${escHtml((v.label).replace(/'/g, "\\'"))}';applyEntityLinkDefaults();toggleAdvanced(true);doSearch(1)" title="Search for ${escHtml(v.label)}" style="color:var(--fg)">${escHtml(v.label)}</a> <a href="#" class="album-title-search" onclick="event.preventDefault();searchCollectionFor('cw-label','${escHtml((v.label).replace(/'/g, "\\'"))}')" title="Search your collection for ${escHtml(v.label)}" style="font-size:0.85em">⌕</a>` : `<span style="color:#888">${escHtml(v.title ?? "—")}</span>`}</span>`;
  }).join("");
  applyVisitedCards();
}

// ── Series browser ────────────────────────────────────────────────────────
let _seriesReleases = [];
let _srFormatFilter = "";

function _srGetMedium(r) {
  const parts = (r.format ?? "").split(",").map(s => s.trim());
  return parts.find(p => _MV_MEDIA.has(p)) || parts[0] || "";
}

async function openSeriesBrowser(seriesId, seriesName) {
  const overlay = document.getElementById("series-overlay");
  const info    = document.getElementById("series-info");
  const loading = document.getElementById("series-loading");
  info.innerHTML = "";
  loading.style.display = "block";
  overlay.classList.add("open");
  _sdLockBodyScroll("series");

  try {
    const resp = await apiFetch(`${API}/series-releases/${seriesId}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    _seriesReleases = data.releases ?? [];
    loading.style.display = "none";

    if (!_seriesReleases.length) {
      info.innerHTML = `<div style="color:var(--muted);padding:1rem">No releases found in this series.</div>`;
      return;
    }

    // Build format filter pills
    const formatSet = new Set();
    _seriesReleases.forEach(r => {
      const m = _srGetMedium(r);
      if (m) formatSet.add(m);
    });
    const formats = [...formatSet].sort();
    _srFormatFilter = "";

    const formatPills = formats.length > 1
      ? `<div class="sr-pill-row">${[
          `<button class="sr-pill sr-format-pill" data-filter="" onclick="setSrFormatFilter('')">All</button>`,
          ...formats.map(f => `<button class="sr-pill sr-format-pill" data-filter="${escHtml(f)}" onclick="setSrFormatFilter('${f.replace(/'/g,"\\'")}')">${escHtml(f)}</button>`)
        ].join("")}</div>`
      : "";

    info.innerHTML = `
      <div style="font-size:0.9rem;color:var(--fg);font-weight:600;margin-bottom:0.15rem">${escHtml(data.name || seriesName)}</div>
      <div style="font-size:0.72rem;color:var(--muted);margin-bottom:0.6rem">${_seriesReleases.length} release${_seriesReleases.length !== 1 ? "s" : ""} in series${data.total > _seriesReleases.length ? ` (showing first ${_seriesReleases.length} of ${data.total})` : ""}</div>
      ${formatPills}
      <div class="sr-grid-scroll" style="overflow-x:auto"><div class="sr-grid"></div></div>`;
    renderSeriesReleases();
  } catch(e) {
    loading.style.display = "none";
    info.innerHTML = `<div style="color:var(--muted);padding:1rem">Failed to load series releases.</div>`;
    console.error("openSeriesBrowser error:", e);
  }
}

function closeSeriesBrowser() {
  document.getElementById("series-overlay").classList.remove("open");
  _sdUnlockBodyScroll("series");
}

document.getElementById("series-overlay")?.addEventListener("click", e => {
  if (e.target === document.getElementById("series-overlay")) closeSeriesBrowser();
});

function setSrFormatFilter(f) {
  _srFormatFilter = f;
  renderSeriesReleases();
}

function renderSeriesReleases() {
  const grid = document.querySelector("#series-info .sr-grid");
  if (!grid) return;

  let filtered = _seriesReleases;
  if (_srFormatFilter) filtered = filtered.filter(r => _srGetMedium(r) === _srFormatFilter);

  // Update pill styles
  document.querySelectorAll(".sr-format-pill").forEach(p => {
    p.style.background = p.dataset.filter === _srFormatFilter ? "var(--accent)" : "#2a2a2a";
    p.style.color      = p.dataset.filter === _srFormatFilter ? "#000" : "var(--fg)";
  });

  if (!filtered.length) {
    grid.innerHTML = `<span style="color:var(--muted);grid-column:1/-1">No releases match this filter.</span>`;
    return;
  }

  grid.innerHTML = filtered.map(r => {
    const inCol  = window._collectionIds?.has(r.id);
    const inWant = window._wantlistIds?.has(r.id);
    const inList = window._listMembership?.[r.id]?.length > 0;
    const inInv  = window._inventoryIds?.has(r.id);
    const isFav  = window._favoriteKeys?.has(`release:${r.id}`);
    const badge = `<span class="mv-dots">` +
      `<span class="mv-dot${inCol ? ' active' : ''}" style="background:${inCol ? '#6ddf70' : ''}" onclick="event.preventDefault();event.stopPropagation();mvToggleCol(this,${r.id})" title="${inCol ? 'In collection — click to remove' : 'Add to collection'}"></span>` +
      `<span class="mv-dot${inWant ? ' active' : ''}" style="background:${inWant ? '#f0c95c' : ''}" onclick="event.preventDefault();event.stopPropagation();mvToggleWant(this,${r.id})" title="${inWant ? 'In wantlist — click to remove' : 'Add to wantlist'}"></span>` +
      `<span class="mv-dot${isFav ? ' active' : ''}" style="background:${isFav ? '#ff6b35' : ''}" onclick="event.preventDefault();event.stopPropagation();mvToggleFav(this,${r.id})" title="${isFav ? 'Favorited — click to remove' : 'Add to favorites'}"></span>` +
      (inList ? `<span class="mv-dot active" style="background:#a0ccf0" title="In a list"></span>` : '') +
      (inInv ? `<span class="mv-dot active" style="background:#cda0f5" title="In your inventory"></span>` : '') +
      `</span>`;

    const thumbHtml = r.thumb
      ? `<img src="${r.thumb}" alt="" loading="lazy" />`
      : `<span style="display:inline-block;width:40px;height:40px;background:#1a1a1a;border-radius:3px"></span>`;

    const yearStr = r.year && r.year !== 0 ? String(r.year) : "?";
    const titleArtist = r.artist ? `${r.artist} — ${r.title}` : r.title;

    return `
      ${thumbHtml}
      <a href="#" class="sr-title" onclick="event.preventDefault();openVersionPopup(event,${r.id})" title="${escHtml(titleArtist)}">${escHtml(titleArtist)}</a>
      ${badge}
      <span style="color:#888">${escHtml(yearStr)}</span>
      <span style="color:#666" title="${escHtml(r.format)}">${escHtml(r.catno || r.format || "—")}</span>`;
  }).join("");
  applyVisitedCards();
}

async function loadMasterVersions(event, masterId) {
  if (event) event.preventDefault();
  const list = document.getElementById("master-versions-list");
  if (!list) return;
  try {
    const resp = await apiFetch(`${API}/master-versions/${masterId}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    _masterVersions = data.versions ?? [];
    if (!_masterVersions.length) { list.textContent = "No pressings found."; return; }

    const formatSet = new Set();
    const countrySet = new Set();
    _masterVersions.forEach(v => {
      const medium = _mvGetMedium(v);
      if (medium) formatSet.add(medium);
      if (v.country) countrySet.add(v.country);
    });
    const formats = [...formatSet].sort();
    const countries = [...countrySet].sort();

    // Restore saved filters if they exist in this master's options
    const savedFormat = localStorage.getItem("mv-format-filter") || "";
    const savedCountry = localStorage.getItem("mv-country-filter") || "";
    _mvFormatFilter = formatSet.has(savedFormat) ? savedFormat : "";
    _mvCountryFilter = countrySet.has(savedCountry) ? savedCountry : "";

    const formatPills = [
      `<button class="mv-format-pill mv-pill" data-filter="" onclick="setMvFormatFilter('')">All</button>`,
      ...formats.map(f => `<button class="mv-format-pill mv-pill" data-filter="${escHtml(f)}" onclick="setMvFormatFilter('${f.replace(/'/g,"\\'")}')">${escHtml(f)}</button>`)
    ].join("");
    const countryPills = [
      `<button class="mv-country-pill mv-pill" data-filter="" onclick="setMvCountryFilter('')">All</button>`,
      ...countries.map(c => `<button class="mv-country-pill mv-pill" data-filter="${escHtml(c)}" onclick="setMvCountryFilter('${c.replace(/'/g,"\\'")}')">${escHtml(c)}</button>`)
    ].join("");

    list.innerHTML = `
      <div style="font-size:0.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.4rem">Pressings / Versions</div>
      ${formats.length > 1 ? `<div class="mv-pill-row">${formatPills}</div>` : ""}
      ${countries.length > 1 ? `<div class="mv-pill-row">${countryPills}</div>` : ""}
      <div class="mv-grid-scroll"><div class="mv-grid" style="display:grid;grid-template-columns:auto auto minmax(0,7rem) 2.5rem minmax(0,8rem) minmax(8rem,1fr);gap:0.2rem 0.7rem;font-size:0.75rem;min-width:36rem"></div></div>`;
    renderMasterVersions();
  } catch(e) {
    console.error("loadMasterVersions error:", e);
    list.textContent = "Failed to load pressings.";
  }
}
