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
let _visited = new Set(JSON.parse(localStorage.getItem(_visitedKey) || "[]"));
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
const _HISTORY_MAX = 120;
function _recordHistory(id, type) {
  if (!id || !type) return;
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
  document.body.classList.add("modal-open");

  const cachedItem = (typeof itemCache !== 'undefined' ? itemCache.get(String(id)) : null) ?? { type, id };
  const endpoint = type === "master" ? "master" : "release";
  Promise.all([
    apiFetch(`${API}/${endpoint}/${id}`).then(r => r.json()),
    apiFetch(`${API}/marketplace-stats/${id}?type=${type}`).then(r => r.json()).catch(() => null),
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
  if (!document.getElementById("version-overlay")?.classList.contains("open") &&
      !document.getElementById("series-overlay")?.classList.contains("open")) {
    document.body.classList.remove("modal-open");
  }
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
  document.addEventListener("keydown", _lbKey);
}

function closeLightbox() {
  document.getElementById("lightbox-overlay").classList.remove("open");
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

document.getElementById("modal-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("modal-overlay")) closeModal();
});

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
  document.body.classList.add("modal-open");
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
  if (!document.getElementById("modal-overlay")?.classList.contains("open") &&
      !document.getElementById("series-overlay")?.classList.contains("open")) {
    document.body.classList.remove("modal-open");
  }
  const u = new URL(window.location.href);
  u.searchParams.delete("vr");
  history.replaceState({}, "", u.toString());
}

document.getElementById("version-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("version-overlay")) closeVersionPopup();
});

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

document.getElementById("bio-full-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("bio-full-overlay")) closeBioFull();
});

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

// Render the search-results list inside the wiki popup. Used by both
// openWikiPopup() and the "← Back to results" button on article view.
async function _renderWikiPopupSearch(q, contentEl) {
  contentEl.innerHTML = `
    <div class="wiki-header">
      <h2 style="margin:0 0 0.3rem 0">Wikipedia: "${escHtml(q)}"</h2>
      <div class="wiki-popup-subnote">Click a title to open the article here.</div>
    </div>
    <div class="wiki-popup-results wiki-results-list"><div class="wiki-results-loading">Searching Wikipedia for <em>${escHtml(q)}</em>…</div></div>`;
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
    listEl.innerHTML = rows.map(rec => {
      const safeTitle = String(rec.title || "").replace(/'/g, "\\'");
      return `
        <div class="wiki-result">
          <a href="#" class="wiki-result-title" onclick="event.preventDefault();openWikiArticle('${escHtml(safeTitle)}','${escHtml(safeQ)}')" title="Open in popup">${escHtml(rec.title || "")}</a>
          <div class="wiki-result-snippet">${rec.snippet || ""}…</div>
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
    const backBtn = sourceQuery
      ? `<button type="button" class="wiki-back-btn" onclick="openWikiPopup('${escHtml(safeSrc)}')">← Back to "${escHtml(sourceQuery)}" results</button>`
      : "";
    content.innerHTML = `
      <div class="wiki-header">
        ${backBtn}
        <h2 style="margin:0.3rem 0 0.4rem 0">${escHtml(data.title)}</h2>
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
  return `
    <div class="wiki-result">
      <a href="#" class="wiki-result-title" onclick="event.preventDefault();openWikiPopup('${escHtml(safeTitle)}')" title="Open in popup">${escHtml(rec.title || "")}</a>
      <div class="wiki-result-snippet">${rec.snippet || ""}…</div>
    </div>`;
}

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

// Small "W" icon to render after a magnifying glass — opens the wiki popup
// for the given query. Wraps the term in double-quotes so Wikipedia's
// CirrusSearch treats it as an exact phrase — fixes fuzzy mismatches
// like "Bill Wax" → Bill Gates by forcing a literal phrase match.
// Pass extraTerms (unquoted) for context like "record label" so the
// quoted phrase isn't the entire query.
function wikiIcon(query, label = "", extraTerms = "") {
  if (!query) return "";
  const phrase = String(query).trim();
  if (!phrase) return "";
  const composed = extraTerms
    ? `"${phrase}" ${String(extraTerms).trim()}`
    : `"${phrase}"`;
  const q = composed.replace(/'/g, "\\'");
  const lab = label || query;
  return ` <a href="#" class="wiki-icon" onclick="event.preventDefault();openWikiPopup('${escHtml(q)}')" title="Wikipedia: ${escHtml(lab)}">W</a>`;
}

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

function setVideoUrl(id) {
  const u = new URL(window.location.href);
  u.searchParams.set("vd", id);
  // Store the video's source release so page reload can reopen the right popup
  const op = u.searchParams.get("op");
  if (op) u.searchParams.set("vp", op);
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
  updatePlayerStatus("loading");
  // Timeout: if still "loading" after 8s, mark unavailable and skip
  if (_ytLoadTimer) clearTimeout(_ytLoadTimer);
  _ytLoadTimer = setTimeout(() => {
    if (vtoken !== _ytVideoToken) return;  // a different video was loaded since
    const statusEl = document.getElementById("mini-player-status");
    if (statusEl && (statusEl.textContent === "loading…" || statusEl.textContent === "buffering…")) {
      updatePlayerStatus("unavailable");
      setTimeout(() => { if (vtoken === _ytVideoToken) playNextVideo(); }, 1500);
    }
  }, 8000);
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
  // Clear load timeout once we get a definitive state
  if (state !== "loading" && state !== "buffering" && _ytLoadTimer) { clearTimeout(_ytLoadTimer); _ytLoadTimer = null; }
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

function _createYTPlayer(id) {
  const session = _ytSession;
  let vtoken = _ytVideoToken;
  document.getElementById("video-player").innerHTML = "";
  ytPlayer = new YT.Player("video-player", {
    height: "100%", width: "100%", videoId: id,
    playerVars: { autoplay: 1, rel: 0 },
    events: {
      onStateChange: function(e) {
        if (session !== _ytSession) return;   // player was destroyed/recreated
        // YT.PlayerState: -1=unstarted, 0=ended, 1=playing, 2=paused, 3=buffering, 5=cued
        if (e.data === 1) {
          _ytHasPlayed = true; updatePlayerStatus("playing"); window._ytRetried = false;
          // If no track meta (e.g. loaded from URL param), grab title from YT player
          const meta = (window._videoQueueMeta ?? [])[window._videoQueueIndex ?? 0];
          if (!meta || (!meta.track && !meta.album && !meta.artist)) {
            try {
              const vd = ytPlayer.getVideoData?.();
              if (vd?.title) {
                const titleEl = document.getElementById("mini-player-title");
                if (titleEl) titleEl.innerHTML = `<span class="vt-track">${escHtml(vd.title)}</span>`;
              }
            } catch {}
          }
        }
        else if (e.data === 2) updatePlayerStatus("paused");
        else if (e.data === 3) updatePlayerStatus("buffering");
        else if (e.data === 0) {
          // Guard against late "ended" events from a previous video on a reused player
          if (vtoken !== _ytVideoToken) return;
          updatePlayerStatus("ended"); onVideoEnded();
        }
        else if (e.data === 5) updatePlayerStatus("loading");
        // Keep vtoken in sync when a new video loads on same player
        if (e.data === -1 || e.data === 5) vtoken = _ytVideoToken;
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
          setTimeout(() => { if (vtoken === _ytVideoToken) playNextVideo(); }, 2000);
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
          setTimeout(() => { if (vtoken === _ytVideoToken) playNextVideo(); }, 2000);
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
  // Update both expanded nav and mini bar buttons
  const prevBtn  = document.getElementById("video-prev");
  const nextBtn  = document.getElementById("video-next");
  const miniPrev = document.getElementById("mini-prev");
  const miniNext = document.getElementById("mini-next");
  const titleEl  = document.getElementById("mini-player-title");
  if (prevBtn)  prevBtn.disabled  = idx <= 0;
  if (nextBtn)  nextBtn.disabled  = idx >= queue.length - 1;
  if (miniPrev) miniPrev.disabled = idx <= 0;
  if (miniNext) miniNext.disabled = idx >= queue.length - 1;
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
  // Show/hide album + share buttons based on whether we have a release to reopen
  const albumBtn = document.getElementById("mini-album");
  if (albumBtn) albumBtn.style.display = window._playerReleaseId ? "" : "none";
  const shareBtn = document.getElementById("mini-share");
  if (shareBtn) shareBtn.style.display = window._playerReleaseId ? "" : "none";
  highlightPlayingTrack();
}

function toggleMiniPlayer() {
  const mp = document.getElementById("mini-player");
  if (mp) mp.classList.toggle("expanded");
}

function openVideo(event, url) {
  if (event) { event.preventDefault(); event.stopPropagation(); }
  // Starting YouTube playback should stop the LOC audio bar so we
  // don't play both simultaneously. _locPlay already does the reverse.
  try { if (typeof _locClosePlayer === "function") _locClosePlayer(); } catch {}
  ensureYTAPI();
  const id = extractYouTubeId(url);
  if (!id) { window.open(url, "_blank", "noopener"); return; }
  // Scope queue to the popup container the clicked track belongs to,
  // so we don't mix tracks from different albums
  const clickedEl = event?.target?.closest?.(".track-link") || event?.target;
  // Scope to the popup the track was clicked in; if called programmatically (no event),
  // prefer the version popup if open, then main modal, then fall back to document
  const container = clickedEl?.closest?.("#album-info, #version-info")
    || (document.getElementById("version-overlay")?.classList.contains("open") ? document.getElementById("version-info") : null)
    || (document.getElementById("modal-overlay")?.classList.contains("open") ? document.getElementById("album-info") : null)
    || document;
  const trackLinks = [...container.querySelectorAll(".track-link[data-video]")];
  window._videoQueue      = trackLinks.map(a => a.dataset.video);
  window._videoQueueMeta  = trackLinks.map(a => ({
    track:  a.dataset.track  || "",
    album:  a.dataset.album  || "",
    artist: a.dataset.artist || "",
  }));
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
  // Save the currently open release so the player bar can reopen it
  const opParam = new URLSearchParams(location.search).get("op");
  if (opParam && opParam.includes(":")) {
    const [pType, pId] = [opParam.slice(0, opParam.indexOf(":")), opParam.slice(opParam.indexOf(":") + 1)];
    window._playerReleaseType = pType;
    window._playerReleaseId   = pId;
    window._playerReleaseUrl  = `https://www.discogs.com/${pType}/${pId}`;
  }
  setVideoUrl(id);
  const mp = document.getElementById("mini-player");
  mp.classList.add("open");
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
  try {
    if (state === 1) ytPlayer.pauseVideo();
    else ytPlayer.playVideo();
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
}

function openPlayerRelease() {
  const rType = window._playerReleaseType;
  const rId   = window._playerReleaseId;
  const rUrl  = window._playerReleaseUrl;
  if (rType && rId) {
    openModal(null, rId, rType, rUrl);
  }
}

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
  const mp = document.getElementById("mini-player");
  mp.classList.remove("open", "expanded");
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
  // Hide album + share buttons
  const albumBtn = document.getElementById("mini-album");
  if (albumBtn) albumBtn.style.display = "none";
  const shareBtn = document.getElementById("mini-share");
  if (shareBtn) shareBtn.style.display = "none";
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
      const nameEl = a.id
        ? `<a href="#" class="modal-internal-link credit-name" data-alt-name="${escHtml(a.name)}" data-alt-id="${a.id}" onclick="selectAltArtist(event,this);closeModal()" title="Search for ${escHtml(a.name)}">${escHtml(a.name)}</a>`
        : `<span class="credit-name">${escHtml(a.name)}</span>`;
      const searchIcon = ` <a href="#" class="album-title-search" onclick="event.preventDefault();searchCollectionFor('cw-artist','${escHtml(a.name.replace(/'/g, "\\'"))}')" title="Search your collection for ${escHtml(a.name)}" style="font-size:1.1em">⌕</a>${wikiIcon(stripDupSuffix(a.name), a.name)}`;
      // Role parentheses come right after the name; ⌕/W go AFTER the role.
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
  function findVideo(trackTitle) {
    const tl = trackTitle.toLowerCase();
    for (const [vt, uri] of videoMap) {
      if (vt.includes(tl) || tl.includes(vt)) return uri;
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
    labelNames.length ? `<span class="detail-label">Label</span><span>${labelNames.map(n => {
      const esc = n.replace(/'/g, "\\'");
      return `<a href="#" class="modal-internal-link" onclick="event.preventDefault();closeModal();clearForm();document.getElementById('f-label').value='${escHtml(esc)}';applyEntityLinkDefaults();toggleAdvanced(true);doSearch(1)" title="Search for ${escHtml(n)} releases">${escHtml(n)}</a> <a href="#" class="catno-collection-search" onclick="event.preventDefault();searchCollectionFor('cw-label','${escHtml(esc)}')" title="Search your collection for ${escHtml(n)}">⌕</a>${wikiIcon(stripDupSuffix(n), n, "record label")}`;
    }).join(", ")}</span>` : "",
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
  const playableUrls = tracks.map(t => findVideo(t.title || "")).filter(Boolean);
  const playableCount = playableUrls.length;
  const firstPlayableUrl = playableUrls[0] || "";
  const playableMeta = playableCount
    ? `<span class="tracklist-playable">(${playableCount}${firstPlayableUrl ? ` <a href="#" class="tracklist-play-all" onclick="event.preventDefault();event.stopPropagation();openVideo(event,'${firstPlayableUrl.replace(/'/g, "\\'")}')" title="Play the first playable track">▶</a>` : ""})</span>`
    : "";
  const tracklistOpen = localStorage.getItem("tracklist-open") !== "false";
  const trackHTML = tracks.length ? `
    <div class="album-tracklist">
      <div class="tracklist-header">
        <div class="tracklist-heading tracklist-toggle" onclick="toggleTracklist(this)" title="Click to collapse/expand tracklist"><span class="tracklist-arrow">${tracklistOpen ? "▼" : "▶"}</span> Tracklist ${playableMeta}</div>
        <input type="text" class="tracklist-filter" placeholder="filter tracks…" oninput="filterTracks(this)" />
      </div>
      <div class="tracklist-body"${tracklistOpen ? "" : ' style="display:none"'}>
      ${tracks.map(t => {
        const url = findVideo(t.title || "");
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
          ? `<a class="track-play-btn track-link" href="#" data-video="${escHtml(url)}" data-track="${escHtml(t.title || "")}" data-album="${escHtml(title)}" data-artist="${escHtml(trackArtist)}" onclick="openVideo(event,'${url.replace(/'/g, "\\'")}')" title="Play this track">▶</a>`
          : "";
        // Track title is now a Discogs new-search link for the track name.
        const titleLink = t.title
          ? `<a href="#" class="track-title-link" onclick="event.preventDefault();closeModal();clearForm();document.getElementById('query').value='${escHtml(trackSearchQ)}';doSearch(1)" title="Search Discogs for &quot;${escHtml(t.title)}&quot;">${escHtml(t.title)}</a>`
          : "";
        // Magnifying glass searches the user's records for the track (orange via .track-search-icon).
        const searchIcon = t.title
          ? ` <a class="track-search-icon" href="#" onclick="event.preventDefault();searchCollectionFor('cw-query','${escHtml(trackSearchQ)}')" title="Search your records for &quot;${escHtml(t.title)}&quot;">⌕</a>`
          : "";
        // W → wiki popup for the track title (artist context helps
        // disambiguation). Quote the title via wikiIcon's phrase arg and
        // pass the artist as unquoted extra terms so the search becomes
        // `"Hi-Heel Sneakers" Tommy Tucker` instead of one big phrase.
        const wikiW = t.title
          ? wikiIcon(t.title, t.title, trackArtist || "")
          : "";
        // External YouTube search at the end of the title row. Shown for
        // every track (even ones with an in-app ▶) because embedded
        // playback sometimes fails or the video is unavailable in the
        // user's region — a one-click fallback to a YouTube search keeps
        // listening uninterrupted.
        const ytSearchEnd = t.title
          ? ` <a class="track-yt-search-end" href="https://www.youtube.com/results?search_query=${ytQuery}" target="_blank" rel="noopener" title="Search on YouTube"><svg width="14" height="10" viewBox="0 0 16 11" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="16" height="11" rx="2.5" fill="#8a2a22"/><path d="M6.5 3L11 5.5L6.5 8V3Z" fill="#e8dcc8"/></svg></a>`
          : "";
        const trackCredits = (t.extraartists ?? []).length
          ? `<div class="track-credits">${t.extraartists.map(a => {
              const nameEl = a.id
                ? `<a href="#" class="modal-internal-link credit-name" data-alt-name="${escHtml(a.name)}" data-alt-id="${a.id}" onclick="selectAltArtist(event,this);closeModal()" title="Search for ${escHtml(a.name)}">${escHtml(a.name)}</a>`
                : `<span class="credit-name">${escHtml(a.name)}</span>`;
              const credSearchIcon = ` <a href="#" class="album-title-search" onclick="event.preventDefault();searchCollectionFor('cw-artist','${escHtml(a.name.replace(/'/g, "\\'"))}')" title="Search your collection for ${escHtml(a.name)}" style="font-size:1.1em">\u2315</a>${wikiIcon(stripDupSuffix(a.name), a.name)}`;
              // Role parentheses come right after the name; the inventory \u2315
              // and wiki W icons go AFTER the role so the line reads
              // "Name (Role) \u2315 W" instead of "Name \u2315 W (Role)".
              return `${nameEl}${a.role ? ` <span class="credit-role">(${escHtml(a.role)})</span>` : ""}${credSearchIcon}`;
            }).join('<span class="credit-sep"> · </span>')}</div>`
          : "";
        return `<div class="track">
          <span class="track-play-cell">${playCell}</span>
          <span class="track-pos">${escHtml(t.position || "")}</span>
          <span class="track-title">${titleLink}${searchIcon}${wikiW}${ytSearchEnd}${trackCredits}</span>
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
        <h2><a href="#" class="modal-title-link" onclick="event.preventDefault();searchCollectionFor('cw-release','${escHtml(title.replace(/'/g, "\\'"))}')" title="Search your collection for this release">${escHtml(title)}</a> <a href="#" class="album-title-search" onclick="event.preventDefault();searchCollectionFor('cw-release','${escHtml(title.replace(/'/g, "\\'"))}')" title="Search your collection for this release">⌕</a>${wikiIcon(stripDupSuffix(title), title, stripDupSuffix(artists[0] || ""))}</h2>
        ${artists.length ? `<div class="album-artist">${artists.map(n => `<a href="#" class="modal-artist-link" data-artist="${escHtml(n)}" onclick="searchArtistFromModal(event,this)" title="Search for ${escHtml(n)}">${escHtml(n)}</a> <a href="#" class="album-title-search" onclick="event.preventDefault();searchCollectionFor('cw-artist','${escHtml(n.replace(/'/g, "\\'"))}')" title="Search your collection for ${escHtml(n)}">⌕</a>${wikiIcon(stripDupSuffix(n), n)}`).join(", ")}</div>` : ""}
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

    let badges = "";
    const inCol = type === "release" && window._collectionIds?.has(id);
    const inWant = type === "release" && window._wantlistIds?.has(id);
    if (type === "release") {
      badges += `<span class="card-badge badge-collection${inCol ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleCollectionFromCard(this,${id})" title="${inCol ? "Remove from collection" : "Add to collection"}">C</span>`;
      badges += `<span class="card-badge badge-wantlist${inWant ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleWantlistFromCard(this,${id})" title="${inWant ? "Remove from wantlist" : "Add to wantlist"}">W</span>`;
      const lists = window._listMembership?.[id];
      if (lists?.length) {
        const names = lists.map(l => l.listName).join(", ");
        badges += `<span class="card-badge badge-list" title="In list: ${escHtml(names)}">L</span>`;
      }
      if (window._inventoryIds?.has(id)) badges += `<span class="card-badge badge-inventory" title="In your inventory">I</span>`;
    }
    const favKey = `${type}:${id}`;
    const isFav = window._favoriteKeys?.has(favKey);
    badges += `<span class="card-badge badge-favorite${isFav ? " is-favorite" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleFavoriteFromCard(this,${id},'${type}')" title="${isFav ? "Remove from favorites" : "Add to favorites"}">F</span>`;
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
    const badge = `<span class="mv-dots">` +
      `<span class="mv-dot${inCol ? ' active' : ''}" style="background:${inCol ? '#6ddf70' : ''}" onclick="event.preventDefault();event.stopPropagation();mvToggleCol(this,${v.id})" title="${inCol ? 'In collection — click to remove' : 'Add to collection'}"></span>` +
      `<span class="mv-dot${inWant ? ' active' : ''}" style="background:${inWant ? '#f0c95c' : ''}" onclick="event.preventDefault();event.stopPropagation();mvToggleWant(this,${v.id})" title="${inWant ? 'In wantlist — click to remove' : 'Add to wantlist'}"></span>` +
      `<span class="mv-dot${isFav ? ' active' : ''}" style="background:${isFav ? '#ff6b35' : ''}" onclick="event.preventDefault();event.stopPropagation();mvToggleFav(this,${v.id})" title="${isFav ? 'Favorited — click to remove' : 'Add to favorites'}"></span>` +
      (inList ? `<span class="mv-dot active" style="background:#a0ccf0" title="${escHtml(listNames ? `In your list${window._listMembership[v.id].length > 1 ? "s" : ""}: ${listNames}` : "In one of your lists")}"></span>` : '') +
      (inInv ? `<span class="mv-dot active" style="background:#cda0f5" title="In your inventory"></span>` : '') +
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
  document.body.classList.add("modal-open");

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
  if (!document.getElementById("modal-overlay")?.classList.contains("open") &&
      !document.getElementById("version-overlay")?.classList.contains("open")) {
    document.body.classList.remove("modal-open");
  }
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
