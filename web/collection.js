// ── Collection / Wantlist / Wants tab state ──────────────────────────────
let _activeTab = "search";
let _colPage = 1, _wlPage = 1;

function setCwStatus(msg) {
  const el = document.getElementById("cw-status");
  if (!el) return;
  el.textContent = msg || "";
  el.style.display = msg ? "block" : "none";
}

function saveCwSort() {
  const v = document.getElementById("cw-sort")?.value ?? "";
  try { localStorage.setItem("cw-sort", v); } catch {}
  // Mirror sort to URL
  const u = new URL(window.location.href);
  if (v) u.searchParams.set("sort", v);
  else u.searchParams.delete("sort");
  history.replaceState(history.state, "", u.toString());
}
function restoreCwSort() {
  try {
    const v = localStorage.getItem("cw-sort");
    if (v !== null) { const el = document.getElementById("cw-sort"); if (el) el.value = v; }
  } catch {}
}

function renderCardFromBasicInfo(basicInfo, index) {
  const artistName = (basicInfo.artists ?? []).map(a => a.name).join(", ");
  const labelStr   = (basicInfo.labels  ?? []).map(l => l.name).join(", ");
  const formatStr  = (basicInfo.formats ?? []).map(f => f.name + (f.descriptions?.length ? ` (${f.descriptions.join(", ")})` : "")).join(" · ");
  const genreStr   = (basicInfo.genres  ?? [])[0] ?? "";

  const catno = (basicInfo.labels ?? []).map(l => l.catno).filter(Boolean)[0] ?? "";

  const syntheticItem = {
    id:           basicInfo.id,
    type:         "release",
    title:        artistName ? `${artistName} - ${basicInfo.title}` : basicInfo.title,
    cover_image:  basicInfo.cover_image || basicInfo.thumb || "",
    label:        (basicInfo.labels ?? []).map(l => l.name),
    format:       (basicInfo.formats ?? []).map(f => f.name),
    genre:        basicInfo.genres ?? [],
    year:         String(basicInfo.year ?? ""),
    country:      "",
    catno:        catno,
    uri:          basicInfo.id ? `/release/${basicInfo.id}` : "",
    _rating:      basicInfo._rating ?? 0,
    _notes:       basicInfo._notes ?? [],
  };
  return renderCard(syntheticItem, index);
}

function addNavTab(view) {
  // Map old collection/wantlist enables to the new "records" tab
  if (view === "collection" || view === "wantlist") view = "records";
  const el = document.querySelector(`#main-nav-tabs [data-view="${view}"]`);
  if (!el) return;
  // Replace the sign-in link with a proper nav button
  if (el.tagName === "A" && view === "records") {
    const btn = document.createElement("button");
    btn.className = "main-nav-tab";
    btn.dataset.view = view;
    btn.textContent = "My Records";
    btn.onclick = () => switchView(view);
    el.replaceWith(btn);
    // Enable swap-to-collection button now that user has records access
    const swapBtn = document.getElementById("swap-to-collection-btn");
    if (swapBtn) { swapBtn.disabled = false; swapBtn.title = "Search your collection with these criteria"; }
  } else {
    el.classList.remove("nav-disabled");
    el.removeAttribute("title");
  }
}

// toggleMobileNav — now in shared.js

function switchView(view, skipPushState = false) {
  document.getElementById("main-nav-tabs")?.classList.remove("mobile-open");
  const tabBtn = document.querySelector(`#main-nav-tabs [data-view="${view}"]`);
  if (tabBtn?.classList.contains("nav-disabled")) return;

  document.querySelectorAll(".main-nav-tab").forEach(btn =>
    btn.classList.toggle("active", btn.dataset.view === view)
  );
  const searchView  = document.getElementById("search-view");
  const dropsView   = document.getElementById("drops-view");
  const liveView    = document.getElementById("live-view");
  const buyView     = document.getElementById("buy-view");
  const gearView    = document.getElementById("gear-view");
  const feedView    = document.getElementById("feed-view");
  const infoView    = document.getElementById("info-view");
  const privacyView = document.getElementById("privacy-view");
  const termsView   = document.getElementById("terms-view");
  if (!skipPushState) {
    if (view === "records") {
      const tab = _cwTab || "collection";
      const url = tab === "collection" ? "?view=records" : `?view=records&tab=${tab}`;
      history.pushState({ view, tab }, "", url);
    } else if (view === "drops" || view === "live" || view === "buy" || view === "gear" || view === "feed" || view === "info" || view === "privacy" || view === "terms" || view === "wanted") {
      history.pushState({ view }, "", "?view=" + view);
    } else {
      history.pushState({}, "", location.pathname);
    }
  }
  if (typeof gtag === "function") {
    const titles = { drops: "Drops", live: "Live", buy: "Buy", gear: "Gear", feed: "Feed", info: "Info", privacy: "Privacy Policy", terms: "Terms of Service", records: "My Records", wanted: "Wants", search: "Search" };
    gtag("event", "page_view", {
      page_location: window.location.href,
      page_path:     window.location.pathname + window.location.search,
      page_title:    "SeaDisco – " + (titles[view] ?? view)
    });
  }
  if (searchView)  searchView.style.display  = "none";
  if (dropsView)   dropsView.style.display   = "none";
  if (liveView)    liveView.style.display    = "none";
  if (buyView)     buyView.style.display      = "none";
  if (gearView)    gearView.style.display    = "none";
  if (feedView)    feedView.style.display    = "none";
  if (infoView)    infoView.style.display    = "none";
  if (privacyView) privacyView.style.display = "none";
  if (termsView)   termsView.style.display   = "none";

  const mainForm    = document.getElementById("main-search-form");
  const recordsWrap = document.getElementById("records-wrap");
  const cwInput     = document.getElementById("cw-query");
  const wantedWrap  = document.getElementById("wanted-search-wrap");

  if (view === "buy") {
    if (buyView) buyView.style.display = "block";
    if (mainForm) mainForm.style.display = "none";
    if (recordsWrap) recordsWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
    loadBuyListings();
  } else if (view === "gear") {
    if (gearView) gearView.style.display = "block";
    if (mainForm) mainForm.style.display = "none";
    if (recordsWrap) recordsWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
    loadGearListings();
  } else if (view === "feed") {
    if (feedView) feedView.style.display = "block";
    if (mainForm) mainForm.style.display = "none";
    if (recordsWrap) recordsWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
    loadFeedArticles();
  } else if (view === "drops") {
    if (dropsView) dropsView.style.display = "block";
    if (mainForm) mainForm.style.display = "";
    if (recordsWrap) recordsWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
  } else if (view === "live") {
    if (liveView) liveView.style.display = "block";
    if (mainForm) mainForm.style.display = "none";
    if (recordsWrap) recordsWrap.style.display = "none";
    if (typeof loadLiveRecentFeed === "function") loadLiveRecentFeed();
    if (wantedWrap) wantedWrap.style.display = "none";
  } else if (view === "info") {
    if (infoView) infoView.style.display = "block";
    if (mainForm) mainForm.style.display = "";
    if (recordsWrap) recordsWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
  } else if (view === "privacy") {
    if (privacyView) privacyView.style.display = "block";
    if (mainForm) mainForm.style.display = "none";
    if (recordsWrap) recordsWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
  } else if (view === "terms") {
    if (termsView) termsView.style.display = "block";
    if (mainForm) mainForm.style.display = "none";
    if (recordsWrap) recordsWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
  } else if (view === "wanted") {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "none";
    if (recordsWrap) recordsWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "";
    document.getElementById("artist-alts").innerHTML = "";
    loadWantedTab();
  } else if (view === "records") {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "none";
    if (recordsWrap) recordsWrap.style.display = "";
    if (wantedWrap) wantedWrap.style.display = "none";
    document.getElementById("artist-alts").innerHTML = "";
    const ws1 = document.getElementById("favorites-sample"); if (ws1) ws1.style.display = "none";
    switchRecordsTab(_cwTab || "collection", true);
  } else {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "";
    if (recordsWrap) recordsWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
    setCwStatus("");
    // If we have previous search results, restore them instead of clearing
    if (window._lastResults && window._lastResults.length > 0) {
      // Re-render saved search results and keep search info visible
      const grid = document.getElementById("results");
      grid.innerHTML = window._lastResults.map((item, i) => renderCard(item, i)).join("");
      document.getElementById("pagination").style.display = "none";
      const ws = document.getElementById("favorites-sample"); if (ws) ws.style.display = "none";
      const blurb = document.getElementById("blurb"); if (blurb) blurb.style.display = "none";
      // Show load-more if there are more pages
      const lmWrap = document.getElementById("search-load-more");
      if (lmWrap) lmWrap.style.display = currentPage < totalPages ? "" : "none";
    } else {
      // No previous search — restore clean default state
      document.getElementById("results").innerHTML = "";
      document.getElementById("pagination").style.display = "none";
      document.getElementById("status").textContent = "";
      const searchDesc = document.getElementById("search-desc");
      if (searchDesc) searchDesc.textContent = "";
      const searchReturned = document.getElementById("search-returned");
      if (searchReturned) searchReturned.textContent = "";
      const blurb = document.getElementById("blurb");
      if (blurb) blurb.style.display = "";
      const ws = document.getElementById("favorites-sample");
      if (ws) ws.style.display = "";
      // Refresh favorites in case user favorited items from another tab
      if (typeof updateFavoritesHeading === "function") updateFavoritesHeading();
      if (window._favoriteKeys?.size > 0 && typeof loadFavoritesGrid === "function") loadFavoritesGrid();
      const artistAlts = document.getElementById("artist-alts");
      if (artistAlts) artistAlts.innerHTML = "";
    }
    if (!skipPushState) history.replaceState({}, "", location.pathname);
  }

  // Animate the entering view
  const shownId = { search: "search-view", drops: "drops-view", live: "live-view", buy: "buy-view", gear: "gear-view", feed: "feed-view", info: "info-view", privacy: "privacy-view", terms: "terms-view", records: "search-view", wanted: "search-view" }[view];
  const shownEl = shownId && document.getElementById(shownId);
  if (shownEl) {
    shownEl.classList.remove("view-enter");
    void shownEl.offsetWidth;
    shownEl.classList.add("view-enter");
  }
}

// ── Collection / Wantlist local search ──
let _cwTab = "collection";
let _cwQuery = "";
let _cwAdvOpen = false;
let _cwFolderId = 0; // active folder filter (0 = all)

async function loadCollectionFolders() {
  const el = document.getElementById("cw-folder-cloud");
  if (!el) return;
  el.innerHTML = "";
  _cwFolderId = 0;
  try {
    const data = await apiFetch("/api/user/folders").then(r => r.json());
    const folders = data.folders ?? [];
    if (!folders.length) { el.style.display = "none"; return; }
    el.style.display = "";
    const totalCount = folders.reduce((sum, f) => sum + (f.count ?? 0), 0);
    let html = `<span class="pill cw-folder-pill active" data-folder="0" onclick="filterByFolder(0)" title="All folders (${totalCount} items)">All</span>`;
    html += folders.map(f =>
      `<span class="pill cw-folder-pill" data-folder="${f.folderId}" onclick="filterByFolder(${f.folderId})" title="Folder: ${escHtml(f.name)} (${f.count} items)">${escHtml(f.name)}</span>`
    ).join("");
    el.innerHTML = html;
  } catch { el.style.display = "none"; }
}

function filterByFolder(folderId) {
  _cwFolderId = folderId;
  const pills = document.querySelectorAll(".cw-folder-pill");
  pills.forEach(p => p.classList.toggle("active", parseInt(p.dataset.folder) === folderId));
  loadCollectionTab(1);
}

function toggleCwAdvanced(forceOpen) {
  const panel = document.getElementById("cw-advanced-panel");
  const arrow = document.getElementById("cw-advanced-arrow");
  if (!panel) return;
  _cwAdvOpen = forceOpen === true ? true : forceOpen === false ? false : !_cwAdvOpen;
  panel.style.display = _cwAdvOpen ? "" : "none";
  if (arrow) arrow.textContent = _cwAdvOpen ? "▼" : "▶";
}

function getCwFilters() {
  const f = {};
  const q       = (document.getElementById("cw-query")?.value   ?? "").trim();
  const artist  = (document.getElementById("cw-artist")?.value  ?? "").trim();
  const release = (document.getElementById("cw-release")?.value ?? "").trim();
  const label   = (document.getElementById("cw-label")?.value   ?? "").trim();
  const year    = (document.getElementById("cw-year")?.value    ?? "").trim();
  const genre   = (document.getElementById("cw-genre")?.value   ?? "").trim();
  const style   = (document.getElementById("cw-style")?.value   ?? "").trim();
  const format  = (document.getElementById("cw-format")?.value  ?? "").trim();
  const rtype   = document.querySelector('input[name="cw-result-type"]:checked')?.value ?? "";
  if (q)       f.q       = q;
  if (artist)  f.artist  = artist;
  if (release) f.release = release;
  if (label)   f.label   = label;
  if (year)    f.year    = year;
  if (genre)   f.genre   = genre;
  if (style)   f.style   = style;
  if (format)  f.format  = format;
  if (rtype)   f.type    = rtype;
  return f;
}

// ── Swap search criteria between main ↔ collection ──────────────────────
function swapSearchToCollection() {
  // Gather current main search fields and store as pending swap
  // (switchView → switchRecordsTab → clearCwFilters runs before we can set fields)
  // Map main sort → collection sort
  const mainSort = document.getElementById("f-sort")?.value ?? "";
  const sortMap = { "year:desc": "year", "year:asc": "year_asc", "title:asc": "title", "title:desc": "title" };
  window._pendingCwSwap = {
    q:       (document.getElementById("query")?.value ?? "").trim(),
    artist:  (document.getElementById("f-artist")?.value ?? "").trim(),
    release: (document.getElementById("f-release")?.value ?? "").trim(),
    label:   (document.getElementById("f-label")?.value ?? "").trim(),
    year:    (document.getElementById("f-year")?.value ?? "").trim(),
    genre:   (document.getElementById("f-genre")?.value ?? "").trim(),
    style:   (document.getElementById("f-style")?.value ?? "").trim(),
    format:  (document.getElementById("f-format")?.value ?? "").trim(),
    rtype:   document.querySelector('input[name="result-type"]:checked')?.value ?? "",
    sort:    sortMap[mainSort] ?? "",
  };
  switchView("records");
}

function swapSearchToMain() {
  // Gather current collection search fields
  const q       = (document.getElementById("cw-query")?.value ?? "").trim();
  const artist  = (document.getElementById("cw-artist")?.value ?? "").trim();
  const release = (document.getElementById("cw-release")?.value ?? "").trim();
  const label   = (document.getElementById("cw-label")?.value ?? "").trim();
  const year    = (document.getElementById("cw-year")?.value ?? "").trim();
  const genre   = (document.getElementById("cw-genre")?.value ?? "").trim();
  const style   = (document.getElementById("cw-style")?.value ?? "").trim();
  const format  = (document.getElementById("cw-format")?.value ?? "").trim();
  const rtype   = document.querySelector('input[name="cw-result-type"]:checked')?.value ?? "";

  // Switch to search view
  switchView("search");

  // Populate main search fields
  const mainQ = document.getElementById("query");
  if (mainQ) mainQ.value = q;

  const hasAdvanced = artist || release || label || year || genre || style || format;
  if (hasAdvanced) {
    toggleAdvanced(true);
    if (artist)  document.getElementById("f-artist").value  = artist;
    if (release) document.getElementById("f-release").value = release;
    if (label)   document.getElementById("f-label").value   = label;
    if (year)    document.getElementById("f-year").value    = year;
    if (genre)   document.getElementById("f-genre").value   = genre;
    if (style)   document.getElementById("f-style").value   = style;
    if (format)  document.getElementById("f-format").value  = format;
  }

  // Map result type (master/release)
  if (rtype === "master" || rtype === "release") {
    const radio = document.querySelector(`input[name="result-type"][value="${rtype}"]`);
    if (radio) radio.checked = true;
  }

  // Map collection sort → main sort
  const cwSort = document.getElementById("cw-sort")?.value ?? "";
  const revSortMap = { "year": "year:desc", "year_asc": "year:asc", "title": "title:asc" };
  const mainSortEl = document.getElementById("f-sort");
  if (mainSortEl && revSortMap[cwSort]) mainSortEl.value = revSortMap[cwSort];
  else if (mainSortEl) mainSortEl.value = "";

  doSearch(1);
}

function doCwSearch(page = 1) {
  const filters = getCwFilters();
  _cwQuery = filters.q || "";
  if (_cwTab === "collection") {
    loadCollectionTab(page, filters);
  } else if (_cwTab === "wantlist") {
    loadWantlistTab(page, filters);
  } else if (_cwTab === "inventory") {
    loadInventoryTab(page, filters);
  } else if (_cwTab === "lists") {
    loadListsTab();
  } else if (_cwTab === "favorites") {
    renderFavoritesTabGrid();
  }
}

function switchRecordsTab(tab, skipPush) {
  _cwTab = tab;
  _cwQuery = "";
  // Update URL to reflect active sub-tab
  if (!skipPush) {
    const sort = document.getElementById("cw-sort")?.value || "";
    let url = tab === "collection" ? "?view=records" : `?view=records&tab=${tab}`;
    if (sort) url += `&sort=${sort}`;
    history.pushState({ view: "records", tab }, "", url);
  }
  // Update sub-tab active state
  document.querySelectorAll(".records-sub-tab").forEach(btn =>
    btn.classList.toggle("active", btn.dataset.rtab === tab)
  );
  // Reset search
  const cwInput = document.getElementById("cw-query");
  const controlsRow = document.getElementById("cw-controls-row");
  const advPanel = document.getElementById("cw-advanced-panel");
  const folderCloud = document.getElementById("cw-folder-cloud");
  const exportBtn = document.getElementById("cw-export-btn");

  clearCwFilters();

  // Apply pending collection search from modal (searchCollectionFor sets this)
  const pending = window._pendingCwSearch;
  if (pending) {
    delete window._pendingCwSearch;
    const el = document.getElementById(pending.field);
    if (el) el.value = pending.value;
    if (pending.field !== "cw-query") toggleCwAdvanced(true);
  }

  // Apply pending swap from main search (swapSearchToCollection sets this)
  const swap = window._pendingCwSwap;
  if (swap) {
    delete window._pendingCwSwap;
    if (swap.q && cwInput) cwInput.value = swap.q;
    const hasAdv = swap.artist || swap.release || swap.label || swap.year || swap.genre || swap.style || swap.format;
    if (hasAdv) {
      toggleCwAdvanced(true);
      if (swap.artist)  document.getElementById("cw-artist").value  = swap.artist;
      if (swap.release) document.getElementById("cw-release").value = swap.release;
      if (swap.label)   document.getElementById("cw-label").value   = swap.label;
      if (swap.year)    document.getElementById("cw-year").value    = swap.year;
      if (swap.format)  document.getElementById("cw-format").value  = swap.format;
      if (swap.genre)   { document.getElementById("cw-genre").value = swap.genre; onCwGenreChange(); }
      if (swap.style)   document.getElementById("cw-style").value   = swap.style;
    }
    if (swap.rtype === "master" || swap.rtype === "release") {
      const radio = document.querySelector(`input[name="cw-result-type"][value="${swap.rtype}"]`);
      if (radio) radio.checked = true;
    }
    if (swap.sort !== undefined) {
      const cwSortEl = document.getElementById("cw-sort");
      if (cwSortEl) cwSortEl.value = swap.sort;
    }
  }

  const hasPending = pending || swap;

  if (tab === "collection") {
    if (cwInput) { cwInput.placeholder = "Search your collection\u2026"; if (!hasPending) cwInput.value = ""; }
    if (controlsRow) controlsRow.style.display = "";
    if (exportBtn) exportBtn.style.display = "";
    loadCwFacets("collection");
    loadCollectionFolders();
    loadCollectionTab(1);
  } else if (tab === "wantlist") {
    if (cwInput) { cwInput.placeholder = "Search your wantlist\u2026"; cwInput.value = ""; }
    if (controlsRow) controlsRow.style.display = "";
    if (exportBtn) exportBtn.style.display = "";
    if (folderCloud) folderCloud.style.display = "none";
    loadCwFacets("wantlist");
    loadWantlistTab(1);
  } else if (tab === "inventory") {
    if (cwInput) { cwInput.placeholder = "Search your inventory\u2026"; cwInput.value = ""; }
    if (controlsRow) controlsRow.style.display = "none";
    if (advPanel) advPanel.style.display = "none";
    if (folderCloud) folderCloud.style.display = "none";
    loadInventoryTab(1);
  } else if (tab === "lists") {
    if (cwInput) { cwInput.placeholder = "Search your lists\u2026"; cwInput.value = ""; }
    if (controlsRow) controlsRow.style.display = "none";
    if (advPanel) advPanel.style.display = "none";
    if (folderCloud) folderCloud.style.display = "none";
    loadListsTab();
  } else if (tab === "favorites") {
    if (cwInput) { cwInput.placeholder = "Search your favorites\u2026"; cwInput.value = ""; }
    if (controlsRow) controlsRow.style.display = "none";
    if (advPanel) advPanel.style.display = "none";
    if (folderCloud) folderCloud.style.display = "none";
    loadFavoritesTab();
  }
}

let _invPage = 1;
async function loadInventoryTab(page = 1, filters) {
  _invPage = page;
  const f = filters || {};
  const q = document.getElementById("cw-query")?.value?.trim() || "";
  if (q) f.q = q;
  setActiveTab("inventory");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = renderSkeletonGrid(16);
  document.getElementById("pagination").style.display = "none";
  setCwStatus("");
  try {
    let url = `/api/user/inventory?page=${page}&per_page=96`;
    if (f.q) url += `&q=${encodeURIComponent(f.q)}`;
    const r = await apiFetch(url);
    const data = await r.json();
    const items = data.items ?? [];
    if (!items.length) {
      setCwStatus("");
      document.getElementById("results").innerHTML = f.q
        ? renderEmptyState("\uD83D\uDD0D", `No inventory items matching "${f.q}"`, "Try a different search")
        : renderEmptyState("\uD83D\uDCE6", "No inventory items synced", "Your Discogs marketplace inventory will appear here after syncing");
      return;
    }
    setCwStatus(`${data.total} inventory listings \u2014 page ${page} of ${data.pages}`);
    document.getElementById("results").innerHTML = items.map((item, i) => renderInventoryCard(item, i)).join("");
    totalPages = data.pages;
    currentPage = page;
    renderInventoryPagination();
  } catch (e) {
    setCwStatus("Failed to load inventory: " + e.message);
  }
}

function renderInventoryCard(item, index) {
  const d = item.data || {};
  const release = d.release || {};
  const artist = release.artist || release.description || "";
  const title = release.title || release.description || "Untitled";
  const thumb = release.thumbnail || release.images?.[0]?.uri150 || "";
  const price = item.price_value ? `${item.price_currency || "$"}${Number(item.price_value).toFixed(2)}` : "";
  const cond = item.condition || "";
  const status = item.status || "For Sale";

  const catno = release.catno || "";

  const syntheticItem = {
    id: item.discogs_release_id || release.id || 0,
    type: "release",
    title: artist ? `${artist} - ${title}` : title,
    cover_image: thumb,
    label: [],
    format: release.format ? [release.format] : [],
    genre: [],
    year: "",
    country: "",
    catno: catno,
    uri: item.discogs_release_id ? `/release/${item.discogs_release_id}` : "",
    _price: price,
    _condition: cond,
    _status: status,
  };
  return renderCard(syntheticItem, index);
}

function renderInventoryPagination() {
  const pag = document.getElementById("pagination");
  if (totalPages <= 1) { pag.style.display = "none"; return; }
  const goTo = (p) => { window.scrollTo({top:0,behavior:'smooth'}); loadInventoryTab(p); };

  const pages = new Set();
  pages.add(1);
  pages.add(totalPages);
  for (let i = currentPage - 2; i <= currentPage + 2; i++) {
    if (i >= 1 && i <= totalPages) pages.add(i);
  }
  const sorted = [...pages].sort((a, b) => a - b);

  let html = `<button class="pag-arrow" ${currentPage <= 1 ? "disabled" : ""} onclick="return false">← Prev</button>`;
  let last = 0;
  for (const p of sorted) {
    if (last && p - last > 1) html += `<span class="pag-ellipsis">…</span>`;
    html += `<button class="pag-num${p === currentPage ? " pag-active" : ""}" data-page="${p}">${p}</button>`;
    last = p;
  }
  html += `<button class="pag-arrow" ${currentPage >= totalPages ? "disabled" : ""} onclick="return false">Next →</button>`;

  pag.innerHTML = html;
  pag.style.display = "flex";

  pag.querySelector(".pag-arrow:first-child").onclick = currentPage > 1 ? () => goTo(currentPage - 1) : null;
  pag.querySelector(".pag-arrow:last-child").onclick = currentPage < totalPages ? () => goTo(currentPage + 1) : null;
  pag.querySelectorAll(".pag-num").forEach(btn => {
    btn.onclick = () => goTo(parseInt(btn.dataset.page));
  });
}

let _favTabItems = [];
let _favTabQuery = "";

async function loadFavoritesTab() {
  setActiveTab("favorites");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = renderSkeletonGrid(16);
  document.getElementById("pagination").style.display = "none";
  setCwStatus("");
  try {
    const r = await apiFetch("/api/user/favorites?limit=200");
    if (!r.ok) throw new Error("Failed to load favorites");
    const data = await r.json();
    _favTabItems = (data.items ?? []).map(row => row.data);
    renderFavoritesTabGrid();
  } catch (e) {
    setCwStatus("Failed to load favorites: " + e.message);
  }
}

function renderFavoritesTabGrid() {
  const q = (document.getElementById("cw-query")?.value ?? "").trim().toLowerCase();
  _favTabQuery = q;
  let items = _favTabItems;
  if (q) {
    items = items.filter(it => {
      const title = (it.title ?? "").toLowerCase();
      const label = (it.label ?? []).join(" ").toLowerCase();
      const genre = (it.genre ?? []).join(" ").toLowerCase();
      const year = String(it.year ?? "");
      return title.includes(q) || label.includes(q) || genre.includes(q) || year.includes(q);
    });
  }
  const grid = document.getElementById("results");
  if (!items.length) {
    setCwStatus("");
    grid.innerHTML = q
      ? renderEmptyState("\uD83D\uDD0D", `No favorites matching "${q}"`, "Try a different search")
      : renderEmptyState("♡", "No favorites yet", "Favorite albums, artists & labels from search results to see them here");
    document.getElementById("pagination").style.display = "none";
    return;
  }
  setCwStatus(`${items.length} favorite${items.length !== 1 ? "s" : ""}`);
  grid.innerHTML = items.map((item, i) => renderCard(item, i)).join("");
  document.getElementById("pagination").style.display = "none";
}

async function loadListsTab() {
  setActiveTab("lists");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = renderSkeletonGrid(16);
  document.getElementById("pagination").style.display = "none";
  setCwStatus("");
  try {
    const r = await apiFetch("/api/user/lists");
    const data = await r.json();
    const lists = data.lists ?? [];
    const q = (document.getElementById("cw-query")?.value ?? "").trim().toLowerCase();
    const filtered = q ? lists.filter(l => (l.name || "").toLowerCase().includes(q) || (l.description || "").toLowerCase().includes(q)) : lists;
    if (!filtered.length) {
      setCwStatus("");
      document.getElementById("results").innerHTML = lists.length
        ? renderEmptyState("\uD83D\uDD0D", `No lists matching "${q}"`, "Try a different search")
        : renderEmptyState("\uD83D\uDCCB", "No lists synced", "Your Discogs lists will appear here after syncing");
      return;
    }
    setCwStatus(`${filtered.length} list${filtered.length !== 1 ? "s" : ""}`);
    document.getElementById("results").innerHTML = `<div class="lists-grid">${filtered.map(renderListCard).join("")}</div>`;
  } catch (e) {
    setCwStatus("Failed to load lists: " + e.message);
  }
}

function renderListCard(list) {
  const name = escHtml(list.name || "Untitled");
  const desc = escHtml(list.description || "");
  const count = list.item_count ?? 0;
  const vis = list.is_public ? "Public" : "Private";
  return `<div class="list-card" onclick="openListDetail(${list.list_id},'${name.replace(/'/g, "\\'")}')">
    <div class="list-card-name">${name}</div>
    ${desc ? `<div class="list-card-desc">${desc}</div>` : ""}
    <div class="list-card-meta">${count} item${count !== 1 ? "s" : ""} · ${vis}
      <a href="https://www.discogs.com/lists/${list.list_id}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:#555;margin-left:0.4rem;font-size:0.7rem" title="View on Discogs">↗</a>
    </div>
  </div>`;
}

// ── List detail view — show items inside a specific list ──────────────
async function openListDetail(listId, listName) {
  const grid = document.getElementById("results");
  grid.innerHTML = renderSkeletonGrid(16);
  setCwStatus("");
  try {
    const r = await apiFetch("/api/user/lists/" + listId + "/items");
    const data = await r.json();
    const items = (data.items ?? []).map(row => {
      const d = row.data ?? {};
      // Discogs list items have display_title, basic_information, etc.
      const basic = d.basic_information ?? {};
      return {
        id:          d.id ?? row.discogs_id,
        type:        row.entity_type ?? "release",
        title:       d.display_title || basic.title || `${row.entity_type} ${row.discogs_id}`,
        cover_image: d.image_url || basic.cover_image || basic.thumb || "",
        uri:         d.uri || `/${row.entity_type}/${row.discogs_id}`,
        label:       basic.labels?.map(l => l.name) ?? [],
        format:      basic.formats?.map(f => f.name) ?? [],
        genre:       basic.genres ?? [],
        year:        basic.year || d.year || "",
        country:     basic.country || "",
        _comment:    row.comment || d.comment || "",
      };
    });
    const q = (document.getElementById("cw-query")?.value ?? "").trim().toLowerCase();
    const filtered = q ? items.filter(it => (it.title || "").toLowerCase().includes(q)) : items;
    if (!filtered.length) {
      grid.innerHTML = items.length
        ? renderEmptyState("\uD83D\uDD0D", `No items matching "${escHtml(q)}"`, "Try a different search")
        : renderEmptyState("\uD83D\uDCCB", "Empty list", "This list has no items");
      setCwStatus(`${listName}`);
      return;
    }
    setCwStatus(`${listName} · ${filtered.length} item${filtered.length !== 1 ? "s" : ""}`);
    grid.innerHTML = `<div style="margin-bottom:0.5rem"><button onclick="loadListsTab()" style="background:none;border:none;color:var(--accent);cursor:pointer;font-size:0.8rem;padding:0">← All lists</button></div>`
      + filtered.map((item, i) => renderCard(item, i)).join("");
  } catch (e) {
    grid.innerHTML = renderEmptyState("⚠", "Failed to load list items", e.message);
    setCwStatus(listName);
  }
}

function clearCwSearch() {
  document.getElementById("cw-query").value   = "";
  document.getElementById("cw-artist").value  = "";
  document.getElementById("cw-release").value = "";
  document.getElementById("cw-label").value   = "";
  document.getElementById("cw-year").value    = "";
  document.getElementById("cw-genre").value   = "";
  document.getElementById("cw-style").value   = "";
  document.getElementById("cw-format").value  = "";
  _cwQuery = "";
  doCwSearch(1);
}

async function loadCwFacets(type, genre) {
  try {
    let url = `/api/user/facets?type=${type}`;
    if (genre) url += `&genre=${encodeURIComponent(genre)}`;
    const r = await apiFetch(url);
    const data = await r.json();
    const genreEl = document.getElementById("cw-genre");
    const styleEl = document.getElementById("cw-style");
    if (!genre && genreEl) {
      genreEl.innerHTML = '<option value="">Any</option>' +
        (data.genres ?? []).map(g => `<option value="${g}">${g}</option>`).join("");
    }
    if (styleEl) {
      const prev = styleEl.value;
      styleEl.innerHTML = '<option value="">Any</option>' +
        (data.styles ?? []).map(s => `<option value="${s}">${s}</option>`).join("");
      if (prev && [...styleEl.options].some(o => o.value === prev)) styleEl.value = prev;
    }
  } catch {}
}

function onCwGenreChange() {
  const genre = document.getElementById("cw-genre")?.value || "";
  document.getElementById("cw-style").value = "";
  loadCwFacets(_cwTab, genre || undefined);
  doCwSearch(1);
}

async function exportCollection() {
  try {
    const r = await apiFetch("/api/user/collection/export");
    if (!r.ok) throw new Error("Export failed");
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = "seadisco-collection.csv"; a.click();
    URL.revokeObjectURL(url);
  } catch (e) { alert("Export failed: " + e.message); }
}

async function exportWantlist() {
  try {
    const r = await apiFetch("/api/user/wantlist/export");
    if (!r.ok) throw new Error("Export failed");
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = "seadisco-wantlist.csv"; a.click();
    URL.revokeObjectURL(url);
  } catch (e) { alert("Export failed: " + e.message); }
}

function clearCwFilters() {
  ["cw-query","cw-artist","cw-release","cw-label","cw-year","cw-genre","cw-style","cw-format","cw-notes"].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = "";
  });
  restoreCwSort();
  const ratingEl = document.getElementById("cw-rating");
  if (ratingEl) ratingEl.value = "";
  const allRadio = document.querySelector('input[name="cw-result-type"][value=""]');
  if (allRadio) allRadio.checked = true;
  toggleCwAdvanced(false);
  // Reset folder filter
  _cwFolderId = 0;
  document.querySelectorAll(".cw-folder-pill").forEach(p =>
    p.classList.toggle("active", parseInt(p.dataset.folder) === 0)
  );
}

function setActiveTab(tab) {
  _activeTab = tab;
}

async function loadCollectionTab(page = 1, filters) {
  _colPage = page;
  const f = filters || getCwFilters();
  const cwSort = document.getElementById("cw-sort")?.value || "";

  setActiveTab("collection");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = renderSkeletonGrid(16);
  document.getElementById("pagination").style.display = "none";
  setCwStatus("");
  try {
    let url = `/api/user/collection?page=${page}&per_page=96`;
    if (f.q)       url += `&q=${encodeURIComponent(f.q)}`;
    if (f.artist)  url += `&artist=${encodeURIComponent(f.artist)}`;
    if (f.release) url += `&release=${encodeURIComponent(f.release)}`;
    if (f.label)   url += `&label=${encodeURIComponent(f.label)}`;
    if (f.year)    url += `&year=${encodeURIComponent(f.year)}`;
    if (f.genre)   url += `&genre=${encodeURIComponent(f.genre)}`;
    if (f.style)   url += `&style=${encodeURIComponent(f.style)}`;
    if (f.format)  url += `&format=${encodeURIComponent(f.format)}`;
    if (f.type)    url += `&type=${encodeURIComponent(f.type)}`;
    if (_cwFolderId > 0) url += `&folderId=${_cwFolderId}`;
    const cwRating = document.getElementById("cw-rating")?.value || "";
    if (cwRating) url += `&rating=${encodeURIComponent(cwRating)}`;
    const cwNotes = (document.getElementById("cw-notes")?.value ?? "").trim();
    if (cwNotes) url += `&notes=${encodeURIComponent(cwNotes)}`;
    if (cwSort) url += `&sort=${encodeURIComponent(cwSort)}`;
    const r = await apiFetch(url);
    const data = await r.json();
    const items = data.items ?? [];
    const hasFilter = Object.keys(f).length > 0 || cwRating || cwNotes;
    const filterDesc = Object.values(f).join(" + ");
    if (!items.length) {
      setCwStatus("");
      document.getElementById("results").innerHTML = hasFilter
        ? renderEmptyState("🔍", `No collection items matching "${filterDesc}"`, "Try adjusting your filters")
        : renderEmptyState("📀", "No collection items synced", "Connect your Discogs token in Account to sync your collection");
      return;
    }
    const prefix = hasFilter ? `${data.total} results for "${filterDesc}"` : `${data.total} items in collection`;
    setCwStatus(`${prefix} — page ${page} of ${data.pages}`);
    document.getElementById("results").innerHTML = items.map(renderCardFromBasicInfo).join("");
    totalPages = data.pages;
    currentPage = page;
    renderCollectionPagination("collection");
  } catch (e) {
    setCwStatus("Failed to load collection: " + e.message);
    showToast("Failed to load collection — please try again", "error");
  }
}

async function loadWantlistTab(page = 1, filters) {
  _wlPage = page;
  const f = filters || getCwFilters();
  setActiveTab("wantlist");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = renderSkeletonGrid(16);
  document.getElementById("pagination").style.display = "none";
  setCwStatus("");
  try {
    let url = `/api/user/wantlist?page=${page}&per_page=96`;
    if (f.q)       url += `&q=${encodeURIComponent(f.q)}`;
    if (f.artist)  url += `&artist=${encodeURIComponent(f.artist)}`;
    if (f.release) url += `&release=${encodeURIComponent(f.release)}`;
    if (f.label)   url += `&label=${encodeURIComponent(f.label)}`;
    if (f.year)    url += `&year=${encodeURIComponent(f.year)}`;
    if (f.genre)   url += `&genre=${encodeURIComponent(f.genre)}`;
    if (f.style)   url += `&style=${encodeURIComponent(f.style)}`;
    if (f.format)  url += `&format=${encodeURIComponent(f.format)}`;
    if (f.type)    url += `&type=${encodeURIComponent(f.type)}`;
    const cwRating = document.getElementById("cw-rating")?.value || "";
    if (cwRating) url += `&rating=${encodeURIComponent(cwRating)}`;
    const cwNotes = (document.getElementById("cw-notes")?.value ?? "").trim();
    if (cwNotes) url += `&notes=${encodeURIComponent(cwNotes)}`;
    const cwSort = document.getElementById("cw-sort")?.value || "";
    if (cwSort) url += `&sort=${encodeURIComponent(cwSort)}`;
    const r = await apiFetch(url);
    const data = await r.json();
    const items = data.items ?? [];
    const hasFilter = Object.keys(f).length > 0 || cwRating || cwNotes;
    const filterDesc = Object.values(f).join(" + ");
    if (!items.length) {
      setCwStatus("");
      document.getElementById("results").innerHTML = hasFilter
        ? renderEmptyState("🔍", `No wantlist items matching "${filterDesc}"`, "Try adjusting your filters")
        : renderEmptyState("💿", "No wantlist items synced", "Connect your Discogs token in Account to sync your wantlist");
      return;
    }
    const prefix = hasFilter ? `${data.total} results for "${filterDesc}"` : `${data.total} items in wantlist`;
    setCwStatus(`${prefix} — page ${page} of ${data.pages}`);
    document.getElementById("results").innerHTML = items.map(renderCardFromBasicInfo).join("");
    totalPages = data.pages;
    currentPage = page;
    renderCollectionPagination("wantlist");
  } catch (e) {
    setCwStatus("Failed to load wantlist: " + e.message);
    showToast("Failed to load wantlist — please try again", "error");
  }
}

// ── Community Wanted ──
let _wantedItems = null;

async function loadWantedTab() {
  setActiveTab("wanted");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  if (_wantedItems) { renderWantedItems(_wantedItems); return; }
  setStatus("Loading wanted items…");
  try {
    const r = await apiFetch("/api/wanted");
    const data = await r.json();
    _wantedItems = data.items ?? [];
    renderWantedItems(_wantedItems);
  } catch (e) {
    setStatus("Failed to load wanted items: " + e.message, true);
  }
}

function filterWantedItems() {
  if (!_wantedItems) return;
  const q = (document.getElementById("wanted-q")?.value ?? "").trim().toLowerCase();
  if (!q) { renderWantedItems(_wantedItems); return; }
  const filtered = _wantedItems.filter(item => {
    const artist = (item.artists ?? []).map(a => a.name).join(" ").toLowerCase();
    const title  = (item.title  ?? "").toLowerCase();
    const label  = (item.labels ?? []).map(l => l.name).join(" ").toLowerCase();
    const genre  = (item.genres ?? []).join(" ").toLowerCase();
    const style  = (item.styles ?? []).join(" ").toLowerCase();
    const year   = String(item.year ?? "");
    return `${artist} ${title} ${label} ${genre} ${style} ${year}`.includes(q);
  });
  renderWantedItems(filtered);
}

function renderWantedItems(items) {
  if (!items.length) {
    setStatus("No wanted items found.");
    document.getElementById("results").innerHTML = "";
    return;
  }
  const q = (document.getElementById("wanted-q")?.value ?? "").trim();
  setStatus(q ? `${items.length} wanted items matching "${q}"` : `${items.length} random community wantlist items`);
  document.getElementById("results").innerHTML = items.map((item, i) => renderCardFromBasicInfo(item, i)).join("");
}

function renderCollectionPagination(tab) {
  const pag = document.getElementById("pagination");
  if (totalPages <= 1) { pag.style.display = "none"; return; }
  const goTo = (p) => { window.scrollTo({top:0,behavior:'smooth'}); tab === "collection" ? loadCollectionTab(p) : loadWantlistTab(p); };

  // Build page number list: 1 ... [cur-2 cur-1 cur cur+1 cur+2] ... last
  const pages = new Set();
  pages.add(1);
  pages.add(totalPages);
  for (let i = currentPage - 2; i <= currentPage + 2; i++) {
    if (i >= 1 && i <= totalPages) pages.add(i);
  }
  const sorted = [...pages].sort((a, b) => a - b);

  let html = `<button class="pag-arrow" ${currentPage <= 1 ? "disabled" : ""} onclick="return false">← Prev</button>`;
  let last = 0;
  for (const p of sorted) {
    if (last && p - last > 1) html += `<span class="pag-ellipsis">…</span>`;
    html += `<button class="pag-num${p === currentPage ? " pag-active" : ""}" data-page="${p}">${p}</button>`;
    last = p;
  }
  html += `<button class="pag-arrow" ${currentPage >= totalPages ? "disabled" : ""} onclick="return false">Next →</button>`;

  pag.innerHTML = html;
  pag.style.display = "flex";

  // Attach click handlers
  pag.querySelector(".pag-arrow:first-child").onclick = currentPage > 1 ? () => goTo(currentPage - 1) : null;
  pag.querySelector(".pag-arrow:last-child").onclick = currentPage < totalPages ? () => goTo(currentPage + 1) : null;
  pag.querySelectorAll(".pag-num").forEach(btn => {
    btn.onclick = () => goTo(parseInt(btn.dataset.page));
  });
}

async function showSyncStatus(type) {
  const el = document.getElementById("sync-status");
  if (!el) return;
  try {
    const r = await apiFetch("/api/user/collection?page=1&per_page=1");
  } catch {}
  el.innerHTML = `<a href="#" onclick="triggerSync('${type}');return false;" style="color:var(--accent);text-decoration:none">Sync now</a>`;
}

let _mainSyncPoll = null;

async function triggerSync(type = "both") {
  const el = document.getElementById("sync-status");
  if (el) el.textContent = "Syncing in background…";
  try {
    const r = await apiFetch("/api/user/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type }),
    });
    const data = await r.json();
    if (data.skipped) {
      if (el) el.innerHTML = `Recently synced &nbsp;·&nbsp; <a href="#" onclick="triggerSync('${type}');return false;" style="color:var(--accent);text-decoration:none">Sync now</a>`;
      return;
    }
    if (_mainSyncPoll) clearInterval(_mainSyncPoll);
    _mainSyncPoll = setInterval(async () => {
      try {
        const sr = await apiFetch("/api/user/sync-status");
        const sd = await sr.json();
        if (sd.syncStatus === "syncing") {
          if (sd.syncTotal) {
            const pct = Math.round((sd.syncProgress / sd.syncTotal) * 100);
            if (el) el.textContent = `Syncing… ${sd.syncProgress.toLocaleString()} / ${sd.syncTotal.toLocaleString()} (${pct}%)`;
          } else {
            if (el) el.textContent = sd.syncProgress > 0 ? `Syncing… ${sd.syncProgress.toLocaleString()} new items` : `Syncing…`;
          }
        } else {
          clearInterval(_mainSyncPoll); _mainSyncPoll = null;
          const completeMsg = sd.syncTotal
            ? `Synced ${sd.syncProgress.toLocaleString()} items`
            : sd.syncProgress > 0
              ? `${sd.syncProgress.toLocaleString()} new items added`
              : `Up to date`;
          if (el) el.innerHTML = `${completeMsg} &nbsp;·&nbsp; <a href="#" onclick="triggerSync('${type}');return false;" style="color:var(--accent);text-decoration:none">Sync now</a>`;
          await loadDiscogsIds();
          if (_activeTab === "collection") loadCollectionTab(1);
          else if (_activeTab === "wantlist") loadWantlistTab(1);
        }
      } catch { clearInterval(_mainSyncPoll); _mainSyncPoll = null; }
    }, 4000);
  } catch (e) {
    if (el) el.textContent = "Sync failed: " + e.message;
  }
}

async function loadDiscogsIds() {
  try {
    const r = await apiFetch("/api/user/discogs-ids");
    if (r.ok) {
      const data = await r.json();
      window._collectionIds  = new Set(data.collectionIds ?? []);
      window._wantlistIds    = new Set(data.wantlistIds   ?? []);
      window._favoriteKeys   = new Set((data.favoriteIds ?? []).map(f => `${f.entity_type}:${f.discogs_id}`));
      window._inventoryIds   = new Set(data.inventoryIds ?? []);
      window._listMembership = data.listMembership ?? {};  // { discogsId: [{listId, listName}] }
      const cb = document.getElementById("hide-owned");
      const lbl = document.getElementById("hide-owned-label");
      if (cb && cb.disabled) {
        cb.disabled = false; cb.style.opacity = "1"; cb.style.cursor = "pointer";
        cb.addEventListener("change", () => { if (window._lastResults) renderResults(window._lastResults); });
      }
      if (lbl) {
        lbl.style.color = "#aaa"; lbl.style.cursor = "pointer";
        lbl.title = window._collectionIds.size > 0 ? "Hide releases already in your collection" : "Sync your collection on the Account page to use this filter";
      }
    }
  } catch { /* ignore */ }
  // Always update heading + load favorites grid (even if discogs-ids failed)
  if (typeof updateFavoritesHeading === "function") updateFavoritesHeading();
  if (window._favoriteKeys?.size > 0 && typeof loadFavoritesGrid === "function") {
    loadFavoritesGrid();
  }
}

