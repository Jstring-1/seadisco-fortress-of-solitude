// ── Collection / Wantlist / Wants tab state ──────────────────────────────
let _activeTab = "search";
let _colPage = 1, _wlPage = 1;

function renderCardFromBasicInfo(basicInfo) {
  const artistName = (basicInfo.artists ?? []).map(a => a.name).join(", ");
  const labelStr   = (basicInfo.labels  ?? []).map(l => l.name).join(", ");
  const formatStr  = (basicInfo.formats ?? []).map(f => f.name + (f.descriptions?.length ? ` (${f.descriptions.join(", ")})` : "")).join(" · ");
  const genreStr   = (basicInfo.genres  ?? [])[0] ?? "";

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
    uri:          basicInfo.id ? `/release/${basicInfo.id}` : "",
  };
  return renderCard(syntheticItem);
}

function addNavTab(view) {
  const btn = document.querySelector(`#main-nav-tabs [data-view="${view}"]`);
  if (btn) { btn.classList.remove("nav-disabled"); btn.removeAttribute("title"); }
}

function toggleMobileNav() {
  document.getElementById("main-nav-tabs")?.classList.toggle("mobile-open");
}

// Close hamburger menu when clicking outside
document.addEventListener("click", e => {
  if (!e.target.closest("#main-nav-tabs") && !e.target.closest("#nav-hamburger")) {
    document.getElementById("main-nav-tabs")?.classList.remove("mobile-open");
  }
});

function switchView(view, skipPushState = false) {
  document.getElementById("main-nav-tabs")?.classList.remove("mobile-open");
  const tabBtn = document.querySelector(`#main-nav-tabs [data-view="${view}"]`);
  if (tabBtn?.classList.contains("nav-disabled")) return;

  document.querySelectorAll(".main-nav-tab").forEach(btn =>
    btn.classList.toggle("active", btn.dataset.view === view)
  );
  const searchView = document.getElementById("search-view");
  const dropsView  = document.getElementById("drops-view");
  const liveView   = document.getElementById("live-view");
  const infoView   = document.getElementById("info-view");
  if (!skipPushState) {
    if (view === "drops" || view === "live" || view === "collection" || view === "wantlist" || view === "info" || view === "wanted") {
      history.pushState({ view }, "", "?view=" + view);
    } else {
      history.pushState({}, "", location.pathname);
    }
  }
  if (typeof gtag === "function") {
    const titles = { drops: "Drops", live: "Live", info: "Info", collection: "Collection", wantlist: "Wantlist", wanted: "Wants", search: "Search" };
    gtag("event", "page_view", {
      page_location: window.location.href,
      page_path:     window.location.pathname + window.location.search,
      page_title:    "SeaDisco – " + (titles[view] ?? view)
    });
  }
  if (searchView) searchView.style.display = "none";
  if (dropsView)  dropsView.style.display  = "none";
  if (liveView)   liveView.style.display   = "none";
  if (infoView)   infoView.style.display   = "none";

  const mainForm    = document.getElementById("main-search-form");
  const cwWrap      = document.getElementById("cw-search-wrap");
  const cwInput     = document.getElementById("cw-query");
  const wantedWrap  = document.getElementById("wanted-search-wrap");

  if (view === "drops") {
    if (dropsView) dropsView.style.display = "block";
    if (mainForm) mainForm.style.display = "";
    if (cwWrap) cwWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
  } else if (view === "live") {
    if (liveView) liveView.style.display = "block";
    if (mainForm) mainForm.style.display = "none";
    if (cwWrap) cwWrap.style.display = "none";
    if (typeof loadLiveRecentFeed === "function") loadLiveRecentFeed();
    if (wantedWrap) wantedWrap.style.display = "none";
  } else if (view === "info") {
    if (infoView) infoView.style.display = "block";
    if (mainForm) mainForm.style.display = "";
    if (cwWrap) cwWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
  } else if (view === "wanted") {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "none";
    if (cwWrap) cwWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "";
    document.getElementById("artist-alts").innerHTML = "";
    const feed = document.getElementById("recent-feed"); if (feed) feed.style.display = "none";
    loadWantedTab();
  } else if (view === "collection") {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "none";
    if (cwWrap) cwWrap.style.display = "";
    if (wantedWrap) wantedWrap.style.display = "none";
    if (cwInput) { cwInput.placeholder = "Search your collection…"; cwInput.value = ""; }
    clearCwFilters();
    _cwTab = "collection"; _cwQuery = "";
    document.getElementById("artist-alts").innerHTML = "";
    const feed = document.getElementById("recent-feed"); if (feed) feed.style.display = "none";
    loadCwFacets("collection");
    loadCollectionFolders();
    loadCollectionTab(1);
  } else if (view === "wantlist") {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "none";
    if (cwWrap) cwWrap.style.display = "";
    if (wantedWrap) wantedWrap.style.display = "none";
    if (cwInput) { cwInput.placeholder = "Search your wantlist…"; cwInput.value = ""; }
    clearCwFilters();
    _cwTab = "wantlist"; _cwQuery = "";
    document.getElementById("artist-alts").innerHTML = "";
    const feed = document.getElementById("recent-feed"); if (feed) feed.style.display = "none";
    const fc = document.getElementById("cw-folder-cloud"); if (fc) fc.style.display = "none";
    loadCwFacets("wantlist");
    loadWantlistTab(1);
  } else {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "";
    if (cwWrap) cwWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
    document.getElementById("results").innerHTML = "";
    document.getElementById("pagination").style.display = "none";
    setStatus("");
    document.getElementById("blurb").style.display = "none";
    const feed = document.getElementById("recent-feed"); if (feed) feed.style.display = "";
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
    let html = `<span class="cw-folder-pill active" data-folder="0" onclick="filterByFolder(0)">All</span>`;
    html += folders.map(f =>
      `<span class="cw-folder-pill" data-folder="${f.folderId}" onclick="filterByFolder(${f.folderId})" title="${escHtml(f.name)} (${f.count})">${escHtml(f.name)}</span>`
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
  if (q)       f.q       = q;
  if (artist)  f.artist  = artist;
  if (release) f.release = release;
  if (label)   f.label   = label;
  if (year)    f.year    = year;
  if (genre)   f.genre   = genre;
  if (style)   f.style   = style;
  if (format)  f.format  = format;
  return f;
}

function doCwSearch(page = 1) {
  const filters = getCwFilters();
  _cwQuery = filters.q || "";
  if (_cwTab === "collection") {
    loadCollectionTab(page, filters);
  } else {
    loadWantlistTab(page, filters);
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

function clearCwFilters() {
  ["cw-query","cw-artist","cw-release","cw-label","cw-year","cw-genre","cw-style","cw-format"].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = "";
  });
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
  setActiveTab("collection");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  setStatus("Loading collection…");
  try {
    let url = `/api/user/collection?page=${page}&per_page=24`;
    if (f.q)       url += `&q=${encodeURIComponent(f.q)}`;
    if (f.artist)  url += `&artist=${encodeURIComponent(f.artist)}`;
    if (f.release) url += `&release=${encodeURIComponent(f.release)}`;
    if (f.label)   url += `&label=${encodeURIComponent(f.label)}`;
    if (f.year)    url += `&year=${encodeURIComponent(f.year)}`;
    if (f.genre)   url += `&genre=${encodeURIComponent(f.genre)}`;
    if (f.style)   url += `&style=${encodeURIComponent(f.style)}`;
    if (f.format)  url += `&format=${encodeURIComponent(f.format)}`;
    if (_cwFolderId > 0) url += `&folderId=${_cwFolderId}`;
    const r = await apiFetch(url);
    const data = await r.json();
    const items = data.items ?? [];
    const hasFilter = Object.keys(f).length > 0;
    const filterDesc = Object.values(f).join(" + ");
    if (!items.length) {
      setStatus(hasFilter ? `No collection items matching "${filterDesc}".` : "No collection items synced yet. Click 'Sync now' to fetch from Discogs.");
      return;
    }
    const prefix = hasFilter ? `${data.total} results for "${filterDesc}"` : `${data.total} items in collection`;
    setStatus(`${prefix} — page ${page} of ${data.pages}`);
    document.getElementById("results").innerHTML = items.map(renderCardFromBasicInfo).join("");
    totalPages = data.pages;
    currentPage = page;
    renderCollectionPagination("collection");
  } catch (e) {
    setStatus("Failed to load collection: " + e.message, true);
  }
}

async function loadWantlistTab(page = 1, filters) {
  _wlPage = page;
  const f = filters || getCwFilters();
  setActiveTab("wantlist");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  setStatus("Loading wantlist…");
  try {
    let url = `/api/user/wantlist?page=${page}&per_page=24`;
    if (f.q)       url += `&q=${encodeURIComponent(f.q)}`;
    if (f.artist)  url += `&artist=${encodeURIComponent(f.artist)}`;
    if (f.release) url += `&release=${encodeURIComponent(f.release)}`;
    if (f.label)   url += `&label=${encodeURIComponent(f.label)}`;
    if (f.year)    url += `&year=${encodeURIComponent(f.year)}`;
    if (f.genre)   url += `&genre=${encodeURIComponent(f.genre)}`;
    if (f.style)   url += `&style=${encodeURIComponent(f.style)}`;
    if (f.format)  url += `&format=${encodeURIComponent(f.format)}`;
    const r = await apiFetch(url);
    const data = await r.json();
    const items = data.items ?? [];
    const hasFilter = Object.keys(f).length > 0;
    const filterDesc = Object.values(f).join(" + ");
    if (!items.length) {
      setStatus(hasFilter ? `No wantlist items matching "${filterDesc}".` : "No wantlist items synced yet. Click 'Sync now' to fetch from Discogs.");
      return;
    }
    const prefix = hasFilter ? `${data.total} results for "${filterDesc}"` : `${data.total} items in wantlist`;
    setStatus(`${prefix} — page ${page} of ${data.pages}`);
    document.getElementById("results").innerHTML = items.map(renderCardFromBasicInfo).join("");
    totalPages = data.pages;
    currentPage = page;
    renderCollectionPagination("wantlist");
  } catch (e) {
    setStatus("Failed to load wantlist: " + e.message, true);
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
  document.getElementById("results").innerHTML = items.map(item => renderCardFromBasicInfo(item)).join("");
}

function renderCollectionPagination(tab) {
  if (totalPages <= 1) return;
  const pag = document.getElementById("pagination");
  pag.style.display = "flex";
  document.getElementById("page-info").textContent = `${currentPage} / ${totalPages}`;
  document.getElementById("prev-btn").disabled = currentPage <= 1;
  document.getElementById("next-btn").disabled = currentPage >= totalPages;
  document.getElementById("prev-btn").onclick = currentPage > 1
    ? () => { window.scrollTo({top:0,behavior:'smooth'}); tab === "collection" ? loadCollectionTab(currentPage - 1) : loadWantlistTab(currentPage - 1); }
    : null;
  document.getElementById("next-btn").onclick = currentPage < totalPages
    ? () => { window.scrollTo({top:0,behavior:'smooth'}); tab === "collection" ? loadCollectionTab(currentPage + 1) : loadWantlistTab(currentPage + 1); }
    : null;
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
          const pct = sd.syncTotal ? Math.round((sd.syncProgress / sd.syncTotal) * 100) : 0;
          if (el) el.textContent = `Syncing… ${sd.syncProgress.toLocaleString()} / ${sd.syncTotal.toLocaleString()} (${pct}%)`;
        } else {
          clearInterval(_mainSyncPoll); _mainSyncPoll = null;
          if (el) el.innerHTML = `Sync complete &nbsp;·&nbsp; <a href="#" onclick="triggerSync('${type}');return false;" style="color:var(--accent);text-decoration:none">Sync now</a>`;
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
    if (!r.ok) return;
    const data = await r.json();
    window._collectionIds = new Set(data.collectionIds ?? []);
    window._wantlistIds   = new Set(data.wantlistIds   ?? []);
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
  } catch { /* ignore */ }
}
