// ── Cached Blues view (admin-only Discovery surface) ────────────────
// Lists every release_cache row tagged Blues, joined to a per-artist
// release-count aggregate so the curator can sort "obscure first" to
// find artists with the LEAST cached releases (the original ask).
//
// Backend: GET /api/admin/cached-blues — see src/db.ts:listCachedBluesReleases.
// SPA wiring: lazy-loaded by switchView when v=cached-blues.

(function () {
  const PAGE_SIZE = 60;
  let _cbPage = 0;
  let _cbTotal = 0;
  let _cbRows = [];
  let _cbFilterTimer = null;

  function _esc(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  // Library badge strip — same shape as the search/records cards.
  // Pulls from the site-wide window._collectionIds / _wantlistIds /
  // _favoriteKeys / _inventoryIds / _listMembership snapshots populated
  // by shared.js. Each badge: collection, wantlist, favorite,
  // (inventory + lists when the user has any). The 🎸 Blues Archive
  // badge is stamped on by _baStampCards after render — not here.
  function _cbBadgesForRelease(releaseId, type) {
    const navIcon = (typeof window._sdNavIconSvg === "function") ? window._sdNavIconSvg : (() => "");
    let badges = "";
    if (type === "release") {
      const inCol  = window._collectionIds?.has(releaseId);
      const inWant = window._wantlistIds?.has(releaseId);
      badges += `<span class="card-badge badge-collection${inCol ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleCollectionFromCard(this,${releaseId})" title="${inCol ? "Remove from collection" : "Add to collection"}">${navIcon("collection")}</span>`;
      badges += `<span class="card-badge badge-wantlist${inWant ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleWantlistFromCard(this,${releaseId})" title="${inWant ? "Remove from wantlist" : "Add to wantlist"}">${navIcon("wantlist")}</span>`;
    }
    const favKey = `${type}:${releaseId}`;
    const isFav = window._favoriteKeys?.has(favKey);
    badges += `<span class="card-badge badge-favorite${isFav ? " is-favorite" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleFavoriteFromCard(this,${releaseId},'${type}')" title="${isFav ? "Remove from favorites" : "Add to favorites"}">${navIcon("favorites")}</span>`;
    if (type === "release") {
      const userHasInventory = (window._inventoryIds?.size ?? 0) > 0;
      if (userHasInventory) {
        const inInv = window._inventoryIds?.has(releaseId);
        badges += `<span class="card-badge badge-inventory${inInv ? " is-active" : ""}" title="${inInv ? "In your inventory" : "Not in your inventory"}">${navIcon("inventory")}</span>`;
      }
      const userHasLists = window._listMembership && Object.keys(window._listMembership).length > 0;
      if (userHasLists) {
        const lists = window._listMembership?.[releaseId];
        const inList = !!(lists && lists.length);
        const names = inList ? lists.map(l => l.listName).join(", ") : "";
        badges += `<span class="card-badge badge-list${inList ? " is-active" : ""}" title="${inList ? `In list: ${_esc(names)}` : "Not in any of your lists"}">${navIcon("lists")}</span>`;
      }
    }
    return badges;
  }

  function _readFilters() {
    return {
      q:         document.getElementById("cb-q")?.value || "",
      year_from: document.getElementById("cb-year-from")?.value || "",
      year_to:   document.getElementById("cb-year-to")?.value || "",
      country:   document.getElementById("cb-country")?.value || "",
      format:    document.getElementById("cb-format")?.value || "",
      label:     document.getElementById("cb-label")?.value || "",
      sort:      document.getElementById("cb-sort")?.value || "cached_at",
      order:     document.getElementById("cb-order")?.value || "desc",
    };
  }

  async function _cbLoad() {
    const grid = document.getElementById("cb-content");
    const statusEl = document.getElementById("cb-status");
    if (grid) grid.textContent = "Loading…";
    if (statusEl) statusEl.textContent = "";
    try {
      const f = _readFilters();
      const params = new URLSearchParams();
      for (const [k, v] of Object.entries(f)) {
        if (String(v).trim() !== "") params.set(k, String(v));
      }
      params.set("limit",  String(PAGE_SIZE));
      params.set("offset", String(_cbPage * PAGE_SIZE));
      const r = await apiFetch(`/api/admin/cached-blues?${params}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const { rows = [], total = 0 } = await r.json();
      _cbRows = rows;
      _cbTotal = total;
      _cbRender();
    } catch (e) {
      if (grid) grid.innerHTML = `<div style="color:#e88">Load failed: ${_esc(e?.message || e)}</div>`;
    }
  }
  window._cbLoad = _cbLoad;

  function _cbRender() {
    const grid = document.getElementById("cb-content");
    const statusEl = document.getElementById("cb-status");
    const pagerEl = document.getElementById("cb-pager");
    if (!grid) return;
    if (statusEl) {
      const start = _cbRows.length ? _cbPage * PAGE_SIZE + 1 : 0;
      const end   = _cbPage * PAGE_SIZE + _cbRows.length;
      statusEl.textContent = _cbTotal
        ? `${start.toLocaleString()}–${end.toLocaleString()} of ${_cbTotal.toLocaleString()}`
        : "0 of 0";
    }
    if (!_cbRows.length) {
      grid.innerHTML = `<div style="color:var(--muted);font-style:italic;padding:1rem 0">No cached releases match your filters.</div>`;
      if (pagerEl) pagerEl.innerHTML = "";
      return;
    }
    // Use the site-wide .card-grid + .card markup so the existing
    // body.card-mode-wide CSS reshapes the layout for free.
    grid.innerHTML = `
      <div class="card-grid">
        ${_cbRows.map(_cbCardHtml).join("")}
      </div>
    `;
    _cbRenderPager();
    // After paint: stamp 🎸 Blues Archive badges on cards whose
    // artist matches an archive row. _baStampCards reads
    // data-card-type and the title attribute (parses "Artist - Title"
    // for the artist name), so the items array we pass mirrors that
    // shape. Fire-and-forget — silent on failure.
    const gridEl = grid.querySelector(".card-grid");
    if (gridEl && typeof window._baStampCards === "function") {
      const stampItems = _cbRows.map(r => ({
        id: Number(r.release_id),
        type: String(r.release_type || "release"),
        title: `${String(r.artist || "")} - ${String(r.title || "")}`,
      }));
      window._baStampCards(stampItems, gridEl).catch(() => {});
    }
  }

  function _cbCardHtml(row) {
    const id      = Number(row.release_id);
    const type    = String(row.release_type || "release");
    const title   = String(row.title || "Untitled");
    const artist  = String(row.artist || "");
    const year    = row.year ? String(row.year) : "";
    const label   = row.label ? String(row.label) : "";
    const format  = row.format ? String(row.format) : "";
    const country = row.country ? String(row.country) : "";
    const count   = Number(row.artist_release_count) || 0;
    const thumb   = row.cover_thumb ? String(row.cover_thumb) : "";
    const safeUrl = `https://www.discogs.com/${type === "master" ? "master" : "release"}/${id}`;
    const thumbHtml = thumb
      ? `<img src="${_esc(thumb)}" alt="" loading="lazy" />`
      : `<div class="thumb-placeholder">♪</div>`;
    // Meta line — year + format + country, joined with the same
    // " · " separator the standard cards use.
    const metaParts = [year, format, country].filter(Boolean);
    const metaHtml = metaParts.length
      ? `<div class="card-meta">${metaParts.map(_esc).join(" · ")}</div>`
      : "";
    const labelHtml = label
      ? `<div class="card-sub" title="${_esc(label)}">${_esc(label)}</div>`
      : "";
    // Artist-release-count chip — the headline feature of this view.
    // Click on the count opens a filter for this artist (so the user
    // can see all of their cached releases at once).
    const countChip = count > 0
      ? `<div class="card-sub" style="display:flex;align-items:center;gap:0.35rem">
           <span style="padding:0.05rem 0.4rem;border:1px solid var(--border);border-radius:999px;font-size:0.7rem;color:var(--muted);font-variant-numeric:tabular-nums"
                 title="${count} cached release${count === 1 ? "" : "s"} by ${_esc(artist)}">${count} by artist</span>
         </div>`
      : "";
    // title attribute carries "Artist - Title" so _baStampCards can
    // extract the artist name post-render (same convention search
    // cards use). data-card-type lets the same helper know which
    // strategy to apply (release/master/artist).
    const badges = _cbBadgesForRelease(id, type);
    const titleAttr = artist ? `${artist} - ${title}` : title;
    // data-card-id + data-card-type are what shared.js's
    // MutationObserver-driven enrichment pass (_sdEnrichWideCards)
    // selects on; without data-card-id the cards stay sparse and
    // tracks/images strip never get injected. Same data-release-id
    // kept too for any release-specific helpers that key off it.
    return `
      <a class="card card-type-${_esc(type)}" href="#"
         data-card-id="${id}"
         data-card-type="${_esc(type)}"
         data-release-id="${id}"
         onclick="event.preventDefault();event.stopPropagation();_cbOpenRelease(${id}, '${_esc(type)}', '${_esc(safeUrl)}')"
         title="${_esc(titleAttr)}">
        <div class="card-thumb-wrap">
          ${thumbHtml}
          <div class="card-thumb-badges">${badges}</div>
        </div>
        <div class="card-body">
          ${artist ? `<div class="card-artist">${_esc(artist)}</div>` : ""}
          <div class="card-title">${_esc(title)}</div>
          <div class="card-bottom">
            ${labelHtml}
            ${metaHtml}
            ${countChip}
          </div>
        </div>
      </a>
    `;
  }

  function _cbRenderPager() {
    const pagerEl = document.getElementById("cb-pager");
    if (!pagerEl) return;
    const totalPages = Math.max(1, Math.ceil(_cbTotal / PAGE_SIZE));
    const cur = _cbPage + 1;
    pagerEl.innerHTML = `
      <button class="archive-btn" ${cur <= 1 ? "disabled" : ""} onclick="_cbGoToPage(0)">« First</button>
      <button class="archive-btn" ${cur <= 1 ? "disabled" : ""} onclick="_cbGoToPage(${_cbPage - 1})">‹ Prev</button>
      <span style="align-self:center;color:var(--muted)">Page ${cur} of ${totalPages.toLocaleString()}</span>
      <button class="archive-btn" ${cur >= totalPages ? "disabled" : ""} onclick="_cbGoToPage(${_cbPage + 1})">Next ›</button>
      <button class="archive-btn" ${cur >= totalPages ? "disabled" : ""} onclick="_cbGoToPage(${totalPages - 1})">Last »</button>
    `;
  }

  function _cbGoToPage(p) {
    _cbPage = Math.max(0, p);
    _cbLoad();
    window.scrollTo({ top: 0, behavior: "smooth" });
  }
  window._cbGoToPage = _cbGoToPage;

  // Debounced filter change — typing in inputs shouldn't refetch on
  // every keystroke. Sort/order select changes fire through this too.
  function _cbFilterChanged() {
    if (_cbFilterTimer) clearTimeout(_cbFilterTimer);
    _cbFilterTimer = setTimeout(() => { _cbPage = 0; _cbLoad(); }, 280);
  }
  window._cbFilterChanged = _cbFilterChanged;

  function _cbClearFilters() {
    ["cb-q","cb-year-from","cb-year-to","cb-country","cb-format","cb-label"].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.value = "";
    });
    const sort = document.getElementById("cb-sort"); if (sort) sort.value = "cached_at";
    const order = document.getElementById("cb-order"); if (order) order.value = "desc";
    _cbPage = 0;
    _cbLoad();
  }
  window._cbClearFilters = _cbClearFilters;

  // Cards open the release modal with skipStats:true so the
  // marketplace-stats fetch (which goes to Discogs and burns rate
  // limit) is bypassed — this view is local-db only. Prefer
  // openModal directly when available so we can pass the flag;
  // fall back to _baOpenRelease (no flag) or the discogs.com link
  // as last-resort.
  function _cbOpenRelease(id, type, url) {
    if (typeof window.openModal === "function") {
      try { window.openModal(null, id, type, url, { skipStats: true }); return; } catch {}
    }
    if (typeof window._baOpenRelease === "function") {
      try { window._baOpenRelease(id, type, url); return; } catch {}
    }
    window.open(url, "_blank", "noopener");
  }
  window._cbOpenRelease = _cbOpenRelease;

  // SPA entry point — idempotent. switchView calls this every time
  // v=cached-blues is shown. Warms blues-archive.js so release modal
  // clicks have a handler ready.
  function initCachedBluesView() {
    if (typeof window._baOpenRelease !== "function"
        && typeof window._sdLoadModule === "function") {
      try { window._sdLoadModule("/blues-archive.js"); } catch {}
    }
    _cbLoad();
  }
  window.initCachedBluesView = initCachedBluesView;
})();
