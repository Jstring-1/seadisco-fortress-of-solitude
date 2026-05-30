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
    grid.innerHTML = `
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:0.6rem">
        ${_cbRows.map(_cbCardHtml).join("")}
      </div>
    `;
    _cbRenderPager();
  }

  function _cbCardHtml(row) {
    const id     = Number(row.release_id);
    const type   = String(row.release_type || "release");
    const title  = String(row.title || "Untitled");
    const artist = String(row.artist || "");
    const year   = row.year ? String(row.year) : "";
    const label  = row.label ? String(row.label) : "";
    const format = row.format ? String(row.format) : "";
    const country = row.country ? String(row.country) : "";
    const count  = Number(row.artist_release_count) || 0;
    const thumb  = row.cover_thumb ? String(row.cover_thumb) : "";
    const safeUrl = `https://www.discogs.com/${type === "master" ? "master" : "release"}/${id}`;
    const coverHtml = thumb
      ? `<img src="${_esc(thumb)}" alt="" loading="lazy"
             style="width:80px;height:80px;object-fit:cover;border-radius:4px;flex:0 0 auto;background:var(--surface-raised)" />`
      : `<div style="width:80px;height:80px;border-radius:4px;background:var(--surface-raised);display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:2rem;flex:0 0 auto">♪</div>`;
    const countPill = count > 0
      ? `<span title="${count} cached release${count === 1 ? "" : "s"} by this artist" style="display:inline-block;padding:0.1rem 0.45rem;border:1px solid var(--border);border-radius:999px;font-size:0.72rem;color:var(--muted);font-variant-numeric:tabular-nums">${count} by artist</span>`
      : "";
    return `
      <article style="display:flex;gap:0.7rem;padding:0.6rem;border:1px solid var(--border);border-radius:6px;background:rgba(255,255,255,0.02);align-items:flex-start">
        <a href="#" onclick="event.preventDefault();event.stopPropagation();_cbOpenRelease(${id}, '${_esc(type)}', '${_esc(safeUrl)}')" title="Open ${type.toUpperCase()} popup" style="flex:0 0 auto">${coverHtml}</a>
        <div style="min-width:0;flex:1;display:flex;flex-direction:column;gap:0.2rem">
          <a href="#" onclick="event.preventDefault();event.stopPropagation();_cbOpenRelease(${id}, '${_esc(type)}', '${_esc(safeUrl)}')"
             title="Open ${type.toUpperCase()} popup"
             style="font-weight:600;color:var(--text);text-decoration:none;line-height:1.25">${_esc(title)}</a>
          <div style="font-size:0.82rem;color:var(--accent);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_esc(artist)}</div>
          <div style="font-size:0.74rem;color:var(--muted);display:flex;flex-wrap:wrap;gap:0.4rem;align-items:center">
            ${year ? `<span style="font-variant-numeric:tabular-nums">${_esc(year)}</span>` : ""}
            ${format ? `<span>${_esc(format)}</span>` : ""}
            ${country ? `<span>${_esc(country)}</span>` : ""}
          </div>
          ${label ? `<div style="font-size:0.74rem;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_esc(label)}">${_esc(label)}</div>` : ""}
          ${countPill}
        </div>
      </article>
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

  // Defer to the existing release-modal opener if blues-archive.js is
  // loaded (which it is for admin users), else fall back to opening
  // discogs.com directly.
  function _cbOpenRelease(id, type, url) {
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
