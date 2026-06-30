// ── Labels page (admin-only) ─────────────────────────────────────
// Chronological browse of cached releases grouped by record label.
// Picks a label from a server-driven dropdown (powered by the same
// /api/admin/release-cache/labels endpoint the export-form picker
// uses) and renders the matching release_cache rows sorted by year
// ASC then catalog number ASC. A sticky controls bar carries the
// label picker + year-range + type filter + pagination. The Year
// anchors strip lets the admin jump straight to a specific year
// within the current label.

(function () {
  if (window.__sdLabelsBound) return;
  window.__sdLabelsBound = true;

  const _state = {
    label:     "",
    type:      "",         // "release" | "master" | ""
    yearFrom:  "",
    yearTo:    "",
    page:      1,
    perPage:   60,
    items:     [],
    yearAnchors: [],
    total:     0,
    hasMore:   false,
    labelsAll: null,       // [{name, count}] cached from /labels endpoint
    pickerOpen: false,
  };
  window._sdLabelsState = _state;

  function _esc(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }
  function _fmt(n) { return Number(n || 0).toLocaleString(); }

  async function _fetchLabelsList() {
    if (_state.labelsAll) return _state.labelsAll;
    const r = await apiFetch("/api/admin/release-cache/labels?limit=2000");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    _state.labelsAll = Array.isArray(j.items) ? j.items : [];
    return _state.labelsAll;
  }

  async function _fetchReleases() {
    if (!_state.label) {
      _state.items = []; _state.yearAnchors = []; _state.total = 0; _state.hasMore = false;
      return;
    }
    const q = new URLSearchParams();
    q.set("label", _state.label);
    q.set("page",  String(_state.page));
    q.set("per_page", String(_state.perPage));
    if (_state.type) q.set("type", _state.type);
    if (_state.yearFrom) q.set("year_from", _state.yearFrom);
    if (_state.yearTo)   q.set("year_to",   _state.yearTo);
    const r = await apiFetch(`/api/admin/labels/releases?${q.toString()}`);
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      throw new Error(j?.error || `HTTP ${r.status}`);
    }
    const j = await r.json();
    _state.items = Array.isArray(j.items) ? j.items : [];
    _state.yearAnchors = Array.isArray(j.yearAnchors) ? j.yearAnchors : [];
    _state.total = Number(j.total) || 0;
    _state.hasMore = !!j.hasMore;
  }

  function _renderControls() {
    const el = document.getElementById("labels-controls");
    if (!el) return;
    const labelBtn = _state.label
      ? _state.label
      : `(choose a label)`;
    el.innerHTML = `
      <div style="display:flex;flex-wrap:wrap;gap:0.4rem;align-items:center;font-size:0.82rem">
        <span style="position:relative">
          <button type="button" id="labels-picker-btn" onclick="_labelsTogglePicker(event)"
            style="padding:0.35rem 0.6rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;cursor:pointer;min-width:220px;text-align:left">
            ${_esc(labelBtn)}${_state.label ? "" : ` <span style="color:var(--muted)">▼</span>`}
          </button>
          <div id="labels-picker-panel" style="display:${_state.pickerOpen ? "flex" : "none"};position:absolute;left:0;top:100%;z-index:50;background:var(--surface);border:1px solid var(--border);border-radius:5px;padding:0.5rem;margin-top:0.2rem;box-shadow:0 6px 18px rgba(0,0,0,0.4);min-width:320px;max-height:380px;overflow:hidden;flex-direction:column;gap:0.4rem">
            <input id="labels-picker-search" type="text" placeholder="Filter labels…" oninput="_labelsRenderPickerList()"
              style="padding:0.35rem;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:3px">
            <div id="labels-picker-list" style="overflow-y:auto;max-height:280px;border:1px solid var(--border);border-radius:3px;padding:0.3rem 0.4rem;font-size:0.8rem">Loading…</div>
          </div>
        </span>
        <label style="display:inline-flex;gap:0.3rem;align-items:center">Type
          <select id="labels-type" onchange="_labelsOnFiltersChange()"
            style="padding:0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
            <option value="">both</option>
            <option value="release" ${_state.type === "release" ? "selected" : ""}>release</option>
            <option value="master"  ${_state.type === "master"  ? "selected" : ""}>master</option>
          </select>
        </label>
        <label style="display:inline-flex;gap:0.3rem;align-items:center">Year
          <input id="labels-year-from" type="number" placeholder="from" value="${_esc(_state.yearFrom)}"
            onchange="_labelsOnFiltersChange()" style="width:70px;padding:0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          <input id="labels-year-to" type="number" placeholder="to" value="${_esc(_state.yearTo)}"
            onchange="_labelsOnFiltersChange()" style="width:70px;padding:0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
        </label>
        <span style="margin-left:auto;color:var(--muted);font-size:0.78rem">
          ${_state.total ? `${_fmt(_state.total)} releases` : ""}
        </span>
      </div>
    `;
    if (_state.pickerOpen) {
      _renderPickerList();
      // One-shot outside-click to close.
      setTimeout(() => {
        const off = (e) => {
          const panel = document.getElementById("labels-picker-panel");
          if (!panel) return;
          if (!panel.contains(e.target) && e.target.id !== "labels-picker-btn") {
            _state.pickerOpen = false;
            _renderControls();
            document.removeEventListener("click", off, true);
          }
        };
        document.addEventListener("click", off, true);
      }, 0);
    }
  }
  window._renderLabelsControls = _renderControls;

  async function _labelsTogglePicker(ev) {
    ev?.preventDefault?.(); ev?.stopPropagation?.();
    _state.pickerOpen = !_state.pickerOpen;
    _renderControls();
    if (_state.pickerOpen) {
      try { await _fetchLabelsList(); } catch (e) {
        const list = document.getElementById("labels-picker-list");
        if (list) list.innerHTML = `<span style="color:#e88">${_esc(String(e).slice(0, 200))}</span>`;
        return;
      }
      _renderPickerList();
    }
  }
  window._labelsTogglePicker = _labelsTogglePicker;

  function _renderPickerList() {
    const list = document.getElementById("labels-picker-list");
    if (!list || !_state.labelsAll) return;
    const q = (document.getElementById("labels-picker-search")?.value || "").toLowerCase().trim();
    const filtered = q
      ? _state.labelsAll.filter(it => String(it.name).toLowerCase().includes(q))
      : _state.labelsAll;
    const rows = filtered.slice(0, 800).map(it => {
      const selected = it.name === _state.label;
      return `<div onclick="_labelsPickLabel('${_esc(it.name)}')"
        style="display:flex;gap:0.4rem;align-items:center;padding:0.2rem 0.3rem;cursor:pointer;border-radius:3px;${selected ? "background:rgba(120,220,140,0.12)" : ""}"
        onmouseenter="this.style.background='rgba(255,255,255,0.06)'"
        onmouseleave="this.style.background='${selected ? "rgba(120,220,140,0.12)" : "transparent"}'">
        <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_esc(it.name)}</span>
        <span style="color:var(--muted);font-size:0.72rem">${_fmt(it.count)}</span>
      </div>`;
    }).join("");
    list.innerHTML = rows || `<div style="color:var(--muted)">No matches.</div>`;
  }
  window._labelsRenderPickerList = _renderPickerList;

  async function _labelsPickLabel(name) {
    _state.label = String(name || "");
    _state.page  = 1;
    _state.pickerOpen = false;
    _renderControls();
    await _loadAndRender();
  }
  window._labelsPickLabel = _labelsPickLabel;

  function _labelsOnFiltersChange() {
    _state.type     = document.getElementById("labels-type")?.value || "";
    _state.yearFrom = document.getElementById("labels-year-from")?.value || "";
    _state.yearTo   = document.getElementById("labels-year-to")?.value || "";
    _state.page = 1;
    _loadAndRender();
  }
  window._labelsOnFiltersChange = _labelsOnFiltersChange;

  function _renderYearAnchors() {
    const el = document.getElementById("labels-year-anchors");
    if (!el) return;
    if (!_state.label || !_state.yearAnchors.length) { el.innerHTML = ""; return; }
    const chips = _state.yearAnchors.map(a => {
      if (!a.year) return "";
      return `<button type="button" onclick="_labelsJumpToYear(${a.year})"
        title="${a.count} releases in ${a.year}"
        style="padding:0.15rem 0.45rem;background:rgba(255,255,255,0.04);color:var(--text);border:1px solid var(--border);border-radius:999px;font-size:0.74rem;cursor:pointer">
        ${a.year} <span style="color:var(--muted)">${a.count}</span>
      </button>`;
    }).join("");
    el.innerHTML = `<span style="color:var(--muted);align-self:center">Jump:</span> ${chips}`;
  }

  function _labelsJumpToYear(year) {
    // Set year_from = year_to = clicked year, narrowing the page to
    // releases in that year. Click an empty space (clear filters) to
    // back out — or just clear the year inputs in the controls bar.
    _state.yearFrom = String(year);
    _state.yearTo   = String(year);
    _state.page = 1;
    // Reflect in the inputs without re-rendering the whole controls bar.
    const fromEl = document.getElementById("labels-year-from");
    const toEl   = document.getElementById("labels-year-to");
    if (fromEl) fromEl.value = String(year);
    if (toEl)   toEl.value   = String(year);
    _loadAndRender();
  }
  window._labelsJumpToYear = _labelsJumpToYear;

  function _renderResults() {
    const el = document.getElementById("labels-results");
    if (!el) return;
    if (!_state.label) {
      el.innerHTML = `<div class="loc-empty">Pick a label to start.</div>`;
      return;
    }
    if (!_state.items.length) {
      el.innerHTML = `<div class="loc-empty">No releases match.</div>`;
      return;
    }
    // Group rows by year for visible chronological grouping. Within
    // each year they're already sorted by catno ASC from the server.
    const groups = new Map();
    for (const it of _state.items) {
      const y = it.year || 0;
      if (!groups.has(y)) groups.set(y, []);
      groups.get(y).push(it);
    }
    const sectionHtml = [];
    for (const [year, rows] of groups) {
      sectionHtml.push(`<div style="margin:0.8rem 0 0.3rem;font-size:0.95rem;font-weight:600;color:var(--text)">${year || "—"}</div>`);
      sectionHtml.push(`<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:0.6rem">`);
      for (const r of rows) {
        const formats = Array.isArray(r.formats) && r.formats.length
          ? r.formats.map(f => f?.name).filter(Boolean).slice(0, 2).join(", ")
          : "";
        const cover = r.cover ? `<img src="${_esc(r.cover)}" alt="" loading="lazy" style="width:100%;aspect-ratio:1/1;object-fit:cover;border-radius:4px;background:rgba(255,255,255,0.05)">`
          : `<div style="width:100%;aspect-ratio:1/1;background:rgba(255,255,255,0.04);border-radius:4px;display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:0.7rem">no image</div>`;
        sectionHtml.push(`
          <div onclick="_labelsOpenRelease(${r.id}, '${r.type}')"
            style="border:1px solid var(--border);border-radius:6px;padding:0.45rem;background:rgba(255,255,255,0.02);cursor:pointer;display:flex;flex-direction:column;gap:0.3rem"
            onmouseenter="this.style.borderColor='var(--accent)'"
            onmouseleave="this.style.borderColor='var(--border)'">
            ${cover}
            <div style="font-size:0.74rem;color:var(--muted);display:flex;justify-content:space-between;gap:0.4rem">
              <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_esc(r.catno)}">${_esc(r.catno || "—")}</span>
              <span style="color:var(--accent)">${_esc(r.type)}</span>
            </div>
            <div style="font-size:0.82rem;font-weight:600;line-height:1.2;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical">${_esc(r.title || "(untitled)")}</div>
            <div style="font-size:0.76rem;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_esc(r.artist || "")}</div>
            ${formats || r.country ? `<div style="font-size:0.7rem;color:var(--muted);display:flex;gap:0.4rem;justify-content:space-between"><span>${_esc(formats)}</span><span>${_esc(r.country)}</span></div>` : ""}
          </div>`);
      }
      sectionHtml.push(`</div>`);
    }
    el.innerHTML = sectionHtml.join("");
  }

  function _renderPagination() {
    const el = document.getElementById("labels-pagination");
    if (!el) return;
    if (!_state.label || _state.total <= _state.perPage) { el.innerHTML = ""; return; }
    const totalPages = Math.max(1, Math.ceil(_state.total / _state.perPage));
    const prevDis = _state.page <= 1 ? "disabled" : "";
    const nextDis = !_state.hasMore ? "disabled" : "";
    el.innerHTML = `
      <button class="admin-btn" ${prevDis} onclick="_labelsGoto(${_state.page - 1})">← Prev</button>
      <span style="color:var(--muted);align-self:center">Page ${_state.page} of ${totalPages}</span>
      <button class="admin-btn" ${nextDis} onclick="_labelsGoto(${_state.page + 1})">Next →</button>
    `;
  }
  function _labelsGoto(p) {
    if (p < 1) return;
    _state.page = p;
    _loadAndRender();
    window.scrollTo({ top: 0, behavior: "smooth" });
  }
  window._labelsGoto = _labelsGoto;

  function _labelsOpenRelease(id, type) {
    // Reuse the existing album modal — same path the lookup popup
    // already takes for cached releases.
    const url = `/?op=${encodeURIComponent(type === "master" ? "master" : "release")}:${id}`;
    if (typeof window.openLookupPopup === "function") {
      window.openLookupPopup({ kind: type === "master" ? "master" : "release", id });
    } else {
      window.open(url, "_self");
    }
  }
  window._labelsOpenRelease = _labelsOpenRelease;

  async function _loadAndRender() {
    const resEl = document.getElementById("labels-results");
    if (resEl && _state.label) resEl.innerHTML = `<div class="loc-empty">Loading…</div>`;
    try {
      await _fetchReleases();
    } catch (e) {
      if (resEl) resEl.innerHTML = `<div class="loc-empty" style="color:#e88">Failed: ${_esc(String(e).slice(0, 200))}</div>`;
      return;
    }
    _renderControls();
    _renderYearAnchors();
    _renderResults();
    _renderPagination();
  }

  window.initLabelsView = function initLabelsView() {
    if (!window._isAdmin) return;
    _renderControls();
    _renderYearAnchors();
    _renderResults();
    _renderPagination();
    // Lazy-load the labels list in the background so the picker is
    // ready the moment the admin clicks the dropdown.
    _fetchLabelsList().catch(() => {});
  };
})();
