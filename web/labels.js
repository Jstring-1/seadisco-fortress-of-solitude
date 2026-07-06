// ── Labels page (admin-only) — chronological carousel ────────────
// One release on screen at a time, big "open" card with the
// release's cover, metadata, and a collapsed tracklist. Click the
// prev/next peek card (or use ← →) to flip through the label's
// catalog in year ASC + catno ASC order. Source is release_cache
// only — no Discogs calls. Pages of 200 rows are loaded lazily; the
// next page is prefetched as the user nears the end of the current
// one.

(function () {
  if (window.__sdLabelsBound) return;
  window.__sdLabelsBound = true;

  const PER_PAGE = 200;

  const _state = {
    label:        "",
    type:         "",
    vinylOnly:    false,
    yearFrom:     "",
    yearTo:       "",
    items:        [],          // flat list across all loaded pages
    yearAnchors: [],
    total:        0,
    pagesLoaded:  0,
    hasMore:      false,
    index:        0,           // current carousel position within items
    labelsAll:    null,
    pickerOpen:   false,
    tracksOpen:   false,
    loading:      false,
    inFlightPage: null,
  };
  window._sdLabelsState = _state;

  function _esc(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function _fmt(n) { return Number(n || 0).toLocaleString(); }

  // ── Data ────────────────────────────────────────────────────────
  async function _fetchLabelsList() {
    if (_state.labelsAll) return _state.labelsAll;
    const r = await apiFetch("/api/admin/release-cache/labels?limit=2000");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    _state.labelsAll = Array.isArray(j.items) ? j.items : [];
    return _state.labelsAll;
  }

  async function _fetchPage(page) {
    if (!_state.label) return null;
    const q = new URLSearchParams();
    q.set("label", _state.label);
    q.set("page",  String(page));
    q.set("per_page", String(PER_PAGE));
    if (_state.type) q.set("type", _state.type);
    if (_state.vinylOnly) q.set("format", "vinyl");
    if (_state.yearFrom) q.set("year_from", _state.yearFrom);
    if (_state.yearTo)   q.set("year_to",   _state.yearTo);
    const r = await apiFetch(`/api/admin/labels/releases?${q.toString()}`);
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      throw new Error(j?.error || `HTTP ${r.status}`);
    }
    return await r.json();
  }

  async function _resetAndLoad() {
    _state.items = [];
    _state.yearAnchors = [];
    _state.total = 0;
    _state.pagesLoaded = 0;
    _state.hasMore = false;
    _state.index = 0;
    _state.tracksOpen = false;
    if (!_state.label) { _render(); return; }
    _state.loading = true;
    _render();
    try {
      const j = await _fetchPage(1);
      _state.items = Array.isArray(j.items) ? j.items : [];
      _state.yearAnchors = Array.isArray(j.yearAnchors) ? j.yearAnchors : [];
      _state.total = Number(j.total) || 0;
      _state.pagesLoaded = 1;
      _state.hasMore = !!j.hasMore;
    } catch (e) {
      _state.loadError = String(e);
    } finally {
      _state.loading = false;
      _render();
    }
  }

  async function _ensureLoaded(index) {
    // Lazy-load subsequent pages so flipping past the boundary doesn't
    // stall the carousel. We load when the user is within 20 cards of
    // the loaded edge.
    if (!_state.hasMore) return;
    if (_state.inFlightPage) return;
    if (index < _state.items.length - 20) return;
    const next = _state.pagesLoaded + 1;
    _state.inFlightPage = next;
    try {
      const j = await _fetchPage(next);
      if (j?.items?.length) {
        _state.items.push(...j.items);
        _state.pagesLoaded = next;
        _state.hasMore = !!j.hasMore;
      } else {
        _state.hasMore = false;
      }
    } catch {} finally {
      _state.inFlightPage = null;
    }
  }

  // ── Navigation ──────────────────────────────────────────────────
  async function _goto(idx) {
    if (idx < 0) idx = 0;
    if (idx >= _state.total) idx = _state.total - 1;
    _state.index = idx;
    _state.tracksOpen = false;
    _ensureLoaded(idx).then(() => _render());
    _render();
  }
  window._labelsGoto = _goto;

  function _prev() { _goto(Math.max(0, _state.index - 1)); }
  function _next() { _goto(Math.min(_state.total - 1, _state.index + 1)); }
  window._labelsPrev = _prev;
  window._labelsNext = _next;

  function _onKey(ev) {
    const v = document.getElementById("labels-view");
    if (!v || v.style.display === "none") return;
    // Ignore keystrokes when typing in an input.
    const t = ev.target;
    if (t && /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName)) return;
    if (ev.key === "ArrowLeft")  { ev.preventDefault(); _prev(); }
    if (ev.key === "ArrowRight") { ev.preventDefault(); _next(); }
  }

  // ── Rendering ───────────────────────────────────────────────────
  function _renderControls() {
    const el = document.getElementById("labels-controls");
    if (!el) return;
    const labelBtnText = _state.label || "(choose a label)";
    const yearJumpOpts = (_state.yearAnchors || [])
      .filter(a => a.year)
      .map(a => `<option value="${a.year}">${a.year} (${a.count})</option>`)
      .join("");
    const cur = _state.items[_state.index];
    const posText = _state.total
      ? `${(_state.index + 1).toLocaleString()} of ${_fmt(_state.total)}${cur?.year ? ` · ${cur.year}` : ""}`
      : "";
    el.innerHTML = `
      <div style="display:flex;flex-wrap:wrap;gap:0.4rem;align-items:center;font-size:0.82rem;padding:0.4rem 0.6rem;background:var(--bg);border-bottom:1px solid var(--border)">
        <span style="position:relative">
          <button type="button" id="labels-picker-btn" onclick="_labelsTogglePicker(event)"
            style="padding:0.4rem 0.7rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;cursor:pointer;min-width:220px;text-align:left;font-weight:600">
            ${_esc(labelBtnText)} <span style="color:var(--muted)">▼</span>
          </button>
          <div id="labels-picker-panel" style="display:${_state.pickerOpen ? "flex" : "none"};position:absolute;left:0;top:100%;z-index:50;background:var(--surface);border:1px solid var(--border);border-radius:5px;padding:0.5rem;margin-top:0.2rem;box-shadow:0 6px 18px rgba(0,0,0,0.5);min-width:320px;max-height:400px;overflow:hidden;flex-direction:column;gap:0.4rem">
            <input id="labels-picker-search" type="text" placeholder="Filter labels…" oninput="_labelsRenderPickerList()"
              style="padding:0.35rem;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:3px">
            <div id="labels-picker-list" style="overflow-y:auto;max-height:300px;border:1px solid var(--border);border-radius:3px;padding:0.3rem 0.4rem;font-size:0.8rem">Loading…</div>
          </div>
        </span>

        <label style="display:inline-flex;gap:0.3rem;align-items:center;color:var(--muted)">Type
          <select id="labels-type" onchange="_labelsOnFiltersChange()"
            style="padding:0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
            <option value=""        ${(_state.type === "" || _state.type === "masters_plus") ? "selected" : ""}>Masters+</option>
            <option value="both"    ${_state.type === "both"    ? "selected" : ""}>both</option>
            <option value="master"  ${_state.type === "master"  ? "selected" : ""}>masters only</option>
            <option value="release" ${_state.type === "release" ? "selected" : ""}>releases only</option>
          </select>
        </label>

        <label style="display:inline-flex;gap:0.3rem;align-items:center;color:var(--muted);cursor:pointer">
          <input type="checkbox" id="labels-vinyl-only" ${_state.vinylOnly ? "checked" : ""} onchange="_labelsOnFiltersChange()">
          Vinyl only
        </label>

        <label style="display:inline-flex;gap:0.3rem;align-items:center;color:var(--muted)">Year
          <input id="labels-year-from" type="number" placeholder="from" value="${_esc(_state.yearFrom)}"
            onchange="_labelsOnFiltersChange()" style="width:70px;padding:0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          <input id="labels-year-to" type="number" placeholder="to" value="${_esc(_state.yearTo)}"
            onchange="_labelsOnFiltersChange()" style="width:70px;padding:0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          ${(_state.yearFrom || _state.yearTo) ? `<button type="button" onclick="_labelsClearYears()" style="padding:0.15rem 0.4rem;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:3px;cursor:pointer;font-size:0.72rem">clear</button>` : ""}
        </label>

        <label style="display:inline-flex;gap:0.3rem;align-items:center;color:var(--muted)">Jump
          <select id="labels-jump" onchange="_labelsJumpToYear(parseInt(this.value,10))"
            style="padding:0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
            <option value="">— year —</option>
            ${yearJumpOpts}
          </select>
        </label>

        <span style="margin-left:auto;color:var(--text);font-size:0.85rem;font-weight:600;text-align:right">
          ${posText}
        </span>
      </div>

      ${_state.total > 1 ? `
      <div style="padding:0.4rem 0.6rem;border-bottom:1px solid var(--border)">
        <input type="range" id="labels-scrubber" min="0" max="${_state.total - 1}" value="${_state.index}"
          oninput="_labelsScrubberChange(this.value)" onchange="_labelsScrubberChange(this.value)"
          style="width:100%;cursor:pointer">
      </div>` : ""}
    `;

    if (_state.pickerOpen) {
      _renderPickerList();
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

  async function _togglePicker(ev) {
    ev?.preventDefault?.(); ev?.stopPropagation?.();
    _state.pickerOpen = !_state.pickerOpen;
    _renderControls();
    if (_state.pickerOpen) {
      try { await _fetchLabelsList(); }
      catch (e) {
        const list = document.getElementById("labels-picker-list");
        if (list) list.innerHTML = `<span style="color:#e88">${_esc(String(e).slice(0, 200))}</span>`;
        return;
      }
      _renderPickerList();
    }
  }
  window._labelsTogglePicker = _togglePicker;

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
        style="display:flex;gap:0.4rem;align-items:center;padding:0.25rem 0.3rem;cursor:pointer;border-radius:3px;${selected ? "background:rgba(120,220,140,0.12);color:#7ddc8c" : ""}"
        onmouseenter="this.style.background='rgba(255,255,255,0.06)'"
        onmouseleave="this.style.background='${selected ? "rgba(120,220,140,0.12)" : "transparent"}'">
        <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_esc(it.name)}</span>
        <span style="color:var(--muted);font-size:0.72rem">${_fmt(it.count)}</span>
      </div>`;
    }).join("");
    list.innerHTML = rows || `<div style="color:var(--muted)">No matches.</div>`;
  }
  window._labelsRenderPickerList = _renderPickerList;

  async function _pickLabel(name) {
    _state.label = String(name || "");
    _state.pickerOpen = false;
    _state.yearFrom = "";
    _state.yearTo = "";
    _renderControls();
    await _resetAndLoad();
  }
  window._labelsPickLabel = _pickLabel;

  function _onFiltersChange() {
    _state.type       = document.getElementById("labels-type")?.value || "";
    _state.vinylOnly  = !!document.getElementById("labels-vinyl-only")?.checked;
    _state.yearFrom   = document.getElementById("labels-year-from")?.value || "";
    _state.yearTo     = document.getElementById("labels-year-to")?.value || "";
    _resetAndLoad();
  }
  window._labelsOnFiltersChange = _onFiltersChange;

  function _clearYears() {
    _state.yearFrom = "";
    _state.yearTo = "";
    _resetAndLoad();
  }
  window._labelsClearYears = _clearYears;

  async function _jumpToYear(year) {
    if (!Number.isFinite(year)) return;
    // Find the first loaded item with this year. If not loaded yet,
    // walk pages until we hit it (cheap — anchors guarantee it exists
    // within the current filter set).
    const findIdx = () => _state.items.findIndex(it => it.year === year);
    let idx = findIdx();
    while (idx < 0 && _state.hasMore) {
      await _ensureLoaded(_state.items.length); // pull the next page
      idx = findIdx();
    }
    if (idx >= 0) _goto(idx);
    // Reset the dropdown so it can be re-fired for the same year.
    const sel = document.getElementById("labels-jump");
    if (sel) sel.value = "";
  }
  window._labelsJumpToYear = _jumpToYear;

  function _scrubberChange(v) {
    const idx = Math.max(0, Math.min(_state.total - 1, parseInt(String(v), 10) || 0));
    _goto(idx);
  }
  window._labelsScrubberChange = _scrubberChange;

  function _toggleTracks() {
    _state.tracksOpen = !_state.tracksOpen;
    _renderStage();
  }
  window._labelsToggleTracks = _toggleTracks;

  function _renderStage() {
    const el = document.getElementById("labels-stage");
    if (!el) return;
    if (!_state.label) {
      el.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:60vh;color:var(--muted);font-size:1.1rem">Pick a label to start.</div>`;
      return;
    }
    if (_state.loading) {
      el.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:60vh;color:var(--muted)">Loading…</div>`;
      return;
    }
    if (_state.loadError) {
      el.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:60vh;color:#e88">${_esc(String(_state.loadError).slice(0, 200))}</div>`;
      return;
    }
    if (!_state.items.length) {
      el.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:60vh;color:var(--muted)">No releases match.</div>`;
      return;
    }
    const cur = _state.items[_state.index];
    if (!cur) {
      el.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:60vh;color:var(--muted)">Loading…</div>`;
      return;
    }
    const prevIt = _state.items[_state.index - 1];
    const nextIt = _state.items[_state.index + 1];

    const formats = Array.isArray(cur.formats) && cur.formats.length
      ? cur.formats.map(f => {
          const desc = Array.isArray(f?.descriptions) && f.descriptions.length ? `, ${f.descriptions.join(", ")}` : "";
          const qty  = f?.qty && Number(f.qty) > 1 ? `${f.qty}×` : "";
          return `${qty}${_esc(f?.name || "")}${_esc(desc)}`;
        }).join(" / ")
      : "";
    // Catalog number(s) only — the label name itself is shown once in
    // the shared banner above the stage (every card on this page is
    // the same label, so repeating the name per-card was noise).
    const _catnoFor = (it) => {
      const fromLabels = Array.isArray(it?.labels) && it.labels.length
        ? it.labels.map(l => l?.catno || "").filter(Boolean).join(" · ")
        : "";
      return _esc(fromLabels || it?.catno || "");
    };
    const _typeLineFor = (it) => it?._source === "external"
      ? `<span style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:3px;border:1px dashed rgba(255,255,255,0.2)">External</span>`
      : _esc(it?.type || "");

    const isExternal = cur._source === "external";
    const cover = cur.cover
      ? `<img src="${_esc(cur.cover)}" alt="" loading="eager"
            style="width:100%;max-width:240px;aspect-ratio:1/1;object-fit:cover;border-radius:8px;background:rgba(255,255,255,0.05);box-shadow:0 4px 18px rgba(0,0,0,0.4)">`
      : isExternal
        ? `<div style="width:240px;max-width:100%;aspect-ratio:1/1;background:repeating-linear-gradient(45deg,rgba(255,255,255,0.04),rgba(255,255,255,0.04) 10px,rgba(255,255,255,0.07) 10px,rgba(255,255,255,0.07) 20px);border-radius:8px;display:flex;flex-direction:column;align-items:center;justify-content:center;color:var(--muted);gap:0.4rem;border:1px dashed rgba(255,255,255,0.18)">
            <div style="font-size:0.7rem;letter-spacing:0.1em;text-transform:uppercase">Not in Discogs</div>
            <div style="font-size:2rem;font-weight:700;color:var(--text)">${_esc(cur.catno || "")}</div>
            <div style="font-size:0.7rem">${_esc(cur._externalSource || "external source")}</div>
          </div>`
        : `<div style="width:240px;max-width:100%;aspect-ratio:1/1;background:rgba(255,255,255,0.04);border-radius:8px;display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:0.85rem">no image</div>`;
    // Cover opens the album popup directly — external stub cards have
    // no real Discogs id to open, so they stay non-clickable.
    const coverHtml = isExternal
      ? cover
      : `<div onclick="_labelsOpenRelease(${cur.id}, '${cur.type}')" style="cursor:pointer" title="Open album">${cover}</div>`;

    const tracks = Array.isArray(cur.tracklist) ? cur.tracklist : [];
    const tracksLabel = isExternal ? "Sides" : "Tracks";
    const tracksBlock = tracks.length
      ? `<div style="margin-top:0.8rem;border:1px solid var(--border);border-radius:6px;overflow:hidden">
          <button type="button" onclick="_labelsToggleTracks()"
            style="width:100%;text-align:left;background:rgba(255,255,255,0.04);color:var(--text);padding:0.45rem 0.7rem;border:none;cursor:pointer;font-size:0.85rem;display:flex;justify-content:space-between;align-items:center">
            <span><strong>${tracksLabel}</strong> <span style="color:var(--muted)">(${tracks.length})</span></span>
            <span style="color:var(--muted);font-size:0.75rem">${_state.tracksOpen ? "▲ collapse" : "▼ expand"}</span>
          </button>
          ${_state.tracksOpen ? `<div style="padding:0.4rem 0.7rem;font-size:0.82rem;max-height:220px;overflow-y:auto">
            ${tracks.map(t => `<div style="display:flex;gap:0.6rem;padding:0.15rem 0;border-bottom:1px dashed rgba(255,255,255,0.05);align-items:baseline">
              <span style="color:var(--muted);min-width:2.5em">${_esc(t.position || "")}</span>
              <span style="flex:1">
                ${_esc(t.title || "")}
                ${isExternal && t._artist && t._artist !== cur.artist ? `<span style="color:var(--muted);font-size:0.72rem"> · ${_esc(t._artist)}</span>` : ""}
                ${isExternal && t._composer ? `<span style="color:var(--muted);font-size:0.72rem"> · (${_esc(t._composer)})</span>` : ""}
                ${isExternal && t._matrix ? `<span style="color:var(--muted);font-size:0.7rem;margin-left:0.4rem">mx: ${_esc(t._matrix)}</span>` : ""}
              </span>
              ${t.duration ? `<span style="color:var(--muted)">${_esc(t.duration)}</span>` : ""}
            </div>`).join("")}
          </div>` : ""}
        </div>`
      : "";

    // Same top-to-bottom order as the center card (title, artist,
    // catno, type) so scanning ahead/back doesn't require re-locating
    // each field in a different spot.
    const peekText = (it) => it
      ? `<div style="font-size:0.82rem;color:var(--text);margin-top:0.4rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_esc(it.title || "")}</div>
        <div style="font-size:0.75rem;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_esc(it.artist || "")}</div>
        <div style="font-size:0.68rem;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_catnoFor(it)}</div>
        <div style="font-size:0.66rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.03em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_typeLineFor(it)}</div>`
      : "";

    const peekCover = (it) => it && it.coverThumb
      ? `<img src="${_esc(it.coverThumb)}" alt="" loading="lazy"
            style="width:100%;aspect-ratio:1/1;object-fit:cover;border-radius:5px;opacity:0.55;transition:opacity 0.2s">`
      : it && it._source === "external"
        ? `<div style="width:100%;aspect-ratio:1/1;background:repeating-linear-gradient(45deg,rgba(255,255,255,0.03),rgba(255,255,255,0.03) 6px,rgba(255,255,255,0.06) 6px,rgba(255,255,255,0.06) 12px);border-radius:5px;opacity:0.55;display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:0.72rem">ext</div>`
        : `<div style="width:100%;aspect-ratio:1/1;background:rgba(255,255,255,0.03);border-radius:5px;opacity:0.55"></div>`;

    // Same lookup popup used on album-popup title/artist text (search
    // SeaDisco / Wikipedia / copy / etc.) — clicking either opens it.
    // The title also gets a direct "Open release/master" shortcut at
    // the top of that popup (external stub cards have no real id, so
    // they're skipped).
    const titleHtml = (typeof entityLookupLinkHtml === "function")
      ? entityLookupLinkHtml("release", cur.title || "(untitled)", {
          title: `Lookup options for "${cur.title || ""}"`,
          ...(isExternal ? {} : { openId: cur.id, openType: cur.type === "master" ? "master" : "release" }),
        })
      : _esc(cur.title || "(untitled)");
    const artistHtml = cur.artist && typeof entityLookupLinkHtml === "function"
      ? entityLookupLinkHtml("artist", cur.artist, { title: `Lookup options for "${cur.artist}"` })
      : _esc(cur.artist || "");
    const ebayUrl = `https://www.ebay.com/sch/i.html?_nkw=${encodeURIComponent(`${cur.artist || ""} ${cur.title || ""}`.trim())}`;

    // Year + label banner, shown once above the whole row — every card
    // on this page is the same label, so it doesn't need repeating in
    // each of the three cards.
    const bannerHtml = `
      <div style="text-align:center;font-size:0.8rem;color:var(--accent);text-transform:uppercase;letter-spacing:0.05em;padding:0 0.6rem 0.3rem">
        ${_esc(cur.year || "—")} · ${_esc(_state.label)}
      </div>`;

    el.innerHTML = `
      ${bannerHtml}
      <div style="display:grid;grid-template-columns:120px 1fr 120px;gap:0.8rem;align-items:start;padding:0.6rem">
        <!-- Prev peek -->
        <div style="cursor:${prevIt ? "pointer" : "default"}" onclick="${prevIt ? "_labelsPrev()" : ""}"
             onmouseenter="this.querySelector('img')?.style.setProperty('opacity','1')"
             onmouseleave="this.querySelector('img')?.style.setProperty('opacity','0.55')">
          ${peekCover(prevIt)}
          ${peekText(prevIt)}
        </div>

        <!-- Center card -->
        <div style="display:flex;flex-direction:column;align-items:center;text-align:center;gap:0.3rem">
          ${coverHtml}
          <div style="font-size:1.2rem;font-weight:600;line-height:1.2;margin-top:0.3rem">${titleHtml}</div>
          <div style="font-size:0.95rem;color:var(--text)">${artistHtml}</div>
          <div style="font-size:0.82rem;color:var(--muted)">${_catnoFor(cur)}</div>
          <div style="font-size:0.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.05em">${_typeLineFor(cur)}</div>
          ${formats ? `<div style="font-size:0.78rem;color:var(--muted)">${formats}${cur.country ? ` · ${_esc(cur.country)}` : ""}</div>` : (cur.country ? `<div style="font-size:0.78rem;color:var(--muted)">${_esc(cur.country)}</div>` : "")}

          <div style="display:flex;gap:0.6rem;flex-wrap:wrap;justify-content:center;margin-top:0.3rem">
            ${isExternal
              ? `<span style="font-size:0.75rem;color:var(--muted);font-style:italic">Source: ${_esc(cur._externalSource || "unknown")}</span>`
              : `<a class="admin-btn" href="https://www.discogs.com/${cur.type === 'master' ? 'master' : 'release'}/${cur.id}" target="_blank" rel="noopener" style="text-decoration:none">Discogs ↗</a>`}
            <a class="admin-btn" href="${_esc(ebayUrl)}" target="_blank" rel="noopener" style="text-decoration:none">eBay ↗</a>
          </div>

          <div style="width:100%;max-width:520px;text-align:left">
            ${tracksBlock}
            ${cur.notes ? `<details style="margin-top:0.5rem;font-size:0.8rem"><summary style="cursor:pointer;color:var(--muted)">Notes</summary><div style="margin-top:0.3rem;color:var(--muted);white-space:pre-wrap">${_esc(String(cur.notes).slice(0, 1500))}</div></details>` : ""}
          </div>
        </div>

        <!-- Next peek -->
        <div style="cursor:${nextIt ? "pointer" : "default"}" onclick="${nextIt ? "_labelsNext()" : ""}"
             onmouseenter="this.querySelector('img')?.style.setProperty('opacity','1')"
             onmouseleave="this.querySelector('img')?.style.setProperty('opacity','0.55')">
          ${peekCover(nextIt)}
          ${peekText(nextIt)}
        </div>
      </div>
    `;
  }

  function _openRelease(id, type) {
    const endpoint = type === "master" ? "master" : "release";
    if (typeof window.openModal === "function") {
      window.openModal(null, id, endpoint, `https://www.discogs.com/${endpoint}/${id}`);
    } else {
      window.open(`/?op=${encodeURIComponent(endpoint)}:${id}`, "_self");
    }
  }
  window._labelsOpenRelease = _openRelease;

  // ── URL deep-link — identify + reopen a specific album on this page ──
  // lbl = label name, lid = discogs release/master id (or the negative
  // external_discography row id), ltype = master|release|external.
  // Every render stamps the current position into the URL (replaceState,
  // so prev/next clicks don't spam browser history); loading that URL
  // fresh re-selects the label and jumps straight to the same album.
  function _urlParams() {
    const p = new URLSearchParams(location.search);
    return { label: p.get("lbl") || "", id: p.get("lid") || "", type: p.get("ltype") || "" };
  }

  function _updateUrl() {
    const u = new URL(location.href);
    u.searchParams.set("v", "labels");
    if (_state.label) u.searchParams.set("lbl", _state.label); else u.searchParams.delete("lbl");
    const cur = _state.items[_state.index];
    if (cur) {
      u.searchParams.set("lid", String(cur.id));
      u.searchParams.set("ltype", cur.type || "release");
    } else {
      u.searchParams.delete("lid");
      u.searchParams.delete("ltype");
    }
    history.replaceState({}, "", u.toString());
  }

  async function _openFromUrl(label, id, type) {
    _state.label = label;
    // Bypass the default Masters+ filter so a deep-linked release
    // isn't hidden just because it also has a parent master.
    if (id) _state.type = "both";
    _renderControls();
    await _resetAndLoad();
    if (id) {
      const targetId = Number(id);
      const findIdx = () => _state.items.findIndex(it => Number(it.id) === targetId && (!type || it.type === type));
      let idx = findIdx();
      while (idx < 0 && _state.hasMore) {
        await _ensureLoaded(_state.items.length);
        idx = findIdx();
      }
      if (idx >= 0) { await _goto(idx); return; }
    }
    _render();
  }

  function _render() {
    _renderControls();
    _renderStage();
    _updateUrl();
  }

  // ── Init ────────────────────────────────────────────────────────
  window.initLabelsView = function initLabelsView() {
    if (!window._isAdmin) return;
    // Replace the legacy view skeleton with the carousel scaffolding
    // on first init. Idempotent — re-init reuses existing nodes.
    const view = document.getElementById("labels-view");
    if (view && !view.dataset.sdLabelsCarouselReady) {
      view.dataset.sdLabelsCarouselReady = "1";
      view.innerHTML = `
        <div style="margin:0.4rem 0 0.6rem">
          <div style="font-size:1.15rem;font-weight:600">Labels</div>
        </div>
        <div id="labels-controls" style="position:sticky;top:0;background:var(--bg);z-index:50;border:1px solid var(--border);border-radius:6px;margin-bottom:0.5rem"></div>
        <div id="labels-stage"></div>
      `;
      document.addEventListener("keydown", _onKey);
    }
    _fetchLabelsList().catch(() => {});
    if (!_state.label) {
      const { label, id, type } = _urlParams();
      if (label) { _openFromUrl(label, id, type); return; }
    }
    _render();
  };
})();
