// ── Timeline view (admin-only) ─────────────────────────────────────
// Year-bucketed release feed from the Blues Archive. Renders into
// the SPA's #timeline-view (lazy-loaded by switchView when v=timeline).
// Backed by /api/blues-archive/timeline which unnests
// blues_artists.discogs_releases for the configured year range and
// LEFT JOINs release_cache for cover thumbs (null when uncached).
//
// Layout: sticky year column on the left ("1925" "1926" … click to
// jump), card flow on the right grouped by year. Cards have a small
// cover, the release title (links to in-app release/master modal),
// artist name (links to archive profile), label, type.

(function () {
  // Default to the project's intended early-blues window. Single
  // fetch — at limit 500 we don't need lazy-load per year.
  const TL_FROM = 1925;
  const TL_TO   = 1930;
  const TL_LIMIT = 500;

  let _tlData = null;   // { rows, total }
  let _tlLoaded = false;

  function _esc(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  function _labelStr(lbl) {
    // release_label comes back as either a JSON array of label objects
    // (when seeded from Discogs) or a plain string (manual entries).
    if (!lbl) return "";
    if (typeof lbl === "string") return lbl;
    if (Array.isArray(lbl)) {
      return lbl.map(x => typeof x === "string" ? x : (x?.name || "")).filter(Boolean).join(", ");
    }
    if (typeof lbl === "object" && lbl.name) return String(lbl.name);
    return "";
  }

  // Bucket rows by year, sorted ascending.
  function _bucketByYear(rows) {
    const m = new Map();
    for (const r of rows) {
      const y = Number(r.release_year);
      if (!Number.isFinite(y)) continue;
      if (!m.has(y)) m.set(y, []);
      m.get(y).push(r);
    }
    return Array.from(m.entries()).sort((a, b) => a[0] - b[0]);
  }

  async function _tlLoad() {
    const grid = document.getElementById("timeline-grid");
    const status = document.getElementById("timeline-status");
    if (grid) grid.textContent = "Loading…";
    if (status) status.textContent = "";
    try {
      const params = new URLSearchParams({
        from: String(TL_FROM),
        to:   String(TL_TO),
        limit: String(TL_LIMIT),
      });
      const r = await apiFetch(`/api/blues-archive/timeline?${params}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      _tlData = await r.json();
      _tlLoaded = true;
      _tlRender();
    } catch (e) {
      if (grid) grid.innerHTML = `<div style="color:#e88">Load failed: ${_esc(e?.message || e)}</div>`;
    }
  }
  window._tlLoad = _tlLoad;

  function _tlRender() {
    const grid    = document.getElementById("timeline-grid");
    const yearsEl = document.getElementById("timeline-years");
    const status  = document.getElementById("timeline-status");
    if (!grid || !yearsEl) return;
    const rows = _tlData?.rows || [];
    const total = _tlData?.total ?? rows.length;
    if (status) {
      status.textContent =
        `${rows.length.toLocaleString()} shown · ${total.toLocaleString()} total in ${TL_FROM}–${TL_TO}`;
    }
    if (!rows.length) {
      grid.innerHTML = `<div style="color:var(--muted);font-style:italic">No releases in ${TL_FROM}–${TL_TO} yet. Seed Discogs years on the Blues DB tab.</div>`;
      yearsEl.innerHTML = "";
      return;
    }
    const buckets = _bucketByYear(rows);

    // Year axis — sticky on scroll. Each year jumps to its section.
    yearsEl.innerHTML = buckets.map(([y, items]) => `
      <a href="#tl-year-${y}" onclick="event.preventDefault();document.getElementById('tl-year-${y}')?.scrollIntoView({behavior:'smooth',block:'start'})"
         style="color:var(--accent);text-decoration:none;font-weight:600;font-variant-numeric:tabular-nums">
        ${y} <span style="color:var(--muted);font-weight:400;font-size:0.8rem">${items.length}</span>
      </a>
    `).join("");

    // Card flow — one section per year.
    grid.innerHTML = buckets.map(([y, items]) => `
      <section id="tl-year-${y}" style="border-top:1px solid var(--border);padding-top:0.5rem">
        <h3 style="margin:0 0 0.4rem;font-size:1.4rem;color:var(--accent);font-variant-numeric:tabular-nums">${y}</h3>
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:0.6rem">
          ${items.map(_tlCardHtml).join("")}
        </div>
      </section>
    `).join("");
  }

  function _tlCardHtml(row) {
    const id     = Number(row.release_id);
    const type   = String(row.release_type || "release");
    const title  = String(row.release_title || "Untitled");
    const label  = _labelStr(row.release_label);
    const artist = String(row.artist_name || "");
    const aid    = Number(row.artist_id);
    const thumb  = row.cover_thumb ? String(row.cover_thumb) : "";
    const safeUrl = `https://www.discogs.com/${type === "master" ? "master" : "release"}/${id}`;
    const typeUp = type.toUpperCase();
    const coverHtml = thumb
      ? `<img src="${_esc(thumb)}" alt="" loading="lazy"
              style="width:64px;height:64px;object-fit:cover;border-radius:4px;flex:0 0 auto;background:var(--surface-raised)" />`
      : `<div style="width:64px;height:64px;border-radius:4px;background:var(--surface-raised);display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:1.6rem;flex:0 0 auto">♪</div>`;
    return `
      <article style="display:flex;gap:0.6rem;padding:0.5rem;border:1px solid var(--border);border-radius:5px;background:rgba(255,255,255,0.02);align-items:flex-start">
        <a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenRelease?.(${id},'${_esc(type)}','${_esc(safeUrl)}')" title="Open ${typeUp} popup" style="flex:0 0 auto">${coverHtml}</a>
        <div style="min-width:0;flex:1;display:flex;flex-direction:column;gap:0.15rem">
          <a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenRelease?.(${id},'${_esc(type)}','${_esc(safeUrl)}')"
             title="Open ${typeUp} popup"
             style="font-weight:600;color:var(--text);text-decoration:none;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${_esc(title)}</a>
          ${artist
            ? `<a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenArtist?.(${aid})"
                  title="Open ${_esc(artist)} profile"
                  style="font-size:0.82rem;color:var(--accent);text-decoration:none;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${_esc(artist)}</a>`
            : ""}
          <div style="font-size:0.74rem;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">
            ${label ? `${_esc(label)} · ` : ""}<span style="text-transform:uppercase">${_esc(type)}</span>
          </div>
        </div>
      </article>
    `;
  }

  // SPA entry point — idempotent. switchView calls this every time
  // v=timeline is shown; we re-render from cached data if loaded, only
  // refetch on first show or explicit ↻ Refresh click. Eagerly warms
  // /blues-archive.js so card clicks (which call _baOpenRelease /
  // _baOpenArtist) hit ready functions instead of optional-chain
  // no-ops when the user lands here first.
  function initTimelineView() {
    if (typeof window._baOpenRelease !== "function"
        && typeof window._sdLoadModule === "function") {
      try { window._sdLoadModule("/blues-archive.js"); } catch {}
    }
    if (!_tlLoaded) {
      _tlLoad();
    } else {
      _tlRender();
    }
  }
  window.initTimelineView = initTimelineView;
})();
