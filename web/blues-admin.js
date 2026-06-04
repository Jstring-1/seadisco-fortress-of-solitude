// blues-admin.js — Blues DB + Lyrics archive admin features.
// Extracted from web/admin.html so the same functions can be loaded
// by the main site (web/index.html) and surface inside the Discovery
// Blues Archive view. No functional changes — pure relocation.
//
// Loaded by both admin.html and (lazily) by the Discovery view.
// All helpers and functions remain on the global scope; admin.html
// markup and Discovery view markup both look them up by name.
//
// Caveat: _adminClickRefresh / _adminWithRefresh stay in admin.html —
// they're shared by ~10 other admin tabs and tangled with their own
// helpers. The blues-archive panel on index.html doesn't render the
// admin refresh buttons, so it doesn't need those helpers in scope.

// ── Shared admin grid sort helpers ──────────────────────────────────
// (moved from admin.html — used by Blues DB, Lyrics, AND other admin
//  tabs (Submitted Tracks, Unavailable), so they have to be in scope
//  before any of them initialize.)
// ── Shared: click-sortable admin grids ────────────────────────────────
// Generic client-side sort. colTypes maps a key → "num" | "date";
// anything else is a case-insensitive string compare.
function _adminSortApply(rows, state, colTypes) {
  if (!state || !state.key) return rows;
  const t = (colTypes || {})[state.key] || "str";
  const dir = state.dir === "desc" ? -1 : 1;
  const val = (o) => {
    let v = o[state.key];
    if (t === "num")  { v = Number(v); return Number.isFinite(v) ? v : -Infinity; }
    if (t === "date") { const d = Date.parse(v); return Number.isFinite(d) ? d : -Infinity; }
    return String(v ?? "").toLowerCase();
  };
  return rows.slice().sort((a, b) => {
    const av = val(a), bv = val(b);
    if (av < bv) return -1 * dir;
    if (av > bv) return  1 * dir;
    return 0;
  });
}
function _adminToggleSort(state, key) {
  if (state.key === key) state.dir = state.dir === "asc" ? "desc" : "asc";
  else { state.key = key; state.dir = "asc"; }
}
// Build a clickable sort header cell.
function _adminSortTh(label, key, state, fn, extraStyle) {
  const active = state.key === key;
  const arrow = active ? (state.dir === "desc" ? "▼" : "▲") : "";
  return `<th class="admin-sort-th${active ? " is-active" : ""}" style="padding:0.3rem 0.5rem;${extraStyle || ""}" onclick="${fn}('${key}')">${label}<span class="admin-sort-arrow">${arrow}</span></th>`;
}
// Clickable album cell — opens the shared in-page album modal
// (modal.js is loaded on /admin). Falls back to a dash when there's
// no release context (e.g. an unavailable Discogs-native video with
// no crowd-submitted override to map it back to an album).
function _adminAlbumLink(type, id, label) {
  if (!id || !type) return '<span style="color:var(--muted)">—</span>';
  const t = String(type), i = String(id);
  const lbl = label || `${t}/${i}`;
  const url = `https://www.discogs.com/${t}/${i}`;
  return `<a href="#" onclick="event.preventDefault();event.stopPropagation();openModal(event,'${i}','${t}','${url}')" title="Open album popup" style="color:#7eb8da;text-decoration:none">${escHtml(lbl)} ↗</a>`;
}

const _adminSubSortState  = { key: "submitted_at",  dir: "desc" };
const _adminUnavSortState = { key: "report_count",  dir: "desc" };
function _adminSubSort(key)  { _adminToggleSort(_adminSubSortState, key);  _renderAdminSubmissionsTable(); }
function _adminUnavSort(key) { _adminToggleSort(_adminUnavSortState, key); _renderAdminUnavailableTable(); }
window._adminSubSort  = _adminSubSort;
window._adminUnavSort = _adminUnavSort;

// ── Blues DB tab ────────────────────────────────────────────────────────
// rows = the FULL server-fetched list (one request per page-load).
// All subsequent sorts / search / filter changes / individual saves
// operate on this cached array so the UI updates instantly without
// re-fetching. Server is only re-hit when we genuinely need fresh
// data (page-load, bulk-enrich completion, etc.).
let _bluesDbState = { rows: [], total: 0, search: "", editingId: null, sort: "name", order: "asc" };
let _bluesSearchTimer = null;

function bluesDbInit() {
  bluesDbLoadStats();
  bluesDbLoadList();
  // Reattach to any background job already in flight (so reloading the
  // page mid-run still shows live progress on each button).
  // Only the bulk Discogs-full job has a button now; seed +
  // MB / Wiki / Discogs-ID buttons were removed when we moved to
  // ad-hoc "+" curation. The corresponding endpoints stay so the
  // resume-poll for Discogs-full works after a page reload.
  _bluesResumeBulkIfRunning("blues-enrich-discogs-full-btn", "/api/admin/blues/enrich-discogs-full/status", "Get all info from Discogs");
}

async function _bluesResumeBulkIfRunning(btnId, statusEndpoint, label) {
  try {
    const r = await apiFetch(statusEndpoint);
    if (!r.ok) return;
    const job = await r.json();
    if (job.status === "running") {
      const btn = document.getElementById(btnId);
      if (btn) btn.disabled = true;
      _bluesEnrichBulkPoll({ btnId, statusEndpoint, label });
    }
  } catch {}
}

async function bluesDbLoadStats() {
  // No-op when the admin Blues DB panel isn't in this page (e.g. when
  // blues-admin.js is loaded for the editor overlay on the main site).
  const statsEl = document.getElementById("blues-stats");
  if (!statsEl) return;
  try {
    const r = await apiFetch("/api/admin/blues/stats");
    if (!r.ok) return;
    const s = await r.json();
    const last = s.lastUpdate ? new Date(s.lastUpdate).toLocaleString() : "—";
    statsEl.textContent = `${s.total} artists · last update ${last}`;
  } catch {}
}

async function bluesDbLoadList() {
  const list = document.getElementById("blues-list");
  if (!list) return;
  list.innerHTML = '<div style="padding:0.6rem">Loading…</div>';
  // Admin grid shows ALL rows in one shot — `all=1` sidesteps the
  // server-side default 50-row cap. Server-side sort is still
  // requested as the initial order, but every subsequent sort /
  // search / filter pass is purely client-side off the cached rows
  // array. Don't pass `search` to the server — we let the client
  // filter so the search input updates instantly.
  const params = new URLSearchParams({
    all: "1",
    sort: _bluesDbState.sort,
    order: _bluesDbState.order,
  });
  try {
    const r = await apiFetch("/api/admin/blues/list?" + params.toString());
    if (!r.ok) { list.innerHTML = '<div style="color:#e88">Could not load.</div>'; return; }
    const data = await r.json();
    _bluesDbState.rows = data.rows; _bluesDbState.total = data.total;
    bluesDbRenderList();
    bluesDbRenderPager();
  } catch { list.innerHTML = '<div style="color:#e88">Error loading.</div>'; }
}

// Pull a single row fresh from the server and patch the cached array
// in place. Used after save / single-row enrich so the UI updates
// without re-fetching the entire list.
async function _bluesDbRefreshRow(id) {
  if (!id) return false;
  try {
    const r = await apiFetch("/api/admin/blues/" + id);
    if (!r.ok) return false;
    const fresh = await r.json();
    const idx = _bluesDbState.rows.findIndex(row => Number(row.id) === Number(id));
    if (idx >= 0) {
      _bluesDbState.rows[idx] = fresh;
    } else {
      _bluesDbState.rows.push(fresh);
      _bluesDbState.total = (_bluesDbState.total || 0) + 1;
    }
    return true;
  } catch { return false; }
}

// Drop a row from the cached array without a server round-trip.
function _bluesDbRemoveRowLocal(id) {
  const idx = _bluesDbState.rows.findIndex(row => Number(row.id) === Number(id));
  if (idx >= 0) {
    _bluesDbState.rows.splice(idx, 1);
    _bluesDbState.total = Math.max(0, (_bluesDbState.total || 0) - 1);
  }
}

// Client-side sort over the cached rows. Mirrors the column keys the
// server understands so the header arrows reflect the active order.
function _bluesDbSortRows(rows) {
  const key = _bluesDbState.sort || "name";
  const order = _bluesDbState.order === "desc" ? -1 : 1;
  // Picker per known sort column — keeps comparisons stable across
  // null / undefined / string-vs-number rows.
  const pick = (r) => {
    if (key === "name") return String(r.name || "").toLowerCase();
    if (key === "earliest_release") {
      const direct = r.earliest_release_year;
      if (Number.isFinite(direct)) return direct;
      const rels = Array.isArray(r.discogs_releases) ? r.discogs_releases : [];
      let min = Infinity;
      for (const x of rels) {
        const y = Number(x?.year);
        if (Number.isFinite(y) && y > 0 && y < min) min = y;
      }
      return Number.isFinite(min) ? min : null;
    }
    if (key === "styles") {
      const arr = Array.isArray(r.styles) ? r.styles : [];
      return arr.join(", ").toLowerCase();
    }
    if (key === "discogs_id") return Number(r.discogs_id) || 0;
    if (key === "release_count") {
      return Array.isArray(r.discogs_releases) ? r.discogs_releases.length : 0;
    }
    return String(r[key] ?? "").toLowerCase();
  };
  const out = rows.slice();
  out.sort((a, b) => {
    const va = pick(a);
    const vb = pick(b);
    // Push null/undefined to the bottom regardless of asc/desc so an
    // empty cell doesn't crowd out actual values.
    const na = va == null || va === "" || (typeof va === "number" && !Number.isFinite(va));
    const nb = vb == null || vb === "" || (typeof vb === "number" && !Number.isFinite(vb));
    if (na && !nb) return 1;
    if (!na && nb) return -1;
    if (na && nb) return 0;
    if (typeof va === "number" && typeof vb === "number") return (va - vb) * order;
    return String(va).localeCompare(String(vb)) * order;
  });
  return out;
}

// Apply search + thumbnail filter to the cached rows.
function _bluesDbFilterRows(rows) {
  const q = (_bluesDbState.search || "").toLowerCase();
  const noThumb = !!document.getElementById("blues-filter-nothumb")?.checked;
  // Pure-digit queries get an additional exact-id match path so the
  // admin can search "12345" and find rows by discogs_id regardless
  // of where it appears in the rendered list. Substring match too,
  // so partial pastes still work.
  const isAllDigits = /^\d+$/.test(q);
  return rows.filter(r => {
    if (noThumb && r.photo_url) return false;
    if (!q) return true;
    if (isAllDigits && r.discogs_id != null) {
      if (String(r.discogs_id) === q) return true;
      if (String(r.discogs_id).includes(q)) return true;
    }
    const hay = `${r.name || ""}\n${r.hometown || ""}\n${r.birthplace || ""}\n${(Array.isArray(r.styles) ? r.styles.join(" ") : "")}\n${r.discogs_id ?? ""}`.toLowerCase();
    return hay.includes(q);
  });
}

function bluesDbRenderList() {
  const list = document.getElementById("blues-list");
  // No admin Blues DB panel on this page (main site loads
  // blues-admin.js only for the editor overlay). When that's the
  // case, refresh the Discovery-side Blues Archive grid instead so
  // inline editor actions (Refresh from Discogs, Pick Discogs match,
  // per-row enrich, etc.) reflect immediately in the row behind the
  // overlay. Otherwise the user closes the editor and the row is
  // still showing the pre-edit photo / id / name.
  if (!list) {
    if (typeof window._baLoadList === "function") {
      try { window._baLoadList(); } catch {}
    }
    if (typeof window._baLoadStats === "function") {
      try { window._baLoadStats(); } catch {}
    }
    return;
  }
  if (!_bluesDbState.rows.length) {
    list.innerHTML = '<div style="padding:0.6rem;color:var(--muted)">No artists yet. Click <strong>Seed from Discogs</strong> or <strong>+ Add artist</strong>.</div>';
    bluesDbRenderPager();
    return;
  }
  // Run filter (search + no-thumbnail) then sort, both client-side
  // off the cached rows array. Avoids a server round-trip for every
  // keypress / sort / filter toggle.
  const visible = _bluesDbSortRows(_bluesDbFilterRows(_bluesDbState.rows));
  if (!visible.length) {
    list.innerHTML = '<div style="padding:0.6rem;color:var(--muted)">No artists match the current filter.</div>';
    bluesDbRenderPager(visible.length);
    return;
  }
  const esc = s => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
  const fmtRange = (a, b) => {
    if (a && b) return `${a} – ${b}`;
    if (a) return `b. ${a}`;
    if (b) return `d. ${b}`;
    return "";
  };
  const rows = visible.map(r => {
    const styles = (Array.isArray(r.styles) ? r.styles : []).slice(0, 3).join(", ");
    const relCount = Array.isArray(r.discogs_releases) ? r.discogs_releases.length : 0;
    // Cross-links to the main app:
    //   ⌕ → artist-specific search, type=Masters+, sorted year asc so
    //       the artist's earliest releases land first.
    //   W → wiki popup (search-first) with the name double-quoted.
    const searchHref = "/?a=" + encodeURIComponent(r.name)
                     + "&r="  + encodeURIComponent("master+")
                     + "&s="  + encodeURIComponent("year:asc");
    const wikiHref   = "/?wk=" + encodeURIComponent('"' + r.name + '"');
    // Discogs's site search, scoped to artists — fastest way to find
    // the canonical artist ID to drop into the Discogs ID field above.
    const discogsHref = "https://www.discogs.com/search/?type=artist&q=" + encodeURIComponent(r.name);
    // Photo thumb cell — small avatar that pops a full-size lightbox on
    // click. Empty placeholder square keeps the column aligned for rows
    // with no photo yet.
    const thumbCell = r.photo_url
      ? `<img src="${esc(r.photo_url)}" alt="" class="blues-row-thumb" loading="lazy" decoding="async" onclick="bluesShowImage('${esc(r.photo_url).replace(/'/g, "&#39;")}')" />`
      : `<span class="blues-row-thumb blues-row-thumb-empty"></span>`;
    return `<tr data-row-id="${r.id}">
      <td>${thumbCell}</td>
      <td>
        <a href="#" class="blues-row-name" data-hover-type="name" data-row-id="${r.id}" onclick="event.preventDefault();bluesDbOpenEditor(${r.id})">${esc(r.name)}</a>
        <a href="${esc(searchHref)}" target="_blank" rel="noopener" title="Search SeaDisco" class="blues-row-icon blues-row-search">⌕</a>
        <a href="${esc(discogsHref)}" target="_blank" rel="noopener" title="Search Discogs for this artist (use to find the canonical Discogs ID)" class="blues-row-icon blues-row-discogs">D</a>
        <a href="${esc(wikiHref)}" target="_blank" rel="noopener" title="Wikipedia popup" class="blues-row-icon blues-row-wiki">W</a>
        <a href="#" class="blues-row-icon" title="Merge this artist into another (moves lyrics + releases, deletes this row)" onclick="event.preventDefault();bluesDbMergePicker(${r.id}, '${esc(r.name).replace(/'/g, "&#39;")}')">⇄</a>
      </td>
      <td style="text-align:right" class="blues-hover-cell" data-hover-type="first-rel" data-row-id="${r.id}">${r.earliest_release_year ?? ""}</td>
      <td>${esc(styles)}</td>
      <td>${r.discogs_id ? esc(r.discogs_id) : ""}</td>
      <td style="text-align:right" class="blues-hover-cell" data-hover-type="releases" data-row-id="${r.id}">${relCount || ""}</td>
    </tr>`;
  }).join("");
  // Build clickable column headers. Sort key matches the server's
  // _BLUES_SORT_COLUMNS whitelist.
  const _bluesCols = [
    { key: null,               label: "" },               // image column — non-sortable
    { key: "name",             label: "Name" },
    { key: "earliest_release", label: "1st rel", align: "right", title: "Earliest year — first_recording_year or min(discogs_releases.year). Hover for title." },
    { key: "styles",           label: "Styles" },
    { key: "discogs_id",       label: "Discogs" },
    { key: "release_count",    label: "#",       align: "right", title: "Discogs masters/releases — hover for full list" },
  ];
  const headerCells = _bluesCols.map(c => {
    const sortable = !!c.key;
    const active = sortable && _bluesDbState.sort === c.key;
    const arrow = active ? (_bluesDbState.order === "asc" ? " ▲" : " ▼") : "";
    const align = c.align === "right" ? "text-align:right;" : "";
    const title = c.title ? ` title="${c.title}"` : "";
    const cursor = sortable ? "cursor:pointer;" : "";
    const onclick = sortable ? ` onclick="bluesDbToggleSort('${c.key}')"` : "";
    const colour = active ? "color:var(--accent);" : "";
    return `<th style="padding:0.35rem 0.5rem;${cursor}user-select:none;${align}${colour}"${onclick}${title}>${c.label}${arrow}</th>`;
  }).join("");
  list.innerHTML = `
    <table style="width:100%;border-collapse:collapse;font-size:0.82rem">
      <thead>
        <tr style="text-align:left;color:var(--muted);border-bottom:1px solid var(--border)">
          ${headerCells}
        </tr>
      </thead>
      <tbody style="color:var(--text)">${rows}</tbody>
    </table>`;
  list.querySelectorAll("tbody tr").forEach(tr => {
    tr.style.borderBottom = "1px solid var(--border)";
    tr.querySelectorAll("td").forEach(td => td.style.padding = "0.35rem 0.5rem");
  });
  // Wire hover popups on cells flagged with data-hover-type. Hide is
  // debounced so the cursor can travel from the trigger cell into the
  // popup (to scroll long bios / release lists) without flicker. The
  // popup itself cancels the hide on mouseenter and reschedules it on
  // mouseleave (wired below in _bluesEnsureHoverPopup).
  list.querySelectorAll("[data-hover-type]").forEach(el => {
    el.addEventListener("mouseenter", () => { _bluesHoverCancelHide(); _bluesHoverShow(el); });
    el.addEventListener("mouseleave", _bluesHoverScheduleHide);
  });
  // Pager shows the filtered vs total count so the user can see how
  // many rows the active search / "no thumbnail" toggle is hiding.
  bluesDbRenderPager(visible.length);
}

let _bluesHoverHideTimer = null;
function _bluesHoverScheduleHide() {
  if (_bluesHoverHideTimer) clearTimeout(_bluesHoverHideTimer);
  _bluesHoverHideTimer = setTimeout(_bluesHoverHide, 140);
}
function _bluesHoverCancelHide() {
  if (_bluesHoverHideTimer) { clearTimeout(_bluesHoverHideTimer); _bluesHoverHideTimer = null; }
}

// Build the popup HTML for whichever cell is being hovered. Pulls
// data straight from _bluesDbState.rows so we don't have to embed
// big strings in DOM attributes.
function _bluesHoverShow(target) {
  const id = parseInt(target.dataset.rowId, 10);
  const row = _bluesDbState.rows.find(r => r.id === id);
  if (!row) return;
  const esc = s => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
  const type = target.dataset.hoverType;
  let html = "";
  // URL builders for cross-links into the main app (open in new tabs).
  // Artist search lands on Masters+, sorted year asc — same shape as the
  // ⌕ link in the row. Release link uses ?op=<type>:<id> which the home
  // page's URL-restore IIFE auto-pops as a modal. Combining the two
  // (?a=…&op=master:…) opens the modal AND prefills the search behind
  // it so closing the modal leaves you on that artist's discography.
  const artistSearchHref = (name) =>
    "/?a=" + encodeURIComponent(name) + "&r=" + encodeURIComponent("master+") + "&s=" + encodeURIComponent("year:asc");
  const releaseModalHref = (rel, withArtistSearch) => {
    if (!rel?.id) return "";
    const t = (rel.type === "release") ? "release" : "master";
    const op = "op=" + t + ":" + encodeURIComponent(String(rel.id));
    if (withArtistSearch) {
      return "/?a=" + encodeURIComponent(row.name)
           + "&r=" + encodeURIComponent("master+")
           + "&s=" + encodeURIComponent("year:asc")
           + "&" + op;
    }
    return "/?" + op;
  };

  if (type === "name") {
    const aliases = (Array.isArray(row.aliases) ? row.aliases : []).filter(Boolean);
    const collabs = (Array.isArray(row.collaborators) ? row.collaborators : [])
      .map(c => typeof c === "string" ? c : c?.name)
      .filter(Boolean);
    const notes = (row.notes ?? "").trim();
    // Each alias / collaborator becomes a clickable link that opens a
    // SeaDisco artist search in a new tab.
    const linkified = (names) => names.map(n =>
      `<a href="${esc(artistSearchHref(n))}" target="_blank" rel="noopener" class="blues-hover-link">${esc(n)}</a>`
    ).join(", ");
    const sec = (label, items) => items.length
      ? `<div class="blues-hover-section"><span class="blues-hover-label">${label}</span> ${linkified(items)}</div>`
      : "";
    html =
      sec("Aliases:",       aliases) +
      sec("Collaborators:", collabs);
    if (notes) html += `<div class="blues-hover-section blues-hover-notes">${esc(notes)}</div>`;
    if (!html) html = `<div class="blues-hover-section blues-hover-empty">(no aliases / collaborators / notes yet)</div>`;
  } else if (type === "first-rel") {
    // Find the title + master/release for the earliest year. Prefer
    // first_recording_title when its year matches; otherwise fall back
    // to the discogs_releases entry with that year so we have an id.
    const earliest = row.earliest_release_year;
    let title = null;
    let earliestRel = null;
    if (earliest != null && Array.isArray(row.discogs_releases)) {
      earliestRel = row.discogs_releases.find(rel => rel?.year === earliest) ?? null;
    }
    if (row.first_recording_year === earliest && row.first_recording_title) {
      title = row.first_recording_title;
    } else if (earliestRel) {
      title = earliestRel.title ?? null;
    }
    if (title && earliestRel?.id) {
      html = `<div class="blues-hover-section"><a href="${esc(releaseModalHref(earliestRel, false))}" target="_blank" rel="noopener" class="blues-hover-link">${esc(title)}</a></div>`;
    } else {
      html = `<div class="blues-hover-section">${title ? esc(title) : "(no title recorded)"}</div>`;
    }
  } else if (type === "releases") {
    const list = (Array.isArray(row.discogs_releases) ? row.discogs_releases : [])
      .slice()
      .sort((a, b) => (a?.year ?? 9999) - (b?.year ?? 9999));
    if (!list.length) {
      html = `<div class="blues-hover-section blues-hover-empty">(no Discogs releases stored)</div>`;
    } else {
      // Every entry links to /?a=<artist>&op=<type>:<id> — opens the
      // master/release modal AND seeds an artist search behind it, so
      // closing the modal leaves the user on this artist's catalogue.
      html = `<div class="blues-hover-section"><ul class="blues-hover-list">` +
        list.map(rel => {
          const labelText = `<span class="blues-hover-year">${esc(rel?.year ?? "?")}</span> ${esc(rel?.title ?? "")}`;
          if (!rel?.id) return `<li>${labelText}</li>`;
          return `<li><a href="${esc(releaseModalHref(rel, true))}" target="_blank" rel="noopener" class="blues-hover-link">${labelText}</a></li>`;
        }).join("") +
        `</ul></div>`;
    }
  }
  let popup = document.getElementById("blues-hover-popup");
  if (!popup) {
    popup = document.createElement("div");
    popup.id = "blues-hover-popup";
    document.body.appendChild(popup);
    // Keep popup alive while cursor is over it (so scrollable bios /
    // release lists are usable); reschedule the hide on leave.
    popup.addEventListener("mouseenter", _bluesHoverCancelHide);
    popup.addEventListener("mouseleave", _bluesHoverScheduleHide);
  }
  popup.innerHTML = html;
  popup.style.display = "block";
  // Position below the trigger; clamp inside the viewport horizontally
  // so long bio popups don't get clipped on the right.
  const rect = target.getBoundingClientRect();
  let top = rect.bottom + window.scrollY + 4;
  let left = rect.left + window.scrollX;
  popup.style.top = top + "px";
  popup.style.left = left + "px";
  // After paint, nudge inward if we ran off the viewport.
  requestAnimationFrame(() => {
    const pr = popup.getBoundingClientRect();
    const overflowRight = pr.right - window.innerWidth + 12;
    if (overflowRight > 0) popup.style.left = (left - overflowRight) + "px";
  });
}
function _bluesHoverHide() {
  const popup = document.getElementById("blues-hover-popup");
  if (popup) popup.style.display = "none";
}

// Click-to-enlarge for the row thumbnails — quick lightbox, click
// anywhere on the backdrop to dismiss.
function bluesShowImage(url) {
  if (!url) return;
  const overlay = document.createElement("div");
  overlay.className = "blues-image-overlay";
  overlay.onclick = () => overlay.remove();
  const img = document.createElement("img");
  img.src = url;
  img.alt = "";
  overlay.appendChild(img);
  document.body.appendChild(overlay);
  // Esc dismiss
  const onEsc = (e) => { if (e.key === "Escape") { overlay.remove(); document.removeEventListener("keydown", onEsc); } };
  document.addEventListener("keydown", onEsc);
}

// Pager is gone — admin grid shows everything in one shot. Keep this
// shim for any stale callers but it just renders the row count.
function bluesDbRenderPager(visibleCount) {
  const pag = document.getElementById("blues-pager");
  if (!pag) return;
  const total = _bluesDbState.total ?? _bluesDbState.rows.length;
  const v = (typeof visibleCount === "number") ? visibleCount : total;
  const label = (v === total)
    ? `${total} rows`
    : `${v.toLocaleString()} of ${total.toLocaleString()} rows`;
  pag.innerHTML = `<span style="color:var(--muted)">${label}</span>`;
}

// Click a column header → if it's already the active sort, flip
// asc↔desc; otherwise switch to that column with a sensible default
// direction (numeric/date columns default to descending so the most
// recent / largest values surface first).
function bluesDbToggleSort(key) {
  if (_bluesDbState.sort === key) {
    _bluesDbState.order = _bluesDbState.order === "asc" ? "desc" : "asc";
  } else {
    _bluesDbState.sort = key;
    const numericish = ["birth_date","death_date","discogs_id","release_count","date_added","updated_at"];
    _bluesDbState.order = numericish.includes(key) ? "desc" : "asc";
  }
  _bluesDbState.offset = 0;
  // Sort happens client-side over the cached rows — no server round-
  // trip. The full list is already in _bluesDbState.rows from the
  // initial load. Server is only re-hit when something genuinely
  // mutates the row set (bulk enrich completion, etc).
  bluesDbRenderList();
}

async function bluesDbExportCsv() {
  // Use apiFetch so the Bearer token comes along, then trigger a blob
  // download client-side — direct anchor with `?token=…` would leak the
  // session token into browser history.
  // Button null-safe: the Discovery view has a different button id, so
  // we just no-op the loading-state UI when it's missing.
  const btn = document.getElementById("blues-export-btn");
  const orig = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "Exporting…"; }
  try {
    const r = await apiFetch("/api/admin/blues/export.csv?sort=" + encodeURIComponent(_bluesDbState.sort) + "&order=" + encodeURIComponent(_bluesDbState.order));
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Export failed: " + (err.error ?? r.status));
      return;
    }
    const blob = await r.blob();
    const fname = `seadisco-blues-${new Date().toISOString().slice(0,10)}.csv`;
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = fname;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 5000);
  } catch (e) { alert("Export failed: " + e); }
  finally { if (btn) { btn.disabled = false; btn.textContent = orig; } }
}

async function bluesDbDeleteAll() {
  // Two-step confirm: simple OK/Cancel first, then a typed token so a
  // stray button click can't wipe the table. Backend ALSO requires the
  // token in the request, so even a bypassed confirm() can't nuke it.
  if (!confirm("Delete EVERY row in the blues_artists table?\n\nThis is irreversible. You'll need to reseed afterwards.")) return;
  const typed = prompt("Type DELETE ALL (in caps) to confirm.");
  if (typed !== "DELETE ALL") { alert("Cancelled — token didn't match."); return; }
  try {
    const r = await apiFetch("/api/admin/blues/all?confirm=DELETE%20ALL", { method: "DELETE" });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Delete failed: " + (err.error ?? r.status));
      return;
    }
    const out = await r.json();
    alert(`Deleted ${out.deleted} rows.`);
    _bluesDbState.offset = 0;
    bluesDbLoadStats();
    bluesDbLoadList();
  } catch (e) { alert("Delete failed: " + e); }
}
function bluesDbDebouncedSearch() {
  // Search filters the cached rows in place — no server round-trip.
  // Tiny debounce so each keystroke doesn't trigger a full re-render
  // mid-typing on multi-thousand-row datasets.
  clearTimeout(_bluesSearchTimer);
  _bluesSearchTimer = setTimeout(() => {
    _bluesDbState.search = document.getElementById("blues-search").value.trim();
    _bluesDbState.offset = 0;
    bluesDbRenderList();
  }, 80);
}

async function bluesDbSeed() {
  const btn = document.getElementById("blues-seed-btn");
  if (!confirm("Pull the seed list from Wikidata?\n\nIdempotent — re-running upserts on QID and won't overwrite manual edits to other fields.")) return;
  btn.disabled = true; btn.textContent = "Seeding…";
  try {
    const r = await apiFetch("/api/admin/blues/seed", { method: "POST" });
    if (!r.ok) { alert("Seed failed: HTTP " + r.status); return; }
    const out = await r.json();
    alert(`Seeded ${out.upserted} of ${out.fetched} artists in ${(out.durationMs/1000).toFixed(1)}s${out.errors?.length ? ` (${out.errors.length} errors)` : ""}.`);
    bluesDbLoadStats();
    bluesDbLoadList();
  } catch (e) { alert("Seed failed: " + e); }
  finally { btn.disabled = false; btn.textContent = "Seed from Wikidata"; }
}

// Discogs seed runs as a background job — kick it off, return 202 right
// away, then poll status every 5s so we can show progress and pick up
// the final summary without holding an HTTP request open (Railway's
// edge proxy 502s connections after ~5 min).
let _bluesSeedPollTimer = null;

async function bluesDbSeedDiscogs() {
  const btn = document.getElementById("blues-seed-discogs-btn");
  if (!confirm("Walk Discogs blues releases 1900-1930?\n\nLong pass — ~10 minutes at 1 req/sec. Uses your admin Discogs token. Runs in the background; you can leave this page and come back. Idempotent: existing rows get new releases merged in.")) return;
  btn.disabled = true; btn.textContent = "Starting…";
  try {
    const r = await apiFetch("/api/admin/blues/seed-discogs", { method: "POST" });
    if (r.status === 409) {
      const j = await r.json().catch(() => ({}));
      alert("A Discogs seed is already running (started " + (j.startedAt ?? "earlier") + "). Watching progress.");
    } else if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Could not start seed: " + (err.error ?? r.status));
      btn.disabled = false; btn.textContent = "Seed from Discogs";
      return;
    }
  } catch (e) {
    alert("Could not start seed: " + e);
    btn.disabled = false; btn.textContent = "Seed from Discogs";
    return;
  }
  _bluesPollSeedStatus();
}

function _bluesPollSeedStatus() {
  const btn = document.getElementById("blues-seed-discogs-btn");
  if (_bluesSeedPollTimer) { clearInterval(_bluesSeedPollTimer); _bluesSeedPollTimer = null; }
  const tick = async () => {
    try {
      const r = await apiFetch("/api/admin/blues/seed-discogs/status");
      if (!r.ok) return;
      const job = await r.json();
      if (job.status === "running") {
        const p = job.progress;
        if (p?.phase === "scanning") {
          btn.textContent = `Scanning ${p.yearsScannedSoFar}/${p.yearsTotal}y · ${p.uniqueArtistsSoFar} artists`;
        } else if (p?.phase === "upserting") {
          btn.textContent = `Upserting ${p.upsertsDone}/${p.upsertsTotal}`;
        } else {
          btn.textContent = "Seeding…";
        }
        return;
      }
      // Terminal state — stop polling, refresh list, summarise.
      clearInterval(_bluesSeedPollTimer); _bluesSeedPollTimer = null;
      btn.disabled = false; btn.textContent = "Seed from Discogs";
      bluesDbLoadStats();
      bluesDbLoadList();
      if (job.status === "done" && job.result) {
        const o = job.result;
        alert(`Discogs seed done in ${(o.durationMs/60000).toFixed(1)} min:\n` +
          `· scanned ${o.rowsScanned} release rows across ${o.yearsScanned?.length || 0} years\n` +
          `· ${o.uniqueArtists} unique artists found\n` +
          `· ${o.artistsCreated} new rows created · ${o.artistsMerged} existing rows merged\n` +
          `· ${o.releasesAdded} releases added` +
          (o.errors?.length ? `\n· ${o.errors.length} errors` : ""));
      } else if (job.status === "error") {
        alert("Discogs seed errored: " + (job.error ?? "unknown"));
      }
    } catch { /* network blip — next tick will retry */ }
  };
  tick();
  _bluesSeedPollTimer = setInterval(tick, 5000);
}

// Hook into any in-flight seed when the tab loads — so the button shows
// progress even if the admin reloaded the page mid-seed.
async function _bluesResumeSeedPollIfRunning() {
  try {
    const r = await apiFetch("/api/admin/blues/seed-discogs/status");
    if (!r.ok) return;
    const job = await r.json();
    if (job.status === "running") {
      const btn = document.getElementById("blues-seed-discogs-btn");
      if (btn) btn.disabled = true;
      _bluesPollSeedStatus();
    }
  } catch {}
}

// MusicBrainz enrichment.
//   id = N    → single row, runs synchronously (~3-5s).
//   id = null → bulk, fires a background job and polls status every 5s
//               (avoids the 502 timeout we'd otherwise hit at ~5 min).
let _bluesMbPollTimer = null;

async function bluesDbEnrichMb(id) {
  if (id != null) return _bluesEnrichMbSingleRow(id);
  return _bluesEnrichMbBulk();
}

async function _bluesEnrichMbSingleRow(id) {
  const btn = document.getElementById("blues-editor-mb");
  const orig = btn.textContent;
  btn.disabled = true; btn.textContent = "Enriching…";
  try {
    const r = await apiFetch("/api/admin/blues/enrich-mb?id=" + id, { method: "POST" });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Enrich failed: " + (err.error ?? r.status));
      return;
    }
    const out = await r.json();
    const summary = `${out.enriched}/${out.attempted} enriched in ${(out.durationMs/1000).toFixed(1)}s` +
      (out.skipped ? ` · ${out.skipped} skipped (no MBID)` : "") +
      (out.errors?.length ? ` · ${out.errors.length} errors` : "");
    // Patch the cache from the fresh row server returned, then re-
    // render the list and refresh the editor. No full re-list fetch.
    await _bluesDbRefreshRow(id);
    bluesDbOpenEditor(id);
    bluesDbRenderList();
    alert(summary);
  } catch (e) { alert("Enrich failed: " + e); }
  finally { btn.disabled = false; btn.textContent = orig; }
}

async function _bluesEnrichMbBulk() {
  const btn = document.getElementById("blues-enrich-mb-btn");
  if (!confirm("Pull first/last recording year + title from MusicBrainz for every row?\n\nRate-limited 1 req/s — bulk pass takes ~7-10 min. Runs in the background; you can leave this page and come back. Idempotent.")) return;
  btn.disabled = true; btn.textContent = "Starting…";
  try {
    const r = await apiFetch("/api/admin/blues/enrich-mb", { method: "POST" });
    if (r.status === 409) {
      const j = await r.json().catch(() => ({}));
      alert("MB enrichment already running (started " + (j.startedAt ?? "earlier") + "). Watching progress.");
    } else if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Could not start: " + (err.error ?? r.status));
      btn.disabled = false; btn.textContent = "Enrich from MB";
      return;
    }
  } catch (e) {
    alert("Could not start: " + e);
    btn.disabled = false; btn.textContent = "Enrich from MB";
    return;
  }
  _bluesPollMbStatus();
}

function _bluesPollMbStatus() {
  const btn = document.getElementById("blues-enrich-mb-btn");
  if (_bluesMbPollTimer) { clearInterval(_bluesMbPollTimer); _bluesMbPollTimer = null; }
  const tick = async () => {
    try {
      const r = await apiFetch("/api/admin/blues/enrich-mb/status");
      if (!r.ok) return;
      const job = await r.json();
      if (job.status === "running") {
        const p = job.progress;
        if (p) {
          btn.textContent = `MB: ${p.processed}/${p.total} · ${p.enriched} hit · ${p.skipped} skip` + (p.errors ? ` · ${p.errors} err` : "");
        } else {
          btn.textContent = "Enriching…";
        }
        return;
      }
      clearInterval(_bluesMbPollTimer); _bluesMbPollTimer = null;
      btn.disabled = false; btn.textContent = "Enrich from MB";
      bluesDbLoadStats();
      bluesDbLoadList();
      if (job.status === "done" && job.result) {
        const o = job.result;
        alert(`MB enrichment done in ${(o.durationMs/60000).toFixed(1)} min:\n` +
          `· ${o.enriched}/${o.attempted} enriched\n` +
          `· ${o.skipped} skipped (no confident match)\n` +
          (o.errors?.length ? `· ${o.errors.length} errors` : "· no errors"));
      } else if (job.status === "error") {
        alert("MB enrichment errored: " + (job.error ?? "unknown"));
      }
    } catch { /* network blip — next tick */ }
  };
  tick();
  _bluesMbPollTimer = setInterval(tick, 5000);
}

async function _bluesResumeMbPollIfRunning() {
  try {
    const r = await apiFetch("/api/admin/blues/enrich-mb/status");
    if (!r.ok) return;
    const job = await r.json();
    if (job.status === "running") {
      const btn = document.getElementById("blues-enrich-mb-btn");
      if (btn) btn.disabled = true;
      _bluesPollMbStatus();
    }
  } catch {}
}

function bluesDbEnrichEditorRow() {
  const id = _bluesDbState.editingId;
  if (!id) return;
  bluesDbEnrichMb(id);
}

// Generic helper for the three remaining enrichers — same shape as
// bluesDbEnrichMb but parameterised so we don't repeat ourselves four
// times. label + slow-warning + endpoint differ; everything else is
// identical (button disable + reload + summary alert).
async function _bluesEnrichGeneric({ id, endpoint, btnId, runningLabel, confirmMsg, urlExtras, idAsPath }) {
  const btn = document.getElementById(btnId);
  if (id == null && confirmMsg && !confirm(confirmMsg)) return;
  const orig = btn.textContent;
  btn.disabled = true;
  btn.textContent = runningLabel;
  try {
    // YouTube wants id as a path segment (/:id); MB/Wiki/Discogs use ?id=N.
    let url = endpoint;
    if (id != null && idAsPath) url += "/" + id;
    url += (urlExtras ?? "");
    if (id != null && !idAsPath) {
      url += (url.includes("?") ? "&" : "?") + "id=" + encodeURIComponent(String(id));
    }
    const r = await apiFetch(url, { method: "POST" });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Failed: " + (err.error ?? r.status));
      return;
    }
    const out = await r.json();
    const summary = out.attempted != null
      ? `${out.enriched}/${out.attempted} enriched in ${(out.durationMs/1000).toFixed(1)}s` +
        (out.skipped ? ` · ${out.skipped} skipped` : "") +
        (out.errors?.length ? ` · ${out.errors.length} errors` : "")
      : (Array.isArray(out.added) ? `Added ${out.added.length} URL(s).` : JSON.stringify(out));
    // Single-row enrich: patch the cache and re-render rather than
    // reloading the entire list. Bulk runs (id == null) still need a
    // full re-list because they mutate many rows server-side.
    if (id != null) {
      await _bluesDbRefreshRow(id);
      if (_bluesDbState.editingId === id) bluesDbOpenEditor(id);
      bluesDbLoadStats();
      bluesDbRenderList();
      // Discovery-side refresh: when the editor was launched from the
      // Blues Archive artist profile popup, the profile itself
      // (#ba-artist-overlay) is what the user sees behind the editor.
      // It was rendered with pre-enrich data, so without this it
      // shows STALE bio / photo / releases until the user re-opens
      // the artist. Re-open it now so closing the editor reveals the
      // fresh data the enrich just wrote.
      if (typeof window._baOpenArtist === "function" && window._baCurrentArtistId === id) {
        try { window._baOpenArtist(id); } catch {}
      }
    } else {
      bluesDbLoadStats();
      bluesDbLoadList();
    }
    alert(summary);
  } catch (e) { alert("Failed: " + e); }
  finally { btn.disabled = false; btn.textContent = orig; }
}

// Generic bulk runner — kicks off a background job and polls /status
// every 5s. Same shape for Wiki + Discogs ID enrichers.
function _bluesEnrichBulkPoll({ btnId, statusEndpoint, label }) {
  const timerKey = "_bluesPollTimer_" + btnId;
  if (window[timerKey]) { clearInterval(window[timerKey]); window[timerKey] = null; }
  const btn = document.getElementById(btnId);
  const tick = async () => {
    try {
      const r = await apiFetch(statusEndpoint);
      if (!r.ok) return;
      const job = await r.json();
      if (job.status === "running") {
        const p = job.progress;
        if (p) btn.textContent = `${label}: ${p.processed}/${p.total} · ${p.enriched} hit · ${p.skipped} skip` + (p.errors ? ` · ${p.errors} err` : "");
        else btn.textContent = label + "…";
        // Mid-run: server now exposes the last 20 errors via
        // job.progress.recentErrors so the curator can hover-peek
        // without waiting for the job to finish.
        const recent = job?.progress?.recentErrors;
        if (Array.isArray(recent) && recent.length) {
          const sample = recent.slice(-5).map(e => `${e.name || `#${e.id}`}: ${(e.message || "").slice(0, 80)}`).join("\n");
          btn.title = `Latest errors (click 'Stop' on /admin if these look systemic):\n${sample}`;
        } else { btn.title = ""; }
        return;
      }
      clearInterval(window[timerKey]); window[timerKey] = null;
      btn.disabled = false; btn.textContent = label;
      bluesDbLoadStats(); bluesDbLoadList();
      if (job.status === "done" && job.result) {
        const o = job.result;
        // When there are errors, show a proper details overlay so the
        // curator can see WHICH artists failed and WHY — the prior
        // 'N errors' alert was a dead end.
        if (o.errors?.length) {
          _bluesShowEnrichErrors(label, o);
        } else {
          alert(`${label} done in ${(o.durationMs/60000).toFixed(1)} min:\n` +
            `· ${o.enriched}/${o.attempted} enriched\n` +
            `· ${o.skipped} skipped\n` +
            `· no errors`);
        }
      } else if (job.status === "error") {
        alert(`${label} errored: ` + (job.error ?? "unknown"));
      }
    } catch { /* network blip */ }
  };
  tick();
  window[timerKey] = setInterval(tick, 5000);
}
// Show a small modal with a summary + the per-row error list when
// a bulk enrichment finishes. Group identical messages so a Discogs
// 429-storm doesn't fill the list with 100 identical lines. Each row
// is clickable to open that artist's editor for direct curation.
function _bluesShowEnrichErrors(label, result) {
  document.getElementById("blues-enrich-errors-overlay")?.remove();
  const overlay = document.createElement("div");
  overlay.id = "blues-enrich-errors-overlay";
  Object.assign(overlay.style, {
    position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
    zIndex: "330", display: "flex", alignItems: "flex-start",
    justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
  });
  overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
  // Group errors by message so a Discogs 429-storm collapses to one
  // line with a count, and the rare unique failures are easy to spot.
  const groups = new Map();
  for (const e of result.errors) {
    const k = e?.message || "(no message)";
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(e);
  }
  const groupRows = [...groups.entries()]
    .sort((a, b) => b[1].length - a[1].length)
    .map(([msg, list]) => {
      const sample = list.slice(0, 50).map(e => {
        const name  = (e.name || `id ${e.id}`).replace(/</g, "&lt;");
        const did   = e.discogs_id ? ` <span style="color:#888">#${e.discogs_id}</span>` : "";
        const editAttr = e.id ? `onclick="document.getElementById('blues-enrich-errors-overlay')?.remove();_baOpenFullEditor?.(${Number(e.id)})"` : "";
        return `<li style="margin:0.15rem 0"><a href="#" ${editAttr} style="color:var(--accent);text-decoration:none">${name}${did}</a></li>`;
      }).join("");
      const more = list.length > 50 ? `<li style="color:#888">…and ${list.length - 50} more</li>` : "";
      return `<details style="margin:0.4rem 0;border:1px solid var(--border);border-radius:4px;padding:0.4rem 0.6rem">
        <summary style="cursor:pointer;color:#e88">
          <strong>${list.length}×</strong>
          <code style="color:var(--text);background:rgba(255,255,255,0.05);padding:0.05rem 0.3rem;border-radius:3px">${(msg || "").replace(/</g, "&lt;").slice(0, 180)}</code>
        </summary>
        <ul style="margin:0.4rem 0 0 1rem;padding:0;list-style:disc;font-size:0.82rem">${sample}${more}</ul>
      </details>`;
    }).join("");
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1rem 1.2rem;width:min(820px,100%);max-height:85vh;overflow:auto">
      <div style="display:flex;align-items:center;justify-content:space-between;gap:0.6rem;margin-bottom:0.6rem">
        <h3 style="margin:0;font-size:1rem">${label.replace(/</g,'&lt;')} — finished with errors</h3>
        <button class="archive-btn" onclick="document.getElementById('blues-enrich-errors-overlay')?.remove()">Close</button>
      </div>
      <div style="font-size:0.82rem;color:var(--muted);margin-bottom:0.6rem">
        ${result.enriched}/${result.attempted} enriched · ${result.skipped} skipped · <span style="color:#e88">${result.errors.length} errors</span> · ${(result.durationMs/60000).toFixed(1)} min
      </div>
      <p style="font-size:0.78rem;color:var(--muted);margin:0 0 0.6rem">
        Errors grouped by message. Expand a group to see the failing artists; click an artist name to open its editor.
      </p>
      ${groupRows}
    </div>
  `;
  document.body.appendChild(overlay);
}
window._bluesShowEnrichErrors = _bluesShowEnrichErrors;

async function _bluesEnrichBulkStart({ endpoint, statusEndpoint, btnId, label, confirmMsg, urlExtras }) {
  if (confirmMsg && !confirm(confirmMsg)) return;
  const btn = document.getElementById(btnId);
  btn.disabled = true; btn.textContent = "Starting…";
  try {
    const r = await apiFetch(endpoint + (urlExtras ?? ""), { method: "POST" });
    if (r.status === 409) {
      const j = await r.json().catch(() => ({}));
      alert(`${label} already running (started ` + (j.startedAt ?? "earlier") + "). Watching progress.");
    } else if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Could not start: " + (err.error ?? r.status));
      btn.disabled = false; btn.textContent = label;
      return;
    }
  } catch (e) {
    alert("Could not start: " + e);
    btn.disabled = false; btn.textContent = label;
    return;
  }
  _bluesEnrichBulkPoll({ btnId, statusEndpoint, label });
}

function bluesDbEnrichWiki(id) {
  if (id != null) {
    return _bluesEnrichGeneric({
      id, endpoint: "/api/admin/blues/enrich-wiki",
      btnId: "blues-editor-wiki", runningLabel: "Fetching…",
      urlExtras: "?force=1",
    });
  }
  return _bluesEnrichBulkStart({
    endpoint: "/api/admin/blues/enrich-wiki",
    statusEndpoint: "/api/admin/blues/enrich-wiki/status",
    btnId: "blues-enrich-wiki-btn",
    label: "Enrich notes (Wiki)",
    confirmMsg: "Pull Wikipedia lead paragraphs into the notes field for every row?\n\nRuns in the background — you can leave this page. Skips rows that already have notes.",
  });
}
function bluesDbEnrichDiscogs(id) {
  if (id != null) {
    return _bluesEnrichGeneric({
      id, endpoint: "/api/admin/blues/enrich-discogs",
      btnId: "blues-editor-discogs", runningLabel: "Searching Discogs…",
    });
  }
  return _bluesEnrichBulkStart({
    endpoint: "/api/admin/blues/enrich-discogs",
    statusEndpoint: "/api/admin/blues/enrich-discogs/status",
    btnId: "blues-enrich-discogs-btn",
    label: "Enrich Discogs",
    confirmMsg: "Look up Discogs IDs by name for every row missing one?\n\nRuns in the background. Idempotent.",
  });
}

// One-stop "Get all info from Discogs" — pulls /artists/:id for every
// row in the DB with a discogs_id and stores profile (bio), aliases,
// realname, namevariations, members, groups, first image, and URLs.
// Mirrors the bulk-job + status-poll pattern of the others.
function bluesDbEnrichDiscogsFull(id) {
  if (id != null) {
    return _bluesEnrichGeneric({
      id, endpoint: "/api/admin/blues/enrich-discogs-full",
      btnId: "blues-editor-discogs", runningLabel: "Loading Discogs…",
    });
  }
  return _bluesEnrichBulkStart({
    endpoint: "/api/admin/blues/enrich-discogs-full",
    statusEndpoint: "/api/admin/blues/enrich-discogs-full/status",
    btnId: "blues-enrich-discogs-full-btn",
    label: "Get all info from Discogs",
    confirmMsg: "Pull the full Discogs artist record (bio, aliases, realname, name variations, members, groups, photo, URLs) for every row?\n\nRuns in the background — leave the page if you like. Rate-limited 1 req/sec, ~3-5 min for 200 rows. Idempotent.",
  });
}
function bluesDbEnrichYt(id) {
  // YouTube is per-row only (quota cost) and wants id as a path segment.
  return _bluesEnrichGeneric({
    id, endpoint: "/api/admin/blues/enrich-yt",
    btnId: "blues-editor-yt", runningLabel: "Searching YouTube…",
    idAsPath: true,
  });
}
function bluesDbEnrichEditorWiki()    { const id = _bluesDbState.editingId; if (id) bluesDbEnrichWiki(id); }
function bluesDbEnrichEditorYt()      { const id = _bluesDbState.editingId; if (id) bluesDbEnrichYt(id); }

// Per-row preview from Discogs. PULLS the data, populates the open
// editor form, but does NOT write to the DB. The curator reviews
// (and tweaks) the fields, then hits Save to commit. force=1 so
// the preview includes overwrite-eligible fields (photo, notes,
// releases-replaced); non-force would only fill blanks.
async function bluesDbRefreshFromDiscogs() {
  const id = _bluesDbState.editingId;
  if (!id) return;
  const form = document.getElementById("blues-editor-form");
  if (!form) return;
  const btn = document.getElementById("blues-editor-discogs-refresh");
  const orig = btn?.textContent;
  if (btn) { btn.disabled = true; btn.textContent = "Loading Discogs…"; }
  try {
    const r = await apiFetch(`/api/admin/blues/${id}/discogs-preview?force=1`, { method: "POST" });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Refresh failed: " + (err.error ?? r.status));
      return;
    }
    const { patch = {} } = await r.json();
    // Apply the patch to the open form. Each field gets the value
    // Discogs returned; the curator can edit it inline before saving.
    // Arrays render as JSON in textareas (matches the editor's load
    // path) or comma-separated for simple-array inputs.
    let changed = 0;
    for (const [k, v] of Object.entries(patch)) {
      const el = form.elements.namedItem(k);
      if (!el) continue;
      if (Array.isArray(v)) {
        const isComplex = v.length > 0 && typeof v[0] === "object" && v[0] !== null;
        el.value = isComplex ? JSON.stringify(v, null, 2) : v.join(", ");
      } else if (v == null) {
        el.value = "";
      } else if (typeof v === "object") {
        el.value = JSON.stringify(v, null, 2);
      } else {
        el.value = String(v);
      }
      // Tint changed inputs briefly so the curator sees what moved.
      try {
        el.style.outline = "1px solid var(--accent)";
        setTimeout(() => { try { el.style.outline = ""; } catch {} }, 1800);
      } catch {}
      changed++;
    }
    if (!changed) {
      alert("Discogs returned no new info for this artist — nothing to preview.");
      return;
    }
    // Refresh the editor's external-links bar in case discogs_id changed.
    if (typeof _bluesDbUpdateEditorLinks === "function") _bluesDbUpdateEditorLinks();
    if (typeof showToast === "function") {
      showToast(`Discogs preview loaded — review and hit Save to commit (${changed} field${changed === 1 ? "" : "s"})`, "ok");
    }
  } catch (e) {
    alert("Refresh failed: " + (e?.message || e));
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = orig || "Refresh from Discogs"; }
  }
}
window.bluesDbRefreshFromDiscogs = bluesDbRefreshFromDiscogs;

// ── Discogs candidate picker ────────────────────────────────────────
// Opened from the editor's "Pick Discogs match" button. Shows the top
// Discogs artist hits for the row's name so the admin can pick the
// right one when several artists share a name. The chosen id is
// PUT onto the row via the existing /api/admin/blues/:id endpoint.
function bluesDbOpenDiscogsPicker() {
  const id = _bluesDbState.editingId;
  if (!id) return;
  // Default the search box to whatever's currently in the name field.
  const form = document.getElementById("blues-editor-form");
  const nameVal = form?.elements?.namedItem("name")?.value?.trim() || "";
  const overlay = document.getElementById("blues-discogs-picker");
  const qInput = document.getElementById("blues-discogs-picker-q");
  const status = document.getElementById("blues-discogs-picker-status");
  const results = document.getElementById("blues-discogs-picker-results");
  if (!overlay || !qInput || !results) return;
  qInput.value = nameVal;
  if (status) status.textContent = "";
  results.innerHTML = nameVal
    ? `<div style="color:#777">Click Search to fetch candidates.</div>`
    : `<div style="color:#777">Type a name above and click Search.</div>`;
  overlay.style.display = "flex";
  setTimeout(() => qInput.focus(), 30);
  // Auto-fetch if we have a name to save the user a click.
  if (nameVal) bluesDbRunDiscogsPicker();
}
function bluesDbCloseDiscogsPicker() {
  const overlay = document.getElementById("blues-discogs-picker");
  if (overlay) overlay.style.display = "none";
}
async function bluesDbRunDiscogsPicker() {
  const qInput = document.getElementById("blues-discogs-picker-q");
  const status = document.getElementById("blues-discogs-picker-status");
  const results = document.getElementById("blues-discogs-picker-results");
  if (!qInput || !results) return;
  const q = qInput.value.trim();
  if (!q) { results.innerHTML = `<div style="color:#a55">Enter a name first.</div>`; return; }
  if (status) status.textContent = "Searching…";
  results.innerHTML = `<div style="color:#777">Loading…</div>`;
  try {
    const r = await apiFetch("/api/admin/blues/discogs-candidates?q=" + encodeURIComponent(q));
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      results.innerHTML = `<div style="color:#a55">${(err.error || "Search failed").replace(/</g,"&lt;")}</div>`;
      if (status) status.textContent = "";
      return;
    }
    const data = await r.json();
    const items = Array.isArray(data?.items) ? data.items : [];
    if (status) status.textContent = `${items.length} match${items.length === 1 ? "" : "es"}`;
    if (!items.length) {
      results.innerHTML = `<div style="color:#777">No matches on Discogs for "${q.replace(/</g,"&lt;")}".</div>`;
      return;
    }
    const esc = (s) => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/"/g,"&quot;");
    results.innerHTML = `<ul style="list-style:none;margin:0;padding:0">${items.map(it => {
      const thumbHtml = it.thumb
        ? `<img src="${esc(it.thumb)}" alt="" loading="lazy" decoding="async" style="width:48px;height:48px;object-fit:cover;border-radius:3px;flex-shrink:0;background:var(--border)" />`
        : `<span style="width:48px;height:48px;border-radius:3px;background:var(--border);flex-shrink:0;display:inline-block"></span>`;
      const discogsLink = it.uri
        ? `https://www.discogs.com${it.uri}`
        : `https://www.discogs.com/artist/${it.id}`;
      return `<li style="display:flex;align-items:center;gap:0.6rem;padding:0.4rem 0.3rem;border-bottom:1px solid rgba(255,255,255,0.06)">
        ${thumbHtml}
        <span style="flex:1;min-width:0">
          <a href="${esc(discogsLink)}" target="_blank" rel="noopener" style="color:#ddd;text-decoration:none;font-weight:600">${esc(it.title)}</a>
          <div style="font-size:0.7rem;color:#777">id ${it.id} · <a href="${esc(discogsLink)}" target="_blank" rel="noopener" style="color:#888;text-decoration:underline">view on Discogs</a></div>
        </span>
        <button type="button" class="admin-btn" onclick="bluesDbPickDiscogsCandidate(${it.id})">Pick</button>
      </li>`;
    }).join("")}</ul>`;
  } catch (e) {
    results.innerHTML = `<div style="color:#a55">Search error: ${String(e).replace(/</g,"&lt;")}</div>`;
    if (status) status.textContent = "";
  }
}
// ── Merge picker ─────────────────────────────────────────────────────
// Opened from the editor's "Merge into…" button. Lets the admin pick
// a target blues_artists row; on confirm, calls POST /api/blues-archive/merge
// which reassigns lyrics, dedupes discogs_releases, and deletes the
// source. Then closes the editor and refreshes the list.
let _bluesMergeDebounce = null;
function bluesDbOpenMergePicker() {
  const id = _bluesDbState.editingId;
  if (!id) return;
  const form = document.getElementById("blues-editor-form");
  const nameVal = form?.elements?.namedItem("name")?.value?.trim() || "";
  const overlay = document.getElementById("blues-merge-picker");
  const qInput = document.getElementById("blues-merge-picker-q");
  const status = document.getElementById("blues-merge-picker-status");
  const results = document.getElementById("blues-merge-picker-results");
  const srcLbl = document.getElementById("blues-merge-source-name");
  if (!overlay || !qInput || !results) return;
  if (srcLbl) srcLbl.textContent = nameVal || `#${id}`;
  // Seed the search box with the current name so the top hit is
  // usually the obvious dup. Admin can refine if not.
  qInput.value = nameVal;
  if (status) status.textContent = "";
  results.innerHTML = `<div style="color:#777">Loading candidates…</div>`;
  overlay.style.display = "flex";
  setTimeout(() => qInput.focus(), 30);
  bluesDbRunMergePicker();
}
function bluesDbCloseMergePicker() {
  const overlay = document.getElementById("blues-merge-picker");
  if (overlay) overlay.style.display = "none";
}
function bluesDbRunMergePickerDebounced() {
  if (_bluesMergeDebounce) clearTimeout(_bluesMergeDebounce);
  _bluesMergeDebounce = setTimeout(bluesDbRunMergePicker, 250);
}
async function bluesDbRunMergePicker() {
  const sourceId = _bluesDbState.editingId;
  if (!sourceId) return;
  const qInput = document.getElementById("blues-merge-picker-q");
  const status = document.getElementById("blues-merge-picker-status");
  const results = document.getElementById("blues-merge-picker-results");
  if (!qInput || !results) return;
  const q = qInput.value.trim();
  if (status) status.textContent = "Searching…";
  try {
    const r = await apiFetch(
      "/api/admin/blues/list?limit=25&search=" + encodeURIComponent(q),
    );
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      results.innerHTML = `<div style="color:#a55">${(err.error || "Search failed").replace(/</g,"&lt;")}</div>`;
      if (status) status.textContent = "";
      return;
    }
    const data = await r.json();
    // Filter out the source row itself — merging into yourself errors.
    const rows = (Array.isArray(data?.rows) ? data.rows : []).filter(
      (row) => Number(row.id) !== Number(sourceId),
    );
    if (status) status.textContent = `${rows.length} match${rows.length === 1 ? "" : "es"}`;
    if (!rows.length) {
      results.innerHTML = `<div style="color:#777">No other artists match "${q.replace(/</g,"&lt;")}".</div>`;
      return;
    }
    // Stash names in a sidecar map keyed by id. We can't safely embed
    // arbitrary names in the onclick="…" attribute — artist names like
    // Aaron "Pinetop" Sparks contain double quotes that close the attr
    // and silently swallow the click. Passing just the id keeps the
    // attribute boring; the click handler looks the name up here.
    window._bluesMergeNames = window._bluesMergeNames || {};
    const esc = (s) => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/"/g,"&quot;");
    results.innerHTML = `<ul style="list-style:none;margin:0;padding:0">${rows.map((row) => {
      window._bluesMergeNames[row.id] = row.name;
      const lyricsCount = Number(row.lyrics_count ?? row.lyric_count ?? 0);
      const releasesCount = Array.isArray(row.discogs_releases) ? row.discogs_releases.length : 0;
      const discogsId = row.discogs_id ?? "";
      const subtitle = [
        discogsId ? `Discogs ${esc(discogsId)}` : "",
        lyricsCount ? `${lyricsCount} lyric${lyricsCount === 1 ? "" : "s"}` : "",
        releasesCount ? `${releasesCount} release${releasesCount === 1 ? "" : "s"}` : "",
      ].filter(Boolean).join(" · ");
      return `<li style="display:flex;align-items:center;gap:0.6rem;padding:0.4rem 0.3rem;border-bottom:1px solid rgba(255,255,255,0.06)">
        <span style="flex:1;min-width:0">
          <span style="color:#ddd;font-weight:600">${esc(row.name)}</span>
          <div style="font-size:0.7rem;color:#777">id ${esc(row.id)}${subtitle ? " · " + subtitle : ""}</div>
        </span>
        <button type="button" class="admin-btn" onclick="bluesDbPickMergeTarget(${Number(row.id)})">Merge here</button>
      </li>`;
    }).join("")}</ul>`;
  } catch (e) {
    results.innerHTML = `<div style="color:#a55">Search error: ${String(e).replace(/</g,"&lt;")}</div>`;
    if (status) status.textContent = "";
  }
}
async function bluesDbPickMergeTarget(targetId, targetNameArg) {
  const sourceId = _bluesDbState.editingId;
  if (!sourceId) return;
  const form = document.getElementById("blues-editor-form");
  const sourceName = form?.elements?.namedItem("name")?.value?.trim() || `#${sourceId}`;
  // Caller now passes only the id (names containing " broke the
  // onclick="…" attribute). Look the name up from the sidecar map we
  // populated when rendering the results, with a graceful fallback.
  const targetName = targetNameArg
    || (window._bluesMergeNames && window._bluesMergeNames[targetId])
    || `#${targetId}`;
  const msg =
    `Merge "${sourceName}" INTO "${targetName}"?\n\n` +
    `• Every lyric currently linked to "${sourceName}" (by FK or name match) will be reassigned to "${targetName}".\n` +
    `• "${targetName}"'s discogs_releases will absorb any new entries from "${sourceName}".\n` +
    `• Other metadata on "${targetName}" wins — fields like notes, bio, photos on "${sourceName}" are LOST.\n` +
    `• "${sourceName}" (row #${sourceId}) will be deleted.\n\n` +
    `This cannot be undone. Continue?`;
  if (!confirm(msg)) return;
  try {
    const r = await apiFetch("/api/blues-archive/merge", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fromId: sourceId, intoId: targetId }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Merge failed: " + (err.error ?? r.status));
      return;
    }
    const out = await r.json();
    bluesDbCloseMergePicker();
    bluesDbCloseEditor();
    // List refresh — the source row is gone, target's lyric count grew.
    if (typeof bluesDbRenderList === "function") {
      try { await _bluesDbLoad?.(); } catch {}
      bluesDbRenderList();
    }
    // Also refresh stats strip + archive list on the public-facing
    // Blues Archive view so the change reflects everywhere it shows.
    try { window._baLoadStats?.(); } catch {}
    try { window._baLoadList?.(); } catch {}
    if (typeof showToast === "function") {
      showToast(`Merged into "${out.intoName}" (${out.lyricsReassigned} lyric${out.lyricsReassigned === 1 ? "" : "s"} moved)`, "ok");
    } else {
      alert(`Merged. ${out.lyricsReassigned} lyrics moved to "${out.intoName}".`);
    }
  } catch (e) {
    alert("Merge failed: " + e);
  }
}
window.bluesDbOpenMergePicker = bluesDbOpenMergePicker;
window.bluesDbCloseMergePicker = bluesDbCloseMergePicker;
window.bluesDbRunMergePicker = bluesDbRunMergePicker;
window.bluesDbRunMergePickerDebounced = bluesDbRunMergePickerDebounced;
window.bluesDbPickMergeTarget = bluesDbPickMergeTarget;

async function bluesDbPickDiscogsCandidate(discogsId) {
  const id = _bluesDbState.editingId;
  if (!id) return;
  if (!confirm(`Set this row's discogs_id to ${discogsId}?`)) return;
  try {
    const r = await apiFetch("/api/admin/blues/" + id, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ discogs_id: discogsId }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Save failed: " + (err.error ?? r.status));
      return;
    }
    // Refresh the editor with the new id so the user can immediately
    // hit "Get all info from Discogs" against the confirmed match.
    // Cache patch + re-render avoids a full re-list fetch.
    await _bluesDbRefreshRow(id);
    bluesDbCloseDiscogsPicker();
    bluesDbOpenEditor(id);
    bluesDbRenderList();
  } catch (e) {
    alert("Save failed: " + e);
  }
}

async function bluesDbOpenEditor(id) {
  _bluesDbState.editingId = id;
  const overlay = document.getElementById("blues-editor-overlay");
  const form = document.getElementById("blues-editor-form");
  const title = document.getElementById("blues-editor-title");
  const delBtn = document.getElementById("blues-editor-delete");
  const mbBtn = document.getElementById("blues-editor-mb");
  const wikiBtn = document.getElementById("blues-editor-wiki");
  const discogsBtn = document.getElementById("blues-editor-discogs");
  const discogsRefreshBtn = document.getElementById("blues-editor-discogs-refresh");
  const mergeBtn = document.getElementById("blues-editor-merge");
  const ytBtn = document.getElementById("blues-editor-yt");
  form.reset();
  if (id) {
    title.textContent = "Edit artist";
    delBtn.style.display = "";
    mbBtn.style.display = "";
    wikiBtn.style.display = "";
    discogsBtn.style.display = "";
    if (discogsRefreshBtn) discogsRefreshBtn.style.display = "";
    if (mergeBtn) mergeBtn.style.display = "";
    ytBtn.style.display = "";
    try {
      const r = await apiFetch("/api/admin/blues/" + id);
      if (!r.ok) { alert("Could not load row."); return; }
      const row = await r.json();
      for (const [k, v] of Object.entries(row)) {
        const el = form.elements.namedItem(k);
        if (!el) continue;
        if (Array.isArray(v)) {
          // Complex arrays (objects) → JSON in a textarea so they're
          // editable. Plain string arrays → comma-separated for inputs.
          const isComplex = v.length > 0 && typeof v[0] === "object" && v[0] !== null;
          el.value = isComplex ? JSON.stringify(v, null, 2) : v.join(", ");
        }
        else if (v == null) el.value = "";
        else if (typeof v === "object") el.value = JSON.stringify(v, null, 2);
        else el.value = v;
      }
    } catch { alert("Error loading row."); return; }
  } else {
    title.textContent = "Add artist";
    delBtn.style.display = "none";
    mbBtn.style.display = "none";
    wikiBtn.style.display = "none";
    discogsBtn.style.display = "none";
    if (discogsRefreshBtn) discogsRefreshBtn.style.display = "none";
    if (mergeBtn) mergeBtn.style.display = "none";
    ytBtn.style.display = "none";
  }
  _bluesDbUpdateEditorLinks();
  // Linked artists list (separate /links endpoint — not part of the
  // main row payload). Fires only for existing rows; new rows show
  // a "save first" prompt.
  try { bluesDbLoadLinks(id || null); } catch {}
  overlay.style.display = "flex";
}

// Build hrefs for the editor's external-lookup bar from whatever
// IDs are currently in the form. Falls back to a name search if no
// stable identifier exists yet — the common case for a brand-new
// row. Called once when the editor opens; the bar is hidden when
// there's no name to search on at all.
function _bluesDbUpdateEditorLinks() {
  const form = document.getElementById("blues-editor-form");
  const bar = document.getElementById("blues-editor-extlinks");
  if (!form || !bar) return;
  const get = (n) => (form.elements.namedItem(n)?.value || "").trim();
  const name = get("name");
  const discogsId = get("discogs_id");
  const mbid = get("musicbrainz_mbid");
  const qid = get("wikidata_qid");
  const wikiSuffix = get("wikipedia_suffix");
  if (!name && !discogsId && !mbid && !qid && !wikiSuffix) {
    bar.style.display = "none";
    return;
  }
  bar.style.display = "flex";
  const enc = encodeURIComponent;
  const discogs = discogsId
    ? "https://www.discogs.com/artist/" + enc(discogsId)
    : "https://www.discogs.com/search/?type=artist&q=" + enc(name);
  const wiki = wikiSuffix
    ? "https://en.wikipedia.org" + (wikiSuffix.startsWith("/") ? wikiSuffix : "/wiki/" + wikiSuffix)
    : "https://en.wikipedia.org/w/index.php?search=" + enc(name);
  const mb = mbid
    ? "https://musicbrainz.org/artist/" + enc(mbid)
    : "https://musicbrainz.org/search?type=artist&query=" + enc(name);
  const wd = qid
    ? "https://www.wikidata.org/wiki/" + enc(qid)
    : "https://www.wikidata.org/w/index.php?search=" + enc(name);
  document.getElementById("blues-editor-link-discogs").href = discogs;
  document.getElementById("blues-editor-link-wikipedia").href = wiki;
  document.getElementById("blues-editor-link-musicbrainz").href = mb;
  document.getElementById("blues-editor-link-wikidata").href = wd;
}
function bluesDbCloseEditor() {
  document.getElementById("blues-editor-overlay").style.display = "none";
  _bluesDbState.editingId = null;
}
async function bluesDbSaveEditor() {
  const form = document.getElementById("blues-editor-form");
  const data = {};
  for (const el of form.elements) {
    if (!el.name) continue;
    data[el.name] = el.value.trim();
  }
  const id = _bluesDbState.editingId;
  const url = id ? "/api/admin/blues/" + id : "/api/admin/blues";
  const method = id ? "PUT" : "POST";
  try {
    const r = await apiFetch(url, {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Save failed: " + (err.error ?? r.status));
      return;
    }
    // For an edit (PUT): pull the saved row fresh and patch the cache
    // in place — no full re-list fetch. For a create (POST): the new
    // id lives in the response body; refresh that single row.
    let newId = id;
    if (!newId) {
      try {
        const j = await r.json();
        if (j && j.id) newId = j.id;
      } catch {}
    }
    if (newId) await _bluesDbRefreshRow(newId);
    bluesDbCloseEditor();
    bluesDbLoadStats();
    bluesDbRenderList();
    // One-shot hook for non-admin callers (e.g. the Discovery Blues
    // Archive view) — gets the just-saved id so they can refresh
    // whatever surface they had open behind the editor overlay.
    if (typeof window._bluesDbAfterSaveOnce === "function") {
      try { window._bluesDbAfterSaveOnce(newId); } catch {}
      window._bluesDbAfterSaveOnce = null;
    }
  } catch (e) { alert("Save failed: " + e); }
}

// ── Merge picker (admin Blues DB) ───────────────────────────────────
// Lightweight overlay — same flow as the Discovery-side merge picker
// but inline in admin.html so admin tooling doesn't depend on
// blues-archive.js. Reuses the POST /api/blues-archive/merge endpoint.
let _bluesMergeSearchTimer = null;
function bluesDbMergePicker(fromId, fromName) {
  let overlay = document.getElementById("blues-db-merge-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "blues-db-merge-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "310", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
    });
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }
  const safeFrom = String(fromName).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(600px,100%)">
      <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.6rem">
        <h3 style="margin:0">Merge <em>${safeFrom}</em> into…</h3>
        <button class="admin-btn" onclick="document.getElementById('blues-db-merge-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <p style="font-size:0.78rem;color:var(--muted);margin:0 0 0.6rem">
        Lyrics keyed to <em>${safeFrom}</em> are reassigned to the target.
        Release JSONB arrays are concatenated (deduped by id+type).
        This row is then deleted. Runs as a single transaction.
      </p>
      <input id="blues-merge-search" type="search" placeholder="Type to filter artists…" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem;margin-bottom:0.5rem" oninput="bluesDbMergePickerSearch(${fromId})">
      <div id="blues-merge-results" style="max-height:40vh;overflow:auto;border:1px solid var(--border);border-radius:4px;padding:0.4rem 0.6rem;font-size:0.84rem"></div>
      <div id="blues-merge-status" style="font-size:0.76rem;color:var(--muted);margin-top:0.5rem;min-height:1em"></div>
    </div>
  `;
  setTimeout(() => document.getElementById("blues-merge-search")?.focus(), 50);
  _bluesDbMergePickerLoad(fromId);
}
window.bluesDbMergePicker = bluesDbMergePicker;

function bluesDbMergePickerSearch(fromId) {
  if (_bluesMergeSearchTimer) clearTimeout(_bluesMergeSearchTimer);
  _bluesMergeSearchTimer = setTimeout(() => _bluesDbMergePickerLoad(fromId), 240);
}
window.bluesDbMergePickerSearch = bluesDbMergePickerSearch;

async function _bluesDbMergePickerLoad(fromId) {
  const q = (document.getElementById("blues-merge-search")?.value || "").trim();
  const el = document.getElementById("blues-merge-results");
  if (!el) return;
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  params.set("limit", "40");
  el.textContent = "Loading…";
  try {
    const r = await apiFetch(`/api/blues-archive/artists?${params}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const { rows = [] } = await r.json();
    const choices = rows.filter(row => row.id !== fromId);
    if (!choices.length) {
      el.innerHTML = `<div style="color:var(--muted);padding:0.4rem 0">No matches.</div>`;
      return;
    }
    const esc = s => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g, "&#39;");
    el.innerHTML = choices.map(row => `
      <div onclick="_bluesDbConfirmMerge(${fromId}, ${row.id}, '${esc(row.name || "")}')" style="cursor:pointer;padding:0.3rem 0;border-bottom:1px solid rgba(255,255,255,0.04);display:flex;justify-content:space-between;gap:0.6rem">
        <span style="font-weight:600;color:var(--text)">${esc(row.name || "")}</span>
        <span style="color:var(--muted);font-size:0.76rem">${row.lyrics_count || 0}L · ${row.releases_count || 0}R</span>
      </div>`).join("");
  } catch (e) {
    el.innerHTML = `<div style="color:#e88">Failed: ${e?.message || e}</div>`;
  }
}

async function _bluesDbConfirmMerge(fromId, intoId, intoName) {
  if (!confirm(`Merge into "${intoName}"? Lyrics get reassigned; releases get concatenated; the source row is deleted. Cannot be undone from the UI.`)) return;
  const statusEl = document.getElementById("blues-merge-status");
  if (statusEl) statusEl.textContent = "Merging…";
  try {
    const r = await apiFetch("/api/blues-archive/merge", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fromId, intoId }),
    });
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const j = await r.json();
    document.getElementById("blues-db-merge-overlay")?.remove();
    bluesDbLoadList();
    bluesDbLoadStats();
    alert(`Merged. ${j.lyricsReassigned} lyrics reassigned, ${j.releasesAdded} releases added.`);
  } catch (e) {
    if (statusEl) statusEl.textContent = `Merge failed: ${e?.message || e}`;
  }
}
window._bluesDbConfirmMerge = _bluesDbConfirmMerge;

// ── Linked artists (pseudonym / band) — editor section ────────────
// Loads the current set on editor open, lets the admin add/remove
// via a search picker. Storage is blues_artist_links — a symmetric
// junction table. Separate from the freeform aliases / collaborators
// inputs above because those are plain strings; this points at
// actual other rows by id so chip clicks open their profiles.
async function bluesDbLoadLinks(id) {
  const listEl = document.getElementById("blues-editor-links-list");
  if (!listEl) return;
  if (!id) { listEl.textContent = "Save the artist first to add links."; return; }
  listEl.textContent = "Loading…";
  try {
    const r = await apiFetch(`/api/admin/blues/${id}/links`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const { rows = [] } = await r.json();
    _bluesDbRenderLinks(rows);
  } catch (e) {
    listEl.innerHTML = `<span style="color:#e88">Load failed: ${e?.message || e}</span>`;
  }
}
window.bluesDbLoadLinks = bluesDbLoadLinks;

function _bluesDbRenderLinks(rows) {
  const listEl = document.getElementById("blues-editor-links-list");
  if (!listEl) return;
  if (!rows.length) { listEl.innerHTML = `<span style="color:var(--muted);font-style:italic">No links yet.</span>`; return; }
  const esc = s => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
  listEl.innerHTML = rows.map(r => {
    const label = r.kind === "band" ? "band" : "pseudonym";
    return `<span style="display:inline-flex;align-items:center;gap:0.35rem;padding:0.18rem 0.45rem;border:1px solid var(--accent);border-radius:999px;color:var(--accent);background:rgba(255,255,255,0.03)">
      <span style="color:var(--muted);font-size:0.68rem;text-transform:uppercase;letter-spacing:0.04em">${label}</span>
      <span>${esc(r.name)}</span>
      <a href="#" onclick="event.preventDefault();bluesDbRemoveLink(${r.id})" title="Remove link" style="color:#e88;text-decoration:none;font-weight:600">×</a>
    </span>`;
  }).join("");
}

async function bluesDbRemoveLink(otherId) {
  const id = _bluesDbState.editingId;
  if (!id) return;
  try {
    const r = await apiFetch(`/api/admin/blues/${id}/links/${otherId}`, { method: "DELETE" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    await bluesDbLoadLinks(id);
  } catch (e) {
    const s = document.getElementById("blues-editor-links-status");
    if (s) s.textContent = `Remove failed: ${e?.message || e}`;
  }
}
window.bluesDbRemoveLink = bluesDbRemoveLink;

// Picker overlay — same pattern as the Merge picker. Search field
// debounced; selecting a row prompts for kind (pseudonym/band) and
// POSTs to /links. Multi-add: stays open until the user closes.
let _bluesLinkSearchTimer = null;
function bluesDbOpenLinkPicker() {
  const fromId = _bluesDbState.editingId;
  if (!fromId) { alert("Save the artist first."); return; }
  let overlay = document.getElementById("blues-db-link-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "blues-db-link-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "360", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
    });
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(600px,100%)">
      <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.6rem">
        <h3 style="margin:0">Link to another artist</h3>
        <button class="archive-btn" onclick="document.getElementById('blues-db-link-overlay')?.remove();bluesDbLoadLinks(_bluesDbState.editingId)" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <p style="font-size:0.78rem;color:var(--muted);margin:0 0 0.6rem">
        Picks a target row, then asks: is this a <em>pseudonym</em>
        (same person, different recording name) or a <em>band</em>
        (they played together)? Symmetric — adding it from either
        side links both. Use Merge instead if you actually want to
        consolidate two rows.
      </p>
      <input id="blues-link-search" type="search" placeholder="Type to filter artists…" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem;margin-bottom:0.5rem" oninput="bluesDbLinkPickerSearch(${fromId})">
      <div id="blues-link-results" style="max-height:40vh;overflow:auto;border:1px solid var(--border);border-radius:4px;padding:0.4rem 0.6rem;font-size:0.84rem"></div>
      <div id="blues-link-status" style="font-size:0.76rem;color:var(--muted);margin-top:0.5rem;min-height:1em"></div>
    </div>
  `;
  setTimeout(() => document.getElementById("blues-link-search")?.focus(), 50);
  _bluesDbLinkPickerLoad(fromId);
}
window.bluesDbOpenLinkPicker = bluesDbOpenLinkPicker;

function bluesDbLinkPickerSearch(fromId) {
  if (_bluesLinkSearchTimer) clearTimeout(_bluesLinkSearchTimer);
  _bluesLinkSearchTimer = setTimeout(() => _bluesDbLinkPickerLoad(fromId), 240);
}
window.bluesDbLinkPickerSearch = bluesDbLinkPickerSearch;

async function _bluesDbLinkPickerLoad(fromId) {
  const q = (document.getElementById("blues-link-search")?.value || "").trim();
  const el = document.getElementById("blues-link-results");
  if (!el) return;
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  params.set("limit", "40");
  el.textContent = "Loading…";
  try {
    const r = await apiFetch(`/api/blues-archive/artists?${params}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const { rows = [] } = await r.json();
    const choices = rows.filter(row => row.id !== fromId);
    if (!choices.length) {
      el.innerHTML = `<div style="color:var(--muted);padding:0.4rem 0">No matches.</div>`;
      return;
    }
    const esc = s => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g, "&#39;");
    el.innerHTML = choices.map(row => `
      <div style="padding:0.3rem 0;border-bottom:1px solid rgba(255,255,255,0.04);display:flex;justify-content:space-between;align-items:center;gap:0.6rem">
        <span style="font-weight:600;color:var(--text)">${esc(row.name || "")}</span>
        <span style="display:inline-flex;gap:0.35rem">
          <button type="button" class="archive-btn" onclick="_bluesDbConfirmLink(${fromId}, ${row.id}, '${esc(row.name || "")}', 'pseudonym')" title="Same person, different recording name">Pseudonym</button>
          <button type="button" class="archive-btn" onclick="_bluesDbConfirmLink(${fromId}, ${row.id}, '${esc(row.name || "")}', 'band')" title="Played together (band, sideman, side project)">Band</button>
        </span>
      </div>`).join("");
  } catch (e) {
    el.innerHTML = `<div style="color:#e88">Failed: ${e?.message || e}</div>`;
  }
}

async function _bluesDbConfirmLink(fromId, otherId, otherName, kind) {
  const statusEl = document.getElementById("blues-link-status");
  if (statusEl) statusEl.textContent = `Linking ${otherName} as ${kind}…`;
  try {
    const r = await apiFetch(`/api/admin/blues/${fromId}/links`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ otherId, kind }),
    });
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    if (statusEl) statusEl.textContent = `Linked ${otherName} as ${kind}. Add more or close.`;
    await bluesDbLoadLinks(fromId);
  } catch (e) {
    if (statusEl) statusEl.textContent = `Failed: ${e?.message || e}`;
  }
}
window._bluesDbConfirmLink = _bluesDbConfirmLink;

async function bluesDbDeleteFromEditor() {
  const id = _bluesDbState.editingId;
  if (!id) return;
  if (!confirm("Delete this artist row? This cannot be undone.")) return;
  try {
    const r = await apiFetch("/api/admin/blues/" + id, { method: "DELETE" });
    if (!r.ok) { alert("Delete failed."); return; }
    // Drop from the local cache and re-render — no need to re-fetch.
    _bluesDbRemoveRowLocal(id);
    bluesDbCloseEditor();
    bluesDbLoadStats();
    bluesDbRenderList();
  } catch (e) { alert("Delete failed: " + e); }
}

// ── Lyrics archive (weeniecampbell.com scrape) ─────────────────────
let _lyricsPage = 0;
let _lyricsLimit = 100;
let _lyricsTotal = 0;
let _lyricsSearchTimer = null;
let _lyricsScrapePoll = null;

function lyricsInit() {
  lyricsLoadTunings();
  lyricsLoadList();
  // If a scrape is already running on the server (e.g. user reloaded
  // the admin page mid-run), poll status so the UI re-attaches.
  lyricsPollScrapeOnce();
}
window.lyricsInit = lyricsInit;

async function lyricsLoadTunings() {
  try {
    const r = await apiFetch("/api/admin/lyrics/tunings");
    if (!r.ok) return;
    const { tunings = [] } = await r.json();
    const sel = document.getElementById("lyrics-tuning");
    if (!sel) return;
    const current = sel.value;
    sel.innerHTML = `<option value="">All tunings</option>` + tunings.map(t =>
      `<option value="${escHtml(t.tuning)}">${escHtml(t.tuning)} (${t.n})</option>`
    ).join("");
    if (current) sel.value = current;
  } catch {}
}

function lyricsDebouncedSearch() {
  if (_lyricsSearchTimer) clearTimeout(_lyricsSearchTimer);
  _lyricsSearchTimer = setTimeout(() => { _lyricsPage = 0; lyricsLoadList(); }, 280);
}
window.lyricsDebouncedSearch = lyricsDebouncedSearch;

// Cached rows of the current page + a sort state. Sort is now driven
// server-side — clicking a header re-fetches the list with the new
// sort/order. (Previously this was a client-side sort over the visible
// 100 rows, which gave confusing partial orderings on large datasets.)
let _lyricsRowsCache = [];
const _lyricsSortState = { key: "page_title", dir: "asc" };
const _LYRICS_SORT_TYPES = { page_title: "str", artist: "str", tuning: "str", snippet: "str", scraped_at: "date" };
function _lyricsSort(key) {
  _adminToggleSort(_lyricsSortState, key);
  _lyricsPage = 0;          // back to page 1 when the order changes
  lyricsLoadList();
}
window._lyricsSort = _lyricsSort;

function _lyricsRenderTable() {
  const list = document.getElementById("lyrics-list");
  if (!list) return;
  // Server-side sort: just render what we got back, no client re-sort.
  const rows = _lyricsRowsCache;
  if (!rows.length) {
    list.innerHTML = `<p style="color:var(--muted);padding:0.5rem 0">No matches.</p>`;
    return;
  }
  const S = _lyricsSortState;
  list.innerHTML = `<table class="api-log-table" style="font-size:0.82rem;width:100%">
    <thead><tr>
      ${_adminSortTh("Title",   "page_title", S, "_lyricsSort")}
      ${_adminSortTh("Artist",  "artist",     S, "_lyricsSort")}
      ${_adminSortTh("Tuning",  "tuning",     S, "_lyricsSort")}
      ${_adminSortTh("Snippet", "snippet",    S, "_lyricsSort")}
      <th></th>
    </tr></thead>
    <tbody>${rows.map(row => `
      <tr style="cursor:pointer" onclick="lyricsOpenViewer(${row.id})">
        <td style="white-space:nowrap;color:var(--text);font-weight:600">${escHtml(row.page_title || "")}</td>
        <td style="white-space:nowrap">${escHtml(row.artist || "—")}</td>
        <td style="white-space:nowrap;color:var(--accent)">${escHtml(row.tuning || "")}</td>
        <td style="font-size:0.75rem;color:#888">${escHtml((row.snippet || "").replace(/\s+/g, " ").slice(0, 120))}…</td>
        <td><a href="${escHtml(row.page_url || "")}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--accent);text-decoration:none;font-size:0.74rem">wiki ↗</a></td>
      </tr>`).join("")}
    </tbody></table>`;
}

async function lyricsLoadList() {
  const list = document.getElementById("lyrics-list");
  if (!list) return;
  const q = (document.getElementById("lyrics-search")?.value || "").trim();
  const tuning = document.getElementById("lyrics-tuning")?.value || "";
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (tuning) params.set("tuning", tuning);
  // Plumb the current sort state through — the server is the
  // authority now, not _adminSortApply.
  if (_lyricsSortState.key) {
    params.set("sort",  _lyricsSortState.key);
    params.set("order", _lyricsSortState.dir);
  }
  params.set("limit", String(_lyricsLimit));
  params.set("offset", String(_lyricsPage * _lyricsLimit));
  list.textContent = "Loading…";
  try {
    const r = await apiFetch(`/api/admin/lyrics?${params}`);
    if (!r.ok) { list.innerHTML = `<p style="color:#e88">Failed: HTTP ${r.status}</p>`; return; }
    const { rows = [], total = 0 } = await r.json();
    _lyricsTotal = total;
    _lyricsRowsCache = rows;
    document.getElementById("lyrics-stats").textContent = total
      ? `${total.toLocaleString()} lyric${total === 1 ? "" : "s"} stored`
      : "No lyrics stored yet.";
    _lyricsRenderTable();
    lyricsRenderPager();
  } catch (e) {
    list.innerHTML = `<p style="color:#e88">Failed: ${escHtml(String(e?.message || e))}</p>`;
  }
}
window.lyricsLoadList = lyricsLoadList;

function lyricsRenderPager() {
  const el = document.getElementById("lyrics-pager");
  if (!el) return;
  const pageCount = Math.max(1, Math.ceil(_lyricsTotal / _lyricsLimit));
  const cur = _lyricsPage + 1;
  if (pageCount <= 1) { el.innerHTML = ""; return; }
  el.innerHTML = `
    <button class="admin-btn" ${cur <= 1 ? "disabled" : ""} onclick="lyricsGoToPage(${_lyricsPage - 1})">‹ Prev</button>
    <span style="color:var(--muted)">Page ${cur} / ${pageCount}</span>
    <button class="admin-btn" ${cur >= pageCount ? "disabled" : ""} onclick="lyricsGoToPage(${_lyricsPage + 1})">Next ›</button>
  `;
}

function lyricsGoToPage(p) {
  _lyricsPage = Math.max(0, p);
  lyricsLoadList();
}
window.lyricsGoToPage = lyricsGoToPage;

async function lyricsOpenViewer(id) {
  const overlay = document.getElementById("lyrics-viewer-overlay");
  const titleEl = document.getElementById("lyrics-viewer-title");
  const metaEl  = document.getElementById("lyrics-viewer-meta");
  const bodyEl  = document.getElementById("lyrics-viewer-body");
  if (!overlay) return;
  overlay.style.display = "flex";
  titleEl.textContent = "Loading…";
  metaEl.textContent = "";
  bodyEl.textContent = "";
  try {
    const r = await apiFetch(`/api/admin/lyrics/${id}`);
    if (!r.ok) { bodyEl.textContent = `Failed: HTTP ${r.status}`; return; }
    const row = await r.json();
    titleEl.textContent = row.page_title || "(untitled)";
    const meta = [
      row.artist ? `Artist: ${row.artist}` : "",
      row.tuning ? `Tuning: ${row.tuning}` : "",
      row.scraped_at ? `Scraped ${new Date(row.scraped_at).toLocaleString()}` : "",
    ].filter(Boolean).join(" · ");
    metaEl.innerHTML = `${escHtml(meta)} · <a href="${escHtml(row.page_url || "")}" target="_blank" rel="noopener" style="color:var(--accent)">View on wiki ↗</a>`;
    bodyEl.textContent = row.plaintext || "(no plaintext)";
  } catch (e) {
    bodyEl.textContent = `Failed: ${e?.message || e}`;
  }
}
window.lyricsOpenViewer = lyricsOpenViewer;

function lyricsCloseViewer() {
  const overlay = document.getElementById("lyrics-viewer-overlay");
  if (overlay) overlay.style.display = "none";
}
window.lyricsCloseViewer = lyricsCloseViewer;

async function lyricsReextract() {
  const btn = document.getElementById("lyrics-reextract-btn");
  const statusEl = document.getElementById("lyrics-scrape-status");
  if (!confirm("Re-run tuning + artist extractors over every stored row? (no scraping; just re-parses wikitext already in the DB)")) return;
  btn.disabled = true;
  if (statusEl) statusEl.textContent = "Re-extracting…";
  try {
    const r = await apiFetch("/api/admin/lyrics/reextract", { method: "POST", timeoutMs: 120000 });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    if (statusEl) statusEl.innerHTML = `<span style="color:#4caf50">Re-extracted</span> ${j.updated.toLocaleString()} of ${j.total.toLocaleString()} rows · ${j.tuningChanged} tuning · ${j.artistChanged} artist`;
    lyricsLoadTunings();
    lyricsLoadList();
  } catch (e) {
    if (statusEl) statusEl.textContent = `Re-extract failed: ${e?.message || e}`;
  } finally {
    btn.disabled = false;
  }
}
window.lyricsReextract = lyricsReextract;

async function lyricsSyncArtists(ev) {
  const force = !!(ev && ev.shiftKey);
  const btn = document.getElementById("lyrics-sync-artists-btn");
  const statusEl = document.getElementById("lyrics-scrape-status");
  const prompt = force
    ? "Force-overwrite every lyric's artist using the wiki's Category:Lyrics by Artist mapping?\n\n(Shift was held — this will replace artist values you may have manually set.)"
    : "Walk Category:Lyrics by Artist on weeniecampbell.com and fill the artist field on rows that don't have one yet?\n\nManually-set artist values are preserved unless you re-run with Shift held.";
  if (!confirm(prompt)) return;
  btn.disabled = true;
  if (statusEl) statusEl.textContent = "Starting…";
  try {
    const r = await apiFetch(`/api/admin/lyrics/sync-artists-from-wiki${force ? "?force=1" : ""}`, { method: "POST" });
    if (r.status === 409) {
      if (statusEl) statusEl.textContent = "Another scrape/sync is running — attaching to status…";
    } else if (!r.ok) {
      if (statusEl) statusEl.textContent = `Failed: HTTP ${r.status}`;
      btn.disabled = false;
      return;
    }
    // Reuses the same scrape-status poll/UI as the main scrape job —
    // they share _lyricsScrapeState on the server.
    lyricsStartPolling();
  } catch (e) {
    if (statusEl) statusEl.textContent = `Failed: ${e?.message || e}`;
    btn.disabled = false;
  }
}
window.lyricsSyncArtists = lyricsSyncArtists;

async function lyricsStopScrape() {
  const statusEl = document.getElementById("lyrics-scrape-status");
  if (!confirm("Stop the lyrics scrape after the current page?")) return;
  try {
    await apiFetch("/api/admin/lyrics/scrape/stop", { method: "POST" });
    if (statusEl) statusEl.textContent = "Stop requested — finishing current page…";
  } catch (e) {
    if (statusEl) statusEl.textContent = `Stop failed: ${e?.message || e}`;
  }
}
window.lyricsStopScrape = lyricsStopScrape;

async function lyricsStartScrape() {
  const btn = document.getElementById("lyrics-scrape-btn");
  const statusEl = document.getElementById("lyrics-scrape-status");
  // If lyrics are already in the DB, scraping is rarely what the user
  // wants — Sync artists / Re-extract cover the usual follow-up needs
  // without re-fetching every page. Confirm with the actual count so
  // the user can be sure they meant it.
  let storedCount = 0;
  try {
    const r = await apiFetch("/api/admin/lyrics/scrape/status");
    if (r.ok) storedCount = Number((await r.json()).totalStored) || 0;
  } catch {}
  const baseMsg = "Start the lyrics scrape? Walks ~4006 pages at 1.2 s each — about 80 minutes. You can leave the page; it runs server-side. Resumable.";
  const prompt = storedCount > 0
    ? `You already have ${storedCount.toLocaleString()} lyrics stored. A fresh scrape isn't usually needed once the corpus is in — Sync artists / Re-extract handle the common follow-ups. Run a full scrape anyway?\n\n${baseMsg}`
    : baseMsg;
  if (!confirm(prompt)) return;
  btn.disabled = true;
  statusEl.textContent = "Starting…";
  try {
    const r = await apiFetch("/api/admin/lyrics/scrape", { method: "POST" });
    if (r.status === 409) {
      statusEl.textContent = "Already running — attaching to status…";
    } else if (!r.ok) {
      statusEl.textContent = `Failed: HTTP ${r.status}`;
      btn.disabled = false;
      return;
    }
    lyricsStartPolling();
  } catch (e) {
    statusEl.textContent = `Failed: ${e?.message || e}`;
    btn.disabled = false;
  }
}
window.lyricsStartScrape = lyricsStartScrape;

function lyricsStartPolling() {
  if (_lyricsScrapePoll) clearInterval(_lyricsScrapePoll);
  // Slow poll — scrape is long-running, no need to hammer.
  _lyricsScrapePoll = setInterval(lyricsPollScrapeOnce, 5000);
  lyricsPollScrapeOnce();
}

async function lyricsPollScrapeOnce() {
  const statusEl = document.getElementById("lyrics-scrape-status");
  const btn = document.getElementById("lyrics-scrape-btn");
  if (!statusEl) return;
  try {
    const r = await apiFetch("/api/admin/lyrics/scrape/status");
    if (!r.ok) return;
    const s = await r.json();
    const stats = document.getElementById("lyrics-stats");
    if (stats && typeof s.totalStored === "number") {
      stats.textContent = `${s.totalStored.toLocaleString()} lyric${s.totalStored === 1 ? "" : "s"} stored`;
    }
    const stopBtn = document.getElementById("lyrics-stop-btn");
    if (s.running) {
      btn.disabled = true;
      if (stopBtn) stopBtn.style.display = "";
      const phase = s.phase || "running";
      // Label the job kind so artist-sync runs aren't mistaken for a
      // new scrape — they share progress state but mean different
      // things (sync is DB-only after the wiki walk; scrape fetches
      // page content from the wiki).
      const jobLabel = s.jobKind === "artist-sync" ? "Artist sync"
                     : s.jobKind === "scrape"      ? "Lyric scrape"
                     :                               "Running";
      if (phase === "discovering") {
        statusEl.innerHTML = `<span style="color:var(--accent)">${escHtml(jobLabel)} ·</span> ${escHtml(s.message || "").slice(0, 140)}`;
      } else {
        const pct = s.pagesDiscovered ? Math.round((s.pagesScraped + s.pagesSkipped) / s.pagesDiscovered * 100) : 0;
        statusEl.innerHTML = `<span style="color:var(--accent)">${escHtml(jobLabel)} ·</span> ${s.pagesScraped}/${s.pagesDiscovered} (${pct}%) · skipped ${s.pagesSkipped} · failed ${s.pagesFailed}${s.currentTitle ? ` · <em style="color:var(--muted)">${escHtml(s.currentTitle).slice(0, 60)}</em>` : ""}`;
      }
    } else {
      btn.disabled = false;
      if (stopBtn) stopBtn.style.display = "none";
      if (s.finishedAt && s.startedAt) {
        const mins = Math.round((s.finishedAt - s.startedAt) / 60000);
        statusEl.innerHTML = `<span style="color:#4caf50">${escHtml(s.message)}</span> · ${mins} min`;
      } else {
        statusEl.textContent = s.message || "";
      }
      if (_lyricsScrapePoll) { clearInterval(_lyricsScrapePoll); _lyricsScrapePoll = null; }
      // Refresh list + tunings if a scrape just finished
      if (s.pagesScraped > 0 && s.finishedAt) {
        lyricsLoadTunings();
        lyricsLoadList();
      }
    }
    // Render the recently-added panel (Discovery view only — admin
    // page doesn't have this slot). Lists every new lyric from THIS
    // run with click-through shortcuts to SeaDisco + Discogs search.
    _lyricsRenderRecentlyAdded(s);
  } catch {}
}

// Recently-added list — populated by the rescrape job's recentlyAdded
// array (server-side). Each row gets two shortcut links: 🔍 to search
// SeaDisco prefilled with the lyric title + artist, and ↗ to open
// Discogs artist search in a new tab so the admin can match against
// the catalog. Hidden when there's nothing new (idle session or no
// new rows yet).
function _lyricsRenderRecentlyAdded(s) {
  const panel = document.getElementById("lyrics-scrape-added");
  if (!panel) return;                   // not on the Discovery view
  const rows = Array.isArray(s?.recentlyAdded) ? s.recentlyAdded : [];
  if (!rows.length) { panel.style.display = "none"; panel.innerHTML = ""; return; }
  panel.style.display = "";
  const label = s.running ? "Adding now" : "Newly added this run";
  // Render newest-first so the live progress reads top-down.
  const reversed = rows.slice().reverse();
  const html = reversed.map(r => {
    const title = String(r.title || "");
    const artist = String(r.artist || "");
    const sdQs = `?q=${encodeURIComponent(title)}` +
                 `&a=${encodeURIComponent(artist)}` +
                 `&r=${encodeURIComponent("master+")}` +
                 `&s=${encodeURIComponent("year:asc")}`;
    const dcQs = `https://www.discogs.com/search?type=artist&q=${encodeURIComponent(artist || title)}`;
    return `<div style="display:flex;gap:0.4rem;align-items:baseline;padding:0.15rem 0;font-size:0.84rem;border-bottom:1px solid rgba(255,255,255,0.04)">
      <a href="/${sdQs}" class="ba-lyric-search" title="Search SeaDisco — masters+, oldest first">🔍</a>
      <a href="${dcQs}" target="_blank" rel="noopener" class="ba-lyric-search" title="Search Discogs for this artist (opens in a new tab)">↗</a>
      <span style="color:var(--text);font-weight:600">${escHtml(title)}</span>
      <span style="color:var(--muted)">${artist ? "— " + escHtml(artist) : "<em>(no artist)</em>"}</span>
    </div>`;
  }).join("");
  panel.innerHTML = `
    <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:0.4rem">
      <strong style="font-size:0.86rem">${escHtml(label)} <span style="color:var(--muted);font-weight:400">(${rows.length}${rows.length >= 500 ? "+ shown; older trimmed" : ""})</span></strong>
      <button type="button" class="archive-btn" onclick="document.getElementById('lyrics-scrape-added').style.display='none'" title="Dismiss this panel">×</button>
    </div>
    <div style="max-height:240px;overflow-y:auto">${html}</div>`;
}
