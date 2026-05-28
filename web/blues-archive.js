// blues-archive.js — admin-only Discovery sub-view combining
// blues_artists, blues_lyrics, and the discogs_releases JSONB. Lazy-
// loaded by switchView('blues-archive'). All endpoints are gated by
// requireAdmin server-side; this module trusts that gate and just
// surfaces the responses.

let _baPage = 0;
const _BA_LIMIT = 100;
let _baTotal = 0;
let _baSearchTimer = null;
let _baCurrentArtistId = null;

// Cache + sort state. The list page sorts the visible page client-side
// (server still owns pagination); the detail page sorts whichever
// inner table the user clicks (full lists).
let _baListRowsCache = [];
// Category filter — set by the stats-strip chips, cleared by clicking
// "Artists" (or the × on the filter indicator). One of:
//   "", "with_both", "with_lyrics_only", "with_releases_only", "empty"
let _baListCategory = "";
const _baListSort = { key: "name", dir: "asc" };
const _BA_LIST_TYPES = { name: "str", discogs_id: "num", lyrics_count: "num", releases_count: "num" };

let _baDetailArtist = null;
const _baLyricsSort = { key: "page_title", dir: "asc" };
const _BA_LYRICS_TYPES = { page_title: "str", tuning: "str", snippet: "str", scraped_at: "date" };
const _baReleasesSort = { key: "year", dir: "asc" };
const _BA_RELEASES_TYPES = { year: "num", title: "str", label: "str", type: "str" };

// Tiny self-contained sort helpers — admin.html's _adminSort* lives
// in admin.html only; blues-archive.js loads on the main site so
// duplicating these few lines keeps the module standalone.
function _baSortApply(rows, state, types) {
  if (!state || !state.key) return rows;
  const t = (types || {})[state.key] || "str";
  const dir = state.dir === "desc" ? -1 : 1;
  const val = (o) => {
    let v = o?.[state.key];
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
function _baToggleSort(state, key) {
  if (state.key === key) state.dir = state.dir === "asc" ? "desc" : "asc";
  else { state.key = key; state.dir = "asc"; }
}
function _baSortTh(label, key, state, fn, extraStyle) {
  const active = state.key === key;
  const arrow = active ? (state.dir === "desc" ? "▼" : "▲") : "";
  return `<th class="admin-sort-th${active ? " is-active" : ""}" style="padding:0.3rem 0.5rem;cursor:pointer;user-select:none;${extraStyle || ""}" onclick="${fn}('${key}')">${label}<span class="admin-sort-arrow" style="margin-left:0.3rem">${arrow}</span></th>`;
}

function initBluesArchiveView() {
  // First entry — paint the list view. Artist detail is now a popup
  // overlay (#ba-artist-overlay) created on demand, so we no longer
  // toggle the legacy inline #blues-archive-detail panel; we just make
  // sure it's hidden in case anything left it visible.
  const detail = document.getElementById("blues-archive-detail");
  if (detail) detail.style.display = "none";
  const list = document.querySelector("#blues-archive-view .blues-archive-list");
  if (list) list.style.display = "";
  _baLoadList();
  // Stats strip + recent-edits feed — fire in parallel, non-blocking.
  _baLoadStats().catch(() => {});
  _baLoadRecent().catch(() => {});
  // Lazy-load /blues-admin.js for admins so the bulk-Discogs job
  // status + lyrics scrape polling auto-attach to anything already
  // running on the server (e.g. user reloaded mid-scrape). Non-
  // critical path — silent on failure. Polling itself is started
  // by lyricsPollScrapeOnce + _bluesResumeBulkIfRunning once loaded.
  if (window._isAdmin && typeof window._sdLoadModule === "function") {
    window._sdLoadModule("/blues-admin.js").then(() => {
      // Reattach to any in-flight lyrics scrape (start polling for
      // live progress; the poll self-cancels when the job finishes).
      try { window.lyricsStartPolling?.(); } catch {}
      try { window._bluesResumeBulkIfRunning?.("blues-enrich-discogs-full-btn", "/api/admin/blues/enrich-discogs-full/status", "Get all info from Discogs"); } catch {}
    }).catch(() => { /* non-critical */ });
  }
  // Deep-link support: /?v=blues-archive&baArtist=ID opens the given
  // archive artist after the list view paints. Used by the 🎸 badge
  // on album/version modals to jump straight to a matched artist.
  try {
    const p = new URLSearchParams(window.location.search);
    const aid = parseInt(p.get("baArtist") || "", 10);
    if (Number.isFinite(aid) && aid > 0) {
      setTimeout(() => _baOpenArtist(aid), 40);
    }
  } catch { /* non-fatal */ }
}
window.initBluesArchiveView = initBluesArchiveView;

function _bluesArchiveDebouncedSearch() {
  if (_baSearchTimer) clearTimeout(_baSearchTimer);
  _baSearchTimer = setTimeout(() => { _baPage = 0; _baLoadList(); }, 280);
}
window._bluesArchiveDebouncedSearch = _bluesArchiveDebouncedSearch;

async function _baLoadList() {
  const rowsEl = document.getElementById("blues-archive-rows");
  const countEl = document.getElementById("blues-archive-count");
  if (!rowsEl) return;
  const q = (document.getElementById("blues-archive-search")?.value || "").trim();
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (_baListCategory) params.set("category", _baListCategory);
  // Server-side sort — same fix as lyrics. Client-side sort over only
  // the visible 100 rows used to mislead users into thinking the
  // entire DB had been sorted.
  if (_baListSort?.key) {
    params.set("sort",  _baListSort.key);
    params.set("order", _baListSort.dir);
  }
  params.set("limit", String(_BA_LIMIT));
  params.set("offset", String(_baPage * _BA_LIMIT));
  // Render the active-filter indicator (or hide it) regardless of fetch
  // result so the affordance updates even when the network is slow.
  _baRenderArtistsFilterIndicator();
  // Dim instead of wipe — keeps the existing rows visible (and the
  // user's scroll position) until the new data arrives.
  const scrollY = window.scrollY;
  rowsEl.classList.add("ba-loading");
  try {
    const r = await apiFetch(`/api/blues-archive/artists?${params}`);
    if (!r.ok) { rowsEl.innerHTML = `<p style="color:#e88">Failed: HTTP ${r.status}</p>`; return; }
    const { rows = [], total = 0 } = await r.json();
    _baTotal = total;
    if (countEl) countEl.textContent = total ? `${total.toLocaleString()} artist${total === 1 ? "" : "s"}` : "No artists yet.";
    _baListRowsCache = rows;
    _baRenderListTable();
    _baRenderPager();
    requestAnimationFrame(() => window.scrollTo(0, Math.min(scrollY, document.documentElement.scrollHeight - window.innerHeight)));
  } catch (e) {
    rowsEl.innerHTML = `<p style="color:#e88">Failed: ${escHtml(String(e?.message || e))}</p>`;
  } finally {
    rowsEl.classList.remove("ba-loading");
  }
}

function _baSortList(key) {
  _baToggleSort(_baListSort, key);
  _baPage = 0;           // back to page 1 when the order changes
  _baLoadList();
}
window._baSortList = _baSortList;

function _baRenderListTable() {
  const rowsEl = document.getElementById("blues-archive-rows");
  if (!rowsEl) return;
  // Server-side sort — render as-is.
  const rows = _baListRowsCache;
  if (!rows.length) {
    rowsEl.innerHTML = `<p style="color:var(--muted);padding:0.5rem 0">No matches.</p>`;
    return;
  }
  const S = _baListSort;
  rowsEl.innerHTML = `
    <table class="api-log-table" style="font-size:0.86rem;width:100%">
      <thead><tr>
        ${_baSortTh("Name",       "name",            S, "_baSortList")}
        ${_baSortTh("Discogs ID", "discogs_id",      S, "_baSortList")}
        ${_baSortTh("Lyrics",     "lyrics_count",    S, "_baSortList", "text-align:right")}
        ${_baSortTh("Releases",   "releases_count",  S, "_baSortList", "text-align:right")}
      </tr></thead>
      <tbody>${rows.map(row => {
        // Name cell uses entityLookupLinkHtml so clicking the text
        // opens the unified search-options popup (Wikipedia / YouTube
        // / LOC / Archive.org / Search SeaDisco / Copy). Clicking
        // anywhere else on the row opens the Blues Archive artist
        // detail page (existing behavior).
        const nameHtml = (typeof entityLookupLinkHtml === "function" && row.name)
          ? entityLookupLinkHtml("artist", row.name, { entityId: row.discogs_id, title: `Lookup options for "${row.name}"` })
          : escHtml(row.name || "");
        // Discogs ID — link out to discogs.com/artist/<id> when set so
        // the user can jump straight to the canonical artist page;
        // stopPropagation so the row-click doesn't also fire and steal
        // them into the archive detail. Blank when no id is on file.
        const didHtml = row.discogs_id
          ? `<a href="https://www.discogs.com/artist/${row.discogs_id}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--accent);text-decoration:none;font-variant-numeric:tabular-nums" title="Open on Discogs.com ↗">${row.discogs_id}</a>`
          : `<span style="color:var(--muted)">—</span>`;
        return `<tr style="cursor:pointer" onclick="_baOpenArtist(${row.id})">
          <td style="font-weight:600;color:var(--text)">${nameHtml}</td>
          <td style="font-size:0.78rem">${didHtml}</td>
          <td style="text-align:right;color:${row.lyrics_count ? "var(--accent)" : "var(--muted)"}">${row.lyrics_count || ""}</td>
          <td style="text-align:right;color:${row.releases_count ? "var(--accent)" : "var(--muted)"}">${row.releases_count || ""}</td>
        </tr>`;
      }).join("")}</tbody>
    </table>`;
}

function _baRenderPager() {
  const el = document.getElementById("blues-archive-pager");
  if (!el) return;
  const pageCount = Math.max(1, Math.ceil(_baTotal / _BA_LIMIT));
  const cur = _baPage + 1;
  if (pageCount <= 1) { el.innerHTML = ""; return; }
  el.innerHTML = `
    <button class="archive-btn" ${cur <= 1 ? "disabled" : ""} onclick="_baGoToPage(${_baPage - 1})">‹ Prev</button>
    <span style="color:var(--muted)">Page ${cur} / ${pageCount}</span>
    <button class="archive-btn" ${cur >= pageCount ? "disabled" : ""} onclick="_baGoToPage(${_baPage + 1})">Next ›</button>
  `;
}

function _baGoToPage(p) {
  _baPage = Math.max(0, p);
  _baLoadList();
}
window._baGoToPage = _baGoToPage;

// Open an artist as an overlay popup. Was previously a page swap that
// hid .blues-archive-list and rendered into #blues-archive-detail; the
// popup pattern reads better (the list stays put, close = ×) and lines
// up with how lyric viewing already works. #blues-archive-detail is no
// longer touched — left in the DOM for backward compat only.
async function _baOpenArtist(id) {
  _baCurrentArtistId = id;
  // Reuse overlay if already there (e.g. opened twice in a row).
  let overlay = document.getElementById("ba-artist-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "ba-artist-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "300", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
    });
    overlay.onclick = (e) => { if (e.target === overlay) _baBackToList(); };
    document.body.appendChild(overlay);
  }
  overlay.innerHTML = `<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(980px,100%);color:var(--muted)">Loading…</div>`;
  try {
    const r = await apiFetch(`/api/blues-archive/artists/${id}`);
    if (!r.ok) { overlay.innerHTML = `<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(980px,100%)"><p style="color:#e88;margin:0">Failed: HTTP ${r.status}</p></div>`; return; }
    const a = await r.json();
    _baRenderArtistDetail(a);
  } catch (e) {
    overlay.innerHTML = `<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(980px,100%)"><p style="color:#e88;margin:0">Failed: ${escHtml(String(e?.message || e))}</p></div>`;
  }
}
window._baOpenArtist = _baOpenArtist;

function _baSortLyrics(key) {
  _baToggleSort(_baLyricsSort, key);
  if (_baDetailArtist) _baRenderArtistDetail(_baDetailArtist);
}
window._baSortLyrics = _baSortLyrics;

function _baSortReleases(key) {
  _baToggleSort(_baReleasesSort, key);
  if (_baDetailArtist) _baRenderArtistDetail(_baDetailArtist);
}
window._baSortReleases = _baSortReleases;

function _baRenderArtistDetail(a) {
  _baDetailArtist = a;
  // Render into the overlay's inner box. Falls back to the legacy
  // #blues-archive-detail panel only if the overlay isn't around (e.g.
  // someone called _baRenderArtistDetail directly outside the popup
  // flow). New callers should go through _baOpenArtist.
  const overlay = document.getElementById("ba-artist-overlay");
  const detail = overlay
    ? (overlay.firstElementChild || (() => {
        const inner = document.createElement("div");
        Object.assign(inner.style, {
          background: "var(--surface)", border: "1px solid var(--border)",
          borderRadius: "8px", padding: "1.2rem 1.4rem", width: "min(980px,100%)",
        });
        overlay.appendChild(inner);
        return inner;
      })())
    : document.getElementById("blues-archive-detail");
  if (!detail) return;
  // Ensure the overlay's container has the popup chrome styles
  // (re-rendering swaps innerHTML on the loading shell that already
  // has them, but a defensive re-apply guarantees it).
  if (overlay && detail === overlay.firstElementChild) {
    Object.assign(detail.style, {
      background: "var(--surface)", border: "1px solid var(--border)",
      borderRadius: "8px", padding: "1.2rem 1.4rem", width: "min(980px,100%)",
    });
  }
  const dates = [a.birth_date, a.death_date].filter(Boolean).join(" – ");
  // Schema field is `notes` (no `profile` column on blues_artists).
  const bio = a.notes ? `<p style="font-size:0.86rem;line-height:1.5;color:var(--text);white-space:pre-wrap;margin:0.6rem 0">${escHtml(a.notes).slice(0, 4000)}</p>` : "";
  const photo = a.photo_url
    ? `<img src="${escHtml(a.photo_url)}" alt="" style="width:140px;height:140px;object-fit:cover;border-radius:4px;flex:0 0 auto" loading="lazy" />`
    : "";
  const meta = [
    a.birth_place ? `Born: ${escHtml(a.birth_place)}` : "",
    a.death_place ? `Died: ${escHtml(a.death_place)}` : "",
    a.hometown_region ? `From: ${escHtml(a.hometown_region)}` : "",
    a.first_recording_year ? `First recording: ${a.first_recording_year}` : "",
  ].filter(Boolean).join(" · ");
  const lyricsRaw = Array.isArray(a.lyrics) ? a.lyrics : [];
  const releasesRaw = Array.isArray(a.releases) ? a.releases : [];
  const lyrics   = _baSortApply(lyricsRaw,   _baLyricsSort,   _BA_LYRICS_TYPES);
  const releases = _baSortApply(releasesRaw, _baReleasesSort, _BA_RELEASES_TYPES);
  const LS = _baLyricsSort, RS = _baReleasesSort;
  // Title cells use entityLookupLinkHtml so the title text fires the
  // unified search-options popup (Wikipedia / YouTube / LOC / Archive
  // / Search SeaDisco / Copy). The rest of each row still opens the
  // detail/viewer (existing behavior preserved).
  const lyricsHtml = lyricsRaw.length
    ? `<table class="api-log-table" style="font-size:0.84rem;width:100%">
        <thead><tr>
          ${_baSortTh("Title",   "page_title", LS, "_baSortLyrics")}
          ${_baSortTh("Tuning",  "tuning",     LS, "_baSortLyrics")}
          ${_baSortTh("Snippet", "snippet",    LS, "_baSortLyrics")}
          <th style="width:1%"></th>
        </tr></thead>
        <tbody>${lyrics.map(l => {
          const titleHtml = (typeof entityLookupLinkHtml === "function" && l.page_title)
            ? entityLookupLinkHtml("track", l.page_title, { trackArtist: a.name || "", title: `Lookup options for "${l.page_title}"` })
            : escHtml(l.page_title || "");
          return `<tr data-lyric-row="${l.id}">
            <td style="font-weight:600;color:var(--text);white-space:nowrap">${titleHtml}</td>
            <td style="white-space:nowrap;color:var(--accent);cursor:pointer" onclick="_baOpenLyric(${l.id})">${escHtml(l.tuning || "")}</td>
            <td style="font-size:0.76rem;color:#888;cursor:pointer" onclick="_baOpenLyric(${l.id})">${escHtml((l.snippet || "").replace(/\s+/g, " ").slice(0, 140))}…</td>
            <td style="text-align:right"><a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenLyricEditor(${l.id})" style="color:var(--muted);text-decoration:none;font-size:0.78rem" title="Edit tuning / artist on this lyric">✎</a></td>
          </tr>`;
        }).join("")}</tbody>
      </table>`
    : `<p style="color:var(--muted);font-style:italic;padding:0.4rem 0">No lyrics matched this artist's name. (Try Import from lyrics on the list page if you've just scraped.)</p>`;
  const releasesHtml = releasesRaw.length
    ? `<table class="api-log-table" style="font-size:0.84rem;width:100%">
        <thead><tr>
          ${_baSortTh("Year",  "year",  RS, "_baSortReleases")}
          ${_baSortTh("Title", "title", RS, "_baSortReleases")}
          ${_baSortTh("Label", "label", RS, "_baSortReleases")}
          ${_baSortTh("Type",  "type",  RS, "_baSortReleases")}
        </tr></thead>
        <tbody>${releases.map(rel => {
          const type = String(rel.type || "release");
          const url  = `https://www.discogs.com/${type === "master" ? "master" : "release"}/${rel.id}`;
          const safeUrl = url.replace(/'/g, "\\'");
          const titleHtml = (typeof entityLookupLinkHtml === "function" && rel.title)
            ? entityLookupLinkHtml("release", rel.title, { entityId: rel.id, title: `Lookup options for "${rel.title}"` })
            : escHtml(rel.title || "");
          return `<tr style="cursor:pointer" onclick="_baOpenRelease(${rel.id}, '${escHtml(type).replace(/'/g, "\\'")}', '${escHtml(safeUrl)}')">
            <td style="white-space:nowrap;color:var(--muted);font-variant-numeric:tabular-nums">${rel.year || "—"}</td>
            <td style="font-weight:600;color:var(--text)">${titleHtml}</td>
            <td style="color:#888;font-size:0.78rem">${escHtml(rel.label || "")}</td>
            <td style="color:var(--accent);font-size:0.74rem;text-transform:uppercase">${escHtml(type)}</td>
          </tr>`;
        }).join("")}</tbody>
      </table>`
    : `<p style="color:var(--muted);font-style:italic;padding:0.4rem 0">No releases stored. Use the existing "Get all info from Discogs" button on the Blues DB tab to populate them.</p>`;
  detail.innerHTML = `
    <div style="display:flex;align-items:center;gap:0.6rem;margin-bottom:1rem;flex-wrap:wrap">
      <button class="archive-btn" onclick="_baBackToList()" title="Close">×</button>
      <h2 style="margin:0;font-size:1.1rem">${(typeof entityLookupLinkHtml === "function" && a.name)
        ? entityLookupLinkHtml("artist", a.name, { entityId: a.discogs_id, title: `Lookup options for "${a.name}"` })
        : escHtml(a.name || "")}</h2>
      <span style="color:var(--muted);font-size:0.82rem">${escHtml(dates)}</span>
      <span style="margin-left:auto;display:inline-flex;gap:0.4rem;flex-wrap:wrap">
        <button class="archive-btn archive-btn-suggest" onclick="_baOpenFullEditor(${a.id})" title="Open the full edit form (~25 fields: bio, dates, identifiers, enrichment buttons for Wiki / MusicBrainz / Discogs picker / YouTube).">Edit (full form)</button>
        <button class="archive-btn" onclick="_baOpenReassignPicker(${a.id}, ${JSON.stringify(a.name || "").replace(/"/g, "&quot;")})" title="Reassign every lyric matching some other artist name (or artist row) to this artist. Doesn't delete the source artist — use Merge for that.">Reassign lyrics from…</button>
        <button class="archive-btn" onclick="_baOpenMergePicker(${a.id}, ${JSON.stringify(a.name || "").replace(/"/g, "&quot;")})" title="Merge this artist into another. Lyrics get reassigned by name; release JSONB arrays are concatenated (deduped); this row is then deleted.">Merge into…</button>
      </span>
    </div>
    <div style="display:flex;gap:1rem;margin-bottom:1.2rem;align-items:flex-start;flex-wrap:wrap">
      ${photo}
      <div style="flex:1;min-width:240px">
        ${meta ? `<div style="font-size:0.82rem;color:var(--muted);margin-bottom:0.4rem">${meta}</div>` : ""}
        ${bio}
      </div>
    </div>
    <h3 style="font-size:0.92rem;color:var(--accent);margin:1rem 0 0.5rem">Lyrics (${lyrics.length})</h3>
    ${(() => {
      const tunings = Array.isArray(a.tunings) ? a.tunings : [];
      if (!tunings.length) return "";
      // Tuning chip strip — click filters the master Lyrics tab to
      // this artist + tuning combo. Stays inside the popup so the user
      // sees the filter applied to the larger list.
      return `<div style="display:flex;gap:0.4rem;flex-wrap:wrap;margin-bottom:0.5rem">${
        tunings.map(t => `<a href="#" onclick="event.preventDefault();_baJumpTuningForArtist('${escHtml((t.tuning || "").replace(/'/g, "\\'"))}', '${escHtml((a.name || "").replace(/'/g, "\\'"))}')" style="padding:0.2rem 0.45rem;border:1px solid var(--border);border-radius:999px;font-size:0.74rem;color:var(--accent);text-decoration:none">${escHtml(t.tuning)} <span style="color:var(--muted)">· ${t.n}</span></a>`).join("")
      }</div>`;
    })()}
    ${lyricsHtml}
    <h3 style="font-size:0.92rem;color:var(--accent);margin:1.4rem 0 0.5rem">Releases — oldest to newest (${releases.length})</h3>
    ${releasesHtml}
  `;
}

// Dismiss the artist popup. The function name predates the popup
// rewrite (it used to swap back to the list page); kept so existing
// onclick attributes don't need rewriting.
function _baBackToList() {
  document.getElementById("ba-artist-overlay")?.remove();
  // Legacy: if anything still uses the inline panel, hide it too so
  // we don't end up with both visible after a hot reload.
  const detail = document.getElementById("blues-archive-detail");
  const list = document.querySelector("#blues-archive-view .blues-archive-list");
  if (detail) detail.style.display = "none";
  if (list) list.style.display = "";
  _baCurrentArtistId = null;
}
window._baBackToList = _baBackToList;

// Reuse the same lyric viewer the admin Lyrics tab uses. The admin
// view is on a different page (/admin), so we render a simple inline
// popup here using the existing chronam-style overlay pattern.
async function _baOpenLyric(id) {
  try {
    const r = await apiFetch(`/api/admin/lyrics/${id}`);
    if (!r.ok) return;
    const row = await r.json();
    let overlay = document.getElementById("ba-lyric-overlay");
    if (!overlay) {
      overlay = document.createElement("div");
      overlay.id = "ba-lyric-overlay";
      Object.assign(overlay.style, {
        position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
        zIndex: "300", display: "flex", alignItems: "flex-start",
        justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
      });
      overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
      document.body.appendChild(overlay);
    }
    // Header meta line — artist (clickable to archive popup if linked,
    // plain text otherwise), tuning, and the optional Discogs release /
    // master pins linked out to discogs.com. Each segment shows only
    // if the field has a value, separated by middle dots.
    const metaParts = [];
    if (row.artist) {
      const artistTxt = escHtml(row.artist);
      if (row.artist_id) {
        metaParts.push(`<a href="#" onclick="event.preventDefault();_baOpenArtistFromBadge(${row.artist_id});return false" style="color:var(--accent);text-decoration:none" title="Open in Blues Archive">${artistTxt}</a>`);
      } else {
        metaParts.push(`<span>${artistTxt}</span>`);
      }
    }
    if (row.tuning) metaParts.push(`<span>Tuning: ${escHtml(row.tuning)}</span>`);
    if (row.discogs_release_id) {
      metaParts.push(`<a href="https://www.discogs.com/release/${row.discogs_release_id}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none" title="Open release on Discogs">Release ${row.discogs_release_id} ↗</a>`);
    }
    if (row.discogs_master_id) {
      metaParts.push(`<a href="https://www.discogs.com/master/${row.discogs_master_id}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none" title="Open master on Discogs">Master ${row.discogs_master_id} ↗</a>`);
    }
    const metaHtml = metaParts.length
      ? `<div style="font-size:0.78rem;color:var(--muted);display:flex;flex-wrap:wrap;gap:0.4rem;align-items:center">${metaParts.join('<span style="color:#555">·</span>')}</div>`
      : "";
    overlay.innerHTML = `
      <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(820px,100%)">
        <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.6rem">
          <div style="min-width:0">
            <h3 style="margin:0 0 0.25rem">${escHtml(row.page_title || "")}</h3>
            ${metaHtml}
          </div>
          <div style="display:flex;gap:0.4rem;align-items:start">
            <button class="archive-btn" onclick="_baOpenLyricEditor(${row.id})" title="Edit title / artist / tuning on this lyric">Edit</button>
            <button class="archive-btn" onclick="document.getElementById('ba-lyric-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
          </div>
        </div>
        <pre style="white-space:pre-wrap;font-family:inherit;font-size:0.88rem;line-height:1.5;color:var(--text);max-height:60vh;overflow:auto;background:rgba(255,255,255,0.02);border:1px solid var(--border);border-radius:4px;padding:0.8rem 1rem;margin:0">${escHtml(row.plaintext || "(no plaintext)")}</pre>
      </div>
    `;
  } catch (e) {
    console.warn("[blues-archive] lyric open failed:", e);
  }
}
window._baOpenLyric = _baOpenLyric;

// Open a release in the same modal the main site uses (modal.js's
// openModal). Falls back to opening the Discogs page if the function
// isn't around.
function _baOpenRelease(id, type, discogsUrl) {
  if (typeof openModal === "function") {
    openModal(null, id, type, discogsUrl);
  } else {
    window.open(discogsUrl, "_blank", "noopener");
  }
}
window._baOpenRelease = _baOpenRelease;

// ── Lyric editor (per-row pencil) ────────────────────────────────────
// Small overlay form for fixing tuning / artist on a single lyric.
// Tuning input is a datalist populated from /api/admin/lyrics/tunings
// so existing values auto-complete and stay consistent; the user can
// type a new value too.
async function _baOpenLyricEditor(id) {
  let row;
  try {
    const r = await apiFetch(`/api/admin/lyrics/${id}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    row = await r.json();
  } catch (e) {
    alert(`Couldn't load lyric: ${e?.message || e}`);
    return;
  }
  // Pull existing tunings for the datalist
  let tunings = [];
  try {
    const r = await apiFetch("/api/admin/lyrics/tunings");
    if (r.ok) tunings = (await r.json()).tunings ?? [];
  } catch {}
  const opts = tunings
    .filter(t => t.tuning && t.tuning !== "(unspecified)")
    .map(t => `<option value="${escHtml(t.tuning)}">`)
    .join("");
  // Pull existing artists for an Artist datalist so the user can pick
  // an existing row instead of typing a free-text orphan. Capped at
  // 500 — datalist autocompletes locally so the cap is just a guard
  // against huge transfers.
  let artistOpts = "";
  try {
    const r = await apiFetch("/api/blues-archive/artists?limit=500");
    if (r.ok) {
      const { rows = [] } = await r.json();
      artistOpts = rows
        .map(a => `<option value="${escHtml(a.name)}" data-id="${a.id}">`)
        .join("");
    }
  } catch {}
  let overlay = document.getElementById("ba-lyric-edit-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "ba-lyric-edit-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "310", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
    });
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(560px,100%)">
      <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.6rem">
        <div style="min-width:0">
          <h3 style="margin:0 0 0.25rem">Edit lyric</h3>
          <div style="font-size:0.76rem;color:var(--muted)"><a href="${escHtml(row.page_url || "")}" target="_blank" rel="noopener" style="color:var(--accent)">View on wiki ↗</a></div>
        </div>
        <button class="archive-btn" onclick="document.getElementById('ba-lyric-edit-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <datalist id="ba-tuning-options">${opts}</datalist>
      <datalist id="ba-artist-options">${artistOpts}</datalist>
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)">Title</label>
      <input id="ba-edit-title" type="text" value="${escHtml(row.page_title || "")}" placeholder="(required)" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)" title="Type to search existing artists (autocomplete from the Blues DB). Pick one to link via artist_id, or type a fresh name and hit '+ Create as new' to mint a row.">Artist</label>
      <div style="display:flex;gap:0.4rem;align-items:stretch">
        <input id="ba-edit-artist" type="text" value="${escHtml(row.artist || "")}" list="ba-artist-options" placeholder="(leave blank to clear)" style="flex:1;padding:0.45rem 0.7rem;font-size:0.88rem" autocomplete="off" oninput="_baEditArtistInput()">
        <button type="button" id="ba-edit-create-artist" class="archive-btn" onclick="_baEditCreateArtist()" title="Create a new blues_artists row from the typed name and link this lyric to it. Use when no existing artist matches.">+ Create as new</button>
      </div>
      <div id="ba-edit-artist-status" style="font-size:0.74rem;color:var(--muted);margin-top:0.25rem;min-height:1em">${
        row.artist_id ? "✓ Linked to existing artist row" : (row.artist ? "Orphan — not linked. Pick an existing match or click '+ Create as new'." : "")
      }</div>
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)">Tuning</label>
      <input id="ba-edit-tuning" type="text" value="${escHtml(row.tuning || "")}" list="ba-tuning-options" placeholder="(leave blank to clear)" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
      <!-- Optional Discogs release/master pin. When set, the album-
           modal per-track 📜 affordance prefers this lyric for the
           matching release id instead of guessing by title+artist. -->
      <div style="display:flex;gap:0.5rem;margin:0.6rem 0 0">
        <div style="flex:1">
          <label style="display:block;margin:0 0 0.3rem;font-size:0.82rem;color:var(--muted)" title="Discogs release ID this lyric belongs to (optional). When set, the album modal picks this lyric for exact track matches on that release.">Discogs release ID</label>
          <input id="ba-edit-release-id" type="number" min="1" value="${row.discogs_release_id ?? ""}" placeholder="(optional)" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
        </div>
        <div style="flex:1">
          <label style="display:block;margin:0 0 0.3rem;font-size:0.82rem;color:var(--muted)" title="Discogs master ID this lyric belongs to (optional). Covers every pressing of the work.">Discogs master ID</label>
          <input id="ba-edit-master-id" type="number" min="1" value="${row.discogs_master_id ?? ""}" placeholder="(optional)" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
        </div>
      </div>
      <div style="display:flex;gap:0.5rem;justify-content:flex-end;margin-top:1rem">
        <button class="archive-btn" onclick="document.getElementById('ba-lyric-edit-overlay')?.remove()">Cancel</button>
        <button class="archive-btn archive-btn-suggest" onclick="_baSaveLyricEdit(${id})">Save</button>
      </div>
      <div id="ba-edit-status" style="font-size:0.76rem;color:var(--muted);margin-top:0.5rem;min-height:1em"></div>
    </div>
  `;
}
window._baOpenLyricEditor = _baOpenLyricEditor;

// Live status under the Artist input: tells the user whether the
// currently-typed name matches an existing blues_artists row (✓), is
// blank (cleared), or doesn't match (will save as orphan).
function _baEditArtistInput() {
  const input = document.getElementById("ba-edit-artist");
  const dl    = document.getElementById("ba-artist-options");
  const stat  = document.getElementById("ba-edit-artist-status");
  if (!input || !stat) return;
  const v = input.value.trim();
  if (!v) { stat.textContent = "Will clear the artist on save."; stat.style.color = "var(--muted)"; return; }
  const lc = v.toLowerCase();
  const matched = dl
    ? [...dl.querySelectorAll("option")].some(o => o.value.toLowerCase() === lc)
    : false;
  if (matched) {
    stat.textContent = "✓ Matches an existing artist row — will link on save.";
    stat.style.color = "#7bc77b";
  } else {
    stat.textContent = "No matching artist row — will save as orphan. Click '+ Create as new' to mint one.";
    stat.style.color = "#e8a85a";
  }
}
window._baEditArtistInput = _baEditArtistInput;

// "+ Create as new" — POST a new blues_artists row with the typed
// name, then save the lyric edit so the FK links immediately. Skips
// creation when the name already matches an existing row (just saves).
async function _baEditCreateArtist() {
  const input = document.getElementById("ba-edit-artist");
  const stat  = document.getElementById("ba-edit-artist-status");
  const name  = (input?.value || "").trim();
  if (!name) {
    if (stat) { stat.textContent = "Type a name first."; stat.style.color = "#e8a85a"; }
    return;
  }
  if (stat) { stat.textContent = "Creating…"; stat.style.color = "var(--muted)"; }
  try {
    const r = await apiFetch("/api/blues-archive/artists", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const j = await r.json();
    if (stat) {
      stat.textContent = j.created
        ? `✓ Created new artist row #${j.id}. Click Save to link.`
        : `✓ Existing artist row #${j.id} matched. Click Save to link.`;
      stat.style.color = "#7bc77b";
    }
    // Drop the new option into the datalist so future renders show it.
    const dl = document.getElementById("ba-artist-options");
    if (dl && j.name) {
      dl.insertAdjacentHTML("beforeend", `<option value="${escHtml(j.name)}" data-id="${j.id}">`);
    }
  } catch (e) {
    if (stat) { stat.textContent = `Failed: ${e?.message || e}`; stat.style.color = "#e88"; }
  }
}
window._baEditCreateArtist = _baEditCreateArtist;

async function _baSaveLyricEdit(id) {
  const statusEl   = document.getElementById("ba-edit-status");
  const artist     = document.getElementById("ba-edit-artist")?.value ?? "";
  const tuning     = document.getElementById("ba-edit-tuning")?.value ?? "";
  const page_title = (document.getElementById("ba-edit-title")?.value ?? "").trim();
  // Empty string → null on the server (releases pin cleared).
  const releaseIdRaw = document.getElementById("ba-edit-release-id")?.value ?? "";
  const masterIdRaw  = document.getElementById("ba-edit-master-id")?.value ?? "";
  const discogs_release_id = releaseIdRaw === "" ? null : Number(releaseIdRaw);
  const discogs_master_id  = masterIdRaw  === "" ? null : Number(masterIdRaw);
  if (!page_title) { if (statusEl) statusEl.textContent = "Title is required."; return; }
  if (statusEl) statusEl.textContent = "Saving…";
  try {
    const r = await apiFetch(`/api/admin/lyrics/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ artist, tuning, page_title, discogs_release_id, discogs_master_id }),
    });
    if (!r.ok) {
      // 409 = title already taken on this source_host. Surface the
      // server's reason so the user knows what to do; other failures
      // bubble up as a generic HTTP message.
      if (r.status === 409) throw new Error("Another lyric already has that title on this source.");
      throw new Error(`HTTP ${r.status}`);
    }
    // The PATCH endpoint returns the updated row so we can patch the
    // UI in place without a single extra fetch. This preserves the
    // user's scroll position (the previous version refetched the
    // master list + artist popup, both of which collapsed the DOM
    // and bounced the scrollbar to the top).
    const updated = await r.json().catch(() => null);
    document.getElementById("ba-lyric-edit-overlay")?.remove();
    if (updated && updated.id != null) {
      // Update local caches so subsequent re-renders (e.g. sort
      // header click) still see the patched data.
      const i = _baLyricsRowsCache.findIndex(x => Number(x.id) === Number(updated.id));
      if (i >= 0) _baLyricsRowsCache[i] = { ..._baLyricsRowsCache[i], ...updated };
      if (_baDetailArtist && Array.isArray(_baDetailArtist.lyrics)) {
        const j = _baDetailArtist.lyrics.findIndex(x => Number(x.id) === Number(updated.id));
        if (j >= 0) _baDetailArtist.lyrics[j] = { ..._baDetailArtist.lyrics[j], ...updated };
      }
      // Surgical DOM patch — both master list <tr> AND artist popup
      // sub-table <tr> are swapped if present. Single-row swap
      // doesn't disturb the surrounding scroll geometry.
      _baPatchLyricRowEverywhere(updated);
      // If the viewer popup is showing this same lyric, repaint it
      // with the new values. It's a single overlay so the scroll
      // impact on the underlying page is zero.
      const viewerOpen = !!document.getElementById("ba-lyric-overlay");
      if (viewerOpen) {
        document.getElementById("ba-lyric-overlay")?.remove();
        _baOpenLyric(updated.id);
      }
    }
  } catch (e) {
    if (statusEl) statusEl.textContent = `Save failed: ${e?.message || e}`;
  }
}
window._baSaveLyricEdit = _baSaveLyricEdit;

// ── Merge picker ─────────────────────────────────────────────────────
// Two-step flow: type to filter the artist list, click the target,
// then confirm. Server transaction reassigns lyrics + appends releases
// + deletes the source row.
async function _baOpenMergePicker(fromId, fromName) {
  let overlay = document.getElementById("ba-merge-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "ba-merge-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "310", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
    });
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(600px,100%)">
      <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.6rem">
        <h3 style="margin:0">Merge <em>${escHtml(fromName)}</em> into…</h3>
        <button class="archive-btn" onclick="document.getElementById('ba-merge-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <p style="font-size:0.78rem;color:var(--muted);margin:0 0 0.6rem">
        Lyrics keyed to <em>${escHtml(fromName)}</em> will be reassigned to the target.
        Release JSONB arrays will be concatenated (deduped by id+type).
        The source row will be deleted. This action runs as a single transaction.
      </p>
      <input id="ba-merge-search" type="search" placeholder="Type to filter artists…" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem;margin-bottom:0.5rem" oninput="_baMergePickerSearch(${fromId})">
      <div id="ba-merge-results" style="max-height:40vh;overflow:auto;border:1px solid var(--border);border-radius:4px;padding:0.4rem 0.6rem;font-size:0.84rem"></div>
      <div id="ba-merge-status" style="font-size:0.76rem;color:var(--muted);margin-top:0.5rem;min-height:1em"></div>
    </div>
  `;
  // Auto-focus the filter input
  setTimeout(() => document.getElementById("ba-merge-search")?.focus(), 50);
  _baMergePickerSearch(fromId);
}
window._baOpenMergePicker = _baOpenMergePicker;

let _baMergePickerTimer = null;
function _baMergePickerSearch(fromId) {
  if (_baMergePickerTimer) clearTimeout(_baMergePickerTimer);
  _baMergePickerTimer = setTimeout(() => _baMergePickerLoad(fromId), 240);
}
window._baMergePickerSearch = _baMergePickerSearch;

async function _baMergePickerLoad(fromId) {
  const q = (document.getElementById("ba-merge-search")?.value || "").trim();
  const resultsEl = document.getElementById("ba-merge-results");
  if (!resultsEl) return;
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  params.set("limit", "40");
  resultsEl.textContent = "Loading…";
  try {
    const r = await apiFetch(`/api/blues-archive/artists?${params}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const { rows = [] } = await r.json();
    const choices = rows.filter(row => row.id !== fromId);
    if (!choices.length) {
      resultsEl.innerHTML = `<p style="color:var(--muted);padding:0.4rem 0">No matches.</p>`;
      return;
    }
    resultsEl.innerHTML = choices.map(row => `
      <div onclick="_baConfirmMerge(${fromId}, ${row.id}, ${JSON.stringify(row.name || "").replace(/"/g, "&quot;")})" style="cursor:pointer;padding:0.3rem 0;border-bottom:1px solid rgba(255,255,255,0.04);display:flex;justify-content:space-between;gap:0.6rem">
        <span style="font-weight:600;color:var(--text)">${escHtml(row.name || "")}</span>
        <span style="color:var(--muted);font-size:0.76rem">${row.lyrics_count || 0}L · ${row.releases_count || 0}R</span>
      </div>`).join("");
  } catch (e) {
    resultsEl.innerHTML = `<p style="color:#e88">Failed: ${escHtml(String(e?.message || e))}</p>`;
  }
}

async function _baConfirmMerge(fromId, intoId, intoName) {
  if (!confirm(`Merge into "${intoName}"? Lyrics get reassigned; releases get concatenated; the source row is then deleted. This cannot be undone from the UI.`)) return;
  const statusEl = document.getElementById("ba-merge-status");
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
    document.getElementById("ba-merge-overlay")?.remove();
    // Navigate to the target so the user sees the merged result.
    _baOpenArtist(intoId);
    setTimeout(() => alert(`Merged. ${j.lyricsReassigned} lyrics reassigned, ${j.releasesAdded} releases added.`), 100);
  } catch (e) {
    if (statusEl) statusEl.textContent = `Merge failed: ${e?.message || e}`;
  }
}
window._baConfirmMerge = _baConfirmMerge;

// Admin button — import distinct lyrics-artist names that aren't yet
// in blues_artists. Idempotent (server checks LOWER(name) uniqueness).
async function bluesArchiveImport() {
  const btn = document.getElementById("blues-archive-import-btn");
  const statusEl = document.getElementById("blues-archive-import-status");
  if (!confirm("Walk the scraped lyrics and insert any artist names not already in the Blues DB?")) return;
  if (btn) btn.disabled = true;
  if (statusEl) statusEl.textContent = "Importing…";
  try {
    const r = await apiFetch("/api/blues-archive/import-from-lyrics", { method: "POST", timeoutMs: 60000 });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    if (statusEl) statusEl.innerHTML = `<span style="color:#4caf50">Imported</span> ${j.added} new of ${j.total} distinct · ${j.existing} already in DB · ${j.rejected || 0} rejected by validator`;
    _baLoadList();
  } catch (e) {
    if (statusEl) statusEl.textContent = `Import failed: ${e?.message || e}`;
  } finally {
    if (btn) btn.disabled = false;
  }
}
window.bluesArchiveImport = bluesArchiveImport;

// Admin button — remove blues_artists rows that were created by the
// lyric-import job AND have no other data (no Discogs ID, no Wikidata
// QID, no bio, etc.). Safety net for when the artist extractor pulled
// trash before the validator was tightened. Manually-edited rows are
// preserved server-side.
async function bluesArchivePurgeImports() {
  const btn = document.getElementById("blues-archive-purge-btn");
  const statusEl = document.getElementById("blues-archive-import-status");
  if (!confirm("Remove all unenriched lyric-import rows from the Blues DB? Rows with any manually-added data (Discogs ID, bio, dates, etc.) are kept.")) return;
  if (btn) btn.disabled = true;
  if (statusEl) statusEl.textContent = "Purging…";
  try {
    const r = await apiFetch("/api/blues-archive/purge-lyric-imports", { method: "POST", timeoutMs: 60000 });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    if (statusEl) statusEl.innerHTML = `<span style="color:#4caf50">Purged</span> ${j.removed.toLocaleString()} row${j.removed === 1 ? "" : "s"}`;
    _baLoadList();
  } catch (e) {
    if (statusEl) statusEl.textContent = `Purge failed: ${e?.message || e}`;
  } finally {
    if (btn) btn.disabled = false;
  }
}
window.bluesArchivePurgeImports = bluesArchivePurgeImports;

// ── Lyrics sub-tab ───────────────────────────────────────────────────
// Master searchable list of every scraped lyric, independent of any
// artist. Reuses /api/admin/lyrics (admin-gated, which matches the
// archive's own gate) for data, and the existing _baOpenLyric overlay
// for viewing. Tuning dropdown is populated lazily from /tunings.
let _baSubtab = "artists"; // "artists" | "lyrics"
let _baLyricsPage = 0;
const _BA_LYRICS_LIMIT = 100;
let _baLyricsTotal = 0;
let _baLyricsSearchTimer = null;
let _baLyricsTuning = "";
let _baLyricsUnmatched = false;
let _baLyricsRowsCache = [];
const _baLyricsListSort = { key: "page_title", dir: "asc" };
const _BA_LYRICS_LIST_TYPES = { page_title: "str", artist: "str", tuning: "str", snippet: "str" };
let _baLyricsTuningsLoaded = false;

function _baSwitchSubtab(tab) {
  _baSubtab = tab === "lyrics" ? "lyrics" : "artists";
  // Toggle button active state
  document.querySelectorAll("#blues-archive-subtabs .ba-subtab").forEach(b => {
    b.classList.toggle("is-active", b.dataset.baTab === _baSubtab);
  });
  const ap = document.getElementById("blues-archive-artists-panel");
  const lp = document.getElementById("blues-archive-lyrics-panel");
  if (ap) ap.style.display = _baSubtab === "artists" ? "" : "none";
  if (lp) lp.style.display = _baSubtab === "lyrics"  ? "" : "none";
  if (_baSubtab === "lyrics") {
    if (!_baLyricsTuningsLoaded) _baLoadTunings();
    _baLoadLyrics();
  }
}
window._baSwitchSubtab = _baSwitchSubtab;

function _baLyricsDebouncedSearch() {
  if (_baLyricsSearchTimer) clearTimeout(_baLyricsSearchTimer);
  _baLyricsSearchTimer = setTimeout(() => { _baLyricsPage = 0; _baLoadLyrics(); }, 280);
}
window._baLyricsDebouncedSearch = _baLyricsDebouncedSearch;

function _baLyricsApplyTuning() {
  _baLyricsTuning = document.getElementById("blues-archive-lyrics-tuning")?.value || "";
  _baLyricsPage = 0;
  _baLoadLyrics();
}
window._baLyricsApplyTuning = _baLyricsApplyTuning;

function _baLyricsApplyUnmatched() {
  _baLyricsUnmatched = !!document.getElementById("blues-archive-lyrics-unmatched")?.checked;
  _baLyricsPage = 0;
  _baLoadLyrics();
}
window._baLyricsApplyUnmatched = _baLyricsApplyUnmatched;

// Clear all active filters on the Lyrics tab. Bound to the
// "Clear filters" button that auto-shows/hides based on filter state.
function _baLyricsClearFilters() {
  const search    = document.getElementById("blues-archive-lyrics-search");
  const tuningSel = document.getElementById("blues-archive-lyrics-tuning");
  const unmatched = document.getElementById("blues-archive-lyrics-unmatched");
  if (search)    search.value = "";
  if (tuningSel) tuningSel.value = "";
  if (unmatched) unmatched.checked = false;
  _baLyricsTuning    = "";
  _baLyricsUnmatched = false;
  _baLyricsPage      = 0;
  _baLoadLyrics();
}
window._baLyricsClearFilters = _baLyricsClearFilters;

// Bulk-set every NULL / empty tuning to "Standard". On a blues-lyrics
// wiki an unmentioned tuning is overwhelmingly standard, so collapsing
// the "(unspecified)" bucket into the Standard one matches reality.
// Confirms, then PATCHes via a single dedicated server endpoint added
// alongside this UI.
async function _baNormalizeEmptyTuningsToStandard() {
  if (!confirm("Set every lyric with no extracted tuning to 'Standard'?\n\nOn a blues-lyrics wiki the unmentioned default is standard. Reversible by editing rows individually afterward.")) return;
  try {
    const r = await apiFetch("/api/blues-archive/lyrics/normalize-empty-tunings", { method: "POST", timeoutMs: 60000 });
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const j = await r.json();
    alert(`Updated ${j.updated.toLocaleString()} lyric${j.updated === 1 ? "" : "s"} → Standard.`);
    _baLoadStats().catch(() => {});
    if (_baSubtab === "lyrics") _baLoadLyrics();
    // Refresh tuning dropdown so "(unspecified)" disappears.
    _baLyricsTuningsLoaded = false;
    _baLoadTunings().catch(() => {});
  } catch (e) {
    alert(`Failed: ${e?.message || e}`);
  }
}
window._baNormalizeEmptyTuningsToStandard = _baNormalizeEmptyTuningsToStandard;

async function _baLoadTunings() {
  try {
    const r = await apiFetch("/api/admin/lyrics/tunings");
    if (!r.ok) return;
    const { tunings = [] } = await r.json();
    const sel = document.getElementById("blues-archive-lyrics-tuning");
    if (!sel) return;
    // Preserve current selection if user already picked one
    const current = sel.value;
    sel.innerHTML = `<option value="">All tunings</option>` +
      tunings.map(t => `<option value="${escHtml(t.tuning)}">${escHtml(t.tuning)} (${t.n})</option>`).join("");
    if (current) sel.value = current;
    _baLyricsTuningsLoaded = true;
  } catch { /* non-fatal */ }
}

async function _baLoadLyrics() {
  const rowsEl  = document.getElementById("blues-archive-lyrics-rows");
  const countEl = document.getElementById("blues-archive-lyrics-count");
  if (!rowsEl) return;
  const q = (document.getElementById("blues-archive-lyrics-search")?.value || "").trim();
  const params = new URLSearchParams();
  if (q)                 params.set("q", q);
  if (_baLyricsTuning)   params.set("tuning", _baLyricsTuning);
  if (_baLyricsUnmatched) params.set("unmatched", "1");
  // Toggle the "Clear filters" button visibility based on whether
  // any filter is currently active.
  const clearBtn = document.getElementById("blues-archive-lyrics-clear");
  if (clearBtn) clearBtn.style.display = (q || _baLyricsTuning || _baLyricsUnmatched) ? "" : "none";
  // Server-side sort — see admin.html for the parallel wiring. The
  // client-side _baSortApply over the visible page was misleading
  // on the master Lyrics list because it only reordered the current
  // 100 rows, not the full dataset.
  if (_baLyricsListSort?.key) {
    params.set("sort",  _baLyricsListSort.key);
    params.set("order", _baLyricsListSort.dir);
  }
  params.set("limit",  String(_BA_LYRICS_LIMIT));
  params.set("offset", String(_baLyricsPage * _BA_LYRICS_LIMIT));
  // No "Loading…" wipe — that collapsed the table height and dropped
  // the user's scroll position. Instead dim the existing rows via a
  // CSS class while the fetch runs.
  const scrollY = window.scrollY;
  rowsEl.classList.add("ba-loading");
  try {
    const r = await apiFetch(`/api/admin/lyrics?${params}`);
    if (!r.ok) { rowsEl.innerHTML = `<p style="color:#e88">Failed: HTTP ${r.status}</p>`; return; }
    const { rows = [], total = 0 } = await r.json();
    _baLyricsTotal = total;
    if (countEl) countEl.textContent = total ? `${total.toLocaleString()} lyric${total === 1 ? "" : "s"}` : "No matches.";
    _baLyricsRowsCache = rows;
    _baRenderLyricsTable();
    _baRenderLyricsPager();
    // Restore scroll. The new table may be a different height than
    // the old one; clamp manually to avoid bouncing past the bottom.
    requestAnimationFrame(() => window.scrollTo(0, Math.min(scrollY, document.documentElement.scrollHeight - window.innerHeight)));
  } catch (e) {
    rowsEl.innerHTML = `<p style="color:#e88">Failed: ${escHtml(String(e?.message || e))}</p>`;
  } finally {
    rowsEl.classList.remove("ba-loading");
  }
}

function _baSortLyricsList(key) {
  _baToggleSort(_baLyricsListSort, key);
  _baLyricsPage = 0;          // back to page 1 when the order changes
  _baLoadLyrics();
}
window._baSortLyricsList = _baSortLyricsList;

// Single-row HTML — extracted so saves can do surgical in-place
// replacement without rebuilding the whole table (and blowing the
// user's scroll position). Used by both the master Lyrics table and
// the artist popup's lyric sub-table.
function _baLyricRowHtml(l) {
  const titleHtml = (typeof entityLookupLinkHtml === "function" && l.page_title)
    ? entityLookupLinkHtml("track", l.page_title, { trackArtist: l.artist || "", title: `Lookup options for "${l.page_title}"` })
    : escHtml(l.page_title || "");
  const artistHtml = l.artist && typeof entityLookupLinkHtml === "function"
    ? entityLookupLinkHtml("artist", l.artist, { title: `Lookup options for "${l.artist}"` })
    : escHtml(l.artist || "");
  // 🎸 archive-link when the lyric resolves to a blues_artists row
  // (canonical artist_id), or a "+ promote" button when it's an
  // orphan with a non-blank artist string. Mutually exclusive.
  const archiveAffordance = l.artist_id
    ? `<a href="#" class="ba-archive-badge" onclick="event.preventDefault();event.stopPropagation();_baOpenArtistFromBadge(${l.artist_id})" title="Open in Blues Archive">🎸</a>`
    : (l.artist && String(l.artist).trim()
        ? `<a href="#" class="ba-promote-link" onclick="event.preventDefault();event.stopPropagation();_baPromoteOrphan(${l.id})" title="Add as a new blues_artists row and link all orphans with this name" style="color:var(--accent);text-decoration:none;font-size:0.86em;margin-left:0.25rem">+ artist</a>`
        : "");
  return `<tr data-lyric-row="${l.id}">
    <td style="font-weight:600;color:var(--text);white-space:nowrap">${titleHtml}</td>
    <td style="color:var(--muted);white-space:nowrap">${artistHtml}${archiveAffordance}</td>
    <td style="white-space:nowrap;color:var(--accent);cursor:pointer" onclick="_baOpenLyric(${l.id})">${escHtml(l.tuning || "")}</td>
    <td style="font-size:0.76rem;color:#888;cursor:pointer" onclick="_baOpenLyric(${l.id})">${escHtml((l.snippet || "").replace(/\s+/g, " ").slice(0, 140))}…</td>
    <td style="text-align:right"><a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenLyricEditor(${l.id})" style="color:var(--muted);text-decoration:none;font-size:0.78rem" title="Edit title / artist / tuning on this lyric">✎</a></td>
  </tr>`;
}

// Patch a single rendered row in place after a save. Walks every <tr>
// with data-lyric-row=id (covers both the master list and the artist
// popup sub-table) and replaces its outerHTML with the fresh row.
// Scroll position is preserved because we only swap the one row.
function _baPatchLyricRowEverywhere(updated) {
  if (!updated || updated.id == null) return;
  document.querySelectorAll(`tr[data-lyric-row="${updated.id}"]`).forEach(tr => {
    tr.outerHTML = _baLyricRowHtml(updated);
  });
}

function _baRenderLyricsTable() {
  const rowsEl = document.getElementById("blues-archive-lyrics-rows");
  if (!rowsEl) return;
  // Server-side sort — render as-is.
  const rows = _baLyricsRowsCache;
  if (!rows.length) {
    rowsEl.innerHTML = `<p style="color:var(--muted);padding:0.5rem 0">No matches.</p>`;
    return;
  }
  const S = _baLyricsListSort;
  rowsEl.innerHTML = `
    <table class="api-log-table" style="font-size:0.84rem;width:100%">
      <thead><tr>
        ${_baSortTh("Title",   "page_title", S, "_baSortLyricsList")}
        ${_baSortTh("Artist",  "artist",     S, "_baSortLyricsList")}
        ${_baSortTh("Tuning",  "tuning",     S, "_baSortLyricsList")}
        ${_baSortTh("Snippet", "snippet",    S, "_baSortLyricsList")}
        <th style="width:1%"></th>
      </tr></thead>
      <tbody>${rows.map(_baLyricRowHtml).join("")}</tbody>
    </table>`;
}

function _baRenderLyricsPager() {
  const el = document.getElementById("blues-archive-lyrics-pager");
  if (!el) return;
  const pageCount = Math.max(1, Math.ceil(_baLyricsTotal / _BA_LYRICS_LIMIT));
  const cur = _baLyricsPage + 1;
  if (pageCount <= 1) { el.innerHTML = ""; return; }
  el.innerHTML = `
    <button class="archive-btn" ${cur <= 1 ? "disabled" : ""} onclick="_baLyricsGoToPage(${_baLyricsPage - 1})">‹ Prev</button>
    <span style="color:var(--muted)">Page ${cur} / ${pageCount}</span>
    <button class="archive-btn" ${cur >= pageCount ? "disabled" : ""} onclick="_baLyricsGoToPage(${_baLyricsPage + 1})">Next ›</button>
  `;
}

function _baLyricsGoToPage(p) {
  _baLyricsPage = Math.max(0, p);
  _baLoadLyrics();
}
window._baLyricsGoToPage = _baLyricsGoToPage;

// ── Stats strip ──────────────────────────────────────────────────────
// Curation dashboard chips at the top of the archive list view. Each
// chip is also a one-click shortcut: clicking "Orphan lyrics" jumps to
// the Lyrics sub-tab with the unmatched filter on, "Missing tuning"
// jumps with the tuning filter set to (unspecified), etc.
async function _baLoadStats() {
  const el = document.getElementById("blues-archive-stats");
  if (!el) return;
  try {
    const r = await apiFetch("/api/blues-archive/stats");
    if (!r.ok) { el.innerHTML = ""; return; }
    const s = await r.json();
    const chip = (label, n, opts = {}) => {
      const click = opts.onclick ? ` onclick="${opts.onclick}"` : "";
      const cur = opts.onclick ? "cursor:pointer;" : "";
      const tone = opts.tone === "warn" ? "color:#e8a85a" : "color:var(--muted)";
      // title= tooltip on every chip so the hover spells out what
      // the bucket means + (for clickable ones) what the click does.
      return `<span class="ba-stat-chip"${click} title="${escHtml(opts.title || label)}" style="${cur}padding:0.25rem 0.5rem;border:1px solid var(--border);border-radius:999px;${tone}">${escHtml(label)}: <strong style="color:var(--text)">${(n || 0).toLocaleString()}</strong></span>`;
    };
    el.innerHTML = [
      chip("Artists", s.artists_total, {
        onclick: "_baJumpArtists('')",
        title: "Total rows in the blues_artists table. Click to view all artists (clears any active category filter).",
      }),
      chip("With lyrics + releases", s.artists_with_both, {
        onclick: "_baJumpArtists('with_both')",
        title: "Artists with at least one lyric (linked or name-matched) AND at least one Discogs release stored. Click to filter the Artists tab.",
      }),
      chip("With lyrics only", s.artists_with_lyrics - s.artists_with_both, {
        onclick: "_baJumpArtists('with_lyrics_only')",
        title: "Artists with lyrics but no Discogs releases stored. Click to filter the Artists tab — use 'Get all info from Discogs' on /admin Blues DB to enrich.",
      }),
      chip("With releases only", s.artists_with_releases - s.artists_with_both, {
        onclick: "_baJumpArtists('with_releases_only')",
        title: "Artists with releases but no matched lyrics yet. Click to filter the Artists tab. Usually a name mismatch with the lyrics table.",
      }),
      chip("Empty", s.artists_empty, {
        tone: "warn",
        onclick: "_baJumpArtists('empty')",
        title: "Artists with neither lyrics nor releases — candidates for purge or enrichment. Click to filter the Artists tab.",
      }),
      chip("Lyrics total", s.lyrics_total, {
        onclick: "_baJumpAllLyrics()",
        title: "Total rows in the blues_lyrics table (scraped from weeniecampbell.com). Click to view all lyrics (clears any active filter).",
      }),
      chip("Orphan lyrics", s.lyrics_orphan, {
        tone: "warn",
        onclick: "_baJumpOrphans()",
        title: "Lyrics whose artist string doesn't link to a blues_artists row (artist_id IS NULL). Click to jump to the Lyrics tab with the unmatched filter on.",
      }),
      chip("Missing tuning", s.lyrics_missing_tuning, {
        tone: "warn",
        onclick: "_baJumpMissingTuning()",
        title: "Lyrics with no tuning extracted from the wiki page (tuning IS NULL). Click to jump to the Lyrics tab pre-filtered to '(unspecified)'.",
      }),
    ].join("");
  } catch { el.innerHTML = ""; }
}

// Jump handlers used by the stats-strip chips on the Artists side.
// Set the category filter + switch to the Artists tab.
function _baJumpArtists(category) {
  _baListCategory = category || "";
  _baPage = 0;
  _baSwitchSubtab("artists");
  _baLoadList();
}
window._baJumpArtists = _baJumpArtists;

function _baJumpAllLyrics() {
  // Clear every lyrics-side filter and switch to the Lyrics tab.
  _baLyricsClearFilters();
  _baSwitchSubtab("lyrics");
}
window._baJumpAllLyrics = _baJumpAllLyrics;

// Active-filter indicator on the Artists tab. Renders nothing when
// no category is set; otherwise a labeled chip with an × that clears
// the filter on click. Labels mirror the stats-strip chip names.
const _BA_CATEGORY_LABEL = {
  "with_both":          "With lyrics + releases",
  "with_lyrics_only":   "With lyrics only",
  "with_releases_only": "With releases only",
  "empty":              "Empty (no lyrics or releases)",
};
function _baRenderArtistsFilterIndicator() {
  const el = document.getElementById("blues-archive-artists-filter");
  if (!el) return;
  if (!_baListCategory) { el.innerHTML = ""; return; }
  const label = _BA_CATEGORY_LABEL[_baListCategory] || _baListCategory;
  el.innerHTML = `<span style="display:inline-flex;align-items:center;gap:0.4rem;padding:0.25rem 0.6rem;border:1px solid var(--accent);border-radius:999px;font-size:0.78rem;color:var(--accent)">Filter: ${escHtml(label)} <a href="#" onclick="event.preventDefault();_baClearArtistsCategory()" title="Clear filter — show all artists" style="color:var(--accent);text-decoration:none">×</a></span>`;
}

function _baClearArtistsCategory() {
  _baListCategory = "";
  _baPage = 0;
  _baLoadList();
}
window._baClearArtistsCategory = _baClearArtistsCategory;

// Lyrics CSV export — backed by a dedicated server endpoint that dumps
// the entire blues_lyrics table. Same blob-download pattern as the
// artists export (apiFetch to carry the bearer token, then trigger a
// client-side anchor click on an object URL).
// "Re-link orphans" sweep. Calls the server endpoint that re-matches
// every orphan lyric to a blues_artists row using two strategies:
//   1. exact LOWER(TRIM(artist)) = LOWER(name)
//   2. " (N)" Discogs disambiguator stripped from EITHER side
// Reports the count + refreshes the list and stats so the user sees
// the orphan count drop immediately.
async function _baRelinkOrphans() {
  if (!confirm("Sweep orphan lyrics and link them to existing artists when names match (with or without ' (N)' disambiguators)?")) return;
  try {
    const r = await apiFetch("/api/blues-archive/lyrics/relink-orphans", { method: "POST", timeoutMs: 60000 });
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const j = await r.json();
    alert(`Linked ${j.linked.toLocaleString()} orphan${j.linked === 1 ? "" : "s"}.`);
    _baLoadStats().catch(() => {});
    if (_baSubtab === "lyrics") _baLoadLyrics();
  } catch (e) {
    alert("Re-link failed: " + (e?.message || e));
  }
}
window._baRelinkOrphans = _baRelinkOrphans;

async function _baExportLyricsCsv() {
  const btn = document.getElementById("blues-export-lyrics-btn");
  const orig = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "Exporting…"; }
  try {
    const r = await apiFetch("/api/admin/lyrics/export.csv");
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const blob = await r.blob();
    const fname = `seadisco-lyrics-${new Date().toISOString().slice(0, 10)}.csv`;
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = fname;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 5000);
  } catch (e) {
    alert("Export failed: " + (e?.message || e));
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = orig; }
  }
}
window._baExportLyricsCsv = _baExportLyricsCsv;

// Single dispatcher for every admin-only action that's powered by
// /blues-admin.js. Each kind maps to a function name; we lazy-load
// the module on first call, then invoke. Keeps the inline onclick
// attributes short and centralizes the lazy-load plumbing.
function _baAdminAction(kind, ev) {
  const map = {
    addArtist:        () => window.bluesDbOpenEditor?.(null),
    enrichDiscogsFull:() => window.bluesDbEnrichDiscogsFull?.(null),
    // Two exports now: artists (with the discogs_releases JSONB
    // column embedded as JSON) and lyrics (separate dump of the
    // blues_lyrics table). Old `exportCsv` kept as an alias so any
    // stale callers don't 404.
    exportArtistsCsv: () => window.bluesDbExportCsv?.(),
    exportCsv:        () => window.bluesDbExportCsv?.(),
    exportLyricsCsv:  () => _baExportLyricsCsv(),
    deleteAll:        () => window.bluesDbDeleteAll?.(),
    lyricsScrape:     () => window.lyricsStartScrape?.(),
    lyricsStop:       () => window.lyricsStopScrape?.(),
    lyricsReextract:  () => window.lyricsReextract?.(),
    lyricsSyncArtists:() => window.lyricsSyncArtists?.(ev),
  };
  const fn = map[kind];
  if (!fn) return;
  const tryOnce = () => {
    try { fn(); } catch (e) { console.warn("[_baAdminAction]", kind, e); }
  };
  // Already loaded? Just run.
  if (typeof window.bluesDbOpenEditor === "function") return tryOnce();
  if (typeof window._sdLoadModule === "function") {
    window._sdLoadModule("/blues-admin.js")
      .then(tryOnce)
      .catch(err => alert("Couldn't load admin module: " + (err?.message || err)));
  } else {
    alert("Admin module not available.");
  }
}
window._baAdminAction = _baAdminAction;

// Open the full /admin-style editor (25 fields + enrichment buttons)
// in place. The editor JS lives in /blues-admin.js — lazy-loaded so
// non-admin sessions never pay for it. The editor overlay DOM is
// embedded in index.html as #blues-editor-overlay and friends.
// On save, the editor fires window._bluesDbAfterSaveOnce so we can
// refresh the artist popup that was open behind it.
function _baOpenFullEditor(id) {
  // Set the one-shot post-save callback BEFORE opening. The Discovery
  // artist popup is what's open behind the editor overlay; refreshing
  // it re-renders the bio / dates / photo / lyrics with any edits.
  window._bluesDbAfterSaveOnce = (savedId) => {
    const targetId = Number(savedId || id);
    if (Number.isFinite(targetId)) _baOpenArtist(targetId);
  };
  const openIt = () => {
    if (typeof window.bluesDbOpenEditor === "function") {
      window.bluesDbOpenEditor(id);
      return true;
    }
    return false;
  };
  if (openIt()) return;
  if (typeof window._sdLoadModule === "function") {
    window._sdLoadModule("/blues-admin.js")
      .then(() => { if (!openIt()) alert("Editor not available."); })
      .catch(err => alert("Couldn't load editor: " + (err?.message || err)));
  } else {
    alert("Editor not available.");
  }
}
window._baOpenFullEditor = _baOpenFullEditor;

function _baJumpOrphans() {
  _baSwitchSubtab("lyrics");
  const cb = document.getElementById("blues-archive-lyrics-unmatched");
  if (cb && !cb.checked) { cb.checked = true; _baLyricsApplyUnmatched(); }
}
window._baJumpOrphans = _baJumpOrphans;

function _baJumpMissingTuning() {
  _baSwitchSubtab("lyrics");
  setTimeout(() => {
    const sel = document.getElementById("blues-archive-lyrics-tuning");
    if (sel) {
      // Make sure the dropdown is populated, then pick the sentinel.
      if (![...sel.options].some(o => o.value === "(unspecified)")) _baLoadTunings().then(() => {
        sel.value = "(unspecified)";
        _baLyricsApplyTuning();
      });
      else {
        sel.value = "(unspecified)";
        _baLyricsApplyTuning();
      }
    }
  }, 50);
}
window._baJumpMissingTuning = _baJumpMissingTuning;

// ── Recent edits feed ────────────────────────────────────────────────
async function _baLoadRecent() {
  const el = document.getElementById("blues-archive-recent");
  if (!el) return;
  try {
    const r = await apiFetch("/api/blues-archive/recent?limit=20");
    if (!r.ok) { el.innerHTML = ""; return; }
    const { rows = [] } = await r.json();
    if (!rows.length) { el.innerHTML = `<span style="color:var(--muted);font-style:italic">No edits yet.</span>`; return; }
    el.innerHTML = rows.map(row => {
      const when = row.updated_at ? new Date(row.updated_at).toLocaleString() : "";
      if (row.kind === "artist") {
        return `<div style="padding:0.2rem 0;display:flex;gap:0.5rem;align-items:center"><span style="color:var(--accent);font-size:0.76rem">ARTIST</span><a href="#" onclick="event.preventDefault();_baOpenArtist(${row.id})" style="color:var(--text);text-decoration:none">${escHtml(row.title)}</a><span style="color:#666;font-size:0.74rem;margin-left:auto">${escHtml(when)}</span></div>`;
      }
      return `<div style="padding:0.2rem 0;display:flex;gap:0.5rem;align-items:center"><span style="color:#7eb8da;font-size:0.76rem">LYRIC</span><a href="#" onclick="event.preventDefault();_baOpenLyric(${row.id})" style="color:var(--text);text-decoration:none">${escHtml(row.title)}</a>${row.artist_name ? `<span style="color:var(--muted);font-size:0.76rem">· ${escHtml(row.artist_name)}</span>` : ""}<span style="color:#666;font-size:0.74rem;margin-left:auto">${escHtml(when)}</span></div>`;
    }).join("");
  } catch { el.innerHTML = ""; }
}

// ── Promote orphan lyric to a new artist ─────────────────────────────
// Tuning-chip click on the artist popup: switch to the Lyrics tab and
// pre-filter to (artist, tuning).
function _baJumpTuningForArtist(tuning, artistName) {
  // Close the artist overlay so the lyrics list is visible.
  _baBackToList();
  _baSwitchSubtab("lyrics");
  setTimeout(() => {
    const searchEl = document.getElementById("blues-archive-lyrics-search");
    const tuningEl = document.getElementById("blues-archive-lyrics-tuning");
    if (searchEl) searchEl.value = artistName || "";
    const applyTuning = () => {
      if (tuningEl) {
        if (![...tuningEl.options].some(o => o.value === tuning)) {
          // Tuning may not yet be in the dropdown (lazy load). Fall back
          // to just setting the underlying state and reloading.
          _baLyricsTuning = tuning || "";
        } else {
          tuningEl.value = tuning || "";
          _baLyricsTuning = tuningEl.value;
        }
      }
      _baLyricsPage = 0;
      _baLoadLyrics();
    };
    if (!_baLyricsTuningsLoaded) _baLoadTunings().then(applyTuning); else applyTuning();
  }, 50);
}
window._baJumpTuningForArtist = _baJumpTuningForArtist;

// ── Bulk reassign picker ─────────────────────────────────────────────
// Two ways to specify the source: either pick an existing artist
// (lyrics with that artist_id) or type a free-text name (matches
// LOWER(TRIM(artist)) for unmigrated rows). Server endpoint runs the
// reassignment in a single transaction.
async function _baOpenReassignPicker(toId, toName) {
  let overlay = document.getElementById("ba-reassign-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "ba-reassign-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "310", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
    });
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(600px,100%)">
      <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.6rem">
        <h3 style="margin:0">Reassign lyrics → <em>${escHtml(toName)}</em></h3>
        <button class="archive-btn" onclick="document.getElementById('ba-reassign-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <p style="font-size:0.78rem;color:var(--muted);margin:0 0 0.6rem">
        Source artist's lyrics get re-keyed to <em>${escHtml(toName)}</em>.
        The source row is <strong>not</strong> deleted (use Merge for that).
      </p>
      <label style="display:block;margin:0.4rem 0 0.3rem;font-size:0.82rem;color:var(--muted)">By artist name (matches lyrics whose artist string matches, even when not yet linked)</label>
      <input id="ba-reassign-from-name" type="text" placeholder="e.g. Robert Jonson" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)">OR by existing artist row (matches lyrics whose artist_id = this row's id)</label>
      <input id="ba-reassign-from-search" type="search" placeholder="Type to filter artists…" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem" oninput="_baReassignPickerSearch(${toId})">
      <div id="ba-reassign-results" style="max-height:30vh;overflow:auto;border:1px solid var(--border);border-radius:4px;padding:0.4rem 0.6rem;font-size:0.84rem;margin-top:0.3rem"></div>
      <div style="display:flex;gap:0.5rem;justify-content:flex-end;margin-top:1rem">
        <button class="archive-btn" onclick="document.getElementById('ba-reassign-overlay')?.remove()">Cancel</button>
        <button class="archive-btn archive-btn-suggest" onclick="_baConfirmReassign(${toId}, ${JSON.stringify(toName || '').replace(/"/g, '&quot;')})">Reassign</button>
      </div>
      <div id="ba-reassign-status" style="font-size:0.76rem;color:var(--muted);margin-top:0.5rem;min-height:1em"></div>
    </div>
  `;
  setTimeout(() => document.getElementById("ba-reassign-from-name")?.focus(), 50);
}
window._baOpenReassignPicker = _baOpenReassignPicker;

let _baReassignPickerTimer = null;
let _baReassignFromId = null;
function _baReassignPickerSearch(toId) {
  if (_baReassignPickerTimer) clearTimeout(_baReassignPickerTimer);
  _baReassignPickerTimer = setTimeout(() => _baReassignPickerLoad(toId), 240);
}
window._baReassignPickerSearch = _baReassignPickerSearch;

async function _baReassignPickerLoad(toId) {
  const q = (document.getElementById("ba-reassign-from-search")?.value || "").trim();
  const el = document.getElementById("ba-reassign-results");
  if (!el) return;
  if (!q) { el.innerHTML = `<span style="color:var(--muted)">Type to filter…</span>`; _baReassignFromId = null; return; }
  el.textContent = "Loading…";
  try {
    const p = new URLSearchParams(); p.set("q", q); p.set("limit", "30");
    const r = await apiFetch(`/api/blues-archive/artists?${p}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const { rows = [] } = await r.json();
    const choices = rows.filter(row => row.id !== toId);
    if (!choices.length) { el.innerHTML = `<span style="color:var(--muted)">No matches.</span>`; return; }
    el.innerHTML = choices.map(row => `
      <div onclick="_baReassignFromId=${row.id};document.querySelectorAll('#ba-reassign-results > div').forEach(d=>d.style.background='');this.style.background='rgba(255,255,255,0.07)'" style="cursor:pointer;padding:0.3rem 0.4rem;border-bottom:1px solid rgba(255,255,255,0.04);display:flex;justify-content:space-between;gap:0.6rem">
        <span>${escHtml(row.name || "")}</span>
        <span style="color:var(--muted);font-size:0.76rem">${row.lyrics_count || 0}L</span>
      </div>`).join("");
  } catch (e) {
    el.innerHTML = `<span style="color:#e88">Failed: ${escHtml(String(e?.message || e))}</span>`;
  }
}

async function _baConfirmReassign(toId, toName) {
  const fromArtistName = (document.getElementById("ba-reassign-from-name")?.value || "").trim();
  const fromArtistId = _baReassignFromId;
  if (!fromArtistName && !fromArtistId) {
    const s = document.getElementById("ba-reassign-status");
    if (s) s.textContent = "Pick a source artist or type a name.";
    return;
  }
  const desc = [];
  if (fromArtistId) desc.push(`artist row #${fromArtistId}`);
  if (fromArtistName) desc.push(`name "${fromArtistName}"`);
  if (!confirm(`Reassign all lyrics matching ${desc.join(" and ")} to "${toName}"?`)) return;
  const s = document.getElementById("ba-reassign-status");
  if (s) s.textContent = "Reassigning…";
  try {
    const r = await apiFetch("/api/blues-archive/lyrics/reassign", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ toArtistId: toId, fromArtistId, fromArtistName: fromArtistName || null }),
    });
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const j = await r.json();
    document.getElementById("ba-reassign-overlay")?.remove();
    _baReassignFromId = null;
    // Refresh the artist popup so the new lyric rows show up.
    if (_baCurrentArtistId != null) _baOpenArtist(_baCurrentArtistId);
    _baLoadStats().catch(() => {});
    alert(`Reassigned ${j.reassigned} lyric${j.reassigned === 1 ? "" : "s"} to "${j.toName}".`);
  } catch (e) {
    if (s) s.textContent = `Reassign failed: ${e?.message || e}`;
  }
}
window._baConfirmReassign = _baConfirmReassign;

// Click handler for the "+ artist" link on orphan lyric rows. Opens a
// picker so the user can EITHER link the orphan to an existing artist
// (the common case: "John Lee" → John Lee Hooker) OR fall back to
// creating a new artist row from the orphan string. Pre-fills the
// search with the orphan name so existing matches show up immediately.
async function _baPromoteOrphan(lyricId) {
  // Pull the lyric to get its current artist string. Without this,
  // we'd have no name to pre-fill or to use as the bulk-reassign key.
  let row;
  try {
    const r = await apiFetch(`/api/admin/lyrics/${lyricId}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    row = await r.json();
  } catch (e) {
    alert(`Couldn't load lyric: ${e?.message || e}`);
    return;
  }
  const orphanName = String(row.artist || "").trim();
  if (!orphanName) {
    alert("This lyric has no artist string to link.");
    return;
  }
  let overlay = document.getElementById("ba-orphan-picker-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "ba-orphan-picker-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "320", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
    });
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }
  const safeName = orphanName.replace(/"/g, "&quot;").replace(/'/g, "\\'");
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(620px,100%)">
      <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.6rem">
        <h3 style="margin:0">Link <em>${escHtml(orphanName)}</em> to an artist</h3>
        <button class="archive-btn" onclick="document.getElementById('ba-orphan-picker-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <p style="font-size:0.78rem;color:var(--muted);margin:0 0 0.6rem">
        Pick an existing artist row to take this orphan (and, when the checkbox is on, every other orphan with the same name).
        If no existing artist fits, use the "Create new" button at the bottom.
      </p>
      <input id="ba-orphan-picker-search" type="search" value="${escHtml(orphanName)}" placeholder="Type to filter artists…" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem;margin-bottom:0.5rem" oninput="_baOrphanPickerSearch(${lyricId})">
      <div id="ba-orphan-picker-results" style="max-height:40vh;overflow:auto;border:1px solid var(--border);border-radius:4px;padding:0.4rem 0.6rem;font-size:0.84rem"></div>
      <label style="display:flex;align-items:center;gap:0.4rem;margin-top:0.6rem;font-size:0.82rem;color:var(--muted)" title="When on, every orphan lyric whose artist string matches the orphan name (case-insensitive) gets reassigned in the same transaction. Off = only this lyric.">
        <input id="ba-orphan-picker-bulk" type="checkbox" checked>
        Also link all other orphans named "${escHtml(orphanName)}"
      </label>
      <div style="display:flex;gap:0.5rem;justify-content:space-between;margin-top:1rem;align-items:center;flex-wrap:wrap">
        <button class="archive-btn" onclick="_baOrphanCreateAsNew(${lyricId})" title="Fallback: create a brand-new blues_artists row using the orphan name as-is and link this lyric (plus siblings) to it.">+ Create "${escHtml(orphanName)}" as new artist</button>
        <button class="archive-btn" onclick="document.getElementById('ba-orphan-picker-overlay')?.remove()">Cancel</button>
      </div>
      <div id="ba-orphan-picker-status" style="font-size:0.76rem;color:var(--muted);margin-top:0.5rem;min-height:1em"></div>
    </div>
  `;
  // Auto-run the initial search using the pre-filled name so matches
  // show up without the user having to type anything.
  setTimeout(() => {
    document.getElementById("ba-orphan-picker-search")?.focus();
    _baOrphanPickerSearch(lyricId);
  }, 30);
}
window._baPromoteOrphan = _baPromoteOrphan;

let _baOrphanPickerTimer = null;
function _baOrphanPickerSearch(lyricId) {
  if (_baOrphanPickerTimer) clearTimeout(_baOrphanPickerTimer);
  _baOrphanPickerTimer = setTimeout(() => _baOrphanPickerLoad(lyricId), 220);
}
window._baOrphanPickerSearch = _baOrphanPickerSearch;

async function _baOrphanPickerLoad(lyricId) {
  const q = (document.getElementById("ba-orphan-picker-search")?.value || "").trim();
  const el = document.getElementById("ba-orphan-picker-results");
  if (!el) return;
  if (!q) { el.innerHTML = `<span style="color:var(--muted)">Type to filter…</span>`; return; }
  el.textContent = "Loading…";
  try {
    const p = new URLSearchParams(); p.set("q", q); p.set("limit", "40");
    const r = await apiFetch(`/api/blues-archive/artists?${p}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const { rows = [] } = await r.json();
    if (!rows.length) {
      el.innerHTML = `<span style="color:var(--muted)">No matches. Use "+ Create" below to mint a new artist instead.</span>`;
      return;
    }
    el.innerHTML = rows.map(row => `
      <div onclick="_baOrphanPickerConfirm(${lyricId}, ${row.id}, ${JSON.stringify(row.name || "").replace(/"/g, "&quot;")})" style="cursor:pointer;padding:0.35rem 0.4rem;border-bottom:1px solid rgba(255,255,255,0.04);display:flex;justify-content:space-between;gap:0.6rem" onmouseover="this.style.background='rgba(255,255,255,0.05)'" onmouseout="this.style.background=''">
        <span style="color:var(--text);font-weight:600">${escHtml(row.name || "")}</span>
        <span style="color:var(--muted);font-size:0.76rem;white-space:nowrap">${row.lyrics_count || 0}L · ${row.releases_count || 0}R${row.discogs_id ? ` · #${row.discogs_id}` : ""}</span>
      </div>`).join("");
  } catch (e) {
    el.innerHTML = `<span style="color:#e88">Failed: ${escHtml(String(e?.message || e))}</span>`;
  }
}

async function _baOrphanPickerConfirm(lyricId, toId, toName) {
  const bulk = !!document.getElementById("ba-orphan-picker-bulk")?.checked;
  const status = document.getElementById("ba-orphan-picker-status");
  // We need the orphan name even after the overlay closes, so capture
  // it before the request. It's the value of the search input — same
  // as what we pre-filled with.
  const orphanName = (document.getElementById("ba-orphan-picker-search")?.value || "").trim();
  if (!confirm(`Link "${orphanName}" to "${toName}"?\n\n${bulk ? `Every orphan lyric with artist = "${orphanName}" will be reassigned.` : "Only this one lyric will be reassigned."}`)) return;
  if (status) status.textContent = "Linking…";
  try {
    let summary;
    if (bulk) {
      // Bulk path — use the existing reassign endpoint keyed by name.
      const r = await apiFetch("/api/blues-archive/lyrics/reassign", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ toArtistId: toId, fromArtistName: orphanName }),
      });
      if (!r.ok) {
        const txt = await r.text().catch(() => "");
        throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
      }
      const j = await r.json();
      summary = `Reassigned ${j.reassigned} lyric${j.reassigned === 1 ? "" : "s"} → ${j.toName}`;
    } else {
      // Single-lyric path — PATCH this row's artist_id directly. Also
      // overwrite the artist string with the target name so the
      // display matches the canonical artist row.
      const r = await apiFetch(`/api/admin/lyrics/${lyricId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ artist: toName, artist_id: toId }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const updated = await r.json().catch(() => null);
      if (updated) {
        const i = _baLyricsRowsCache.findIndex(x => Number(x.id) === Number(updated.id));
        if (i >= 0) _baLyricsRowsCache[i] = { ..._baLyricsRowsCache[i], ...updated };
        _baPatchLyricRowEverywhere(updated);
      }
      summary = `Linked this lyric to ${toName}.`;
    }
    document.getElementById("ba-orphan-picker-overlay")?.remove();
    _baLoadStats().catch(() => {});
    // Bulk mode changed many rows the local cache doesn't know about
    // — just reload the lyrics list. Scroll position is restored
    // automatically by _baLoadLyrics.
    if (bulk && _baSubtab === "lyrics") _baLoadLyrics();
    alert(summary);
  } catch (e) {
    if (status) status.textContent = `Failed: ${e?.message || e}`;
  }
}
window._baOrphanPickerConfirm = _baOrphanPickerConfirm;

// "+ Create as new artist" fallback inside the orphan picker — calls
// the existing promote endpoint that creates the row + links siblings.
async function _baOrphanCreateAsNew(lyricId) {
  const orphanName = (document.getElementById("ba-orphan-picker-search")?.value || "").trim();
  if (!orphanName) return;
  if (!confirm(`Create a new blues_artists row named "${orphanName}" and link all orphan lyrics with that name?`)) return;
  const status = document.getElementById("ba-orphan-picker-status");
  if (status) status.textContent = "Creating…";
  try {
    const r = await apiFetch(`/api/blues-archive/lyrics/${lyricId}/promote-to-artist`, { method: "POST" });
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const j = await r.json();
    document.getElementById("ba-orphan-picker-overlay")?.remove();
    if (_baSubtab === "lyrics") _baLoadLyrics();
    _baLoadStats().catch(() => {});
    alert(`Created "${j.artistName}". ${j.lyricsLinked} lyric${j.lyricsLinked === 1 ? "" : "s"} linked.`);
  } catch (e) {
    if (status) status.textContent = `Failed: ${e?.message || e}`;
  }
}
window._baOrphanCreateAsNew = _baOrphanCreateAsNew;
