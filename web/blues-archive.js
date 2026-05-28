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
  params.set("limit", String(_BA_LIMIT));
  params.set("offset", String(_baPage * _BA_LIMIT));
  rowsEl.textContent = "Loading…";
  try {
    const r = await apiFetch(`/api/blues-archive/artists?${params}`);
    if (!r.ok) { rowsEl.innerHTML = `<p style="color:#e88">Failed: HTTP ${r.status}</p>`; return; }
    const { rows = [], total = 0 } = await r.json();
    _baTotal = total;
    if (countEl) countEl.textContent = total ? `${total.toLocaleString()} artist${total === 1 ? "" : "s"}` : "No artists yet.";
    _baListRowsCache = rows;
    _baRenderListTable();
    _baRenderPager();
  } catch (e) {
    rowsEl.innerHTML = `<p style="color:#e88">Failed: ${escHtml(String(e?.message || e))}</p>`;
  }
}

function _baSortList(key) {
  _baToggleSort(_baListSort, key);
  _baRenderListTable();
}
window._baSortList = _baSortList;

function _baRenderListTable() {
  const rowsEl = document.getElementById("blues-archive-rows");
  if (!rowsEl) return;
  const rows = _baSortApply(_baListRowsCache, _baListSort, _BA_LIST_TYPES);
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
      <span style="margin-left:auto;display:inline-flex;gap:0.4rem">
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
    overlay.innerHTML = `
      <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(820px,100%)">
        <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.6rem">
          <div style="min-width:0">
            <h3 style="margin:0 0 0.25rem">${escHtml(row.page_title || "")}</h3>
            ${row.tuning ? `<div style="font-size:0.78rem;color:var(--muted)">Tuning: ${escHtml(row.tuning)}</div>` : ""}
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
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)">Title</label>
      <input id="ba-edit-title" type="text" value="${escHtml(row.page_title || "")}" placeholder="(required)" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)">Artist</label>
      <input id="ba-edit-artist" type="text" value="${escHtml(row.artist || "")}" placeholder="(leave blank to clear)" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)">Tuning</label>
      <input id="ba-edit-tuning" type="text" value="${escHtml(row.tuning || "")}" list="ba-tuning-options" placeholder="(leave blank to clear)" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
      <div style="display:flex;gap:0.5rem;justify-content:flex-end;margin-top:1rem">
        <button class="archive-btn" onclick="document.getElementById('ba-lyric-edit-overlay')?.remove()">Cancel</button>
        <button class="archive-btn archive-btn-suggest" onclick="_baSaveLyricEdit(${id})">Save</button>
      </div>
      <div id="ba-edit-status" style="font-size:0.76rem;color:var(--muted);margin-top:0.5rem;min-height:1em"></div>
    </div>
  `;
}
window._baOpenLyricEditor = _baOpenLyricEditor;

async function _baSaveLyricEdit(id) {
  const statusEl   = document.getElementById("ba-edit-status");
  const artist     = document.getElementById("ba-edit-artist")?.value ?? "";
  const tuning     = document.getElementById("ba-edit-tuning")?.value ?? "";
  const page_title = (document.getElementById("ba-edit-title")?.value ?? "").trim();
  if (!page_title) { if (statusEl) statusEl.textContent = "Title is required."; return; }
  if (statusEl) statusEl.textContent = "Saving…";
  try {
    const r = await apiFetch(`/api/admin/lyrics/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ artist, tuning, page_title }),
    });
    if (!r.ok) {
      // 409 = title already taken on this source_host. Surface the
      // server's reason so the user knows what to do; other failures
      // bubble up as a generic HTTP message.
      if (r.status === 409) throw new Error("Another lyric already has that title on this source.");
      throw new Error(`HTTP ${r.status}`);
    }
    document.getElementById("ba-lyric-edit-overlay")?.remove();
    // Repaint anything currently showing this row. The viewer popup
    // (if open) is reopened with fresh data so the user sees the
    // updated title/tuning without closing+reopening manually.
    const viewerOpen = !!document.getElementById("ba-lyric-overlay");
    if (viewerOpen) {
      document.getElementById("ba-lyric-overlay")?.remove();
      _baOpenLyric(id);
    }
    // Refresh the artist detail so the row repaints with new values.
    if (_baCurrentArtistId != null) _baOpenArtist(_baCurrentArtistId);
    // Refresh the master lyrics list if it's loaded.
    if (_baSubtab === "lyrics") _baLoadLyrics();
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
  if (q)               params.set("q", q);
  if (_baLyricsTuning) params.set("tuning", _baLyricsTuning);
  params.set("limit",  String(_BA_LYRICS_LIMIT));
  params.set("offset", String(_baLyricsPage * _BA_LYRICS_LIMIT));
  rowsEl.textContent = "Loading…";
  try {
    const r = await apiFetch(`/api/admin/lyrics?${params}`);
    if (!r.ok) { rowsEl.innerHTML = `<p style="color:#e88">Failed: HTTP ${r.status}</p>`; return; }
    const { rows = [], total = 0 } = await r.json();
    _baLyricsTotal = total;
    if (countEl) countEl.textContent = total ? `${total.toLocaleString()} lyric${total === 1 ? "" : "s"}` : "No matches.";
    _baLyricsRowsCache = rows;
    _baRenderLyricsTable();
    _baRenderLyricsPager();
  } catch (e) {
    rowsEl.innerHTML = `<p style="color:#e88">Failed: ${escHtml(String(e?.message || e))}</p>`;
  }
}

function _baSortLyricsList(key) {
  _baToggleSort(_baLyricsListSort, key);
  _baRenderLyricsTable();
}
window._baSortLyricsList = _baSortLyricsList;

function _baRenderLyricsTable() {
  const rowsEl = document.getElementById("blues-archive-lyrics-rows");
  if (!rowsEl) return;
  const rows = _baSortApply(_baLyricsRowsCache, _baLyricsListSort, _BA_LYRICS_LIST_TYPES);
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
      <tbody>${rows.map(l => {
        // Title + artist are both lookup-popup triggers (title = track
        // scope, artist = artist scope). Clicking elsewhere on the row
        // opens the lyric viewer overlay (_baOpenLyric).
        const titleHtml = (typeof entityLookupLinkHtml === "function" && l.page_title)
          ? entityLookupLinkHtml("track", l.page_title, { trackArtist: l.artist || "", title: `Lookup options for "${l.page_title}"` })
          : escHtml(l.page_title || "");
        const artistHtml = l.artist && typeof entityLookupLinkHtml === "function"
          ? entityLookupLinkHtml("artist", l.artist, { title: `Lookup options for "${l.artist}"` })
          : escHtml(l.artist || "");
        return `<tr data-lyric-row="${l.id}">
          <td style="font-weight:600;color:var(--text);white-space:nowrap">${titleHtml}</td>
          <td style="color:var(--muted);white-space:nowrap">${artistHtml}</td>
          <td style="white-space:nowrap;color:var(--accent);cursor:pointer" onclick="_baOpenLyric(${l.id})">${escHtml(l.tuning || "")}</td>
          <td style="font-size:0.76rem;color:#888;cursor:pointer" onclick="_baOpenLyric(${l.id})">${escHtml((l.snippet || "").replace(/\s+/g, " ").slice(0, 140))}…</td>
          <td style="text-align:right"><a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenLyricEditor(${l.id})" style="color:var(--muted);text-decoration:none;font-size:0.78rem" title="Edit tuning / artist on this lyric">✎</a></td>
        </tr>`;
      }).join("")}</tbody>
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
