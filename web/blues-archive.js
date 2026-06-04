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
const _BA_LIST_TYPES = { name: "str", discogs_id: "num", has_photo: "num", first_release_year: "num", lyrics_count: "num", releases_count: "num" };

let _baDetailArtist = null;
const _baLyricsSort = { key: "page_title", dir: "asc" };
const _BA_LYRICS_TYPES = { page_title: "str", tuning: "str", snippet: "str", scraped_at: "date" };

// Page-state persistence — subtab + lyrics filters + scroll. Saved on
// every relevant change and on subtab switch; restored when the user
// returns to the Blues Archive view (initBluesArchiveView reads this
// and routes through _baSwitchSubtab + populates the lyrics inputs).
// Capped by age (1 hour) so a stale state from yesterday doesn't pull
// you back to a long-forgotten filter on next session.
const _BA_VIEW_STATE_KEY = "sd_ba_view_state";
const _BA_VIEW_STATE_MAX_AGE_MS = 60 * 60 * 1000;
let _baStateScrollTimer = null;
function _baPersistViewState() {
  try {
    const state = {
      at: Date.now(),
      subtab: _baSubtab,
      lyrics: _baSubtab === "lyrics" ? {
        q:         document.getElementById("blues-archive-lyrics-search")?.value || "",
        tuning:    _baLyricsTuning,
        unmatched: _baLyricsUnmatched,
        unpinned:  _baLyricsUnpinned,
        empty:     _baLyricsEmpty,
        page:      _baLyricsPage,
        sort:      { key: _baLyricsListSort.key, dir: _baLyricsListSort.dir },
        scrollY:   window.scrollY,
      } : null,
    };
    localStorage.setItem(_BA_VIEW_STATE_KEY, JSON.stringify(state));
  } catch {}
}
function _baReadViewState() {
  try {
    const raw = localStorage.getItem(_BA_VIEW_STATE_KEY);
    if (!raw) return null;
    const s = JSON.parse(raw);
    if (!s || typeof s !== "object") return null;
    if (Date.now() - Number(s.at || 0) > _BA_VIEW_STATE_MAX_AGE_MS) return null;
    return s;
  } catch { return null; }
}
// Debounced scroll listener — only persists while the user is actually
// on the Blues Archive lyrics tab. Cheap when the tab isn't visible
// (returns early) so the global listener is harmless elsewhere.
if (typeof window !== "undefined") {
  window.addEventListener("scroll", () => {
    if (_baSubtab !== "lyrics") return;
    const lp = document.getElementById("blues-archive-lyrics-panel");
    if (!lp || lp.style.display === "none") return;
    if (_baStateScrollTimer) clearTimeout(_baStateScrollTimer);
    _baStateScrollTimer = setTimeout(_baPersistViewState, 400);
  }, { passive: true });
}

// Visited-lyric tracker. localStorage-backed Set so the snippet column
// dims for already-opened rows on revisit (like a browser's visited
// link colour). Cap at 5000 to bound the JSON blob. Mutations are
// pushed back to LS immediately so they survive reload.
const _BA_VISITED_LYRICS_KEY = "sd_ba_visited_lyrics";
const _BA_VISITED_LYRICS_MAX = 5000;
const _baVisitedLyrics = (() => {
  try {
    const raw = localStorage.getItem(_BA_VISITED_LYRICS_KEY);
    const arr = raw ? JSON.parse(raw) : [];
    return new Set(Array.isArray(arr) ? arr.map(n => Number(n)).filter(Number.isFinite) : []);
  } catch { return new Set(); }
})();
function _baMarkLyricVisited(id) {
  const n = Number(id);
  if (!Number.isFinite(n)) return;
  if (_baVisitedLyrics.has(n)) return; // already marked → no LS churn
  _baVisitedLyrics.add(n);
  // Drop oldest entries (insertion-order) once we blow past the cap.
  if (_baVisitedLyrics.size > _BA_VISITED_LYRICS_MAX) {
    const drop = _baVisitedLyrics.size - _BA_VISITED_LYRICS_MAX;
    const it = _baVisitedLyrics.values();
    for (let i = 0; i < drop; i++) _baVisitedLyrics.delete(it.next().value);
  }
  try { localStorage.setItem(_BA_VISITED_LYRICS_KEY, JSON.stringify([..._baVisitedLyrics])); } catch {}
  // Live-update any rows for this lyric currently on screen so the
  // visual change is immediate without a re-render.
  document.querySelectorAll(`tr[data-lyric-row="${n}"]`).forEach(tr => {
    tr.classList.add("ba-lyric-visited");
  });
}
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
  // Stats strip — fire non-blocking. Recent-edits feed removed from
  // the UI (the BE endpoint stays for future re-enablement).
  _baLoadStats().catch(() => {});
  // Warm the favorites cache so the star renders correct on the first
  // lyric viewer open. Non-blocking; default empty Set if it fails.
  _baLoadFavoriteIds().catch(() => {});
  // Auto-resume the Discogs-search worker poll if a job is already
  // running on the server (page reload mid-run shouldn't lose the UI).
  (async () => {
    try {
      const r = await apiFetch("/api/admin/lyrics/resolve-years-discogs/status");
      if (!r.ok) return;
      const job = await r.json();
      if (job.status === "running") _baPollYearJob();
    } catch {}
  })();
  // Lazy-load /blues-admin.js for admins so the bulk-Discogs job
  // status auto-attaches to anything already running on the server.
  // Non-critical path — silent on failure. The lyrics-scrape
  // auto-poll was removed: the corpus is stable, no scrape runs are
  // expected, and the polling loop wasn't self-cancelling cleanly
  // after the scrape UI was retired (cancel code referenced the now-
  // deleted #lyrics-scrape-btn, threw on null, the try/catch
  // swallowed it, and the 5s setInterval kept firing forever).
  if (window._isAdmin && typeof window._sdLoadModule === "function") {
    window._sdLoadModule("/blues-admin.js").then(() => {
      try { window._bluesResumeBulkIfRunning?.("blues-enrich-discogs-full-btn", "/api/admin/blues/enrich-discogs-full/status", "Get all info from Discogs"); } catch {}
    }).catch(() => { /* non-critical */ });
  }
  // Deep-link support: /?v=blues-archive&baArtist=ID opens the given
  // archive artist after the list view paints. Used by the 🎸 badge
  // on album/version modals to jump straight to a matched artist.
  // A URL deep-link wins over any persisted view-state so the badge
  // jump never lands you on the Lyrics tab from a stale session.
  let hasUrlDeepLink = false;
  try {
    const p = new URLSearchParams(window.location.search);
    const aid = parseInt(p.get("baArtist") || "", 10);
    if (Number.isFinite(aid) && aid > 0) {
      hasUrlDeepLink = true;
      setTimeout(() => _baOpenArtist(aid), 40);
    }
  } catch { /* non-fatal */ }
  // Restore the previously-active subtab + filters if the user just
  // bounced over to Search (or any other view) and came back. Only
  // honoured when no URL deep-link is active.
  if (!hasUrlDeepLink) {
    const saved = _baReadViewState();
    if (saved?.subtab === "lyrics" && saved.lyrics) {
      _baLyricsTuning    = String(saved.lyrics.tuning ?? "");
      _baLyricsUnmatched = !!saved.lyrics.unmatched;
      _baLyricsUnpinned  = !!saved.lyrics.unpinned;
      _baLyricsEmpty     = !!saved.lyrics.empty;
      _baLyricsPage      = Number(saved.lyrics.page) || 0;
      if (saved.lyrics.sort?.key) {
        _baLyricsListSort.key = saved.lyrics.sort.key;
        _baLyricsListSort.dir = saved.lyrics.sort.dir || "asc";
      }
      // DOM inputs — set what we can synchronously. The tuning <select>
      // gets populated asynchronously by _baLoadTunings; the module
      // var _baLyricsTuning is what _baLoadLyrics actually reads, so
      // the dropdown's stale value doesn't matter for the fetch.
      const sq = document.getElementById("blues-archive-lyrics-search");
      if (sq) sq.value = String(saved.lyrics.q ?? "");
      const un = document.getElementById("blues-archive-lyrics-unmatched");
      if (un) un.checked = !!saved.lyrics.unmatched;
      const up = document.getElementById("blues-archive-lyrics-unpinned");
      if (up) up.checked = !!saved.lyrics.unpinned;
      const em = document.getElementById("blues-archive-lyrics-empty");
      if (em) em.checked = !!saved.lyrics.empty;
      _baSwitchSubtab("lyrics");
      // Scroll restore — wait past the table render. RAF + ~250ms
      // settles the layout for long lists; clamp to body height so a
      // shorter result set doesn't overshoot.
      const targetY = Number(saved.lyrics.scrollY) || 0;
      if (targetY > 0) {
        setTimeout(() => {
          requestAnimationFrame(() => {
            const maxY = Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
            window.scrollTo(0, Math.min(targetY, maxY));
          });
        }, 280);
      }
    }
  }
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
// Expose so blues-admin.js's bluesDbRenderList() can refresh this
// grid after inline editor mutations (Refresh from Discogs, Pick
// Discogs match, per-row enrich, etc.) without admin.html present.
window._baLoadList = _baLoadList;

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
    <table class="api-log-table" style="font-size:0.86rem;width:100%;table-layout:fixed">
      <colgroup>
        <col style="width:52px">
        <col>
        <col style="width:110px">
        <col style="width:70px">
        <col style="width:70px">
        <col style="width:80px">
      </colgroup>
      <thead><tr>
        ${_baSortTh("📷",          "has_photo",          S, "_baSortList", "width:48px;text-align:center")}
        ${_baSortTh("Name",       "name",               S, "_baSortList")}
        ${_baSortTh("Discogs ID", "discogs_id",         S, "_baSortList")}
        ${_baSortTh("Year",       "first_release_year", S, "_baSortList", "text-align:right")}
        ${_baSortTh("Lyrics",     "lyrics_count",       S, "_baSortList", "text-align:right")}
        ${_baSortTh("Releases",   "releases_count",     S, "_baSortList", "text-align:right")}
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
        // Per-row "Search Discogs as artist" affordance — fastest way
        // to track down the canonical id when curating. Opens
        // discogs.com's artist-scoped site search in a new tab; the
        // admin then pastes the right id into the editor + hits
        // Refresh from Discogs.
        const discogsSearchHref = row.name
          ? "https://www.discogs.com/search/?type=artist&q=" + encodeURIComponent(row.name)
          : "";
        const discogsSearchHtml = discogsSearchHref
          ? `<a href="${escHtml(discogsSearchHref)}" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="Search Discogs for an artist named &quot;${escHtml(row.name || "")}&quot; — opens discogs.com in a new tab so you can grab the right id" style="margin-left:0.4rem;font-size:0.78rem;color:var(--muted);text-decoration:none;border:1px solid var(--border);border-radius:4px;padding:0.05rem 0.35rem;font-variant-numeric:tabular-nums">🔎</a>`
          : "";
        // Discogs ID — click opens the full Edit Artist form so the
        // curator can fix / add the id (or any other field) without
        // an extra trip into the artist profile. stopPropagation so
        // the row's profile-open click doesn't also fire. Blank-id
        // rows still click-to-edit (rendered as an em-dash). The
        // 'Open on Discogs.com' link lives inside the editor's
        // external-links bar, so it's not lost.
        const didHtml = row.discogs_id
          ? `<a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenFullEditor(${row.id})" style="color:var(--accent);text-decoration:none;font-variant-numeric:tabular-nums" title="Click to edit this artist">${row.discogs_id}</a>`
          : `<a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenFullEditor(${row.id})" style="color:var(--muted);text-decoration:none" title="Click to edit this artist and add a Discogs ID">—</a>`;
        // first_release_year: server computes LEAST(MB first_recording_year,
        // MIN(discogs_releases.year)). Renders as a plain year cell;
        // dash when neither MB nor Discogs has supplied one yet.
        const yr = Number.isFinite(Number(row.first_release_year)) ? Number(row.first_release_year) : null;
        const yrHtml = yr ? `<span style="font-variant-numeric:tabular-nums">${yr}</span>` : `<span style="color:var(--muted)">—</span>`;
        // Photo thumb — blues_artists.photo_url is populated by the
        // Wikidata / Wikipedia / Discogs seeds. Lazy-load + decode async
        // so a row of 100 thumbs doesn't block the table render; broken
        // URLs collapse to a faint placeholder so the column width
        // stays consistent across rows that have / don't have a photo.
        const photo = (typeof row.photo_url === "string" && row.photo_url) ? row.photo_url : "";
        const photoHtml = photo
          ? `<img src="${escHtml(photo)}" alt="" loading="lazy" decoding="async" style="width:40px;height:40px;object-fit:cover;border-radius:4px;background:var(--border);display:block" onerror="this.style.visibility='hidden'">`
          : `<span style="width:40px;height:40px;border-radius:4px;background:rgba(255,255,255,0.04);display:inline-block" aria-hidden="true"></span>`;
        const fullName = String(row.name || "");
        return `<tr style="cursor:pointer" onclick="_baOpenArtist(${row.id})">
          <td style="padding:0.25rem 0.4rem">${photoHtml}</td>
          <td style="font-weight:600;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(fullName)}">${nameHtml}${discogsSearchHtml}</td>
          <td style="font-size:0.78rem">${didHtml}</td>
          <td style="text-align:right;font-size:0.82rem">${yrHtml}</td>
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
  // Pseudonyms / bands strip — aliases (alternate names the artist
  // recorded under) and collaborators (bands or sidemen they played
  // with). Both are plain string arrays on the row; clicking a chip
  // opens its lookup popup so the curator can jump to a search /
  // Wikipedia / Discogs for that name. Hidden when both are empty.
  const aliasArr = Array.isArray(a.aliases) ? a.aliases.filter(Boolean) : [];
  const bandsArr = Array.isArray(a.collaborators)
    ? a.collaborators
        .map(c => (typeof c === "string" ? c : (c && typeof c === "object" ? (c.name || "") : "")))
        .filter(Boolean)
    : [];
  const chipHtml = (label, items) => items.length
    ? `<div style="display:flex;gap:0.4rem;flex-wrap:wrap;align-items:center;margin-top:0.4rem">
         <span style="font-size:0.74rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.04em">${label}</span>
         ${items.map(n => {
           const safe = escHtml(n);
           // Custom chip — entityLookupLinkHtml doesn't accept inline
           // style, so wire openLookupPopup directly via onclick. Same
           // lookup popup the artist names use, in artist scope.
           return `<span role="button" tabindex="0" onclick="event.preventDefault();event.stopPropagation();openLookupPopup(event,'artist',${JSON.stringify(n).replace(/"/g, '&quot;')},{})" title="Lookup options for &quot;${safe}&quot;" style="padding:0.18rem 0.5rem;border:1px solid var(--border);border-radius:999px;font-size:0.76rem;color:var(--text);background:rgba(255,255,255,0.03);cursor:pointer">${safe}</span>`;
         }).join("")}
       </div>`
    : "";
  const aliasBandsHtml = chipHtml("Pseudonyms", aliasArr) + chipHtml("Bands / played with", bandsArr);
  // Structured artist↔artist links — separate from the freeform
  // aliases/collaborators above. These point at OTHER blues_artists
  // rows by id so the chip can open that artist's profile directly.
  const linksAll = Array.isArray(a.links) ? a.links : [];
  const linkPseudo = linksAll.filter(l => l.kind === "pseudonym");
  const linkBand   = linksAll.filter(l => l.kind === "band");
  const linkChipHtml = (label, items) => items.length
    ? `<div style="display:flex;gap:0.4rem;flex-wrap:wrap;align-items:center;margin-top:0.4rem">
         <span style="font-size:0.74rem;color:var(--accent);text-transform:uppercase;letter-spacing:0.04em">${label}</span>
         ${items.map(l => {
           const safe = escHtml(l.name || "");
           return `<a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenArtist(${l.id})" title="Open ${safe} in Blues Archive" style="padding:0.18rem 0.5rem;border:1px solid var(--accent);border-radius:999px;font-size:0.76rem;color:var(--accent);background:rgba(255,255,255,0.03);text-decoration:none;cursor:pointer">🔗 ${safe}</a>`;
         }).join("")}
       </div>`
    : "";
  const linksHtml = linkChipHtml("Linked: pseudonym", linkPseudo)
                  + linkChipHtml("Linked: band / member", linkBand);
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
          ${_baSortTh("Title",   "page_title",         LS, "_baSortLyrics")}
          ${_baSortTh("Year",    "first_release_year", LS, "_baSortLyrics", "text-align:right;padding-right:0.6rem")}
          ${_baSortTh("Tuning",  "tuning",             LS, "_baSortLyrics")}
          ${_baSortTh("Snippet", "snippet",            LS, "_baSortLyrics")}
          <th style="width:1%"></th>
        </tr></thead>
        <tbody>${lyrics.map(l => {
          const titleHtml = (typeof entityLookupLinkHtml === "function" && l.page_title)
            ? entityLookupLinkHtml("track", l.page_title, { trackArtist: a.name || "", title: `Lookup options for "${l.page_title}"` })
            : escHtml(l.page_title || "");
          const yr = Number.isFinite(Number(l.first_release_year)) ? Number(l.first_release_year) : null;
          const yrHtml = yr
            ? `<span style="font-variant-numeric:tabular-nums" title="${escHtml(l.first_release_source ? "via " + l.first_release_source : "")}">${yr}</span>`
            : `<span style="color:#555">—</span>`;
          // Search-this-track shortcut: jumps to the main SeaDisco
          // search with title in `q`, artist name in `a`, restricted
          // to master+ results, sorted by year ascending. Params here
          // mirror restoreFromParams() in search.js (1-letter keys).
          const searchQs = `?q=${encodeURIComponent(l.page_title || "")}` +
                           `&a=${encodeURIComponent(a.name || "")}` +
                           `&r=${encodeURIComponent("master+")}` +
                           `&s=${encodeURIComponent("year:asc")}`;
          const searchLink = `<a href="/${searchQs}" target="_blank" rel="noopener" onclick="event.stopPropagation()" class="ba-lyric-search" title="Search SeaDisco — masters+, oldest first">🔍</a>`;
          const visitedCls = _baVisitedLyrics.has(Number(l.id)) ? " ba-lyric-visited" : "";
          return `<tr data-lyric-row="${l.id}" class="${visitedCls.trim()}">
            <td style="font-weight:600;color:var(--text);white-space:nowrap">${searchLink} ${titleHtml}</td>
            <td style="text-align:right;font-size:0.82rem;padding-right:0.6rem;white-space:nowrap">${yrHtml}</td>
            <td style="white-space:nowrap;color:var(--accent);cursor:pointer" onclick="_baOpenLyric(${l.id})">${escHtml(l.tuning || "")}</td>
            <td class="ba-lyric-snippet" style="font-size:0.76rem;cursor:pointer" onclick="_baOpenLyric(${l.id})">${escHtml((l.snippet || "").replace(/\s+/g, " ").slice(0, 140))}…</td>
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
          // Type cell is an explicit anchor that opens the in-app
          // release/master popup. Title still routes through
          // entityLookupLinkHtml (search-options popup), so the curator
          // can pick: title → lookup options, type → straight to the
          // release/album popup.
          const typeSafe = escHtml(type).replace(/'/g, "\\'");
          const typeLinkHtml = `<a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenRelease(${rel.id}, '${typeSafe}', '${escHtml(safeUrl)}')" style="color:var(--accent);text-decoration:none;text-transform:uppercase" title="Open ${type} popup">${escHtml(type)} ↗</a>`;
          return `<tr style="cursor:pointer" onclick="_baOpenRelease(${rel.id}, '${typeSafe}', '${escHtml(safeUrl)}')">
            <td style="white-space:nowrap;color:var(--muted);font-variant-numeric:tabular-nums">${rel.year || "—"}</td>
            <td style="font-weight:600;color:var(--text)">${titleHtml}</td>
            <td style="color:#888;font-size:0.78rem">${escHtml(rel.label || "")}</td>
            <td style="font-size:0.74rem">${typeLinkHtml}</td>
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
        <button class="archive-btn archive-btn-suggest" onclick="_baOpenFullEditor(${a.id})" title="Open the full edit form (~25 fields: name / dates / identifiers / pseudonyms / bands / notes / photo + enrichment buttons for Wiki, MusicBrainz, Discogs, YouTube).">✎ Edit artist</button>
        <button class="archive-btn" onclick="_baOpenReassignPicker(${a.id}, ${JSON.stringify(a.name || "").replace(/"/g, "&quot;")})" title="Reassign every lyric matching some other artist name (or artist row) to this artist. Doesn't delete the source artist — use Merge for that.">Reassign lyrics from…</button>
        <button class="archive-btn" onclick="_baOpenMergePicker(${a.id}, ${JSON.stringify(a.name || "").replace(/"/g, "&quot;")})" title="Merge this artist into another. Lyrics get reassigned by name; release JSONB arrays are concatenated (deduped); this row is then deleted.">Merge into…</button>
        <button class="archive-btn" onclick="_baDeleteArtistWithLyrics(${a.id})" title="Delete THIS artist AND every lyric tied to them (FK-linked OR name-matched). Cannot be undone. Use Merge instead if you just want to consolidate." style="color:#e88;border-color:rgba(232,136,136,0.4)">Delete artist + lyrics</button>
      </span>
    </div>
    <div style="display:flex;gap:1rem;margin-bottom:1.2rem;align-items:flex-start;flex-wrap:wrap">
      ${photo}
      <div style="flex:1;min-width:240px">
        ${meta ? `<div style="font-size:0.82rem;color:var(--muted);margin-bottom:0.4rem">${meta}</div>` : ""}
        ${linksHtml}
        ${aliasBandsHtml}
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

// Cascade-delete an artist AND every lyric tied to them. Two-step
// confirm (the count of affected lyrics is shown after we look it
// up) so a stray click can't nuke a row with 40 lyrics by accident.
// On success: closes the popup, reloads the grid + stats so the
// numbers reflect the deletion.
async function _baDeleteArtistWithLyrics(id) {
  const artistName = _baDetailArtist?.name || `#${id}`;
  const lyricsN = Array.isArray(_baDetailArtist?.lyrics) ? _baDetailArtist.lyrics.length : 0;
  const releasesN = Array.isArray(_baDetailArtist?.releases) ? _baDetailArtist.releases.length : 0;
  const msg =
    `Permanently delete ${artistName}?\n\n` +
    `This removes:\n` +
    `· the artist row\n` +
    `· ${lyricsN} associated lyric${lyricsN === 1 ? "" : "s"}\n` +
    (releasesN ? `\nThe Discogs releases array stored on this row is NOT pushed back to Discogs — only this DB row is affected.\n` : "") +
    `\nThis cannot be undone. Use 'Merge into…' instead if you want to consolidate without losing lyrics.`;
  if (!confirm(msg)) return;
  try {
    const r = await apiFetch(`/api/admin/blues/${id}?with_lyrics=1`, { method: "DELETE" });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Delete failed: " + (err?.error ?? r.status));
      return;
    }
    const out = await r.json().catch(() => ({}));
    // Close the popup, refresh the underlying list + stats so the
    // row vanishes and the chip counts update.
    document.getElementById("ba-artist-overlay")?.remove();
    _baCurrentArtistId = null;
    if (typeof _baLoadList === "function")  { try { _baLoadList();  } catch {} }
    if (typeof _baLoadStats === "function") { try { _baLoadStats(); } catch {} }
    if (typeof showToast === "function") {
      showToast(`Deleted ${out.artistName || artistName} + ${out.lyricsDeleted ?? 0} lyric${out.lyricsDeleted === 1 ? "" : "s"}`, "ok");
    }
  } catch (e) {
    alert("Delete failed: " + (e?.message || e));
  }
}
window._baDeleteArtistWithLyrics = _baDeleteArtistWithLyrics;

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
  // Mark the row visited immediately (before the fetch resolves) so
  // the snippet column dims even if the user dismisses the overlay
  // before the body lands.
  _baMarkLyricVisited(id);
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
            <button class="archive-btn" data-ba-fav-id="${row.id}" onclick="_baToggleLyricFavorite(${row.id})" title="${_baFavoriteIds.has(Number(row.id)) ? "Favorited — click to un-favorite" : "Click to favorite"}" style="font-size:1.1rem;padding:0 0.55rem;color:#ffd166">${_baFavoriteIds.has(Number(row.id)) ? "★" : "☆"}</button>
            <button class="archive-btn" onclick="_baAddLyricToSetlist(${row.id})" title="Add this lyric to a setlist…">+ Setlist</button>
            <button class="archive-btn" onclick="_baOpenLyricEditor(${row.id})" title="Edit title / artist / tuning on this lyric">Edit</button>
            <button class="archive-btn" onclick="_baDeleteLyric(${row.id})" style="color:#e88" title="Permanently delete this lyric row">Delete</button>
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
async function _baOpenLyricEditor(id, prefill) {
  // id === null → "new lyric" mode. Empty row template, "Create"
  // button instead of Save, no Delete affordance. The optional
  // `prefill` object lets callers (e.g. the site-wide "Add to lyrics"
  // popup action on a track) seed title + artist so the curator
  // only needs to paste the body and hit Create.
  const isNew = id == null;
  let row;
  if (isNew) {
    row = { id: null, page_title: "", artist: "", artist_id: null, tuning: "", page_url: "", discogs_release_id: null, discogs_master_id: null, plaintext: "", wikitext: "" };
    if (prefill && typeof prefill === "object") {
      if (typeof prefill.page_title === "string") row.page_title = prefill.page_title;
      if (typeof prefill.artist     === "string") row.artist     = prefill.artist;
      if (Number.isFinite(prefill.discogs_release_id)) row.discogs_release_id = Number(prefill.discogs_release_id);
      if (Number.isFinite(prefill.discogs_master_id))  row.discogs_master_id  = Number(prefill.discogs_master_id);
    }
  } else {
    try {
      const r = await apiFetch(`/api/admin/lyrics/${id}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      row = await r.json();
    } catch (e) {
      alert(`Couldn't load lyric: ${e?.message || e}`);
      return;
    }
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
          <h3 style="margin:0 0 0.25rem">${isNew ? "Add lyric" : "Edit lyric"}</h3>
          ${row.page_url ? `<div style="font-size:0.76rem;color:var(--muted)"><a href="${escHtml(row.page_url)}" target="_blank" rel="noopener" style="color:var(--accent)">View on wiki ↗</a></div>` : ""}
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
        <div style="width:120px">
          <label style="display:block;margin:0 0 0.3rem;font-size:0.82rem;color:var(--muted)" title="Year the song was first recorded/released. Auto-resolved by 'Resolve years' from the linked artist's Discogs releases; enter manually here to override or fill blanks the resolver couldn't find.">First year</label>
          <input id="ba-edit-first-year" type="number" min="1850" max="2100" value="${row.first_release_year ?? ""}" placeholder="YYYY" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
        </div>
      </div>
      ${isNew ? `
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)">Source URL (optional)</label>
      <input id="ba-edit-page-url" type="url" placeholder="https://… (only for new manual lyrics; scraped rows already have one)" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)" title="The lyric body. Used when no wiki source is available — the viewer popup renders this verbatim.">Lyrics (plaintext)</label>
      <textarea id="ba-edit-plaintext" rows="10" placeholder="Paste or type the song lyrics here…" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem;font-family:inherit"></textarea>
      ` : ""}
      <div style="display:flex;gap:0.5rem;justify-content:flex-end;margin-top:1rem;flex-wrap:wrap">
        ${isNew ? "" : `<button class="archive-btn" onclick="_baDeleteLyric(${id})" style="margin-right:auto;color:#e88" title="Permanently delete this lyric row">Delete</button>`}
        <button class="archive-btn" onclick="document.getElementById('ba-lyric-edit-overlay')?.remove()">Cancel</button>
        <button class="archive-btn archive-btn-suggest" onclick="${isNew ? "_baCreateLyric()" : `_baSaveLyricEdit(${id})`}">${isNew ? "Create" : "Save"}</button>
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

// Create a brand-new lyric row from the editor's "new" mode.
// Source host defaults to "manual" server-side so the new row
// doesn't collide with the scraped corpus. Title + artist together
// are unique per source_host — a 409 from the server means the
// same title + same artist already exists.
async function _baCreateLyric() {
  const statusEl   = document.getElementById("ba-edit-status");
  const page_title = (document.getElementById("ba-edit-title")?.value ?? "").trim();
  if (!page_title) { if (statusEl) statusEl.textContent = "Title is required."; return; }
  const artist     = (document.getElementById("ba-edit-artist")?.value ?? "").trim();
  const tuning     = (document.getElementById("ba-edit-tuning")?.value ?? "").trim();
  const page_url   = (document.getElementById("ba-edit-page-url")?.value ?? "").trim();
  const plaintext  = (document.getElementById("ba-edit-plaintext")?.value ?? "");
  const releaseIdRaw = document.getElementById("ba-edit-release-id")?.value ?? "";
  const masterIdRaw  = document.getElementById("ba-edit-master-id")?.value ?? "";
  const firstYearRaw = document.getElementById("ba-edit-first-year")?.value ?? "";
  const discogs_release_id = releaseIdRaw === "" ? null : Number(releaseIdRaw);
  const discogs_master_id  = masterIdRaw  === "" ? null : Number(masterIdRaw);
  const first_release_year = firstYearRaw === "" ? null : Number(firstYearRaw);
  if (statusEl) statusEl.textContent = "Creating…";
  try {
    const r = await apiFetch("/api/admin/lyrics", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        page_title, artist, tuning, page_url, plaintext,
        discogs_release_id, discogs_master_id, first_release_year,
      }),
    });
    if (!r.ok) {
      if (r.status === 409) throw new Error(`A lyric titled "${page_title}" by "${artist || "(no artist)"}" already exists on this source.`);
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const created = await r.json();
    document.getElementById("ba-lyric-edit-overlay")?.remove();
    _baLoadStats().catch(() => {});
    if (_baSubtab === "lyrics") _baLoadLyrics();
    // Open the newly-created lyric in the viewer so the user sees
    // confirmation + can quick-fix anything they got wrong.
    setTimeout(() => _baOpenLyric(created.id), 80);
  } catch (e) {
    if (statusEl) statusEl.textContent = `Create failed: ${e?.message || e}`;
  }
}
window._baCreateLyric = _baCreateLyric;

async function _baSaveLyricEdit(id) {
  const statusEl   = document.getElementById("ba-edit-status");
  const artist     = document.getElementById("ba-edit-artist")?.value ?? "";
  const tuning     = document.getElementById("ba-edit-tuning")?.value ?? "";
  const page_title = (document.getElementById("ba-edit-title")?.value ?? "").trim();
  // Empty string → null on the server (releases pin cleared).
  const releaseIdRaw = document.getElementById("ba-edit-release-id")?.value ?? "";
  const masterIdRaw  = document.getElementById("ba-edit-master-id")?.value ?? "";
  const firstYearRaw = document.getElementById("ba-edit-first-year")?.value ?? "";
  const discogs_release_id = releaseIdRaw === "" ? null : Number(releaseIdRaw);
  const discogs_master_id  = masterIdRaw  === "" ? null : Number(masterIdRaw);
  const first_release_year = firstYearRaw === "" ? null : Number(firstYearRaw);
  if (!page_title) { if (statusEl) statusEl.textContent = "Title is required."; return; }
  if (statusEl) statusEl.textContent = "Saving…";
  try {
    const r = await apiFetch(`/api/admin/lyrics/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ artist, tuning, page_title, discogs_release_id, discogs_master_id, first_release_year }),
    });
    if (!r.ok) {
      // 409 = title already taken on this source_host. Server returns
      // the blocking row's id so we can give the curator a way out
      // (open the conflict to merge content / delete the duplicate /
      // change the title here) instead of just a dead-end message.
      if (r.status === 409) {
        const body = await r.json().catch(() => ({}));
        if (body?.conflictId) {
          if (statusEl) {
            const otherArtist = body.conflictArtist || "(no artist)";
            const otherTitle = body.conflictTitle || "(no title)";
            statusEl.innerHTML =
              `Save failed: lyric #${escHtml(String(body.conflictId))} already has ` +
              `<strong>${escHtml(otherTitle)}</strong> by <strong>${escHtml(otherArtist)}</strong> on this source. ` +
              `<a href="#" onclick="event.preventDefault();document.getElementById('ba-lyric-edit-overlay')?.remove();_baOpenLyric(${Number(body.conflictId)})" style="color:var(--accent);text-decoration:underline">Open it ↗</a> ` +
              `<span style="color:var(--muted)">· then delete or rename one of them.</span>`;
          }
          return;
        }
        throw new Error("Another lyric already has that title on this source.");
      }
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

// Hard-delete a single lyric row. Used both by the editor's Delete
// button and by the viewer popup's Delete (the latter is the main
// path for resolving title-conflict 409s — open the blocking row,
// hit Delete, then re-save the original). Closes whichever overlays
// are open and drops the row from the visible cache + list DOM so
// the user doesn't need to refresh.
async function _baDeleteLyric(id) {
  const n = Number(id);
  if (!Number.isFinite(n) || n <= 0) return;
  if (!confirm(`Permanently delete lyric #${n}? This cannot be undone.`)) return;
  try {
    const r = await apiFetch(`/api/admin/lyrics/${n}`, { method: "DELETE" });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      alert(`Delete failed: ${body?.error || `HTTP ${r.status}`}`);
      return;
    }
    // Close both possible overlays.
    document.getElementById("ba-lyric-edit-overlay")?.remove();
    document.getElementById("ba-lyric-overlay")?.remove();
    // Prune from local caches so the master list / artist popup
    // re-renders without an extra fetch.
    _baLyricsRowsCache = _baLyricsRowsCache.filter(x => Number(x.id) !== n);
    if (_baDetailArtist && Array.isArray(_baDetailArtist.lyrics)) {
      _baDetailArtist.lyrics = _baDetailArtist.lyrics.filter(x => Number(x.id) !== n);
    }
    // Drop the row from the DOM. Both the master list and any open
    // artist-popup sub-table tag their <tr> with data-lyric-row.
    document.querySelectorAll(`tr[data-lyric-row="${n}"]`).forEach(tr => tr.remove());
    if (typeof showToast === "function") showToast("Lyric deleted", "ok");
  } catch (e) {
    alert(`Delete failed: ${e?.message || e}`);
  }
}
window._baDeleteLyric = _baDeleteLyric;

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
let _baSubtab = "artists"; // "artists" | "lyrics" | "releases"
let _baLyricsPage = 0;
const _BA_LYRICS_LIMIT = 100;
let _baLyricsTotal = 0;
let _baLyricsSearchTimer = null;
let _baLyricsTuning = "";
let _baLyricsUnmatched = false;
let _baLyricsUnpinned = false;
let _baLyricsEmpty = false;
let _baLyricsRowsCache = [];
const _baLyricsListSort = { key: "page_title", dir: "asc" };
const _BA_LYRICS_LIST_TYPES = { page_title: "str", artist: "str", tuning: "str", snippet: "str", first_release_year: "num" };
let _baLyricsTuningsLoaded = false;

function _baSwitchSubtab(tab) {
  _baSubtab = (tab === "lyrics" || tab === "releases" || tab === "setlists") ? tab : "artists";
  // Toggle button active state
  document.querySelectorAll("#blues-archive-subtabs .ba-subtab").forEach(b => {
    b.classList.toggle("is-active", b.dataset.baTab === _baSubtab);
  });
  const ap = document.getElementById("blues-archive-artists-panel");
  const lp = document.getElementById("blues-archive-lyrics-panel");
  const rp = document.getElementById("blues-archive-releases-panel");
  const sp = document.getElementById("blues-archive-setlists-panel");
  if (ap) ap.style.display = _baSubtab === "artists"  ? "" : "none";
  if (lp) lp.style.display = _baSubtab === "lyrics"   ? "" : "none";
  if (rp) rp.style.display = _baSubtab === "releases" ? "" : "none";
  if (sp) sp.style.display = _baSubtab === "setlists" ? "" : "none";
  if (_baSubtab === "lyrics") {
    if (!_baLyricsTuningsLoaded) _baLoadTunings();
    _baLoadLyrics();
  } else if (_baSubtab === "releases") {
    _baLoadReleases();
  } else if (_baSubtab === "setlists") {
    _baLoadSetlists();
  }
  // Persist after every subtab switch so a quick "Lyrics → Search"
  // round trip pulls the user back to Lyrics on return.
  _baPersistViewState();
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

function _baLyricsApplyUnpinned() {
  _baLyricsUnpinned = !!document.getElementById("blues-archive-lyrics-unpinned")?.checked;
  _baLyricsPage = 0;
  _baLoadLyrics();
}
window._baLyricsApplyUnpinned = _baLyricsApplyUnpinned;

function _baLyricsApplyEmpty() {
  _baLyricsEmpty = !!document.getElementById("blues-archive-lyrics-empty")?.checked;
  _baLyricsPage = 0;
  _baLoadLyrics();
}
window._baLyricsApplyEmpty = _baLyricsApplyEmpty;

// Cheap pass: set every lyric's first_release_year from its linked
// artist's discogs_releases (title match). Zero Discogs API calls.
// Reports the rows updated and how many still have no year — the
// remainder will need either manual entry or a future Discogs-search
// worker.
async function _baResolveYearsCheap() {
  const btn = document.getElementById("blues-archive-lyrics-resolve-years-btn");
  const orig = btn?.textContent;
  if (btn) { btn.disabled = true; btn.textContent = "Resolving…"; }
  try {
    const r = await apiFetch("/api/admin/lyrics/resolve-years-cheap", { method: "POST" });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Resolve failed: " + (err?.error ?? r.status));
      return;
    }
    const { updated = 0, stillMissing = 0 } = await r.json();
    const msg = `Resolved ${updated} year${updated === 1 ? "" : "s"} from the linked artist's Discogs releases.\n\n` +
      (stillMissing
        ? `${stillMissing.toLocaleString()} lyric${stillMissing === 1 ? "" : "s"} still have no year — they need either a release pin in the editor, or a future Discogs-search worker pass.`
        : `All lyrics now have a first_release_year.`);
    alert(msg);
    _baLoadLyrics();
  } catch (e) {
    alert("Resolve failed: " + (e?.message || e));
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = orig || "Resolve years"; }
  }
}
window._baResolveYearsCheap = _baResolveYearsCheap;

// Slow Discogs-search resolver. POSTs to the background-job endpoint
// then polls /status every 4s until done. Button stays disabled while
// running and shows live counts; click while running flips to "Stop"
// (which requests graceful shutdown after the current row).
let _baYearJobTimer = null;
async function _baResolveYearsDiscogs() {
  const btn = document.getElementById("blues-archive-lyrics-resolve-years-discogs-btn");
  // If already running, this click is a stop request.
  if (btn?.dataset.running === "1") {
    if (!confirm("Stop the Discogs-search worker after the current row?")) return;
    try { await apiFetch("/api/admin/lyrics/resolve-years-discogs/stop", { method: "POST" }); }
    catch {}
    return;
  }
  if (!confirm(
    "Run the Discogs-search worker for every lyric still missing a year?\n\n" +
    "Slow — rate-limited 1 req/sec on Discogs.\n" +
    "Leaving the page is fine; the job runs server-side and you can reload to watch progress."
  )) return;
  try {
    const r = await apiFetch("/api/admin/lyrics/resolve-years-discogs", { method: "POST" });
    if (r.status === 409) {
      const j = await r.json().catch(() => ({}));
      alert("Already running since " + (j.startedAt ?? "earlier") + ". Watching progress.");
    } else if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Couldn't start: " + (err.error ?? r.status));
      return;
    }
  } catch (e) { alert("Couldn't start: " + (e?.message || e)); return; }
  _baPollYearJob();
}
window._baResolveYearsDiscogs = _baResolveYearsDiscogs;

function _baPollYearJob() {
  const btn = document.getElementById("blues-archive-lyrics-resolve-years-discogs-btn");
  if (!btn) return;
  if (_baYearJobTimer) { clearInterval(_baYearJobTimer); _baYearJobTimer = null; }
  const tick = async () => {
    try {
      const r = await apiFetch("/api/admin/lyrics/resolve-years-discogs/status");
      if (!r.ok) return;
      const job = await r.json();
      if (job.status === "running") {
        btn.dataset.running = "1";
        const p = job.progress;
        btn.textContent = p
          ? `Discogs: ${p.processed}/${p.total} · ${p.resolved} hit · ${p.notFound} miss · click to Stop`
          : "Running… click to Stop";
        if (Array.isArray(p?.recentErrors) && p.recentErrors.length) {
          btn.title = "Latest errors:\n" + p.recentErrors.slice(-5).map(e => `${e.title}: ${e.message?.slice(0, 80) || ""}`).join("\n");
        }
        return;
      }
      // Done / error / idle
      clearInterval(_baYearJobTimer); _baYearJobTimer = null;
      btn.dataset.running = "";
      btn.textContent = "Resolve via Discogs ↗";
      btn.title = "Slow background worker: Discogs-searches each still-missing lyric by artist + title and takes the earliest year. ~1 req/sec → ~1-2 min per 100 lyrics. Resumable.";
      if (job.status === "done" && job.result) {
        const o = job.result;
        alert(
          `Discogs-search worker done in ${(o.durationMs / 60000).toFixed(1)} min:\n` +
          `· ${o.resolved} year${o.resolved === 1 ? "" : "s"} resolved\n` +
          `· ${o.notFound} not found on Discogs\n` +
          (o.errors?.length ? `· ${o.errors.length} errors` : `· no errors`)
        );
        if (_baSubtab === "lyrics") _baLoadLyrics();
      } else if (job.status === "error") {
        alert("Worker errored: " + (job.error ?? "unknown"));
      }
    } catch {}
  };
  tick();
  _baYearJobTimer = setInterval(tick, 4000);
}
window._baPollYearJob = _baPollYearJob;

// ── Favorites + Setlists ─────────────────────────────────────────────
// In-memory cache of favorited lyric IDs so the viewer star renders
// the right state without a network round-trip per popup.
let _baFavoriteIds = new Set();
let _baSetlistsCache = [];
let _baCurrentSetlistId = null;

async function _baLoadFavoriteIds() {
  try {
    const r = await apiFetch("/api/blues-archive/favorites/ids");
    if (!r.ok) return;
    const { ids = [] } = await r.json();
    _baFavoriteIds = new Set(ids.map(Number));
  } catch {}
}
window._baLoadFavoriteIds = _baLoadFavoriteIds;

async function _baToggleLyricFavorite(lyricId) {
  const id = Number(lyricId);
  if (!Number.isFinite(id)) return;
  const wasFav = _baFavoriteIds.has(id);
  // Optimistic flip — repaint star immediately, undo on error.
  if (wasFav) _baFavoriteIds.delete(id); else _baFavoriteIds.add(id);
  document.querySelectorAll(`[data-ba-fav-id="${id}"]`).forEach(el => {
    el.textContent = _baFavoriteIds.has(id) ? "★" : "☆";
    el.title = _baFavoriteIds.has(id) ? "Favorited — click to un-favorite" : "Click to favorite";
  });
  try {
    const r = wasFav
      ? await apiFetch(`/api/blues-archive/favorites/${id}`, { method: "DELETE" })
      : await apiFetch(`/api/blues-archive/favorites`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ lyricId: id }),
        });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
  } catch (e) {
    // Revert the optimistic flip on failure.
    if (wasFav) _baFavoriteIds.add(id); else _baFavoriteIds.delete(id);
    document.querySelectorAll(`[data-ba-fav-id="${id}"]`).forEach(el => {
      el.textContent = _baFavoriteIds.has(id) ? "★" : "☆";
    });
    if (typeof showToast === "function") showToast("Favorite update failed", "error");
  }
}
window._baToggleLyricFavorite = _baToggleLyricFavorite;

// Setlists: list rendering ───────────────────────────────────────────
async function _baLoadSetlists() {
  const listEl  = document.getElementById("blues-archive-setlists-list");
  const countEl = document.getElementById("blues-archive-setlists-count");
  if (!listEl) return;
  listEl.innerHTML = `<div style="color:var(--muted)">Loading…</div>`;
  try {
    const r = await apiFetch("/api/blues-archive/setlists");
    if (!r.ok) { listEl.innerHTML = `<div style="color:#e88">Failed: HTTP ${r.status}</div>`; return; }
    const { rows = [] } = await r.json();
    _baSetlistsCache = rows;
    if (countEl) countEl.textContent = rows.length ? `${rows.length} setlist${rows.length === 1 ? "" : "s"}` : "No setlists yet.";
    if (!rows.length) {
      listEl.innerHTML = `<div style="color:var(--muted);padding:0.4rem">No setlists yet. Click <strong>+ New setlist</strong>.</div>`;
      return;
    }
    listEl.innerHTML = rows.map(s => {
      const sel = (_baCurrentSetlistId === s.id) ? "background:rgba(255,255,255,0.06);" : "";
      const updated = s.updated_at ? new Date(s.updated_at).toLocaleDateString() : "";
      return `<div style="padding:0.4rem 0.5rem;border-bottom:1px solid var(--border);cursor:pointer;${sel}" onclick="_baOpenSetlist(${s.id})">
        <div style="font-weight:600;color:var(--text)">${escHtml(s.name)}</div>
        <div style="font-size:0.72rem;color:var(--muted);margin-top:0.1rem">${s.item_count} song${s.item_count === 1 ? "" : "s"}${updated ? " · " + escHtml(updated) : ""}</div>
      </div>`;
    }).join("");
  } catch (e) {
    listEl.innerHTML = `<div style="color:#e88">Failed: ${escHtml(e?.message || String(e))}</div>`;
  }
}
window._baLoadSetlists = _baLoadSetlists;

async function _baOpenSetlist(id) {
  _baCurrentSetlistId = Number(id);
  const detailEl = document.getElementById("blues-archive-setlists-detail");
  if (!detailEl) return;
  detailEl.innerHTML = `<div style="color:var(--muted)">Loading…</div>`;
  try {
    const r = await apiFetch(`/api/blues-archive/setlists/${id}`);
    if (!r.ok) { detailEl.innerHTML = `<div style="color:#e88">Failed: HTTP ${r.status}</div>`; return; }
    const s = await r.json();
    _baLoadSetlists(); // repaint left list with new selection highlight
    const items = Array.isArray(s.items) ? s.items : [];
    const itemsHtml = items.length
      ? `<ol style="list-style:none;margin:0;padding:0;counter-reset:setlist">${items.map((it, i) => {
          const pos = i + 1;
          const tuning = it.tuning ? `<span style="color:#888;font-size:0.74rem;margin-left:0.4rem">${escHtml(it.tuning)}</span>` : "";
          return `<li data-li="${it.lyric_id}" style="display:flex;align-items:center;gap:0.4rem;padding:0.35rem 0.4rem;border-bottom:1px solid var(--border)">
            <span style="color:var(--muted);font-size:0.78rem;width:1.8em;text-align:right">${pos}.</span>
            <span style="flex:1;min-width:0">
              <a href="#" onclick="event.preventDefault();_baOpenLyric(${it.lyric_id})" style="color:var(--text);text-decoration:none;font-weight:600">${escHtml(it.page_title || "(untitled)")}</a>
              <span style="color:var(--muted);font-size:0.78rem;margin-left:0.4rem">${escHtml(it.artist || "")}</span>
              ${tuning}
            </span>
            <button class="archive-btn" title="Move up"   ${i === 0 ? "disabled" : ""} onclick="_baSetlistMoveItem(${s.id}, ${it.lyric_id}, -1)" style="padding:0 0.5rem">↑</button>
            <button class="archive-btn" title="Move down" ${i === items.length - 1 ? "disabled" : ""} onclick="_baSetlistMoveItem(${s.id}, ${it.lyric_id}, +1)" style="padding:0 0.5rem">↓</button>
            <button class="archive-btn" title="Remove from setlist" onclick="_baSetlistRemoveItem(${s.id}, ${it.lyric_id})" style="color:#e88;padding:0 0.5rem">×</button>
          </li>`;
        }).join("")}</ol>`
      : `<div style="color:var(--muted);padding:0.6rem 0;font-style:italic">No songs yet. Add lyrics from their viewer popup (the + Setlist button).</div>`;
    detailEl.innerHTML = `
      <div style="display:flex;align-items:start;justify-content:space-between;gap:0.6rem;margin-bottom:0.6rem">
        <div style="min-width:0">
          <h3 style="margin:0 0 0.2rem;font-size:1rem;color:var(--text)">${escHtml(s.name)}</h3>
          ${s.notes ? `<div style="font-size:0.78rem;color:var(--muted);white-space:pre-wrap">${escHtml(s.notes)}</div>` : ""}
        </div>
        <div style="display:flex;gap:0.3rem;flex-wrap:wrap;justify-content:flex-end">
          <button class="archive-btn" onclick="_baRenameSetlist(${s.id})" title="Rename / edit notes">Edit</button>
          <button class="archive-btn" onclick="window.open('/api/blues-archive/setlists/${s.id}/export.txt')" title="Download as performer-friendly plain text">Export TXT</button>
          <button class="archive-btn" onclick="window.open('/api/blues-archive/setlists/${s.id}/export.csv')" title="Download as CSV (spreadsheet-friendly)">Export CSV</button>
          <button class="archive-btn" onclick="window.open('/api/blues-archive/setlists/${s.id}/export.json')" title="Download as JSON (round-trip)">Export JSON</button>
          <button class="archive-btn" onclick="_baDeleteSetlist(${s.id})" style="color:#e88" title="Permanently delete this setlist">Delete</button>
        </div>
      </div>
      ${itemsHtml}`;
  } catch (e) {
    detailEl.innerHTML = `<div style="color:#e88">Failed: ${escHtml(e?.message || String(e))}</div>`;
  }
}
window._baOpenSetlist = _baOpenSetlist;

async function _baOpenNewSetlistDialog() {
  const name = prompt("Name your setlist:");
  if (!name || !name.trim()) return;
  try {
    const r = await apiFetch("/api/blues-archive/setlists", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: name.trim() }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Create failed: " + (err?.error ?? r.status));
      return;
    }
    const { id } = await r.json();
    await _baLoadSetlists();
    _baOpenSetlist(id);
  } catch (e) { alert("Create failed: " + (e?.message || e)); }
}
window._baOpenNewSetlistDialog = _baOpenNewSetlistDialog;

async function _baRenameSetlist(id) {
  const cur = _baSetlistsCache.find(s => Number(s.id) === Number(id));
  const newName = prompt("Setlist name:", cur?.name || "");
  if (newName == null) return;
  const trimmed = newName.trim();
  if (!trimmed) return;
  try {
    const r = await apiFetch(`/api/blues-archive/setlists/${id}`, {
      method: "PUT", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: trimmed }),
    });
    if (!r.ok) { const err = await r.json().catch(() => ({})); alert("Rename failed: " + (err?.error ?? r.status)); return; }
    await _baLoadSetlists();
    _baOpenSetlist(id);
  } catch (e) { alert("Rename failed: " + (e?.message || e)); }
}
window._baRenameSetlist = _baRenameSetlist;

async function _baDeleteSetlist(id) {
  const cur = _baSetlistsCache.find(s => Number(s.id) === Number(id));
  if (!confirm(`Delete setlist "${cur?.name || ""}"?\n\nSongs are removed from the setlist but the underlying lyrics stay in the archive.`)) return;
  try {
    const r = await apiFetch(`/api/blues-archive/setlists/${id}`, { method: "DELETE" });
    if (!r.ok) { alert("Delete failed: HTTP " + r.status); return; }
    _baCurrentSetlistId = null;
    await _baLoadSetlists();
    const detail = document.getElementById("blues-archive-setlists-detail");
    if (detail) detail.innerHTML = `Pick a setlist on the left, or create a new one.`;
  } catch (e) { alert("Delete failed: " + (e?.message || e)); }
}
window._baDeleteSetlist = _baDeleteSetlist;

// Reorder via two-pass: pull current order, swap the moved item with
// its neighbor, PUT the new sort order. Server doesn't trust the
// client's numbers blindly — it overwrites whatever is sent. Up-arrow
// at top and down-arrow at bottom are disabled in the UI so this only
// fires on valid moves.
async function _baSetlistMoveItem(setlistId, lyricId, direction) {
  try {
    const r = await apiFetch(`/api/blues-archive/setlists/${setlistId}`);
    if (!r.ok) return;
    const s = await r.json();
    const items = Array.isArray(s.items) ? s.items.slice() : [];
    const i = items.findIndex(it => Number(it.lyric_id) === Number(lyricId));
    if (i < 0) return;
    const j = i + direction;
    if (j < 0 || j >= items.length) return;
    [items[i], items[j]] = [items[j], items[i]];
    const payload = items.map((it, idx) => ({ lyricId: it.lyric_id, sort_order: idx + 1 }));
    const pr = await apiFetch(`/api/blues-archive/setlists/${setlistId}/items`, {
      method: "PUT", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items: payload }),
    });
    if (!pr.ok) return;
    _baOpenSetlist(setlistId);
  } catch {}
}
window._baSetlistMoveItem = _baSetlistMoveItem;

async function _baSetlistRemoveItem(setlistId, lyricId) {
  try {
    const r = await apiFetch(`/api/blues-archive/setlists/${setlistId}/items/${lyricId}`, { method: "DELETE" });
    if (!r.ok) { alert("Remove failed: HTTP " + r.status); return; }
    _baOpenSetlist(setlistId);
  } catch (e) { alert("Remove failed: " + (e?.message || e)); }
}
window._baSetlistRemoveItem = _baSetlistRemoveItem;

// Add a lyric to a setlist — picker dialog if more than one option,
// straight-add when only one exists, "+ Create new" when zero.
async function _baAddLyricToSetlist(lyricId) {
  const id = Number(lyricId);
  if (!Number.isFinite(id)) return;
  // Refresh setlists cache so a freshly-created list shows up.
  if (!_baSetlistsCache.length) {
    try {
      const r = await apiFetch("/api/blues-archive/setlists");
      if (r.ok) _baSetlistsCache = (await r.json()).rows ?? [];
    } catch {}
  }
  let chosenId = null;
  if (_baSetlistsCache.length === 0) {
    if (!confirm("You have no setlists yet. Create one now?")) return;
    const name = prompt("Name your setlist:");
    if (!name || !name.trim()) return;
    try {
      const cr = await apiFetch("/api/blues-archive/setlists", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: name.trim() }),
      });
      if (!cr.ok) { alert("Create failed."); return; }
      chosenId = Number((await cr.json()).id);
      _baSetlistsCache = []; // force refresh next time
    } catch (e) { alert("Create failed: " + e); return; }
  } else {
    // Inline picker overlay — small list with each setlist + "+ New".
    chosenId = await _baPickSetlistDialog();
    if (chosenId == null) return;
  }
  try {
    const ar = await apiFetch(`/api/blues-archive/setlists/${chosenId}/items`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lyricId: id }),
    });
    if (!ar.ok) { alert("Add failed: HTTP " + ar.status); return; }
    const out = await ar.json().catch(() => ({}));
    if (typeof showToast === "function") {
      showToast(out.added ? "Added to setlist" : "Already in setlist", "ok");
    }
  } catch (e) { alert("Add failed: " + (e?.message || e)); }
}
window._baAddLyricToSetlist = _baAddLyricToSetlist;

// Setlist picker overlay — returns the chosen id via Promise. New
// setlist creation is offered inline so the curator doesn't bounce
// through the Setlists tab.
function _baPickSetlistDialog() {
  return new Promise((resolve) => {
    document.getElementById("ba-pick-setlist-overlay")?.remove();
    const overlay = document.createElement("div");
    overlay.id = "ba-pick-setlist-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "380", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "3rem 1rem", overflow: "auto",
    });
    overlay.onclick = (e) => { if (e.target === overlay) { overlay.remove(); resolve(null); } };
    const items = _baSetlistsCache.map(s =>
      `<button type="button" class="archive-btn" style="display:block;width:100%;text-align:left;margin:0.2rem 0;padding:0.4rem 0.6rem" onclick="document.getElementById('ba-pick-setlist-overlay')?.remove();window.__baPickSetlistResolve?.(${s.id})"><strong>${escHtml(s.name)}</strong> <span style="color:var(--muted);font-size:0.72rem;margin-left:0.4rem">${s.item_count} song${s.item_count === 1 ? "" : "s"}</span></button>`
    ).join("");
    overlay.innerHTML = `
      <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1rem 1.2rem;width:min(440px,100%)">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.6rem">
          <h3 style="margin:0;font-size:1rem">Add to setlist…</h3>
          <button class="archive-btn" onclick="document.getElementById('ba-pick-setlist-overlay')?.remove();window.__baPickSetlistResolve?.(null)">Cancel</button>
        </div>
        ${items}
        <button type="button" class="archive-btn" style="display:block;width:100%;text-align:left;margin-top:0.6rem;padding:0.4rem 0.6rem;color:var(--accent)" onclick="document.getElementById('ba-pick-setlist-overlay')?.remove();window.__baPickSetlistResolve?.('__new__')">+ Create new setlist…</button>
      </div>`;
    document.body.appendChild(overlay);
    window.__baPickSetlistResolve = async (val) => {
      window.__baPickSetlistResolve = null;
      if (val === "__new__") {
        const name = prompt("Name your new setlist:");
        if (!name || !name.trim()) { resolve(null); return; }
        try {
          const cr = await apiFetch("/api/blues-archive/setlists", {
            method: "POST", headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: name.trim() }),
          });
          if (!cr.ok) { alert("Create failed."); resolve(null); return; }
          const { id } = await cr.json();
          _baSetlistsCache = []; // force refresh
          resolve(id);
        } catch (e) { alert("Create failed: " + e); resolve(null); }
      } else { resolve(val); }
    };
  });
}

// Quick favorites browser — opens a setlist-style overlay showing
// every favorited lyric so the curator can revisit / add-to-setlist
// without scrolling through the master list.
async function _baShowFavorites() {
  const detailEl = document.getElementById("blues-archive-setlists-detail");
  if (!detailEl) return;
  _baCurrentSetlistId = null;
  detailEl.innerHTML = `<div style="color:var(--muted)">Loading…</div>`;
  try {
    const r = await apiFetch("/api/blues-archive/favorites");
    if (!r.ok) { detailEl.innerHTML = `<div style="color:#e88">Failed: HTTP ${r.status}</div>`; return; }
    const { rows = [] } = await r.json();
    if (!rows.length) {
      detailEl.innerHTML = `<div style="color:var(--muted);padding:0.6rem 0;font-style:italic">No favorites yet. Open a lyric and click ★ to flag it.</div>`;
      return;
    }
    detailEl.innerHTML = `
      <h3 style="margin:0 0 0.6rem;font-size:1rem;color:var(--text)">★ Favorites <span style="color:var(--muted);font-size:0.78rem;font-weight:normal">· ${rows.length}</span></h3>
      <ol style="list-style:none;margin:0;padding:0">${rows.map((it, i) => `
        <li style="display:flex;align-items:center;gap:0.4rem;padding:0.35rem 0.4rem;border-bottom:1px solid var(--border)">
          <span style="color:var(--muted);font-size:0.78rem;width:1.8em;text-align:right">${i + 1}.</span>
          <span style="flex:1;min-width:0">
            <a href="#" onclick="event.preventDefault();_baOpenLyric(${it.id})" style="color:var(--text);text-decoration:none;font-weight:600">${escHtml(it.page_title || "(untitled)")}</a>
            <span style="color:var(--muted);font-size:0.78rem;margin-left:0.4rem">${escHtml(it.artist || "")}</span>
            ${it.tuning ? `<span style="color:#888;font-size:0.74rem;margin-left:0.4rem">${escHtml(it.tuning)}</span>` : ""}
          </span>
          <button class="archive-btn" onclick="_baAddLyricToSetlist(${it.id})" title="Add to a setlist…">+ Setlist</button>
        </li>
      `).join("")}</ol>`;
  } catch (e) {
    detailEl.innerHTML = `<div style="color:#e88">Failed: ${escHtml(e?.message || String(e))}</div>`;
  }
}
window._baShowFavorites = _baShowFavorites;

// Clear all active filters on the Lyrics tab. Bound to the
// "Clear filters" button that auto-shows/hides based on filter state.
function _baLyricsClearFilters() {
  const search    = document.getElementById("blues-archive-lyrics-search");
  const tuningSel = document.getElementById("blues-archive-lyrics-tuning");
  const unmatched = document.getElementById("blues-archive-lyrics-unmatched");
  const unpinned  = document.getElementById("blues-archive-lyrics-unpinned");
  const empty     = document.getElementById("blues-archive-lyrics-empty");
  if (search)    search.value = "";
  if (tuningSel) tuningSel.value = "";
  if (unmatched) unmatched.checked = false;
  if (unpinned)  unpinned.checked  = false;
  if (empty)     empty.checked     = false;
  _baLyricsTuning    = "";
  _baLyricsUnmatched = false;
  _baLyricsUnpinned  = false;
  _baLyricsEmpty     = false;
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
  if (_baLyricsUnpinned)  params.set("unpinned",  "1");
  if (_baLyricsEmpty)     params.set("empty",     "1");
  // Toggle the "Clear filters" button visibility based on whether
  // any filter is currently active.
  const clearBtn = document.getElementById("blues-archive-lyrics-clear");
  if (clearBtn) clearBtn.style.display = (q || _baLyricsTuning || _baLyricsUnmatched || _baLyricsUnpinned || _baLyricsEmpty) ? "" : "none";
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
    // Persist the current filter/page/sort tuple so re-entry restores it.
    _baPersistViewState();
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
  // Year of first release — resolved by /api/admin/lyrics/resolve-years-cheap
  // against the linked artist's discogs_releases. NULL until resolved
  // (rendered as a faint em-dash so the column doesn't visually wobble).
  const yr = Number.isFinite(Number(l.first_release_year)) ? Number(l.first_release_year) : null;
  const yrHtml = yr
    ? `<span style="font-variant-numeric:tabular-nums" title="${escHtml(l.first_release_source ? "via " + l.first_release_source : "")}">${yr}</span>`
    : `<span style="color:#555">—</span>`;
  // Full-text values used as title= for hover when the cell ellipses
  // out a long title / artist / snippet. Saves the user from squinting
  // or widening the column.
  const fullTitle  = String(l.page_title || "");
  const fullArtist = String(l.artist || "");
  const fullSnip   = (l.snippet || "").replace(/\s+/g, " ");
  // Search-this-track shortcut: opens a SeaDisco search prefilled with
  // title (q), artist (a), restricted to master+, sorted oldest first.
  // Param keys mirror restoreFromParams() in search.js.
  const searchQs = `?q=${encodeURIComponent(fullTitle)}` +
                   `&a=${encodeURIComponent(fullArtist)}` +
                   `&r=${encodeURIComponent("master+")}` +
                   `&s=${encodeURIComponent("year:asc")}`;
  const searchLink = `<a href="/${searchQs}" target="_blank" rel="noopener" onclick="event.stopPropagation()" class="ba-lyric-search" title="Search SeaDisco — masters+, oldest first">🔍</a>`;
  const visitedCls = _baVisitedLyrics.has(Number(l.id)) ? "ba-lyric-visited" : "";
  return `<tr data-lyric-row="${l.id}" class="${visitedCls}">
    <td style="font-weight:600;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(fullTitle)}">${searchLink} ${titleHtml}</td>
    <td style="color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(fullArtist)}">${artistHtml}${archiveAffordance}</td>
    <td style="text-align:right;font-size:0.82rem;padding-right:0.6rem;white-space:nowrap">${yrHtml}</td>
    <td style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--accent);cursor:pointer" onclick="_baOpenLyric(${l.id})" title="${escHtml(l.tuning || "")}">${escHtml(l.tuning || "")}</td>
    <td class="ba-lyric-snippet" style="font-size:0.7rem;cursor:pointer;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" onclick="_baOpenLyric(${l.id})" title="${escHtml(fullSnip.slice(0, 400))}">${escHtml(fullSnip.slice(0, 140))}…</td>
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
    <table class="api-log-table" style="font-size:0.84rem;width:100%;table-layout:fixed">
      <colgroup>
        <col style="width:28%">
        <col style="width:18%">
        <col style="width:60px">
        <col style="width:11%">
        <col>
        <col style="width:32px">
      </colgroup>
      <thead><tr>
        ${_baSortTh("Title",   "page_title",         S, "_baSortLyricsList")}
        ${_baSortTh("Artist",  "artist",             S, "_baSortLyricsList")}
        ${_baSortTh("Year",    "first_release_year", S, "_baSortLyricsList", "text-align:right;padding-right:0.6rem")}
        ${_baSortTh("Tuning",  "tuning",             S, "_baSortLyricsList")}
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

// ── Releases sub-tab ──────────────────────────────────────────────────
// Flat view over every blues_artists.discogs_releases entry. Powered
// by GET /api/blues-archive/releases (admin-gated). Filters: free-text
// search over title OR artist name, year (single int), type
// (release | master), role (Main | TrackAppearance | …). Sort is
// server-side; tie-break is artist then title for stability.
let _baReleasesPage = 0;
const _BA_RELEASES_LIMIT = 100;
let _baReleasesTotal = 0;
let _baReleasesSearchTimer = null;
let _baReleasesRowsCache = [];
const _baReleasesListSort = { key: "year", dir: "desc" };
const _BA_RELEASES_LIST_TYPES = { artist: "str", title: "str", year: "num", type: "str", role: "str" };

async function _baLoadReleases() {
  const rowsEl  = document.getElementById("blues-archive-releases-rows");
  const countEl = document.getElementById("blues-archive-releases-count");
  if (!rowsEl) return;
  const q     = (document.getElementById("blues-archive-releases-search")?.value || "").trim();
  const year  = (document.getElementById("blues-archive-releases-year")?.value || "").trim();
  const type  = document.getElementById("blues-archive-releases-type")?.value || "";
  const role  = document.getElementById("blues-archive-releases-role")?.value || "";
  const params = new URLSearchParams();
  if (q)    params.set("q",    q);
  if (year) params.set("year", year);
  if (type) params.set("type", type);
  if (role) params.set("role", role);
  if (_baReleasesListSort?.key) {
    params.set("sort",  _baReleasesListSort.key);
    params.set("order", _baReleasesListSort.dir);
  }
  params.set("limit",  String(_BA_RELEASES_LIMIT));
  params.set("offset", String(_baReleasesPage * _BA_RELEASES_LIMIT));
  // Show the "Clear filters" button when any filter is active.
  const clearBtn = document.getElementById("blues-archive-releases-clear");
  if (clearBtn) clearBtn.style.display = (q || year || type || role) ? "" : "none";
  const scrollY = window.scrollY;
  rowsEl.classList.add("ba-loading");
  try {
    const r = await apiFetch(`/api/blues-archive/releases?${params}`);
    if (!r.ok) { rowsEl.innerHTML = `<p style="color:#e88">Failed: HTTP ${r.status}</p>`; return; }
    const { rows = [], total = 0 } = await r.json();
    _baReleasesTotal = total;
    if (countEl) countEl.textContent = total
      ? `${total.toLocaleString()} release${total === 1 ? "" : "s"}`
      : "No matches.";
    _baReleasesRowsCache = rows;
    _baRenderReleasesTable();
    _baRenderReleasesPager();
    requestAnimationFrame(() => window.scrollTo(0, Math.min(scrollY, document.documentElement.scrollHeight - window.innerHeight)));
  } catch (e) {
    rowsEl.innerHTML = `<p style="color:#e88">Failed: ${escHtml(e?.message || String(e))}</p>`;
  } finally {
    rowsEl.classList.remove("ba-loading");
  }
}

function _baRenderReleasesTable() {
  const rowsEl = document.getElementById("blues-archive-releases-rows");
  if (!rowsEl) return;
  const rows = _baReleasesRowsCache;
  if (!rows.length) {
    rowsEl.innerHTML = `<p style="color:var(--muted);padding:0.5rem 0">No matches.</p>`;
    return;
  }
  const S = _baReleasesListSort;
  rowsEl.innerHTML = `
    <table class="api-log-table" style="font-size:0.84rem;width:100%;table-layout:fixed">
      <colgroup>
        <col style="width:44px">
        <col style="width:60px">
        <col>
        <col style="width:24%">
        <col style="width:80px">
        <col style="width:100px">
        <col style="width:1%">
      </colgroup>
      <thead><tr>
        <th style="width:44px"></th>
        ${_baSortTh("Year",   "year",   S, "_baSortReleasesList", "width:60px;text-align:right;padding-right:0.9rem")}
        ${_baSortTh("Title",  "title",  S, "_baSortReleasesList")}
        ${_baSortTh("Artist", "artist", S, "_baSortReleasesList")}
        ${_baSortTh("Type",   "type",   S, "_baSortReleasesList", "width:70px")}
        ${_baSortTh("Role",   "role",   S, "_baSortReleasesList", "width:100px")}
        <th style="width:1%"></th>
      </tr></thead>
      <tbody>${rows.map(_baReleaseRowHtml).join("")}</tbody>
    </table>`;
}

// Per-row HTML. Title links to discogs.com/release|/master/<id> in a
// new tab. Artist links to the in-app artist popup (so the curator
// can pivot to the artist's full record). Type+role rendered as
// muted text. Cells are click-through-friendly: stopPropagation on
// the action links so any future row-click handler doesn't pre-empt.
function _baReleaseRowHtml(row) {
  const id   = row.release_id;
  const type = (row.release_type || "release").toLowerCase();
  const url  = id ? `https://www.discogs.com/${type}/${id}` : "";
  // Title click opens the SeaDisco album/release modal (same modal
  // the search-results cards use) so the curator can see tracklist,
  // credits, marketplace, etc. without leaving the page.
  const titleText = escHtml(row.release_title || "(untitled)");
  const titleHtml = id
    ? `<a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenRelease(${id}, '${type}', '${escHtml(url)}')" style="color:var(--accent);text-decoration:none" title="Open ${type} popup">${titleText}</a>`
    : titleText;
  const artistHtml = row.artist_id
    ? `<a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenArtist(${row.artist_id})" style="color:var(--text);text-decoration:none" title="Open in Blues Archive">${escHtml(row.artist_name || "")}</a>`
    : escHtml(row.artist_name || "");
  const yr = Number.isFinite(Number(row.release_year)) ? Number(row.release_year) : null;
  const yrHtml = yr ? `<span style="font-variant-numeric:tabular-nums">${yr}</span>` : `<span style="color:var(--muted)">—</span>`;
  const fullTitle  = String(row.release_title || "(untitled)");
  const fullArtist = String(row.artist_name || "");
  // Cover thumb when release_cache has the release; placeholder
  // otherwise. Click opens the release modal (same as Title).
  const thumb = row.cover_thumb || "";
  const thumbInner = thumb
    ? `<img src="${escHtml(thumb)}" alt="" loading="lazy" style="width:36px;height:36px;object-fit:cover;border-radius:3px;display:block;background:var(--surface-raised)" />`
    : `<div style="width:36px;height:36px;border-radius:3px;background:var(--surface-raised);display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:0.9rem">♪</div>`;
  const thumbHtml = id
    ? `<a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenRelease(${id}, '${type}', '${escHtml(url)}')" title="Open ${type} popup">${thumbInner}</a>`
    : thumbInner;
  return `<tr data-release-id="${id || ""}">
    <td style="padding:0.2rem 0.3rem">${thumbHtml}</td>
    <td style="text-align:right;font-size:0.82rem;padding-right:0.9rem;white-space:nowrap">${yrHtml}</td>
    <td style="padding-left:0.4rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(fullTitle)}">${titleHtml}</td>
    <td style="color:var(--text);padding-left:0.6rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(fullArtist)}">${artistHtml}</td>
    <td style="font-size:0.78rem;color:var(--muted);padding-left:0.6rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(row.release_type || "")}">${escHtml(row.release_type || "")}</td>
    <td style="font-size:0.78rem;color:var(--muted);padding-left:0.6rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(row.role || "")}">${escHtml(row.role || "")}</td>
    <td></td>
  </tr>`;
}

function _baRenderReleasesPager() {
  const el = document.getElementById("blues-archive-releases-pager");
  if (!el) return;
  const pageCount = Math.max(1, Math.ceil(_baReleasesTotal / _BA_RELEASES_LIMIT));
  const cur = _baReleasesPage + 1;
  if (pageCount <= 1) { el.innerHTML = ""; return; }
  el.innerHTML = `
    <button class="archive-btn" ${cur <= 1 ? "disabled" : ""} onclick="_baReleasesGoToPage(${_baReleasesPage - 1})">‹ Prev</button>
    <span style="color:var(--muted)">Page ${cur} / ${pageCount}</span>
    <button class="archive-btn" ${cur >= pageCount ? "disabled" : ""} onclick="_baReleasesGoToPage(${_baReleasesPage + 1})">Next ›</button>
  `;
}

function _baReleasesGoToPage(p) { _baReleasesPage = Math.max(0, p); _baLoadReleases(); }
window._baReleasesGoToPage = _baReleasesGoToPage;

function _baSortReleasesList(key) {
  _baToggleSort(_baReleasesListSort, key);
  _baReleasesPage = 0;
  _baLoadReleases();
}
window._baSortReleasesList = _baSortReleasesList;

function _baReleasesDebouncedSearch() {
  if (_baReleasesSearchTimer) clearTimeout(_baReleasesSearchTimer);
  _baReleasesSearchTimer = setTimeout(() => { _baReleasesPage = 0; _baLoadReleases(); }, 280);
}
window._baReleasesDebouncedSearch = _baReleasesDebouncedSearch;

function _baReleasesApplyFilters() { _baReleasesPage = 0; _baLoadReleases(); }
window._baReleasesApplyFilters = _baReleasesApplyFilters;

function _baReleasesClearFilters() {
  const s = document.getElementById("blues-archive-releases-search");
  const y = document.getElementById("blues-archive-releases-year");
  const t = document.getElementById("blues-archive-releases-type");
  const r = document.getElementById("blues-archive-releases-role");
  if (s) s.value = "";
  if (y) y.value = "";
  if (t) t.value = "";
  if (r) r.value = "";
  _baReleasesPage = 0;
  _baLoadReleases();
}
window._baReleasesClearFilters = _baReleasesClearFilters;

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
      // Hide chips with zero instances — clicking them produces an
      // empty filtered view and they only add noise to the strip.
      // The "Artists" total chip is always shown so the strip has at
      // least one anchor when the DB is freshly empty.
      const count = Number(n) || 0;
      if (count === 0 && !opts.alwaysShow) return "";
      const click = opts.onclick ? ` onclick="${opts.onclick}"` : "";
      const cur = opts.onclick ? "cursor:pointer;" : "";
      const tone = opts.tone === "warn" ? "color:#e8a85a" : "color:var(--muted)";
      // title= tooltip on every chip so the hover spells out what
      // the bucket means + (for clickable ones) what the click does.
      return `<span class="ba-stat-chip"${click} title="${escHtml(opts.title || label)}" style="${cur}font-size:0.7rem;padding:0.05rem 0.25rem;${tone}">${escHtml(label)}: <strong style="color:var(--text)">${count.toLocaleString()}</strong></span>`;
    };
    el.innerHTML = [
      chip("Artists", s.artists_total, {
        alwaysShow: true,
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
// Expose alongside _baLoadList so blues-admin.js can repaint the
// stats strip when the editor mutates a row (e.g. clearing a photo
// shifts the empty/with-photo bucket counts).
window._baLoadStats = _baLoadStats;

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

// PDF mirror of the CSV export. Same shape: hit /export.pdf, download
// the blob. The endpoint streams a multi-MB document so we still wait
// for it as a blob (no easy chunked progress in the fetch flow). Long
// runs flash "Exporting…" on the button until it lands.
async function _baExportLyricsPdf() {
  const btn = document.getElementById("blues-export-lyrics-pdf-btn");
  const orig = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "Building PDF…"; }
  try {
    const r = await apiFetch("/api/admin/lyrics/export.pdf");
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const blob = await r.blob();
    const fname = `seadisco-lyrics-${new Date().toISOString().slice(0, 10)}.pdf`;
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
window._baExportLyricsPdf = _baExportLyricsPdf;

// Word-openable .doc export — HTML masquerading as application/msword.
// Same flow as the CSV / PDF handlers: hit the endpoint, blob-download.
async function _baExportLyricsDoc() {
  const btn = document.getElementById("blues-export-lyrics-doc-btn");
  const orig = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "Exporting…"; }
  try {
    const r = await apiFetch("/api/admin/lyrics/export.doc");
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const blob = await r.blob();
    const fname = `seadisco-lyrics-${new Date().toISOString().slice(0, 10)}.doc`;
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
window._baExportLyricsDoc = _baExportLyricsDoc;

// Artist-profile PDF — same flow as the lyrics one. Big bib of every
// artist alphabetised: name + dates / hometown / first-last recording
// + pseudonyms + bands + bio + every Discogs release oldest→newest.
async function _baExportArtistsPdf() {
  const btn = document.getElementById("blues-export-artists-pdf-btn");
  const orig = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "Building PDF…"; }
  try {
    const r = await apiFetch("/api/admin/blues/export.pdf");
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const blob = await r.blob();
    const fname = `seadisco-artists-${new Date().toISOString().slice(0, 10)}.pdf`;
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
window._baExportArtistsPdf = _baExportArtistsPdf;

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
    exportLyricsPdf:  () => _baExportLyricsPdf(),
    exportLyricsDoc:  () => _baExportLyricsDoc(),
    exportArtistsPdf: () => _baExportArtistsPdf(),
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
  // Also reload the underlying artist list so the row reflects the
  // save (cleared photo → empty placeholder, updated name → new
  // entry text, etc.) without needing a manual refresh.
  window._bluesDbAfterSaveOnce = (savedId) => {
    const targetId = Number(savedId || id);
    if (Number.isFinite(targetId)) _baOpenArtist(targetId);
    if (typeof _baLoadList === "function") { try { _baLoadList(); } catch {} }
    if (typeof _baLoadStats === "function") { try { _baLoadStats(); } catch {} }
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
  // Set the filter state + checkbox BEFORE switching the subtab so
  // the switch's auto-load already runs with unmatched=1. Otherwise
  // two requests race: switchSubtab's no-filter load + the follow-up
  // _baLyricsApplyUnmatched load. The no-filter response sometimes
  // wins the race and overwrites the filtered rows with the full
  // 4k+ list.
  _baLyricsUnmatched = true;
  _baLyricsPage      = 0;
  const cb = document.getElementById("blues-archive-lyrics-unmatched");
  if (cb) cb.checked = true;
  _baSwitchSubtab("lyrics");
}
window._baJumpOrphans = _baJumpOrphans;

function _baJumpMissingTuning() {
  // Same race fix as orphans — set the tuning filter BEFORE switching
  // so the switch's load already includes the filter param.
  _baLyricsTuning = "(unspecified)";
  _baLyricsPage   = 0;
  // Update the dropdown UI so it reflects the active filter once the
  // tab is visible. If the dropdown hasn't loaded its options yet,
  // queue the value-set for after the lazy fetch.
  const applySelection = () => {
    const sel = document.getElementById("blues-archive-lyrics-tuning");
    if (sel) sel.value = "(unspecified)";
  };
  if (_baLyricsTuningsLoaded) applySelection();
  else _baLoadTunings().then(applySelection);
  _baSwitchSubtab("lyrics");
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
