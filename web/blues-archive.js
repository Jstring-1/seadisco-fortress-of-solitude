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
// Bulk-select state for the artist list. A Set of artist ids that
// survives pagination (rows re-bind to it every render); the
// "Select all matching" fan-out can push in ids beyond the current
// page. Capped flag mirrors the lyrics bulk editor.
const _baArtistsSelectedIds = new Set();
let _baArtistsSelectedCapped = false;
// Category filter — set by the stats-strip chips, cleared by clicking
// "Artists" (or the × on the filter indicator). One of:
//   "", "with_both", "with_lyrics_only", "with_releases_only", "empty"
let _baListCategory = "";
const _baListSort = { key: "name", dir: "asc" };
const _BA_LIST_TYPES = { name: "str", discogs_id: "num", has_photo: "num", first_release_year: "num", lyrics_count: "num", releases_count: "num", strict_count: "num" };

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
        noArtist:  _baLyricsNoArtist,
        noYear:    _baLyricsNoYear,
        pinned:    _baLyricsPinned,
        favorites: _baLyricsFavorites,
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

// Artist-popup collapsed-section state. localStorage-backed map keyed
// by section name ("bio" / "tunings" / "lyrics" / "releases"). True =
// collapsed. Persists across sessions per the user's last choice.
const _BA_POPUP_COLLAPSE_KEY = "sd_ba_artist_popup_collapse";
function _baGetCollapsedState() {
  try {
    const o = JSON.parse(localStorage.getItem(_BA_POPUP_COLLAPSE_KEY) || "{}");
    return o && typeof o === "object" ? o : {};
  } catch { return {}; }
}
function _baSetSectionCollapsed(section, collapsed) {
  const s = _baGetCollapsedState();
  s[section] = !!collapsed;
  try { localStorage.setItem(_BA_POPUP_COLLAPSE_KEY, JSON.stringify(s)); } catch {}
}
window._baSetSectionCollapsed = _baSetSectionCollapsed;

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

// Rescrape ban-list viewer/editor — fetches /api/admin/lyrics/bans
// and renders the rows in a simple overlay with a × Remove button per
// entry. Two columns of bans (title vs artist) appear side-by-side
// since the use case is "I want to see what's blocked and selectively
// unblock things". Refreshes itself after each remove so the row
// vanishes without a manual reload.
async function _baOpenBansOverlay() {
  let overlay = document.getElementById("ba-bans-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "ba-bans-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "1300", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "3rem 1rem", overflowY: "auto",
    });
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(800px,100%);max-height:80vh;overflow-y:auto">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.8rem">
        <h3 style="margin:0;font-size:1.05rem">Rescrape ban list</h3>
        <button class="archive-btn" onclick="document.getElementById('ba-bans-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <p style="margin:0 0 0.8rem;font-size:0.82rem;color:var(--muted)">Titles and artist names listed here are SKIPPED by the wiki rescrape. Use × to unban a row — the next rescrape will treat that title / artist as eligible again.</p>
      <div id="ba-bans-body" style="font-size:0.86rem">Loading…</div>
    </div>`;
  await _baRenderBansBody();
}
window._baOpenBansOverlay = _baOpenBansOverlay;

async function _baRenderBansBody() {
  const body = document.getElementById("ba-bans-body");
  if (!body) return;
  try {
    const r = await apiFetch("/api/admin/lyrics/bans?limit=500");
    if (!r.ok) { body.textContent = `Failed: HTTP ${r.status}`; return; }
    const { rows = [] } = await r.json();
    if (!rows.length) {
      body.innerHTML = `<p style="color:var(--muted);font-style:italic;padding:0.6rem 0">No bans yet. The "Delete + don't re-add" button on artist popups adds rows here.</p>`;
      return;
    }
    const byKind = { artist: [], title: [] };
    for (const b of rows) {
      const arr = byKind[b.kind] || (byKind[b.kind] = []);
      arr.push(b);
    }
    const col = (kind, label) => {
      const list = byKind[kind] || [];
      const items = list.map(b => {
        const when = b.banned_at ? new Date(b.banned_at).toLocaleDateString() : "";
        return `<li style="display:flex;align-items:baseline;gap:0.4rem;padding:0.25rem 0;border-bottom:1px solid rgba(255,255,255,0.04)">
          <button class="archive-btn" onclick="_baRemoveBan(${b.id})" title="Unban this ${kind}" style="font-size:0.78rem;padding:0.1rem 0.45rem;color:#e88;border-color:rgba(232,136,136,0.4)">×</button>
          <span style="flex:1;color:var(--text)">${escHtml(b.value || "")}</span>
          <span style="font-size:0.74rem;color:var(--muted)" title="${escHtml(b.reason || "")}">${escHtml(when)}</span>
        </li>`;
      }).join("");
      return `<div style="flex:1;min-width:280px">
        <h4 style="margin:0 0 0.4rem;font-size:0.88rem;color:var(--accent)">${escHtml(label)} <span style="color:var(--muted);font-weight:400">(${list.length})</span></h4>
        <ul style="list-style:none;margin:0;padding:0">${items || `<li style="color:var(--muted);font-style:italic">none</li>`}</ul>
      </div>`;
    };
    body.innerHTML = `<div style="display:flex;gap:1.2rem;flex-wrap:wrap">${col("artist", "Banned artists")}${col("title", "Banned titles")}</div>`;
  } catch (e) {
    body.textContent = `Failed: ${e?.message || e}`;
  }
}

async function _baRemoveBan(id) {
  if (!confirm("Unban this row? The next rescrape will be allowed to add it back.")) return;
  try {
    const r = await apiFetch(`/api/admin/lyrics/bans/${id}`, { method: "DELETE" });
    if (!r.ok) { alert(`Unban failed: HTTP ${r.status}`); return; }
    await _baRenderBansBody();
  } catch (e) {
    alert(`Unban failed: ${e?.message || e}`);
  }
}
window._baRemoveBan = _baRemoveBan;

// Reset button on the Lyrics tab — wipes the visited set + its LS
// blob and yanks the .ba-lyric-visited class off every rendered row
// so the table immediately reads as "all unread" without a re-fetch.
function _baClearVisitedLyrics() {
  if (!_baVisitedLyrics.size) {
    if (typeof showToast === "function") showToast("No visited lyrics to reset", "info");
    return;
  }
  if (!confirm(`Clear visited state for ${_baVisitedLyrics.size} lyric${_baVisitedLyrics.size === 1 ? "" : "s"}?`)) return;
  const n = _baVisitedLyrics.size;
  _baVisitedLyrics.clear();
  try { localStorage.removeItem(_BA_VISITED_LYRICS_KEY); } catch {}
  document.querySelectorAll("tr.ba-lyric-visited").forEach(tr => {
    tr.classList.remove("ba-lyric-visited");
  });
  if (typeof showToast === "function") {
    showToast(`Reset visited state on ${n} lyric${n === 1 ? "" : "s"}`);
  }
}
window._baClearVisitedLyrics = _baClearVisitedLyrics;
const _baReleasesSort = { key: "year", dir: "asc" };
const _BA_RELEASES_TYPES = { year: "num", title: "str", label: "str", type: "str" };
// Free-text filter for the artist-popup releases section. Kept in a
// module var (not just the DOM input) so it survives the full-popup
// re-render that a sort-header click triggers. Reset to "" on each fresh
// artist open in _baOpenArtist so it doesn't bleed across artists.
let _baPopupReleasesFilter = "";

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
function _baSortTh(label, key, state, fn, extraStyle, title) {
  const active = state.key === key;
  const arrow = active ? (state.dir === "desc" ? "▼" : "▲") : "";
  const titleAttr = title ? ` title="${String(title).replace(/"/g, "&quot;")}"` : "";
  return `<th class="admin-sort-th${active ? " is-active" : ""}"${titleAttr} style="padding:0.3rem 0.5rem;cursor:pointer;user-select:none;${extraStyle || ""}" onclick="${fn}('${key}')">${label}<span class="admin-sort-arrow" style="margin-left:0.3rem">${arrow}</span></th>`;
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
  // Artists / Releases / Connections were removed — Lyrics is the
  // default landing tab now.
  if (typeof _baSwitchSubtab === "function") _baSwitchSubtab("lyrics");
  else if (typeof _baLoadLyrics === "function") _baLoadLyrics();
  // Stats strip — fire non-blocking.
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
  // (The artist-enrichment "resume bulk job" poll that used to run here
  // was removed with the artist subsystem — its endpoint is gone, so the
  // poll only 404'd.)
  // The ?baArtist=ID deep-link was removed with the artist subsystem;
  // nothing deep-links now, so the persisted view-state restore below
  // always runs.
  const hasUrlDeepLink = false;
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
      _baLyricsNoArtist  = !!saved.lyrics.noArtist;
      _baLyricsNoYear    = !!saved.lyrics.noYear;
      _baLyricsPinned    = !!saved.lyrics.pinned;
      _baLyricsFavorites = !!saved.lyrics.favorites;
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
      const na = document.getElementById("blues-archive-lyrics-no-artist");
      if (na) na.checked = !!saved.lyrics.noArtist;
      const ny = document.getElementById("blues-archive-lyrics-no-year");
      if (ny) ny.checked = !!saved.lyrics.noYear;
      const pn = document.getElementById("blues-archive-lyrics-pinned");
      if (pn) pn.checked = !!saved.lyrics.pinned;
      const fv = document.getElementById("blues-archive-lyrics-favorites");
      if (fv) fv.checked = !!saved.lyrics.favorites;
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
    } else if (saved?.subtab && (saved.subtab === "artists" || saved.subtab === "releases" || saved.subtab === "tunings" || saved.subtab === "connections")) {
      // Non-lyrics subtabs: filter / sort / page state lives in module
      // vars that persist within the session (display:none keeps the
      // input values, the JS state is never reset). Just land the user
      // on the right tab — _baSwitchSubtab triggers the appropriate
      // _baLoad* call which honours the persisted state.
      _baSwitchSubtab(saved.subtab);
    }
  }
}
window.initBluesArchiveView = initBluesArchiveView;



// Expose so blues-admin.js's bluesDbRenderList() can refresh this
// grid after inline editor mutations (Refresh from Discogs, Pick
// Discogs match, per-row enrich, etc.) without admin.html present.




// ── Bulk artist select + delete ──────────────────────────────────────
// Mirrors the lyrics bulk editor: per-row checkboxes bind to a Set that
// persists across pagination; a bulk bar appears once anything's picked.


// Refresh the header tri-state checkbox + bulk bar from the Set. Called
// after each table render and every selection change.





// Open an artist as an overlay popup. Was previously a page swap that
// hid .blues-archive-list and rendered into #blues-archive-detail; the
// popup pattern reads better (the list stays put, close = ×) and lines
// up with how lyric viewing already works. #blues-archive-detail is no
// longer touched — left in the DOM for backward compat only.
// LRU cache of artist payloads. Hit-rate is near 100% when the user
// walks the pseudonym / band / linked-artist pill chain because
// _baPreloadLinkedArtists pre-fetches every neighbor in the
// background after the current artist renders.
const _baArtistCache = new Map();
const _BA_CACHE_LIMIT = 50;


// Background fetch of every linked artist (pseudonyms / bands /
// spouse / mentor / family / traveled-with) so clicking a pill
// renders from cache. Chunks of 5 in-flight to avoid stampeding
// the API on artists with long link lists. Silent on errors —
// a miss just means the cache won't hit for that one and we fall
// back to the spinner-fetch path.




// In-place filter for the artist-popup releases table. Reads the field,
// stashes the value in the module var (so a sort re-render can restore
// it), and shows/hides rows by substring match against each row's
// data-relfilter haystack. Updates the "N of M" count beside the input.

// Cascade-delete an artist AND every lyric tied to them. Two-step
// confirm (the count of affected lyrics is shown after we look it
// up) so a stray click can't nuke a row with 40 lyrics by accident.
// On success: closes the popup, reloads the grid + stats so the
// numbers reflect the deletion.
//
// opts.ban=true → also adds the artist name and every deleted page
// title to blues_lyrics_bans so a future rescrape doesn't re-add
// them. Pass true from the "Delete + don't re-add" button; the
// plain delete button calls without ban.

// Dismiss the artist popup. The function name predates the popup
// rewrite (it used to swap back to the list page); kept so existing
// onclick attributes don't need rewriting.

// Session caches for the merged lyric popup's datalists. Both
// endpoints used to fire on every popup open (the artist list pulls
// 500 rows) and were serialised — that's the lag the user noticed.
// Holding the rendered <option> strings in memory means the second
// (and every subsequent) popup open is a pure local lookup. _baLazyArtist…
// is exposed as window._baInvalidateArtistOptionsCache so the
// "+ Create as new" flow can blow the cache when a new artist row
// gets minted mid-session.
let _baTuningOptionsCache = null;
let _baTuningOptionsPromise = null;
let _baArtistOptionsCache = null;
let _baArtistOptionsPromise = null;
async function _baGetTuningOptionsCached() {
  if (_baTuningOptionsCache != null) return _baTuningOptionsCache;
  if (!_baTuningOptionsPromise) {
    _baTuningOptionsPromise = (async () => {
      try {
        const tr = await apiFetch("/api/admin/lyrics/tunings");
        const tunings = tr.ok ? ((await tr.json()).tunings ?? []) : [];
        const html = tunings
          .filter(t => t.tuning && t.tuning !== "(unspecified)")
          .map(t => `<option value="${escHtml(t.tuning)}">`)
          .join("");
        _baTuningOptionsCache = html;
        return html;
      } catch { _baTuningOptionsCache = ""; return ""; }
      finally { _baTuningOptionsPromise = null; }
    })();
  }
  return _baTuningOptionsPromise;
}

// Reuse the same lyric viewer the admin Lyrics tab uses. The admin
// view is on a different page (/admin), so we render a simple inline
// popup here using the existing chronam-style overlay pattern.
async function _baOpenLyric(id) {
  // Mark the row visited immediately (before the fetch resolves) so
  // the snippet column dims even if the user dismisses the overlay
  // before the body lands.
  _baMarkLyricVisited(id);
  try {
    // Three fetches the popup used to do sequentially (lyric →
    // tunings → 500-row artist list) are now parallel + cached.
    // Tunings and artists are session-cached because they barely
    // change during a curation pass; if they DO change, the cached
    // datalist is a superset by the time autocomplete matters.
    const r = await apiFetch(`/api/admin/lyrics/${id}`);
    if (!r.ok) return;
    const row = await r.json();
    const tuningOpts = await _baGetTuningOptionsCached();
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
    // Merged viewer + editor: every field is an inline input. Save is
    // disabled until any value differs from what the server returned.
    // The previous read-only viewer + separate editor overlay flow is
    // gone — one popup handles both reading and editing.
    const releaseLink = row.discogs_release_id
      ? `<a href="https://www.discogs.com/release/${row.discogs_release_id}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none;font-size:0.72rem;margin-left:0.4rem" title="Open release on Discogs">↗</a>`
      : "";
    const masterLink = row.discogs_master_id
      ? `<a href="https://www.discogs.com/master/${row.discogs_master_id}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none;font-size:0.72rem;margin-left:0.4rem" title="Open master on Discogs">↗</a>`
      : "";
    overlay.innerHTML = `
      <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(820px,100%)">
        <!-- Action bar: all controls on a single row at the top so the
             title input directly below gets the full popup width. Makes
             editing long titles a lot easier than the previous layout
             that squeezed the input around the buttons. -->
        <div style="display:flex;gap:0.4rem;align-items:center;justify-content:flex-end;margin-bottom:0.5rem;flex-wrap:wrap">
          <button class="archive-btn" data-ba-fav-id="${row.id}" onclick="_baToggleLyricFavorite(${row.id})" title="${_baFavoriteIds.has(Number(row.id)) ? "Favorited — click to un-favorite" : "Click to favorite"}" style="font-size:1.1rem;padding:0 0.55rem;color:#ffd166">${_baFavoriteIds.has(Number(row.id)) ? "★" : "☆"}</button>
          <button id="ba-edit-save-btn" class="archive-btn archive-btn-suggest" onclick="_baSaveLyricEdit(${row.id})" title="Save any changed fields" disabled style="opacity:0.55">Save</button>
          <button class="archive-btn" onclick="_baDeleteLyric(${row.id})" style="color:#e88" title="Permanently delete this lyric row (a future wiki rescrape can pull the same title back in unless you also ban it).">Delete</button>
          <button class="archive-btn" onclick="_baDeleteAndBanLyric(${row.id}, ${JSON.stringify(row.page_title || "")})" style="color:#e88" title="Delete this row AND fingerprint its EXACT text body (SHA-256 of the normalized plaintext). A re-upload of the same body gets skipped on rescrape, but a re-upload with even a single character changed comes through normally. Doesn't care about title or artist. Manage bans from the Lyrics toolbar Bans button.">Delete + block this exact text</button>
          <button class="archive-btn" onclick="document.getElementById('ba-lyric-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
        </div>
        <input id="ba-edit-title" type="text" value="${escHtml(row.page_title || "")}" placeholder="(title required)" style="width:100%;font-size:1.05rem;font-weight:600;padding:0.45rem 0.6rem;background:transparent;color:var(--text);border:1px solid var(--border);border-radius:4px;margin-bottom:0.6rem" onfocus="this.style.background='rgba(255,255,255,0.03)'" onblur="this.style.background='transparent'" oninput="_baLyricDirty()">
        <datalist id="ba-tuning-options">${tuningOpts}</datalist>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.5rem;margin-bottom:0.5rem">
          <div>
            <label style="display:block;margin:0 0 0.2rem;font-size:0.74rem;color:var(--muted)">Artist</label>
            <input id="ba-edit-artist" type="text" value="${escHtml(row.artist || "")}" placeholder="(leave blank to clear)" style="width:100%;padding:0.35rem 0.55rem;font-size:0.82rem" oninput="_baLyricDirty()" autocomplete="off">
          </div>
          <div>
            <label style="display:block;margin:0 0 0.2rem;font-size:0.74rem;color:var(--muted)">Tuning</label>
            <input id="ba-edit-tuning" type="text" value="${escHtml(row.tuning || "")}" list="ba-tuning-options" placeholder="(leave blank to clear)" style="width:100%;padding:0.35rem 0.55rem;font-size:0.82rem" oninput="_baLyricDirty()">
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr 110px;gap:0.5rem;margin-bottom:0.6rem">
          <div>
            <label style="display:block;margin:0 0 0.2rem;font-size:0.74rem;color:var(--muted)">Discogs release ID${releaseLink}</label>
            <input id="ba-edit-release-id" type="number" min="1" value="${row.discogs_release_id ?? ""}" placeholder="(optional)" style="width:100%;padding:0.35rem 0.55rem;font-size:0.82rem" oninput="_baLyricDirty()">
          </div>
          <div>
            <label style="display:block;margin:0 0 0.2rem;font-size:0.74rem;color:var(--muted)">Discogs master ID${masterLink}</label>
            <input id="ba-edit-master-id" type="number" min="1" value="${row.discogs_master_id ?? ""}" placeholder="(optional)" style="width:100%;padding:0.35rem 0.55rem;font-size:0.82rem" oninput="_baLyricDirty()">
          </div>
          <div>
            <label style="display:block;margin:0 0 0.2rem;font-size:0.74rem;color:var(--muted)">First year</label>
            <input id="ba-edit-first-year" type="number" min="1850" max="2100" value="${row.first_release_year ?? ""}" placeholder="YYYY" style="width:100%;padding:0.35rem 0.55rem;font-size:0.82rem" oninput="_baLyricDirty()">
          </div>
        </div>
        <textarea id="ba-edit-plaintext" rows="20" placeholder="Paste or type the song lyrics here…" style="width:100%;padding:0.55rem 0.8rem;font-size:0.88rem;line-height:1.5;font-family:inherit;max-height:60vh" oninput="_baLyricDirty()">${escHtml(row.plaintext || "")}</textarea>
        <div id="ba-edit-status" style="font-size:0.74rem;color:var(--muted);margin-top:0.4rem;min-height:1em"></div>
      </div>
    `;
    // Snapshot the initial form values so the Save button can stay
    // disabled until the user actually changes something.
    overlay.dataset.baInitial = JSON.stringify({
      page_title: row.page_title || "",
      artist:     row.artist || "",
      tuning:     row.tuning || "",
      discogs_release_id: row.discogs_release_id == null ? "" : String(row.discogs_release_id),
      discogs_master_id:  row.discogs_master_id  == null ? "" : String(row.discogs_master_id),
      first_release_year: row.first_release_year == null ? "" : String(row.first_release_year),
      plaintext:  row.plaintext || "",
    });
  } catch (e) {
    console.warn("[blues-archive] lyric open failed:", e);
  }
}
window._baOpenLyric = _baOpenLyric;

// Toggles the Save button's disabled state by comparing the current
// form values to the initial snapshot stashed on overlay.dataset. Each
// inline input calls this from oninput, so the button accurately
// reflects "is anything dirty right now."
function _baLyricDirty() {
  const overlay = document.getElementById("ba-lyric-overlay");
  const btn = document.getElementById("ba-edit-save-btn");
  if (!overlay || !btn) return;
  let initial;
  try { initial = JSON.parse(overlay.dataset.baInitial || "{}"); }
  catch { initial = {}; }
  const cur = {
    page_title: (document.getElementById("ba-edit-title")?.value ?? "").trim(),
    artist:     document.getElementById("ba-edit-artist")?.value ?? "",
    tuning:     document.getElementById("ba-edit-tuning")?.value ?? "",
    discogs_release_id: document.getElementById("ba-edit-release-id")?.value ?? "",
    discogs_master_id:  document.getElementById("ba-edit-master-id")?.value ?? "",
    first_release_year: document.getElementById("ba-edit-first-year")?.value ?? "",
    plaintext:  document.getElementById("ba-edit-plaintext")?.value ?? "",
  };
  const dirty = Object.keys(cur).some(k => String(cur[k]) !== String(initial[k] ?? ""));
  btn.disabled = !dirty;
  btn.style.opacity = dirty ? "1" : "0.55";
}
window._baLyricDirty = _baLyricDirty;

// Open a release in the same modal the main site uses (modal.js's
// openModal). Falls back to opening the Discogs page if the function
// isn't around.

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
  // For an existing row, the viewer popup IS the editor now — route
  // there so we don't pop a second overlay.
  if (id != null) { _baOpenLyric(id); return; }
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
          ${isNew && row.page_title && row.artist ? `<div style="font-size:0.76rem;color:var(--muted);margin-top:0.15rem"><a href="https://www.google.com/search?q=${encodeURIComponent(`${row.artist} Lyrics ${row.page_title}`)}" target="_blank" rel="noopener" style="color:var(--accent)" title="Open a Google search to find the lyrics text">🔍 Google "${escHtml(row.artist)} Lyrics ${escHtml(row.page_title)}" ↗</a></div>` : ""}
        </div>
        <button class="archive-btn" onclick="document.getElementById('ba-lyric-edit-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <datalist id="ba-tuning-options">${opts}</datalist>
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)">Title</label>
      <input id="ba-edit-title" type="text" value="${escHtml(row.page_title || "")}" placeholder="(required)" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem">
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)">Artist</label>
      <input id="ba-edit-artist" type="text" value="${escHtml(row.artist || "")}" placeholder="(leave blank to clear)" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem" autocomplete="off">
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
      ` : ""}
      <label style="display:block;margin:0.6rem 0 0.3rem;font-size:0.82rem;color:var(--muted)" title="The lyric body the viewer popup renders verbatim. Editing here overwrites the stored plaintext directly — for wiki-sourced rows, use this to override the scraped body with hand-corrected lyrics.">Lyrics (plaintext)</label>
      <textarea id="ba-edit-plaintext" rows="14" placeholder="Paste or type the song lyrics here…" style="width:100%;padding:0.45rem 0.7rem;font-size:0.88rem;font-family:inherit;line-height:1.45">${escHtml(row.plaintext || "")}</textarea>
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
    // Re-stamp the mini-player so the 📜 appears immediately if the
    // new lyric matches the currently-playing track.
    try { window._baStampMiniPlayer?.(); } catch {}
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
  const plaintext  = document.getElementById("ba-edit-plaintext")?.value ?? "";
  const discogs_release_id = releaseIdRaw === "" ? null : Number(releaseIdRaw);
  const discogs_master_id  = masterIdRaw  === "" ? null : Number(masterIdRaw);
  const first_release_year = firstYearRaw === "" ? null : Number(firstYearRaw);
  if (!page_title) { if (statusEl) statusEl.textContent = "Title is required."; return; }
  if (statusEl) statusEl.textContent = "Saving…";
  try {
    const r = await apiFetch(`/api/admin/lyrics/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ artist, tuning, page_title, discogs_release_id, discogs_master_id, first_release_year, plaintext }),
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
      // Derive `snippet` from the returned plaintext so the row
      // re-render shows the new body in the Snippet column. The PATCH
      // endpoint returns the full blues_lyrics row, but `snippet` is
      // a computed alias on list queries (substring(plaintext,1,240))
      // — getLyricById doesn't ship it. Match the server's 240-char
      // cap so what the user sees after Save lines up with what the
      // next list refresh will show.
      if (updated.snippet == null && typeof updated.plaintext === "string") {
        updated.snippet = updated.plaintext.slice(0, 240);
      }
      // Surgical DOM patch — both master list <tr> AND artist popup
      // sub-table <tr> are swapped if present. Single-row swap
      // doesn't disturb the surrounding scroll geometry.
      _baPatchLyricRowEverywhere(updated);
      // Merged viewer+editor popup: keep it mounted and patch it in
      // place. Close-and-reopen would re-fetch the lyric, the tunings
      // datalist, AND a 500-row artist datalist — felt sluggish on
      // every save. Server already returned the canonical row, so we
      // just sync the form inputs, reset the dirty snapshot, and
      // flash a "Saved" hint. No DOM remount, no extra network.
      const overlay = document.getElementById("ba-lyric-overlay");
      if (overlay) {
        const setVal = (elId, v) => {
          const el = document.getElementById(elId);
          if (el && el.value !== String(v ?? "")) el.value = String(v ?? "");
        };
        setVal("ba-edit-title",       updated.page_title || "");
        setVal("ba-edit-artist",      updated.artist || "");
        setVal("ba-edit-tuning",      updated.tuning || "");
        setVal("ba-edit-release-id",  updated.discogs_release_id == null ? "" : updated.discogs_release_id);
        setVal("ba-edit-master-id",   updated.discogs_master_id  == null ? "" : updated.discogs_master_id);
        setVal("ba-edit-first-year",  updated.first_release_year == null ? "" : updated.first_release_year);
        setVal("ba-edit-plaintext",   updated.plaintext || "");
        // Reset the initial-values snapshot so the dirty check reads
        // clean again and the Save button stays disabled until the
        // next real edit.
        overlay.dataset.baInitial = JSON.stringify({
          page_title: updated.page_title || "",
          artist:     updated.artist || "",
          tuning:     updated.tuning || "",
          discogs_release_id: updated.discogs_release_id == null ? "" : String(updated.discogs_release_id),
          discogs_master_id:  updated.discogs_master_id  == null ? "" : String(updated.discogs_master_id),
          first_release_year: updated.first_release_year == null ? "" : String(updated.first_release_year),
          plaintext:  updated.plaintext || "",
        });
        const saveBtn = document.getElementById("ba-edit-save-btn");
        if (saveBtn) { saveBtn.disabled = true; saveBtn.style.opacity = "0.55"; }
        // Re-stamp the mini-player so a title/artist edit that newly
        // matches the playing track shows 📜 immediately.
        try { window._baStampMiniPlayer?.(); } catch {}
        // Brief "Saved" confirmation that fades on the next dirty edit.
        if (statusEl) {
          statusEl.textContent = "Saved.";
          statusEl.style.color = "#7bc77b";
          setTimeout(() => {
            if (statusEl.textContent === "Saved.") { statusEl.textContent = ""; statusEl.style.color = "var(--muted)"; }
          }, 1800);
        }
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

// Delete this row AND fingerprint its EXACT plaintext body so a wiki
// re-upload of the same text gets skipped on rescrape. The server reads
// the row's plaintext, normalizes (CRLF→LF + trailing-whitespace trim),
// SHA-256s, and stores the digest as a body_hash ban. A re-upload of a
// DIFFERENT version (even one character off) hashes differently and
// comes through — the curator can then accept or re-ban that version.
// Bans are reversible from the Lyrics toolbar's Bans button.
async function _baDeleteAndBanLyric(id, pageTitle) {
  const n = Number(id);
  if (!Number.isFinite(n) || n <= 0) return;
  const label = (pageTitle || "").trim() || `lyric #${n}`;
  if (!confirm(`Delete ${label} AND block this exact body text from future rescrapes? A re-upload of a different version of the same song still comes through.`)) return;
  try {
    // Add the body-hash ban first so even if Delete fails the text is
    // still protected. Server computes the hash from the row's
    // plaintext, so we just send the lyricId.
    const br = await apiFetch("/api/admin/lyrics/bans", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ kind: "body_hash", lyricId: n, reason: "deleted via lyric popup" }),
    });
    if (!br.ok) {
      const body = await br.json().catch(() => ({}));
      // A row with empty plaintext can't be fingerprinted — fall back
      // to a plain delete so the curator isn't stranded.
      if (br.status === 400 && /no plaintext/i.test(body?.error || "")) {
        if (!confirm(`This row has no plaintext to fingerprint — delete without ban?`)) return;
      } else {
        alert(`Ban failed: ${body?.error || `HTTP ${br.status}`} — row was NOT deleted.`);
        return;
      }
    }
    const dr = await apiFetch(`/api/admin/lyrics/${n}`, { method: "DELETE" });
    if (!dr.ok) {
      const body = await dr.json().catch(() => ({}));
      alert(`Delete failed (ban added anyway): ${body?.error || `HTTP ${dr.status}`}`);
      return;
    }
    document.getElementById("ba-lyric-edit-overlay")?.remove();
    document.getElementById("ba-lyric-overlay")?.remove();
    _baLyricsRowsCache = _baLyricsRowsCache.filter(x => Number(x.id) !== n);
    if (_baDetailArtist && Array.isArray(_baDetailArtist.lyrics)) {
      _baDetailArtist.lyrics = _baDetailArtist.lyrics.filter(x => Number(x.id) !== n);
    }
    document.querySelectorAll(`tr[data-lyric-row="${n}"]`).forEach(tr => tr.remove());
    if (typeof showToast === "function") showToast(`Deleted + body-hash blocked: ${label}`, "ok");
  } catch (e) {
    alert(`Failed: ${e?.message || e}`);
  }
}
window._baDeleteAndBanLyric = _baDeleteAndBanLyric;

// ── Merge picker ─────────────────────────────────────────────────────
// Two-step flow: type to filter the artist list, click the target,
// then confirm. Server transaction reassigns lyrics + appends releases
// + deletes the source row.

let _baMergePickerTimer = null;



// Admin button — import distinct lyrics-artist names that aren't yet
// in blues_artists. Idempotent (server checks LOWER(name) uniqueness).

// Admin button — remove blues_artists rows that were created by the
// lyric-import job AND have no other data (no Discogs ID, no Wikidata
// QID, no bio, etc.). Safety net for when the artist extractor pulled
// trash before the validator was tightened. Manually-edited rows are
// preserved server-side.

// Admin button — cleanup for the (removed) no-year pad button.
// Deletes every blues_artists row added in the last 24 hours.
// User confirmed they haven't manually added anyone in over a week,
// so this is a precise pad-insert signal.

// Admin button — re-pad with strict-Blues artists whose earliest
// master is in or before 1950. Inserts are tagged
// enrichment_status.source = "strict_pad_pre1950" for precise future
// cleanup. Existing rows are untouched beyond seed_strict_count.

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
let _baLyricsTuningLike = "";       // ILIKE substring filter on the tuning column
let _baLyricsTuningLikeTimer = null; // debounce timer for the tuning-text input
const _baLyricsSelectedIds = new Set(); // bulk-editor selection (lyric ids)
let _baLyricsSelectedCapped = false;    // true if "Select all matching" hit the 10k cap
let _baLyricsUnmatched = false;
let _baLyricsUnpinned = false;
let _baLyricsEmpty = false;
let _baLyricsNoArtist = false;
let _baLyricsPinned = false;     // only rows with a release/master pin
let _baLyricsFavorites = false;  // only rows the user has favorited
let _baLyricsTitlePunct = false; // only rows whose title has '-' or '('
let _baLyricsNoYear = false;     // only rows with no first_release_year
let _baLyricsRowsCache = [];
const _baLyricsListSort = { key: "page_title", dir: "asc" };
const _BA_LYRICS_LIST_TYPES = { page_title: "str", artist: "str", tuning: "str", snippet: "str", first_release_year: "num" };
let _baLyricsTuningsLoaded = false;

function _baSwitchSubtab(tab) {
  _baSubtab = (tab === "lyrics" || tab === "tunings" || tab === "export") ? tab : "lyrics";
  // Toggle button active state. Both classes are kept in sync so any
  // lingering CSS that targets the legacy `is-active` still works; the
  // new loc-tab styling (after the header rearrangement) keys on
  // `active`, matching wiki/youtube/gutenberg/chronam.
  document.querySelectorAll("#blues-archive-subtabs .ba-subtab").forEach(b => {
    const on = b.dataset.baTab === _baSubtab;
    b.classList.toggle("is-active", on);
    b.classList.toggle("active", on);
  });
  const lp = document.getElementById("blues-archive-lyrics-panel");
  const tp = document.getElementById("blues-archive-tunings-panel");
  const ep = document.getElementById("blues-archive-export-panel");
  if (lp) lp.style.display = _baSubtab === "lyrics"  ? "" : "none";
  if (tp) tp.style.display = _baSubtab === "tunings" ? "" : "none";
  if (ep) ep.style.display = _baSubtab === "export"  ? "" : "none";
  if (_baSubtab === "lyrics") {
    if (!_baLyricsTuningsLoaded) _baLoadTunings();
    _baLoadLyrics();
    // If a scrape job is already running on the server (page reload
    // mid-run, another tab kicked it), attach the status poller so
    // the UI reflects live progress. lyricsStartPolling is defined
    // in blues-admin.js (loaded lazily for admins by initBluesArchiveView);
    // it does a first poll immediately and clears its own interval
    // when the server returns running=false, so calling it when no
    // scrape exists is cheap — one round-trip then idle.
    if (typeof window.lyricsStartPolling === "function") {
      try { window.lyricsStartPolling(); } catch {}
    }
  } else if (_baSubtab === "tunings") {
    if (!_baTuningsFacetsLoaded) _baLoadTuningsFacets();
    _baLoadTuningsGrid();
  } else if (_baSubtab === "export") {
    _baExportInit();
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

// Free-text substring filter on the `tuning` column. Debounced so a
// keystroke storm doesn't fan out into a request per character — same
// 280 ms cadence as the main search input.
function _baLyricsDebouncedTuningLike() {
  clearTimeout(_baLyricsTuningLikeTimer);
  _baLyricsTuningLikeTimer = setTimeout(() => {
    _baLyricsTuningLike = (document.getElementById("blues-archive-lyrics-tuning-like")?.value || "").trim();
    _baLyricsPage = 0;
    _baLoadLyrics();
  }, 280);
}
window._baLyricsDebouncedTuningLike = _baLyricsDebouncedTuningLike;

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

function _baLyricsApplyNoArtist() {
  _baLyricsNoArtist = !!document.getElementById("blues-archive-lyrics-no-artist")?.checked;
  _baLyricsPage = 0;
  _baLoadLyrics();
}
window._baLyricsApplyNoArtist = _baLyricsApplyNoArtist;

function _baLyricsApplyPinned() {
  _baLyricsPinned = !!document.getElementById("blues-archive-lyrics-pinned")?.checked;
  _baLyricsPage = 0;
  _baLoadLyrics();
}
window._baLyricsApplyPinned = _baLyricsApplyPinned;

function _baLyricsApplyFavorites() {
  _baLyricsFavorites = !!document.getElementById("blues-archive-lyrics-favorites")?.checked;
  _baLyricsPage = 0;
  _baLoadLyrics();
}
window._baLyricsApplyFavorites = _baLyricsApplyFavorites;

// "Title has - or (" — surfaces lyrics whose page_title contains a
// dash or opening parenthesis. Useful for finding parenthetical
// disambiguation ("Stagger Lee (1928 version)") or dash-variants that
// pile up across multiple takes of the same song.
function _baLyricsApplyTitlePunct() {
  _baLyricsTitlePunct = !!document.getElementById("blues-archive-lyrics-title-punct")?.checked;
  _baLyricsPage = 0;
  _baLoadLyrics();
}
window._baLyricsApplyTitlePunct = _baLyricsApplyTitlePunct;

// "No year" — surfaces lyrics with no resolved first_release_year. This
// is the resolver worklist: a lyric's text stays out of the public
// viewer until a year lands (public viewing gates on the PD cutoff).
function _baLyricsApplyNoYear() {
  _baLyricsNoYear = !!document.getElementById("blues-archive-lyrics-no-year")?.checked;
  _baLyricsPage = 0;
  _baLoadLyrics();
}
window._baLyricsApplyNoYear = _baLyricsApplyNoYear;

// Cache pass: set each year-less lyric's first_release_year by matching
// its title + artist against the big release_cache (everything the
// masters+ sweeps pulled in). Zero Discogs API calls — one DB pass.
// Run this before the per-artist "Resolve years" or the slow Discogs
// worker; with a heavily-swept early-blues cache it resolves the most.
async function _baResolveYearsCache() {
  const btn = document.getElementById("blues-archive-lyrics-resolve-years-cache-btn");
  const orig = btn?.textContent;
  if (btn) { btn.disabled = true; btn.textContent = "Resolving…"; }
  try {
    const r = await apiFetch("/api/admin/lyrics/resolve-years-cache", { method: "POST", timeoutMs: 120000 });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert("Resolve failed: " + (err?.error ?? r.status));
      return;
    }
    const { updated = 0, stillMissing = 0 } = await r.json();
    const msg = `Resolved ${updated} year${updated === 1 ? "" : "s"} from the release cache.\n\n` +
      (stillMissing
        ? `${stillMissing.toLocaleString()} lyric${stillMissing === 1 ? "" : "s"} still have no year — try "Resolve years" (per-artist) or "Resolve via Discogs" (live API) next, or enter one manually.`
        : `All lyrics now have a first_release_year.`);
    alert(msg);
    _baLoadLyrics();
  } catch (e) {
    alert("Resolve failed: " + (e?.message || e));
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = orig || "Resolve via cache"; }
  }
}
window._baResolveYearsCache = _baResolveYearsCache;

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
    const isFav = _baFavoriteIds.has(id);
    el.textContent = isFav ? "★" : "☆";
    el.title = isFav ? "Favorited — click to un-favorite" : "Click to favorite";
    // Inline-style swap so the row-stars (yellow / grey) and the
    // viewer overlay's bigger star both repaint without a re-render.
    // Overlay button keeps its own yellow when favorited (overlay
    // stamps colour: #ffd166 in markup); only the row-star anchors
    // need the grey-when-empty treatment.
    if (el.tagName === "A") el.style.color = isFav ? "#ffd166" : "var(--muted)";
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
      const isFav = _baFavoriteIds.has(id);
      el.textContent = isFav ? "★" : "☆";
      if (el.tagName === "A") el.style.color = isFav ? "#ffd166" : "var(--muted)";
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
  const tuningLike = document.getElementById("blues-archive-lyrics-tuning-like");
  const unmatched = document.getElementById("blues-archive-lyrics-unmatched");
  const unpinned  = document.getElementById("blues-archive-lyrics-unpinned");
  const empty     = document.getElementById("blues-archive-lyrics-empty");
  const noArtist  = document.getElementById("blues-archive-lyrics-no-artist");
  const noYear    = document.getElementById("blues-archive-lyrics-no-year");
  const pinned    = document.getElementById("blues-archive-lyrics-pinned");
  const favorites = document.getElementById("blues-archive-lyrics-favorites");
  if (search)    search.value = "";
  if (tuningSel) tuningSel.value = "";
  if (tuningLike) tuningLike.value = "";
  if (unmatched) unmatched.checked = false;
  if (unpinned)  unpinned.checked  = false;
  if (empty)     empty.checked     = false;
  if (noArtist)  noArtist.checked  = false;
  if (noYear)    noYear.checked    = false;
  if (pinned)    pinned.checked    = false;
  if (favorites) favorites.checked = false;
  _baLyricsTuning    = "";
  _baLyricsTuningLike = "";
  _baLyricsUnmatched = false;
  _baLyricsUnpinned  = false;
  _baLyricsEmpty     = false;
  _baLyricsNoArtist  = false;
  _baLyricsNoYear    = false;
  _baLyricsPinned    = false;
  _baLyricsFavorites = false;
  _baLyricsTitlePunct = false;
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
    // Stash the list of distinct tuning strings so the bulk-edit bar's
    // <datalist> can suggest them as canonical values to apply.
    window._baLyricsTuningsList = tunings.map(t => t.tuning).filter(Boolean);
    _baLyricsTuningsLoaded = true;
  } catch { /* non-fatal */ }
}

async function _baLoadLyrics() {
  const rowsEl  = document.getElementById("blues-archive-lyrics-rows");
  const countEl = document.getElementById("blues-archive-lyrics-count");
  if (!rowsEl) return;
  const q = (document.getElementById("blues-archive-lyrics-search")?.value || "").trim();
  const params = new URLSearchParams();
  if (q)                  params.set("q", q);
  if (_baLyricsTuning)    params.set("tuning", _baLyricsTuning);
  if (_baLyricsTuningLike) params.set("tuningLike", _baLyricsTuningLike);
  if (_baLyricsUnmatched) params.set("unmatched", "1");
  if (_baLyricsUnpinned)  params.set("unpinned",  "1");
  if (_baLyricsEmpty)     params.set("empty",     "1");
  if (_baLyricsNoArtist)  params.set("noArtist",  "1");
  if (_baLyricsPinned)    params.set("pinned",    "1");
  if (_baLyricsFavorites) params.set("favorites", "1");
  if (_baLyricsTitlePunct) params.set("titlePunct", "1");
  if (_baLyricsNoYear)    params.set("noYear",    "1");
  // Toggle the "Clear filters" button visibility based on whether
  // any filter is currently active.
  const clearBtn = document.getElementById("blues-archive-lyrics-clear");
  if (clearBtn) clearBtn.style.display = (q || _baLyricsTuning || _baLyricsTuningLike || _baLyricsUnmatched || _baLyricsUnpinned || _baLyricsEmpty || _baLyricsNoArtist || _baLyricsPinned || _baLyricsFavorites || _baLyricsTitlePunct || _baLyricsNoYear) ? "" : "none";
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
  // Site-wide policy: wipe stale pagination immediately on a fresh
  // search so old page chrome doesn't linger while the fetch runs.
  const _lyricsPagEl = document.getElementById("blues-archive-lyrics-pager");
  if (_lyricsPagEl && _baLyricsPage === 0) _lyricsPagEl.innerHTML = "";
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

// Tiny ★/☆ star rendered inline at the start of a lyric row's title
// cell. Clicking flips favorite status via the same handler used by
// the lyric-viewer overlay, and _baToggleLyricFavorite already syncs
// every element with data-ba-fav-id=<id> to the new state — so the
// row-star and the overlay-star stay in sync automatically.
function _baLyricFavStar(l) {
  if (!l || l.id == null) return "";
  const id = Number(l.id);
  const fav = _baFavoriteIds.has(id);
  const glyph = fav ? "★" : "☆";
  const tip = fav ? "Favorited — click to un-favorite" : "Click to favorite";
  // Filled = saturated yellow, empty = muted grey so the column reads
  // as "favorited / not" at a glance. The data-ba-fav-id hook lets
  // _baToggleLyricFavorite re-stamp the colour without re-rendering.
  const colour = fav ? "#ffd166" : "var(--muted)";
  return `<a href="#" data-ba-fav-id="${id}" onclick="event.preventDefault();event.stopPropagation();_baToggleLyricFavorite(${id})" title="${tip}" style="color:${colour};text-decoration:none;margin-right:0.25rem;font-size:0.95em">${glyph}</a>`;
}

// Small inline badge shown after the lyric title to indicate whether
// the row has a Discogs master and/or release pin attached. Each glyph
// is itself a link to the matching discogs.com page (target=_blank so
// the SPA stays put / music keeps playing). Hover title reveals the
// numeric id. Rendered as empty string when neither pin is set, so
// unpinned rows look identical to before.
function _baLyricPinBadge(l) {
  if (!l) return "";
  const mid = Number(l.discogs_master_id);
  const rid = Number(l.discogs_release_id);
  const hasMaster  = Number.isFinite(mid) && mid > 0;
  const hasRelease = Number.isFinite(rid) && rid > 0;
  if (!hasMaster && !hasRelease) return "";
  // Single 💿 glyph for either pin — prefer the master link when both
  // exist (master is the canonical group), otherwise fall back to the
  // release page. Hover title lists whichever ids are set so the user
  // can still see the numbers.
  const href = hasMaster
    ? `https://www.discogs.com/master/${mid}`
    : `https://www.discogs.com/release/${rid}`;
  const tipParts = [];
  if (hasMaster)  tipParts.push(`Discogs master ${mid}`);
  if (hasRelease) tipParts.push(`Discogs release ${rid}`);
  const tip = tipParts.join(" · ");
  return `<a href="${href}" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="${tip}" style="text-decoration:none;margin-right:0.3rem;font-size:0.78em">💿</a>`;
}

// Single-row HTML — extracted so saves can do surgical in-place
// replacement without rebuilding the whole table (and blowing the
// user's scroll position). Used by both the master Lyrics table and
// the artist popup's lyric sub-table.
function _baLyricRowHtml(l) {
  const titleHtml = (typeof entityLookupLinkHtml === "function" && l.page_title)
    ? entityLookupLinkHtml("track", l.page_title, { trackArtist: l.artist || "", openId: l.id, openType: "lyric", title: `Lookup options for "${l.page_title}"` })
    : escHtml(l.page_title || "");
  const artistHtml = l.artist && typeof entityLookupLinkHtml === "function"
    ? entityLookupLinkHtml("artist", l.artist, { title: `Lookup options for "${l.artist}"` })
    : escHtml(l.artist || "");
  const archiveAffordance = "";
  // Year of first release — resolved from release_cache / Discogs.
  // NULL until resolved (rendered as a faint em-dash so the column
  // doesn't visually wobble).
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
  const searchLink = `<a href="/${searchQs}" onclick="event.stopPropagation()" class="ba-lyric-search" title="Search SeaDisco — masters+, oldest first">🔍</a>`;
  const favStar = _baLyricFavStar(l);
  const pinBadge = _baLyricPinBadge(l);
  const visitedCls = _baVisitedLyrics.has(Number(l.id)) ? "ba-lyric-visited" : "";
  const selectedAttr = _baLyricsSelectedIds.has(Number(l.id)) ? " checked" : "";
  return `<tr data-lyric-row="${l.id}" class="${visitedCls}">
    <td style="text-align:center"><input type="checkbox" class="ba-lyric-cb" data-lyric-cb="${l.id}"${selectedAttr} onclick="event.stopPropagation();_baLyricsToggleRow(${l.id}, this.checked)"></td>
    <td style="font-weight:600;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(fullTitle)}">${favStar}${searchLink} ${pinBadge}${titleHtml}</td>
    <td style="color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escHtml(fullArtist)}">${artistHtml}${archiveAffordance}</td>
    <td style="text-align:right;font-size:0.82rem;padding-right:0.6rem;white-space:nowrap">${yrHtml}</td>
    <td style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--accent);cursor:pointer" onclick="_baOpenLyric(${l.id})" title="${escHtml(l.tuning || "")}">${escHtml(l.tuning || "")}</td>
    <td class="ba-lyric-snippet" style="font-size:0.7rem;cursor:pointer;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" onclick="_baOpenLyric(${l.id})" title="${escHtml(fullSnip.slice(0, 400))}">${escHtml(fullSnip.slice(0, 140))}…</td>
    <td style="text-align:right"><a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenLyricEditor(${l.id})" style="color:var(--muted);text-decoration:none;font-size:0.78rem" title="Edit title / artist / tuning on this lyric">✎</a></td>
  </tr>`;
}

// Popup-variant row HTML — the artist detail overlay's lyrics table
// has 5 cells (Title, Year, Tuning, Snippet, ✎) instead of the master
// list's 6 (master adds an Artist column). Without this we'd splat
// the 6-cell HTML into a 5-cell layout and every column would shift
// left, dropping the tuning into the snippet slot, etc.
function _baLyricRowHtmlPopup(l, artistName) {
  const titleHtml = (typeof entityLookupLinkHtml === "function" && l.page_title)
    ? entityLookupLinkHtml("track", l.page_title, { trackArtist: artistName || l.artist || "", openId: l.id, openType: "lyric", title: `Lookup options for "${l.page_title}"` })
    : escHtml(l.page_title || "");
  const yr = Number.isFinite(Number(l.first_release_year)) ? Number(l.first_release_year) : null;
  const yrHtml = yr
    ? `<span style="font-variant-numeric:tabular-nums" title="${escHtml(l.first_release_source ? "via " + l.first_release_source : "")}">${yr}</span>`
    : `<span style="color:#555">—</span>`;
  const searchQs = `?q=${encodeURIComponent(l.page_title || "")}` +
                   `&a=${encodeURIComponent(artistName || l.artist || "")}` +
                   `&r=${encodeURIComponent("master+")}` +
                   `&s=${encodeURIComponent("year:asc")}`;
  const searchLink = `<a href="/${searchQs}" onclick="event.stopPropagation()" class="ba-lyric-search" title="Search SeaDisco — masters+, oldest first">🔍</a>`;
  const favStar = _baLyricFavStar(l);
  const pinBadge = _baLyricPinBadge(l);
  const visitedCls = _baVisitedLyrics.has(Number(l.id)) ? "ba-lyric-visited" : "";
  return `<tr data-lyric-row="${l.id}" class="${visitedCls}">
    <td style="font-weight:600;color:var(--text);white-space:nowrap">${favStar}${searchLink} ${pinBadge}${titleHtml}</td>
    <td style="text-align:right;font-size:0.82rem;padding-right:0.6rem;white-space:nowrap">${yrHtml}</td>
    <td style="white-space:nowrap;color:var(--accent);cursor:pointer" onclick="_baOpenLyric(${l.id})">${escHtml(l.tuning || "")}</td>
    <td class="ba-lyric-snippet" style="font-size:0.76rem;cursor:pointer" onclick="_baOpenLyric(${l.id})">${escHtml((l.snippet || "").replace(/\s+/g, " ").slice(0, 140))}…</td>
    <td style="text-align:right"><a href="#" onclick="event.preventDefault();event.stopPropagation();_baOpenLyricEditor(${l.id})" style="color:var(--muted);text-decoration:none;font-size:0.78rem" title="Edit tuning / artist on this lyric">✎</a></td>
  </tr>`;
}

// Patch a single rendered row in place after a save. Walks every <tr>
// with data-lyric-row=id (covers both the master list and the artist
// popup sub-table) and replaces its outerHTML with the variant that
// matches whichever container the row lives in — the artist popup
// (#ba-artist-overlay) uses a 5-cell layout, the master list a 6-cell.
// Using the wrong builder shifted every column over by one, e.g. the
// tuning landed in the snippet slot. Scroll position is preserved
// because we only swap the one row.
function _baPatchLyricRowEverywhere(updated) {
  if (!updated || updated.id == null) return;
  document.querySelectorAll(`tr[data-lyric-row="${updated.id}"]`).forEach(tr => {
    const inPopup = !!tr.closest("#ba-artist-overlay");
    tr.outerHTML = inPopup
      ? _baLyricRowHtmlPopup(updated, _baDetailArtist?.name || updated.artist || "")
      : _baLyricRowHtml(updated);
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
  // Select-all-on-page state: header checkbox is "checked" only when
  // every visible row is in the selection set, "indeterminate" when
  // some-but-not-all are, and clear otherwise.
  const pageIds = rows.map(r => Number(r.id));
  const pageSelectedCount = pageIds.filter(id => _baLyricsSelectedIds.has(id)).length;
  const allOnPageSelected = pageIds.length > 0 && pageSelectedCount === pageIds.length;
  rowsEl.innerHTML = `
    <table class="api-log-table" style="font-size:0.84rem;width:100%;table-layout:fixed">
      <colgroup>
        <col style="width:30px">
        <col style="width:26%">
        <col style="width:18%">
        <col style="width:60px">
        <col style="width:18%">
        <col>
        <col style="width:32px">
      </colgroup>
      <thead><tr>
        <th style="text-align:center" title="Select all on this page"><input type="checkbox" id="ba-lyrics-cb-all" ${allOnPageSelected ? "checked" : ""} onclick="_baLyricsToggleAllOnPage(this.checked)"></th>
        ${_baSortTh("Title",   "page_title",         S, "_baSortLyricsList")}
        ${_baSortTh("Artist",  "artist",             S, "_baSortLyricsList")}
        ${_baSortTh("Year",    "first_release_year", S, "_baSortLyricsList", "text-align:right;padding-right:0.6rem")}
        ${_baSortTh("Tuning",  "tuning",             S, "_baSortLyricsList")}
        ${_baSortTh("Snippet", "snippet",    S, "_baSortLyricsList")}
        <th style="width:1%"></th>
      </tr></thead>
      <tbody>${rows.map(_baLyricRowHtml).join("")}</tbody>
    </table>`;
  // Header checkbox indeterminate state is a DOM property — can't be
  // set via attribute, so apply it after innerHTML lands.
  const hdrCb = document.getElementById("ba-lyrics-cb-all");
  if (hdrCb) hdrCb.indeterminate = pageSelectedCount > 0 && !allOnPageSelected;
  _baLyricsRenderBulkBar();
}

// ── Bulk tuning editor ────────────────────────────────────────────
// All the wiring for the checkbox-driven bulk-edit bar lives here so
// the table renderer stays focused on row layout. Selection is a Set
// of lyric ids that survives pagination — the rows just rebind to it
// every render via the data-lyric-cb attribute.
function _baLyricsToggleRow(id, checked) {
  const n = Number(id);
  if (!Number.isFinite(n)) return;
  if (checked) _baLyricsSelectedIds.add(n);
  else         _baLyricsSelectedIds.delete(n);
  _baLyricsSelectedCapped = false; // any manual edit invalidates the cap flag
  _baLyricsRenderBulkBar();
  _baLyricsRefreshHeaderCheckbox();
}
window._baLyricsToggleRow = _baLyricsToggleRow;

function _baLyricsToggleAllOnPage(checked) {
  const rows = _baLyricsRowsCache || [];
  for (const r of rows) {
    const n = Number(r.id);
    if (!Number.isFinite(n)) continue;
    if (checked) _baLyricsSelectedIds.add(n);
    else         _baLyricsSelectedIds.delete(n);
  }
  _baLyricsSelectedCapped = false;
  // Re-mark the visible per-row checkboxes without rebuilding the
  // whole table (preserves scroll position).
  document.querySelectorAll(".ba-lyric-cb").forEach(cb => {
    const id = Number(cb.getAttribute("data-lyric-cb"));
    cb.checked = _baLyricsSelectedIds.has(id);
  });
  _baLyricsRenderBulkBar();
  _baLyricsRefreshHeaderCheckbox();
}
window._baLyricsToggleAllOnPage = _baLyricsToggleAllOnPage;

function _baLyricsRefreshHeaderCheckbox() {
  const hdrCb = document.getElementById("ba-lyrics-cb-all");
  if (!hdrCb) return;
  const rows = _baLyricsRowsCache || [];
  const pageIds = rows.map(r => Number(r.id));
  const pageSelectedCount = pageIds.filter(id => _baLyricsSelectedIds.has(id)).length;
  const allOnPage = pageIds.length > 0 && pageSelectedCount === pageIds.length;
  hdrCb.checked = allOnPage;
  hdrCb.indeterminate = pageSelectedCount > 0 && !allOnPage;
}

// "Select all matching the current filter" — fans out to the
// /api/admin/lyrics/matching-ids endpoint, which returns every id
// that hits the same WHERE clause as the listing endpoint (capped at
// 10k). Used for mass cleanup over a tuning-text filter result.
async function _baLyricsSelectAllMatching() {
  const params = new URLSearchParams();
  const q = (document.getElementById("blues-archive-lyrics-search")?.value || "").trim();
  if (q)                   params.set("q", q);
  if (_baLyricsTuning)     params.set("tuning", _baLyricsTuning);
  if (_baLyricsTuningLike) params.set("tuningLike", _baLyricsTuningLike);
  if (_baLyricsUnmatched)  params.set("unmatched", "1");
  if (_baLyricsUnpinned)   params.set("unpinned",  "1");
  if (_baLyricsEmpty)      params.set("empty",     "1");
  if (_baLyricsNoArtist)   params.set("noArtist",  "1");
  if (_baLyricsPinned)     params.set("pinned",    "1");
  if (_baLyricsFavorites)  params.set("favorites", "1");
  if (_baLyricsTitlePunct) params.set("titlePunct", "1");
  try {
    const r = await apiFetch(`/api/admin/lyrics/matching-ids?${params}`);
    if (!r.ok) { alert(`Select-all failed: HTTP ${r.status}`); return; }
    const { ids = [], capped = false } = await r.json();
    for (const id of ids) {
      const n = Number(id);
      if (Number.isFinite(n)) _baLyricsSelectedIds.add(n);
    }
    _baLyricsSelectedCapped = !!capped;
    // Re-check visible boxes + re-render bulk bar.
    document.querySelectorAll(".ba-lyric-cb").forEach(cb => {
      const id = Number(cb.getAttribute("data-lyric-cb"));
      cb.checked = _baLyricsSelectedIds.has(id);
    });
    _baLyricsRefreshHeaderCheckbox();
    _baLyricsRenderBulkBar();
  } catch (e) {
    alert(`Select-all failed: ${String(e?.message || e)}`);
  }
}
window._baLyricsSelectAllMatching = _baLyricsSelectAllMatching;

function _baLyricsClearSelection() {
  _baLyricsSelectedIds.clear();
  _baLyricsSelectedCapped = false;
  document.querySelectorAll(".ba-lyric-cb").forEach(cb => { cb.checked = false; });
  _baLyricsRefreshHeaderCheckbox();
  _baLyricsRenderBulkBar();
}
window._baLyricsClearSelection = _baLyricsClearSelection;

function _baLyricsRenderBulkBar() {
  const el = document.getElementById("blues-archive-lyrics-bulkbar");
  if (!el) return;
  const n = _baLyricsSelectedIds.size;
  if (!n) { el.style.display = "none"; el.innerHTML = ""; return; }
  el.style.display = "";
  const capNote = _baLyricsSelectedCapped
    ? ` <span style="color:#e88" title="Server capped the matching-ids response at 10k; some matches above that limit aren't in the selection.">(capped at 10k)</span>`
    : "";
  // Datalist of existing tunings so the curator can pick canonical
  // values without retyping them. The dropdown <select> next to the
  // tuning-text input already holds the full list with counts.
  const tuningOpts = (window._baLyricsTuningsList || []).map(t => `<option value="${escHtml(t)}">`).join("");
  el.innerHTML = `
    <div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap">
      <strong>${n.toLocaleString()} selected</strong>${capNote}
      <span style="color:var(--muted)">·</span>
      <a href="#" onclick="event.preventDefault();_baLyricsSelectAllMatching()" style="color:var(--accent);text-decoration:none">Select all matching</a>
      <span style="color:var(--muted)">·</span>
      <a href="#" onclick="event.preventDefault();_baLyricsClearSelection()" style="color:var(--muted);text-decoration:none">Clear selection</a>
      <span style="margin-left:auto;display:flex;gap:0.4rem;align-items:center;flex-wrap:wrap">
        <label style="font-weight:600">Set tuning →</label>
        <input id="ba-lyrics-bulk-tuning" type="text" list="ba-lyrics-bulk-tuning-list" placeholder="e.g. Open D" style="padding:0.35rem 0.5rem;font-size:0.82rem;min-width:160px" onkeydown="if(event.key==='Enter'){event.preventDefault();_baLyricsBulkSetTuningFromInput()}">
        <datalist id="ba-lyrics-bulk-tuning-list">${tuningOpts}</datalist>
        <button type="button" class="archive-btn" onclick="_baLyricsBulkSetTuningFromInput()" title="Apply the text above to every selected row.">Apply</button>
        <button type="button" class="archive-btn" onclick="_baLyricsBulkSetTuning(null)" title="Clear the tuning column on every selected row.">Clear tuning</button>
        <button type="button" class="archive-btn" onclick="_baLyricsBulkDelete()" title="Hard-delete every selected lyric row. Cannot be undone." style="color:#e88;border-color:rgba(232,136,136,0.5)">⚠ Delete selected</button>
      </span>
    </div>
  `;
}
window._baLyricsRenderBulkBar = _baLyricsRenderBulkBar;

function _baLyricsBulkSetTuningFromInput() {
  const inp = document.getElementById("ba-lyrics-bulk-tuning");
  const val = (inp?.value || "").trim();
  if (!val) { alert("Enter a tuning value (or click Clear tuning to wipe)."); return; }
  _baLyricsBulkSetTuning(val);
}
window._baLyricsBulkSetTuningFromInput = _baLyricsBulkSetTuningFromInput;

async function _baLyricsBulkSetTuning(value) {
  const ids = Array.from(_baLyricsSelectedIds);
  if (!ids.length) return;
  const display = value == null ? "(blank — clear tuning)" : `"${value}"`;
  if (!confirm(`Set tuning on ${ids.length.toLocaleString()} lyric${ids.length === 1 ? "" : "s"} to ${display}?`)) return;
  try {
    const r = await apiFetch("/api/admin/lyrics/bulk-update-tuning", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ids, tuning: value }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Bulk update failed: ${j?.error || r.status}`); return; }
    if (typeof showToast === "function") {
      showToast(`Updated ${(j.updated ?? 0).toLocaleString()} lyric${j.updated === 1 ? "" : "s"}`, "info");
    }
    _baLyricsClearSelection();
    // Reload the list so the table reflects the new values + the
    // tuning dropdown picks up any new value the curator just coined.
    _baLyricsTuningsLoaded = false;
    if (typeof _baLoadTunings === "function") _baLoadTunings();
    _baLoadLyrics();
  } catch (e) {
    alert(`Bulk update failed: ${String(e?.message || e)}`);
  }
}
window._baLyricsBulkSetTuning = _baLyricsBulkSetTuning;

async function _baLyricsBulkDelete() {
  const ids = Array.from(_baLyricsSelectedIds);
  if (!ids.length) return;
  const n = ids.length;
  if (!confirm(`Hard-delete ${n.toLocaleString()} lyric${n === 1 ? "" : "s"}? This cannot be undone.`)) return;
  // Big-batch second confirm — same pattern as the release-cache
  // delete in admin. Stops a typo from wiping thousands of rows.
  if (n > 50) {
    const typed = prompt(`Type "delete ${n}" to confirm:`);
    if (typed !== `delete ${n}`) { alert("Confirmation didn't match — cancelled."); return; }
  }
  try {
    const r = await apiFetch("/api/admin/lyrics/bulk-delete", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ids }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Bulk delete failed: ${j?.error || r.status}`); return; }
    if (typeof showToast === "function") {
      showToast(`Deleted ${(j.deleted ?? 0).toLocaleString()} lyric${j.deleted === 1 ? "" : "s"}`, "info");
    }
    _baLyricsClearSelection();
    // Reload list + stats; the bulk delete may have wiped whole
    // artists' worth of lyrics so the stats strip drifts otherwise.
    if (typeof _baLoadStats === "function") _baLoadStats();
    _baLoadLyrics();
  } catch (e) {
    alert(`Bulk delete failed: ${String(e?.message || e)}`);
  }
}
window._baLyricsBulkDelete = _baLyricsBulkDelete;

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



// Per-row HTML. Title links to discogs.com/release|/master/<id> in a
// new tab. Artist links to the in-app artist popup (so the curator
// can pivot to the artist's full record). Type+role rendered as
// muted text. Cells are click-through-friendly: stopPropagation on
// the action links so any future row-click handler doesn't pre-empt.







// ── Tunings grid ─────────────────────────────────────────────────────
// Read-only view over blues_tunings_grid, seeded server-side from
// src/data/tunings.csv. Filters: free-text + per-facet dropdowns
// (artist / position / pitch). Sort is server-side; pagination is
// the same 100-row pattern as the Lyrics / Releases tabs.
let _baTuningsFacetsLoaded = false;
let _baTuningsPage = 0;
const _BA_TUNINGS_LIMIT = 100;
let _baTuningsTotal = 0;
let _baTuningsSearchTimer = null;
const _baTuningsSort = { key: "artist", dir: "asc" };
let _baTuningsRowsCache = [];                  // current page rows; rebound by _baLoadTuningsGrid
const _baTuningsSelectedIds = new Set();       // bulk-editor selection
let _baTuningsSelectedCapped = false;          // true if "Select all matching" hit the 10k cap

async function _baLoadTuningsFacets() {
  try {
    const r = await apiFetch("/api/blues-archive/tunings/facets");
    if (!r.ok) return;
    const { artists = [], positions = [] } = await r.json();
    const fill = (id, label, vals) => {
      const sel = document.getElementById(id);
      if (!sel) return;
      sel.innerHTML = `<option value="">${label}</option>` +
        vals.map(v => `<option value="${escHtml(v)}">${escHtml(v)}</option>`).join("");
    };
    fill("ba-tunings-artist",   "All artists",   artists);
    fill("ba-tunings-position", "All positions", positions);
    _baTuningsFacetsLoaded = true;
  } catch { /* non-fatal */ }
}

async function _baLoadTuningsGrid() {
  const rowsEl  = document.getElementById("ba-tunings-rows");
  const countEl = document.getElementById("ba-tunings-count");
  if (!rowsEl) return;
  const _tunPagEl = document.getElementById("ba-tunings-pager");
  if (_tunPagEl && _baTuningsPage === 0) _tunPagEl.innerHTML = "";
  const q        = (document.getElementById("ba-tunings-search")?.value   || "").trim();
  const artist   = (document.getElementById("ba-tunings-artist")?.value   || "").trim();
  const position = (document.getElementById("ba-tunings-position")?.value || "").trim();
  const params = new URLSearchParams();
  if (q)        params.set("q",        q);
  if (artist)   params.set("artist",   artist);
  if (position) params.set("position", position);
  params.set("sort",   _baTuningsSort.key);
  params.set("order",  _baTuningsSort.dir);
  params.set("limit",  String(_BA_TUNINGS_LIMIT));
  params.set("offset", String(_baTuningsPage * _BA_TUNINGS_LIMIT));
  const clearBtn = document.getElementById("ba-tunings-clear");
  if (clearBtn) clearBtn.style.display = (q || artist || position) ? "" : "none";
  rowsEl.classList.add("ba-loading");
  try {
    const r = await apiFetch(`/api/blues-archive/tunings?${params}`);
    if (!r.ok) { rowsEl.innerHTML = `<p style="color:#e88">Failed: HTTP ${r.status}</p>`; return; }
    const { rows = [], total = 0 } = await r.json();
    _baTuningsTotal = total;
    _baTuningsRowsCache = rows;
    if (countEl) countEl.textContent = total ? `${total.toLocaleString()} row${total === 1 ? "" : "s"}` : "No matches.";
    if (!rows.length) {
      rowsEl.innerHTML = `<p style="color:var(--muted);padding:0.5rem 0">No matches.</p>`;
    } else {
      const S = _baTuningsSort;
      // The search-link shortcut on each title mirrors the one on the
      // Lyrics table: opens SeaDisco search prefilled with title + artist.
      // Notes-as-title fallback: collaborative-session rows in the CSV
      // leave Title blank and stash the real track name in Notes as
      // "With <collaborator>:: <Track Title>". Surface the parsed
      // track title in the Title column and the collaborator name as
      // a small grey prefix on the Notes column so the grid reads
      // cleanly without losing either piece of info.
      const rowHtml = (r) => {
        const rawTitle = String(r.title || "").trim();
        const rawNotes = String(r.notes || "").trim();
        let displayTitle = rawTitle;
        let displayNotes = rawNotes;
        let collaborator = "";
        if (!rawTitle && rawNotes) {
          const m = rawNotes.match(/^(.*?)::\s*(.+)$/);
          if (m) {
            collaborator = m[1].trim();           // "With Gus Cannon (Banjo Joe)"
            displayTitle = m[2].trim();           // "My Money Never Runs Out"
            displayNotes = collaborator;
          } else {
            // No "::" — show notes in the title slot as a last resort
            // so the row isn't a row of blanks.
            displayTitle = rawNotes;
            displayNotes = "";
          }
        }
        const artist = String(r.artist || "");
        const searchQs = `?q=${encodeURIComponent(displayTitle)}` +
                         `&a=${encodeURIComponent(artist)}` +
                         `&r=${encodeURIComponent("master+")}` +
                         `&s=${encodeURIComponent("year:asc")}`;
        const searchLink = displayTitle
          ? `<a href="/${searchQs}" class="ba-lyric-search" title="Search SeaDisco — masters+, oldest first">🔍</a> `
          : "";
        const selectedAttr = _baTuningsSelectedIds.has(Number(r.id)) ? " checked" : "";
        return `<tr>
          <td style="text-align:center"><input type="checkbox" class="ba-tuning-cb" data-tuning-cb="${r.id}"${selectedAttr} onclick="event.stopPropagation();_baTuningsToggleRow(${r.id}, this.checked)"></td>
          <td style="font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${escHtml(artist)}">${escHtml(artist)}</td>
          <td style="color:var(--text);overflow:hidden;text-overflow:ellipsis" title="${escHtml(displayTitle)}">${searchLink}${escHtml(displayTitle)}</td>
          <td style="color:var(--accent);white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${escHtml(String(r.position || ""))}">${escHtml(String(r.position || ""))}</td>
          <td style="color:var(--muted);font-size:0.78rem;overflow:hidden" title="${escHtml(displayNotes)}">${escHtml(displayNotes)}</td>
          <td style="text-align:right;white-space:nowrap"><a href="#" onclick="event.preventDefault();_baDeleteTuning(${r.id}, ${JSON.stringify(displayTitle).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;")})" title="Delete this tuning row" style="color:#e88;text-decoration:none;font-weight:600;padding:0.1rem 0.4rem">×</a></td>
        </tr>`;
      };
      const pageIds = rows.map(rr => Number(rr.id));
      const pageSelectedCount = pageIds.filter(id => _baTuningsSelectedIds.has(id)).length;
      const allOnPageSelected = pageIds.length > 0 && pageSelectedCount === pageIds.length;
      rowsEl.innerHTML = `
        <table class="api-log-table" style="font-size:0.84rem;width:100%;table-layout:fixed">
          <colgroup>
            <col style="width:30px">
            <col style="width:18%">
            <col style="width:36%">
            <col style="width:120px">
            <col>
            <col style="width:32px">
          </colgroup>
          <thead><tr>
            <th style="text-align:center" title="Select all on this page"><input type="checkbox" id="ba-tunings-cb-all" ${allOnPageSelected ? "checked" : ""} onclick="_baTuningsToggleAllOnPage(this.checked)"></th>
            ${_baSortTh("Artist",   "artist",   S, "_baSortTunings")}
            ${_baSortTh("Title",    "title",    S, "_baSortTunings")}
            ${_baSortTh("Position", "position", S, "_baSortTunings")}
            <th>Notes</th>
            <th></th>
          </tr></thead>
          <tbody>${rows.map(rowHtml).join("")}</tbody>
        </table>`;
      const hdrCb = document.getElementById("ba-tunings-cb-all");
      if (hdrCb) hdrCb.indeterminate = pageSelectedCount > 0 && !allOnPageSelected;
    }
    _baTuningsRenderBulkBar();
    _baRenderTuningsPager();
  } catch (e) {
    rowsEl.innerHTML = `<p style="color:#e88">Failed: ${escHtml(String(e?.message || e))}</p>`;
  } finally {
    rowsEl.classList.remove("ba-loading");
  }
}
window._baLoadTuningsGrid = _baLoadTuningsGrid;

// ── Tunings bulk editor ──────────────────────────────────────────
// Mirror of the lyrics bulk editor: checkbox column + bulk action bar
// for mass-setting position and mass-deleting rows. Selection is a Set
// of tuning ids that survives pagination — rows just rebind to it
// every render via the data-tuning-cb attribute.
function _baTuningsToggleRow(id, checked) {
  const n = Number(id);
  if (!Number.isFinite(n)) return;
  if (checked) _baTuningsSelectedIds.add(n);
  else         _baTuningsSelectedIds.delete(n);
  _baTuningsSelectedCapped = false;
  _baTuningsRenderBulkBar();
  _baTuningsRefreshHeaderCheckbox();
}
window._baTuningsToggleRow = _baTuningsToggleRow;

function _baTuningsToggleAllOnPage(checked) {
  for (const r of (_baTuningsRowsCache || [])) {
    const n = Number(r.id);
    if (!Number.isFinite(n)) continue;
    if (checked) _baTuningsSelectedIds.add(n);
    else         _baTuningsSelectedIds.delete(n);
  }
  _baTuningsSelectedCapped = false;
  document.querySelectorAll(".ba-tuning-cb").forEach(cb => {
    const id = Number(cb.getAttribute("data-tuning-cb"));
    cb.checked = _baTuningsSelectedIds.has(id);
  });
  _baTuningsRenderBulkBar();
  _baTuningsRefreshHeaderCheckbox();
}
window._baTuningsToggleAllOnPage = _baTuningsToggleAllOnPage;

function _baTuningsRefreshHeaderCheckbox() {
  const hdrCb = document.getElementById("ba-tunings-cb-all");
  if (!hdrCb) return;
  const pageIds = (_baTuningsRowsCache || []).map(r => Number(r.id));
  const pageSelectedCount = pageIds.filter(id => _baTuningsSelectedIds.has(id)).length;
  const allOnPage = pageIds.length > 0 && pageSelectedCount === pageIds.length;
  hdrCb.checked = allOnPage;
  hdrCb.indeterminate = pageSelectedCount > 0 && !allOnPage;
}

async function _baTuningsSelectAllMatching() {
  const params = new URLSearchParams();
  const q        = (document.getElementById("ba-tunings-search")?.value   || "").trim();
  const artist   = (document.getElementById("ba-tunings-artist")?.value   || "").trim();
  const position = (document.getElementById("ba-tunings-position")?.value || "").trim();
  if (q)        params.set("q",        q);
  if (artist)   params.set("artist",   artist);
  if (position) params.set("position", position);
  try {
    const r = await apiFetch(`/api/blues-archive/tunings/matching-ids?${params}`);
    if (!r.ok) { alert(`Select-all failed: HTTP ${r.status}`); return; }
    const { ids = [], capped = false } = await r.json();
    for (const id of ids) {
      const n = Number(id);
      if (Number.isFinite(n)) _baTuningsSelectedIds.add(n);
    }
    _baTuningsSelectedCapped = !!capped;
    document.querySelectorAll(".ba-tuning-cb").forEach(cb => {
      const id = Number(cb.getAttribute("data-tuning-cb"));
      cb.checked = _baTuningsSelectedIds.has(id);
    });
    _baTuningsRefreshHeaderCheckbox();
    _baTuningsRenderBulkBar();
  } catch (e) {
    alert(`Select-all failed: ${String(e?.message || e)}`);
  }
}
window._baTuningsSelectAllMatching = _baTuningsSelectAllMatching;

function _baTuningsClearSelection() {
  _baTuningsSelectedIds.clear();
  _baTuningsSelectedCapped = false;
  document.querySelectorAll(".ba-tuning-cb").forEach(cb => { cb.checked = false; });
  _baTuningsRefreshHeaderCheckbox();
  _baTuningsRenderBulkBar();
}
window._baTuningsClearSelection = _baTuningsClearSelection;

function _baTuningsRenderBulkBar() {
  const el = document.getElementById("ba-tunings-bulkbar");
  if (!el) return;
  const n = _baTuningsSelectedIds.size;
  if (!n) { el.style.display = "none"; el.innerHTML = ""; return; }
  el.style.display = "";
  const capNote = _baTuningsSelectedCapped
    ? ` <span style="color:#e88" title="Server capped the matching-ids response at 10k; some matches above that limit aren't in the selection.">(capped at 10k)</span>`
    : "";
  // Pull canonical position values from the facet dropdown — the
  // facets endpoint already supplies them and they're loaded into
  // #ba-tunings-position as <option>s.
  const positionOpts = Array.from(document.querySelectorAll("#ba-tunings-position option"))
    .map(o => o.value)
    .filter(Boolean)
    .map(v => `<option value="${escHtml(v)}">`)
    .join("");
  el.innerHTML = `
    <div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap">
      <strong>${n.toLocaleString()} selected</strong>${capNote}
      <span style="color:var(--muted)">·</span>
      <a href="#" onclick="event.preventDefault();_baTuningsSelectAllMatching()" style="color:var(--accent);text-decoration:none">Select all matching</a>
      <span style="color:var(--muted)">·</span>
      <a href="#" onclick="event.preventDefault();_baTuningsClearSelection()" style="color:var(--muted);text-decoration:none">Clear selection</a>
      <span style="margin-left:auto;display:flex;gap:0.4rem;align-items:center;flex-wrap:wrap">
        <label style="font-weight:600">Set position →</label>
        <input id="ba-tunings-bulk-position" type="text" list="ba-tunings-bulk-position-list" placeholder="e.g. Open G" style="padding:0.35rem 0.5rem;font-size:0.82rem;min-width:160px" onkeydown="if(event.key==='Enter'){event.preventDefault();_baTuningsBulkSetPositionFromInput()}">
        <datalist id="ba-tunings-bulk-position-list">${positionOpts}</datalist>
        <button type="button" class="archive-btn" onclick="_baTuningsBulkSetPositionFromInput()" title="Apply the text above to every selected row.">Apply</button>
        <button type="button" class="archive-btn" onclick="_baTuningsBulkSetPosition(null)" title="Clear the position column on every selected row.">Clear position</button>
        <button type="button" class="archive-btn" onclick="_baTuningsBulkDelete()" title="Hard-delete every selected tuning row. Cannot be undone." style="color:#e88;border-color:rgba(232,136,136,0.5)">⚠ Delete selected</button>
      </span>
    </div>
  `;
}
window._baTuningsRenderBulkBar = _baTuningsRenderBulkBar;

function _baTuningsBulkSetPositionFromInput() {
  const inp = document.getElementById("ba-tunings-bulk-position");
  const val = (inp?.value || "").trim();
  if (!val) { alert("Enter a position value (or click Clear position to wipe)."); return; }
  _baTuningsBulkSetPosition(val);
}
window._baTuningsBulkSetPositionFromInput = _baTuningsBulkSetPositionFromInput;

async function _baTuningsBulkSetPosition(value) {
  const ids = Array.from(_baTuningsSelectedIds);
  if (!ids.length) return;
  const display = value == null ? "(blank — clear position)" : `"${value}"`;
  if (!confirm(`Set position on ${ids.length.toLocaleString()} tuning row${ids.length === 1 ? "" : "s"} to ${display}?`)) return;
  try {
    const r = await apiFetch("/api/blues-archive/tunings/bulk-update-position", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ids, position: value }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Bulk update failed: ${j?.error || r.status}`); return; }
    if (typeof showToast === "function") {
      showToast(`Updated ${(j.updated ?? 0).toLocaleString()} row${j.updated === 1 ? "" : "s"}`, "info");
    }
    _baTuningsClearSelection();
    _baTuningsFacetsLoaded = false;
    if (typeof _baLoadTuningsFacets === "function") _baLoadTuningsFacets();
    _baLoadTuningsGrid();
  } catch (e) {
    alert(`Bulk update failed: ${String(e?.message || e)}`);
  }
}
window._baTuningsBulkSetPosition = _baTuningsBulkSetPosition;

async function _baTuningsBulkDelete() {
  const ids = Array.from(_baTuningsSelectedIds);
  if (!ids.length) return;
  const n = ids.length;
  if (!confirm(`Hard-delete ${n.toLocaleString()} tuning row${n === 1 ? "" : "s"}? This cannot be undone.`)) return;
  if (n > 50) {
    const typed = prompt(`Type "delete ${n}" to confirm:`);
    if (typed !== `delete ${n}`) { alert("Confirmation didn't match — cancelled."); return; }
  }
  try {
    const r = await apiFetch("/api/blues-archive/tunings/bulk-delete", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ids }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Bulk delete failed: ${j?.error || r.status}`); return; }
    if (typeof showToast === "function") {
      showToast(`Deleted ${(j.deleted ?? 0).toLocaleString()} tuning row${j.deleted === 1 ? "" : "s"}`, "info");
    }
    _baTuningsClearSelection();
    _baLoadTuningsGrid();
  } catch (e) {
    alert(`Bulk delete failed: ${String(e?.message || e)}`);
  }
}
window._baTuningsBulkDelete = _baTuningsBulkDelete;

// Tuning CRUD: add + delete (admin only — gated server-side too).
async function _baDeleteTuning(id, title) {
  if (!Number.isFinite(id) || id <= 0) return;
  if (!confirm(`Delete tuning row${title ? ` for "${title}"` : ""}? Cannot be undone.`)) return;
  try {
    const r = await apiFetch(`/api/blues-archive/tunings/${id}`, { method: "DELETE" });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      alert(`Delete failed: ${err.error || `HTTP ${r.status}`}`);
      return;
    }
    _baLoadTuningsGrid();
  } catch (e) {
    alert(`Delete failed: ${e?.message || e}`);
  }
}
window._baDeleteTuning = _baDeleteTuning;

function _baOpenTuningAdd() {
  let overlay = document.getElementById("ba-tuning-add-overlay");
  if (overlay) overlay.remove();
  overlay = document.createElement("div");
  overlay.id = "ba-tuning-add-overlay";
  Object.assign(overlay.style, {
    position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
    zIndex: "360", display: "flex", alignItems: "flex-start",
    justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
  });
  overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(560px,100%)">
      <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.8rem">
        <h3 style="margin:0">Add tuning</h3>
        <button type="button" class="archive-btn" onclick="document.getElementById('ba-tuning-add-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <form id="ba-tuning-add-form" onsubmit="event.preventDefault();_baSubmitTuningAdd()" style="display:grid;grid-template-columns:1fr 1fr;gap:0.6rem 1rem;font-size:0.86rem">
        <label style="grid-column:1/-1">Artist <input name="artist" required style="width:100%" placeholder="e.g. Charley Patton"></label>
        <label style="grid-column:1/-1">Title <input name="title" required style="width:100%" placeholder="e.g. Pony Blues"></label>
        <label style="grid-column:1/-1">Position <input name="position" placeholder="e.g. Open G"></label>
        <label style="grid-column:1/-1">Notes <textarea name="notes" rows="3" style="width:100%" placeholder="Optional"></textarea></label>
        <div style="grid-column:1/-1;display:flex;justify-content:flex-end;gap:0.5rem;margin-top:0.5rem">
          <button type="button" class="archive-btn" onclick="document.getElementById('ba-tuning-add-overlay')?.remove()">Cancel</button>
          <button type="submit" class="archive-btn archive-btn-suggest">Save</button>
        </div>
        <div id="ba-tuning-add-status" style="grid-column:1/-1;color:var(--muted);font-size:0.78rem;min-height:1em"></div>
      </form>
    </div>`;
  document.body.appendChild(overlay);
  setTimeout(() => overlay.querySelector('input[name="artist"]')?.focus(), 30);
}
window._baOpenTuningAdd = _baOpenTuningAdd;

async function _baSubmitTuningAdd() {
  const form = document.getElementById("ba-tuning-add-form");
  if (!form) return;
  const statusEl = document.getElementById("ba-tuning-add-status");
  const data = {};
  for (const el of form.elements) {
    if (!el.name) continue;
    data[el.name] = el.value.trim();
  }
  if (!data.artist || !data.title) {
    if (statusEl) statusEl.textContent = "Artist and Title are required.";
    return;
  }
  if (statusEl) statusEl.textContent = "Saving…";
  try {
    const r = await apiFetch("/api/blues-archive/tunings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      if (statusEl) statusEl.textContent = `Save failed: ${err.error || `HTTP ${r.status}`}`;
      return;
    }
    document.getElementById("ba-tuning-add-overlay")?.remove();
    _baLoadTuningsGrid();
  } catch (e) {
    if (statusEl) statusEl.textContent = `Save failed: ${e?.message || e}`;
  }
}
window._baSubmitTuningAdd = _baSubmitTuningAdd;

function _baRenderTuningsPager() {
  const el = document.getElementById("ba-tunings-pager");
  if (!el) return;
  const pageCount = Math.max(1, Math.ceil(_baTuningsTotal / _BA_TUNINGS_LIMIT));
  const cur = _baTuningsPage + 1;
  if (pageCount <= 1) { el.innerHTML = ""; return; }
  el.innerHTML = `
    <button class="archive-btn" ${cur <= 1 ? "disabled" : ""} onclick="_baTuningsGoToPage(${_baTuningsPage - 1})">‹ Prev</button>
    <span style="color:var(--muted)">Page ${cur} / ${pageCount}</span>
    <button class="archive-btn" ${cur >= pageCount ? "disabled" : ""} onclick="_baTuningsGoToPage(${_baTuningsPage + 1})">Next ›</button>`;
}
function _baTuningsGoToPage(p) { _baTuningsPage = Math.max(0, p); _baLoadTuningsGrid(); }
window._baTuningsGoToPage = _baTuningsGoToPage;

function _baSortTunings(key) {
  _baToggleSort(_baTuningsSort, key);
  _baTuningsPage = 0;
  _baLoadTuningsGrid();
}
window._baSortTunings = _baSortTunings;

function _baTuningsDebouncedSearch() {
  if (_baTuningsSearchTimer) clearTimeout(_baTuningsSearchTimer);
  _baTuningsSearchTimer = setTimeout(() => { _baTuningsPage = 0; _baLoadTuningsGrid(); }, 280);
}
window._baTuningsDebouncedSearch = _baTuningsDebouncedSearch;

function _baTuningsApplyFilters() { _baTuningsPage = 0; _baLoadTuningsGrid(); }
window._baTuningsApplyFilters = _baTuningsApplyFilters;

function _baTuningsClearFilters() {
  const s = document.getElementById("ba-tunings-search");
  const a = document.getElementById("ba-tunings-artist");
  const p = document.getElementById("ba-tunings-position");
  if (s) s.value = "";
  if (a) a.value = "";
  if (p) p.value = "";
  _baTuningsPage = 0;
  _baLoadTuningsGrid();
}
window._baTuningsClearFilters = _baTuningsClearFilters;

// ── Stats strip ──────────────────────────────────────────────────────
// Curation dashboard chips at the top of the archive list view. Each
// chip is also a one-click shortcut: clicking "Orphan lyrics" jumps to
// the Lyrics sub-tab with the unmatched filter on, "Missing tuning"
// jumps with the tuning filter set to (unspecified), etc.
async function _baLoadStats() {
  const artistsEl = document.getElementById("blues-archive-stats");
  const lyricsEl  = document.getElementById("blues-archive-stats-lyrics");
  if (!artistsEl && !lyricsEl) return;
  try {
    const r = await apiFetch("/api/blues-archive/stats");
    if (!r.ok) {
      if (artistsEl) artistsEl.innerHTML = "";
      if (lyricsEl)  lyricsEl.innerHTML  = "";
      return;
    }
    const s = await r.json();
    const chip = (label, n, opts = {}) => {
      // Hide chips with zero instances — clicking them produces an
      // empty filtered view and they only add noise to the strip.
      // alwaysShow keeps the anchor chip (Artists total / Lyrics total)
      // even when the DB is freshly empty.
      const count = Number(n) || 0;
      if (count === 0 && !opts.alwaysShow) return "";
      const click = opts.onclick ? ` onclick="${opts.onclick}"` : "";
      const cur = opts.onclick ? "cursor:pointer;" : "";
      const tone = opts.tone === "warn" ? "color:#e8a85a" : "color:var(--muted)";
      return `<span class="ba-stat-chip"${click} title="${escHtml(opts.title || label)}" style="${cur}font-size:0.7rem;padding:0.05rem 0.25rem;${tone}">${escHtml(label)}: <strong style="color:var(--text)">${count.toLocaleString()}</strong></span>`;
    };
    // Artist buckets retired — the Blues Archive is a lyrics + tunings
    // reference now, so the artist stats strip stays empty.
    if (artistsEl) artistsEl.innerHTML = "";
    // Lyrics panel: only the lyrics-side buckets.
    if (lyricsEl) {
      lyricsEl.innerHTML = [
        chip("Lyrics total", s.lyrics_total, {
          alwaysShow: true,
          onclick: "_baJumpAllLyrics()",
          title: "Total rows in the blues_lyrics table (scraped from weeniecampbell.com). Click to clear any active filter.",
        }),
        chip("Orphan lyrics", s.lyrics_orphan, {
          tone: "warn",
          onclick: "_baJumpOrphans()",
          title: "Lyrics whose artist string doesn't link to a blues_artists row (artist_id IS NULL). Click to filter to unmatched only.",
        }),
        chip("Missing tuning", s.lyrics_missing_tuning, {
          tone: "warn",
          onclick: "_baJumpMissingTuning()",
          title: "Lyrics with no tuning extracted from the wiki page (tuning IS NULL). Click to filter to '(unspecified)'.",
        }),
      ].join("");
    }
  } catch {
    if (artistsEl) artistsEl.innerHTML = "";
    if (lyricsEl)  lyricsEl.innerHTML  = "";
  }
}
// Expose alongside _baLoadList so blues-admin.js can repaint the
// stats strip when the editor mutates a row (e.g. clearing a photo
// shifts the empty/with-photo bucket counts).
window._baLoadStats = _baLoadStats;

// Jump handlers used by the stats-strip chips on the Artists side.
// Set the category filter + switch to the Artists tab.

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

// Releases + Tunings CSV exports — mirror the lyrics CSV flow.
// Endpoints stream the full table; the download trigger is the same
// blob → object URL → anchor.click pattern used elsewhere.

async function _baExportTuningsCsv() {
  const btn = document.getElementById("blues-export-tunings-btn");
  const orig = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "Exporting…"; }
  try {
    const r = await apiFetch("/api/blues-archive/tunings/export.csv");
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const blob = await r.blob();
    const fname = `seadisco-tunings-${new Date().toISOString().slice(0, 10)}.csv`;
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
window._baExportTuningsCsv = _baExportTuningsCsv;

// ── Unified Export sub-tab ───────────────────────────────────────────
// Source → supported format list. Each entry maps to a pair {endpoint,
// fname} via _baExportRun. Filter inputs live on the panel — only the
// keys present in `filters` get appended as query params, so endpoints
// that don't yet recognise a key just ignore it.
const _BA_EXPORT_FORMATS = {
  lyrics:   ["csv", "pdf", "doc"],
  tunings:  ["csv"],
};
function _baExportInit() {
  const src = document.getElementById("ba-export-source");
  if (!src) return;
  _baExportOnSourceChange();
}
window._baExportInit = _baExportInit;

function _baExportOnSourceChange() {
  const src = document.getElementById("ba-export-source")?.value || "lyrics";
  const fmtSel = document.getElementById("ba-export-format");
  if (fmtSel) {
    const formats = _BA_EXPORT_FORMATS[src] || ["csv"];
    fmtSel.innerHTML = formats.map(f => `<option value="${f}">${f.toUpperCase()}</option>`).join("");
  }
  // Source-specific filter containers — show the matching strip, hide
  // the others. Containers that don't exist (e.g. releases / tunings)
  // simply have no filter UI.
  document.getElementById("ba-export-filters-lyrics").style.display  = src === "lyrics"  ? "flex" : "none";
}
window._baExportOnSourceChange = _baExportOnSourceChange;

async function _baExportRun() {
  const src = document.getElementById("ba-export-source")?.value || "lyrics";
  const fmt = document.getElementById("ba-export-format")?.value || "csv";
  const btn = document.getElementById("ba-export-go");
  const statusEl = document.getElementById("ba-export-status");
  // Map (source, format) to an endpoint URL + suggested filename. The
  // endpoints already exist for lyrics CSV/PDF/DOC and artists CSV/PDF;
  // releases + tunings ship CSV only. Filter inputs are appended as
  // query params; endpoints that don't recognise them just ignore.
  const today = new Date().toISOString().slice(0, 10);
  let endpoint, fname;
  const qs = new URLSearchParams();
  if (src === "lyrics") {
    const q       = document.getElementById("ba-export-lyrics-q")?.value.trim()       || "";
    const tuning  = document.getElementById("ba-export-lyrics-tuning")?.value.trim()  || "";
    const favOnly = document.getElementById("ba-export-lyrics-favorites")?.checked    || false;
    if (q)       qs.set("q", q);
    if (tuning)  qs.set("tuning", tuning);
    if (favOnly) qs.set("favorites", "1");
    endpoint = `/api/admin/lyrics/export.${fmt}`;
    fname    = `seadisco-lyrics-${today}.${fmt}`;
  } else if (src === "artists") {
    const q = document.getElementById("ba-export-artists-q")?.value.trim() || "";
    if (q) qs.set("q", q);
    if (fmt === "csv") { endpoint = `/api/admin/blues/export.csv`; fname = `seadisco-blues-${today}.csv`; }
    else               { endpoint = `/api/admin/blues/export.pdf`; fname = `seadisco-artists-${today}.pdf`; }
  } else if (src === "releases") {
    endpoint = `/api/blues-archive/releases/export.${fmt}`;
    fname    = `seadisco-releases-${today}.${fmt}`;
  } else if (src === "tunings") {
    endpoint = `/api/blues-archive/tunings/export.${fmt}`;
    fname    = `seadisco-tunings-${today}.${fmt}`;
  } else {
    if (statusEl) statusEl.textContent = `Unsupported source: ${src}`;
    return;
  }
  const url = qs.toString() ? `${endpoint}?${qs.toString()}` : endpoint;
  if (btn) { btn.disabled = true; btn.textContent = "Building…"; }
  if (statusEl) statusEl.textContent = "Requesting…";
  try {
    const r = await apiFetch(url);
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const blob = await r.blob();
    const objUrl = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = objUrl; a.download = fname;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(objUrl), 5000);
    if (statusEl) statusEl.textContent = `Downloaded ${fname} (${(blob.size / 1024 / 1024).toFixed(2)} MB).`;
  } catch (e) {
    if (statusEl) statusEl.textContent = `Export failed: ${e?.message || e}`;
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "Download"; }
  }
}
window._baExportRun = _baExportRun;

// Single dispatcher for every admin-only action that's powered by
// /blues-admin.js. Each kind maps to a function name; we lazy-load
// the module on first call, then invoke. Keeps the inline onclick
// attributes short and centralizes the lazy-load plumbing.

// Open the full /admin-style editor (25 fields + enrichment buttons)
// in place. The editor JS lives in /blues-admin.js — lazy-loaded so
// non-admin sessions never pay for it. The editor overlay DOM is
// embedded in index.html as #blues-editor-overlay and friends.
// On save, the editor fires window._bluesDbAfterSaveOnce so we can
// refresh the artist popup that was open behind it.

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

// ── Promote orphan lyric to a new artist ─────────────────────────────
// Tuning-chip click on the artist popup: switch to the Lyrics tab and
// pre-filter to (artist, tuning).

// ── Bulk reassign picker ─────────────────────────────────────────────
// Two ways to specify the source: either pick an existing artist
// (lyrics with that artist_id) or type a free-text name (matches
// LOWER(TRIM(artist)) for unmigrated rows). Server endpoint runs the
// reassignment in a single transaction.

let _baReassignPickerTimer = null;
let _baReassignFromId = null;



// Click handler for the "+ artist" link on orphan lyric rows. Opens a
// picker so the user can EITHER link the orphan to an existing artist
// (the common case: "John Lee" → John Lee Hooker) OR fall back to
// creating a new artist row from the orphan string. Pre-fills the
// search with the orphan name so existing matches show up immediately.

let _baOrphanPickerTimer = null;



// "+ Create as new artist" fallback inside the orphan picker — calls
// the existing promote endpoint that creates the row + links siblings.

// ─── Connections subtab ──────────────────────────────────────────────
// Visualisation of blues_artist_links. Three view modes — network
// (force-directed Cytoscape graph), hub (one artist centred with their
// direct relations), and matrix (sortable table). Cytoscape.js is
// lazy-loaded from CDN on first open. Relation-type toggle chips let
// the curator narrow the graph by edge kind. Whole-graph payload is
// small (one row per link) so we fetch once + cache in memory.

// Vivid, evenly-spaced hues on the dark canvas — the original palette
// was muted enough that nearly every line read as the same desaturated
// grey-ish tone. Spaced ~60° apart on the colour wheel so adjacent
// kinds still stay distinguishable: gold, green, magenta, cyan, orange,
// violet. All ≥ 70% saturation at ~60% lightness so edges and chips
// pop against the rgba(0,0,0,0.25) graph background.
const _BA_CONN_KINDS = [
  { id: "pseudonym", label: "Pseudonym", color: "#f5d442" }, // gold
  { id: "band",      label: "Band",      color: "#4ade80" }, // bright green
  { id: "spouse",    label: "Spouse",    color: "#ec4899" }, // hot pink
  { id: "traveled",  label: "Traveled",  color: "#22d3ee" }, // cyan
  { id: "mentor",    label: "Mentor",    color: "#f97316" }, // orange
  { id: "family",    label: "Family",    color: "#a855f7" }, // violet
];
const _BA_CONN_VIEW_KEY = "sd_ba_conn_view";
const _BA_CONN_KINDS_KEY = "sd_ba_conn_kinds";
let _baConnGraph = null;     // {nodes, edges} from API
let _baConnCy = null;        // active Cytoscape instance
let _baCytoscapeLoading = null;
let _baConnView = (() => {
  try { return localStorage.getItem(_BA_CONN_VIEW_KEY) || "network"; }
  catch { return "network"; }
})();
let _baConnKindsOn = (() => {
  try {
    const raw = localStorage.getItem(_BA_CONN_KINDS_KEY);
    const arr = raw ? JSON.parse(raw) : null;
    if (Array.isArray(arr) && arr.length) return new Set(arr);
  } catch {}
  return new Set(_BA_CONN_KINDS.map(k => k.id));
})();
let _baConnHubId = null;
// Network-mode focus: when set, the network view only draws the
// connected component reachable from this id via the currently-enabled
// link kinds. null = whole graph. Persisted across reloads.
const _BA_CONN_FOCUS_KEY = "sd_ba_conn_focus";
let _baConnFocusId = (() => {
  try {
    const v = localStorage.getItem(_BA_CONN_FOCUS_KEY);
    const n = v ? Number(v) : NaN;
    return Number.isFinite(n) ? n : null;
  } catch { return null; }
})();






// BFS from `startId` through the supplied set of enabled kinds. Returns
// a Set of every node id reachable (inclusive of start). Adjacency is
// built on the fly from _baConnGraph.edges to keep things simple — the
// link table is small.






let _baConnMatrixSort = { key: "lo_name", dir: "asc" };


