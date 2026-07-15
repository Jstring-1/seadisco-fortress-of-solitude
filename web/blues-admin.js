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
  // Surface the "Last fetched" stamp next to the Fetch new lyrics
  // button so the curator knows whether the next click would walk a
  // tight or wide window.
  if (typeof lyricsRefreshSinceLastHint === "function") lyricsRefreshSinceLastHint();
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

async function lyricsStartRecentRefresh() {
  const btn = document.getElementById("lyrics-recent-btn");
  const statusEl = document.getElementById("lyrics-scrape-status");
  if (!btn || !statusEl) return;
  // Default flow: 30 days, new pages only. Holding Shift while
  // clicking includes edits too (refetches existing rows so the
  // wikitext/tuning columns catch any upstream edits). Keeps the
  // button single-click for the common case while exposing the
  // rare full-refresh path via a modifier the curator can discover
  // from the button title attribute.
  const days = 30;
  const includeEdits = !!(window.event && window.event.shiftKey);
  btn.disabled = true;
  statusEl.textContent = `Starting recent-changes refresh (${days}d${includeEdits ? ", incl. edits" : ", new only"})…`;
  try {
    const r = await apiFetch("/api/admin/lyrics/scrape/recent", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ days, edits: includeEdits ? 1 : 0 }),
    });
    if (r.status === 409) {
      statusEl.textContent = "Another job is running — attaching to its status…";
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
window.lyricsStartRecentRefresh = lyricsStartRecentRefresh;

async function lyricsStartPreScrape() {
  const btn = document.getElementById("lyrics-prescrape-btn");
  const statusEl = document.getElementById("lyrics-scrape-status");
  if (!btn || !statusEl) return;
  btn.disabled = true;
  statusEl.textContent = "Pre-scrape starting…";
  try {
    const r = await apiFetch("/api/admin/lyrics/scrape/precheck", { method: "POST" });
    if (r.status === 409) {
      statusEl.textContent = "Another job is running — attaching to its status…";
    } else if (!r.ok) {
      statusEl.textContent = `Pre-scrape failed: HTTP ${r.status}`;
      btn.disabled = false;
      return;
    }
    lyricsStartPolling();
  } catch (e) {
    statusEl.textContent = `Pre-scrape failed: ${e?.message || e}`;
    btn.disabled = false;
  }
}
window.lyricsStartPreScrape = lyricsStartPreScrape;

// "Fetch new lyrics" button — calls the /since-last endpoint which
// reads the server-side lyrics_scrape_last_at stamp, computes the
// recentchanges window from that timestamp (or 180 days on first run),
// fires the same recent-refresh worker, and re-stamps on clean finish.
async function lyricsFetchNewSinceLast() {
  const btn = document.getElementById("lyrics-scrape-btn");
  const statusEl = document.getElementById("lyrics-scrape-status");
  if (!btn || !statusEl) return;
  btn.disabled = true;
  statusEl.textContent = "Starting…";
  try {
    const r = await apiFetch("/api/admin/lyrics/scrape/since-last", { method: "POST" });
    if (r.status === 409) {
      statusEl.textContent = "Another job is running — attaching to its status…";
    } else if (!r.ok) {
      statusEl.textContent = `Failed: HTTP ${r.status}`;
      btn.disabled = false;
      return;
    } else {
      const body = await r.json().catch(() => ({}));
      const win = body?.daysBack ? `${body.daysBack}d` : "180d";
      statusEl.textContent = body?.lastAt
        ? `Fetching changes since ${new Date(body.lastAt).toLocaleString()} (window ${win})…`
        : `First run — falling back to ${win} window…`;
    }
    lyricsStartPolling();
    // Refresh the "Last fetched" hint so the next click already
    // reflects this run.
    setTimeout(lyricsRefreshSinceLastHint, 4000);
  } catch (e) {
    statusEl.textContent = `Failed: ${e?.message || e}`;
    btn.disabled = false;
  }
}
window.lyricsFetchNewSinceLast = lyricsFetchNewSinceLast;

// Show the lyrics_scrape_last_at timestamp next to the button so the
// user can tell whether the next click would walk a tight window or
// a wide one. Polled on tab show + after a fetch completes.
async function lyricsRefreshSinceLastHint() {
  const el = document.getElementById("lyrics-since-last");
  if (!el) return;
  try {
    const r = await apiFetch("/api/admin/lyrics/scrape/since-last");
    if (!r.ok) { el.textContent = ""; return; }
    const { lastAt } = await r.json();
    el.textContent = lastAt
      ? `Last fetched: ${new Date(lastAt).toLocaleString()}`
      : "Never fetched yet";
  } catch { el.textContent = ""; }
}
window.lyricsRefreshSinceLastHint = lyricsRefreshSinceLastHint;

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
    const preBtn  = document.getElementById("lyrics-prescrape-btn");
    const recBtn  = document.getElementById("lyrics-recent-btn");
    if (s.running) {
      btn.disabled = true;
      if (preBtn) preBtn.disabled = true;
      if (recBtn) recBtn.disabled = true;
      // Stop button is only meaningful for the long scrape; pre-scrape
      // finishes on its own in a few minutes and there's no partial
      // progress to preserve.
      if (stopBtn) stopBtn.style.display = s.jobKind === "scrape" ? "" : "none";
      const phase = s.phase || "running";
      // Label the job kind so artist-sync runs aren't mistaken for a
      // new scrape — they share progress state but mean different
      // things (sync is DB-only after the wiki walk; scrape fetches
      // page content from the wiki).
      const jobLabel = s.jobKind === "artist-sync"    ? "Artist sync"
                     : s.jobKind === "scrape"         ? "Lyric scrape"
                     : s.jobKind === "pre-scrape"     ? "Pre-scrape"
                     : s.jobKind === "recent-changes" ? "Recent refresh"
                     :                                  "Running";
      if (phase === "discovering") {
        statusEl.innerHTML = `<span style="color:var(--accent)">${escHtml(jobLabel)} ·</span> ${escHtml(s.message || "").slice(0, 140)}`;
      } else {
        const pct = s.pagesDiscovered ? Math.round((s.pagesScraped + s.pagesSkipped) / s.pagesDiscovered * 100) : 0;
        statusEl.innerHTML = `<span style="color:var(--accent)">${escHtml(jobLabel)} ·</span> ${s.pagesScraped}/${s.pagesDiscovered} (${pct}%) · skipped ${s.pagesSkipped} · failed ${s.pagesFailed}${s.currentTitle ? ` · <em style="color:var(--muted)">${escHtml(s.currentTitle).slice(0, 60)}</em>` : ""}`;
      }
    } else {
      btn.disabled = false;
      if (preBtn) preBtn.disabled = false;
      if (recBtn) recBtn.disabled = false;
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
  // Pre-scrape result: show the list of titles the wiki has that we
  // don't, with a header that summarises the diff. Distinct from the
  // recentlyAdded panel because nothing is in our DB yet — these
  // titles aren't clickable links to a viewer, just shortcuts to a
  // SeaDisco / Discogs search so the admin can scout ahead of a
  // commit to the full ~80 min scrape.
  if (s?.jobKind === "pre-scrape" && Array.isArray(s.newTitles)) {
    const titles = s.newTitles;
    const total = Number(s.newTitlesTotal) || titles.length;
    if (!titles.length && !s.running && total === 0) {
      panel.style.display = "";
      panel.innerHTML = `
        <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:0.4rem">
          <strong style="font-size:0.86rem">Pre-scrape complete <span style="color:var(--muted);font-weight:400">— no new lyrics on the wiki</span></strong>
          <button type="button" class="archive-btn" onclick="document.getElementById('lyrics-scrape-added').style.display='none'">×</button>
        </div>
        <div style="color:var(--muted);font-size:0.84rem">Wiki has ${(s.wikiTotalPages || 0).toLocaleString()} pages and you already have all of them. A rescrape would be a no-op right now.</div>`;
      return;
    }
    if (titles.length) {
      panel.style.display = "";
      const headerLabel = s.running ? "Pre-scrape in progress…" : `Pre-scrape result — ${total.toLocaleString()} new title${total === 1 ? "" : "s"} on the wiki`;
      const sub = `<span style="color:var(--muted);font-weight:400">${s.wikiTotalPages ? `${s.wikiTotalPages.toLocaleString()} total on wiki` : ""}${total > titles.length ? ` · showing first ${titles.length.toLocaleString()}` : ""}</span>`;
      const html = titles.map(t => {
        const title = String(t || "");
        const sdQs = `?q=${encodeURIComponent(title)}` +
                     `&r=${encodeURIComponent("master+")}` +
                     `&s=${encodeURIComponent("year:asc")}`;
        return `<div style="display:flex;gap:0.4rem;align-items:baseline;padding:0.15rem 0;font-size:0.84rem;border-bottom:1px solid rgba(255,255,255,0.04)">
          <a href="/${sdQs}" class="ba-lyric-search" title="Search SeaDisco — masters+, oldest first">🔍</a>
          <span style="color:var(--text)">${escHtml(title)}</span>
        </div>`;
      }).join("");
      panel.innerHTML = `
        <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:0.4rem">
          <strong style="font-size:0.86rem">${escHtml(headerLabel)} ${sub}</strong>
          <button type="button" class="archive-btn" onclick="document.getElementById('lyrics-scrape-added').style.display='none'">×</button>
        </div>
        <div style="max-height:240px;overflow-y:auto">${html}</div>
        ${!s.running && total > 0 ? `<div style="margin-top:0.5rem;font-size:0.78rem;color:var(--muted)">Ready to fetch? Click <strong style="color:var(--text)">Rescrape new</strong> to pull these (and skip everything you already have).</div>` : ""}`;
      return;
    }
    // pre-scrape is running but no titles yet — let the status row do
    // the talking and don't blank the panel mid-run.
    if (s.running) return;
  }
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
