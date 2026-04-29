// ── In-app YouTube search + Saved tab ────────────────────────────────
//
// Mirrors the LOC / Archive / Wiki saved-tab pattern. Search results
// come from the server proxy (/api/youtube/search) which talks to
// the YouTube Data API v3 with our key + 24h cache. Saved videos
// persist in the user_youtube_saves table; ★ on any result toggles.
// Clicking ▶ on a result hands off to the existing mini-player
// iframe (openVideo); ＋ adds to the cross-source play queue with
// the same shape as a track-row queue add.

let _ytTab = "search";        // "search" | "saved"
let _ytLastQuery = "";
let _ytLastResults = [];
let _ytNextPageToken = null;

// Saved-tab state
let _ytSavedIds = null;       // Set<videoId>
let _ytSavedItems = null;     // Array<saved video record>
let _ytSavedLoading = false;
let _ytSavedFilter = "";
let _ytSavedSort = "recent";  // recent | title | channel

async function initYoutubeView() {
  // Sync ★ state for already-rendered cards on first open.
  if (_ytSavedIds == null) _youtubeLoadSavedIds();
  // Honor ?tab=saved deep links + ?yq= search query.
  let initialTab = "search";
  let initialQuery = "";
  try {
    const qs = new URLSearchParams(location.search);
    if (qs.get("tab") === "saved") initialTab = "saved";
    initialQuery = qs.get("yq") || "";
  } catch {}
  _youtubeSwitchTab(initialTab, { pushUrl: false });
  if (initialTab === "search") {
    const qInput = document.getElementById("youtube-view-q");
    if (qInput) {
      qInput.value = initialQuery;
      setTimeout(() => qInput.focus(), 50);
    }
    if (initialQuery) runYoutubeSearch(initialQuery);
  }
}

function _youtubeSwitchTab(tab, { pushUrl = true } = {}) {
  _ytTab = tab === "saved" ? "saved" : "search";
  document.querySelectorAll("#youtube-view .loc-tab").forEach(b => b.classList.remove("active"));
  document.querySelector(`#youtube-view .loc-tab-${_ytTab}`)?.classList.add("active");
  const sp = document.querySelector(".youtube-panel-search");
  const ss = document.querySelector(".youtube-panel-saved");
  if (sp) sp.style.display = _ytTab === "search" ? "" : "none";
  if (ss) ss.style.display = _ytTab === "saved"  ? "" : "none";
  if (_ytTab === "saved" && _ytSavedItems == null) _youtubeLoadSaved();
  // Reflect tab in URL.
  if (pushUrl && typeof history?.pushState === "function") {
    const qs = new URLSearchParams(location.search);
    qs.set("v", "youtube");
    if (_ytTab === "saved") qs.set("tab", "saved"); else qs.delete("tab");
    const next = "/?" + qs.toString();
    if (location.pathname + location.search !== next) {
      history.pushState({}, "", next);
    }
  }
}

// ── Search ─────────────────────────────────────────────────────────────
async function runYoutubeSearch(query, opts) {
  const q = String(query ?? "").trim();
  const statusEl = document.getElementById("youtube-view-status");
  const resultsEl = document.getElementById("youtube-view-results");
  const pageEl = document.getElementById("youtube-view-pagination");
  if (!q) {
    if (resultsEl) resultsEl.innerHTML = "";
    if (statusEl) statusEl.textContent = "";
    if (pageEl) pageEl.innerHTML = "";
    return;
  }
  // Update URL so the search is shareable / back-button-able.
  try {
    const qs = new URLSearchParams(location.search);
    qs.set("v", "youtube");
    qs.set("yq", q);
    qs.delete("tab");
    history.replaceState({}, "", "/?" + qs.toString());
  } catch {}
  const isPaging = !!opts?.pageToken;
  if (!isPaging) {
    _ytLastQuery = q;
    _ytLastResults = [];
    if (resultsEl) resultsEl.innerHTML = `<div class="loc-empty">Searching…</div>`;
    if (statusEl)  statusEl.textContent = "";
    if (pageEl)    pageEl.innerHTML = "";
  }
  try {
    const params = [`q=${encodeURIComponent(q)}`];
    if (opts?.pageToken) params.push(`pageToken=${encodeURIComponent(opts.pageToken)}`);
    // apiFetch (not raw fetch) attaches the Clerk Bearer so the server
    // can identify signed-in users and bypass the anon-IP rate limit.
    // Without this, every request looks anonymous to the server.
    const r = await apiFetch(`/api/youtube/search?${params.join("&")}`, { cache: "no-store" });
    if (!r.ok) {
      const errBody = await r.text().catch(() => "");
      console.warn("[youtube] search failed:", r.status, errBody);
      if (statusEl) statusEl.textContent = `Search failed (${r.status}). Try again in a minute.`;
      if (resultsEl) resultsEl.innerHTML = "";
      return;
    }
    const j = await r.json();
    const items = Array.isArray(j?.items) ? j.items : [];
    _ytLastResults = isPaging ? _ytLastResults.concat(items) : items;
    _ytNextPageToken = j?.nextPageToken ?? null;
    if (statusEl) {
      const count = _ytLastResults.length;
      statusEl.textContent = count ? `${count} result${count === 1 ? "" : "s"}` : "";
    }
    _renderYoutubeResults();
  } catch (e) {
    console.warn("[youtube] search threw:", e);
    if (statusEl) statusEl.textContent = "Search failed.";
    if (resultsEl) resultsEl.innerHTML = "";
  }
}

function _renderYoutubeResults() {
  const resultsEl = document.getElementById("youtube-view-results");
  const pageEl = document.getElementById("youtube-view-pagination");
  if (!resultsEl) return;
  if (!_ytLastResults.length) {
    resultsEl.innerHTML = `<div class="loc-empty">No results.</div>`;
    if (pageEl) pageEl.innerHTML = "";
    return;
  }
  resultsEl.innerHTML = _ytLastResults.map(it => _youtubeRowHtml(it)).join("");
  if (pageEl) {
    pageEl.innerHTML = _ytNextPageToken
      ? `<button type="button" class="archive-load-more" onclick="_youtubeLoadMore()">Load more results</button>`
      : "";
  }
}

function _youtubeLoadMore() {
  if (!_ytNextPageToken || !_ytLastQuery) return;
  runYoutubeSearch(_ytLastQuery, { pageToken: _ytNextPageToken });
}

// ── Saved tab ──────────────────────────────────────────────────────────
async function _youtubeLoadSavedIds() {
  if (!window._clerk?.user) { _ytSavedIds = new Set(); return; }
  try {
    const r = await apiFetch("/api/user/youtube-saves/ids");
    if (!r.ok) { _ytSavedIds = new Set(); return; }
    const j = await r.json();
    _ytSavedIds = new Set(Array.isArray(j?.ids) ? j.ids : []);
    document.querySelectorAll("#youtube-view .yt-row[data-vid]").forEach(el => {
      const id = el.dataset.vid;
      const star = el.querySelector(".yt-save-btn");
      if (star) {
        const saved = _ytSavedIds.has(id);
        star.classList.toggle("is-saved", saved);
        star.textContent = saved ? "★" : "☆";
      }
    });
  } catch { _ytSavedIds = new Set(); }
}

async function _youtubeLoadSaved() {
  if (_ytSavedLoading) return;
  _ytSavedLoading = true;
  const listEl = document.getElementById("youtube-saved-results");
  if (!window._clerk?.user) {
    if (listEl) listEl.innerHTML = `<div class="loc-empty">Sign in to save YouTube videos.</div>`;
    _ytSavedLoading = false;
    return;
  }
  if (listEl) listEl.innerHTML = `<div class="loc-empty">Loading…</div>`;
  try {
    const r = await apiFetch("/api/user/youtube-saves");
    if (!r.ok) {
      if (listEl) listEl.innerHTML = `<div class="loc-empty">Could not load saved videos.</div>`;
      return;
    }
    const j = await r.json();
    _ytSavedItems = (Array.isArray(j?.items) ? j.items : []).map(s => ({
      videoId:     s.videoId,
      title:       s.title || s.data?.title || s.videoId,
      channel:     s.channel || s.data?.channel || "",
      thumbnail:   s.thumbnail || s.data?.thumbnail || `https://i.ytimg.com/vi/${encodeURIComponent(s.videoId)}/mqdefault.jpg`,
      description: s.data?.description || "",
      _savedAt:    s.savedAt,
    }));
    _renderYoutubeSavedRows();
  } catch {
    if (listEl) listEl.innerHTML = `<div class="loc-empty">Could not load saved videos.</div>`;
  } finally {
    _ytSavedLoading = false;
  }
}

function _youtubeSavedFilterSort() {
  if (!Array.isArray(_ytSavedItems)) return [];
  const q = _ytSavedFilter.trim().toLowerCase();
  const filtered = _ytSavedItems.filter(it => {
    if (!q) return true;
    const hay = [it.title, it.channel, it.description].filter(Boolean).join(" ").toLowerCase();
    return hay.includes(q);
  });
  const cmp = {
    "recent":  (a, b) => String(b._savedAt || "").localeCompare(String(a._savedAt || "")),
    "title":   (a, b) => String(a.title   || "").localeCompare(String(b.title   || ""), undefined, { sensitivity: "base" }),
    "channel": (a, b) => String(a.channel || "").localeCompare(String(b.channel || ""), undefined, { sensitivity: "base" }),
  }[_ytSavedSort] || ((a, b) => 0);
  filtered.sort(cmp);
  return filtered;
}

function _renderYoutubeSavedRows() {
  const listEl = document.getElementById("youtube-saved-results");
  const countEl = document.getElementById("youtube-saved-count");
  if (!listEl) return;
  const view = _youtubeSavedFilterSort();
  if (countEl) countEl.textContent = String(view.length);
  if (!_ytSavedItems?.length) {
    listEl.innerHTML = `<div class="loc-empty">No saved videos yet. Click ★ on any result in Search to save it.</div>`;
    return;
  }
  if (!view.length) {
    listEl.innerHTML = `<div class="loc-empty">No saved videos match.</div>`;
    return;
  }
  listEl.innerHTML = view.map(it => _youtubeRowHtml(it)).join("");
}

function _youtubeOnSavedFilterInput(input) {
  _ytSavedFilter = input.value || "";
  _renderYoutubeSavedRows();
}
function _youtubeOnSavedSortChange(select) {
  _ytSavedSort = select.value || "recent";
  _renderYoutubeSavedRows();
}

// ── Row HTML ──────────────────────────────────────────────────────────
function _youtubeRowHtml(it) {
  const id = String(it.videoId || it.id || "");
  if (!id) return "";
  const safeId = escHtml(id);
  const safeTitle = escHtml(it.title || "Untitled");
  const safeChannel = escHtml(it.channel || "");
  const safeDesc = escHtml(String(it.description || "").slice(0, 200));
  const thumb = it.thumbnail || `https://i.ytimg.com/vi/${encodeURIComponent(id)}/mqdefault.jpg`;
  const isSaved = !!_ytSavedIds?.has(id);
  const saveBtn = `<button type="button" class="archive-btn yt-save-btn${isSaved ? " is-saved" : ""}" onclick="_youtubeToggleSave(this)" title="${isSaved ? "Remove from Saved" : "Save"}">${isSaved ? "★" : "☆"}</button>`;
  const playBtn = `<button type="button" class="archive-btn archive-btn-play" onclick="_youtubePlayRow(this)" title="Play in the bar">▶ Play</button>`;
  const queueBtn = `<button type="button" class="archive-btn archive-btn-queue" onclick="_youtubeQueueRow(this)" title="Add to play queue">＋ Queue</button>`;
  const linkBtn = `<a class="archive-btn archive-btn-link" href="https://www.youtube.com/watch?v=${encodeURIComponent(id)}" target="_blank" rel="noopener">Open on YouTube ↗</a>`;
  // When the popup was opened from the per-track 🎵 Suggest affordance,
  // window._sdSuggestForTrack is set with the track context. Render an
  // extra "✓ Suggest" button so users can pin the picked video to that
  // track. First-submission-wins on the server.
  const suggestBtn = window._sdSuggestForTrack
    ? `<button type="button" class="archive-btn archive-btn-suggest" onclick="_youtubeSuggestForTrack(this)" title="Suggest this video for the track">✓ Suggest</button>`
    : "";
  // Album-mode: when window._sdSuggestAlbumContext is set, render a
  // dropdown of missing tracks and a "Stage" button. Picking from
  // the dropdown + clicking Stage queues the assignment locally; the
  // user submits the whole batch via a footer button.
  let albumAssignControl = "";
  if (window._sdSuggestAlbumContext) {
    const ctx = window._sdSuggestAlbumContext;
    const auto = _albumAutoMatchTrack(it.title || "", ctx.tracks || []);
    const opts = (ctx.tracks || []).map(t => {
      const sel = (auto && auto.position === t.position) ? " selected" : "";
      const taken = (window._sdSuggestStaged || {})[t.position] && (window._sdSuggestStaged[t.position].videoId !== id);
      const label = taken ? `${t.position}. ${t.title} (already staged)` : `${t.position}. ${t.title}`;
      return `<option value="${escHtml(t.position)}"${sel}${taken ? " disabled" : ""}>${escHtml(label)}</option>`;
    }).join("");
    const staged = (window._sdSuggestStaged || {});
    // Find which track (if any) is currently staged with THIS video.
    const stagedFor = Object.entries(staged).find(([_, v]) => v && v.videoId === id);
    const isStaged = !!stagedFor;
    albumAssignControl = `
      <select class="sd-filter-select album-assign-select" data-vid="${safeId}" data-vtitle="${safeTitle}" onchange="_youtubeAlbumAssignChanged(this)">
        <option value="">— skip —</option>
        ${opts}
      </select>
      <button type="button" class="archive-btn archive-btn-suggest album-assign-stage${isStaged ? " is-staged" : ""}" onclick="_youtubeAlbumStage(this)" title="${isStaged ? "Already staged — click to update" : "Stage this assignment (submit all at the bottom)"}">${isStaged ? "✓ Staged" : "Stage"}</button>
    `;
  }
  return `
    <div class="yt-row archive-row" data-vid="${safeId}" data-title="${safeTitle}" data-channel="${safeChannel}" data-thumb="${escHtml(thumb)}">
      <img class="yt-row-thumb" src="${escHtml(thumb)}" alt="" loading="lazy" width="120" height="68" decoding="async">
      <div class="archive-row-main">
        <div class="archive-row-title">${safeTitle}</div>
        ${safeChannel ? `<div class="archive-row-date">${safeChannel}</div>` : ""}
        ${safeDesc ? `<div class="archive-row-desc">${safeDesc}${(it.description || "").length > 200 ? "…" : ""}</div>` : ""}
        ${albumAssignControl ? `<div class="album-assign-row">${albumAssignControl}</div>` : ""}
      </div>
      <div class="archive-row-actions">${saveBtn}${playBtn}${queueBtn}${suggestBtn}${linkBtn}</div>
    </div>
  `;
}

// Album-mode auto-match: pick the missing track whose title appears as
// a substring of the YouTube video title. Score by track-title length
// (longer = more specific). Returns null if no track matches.
function _albumAutoMatchTrack(videoTitle, tracks) {
  const v = String(videoTitle || "").toLowerCase().replace(/\s+/g, " ").trim();
  if (!v || !Array.isArray(tracks) || !tracks.length) return null;
  let best = null, bestScore = 0;
  for (const t of tracks) {
    const tt = String(t.title || "").toLowerCase().replace(/\s+/g, " ").trim();
    if (!tt || tt.length < 3) continue; // skip generic 1-2 char titles
    if (v.includes(tt)) {
      if (tt.length > bestScore) { best = t; bestScore = tt.length; }
    }
  }
  return best;
}

// ── Actions ───────────────────────────────────────────────────────────
function _youtubeRowDataFromBtn(btn) {
  const row = btn?.closest(".yt-row");
  if (!row) return null;
  return {
    videoId: row.dataset.vid,
    title:   row.dataset.title,
    channel: row.dataset.channel,
    thumb:   row.dataset.thumb,
  };
}

function _youtubePlayRow(btn) {
  const it = _youtubeRowDataFromBtn(btn);
  if (!it?.videoId) return;
  // openVideo's external-play hook will insert this at the head of
  // the queue (matching the universal "every play = top of queue"
  // rule). We synthesize an event-target so the click handler can
  // pull the data attrs the queue insert needs.
  const url = `https://www.youtube.com/watch?v=${encodeURIComponent(it.videoId)}`;
  // Stash data attributes on a synthetic anchor so openVideo's
  // event.target.closest(".track-link") path picks up the metadata
  // for the optimistic queue insert.
  const synth = document.createElement("a");
  synth.className = "track-link";
  synth.dataset.video = url;
  synth.dataset.track = it.title || "";
  synth.dataset.album = "";
  synth.dataset.artist = it.channel || "";
  // Use openVideo with a fake event referencing the synthetic anchor.
  if (typeof openVideo === "function") {
    openVideo({
      preventDefault() {},
      stopPropagation() {},
      target: synth,
    }, url);
  }
}

function _youtubeQueueRow(btn) {
  const it = _youtubeRowDataFromBtn(btn);
  if (!it?.videoId) return;
  if (typeof queueAddYt === "function") {
    // ＋ button → tail of queue. ▶ is the "play now" path and is
    // handled by _youtubePlayRow above.
    queueAddYt(it.videoId, {
      title:  it.title  || "",
      artist: it.channel || "",
      image:  it.thumb  || "",
    }, { mode: "append" });
  }
}

async function _youtubeToggleSave(btn) {
  const it = _youtubeRowDataFromBtn(btn);
  if (!it?.videoId) return;
  if (!window._clerk?.user) {
    if (typeof showToast === "function") showToast("Sign in to save videos", "error");
    return;
  }
  if (!_ytSavedIds) _ytSavedIds = new Set();
  const saving = !_ytSavedIds.has(it.videoId);
  // Optimistic toggle
  _ytSavedIds[saving ? "add" : "delete"](it.videoId);
  btn.classList.toggle("is-saved", saving);
  btn.textContent = saving ? "★" : "☆";
  btn.title = saving ? "Remove from Saved" : "Save";
  try {
    if (saving) {
      const r = await apiFetch("/api/user/youtube-saves", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          videoId:   it.videoId,
          title:     it.title || "",
          channel:   it.channel || "",
          thumbnail: it.thumb || "",
          data: {
            title:     it.title || "",
            channel:   it.channel || "",
            thumbnail: it.thumb || "",
          },
        }),
      });
      if (!r.ok) throw new Error(`save failed (${r.status})`);
      showToast?.("Saved");
    } else {
      const r = await apiFetch("/api/user/youtube-saves", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ videoId: it.videoId }),
      });
      if (!r.ok) throw new Error(`remove failed (${r.status})`);
      showToast?.("Removed from Saved");
      if (_ytSavedItems) _ytSavedItems = _ytSavedItems.filter(x => x.videoId !== it.videoId);
      if (_ytTab === "saved") _renderYoutubeSavedRows();
    }
    // Sync any other on-screen rows for this id (could appear in
    // both Search and Saved tabs simultaneously — they don't, since
    // they're mutually exclusive — but stay defensive).
    document.querySelectorAll(`#youtube-view .yt-row[data-vid="${CSS.escape(it.videoId)}"] .yt-save-btn`).forEach(el => {
      el.classList.toggle("is-saved", saving);
      el.textContent = saving ? "★" : "☆";
    });
  } catch (e) {
    _ytSavedIds[saving ? "delete" : "add"](it.videoId);
    btn.classList.toggle("is-saved", !saving);
    btn.textContent = !saving ? "★" : "☆";
    showToast?.(e?.message || "Action failed", "error");
  }
}

// ── YouTube popup (overlays album / version popups) ─────────────────
// Mirrors the wiki-popup pattern: a small overlay with search results
// that doesn't navigate away from the underlying modal. Lets users
// pick a video / play / queue / save without losing the album popup
// context. Standalone YouTube searches still go to /?v=youtube via
// the "Full page ↗" link in the popup header.

let _ytPopupQuery = "";

async function openYoutubePopup(query) {
  const q = String(query ?? "").trim();
  if (!q) return;
  _ytPopupQuery = q;
  const overlay = document.getElementById("youtube-popup-overlay");
  const titleEl = document.getElementById("youtube-popup-title");
  const statusEl = document.getElementById("youtube-popup-status");
  const resultsEl = document.getElementById("youtube-popup-results");
  if (!overlay) return;
  overlay.classList.add("open");
  // Album mode: show "Find missing tracks for ALBUM" + clear any
  // prior staged-assignments map. Per-track mode keeps the original
  // YouTube · "query" title.
  const albumCtx = window._sdSuggestAlbumContext;
  if (albumCtx) {
    if (titleEl) titleEl.textContent = `Find missing tracks · ${albumCtx.albumTitle || "album"}`;
    window._sdSuggestStaged = {}; // { trackPosition → { videoId, videoTitle, trackTitle } }
  } else {
    if (titleEl) titleEl.textContent = `YouTube · "${q}"`;
  }
  if (statusEl) statusEl.textContent = "Searching…";
  if (resultsEl) resultsEl.innerHTML = "";
  // Sync ★ state once per session.
  if (_ytSavedIds == null) _youtubeLoadSavedIds();
  try {
    // apiFetch attaches the Clerk Bearer so signed-in callers bypass
    // the anon-IP rate limit. Raw fetch would have the server treat
    // the request as anon and trip the per-IP throttle after 30/hr.
    const r = await apiFetch(`/api/youtube/search?q=${encodeURIComponent(q)}`, { cache: "no-store" });
    if (!r.ok) {
      const errBody = await r.text().catch(() => "");
      console.warn("[youtube popup] search failed:", r.status, errBody);
      if (statusEl) statusEl.textContent = `Search failed (${r.status}).`;
      return;
    }
    const j = await r.json();
    const items = Array.isArray(j?.items) ? j.items : [];
    if (statusEl) statusEl.textContent = items.length ? `${items.length} result${items.length === 1 ? "" : "s"}` : "No results.";
    if (resultsEl) resultsEl.innerHTML = items.map(it => _youtubeRowHtml(it)).join("");
    // Album mode: append the sticky status footer + submit button.
    if (albumCtx) _albumRenderFooter();
  } catch (e) {
    console.warn("[youtube popup] threw:", e);
    if (statusEl) statusEl.textContent = "Search failed.";
  }
}

function closeYoutubePopup() {
  const overlay = document.getElementById("youtube-popup-overlay");
  if (overlay) overlay.classList.remove("open");
  // Drop any pending track-suggest context so the next popup open
  // doesn't accidentally show ✓ Suggest buttons.
  window._sdSuggestForTrack = null;
  window._sdSuggestAlbumContext = null;
  window._sdSuggestStaged = null;
  // Remove album footer if present.
  document.getElementById("album-suggest-footer")?.remove();
}

// ── Album-mode handlers ──────────────────────────────────────────────

// Re-render the staged-assignments status + submit footer below the
// results list. Idempotent — safe to call repeatedly.
function _albumRenderFooter() {
  const ctx = window._sdSuggestAlbumContext;
  if (!ctx) return;
  const overlay = document.getElementById("youtube-popup-overlay");
  if (!overlay) return;
  let footer = document.getElementById("album-suggest-footer");
  if (!footer) {
    footer = document.createElement("div");
    footer.id = "album-suggest-footer";
    footer.className = "album-suggest-footer";
    overlay.querySelector(".youtube-popup-content")?.appendChild(footer)
      || overlay.appendChild(footer);
  }
  const staged = window._sdSuggestStaged || {};
  const stagedCount = Object.values(staged).filter(Boolean).length;
  const rows = (ctx.tracks || []).map(t => {
    const s = staged[t.position];
    const mark = s ? "✓" : "✗";
    const cls = s ? "album-suggest-track is-staged" : "album-suggest-track";
    const detail = s ? `<span class="album-suggest-staged-vid">${escHtml(s.videoTitle || s.videoId)}</span>` : "";
    return `<div class="${cls}"><span class="album-suggest-mark">${mark}</span> <span class="album-suggest-tnum">${escHtml(t.position)}.</span> <span class="album-suggest-ttitle">${escHtml(t.title)}</span> ${detail}</div>`;
  }).join("");
  footer.innerHTML = `
    <div class="album-suggest-status-list">${rows}</div>
    <div class="album-suggest-submit-row">
      <button type="button" class="archive-btn archive-btn-suggest album-suggest-submit-btn" ${stagedCount ? "" : "disabled"} onclick="_youtubeAlbumSubmit(this)">Submit ${stagedCount} assignment${stagedCount === 1 ? "" : "s"}</button>
      <button type="button" class="archive-btn" onclick="closeYoutubePopup()">Cancel</button>
    </div>
  `;
}

// Dropdown change handler — does nothing on its own; the user has to
// press "Stage" to commit. This way they can scan multiple videos
// and switch their mind without surprise side-effects.
function _youtubeAlbumAssignChanged(_select) {
  // No-op — staging happens via _youtubeAlbumStage.
}
window._youtubeAlbumAssignChanged = _youtubeAlbumAssignChanged;

// Stage button handler: read the dropdown's selected track + the
// row's video metadata, save to window._sdSuggestStaged, and re-render
// the footer + the row to reflect the staged state. Re-staging onto a
// different track is allowed (overwrites the previous assignment).
function _youtubeAlbumStage(btn) {
  const ctx = window._sdSuggestAlbumContext;
  if (!ctx) return;
  const row = btn.closest(".yt-row");
  const select = row?.querySelector(".album-assign-select");
  if (!row || !select) return;
  const videoId    = row.dataset.vid || "";
  const videoTitle = row.dataset.title || "";
  const pos        = select.value || "";
  const staged = window._sdSuggestStaged || (window._sdSuggestStaged = {});
  if (!pos) {
    // "— skip —" selected — if this video was previously staged for
    // any track, remove it.
    for (const k of Object.keys(staged)) {
      if (staged[k] && staged[k].videoId === videoId) delete staged[k];
    }
  } else {
    // First, drop any existing staging of THIS video for OTHER tracks
    // (a video can only fill one track at a time). Also drop the
    // current assignment for THIS track if it was a different video.
    for (const k of Object.keys(staged)) {
      if (staged[k] && staged[k].videoId === videoId && k !== pos) delete staged[k];
    }
    const trackTitle = (ctx.tracks || []).find(t => t.position === pos)?.title || "";
    staged[pos] = { videoId, videoTitle, trackTitle };
  }
  // Re-render so disabled-options + staged-marks update across rows.
  const resultsEl = document.getElementById("youtube-popup-results");
  if (resultsEl) {
    // We don't have the original items array here; re-render from the
    // current DOM. Each .yt-row carries enough data attrs to rebuild
    // a row item shape.
    const rows = Array.from(resultsEl.querySelectorAll(".yt-row")).map(el => ({
      videoId: el.dataset.vid,
      title:   el.dataset.title,
      channel: el.dataset.channel,
      thumbnail: el.dataset.thumb,
      description: el.querySelector(".archive-row-desc")?.textContent || "",
    }));
    resultsEl.innerHTML = rows.map(it => _youtubeRowHtml(it)).join("");
  }
  _albumRenderFooter();
}
window._youtubeAlbumStage = _youtubeAlbumStage;

// Submit all staged assignments as a batch POST. Closes the popup
// on success and triggers the album popup's override re-fetch so the
// newly-assigned tracks get their ▶ buttons immediately.
async function _youtubeAlbumSubmit(btn) {
  const ctx = window._sdSuggestAlbumContext;
  if (!ctx) return;
  const staged = window._sdSuggestStaged || {};
  const assignments = Object.entries(staged)
    .filter(([_, v]) => v && v.videoId)
    .map(([position, v]) => ({
      releaseId:     ctx.releaseId,
      releaseType:   ctx.releaseType,
      trackPosition: position,
      trackTitle:    v.trackTitle,
      videoId:       v.videoId,
      videoTitle:    v.videoTitle,
    }));
  if (!assignments.length) return;
  btn.disabled = true;
  const oldText = btn.textContent;
  btn.textContent = "Saving…";
  try {
    const r = await apiFetch("/api/track-yt/suggest-batch", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ assignments }),
    });
    if (!r.ok) throw new Error(`save failed (${r.status})`);
    const j = await r.json().catch(() => ({}));
    const ins = j?.inserted ?? 0;
    const skip = j?.skipped ?? 0;
    if (ins && skip) showToast?.(`Saved ${ins}, ${skip} already taken`);
    else if (ins)    showToast?.(`Saved ${ins} track${ins === 1 ? "" : "s"}.`);
    else             showToast?.(`All ${skip} tracks were already taken`, "info");
    // Snapshot before closing (closeYoutubePopup wipes the ctx).
    const targetId    = ctx.targetId || "album-info";
    const releaseType = ctx.releaseType;
    const releaseId   = ctx.releaseId;
    const masterId    = ctx.masterId || (releaseType === "master" ? releaseId : "");
    closeYoutubePopup();
    if (typeof window._trackYtKickFetchAndApply === "function") {
      const isMaster = releaseType === "master";
      window._trackYtKickFetchAndApply(
        targetId,
        masterId,
        isMaster ? "" : releaseId,
        isMaster
      ).catch(() => {});
    }
  } catch (e) {
    showToast?.(e?.message || "Could not save", "error");
    btn.disabled = false;
    btn.textContent = oldText;
  }
}
window._youtubeAlbumSubmit = _youtubeAlbumSubmit;

// Crowd-sourced override submit. Reads window._sdSuggestForTrack
// (stashed by modal.js when the row's 🎵 affordance was clicked) and
// the picked video's row data, POSTs the suggestion, then closes the
// popup and re-applies the album's tracklist DOM patch so the new
// override surfaces immediately.
async function _youtubeSuggestForTrack(btn) {
  const ctx = window._sdSuggestForTrack;
  if (!ctx) {
    if (typeof showToast === "function") showToast("Track context lost — re-open from the album", "error");
    return;
  }
  const it = _youtubeRowDataFromBtn(btn);
  if (!it?.videoId) return;
  // Optimistic UI: disable the button so a double-click doesn't fire
  // two POSTs (the second would be a no-op anyway thanks to ON CONFLICT
  // DO NOTHING, but the user-visible result would be confusing).
  btn.disabled = true;
  const oldText = btn.textContent;
  btn.textContent = "Saving…";
  try {
    const r = await apiFetch("/api/track-yt/suggest", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        releaseId:     ctx.releaseId,
        releaseType:   ctx.releaseType,
        trackPosition: ctx.trackPosition,
        trackTitle:    ctx.trackTitle,
        videoId:       it.videoId,
        videoTitle:    it.title || "",
      }),
    });
    if (!r.ok) throw new Error(`save failed (${r.status})`);
    const j = await r.json().catch(() => ({}));
    if (j?.inserted === false) {
      showToast?.("Already set for this track — nothing changed", "info");
    } else {
      showToast?.("Thanks! Suggestion saved.");
    }
    // Snapshot before we close — closeYoutubePopup() drops the ctx.
    const targetId    = ctx.targetId || "album-info";
    const releaseType = ctx.releaseType;
    const releaseId   = ctx.releaseId;
    const masterId    = ctx.masterId || (releaseType === "master" ? releaseId : "");
    closeYoutubePopup();
    // Bust the modal-side cache so the re-apply pulls the new row.
    if (typeof window._trackYtKickFetchAndApply === "function") {
      const isMaster = releaseType === "master";
      // Pass both ids when known so the refetch keeps master-scope
      // rows visible alongside the freshly inserted release-scope one.
      window._trackYtKickFetchAndApply(
        targetId,
        masterId,
        isMaster ? "" : releaseId,
        isMaster
      ).catch(() => {});
    }
  } catch (e) {
    showToast?.(e?.message || "Could not save suggestion", "error");
    btn.disabled = false;
    btn.textContent = oldText;
  }
}

// "Full page ↗" link click — close the popup, navigate to the
// standalone /?v=youtube view with the same query so the user can
// browse paginated results / use the Saved tab.
function _youtubePopupOpenFullPage() {
  const q = _ytPopupQuery;
  closeYoutubePopup();
  if (typeof switchView === "function") {
    try { switchView("youtube"); } catch {}
    setTimeout(() => {
      const qInput = document.getElementById("youtube-view-q");
      if (qInput) qInput.value = q;
      if (typeof runYoutubeSearch === "function") runYoutubeSearch(q);
    }, 30);
  } else if (q) {
    location.href = "/?v=youtube&yq=" + encodeURIComponent(q);
  }
}

// ── Globals ───────────────────────────────────────────────────────────
window.initYoutubeView           = initYoutubeView;
window.runYoutubeSearch          = runYoutubeSearch;
window._youtubeSwitchTab         = _youtubeSwitchTab;
window._youtubeLoadMore          = _youtubeLoadMore;
window._youtubePlayRow           = _youtubePlayRow;
window._youtubeQueueRow          = _youtubeQueueRow;
window._youtubeToggleSave        = _youtubeToggleSave;
window._youtubeOnSavedFilterInput = _youtubeOnSavedFilterInput;
window._youtubeOnSavedSortChange  = _youtubeOnSavedSortChange;
window.openYoutubePopup           = openYoutubePopup;
window.closeYoutubePopup          = closeYoutubePopup;
window._youtubePopupOpenFullPage  = _youtubePopupOpenFullPage;
window._youtubeSuggestForTrack    = _youtubeSuggestForTrack;
