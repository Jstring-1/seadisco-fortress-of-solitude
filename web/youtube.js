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
    const r = await fetch(`/api/youtube/search?${params.join("&")}`, { cache: "no-store" });
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
  return `
    <div class="yt-row archive-row" data-vid="${safeId}" data-title="${safeTitle}" data-channel="${safeChannel}" data-thumb="${escHtml(thumb)}">
      <img class="yt-row-thumb" src="${escHtml(thumb)}" alt="" loading="lazy">
      <div class="archive-row-main">
        <div class="archive-row-title">${safeTitle}</div>
        ${safeChannel ? `<div class="archive-row-date">${safeChannel}</div>` : ""}
        ${safeDesc ? `<div class="archive-row-desc">${safeDesc}${(it.description || "").length > 200 ? "…" : ""}</div>` : ""}
      </div>
      <div class="archive-row-actions">${saveBtn}${playBtn}${queueBtn}${linkBtn}</div>
    </div>
  `;
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
    queueAddYt(it.videoId, {
      title:  it.title  || "",
      artist: it.channel || "",
      image:  it.thumb  || "",
    });
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
