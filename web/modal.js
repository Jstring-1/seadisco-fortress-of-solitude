// ── Modal ─────────────────────────────────────────────────────────────────
// Guard SPA-only functions so modal.js works on account/admin pages too
if (typeof selectAltArtist === "undefined") window.selectAltArtist = () => {};
if (typeof searchArtistFromModal === "undefined") window.searchArtistFromModal = () => {};
if (typeof toggleAdvanced === "undefined") window.toggleAdvanced = () => {};
if (typeof doSearch === "undefined") window.doSearch = () => {};
if (typeof switchView === "undefined") window.switchView = () => {};
if (typeof doCwSearch === "undefined") window.doCwSearch = () => {};
if (typeof toggleCwAdvanced === "undefined") window.toggleCwAdvanced = () => {};

// Search user's collection from modal — navigates to My Records with a filter
// Uses a pending search mechanism because switchView("records") calls
// switchRecordsTab which calls clearCwFilters, wiping any pre-set values.
function searchCollectionFor(field, value) {
  closeModal();
  // Store pending search — switchRecordsTab will pick it up after clearing
  window._pendingCwSearch = { field, value };
  // Switch to records view (this clears filters then loads collection)
  if (typeof switchView === "function") switchView("records");
}
function openModal(event, id, type, discogsUrl) {
  if (event) event.preventDefault();
  const u = new URL(window.location.href);
  u.searchParams.set("op", `${type}:${id}`);
  history.replaceState({}, "", u.toString());
  const overlay = document.getElementById("modal-overlay");
  document.getElementById("album-info").innerHTML = "";
  document.getElementById("modal-loading").style.display = "block";
  overlay.classList.add("open");
  document.body.classList.add("modal-open");

  const cachedItem = (typeof itemCache !== 'undefined' ? itemCache.get(String(id)) : null) ?? { type, id };
  const endpoint = type === "master" ? "master" : "release";
  Promise.all([
    apiFetch(`${API}/${endpoint}/${id}`).then(r => r.json()),
    apiFetch(`${API}/marketplace-stats/${id}?type=${type}`).then(r => r.json()).catch(() => null),
  ])
    .then(([d, stats]) => {
      document.getElementById("modal-loading").style.display = "none";
      renderAlbumInfo(d, cachedItem, discogsUrl, stats);
    })
    .catch(() => {
      document.getElementById("modal-loading").textContent = "Failed to load.";
    });
}

function closeModal() {
  document.getElementById("modal-overlay").classList.remove("open");
  if (!document.getElementById("version-overlay")?.classList.contains("open") &&
      !document.getElementById("series-overlay")?.classList.contains("open")) {
    document.body.classList.remove("modal-open");
  }
  const u = new URL(window.location.href);
  u.searchParams.delete("op");
  history.replaceState({}, "", u.toString());
}

// ── Tracklist collapse/expand ──────────────────────────────────────────────
function toggleTracklist(headingEl) {
  const body = headingEl.closest(".tracklist-header").nextElementSibling;
  if (!body) return;
  const isOpen = body.style.display !== "none";
  body.style.display = isOpen ? "none" : "";
  const arrow = headingEl.querySelector(".tracklist-arrow");
  if (arrow) arrow.textContent = isOpen ? "▶" : "▼";
  localStorage.setItem("tracklist-open", isOpen ? "false" : "true");
}

function filterTracks(input) {
  const q = input.value.toLowerCase().trim();
  const body = input.closest(".tracklist-header").nextElementSibling;
  if (!body) return;
  // Ensure tracklist is expanded when filtering
  if (q && body.style.display === "none") {
    body.style.display = "";
    const arrow = input.closest(".tracklist-header").querySelector(".tracklist-arrow");
    if (arrow) arrow.textContent = "▼";
  }
  body.querySelectorAll(".track").forEach(row => {
    const title = row.querySelector(".track-title")?.textContent.toLowerCase() ?? "";
    row.style.display = !q || title.includes(q) ? "" : "none";
  });
}

function filterCredits(input) {
  const q = input.value.toLowerCase().trim();
  const body = input.closest(".credits-header").nextElementSibling;
  if (!body) return;
  body.querySelectorAll(".credit-item").forEach(item => {
    const text = item.textContent.toLowerCase();
    const show = !q || text.includes(q);
    item.style.display = show ? "" : "none";
    // Hide the separator after hidden items
    const sep = item.nextElementSibling;
    if (sep?.classList.contains("credit-sep")) sep.style.display = show ? "" : "none";
  });
  // Clean up leading separators (first visible item shouldn't have a sep before it)
  const items = body.querySelectorAll(".credit-item, .credit-sep");
  let needSep = false;
  items.forEach(el => {
    if (el.classList.contains("credit-item")) {
      if (el.style.display !== "none") needSep = true;
    } else if (el.classList.contains("credit-sep")) {
      // Show sep only between two visible items
      const next = el.nextElementSibling;
      el.style.display = (needSep && next?.classList.contains("credit-item") && next.style.display !== "none") ? "" : "none";
    }
  });
}

// ── Concert popup ─────────────────────────────────────────────────────────
async function openConcertPopup(event, artistName) {
  if (event) event.preventDefault();
  const overlay   = document.getElementById("concert-overlay");
  const infoEl    = document.getElementById("concert-info");
  const loadingEl = document.getElementById("concert-loading");
  infoEl.innerHTML = "";
  loadingEl.textContent = "Loading concerts…";
  loadingEl.style.display = "block";
  overlay.classList.add("open");

  const u = new URL(window.location.href);
  u.searchParams.set("ct", artistName);
  history.pushState({}, "", u.toString());

  try {
    const data = await fetch(`/api/concerts/${encodeURIComponent(artistName)}`).then(r => r.json());
    loadingEl.style.display = "none";
    const events = data.events ?? [];

    if (!events.length) {
      infoEl.innerHTML = `<div class="concert-empty">No concert info found for ${escHtml(artistName)}</div>`;
      return;
    }

    const fmtDate = (d) => {
      if (!d) return "";
      try { return new Date(d + "T12:00:00").toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric", year: "numeric" }); }
      catch { return d; }
    };
    const fmtTime = (t) => {
      if (!t) return "";
      try {
        const [h, m] = t.split(":");
        const hr = parseInt(h);
        return `${hr > 12 ? hr - 12 : hr}:${m} ${hr >= 12 ? "PM" : "AM"}`;
      } catch { return t; }
    };

    const escArt = escHtml(artistName).replace(/'/g, "\\'");
    const liveLink = typeof switchView === 'function'
      ? ` <a href="#" onclick="event.preventDefault();closeConcertPopup();closeModal();document.getElementById('live-artist').value='${escArt}';switchView('live');doLiveSearch()" title="Search for live events on the Live tab" style="font-size:0.75rem;color:var(--accent);text-decoration:none;margin-left:0.5rem">Search on Live →</a>`
      : '';
    let html = `<div class="concert-artist-name">${escHtml(artistName)} — Upcoming Shows${liveLink}</div>`;
    html += `<div class="concert-list">`;
    for (const ev of events) {
      const googleQ = encodeURIComponent(`${artistName} ${ev.venue} ${ev.city} concert`);
      const googleUrl = `https://www.google.com/search?q=${googleQ}`;
      const location = [ev.city, ev.region, ev.country].filter(Boolean).join(", ");
      html += `<div class="concert-item">
        <div class="concert-date">
          ${escHtml(fmtDate(ev.date))}
          ${ev.time ? `<span class="concert-time">${escHtml(fmtTime(ev.time))}</span>` : ""}
        </div>
        <div class="concert-details">
          <div class="concert-event-name">${escHtml(ev.name)}</div>
          <div class="concert-venue">
            <a href="${googleUrl}" target="_blank" rel="noopener" title="Search Google for this venue">${escHtml(ev.venue)}</a>
            ${location ? ` — ${escHtml(location)}` : ""}
          </div>
          <span class="concert-source">${escHtml(ev.source)}</span>
        </div>
      </div>`;
    }
    html += `</div>`;
    infoEl.innerHTML = html;
  } catch (err) {
    loadingEl.style.display = "none";
    infoEl.innerHTML = `<div class="concert-empty">Failed to load concert info.</div>`;
  }
}

function closeConcertPopup() {
  document.getElementById("concert-overlay").classList.remove("open");
  const u = new URL(window.location.href);
  u.searchParams.delete("ct");
  history.replaceState({}, "", u.toString());
}

document.getElementById("concert-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("concert-overlay")) closeConcertPopup();
});

// ── Image lightbox / carousel ─────────────────────────────────────────────
let _lbImages = [], _lbIdx = 0;

function openLightbox(images, startIdx) {
  _lbImages = images;
  _lbIdx = startIdx ?? 0;
  _renderLightbox();
  document.getElementById("lightbox-overlay").classList.add("open");
  document.addEventListener("keydown", _lbKey);
}

function closeLightbox() {
  document.getElementById("lightbox-overlay").classList.remove("open");
  document.removeEventListener("keydown", _lbKey);
}

function lightboxStep(e, dir) {
  e.stopPropagation();
  _lbIdx = Math.max(0, Math.min(_lbImages.length - 1, _lbIdx + dir));
  _renderLightbox();
}

function _lbKey(e) {
  if (e.key === "ArrowLeft")  { _lbIdx = Math.max(0, _lbIdx - 1); _renderLightbox(); }
  if (e.key === "ArrowRight") { _lbIdx = Math.min(_lbImages.length - 1, _lbIdx + 1); _renderLightbox(); }
  if (e.key === "Escape")     closeLightbox();
}

function _renderLightbox() {
  document.getElementById("lightbox-img").src = _lbImages[_lbIdx] ?? "";
  document.getElementById("lightbox-counter").textContent =
    _lbImages.length > 1 ? `${_lbIdx + 1} / ${_lbImages.length}` : "";
  document.getElementById("lightbox-prev").disabled = _lbIdx === 0;
  document.getElementById("lightbox-next").disabled = _lbIdx === _lbImages.length - 1;
  const single = _lbImages.length <= 1;
  document.getElementById("lightbox-prev").style.display = single ? "none" : "";
  document.getElementById("lightbox-next").style.display = single ? "none" : "";
}

document.getElementById("modal-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("modal-overlay")) closeModal();
});

// ── Version popup ─────────────────────────────────────────────────────────
async function openVersionPopup(event, releaseId) {
  if (event) event.preventDefault();
  const overlay = document.getElementById("version-overlay");
  const info    = document.getElementById("version-info");
  const loading = document.getElementById("version-loading");
  info.innerHTML = "";
  loading.style.display = "block";
  overlay.classList.add("open");
  document.body.classList.add("modal-open");
  const u = new URL(window.location.href);
  u.searchParams.set("vr", releaseId);
  history.replaceState({}, "", u.toString());
  try {
    const discogsUrl = `https://www.discogs.com/release/${releaseId}`;
    const [d, stats] = await Promise.all([
      apiFetch(`${API}/release/${releaseId}`).then(r => r.json()),
      apiFetch(`${API}/marketplace-stats/${releaseId}?type=release`).then(r => r.json()).catch(() => null),
    ]);
    loading.style.display = "none";
    const fakeResult = { type: "release", id: releaseId, title: d.title ?? "", cover_image: d.images?.[0]?.uri ?? "", format: [], country: d.country ?? "", year: d.year ?? "" };
    renderAlbumInfo(d, fakeResult, discogsUrl, stats, "version-info");
  } catch(e) {
    loading.style.display = "none";
    info.innerHTML = `<div style="padding:1rem;color:var(--muted)">Failed to load release details.</div>`;
  }
}

function closeVersionPopup() {
  document.getElementById("version-overlay").classList.remove("open");
  if (!document.getElementById("modal-overlay")?.classList.contains("open") &&
      !document.getElementById("series-overlay")?.classList.contains("open")) {
    document.body.classList.remove("modal-open");
  }
  const u = new URL(window.location.href);
  u.searchParams.delete("vr");
  history.replaceState({}, "", u.toString());
}

document.getElementById("version-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("version-overlay")) closeVersionPopup();
});

// ── Bio full popup ────────────────────────────────────────────────────────
function openBioFull(event) {
  if (event) event.preventDefault();
  const u = new URL(window.location.href);
  u.searchParams.set("bi", "1");
  history.replaceState({}, "", u.toString());
  const {
    name, text, discogsId = null,
    members = [], groups = [], aliases = [],
    namevariations = [], urls = [],
    parentLabel = null, sublabels = [],
    alternatives = [],
  } = window._currentBio ?? {};
  document.getElementById("bio-full-name").textContent = name ?? "";
  let html = renderBioMarkup(text ?? "");
  if (alternatives.length > 0) {
    const altLinks = alternatives.map(a =>
      `<a href="#" class="bio-artist-link modal-internal-link" onclick="selectAltArtist(event,this);closeBioFull()" data-alt-name="${escHtml(a.name)}"${a.id ? ` data-alt-id="${a.id}"` : ""} title="Search for ${escHtml(a.name)}" style="color:var(--accent)">${escHtml(a.name)}</a>`
    ).join('<span style="color:#555;margin:0 0.3em">·</span>');
    html += `<div style="font-size:0.78rem;margin-top:0.7rem;line-height:1.6"><span style="color:#777;margin-right:0.4em">Also:</span>${altLinks}</div>`;
  }
  const relLinks = renderArtistRelations(members, groups, aliases, namevariations, urls, parentLabel, sublabels);
  if (relLinks) html += relLinks;
  if (discogsId) {
    html += `<div style="margin-top:1.1rem"><a href="https://www.discogs.com/artist/${discogsId}" target="_blank" rel="noopener" title="Open artist page on Discogs.com" style="font-size:0.75rem;color:#666;text-decoration:none">View profile on Discogs ↗</a></div>`;
  }
  document.getElementById("bio-full-text").innerHTML = html;
  document.getElementById("bio-full-overlay").classList.add("open");
}

function closeBioFull() {
  document.getElementById("bio-full-overlay").classList.remove("open");
  const u = new URL(window.location.href);
  u.searchParams.delete("bi");
  history.replaceState({}, "", u.toString());
}

document.getElementById("bio-full-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("bio-full-overlay")) closeBioFull();
});

// ── Video popup ────────────────────────────────────────────────────────────
let ytPlayer = null;
let _ytLoading = false;
let _ytSession = 0;        // incremented on each openVideo, checked by async callbacks
let _ytPollId  = null;     // polling interval for YT API readiness
// Repeat modes: "off" → "album" → "one" → "off"
let _ytRepeat = "off";

function toggleRepeat() {
  _ytRepeat = _ytRepeat === "off" ? "album" : _ytRepeat === "album" ? "one" : "off";
  const labels  = { off: "Repeat: off", album: "Repeat: album", one: "Repeat: one" };
  const colors  = { off: "#666", album: "#4caf50", one: "var(--accent)" };
  // Mini bar icons: off = plain arrow, album = circled arrows, one = circled 1
  const miniIcons = { off: "↻", album: "⟳ ALL", one: "⟳ 1" };
  // Expanded nav labels
  const navLabels = { off: "↻ repeat", album: "⟳ repeat all", one: "⟳ repeat 1" };
  document.querySelectorAll(".repeat-btn").forEach(btn => {
    btn.style.color = colors[_ytRepeat];
    btn.title = labels[_ytRepeat];
    if (btn.closest("#video-nav")) btn.innerHTML = navLabels[_ytRepeat];
    else btn.innerHTML = miniIcons[_ytRepeat];
  });
}
window.onYouTubeIframeAPIReady = function() { window._ytAPIReady = true; };

// Highlight the currently playing track in any open popup tracklist
function highlightPlayingTrack() {
  document.querySelectorAll(".track-link.now-playing").forEach(el => el.classList.remove("now-playing"));
  const currentUrl = (window._videoQueue ?? [])[window._videoQueueIndex ?? -1];
  if (!currentUrl) return;
  // Highlight first match per popup container to avoid duplicates when
  // multiple tracks share the same YouTube URL (e.g. full-album videos)
  const seen = new Set();
  document.querySelectorAll(".track-link[data-video]").forEach(el => {
    if (el.dataset.video !== currentUrl) return;
    const container = el.closest("#album-info, #version-info") || document;
    const key = container.id || "_root";
    if (seen.has(key)) return;
    seen.add(key);
    el.classList.add("now-playing");
  });
}

function ensureYTAPI() {
  if (window._ytAPIReady || _ytLoading) return;
  _ytLoading = true;
  const s = document.createElement("script");
  s.src = "https://www.youtube.com/iframe_api";
  document.head.appendChild(s);
}

function setVideoUrl(id) {
  const u = new URL(window.location.href);
  u.searchParams.set("vd", id);
  // Store the video's source release so page reload can reopen the right popup
  const op = u.searchParams.get("op");
  if (op) u.searchParams.set("vp", op);
  history.replaceState({}, "", u.toString());
}

let _ytLoadTimer = null;
let _ytHasPlayed = false;  // true once the current video reaches "playing" state
let _ytVideoToken = 0;     // increments per video load — guards against stale per-video callbacks
function loadYTVideo(id) {
  _ytHasPlayed = false;
  window._ytRetried = false;  // reset retry flag for new video
  _ytVideoToken++;
  const vtoken = _ytVideoToken;
  updatePlayerStatus("loading");
  // Timeout: if still "loading" after 8s, mark unavailable and skip
  if (_ytLoadTimer) clearTimeout(_ytLoadTimer);
  _ytLoadTimer = setTimeout(() => {
    if (vtoken !== _ytVideoToken) return;  // a different video was loaded since
    const statusEl = document.getElementById("mini-player-status");
    if (statusEl && (statusEl.textContent === "loading…" || statusEl.textContent === "buffering…")) {
      updatePlayerStatus("unavailable");
      setTimeout(() => { if (vtoken === _ytVideoToken) playNextVideo(); }, 1500);
    }
  }, 8000);
  if (ytPlayer && typeof ytPlayer.loadVideoById === "function") {
    ytPlayer.loadVideoById(id);
    return;
  }
  // Wait for YT API so we always get onStateChange events (needed for repeat/auto-advance)
  if (window._ytAPIReady && typeof YT !== "undefined") {
    _createYTPlayer(id);
  } else {
    // Poll briefly for API readiness, then create player
    const pollSession = _ytSession;
    let attempts = 0;
    if (_ytPollId) clearInterval(_ytPollId);
    _ytPollId = setInterval(() => {
      attempts++;
      if (pollSession !== _ytSession) { clearInterval(_ytPollId); _ytPollId = null; return; }
      if (window._ytAPIReady && typeof YT !== "undefined") {
        clearInterval(_ytPollId); _ytPollId = null;
        _createYTPlayer(id);
      } else if (attempts > 40) {
        // Fallback after ~4s if API never loads
        clearInterval(_ytPollId); _ytPollId = null;
        document.getElementById("video-player").innerHTML =
          `<iframe src="https://www.youtube.com/embed/${id}?autoplay=1" style="width:100%;height:100%;border:none" allow="autoplay;encrypted-media" allowfullscreen></iframe>`;
      }
    }, 100);
  }
}

function updatePlayerStatus(state, errorCode) {
  // Clear load timeout once we get a definitive state
  if (state !== "loading" && state !== "buffering" && _ytLoadTimer) { clearTimeout(_ytLoadTimer); _ytLoadTimer = null; }
  const el = document.getElementById("mini-player-status");
  if (!el) return;
  const map = {
    loading:     { text: "loading…",    cls: "status-loading" },
    buffering:   { text: "buffering…",  cls: "status-loading" },
    playing:     { text: "▶ playing",   cls: "status-playing" },
    paused:      { text: "⏸ paused",   cls: "status-paused"  },
    ended:       { text: "ended",       cls: "status-ended"   },
    unavailable: { text: "⚠ unavailable", cls: "status-error" },
    error:       { text: "⚠ error",     cls: "status-error"   },
  };
  const info = map[state] ?? { text: "", cls: "" };
  el.textContent = info.text;
  el.className = "mini-player-status " + info.cls;
}

function _createYTPlayer(id) {
  const session = _ytSession;
  let vtoken = _ytVideoToken;
  document.getElementById("video-player").innerHTML = "";
  ytPlayer = new YT.Player("video-player", {
    height: "100%", width: "100%", videoId: id,
    playerVars: { autoplay: 1, rel: 0 },
    events: {
      onStateChange: function(e) {
        if (session !== _ytSession) return;   // player was destroyed/recreated
        // YT.PlayerState: -1=unstarted, 0=ended, 1=playing, 2=paused, 3=buffering, 5=cued
        if (e.data === 1) { _ytHasPlayed = true; updatePlayerStatus("playing"); window._ytRetried = false; }
        else if (e.data === 2) updatePlayerStatus("paused");
        else if (e.data === 3) updatePlayerStatus("buffering");
        else if (e.data === 0) {
          // Guard against late "ended" events from a previous video on a reused player
          if (vtoken !== _ytVideoToken) return;
          updatePlayerStatus("ended"); onVideoEnded();
        }
        else if (e.data === 5) updatePlayerStatus("loading");
        // Keep vtoken in sync when a new video loads on same player
        if (e.data === -1 || e.data === 5) vtoken = _ytVideoToken;
      },
      onError: function(e) {
        if (session !== _ytSession) return;   // player was destroyed/recreated
        // Guard against errors from a previous video on a reused player
        if (vtoken !== _ytVideoToken) return;
        // Ignore errors if the video was already playing (late rights checks, transient issues)
        if (_ytHasPlayed) return;
        // Error codes: 2=invalid id, 5=HTML5 error, 100=not found, 101/150=embedding disabled
        const code = e?.data;
        if (code === 100 || code === 101 || code === 150) {
          updatePlayerStatus("unavailable");
          setTimeout(() => { if (vtoken === _ytVideoToken) playNextVideo(); }, 2000);
        } else if (code === 5 && !window._ytRetried) {
          // HTML5 error can be transient — retry once
          window._ytRetried = true;
          const retryId = id;  // capture current video id for retry
          const retryToken = vtoken;
          updatePlayerStatus("buffering");
          setTimeout(() => {
            if (retryToken !== _ytVideoToken) return;  // user moved on
            if (ytPlayer && typeof ytPlayer.loadVideoById === "function") ytPlayer.loadVideoById(retryId);
          }, 1000);
        } else {
          window._ytRetried = false;
          updatePlayerStatus("error");
          setTimeout(() => { if (vtoken === _ytVideoToken) playNextVideo(); }, 2000);
        }
      },
      onReady: function() {
        // onReady fires when iframe is ready, not when video plays — don't set "playing" yet
        if (session === _ytSession) updatePlayerStatus("loading");
      }
    }
  });
}

function updateVideoNavButtons() {
  const queue = window._videoQueue ?? [];
  const idx   = window._videoQueueIndex ?? 0;
  // Update both expanded nav and mini bar buttons
  const prevBtn  = document.getElementById("video-prev");
  const nextBtn  = document.getElementById("video-next");
  const miniPrev = document.getElementById("mini-prev");
  const miniNext = document.getElementById("mini-next");
  const titleEl  = document.getElementById("mini-player-title");
  if (prevBtn)  prevBtn.disabled  = idx <= 0;
  if (nextBtn)  nextBtn.disabled  = idx >= queue.length - 1;
  if (miniPrev) miniPrev.disabled = idx <= 0;
  if (miniNext) miniNext.disabled = idx >= queue.length - 1;
  if (titleEl) {
    const meta = (window._videoQueueMeta ?? [])[idx];
    if (meta) {
      const parts = [];
      if (meta.track)  parts.push(`<span class="vt-track">${escHtml(meta.track)}</span>`);
      if (meta.album)  parts.push(`<span>${escHtml(meta.album)}</span>`);
      if (meta.artist) parts.push(`<span>${escHtml(meta.artist)}</span>`);
      titleEl.innerHTML = parts.join(`<span class="vt-sep">·</span>`);
    } else {
      titleEl.innerHTML = "Playing";
    }
  }
  // Show/hide album + share buttons based on whether we have a release to reopen
  const albumBtn = document.getElementById("mini-album");
  if (albumBtn) albumBtn.style.display = window._playerReleaseId ? "" : "none";
  const shareBtn = document.getElementById("mini-share");
  if (shareBtn) shareBtn.style.display = window._playerReleaseId ? "" : "none";
  highlightPlayingTrack();
}

function toggleMiniPlayer() {
  const mp = document.getElementById("mini-player");
  if (mp) mp.classList.toggle("expanded");
}

function openVideo(event, url) {
  if (event) { event.preventDefault(); event.stopPropagation(); }
  ensureYTAPI();
  const id = extractYouTubeId(url);
  if (!id) { window.open(url, "_blank", "noopener"); return; }
  // Scope queue to the popup container the clicked track belongs to,
  // so we don't mix tracks from different albums
  const clickedEl = event?.target?.closest?.(".track-link") || event?.target;
  // Scope to the popup the track was clicked in; if called programmatically (no event),
  // prefer the version popup if open, then main modal, then fall back to document
  const container = clickedEl?.closest?.("#album-info, #version-info")
    || (document.getElementById("version-overlay")?.classList.contains("open") ? document.getElementById("version-info") : null)
    || (document.getElementById("modal-overlay")?.classList.contains("open") ? document.getElementById("album-info") : null)
    || document;
  const trackLinks = [...container.querySelectorAll(".track-link[data-video]")];
  window._videoQueue      = trackLinks.map(a => a.dataset.video);
  window._videoQueueMeta  = trackLinks.map(a => ({
    track:  a.dataset.track  || "",
    album:  a.dataset.album  || "",
    artist: a.dataset.artist || "",
  }));
  // Use the clicked element's position in the list (not indexOf, which fails with duplicate URLs)
  const clickedTrack = event?.target?.closest?.(".track-link");
  const clickedIdx = clickedTrack ? trackLinks.indexOf(clickedTrack) : -1;
  window._videoQueueIndex = clickedIdx >= 0 ? clickedIdx : window._videoQueue.indexOf(url);
  if (window._videoQueueIndex === -1) window._videoQueueIndex = 0;
  // Save the currently open release so the player bar can reopen it
  const opParam = new URLSearchParams(location.search).get("op");
  if (opParam && opParam.includes(":")) {
    const [pType, pId] = [opParam.slice(0, opParam.indexOf(":")), opParam.slice(opParam.indexOf(":") + 1)];
    window._playerReleaseType = pType;
    window._playerReleaseId   = pId;
    window._playerReleaseUrl  = `https://www.discogs.com/${pType}/${pId}`;
  }
  setVideoUrl(id);
  const mp = document.getElementById("mini-player");
  mp.classList.add("open");
  document.body.classList.add("player-open");
  loadYTVideo(id);
  updateVideoNavButtons();
}

function videoPrev() {
  const queue = window._videoQueue ?? [];
  let prev = (window._videoQueueIndex ?? 0) - 1;
  while (prev >= 0) {
    const id = extractYouTubeId(queue[prev]);
    if (id) {
      window._videoQueueIndex = prev;
      setVideoUrl(id);
      loadYTVideo(id);
      updateVideoNavButtons();
      return;
    }
    prev--;
  }
}

function playNextVideo() {
  const queue = window._videoQueue ?? [];
  let next = (window._videoQueueIndex ?? -1) + 1;
  while (next < queue.length) {
    const id = extractYouTubeId(queue[next]);
    if (id) {
      window._videoQueueIndex = next;
      setVideoUrl(id);
      loadYTVideo(id);
      updateVideoNavButtons();
      return;
    }
    next++;
  }
  // No more tracks in queue
  updatePlayerStatus("ended");
  updateVideoNavButtons();
}

function onVideoEnded() {
  if (_ytRepeat === "one" && ytPlayer) {
    ytPlayer.seekTo(0);
    ytPlayer.playVideo();
    return;
  }
  if (_ytRepeat === "album") {
    const queue = window._videoQueue ?? [];
    let next = (window._videoQueueIndex ?? -1) + 1;
    // Try next track; if at end, loop back to first
    while (next < queue.length) {
      const id = extractYouTubeId(queue[next]);
      if (id) { window._videoQueueIndex = next; setVideoUrl(id); loadYTVideo(id); updateVideoNavButtons(); return; }
      next++;
    }
    // Wrap to beginning
    for (let i = 0; i < queue.length; i++) {
      const id = extractYouTubeId(queue[i]);
      if (id) { window._videoQueueIndex = i; setVideoUrl(id); loadYTVideo(id); updateVideoNavButtons(); return; }
    }
    return;
  }
  playNextVideo();
}

function openPlayerRelease() {
  const rType = window._playerReleaseType;
  const rId   = window._playerReleaseId;
  const rUrl  = window._playerReleaseUrl;
  if (rType && rId) {
    openModal(null, rId, rType, rUrl);
  }
}

function sharePlayerUrl() {
  const u = new URL(window.location.origin);
  const cur = new URLSearchParams(location.search);
  // Include open popup if one is showing
  const op = cur.get("op");
  if (op) u.searchParams.set("op", op);
  const vd = cur.get("vd");
  const vp = cur.get("vp");
  if (vd) u.searchParams.set("vd", vd);
  if (vp) u.searchParams.set("vp", vp);
  navigator.clipboard.writeText(u.toString())
    .then(() => showToast("Share link copied"))
    .catch(() => showToast("Could not copy link", "error"));
}

function closeVideo() {
  _ytSession++;                             // invalidate any pending callbacks
  if (_ytPollId) { clearInterval(_ytPollId); _ytPollId = null; }
  if (_ytLoadTimer) { clearTimeout(_ytLoadTimer); _ytLoadTimer = null; }
  const mp = document.getElementById("mini-player");
  mp.classList.remove("open", "expanded");
  document.body.classList.remove("player-open");
  if (ytPlayer && typeof ytPlayer.stopVideo === "function") {
    ytPlayer.stopVideo();
    ytPlayer.destroy();
    ytPlayer = null;
  }
  document.getElementById("video-player").innerHTML = "";
  const titleEl = document.getElementById("mini-player-title");
  if (titleEl) titleEl.textContent = "Not playing";
  updatePlayerStatus("");
  const u = new URL(window.location.href);
  u.searchParams.delete("vd");
  u.searchParams.delete("vp");
  history.replaceState({}, "", u.toString());
  // Hide album + share buttons
  const albumBtn = document.getElementById("mini-album");
  if (albumBtn) albumBtn.style.display = "none";
  const shareBtn = document.getElementById("mini-share");
  if (shareBtn) shareBtn.style.display = "none";
  // Clear playing track highlights
  document.querySelectorAll(".track-link.now-playing").forEach(el => el.classList.remove("now-playing"));
  window._playerReleaseType = null;
  window._playerReleaseId = null;
  window._playerReleaseUrl = null;
  window._videoQueue = [];
  window._videoQueueMeta = [];
  window._videoQueueIndex = -1;
}

function extractYouTubeId(url) {
  try {
    const u = new URL(url);
    if (u.hostname === "youtu.be") return u.pathname.slice(1);
    return u.searchParams.get("v");
  } catch { return null; }
}

// video-overlay was replaced by mini-player — no click-to-close needed

document.addEventListener("keydown", e => {
  if (e.key === "Escape") {
    closeConcertPopup();
    closeVideo();
    closeModal();
    closeBioFull();
  }
});

// ── Album info panel ──────────────────────────────────────────────────────
function renderAlbumInfo(d, searchResult, discogsUrl = "", stats = null, targetId = "album-info") {
  const el = document.getElementById(targetId);

  const rawTitle = d.title ?? searchResult.title ?? "";
  const title    = rawTitle.includes(" - ") && !d.title
                   ? rawTitle.slice(rawTitle.indexOf(" - ") + 3) : rawTitle;
  const artistNames = (d.artists ?? []).map(a => a.name);
  const artists  = artistNames.length ? artistNames
                   : ((searchResult.title ?? "").split(" - ")[0] ? [(searchResult.title ?? "").split(" - ")[0]] : []);
  const year     = d.year ?? searchResult.year ?? "";
  const labelNames = (d.labels ?? []).map(l => l.name).slice(0, 2);
  if (!labelNames.length) labelNames.push(...(searchResult.label ?? []).slice(0, 2));
  const labels   = labelNames.join(", ");
  const genres   = [...(d.genres ?? []), ...(d.styles ?? [])].slice(0, 4).join(" · ");
  const country  = d.country ?? searchResult.country ?? "";
  const allImages = (d.images ?? []).map(i => i.uri).filter(Boolean);
  if (allImages.length === 0 && searchResult.cover_image) allImages.push(searchResult.cover_image);
  const img      = allImages[0] ?? "";
  const released    = d.released_formatted ?? d.released ?? "";
  const formats     = (d.formats ?? []).map(f =>
    [f.name, ...(f.descriptions ?? [])].filter(Boolean).join(" · ")
  ).join("; ") || (searchResult.format ?? []).join(" · ");
  const creditItems = (d.extraartists ?? [])
    .map(a => {
      const nameEl = a.id
        ? `<a href="#" class="modal-internal-link credit-name" data-alt-name="${escHtml(a.name)}" data-alt-id="${a.id}" onclick="selectAltArtist(event,this);closeModal()" title="Search for ${escHtml(a.name)}">${escHtml(a.name)}</a>`
        : `<span class="credit-name">${escHtml(a.name)}</span>`;
      const searchIcon = ` <a href="#" class="album-title-search" onclick="event.preventDefault();searchCollectionFor('cw-artist','${escHtml(a.name.replace(/'/g, "\\'"))}')" title="Search your collection for ${escHtml(a.name)}" style="font-size:1.1em">⌕</a>`;
      return `<span class="credit-item">${nameEl}${searchIcon}${a.role ? ` <span class="credit-role">(${escHtml(a.role)})</span>` : ""}</span>`;
    });
  const notes       = d.notes ? stripDiscogsMarkup(d.notes) : "";
  const catno     = (d.labels ?? [])[0]?.catno ?? "";
  const releaseId = d.id ?? searchResult.id ?? "";
  const typeName = targetId === "version-info"
    ? "Version"
    : searchResult.type === "master" ? "Master" : searchResult.type === "release" ? "Release" : "";
  const typeLabel = typeName && releaseId ? `${typeName}: ${releaseId}` : typeName;

  // Store rich card-shaped data for favorites (popup may not have itemCache entry)
  const entityType = targetId === "version-info" ? "release" : (searchResult.type || "release");
  if (!window._popupCardData) window._popupCardData = {};
  window._popupCardData[String(releaseId)] = {
    id: releaseId,
    type: entityType,
    title: artists.length ? `${artists.join(", ")} - ${title}` : title,
    cover_image: img,
    uri: searchResult.uri || `/${entityType}/${releaseId}`,
    year: String(year),
    country: country,
    genre: [...(d.genres ?? [])].slice(0, 2),
    label: labelNames.slice(0, 2),
    format: (d.formats ?? []).map(f => f.name).slice(0, 3),
    catno: catno,
  };

  const videoMap = new Map();
  for (const v of (d.videos ?? [])) {
    if (v.title && v.uri) videoMap.set(v.title.toLowerCase(), v.uri);
  }
  function findVideo(trackTitle) {
    const tl = trackTitle.toLowerCase();
    for (const [vt, uri] of videoMap) {
      if (vt.includes(tl) || tl.includes(vt)) return uri;
    }
    return null;
  }

  const identifierTypes = ["Barcode","Matrix / Runout","ASIN","Catalog Number","Label Code"];
  const identifierGroups = {};
  for (const i of (d.identifiers ?? [])) {
    if (i.value && identifierTypes.includes(i.type)) {
      identifierGroups[i.type] = identifierGroups[i.type]
        ? identifierGroups[i.type] + ", " + i.value
        : i.value;
    }
  }

  // Extract special identifier rows for repositioning in the detail grid
  const labelCodeRow = identifierGroups["Label Code"]
    ? `<span class="detail-label">Label Code</span><span>${escHtml(identifierGroups["Label Code"])}</span>` : "";
  const matrixRow = identifierGroups["Matrix / Runout"]
    ? (() => { const val = identifierGroups["Matrix / Runout"]; return `<span class="detail-label">Matrix / Runout</span><span class="matrix-runout" style="color:#7ec87e;cursor:pointer" onclick="navigator.clipboard.writeText('${escHtml(val.replace(/'/g, "\\'"))}');this.dataset.copied='true';setTimeout(()=>this.dataset.copied='',1200)" title="Click to copy">${escHtml(val)}</span>`; })() : "";

  // Remaining identifiers (exclude Label Code and Matrix / Runout — placed separately)
  const otherIdentifierTypes = ["Barcode","ASIN","Catalog Number"];
  const identifierRows = otherIdentifierTypes
    .filter(t => identifierGroups[t])
    .map(t => {
      if (t === "Catalog Number") {
        const vals = identifierGroups[t].split(", ");
        const linked = vals.map(v => {
          const esc = escHtml(v.replace(/'/g, "\\'"));
          return `<a href="#" class="modal-internal-link catno-link" onclick="event.preventDefault();closeModal();document.getElementById('query').value='${esc}';toggleAdvanced(false);document.querySelector('input[name=\\'result-type\\'][value=\\'\\']').checked=true;doSearch(1)" title="Search for this catalog number">${escHtml(v)}</a> <a href="#" class="catno-collection-search" onclick="event.preventDefault();searchCollectionFor('cw-query','${esc}')" title="Search your collection for ${escHtml(v)}">⌕</a>`;
        }).join(", ");
        return `<span class="detail-label">${escHtml(t)}</span><span>${linked}</span>`;
      }
      return `<span class="detail-label">${escHtml(t)}</span><span>${escHtml(identifierGroups[t])}</span>`;
    })
    .join("");

  const companyTypes = ["Pressed By","Manufactured By","Mastered At","Recorded At","Mixed At","Distributed By","Licensed From","Phonographic Copyright (p)"];
  const companyGroups = {};
  for (const c of (d.companies ?? [])) {
    const t = c.entity_type_name;
    if (companyTypes.includes(t)) {
      if (!companyGroups[t]) companyGroups[t] = [];
      companyGroups[t].push(c.name);
    }
  }
  const companyRows = companyTypes
    .filter(t => companyGroups[t])
    .map(t => `<span class="detail-label">${escHtml(t)}</span><span>${escHtml(companyGroups[t].join(", "))}</span>`)
    .join("");

  const seriesLinks = (d.series ?? [])
    .filter(s => s.name)
    .map(s => {
      const label = s.catno ? `${s.name} (${s.catno})` : s.name;
      return `<a href="#" class="modal-internal-link" onclick="event.preventDefault();openSeriesBrowser(${s.id},'${escHtml(s.name.replace(/'/g, "\\'"))}')" title="Browse series: ${escHtml(s.name)}">${escHtml(label)}</a>`;
    }).join(", ");

  const isMaster = searchResult.type === "master";
  const catnoEsc = catno.replace(/'/g, "\\'");
  const detailRows = [
    labelNames.length ? `<span class="detail-label">Label</span><span>${labelNames.map(n => {
      const esc = n.replace(/'/g, "\\'");
      return `<a href="#" class="modal-internal-link" onclick="event.preventDefault();closeModal();clearForm();document.getElementById('f-label').value='${escHtml(esc)}';toggleAdvanced(true);doSearch(1)" title="Search for ${escHtml(n)} releases">${escHtml(n)}</a> <a href="#" class="catno-collection-search" onclick="event.preventDefault();searchCollectionFor('cw-label','${escHtml(esc)}')" title="Search your collection for ${escHtml(n)}">⌕</a>`;
    }).join(", ")}</span>` : "",
    (labels && labelCodeRow) ? labelCodeRow : "",
    (!isMaster && catno) ? `<span class="detail-label">Cat#</span><span><a href="#" class="modal-internal-link catno-link" onclick="event.preventDefault();closeModal();clearForm();document.getElementById('query').value='${escHtml(catnoEsc)}';doSearch(1)" title="Search for this catalog number">${escHtml(catno)}</a> <a href="#" class="catno-collection-search" onclick="event.preventDefault();searchCollectionFor('cw-query','${escHtml(catnoEsc)}')" title="Search your collection for ${escHtml(catno)}">⌕</a></span>` : "",
    (!isMaster && formats) ? `<span class="detail-label">Format</span><span>${escHtml(formats)}</span>` : "",
    year    ? `<span class="detail-label">Year</span><span>${escHtml(String(year))}</span>` : "",
    country ? `<span class="detail-label">Country</span><span>${escHtml(country)}</span>` : "",
    seriesLinks ? `<span class="detail-label">Series</span><span>${seriesLinks}</span>` : "",
    companyRows,
    identifierRows,
    matrixRow,
    genres  ? `<span class="detail-label">Genre</span><span>${escHtml(genres)}</span>`   : "",
  ].filter(Boolean).join("");

  const tracks = (d.tracklist ?? []).filter(t => t.type_ !== "heading");
  const tracklistOpen = localStorage.getItem("tracklist-open") !== "false";
  const trackHTML = tracks.length ? `
    <div class="album-tracklist">
      <div class="tracklist-header">
        <div class="tracklist-heading tracklist-toggle" onclick="toggleTracklist(this)" title="Click to collapse/expand tracklist"><span class="tracklist-arrow">${tracklistOpen ? "▼" : "▶"}</span> Tracklist</div>
        <input type="text" class="tracklist-filter" placeholder="filter tracks…" oninput="filterTracks(this)" />
      </div>
      <div class="tracklist-body"${tracklistOpen ? "" : ' style="display:none"'}>
      ${tracks.map(t => {
        const url = findVideo(t.title || "");
        const trackArtist = artists.length ? artists[0] : "";
        const ytQuery = encodeURIComponent(`${trackArtist} ${title} ${t.title || ""}`);
        const ytIcon = `<a class="yt-search" href="https://www.youtube.com/results?search_query=${ytQuery}" target="_blank" rel="noopener" title="Search on YouTube"><svg width="16" height="11" viewBox="0 0 16 11" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="16" height="11" rx="2.5" fill="#FF0000"/><path d="M6.5 3L11 5.5L6.5 8V3Z" fill="white"/></svg></a>`;
        const trackSearchQ = ('"' + (t.title || '').trim() + '"').replace(/'/g, "\\'");
        const searchIcon = t.title ? ` <a class="track-search-icon" href="#" onclick="event.preventDefault();closeModal();clearForm();document.getElementById('query').value='${escHtml(trackSearchQ)}';doSearch(1)" title="Search for other versions of this track" style="text-decoration:none">⌕</a>` : "";
        const titleEl = url
          ? `<a class="track-link" href="#" data-video="${escHtml(url)}" data-track="${escHtml(t.title || "")}" data-album="${escHtml(title)}" data-artist="${escHtml(trackArtist)}" onclick="openVideo(event,'${url.replace(/'/g, "\\'")}')" title="Play this track">${escHtml(t.title || "")} ▶</a>${searchIcon}`
          : `${escHtml(t.title || "")}${ytIcon}${searchIcon}`;
        return `<div class="track">
          <span class="track-pos">${escHtml(t.position || "")}</span>
          <span class="track-title">${titleEl}</span>
          ${t.duration ? `<span class="track-dur">${escHtml(t.duration)}</span>` : ""}
        </div>`;
      }).join("")}
      </div>
    </div>` : "";

  const creditsHTML = creditItems.length ? `
    <div class="album-credits">
      <div class="credits-header">
        <div class="tracklist-heading">Credits</div>
        ${creditItems.length > 4 ? `<input type="text" class="tracklist-filter" placeholder="filter credits…" oninput="filterCredits(this)" />` : ""}
      </div>
      <div class="credits-body">${creditItems.join('<span class="credit-sep"> · </span>')}</div>
    </div>` : "";

  const metaRows = [
    creditsHTML,
    notes    ? `<div class="album-notes"><div class="tracklist-heading" style="margin-top:0.5rem">Notes</div>${escHtml(notes)}</div>` : "",
  ].filter(Boolean).join("");

  el.innerHTML = `
    <div class="album-header">
      ${img ? `<img class="album-cover" src="${img}" alt="${escHtml(title)}" loading="lazy"
               onclick="openLightbox(${escHtml(JSON.stringify(allImages))},0)"
               title="${allImages.length > 1 ? `View ${allImages.length} photos` : 'View photo'}" />`
             : `<div class="album-cover-placeholder">♪</div>`}
      <div class="album-meta">
        ${typeLabel ? `<div class="album-type-badge" style="cursor:pointer;user-select:none" onclick="navigator.clipboard.writeText('${escHtml(String(releaseId))}');this.dataset.copied='true';setTimeout(()=>this.dataset.copied='',1200)" title="Click to copy ID">${escHtml(typeLabel)}</div>` : ""}
        <h2><a href="#" class="modal-title-link" onclick="event.preventDefault();searchCollectionFor('cw-release','${escHtml(title.replace(/'/g, "\\'"))}')" title="Search your collection for this release">${escHtml(title)}</a> <a href="#" class="album-title-search" onclick="event.preventDefault();searchCollectionFor('cw-release','${escHtml(title.replace(/'/g, "\\'"))}')" title="Search your collection for this release">⌕</a></h2>
        ${artists.length ? `<div class="album-artist">${artists.map(n => `<a href="#" class="modal-artist-link" data-artist="${escHtml(n)}" onclick="searchArtistFromModal(event,this)" title="Search for ${escHtml(n)}">${escHtml(n)}</a> <a href="#" class="album-title-search" onclick="event.preventDefault();searchCollectionFor('cw-artist','${escHtml(n.replace(/'/g, "\\'"))}')" title="Search your collection for ${escHtml(n)}">⌕</a>`).join(", ")}</div>` : ""}
        ${detailRows ? `<div class="album-detail-grid">${detailRows}</div>` : ""}
        ${(() => {
          const r = d.community?.rating;
          const have = d.community?.have;
          const want = d.community?.want;
          const parts = [];
          if (r?.count > 0) parts.push(`★ ${parseFloat(r.average).toFixed(2)} <span style="color:#555">(${r.count.toLocaleString()} ratings)</span>`);
          if (have || want) parts.push(`${(have ?? 0).toLocaleString()} have · ${(want ?? 0).toLocaleString()} want`);
          return parts.length ? `<div style="font-size:0.72rem;color:#888;margin-top:0.35rem">${parts.join('<span style="color:#444;margin:0 0.35em">·</span>')}</div>` : "";
        })()}
        ${(!isMaster && d.master_id) ? `<div style="margin-top:0.4rem"><a href="#" class="modal-internal-link" onclick="event.preventDefault();closeModal();setTimeout(()=>openModal(null,${d.master_id},'master','https://www.discogs.com/master/${d.master_id}'),100)" title="View all pressings of this release" style="font-size:0.75rem;color:#7eb8da;text-decoration:none">Master/Versions</a></div>` : ""}
        ${discogsUrl ? `<a href="${discogsUrl}" target="_blank" rel="noopener" title="Open this release on Discogs.com" style="font-size:0.75rem;color:#888;text-decoration:none;margin-top:0.25rem;display:inline-block">View on Discogs ↗</a>` : ""}
        ${stats?.numForSale > 0 && (stats?.lowestPrice != null || stats?.medianPrice != null || stats?.highestPrice != null)
          ? (() => {
              const low = stats.lowestPrice != null ? parseFloat(stats.lowestPrice).toFixed(2) : null;
              const med = stats.medianPrice != null ? parseFloat(stats.medianPrice).toFixed(2) : null;
              const high = stats.highestPrice != null ? parseFloat(stats.highestPrice).toFixed(2) : null;
              const sellUrl = `https://www.discogs.com/sell/list?release_id=${escHtml(String(stats.releaseId))}`;
              const count = escHtml(String(stats.numForSale));
              const dash = `<span style="color:#555;margin:0 0.15rem"> ── </span>`;
              const parts = [];
              if (low) parts.push(`<span style="color:var(--accent)">$${low}</span>`);
              if (med) parts.push(`<span style="color:#999">$${med}</span>`);
              if (high) parts.push(`<span style="color:#777">$${high}</span>`);
              const priceBar = parts.length === 1
                ? `from ${parts[0]}`
                : parts.join(dash);
              const estId = `price-est-${escHtml(String(stats.releaseId))}`;
              return `<div style="font-size:0.75rem;margin-top:0.2rem">
                <a href="${sellUrl}" target="_blank" rel="noopener" title="Browse ${count} listings on Discogs marketplace" style="color:#888;text-decoration:none">(${count}) :: ${priceBar} ↗</a>
                ${!isMaster ? `<a href="#" onclick="event.preventDefault();loadPriceEstimates('${escHtml(String(stats.releaseId))}','${estId}')" style="color:#555;text-decoration:none;margin-left:0.4rem;font-size:0.7rem" title="Show estimated prices by condition">(est)</a><div id="${estId}"></div>` : ""}
              </div>`;
            })()
          : (stats?.numForSale === 0 ? `<div style="font-size:0.75rem;color:#555;margin-top:0.2rem">Not currently available on Discogs marketplace</div>` : "")
        }
        ${(!isMaster && releaseId) ? `<a href="https://www.discogs.com/sell/post/${escHtml(String(releaseId))}" target="_blank" rel="noopener" title="Sell a copy of this release on Discogs" style="font-size:0.75rem;color:#888;text-decoration:none;margin-top:0.2rem;display:inline-block">Sell a copy ↗</a>` : ""}
        ${releaseId ? renderActionsImmediate(Number(releaseId), isMaster ? "master" : "release") : ""}
      </div>
    </div>
    ${trackHTML}
    ${metaRows ? `<div class="album-extra">${metaRows}</div>` : ""}
    ${isMaster ? `<div id="master-versions-list" style="padding:0.75rem;font-size:0.78rem;color:var(--muted)">Loading pressings…</div>` : ""}`;

  if (isMaster) loadMasterVersions(null, searchResult.id);
  // Fetch instance data in background (for rating stars + instanceId)
  if (!isMaster && releaseId && window._collectionIds?.has(Number(releaseId))) loadModalInstanceData(Number(releaseId));
  // Highlight currently playing track if a video is active
  highlightPlayingTrack();
}

// ── Modal action buttons (collection/wantlist/rating) ────────────────────

async function loadPriceEstimates(releaseId, targetId) {
  const el = document.getElementById(targetId);
  if (!el) return;
  // Toggle off if already showing
  if (el.innerHTML) { el.innerHTML = ""; return; }
  el.innerHTML = `<span style="color:#555;font-size:0.7rem">Loading…</span>`;
  try {
    const r = await apiFetch(`/api/price-suggestions/${releaseId}`);
    if (!r.ok) throw new Error();
    const data = await r.json();
    // Conditions from best → worst, with short labels and gradient colors (orange → blue)
    const grades = [
      { key: "Mint (M)",                 label: "M",   color: "#e08a3a" },
      { key: "Near Mint (NM or M-)",     label: "NM",  color: "#c4893f" },
      { key: "Very Good Plus (VG+)",     label: "VG+", color: "#a08850" },
      { key: "Very Good (VG)",           label: "VG",  color: "#7c8766" },
      { key: "Good Plus (G+)",           label: "G+",  color: "#58867c" },
      { key: "Good (G)",                 label: "G",   color: "#3a8596" },
    ];
    const rows = grades
      .filter(g => data[g.key]?.value != null)
      .map(g => {
        const val = parseFloat(data[g.key].value).toFixed(2);
        return `<span style="color:${g.color};font-weight:600">${g.label}</span>&nbsp;<span style="color:#aaa">$${val}</span>`;
      });
    el.innerHTML = rows.length
      ? `<div style="font-size:0.72rem;margin-top:0.25rem;display:flex;flex-wrap:wrap;gap:0.15rem 0.6rem">${rows.map(r => `<span>${r}</span>`).join("")}</div>`
      : `<span style="color:#555;font-size:0.7rem">No estimates available</span>`;
  } catch {
    el.innerHTML = `<span style="color:#555;font-size:0.7rem">Estimates unavailable</span>`;
  }
}

// Render buttons immediately from local Sets (no network call)
function renderActionsImmediate(rid, entityType = "release") {
  const inCol = window._collectionIds?.has(rid);
  const inWant = window._wantlistIds?.has(rid);
  const favKey = `${entityType}:${rid}`;
  const isFav = window._favoriteKeys?.has(favKey);
  const favBtn = `<button class="modal-act-btn ${isFav ? 'is-favorite' : ''}" id="modal-fav-btn" onclick="toggleFavoriteFromModal(${rid},'${entityType}')" title="${isFav ? 'Remove from favorites' : 'Add to favorites'}">
      ${isFav ? 'Favorited' : 'Favorite'}
    </button>`;
  if (entityType !== "release") {
    // Master/artist/label — only show favorite button
    return `<div id="modal-actions" class="modal-actions" data-release-id="${rid}" data-entity-type="${entityType}">${favBtn}</div>`;
  }
  return `<div id="modal-actions" class="modal-actions" data-release-id="${rid}" data-entity-type="${entityType}">
    <button class="modal-act-btn ${inCol ? 'in-collection' : ''}" id="modal-col-btn" onclick="toggleCollection(${rid})" title="${inCol ? 'Remove from collection' : 'Add to collection'}">
      ${inCol ? 'Collected' : 'Collection'}
    </button>
    <button class="modal-act-btn ${inWant ? 'in-wantlist' : ''}" id="modal-want-btn" onclick="toggleWantlist(${rid})" title="${inWant ? 'Remove from wantlist' : 'Add to wantlist'}">
      ${inWant ? 'Wanted' : 'Want'}
    </button>
    ${favBtn}
    ${inCol ? '<span class="modal-rating" id="modal-rating" style="opacity:0.4">☆☆☆☆☆</span>' : ''}
  </div>`;
}

// Fetch instance data in background and upgrade the rating stars
async function loadModalInstanceData(releaseId) {
  const el = document.getElementById("modal-actions");
  if (!el) return;
  const rid = Number(releaseId);
  let instanceId = null, folderId = 1, currentRating = 0;
  try {
    const sessionToken = window._clerk?.session ? await window._clerk.session.getToken() : null;
    if (sessionToken) {
      const data = await fetch(`/api/user/collection/instance?releaseId=${rid}`, {
        headers: { Authorization: `Bearer ${sessionToken}` }
      }).then(r => r.json());
      if (data?.found) {
        instanceId = data.instance_id;
        folderId = data.folder_id ?? 1;
        currentRating = data.rating ?? 0;
      }
    }
  } catch {}
  el.dataset.instanceId = instanceId ?? "";
  el.dataset.folderId = folderId;
  // Upgrade rating stars with actual data
  const ratingEl = document.getElementById("modal-rating");
  if (ratingEl) {
    ratingEl.innerHTML = renderStars(currentRating, rid);
    ratingEl.dataset.rating = currentRating;
    ratingEl.style.opacity = "";
  }
}

// Full re-render of action buttons (used after toggle actions)
// context: "version-info" for version popup, otherwise main modal
function loadModalActions(releaseId, context) {
  const container = context ? document.getElementById(context) : null;
  const el = container ? container.querySelector(".modal-actions") : document.getElementById("modal-actions");
  if (!el) return;
  const rid = Number(releaseId);
  const entityType = el.dataset.entityType || "release";
  const inCol = window._collectionIds?.has(rid);
  const inWant = window._wantlistIds?.has(rid);
  const favKey = `${entityType}:${rid}`;
  const isFav = window._favoriteKeys?.has(favKey);
  const favBtn = `<button class="modal-act-btn ${isFav ? 'is-favorite' : ''}" id="modal-fav-btn" onclick="toggleFavoriteFromModal(${rid},'${entityType}')" title="${isFav ? 'Remove from favorites' : 'Add to favorites'}">
      ${isFav ? 'Favorited' : 'Favorite'}
    </button>`;

  if (entityType !== "release") {
    el.innerHTML = favBtn;
  } else {
    el.innerHTML = `
      <button class="modal-act-btn ${inCol ? 'in-collection' : ''}" id="modal-col-btn" onclick="toggleCollection(${rid})" title="${inCol ? 'Remove from collection' : 'Add to collection'}">
        ${inCol ? 'Collected' : 'Collection'}
      </button>
      <button class="modal-act-btn ${inWant ? 'in-wantlist' : ''}" id="modal-want-btn" onclick="toggleWantlist(${rid})" title="${inWant ? 'Remove from wantlist' : 'Add to wantlist'}">
        ${inWant ? 'Wanted' : 'Want'}
      </button>
      ${favBtn}
      ${inCol ? '<span class="modal-rating" id="modal-rating" style="opacity:0.4">☆☆☆☆☆</span>' : ''}
    `;
  }
  el.style.display = "";
  // If in collection, fetch instance data for rating
  if (entityType === "release" && inCol) loadModalInstanceData(rid);
}

function renderStars(rating, releaseId) {
  let html = '';
  for (let i = 1; i <= 5; i++) {
    const active = i <= rating;
    html += `<span class="modal-star ${active ? 'active' : ''}" onclick="setRating(event,${releaseId},${i})" onmouseover="previewStars(this,${i})" onmouseout="resetStars(this)" title="Rate ${i} out of 5">${active ? '★' : '☆'}</span>`;
  }
  return html;
}

function previewStars(el, n) {
  const container = el.parentElement;
  container.querySelectorAll('.modal-star').forEach((s, i) => {
    s.textContent = i < n ? '★' : '☆';
    s.classList.toggle('preview', i < n);
  });
}

function resetStars(el) {
  const container = el.parentElement;
  const current = Number(container.dataset.rating ?? 0);
  container.querySelectorAll('.modal-star').forEach((s, i) => {
    s.textContent = i < current ? '★' : '☆';
    s.classList.remove('preview');
    s.classList.toggle('active', i < current);
  });
}

async function toggleCollection(releaseId) {
  const btn = document.getElementById("modal-col-btn");
  if (btn) btn.disabled = true;
  const inCol = window._collectionIds?.has(releaseId);
  try {
    const sessionToken = window._clerk?.session ? await window._clerk.session.getToken() : null;
    if (!sessionToken) { showToast("Sign in to manage your collection", "error"); return; }

    // Optimistic update
    if (inCol) {
      btn.innerHTML = '+ Collection';
      btn.classList.remove('in-collection');
      window._collectionIds?.delete(releaseId);
    } else {
      btn.innerHTML = '✓ Collected';
      btn.classList.add('in-collection');
      window._collectionIds?.add(releaseId);
    }
    refreshCardBadges(releaseId);

    const actionsEl = document.getElementById("modal-actions");
    const endpoint = inCol ? "/api/user/collection/remove" : "/api/user/collection/add";
    const body = inCol
      ? { releaseId, instanceId: Number(actionsEl?.dataset.instanceId) || null, folderId: Number(actionsEl?.dataset.folderId) || 1 }
      : { releaseId };

    const r = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${sessionToken}` },
      body: JSON.stringify(body)
    }).then(r => r.json());

    if (!r.ok && r.error) {
      // Revert
      if (inCol) { window._collectionIds?.add(releaseId); } else { window._collectionIds?.delete(releaseId); }
      showToast(r.error, "error");
      loadModalActions(releaseId);
      return;
    }

    showToast(inCol ? "Removed from collection" : "Added to collection");
    // Refresh action row to update rating stars, instance info
    loadModalActions(releaseId);
    // Update badges on cards in the background
    refreshCardBadges(releaseId);
  } catch (e) {
    if (inCol) { window._collectionIds?.add(releaseId); } else { window._collectionIds?.delete(releaseId); }
    showToast("Action failed — try again", "error");
    loadModalActions(releaseId);
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function toggleWantlist(releaseId) {
  const btn = document.getElementById("modal-want-btn");
  if (btn) btn.disabled = true;
  const inWant = window._wantlistIds?.has(releaseId);
  try {
    const sessionToken = window._clerk?.session ? await window._clerk.session.getToken() : null;
    if (!sessionToken) { showToast("Sign in to manage your wantlist", "error"); return; }

    // Optimistic update
    if (inWant) {
      btn.innerHTML = '<span class="want-icon">🤞</span> Want';
      btn.classList.remove('in-wantlist');
      window._wantlistIds?.delete(releaseId);
    } else {
      btn.innerHTML = '<span class="want-icon active">🤞</span> Wanted';
      btn.classList.add('in-wantlist');
      window._wantlistIds?.add(releaseId);
    }
    refreshCardBadges(releaseId);

    const endpoint = inWant ? "/api/user/wantlist/remove" : "/api/user/wantlist/add";
    const r = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${sessionToken}` },
      body: JSON.stringify({ releaseId })
    }).then(r => r.json());

    if (!r.ok && r.error) {
      if (inWant) { window._wantlistIds?.add(releaseId); } else { window._wantlistIds?.delete(releaseId); }
      showToast(r.error, "error");
      loadModalActions(releaseId);
      return;
    }

    showToast(inWant ? "Removed from wantlist" : "Added to wantlist");
    refreshCardBadges(releaseId);
    loadModalActions(releaseId);
  } catch (e) {
    if (inWant) { window._wantlistIds?.add(releaseId); } else { window._wantlistIds?.delete(releaseId); }
    showToast("Action failed — try again", "error");
    loadModalActions(releaseId);
  } finally {
    if (btn) btn.disabled = false;
  }
}

let _ratingDebounce = null;
async function setRating(event, releaseId, rating) {
  event.stopPropagation();
  const container = document.getElementById("modal-rating");
  if (!container) return;
  // Update stars immediately
  container.dataset.rating = rating;
  container.querySelectorAll('.modal-star').forEach((s, i) => {
    s.textContent = i < rating ? '★' : '☆';
    s.classList.toggle('active', i < rating);
  });

  // Debounce the API call
  clearTimeout(_ratingDebounce);
  _ratingDebounce = setTimeout(async () => {
    try {
      const sessionToken = window._clerk?.session ? await window._clerk.session.getToken() : null;
      if (!sessionToken) return;
      const actionsEl = document.getElementById("modal-actions");
      const r = await fetch("/api/user/collection/rating", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${sessionToken}` },
        body: JSON.stringify({
          releaseId,
          instanceId: Number(actionsEl?.dataset.instanceId) || null,
          folderId: Number(actionsEl?.dataset.folderId) || 1,
          rating
        })
      }).then(r => r.json());
      if (r.ok) showToast(`Rated ${rating}/5`);
    } catch { showToast("Failed to save rating", "error"); }
  }, 500);
}

// ── Price sparkline + alert UI in modal ─────────────────────────────────


function refreshCardBadges(releaseId) {
  // Re-render badges on any visible card with this release ID
  document.querySelectorAll(`.card-thumb-badges`).forEach(el => {
    const card = el.closest('a[onclick]');
    if (!card) return;
    // Match openModal cards (releases/masters)
    const modalMatch = card.getAttribute('onclick')?.match(/openModal\(event,['"]?(\d+)['"]?,\s*'(\w+)'/);
    // Match searchByEntity cards (artists/labels)
    const entityMatch = !modalMatch ? card.dataset.entityId : null;
    const entityType = !modalMatch ? card.dataset.entityType : null;

    let id, type;
    if (modalMatch && Number(modalMatch[1]) === releaseId) {
      id = releaseId; type = modalMatch[2] || "release";
    } else if (entityMatch && Number(entityMatch) === releaseId) {
      id = releaseId; type = entityType || "artist";
    } else return;

    let badges = "";
    const inCol = type === "release" && window._collectionIds?.has(id);
    const inWant = type === "release" && window._wantlistIds?.has(id);
    if (type === "release") {
      badges += `<span class="card-badge badge-collection${inCol ? " is-active" : ""}" title="${inCol ? "In your collection" : "Not in collection"}">C</span>`;
      badges += `<span class="card-badge badge-wantlist${inWant ? " is-active" : ""}" title="${inWant ? "In your wantlist" : "Not in wantlist"}">W</span>`;
      const lists = window._listMembership?.[id];
      if (lists?.length) {
        const names = lists.map(l => l.listName).join(", ");
        badges += `<span class="card-badge badge-list" title="In list: ${escHtml(names)}">L</span>`;
      }
      if (window._inventoryIds?.has(id)) badges += `<span class="card-badge badge-inventory" title="In your inventory">I</span>`;
    }
    const favKey = `${type}:${id}`;
    const isFav = window._favoriteKeys?.has(favKey);
    badges += `<span class="card-badge badge-favorite${isFav ? " is-favorite" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleFavoriteFromCard(this,${id},'${type}')" title="${isFav ? "Remove from favorites" : "Add to favorites"}">${isFav ? "♥" : "♡"}</span>`;
    el.innerHTML = badges;
  });
}

function toggleFavoriteFromModal(discogsId, entityType) {
  const key = `${entityType}:${discogsId}`;
  const wasFav = window._favoriteKeys?.has(key);
  if (!window._favoriteKeys) window._favoriteKeys = new Set();

  // Detect which popup this came from (version overlay or main modal)
  const inVersion = document.getElementById("version-overlay")?.classList.contains("open");
  const context = inVersion ? "version-info" : null;

  // Optimistic toggle
  if (wasFav) window._favoriteKeys.delete(key); else window._favoriteKeys.add(key);
  loadModalActions(discogsId, context);
  refreshCardBadges(discogsId);

  // Build card data — use _popupCardData (built in renderAlbumInfo with full API data)
  const popupData = window._popupCardData?.[String(discogsId)];
  const cached = (typeof itemCache !== "undefined") ? itemCache.get(String(discogsId)) : null;
  // Prefer popupCardData (always rich), then itemCache if it has detail fields
  const cardData = popupData
    || (cached && (cached.label?.length || cached.format?.length) ? cached : null)
    || (() => {
      const prefix = inVersion ? "#version-info" : "#album-info";
      const modalTitle = document.querySelector(`${prefix} .album-meta h2`);
      const modalArtist = document.querySelector(`${prefix} .album-artist`);
      const modalImg = document.querySelector(`${prefix} .album-cover`);
      return {
        id: discogsId, type: entityType,
        title: [modalArtist?.textContent?.replace(/⌕/g,"").trim(), modalTitle?.textContent?.replace(/⌕/g,"").trim()].filter(Boolean).join(" - "),
        cover_image: modalImg?.src || "", uri: `/${entityType}/${discogsId}`,
      };
    })();

  const endpoint = wasFav ? "/api/user/favorites/remove" : "/api/user/favorites/add";
  const body = wasFav ? { discogsId, entityType } : { discogsId, entityType, data: cardData };
  apiFetch(endpoint, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
    .then(r => { if (!r.ok) throw new Error(); const n = window._favoriteKeys?.size ?? 0; showToast(wasFav ? "Removed from favorites" : `Added to favorites (${n})`); })
    .catch(() => {
      if (wasFav) window._favoriteKeys.add(key); else window._favoriteKeys.delete(key);
      loadModalActions(discogsId, context);
      refreshCardBadges(discogsId);
      showToast("Failed to update favorite", "error");
    });
}

let _masterVersions = [];
let _mvFormatFilter = "";
let _mvCountryFilter = "";

const _MV_MEDIA = new Set(["Vinyl","CD","Cassette","DVD","Blu-ray","File","Box Set","Lathe Cut","Flexi-disc","Shellac","8-Track Cartridge","Reel-To-Reel","MiniDisc","SACD","Betamax","VHS"]);
function _mvGetMedium(v) {
  const parts = (v.format ?? "").split(",").map(s => s.trim());
  return parts.find(p => _MV_MEDIA.has(p)) || (v.majorFormats ?? []).find(f => _MV_MEDIA.has(f)) || parts[0] || "";
}
function _mvGetDisplayFormat(v) {
  const fmt = (v.format ?? "").trim();
  const medium = (v.majorFormats ?? []).find(f => _MV_MEDIA.has(f));
  if (!fmt) return medium || "—";
  if (!medium || fmt.split(",").map(s => s.trim()).includes(medium)) return fmt;
  return `${medium}, ${fmt}`;
}

function setMvFormatFilter(f) { _mvFormatFilter = f; localStorage.setItem("mv-format-filter", f); renderMasterVersions(); }
function setMvCountryFilter(c) { _mvCountryFilter = c; localStorage.setItem("mv-country-filter", c); renderMasterVersions(); }

function renderMasterVersions() {
  const list = document.getElementById("master-versions-list");
  if (!list) return;

  let filtered = _masterVersions;
  if (_mvFormatFilter) filtered = filtered.filter(v => _mvGetMedium(v) === _mvFormatFilter);
  if (_mvCountryFilter) filtered = filtered.filter(v => (v.country || "") === _mvCountryFilter);

  list.querySelectorAll(".mv-format-pill").forEach(p => {
    p.style.background = p.dataset.filter === (_mvFormatFilter ?? "") ? "var(--accent)" : "#2a2a2a";
    p.style.color      = p.dataset.filter === (_mvFormatFilter ?? "") ? "#000" : "var(--fg)";
  });
  list.querySelectorAll(".mv-country-pill").forEach(p => {
    p.style.background = p.dataset.filter === (_mvCountryFilter ?? "") ? "var(--accent)" : "#2a2a2a";
    p.style.color      = p.dataset.filter === (_mvCountryFilter ?? "") ? "#000" : "var(--fg)";
  });

  const grid = list.querySelector(".mv-grid");
  if (!grid) return;
  if (!filtered.length) { grid.innerHTML = `<span style="color:var(--muted);grid-column:1/-1">No pressings match this filter.</span>`; return; }
  grid.innerHTML = filtered.map(v => {
    const inCol  = window._collectionIds?.has(v.id);
    const inWant = window._wantlistIds?.has(v.id);
    const inList = window._listMembership?.[v.id]?.length > 0;
    const inInv  = window._inventoryIds?.has(v.id);
    const isFav  = window._favoriteKeys?.has(`release:${v.id}`);
    const badgeParts = [];
    if (inCol)  badgeParts.push(`<span style="color:#6ddf70;font-weight:700">C</span>`);
    if (inWant) badgeParts.push(`<span style="color:#f0c95c;font-weight:700">W</span>`);
    if (inList) badgeParts.push(`<span style="color:#a0ccf0;font-weight:700">L</span>`);
    if (inInv)  badgeParts.push(`<span style="color:#cda0f5;font-weight:700">I</span>`);
    if (isFav)  badgeParts.push(`<span style="color:#ff80ab">♥</span>`);
    const badge  = `<span>${badgeParts.length ? badgeParts.join("") : "&nbsp;"}</span>`;
    return `
      <span style="color:#888">${escHtml(!v.year || v.year === "0" ? "?" : String(v.year))}</span>
      <span style="color:#aaa">${escHtml(v.country || "?")}</span>
      <span style="color:#888" title="${escHtml(_mvGetDisplayFormat(v))}">${escHtml(_mvGetDisplayFormat(v))}</span>
      ${badge}
      <span title="${escHtml(v.catno || "")}">${v.catno && v.catno !== "—" ? `<a href="#" class="modal-internal-link catno-link" onclick="openVersionPopup(event,${v.id})" title="Open this release">${escHtml(v.catno)}</a>` : `<span style="color:#7ec87e">—</span>`}</span>
      <span title="${escHtml(v.label ?? v.title ?? "")}">${(v.label) ? `<a href="#" class="modal-internal-link" onclick="event.preventDefault();closeModal();clearForm();document.getElementById('f-label').value='${escHtml((v.label).replace(/'/g, "\\'"))}';toggleAdvanced(true);doSearch(1)" title="Search for ${escHtml(v.label)}" style="color:var(--fg)">${escHtml(v.label)}</a> <a href="#" class="album-title-search" onclick="event.preventDefault();searchCollectionFor('cw-label','${escHtml((v.label).replace(/'/g, "\\'"))}')" title="Search your collection for ${escHtml(v.label)}" style="font-size:0.85em">⌕</a>` : `<span style="color:#888">${escHtml(v.title ?? "—")}</span>`}</span>`;
  }).join("");
}

// ── Series browser ────────────────────────────────────────────────────────
let _seriesReleases = [];
let _srFormatFilter = "";

function _srGetMedium(r) {
  const parts = (r.format ?? "").split(",").map(s => s.trim());
  return parts.find(p => _MV_MEDIA.has(p)) || parts[0] || "";
}

async function openSeriesBrowser(seriesId, seriesName) {
  const overlay = document.getElementById("series-overlay");
  const info    = document.getElementById("series-info");
  const loading = document.getElementById("series-loading");
  info.innerHTML = "";
  loading.style.display = "block";
  overlay.classList.add("open");
  document.body.classList.add("modal-open");

  try {
    const resp = await apiFetch(`${API}/series-releases/${seriesId}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    _seriesReleases = data.releases ?? [];
    loading.style.display = "none";

    if (!_seriesReleases.length) {
      info.innerHTML = `<div style="color:var(--muted);padding:1rem">No releases found in this series.</div>`;
      return;
    }

    // Build format filter pills
    const formatSet = new Set();
    _seriesReleases.forEach(r => {
      const m = _srGetMedium(r);
      if (m) formatSet.add(m);
    });
    const formats = [...formatSet].sort();
    _srFormatFilter = "";

    const formatPills = formats.length > 1
      ? `<div class="sr-pill-row">${[
          `<button class="sr-pill sr-format-pill" data-filter="" onclick="setSrFormatFilter('')">All</button>`,
          ...formats.map(f => `<button class="sr-pill sr-format-pill" data-filter="${escHtml(f)}" onclick="setSrFormatFilter('${f.replace(/'/g,"\\'")}')">${escHtml(f)}</button>`)
        ].join("")}</div>`
      : "";

    info.innerHTML = `
      <div style="font-size:0.9rem;color:var(--fg);font-weight:600;margin-bottom:0.15rem">${escHtml(data.name || seriesName)}</div>
      <div style="font-size:0.72rem;color:var(--muted);margin-bottom:0.6rem">${_seriesReleases.length} release${_seriesReleases.length !== 1 ? "s" : ""} in series${data.total > _seriesReleases.length ? ` (showing first ${_seriesReleases.length} of ${data.total})` : ""}</div>
      ${formatPills}
      <div class="sr-grid-scroll" style="overflow-x:auto"><div class="sr-grid"></div></div>`;
    renderSeriesReleases();
  } catch(e) {
    loading.style.display = "none";
    info.innerHTML = `<div style="color:var(--muted);padding:1rem">Failed to load series releases.</div>`;
    console.error("openSeriesBrowser error:", e);
  }
}

function closeSeriesBrowser() {
  document.getElementById("series-overlay").classList.remove("open");
  if (!document.getElementById("modal-overlay")?.classList.contains("open") &&
      !document.getElementById("version-overlay")?.classList.contains("open")) {
    document.body.classList.remove("modal-open");
  }
}

document.getElementById("series-overlay")?.addEventListener("click", e => {
  if (e.target === document.getElementById("series-overlay")) closeSeriesBrowser();
});

function setSrFormatFilter(f) {
  _srFormatFilter = f;
  renderSeriesReleases();
}

function renderSeriesReleases() {
  const grid = document.querySelector("#series-info .sr-grid");
  if (!grid) return;

  let filtered = _seriesReleases;
  if (_srFormatFilter) filtered = filtered.filter(r => _srGetMedium(r) === _srFormatFilter);

  // Update pill styles
  document.querySelectorAll(".sr-format-pill").forEach(p => {
    p.style.background = p.dataset.filter === _srFormatFilter ? "var(--accent)" : "#2a2a2a";
    p.style.color      = p.dataset.filter === _srFormatFilter ? "#000" : "var(--fg)";
  });

  if (!filtered.length) {
    grid.innerHTML = `<span style="color:var(--muted);grid-column:1/-1">No releases match this filter.</span>`;
    return;
  }

  grid.innerHTML = filtered.map(r => {
    const inCol  = window._collectionIds?.has(r.id);
    const inWant = window._wantlistIds?.has(r.id);
    const inList = window._listMembership?.[r.id]?.length > 0;
    const inInv  = window._inventoryIds?.has(r.id);
    const isFav  = window._favoriteKeys?.has(`release:${r.id}`);
    const badges = [];
    if (inCol)  badges.push(`<span style="color:#6ddf70;font-weight:700">C</span>`);
    if (inWant) badges.push(`<span style="color:#f0c95c;font-weight:700">W</span>`);
    if (inList) badges.push(`<span style="color:#a0ccf0;font-weight:700">L</span>`);
    if (inInv)  badges.push(`<span style="color:#cda0f5;font-weight:700">I</span>`);
    if (isFav)  badges.push(`<span style="color:#ff80ab">♥</span>`);
    const badge = badges.length ? badges.join("") : `<span style="visibility:hidden">C</span>`;

    const thumbHtml = r.thumb
      ? `<img src="${r.thumb}" alt="" loading="lazy" />`
      : `<span style="display:inline-block;width:40px;height:40px;background:#1a1a1a;border-radius:3px"></span>`;

    const yearStr = r.year && r.year !== 0 ? String(r.year) : "?";
    const titleArtist = r.artist ? `${r.artist} — ${r.title}` : r.title;

    return `
      ${thumbHtml}
      <a href="#" class="sr-title" onclick="event.preventDefault();openVersionPopup(event,${r.id})" title="${escHtml(titleArtist)}">${escHtml(titleArtist)}</a>
      ${badge}
      <span style="color:#888">${escHtml(yearStr)}</span>
      <span style="color:#666" title="${escHtml(r.format)}">${escHtml(r.catno || r.format || "—")}</span>`;
  }).join("");
}

async function loadMasterVersions(event, masterId) {
  if (event) event.preventDefault();
  const list = document.getElementById("master-versions-list");
  if (!list) return;
  try {
    const resp = await apiFetch(`${API}/master-versions/${masterId}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    _masterVersions = data.versions ?? [];
    if (!_masterVersions.length) { list.textContent = "No pressings found."; return; }

    const formatSet = new Set();
    const countrySet = new Set();
    _masterVersions.forEach(v => {
      const medium = _mvGetMedium(v);
      if (medium) formatSet.add(medium);
      if (v.country) countrySet.add(v.country);
    });
    const formats = [...formatSet].sort();
    const countries = [...countrySet].sort();

    // Restore saved filters if they exist in this master's options
    const savedFormat = localStorage.getItem("mv-format-filter") || "";
    const savedCountry = localStorage.getItem("mv-country-filter") || "";
    _mvFormatFilter = formatSet.has(savedFormat) ? savedFormat : "";
    _mvCountryFilter = countrySet.has(savedCountry) ? savedCountry : "";

    const formatPills = [
      `<button class="mv-format-pill mv-pill" data-filter="" onclick="setMvFormatFilter('')">All</button>`,
      ...formats.map(f => `<button class="mv-format-pill mv-pill" data-filter="${escHtml(f)}" onclick="setMvFormatFilter('${f.replace(/'/g,"\\'")}')">${escHtml(f)}</button>`)
    ].join("");
    const countryPills = [
      `<button class="mv-country-pill mv-pill" data-filter="" onclick="setMvCountryFilter('')">All</button>`,
      ...countries.map(c => `<button class="mv-country-pill mv-pill" data-filter="${escHtml(c)}" onclick="setMvCountryFilter('${c.replace(/'/g,"\\'")}')">${escHtml(c)}</button>`)
    ].join("");

    list.innerHTML = `
      <div style="font-size:0.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.4rem">Pressings / Versions</div>
      ${formats.length > 1 ? `<div class="mv-pill-row">${formatPills}</div>` : ""}
      ${countries.length > 1 ? `<div class="mv-pill-row">${countryPills}</div>` : ""}
      <div class="mv-grid-scroll"><div class="mv-grid" style="display:grid;grid-template-columns:auto auto minmax(0,7rem) 2.5rem minmax(0,8rem) minmax(8rem,1fr);gap:0.2rem 0.7rem;font-size:0.75rem;min-width:36rem"></div></div>`;
    renderMasterVersions();
  } catch(e) {
    console.error("loadMasterVersions error:", e);
    list.textContent = "Failed to load pressings.";
  }
}
