// ── Modal ─────────────────────────────────────────────────────────────────
function openModal(event, id, type, discogsUrl) {
  if (event) event.preventDefault();
  const u = new URL(window.location.href);
  u.searchParams.set("op", `${type}:${id}`);
  history.replaceState({}, "", u.toString());
  const overlay = document.getElementById("modal-overlay");
  document.getElementById("album-info").innerHTML = "";
  document.getElementById("modal-loading").style.display = "block";
  overlay.classList.add("open");

  const cachedItem = itemCache.get(String(id)) ?? { type };
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
  const u = new URL(window.location.href);
  u.searchParams.delete("op");
  history.replaceState({}, "", u.toString());
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
    let html = `<div class="concert-artist-name">${escHtml(artistName)} — Upcoming Shows <a href="#" onclick="event.preventDefault();closeConcertPopup();closeModal();document.getElementById('live-artist').value='${escArt}';switchView('live');doLiveSearch()" style="font-size:0.75rem;color:var(--accent);text-decoration:none;margin-left:0.5rem">Search on Live →</a></div>`;
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
            <a href="${googleUrl}" target="_blank" rel="noopener">${escHtml(ev.venue)}</a>
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
      `<a href="#" class="bio-artist-link" onclick="selectAltArtist(event,this);closeBioFull()" data-alt-name="${escHtml(a.name)}"${a.id ? ` data-alt-id="${a.id}"` : ""} style="color:var(--accent);text-decoration:none">${escHtml(a.name)}</a>`
    ).join('<span style="color:#555;margin:0 0.3em">·</span>');
    html += `<div style="font-size:0.78rem;margin-top:0.7rem;line-height:1.6"><span style="color:#777;margin-right:0.4em">Also:</span>${altLinks}</div>`;
  }
  const relLinks = renderArtistRelations(members, groups, aliases, namevariations, urls, parentLabel, sublabels);
  if (relLinks) html += relLinks;
  if (discogsId) {
    html += `<div style="margin-top:1.1rem"><a href="https://www.discogs.com/artist/${discogsId}" target="_blank" rel="noopener" style="font-size:0.75rem;color:#666;text-decoration:none">View profile on Discogs ↗</a></div>`;
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
window.onYouTubeIframeAPIReady = function() { window._ytAPIReady = true; };

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
  history.replaceState({}, "", u.toString());
}

function loadYTVideo(id) {
  if (ytPlayer && typeof ytPlayer.loadVideoById === "function") {
    ytPlayer.loadVideoById(id);
    return;
  }
  if (window._ytAPIReady && typeof YT !== "undefined") {
    document.getElementById("video-player").innerHTML = "";
    ytPlayer = new YT.Player("video-player", {
      height: "100%", width: "100%", videoId: id,
      playerVars: { autoplay: 1, rel: 0 },
      events: { onStateChange: function(e) { if (e.data === 0) playNextVideo(); } }
    });
  } else {
    document.getElementById("video-player").innerHTML =
      `<iframe src="https://www.youtube.com/embed/${id}?autoplay=1&enablejsapi=1" style="width:100%;height:100%;border:none" allow="autoplay;encrypted-media" allowfullscreen></iframe>`;
  }
}

function updateVideoNavButtons() {
  const queue = window._videoQueue ?? [];
  const idx   = window._videoQueueIndex ?? 0;
  const prevBtn  = document.getElementById("video-prev");
  const nextBtn  = document.getElementById("video-next");
  const titleEl  = document.getElementById("video-title");
  if (prevBtn) prevBtn.disabled = idx <= 0;
  if (nextBtn) nextBtn.disabled = idx >= queue.length - 1;
  if (titleEl) {
    const meta = (window._videoQueueMeta ?? [])[idx];
    if (meta) {
      const parts = [];
      if (meta.track)  parts.push(`<span class="vt-track">${escHtml(meta.track)}</span>`);
      if (meta.album)  parts.push(`<span>${escHtml(meta.album)}</span>`);
      if (meta.artist) parts.push(`<span>${escHtml(meta.artist)}</span>`);
      titleEl.innerHTML = parts.join(`<span class="vt-sep">·</span>`);
    } else {
      titleEl.innerHTML = "";
    }
  }
}

function openVideo(event, url) {
  if (event) { event.preventDefault(); event.stopPropagation(); }
  ensureYTAPI();
  const id = extractYouTubeId(url);
  if (!id) { window.open(url, "_blank", "noopener"); return; }
  const trackLinks = [...document.querySelectorAll(".track-link[data-video]")];
  window._videoQueue      = trackLinks.map(a => a.dataset.video);
  window._videoQueueMeta  = trackLinks.map(a => ({
    track:  a.dataset.track  || "",
    album:  a.dataset.album  || "",
    artist: a.dataset.artist || "",
  }));
  window._videoQueueIndex = window._videoQueue.indexOf(url);
  if (window._videoQueueIndex === -1) window._videoQueueIndex = 0;
  setVideoUrl(id);
  document.getElementById("video-overlay").classList.add("open");
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
}

function closeVideo() {
  document.getElementById("video-overlay").classList.remove("open");
  if (ytPlayer && typeof ytPlayer.stopVideo === "function") {
    ytPlayer.stopVideo();
    ytPlayer.destroy();
    ytPlayer = null;
  }
  document.getElementById("video-player").innerHTML = "";
  const u = new URL(window.location.href);
  u.searchParams.delete("vd");
  history.replaceState({}, "", u.toString());
}

function extractYouTubeId(url) {
  try {
    const u = new URL(url);
    if (u.hostname === "youtu.be") return u.pathname.slice(1);
    return u.searchParams.get("v");
  } catch { return null; }
}

document.getElementById("video-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("video-overlay")) closeVideo();
});

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
  const labels   = (d.labels ?? []).map(l => l.name).slice(0, 2).join(", ")
                   || (searchResult.label ?? []).slice(0, 2).join(", ");
  const genres   = [...(d.genres ?? []), ...(d.styles ?? [])].slice(0, 4).join(" · ");
  const country  = d.country ?? searchResult.country ?? "";
  const allImages = (d.images ?? []).map(i => i.uri).filter(Boolean);
  if (allImages.length === 0 && searchResult.cover_image) allImages.push(searchResult.cover_image);
  const img      = allImages[0] ?? "";
  const released    = d.released_formatted ?? d.released ?? "";
  const formats     = (d.formats ?? []).map(f =>
    [f.name, ...(f.descriptions ?? [])].filter(Boolean).join(" · ")
  ).join("; ") || (searchResult.format ?? []).join(" · ");
  const credits     = (d.extraartists ?? [])
    .map(a => {
      const nameEl = a.id
        ? `<a href="#" data-alt-name="${escHtml(a.name)}" data-alt-id="${a.id}" onclick="selectAltArtist(event,this);closeModal()" style="color:var(--accent);text-decoration:none">${escHtml(a.name)}</a>`
        : escHtml(a.name);
      return `${nameEl}${a.role ? ` <span class="credit-role">(${escHtml(a.role)})</span>` : ""}`;
    })
    .join(" · ");
  const notes       = d.notes ? stripDiscogsMarkup(d.notes) : "";
  const catno     = (d.labels ?? [])[0]?.catno ?? "";
  const releaseId = d.id ?? searchResult.id ?? "";
  const typeName = targetId === "version-info"
    ? "Version"
    : searchResult.type === "master" ? "Master" : searchResult.type === "release" ? "Release" : "";
  const typeLabel = typeName && releaseId ? `${typeName}: ${releaseId}` : typeName;

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
  const identifierRows = identifierTypes
    .filter(t => identifierGroups[t])
    .map(t => `<span class="detail-label">${escHtml(t)}</span><span>${escHtml(identifierGroups[t])}</span>`)
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

  const seriesText = (d.series ?? [])
    .map(s => s.catno ? `${s.name} (${s.catno})` : s.name)
    .filter(Boolean).join(", ");

  const isMaster = searchResult.type === "master";
  const detailRows = [
    labels  ? `<span class="detail-label">Label</span><span>${escHtml(labels)}</span>`   : "",
    (!isMaster && formats) ? `<span class="detail-label">Format</span><span>${escHtml(formats)}</span>` : "",
    country ? `<span class="detail-label">Country</span><span>${escHtml(country)}</span>` : "",
    genres  ? `<span class="detail-label">Genre</span><span>${escHtml(genres)}</span>`   : "",
    (!isMaster && catno) ? `<span class="detail-label">Cat#</span><span>${escHtml(catno)}</span>` : "",
    year    ? `<span class="detail-label">Year</span><span>${escHtml(String(year))}</span>` : "",
    seriesText ? `<span class="detail-label">Series</span><span>${escHtml(seriesText)}</span>` : "",
    companyRows,
    identifierRows,
  ].filter(Boolean).join("");

  const tracks = (d.tracklist ?? []).filter(t => t.type_ !== "heading");
  const trackHTML = tracks.length ? `
    <div class="album-tracklist">
      <div class="tracklist-heading">Tracklist</div>
      ${tracks.map(t => {
        const url = findVideo(t.title || "");
        const trackArtist = artists.length ? artists[0] : "";
        const ytQuery = encodeURIComponent(`${trackArtist} ${title} ${t.title || ""}`);
        const ytIcon = `<a class="yt-search" href="https://www.youtube.com/results?search_query=${ytQuery}" target="_blank" rel="noopener" title="Search on YouTube"><svg width="16" height="11" viewBox="0 0 16 11" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="16" height="11" rx="2.5" fill="#FF0000"/><path d="M6.5 3L11 5.5L6.5 8V3Z" fill="white"/></svg></a>`;
        const trackSearchQ = ('"' + trackArtist + ' ' + (t.title || '').trim() + '"').replace(/'/g, "\\'");
        const searchIcon = t.title ? ` <a class="track-search-icon" href="#" onclick="event.preventDefault();closeModal();document.getElementById('query').value='${escHtml(trackSearchQ)}';toggleAdvanced(false);document.querySelector('input[name=\\'result-type\\'][value=\\'\\']').checked=true;doSearch(1)" title="Search for other versions" style="text-decoration:none;color:var(--accent);font-size:0.85em">⌕</a>` : "";
        const titleEl = url
          ? `<a class="track-link" href="#" data-video="${escHtml(url)}" data-track="${escHtml(t.title || "")}" data-album="${escHtml(title)}" data-artist="${escHtml(trackArtist)}" onclick="openVideo(event,'${url.replace(/'/g, "\\'")}')">${escHtml(t.title || "")} ▶</a>${searchIcon}`
          : `${escHtml(t.title || "")}${ytIcon}${searchIcon}`;
        return `<div class="track">
          <span class="track-pos">${escHtml(t.position || "")}</span>
          <span class="track-title">${titleEl}</span>
          ${t.duration ? `<span class="track-dur">${escHtml(t.duration)}</span>` : ""}
        </div>`;
      }).join("")}
    </div>` : "";

  const metaRows = [
    released ? `<div class="album-meta-row"><span class="meta-label">Released</span><span>${escHtml(released)}</span></div>` : "",
    (!isMaster && formats) ? `<div class="album-meta-row"><span class="meta-label">Format</span><span>${escHtml(formats)}</span></div>` : "",
    credits  ? `<div class="album-meta-row"><span class="meta-label">Credits</span><span>${credits}</span></div>` : "",
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
        <h2>${escHtml(title)} <a href="#" class="album-title-search" onclick="event.preventDefault();closeModal();document.getElementById('query').value='${escHtml(title.replace(/'/g, "\\'"))}';toggleAdvanced(false);document.querySelector('input[name=\\'result-type\\'][value=\\'\\']').checked=true;doSearch(1)" title="Search for other versions">⌕</a>${window._collectionIds?.has(Number(releaseId)) ? ` <span class="collection-badge" title="In your collection">✓</span>` : ""}${window._wantlistIds?.has(Number(releaseId)) ? ` <span class="wantlist-badge" title="In your wantlist">♡</span>` : ""}</h2>
        ${artists.length ? `<div class="album-artist">${artists.map(n => `<a href="#" class="modal-artist-link" data-artist="${escHtml(n)}" onclick="searchArtistFromModal(event,this)">${escHtml(n)}</a>`).join(", ")}</div>` : ""}
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
        ${discogsUrl ? `<a href="${discogsUrl}" target="_blank" rel="noopener" style="font-size:0.75rem;color:var(--accent);text-decoration:none;margin-top:0.4rem;display:inline-block">View on Discogs ↗</a>` : ""}
        ${stats?.numForSale > 0 && stats?.lowestPrice != null
          ? `<a href="https://www.discogs.com/sell/list?release_id=${escHtml(String(stats.releaseId))}" target="_blank" rel="noopener" style="font-size:0.75rem;color:#888;text-decoration:none;margin-top:0.2rem;display:block">${escHtml(String(stats.numForSale))} available from $${parseFloat(stats.lowestPrice).toFixed(2)}</a>`
          : (stats?.numForSale === 0 ? `<div style="font-size:0.75rem;color:#555;margin-top:0.2rem">Not currently available on Discogs marketplace</div>` : "")
        }
        ${artists.length ? `<a href="#" onclick="openConcertPopup(event,'${escHtml(artists[0]).replace(/'/g, "\\'")}')" style="font-size:0.75rem;color:#5a9aaa;text-decoration:none;margin-top:0.2rem;display:block">Concerts ♪</a>` : ""}
      </div>
    </div>
    ${trackHTML}
    ${metaRows ? `<div class="album-extra">${metaRows}</div>` : ""}
    ${isMaster ? `<div id="master-versions-list" style="padding:0.75rem 1rem 0.5rem;font-size:0.78rem;color:var(--muted)">Loading pressings…</div>` : ""}`;

  if (isMaster) loadMasterVersions(null, searchResult.id);
}

let _masterVersions = [];

function renderMasterVersions(filter) {
  const list = document.getElementById("master-versions-list");
  if (!list) return;
  const MEDIA = new Set(["Vinyl","CD","Cassette","DVD","Blu-ray","File","Box Set","Lathe Cut","Flexi-disc","Shellac","8-Track Cartridge","Reel-To-Reel","MiniDisc","SACD","Betamax","VHS"]);
  const getMedium = v => {
    const parts = (v.format ?? "").split(",").map(s => s.trim());
    return parts.find(p => MEDIA.has(p)) || (v.majorFormats ?? []).find(f => MEDIA.has(f)) || parts[0] || "";
  };
  const getDisplayFormat = v => {
    const fmt = (v.format ?? "").trim();
    const medium = (v.majorFormats ?? []).find(f => MEDIA.has(f));
    if (!fmt) return medium || "—";
    if (!medium || fmt.split(",").map(s => s.trim()).includes(medium)) return fmt;
    return `${medium}, ${fmt}`;
  };
  const filtered = filter ? _masterVersions.filter(v => getMedium(v) === filter) : _masterVersions;

  list.querySelectorAll(".mv-filter-pill").forEach(p => {
    p.style.background = p.dataset.filter === (filter ?? "") ? "var(--accent)" : "#2a2a2a";
    p.style.color      = p.dataset.filter === (filter ?? "") ? "#000" : "var(--fg)";
  });

  const grid = list.querySelector(".mv-grid");
  if (!grid) return;
  if (!filtered.length) { grid.innerHTML = `<span style="color:var(--muted);grid-column:1/-1">No pressings match this filter.</span>`; return; }
  grid.innerHTML = filtered.map(v => {
    const inCol  = window._collectionIds?.has(v.id);
    const inWant = window._wantlistIds?.has(v.id);
    const badge  = inCol  ? `<span class="collection-badge">✓</span>` :
                   inWant ? `<span class="wantlist-badge">♡</span>` : "";
    return `
      <span style="color:#888">${escHtml(!v.year || v.year === "0" ? "?" : String(v.year))}</span>
      <span style="color:#aaa">${escHtml(v.country || "?")}</span>
      <span style="color:#888">${escHtml(getDisplayFormat(v))}</span>
      <span style="color:#aaa">${escHtml(v.catno ?? "—")}</span>
      <span><a href="#" onclick="openVersionPopup(event,${v.id})" style="color:var(--accent);text-decoration:none">${escHtml(v.label ?? v.title ?? "—")}</a>${badge}</span>`;
  }).join("");
}

async function loadMasterVersions(event, masterId) {
  if (event) event.preventDefault();
  const list = document.getElementById("master-versions-list");
  if (!list) return;
  try {
    const data = await apiFetch(`${API}/master-versions/${masterId}`).then(r => r.json());
    _masterVersions = data.versions ?? [];
    if (!_masterVersions.length) { list.textContent = "No pressings found."; return; }

    const MEDIA2 = new Set(["Vinyl","CD","Cassette","DVD","Blu-ray","File","Box Set","Lathe Cut","Flexi-disc","Shellac","8-Track Cartridge","Reel-To-Reel","MiniDisc","SACD","Betamax","VHS"]);
    const getMedium2 = v => { const parts = (v.format ?? "").split(",").map(s => s.trim()); return parts.find(p => MEDIA2.has(p)) || (v.majorFormats ?? []).find(f => MEDIA2.has(f)) || parts[0] || ""; };
    const formatSet = new Set();
    _masterVersions.forEach(v => {
      const medium = getMedium2(v);
      if (medium) formatSet.add(medium);
    });
    const formats = [...formatSet].sort();
    const pillStyle = `cursor:pointer;border:none;border-radius:20px;padding:0.15rem 0.6rem;font-size:0.72rem;font-weight:600;transition:background 0.15s`;
    const pills = [
      `<button class="mv-filter-pill" data-filter="" onclick="renderMasterVersions('')" style="${pillStyle};background:var(--accent);color:#000">All</button>`,
      ...formats.map(f => `<button class="mv-filter-pill" data-filter="${escHtml(f)}" onclick="renderMasterVersions('${f.replace(/'/g,"\\'")}')\" style="${pillStyle};background:#2a2a2a;color:var(--fg)">${escHtml(f)}</button>`)
    ].join("");

    list.innerHTML = `
      <div style="font-size:0.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.4rem">Pressings / Versions</div>
      ${formats.length > 1 ? `<div style="display:flex;flex-wrap:wrap;gap:0.35rem;margin-bottom:0.6rem">${pills}</div>` : ""}
      <div class="mv-grid" style="display:grid;grid-template-columns:auto auto auto auto 1fr;gap:0.2rem 0.7rem;font-size:0.75rem"></div>`;
    renderMasterVersions("");
  } catch(e) {
    list.textContent = "Failed to load pressings.";
  }
}
