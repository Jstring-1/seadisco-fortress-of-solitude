// ── Advanced panel toggle ─────────────────────────────────────────────────
function toggleAdvanced(forceOpen) {
  const isAi = document.querySelector('input[name="result-type"]:checked')?.value === "ai";
  if (isAi) {
    const btn = document.getElementById("advanced-toggle");
    const existing = document.getElementById("ai-adv-hint");
    if (!existing) {
      const hint = document.createElement("span");
      hint.id = "ai-adv-hint";
      hint.textContent = "Not available in AI mode";
      hint.style.cssText = "font-size:0.72rem;color:#888;margin-left:0.5rem;transition:opacity 1.5s";
      btn.parentNode.insertBefore(hint, btn.nextSibling);
      setTimeout(() => hint.style.opacity = "0", 1500);
      setTimeout(() => hint.remove(), 3000);
    }
    return;
  }
  const panel = document.getElementById("advanced-panel");
  const arrow = document.getElementById("advanced-arrow");
  const open  = forceOpen !== undefined ? forceOpen : panel.dataset.open !== "true";
  panel.dataset.open = open ? "true" : "false";
  arrow.textContent  = open ? "▼" : "▶";
}

// ── URL state helpers ────────────────────────────────────────────────────
function pushSearchState(q, artistRaw, release, year, label, genre, sort, resultType, page) {
  const p = new URLSearchParams();
  if (q)          p.set("q",  q);
  if (artistRaw)  p.set("ar", artistRaw);
  if (release)    p.set("re", release);
  if (year)       p.set("yr", year);
  if (label)      p.set("lb", label);
  if (genre)      p.set("gn", genre);
  const style  = document.getElementById("f-style")?.value ?? "";
  const format = document.getElementById("f-format")?.value ?? "";
  if (style)      p.set("st", style);
  if (format) p.set("fm", format);
  if (sort)       p.set("sr", sort);
  if (resultType) p.set("rt", resultType);
  if (page > 1)   p.set("pg", String(page));
  history.pushState({}, "", p.toString() ? "?" + p.toString() : location.pathname);
}

function restoreFromParams(p) {
  document.getElementById("query").value     = p.get("q")  ?? "";
  document.getElementById("f-artist").value  = p.get("ar") ?? "";
  document.getElementById("f-release").value = p.get("re") ?? "";
  document.getElementById("f-year").value    = p.get("yr") ?? "";
  document.getElementById("f-label").value   = p.get("lb") ?? "";
  document.getElementById("f-genre").value   = p.get("gn") ?? "";
  const sortEl = document.getElementById("f-sort");
  sortEl.value = p.get("sr") ?? "";
  if (!sortEl.value) sortEl.selectedIndex = 0;
  document.getElementById("f-format").value  = p.get("fm") || "";
  populateStyles();
  document.getElementById("f-style").value   = p.get("st") ?? "";
  const rtype = p.get("rt") ?? "";
  const radio = document.querySelector(`input[name="result-type"][value="${rtype}"]`);
  if (radio) radio.checked = true;
  const hasAdvanced = p.get("ar") || p.get("re") || p.get("yr") || p.get("lb") || p.get("gn") || p.get("st") || p.get("fm");
  if (hasAdvanced) toggleAdvanced(true);
}

function clearForm() {
  ["query","f-artist","f-release","f-year","f-label","f-genre","f-style"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });
  document.getElementById("f-sort").value = "";
  document.getElementById("f-format").value = "";
  document.getElementById("f-style").innerHTML = '<option value="">Any</option>';
  document.getElementById("f-style").disabled = true;
  document.querySelector('input[name="result-type"][value=""]').checked = true;

  document.getElementById("search-desc").textContent = "";
  document.getElementById("search-returned").textContent = "";
  document.getElementById("search-ai-summary").textContent = "";
  document.getElementById("search-info-block").style.display = "none";
}

// ── Main search ──────────────────────────────────────────────────────────
async function doSearch(page = 1, skipPushState = false) {
  const q         = document.getElementById("query").value.trim();
  const advOpen   = document.getElementById("advanced-panel")?.dataset.open === "true";
  const artistRaw = advOpen ? document.getElementById("f-artist").value.trim() : "";
  const artist    = artistRaw.replace(/\s*\(\d+\)$/, "");
  const release   = advOpen ? document.getElementById("f-release").value.trim() : "";
  const year      = advOpen ? document.getElementById("f-year").value.trim() : "";
  const label     = advOpen ? document.getElementById("f-label").value.trim() : "";
  const genre     = advOpen ? document.getElementById("f-genre").value.trim() : "";
  const style     = advOpen ? (document.getElementById("f-style")?.value.trim() ?? "") : "";
  const format    = advOpen ? document.getElementById("f-format").value : "";
  const sort      = document.getElementById("f-sort").value;
  const resultType = document.querySelector('input[name="result-type"]:checked')?.value ?? "";

  if (resultType === "ai") { doAiSearch(q); return; }

  if (!q && !artist && !release && !year && !label && !genre) {
    setStatus("Enter a search term or fill in at least one filter.", false);
    return;
  }

  switchView("search", true);
  setActiveTab("search");

  if (!skipPushState) pushSearchState(q, artistRaw, release, year, label, genre, sort, resultType, page);

  if (page === 1) detectedArtist = null;

  currentPage = page;
  document.getElementById("search-btn").disabled = true;
  document.getElementById("pagination").style.display = "none";
  document.getElementById("blurb").style.display = "none";
  document.getElementById("artist-alts").innerHTML = "";
  closeAltsPopup();
  setStatus("Searching…");
  document.getElementById("results").innerHTML = "";

  if (page === 1) {
    const parts = [];
    if (q)       parts.push(q);
    if (artist)  parts.push(`Artist: ${artist}`);
    if (release) parts.push(`Release: ${release}`);
    if (year)    parts.push(`Year: ${year}`);
    if (label)   parts.push(`Label: ${label}`);
    if (genre)   parts.push(`Genre: ${genre}`);
    if (style)   parts.push(`Style: ${style}`);
    if (format)  parts.push(`Format: ${format}`);
    const typeLabels = { "master":"Masters", "release":"Releases", "artist":"Artists", "label":"Labels" };
    const typeLabel = typeLabels[resultType] ?? "";
    const sortLabels = { "year:asc":"Year ↑", "year:desc":"Year ↓", "title:asc":"Title A→Z", "title:desc":"Title Z→A", "label:asc":"Label A→Z" };
    const sortLabel = sortLabels[sort] ?? "";
    const extras = [typeLabel, sortLabel].filter(Boolean).join(" · ");
    const searchTerms = parts.join(", ") + (extras ? "  ·  " + extras : "");
    const descEl = document.getElementById("search-desc");
    if (parts.length) {
      descEl.innerHTML = `Searched :: <span onclick="copySearchUrl(this)" title="Click to copy search link" style="cursor:pointer;border-bottom:1px dotted transparent;transition:border-color 0.2s" onmouseover="this.style.borderBottomColor='var(--accent)'" onmouseout="this.style.borderBottomColor='transparent'">${escHtml(searchTerms)}</span>`;
    } else {
      descEl.textContent = "";
    }
    document.getElementById("search-returned").textContent = "";
    document.getElementById("search-ai-summary").textContent = "";
    if (parts.length) document.getElementById("search-info-block").style.display = "";
  }

  const buildParams = (perPage) => {
    const effectiveArtist = artist || (page > 1 ? detectedArtist : null) || "";
    const p = new URLSearchParams({ page, per_page: perPage });
    if (q) p.set("q", q);
    if (resultType) p.set("type", resultType);
    if (effectiveArtist) p.set("artist", effectiveArtist);
    if (release) p.set("release_title", release);
    if (year)    p.set("year",          year);
    if (label)   p.set("label",         label);
    if (genre)   p.set("genre",         genre);
    if (style)   p.set("style",         style);
    if (format)  p.set("format",        format);
    if (sort) {
      const [sortField, sortOrder] = sort.split(":");
      p.set("sort",       sortField);
      p.set("sort_order", sortOrder);
    }
    return p;
  };

  try {
    let bioFetch = null;
    if (page === 1) {
      if (artistRaw) {
        const bioUrl = `${API}/artist-bio?name=${encodeURIComponent(artistRaw)}`
                     + (currentArtistId ? `&id=${currentArtistId}` : "");
        bioFetch = apiFetch(bioUrl).catch(() => null);
      } else if (label) {
        bioFetch = apiFetch(`${API}/label-bio?name=${encodeURIComponent(label)}`).catch(() => null);
      } else if (genre) {
        bioFetch = apiFetch(`${API}/genre-info?genre=${encodeURIComponent(genre)}`).catch(() => null);
      }
    }

    let items, totalPages_new, totalItems_new = 0;
    const [res, bioRes] = await Promise.all([
      apiFetch(`${API}/search?${buildParams(24)}`),
      bioFetch ?? Promise.resolve(null),
    ]);
    bioFetch = bioRes ? { json: () => bioRes.json() } : null;
    if (res.status === 401 || res.status === 429) {
      const errData = await res.json().catch(() => ({}));
      if (errData.error === "no_token") {
        document.getElementById("status").innerHTML =
          `<a href="/account" style="color:var(--accent)">Sign in and add your Discogs token</a> to start searching.`;
        return;
      }
      if (errData.error === "rate_limited") {
        document.getElementById("status").innerHTML =
          `You've used your 5 free searches for today. <a href="/account" style="color:var(--accent)">Add your Discogs token</a> for unlimited searches.`;
        return;
      }
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    items = data.results ?? [];
    totalPages_new = data.pagination?.pages ?? 1;
    totalItems_new = data.pagination?.items ?? items.length;
    totalPages = totalPages_new;

    const blurbEl = document.getElementById("blurb");
    if (page === 1) {
      let bioData = null;
      if (bioFetch) {
        bioData = await bioFetch.json().catch(() => null);
      }
      if (!bioData && !q && !artist && !release && !label && !genre && items.length > 0) {
        const firstMedia = items.find(it => (it.type === "release" || it.type === "master") && it.title?.includes(" - "));
        if (firstMedia) {
          const derivedArtist = firstMedia.title.slice(0, firstMedia.title.indexOf(" - "));
          bioData = await apiFetch(`${API}/artist-bio?name=${encodeURIComponent(derivedArtist)}`).then(r => r.json()).catch(() => null);
        }
      }
      const alts = (bioData?.alternatives ?? []).filter(a => a.name);
      document.getElementById("artist-alts").innerHTML = "";
      if (alts.length > 0) {
        const popupEl = document.getElementById("alts-popup");
        popupEl.innerHTML = `<h4>Other artists</h4>` +
          alts.map(a => `<a href="#" data-alt-name="${escHtml(a.name)}"${a.id ? ` data-alt-id="${a.id}"` : ""} onclick="selectAltArtist(event,this);closeAltsPopup()">${escHtml(a.name)}</a>`).join("");
      } else {
        document.getElementById("alts-popup").innerHTML = "<h4>Other artists</h4>";
      }

      const firstMediaItem = items.find(it => (it.type === "release" || it.type === "master") && it.title?.includes(" - "));
      const searchedArtistName = artistRaw || (!label && !genre && firstMediaItem
        ? firstMediaItem.title.slice(0, firstMediaItem.title.indexOf(" - ")) : "");
      if (bioData && artistNamesMatch(searchedArtistName, bioData.name) === false) {
        bioData = null;
      }

      if (bioData?.name && !artistRaw && !release && !label && !genre && page === 1) {
        const constrainedArtist = bioData.name.replace(/\s*\(\d+\)$/, "").trim();
        detectedArtist = constrainedArtist;
        try {
          const cp = new URLSearchParams({ q: q || constrainedArtist, page, per_page: 48, artist: constrainedArtist });
          if (resultType) cp.set("type", resultType);
          if (release)    cp.set("release_title", release);
          if (year)       cp.set("year", year);
          if (format)     cp.set("format", format);
          if (sort) { const [sf, so] = sort.split(":"); cp.set("sort", sf); cp.set("sort_order", so); }
          const cr = await apiFetch(`${API}/search?${cp}`);
          if (cr.ok) {
            const cd = await cr.json();
            if ((cd.results ?? []).length > 0) {
              items = cd.results;
              totalPages = cd.pagination?.pages ?? totalPages;
              totalItems_new = cd.pagination?.items ?? totalItems_new;
            }
          }
        } catch { /* keep original results */ }
      }

      if (bioData?.profile) {
        window._currentBio = {
          name: bioData.name, text: bioData.profile ?? null,
          members:        bioData.members        ?? [],
          groups:         bioData.groups         ?? [],
          aliases:        bioData.aliases        ?? [],
          namevariations: bioData.namevariations ?? [],
          urls:           bioData.urls           ?? [],
          parentLabel:    bioData.parentLabel    ?? null,
          sublabels:      bioData.sublabels      ?? [],
          discogsId: bioData.discogsId ?? null,
          alternatives:   alts,
        };

        const rawBioText  = bioData.profile ?? null;
        const displayText = rawBioText ? stripDiscogsMarkup(rawBioText) : "";
        const TRUNCATE = 552;
        const needsMore = displayText.length > TRUNCATE;
        const truncatedRaw = rawBioText
          ? (needsMore ? truncateRaw(rawBioText, TRUNCATE) + '\u2026' : rawBioText)
          : "";

        const readMore = needsMore
          ? ` <a href="#" onclick="openBioFull(event)" style="font-size:0.8rem;color:var(--accent);white-space:nowrap;text-decoration:none">read more</a>`
          : "";
        const heading = bioData.name
          ? `<strong style="display:block;margin-bottom:0.4rem;color:var(--accent)">${escHtml(bioData.name)}</strong>`
          : "";

        const relLinks = renderArtistRelations(
          bioData.members        ?? [], bioData.groups    ?? [], bioData.aliases  ?? [],
          bioData.namevariations ?? [], [],
          bioData.parentLabel    ?? null, bioData.sublabels ?? [], true
        );
        const bioHtml  = rawBioText ? renderBioMarkup(truncatedRaw) : escHtml(truncatedRaw);
        blurbEl.innerHTML = heading + bioHtml + readMore + relLinks;
        blurbEl.style.display = "block";

        apiFetch("/api/user/mb", { method: "POST" }).catch(() => {});
        const bu = new URL(window.location.href);
        if (!bu.searchParams.has("b")) { bu.searchParams.set("b", "y"); history.replaceState({}, "", bu.toString()); }
      }
    }

    if (!items.length) {
      setStatus("");
      document.getElementById("search-ai-summary").innerHTML = "<i>Couldn't find any results at Discogs.</i>";
      document.getElementById("search-info-block").style.display = "";
      return;
    }

    setStatus("");
    const returnedMsg = `Returned :: ${totalItems_new.toLocaleString()} results — page ${currentPage} of ${totalPages}`;
    document.getElementById("search-returned").textContent = returnedMsg;
    document.getElementById("search-info-block").style.display = "";
    renderResults(items);
    renderPagination();

    {
      const qualityQuery = [
        q,
        artist  ? `Artist: ${artist}`   : "",
        release ? `Release: ${release}` : "",
        year    ? `Year: ${year}`       : "",
        label   ? `Label: ${label}`     : "",
        genre   ? `Genre: ${genre}`     : "",
        style   ? `Style: ${style}`     : "",
      ].filter(Boolean).join(", ");
      const qualityTitles = items.slice(0, 6).map(it => it.title ?? it.name ?? "").filter(Boolean);
      if (qualityQuery && qualityTitles.length) {
        const aiEl = document.getElementById("search-ai-summary");
        if (aiEl) aiEl.textContent = "…";
        fetch("/api/result-quality", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query: qualityQuery, titles: qualityTitles }),
        }).then(r => r.json()).then(d => {
          if (aiEl) aiEl.textContent = d.phrase || "";
        }).catch(() => {
          if (aiEl) aiEl.textContent = "";
        });
      }
    }

    if (typeof gtag === "function") {
      gtag("event", "page_view", {
        page_location: window.location.href,
        page_path:     window.location.pathname + window.location.search,
        page_title:    document.title
      });
      gtag("event", "search", {
        search_term: [q, artist, label, genre].filter(Boolean).join(" | ") || "(filters only)"
      });
    }

    const urlP = new URLSearchParams(location.search);
    const openParam  = urlP.get("op");
    const videoParam = urlP.get("vd");
    if (openParam) {
      const colon = openParam.indexOf(":");
      const pType = openParam.slice(0, colon);
      const pId   = openParam.slice(colon + 1);
      const pUrl  = `https://www.discogs.com/${pType}/${pId}`;
      openModal(null, pId, pType, pUrl);
      const versionParam = urlP.get("vr");
      if (versionParam) {
        setTimeout(() => openVersionPopup(null, versionParam), 1200);
      }
      if (videoParam) {
        setTimeout(() => openVideo(null, `https://www.youtube.com/watch?v=${videoParam}`), 1200);
      }
    } else if (urlP.get("bi") === "1") {
      openBioFull(null);
    } else if (videoParam) {
      openVideo(null, `https://www.youtube.com/watch?v=${videoParam}`);
    }
  } catch (e) {
    setStatus("Search failed: " + e.message, true);
  } finally {
    document.getElementById("search-btn").disabled = false;
  }
}

// ── Render cards ──────────────────────────────────────────────────────────
function renderResults(items) {
  window._lastResults = items;
  const hideOwned = document.getElementById("hide-owned")?.checked;
  const filtered = hideOwned && window._collectionIds?.size
    ? items.filter(item => !window._collectionIds.has(Number(item.id)))
    : items;
  const grid = document.getElementById("results");
  grid.innerHTML = filtered.map(item => renderCard(item)).join("");
  // Hide wanted sample when showing search results
  const ws = document.getElementById("wanted-sample"); if (ws) ws.style.display = "none";
}

function renderCard(item) {
  const url  = item.uri ? `https://www.discogs.com${item.uri}` : "#";
  const type = item.type ?? "";

  let artist = "";
  let title  = item.title ?? "Unknown";
  if ((type === "release" || type === "master") && title.includes(" - ")) {
    const idx = title.indexOf(" - ");
    artist = title.slice(0, idx);
    title  = title.slice(idx + 3);
  }

  const label   = (item.label   ?? []).slice(0, 2).join(", ");
  const catno   = (type === "release" || type === "master") ? (item.catno ?? "") : "";
  const formats = (item.format  ?? []).slice(0, 3).join(" · ");
  const genre   = (item.genre   ?? []).slice(0, 1).join("");
  const country = item.country ?? "";
  const year    = item.year    ?? "";

  const metaParts = [year, type, country].filter(Boolean);

  const thumb = item.cover_image
    ? `<img src="${item.cover_image}" alt="${escHtml(title)}" loading="lazy" />`
    : `<div class="thumb-placeholder">♪</div>`;

  const isRelease = type === "release" || type === "master";
  const isArtist  = type === "artist";
  const isLabel   = type === "label";
  if (isRelease) itemCache.set(String(item.id), item);
  const typeClass = `card card-type-${type}`;
  const cardAttrs = isRelease
    ? `class="${typeClass}" href="#" onclick="openModal(event,'${item.id}','${type}','${url.replace(/'/g, "\\'")}')" `
    : (isArtist || isLabel)
      ? `class="${typeClass}" href="#" data-entity-type="${escHtml(type)}" data-entity-name="${escHtml(title)}" data-entity-id="${item.id}" onclick="searchByEntity(event,this)"`
      : `class="${typeClass}" href="${url}" target="_blank" rel="noopener"`;

  let badges = "";
  const releaseId = item.id;
  if (releaseId) {
    if (window._collectionIds?.has(releaseId)) badges += `<span class="collection-badge" title="In your collection">✓</span>`;
    if (window._wantlistIds?.has(releaseId))   badges += `<span class="wantlist-badge" title="In your wantlist">♡</span>`;
  }

  const thumbWrap = `<div class="card-thumb-wrap">${thumb}${badges ? `<div class="card-thumb-badges">${badges}</div>` : ""}</div>`;

  // Rating stars (only for collection/wantlist cards)
  const rating = item._rating ?? 0;
  const ratingHtml = rating > 0
    ? `<div class="card-rating">${"★".repeat(rating)}${"☆".repeat(5 - rating)}</div>`
    : "";

  // Notes indicator (only for collection/wantlist cards with notes)
  const notes = item._notes ?? [];
  const hasNotes = notes.length > 0 && notes.some(n => n.value);
  let notesHtml = "";
  if (hasNotes) {
    // Store notes for popup lookup
    if (!window._cardNotes) window._cardNotes = {};
    window._cardNotes[releaseId] = notes;
    notesHtml = `<div class="card-notes-btn" onclick="event.preventDefault();event.stopPropagation();showCardNotes(event,${releaseId})" title="View notes">📝</div>`;
  }

  return `
    <a ${cardAttrs}>
      ${thumbWrap}
      <div class="card-body">
        ${artist ? `<div class="card-artist">${escHtml(artist)}</div>` : ""}
        <div class="card-title">${escHtml(title)}</div>
        ${label   ? `<div class="card-sub">${escHtml(label)}</div>` : ""}
        ${formats ? `<div class="card-format">${escHtml(formats)}</div>` : ""}
        ${genre   ? `<div class="card-format">${escHtml(genre)}</div>`   : ""}
        ${catno   ? `<div class="card-catno-line">${escHtml(catno)}</div>` : ""}
        ${ratingHtml}
        <div class="card-meta">${metaParts.map(escHtml).join(" · ")}</div>
        ${notesHtml}
      </div>
    </a>`;
}

// ── Card notes popup ─────────────────────────────────────────────────────
function showCardNotes(event, releaseId) {
  const notes = window._cardNotes?.[releaseId];
  if (!notes) return;
  let popup = document.getElementById("card-notes-popup");
  if (!popup) {
    popup = document.createElement("div");
    popup.id = "card-notes-popup";
    popup.style.cssText = "position:absolute;z-index:600;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:0.6rem 0.85rem;max-width:280px;font-size:0.78rem;line-height:1.6;box-shadow:0 4px 20px rgba(0,0,0,0.6);display:none";
    popup.onclick = e => e.stopPropagation();
    document.body.appendChild(popup);
    document.addEventListener("click", () => { popup.style.display = "none"; });
  }
  const rows = notes.filter(n => n.value).map(n =>
    `<div style="margin-bottom:0.3rem"><span style="color:#777">${escHtml(n.field_name ?? n.field_id ?? "Note")}:</span> <span style="color:#ccc">${escHtml(n.value)}</span></div>`
  ).join("");
  popup.innerHTML = rows || `<div style="color:#555">No notes</div>`;
  const rect = event.target.getBoundingClientRect();
  popup.style.display = "block";
  popup.style.top  = (rect.bottom + window.scrollY + 4) + "px";
  popup.style.left = Math.min(rect.left + window.scrollX, window.innerWidth - 290) + "px";
}

// ── Pagination ────────────────────────────────────────────────────────────
function renderPagination() {
  if (totalPages <= 1) return;
  document.getElementById("pagination").style.display = "flex";
  document.getElementById("page-info").textContent = `${currentPage} / ${totalPages}`;
  document.getElementById("prev-btn").disabled = currentPage <= 1;
  document.getElementById("next-btn").disabled = currentPage >= totalPages;
}

// ── AI search ─────────────────────────────────────────────────────────────
async function doAiSearch(q) {
  if (!q) { setStatus("Enter a question or description to search with AI.", false); return; }
  switchView("search", true);
  setActiveTab("search");
  const blurbEl = document.getElementById("blurb");
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  document.getElementById("artist-alts").innerHTML = "";
  blurbEl.style.display = "none";
  setStatus("Asking Claude…");
  document.getElementById("search-btn").disabled = true;
  try {
    const r = await apiFetch("/api/ai-search", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ q }) });
    if (r.status === 401) { setStatus("Sign in and add your Discogs token to use AI search.", false); return; }
    if (!r.ok) { setStatus("AI search failed. Try again.", false); return; }
    const { recommendations, blurb } = await r.json();
    if (!recommendations?.length) { setStatus("No recommendations returned.", false); return; }
    setStatus("");

    blurbEl.innerHTML = `
      <div style="margin-bottom:1rem">
        <div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:0.1em;color:#666;margin-bottom:0.4rem">✦ AI Recommendations for "${escHtml(q)}"</div>
        ${blurb ? `<div style="font-size:0.85rem;color:var(--muted);font-style:italic;margin-bottom:0.75rem">${escHtml(blurb)}</div>` : ""}
        ${recommendations.map(rec => {
          const params = new URLSearchParams();
          const p = rec.discogsParams ?? {};
          if (rec.type === "artist" && p.artist) {
            params.set("ar", p.artist);
          } else if (rec.type === "release" && p.artist) {
            params.set("ar", p.artist);
            if (p.q) params.set("re", p.q);
          } else {
            if (p.q)      params.set("q",  p.q);
            if (p.artist) params.set("ar", p.artist);
          }
          if (p.label)  params.set("lb", p.label);
          if (p.genre)  params.set("gn", p.genre);
          if (p.style)  params.set("st", p.style);
          const yr = p.year ? String(p.year).split(/[-–]/)[0].trim() : "";
          if (/^\d{4}$/.test(yr)) params.set("yr", yr);
          const href = "/?" + params.toString();
          return `<div style="padding:0.75rem 0;border-bottom:1px solid #222">
            <div style="display:flex;justify-content:space-between;align-items:baseline;gap:1rem">
              <span style="color:var(--fg);font-weight:600">${rec.name}</span>
              <a href="${href}" style="font-size:0.78rem;color:var(--accent);white-space:nowrap;flex-shrink:0">New Search →</a>
            </div>
            <div style="font-size:0.83rem;color:var(--muted);margin-top:0.25rem">${rec.description}</div>
          </div>`;
        }).join("")}
      </div>`;
    blurbEl.style.display = "block";

    if (typeof gtag === "function") {
      gtag("event", "page_view", {
        page_location: window.location.href,
        page_title: document.title
      });
      gtag("event", "ai_search", { search_term: q });
    }
  } catch (err) {
    setStatus("AI search error: " + err.message, false);
  } finally {
    document.getElementById("search-btn").disabled = false;
  }
}

// ── Recent searches feed ─────────────────────────────────────────────────
let _recentSearches = [];

async function loadRecentFeed() {
  try {
    const data = await fetch("/api/recent-searches").then(r => r.json());
    const searches = data.searches ?? [];
    const el = document.getElementById("recent-feed");
    if (!el) return;
    if (!searches.length) { el.style.display = "none"; return; }
    const filtered = searches.filter(s => {
      const { full } = feedLabel(s.params);
      return full !== "Search";
    });
    if (!filtered.length) { el.style.display = "none"; return; }
    _recentSearches = filtered;
    const pillsHtml = `<div class="feed-label">Recent Searches</div><div class="feed-pills">${
      filtered.map((s, i) => {
        const { full, short } = feedLabel(s.params);
        return `<span class="pill feed-pill" data-idx="${i}" title="${escHtml(full)}">${escHtml(short)}</span>`;
      }).join("")
    }</div>`;
    el.style.opacity = "0";
    el.innerHTML = pillsHtml;
    el.querySelectorAll(".feed-pill").forEach((pill, i) => {
      pill.addEventListener("click", () => feedApply(filtered[i].params));
    });
    requestAnimationFrame(() => { el.style.opacity = "1"; });
  } catch {
    const el = document.getElementById("recent-feed");
    if (el) el.style.display = "none";
  }
}

// ── Wanted sample cards for Find page filler ─────────────────────────────
let _wantedSampleAll = [];
const WANTED_SAMPLE_PAGE = 16;

async function loadWantedSample() {
  try {
    const r = await fetch("/api/wanted-sample");
    if (!r.ok) return;
    const data = await r.json();
    _wantedSampleAll = data.items ?? [];
    if (!_wantedSampleAll.length) return;
    const wrap = document.getElementById("wanted-sample");
    if (!wrap) return;
    _renderWantedSlice(WANTED_SAMPLE_PAGE);
    wrap.style.display = "";
  } catch { /* silent fail */ }
}

function _renderWantedSlice(count) {
  const grid = document.getElementById("wanted-sample-grid");
  if (!grid) return;
  const slice = _wantedSampleAll.slice(0, count);
  let html = slice.map(item => renderCardFromBasicInfo(item)).join("");
  if (count < _wantedSampleAll.length) {
    html += `<div class="wanted-load-more" style="grid-column:1/-1;text-align:center;padding:0.5rem 0">
      <a href="#" onclick="event.preventDefault();_renderWantedSlice(${count + WANTED_SAMPLE_PAGE})" style="color:var(--accent);text-decoration:none;font-size:0.82rem">Load more →</a>
    </div>`;
  }
  grid.innerHTML = html;
}

// ── Artist / entity navigation ───────────────────────────────────────────
function searchArtistFromModal(event, el) {
  event.preventDefault();
  closeModal();
  document.getElementById("f-artist").value  = el.dataset.artist;
  document.getElementById("query").value     = "";
  document.getElementById("f-release").value = "";
  document.getElementById("f-year").value    = "";
  document.getElementById("f-label").value   = "";
  document.getElementById("f-genre").value   = "";
  toggleAdvanced(true);
  doSearch(1);
}

function selectAltArtist(event, el) {
  event.preventDefault();
  document.getElementById("f-artist").value = el.dataset.altName;
  document.getElementById("query").value = "";
  currentArtistId = el.dataset.altId || null;
  toggleAdvanced(true);
  doSearch(1);
}

function openAltsPopup(event) {
  event.preventDefault();
  event.stopPropagation();
  document.getElementById("alts-popup").classList.add("open");
  document.getElementById("alts-popup-backdrop").classList.add("open");
}

function closeAltsPopup() {
  document.getElementById("alts-popup").classList.remove("open");
  document.getElementById("alts-popup-backdrop").classList.remove("open");
}

function searchByEntity(event, el) {
  event.preventDefault();
  switchView("search", true);
  const type = el.dataset.entityType;
  const name = el.dataset.entityName;
  document.getElementById("query").value = "";
  document.getElementById("f-artist").value  = "";
  document.getElementById("f-release").value = "";
  document.getElementById("f-year").value    = "";
  document.getElementById("f-label").value   = "";
  document.getElementById("f-genre").value   = "";
  if (type === "artist") {
    document.getElementById("f-artist").value = name;
    currentArtistId = el.dataset.entityId || null;
  }
  if (type === "label")  document.getElementById("f-label").value  = name;
  toggleAdvanced(true);
  doSearch(1);
}

function searchBioArtist(event, el) {
  event.preventDefault();
  closeBioFull();
  switchView("search", true);
  document.getElementById("f-artist").value  = el.dataset.artist;
  document.getElementById("query").value     = "";
  document.getElementById("f-release").value = "";
  document.getElementById("f-year").value    = "";
  document.getElementById("f-label").value   = "";
  document.getElementById("f-genre").value   = "";
  currentArtistId = el.dataset.artistId || null;
  toggleAdvanced(true);
  doSearch(1);
}
