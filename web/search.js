// ── Masters+ merged sort ──────────────────────────────────────────────────
function _catnoNum(item) {
  // Extract trailing number from catno for secondary sort (e.g. "TOM-2-1305" → 1305)
  const m = (item.catno ?? "").match(/(\d+)\s*$/);
  return m ? parseInt(m[1]) : 0;
}
function _sortMerged(arr, sortStr) {
  const [sf, so] = sortStr.split(":");
  const dir = so === "desc" ? -1 : 1;
  const getVal = (item) => {
    if (sf === "year") return parseInt(item.year) || 0;
    if (sf === "title") return (item.title ?? "").toLowerCase();
    if (sf === "label") {
      const labels = item.label ?? [];
      return (Array.isArray(labels) ? labels[0] ?? "" : labels).toLowerCase();
    }
    return String(item[sf] ?? "").toLowerCase();
  };
  arr.sort((a, b) => {
    const va = getVal(a), vb = getVal(b);
    let cmp;
    if (sf === "year") cmp = (va - vb) * dir;
    else cmp = va < vb ? -1 * dir : va > vb ? 1 * dir : 0;
    // Secondary sort by catalog number when primary values are equal
    if (cmp === 0) return _catnoNum(a) - _catnoNum(b);
    return cmp;
  });
}

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
  const country = document.getElementById("f-country")?.value ?? "";
  if (country) p.set("co", country);
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
  const fCountry = document.getElementById("f-country");
  if (fCountry) fCountry.value = p.get("co") ?? "";
  populateStyles();
  document.getElementById("f-style").value   = p.get("st") ?? "";
  const rtype = p.get("rt") ?? "";
  const radio = document.querySelector(`input[name="result-type"][value="${rtype}"]`);
  if (radio) radio.checked = true;
  const hasAdvanced = p.get("ar") || p.get("re") || p.get("yr") || p.get("lb") || p.get("gn") || p.get("st") || p.get("fm") || p.get("co");
  if (hasAdvanced) toggleAdvanced(true);
}

function clearForm() {
  ["query","f-artist","f-release","f-year","f-label","f-genre","f-style","f-country"].forEach(id => {
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
  window._lastResults = null;
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
  const country   = advOpen ? (document.getElementById("f-country")?.value.trim() ?? "") : "";
  const sort      = document.getElementById("f-sort").value;
  const resultType = document.querySelector('input[name="result-type"]:checked')?.value ?? "";

  if (resultType === "ai") { doAiSearch(q); return; }

  if (!q && !artist && !release && !year && !label && !genre && !country) {
    setStatus("Enter a search term or fill in at least one filter.", false);
    return;
  }

  // Only switch view on first page — pagination should not reset the view/form
  if (page === 1) {
    switchView("search", true);
    setActiveTab("search");
    // Hide favorites section when search results are incoming
    const ws = document.getElementById("random-records"); if (ws) ws.style.display = "none";
  }

  if (!skipPushState) pushSearchState(q, artistRaw, release, year, label, genre, sort, resultType, page);

  if (page === 1) detectedArtist = null;

  const _append = page > 1;
  currentPage = page;
  document.getElementById("search-btn").disabled = true;
  document.getElementById("search-load-more").style.display = "none";
  if (!_append) {
    document.getElementById("blurb").style.display = "none";
    document.getElementById("artist-alts").innerHTML = "";
    closeAltsPopup();
    setStatus("");
    document.getElementById("results").innerHTML = renderSkeletonGrid(16);
    document.getElementById("pagination").style.display = "none";
  } else {
    // Show loading indicator for "load more"
    const lmBtn = document.getElementById("search-load-more-btn");
    if (lmBtn) { lmBtn.classList.add("loading"); lmBtn.textContent = "Loading…"; }
  }

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
    if (country) parts.push(`Country: ${country}`);
    const typeLabels = { "master":"Masters", "master+":"Masters+", "release":"Releases", "artist":"Artists", "label":"Labels" };
    const typeLabel = typeLabels[resultType] ?? "";
    const sortLabels = { "year:asc":"Year ↑", "year:desc":"Year ↓", "title:asc":"Title A→Z", "title:desc":"Title Z→A", "label:asc":"Label A→Z" };
    const sortLabel = sortLabels[sort] ?? "";
    const extras = [typeLabel, sortLabel].filter(Boolean).join(" · ");
    const searchTerms = parts.join(", ") + (extras ? "  ·  " + extras : "");
    const descEl = document.getElementById("search-desc");
    if (parts.length) {
      const fullText = `Searched :: ${searchTerms}`;
      descEl.title = fullText;
      descEl.innerHTML = `Searched :: <span onclick="copySearchUrl(this)" title="Click to copy search link" style="cursor:pointer;border-bottom:1px dotted transparent;transition:border-color 0.2s" onmouseover="this.style.borderBottomColor='var(--accent)'" onmouseout="this.style.borderBottomColor='transparent'">${escHtml(searchTerms)}</span>`;
    } else {
      descEl.textContent = "";
      descEl.title = "";
    }
    const retClr = document.getElementById("search-returned");
    retClr.textContent = ""; retClr.title = "";
    const aiClr = document.getElementById("search-ai-summary");
    aiClr.textContent = ""; aiClr.title = "";
    if (parts.length) document.getElementById("search-info-block").style.display = "";
  }

  const buildParams = (perPage) => {
    const effectiveArtist = artist || (page > 1 ? detectedArtist : null) || "";
    const p = new URLSearchParams({ page, per_page: perPage });
    let effectiveQ = q || (page > 1 && detectedArtist && !artist ? detectedArtist : "");
    // When searching for label/artist entities, Discogs needs the name in `q`, not the field filter
    // (the `artist`/`label` params filter releases BY that artist/label, not search for entities)
    let useArtist = effectiveArtist;
    let useLabel = label;
    if (resultType === "label" && !effectiveQ && label) { effectiveQ = label; useLabel = ""; }
    if (resultType === "artist" && !effectiveQ && effectiveArtist) { effectiveQ = effectiveArtist; useArtist = ""; }
    if (effectiveQ) p.set("q", effectiveQ);
    if (resultType && resultType !== "master+") p.set("type", resultType);
    if (useArtist) p.set("artist", useArtist);
    if (release) p.set("release_title", release);
    if (year)    p.set("year",          year);
    if (useLabel) p.set("label",        useLabel);
    if (genre)   p.set("genre",         genre);
    if (style)   p.set("style",         style);
    if (format)  p.set("format",        format);
    if (country) p.set("country",      country);
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

    // Masters+ mode: run master + release searches in parallel, merge.
    // Resilient to partial failures — if one endpoint errors or returns
    // nothing (e.g. exhausted pagination), use whatever succeeded.
    const isMasterPlus = resultType === "master+";
    let searchPromise;
    if (isMasterPlus) {
      const masterParams = buildParams(48); masterParams.set("type", "master");
      const releaseParams = buildParams(48); releaseParams.set("type", "release");
      searchPromise = Promise.all([
        apiFetch(`${API}/search?${masterParams}`).catch(e => ({ ok: false, status: 0, _err: e })),
        apiFetch(`${API}/search?${releaseParams}`).catch(e => ({ ok: false, status: 0, _err: e })),
      ]).then(async ([mRes, rRes]) => {
        mRes._masterPlus = true;
        // Forward auth/rate-limit errors (check both responses)
        if (mRes.status === 401 || mRes.status === 429) return mRes;
        if (rRes.status === 401 || rRes.status === 429) { rRes._masterPlus = true; return rRes; }

        let mData = null, rData = null;
        if (mRes.ok) { try { mData = await mRes.json(); } catch {} }
        if (rRes.ok) { try { rData = await rRes.json(); } catch {} }

        const masters  = mData?.results ?? [];
        const releases = rData?.results ?? [];

        // If both empty/failed and this is page 1, surface the error
        if (!mData && !rData) {
          return mRes.ok ? mRes : (rRes.ok ? rRes : mRes);
        }

        const masterIds = new Set(masters.map(m => m.id));
        const orphans = releases.filter(r => !r.master_id || !masterIds.has(r.master_id));
        const seen = new Set(masters.map(m => m.id));
        const uniqueOrphans = orphans.filter(r => {
          if (seen.has(r.id)) return false;
          seen.add(r.id);
          return true;
        });
        let merged = [...masters, ...uniqueOrphans];
        if (sort) _sortMerged(merged, sort);
        mRes._mergedData = {
          results: merged,
          pagination: {
            pages: Math.max(mData?.pagination?.pages ?? 0, rData?.pagination?.pages ?? 0, 1),
            items: (mData?.pagination?.items ?? 0) + uniqueOrphans.length,
          }
        };
        // Ensure the response we return looks OK to the caller even if
        // only one endpoint succeeded.
        if (!mRes.ok && rRes.ok) {
          return { ok: true, status: 200, headers: rRes.headers, _masterPlus: true, _mergedData: mRes._mergedData, json: async () => mRes._mergedData };
        }
        return mRes;
      });
    } else {
      searchPromise = apiFetch(`${API}/search?${buildParams(48)}`);
    }

    const [res, bioRes] = await Promise.all([
      searchPromise,
      bioFetch ?? Promise.resolve(null),
    ]);
    bioFetch = bioRes ? { json: () => bioRes.json() } : null;
    if (res.status === 401 || res.status === 429) {
      const errData = await res.json().catch(() => ({}));
      if (errData.error === "no_token") {
        setStatus("");
        document.getElementById("results").innerHTML =
          `<div class="empty-state"><div class="empty-state-icon">🔑</div>` +
          `<div class="empty-state-title">Sign in to search</div>` +
          `<div class="empty-state-subtitle"><a href="/account" style="color:var(--accent)">Create a free account</a> and add your Discogs token to start discovering music.</div></div>`;
        return;
      }
      if (errData.error === "rate_limited") {
        setStatus("");
        document.getElementById("results").innerHTML =
          `<div class="empty-state"><div class="empty-state-icon">✨</div>` +
          `<div class="empty-state-title">You've used your free searches for today</div>` +
          `<div class="empty-state-subtitle"><a href="/account" style="color:var(--accent)">Sign in with a free account</a> and connect your Discogs token for unlimited searches, collection sync, favorites, and more.</div></div>`;
        return;
      }
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    // Show remaining searches hint for unauthenticated users
    if (!window._clerk?.user) {
      const rlRemaining = parseInt(res.headers.get("X-RateLimit-Remaining") ?? "");
      if (!isNaN(rlRemaining) && rlRemaining <= 3) {
        const plural = rlRemaining === 1 ? "search" : "searches";
        showToast(`${rlRemaining} free ${plural} remaining today — sign in for unlimited`, "info", 5000);
      }
    }
    const data = res._mergedData ?? await res.json();
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
          if (isMasterPlus) {
            // Masters+ constrained: parallel master+release with artist constraint
            const base = { q: q || constrainedArtist, page, per_page: 48, artist: constrainedArtist };
            const mp = new URLSearchParams(base); mp.set("type", "master");
            const rp = new URLSearchParams(base); rp.set("type", "release");
            [mp, rp].forEach(p => {
              if (release) p.set("release_title", release);
              if (year)    p.set("year", year);
              if (format)  p.set("format", format);
              if (sort) { const [sf, so] = sort.split(":"); p.set("sort", sf); p.set("sort_order", so); }
            });
            const [mR, rR] = await Promise.all([
              apiFetch(`${API}/search?${mp}`),
              apiFetch(`${API}/search?${rp}`),
            ]);
            if (mR.ok && rR.ok) {
              const [mD, rD] = await Promise.all([mR.json(), rR.json()]);
              const masters = mD.results ?? [];
              const releases = rD.results ?? [];
              const masterIds = new Set(masters.map(m => m.id));
              const orphans = releases.filter(r => !r.master_id || !masterIds.has(r.master_id));
              const seen = new Set(masters.map(m => m.id));
              const uniqueOrphans = orphans.filter(r => { if (seen.has(r.id)) return false; seen.add(r.id); return true; });
              let merged = [...masters, ...uniqueOrphans];
              if (sort) _sortMerged(merged, sort);
              if (merged.length > 0) {
                items = merged;
                totalPages = Math.max(mD.pagination?.pages ?? 1, rD.pagination?.pages ?? 1);
                totalItems_new = (mD.pagination?.items ?? 0) + uniqueOrphans.length;
              }
            }
          } else {
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

        const bu = new URL(window.location.href);
        if (!bu.searchParams.has("b")) { bu.searchParams.set("b", "y"); history.replaceState({}, "", bu.toString()); }
      }
    }

    if (!items.length) {
      setStatus("");
      if (_append) {
        // Load more returned nothing — keep existing results, hide button
        document.getElementById("search-load-more").style.display = "none";
        const lmBtn = document.getElementById("search-load-more-btn");
        if (lmBtn) { lmBtn.classList.remove("loading"); lmBtn.textContent = "Load more results"; }
        return;
      }
      document.getElementById("results").innerHTML = renderEmptyState("🔍", "No results found", "Try a different search term or broaden your filters");
      const noResAi = document.getElementById("search-ai-summary");
      noResAi.innerHTML = "<i>Couldn't find any results at Discogs.</i>";
      noResAi.title = "Couldn't find any results at Discogs.";
      document.getElementById("search-info-block").style.display = "";
      const ws = document.getElementById("random-records"); if (ws) ws.style.display = "none";
      return;
    }

    setStatus("");
    // Masters+ with sort on load more: merge new items with existing and re-sort
    // the entire list, then re-render from scratch so chronological order is
    // preserved across pages (individual page fetches can return items older
    // than the previous page's max).
    let _appendMode = _append;
    if (_append && isMasterPlus && sort && window._lastResults?.length) {
      const existingIds = new Set(window._lastResults.map(it => it.id));
      const newItems = items.filter(it => !existingIds.has(it.id));
      const combined = [...window._lastResults, ...newItems];
      _sortMerged(combined, sort);
      items = combined;
      _appendMode = false; // full re-render
    }
    const shown = _appendMode ? document.getElementById("results").querySelectorAll(".card, .card-animate").length + items.length : items.length;
    const returnedMsg = `Returned :: ${totalItems_new.toLocaleString()} results — showing ${shown}`;
    const retEl = document.getElementById("search-returned");
    retEl.textContent = returnedMsg;
    retEl.title = returnedMsg;
    document.getElementById("search-info-block").style.display = "";
    renderResults(items, _appendMode);
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
          if (aiEl) { aiEl.textContent = d.phrase || ""; aiEl.title = d.phrase || ""; }
        }).catch(() => {
          if (aiEl) { aiEl.textContent = ""; aiEl.title = ""; }
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

    // Only auto-open modals/videos from URL params on first page load,
    // not when appending via "load more" (which would restart the player)
    if (!_append) {
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
    }
  } catch (e) {
    console.error("Search failed:", e);
    setStatus("");
    if (_append) {
      // Load-more failure: keep existing results, reset the button, hide "load more"
      const lmBtn = document.getElementById("search-load-more-btn");
      if (lmBtn) { lmBtn.classList.remove("loading"); lmBtn.textContent = "Load more results"; }
      document.getElementById("search-load-more").style.display = "none";
      showToast("Couldn't load more results — you may have reached the end", "info", 4000);
    } else {
      document.getElementById("results").innerHTML =
        `<div class="empty-state"><div class="empty-state-icon">⚠️</div>` +
        `<div class="empty-state-title">Search failed</div>` +
        `<div class="empty-state-subtitle">${escHtml(e.message)} — please try again</div></div>`;
      showToast("Search failed — please try again", "error");
    }
  } finally {
    document.getElementById("search-btn").disabled = false;
  }
}

// ── Render cards ──────────────────────────────────────────────────────────
function renderResults(items, append = false) {
  if (!append) window._lastResults = items;
  else {
    const existingIds = new Set((window._lastResults || []).map(it => it.id));
    items = items.filter(it => !existingIds.has(it.id));
    window._lastResults = (window._lastResults || []).concat(items);
  }
  const hideOwned = document.getElementById("hide-owned")?.checked;
  const filtered = hideOwned && window._collectionIds?.size
    ? items.filter(item => !window._collectionIds.has(Number(item.id)))
    : items;
  const grid = document.getElementById("results");
  const startIdx = append ? grid.querySelectorAll(".card, .card-animate").length : 0;
  const html = filtered.map((item, i) => renderCard(item, startIdx + i)).join("");
  if (append) {
    grid.insertAdjacentHTML("beforeend", html);
  } else {
    grid.innerHTML = html;
  }
  // Hide favorites section when showing search results
  const ws = document.getElementById("random-records"); if (ws) ws.style.display = "none";
}

function renderCard(item, index) {
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
  if (isRelease || isArtist || isLabel) {
    const existing = itemCache.get(String(item.id));
    // Don't overwrite rich cached data with sparse favorite data
    if (!existing || (item.label?.length || item.format?.length || item.genre?.length)) {
      itemCache.set(String(item.id), item);
    }
  }
  const animClass = index != null ? " card-animate" : "";
  const animStyle = index != null ? ` style="--i:${Math.min(index, 20)}"` : "";
  const typeClass = `card card-type-${type}${animClass}`;
  const fullTitle = artist ? `${artist} - ${title}` : title;
  const cardAttrs = isRelease
    ? `class="${typeClass}" href="#" title="${escHtml(fullTitle)}" onclick="openModal(event,'${item.id}','${type}','${url.replace(/'/g, "\\'")}')" `
    : (isArtist || isLabel)
      ? `class="${typeClass}" href="#" title="${escHtml(fullTitle)}" data-entity-type="${escHtml(type)}" data-entity-name="${escHtml(title)}" data-entity-id="${item.id}" onclick="searchByEntity(event,this)"`
      : `class="${typeClass}" href="${url}" title="${escHtml(fullTitle)}" target="_blank" rel="noopener"`;

  // ── Badge strip: fixed order — collection, wantlist, list, inventory, favorite
  // C/W/♥ always shown (dimmed when inactive); L/I only when active
  let badges = "";
  const releaseId = item.id;
  const inCol = releaseId && type === "release" && window._collectionIds?.has(releaseId);
  const inWant = releaseId && type === "release" && window._wantlistIds?.has(releaseId);
  if (releaseId && type === "release") {
    badges += `<span class="card-badge badge-collection${inCol ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleCollectionFromCard(this,${releaseId})" title="${inCol ? "Remove from collection" : "Add to collection"}">C</span>`;
    badges += `<span class="card-badge badge-wantlist${inWant ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleWantlistFromCard(this,${releaseId})" title="${inWant ? "Remove from wantlist" : "Add to wantlist"}">W</span>`;
    const lists = window._listMembership?.[releaseId];
    if (lists?.length) {
      const names = lists.map(l => l.listName).join(", ");
      badges += `<span class="card-badge badge-list" title="In list: ${escHtml(names)}">L</span>`;
    }
    if (window._inventoryIds?.has(releaseId))
      badges += `<span class="card-badge badge-inventory" title="In your inventory">I</span>`;
  }
  const favKey = `${type}:${item.id}`;
  const isFav = window._favoriteKeys?.has(favKey);
  if (type && item.id)
    badges += `<span class="card-badge badge-favorite${isFav ? " is-favorite" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleFavoriteFromCard(this,${item.id},'${type}')" title="${isFav ? "Remove from favorites" : "Add to favorites"}">${isFav ? "♥" : "♡"}</span>`;

  const thumbWrap = `<div class="card-thumb-wrap">${thumb}<div class="card-thumb-badges">${badges}</div></div>`;

  // Rating stars (only for collection/wantlist cards)
  const rating = item._rating ?? 0;
  const ratingHtml = rating > 0
    ? `<div class="card-rating">${"★".repeat(rating)}${"☆".repeat(5 - rating)}</div>`
    : "";

  // Price badge (for price-sorted collection views)
  const priceData = item._price ?? null;
  let priceHtml = "";
  if (priceData && priceData.median) {
    const median = parseFloat(priceData.median).toFixed(0);
    let changeHtml = "";
    if (priceData.priceChange && Math.abs(priceData.priceChange) >= 1) {
      const pct = parseFloat(priceData.priceChange).toFixed(0);
      const cls = priceData.priceChange > 0 ? "price-up" : "price-down";
      const arrow = priceData.priceChange > 0 ? "↑" : "↓";
      changeHtml = ` <span class="price-change ${cls}">${arrow}${Math.abs(pct)}%</span>`;
    }
    priceHtml = `<div class="price-badge">~$${median}${changeHtml}</div>`;
  }

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
    <a ${cardAttrs}${animStyle}>
      ${thumbWrap}
      <div class="card-body">
        ${artist ? `<div class="card-artist">${escHtml(artist)}</div>` : ""}
        <div class="card-title">${escHtml(title)}</div>
        <div class="card-bottom">
          ${label   ? `<div class="card-sub">${escHtml(label)}</div>` : ""}
          ${formats ? `<div class="card-format">${escHtml(formats)}</div>` : ""}
          ${genre   ? `<div class="card-sub">${escHtml(genre)}</div>`   : ""}
          ${catno   ? `<div class="card-catno-line">${escHtml(catno)}</div>` : ""}
          ${ratingHtml}
          ${priceHtml}
          <div class="card-meta">${metaParts.map(escHtml).join(" · ")}</div>
          ${notesHtml}
        </div>
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

// ── Load More ─────────────────────────────────────────────────────────
function loadMoreResults() {
  doSearch(currentPage + 1, true);
}

function renderPagination() {
  const el = document.getElementById("search-load-more");
  if (currentPage >= totalPages) { el.style.display = "none"; return; }
  el.style.display = "";
  const btn = document.getElementById("search-load-more-btn");
  btn.classList.remove("loading");
  btn.textContent = "Load more results";
}

// ── AI search ─────────────────────────────────────────────────────────────
async function doAiSearch(q) {
  if (!q) { setStatus("Enter a question or description to search with AI.", false); return; }
  switchView("search", true);
  setActiveTab("search");
  const blurbEl = document.getElementById("blurb");
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  document.getElementById("search-load-more").style.display = "none";
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


// ── Random records for search page default ──────────────────────────────

let _randomAll = [];     // all fetched items (up to 192)
let _randomShown = 0;    // how many currently rendered
const _RANDOM_PAGE = 48;

function _shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

// Parse a row from the random-records API into a card-compatible item
function _parseRandomRow(row) {
  const d = row.data ?? {};
  const basic = d.basic_information ?? d;
  // Collection/wantlist items have basic_information; favorites have card data directly
  if (d.basic_information) {
    const labels = (basic.labels ?? []).map(l => l.name);
    const formats = (basic.formats ?? []).map(f => f.name);
    const title = basic.artists?.length
      ? `${basic.artists.map(a => a.name).join(", ")} - ${basic.title}`
      : basic.title || "Unknown";
    return {
      id: basic.id ?? row.rid,
      type: "release",
      title,
      cover_image: basic.cover_image || basic.thumb || "",
      uri: `/release/${basic.id ?? row.rid}`,
      label: labels,
      format: formats,
      genre: basic.genres ?? [],
      year: String(basic.year ?? ""),
      country: basic.country ?? "",
    };
  }
  // Favorites / list items store card-format data
  return {
    id: d.id ?? row.rid,
    type: d.type ?? "release",
    title: d.title || `Release ${row.rid}`,
    cover_image: d.cover_image || d.thumb || "",
    uri: d.uri || `/release/${row.rid}`,
    label: d.label ?? [],
    format: d.format ?? [],
    genre: d.genre ?? [],
    year: String(d.year ?? ""),
    country: d.country ?? "",
  };
}

async function loadRandomRecords(more) {
  const grid = document.getElementById("random-records-grid");
  const wrap = document.getElementById("random-records");
  if (!grid || !wrap) return;

  // First call: fetch all and shuffle
  if (!more) {
    try {
      const isLoggedIn = !!window._clerk?.user;
      const url = isLoggedIn ? "/api/user/random-records?limit=192" : "/api/public/featured-records?limit=192";
      const r = isLoggedIn ? await apiFetch(url) : await fetch(url);
      if (!r.ok) return;
      const data = await r.json();
      _randomAll = _shuffle((data.items ?? []).map(_parseRandomRow)
        .filter(r => r.type === "release" && r.cover_image));
      _randomShown = 0;
    } catch { return; }
    if (!_randomAll.length) return;
  }

  // Render next page of 48
  const slice = _randomAll.slice(_randomShown, _randomShown + _RANDOM_PAGE);
  if (!slice.length) return;
  const html = slice.map((item, i) => renderCard(item, _randomShown + i)).join("");
  grid.querySelector(".random-load-more")?.remove();
  if (more) {
    grid.insertAdjacentHTML("beforeend", html);
  } else {
    grid.innerHTML = html;
  }
  _randomShown += slice.length;

  // Add load-more if there are more to show
  if (_randomShown < _randomAll.length) {
    grid.insertAdjacentHTML("beforeend",
      `<div class="random-load-more" style="grid-column:1/-1;text-align:center;padding:0.75rem 0">` +
      `<button onclick="loadRandomRecords(true)" style="background:none;border:1px solid var(--border);color:var(--muted);padding:0.4rem 1.2rem;border-radius:var(--radius);cursor:pointer;font-size:0.8rem" ` +
      `onmouseover="this.style.borderColor='var(--accent)';this.style.color='var(--accent)'" ` +
      `onmouseout="this.style.borderColor='var(--border)';this.style.color='var(--muted)'"` +
      `>Load More</button></div>`
    );
  }

  // Show the wrap if on search view with no search results
  const view = new URLSearchParams(location.search).get("view") || "";
  const hasSearchResults = document.getElementById("results")?.children.length > 0;
  if ((!view || view === "search" || view === "find") && !hasSearchResults) {
    wrap.style.display = "";
  }
}

function showRandomRecords() {
  const wrap = document.getElementById("random-records");
  if (!wrap) return;
  if (!_randomAll.length) {
    loadRandomRecords();
  } else {
    wrap.style.display = "";
  }
}

function toggleFavoriteFromCard(btn, discogsId, entityType) {
  const key = `${entityType}:${discogsId}`;
  const wasFav = window._favoriteKeys?.has(key);
  if (!window._favoriteKeys) window._favoriteKeys = new Set();

  // Optimistic update
  if (wasFav) {
    window._favoriteKeys.delete(key);
    btn.classList.remove("is-favorite");
    btn.textContent = "♡";
    btn.title = "Add to favorites";
  } else {
    window._favoriteKeys.add(key);
    btn.classList.add("is-favorite");
    btn.textContent = "♥";
    btn.title = "Remove from favorites";
  }

  // Build card data for storage
  const card = btn.closest("a");
  const cached = itemCache.get(String(discogsId));
  const cardImg = card?.querySelector("img")?.src || "";
  const cardArtist = card?.querySelector(".card-artist")?.textContent || "";
  const cardTitle = card?.querySelector(".card-title")?.textContent || "";
  const fullTitle = cardArtist ? `${cardArtist} - ${cardTitle}` : cardTitle;
  const cardData = cached || { id: discogsId, type: entityType, title: fullTitle, cover_image: cardImg, uri: `/${entityType}/${discogsId}` };

  // API call
  const endpoint = wasFav ? "/api/user/favorites/remove" : "/api/user/favorites/add";
  const body = wasFav ? { discogsId, entityType } : { discogsId, entityType, data: cardData };
  apiFetch(endpoint, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
    .then(r => {
      if (!r.ok) throw new Error();
      const n = window._favoriteKeys?.size ?? 0;
      showToast(wasFav ? "Removed from favorites" : `Added to favorites (${n})`);
      // Update badge on the card
      refreshCardBadges?.(discogsId);
    })
    .catch(() => {
      // Revert on error
      if (wasFav) window._favoriteKeys.add(key); else window._favoriteKeys.delete(key);
      btn.classList.toggle("is-favorite", wasFav);
      btn.textContent = wasFav ? "❤" : "♡";
      showToast("Failed to update favorite", "error");
    });
}

async function toggleCollectionFromCard(btn, releaseId) {
  const inCol = window._collectionIds?.has(releaseId);
  if (!window._collectionIds) window._collectionIds = new Set();
  const sessionToken = window._clerk?.session ? await window._clerk.session.getToken() : null;
  if (!sessionToken) { showToast("Sign in to manage your collection", "error"); return; }

  // Optimistic update
  if (inCol) { window._collectionIds.delete(releaseId); } else { window._collectionIds.add(releaseId); }
  refreshCardBadges?.(releaseId);

  const endpoint = inCol ? "/api/user/collection/remove" : "/api/user/collection/add";
  const body = inCol ? { releaseId, instanceId: null, folderId: 1 } : { releaseId };
  try {
    const r = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${sessionToken}` },
      body: JSON.stringify(body),
    }).then(r => r.json());
    if (!r.ok && r.error) throw new Error(r.error);
    showToast(inCol ? "Removed from collection" : "Added to collection");
    // Update modal buttons if open
    const modalBtn = document.getElementById("modal-col-btn");
    if (modalBtn) {
      modalBtn.classList.toggle("in-collection", !inCol);
      modalBtn.innerHTML = inCol ? "Collection" : "Collected";
    }
  } catch (e) {
    // Revert
    if (inCol) { window._collectionIds.add(releaseId); } else { window._collectionIds.delete(releaseId); }
    refreshCardBadges?.(releaseId);
    showToast(e.message || "Failed to update collection", "error");
  }
}

async function toggleWantlistFromCard(btn, releaseId) {
  const inWant = window._wantlistIds?.has(releaseId);
  if (!window._wantlistIds) window._wantlistIds = new Set();
  const sessionToken = window._clerk?.session ? await window._clerk.session.getToken() : null;
  if (!sessionToken) { showToast("Sign in to manage your wantlist", "error"); return; }

  // Optimistic update
  if (inWant) { window._wantlistIds.delete(releaseId); } else { window._wantlistIds.add(releaseId); }
  refreshCardBadges?.(releaseId);

  const endpoint = inWant ? "/api/user/wantlist/remove" : "/api/user/wantlist/add";
  try {
    const r = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${sessionToken}` },
      body: JSON.stringify({ releaseId }),
    }).then(r => r.json());
    if (!r.ok && r.error) throw new Error(r.error);
    showToast(inWant ? "Removed from wantlist" : "Added to wantlist");
    // Update modal buttons if open
    const modalBtn = document.getElementById("modal-want-btn");
    if (modalBtn) {
      modalBtn.classList.toggle("in-wantlist", !inWant);
      modalBtn.innerHTML = inWant ? "Want" : "Wanted";
    }
  } catch (e) {
    // Revert
    if (inWant) { window._wantlistIds.add(releaseId); } else { window._wantlistIds.delete(releaseId); }
    refreshCardBadges?.(releaseId);
    showToast(e.message || "Failed to update wantlist", "error");
  }
}

// ── Artist / entity navigation ───────────────────────────────────────────
function searchArtistFromModal(event, el) {
  event.preventDefault();
  closeModal();
  clearForm();
  document.getElementById("f-artist").value = el.dataset.artist;
  toggleAdvanced(true);
  doSearch(1);
}

function selectAltArtist(event, el) {
  event.preventDefault();
  clearForm();
  document.getElementById("f-artist").value = el.dataset.altName;
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
