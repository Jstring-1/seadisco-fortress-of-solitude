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
  if (q)          p.set("q", q);
  if (artistRaw)  p.set("a", artistRaw);
  if (release)    p.set("e", release);
  if (year)       p.set("y", year);
  if (label)      p.set("l", label);
  if (genre)      p.set("g", genre);
  const style  = document.getElementById("f-style")?.value ?? "";
  const format = document.getElementById("f-format")?.value ?? "";
  if (style)      p.set("t", style);
  if (format) p.set("f", format);
  const country = document.getElementById("f-country")?.value ?? "";
  if (country) p.set("c", country);
  if (sort)       p.set("s", sort);
  if (resultType) p.set("r", resultType);
  if (page > 1)   p.set("p", String(page));
  history.pushState({}, "", p.toString() ? "?" + p.toString() : location.pathname);
}

function restoreFromParams(p) {
  // Support both new 1-letter and old 2-letter param names for backward compat
  document.getElementById("query").value     = p.get("q")  ?? "";
  document.getElementById("f-artist").value  = p.get("a") || p.get("ar") || "";
  document.getElementById("f-release").value = p.get("e") || p.get("re") || "";
  document.getElementById("f-year").value    = p.get("y") || p.get("yr") || "";
  document.getElementById("f-label").value   = p.get("l") || p.get("lb") || "";
  document.getElementById("f-genre").value   = p.get("g") || p.get("gn") || "";
  const sortEl = document.getElementById("f-sort");
  sortEl.value = p.get("s") || p.get("sr") || "";
  if (!sortEl.value) sortEl.selectedIndex = 0;
  document.getElementById("f-format").value  = p.get("f") || p.get("fm") || "";
  const fCountry = document.getElementById("f-country");
  if (fCountry) fCountry.value = p.get("c") || p.get("co") || "";
  populateStyles();
  document.getElementById("f-style").value   = p.get("t") || p.get("st") || "";
  const rtype = p.get("r") || p.get("rt") || "";
  const radio = document.querySelector(`input[name="result-type"][value="${rtype}"]`);
  if (radio) radio.checked = true;
  const hasAdvanced = p.get("a") || p.get("ar") || p.get("e") || p.get("re") || p.get("y") || p.get("yr") || p.get("l") || p.get("lb") || p.get("g") || p.get("gn") || p.get("t") || p.get("st") || p.get("f") || p.get("fm") || p.get("c") || p.get("co");
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
  if (typeof resetSelectHighlights === "function") resetSelectHighlights();
  // Clear-form X also dismisses the AI results panel (per user spec).
  if (typeof closeAiPanel === "function") closeAiPanel();
}

// ── Main search ──────────────────────────────────────────────────────────
async function doSearch(page = 1, skipPushState = false) {
  const q         = document.getElementById("query").value.trim();
  const advOpen   = document.getElementById("advanced-panel")?.dataset.open === "true";
  const artistRaw = advOpen ? document.getElementById("f-artist").value.trim() : "";
  // Pass the artist filter through verbatim — the Discogs disambiguator
  // "(N)" is meaningful and dropping it merges results with other artists
  // who share the base name. Both `artist` and `artistRaw` now refer to
  // the same value; the second name is preserved for downstream readers.
  const artist    = artistRaw;
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

  if (page === 1) saveSearchHistory("main");

  // Only switch view on first page — pagination should not reset the view/form
  if (page === 1) {
    switchView("search", true);
    setActiveTab("search");
    // Hide the Recent strip when a search is running
    const ws = document.getElementById("random-records"); if (ws) ws.style.display = "none";
  }

  if (!skipPushState) pushSearchState(q, artistRaw, release, year, label, genre, sort, resultType, page);

  if (page === 1) { detectedArtist = null; }

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

        // Only include releases that have NO master at all (true orphans).
        // Any release with a master_id has a master somewhere in Discogs,
        // so the master search already covers it.
        const orphans = releases.filter(r => !r.master_id);
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
          `<div class="empty-state-subtitle"><a href="/?v=account" onclick="switchView('account');return false;" style="color:var(--accent)">Create a free account</a> and add your Discogs token to start discovering music.</div></div>`;
        return;
      }
      if (errData.error === "rate_limited") {
        setStatus("");
        document.getElementById("results").innerHTML =
          `<div class="empty-state"><div class="empty-state-icon">🔑</div>` +
          `<div class="empty-state-title">Sign in to search</div>` +
          `<div class="empty-state-subtitle"><a href="/?v=account" onclick="switchView('account');return false;" style="color:var(--accent)">Sign in</a> and connect your Discogs token to continue.</div></div>`;
        return;
      }
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
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
        // Keep the disambiguator on the constrained artist so the
        // follow-up search hits the SAME artist whose bio we just
        // fetched, not a same-named neighbor.
        const constrainedArtist = bioData.name.trim();
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
              const orphans = releases.filter(r => !r.master_id);
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
    // Masters+ load more: deduplicate against existing results (use type+id
    // since masters and releases can share the same numeric id)
    let _appendMode = _append;
    if (_append && isMasterPlus && window._lastResults?.length) {
      const existingKeys = new Set(window._lastResults.map(it => `${it.type}:${it.id}`));
      items = items.filter(it => !existingKeys.has(`${it.type}:${it.id}`));
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
    const existingKeys = new Set((window._lastResults || []).map(it => `${it.type}:${it.id}`));
    items = items.filter(it => !existingKeys.has(`${it.type}:${it.id}`));
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
  if (typeof applyVisitedCards === "function") applyVisitedCards();
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
  // Keep Discogs' " (N)" disambiguator on cards too — both display and
  // hover/title text — so clicking "Tommy Tucker (3)" leads to that
  // exact artist's results instead of the merged base-name set.

  const label   = (item.label ?? []).slice(0, 2).join(", ");
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
  // C/W/F always shown (dimmed when inactive); L/I only when active.
  // For master cards we show the count of distinct releases (versions)
  // the user owns from this master inside the badge — pulled from the
  // pre-built _collectionMasterCounts / _wantlistMasterCounts maps
  // populated by loadDiscogsIds.
  let badges = "";
  const releaseId = item.id;
  const isReleaseOrMaster = type === "release" || type === "master";
  const inCol = releaseId && type === "release" && window._collectionIds?.has(releaseId);
  const inWant = releaseId && type === "release" && window._wantlistIds?.has(releaseId);
  if (releaseId && isReleaseOrMaster) {
    if (type === "release") {
      badges += `<span class="card-badge badge-collection${inCol ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleCollectionFromCard(this,${releaseId})" title="${inCol ? "Remove from collection" : "Add to collection"}">C</span>`;
      badges += `<span class="card-badge badge-wantlist${inWant ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleWantlistFromCard(this,${releaseId})" title="${inWant ? "Remove from wantlist" : "Add to wantlist"}">W</span>`;
    } else {
      // Master — look up distinct-release count for this master
      const colCount = Number(window._collectionMasterCounts?.[releaseId]) || 0;
      const wantCount = Number(window._wantlistMasterCounts?.[releaseId]) || 0;
      const colActive = colCount > 0;
      const wantActive = wantCount > 0;
      const colTitle = colActive
        ? `${colCount} ${colCount === 1 ? "version" : "versions"} of this master in your collection — click to view pressings`
        : "Open to add a version to collection";
      const wantTitle = wantActive
        ? `${wantCount} ${wantCount === 1 ? "version" : "versions"} of this master in your wantlist — click to view pressings`
        : "Open to add a version to wantlist";
      // When active, show the count number; otherwise show the C/W glyph.
      badges += `<span class="card-badge badge-collection${colActive ? " is-active has-count" : ""}" onclick="event.preventDefault();event.stopPropagation();openModal(event,'${releaseId}','master','')" title="${colTitle}">${colActive ? colCount : "C"}</span>`;
      badges += `<span class="card-badge badge-wantlist${wantActive ? " is-active has-count" : ""}" onclick="event.preventDefault();event.stopPropagation();openModal(event,'${releaseId}','master','')" title="${wantTitle}">${wantActive ? wantCount : "W"}</span>`;
    }
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
    badges += `<span class="card-badge badge-favorite${isFav ? " is-favorite" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleFavoriteFromCard(this,${item.id},'${type}')" title="${isFav ? "Remove from favorites" : "Add to favorites"}">F</span>`;

  // Multi-instance "(N)" badge at lower-left — shown when user owns multiple copies
  // of the same release. Clicking opens a popover listing each instance.
  const instanceCount = Number(
    item._instanceCount ?? (isRelease ? (window._collectionInstanceCounts?.[item.id] ?? 0) : 0)
  );
  const instanceBadge = (instanceCount > 1 && isRelease)
    ? `<span class="card-instance-badge" onclick="event.preventDefault();event.stopPropagation();openInstancesPopover(event,${item.id})" title="${instanceCount} copies in your collection — click to view">(${instanceCount})</span>`
    : "";
  const thumbWrap = `<div class="card-thumb-wrap">${thumb}<div class="card-thumb-badges">${badges}</div>${instanceBadge}</div>`;

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
// AI results render into a dedicated, persistent panel (#ai-results-panel)
// instead of the shared #blurb. The panel survives subsequent regular
// searches so the user can read suggestions while pursuing them. It closes
// only when the form's clear button (X) is pressed or the panel's own X
// (top corner) is pressed.
function _aiPanelEl() {
  let el = document.getElementById("ai-results-panel");
  if (el) return el;
  el = document.createElement("div");
  el.id = "ai-results-panel";
  el.className = "ai-results-panel";
  el.style.display = "none";
  // Insert just above #blurb so it lives at the top of the search content
  const blurb = document.getElementById("blurb");
  if (blurb && blurb.parentNode) blurb.parentNode.insertBefore(el, blurb);
  else document.getElementById("search-view")?.prepend(el);
  return el;
}
function closeAiPanel() {
  const el = document.getElementById("ai-results-panel");
  if (el) { el.style.display = "none"; el.innerHTML = ""; el.classList.remove("is-minimized"); }
  try {
    const u = new URL(window.location.href);
    u.searchParams.delete("ai");
    history.replaceState({}, "", u.toString());
  } catch {}
}

// Toggle minimized state — collapses the body to a thin clickable header bar.
// Content and ai= URL state are preserved so the user can re-expand instantly.
function toggleAiPanelMinimize(ev) {
  if (ev) ev.stopPropagation();
  const el = document.getElementById("ai-results-panel");
  if (!el) return;
  const nowMin = !el.classList.contains("is-minimized");
  el.classList.toggle("is-minimized", nowMin);
  const btn = el.querySelector(".ai-panel-min");
  if (btn) {
    btn.textContent = nowMin ? "+" : "–";
    btn.title = nowMin ? "Expand AI results" : "Minimize AI results";
  }
}

// Build a single AI item row. Each entity (artist/album/label) gets a trio
// of links: new SeaDisco search, your-records search (orange ⌕), Wikipedia (W).
function _aiEntityLinks(name, kind) {
  if (!name) return "";
  const safe = String(name).replace(/'/g, "\\'");
  const safeEsc = escHtml(safe);
  // SeaDisco new search — kind controls which field gets populated
  let newSearch = "";
  if (kind === "artist") {
    newSearch = `event.preventDefault();closeAiPanel();clearForm();document.getElementById('f-artist').value='${safeEsc}';applyEntityLinkDefaults();toggleAdvanced(true);doSearch(1)`;
  } else if (kind === "label") {
    newSearch = `event.preventDefault();closeAiPanel();clearForm();document.getElementById('f-label').value='${safeEsc}';applyEntityLinkDefaults();toggleAdvanced(true);doSearch(1)`;
  } else {
    // release/album/general — put into main query
    newSearch = `event.preventDefault();closeAiPanel();clearForm();document.getElementById('query').value='${safeEsc}';doSearch(1)`;
  }
  const cwField = kind === "artist" ? "cw-artist" : kind === "label" ? "cw-label" : "cw-query";
  const wikiQ = kind === "label" ? `${name} record label` : name;
  return `<a href="#" class="ai-entity-link" onclick="${newSearch}" title="New SeaDisco search for ${escHtml(name)}">${escHtml(name)}</a>` +
    ` <a href="#" class="track-search-icon ai-entity-icon" onclick="event.preventDefault();searchCollectionFor('${cwField}','${safeEsc}')" title="Search your records for ${escHtml(name)}">⌕</a>` +
    ` <a href="#" class="wiki-icon ai-entity-icon" onclick="event.preventDefault();openWikiPopup('${escHtml(String(wikiQ).replace(/'/g, "\\'"))}')" title="Wikipedia: ${escHtml(name)}">W</a>`;
}

async function doAiSearch(q) {
  if (!q) { setStatus("Enter a question or description to search with AI.", false); return; }
  switchView("search", true);
  setActiveTab("search");
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  document.getElementById("search-load-more").style.display = "none";
  document.getElementById("artist-alts").innerHTML = "";
  const blurbEl = document.getElementById("blurb");
  if (blurbEl) blurbEl.style.display = "none";
  setStatus("Asking Claude…");
  document.getElementById("search-btn").disabled = true;
  try {
    const r = await apiFetch("/api/ai-search", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ q }) });
    if (r.status === 401) { setStatus("Sign in and add your Discogs token to use AI search.", false); return; }
    if (!r.ok) { setStatus("AI search failed. Try again.", false); return; }
    const { recommendations, blurb } = await r.json();
    if (!recommendations?.length) { setStatus("No recommendations returned.", false); return; }
    setStatus("");

    // Reflect in URL so AI results are shareable.
    try {
      const u = new URL(window.location.href);
      u.searchParams.set("ai", q);
      history.replaceState({}, "", u.toString());
    } catch {}

    const panel = _aiPanelEl();
    panel.classList.remove("is-minimized");
    panel.innerHTML = `
      <button class="ai-panel-min" onclick="toggleAiPanelMinimize(event)" title="Minimize AI results">–</button>
      <button class="ai-panel-close" onclick="closeAiPanel()" title="Close AI results">×</button>
      <div class="ai-panel-head" onclick="if(document.getElementById('ai-results-panel')?.classList.contains('is-minimized')) toggleAiPanelMinimize(event)">
        <div class="ai-panel-eyebrow">✦ AI Recommendations for "${escHtml(q)}"</div>
        ${blurb ? `<div class="ai-panel-blurb">${escHtml(blurb)}</div>` : ""}
      </div>
      <div class="ai-panel-scroll">
        ${recommendations.map(rec => {
          const artist = rec.artist || "";
          const album = rec.album || (rec.type === "release" && rec.name ? rec.name : "");
          const label = rec.label || "";
          // Prefer structured fields. Row title is the primary entity link.
          let titleHtml;
          if (rec.type === "artist" && artist) {
            titleHtml = _aiEntityLinks(artist, "artist");
          } else if (album) {
            titleHtml = (artist ? `${_aiEntityLinks(artist, "artist")} <span class="ai-sep">·</span> ` : "") + _aiEntityLinks(album, "release");
          } else {
            titleHtml = _aiEntityLinks(rec.name || "", rec.type === "artist" ? "artist" : "release");
          }
          const labelHtml = label ? `<div class="ai-row-label"><span class="ai-row-label-tag">Label</span> ${_aiEntityLinks(label, "label")}</div>` : "";
          return `<div class="ai-row">
            <div class="ai-row-title">${titleHtml}</div>
            <div class="ai-row-desc">${escHtml(rec.description || "")}</div>
            ${labelHtml}
          </div>`;
        }).join("")}
      </div>`;
    panel.style.display = "block";

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

// Extract just the artist portion of a "Artist - Title" combined title
function _favArtist(item) {
  const t = item.title ?? "";
  const i = t.indexOf(" - ");
  return (i > 0 ? t.slice(0, i) : t).toLowerCase();
}
function _favTitle(item) {
  const t = item.title ?? "";
  const i = t.indexOf(" - ");
  return (i > 0 ? t.slice(i + 3) : t).toLowerCase();
}

function _applyFavoritesSort() {
  const sel = document.getElementById("favorites-sort");
  const sort = sel?.value || "added:desc";
  const [field, order] = sort.split(":");
  const dir = order === "desc" ? -1 : 1;
  const getVal = (it) => {
    if (field === "added") return it._addedAt ?? 0;
    if (field === "year")  return parseInt(it.year) || 0;
    if (field === "title") return _favTitle(it);
    if (field === "artist") return _favArtist(it);
    return 0;
  };
  _randomAll.sort((a, b) => {
    const va = getVal(a), vb = getVal(b);
    if (typeof va === "number") return (va - vb) * dir;
    return va < vb ? -1 * dir : va > vb ? 1 * dir : 0;
  });
}

// Called when the sort dropdown changes
function sortFavoritesGrid() {
  if (!_randomAll.length) return;
  _applyFavoritesSort();
  _randomShown = 0;
  const grid = document.getElementById("random-records-grid");
  if (grid) grid.innerHTML = "";
  loadRandomRecords(true);
}

// ── Recent front-page strip ─────────────────────────────────────────────
//
// The #random-records section shows the user's browsing history as a
// "Recent" grid. Storage is write-through: localStorage gives instant
// render, and modal.js also POSTs each opened release to /api/user/recent
// so the list syncs across devices. On first load of the search view we
// fetch the server copy once and merge it in (server rows win on conflict
// because they carry the authoritative opened_at timestamp).

const _HISTORY_KEY = "sd_history";
const _HISTORY_MAX_CLIENT = 120;
let _historyHydrated = false;

function _readHistory() {
  try {
    const raw = localStorage.getItem(_HISTORY_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch { return []; }
}

function _writeHistory(arr) {
  try { localStorage.setItem(_HISTORY_KEY, JSON.stringify(arr)); } catch { /* quota */ }
}

// Fetch the server's Recent list and merge it with the local cache.
// Runs at most once per page load; subsequent loadRandomRecords() calls
// hit localStorage directly. Safe to call when signed out — just no-ops.
async function _hydrateHistoryFromServer() {
  if (_historyHydrated) return;
  _historyHydrated = true;
  if (!window._clerk?.user || typeof apiFetch !== "function") return;
  try {
    const r = await apiFetch("/api/user/recent?limit=120");
    if (!r.ok) return;
    const body = await r.json().catch(() => null);
    const serverItems = Array.isArray(body?.items) ? body.items : [];
    if (!serverItems.length) return;
    // Server rows have shape { id, type, data, openedAt }. Flatten into the
    // localStorage card shape and key by id for fast lookup.
    const serverByKey = new Map();
    for (const row of serverItems) {
      const key = `${row.type || "release"}:${row.id}`;
      serverByKey.set(key, {
        ...(row.data || {}),
        id: row.id,
        type: row.type || "release",
        _openedAt: row.openedAt ? new Date(row.openedAt).getTime() : Date.now(),
      });
    }
    // Merge: start from the local cache (preserves entries the server
    // hasn't acked yet, e.g. offline opens), then overwrite with server
    // entries by key, then sort by opened_at desc.
    const local = _readHistory();
    const byKey = new Map();
    for (const h of local) {
      const key = `${h.type || "release"}:${h.id}`;
      byKey.set(key, h);
    }
    for (const [k, v] of serverByKey) byKey.set(k, v);
    const merged = Array.from(byKey.values())
      .sort((a, b) => (b._openedAt || 0) - (a._openedAt || 0))
      .slice(0, _HISTORY_MAX_CLIENT);
    _writeHistory(merged);
    window.dispatchEvent(new CustomEvent("sd-history-change"));
  } catch { /* silent — the local cache still renders */ }
}

// Kick off hydration as soon as the user is signed in. The auth bootstrap
// in app.js / shared.js dispatches a synthetic event when Clerk resolves;
// falling back to a short delay keeps this resilient if that event
// changes. Either way, each call early-returns after the first run.
window.addEventListener("load", () => { setTimeout(_hydrateHistoryFromServer, 600); });

// Load history into _randomAll as card-compatible items
function _loadHistoryIntoRandom() {
  const hist = _readHistory();
  _randomAll = hist
    .filter(h => h && h.cover_image) // only items with a cover render cleanly
    .map(h => ({
      id: h.id,
      type: h.type ?? "release",
      title: h.title || `Release ${h.id}`,
      cover_image: h.cover_image || "",
      uri: h.uri || `/${h.type ?? "release"}/${h.id}`,
      label: h.label ?? [],
      format: h.format ?? [],
      genre: h.genre ?? [],
      year: String(h.year ?? ""),
      country: h.country ?? "",
      catno: h.catno ?? "",
      _addedAt: h._openedAt ?? 0,
    }));
  _applyFavoritesSort();
}

async function loadRandomRecords(more) {
  const grid = document.getElementById("random-records-grid");
  const wrap = document.getElementById("random-records");
  if (!grid || !wrap) return;

  // First call (or full reload): rebuild from localStorage history
  if (!more) {
    _loadHistoryIntoRandom();
    _randomShown = 0;
    // Update header title — always "Recent" now
    const titleEl = document.getElementById("random-records-title");
    if (titleEl) titleEl.textContent = "Recent";
    if (!_randomAll.length) {
      wrap.style.display = "none";
      return;
    }
  }

  // Render next page of 48
  const slice = _randomAll.slice(_randomShown, _randomShown + _RANDOM_PAGE);
  if (!slice.length) return;
  // Each card gets a small X overlay so the user can drop a single item
  // from their local history without clearing everything.
  const html = slice.map((item, i) => {
    const card = renderCard(item, _randomShown + i);
    const safeId = escHtml(String(item.id));
    return `<div class="recent-wrap" data-hist-id="${safeId}">${card}` +
      `<button class="recent-dismiss" onclick="removeFromHistory(event,'${safeId}')" title="Remove from history">✕</button>` +
      `</div>`;
  }).join("");
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
  const view = new URLSearchParams(location.search).get("v") || new URLSearchParams(location.search).get("view") || "";
  const hasSearchResults = document.getElementById("results")?.children.length > 0;
  if ((!view || view === "search" || view === "find") && !hasSearchResults) {
    wrap.style.display = "";
  }
  if (typeof applyVisitedCards === "function") applyVisitedCards();
}

function showRandomRecords() {
  const wrap = document.getElementById("random-records");
  if (!wrap) return;
  // Always rebuild from current history — cheap and guarantees freshness
  loadRandomRecords();
}

// Logo / title click handler. Drops the user back on the search-home
// view (Recent strip restored) without disturbing whatever's currently
// playing in the LOC bar / YouTube mini-player. Form field values are
// intentionally preserved and the Advanced section is collapsed so the
// page reads as "fresh" without losing what the user typed.
function goHome() {
  try {
    window._lastResults = null;  // forces switchView to take the "no results" branch and show Recent
    if (typeof switchView === "function") {
      switchView("search");
    } else {
      location.href = "/";
      return;
    }
    // Collapse the advanced panel but keep its inputs filled
    if (typeof toggleAdvanced === "function") toggleAdvanced(false);
    // Scroll back to the top so the user lands on the search box
    window.scrollTo({ top: 0, behavior: "smooth" });
  } catch {
    location.href = "/";
  }
}

/** Remove a single entry from history (X button on a card). */
function removeFromHistory(ev, id) {
  if (ev) { ev.preventDefault(); ev.stopPropagation(); }
  const hist = _readHistory().filter(h => String(h.id) !== String(id));
  _writeHistory(hist);
  // Optimistic DOM removal
  const el = document.querySelector(`.recent-wrap[data-hist-id="${CSS.escape(String(id))}"]`);
  if (el) el.remove();
  // If grid is now empty, hide the whole strip
  if (!hist.length) {
    const wrap = document.getElementById("random-records");
    if (wrap) wrap.style.display = "none";
  }
  // Mirror to server (fire-and-forget). Any error is ignored — the local
  // cache already succeeded and next hydration will reconcile.
  if (window._clerk?.user && typeof apiFetch === "function") {
    try { apiFetch(`/api/user/recent/${encodeURIComponent(id)}`, { method: "DELETE" }).catch(() => {}); } catch {}
  }
}

/** Clear-all button — drops all history after confirmation. */
function clearRecentHistory() {
  if (!confirm("Clear all recently opened releases?")) return;
  _writeHistory([]);
  _randomAll = [];
  _randomShown = 0;
  const wrap = document.getElementById("random-records");
  if (wrap) wrap.style.display = "none";
  if (window._clerk?.user && typeof apiFetch === "function") {
    try { apiFetch("/api/user/recent", { method: "DELETE" }).catch(() => {}); } catch {}
  }
}

// Re-render the strip whenever the history changes (modal opened elsewhere)
window.addEventListener("sd-history-change", () => {
  clearTimeout(window._recentReloadTimer);
  window._recentReloadTimer = setTimeout(() => {
    // Only re-render if the strip is currently visible on the search view
    const wrap = document.getElementById("random-records");
    if (wrap && wrap.style.display !== "none") loadRandomRecords();
  }, 400);
});

function toggleFavoriteFromCard(btn, discogsId, entityType) {
  const key = `${entityType}:${discogsId}`;
  const wasFav = window._favoriteKeys?.has(key);
  if (!window._favoriteKeys) window._favoriteKeys = new Set();

  // Optimistic update
  if (wasFav) {
    window._favoriteKeys.delete(key);
    btn.classList.remove("is-favorite");
    btn.textContent = "F";
    btn.title = "Add to favorites";
  } else {
    window._favoriteKeys.add(key);
    btn.classList.add("is-favorite");
    btn.textContent = "F";
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
      btn.textContent = "F";
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
    if (inCol) {
      showToast("Removed from collection");
    } else {
      // Post-add toast with a "Move…" action so the user can redirect this copy
      const landedFolderId = Number(r.folderId) || 1;
      const folderName = r.folderName || (window._collectionFolders || []).find(f => Number(f.folderId) === landedFolderId)?.name || "Uncategorized";
      const instanceId = Number(r.instanceId) || null;
      if (instanceId && typeof showToastWithAction === "function") {
        showToastWithAction(
          `Added to ${folderName}`,
          "Move…",
          () => openQuickFolderPicker?.(releaseId, instanceId, landedFolderId),
          { type: "success", duration: 6000 }
        );
      } else {
        showToast(`Added to ${folderName}`, "success");
      }
    }
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
// When the user follows an artist/label/entity link from a popup we want
// the resulting search to land on a discography view rather than a flood of
// individual pressings. Default to Masters+ (groups pressings under one
// master and still surfaces standalone releases that have no master) and
// sort oldest-first so the catalogue reads chronologically.
function applyEntityLinkDefaults() {
  const masterPlus = document.querySelector('input[name="result-type"][value="master+"]');
  if (masterPlus) masterPlus.checked = true;
  const sortSel = document.getElementById("f-sort");
  if (sortSel) sortSel.value = "year:asc";
}

function searchArtistFromModal(event, el) {
  event.preventDefault();
  closeModal();
  clearForm();
  document.getElementById("f-artist").value = el.dataset.artist;
  applyEntityLinkDefaults();
  toggleAdvanced(true);
  doSearch(1);
}

function selectAltArtist(event, el) {
  event.preventDefault();
  clearForm();
  document.getElementById("f-artist").value = el.dataset.altName;
  currentArtistId = el.dataset.altId || null;
  applyEntityLinkDefaults();
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
  applyEntityLinkDefaults();
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

// ── Search history dropdowns ─────────────────────────────────────────────
// Per-field search history stored in localStorage. Each text input field
// gets its own history keyed by element ID. Completely independent from
// the bookmarked saved-searches feature.
const _SH_KEY = "sd_search_history";
const _SH_MAX = 50; // max entries per field
let _shData = {};
try { _shData = JSON.parse(localStorage.getItem(_SH_KEY) || "{}"); } catch { _shData = {}; }
let _shActiveField = null;

function _shSave() { localStorage.setItem(_SH_KEY, JSON.stringify(_shData)); }

function _shAdd(fieldId, value) {
  const v = (value ?? "").trim();
  if (!v) return;
  if (!_shData[fieldId]) _shData[fieldId] = [];
  _shData[fieldId] = _shData[fieldId].filter(e => e !== v);
  _shData[fieldId].unshift(v);
  if (_shData[fieldId].length > _SH_MAX) _shData[fieldId] = _shData[fieldId].slice(0, _SH_MAX);
  _shSave();
}

function _shRemove(fieldId, value) {
  if (!_shData[fieldId]) return;
  _shData[fieldId] = _shData[fieldId].filter(e => e !== value);
  if (!_shData[fieldId].length) delete _shData[fieldId];
  _shSave();
}

/** Record current values of all search fields for the given context */
function saveSearchHistory(context) {
  if (context === "main") {
    _shAdd("query", document.getElementById("query")?.value);
    if (document.getElementById("advanced-panel")?.dataset.open === "true") {
      _shAdd("f-artist",  document.getElementById("f-artist")?.value);
      _shAdd("f-release", document.getElementById("f-release")?.value);
      _shAdd("f-label",   document.getElementById("f-label")?.value);
      _shAdd("f-year",    document.getElementById("f-year")?.value);
      _shAdd("f-country", document.getElementById("f-country")?.value);
    }
  } else if (context === "cw") {
    _shAdd("cw-query",   document.getElementById("cw-query")?.value);
    _shAdd("cw-artist",  document.getElementById("cw-artist")?.value);
    _shAdd("cw-release", document.getElementById("cw-release")?.value);
    _shAdd("cw-label",   document.getElementById("cw-label")?.value);
    _shAdd("cw-year",    document.getElementById("cw-year")?.value);
    _shAdd("cw-notes",   document.getElementById("cw-notes")?.value);
  }
}

function _shShow(field) {
  _shHide();
  const entries = _shData[field.id];
  if (!entries?.length) return;
  _shActiveField = field;

  const drop = document.createElement("div");
  drop.className = "sh-dropdown";
  drop.id = "sh-dropdown";

  entries.forEach(val => {
    const row = document.createElement("div");
    row.className = "sh-row";
    const text = document.createElement("span");
    text.className = "sh-text";
    text.textContent = val;
    text.onclick = () => { field.value = val; field.dispatchEvent(new Event("input", { bubbles: true })); field.focus(); _shHide(); };
    const del = document.createElement("span");
    del.className = "sh-del";
    del.textContent = "×";
    del.title = "Remove from history";
    del.onclick = (e) => {
      e.stopPropagation();
      _shRemove(field.id, val);
      row.remove();
      if (!drop.querySelector(".sh-row")) _shHide();
    };
    row.appendChild(text);
    row.appendChild(del);
    drop.appendChild(row);
  });

  const anchor = field.closest("label") || field.parentElement;
  anchor.style.position = "relative";
  // Match the field's actual position and width so dropdown doesn't
  // stretch across flex containers
  drop.style.left = field.offsetLeft + "px";
  drop.style.top = (field.offsetTop + field.offsetHeight) + "px";
  drop.style.width = field.offsetWidth + "px";
  anchor.appendChild(drop);
}

function _shHide() {
  _shActiveField = null;
  document.getElementById("sh-dropdown")?.remove();
}

const _shFieldIds = [
  "query", "f-artist", "f-release", "f-label", "f-year", "f-country",
  "cw-query", "cw-artist", "cw-release", "cw-label", "cw-year", "cw-notes",
];

function _shInit() {
  _shFieldIds.forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("focus", () => _shShow(el));
    el.addEventListener("input", () => _shHide());
  });
  document.addEventListener("mousedown", (e) => {
    if (_shActiveField && !e.target.closest("#sh-dropdown") && !e.target.closest("input")) _shHide();
  });
}
if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", _shInit);
else _shInit();

// ── Active-field highlighting for selects ────────────────────────────────
// Text inputs use CSS :not(:placeholder-shown). Selects need JS to toggle
// .has-value when changed from default (first option).
function _markSelectValue(sel) {
  sel.classList.toggle("has-value", sel.selectedIndex > 0);
}
function _initSelectHighlights() {
  document.querySelectorAll("select").forEach(sel => {
    _markSelectValue(sel);
    sel.addEventListener("change", () => _markSelectValue(sel));
  });
}
/** Call after clearing form to reset select highlights */
function resetSelectHighlights() {
  document.querySelectorAll("select.has-value").forEach(sel => sel.classList.remove("has-value"));
}
/** Re-evaluate all selects (call after programmatically setting field values) */
function refreshFieldHighlights() {
  document.querySelectorAll("select").forEach(sel => _markSelectValue(sel));
}
if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", _initSelectHighlights);
else _initSelectHighlights();
