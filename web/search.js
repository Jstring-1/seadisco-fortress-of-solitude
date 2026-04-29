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

// Reset every form field + status row, but DON'T touch the AI results
// panel. AI entity-link clicks call this directly so they can spawn a
// fresh search without dismissing the panel they came from.
function clearFormFieldsOnly() {
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
}

function clearForm() {
  clearFormFieldsOnly();
  // The clear-form ✕ explicitly dismisses the AI results panel too.
  if (typeof closeAiPanel === "function") closeAiPanel();
}

// AI panel entry-point: clear fields, fill the right one, run the search,
// keep the AI panel visible so the user can pick another row afterward.
// Search results render below the AI panel (the panel is inserted above
// #blurb, which is above #results).
function searchFromAiPanel(field, value) {
  clearFormFieldsOnly();
  if (field === "f-artist") {
    document.getElementById("f-artist").value = value;
    if (typeof applyEntityLinkDefaults === "function") applyEntityLinkDefaults();
    if (typeof toggleAdvanced === "function") toggleAdvanced(true);
  } else if (field === "f-label") {
    document.getElementById("f-label").value = value;
    if (typeof applyEntityLinkDefaults === "function") applyEntityLinkDefaults();
    if (typeof toggleAdvanced === "function") toggleAdvanced(true);
  } else {
    const q = document.getElementById("query");
    if (q) q.value = value;
  }
  // Third arg = keepAiPanel: true → doSearch skips its closeAiPanel call.
  doSearch(1, false, true);
}

// ── Main search ──────────────────────────────────────────────────────────
async function doSearch(page = 1, skipPushState = false, keepAiPanel = false) {
  // A fresh, user-initiated search dismisses the AI results panel. Calls
  // from inside the AI panel pass keepAiPanel=true via searchFromAiPanel
  // so the panel stays put and the new results render below it.
  if (!keepAiPanel && typeof closeAiPanel === "function") {
    closeAiPanel();
  }
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
  let format      = advOpen ? document.getElementById("f-format").value : "";
  const country   = advOpen ? (document.getElementById("f-country")?.value.trim() ?? "") : "";
  let sort        = document.getElementById("f-sort").value;
  // "Hard to Find" mode — biases the query toward physical-format,
  // pre-streaming-era masters that are likely to need YT contributions.
  // Only fills in defaults the user hasn't explicitly set; an explicit
  // Format / Year / Sort always wins.
  const hard2find = !!document.getElementById("f-hard2find")?.checked;
  let yearForSearch = year;
  if (hard2find) {
    if (!format) format = "Vinyl";
    if (!yearForSearch) yearForSearch = "1900-1985";
    if (!sort) sort = "have:asc";
  }
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
    if (yearForSearch) parts.push(`Year: ${yearForSearch}`);
    if (label)   parts.push(`Label: ${label}`);
    if (genre)   parts.push(`Genre: ${genre}`);
    if (style)   parts.push(`Style: ${style}`);
    if (format)  parts.push(`Format: ${format}`);
    if (country) parts.push(`Country: ${country}`);
    if (hard2find) parts.push(`Hard to Find`);
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
    if (yearForSearch) p.set("year",    yearForSearch);
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
      // auth_required = anon user (no Clerk session)
      // no_token      = signed-in but Discogs OAuth not connected
      // Both lead to the same "you need to sign in / connect" CTA.
      if (errData.error === "no_token" || errData.error === "auth_required") {
        setStatus("");
        document.getElementById("results").innerHTML =
          `<div class="empty-state"><div class="empty-state-icon">🔑</div>` +
          `<div class="empty-state-title">Sign in to search Discogs</div>` +
          `<div class="empty-state-subtitle"><a href="/?v=account" onclick="switchView('account');return false;" style="color:var(--accent)">Create a free account</a> to search the full Discogs catalog. The track will keep playing.</div></div>`;
        return;
      }
      if (errData.error === "rate_limited") {
        setStatus("");
        document.getElementById("results").innerHTML =
          `<div class="empty-state"><div class="empty-state-icon">🔑</div>` +
          `<div class="empty-state-title">Sign in to search</div>` +
          `<div class="empty-state-subtitle"><a href="/?v=account" onclick="switchView('account');return false;" style="color:var(--accent)">Sign in</a> and connect your Discogs account to continue.</div></div>`;
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
  const excludeCd = document.getElementById("f-exclude-cd")?.classList.contains("active");
  let filtered = items;
  if (hideOwned && window._collectionIds?.size) {
    filtered = filtered.filter(item => !window._collectionIds.has(Number(item.id)));
  }
  if (excludeCd) {
    filtered = filtered.filter(_sdItemIsNotCdMain);
  }
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
  // "Hard to Find" mode — decorate cards that have no embedded YT
  // videos with a small purple 🎵 badge so users can spot contribution
  // opportunities. No-op when the toggle isn't on.
  if (document.getElementById("f-hard2find")?.checked) {
    _sdDecorateNoVideoCards(filtered);
  }
}

// ── "Hard to Find" toggle UX ─────────────────────────────────────────
// Show/hide the helper hint text when the toggle flips. Doesn't auto-
// re-run the search — the user still hits the search button.
function _sdHard2FindChanged(cb) {
  const hint = document.getElementById("f-hard2find-hint");
  if (hint) hint.style.display = cb.checked ? "" : "none";
}
window._sdHard2FindChanged = _sdHard2FindChanged;

// ── "No CDs" client-side exclude (CD-family + digital formats) ───────
// Discogs returns each search result's `format` array reflecting that
// master's main_release (or the release itself). If any token reads
// like CD-family OR digital (the modern equivalent of "obviously
// digitized") we treat the item as digitization-likely and exclude
// it. The toggle's icon is a CD with a strikethrough but the intent
// is broader — "physical, analog-era only".
//
// Caveat: this is a heuristic against the search-result format array,
// not a per-master versions fetch. A master whose main_release is
// CD/digital will be excluded even if vinyl pressings exist, and
// vinyl-main-release masters with later CD reissues will slip
// through. Users wanting strict "no digital pressings anywhere" need
// the master's full versions list — that's a future add.
const _SD_CD_FORMAT_TOKENS = new Set([
  // CD family
  "cd", "cdr", "sacd", "hdcd", "cd-rom", "mini-cd",
  // Optical video / data
  "dvd", "dvdr", "dvd-rom", "blu-ray", "blu-ray-r", "minidisc",
  // Pure digital — Discogs uses "File" as the umbrella; codec names
  // may show up in the descriptions array next to it. Catching the
  // codec list is paranoid but the toggle's intent is "no digital".
  "file", "digital", "streaming",
  "mp3", "flac", "alac", "aac", "wav", "aiff", "ogg", "wma", "ape",
  "usb", "memory stick",
]);
function _sdItemIsNotCdMain(item) {
  // Artist/label cards have no format — keep them.
  if (item.type !== "release" && item.type !== "master") return true;
  const fmt = Array.isArray(item.format) ? item.format : [];
  for (const tok of fmt) {
    const norm = String(tok || "").trim().toLowerCase();
    if (_SD_CD_FORMAT_TOKENS.has(norm)) return false;
  }
  return true;
}

function _sdToggleExcludeCd(btn) {
  const on = btn.classList.toggle("active");
  btn.setAttribute("aria-pressed", on ? "true" : "false");
  // Re-render from the cached result set so the filter takes effect
  // without a Discogs re-query. _lastResults holds the full server
  // response; the renderer applies the filter itself.
  if (window._lastResults && window._lastResults.length) {
    renderResults(window._lastResults, false);
  }
}
window._sdToggleExcludeCd = _sdToggleExcludeCd;

// ── Home strip Recent / Suggestions / Submitted toggle ────────────────
//
// Defaults to "recent" on every fresh page load. Optionally honours
// a ?strip=suggestions or ?strip=submitted URL param so links can
// deep-link into a specific tab. Click handlers replaceState (no
// new history entry) the URL so the address bar stays in sync with
// what the user sees. We deliberately do NOT persist across
// sessions — only this URL handshake.
function _sdInitialHomeStripMode() {
  try {
    const v = new URLSearchParams(location.search).get("strip");
    if (v === "suggestions" || v === "submitted") return v;
  } catch {}
  return "recent";
}
window._sdHomeStripMode = _sdInitialHomeStripMode();
window._sdHomeStripFilter = "";

// Force every tab's class + inline color to match the current mode.
// Called both on click (via _sdSwitchHomeStripTab) and on initial
// render so the visual state can never drift from the data state.
function _sdSyncHomeStripTabsVisual() {
  const m = window._sdHomeStripMode || "recent";
  const tabs = {
    recent:      document.getElementById("rr-tab-recent"),
    suggestions: document.getElementById("rr-tab-suggestions"),
    submitted:   document.getElementById("rr-tab-submitted"),
  };
  for (const [k, el] of Object.entries(tabs)) {
    if (!el) continue;
    el.classList.toggle("rr-tab-active", k === m);
    el.style.color = k === m ? "var(--text)" : "var(--muted)";
  }
}
window._sdSyncHomeStripTabsVisual = _sdSyncHomeStripTabsVisual;

// On page boot, sync the visual tab state once the DOM is ready so
// the static markup (Recent active by default) matches whatever
// mode was selected via ?strip=. Idempotent.
if (typeof document !== "undefined") {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => _sdSyncHomeStripTabsVisual());
  } else {
    setTimeout(_sdSyncHomeStripTabsVisual, 0);
  }
}

function _sdSwitchHomeStripTab(mode) {
  const m = mode === "suggestions" ? "suggestions"
          : mode === "submitted"   ? "submitted"
          : "recent";
  if (window._sdHomeStripMode === m) {
    // Idempotent re-click: still re-sync visual state in case
    // something else (markup edit, race, mid-load DOM update) left
    // the highlight on the wrong tab.
    _sdSyncHomeStripTabsVisual();
    return;
  }
  window._sdHomeStripMode = m;
  _sdSyncHomeStripTabsVisual();
  _sdReflectHomeStripModeInUrl(m);
  // Reload strip from the appropriate source.
  loadRandomRecords(false);
}
window._sdSwitchHomeStripTab = _sdSwitchHomeStripTab;

// Mirror the active strip tab in the URL via replaceState so the
// browser address bar matches what the user sees AND a copied URL
// re-opens the same tab. replaceState (not pushState) so back-button
// doesn't walk through tab clicks — only meaningful navigation.
// Default tab "recent" drops the param entirely to keep URLs clean.
function _sdReflectHomeStripModeInUrl(mode) {
  try {
    const u = new URL(location.href);
    if (mode === "recent") u.searchParams.delete("strip");
    else u.searchParams.set("strip", mode);
    history.replaceState(history.state, "", u.toString());
  } catch {}
}

// Filter input on the strip — applies to whatever's currently
// displayed (recent / suggestions / submitted tracks). Pure
// client-side: re-renders from _randomAll without re-fetching.
function _sdHomeStripFilterChanged(input) {
  window._sdHomeStripFilter = String(input?.value || "").trim().toLowerCase();
  // Reset paging so the filter applies from the top.
  _randomShown = 0;
  const grid = document.getElementById("random-records-grid");
  if (grid) grid.innerHTML = "";
  // Render synchronously — no fetch needed.
  _sdRenderRandomSlice();
}
window._sdHomeStripFilterChanged = _sdHomeStripFilterChanged;

// Apply the current filter to _randomAll and return the visible
// subset. Match against title (handles "Artist - Title" composed),
// individual artist field, label, year — broad to feel responsive.
function _sdFilterRandom(items) {
  const q = window._sdHomeStripFilter || "";
  if (!q) return items;
  return items.filter(it => {
    const hay = [
      it.title, it.artist, (it.label || []).join(" "),
      it.country, String(it.year || "")
    ].filter(Boolean).join(" ").toLowerCase();
    return hay.includes(q);
  });
}

// Render the next slice of _randomAll into the grid, honoring filter
// + paging. Extracted from loadRandomRecords so the filter input can
// re-run rendering without a refetch.
function _sdRenderRandomSlice() {
  const grid = document.getElementById("random-records-grid");
  if (!grid) return;
  const filtered = _sdFilterRandom(_randomAll);
  const slice = filtered.slice(_randomShown, _randomShown + _RANDOM_PAGE);
  if (!slice.length && _randomShown === 0) {
    if (window._sdHomeStripFilter) {
      grid.innerHTML = `<div style="grid-column:1/-1;text-align:center;color:var(--muted);font-size:0.85rem;padding:1rem 0">No matches.</div>`;
    }
    return;
  }
  for (let i = 0; i < slice.length; i++) {
    const card = renderCard(slice[i], _randomShown + i);
    grid.insertAdjacentHTML("beforeend", card);
  }
  _randomShown += slice.length;
}
window._sdRenderRandomSlice = _sdRenderRandomSlice;

// Module-level cache of (type:id) → hasVideos so a second-page load
// doesn't re-query items already known. null = not yet checked.
const _sdHasVideosCache = new Map();

async function _sdDecorateNoVideoCards(items) {
  if (!Array.isArray(items) || !items.length) return;
  // Only ask about masters/releases — artists/labels don't have videos.
  // Skip items already in the cache (decorated on a prior page).
  const todo = items.filter(it => {
    if (it.type !== "master" && it.type !== "release") return false;
    return !_sdHasVideosCache.has(`${it.type}:${it.id}`);
  }).map(it => ({ id: Number(it.id), type: it.type }));
  if (todo.length) {
    try {
      const r = await fetch("/api/has-videos", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items: todo }),
      });
      if (r.ok) {
        const j = await r.json();
        for (const row of (j.results || [])) {
          _sdHasVideosCache.set(`${row.type}:${row.id}`, row.hasVideos);
        }
      }
    } catch { /* badge stays hidden — no-op */ }
  }
  // Apply to DOM. Find each card by data-id / data-type — but the
  // existing card markup doesn't carry those, so we match via the
  // onclick attribute that openModal injects (`'12345','master',`).
  const grid = document.getElementById("results");
  if (!grid) return;
  for (const it of items) {
    if (it.type !== "master" && it.type !== "release") continue;
    const status = _sdHasVideosCache.get(`${it.type}:${it.id}`);
    if (status !== false) continue; // null=unknown, true=has videos — skip
    // Find the card. openModal calls embed the id in onclick — escape
    // for attribute-selector use.
    const sel = `a[onclick*="openModal(event,'${String(it.id)}','${it.type}'"]`;
    const card = grid.querySelector(sel);
    if (!card || card.querySelector(".card-needs-yt-badge")) continue;
    const thumbWrap = card.querySelector(".card-thumb-wrap");
    if (!thumbWrap) continue;
    const badge = document.createElement("span");
    badge.className = "card-needs-yt-badge";
    badge.title = "No YouTube videos yet — open the album to suggest one";
    badge.textContent = "🎵";
    thumbWrap.appendChild(badge);
  }
}
window._sdDecorateNoVideoCards = _sdDecorateNoVideoCards;

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

  // width/height attrs let the browser reserve layout space before
  // the image arrives, eliminating the cumulative-layout-shift jolt
  // when a grid of cards loads. The 300×300 ratio matches the .card
  // CSS aspect-ratio; actual display size still comes from CSS.
  const thumb = item.cover_image
    ? `<img src="${item.cover_image}" alt="${escHtml(title)}" loading="lazy" width="300" height="300" decoding="async" />`
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

  // ── Badge strip: fixed order matching the navbar — collection,
  // wantlist, lists, inventory, favorite. Each badge uses the same
  // line-art SVG icon as its nav tab and is colored to match.
  // C / W / F always render (active = full color, inactive = muted
  // placeholder). L / I only render when the user has ANY items in
  // that category — if the corresponding nav tab is hidden (because
  // the user has zero), so are the placeholders here.
  const navIcon = (typeof window._sdNavIconSvg === "function") ? window._sdNavIconSvg : (() => "");
  let badges = "";
  const releaseId = item.id;
  const isReleaseOrMaster = type === "release" || type === "master";
  const inCol = releaseId && type === "release" && window._collectionIds?.has(releaseId);
  const inWant = releaseId && type === "release" && window._wantlistIds?.has(releaseId);
  const userHasLists = window._listMembership && Object.keys(window._listMembership).length > 0;
  const userHasInventory = (window._inventoryIds?.size ?? 0) > 0;
  if (releaseId && isReleaseOrMaster) {
    if (type === "release") {
      badges += `<span class="card-badge badge-collection${inCol ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleCollectionFromCard(this,${releaseId})" title="${inCol ? "Remove from collection" : "Add to collection"}">${navIcon("collection")}</span>`;
      badges += `<span class="card-badge badge-wantlist${inWant ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleWantlistFromCard(this,${releaseId})" title="${inWant ? "Remove from wantlist" : "Add to wantlist"}">${navIcon("wantlist")}</span>`;
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
      const colSup  = colCount  >= 2 ? `<sup class="card-badge-count">${colCount}</sup>`  : "";
      const wantSup = wantCount >= 2 ? `<sup class="card-badge-count">${wantCount}</sup>` : "";
      badges += `<span class="card-badge badge-collection${colActive ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();openModal(event,'${releaseId}','master','')" title="${colTitle}">${navIcon("collection")}${colSup}</span>`;
      badges += `<span class="card-badge badge-wantlist${wantActive ? " is-active" : ""}" onclick="event.preventDefault();event.stopPropagation();openModal(event,'${releaseId}','master','')" title="${wantTitle}">${navIcon("wantlist")}${wantSup}</span>`;
    }
  }
  // Favorite badge — always rendered (placeholder when not favorited).
  // Order: collection, wantlist, favorites, inventory, lists — matches
  // the navbar tab order.
  const favKey = `${type}:${item.id}`;
  const isFav = window._favoriteKeys?.has(favKey);
  if (type && item.id)
    badges += `<span class="card-badge badge-favorite${isFav ? " is-favorite" : ""}" onclick="event.preventDefault();event.stopPropagation();toggleFavoriteFromCard(this,${item.id},'${type}')" title="${isFav ? "Remove from favorites" : "Add to favorites"}">${navIcon("favorites")}</span>`;
  if (releaseId && isReleaseOrMaster) {
    // Inventory badge — placeholder visible whenever the user has any
    // inventory items. Active when this release is one of them.
    if (userHasInventory) {
      const inInv = window._inventoryIds?.has(releaseId);
      const iTitle = inInv ? "In your inventory" : "Not in your inventory";
      badges += `<span class="card-badge badge-inventory${inInv ? " is-active" : ""}" title="${iTitle}">${navIcon("inventory")}</span>`;
    }
    // Lists badge — same placeholder rule.
    if (userHasLists) {
      const lists = window._listMembership?.[releaseId];
      const inList = !!(lists && lists.length);
      const names = inList ? lists.map(l => l.listName).join(", ") : "";
      const lTitle = inList ? `In list: ${escHtml(names)}` : "Not in any of your lists";
      badges += `<span class="card-badge badge-list${inList ? " is-active" : ""}" title="${lTitle}">${navIcon("lists")}</span>`;
    }
  }

  // Multi-instance "(N)" badge at lower-left — shown when user owns multiple copies
  // of the same release. Clicking opens a popover listing each instance.
  const instanceCount = Number(
    item._instanceCount ?? (isRelease ? (window._collectionInstanceCounts?.[item.id] ?? 0) : 0)
  );
  const instanceBadge = (instanceCount > 1 && isRelease)
    ? `<span class="card-instance-badge" onclick="event.preventDefault();event.stopPropagation();openInstancesPopover(event,${item.id})" title="${instanceCount} copies in your collection — click to view">(${instanceCount})</span>`
    : "";
  // Contribution-count badge — only on cards from the Picks page (or
  // anywhere _contributionCount is populated by the server). Shows the
  // total number of crowd-sourced YT overrides for this album so users
  // can compare at a glance.
  const contributionCount = Number(item._contributionCount) || 0;
  const contributionBadge = contributionCount > 0
    ? `<span class="card-contribution-badge" title="${contributionCount} user-contributed YouTube ${contributionCount === 1 ? "video" : "videos"} for this album">🎵 ${contributionCount}</span>`
    : "";
  const thumbWrap = `<div class="card-thumb-wrap">${thumb}<div class="card-thumb-badges">${badges}</div>${instanceBadge}${contributionBadge}</div>`;

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

  // Admin-only "+ add to Blues DB" icon — renders only when:
  //   • admin is signed in (window._isAdmin set in shared.js)
  //   • the card's genre array contains "Blues"
  //   • the artist isn't already in the DB (cached names lookup)
  // MUST be a <span> not <a>, because the entire card is wrapped in
  // an outer <a> and nested anchors are invalid HTML (browsers auto-
  // Inline "add to Blues DB" + button on artist cards is disabled
  // per admin request — curation happens via /admin → Blues panel
  // manually. The detection logic and _bluesAddArtistByName helper
  // remain so flipping this back on later is one line.
  let bluesAddBtn = "";
  return `
    <a ${cardAttrs}${animStyle}>
      ${thumbWrap}
      <div class="card-body">
        ${artist ? `<div class="card-artist">${bluesAddBtn}${escHtml(artist)}</div>` : ""}
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
  // SeaDisco new search — kind controls which field gets populated.
  // Routes through searchFromAiPanel so the AI panel stays open and the
  // search results render below it.
  let newSearch = "";
  if (kind === "artist") {
    newSearch = `event.preventDefault();searchFromAiPanel('f-artist','${safeEsc}')`;
  } else if (kind === "label") {
    newSearch = `event.preventDefault();searchFromAiPanel('f-label','${safeEsc}')`;
  } else {
    // release/album/general — put into main query
    newSearch = `event.preventDefault();searchFromAiPanel('query','${safeEsc}')`;
  }
  const cwField = kind === "artist" ? "cw-artist" : kind === "label" ? "cw-label" : "cw-query";
  // Quote the entity name for Wikipedia so we get an exact-phrase match;
  // for labels add the unquoted "record label" hint as extra search context.
  const wikiQ = kind === "label" ? `"${name}" record label` : `"${name}"`;
  return `<a href="#" class="ai-entity-link" onclick="${newSearch}" title="New SeaDisco search for ${escHtml(name)}">${escHtml(name)}</a>` +
    ` <a href="#" class="track-search-icon ai-entity-icon" onclick="event.preventDefault();searchCollectionFor('${cwField}','${safeEsc}')" title="Search your records for ${escHtml(name)}">⌕</a>` +
    ` <a href="#" class="wiki-icon ai-entity-icon" onclick="event.preventDefault();openWikiPopup('${escHtml(String(wikiQ).replace(/'/g, "\\'"))}')" title="Wikipedia: ${escHtml(name)}">W</a>`;
}

// Build a Set of "artist|album" normalized keys from items already
// rendered in the user's session — primarily _lastResults (search
// results they've seen), recents, and the home strip cards. This is
// a coarse fingerprint, not a full library scan, but combined with
// the server-side prompt instruction it dampens the LLM's tendency
// to recommend things the user clearly already knows about.
function _sdAiBuildLibraryFingerprintSet() {
  const set = new Set();
  const add = (item) => {
    const key = _sdAiNormalizeItemKey(item);
    if (key) set.add(key);
  };
  // Local recents (history) — already-opened albums.
  try {
    const hist = (typeof _readHistory === "function") ? _readHistory() : [];
    for (const h of hist) add(h);
  } catch {}
  // Last search results — passive but useful signal.
  if (Array.isArray(window._lastResults)) {
    for (const it of window._lastResults) add(it);
  }
  return set;
}

// Normalize an album card item shape ({ title, ... } where title is
// either "Artist - Album" or just the album name) to a stable key.
function _sdAiNormalizeItemKey(item) {
  if (!item) return "";
  const raw = String(item.title || "");
  const idx = raw.indexOf(" - ");
  let artist = idx > 0 ? raw.slice(0, idx) : "";
  let album  = idx > 0 ? raw.slice(idx + 3) : raw;
  // Some shapes carry artist separately.
  if (Array.isArray(item.artists) && item.artists[0]?.name) artist = item.artists[0].name;
  return _sdAiNormalize(artist) + "|" + _sdAiNormalize(album);
}

// Same shape but for an AI recommendation { artist, album, name, type }.
function _sdAiNormalizeRecKey(rec) {
  if (!rec) return "";
  const artist = rec.artist || "";
  let album = rec.album || "";
  if (!album && rec.name) {
    // "Album by Artist" → split
    const m = /^(.*?)\s+by\s+(.*)$/i.exec(rec.name);
    if (m) album = m[1];
    else album = rec.name;
  }
  return _sdAiNormalize(artist) + "|" + _sdAiNormalize(album);
}

// Lowercase, strip parens/brackets, collapse whitespace, drop
// punctuation that varies (apostrophes, hyphens). Aggressive but
// good enough for fuzzy fingerprinting.
function _sdAiNormalize(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\(.*?\)/g, "")
    .replace(/\[.*?\]/g, "")
    .replace(/[''""`.,!?\-_]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

async function doAiSearch(q) {
  if (!q) { setStatus("Enter a question or description to search with AI.", false); return; }
  // Stash the query into #query first so saveSearchHistory("main") below
  // captures it into the existing per-field history (sd_search_history)
  // — same dropdown that surfaces general searches.
  const qInput = document.getElementById("query");
  if (qInput && qInput.value.trim() !== q.trim()) qInput.value = q;
  saveSearchHistory("main");
  // Only call switchView when we aren't already on the search view —
  // when we are, switchView's else-branch tears down and rebuilds the
  // Recent grid (loadRandomRecords does grid.innerHTML = ...) which
  // causes a noticeable flash on every AI submit. The Recent grid
  // already shows correctly; AI search just floats a panel over it.
  const searchVisible = document.getElementById("search-view")?.style.display !== "none";
  if (!searchVisible) {
    switchView("search", true);
    setActiveTab("search");
  }
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
    if (r.status === 401) { setStatus("Sign in and connect your Discogs account to use AI search.", false); return; }
    if (!r.ok) { setStatus("AI search failed. Try again.", false); return; }
    let { recommendations, blurb } = await r.json();
    if (!recommendations?.length) { setStatus("No recommendations returned.", false); return; }
    // Belt-and-suspenders post-filter: the server-side prompt asks
    // Claude to skip the user's library, but the LLM doesn't always
    // obey perfectly. Drop any recommendation whose normalized
    // "artist - album" string fuzzy-matches a known collection /
    // wantlist title. We don't have id-based mapping for AI picks
    // (Claude returns titles, not Discogs ids) so name match is the
    // best we can do client-side.
    try {
      const seen = _sdAiBuildLibraryFingerprintSet();
      if (seen.size) {
        const keep = [];
        for (const rec of recommendations) {
          const key = _sdAiNormalizeRecKey(rec);
          if (key && seen.has(key)) continue;
          keep.push(rec);
        }
        if (keep.length !== recommendations.length) {
          recommendations = keep;
        }
      }
    } catch { /* filter is best-effort */ }
    if (!recommendations.length) { setStatus("All recommendations were already in your library — try a different query.", false); return; }
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
const _HISTORY_MAX_CLIENT = 576;
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
    const r = await apiFetch("/api/user/recent?limit=576");
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
  // Anonymous visitors don't see the home strip — they get the
  // waitlist splash panel (#anon-splash) instead. Hide the strip
  // defensively in case any path tries to render into it.
  if (!window._clerk?.user) {
    wrap.style.display = "none";
    return;
  }

  // First call (or full reload): rebuild from localStorage history.
  // For logged-out users with no local history (and signed-in users
  // with empty history), fall back to a random sample of admin's
  // favorites — gives anon visitors a curated set of records to
  // browse on the home page instead of nothing.
  if (!more) {
    _randomShown = 0;
    const titleEl = document.getElementById("random-records-title"); // legacy hidden span
    const tabWrap = document.getElementById("random-records-title-wrap");
    if (tabWrap) tabWrap.style.display = "";
    let isSuggested = false;
    let titleText = "Recent";

    if (window._sdHomeStripMode === "submitted") {
      // ── Submitted (the SIGNED-IN user's own contributions) ──────
      // Distinct list of albums the current user has submitted YT
      // overrides for. Empty for users who haven't contributed yet —
      // placeholder text below points them at the contribution path.
      _randomAll = [];
      if (window._clerk?.user) {
        try {
          const r = await apiFetch("/api/user/my-submitted-albums?limit=200");
          if (r.ok) {
            const j = await r.json();
            if (Array.isArray(j?.items) && j.items.length) {
              _randomAll = j.items.map(it => ({ ...it, _addedAt: 0, _isSuggested: true }));
            }
          }
        } catch { /* leave empty — placeholder below */ }
      }
      titleText = "Submitted";
      isSuggested = true;
    } else if (window._sdHomeStripMode === "suggestions") {
      // ── Suggestions tab on main search ──────────────────────────
      // Signed-in: per-user background-generated feed. Anon: fall
      // back to community-picks so the tab still has content.
      _randomAll = [];
      if (window._clerk?.user) {
        try {
          // Fetch the full saved batch (cap 1000) — the strip pages
          // through it client-side via Load More so dismissing many
          // doesn't leave the user empty-handed.
          const r = await apiFetch("/api/user/personal-suggestions?limit=1000");
          if (r.ok) {
            const j = await r.json();
            if (Array.isArray(j?.items) && j.items.length) {
              _randomAll = j.items.map(it => ({ ...it, _addedAt: 0, _isSuggested: true }));
            }
          }
        } catch { /* fall through */ }
      }
      if (!_randomAll.length) {
        try {
          const r = await fetch("/api/contributed-favorites/sample?limit=24&order=most", { cache: "no-store" });
          if (r.ok) {
            const j = await r.json();
            if (Array.isArray(j?.items) && j.items.length) {
              _randomAll = j.items.map(it => ({ ...it, _addedAt: 0, _isSuggested: true }));
            }
          }
        } catch { /* leave empty */ }
      }
      titleText = "Suggestions";
      isSuggested = true;
    } else {
      // ── Recent (default) ────────────────────────────────────────
      _loadHistoryIntoRandom();
      if (!_randomAll.length) {
        // No local history — fall back to community-picks → admin-
        // favorites so anon users land on something curated.
        let gotIt = false;
        try {
          const r = await fetch("/api/contributed-favorites/sample?limit=24&order=most", { cache: "no-store" });
          if (r.ok) {
            const j = await r.json();
            if (Array.isArray(j?.items) && j.items.length) {
              _randomAll = j.items.map(it => ({ ...it, _addedAt: 0, _isSuggested: true }));
              gotIt = true;
            }
          }
        } catch {}
        if (!gotIt) {
          try {
            const r = await fetch("/api/admin-favorites/sample?limit=24", { cache: "no-store" });
            if (r.ok) {
              const j = await r.json();
              if (Array.isArray(j?.items) && j.items.length) {
                _randomAll = j.items.map(it => ({ ...it, _addedAt: 0, _isSuggested: true }));
              }
            }
          } catch {}
        }
        titleText = "Recent";
        isSuggested = true;
      } else {
        titleText = "Recent";
        isSuggested = false;
      }
    }

    if (titleEl) titleEl.textContent = titleText;
    // Filter + Sort are available in both Recent and Suggestions
    // modes — same control set so the toggle feels symmetric. Clear
    // is Recent-only (it wipes local history; meaningless against a
    // server-generated suggestions feed).
    const sortLabel = document.querySelector('#random-records-controls .sd-filter-label');
    const clearBtn = document.getElementById("recent-clear-btn");
    if (sortLabel) sortLabel.style.display = "";
    if (clearBtn) clearBtn.style.display = isSuggested ? "none" : "";

    if (!_randomAll.length) {
      if (window._sdHomeStripMode === "submitted") {
        wrap.style.display = "";
        grid.innerHTML = `<div style="grid-column:1/-1;text-align:center;color:var(--muted);font-size:0.85rem;padding:2rem 1rem">You haven't submitted any tracks yet. Open any album that has missing YouTube videos and click the <span style="color:#c084fc">🎵 N missing</span> link in the tracklist heading to contribute. Your submissions will appear here.</div>`;
        return;
      }
      if (window._sdHomeStripMode === "suggestions") {
        wrap.style.display = "";
        const isSignedIn = !!window._clerk?.user;
        grid.innerHTML = `<div style="grid-column:1/-1;text-align:center;color:var(--muted);font-size:0.85rem;padding:2rem 1rem">${
          isSignedIn
            ? "No suggestions yet — they're generated hourly from your library's taste profile. Check back soon."
            : "Sign in to see personalized suggestions, or browse Submitted above."
        }</div>`;
        return;
      }
      wrap.style.display = "none";
      return;
    }
  }

  // Filter is client-side and re-runs on every render so the visible
  // grid always matches the current search input.
  const filteredAll = _sdFilterRandom(_randomAll);
  const slice = filteredAll.slice(_randomShown, _randomShown + _RANDOM_PAGE);
  if (!slice.length) {
    if (!more && window._sdHomeStripFilter) {
      grid.innerHTML = `<div style="grid-column:1/-1;text-align:center;color:var(--muted);font-size:0.85rem;padding:1rem 0">No matches.</div>`;
    }
    return;
  }
  // Each card gets a small × overlay. In Recent mode, × drops the row
  // from local history. In Suggestions mode, × banishes that
  // master/release from the user's personal feed forever (server-side
  // record). On Submitted (global community feed) we don't show ×.
  const inSuggestions = window._sdHomeStripMode === "suggestions";
  const inSubmitted   = window._sdHomeStripMode === "submitted";
  const html = slice.map((item, i) => {
    const card = renderCard(item, _randomShown + i);
    const safeId = escHtml(String(item.id));
    const safeType = escHtml(String(item.type || "master"));
    let dismiss = "";
    if (inSubmitted) {
      // Global feed — no per-user dismiss.
    } else if (inSuggestions && window._clerk?.user) {
      dismiss = `<button class="recent-dismiss" onclick="_sdDismissSuggestion(event,'${safeId}','${safeType}')" title="Hide this suggestion forever">✕</button>`;
    } else if (!item._isSuggested) {
      dismiss = `<button class="recent-dismiss" onclick="removeFromHistory(event,'${safeId}')" title="Remove from history">✕</button>`;
    }
    return `<div class="recent-wrap" data-hist-id="${safeId}">${card}${dismiss}</div>`;
  }).join("");
  grid.querySelector(".random-load-more")?.remove();
  if (more) {
    grid.insertAdjacentHTML("beforeend", html);
  } else {
    grid.innerHTML = html;
  }
  _randomShown += slice.length;

  // Add load-more if there are more to show
  if (_randomShown < filteredAll.length) {
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

/** Banish a suggestion from the user's personal feed. Optimistic
 *  DOM removal + server POST. The next background run will skip the
 *  banished (id,type) on every subsequent pass. */
function _sdDismissSuggestion(ev, id, type) {
  if (ev) { ev.preventDefault(); ev.stopPropagation(); }
  const safeId = String(id);
  const safeType = type === "release" ? "release" : "master";
  // Optimistic removal — drop the card AND the underlying _randomAll
  // entry so re-render / paging doesn't bring it back.
  const el = document.querySelector(`.recent-wrap[data-hist-id="${CSS.escape(safeId)}"]`);
  if (el) el.remove();
  if (Array.isArray(_randomAll)) {
    _randomAll = _randomAll.filter(it => !(String(it.id) === safeId && String(it.type) === safeType));
  }
  if (!_randomAll.length) {
    const wrap = document.getElementById("random-records");
    if (wrap) wrap.style.display = "none";
  }
  // Server-side banish — fire-and-forget. If it fails the next page
  // load will pull the same card back, but the user can dismiss again.
  if (window._clerk?.user && typeof apiFetch === "function") {
    try {
      apiFetch("/api/user/personal-suggestions/dismiss", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: Number(safeId), type: safeType }),
      }).catch(() => {});
    } catch {}
  }
}
window._sdDismissSuggestion = _sdDismissSuggestion;

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

// History-change listener — DOES NOT auto-rebuild the visible grid.
// Rebuilding would reset _randomShown to 0 and call grid.innerHTML = ...
// which (a) loses the user's scroll position and (b) drops every
// "Load more" page they'd already opened. The strip refreshes whenever
// the user navigates away from search and back (switchView fallthrough
// calls showRandomRecords). For the rare "I want to see the change
// without leaving the page" case, the user can click the logo (goHome)
// which force-rebuilds. This listener is left in place as a hook for
// future incremental in-place updates that don't tear down the grid.
window.addEventListener("sd-history-change", () => {
  // Intentionally a no-op — see comment above.
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
_shData = getStorageJSON(_SH_KEY, {});
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
    // Dismiss when the click target is outside the dropdown AND
    // outside the currently-active field. Earlier guard skipped
    // dismissal whenever the click was on ANY input, which kept the
    // Artist dropdown lingering on top of Country/Genre/Style when
    // the user moved to another field.
    if (_shActiveField && !e.target.closest("#sh-dropdown") && e.target !== _shActiveField) {
      _shHide();
    }
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
