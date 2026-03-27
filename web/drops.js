// ── Fresh releases grid ──────────────────────────────────────────────────
function renderFreshGrid(releases) {
  const grid = document.getElementById("fresh-releases-grid");
  if (!grid) return;
  if (!releases.length) {
    grid.innerHTML = `<div style="color:var(--muted);font-size:0.8rem;grid-column:1/-1;text-align:center;padding:2rem 0">No releases found for this tag.</div>`;
    return;
  }
  grid.innerHTML = releases.map((rel, i) => {
    const img = rel.cover_url
      ? `<img src="${escHtml(rel.cover_url)}" alt="${escHtml(rel.release_name ?? '')}" loading="lazy" onerror="this.style.display='none'">`
      : `<div class="fresh-card-no-img">♪</div>`;

    const types = [rel.primary_type, rel.secondary_type].filter(Boolean).join(" · ");

    const dateStr = rel.release_date ? String(rel.release_date).slice(0, 10) : "";
    const date = dateStr
      ? new Date(dateStr + "T12:00:00").toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" })
      : "";

    const mbData = `data-artist="${escHtml(rel.artist_credit_name ?? '')}" data-title="${escHtml(rel.release_name ?? '')}" data-cover="${escHtml(rel.cover_url ?? '')}" data-date="${escHtml(dateStr)}" data-type="${escHtml(types)}" data-tags="${escHtml((rel.tags ?? []).join(','))}" data-rgmbid="${escHtml(rel.release_group_mbid ?? '')}" data-artmbids="${escHtml((rel.artist_mbids ?? []).join(','))}"`;

    return `<div class="card fresh-card card-animate" style="--i:${Math.min(i, 20)}" ${mbData} onclick="openDropCardPopup(this)">
      <div class="fresh-card-img">${img}</div>
      <div class="fresh-card-body">
        <div class="fresh-card-title">${escHtml(rel.release_name ?? "Unknown")}</div>
        <div class="fresh-card-artist">${escHtml(rel.artist_credit_name ?? "")}</div>
        ${date ? `<div class="fresh-card-date">${date}</div>` : ""}
        ${types ? `<div class="fresh-card-type">${escHtml(types)}</div>` : ""}
      </div>
    </div>`;
  }).join("");
}

let _freshActiveTag = "";
let _freshAll = [];
let _freshBrowse = [];
let _freshSearchTimer = null;

function filterFreshByTag(tag) {
  const pills = document.querySelectorAll(".fresh-tag-pill");
  pills.forEach(p => p.classList.toggle("active", p.dataset.tag === tag));
  _freshActiveTag = tag;
  const gs = document.getElementById("fresh-genre-select");
  const ss = document.getElementById("fresh-style-select");
  if (gs) gs.value = "";
  if (ss) { ss.value = ""; ss.style.display = "none"; }
  const filtered = tag ? _freshAll.filter(r => (r.tags ?? []).includes(tag)) : _freshAll;
  renderFreshGrid(filtered);
  const allBtn = document.getElementById("fresh-all-btn");
  const label  = document.getElementById("fresh-active-label");
  const searchInput = document.getElementById("fresh-tag-input");
  const inSearch = searchInput && searchInput.value.trim();
  if (allBtn) allBtn.classList.toggle("active", !tag);
  if (label) {
    if (tag) label.textContent = tag;
    else if (inSearch) label.textContent = `"${searchInput.value.trim()}" — ${_freshAll.length} result${_freshAll.length !== 1 ? "s" : ""}`;
    else label.textContent = "All releases";
  }
}

function _normTag(s) {
  return s.toLowerCase()
    .replace(/[&\/,]/g, " ")
    .replace(/\b(and|'n')\b/g, " ")
    .replace(/[-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function _applyGenreStyleFilter(label) {
  document.querySelectorAll(".fresh-tag-pill").forEach(p => p.classList.remove("active"));
  _freshActiveTag = "";
  const allBtn = document.getElementById("fresh-all-btn");
  const activeLabel = document.getElementById("fresh-active-label");
  if (allBtn) allBtn.classList.remove("active");
  if (activeLabel) activeLabel.textContent = label || "All releases";
  const ti = document.getElementById("fresh-tag-input");
  if (ti) ti.value = "";
}

function onFreshGenreChange() {
  const genre = document.getElementById("fresh-genre-select")?.value ?? "";
  const ss    = document.getElementById("fresh-style-select");
  if (ss) {
    if (genre && GENRE_STYLES[genre]) {
      ss.innerHTML = '<option value="">Style</option>' +
        GENRE_STYLES[genre].map(s => `<option value="${escHtml(s)}">${escHtml(s)}</option>`).join("");
      ss.style.display = "";
    } else {
      ss.innerHTML = '<option value="">Style</option>';
      ss.style.display = "none";
    }
    ss.value = "";
  }
  filterFreshByGenreStyle();
}

function filterFreshByGenreStyle() {
  const genre = document.getElementById("fresh-genre-select")?.value ?? "";
  const style = document.getElementById("fresh-style-select")?.value ?? "";
  const filter = style || genre;
  if (!filter) {
    filterFreshByTag("");
    return;
  }
  _applyGenreStyleFilter(filter);
  const normFilter = _normTag(filter);
  const filtered = _freshAll.filter(r =>
    (r.tags ?? []).some(t => {
      const nt = _normTag(t);
      return nt === normFilter || nt.includes(normFilter) || normFilter.includes(nt);
    })
  );
  renderFreshGrid(filtered);
}

function debounceFreshSearch(val) {
  clearTimeout(_freshSearchTimer);
  const trimmed = val.trim();
  if (!trimmed) {
    _freshAll = _freshBrowse;
    _freshActiveTag = "";
    const gs = document.getElementById("fresh-genre-select");
    const ss = document.getElementById("fresh-style-select");
    if (gs) gs.value = "";
    if (ss) { ss.value = ""; ss.style.display = "none"; }
    rebuildFreshTagCloud(_freshAll);
    filterFreshByTag("");
    return;
  }
  _freshSearchTimer = setTimeout(() => runFreshSearch(trimmed), 400);
}

async function runFreshSearch(query) {
  document.querySelectorAll(".fresh-tag-pill").forEach(p => p.classList.remove("active"));
  _freshActiveTag = "";
  const gs = document.getElementById("fresh-genre-select");
  const ss = document.getElementById("fresh-style-select");
  if (gs) gs.value = "";
  if (ss) { ss.value = ""; ss.style.display = "none"; }
  const allBtn = document.getElementById("fresh-all-btn");
  const activeLabel = document.getElementById("fresh-active-label");
  if (allBtn) allBtn.classList.remove("active");
  if (activeLabel) activeLabel.textContent = `"${query}"`;
  try {
    const data = await fetch(`/api/fresh-releases/search?q=${encodeURIComponent(query)}`).then(r => r.json());
    _freshAll = data.releases ?? [];
    rebuildFreshTagCloud(_freshAll);
    renderFreshGrid(_freshAll);
    if (activeLabel) activeLabel.textContent = `"${query}" — ${_freshAll.length} result${_freshAll.length !== 1 ? "s" : ""}`;
  } catch {
    renderFreshGrid([]);
  }
}

function rebuildFreshTagCloud(releases) {
  const tagCounts = new Map();
  for (const r of releases) {
    for (const t of (r.tags ?? [])) tagCounts.set(t, (tagCounts.get(t) ?? 0) + 1);
  }
  const topTags = [...tagCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 50)
    .map(([tag]) => tag)
    .sort(() => Math.random() - 0.5);
  const tagCloud = document.getElementById("fresh-tag-cloud");
  if (tagCloud) {
    tagCloud.innerHTML = topTags.length
      ? topTags.map(t =>
          `<span class="pill fresh-tag-pill" data-tag="${escHtml(t)}" onclick="filterFreshByTag('${escHtml(t)}')" title="${escHtml(t)}">${escHtml(t.length > 30 ? t.slice(0, 28) + "…" : t)}</span>`
        ).join("")
      : "";
  }
}

// ── Drop card → popup ────────────────────────────────────────────────────
function searchFromDropCard(event, field, value) {
  event.preventDefault();
  document.getElementById("query").value     = "";
  document.getElementById("f-artist").value  = "";
  document.getElementById("f-release").value = "";
  document.getElementById("f-year").value    = "";
  document.getElementById("f-label").value   = "";
  document.getElementById("f-genre").value   = "";
  populateStyles();
  document.querySelector('input[name="result-type"][value=""]').checked = true;
  if (field === "artist") {
    document.getElementById("f-artist").value = value;
    toggleAdvanced(true);
  } else {
    document.getElementById("f-release").value = value;
    toggleAdvanced(true);
  }
  switchView("search");
  doSearch(1);
}

function dropTagSearch(tag) {
  switchView("drops");
  const input = document.getElementById("fresh-tag-input");
  if (input) { input.value = tag; debounceFreshSearch(tag); }
}

async function openDropCardPopup(el) {
  const artist  = el.dataset.artist  || "";
  const title   = el.dataset.title   || "";
  const cover   = el.dataset.cover   || "";
  const date    = el.dataset.date    || "";
  const types   = el.dataset.type    || "";
  const tags    = el.dataset.tags    ? el.dataset.tags.split(",").filter(Boolean) : [];
  const rgMbid  = el.dataset.rgmbid  || "";
  const artMbids = el.dataset.artmbids ? el.dataset.artmbids.split(",").filter(Boolean) : [];

  const overlay   = document.getElementById("modal-overlay");
  const infoEl    = document.getElementById("album-info");
  const loadingEl = document.getElementById("modal-loading");
  infoEl.innerHTML = "";
  loadingEl.textContent = "Searching Discogs…";
  loadingEl.style.display = "block";
  overlay.classList.add("open");

  try {
    const q    = encodeURIComponent(title);
    const art  = encodeURIComponent(artist);
    const data = await apiFetch(`${API}/search?q=${q}&artist=${art}&type=release&per_page=5`).then(r => r.json());
    const results = data.results ?? [];

    loadingEl.style.display = "none";

    if (!results.length) {
      const fmtDate = date
        ? new Date(date + "T12:00:00").toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" })
        : "";
      const year = date ? date.slice(0, 4) : "";
      const googleQ   = [artist, title, "new release"].filter(Boolean).join(" ");
      const googleUrl = `https://www.google.com/search?q=${encodeURIComponent(googleQ)}`;

      const escArt = escHtml(artist).replace(/'/g, "\\'");
      const escTit = escHtml(title).replace(/'/g, "\\'");

      let html = `<div class="drop-nomatch">`;
      if (cover) html += `<img class="drop-nomatch-cover" src="${escHtml(cover)}" alt="">`;
      html += `<div class="drop-nomatch-details">`;
      html += `<div class="drop-nomatch-artist"><a href="#" onclick="event.preventDefault();closeModal();searchFromDropCard(event,'artist','${escArt}')">${escHtml(artist)}</a></div>`;
      html += `<div class="drop-nomatch-title"><a href="#" onclick="event.preventDefault();closeModal();searchFromDropCard(event,'release','${escTit}')">${escHtml(title)}</a></div>`;
      if (types) html += `<div class="drop-nomatch-line">${escHtml(types)}</div>`;
      if (fmtDate) html += `<div class="drop-nomatch-line">Released: ${fmtDate}</div>`;
      html += `<div class="drop-nomatch-tags">`;
      if (tags.length) {
        const shown = tags.slice(0, 8);
        html += shown.map(t => `<a href="#" class="drop-nomatch-tag" onclick="event.preventDefault();closeModal();dropTagSearch('${escHtml(t).replace(/'/g, "\\'")}')" title="${escHtml(t)}">${escHtml(t.length > 30 ? t.slice(0, 28) + "…" : t)}</a>`).join("");
        if (tags.length > 8) html += `<span class="drop-nomatch-tag" style="color:#555">+${tags.length - 8}</span>`;
      } else {
        html += `<span style="font-size:0.65rem;color:#555">--</span>`;
      }
      html += `</div>`;
      html += `<div class="drop-nomatch-msg">No Discogs entry yet</div>`;
      html += `<a href="#" onclick="openConcertPopup(event,'${escArt}')" class="drop-nomatch-google" style="margin-top:0.3rem">Concerts ♪</a>`;
      html += `<a href="${googleUrl}" target="_blank" rel="noopener" class="drop-nomatch-google">Google →</a>`;
      html += `</div></div>`;
      infoEl.innerHTML = html;
      return;
    }

    const primary = results[0];
    const alts    = results.slice(1, 4);

    const pType = primary.type === "master" ? "master" : "release";
    itemCache.set(String(primary.id), primary);

    const [d, stats] = await Promise.all([
      apiFetch(`${API}/${pType}/${primary.id}`).then(r => r.json()),
      apiFetch(`${API}/marketplace-stats/${primary.id}?type=${pType}`).then(r => r.json()).catch(() => null),
    ]);

    loadingEl.style.display = "none";
    const pUrl = primary.uri ? `https://www.discogs.com${primary.uri}` : "";
    renderAlbumInfo(d, primary, pUrl, stats);

    if (alts.length) {
      const alsoDiv = document.createElement("div");
      alsoDiv.className = "fresh-also-section";
      alsoDiv.innerHTML = `<span style="color:#666;margin-right:0.3em">Also:</span>` +
        alts.map(a => {
          const aType = a.type === "master" ? "master" : "release";
          const aUrl  = a.uri ? `https://www.discogs.com${a.uri}` : "";
          return `<a href="#" class="fresh-also-link" onclick="openModal(event,${a.id},'${aType}','${aUrl.replace(/'/g, "\\'")}')">${escHtml(a.title ?? "")}</a>`;
        }).join('<span style="color:#444;margin:0 0.3em">·</span>');
      infoEl.appendChild(alsoDiv);
    }

  } catch (err) {
    loadingEl.style.display = "none";
    infoEl.innerHTML = `<div style="padding:2rem;text-align:center;color:#888">Failed to search Discogs.</div>`;
  }
}

function initFreshGenreDropdown() {
  const gs = document.getElementById("fresh-genre-select");
  if (!gs || gs.options.length > 1) return;
  for (const genre of Object.keys(GENRE_STYLES)) {
    const opt = document.createElement("option");
    opt.value = genre;
    opt.textContent = genre;
    gs.appendChild(opt);
  }
}

async function loadFreshReleases() {
  initFreshGenreDropdown();
  document.getElementById("fresh-releases-grid").innerHTML = renderSkeletonGrid(16);
  try {
    const data = await apiFetch("/api/fresh-releases").then(r => r.json());
    _freshAll = data.releases ?? [];
    _freshBrowse = _freshAll;
    if (!_freshAll.length) {
      document.getElementById("fresh-releases-grid").innerHTML = renderEmptyState("🆕", "No new releases", "Check back soon — releases are updated every 6 hours");
      return;
    }
    rebuildFreshTagCloud(_freshAll);
    renderFreshGrid(_freshAll);
  } catch {
    showToast("Failed to load releases — please try again", "error");
  }
}
