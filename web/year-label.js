// ── Home-strip "Year-Label" tab ──────────────────────────────────────────
// Browse the catalog cache by year → label → album → every release of
// that album. Filters: genre (includes / only), one entry per album
// (Masters+) vs every pressing, and "all tracks on YouTube" (applied to
// the album list; full-album videos don't count).
// Data: /api/year-label/{years,labels,albums} and /master-versions/:id.
// Loaded on demand by loadRandomRecords (search.js) when the tab is
// active; the panel lives inside #random-records in place of the grid.

(function () {
  const GENRES_FALLBACK = [
    "Blues", "Brass & Military", "Children's", "Classical", "Electronic",
    "Folk, World, & Country", "Funk / Soul", "Hip Hop", "Jazz", "Latin",
    "Non-Music", "Pop", "Reggae", "Rock", "Stage & Screen",
  ];
  const NO_LABEL = "(no label)";
  const pref = (k, d) => { try { const v = localStorage.getItem(k); return v == null ? d : v; } catch { return d; } };
  const setPref = (k, v) => { try { localStorage.setItem(k, v); } catch {} };

  const st = {
    genre: pref("sd-yl-genre", ""),       // "" = all genres
    strict: pref("sd-yl-strict", "0") === "1",
    mp: pref("sd-yl-mp", "0") === "1",
    ytOnly: pref("sd-yl-ytonly", "0") === "1",
    genres: GENRES_FALLBACK,
    years: null, yearsErr: "", yearsSeq: 0,
    year: null,
    labels: null, labelsErr: "", labelsSeq: 0,
    labelQuery: "",
    label: null,
    albums: null, albumsErr: "", albumsTrunc: false, albumsSeq: 0,
    album: null,                           // "type:id"
    pendingFocus: null,                    // column id to focus once it renders
  };
  const versions = new Map();              // masterId -> { loading, list, error }

  const qs = (extra) => {
    const p = new URLSearchParams();
    if (st.genre) { p.set("genre", st.genre); if (st.strict) p.set("strict", "1"); }
    if (st.mp) p.set("mp", "1");
    for (const [k, v] of Object.entries(extra || {})) if (v != null) p.set(k, String(v));
    return p.toString();
  };
  async function getJson(url) {
    const r = await apiFetch(url);
    if (!r.ok) {
      let msg = `HTTP ${r.status}`;
      try { const j = await r.json(); if (j?.error) msg = j.error; } catch {}
      throw new Error(msg);
    }
    return r.json();
  }

  // ── Loading (each level keeps the selection below it if it survives) ──
  async function loadYears(refresh) {
    const my = ++st.yearsSeq;
    st.years = null; st.yearsErr = "";
    renderYears(); renderBar();
    try {
      const d = await getJson(`/api/year-label/years?${qs(refresh ? { refresh: 1 } : null)}`);
      if (my !== st.yearsSeq) return;
      if (Array.isArray(d.genres) && d.genres.length) st.genres = d.genres;
      st.years = Array.isArray(d.years) ? d.years : [];
      if (st.year != null && !st.years.some(y => y.year === st.year)) { st.year = null; st.label = null; st.album = null; }
    } catch (e) {
      if (my === st.yearsSeq) st.yearsErr = String(e?.message || e);
    }
    if (my !== st.yearsSeq) return;
    renderBar(); renderYears();
    if (st.year != null) loadLabels(refresh); else { st.labels = null; renderLabels(); renderAlbums(); renderReleases(); }
  }
  async function loadLabels(refresh) {
    const my = ++st.labelsSeq;
    st.labels = null; st.labelsErr = "";
    renderLabels(); renderAlbums(); renderReleases();
    try {
      const d = await getJson(`/api/year-label/labels?${qs({ year: st.year, refresh: refresh ? 1 : null })}`);
      if (my !== st.labelsSeq) return;
      st.labels = Array.isArray(d.labels) ? d.labels : [];
      if (st.label != null && !st.labels.some(l => l.label === st.label)) { st.label = null; st.album = null; }
    } catch (e) {
      if (my === st.labelsSeq) st.labelsErr = String(e?.message || e);
    }
    if (my !== st.labelsSeq) return;
    renderLabels();
    if (st.label != null) loadAlbums(refresh); else { st.albums = null; renderAlbums(); renderReleases(); }
  }
  async function loadAlbums(refresh) {
    const my = ++st.albumsSeq;
    st.albums = null; st.albumsErr = "";
    renderAlbums(); renderReleases();
    try {
      const d = await getJson(`/api/year-label/albums?${qs({ year: st.year, label: st.label, refresh: refresh ? 1 : null })}`);
      if (my !== st.albumsSeq) return;
      st.albums = Array.isArray(d.albums) ? d.albums : [];
      st.albumsTrunc = !!d.truncated;
      if (st.album != null && !visibleAlbums().some(a => keyOf(a) === st.album)) st.album = null;
    } catch (e) {
      if (my === st.albumsSeq) st.albumsErr = String(e?.message || e);
    }
    if (my !== st.albumsSeq) return;
    renderAlbums(); renderReleases();
  }
  async function loadVersions(masterId) {
    const cur = versions.get(masterId);
    if (cur && (cur.loading || cur.list)) return;
    versions.set(masterId, { loading: true, list: null, error: "" });
    renderReleases();
    try {
      const d = await getJson(`/master-versions/${encodeURIComponent(masterId)}`);
      versions.set(masterId, { loading: false, list: Array.isArray(d.versions) ? d.versions : [], error: "" });
    } catch (e) {
      versions.set(masterId, { loading: false, list: null, error: String(e?.message || e) });
    }
    renderReleases();
  }

  const keyOf = (a) => `${a.type}:${a.id}`;
  const byName = (a, b) => String(a).localeCompare(String(b), undefined, { sensitivity: "base", numeric: true });
  const fullyOnYt = (a) => a.tracks > 0 && a.ytTracks >= a.tracks;
  const visibleAlbums = () => (st.albums || []).filter(a => !st.ytOnly || fullyOnYt(a));
  const selectedAlbum = () => st.album == null ? null : visibleAlbums().find(a => keyOf(a) === st.album) || null;
  const nf = (n) => Number(n || 0).toLocaleString();
  const thumb = (src) => src
    ? `<img src="${escHtml(src)}" alt="" loading="lazy" width="36" height="36">`
    : `<span class="yl-nothumb"></span>`;
  const hint = (text) => `<div class="yl-hint">${text}</div>`;

  // ── Rendering ───────────────────────────────────────────────────────
  function root() {
    let r = document.getElementById("year-label");
    if (r) return r;
    const wrap = document.getElementById("random-records");
    if (!wrap) return null;
    r = document.createElement("div");
    r.id = "year-label";
    r.className = "yl";
    r.innerHTML = `
      <div class="yl-bar" id="yl-bar"></div>
      <p class="yl-help">Pick a year, then a label, then an album to see every release of it. Counts are albums in SeaDisco's catalog cache.</p>
      <div class="yl-cols">
        <div class="yl-col yl-years" aria-label="Years">
          <div class="yl-col-head">Year</div>
          <div id="yl-years"></div>
        </div>
        <div class="yl-col yl-labels" aria-label="Labels">
          <div class="yl-col-head" id="yl-labels-head">Label</div>
          <input type="search" class="sd-filter-input yl-label-filter" id="yl-label-filter" placeholder="Filter labels…" aria-label="Filter labels">
          <div id="yl-labels"></div>
        </div>
        <div class="yl-col yl-albums" aria-label="Albums">
          <div class="yl-col-head" id="yl-albums-head">Albums</div>
          <div id="yl-albums"></div>
        </div>
        <div class="yl-col yl-releases" aria-label="Releases">
          <div class="yl-col-head" id="yl-releases-head">Releases</div>
          <div id="yl-releases"></div>
        </div>
      </div>`;
    const header = document.getElementById("random-records-header");
    if (header && header.parentNode === wrap) header.after(r);
    else wrap.appendChild(r);
    r.addEventListener("click", onClick);
    r.addEventListener("keydown", (e) => {
      if ((e.key === "Enter" || e.key === " ") && e.target.closest("[data-yl]")) {
        e.preventDefault();
        onClick(e);
      }
    });
    r.addEventListener("change", (e) => {
      if (e.target.id !== "yl-genre") return;
      st.genre = e.target.value;
      setPref("sd-yl-genre", st.genre);
      loadYears(false);
    });
    r.querySelector("#yl-label-filter").addEventListener("input", (e) => {
      st.labelQuery = e.target.value;
      renderLabels();
    });
    return r;
  }
  const $ = (id) => document.getElementById(id);

  const toggle = (kind, on, offLabel, onLabel, offTitle, onTitle) => `
      <span class="rr-tab${on ? "" : " rr-tab-active"}" role="button" tabindex="0" data-yl="${kind}" data-v="0" title="${escHtml(offTitle)}">${offLabel}</span>
      <span class="rr-tab-sep">/</span>
      <span class="rr-tab${on ? " rr-tab-active" : ""}" role="button" tabindex="0" data-yl="${kind}" data-v="1" title="${escHtml(onTitle)}">${onLabel}</span>`;

  function renderBar() {
    const el = $("yl-bar");
    if (!el) return;
    const total = (st.years || []).reduce((s, y) => s + y.n, 0);
    const status = st.years == null
      ? (st.yearsErr ? `<span class="yl-err">Couldn't load: ${escHtml(st.yearsErr)}</span>` : "Loading…")
      : `${nf(total)} album${total === 1 ? "" : "s"} across ${nf(st.years.length)} year${st.years.length === 1 ? "" : "s"}`;
    el.innerHTML = `
      <label class="yl-opt">
        <span class="yl-opt-label">Genre</span>
        <select id="yl-genre" class="sd-filter-select">
          <option value="">All genres</option>
          ${st.genres.map(g => `<option value="${escHtml(g)}"${g === st.genre ? " selected" : ""}>${escHtml(g)}</option>`).join("")}
        </select>
      </label>
      ${st.genre ? `<span class="yl-opt">${toggle("strict", st.strict, "Includes", "Only",
        `Albums tagged ${st.genre}, alongside any other genre`, `Albums whose only genre is ${st.genre}`)}</span>` : ""}
      <span class="yl-opt">
        <span class="yl-opt-label">Show</span>
        ${toggle("mp", st.mp, "Every pressing", "One per album",
          "Every cached release, each pressing listed on its own",
          "One entry per album (its master), plus releases that have no master")}
      </span>
      <span class="yl-opt">
        <span class="yl-opt-label">Tracks</span>
        ${toggle("yt", st.ytOnly, "Any", "All on YouTube",
          "Every album, whatever its YouTube coverage",
          "Only albums where every track has a YouTube video (full-album videos don't count)")}
      </span>
      <span class="yl-status">${status}</span>
      <span class="yl-refresh" role="button" tabindex="0" data-yl="refresh" title="Refresh the counts">↻</span>`;
  }

  function renderYears() {
    const el = $("yl-years");
    if (!el) return;
    if (st.years == null) { el.innerHTML = st.yearsErr ? hint(`<span class="yl-err">${escHtml(st.yearsErr)}</span>`) : hint("Loading…"); return; }
    if (!st.years.length) { el.innerHTML = hint("Nothing in the cache matches these filters."); return; }
    let html = "", decade = null;
    for (const y of st.years) {
      const d = Math.floor(y.year / 10) * 10;
      if (d !== decade) { decade = d; html += `<div class="yl-decade">${d}s</div>`; }
      html += `<div class="yl-item${y.year === st.year ? " yl-sel" : ""}" role="button" tabindex="0" data-yl="year" data-v="${y.year}"><span>${y.year}</span><span class="yl-n">${nf(y.n)}</span></div>`;
    }
    el.innerHTML = html;
  }

  function renderLabels() {
    const el = $("yl-labels");
    if (!el) return;
    const head = $("yl-labels-head");
    if (head) head.textContent = st.year == null ? "Label" : `Labels in ${st.year}`;
    const filter = $("yl-label-filter");
    if (filter) filter.style.display = st.labels && st.labels.length > 12 ? "" : "none";
    if (st.year == null) { el.innerHTML = hint(st.years && st.years.length ? "Pick a year" : ""); return; }
    if (st.labels == null) { el.innerHTML = st.labelsErr ? hint(`<span class="yl-err">${escHtml(st.labelsErr)}</span>`) : hint("Loading…"); return; }
    const q = st.labelQuery.trim().toLowerCase();
    const list = st.labels
      .filter(l => !q || (l.label || NO_LABEL).toLowerCase().includes(q))
      .sort((a, b) => (a.label === "" ? 1 : b.label === "" ? -1 : byName(a.label, b.label)));
    el.innerHTML = list.length
      ? list.map(l => `<div class="yl-item${l.label === st.label ? " yl-sel" : ""}" role="button" tabindex="0" data-yl="label" data-v="${escHtml(l.label)}"><span class="yl-name">${escHtml(l.label || NO_LABEL)}</span><span class="yl-n">${nf(l.n)}</span></div>`).join("")
      : hint(q ? "No labels match" : `Nothing for ${st.year}`);
    applyFocus("yl-labels");
  }

  function renderAlbums() {
    const el = $("yl-albums");
    if (!el) return;
    const head = $("yl-albums-head");
    if (st.label == null) {
      if (head) head.textContent = "Albums";
      el.innerHTML = hint(st.year == null ? "" : "Pick a label");
      return;
    }
    if (st.albums == null) {
      if (head) head.textContent = "Albums";
      el.innerHTML = st.albumsErr ? hint(`<span class="yl-err">${escHtml(st.albumsErr)}</span>`) : hint("Loading…");
      return;
    }
    const all = st.albums;
    const list = visibleAlbums()
      .sort((a, b) => byName(a.catno || "", b.catno || "") || byName(a.artist || "", b.artist || "") || byName(a.title || "", b.title || ""));
    if (head) {
      head.textContent = st.ytOnly
        ? `${nf(list.length)} of ${nf(all.length)} fully on YouTube`
        : `${nf(all.length)} album${all.length === 1 ? "" : "s"}${st.albumsTrunc ? " (first shown)" : ""}`;
    }
    if (!list.length) {
      el.innerHTML = hint(st.ytOnly ? "None of these have every track on YouTube. Switch Tracks to Any to see them all." : "Nothing here");
      return;
    }
    el.innerHTML = list.map(a => {
      const yt = a.tracks ? `${a.ytTracks}/${a.tracks} tracks on YouTube` : "no tracklist";
      const meta = [
        a.catno && a.catno.toLowerCase() !== "none" ? escHtml(a.catno) : "",
        a.format ? escHtml(a.format) : "",
        `<span class="${fullyOnYt(a) ? "yl-yt-full" : ""}">${yt}</span>`,
        st.mp && a.type === "master" ? "album" : "",
      ].filter(Boolean).join(" · ");
      return `
        <div class="yl-album${keyOf(a) === st.album ? " yl-sel" : ""}" role="button" tabindex="0" data-yl="album" data-key="${escHtml(keyOf(a))}" title="See every release of this album">
          ${thumb(a.thumb)}
          <span class="yl-album-text">
            <span class="yl-album-title">${escHtml(a.artist ? `${a.artist} — ${a.title}` : a.title)}</span>
            <span class="yl-album-meta">${meta}</span>
          </span>
          <span class="yl-open" role="button" tabindex="0" data-yl="open" data-id="${escHtml(String(a.id))}" data-type="${escHtml(a.type)}" title="Open">↗</span>
        </div>`;
    }).join("");
    applyFocus("yl-albums");
  }

  function renderReleases() {
    const el = $("yl-releases");
    if (!el) return;
    const head = $("yl-releases-head");
    const sel = selectedAlbum();
    const open = (type, id, text) =>
      `<span class="yl-open" role="button" tabindex="0" data-yl="open" data-id="${escHtml(String(id))}" data-type="${type}" title="Open">${text}</span>`;
    if (!sel) {
      if (head) head.textContent = "Releases";
      el.innerHTML = hint(st.albums && st.albums.length ? "Pick an album to see every release of it" : "");
      return;
    }
    if (sel.masterId == null) {
      if (head) head.innerHTML = `Releases ${open("release", sel.id, "Open ↗")}`;
      el.innerHTML = hint("This release isn't grouped under an album on Discogs, so it's the only one.");
      return;
    }
    const v = versions.get(sel.masterId);
    if (head) head.innerHTML = `${v && v.list ? `${nf(v.list.length)} release${v.list.length === 1 ? "" : "s"}` : "Releases"} ${open("master", sel.masterId, "Open album ↗")}`;
    if (!v || v.loading) { el.innerHTML = hint("Loading…"); return; }
    if (v.error) { el.innerHTML = hint(`<span class="yl-err">Couldn't load releases: ${escHtml(v.error)}</span>`); return; }
    if (!v.list.length) { el.innerHTML = hint("No release list available for this album."); return; }
    el.innerHTML = v.list.map(r => {
      const isSel = sel.type === "release" && String(r.id) === String(sel.id);
      const meta = [r.released || r.year || "", r.country || "", r.format || ""].filter(Boolean).map(x => escHtml(String(x))).join(" · ");
      const lbl = [r.label || "", r.catno && String(r.catno).toLowerCase() !== "none" ? r.catno : ""].filter(Boolean).map(x => escHtml(String(x))).join(" — ");
      return `
        <div class="yl-album${isSel ? " yl-sel" : ""}" role="button" tabindex="0" data-yl="open" data-id="${escHtml(String(r.id))}" data-type="release" title="Open this release">
          ${thumb(r.thumb)}
          <span class="yl-album-text">
            <span class="yl-album-title">${lbl || escHtml(r.title || "")}</span>
            <span class="yl-album-meta">${meta}</span>
          </span>
        </div>`;
    }).join("");
    applyFocus("yl-releases");
  }

  function renderAll() { renderBar(); renderYears(); renderLabels(); renderAlbums(); renderReleases(); }

  // ── Interaction ─────────────────────────────────────────────────────
  function onClick(e) {
    const t = e.target.closest("[data-yl]");
    if (!t) return;
    const kind = t.dataset.yl;
    const on = t.dataset.v === "1";
    if (kind === "strict" || kind === "mp") {
      if (on === st[kind]) return;
      st[kind] = on;
      setPref(kind === "strict" ? "sd-yl-strict" : "sd-yl-mp", on ? "1" : "0");
      loadYears(false);
    } else if (kind === "yt") {
      if (on === st.ytOnly) return;
      st.ytOnly = on;
      setPref("sd-yl-ytonly", on ? "1" : "0");
      if (st.album != null && !selectedAlbum()) st.album = null;
      renderBar(); renderAlbums(); renderReleases();
    } else if (kind === "refresh") {
      loadYears(true);
    } else if (kind === "year") {
      const y = Number(t.dataset.v);
      if (y !== st.year) {
        st.year = y; st.label = null; st.album = null; st.labelQuery = "";
        const f = $("yl-label-filter"); if (f) f.value = "";
        renderYears();
        loadLabels(false);
      }
      if (e.type === "keydown") focusFirst("yl-labels");
    } else if (kind === "label") {
      if (t.dataset.v !== st.label) {
        st.label = t.dataset.v; st.album = null;
        renderLabels();
        loadAlbums(false);
      }
      if (e.type === "keydown") focusFirst("yl-albums");
    } else if (kind === "album") {
      st.album = t.dataset.key;
      const sel = selectedAlbum();
      if (sel && sel.masterId != null) loadVersions(sel.masterId);
      renderAlbums(); renderReleases();
      if (e.type === "keydown") focusFirst("yl-releases");
    } else if (kind === "open") {
      e.stopPropagation();
      const id = t.dataset.id, type = t.dataset.type;
      if (typeof openModal === "function") openModal(e, id, type, `https://www.discogs.com/${type}/${id}`);
    }
  }

  // After a keyboard drill-in, move focus to the next column's first item
  // once that column has rendered its data (it may still be loading).
  function focusFirst(id) {
    st.pendingFocus = id;
    applyFocus(id);
  }
  function applyFocus(id) {
    if (st.pendingFocus !== id) return;
    const next = document.querySelector(`#${id} [data-yl]`);
    if (next) { st.pendingFocus = null; next.focus(); }
  }

  window._sdYearLabelMount = function () {
    const r = root();
    if (!r) return;
    r.style.display = "";
    if (st.years == null && !st.yearsErr && st.yearsSeq === 0) loadYears(false);
    else renderAll();
  };
})();
