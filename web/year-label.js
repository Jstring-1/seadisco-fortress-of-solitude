// ── Home-strip "Year-Label" tab ──────────────────────────────────────────
// Browse the catalog cache (Masters+: masters + releases with no master)
// by year → label → album, or label → year → album. Filters: genre
// (includes / only) and "all tracks on YouTube" (applied to the album
// list; full-album videos don't count). Picking an album opens it.
// Data: /api/year-label/{years,labels,albums}.
// Loaded on demand by loadRandomRecords (search.js) when the tab is
// active; the panel lives inside #random-records in place of the grid.

(function () {
  const GENRES_FALLBACK = [
    "Blues", "Brass & Military", "Children's", "Classical", "Electronic",
    "Folk, World, & Country", "Funk / Soul", "Hip Hop", "Jazz", "Latin",
    "Non-Music", "Pop", "Reggae", "Rock", "Stage & Screen",
  ];
  const NO_LABEL = "Unknown label";       // no label found in the cache for these albums
  const LABEL_RENDER_CAP = 400;           // label rows drawn before "type to narrow"
  const pref = (k, d) => { try { const v = localStorage.getItem(k); return v == null ? d : v; } catch { return d; } };
  const setPref = (k, v) => { try { localStorage.setItem(k, v); } catch {} };

  const st = {
    order: pref("sd-yl-order", "yl") === "ly" ? "ly" : "yl",   // yl = year first, ly = label first
    genre: pref("sd-yl-genre", ""),       // "" = all genres
    strict: pref("sd-yl-strict", "0") === "1",
    ytOnly: pref("sd-yl-ytonly", "0") === "1",
    genres: GENRES_FALLBACK,
    years: null, yearsErr: "", yearsSeq: 0,
    labels: null, labelsErr: "", labelsSeq: 0,
    year: null,
    label: null,
    labelQuery: "",
    albums: null, albumsErr: "", albumsTrunc: false, albumsSeq: 0,
    pendingFocus: null,                    // column id to focus once it renders
  };
  const yearFirst = () => st.order === "yl";

  const qs = (extra) => {
    const p = new URLSearchParams();
    if (st.genre) { p.set("genre", st.genre); if (st.strict) p.set("strict", "1"); }
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

  // ── Loading ─────────────────────────────────────────────────────────
  // Years and labels each load either as the first column (everything) or
  // the second (scoped to the other column's pick). Each level keeps the
  // selection below it when it still exists, then cascades.
  async function loadYears(refresh) {
    const my = ++st.yearsSeq;
    const scoped = !yearFirst();
    if (scoped && st.label == null) { st.years = null; st.yearsErr = ""; renderYears(); loadAlbums(false); return; }
    st.years = null; st.yearsErr = "";
    renderYears(); if (!scoped) renderBar();
    try {
      const d = await getJson(`/api/year-label/years?${qs({ label: scoped ? st.label : null, refresh: refresh ? 1 : null })}`);
      if (my !== st.yearsSeq) return;
      if (Array.isArray(d.genres) && d.genres.length) st.genres = d.genres;
      st.years = Array.isArray(d.years) ? d.years : [];
      if (st.year != null && !st.years.some(y => y.year === st.year)) st.year = null;
    } catch (e) {
      if (my === st.yearsSeq) st.yearsErr = String(e?.message || e);
    }
    if (my !== st.yearsSeq) return;
    renderYears(); if (!scoped) renderBar();
    if (yearFirst()) loadLabels(refresh); else loadAlbums(refresh);
  }
  async function loadLabels(refresh) {
    const my = ++st.labelsSeq;
    const scoped = yearFirst();
    if (scoped && st.year == null) { st.labels = null; st.labelsErr = ""; renderLabels(); loadAlbums(false); return; }
    st.labels = null; st.labelsErr = "";
    renderLabels(); if (!scoped) renderBar();
    try {
      const d = await getJson(`/api/year-label/labels?${qs({ year: scoped ? st.year : null, refresh: refresh ? 1 : null })}`);
      if (my !== st.labelsSeq) return;
      if (Array.isArray(d.genres) && d.genres.length) st.genres = d.genres;
      st.labels = Array.isArray(d.labels) ? d.labels : [];
      if (st.label != null && !st.labels.some(l => l.label === st.label)) st.label = null;
    } catch (e) {
      if (my === st.labelsSeq) st.labelsErr = String(e?.message || e);
    }
    if (my !== st.labelsSeq) return;
    renderLabels(); if (!scoped) renderBar();
    if (yearFirst()) loadAlbums(refresh); else loadYears(refresh);
  }
  const loadFirst = (refresh) => (yearFirst() ? loadYears(refresh) : loadLabels(refresh));

  async function loadAlbums(refresh) {
    const my = ++st.albumsSeq;
    st.albums = null; st.albumsErr = "";
    if (st.year == null || st.label == null) { renderAlbums(); return; }
    renderAlbums();
    try {
      const d = await getJson(`/api/year-label/albums?${qs({ year: st.year, label: st.label, refresh: refresh ? 1 : null })}`);
      if (my !== st.albumsSeq) return;
      st.albums = Array.isArray(d.albums) ? d.albums : [];
      st.albumsTrunc = !!d.truncated;
    } catch (e) {
      if (my === st.albumsSeq) st.albumsErr = String(e?.message || e);
    }
    if (my !== st.albumsSeq) return;
    renderAlbums();
  }

  const byName = (a, b) => String(a).localeCompare(String(b), undefined, { sensitivity: "base", numeric: true });
  const fullyOnYt = (a) => a.tracks > 0 && a.ytTracks >= a.tracks;
  const nf = (n) => Number(n || 0).toLocaleString();
  const thumb = (src) => src
    ? `<img src="${escHtml(src)}" alt="" loading="lazy" width="36" height="36">`
    : `<span class="yl-nothumb"></span>`;
  const hint = (text) => `<div class="yl-hint">${text}</div>`;
  const labelName = (l) => l || NO_LABEL;

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
      <p class="yl-help" id="yl-help"></p>
      <div class="yl-cols" id="yl-cols">
        <div class="yl-col yl-years" aria-label="Years">
          <div class="yl-col-head" id="yl-years-head">Year</div>
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
      loadFirst(false);
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
    const first = yearFirst() ? st.years : st.labels;
    const firstErr = yearFirst() ? st.yearsErr : st.labelsErr;
    const total = (first || []).reduce((s, x) => s + x.n, 0);
    const status = first == null
      ? (firstErr ? `<span class="yl-err">Couldn't load: ${escHtml(firstErr)}</span>` : "Loading…")
      : `${nf(total)} album${total === 1 ? "" : "s"}`;
    el.innerHTML = `
      <span class="yl-opt">
        <span class="yl-opt-label">Order</span>
        ${toggle("order", !yearFirst(), "Year → Label", "Label → Year",
          "Pick a year, then a label on it", "Pick a label, then a year it released in")}
      </span>
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
        <span class="yl-opt-label">Tracks</span>
        ${toggle("yt", st.ytOnly, "Any", "All on YouTube",
          "Every album, whatever its YouTube coverage",
          "Only albums where every track has a YouTube video (full-album videos don't count)")}
      </span>
      <span class="yl-status">${status}</span>
      <span class="yl-refresh" role="button" tabindex="0" data-yl="refresh" title="Refresh the counts">↻</span>`;
    const help = $("yl-help");
    if (help) help.textContent = yearFirst()
      ? "Pick a year, then a label, then an album to open it. Counts are albums in SeaDisco's catalog cache."
      : "Pick a label, then a year, then an album to open it. Counts are albums in SeaDisco's catalog cache.";
    const cols = $("yl-cols");
    if (cols) cols.classList.toggle("yl-order-ly", !yearFirst());
  }

  function renderYears() {
    const el = $("yl-years");
    if (!el) return;
    const head = $("yl-years-head");
    if (head) head.textContent = !yearFirst() && st.label != null ? `Years for ${labelName(st.label)}` : "Year";
    if (!yearFirst() && st.label == null) { el.innerHTML = hint(st.labels && st.labels.length ? "Pick a label" : ""); return; }
    if (st.years == null) { el.innerHTML = st.yearsErr ? hint(`<span class="yl-err">${escHtml(st.yearsErr)}</span>`) : hint("Loading…"); return; }
    if (!st.years.length) { el.innerHTML = hint("Nothing matches these filters."); return; }
    let html = "", decade = null;
    for (const y of st.years) {
      const d = Math.floor(y.year / 10) * 10;
      if (d !== decade) { decade = d; html += `<div class="yl-decade">${d}s</div>`; }
      html += `<div class="yl-item${y.year === st.year ? " yl-sel" : ""}" role="button" tabindex="0" data-yl="year" data-v="${y.year}"><span>${y.year}</span><span class="yl-n">${nf(y.n)}</span></div>`;
    }
    el.innerHTML = html;
    applyFocus("yl-years");
  }

  function renderLabels() {
    const el = $("yl-labels");
    if (!el) return;
    const head = $("yl-labels-head");
    if (head) head.textContent = yearFirst() && st.year != null ? `Labels in ${st.year}` : "Label";
    const filter = $("yl-label-filter");
    if (filter) filter.style.display = st.labels && st.labels.length > 12 ? "" : "none";
    if (yearFirst() && st.year == null) { el.innerHTML = hint(st.years && st.years.length ? "Pick a year" : ""); return; }
    if (st.labels == null) { el.innerHTML = st.labelsErr ? hint(`<span class="yl-err">${escHtml(st.labelsErr)}</span>`) : hint("Loading…"); return; }
    const q = st.labelQuery.trim().toLowerCase();
    const list = st.labels
      .filter(l => !q || labelName(l.label).toLowerCase().includes(q))
      .sort((a, b) => (a.label === "" ? 1 : b.label === "" ? -1 : byName(a.label, b.label)));
    if (!list.length) { el.innerHTML = hint(q ? "No labels match" : "Nothing matches these filters."); return; }
    // Keep the selected label visible even past the render cap.
    let shown = list.slice(0, LABEL_RENDER_CAP);
    if (st.label != null && !shown.some(l => l.label === st.label)) {
      const sel = list.find(l => l.label === st.label);
      if (sel) shown = [sel, ...shown];
    }
    el.innerHTML = shown.map(l => `<div class="yl-item${l.label === st.label ? " yl-sel" : ""}" role="button" tabindex="0" data-yl="label" data-v="${escHtml(l.label)}"><span class="yl-name">${escHtml(labelName(l.label))}</span><span class="yl-n">${nf(l.n)}</span></div>`).join("")
      + (list.length > LABEL_RENDER_CAP ? hint(`Showing ${nf(LABEL_RENDER_CAP)} of ${nf(list.length)} labels. Type above to narrow.`) : "");
    applyFocus("yl-labels");
  }

  function renderAlbums() {
    const el = $("yl-albums");
    if (!el) return;
    const head = $("yl-albums-head");
    if (st.year == null || st.label == null) {
      if (head) head.textContent = "Albums";
      const next = yearFirst() ? (st.year == null ? "" : "Pick a label") : (st.label == null ? "" : "Pick a year");
      el.innerHTML = hint(next);
      return;
    }
    if (st.albums == null) {
      if (head) head.textContent = "Albums";
      el.innerHTML = st.albumsErr ? hint(`<span class="yl-err">${escHtml(st.albumsErr)}</span>`) : hint("Loading…");
      return;
    }
    const all = st.albums;
    const list = all.filter(a => !st.ytOnly || fullyOnYt(a))
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
      ].filter(Boolean).join(" · ");
      return `
        <div class="yl-album" role="button" tabindex="0" data-yl="open" data-id="${escHtml(String(a.id))}" data-type="${escHtml(a.type)}" title="Open this album">
          ${thumb(a.thumb)}
          <span class="yl-album-text">
            <span class="yl-album-title">${escHtml(a.artist ? `${a.artist} — ${a.title}` : a.title)}</span>
            <span class="yl-album-meta">${meta}</span>
          </span>
        </div>`;
    }).join("");
    applyFocus("yl-albums");
  }

  function renderAll() { renderBar(); renderYears(); renderLabels(); renderAlbums(); }

  // ── Interaction ─────────────────────────────────────────────────────
  function onClick(e) {
    const t = e.target.closest("[data-yl]");
    if (!t) return;
    const kind = t.dataset.yl;
    const on = t.dataset.v === "1";
    if (kind === "order") {
      const order = on ? "ly" : "yl";
      if (order === st.order) return;
      st.order = order;
      setPref("sd-yl-order", order);
      // Keep the current year + label picks; reload in the new order.
      st.years = null; st.labels = null; st.albums = null;
      renderAll();
      loadFirst(false);
    } else if (kind === "strict") {
      if (on === st.strict) return;
      st.strict = on;
      setPref("sd-yl-strict", on ? "1" : "0");
      loadFirst(false);
    } else if (kind === "yt") {
      if (on === st.ytOnly) return;
      st.ytOnly = on;
      setPref("sd-yl-ytonly", on ? "1" : "0");
      renderBar(); renderAlbums();
    } else if (kind === "refresh") {
      loadFirst(true);
    } else if (kind === "year") {
      const y = Number(t.dataset.v);
      if (y !== st.year) {
        st.year = y;
        if (yearFirst()) {
          // The label pick survives if that label also has albums in the
          // new year (loadLabels drops it otherwise).
          renderYears();
          loadLabels(false);
        } else {
          renderYears();
          loadAlbums(false);
        }
      }
      if (e.type === "keydown") focusFirst(yearFirst() ? "yl-labels" : "yl-albums");
    } else if (kind === "label") {
      if (t.dataset.v !== st.label) {
        st.label = t.dataset.v;
        renderLabels();
        if (yearFirst()) loadAlbums(false);
        else loadYears(false);   // keeps the year if this label has it
      }
      if (e.type === "keydown") focusFirst(yearFirst() ? "yl-albums" : "yl-years");
    } else if (kind === "open") {
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
    const first = yearFirst() ? st.years : st.labels;
    if (first == null && !st.yearsErr && !st.labelsErr && st.yearsSeq === 0 && st.labelsSeq === 0) {
      renderBar();
      loadFirst(false);
    } else renderAll();
  };
})();
