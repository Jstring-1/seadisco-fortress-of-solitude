// ── Admin home-strip "Blues" tab ─────────────────────────────────────────
// Year → label → album → releases picker over cached Blues albums from
// 1920–1960. Options: genre Loose/Strict, Masters+ (masters + releases
// with no master) vs every cached release, and "all tracks on YouTube"
// (full-album videos don't count). Picking an album lists every release
// of its master in the right-hand panel (via /master-versions).
// Data: GET /api/admin/blues-picker?strict=0|1&mp=0|1.
// Loaded on demand by loadRandomRecords (search.js) when the Blues tab is
// active; the panel lives inside #random-records in place of the grid.

(function () {
  const YEARS = [];
  for (let y = 1920; y <= 1960; y++) YEARS.push(y);
  const NO_LABEL = "(no label)";
  const pref = (k) => { try { return localStorage.getItem(k) === "1"; } catch { return false; } };
  const setPref = (k, on) => { try { localStorage.setItem(k, on ? "1" : "0"); } catch {} };

  const st = {
    strict: pref("sd-blues-strict"),
    mastersPlus: pref("sd-blues-mp"),
    ytOnly: pref("sd-blues-ytonly"),
    albums: null,        // current genre/Masters+ mode's list (unfiltered)
    computedAt: "",
    year: null,
    label: null,
    album: null,         // "type:id" of the selected album
    loading: false,
    error: "",
    seq: 0,
  };
  // masterId -> { loading, list, error } for the releases panel.
  const versions = new Map();

  function el() {
    let root = document.getElementById("blues-picker");
    if (root) return root;
    const wrap = document.getElementById("random-records");
    if (!wrap) return null;
    root = document.createElement("div");
    root.id = "blues-picker";
    root.className = "bp";
    const header = document.getElementById("random-records-header");
    if (header && header.parentNode === wrap) header.after(root);
    else wrap.appendChild(root);
    root.addEventListener("click", onClick);
    root.addEventListener("keydown", (e) => {
      if ((e.key === "Enter" || e.key === " ") && e.target.closest("[data-bp]")) {
        e.preventDefault();
        onClick(e);
      }
    });
    return root;
  }

  async function load(refresh) {
    const my = ++st.seq;
    st.loading = true; st.error = "";
    render();
    try {
      const r = await apiFetch(`/api/admin/blues-picker?strict=${st.strict ? 1 : 0}&mp=${st.mastersPlus ? 1 : 0}${refresh ? "&refresh=1" : ""}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      if (my !== st.seq) return; // superseded (mode flipped / refresh)
      st.albums = Array.isArray(d.albums) ? d.albums : [];
      st.computedAt = d.computedAt || "";
      fixSelection();
    } catch (e) {
      if (my === st.seq) st.error = String(e?.message || e);
    } finally {
      if (my === st.seq) { st.loading = false; render(); }
    }
  }

  async function loadVersions(masterId) {
    const cur = versions.get(masterId);
    if (cur && (cur.loading || cur.list)) return;
    versions.set(masterId, { loading: true, list: null, error: "" });
    render();
    try {
      const r = await apiFetch(`/master-versions/${encodeURIComponent(masterId)}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      versions.set(masterId, { loading: false, list: Array.isArray(d.versions) ? d.versions : [], error: "" });
    } catch (e) {
      versions.set(masterId, { loading: false, list: null, error: String(e?.message || e) });
    }
    render();
  }

  const labelOf = (a) => (a.label || "").trim() || NO_LABEL;
  const keyOf = (a) => `${a.type}:${a.id}`;
  const byName = (a, b) => String(a).localeCompare(String(b), undefined, { sensitivity: "base", numeric: true });
  const visible = () => (st.albums || []).filter(a => !st.ytOnly || (a.tracks > 0 && a.ytTracks >= a.tracks));
  const selectedAlbum = () => st.album == null ? null : visible().find(a => keyOf(a) === st.album) || null;

  // Keep the year/label/album selection if it still exists under the
  // current filters; otherwise fall back to the first year that has any.
  function fixSelection() {
    const albums = visible();
    if (st.year != null && !albums.some(a => a.year === st.year)) { st.year = null; st.label = null; }
    if (st.label != null && !albums.some(a => a.year === st.year && labelOf(a) === st.label)) st.label = null;
    if (st.album != null && !albums.some(a => keyOf(a) === st.album && a.year === st.year && labelOf(a) === st.label)) st.album = null;
    if (st.year == null) {
      const first = YEARS.find(y => albums.some(a => a.year === y));
      if (first != null) st.year = first;
    }
  }

  const toggle = (kind, on, offLabel, onLabel, offTitle, onTitle) => `
        <span class="rr-tab${on ? "" : " rr-tab-active"}" role="button" tabindex="0" data-bp="${kind}" data-v="0" title="${offTitle}">${offLabel}</span>
        <span class="rr-tab-sep">/</span>
        <span class="rr-tab${on ? " rr-tab-active" : ""}" role="button" tabindex="0" data-bp="${kind}" data-v="1" title="${onTitle}">${onLabel}</span>`;

  function releasesPanel(sel) {
    if (!sel) return `<div class="bp-hint">${st.label == null ? "" : "Pick an album to see all its releases"}</div>`;
    const open = (type, id, text) =>
      `<span class="bp-open" role="button" tabindex="0" data-bp="open" data-id="${escHtml(String(id))}" data-type="${type}" title="Open the album popup">${text}</span>`;
    if (sel.masterId == null) {
      return `<div class="bp-rel-head"><span>No master on Discogs, so this is the only release.</span>${open("release", sel.id, "Open ↗")}</div>`;
    }
    const v = versions.get(sel.masterId);
    if (!v || v.loading) return `<div class="bp-rel-head"><span>Loading releases…</span>${open("master", sel.masterId, "Open master ↗")}</div>`;
    if (v.error) return `<div class="bp-rel-head"><span class="bp-err">Couldn't load releases: ${escHtml(v.error)}</span>${open("master", sel.masterId, "Open master ↗")}</div>`;
    const list = v.list || [];
    return `<div class="bp-rel-head"><span>${list.length} release${list.length === 1 ? "" : "s"}</span>${open("master", sel.masterId, "Open master ↗")}</div>` +
      list.map(r => {
        const isSel = sel.type === "release" && String(r.id) === String(sel.id);
        const meta = [r.released || r.year || "", r.country || "", r.format || ""].filter(Boolean).map(x => escHtml(String(x))).join(" · ");
        const lbl = [r.label || "", r.catno && String(r.catno).toLowerCase() !== "none" ? r.catno : ""].filter(Boolean).map(x => escHtml(String(x))).join(" — ");
        return `
          <div class="bp-album${isSel ? " bp-sel" : ""}" role="button" tabindex="0" data-bp="open" data-id="${escHtml(String(r.id))}" data-type="release" title="Open this release">
            ${r.thumb ? `<img src="${escHtml(r.thumb)}" alt="" loading="lazy" width="36" height="36">` : `<span class="bp-nothumb"></span>`}
            <span class="bp-album-text">
              <span class="bp-album-title">${lbl || escHtml(r.title || "")}</span>
              <span class="bp-album-meta">${meta}</span>
            </span>
          </div>`;
      }).join("");
  }

  function render() {
    const root = el();
    if (!root) return;
    const albums = visible();
    const yearCounts = new Map();
    for (const a of albums) yearCounts.set(a.year, (yearCounts.get(a.year) || 0) + 1);

    const inYear = albums.filter(a => a.year === st.year);
    const labelCounts = new Map();
    for (const a of inYear) labelCounts.set(labelOf(a), (labelCounts.get(labelOf(a)) || 0) + 1);
    const labels = [...labelCounts.keys()].sort((a, b) =>
      a === NO_LABEL ? 1 : b === NO_LABEL ? -1 : byName(a, b));

    const list = st.label == null ? [] : inYear
      .filter(a => labelOf(a) === st.label)
      .sort((a, b) => byName(a.catno || "", b.catno || "") || byName(a.artist || "", b.artist || "") || byName(a.title || "", b.title || ""));
    const sel = selectedAlbum();

    const when = st.computedAt ? new Date(st.computedAt).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" }) : "";
    const status = st.loading
      ? "Loading…"
      : st.error
        ? `<span class="bp-err">Couldn't load: ${escHtml(st.error)}</span>`
        : `${albums.length.toLocaleString()} album${albums.length === 1 ? "" : "s"}${when ? ` · as of ${escHtml(when)}` : ""}`;

    root.innerHTML = `
      <div class="bp-bar">
        <span class="bp-opt">
          <span class="bp-bar-label">Blues genre:</span>
          ${toggle("mode", st.strict, "Loose", "Strict", "Blues is one of the album's genres", "Blues is the album's only genre")}
        </span>
        <span class="bp-opt">
          <span class="bp-bar-label">Albums:</span>
          ${toggle("mp", st.mastersPlus, "All releases", "Masters+", "Every cached release, each listed on its own", "Masters plus releases that have no master")}
        </span>
        <span class="bp-opt">
          <span class="bp-bar-label">Tracks:</span>
          ${toggle("yt", st.ytOnly, "Any", "All on YouTube", "Every album, whatever its YouTube coverage", "Only albums where every track has a YouTube video (full-album videos don't count)")}
        </span>
        <span class="bp-status">${status}</span>
        <span class="bp-refresh" role="button" tabindex="0" data-bp="refresh" title="Recompute from the cache">↻</span>
      </div>
      <div class="bp-cols">
        <div class="bp-col bp-years" aria-label="Years">
          ${YEARS.map(y => {
            const n = yearCounts.get(y) || 0;
            const cls = "bp-item" + (y === st.year ? " bp-sel" : "") + (n ? "" : " bp-empty");
            return n
              ? `<div class="${cls}" role="button" tabindex="0" data-bp="year" data-v="${y}"><span>${y}</span><span class="bp-n">${n}</span></div>`
              : `<div class="${cls}"><span>${y}</span></div>`;
          }).join("")}
        </div>
        <div class="bp-col bp-labels" aria-label="Labels">
          ${st.year == null
            ? `<div class="bp-hint">${st.loading ? "" : "Pick a year"}</div>`
            : labels.map(l => `<div class="bp-item${l === st.label ? " bp-sel" : ""}" role="button" tabindex="0" data-bp="label" data-v="${escHtml(l)}"><span class="bp-name">${escHtml(l)}</span><span class="bp-n">${labelCounts.get(l)}</span></div>`).join("")
              || `<div class="bp-hint">Nothing for ${st.year}</div>`}
        </div>
        <div class="bp-col bp-albums" aria-label="Albums">
          ${st.label == null
            ? `<div class="bp-hint">${st.year == null ? "" : "Pick a label"}</div>`
            : list.map(a => `
              <div class="bp-album${keyOf(a) === st.album ? " bp-sel" : ""}" role="button" tabindex="0" data-bp="album" data-key="${escHtml(keyOf(a))}" title="Show all releases">
                ${a.thumb ? `<img src="${escHtml(a.thumb)}" alt="" loading="lazy" width="36" height="36">` : `<span class="bp-nothumb"></span>`}
                <span class="bp-album-text">
                  <span class="bp-album-title">${escHtml(a.artist ? `${a.artist} — ${a.title}` : a.title)}</span>
                  <span class="bp-album-meta">${[a.catno && a.catno.toLowerCase() !== "none" ? escHtml(a.catno) : "", `${a.ytTracks}/${a.tracks} on YouTube`, a.type].filter(Boolean).join(" · ")}</span>
                </span>
                <span class="bp-open" role="button" tabindex="0" data-bp="open" data-id="${escHtml(String(a.id))}" data-type="${escHtml(a.type)}" title="Open the album popup">↗</span>
              </div>`).join("")}
        </div>
        <div class="bp-col bp-releases" aria-label="Releases">
          ${releasesPanel(sel)}
        </div>
      </div>`;
  }

  function onClick(e) {
    const t = e.target.closest("[data-bp]");
    if (!t) return;
    const kind = t.dataset.bp;
    const on = t.dataset.v === "1";
    if (kind === "mode" || kind === "mp") {
      const prop = kind === "mode" ? "strict" : "mastersPlus";
      if (on === st[prop]) return;
      st[prop] = on;
      setPref(kind === "mode" ? "sd-blues-strict" : "sd-blues-mp", on);
      st.albums = null;
      load(false);
    } else if (kind === "yt") {
      if (on === st.ytOnly) return;
      st.ytOnly = on;
      setPref("sd-blues-ytonly", on);
      fixSelection();
      render();
    } else if (kind === "refresh") {
      if (!st.loading) load(true);
    } else if (kind === "year") {
      const y = Number(t.dataset.v);
      if (y !== st.year) { st.year = y; st.label = null; st.album = null; }
      render();
      if (e.type === "keydown") focusFirst(".bp-labels");
    } else if (kind === "label") {
      if (t.dataset.v !== st.label) { st.label = t.dataset.v; st.album = null; }
      render();
      if (e.type === "keydown") focusFirst(".bp-albums");
    } else if (kind === "album") {
      st.album = t.dataset.key;
      const sel = selectedAlbum();
      if (sel && sel.masterId != null) loadVersions(sel.masterId);
      render();
      if (e.type === "keydown") focusFirst(".bp-releases");
    } else if (kind === "open") {
      const id = t.dataset.id, type = t.dataset.type;
      if (typeof openModal === "function") {
        openModal(e, id, type, `https://www.discogs.com/${type}/${id}`);
      }
    }
  }

  // After a keyboard drill-in, move focus into the next column (the
  // re-render replaced the focused element).
  function focusFirst(sel) {
    const next = document.querySelector(`#blues-picker ${sel} [data-bp]`);
    if (next) next.focus();
  }

  window._sdBluesPickerOpen = function () {
    const root = el();
    if (!root) return;
    root.style.display = "";
    if (st.albums == null && !st.loading) load(false);
    else render();
  };
})();
