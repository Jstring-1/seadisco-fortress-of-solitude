// ── Admin home-strip "Blues" tab ─────────────────────────────────────────
// Year → label → album picker over cached Masters+ Blues albums from
// 1920–1960 where every track has a playable YouTube video (full-album
// videos don't count). Data: GET /api/admin/blues-picker?strict=0|1.
// Loaded on demand by loadRandomRecords (search.js) when the Blues tab is
// active; the panel lives inside #random-records in place of the grid.

(function () {
  const YEARS = [];
  for (let y = 1920; y <= 1960; y++) YEARS.push(y);
  const NO_LABEL = "(no label)";

  const st = {
    strict: (() => { try { return localStorage.getItem("sd-blues-strict") === "1"; } catch { return false; } })(),
    albums: null,        // current mode's album list
    computedAt: "",
    year: null,
    label: null,
    loading: false,
    error: "",
    seq: 0,
  };

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
      const r = await apiFetch(`/api/admin/blues-picker?strict=${st.strict ? 1 : 0}${refresh ? "&refresh=1" : ""}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      if (my !== st.seq) return; // superseded (mode flipped / refresh)
      st.albums = Array.isArray(d.albums) ? d.albums : [];
      st.computedAt = d.computedAt || "";
      // Keep the selection if it still exists in the new data.
      if (st.year != null && !st.albums.some(a => a.year === st.year)) { st.year = null; st.label = null; }
      if (st.label != null && !st.albums.some(a => a.year === st.year && labelOf(a) === st.label)) st.label = null;
      if (st.year == null) {
        const first = YEARS.find(y => st.albums.some(a => a.year === y));
        if (first != null) st.year = first;
      }
    } catch (e) {
      if (my === st.seq) st.error = String(e?.message || e);
    } finally {
      if (my === st.seq) { st.loading = false; render(); }
    }
  }

  const labelOf = (a) => (a.label || "").trim() || NO_LABEL;
  const byName = (a, b) => a.localeCompare(b, undefined, { sensitivity: "base", numeric: true });

  function render() {
    const root = el();
    if (!root) return;
    const albums = st.albums || [];
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

    const when = st.computedAt ? new Date(st.computedAt).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" }) : "";
    const status = st.loading
      ? "Loading…"
      : st.error
        ? `<span class="bp-err">Couldn't load: ${escHtml(st.error)}</span>`
        : `${albums.length.toLocaleString()} album${albums.length === 1 ? "" : "s"}${when ? ` · as of ${escHtml(when)}` : ""}`;

    root.innerHTML = `
      <div class="bp-bar">
        <span class="bp-bar-label">Blues genre:</span>
        <span class="rr-tab${st.strict ? "" : " rr-tab-active"}" role="button" tabindex="0" data-bp="mode" data-v="0"
              title="Blues is one of the album's genres">Loose</span>
        <span class="rr-tab-sep">/</span>
        <span class="rr-tab${st.strict ? " rr-tab-active" : ""}" role="button" tabindex="0" data-bp="mode" data-v="1"
              title="Blues is the album's only genre">Strict</span>
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
              <div class="bp-album" role="button" tabindex="0" data-bp="album" data-id="${escHtml(String(a.id))}" data-type="${escHtml(a.type)}">
                ${a.thumb ? `<img src="${escHtml(a.thumb)}" alt="" loading="lazy" width="36" height="36">` : `<span class="bp-nothumb"></span>`}
                <span class="bp-album-text">
                  <span class="bp-album-title">${escHtml(a.artist ? `${a.artist} — ${a.title}` : a.title)}</span>
                  <span class="bp-album-meta">${[a.catno && a.catno.toLowerCase() !== "none" ? escHtml(a.catno) : "", `${a.tracks} track${a.tracks === 1 ? "" : "s"}`, a.type === "master" ? "master" : "release"].filter(Boolean).join(" · ")}</span>
                </span>
              </div>`).join("")}
        </div>
      </div>`;
  }

  function onClick(e) {
    const t = e.target.closest("[data-bp]");
    if (!t) return;
    const kind = t.dataset.bp;
    if (kind === "mode") {
      const strict = t.dataset.v === "1";
      if (strict === st.strict) return;
      st.strict = strict;
      try { localStorage.setItem("sd-blues-strict", strict ? "1" : "0"); } catch {}
      st.albums = null;
      load(false);
    } else if (kind === "refresh") {
      if (!st.loading) load(true);
    } else if (kind === "year") {
      const y = Number(t.dataset.v);
      if (y !== st.year) { st.year = y; st.label = null; }
      render();
      if (e.type === "keydown") focusFirst(".bp-labels");
    } else if (kind === "label") {
      st.label = t.dataset.v;
      render();
      if (e.type === "keydown") focusFirst(".bp-albums");
    } else if (kind === "album") {
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
