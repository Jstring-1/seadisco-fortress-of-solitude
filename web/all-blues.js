// ── All Blues view ────────────────────────────────────────────────
//
// Admin-only network visualization of artist relationships parsed
// from cached Discogs profiles. Loaded lazily by switchView('all-
// blues'). Reuses the Cytoscape lazy-loader pattern from blues-
// archive.js's Connections subtab.

const _AB_KINDS = [
  { key: "spouse",  color: "#ec4899", label: "Spouse" },
  { key: "family",  color: "#f97316", label: "Family" },
  { key: "mentor",  color: "#a855f7", label: "Mentor" },
  { key: "band",    color: "#4ade80", label: "Band" },
  { key: "alias",   color: "#22d3ee", label: "Alias" },
  { key: "mention", color: "#f5d442", label: "Mention" },
];
const _AB_KIND_KEY  = "sd_all_blues_kinds";
const _AB_FOCUS_KEY = "sd_all_blues_focus";
let _abCy = null;
let _abFirstLoad = true;
let _abLastGraph = { nodes: [], edges: [] }; // most recent graph payload, used for focus search
let _abFocusId = null;                       // current focused artist id (null = whole graph)
let _abFocusName = "";                       // display name for the focused artist

function _abEsc(s) {
  // Escapes for both text content AND attribute values. Names with
  // double or single quotes (e.g. JSON.stringify output embedded in
  // an onclick="…") prematurely closed the attribute before quotes
  // were in the escape set — the link styling stayed but onclick
  // never fired. Now safe for any string in either context.
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function _abGetEnabledKinds() {
  try {
    const raw = localStorage.getItem(_AB_KIND_KEY);
    if (!raw) return new Set(_AB_KINDS.map(k => k.key));
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) return new Set(_AB_KINDS.map(k => k.key));
    return new Set(arr);
  } catch { return new Set(_AB_KINDS.map(k => k.key)); }
}

function _abSetEnabledKinds(set) {
  try { localStorage.setItem(_AB_KIND_KEY, JSON.stringify([...set])); } catch {}
}

function _abToggleKind(kind) {
  const set = _abGetEnabledKinds();
  if (set.has(kind)) set.delete(kind); else set.add(kind);
  _abSetEnabledKinds(set);
  _abRenderKindChips();
  allBluesReload();
}

function _abRenderKindChips() {
  const el = document.getElementById("all-blues-kind-chips");
  if (!el) return;
  const enabled = _abGetEnabledKinds();
  el.innerHTML = _AB_KINDS.map(k => {
    const on = enabled.has(k.key);
    const bg = on ? k.color : "transparent";
    const border = `1px solid ${k.color}`;
    const fg = on ? "#000" : k.color;
    return `<span onclick="_abToggleKind('${k.key}')" style="cursor:pointer;display:inline-flex;align-items:center;padding:0.18rem 0.5rem;border-radius:999px;border:${border};background:${bg};color:${fg};margin-right:0.3rem;font-size:0.75rem">${k.label}</span>`;
  }).join("");
}

async function initAllBluesView() {
  // Public view — worker controls + stats live in /admin → All Blues.
  _abRenderKindChips();
  // Restore persisted focus (id only — name resolves once graph loads).
  try {
    const raw = localStorage.getItem(_AB_FOCUS_KEY);
    if (raw) {
      const obj = JSON.parse(raw);
      if (obj && Number.isFinite(obj.id)) {
        _abFocusId = obj.id;
        _abFocusName = obj.name || "";
      }
    }
  } catch {}
  const inp = document.getElementById("ab-focus-input");
  if (inp && _abFocusName) inp.value = _abFocusName;
  if (_abFirstLoad) {
    _abFirstLoad = false;
    await allBluesReload();
  }
}
window.initAllBluesView = initAllBluesView;

// Admin worker controls (Start/Stop/Status) moved to the admin
// dashboard (/admin → All Blues tab). This file is read-only public.

// ── Graph rendering ───────────────────────────────────────────────

async function _abEnsureCytoscape() {
  if (window.cytoscape) return true;
  const urls = [
    "https://unpkg.com/cytoscape@3.30.0/dist/cytoscape.min.js",
    "https://unpkg.com/layout-base@2.0.1/layout-base.js",
    "https://unpkg.com/cose-base@2.2.0/cose-base.js",
    "https://unpkg.com/cytoscape-fcose@2.2.0/cytoscape-fcose.js",
  ];
  for (const url of urls) {
    await new Promise((resolve) => {
      const s = document.createElement("script");
      s.src = url; s.onload = resolve; s.onerror = resolve;
      document.head.appendChild(s);
    });
  }
  if (window.cytoscape && window.cytoscapeFcose) {
    try { window.cytoscape.use(window.cytoscapeFcose); } catch {}
  }
  return !!window.cytoscape;
}

async function allBluesReload() {
  const el = document.getElementById("all-blues-graph");
  if (!el) return;
  el.innerHTML = `<div style="padding:1rem;color:var(--muted)">Loading…</div>`;
  const enabled = [..._abGetEnabledKinds()];
  const minDeg = parseInt(document.getElementById("ab-min-degree")?.value || "1", 10) || 1;
  const qs = new URLSearchParams({ kinds: enabled.join(","), minDegree: String(minDeg) });
  let data;
  try {
    const r = await fetch(`/api/all-blues/graph?${qs}`);
    if (!r.ok) { el.innerHTML = `<div style="padding:1rem;color:#c66">Failed to load graph</div>`; return; }
    data = await r.json();
  } catch (err) {
    el.innerHTML = `<div style="padding:1rem;color:#c66">Failed: ${_abEsc(err.message)}</div>`; return;
  }
  _abLastGraph = data;
  // If focused id doesn't exist in current dataset (filtered out by
  // kinds / minDegree), clear it silently so the graph still renders.
  let focusedNodes = data.nodes;
  let focusedEdges = data.edges;
  let focusBanner = "";
  if (_abFocusId != null) {
    const hasNode = data.nodes.some(n => n.id === _abFocusId);
    if (hasNode) {
      const reach = _abBfsReachable(data.nodes, data.edges, _abFocusId);
      focusedNodes = data.nodes.filter(n => reach.has(n.id));
      focusedEdges = data.edges.filter(e => reach.has(e.src_id) && reach.has(e.dst_id));
      const display = focusedNodes.find(n => n.id === _abFocusId)?.name || _abFocusName || `Artist ${_abFocusId}`;
      focusBanner = `· focused on ${display} (${focusedNodes.length} reachable)`;
    } else {
      focusBanner = `· focused artist not in current filter`;
    }
  }
  const counts = document.getElementById("ab-graph-counts");
  if (counts) counts.textContent = `· ${data.nodes.length} artists, ${data.edges.length} links ${focusBanner}`;
  _abRenderFocusActive();
  if (!focusedNodes.length) {
    el.innerHTML = `<div style="padding:1rem;color:var(--muted)">No data yet.${window._isAdmin ? ` <a href="/admin#all-blues" style="color:inherit;text-decoration:underline">Run the worker</a> to populate the graph.` : ""}</div>`;
    return;
  }
  const ok = await _abEnsureCytoscape();
  if (!ok) { el.innerHTML = `<div style="padding:1rem;color:#c66">Cytoscape failed to load</div>`; return; }
  el.innerHTML = "";

  const kindColor = Object.fromEntries(_AB_KINDS.map(k => [k.key, k.color]));
  // Build a multi-band gradient per edge so pairs with multiple
  // kinds (family + band, etc.) show every color along the line
  // with hard stops between segments.
  const buildEdgeGradient = (kinds) => {
    const list = Array.isArray(kinds) && kinds.length ? kinds : ["mention"];
    const colors = list.map(k => kindColor[k] || "#888");
    if (colors.length === 1) {
      return { colors: `${colors[0]} ${colors[0]}`, positions: "0% 100%" };
    }
    const n = colors.length;
    const stopColors = [];
    const stopPositions = [];
    for (let i = 0; i < n; i++) {
      const start = (i / n) * 100;
      const end   = ((i + 1) / n) * 100;
      stopColors.push(colors[i], colors[i]);
      stopPositions.push(start.toFixed(2) + "%", end.toFixed(2) + "%");
    }
    return { colors: stopColors.join(" "), positions: stopPositions.join(" ") };
  };
  const elements = [
    ...focusedNodes.map(n => ({ data: { id: String(n.id), label: n.name, focused: n.id === _abFocusId ? 1 : 0 } })),
    ...focusedEdges.map((e, i) => {
      const kinds = Array.isArray(e.kinds) ? e.kinds : (e.kind ? [e.kind] : ["mention"]);
      const grad = buildEdgeGradient(kinds);
      return {
        data: {
          id: `e${i}`,
          source: String(e.src_id),
          target: String(e.dst_id),
          kinds: kinds.join(","),
          gradientColors: grad.colors,
          gradientPositions: grad.positions,
        },
      };
    }),
  ];

  if (_abCy) { try { _abCy.destroy(); } catch {} _abCy = null; }
  _abCy = window.cytoscape({
    container: el,
    elements,
    style: [
      { selector: "node", style: {
        "background-color": "#1f2937",
        "border-color": "#94a3b8", "border-width": 1,
        "label": "data(label)", "color": "#e2e8f0",
        "font-size": 10,
        // Label sits below the node with a thick dark outline AND a
        // semi-transparent dark pill behind it. The combination keeps
        // text readable over yellow focused nodes, over crossing
        // amber/pink/cyan edges, and against the dim background.
        "text-valign": "bottom", "text-halign": "center",
        "text-margin-y": 5,
        "text-outline-width": 3, "text-outline-color": "#000", "text-outline-opacity": 1,
        "text-background-color": "#000", "text-background-opacity": 0.55,
        "text-background-padding": 2, "text-background-shape": "round-rectangle",
        "text-wrap": "ellipsis", "text-max-width": 110,
        "width": 22, "height": 22,
      }},
      { selector: "edge", style: {
        // Banded linear gradient — one segment per kind on the pair.
        // gradientColors lists each color twice with the corresponding
        // gradientPositions creating hard stops between segments so
        // there's no smooth blend.
        "line-fill": "linear-gradient",
        "line-gradient-stop-colors": "data(gradientColors)",
        "line-gradient-stop-positions": "data(gradientPositions)",
        "width": 2.8, "opacity": 0.95,
        "curve-style": "bezier",
      }},
      { selector: "node:selected", style: { "border-color": "#fbbf24", "border-width": 3 }},
      { selector: "node[focused = 1]", style: {
        // Solid amber dot, larger; label still sits below in white-on-
        // black-outline so the text is always readable. The previous
        // version put yellow text on the yellow dot — invisible.
        "background-color": "#fbbf24",
        "border-color": "#000", "border-width": 2,
        "width": 32, "height": 32,
        "color": "#fbbf24", "font-weight": "bold", "font-size": 12,
        "text-outline-width": 3, "text-outline-color": "#000", "text-outline-opacity": 1,
        "text-margin-y": 5,
      }},
    ],
    layout: window.cytoscapeFcose
      ? {
          name: "fcose",
          quality: "proof",
          animate: false,
          // nodeDimensionsIncludeLabels is the key one for overlap —
          // fcose factors the label box into the collision/repulsion
          // math so wide names actually claim their own slot. Without
          // it, the layout packs nodes tight and labels collide.
          nodeDimensionsIncludeLabels: true,
          // Beefier repulsion + longer edges + an explicit minimum
          // node separation gives names room to breathe. Higher
          // numIter (defaults to 2500) lets the simulated annealing
          // settle further so tight clusters un-stick.
          nodeRepulsion: () => 18000,
          idealEdgeLength: () => 140,
          edgeElasticity: () => 0.45,
          nodeSeparation: 90,
          gravity: 0.25,
          gravityRange: 3.8,
          numIter: 4000,
          // tile + packComponents: spread disconnected sub-graphs
          // across the canvas instead of stacking them at the origin.
          tile: true, packComponents: true,
          tilingPaddingVertical: 30, tilingPaddingHorizontal: 30,
          randomize: true,
        }
      : {
          name: "cose",
          animate: false,
          nodeDimensionsIncludeLabels: true,
          nodeRepulsion: 90000, idealEdgeLength: 240,
          numIter: 3000,
        },
    wheelSensitivity: 0.2,
  });
  // Click handlers. Edges → detail popup; nodes → focus that artist.
  _abCy.on("tap", "edge", (evt) => {
    const e = evt.target;
    const src = parseInt(e.data("source"), 10);
    const dst = parseInt(e.data("target"), 10);
    if (Number.isFinite(src) && Number.isFinite(dst)) _abOpenEdgePopup(src, dst);
  });
  _abCy.on("tap", "node", (evt) => {
    const n = evt.target;
    const id = parseInt(n.data("id"), 10);
    if (Number.isFinite(id)) _abOpenArtistPopup(id);
  });
}
window.allBluesReload = allBluesReload;
window._abToggleKind = _abToggleKind;

// ── Focus / search ────────────────────────────────────────────────

function _abBfsReachable(nodes, edges, startId) {
  const adj = new Map();
  for (const n of nodes) adj.set(n.id, []);
  for (const e of edges) {
    if (adj.has(e.src_id) && adj.has(e.dst_id)) {
      adj.get(e.src_id).push(e.dst_id);
      adj.get(e.dst_id).push(e.src_id);
    }
  }
  const seen = new Set([startId]);
  const queue = [startId];
  while (queue.length) {
    const cur = queue.shift();
    for (const nb of (adj.get(cur) || [])) {
      if (!seen.has(nb)) { seen.add(nb); queue.push(nb); }
    }
  }
  return seen;
}

function _abFocusInputChanged() {
  const inp = document.getElementById("ab-focus-input");
  const sug = document.getElementById("ab-focus-suggestions");
  if (!inp || !sug) return;
  const q = inp.value.trim().toLowerCase();
  if (!q) { sug.style.display = "none"; sug.innerHTML = ""; return; }
  const nodes = _abLastGraph.nodes || [];
  const matches = nodes
    .filter(n => (n.name || "").toLowerCase().includes(q))
    .slice(0, 20);
  if (!matches.length) {
    sug.style.display = "block";
    sug.innerHTML = `<div style="padding:0.3rem 0.5rem;color:var(--muted)">No matches in current graph</div>`;
    return;
  }
  sug.style.display = "block";
  sug.innerHTML = matches.map(n => {
    const raw = n.name || `Artist ${n.id}`;
    const name = _abEsc(raw);
    // HTML-escape the JSON-encoded string so quotes/apostrophes in
    // artist names ("Ramblin' Thomas") don't blow up the attribute.
    const jsArg = _abEsc(JSON.stringify(raw));
    return `<div style="padding:0.25rem 0.55rem;cursor:pointer" onmouseover="this.style.background='rgba(255,255,255,0.06)'" onmouseout="this.style.background=''" onclick="_abFocusPick(${n.id}, ${jsArg})">${name} <span style="color:var(--muted);font-size:0.7rem">#${n.id}</span></div>`;
  }).join("");
}
window._abFocusInputChanged = _abFocusInputChanged;

function _abFocusPick(id, name) {
  _abFocusId = id;
  _abFocusName = name || "";
  try { localStorage.setItem(_AB_FOCUS_KEY, JSON.stringify({ id, name })); } catch {}
  const inp = document.getElementById("ab-focus-input");
  if (inp) inp.value = name || "";
  const sug = document.getElementById("ab-focus-suggestions");
  if (sug) { sug.style.display = "none"; sug.innerHTML = ""; }
  allBluesReload();
}
window._abFocusPick = _abFocusPick;

function _abFocusSubmit() {
  // Pressing Enter / Focus button: if there's exactly one match,
  // pick it; otherwise just show the suggestion list.
  const inp = document.getElementById("ab-focus-input");
  if (!inp) return;
  const q = inp.value.trim().toLowerCase();
  if (!q) { _abFocusClear(); return; }
  const nodes = _abLastGraph.nodes || [];
  const matches = nodes.filter(n => (n.name || "").toLowerCase().includes(q));
  if (matches.length === 1) {
    _abFocusPick(matches[0].id, matches[0].name);
  } else if (matches.length === 0) {
    showToast?.("No artist matches “" + inp.value + "” in the current graph", "info");
  } else {
    // Try exact match first.
    const exact = matches.find(n => (n.name || "").toLowerCase() === q);
    if (exact) _abFocusPick(exact.id, exact.name);
    else _abFocusInputChanged();
  }
}
window._abFocusSubmit = _abFocusSubmit;

function _abFocusClear() {
  _abFocusId = null;
  _abFocusName = "";
  try { localStorage.removeItem(_AB_FOCUS_KEY); } catch {}
  const inp = document.getElementById("ab-focus-input");
  if (inp) inp.value = "";
  const sug = document.getElementById("ab-focus-suggestions");
  if (sug) { sug.style.display = "none"; sug.innerHTML = ""; }
  allBluesReload();
}
window._abFocusClear = _abFocusClear;

function _abRenderFocusActive() {
  const el = document.getElementById("ab-focus-active");
  if (!el) return;
  if (_abFocusId == null) { el.textContent = ""; return; }
  el.textContent = `· ${_abFocusName || ("Artist " + _abFocusId)}`;
}

// ── Edge detail popup ─────────────────────────────────────────────
// Clicking a network edge opens this floating panel: both artists as
// mini-cards at top, every edge kind between them with its excerpt,
// and every shared release/master as a clickable mini-card.

function _abEnsureEdgePopup() {
  let el = document.getElementById("ab-edge-popup");
  if (el) return el;
  el = document.createElement("div");
  el.id = "ab-edge-popup";
  el.style.cssText = `
    position:fixed;inset:50% auto auto 50%;transform:translate(-50%,-50%);
    background:#0b1220;border:1px solid var(--border, #333);border-radius:8px;
    padding:0.9rem 1rem;z-index:9999;max-width:min(720px, 95vw);max-height:85vh;
    overflow:hidden auto;box-shadow:0 8px 32px rgba(0,0,0,0.6);
    color:var(--text, #e2e8f0);font-size:0.84rem;display:none`;
  document.body.appendChild(el);
  // Backdrop for outside-click close
  let bd = document.getElementById("ab-edge-popup-bd");
  if (!bd) {
    bd = document.createElement("div");
    bd.id = "ab-edge-popup-bd";
    bd.style.cssText = `position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:9998;display:none`;
    bd.onclick = _abCloseEdgePopup;
    document.body.appendChild(bd);
  }
  return el;
}

function _abCloseEdgePopup() {
  const el = document.getElementById("ab-edge-popup");
  const bd = document.getElementById("ab-edge-popup-bd");
  if (el) el.style.display = "none";
  if (bd) bd.style.display = "none";
}
window._abCloseEdgePopup = _abCloseEdgePopup;

async function _abOpenEdgePopup(srcId, dstId) {
  const el = _abEnsureEdgePopup();
  const bd = document.getElementById("ab-edge-popup-bd");
  if (bd) bd.style.display = "block";
  el.style.display = "block";
  el.innerHTML = `<div style="padding:1rem;color:var(--muted)">Loading…</div>`;
  let data;
  try {
    const r = await fetch(`/api/all-blues/edge?src=${srcId}&dst=${dstId}`);
    if (!r.ok) { el.innerHTML = `<div style="padding:1rem;color:#c66">Failed to load edge details (HTTP ${r.status})</div>`; return; }
    data = await r.json();
  } catch (err) {
    el.innerHTML = `<div style="padding:1rem;color:#c66">Failed: ${_abEsc(err.message)}</div>`; return;
  }
  const kindColor = Object.fromEntries(_AB_KINDS.map(k => [k.key, k.color]));
  const artistCard = (a) => {
    const thumb = a.thumb
      ? `<img src="${_abEsc(a.thumb)}" alt="" style="width:56px;height:56px;border-radius:6px;object-fit:cover;background:#1f2937">`
      : `<div style="width:56px;height:56px;border-radius:6px;background:#1f2937;display:flex;align-items:center;justify-content:center;font-size:0.7rem;color:var(--muted)">no img</div>`;
    // HTML-escape the JSON-stringified name so apostrophes etc. survive
    // the attribute boundary (same pattern as everywhere else we embed
    // a JS-string literal into an onclick).
    const nameArg = _abEsc(JSON.stringify(a.name));
    return `
      <div style="display:flex;gap:0.55rem;align-items:flex-start;padding:0.5rem;background:rgba(255,255,255,0.04);border-radius:6px;flex:1;min-width:0">
        ${thumb}
        <div style="min-width:0;flex:1">
          <a href="#" onclick="event.preventDefault();event.stopPropagation();if(typeof openLookupPopup==='function')openLookupPopup(event,'artist',${nameArg});return false"
             style="font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block;color:inherit;text-decoration:none;cursor:pointer"
             title="Search SeaDisco, Wikipedia, YouTube, Discogs, etc.">${_abEsc(a.name)}</a>
          <div style="font-size:0.7rem;color:var(--muted);margin-top:0.1rem">#${a.id}</div>
        </div>
      </div>`;
  };
  const edgeKindRows = data.edges.length
    ? data.edges.map(e => {
      const color = kindColor[e.kind] || "#888";
      const direction = e.src_id === data.src.id
        ? `<span style="color:var(--muted)">${_abEsc(data.src.name)} →</span> ${_abEsc(data.dst.name)}`
        : `<span style="color:var(--muted)">${_abEsc(data.dst.name)} →</span> ${_abEsc(data.src.name)}`;
      return `
        <div style="padding:0.4rem 0.5rem;border-left:3px solid ${color};margin-bottom:0.35rem;background:rgba(255,255,255,0.03);border-radius:4px">
          <div style="display:flex;align-items:baseline;gap:0.4rem;margin-bottom:0.15rem">
            <span style="background:${color};color:#000;padding:0.08rem 0.4rem;border-radius:999px;font-size:0.68rem;font-weight:600;text-transform:uppercase">${_abEsc(e.kind)}</span>
            <span style="font-size:0.72rem">${direction}</span>
          </div>
          ${e.excerpt ? `<div style="font-size:0.74rem;color:var(--muted);font-style:italic">"…${_abEsc(e.excerpt)}…"</div>` : ""}
        </div>`;
    }).join("")
    : `<div style="color:var(--muted);font-size:0.8rem">No edge metadata.</div>`;
  const releaseCards = data.releases.length
    ? data.releases.map(r => {
      const thumb = r.thumb
        ? `<img src="${_abEsc(r.thumb)}" alt="" style="width:48px;height:48px;border-radius:4px;object-fit:cover;background:#1f2937">`
        : `<div style="width:48px;height:48px;border-radius:4px;background:#1f2937"></div>`;
      const onclick = `onclick="event.stopPropagation();_abOpenReleaseFromPopup(event, ${r.id}, '${_abEsc(r.type)}')"`;
      return `
        <div ${onclick} style="display:flex;gap:0.5rem;align-items:center;padding:0.35rem 0.45rem;background:rgba(255,255,255,0.04);border-radius:5px;cursor:pointer;margin-bottom:0.3rem"
             onmouseover="this.style.background='rgba(255,255,255,0.08)'" onmouseout="this.style.background='rgba(255,255,255,0.04)'">
          ${thumb}
          <div style="min-width:0;flex:1">
            <div style="font-size:0.78rem;font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_abEsc(r.title)}</div>
            <div style="font-size:0.68rem;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
              ${r.year ? `${_abEsc(r.year)} · ` : ""}${r.primary_artists ? _abEsc(r.primary_artists) : `<span style="color:#777;font-style:italic">no credit in cache</span>`}${r.type === "master" ? ' · <span style="color:#f5d442">master</span>' : ""}
            </div>
          </div>
        </div>`;
    }).join("")
    : `<div style="color:var(--muted);font-size:0.78rem;padding:0.4rem 0">No shared releases tracked yet. Mention was from artist profile prose, not liner notes.</div>`;
  el.innerHTML = `
    <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.7rem">
      <div style="font-weight:600;font-size:1rem;flex:1">Connection details</div>
      <button onclick="_abCloseEdgePopup()" style="background:transparent;border:1px solid var(--border, #333);color:inherit;padding:0.15rem 0.55rem;border-radius:4px;cursor:pointer;font-size:0.85rem">×</button>
    </div>
    <div style="display:flex;gap:0.5rem;margin-bottom:0.8rem">
      ${artistCard(data.src)}
      ${artistCard(data.dst)}
    </div>
    <div style="font-size:0.74rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.04em;margin-bottom:0.25rem">Edges</div>
    ${edgeKindRows}
    <div style="font-size:0.74rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.04em;margin:0.7rem 0 0.25rem">Shared releases (${data.releases.length})</div>
    ${releaseCards}`;
}

function _abOpenReleaseFromPopup(event, id, type) {
  // openModal signature: (event, id, type, discogsUrl, opts)
  // The release/album modal is the SPA's main popup — calling it
  // doesn't close ours, so do that explicitly so the user can see it.
  _abCloseEdgePopup();
  if (typeof window.openModal === "function") {
    window.openModal(event, id, type || "release");
  } else {
    window.open(`https://www.discogs.com/${type === "master" ? "master" : "release"}/${id}`, "_blank");
  }
}
window._abOpenReleaseFromPopup = _abOpenReleaseFromPopup;

// ── Artist profile popup ──────────────────────────────────────────
// Tapping a graph node opens this: artist's name + thumb + bio,
// every release we've cached where they're a primary credit, and
// every connection (with all kinds collapsed per partner). Each
// connection row is clickable — it calls _abOpenEdgePopup so the
// user can dive into the detail for that specific pair.

function _abEnsureArtistPopup() {
  let el = document.getElementById("ab-artist-popup");
  if (el) return el;
  el = document.createElement("div");
  el.id = "ab-artist-popup";
  el.style.cssText = `
    position:fixed;inset:50% auto auto 50%;transform:translate(-50%,-50%);
    background:#0b1220;border:1px solid var(--border, #333);border-radius:8px;
    padding:0.9rem 1rem;z-index:9999;max-width:min(760px, 95vw);max-height:88vh;
    overflow:hidden auto;box-shadow:0 8px 32px rgba(0,0,0,0.6);
    color:var(--text, #e2e8f0);font-size:0.84rem;display:none`;
  document.body.appendChild(el);
  let bd = document.getElementById("ab-artist-popup-bd");
  if (!bd) {
    bd = document.createElement("div");
    bd.id = "ab-artist-popup-bd";
    bd.style.cssText = `position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:9998;display:none`;
    bd.onclick = _abCloseArtistPopup;
    document.body.appendChild(bd);
  }
  return el;
}

function _abCloseArtistPopup() {
  const el = document.getElementById("ab-artist-popup");
  const bd = document.getElementById("ab-artist-popup-bd");
  if (el) el.style.display = "none";
  if (bd) bd.style.display = "none";
}
window._abCloseArtistPopup = _abCloseArtistPopup;

async function _abOpenArtistPopup(artistId) {
  const el = _abEnsureArtistPopup();
  const bd = document.getElementById("ab-artist-popup-bd");
  if (bd) bd.style.display = "block";
  el.style.display = "block";
  el.innerHTML = `<div style="padding:1rem;color:var(--muted)">Loading…</div>`;
  let data;
  try {
    const r = await fetch(`/api/all-blues/artist?id=${artistId}`);
    if (!r.ok) { el.innerHTML = `<div style="padding:1rem;color:#c66">Failed to load artist (HTTP ${r.status})</div>`; return; }
    data = await r.json();
  } catch (err) {
    el.innerHTML = `<div style="padding:1rem;color:#c66">Failed: ${_abEsc(err.message)}</div>`; return;
  }
  const kindColor = Object.fromEntries(_AB_KINDS.map(k => [k.key, k.color]));
  const a = data.artist;
  const thumb = a.thumb
    ? `<img src="${_abEsc(a.thumb)}" alt="" style="width:90px;height:90px;border-radius:8px;object-fit:cover;background:#1f2937;flex-shrink:0">`
    : `<div style="width:90px;height:90px;border-radius:8px;background:#1f2937;display:flex;align-items:center;justify-content:center;font-size:0.75rem;color:var(--muted);flex-shrink:0">no img</div>`;
  const nameArg = _abEsc(JSON.stringify(a.name));
  const bio = a.profile
    ? `<div style="font-size:0.8rem;color:var(--muted);line-height:1.45;margin-bottom:0.8rem;white-space:pre-wrap">${_abEsc(a.profile)}</div>`
    : "";
  const connectionRows = data.connections.length
    ? data.connections.map(c => {
        const chips = c.kinds.map(k => {
          const color = kindColor[k] || "#888";
          return `<span style="background:${color};color:#000;padding:0.08rem 0.4rem;border-radius:999px;font-size:0.66rem;font-weight:600;text-transform:uppercase;margin-right:0.25rem">${_abEsc(k)}</span>`;
        }).join("");
        return `
          <div onclick="event.stopPropagation();_abCloseArtistPopup();_abOpenEdgePopup(${a.id}, ${c.partner_id})"
               style="padding:0.35rem 0.5rem;background:rgba(255,255,255,0.04);border-radius:5px;cursor:pointer;margin-bottom:0.3rem;display:flex;align-items:center;gap:0.5rem"
               onmouseover="this.style.background='rgba(255,255,255,0.08)'" onmouseout="this.style.background='rgba(255,255,255,0.04)'">
            <span style="font-size:0.82rem;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_abEsc(c.partner_name)}</span>
            <span style="flex-shrink:0">${chips}</span>
          </div>`;
      }).join("")
    : `<div style="color:var(--muted);font-size:0.78rem;padding:0.3rem 0">No tracked connections yet.</div>`;
  const releaseCards = data.releases.length
    ? data.releases.map(r => {
        const t = r.thumb
          ? `<img src="${_abEsc(r.thumb)}" alt="" style="width:42px;height:42px;border-radius:4px;object-fit:cover;background:#1f2937">`
          : `<div style="width:42px;height:42px;border-radius:4px;background:#1f2937"></div>`;
        return `
          <div onclick="event.stopPropagation();_abOpenReleaseFromArtistPopup(event, ${r.id}, '${_abEsc(r.type)}')"
               style="display:flex;gap:0.5rem;align-items:center;padding:0.3rem 0.4rem;background:rgba(255,255,255,0.04);border-radius:5px;cursor:pointer;margin-bottom:0.25rem"
               onmouseover="this.style.background='rgba(255,255,255,0.08)'" onmouseout="this.style.background='rgba(255,255,255,0.04)'">
            ${t}
            <div style="min-width:0;flex:1">
              <div style="font-size:0.78rem;font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${_abEsc(r.title)}</div>
              <div style="font-size:0.66rem;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
                ${r.year ? `${_abEsc(r.year)} · ` : ""}${r.primary_artists ? _abEsc(r.primary_artists) : `<span style="font-style:italic">no credit</span>`}${r.type === "master" ? ' · <span style="color:#f5d442">master</span>' : ""}
              </div>
            </div>
          </div>`;
      }).join("")
    : `<div style="color:var(--muted);font-size:0.78rem;padding:0.3rem 0">No cached releases for this artist.</div>`;
  el.innerHTML = `
    <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.7rem">
      <div style="font-weight:600;font-size:1rem;flex:1">Artist</div>
      <button onclick="_abCloseArtistPopup()" style="background:transparent;border:1px solid var(--border, #333);color:inherit;padding:0.15rem 0.55rem;border-radius:4px;cursor:pointer;font-size:0.85rem">×</button>
    </div>
    <div style="display:flex;gap:0.75rem;align-items:flex-start;margin-bottom:0.8rem">
      ${thumb}
      <div style="min-width:0;flex:1">
        <a href="#" onclick="event.preventDefault();event.stopPropagation();if(typeof openLookupPopup==='function')openLookupPopup(event,'artist',${nameArg});return false"
           style="font-weight:700;font-size:1.05rem;color:inherit;text-decoration:none;cursor:pointer"
           title="Search SeaDisco, Wikipedia, YouTube, Discogs, etc.">${_abEsc(a.name)}</a>
        <div style="font-size:0.72rem;color:var(--muted);margin-top:0.2rem">
          #${a.id}${a.seed_year ? ` · first cached: ${_abEsc(a.seed_year)}` : ""}
        </div>
        <div style="margin-top:0.4rem;font-size:0.72rem">
          <a href="${_abEsc(a.discogs_url)}" target="_blank" rel="noopener" style="color:#60a5fa">Discogs ↗</a>
        </div>
      </div>
    </div>
    ${bio}
    <div style="font-size:0.74rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.04em;margin:0.5rem 0 0.25rem">Connections (${data.connections.length})</div>
    ${connectionRows}
    <div style="font-size:0.74rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.04em;margin:0.7rem 0 0.25rem">Releases (${data.releases.length})</div>
    ${releaseCards}`;
}
window._abOpenArtistPopup = _abOpenArtistPopup;

function _abOpenReleaseFromArtistPopup(event, id, type) {
  _abCloseArtistPopup();
  if (typeof window.openModal === "function") {
    window.openModal(event, id, type || "release");
  } else {
    window.open(`https://www.discogs.com/${type === "master" ? "master" : "release"}/${id}`, "_blank");
  }
}
window._abOpenReleaseFromArtistPopup = _abOpenReleaseFromArtistPopup;
