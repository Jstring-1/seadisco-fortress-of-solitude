// ── All Blues view ────────────────────────────────────────────────
//
// Admin-only network visualization of artist relationships parsed
// from cached Discogs profiles. Loaded lazily by switchView('all-
// blues'). Reuses the Cytoscape lazy-loader pattern from blues-
// archive.js's Connections subtab.

const _AB_KINDS = [
  { key: "spouse",   color: "#ec4899", label: "Spouse" },
  { key: "family",   color: "#f97316", label: "Family" },
  { key: "mentor",   color: "#a855f7", label: "Mentor" },
  { key: "band",     color: "#4ade80", label: "Band" },
  { key: "alias",    color: "#22d3ee", label: "Alias" },
  { key: "traveled", color: "#fb7185", label: "Traveled" },
  { key: "mention",  color: "#f5d442", label: "Mention" },
];
const _AB_KIND_KEY  = "sd_all_blues_kinds";
const _AB_FOCUS_KEY = "sd_all_blues_focus";
const _AB_YEARS_KEY = "sd_all_blues_years";
let _abCy = null;
let _abFirstLoad = true;
let _abLastLayoutWasCached = false; // true if the latest render used preset positions
let _abForceFreshLayout = false;    // admin "Recompute layout" override — strip positions for one render
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
  // Re-sync the admin-only layout buttons once the lazy admin probe
  // resolves, so the buttons pop in on direct URL nav without
  // requiring a reload.
  if (typeof window._isAdmin !== "boolean" && typeof window._ensureAdminFlag === "function") {
    window._ensureAdminFlag().then(() => _abSyncSaveLayoutButton?.()).catch(() => {});
  }
  _abRenderKindChips();
  // Hydrate the year-range inputs from localStorage so a refresh
  // preserves the user's last filter.
  try {
    const raw = localStorage.getItem(_AB_YEARS_KEY);
    if (raw) {
      const obj = JSON.parse(raw);
      const fromI = document.getElementById("ab-from-year-filter");
      const toI   = document.getElementById("ab-to-year-filter");
      if (fromI && Number.isFinite(obj.from)) fromI.value = obj.from;
      if (toI   && Number.isFinite(obj.to))   toI.value   = obj.to;
    }
  } catch {}
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
  const fromY = parseInt(document.getElementById("ab-from-year-filter")?.value || "1900", 10) || 1900;
  const toY   = parseInt(document.getElementById("ab-to-year-filter")?.value   || "2100", 10) || 2100;
  const qs = new URLSearchParams({
    kinds: enabled.join(","),
    minDegree: String(minDeg),
    fromYear: String(fromY),
    toYear:   String(toY),
  });
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
  // Position cache: if EVERY visible node has a cached (x,y), use a
  // preset layout — instant render. Otherwise fall back to fcose so
  // new/unpositioned nodes still get a real layout pass. Admin's
  // "Recompute layout" sets _abForceFreshLayout to bypass the cache
  // for one render.
  const allPositioned = !_abForceFreshLayout
    && focusedNodes.every(n => Number.isFinite(n.x) && Number.isFinite(n.y));
  _abForceFreshLayout = false;
  const elements = [
    ...focusedNodes.map(n => {
      const el = {
        data: { id: String(n.id), label: n.name, focused: n.id === _abFocusId ? 1 : 0 },
      };
      if (Number.isFinite(n.x) && Number.isFinite(n.y)) {
        el.position = { x: n.x, y: n.y };
      }
      return el;
    }),
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
  _abLastLayoutWasCached = allPositioned;

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
      // Highlight system: tap a node → it + its edges + 1-hop
      // neighbors get .ab-highlighted; everything else gets
      // .ab-faded. Tap the empty canvas to clear.
      { selector: ".ab-faded", style: {
        "opacity": 0.12, "text-opacity": 0,
        // Faded elements render behind highlighted ones so clicks
        // near an intersection don't fall through to a dimmed edge.
        "z-index": 1,
      }},
      { selector: "node.ab-highlighted", style: {
        "border-color": "#fbbf24", "border-width": 2.5,
        "background-color": "#3b4456",
        "z-index": 20,
      }},
      { selector: "node.ab-source", style: {
        "background-color": "#fbbf24",
        "border-color": "#000", "border-width": 3,
        "width": 32, "height": 32,
        "color": "#fbbf24", "font-weight": "bold", "font-size": 12,
        "text-outline-width": 3, "text-outline-color": "#000",
        "z-index": 30,
      }},
      { selector: "edge.ab-highlighted", style: {
        "width": 4.5, "opacity": 1,
        // Bump highlighted edges above the faded ones so a click on
        // an intersection hits the bold (focused) line, not the dim
        // one underneath. Cytoscape's hit-test order follows z-index.
        "z-index": 15,
      }},
    ],
    layout: allPositioned
      ? { name: "preset", animate: false, fit: true, padding: 40 }
      : (window.cytoscapeFcose
        ? {
            name: "fcose",
            quality: "proof",
            animate: false,
            nodeDimensionsIncludeLabels: true,
            nodeRepulsion: () => 90000,
            idealEdgeLength: () => 280,
            edgeElasticity: () => 0.25,
            nodeSeparation: 180,
            gravity: 0.08,
            gravityRange: 5,
            numIter: 6000,
            tile: true, packComponents: true,
            tilingPaddingVertical: 60, tilingPaddingHorizontal: 60,
            randomize: true,
          }
        : {
            name: "cose",
            animate: false,
            nodeDimensionsIncludeLabels: true,
            nodeRepulsion: 200000, idealEdgeLength: 320,
            gravity: 0.08,
            numIter: 4000,
          }),
    wheelSensitivity: 0.2,
    minZoom: 0.05,
    maxZoom: 4,
  });
  // Update the admin save-layout button visibility: only relevant when
  // we just ran a real layout pass (not preset).
  _abSyncSaveLayoutButton();
  // Click handlers. Edges → detail popup; nodes → focus that artist.
  _abCy.on("tap", "edge", (evt) => {
    const e = evt.target;
    // Read source/target via the node-level API rather than data()
    // — data("source") returns the raw stored value (string) but
    // edge.source().id() always returns the actual connected node's
    // id, which is the safer truth in case data attrs got mangled
    // during a re-render.
    const srcStr = e.source?.()?.id?.() ?? e.data("source");
    const dstStr = e.target?.()?.id?.() ?? e.data("target");
    const src = parseInt(srcStr, 10);
    const dst = parseInt(dstStr, 10);
    if (Number.isFinite(src) && Number.isFinite(dst) && src > 0 && dst > 0 && src !== dst) {
      _abOpenEdgePopup(src, dst);
    } else {
      console.warn("[constellations] edge tap: invalid endpoints", { srcStr, dstStr });
    }
  });
  _abCy.on("tap", "node", (evt) => {
    const n = evt.target;
    const id = parseInt(n.data("id"), 10);
    if (!Number.isFinite(id)) return;
    // Two things at once: highlight the node's web so the user can
    // actually see its connections in a dense graph, AND open the
    // action menu so they can choose between profile / focus / search.
    _abHighlightNode(n);
    const cyEl = document.getElementById("all-blues-graph");
    const rect = cyEl?.getBoundingClientRect();
    const x = (rect?.left ?? 0) + (evt.renderedPosition?.x ?? 0);
    const y = (rect?.top  ?? 0) + (evt.renderedPosition?.y ?? 0);
    _abShowNodeActions(id, n.data("label") || `Artist ${id}`, x, y);
  });
  // Tap the background (anywhere other than a node or edge) to clear
  // the highlight and any open menu. evt.target === cy means the tap
  // hit the canvas itself.
  _abCy.on("tap", (evt) => {
    if (evt.target === _abCy) {
      _abClearHighlight();
      _abCloseNodeMenu();
    }
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
  if (!q) {
    sug.style.display = "none";
    sug.innerHTML = "";
    // Emptying the input — whether via the browser's native X clear,
    // Backspace, Ctrl+A+Delete, or any other path — should reset the
    // focus filter and reload the unfiltered network, same as the
    // dedicated Clear button. Only fire if there's an active focus to
    // clear so we don't reload the graph for every empty keystroke.
    if (_abFocusId != null) _abFocusClear();
    return;
  }
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

// Year-range filter change handler: clamps to a valid range, persists
// to localStorage, then reloads the graph. Hooked from the From/To
// input onchange so any edit (keyboard, scroll-wheel, spinner) fires.
// Layout cache controls (admin-only) — visible via _abSyncSaveLayoutButton.
function _abSyncSaveLayoutButton() {
  const save = document.getElementById("ab-save-layout");
  const recomp = document.getElementById("ab-recompute-layout");
  if (!save || !recomp) return;
  // Belt-and-suspenders gate: real admin flag AND respect the
  // "view as user" toggle so a curator browsing in user-mode sees
  // the same UI as a public visitor. localStorage key matches the
  // admin dashboard's "View as user" button.
  let viewAsUser = false;
  try { viewAsUser = localStorage.getItem("sd-admin-as-user") === "1"; } catch {}
  const isAdmin = !!window._isAdmin && !viewAsUser;
  if (!isAdmin) {
    save.style.display = "none";
    recomp.style.display = "none";
    return;
  }
  // Save shows only when there are positions to save (i.e. we just
  // ran fcose). Recompute is always available for admin.
  save.style.display = _abCy && !_abLastLayoutWasCached ? "" : "none";
  recomp.style.display = "";
}

async function _abSaveLayoutClick() {
  if (!_abCy) return;
  const positions = {};
  _abCy.nodes().forEach(n => {
    const id = parseInt(n.data("id"), 10);
    const p = n.position();
    if (Number.isFinite(id) && p && Number.isFinite(p.x) && Number.isFinite(p.y)) {
      positions[id] = { x: p.x, y: p.y };
    }
  });
  if (!Object.keys(positions).length) return;
  const btn = document.getElementById("ab-save-layout");
  if (btn) { btn.disabled = true; btn.textContent = "Saving…"; }
  try {
    const r = await fetch("/api/admin/all-blues/positions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ positions }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    showToast?.(`Saved positions for ${j.updated} artists`, "success");
    // The current render is already what was saved — flip the cached
    // flag so the Save button hides until the next fresh layout pass.
    _abLastLayoutWasCached = true;
    _abSyncSaveLayoutButton();
  } catch (err) {
    showToast?.("Save failed: " + err.message, "error");
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "Save layout"; }
  }
}
window._abSaveLayoutClick = _abSaveLayoutClick;

function _abRecomputeLayoutClick() {
  // Force-clear cached positions for the *visible* nodes, then reload
  // — the graph endpoint returns positions but allPositioned will be
  // false because we just stripped them in-memory. Result: fcose runs
  // fresh. Admin then clicks Save layout to persist.
  if (!_abCy) return;
  _abCy.nodes().forEach(n => n.removeData?.("x"));
  // Hacky but effective: reload and forcibly null the .x/.y of the
  // incoming data so the layout-selection logic picks fcose.
  _abForceFreshLayout = true;
  allBluesReload();
}
window._abRecomputeLayoutClick = _abRecomputeLayoutClick;

function _abYearInputChanged() {
  const fromI = document.getElementById("ab-from-year-filter");
  const toI   = document.getElementById("ab-to-year-filter");
  if (!fromI || !toI) return;
  let from = parseInt(fromI.value, 10);
  let to   = parseInt(toI.value, 10);
  if (!Number.isFinite(from)) from = 1900;
  if (!Number.isFinite(to))   to   = 2100;
  // Swap if reversed so the user doesn't get an empty graph just for
  // typing the larger number first.
  if (from > to) { [from, to] = [to, from]; }
  fromI.value = from;
  toI.value   = to;
  try { localStorage.setItem(_AB_YEARS_KEY, JSON.stringify({ from, to })); } catch {}
  allBluesReload();
}
window._abYearInputChanged = _abYearInputChanged;

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
    padding:0.9rem 1rem;z-index:120;max-width:min(720px, 95vw);max-height:85vh;
    overflow:hidden auto;box-shadow:0 8px 32px rgba(0,0,0,0.6);
    color:var(--text, #e2e8f0);font-size:0.84rem;display:none`;
  document.body.appendChild(el);
  // Backdrop for outside-click close
  let bd = document.getElementById("ab-edge-popup-bd");
  if (!bd) {
    bd = document.createElement("div");
    bd.id = "ab-edge-popup-bd";
    bd.style.cssText = `position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:115;display:none`;
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
  // Sanity-check inputs before showing the popup. Without this we
  // surfaced an HTTP 400 every time a tap landed on stale/invalid
  // edge data (transient during graph re-renders, intersection
  // mis-hits, etc).
  const src = Number(srcId);
  const dst = Number(dstId);
  if (!Number.isFinite(src) || !Number.isFinite(dst) || src <= 0 || dst <= 0 || src === dst) {
    console.warn("[constellations] edge popup: invalid src/dst", srcId, dstId);
    return;
  }
  const el = _abEnsureEdgePopup();
  const bd = document.getElementById("ab-edge-popup-bd");
  if (bd) bd.style.display = "block";
  el.style.display = "block";
  el.innerHTML = `<div style="padding:1rem;color:var(--muted)">Loading…</div>`;
  let data;
  try {
    const url = `/api/all-blues/edge?src=${src}&dst=${dst}`;
    const r = await fetch(url);
    if (!r.ok) {
      const body = await r.text().catch(() => "");
      console.warn("[constellations] edge popup failed", { url, status: r.status, body });
      el.innerHTML = `<div style="padding:1rem;color:#c66">Failed to load edge details (HTTP ${r.status})${body ? `<div style="margin-top:0.4rem;font-size:0.72rem;color:var(--muted);font-family:monospace;word-break:break-all">${_abEsc(body.slice(0, 240))}</div>` : ""}</div>`;
      return;
    }
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
          <a href="#" onclick="event.preventDefault();event.stopPropagation();_abOpenArtistProfile(${a.id}, ${nameArg});return false"
             style="font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block;color:inherit;text-decoration:none;cursor:pointer"
             title="Open profile — Blues Archive if available, otherwise the Constellations summary">${_abEsc(a.name)}</a>
          <div style="font-size:0.7rem;color:var(--muted);margin-top:0.1rem">#${a.id}</div>
          <div style="margin-top:0.25rem">
            <a href="#" onclick="event.preventDefault();event.stopPropagation();if(typeof openLookupPopup==='function')openLookupPopup(event,'artist',${nameArg});return false"
               style="color:#60a5fa;font-size:0.7rem;text-decoration:none">Search… ↗</a>
          </div>
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
  // Leave the edge popup open behind the album modal so the user can
  // dismiss the release view and continue reading the connection
  // details. The SPA's modal-overlay layers above our popup, so the
  // active modal wins visually without us having to tear ours down.
  if (typeof window.openModal === "function") {
    window.openModal(event, id, type || "release");
  } else {
    window.open(`https://www.discogs.com/${type === "master" ? "master" : "release"}/${id}`, "_blank");
  }
}
window._abOpenReleaseFromPopup = _abOpenReleaseFromPopup;

// ── Highlight / fade ──────────────────────────────────────────────
// When the user taps a node, dim everything in the graph and brighten
// just that node + its connected edges + 1-hop neighbors. Lets the
// user see the artist's web at a glance even when the surrounding
// graph is dense. Tap the empty canvas to clear.

function _abHighlightNode(node) {
  if (!_abCy) return;
  const incident = node.connectedEdges();
  const neighbors = incident.connectedNodes();
  const focus = node.union(incident).union(neighbors);
  // Reset first so re-tapping a different node doesn't accumulate classes.
  _abCy.elements().removeClass("ab-highlighted ab-source ab-faded");
  // Fade everything not in the focus set.
  _abCy.elements().difference(focus).addClass("ab-faded");
  // Highlight the focused subset; mark the originating node as
  // "source" so it stays the visual anchor (amber dot).
  focus.addClass("ab-highlighted");
  node.addClass("ab-source");
}

function _abClearHighlight() {
  if (!_abCy) return;
  _abCy.elements().removeClass("ab-highlighted ab-source ab-faded");
}
window._abClearHighlight = _abClearHighlight;

// ── Node action menu ──────────────────────────────────────────────
// Small two-button floater that pops up at the click point when a
// graph node is tapped. Lets the user disambiguate between "open
// the artist profile popup" and "focus this artist in the network"
// since both gestures are valuable but mean different things.

function _abEnsureNodeMenu() {
  let el = document.getElementById("ab-node-menu");
  if (el) return el;
  el = document.createElement("div");
  el.id = "ab-node-menu";
  el.style.cssText = `
    position:fixed;z-index:110;display:none;
    background:#0b1220;border:1px solid var(--border, #333);border-radius:6px;
    box-shadow:0 6px 20px rgba(0,0,0,0.5);
    padding:0.25rem;font-size:0.82rem;min-width:180px`;
  document.body.appendChild(el);
  // Single global click-outside dismiss handler — attached once.
  document.addEventListener("click", (e) => {
    const m = document.getElementById("ab-node-menu");
    if (!m || m.style.display === "none") return;
    if (!m.contains(e.target)) _abCloseNodeMenu();
  }, true);
  return el;
}

function _abCloseNodeMenu() {
  const el = document.getElementById("ab-node-menu");
  if (el) el.style.display = "none";
}
window._abCloseNodeMenu = _abCloseNodeMenu;

function _abShowNodeActions(artistId, artistName, x, y) {
  // Defer the open by one tick. The DOM click event from the cytoscape
  // tap is still propagating when this function fires; without the
  // setTimeout, the document-level "close on outside click" handler
  // sees the menu we just made visible and the click target (a graph
  // node, not inside the menu) and dismisses it immediately. Running
  // on the next tick lets that click finish first, then we display.
  setTimeout(() => _abShowNodeActionsImpl(artistId, artistName, x, y), 0);
}

function _abShowNodeActionsImpl(artistId, artistName, x, y) {
  const el = _abEnsureNodeMenu();
  const nameArg = _abEsc(JSON.stringify(artistName));
  // Two primary actions + a header showing which artist this menu is for.
  el.innerHTML = `
    <div style="padding:0.35rem 0.55rem 0.4rem;border-bottom:1px solid rgba(255,255,255,0.08);font-weight:600;font-size:0.78rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_abEsc(artistName)}">${_abEsc(artistName)}</div>
    <button type="button"
            onclick="_abCloseNodeMenu();_abOpenArtistProfile(${artistId}, ${nameArg})"
            style="display:block;width:100%;text-align:left;padding:0.4rem 0.55rem;background:transparent;border:0;color:inherit;cursor:pointer;font-size:0.82rem"
            onmouseover="this.style.background='rgba(255,255,255,0.08)'" onmouseout="this.style.background='transparent'">
      <span style="margin-right:0.4rem">👤</span>Open profile
    </button>
    <button type="button"
            onclick="_abCloseNodeMenu();_abFocusPick(${artistId}, ${nameArg})"
            style="display:block;width:100%;text-align:left;padding:0.4rem 0.55rem;background:transparent;border:0;color:inherit;cursor:pointer;font-size:0.82rem"
            onmouseover="this.style.background='rgba(255,255,255,0.08)'" onmouseout="this.style.background='transparent'">
      <span style="margin-right:0.4rem">🎯</span>Focus on artist in network
    </button>
    <button type="button"
            onclick="event.stopPropagation();_abCloseNodeMenu();if(typeof openLookupPopup==='function')openLookupPopup(event,'artist',${nameArg})"
            style="display:block;width:100%;text-align:left;padding:0.4rem 0.55rem;background:transparent;border:0;color:inherit;cursor:pointer;font-size:0.82rem"
            onmouseover="this.style.background='rgba(255,255,255,0.08)'" onmouseout="this.style.background='transparent'">
      <span style="margin-right:0.4rem">🔎</span>Search…
    </button>`;
  el.style.display = "block";
  // Position relative to click; clamp to viewport.
  const W = 220, H = 130;
  const left = Math.min(window.innerWidth  - W - 8, Math.max(8, x));
  const top  = Math.min(window.innerHeight - H - 8, Math.max(8, y + 8));
  el.style.left = `${left}px`;
  el.style.top  = `${top}px`;
}

// Open the right profile for a Discogs artist id. Admin gets routed
// to the Blues Archive popup when the artist is in the archive (more
// curated info: bio, photo, lyrics, tunings, releases). Public
// visitors and unarchived artists fall back to the Constellations
// artist popup with the cached Discogs profile + cached releases +
// connections list. Lazy-loads /blues-archive.js when needed.
async function _abOpenArtistProfile(discogsId, artistName) {
  if (window._isAdmin) {
    try {
      const r = await fetch("/api/blues-archive/check", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          artistIds: [discogsId],
          artistNames: artistName ? [artistName] : [],
        }),
      });
      if (r.ok) {
        const result = await r.json();
        const hit = result.artistsById?.[String(discogsId)]
          || (artistName && result.artists?.[String(artistName).toLowerCase().trim()]);
        if (hit?.id) {
          // BA opener lives in blues-archive.js — lazy-load if it's
          // not in the page yet (admin lands here from any view).
          if (typeof window._baOpenArtistFromBadge !== "function" && typeof window._sdLoadModule === "function") {
            try { await window._sdLoadModule("/blues-archive.js"); } catch {}
          }
          if (typeof window._baOpenArtistFromBadge === "function") {
            window._baOpenArtistFromBadge(hit.id);
            return;
          }
        }
      }
    } catch (err) {
      console.warn("[constellations] BA lookup failed, falling back to local popup", err);
    }
  }
  // Fallback for everyone: the Constellations artist popup.
  _abOpenArtistPopup(discogsId);
}
window._abOpenArtistProfile = _abOpenArtistProfile;

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
    padding:0.9rem 1rem;z-index:120;max-width:min(760px, 95vw);max-height:88vh;
    overflow:hidden auto;box-shadow:0 8px 32px rgba(0,0,0,0.6);
    color:var(--text, #e2e8f0);font-size:0.84rem;display:none`;
  document.body.appendChild(el);
  let bd = document.getElementById("ab-artist-popup-bd");
  if (!bd) {
    bd = document.createElement("div");
    bd.id = "ab-artist-popup-bd";
    bd.style.cssText = `position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:115;display:none`;
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
  // Same as _abOpenReleaseFromPopup — keep the artist popup open
  // behind the album modal so the user returns to it on close.
  if (typeof window.openModal === "function") {
    window.openModal(event, id, type || "release");
  } else {
    window.open(`https://www.discogs.com/${type === "master" ? "master" : "release"}/${id}`, "_blank");
  }
}
window._abOpenReleaseFromArtistPopup = _abOpenReleaseFromArtistPopup;
