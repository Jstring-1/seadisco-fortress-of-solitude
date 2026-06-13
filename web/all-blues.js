// ── All Blues view ────────────────────────────────────────────────
//
// Admin-only network visualization of artist relationships parsed
// from cached Discogs profiles. Loaded lazily by switchView('all-
// blues'). Reuses the Cytoscape lazy-loader pattern from blues-
// archive.js's Connections subtab.

// Four user-facing buckets that fold together the seven raw worker
// kinds. The backend storage stays the same; this mapping translates
// in both directions: when sending the kinds filter to the API we
// expand each enabled bucket into the underlying raw kinds, and when
// rendering edges we collapse the raw kinds back into bucket colors.
const _AB_KINDS = [
  { key: "played_with", color: "#4ade80", label: "Played with", raw: ["band"] },
  { key: "pseudonyms",  color: "#22d3ee", label: "Pseudonyms",   raw: ["alias"] },
  { key: "family",      color: "#f97316", label: "Family",       raw: ["family", "spouse", "mentor"] },
  { key: "mentions",    color: "#f5d442", label: "Mentions",     raw: ["mention", "traveled"] },
];
// Reverse lookup: raw worker kind → bucket key.
const _AB_KIND_TO_BUCKET = (() => {
  const m = {};
  for (const k of _AB_KINDS) for (const r of k.raw) m[r] = k.key;
  return m;
})();
// Resolve a raw worker kind (band, alias, family, spouse, mentor,
// mention, traveled) to the user-facing bucket {label, color} so the
// connection pills in the edge / artist popups match the filter chips
// at the top of the graph.
function _abBucketFor(rawKind) {
  const bucketKey = _AB_KIND_TO_BUCKET[rawKind];
  const def = _AB_KINDS.find(k => k.key === bucketKey);
  return def
    ? { label: def.label, color: def.color }
    : { label: String(rawKind || "").replace(/_/g, " "), color: "#888" };
}
const _AB_KIND_KEY  = "sd_all_blues_kinds";
const _AB_FOCUS_KEY = "sd_all_blues_focus";
const _AB_YEARS_KEY = "sd_all_blues_years";
let _abCy = null;
let _abFirstLoad = true;
let _abLastLayoutWasCached = false; // true if the latest render used preset positions
let _abUnpositionedCount = 0; // how many nodes this render came in without saved positions
let _abTotalNodeCount = 0;    // total nodes this render — for the Save button hint
let _abLastFetchedAt = null;  // timestamp of last successful graph fetch
let _abForceFreshLayout = false;    // admin "Recompute layout" override — strip positions for one render
let _abFitToken = 0;                 // bump from reload so applyFilters re-centers + caps zoom
let _abLastFitToken = -1;            // last token applyFilters acted on
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
  // Visibility filter, not re-fetch — kinds chip toggle is instant.
  if (_abCy) _abApplyFilters(); else allBluesReload();
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
  // Hydrate the (hidden) year-range inputs from localStorage so a
  // refresh preserves the user's last window. Old saves with custom
  // widths (from earlier "any range" version) get snapped to the
  // locked 11-year width by _abSyncWindowSlider below.
  try {
    const raw = localStorage.getItem(_AB_YEARS_KEY);
    const fromI = document.getElementById("ab-from-year-filter");
    const toI   = document.getElementById("ab-to-year-filter");
    if (raw && fromI && toI) {
      const obj = JSON.parse(raw);
      if (Number.isFinite(obj.from)) fromI.value = obj.from;
      if (Number.isFinite(obj.to))   toI.value   = obj.to;
    }
  } catch {}
  // Sync the window slider thumb + label and normalize the inputs to
  // the locked 11-year width so the toolbar reflects state right from
  // first paint. If nothing was stored, the HTML defaults (1930/1940)
  // win — first-time visitors land on the 1930s era.
  _abSyncWindowSlider();
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
  let data;
  let fetchedAt;
  try {
    // Always fetch the FULL graph — no kind / year / degree params.
    // The server response-cache returns the same hit for everyone, so
    // this is one hot DB query shared across users. Filtering (slider,
    // kind chips, min degree) happens client-side via visibility on
    // the laid-out cytoscape, so the slider never re-fetches.
    const r = await fetch(`/api/all-blues/graph?_=${Date.now()}`, {
      cache: "no-store",
    });
    if (!r.ok) { el.innerHTML = `<div style="padding:1rem;color:#c66">Failed to load graph</div>`; return; }
    data = await r.json();
    fetchedAt = new Date();
  } catch (err) {
    el.innerHTML = `<div style="padding:1rem;color:#c66">Failed: ${_abEsc(err.message)}</div>`; return;
  }
  _abLastGraph = data;
  _abLastFetchedAt = fetchedAt;
  // Render the FULL graph. Every node, every edge. Filtering (slider,
  // kind chips, min degree, focus) happens after layout via
  // _abApplyFilters() which only flips visibility on the laid-out
  // cytoscape — no re-fetch, no re-layout when those change.
  // Admin Recompute: nuke every node's saved x/y so the cold fcose
  // path runs (refinement of an already-good layout is a no-op and
  // leaves the canvas blank for big graphs).
  if (_abForceFreshLayout) {
    for (const n of data.nodes) { n.x = null; n.y = null; }
  }
  const focusedNodes = data.nodes;
  const focusedEdges = data.edges;
  const counts = document.getElementById("ab-graph-counts");
  if (counts) {
    const stamp = fetchedAt ? fetchedAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "";
    counts.textContent = `· ${data.nodes.length} artists, ${data.edges.length} links${stamp ? ` · fetched ${stamp}` : ""}`;
  }
  _abRenderFocusActive();
  if (!focusedNodes.length) {
    el.innerHTML = `<div style="padding:1rem;color:var(--muted)">No data yet.${window._isAdmin ? ` <a href="/admin#all-blues" style="color:inherit;text-decoration:underline">Run the worker</a> to populate the graph.` : ""}</div>`;
    return;
  }
  const ok = await _abEnsureCytoscape();
  if (!ok) { el.innerHTML = `<div style="padding:1rem;color:#c66">Cytoscape failed to load</div>`; return; }
  el.innerHTML = "";

  const bucketColor = Object.fromEntries(_AB_KINDS.map(k => [k.key, k.color]));
  // Translate the raw worker kinds on an edge into the user-facing
  // buckets, deduped + ordered by the bucket priority. e.g. an edge
  // with kinds ["band", "family", "spouse"] becomes buckets
  // ["played_with", "family"] (spouse folds into family).
  const bucketKinds = (rawList) => {
    const list = Array.isArray(rawList) && rawList.length ? rawList : ["mention"];
    const seen = new Set();
    const out = [];
    for (const raw of list) {
      const b = _AB_KIND_TO_BUCKET[raw] || "mentions";
      if (!seen.has(b)) { seen.add(b); out.push(b); }
    }
    // Order by the chip order so the gradient is stable across renders.
    const priority = Object.fromEntries(_AB_KINDS.map((k, i) => [k.key, i]));
    out.sort((a, b) => (priority[a] ?? 99) - (priority[b] ?? 99));
    return out;
  };
  // Build a multi-band gradient per edge so pairs with multiple
  // bucket kinds show every color along the line with hard stops.
  const buildEdgeGradient = (rawKinds) => {
    const buckets = bucketKinds(rawKinds);
    const colors = buckets.map(b => bucketColor[b] || "#888");
    if (colors.length === 1) {
      return { buckets, colors: `${colors[0]} ${colors[0]}`, positions: "0% 100%" };
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
    return { buckets, colors: stopColors.join(" "), positions: stopPositions.join(" ") };
  };

  // ── Visual derivations: degree, era color, cluster, label rank ──
  // Degree per node from the edges in this view.
  const degMap = new Map();
  for (const e of focusedEdges) {
    degMap.set(e.src_id, (degMap.get(e.src_id) || 0) + 1);
    degMap.set(e.dst_id, (degMap.get(e.dst_id) || 0) + 1);
  }
  const maxDeg = Math.max(1, ...focusedNodes.map(n => degMap.get(n.id) || 0));
  // Connected components — BFS over the edge graph; each component
  // gets the next cluster id. 8 distinct hues cycle through them so
  // visually adjacent clusters stay distinguishable.
  const CLUSTER_HUES = ["#60a5fa","#a78bfa","#f472b6","#34d399","#fbbf24","#fb923c","#22d3ee","#94a3b8"];
  const adj = new Map();
  for (const n of focusedNodes) adj.set(n.id, []);
  for (const e of focusedEdges) {
    if (adj.has(e.src_id) && adj.has(e.dst_id)) {
      adj.get(e.src_id).push(e.dst_id);
      adj.get(e.dst_id).push(e.src_id);
    }
  }
  const clusterMap = new Map();
  let nextCluster = 0;
  for (const n of focusedNodes) {
    if (clusterMap.has(n.id)) continue;
    const cid = nextCluster++;
    const queue = [n.id];
    clusterMap.set(n.id, cid);
    while (queue.length) {
      const cur = queue.shift();
      for (const nb of (adj.get(cur) || [])) {
        if (!clusterMap.has(nb)) { clusterMap.set(nb, cid); queue.push(nb); }
      }
    }
  }
  // Era color: gradient from blue (1900) → orange (1970+). Unknown
  // seed_year falls back to a muted slate.
  const eraColor = (yr) => {
    if (!Number.isFinite(yr)) return "#1f2937";
    const t = Math.max(0, Math.min(1, (yr - 1900) / 80));
    const hue = 210 - t * 180; // 210 (blue) → 30 (orange)
    return `hsl(${hue.toFixed(0)}, 50%, 32%)`;
  };
  // Adaptive labels: only the top-N most-connected nodes keep their
  // label visible until the user zooms in. Tight set (top 3% / min 8)
  // so the default view is readable instead of a label storm. User
  // zooms in past the threshold OR clicks a node to surface the rest.
  const sortedByDeg = [...focusedNodes].sort((a, b) =>
    (degMap.get(b.id) || 0) - (degMap.get(a.id) || 0));
  const labelTopN = Math.max(8, Math.ceil(focusedNodes.length * 0.03));
  const labelAlwaysVisible = new Set(sortedByDeg.slice(0, labelTopN).map(n => n.id));

  // Position cache (lenient): if ANY node has a cached (x,y), use a
  // preset layout — instant render. Nodes without positions get
  // scattered near the cached cluster's bounding box so they don't
  // all pile up at (0,0). Admin's "Recompute layout" sets
  // _abForceFreshLayout to bypass the cache for one full fcose pass.
  const positionedNodes = focusedNodes.filter(n => Number.isFinite(n.x) && Number.isFinite(n.y));
  const useCachedPositions = !_abForceFreshLayout && positionedNodes.length > 0;
  // Refinement mode: user clicked "Recompute" AND there are existing
  // positions to start from. fcose can run much cheaper here — we
  // skip randomize, use draft quality, fewer iterations. It's a
  // re-fit on top of the saved layout, not a fresh discovery. Without
  // this, a Recompute on 2000+ nodes locks the main thread for tens
  // of seconds and trips the browser's "page unresponsive" dialog.
  const isRefinement = _abForceFreshLayout && positionedNodes.length > 0;
  _abForceFreshLayout = false;
  // Bounding box of the cached positions; new nodes get random spots
  // inside it so the graph stays coherent after a partial worker run.
  let scatterMinX = -500, scatterMaxX = 500, scatterMinY = -500, scatterMaxY = 500;
  if (useCachedPositions && positionedNodes.length) {
    scatterMinX = Math.min(...positionedNodes.map(n => n.x));
    scatterMaxX = Math.max(...positionedNodes.map(n => n.x));
    scatterMinY = Math.min(...positionedNodes.map(n => n.y));
    scatterMaxY = Math.max(...positionedNodes.map(n => n.y));
  }
  const randXY = () => ({
    x: scatterMinX + Math.random() * (scatterMaxX - scatterMinX),
    y: scatterMinY + Math.random() * (scatterMaxY - scatterMinY),
  });
  const elements = [
    ...focusedNodes.map(n => {
      const deg = degMap.get(n.id) || 0;
      const cid = clusterMap.get(n.id) || 0;
      const el = {
        data: {
          id: String(n.id),
          label: n.name,
          focused: n.id === _abFocusId ? 1 : 0,
          degree: deg,
          // Sizing math kept here so the stylesheet's mapData range
          // gets a stable max-degree cap regardless of graph size.
          eraColor: eraColor(n.seed_year),
          clusterColor: CLUSTER_HUES[cid % CLUSTER_HUES.length],
          alwaysLabel: labelAlwaysVisible.has(n.id) ? 1 : 0,
          seedYear: Number.isFinite(n.seed_year) ? n.seed_year : null,
        },
      };
      if (Number.isFinite(n.x) && Number.isFinite(n.y)) {
        el.position = { x: n.x, y: n.y };
      } else if (useCachedPositions) {
        // Cache mode: unpositioned new node gets a random spot inside
        // the cached cluster bounds so it doesn't pile up at (0,0).
        // User can hit Recompute layout to fold it in properly.
        el.position = randXY();
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
          // rawKinds carries the underlying worker kinds for
          // client-side kind-chip filtering. The bucket lookup
          // happens once at filter time, not per render.
          rawKinds: kinds.join(","),
          kinds: kinds.join(","),
          gradientColors: grad.colors,
          gradientPositions: grad.positions,
        },
      };
    }),
  ];
  _abLastLayoutWasCached = useCachedPositions;
  _abUnpositionedCount = focusedNodes.length - positionedNodes.length;
  _abTotalNodeCount = focusedNodes.length;
  // Surface what's happening in the toolbar so the user can tell
  // whether the slow path (fcose) is running and how much of the
  // graph is cached.
  if (counts) {
    const pct = focusedNodes.length
      ? Math.round((positionedNodes.length / focusedNodes.length) * 100)
      : 0;
    const layoutNote = useCachedPositions
      ? ` · cached layout (${pct}% positioned${pct < 100 ? ", new nodes scattered" : ""})`
      : isRefinement
        ? ` · fcose refinement (${pct}% prior positions, refit ~5s)`
        : ` · fresh fcose layout (slow first render)`;
    counts.textContent += layoutNote;
  }

  if (_abCy) { try { _abCy.destroy(); } catch {} _abCy = null; }
  _abCy = window._abCy = window.cytoscape({
    container: el,
    elements,
    style: [
      { selector: "node", style: {
        // Fill color now comes from the era gradient (blue→orange by
        // seed_year). Cluster id drives the border color so each
        // connected sub-graph has a subtle visual tint.
        "background-color": "data(eraColor)",
        "border-color": "data(clusterColor)", "border-width": 1.5,
        "label": "data(label)", "color": "#e2e8f0",
        "font-size": 10,
        // Label sits below the node with a thick dark outline AND a
        // semi-transparent dark pill behind it.
        "text-valign": "bottom", "text-halign": "center",
        "text-margin-y": 5,
        "text-outline-width": 3, "text-outline-color": "#000", "text-outline-opacity": 1,
        "text-background-color": "#000", "text-background-opacity": 0.55,
        "text-background-padding": 2, "text-background-shape": "round-rectangle",
        "text-wrap": "ellipsis", "text-max-width": 75,
        // Sizing by degree: bigger spread so hubs visually dominate.
        // 1 connection → 14px, max-degree → 80px. The eye finds the
        // cores first, periphery recedes.
        "width":  `mapData(degree, 0, ${maxDeg}, 14, 80)`,
        "height": `mapData(degree, 0, ${maxDeg}, 14, 80)`,
        // Layer by connection count: well-connected hubs render above
        // sparsely-connected nodes so labels of important artists win
        // overlap fights. Scale 2..14 nests cleanly under the class
        // overrides (faded=1, highlighted=20, source=30).
        "z-index": `mapData(degree, 0, ${maxDeg}, 2, 14)`,
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
      // Make faded EDGES un-tappable so a click on a bold focused
      // line never accidentally opens a connection popup for a dim
      // edge that happens to cross under it. Nodes stay tappable so
      // the user can pivot focus to a neighbor in one click.
      { selector: "edge.ab-faded", style: {
        "events": "no",
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
    layout: useCachedPositions
      ? { name: "preset", animate: false, fit: true, padding: 40 }
      : (window.cytoscapeFcose
        ? (isRefinement
          // REFINEMENT pass — existing positions are the starting point.
          // Draft quality + ~1/4 the iterations + randomize:false keeps
          // the recompute under a few seconds on 2000+ nodes instead of
          // locking the tab. Same force constants so the steady-state
          // looks consistent with a full proof run.
          ? {
              name: "fcose",
              quality: "draft",
              animate: false,
              nodeDimensionsIncludeLabels: true,
              nodeRepulsion: () => 90000,
              idealEdgeLength: () => 280,
              edgeElasticity: () => 0.25,
              nodeSeparation: 180,
              gravity: 0.08,
              gravityRange: 5,
              numIter: 1500,
              tile: true, packComponents: true,
              tilingPaddingVertical: 60, tilingPaddingHorizontal: 60,
              randomize: false,
              incremental: true,
            }
          // COLD pass — no existing layout to refine. Dialed-down
          // strength so a Recompute on 5000+ nodes finishes in
          // seconds, not minutes. quality:"default" + ~2500 iters is
          // plenty given that applyChrono() reshapes the result into
          // a horizontal-chronological band anyway.
          : {
              name: "fcose",
              quality: "default",
              animate: false,
              nodeDimensionsIncludeLabels: true,
              nodeRepulsion: () => 90000,
              idealEdgeLength: () => 280,
              edgeElasticity: () => 0.25,
              nodeSeparation: 180,
              gravity: 0.08,
              gravityRange: 5,
              numIter: 2500,
              tile: true, packComponents: true,
              tilingPaddingVertical: 60, tilingPaddingHorizontal: 60,
              randomize: true,
            })
        : {
            name: "cose",
            animate: false,
            nodeDimensionsIncludeLabels: true,
            nodeRepulsion: 200000, idealEdgeLength: 320,
            gravity: 0.08,
            numIter: isRefinement ? 1500 : 4000,
            randomize: !isRefinement,
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
    // Pulse for visual lock-on, highlight the artist's web, open the
    // profile directly. The profile's name link surfaces all the
    // search options (Search…), so no intermediate action menu needed.
    _abPulseNode(n);
    _abHighlightNode(n);
    const name = n.data("label") || `Artist ${id}`;
    _abOpenArtistProfile(id, name);
  });
  // Tap the background (anywhere other than a node or edge) to clear
  // the highlight. evt.target === cy means the tap hit the canvas.
  _abCy.on("tap", (evt) => {
    if (evt.target === _abCy) _abClearHighlight();
  });
  // Adaptive labels: at low zoom only the top-degree nodes keep their
  // label visible. Threshold + label set are precomputed above per
  // render; the handler just toggles a class on the cy root that the
  // stylesheet keys off.
  // Every label visible at every zoom — user explicitly asked to see
  // all names. Overlap is solved by spreading nodes out farther in
  // applyChrono() and starting the user at a readable zoom instead.
  const _syncLabelVisibility = () => {
    _abCy.nodes().forEach(n => {
      n.style("text-opacity", 1);
      n.style("text-background-opacity", 0.55);
    });
  };
  _abCy.on("zoom", _syncLabelVisibility);
  window._abSyncLabelVisibility = _syncLabelVisibility;
  _syncLabelVisibility();
  // Chronological bias — dominant left-to-right ordering on x,
  // compressed y so the network forms a horizontal rectangle (much
  // wider than tall). Always on, applied AFTER the layout settles.
  // 85% chronological + 15% original x preserves enough of the
  // force-directed cluster structure to make spatial sense.
  // Direction: older artists (low seed_year) land on the LEFT,
  // newer artists (high seed_year) land on the RIGHT.
  const applyChrono = () => {
    if (!_abCy) return;
    const yearVals = focusedNodes
      .map(n => Number(n.seed_year))
      .filter(v => Number.isFinite(v));
    if (yearVals.length < 2) return;
    const minYr = Math.min(...yearVals);
    const maxYr = Math.max(...yearVals);
    if (minYr === maxYr) return;
    // ── Massive spread so every label has room to breathe ─────────
    // User wants every name readable at native zoom. To make that work
    // without overlap we abandon "fit to canvas" sizing — instead we
    // budget a real label footprint per node and let the layout grow
    // as big as the data needs. User pans/zooms the giant canvas.
    const NODE_X_PX = 160; // ~2 label widths of horizontal slot per node
    const NODE_Y_PX = 55;  // line-height + padding per row
    const X_LANES = 4;     // four nodes side-by-side per year bin
    const yearById = new Map(focusedNodes.map(n => [n.id, Number(n.seed_year)]));
    const curYById = new Map();
    _abCy.nodes().forEach(n => {
      const id = parseInt(n.data("id"), 10);
      curYById.set(id, n.position().y);
    });
    // Group by integer seed_year so we get one column per year.
    const yearBins = new Map();
    focusedNodes.forEach(n => {
      const yr = yearById.get(n.id);
      if (!Number.isFinite(yr)) return;
      const k = Math.round(yr);
      if (!yearBins.has(k)) yearBins.set(k, []);
      yearBins.get(k).push(n.id);
    });
    // Size the canvas off the BUSIEST year so even the densest column
    // has room. Height = (rows in tallest column) * NODE_Y_PX.
    const maxColCount = Math.max(1, ...[...yearBins.values()].map(a => a.length));
    const rowsPerLane = Math.ceil(maxColCount / X_LANES);
    const targetYSpan = Math.max(800, rowsPerLane * NODE_Y_PX);
    const yearSpan = (maxYr - minYr) + 1;
    const targetXSpan = Math.max(2000, yearSpan * NODE_X_PX * X_LANES);
    const xL = -targetXSpan / 2;
    const xR =  targetXSpan / 2;
    const yT = -targetYSpan / 2;
    const yB =  targetYSpan / 2;
    const targetByNode = new Map();
    // Sort years ascending so the year-keyed bins lay out left → right.
    const sortedYears = [...yearBins.keys()].sort((a, b) => a - b);
    sortedYears.forEach(yr => {
      const ids = yearBins.get(yr);
      // Stable sort within a year by current Y so the fcose-derived
      // cluster structure carries through visually (people who clustered
      // near each other in y stay near each other in the column).
      ids.sort((a, b) => (curYById.get(a) || 0) - (curYById.get(b) || 0));
      const t = yearSpan > 1 ? (yr - minYr) / (yearSpan - 1) : 0.5;
      const colCenterX = xL + t * (xR - xL);
      ids.forEach((id, i) => {
        const lane = i % X_LANES;
        const row  = Math.floor(i / X_LANES);
        // Each lane is a fixed offset from the column center.
        const laneOffset = (lane - (X_LANES - 1) / 2) * NODE_X_PX;
        const rows = Math.max(1, Math.ceil(ids.length / X_LANES));
        const ty = rows > 1
          ? yT + (row / (rows - 1)) * (yB - yT)
          : (yT + yB) / 2;
        targetByNode.set(id, { x: colCenterX + laneOffset, y: ty });
      });
    });
    // Apply targets directly — no blending with fcose. We want strict
    // grid spacing so labels never collide. Edges still curve organically
    // because their geometry is computed from node positions at render.
    _abCy.nodes().forEach(n => {
      const id = parseInt(n.data("id"), 10);
      const target = targetByNode.get(id);
      if (!target) return;
      n.position(target);
    });
    // Don't fit() here — _abApplyFilters fits to the *visible* subset
    // and caps zoom so labels stay at native size.
  };
  applyChrono();
  // Bump the fit token so _abApplyFilters re-centers + caps zoom on
  // this fresh layout (subsequent slider/chip touches keep pan/zoom).
  _abFitToken++;
  // Apply the toolbar's current filter state (slider, kinds, min
  // degree) via visibility — no re-layout, no re-fetch.
  _abApplyFilters();
}
window.allBluesReload = allBluesReload;
window._abToggleKind = _abToggleKind;

// ── Visibility-only filter pass ────────────────────────────────────
// Reads the toolbar state (year window, kind buckets, min degree,
// focus) and applies CLASSES to the rendered cytoscape instance.
// No re-layout. No re-fetch. Used by the slider, the kind chips, and
// the min degree input — every filter touch is instant.
function _abApplyFilters() {
  if (!_abCy) return;
  const fromI = document.getElementById("ab-from-year-filter");
  const toI   = document.getElementById("ab-to-year-filter");
  const fromY = parseInt(fromI?.value || "1900", 10) || 1900;
  const toY   = parseInt(toI?.value   || "2100", 10) || 2100;
  const enabledBuckets = _abGetEnabledKinds();
  const enabledRaw = new Set();
  for (const b of enabledBuckets) {
    const def = _AB_KINDS.find(k => k.key === b);
    if (def) for (const r of def.raw) enabledRaw.add(r);
  }
  const minDeg = parseInt(document.getElementById("ab-min-degree")?.value || "1", 10) || 1;
  // Pass 1: nodes survive the year window
  const yearOK = (n) => {
    const yr = n.data("seedYear");
    if (!Number.isFinite(yr)) return true; // unknown year stays visible
    return yr >= fromY && yr <= toY;
  };
  // Pass 2: edges survive when at least one of their raw kinds is on
  // AND both endpoints survive the year window.
  const kindOK = (e) => {
    const list = (e.data("rawKinds") || "").split(",").filter(Boolean);
    if (!list.length) return enabledRaw.has("mention");
    return list.some(k => enabledRaw.has(k));
  };
  // Apply year + kind first, then min-degree on the surviving subgraph.
  const visibleNodes = new Set();
  _abCy.nodes().forEach(n => { if (yearOK(n)) visibleNodes.add(n.id()); });
  const liveEdges = [];
  _abCy.edges().forEach(e => {
    const src = e.source().id();
    const dst = e.target().id();
    const live = visibleNodes.has(src) && visibleNodes.has(dst) && kindOK(e);
    if (live) liveEdges.push(e);
  });
  // Min-degree pass — iterative prune. A single pass leaves orphans:
  // a node with 2 leaf neighbors survives the first check, but once
  // those leaves get kicked out the node is stranded with 0 visible
  // edges. Loop until no more nodes drop, so the visible subgraph
  // genuinely has min-degree ≥ minDeg.
  let finalNodes = visibleNodes;
  if (minDeg > 1) {
    finalNodes = new Set(visibleNodes);
    while (true) {
      const deg = new Map();
      for (const e of liveEdges) {
        const s = e.source().id(), d = e.target().id();
        if (!finalNodes.has(s) || !finalNodes.has(d)) continue;
        deg.set(s, (deg.get(s) || 0) + 1);
        deg.set(d, (deg.get(d) || 0) + 1);
      }
      const before = finalNodes.size;
      for (const id of [...finalNodes]) {
        if ((deg.get(id) || 0) < minDeg) finalNodes.delete(id);
      }
      if (finalNodes.size === before) break;
    }
  }
  // Focus / BFS — narrow to the reachable component.
  if (_abFocusId != null && finalNodes.has(String(_abFocusId))) {
    const adj = new Map();
    for (const id of finalNodes) adj.set(id, []);
    for (const e of liveEdges) {
      const s = e.source().id(), d = e.target().id();
      if (adj.has(s) && adj.has(d)) {
        adj.get(s).push(d);
        adj.get(d).push(s);
      }
    }
    const reach = new Set([String(_abFocusId)]);
    const queue = [String(_abFocusId)];
    while (queue.length) {
      const cur = queue.shift();
      for (const nb of (adj.get(cur) || [])) {
        if (!reach.has(nb)) { reach.add(nb); queue.push(nb); }
      }
    }
    finalNodes = new Set([...finalNodes].filter(id => reach.has(id)));
  }
  // Stamp visibility via inline cytoscape style — display:none for
  // hidden, default for visible. Cytoscape culls display:none from
  // hit-testing and rendering, so this stays fast even with 8000
  // nodes when most are hidden.
  let visN = 0, visE = 0;
  _abCy.nodes().forEach(n => {
    const on = finalNodes.has(n.id());
    n.style("display", on ? "element" : "none");
    if (on) visN++;
  });
  _abCy.edges().forEach(e => {
    const on = finalNodes.has(e.source().id())
            && finalNodes.has(e.target().id())
            && kindOK(e);
    e.style("display", on ? "element" : "none");
    if (on) visE++;
  });
  // Update the counts label with what's visible vs total.
  const counts = document.getElementById("ab-graph-counts");
  if (counts && _abLastGraph) {
    const stamp = _abLastFetchedAt
      ? _abLastFetchedAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
      : "";
    const total = _abLastGraph.nodes?.length ?? 0;
    const totalE = _abLastGraph.edges?.length ?? 0;
    counts.textContent = `· ${visN.toLocaleString()} of ${total.toLocaleString()} artists, ${visE.toLocaleString()} of ${totalE.toLocaleString()} links${stamp ? ` · fetched ${stamp}` : ""}`;
  }
  // Fit camera to *visible* nodes, then cap zoom so labels stay at
  // native size. The grid layout is enormous (every label gets its own
  // slot) so a default cytoscape fit() zooms way out — labels become
  // pixel dust. Cap at 1.0 and let the user pan.
  if (_abLastFitToken !== _abFitToken) {
    const vis = _abCy.nodes(":visible");
    if (vis.length) {
      _abCy.fit(vis, 60);
      if (_abCy.zoom() > 1.0) _abCy.zoom(1.0);
      _abCy.center(vis);
    }
    _abLastFitToken = _abFitToken;
  }
}
window._abApplyFilters = _abApplyFilters;

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
  if (_abCy) _abApplyFilters(); else allBluesReload();
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
  if (_abCy) _abApplyFilters(); else allBluesReload();
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
  // Save shows when there's anything new worth persisting:
  //   • A fresh fcose just ran (everything has new positions), OR
  //   • Some visible nodes came in without saved positions (worker
  //     added new seeds; they got scattered + a fcose recompute would
  //     give them proper coords worth saving).
  // Recompute is always available for admin so they can re-fit after
  // any worker run.
  const hasUnpositioned = _abUnpositionedCount > 0;
  const showSave = _abCy && (!_abLastLayoutWasCached || hasUnpositioned);
  save.style.display = showSave ? "" : "none";
  if (showSave) {
    save.title = hasUnpositioned
      ? `${_abUnpositionedCount} of ${_abTotalNodeCount} nodes don't have saved positions yet — Save (or Recompute → Save) to fix.`
      : "Save the current fcose layout so future loads use it directly.";
  }
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
    // Admin endpoint needs the Clerk session token — use apiFetch
    // (from shared.js), which attaches the auth header. Plain fetch
    // skips auth and returns 401.
    const fetcher = window.apiFetch || fetch;
    const r = await fetcher("/api/admin/all-blues/positions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ positions }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    showToast?.(`Saved positions for ${j.updated} artists`, "success");
    // The current render is already what was saved — flip the cached
    // flag and zero the unpositioned counter so the Save button hides
    // until either fcose runs again or a Reload brings in more
    // unpositioned worker-added nodes.
    _abLastLayoutWasCached = true;
    _abUnpositionedCount = 0;
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

// Window is locked to 11 years (slider value = start, to = start + 10).
// Default 1930–1940. The year inputs are hidden in the DOM; the slider
// is the only user control. _abYearInputChanged stays exposed for the
// localStorage restore path so existing state migrates cleanly.
const _AB_WINDOW_SPAN = 10; // years between from and to → 11 distinct years
const _AB_DEFAULT_START = 1930;

function _abYearInputChanged() {
  // Inputs are hidden; this only fires now from the restore-from-
  // localStorage path. Normalize whatever's in them to the locked
  // 11-year width starting at the stored "from".
  const fromI = document.getElementById("ab-from-year-filter");
  const toI   = document.getElementById("ab-to-year-filter");
  if (!fromI || !toI) return;
  let from = parseInt(fromI.value, 10);
  if (!Number.isFinite(from)) from = _AB_DEFAULT_START;
  from = Math.max(1900, Math.min(2010, from));
  const to = from + _AB_WINDOW_SPAN;
  fromI.value = from;
  toI.value   = to;
  try { localStorage.setItem(_AB_YEARS_KEY, JSON.stringify({ from, to })); } catch {}
  _abSyncWindowSlider();
  if (_abCy) _abApplyFilters(); else allBluesReload();
}
window._abYearInputChanged = _abYearInputChanged;

// Slider thumb position = start year. Window end = start + 10, giving
// an inclusive 11-year span (e.g. start=1930 → "1930–1940").
function _abWindowSliderInput() {
  const slider = document.getElementById("ab-window-slider");
  const label  = document.getElementById("ab-window-label");
  const fromI  = document.getElementById("ab-from-year-filter");
  const toI    = document.getElementById("ab-to-year-filter");
  if (!slider || !fromI || !toI) return;
  const start = parseInt(slider.value, 10);
  if (!Number.isFinite(start)) return;
  const end = start + _AB_WINDOW_SPAN;
  fromI.value = start;
  toI.value   = end;
  if (label) label.textContent = `${start}–${end}`;
  try { localStorage.setItem(_AB_YEARS_KEY, JSON.stringify({ from: start, to: end })); } catch {}
  // The slider must NEVER re-fetch or re-layout — it only dictates
  // which slice of the already-rendered network is visible. Cytoscape
  // visibility toggle is fast even on 8000+ nodes.
  if (_abCy) _abApplyFilters(); else allBluesReload();
}
window._abWindowSliderInput = _abWindowSliderInput;

// Push the current From/To input state back onto the slider thumb +
// label. Called on init so the toolbar reflects whatever's in the
// hidden inputs (localStorage or HTML default).
function _abSyncWindowSlider() {
  const slider = document.getElementById("ab-window-slider");
  const label  = document.getElementById("ab-window-label");
  const fromI  = document.getElementById("ab-from-year-filter");
  const toI    = document.getElementById("ab-to-year-filter");
  if (!slider || !fromI || !toI) return;
  const from = parseInt(fromI.value, 10);
  if (!Number.isFinite(from)) return;
  const clamped = Math.max(Number(slider.min), Math.min(Number(slider.max), from));
  slider.value = clamped;
  // Always snap to the locked window width so label and inputs agree
  // even if the stored 'to' got nudged by an older custom-range run.
  const to = clamped + _AB_WINDOW_SPAN;
  toI.value = to;
  if (label) label.textContent = `${clamped}–${to}`;
}

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
  // Move popup + backdrop to the END of <body> so it stacks on top
  // of any other popups already open. Both popup elements share the
  // same z-index, so document order is what decides which one wins.
  // appendChild on an existing child MOVES it to the end.
  if (bd) { document.body.appendChild(bd); bd.style.display = "block"; }
  document.body.appendChild(el);
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
        </div>
      </div>`;
  };
  const edgeKindRows = data.edges.length
    ? data.edges.map(e => {
      // Use bucket label + color so the per-edge row matches the
      // chips in the toolbar (e.g. "Pseudonyms" cyan, not "ALIAS"
      // grey). Raw kind still drives src/dst direction text below.
      const b = _abBucketFor(e.kind);
      const color = b.color;
      const direction = e.src_id === data.src.id
        ? `<span style="color:var(--muted)">${_abEsc(data.src.name)} →</span> ${_abEsc(data.dst.name)}`
        : `<span style="color:var(--muted)">${_abEsc(data.dst.name)} →</span> ${_abEsc(data.src.name)}`;
      return `
        <div style="padding:0.4rem 0.5rem;border-left:3px solid ${color};margin-bottom:0.35rem;background:rgba(255,255,255,0.03);border-radius:4px">
          <div style="display:flex;align-items:baseline;gap:0.4rem;margin-bottom:0.15rem">
            <span style="background:${color};color:#000;padding:0.08rem 0.5rem;border-radius:999px;font-size:0.68rem;font-weight:600">${_abEsc(b.label)}</span>
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
  // Force labels on for the focused web so they're readable at any
  // zoom — sync handles the hub set + zoom threshold + highlight set.
  window._abSyncLabelVisibility?.();
}

function _abClearHighlight() {
  if (!_abCy) return;
  _abCy.elements().removeClass("ab-highlighted ab-source ab-faded");
  window._abSyncLabelVisibility?.();
}
window._abClearHighlight = _abClearHighlight;

// Brief size pulse on the tapped node so the eye locks on. Reads the
// node's current width (varies by degree mapData), pulses ~50% larger
// for 180ms, then settles back. Cytoscape's animate() handles the
// interpolation; we re-assert the original size on complete so the
// stylesheet's mapData still drives the steady state.
function _abPulseNode(node) {
  if (!node || !node.animate) return;
  const baseW = node.width();
  const baseH = node.height();
  try {
    node.animate(
      { style: { "width": baseW * 1.55, "height": baseH * 1.55 } },
      { duration: 180, easing: "ease-out-quad",
        complete: () => {
          try {
            node.animate(
              { style: { "width": baseW, "height": baseH } },
              { duration: 240, easing: "ease-in-quad" },
            );
          } catch {}
        },
      },
    );
  } catch {}
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
  // Same trick as the edge popup — move both to the end of <body>
  // so this popup ends up on top of any earlier-opened ones.
  if (bd) { document.body.appendChild(bd); bd.style.display = "block"; }
  document.body.appendChild(el);
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
        // Bucket the raw worker kinds (band/alias/family/spouse/...)
        // into the same 4 user-facing chips shown in the toolbar so
        // a connection pill says "Pseudonyms" + cyan, not raw "ALIAS"
        // + grey fallback.
        const seen = new Set();
        const chips = c.kinds.map(k => {
          const b = _abBucketFor(k);
          if (seen.has(b.label)) return "";
          seen.add(b.label);
          return `<span style="background:${b.color};color:#000;padding:0.08rem 0.5rem;border-radius:999px;font-size:0.66rem;font-weight:600;margin-right:0.25rem">${_abEsc(b.label)}</span>`;
        }).join("");
        return `
          <div onclick="event.stopPropagation();_abOpenEdgePopup(${a.id}, ${c.partner_id})"
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
