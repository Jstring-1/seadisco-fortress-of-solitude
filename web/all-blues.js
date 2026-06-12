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
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
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
  // Admin controls only show if window._isAdmin
  const ctrl = document.getElementById("all-blues-controls");
  if (ctrl) ctrl.style.display = window._isAdmin ? "" : "none";
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
    if (window._isAdmin) await allBluesRefreshStatus();
    await allBluesReload();
  } else {
    if (window._isAdmin) allBluesRefreshStatus().catch(() => {});
  }
}
window.initAllBluesView = initAllBluesView;

// ── Admin worker controls ─────────────────────────────────────────

async function allBluesRefreshStatus() {
  try {
    const r = await fetch("/api/admin/all-blues/status");
    if (!r.ok) return;
    const s = await r.json();
    const startBtn = document.getElementById("ab-start");
    const stopBtn  = document.getElementById("ab-stop");
    if (startBtn) startBtn.style.display = s.running ? "none" : "";
    if (stopBtn)  stopBtn.style.display  = s.running ? "" : "none";
    const out = document.getElementById("ab-status");
    if (out) {
      const lines = [];
      lines.push(`running: ${s.running ? "yes" : "no"}`);
      if (s.state?.phase)    lines.push(`phase: ${s.state.phase}`);
      lines.push(`queue: pending=${s.queue?.pending ?? 0}  done=${s.queue?.done ?? 0}  error=${s.queue?.error ?? 0}`);
      lines.push(`cached artists: ${s.cached_artists}`);
      const kinds = s.links_by_kind || {};
      const total = Object.values(kinds).reduce((a, b) => a + b, 0);
      lines.push(`links: total=${total}  ` + Object.entries(kinds).map(([k, v]) => `${k}=${v}`).join("  "));
      if (s.state?.last_error) lines.push(`last error: ${s.state.last_error}`);
      out.textContent = lines.join("\n");
    }
  } catch (err) {
    console.error("[all-blues] status fetch failed:", err);
  }
}
window.allBluesRefreshStatus = allBluesRefreshStatus;

async function allBluesStart() {
  const from = parseInt(document.getElementById("ab-from-year")?.value || "1900", 10);
  const to   = parseInt(document.getElementById("ab-to-year")?.value   || "1970", 10);
  const reset = !!document.getElementById("ab-reset-queue")?.checked;
  try {
    const r = await fetch("/api/admin/all-blues/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fromYear: from, toYear: to, resetQueue: reset }),
    });
    const j = await r.json();
    if (!r.ok) { showToast(j.error || "could not start", "error"); return; }
    showToast("All Blues worker started", "success");
    setTimeout(allBluesRefreshStatus, 800);
  } catch (err) { showToast("Start failed: " + err.message, "error"); }
}
window.allBluesStart = allBluesStart;

async function allBluesStop() {
  try {
    await fetch("/api/admin/all-blues/stop", { method: "POST" });
    showToast("Stop requested", "info");
    setTimeout(allBluesRefreshStatus, 800);
  } catch (err) { showToast("Stop failed: " + err.message, "error"); }
}
window.allBluesStop = allBluesStop;

async function allBluesForceClear() {
  if (!confirm("Force-clear the in-memory running flag? Only use if the worker is wedged.")) return;
  try {
    await fetch("/api/admin/all-blues/force-clear", { method: "POST" });
    showToast("Force-cleared", "info");
    setTimeout(allBluesRefreshStatus, 400);
  } catch (err) { showToast("Force-clear failed: " + err.message, "error"); }
}
window.allBluesForceClear = allBluesForceClear;

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
    el.innerHTML = `<div style="padding:1rem;color:var(--muted)">No data yet. ${window._isAdmin ? "Click Start above to begin warming." : ""}</div>`;
    return;
  }
  const ok = await _abEnsureCytoscape();
  if (!ok) { el.innerHTML = `<div style="padding:1rem;color:#c66">Cytoscape failed to load</div>`; return; }
  el.innerHTML = "";

  const kindColor = Object.fromEntries(_AB_KINDS.map(k => [k.key, k.color]));
  const elements = [
    ...focusedNodes.map(n => ({ data: { id: String(n.id), label: n.name, focused: n.id === _abFocusId ? 1 : 0 } })),
    ...focusedEdges.map((e, i) => ({
      data: {
        id: `e${i}`,
        source: String(e.src_id),
        target: String(e.dst_id),
        kind: e.kind,
        color: kindColor[e.kind] || "#888",
      },
    })),
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
        "font-size": 10, "text-valign": "center", "text-halign": "center",
        "text-wrap": "ellipsis", "text-max-width": 90,
        "width": 22, "height": 22,
      }},
      { selector: "edge", style: {
        "line-color": "data(color)", "width": 2.5, "opacity": 0.95,
        "curve-style": "bezier",
      }},
      { selector: "node:selected", style: { "border-color": "#fbbf24", "border-width": 3 }},
      { selector: "node[focused = 1]", style: {
        "background-color": "#fbbf24",
        "border-color": "#fbbf24", "border-width": 3,
        "width": 30, "height": 30, "color": "#000", "font-weight": "bold",
      }},
    ],
    layout: window.cytoscapeFcose
      ? { name: "fcose", quality: "proof", animate: false, nodeRepulsion: 8000, idealEdgeLength: 90 }
      : { name: "cose", animate: false, nodeRepulsion: 60000, idealEdgeLength: 220 },
    wheelSensitivity: 0.2,
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
