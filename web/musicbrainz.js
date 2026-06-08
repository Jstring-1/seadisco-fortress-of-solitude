// ── MusicBrainz view ────────────────────────────────────────────────
//
// Admin-only discovery sub-view that proxies MusicBrainz entity
// search through /api/musicbrainz/*. The server side handles rate
// limiting + caching so we just render the response.
//
// State:
//   _mbEntity   — current entity type (artist / release / recording /
//                 release-group / work / label)
//   _mbTab      — "search" | "recent"
//   _mbLastQs   — last query string sent (drives pagination)
//   _mbPage     — 1-based page number for paginated search
//
// Persisted via _sdSaveViewState("musicbrainz", …) so bouncing to
// another view and back restores the form + results.

let _mbEntity = "artist";
let _mbTab    = "search";
let _mbPage   = 1;
const _MB_PER_PAGE = 25;
let _mbLastQs = "";
let _mbLastTotal = 0;

function initMusicbrainzView() {
  // Wire entity dropdown change → toggle which advanced fields show.
  _mbToggleEntityFilters();
  // Restore form state from per-view persistence so a switchView
  // round-trip drops the user back into their last search. Skip when
  // the form already has a value typed this session (we never
  // clobber).
  try {
    const qEl = document.getElementById("mb-q");
    const filled = qEl?.value || _mbAnyAdvancedFilled();
    if (!filled && typeof window._sdReadViewState === "function") {
      const sav = window._sdReadViewState("musicbrainz");
      if (sav) {
        if (sav.entity) {
          _mbEntity = sav.entity;
          const sel = document.getElementById("mb-entity");
          if (sel) sel.value = sav.entity;
        }
        for (const k of ["q","artist","release","recording","label","country","year","tag","type"]) {
          const el = document.getElementById(`mb-${k}`);
          if (el && sav[k] != null) el.value = String(sav[k]);
        }
        _mbToggleEntityFilters();
        if (sav.tab === "recent") { _mbSwitchTab("recent"); return; }
        // Defer search slightly so DOM settles.
        if (sav.q || _mbAnyAdvancedFilled()) {
          setTimeout(() => { try { _mbRunSearch(); } catch {} }, 0);
        }
      }
    }
  } catch {}
}
window.initMusicbrainzView = initMusicbrainzView;

function _mbAnyAdvancedFilled() {
  for (const k of ["artist","release","recording","label","country","year","tag","type"]) {
    const v = document.getElementById(`mb-${k}`)?.value?.trim();
    if (v) return true;
  }
  return false;
}

// Some advanced fields don't apply to every entity type (e.g.
// searching `release` already takes the release name in the title
// slot, so the "release" advanced field is a finer-grained filter
// that only makes sense when searching a *recording*). Toggle row
// visibility to keep the form honest.
function _mbToggleEntityFilters() {
  const sel = document.getElementById("mb-entity");
  if (sel) _mbEntity = sel.value;
  // Which advanced fields apply per entity type. Conservative: when
  // in doubt we keep a filter visible — MB just ignores irrelevant
  // ones.
  const allowByEntity = {
    "artist":        new Set(["country", "tag", "type"]),
    "release":       new Set(["artist", "release", "country", "year", "tag", "type", "label"]),
    "release-group": new Set(["artist", "release", "year", "tag", "type"]),
    "recording":     new Set(["artist", "recording", "year", "tag"]),
    "work":          new Set(["artist", "tag", "type"]),
    "label":         new Set(["label", "country", "tag", "type"]),
  };
  const allow = allowByEntity[_mbEntity] || new Set();
  document.querySelectorAll("#mb-advanced [data-mb-filter]").forEach(lbl => {
    const k = lbl.dataset.mbFilter;
    lbl.style.display = allow.has(k) ? "" : "none";
  });
}
window._mbToggleEntityFilters = _mbToggleEntityFilters;

function _mbSwitchTab(tab) {
  _mbTab = tab === "recent" ? "recent" : "search";
  document.querySelectorAll("#musicbrainz-tabs .loc-tab").forEach(b => {
    b.classList.toggle("active", b.dataset.mbTab === _mbTab);
  });
  const ps = document.querySelector(".musicbrainz-panel-search");
  const pr = document.querySelector(".musicbrainz-panel-recent");
  if (ps) ps.style.display = _mbTab === "search" ? "" : "none";
  if (pr) pr.style.display = _mbTab === "recent" ? "" : "none";
  if (_mbTab === "recent") _mbLoadRecent();
  _mbPersistState();
}
window._mbSwitchTab = _mbSwitchTab;

function _mbPersistState() {
  try {
    if (typeof window._sdSaveViewState !== "function") return;
    const state = {
      entity: _mbEntity,
      tab:    _mbTab,
      q:      document.getElementById("mb-q")?.value || "",
    };
    for (const k of ["artist","release","recording","label","country","year","tag","type"]) {
      const v = document.getElementById(`mb-${k}`)?.value || "";
      if (v) state[k] = v;
    }
    window._sdSaveViewState("musicbrainz", state);
  } catch {}
}

async function _mbRunSearch(opts) {
  const append = !!opts?.append;
  if (!append) _mbPage = 1;
  const entitySel = document.getElementById("mb-entity");
  if (entitySel) _mbEntity = entitySel.value;
  const params = new URLSearchParams();
  params.set("type", _mbEntity);
  const q = document.getElementById("mb-q")?.value?.trim() || "";
  if (q) params.set("q", q);
  for (const k of ["artist","release","recording","label","country","year","tag","type"]) {
    const v = document.getElementById(`mb-${k}`)?.value?.trim() || "";
    if (v) params.set(k, v);
  }
  if (!q && ![...params.keys()].some(k => k !== "type")) {
    document.getElementById("mb-results").innerHTML =
      `<div class="loc-empty">Type a query or fill an advanced filter to search MusicBrainz.</div>`;
    document.getElementById("mb-pagination").innerHTML = "";
    return;
  }
  params.set("limit",  String(_MB_PER_PAGE));
  params.set("offset", String((_mbPage - 1) * _MB_PER_PAGE));
  _mbLastQs = params.toString();
  _mbPersistState();

  const statusEl = document.getElementById("mb-status");
  const resultsEl = document.getElementById("mb-results");
  if (!append) {
    if (resultsEl) resultsEl.innerHTML = `<div class="loc-empty">Searching MusicBrainz…</div>`;
  }
  if (statusEl) statusEl.textContent = "";
  try {
    const r = await apiFetch(`/api/musicbrainz/search?${params}`);
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const j = await r.json();
    const key = _mbEntity === "release-group" ? "release-groups" : `${_mbEntity}s`;
    const rows = Array.isArray(j[key]) ? j[key] : [];
    _mbLastTotal = Number(j.count ?? j["release-count"] ?? j[`${_mbEntity}-count`] ?? rows.length) || rows.length;
    if (statusEl) {
      const src = j.source === "cache" ? "cached" : "live";
      statusEl.textContent = `${_mbLastTotal.toLocaleString()} result${_mbLastTotal === 1 ? "" : "s"} · ${src}`;
    }
    if (!rows.length) {
      if (!append && resultsEl) resultsEl.innerHTML = `<div class="loc-empty">No matches.</div>`;
      _mbRenderPagination();
      return;
    }
    const html = rows.map(r => _mbRowHtml(_mbEntity, r)).join("");
    if (append) resultsEl.insertAdjacentHTML("beforeend", html);
    else        resultsEl.innerHTML = html;
    _mbRenderPagination();
  } catch (e) {
    if (resultsEl) resultsEl.innerHTML = `<div class="loc-empty" style="color:#e88">Search failed: ${_mbEsc(String(e?.message || e))}</div>`;
    document.getElementById("mb-pagination").innerHTML = "";
  }
}
window._mbRunSearch = _mbRunSearch;

function _mbEsc(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// Per-entity row rendering. Keeps the same outer shape (clickable row
// → detail popup) so the user gets consistent affordances across MB
// entity types.
function _mbRowHtml(entity, r) {
  const mbid = String(r.id || "");
  const safeId = mbid.replace(/'/g, "");
  const name = r.title || r.name || "(untitled)";
  const score = r.score != null ? `<span style="color:var(--muted);font-size:0.74rem;margin-left:0.4rem">${Number(r.score).toFixed(0)}%</span>` : "";
  const sub = [];
  if (entity === "artist") {
    if (r.type)     sub.push(_mbEsc(r.type));
    if (r.country)  sub.push(_mbEsc(r.country));
    if (r["life-span"]?.begin || r["life-span"]?.end) {
      sub.push(`${r["life-span"]?.begin || ""}–${r["life-span"]?.ended ? (r["life-span"]?.end || "") : ""}`);
    }
    if (r.disambiguation) sub.push(`<em>${_mbEsc(r.disambiguation)}</em>`);
  } else if (entity === "release" || entity === "release-group") {
    const ac = (r["artist-credit"] || []).map(a => a.name || a.artist?.name).filter(Boolean).join(", ");
    if (ac)       sub.push(_mbEsc(ac));
    if (r.date)   sub.push(_mbEsc(r.date));
    if (r.country) sub.push(_mbEsc(r.country));
    if (r["primary-type"]) sub.push(_mbEsc(r["primary-type"]));
  } else if (entity === "recording") {
    const ac = (r["artist-credit"] || []).map(a => a.name || a.artist?.name).filter(Boolean).join(", ");
    if (ac)        sub.push(_mbEsc(ac));
    if (r.length)  sub.push(_mbFormatMs(r.length));
    if (r["first-release-date"]) sub.push(_mbEsc(r["first-release-date"]));
  } else if (entity === "work") {
    if (r.type)  sub.push(_mbEsc(r.type));
    if (r.iswcs?.length) sub.push("ISWC " + _mbEsc(r.iswcs[0]));
  } else if (entity === "label") {
    if (r.type)    sub.push(_mbEsc(r.type));
    if (r.country) sub.push(_mbEsc(r.country));
    if (r["life-span"]?.begin) sub.push(`since ${_mbEsc(r["life-span"].begin)}`);
  }
  const subHtml = sub.length ? `<div style="color:var(--muted);font-size:0.78rem">${sub.join(" · ")}</div>` : "";
  return `
    <div class="mb-row" data-mbid="${_mbEsc(mbid)}" onclick="_mbOpenDetail('${_mbEntity}','${safeId}')" style="padding:0.55rem 0.7rem;border-bottom:1px solid var(--border);cursor:pointer">
      <div style="font-weight:600">${_mbEsc(name)}${score}</div>
      ${subHtml}
    </div>
  `;
}

function _mbFormatMs(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n <= 0) return "";
  const s = Math.round(n / 1000);
  const m = Math.floor(s / 60);
  const r = s % 60;
  return `${m}:${String(r).padStart(2, "0")}`;
}

function _mbRenderPagination() {
  const pagEl = document.getElementById("mb-pagination");
  if (!pagEl) return;
  const totalPages = Math.max(1, Math.ceil(_mbLastTotal / _MB_PER_PAGE));
  if (totalPages <= 1) { pagEl.innerHTML = ""; return; }
  pagEl.innerHTML = `
    <button class="archive-btn" ${_mbPage <= 1 ? "disabled" : ""} onclick="_mbGoPage(${_mbPage - 1})">‹ Prev</button>
    <span style="font-size:0.78rem;color:var(--muted);padding:0 0.6rem">Page ${_mbPage} of ${totalPages}</span>
    <button class="archive-btn" ${_mbPage >= totalPages ? "disabled" : ""} onclick="_mbGoPage(${_mbPage + 1})">Next ›</button>
  `;
}

function _mbGoPage(p) {
  _mbPage = Math.max(1, p);
  _mbRunSearch();
}
window._mbGoPage = _mbGoPage;

// Detail overlay. Hits /api/musicbrainz/:type/:mbid (the server attaches
// the appropriate `inc=` defaults so we get linked entities for free)
// and renders a scrolling panel with the upstream JSON pretty-printed
// alongside a quick-link list (Wikipedia / Discogs / homepage from
// url-rels). Enough for the v1 — richer per-entity rendering can layer
// on top later without changing the data contract.
async function _mbOpenDetail(type, mbid) {
  let overlay = document.getElementById("mb-detail-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "mb-detail-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "300", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
    });
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1rem 1.2rem;width:min(900px,100%)">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">
        <h3 style="margin:0;font-size:1rem">Loading ${_mbEsc(type)}…</h3>
        <button class="archive-btn" onclick="document.getElementById('mb-detail-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <div id="mb-detail-body" style="font-size:0.84rem;color:var(--muted)">Fetching…</div>
    </div>
  `;
  try {
    const r = await apiFetch(`/api/musicbrainz/${encodeURIComponent(type)}/${encodeURIComponent(mbid)}`);
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const j = await r.json();
    const title = j.title || j.name || "(untitled)";
    const links = Array.isArray(j.relations) ? j.relations.filter(rel => rel.type === "wikipedia" || rel.type === "wikidata" || rel.type === "discogs" || rel.type === "official homepage" || rel.type === "allmusic") : [];
    const linksHtml = links.length ? `
      <div style="margin:0.4rem 0">
        ${links.map(rel => {
          const url = rel.url?.resource || "";
          if (!url) return "";
          return `<a href="${_mbEsc(url)}" target="_blank" rel="noopener" style="display:inline-block;margin-right:0.6rem;color:var(--accent);text-decoration:none">${_mbEsc(rel.type)} ↗</a>`;
        }).join("")}
      </div>` : "";
    overlay.querySelector("div > div h3").textContent = title;
    overlay.querySelector("#mb-detail-body").innerHTML = `
      <div style="color:var(--muted);font-size:0.78rem;margin-bottom:0.4rem">
        ${_mbEsc(type)} · <code style="color:var(--accent)">${_mbEsc(mbid)}</code>
        · <a href="https://musicbrainz.org/${_mbEsc(type)}/${_mbEsc(mbid)}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">Open on MusicBrainz ↗</a>
        ${j.source === "cache" ? `<span style="color:#7c7" title="Served from local cache">· cached</span>` : ""}
      </div>
      ${linksHtml}
      <pre style="background:rgba(255,255,255,0.03);border:1px solid var(--border);border-radius:4px;padding:0.8rem;max-height:60vh;overflow:auto;color:var(--text);font-size:0.78rem;white-space:pre-wrap">${_mbEsc(JSON.stringify(j, null, 2))}</pre>
    `;
  } catch (e) {
    overlay.querySelector("#mb-detail-body").innerHTML = `<div style="color:#e88">Load failed: ${_mbEsc(String(e?.message || e))}</div>`;
  }
}
window._mbOpenDetail = _mbOpenDetail;

// Recent tab placeholder. Future: list the most recently-cached MB
// entities (with link to re-open their detail). For now, just shows
// a note explaining what the tab will do.
async function _mbLoadRecent() {
  const el = document.getElementById("mb-recent");
  if (!el) return;
  el.innerHTML = `<div class="loc-empty">Recently-cached MB entities will land here. (Coming with the next iteration — for now switch back to Search to re-run a query.)</div>`;
}
