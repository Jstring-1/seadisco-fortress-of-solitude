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
  // `entity` is the MB entity selector (artist / release / …). The
  // `type` advanced field is a separate Lucene filter (release type =
  // Album / Single / Live, label type = Original Production, etc.) —
  // overloading the same param name into both slots made every
  // artist search collapse to zero because the server then injected
  // `type:artist` into the Lucene query, which MB's artist index has
  // no value for.
  params.set("entity", _mbEntity);
  const q = document.getElementById("mb-q")?.value?.trim() || "";
  if (q) params.set("q", q);
  for (const k of ["artist","release","recording","label","country","year","tag","type"]) {
    const v = document.getElementById(`mb-${k}`)?.value?.trim() || "";
    if (v) params.set(k, v);
  }
  if (!q && ![...params.keys()].some(k => k !== "entity")) {
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

// Attribute-safe escape: strips backslashes (so single-quotes can't be
// re-introduced via a stray escape) then HTML-escapes + replaces
// single quotes with &#39; so the value can be embedded inside an
// onclick='…' single-quoted string without breaking the JS literal.
function _mbAttr(s) {
  return String(s ?? "")
    .replace(/\\/g, "")
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// Per-entity row rendering. Keeps the same outer shape (clickable row
// → detail popup) so the user gets consistent affordances across MB
// entity types. The 🔍 icon at the front of the title routes through
// entityLookupLinkHtml so the user can pop the search-options menu
// (SeaDisco / Wikipedia / YouTube / Discogs / etc.) right from the
// results grid — without leaving the MB view.
function _mbRowHtml(entity, r) {
  const mbid = String(r.id || "");
  const safeId = mbid.replace(/'/g, "");
  const name = r.title || r.name || "(untitled)";
  const score = r.score != null ? `<span style="color:var(--muted);font-size:0.74rem;margin-left:0.4rem">${Number(r.score).toFixed(0)}%</span>` : "";
  // Map MB entity → entityLookupLinkHtml scope so the popup's
  // SeaDisco button fills the right form field on the resulting
  // search.
  const lookupScope = (entity === "artist" || entity === "label") ? entity
                    : (entity === "recording" || entity === "work")  ? "track"
                    : "release";
  // 🔍 lookup affordance — invokes openLookupPopup directly so the
  // displayed glyph stays a magnifier while the popup still searches
  // for `name`. Stops propagation so clicking the icon doesn't also
  // open the MB detail overlay.
  const lookupHtml = `<a href="#" class="mb-row-lookup-link" onclick="event.preventDefault();event.stopPropagation();openLookupPopup(event,'${lookupScope}','${_mbAttr(name)}');return false" title="Search options for &quot;${_mbAttr(name)}&quot;" style="margin-right:0.4rem;color:var(--muted);text-decoration:none">🔍</a>`;
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
      <div style="font-weight:600">${lookupHtml}${_mbEsc(name)}${score}</div>
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

// Detail overlay. Hits /api/musicbrainz/:type/:mbid (the server
// attaches per-entity `inc=` defaults so we get linked entities for
// free) and renders a Discogs-popup-styled panel: title + lookup
// affordance, meta line, external-link strip, and entity-specific
// content sections. Raw-JSON view is still available behind a toggle
// for the curator who wants to inspect MB fields verbatim.
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
    <div class="mb-detail-card" style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1rem 1.2rem;width:min(900px,100%)">
      <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.5rem">
        <div style="min-width:0;flex:1">
          <h3 class="mb-detail-title" style="margin:0 0 0.2rem;font-size:1.05rem">Loading…</h3>
          <div class="mb-detail-meta" style="color:var(--muted);font-size:0.78rem"></div>
        </div>
        <button class="archive-btn" onclick="document.getElementById('mb-detail-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
      </div>
      <div class="mb-detail-body" style="font-size:0.84rem;color:var(--text)">Fetching…</div>
    </div>
  `;
  try {
    const r = await apiFetch(`/api/musicbrainz/${encodeURIComponent(type)}/${encodeURIComponent(mbid)}`);
    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
    }
    const j = await r.json();
    _mbRenderDetail(overlay, type, mbid, j);
  } catch (e) {
    overlay.querySelector(".mb-detail-body").innerHTML =
      `<div style="color:#e88">Load failed: ${_mbEsc(String(e?.message || e))}</div>`;
  }
}
window._mbOpenDetail = _mbOpenDetail;

// Per-entity formatted detail. Mirrors the Discogs artist / release
// popup shape: title row + meta line + external links + content
// sections. Each entity-typed name gets routed through
// entityLookupLinkHtml so the user can pop the search-options menu
// (SeaDisco / Wikipedia / YouTube / etc.) right from this view.
function _mbRenderDetail(overlay, type, mbid, j) {
  const titleEl = overlay.querySelector(".mb-detail-title");
  const metaEl  = overlay.querySelector(".mb-detail-meta");
  const bodyEl  = overlay.querySelector(".mb-detail-body");
  const name = j.title || j.name || "(untitled)";

  // Title — wrapped in entityLookupLinkHtml so clicking the name
  // opens the search-options popup. Scope by entity type so the
  // resulting search routes through the right SeaDisco field.
  const scope = (type === "artist" || type === "label") ? type
              : (type === "recording" || type === "work")  ? "track"
              : "release";
  const titleHtml = (typeof entityLookupLinkHtml === "function")
    ? entityLookupLinkHtml(scope, name, { title: `Lookup options for "${name}"` })
    : _mbEsc(name);
  titleEl.innerHTML = titleHtml;
  if (j.disambiguation) {
    titleEl.innerHTML += ` <span style="color:var(--muted);font-size:0.8rem;font-style:italic;font-weight:normal">(${_mbEsc(j.disambiguation)})</span>`;
  }

  // Meta line — entity-specific (lifespan/country/type for artist;
  // date/country/format for release; etc.).
  const metaParts = [];
  metaParts.push(`<code style="color:var(--accent)">${_mbEsc(mbid)}</code>`);
  metaParts.push(`<a href="https://musicbrainz.org/${_mbEsc(type)}/${_mbEsc(mbid)}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">MusicBrainz ↗</a>`);
  if (type === "artist") {
    if (j.type)    metaParts.push(_mbEsc(j.type));
    if (j.gender)  metaParts.push(_mbEsc(j.gender));
    if (j.country) metaParts.push(_mbEsc(j.country));
    const ls = j["life-span"];
    if (ls?.begin || ls?.end) {
      metaParts.push(`${_mbEsc(ls?.begin || "?")} – ${_mbEsc(ls?.ended ? (ls?.end || "?") : "present")}`);
    }
  } else if (type === "release" || type === "release-group") {
    if (j.date)             metaParts.push(_mbEsc(j.date));
    if (j.country)          metaParts.push(_mbEsc(j.country));
    if (j.status)           metaParts.push(_mbEsc(j.status));
    if (j["primary-type"])  metaParts.push(_mbEsc(j["primary-type"]));
    if (Array.isArray(j["secondary-types"]) && j["secondary-types"].length) {
      metaParts.push(_mbEsc(j["secondary-types"].join(" / ")));
    }
    if (j.barcode)          metaParts.push("UPC " + _mbEsc(j.barcode));
  } else if (type === "recording") {
    if (j.length)               metaParts.push(_mbFormatMs(j.length));
    if (j["first-release-date"]) metaParts.push(_mbEsc(j["first-release-date"]));
    if (j.video)                metaParts.push("video");
  } else if (type === "label") {
    if (j.type)    metaParts.push(_mbEsc(j.type));
    if (j.country) metaParts.push(_mbEsc(j.country));
    if (j["label-code"]) metaParts.push("LC " + _mbEsc(j["label-code"]));
    const ls = j["life-span"];
    if (ls?.begin || ls?.end) metaParts.push(`${_mbEsc(ls?.begin || "?")} – ${_mbEsc(ls?.ended ? (ls?.end || "?") : "present")}`);
  } else if (type === "work") {
    if (j.type) metaParts.push(_mbEsc(j.type));
    if (Array.isArray(j.iswcs) && j.iswcs.length) metaParts.push("ISWC " + _mbEsc(j.iswcs[0]));
  }
  if (j.source === "cache") metaParts.push(`<span style="color:#7c7" title="Served from local cache">cached</span>`);
  metaEl.innerHTML = metaParts.join(' <span style="color:#555">·</span> ');

  // Body — assemble sections then drop them in.
  const sections = [];

  // Cross-link bar — when MB has a `discogs` URL relation we extract
  // the Discogs artist / label id and offer one-click bridges into
  // SeaDisco's search-by-id flow and (for artists) the Blues Archive
  // adder. Gives the curator a fast path from MB → already-known
  // SeaDisco state without copy-pasting ids.
  sections.push(_mbRenderCrossLinks(type, j));

  // External links from url-rels relations.
  sections.push(_mbRenderUrlRels(j));

  // Artist-credit byline (release / release-group / recording) — each
  // artist name is a separate lookup-popup link.
  if (type === "release" || type === "release-group" || type === "recording") {
    const ac = Array.isArray(j["artist-credit"]) ? j["artist-credit"] : [];
    if (ac.length) {
      const parts = ac.map(c => {
        const aName = c.name || c.artist?.name || "";
        const join  = c.joinphrase ?? c["joinphrase"] ?? "";
        const aid   = c.artist?.id;
        if (!aName) return "";
        let link = (typeof entityLookupLinkHtml === "function")
          ? entityLookupLinkHtml("artist", aName, { openId: aid, openType: aid ? "artist" : undefined, title: `Lookup options for "${aName}"` })
          : _mbEsc(aName);
        if (aid) {
          link += ` <a href="#" onclick="event.preventDefault();event.stopPropagation();_mbOpenDetail('artist','${aid.replace(/'/g, "")}')" title="Open this MB artist" style="color:var(--muted);text-decoration:none;font-size:0.78em">↗</a>`;
        }
        return `${link}${_mbEsc(join)}`;
      }).filter(Boolean).join("");
      if (parts) sections.push(`<div style="margin:0.4rem 0"><span style="color:var(--muted);font-size:0.78rem">by</span> ${parts}</div>`);
    }
  }

  // Tags (artist / release / recording / work / label) — chip strip.
  const tags = Array.isArray(j.tags) ? j.tags.slice(0, 25) : [];
  if (tags.length) {
    sections.push(`<div style="display:flex;gap:0.3rem;flex-wrap:wrap;margin:0.4rem 0">${
      tags.map(t => `<span style="padding:0.15rem 0.45rem;border:1px solid var(--border);border-radius:999px;font-size:0.7rem;color:var(--muted)">${_mbEsc(t.name || "")} <span style="color:#666">${Number(t.count) || ""}</span></span>`).join("")
    }</div>`);
  }

  // Aliases (artist / label).
  if (type === "artist" || type === "label") {
    const aliases = Array.isArray(j.aliases) ? j.aliases.filter(a => a.name && a.name !== name).slice(0, 12) : [];
    if (aliases.length) {
      sections.push(`<div style="margin:0.6rem 0;color:var(--muted);font-size:0.8rem"><span style="color:var(--accent)">Aliases:</span> ${aliases.map(a => _mbEsc(a.name)).join(", ")}</div>`);
    }
  }

  // Entity-specific lists.
  if (type === "artist") {
    sections.push(_mbRenderReleaseGroupsList(j["release-groups"] || []));
  } else if (type === "release-group") {
    sections.push(_mbRenderReleasesList(j.releases || []));
  } else if (type === "release") {
    sections.push(_mbRenderMediaTracklist(j.media || []));
  } else if (type === "recording") {
    sections.push(_mbRenderReleasesList(j.releases || [], "recording"));
  } else if (type === "label") {
    sections.push(_mbRenderReleasesList(j.releases || []));
  } else if (type === "work") {
    sections.push(_mbRenderWorkRels(j.relations || []));
  }

  // Raw JSON toggle — collapsed by default. The curator can expand to
  // verify what MB returned without us having to render every field.
  sections.push(`
    <details style="margin-top:1rem">
      <summary style="cursor:pointer;color:var(--muted);font-size:0.78rem">Raw JSON</summary>
      <pre style="background:rgba(255,255,255,0.03);border:1px solid var(--border);border-radius:4px;padding:0.8rem;max-height:40vh;overflow:auto;color:var(--text);font-size:0.74rem;white-space:pre-wrap;margin-top:0.4rem">${_mbEsc(JSON.stringify(j, null, 2))}</pre>
    </details>
  `);

  bodyEl.innerHTML = sections.filter(Boolean).join("\n");
}

// Cross-link strip — for entity types that map onto Discogs (artist /
// label), surface explicit buttons to (a) open the linked Discogs
// page, (b) search SeaDisco scoped to that entity ID (fast by-id
// path), and (c) — for artists — probe the Blues Archive and offer
// either Open or Add. Probes are debounced and only fire when the
// detail panel actually has a discogs URL relation to work with.
function _mbRenderCrossLinks(type, j) {
  if (type !== "artist" && type !== "label") return "";
  const rels = Array.isArray(j.relations) ? j.relations : [];
  const discogsRel = rels.find(r => r.type === "discogs" && r.url?.resource);
  if (!discogsRel) return "";
  const url = String(discogsRel.url?.resource || "");
  // URLs look like https://www.discogs.com/artist/12345 (or /label/N).
  // Extract the trailing numeric id; bail if the URL shape doesn't
  // match so we don't post a garbage id to /api/blues-archive/check.
  const m = url.match(/discogs\.com\/(artist|label)\/(\d+)/i);
  if (!m) return "";
  const kind  = m[1].toLowerCase();
  const dcId  = m[2];
  const name  = String(j.name || j.title || "");
  const safeName = name.replace(/'/g, "&#39;");
  // SeaDisco search-by-id — calls _lookupSearchSeaDisco's by-id path
  // for artist/label. Routes through the existing artist-releases /
  // label-releases endpoints so it's an exact match, not a name
  // substring search.
  const sdBtn = `<button type="button" class="archive-btn" onclick="_mbSearchSeaDiscoById('${kind}','${dcId}','${safeName}')" title="Search SeaDisco scoped to Discogs ${kind} id ${dcId} (exact match)">Open in SeaDisco</button>`;
  const dcBtn = `<a href="${_mbEsc(url)}" target="_blank" rel="noopener" class="archive-btn" style="text-decoration:none" title="Open this Discogs ${kind} page">Discogs ↗</a>`;
  let baBtn = "";
  if (kind === "artist") {
    // Blues Archive button — initially says "Add" with a question
    // mark; on click we either jump to the existing row or fire the
    // adder. Resolution happens lazily so we don't spam the check
    // endpoint on every detail open.
    baBtn = `<button type="button" class="archive-btn" id="mb-ba-btn-${dcId}" onclick="_mbResolveBluesArchive('${dcId}','${safeName}',this)" title="Probe the Blues Archive — open the row if it exists, else offer to add it">Blues Archive…</button>`;
  }
  return `<div style="display:flex;gap:0.4rem;flex-wrap:wrap;margin:0.6rem 0;padding:0.5rem;background:rgba(255,255,255,0.02);border:1px solid var(--border);border-radius:6px">${sdBtn}${dcBtn}${baBtn}</div>`;
}

// Imperative SeaDisco jump scoped to a Discogs id. Closes the MB
// overlay, hands the existing entity-lookup search routine the id +
// scope so it walks the same code path the popup's "Search SeaDisco"
// button uses (with all the field-pinning + master+ default behavior).
function _mbSearchSeaDiscoById(scope, dcId, name) {
  document.getElementById("mb-detail-overlay")?.remove();
  if (typeof window._lookupSearchSeaDisco === "function") {
    try { window._lookupSearchSeaDisco(scope, name, dcId); return; } catch {}
  }
  // Fallback if _lookupSearchSeaDisco isn't loaded for some reason:
  // build the deep-link URL ourselves. _sdRunPrefilledSearch is
  // defined by search.js and runs in-SPA so the mini-player stays.
  const qs = scope === "artist"
    ? { a: name, r: "master+", s: "year:asc" }
    : { l: name, r: "master+", s: "year:asc" };
  if (typeof window._sdRunPrefilledSearch === "function") {
    if (scope === "artist") window.currentArtistId = dcId; else window.currentLabelId = dcId;
    window._sdRunPrefilledSearch(qs);
  }
}
window._mbSearchSeaDiscoById = _mbSearchSeaDiscoById;

// Probe + branch for the Blues Archive button. Calls the same check
// endpoint the in-modal +blues icon uses; on a hit, jumps to the
// archive artist popup; on a miss, fires the existing add-by-id
// helper (which opens the editor with a pre-populated name + id).
async function _mbResolveBluesArchive(dcId, name, btnEl) {
  if (btnEl) { btnEl.disabled = true; btnEl.textContent = "Checking…"; }
  try {
    const r = await apiFetch("/api/blues-archive/check", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        artistIds:   [Number(dcId)],
        artistNames: name ? [name] : [],
      }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const result = await r.json();
    const hit = result.artistsById?.[String(dcId)]
             || result.artists?.[String(name || "").trim().toLowerCase()];
    if (hit && hit.id) {
      // Already in archive — close the MB overlay and open the
      // Blues Archive detail directly. switchView routing handles
      // lazy-loading blues-archive.js if it's not in memory yet.
      document.getElementById("mb-detail-overlay")?.remove();
      if (typeof window._baOpenArtistFromBadge === "function") {
        window._baOpenArtistFromBadge(hit.id);
      } else if (typeof switchView === "function") {
        switchView("blues-archive");
        setTimeout(() => window._baOpenArtistFromBadge?.(hit.id), 200);
      }
      return;
    }
    // Not in archive — offer to add. The existing _bluesAddArtist
    // helper opens the +blues add-by-id editor; we pass the MB-known
    // name as the seed.
    if (!confirm(`"${name}" isn't in the Blues Archive yet (Discogs id ${dcId}). Add now?`)) {
      if (btnEl) { btnEl.disabled = false; btnEl.textContent = "Blues Archive…"; }
      return;
    }
    if (typeof window._bluesAddArtist === "function") {
      window._bluesAddArtist(Number(dcId), name, btnEl);
    } else {
      alert("Blues Archive adder not loaded yet — switch to Discover → Blues Archive once, then retry.");
      if (btnEl) { btnEl.disabled = false; btnEl.textContent = "Blues Archive…"; }
    }
  } catch (err) {
    console.warn("[mb blues archive resolve]", err);
    if (btnEl) { btnEl.disabled = false; btnEl.textContent = "Blues Archive…"; }
  }
}
window._mbResolveBluesArchive = _mbResolveBluesArchive;

function _mbRenderUrlRels(j) {
  const rels = Array.isArray(j.relations) ? j.relations.filter(rel => rel["target-type"] === "url" || rel.url) : [];
  if (!rels.length) return "";
  // Sort: the well-known external sources first, then everything else.
  const priority = ["wikipedia", "wikidata", "discogs", "allmusic", "official homepage", "imdb", "bandcamp", "soundcloud"];
  rels.sort((a, b) => (priority.indexOf(a.type) + 1 || 99) - (priority.indexOf(b.type) + 1 || 99));
  const out = rels.map(rel => {
    const url = rel.url?.resource || "";
    if (!url) return "";
    return `<a href="${_mbEsc(url)}" target="_blank" rel="noopener" style="margin-right:0.7rem;color:var(--accent);text-decoration:none;font-size:0.82rem">${_mbEsc(rel.type)} ↗</a>`;
  }).filter(Boolean);
  if (!out.length) return "";
  return `<div style="margin:0.4rem 0">${out.join("")}</div>`;
}

function _mbRenderReleaseGroupsList(groups) {
  if (!groups.length) return "";
  const sorted = [...groups].sort((a, b) => {
    const aY = String(a["first-release-date"] || "").slice(0, 4);
    const bY = String(b["first-release-date"] || "").slice(0, 4);
    return aY.localeCompare(bY);
  });
  return `
    <h4 style="font-size:0.86rem;color:var(--accent);margin:1rem 0 0.4rem">Release groups (${sorted.length})</h4>
    <table class="api-log-table" style="font-size:0.82rem;width:100%">
      <thead><tr>
        <th style="text-align:left">Year</th>
        <th style="text-align:left">Title</th>
        <th style="text-align:left">Type</th>
      </tr></thead>
      <tbody>${sorted.map(g => {
        const yr   = String(g["first-release-date"] || "").slice(0, 4);
        const tp   = [g["primary-type"], ...(g["secondary-types"] || [])].filter(Boolean).join(" / ");
        const safeId = String(g.id || "").replace(/'/g, "");
        // Title cell carries TWO affordances side-by-side: 🔍 pops
        // the SeaDisco / Wikipedia / YouTube search-options menu for
        // this release name; clicking the title text walks into the
        // MB release-group detail (in-popup navigation).
        const gName = g.title || "(untitled)";
        const lookupHtml = `<a href="#" class="mb-row-lookup-link" onclick="event.preventDefault();event.stopPropagation();openLookupPopup(event,'release','${_mbAttr(gName)}');return false" title="Search options for &quot;${_mbAttr(gName)}&quot;" style="margin-right:0.3rem;color:var(--muted);text-decoration:none">🔍</a>`;
        const titleLink = `<a href="#" onclick="event.preventDefault();_mbOpenDetail('release-group','${safeId}')" style="color:var(--text);text-decoration:none;font-weight:600">${_mbEsc(gName)}</a>`;
        return `<tr>
          <td style="color:var(--muted);font-variant-numeric:tabular-nums">${_mbEsc(yr || "—")}</td>
          <td>${lookupHtml}${titleLink}</td>
          <td style="color:var(--muted);font-size:0.76rem">${_mbEsc(tp)}</td>
        </tr>`;
      }).join("")}</tbody>
    </table>
  `;
}

function _mbRenderReleasesList(releases, sourceType) {
  if (!releases.length) return "";
  const sorted = [...releases].sort((a, b) => String(a.date || "").localeCompare(String(b.date || "")));
  return `
    <h4 style="font-size:0.86rem;color:var(--accent);margin:1rem 0 0.4rem">Releases (${sorted.length})</h4>
    <table class="api-log-table" style="font-size:0.82rem;width:100%">
      <thead><tr>
        <th style="text-align:left">Date</th>
        <th style="text-align:left">Title</th>
        <th style="text-align:left">Country</th>
      </tr></thead>
      <tbody>${sorted.map(rel => {
        const safeId = String(rel.id || "").replace(/'/g, "");
        const rName = rel.title || "(untitled)";
        const lookupHtml = `<a href="#" class="mb-row-lookup-link" onclick="event.preventDefault();event.stopPropagation();openLookupPopup(event,'release','${_mbAttr(rName)}');return false" title="Search options for &quot;${_mbAttr(rName)}&quot;" style="margin-right:0.3rem;color:var(--muted);text-decoration:none">🔍</a>`;
        const titleLink = `<a href="#" onclick="event.preventDefault();_mbOpenDetail('release','${safeId}')" style="color:var(--text);text-decoration:none;font-weight:600">${_mbEsc(rName)}</a>`;
        return `<tr>
          <td style="color:var(--muted);font-variant-numeric:tabular-nums">${_mbEsc(rel.date || "—")}</td>
          <td>${lookupHtml}${titleLink}</td>
          <td style="color:var(--muted);font-size:0.76rem">${_mbEsc(rel.country || "")}</td>
        </tr>`;
      }).join("")}</tbody>
    </table>
  `;
}

function _mbRenderMediaTracklist(media) {
  if (!media.length) return "";
  return media.map((m, i) => {
    const tracks = Array.isArray(m.tracks) ? m.tracks : (Array.isArray(m["track-list"]) ? m["track-list"] : []);
    return `
      <h4 style="font-size:0.86rem;color:var(--accent);margin:1rem 0 0.4rem">${_mbEsc(m.format || "Disc")} ${m.position || (i + 1)} (${tracks.length} track${tracks.length === 1 ? "" : "s"})</h4>
      <table class="api-log-table" style="font-size:0.82rem;width:100%">
        <tbody>${tracks.map(t => {
          const trackTitle = t.title || t.recording?.title || "(untitled)";
          const trackLen   = t.length || t.recording?.length;
          const recId      = t.recording?.id;
          const safeRecId  = String(recId || "").replace(/'/g, "");
          const titleHtml = (typeof entityLookupLinkHtml === "function")
            ? entityLookupLinkHtml("track", trackTitle, { title: `Lookup options for "${trackTitle}"` })
            : _mbEsc(trackTitle);
          const openLink = recId
            ? ` <a href="#" onclick="event.preventDefault();event.stopPropagation();_mbOpenDetail('recording','${safeRecId}')" title="Open this MB recording" style="color:var(--muted);text-decoration:none;font-size:0.78em">↗</a>`
            : "";
          return `<tr>
            <td style="width:2.5rem;color:var(--muted);font-variant-numeric:tabular-nums">${_mbEsc(t.position || "")}</td>
            <td>${titleHtml}${openLink}</td>
            <td style="width:4rem;text-align:right;color:var(--muted);font-variant-numeric:tabular-nums">${trackLen ? _mbFormatMs(trackLen) : ""}</td>
          </tr>`;
        }).join("")}</tbody>
      </table>
    `;
  }).join("");
}

function _mbRenderWorkRels(relations) {
  if (!relations.length) return "";
  const writers   = relations.filter(r => r["target-type"] === "artist" && (r.type === "composer" || r.type === "lyricist" || r.type === "writer"));
  const recordings = relations.filter(r => r["target-type"] === "recording");
  const out = [];
  if (writers.length) {
    out.push(`<div style="margin:0.6rem 0;font-size:0.82rem"><span style="color:var(--accent)">Writers:</span> ${
      writers.map(w => {
        const aName = w.artist?.name || "";
        const aid   = w.artist?.id || "";
        const safeId = aid.replace(/'/g, "");
        if (!aName) return "";
        const link = (typeof entityLookupLinkHtml === "function")
          ? entityLookupLinkHtml("artist", aName, { title: `Lookup options for "${aName}"` })
          : _mbEsc(aName);
        const openLink = aid ? ` <a href="#" onclick="event.preventDefault();_mbOpenDetail('artist','${safeId}')" style="color:var(--muted);font-size:0.78em">↗</a>` : "";
        return `${link}${openLink} <span style="color:var(--muted);font-size:0.75rem">(${_mbEsc(w.type)})</span>`;
      }).join(", ")
    }</div>`);
  }
  if (recordings.length) {
    out.push(`<h4 style="font-size:0.86rem;color:var(--accent);margin:1rem 0 0.4rem">Recordings (${recordings.length})</h4>`);
    out.push(`<ul style="margin:0;padding-left:1.2rem;font-size:0.82rem">${
      recordings.map(r => {
        const rid = r.recording?.id || "";
        const safeId = rid.replace(/'/g, "");
        const title = r.recording?.title || "(untitled)";
        const link = rid
          ? `<a href="#" onclick="event.preventDefault();_mbOpenDetail('recording','${safeId}')" style="color:var(--text);text-decoration:none">${_mbEsc(title)}</a>`
          : _mbEsc(title);
        return `<li style="margin:0.15rem 0">${link}</li>`;
      }).join("")
    }</ul>`);
  }
  return out.join("");
}

// Recent tab placeholder. Future: list the most recently-cached MB
// entities (with link to re-open their detail). For now, just shows
// a note explaining what the tab will do.
async function _mbLoadRecent() {
  const el = document.getElementById("mb-recent");
  if (!el) return;
  el.innerHTML = `<div class="loc-empty">Recently-cached MB entities will land here. (Coming with the next iteration — for now switch back to Search to re-run a query.)</div>`;
}
