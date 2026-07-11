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
// ★ save state. _mbSaveIds is a Set of "type:mbid" strings, pulled
// on init via /api/musicbrainz/saves/ids and maintained optimistically
// on every star toggle. _mbSavedRows is the full save-row list,
// fetched when the Saved tab opens.
let _mbSaveIds = new Set();
let _mbSavedRows = [];
let _mbAdvancedOpen = false;

function initMusicbrainzView() {
  // Pull the user's existing save-id set so result rows render the
  // correct ★/☆ glyph on first paint. Best-effort; an empty Set is
  // a safe fallback (clicks would error-toast on a 401).
  _mbLoadSaveIds().catch(() => {});
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
        if (sav.tab === "saved") { _mbSwitchTab("saved"); return; }
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
  // Panel id is mb-advanced-panel — the previous selector targeted
  // a nonexistent #mb-advanced, so every filter stayed visible
  // regardless of entity. That let users type into fields MB doesn't
  // honour on the selected index (e.g. Label on the artist index),
  // and the Lucene query came back with garbage.
  document.querySelectorAll("#mb-advanced-panel [data-mb-filter]").forEach(lbl => {
    const k = lbl.dataset.mbFilter;
    lbl.style.display = allow.has(k) ? "" : "none";
  });
}
window._mbToggleEntityFilters = _mbToggleEntityFilters;

function _mbSwitchTab(tab) {
  _mbTab = tab === "saved" ? "saved" : "search";
  document.querySelectorAll("#musicbrainz-tabs .loc-tab").forEach(b => {
    b.classList.toggle("active", b.dataset.mbTab === _mbTab);
  });
  const ps = document.querySelector(".musicbrainz-panel-search");
  const pr = document.querySelector(".musicbrainz-panel-saved");
  if (ps) ps.style.display = _mbTab === "search" ? "" : "none";
  if (pr) pr.style.display = _mbTab === "saved"  ? "" : "none";
  if (_mbTab === "saved") _mbLoadSaved();
  _mbPersistState();
}
window._mbSwitchTab = _mbSwitchTab;

// Advanced-panel disclosure is gone — the filter grid (incl. the
// Entity selector) lives inline on one row. Stub kept so any saved
// view state referring to the toggle is a no-op rather than a crash.
function _mbToggleAdvanced() { /* no-op */ }
window._mbToggleAdvanced = _mbToggleAdvanced;

function _mbPersistState() {
  try {
    if (typeof window._sdSaveViewState !== "function") return;
    const state = {
      entity: _mbEntity,
      tab:    _mbTab,
      adv:    _mbAdvancedOpen,
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
  const pagEl    = document.getElementById("mb-pagination");
  if (!append) {
    if (resultsEl) resultsEl.innerHTML = `<div class="loc-empty">Searching MusicBrainz…</div>`;
    // Wipe the stale pager immediately so a previous result set's
    // page chrome doesn't linger while the new request is in
    // flight. Site-wide pagination-clears-on-new-search policy.
    if (pagEl) pagEl.innerHTML = "";
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

// Tag-pill click handler — closes any open detail overlay, switches
// the entity selector to match the tagged entity type, fills the
// query box with MB Lucene `tag:"<name>"` syntax, and fires a fresh
// search. Tag names with embedded quotes are escaped MB-side via
// backslash, but we keep the search string simple and rely on URL
// encoding for transport.
function _mbSearchByTag(tag, entityType) {
  try {
    document.getElementById("mb-detail-overlay")?.remove();
    if (Array.isArray(_mbDetailStack)) _mbDetailStack.length = 0;
  } catch {}
  const entSel = document.getElementById("mb-entity");
  if (entSel && entityType) {
    const opt = Array.from(entSel.options || []).find(o => o.value === entityType);
    if (opt) entSel.value = entityType;
  }
  const qEl = document.getElementById("mb-q");
  if (qEl) qEl.value = `tag:"${String(tag).replace(/"/g, '\\"')}"`;
  // Make sure we're on the Search tab, not Saved.
  try { _mbSwitchTab("search"); } catch {}
  _mbRunSearch();
}
window._mbSearchByTag = _mbSearchByTag;

function _mbEsc(s) {
  return escHtml(s);   // canonical escaper (shared.js) — escapes & < > " '
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
  // ★ save affordance — toggles persistence to musicbrainz_saves.
  // _mbSaveIds.has() resolves synchronously off the in-memory set
  // populated at view-init, so the right glyph renders without a
  // network probe per row.
  const saveKey = `${entity}:${mbid}`;
  const saved   = _mbSaveIds.has(saveKey);
  const saveHtml = `<a href="#" class="mb-row-save-link" data-mb-save-id="${_mbAttr(saveKey)}" onclick="event.preventDefault();event.stopPropagation();_mbToggleSave('${entity}','${safeId}','${_mbAttr(name)}',this)" title="${saved ? 'Saved — click to remove' : 'Save'}" style="margin-right:0.4rem;color:${saved ? '#ffd166' : 'var(--muted)'};text-decoration:none">${saved ? '★' : '☆'}</a>`;
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
      <div style="font-weight:600">${saveHtml}${lookupHtml}${_mbEsc(name)}${score}</div>
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

// Navigation stack — every entity we've opened in the current
// overlay session, in order. Clicking a nested entity ↗ pushes a
// new frame; the ← back button pops back to the previous. Cleared
// on overlay close (× / outside-click) so a fresh open always starts
// at a single-entry stack.
const _mbDetailStack = [];

// Detail overlay. Hits /api/musicbrainz/:type/:mbid (the server
// attaches per-entity `inc=` defaults so we get linked entities for
// free) and renders a Discogs-popup-styled panel: title + lookup
// affordance, meta line, external-link strip, and entity-specific
// content sections. Caches the response on each stack frame so the
// ← back navigation re-renders without a second network call.
async function _mbOpenDetail(type, mbid, opts = {}) {
  let overlay = document.getElementById("mb-detail-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "mb-detail-overlay";
    Object.assign(overlay.style, {
      position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
      zIndex: "300", display: "flex", alignItems: "flex-start",
      justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
    });
    overlay.onclick = (e) => {
      if (e.target === overlay) {
        overlay.remove();
        _mbDetailStack.length = 0;
      }
    };
    document.body.appendChild(overlay);
  }
  // Stack management: a normal open pushes a new frame; the ← back
  // handler passes { fromBack: true } so we don't push the frame we
  // just popped back to. We don't dedupe same (type, mbid) frames —
  // walking a release → its artist → that artist's release-group →
  // a different release in the same group is a legitimate path the
  // user might want to retrace.
  if (!opts.fromBack) {
    _mbDetailStack.push({ type, mbid, j: null });
  }
  overlay.innerHTML = `
    <div class="mb-detail-card" style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1rem 1.2rem;width:min(900px,100%)">
      <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.5rem">
        <div style="min-width:0;flex:1">
          <h3 class="mb-detail-title" style="margin:0 0 0.2rem;font-size:1.05rem">Loading…</h3>
          <div class="mb-detail-meta" style="color:var(--muted);font-size:0.78rem"></div>
        </div>
        <div style="display:flex;gap:0.3rem;align-items:start">
          ${_mbDetailStack.length > 1
            ? `<button class="archive-btn mb-detail-back" onclick="_mbDetailBack()" title="Back to ${_mbEsc(_mbDetailStack[_mbDetailStack.length - 2].type)}" style="padding:0.25rem 0.6rem">← Back</button>`
            : ""}
          <button class="archive-btn" onclick="document.getElementById('mb-detail-overlay')?.remove();_mbDetailStack.length=0" style="font-size:1.2rem;padding:0 0.6rem">×</button>
        </div>
      </div>
      <div class="mb-detail-body" style="font-size:0.84rem;color:var(--text)">Fetching…</div>
    </div>
  `;
  try {
    // Frames already on the stack carry their fetched JSON, so a back
    // nav re-renders without a network round trip.
    const top = _mbDetailStack[_mbDetailStack.length - 1];
    let j = top?.j;
    if (!j) {
      const r = await apiFetch(`/api/musicbrainz/${encodeURIComponent(type)}/${encodeURIComponent(mbid)}`);
      if (!r.ok) {
        const txt = await r.text().catch(() => "");
        throw new Error(`HTTP ${r.status}: ${txt.slice(0, 200)}`);
      }
      j = await r.json();
      if (top) top.j = j;
    }
    _mbRenderDetail(overlay, type, mbid, j);
  } catch (e) {
    overlay.querySelector(".mb-detail-body").innerHTML =
      `<div style="color:#e88">Load failed: ${_mbEsc(String(e?.message || e))}</div>`;
  }
}
window._mbOpenDetail = _mbOpenDetail;

// Back-button handler: pops the top of the navigation stack and
// re-renders whatever frame is now at the top. Cached JSON from the
// previous fetch means no network call. No-op if the stack is too
// shallow (the × handler is what closes the overlay entirely).
function _mbDetailBack() {
  if (_mbDetailStack.length <= 1) return;
  _mbDetailStack.pop();
  const prev = _mbDetailStack[_mbDetailStack.length - 1];
  if (!prev) return;
  _mbOpenDetail(prev.type, prev.mbid, { fromBack: true });
}
window._mbDetailBack = _mbDetailBack;

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

  // Extract Discogs id from url-rels — when MB has a `discogs` link
  // for this artist / label, we thread the id into the lookup popup
  // (as entityId) so the "Search SeaDisco" / "Edit in Blues Archive"
  // / "Open archive profile" buttons all use the exact-match by-id
  // fast paths instead of name substring matching. The external
  // Discogs link previously rendered alongside is removed — the
  // lookup popup is the single entry point.
  const discogsId = _mbExtractDiscogsId(type, j);

  // Title — wrapped in entityLookupLinkHtml so clicking the name
  // opens the search-options popup. Scope by entity type so the
  // resulting search routes through the right SeaDisco field.
  const scope = (type === "artist" || type === "label") ? type
              : (type === "recording" || type === "work")  ? "track"
              : "release";
  const titleHtml = (typeof entityLookupLinkHtml === "function")
    ? entityLookupLinkHtml(scope, name, {
        entityId: discogsId || undefined,
        title: `Lookup options for "${name}"`,
      })
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
    // Rating: MB returns { value, votes-count }. Hide when zero votes.
    const rat = j.rating;
    if (rat && Number(rat.value) > 0) {
      metaParts.push(`★ ${Number(rat.value).toFixed(1)}/5 <span style="color:#666">(${Number(rat["votes-count"]) || 0})</span>`);
    }
    if (Array.isArray(j.isnis) && j.isnis.length) metaParts.push("ISNI " + _mbEsc(j.isnis[0]));
    if (Array.isArray(j.ipis)  && j.ipis.length)  metaParts.push("IPI "  + _mbEsc(j.ipis[0]));
  } else if (type === "release" || type === "release-group") {
    if (j.date)             metaParts.push(_mbEsc(j.date));
    if (j.country)          metaParts.push(_mbEsc(j.country));
    if (j.status)           metaParts.push(_mbEsc(j.status));
    if (j.packaging)        metaParts.push(_mbEsc(j.packaging));
    if (j["primary-type"])  metaParts.push(_mbEsc(j["primary-type"]));
    if (Array.isArray(j["secondary-types"]) && j["secondary-types"].length) {
      metaParts.push(_mbEsc(j["secondary-types"].join(" / ")));
    }
    if (j.barcode)          metaParts.push("UPC " + _mbEsc(j.barcode));
    if (j.asin)             metaParts.push("ASIN " + _mbEsc(j.asin));
    const tr = j["text-representation"];
    if (tr?.language || tr?.script) {
      metaParts.push(`${_mbEsc(tr?.language || "")}${tr?.language && tr?.script ? "/" : ""}${_mbEsc(tr?.script || "")}`);
    }
    if (j["cover-art-archive"]?.artwork) metaParts.push(`<a href="https://coverartarchive.org/release/${_mbEsc(mbid)}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">cover ↗</a>`);
  } else if (type === "recording") {
    if (j.length)               metaParts.push(_mbFormatMs(j.length));
    if (j["first-release-date"]) metaParts.push(_mbEsc(j["first-release-date"]));
    if (j.video)                metaParts.push("video");
    if (Array.isArray(j.isrcs) && j.isrcs.length) metaParts.push("ISRC " + _mbEsc(j.isrcs[0]));
  } else if (type === "label") {
    if (j.type)    metaParts.push(_mbEsc(j.type));
    if (j.country) metaParts.push(_mbEsc(j.country));
    if (j["label-code"]) metaParts.push("LC " + _mbEsc(j["label-code"]));
    const ls = j["life-span"];
    if (ls?.begin || ls?.end) metaParts.push(`${_mbEsc(ls?.begin || "?")} – ${_mbEsc(ls?.ended ? (ls?.end || "?") : "present")}`);
    if (Array.isArray(j.isnis) && j.isnis.length) metaParts.push("ISNI " + _mbEsc(j.isnis[0]));
    if (Array.isArray(j.ipis)  && j.ipis.length)  metaParts.push("IPI "  + _mbEsc(j.ipis[0]));
  } else if (type === "work") {
    if (j.type) metaParts.push(_mbEsc(j.type));
    if (Array.isArray(j.iswcs) && j.iswcs.length) metaParts.push("ISWC " + _mbEsc(j.iswcs[0]));
    if (Array.isArray(j.languages) && j.languages.length) metaParts.push(_mbEsc(j.languages.join(", ")));
  }
  if (j.source === "cache") metaParts.push(`<span style="color:#7c7" title="Served from local cache">cached</span>`);
  // Meta line lives at the bottom of the body (just above the external
  // links chip strip) — leaves the popup header clean (title +
  // disambiguation only) and groups the structural metadata with the
  // outbound link bar as a footer block.
  metaEl.innerHTML = "";
  const metaFooterHtml = `<div style="margin:0.6rem 0 0.3rem;padding-top:0.5rem;border-top:1px solid var(--border);font-size:0.82rem;color:var(--muted)">${metaParts.join(' <span style="color:#555">·</span> ')}</div>`;

  // Body — assemble sections then drop them in.
  const sections = [];

  // Annotation (free-form curator note attached to any MB entity —
  // rare on artists / releases but used for release-groups + works to
  // explain composition / version differences). Surface when present.
  if (j.annotation) {
    sections.push(`<div style="margin:0.6rem 0;padding:0.5rem 0.7rem;border-left:3px solid var(--border);color:var(--muted);font-size:0.82rem;font-style:italic;white-space:pre-wrap">${_mbEsc(j.annotation)}</div>`);
  }

  // Wikipedia bio (collapsible, lazy-loaded). MB stores artist
  // biographies via either a direct `wikipedia` URL relation or, more
  // commonly nowadays, a `wikidata` relation that points at the QID
  // (the server then resolves QID → enwiki title via Wikidata's
  // wbgetentities API). Either way the panel hits the same
  // /api/musicbrainz/wiki endpoint, which caches results on hit.
  // Prefer direct wikipedia rel when present (skips the QID round-
  // trip); fall back to wikidata otherwise.
  let wikiOpenUrl = "";
  let wikiLabel = "";
  let wikiLookupArg = ""; // "title:<x>" or "qid:Qxxx"
  if (Array.isArray(j.relations)) {
    const wpRel = j.relations.find(rel => rel.type === "wikipedia" && rel.url?.resource);
    if (wpRel) {
      const wikiUrl = String(wpRel.url.resource);
      const m = wikiUrl.match(/\/wiki\/([^?#]+)/);
      if (m) {
        const title = decodeURIComponent(m[1]).replace(/_/g, " ");
        wikiOpenUrl = wikiUrl;
        wikiLabel = title;
        wikiLookupArg = `title:${title}`;
      }
    }
    if (!wikiLookupArg) {
      const wdRel = j.relations.find(rel => rel.type === "wikidata" && rel.url?.resource);
      if (wdRel) {
        const wdUrl = String(wdRel.url.resource);
        const qm = wdUrl.match(/\/(Q\d+)\b/i);
        if (qm) {
          const qid = qm[1].toUpperCase();
          wikiOpenUrl = wdUrl;
          wikiLabel = `via Wikidata ${qid}`;
          wikiLookupArg = `qid:${qid}`;
        }
      }
    }
  }
  if (wikiLookupArg) {
    sections.push(`
      <div class="mb-wiki-bio" style="margin:0.6rem 0;border:1px solid var(--border);border-radius:6px;background:rgba(255,255,255,0.02)">
        <button type="button" class="mb-wiki-bio-toggle" onclick="_mbToggleWikiBio(this,'${_mbAttr(wikiLookupArg)}')" style="display:flex;align-items:center;justify-content:space-between;width:100%;padding:0.5rem 0.7rem;border:none;background:transparent;color:var(--accent);font-size:0.86rem;cursor:pointer;font-family:inherit;text-align:left">
          <span><span class="mb-wiki-bio-arrow">▶</span> Wikipedia bio — ${_mbEsc(wikiLabel)}</span>
          <a href="${_mbEsc(wikiOpenUrl)}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--muted);text-decoration:none;font-size:0.78em">open ↗</a>
        </button>
        <div class="mb-wiki-bio-body" style="display:none"></div>
      </div>
    `);
  }

  // Toggle handler for the Wikipedia bio panel — declared inline here
  // so it captures the section's already-rendered DOM via the button
  // arg. Lazy fetch on first expand; subsequent expands reuse the
  // cached HTML on the DOM node. Server-side the /api/musicbrainz/wiki
  // endpoint caches the article in musicbrainz_cache (entity_type
  // "wiki"), so re-opening the same artist across sessions is free.
  if (!window._mbToggleWikiBio) {
    window._mbToggleWikiBio = async function (btn, lookupArg) {
      const body = btn.nextElementSibling;
      const arrowEl = btn.querySelector(".mb-wiki-bio-arrow");
      if (!body) return;
      const isOpen = body.style.display !== "none";
      if (isOpen) {
        body.style.display = "none";
        if (arrowEl) arrowEl.textContent = "▶";
        return;
      }
      body.style.display = "block";
      if (arrowEl) arrowEl.textContent = "▼";
      if (body.dataset.loaded === "1") return;
      body.innerHTML = `<div style="padding:0.6rem 0.7rem;color:var(--muted);font-size:0.82rem">Loading bio…</div>`;
      try {
        // lookupArg is "title:<x>" or "qid:Qxxx". Both go through
        // /api/musicbrainz/wiki — the server resolves qid → enwiki
        // title via Wikidata.
        let qs;
        if (lookupArg.startsWith("qid:")) {
          qs = `qid=${encodeURIComponent(lookupArg.slice(4))}`;
        } else {
          const t = lookupArg.startsWith("title:") ? lookupArg.slice(6) : lookupArg;
          qs = `title=${encodeURIComponent(t)}`;
        }
        const r = await apiFetch(`/api/musicbrainz/wiki?${qs}`);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const j = await r.json();
        if (!j.found || !j.html) {
          body.innerHTML = `<div style="padding:0.6rem 0.7rem;color:var(--muted);font-size:0.82rem">No Wikipedia article found.</div>`;
          return;
        }
        // Inject a scoped <style> block once so the article HTML
        // inherits sane typography inside the popup — Wikipedia ships
        // its own class names with no inline styles, so a scoped block
        // here is enough to tame heading sizes, link colors, image
        // sizing, list indents and table widths. Marker on
        // documentElement prevents duplicate injections across opens.
        if (!document.documentElement.dataset.mbWikiBioStyled) {
          const style = document.createElement("style");
          style.textContent = `
            .mb-wiki-bio-content { padding: 0.8rem 1rem 0.6rem; max-height: 60vh; overflow-y: auto; font-size: 0.85rem; line-height: 1.6; color: var(--text); border-top: 1px solid var(--border); }
            .mb-wiki-bio-content p { margin: 0 0 0.7rem; }
            .mb-wiki-bio-content p:last-child { margin-bottom: 0.2rem; }
            .mb-wiki-bio-content h2 { font-size: 0.98rem; margin: 1.1rem 0 0.4rem; padding-bottom: 0.2rem; border-bottom: 1px solid var(--border); color: var(--accent); font-weight: 600; }
            .mb-wiki-bio-content h3 { font-size: 0.9rem; margin: 0.9rem 0 0.35rem; color: var(--accent); font-weight: 600; }
            .mb-wiki-bio-content h4, .mb-wiki-bio-content h5, .mb-wiki-bio-content h6 { font-size: 0.85rem; margin: 0.7rem 0 0.3rem; color: var(--accent); font-weight: 600; }
            .mb-wiki-bio-content a { color: var(--accent); text-decoration: none; }
            .mb-wiki-bio-content a:hover { text-decoration: underline; }
            .mb-wiki-bio-content ul, .mb-wiki-bio-content ol { margin: 0.3rem 0 0.7rem 1.2rem; padding: 0; }
            .mb-wiki-bio-content li { margin: 0.15rem 0; }
            .mb-wiki-bio-content img { max-width: 100%; height: auto; border-radius: 4px; display: block; }
            .mb-wiki-bio-content figure, .mb-wiki-bio-content .thumb { max-width: min(280px, 100%) !important; margin: 0.4rem 0 0.6rem 0.8rem !important; float: right !important; clear: right; }
            .mb-wiki-bio-content figure img, .mb-wiki-bio-content .thumb img { width: 100%; height: auto; }
            .mb-wiki-bio-content .thumbcaption, .mb-wiki-bio-content figcaption { font-size: 0.75rem; color: var(--muted); padding: 0.25rem 0.3rem 0; line-height: 1.35; }
            .mb-wiki-bio-content .thumbinner { padding: 0.25rem; border: 1px solid var(--border); border-radius: 4px; background: rgba(255,255,255,0.02); }
            .mb-wiki-bio-content .magnify { display: none; }
            .mb-wiki-bio-content::after { content: ""; display: block; clear: both; }
            .mb-wiki-bio-content table { max-width: 100%; border-collapse: collapse; margin: 0.4rem 0; font-size: 0.82rem; }
            .mb-wiki-bio-content table td, .mb-wiki-bio-content table th { padding: 0.2rem 0.4rem; border: 1px solid var(--border); }
            .mb-wiki-bio-content blockquote { margin: 0.4rem 0; padding: 0.3rem 0.7rem; border-left: 3px solid var(--border); color: var(--muted); }
            .mb-wiki-bio-content code, .mb-wiki-bio-content pre { font-size: 0.8rem; background: rgba(255,255,255,0.04); padding: 0.05rem 0.3rem; border-radius: 3px; }
            .mb-wiki-bio-content .IPA, .mb-wiki-bio-content .nowrap { white-space: nowrap; }
            .mb-wiki-bio-content sup, .mb-wiki-bio-content sub { font-size: 0.7em; }
            .mb-wiki-bio-content hr { border: none; border-top: 1px solid var(--border); margin: 0.8rem 0; }
          `;
          document.head.appendChild(style);
          document.documentElement.dataset.mbWikiBioStyled = "1";
        }
        body.innerHTML = `<div class="mb-wiki-bio-content">${j.html}</div>`;
        body.dataset.loaded = "1";
      } catch (err) {
        body.innerHTML = `<div style="padding:0.6rem 0.7rem;color:#e88;font-size:0.82rem">Bio load failed: ${String(err?.message || err)}</div>`;
      }
    };
  }

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
  // Each chip is a link that fires a new MB search filtered to the
  // current entity type by `tag:"<name>"` (MB Lucene syntax). Number
  // is the community vote count — net upvotes on that tag for this
  // entity, not a result count.
  const tags = Array.isArray(j.tags) ? j.tags.slice(0, 25) : [];
  if (tags.length) {
    sections.push(`<div style="display:flex;gap:0.3rem;flex-wrap:wrap;margin:0.4rem 0">${
      tags.map(t => {
        const tn = t.name || "";
        if (!tn) return "";
        return `<a href="#" onclick="event.preventDefault();_mbSearchByTag('${_mbAttr(tn)}','${_mbAttr(type)}')" title="Search ${_mbEsc(type)}s tagged &quot;${_mbEsc(tn)}&quot;" style="padding:0.15rem 0.45rem;border:1px solid var(--border);border-radius:999px;font-size:0.7rem;color:var(--muted);text-decoration:none;cursor:pointer">${_mbEsc(tn)} <span style="color:#666">${Number(t.count) || ""}</span></a>`;
      }).join("")
    }</div>`);
  }

  // Aliases (artist / label) — each alias name routes through the
  // standard lookup-options popup so the curator can pivot to a
  // SeaDisco / Discogs / Blues Archive / MB search by the alias.
  if (type === "artist" || type === "label") {
    const aliases = Array.isArray(j.aliases) ? j.aliases.filter(a => a.name && a.name !== name).slice(0, 12) : [];
    if (aliases.length) {
      const aliasHtml = aliases.map(a => {
        return (typeof entityLookupLinkHtml === "function")
          ? entityLookupLinkHtml(scope, a.name, { title: `Lookup options for "${a.name}"` })
          : _mbEsc(a.name);
      }).join(", ");
      sections.push(`<div style="margin:0.6rem 0;color:var(--muted);font-size:0.8rem"><span style="color:var(--accent)">Aliases:</span> ${aliasHtml}</div>`);
    }
  }

  // Entity-specific lists.
  if (type === "artist") {
    sections.push(_mbRenderReleaseGroupsList(j["release-groups"] || []));
  } else if (type === "release-group") {
    sections.push(_mbRenderReleasesList(j.releases || []));
  } else if (type === "release") {
    // Parent release-group link, when MB attaches it.
    if (j["release-group"]?.id) {
      const rgId   = String(j["release-group"].id).replace(/'/g, "");
      const rgName = j["release-group"].title || "";
      sections.push(`<div style="margin:0.4rem 0;font-size:0.82rem"><span style="color:var(--muted)">part of</span> <a href="#" onclick="event.preventDefault();_mbOpenDetail('release-group','${rgId}')" style="color:var(--accent);text-decoration:none">${_mbEsc(rgName)}</a> ↗</div>`);
    }
    // Labels — each one shown with the catalog number for this
    // release. Compact strip with the label name as a lookup-popup
    // link so the curator can pivot to Discogs / SeaDisco by label.
    const labelInfos = Array.isArray(j["label-info"]) ? j["label-info"] : [];
    if (labelInfos.length) {
      const labelHtml = labelInfos.map(li => {
        const ln = li.label?.name || "";
        const cn = li["catalog-number"] || "";
        const lid = li.label?.id;
        const safeLid = String(lid || "").replace(/'/g, "");
        if (!ln && !cn) return "";
        const lookupHtml = ln && (typeof entityLookupLinkHtml === "function")
          ? entityLookupLinkHtml("label", ln, { title: `Lookup options for "${ln}"` })
          : _mbEsc(ln);
        const openLink = lid ? ` <a href="#" onclick="event.preventDefault();_mbOpenDetail('label','${safeLid}')" title="Open this MB label" style="color:var(--muted);text-decoration:none;font-size:0.78em">↗</a>` : "";
        const catHtml = cn ? ` <code style="color:var(--accent);font-size:0.8em">${_mbEsc(cn)}</code>` : "";
        return `<span style="margin-right:0.6rem">${lookupHtml}${openLink}${catHtml}</span>`;
      }).filter(Boolean).join("");
      if (labelHtml) sections.push(`<div style="margin:0.4rem 0;font-size:0.82rem"><span style="color:var(--muted)">Labels:</span> ${labelHtml}</div>`);
    }
    // Release events — only when there are multiple country/date
    // combos worth surfacing (e.g. "US 1968-05, UK 1968-07"). A
    // single event matches the meta line's date+country so we hide
    // that case to avoid duplication.
    const events = Array.isArray(j["release-events"]) ? j["release-events"] : [];
    if (events.length > 1) {
      sections.push(`<div style="margin:0.4rem 0;font-size:0.82rem;color:var(--muted)"><span style="color:var(--accent)">Released:</span> ${events.map(e => {
        const c = e.area?.["iso-3166-1-codes"]?.[0] || e.area?.name || "";
        return `${_mbEsc(e.date || "?")}${c ? " (" + _mbEsc(c) + ")" : ""}`;
      }).join(", ")}</div>`);
    }
    sections.push(_mbRenderMediaTracklist(j.media || []));
  } else if (type === "recording") {
    sections.push(_mbRenderReleasesList(j.releases || [], "recording"));
  } else if (type === "label") {
    sections.push(_mbRenderReleasesList(j.releases || []));
  } else if (type === "work") {
    sections.push(_mbRenderWorkRels(j.relations || []));
  }

  // Meta footer (mbid · MusicBrainz ↗ · type/gender/country/lifespan ·
  // ISNI · cached) — sits directly above the external links so the
  // structural metadata + outbound link bar read as a single footer.
  sections.push(metaFooterHtml);

  // External links chip strip — rendered at the very bottom so the
  // popup leads with content (bio, tags, release lists, tracklists)
  // and exits with the external-link bar. Discogs + streaming/purchase
  // rel types are filtered inside _mbRenderUrlRels.
  sections.push(_mbRenderUrlRels(j));

  bodyEl.innerHTML = sections.filter(Boolean).join("\n");
}

function _mbRenderUrlRels(j) {
  // Discogs intentionally dropped — the title-lookup popup already
  // handles every Discogs-scoped operation (open the discogs.com
  // page, search SeaDisco by id, edit Blues Archive row) once we've
  // threaded the id through entityId. Rendering a separate Discogs ↗
  // anchor here was redundant clutter.
  //
  // Streaming + purchase relation types also filtered out — the
  // entries are mostly platform-deep links (Spotify / Apple Music /
  // Tidal / iTunes / Amazon) that don't help a curator and bloat
  // the chip strip. The lookup popup's "YouTube" entry covers the
  // playback intent for the rare case it's wanted.
  const HIDDEN_REL_TYPES = new Set([
    "discogs", "free streaming", "streaming",
    "purchase for download", "purchase for mail-order", "download for free",
  ]);
  const rels = Array.isArray(j.relations)
    ? j.relations.filter(rel => (rel["target-type"] === "url" || rel.url) && !HIDDEN_REL_TYPES.has(rel.type))
    : [];
  if (!rels.length) return "";
  // Sort: well-known external sources first, then everything else.
  const priority = ["wikipedia", "wikidata", "allmusic", "official homepage", "imdb", "bandcamp", "soundcloud", "youtube", "spotify"];
  rels.sort((a, b) => (priority.indexOf(a.type) + 1 || 99) - (priority.indexOf(b.type) + 1 || 99));
  // Per-source visual hint: a glyph that's recognisable at a glance,
  // a label scrubbed of the relation-type cruft ("free streaming" →
  // the host name when possible), and a fallback for unknowns. Each
  // link renders as a chip — bordered pill the same shape as the tag
  // strip — so the section reads as a tidy link bar instead of loose
  // accent text.
  const out = rels.map(rel => {
    const url = String(rel.url?.resource || "");
    if (!url) return "";
    const meta = _mbLinkMeta(rel.type, url);
    return `<a href="${_mbEsc(url)}" target="_blank" rel="noopener" class="mb-url-chip" title="${_mbEsc(rel.type)} · ${_mbEsc(url)}" style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.2rem 0.55rem;border:1px solid var(--border);border-radius:999px;background:rgba(255,255,255,0.02);color:var(--accent);text-decoration:none;font-size:0.78rem"><span aria-hidden="true">${meta.icon}</span>${_mbEsc(meta.label)} <span style="color:var(--muted);font-size:0.78em">↗</span></a>`;
  }).filter(Boolean);
  if (!out.length) return "";
  return `<div style="display:flex;flex-wrap:wrap;gap:0.35rem;margin:0.5rem 0">${out.join("")}</div>`;
}

// Map an MB relation type + url to a display chip's icon + label.
// "free streaming" / "streaming" / "other databases" are MB's generic
// catch-alls — we lift the actual host name out of the URL so the
// chip reads "Spotify" instead of "free streaming". Falls back to
// the raw relation type for anything we haven't curated.
function _mbLinkMeta(relType, url) {
  const host = (() => {
    try { return new URL(url).hostname.replace(/^www\./, ""); } catch { return ""; }
  })();
  const KNOWN = {
    "wikipedia.org":     { icon: "W", label: "Wikipedia" },
    "wikidata.org":      { icon: "W", label: "Wikidata" },
    "allmusic.com":      { icon: "♪", label: "AllMusic" },
    "imdb.com":          { icon: "▣", label: "IMDb" },
    "bandcamp.com":      { icon: "■", label: "Bandcamp" },
    "soundcloud.com":    { icon: "☁", label: "SoundCloud" },
    "youtube.com":       { icon: "▶", label: "YouTube" },
    "youtu.be":          { icon: "▶", label: "YouTube" },
    "music.youtube.com": { icon: "▶", label: "YouTube Music" },
    "spotify.com":       { icon: "●", label: "Spotify" },
    "open.spotify.com":  { icon: "●", label: "Spotify" },
    "music.apple.com":   { icon: "♪", label: "Apple Music" },
    "tidal.com":         { icon: "≋", label: "Tidal" },
    "amazon.com":        { icon: "▾", label: "Amazon" },
    "deezer.com":        { icon: "♬", label: "Deezer" },
    "rateyourmusic.com": { icon: "★", label: "RYM" },
    "setlist.fm":        { icon: "≡", label: "Setlist.fm" },
    "secondhandsongs.com": { icon: "♫", label: "SecondHandSongs" },
    "genius.com":        { icon: "G", label: "Genius" },
    "last.fm":           { icon: "♫", label: "Last.fm" },
    "viaf.org":          { icon: "ⓘ", label: "VIAF" },
    "isni.org":          { icon: "ⓘ", label: "ISNI" },
    "loc.gov":           { icon: "🏛", label: "LOC" },
    "id.loc.gov":        { icon: "🏛", label: "LOC" },
    "musicbrainz.org":   { icon: "M", label: "MusicBrainz" },
    "facebook.com":      { icon: "f", label: "Facebook" },
    "instagram.com":     { icon: "◉", label: "Instagram" },
    "twitter.com":       { icon: "𝕏", label: "Twitter" },
    "x.com":             { icon: "𝕏", label: "X" },
    "myspace.com":       { icon: "m", label: "MySpace" },
    "archive.org":       { icon: "▤", label: "Archive.org" },
    "vimeo.com":         { icon: "▶", label: "Vimeo" },
    "soundtrackcollector.com": { icon: "♫", label: "Soundtrack Collector" },
  };
  if (host && KNOWN[host]) return KNOWN[host];
  // Friendlier fallbacks for the generic MB relation types.
  const friendly = {
    "official homepage": { icon: "🏠", label: "Homepage" },
    "free streaming":    { icon: "▶", label: host || "Streaming" },
    "streaming":         { icon: "▶", label: host || "Streaming" },
    "other databases":   { icon: "ⓘ", label: host || "Database" },
    "social network":    { icon: "◉", label: host || "Social" },
    "purchase for download": { icon: "↓", label: host || "Purchase" },
    "purchase for mail-order": { icon: "✉", label: host || "Purchase" },
    "lyrics":            { icon: "📜", label: host || "Lyrics" },
    "image":             { icon: "🖼", label: host || "Image" },
    "blog":              { icon: "✎", label: host || "Blog" },
    "fanpage":           { icon: "◉", label: host || "Fan page" },
  };
  if (friendly[relType]) return friendly[relType];
  // Last resort: the relation type itself with a generic link icon.
  return { icon: "↗", label: relType || (host || "Link") };
}

// Extract the Discogs id (artist or label) from an MB entity's
// url-rels block. Only meaningful for entity types that map onto
// Discogs (artist / label); returns "" otherwise. The id ends up
// threaded into entityLookupLinkHtml as entityId, which is what
// powers the popup's "Search SeaDisco" / "Edit in Blues Archive"
// fast paths.
function _mbExtractDiscogsId(type, j) {
  if (type !== "artist" && type !== "label") return "";
  const rels = Array.isArray(j.relations) ? j.relations : [];
  for (const rel of rels) {
    if (rel.type !== "discogs") continue;
    const url = String(rel.url?.resource || "");
    const m = url.match(/discogs\.com\/(?:artist|label)\/(\d+)/i);
    if (m) return m[1];
  }
  return "";
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

// ── Saves ──────────────────────────────────────────────────────
// Per-admin ★ bookmark list. All four functions short-circuit on
// 401 so an unauth state doesn't spam errors.
async function _mbLoadSaveIds() {
  try {
    const r = await apiFetch("/api/musicbrainz/saves/ids");
    if (!r.ok) return;
    const { ids = [] } = await r.json();
    _mbSaveIds = new Set(ids);
  } catch {}
}

async function _mbToggleSave(entityType, mbid, name, btnEl) {
  const key = `${entityType}:${mbid}`;
  const wasSaved = _mbSaveIds.has(key);
  // Optimistic flip — repaint immediately, revert on failure.
  if (wasSaved) _mbSaveIds.delete(key); else _mbSaveIds.add(key);
  document.querySelectorAll(`[data-mb-save-id="${CSS.escape(key)}"]`).forEach(el => {
    el.textContent = _mbSaveIds.has(key) ? "★" : "☆";
    el.style.color = _mbSaveIds.has(key) ? "#ffd166" : "var(--muted)";
    el.title       = _mbSaveIds.has(key) ? "Saved — click to remove" : "Save";
  });
  try {
    const r = wasSaved
      ? await apiFetch(`/api/musicbrainz/saves/${encodeURIComponent(entityType)}/${encodeURIComponent(mbid)}`, { method: "DELETE" })
      : await apiFetch("/api/musicbrainz/saves", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ entity_type: entityType, mbid, name, meta: null }),
        });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    // Newly-saved rows: a refresh of the Saved tab's cached list is
    // cheaper than a synthetic insert, so just blow the cache so
    // next open re-fetches. The Saved tab is rarely opened so this
    // is a one-shot cost.
    _mbSavedRows = [];
  } catch (e) {
    // Revert optimistic flip on failure.
    if (wasSaved) _mbSaveIds.add(key); else _mbSaveIds.delete(key);
    document.querySelectorAll(`[data-mb-save-id="${CSS.escape(key)}"]`).forEach(el => {
      el.textContent = _mbSaveIds.has(key) ? "★" : "☆";
      el.style.color = _mbSaveIds.has(key) ? "#ffd166" : "var(--muted)";
    });
    console.warn("[mb save toggle]", e);
  }
}
window._mbToggleSave = _mbToggleSave;

async function _mbLoadSaved() {
  const el = document.getElementById("mb-saved-results");
  if (!el) return;
  const typeSel = document.getElementById("mb-saved-type")?.value || "";
  el.innerHTML = `<div class="loc-empty">Loading saves…</div>`;
  try {
    const qs = typeSel ? `?type=${encodeURIComponent(typeSel)}` : "";
    const r = await apiFetch(`/api/musicbrainz/saves${qs}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const { rows = [] } = await r.json();
    _mbSavedRows = rows;
    _mbRenderSaved();
  } catch (e) {
    el.innerHTML = `<div class="loc-empty" style="color:#e88">Load failed: ${_mbEsc(String(e?.message || e))}</div>`;
  }
}
window._mbLoadSaved = _mbLoadSaved;

function _mbRenderSaved() {
  const el = document.getElementById("mb-saved-results");
  const countEl = document.getElementById("mb-saved-count");
  if (!el) return;
  const filterRaw = document.getElementById("mb-saved-filter")?.value?.trim().toLowerCase() || "";
  const rows = _mbSavedRows.filter(r => {
    if (!filterRaw) return true;
    return (r.name || "").toLowerCase().includes(filterRaw)
        || (r.entity_type || "").toLowerCase().includes(filterRaw);
  });
  if (countEl) countEl.textContent = `${rows.length.toLocaleString()} of ${_mbSavedRows.length.toLocaleString()}`;
  if (!rows.length) {
    el.innerHTML = `<div class="loc-empty">${_mbSavedRows.length ? "No matches." : "No saves yet — click ★ on a search result to start a list."}</div>`;
    return;
  }
  el.innerHTML = rows.map(row => {
    const safeId = String(row.mbid || "").replace(/'/g, "");
    const saveKey = `${row.entity_type}:${row.mbid}`;
    const when = row.saved_at ? new Date(row.saved_at).toLocaleDateString() : "";
    const meta = row.meta || {};
    const sub = [];
    sub.push(_mbEsc(row.entity_type));
    if (meta.disambiguation) sub.push(`<em>${_mbEsc(meta.disambiguation)}</em>`);
    if (meta.country)        sub.push(_mbEsc(meta.country));
    if (meta.date)           sub.push(_mbEsc(meta.date));
    if (when) sub.push(`<span style="color:#666">saved ${_mbEsc(when)}</span>`);
    const saveBtn = `<a href="#" class="mb-row-save-link" data-mb-save-id="${_mbAttr(saveKey)}" onclick="event.preventDefault();event.stopPropagation();_mbToggleSave('${_mbEsc(row.entity_type)}','${safeId}','${_mbAttr(row.name || "")}',this)" title="Remove from saves" style="margin-right:0.4rem;color:#ffd166;text-decoration:none">★</a>`;
    const lookupScope = (row.entity_type === "artist" || row.entity_type === "label") ? row.entity_type
                      : (row.entity_type === "recording" || row.entity_type === "work") ? "track"
                      : "release";
    const lookupBtn = `<a href="#" class="mb-row-lookup-link" onclick="event.preventDefault();event.stopPropagation();openLookupPopup(event,'${lookupScope}','${_mbAttr(row.name || "")}');return false" title="Search options" style="margin-right:0.4rem;color:var(--muted);text-decoration:none">🔍</a>`;
    return `
      <div class="mb-row" data-mbid="${_mbEsc(row.mbid)}" onclick="_mbOpenDetail('${_mbEsc(row.entity_type)}','${safeId}')" style="padding:0.55rem 0.7rem;border-bottom:1px solid var(--border);cursor:pointer">
        <div style="font-weight:600">${saveBtn}${lookupBtn}${_mbEsc(row.name || "(untitled)")}</div>
        <div style="color:var(--muted);font-size:0.78rem">${sub.join(" · ")}</div>
      </div>
    `;
  }).join("");
}
window._mbRenderSaved = _mbRenderSaved;
