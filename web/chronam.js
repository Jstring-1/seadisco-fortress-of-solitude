// ── Chronicling America (LOC historic newspapers) view ──────────────────
//
// Discovery tab over the chroniclingamerica.loc.gov public JSON API.
// Mirrors the Gutenberg view structurally: two tabs (Search / Saved),
// shared saved-search bookmark dropdown, per-card save toggle. Server
// gates access to admin/demo for now (requireChronAmAccess).
//
// All endpoints live under /api/chronam/* on the server.

let _chronamSavedIds = null;   // Set<string> — chronam_id strings the user has saved
let _chronamLastQuery = "";
let _chronamLastPage  = 1;
let _chronamLastDate1 = "";
let _chronamLastDate2 = "";
// Map from chronam_id → full item so the popup can render rich detail
// from a card click without re-fetching. Populated as results render
// (search + saved tabs) and read by openChronAmPopup.
const _chronamItemsById = new Map();

function initChronAmView() {
  const view = document.getElementById("chronam-view");
  if (!view) return;
  if (view.dataset.initialized) return;
  view.dataset.initialized = "1";

  // Pre-fetch the user's save ids so the ★/☆ state is correct on first
  // search render. Anon users skip (gate would 401 anyway).
  if (window._clerk?.user) {
    _chronamLoadSavedIds().catch(() => {});
  }

  // Saved-search bookmark dropdown — same shared widget the rest of
  // the site uses. Persists q + date range under the "chronam" view.
  if (typeof buildSavedSearchUI === "function" && window._clerk?.user) {
    const formRow = view.querySelector(".chronam-form-row");
    if (formRow && !formRow.querySelector(".saved-search-wrap")) {
      buildSavedSearchUI(
        "chronam",
        () => {
          const q     = document.getElementById("chronam-q")?.value?.trim()     ?? "";
          const date1 = document.getElementById("chronam-date1")?.value?.trim() ?? "";
          const date2 = document.getElementById("chronam-date2")?.value?.trim() ?? "";
          const out = {};
          if (q)     out.q     = q;
          if (date1) out.date1 = date1;
          if (date2) out.date2 = date2;
          return out;
        },
        (params) => {
          const qEl = document.getElementById("chronam-q");
          const d1  = document.getElementById("chronam-date1");
          const d2  = document.getElementById("chronam-date2");
          if (qEl) qEl.value = params.q     ?? "";
          if (d1)  d1.value  = params.date1 ?? "";
          if (d2)  d2.value  = params.date2 ?? "";
          _chronamSwitchTab("search");
          runChronAmSearch(params.q ?? "");
        },
        formRow,
      );
    }
  }
}
window.initChronAmView = initChronAmView;

// ── Tab switching ───────────────────────────────────────────────────
function _chronamSwitchTab(tab) {
  const view = document.getElementById("chronam-view");
  if (!view) return;
  view.querySelectorAll(".loc-tab").forEach(b => {
    const tabKey = b.classList.contains("loc-tab-search") ? "search"
                 : b.classList.contains("loc-tab-saved")  ? "saved"
                 : "";
    b.classList.toggle("active", tabKey === tab);
  });
  const psearch = view.querySelector(".chronam-panel-search");
  const psaved  = view.querySelector(".chronam-panel-saved");
  if (psearch) psearch.style.display = tab === "search" ? "block" : "none";
  if (psaved)  psaved.style.display  = tab === "saved"  ? "block" : "none";
  if (tab === "saved") _chronamRenderSaved();
}
window._chronamSwitchTab = _chronamSwitchTab;

// ── Search ──────────────────────────────────────────────────────────
async function runChronAmSearch(q, opts) {
  const append = !!opts?.append;
  const query = String(q ?? "").trim();
  const date1 = document.getElementById("chronam-date1")?.value?.trim() ?? "";
  const date2 = document.getElementById("chronam-date2")?.value?.trim() ?? "";
  const target = document.getElementById("chronam-results");
  if (!target) return;

  if (!query) {
    target.innerHTML = `<div class="loc-empty">Type a name, place, or phrase to search historic American newspapers (1777–1963).</div>`;
    document.getElementById("chronam-pagination").innerHTML = "";
    return;
  }

  // Persist + signal the search-history dropdown.
  if (typeof saveSearchHistory === "function") {
    try { saveSearchHistory("chronam"); } catch {}
  }

  const sort = document.getElementById("chronam-sort")?.value || "relevance";
  const page = append ? _chronamLastPage + 1 : 1;
  if (!append) target.innerHTML = `<div class="loc-empty">Searching Chronicling America…</div>`;

  try {
    const params = new URLSearchParams({ q: query, page: String(page) });
    if (/^\d{4}$/.test(date1)) params.set("date1", date1);
    if (/^\d{4}$/.test(date2)) params.set("date2", date2);
    if (sort && sort !== "relevance") params.set("sort", sort);
    const r = await apiFetch(`/api/chronam/search?${params.toString()}`);
    if (!r.ok) {
      target.innerHTML = `<div class="loc-empty">Search failed (HTTP ${r.status}).</div>`;
      return;
    }
    const j = await r.json();
    const items = Array.isArray(j.items) ? j.items : [];
    _chronamLastQuery = query;
    _chronamLastPage  = page;
    _chronamLastDate1 = date1;
    _chronamLastDate2 = date2;

    if (!items.length && !append) {
      target.innerHTML = `<div class="loc-empty">No results for "${escHtml(query)}".</div>`;
      document.getElementById("chronam-pagination").innerHTML = "";
      return;
    }

    // Populate the popup lookup before rendering so click handlers can
    // resolve the id back to a full item.
    for (const it of items) {
      if (it?.id) _chronamItemsById.set(it.id, it);
    }
    const html = items.map(it => _chronamCardHtml(it)).join("");
    if (append) {
      // Drop pagination placeholder if any, then append.
      target.insertAdjacentHTML("beforeend", html);
    } else {
      target.innerHTML = html;
    }
    _chronamRenderPagination(j);
  } catch (e) {
    console.warn("[chronam/search]", e);
    target.innerHTML = `<div class="loc-empty">Search failed.</div>`;
  }
}
window.runChronAmSearch = runChronAmSearch;

function _chronamRenderPagination(j) {
  const el = document.getElementById("chronam-pagination");
  if (!el) return;
  const total = Number(j?.totalItems) || 0;
  const per   = Number(j?.itemsPerPage) || 20;
  const page  = Number(j?.page) || _chronamLastPage;
  const more  = total > page * per;
  el.innerHTML = more
    ? `<button type="button" class="loc-submit" onclick="runChronAmSearch(document.getElementById('chronam-q').value,{append:true})">Load more</button>
       <span class="chronam-page-info" style="color:var(--muted);margin-left:0.8rem;font-size:0.85rem">Page ${page} · ${total.toLocaleString()} hits</span>`
    : (total ? `<span class="chronam-page-info" style="color:var(--muted);font-size:0.85rem">${total.toLocaleString()} hit${total === 1 ? "" : "s"}</span>` : "");
}

// ── Card markup ─────────────────────────────────────────────────────
function _chronamCardHtml(it) {
  const idAttr = JSON.stringify(it.id || "").replace(/"/g, "&quot;");
  const saved  = _chronamSavedIds?.has(it.id);
  const star   = saved ? "★" : "☆";
  const starTitle = saved ? "Remove from Saved" : "Save this page";
  const place = [it.city, it.state].filter(Boolean).join(" · ");
  const snippet = (it.ocr_eng || "")
    .replace(/\s+/g, " ")
    .slice(0, 360);
  const thumb = it.thumb_url
    ? `<img class="chronam-thumb" src="${escHtml(it.thumb_url)}" alt="" loading="lazy" onerror="this.classList.add('chronam-thumb-broken');this.onerror=null">`
    : `<div class="chronam-thumb chronam-thumb-empty">📰</div>`;
  // Display the page URL host below the title so users have a visible
  // indicator that the link goes off-site (and which site).
  let hostLabel = "";
  try { hostLabel = new URL(it.page_url).host.replace(/^www\./, ""); } catch {}
  const viewBtn = it.page_url
    ? `<a href="${escHtml(it.page_url)}" target="_blank" rel="noopener noreferrer" class="chronam-open-link" title="Open original page on LOC.gov">View ↗</a>`
    : "";
  // Card title + thumb open the in-app popup (full page image + meta);
  // the "View ↗" pill still routes to LoC.gov for the original.
  return `
    <div class="chronam-card" data-chronam-id="${escHtml(it.id || "")}">
      <div class="chronam-thumb-wrap" onclick="openChronAmPopup(${idAttr})" style="cursor:pointer" title="Open page">
        ${thumb}
      </div>
      <div class="chronam-card-body">
        <div class="chronam-card-head">
          <a href="#" onclick="event.preventDefault();openChronAmPopup(${idAttr})" class="chronam-title">${escHtml(it.title || "(untitled)")}</a>
          <div class="chronam-card-actions">
            ${viewBtn}
            <button type="button" class="archive-btn chronam-save-btn${saved ? " is-saved" : ""}"
                    onclick="_chronamToggleSave(this, ${idAttr})" title="${starTitle}">${star}</button>
          </div>
        </div>
        <div class="chronam-meta">
          ${it.date ? `<span class="chronam-date">${escHtml(it.date)}</span>` : ""}
          ${place ? `<span class="chronam-place">${escHtml(place)}</span>` : ""}
          ${it.sequence ? `<span class="chronam-seq">Page ${escHtml(String(it.sequence))}</span>` : ""}
          ${hostLabel ? `<span class="chronam-host" title="${escHtml(it.page_url)}">${escHtml(hostLabel)}</span>` : ""}
        </div>
        ${snippet ? `<div class="chronam-snippet">${escHtml(snippet)}${(it.ocr_eng || "").length > 360 ? "…" : ""}</div>` : ""}
      </div>
    </div>`;
}

// ── In-app page popup ───────────────────────────────────────────────
function openChronAmPopup(id) {
  const it = _chronamItemsById.get(id);
  if (!it) return;
  const overlay = document.getElementById("chronam-popup-overlay");
  const content = document.getElementById("chronam-popup-content");
  if (!overlay || !content) return;

  const place = [it.city, it.state].filter(Boolean).join(" · ");
  const idAttr = JSON.stringify(it.id || "").replace(/"/g, "&quot;");
  const saved  = _chronamSavedIds?.has(it.id);
  const star   = saved ? "★" : "☆";
  const starTitle = saved ? "Remove from Saved" : "Save this page";

  // Prefer the largest available IIIF image; fall back to thumb.
  const imgSrc = it.thumb_url || "";

  content.innerHTML = `
    <div class="chronam-popup-head">
      <div class="chronam-popup-head-text">
        <h2 class="chronam-popup-title">${escHtml(it.title || "(untitled)")}</h2>
        <div class="chronam-popup-meta">
          ${it.date ? `<span>${escHtml(it.date)}</span>` : ""}
          ${place ? `<span>${escHtml(place)}</span>` : ""}
          ${it.sequence ? `<span>Page ${escHtml(String(it.sequence))}</span>` : ""}
        </div>
      </div>
      <div class="chronam-popup-actions">
        ${it.page_url ? `<a href="${escHtml(it.page_url)}" target="_blank" rel="noopener noreferrer" class="chronam-open-link">View on LOC ↗</a>` : ""}
        <button type="button" class="archive-btn chronam-save-btn${saved ? " is-saved" : ""}"
                onclick="_chronamToggleSave(this, ${idAttr})" title="${starTitle}">${star}</button>
        <button type="button" class="archive-btn" onclick="closeChronAmPopup()" title="Close">✕</button>
      </div>
    </div>
    <div class="chronam-popup-image-wrap">
      ${imgSrc
        ? `<img class="chronam-popup-image" src="${escHtml(imgSrc)}" alt="Newspaper page scan" onerror="this.replaceWith(Object.assign(document.createElement('div'),{className:'chronam-popup-image-fail',textContent:'Image unavailable. Use \\'View on LOC\\' to see the page.'}))">`
        : `<div class="chronam-popup-image-fail">No image available. Use "View on LOC" to see the page.</div>`}
    </div>
    ${it.ocr_eng ? `<div class="chronam-popup-snippet"><strong>OCR snippet</strong><br>${escHtml(it.ocr_eng)}</div>` : ""}
  `;
  overlay.classList.add("open");
  if (typeof _sdLockBodyScroll === "function") _sdLockBodyScroll("chronam-popup");
}
window.openChronAmPopup = openChronAmPopup;

function closeChronAmPopup() {
  const overlay = document.getElementById("chronam-popup-overlay");
  if (overlay) overlay.classList.remove("open");
  if (typeof _sdUnlockBodyScroll === "function") _sdUnlockBodyScroll("chronam-popup");
}
window.closeChronAmPopup = closeChronAmPopup;

// ── Save toggle ─────────────────────────────────────────────────────
async function _chronamLoadSavedIds() {
  try {
    const r = await apiFetch("/api/chronam/saves/ids");
    if (!r.ok) return;
    const j = await r.json();
    _chronamSavedIds = new Set(Array.isArray(j.ids) ? j.ids : []);
  } catch { /* best effort */ }
}

async function _chronamToggleSave(btn, id) {
  if (!id) return;
  if (!window._clerk?.user) {
    if (typeof showToast === "function") showToast("Sign in to save pages", "error");
    return;
  }
  if (!_chronamSavedIds) _chronamSavedIds = new Set();
  const wasSaved = _chronamSavedIds.has(id);
  // Optimistic toggle.
  btn.classList.toggle("is-saved", !wasSaved);
  btn.textContent = wasSaved ? "☆" : "★";
  btn.title = wasSaved ? "Save this page" : "Remove from Saved";
  if (wasSaved) _chronamSavedIds.delete(id);
  else _chronamSavedIds.add(id);

  try {
    if (wasSaved) {
      const r = await apiFetch(`/api/chronam/save?id=${encodeURIComponent(id)}`, { method: "DELETE" });
      if (!r.ok) throw new Error(`delete failed (${r.status})`);
      if (typeof showToast === "function") showToast("Removed from Saved");
    } else {
      // Pull the card's data from the DOM for the snapshot fields. The
      // server is happy with just `id`; we fill what we can so the
      // Saved tab can render without a re-fetch.
      const card = btn.closest(".chronam-card");
      const titleEl = card?.querySelector(".chronam-title");
      const dateEl  = card?.querySelector(".chronam-date");
      const snipEl  = card?.querySelector(".chronam-snippet");
      const thumbEl = card?.querySelector(".chronam-thumb");
      const body = {
        id,
        paperTitle: titleEl?.textContent || null,
        issueDate:  dateEl?.textContent  || null,
        snippet:    snipEl?.textContent  || null,
        thumbnail:  thumbEl?.getAttribute("src") || null,
        data: {
          page_url: titleEl?.getAttribute("href") || "",
        },
      };
      const r = await apiFetch("/api/chronam/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!r.ok) throw new Error(`save failed (${r.status})`);
      if (typeof showToast === "function") showToast("Saved");
    }
  } catch (e) {
    console.warn("[chronam/toggleSave]", e);
    // Revert optimistic toggle.
    btn.classList.toggle("is-saved", wasSaved);
    btn.textContent = wasSaved ? "★" : "☆";
    btn.title = wasSaved ? "Remove from Saved" : "Save this page";
    if (wasSaved) _chronamSavedIds.add(id);
    else _chronamSavedIds.delete(id);
    if (typeof showToast === "function") showToast(e?.message || "Could not update save", "error");
  }
}
window._chronamToggleSave = _chronamToggleSave;

// ── Saved tab render ────────────────────────────────────────────────
async function _chronamRenderSaved() {
  const target = document.getElementById("chronam-saved-results");
  const countEl = document.getElementById("chronam-saved-count");
  if (!target) return;
  target.innerHTML = `<div class="loc-empty">Loading saved pages…</div>`;
  try {
    const r = await apiFetch("/api/chronam/saves");
    if (!r.ok) {
      target.innerHTML = `<div class="loc-empty">Could not load saved pages (HTTP ${r.status}).</div>`;
      return;
    }
    const j = await r.json();
    const items = Array.isArray(j.items) ? j.items : [];
    if (countEl) countEl.textContent = String(items.length);
    if (!items.length) {
      target.innerHTML = `<div class="loc-empty">No saved pages yet. Use ☆ on a search result to save it.</div>`;
      return;
    }
    // Keep the save-id set in sync so the ★ state on the Search tab is
    // correct after a delete from Saved.
    if (!_chronamSavedIds) _chronamSavedIds = new Set();
    items.forEach(i => _chronamSavedIds.add(i.id));
    // Reshape saved items to look like search items so the same card
    // template can render both.
    const reshaped = items.map(s => ({
      id: s.id,
      title: s.paperTitle || "",
      date:  s.issueDate || "",
      city:  "",
      state: "",
      sequence: null,
      ocr_eng: s.snippet || "",
      page_url: s.data?.page_url || `https://www.loc.gov${s.id}`,
      thumb_url: s.thumbnail || "",
    }));
    for (const it of reshaped) {
      if (it.id) _chronamItemsById.set(it.id, it);
    }
    target.innerHTML = reshaped.map(_chronamCardHtml).join("");
  } catch (e) {
    console.warn("[chronam/saved]", e);
    target.innerHTML = `<div class="loc-empty">Could not load saved pages.</div>`;
  }
}
