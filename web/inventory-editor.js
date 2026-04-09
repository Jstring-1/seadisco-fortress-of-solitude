// ── Marketplace listing editor (create / edit / delete) ──────────────────
// Opens a modal to create a new listing or edit an existing one. Uses the
// shared modal-overlay pattern. All field names mirror the Discogs API.

const INV_CONDITIONS = ["Mint (M)", "Near Mint (NM or M-)", "Very Good Plus (VG+)", "Very Good (VG)", "Good Plus (G+)", "Good (G)", "Fair (F)", "Poor (P)"];
const INV_SLEEVE_CONDITIONS = [...INV_CONDITIONS, "Generic", "Not Graded", "No Cover"];
const INV_STATUSES = ["For Sale", "Draft", "Expired"];
const INV_CURRENCIES = ["USD", "GBP", "EUR", "CAD", "AUD", "JPY", "CHF", "MXN", "BRL", "NZD", "SEK", "ZAR"];

// Persist a session flag when Discogs rejects a marketplace write as
// unauthorized. The account page reads this on load and shows a
// reconnect banner. A toast also surfaces the issue immediately for
// users who aren't on the account page.
function _invFlagScopeIssue() {
  try { sessionStorage.setItem("marketplaceScopeIssue", "1"); } catch {}
  showToast?.("Your Discogs connection doesn't have marketplace permission. Reconnect from the Account page.", "error", 6000);
  // If the account page is currently mounted, render the banner inline.
  if (typeof renderMarketplaceScopeBanner === "function") renderMarketplaceScopeBanner();
}

function _invShortTitle(data) {
  const rel = data?.release || {};
  const artist = rel.artist || rel.description || "";
  const title = rel.title || rel.description || `Release ${rel.id ?? ""}`;
  return artist ? `${artist} — ${title}` : title;
}

let _invEditorState = null; // { mode: 'create'|'edit', listingId, releaseId, data }

function closeInventoryEditor() {
  const overlay = document.getElementById("inventory-editor-overlay");
  if (overlay) overlay.remove();
  document.removeEventListener("keydown", _invEscHandler);
  _invEditorState = null;
}

// Snapshot the current form state. Used for dirty-state tracking so we can
// warn before discarding unsaved changes.
function _invSnapshot() {
  try { return JSON.stringify(_invCollectFromEditor()); } catch { return ""; }
}

function _invIsDirty() {
  if (!_invEditorState || !_invEditorState.originalSnapshot) return false;
  return _invSnapshot() !== _invEditorState.originalSnapshot;
}

// Wrapper used by Cancel button / outside-click / Esc. Prompts if dirty.
async function _invRequestClose() {
  if (!_invIsDirty()) { closeInventoryEditor(); return; }
  const ok = window.confirm("You have unsaved changes. Discard them?");
  if (ok) closeInventoryEditor();
}

/**
 * Open the editor.
 * opts: { mode: 'create'|'edit', listingId?, releaseId?, prefill?: { condition, sleeveCondition, price, priceCurrency, status, comments, allowOffers, location, weight, formatQuantity, externalId, releaseTitle } }
 */
async function openInventoryEditor(opts = {}) {
  closeInventoryEditor();
  const mode = opts.mode === "edit" ? "edit" : "create";
  let prefill = opts.prefill || {};
  let releaseId = Number(opts.releaseId) || null;
  let releaseTitle = prefill.releaseTitle || "";
  let listingId = Number(opts.listingId) || null;

  // Edit mode: pull the full listing from the server
  if (mode === "edit" && listingId) {
    try {
      const r = await apiFetch(`/api/user/inventory/${listingId}`);
      if (!r.ok) throw new Error("Failed to load listing");
      const j = await r.json();
      const it = j.item || {};
      const d = it.data || {};
      releaseId = Number(it.discogs_release_id || d.release?.id);
      releaseTitle = _invShortTitle(d);
      prefill = {
        condition:        it.condition || d.condition || "",
        sleeveCondition:  it.sleeve_condition || d.sleeve_condition || "",
        price:            it.price_value || d.price?.value || "",
        priceCurrency:    it.price_currency || d.price?.currency || window._userCurrency || "USD",
        status:           it.status || d.status || "For Sale",
        comments:         d.comments || "",
        allowOffers:      !!d.allow_offers,
        location:         d.location || "",
        weight:           d.weight ?? "",
        formatQuantity:   d.format_quantity ?? "",
        externalId:       d.external_id || "",
      };
    } catch (e) {
      showToast?.("Could not load listing: " + e.message, "error");
      return;
    }
  }

  _invEditorState = { mode, listingId, releaseId, data: null };

  const overlay = document.createElement("div");
  overlay.id = "inventory-editor-overlay";
  overlay.innerHTML = `
    <div id="inventory-editor-panel" role="dialog" aria-modal="true" aria-label="${mode === "edit" ? "Edit listing" : "New listing"}">
      <button type="button" class="inv-editor-close" onclick="_invRequestClose()" aria-label="Close">×</button>
      <h2 class="inv-editor-title">${mode === "edit" ? "Edit listing" : "New listing"}</h2>
      <div class="inv-editor-release">
        ${releaseId
          ? `<div class="inv-editor-release-name">${escHtml(releaseTitle || `Release ${releaseId}`)}</div>
             <div class="inv-editor-release-id">Discogs release ID: <strong>${releaseId}</strong></div>`
          : `<label class="inv-editor-label">Release
               <input type="text" id="inv-release-search" placeholder="Search by artist + title, or paste a release ID / URL" autocomplete="off"/>
             </label>
             <div id="inv-release-results" class="inv-release-results" style="display:none"></div>
             <input type="hidden" id="inv-release-id"/>
             <div id="inv-release-chosen" class="inv-editor-release-id" style="display:none"></div>`}
      </div>

      <div class="inv-editor-grid">
        <label class="inv-editor-label">Media condition *
          <select id="inv-condition">${INV_CONDITIONS.map(c => `<option${c === prefill.condition ? " selected" : ""}>${c}</option>`).join("")}</select>
        </label>
        <label class="inv-editor-label">Sleeve condition *
          <select id="inv-sleeve">${INV_SLEEVE_CONDITIONS.map(c => `<option${c === prefill.sleeveCondition ? " selected" : ""}>${c}</option>`).join("")}</select>
        </label>
        <label class="inv-editor-label">Price *
          <div class="inv-price-row">
            <input type="number" step="0.01" min="0" id="inv-price" value="${prefill.price ?? ""}"/>
            <select id="inv-currency">${INV_CURRENCIES.map(c => `<option${c === (prefill.priceCurrency || window._userCurrency || "USD") ? " selected" : ""}>${c}</option>`).join("")}</select>
            <button type="button" class="inv-editor-suggest" onclick="_invGetPriceSuggestions()" title="Show Discogs median price suggestions for this release">Suggest price</button>
          </div>
        </label>
        <label class="inv-editor-label">Status *
          <select id="inv-status">${INV_STATUSES.map(s => `<option${s === (prefill.status || "For Sale") ? " selected" : ""}>${s}</option>`).join("")}</select>
        </label>
        <label class="inv-editor-label inv-editor-wide">Comments
          <textarea id="inv-comments" rows="3" placeholder="Any condition notes, extras, etc.">${escHtml(prefill.comments || "")}</textarea>
        </label>
        <label class="inv-editor-label inv-editor-checkbox">
          <input type="checkbox" id="inv-allow-offers" ${prefill.allowOffers ? "checked" : ""}/>
          Allow offers
        </label>
        <label class="inv-editor-label">Location
          <input type="text" id="inv-location" value="${escHtml(prefill.location || "")}" placeholder="Shelf / bin"/>
        </label>
        <label class="inv-editor-label">Weight (grams, or "auto")
          <input type="text" id="inv-weight" value="${escHtml(prefill.weight ?? "")}" placeholder="auto"/>
        </label>
        <label class="inv-editor-label">Format quantity
          <input type="number" min="1" id="inv-format-qty" value="${escHtml(prefill.formatQuantity ?? "")}"/>
        </label>
        <label class="inv-editor-label">External ID
          <input type="text" id="inv-external-id" value="${escHtml(prefill.externalId || "")}"/>
        </label>
      </div>

      <div id="inv-price-suggestions" class="inv-price-suggestions" style="display:none"></div>
      <div id="inv-editor-error" class="inv-editor-error" style="display:none"></div>

      <div class="inv-editor-actions">
        ${mode === "edit" ? `<button type="button" class="inv-editor-delete" onclick="_invDeleteFromEditor()">Delete listing</button>` : ""}
        <span style="flex:1"></span>
        <button type="button" class="inv-editor-cancel" onclick="_invRequestClose()">Cancel</button>
        <button type="button" class="inv-editor-save" onclick="_invSaveFromEditor()">${mode === "edit" ? "Save changes" : "Create listing"}</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);

  // Click outside to dismiss (prompts if there are unsaved changes)
  overlay.addEventListener("click", (ev) => {
    if (ev.target === overlay) _invRequestClose();
  });
  document.addEventListener("keydown", _invEscHandler);

  // Capture the initial form state *after* the DOM is rendered so we can
  // detect unsaved changes on close.
  try {
    if (_invEditorState) _invEditorState.originalSnapshot = _invSnapshot();
  } catch {}

  // Wire up the release-search input in create mode
  const searchInp = document.getElementById("inv-release-search");
  if (searchInp) {
    let tId;
    searchInp.addEventListener("input", () => {
      clearTimeout(tId);
      const raw = searchInp.value.trim();
      if (!raw) { _invRenderReleaseResults([]); return; }
      // Paste support: bare numeric ID, or a discogs.com release URL
      const urlMatch = raw.match(/discogs\.com\/(?:[^/]+\/)?release\/(\d+)/i) || raw.match(/\[r(\d+)\]/i);
      if (urlMatch) { _invPickRelease({ id: Number(urlMatch[1]), title: `Release ${urlMatch[1]}` }); return; }
      if (/^\d+$/.test(raw)) { _invPickRelease({ id: Number(raw), title: `Release ${raw}` }); return; }
      tId = setTimeout(() => _invDoReleaseSearch(raw), 350);
    });
  }
}

async function _invDoReleaseSearch(q) {
  const box = document.getElementById("inv-release-results");
  if (!box) return;
  box.style.display = "block";
  box.innerHTML = `<div class="inv-release-loading">Searching…</div>`;
  try {
    const r = await apiFetch(`/search?q=${encodeURIComponent(q)}&type=release&per_page=8`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const results = (data.results || []).slice(0, 8);
    _invRenderReleaseResults(results);
  } catch (e) {
    box.innerHTML = `<div class="inv-release-loading">Search failed</div>`;
  }
}

function _invRenderReleaseResults(results) {
  const box = document.getElementById("inv-release-results");
  if (!box) return;
  if (!results.length) { box.style.display = "none"; box.innerHTML = ""; return; }
  box.style.display = "block";
  box.innerHTML = results.map(r => {
    const id = r.id;
    const title = r.title || `Release ${id}`;
    const thumb = r.thumb || r.cover_image || "";
    const year = r.year || "";
    const format = Array.isArray(r.format) ? r.format.slice(0, 2).join(", ") : "";
    const label = Array.isArray(r.label) ? r.label[0] : "";
    const meta = [year, format, label].filter(Boolean).join(" · ");
    const payload = encodeURIComponent(JSON.stringify({ id, title }));
    return `<div class="inv-release-result" onclick="_invPickRelease(JSON.parse(decodeURIComponent('${payload}')))">
      ${thumb ? `<img src="${thumb}" alt=""/>` : `<div class="inv-release-thumb-ph"></div>`}
      <div class="inv-release-info">
        <div class="inv-release-title">${escHtml(title)}</div>
        <div class="inv-release-meta">${escHtml(meta)} · #${id}</div>
      </div>
    </div>`;
  }).join("");
}

function _invPickRelease(release) {
  if (!_invEditorState) return;
  _invEditorState.releaseId = Number(release.id);
  const hidden = document.getElementById("inv-release-id");
  if (hidden) hidden.value = String(release.id);
  const searchInp = document.getElementById("inv-release-search");
  if (searchInp) searchInp.value = release.title || `Release ${release.id}`;
  const box = document.getElementById("inv-release-results");
  if (box) { box.style.display = "none"; box.innerHTML = ""; }
  const chosen = document.getElementById("inv-release-chosen");
  if (chosen) { chosen.style.display = "block"; chosen.innerHTML = `Selected: <strong>${escHtml(release.title || "")}</strong> (#${release.id})`; }
}

// In-modal delete confirmation. Returns a Promise<boolean>.
// Safety features to prevent accidental listing deletion:
//   1. Enter key does NOT trigger delete (Enter = Cancel, the safe default).
//   2. The Delete button is disabled for 1.2 s after open to prevent
//      reflexive double-clicks / spacebar presses from blasting through.
//   3. The dialog shows the exact release title and price that will be
//      removed so the user can verify they're deleting the right thing.
function _invConfirmDelete(context) {
  return new Promise((resolve) => {
    const existing = document.getElementById("inv-confirm-overlay");
    if (existing) existing.remove();
    const ov = document.createElement("div");
    ov.id = "inv-confirm-overlay";
    const title = context?.title ? escHtml(context.title) : "this listing";
    const price = context?.price ? ` for <strong>${escHtml(context.price)}</strong>` : "";
    ov.innerHTML = `
      <div id="inv-confirm-panel" role="dialog" aria-modal="true" aria-labelledby="inv-confirm-heading">
        <h3 id="inv-confirm-heading" style="margin:0 0 0.5rem 0">Delete this listing?</h3>
        <p style="margin:0 0 0.6rem 0;font-size:0.9rem">
          You are about to delete <strong>${title}</strong>${price}.
        </p>
        <p style="margin:0 0 1rem 0;font-size:0.82rem;color:#e8a020">
          ⚠ This permanently removes the listing from the Discogs marketplace and cannot be undone. If a buyer is currently viewing this listing, it will disappear for them too.
        </p>
        <div style="display:flex;justify-content:flex-end;gap:0.5rem">
          <button type="button" class="inv-editor-cancel" id="inv-confirm-cancel" autofocus>Cancel</button>
          <button type="button" class="inv-editor-delete" id="inv-confirm-ok" disabled>Delete listing</button>
        </div>
      </div>
    `;
    document.body.appendChild(ov);
    const done = (val) => { ov.remove(); document.removeEventListener("keydown", onKey); resolve(val); };
    // Enter is deliberately mapped to Cancel (safe default). Escape cancels.
    const onKey = (ev) => {
      if (ev.key === "Escape" || ev.key === "Enter") { ev.preventDefault(); done(false); }
    };
    document.addEventListener("keydown", onKey);
    ov.addEventListener("click", (ev) => { if (ev.target === ov) done(false); });
    const okBtn = document.getElementById("inv-confirm-ok");
    const cancelBtn = document.getElementById("inv-confirm-cancel");
    cancelBtn.onclick = () => done(false);
    okBtn.onclick = () => done(true);
    // Enforce a short delay before the Delete button becomes clickable so
    // the user can't blast through this dialog by reflex.
    setTimeout(() => { if (okBtn) okBtn.disabled = false; }, 1200);
    try { cancelBtn.focus(); } catch {}
  });
}

function _invEscHandler(ev) {
  if (ev.key === "Escape") {
    // Don't intercept Escape while the delete-confirmation dialog is open;
    // that dialog has its own handler and should close itself first.
    if (document.getElementById("inv-confirm-overlay")) return;
    _invRequestClose();
  }
}

function _invCollectFromEditor() {
  const rid = _invEditorState?.releaseId || Number(document.getElementById("inv-release-id")?.value);
  return {
    releaseId:       rid || null,
    condition:       document.getElementById("inv-condition")?.value || "",
    sleeveCondition: document.getElementById("inv-sleeve")?.value || "",
    price:           Number(document.getElementById("inv-price")?.value),
    priceCurrency:   document.getElementById("inv-currency")?.value || window._userCurrency || "USD",
    status:          document.getElementById("inv-status")?.value || "For Sale",
    comments:        document.getElementById("inv-comments")?.value || "",
    allowOffers:     !!document.getElementById("inv-allow-offers")?.checked,
    location:        document.getElementById("inv-location")?.value || "",
    weight:          document.getElementById("inv-weight")?.value || "",
    formatQuantity:  document.getElementById("inv-format-qty")?.value || null,
    externalId:      document.getElementById("inv-external-id")?.value || "",
  };
}

function _invShowError(msg) {
  const el = document.getElementById("inv-editor-error");
  if (!el) return;
  el.textContent = msg;
  el.style.display = "block";
}

async function _invSaveFromEditor() {
  if (!_invEditorState) return;
  const body = _invCollectFromEditor();
  if (!body.releaseId)     return _invShowError("Release ID required");
  if (!body.condition)     return _invShowError("Media condition required");
  if (!body.sleeveCondition) return _invShowError("Sleeve condition required");
  if (!body.price || body.price <= 0) return _invShowError("Price must be > 0");

  // Discogs API expects price as a number; currency is set on the seller profile,
  // but our editor lets the user confirm/override it. Price value sent as-is.
  // Normalize weight/formatQuantity (blank → omit)
  if (body.weight === "") delete body.weight;
  if (body.formatQuantity == null || body.formatQuantity === "") delete body.formatQuantity;

  const saveBtn = document.querySelector(".inv-editor-save");
  if (saveBtn) { saveBtn.disabled = true; saveBtn.textContent = "Saving…"; }

  try {
    const url = _invEditorState.mode === "edit"
      ? `/api/user/inventory/${_invEditorState.listingId}`
      : "/api/user/inventory/create";
    const r = await apiFetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (r.status === 401 || r.status === 403) _invFlagScopeIssue();
      _invShowError(j.error || `Save failed (${r.status})`);
      if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = _invEditorState.mode === "edit" ? "Save changes" : "Create listing"; }
      return;
    }
    closeInventoryEditor();
    showToast?.(_invEditorState?.mode === "edit" ? "Listing updated" : "Listing created", "success");
    // Refresh the inventory tab if visible
    if (typeof loadInventoryTab === "function" && document.querySelector(".nav-rtab.active")?.dataset?.rtab === "inventory") {
      loadInventoryTab(_invPage || 1);
    }
    // Refresh the cached listing-id map
    if (typeof loadDiscogsIds === "function") loadDiscogsIds();
  } catch (e) {
    _invShowError(String(e));
    if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = "Save changes"; }
  }
}

async function _invDeleteFromEditor() {
  if (!_invEditorState || _invEditorState.mode !== "edit") return;
  // Build a human-readable context for the confirm dialog so the user
  // sees exactly which release is about to be deleted, and for how much.
  const titleEl = document.querySelector(".inv-editor-release-name");
  const priceInput = document.getElementById("inv-price");
  const currSel = document.getElementById("inv-currency");
  const ctx = {
    title: titleEl?.textContent || "",
    price: (priceInput?.value && currSel?.value)
      ? `${currSel.value} ${Number(priceInput.value).toFixed(2)}`
      : "",
  };
  const confirmed = await _invConfirmDelete(ctx);
  if (!confirmed) return;
  try {
    const r = await apiFetch(`/api/user/inventory/${_invEditorState.listingId}`, { method: "DELETE" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (r.status === 401 || r.status === 403) _invFlagScopeIssue();
      _invShowError(j.error || `Delete failed (${r.status})`);
      return;
    }
    closeInventoryEditor();
    showToast?.("Listing deleted", "success");
    if (typeof loadInventoryTab === "function" && document.querySelector(".nav-rtab.active")?.dataset?.rtab === "inventory") {
      loadInventoryTab(_invPage || 1);
    }
    if (typeof loadDiscogsIds === "function") loadDiscogsIds();
  } catch (e) {
    _invShowError(String(e));
  }
}

async function _invGetPriceSuggestions() {
  const rid = _invEditorState?.releaseId || Number(document.getElementById("inv-release-id")?.value);
  if (!rid) { _invShowError("Enter a release ID first"); return; }
  const box = document.getElementById("inv-price-suggestions");
  if (box) { box.style.display = "block"; box.textContent = "Loading price suggestions…"; }
  try {
    const r = await apiFetch(`/api/user/inventory/price-suggestions/${rid}`);
    if (!r.ok) { box.textContent = "No price suggestions available"; return; }
    const j = await r.json();
    const s = j.suggestions || {};
    const rows = Object.entries(s).map(([cond, v]) => {
      const val = v?.value ? `${v.currency || "USD"} ${Number(v.value).toFixed(2)}` : "—";
      return `<div class="inv-ps-row"><span>${escHtml(cond)}</span><strong>${val}</strong></div>`;
    }).join("");
    box.innerHTML = rows || "No suggestions for this release";
  } catch (e) {
    box.textContent = "Could not load suggestions";
  }
}
