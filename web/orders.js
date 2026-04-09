// ── Seller Orders section on the Account page ────────────────────────────
// Loads a paginated list of seller orders from /api/user/orders and renders
// a compact list with status chips. Clicking an order opens a detail modal
// that shows items, buyer info, shipping, status controls and a message
// thread. All Discogs writes go through the local `/api/user/orders/*`
// endpoints which refresh the cache inline.

const ORDER_STATUSES = [
  "New Order", "Buyer Contacted", "Invoice Sent", "Payment Pending",
  "Payment Received", "In Progress", "Shipped", "Refund Sent", "Cancelled",
  "Cancelled (Non-Paying Buyer)", "Cancelled (Item Unavailable)", "Cancelled (Per Buyer's Request)",
];

// Valid next statuses by current status — mirrors Discogs' seller state
// machine (https://www.discogs.com/developers#page:marketplace,header:marketplace-order-changes).
// We always allow the current state itself (so the select renders) and the
// Cancelled family (always reachable before Shipped).
const ORDER_STATUS_TRANSITIONS = {
  "New Order":         ["New Order", "Buyer Contacted", "Invoice Sent", "Payment Pending", "Payment Received", "Cancelled (Non-Paying Buyer)", "Cancelled (Item Unavailable)", "Cancelled (Per Buyer's Request)"],
  "Buyer Contacted":   ["Buyer Contacted", "Invoice Sent", "Payment Pending", "Payment Received", "Cancelled (Non-Paying Buyer)", "Cancelled (Item Unavailable)", "Cancelled (Per Buyer's Request)"],
  "Invoice Sent":      ["Invoice Sent", "Payment Pending", "Payment Received", "Cancelled (Non-Paying Buyer)", "Cancelled (Item Unavailable)", "Cancelled (Per Buyer's Request)"],
  "Payment Pending":   ["Payment Pending", "Payment Received", "Cancelled (Non-Paying Buyer)", "Cancelled (Item Unavailable)", "Cancelled (Per Buyer's Request)"],
  "Payment Received":  ["Payment Received", "In Progress", "Shipped", "Refund Sent", "Cancelled (Item Unavailable)", "Cancelled (Per Buyer's Request)"],
  "In Progress":       ["In Progress", "Shipped", "Refund Sent", "Cancelled (Item Unavailable)", "Cancelled (Per Buyer's Request)"],
  "Shipped":           ["Shipped", "Refund Sent"],
  "Refund Sent":       ["Refund Sent"],
  "Cancelled":                            ["Cancelled"],
  "Cancelled (Non-Paying Buyer)":         ["Cancelled (Non-Paying Buyer)"],
  "Cancelled (Item Unavailable)":         ["Cancelled (Item Unavailable)"],
  "Cancelled (Per Buyer's Request)":      ["Cancelled (Per Buyer's Request)"],
};

function _validNextStatuses(current) {
  return ORDER_STATUS_TRANSITIONS[current] || ORDER_STATUSES;
}

let _ordersState = { page: 1, perPage: 20, status: "", q: "" };

function _statusChipClass(status) {
  const s = String(status || "").toLowerCase();
  if (s.includes("shipped")) return "ord-chip ord-chip-shipped";
  if (s.includes("cancel")) return "ord-chip ord-chip-cancelled";
  if (s.includes("payment received") || s.includes("in progress")) return "ord-chip ord-chip-progress";
  if (s.includes("payment pending") || s.includes("invoice") || s.includes("buyer contacted")) return "ord-chip ord-chip-pending";
  if (s.includes("refund")) return "ord-chip ord-chip-refund";
  return "ord-chip ord-chip-new";
}

async function loadOrdersSection() {
  const section = document.getElementById("orders-section");
  if (!section) return;
  try {
    const cr = await apiFetch("/api/user/orders/count");
    const cd = cr.ok ? await cr.json() : { count: 0 };
    if (!cd.count) { section.style.display = "none"; return; }
    section.style.display = "block";
    await Promise.all([_renderOrdersPage(), _refreshUnreadBadge()]);
  } catch {
    section.style.display = "none";
  }
}

async function _refreshUnreadBadge() {
  const badge = document.getElementById("orders-unread-badge");
  if (!badge) return;
  try {
    const r = await apiFetch("/api/user/orders/unread-count");
    if (!r.ok) { badge.style.display = "none"; return; }
    const j = await r.json();
    const n = Number(j.count || 0);
    if (n > 0) { badge.textContent = String(n); badge.style.display = "inline-flex"; }
    else { badge.style.display = "none"; }
  } catch { badge.style.display = "none"; }
}

async function _renderOrdersPage() {
  const listEl = document.getElementById("orders-list");
  const statusEl = document.getElementById("orders-sync-status");
  const filtersEl = document.getElementById("orders-filters");
  if (!listEl) return;
  listEl.innerHTML = `<div style="color:var(--muted);font-size:0.85rem">Loading orders…</div>`;

  // Render filter pills once
  if (filtersEl && !filtersEl.dataset.ready) {
    const pills = ["", "New Order", "Payment Received", "In Progress", "Shipped", "Cancelled"]
      .map(s => `<button class="ord-pill${s === _ordersState.status ? " ord-pill-active" : ""}" data-status="${escHtml(s)}">${s || "All"}</button>`).join("");
    filtersEl.innerHTML = pills + `<input type="text" id="orders-search" placeholder="Search buyer / title" style="margin-left:auto;padding:0.3rem 0.55rem;background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:5px;font-size:0.85rem"/>`;
    filtersEl.dataset.ready = "1";
    filtersEl.querySelectorAll(".ord-pill").forEach(btn => {
      btn.onclick = () => { _ordersState.status = btn.dataset.status || ""; _ordersState.page = 1; filtersEl.dataset.ready = ""; _renderOrdersPage(); };
    });
    const searchInp = document.getElementById("orders-search");
    if (searchInp) {
      searchInp.value = _ordersState.q;
      let t;
      searchInp.oninput = () => { clearTimeout(t); t = setTimeout(() => { _ordersState.q = searchInp.value.trim(); _ordersState.page = 1; _renderOrdersPage(); }, 300); };
    }
  }

  const params = new URLSearchParams({ page: String(_ordersState.page), per_page: String(_ordersState.perPage) });
  if (_ordersState.status) params.set("status", _ordersState.status);
  if (_ordersState.q) params.set("q", _ordersState.q);
  try {
    const r = await apiFetch(`/api/user/orders?${params}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    const items = d.items || [];
    if (statusEl) statusEl.textContent = `${d.total || 0} orders — page ${d.page || 1} of ${d.pages || 1}`;
    if (!items.length) { listEl.innerHTML = `<div style="color:var(--muted);font-size:0.85rem;padding:0.8rem 0">No orders found.</div>`; return; }
    const rows = items.map(it => {
      const data = it.data || {};
      const created = it.created_at ? new Date(it.created_at).toLocaleDateString() : "";
      const total = it.total_value ? `${it.total_currency || ""} ${Number(it.total_value).toFixed(2)}` : "";
      const itemCount = it.item_count ?? (data.items?.length || 0);
      const hasNew = !!it.has_new;
      const oid = escHtml(it.order_id);
      return `<div class="ord-row${hasNew ? " ord-row-unread" : ""}" data-oid="${oid}">
        <div class="ord-row-clickarea" onclick="openOrderDetail('${oid}')">
          <div class="ord-row-main">
            ${hasNew ? `<span class="ord-unread-dot" title="New activity"></span>` : ""}
            <span class="${_statusChipClass(it.status)}">${escHtml(it.status || "—")}</span>
            <strong class="ord-row-id">#${oid}</strong>
            <span class="ord-row-buyer">${escHtml(it.buyer_username || "")}</span>
          </div>
          <div class="ord-row-meta">
            <span>${itemCount} item${itemCount === 1 ? "" : "s"}</span>
            <span>${escHtml(total)}</span>
            <span>${escHtml(created)}</span>
          </div>
        </div>
        <div class="ord-row-quick">
          <button type="button" class="ord-quick-toggle" onclick="_ordToggleQuickReply('${oid}', event)" title="Quick reply">💬</button>
          <div class="ord-quick-panel" id="ord-quick-${oid}" style="display:none" onclick="event.stopPropagation()">
            <textarea rows="2" placeholder="Quick reply to ${escHtml(it.buyer_username || "buyer")}…"></textarea>
            <div class="ord-quick-actions">
              <button type="button" onclick="_ordQuickCancel('${oid}')">Cancel</button>
              <button type="button" class="ord-btn-primary" onclick="_ordQuickSend('${oid}')">Send</button>
            </div>
          </div>
        </div>
      </div>`;
    }).join("");
    // Pagination
    const pages = d.pages || 1;
    const pager = pages > 1 ? `<div class="ord-pager">
      <button ${_ordersState.page <= 1 ? "disabled" : ""} onclick="_ordersGoto(${_ordersState.page - 1})">← Prev</button>
      <span>Page ${_ordersState.page} / ${pages}</span>
      <button ${_ordersState.page >= pages ? "disabled" : ""} onclick="_ordersGoto(${_ordersState.page + 1})">Next →</button>
    </div>` : "";
    listEl.innerHTML = rows + pager;
  } catch (e) {
    listEl.innerHTML = `<div style="color:#e88">Failed to load orders: ${escHtml(e.message)}</div>`;
  }
}

function _ordersGoto(p) { _ordersState.page = p; _renderOrdersPage(); }

function _ordToggleQuickReply(orderId, ev) {
  if (ev) ev.stopPropagation();
  const panel = document.getElementById(`ord-quick-${orderId}`);
  if (!panel) return;
  const open = panel.style.display !== "none";
  document.querySelectorAll(".ord-quick-panel").forEach(p => { p.style.display = "none"; });
  if (!open) {
    panel.style.display = "block";
    const ta = panel.querySelector("textarea");
    if (ta) ta.focus();
  }
}

function _ordQuickCancel(orderId) {
  const panel = document.getElementById(`ord-quick-${orderId}`);
  if (!panel) return;
  panel.style.display = "none";
  const ta = panel.querySelector("textarea");
  if (ta) ta.value = "";
}

async function _ordQuickSend(orderId) {
  const panel = document.getElementById(`ord-quick-${orderId}`);
  if (!panel) return;
  const ta = panel.querySelector("textarea");
  const text = (ta?.value || "").trim();
  if (!text) { showToast?.("Message is empty", "error"); return; }
  const btns = panel.querySelectorAll("button");
  btns.forEach(b => b.disabled = true);
  try {
    const r = await apiFetch(`/api/user/orders/${encodeURIComponent(orderId)}/messages`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: text }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (r.status === 401 || r.status === 403) { try { sessionStorage.setItem("marketplaceScopeIssue", "1"); } catch {} if (typeof renderMarketplaceScopeBanner === "function") renderMarketplaceScopeBanner(); }
      showToast?.(j.error || `Send failed (${r.status})`, "error");
      return;
    }
    apiFetch(`/api/user/orders/${encodeURIComponent(orderId)}/view`, { method: "POST" }).catch(() => {});
    showToast?.("Reply sent", "success");
    _ordQuickCancel(orderId);
    _renderOrdersPage();
    _refreshUnreadBadge();
  } finally {
    btns.forEach(b => b.disabled = false);
  }
}

async function refreshOrders() {
  const btn = document.getElementById("orders-refresh-btn");
  if (btn) { btn.disabled = true; btn.textContent = "Refreshing…"; }
  try {
    const r = await apiFetch("/api/user/orders/refresh", { method: "POST" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      showToast?.(j.error || `Refresh failed (${r.status})`, "error");
    } else {
      showToast?.(`Synced ${j.count || 0} orders`, "success");
    }
    await _renderOrdersPage();
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "↻ Refresh"; }
  }
}

// ── Order detail modal ────────────────────────────────────────────────────

async function openOrderDetail(orderId) {
  const existing = document.getElementById("order-detail-overlay");
  if (existing) existing.remove();
  const overlay = document.createElement("div");
  overlay.id = "order-detail-overlay";
  overlay.innerHTML = `<div id="order-detail-panel" role="dialog" aria-modal="true">
    <button type="button" class="ord-detail-close" onclick="closeOrderDetail()" aria-label="Close">×</button>
    <div id="order-detail-body"><div style="padding:1rem;color:var(--muted)">Loading order…</div></div>
  </div>`;
  document.body.appendChild(overlay);
  overlay.addEventListener("click", ev => { if (ev.target === overlay) closeOrderDetail(); });
  document.addEventListener("keydown", _ordEscHandler);
  try {
    const [oRes, mRes] = await Promise.all([
      apiFetch(`/api/user/orders/${encodeURIComponent(orderId)}`),
      apiFetch(`/api/user/orders/${encodeURIComponent(orderId)}/messages`),
    ]);
    const oJson = oRes.ok ? await oRes.json() : {};
    const mJson = mRes.ok ? await mRes.json() : { messages: [] };
    const it = oJson.item || {};
    const data = it.data || {};
    const items = Array.isArray(data.items) ? data.items : [];
    const shipping = data.shipping_address || data.shipping || "";
    const buyer = data.buyer || {};
    const total = it.total_value ? `${it.total_currency || ""} ${Number(it.total_value).toFixed(2)}` : "";

    const itemsHtml = items.map(li => {
      const rel = li.release || {};
      const thumb = rel.thumbnail || "";
      return `<div class="ord-detail-item">
        ${thumb ? `<img src="${escHtml(thumb)}" alt=""/>` : `<div class="ord-thumb-ph"></div>`}
        <div class="ord-detail-item-body">
          <div><strong>${escHtml(rel.description || rel.title || ("Release " + (rel.id || "")))}</strong></div>
          <div style="font-size:0.8rem;color:var(--muted)">${escHtml(li.condition || "")} / ${escHtml(li.sleeve_condition || "")}</div>
          <div style="font-size:0.82rem">${escHtml(li.price?.currency || "")} ${escHtml(li.price?.value ?? "")}</div>
        </div>
      </div>`;
    }).join("");

    const msgsHtml = (mJson.messages || []).map(m => {
      const when = m.ts ? new Date(m.ts).toLocaleString() : "";
      return `<div class="ord-msg">
        <div class="ord-msg-head"><strong>${escHtml(m.from_user || "")}</strong> <span>${escHtml(when)}</span></div>
        ${m.subject ? `<div class="ord-msg-subj">${escHtml(m.subject)}</div>` : ""}
        <div class="ord-msg-body">${escHtml(m.message || "")}</div>
      </div>`;
    }).join("") || `<div style="color:var(--muted);font-size:0.85rem">No messages yet.</div>`;

    const allowed = _validNextStatuses(it.status);
    const statusOptions = allowed.map(s => `<option${s === it.status ? " selected" : ""}>${s}</option>`).join("");

    document.getElementById("order-detail-body").innerHTML = `
      <h2 style="margin:0 0 0.4rem 0">Order #${escHtml(it.order_id)}</h2>
      <div style="margin-bottom:0.8rem"><span class="${_statusChipClass(it.status)}">${escHtml(it.status || "—")}</span></div>

      <div class="ord-detail-grid">
        <div>
          <div class="ord-detail-label">Buyer</div>
          <div>${escHtml(buyer.username || it.buyer_username || "")}</div>
        </div>
        <div>
          <div class="ord-detail-label">Total</div>
          <div>${escHtml(total)}</div>
        </div>
        <div class="ord-detail-wide">
          <div class="ord-detail-label">Shipping address</div>
          <pre style="white-space:pre-wrap;margin:0;font-family:inherit;font-size:0.85rem">${escHtml(typeof shipping === "string" ? shipping : JSON.stringify(shipping, null, 2))}</pre>
        </div>
      </div>

      <div class="ord-detail-section">
        <h3>Items</h3>
        <div class="ord-detail-items">${itemsHtml || "<div style='color:var(--muted)'>No items</div>"}</div>
      </div>

      <div class="ord-detail-section">
        <h3>Status</h3>
        <div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap">
          <select id="ord-status-select" data-current-status="${escHtml(it.status || "")}">${statusOptions}</select>
          <button onclick="_ordChangeStatus('${escHtml(it.order_id)}')" class="ord-btn-primary">Update status</button>
        </div>
      </div>

      <div class="ord-detail-section">
        <h3>Messages</h3>
        <div class="ord-msgs">${msgsHtml}</div>
        <textarea id="ord-new-msg" rows="3" placeholder="Write a message to the buyer…" style="width:100%;margin-top:0.5rem;padding:0.5rem;background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:5px;font-family:inherit"></textarea>
        <div style="margin-top:0.4rem;text-align:right">
          <button onclick="_ordSendMessage('${escHtml(it.order_id)}')" class="ord-btn-primary">Send message</button>
        </div>
      </div>
    `;
    // Opening the order marks it viewed on the backend — clear indicators.
    const rowEl = document.querySelector(`.ord-row[data-oid="${CSS.escape(String(orderId))}"]`);
    if (rowEl) {
      rowEl.classList.remove("ord-row-unread");
      rowEl.querySelector(".ord-unread-dot")?.remove();
    }
    _refreshUnreadBadge();
  } catch (e) {
    document.getElementById("order-detail-body").innerHTML = `<div style="color:#e88;padding:1rem">Failed to load: ${escHtml(String(e))}</div>`;
  }
}

function closeOrderDetail() {
  document.getElementById("order-detail-overlay")?.remove();
  document.removeEventListener("keydown", _ordEscHandler);
}

function _ordEscHandler(ev) {
  if (ev.key === "Escape") closeOrderDetail();
}

// Status transitions that are near-irreversible on Discogs and therefore
// require an explicit confirmation before we POST. Cancelling refunds buyer
// funds, "Shipped" locks the order for the buyer, and "Refund Sent" is final.
const ORD_DESTRUCTIVE_STATUSES = new Set([
  "Cancelled (Non-Paying Buyer)",
  "Cancelled (Item Unavailable)",
  "Cancelled (Per Buyer's Request)",
  "Shipped",
  "Refund Sent",
]);

async function _ordChangeStatus(orderId) {
  const sel = document.getElementById("ord-status-select");
  if (!sel) return;
  const newStatus = sel.value;
  if (ORD_DESTRUCTIVE_STATUSES.has(newStatus)) {
    const msg = newStatus === "Shipped"
      ? `Mark order ${orderId} as Shipped?\n\nThis notifies the buyer and locks the order. Make sure the package is actually on its way before confirming.`
      : newStatus === "Refund Sent"
      ? `Mark order ${orderId} as Refund Sent?\n\nThis is a final status and cannot be undone from SeaDisco. Only confirm after you've actually issued the refund on Discogs / your payment processor.`
      : `Cancel order ${orderId}?\n\nStatus: "${newStatus}"\n\nThis releases any reserved inventory and notifies the buyer. The cancellation cannot be undone.`;
    if (!window.confirm(msg)) {
      // Revert the dropdown selection so the user isn't left thinking it changed.
      try { if (sel.dataset.currentStatus) sel.value = sel.dataset.currentStatus; } catch {}
      return;
    }
  }
  try {
    const r = await apiFetch(`/api/user/orders/${encodeURIComponent(orderId)}/status`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: newStatus }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (r.status === 401 || r.status === 403) { try { sessionStorage.setItem("marketplaceScopeIssue", "1"); } catch {} if (typeof renderMarketplaceScopeBanner === "function") renderMarketplaceScopeBanner(); }
      showToast?.(j.error || `Update failed (${r.status})`, "error"); return;
    }
    try { sessionStorage.removeItem("marketplaceScopeIssue"); } catch {}
    showToast?.("Status updated", "success");
    openOrderDetail(orderId);
    _renderOrdersPage();
    _refreshUnreadBadge();
  } catch (e) {
    showToast?.(String(e), "error");
  }
}

async function _ordSendMessage(orderId) {
  const ta = document.getElementById("ord-new-msg");
  if (!ta || !ta.value.trim()) return;
  const text = ta.value.trim();
  try {
    const r = await apiFetch(`/api/user/orders/${encodeURIComponent(orderId)}/messages`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: text }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (r.status === 401 || r.status === 403) { try { sessionStorage.setItem("marketplaceScopeIssue", "1"); } catch {} if (typeof renderMarketplaceScopeBanner === "function") renderMarketplaceScopeBanner(); }
      showToast?.(j.error || `Send failed (${r.status})`, "error"); return;
    }
    showToast?.("Message sent", "success");
    ta.value = "";
    openOrderDetail(orderId);
  } catch (e) {
    showToast?.(String(e), "error");
  }
}
