// ── Account view logic (extracted from account.html for SPA use) ─────────
// clerk ref is window._clerk, set by app.js auth

function showSignInWidget(c) {
  document.getElementById("loading-section").style.display = "none";
  document.getElementById("signed-out-section").style.display = "block";
  // Show spots remaining
  fetch("/api/user-count").then(r => r.json()).then(d => {
    const el = document.getElementById("signed-out-spots");
    if (el && d.count != null) {
      const remaining = Math.max(0, d.limit - d.count);
      el.textContent = remaining > 0
        ? `Currently ${remaining} of ${d.limit} spots available.`
        : `All ${d.limit} spots are currently taken. Check back later.`;
      el.style.display = "";
    }
  }).catch(() => {});
  c.mountSignUp(document.getElementById("clerk-sign-in"), {
    afterSignInUrl: "/?v=account",
    afterSignUpUrl: "/?v=account",
    appearance: {
      baseTheme: undefined,
      variables: {
        colorBackground:        "#15120e",
        colorInputBackground:   "#0e0c08",
        colorInputText:         "#e8dcc8",
        colorText:              "#e8dcc8",
        colorTextSecondary:     "#a89880",
        colorPrimary:           "#ff6b35",
        colorDanger:            "#e05050",
        colorNeutral:           "#a89880",
        borderRadius:           "6px",
        fontFamily:             "system-ui, -apple-system, sans-serif",
      },
      elements: {
        card:               "background:#15120e; border:1px solid #2e2518; box-shadow:none;",
        headerTitle:        "color:#e8dcc8;",
        headerSubtitle:     "color:#8a7d6b;",
        socialButtonsBlockButton: "background:#0e0c08; border:1px solid #2e2518; color:#e8dcc8;",
        socialButtonsBlockButtonText: "color:#e8dcc8;",
        dividerLine:        "background:#2e2518;",
        dividerText:        "color:#8a7d6b;",
        formFieldLabel:     "color:#8a7d6b;",
        formFieldInput:     "background:#0e0c08; border:1px solid #2e2518; color:#e8dcc8;",
        footerActionLink:   "color:#ff6b35;",
        footer:             "display:none;",
        footerAction:       "display:none;",
        identityPreviewText: "color:#e8dcc8;",
        identityPreviewEditButton: "color:#ff6b35;",
      },
    },
  });
}

function handleSignedIn(c) {
  window._clerk = c;
  document.getElementById("loading-section").style.display = "none";
  document.getElementById("signed-out-section").style.display = "none";
  showAuthSection();
}

async function showAuthSection() {
  document.getElementById("auth-section").style.display = "block";
  const email = window._clerk?.user?.primaryEmailAddress?.emailAddress ?? "";
  document.getElementById("user-email").textContent = email;

  // Check if they already have a token saved
  const tokenRes = await apiFetch("/api/user/token").catch(() => null);
  if (tokenRes && tokenRes.status === 403) {
    const err = await tokenRes.json().catch(() => ({}));
    if (err.error === "hibernated") {
      document.getElementById("auth-section").innerHTML = `
        <div style="text-align:center;padding:2rem 1rem">
          <h2 style="color:#e8a020;margin-bottom:0.8rem">Account Hibernated</h2>
          <p style="color:var(--muted);font-size:0.9rem;line-height:1.6;max-width:380px;margin:0 auto 1rem">${escHtml(err.message || "Your account is hibernated due to inactivity. All spots are currently full.")}</p>
          <p style="color:#666;font-size:0.82rem">Your data is preserved. You'll be reactivated automatically when a spot opens up.</p>
          <div style="margin-top:1.5rem">
            <button onclick="signOut()" style="background:none;border:1px solid #444;color:#aaa;font-size:0.85rem;padding:0.4rem 1rem;border-radius:5px;cursor:pointer">Sign out</button>
          </div>
        </div>`;
      return;
    }
  }
  const data = tokenRes ? await tokenRes.json().catch(() => null) : null;

  const statusEl = document.getElementById("token-status");
  const removeBtn = document.getElementById("remove-btn");
  const oauthSection = document.getElementById("oauth-section");
  const patSection = document.getElementById("pat-section");

  // Show OAuth connect option if server has it configured
  const patHint = document.getElementById("pat-oauth-hint");
  if (data?.oauthEnabled) {
    oauthSection.style.display = "";
    if (patHint) patHint.style.display = "";
  }

  if (data?.authMethod === "oauth") {
    // OAuth connected — show profile, collapse PAT section
    oauthSection.style.display = "none";
    if (patHint) patHint.style.display = "none";
    patSection.querySelector("h2").textContent = "Personal Access Token (optional)";
    // If user also has a PAT saved, show it
    if (data.masked) {
      statusEl.textContent = `Saved token: ${data.masked}`;
      statusEl.className = "token-status ok";
      removeBtn.style.display = "block";
    } else {
      statusEl.textContent = "Connected via OAuth — PAT is optional.";
      statusEl.className = "token-status ok";
    }
  } else if (data?.hasToken) {
    statusEl.textContent = `Saved token: ${data.masked}`;
    statusEl.className = "token-status ok";
    removeBtn.style.display = "block";
  } else {
    statusEl.textContent = "No token saved — connect with Discogs or paste a token below.";
    statusEl.className = "token-status";
  }

  if (data?.hasToken) { loadProfilePanel(); loadSyncStatus(); if (typeof loadOrdersSection === "function") loadOrdersSection(); renderMarketplaceScopeBanner(); }

  // Clean up OAuth success redirect param (UI is already refreshed above)
  const urlParams = new URLSearchParams(location.search);
  if (urlParams.get("oauth") === "success") {
    history.replaceState({}, "", "/?v=account");
  }
}

async function startOAuth() {
  const btn = document.getElementById("oauth-connect-btn");
  const statusEl = document.getElementById("oauth-status");
  btn.disabled = true;
  btn.style.opacity = "0.5";
  statusEl.style.display = "";
  statusEl.textContent = "Connecting to Discogs…";
  try {
    const data = await apiFetch("/api/auth/discogs/start").then(r => r.json());
    if (data?.authorizeUrl) {
      statusEl.textContent = "Redirecting to Discogs…";
      window.location.href = data.authorizeUrl;
    } else {
      statusEl.textContent = data?.error || "Failed to start OAuth";
      btn.disabled = false;
      btn.style.opacity = "1";
    }
  } catch (e) {
    statusEl.textContent = "Connection failed. Please try again.";
    btn.disabled = false;
    btn.style.opacity = "1";
  }
}

// Loads the cached Discogs profile and renders a small dashboard panel
async function loadProfilePanel() {
  const panel = document.getElementById("profile-panel");
  if (!panel) return;
  try {
    const r = await apiFetch("/api/user/profile");
    if (!r.ok) { panel.style.display = "none"; return; }
    const p = await r.json();
    const d = p.profileData || {};
    if (!p.username) { panel.style.display = "none"; return; }
    const name = d.name || p.username;
    const avatar = p.avatarUrl || "";
    const loc = d.location || "";
    const joined = d.registered ? new Date(d.registered).toLocaleDateString(undefined, { year: "numeric", month: "short" }) : "";
    const esc = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

    const fmt = (n) => (n == null ? "\u2014" : Number(n).toLocaleString());
    const stars = (n) => {
      if (n == null) return "";
      const v = Number(n);
      if (!Number.isFinite(v)) return "";
      const full = Math.round(v);
      return "\u2605".repeat(full) + "\u2606".repeat(Math.max(0, 5 - full));
    };

    const sellerRatings = d.seller_num_ratings ?? 0;
    const sellerStars = d.seller_rating_stars != null ? stars(d.seller_rating_stars) : (d.seller_rating != null ? stars(d.seller_rating) : "");
    const sellerText = sellerRatings > 0 ? `${sellerStars} (${sellerRatings} rating${sellerRatings === 1 ? "" : "s"})` : "No seller ratings yet";

    const curr = p.currAbbr || d.curr_abbr || "USD";
    const isOAuth = p.authMethod === "oauth";
    const badgeHtml = isOAuth
      ? `<span class="profile-badge profile-badge-oauth" title="Connected via OAuth \u2014 full access including marketplace management">OAuth</span>`
      : `<span class="profile-badge profile-badge-pat" title="Connected via Personal Access Token \u2014 read-only; connect via OAuth above for marketplace management">Token</span>`;
    const authHint = isOAuth
      ? `<div class="profile-sub" style="color:#6bcf8e;font-size:0.72rem;margin-top:0.15rem">Full access \u2014 collection, wantlist, marketplace listings, orders &amp; messages</div>`
      : `<div class="profile-sub" style="color:#e8a020;font-size:0.72rem;margin-top:0.15rem">Read-only \u2014 connect via OAuth above to manage marketplace listings &amp; orders</div>`;
    const disconnectHtml = isOAuth
      ? `<button type="button" class="profile-disconnect" onclick="disconnectOAuth()" title="Disconnect your Discogs connection">Disconnect</button>`
      : "";

    panel.innerHTML = `
      <div class="profile-panel">
        <div class="profile-head">
          ${avatar ? `<img class="profile-avatar" src="${esc(avatar)}" alt=""/>` : `<div class="profile-avatar profile-avatar-ph"></div>`}
          <div class="profile-head-text">
            <div class="profile-name">${esc(name)} ${badgeHtml}</div>
            <div class="profile-sub">
              <strong>@${esc(p.username)}</strong>
              ${loc ? ` \u00b7 ${esc(loc)}` : ""}
              ${joined ? ` \u00b7 joined ${esc(joined)}` : ""}
            </div>
            <div class="profile-sub">Marketplace currency: <strong>${esc(curr)}</strong></div>
            ${authHint}
          </div>
          <div class="profile-head-actions">
            <button type="button" class="profile-refresh" onclick="refreshProfilePanel(this)" title="Re-sync profile from Discogs">\u21bb</button>
            ${disconnectHtml}
          </div>
        </div>
        <div class="profile-stats">
          <div class="profile-stat"><div class="profile-stat-num">${fmt(d.num_collection)}</div><div class="profile-stat-label">Collection</div></div>
          <div class="profile-stat"><div class="profile-stat-num">${fmt(d.num_wantlist)}</div><div class="profile-stat-label">Wantlist</div></div>
          <div class="profile-stat"><div class="profile-stat-num">${fmt(d.num_lists)}</div><div class="profile-stat-label">Lists</div></div>
          <div class="profile-stat"><div class="profile-stat-num">${fmt(d.num_for_sale)}</div><div class="profile-stat-label">For sale</div></div>
          <div class="profile-stat"><div class="profile-stat-num">${fmt(d.releases_rated)}</div><div class="profile-stat-label">Rated</div></div>
        </div>
        <div class="profile-seller">Seller: ${esc(sellerText)}</div>
      </div>
    `;
    panel.style.display = "block";
  } catch {
    panel.style.display = "none";
  }
}

async function refreshProfilePanel(btn) {
  if (btn) { btn.disabled = true; btn.textContent = "\u2026"; }
  try {
    await apiFetch("/api/user/profile/refresh", { method: "POST", headers: { "Content-Type": "application/json" } });
    await loadProfilePanel();
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "\u21bb"; }
  }
}

// Reads a session flag set by inventory-editor.js / orders.js whenever
// Discogs rejects a marketplace write as unauthorized, and shows a
// dismissible banner prompting the user to reconnect.
function renderMarketplaceScopeBanner() {
  const el = document.getElementById("marketplace-scope-banner");
  if (!el) return;
  let flag = "";
  try { flag = sessionStorage.getItem("marketplaceScopeIssue") || ""; } catch {}
  el.style.display = flag ? "block" : "none";
}
function dismissMarketplaceBanner() {
  try { sessionStorage.removeItem("marketplaceScopeIssue"); } catch {}
  const el = document.getElementById("marketplace-scope-banner");
  if (el) el.style.display = "none";
}

async function disconnectOAuth() {
  if (!confirm("Disconnect your Discogs OAuth connection? You can reconnect anytime.")) return;
  try {
    await apiFetch("/api/auth/discogs/disconnect", { method: "DELETE" });
    showAuthSection();
  } catch {}
}

let _syncPollTimer = null;

async function loadSyncStatus() {
  const syncSection = document.getElementById("sync-section");
  const statusEl = document.getElementById("account-sync-status");
  const btn = document.getElementById("account-sync-btn");
  syncSection.style.display = "block";
  try {
    const d = await apiFetch("/api/user/sync-status").then(r => r.json());

    // If sync is currently running, show progress and start polling
    if (d.syncStatus === "syncing") {
      const pct = d.syncTotal ? Math.round((d.syncProgress / d.syncTotal) * 100) : 0;
      statusEl.textContent = `Syncing\u2026 ${d.syncProgress.toLocaleString()} / ${d.syncTotal.toLocaleString()} items (${pct}%)`;
      btn.disabled = true;
      btn.textContent = "Syncing\u2026";
      btn.style.opacity = "0.4";
      btn.style.cursor = "default";
      startSyncPolling();
      return;
    }

    // Sync finished or idle — stop polling if running
    stopSyncPolling();

    let statusText = `Collection: synced ${fmtTime(d.collectionSyncedAt)} \u00b7 Wantlist: synced ${fmtTime(d.wantlistSyncedAt)}`;
    if (d.discogsUsername) statusText += ` \u00b7 @${d.discogsUsername}`;
    if (d.syncStatus === "complete" && d.syncProgress > 0) {
      statusText += ` \u00b7 Last sync: ${d.syncProgress.toLocaleString()} items`;
    }
    if (d.syncStatus === "error" && d.syncError) {
      statusText += ` \u00b7 Sync error: ${d.syncError}`;
    }
    statusEl.textContent = statusText;

    // Auto-sync if either collection or wantlist is more than 25 hours stale
    const twentyFiveHoursAgo = Date.now() - 25 * 60 * 60 * 1000;
    const collStale = !d.collectionSyncedAt || new Date(d.collectionSyncedAt).getTime() < twentyFiveHoursAgo;
    const wantStale = !d.wantlistSyncedAt   || new Date(d.wantlistSyncedAt).getTime()   < twentyFiveHoursAgo;
    if ((collStale || wantStale) && d.syncStatus !== "syncing") {
      accountSync(true); // silent auto-sync
      return;
    }

    // Dim the sync button if both were synced within the last 5 minutes
    const cooldownAgo = Date.now() - 5 * 60 * 1000;
    const collRecent = d.collectionSyncedAt && new Date(d.collectionSyncedAt).getTime() > cooldownAgo;
    const wantRecent = d.wantlistSyncedAt && new Date(d.wantlistSyncedAt).getTime() > cooldownAgo;
    if (collRecent && wantRecent) {
      btn.disabled = true;
      btn.style.opacity = "0.4";
      btn.style.cursor = "default";
      btn.title = "Synced recently \u2014 available again in a few minutes";
    } else {
      btn.disabled = false;
      btn.style.opacity = "1";
      btn.style.cursor = "pointer";
      btn.title = "";
    }
    btn.textContent = "Sync Now";
  } catch {
    statusEl.textContent = "Could not load sync status.";
  }
}

function startSyncPolling() {
  if (_syncPollTimer) return;
  _syncPollTimer = setInterval(loadSyncStatus, 4000);
}

function stopSyncPolling() {
  if (_syncPollTimer) { clearInterval(_syncPollTimer); _syncPollTimer = null; }
}

async function accountSync(silent = false) {
  const btn = document.getElementById("account-sync-btn");
  const statusEl = document.getElementById("account-sync-status");
  if (!silent) {
    btn.disabled = true;
    btn.textContent = "Syncing\u2026";
    btn.style.opacity = "0.4";
    statusEl.textContent = "Starting sync\u2026";
  }
  try {
    const r = await apiFetch("/api/user/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type: "both" })
    }).then(r => r.json());
    if (r.skipped) {
      if (!silent) {
        statusEl.textContent = "Synced recently \u2014 try again in a few minutes.";
        btn.textContent = "Sync Now";
        btn.title = "Synced recently \u2014 available again in a few minutes";
      }
      return;
    }
    // Sync started in background — poll for progress
    if (!silent) statusEl.textContent = "Syncing in background\u2026";
    startSyncPolling();
  } catch {
    if (!silent) {
      statusEl.textContent = "Sync failed. Please try again.";
      btn.textContent = "Sync Now";
      btn.disabled = false;
      btn.style.opacity = "1";
    }
  }
}

async function saveToken() {
  const token = document.getElementById("token-input").value.trim();
  if (!token) { alert("Please paste your Discogs token first."); return; }

  const r = await apiFetch("/api/user/token", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token }),
  });

  if (r.ok) {
    document.getElementById("token-input").value = "";
    await showAuthSection();
  } else {
    const err = await r.json().catch(() => ({}));
    alert("Error saving token: " + (err.error ?? r.status));
  }
}

async function removeToken() {
  if (!confirm("Remove your saved Discogs token?")) return;
  await apiFetch("/api/user/token", { method: "DELETE" });
  await showAuthSection();
}

function openFeedback() {
  const ov = document.getElementById("feedback-overlay");
  ov.style.display = "flex";
  document.getElementById("feedback-text").focus();
}

function closeFeedback() {
  document.getElementById("feedback-overlay").style.display = "none";
  document.getElementById("feedback-text").value = "";
  document.getElementById("feedback-msg").style.display = "none";
}

async function submitFeedback() {
  const message = document.getElementById("feedback-text").value.trim();
  if (!message) return;
  const btn = document.getElementById("feedback-submit-btn");
  btn.disabled = true; btn.textContent = "Sending\u2026";
  try {
    const userEmail = window._clerk?.user?.primaryEmailAddress?.emailAddress ?? "";
    const r = await apiFetch("/api/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message, userEmail }),
    });
    if (r.ok) {
      const msg = document.getElementById("feedback-msg");
      msg.textContent = "Thanks \u2014 feedback sent!";
      msg.style.display = "block";
      document.getElementById("feedback-text").value = "";
      setTimeout(closeFeedback, 1500);
    } else {
      alert("Error sending feedback.");
    }
  } finally {
    btn.disabled = false; btn.textContent = "Send";
  }
}

async function signOut() {
  await window._clerk?.signOut();
  // Stay in SPA — just go back to search view and clear auth state
  _cachedToken = null;
  _cachedTokenAt = 0;
  switchView("search");
}

async function deleteAccount() {
  if (!confirm("Permanently delete your account and all saved data? This cannot be undone.")) return;
  try {
    // Delete our DB records first
    await apiFetch("/api/user/account", { method: "DELETE" });
    // Then delete the Clerk account
    await window._clerk?.user?.delete();
    _cachedToken = null;
    _cachedTokenAt = 0;
    switchView("search");
  } catch (err) {
    alert("Error deleting account: " + (err?.message ?? err));
  }
}

// ── SPA entry point: called by switchView("account") ────────────────────
function initAccountView() {
  const clerk = window._clerk;
  if (clerk?.user) {
    handleSignedIn(clerk);
  } else if (clerk) {
    showSignInWidget(clerk);
  } else {
    // Auth not ready yet — wait for Clerk to become available, then retry
    const poll = setInterval(() => {
      const c = window._clerk;
      if (!c) return;
      clearInterval(poll);
      if (c.user) handleSignedIn(c);
      else showSignInWidget(c);
    }, 150);
    // Stop polling after 10 seconds to avoid leaking
    setTimeout(() => clearInterval(poll), 10000);
  }
}
