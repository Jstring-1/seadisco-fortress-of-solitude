// ── Account view logic (extracted from account.html for SPA use) ─────────
// clerk ref is window._clerk, set by app.js auth

function showSignInWidget(c) {
  document.getElementById("loading-section").style.display = "none";
  document.getElementById("signed-out-section").style.display = "block";
  const mount = document.getElementById("clerk-sign-in");
  // Public mode — mountSignIn shows the sign-in form with a built-in
  // "Sign up" link for new visitors (Clerk handles the toggle).
  const opts = {
    afterSignInUrl: "/?v=account",
    afterSignUpUrl: "/?v=account",
    appearance: {
      ...SEADISCO_CLERK_APPEARANCE,
      elements: {
        ...SEADISCO_CLERK_APPEARANCE.elements,
        socialButtonsBlockButton:     "background:#0e0c08; border:1px solid #2e2518; color:#e8dcc8;",
        socialButtonsBlockButtonText: "color:#e8dcc8;",
        dividerLine:                  "background:#2e2518;",
        dividerText:                  "color:#8a7d6b;",
        identityPreviewText:          "color:#e8dcc8;",
        identityPreviewEditButton:    "color:#ff6b35;",
      },
    },
  };
  if (typeof c.mountSignIn === "function") {
    c.mountSignIn(mount, opts);
  } else if (typeof c.mountSignUp === "function") {
    c.mountSignUp(mount, opts);
  }
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
          <p style="color:var(--muted);font-size:0.9rem;line-height:1.6;max-width:380px;margin:0 auto 1rem">${escHtml(err.message || "Your account is hibernated due to inactivity.")}</p>
          <p style="color:#666;font-size:0.82rem">Your data is preserved. Contact the admin to reactivate.</p>
          <div style="margin-top:1.5rem">
            <button onclick="signOut()" style="background:none;border:1px solid #444;color:#aaa;font-size:0.85rem;padding:0.4rem 1rem;border-radius:5px;cursor:pointer">Sign out</button>
          </div>
        </div>`;
      return;
    }
  }
  const data = tokenRes ? await tokenRes.json().catch(() => null) : null;

  const oauthSection = document.getElementById("oauth-section");

  // OAuth is the only supported connection method now. Show the connect
  // button when nothing is connected; hide it once we're connected.
  if (data?.oauthEnabled) {
    oauthSection.style.display = data?.authMethod === "oauth" ? "none" : "";
  }

  if (data?.hasToken) { loadProfilePanel(); loadSyncStatus(); if (typeof loadOrdersSection === "function") loadOrdersSection(); renderMarketplaceScopeBanner(); }
  // Surface the offline-access section. Independent of the OAuth /
  // hasToken state — it just caches whatever the user can already see
  // when signed in. Re-rendered on reconnect by offline.js.
  if (typeof renderOfflineSection === "function") renderOfflineSection();

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

    const isOAuth = p.authMethod === "oauth";
    const badgeHtml = isOAuth
      ? `<span class="profile-badge profile-badge-oauth" title="Connected via OAuth">OAuth</span>`
      : "";
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
          </div>
          <div class="profile-head-actions">
            <button type="button" class="profile-refresh" onclick="refreshProfilePanel(this)" title="Re-sync profile from Discogs">\u21bb</button>
            ${disconnectHtml}
          </div>
        </div>
        <div class="profile-stats">
          <a href="/?v=collection" class="profile-stat profile-stat-link" onclick="event.preventDefault();_cwTab='collection';switchView('records');return false" title="Open your collection"><div class="profile-stat-num">${fmt(d.num_collection)}</div><div class="profile-stat-label">Collection</div></a>
          <a href="/?v=wantlist" class="profile-stat profile-stat-link" onclick="event.preventDefault();_cwTab='wantlist';switchView('records');return false" title="Open your wantlist"><div class="profile-stat-num">${fmt(d.num_wantlist)}</div><div class="profile-stat-label">Wantlist</div></a>
          <a href="/?v=lists" class="profile-stat profile-stat-link" onclick="event.preventDefault();_cwTab='lists';switchView('records');return false" title="Open your lists"><div class="profile-stat-num">${fmt(d.num_lists)}</div><div class="profile-stat-label">Lists</div></a>
          <a href="/?v=inventory" class="profile-stat profile-stat-link" onclick="event.preventDefault();_cwTab='inventory';switchView('records');return false" title="Open your inventory"><div class="profile-stat-num">${fmt(d.num_for_sale)}</div><div class="profile-stat-label">For sale</div></a>
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

    let statusText = `Collection: synced ${fmtTime(d.collectionSyncedAt, "never")} \u00b7 Wantlist: synced ${fmtTime(d.wantlistSyncedAt, "never")}`;
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
  _cachedToken = null;
  _cachedTokenAt = 0;
  // Hard reload to "/" so the suggested cards / nav state rebuild
  // fresh as a signed-out user. Staying in the SPA left the prior
  // user's recents visible until something else triggered a refetch
  // (and could expose stale signed-in data on a shared device).
  location.replace("/");
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
// ── Offline access section ───────────────────────────────────────────
// Driven by web/offline.js (window.sdOffline). Re-renders on
// enable/disable/sync/install events so the status line stays fresh.
function _fmtBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 MB";
  const mb = bytes / (1024 * 1024);
  if (mb >= 100) return `${Math.round(mb)} MB`;
  if (mb >= 1)   return `${mb.toFixed(1)} MB`;
  const kb = bytes / 1024;
  return `${Math.round(kb)} KB`;
}
function _fmtAgo(ts) {
  if (!ts) return "never";
  const diff = Date.now() - Number(ts);
  if (diff < 60_000)        return "just now";
  if (diff < 3_600_000)     return `${Math.floor(diff / 60_000)} min ago`;
  if (diff < 86_400_000)    return `${Math.floor(diff / 3_600_000)} h ago`;
  return `${Math.floor(diff / 86_400_000)} d ago`;
}

async function renderOfflineSection() {
  const wrap = document.getElementById("offline-section");
  const body = document.getElementById("offline-section-body");
  if (!wrap || !body) return;
  if (!window.sdOffline) {
    wrap.style.display = "none";
    return;
  }
  wrap.style.display = "block";
  const enabled  = window.sdOffline.isEnabled();
  const stats    = await window.sdOffline.getStorageStats();
  const installed = window.sdOffline.isInstalled();
  const canInstall = window.sdOffline.canInstall();

  // Show only what offline mode actually uses, broken down so the
  // user knows what's growing. Prior version used navigator.storage
  // .estimate() which reports the whole origin's quota — drifted by
  // hundreds of KB on every sync, made the line look like things
  // were leaking.
  let sizeBlurb;
  if (!enabled) {
    sizeBlurb = "Currently off — your library is fetched live each visit.";
  } else {
    const parts = [];
    parts.push(`Library ${_fmtBytes(stats.libraryBytes)}`);
    if (stats.imageBytes)    parts.push(`images ${_fmtBytes(stats.imageBytes)}`);
    if (stats.shellBytes)    parts.push(`app ${_fmtBytes(stats.shellBytes)}`);
    if (stats.apiCacheBytes) parts.push(`api ${_fmtBytes(stats.apiCacheBytes)}`);
    sizeBlurb = `${parts.join(" · ")}  (total ${_fmtBytes(stats.totalBytes)})`;
  }
  const lastSync = enabled
    ? `Last synced ${_fmtAgo(stats.lastSyncAt)}`
    : "";

  // Install-as-app sub-row: shown only when offline mode is enabled
  // (prerequisite for an installable PWA in our setup) AND the
  // browser actually offered an install prompt this session.
  let installRow = "";
  if (enabled) {
    if (installed) {
      installRow = `<div style="font-size:0.82rem;color:var(--muted);margin-top:0.6rem">Running as an installed app.</div>`;
    } else if (canInstall) {
      installRow = `
        <div style="margin-top:0.6rem;display:flex;align-items:center;gap:0.6rem;flex-wrap:wrap">
          <button type="button" onclick="offlineInstallApp()" style="font-size:0.82rem;padding:0.35rem 0.9rem;background:var(--bg-elevated);border:1px solid var(--border);color:var(--text);border-radius:5px;cursor:pointer">Install as app</button>
          <span style="font-size:0.78rem;color:var(--muted)">Adds a desktop / home-screen icon. Same site, opens in its own window.</span>
        </div>`;
    } else {
      installRow = `<div style="font-size:0.78rem;color:var(--muted);margin-top:0.6rem">Tip: in Safari iOS, use Share → Add to Home Screen to install.</div>`;
    }
  }

  body.innerHTML = `
    <p style="font-size:0.84rem;color:var(--muted);margin:0 0 0.7rem">
      Download your collection, wantlist, favorites, lists, and inventory to this device so you can browse them when you're offline. Read-only — adding or editing still needs a connection. Cover images cache as you scroll through them online; ones you haven't viewed yet won't appear offline.
    </p>
    <label class="offline-toggle" style="display:inline-flex;align-items:center;gap:0.55rem;cursor:pointer;user-select:none">
      <input type="checkbox" ${enabled ? "checked" : ""} onchange="${enabled ? "disableOffline" : "enableOffline"}(this)" style="width:1rem;height:1rem;accent-color:var(--accent);cursor:pointer">
      <span style="font-size:0.88rem">${enabled ? "Offline access is on" : "Enable offline access"}</span>
    </label>
    <div id="offline-progress" style="margin-top:0.6rem;font-size:0.82rem;color:var(--muted);display:none"></div>
    <div style="font-size:0.82rem;color:var(--muted);margin-top:0.5rem">${escHtml(sizeBlurb)}${lastSync ? ` · ${escHtml(lastSync)}` : ""}</div>
    ${enabled ? `
      <div style="margin-top:0.6rem;display:flex;gap:0.6rem;flex-wrap:wrap">
        <button type="button" onclick="offlineSyncNow(this)" style="font-size:0.82rem;padding:0.3rem 0.85rem;background:var(--accent);color:#000;border:none;border-radius:5px;cursor:pointer;font-weight:600">Sync now</button>
        <button type="button" onclick="offlineClearCache(this)" style="font-size:0.82rem;padding:0.3rem 0.85rem;background:none;border:1px solid var(--border);color:var(--muted);border-radius:5px;cursor:pointer">Clear cache</button>
      </div>
    ` : ""}
    ${installRow}
  `;
}
window._sdOfflineRenderAccount = renderOfflineSection;

async function enableOffline(checkboxEl) {
  if (checkboxEl) checkboxEl.disabled = true;
  const progress = document.getElementById("offline-progress");
  if (progress) { progress.style.display = "block"; progress.textContent = "Enabling offline access…"; }
  try {
    await window.sdOffline.enable((p) => {
      if (progress) progress.textContent = `Downloading ${p.label}… (${p.done} / ${p.total})`;
    });
    if (typeof showToast === "function") showToast("Offline access enabled");
  } catch (e) {
    if (typeof showToast === "function") showToast("Could not enable offline access", "error");
  } finally {
    if (progress) progress.style.display = "none";
    if (checkboxEl) checkboxEl.disabled = false;
    renderOfflineSection();
  }
}
window.enableOffline = enableOffline;

async function disableOffline(checkboxEl) {
  if (!confirm("Turn off offline access and clear cached data on this device?")) {
    if (checkboxEl) checkboxEl.checked = true;
    return;
  }
  if (checkboxEl) checkboxEl.disabled = true;
  try {
    await window.sdOffline.disable();
    if (typeof showToast === "function") showToast("Offline access turned off");
  } finally {
    if (checkboxEl) checkboxEl.disabled = false;
    renderOfflineSection();
  }
}
window.disableOffline = disableOffline;

async function offlineSyncNow(btn) {
  if (btn) { btn.disabled = true; btn.textContent = "Syncing…"; }
  const progress = document.getElementById("offline-progress");
  if (progress) { progress.style.display = "block"; progress.textContent = "Refreshing…"; }
  try {
    await window.sdOffline.syncNow((p) => {
      if (progress) progress.textContent = `Refreshing ${p.label}… (${p.done} / ${p.total})`;
    });
    if (typeof showToast === "function") showToast("Library re-synced");
  } catch {
    if (typeof showToast === "function") showToast("Sync failed — try again", "error");
  } finally {
    if (progress) progress.style.display = "none";
    if (btn) { btn.disabled = false; btn.textContent = "Sync now"; }
    renderOfflineSection();
  }
}
window.offlineSyncNow = offlineSyncNow;

async function offlineClearCache(btn) {
  if (!confirm("Clear cached library and images on this device? Offline access will stay on (re-sync to populate again).")) return;
  if (btn) { btn.disabled = true; btn.textContent = "Clearing…"; }
  try {
    if (window.sdIdb) {
      await window.sdIdb.libraryClear();
      await window.sdIdb.metaClear();
    }
    // Ask the SW to wipe its caches without unregistering.
    try {
      const ctrl = navigator.serviceWorker?.controller;
      if (ctrl) ctrl.postMessage({ type: "SD_CLEAR_CACHES" });
    } catch {}
    if (typeof showToast === "function") showToast("Cache cleared");
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "Clear cache"; }
    renderOfflineSection();
  }
}
window.offlineClearCache = offlineClearCache;

async function offlineInstallApp() {
  if (!window.sdOffline?.canInstall?.()) return;
  const r = await window.sdOffline.promptInstall();
  if (r.outcome === "accepted" && typeof showToast === "function") {
    showToast("Installed");
  }
  renderOfflineSection();
}
window.offlineInstallApp = offlineInstallApp;

// On reconnect, refresh the visible Account view's offline section
// (storage estimates may have changed after a background SW update).
window._sdOfflineRefreshOnReconnect = () => {
  if (document.getElementById("account-view")?.style.display !== "none") {
    renderOfflineSection();
  }
};

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
