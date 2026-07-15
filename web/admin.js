// admin.js — extracted from the former inline <script> in admin.html.
// Behavior-identical: this is a classic (non-module) deferred script, so
// top-level let/const/function bindings stay global exactly as when inline.
// Loaded last among admin.html's deferred scripts (after shared/modal/blues-admin).
let clerk = null;

// ── Dual-surface boot ─────────────────────────────────────────────
// admin.js runs on two surfaces:
//   standalone — /admin (admin.html thin shell; this script is a
//     deferred tag there, so the shell markers below exist by the
//     time we evaluate).
//   inline — /?v=admin on index.html; the script is lazy-loaded by
//     switchView long after DOMContentLoaded, no shell markers.
// Both fetch the dashboard markup from the requireAdmin-gated
// /admin-panel.html fragment into #admin-content, so there is exactly
// one copy of the admin markup.
const _ADMIN_STANDALONE = !!document.getElementById("admin-denied-section");

// True while the admin content is actually on screen. Every poll loop
// checks this so the inline view stops all admin fetches while hidden
// behind another SPA view (music playing, admin idle in background).
// Poll chains that die while hidden are restarted by _adminInlineOpen
// on re-entry.
function _adminPanelVisible() {
  const host = document.getElementById("admin-content");
  return !!host && host.offsetParent !== null;
}

// Fetch /admin-panel.html into #admin-content once. Returns the HTTP
// status (200 on success, 401/403 when not admin, 0 on network error).
// The fetch doubles as the auth probe on the inline surface.
let _adminDomReady = false;
async function _adminEnsureDom() {
  if (_adminDomReady) return 200;
  const host = document.getElementById("admin-content");
  if (!host) return 0;
  try {
    const r = await apiFetch("/admin-panel.html");
    if (!r.ok) return r.status;
    host.innerHTML = await r.text();
    _adminDomReady = true;
    return 200;
  } catch { return 0; }
}

// Convenience entry point for refresh buttons. Looks up the matching
// status element by id convention (<btnId>.replace("-btn","-status"))
// and runs the loader inside _adminWithRefresh — gives every refresh
// button visual feedback without needing to edit each loader.
async function _adminClickRefresh(btn, work) {
  const statusId = (btn?.id || "").replace(/-btn$/, "-status");
  const statusEl = statusId ? document.getElementById(statusId) : null;
  return _adminWithRefresh(btn, statusEl, work);
}

// Visual refresh feedback helper used by every admin loader. Spins
// the icon, disables the button, drives the status text. Pass the
// loader's body as `work`. Errors thrown from `work` flip the status
// to red.
async function _adminWithRefresh(btn, statusEl, work) {
  if (btn) {
    btn.classList.add("is-refreshing");
    btn.classList.remove("just-refreshed");
    btn.disabled = true;
  }
  if (statusEl) {
    statusEl.textContent = "Refreshing…";
    statusEl.classList.remove("is-success", "is-error");
  }
  const t0 = performance.now();
  try {
    await work();
    if (statusEl) {
      const elapsed = Math.round(performance.now() - t0);
      statusEl.textContent = `Refreshed at ${new Date().toLocaleTimeString()} · ${elapsed}ms`;
      statusEl.classList.add("is-success");
      setTimeout(() => { statusEl.classList.remove("is-success"); }, 2500);
    }
    if (btn) {
      btn.classList.add("just-refreshed");
      setTimeout(() => btn.classList.remove("just-refreshed"), 1200);
    }
  } catch (e) {
    if (statusEl) {
      statusEl.textContent = "Failed: " + (e?.message || e);
      statusEl.classList.add("is-error");
    }
  } finally {
    if (btn) {
      btn.classList.remove("is-refreshing");
      btn.disabled = false;
    }
  }
}

// Consolidated 5-group model. Each group maps to one or more existing
// panels (stacked = the "sub-sections" the user sees) and the loaders
// that hydrate them. Existing per-panel loaders are reused verbatim;
// only the grouping/orchestration changed.
const _adminGroups = {
  'overview': {
    panels: ['panel-overview-kpis', 'panel-collection-stats', 'panel-media-stats'],
    load: () => { loadAdminOverview(); loadCollectionStats(); loadAdminMediaStats(); },
  },
  'users': {
    panels: ['panel-user-stats', 'panel-sync-status', 'panel-behavior', 'panel-suggestions'],
    load: () => { loadAdminUserStats(); loadAdminSyncStatus(); loadAdminBehavior(); loadAdminSuggestions(); },
  },
  'content': {
    panels: ['panel-submissions', 'panel-unavailable'],
    load: () => { loadAdminSubmissions(); loadAdminUnavailable(); },
  },
  'system': {
    panels: ['panel-system-bar', 'panel-db-stats', 'panel-api-log'],
    load: () => { loadDbStats(); loadApiLog(); },
  },
  'cache': {
    panels: ['panel-cache-warm'],
    load: () => { loadCacheWarm(); loadCacheRate(); loadCacheAnalytics(); loadCacheProjection(); },
  },
  'labels': {
    panels: ['panel-labels'],
    load: () => { loadLabelDirectory(); loadCoverageSweeps(); },
  },
  'yt-review': {
    panels: ['panel-yt-review'],
    load: () => { loadYtReview(); },
  },
  'query': {
    panels: ['panel-query'],
    load: () => { loadQuerySchema(); },
  },
  // 'blues-db' + 'lyrics' tabs removed; their content lives in the
  // Discovery Blues Archive view (/?v=blues-archive). The runtime
  // entries are gone too, so a stale URL hash like /admin#blues-db
  // just falls through to Overview.
};

// "View as user" — flips the localStorage flag the main site reads to
// hide admin-only affordances, then reloads. Single button under
// Actions now (was its own tab). Toggles both ways.
function adminToggleViewAsUser() {
  try {
    const on = localStorage.getItem("sd-admin-as-user") === "1";
    if (on) localStorage.removeItem("sd-admin-as-user");
    else localStorage.setItem("sd-admin-as-user", "1");
  } catch {}
  location.reload();
}
// Reflect current state on the button label.
function _adminSyncViewBtn() {
  const b = document.getElementById("admin-view-btn");
  if (!b) return;
  const on = localStorage.getItem("sd-admin-as-user") === "1";
  b.textContent = on ? "Viewing as user ✓" : "View as user";
  b.classList.toggle("admin-btn-extras", on);
}
const _adminTabLoaded = {};
// Current group — used by _adminInlineOpen to restart the active
// tab's poll chains when the inline view is re-entered.
let _adminActiveGroup = null;

function switchAdminTab(group) {
  const g = _adminGroups[group];
  if (!g) return;
  _adminActiveGroup = group;
  document.querySelectorAll('.admin-tab-panel').forEach(p => p.style.display = 'none');
  g.panels.forEach(id => { const el = document.getElementById(id); if (el) el.style.display = ''; });
  document.querySelectorAll('.admin-tab').forEach(b => {
    b.classList.toggle('active', (b.getAttribute('onclick') || '').includes("'" + group + "'"));
  });
  if (!_adminTabLoaded[group]) {
    _adminTabLoaded[group] = true;
    try { g.load(); } catch (e) { console.warn('[admin] group load failed', group, e); }
  }
  // Mirror the current tab in the URL hash so refresh / share-link
  // lands back on the same tab. replaceState (not push) so the browser
  // back button still exits the admin rather than cycling through tabs.
  if (location.hash !== '#' + group) {
    try { history.replaceState(null, '', '#' + group); } catch {}
  }
}

// URL-backed tabs — read the hash on boot + on external navigation.
function _adminBootTabFromHash() {
  const h = (location.hash || '').replace(/^#/, '');
  if (h && _adminGroups[h]) switchAdminTab(h);
}
window.addEventListener('hashchange', _adminBootTabFromHash);

// ── Persistent worker-status bar ──────────────────────────────────
// Polls every worker's status endpoint in parallel every 8s and
// renders one badge per active worker with click-to-tab. Silent when
// nothing's running.
async function loadAdminWorkerStatus() {
  const bar = document.getElementById('admin-worker-status');
  if (!bar) return;
  // Inline view hidden (or standalone pre-auth) — skip the fetch; the
  // interval keeps ticking and resumes polling once visible again.
  if (!_adminPanelVisible()) return;
  try {
    // One aggregate request instead of 10 per poll — see
    // /api/admin/workers/status. Response is keyed by worker.
    const wr = await apiFetch('/api/admin/workers/status').catch(() => null);
    const w = (wr?.ok ? await wr.json().catch(() => null) : null) || {};
    const badges = [];
    // Track whether the bulk sweep is running so we can suppress the
    // catno badge below — otherwise we get two badges for one activity
    // (the outer queue runner + the inner per-label sweep it fired).
    let bulkRunning = false;
    if (w.bulk?.running) {
      const j = w.bulk;
      bulkRunning = true;
      const pos = j.total > 0 ? ` (${j.cursor}/${j.total})` : '';
      const cur = j.currentLabel?.labelName ? ` · ${j.currentLabel.labelName}` : '';
      badges.push({ label: `Bulk sweep${pos}${cur}`, tab: 'labels' });
    }
    if (w.projection?.running) {
      const j = w.projection;
      const total = j.total ?? j.stats?.releaseCacheProjectable;
      const pos = total ? ` (${j.processed}/${total})` : ` (${j.processed})`;
      badges.push({ label: `Split-cache projection${pos}`, tab: 'cache' });
    }
    if (w.catno?.running && !bulkRunning) {
      // Suppress this badge while the bulk sweep is running — that
      // badge already shows the current label name. Standalone catno
      // runs (someone hit ▶ on a single row) still get a badge, but
      // hide the raw series key when it's just "adhoc:<labelId>";
      // curated series keep their human-readable key.
      const active = String(w.catno.active || '');
      const label = active.startsWith('adhoc:')
        ? 'Label sweep'
        : `Sweep · ${active || '?'}`;
      badges.push({ label, tab: 'labels' });
    }
    if (w.ext?.running) {
      const j = w.ext;
      const pos = j.total > 0 ? ` (${j.cursor}/${j.total})` : '';
      badges.push({ label: `Ext scrape · ${j.source || 'unknown'}${pos}`, tab: 'labels' });
    }
    if (w.warm?.running) {
      const p = w.warm.active || {};
      const label = p.genreKey ? `Cache-warm · ${p.genreKey}${p.styleKey ? '/' + p.styleKey : ''}` : 'Cache-warm run';
      badges.push({ label, tab: 'cache' });
    }
    if (w.yt?.running) {
      const pending = w.yt?.counts?.pending;
      const suffix  = Number.isFinite(pending) ? ` (${pending} pending)` : '';
      badges.push({ label: `YT review${suffix}`, tab: 'yt-review' });
    }
    if (w.faceted?.running) {
      const j = w.faceted;
      const pos = j.total > 0 ? ` (${j.cursor}/${j.total})` : '';
      const cur = j.currentSlot ? ` · ${j.currentSlot.value}/${j.currentSlot.year}` : '';
      badges.push({ label: `Year×${j.mode || 'facet'}${pos}${cur}`, tab: 'labels' });
    }
    if (w.upstream?.running) {
      const j = w.upstream;
      const pos = j.total > 0 ? ` (${j.cursor}/${j.total})` : '';
      badges.push({ label: `Upstream totals${pos}`, tab: 'labels' });
    }
    if (badges.length === 0) {
      bar.style.display = 'none';
      bar.innerHTML = '';
      return;
    }
    bar.style.display = '';
    bar.innerHTML = `
      <strong style="color:#fc8">⚙ Workers running:</strong>
      ${badges.map(b => `
        <button type="button" onclick="switchAdminTab('${b.tab}')"
          style="margin-left:0.4rem;padding:0.15rem 0.55rem;background:rgba(255,255,255,0.06);color:var(--text);border:1px solid rgba(255,255,255,0.18);border-radius:3px;cursor:pointer;font-size:0.78rem">
          ${_adminWorkerEscape(b.label)} ↗
        </button>
      `).join('')}
    `;
  } catch (err) {
    // Silent — the bar just stays hidden. Persistent workers status
    // isn't critical enough to alert on network hiccups.
  }
}
function _adminWorkerEscape(s) {
  return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
// Kick off polling once. The interval survives tab switches since
// nothing tears it down.
if (!window._adminWorkerStatusTimer) {
  window._adminWorkerStatusTimer = setInterval(loadAdminWorkerStatus, 8000);
  setTimeout(loadAdminWorkerStatus, 500);   // first tick soon after boot
}

function showDenied() {
  const l = document.getElementById("admin-loading-section");
  const d = document.getElementById("admin-denied-section");
  if (l) l.style.display = "none";
  if (d) d.style.display = "block";
}

// Post-auth boot shared by both surfaces: render the feedback probe
// result, pick the boot tab, start the kill-switch check.
function _adminBootContent(items) {
  renderFeedback(items);
  _adminUpdateFeedbackDot(Array.isArray(items) ? items.length : 0);
  _adminSyncViewBtn();
  // Boot tab: URL hash wins (e.g. /admin#labels reloads to Labels),
  // otherwise default to Overview.
  const bootHash = (location.hash || '').replace(/^#/, '');
  if (bootHash && _adminGroups[bootHash]) {
    switchAdminTab(bootHash);
  } else {
    switchAdminTab('overview');
  }
  checkKillStatus();
}

async function verifyAdmin(c) {
  clerk = c;
  try {
    const r = await apiFetch("/api/admin/feedback", { signal: AbortSignal.timeout(10000) });
    // Only deny on a real auth failure. A 500 here usually means the
    // DB is temporarily unreachable — the admin IS authorized, the
    // backend just can't talk to Postgres right now. Showing the
    // denied screen for that wasted the admin's time; better to
    // surface the actual error and still let them into the admin
    // content so they can read other panels / hit the kill switch /
    // etc.
    if (r.status === 401 || r.status === 403) { showDenied(); return; }

    let items = [];
    if (r.ok) {
      try { items = (await r.json()).items ?? []; } catch {}
    } else {
      console.warn("[verifyAdmin] feedback fetch failed:", r.status);
    }

    const st = await _adminEnsureDom();
    if (st !== 200) {
      document.getElementById("admin-loading-section").innerHTML =
        `<p style="color:#e88;padding:1rem">Couldn't load the admin panel (${st || "network error"}). Refresh to retry.</p>`;
      return;
    }
    document.getElementById("admin-loading-section").style.display = "none";
    document.getElementById("admin-denied-section").style.display = "none";
    document.getElementById("admin-content").style.display = "block";

    _adminBootContent(items);
  } catch (e) {
    // Network / timeout / abort. Distinguish from real auth denial:
    // if the user has a Clerk session, they're at least signed in —
    // showing the admin frame with an inline error is more useful
    // than the denied screen.
    console.warn("[verifyAdmin] threw:", e);
    if (clerk?.user) {
      document.getElementById("admin-loading-section").innerHTML =
        `<p style="color:#e88;padding:1rem">Couldn't reach the admin API. Check your connection and refresh.</p>`;
    } else {
      showDenied();
    }
  }
}

// ── Inline SPA entry ──────────────────────────────────────────────
// Called by switchView('admin') on index.html after lazy-loading
// blues-admin.js + admin.js. First call fetches the fragment (which
// doubles as the auth probe) and boots; later calls just restart the
// active tab's poll chains, which stop themselves while hidden.
let _adminInlineBooted = false;
window._adminInlineOpen = async function () {
  const host = document.getElementById("admin-content");
  const status = document.getElementById("admin-inline-status");
  if (!host) return;
  clerk = clerk || window._clerk || null;
  if (_adminInlineBooted) {
    host.style.display = "block";
    loadAdminWorkerStatus();
    if (_adminActiveGroup && _adminGroups[_adminActiveGroup]) {
      try { _adminGroups[_adminActiveGroup].load(); } catch {}
    }
    return;
  }
  if (status) status.textContent = "Loading admin…";
  const st = await _adminEnsureDom();
  if (st === 401 || st === 403) {
    if (status) status.textContent = "Access denied. Admin only.";
    return;
  }
  if (st !== 200) {
    if (status) status.textContent = "Couldn't load the admin panel. Leave and re-enter the view to retry.";
    return;
  }
  if (status) status.textContent = "";
  host.style.display = "block";
  let items = [];
  try {
    const r = await apiFetch("/api/admin/feedback", { signal: AbortSignal.timeout(10000) });
    if (r.ok) { try { items = (await r.json()).items ?? []; } catch {} }
  } catch {}
  _adminInlineBooted = true;
  _adminBootContent(items);
};



// ── User Sync Status ──────────────────────────────────────────────────
let _adminSyncPoll = null;
let _syncStatusLoaded = false; // true after first successful load
// Auto-stop the poll after consecutive failures so a transient Clerk
// token blip (or full-on session expiry) doesn't spam the network tab
// with 401s for minutes. Manual refresh / view re-entry resets the
// counter and revives polling.
let _syncStatusConsecFail = 0;

async function loadAdminSyncStatus() {
  const el = document.getElementById("admin-sync-status-list");
  // Inline view hidden — skip this tick; the interval keeps ticking
  // and resumes fetching when the view is back on screen.
  if (!_adminPanelVisible()) return;
  try {
    const r = await apiFetch("/api/admin/sync-status");
    if (!r.ok) {
      _syncStatusConsecFail++;
      // 401 = no valid Clerk session right now. Stop polling
      // immediately; user can re-trigger via the refresh button or by
      // navigating back into the admin view.
      if (r.status === 401 || _syncStatusConsecFail >= 3) {
        if (_adminSyncPoll) { clearInterval(_adminSyncPoll); _adminSyncPoll = null; }
        if (!_syncStatusLoaded && el) el.textContent = r.status === 401
          ? "Sign-in expired — refresh the page."
          : "Could not load sync status.";
        return;
      }
      // Other transient errors: keep existing data visible, skip this tick.
      if (_syncStatusLoaded) return;
      el.textContent = "Could not load sync status.";
      return;
    }
    _syncStatusConsecFail = 0;
    const { users } = await r.json();
    _syncStatusLoaded = true;
    if (!users?.length) { el.textContent = "No users have connected Discogs."; return; }

    el.innerHTML = users.map(u => {
      const isSyncing = u.syncStatus === "syncing";
      const isError = u.syncStatus === "error";
      const isComplete = u.syncStatus === "complete";
      const pct = u.syncTotal ? Math.round((u.syncProgress / u.syncTotal) * 100) : 0;
      const statusColor = isError ? "#c0392b" : isSyncing ? "var(--accent)" : isComplete ? "#4caf50" : "#555";
      let statusText;
      if (isSyncing) {
        statusText = u.syncTotal
          ? `Syncing\u2026 ${u.syncProgress.toLocaleString()} / ${u.syncTotal.toLocaleString()} (${pct}%)`
          : `Syncing\u2026 ${u.syncProgress.toLocaleString()} items`;
      } else if (isError) {
        statusText = `Error: ${u.syncError}`;
      } else if (isComplete) {
        statusText = u.syncTotal
          ? `Complete \u2014 ${u.syncProgress.toLocaleString()} / ${u.syncTotal.toLocaleString()} items`
          : u.syncProgress > 0
            ? `Complete \u2014 ${u.syncProgress.toLocaleString()} items`
            : `Complete \u2014 up to date`;
      } else {
        const countStr = u.syncProgress > 0 ? ` \u2014 ${u.syncProgress.toLocaleString()} items` : "";
        statusText = `Idle${countStr}`;
      }
      const syncBtn = isSyncing ? '' : `<button onclick="adminSyncUser('${u.username}')" class="admin-btn" style="font-size:0.72rem;padding:0.15rem 0.5rem">Sync</button>`;
      const badges = [];
      if (u.hasOAuth) badges.push('<span style="font-size:0.65rem;padding:0.1rem 0.35rem;border-radius:3px;background:#1a3a5c;color:#7eb8da;font-weight:600;margin-left:0.4rem" title="OAuth connected">OAuth</span>');
      if (!badges.length) badges.push('<span style="font-size:0.65rem;padding:0.1rem 0.35rem;border-radius:3px;background:rgba(255,255,255,0.08);color:#666;font-weight:600;margin-left:0.4rem">\u2014</span>');
      const authBadge = badges.join('');
      const lastActiveStr = u.lastActiveAt ? fmtRelativeTime(u.lastActiveAt) : "\u2014";
      const lastActiveColor = u.online ? "#4caf50" : "#777";
      // Display name: prefer Clerk username when present, fall back
      // to the @discogs-handle. When both are present and differ,
      // show "clerk (@discogs)" so the admin can match across.
      const dispName = u.clerkUsername
        ? (u.clerkUsername !== u.username ? `${u.clerkUsername} (@${u.username})` : u.clerkUsername)
        : `@${u.username}`;
      return `<div class="sync-row">
        <div class="sync-row-header">
          <span style="color:var(--fg);font-weight:600;white-space:nowrap"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${u.online ? '#4caf50' : '#444'};margin-right:0.4rem;vertical-align:middle" title="${u.online ? 'Active in last 24h' : 'Inactive 24h+'}"></span>${dispName}${authBadge}</span>
          <span style="display:flex;gap:0.3rem;align-items:center;flex-shrink:0">
            <a href="#" onclick="event.preventDefault();openAdminItems('${u.username}','collection')" style="font-size:0.68rem;color:var(--accent);text-decoration:none" title="View collection">C</a>
            <a href="#" onclick="event.preventDefault();openAdminItems('${u.username}','wantlist')" style="font-size:0.68rem;color:var(--accent);text-decoration:none" title="View wantlist">W</a>
            <a href="#" onclick="event.preventDefault();openAdminItems('${u.username}','favorites')" style="font-size:0.68rem;color:#e57;text-decoration:none" title="View favorites">♥${u.favoriteCount ? u.favoriteCount : ''}</a>
            ${syncBtn}
          </span>
        </div>
        <span class="sync-row-status" style="color:${statusColor}">${statusText}</span>
        <div class="sync-row-meta">
          <span style="color:${lastActiveColor};font-size:0.75rem">${lastActiveStr}</span>
          <span style="color:#555">Coll: ${fmtTime(u.collectionSyncedAt, "never")} \u00b7 Want: ${fmtTime(u.wantlistSyncedAt, "never")}</span>
        </div>
      </div>`;
    }).join("");

    const anyRunning = users.some(u => u.syncStatus === "syncing");
    const interval = anyRunning ? 5000 : 15000;
    if (_adminSyncPoll) clearInterval(_adminSyncPoll);
    _adminSyncPoll = setInterval(loadAdminSyncStatus, interval);
  } catch {
    if (!_syncStatusLoaded) el.textContent = "Could not load sync status.";
    // On polling refresh, silently ignore — keep existing data
  }
}

// ── Admin actions ─────────────────────────────────────────────────────
async function adminSyncAll() {
  const btn = document.getElementById("admin-sync-all-btn");
  const statusEl = document.getElementById("admin-action-status");
  btn.disabled = true; btn.textContent = "Starting\u2026";
  try {
    const r = await apiFetch("/api/admin/sync-all", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    }).then(r => r.json());
    statusEl.textContent = `Full sync kicked off for ${r.queued} user${r.queued !== 1 ? "s" : ""}.`;
    loadAdminSyncStatus();
  } catch { statusEl.textContent = "Sync-all failed."; }
  finally { btn.disabled = false; btn.textContent = "Sync All"; }
}

async function adminSyncUser(username) {
  try {
    await apiFetch("/api/admin/sync-user", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username })
    });
    loadAdminSyncStatus();
  } catch { alert("Sync failed for " + username); }
}

async function adminSyncStop() {
  const btn = document.getElementById("admin-sync-stop-btn");
  const statusEl = document.getElementById("admin-action-status");
  btn.disabled = true; btn.textContent = "Stopping\u2026";
  try {
    const r = await apiFetch("/api/admin/sync-stop", { method: "POST" }).then(r => r.json());
    statusEl.textContent = r.message || "Stop signal sent.";
  } catch { statusEl.textContent = "Stop request failed."; }
  finally { btn.disabled = false; btn.textContent = "Stop All"; }
}

async function toggleApiKill() {
  const btn = document.getElementById("admin-api-kill-btn");
  const statusEl = document.getElementById("admin-action-status");
  btn.disabled = true;
  try {
    const r = await apiFetch("/api/admin/api-kill", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    }).then(r => r.json());
    updateKillButton(r.killSwitch);
    statusEl.textContent = r.killSwitch
      ? "ALL outgoing API requests are now BLOCKED."
      : "API requests resumed \u2014 all services flowing.";
  } catch { statusEl.textContent = "Kill switch toggle failed."; }
  finally { btn.disabled = false; }
}

async function adminRevokeSessions() {
  const btn = document.getElementById("admin-revoke-btn");
  const statusEl = document.getElementById("admin-action-status");
  if (!confirm("Log out ALL users except you?")) return;
  btn.disabled = true; btn.textContent = "Revoking…";
  try {
    const r = await apiFetch("/api/admin/revoke-sessions", { method: "POST" }).then(r => r.json());
    statusEl.textContent = r.ok ? `Revoked sessions for ${r.revokedUsers} user(s).` : (r.error || "Failed.");
  } catch { statusEl.textContent = "Revoke request failed."; }
  finally { btn.disabled = false; btn.textContent = "Logout All"; }
}

function updateKillButton(active) {
  const btn = document.getElementById("admin-api-kill-btn");
  if (active) {
    btn.textContent = "Resume APIs";
    btn.style.background = "#1a6b1a";
    btn.style.borderColor = "#3a3";
  } else {
    btn.textContent = "Kill All APIs";
    btn.style.background = "#6b1a1a";
    btn.style.borderColor = "#933";
  }
}

async function checkKillStatus() {
  try {
    const r = await apiFetch("/api/admin/api-kill").then(r => r.json());
    updateKillButton(r.killSwitch);
  } catch {}
}

async function adminExtrasFetch() {
  const btn = document.getElementById("admin-extras-fetch-btn");
  const statusEl = document.getElementById("admin-action-status");
  btn.disabled = true; btn.textContent = "Fetching\u2026";
  try {
    await apiFetch("/api/admin/extras/fetch", { method: "POST" }).then(r => r.json());
    statusEl.textContent = "Extras sync started (inventory + lists + list items + seller orders) — running in background.";
  } catch { statusEl.textContent = "Extras fetch failed."; }
  finally { btn.disabled = false; btn.textContent = "Fetch Extras"; }
}

// ── Database Stats ────────────────────────────────────────────────────
// Manual trigger to refresh the archive.org cache. The server runs
// the same refresh weekly on its own schedule; this button is for
// when admin uploads new shows and doesn't want to wait. Returns
// 202 immediately; we poll the listing endpoint to detect when the
// new data has landed in DB.
async function adminWarmFavoritesCache(btn) {
  if (btn) { btn.disabled = true; btn.textContent = "Warming…"; }
  try {
    const r = await apiFetch("/api/admin/warm-favorites-cache", { method: "POST" });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      if (typeof showToast === "function") showToast(`Warm failed: ${err.error || r.status}`, "error");
      return;
    }
    if (typeof showToast === "function") {
      showToast("Cache warm started — check railway logs for progress (typically 1 req/sec).");
    }
  } catch (e) {
    if (typeof showToast === "function") showToast(`Warm failed: ${e}`, "error");
  } finally {
    setTimeout(() => { if (btn) { btn.disabled = false; btn.textContent = "Warm favorites cache"; } }, 2000);
  }
}

async function adminRefreshArchive(btn) {
  const statusEl = document.getElementById("admin-archive-status");
  const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg; };
  if (btn) { btn.disabled = true; btn.textContent = "Refreshing…"; }
  setStatus("Triggered — walking archive.org pages, may take 1–2 min for large collections");
  try {
    const r = await apiFetch("/api/admin/archive/refresh", { method: "POST" });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      setStatus("Failed: " + (err.error ?? r.status));
      if (btn) { btn.disabled = false; btn.textContent = "Get archive data"; }
      return;
    }
    // Poll the listing endpoint every 6s; show item count on success.
    let lastCount = -1;
    let polls = 0;
    const tick = async () => {
      polls++;
      try {
        const lr = await apiFetch("/api/archive/aadamjacobs");
        if (lr.ok) {
          const j = await lr.json();
          const count = (j.items || []).length;
          if (count !== lastCount) {
            lastCount = count;
            const fetched = j.fetchedAt ? new Date(j.fetchedAt).toLocaleString() : "";
            setStatus(`${count} item${count === 1 ? "" : "s"} cached${fetched ? " · " + fetched : ""}`);
          }
        }
      } catch {}
      if (polls < 40) setTimeout(tick, 6000); // up to ~4 minutes
      else if (btn) { btn.disabled = false; btn.textContent = "Get archive data"; }
    };
    setTimeout(tick, 4000);
    if (btn) {
      // Re-enable after polling window so the button isn't stuck disabled
      setTimeout(() => { btn.disabled = false; btn.textContent = "Get archive data"; }, 60000);
    }
  } catch (e) {
    setStatus("Failed: " + e);
    if (btn) { btn.disabled = false; btn.textContent = "Get archive data"; }
  }
}

async function loadDbStats(triggerBtn) {
  const el = document.getElementById("db-stats");
  // Visual refresh feedback: spin the icon, disable the button, show
  // a status string. The trigger arg is the clicked button (the panel
  // auto-load case calls this without an arg, so we fall back to the
  // known id).
  const btn = triggerBtn || document.getElementById("db-stats-refresh-btn");
  const statusEl = document.getElementById("db-stats-refresh-status");
  if (btn) {
    btn.classList.add("is-refreshing");
    btn.classList.remove("just-refreshed");
    btn.disabled = true;
  }
  if (statusEl) {
    statusEl.textContent = "Refreshing…";
    statusEl.classList.remove("is-success", "is-error");
  }
  const t0 = performance.now();
  try {
    const r = await apiFetch("/api/admin/db-stats");
    if (!r.ok) {
      el.textContent = "Could not load DB stats.";
      if (statusEl) {
        statusEl.textContent = `Failed (${r.status})`;
        statusEl.classList.add("is-error");
      }
      return;
    }
    const { tables, totalRows } = await r.json();

    const groups = [
      { label: "Users & Auth", tables: ["user_tokens", "oauth_request_tokens"] },
      { label: "Collection & Library", tables: ["user_collection", "user_collection_folders", "user_wantlist", "user_inventory", "user_lists", "user_list_items", "user_orders", "user_order_messages"] },
      { label: "Pricing", tables: ["price_cache", "price_history"] },
      { label: "User Features", tables: ["saved_searches", "user_favorites", "user_recent_views", "user_loc_saves", "user_archive_saves", "user_wiki_saves", "user_play_queue", "feedback", "release_cache"] },
      { label: "Curated Data", tables: ["blues_artists"] },
      { label: "System", tables: ["api_request_log", "app_settings"] },
    ];

    const countMap = {};
    tables.forEach(t => countMap[t.table] = t.rows);

    // Flag any tables the server reports that aren't in any of our groups
    // — keeps this panel self-checking when a new table is added without
    // remembering to update the grouping above.
    const knownTables = new Set(groups.flatMap(g => g.tables));
    const ungrouped = tables.map(t => t.table).filter(t => !knownTables.has(t));
    if (ungrouped.length) groups.push({ label: "Ungrouped (unrecognized)", tables: ungrouped });

    let html = `<div style="margin-bottom:0.6rem;font-weight:600;color:var(--fg)">${totalRows.toLocaleString()} total rows · ${tables.length} tables</div>`;
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:0.8rem">';
    for (const g of groups) {
      html += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:0.5rem 0.7rem">`;
      html += `<div style="font-size:0.72rem;color:var(--accent);font-weight:600;margin-bottom:0.3rem;text-transform:uppercase;letter-spacing:0.5px">${g.label}</div>`;
      for (const t of g.tables) {
        const count = countMap[t] ?? -1;
        const display = count < 0 ? "err" : count.toLocaleString();
        const color = count < 0 ? "var(--danger)" : count === 0 ? "var(--muted-dim)" : "var(--fg)";
        const exists = count >= 0;
        const nameMarkup = exists
          ? `<a href="#" onclick="event.preventDefault();adminOpenDbTablePopup('${escHtml(t).replace(/'/g, "\\'")}')" style="color:var(--muted);text-decoration:none;border-bottom:1px dotted transparent" onmouseover="this.style.borderBottomColor='var(--accent)';this.style.color='var(--text)'" onmouseout="this.style.borderBottomColor='transparent';this.style.color='var(--muted)'" title="Show schema, indexes, size">${t}</a>`
          : `<span style="color:var(--muted)">${t}</span>`;
        html += `<div style="display:flex;justify-content:space-between;padding:0.12rem 0;font-size:0.78rem">${nameMarkup}<span style="color:${color};font-weight:500">${display}</span></div>`;
      }
      html += `</div>`;
    }
    html += '</div>';
    el.innerHTML = html;

    // Flat all-tables view inside the <details> below. Sorted by row
    // count desc so the largest tables surface first.
    const flatEl = document.getElementById("db-stats-flat");
    if (flatEl) {
      const sorted = [...tables].sort((a, b) => (b.rows ?? 0) - (a.rows ?? 0));
      flatEl.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));column-gap:1rem;row-gap:0.15rem">` +
        sorted.map(t => {
          const count = t.rows;
          const display = count < 0 ? "err" : count.toLocaleString();
          const color = count < 0 ? "var(--danger)" : count === 0 ? "var(--muted-dim)" : "var(--fg)";
          const exists = count >= 0;
          const nameMarkup = exists
            ? `<a href="#" onclick="event.preventDefault();adminOpenDbTablePopup('${escHtml(t.table).replace(/'/g, "\\'")}')" style="color:var(--muted);font-family:monospace;text-decoration:none;border-bottom:1px dotted transparent" onmouseover="this.style.borderBottomColor='var(--accent)';this.style.color='var(--text)'" onmouseout="this.style.borderBottomColor='transparent';this.style.color='var(--muted)'" title="Show schema, indexes, size">${t.table}</a>`
            : `<span style="color:var(--muted);font-family:monospace">${t.table}</span>`;
          return `<div style="display:flex;justify-content:space-between;font-size:0.78rem">${nameMarkup}<span style="color:${color};font-weight:500">${display}</span></div>`;
        }).join("") +
        `</div>`;
    }
    // Success indicator: brief green flash + "Refreshed at HH:MM:SS"
    // status that fades to muted after a few seconds.
    if (statusEl) {
      const elapsed = Math.round(performance.now() - t0);
      const time = new Date().toLocaleTimeString();
      statusEl.textContent = `Refreshed at ${time} · ${elapsed}ms`;
      statusEl.classList.add("is-success");
      setTimeout(() => {
        statusEl.classList.remove("is-success");
      }, 2500);
    }
    if (btn) {
      btn.classList.add("just-refreshed");
      setTimeout(() => btn.classList.remove("just-refreshed"), 1200);
    }
  } catch (e) {
    el.textContent = "Could not load DB stats.";
    if (statusEl) {
      statusEl.textContent = "Failed: " + (e?.message || e);
      statusEl.classList.add("is-error");
    }
  } finally {
    if (btn) {
      btn.classList.remove("is-refreshing");
      btn.disabled = false;
    }
  }
}

// ── LOC Stats ─────────────────────────────────────────────────────────
async function loadLocStats() {
  const el = document.getElementById("loc-stats-body");
  if (!el) return;
  try {
    const r = await apiFetch("/api/admin/loc-stats");
    if (!r.ok) { el.textContent = "Could not load LOC stats."; return; }
    const s = await r.json();
    const rl = s.rateLimiter || {};
    const fmtTime = (t) => t ? new Date(t).toLocaleString() : "—";
    const fmtDur = (ms) => {
      if (!ms) return "—";
      const s = Math.floor(ms / 1000);
      if (s < 60) return `${s}s`;
      if (s < 3600) return `${Math.floor(s / 60)}m`;
      return `${Math.floor(s / 3600)}h`;
    };
    const queueDepth = (rl.queued ?? 0);
    const queuePct = rl.maxQueueDepth ? Math.round((queueDepth / rl.maxQueueDepth) * 100) : 0;
    const windowPct = rl.max ? Math.round((rl.inWindow / rl.max) * 100) : 0;
    const queueColor = queueDepth === 0 ? "#6b8f71" : queuePct > 60 ? "#ff6b35" : "#e8d44d";
    const windowColor = windowPct > 80 ? "#ff6b35" : windowPct > 50 ? "#e8d44d" : "#6b8f71";
    const failureColor = s.failures > 0 ? "#ff6b35" : "#555";
    const rlColor = s.rateLimitHits > 0 ? "#ff6b35" : "#555";

    el.innerHTML = `
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:0.8rem;margin-bottom:1rem">
        <div class="admin-card">
          <div style="font-size:0.68rem;color:var(--accent);font-weight:600;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:0.3rem">Cache</div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Hit rate</span><strong style="color:var(--fg)">${s.hitRatePct}%</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Hits</span><strong style="color:#6b8f71">${s.cacheHits.toLocaleString()}</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Misses</span><strong style="color:var(--fg)">${s.cacheMisses.toLocaleString()}</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Entries</span><strong style="color:var(--fg)">${s.cacheEntries} / ${s.cacheMax}</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">TTL</span><strong style="color:var(--fg)">${fmtDur(s.cacheTtlMs)}</strong></div>
        </div>
        <div class="admin-card">
          <div style="font-size:0.68rem;color:var(--accent);font-weight:600;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:0.3rem">Rate Limiter</div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">In window</span><strong style="color:${windowColor}">${rl.inWindow ?? 0} / ${rl.max ?? 0}</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Queue depth</span><strong style="color:${queueColor}">${queueDepth} / ${rl.maxQueueDepth ?? 0}</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Window</span><strong style="color:var(--fg)">${fmtDur(rl.windowMs)}</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">503s fired</span><strong style="color:${rlColor}">${s.rateLimitHits.toLocaleString()}</strong></div>
        </div>
        <div class="admin-card">
          <div style="font-size:0.68rem;color:var(--accent);font-weight:600;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:0.3rem">Traffic</div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Last 10 min</span><strong style="color:var(--fg)">${s.requestsLast10m.toLocaleString()}</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Last hour</span><strong style="color:var(--fg)">${s.requestsLastHour.toLocaleString()}</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Last 24h</span><strong style="color:var(--fg)">${s.requestsLastDay.toLocaleString()}</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Last request</span><strong style="color:var(--fg);font-size:0.72rem">${fmtTime(s.lastRequestAt)}</strong></div>
        </div>
        <div class="admin-card">
          <div style="font-size:0.68rem;color:var(--accent);font-weight:600;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:0.3rem">Failures</div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Total</span><strong style="color:${failureColor}">${s.failures.toLocaleString()}</strong></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem"><span style="color:#888">Last failure</span><strong style="color:var(--fg);font-size:0.72rem">${fmtTime(s.lastFailureAt)}</strong></div>
          ${s.lastFailureMsg ? `<div style="font-size:0.72rem;color:#ff9470;margin-top:0.3rem;word-break:break-word">${escHtml(s.lastFailureMsg)}</div>` : ""}
        </div>
      </div>
      <div style="display:flex;gap:0.5rem;align-items:center">
        <button onclick="clearLocCache()" style="background:var(--surface);border:1px solid #6b4a18;color:#e8a84a;font-size:0.78rem;padding:0.4rem 0.9rem;border-radius:5px;cursor:pointer">Clear response cache</button>
        <span style="font-size:0.72rem;color:#666">Evicts all ${s.cacheEntries} cached search responses. Next searches re-hit loc.gov.</span>
      </div>
    `;
  } catch (e) {
    el.textContent = "Could not load LOC stats.";
  }
}

async function clearLocCache() {
  if (!confirm("Clear the LOC response cache? The next few searches will hit loc.gov directly until the cache warms up.")) return;
  try {
    const r = await apiFetch("/api/admin/loc-cache/clear", { method: "POST" });
    if (!r.ok) { alert("Failed to clear cache."); return; }
    const body = await r.json();
    alert(`Cleared ${body.cleared} cached responses.`);
    loadLocStats();
  } catch {
    alert("Failed to clear cache.");
  }
}

// ── Cache-warm manual control + stats grid ────────────────────────
// Single control box at the top (genre / style / year range / Start
// or Stop), stats grid below listing every (genre, style) combo the
// admin has ever run. Polls every 5s while a run is in progress.
const _CW_GENRES = [
  { key: "Blues",                   list: "Blues"      },
  { key: "Folk, World, & Country",  list: "Folk"       },
  { key: "Jazz",                    list: "Jazz"       },
  { key: "Reggae",                  list: "Reggae"     },
  { key: "Latin",                   list: "Latin"      },
  { key: "Rock",                    list: "Rock"       },
  { key: "Electronic",              list: "Electronic" },
  { key: "Funk / Soul",             list: "FunkSoul"   },
  { key: "Hip Hop",                 list: "HipHop"     },
  { key: "Pop",                     list: "Pop"        },
  { key: "Classical",               list: "Classical"  },
  { key: "Stage & Screen",          list: "Stage"      },
  { key: "Brass & Military",        list: "Brass"      },
  { key: "Children's",              list: "Childrens"  },
  { key: "Non-Music",               list: "NonMusic"   },
];
let _cwPollTimer = null;
// Sort state for the per-combo stats grid. Default: in_cache desc
// (biggest cached buckets first). Numeric vs string columns get
// different comparators inside _cwSortRows.
let _cwSort = { col: "in_cache", dir: "desc" };
// When true, the per-combo grid hides rows that have a style_key —
// only genre-only rows are shown so you can see total cache by genre.
let _cwGenresOnly = false;
// Last /status response, kept so display-only toggles (genres-only
// filter, sort header clicks) can re-render without paying for
// another network round-trip + heavy server-side CTE.
let _cwLastResp = null;
function _cwToggleGenresOnly() {
  _cwGenresOnly = !_cwGenresOnly;
  // Re-render from the cached status payload — toggling a pure
  // display filter shouldn't pay for another /status round-trip (the
  // CTE behind it is heavy and the click felt unresponsive).
  loadCacheWarm({ fromCache: true });
}
window._cwToggleGenresOnly = _cwToggleGenresOnly;
const _CW_NUMERIC_COLS = new Set([
  "in_cache", "total_cached", "total_skipped", "total_errors", "current_year",
]);
function _cwSortRows(rows) {
  const { col, dir } = _cwSort;
  const mul = dir === "asc" ? 1 : -1;
  const numeric = _CW_NUMERIC_COLS.has(col);
  return rows.slice().sort((a, b) => {
    const av = a?.[col];
    const bv = b?.[col];
    // Push null/undefined to the bottom regardless of direction so
    // empty cells don't crowd the top when sorting ascending.
    const aMissing = av == null || av === "";
    const bMissing = bv == null || bv === "";
    if (aMissing && !bMissing) return 1;
    if (!aMissing && bMissing) return -1;
    if (aMissing && bMissing) return 0;
    if (numeric) return (Number(av) - Number(bv)) * mul;
    if (col === "last_run_at") return (new Date(av).getTime() - new Date(bv).getTime()) * mul;
    return String(av).toLowerCase().localeCompare(String(bv).toLowerCase()) * mul;
  });
}
function _cwSortBy(col) {
  if (_cwSort.col === col) {
    _cwSort.dir = _cwSort.dir === "asc" ? "desc" : "asc";
  } else {
    _cwSort.col = col;
    // Numeric columns default to desc (biggest first); text columns
    // default to asc (A-Z).
    _cwSort.dir = _CW_NUMERIC_COLS.has(col) || col === "last_run_at" ? "desc" : "asc";
  }
  // Sort is purely client-side — re-render off the cached payload so
  // header clicks feel instant.
  loadCacheWarm({ fromCache: true });
}
window._cwSortBy = _cwSortBy;
// ── Catalog-number worker UI ──────────────────────────────────────
async function loadCacheWarmCatno() {
  const el = document.getElementById("cwc-content");
  if (!el) return;
  try {
    const r = await apiFetch("/api/admin/cache-warm-catno/status");
    if (!r.ok) { el.innerHTML = `<span style="color:#e88">Failed: HTTP ${r.status}</span>`; return; }
    const resp = await r.json();
    const series = Array.isArray(resp?.series) ? resp.series : [];
    const running = !!resp.running;
    const active = resp.active || null;
    const esc = escHtml;   // canonical escaper (shared.js) — escapes & < > " '
    const fmt = n => Number(n || 0).toLocaleString();
    if (!series.length) {
      el.innerHTML = `<div style="color:var(--muted)">No catalog series configured.</div>`;
      return;
    }
    const runningPill = running
      ? `<span style="padding:0.1rem 0.4rem;border-radius:999px;background:rgba(120,220,140,0.12);color:#7ddc8c;border:1px solid rgba(120,220,140,0.5);font-size:0.7rem">running · ${esc(active || "")}</span>`
      : `<span style="padding:0.1rem 0.4rem;border-radius:999px;background:rgba(255,255,255,0.04);color:var(--muted);border:1px solid var(--border);font-size:0.7rem">idle</span>`;
    const rows = series.map(s => {
      const isActive = running && active === s.key;
      const range = `from ${s.lo}`;
      const phase = String(s.phase || "catno");
      let cursorTxt;
      let progressPct = 0;
      if (phase === "label_sweep_masters" || phase === "label_sweep_orphans") {
        const page = Number.isFinite(Number(s.label_sweep_page)) ? Number(s.label_sweep_page) : 1;
        cursorTxt = phase === "label_sweep_masters" ? `label-sweep masters p${page}` : `label-sweep orphans p${page}`;
        progressPct = 100;
      } else if (phase === "label_sweep_done") {
        cursorTxt = "label sweep done";
        progressPct = 100;
      } else if (phase === "catno_done") {
        cursorTxt = "catno walk done";
        progressPct = 100;
      } else {
        cursorTxt = s.current_catno != null ? `catno ${s.current_catno}` : "not started";
        progressPct = s.current_catno != null && s.hi > s.lo
          ? Math.min(100, Math.max(0, Math.round(((s.current_catno - s.lo) / (s.hi - s.lo)) * 100)))
          : 0;
      }
      const sweepPageTxt = Number.isFinite(Number(s.label_sweep_page)) && Number(s.label_sweep_page) > 1
        ? ` (resume p${s.label_sweep_page})` : "";
      const buttons = !running
        ? `<button class="admin-btn" onclick="_cwcStart('${esc(s.key)}', false)" title="Walk the configured catno range">Catno walk</button>
           <button class="admin-btn" onclick="_cwcStart('${esc(s.key)}', true)" title="Reset cursor to ${s.lo} and re-walk the catno range">Restart catno</button>
           <button class="admin-btn" onclick="_cwcSweepLabel('${esc(s.key)}', false)" title="Paginated /search?label= sweep: every master, then every orphan release (no parent master).${sweepPageTxt}">Sweep label</button>
           <button class="admin-btn" onclick="_cwcSweepLabel('${esc(s.key)}', true)" title="Reset label-sweep page to 1 and re-sweep">Restart sweep</button>`
        : isActive
          ? `<button class="admin-btn" onclick="_cwcStop()">Stop</button>`
          : `<span style="color:var(--muted);font-size:0.78rem">(another series is running)</span>`;
      return `
        <tr style="${isActive ? "background:rgba(120,220,140,0.06)" : ""}">
          <td style="padding:0.4rem 0.5rem"><strong>${esc(s.label)}</strong>${s.prefix ? ` <code style="font-size:0.7rem">${esc(s.prefix)}</code>` : ""}
            <div style="font-size:0.72rem;color:var(--muted);max-width:32ch">${esc(s.notes || "")}</div>
          </td>
          <td style="padding:0.4rem 0.5rem;white-space:nowrap">${range}</td>
          <td style="padding:0.4rem 0.5rem;white-space:nowrap">${cursorTxt}<div style="background:rgba(255,255,255,0.06);height:4px;width:80px;border-radius:2px;margin-top:0.3rem;overflow:hidden"><div style="background:var(--accent);height:100%;width:${progressPct}%"></div></div></td>
          <td style="padding:0.4rem 0.5rem;text-align:right">${fmt(s.total_cached)}</td>
          <td style="padding:0.4rem 0.5rem;text-align:right">${fmt(s.total_skipped)}</td>
          <td style="padding:0.4rem 0.5rem;text-align:right">${s.total_errors ? `<span style="color:#e88">${fmt(s.total_errors)}</span>` : "0"}</td>
          <td style="padding:0.4rem 0.5rem;white-space:nowrap;display:flex;gap:0.3rem;align-items:center;flex-wrap:wrap">${buttons}<button class="admin-btn" onclick="_cwcReset('${esc(s.key)}')" title="Zero out counters + cursor for this series">Reset</button></td>
        </tr>`;
    }).join("");
    const recentBlock = (() => {
      const activeSeries = active && series.find(s => s.key === active);
      const rc = Array.isArray(activeSeries?.recent_cached) ? activeSeries.recent_cached : [];
      if (!running || !rc.length) return "";
      const items = rc.slice(0, 8).map(c => `<li style="margin:0">${esc(c.title || "")} <span style="color:var(--muted);font-size:0.7rem">#${c.id}</span></li>`).join("");
      return `<div style="margin-top:0.6rem;padding:0.5rem 0.7rem;border:1px solid var(--border);border-radius:5px;background:rgba(120,220,140,0.04)"><strong style="font-size:0.78rem">Recent hits</strong><ul style="margin:0.3rem 0 0;padding-left:1.1rem;font-size:0.76rem">${items}</ul></div>`;
    })();
    el.innerHTML = `
      <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.5rem">${runningPill}</div>
      <div style="overflow-x:auto">
      <table style="width:100%;border-collapse:collapse;font-size:0.78rem">
        <thead><tr style="text-align:left;color:var(--muted);border-bottom:1px solid var(--border)">
          <th style="padding:0.3rem 0.5rem">Series</th>
          <th style="padding:0.3rem 0.5rem">Range</th>
          <th style="padding:0.3rem 0.5rem">Cursor</th>
          <th style="padding:0.3rem 0.5rem;text-align:right">Cached</th>
          <th style="padding:0.3rem 0.5rem;text-align:right">Skipped</th>
          <th style="padding:0.3rem 0.5rem;text-align:right">Errors</th>
          <th style="padding:0.3rem 0.5rem"></th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
      </div>
      ${recentBlock}
    `;
    // Auto-poll while the worker is running so progress visibly moves.
    if (running && _adminPanelVisible() && document.getElementById("panel-labels")?.style.display !== "none") {
      clearTimeout(window._cwcPollTimer);
      window._cwcPollTimer = setTimeout(loadCacheWarmCatno, 5000);
    }
  } catch (err) {
    el.innerHTML = `<span style="color:#e88">${String(err).slice(0, 200)}</span>`;
  }
}
async function _cwcStart(seriesKey, resetCursor) {
  try {
    const r = await apiFetch("/api/admin/cache-warm-catno/start", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ seriesKey, resetCursor: !!resetCursor }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(j?.error || `Start failed: HTTP ${r.status}`); return; }
    loadCacheWarmCatno();
  } catch (err) { alert(String(err)); }
}
async function _cwcSweepLabel(seriesKey, resetCursor) {
  try {
    const r = await apiFetch("/api/admin/cache-warm-catno/start-label-sweep", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ seriesKey, resetCursor: !!resetCursor }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(j?.error || `Label sweep start failed: HTTP ${r.status}`); return; }
    loadCacheWarmCatno();
  } catch (err) { alert(String(err)); }
}
async function _cwcStop() {
  try {
    await apiFetch("/api/admin/cache-warm-catno/stop", { method: "POST" });
    loadCacheWarmCatno();
  } catch (err) { alert(String(err)); }
}
async function _cwcReset(seriesKey) {
  if (!confirm(`Reset counters and cursor for ${seriesKey}?`)) return;
  try {
    const r = await apiFetch("/api/admin/cache-warm-catno/reset", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ seriesKey }),
    });
    if (!r.ok) { alert(`Reset failed: HTTP ${r.status}`); return; }
    loadCacheWarmCatno();
  } catch (err) { alert(String(err)); }
}

// ── External discography (wirz.de + 78discography.com) ─────────────
async function loadExternalDiscography() {
  const el = document.getElementById("extdisc-content");
  if (!el) return;
  try {
    const r = await apiFetch("/api/admin/external-discography-worker/status");
    if (!r.ok) { el.innerHTML = `<span style="color:#e88">Failed: HTTP ${r.status}</span>`; return; }
    const s = await r.json();
    const running = !!s.running;
    const pct = s.total > 0 ? Math.round(100 * s.cursor / s.total) : 0;

    const recentRows = (s.recentInserted || []).slice(0, 8).map(it =>
      `<li>${_eHtml(it.source)} · ${_eHtml(it.label)} — ${it.rows} rows</li>`
    ).join("");
    const recentErrs = (s.recentErrors || []).slice(0, 8).map(it =>
      `<li><code>${_eHtml(it.slug)}</code> — ${_eHtml(it.msg)}</li>`
    ).join("");

    el.innerHTML = `
      <div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap;margin-bottom:0.6rem">
        ${running
          ? `<span style="background:#264;color:#9e8;padding:0.15rem 0.5rem;border-radius:3px;font-size:0.75rem">running · ${_eHtml(s.source || "")}</span>
             <span style="color:var(--muted);font-size:0.78rem">cursor ${s.cursor}/${s.total} (${pct}%)</span>`
          : `<span style="color:var(--muted);font-size:0.75rem">idle</span>`}
      </div>

      <div style="display:flex;gap:0.4rem;flex-wrap:wrap;margin-bottom:0.6rem">
        <button class="admin-btn" type="button" onclick="_extdiscStart('wirz')" ${running ? "disabled" : ""}>▶ Run Wirz</button>
        <button class="admin-btn" type="button" onclick="_extdiscStart('abrams')" ${running ? "disabled" : ""}>▶ Run Abrams</button>
        <button class="admin-btn" type="button" onclick="_extdiscStop()" ${running ? "" : "disabled"}>■ Stop</button>
        <button class="admin-btn" type="button" onclick="_extdiscPurge()">🧹 Purge covered</button>
      </div>

      <div style="border:1px solid var(--border);border-radius:6px;padding:0.5rem 0.7rem;background:rgba(255,255,255,0.02);margin-bottom:0.6rem">
        <div style="font-weight:600;font-size:0.82rem;margin-bottom:0.3rem">Upload Excello-style xlsx</div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr) auto;gap:0.4rem;font-size:0.78rem;align-items:end">
          <label>Label name
            <input id="extdisc-xlsx-label" type="text" value="Excello" style="width:100%;padding:0.25rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          </label>
          <label>Discogs label ID
            <input id="extdisc-xlsx-label-id" type="number" value="51225" style="width:100%;padding:0.25rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          </label>
          <label>Source tag
            <input id="extdisc-xlsx-source" type="text" value="excello-xlsx-praguefrank" style="width:100%;padding:0.25rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          </label>
          <input id="extdisc-xlsx-file" type="file" accept=".xlsx" onchange="_extdiscUploadXlsx(event)" style="font-size:0.78rem">
        </div>
        <div id="extdisc-xlsx-status" style="margin-top:0.3rem;font-size:0.78rem;color:var(--muted)"></div>
      </div>

      ${recentRows ? `<div style="font-weight:600;font-size:0.8rem">Recent ingests</div>
        <ul style="margin:0.2rem 0 0.6rem 1.2rem;font-size:0.78rem;color:var(--muted)">${recentRows}</ul>` : ""}
      ${recentErrs ? `<div style="font-weight:600;font-size:0.8rem;color:#e88">Recent errors</div>
        <ul style="margin:0.2rem 0 0 1.2rem;font-size:0.78rem;color:#caa">${recentErrs}</ul>` : ""}
    `;

    clearTimeout(window._extdiscPollTimer);
    if (running && _adminPanelVisible() && document.getElementById("panel-labels")?.style.display !== "none") {
      window._extdiscPollTimer = setTimeout(loadExternalDiscography, 5000);
    }
  } catch (err) {
    el.innerHTML = `<span style="color:#e88">${_eHtml(String(err))}</span>`;
  }
}
// Delegates to the canonical escaper in shared.js (escapes & < > " ').
// Kept as a hoisted function (not `const _eHtml = escHtml`) because
// several admin functions reference _eHtml above this line; hoisting
// keeps those safe regardless of execution order.
function _eHtml(s) {
  return escHtml(s);
}
async function _extdiscStart(source) {
  try {
    const r = await apiFetch("/api/admin/external-discography-worker/start", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ source }),
    });
    if (!r.ok) { alert(`Start failed: ${(await r.json()).error || r.status}`); return; }
    loadExternalDiscography();
  } catch (err) { alert(String(err)); }
}
async function _extdiscStop() {
  try {
    await apiFetch("/api/admin/external-discography-worker/stop", { method: "POST" });
    loadExternalDiscography();
  } catch (err) { alert(String(err)); }
}
async function _extdiscPurge() {
  if (!confirm("Delete every external_discography row whose (label, catno) is already in release_cache?")) return;
  try {
    const r = await apiFetch("/api/admin/external-discography/purge-covered", {
      method: "POST", headers: { "content-type": "application/json" },
      body: "{}",
    });
    if (!r.ok) { alert(`Purge failed: ${r.status}`); return; }
    const j = await r.json();
    alert(`Purged ${j.deleted} redundant rows.`);
    loadExternalDiscography();
  } catch (err) { alert(String(err)); }
}
async function _extdiscUploadXlsx(ev) {
  const f = ev.target.files?.[0];
  if (!f) return;
  const statusEl = document.getElementById("extdisc-xlsx-status");
  const label   = document.getElementById("extdisc-xlsx-label").value.trim();
  const labelId = document.getElementById("extdisc-xlsx-label-id").value.trim();
  const source  = document.getElementById("extdisc-xlsx-source").value.trim() || "xlsx-upload";
  if (!label) { alert("Label name required"); return; }
  statusEl.textContent = `Uploading ${f.name}…`;
  try {
    const buf = await f.arrayBuffer();
    const r = await apiFetch("/api/admin/external-discography/upload-xlsx", {
      method: "POST",
      headers: {
        "content-type": "application/octet-stream",
        "x-label":      label,
        "x-label-id":   labelId,
        "x-source":     source,
      },
      body: buf,
    });
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      statusEl.innerHTML = `<span style="color:#e88">Upload failed: ${_eHtml(j.error || r.status)}</span>`;
      return;
    }
    const j = await r.json();
    statusEl.innerHTML = `<span style="color:#9e8">Parsed ${j.parsed} rows · inserted ${j.inserted}</span>`;
    ev.target.value = "";
    loadExternalDiscography();
  } catch (err) {
    statusEl.innerHTML = `<span style="color:#e88">${_eHtml(String(err))}</span>`;
  }
}

// ── Label directory (Labels tab) ────────────────────────────────
let _labelDirRows = [];
let _labelDirQuery = "";
let _labelDirSearchOpen = null;        // label_name currently expanded
let _labelDirSearchResults = null;     // {[label_name]: [...candidates]}
let _labelDirSearchLoading = false;
let _labelDirQueryTimer = null;
let _labelDirSortKey = "upstream_total";
let _labelDirSortDir = "desc";
let _labelDirUnassignedOnly = false;
let _labelDirActiveSweepKey = null;    // cache_warm_catno_runs.series_key currently running
let _labelDirMergeOpen = null;          // label_name of the row whose merge popover is open
let _labelDirMergeQuery = "";           // filter text inside the merge popover
let _labelDirAliasOpen  = null;         // {label_name, label_id} whose alias popover is open
let _labelDirAliasQuery = "";           // filter text inside the alias popover
let _labelDirBulkStatus = null;         // last poll of /bulk-sweep/status

async function loadLabelDirectory() {
  const el = document.getElementById("labeldir-content");
  if (!el) return;
  try {
    const url = `/api/admin/label-directory?limit=2000${_labelDirQuery ? `&q=${encodeURIComponent(_labelDirQuery)}` : ""}`;
    // Fetch directory + catno-worker + bulk-sweep status in parallel
    // so the row badges, banners, and bulk progress bar stay in sync.
    const [r, statusR, bulkR] = await Promise.all([
      apiFetch(url),
      apiFetch("/api/admin/cache-warm-catno/status").catch(() => null),
      apiFetch("/api/admin/label-directory/bulk-sweep/status").catch(() => null),
    ]);
    if (!r.ok) { el.innerHTML = `<span style="color:#e88">Failed: HTTP ${r.status}</span>`; return; }
    const j = await r.json();
    _labelDirRows = j.rows || [];
    _labelDirActiveSweepKey = null;
    if (statusR?.ok) {
      const sj = await statusR.json().catch(() => null);
      _labelDirActiveSweepKey = sj?.running ? (sj?.active || null) : null;
    }
    _labelDirBulkStatus = null;
    if (bulkR?.ok) {
      _labelDirBulkStatus = await bulkR.json().catch(() => null);
    }
    _renderLabelDir();
    // Auto-poll while any sweep or bulk queue is running.
    clearTimeout(window._labelDirPollTimer);
    const anyRunning = _labelDirActiveSweepKey || _labelDirBulkStatus?.running;
    if (anyRunning && _adminPanelVisible() && document.getElementById("panel-labels")?.style.display !== "none") {
      window._labelDirPollTimer = setTimeout(loadLabelDirectory, 5000);
    }
  } catch (err) {
    el.innerHTML = `<span style="color:#e88">${_eHtml(String(err))}</span>`;
  }
}

function _renderLabelDir() {
  const el = document.getElementById("labeldir-content");
  if (!el) return;
  // Capture scroll position of the table container + page before we
  // blow away innerHTML, so save/clear interactions don't yank the
  // user back to the top of the grid.
  const prevScroll = document.getElementById("labeldir-scroll")?.scrollTop ?? 0;
  const prevPage   = window.scrollY;
  const withId    = _labelDirRows.filter(r => r.label_id).length;
  const withoutId = _labelDirRows.length - withId;
  el.innerHTML = `
    <div style="display:flex;gap:0.5rem;align-items:center;margin-bottom:0.5rem;flex-wrap:wrap">
      <input id="labeldir-search" type="search" placeholder="Filter labels…" value="${_eHtml(_labelDirQuery)}"
             oninput="_labelDirOnSearch(event)"
             style="flex:1;min-width:180px;padding:0.3rem 0.5rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
      <label style="display:flex;align-items:center;gap:0.3rem;font-size:0.78rem;color:var(--muted);white-space:nowrap;cursor:pointer">
        <input type="checkbox" ${_labelDirUnassignedOnly ? "checked" : ""} onchange="_labelDirToggleUnassigned(event)">
        Unassigned only
      </label>
      <button class="admin-btn" type="button" onclick="_labelDirCleanNames()" title="Strip HTML/whitespace from any label_name that has it (one-shot fix for dirty seed entries).">🧼 Clean names</button>
      <button class="admin-btn" type="button" onclick="_labelDirBulkSweepStart(false)" ${_labelDirBulkStatus?.running ? "disabled" : ""}
        title="Iterate every label with a Discogs ID, biggest Discogs catalog first (labels whose upstream total hasn't been fetched fall back to pad-row order and land after the known ones — run 📊 Fetch upstream totals first). Labels swept within the 'skip' window are dropped from the queue. Resumes from the persisted cursor if one exists.">⏵ Bulk masters+ sweep</button>
      <button class="admin-btn" type="button" onclick="_labelDirBulkSweepStart(true)" ${_labelDirBulkStatus?.running ? "disabled" : ""}
        title="Rebuild the queue and start from cursor 0. Use if the directory changed and you want the new labels included from the top.">↻ Restart from top</button>
      <label style="display:flex;align-items:center;gap:0.3rem;font-size:0.78rem;color:var(--muted);white-space:nowrap" title="Labels whose last successful sweep is newer than this window are dropped from the bulk queue entirely. Set to 0 to sweep every label regardless.">
        skip if swept in last
        <input id="bulk-skip-days" type="number" min="0" step="1" value="14" style="width:3.5rem;padding:0.15rem 0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
        days
      </label>
      <button class="admin-btn" type="button" onclick="_labelUpstreamStatsStart(false)" title="Fetch Discogs's total release count for labels missing it (or last fetched more than the stale window ago). One API call per label at ~1 req/sec; populates the 'Discogs total' column. Progress persists across redeploys.">📊 Fetch upstream totals</button>
      <span style="font-size:0.78rem;color:var(--muted)">${_labelDirRows.length} labels · ${withId} with ID · ${withoutId} unassigned</span>
    </div>
    ${_labelDirBulkStatus?.running ? _labelDirBulkBannerHtml() : ""}
    ${(_labelDirActiveSweepKey && !_labelDirBulkStatus?.running) ? `
      <div style="display:flex;gap:0.4rem;align-items:center;padding:0.4rem 0.6rem;margin-bottom:0.5rem;background:rgba(160,60,40,0.15);border:1px solid #a44;border-radius:4px;flex-wrap:wrap">
        <span style="font-size:0.82rem"><strong>Sweep running:</strong> ${
          _labelDirActiveSweepKey.startsWith('adhoc:')
            ? '<em>ad-hoc label sweep</em>'
            : `<code>${_eHtml(_labelDirActiveSweepKey)}</code>`
        }</span>
        <button class="admin-btn" type="button" onclick="_labelDirStopSweep()" style="background:#a33;color:#fff">■ Stop sweep</button>
        <button class="admin-btn" type="button" onclick="_labelDirForceClearSweep()" title="Only if Stop doesn't take. Clears the in-memory + persisted 'active' flag so a new sweep can start; the running IIFE may still be processing its current page.">⚠ Force clear</button>
      </div>` : ""}
    <div id="labeldir-scroll" style="overflow:auto;max-height:60vh;border:1px solid var(--border);border-radius:4px">
      <table style="width:100%;font-size:0.8rem;border-collapse:collapse;table-layout:fixed">
        <colgroup>
          <col style="width:26%">
          <col style="width:78px">
          <col style="width:78px">
          <col style="width:88px">
          <col style="width:16%">
          <col style="width:106px">
          <col style="width:170px">
        </colgroup>
        <thead style="position:sticky;top:0;background:var(--surface);z-index:1">
          <tr style="text-align:left;color:var(--muted);border-bottom:1px solid var(--border)">
            ${_labelDirHeader("label_name",     "Label",         "left")}
            ${_labelDirHeader("cache_releases", "Cache R",       "right")}
            ${_labelDirHeader("cache_masters",  "Cache M",       "right")}
            ${_labelDirHeader("upstream_total", "Discogs total", "right")}
            ${_labelDirHeader("sources",        "Sources",       "left")}
            ${_labelDirHeader("label_id",       "Discogs ID",    "left")}
            <th style="padding:0.35rem 0.5rem"></th>
          </tr>
        </thead>
        <tbody>
          ${_labelDirSortedRows().map(r => _labelDirRowHtml(r)).join("")}
        </tbody>
      </table>
    </div>
    ${_labelDirPopoverHtml()}
    ${_labelDirMergePopoverHtml()}
    ${_labelDirAliasPopoverHtml()}
  `;
  // Restore scroll on next frame so the freshly-rendered DOM has
  // measured itself before we set scrollTop.
  requestAnimationFrame(() => {
    const sc = document.getElementById("labeldir-scroll");
    if (sc) sc.scrollTop = prevScroll;
    if (Math.abs(window.scrollY - prevPage) > 4) window.scrollTo(window.scrollX, prevPage);
  });
}

function _labelDirPopoverHtml() {
  if (!_labelDirSearchOpen) return "";
  const labelName = _labelDirSearchOpen;
  const results = _labelDirSearchResults?.[labelName];
  const body = _labelDirSearchLoading
    ? `<div style="color:var(--muted);padding:0.6rem 0">Searching Discogs…</div>`
    : results === null
      ? `<div style="color:#e88;padding:0.6rem 0">Search failed.</div>`
      : !results?.length
        ? `<div style="color:var(--muted);padding:0.6rem 0">No Discogs label matches.</div>`
        : `<div style="display:flex;flex-direction:column;gap:0.3rem;max-height:50vh;overflow-y:auto">
            ${results.map(c => `<div style="display:flex;gap:0.5rem;align-items:center;padding:0.3rem 0.4rem;border:1px solid var(--border);border-radius:3px">
              ${c.thumb ? `<img src="${_eHtml(c.thumb)}" alt="" style="width:32px;height:32px;object-fit:cover;border-radius:2px">` : `<div style="width:32px;height:32px;background:rgba(255,255,255,0.05);border-radius:2px"></div>`}
              <div style="flex:1;min-width:0">
                <div style="font-weight:600">${_eHtml(c.title)}</div>
                <div style="font-size:0.72rem;color:var(--muted)">ID ${c.id} · <a href="https://www.discogs.com${_eHtml(c.uri)}" target="_blank" rel="noopener" style="color:var(--muted)">view ↗</a></div>
              </div>
              <button class="admin-btn" type="button" onclick="_labelDirSaveId('${_jsEsc(labelName)}', ${c.id})">Save</button>
            </div>`).join("")}
          </div>`;
  return `
    <div onclick="_labelDirCloseSearch()" style="position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:80"></div>
    <div role="dialog" style="position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);width:min(560px,92vw);max-height:80vh;overflow:auto;background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:0.9rem 1rem;z-index:81;box-shadow:0 8px 32px rgba(0,0,0,0.4)">
      <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:0.5rem;gap:0.5rem">
        <div style="font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_eHtml(labelName)}">Discogs label search · "${_eHtml(labelName.slice(0, 80))}${labelName.length > 80 ? "…" : ""}"</div>
        <button class="admin-btn" type="button" onclick="_labelDirCloseSearch()" style="font-size:0.78rem">✕</button>
      </div>
      ${body}
      <div style="margin-top:0.5rem;display:flex;gap:0.4rem;align-items:center;flex-wrap:wrap">
        <input data-labeldir-manual="${_eHtml(labelName)}" type="number" placeholder="…or enter ID manually" style="flex:1;min-width:160px;padding:0.3rem;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:3px;font-size:0.82rem">
        <button class="admin-btn" type="button" onclick="_labelDirSaveIdManual('${_jsEsc(labelName)}')">Save manual ID</button>
      </div>
    </div>
  `;
}

function _labelDirAliasPopoverHtml() {
  if (!_labelDirAliasOpen) return "";
  const from = _labelDirAliasOpen;   // { label_name, label_id }
  const q = _labelDirAliasQuery.trim().toLowerCase();
  // Candidates: any OTHER row with a label_id set.
  const candidates = _labelDirRows
    .filter(r => r.label_id && r.label_id !== from.label_id)
    .filter(r => !q || r.label_name.toLowerCase().includes(q) || String(r.label_id).includes(q))
    .slice(0, 80);
  const listBody = candidates.length
    ? `<div style="display:flex;flex-direction:column;gap:0.2rem;max-height:44vh;overflow-y:auto">
        ${candidates.map(c => `<button type="button" onclick="_labelDirConfirmAlias(${from.label_id}, ${c.label_id})"
            style="display:flex;justify-content:space-between;align-items:center;gap:0.5rem;padding:0.35rem 0.5rem;background:rgba(255,255,255,0.03);color:var(--text);border:1px solid var(--border);border-radius:3px;cursor:pointer;text-align:left">
            <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_eHtml(c.label_name)}">${_eHtml(c.label_name.length > 60 ? c.label_name.slice(0, 60) + "…" : c.label_name)}</span>
            <span style="color:var(--muted);font-size:0.72rem;white-space:nowrap">ID ${c.label_id} · pad ${c.external_count} · R ${c.cache_releases}</span>
          </button>`).join("")}
      </div>`
    : `<div style="color:var(--muted);padding:0.6rem 0">No candidate labels with an ID.</div>`;
  return `
    <div onclick="_labelDirCloseAlias()" style="position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:80"></div>
    <div role="dialog" style="position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);width:min(560px,92vw);max-height:80vh;display:flex;flex-direction:column;background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:0.9rem 1rem;z-index:81;box-shadow:0 8px 32px rgba(0,0,0,0.4)">
      <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:0.4rem;gap:0.5rem">
        <div style="font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_eHtml(from.label_name)}">Alias "${_eHtml(from.label_name)}" (ID ${from.label_id}) under…</div>
        <button class="admin-btn" type="button" onclick="_labelDirCloseAlias()" style="font-size:0.78rem">✕</button>
      </div>
      <input type="search" oninput="_labelDirAliasFilter(event)" autofocus
             placeholder="Filter target labels…" value="${_eHtml(_labelDirAliasQuery)}"
             style="margin-bottom:0.5rem;padding:0.35rem 0.5rem;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:3px">
      ${listBody}
      <div style="margin-top:0.4rem;color:var(--muted);font-size:0.72rem">Groups both Discogs IDs under the target for display. Underlying data is untouched — un-alias with the × next to the entry on the canonical row.</div>
    </div>
  `;
}

function _labelDirMergePopoverHtml() {
  if (!_labelDirMergeOpen) return "";
  const fromName = _labelDirMergeOpen;
  const fromRow  = _labelDirRows.find(r => r.label_name === fromName);
  const q = _labelDirMergeQuery.trim().toLowerCase();
  // Candidates: every OTHER label, optionally filtered by substring.
  const candidates = _labelDirRows
    .filter(r => r.label_name !== fromName)
    .filter(r => !q || r.label_name.toLowerCase().includes(q))
    .slice(0, 80);
  const listBody = candidates.length
    ? `<div style="display:flex;flex-direction:column;gap:0.2rem;max-height:46vh;overflow-y:auto">
        ${candidates.map(c => `<button type="button" onclick="_labelDirConfirmMerge('${_jsEsc(fromName)}', '${_jsEsc(c.label_name)}')"
            style="display:flex;justify-content:space-between;align-items:center;gap:0.5rem;padding:0.35rem 0.5rem;background:rgba(255,255,255,0.03);color:var(--text);border:1px solid var(--border);border-radius:3px;cursor:pointer;text-align:left">
            <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_eHtml(c.label_name)}">${_eHtml(c.label_name.length > 60 ? c.label_name.slice(0, 60) + "…" : c.label_name)}</span>
            <span style="color:var(--muted);font-size:0.72rem;white-space:nowrap">
              ${c.label_id ? `ID ${c.label_id} · ` : ""}pad ${c.external_count} · R ${c.cache_releases}
            </span>
          </button>`).join("")}
      </div>`
    : `<div style="color:var(--muted);padding:0.6rem 0">No matching labels.</div>`;
  const fromMeta = fromRow
    ? `pad ${fromRow.external_count} · cache R ${fromRow.cache_releases}${fromRow.label_id ? ` · ID ${fromRow.label_id}` : ""}`
    : "";
  return `
    <div onclick="_labelDirCloseMerge()" style="position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:80"></div>
    <div role="dialog" style="position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);width:min(560px,92vw);max-height:80vh;display:flex;flex-direction:column;background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:0.9rem 1rem;z-index:81;box-shadow:0 8px 32px rgba(0,0,0,0.4)">
      <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:0.4rem;gap:0.5rem">
        <div style="font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_eHtml(fromName)}">Merge "${_eHtml(fromName.length > 50 ? fromName.slice(0, 50) + "…" : fromName)}" into…</div>
        <button class="admin-btn" type="button" onclick="_labelDirCloseMerge()" style="font-size:0.78rem">✕</button>
      </div>
      <div style="color:var(--muted);font-size:0.78rem;margin-bottom:0.5rem">${fromMeta}</div>
      <input type="search" oninput="_labelDirMergeFilter(event)" autofocus
             placeholder="Filter target labels…" value="${_eHtml(_labelDirMergeQuery)}"
             style="margin-bottom:0.5rem;padding:0.35rem 0.5rem;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:3px">
      ${listBody}
      <div style="margin-top:0.4rem;color:var(--muted);font-size:0.72rem">Renames every external_discography row from the source label to the target. Catno collisions are merged: the source row wins, the colliding target row is dropped. Discogs ID propagates from the target (or source if target had none).</div>
    </div>
  `;
}

function _labelDirHeader(key, label, align) {
  const active = _labelDirSortKey === key;
  const arrow  = active ? (_labelDirSortDir === "asc" ? " ▲" : " ▼") : "";
  return `<th onclick="_labelDirToggleSort('${key}')"
              style="padding:0.35rem 0.5rem;text-align:${align};cursor:pointer;user-select:none;${active ? "color:var(--text)" : ""}">
            ${label}${arrow}
          </th>`;
}

function _labelDirSortedRows() {
  let rows = _labelDirRows.slice();
  if (_labelDirUnassignedOnly) rows = rows.filter(r => !r.label_id);
  const key = _labelDirSortKey;
  const dir = _labelDirSortDir === "asc" ? 1 : -1;
  const sortValue = (row) => {
    const v = row[key];
    if (Array.isArray(v)) return v.join(", ");   // Sources column
    return v;
  };
  rows.sort((a, b) => {
    const av = sortValue(a);
    const bv = sortValue(b);
    // Treat null AND empty string as "missing" so unassigned rows
    // (which is what the user is usually triaging) sort to the bottom.
    const aMissing = av == null || av === "";
    const bMissing = bv == null || bv === "";
    if (aMissing && bMissing) return 0;
    if (aMissing) return 1;
    if (bMissing) return -1;
    if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
    return String(av).localeCompare(String(bv), undefined, { sensitivity: "base" }) * dir;
  });
  return rows;
}

function _labelDirToggleSort(key) {
  if (_labelDirSortKey === key) {
    _labelDirSortDir = _labelDirSortDir === "asc" ? "desc" : "asc";
  } else {
    _labelDirSortKey = key;
    // Numeric columns default to desc; text columns default to asc.
    _labelDirSortDir = (key === "label_name" || key === "sources") ? "asc" : "desc";
  }
  _renderLabelDir();
}

function _labelDirRowHtml(r) {
  const sources = (r.sources || []).map(s => _eHtml(s)).join(", ");
  const idCell = r.label_id
    ? `<a href="https://www.discogs.com/label/${r.label_id}" target="_blank" rel="noopener" style="color:var(--accent)">${r.label_id}</a>
       <button class="admin-btn" type="button" onclick="_labelDirClearId('${_jsEsc(r.label_name)}')" style="font-size:0.7rem;padding:0.1rem 0.3rem;margin-left:0.3rem" title="Clear ID">×</button>`
    : `<span style="color:var(--muted);font-size:0.78rem">—</span>`;
  // Popover renders outside the table — see _labelDirRenderPopover.
  // Keeping it inline (as a <tr> inset) caused the fixed-layout table
  // to reflow and the sticky thead to recalculate position on every
  // expand/collapse, which the user perceived as the grid "shifting".
  const searchBlock = "";
  // Defensive render: strip HTML tags + collapse whitespace + truncate
  // so a dirty-data row (e.g. label_name still has HTML chrome before
  // Clean names has been clicked) can't wreck the table layout.
  const cleanName = String(r.label_name || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const displayName = cleanName.length > 70 ? cleanName.slice(0, 70) + "…" : cleanName;
  const isDirty = cleanName !== String(r.label_name || "").trim();
  const dirtyHint = isDirty
    ? `<span title="HTML/whitespace in source — click 🧼 Clean names to fix" style="color:#ea8;font-size:0.72rem;margin-left:0.3rem">⚠</span>`
    : "";
  // Aliases sub-line: shows on canonical rows so the curator sees
  // which Discogs IDs got folded in. Click × to un-alias.
  const aliasesLine = (r.aliases && r.aliases.length)
    ? `<div style="font-size:0.7rem;color:var(--muted);margin-top:0.15rem">
         aliases: ${r.aliases.map(a => `
           <span title="${a.reason ? _eHtml(a.reason) : ''}" style="margin-right:0.4rem">
             <a href="https://www.discogs.com/label/${a.label_id}" target="_blank" rel="noopener" style="color:var(--muted)">${_eHtml(a.label_name)}</a>
             <span style="color:#777">·id ${a.label_id}</span>
             <button class="admin-btn" type="button" onclick="_labelDirRemoveAlias(${a.label_id})" title="Un-alias" style="font-size:0.62rem;padding:0 0.25rem;margin-left:0.1rem">×</button>
           </span>`).join('')}
       </div>`
    : "";
  return `
    <tr style="border-bottom:1px dashed rgba(255,255,255,0.05)">
      <td style="padding:0.3rem 0.5rem;font-weight:500;overflow:hidden;text-overflow:ellipsis" title="${_eHtml(cleanName)}">
        <div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${_eHtml(displayName)}${dirtyHint}</div>
        ${aliasesLine}
      </td>
      <td style="padding:0.3rem 0.5rem;text-align:right">${r.cache_releases.toLocaleString()}</td>
      <td style="padding:0.3rem 0.5rem;text-align:right">${r.cache_masters.toLocaleString()}</td>
      <td style="padding:0.3rem 0.5rem;text-align:right${r.upstream_total == null ? ';color:var(--muted)' : ''}" title="${r.upstream_fetched_at ? `Fetched ${new Date(r.upstream_fetched_at).toLocaleString()}` : (r.label_id ? 'Not fetched yet — run Fetch upstream totals in the toolbar' : 'No Discogs ID assigned')}">${r.upstream_total == null ? '—' : Number(r.upstream_total).toLocaleString()}</td>
      <td style="padding:0.3rem 0.5rem;color:var(--muted);font-size:0.72rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_eHtml((r.sources || []).join(', '))}">${sources}</td>
      <td style="padding:0.3rem 0.5rem">${idCell}</td>
      <td style="padding:0.3rem 0.5rem;text-align:right;white-space:nowrap">
        ${_labelDirSweepCell(r)}
        <button class="admin-btn" type="button" onclick="_labelDirSearch('${_jsEsc(r.label_name)}')" style="font-size:0.78rem;padding:0.15rem 0.4rem" title="Search Discogs for this label">🔍</button>
        <button class="admin-btn" type="button" onclick="_labelDirOpenMerge('${_jsEsc(r.label_name)}')" style="font-size:0.78rem;padding:0.15rem 0.4rem;margin-left:0.2rem" title="Merge (rename external rows into another label)">⇄</button>
        ${r.label_id ? `<button class="admin-btn" type="button" onclick="_labelDirOpenAlias('${_jsEsc(r.label_name)}', ${r.label_id})" style="font-size:0.78rem;padding:0.15rem 0.4rem;margin-left:0.2rem" title="Alias this Discogs ID under another (fold two IDs for the same conceptual label together)">🔗</button>` : ""}
      </td>
    </tr>
    ${searchBlock}
  `;
}

function _labelDirSweepCell(r) {
  if (!r.label_id) return "";
  const myKey   = `adhoc:${r.label_id}`;
  const someoneRunning = !!_labelDirActiveSweepKey;
  const meRunning      = _labelDirActiveSweepKey === myKey;
  if (meRunning) {
    return `<span style="font-size:0.72rem;color:#9e8;background:#264;padding:0.1rem 0.4rem;border-radius:3px;margin-right:0.3rem" title="Sweep in progress — view in Catalog-number crawls">running…</span>`;
  }
  const sweptBadge = r.swept_at
    ? `<span style="font-size:0.72rem;color:#9e8;background:rgba(80,160,80,0.15);border:1px solid #4a8;padding:0.1rem 0.4rem;border-radius:3px;margin-right:0.3rem" title="Sweep completed ${new Date(r.swept_at).toLocaleString()}">swept</span>`
    : "";
  // Curated catno-series keys (e.g. excello:2000-2400) overlap by labelId
  // too — let the curator still kick the ad-hoc path if needed, but
  // disable while ANY worker is active so we don't race.
  const dis = someoneRunning ? "disabled" : "";
  const title = someoneRunning
    ? `Another sweep is running (${_labelDirActiveSweepKey}). Wait or stop it first.`
    : r.swept_at
      ? `Re-sweep (last completed ${new Date(r.swept_at).toLocaleString()})`
      : `Sweep every master Discogs has tagged with this label, plus any orphan releases (no parent master), into the cache.`;
  return `${sweptBadge}<button class="admin-btn" type="button" ${dis}
            onclick="_labelDirStartSweep(${r.label_id}, '${_jsEsc(r.label_name)}')"
            title="${_eHtml(title)}"
            style="font-size:0.78rem;padding:0.15rem 0.4rem;margin-right:0.3rem">▶ Sweep</button>`;
}

function _jsEsc(s) {
  return String(s ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function _labelDirOnSearch(ev) {
  _labelDirQuery = String(ev.target.value || "").trim();
  clearTimeout(_labelDirQueryTimer);
  _labelDirQueryTimer = setTimeout(() => loadLabelDirectory(), 250);
}

function _labelDirToggleUnassigned(ev) {
  _labelDirUnassignedOnly = !!ev.target.checked;
  _renderLabelDir();
}

function _labelDirBulkBannerHtml() {
  const s = _labelDirBulkStatus || {};
  const cur = s.currentLabel;
  const pct = s.total > 0 ? Math.round((s.cursor / s.total) * 100) : 0;
  const nextPreview = (s.remaining || []).slice(0, 3)
    .map(q => `${q.labelName} (${q.externalCount})`).join(", ");
  return `
    <div style="display:flex;gap:0.5rem;align-items:center;padding:0.4rem 0.6rem;margin-bottom:0.5rem;background:rgba(60,120,200,0.15);border:1px solid #48a;border-radius:4px;flex-wrap:wrap">
      <span style="font-size:0.82rem"><strong>Bulk sweep:</strong> ${s.cursor}/${s.total} (${pct}%)${s.completed != null ? ` · ✓${s.completed}` : ""}${s.errors ? ` · ⚠${s.errors}` : ""}</span>
      ${cur ? `<span style="font-size:0.78rem;color:var(--muted)">current: <code>${_eHtml(cur.labelName)}</code> · pad ${cur.externalCount}</span>` : ""}
      ${nextPreview ? `<span style="font-size:0.72rem;color:var(--muted)" title="${_eHtml(nextPreview)}">next: ${_eHtml(nextPreview.length > 60 ? nextPreview.slice(0, 60) + "…" : nextPreview)}</span>` : ""}
      <button class="admin-btn" type="button" onclick="_labelDirBulkSweepStop()" style="background:#a33;color:#fff">■ Stop bulk</button>
      <button class="admin-btn" type="button" onclick="_labelDirBulkSweepForceClear()" title="Only if Stop doesn't take. Wipes bulk state without touching the inner sweep.">⚠ Force clear</button>
    </div>
  `;
}

async function _labelDirBulkSweepStart(resetCursor) {
  // One quick preview call so the confirm can show the queue size.
  let totalWithId = null, alreadySwept = null;
  const skipDaysRaw = Number(document.getElementById("bulk-skip-days")?.value);
  const skipSweptSinceDays = Number.isFinite(skipDaysRaw) && skipDaysRaw >= 0 ? skipDaysRaw : 14;
  const cutoffMs = skipSweptSinceDays > 0 ? Date.now() - skipSweptSinceDays * 86_400_000 : 0;
  try {
    const r = await apiFetch("/api/admin/label-directory?limit=20000");
    if (r.ok) {
      const j = await r.json();
      const rows = (j.rows || []).filter(x => x.label_id);
      totalWithId = rows.length;
      if (skipSweptSinceDays > 0) {
        alreadySwept = rows.filter(x => x.swept_at && Date.parse(x.swept_at) >= cutoffMs).length;
      }
    }
  } catch {}
  const verb = resetCursor ? "Restart from top of" : "Start (or resume)";
  const skipLine = skipSweptSinceDays > 0
    ? `\nSkipping labels swept in the last ${skipSweptSinceDays} day${skipSweptSinceDays === 1 ? "" : "s"}${alreadySwept != null ? ` (~${alreadySwept} of ${totalWithId} will drop)` : ""}.`
    : `\nSweeping every label regardless of recent sweep history.`;
  const msg = totalWithId != null
    ? `${verb} bulk masters+ sweep across ${totalWithId} labels with a Discogs ID, biggest Discogs catalog first (un-fetched totals fall back to pad-row order, last)?${skipLine}\n\nEach label runs the same masters + orphans sweep as the per-row ▶ button. ${resetCursor ? "Cursor resets to 0." : "Progress resumes from the persisted cursor."} Stop at any time.`
    : `${verb} bulk masters+ sweep across every label with a Discogs ID?${skipLine}`;
  if (!confirm(msg)) return;
  try {
    const r = await apiFetch("/api/admin/label-directory/bulk-sweep/start", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ minExternalCount: 0, resetCursor: !!resetCursor, skipSweptSinceDays }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Bulk start failed: ${j.error || r.status}`); return; }
    if (Number.isFinite(j?.skippedSwept) && j.skippedSwept > 0) {
      // Non-blocking hint so the user knows the queue is smaller than
      // the directory total by design, not by bug.
      if (typeof showToast === "function") showToast(`Bulk sweep started — ${j.queued} queued (${j.skippedSwept} recently-swept labels skipped)`);
    }
    loadLabelDirectory();
    if (typeof loadCacheWarmCatno === "function") loadCacheWarmCatno();
    if (typeof loadAdminWorkerStatus === "function") loadAdminWorkerStatus();
  } catch (err) { alert(String(err)); }
}

async function _labelDirBulkSweepStop() {
  if (!confirm("Stop the bulk sweep queue?\n\nThis also stops the currently-running per-label sweep. Queue state is kept so you can inspect where it stopped, but starting a new bulk sweep rebuilds the queue from scratch.")) return;
  try {
    const r = await apiFetch("/api/admin/label-directory/bulk-sweep/stop", { method: "POST" });
    if (!r.ok) { alert(`Stop failed: ${r.status}`); return; }
    loadLabelDirectory();
  } catch (err) { alert(String(err)); }
}

async function _labelDirBulkSweepForceClear() {
  if (!confirm("Force-clear bulk sweep state?\n\nOnly use if Stop didn't take. The current inner per-label sweep is NOT touched (use ■ Stop sweep for that).")) return;
  try {
    const r = await apiFetch("/api/admin/label-directory/bulk-sweep/force-clear", { method: "POST" });
    if (!r.ok) { alert(`Force-clear failed: ${r.status}`); return; }
    loadLabelDirectory();
  } catch (err) { alert(String(err)); }
}

async function _labelUpstreamStatsStart(resetCursor) {
  const staleDaysRaw = 30;   // matches the worker's default; server ignores if we omit
  if (!confirm(`Fetch Discogs upstream release totals for labels missing them (or last fetched >${staleDaysRaw} days ago)?\n\nOne API call per label at ~1 req/sec; ~35min for the full universe. Progress persists across redeploys. Populates the 'Discogs total' column.`)) return;
  try {
    const r = await apiFetch("/api/admin/label-upstream-stats/start", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ staleAfterDays: staleDaysRaw, resetCursor: !!resetCursor }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      const msg = j?.error || `HTTP ${r.status}`;
      // 409 with 'no labels need refresh' is a benign "everything's already fresh".
      alert(String(msg).includes("no labels") ? `Nothing to fetch — every label already has a fresh upstream total.` : `Start failed: ${msg}`);
      return;
    }
    loadLabelDirectory();
    if (typeof loadAdminWorkerStatus === "function") loadAdminWorkerStatus();
  } catch (err) { alert(String(err)); }
}
async function _labelUpstreamStatsStop() {
  const r = await apiFetch("/api/admin/label-upstream-stats/stop", { method: "POST" });
  if (!r.ok) { alert(`Stop failed: ${r.status}`); return; }
  if (typeof loadAdminWorkerStatus === "function") loadAdminWorkerStatus();
}
async function _labelUpstreamStatsForceClear() {
  if (!confirm("Force-clear upstream-stats worker state? Only if Stop didn't take.")) return;
  const r = await apiFetch("/api/admin/label-upstream-stats/force-clear", { method: "POST" });
  if (!r.ok) { alert(`Force-clear failed: ${r.status}`); return; }
  if (typeof loadAdminWorkerStatus === "function") loadAdminWorkerStatus();
}
window._labelUpstreamStatsStart = _labelUpstreamStatsStart;
window._labelUpstreamStatsStop = _labelUpstreamStatsStop;
window._labelUpstreamStatsForceClear = _labelUpstreamStatsForceClear;

// ── Coverage sweeps (artist / faceted)
let _covFaceted = null;
async function loadCoverageSweeps() {
  const el = document.getElementById("coverage-sweeps-content");
  if (!el) return;
  try {
    const fR = await apiFetch("/api/admin/faceted-sweep/status").catch(() => null);
    _covFaceted = fR?.ok ? await fR.json() : null;
    _renderCoverageSweeps();
    clearTimeout(window._covPollTimer);
    const anyRunning = _covFaceted?.running;
    if (anyRunning && _adminPanelVisible() && document.getElementById("panel-labels")?.style.display !== "none") {
      window._covPollTimer = setTimeout(loadCoverageSweeps, 5000);
    }
  } catch (err) {
    el.innerHTML = `<span style="color:#e88">${_eHtml(String(err))}</span>`;
  }
}
window.loadCoverageSweeps = loadCoverageSweeps;

function _renderCoverageSweeps() {
  const el = document.getElementById("coverage-sweeps-content");
  if (!el) return;
  const f = _covFaceted || {};
  const pct = (cur, tot) => tot > 0 ? Math.round((cur / tot) * 100) : 0;
  el.innerHTML = `
    <div style="display:flex;flex-direction:column;gap:0.7rem">

      <!-- Faceted (year × format / country) -->
      <div style="border:1px solid var(--border);border-radius:5px;padding:0.5rem 0.7rem;background:rgba(255,255,255,0.02)">
        <div style="display:flex;gap:0.4rem;align-items:center;flex-wrap:wrap">
          <strong>🗓 Year × facet sweep</strong>
          <span style="font-size:0.72rem;color:var(--muted)">
            /database/search?type=master&amp;year=YYYY&amp;(format|country)=X — surfaces records the year × genre + label sweeps miss under Discogs pagination cap
          </span>
        </div>
        <div style="display:flex;gap:0.4rem;align-items:center;flex-wrap:wrap;margin-top:0.4rem">
          <label style="font-size:0.78rem;color:var(--muted)">mode
            <select id="cov-fc-mode" onchange="_covFacetedSyncMode()" style="padding:0.15rem 0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
              <option value="format" ${f.mode === "format" ? "selected" : ""}>format</option>
              <option value="country" ${f.mode === "country" ? "selected" : ""}>country</option>
            </select>
          </label>
          <label style="font-size:0.78rem;color:var(--muted)">yearFrom
            <input id="cov-fc-yfrom" type="number" value="1900" style="width:5rem;padding:0.15rem 0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          </label>
          <label style="font-size:0.78rem;color:var(--muted)">yearTo
            <input id="cov-fc-yto" type="number" value="1970" style="width:5rem;padding:0.15rem 0.3rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          </label>
          ${f.running
            ? `<span style="font-size:0.78rem;color:var(--muted)">progress: ${f.cursor}/${f.total} (${pct(f.cursor,f.total)}%)${f.currentSlot ? ` · slot: ${_eHtml(f.currentSlot.value)}/${f.currentSlot.year}` : ""} · ✓${f.hits} · ⏭${f.skipped} · ⚠${f.errors}</span>
               <button class="admin-btn" type="button" onclick="_covFacetedStop()" style="background:#a33;color:#fff">■ Stop</button>
               <button class="admin-btn" type="button" onclick="_covFacetedForceClear()">⚠ Force clear</button>`
            : `<button class="admin-btn" type="button" onclick="_covFacetedStart(false)">⏵ Start / resume</button>
               <button class="admin-btn" type="button" onclick="_covFacetedStart(true)" title="Rebuild the (year, value) queue and start from cursor 0.">↻ Restart from top</button>
               ${f.total ? `<span style="font-size:0.72rem;color:var(--muted)">last: ${f.hits ?? 0} hits · ${f.skipped ?? 0} skipped</span>` : ""}`}
        </div>
        <div style="margin-top:0.4rem;font-size:0.72rem;color:var(--muted)">values (leave all unchecked to use defaults · toggle-all buttons at right)</div>
        <div id="cov-fc-values-format" style="display:${f.mode === "country" ? "none" : "flex"};flex-wrap:wrap;gap:0.25rem 0.6rem;margin-top:0.3rem;max-height:8rem;overflow:auto;padding:0.3rem 0.4rem;border:1px solid var(--border);border-radius:3px;background:var(--surface)">
          ${_covFacetedRenderValueChecks("format", f)}
          <div style="flex-basis:100%;display:flex;gap:0.4rem;margin-top:0.2rem"><button class="admin-btn" type="button" style="padding:0 0.5rem;font-size:0.7rem" onclick="_covFacetedToggleAll('format', true)">all</button><button class="admin-btn" type="button" style="padding:0 0.5rem;font-size:0.7rem" onclick="_covFacetedToggleAll('format', false)">none</button></div>
        </div>
        <div id="cov-fc-values-country" style="display:${f.mode === "country" ? "flex" : "none"};flex-wrap:wrap;gap:0.25rem 0.6rem;margin-top:0.3rem;max-height:8rem;overflow:auto;padding:0.3rem 0.4rem;border:1px solid var(--border);border-radius:3px;background:var(--surface)">
          ${_covFacetedRenderValueChecks("country", f)}
          <div style="flex-basis:100%;display:flex;gap:0.4rem;margin-top:0.2rem"><button class="admin-btn" type="button" style="padding:0 0.5rem;font-size:0.7rem" onclick="_covFacetedToggleAll('country', true)">all</button><button class="admin-btn" type="button" style="padding:0 0.5rem;font-size:0.7rem" onclick="_covFacetedToggleAll('country', false)">none</button></div>
        </div>
        ${f.lastError ? `<div style="font-size:0.7rem;color:#e88;margin-top:0.3rem">${_eHtml(f.lastError)}</div>` : ""}
      </div>

    </div>
  `;
}

// Discogs-supported facet values. Formats mirror what /database/search
// accepts as the format= param; countries are common Discogs country
// strings (name-cased, not ISO codes — Discogs matches on those).
const _COV_FC_FORMAT_OPTS  = ["Shellac", "78 RPM", "7\"", "10\"", "12\"", "LP", "EP", "Single", "Album", "Compilation", "Vinyl", "Flexi-disc", "Reel-To-Reel", "Acetate", "Cassette", "CD"];
const _COV_FC_COUNTRY_OPTS = ["US", "UK", "France", "Germany", "Italy", "Japan", "Jamaica", "Brazil", "Nigeria", "Canada", "Australia", "Netherlands", "Belgium", "Sweden", "Norway", "Denmark", "Spain", "Portugal", "Mexico", "Argentina", "Cuba", "South Africa", "Ghana", "India", "Trinidad & Tobago", "Congo", "Russia"];
// Default-checked set for each mode (matches the worker's DEFAULT_* on
// server side so "leave everything unchecked" and "check the defaults"
// produce the same queue).
const _COV_FC_FORMAT_DEFAULTS  = new Set(["Shellac", "7\"", "10\"", "12\"", "LP"]);
const _COV_FC_COUNTRY_DEFAULTS = new Set(["US", "UK", "France", "Germany", "Italy", "Japan", "Jamaica", "Brazil", "Nigeria"]);

function _covFacetedRenderValueChecks(mode, state) {
  const opts     = mode === "country" ? _COV_FC_COUNTRY_OPTS     : _COV_FC_FORMAT_OPTS;
  const defaults = mode === "country" ? _COV_FC_COUNTRY_DEFAULTS : _COV_FC_FORMAT_DEFAULTS;
  // Restore prior selection from the running state's values, else defaults.
  const active = new Set(Array.isArray(state?.values) && state.values.length ? state.values : Array.from(defaults));
  return opts.map(v => {
    const id = `cov-fc-${mode}-${v.replace(/[^a-z0-9]/gi, "_")}`;
    const checked = active.has(v) ? "checked" : "";
    return `<label style="display:inline-flex;align-items:center;gap:0.25rem;font-size:0.75rem;color:var(--text);white-space:nowrap;cursor:pointer">
      <input type="checkbox" id="${id}" data-fc-mode="${mode}" data-fc-value="${_eHtml(v)}" value="${_eHtml(v)}" ${checked}> ${_eHtml(v)}
    </label>`;
  }).join("");
}

function _covFacetedSyncMode() {
  const mode = document.getElementById("cov-fc-mode")?.value || "format";
  const fmt = document.getElementById("cov-fc-values-format");
  const cty = document.getElementById("cov-fc-values-country");
  if (fmt) fmt.style.display = mode === "format" ? "flex" : "none";
  if (cty) cty.style.display = mode === "country" ? "flex" : "none";
}
window._covFacetedSyncMode = _covFacetedSyncMode;

function _covFacetedToggleAll(mode, on) {
  const scope = document.getElementById(`cov-fc-values-${mode}`);
  if (!scope) return;
  scope.querySelectorAll('input[type="checkbox"]').forEach(cb => { cb.checked = !!on; });
}
window._covFacetedToggleAll = _covFacetedToggleAll;

function _covFacetedGatherValues(mode) {
  const scope = document.getElementById(`cov-fc-values-${mode}`);
  if (!scope) return [];
  return Array.from(scope.querySelectorAll('input[type="checkbox"]:checked')).map(cb => cb.value);
}

async function _covFacetedStart(reset) {
  const mode = document.getElementById("cov-fc-mode")?.value || "format";
  const yearFrom = Number(document.getElementById("cov-fc-yfrom")?.value || 1900);
  const yearTo   = Number(document.getElementById("cov-fc-yto")?.value   || 1970);
  const values   = _covFacetedGatherValues(mode);
  // Server treats empty/undefined as "use worker defaults". Send them
  // explicitly only when the user checked something so the confirm text
  // reflects reality.
  const useDefaults = values.length === 0;
  if (!confirm(`Start year × ${mode} sweep from ${yearFrom} to ${yearTo}${useDefaults ? " (server defaults)" : ` for [${values.join(", ")}]`}?`)) return;
  try {
    const r = await apiFetch("/api/admin/faceted-sweep/start", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ mode, yearFrom, yearTo, values: useDefaults ? undefined : values, resetCursor: !!reset }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Start failed: ${j.error || r.status}`); return; }
    loadCoverageSweeps();
    if (typeof loadAdminWorkerStatus === "function") loadAdminWorkerStatus();
  } catch (err) { alert(String(err)); }
}
async function _covFacetedStop() {
  if (!confirm("Stop faceted sweep?")) return;
  const r = await apiFetch("/api/admin/faceted-sweep/stop", { method: "POST" });
  if (!r.ok) { alert(`Stop failed: ${r.status}`); return; }
  loadCoverageSweeps();
}
async function _covFacetedForceClear() {
  if (!confirm("Force-clear faceted sweep state?")) return;
  const r = await apiFetch("/api/admin/faceted-sweep/force-clear", { method: "POST" });
  if (!r.ok) { alert(`Force-clear failed: ${r.status}`); return; }
  loadCoverageSweeps();
}


async function _labelDirStopSweep() {
  if (!confirm("Stop the currently-running sweep? Cursor state is persisted so you can Restart later from where it stops.")) return;
  try {
    const r = await apiFetch("/api/admin/cache-warm-catno/stop", { method: "POST" });
    if (!r.ok) { alert(`Stop failed: ${r.status}`); return; }
    loadLabelDirectory();
    if (typeof loadCacheWarmCatno === "function") loadCacheWarmCatno();
  } catch (err) { alert(String(err)); }
}

async function _labelDirForceClearSweep() {
  if (!confirm("Force-clear the sweep lock? Only do this if Stop didn't take. The running IIFE may keep processing its current page for a bit — but a new sweep can start.")) return;
  try {
    const r = await apiFetch("/api/admin/cache-warm-catno/force-clear", { method: "POST" });
    if (!r.ok) { alert(`Force-clear failed: ${r.status}`); return; }
    loadLabelDirectory();
    if (typeof loadCacheWarmCatno === "function") loadCacheWarmCatno();
  } catch (err) { alert(String(err)); }
}

async function _labelDirSearch(labelName) {
  _labelDirSearchOpen = labelName;
  _labelDirSearchLoading = true;
  _labelDirSearchResults = { ...(_labelDirSearchResults || {}), [labelName]: [] };
  _renderLabelDir();
  try {
    const r = await apiFetch(`/api/admin/label-directory/discogs-search?q=${encodeURIComponent(labelName)}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    _labelDirSearchResults = { ...(_labelDirSearchResults || {}), [labelName]: j.results || [] };
  } catch (err) {
    _labelDirSearchResults = { ...(_labelDirSearchResults || {}), [labelName]: null };
  } finally {
    _labelDirSearchLoading = false;
    _renderLabelDir();
  }
}

function _labelDirCloseSearch() {
  _labelDirSearchOpen = null;
  _renderLabelDir();
}

async function _labelDirSaveId(labelName, labelId) {
  try {
    const r = await apiFetch("/api/admin/label-directory/set-id", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ labelName, labelId }),
    });
    if (!r.ok) { alert(`Save failed: ${r.status}`); return; }
    _labelDirSearchOpen = null;
    // Patch in memory — the row we changed already matches the server.
    // Skip the full reload so the user keeps their scroll position.
    const row = _labelDirRows.find(x => x.label_name === labelName);
    if (row) row.label_id = labelId;
    _renderLabelDir();
  } catch (err) { alert(String(err)); }
}

async function _labelDirSaveIdManual(labelName) {
  const inp = document.querySelector(`input[data-labeldir-manual="${labelName.replace(/"/g, '\\"')}"]`);
  const raw = inp ? inp.value : "";
  const id = Number(String(raw).trim());
  if (!Number.isFinite(id) || id <= 0) { alert("Enter a positive Discogs label ID"); return; }
  await _labelDirSaveId(labelName, id);
}

async function _labelDirClearId(labelName) {
  if (!confirm(`Clear Discogs ID on "${labelName}"?`)) return;
  await _labelDirSaveId(labelName, null);
}

async function _labelDirStartSweep(labelId, labelName) {
  if (!confirm(`Start a label sweep for "${labelName}"?\n\nThis fetches+caches every master Discogs has tagged with that label, then every orphan release (no parent master). Existing cached releases aren't touched. Progress is visible in the Catalog-number crawls panel below.`)) return;
  try {
    const r = await apiFetch("/api/admin/label-directory/start-sweep", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ labelId, labelName }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Start failed: ${j.error || r.status}`); return; }
    loadLabelDirectory();
    // Also kick the catno panel so the user can scroll down and watch.
    if (typeof loadCacheWarmCatno === "function") loadCacheWarmCatno();
  } catch (err) { alert(String(err)); }
}

function _labelDirOpenAlias(labelName, labelId) {
  _labelDirAliasOpen  = { label_name: labelName, label_id: Number(labelId) };
  _labelDirAliasQuery = labelName;
  _renderLabelDir();
}
function _labelDirCloseAlias() {
  _labelDirAliasOpen  = null;
  _labelDirAliasQuery = "";
  _renderLabelDir();
}
function _labelDirAliasFilter(ev) {
  _labelDirAliasQuery = String(ev.target.value || "");
  _renderLabelDir();
  requestAnimationFrame(() => {
    const inp = document.querySelector('div[role="dialog"] input[type="search"]');
    if (inp) { inp.focus(); inp.setSelectionRange(_labelDirAliasQuery.length, _labelDirAliasQuery.length); }
  });
}
async function _labelDirConfirmAlias(aliasId, canonicalId) {
  const reason = prompt(`Reason (optional): variant name / reissue / ownership change / etc.`, "");
  if (reason === null) return;   // cancelled
  try {
    const r = await apiFetch("/api/admin/label-directory/set-alias", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ aliasLabelId: aliasId, canonicalLabelId: canonicalId, reason: reason.trim() || null }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Alias failed: ${j.error || r.status}`); return; }
    _labelDirAliasOpen = null;
    loadLabelDirectory();
  } catch (err) { alert(String(err)); }
}
async function _labelDirRemoveAlias(aliasId) {
  if (!confirm(`Un-alias label ID ${aliasId}? It becomes its own directory row again.`)) return;
  try {
    const r = await apiFetch("/api/admin/label-directory/remove-alias", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ aliasLabelId: aliasId }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Un-alias failed: ${j.error || r.status}`); return; }
    loadLabelDirectory();
  } catch (err) { alert(String(err)); }
}

function _labelDirOpenMerge(labelName) {
  _labelDirMergeOpen  = labelName;
  // Pre-seed the filter with the source name so similar-named candidates
  // (typo variants, "Foo" vs "Foo Records") surface immediately.
  _labelDirMergeQuery = labelName;
  _renderLabelDir();
}
function _labelDirCloseMerge() {
  _labelDirMergeOpen  = null;
  _labelDirMergeQuery = "";
  _renderLabelDir();
}
function _labelDirMergeFilter(ev) {
  _labelDirMergeQuery = String(ev.target.value || "");
  _renderLabelDir();
  // Refocus the input — innerHTML rewrite blew it away.
  requestAnimationFrame(() => {
    const inp = document.querySelector('div[role="dialog"] input[type="search"]');
    if (inp) { inp.focus(); inp.setSelectionRange(_labelDirMergeQuery.length, _labelDirMergeQuery.length); }
  });
}
async function _labelDirConfirmMerge(fromLabel, toLabel) {
  if (!confirm(`Merge "${fromLabel}" → "${toLabel}"?\n\nEvery external_discography row tagged with the first label will be renamed to the second. Catno collisions: source wins, the colliding target row is dropped.`)) return;
  try {
    const r = await apiFetch("/api/admin/label-directory/merge", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ fromLabel, toLabel }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Merge failed: ${j.error || r.status}`); return; }
    alert(`Merged. Renamed ${j.renamed} rows · dropped ${j.mergedDuplicates} duplicates · final ID ${j.finalLabelId ?? "(none)"}.`);
    _labelDirMergeOpen = null;
    loadLabelDirectory();   // scroll position is preserved by _renderLabelDir
  } catch (err) { alert(String(err)); }
}

async function _labelDirCleanNames() {
  if (!confirm("Strip HTML and collapse whitespace on every dirty label_name? Merges rows that collide after cleaning.")) return;
  try {
    const r = await apiFetch("/api/admin/external-discography/clean-label-names", { method: "POST" });
    if (!r.ok) { alert(`Clean failed: ${r.status}`); return; }
    const j = await r.json();
    const summary = (j.renames || []).slice(0, 8).map(x => `  • "${x.from}" → "${x.to}" (${x.rows} rows)`).join("\n");
    alert(`Renamed ${j.renamed} rows · merged ${j.mergedDuplicates} duplicates.\n\n${summary || "(no changes)"}`);
    loadLabelDirectory();
  } catch (err) { alert(String(err)); }
}

// ── Split-cache projection backfill ────────────────────────────
let _cacheProjectionStatus = null;
// Cache write-rate card: release_cache throughput by cached_at over a
// few rolling windows + a 24h hourly sparkline. Polls every 30s while
// the Cache panel is open so the numbers move during an active sweep.
// Hardened so the card can never sit on a dead "Loading…": the element
// lookup retries (in case the fragment isn't in the DOM yet), and the
// fetch has a hard client-side timeout so a slow/hung request surfaces
// a visible error + retry link instead of an indefinite spinner.
async function loadCacheRate(_elRetry = 0) {
  const el = document.getElementById("cache-rate-content");
  if (!el) {
    // Fragment may not be injected yet on the very first tab open —
    // retry a few times before giving up silently.
    if (_elRetry < 10) { setTimeout(() => loadCacheRate(_elRetry + 1), 300); }
    return;
  }
  const retryLink = `<a href="#" onclick="event.preventDefault();loadCacheRate();return false" style="color:var(--accent);margin-left:0.5rem">↻ retry</a>`;
  try {
    // Hard 12s timeout so a saturated pool / slow scan can't hang the
    // card. AbortController aborts the fetch; the catch renders the
    // error + a retry link.
    const ctrl = new AbortController();
    const to = setTimeout(() => ctrl.abort(), 12000);
    let r;
    try {
      r = await apiFetch("/api/admin/cache-rate", { signal: ctrl.signal });
    } finally { clearTimeout(to); }
    if (!r.ok) { el.innerHTML = `<span style="color:#e88">Failed: HTTP ${r.status}</span>${retryLink}`; return; }
    const s = await r.json();
    const n = (x) => (Number(x) || 0).toLocaleString();
    const series = Array.isArray(s.hourly) ? s.hourly : [];
    const max = series.reduce((m, h) => Math.max(m, Number(h.n) || 0), 0);
    const bars = series.map(h => {
      const v = Number(h.n) || 0;
      const pct = max > 0 ? Math.round((v / max) * 100) : 0;
      const hh = String(h.hour).slice(11, 16);
      return `<span title="${hh}: ${n(v)} writes" style="display:inline-block;width:5px;height:26px;background:var(--surface);border-radius:1px;vertical-align:bottom;position:relative">`
           + `<span style="position:absolute;bottom:0;left:0;right:0;height:${pct}%;min-height:${v > 0 ? 2 : 0}px;background:var(--accent);border-radius:1px"></span></span>`;
    }).join("");
    el.innerHTML = `
      <div style="display:flex;gap:1.1rem;flex-wrap:wrap;align-items:baseline;margin-bottom:0.5rem">
        <span title="release_cache writes in the last hour">Last hour: <strong style="color:var(--text)">${n(s.window?.["1h"])}</strong></span>
        <span title="release_cache writes in the last 24 hours (masters + releases)">Last 24h: <strong style="color:var(--text)">${n(s.window?.["24h"])}</strong> <span style="font-size:0.72rem">(${n(s.window?.master24)} master · ${n(s.window?.release24)} release)</span></span>
        <span title="Average writes per hour across the last 24h">Rate: <strong style="color:var(--text)">${n(s.ratePerHour24h)}</strong>/hr</span>
        <span title="release_cache writes in the last 7 days">Last 7d: <strong style="color:var(--text)">${n(s.window?.["7d"])}</strong></span>
      </div>
      <div style="display:flex;gap:1px;align-items:flex-end;height:26px" title="Writes per hour, last 24 hours">${bars || '<span style="font-size:0.72rem">no writes in the last 24h</span>'}</div>
      <div style="font-size:0.72rem;margin-top:0.4rem">Total cached: <strong style="color:var(--text)">${n(s.total?.all)}</strong> (${n(s.total?.master)} masters · ${n(s.total?.release)} releases)</div>
    `;
    clearTimeout(window._cacheRatePollTimer);
    if (_adminPanelVisible() && document.getElementById("panel-cache-warm")?.style.display !== "none") {
      window._cacheRatePollTimer = setTimeout(loadCacheRate, 30000);
    }
  } catch (err) {
    const msg = err?.name === "AbortError"
      ? "Timed out (the cache query took over 12s — likely the DB pool is busy warming). "
      : `${_eHtml(String(err))} `;
    el.innerHTML = `<span style="color:#e88">${msg}</span>${retryLink}`;
  }
}
window.loadCacheRate = loadCacheRate;

async function loadCacheProjection() {
  const el = document.getElementById("cache-projection-content");
  if (!el) return;
  try {
    const r = await apiFetch("/api/admin/cache-projection/status");
    if (!r.ok) { el.innerHTML = `<span style="color:#e88">Failed: HTTP ${r.status}</span>`; return; }
    _cacheProjectionStatus = await r.json();
    _renderCacheProjection();
    clearTimeout(window._cacheProjectionPollTimer);
    if (_cacheProjectionStatus?.running && _adminPanelVisible() && document.getElementById("panel-cache-warm")?.style.display !== "none") {
      window._cacheProjectionPollTimer = setTimeout(loadCacheProjection, 5000);
    }
  } catch (err) {
    el.innerHTML = `<span style="color:#e88">${_eHtml(String(err))}</span>`;
  }
}
// Populate the V1-vs-V2 totals badge next to the Explore cache header
// from the projection stats already fetched by loadCacheProjection().
// V1 = projectable release_cache rows (release+master); V2 = the
// masters_plus + pressings tables — apples-to-apples so a gap means
// the projection backfill hasn't fully drained.
function _renderCaTotalsBadge() {
  const badge = document.getElementById("ca-totals-badge");
  if (!badge) return;
  const st = _cacheProjectionStatus && _cacheProjectionStatus.stats;
  if (!st) { badge.textContent = ""; return; }
  const v1 = Number(st.releaseCacheProjectable ?? 0);
  const v2 = Number(st.mastersPlusRows ?? 0) + Number(st.pressingsRows ?? 0);
  const gap = v1 - v2;
  const gapTxt = gap === 0 ? "" : ` · Δ${gap > 0 ? "+" : ""}${gap.toLocaleString()}`;
  badge.innerHTML =
    `<span title="release_cache rows, release+master only (V1)">V1 ${v1.toLocaleString()}</span>` +
    ` · <span title="discogs_cache_masters_plus + discogs_cache_pressings (V2)">V2 ${v2.toLocaleString()}</span>` +
    (gap !== 0 ? `<span title="V1 minus V2 — nonzero means the split projection is behind" style="color:#e8a">${gapTxt}</span>` : "");
}
function _renderCacheProjection() {
  const el = document.getElementById("cache-projection-content");
  _renderCaTotalsBadge();
  if (!el) return;
  const s = _cacheProjectionStatus || {};
  const stats = s.stats || {};
  const running = !!s.running;
  const total = Number.isFinite(s.total) ? s.total : (stats.releaseCacheProjectable ?? null);
  const pct = total > 0 ? Math.round((s.processed / total) * 100) : 0;
  el.innerHTML = `
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:0.4rem;margin-bottom:0.7rem">
      ${_cpStat("release_cache (projectable)", stats.releaseCacheProjectable)}
      ${_cpStat("masters + orphans", stats.mastersPlusRows)}
      ${_cpStat("pressings", stats.pressingsRows)}
      ${_cpStat("release_labels", stats.labelsRows)}
      ${_cpStat("release_artists", stats.artistsRows)}
      ${_cpStat("release_tags", stats.tagsRows)}
    </div>
    ${running ? `
      <div style="display:flex;gap:0.5rem;align-items:center;padding:0.4rem 0.6rem;margin-bottom:0.5rem;background:rgba(60,120,200,0.15);border:1px solid #48a;border-radius:4px;flex-wrap:wrap">
        <span style="font-size:0.82rem"><strong>Projecting:</strong> ${s.processed}${total ? ` / ${total}` : ""} (${pct}%)${s.errors ? ` · ⚠${s.errors}` : ""}</span>
        <span style="font-size:0.72rem;color:var(--muted)">cursor id ${s.cursorId}</span>
        <button class="admin-btn" type="button" onclick="_cacheProjectionStop()" style="background:#a33;color:#fff">■ Stop</button>
        <button class="admin-btn" type="button" onclick="_cacheProjectionForceClear()" title="Only if Stop doesn't take. Wipes worker state (doesn't touch already-written projected rows).">⚠ Force clear</button>
      </div>
    ` : `
      <div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap;margin-bottom:0.5rem">
        <button class="admin-btn" type="button" onclick="_cacheProjectionStart(false)">⏵ Start / resume backfill</button>
        <button class="admin-btn" type="button" onclick="_cacheProjectionStart(true)" title="Reset cursor to 0 and rescan every row from the top. Idempotent — safe, just slower.">↻ Restart from top</button>
        ${s.processed ? `<span style="font-size:0.78rem;color:var(--muted)">last: ${s.processed} processed, cursor id ${s.cursorId}${s.errors ? ` · ⚠${s.errors}` : ""}</span>` : ""}
      </div>
    `}
    ${s.lastError ? `<div style="font-size:0.72rem;color:#e88;margin-top:0.3rem"><strong>Last error:</strong> ${_eHtml(s.lastError)}</div>` : ""}
    <div style="font-size:0.72rem;color:var(--muted);margin-top:0.4rem">
      Dual-write is live: every new cacheRelease() call already writes to the split schema. This backfill just projects the existing rows.
    </div>
    <hr style="margin:0.7rem 0;border-color:var(--border)">
    <div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap"
         title="When on, admin panels + samplers read from the projected schema (release_labels + release_artists + release_tags + discogs_cache_masters_plus / discogs_cache_pressings) instead of unrolling JSONB from release_cache. Flip ON only after the backfill has drained — otherwise counts are missing. Currently covers: label directory, cache analytics, mini-player enrichment fallback, Rare feed sampler. Note: Rare feed uses community_want / community_have / num_for_sale columns added later — click ↻ Restart from top once after every schema-adding deploy to populate them.">
      <label style="display:flex;align-items:center;gap:0.4rem;cursor:pointer;font-size:0.82rem">
        <input id="cp-split-readers-toggle" type="checkbox" ${s.splitReadersEnabled ? "checked" : ""} onchange="_cacheProjectionToggleReaders(event)">
        Use split-cache readers (label directory + analytics + enrichment + rare feed)
      </label>
      <span style="font-size:0.72rem;color:var(--muted)">${s.splitReadersEnabled ? "reading from projected schema" : "reading from release_cache (old)"}</span>
    </div>
  `;
}
async function _cacheProjectionToggleReaders(ev) {
  const enabled = !!ev.target.checked;
  if (enabled && !confirm("Turn ON split-cache readers?\n\nThe label directory will start reading from the projected schema. If the backfill hasn't finished, cache counts will look low. Toggle back OFF anytime.")) {
    ev.target.checked = false; return;
  }
  try {
    const r = await apiFetch("/api/admin/cache-projection/set-reader-flag", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ enabled }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Toggle failed: ${j.error || r.status}`); ev.target.checked = !enabled; return; }
    loadCacheProjection();
    if (typeof loadLabelDirectory === "function") loadLabelDirectory();
  } catch (err) { alert(String(err)); ev.target.checked = !enabled; }
}
function _cpStat(label, n) {
  const val = Number.isFinite(n) ? n.toLocaleString() : "—";
  return `<div style="padding:0.3rem 0.5rem;background:rgba(255,255,255,0.03);border:1px solid var(--border);border-radius:3px">
    <div style="font-size:0.68rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.03em">${_eHtml(label)}</div>
    <div style="font-size:0.95rem;font-weight:600">${val}</div>
  </div>`;
}
async function _cacheProjectionStart(reset) {
  const msg = reset
    ? "Restart the backfill from cursor 0? Every existing row will be reprojected (idempotent, but slower)."
    : "Start (or resume) the projection backfill? Walks release_cache in 200-row batches, writing into the split schema.";
  if (!confirm(msg)) return;
  try {
    const r = await apiFetch("/api/admin/cache-projection/start", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ resetCursor: !!reset }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Start failed: ${j.error || r.status}`); return; }
    loadCacheProjection();
    if (typeof loadAdminWorkerStatus === "function") loadAdminWorkerStatus();
  } catch (err) { alert(String(err)); }
}
async function _cacheProjectionStop() {
  if (!confirm("Stop the projection backfill? Cursor is persisted so you can resume later.")) return;
  try {
    const r = await apiFetch("/api/admin/cache-projection/stop", { method: "POST" });
    if (!r.ok) { alert(`Stop failed: ${r.status}`); return; }
    loadCacheProjection();
  } catch (err) { alert(String(err)); }
}
async function _cacheProjectionForceClear() {
  if (!confirm("Force-clear projection worker state?\n\nOnly use if Stop didn't take. Does NOT touch already-written projected rows — they stay in the split cache. A subsequent Start / Restart will re-scan from the top or use a fresh cursor.")) return;
  try {
    const r = await apiFetch("/api/admin/cache-projection/force-clear", { method: "POST" });
    if (!r.ok) { alert(`Force-clear failed: ${r.status}`); return; }
    loadCacheProjection();
  } catch (err) { alert(String(err)); }
}

// ── Cache analytics (faceted drilldown) ─────────────────────────
let _cacheAnalyticsFilters = { label:"", artist:"", genre:"", style:"", country:"", yearFrom:"", yearTo:"", type:"" };
let _cacheAnalyticsResult = null;
let _cacheAnalyticsLoading = false;
// Default to V1 (release_cache) — it's the complete dual-write source
// of truth, so broad genre/style searches always resolve even when the
// split projection (release_tags etc.) is behind. Selector still offers
// Auto / V2 for comparison against what the live site reads.
let _cacheAnalyticsReader = "v1";  // "" = auto (follows split-cache flag), "v1", "v2"

function loadCacheAnalytics() {
  // Just render the form on first tab open — running the query is
  // explicit (button click) since it walks release_cache end-to-end
  // and can take a few seconds on wide filters.
  _renderCacheAnalytics();
}
function _renderCacheAnalytics() {
  const el = document.getElementById("cache-analytics-content");
  _renderCaTotalsBadge();
  if (!el) return;
  const f = _cacheAnalyticsFilters;
  const facetCol = (title, rows, keyName) => {
    const list = (rows || []).map(r => {
      const name = keyName === "decade" ? `${r.decade}s` : (r.name || "(none)");
      const click = keyName === "decade" ? "" : `onclick="_caFacetPin('${keyName}','${_eHtml(String(r.name || "")).replace(/'/g, "\\'")}')"`;
      const style = keyName === "decade"
        ? ""
        : "cursor:pointer;text-decoration:underline;text-decoration-style:dotted";
      return `<li ${click} style="${style}"><span>${_eHtml(name)}</span> <span style="color:var(--muted)">${r.count.toLocaleString()}</span></li>`;
    }).join("") || `<li style="color:var(--muted);list-style:none">—</li>`;
    return `<div>
      <div style="font-weight:600;font-size:0.82rem;margin-bottom:0.3rem">${title}</div>
      <ul style="margin:0;padding-left:1.1rem;font-size:0.78rem;line-height:1.4;max-height:220px;overflow-y:auto">${list}</ul>
    </div>`;
  };
  const resultsHtml = _cacheAnalyticsLoading
    ? `<div style="color:var(--muted);margin-top:0.6rem">Running query…</div>`
    : !_cacheAnalyticsResult
      ? `<div style="color:var(--muted);margin-top:0.6rem">Set filters and hit Analyze.</div>`
      : (() => {
          const r = _cacheAnalyticsResult;
          const sampleRows = (r.sample || []).map(s => `
            <tr style="border-bottom:1px dashed rgba(255,255,255,0.05)">
              <td style="padding:0.2rem 0.4rem">${_eHtml(s.type)}</td>
              <td style="padding:0.2rem 0.4rem">${_eHtml(s.year != null ? String(s.year) : '—')}</td>
              <td style="padding:0.2rem 0.4rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:280px" title="${_eHtml(s.title)}">${_eHtml(s.title || '')}</td>
              <td style="padding:0.2rem 0.4rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:180px" title="${_eHtml(s.artist)}">${_eHtml(s.artist || '')}</td>
              <td style="padding:0.2rem 0.4rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:180px" title="${_eHtml(s.label)}">${_eHtml(s.label || '')}</td>
              <td style="padding:0.2rem 0.4rem"><a href="https://www.discogs.com/${s.type === 'master' ? 'master' : 'release'}/${s.id}" target="_blank" rel="noopener" style="color:var(--accent)">↗</a></td>
            </tr>`).join("");
          return `
            <div style="font-size:0.9rem;margin:0.5rem 0"><strong>${r.totalCount.toLocaleString()}</strong> matching cache rows.</div>
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:0.8rem;margin-bottom:0.8rem">
              ${facetCol("Genres",    r.facets.genres,    "genre")}
              ${facetCol("Styles",    r.facets.styles,    "style")}
              ${facetCol("Labels",    r.facets.labels,    "label")}
              ${facetCol("Artists",   r.facets.artists,   "artist")}
              ${facetCol("Countries", r.facets.countries, "country")}
              ${facetCol("Decades",   r.facets.decades,   "decade")}
            </div>
            <div style="font-weight:600;font-size:0.82rem;margin:0.5rem 0 0.3rem">Sample (earliest 20)</div>
            <table style="width:100%;font-size:0.78rem;border-collapse:collapse">
              <thead><tr style="text-align:left;color:var(--muted);border-bottom:1px solid var(--border)">
                <th style="padding:0.25rem 0.4rem">Type</th>
                <th style="padding:0.25rem 0.4rem">Year</th>
                <th style="padding:0.25rem 0.4rem">Title</th>
                <th style="padding:0.25rem 0.4rem">Artist</th>
                <th style="padding:0.25rem 0.4rem">Label</th>
                <th></th>
              </tr></thead>
              <tbody>${sampleRows || `<tr><td colspan="6" style="padding:0.4rem;color:var(--muted)">No matches.</td></tr>`}</tbody>
            </table>
          `;
        })();
  const inp = (id, label, placeholder = "") => `
    <label style="display:flex;flex-direction:column;gap:0.15rem;font-size:0.75rem;color:var(--muted)">
      <span>${label}</span>
      <input id="${id}" type="text" value="${_eHtml(f[id.replace(/^ca-/, '').replace(/-/g, '_')] || "")}"
             oninput="_caFilterInput(event)" placeholder="${_eHtml(placeholder)}"
             style="padding:0.25rem 0.4rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
    </label>`;
  el.innerHTML = `
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:0.5rem;margin-bottom:0.5rem">
      ${inp("ca-label",  "Label",   "Excello")}
      ${inp("ca-artist", "Artist",  "Slim Harpo")}
      ${inp("ca-genre",  "Genre",   "Blues")}
      ${inp("ca-style",  "Style",   "Delta Blues")}
      ${inp("ca-country","Country", "US")}
      <label style="display:flex;flex-direction:column;gap:0.15rem;font-size:0.75rem;color:var(--muted)">
        <span>Year range</span>
        <div style="display:flex;gap:0.3rem">
          <input id="ca-year-from" type="number" value="${_eHtml(String(f.yearFrom || ""))}" oninput="_caFilterInput(event)" placeholder="from"
                 style="width:50%;padding:0.25rem 0.4rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          <input id="ca-year-to"   type="number" value="${_eHtml(String(f.yearTo   || ""))}" oninput="_caFilterInput(event)" placeholder="to"
                 style="width:50%;padding:0.25rem 0.4rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
        </div>
      </label>
      <label style="display:flex;flex-direction:column;gap:0.15rem;font-size:0.75rem;color:var(--muted)">
        <span>Type</span>
        <select id="ca-type" onchange="_caFilterInput(event)"
                style="padding:0.25rem 0.4rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          <option value=""${f.type === "" ? " selected" : ""}>Both</option>
          <option value="release"${f.type === "release" ? " selected" : ""}>Release</option>
          <option value="master"${f.type === "master" ? " selected" : ""}>Master</option>
          <option value="masters_plus"${f.type === "masters_plus" ? " selected" : ""}>Masters+ (masters + orphans)</option>
        </select>
      </label>
    </div>
    <div style="display:flex;gap:0.4rem;margin-bottom:0.4rem;align-items:center;flex-wrap:wrap">
      <button class="admin-btn" type="button" onclick="_caRun()" ${_cacheAnalyticsLoading ? "disabled" : ""}>▶ Analyze</button>
      <button class="admin-btn" type="button" onclick="_caReset()">Reset</button>
      <label style="font-size:0.75rem;color:var(--muted);display:inline-flex;gap:0.3rem;align-items:center"
             title="Which cache to read. Auto follows the split-cache readers flag. If Auto returns nothing, try V1 (release_cache) — empty V2 results mean the split projection hasn't populated those tables.">Source
        <select id="ca-reader" onchange="_caReaderChange(event)"
                style="padding:0.25rem 0.4rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          <option value=""${_cacheAnalyticsReader === "" ? " selected" : ""}>Auto</option>
          <option value="v1"${_cacheAnalyticsReader === "v1" ? " selected" : ""}>release_cache (V1)</option>
          <option value="v2"${_cacheAnalyticsReader === "v2" ? " selected" : ""}>Split cache (V2)</option>
        </select>
      </label>
    </div>
    ${resultsHtml}
  `;
}
function _caFilterInput(ev) {
  const id = ev.target.id;
  const map = { "ca-label":"label", "ca-artist":"artist", "ca-genre":"genre", "ca-style":"style",
                "ca-country":"country", "ca-year-from":"yearFrom", "ca-year-to":"yearTo", "ca-type":"type" };
  const key = map[id];
  if (!key) return;
  _cacheAnalyticsFilters[key] = ev.target.value.trim();
}
function _caFacetPin(dim, name) {
  const map = { genre:"genre", style:"style", label:"label", artist:"artist", country:"country" };
  const key = map[dim];
  if (!key) return;
  _cacheAnalyticsFilters[key] = name;
  _renderCacheAnalytics();
  _caRun();
}
function _caReset() {
  _cacheAnalyticsFilters = { label:"", artist:"", genre:"", style:"", country:"", yearFrom:"", yearTo:"", type:"" };
  _cacheAnalyticsResult = null;
  _renderCacheAnalytics();
}
function _caReaderChange(ev) {
  const v = ev.target.value;
  _cacheAnalyticsReader = (v === "v1" || v === "v2") ? v : "";
}
window._caReaderChange = _caReaderChange;
async function _caRun() {
  _cacheAnalyticsLoading = true;
  _renderCacheAnalytics();
  try {
    const body = { ...(_cacheAnalyticsFilters) };
    // Blank strings should not be sent as filter values.
    for (const k of Object.keys(body)) if (body[k] === "") delete body[k];
    const qs = _cacheAnalyticsReader ? `?reader=${_cacheAnalyticsReader}` : "";
    const r = await apiFetch(`/api/admin/cache-analytics${qs}`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      throw new Error(j.error ? `HTTP ${r.status}: ${j.error}` : `HTTP ${r.status}`);
    }
    _cacheAnalyticsResult = await r.json();
  } catch (err) {
    alert(`Analyze failed: ${err}`);
    _cacheAnalyticsResult = null;
  } finally {
    _cacheAnalyticsLoading = false;
    _renderCacheAnalytics();
  }
}

async function loadCacheWarm(opts) {
  const el = document.getElementById("cw-content");
  if (!el) return;
  try {
    let resp;
    if (opts?.fromCache && _cwLastResp) {
      resp = _cwLastResp;
    } else {
      const r = await apiFetch("/api/admin/cache-warm-runs/status");
      if (!r.ok) { el.innerHTML = `<span style="color:#e88">Failed: HTTP ${r.status}</span>`; return; }
      resp = await r.json();
      _cwLastResp = resp;
    }
    // let (not const) on rows because the sort-headers helper
    // re-assigns it to the sorted copy before rendering tbody.
    let { rows = [], release_cache_total = 0, active = null, running = false } = resp;
    const esc = escHtml;   // canonical escaper (shared.js) — escapes & < > " '
    const fmt = n => Number(n || 0).toLocaleString();
    // Preserve form values across re-renders (poll, refresh).
    const prevGenre = document.getElementById("cw-form-genre")?.value;
    const prevStyle = document.getElementById("cw-form-style")?.value || "";
    const prevFrom  = document.getElementById("cw-form-from")?.value  || "";
    const prevTo    = document.getElementById("cw-form-to")?.value    || "";
    const selectedGenre = active?.genreKey ?? prevGenre ?? _CW_GENRES[0].key;
    const selectedStyle = active?.styleKey ?? prevStyle ?? "";
    // Active-run rate from the matching row's recent_cached ring.
    // Also fetch the row's cursor + counters so the control box can
    // surface in-flight progress without making the admin glance
    // down at the stats grid.
    const activeRow = (running && active)
      ? rows.find(x => x.genre_key === active.genreKey && (x.style_key || "") === (active.styleKey || ""))
      : null;
    const rc = Array.isArray(activeRow?.recent_cached) ? activeRow.recent_cached : [];
    const recentErrors = Array.isArray(activeRow?.recent_errors) ? activeRow.recent_errors : [];
    let ratePill = "";
    if (running && rc.length >= 2) {
      const dt = new Date(rc[0]?.at || 0).getTime() - new Date(rc[rc.length - 1]?.at || 0).getTime();
      if (dt > 0) ratePill = `<span style="padding:0.1rem 0.4rem;border-radius:999px;background:rgba(255,255,255,0.04);color:var(--muted);border:1px solid var(--border);font-size:0.7rem">${((rc.length - 1) / (dt / 60000)).toFixed(1)} / min</span>`;
    }
    const runningPill = running
      ? `<span style="padding:0.1rem 0.4rem;border-radius:999px;background:rgba(125,225,150,0.15);color:#7ed196;border:1px solid rgba(125,225,150,0.4);font-size:0.7rem;font-weight:600">● RUNNING</span>`
      : `<span style="padding:0.1rem 0.4rem;border-radius:999px;background:rgba(255,255,255,0.04);color:var(--muted);border:1px solid var(--border);font-size:0.7rem">○ Idle</span>`;
    // Beefier active block: progress tiles + recent + errors. Shown
    // only when running so an idle panel stays compact.
    const activeBlock = (running && active && activeRow) ? `
      <div style="border:1px solid rgba(125,225,150,0.3);border-radius:5px;padding:0.5rem 0.7rem;margin-bottom:0.6rem;background:rgba(125,225,150,0.05)">
        <div style="font-size:0.82rem;color:var(--accent);margin-bottom:0.4rem">
          Running: <strong>${esc(active.genreKey)}</strong>${active.styleKey ? ` / <strong>${esc(active.styleKey)}</strong>` : ""} · ${active.fromYear}–${active.toYear} · started ${new Date(active.startedAt).toLocaleString()}
        </div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:0.5rem;margin-bottom:0.4rem">
          <div><div style="font-size:0.7rem;color:var(--muted);text-transform:uppercase">Cursor</div><div style="font-size:0.95rem;font-weight:600;color:var(--text);font-variant-numeric:tabular-nums">${activeRow.current_year ?? "—"} · p${activeRow.current_page ?? 1}</div></div>
          <div><div style="font-size:0.7rem;color:var(--muted);text-transform:uppercase">Cached (run)</div><div style="font-size:0.95rem;font-weight:600;color:#7ed196;font-variant-numeric:tabular-nums">${fmt(activeRow.total_cached)}</div></div>
          <div><div style="font-size:0.7rem;color:var(--muted);text-transform:uppercase">Skipped</div><div style="font-size:0.95rem;color:var(--muted);font-variant-numeric:tabular-nums">${fmt(activeRow.total_skipped)}</div></div>
          <div><div style="font-size:0.7rem;color:var(--muted);text-transform:uppercase">Searched</div><div style="font-size:0.95rem;color:var(--muted);font-variant-numeric:tabular-nums">${fmt(activeRow.total_searched)}</div></div>
          <div><div style="font-size:0.7rem;color:var(--muted);text-transform:uppercase">Errors</div><div style="font-size:0.95rem;color:${activeRow.total_errors ? "#e88" : "var(--muted)"};font-variant-numeric:tabular-nums">${fmt(activeRow.total_errors)}</div></div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.5rem">
          <div>
            <div style="font-size:0.7rem;color:var(--muted);text-transform:uppercase;margin-bottom:0.2rem">Recent cached</div>
            ${rc.length
              ? `<ul style="margin:0;padding-left:1rem;font-size:0.74rem;color:var(--text)">${rc.slice(0, 5).map(c => `<li style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(c.title || "")}">${esc(c.title || "")} <span style="color:var(--muted)">#${esc(c.id)}</span></li>`).join("")}</ul>`
              : `<div style="font-style:italic;color:var(--muted);font-size:0.74rem">(none yet)</div>`}
          </div>
          <div>
            ${recentErrors.length
              ? `<details><summary style="cursor:pointer;color:#e88;font-size:0.74rem;font-weight:600">${recentErrors.length} recent error${recentErrors.length === 1 ? "" : "s"}</summary><ul style="margin:0.2rem 0 0;padding-left:1rem;font-size:0.72rem;color:#e88">${recentErrors.map(e => `<li>${esc(e.msg || "")}</li>`).join("")}</ul></details>`
              : ""}
          </div>
        </div>
      </div>`
      : "";

    // Genre <select> with options. Pre-select the active or
    // previously chosen genre. Style <input> bound to a per-genre
    // datalist (set in HTML).
    const genreOptions = _CW_GENRES.map(g =>
      `<option value="${esc(g.key)}"${g.key === selectedGenre ? " selected" : ""}>${esc(g.key)}</option>`,
    ).join("");
    const currentList = (_CW_GENRES.find(g => g.key === selectedGenre) || _CW_GENRES[0]).list;

    el.innerHTML = `
      <div style="border:1px solid var(--border);border-radius:6px;padding:0.7rem 0.8rem;margin-bottom:1rem;background:rgba(255,255,255,0.02)">
        <div style="display:flex;gap:0.8rem;flex-wrap:wrap;align-items:center;margin-bottom:0.5rem">
          <strong style="font-size:0.95rem">Start a run</strong>
          ${runningPill}${ratePill}
          <span style="font-size:0.78rem;color:var(--muted);margin-left:auto">release_cache total: <strong style="color:var(--text)">${fmt(release_cache_total)}</strong></span>
        </div>
        ${activeBlock}
        <div class="cw-form-grid" style="display:grid;grid-template-columns:1fr 1fr 100px 100px auto;gap:0.5rem;align-items:end">
          <label style="font-size:0.74rem;color:var(--muted)">Genre
            <select id="cw-form-genre" onchange="_cwSyncStyleList()" ${running ? "disabled" : ""} style="width:100%;padding:0.4rem 0.5rem;font-size:0.86rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px">${genreOptions}</select>
          </label>
          <label style="font-size:0.74rem;color:var(--muted)">Style <em style="color:#888">(optional)</em>
            <input id="cw-form-style" type="text" list="cw-styles-${currentList}" value="${esc(selectedStyle)}" ${running ? "disabled" : ""} placeholder="(all of genre)" style="width:100%;padding:0.4rem 0.5rem;font-size:0.86rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px">
          </label>
          <label style="font-size:0.74rem;color:var(--muted)">From year
            <input id="cw-form-from" type="number" placeholder="1900" value="${esc(prevFrom)}" ${running ? "disabled" : ""} style="width:100%;padding:0.4rem 0.5rem;font-size:0.86rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px">
          </label>
          <label style="font-size:0.74rem;color:var(--muted)">To year
            <input id="cw-form-to" type="number" placeholder="${new Date().getFullYear()}" value="${esc(prevTo)}" ${running ? "disabled" : ""} style="width:100%;padding:0.4rem 0.5rem;font-size:0.86rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px">
          </label>
          <div style="display:flex;gap:0.4rem;flex-wrap:wrap">
            ${running
              ? `<button class="admin-btn" onclick="cacheWarmStop()" title="Signal the worker to wind down at the next safe boundary.">■ Stop</button>`
              : `<button class="admin-btn" onclick="cacheWarmStartFromForm(false)" title="Resume from the persisted cursor for this combo, or start fresh if none. From-year only applies on first run for the combo.">▶ Start</button>
                 <button class="admin-btn" onclick="cacheWarmStartFromForm(true)" title="Reset the cursor for this combo to From-year before starting.">↻ Start over</button>
                 <button class="admin-btn" onclick="cacheWarmStartNoYearForForm()" title="Sweep releases in this genre/style that have NO year on Discogs — year-filtered runs (e.g. 1900-1970) skip these. Cursor for the no-year run is independent of the dated cursor.">📅 No-year sweep</button>`}
            <button class="admin-btn" onclick="cacheWarmForceClear()" title="Force-clear the in-memory 'running' lock when the worker is stuck or crashed silently. Doesn't affect cached data. Use if Start refuses to fire.">⚠ Force clear lock</button>
          </div>
        </div>
      </div>

      <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.4rem;flex-wrap:wrap">
        <button class="admin-btn" type="button" onclick="_cwToggleGenresOnly()" style="margin-left:auto" title="Hide rows that have a style set so only top-level genre rows remain.">${_cwGenresOnly ? "Show all (genres + styles)" : "Hide styles (genres only)"}</button>
      </div>
      ${rows.length
        ? (() => {
          // Genres-only filter: drop rows with a style_key so the grid
          // is one row per top-level genre.
          if (_cwGenresOnly) rows = rows.filter(r => !r.style_key);
          // Apply current sort (column + direction) before rendering
          // tbody. _cwSort defaults to in_cache desc; clicking a
          // header toggles direction or sets a new column.
          rows = _cwSortRows(rows);
          const th = (key, label, align = "left") => {
            const isActive = _cwSort.col === key;
            const arrow = isActive ? (_cwSort.dir === "asc" ? " ↑" : " ↓") : "";
            return `<th style="text-align:${align};cursor:pointer;user-select:none" onclick="_cwSortBy('${key}')" title="Sort by ${label}">${label}${arrow}</th>`;
          };
          return `<div class="cw-table-wrap" style="overflow-x:auto"><table class="api-log-table cw-stats-table" style="font-size:0.82rem;width:100%;table-layout:fixed">
            <colgroup>
              <col style="width:15%">
              <col style="width:15%">
              <col style="width:8%">
              <col style="width:8%">
              <col style="width:7%">
              <col style="width:7%">
              <col style="width:7%">
              <col style="width:11%">
              <col style="width:22%">
            </colgroup>
            <thead><tr>
              ${th("genre_key",     "Genre",       "left")}
              ${th("style_key",     "Style",       "left")}
              ${th("in_cache",      "In cache",    "right")}
              ${th("total_cached",  "Cached (run)","right")}
              ${th("total_skipped", "Skipped",     "right")}
              ${th("total_errors",  "Errors",      "right")}
              ${th("current_year",  "Cursor",      "right")}
              ${th("last_run_at",   "Last run",    "left")}
              <th></th>
            </tr></thead>
            <tbody>${rows.map(r => {
              const isActive = !!(active && r.genre_key === active.genreKey && (r.style_key || "") === (active.styleKey || ""));
              const safeG = esc(r.genre_key).replace(/'/g, "\\'");
              const safeS = esc(r.style_key || "").replace(/'/g, "\\'");
              const cursor = r.current_year ? `${r.current_year}·p${r.current_page}` : "—";
              const last = r.last_run_at ? new Date(r.last_run_at).toLocaleString() : "—";
              // No-year sweep indicator: server stamps no_year_last_run_at
              // each time the worker advances through a year=0 sweep. A
              // dim 📅 (never run) or accented 📅✓ (run) makes it obvious
              // which combos still need the no-year pass.
              const nySwept = !!r.no_year_last_run_at;
              const nyTitle = nySwept
                ? `No-year sweep last ran ${new Date(r.no_year_last_run_at).toLocaleString()}${r.no_year_pages_seen ? ` · ${r.no_year_pages_seen} pages` : ""}`
                : "No-year sweep has never been run for this combo";
              const nyPill = `<span title="${esc(nyTitle)}" style="display:inline-block;margin-left:0.4rem;padding:0 0.3rem;border-radius:3px;font-size:0.7rem;border:1px solid ${nySwept ? "rgba(125,225,150,0.5)" : "var(--border)"};color:${nySwept ? "rgb(125,225,150)" : "var(--muted)"};opacity:${nySwept ? "1" : "0.55"};font-variant-numeric:tabular-nums">📅${nySwept ? "✓" : ""}</span>`;
              // has_run distinguishes combos the admin has actually
              // swept from auto-derived ones (only seen via the
              // release_cache genre/style breakdown). Auto-derived
              // rows dim out the run-specific cells; the ↗ button
              // loads any combo into the form so the admin can
              // promote it to a real run.
              const isAuto = !r.has_run;
              const styleLabel = r.style_key || "(all)";
              return `<tr${isActive ? ' style="background:rgba(125,225,150,0.06)"' : (isAuto ? ' style="opacity:0.78"' : "")}>
                <td style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.genre_key)}">${esc(r.genre_key)}</td>
                <td style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:${r.style_key ? "var(--text)" : "var(--muted)"}" title="${esc(styleLabel)}">${esc(styleLabel)}</td>
                <td style="text-align:right;font-variant-numeric:tabular-nums"><strong>${fmt(r.in_cache)}</strong></td>
                <td style="text-align:right;font-variant-numeric:tabular-nums;color:${isAuto ? "var(--muted)" : ""}">${isAuto ? "—" : fmt(r.total_cached)}</td>
                <td style="text-align:right;font-variant-numeric:tabular-nums;color:var(--muted)">${isAuto ? "—" : fmt(r.total_skipped)}</td>
                <td style="text-align:right;font-variant-numeric:tabular-nums;color:${isAuto ? "var(--muted)" : (r.total_errors ? "#e88" : "var(--muted)")}">${isAuto ? "—" : fmt(r.total_errors)}</td>
                <td style="text-align:right;color:var(--muted);font-size:0.74rem">${cursor}</td>
                <td style="color:var(--muted);font-size:0.74rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(last)}${nyPill}</td>
                <td style="text-align:right;white-space:nowrap">
                  <button class="admin-btn" ${running ? "disabled" : ""} onclick="cacheWarmRunComboBlues('${safeG}','${safeS}')" title="Start a cache-warm run for this combo with year range 1900–1970, then automatically chain a no-year sweep so long-tail undated releases get picked up too." style="margin-right:0.25rem">▶ 1900-1970</button>
                  <button class="admin-btn" ${running ? "disabled" : ""} onclick="_cwLoadIntoForm('${safeG}','${safeS}')" title="Load this combo into the form so you can run it" style="margin-right:0.25rem">↗</button>
                  <button class="admin-btn" ${running ? "disabled" : ""} onclick="_cwDeleteCombo('${safeG}','${safeS}', ${r.in_cache || 0})" title="Delete every release_cache row for this (genre, style) combo. Cannot be undone." style="color:#e88;border-color:rgba(232,136,136,0.5)">⌫</button>
                </td>
              </tr>`;
            }).join("")}</tbody>
          </table></div>`;
        })()
        : `<div style="color:var(--muted);font-style:italic;padding:0.5rem 0">No cached releases yet — start a run above.</div>`}
    `;

    if (_cwPollTimer) { clearTimeout(_cwPollTimer); _cwPollTimer = null; }
    if (running && _adminPanelVisible() && document.getElementById("panel-cache-warm")?.style.display !== "none") {
      _cwPollTimer = setTimeout(loadCacheWarm, 20000);
    }
  } catch (e) {
    el.innerHTML = `<span style="color:#e88">Load failed: ${(e && e.message) || e}</span>`;
  }
}
function _cwSyncStyleList() {
  const g = document.getElementById("cw-form-genre")?.value || "";
  const def = _CW_GENRES.find(x => x.key === g) || _CW_GENRES[0];
  const styleEl = document.getElementById("cw-form-style");
  if (styleEl) styleEl.setAttribute("list", `cw-styles-${def.list}`);
}
window._cwSyncStyleList = _cwSyncStyleList;
function _cwLoadIntoForm(genre, style) {
  const g = document.getElementById("cw-form-genre"); if (g) g.value = genre;
  const s = document.getElementById("cw-form-style"); if (s) s.value = style;
  _cwSyncStyleList();
}
window._cwLoadIntoForm = _cwLoadIntoForm;

// Per-row delete from the grid. Reuses the export's delete endpoint
// so the same filter pipeline applies. Requires a typed-N confirm
// above 1000 rows so the bigger ⌫ in a popular genre can't fire on
// a misclick. Refreshes the grid on success.
async function _cwDeleteCombo(genre, style, expectedCount) {
  if (!genre) return;
  const label = style ? `${genre} / ${style}` : `${genre} (all styles)`;
  if (!confirm(`Delete ${Number(expectedCount || 0).toLocaleString()} release_cache row${expectedCount === 1 ? "" : "s"} for ${label}? This cannot be undone.`)) return;
  if (expectedCount > 1000) {
    const typed = prompt(`Type "delete ${expectedCount}" to confirm:`);
    if (typed !== `delete ${expectedCount}`) { alert("Confirmation didn't match — cancelled."); return; }
  }
  try {
    const body = { genre };
    if (style) body.style = style;
    const r = await apiFetch("/api/admin/release-cache/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Delete failed: ${j.error || r.status}`); return; }
    if (typeof loadCacheWarm === "function") { try { loadCacheWarm(); } catch {} }
  } catch (e) { alert(`Delete failed: ${e}`); }
}
window._cwDeleteCombo = _cwDeleteCombo;
async function cacheWarmStartFromForm(resetCursor) {
  const genreKey = document.getElementById("cw-form-genre")?.value || "";
  const styleKey = document.getElementById("cw-form-style")?.value || "";
  const from     = document.getElementById("cw-form-from")?.value || "";
  const to       = document.getElementById("cw-form-to")?.value   || "";
  if (!genreKey) { alert("Pick a genre."); return; }
  try {
    const r = await apiFetch("/api/admin/cache-warm-runs/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        genreKey, styleKey,
        fromYear: from ? Number(from) : undefined,
        toYear:   to   ? Number(to)   : undefined,
        resetCursor: !!resetCursor,
      }),
    });
    if (r.status === 409) {
      const body = await r.json().catch(() => ({}));
      alert(body.error || "Another run is in progress.");
      return;
    }
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.error || `HTTP ${r.status}`);
    }
    loadCacheWarm();
  } catch (e) { alert("Start failed: " + ((e && e.message) || e)); }
}
window.cacheWarmStartFromForm = cacheWarmStartFromForm;
// No-year sweep: starts the worker on the selected genre/style with
// year=0 (worker reads that as "drop the year filter on Discogs
// search"). Captures releases that have NO year on Discogs — these
// are skipped by every year-filtered sweep, so the cache misses them
// entirely until this runs.
async function cacheWarmStartNoYearForForm() {
  const genreKey = document.getElementById("cw-form-genre")?.value || "";
  const styleKey = document.getElementById("cw-form-style")?.value || "";
  if (!genreKey) { alert("Pick a genre."); return; }
  const label = styleKey ? `${genreKey} / ${styleKey}` : genreKey;
  if (!confirm(`Sweep no-year releases for ${label}? This runs until Discogs returns no more pages.`)) return;
  await _cwStartCombo(genreKey, styleKey, 0, 0, true);
}
window.cacheWarmStartNoYearForForm = cacheWarmStartNoYearForForm;


// ── Cache exports ────────────────────────────────────────────────────
// Swap the Style input's datalist to the chosen genre's bucket so
// autocomplete suggests valid Discogs style names for that genre only.
function _rcxSyncStyleList() {
  const g = document.getElementById("rcx-genre")?.value || "";
  const def = _CW_GENRES.find(x => x.key === g);
  const styleEl = document.getElementById("rcx-style");
  if (!styleEl) return;
  styleEl.setAttribute("list", def ? `cw-styles-${def.list}` : "cw-styles-Blues");
}
window._rcxSyncStyleList = _rcxSyncStyleList;

function _rcxBuildParams() {
  const q = new URLSearchParams();
  const add = (k, v) => { if (v !== "" && v != null) q.set(k, v); };
  add("type",      document.getElementById("rcx-type")?.value);
  add("genre",     document.getElementById("rcx-genre")?.value?.trim());
  add("style",     document.getElementById("rcx-style")?.value?.trim());
  add("format",    document.getElementById("rcx-format")?.value?.trim());
  add("year_from", document.getElementById("rcx-year-from")?.value);
  add("year_to",   document.getElementById("rcx-year-to")?.value);
  add("country",   document.getElementById("rcx-country")?.value?.trim());
  if (document.getElementById("rcx-has-yt")?.checked) q.set("has_youtube", "1");
  if (window._rcxSelectedLabels && window._rcxSelectedLabels.size) {
    q.set("labels", Array.from(window._rcxSelectedLabels).join(","));
  }
  add("sort",  document.getElementById("rcx-sort")?.value);
  add("order", document.getElementById("rcx-order")?.value);
  add("limit", document.getElementById("rcx-limit")?.value);
  // ?reader= override for the export panel — empty means "let the
  // server decide based on the global split-cache readers flag".
  add("reader", document.getElementById("rcx-reader")?.value);
  return q;
}

async function rcxDumpSplit() {
  if (!confirm("Stream every row across all 5 split-cache tables (masters_plus + pressings + release_labels + release_artists + release_tags) as one NDJSON file?\n\nFilters are ignored. Uncompressed — file size scales with your cache; expect gigabytes at millions of rows.")) return;
  const link = document.createElement("a");
  link.href = "/api/admin/release-cache/dump-split";
  link.download = "";
  document.body.appendChild(link);
  link.click();
  setTimeout(() => link.remove(), 0);
}
window.rcxDumpSplit = rcxDumpSplit;

async function rcxDumpV1() {
  if (!confirm("Stream every row of the old single-table release_cache (V1) as NDJSON — straight SELECT *, all columns?\n\nFilters are ignored. Uncompressed — expect gigabytes at millions of rows.")) return;
  const link = document.createElement("a");
  link.href = "/api/admin/release-cache/dump-v1";
  link.download = "";
  document.body.appendChild(link);
  link.click();
  setTimeout(() => link.remove(), 0);
}
window.rcxDumpV1 = rcxDumpV1;

async function rcxDumpAll() {
  if (!confirm("Dump the ENTIRE database — every base table, all columns — as one NDJSON file (each row tagged __table)?\n\nThis is everything, including lyrics, words, artists, logs. Uncompressed and potentially very large.")) return;
  const link = document.createElement("a");
  link.href = "/api/admin/db/dump-all";
  link.download = "";
  document.body.appendChild(link);
  link.click();
  setTimeout(() => link.remove(), 0);
}
window.rcxDumpAll = rcxDumpAll;

// ── Labels multi-select picker ────────────────────────────────────
window._rcxSelectedLabels = window._rcxSelectedLabels || new Set();
window._rcxLabelsAll      = window._rcxLabelsAll      || null;  // cached fetch
async function _rcxToggleLabelsPanel(ev) {
  ev?.preventDefault?.(); ev?.stopPropagation?.();
  const panel = document.getElementById("rcx-labels-panel");
  if (!panel) return;
  const open = panel.style.display !== "none" && panel.style.display !== "";
  if (open) { panel.style.display = "none"; return; }
  panel.style.display = "flex";
  if (!window._rcxLabelsAll) {
    document.getElementById("rcx-labels-list").textContent = "Loading…";
    try {
      const r = await apiFetch("/api/admin/release-cache/labels?limit=1000");
      const j = await r.json().catch(() => ({}));
      if (!r.ok) {
        document.getElementById("rcx-labels-list").innerHTML = `<span style="color:#e88">Failed: ${j.error || r.status}</span>`;
        return;
      }
      window._rcxLabelsAll = Array.isArray(j.items) ? j.items : [];
    } catch (e) {
      document.getElementById("rcx-labels-list").innerHTML = `<span style="color:#e88">${String(e).slice(0, 200)}</span>`;
      return;
    }
  }
  _rcxRenderLabelsList();
  // One-shot outside click to close.
  setTimeout(() => {
    const off = (e) => {
      if (!panel.contains(e.target) && e.target.id !== "rcx-labels-btn") {
        panel.style.display = "none";
        document.removeEventListener("click", off, true);
      }
    };
    document.addEventListener("click", off, true);
  }, 0);
}
window._rcxToggleLabelsPanel = _rcxToggleLabelsPanel;
function _rcxRenderLabelsList() {
  const list = document.getElementById("rcx-labels-list");
  if (!list || !window._rcxLabelsAll) return;
  const q = (document.getElementById("rcx-labels-search")?.value || "").toLowerCase().trim();
  const filtered = q
    ? window._rcxLabelsAll.filter(it => String(it.name).toLowerCase().includes(q))
    : window._rcxLabelsAll;
  const esc = escHtml;   // canonical escaper (shared.js) — escapes & < > " '
  const rows = filtered.slice(0, 600).map(it => {
    const checked = window._rcxSelectedLabels.has(it.name) ? "checked" : "";
    return `<label style="display:flex;gap:0.4rem;align-items:center;padding:0.15rem 0;cursor:pointer">
      <input type="checkbox" ${checked} onchange="_rcxToggleLabel(this,'${esc(it.name)}')">
      <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(it.name)}</span>
      <span style="color:var(--muted);font-size:0.72rem">${Number(it.count).toLocaleString()}</span>
    </label>`;
  }).join("");
  list.innerHTML = rows || `<div style="color:var(--muted)">No matches.</div>`;
  const countEl = document.getElementById("rcx-labels-count");
  if (countEl) countEl.textContent = `${window._rcxSelectedLabels.size} selected · ${filtered.length} shown of ${window._rcxLabelsAll.length}`;
}
window._rcxRenderLabelsList = _rcxRenderLabelsList;
function _rcxToggleLabel(cb, name) {
  if (cb.checked) window._rcxSelectedLabels.add(name);
  else window._rcxSelectedLabels.delete(name);
  _rcxUpdateLabelsButton();
  const countEl = document.getElementById("rcx-labels-count");
  if (countEl && window._rcxLabelsAll) {
    const q = (document.getElementById("rcx-labels-search")?.value || "").toLowerCase().trim();
    const filtered = q ? window._rcxLabelsAll.filter(it => String(it.name).toLowerCase().includes(q)) : window._rcxLabelsAll;
    countEl.textContent = `${window._rcxSelectedLabels.size} selected · ${filtered.length} shown of ${window._rcxLabelsAll.length}`;
  }
}
window._rcxToggleLabel = _rcxToggleLabel;
function _rcxClearLabels() {
  window._rcxSelectedLabels.clear();
  _rcxRenderLabelsList();
  _rcxUpdateLabelsButton();
}
window._rcxClearLabels = _rcxClearLabels;
function _rcxUpdateLabelsButton() {
  const btn = document.getElementById("rcx-labels-btn");
  if (!btn) return;
  const n = window._rcxSelectedLabels.size;
  if (!n) { btn.textContent = "(any)"; return; }
  const names = Array.from(window._rcxSelectedLabels);
  btn.textContent = n <= 3 ? names.join(", ") : `${names.slice(0, 2).join(", ")} +${n - 2} more`;
}
window._rcxUpdateLabelsButton = _rcxUpdateLabelsButton;
async function rcxPreview() {
  const status = document.getElementById("rcx-status");
  if (status) status.textContent = "Counting…";
  try {
    const r = await apiFetch("/api/admin/release-cache/preview?" + _rcxBuildParams().toString());
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { if (status) status.textContent = `Failed: ${j.error || r.status}`; return; }
    if (status) status.textContent = `${(j.count ?? 0).toLocaleString()} row${j.count === 1 ? "" : "s"} match.`;
  } catch (e) { if (status) status.textContent = `Failed: ${e}`; }
}
window.rcxPreview = rcxPreview;
async function rcxDownload() {
  const status = document.getElementById("rcx-status");
  const format = document.getElementById("rcx-format-out")?.value || "csv";
  const params = _rcxBuildParams();
  // OUTPUT format goes under `out` — the form's "Format contains"
  // filter (Vinyl/CD/etc.) also uses `format`, so this collision
  // was silently turning every download into `formats CONTAINS "csv"`
  // and matching 0 rows.
  params.set("out", format);
  if (status) status.textContent = "Preparing download…";
  // Streamed responses need the auth token in the URL too — easiest
  // path is to fetch with apiFetch, get a Blob, then create an object
  // URL and click an anchor. Otherwise navigate the browser to the URL
  // (no Bearer header) and it'd 401.
  try {
    const r = await apiFetch("/api/admin/release-cache/export?" + params.toString(), { timeoutMs: 1000 * 60 * 10 });
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      if (status) status.textContent = `Failed: ${j.error || r.status}`;
      return;
    }
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    const ext = format === "json" ? "json" : (format === "ndjson" ? "ndjson" : "csv");
    a.download = `seadisco-release-cache-${new Date().toISOString().slice(0,10)}.${ext}`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
    if (status) status.textContent = `Downloaded (${(blob.size/1024/1024).toFixed(1)} MB).`;
  } catch (e) {
    if (status) status.textContent = `Failed: ${e}`;
  }
}
window.rcxDownload = rcxDownload;

// Delete every release_cache row matching the current filter form.
// Two-step confirm: hit /preview to get the count, then a typed
// confirmation for anything > 1000 rows so a careless click can't
// wipe a whole genre. Refuses entirely when no filter is set.
async function rcxDelete() {
  const status = document.getElementById("rcx-status");
  const params = _rcxBuildParams();
  // Drop pagination-only params; they don't affect the WHERE.
  for (const k of ["sort", "order", "limit"]) params.delete(k);
  if ([...params.keys()].length === 0) {
    if (status) status.textContent = "Set at least one filter — refusing to wipe the entire cache.";
    return;
  }
  if (status) status.textContent = "Counting…";
  let count = 0;
  try {
    const pr = await apiFetch("/api/admin/release-cache/preview?" + params.toString());
    const pj = await pr.json().catch(() => ({}));
    if (!pr.ok) { if (status) status.textContent = `Preview failed: ${pj.error || pr.status}`; return; }
    count = Number(pj.count ?? 0);
  } catch (e) { if (status) status.textContent = `Preview failed: ${e}`; return; }
  if (!count) { if (status) status.textContent = "0 rows match — nothing to delete."; return; }
  const summary = [...params.entries()].map(([k, v]) => `${k}=${v}`).join(", ");
  if (!confirm(`Delete ${count.toLocaleString()} release_cache row${count === 1 ? "" : "s"} matching:\n${summary}\n\nThis cannot be undone.`)) {
    if (status) status.textContent = "Cancelled.";
    return;
  }
  if (count > 1000) {
    const typed = prompt(`Type "delete ${count}" to confirm:`);
    if (typed !== `delete ${count}`) { if (status) status.textContent = "Confirmation didn't match — cancelled."; return; }
  }
  if (status) status.textContent = "Deleting…";
  try {
    const body = Object.fromEntries(params.entries());
    const r = await apiFetch("/api/admin/release-cache/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { if (status) status.textContent = `Failed: ${j.error || r.status}`; return; }
    if (status) status.textContent = `Deleted ${Number(j.deleted ?? 0).toLocaleString()} row${j.deleted === 1 ? "" : "s"}.`;
    // Refresh the cache-warm grid so the totals reflect the delete —
    // the server already invalidated its memoized stats cache, so
    // this fetch recomputes fresh.
    if (typeof loadCacheWarm === "function") { try { loadCacheWarm(); } catch {} }
  } catch (e) { if (status) status.textContent = `Failed: ${e}`; }
}
window.rcxDelete = rcxDelete;

function _baxBuildParams() {
  const q = new URLSearchParams();
  const add = (k, v) => { if (v !== "" && v != null) q.set(k, v); };
  add("name",     document.getElementById("bax-name")?.value?.trim());
  add("hometown", document.getElementById("bax-hometown")?.value?.trim());
  add("born_decade", document.getElementById("bax-decade")?.value);
  if (document.getElementById("bax-has-discogs")?.checked) q.set("has_discogs", "1");
  if (document.getElementById("bax-has-wiki")?.checked)    q.set("has_wiki", "1");
  if (document.getElementById("bax-has-youtube")?.checked) q.set("has_youtube", "1");
  return q;
}
async function baxPreview() {
  const status = document.getElementById("bax-status");
  if (status) status.textContent = "Counting…";
  try {
    const r = await apiFetch("/api/admin/blues/preview?" + _baxBuildParams().toString());
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { if (status) status.textContent = `Failed: ${j.error || r.status}`; return; }
    if (status) status.textContent = `${(j.count ?? 0).toLocaleString()} artist${j.count === 1 ? "" : "s"} match.`;
  } catch (e) { if (status) status.textContent = `Failed: ${e}`; }
}
window.baxPreview = baxPreview;
async function baxDownload() {
  const status = document.getElementById("bax-status");
  const format = document.getElementById("bax-format-out")?.value || "csv";
  const params = _baxBuildParams();
  // existing endpoints: /api/admin/blues/export.{csv,pdf,json,ndjson}
  // CSV + PDF accept only the legacy sort/order params; the new
  // JSON/NDJSON ones honour the filter set above. We pass the filters
  // anyway — the existing CSV/PDF endpoints just ignore unknown keys.
  const url = `/api/admin/blues/export.${format}?${params.toString()}`;
  if (status) status.textContent = "Preparing download…";
  try {
    const r = await apiFetch(url, { timeoutMs: 1000 * 60 * 10 });
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      if (status) status.textContent = `Failed: ${j.error || r.status}`;
      return;
    }
    const blob = await r.blob();
    const objectUrl = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = objectUrl;
    a.download = `seadisco-blues-artists-${new Date().toISOString().slice(0,10)}.${format}`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(objectUrl), 60_000);
    if (status) status.textContent = `Downloaded (${(blob.size/1024/1024).toFixed(1)} MB).`;
  } catch (e) {
    if (status) status.textContent = `Failed: ${e}`;
  }
}
window.baxDownload = baxDownload;
// Per-row "▶ 1900-1970" — directly fires a year-bounded run on a
// specific (genre, style) combo without round-tripping through the
// form. Used by the per-combo stats grid.
async function cacheWarmRunComboBlues(genreKey, styleKey) {
  if (!genreKey) return;
  const label = styleKey ? `${genreKey} / ${styleKey}` : genreKey;
  if (!confirm(`Start ${label} cache-warm for 1900–1970 (chains no-year sweep on completion)?`)) return;
  // ▶ 1900-1970 is meant as a one-click "cover this genre": dated walk
  // first, then a no-year sweep so long-tail undated releases get
  // picked up in the same session.
  await _cwStartCombo(genreKey, styleKey, 1900, 1970, false, { alsoNoYear: true });
}
window.cacheWarmRunComboBlues = cacheWarmRunComboBlues;
async function _cwStartCombo(genreKey, styleKey, fromYear, toYear, resetCursor, extra) {
  try {
    const body = { genreKey, styleKey, fromYear, toYear, resetCursor: !!resetCursor };
    if (extra && typeof extra === "object") Object.assign(body, extra);
    const r = await apiFetch("/api/admin/cache-warm-runs/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (r.status === 409) {
      const body = await r.json().catch(() => ({}));
      alert(body.error || "Another run is in progress.");
      return;
    }
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.error || `HTTP ${r.status}`);
    }
    loadCacheWarm();
  } catch (e) { alert("Start failed: " + ((e && e.message) || e)); }
}
async function cacheWarmForceClear() {
  if (!confirm("Force-clear the in-memory 'running' lock? Use this when a worker crashed silently and Start now refuses to fire. Doesn't affect cached data.")) return;
  try {
    const r = await apiFetch("/api/admin/cache-warm-runs/force-clear", { method: "POST" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    loadCacheWarm();
  } catch (e) { alert("Force clear failed: " + ((e && e.message) || e)); }
}
window.cacheWarmForceClear = cacheWarmForceClear;
async function cacheWarmStop() {
  try {
    const r = await apiFetch("/api/admin/cache-warm-runs/stop", { method: "POST" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    loadCacheWarm();
  } catch (e) { alert("Stop failed: " + ((e && e.message) || e)); }
}
window.cacheWarmStop = cacheWarmStop;

// Legacy stub — kept so any session that cached the old admin.html
// can call this safely. No-op; the new path is loadCacheWarm().
async function loadGenreCacheWarm() { return loadCacheWarm(); }

// ── All Blues worker (admin panel) ────────────────────────────────
// Same pattern as cache-warm: Start posts the year window + reset
// flag, Stop signals the running worker, status polls counters.
// ── YT Review worker (v1) ────────────────────────────────────────────
// Background YouTube-match proposer for earliest-year Blues masters.
// Every candidate goes through human review here before being pinned.
let _ytrPage = 0;
let _ytrStatus = "pending";
const _YTR_LIMIT = 50;
let _ytrPollTimer = null;
async function loadYtReview() {
  const stEl = document.getElementById("ytr-status");
  if (!stEl) return;
  // esc is a function-local const elsewhere in this file (loadCacheWarm
  // et al), not global — define our own here so the templates below
  // don't ReferenceError.
  const esc = escHtml;   // canonical escaper (shared.js) — escapes & < > " '
  try {
    const r = await apiFetch("/api/admin/yt-review/status");
    if (!r.ok) { stEl.innerHTML = `<span style="color:#e88">Failed: HTTP ${r.status}</span>`; return; }
    const s = await r.json();
    const c = s.counts || { pending: 0, approved: 0, rejected: 0, skipped: 0, total: 0 };
    const running = !!s.running;
    const st = s.state || {};
    const runPill = running
      ? `<span style="padding:0.1rem 0.4rem;border-radius:999px;background:rgba(125,225,150,0.15);color:#7ed196;border:1px solid rgba(125,225,150,0.4);font-size:0.7rem;font-weight:600">● RUNNING</span>`
      : `<span style="padding:0.1rem 0.4rem;border-radius:999px;background:rgba(255,255,255,0.04);color:var(--muted);border:1px solid var(--border);font-size:0.7rem">○ Idle</span>`;
    stEl.innerHTML = `
      <div style="display:flex;flex-wrap:wrap;gap:0.7rem;align-items:center;margin-bottom:0.5rem">
        ${runPill}
        ${running
          ? `<button class="admin-btn" onclick="ytrStop()" title="Signal the worker to wind down at the next safe boundary.">■ Stop</button>`
          : `<button class="admin-btn" onclick="ytrStart()" title="Walk earliest-year Blues masters and propose YouTube videos for tracks with no override yet. Throttled to 1 search per ${Math.round((s.throttleMs||45000)/1000)}s; daily budget ${s.dailyBudget}.">▶ Start</button>
             <button class="admin-btn" onclick="ytrRestartFromTop()" title="Clear the walk cursor so the next Start begins at the earliest Blues master again. Doesn't touch already-approved / rejected rows or re-search tracks (per-track search log is preserved).">↻ Restart from top</button>`}
        <span style="font-size:0.78rem;color:var(--muted)">cursor: <strong style="color:var(--text)">${st.cursor_year ?? "—"}</strong> · master <strong style="color:var(--text)">${st.cursor_master_id ?? "—"}</strong></span>
        <span style="font-size:0.74rem;color:var(--muted)" title="Worker searches today / daily cap. Hard cap so manual searches always have budget left.">worker: <strong style="color:var(--text);font-variant-numeric:tabular-nums">${Number(s.searchesToday||0).toLocaleString()}</strong>/${Number(s.dailyBudget||9000).toLocaleString()}</span>
        <span style="font-size:0.74rem;color:var(--muted)" title="Project-wide YouTube quota units consumed today (worker + manual). Resets at UTC midnight.">project: <strong style="color:var(--text);font-variant-numeric:tabular-nums">${Number(s.projectUnitsToday||0).toLocaleString()}</strong>/${Number(s.projectUnitsCap||950000).toLocaleString()} u</span>
        <span style="font-size:0.78rem;color:var(--muted);margin-left:auto">${esc(st.message || "Idle.")}</span>
      </div>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:0.5rem;font-size:0.82rem">
        ${ytrTile("Pending",  c.pending,  "#7ed196", "pending")}
        ${ytrTile("Approved", c.approved, "#7ed196", "approved")}
        ${ytrTile("Rejected", c.rejected, "#e88",    "rejected")}
        ${ytrTile("Skipped",  c.skipped,  "var(--muted)", "skipped")}
        ${ytrTile("Searched", st.total_searched || 0, "var(--text)")}
        ${ytrTile("Queued",   st.total_queued   || 0, "var(--text)")}
        <div style="cursor:pointer" onclick="ytrShowErrors()" title="Click to view recent worker errors.">
          ${ytrTile("Errors",   st.total_errors   || 0, st.total_errors ? "#e88" : "var(--muted)")}
        </div>
      </div>
    `;
    // Poll while running so the cursor + counters stay live.
    if (_ytrPollTimer) { clearTimeout(_ytrPollTimer); _ytrPollTimer = null; }
    if (running && _adminPanelVisible() && document.getElementById("panel-yt-review")?.style.display !== "none") {
      _ytrPollTimer = setTimeout(loadYtReview, 5000);
    }
    await loadYtReviewQueue();
  } catch (e) { stEl.innerHTML = `<span style="color:#e88">Failed: ${esc(e?.message || e)}</span>`; }
}
function ytrTile(label, n, color, status) {
  const onclick = status ? ` style="cursor:pointer;text-decoration:underline" onclick="ytrSetStatus('${status}')"` : "";
  return `<div${onclick ? ' ' + onclick : ''} style="border:1px solid var(--border);border-radius:5px;padding:0.4rem 0.55rem">
    <div style="font-size:0.7rem;color:var(--muted);text-transform:uppercase">${label}</div>
    <div style="font-size:0.95rem;font-weight:600;color:${color};font-variant-numeric:tabular-nums">${Number(n||0).toLocaleString()}</div>
  </div>`;
}
function ytrSetStatus(s) { _ytrStatus = s; _ytrPage = 0; loadYtReviewQueue(); }
window.ytrSetStatus = ytrSetStatus;
async function ytrStart() {
  try {
    const r = await apiFetch("/api/admin/yt-review/start", { method: "POST" });
    if (r.status === 409) { alert("Already running."); loadYtReview(); return; }
    if (!r.ok) { alert(`Start failed: HTTP ${r.status}`); return; }
    loadYtReview();
  } catch (e) { alert(`Start failed: ${e?.message || e}`); }
}
window.ytrStart = ytrStart;
async function ytrRestartFromTop() {
  if (!confirm("Reset the YT review walk cursor to the earliest Blues master and start the worker?\n\nApproved / rejected rows are untouched. Tracks already searched will still be skipped, so the walk mostly races through until it finds new work.")) return;
  const alsoResetSearchLog = confirm("Also wipe the per-track search log (every track will be re-searched — burns YouTube quota)?\n\nOK = wipe log too. Cancel = keep the search log (recommended).");
  try {
    const r = await apiFetch("/api/admin/yt-review/reset-cursor", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ alsoResetSearchLog }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Reset failed: ${j.error || r.status}`); return; }
    // Fire the worker off immediately. Prior version left the cursor
    // reset but idle, which surprised users who read "restart" as
    // "reset AND start" — matches how other bulk workers behave.
    const startR = await apiFetch("/api/admin/yt-review/start", { method: "POST" });
    if (startR.status === 409) {
      // Rare — someone else clicked Start in the ~1s gap. Not worth alerting.
    } else if (!startR.ok) {
      alert(`Cursor reset but Start failed: HTTP ${startR.status}. Click ▶ Start to run.`);
    }
    if (alsoResetSearchLog) {
      alert(`Cursor cleared and worker started. Also wiped ${j.clearedSearches ?? 0} per-track search log rows.`);
    }
    loadYtReview();
  } catch (e) { alert(`Reset failed: ${e?.message || e}`); }
}
window.ytrRestartFromTop = ytrRestartFromTop;
async function ytrStop() {
  try {
    await apiFetch("/api/admin/yt-review/stop", { method: "POST" });
    loadYtReview();
  } catch (e) { alert(`Stop failed: ${e?.message || e}`); }
}
window.ytrStop = ytrStop;
async function loadYtReviewQueue() {
  const el = document.getElementById("ytr-queue");
  if (!el) return;
  const esc = escHtml;   // canonical escaper (shared.js) — escapes & < > " '
  try {
    const params = new URLSearchParams({ status: _ytrStatus, limit: String(_YTR_LIMIT), offset: String(_ytrPage * _YTR_LIMIT) });
    const r = await apiFetch(`/api/admin/yt-review/queue?${params}`);
    if (!r.ok) { el.innerHTML = `<span style="color:#e88">Queue load failed: HTTP ${r.status}</span>`; return; }
    const { rows = [], total = 0 } = await r.json();
    if (!rows.length) {
      el.innerHTML = `<div style="color:var(--muted);padding:0.6rem 0;font-style:italic">No ${_ytrStatus} rows.</div>`;
      ytrRenderPager(total);
      return;
    }
    el.innerHTML = rows.map(ytrRowHtml).join("");
    ytrRenderPager(total);
  } catch (e) { el.innerHTML = `<span style="color:#e88">Queue load failed: ${esc(e?.message || e)}</span>`; }
}
function ytrRowHtml(r) {
  const decode = s => String(s ?? "").replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&apos;/g,"'").replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/&amp;/g,"&");
  const esc = s => decode(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/'/g,"&#39;").replace(/"/g,"&quot;");
  const ytUrl = `https://www.youtube.com/watch?v=${esc(r.candidate_video_id)}`;
  const score = r.title_score != null ? Number(r.title_score).toFixed(2) : "—";
  const showActions = _ytrStatus === "pending";
  const showDelete = _ytrStatus === "approved";
  const yr = r.master_year || "?";
  return `<div style="border:1px solid var(--border);border-radius:6px;padding:0.6rem 0.75rem;display:grid;grid-template-columns:64px 64px 1fr auto;gap:0.7rem;align-items:center;background:rgba(255,255,255,0.015)">
    ${r.master_cover_url
      ? `<img src="${esc(r.master_cover_url)}" alt="" style="width:64px;height:64px;object-fit:cover;border-radius:4px;background:var(--border)" loading="lazy">`
      : `<div style="width:64px;height:64px;border-radius:4px;background:rgba(255,255,255,0.04)"></div>`}
    ${r.candidate_thumbnail_url
      ? `<a href="${esc(ytUrl)}" target="_blank" rel="noopener" title="Open on YouTube"><img src="${esc(r.candidate_thumbnail_url)}" alt="" style="width:64px;height:64px;object-fit:cover;border-radius:4px;background:var(--border)" loading="lazy"></a>`
      : `<div style="width:64px;height:64px;border-radius:4px;background:rgba(255,255,255,0.04)"></div>`}
    <div style="min-width:0">
      <div style="font-size:0.86rem;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><span style="color:var(--muted);font-weight:normal">${yr} · ${esc(r.track_position || "")}</span> ${esc(r.track_title || "")} <span style="color:var(--muted);font-weight:normal">— ${esc(r.track_artist || "")}</span></div>
      <div style="font-size:0.78rem;color:var(--muted);margin-top:0.15rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><a href="${esc(ytUrl)}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">${esc(r.candidate_title || "")}</a> <span style="color:#555">·</span> ${esc(r.candidate_channel_title || "")} <span style="color:#555">·</span> match ${score}</div>
      <div style="font-size:0.72rem;color:var(--muted);margin-top:0.15rem">master #${r.master_id} ${r.reviewed_by ? `· decided by ${esc(r.reviewed_by)}` : ""}</div>
    </div>
    ${showActions
      ? `<div style="display:flex;flex-direction:column;gap:0.3rem;align-items:flex-end">
          <div style="display:flex;gap:0.3rem;align-items:center">
            <button class="admin-btn" onclick="ytrDecide(${r.id},'approve')" title="Approve and pin this video to the track as a master override.">✓ Approve</button>
            <button class="admin-btn" onclick="ytrDecide(${r.id},'reject')" title="Reject this candidate. Worker won't re-propose this video on this track.">✗ Reject</button>
            <button class="admin-btn" onclick="ytrDecide(${r.id},'skip')" title="Skip — neither pin nor reject, just remove from the pending queue. Track can still be re-proposed.">Skip</button>
          </div>
          <div style="display:flex;gap:0.3rem;align-items:center">
            <input type="text" id="ytr-custom-${r.id}" placeholder="paste YouTube URL or ID" style="width:14rem;padding:0.15rem 0.35rem;font-size:0.75rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px" onkeydown="if(event.key==='Enter'){event.preventDefault();ytrCustomApprove(${r.id});}">
            <button class="admin-btn" onclick="ytrCustomApprove(${r.id})" title="Pin your own URL to this track instead of the worker's candidate. Overwrites any existing override.">↳ Use my URL</button>
          </div>
        </div>`
      : showDelete
        ? `<div style="display:flex;gap:0.3rem;align-items:center">
            <button class="admin-btn" onclick="ytrDeleteApproval(${r.id})" title="Remove the override this approval created and mark the candidate rejected.">🗑 Delete</button>
          </div>`
        : `<div style="color:var(--muted);font-size:0.74rem">${esc(r.status || "")}</div>`}
  </div>`;
}
function ytrRenderPager(total) {
  const el = document.getElementById("ytr-pager");
  if (!el) return;
  const pages = Math.max(1, Math.ceil(total / _YTR_LIMIT));
  const cur = _ytrPage + 1;
  if (pages <= 1) { el.innerHTML = `<span style="color:var(--muted)">${total} row${total === 1 ? "" : "s"}</span>`; return; }
  el.innerHTML = `
    <button class="admin-btn" ${cur <= 1 ? "disabled" : ""} onclick="ytrPage(${_ytrPage - 1})">‹ Prev</button>
    <span style="color:var(--muted)">Page ${cur} / ${pages} · ${total.toLocaleString()} ${_ytrStatus}</span>
    <button class="admin-btn" ${cur >= pages ? "disabled" : ""} onclick="ytrPage(${_ytrPage + 1})">Next ›</button>
  `;
}
function ytrPage(p) { _ytrPage = Math.max(0, p); loadYtReviewQueue(); }
window.ytrPage = ytrPage;
async function ytrDecide(id, action) {
  try {
    const r = await apiFetch("/api/admin/yt-review/decide", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id, action }),
    });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      alert(`${action} failed: ${body?.error || `HTTP ${r.status}`}`);
      return;
    }
    loadYtReview();
  } catch (e) { alert(`${action} failed: ${e?.message || e}`); }
}
window.ytrDecide = ytrDecide;
async function ytrCustomApprove(id) {
  const input = document.getElementById(`ytr-custom-${id}`);
  const url = (input?.value || "").trim();
  if (!url) { alert("Paste a YouTube URL or 11-char video ID first."); input?.focus(); return; }
  try {
    const r = await apiFetch("/api/admin/yt-review/custom-approve", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id, url }),
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) { alert(`Custom approve failed: ${body?.error || `HTTP ${r.status}`}${body?.detail ? "\n" + body.detail : ""}`); return; }
    loadYtReview();
  } catch (e) { alert(`Custom approve failed: ${e?.message || e}`); }
}
window.ytrCustomApprove = ytrCustomApprove;
async function ytrDeleteApproval(id) {
  if (!confirm("Remove this approval? The pinned YouTube override will be deleted and the candidate marked rejected.")) return;
  try {
    const r = await apiFetch("/api/admin/yt-review/delete-approval", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id }),
    });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      alert(`Delete failed: ${body?.error || `HTTP ${r.status}`}`);
      return;
    }
    loadYtReview();
  } catch (e) { alert(`Delete failed: ${e?.message || e}`); }
}
window.ytrDeleteApproval = ytrDeleteApproval;
async function ytrShowErrors() {
  const esc = escHtml;   // canonical escaper (shared.js) — escapes & < > " '
  const existing = document.getElementById("ytr-errors-modal");
  if (existing) existing.remove();
  const overlay = document.createElement("div");
  overlay.id = "ytr-errors-modal";
  overlay.style.cssText = "position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:9999;display:flex;align-items:center;justify-content:center;padding:1rem";
  overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
  overlay.innerHTML = `<div style="background:var(--bg);border:1px solid var(--border);border-radius:8px;max-width:900px;width:100%;max-height:80vh;overflow:auto;padding:1rem">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.6rem">
      <div style="font-weight:600">YT Review worker errors</div>
      <button class="admin-btn" onclick="document.getElementById('ytr-errors-modal').remove()">Close</button>
    </div>
    <div id="ytr-errors-body" style="color:var(--muted);font-size:0.85rem">Loading…</div>
  </div>`;
  document.body.appendChild(overlay);
  try {
    const r = await apiFetch("/api/admin/yt-review/errors?limit=200");
    if (!r.ok) { document.getElementById("ytr-errors-body").innerHTML = `<span style="color:#e88">HTTP ${r.status}</span>`; return; }
    const { rows = [] } = await r.json();
    const body = document.getElementById("ytr-errors-body");
    if (!rows.length) { body.innerHTML = "No errors logged."; return; }
    body.innerHTML = `<div style="display:grid;grid-template-columns:160px 90px 1fr 1fr;gap:0.4rem;font-size:0.78rem;font-family:ui-monospace,monospace">
      <div style="color:var(--muted);text-transform:uppercase;font-size:0.68rem">When</div>
      <div style="color:var(--muted);text-transform:uppercase;font-size:0.68rem">Master</div>
      <div style="color:var(--muted);text-transform:uppercase;font-size:0.68rem">Query</div>
      <div style="color:var(--muted);text-transform:uppercase;font-size:0.68rem">Reason</div>
      ${rows.map(r => `
        <div>${esc(new Date(r.ts).toLocaleString())}</div>
        <div>${r.master_id ?? "—"}</div>
        <div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.query || "")}">${esc(r.query || "—")}</div>
        <div style="color:#e88;overflow-wrap:anywhere">${esc(r.reason || "")}</div>
      `).join("")}
    </div>`;
  } catch (e) {
    const b = document.getElementById("ytr-errors-body");
    if (b) b.innerHTML = `<span style="color:#e88">${esc(e?.message || e)}</span>`;
  }
}
window.ytrShowErrors = ytrShowErrors;
window.loadYtReview = loadYtReview;


// ── API Request Log ───────────────────────────────────────────────────
let _apiLogSeq = 0;
async function loadApiLog() {
  const mySeq = ++_apiLogSeq;
  try {
    const service = document.getElementById("api-log-filter").value;
    const errorsOnly = document.getElementById("api-log-errors-only").checked;
    const scheduledOnly = document.getElementById("api-log-scheduled-only").checked;
    const hours = document.getElementById("api-log-hours").value || "24";
    const params = new URLSearchParams({ hours });
    if (service) params.set("service", service);
    if (errorsOnly) params.set("errors", "true");
    if (scheduledOnly) params.set("scheduled", "true");

    // Update header label
    const rangeLabel = document.getElementById("api-log-range-label");
    if (rangeLabel) rangeLabel.textContent = hours === "168" ? "(7 days)" : "(24h)";

    params.set("_t", Date.now().toString());
    const logRes = await apiFetch(`/api/admin/api-log?${params}`);
    if (!logRes.ok || mySeq !== _apiLogSeq) return;
    const { items, total } = await logRes.json();
    if (mySeq !== _apiLogSeq) return;

    // Derive stats from the same items so they always match
    const statsMap = {};
    for (const it of items) {
      const s = statsMap[it.service] ??= { service: it.service, total_requests: 0, successes: 0, failures: 0, durations: [], last_request_at: null };
      s.total_requests++;
      if (it.success) s.successes++; else s.failures++;
      if (it.duration_ms != null) s.durations.push(it.duration_ms);
      if (!s.last_request_at || new Date(it.created_at) > new Date(s.last_request_at)) s.last_request_at = it.created_at;
    }
    const stats = Object.values(statsMap).map(s => ({
      ...s,
      avg_duration_ms: s.durations.length ? Math.round(s.durations.reduce((a, b) => a + b, 0) / s.durations.length) : null
    })).sort((a, b) => b.total_requests - a.total_requests);

    const statsEl = document.getElementById("api-stats-summary");
    if (stats.length) {
      const svcColors = { discogs: "#7eb8da" };
      statsEl.innerHTML = `<div style="display:flex;flex-wrap:wrap;gap:0.4rem 1.2rem">` + stats.map(s => {
        const failRate = s.total_requests ? Math.round((s.failures / s.total_requests) * 100) : 0;
        const failColor = failRate > 20 ? "#ff6b35" : failRate > 5 ? "#e8d44d" : "#6b8f71";
        const svcColor = svcColors[s.service] || "#e8d44d";
        return `<div style="font-size:0.78rem;line-height:1.5">
          <strong style="color:${svcColor};text-transform:uppercase;letter-spacing:0.03em">${s.service}</strong>
          <span style="color:var(--muted);margin-left:0.3rem">${s.total_requests}</span>
          <span style="color:${failColor};margin-left:0.2rem">${s.failures > 0 ? `(${s.failures} fail)` : ''}</span>
          <span style="color:#7a6d58;margin-left:0.2rem">${s.avg_duration_ms ?? "?"}ms</span>
        </div>`;
      }).join("") + `</div>`;
    } else {
      statsEl.textContent = "No API requests in the last 24 hours.";
    }

    const listEl = document.getElementById("api-log-list");
    if (!items.length) { listEl.textContent = "No log entries found."; return; }

    const svcColors = { discogs: "#7eb8da" };

    const rows = items.map(item => {
      const time = new Date(item.created_at).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
      const date = new Date(item.created_at).toLocaleDateString(undefined, { month: "short", day: "numeric" });
      const ok = item.success;
      const statusColor = ok ? "#6b8f71" : "#ff6b35";
      const statusIcon = ok ? "\u2713" : "\u2717";
      const svcColor = svcColors[item.service] || "#e8d44d";
      const dur = item.duration_ms != null ? item.duration_ms : "";
      const durColor = dur > 5000 ? "#ff6b35" : dur > 2000 ? "#e8d44d" : "#7a6d58";
      const ctx = item.context || "";
      const err = item.error_message && !ok ? item.error_message.replace(/</g,"&lt;").slice(0, 150) : "";
      let ep = item.endpoint.replace(/^https?:\/\/[^/]+/, "");
      if (ep.length > 70) ep = ep.slice(0, 70) + "\u2026";

      return `<tr class="${ok ? '' : 'api-row-err'}">
        <td class="api-td-icon" style="color:${statusColor}">${statusIcon}</td>
        <td class="api-td-time">${date}<br>${time}</td>
        <td class="api-td-svc" style="color:${svcColor}">${item.service}</td>
        <td class="api-td-code" style="color:${ok ? '#6b8f71' : '#ff6b35'}">${item.status_code || "\u2014"}</td>
        <td class="api-td-dur" style="color:${durColor}">${dur ? dur + 'ms' : ''}</td>
        <td class="api-td-ctx">${ctx}</td>
        <td class="api-td-ep">${ep}${err ? `<div class="api-td-err">${err}</div>` : ''}</td>
      </tr>`;
    }).join("");

    listEl.innerHTML = `<div style="margin-bottom:0.4rem;color:var(--text);font-size:0.78rem;font-weight:600">${total} entries</div>
      <table class="api-log-table">
        <thead><tr>
          <th></th><th>Time</th><th>Service</th><th>Status</th><th>Duration</th><th>Context</th><th>Endpoint</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  } catch { /* not admin */ }
}

// ── Feedback Inbox ────────────────────────────────────────────────────
function renderFeedback(items) {
  const el = document.getElementById("feedback-list");
  if (!items.length) { el.textContent = "No feedback yet."; return; }
  el.innerHTML = items.map(({ id, user_email, message, created_at }) => {
    const date = new Date(created_at).toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
    return `<div id="fb-${id}" style="padding:0.6rem 0;border-bottom:1px solid var(--border)">
      <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:0.25rem">
        <span style="color:#aaa;font-size:0.8rem">${user_email || "unknown"}</span>
        <div style="display:flex;gap:0.75rem;align-items:center">
          <span style="color:#555;font-size:0.75rem">${date}</span>
          <button onclick="deleteFeedbackItem(${id})" style="background:none;border:none;color:#666;cursor:pointer;font-size:0.75rem;padding:0" title="Delete">\u2715</button>
        </div>
      </div>
      <div style="color:var(--fg)">${message.replace(/</g,"&lt;")}</div>
    </div>`;
  }).join("");
}

async function deleteFeedbackItem(id) {
  const r = await apiFetch(`/api/admin/feedback/${id}`, { method: "DELETE" });
  if (r.ok) {
    document.getElementById(`fb-${id}`)?.remove();
    const n = document.querySelectorAll('#feedback-list [id^="fb-"]').length;
    _adminUpdateFeedbackDot(n);
    const cEl = document.getElementById("admin-feedback-count");
    if (cEl) cEl.textContent = n ? `${n} message${n === 1 ? "" : "s"}` : "";
  }
}

// ── Feedback popup + unread dot ───────────────────────────────────────
// "Unread" = feedback count exceeds the last count the admin saw
// (persisted in localStorage). Opening the popup marks the current
// count as seen and clears the dot.
function _adminUpdateFeedbackDot(count) {
  const dot = document.getElementById("admin-feedback-dot");
  if (!dot) return;
  let seen = 0;
  try { seen = parseInt(localStorage.getItem("sd-admin-fb-seen") || "0", 10) || 0; } catch {}
  dot.style.display = (count > seen) ? "" : "none";
  const cEl = document.getElementById("admin-feedback-count");
  if (cEl) cEl.textContent = count ? `${count} message${count === 1 ? "" : "s"}` : "";
}
function adminOpenFeedback() {
  const ov = document.getElementById("admin-feedback-overlay");
  if (ov) ov.style.display = "flex";
  const n = document.querySelectorAll('#feedback-list [id^="fb-"]').length;
  try { localStorage.setItem("sd-admin-fb-seen", String(n)); } catch {}
  const dot = document.getElementById("admin-feedback-dot");
  if (dot) dot.style.display = "none";
}
function adminCloseFeedback() {
  const ov = document.getElementById("admin-feedback-overlay");
  if (ov) ov.style.display = "none";
}

// ── Active APIs popup (folds in the old LOC tab) ──────────────────────
function adminOpenApis() {
  const ov = document.getElementById("admin-apis-overlay");
  if (ov) ov.style.display = "flex";
  if (!adminOpenApis._loaded) { adminOpenApis._loaded = true; loadApiHealth(); }
}
function adminCloseApis() {
  const ov = document.getElementById("admin-apis-overlay");
  if (ov) ov.style.display = "none";
}
// Toggle + lazy-load a job's recent run history (job_runs audit).
async function _adminToggleJobHistory(jobKey, linkEl) {
  const box = linkEl?.parentElement?.querySelector(`.admin-job-hist[data-job="${jobKey}"]`);
  if (!box) return;
  if (box.style.display !== "none") { box.style.display = "none"; return; }
  box.style.display = "";
  if (box.dataset.loaded === "1") return;
  box.textContent = "Loading…";
  try {
    const r = await apiFetch(`/api/admin/job-runs?job=${encodeURIComponent(jobKey)}&limit=15`);
    if (!r.ok) { box.textContent = "Could not load history."; return; }
    const { runs } = await r.json();
    if (!Array.isArray(runs) || !runs.length) { box.textContent = "No runs recorded yet."; return; }
    const esc = escHtml;   // canonical escaper (shared.js) — escapes & < > " '
    box.innerHTML = `<ul style="list-style:none;margin:0;padding:0;font-size:0.74rem;color:var(--muted)">` +
      runs.map(x => {
        const when = x.ended_at || x.started_at;
        const col = x.status === "error" ? "#e0564f" : x.status === "running" ? "#e6c14b" : "#6ddf70";
        const dur = (x.started_at && x.ended_at)
          ? ` · ${Math.max(0, Math.round((Date.parse(x.ended_at) - Date.parse(x.started_at)) / 1000))}s` : "";
        return `<li style="padding:0.12rem 0;border-bottom:1px solid var(--border)">
          <span style="color:${col};font-weight:600;text-transform:uppercase;font-size:0.68rem">${esc(x.status)}</span>
          ${esc(new Date(when).toLocaleString())}${dur}
          ${x.items ? ` · ${esc(x.items)} items` : ""}${x.errors ? ` · ${esc(x.errors)} err` : ""}
          ${x.detail ? `<br><span style="opacity:0.8">${esc(x.detail)}</span>` : ""}
        </li>`;
      }).join("") + `</ul>`;
    box.dataset.loaded = "1";
  } catch (e) {
    box.textContent = "Could not load history: " + (e?.message || e);
  }
}
window._adminToggleJobHistory = _adminToggleJobHistory;

async function loadApiHealth() {
  const el = document.getElementById("admin-api-health");
  if (!el) return;
  const hours = document.getElementById("api-health-hours")?.value || "24";
  el.innerHTML = "Loading…";
  try {
    const [hr, lr] = await Promise.all([
      apiFetch(`/api/admin/api-health?hours=${hours}`).then(r => r.ok ? r.json() : null).catch(() => null),
      apiFetch(`/api/admin/loc-stats`).then(r => r.ok ? r.json() : null).catch(() => null),
    ]);
    const esc = escHtml;   // canonical escaper (shared.js) — escapes & < > " '
    let html = "";
    const svcs = Array.isArray(hr?.services) ? hr.services : [];
    if (svcs.length) {
      html += `<table class="admin-api-tbl"><thead><tr>
        <th>Service</th><th>Reqs</th><th>OK %</th><th>p50</th><th>p95</th><th>Errors</th><th>Last error</th></tr></thead><tbody>`;
      html += svcs.map(s => {
        const okPct = s.total ? Math.round((s.successes / s.total) * 100) : 0;
        const okCls = okPct >= 99 ? "ok" : okPct >= 90 ? "warn" : "bad";
        const lastErr = s.last_error
          ? `<span title="${esc(s.last_error_at || "")}">${esc(String(s.last_error).slice(0, 80))}</span>`
          : "<span style='color:var(--muted)'>—</span>";
        return `<tr>
          <td>${esc(s.service)}</td>
          <td>${s.total}</td>
          <td class="admin-api-${okCls}">${okPct}%</td>
          <td>${s.p50_ms ?? "–"}ms</td>
          <td>${s.p95_ms ?? "–"}ms</td>
          <td>${s.failures || 0}</td>
          <td>${lastErr}</td></tr>`;
      }).join("");
      html += `</tbody></table>`;
    } else {
      html += `<p style="color:var(--muted)">No API activity in this window.</p>`;
    }
    // Background-job health (label → job_runs key for the history link)
    const _jobKey = { "Cache-warm worker": "cache-warm", "Daily suggestions": "daily-suggestions", "Archive refresh": "archive-refresh" };
    if (hr?.jobs) {
      html += `<h3 style="margin:1rem 0 0.4rem;font-size:0.85rem;color:var(--fg)">Background jobs</h3><ul class="admin-job-list">`;
      for (const [k, j] of Object.entries(hr.jobs)) {
        const key = _jobKey[k];
        const histLink = key
          ? ` <a href="#" onclick="event.preventDefault();_adminToggleJobHistory('${key}',this)" style="font-size:0.72rem;color:#7eb8da;text-decoration:none">history</a><div class="admin-job-hist" data-job="${key}" style="display:none;margin:0.3rem 0 0.4rem 0.6rem"></div>`
          : "";
        html += `<li><strong>${esc(k)}</strong>: ${esc(j)}${histLink}</li>`;
      }
      html += `</ul>`;
    }
    // Rate-limit headroom
    if (hr?.limiters || hr?.discogs) {
      html += `<h3 style="margin:1rem 0 0.4rem;font-size:0.85rem;color:var(--fg)">Rate-limit headroom</h3><ul class="admin-job-list">`;
      const loc = hr?.limiters?.loc;
      if (loc) {
        html += `<li><strong>LOC limiter</strong>: ${esc(loc.inWindow)}/${esc(loc.max)} used in window · queued ${esc(loc.queued)}/${esc(loc.maxQueueDepth)}</li>`;
      }
      if (hr?.discogs) {
        const d = hr.discogs;
        const lowCls = d.headroom <= 5 ? ' style="color:#e0564f"' : d.headroom <= 15 ? ' style="color:#e6c14b"' : '';
        html += `<li><strong>Discogs</strong> (approx., ${esc(d.ceilingPerMin)}/min cap): ${esc(d.lastMinute)} reqs last 60s · <span${lowCls}>~${esc(d.headroom)} headroom</span> · ${esc(d.last24h)} in 24h</li>`;
      }
      html += `</ul>`;
    }
    // LOC proxy (was its own tab)
    if (lr && typeof lr === "object") {
      const s = lr.stats || lr;
      html += `<h3 style="margin:1rem 0 0.4rem;font-size:0.85rem;color:var(--fg)">Library of Congress proxy</h3>
        <ul class="admin-job-list">
          <li>Cache: ${esc(s.cacheHits ?? "?")} hits / ${esc(s.cacheMisses ?? "?")} misses</li>
          <li>Failures: ${esc(s.failures ?? "?")} · rate-limit hits: ${esc(s.rateLimitHits ?? "?")}</li>
          ${s.lastFailureMsg ? `<li>Last failure: ${esc(s.lastFailureMsg)}</li>` : ""}
        </ul>`;
    }
    el.innerHTML = html;
  } catch (e) {
    el.innerHTML = `<p style="color:#e88">Failed to load API health: ${String(e?.message || e)}</p>`;
  }
}

// ── Overview KPIs + per-user stats box ────────────────────────────────
function _kpiCard(label, value, sub, title) {
  const t = title ? ` title="${String(title).replace(/"/g, "&quot;")}"` : "";
  return `<div class="admin-kpi"${t}><div class="admin-kpi-val">${value}</div>
    <div class="admin-kpi-label">${label}</div>
    ${sub ? `<div class="admin-kpi-sub">${sub}</div>` : ""}</div>`;
}
async function loadAdminOverview() {
  const el = document.getElementById("admin-overview-kpis");
  if (!el) return;
  try {
    const r = await apiFetch("/api/admin/overview");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    el.innerHTML = [
      _kpiCard("Clerk users", d.clerkUsers ?? "–", "total signed-up accounts", "All Clerk accounts, including users who never connected Discogs"),
      _kpiCard("Discogs-connected", d.totalUsers ?? "–", "have linked Discogs", "Users with a Discogs OAuth connection (a row in user_tokens)"),
      _kpiCard("New (7d)", d.newUsers7d ?? "–", d.sinceLabel7d || "", "Discogs-connected users created in the last 7 days"),
      _kpiCard("New (30d)", d.newUsers30d ?? "–", "", "Users created in the last 30 days"),
      _kpiCard("DAU", d.dau ?? "–", "active 24h"),
      _kpiCard("WAU", d.wau ?? "–", "active 7d"),
      _kpiCard("MAU", d.mau ?? "–", "active 30d"),
      _kpiCard("Plays 24h", d.plays24h ?? "–", `${d.plays7d ?? "–"} in 7d`),
      _kpiCard("Searches 24h", d.searches24h ?? "–", `${d.searches7d ?? "–"} in 7d`),
      _kpiCard("Album opens 24h", d.albumOpens24h ?? "–", `${d.albumOpens7d ?? "–"} in 7d`),
    ].join("");
  } catch (e) {
    el.innerHTML = `<p style="color:#e88">Failed: ${String(e?.message || e)}</p>`;
  }
}
async function loadAdminUserStats() {
  const el = document.getElementById("admin-user-stats");
  if (!el) return;
  try {
    const r = await apiFetch("/api/admin/overview");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    el.innerHTML = [
      _kpiCard("Clerk users", d.clerkUsers ?? "–", "signed-up accounts", "All Clerk accounts, including users who never connected Discogs"),
      _kpiCard("Discogs-connected", d.totalUsers ?? "–", "linked Discogs", "Users with a Discogs OAuth connection"),
      _kpiCard("New users", d.newUsers7d ?? 0, d.sinceLabel7d || "since 7d ago", "New Discogs-connected users in the last 7 days"),
      _kpiCard("New (30d)", d.newUsers30d ?? 0, ""),
      _kpiCard("Active 24h", d.dau ?? "–", ""),
      _kpiCard("Active 7d", d.wau ?? "–", ""),
      _kpiCard("Active 30d", d.mau ?? "–", ""),
    ].join("");
  } catch (e) {
    el.innerHTML = `<p style="color:#e88">Failed: ${String(e?.message || e)}</p>`;
  }
}

// ── Media player & usage ──────────────────────────────────────────────
var _adminTopPlayedLimit = 10;
function _adminSetTopPlayed(n) { _adminTopPlayedLimit = n; loadAdminMediaStats(); }
async function loadAdminMediaStats() {
  const el = document.getElementById("admin-media-stats");
  if (!el) return;
  try {
    const r = await apiFetch(`/api/admin/media-stats?topLimit=${_adminTopPlayedLimit}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    const esc = escHtml;   // canonical escaper (shared.js) — escapes & < > " '
    const srcLabel = s => ({ yt: "YouTube", loc: "Library of Congress", archive: "Archive.org" }[s] || s);
    const trackUrl = (src, id) => {
      if (!id) return "";
      if (src === "yt") return `https://www.youtube.com/watch?v=${encodeURIComponent(id)}`;
      if (src === "archive") return `https://archive.org/details/${encodeURIComponent(id)}`;
      if (src === "loc") return `https://www.loc.gov/item/${encodeURIComponent(id)}/`;
      return "";
    };
    let html = `<div class="admin-kpi-grid">` + [
      _kpiCard("Plays 24h", d.plays24h ?? 0, `${d.plays7d ?? 0} · 7d`),
      _kpiCard("Plays 30d", d.plays30d ?? 0, `${d.listeners30d ?? 0} listeners`),
      _kpiCard("Playlists", d.totalPlaylists ?? 0, `${d.usersWithPlaylists ?? 0} users · avg ${d.avgPlaylistLen ?? 0} tracks`),
      _kpiCard("Live queues", d.usersWithQueue ?? 0, `${d.queueRows ?? 0} queued rows`),
    ].join("") + `</div>`;
    const bs = Array.isArray(d.bySource7d) ? d.bySource7d : [];
    if (bs.length) {
      html += `<h3 style="margin:0.8rem 0 0.3rem;font-size:0.82rem;color:var(--fg)">Plays by source (7d)</h3>
        <ul class="admin-job-list">${bs.map(x => `<li>${esc(srcLabel(x.source))}: <strong>${esc(x.n)}</strong></li>`).join("")}</ul>`;
    }
    const tt = Array.isArray(d.topTitles30d) ? d.topTitles30d : [];
    if (tt.length) {
      const expand = _adminTopPlayedLimit <= 10
        ? `<a href="#" onclick="_adminSetTopPlayed(100);return false" style="color:var(--accent);font-size:0.74rem;margin-left:0.5rem">show top 100</a>`
        : `<a href="#" onclick="_adminSetTopPlayed(10);return false" style="color:var(--accent);font-size:0.74rem;margin-left:0.5rem">show top 10</a>`;
      html += `<h3 style="margin:0.8rem 0 0.3rem;font-size:0.82rem;color:var(--fg)">Most played (30d)${expand}</h3>
        <ol class="admin-job-list" style="padding-left:1.4rem">${tt.map(x => {
          const u = trackUrl(x.source, x.external_id);
          const t = u ? `<a href="${u}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">${esc(x.title)}</a>` : esc(x.title);
          return `<li>${t} <span style="color:var(--muted)">(${esc(srcLabel(x.source))}) · ${esc(x.n)}×</span></li>`;
        }).join("")}</ol>`;
    }
    const fu = Array.isArray(d.feature7d) ? d.feature7d : [];
    if (fu.length) {
      html += `<h3 style="margin:0.8rem 0 0.3rem;font-size:0.82rem;color:var(--fg)">Feature activity (7d, upstream calls)</h3>
        <ul class="admin-job-list">${fu.map(x => `<li>${esc(x.service)}: <strong>${esc(x.requests)}</strong></li>`).join("")}</ul>`;
    }
    el.innerHTML = html;
  } catch (e) {
    el.innerHTML = `<p style="color:#e88">Failed: ${String(e?.message || e)}</p>`;
  }
}

// ── Collection Stats ──────────────────────────────────────────────────
async function loadCollectionStats() {
  const el = document.getElementById("collection-stats");
  try {
    const data = await apiFetch("/api/admin/collection-stats").then(r => r.json());
    if (data.error) { el.textContent = "Could not load."; return; }

    const g = data.global;
    const globalHtml = `<div style="display:flex;flex-wrap:wrap;gap:0.8rem 2rem;margin-bottom:1rem;font-size:0.82rem">
      <div><span style="color:var(--accent);font-weight:700;font-size:1.1rem">${(g.total_collection ?? 0).toLocaleString()}</span> <span style="color:#8a7d6b">total collection items</span></div>
      <div><span style="color:var(--accent);font-weight:700;font-size:1.1rem">${(g.total_wantlist ?? 0).toLocaleString()}</span> <span style="color:#8a7d6b">total wantlist items</span></div>
      <div><span style="color:var(--accent);font-weight:700;font-size:1.1rem">${(g.total_inventory ?? 0).toLocaleString()}</span> <span style="color:#8a7d6b">inventory items</span></div>
      <div><span style="color:var(--accent);font-weight:700;font-size:1.1rem">${(g.total_orders ?? 0).toLocaleString()}</span> <span style="color:#8a7d6b">seller orders</span></div>
      <div><span style="color:var(--accent);font-weight:700;font-size:1.1rem">${(g.total_list_items ?? 0).toLocaleString()}</span> <span style="color:#8a7d6b">list items</span></div>
      <div><span style="color:var(--text);font-weight:600">${(g.unique_releases ?? 0).toLocaleString()}</span> <span style="color:#8a7d6b">unique releases</span></div>
      <div><span style="color:var(--text);font-weight:600">${(g.unique_wants ?? 0).toLocaleString()}</span> <span style="color:#8a7d6b">unique wants</span></div>
    </div>`;

    const fmtDate = d => d ? new Date(d).toLocaleDateString("en-US", { month: "short", year: "numeric" }) : "\u2014";
    const users = data.users ?? [];

    // Table 1: per-user counts (collection / wantlist / inventory / orders / lists)
    const countRows = users.map(u => `<tr>
      <td style="color:var(--text);font-weight:600;white-space:nowrap">@${u.username}</td>
      <td style="text-align:right;color:var(--accent);font-weight:600">${(u.collection_count ?? 0).toLocaleString()}</td>
      <td style="text-align:right">${(u.wantlist_count ?? 0).toLocaleString()}</td>
      <td style="text-align:right">${(u.inventory_count ?? 0).toLocaleString()}</td>
      <td style="text-align:right">${(u.orders_count ?? 0).toLocaleString()}</td>
      <td style="text-align:right">${(u.list_count ?? 0).toLocaleString()} <span style="color:#555;font-size:0.68rem">(${(u.list_item_count ?? 0).toLocaleString()})</span></td>
    </tr>`).join("");

    // Table 2: per-user taste (date range / top genres / top styles). More horizontal room.
    const tasteRows = users.map(u => {
      const genres = (u.top_genres ?? []).slice(0, 6).join(", ");
      const styles = (u.top_styles ?? []).slice(0, 6).join(", ");
      return `<tr>
        <td style="color:var(--text);font-weight:600;white-space:nowrap">@${u.username}</td>
        <td style="white-space:nowrap">${fmtDate(u.coll_oldest)} \u2013 ${fmtDate(u.coll_newest)}</td>
        <td style="font-size:0.74rem;color:#9a8a70" title="${genres}">${genres || "\u2014"}</td>
        <td style="font-size:0.74rem;color:#9a8a70" title="${styles}">${styles || "\u2014"}</td>
      </tr>`;
    }).join("");

    el.innerHTML = globalHtml + `
      <div style="font-size:0.7rem;color:#8a7d6b;text-transform:uppercase;letter-spacing:0.05em;margin:0.4rem 0 0.3rem">Per-user counts</div>
      <table class="api-log-table admin-stats-counts" style="font-size:0.78rem">
        <thead><tr>
          <th>User</th>
          <th style="text-align:right">Collection</th>
          <th style="text-align:right">Wantlist</th>
          <th style="text-align:right">Inventory</th>
          <th style="text-align:right">Orders</th>
          <th style="text-align:right">Lists</th>
        </tr></thead>
        <tbody>${countRows}</tbody>
      </table>
      <div style="font-size:0.7rem;color:#8a7d6b;text-transform:uppercase;letter-spacing:0.05em;margin:1rem 0 0.3rem">Per-user taste</div>
      <table class="api-log-table admin-stats-taste" style="font-size:0.78rem">
        <thead><tr>
          <th>User</th>
          <th>Date Range</th>
          <th>Top Genres</th>
          <th>Top Styles</th>
        </tr></thead>
        <tbody>${tasteRows}</tbody>
      </table>`;
  } catch { el.textContent = "Could not load collection stats."; }
}

// Standalone /admin only. shared.js is deferred there, so wait for
// DOMContentLoaded before calling initAuth (which lives in shared.js).
// On the inline surface Clerk is already resolved by app.js and the
// entry point is window._adminInlineOpen instead.
if (_ADMIN_STANDALONE) {
  document.addEventListener("DOMContentLoaded", () => initAuth({
    onSignedIn: verifyAdmin,
    onSignedOut: showDenied,
    onError: (msg) => {
      document.getElementById("admin-loading-section").innerHTML =
        `<p style="color:#e88">Failed to load auth: ${escHtml(msg)}</p>`;
    },
  }));
}

// ── User items popup (collection/wantlist viewer) ────────────────────
let _adminItemsUser = "";
let _adminItemsTab = "collection";
let _adminItemsPage = 1;

function openAdminItems(username, tab) {
  _adminItemsUser = username;
  _adminItemsTab = tab || "collection";
  _adminItemsPage = 1;
  const overlay = document.getElementById("admin-items-overlay");
  overlay.style.display = "flex";
  updateAdminItemsTabs();
  loadAdminItems();
}

function closeAdminItems() {
  document.getElementById("admin-items-overlay").style.display = "none";
}

function adminItemsTab(tab) {
  _adminItemsTab = tab;
  _adminItemsPage = 1;
  updateAdminItemsTabs();
  loadAdminItems();
}

function updateAdminItemsTabs() {
  const tabs = {
    collection: document.getElementById("admin-items-tab-col"),
    wantlist: document.getElementById("admin-items-tab-want"),
    favorites: document.getElementById("admin-items-tab-fav"),
  };
  for (const [key, btn] of Object.entries(tabs)) {
    if (!btn) continue;
    if (key === _adminItemsTab) {
      btn.style.background = "rgba(255,255,255,0.08)"; btn.style.color = "var(--fg)"; btn.style.borderColor = "#444";
    } else {
      btn.style.background = "transparent"; btn.style.color = "var(--muted)"; btn.style.borderColor = "#333";
    }
  }
}

async function loadAdminItems() {
  const list = document.getElementById("admin-items-list");
  const pag = document.getElementById("admin-items-pag");
  const titleEl = document.getElementById("admin-items-title");
  const countEl = document.getElementById("admin-items-count");
  const tabLabel = _adminItemsTab === "favorites" ? "Favorites" : _adminItemsTab === "wantlist" ? "Wantlist" : "Collection";
  titleEl.textContent = `@${_adminItemsUser} \u2014 ${tabLabel}`;
  countEl.textContent = "";
  list.innerHTML = '<div style="color:var(--muted);padding:1rem;text-align:center">Loading\u2026</div>';
  pag.style.display = "none";
  try {
    if (_adminItemsTab === "favorites") {
      const params = new URLSearchParams({ username: _adminItemsUser });
      const r = await apiFetch(`/api/admin/user-favorites?${params}`);
      if (!r.ok) { list.innerHTML = '<div style="color:#e88;padding:1rem">Failed to load.</div>'; return; }
      const data = await r.json();
      countEl.textContent = `${data.total} items`;
      if (!data.items?.length) { list.innerHTML = '<div style="color:var(--muted);padding:1rem;text-align:center">No favorites.</div>'; return; }
      list.innerHTML = data.items.map(row => {
        const d = row.data || {};
        const title = d.title || "";
        const thumb = d.cover_image || "";
        const type = d.type || row.entity_type || "";
        const year = d.year || "";
        const genre = (d.genre || []).slice(0, 2).join(", ");
        const label = (d.label || []).slice(0, 2).join(", ");
        const format = (d.format || []).slice(0, 2).join(", ");
        const country = d.country || "";
        const added = row.created_at ? new Date(row.created_at).toLocaleDateString() : "";
        const meta = [type, year, country, label, genre, format].filter(Boolean).map(s => escHtml(String(s))).join(' <span style="color:#444">\u00b7</span> ');
        const addedTag = added ? ` <span style="color:#555;font-size:0.7rem">${added}</span>` : "";
        const id = d.id || row.discogs_id || "";
        const href = type === "artist" ? `https://www.discogs.com/artist/${id}`
          : type === "label" ? `https://www.discogs.com/label/${id}`
          : `https://www.discogs.com/release/${id}`;
        return `<a href="${href}" target="_blank" rel="noopener" class="admin-item-row">
          ${thumb ? `<img src="${thumb}" class="admin-item-thumb" loading="lazy" decoding="async" />` : `<div class="admin-item-thumb"></div>`}
          <div style="flex:1;min-width:0;overflow:hidden">
            <div class="admin-item-title">${escHtml(title)}${addedTag}</div>
            <div class="admin-item-sub">${meta}</div>
          </div>
        </a>`;
      }).join("");
      return;
    }
    const params = new URLSearchParams({ username: _adminItemsUser, tab: _adminItemsTab, page: _adminItemsPage, per_page: 50 });
    const r = await apiFetch(`/api/admin/user-items?${params}`);
    if (!r.ok) { list.innerHTML = '<div style="color:#e88;padding:1rem">Failed to load.</div>'; return; }
    const data = await r.json();
    countEl.textContent = `${data.total.toLocaleString()} items`;
    if (!data.items?.length) { list.innerHTML = '<div style="color:var(--muted);padding:1rem;text-align:center">No items.</div>'; return; }
    list.innerHTML = data.items.map(item => {
      const title = item.title || item.basic_information?.title || "";
      const artist = item.artists?.[0]?.name || item.basic_information?.artists?.[0]?.name || "";
      const year = item.year || item.basic_information?.year || "";
      const thumb = item.thumb || item.basic_information?.thumb || "";
      const rating = item._rating || 0;
      const id = item.id || item.basic_information?.id || "";
      const labels = (item.labels || item.basic_information?.labels || []).map(l => l.name).slice(0, 2).join(", ");
      const formats = (item.formats || item.basic_information?.formats || []).map(f => f.name).slice(0, 2).join(", ");
      const stars = rating > 0 ? " " + "\u2605".repeat(rating) + "\u2606".repeat(5 - rating) : "";
      const meta = [artist, year, labels, formats].filter(Boolean).map(s => escHtml(String(s))).join(' <span style="color:#444">\u00b7</span> ');
      return `<a href="https://www.discogs.com/release/${id}" target="_blank" rel="noopener" class="admin-item-row">
        ${thumb ? `<img src="${thumb}" class="admin-item-thumb" loading="lazy" decoding="async" />` : `<div class="admin-item-thumb"></div>`}
        <div style="flex:1;min-width:0;overflow:hidden">
          <div class="admin-item-title">${escHtml(title)}</div>
          <div class="admin-item-sub">${meta}${stars ? `<span class="admin-item-rating">${stars}</span>` : ""}</div>
        </div>
      </a>`;
    }).join("");
    // Pagination
    if (data.pages > 1) {
      let pagHtml = "";
      if (_adminItemsPage > 1) pagHtml += `<a href="#" onclick="event.preventDefault();_adminItemsPage--;loadAdminItems()" style="color:var(--accent);text-decoration:none;margin:0 0.3rem">\u2190 Prev</a>`;
      pagHtml += `<span style="color:var(--muted)">Page ${_adminItemsPage} of ${data.pages}</span>`;
      if (_adminItemsPage < data.pages) pagHtml += `<a href="#" onclick="event.preventDefault();_adminItemsPage++;loadAdminItems()" style="color:var(--accent);text-decoration:none;margin:0 0.3rem">Next \u2192</a>`;
      pag.innerHTML = pagHtml;
      pag.style.display = "block";
    }
  } catch { list.innerHTML = '<div style="color:#e88;padding:1rem">Error loading items.</div>'; }
}

// Blues DB + shared admin sort helpers + Lyrics admin code moved to /blues-admin.js
// ── Admin: Submitted Tracks (track YT overrides) ──────────────────────
// Cache the raw submission rows so the filter input can re-render
// without a server round-trip.
let _adminSubmissionsRows = [];

async function loadAdminSubmissions() {
  const el = document.getElementById("submissions-list");
  if (!el) return;
  el.textContent = "Loading…";
  try {
    const r = await apiFetch("/api/admin/track-yt");
    if (!r.ok) { el.textContent = "Could not load submissions."; return; }
    const j = await r.json();
    _adminSubmissionsRows = Array.isArray(j?.overrides) ? j.overrides : [];
    _renderAdminSubmissionsTable();
  } catch (e) {
    el.textContent = "Could not load submissions: " + (e?.message || e);
  }
}
window.loadAdminSubmissions = loadAdminSubmissions;

// Render the table from _adminSubmissionsRows, applying the current
// filter input. Splitting render from fetch lets the filter input
// re-paint without re-querying the server.
function _renderAdminSubmissionsTable() {
  const el = document.getElementById("submissions-list");
  const countEl = document.getElementById("submissions-count");
  if (!el) return;
  const filter = (document.getElementById("submissions-filter")?.value || "").trim().toLowerCase();
  const rows = _adminSubmissionsRows;
  const filtered = filter
    ? rows.filter(o => {
        const hay = [
          o.release_id, o.release_type, o.track_position,
          o.track_title, o.video_id, o.video_title, o.submitted_by,
        ].filter(Boolean).join(" ").toLowerCase();
        return hay.includes(filter);
      })
    : rows;
  if (countEl) {
    if (filter && filtered.length !== rows.length) {
      countEl.textContent = `${filtered.length} of ${rows.length} ${rows.length === 1 ? "row" : "rows"}`;
    } else {
      countEl.textContent = `${rows.length} ${rows.length === 1 ? "row" : "rows"}`;
    }
  }
  if (!rows.length) {
    el.innerHTML = `<div style="color:var(--muted);padding:1rem;text-align:center">No submissions yet.</div>`;
    return;
  }
  if (!filtered.length) {
    el.innerHTML = `<div style="color:var(--muted);padding:1rem;text-align:center">No submissions match.</div>`;
    return;
  }
  const fmtDate = (d) => d ? new Date(d).toLocaleString() : "—";
  const trim = (s, n) => { s = String(s || ""); return s.length > n ? s.slice(0, n - 1) + "…" : s; };
  const sorted = _adminSortApply(filtered, _adminSubSortState, {
    submitted_at: "date", release_id: "num",
  });
  const S = _adminSubSortState;
  const head = `<thead style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em;color:var(--muted);text-align:left">
    <tr>
      ${_adminSortTh("When", "submitted_at", S, "_adminSubSort")}
      ${_adminSortTh("Scope", "release_type", S, "_adminSubSort")}
      ${_adminSortTh("Album", "release_id", S, "_adminSubSort")}
      ${_adminSortTh("Pos", "track_position", S, "_adminSubSort")}
      ${_adminSortTh("Track", "track_title", S, "_adminSubSort")}
      ${_adminSortTh("Video", "video_title", S, "_adminSubSort")}
      ${_adminSortTh("By", "submitted_by", S, "_adminSubSort")}
      <th></th>
    </tr></thead>`;
  const body = sorted.map(o => {
    const albumLink = _adminAlbumLink(o.release_type, o.release_id);
    const ytLink = `<a href="https://www.youtube.com/watch?v=${encodeURIComponent(o.video_id)}" target="_blank" rel="noopener" style="color:#7eb8da;text-decoration:none">${escHtml(trim(o.video_title || o.video_id, 40))}</a>`;
    return `<tr style="border-top:1px solid var(--border);font-size:0.78rem">
      <td style="padding:0.35rem 0.5rem;color:var(--muted);white-space:nowrap">${escHtml(fmtDate(o.submitted_at))}</td>
      <td style="padding:0.35rem 0.5rem">${escHtml(o.release_type)}</td>
      <td style="padding:0.35rem 0.5rem">${albumLink}</td>
      <td style="padding:0.35rem 0.5rem;font-family:monospace">${escHtml(o.track_position)}</td>
      <td style="padding:0.35rem 0.5rem">${escHtml(trim(o.track_title || "", 32))}</td>
      <td style="padding:0.35rem 0.5rem">${ytLink}</td>
      <td style="padding:0.35rem 0.5rem;color:var(--muted);font-family:monospace;font-size:0.72rem">${escHtml(trim(o.submitted_by, 12))}</td>
      <td style="padding:0.35rem 0.5rem"><button class="admin-btn admin-btn-danger" onclick="adminDeleteSubmission(this,'${escHtml(o.release_id)}','${escHtml(o.release_type)}','${escHtml(o.track_position).replace(/'/g, "\\'")}')">Delete</button></td>
    </tr>`;
  }).join("");
  el.innerHTML = `<div class="admin-grid-scroll"><table style="width:100%;border-collapse:collapse">${head}<tbody>${body}</tbody></table></div>`;
}

function _filterAdminSubmissions(_input) {
  _renderAdminSubmissionsTable();
}
window._filterAdminSubmissions = _filterAdminSubmissions;

// ── Admin: Unavailable YouTube videos ─────────────────────────────────
let _adminUnavailableRows = [];

async function loadAdminUnavailable() {
  const el = document.getElementById("unavailable-list");
  if (!el) return;
  el.textContent = "Loading…";
  try {
    const r = await apiFetch("/api/admin/youtube-unavailable");
    if (!r.ok) { el.textContent = "Could not load."; return; }
    const j = await r.json();
    _adminUnavailableRows = Array.isArray(j?.entries) ? j.entries : [];
    _renderAdminUnavailableTable();
  } catch (e) {
    el.textContent = "Could not load: " + (e?.message || e);
  }
}
window.loadAdminUnavailable = loadAdminUnavailable;

function _renderAdminUnavailableTable() {
  const el = document.getElementById("unavailable-list");
  const countEl = document.getElementById("unavailable-count");
  if (!el) return;
  const filter = (document.getElementById("unavailable-filter")?.value || "").trim().toLowerCase();
  const rows = _adminUnavailableRows;
  const filtered = filter
    ? rows.filter(o => {
        const hay = [
          o.video_id, o.status, String(o.report_count),
          o.sample_user_id, String(o.sample_error_code ?? ""),
          o.release_id, o.release_type, o.track_title,
        ].filter(Boolean).join(" ").toLowerCase();
        return hay.includes(filter);
      })
    : rows;
  if (countEl) {
    if (filter && filtered.length !== rows.length) {
      countEl.textContent = `${filtered.length} of ${rows.length} ${rows.length === 1 ? "row" : "rows"}`;
    } else {
      countEl.textContent = `${rows.length} ${rows.length === 1 ? "row" : "rows"}`;
    }
  }
  if (!rows.length) {
    el.innerHTML = `<div style="color:var(--muted);padding:1rem;text-align:center">No unavailable videos reported.</div>`;
    return;
  }
  if (!filtered.length) {
    el.innerHTML = `<div style="color:var(--muted);padding:1rem;text-align:center">No matches.</div>`;
    return;
  }
  const fmtDate = (d) => d ? new Date(d).toLocaleString() : "—";
  const trim = (s, n) => { s = String(s || ""); return s.length > n ? s.slice(0, n - 1) + "…" : s; };
  const sorted = _adminSortApply(filtered, _adminUnavSortState, {
    report_count: "num", first_reported_at: "date", last_reported_at: "date", sample_error_code: "num",
  });
  const U = _adminUnavSortState;
  const head = `<thead style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em;color:var(--muted);text-align:left">
    <tr>
      ${_adminSortTh("Status", "status", U, "_adminUnavSort")}
      ${_adminSortTh("Video", "video_id", U, "_adminUnavSort")}
      ${_adminSortTh("Album", "release_id", U, "_adminUnavSort")}
      ${_adminSortTh("Reports", "report_count", U, "_adminUnavSort", "text-align:right")}
      ${_adminSortTh("First", "first_reported_at", U, "_adminUnavSort")}
      ${_adminSortTh("Last", "last_reported_at", U, "_adminUnavSort")}
      ${_adminSortTh("Code", "sample_error_code", U, "_adminUnavSort")}
      ${_adminSortTh("Reporter", "sample_user_id", U, "_adminUnavSort")}
      <th></th>
    </tr></thead>`;
  const body = sorted.map(o => {
    const ytLink = `<a href="https://www.youtube.com/watch?v=${encodeURIComponent(o.video_id)}" target="_blank" rel="noopener" style="color:#7eb8da;text-decoration:none;font-family:monospace">${escHtml(o.video_id)}</a>`;
    // Prefer the real album title (+ first artist) as the visible
    // link text so you can spot which album is affected at a glance.
    // Falls back to the release_type/release_id pattern when the
    // Discogs row hasn't been cached yet.
    let albumLabel = "";
    if (o.album_title) {
      const artist = o.album_artist ? `${o.album_artist} — ` : "";
      albumLabel = `${artist}${o.album_title}`;
    } else if (o.release_id) {
      albumLabel = `${o.release_type}/${o.release_id}`;
    }
    const albumCell = _adminAlbumLink(o.release_type, o.release_id, albumLabel);
    // Extra sub-line under the album title: which track + position is
    // the one whose video is broken. Empty when we have no override
    // mapping this videoId to a specific pressing.
    const trackSub = o.track_title
      ? `<div style="font-size:0.7rem;color:var(--muted);margin-top:0.15rem">Track: ${escHtml(o.track_title)}${o.track_position ? ` · pos ${escHtml(o.track_position)}` : ""}</div>`
      : "";
    const statusColor = o.status === "unavailable" ? "var(--danger)" : "var(--accent)";
    return `<tr style="border-top:1px solid var(--border);font-size:0.78rem">
      <td style="padding:0.35rem 0.5rem;font-weight:600;color:${statusColor};text-transform:uppercase;font-size:0.7rem;letter-spacing:0.04em">${escHtml(o.status)}</td>
      <td style="padding:0.35rem 0.5rem">${ytLink}</td>
      <td style="padding:0.35rem 0.5rem">${albumCell}${trackSub}</td>
      <td style="padding:0.35rem 0.5rem;text-align:right;font-weight:600">${o.report_count}</td>
      <td style="padding:0.35rem 0.5rem;color:var(--muted);white-space:nowrap">${escHtml(fmtDate(o.first_reported_at))}</td>
      <td style="padding:0.35rem 0.5rem;color:var(--muted);white-space:nowrap">${escHtml(fmtDate(o.last_reported_at))}</td>
      <td style="padding:0.35rem 0.5rem;font-family:monospace">${escHtml(String(o.sample_error_code ?? "—"))}</td>
      <td style="padding:0.35rem 0.5rem;color:var(--muted);font-family:monospace;font-size:0.72rem">${escHtml(trim(o.sample_user_id || "", 12))}</td>
      <td style="padding:0.35rem 0.5rem"><button class="admin-btn" onclick="adminClearUnavailable(this,'${escHtml(o.video_id)}')" title="Clear this entry — videoId starts fresh from count 1 next time it's reported">Clear</button></td>
    </tr>`;
  }).join("");
  el.innerHTML = `<div class="admin-grid-scroll"><table style="width:100%;border-collapse:collapse">${head}<tbody>${body}</tbody></table></div>`;
}

function _filterAdminUnavailable(_input) {
  _renderAdminUnavailableTable();
}
window._filterAdminUnavailable = _filterAdminUnavailable;

async function adminClearUnavailable(btn, videoId) {
  if (!confirm(`Clear ${videoId} from the unavailable list? (resets report count to 0)`)) return;
  btn.disabled = true;
  try {
    const r = await apiFetch(`/api/admin/youtube-unavailable/${encodeURIComponent(videoId)}`, {
      method: "DELETE",
    });
    if (!r.ok) throw new Error(`failed (${r.status})`);
    // Optimistic remove from local cache so the row vanishes without
    // a full refetch.
    _adminUnavailableRows = _adminUnavailableRows.filter(o => o.video_id !== videoId);
    _renderAdminUnavailableTable();
  } catch (e) {
    btn.disabled = false;
    alert("Clear failed: " + (e?.message || e));
  }
}
window.adminClearUnavailable = adminClearUnavailable;

async function adminDeleteSubmission(btn, releaseId, releaseType, trackPosition) {
  if (!confirm(`Delete submission for ${releaseType}/${releaseId} pos ${trackPosition}?`)) return;
  btn.disabled = true;
  try {
    const r = await apiFetch("/api/admin/track-yt", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ releaseId, releaseType, trackPosition }),
    });
    if (!r.ok) throw new Error(`failed (${r.status})`);
    loadAdminSubmissions();
  } catch (e) {
    btn.disabled = false;
    alert("Delete failed: " + (e?.message || e));
  }
}
window.adminDeleteSubmission = adminDeleteSubmission;

// ── Admin: Suggestions (background-generated personal feed) ───────────
async function loadAdminSuggestions() {
  const el = document.getElementById("suggestions-list");
  if (!el) return;
  el.textContent = "Loading…";
  try {
    const r = await apiFetch("/api/admin/suggestions-stats");
    if (!r.ok) { el.textContent = "Could not load suggestions stats."; return; }
    const j = await r.json();
    const rows = Array.isArray(j?.users) ? j.users : [];
    if (!rows.length) {
      el.innerHTML = `<div style="color:var(--muted);padding:1rem;text-align:center">No saved suggestions yet — wait for the next hourly run or click "Run for me" / "Run for everyone" above.</div>`;
      return;
    }
    const fmtDate = (d) => d ? new Date(d).toLocaleString() : "—";
    const head = `<thead style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em;color:var(--muted);text-align:left">
      <tr>
        <th style="padding:0.3rem 0.5rem">User</th>
        <th style="padding:0.3rem 0.5rem">Saved</th>
        <th style="padding:0.3rem 0.5rem">Last generated</th>
      </tr></thead>`;
    const body = rows.map(u => `<tr style="border-top:1px solid var(--border);font-size:0.78rem">
      <td style="padding:0.35rem 0.5rem">
        ${u.clerkUsername ? `<div style="font-weight:600">${escHtml(u.clerkUsername)}</div>` : ""}
        <div style="font-family:monospace;font-size:0.65rem;color:var(--muted)">${escHtml(u.clerkUserId)}</div>
      </td>
      <td style="padding:0.35rem 0.5rem;font-weight:600">${u.count}</td>
      <td style="padding:0.35rem 0.5rem;color:var(--muted)">${escHtml(fmtDate(u.lastGeneratedAt))}</td>
    </tr>`).join("");
    el.innerHTML = `<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse">${head}<tbody>${body}</tbody></table></div>`;
  } catch (e) {
    el.textContent = "Could not load suggestions stats: " + (e?.message || e);
  }
}
window.loadAdminSuggestions = loadAdminSuggestions;

// Sort state for the user-behavior table. Persisted only in memory.
// Defaults: signed_up_at desc (newest accounts first) — the question
// the admin most often asks ("who are my newest users?").
let _adminBehaviorSort = { col: "signed_up_at", dir: "desc" };
const _ADMIN_BEHAVIOR_NUMERIC_COLS = new Set([
  "favorites", "suggestions_pool", "suggestions_favorited",
  "album_clicks_total", "album_clicks_30d",
  "player_plays_total", "player_plays_30d",
  "searches_total",     "searches_30d",
]);
const _ADMIN_BEHAVIOR_DATE_COLS = new Set(["last_active", "signed_up_at"]);
// Read the effective "signed up" timestamp — Clerk's account
// creation when we have it, else the user_tokens.created_at proxy.
function _adminBehaviorSignedUp(u) {
  return u?.clerk_created_at || u?.signed_up_at || null;
}
function _adminBehaviorSortRows(rows) {
  const { col, dir } = _adminBehaviorSort;
  const mul = dir === "asc" ? 1 : -1;
  const isNum  = _ADMIN_BEHAVIOR_NUMERIC_COLS.has(col);
  const isDate = _ADMIN_BEHAVIOR_DATE_COLS.has(col);
  const readCol = (u) => col === "signed_up_at" ? _adminBehaviorSignedUp(u) : u?.[col];
  return rows.slice().sort((a, b) => {
    const av = readCol(a);
    const bv = readCol(b);
    const aMissing = av == null || av === "";
    const bMissing = bv == null || bv === "";
    // Always push missing to bottom regardless of direction so empty
    // cells don't crowd the top when sorting ascending.
    if (aMissing && !bMissing) return 1;
    if (!aMissing && bMissing) return -1;
    if (aMissing && bMissing) return 0;
    if (isDate) return (new Date(av).getTime() - new Date(bv).getTime()) * mul;
    if (isNum)  return (Number(av) - Number(bv)) * mul;
    return String(av).toLowerCase().localeCompare(String(bv).toLowerCase()) * mul;
  });
}
function _adminBehaviorSortBy(col) {
  if (_adminBehaviorSort.col === col) {
    _adminBehaviorSort.dir = _adminBehaviorSort.dir === "asc" ? "desc" : "asc";
  } else {
    _adminBehaviorSort.col = col;
    // Numeric / date columns default to desc; the username column to asc.
    _adminBehaviorSort.dir = (_ADMIN_BEHAVIOR_NUMERIC_COLS.has(col) || _ADMIN_BEHAVIOR_DATE_COLS.has(col)) ? "desc" : "asc";
  }
  loadAdminBehavior();
}
window._adminBehaviorSortBy = _adminBehaviorSortBy;

async function loadAdminBehavior() {
  const el = document.getElementById("behavior-list");
  if (!el) return;
  el.textContent = "Loading…";
  try {
    const r = await apiFetch("/api/admin/behavior-stats");
    if (!r.ok) { el.textContent = "Could not load behavior stats."; return; }
    const j = await r.json();
    let rows = Array.isArray(j?.items) ? j.items : [];
    if (!rows.length) {
      el.innerHTML = `<div style="color:var(--muted);padding:1rem;text-align:center">No users yet.</div>`;
      return;
    }
    rows = _adminBehaviorSortRows(rows);
    // Compact "3/17/26, 7:20p" formatter — saves a column-worth of
    // horizontal space vs toLocaleString's "3/17/2026, 7:20:05 PM".
    const fmtDate = (d) => {
      if (!d) return "—";
      const dt = new Date(d);
      if (isNaN(dt.getTime())) return "—";
      const M = dt.getMonth() + 1;
      const D = dt.getDate();
      const YY = String(dt.getFullYear()).slice(-2);
      let h = dt.getHours();
      const m = String(dt.getMinutes()).padStart(2, "0");
      const ampm = h >= 12 ? "p" : "a";
      h = h % 12; if (h === 0) h = 12;
      return `${M}/${D}/${YY} ${h}:${m}${ampm}`;
    };
    const fmt2 = (recent, total) => {
      const r = recent ?? 0; const t = total ?? 0;
      return `<span style="color:#fff">${r}</span><span style="color:var(--muted)"> / ${t}</span>`;
    };
    const th = (key, label, align = "left", extra = "") => {
      const isActive = _adminBehaviorSort.col === key;
      const arrow = isActive ? (_adminBehaviorSort.dir === "asc" ? " ↑" : " ↓") : "";
      return `<th style="padding:0.3rem 0.5rem;text-align:${align};cursor:pointer;user-select:none${isActive ? ';color:var(--text)' : ''}" onclick="_adminBehaviorSortBy('${key}')" title="Sort by ${label.replace(/<[^>]*>/g, '').trim()}">${label}${arrow}${extra}</th>`;
    };
    // Two-line column header so the "30d / total" pair reads
    // unambiguously without burning horizontal space.
    const head = `<thead style="font-size:0.7rem;text-transform:uppercase;letter-spacing:0.04em;color:var(--muted);text-align:left">
      <tr>
        ${th("username",              "User",         "left")}
        ${th("favorites",             "Favs",         "right")}
        ${th("suggestions_pool",      "Sugg pool",    "right")}
        ${th("suggestions_favorited", "Sugg fav'd",   "right")}
        ${th("album_clicks_total",    "Album clicks<br><span style=\"font-size:0.62rem;text-transform:none;letter-spacing:0\">30d / total</span>", "right")}
        ${th("player_plays_total",    "Plays<br><span style=\"font-size:0.62rem;text-transform:none;letter-spacing:0\">30d / total</span>",       "right")}
        ${th("searches_total",        "Searches<br><span style=\"font-size:0.62rem;text-transform:none;letter-spacing:0\">30d / total</span>",    "right")}
        ${th("signed_up_at",          "Signed up",    "left")}
        ${th("last_active",           "Last active",  "left")}
      </tr></thead>`;
    const body = rows.map(u => {
      // Display priority: Clerk username (fresh handle) → Discogs
      // username (their connected Discogs handle) → "—". When both
      // are present and differ, the secondary line shows the Discogs
      // one so the admin can disambiguate.
      const primary = u.clerk_username || u.username || "—";
      const secondary = (u.clerk_username && u.username && u.clerk_username !== u.username)
        ? `discogs: ${u.username}` : "";
      return `<tr style="border-top:1px solid var(--border);font-size:0.78rem">
      <td style="padding:0.35rem 0.5rem">
        <div style="font-weight:600">${escHtml(primary)}</div>
        ${secondary ? `<div style="font-size:0.65rem;color:var(--muted)">${escHtml(secondary)}</div>` : ""}
        <div style="font-family:monospace;font-size:0.65rem;color:var(--muted)">${escHtml(u.clerk_user_id)}</div>
      </td>
      <td style="padding:0.35rem 0.5rem;text-align:right;font-variant-numeric:tabular-nums">${u.favorites}</td>
      <td style="padding:0.35rem 0.5rem;text-align:right;font-variant-numeric:tabular-nums">${u.suggestions_pool}</td>
      <td style="padding:0.35rem 0.5rem;text-align:right;font-variant-numeric:tabular-nums">${u.suggestions_favorited}</td>
      <td style="padding:0.35rem 0.5rem;text-align:right;font-variant-numeric:tabular-nums">${fmt2(u.album_clicks_30d, u.album_clicks_total)}</td>
      <td style="padding:0.35rem 0.5rem;text-align:right;font-variant-numeric:tabular-nums">${fmt2(u.player_plays_30d, u.player_plays_total)}</td>
      <td style="padding:0.35rem 0.5rem;text-align:right;font-variant-numeric:tabular-nums">${fmt2(u.searches_30d, u.searches_total)}</td>
      <td style="padding:0.35rem 0.5rem;color:var(--muted);font-size:0.72rem" title="${u.clerk_created_at ? 'Clerk signup' : 'Discogs connected (Clerk timestamp not yet cached)'}">${escHtml(fmtDate(_adminBehaviorSignedUp(u)))}</td>
      <td style="padding:0.35rem 0.5rem;color:var(--muted);font-size:0.72rem">${escHtml(fmtDate(u.last_active))}</td>
    </tr>`;
    }).join("");
    el.innerHTML = `<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse">${head}<tbody>${body}</tbody></table></div>`;
  } catch (e) {
    el.textContent = "Could not load behavior stats: " + (e?.message || e);
  }
}
window.loadAdminBehavior = loadAdminBehavior;

function _adminSuggRenderLine(f) {
  // Shared formatter for the diagnostic counters line.
  if (f?.error) return "Failed: " + f.error;
  const d = f?.details;
  let line = `Saved ${f?.saved ?? 0} new${f?.reason ? ` (${f.reason})` : ""}.`;
  if (d) {
    line += ` tuples ${d.tuples}→${d.tuplesAfterAdminFilter} · raw ${d.rawResults}`
          + ` · candidates ${d.candidatesAfterDedup} · merge ${d.itemsToMerge}`
          + ` · excl owned ${d.excludedOwned}/dismissed ${d.excludedDismissed}/recent ${d.excludedRecentlyClicked}/genre ${d.excludedAdminGenre}`
          + ` · total ${d.totalSavedAfter}`;
  }
  return line;
}

async function adminSuggestionsRunSelf() {
  const status = document.getElementById("suggestions-action-status");
  const btn = document.getElementById("suggestions-run-self-btn");
  if (btn) btn.disabled = true;
  if (status) status.textContent = "Starting…";
  // Fire-and-forget: the server kicks the job into the background and
  // returns immediately so a Railway/CF edge timeout (~100s) can't
  // reset the connection during the ~2-minute admin search. Poll the
  // status endpoint until the result lands (or we time out locally).
  let startedAt = Date.now();
  try {
    const r = await apiFetch("/api/admin/run-suggestions-for-self", { method: "POST" });
    if (!r.ok) throw new Error(`failed (${r.status})`);
    const j = await r.json();
    if (j.status === "already-running") {
      if (status) status.textContent = "Already running — polling for result…";
    } else {
      startedAt = Number(j.startedAt) || startedAt;
      if (status) status.textContent = "Running in background — polling for result…";
    }
  } catch (e) {
    if (status) status.textContent = "Failed to start: " + (e?.message || e);
    if (btn) btn.disabled = false;
    return;
  }
  // Poll every 3s for up to 5 minutes. The run is recorded server-side
  // regardless; if the poll times out the suggestions table still
  // updates and the last result can be re-fetched.
  const BASE_POLL_MS = 5000, BACKOFF_POLL_MS = 15000, TIMEOUT_MS = 5 * 60 * 1000;
  const t0 = Date.now();
  let dots = 0;
  let nextWait = BASE_POLL_MS;
  try {
    while (Date.now() - t0 < TIMEOUT_MS) {
      await new Promise(res => setTimeout(res, nextWait));
      nextWait = BASE_POLL_MS;
      const sr = await apiFetch("/api/admin/run-suggestions-for-self/status");
      // Back off on 429 — admin dashboard's other polls share the cap.
      if (sr.status === 429) { nextWait = BACKOFF_POLL_MS; continue; }
      // Transient blip (network, Clerk token refresh) — just retry.
      if (!sr.ok) continue;
      const sj = await sr.json();
      const f = sj.last;
      // "Done" = a result exists AND it was recorded after we started.
      if (f && f.finishedAt >= startedAt) {
        if (status) status.textContent = _adminSuggRenderLine(f);
        console.log("[adminSuggestionsRunSelf]", f);
        loadAdminSuggestions();
        return;
      }
      if (status) status.textContent = "Running" + ".".repeat((dots++ % 3) + 1);
    }
    if (status) status.textContent = "Timed out polling — job may still be running. Refresh the suggestions table in a minute.";
  } finally {
    if (btn) btn.disabled = false;
  }
}
window.adminSuggestionsRunSelf = adminSuggestionsRunSelf;

async function adminSuggestionsRunAll() {
  if (!confirm("Run the suggestion generator for every user? This pulls Discogs for each user with OAuth and may take several minutes.")) return;
  const status = document.getElementById("suggestions-action-status");
  const btn = document.getElementById("suggestions-run-all-btn");
  if (btn) btn.disabled = true;
  if (status) status.textContent = "Kicked off in background…";
  try {
    const r = await apiFetch("/api/admin/run-suggestions", { method: "POST" });
    if (!r.ok) throw new Error(`failed (${r.status})`);
    if (status) status.textContent = "Background run started — refresh in a couple minutes.";
  } catch (e) {
    if (status) status.textContent = "Failed: " + (e?.message || e);
  } finally {
    if (btn) btn.disabled = false;
  }
}
window.adminSuggestionsRunAll = adminSuggestionsRunAll;

// ── Admin: clickable DB table summary popup ────────────────────────────
// Each table name in the Database tab is clickable; opens an overlay
// with schema, indexes, row count, on-disk size. Defensive: the
// /api/admin/db-table endpoint whitelists names against
// getTableRowCounts so we can pass the raw name.
async function adminOpenDbTablePopup(tableName) {
  const overlay = document.getElementById("admin-db-table-overlay") || (() => {
    const o = document.createElement("div");
    o.id = "admin-db-table-overlay";
    o.style.cssText = "position:fixed;inset:0;background:rgba(0,0,0,0.78);z-index:300;display:flex;align-items:center;justify-content:center;padding:2rem 1rem;overflow:auto";
    o.onclick = e => { if (e.target === o) o.remove(); };
    document.body.appendChild(o);
    return o;
  })();
  overlay.innerHTML = `<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(720px,100%);max-height:88vh;overflow:auto;box-shadow:0 8px 28px rgba(0,0,0,0.55), 0 1px 0 rgba(255,255,255,0.04) inset">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.7rem">
      <h3 style="margin:0;font-family:monospace;font-size:1rem">${escHtml(tableName)}</h3>
      <button class="admin-btn" onclick="document.getElementById('admin-db-table-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
    </div>
    <div id="admin-db-table-body" style="font-size:0.82rem;color:var(--muted)">Loading…</div>
  </div>`;
  try {
    const r = await apiFetch(`/api/admin/db-table/${encodeURIComponent(tableName)}`);
    if (!r.ok) throw new Error(`status ${r.status}`);
    const j = await r.json();
    const fmtBytes = (b) => {
      if (!b) return "—";
      const u = ["B", "KB", "MB", "GB"];
      let v = b, i = 0;
      while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
      return `${v.toFixed(v < 10 ? 1 : 0)} ${u[i]}`;
    };
    const body = document.getElementById("admin-db-table-body");
    if (!body) return;
    const colsHtml = j.columns.map(c =>
      `<tr style="border-top:1px solid var(--border)">
        <td style="padding:0.25rem 0.5rem;font-family:monospace;font-size:0.78rem">${escHtml(c.name)}</td>
        <td style="padding:0.25rem 0.5rem;font-family:monospace;font-size:0.78rem;color:var(--muted)">${escHtml(c.type)}</td>
        <td style="padding:0.25rem 0.5rem;font-size:0.72rem;color:${c.nullable ? "var(--muted)" : "var(--text)"}">${c.nullable ? "null" : "NOT NULL"}</td>
        <td style="padding:0.25rem 0.5rem;font-size:0.72rem;color:var(--muted);font-family:monospace">${escHtml(c.default || "")}</td>
      </tr>`
    ).join("");
    const idxHtml = j.indexes.length
      ? j.indexes.map(i => `<div style="padding:0.2rem 0;font-family:monospace;font-size:0.74rem;color:var(--muted)"><span style="color:var(--text)">${escHtml(i.name)}</span> — ${escHtml(i.definition)}</div>`).join("")
      : `<div style="color:var(--muted);font-style:italic">No indexes</div>`;
    body.innerHTML = `
      <div style="display:flex;gap:1.2rem;flex-wrap:wrap;margin-bottom:0.8rem">
        <div><span style="color:var(--muted);font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em">Rows</span><br><span style="font-size:1.1rem;font-weight:600">${j.rowCount.toLocaleString()}</span></div>
        <div><span style="color:var(--muted);font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em">On disk</span><br><span style="font-size:1.1rem;font-weight:600">${fmtBytes(j.totalSizeBytes)}</span></div>
        <div><span style="color:var(--muted);font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em">Columns</span><br><span style="font-size:1.1rem;font-weight:600">${j.columns.length}</span></div>
        <div><span style="color:var(--muted);font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em">Indexes</span><br><span style="font-size:1.1rem;font-weight:600">${j.indexes.length}</span></div>
      </div>
      <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em;color:var(--accent);margin:0.6rem 0 0.3rem">Schema</div>
      <table style="width:100%;border-collapse:collapse">${colsHtml}</table>
      <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em;color:var(--accent);margin:1rem 0 0.3rem">Indexes</div>
      ${idxHtml}`;
  } catch (e) {
    const body = document.getElementById("admin-db-table-body");
    if (body) body.innerHTML = `<div style="color:var(--danger)">Failed: ${escHtml(String(e?.message || e))}</div>`;
  }
}
window.adminOpenDbTablePopup = adminOpenDbTablePopup;


// ── Query tab: ad-hoc read-only SQL over the cache / blues tables ─────
// Run executes a read-only SELECT
// (server enforces READ ONLY txn + SELECT-only + timeout + row cap).
let _querySchemaLoaded = false;
async function loadQuerySchema() {
  if (_querySchemaLoaded) return;
  const el = document.getElementById("query-schema");
  if (!el) return;
  try {
    const r = await apiFetch("/api/admin/query/schema");
    if (!r.ok) { el.textContent = "Couldn't load schema."; return; }
    const { tables } = await r.json();
    const names = Object.keys(tables || {}).sort();
    if (!names.length) { el.textContent = "No tables."; return; }
    el.innerHTML = names.map(t => `
      <div style="margin-bottom:0.5rem">
        <span style="color:var(--accent);font-weight:600;font-family:ui-monospace,monospace">${escHtml(t)}</span>
        <div style="margin-left:0.8rem;line-height:1.5">${
          (tables[t] || []).map(c =>
            `<span title="${escHtml(c.type)}" style="display:inline-block;margin-right:0.6rem;font-family:ui-monospace,monospace;font-size:0.74rem">${escHtml(c.column)}<span style="color:#666"> ${escHtml(_queryShortType(c.type))}</span></span>`
          ).join("")
        }</div>
      </div>`).join("");
    _querySchemaLoaded = true;
  } catch (e) {
    el.textContent = "Couldn't load schema: " + (e?.message || e);
  }
}
function _queryShortType(t) {
  const s = String(t || "");
  if (s === "integer") return "int";
  if (s === "smallint") return "int2";
  if (s === "bigint") return "int8";
  if (s === "character varying") return "text";
  if (s === "timestamp with time zone") return "timestamptz";
  if (s === "timestamp without time zone") return "timestamp";
  if (s === "boolean") return "bool";
  if (s === "double precision") return "float8";
  return s;
}

function _queryCell(v) {
  if (v === null || v === undefined) return `<span style="color:#555">NULL</span>`;
  const s = (typeof v === "object") ? JSON.stringify(v) : String(v);
  // Truncate very long cells for display; full value stays in the title.
  const disp = s.length > 200 ? s.slice(0, 200) + "…" : s;
  return `<span title="${escHtml(s)}">${escHtml(disp)}</span>`;
}

async function queryRun() {
  const status = document.getElementById("query-run-status");
  const btn = document.getElementById("query-run-btn");
  const out = document.getElementById("query-results");
  const sql = (document.getElementById("query-sql")?.value || "").trim();
  if (!sql) { if (status) status.textContent = "Write a query first."; return; }
  const maxRows = Math.max(1, Math.min(5000, Number(document.getElementById("query-maxrows")?.value) || 1000));
  if (btn) btn.disabled = true;
  if (status) { status.textContent = "Running…"; status.style.color = "var(--muted)"; }
  try {
    const r = await apiFetch("/api/admin/query/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql, maxRows }),
      timeoutMs: 30000,
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (out) out.innerHTML = `<pre style="color:#e88;white-space:pre-wrap;font-size:0.8rem;padding:0.5rem 0">${escHtml(j?.error || ("HTTP " + r.status))}</pre>`;
      if (status) { status.textContent = "Error"; status.style.color = "#e88"; }
      return;
    }
    const { columns = [], rows = [], rowCount = 0, truncated = false, elapsedMs = 0 } = j;
    if (status) {
      status.style.color = "var(--muted)";
      status.textContent = `${rowCount} row${rowCount === 1 ? "" : "s"}${truncated ? " (capped)" : ""} · ${elapsedMs}ms`;
    }
    if (!columns.length) { if (out) out.innerHTML = `<div style="color:var(--muted);padding:0.5rem 0">No columns.</div>`; return; }
    if (out) {
      out.innerHTML = `
        <table class="api-log-table" style="font-size:0.78rem;width:100%;margin-top:0.5rem">
          <thead><tr>${columns.map(c => `<th style="white-space:nowrap">${escHtml(c)}</th>`).join("")}</tr></thead>
          <tbody>${rows.map(row => `<tr>${row.map(cell => `<td style="vertical-align:top;max-width:32rem">${_queryCell(cell)}</td>`).join("")}</tr>`).join("")}</tbody>
        </table>
        ${truncated ? `<div style="color:#e8a;font-size:0.76rem;margin-top:0.4rem">Showing first ${rowCount} rows — add a tighter WHERE/LIMIT or use CSV for the full set.</div>` : ""}`;
    }
  } catch (e) {
    if (out) out.innerHTML = `<pre style="color:#e88;white-space:pre-wrap;padding:0.5rem 0">${escHtml(String(e?.message || e))}</pre>`;
    if (status) { status.textContent = "Error"; status.style.color = "#e88"; }
  } finally {
    if (btn) btn.disabled = false;
  }
}
window.queryRun = queryRun;

async function queryDownloadCsv() {
  const status = document.getElementById("query-run-status");
  const btn = document.getElementById("query-csv-btn");
  const sql = (document.getElementById("query-sql")?.value || "").trim();
  if (!sql) { if (status) status.textContent = "Write a query first."; return; }
  if (btn) btn.disabled = true;
  if (status) { status.textContent = "Building CSV…"; status.style.color = "var(--muted)"; }
  try {
    const r = await apiFetch("/api/admin/query/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql, format: "csv", maxRows: 50000 }),
      timeoutMs: 60000,
    });
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      if (status) { status.textContent = "Failed: " + (j?.error || r.status); status.style.color = "#e88"; }
      return;
    }
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `seadisco-query-${new Date().toISOString().slice(0, 10)}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 5000);
    if (status) { status.textContent = "CSV downloaded."; status.style.color = "var(--muted)"; }
  } catch (e) {
    if (status) { status.textContent = "Failed: " + (e?.message || e); status.style.color = "#e88"; }
  } finally {
    if (btn) btn.disabled = false;
  }
}
window.queryDownloadCsv = queryDownloadCsv;
window.loadQuerySchema = loadQuerySchema;
