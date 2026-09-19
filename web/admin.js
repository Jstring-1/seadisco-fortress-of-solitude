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
  // A backgrounded browser tab counts as hidden too, so the 5-8s status
  // polls stop instead of running all day in an idle tab.
  if (document.hidden) return false;
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
    try { _adminMakeSectionsCollapsible(); } catch (e) { console.warn('[admin] collapse wiring failed', e); }
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
    // Overview + Users merged into one tab: the grouped KPI dashboard (site
    // totals) plus the unified per-user table (which now also carries the
    // per-user collection counts + taste that used to be a Collection Stats
    // panel). Media moved to its own tab.
    panels: ['panel-overview-kpis', 'panel-users-unified'],
    load: () => { loadAdminOverview(); loadAdminUsersUnified(); },
  },
  'media': {
    panels: ['panel-media-stats'],
    load: () => { loadAdminMediaStats(); },
  },
  'content': {
    panels: ['panel-submissions', 'panel-unavailable'],
    load: () => { loadAdminSubmissions(); loadAdminUnavailable(); },
  },
  'system': {
    panels: ['panel-system-bar', 'panel-db-stats', 'panel-api-log'],
    load: () => { loadAdminSystem(); loadDbStats(); loadApiLog(); },
  },
  'cache': {
    panels: ['panel-cache-warm'],
    load: () => { loadCacheWarm(); loadCacheRate(); loadCacheAnalytics(); loadMasterLabelsStatus(); },
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
    b.classList.toggle('active', b.dataset.adminTab === group);
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

// ── Collapsible sections ──────────────────────────────────────────
// Every admin tab-panel becomes a disclosure: its header row gets a
// caret and clicking the header collapses/expands the body. Collapsed
// by default; the open/closed choice is remembered per panel in
// localStorage. Done generically here (not in markup) so it covers
// every current + future panel. Clicks on controls inside the header
// (refresh, filters, action buttons) never toggle the collapse.
function _adminMakeSectionsCollapsible() {
  document.querySelectorAll('.admin-tab-panel').forEach(panel => {
    if (panel._collapseWired) return;
    // Panels that host several sections internally (each its own
    // <details>) manage their own collapsing — don't wrap the whole
    // panel, or the entire tab would hide behind one caret.
    if (panel.classList.contains('admin-sections-inside')) return;
    const header = panel.firstElementChild;
    if (!header) return;
    // Everything after the header row folds into a body wrapper.
    const body = document.createElement('div');
    body.className = 'admin-collapse-body';
    let n = header.nextSibling;
    while (n) { const next = n.nextSibling; body.appendChild(n); n = next; }
    // Header-only panel (e.g. the System bar) — nothing to collapse.
    if (!body.querySelector('*') && !body.textContent.trim()) {
      while (body.firstChild) panel.appendChild(body.firstChild);
      return;
    }
    panel.appendChild(body);
    panel._collapseWired = true;
    const caret = document.createElement('span');
    caret.className = 'admin-collapse-caret';
    caret.textContent = '▸'; // ▸
    header.insertBefore(caret, header.firstChild);
    header.classList.add('admin-collapse-header');
    const key = 'sd-admin-collapse:' + (panel.id || '');
    let open = false;
    try { open = localStorage.getItem(key) === '1'; } catch {}
    const apply = () => {
      body.style.display = open ? '' : 'none';
      caret.textContent = open ? '▾' : '▸'; // ▾ / ▸
    };
    apply();
    header.addEventListener('click', (e) => {
      if (e.target.closest('button, input, select, textarea, a, label')) return;
      open = !open;
      try { localStorage.setItem(key, open ? '1' : '0'); } catch {}
      apply();
    });
  });
}

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
  // The first tick fires ~500ms after boot, often before Clerk has
  // handed us a session token. Firing tokenless just logs a 401 in the
  // console; skip until auth is ready — the 8s interval picks it up on
  // the next tick.
  try { if (!(await getSessionToken())) return; } catch { return; }
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
      badges.push({ label: `Bulk sweep${pos}${cur}`, stop: '/api/admin/label-directory/bulk-sweep/stop' });
    }
    // (Split-cache projection retired — no badge.)
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
      badges.push({ label, stop: '/api/admin/cache-warm-catno/stop' });
    }
    if (w.ext?.running) {
      const j = w.ext;
      const pos = j.total > 0 ? ` (${j.cursor}/${j.total})` : '';
      badges.push({ label: `Ext scrape · ${j.source || 'unknown'}${pos}`, stop: '/api/admin/external-discography-worker/stop' });
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
      badges.push({ label: `Year×${j.mode || 'facet'}${pos}${cur}`, stop: '/api/admin/faceted-sweep/stop' });
    }
    if (w.labels?.running) {
      const j = w.labels;
      const pos = j.total > 0 ? ` (${j.processed}/${j.total})` : '';
      badges.push({ label: `Master labels${pos}`, stop: '/api/admin/master-labels/stop' });
    }
    if (w.upstream?.running) {
      const j = w.upstream;
      const pos = j.total > 0 ? ` (${j.cursor}/${j.total})` : '';
      badges.push({ label: `Upstream totals${pos}`, stop: '/api/admin/label-upstream-stats/stop' });
    }
    if (badges.length === 0) {
      bar.style.display = 'none';
      bar.innerHTML = '';
      return;
    }
    bar.style.display = '';
    bar.innerHTML = `
      <strong style="color:#fc8">⚙ Workers running:</strong>
      ${badges.map(b => b.tab ? `
        <button type="button" data-sd-click="${_sdOn(((a0) => function (event) { switchAdminTab(a0) })(String(b.tab ?? "")))}"
          style="margin-left:0.4rem;padding:0.15rem 0.55rem;background:rgba(255,255,255,0.06);color:var(--text);border:1px solid rgba(255,255,255,0.18);border-radius:3px;cursor:pointer;font-size:0.78rem">
          ${_adminWorkerEscape(b.label)} ↗
        </button>`
      // Workers whose tab was retired have no panel to jump to — offer
      // Stop right here so a runaway sweep can still be halted.
      : `<span style="margin-left:0.4rem;padding:0.15rem 0.2rem 0.15rem 0.55rem;background:rgba(255,255,255,0.06);color:var(--text);border:1px solid rgba(255,255,255,0.18);border-radius:3px;font-size:0.78rem">
          ${_adminWorkerEscape(b.label)}
          <button type="button" class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { _adminStopWorker(a0, this) })(String(b.stop ?? "")))}" style="margin-left:0.3rem;font-size:0.72rem;padding:0.05rem 0.4rem;color:#e88" title="Stop this worker">■ Stop</button>
        </span>`).join('')}
    `;
  } catch (err) {
    // Silent — the bar just stays hidden. Persistent workers status
    // isn't critical enough to alert on network hiccups.
  }
}
async function _adminStopWorker(path, btn) {
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch(path, { method: "POST" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    if (typeof showToast === "function") showToast("Stop requested — the worker winds down at its next checkpoint.", "info", 5000);
    setTimeout(loadAdminWorkerStatus, 1500);
  } catch (e) {
    if (btn) btn.disabled = false;
    if (typeof showToast === "function") showToast(`Stop failed: ${e?.message || e}`, "error", 5000);
  }
}
window._adminStopWorker = _adminStopWorker;
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

async function adminSyncUser(username, btn) {
  const prev = btn ? btn.textContent : null;
  if (btn) { btn.disabled = true; btn.textContent = "Starting…"; }
  try {
    const r = await apiFetch("/api/admin/sync-user", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username })
    });
    // The endpoint 4xx's synchronously for the fixable-here problems
    // (no such user, no valid Discogs auth). Surface that message rather
    // than silently leaving the row on its old status.
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      throw new Error(j.error || `HTTP ${r.status}`);
    }
    // 200 only means the run was QUEUED. runBackgroundSync flips the DB
    // status to "syncing" ~1-2s later (after fetching page counts) and
    // may re-write "error" if the run itself fails (expired token,
    // private/renamed Discogs account). Refreshing immediately would just
    // re-show the stale status, so keep the button in a pending state and
    // re-poll a few times to catch the real outcome.
    if (btn) btn.textContent = "Syncing…";
    let polls = 0;
    const poll = () => {
      loadAdminUsersUnified(true); // silent — no "Loading…" flash
      if (++polls < 4) setTimeout(poll, polls * 2500);
    };
    setTimeout(poll, 2000);
  } catch (e) {
    if (btn) { btn.disabled = false; btn.textContent = prev; }
    _adminNotify("Sync failed for " + username + (e?.message ? `: ${e.message}` : "")
      + "\n\nIf the row stays on “error”, hover it to read the reason — an expired or revoked Discogs token can't be fixed by re-syncing; the user has to reconnect Discogs.");
  }
}
window.adminSyncUser = adminSyncUser;

// Reset stuck syncs. Aborts any in-flight run and flips every DB row
// still on "syncing" to "stopped" \u2014 the fix for a sync orphaned at a
// fixed % because its server process died mid-run (e.g. a redeploy).
async function adminSyncStop(btn) {
  btn = btn || document.getElementById("admin-sync-stop-btn");
  const statusEl = document.getElementById("suggestions-action-status");
  const prev = btn ? btn.textContent : null;
  if (btn) { btn.disabled = true; btn.textContent = "Resetting\u2026"; }
  try {
    const r = await apiFetch("/api/admin/sync-stop", { method: "POST" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || `HTTP ${r.status}`);
    if (statusEl) statusEl.textContent = j.message || "Syncs reset.";
    loadAdminUsersUnified();
  } catch (e) {
    if (statusEl) statusEl.textContent = "Reset failed" + (e?.message ? `: ${e.message}` : ".");
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = prev || "Reset stuck syncs"; }
  }
}
window.adminSyncStop = adminSyncStop;

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
    const { tables, totalRows, dbBytes } = await r.json();
    const fmtBytes = b => b >= 1073741824 ? `${(b / 1073741824).toFixed(2)} GB` : b >= 1048576 ? `${(b / 1048576).toFixed(1)} MB` : `${Math.max(1, Math.round((b || 0) / 1024))} KB`;
    const sizeMap = {};
    tables.forEach(t => sizeMap[t.table] = t.bytes);

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

    let html = `<div style="margin-bottom:0.6rem;font-weight:600;color:var(--fg)" title="Row counts are Postgres estimates (updated by autovacuum/ANALYZE); sizes include indexes and TOAST.">≈${totalRows.toLocaleString()} rows · ${tables.length} tables${dbBytes ? ` · ${fmtBytes(dbBytes)} on disk` : ""}</div>`;
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
          ? `<a href="#" data-sd-click="${_sdOn(((a0) => function (event) { event.preventDefault();adminOpenDbTablePopup(a0) })(String(t ?? "")))}" style="color:var(--muted);text-decoration:none;border-bottom:1px dotted transparent" data-sd-mouseover="${_sdOn(function (event) { this.style.borderBottomColor='var(--accent)';this.style.color='var(--text)' })}" data-sd-mouseout="${_sdOn(function (event) { this.style.borderBottomColor='transparent';this.style.color='var(--muted)' })}" title="Show schema, indexes, size">${t}</a>`
          : `<span style="color:var(--muted)">${t}</span>`;
        html += `<div style="display:flex;justify-content:space-between;gap:0.6rem;padding:0.12rem 0;font-size:0.78rem">${nameMarkup}<span style="margin-left:auto;color:var(--muted);font-variant-numeric:tabular-nums">${sizeMap[t] ? fmtBytes(sizeMap[t]) : ""}</span><span style="color:${color};font-weight:500;min-width:4.5em;text-align:right;font-variant-numeric:tabular-nums">${display}</span></div>`;
      }
      html += `</div>`;
    }
    html += '</div>';
    el.innerHTML = html;

    // Flat all-tables view inside the <details> below. Sorted by row
    // count desc so the largest tables surface first.
    const flatEl = document.getElementById("db-stats-flat");
    if (flatEl) {
      // Largest on disk first.
      const sorted = [...tables].sort((a, b) => (b.bytes ?? 0) - (a.bytes ?? 0));
      flatEl.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));column-gap:1rem;row-gap:0.15rem">` +
        sorted.map(t => {
          const count = t.rows;
          const display = count < 0 ? "err" : count.toLocaleString();
          const color = count < 0 ? "var(--danger)" : count === 0 ? "var(--muted-dim)" : "var(--fg)";
          const exists = count >= 0;
          const nameMarkup = exists
            ? `<a href="#" data-sd-click="${_sdOn(((a0) => function (event) { event.preventDefault();adminOpenDbTablePopup(a0) })(String(t.table ?? "")))}" style="color:var(--muted);font-family:monospace;text-decoration:none;border-bottom:1px dotted transparent" data-sd-mouseover="${_sdOn(function (event) { this.style.borderBottomColor='var(--accent)';this.style.color='var(--text)' })}" data-sd-mouseout="${_sdOn(function (event) { this.style.borderBottomColor='transparent';this.style.color='var(--muted)' })}" title="Show schema, indexes, size">${t.table}</a>`
            : `<span style="color:var(--muted);font-family:monospace">${t.table}</span>`;
          return `<div style="display:flex;justify-content:space-between;gap:0.6rem;font-size:0.78rem">${nameMarkup}<span style="margin-left:auto;color:var(--muted);font-variant-numeric:tabular-nums">${fmtBytes(t.bytes)}</span><span style="color:${color};font-weight:500;min-width:4.5em;text-align:right;font-variant-numeric:tabular-nums">${display}</span></div>`;
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
// Combos checked for a back-to-back batch run. Held as a Set of
// "genre||style" keys so the selection survives the 20s poll re-render
// (each checkbox re-derives its checked state from this Set).
let _cwSelected = new Set();
function _cwSelKey(g, s) { return String(g == null ? "" : g) + "||" + String(s == null ? "" : s); }
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
// Delegates to the canonical escaper in shared.js (escapes & < > " ').
// Kept as a hoisted function (not `const _eHtml = escHtml`) because
// several admin functions reference _eHtml above this line; hoisting
// keeps those safe regardless of execution order.
function _eHtml(s) {
  return escHtml(s);
}
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
  const retryLink = `<a href="#" data-sd-click="${_sdOn(function (event) { event.preventDefault();loadCacheRate();return false })}" style="color:var(--accent);margin-left:0.5rem">↻ retry</a>`;
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
    const HOUR_MS = 3600000;
    const len = series.length;
    // The series is always N consecutive hourly buckets ending at the
    // current hour, so derive each bucket's instant from its POSITION.
    // This makes the hover time bulletproof — no dependence on whatever
    // the payload happens to carry (t / hour / nothing).
    const nowHour = Math.floor(Date.now() / HOUR_MS) * HOUR_MS;
    const msAt = (i) => Number.isFinite(Number(series[i]?.t)) ? Number(series[i].t) : (nowHour - (len - 1 - i) * HOUR_MS);
    const ptFull = (ms) => new Date(ms).toLocaleString("en-US", { timeZone: "America/Los_Angeles", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
    const ptHour = (ms) => new Date(ms).toLocaleTimeString("en-US", { timeZone: "America/Los_Angeles", hour: "numeric" });
    const bars = series.map((h, i) => {
      const v = Number(h.n) || 0;
      const ms = msAt(i);
      const pct = max > 0 ? Math.round((v / max) * 100) : 0;
      const last = i === len - 1;
      // Data hours → accent bar (min 4% so tiny counts stay visible).
      // Empty hours → a 2px muted floor tick so "0 writes" reads clearly
      // distinct from a bar. Current hour → accent outline marks "now".
      const inner = v > 0
        ? `<span style="position:absolute;bottom:0;left:0;right:0;height:${Math.max(pct, 4)}%;background:var(--accent)"></span>`
        : `<span style="position:absolute;bottom:0;left:0;right:0;height:2px;background:var(--border)"></span>`;
      return `<span title="${ptFull(ms)} PT · ${n(v)} write${v === 1 ? "" : "s"}" style="display:inline-block;width:7px;height:36px;background:rgba(255,255,255,0.05);vertical-align:bottom;position:relative${last ? ";outline:1px solid var(--accent);outline-offset:-1px" : ""}">${inner}</span>`;
    }).join("");
    const chart = len ? `
      <div style="display:inline-block;max-width:100%;overflow-x:auto">
        <div style="display:flex;gap:1px;align-items:flex-end;height:36px;border-left:1px solid var(--border);border-bottom:1px solid var(--border);padding:0 1px" title="Writes per hour, last ${len} h (Pacific time). Each slot is one hour; the outlined slot on the right is the current hour.">${bars}</div>
        <div style="display:flex;justify-content:space-between;font-size:0.64rem;color:var(--muted);margin-top:3px">
          <span>${ptHour(msAt(0))} PT</span><span>now · ${ptHour(msAt(len - 1))} PT ▸</span>
        </div>
      </div>` : `<div style="font-size:0.72rem;color:var(--muted)">no hourly data</div>`;
    el.innerHTML = `
      <div style="display:flex;gap:1.1rem;flex-wrap:wrap;align-items:baseline;margin-bottom:0.5rem">
        <span title="release_cache writes in the last hour">Last hour: <strong style="color:var(--text)">${n(s.window?.["1h"])}</strong></span>
        <span title="release_cache writes in the last 24 hours (masters + releases)">Last 24h: <strong style="color:var(--text)">${n(s.window?.["24h"])}</strong> <span style="font-size:0.72rem">(${n(s.window?.master24)} master · ${n(s.window?.release24)} release)</span></span>
        <span title="Average writes per hour across the last 24h">Rate: <strong style="color:var(--text)">${n(s.ratePerHour24h)}</strong>/hr</span>
        <span title="release_cache writes in the last 7 days">Last 7d: <strong style="color:var(--text)">${n(s.window?.["7d"])}</strong></span>
      </div>
      ${chart}
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

// ── Prune redundant releases ──────────────────────────────────────────
async function loadRedundantPreview(btn) {
  const out = document.getElementById("rr-preview");
  if (out) out.textContent = "Checking…";
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch("/api/admin/redundant-releases/preview");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    if (out) {
      out.innerHTML =
        `<strong>${j.warmOnly.toLocaleString()}</strong> never-viewed · ` +
        `<strong>${j.total.toLocaleString()}</strong> total redundant ` +
        `<span style="color:var(--muted)">(of ${j.releaseRows.toLocaleString()} releases / ${j.masterRows.toLocaleString()} masters cached)</span>`;
    }
  } catch (e) {
    if (out) out.textContent = `Error: ${e.message || e}`;
  } finally {
    if (btn) btn.disabled = false;
  }
}
window.loadRedundantPreview = loadRedundantPreview;

async function pruneRedundant(btn) {
  const mode = document.getElementById("rr-mode")?.value === "all" ? "all" : "warm_only";
  const label = mode === "all" ? "ALL redundant pressings" : "never-viewed redundant pressings";
  if (!confirm(`Delete ${label}? This removes cached pressings whose master is already cached. Protected items (libraries, playlists, queues, overrides) are always kept. This can't be undone (the data re-fetches from Discogs on demand).`)) return;
  const out = document.getElementById("rr-result");
  if (out) out.textContent = "Pruning…";
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch("/api/admin/redundant-releases/prune", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    if (out) out.innerHTML = `<span style="color:var(--success)">Removed ${Number(j.deleted).toLocaleString()} pressings.</span>`;
    if (typeof showToast === "function") showToast(`Pruned ${Number(j.deleted).toLocaleString()} redundant releases`, "info");
    // Refresh the preview so the counts reflect the deletion.
    loadRedundantPreview();
  } catch (e) {
    if (out) out.textContent = `Error: ${e.message || e}`;
  } finally {
    if (btn) btn.disabled = false;
  }
}
window.pruneRedundant = pruneRedundant;

// ── Fill in master labels (master-label-backfill-worker) ─────────
async function loadMasterLabelsStatus() {
  const out = document.getElementById("mlb-status");
  if (!out) return;
  try {
    const r = await apiFetch("/api/admin/master-labels/status");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    const nf = (n) => Number(n || 0).toLocaleString();
    const parts = [`<strong style="color:var(--text)">${nf(j.remaining)}</strong> masters still need a label`];
    if (j.running) parts.unshift(`<span style="color:var(--success)">Running</span>`);
    if (j.startedAt) parts.push(`this run: ${nf(j.fromCache)} from cache, ${nf(j.fetched)} fetched, ${nf(j.noLabel)} with no label${j.errors ? `, ${nf(j.errors)} errors` : ""}`);
    if (j.done && !j.running) parts.push("finished");
    if (j.lastError) parts.push(`<span style="color:#e05050">${_adminWorkerEscape(j.lastError)}</span>`);
    out.innerHTML = parts.join(" · ");
  } catch (e) {
    out.textContent = `Error: ${e.message || e}`;
  }
}
window.loadMasterLabelsStatus = loadMasterLabelsStatus;
async function masterLabelsAction(action, btn) {
  const out = document.getElementById("mlb-status");
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch(`/api/admin/master-labels/${action}`, { method: "POST" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || `HTTP ${r.status}`);
    if (typeof _adminNotify === "function") _adminNotify(action === "start" ? "Label fill started" : "Stopping label fill", "info");
  } catch (e) {
    if (out) out.textContent = `Error: ${e.message || e}`;
    return;
  } finally {
    if (btn) btn.disabled = false;
  }
  setTimeout(loadMasterLabelsStatus, 1500);
}
window.masterLabelsAction = masterLabelsAction;
// Populate the V1-vs-V2 totals badge next to the Explore cache header
// from projection stats (the split-cache projection is retired, so this
// normally has nothing to show).
// V1 = projectable release_cache rows (release+master); V2 = the
// masters_plus + pressings tables — apples-to-apples so a gap means
// the projection backfill hasn't fully drained.
function _renderCaTotalsBadge() {
  // Split cache retired — the V1-vs-V2 gap badge is gone. release_cache
  // totals live in the "Cache write rate" card now.
  const badge = document.getElementById("ca-totals-badge");
  if (badge) badge.textContent = "";
}

// ── Cache analytics (faceted drilldown) ─────────────────────────
let _cacheAnalyticsFilters = { label:"", artist:"", genre:"", style:"", country:"", yearFrom:"", yearTo:"", type:"" };
let _cacheAnalyticsResult = null;
let _cacheAnalyticsLoading = false;

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
      const click = keyName === "decade" ? "" : `data-sd-click="${_sdOn(((a0, a1) => function (event) { _caFacetPin(a0,a1) })(String((keyName) ?? ""), String((String(r.name || "")) ?? "")))}"`;
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
             data-sd-input="${_sdOn(function (event) { _caFilterInput(event) })}" placeholder="${_eHtml(placeholder)}"
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
          <input id="ca-year-from" type="number" value="${_eHtml(String(f.yearFrom || ""))}" data-sd-input="${_sdOn(function (event) { _caFilterInput(event) })}" placeholder="from"
                 style="width:50%;padding:0.25rem 0.4rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          <input id="ca-year-to"   type="number" value="${_eHtml(String(f.yearTo   || ""))}" data-sd-input="${_sdOn(function (event) { _caFilterInput(event) })}" placeholder="to"
                 style="width:50%;padding:0.25rem 0.4rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
        </div>
      </label>
      <label style="display:flex;flex-direction:column;gap:0.15rem;font-size:0.75rem;color:var(--muted)">
        <span>Type</span>
        <select id="ca-type" data-sd-change="${_sdOn(function (event) { _caFilterInput(event) })}"
                style="padding:0.25rem 0.4rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px">
          <option value=""${f.type === "" ? " selected" : ""}>Both</option>
          <option value="release"${f.type === "release" ? " selected" : ""}>Release</option>
          <option value="master"${f.type === "master" ? " selected" : ""}>Master</option>
        </select>
      </label>
    </div>
    <div style="display:flex;gap:0.4rem;margin-bottom:0.4rem;align-items:center;flex-wrap:wrap">
      <button class="admin-btn" type="button" data-sd-click="${_sdOn(function (event) { _caRun() })}" ${_cacheAnalyticsLoading ? "disabled" : ""}>▶ Analyze</button>
      <button class="admin-btn" type="button" data-sd-click="${_sdOn(function (event) { _caReset() })}">Reset</button>
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
async function _caRun() {
  _cacheAnalyticsLoading = true;
  _renderCacheAnalytics();
  try {
    const body = { ...(_cacheAnalyticsFilters) };
    // Blank strings should not be sent as filter values.
    for (const k of Object.keys(body)) if (body[k] === "") delete body[k];
    const r = await apiFetch(`/api/admin/cache-analytics`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      throw new Error(j.error ? `HTTP ${r.status}: ${j.error}` : `HTTP ${r.status}`);
    }
    _cacheAnalyticsResult = await r.json();
  } catch (err) {
    _adminNotify(`Analyze failed: ${err}`);
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
    let { rows = [], release_cache_total = 0, active = null, running = false, queue = [] } = resp;
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
            <select id="cw-form-genre" data-sd-change="${_sdOn(function (event) { _cwSyncStyleList() })}" ${running ? "disabled" : ""} style="width:100%;padding:0.4rem 0.5rem;font-size:0.86rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px">${genreOptions}</select>
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
              ? `<button class="admin-btn" data-sd-click="${_sdOn(function (event) { cacheWarmStop() })}" title="Signal the worker to wind down at the next safe boundary.">■ Stop</button>`
              : `<button class="admin-btn" data-sd-click="${_sdOn(function (event) { cacheWarmStartFromForm(false) })}" title="Resume from the persisted cursor for this combo, or start fresh if none. From-year only applies on first run for the combo.">▶ Start</button>
                 <button class="admin-btn" data-sd-click="${_sdOn(function (event) { cacheWarmStartFromForm(true) })}" title="Reset the cursor for this combo to From-year before starting.">↻ Start over</button>
                 <button class="admin-btn" data-sd-click="${_sdOn(function (event) { cacheWarmStartNoYearForForm() })}" title="Sweep releases in this genre/style that have NO year on Discogs — year-filtered runs (e.g. 1900-1970) skip these. Cursor for the no-year run is independent of the dated cursor.">📅 No-year sweep</button>`}
            <button class="admin-btn" data-sd-click="${_sdOn(function (event) { cacheWarmForceClear() })}" title="Force-clear the in-memory 'running' lock when the worker is stuck or crashed silently. Doesn't affect cached data. Use if Start refuses to fire.">⚠ Force clear lock</button>
          </div>
        </div>
      </div>

      <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.4rem;flex-wrap:wrap">
        <button class="admin-btn" type="button" data-sd-click="${_sdOn(function (event) { _cwRunSelected() })}" title="Queue every checked row to run 1900–1970 (each chains a no-year sweep), back-to-back. Works while a run is active — they line up behind it.">▶ Run selected (1900-1970)</button>
        <button id="cw-del-selected-btn" class="admin-btn" type="button" data-sd-click="${_sdOn(function (event) { _cwDeleteSelected() })}" title="Delete every release_cache row for all checked combos and zero their run-stat columns. One confirm for the batch. Data re-fetches from Discogs on demand." style="color:#e88;border-color:rgba(232,136,136,0.5)">⌫ Delete selected</button>
        <span id="cw-sel-count" style="font-size:0.76rem;color:var(--muted)">${_cwSelected.size ? _cwSelected.size + " selected" : ""}</span>
        ${queue.length ? `<span style="font-size:0.76rem;color:var(--accent)" title="${esc(queue.map(q => q.genreKey + (q.styleKey ? "/" + q.styleKey : "")).join(", "))}">queued: <strong>${queue.length}</strong> — ${esc(queue.slice(0, 4).map(q => q.genreKey + (q.styleKey ? "/" + q.styleKey : "")).join(", "))}${queue.length > 4 ? "…" : ""}</span>
             <button class="admin-btn" type="button" data-sd-click="${_sdOn(function (event) { _cwClearQueue() })}" title="Remove all pending queued combos. Doesn't stop the active run.">✕ Clear queue</button>` : ""}
        <button class="admin-btn" type="button" data-sd-click="${_sdOn(function (event) { _cwToggleGenresOnly() })}" style="margin-left:auto" title="Hide rows that have a style set so only top-level genre rows remain.">${_cwGenresOnly ? "Show all (genres + styles)" : "Hide styles (genres only)"}</button>
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
            return `<th style="text-align:${align};cursor:pointer;user-select:none" data-sd-click="${_sdOn(((a0) => function (event) { _cwSortBy(a0) })(String(key ?? "")))}" title="Sort by ${label}">${label}${arrow}</th>`;
          };
          return `<div class="cw-table-wrap" style="overflow-x:auto"><table class="api-log-table cw-stats-table" style="font-size:0.82rem;width:100%;table-layout:fixed">
            <colgroup>
              <col style="width:4%">
              <col style="width:14%">
              <col style="width:14%">
              <col style="width:8%">
              <col style="width:8%">
              <col style="width:7%">
              <col style="width:7%">
              <col style="width:7%">
              <col style="width:11%">
              <col style="width:20%">
            </colgroup>
            <thead><tr>
              <th style="text-align:center"><input type="checkbox" data-sd-click="${_sdOn(function (event) { _cwSelectAllVisible(this.checked) })}" title="Select / deselect all visible rows"></th>
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
              const safeG = String(r.genre_key ?? "");
              const safeS = String(r.style_key || "");
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
                <td style="text-align:center"><input type="checkbox" class="cw-sel" data-g="${esc(r.genre_key)}" data-s="${esc(r.style_key || "")}" ${_cwSelected.has(_cwSelKey(r.genre_key, r.style_key)) ? "checked" : ""} data-sd-change="${_sdOn(function (event) { _cwSelToggle(this) })}"></td>
                <td style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.genre_key)}">${esc(r.genre_key)}</td>
                <td style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:${r.style_key ? "var(--text)" : "var(--muted)"}" title="${esc(styleLabel)}">${esc(styleLabel)}</td>
                <td style="text-align:right;font-variant-numeric:tabular-nums"><strong>${fmt(r.in_cache)}</strong></td>
                <td style="text-align:right;font-variant-numeric:tabular-nums;color:${isAuto ? "var(--muted)" : ""}">${isAuto ? "—" : fmt(r.total_cached)}</td>
                <td style="text-align:right;font-variant-numeric:tabular-nums;color:var(--muted)">${isAuto ? "—" : fmt(r.total_skipped)}</td>
                <td style="text-align:right;font-variant-numeric:tabular-nums;color:${isAuto ? "var(--muted)" : (r.total_errors ? "#e88" : "var(--muted)")}">${isAuto ? "—" : fmt(r.total_errors)}</td>
                <td style="text-align:right;color:var(--muted);font-size:0.74rem">${cursor}</td>
                <td style="color:var(--muted);font-size:0.74rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(last)}${nyPill}</td>
                <td style="text-align:right;white-space:nowrap">
                  <button class="admin-btn" ${running ? "disabled" : ""} data-sd-click="${_sdOn(((a0, a1) => function (event) { cacheWarmRunComboBlues(a0,a1) })(String(safeG ?? ""), String(safeS ?? "")))}" title="Start a cache-warm run for this combo with year range 1900–1970, then automatically chain a no-year sweep so long-tail undated releases get picked up too." style="margin-right:0.25rem">▶ 1900-1970</button>
                  <button class="admin-btn" ${running ? "disabled" : ""} data-sd-click="${_sdOn(((a0, a1) => function (event) { _cwLoadIntoForm(a0,a1) })(String(safeG ?? ""), String(safeS ?? "")))}" title="Load this combo into the form so you can run it" style="margin-right:0.25rem">↗</button>
                  <button class="admin-btn" ${running ? "disabled" : ""} data-sd-click="${_sdOn(((a0, a1, a2) => function (event) { _cwDeleteCombo(a0,a1, a2, this) })(String(safeG ?? ""), String(safeS ?? ""), _sdLit(r.in_cache || 0)))}" title="Delete every release_cache row for this (genre, style) combo. Deletes immediately; toast confirms the count. Data re-fetches from Discogs on demand." style="color:#e88;border-color:rgba(232,136,136,0.5)">⌫</button>
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
// ── Batch selection (checkbox column) ─────────────────────────────
function _cwSelToggle(el) {
  const k = _cwSelKey(el.dataset.g, el.dataset.s);
  if (el.checked) _cwSelected.add(k); else _cwSelected.delete(k);
  _cwUpdateSelCount();
}
window._cwSelToggle = _cwSelToggle;
function _cwSelectAllVisible(checked) {
  document.querySelectorAll("input.cw-sel").forEach(cb => {
    cb.checked = checked;
    const k = _cwSelKey(cb.dataset.g, cb.dataset.s);
    if (checked) _cwSelected.add(k); else _cwSelected.delete(k);
  });
  _cwUpdateSelCount();
}
window._cwSelectAllVisible = _cwSelectAllVisible;
function _cwUpdateSelCount() {
  const el = document.getElementById("cw-sel-count");
  if (el) el.textContent = _cwSelected.size ? _cwSelected.size + " selected" : "";
}
async function _cwRunSelected() {
  if (!_cwSelected.size) { _adminNotify("Check one or more rows first."); return; }
  const combos = [..._cwSelected].map(k => {
    const i = k.indexOf("||");
    return { genreKey: k.slice(0, i), styleKey: k.slice(i + 2), fromYear: 1900, toYear: 1970, alsoNoYear: true };
  });
  const label = combos.map(c => c.styleKey ? `${c.genreKey}/${c.styleKey}` : c.genreKey).join(", ");
  if (!confirm(`Queue ${combos.length} combo(s) to run 1900–1970 back-to-back (each chains a no-year sweep)?\n\n${label}`)) return;
  try {
    const r = await apiFetch("/api/admin/cache-warm-runs/start-batch", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ combos }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { _adminNotify(j.error || `HTTP ${r.status}`); return; }
    _cwSelected.clear();
    loadCacheWarm();
  } catch (e) { _adminNotify("Batch start failed: " + ((e && e.message) || e)); }
}
window._cwRunSelected = _cwRunSelected;
// Bulk delete every checked combo: delete its cached rows + zero its
// run-stat columns, same as the per-row ⌫ but for the whole selection.
// One confirm for the batch (each combo can be huge), then a summary toast.
async function _cwDeleteSelected() {
  if (!_cwSelected.size) { _adminNotify("Check one or more rows first."); return; }
  const combos = [..._cwSelected].map(k => {
    const i = k.indexOf("||");
    return { genre: k.slice(0, i), style: k.slice(i + 2) };
  });
  const label = combos.map(c => c.style ? `${c.genre}/${c.style}` : `${c.genre} (all)`).join(", ");
  if (!confirm(`Delete cached rows for ${combos.length} combo${combos.length === 1 ? "" : "s"} and zero their counters?\n\n${label}\n\nData re-fetches from Discogs on demand.`)) return;
  const btn = document.getElementById("cw-del-selected-btn");
  if (btn) { btn.disabled = true; btn.textContent = "Deleting…"; }
  let totalDeleted = 0, failures = 0;
  for (const c of combos) {
    try {
      const body = { genre: c.genre };
      if (c.style) body.style = c.style;
      const r = await apiFetch("/api/admin/release-cache/delete", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) { failures++; continue; }
      totalDeleted += Number(j.deleted ?? 0);
      // Zero the run-stat columns for this combo (same as single delete).
      try {
        await apiFetch("/api/admin/cache-warm-runs/reset", {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ genreKey: c.genre, styleKey: c.style || "" }),
        });
      } catch {}
    } catch { failures++; }
  }
  _cwSelected.clear();
  if (typeof showToast === "function") {
    showToast(
      `Deleted ${totalDeleted.toLocaleString()} row${totalDeleted === 1 ? "" : "s"} across ${combos.length} combo${combos.length === 1 ? "" : "s"}${failures ? ` — ${failures} failed` : ""}`,
      failures ? "error" : "info",
    );
  }
  if (typeof loadCacheWarm === "function") { try { loadCacheWarm(); } catch {} }
}
window._cwDeleteSelected = _cwDeleteSelected;
async function _cwClearQueue() {
  if (!confirm("Clear all pending queued combos? The active run keeps going.")) return;
  try {
    const r = await apiFetch("/api/admin/cache-warm-runs/queue/clear", { method: "POST" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    loadCacheWarm();
  } catch (e) { _adminNotify("Clear queue failed: " + ((e && e.message) || e)); }
}
window._cwClearQueue = _cwClearQueue;

// Per-row delete from the grid. Reuses the export's delete endpoint
// so the same filter pipeline applies. Requires a typed-N confirm
// above 1000 rows so the bigger ⌫ in a popular genre can't fire on
// a misclick. Refreshes the grid on success.
async function _cwDeleteCombo(genre, style, expectedCount, btn) {
  if (!genre) return;
  // No up-front confirm (removed per request) — deletes immediately, then
  // reports the result via toast so it's clearly "working". Data re-fetches
  // from Discogs on demand, so this is recoverable.
  const label = style ? `${genre} / ${style}` : `${genre} (all styles)`;
  if (btn) { btn.disabled = true; btn.dataset._t = btn.textContent; btn.textContent = "…"; }
  try {
    const body = { genre };
    if (style) body.style = style;
    const r = await apiFetch("/api/admin/release-cache/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (typeof showToast === "function") showToast(`Delete failed: ${j.error || r.status}`, "error");
      else _adminNotify(`Delete failed: ${j.error || r.status}`);
      return;
    }
    const n = Number(j.deleted ?? 0);
    // Also zero the run-stat columns (cached/skipped/errors/cursor/last-run)
    // for this combo — those live in cache_warm_runs, not release_cache, so
    // deleting cached rows alone leaves them showing a stale history.
    try {
      await apiFetch("/api/admin/cache-warm-runs/reset", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ genreKey: genre, styleKey: style || "" }),
      });
    } catch {}
    if (typeof showToast === "function") showToast(`Deleted ${n.toLocaleString()} row${n === 1 ? "" : "s"} — ${label}`, "info");
    if (typeof loadCacheWarm === "function") { try { loadCacheWarm(); } catch {} }
  } catch (e) {
    if (typeof showToast === "function") showToast(`Delete failed: ${e}`, "error");
    else _adminNotify(`Delete failed: ${e}`);
  } finally {
    if (btn && btn.isConnected) { btn.disabled = false; if (btn.dataset._t) btn.textContent = btn.dataset._t; }
  }
}
window._cwDeleteCombo = _cwDeleteCombo;
async function cacheWarmStartFromForm(resetCursor) {
  const genreKey = document.getElementById("cw-form-genre")?.value || "";
  const styleKey = document.getElementById("cw-form-style")?.value || "";
  const from     = document.getElementById("cw-form-from")?.value || "";
  const to       = document.getElementById("cw-form-to")?.value   || "";
  if (!genreKey) { _adminNotify("Pick a genre."); return; }
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
      _adminNotify(body.error || "Another run is in progress.");
      return;
    }
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.error || `HTTP ${r.status}`);
    }
    loadCacheWarm();
  } catch (e) { _adminNotify("Start failed: " + ((e && e.message) || e)); }
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
  if (!genreKey) { _adminNotify("Pick a genre."); return; }
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
  return q;
}

// Dump the entire release_cache as NDJSON (straight SELECT *). The old
// split-cache dump was removed with the V2 retirement.
// Native <a href> downloads can't carry the admin Bearer header, so these
// streaming dumps 401'd ("File wasn't available on site"). Mint a one-time
// ticket via an authenticated POST first, then append it to the URL so the
// server authorizes the streaming download without buffering it in the
// browser (a multi-GB blob would OOM the tab).
async function _rcxDownloadTicket() {
  const r = await apiFetch("/api/admin/download-ticket", { method: "POST" });
  if (!r.ok) throw new Error("ticket request failed (" + r.status + ")");
  const j = await r.json();
  if (!j?.ticket) throw new Error("no ticket returned");
  return j.ticket;
}
function _rcxTriggerDownload(url) {
  const link = document.createElement("a");
  link.href = url;
  link.download = "";
  document.body.appendChild(link);
  link.click();
  setTimeout(() => link.remove(), 0);
}

async function rcxDumpV1() {
  if (!confirm("Stream every row of release_cache as NDJSON — straight SELECT *, all columns?\n\nFilters are ignored. Uncompressed — expect gigabytes at millions of rows.")) return;
  try {
    const ticket = await _rcxDownloadTicket();
    _rcxTriggerDownload("/api/admin/release-cache/dump-v1?ticket=" + encodeURIComponent(ticket));
  } catch (e) {
    _adminNotify("Could not authorize download: " + (e?.message || e));
  }
}
window.rcxDumpV1 = rcxDumpV1;

async function rcxDumpAll() {
  if (!confirm("Dump the ENTIRE database — every base table, all columns — as one NDJSON file (each row tagged __table)?\n\nThis is everything, including lyrics, words, artists, logs. Uncompressed and potentially very large.")) return;
  try {
    const ticket = await _rcxDownloadTicket();
    _rcxTriggerDownload("/api/admin/db/dump-all?ticket=" + encodeURIComponent(ticket));
  } catch (e) {
    _adminNotify("Could not authorize download: " + (e?.message || e));
  }
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
      <input type="checkbox" ${checked} data-sd-change="${_sdOn(((a0) => function (event) { _rcxToggleLabel(this,a0) })(String(it.name ?? "")))}">
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
      _adminNotify(body.error || "Another run is in progress.");
      return;
    }
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.error || `HTTP ${r.status}`);
    }
    loadCacheWarm();
  } catch (e) { _adminNotify("Start failed: " + ((e && e.message) || e)); }
}
async function cacheWarmForceClear() {
  if (!confirm("Force-clear the in-memory 'running' lock? Use this when a worker crashed silently and Start now refuses to fire. Doesn't affect cached data.")) return;
  try {
    const r = await apiFetch("/api/admin/cache-warm-runs/force-clear", { method: "POST" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    loadCacheWarm();
  } catch (e) { _adminNotify("Force clear failed: " + ((e && e.message) || e)); }
}
window.cacheWarmForceClear = cacheWarmForceClear;
async function cacheWarmStop() {
  try {
    const r = await apiFetch("/api/admin/cache-warm-runs/stop", { method: "POST" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    loadCacheWarm();
  } catch (e) { _adminNotify("Stop failed: " + ((e && e.message) || e)); }
}
window.cacheWarmStop = cacheWarmStop;

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
let _ytrQuery = "";
let _ytrFilterTimer = null;
// ── Decide bookkeeping ───────────────────────────────────────────────
// Rapid Approve/Reject clicking used to error out. The flow is optimistic
// (card disappears instantly, POST resolves in the background), so a burst
// of clicks leaves several decides in flight at once. Two things went wrong:
//   * every settled POST that found the grid empty fired its own full panel
//     reload, so one burst triggered N concurrent reloads;
//   * those reloads could read the queue BEFORE the in-flight decides had
//     committed, re-rendering rows that were already spoken for. Clicking
//     one of those zombie cards hit the server's atomic
//     "WHERE status = 'pending'" guard, came back 404
//     not_found_or_already_decided, and popped a blocking _adminNotify().
// _ytrInFlight/_ytrDecided let us drop zombie rows before they ever render,
// and reloads are coalesced into one pass after the burst settles.
const _ytrInFlight = new Set();   // ids with a decide POST in flight
const _ytrDecided  = new Set();   // ids resolved this session (incl. superseded siblings)
let _ytrReloadTimer = null;
let _ytrQueueSeq = 0;             // discards stale queue renders
let _ytrSyncRetries = 0;          // bounds the "syncing decisions" retry
function _ytrRememberDecided(id) {
  _ytrDecided.add(Number(id));
  // Bound the set over a long review session.
  if (_ytrDecided.size > 5000) {
    const it = _ytrDecided.values();
    for (let i = 0; i < 1000; i++) { const x = it.next(); if (x.done) break; _ytrDecided.delete(x.value); }
  }
}
// Coalesce reloads: one refresh once the click burst settles, not one per
// click. Waits for every in-flight decide so the queue we fetch reflects
// all of them.
function _ytrScheduleReload(delay = 300) {
  if (_ytrReloadTimer) clearTimeout(_ytrReloadTimer);
  _ytrReloadTimer = setTimeout(() => {
    _ytrReloadTimer = null;
    if (_ytrInFlight.size) { _ytrScheduleReload(delay); return; }
    loadYtReview();
  }, delay);
}
// On-demand YouTube coverage stat: how many cached-master songs have no
// YouTube source (no Discogs video on the master + no pin), overall and
// for strict-Blues masters. Heavy server query — button-triggered.
async function ytrLoadCoverage(btn) {
  const out = document.getElementById("ytr-coverage-out");
  const prev = btn ? btn.textContent : null;
  if (btn) { btn.disabled = true; btn.textContent = "Computing…"; }
  if (out) { out.style.color = "var(--muted)"; out.textContent = "Computing… (scanning every cached master's tracklist — this can take a bit)"; }
  try {
    const r = await apiFetch("/api/admin/yt-review/coverage");
    if (!r.ok) { const j = await r.json().catch(() => ({})); throw new Error(j.error || `HTTP ${r.status}`); }
    const s = await r.json();
    const fmt = n => Number(n || 0).toLocaleString();
    const pct = (m, t) => t ? ((Number(m) / Number(t)) * 100).toFixed(1) : "0.0";
    if (out) {
      out.style.color = "var(--text)";
      out.innerHTML =
        `<strong>All masters:</strong> <strong style="color:#e88">${fmt(s.allMissing)}</strong> missing / ${fmt(s.allTotal)} songs `
        + `<span style="color:var(--muted)">(${pct(s.allMissing, s.allTotal)}% missing)</span>`
        + ` <span style="color:#555">·</span> `
        + `<strong>Strict Blues:</strong> <strong style="color:#e88">${fmt(s.bluesMissing)}</strong> missing / ${fmt(s.bluesTotal)} songs `
        + `<span style="color:var(--muted)">(${pct(s.bluesMissing, s.bluesTotal)}% missing)</span>`;
    }
  } catch (e) {
    if (out) { out.style.color = "#e88"; out.textContent = "Failed: " + ((e && e.message) || e); }
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = prev || "▶ Compute YT coverage"; }
  }
}
window.ytrLoadCoverage = ytrLoadCoverage;

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
          ? `<button class="admin-btn" data-sd-click="${_sdOn(function (event) { ytrStop() })}" title="Signal the worker to wind down at the next safe boundary.">■ Stop</button>`
          : `<button class="admin-btn" data-sd-click="${_sdOn(function (event) { ytrStart() })}" title="Walk pre-1960 Blues masters (earliest year first) and propose YouTube videos for tracks with no override yet. Throttled to 1 search per ${Math.round((s.throttleMs||45000)/1000)}s; daily budget ${s.dailyBudget}.">▶ Start</button>
             <button class="admin-btn" data-sd-click="${_sdOn(function (event) { ytrRestartFromTop() })}" title="Clear the walk cursor so the next Start begins at the earliest Blues master again. Doesn't touch already-approved / rejected rows or re-search tracks (per-track search log is preserved).">↻ Restart from top</button>
             ${c.pending ? `<button class="admin-btn" style="color:#8e8" data-sd-click="${_sdOn(function (event) { ytrApproveTop(this) })}" title="For every pending track, approve the candidate listed first (preferred source, then best title match) and pin it. The track's other candidates are superseded, same as clicking Approve on each.">✓ Approve top pick for all</button>` : ""}
             ${c.pending ? `<button class="admin-btn" style="color:#e88" data-sd-click="${_sdOn(function (event) { ytrDismissPending() })}" title="Throw out all ${Number(c.pending).toLocaleString()} pending tracks' candidates, forget those tracks were searched, and rewind the walk. Does not start the worker — the next ▶ Start (or scheduled daily run) re-searches them with the current search query. Approved / rejected / skipped rows are kept; rejected videos won't come back.">⟳ Dismiss pending &amp; re-search</button>` : ""}
             <button class="admin-btn" data-sd-click="${_sdOn(function (event) { ytrResetQuota() })}" title="Zero the app's daily search counter. Use ONLY when Google Cloud Console shows the 'Search Queries per day' quota has headroom — the app's count can drift high after a Pacific-midnight reset and block the worker while Google still has budget.">↺ Resync quota</button>`}
        <span style="font-size:0.78rem;color:var(--muted)">cursor: <strong style="color:var(--text)">${st.cursor_year ?? "—"}</strong> · master <strong style="color:var(--text)">${st.cursor_master_id ?? "—"}</strong></span>
        <span style="font-size:0.74rem;color:var(--muted)" title="Worker searches today / daily cap. Hard cap so manual searches always have budget left.">worker: <strong style="color:var(--text);font-variant-numeric:tabular-nums">${Number(s.searchesToday||0).toLocaleString()}</strong>/${Number(s.dailyBudget||9000).toLocaleString()}</span>
        <span style="font-size:0.74rem;color:var(--muted)" title="Project-wide YouTube quota units consumed today (worker + manual). Resets at midnight Pacific — Google's quota window.">project: <strong style="color:var(--text);font-variant-numeric:tabular-nums">${Number(s.projectUnitsToday||0).toLocaleString()}</strong>/${Number(s.projectUnitsCap||950000).toLocaleString()} u</span>
        <span style="font-size:0.78rem;color:var(--muted);margin-left:auto">${esc(st.message || "Idle.")}</span>
      </div>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:0.5rem;font-size:0.82rem">
        ${ytrTile("Pending",  c.pending,  "#7ed196", "pending")}
        ${ytrTile("Approved", c.approved, "#7ed196", "approved")}
        ${ytrTile("Rejected", c.rejected, "#e88",    "rejected")}
        ${ytrTile("Skipped",  c.skipped,  "var(--muted)", "skipped")}
        ${ytrTile("Searched", st.total_searched || 0, "var(--text)")}
        ${ytrTile("Queued",   st.total_queued   || 0, "var(--text)")}
        ${ytrTile("Auto-pinned", c.auto, (c.auto ? "#f0c674" : "var(--muted)"), "auto")}
        <div style="cursor:pointer" data-sd-click="${_sdOn(function (event) { ytrShowErrors() })}" title="Click to view recent worker errors.">
          ${ytrTile("Errors",   st.total_errors   || 0, st.total_errors ? "#e88" : "var(--muted)")}
        </div>
      </div>
    `;
    // Reflect the once-a-day auto-run state (static markup, survives polls).
    const dailyToggle = document.getElementById("ytr-daily-toggle");
    if (dailyToggle) dailyToggle.checked = !!s.dailyEnabled;
    const dailyNext = document.getElementById("ytr-daily-next");
    if (dailyNext) {
      dailyNext.textContent = s.dailyEnabled && s.nextDailyRun
        ? `next auto-run ${new Date(s.nextDailyRun).toLocaleString([], { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })}`
        : (s.dailyEnabled ? "" : "auto-run off");
    }
    // Reflect the auto-approve gate (static markup, survives polls).
    const autoToggle = document.getElementById("ytr-auto-toggle");
    if (autoToggle) autoToggle.checked = !!s.autoApprove;
    const autoNote = document.getElementById("ytr-auto-note");
    if (autoNote) {
      autoNote.textContent = s.autoApprove
        ? `Topic + artist + title + duration ±${s.autoToleranceS ?? 5}s`
        : "all candidates queued for review";
    }
    // Poll while running so the cursor + counters stay live.
    if (_ytrPollTimer) { clearTimeout(_ytrPollTimer); _ytrPollTimer = null; }
    if (running && _adminPanelVisible() && document.getElementById("panel-yt-review")?.style.display !== "none") {
      _ytrPollTimer = setTimeout(loadYtReview, 5000);
    }
    await loadYtReviewQueue();
  } catch (e) { stEl.innerHTML = `<span style="color:#e88">Failed: ${esc(e?.message || e)}</span>`; }
}
function ytrTile(label, n, color, status) {
  const onclick = status ? ` style="cursor:pointer;text-decoration:underline" data-sd-click="${_sdOn(((a0) => function (event) { ytrSetStatus(a0) })(String((status) ?? "")))}"` : "";
  return `<div${onclick ? ' ' + onclick : ''} style="border:1px solid var(--border);border-radius:5px;padding:0.4rem 0.55rem">
    <div style="font-size:0.7rem;color:var(--muted);text-transform:uppercase">${label}</div>
    <div data-ytr-count="${status || ''}" style="font-size:0.95rem;font-weight:600;color:${color};font-variant-numeric:tabular-nums">${Number(n||0).toLocaleString()}</div>
  </div>`;
}
// Nudge a count tile without a full reload (optimistic decide UI).
function _ytrAdjustCount(status, delta) {
  const el = document.querySelector(`#ytr-status [data-ytr-count="${status}"]`);
  if (!el) return;
  const cur = parseInt(String(el.textContent || "0").replace(/[^\d-]/g, ""), 10) || 0;
  el.textContent = Math.max(0, cur + delta).toLocaleString();
}
function ytrSetStatus(s) { _ytrStatus = s; _ytrPage = 0; loadYtReviewQueue(); }
window.ytrSetStatus = ytrSetStatus;
// Debounced free-text filter over the queue (track artist or title).
function ytrFilterInput(v) {
  const q = String(v || "").trim();
  if (_ytrFilterTimer) clearTimeout(_ytrFilterTimer);
  _ytrFilterTimer = setTimeout(() => {
    _ytrQuery = q; _ytrPage = 0; loadYtReviewQueue();
  }, 300);
}
window.ytrFilterInput = ytrFilterInput;
// Toggle the once-a-day auto-run.
async function ytrToggleDaily(enabled) {
  try {
    const r = await apiFetch("/api/admin/yt-review/daily", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ enabled: !!enabled }),
    });
    if (!r.ok) { _adminNotify(`Failed to update schedule: HTTP ${r.status}`); }
  } catch (e) { _adminNotify(`Failed to update schedule: ${e?.message || e}`); }
  loadYtReview();
}
window.ytrToggleDaily = ytrToggleDaily;
// Toggle auto-approve of exact Topic-channel matches.
async function ytrToggleAuto(enabled) {
  try {
    const r = await apiFetch("/api/admin/yt-review/auto", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ enabled: !!enabled }),
    });
    if (!r.ok) { _adminNotify(`Failed to update auto-approve: HTTP ${r.status}`); }
  } catch (e) { _adminNotify(`Failed to update auto-approve: ${e?.message || e}`); }
  loadYtReview();
}
window.ytrToggleAuto = ytrToggleAuto;
// Per-channel trust table: approve/reject tallies from YOUR decisions,
// plus manual trust/block overrides.
async function loadYtChannels() {
  const el = document.getElementById("ytr-channels");
  if (!el) return;
  const esc = escHtml;
  el.textContent = "Loading…";
  try {
    const r = await apiFetch("/api/admin/yt-review/channels");
    if (!r.ok) { el.innerHTML = `<span style="color:#e88">Failed: HTTP ${r.status}</span>`; return; }
    const { rows = [] } = await r.json();
    if (!rows.length) {
      el.innerHTML = `<div style="color:var(--muted);font-style:italic">No channel history yet — approve some candidates and channels will start qualifying.</div>`;
      return;
    }
    el.innerHTML = `<table style="width:100%;border-collapse:collapse;font-size:0.78rem">
      <tr style="color:var(--muted);text-align:left">
        <th style="padding:0.25rem 0.4rem">Channel</th>
        <th style="padding:0.25rem 0.4rem;text-align:right" title="Candidates from this channel you approved by hand.">You approved</th>
        <th style="padding:0.25rem 0.4rem;text-align:right" title="Candidates from this channel you rejected by hand.">You rejected</th>
        <th style="padding:0.25rem 0.4rem;text-align:right" title="Pinned automatically. Not counted toward trust.">Auto</th>
        <th style="padding:0.25rem 0.4rem">Trust</th>
        <th style="padding:0.25rem 0.4rem"></th>
      </tr>
      ${rows.map(c => {
        const id = esc(c.channel_id);
        const badge = c.state === "trusted"
          ? `<span style="color:#7ed196;font-weight:600">trusted</span> <span style="color:var(--muted)">(${esc(c.source || "")})</span>`
          : c.state === "blocked"
            ? `<span style="color:#e88;font-weight:600">blocked</span>`
            : `<span style="color:var(--muted)">—</span>`;
        return `<tr style="border-top:1px solid var(--border)">
          <td class="ytr-ch-cell" data-ch="${id}" data-title="${esc(c.channel_title || "")}" style="padding:0.25rem 0.4rem"><a href="https://www.youtube.com/channel/${id}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">${esc(c.channel_title || c.channel_id)}</a></td>
          <td style="padding:0.25rem 0.4rem;text-align:right;font-variant-numeric:tabular-nums">${Number(c.approvals || 0)}</td>
          <td style="padding:0.25rem 0.4rem;text-align:right;font-variant-numeric:tabular-nums;${Number(c.rejections) ? "color:#e88" : ""}">${Number(c.rejections || 0)}</td>
          <td style="padding:0.25rem 0.4rem;text-align:right;font-variant-numeric:tabular-nums;color:var(--muted)">${Number(c.auto_approvals || 0)}</td>
          <td style="padding:0.25rem 0.4rem">${badge}</td>
          <td style="padding:0.25rem 0.4rem;white-space:nowrap">
            <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrSetChannelTrust(a0,'trusted') })(String(id ?? "")))}" title="Always trust this channel, regardless of its tally. Survives the automatic refresh.">Trust</button>
            <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrSetChannelTrust(a0,'blocked') })(String(id ?? "")))}" title="Never auto-approve from this channel, regardless of its tally.">Block</button>
            <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrSetChannelTrust(a0,'') })(String(id ?? "")))}" title="Clear the manual override and let the tally decide again.">Auto</button>
            <button class="admin-btn" style="color:#e88" data-sd-click="${_sdOn(((a0, a1) => function (event) { ytrBanChannel(a0, a1) })(String(id ?? ""), String(c.channel_title || "")))}" title="Ban: remove this channel from ALL YouTube results, drop its pending candidates and auto-approvals.">Ban</button>
          </td>
        </tr>`;
      }).join("")}
    </table>`;
    _ytrEnrichChannels(el);
  } catch (e) { el.innerHTML = `<span style="color:#e88">Failed: ${escHtml(e?.message || e)}</span>`; }
}
window.loadYtChannels = loadYtChannels;
async function ytrSetChannelTrust(channelId, state) {
  try {
    const r = await apiFetch("/api/admin/yt-review/channel-trust", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ channelId, state: state || null }),
    });
    if (!r.ok) { _adminNotify(`Failed: HTTP ${r.status}`); return; }
    loadYtChannels();
  } catch (e) { _adminNotify(`Failed: ${e?.message || e}`); }
}
window.ytrSetChannelTrust = ytrSetChannelTrust;

// ── Channel bans ──────────────────────────────────────────────────
async function loadYtBans() {
  const el = document.getElementById("ytr-bans");
  if (!el) return;
  const esc = escHtml;
  el.textContent = "Loading…";
  try {
    const r = await apiFetch("/api/admin/yt-review/bans");
    if (!r.ok) { el.innerHTML = `<span style="color:#e88">Failed: HTTP ${r.status}</span>`; return; }
    const { rows = [] } = await r.json();
    if (!rows.length) { el.innerHTML = `<div style="color:var(--muted);font-style:italic">No banned channels.</div>`; return; }
    el.innerHTML = `<table style="width:100%;border-collapse:collapse;font-size:0.78rem">
      ${rows.map(b => {
        const id = esc(b.channel_id);
        return `<tr style="border-top:1px solid var(--border)">
          <td class="ytr-ch-cell" data-ch="${id}" data-title="${esc(b.channel_title || "")}" style="padding:0.25rem 0.4rem"><a href="https://www.youtube.com/channel/${id}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">${esc(b.channel_title || b.channel_id)}</a></td>
          <td style="padding:0.25rem 0.4rem;color:var(--muted);font-size:0.72rem">${esc(b.reason || "")}</td>
          <td style="padding:0.25rem 0.4rem;text-align:right"><button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrUnban(a0) })(String(id ?? "")))}" title="Lift the ban — the channel can appear in results again.">Unban</button></td>
        </tr>`;
      }).join("")}
    </table>`;
    _ytrEnrichChannels(el);
  } catch (e) { el.innerHTML = `<span style="color:#e88">Failed: ${escHtml(e?.message || e)}</span>`; }
}
window.loadYtBans = loadYtBans;

// Swap each channel cell for a profile: avatar, handle, subs / videos /
// views / country / start year, description, and a strip of the channel's
// videos that have been through the review queue (border = decision).
// ── AI channel hunt ─────────────────────────────────────────────────
let _ytrAiStatus = "pending";
async function loadYtAiHunt() {
  const st = document.getElementById("ytr-ai-status");
  try {
    const r = await apiFetch("/api/admin/yt-ai/status");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    const c = j.counts || {};
    const badge = document.getElementById("ytr-ai-badge");
    if (badge) badge.textContent = c.pending ? `· ${Number(c.pending).toLocaleString()} flagged` : "";
    const lr = j.lastRun;
    const when = (t) => new Date(t).toLocaleString([], { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
    if (st) st.innerHTML = [
      j.running ? `<span style="color:var(--success)">Running…</span>` : "",
      lr ? `Last run ${escHtml(when(lr.at))} (${escHtml(lr.trigger || "")}): ${lr.searches} search${lr.searches === 1 ? "" : "es"}, ${lr.flagged} flagged, ${lr.added} new${lr.error ? ` · <span style="color:#e88">${escHtml(lr.error)}</span>` : ""}` : "Not run yet.",
      j.nextRun ? `Next daily run ${escHtml(when(j.nextRun))}` : "Daily run off",
      `<button class="admin-btn" style="font-size:0.72rem;padding:0.1rem 0.45rem" data-sd-click="${_sdOn(function (event) { ytrAiRunNow(this) })}" ${j.running ? "disabled" : ""} title="Scan the review queue and run the day's searches now.">Run now</button>`,
    ].filter(Boolean).join(" · ");
    const cfg = j.config || {};
    const qEl = document.getElementById("ytr-ai-queries");
    if (qEl && document.activeElement !== qEl) qEl.value = (cfg.queries || []).join("\n");
    const dEl = document.getElementById("ytr-ai-daily"); if (dEl) dEl.checked = !!cfg.daily;
    const pEl = document.getElementById("ytr-ai-perrun"); if (pEl && document.activeElement !== pEl) pEl.value = cfg.perRun ?? 5;
    const tabs = document.getElementById("ytr-ai-tabs");
    if (tabs) {
      const tab = (key, label) => `<button class="admin-btn${_ytrAiStatus === key ? " active" : ""}" style="font-size:0.74rem;padding:0.1rem 0.5rem;${_ytrAiStatus === key ? "border-color:var(--accent);color:var(--text)" : ""}" data-sd-click="${_sdOn(function (event) { _ytrAiStatus = key; loadYtAiHunt(); })}">${label} (${Number(c[key] || 0).toLocaleString()})</button>`;
      tabs.innerHTML = tab("pending", "Flagged") + tab("banned", "Banned") + tab("ignored", "Not AI")
        + (_ytrAiStatus === "pending" && c.pending ? `<button class="admin-btn" style="margin-left:auto;font-size:0.72rem;padding:0.1rem 0.5rem;color:#e88" data-sd-click="${_sdOn(function (event) { ytrAiBanAll(this) })}" title="Ban every channel in the Flagged list below.">⛔ Ban all flagged</button>` : "");
    }
    if (j.running) setTimeout(() => { if (document.getElementById("ytr-ai-wrap")?.open) loadYtAiHunt(); }, 5000);
  } catch (e) {
    if (st) st.textContent = `Error: ${e.message || e}`;
  }
  loadYtAiList();
}
window.loadYtAiHunt = loadYtAiHunt;

let _ytrAiRows = [];
async function loadYtAiList() {
  const el = document.getElementById("ytr-ai-list");
  if (!el) return;
  try {
    const r = await apiFetch(`/api/admin/yt-ai/candidates?status=${encodeURIComponent(_ytrAiStatus)}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const rows = (await r.json()).rows || [];
    _ytrAiRows = rows;
    if (!rows.length) {
      el.innerHTML = `<div style="color:var(--muted);font-size:0.78rem;font-style:italic">${_ytrAiStatus === "pending" ? "Nothing flagged. Run a search or Run now." : "None."}</div>`;
      return;
    }
    el.innerHTML = rows.map(row => {
      const id = String(row.channel_id);
      const signals = (row.signals || []).map(s => `<span class="ytr-ai-sig">${escHtml(s)}</span>`).join("");
      const actions = _ytrAiStatus === "pending"
        ? `<button class="admin-btn" style="color:#e88" data-sd-click="${_sdOn(((a0, a1) => function (event) { ytrAiDecide(a0, "ban", this, a1) })(id, String(row.channel_title || "")))}" title="Ban this channel from all YouTube results.">⛔ Ban</button>
           <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrAiDecide(a0, "ignore", this) })(id))}" title="Not an AI channel. Won't be flagged again.">Not AI</button>`
        : _ytrAiStatus === "ignored"
          ? `<button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrAiDecide(a0, "restore", this) })(id))}" title="Put it back in the Flagged list.">↺ Restore</button>`
          : `<span style="color:var(--muted);font-size:0.74rem">banned · unban from the list above</span>`;
      return `<div class="ytr-ai-row" data-ch="${escHtml(id)}">
        <div class="ytr-ai-main">
          <div class="ytr-ch-cell" data-ch="${escHtml(id)}" data-title="${escHtml(row.channel_title || "")}"><a href="https://www.youtube.com/channel/${encodeURIComponent(id)}" target="_blank" rel="noopener">${escHtml(row.channel_title || id)}</a></div>
          <div class="ytr-ai-sigs"><span class="ytr-ai-score" title="AI score (flagged at 3+)">${Number(row.score) || 0}</span>${signals}</div>
        </div>
        <div class="ytr-ai-actions">${actions}</div>
      </div>`;
    }).join("");
    _ytrAiEnrich(el);
  } catch (e) {
    el.textContent = `Error: ${e.message || e}`;
  }
}
// Channel cards: profile from the shared profile cache, samples from the
// videos that got the channel flagged.
async function _ytrAiEnrich(root) {
  const cells = Array.from(root.querySelectorAll(".ytr-ch-cell[data-ch]"));
  const ids = Array.from(new Set(cells.map(c => c.getAttribute("data-ch")).filter(Boolean))).slice(0, 200);
  if (!ids.length) return;
  let profiles = {};
  try {
    const r = await apiFetch(`/api/admin/yt-review/channel-profiles?ids=${encodeURIComponent(ids.join(","))}`);
    if (r.ok) profiles = (await r.json()).profiles || {};
  } catch {}
  const byId = new Map(_ytrAiRows.map(row => [String(row.channel_id), row]));
  for (const cell of cells) {
    if (!cell.isConnected) continue;
    const id = cell.getAttribute("data-ch");
    const samples = (byId.get(id)?.samples || []).map(v => ({ videoId: v.videoId, title: v.title, thumb: v.thumbnail, status: "" }));
    cell.innerHTML = _ytrChannelProfileHtml(id, cell.getAttribute("data-title") || "", profiles[id], samples);
  }
}
async function ytrAiDecide(channelId, action, btn, channelTitle) {
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch("/api/admin/yt-ai/decide", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ channelId, action, channelTitle: channelTitle || "" }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || `HTTP ${r.status}`);
    btn?.closest(".ytr-ai-row")?.remove();
    if (action === "ban") { loadYtBans(); loadYtReview(); }
  } catch (e) {
    _adminNotify(`Failed: ${e?.message || e}`);
    if (btn) btn.disabled = false;
    return;
  }
  loadYtAiHunt();
}
window.ytrAiDecide = ytrAiDecide;
async function ytrAiBanAll(btn) {
  const rows = _ytrAiRows.slice();
  if (!rows.length) return;
  if (!confirm(`Ban all ${rows.length} flagged channel${rows.length === 1 ? "" : "s"}? They're removed from every YouTube result; unban from the Banned channels list.`)) return;
  if (btn) btn.disabled = true;
  let n = 0;
  for (const row of rows) {
    try {
      const r = await apiFetch("/api/admin/yt-ai/decide", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ channelId: row.channel_id, action: "ban", channelTitle: row.channel_title || "" }),
      });
      if (r.ok) n++;
    } catch {}
    if (btn) btn.textContent = `Banning… ${n}/${rows.length}`;
  }
  _adminNotify(`Banned ${n} of ${rows.length} channels.`);
  loadYtBans(); loadYtReview(); loadYtAiHunt();
}
window.ytrAiBanAll = ytrAiBanAll;
async function ytrAiSearch(btn) {
  const q = (document.getElementById("ytr-ai-q")?.value || "").trim();
  if (!q) return;
  const recent = !!document.getElementById("ytr-ai-recent")?.checked;
  const st = document.getElementById("ytr-ai-status");
  if (btn) btn.disabled = true;
  if (st) st.textContent = `Searching "${q}"…`;
  try {
    const r = await apiFetch("/api/admin/yt-ai/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ q, recent }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || `HTTP ${r.status}`);
    _adminNotify(`"${q}": ${j.videos} videos from ${j.channels} channels · ${j.flagged} flagged (${j.added} new).`);
    _ytrAiStatus = "pending";
  } catch (e) {
    _adminNotify(`Search failed: ${e?.message || e}`);
  } finally {
    if (btn) btn.disabled = false;
    loadYtAiHunt();
  }
}
window.ytrAiSearch = ytrAiSearch;
async function ytrAiRunNow(btn) {
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch("/api/admin/yt-ai/run", { method: "POST" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || `HTTP ${r.status}`);
  } catch (e) { _adminNotify(`Run failed: ${e?.message || e}`); }
  setTimeout(loadYtAiHunt, 1500);
}
window.ytrAiRunNow = ytrAiRunNow;
async function ytrAiSaveConfig(btn) {
  const queries = (document.getElementById("ytr-ai-queries")?.value || "").split("\n").map(s => s.trim()).filter(Boolean);
  const perRun = Number(document.getElementById("ytr-ai-perrun")?.value);
  const daily = !!document.getElementById("ytr-ai-daily")?.checked;
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch("/api/admin/yt-ai/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ queries, perRun, daily }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || `HTTP ${r.status}`);
    _adminNotify("Saved.");
  } catch (e) {
    _adminNotify(`Save failed: ${e?.message || e}`);
  } finally {
    if (btn) btn.disabled = false;
    loadYtAiHunt();
  }
}
window.ytrAiSaveConfig = ytrAiSaveConfig;

async function _ytrEnrichChannels(root) {
  const cells = Array.from(root.querySelectorAll(".ytr-ch-cell[data-ch]"));
  const ids = Array.from(new Set(cells.map(c => c.getAttribute("data-ch")).filter(Boolean)));
  if (!ids.length) return;
  let j;
  try {
    const r = await apiFetch(`/api/admin/yt-review/channel-profiles?ids=${encodeURIComponent(ids.join(","))}`);
    if (!r.ok) return;
    j = await r.json();
  } catch { return; }
  for (const cell of cells) {
    if (!cell.isConnected) continue;
    const id = cell.getAttribute("data-ch");
    cell.innerHTML = _ytrChannelProfileHtml(id, cell.getAttribute("data-title") || "", j.profiles?.[id], j.samples?.[id] || []);
  }
}
function _ytrChannelProfileHtml(id, fallbackTitle, p, samples) {
  const esc = escHtml;
  const compact = n => (n == null || !Number.isFinite(Number(n))) ? null
    : new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(Number(n));
  const url = `https://www.youtube.com/channel/${encodeURIComponent(id)}`;
  const title = (p && p.title) || fallbackTitle || id;
  const meta = [];
  if (p && !p.gone) {
    const subs = compact(p.subscriberCount);
    const vids = compact(p.videoCount);
    const views = compact(p.viewCount);
    meta.push(subs != null ? `${subs} subs` : "subs hidden");
    if (vids != null) meta.push(`${vids} videos`);
    if (views != null) meta.push(`${views} views`);
    if (p.country) meta.push(esc(p.country));
    if (p.publishedAt) meta.push(`since ${String(p.publishedAt).slice(0, 4)}`);
  }
  const desc = p && p.description ? String(p.description).replace(/\s+/g, " ").trim() : "";
  const statusColor = s => s === "approved" ? "#7ed196" : s === "rejected" ? "#e88" : "var(--border)";
  const strip = samples.length
    ? `<div class="ytr-ch-samples">${samples.map(v => `<a href="https://www.youtube.com/watch?v=${encodeURIComponent(v.videoId)}" target="_blank" rel="noopener" title="${esc(v.title || "")} · ${esc(v.status || "")}"><img src="${esc(v.thumb || "")}" alt="" loading="lazy" style="border-color:${statusColor(v.status)}"></a>`).join("")}</div>`
    : "";
  return `<div class="ytr-ch">
    ${p && p.thumbnail
      ? `<a href="${url}" target="_blank" rel="noopener"><img class="ytr-ch-av" src="${esc(p.thumbnail)}" alt="" loading="lazy"></a>`
      : `<div class="ytr-ch-av ytr-ch-av-empty"></div>`}
    <div class="ytr-ch-body">
      <div class="ytr-ch-name"><a href="${url}" target="_blank" rel="noopener">${esc(title)}</a>${p && p.handle ? ` <span class="ytr-ch-handle">${esc(p.handle)}</span>` : ""}</div>
      ${p && p.gone
        ? `<div class="ytr-ch-meta" style="color:#e88">Channel unavailable (closed or terminated)</div>`
        : meta.length ? `<div class="ytr-ch-meta">${meta.join(" · ")}</div>` : ""}
      ${desc ? `<div class="ytr-ch-desc" title="${esc(desc)}">${esc(desc.length > 160 ? desc.slice(0, 160) + "…" : desc)}</div>` : ""}
      ${strip}
    </div>
  </div>`;
}

async function _ytrPostBan(payload) {
  const r = await apiFetch("/api/admin/yt-review/bans", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) { _adminNotify(`Ban failed: ${j.message || j.error || r.status}`); return false; }
  const bits = [];
  if (j.supersededPending) bits.push(`${j.supersededPending} pending dropped`);
  if (j.removedAutoApproved) bits.push(`${j.removedAutoApproved} auto-approval${j.removedAutoApproved === 1 ? "" : "s"} removed`);
  const who = payload.channelTitle || payload.channelId || "channel";
  showToast(`Banned ${who}${bits.length ? ` · ${bits.join(", ")}` : ""}`, "info", 5000);
  return true;
}
async function ytrBanChannel(channelId, channelTitle) {
  if (await _ytrPostBan({ channelId, channelTitle })) {
    loadYtBans(); loadYtChannels(); loadYtReview();
  }
}
window.ytrBanChannel = ytrBanChannel;
async function ytrBanFromInput() {
  const inp = document.getElementById("ytr-ban-input");
  const val = (inp?.value || "").trim();
  if (!val) return;
  if (await _ytrPostBan({ url: val })) {
    if (inp) inp.value = "";
    loadYtBans();
  }
}
window.ytrBanFromInput = ytrBanFromInput;
async function ytrUnban(channelId) {
  try {
    const r = await apiFetch("/api/admin/yt-review/bans", {
      method: "DELETE", headers: { "content-type": "application/json" },
      body: JSON.stringify({ channelId }),
    });
    if (!r.ok) { _adminNotify(`Unban failed: HTTP ${r.status}`); return; }
    loadYtBans();
  } catch (e) { _adminNotify(`Unban failed: ${e?.message || e}`); }
}
window.ytrUnban = ytrUnban;
async function ytrStart() {
  try {
    const r = await apiFetch("/api/admin/yt-review/start", { method: "POST" });
    if (r.status === 409) { _adminNotify("Already running."); loadYtReview(); return; }
    if (!r.ok) { _adminNotify(`Start failed: HTTP ${r.status}`); return; }
    loadYtReview();
  } catch (e) { _adminNotify(`Start failed: ${e?.message || e}`); }
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
    if (!r.ok) { _adminNotify(`Reset failed: ${j.error || r.status}`); return; }
    // Fire the worker off immediately. Prior version left the cursor
    // reset but idle, which surprised users who read "restart" as
    // "reset AND start" — matches how other bulk workers behave.
    const startR = await apiFetch("/api/admin/yt-review/start", { method: "POST" });
    if (startR.status === 409) {
      // Rare — someone else clicked Start in the ~1s gap. Not worth alerting.
    } else if (!startR.ok) {
      _adminNotify(`Cursor reset but Start failed: HTTP ${startR.status}. Click ▶ Start to run.`);
    }
    if (alsoResetSearchLog) {
      _adminNotify(`Cursor cleared and worker started. Also wiped ${j.clearedSearches ?? 0} per-track search log rows.`);
    }
    loadYtReview();
  } catch (e) { _adminNotify(`Reset failed: ${e?.message || e}`); }
}
window.ytrRestartFromTop = ytrRestartFromTop;
async function ytrDismissPending() {
  if (!confirm("Dismiss ALL pending tracks and re-search them?\n\nEvery pending candidate is removed, those tracks are marked unsearched, and the walk rewinds to the top. The worker does NOT start — click ▶ Start when you want the re-search to run. Approved / rejected / skipped rows are kept.")) return;
  try {
    const r = await apiFetch("/api/admin/yt-review/dismiss-pending", { method: "POST" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { _adminNotify(`Dismiss failed: ${j.error || r.status}`); return; }
    if (typeof showToast === "function") {
      showToast(`Dismissed ${j.dismissed ?? 0} candidates across ${j.tracks ?? 0} tracks — click ▶ Start to re-search`, "info", 5000);
    }
    loadYtReview();
  } catch (e) { _adminNotify(`Dismiss failed: ${e?.message || e}`); }
}
window.ytrDismissPending = ytrDismissPending;
async function ytrApproveTop(btn) {
  if (!confirm("Approve the top suggestion for EVERY pending track?\n\nFor each track, the candidate listed first (preferred source, then best title match) is approved and pinned; the track's other candidates are superseded. Undo means deleting approvals one by one.")) return;
  if (btn) btn.disabled = true;
  // The server approves a batch per call; keep calling until nothing is
  // left, or a batch makes no progress (so a stuck row can't loop).
  let approved = 0, failed = 0;
  try {
    for (;;) {
      const r = await apiFetch("/api/admin/yt-review/approve-top?limit=200", { method: "POST" });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) { _adminNotify(`Approve stopped after ${approved.toLocaleString()} tracks: ${j.error || r.status}`); return; }
      approved += Number(j.approved || 0);
      failed += Number(j.failed || 0);
      const remaining = Number(j.remaining || 0);
      if (btn) btn.textContent = `Approving… ${approved.toLocaleString()} done${remaining ? `, ${remaining.toLocaleString()} to go` : ""}`;
      if (!remaining || !j.approved) break;
    }
    _adminNotify(`Approved the top pick for ${approved.toLocaleString()} tracks${failed ? ` (${failed} failed and stay pending)` : ""}.`);
  } catch (e) {
    _adminNotify(`Approve stopped after ${approved.toLocaleString()} tracks: ${e?.message || e}`);
  } finally {
    loadYtReview();
  }
}
window.ytrApproveTop = ytrApproveTop;
async function ytrResetQuota() {
  if (!confirm("Zero the app's daily YouTube search counter?\n\nOnly do this when Google Cloud Console shows the 'Search Queries per day' quota still has headroom. If Google is actually near 100/day, resetting here will just make the worker hit Google's real limit and log 403 errors.")) return;
  try {
    const r = await apiFetch("/api/admin/yt-review/quota/reset", { method: "POST" });
    if (r.status === 409) { _adminNotify("Stop the worker first, then resync."); return; }
    if (!r.ok) { const j = await r.json().catch(() => ({})); _adminNotify(`Resync failed: ${j.error || r.status}`); return; }
    loadYtReview();
  } catch (e) { _adminNotify(`Resync failed: ${e?.message || e}`); }
}
window.ytrResetQuota = ytrResetQuota;
async function ytrApplyTrust() {
  const btn = document.getElementById("ytr-apply-trust-btn");
  const note = document.getElementById("ytr-apply-trust-note");
  if (!confirm("Re-check every pending candidate against the current trust picture?\n\nTrust is refreshed from your approve/reject history first. Pending rows on a Topic or trusted channel that pass the exact-title / duration / embeddable gate get auto-pinned; banned-channel rows are cleared. Nothing is force-approved. This re-fetches video details, so it spends a little YouTube quota.")) return;
  if (btn) { btn.disabled = true; btn.textContent = "Applying…"; }
  if (note) note.textContent = "";
  try {
    const r = await apiFetch("/api/admin/yt-review/apply-trust", { method: "POST" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (note) { note.style.color = "#e88"; note.textContent = `Failed: ${j.error || r.status}`; }
      return;
    }
    if (note) {
      note.style.color = "var(--muted)";
      note.textContent = `Auto-pinned ${j.approved} of ${j.channelEligible} trusted/Topic pending (${j.scannedPending} scanned)${j.rejectedBanned ? `, cleared ${j.rejectedBanned} banned` : ""}.`;
    }
    loadYtReview();
  } catch (e) {
    if (note) { note.style.color = "#e88"; note.textContent = `Failed: ${e?.message || e}`; }
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "✓ Apply trust to pending"; }
  }
}
window.ytrApplyTrust = ytrApplyTrust;
async function ytrStop() {
  try {
    await apiFetch("/api/admin/yt-review/stop", { method: "POST" });
    loadYtReview();
  } catch (e) { _adminNotify(`Stop failed: ${e?.message || e}`); }
}
window.ytrStop = ytrStop;
async function loadYtReviewQueue() {
  const el = document.getElementById("ytr-queue");
  if (!el) return;
  const esc = escHtml;   // canonical escaper (shared.js) — escapes & < > " '
  try {
    const params = new URLSearchParams({ status: _ytrStatus, limit: String(_YTR_LIMIT), offset: String(_ytrPage * _YTR_LIMIT) });
    if (_ytrQuery) params.set("q", _ytrQuery);
    // Stamp this load; a slower earlier response must not paint over a
    // newer one (the 5s running-poll and a decide reconcile can overlap).
    const _seq = ++_ytrQueueSeq;
    const r = await apiFetch(`/api/admin/yt-review/queue?${params}`);
    if (_seq !== _ytrQueueSeq) return;
    if (!r.ok) { el.innerHTML = `<span style="color:#e88">Queue load failed: HTTP ${r.status}</span>`; return; }
    const { rows = [], total = 0 } = await r.json();
    if (_seq !== _ytrQueueSeq) return;
    // Drop rows we've already decided (or are deciding) this session. A
    // reload can race an uncommitted decide and hand back rows that are
    // already spoken for; rendering them invites a click that 404s.
    // Only meaningful for the pending view — in the approved/rejected views
    // these ids are legitimately what you want to see.
    let visible = rows;
    if (_ytrStatus === "pending" || _ytrStatus === "auto") {
      visible = rows.filter(row => {
        const rid = Number(row.id);
        return !_ytrDecided.has(rid) && !_ytrInFlight.has(rid);
      });
    }
    if (!visible.length) {
      // Everything on this page was filtered out: the decides haven't
      // committed yet. Say so and try again shortly instead of flashing a
      // misleading "No pending rows".
      if (rows.length && _ytrSyncRetries < 5) {
        _ytrSyncRetries++;
        el.innerHTML = `<div style="color:var(--muted);padding:0.6rem 0;font-style:italic">Syncing decisions…</div>`;
        _ytrScheduleReload(700);
      } else if (rows.length) {
        // Gave up waiting — show the page as-is rather than an empty panel.
        _ytrSyncRetries = 0;
        el.innerHTML = ytrGroupedHtml(rows);
        ytrRenderPager(total);
        return;
      } else {
        el.innerHTML = `<div style="color:var(--muted);padding:0.6rem 0;font-style:italic">No ${_ytrStatus === "auto" ? "auto-pinned" : _ytrStatus} rows.</div>`;
      }
      ytrRenderPager(total);
      return;
    }
    _ytrSyncRetries = 0;
    const autoBar = _ytrStatus === "auto"
      ? `<div style="display:flex;gap:0.5rem;align-items:center;margin-bottom:0.5rem;font-size:0.76rem;color:var(--muted)">
          Pins the auto-approver made. Keep moves one to Approved; Remove deletes the pin and rejects it.
          <button class="admin-btn" style="margin-left:auto" data-sd-click="${_sdOn(function (event) { ytrKeepAutoPage(this) })}" title="Keep every auto-pin shown on this page.">✓ Keep all on page</button>
        </div>`
      : "";
    el.innerHTML = autoBar + ytrGroupedHtml(visible);
    ytrRenderPager(total);
  } catch (e) { el.innerHTML = `<span style="color:#e88">Queue load failed: ${esc(e?.message || e)}</span>`; }
}
// Group consecutive queue rows by (master_id, track_position) — the
// server already orders candidates for the same track contiguously — and
// wrap each track's candidates in a bordered block with a header showing
// the song and a count badge, so it's obvious how many candidates compete
// for each track.
function ytrGroupedHtml(rows) {
  const decode = s => String(s ?? "").replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&apos;/g,"'").replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/&amp;/g,"&");
  const esc = s => decode(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/'/g,"&#39;").replace(/"/g,"&quot;");
  const key = r => `${r.master_id}||${r.track_position ?? ""}`;
  let html = "";
  for (let i = 0; i < rows.length; ) {
    const k = key(rows[i]);
    const group = [];
    while (i < rows.length && key(rows[i]) === k) group.push(rows[i++]);
    const first = group[0];
    const n = group.length;
    const yr = first.master_year || "?";
    // "Reject all" / "Ban all" only make sense while reviewing the pending queue.
    const nCh = new Set(group.map(r => r.candidate_channel_id).filter(Boolean)).size;
    const rejectAll = _ytrStatus === "pending"
      ? `<button class="admin-btn" data-sd-click="${_sdOn(function (event) { ytrRejectGroup(this) })}" title="Reject all ${n} candidate${n === 1 ? "" : "s"} for this track — none of them are right." style="margin-left:auto;flex-shrink:0;font-size:0.7rem;padding:0.1rem 0.45rem;color:#e88;border-color:#5a2b2b">✗ Reject all</button>`
        + (nCh ? `<button class="admin-btn" data-sd-click="${_sdOn(function (event) { ytrBanGroup(this) })}" title="Ban all ${nCh} channel${nCh === 1 ? "" : "s"} behind this track's candidates from ALL YouTube results, and reject the rest." style="flex-shrink:0;font-size:0.7rem;padding:0.1rem 0.45rem;color:#e88;border-color:#5a2b2b">⛔ Ban all</button>` : "")
      : "";
    const head = `<div class="ytr-group-head">
      <span class="ytr-group-count" title="${n} candidate${n === 1 ? "" : "s"} for this track">${n}</span>
      <span class="ytr-group-song"><span style="color:var(--muted);font-weight:normal">${esc(String(yr))} · ${esc(first.track_position || "")}</span> ${esc(first.track_title || "")} <span style="color:var(--muted);font-weight:normal">— ${esc(first.track_artist || "")}</span></span>
      ${rejectAll}
    </div>`;
    // Track identity rides on data-* attributes so ytrRejectGroup can read it
    // off the DOM instead of interpolating a position string into inline JS.
    html += `<div class="ytr-group" data-ytr-master="${esc(String(first.master_id ?? ""))}" data-ytr-track="${esc(String(first.track_position ?? ""))}">${head}${group.map(ytrRowHtml).join("")}</div>`;
  }
  return html;
}
// "just now" / "5 min ago" / "3 h ago" / "2 d ago" for decision times.
function _ytrAgo(ts) {
  const s = Math.max(0, (Date.now() - new Date(ts).getTime()) / 1000);
  if (!Number.isFinite(s)) return "";
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
  return `${Math.floor(s / 86400)} d ago`;
}
function ytrRowHtml(r) {
  const decode = s => String(s ?? "").replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&apos;/g,"'").replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/&amp;/g,"&");
  const esc = s => decode(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/'/g,"&#39;").replace(/"/g,"&quot;");
  const ytUrl = `https://www.youtube.com/watch?v=${esc(r.candidate_video_id)}`;
  const score = r.title_score != null ? Number(r.title_score).toFixed(2) : "—";
  // Highlight a 4-digit year (19xx/20xx) inside the escaped candidate
  // title so the pressing year jumps out when scanning the queue. Safe
  // to run on the escaped string — years contain no HTML-special chars.
  const candTitleHtml = esc(r.candidate_title || "")
    .replace(/\b(?:19|20)\d{2}\b/g, m => `<span style="color:#f0c674;font-weight:700">${m}</span>`);
  // Color-code the auto-approve reason (each distinct reason a unique
  // hue) and bold any "trusted_channel_but…" reason so the trusted-path
  // near-misses stand out.
  const reasonHtml = r.auto_reason ? (() => {
    const st = _ytrReasonStyle(r.auto_reason);
    return ` <span style="color:#555">·</span> <span style="color:${st.color}${st.bold ? ";font-weight:700" : ""}" title="Why the auto-approve gate did or didn't take this candidate.">${esc(r.auto_reason)}</span>`;
  })() : "";
  const showActions = _ytrStatus === "pending";
  const showDelete = _ytrStatus === "approved";
  const showUnreject = _ytrStatus === "rejected";
  const showAuto = _ytrStatus === "auto";
  const yr = r.master_year || "?";
  return `<div class="ytr-card" data-ytr-id="${r.id}" data-ytr-ch="${esc(r.candidate_channel_id || "")}" data-ytr-ch-title="${esc(r.candidate_channel_title || "")}" style="border-radius:6px;padding:0.6rem 0.75rem;display:grid;grid-template-columns:64px 64px 1fr auto;gap:0.7rem;align-items:center">
    ${r.master_cover_url
      ? `<img src="${esc(r.master_cover_url)}" alt="" style="width:64px;height:64px;object-fit:cover;border-radius:4px;background:var(--border)" loading="lazy">`
      : `<div style="width:64px;height:64px;border-radius:4px;background:rgba(255,255,255,0.04)"></div>`}
    ${r.candidate_thumbnail_url
      ? `<a href="${esc(ytUrl)}" target="_blank" rel="noopener" title="Open on YouTube"><img src="${esc(r.candidate_thumbnail_url)}" alt="" style="width:64px;height:64px;object-fit:cover;border-radius:4px;background:var(--border)" loading="lazy"></a>`
      : `<div style="width:64px;height:64px;border-radius:4px;background:rgba(255,255,255,0.04)"></div>`}
    <div style="min-width:0">
      <div style="font-size:0.86rem;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><span style="color:var(--muted);font-weight:normal">${yr} · ${esc(r.track_position || "")}</span> ${esc(r.track_title || "")} <span style="color:var(--muted);font-weight:normal">— ${esc(r.track_artist || "")}</span></div>
      <div style="font-size:0.78rem;color:var(--muted);margin-top:0.15rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><a href="${esc(ytUrl)}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">${candTitleHtml}</a> <span style="color:#555">·</span> ${esc(r.candidate_channel_title || "")}${r.is_topic_channel ? ` <span style="color:#7ed196;font-weight:600" title="Official auto-generated artist channel — label-delivered audio.">TOPIC</span>` : ""}${r.preferred_match ? ` <span style="color:#f0c674;font-weight:600" title="Matches preferred source &quot;${esc(r.preferred_match)}&quot; (channel, title or description).">PREFERRED</span>` : ""} <span style="color:#555">·</span> match ${score}${ytrDurationHtml(r)}</div>
      <div style="font-size:0.72rem;color:var(--muted);margin-top:0.15rem">master #${r.master_id} ${r.reviewed_by ? `· decided by ${esc(r.reviewed_by)}` : ""}${r.reviewed_at && _ytrStatus !== "pending" ? ` · <span title="${esc(new Date(r.reviewed_at).toLocaleString())}">${esc(_ytrAgo(r.reviewed_at))}</span>` : ""}${reasonHtml}</div>
      ${r.search_query ? `<div class="ytr-card-q" title="YouTube search that surfaced this candidate: ${esc(r.search_query)}">q: ${esc(r.search_query)}</div>` : ""}
    </div>
    ${showActions
      ? `<div style="display:flex;flex-direction:column;gap:0.3rem;align-items:flex-end">
          <div style="display:flex;gap:0.3rem;align-items:center">
            <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrDecide(a0,'approve',this) })(_sdLit(r.id)))}" title="Approve and pin this video to the track as a master override.">✓ Approve</button>
            <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrDecide(a0,'reject',this) })(_sdLit(r.id)))}" title="Reject this candidate. Worker won't re-propose this video on this track.">✗ Reject</button>
            <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrDecide(a0,'skip',this) })(_sdLit(r.id)))}" title="Skip — neither pin nor reject, just remove from the pending queue. Track can still be re-proposed.">Skip</button>
            ${r.candidate_channel_id ? `<button class="admin-btn" style="color:#e88" data-sd-click="${_sdOn(((a0, a1) => function (event) { ytrBanChannel(a0, a1) })(String(r.candidate_channel_id ?? ""), String(r.candidate_channel_title || "")))}" title="Ban this channel from ALL YouTube results everywhere.">⛔ Ban ch.</button>` : ""}
          </div>
          <div style="display:flex;gap:0.3rem;align-items:center">
            <input type="text" id="ytr-custom-${r.id}" placeholder="paste YouTube URL or ID" style="width:14rem;padding:0.15rem 0.35rem;font-size:0.75rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:3px" data-sd-keydown="${_sdOn(((a0) => function (event) { if(event.key==='Enter'){event.preventDefault();ytrCustomApprove(a0);} })(_sdLit(r.id)))}">
            <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrCustomApprove(a0) })(_sdLit(r.id)))}" title="Pin your own URL to this track instead of the worker's candidate. Overwrites any existing override.">↳ Use my URL</button>
          </div>
        </div>`
      : showAuto
        ? `<div style="display:flex;gap:0.3rem;align-items:center">
            <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrKeepAuto([a0], this) })(_sdLit(r.id)))}" title="Keep this pin. Moves it to Approved and counts as your approval for the channel's trust.">✓ Keep</button>
            <button class="admin-btn" style="color:#e88" data-sd-click="${_sdOn(((a0) => function (event) { ytrRemoveAuto(a0, this) })(_sdLit(r.id)))}" title="Delete the pin and mark the candidate rejected. The track becomes unpinned.">✗ Remove</button>
          </div>`
      : showDelete
        ? `<div style="display:flex;gap:0.3rem;align-items:center">
            <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrDeleteApproval(a0) })(_sdLit(r.id)))}" title="Remove the override this approval created and mark the candidate rejected.">🗑 Delete</button>
          </div>`
      : showUnreject
        ? `<div style="display:flex;gap:0.3rem;align-items:center">
            <button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { ytrUnreject(a0, this) })(_sdLit(r.id)))}" title="Move this candidate back to Pending so you can approve it.">↺ Un-reject</button>
          </div>`
        : `<div style="color:var(--muted);font-size:0.74rem">${esc(r.status || "")}</div>`}
  </div>`;
}
// "3:12 vs 3:14" — Discogs track time against the YouTube duration,
// green when they agree closely enough to have cleared the auto gate.
function ytrDurationHtml(r) {
  const fmt = (n) => {
    const s = Number(n);
    if (!Number.isFinite(s) || s <= 0) return null;
    return `${Math.floor(s / 60)}:${String(Math.round(s % 60)).padStart(2, "0")}`;
  };
  const want = fmt(r.track_duration_seconds);
  const got = fmt(r.candidate_duration_seconds);
  if (!want && !got) return "";
  const color = r.duration_ok === true ? "#7ed196" : r.duration_ok === false ? "#e88" : "var(--muted)";
  return ` <span style="color:#555">·</span> <span style="color:${color}" title="Discogs track time vs YouTube duration.">${want || "—"} / ${got || "—"}</span>`;
}
// Map an auto-approve reason to a display style. Each distinct gate
// reason gets its own hue so the queue is scannable at a glance; the
// positive (auto-pinned) reasons read green, and any trusted-path
// near-miss ("trusted_channel_but…") is bolded per request. The reason
// key is the token before ":" / "(" — the tail (quoted values, ±Ns)
// varies per row but the category doesn't.
function _ytrReasonStyle(reason) {
  const s = String(reason || "");
  // Auto-pinned verdicts: "topic+…" / "trusted+…".
  if (/^(?:topic|trusted)\+/.test(s)) return { color: "#7ed196", bold: false };
  const key = s.split(/[:(]/)[0].trim();
  const MAP = {
    not_topic_or_trusted_channel:          "#8a8f98",
    no_track_artist:                       "#c9a24b",
    artist_mismatch:                       "#e0894f",
    trusted_channel_but_artist_not_in_title: "#e05c5c",
    title_not_exact:                       "#d76d9e",
    version_marker:                        "#b07fd8",
    not_embeddable:                        "#9a7b5c",
    region_blocked:                        "#6a9fd8",
    no_discogs_duration:                   "#5fb0c9",
    no_video_duration:                     "#4f9ab0",
  };
  let color = MAP[key];
  if (!color && /^duration_off_by_/.test(key)) color = "#e0704f";
  const bold = /^trusted_channel_but/.test(key);
  return { color: color || "var(--muted)", bold };
}
function ytrRenderPager(total) {
  const el = document.getElementById("ytr-pager");
  if (!el) return;
  const pages = Math.max(1, Math.ceil(total / _YTR_LIMIT));
  const cur = _ytrPage + 1;
  if (pages <= 1) { el.innerHTML = `<span style="color:var(--muted)">${total} row${total === 1 ? "" : "s"}</span>`; return; }
  el.innerHTML = `
    <button class="admin-btn" ${cur <= 1 ? "disabled" : ""} data-sd-click="${_sdOn(((a0) => function (event) { ytrPage(a0) })(_sdLit(_ytrPage - 1)))}">‹ Prev</button>
    <span style="color:var(--muted)">Page ${cur} / ${pages} · ${total.toLocaleString()} ${_ytrStatus === "auto" ? "auto-pinned" : _ytrStatus}</span>
    <button class="admin-btn" ${cur >= pages ? "disabled" : ""} data-sd-click="${_sdOn(((a0) => function (event) { ytrPage(a0) })(_sdLit(_ytrPage + 1)))}">Next ›</button>
  `;
}
function ytrPage(p) { _ytrPage = Math.max(0, p); loadYtReviewQueue(); }
window.ytrPage = ytrPage;
async function ytrDecide(id, action, btn) {
  id = Number(id);
  // Never send two decisions for the same row. The server's decide is
  // atomic ("WHERE id = $1 AND status = 'pending'"), so a second POST comes
  // back 404 not_found_or_already_decided — which is exactly the error a
  // fast clicker used to see when a mid-burst reload re-rendered a card that
  // was already spoken for. Swallow it at the source instead.
  if (_ytrInFlight.has(id) || _ytrDecided.has(id)) {
    const dupe = btn ? btn.closest(".ytr-card") : null;
    if (dupe) dupe.remove();
    return;
  }
  _ytrInFlight.add(id);

  // OPTIMISTIC: advance the UI the instant you click, resolve the decision in
  // the background. The old flow awaited the decide POST and then a full panel
  // reload (status fetch + queue fetch = 3 sequential round-trips), ~3s per
  // click. Now the card disappears immediately and we only reload once the
  // click burst has settled (or on error, to reconcile).
  const card = btn ? btn.closest(".ytr-card") : null;
  const group = card ? card.closest(".ytr-group") : null;
  if (card) {
    if (action === "approve" && group) {
      // Approving pins a video → every competing candidate for that song is
      // superseded server-side, so drop the whole group. Remember the sibling
      // ids as decided too: otherwise a reload that raced the supersede could
      // resurrect them as clickable cards that now 404.
      group.querySelectorAll(".ytr-card[data-ytr-id]").forEach(el => {
        const sib = Number(el.getAttribute("data-ytr-id"));
        if (Number.isFinite(sib) && sib !== id) _ytrRememberDecided(sib);
      });
      group.remove();
    } else {
      card.remove();
      if (group && !group.querySelector(".ytr-card")) group.remove();
    }
    // Optimistic count nudge so the tiles feel live between batch reloads.
    _ytrAdjustCount("pending", -1);
    const done = { approve: "approved", reject: "rejected", skip: "skipped" }[action];
    if (done) _ytrAdjustCount(done, +1);
  }
  try {
    const r = await apiFetch("/api/admin/yt-review/decide", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id, action }),
    });
    if (r.ok) {
      _ytrRememberDecided(id);
    } else {
      const body = await r.json().catch(() => ({}));
      const already = r.status === 404 || body?.error === "not_found_or_already_decided";
      // "Already decided" is benign in a fast-click flow (a duplicate or a
      // sibling the approve already superseded) — reconcile silently rather
      // than interrupting the review with a modal alert. Any OTHER failure
      // (5xx) may have left the row pending, so forget it and let the
      // reconcile reload bring it back rather than hiding it forever.
      if (already) _ytrRememberDecided(id); else _ytrDecided.delete(id);
      if (!already && typeof showToast === "function") {
        showToast(`${action} failed: ${body?.error || `HTTP ${r.status}`}`, "error", 5000);
      }
    }
  } catch (e) {
    // Genuine network/transport failure: the row is probably still pending,
    // so forget it and let the reconcile reload bring it back.
    _ytrDecided.delete(id);
    if (typeof showToast === "function") showToast(`${action} failed: ${e?.message || e}`, "error", 5000);
  } finally {
    _ytrInFlight.delete(id);
    // Refresh when the visible batch is exhausted, or to reconcile after a
    // failure. Coalesced, so a 20-click burst still costs one reload.
    if (!document.querySelector("#ytr-queue .ytr-card")) _ytrScheduleReload();
  }
}
window.ytrDecide = ytrDecide;
// Reject every candidate for one track in a single call. Same optimistic
// pattern as ytrDecide — the group disappears immediately — but it resolves
// server-side in ONE atomic statement rather than N racing per-row decides.
async function ytrRejectGroup(btn) {
  const group = btn ? btn.closest(".ytr-group") : null;
  if (!group) return;
  const masterId = Number(group.getAttribute("data-ytr-master"));
  const trackPosition = group.getAttribute("data-ytr-track") ?? "";
  if (!Number.isFinite(masterId)) return;
  const ids = Array.from(group.querySelectorAll(".ytr-card[data-ytr-id]"))
    .map(el => Number(el.getAttribute("data-ytr-id")))
    .filter(Number.isFinite);
  // Don't fire twice for the same group, and don't fight a per-row decide
  // that's already resolving one of these candidates.
  if (group.dataset.ytrRejecting === "1") return;
  group.dataset.ytrRejecting = "1";
  const fresh = ids.filter(id => !_ytrInFlight.has(id) && !_ytrDecided.has(id));

  group.remove();
  fresh.forEach(_ytrRememberDecided);
  if (fresh.length) {
    _ytrAdjustCount("pending", -fresh.length);
    _ytrAdjustCount("rejected", +fresh.length);
  }
  try {
    const r = await apiFetch("/api/admin/yt-review/reject-track", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ masterId, trackPosition }),
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) {
      // Un-remember so the reconcile reload can bring the track back.
      fresh.forEach(id => _ytrDecided.delete(id));
      if (typeof showToast === "function") {
        showToast(`Reject all failed: ${body?.error || `HTTP ${r.status}`}`, "error", 5000);
      }
    } else if (typeof body.rejected === "number" && body.rejected !== fresh.length) {
      // The server also rejected candidates that weren't on this page (a
      // track's candidates can straddle a pagination boundary). Correct the
      // optimistic tile nudge with the real number.
      const delta = body.rejected - fresh.length;
      _ytrAdjustCount("pending", -delta);
      _ytrAdjustCount("rejected", +delta);
    }
  } catch (e) {
    fresh.forEach(id => _ytrDecided.delete(id));
    if (typeof showToast === "function") showToast(`Reject all failed: ${e?.message || e}`, "error", 5000);
  } finally {
    if (!document.querySelector("#ytr-queue .ytr-card")) _ytrScheduleReload();
  }
}
window.ytrRejectGroup = ytrRejectGroup;
// Ban every distinct channel behind one track's candidates, then reject
// whatever is left for the track (candidates with no channel id). Banning
// already supersedes each channel's pending rows server-side.
async function ytrBanGroup(btn) {
  const group = btn ? btn.closest(".ytr-group") : null;
  if (!group || group.dataset.ytrRejecting === "1") return;
  const channels = new Map();
  group.querySelectorAll(".ytr-card[data-ytr-ch]").forEach(el => {
    const id = el.getAttribute("data-ytr-ch");
    if (id && !channels.has(id)) channels.set(id, el.getAttribute("data-ytr-ch-title") || "");
  });
  if (!channels.size) return;
  btn.disabled = true;
  const failed = [];
  for (const [channelId, channelTitle] of channels) {
    try {
      const r = await apiFetch("/api/admin/yt-review/bans", {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ channelId, channelTitle }),
      });
      if (!r.ok) failed.push(channelTitle || channelId);
    } catch { failed.push(channelTitle || channelId); }
  }
  const banned = channels.size - failed.length;
  if (typeof showToast === "function") {
    showToast(failed.length
      ? `Banned ${banned} of ${channels.size} channels · failed: ${failed.join(", ")}`
      : `Banned ${banned} channel${banned === 1 ? "" : "s"}`, failed.length ? "error" : "info", 5000);
  }
  if (failed.length) { btn.disabled = false; return; }
  // Clears any channel-less leftovers and removes the group from view.
  await ytrRejectGroup(btn);
  loadYtBans(); loadYtChannels(); loadYtReview();
}
window.ytrBanGroup = ytrBanGroup;

// ── Search query editor (YT Review -> Search query) ──────────────────
// Edits the server-side templates the review worker and the album/track
// popups build their YouTube searches from. Preview renders locally with
// the same builder the popups use (_ytBuildQueryWith in modal.js); "Run test
// search" fires one real search with the UNSAVED form values.
function _ytrDecodeEntities(s) {
  return String(s ?? "").replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");
}
function _ytrQueryForm() {
  return {
    albumTemplate: document.getElementById("ytr-q-album")?.value ?? "",
    trackTemplate: document.getElementById("ytr-q-track")?.value ?? "",
    noise: document.getElementById("ytr-q-noise")?.value ?? "",
    embeddableOnly: !!document.getElementById("ytr-q-emb")?.checked,
    preferred: document.getElementById("ytr-q-pref")?.value ?? "",
    autoApprovePreferred: !!document.getElementById("ytr-q-pref-auto")?.checked,
  };
}
function _ytrQuerySample() {
  return {
    artist: document.getElementById("ytr-qt-artist")?.value ?? "",
    album: document.getElementById("ytr-qt-album")?.value ?? "",
    track: document.getElementById("ytr-qt-track")?.value ?? "",
  };
}
function _ytrQueryStatus(msg, kind) {
  const st = document.getElementById("ytr-q-status");
  if (!st) return;
  st.textContent = msg || "";
  st.style.color = kind === "error" ? "#e88" : kind === "ok" ? "#7ed196" : "var(--muted)";
}
function _ytrQueryFill(cfg) {
  if (!cfg) return;
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.value = v ?? ""; };
  set("ytr-q-album", cfg.albumTemplate);
  set("ytr-q-track", cfg.trackTemplate);
  set("ytr-q-noise", cfg.noise);
  const emb = document.getElementById("ytr-q-emb");
  if (emb) emb.checked = !!cfg.embeddableOnly;
  set("ytr-q-pref", cfg.preferred);
  const prefAuto = document.getElementById("ytr-q-pref-auto");
  if (prefAuto) prefAuto.checked = !!cfg.autoApprovePreferred;
  ytrQueryPreview();
}
async function ytrLoadQueryConfig() {
  try {
    const r = await fetch("/api/youtube/query-config", { cache: "no-store" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    _ytrQueryFill(j.config);
    _ytrQueryStatus("");
  } catch (e) { _ytrQueryStatus(`Couldn't load: ${e?.message || e}`, "error"); }
}
window.ytrLoadQueryConfig = ytrLoadQueryConfig;
function ytrQueryPreview() {
  const out = document.getElementById("ytr-q-preview");
  if (!out) return;
  if (typeof window._ytBuildQueryWith !== "function") { out.textContent = ""; return; }
  const cfg = _ytrQueryForm();
  const sample = _ytrQuerySample();
  const row = (label, q) => `<div class="ytr-q-prow"><span class="ytr-q-plabel">${label}</span>`
    + `<code class="ytr-q-pcode">${q ? escHtml(q) : '<em style="color:#e88">empty</em>'}</code>`
    + `<span class="ytr-q-plen">${q.length}/500</span></div>`;
  out.innerHTML = row("Album", window._ytBuildQueryWith(cfg, "album", sample))
    + row("Track", window._ytBuildQueryWith(cfg, "track", sample));
}
window.ytrQueryPreview = ytrQueryPreview;
async function ytrSaveQueryConfig(btn) {
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch("/api/admin/yt-review/query-config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(_ytrQueryForm()),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      _ytrQueryStatus(Array.isArray(j?.details) && j.details.length ? j.details.join("; ") : (j?.error || `HTTP ${r.status}`), "error");
      return;
    }
    // Popups open on this page pick up the new phrasing immediately.
    window._sdYtQueryConfig = j.config;
    _ytrQueryFill(j.config);
    _ytrQueryStatus("Saved — applies to the next worker run and new popup searches.", "ok");
  } catch (e) { _ytrQueryStatus(`Save failed: ${e?.message || e}`, "error"); }
  finally { if (btn) btn.disabled = false; }
}
window.ytrSaveQueryConfig = ytrSaveQueryConfig;
async function ytrResetQueryConfig(btn) {
  if (!confirm("Reset the YouTube search query to the built-in defaults?")) return;
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch("/api/admin/yt-review/query-config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reset: true }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { _ytrQueryStatus(j?.error || `HTTP ${r.status}`, "error"); return; }
    window._sdYtQueryConfig = j.config;
    _ytrQueryFill(j.config);
    _ytrQueryStatus("Reset to defaults.", "ok");
  } catch (e) { _ytrQueryStatus(`Reset failed: ${e?.message || e}`, "error"); }
  finally { if (btn) btn.disabled = false; }
}
window.ytrResetQueryConfig = ytrResetQueryConfig;
async function ytrTestQuery(btn) {
  const out = document.getElementById("ytr-qt-out");
  if (!out) return;
  const mode = document.getElementById("ytr-qt-mode")?.value === "track" ? "track" : "album";
  const label = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "Searching…"; }
  out.innerHTML = `<div style="color:var(--muted)">Searching…</div>`;
  try {
    const r = await apiFetch("/api/admin/yt-review/query-test", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ config: _ytrQueryForm(), mode, ..._ytrQuerySample() }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      const msg = Array.isArray(j?.details) && j.details.length ? j.details.join("; ")
        : j?.error === "project_cap" ? "Project quota soft cap reached for today — try again after midnight Pacific."
        : (j?.error || `HTTP ${r.status}`);
      out.innerHTML = `<div style="color:#e88">${escHtml(msg)}</div>`;
      return;
    }
    const items = Array.isArray(j.items) ? j.items : [];
    const topic = items.filter(i => i.isTopic).length;
    const banned = items.filter(i => i.banned).length;
    const head = `<div class="ytr-qt-head">${j.cached ? "Cached — no quota spent" : "Fresh search — 100 units"}`
      + ` · ${j.count} result${j.count === 1 ? "" : "s"}${j.count > items.length ? `, showing first ${items.length}` : ""}`
      + ` · ${topic} on Topic channels${banned ? ` · ${banned} from banned channels (the worker skips these)` : ""}</div>`
      + `<div class="ytr-qt-q"><code>${escHtml(j.q || "")}</code></div>`;
    const rows = items.map(i => {
      const title = _ytrDecodeEntities(i.title);
      const chan = _ytrDecodeEntities(i.channel);
      return `<div class="ytr-qt-row${i.banned ? " is-banned" : ""}">`
        + (i.isTopic ? `<span class="ytr-qt-topic">TOPIC</span>` : "")
        + `<a href="https://www.youtube.com/watch?v=${encodeURIComponent(i.videoId)}" target="_blank" rel="noopener" class="ytr-qt-title" title="${escHtml(title)}">${escHtml(title)}</a>`
        + `<span class="ytr-qt-chan">${escHtml(chan)}</span></div>`;
    }).join("");
    out.innerHTML = head + (rows || `<div style="color:var(--muted)">No results.</div>`);
  } catch (e) {
    out.innerHTML = `<div style="color:#e88">Test failed: ${escHtml(e?.message || String(e))}</div>`;
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = label || "Run test search"; }
  }
}
window.ytrTestQuery = ytrTestQuery;
async function ytrCustomApprove(id) {
  const input = document.getElementById(`ytr-custom-${id}`);
  const url = (input?.value || "").trim();
  if (!url) { _adminNotify("Paste a YouTube URL or 11-char video ID first."); input?.focus(); return; }
  try {
    const r = await apiFetch("/api/admin/yt-review/custom-approve", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id, url }),
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) { _adminNotify(`Custom approve failed: ${body?.error || `HTTP ${r.status}`}${body?.detail ? "\n" + body.detail : ""}`); return; }
    // This row is decided now — remember it so a racing reload can not
    // re-render it as a clickable pending card.
    _ytrRememberDecided(Number(id));
    loadYtReview();
  } catch (e) { _adminNotify(`Custom approve failed: ${e?.message || e}`); }
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
      _adminNotify(`Delete failed: ${body?.error || `HTTP ${r.status}`}`);
      return;
    }
    loadYtReview();
  } catch (e) { _adminNotify(`Delete failed: ${e?.message || e}`); }
}
window.ytrDeleteApproval = ytrDeleteApproval;
async function ytrUnreject(id, btn) {
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch("/api/admin/yt-review/unreject", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id }),
    });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      _adminNotify(`Un-reject failed: ${body?.error || `HTTP ${r.status}`}`);
      if (btn) btn.disabled = false;
      return;
    }
    btn?.closest(".ytr-card")?.remove();
    loadYtReview();
  } catch (e) {
    _adminNotify(`Un-reject failed: ${e?.message || e}`);
    if (btn) btn.disabled = false;
  }
}
window.ytrUnreject = ytrUnreject;
// Auto-pinned review. Cards leave immediately (optimistic, like decide);
// on failure they're un-remembered so the reconcile reload brings them back.
function _ytrDropAutoCards(ids) {
  for (const id of ids) {
    _ytrRememberDecided(id);
    const card = document.querySelector(`#ytr-queue .ytr-card[data-ytr-id="${id}"]`);
    const group = card?.closest(".ytr-group");
    card?.remove();
    if (group && !group.querySelector(".ytr-card")) group.remove();
  }
  if (!document.querySelector("#ytr-queue .ytr-card")) _ytrScheduleReload();
}
async function ytrKeepAuto(ids, btn) {
  ids = (ids || []).map(Number).filter(Number.isFinite);
  if (!ids.length) return;
  if (btn) btn.disabled = true;
  _ytrDropAutoCards(ids);
  _ytrAdjustCount("auto", -ids.length);
  _ytrAdjustCount("approved", +ids.length);
  try {
    const r = await apiFetch("/api/admin/yt-review/confirm-auto", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ids }),
    });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      ids.forEach(id => _ytrDecided.delete(id));
      showToast(`Keep failed: ${body?.error || `HTTP ${r.status}`}`, "error", 5000);
      loadYtReview();
    }
  } catch (e) {
    ids.forEach(id => _ytrDecided.delete(id));
    showToast(`Keep failed: ${e?.message || e}`, "error", 5000);
    loadYtReview();
  }
}
window.ytrKeepAuto = ytrKeepAuto;
function ytrKeepAutoPage(btn) {
  const ids = Array.from(document.querySelectorAll("#ytr-queue .ytr-card[data-ytr-id]"))
    .map(el => Number(el.getAttribute("data-ytr-id")));
  ytrKeepAuto(ids, btn);
}
window.ytrKeepAutoPage = ytrKeepAutoPage;
async function ytrRemoveAuto(id, btn) {
  if (btn) btn.disabled = true;
  _ytrDropAutoCards([id]);
  _ytrAdjustCount("auto", -1);
  _ytrAdjustCount("rejected", +1);
  try {
    const r = await apiFetch("/api/admin/yt-review/delete-approval", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id }),
    });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      _ytrDecided.delete(id);
      showToast(`Remove failed: ${body?.error || `HTTP ${r.status}`}`, "error", 5000);
      loadYtReview();
    }
  } catch (e) {
    _ytrDecided.delete(id);
    showToast(`Remove failed: ${e?.message || e}`, "error", 5000);
    loadYtReview();
  }
}
window.ytrRemoveAuto = ytrRemoveAuto;
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
      <button class="admin-btn" data-sd-click="${_sdOn(function (event) { document.getElementById('ytr-errors-modal').remove() })}">Close</button>
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
        <span style="color:#aaa;font-size:0.8rem">${escHtml(user_email || "unknown")}</span>
        <div style="display:flex;gap:0.75rem;align-items:center">
          <span style="color:#555;font-size:0.75rem">${date}</span>
          <button data-sd-click="${_sdOn(((a0) => function (event) { deleteFeedbackItem(a0) })(_sdLit(id)))}" style="background:none;border:none;color:#666;cursor:pointer;font-size:0.75rem;padding:0" title="Delete">\u2715</button>
        </div>
      </div>
      <div style="color:var(--fg);white-space:pre-wrap">${escHtml(message)}</div>
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
          ? ` <a href="#" data-sd-click="${_sdOn(((a0) => function (event) { event.preventDefault();_adminToggleJobHistory(a0,this) })(String(key ?? "")))}" style="font-size:0.72rem;color:#7eb8da;text-decoration:none">history</a><div class="admin-job-hist" data-job="${key}" style="display:none;margin:0.3rem 0 0.4rem 0.6rem"></div>`
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
function _kpiCard(label, value, sub, title, icon) {
  const t = title ? ` title="${String(title).replace(/"/g, "&quot;")}"` : "";
  const ic = icon ? `<span class="admin-kpi-icon">${icon}</span>` : "";
  return `<div class="admin-kpi"${t}>
    <div class="admin-kpi-head">${ic}<span class="admin-kpi-label">${label}</span></div>
    <div class="admin-kpi-val">${value}</div>
    ${sub ? `<div class="admin-kpi-sub">${sub}</div>` : ""}</div>`;
}
// One labelled, colour-accented group of KPI cards.
function _kpiGroup(title, icon, accent, cards) {
  return `<div class="admin-kpi-group ${accent}">
    <div class="admin-kpi-grouphdr"><span class="admin-kpi-groupicon">${icon}</span>${title}</div>
    <div class="admin-kpi-grid">${cards.join("")}</div>
  </div>`;
}
async function loadAdminOverview() {
  const el = document.getElementById("admin-overview-kpis");
  if (!el) return;
  try {
    const r = await apiFetch("/api/admin/overview");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    const nf = n => (typeof n === "number" ? n.toLocaleString() : (n ?? "–"));
    const pct = (a, b) => (b > 0 ? Math.round((a / b) * 100) + "%" : "–");

    // ── People — who's signed up and connected ──────────────────────
    const people = _kpiGroup("People", "👥", "kpi-people", [
      _kpiCard("Accounts", `${nf(d.clerkUsers ?? "–")} / ${nf(d.maxUsers || 100)}`, (d.clerkUsers != null && d.clerkUsers >= (d.maxUsers || 100)) ? "full: sign-ups closed" : "of the account cap", "Every Clerk account, including people who signed up but never connected Discogs. All of them count against the account cap; when it's full, new sign-ups are closed.", "🪪"),
      _kpiCard("Discogs-connected", nf(d.totalUsers), "linked Discogs", "Accounts that completed the Discogs OAuth link (one row in user_tokens).", "🔗"),
      _kpiCard("Connect rate", pct(d.totalUsers, d.clerkUsers), "of signups linked Discogs", "Share of signed-up Clerk accounts that went on to connect a Discogs account (connected ÷ Clerk users). A funnel/health metric.", "📈"),
      _kpiCard("New (7d)", nf(d.newUsers7d), d.sinceLabel7d || "", "Discogs connections created in the last 7 days.", "🌱"),
      _kpiCard("New (30d)", nf(d.newUsers30d), "last 30 days", "Discogs connections created in the last 30 days.", "🌿"),
      _kpiCard("Unconnected", nf(Math.max(0, (d.clerkUsers ?? 0) - d.totalUsers)), "no Discogs link", `Signed-up accounts that haven't connected Discogs. These are deleted automatically after ${d.unconnectedDeleteDays || 42} days of inactivity, freeing their spot.`, "🚫"),
    ]);

    // ── Engagement — who's actually using it ────────────────────────
    const engagement = _kpiGroup("Engagement", "⚡", "kpi-engage", [
      _kpiCard("DAU", nf(d.dau), "active today", "Distinct connected users with any activity in the last 24 hours.", "🟢"),
      _kpiCard("WAU", nf(d.wau), "active this week", "Distinct connected users active in the last 7 days.", "📆"),
      _kpiCard("MAU", nf(d.mau), "active this month", "Distinct connected users active in the last 30 days.", "🗓️"),
      _kpiCard("Stickiness", pct(d.dau, d.mau), "DAU ÷ MAU", "Of everyone active this month, the share active on a given day. 20%+ is considered strong for a hobby app.", "🧲"),
      _kpiCard("30d active rate", pct(d.mau, d.totalUsers), "of connected users", "Share of all connected accounts that were active in the last 30 days — how much of the base is live vs dormant.", "❤️‍🔥"),
      _kpiCard("Searches", nf(d.searches24h), `${nf(d.searches7d)} · 7d`, "Searches run in the last 24 hours (7-day total in the sub-line).", "🔎"),
      _kpiCard("Album opens", nf(d.albumOpens24h), `${nf(d.albumOpens7d)} · 7d`, "Album / master detail views opened in the last 24 hours (7-day total in the sub-line).", "💿"),
      _kpiCard("Plays", nf(d.plays24h), `${nf(d.plays7d)} · 7d`, "Track plays started in the last 24 hours (7-day total in the sub-line). Full media breakdown lives in the Media tab.", "🎧"),
    ]);

    // ── Library — the scale of what's indexed and logged ────────────
    const library = _kpiGroup("Library & totals", "📚", "kpi-library", [
      _kpiCard("Collection items", nf(d.collectionItems), "rows cached", "Total collection rows cached across every user's synced Discogs collection.", "📀"),
      _kpiCard("Wantlist items", nf(d.wantlistItems), "rows cached", "Total wantlist rows cached across every user's synced Discogs wantlist.", "⭐"),
      _kpiCard("Inventory items", nf(d.inventoryItems), "rows cached", "Total marketplace inventory (listings) cached across all users.", "🏷️"),
      _kpiCard("Seller orders", nf(d.ordersTotal), "rows cached", "Total seller orders cached across all users.", "📦"),
      _kpiCard("List items", nf(d.listItems), "rows cached", "Total items across every user's synced Discogs lists.", "🗂️"),
      _kpiCard("Plays all-time", nf(d.playsAllTime), "since launch", "Every track play ever logged, across all users and sources.", "▶️"),
      _kpiCard("Searches all-time", nf(d.searchesAllTime), "since launch", "Every search ever logged, across all users.", "🔍"),
    ]);

    el.innerHTML = `<div class="admin-kpi-groups">${people}${engagement}${library}</div>`;
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
        ? `<a href="#" data-sd-click="${_sdOn(function (event) { _adminSetTopPlayed(100);return false })}" style="color:var(--accent);font-size:0.74rem;margin-left:0.5rem">show top 100</a>`
        : `<a href="#" data-sd-click="${_sdOn(function (event) { _adminSetTopPlayed(10);return false })}" style="color:var(--accent);font-size:0.74rem;margin-left:0.5rem">show top 10</a>`;
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

// Collection Stats panel removed — per-user counts + taste now live as columns
// in the unified users grid; the global totals are Overview "Library" cards.

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
          ${thumb ? `<img src="${escHtml(thumb)}" class="admin-item-thumb" loading="lazy" decoding="async" />` : `<div class="admin-item-thumb"></div>`}
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
        ${thumb ? `<img src="${escHtml(thumb)}" class="admin-item-thumb" loading="lazy" decoding="async" />` : `<div class="admin-item-thumb"></div>`}
        <div style="flex:1;min-width:0;overflow:hidden">
          <div class="admin-item-title">${escHtml(title)}</div>
          <div class="admin-item-sub">${meta}${stars ? `<span class="admin-item-rating">${stars}</span>` : ""}</div>
        </div>
      </a>`;
    }).join("");
    // Pagination
    if (data.pages > 1) {
      let pagHtml = "";
      if (_adminItemsPage > 1) pagHtml += `<a href="#" data-sd-click="${_sdOn(function (event) { event.preventDefault();_adminItemsPage--;loadAdminItems() })}" style="color:var(--accent);text-decoration:none;margin:0 0.3rem">\u2190 Prev</a>`;
      pagHtml += `<span style="color:var(--muted)">Page ${_adminItemsPage} of ${data.pages}</span>`;
      if (_adminItemsPage < data.pages) pagHtml += `<a href="#" data-sd-click="${_sdOn(function (event) { event.preventDefault();_adminItemsPage++;loadAdminItems() })}" style="color:var(--accent);text-decoration:none;margin:0 0.3rem">Next \u2192</a>`;
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
    // mode 'block' rows have no video (admin hid a wrong Discogs match);
    // 'replace' rows pin a video over the Discogs match.
    const ytLink = (o.mode === "block" || !o.video_id)
      ? `<span style="color:var(--muted)">🚫 Discogs video hidden</span>`
      : `<a href="https://www.youtube.com/watch?v=${encodeURIComponent(o.video_id)}" target="_blank" rel="noopener" style="color:#7eb8da;text-decoration:none">${escHtml(trim(o.video_title || o.video_id, 40))}</a>${o.mode === "replace" ? ` <span style="color:var(--muted);font-size:0.7rem" title="Pinned — wins over the Discogs videos[] match">📌</span>` : ""}`;
    return `<tr style="border-top:1px solid var(--border);font-size:0.78rem">
      <td style="padding:0.35rem 0.5rem;color:var(--muted);white-space:nowrap">${escHtml(fmtDate(o.submitted_at))}</td>
      <td style="padding:0.35rem 0.5rem">${escHtml(o.release_type)}</td>
      <td style="padding:0.35rem 0.5rem">${albumLink}</td>
      <td style="padding:0.35rem 0.5rem;font-family:monospace">${escHtml(o.track_position)}</td>
      <td style="padding:0.35rem 0.5rem">${escHtml(trim(o.track_title || "", 32))}</td>
      <td style="padding:0.35rem 0.5rem">${ytLink}</td>
      <td style="padding:0.35rem 0.5rem;color:var(--muted);font-family:monospace;font-size:0.72rem">${escHtml(trim(o.submitted_by, 12))}</td>
      <td style="padding:0.35rem 0.5rem"><button class="admin-btn admin-btn-danger" data-sd-click="${_sdOn(((a0, a1, a2) => function (event) { adminDeleteSubmission(this,a0,a1,a2) })(String(o.release_id ?? ""), String(o.release_type ?? ""), String(o.track_position ?? "")))}">Delete</button></td>
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
      <td style="padding:0.35rem 0.5rem"><button class="admin-btn" data-sd-click="${_sdOn(((a0) => function (event) { adminClearUnavailable(this,a0) })(String(o.video_id ?? "")))}" title="Clear this entry — videoId starts fresh from count 1 next time it's reported">Clear</button></td>
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
    _adminNotify("Clear failed: " + (e?.message || e));
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
    _adminNotify("Delete failed: " + (e?.message || e));
  }
}
window.adminDeleteSubmission = adminDeleteSubmission;


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


// ── Unified users table ───────────────────────────────────────────
// One row per user merged from sync + behavior + suggestions
// (/api/admin/users-unified). A column-group toggle swaps which
// metric columns show WITHOUT refetching or reordering, so a user
// keeps its row across groups — no more matching names across grids.
let _adminUnifiedData = [];
let _adminUnifiedGroup = "all";           // all | sync | behavior | suggestions
let _adminUnifiedSort = { col: "lastActiveAt", dir: "desc" };
let _adminUnifiedFilter = "";
let _adminUnifiedFilterTimer = null;

// Column descriptors. group 'id' always first, 'meta' always last;
// 'sync'/'behavior'/'suggestions' show when their tab (or All) is on.
// type drives both sort comparison and default sort direction.
const _ADMIN_UNIFIED_COLS = [
  { key: "clerkUsername",              label: "User",            group: "id",          type: "str",  align: "left" },
  { key: "hasOAuth",                   label: "Linked",          group: "id",          type: "conn", align: "center" },
  { key: "discogsUsername",            label: "Discogs",         group: "sync",        type: "str",  align: "left" },
  { key: "lastActiveAt",               label: "Last active",     group: "meta",        type: "date", align: "left" },
  { key: "signedUpAt",                 label: "Signed up",       group: "meta",        type: "date", align: "left" },
  { key: "albumClicksTotal",           label: "Clicks 30d/tot",  group: "behavior",    type: "num",  align: "right", pair: "albumClicks30d" },
  { key: "playsTotal",                 label: "Plays 30d/tot",   group: "behavior",    type: "num",  align: "right", pair: "plays30d" },
  { key: "searchesTotal",              label: "Search 30d/tot",  group: "behavior",    type: "num",  align: "right", pair: "searches30d" },
  { key: "suggestionsCount",           label: "Sugg saved",      group: "suggestions", type: "num",  align: "right" },
  { key: "suggestionsFavorited",       label: "Sugg fav'd",      group: "suggestions", type: "num",  align: "right" },
  { key: "collDateRange",              label: "Coll range",      group: "taste",       type: "range", align: "left" },
  { key: "topGenres",                  label: "Top genres",      group: "taste",       type: "list", align: "left" },
  { key: "topStyles",                  label: "Top styles",      group: "taste",       type: "list", align: "left" },
  { key: "suggestionsLastGeneratedAt", label: "Last gen",        group: "suggestions", type: "date", align: "left" },
  { key: "collectionCount",            label: "Collection",      group: "sync",        type: "num",  align: "right" },
  { key: "wantlistCount",              label: "Wantlist",        group: "sync",        type: "num",  align: "right" },
  { key: "inventoryCount",             label: "Inventory",       group: "sync",        type: "num",  align: "right" },
  { key: "ordersCount",                label: "Orders",          group: "sync",        type: "num",  align: "right" },
  { key: "listCount",                  label: "Lists",           group: "sync",        type: "lists", align: "right" },
  { key: "favoriteCount",              label: "Favs",            group: "sync",        type: "num",  align: "right" },
  { key: "collectionSyncedAt",         label: "Coll synced",     group: "sync",        type: "date", align: "left" },
  { key: "wantlistSyncedAt",           label: "Want synced",     group: "sync",        type: "date", align: "left" },
  { key: "syncStatus",                 label: "Sync",            group: "sync",        type: "str",  align: "left" },
  { key: "delete",                     label: "",                group: "meta",        type: "delete", align: "center" },
];
const _ADMIN_UNIFIED_GROUPS = [
  { key: "all",         label: "All" },
  { key: "sync",        label: "Sync" },
  { key: "behavior",    label: "Behavior" },
  { key: "suggestions", label: "Suggestions" },
  { key: "taste",       label: "Taste" },
];

function _adminUnifiedVisibleCols() {
  return _ADMIN_UNIFIED_COLS.filter(c =>
    c.group === "id" || c.group === "meta" ||
    _adminUnifiedGroup === "all" || c.group === _adminUnifiedGroup);
}

// ms number OR ISO string OR null → "3/17/26 7:20p"
function _adminUnifiedFmtDate(v) {
  if (v == null || v === "") return "—";
  const dt = (typeof v === "number") ? new Date(v) : new Date(v);
  if (isNaN(dt.getTime())) return "—";
  const M = dt.getMonth() + 1, D = dt.getDate(), YY = String(dt.getFullYear()).slice(-2);
  let h = dt.getHours(); const m = String(dt.getMinutes()).padStart(2, "0");
  const ap = h >= 12 ? "p" : "a"; h = h % 12; if (h === 0) h = 12;
  return `${M}/${D}/${YY} ${h}:${m}${ap}`;
}

// Relative "time ago" — used for the Last active column so recency reads at a
// glance. Granularity climbs min → hr → day → mo → yr as it ages.
// Admin messages use toasts instead of blocking alert() dialogs:
// anything that reads like a failure is shown as an error toast (and
// stays up longer); everything else as info.
function _adminNotify(message) {
  const msg = String(message ?? "");
  if (typeof showToast !== "function") { window.alert(msg); return; }
  const isError = /\b(fail|failed|error|couldn|could not|cannot|unable|invalid|denied|refus|http \d{3})/i.test(msg);
  showToast(msg, isError ? "error" : "info", isError ? 8000 : 5000);
}
window._adminNotify = _adminNotify;

// ── System info + admin action log (System tab) ─────────────────────
async function loadAdminSystem() {
  const el = document.getElementById("admin-system-info");
  if (!el) return;
  try {
    const r = await apiFetch("/api/admin/system");
    if (!r.ok) { el.textContent = `System info unavailable (HTTP ${r.status})`; return; }
    const s = await r.json();
    const mb = n => `${Math.round((n || 0) / 1048576)} MB`;
    const pct = s.heapLimit ? Math.round((s.heapUsed / s.heapLimit) * 100) : null;
    const up = s.uptimeSec >= 86400 ? `${Math.floor(s.uptimeSec / 86400)}d ${Math.floor(s.uptimeSec % 86400 / 3600)}h`
             : s.uptimeSec >= 3600 ? `${Math.floor(s.uptimeSec / 3600)}h ${Math.floor(s.uptimeSec % 3600 / 60)}m`
             : `${Math.floor(s.uptimeSec / 60)}m`;
    const item = (label, value, title, warn) => `<span title="${escHtml(title)}">${label} <strong style="color:${warn ? "#e88" : "var(--text)"}">${escHtml(value)}</strong></span>`;
    el.innerHTML = [
      item("heap", `${mb(s.heapUsed)} / ${mb(s.heapLimit)}${pct != null ? ` (${pct}%)` : ""}`, "V8 heap in use vs the configured cap. Memory drives the Railway bill.", pct != null && pct >= 80),
      item("RSS", mb(s.rss), "Total process memory (what Railway meters)."),
      item("up", up, "Time since this process started (last deploy or restart)."),
      item("commit", s.commit ? String(s.commit).slice(0, 8) : "—", "Deployed git commit (RAILWAY_GIT_COMMIT_SHA)."),
      item("node", s.node || "—", "Node.js version."),
      item("DB pool", `${s.pool?.total ?? "?"} open · ${s.pool?.idle ?? "?"} idle · ${s.pool?.waiting ?? 0} waiting`, "Postgres connection pool.", (s.pool?.waiting || 0) > 0),
      item("kill switch", s.apiKillSwitch ? "ON" : "off", "Outgoing data-API kill switch.", s.apiKillSwitch),
      item("token encryption", s.tokenEncryption ? "on" : "OFF", "Discogs credentials encrypted at rest (TOKEN_ENC_KEY).", !s.tokenEncryption),
    ].join("");
  } catch (e) { el.textContent = `System info failed: ${e?.message || e}`; }
}
window.loadAdminSystem = loadAdminSystem;

async function loadAdminAudit() {
  const el = document.getElementById("admin-audit");
  if (!el) return;
  el.textContent = "Loading…";
  try {
    const r = await apiFetch("/api/admin/audit?limit=150");
    if (!r.ok) { el.textContent = `Failed: HTTP ${r.status}`; return; }
    const { rows = [] } = await r.json();
    if (!rows.length) { el.innerHTML = `<em>No admin actions recorded yet.</em>`; return; }
    el.innerHTML = `<div style="max-height:360px;overflow-y:auto"><table style="width:100%;border-collapse:collapse">${rows.map(a => {
      const ok = a.status == null || a.status < 400;
      const body = a.detail?.body ? `<div style="color:var(--muted);font-family:monospace;font-size:0.7rem;word-break:break-all;margin-top:0.1rem">${escHtml(a.detail.body)}</div>` : "";
      return `<tr style="border-top:1px solid var(--border);vertical-align:top">
        <td style="padding:0.25rem 0.4rem;white-space:nowrap" title="${escHtml(new Date(a.at).toLocaleString())}">${escHtml(_adminUnifiedRelTime(a.at))}</td>
        <td style="padding:0.25rem 0.4rem;color:var(--text)">${escHtml(a.action)}${a.target ? ` <span style="color:var(--muted)">${escHtml(a.target)}</span>` : ""}${body}</td>
        <td style="padding:0.25rem 0.4rem;white-space:nowrap;color:${ok ? "var(--muted)" : "#e88"}">${a.status ?? ""}</td>
        <td style="padding:0.25rem 0.4rem;white-space:nowrap">${escHtml(a.actor === "system" ? "system" : "admin")}</td>
      </tr>`;
    }).join("")}</table></div>`;
  } catch (e) { el.textContent = `Failed: ${e?.message || e}`; }
}
window.loadAdminAudit = loadAdminAudit;

function _adminUnifiedRelTime(v) {
  if (v == null || v === "") return "—";
  const t = (typeof v === "number") ? v : new Date(v).getTime();
  if (isNaN(t)) return "—";
  const s = Math.floor((Date.now() - t) / 1000);
  if (s < 60) return "just now";
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} min ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h} hr ago`;
  const d = Math.floor(h / 24);
  if (d < 30) return `${d} day${d === 1 ? "" : "s"} ago`;
  const mo = Math.floor(d / 30);
  if (mo < 12) return `${mo} mo ago`;
  return `${Math.floor(d / 365)} yr ago`;
}

async function loadAdminUsersUnified(silent) {
  const el = document.getElementById("users-unified-list");
  if (!el) return;
  // `silent` skips the "Loading…" placeholder — used by the post-sync
  // re-poll so the section doesn't collapse+expand on every refresh.
  if (!silent) el.textContent = "Loading…";
  try {
    const r = await apiFetch("/api/admin/users-unified");
    if (!r.ok) { el.textContent = `Could not load users (HTTP ${r.status}).`; return; }
    const j = await r.json();
    _adminUnifiedData = Array.isArray(j?.items) ? j.items : [];
    _adminUnifiedRender();
  } catch (e) { el.innerHTML = `<span style="color:#e88">Failed: ${escHtml(e?.message || e)}</span>`; }
}
window.loadAdminUsersUnified = loadAdminUsersUnified;

function _adminUnifiedRenderGroupBar() {
  const bar = document.getElementById("users-unified-groupbar");
  if (!bar) return;
  bar.innerHTML = _ADMIN_UNIFIED_GROUPS.map(g => {
    const on = _adminUnifiedGroup === g.key;
    return `<button class="admin-btn${on ? " active" : ""}" data-sd-click="${_sdOn(((a0) => function (event) { _adminUnifiedSetGroup(a0) })(String(g.key ?? "")))}" ${on ? 'style="background:var(--accent);color:#000;font-weight:600"' : ""}>${g.label}</button>`;
  }).join("");
}

function _adminUnifiedSetGroup(g) {
  _adminUnifiedGroup = g;
  // If the active sort column just left the view, fall back to signup.
  if (!_adminUnifiedVisibleCols().some(c => c.key === _adminUnifiedSort.col)) {
    _adminUnifiedSort = { col: "lastActiveAt", dir: "desc" };
  }
  _adminUnifiedRender();
}
window._adminUnifiedSetGroup = _adminUnifiedSetGroup;

function _adminUnifiedSortBy(col) {
  const desc = _ADMIN_UNIFIED_COLS.find(c => c.key === col);
  if (_adminUnifiedSort.col === col) {
    _adminUnifiedSort.dir = _adminUnifiedSort.dir === "asc" ? "desc" : "asc";
  } else {
    _adminUnifiedSort.col = col;
    _adminUnifiedSort.dir = (desc && desc.type === "str") ? "asc" : "desc";
  }
  _adminUnifiedRender();
}
window._adminUnifiedSortBy = _adminUnifiedSortBy;

// Reveal the full sync_error for a row when its (truncated / ⚠) status is
// clicked. A title= tooltip alone was too easy to miss — this guarantees
// the detail is one click away. data-err carries the message.
window._adminSyncErrDetail = function (el) {
  try {
    const msg = (el && el.getAttribute && el.getAttribute("data-err")) || "";
    if (msg) window.alert(msg);
  } catch {}
};

// Confirmation window for deleting a user. Destructive + irreversible, so
// it names the account, shows the id, and requires an explicit click.
function adminDeleteUser(clerkUserId) {
  const u = (_adminUnifiedData || []).find(x => x.clerkUserId === clerkUserId);
  const name = u ? (u.clerkUsername || u.discogsUsername || "(no name)") : clerkUserId;
  document.getElementById("admin-delete-user-overlay")?.remove();
  const overlay = document.createElement("div");
  overlay.id = "admin-delete-user-overlay";
  Object.assign(overlay.style, {
    position: "fixed", inset: "0", background: "rgba(0,0,0,0.7)", zIndex: "600",
    display: "flex", alignItems: "center", justifyContent: "center", padding: "1rem",
  });
  overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;max-width:440px;width:100%;padding:1.4rem">
      <h3 style="margin:0 0 0.7rem;color:#e88;font-size:1rem">Delete this user?</h3>
      <p style="color:var(--text);font-size:0.85rem;margin:0 0 0.35rem">Permanently delete <strong>${escHtml(name)}</strong>${u && u.discogsUsername ? ` (discogs: ${escHtml(u.discogsUsername)})` : ""}.</p>
      <p style="color:var(--muted);font-size:0.72rem;margin:0 0 0.6rem;font-family:monospace">${escHtml(clerkUserId)}</p>
      <p style="color:var(--muted);font-size:0.8rem;margin:0 0 1rem;line-height:1.5">This removes <strong>all</strong> of their SeaDisco data <strong>and</strong> their Clerk login, so they can't sign back in. It cannot be undone.</p>
      <div id="admin-delete-user-status" style="color:#e88;font-size:0.8rem;margin-bottom:0.6rem"></div>
      <div style="display:flex;gap:0.5rem;justify-content:flex-end">
        <button class="admin-btn" data-sd-click="${_sdOn(function (event) { document.getElementById('admin-delete-user-overlay')?.remove() })}">Cancel</button>
        <button class="admin-btn" id="admin-delete-user-confirm" style="background:#7a2b2b;color:#fff;border-color:#7a2b2b" data-sd-click="${_sdOn(((a0) => function (event) { _adminDeleteUserConfirm(a0, this) })(String(clerkUserId ?? "")))}">Delete permanently</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
}
window.adminDeleteUser = adminDeleteUser;

async function _adminDeleteUserConfirm(clerkUserId, btn) {
  const statusEl = document.getElementById("admin-delete-user-status");
  btn.disabled = true; btn.textContent = "Deleting…";
  try {
    const r = await apiFetch(`/api/admin/user/${encodeURIComponent(clerkUserId)}`, { method: "DELETE" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (statusEl) statusEl.textContent = j.message || j.error || `Failed (HTTP ${r.status})`;
      btn.disabled = false; btn.textContent = "Delete permanently";
      return;
    }
    document.getElementById("admin-delete-user-overlay")?.remove();
    if (typeof showToast === "function") {
      showToast(`User deleted${j.clerkDeleted === false ? " (SeaDisco data only — Clerk login not removed)" : ""}`);
    }
    // Drop from local data and re-render so the row disappears immediately.
    _adminUnifiedData = (_adminUnifiedData || []).filter(x => x.clerkUserId !== clerkUserId);
    _adminUnifiedRender();
  } catch {
    if (statusEl) statusEl.textContent = "Request failed — please try again.";
    btn.disabled = false; btn.textContent = "Delete permanently";
  }
}
window._adminDeleteUserConfirm = _adminDeleteUserConfirm;

function _adminUnifiedFilterInput(v) {
  if (_adminUnifiedFilterTimer) clearTimeout(_adminUnifiedFilterTimer);
  _adminUnifiedFilterTimer = setTimeout(() => {
    _adminUnifiedFilter = String(v || "").trim().toLowerCase();
    _adminUnifiedRender();
  }, 250);
}
window._adminUnifiedFilterInput = _adminUnifiedFilterInput;

function _adminUnifiedSortRows(rows) {
  const desc = _ADMIN_UNIFIED_COLS.find(c => c.key === _adminUnifiedSort.col);
  const type = desc ? desc.type : "str";
  const mul = _adminUnifiedSort.dir === "asc" ? 1 : -1;
  const key = _adminUnifiedSort.col;
  const val = (row) => {
    let v = row[key];
    if (type === "num") return (v == null || v === "") ? null : Number(v);
    if (type === "conn") return v ? 1 : 0;
    if (type === "lists") return Number(row.listCount || 0);
    if (type === "range") { const t = new Date(row.collNewest).getTime(); return isNaN(t) ? null : t; }
    if (type === "list") { const a = row[key]; return (Array.isArray(a) && a.length) ? a.join(", ").toLowerCase() : null; }
    if (type === "date") { if (v == null || v === "") return null; const t = new Date(v).getTime(); return isNaN(t) ? null : t; }
    return (v == null || v === "") ? null : String(v).toLowerCase();
  };
  return rows.slice().sort((a, b) => {
    const av = val(a), bv = val(b);
    // Missing values always sink, regardless of direction.
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    if (type === "str" || type === "list") return av.localeCompare(bv) * mul;
    return (av - bv) * mul;
  });
}

function _adminUnifiedUserCell(u) {
  const primary = u.clerkUsername || u.discogsUsername || "(no name)";
  const dot = u.online
    ? `<span title="Active in last 24h" style="display:inline-block;width:7px;height:7px;border-radius:50%;background:#7ed196;margin-right:0.35rem"></span>`
    : `<span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--border);margin-right:0.35rem"></span>`;
  const disc = u.discogsUsername ? `<div style="font-size:0.7rem;color:var(--muted)">discogs: ${escHtml(u.discogsUsername)}</div>` : "";
  const id = `<div style="font-size:0.66rem;color:#555;font-family:monospace">${escHtml(u.clerkUserId || "")}</div>`;
  // Delete affordance now lives in its own trailing column (type: "delete").
  return `<div style="min-width:11rem">${dot}<span style="font-weight:600;color:var(--text)">${escHtml(primary)}</span>${disc}${id}</div>`;
}

function _adminUnifiedCell(u, col) {
  if (col.key === "clerkUsername") return _adminUnifiedUserCell(u);
  if (col.type === "conn") {
    return u.hasOAuth
      ? `<span style="color:#6fcf87;cursor:help" title="Discogs account connected">✓</span>`
      : `<span style="color:#d0743f;cursor:help" title="Signed up but has NOT connected Discogs — deleted after 6 weeks of inactivity">✗</span>`;
  }
  if (col.type === "delete") {
    // Own row can't be deleted (the server also refuses the admin account).
    const isSelf = u.clerkUserId && u.clerkUserId === window._clerk?.user?.id;
    if (!u.clerkUserId || isSelf) return "";
    return `<button class="admin-btn" title="Delete this user — removes their SeaDisco data and Clerk login" data-sd-click="${_sdOn(((a0) => function (event) { event.stopPropagation();adminDeleteUser(a0) })(String(u.clerkUserId ?? "")))}" style="font-size:0.6rem;padding:0.05rem 0.35rem;color:#e88;border-color:#5a2b2b">Delete</button>`;
  }
  if (col.type === "range") {
    const f = v => { if (!v) return null; const dt = new Date(v); return isNaN(dt.getTime()) ? null : dt.toLocaleDateString("en-US", { month: "short", year: "numeric" }); };
    const o = f(u.collOldest), n = f(u.collNewest);
    return (o || n) ? `<span style="white-space:nowrap">${escHtml(o || "—")} – ${escHtml(n || "—")}</span>` : `<span style="color:var(--muted)">—</span>`;
  }
  if (col.type === "list") {
    const arr = Array.isArray(u[col.key]) ? u[col.key] : [];
    if (!arr.length) return `<span style="color:var(--muted)">—</span>`;
    return `<span style="font-size:0.74rem;color:#9a8a70" title="${escHtml(arr.join(", "))}">${escHtml(arr.slice(0, 6).join(", "))}</span>`;
  }
  if (col.type === "lists") {
    return `${Number(u.listCount || 0)} <span style="color:#555;font-size:0.68rem">(${Number(u.listItemCount || 0)})</span>`;
  }
  if (col.key === "lastActiveAt") {
    // Relative "X min ago"; exact timestamp on hover.
    return `<span title="${escHtml(_adminUnifiedFmtDate(u.lastActiveAt))}">${escHtml(_adminUnifiedRelTime(u.lastActiveAt))}</span>`;
  }
  if (col.type === "date") return _adminUnifiedFmtDate(u[col.key]);
  if (col.key === "syncStatus") {
    const s = u.syncStatus || "—";
    // Syncing → live progress, no button (avoid re-triggering an in-flight run).
    if (s === "syncing") {
      const pct = u.syncTotal ? Math.round((Number(u.syncProgress || 0) / Number(u.syncTotal)) * 100) : null;
      return `<span style="color:#f0c674">syncing${pct != null ? ` ${pct}%` : "…"}</span>`;
    }
    // Manual per-user sync button. Only offered when the user has a linked
    // Discogs handle (the sync endpoint keys on discogs_username).
    const btn = u.discogsUsername
      ? ` <button class="admin-btn" style="font-size:0.68rem;padding:0.1rem 0.45rem;margin-left:0.35rem" data-sd-click="${_sdOn(((a0) => function (event) { adminSyncUser(a0, this) })(String(u.discogsUsername ?? "")))}" title="Run a full Discogs library sync for this user">Sync</button>`
      : "";
    // Render by STATUS, not by "has a sync_error". A gapped completion and a
    // restart-interrupted "stopped" run BOTH carry a sync_error note but are
    // not failures — keying off sync_error alone made them all show red
    // "error". Only sync_status === "error" is a real failure.
    // Clickable detail: a bare title= tooltip is easy to miss (slow hover,
    // can't click). When a sync_error note exists, make the status CLICKABLE
    // to show the full text, plus keep the hover title. data-err carries the
    // message to the delegated handler.
    const hasErr = !!u.syncError;
    const errAttr = hasErr
      ? ` title="${escHtml(String(u.syncError))} (click for full detail)" data-err="${escHtml(String(u.syncError))}" data-sd-click="${_sdOn(function (event) { event.stopPropagation();_adminSyncErrDetail(this) })}"`
      : "";
    const clickCur = hasErr ? "cursor:pointer;text-decoration:underline dotted" : "";
    const shortErr = hasErr ? (String(u.syncError).length > 44 ? String(u.syncError).slice(0, 44) + "…" : String(u.syncError)) : "";
    if (s === "error") {
      return `<span style="color:#e88;${clickCur}"${errAttr}>error${shortErr ? ": " + escHtml(shortErr) : ""}</span>${btn}`;
    }
    if (s === "stopped") {
      // Interrupted by a restart, or manually stopped — not a failure. Amber,
      // click for the reason, Sync button to resume.
      return `<span style="color:#c9a24a;${clickCur}"${errAttr}>stopped</span>${btn}`;
    }
    if (s === "complete") {
      // A "complete with gaps" run keeps its gaps note in sync_error; show it
      // as complete (⚠, click for detail), never as an error.
      return hasErr
        ? `<span style="color:#7fae7f;${clickCur}"${errAttr}>complete ⚠</span>${btn}`
        : `<span style="color:#7fae7f">complete</span>${btn}`;
    }
    return `${escHtml(s)}${btn}`;
  }
  if (col.pair != null) {
    // "30d / total" pair; sorts by the total (col.key).
    const recent = Number(u[col.pair] || 0), total = Number(u[col.key] || 0);
    return `<span style="color:#fff">${recent}</span><span style="color:var(--muted)"> / ${total}</span>`;
  }
  if (col.type === "num") return String(Number(u[col.key] || 0));
  const v = u[col.key];
  return v == null || v === "" ? "—" : escHtml(String(v));
}

function _adminUnifiedRender() {
  _adminUnifiedRenderGroupBar();
  const el = document.getElementById("users-unified-list");
  if (!el) return;
  // Preserve the table's scroll position across the re-render so sorting /
  // filtering / a sync-poll refresh doesn't yank the view back to the
  // top-left. The inner overflow scroller is rebuilt below, so capture from
  // the old one and reapply to the new one.
  const _prevScroller = el.querySelector(".au-scroll");
  const _prevLeft = _prevScroller ? _prevScroller.scrollLeft : 0;
  const _prevTop  = _prevScroller ? _prevScroller.scrollTop  : 0;
  let rows = _adminUnifiedData;
  if (_adminUnifiedFilter) {
    const q = _adminUnifiedFilter;
    rows = rows.filter(u =>
      String(u.clerkUsername || "").toLowerCase().includes(q) ||
      String(u.discogsUsername || "").toLowerCase().includes(q) ||
      String(u.clerkUserId || "").toLowerCase().includes(q));
  }
  if (!rows.length) {
    el.innerHTML = `<div style="color:var(--muted);padding:1rem;text-align:center">${_adminUnifiedData.length ? "No users match the filter." : "No users yet."}</div>`;
    return;
  }
  rows = _adminUnifiedSortRows(rows);
  const cols = _adminUnifiedVisibleCols();
  // First column (User) is frozen: mark its header + cells .au-sticky-col.
  const th = (c, i) => {
    const sticky = i === 0 ? " au-sticky-col" : "";
    // Action columns (e.g. the Delete button) aren't sortable — plain header.
    if (c.type === "delete") return `<th class="${sticky.trim()}" style="padding:0.3rem 0.5rem"></th>`;
    const active = _adminUnifiedSort.col === c.key;
    const arrow = active ? (_adminUnifiedSort.dir === "asc" ? " ↑" : " ↓") : "";
    return `<th class="${sticky.trim()}" style="padding:0.3rem 0.5rem;text-align:${c.align};cursor:pointer;user-select:none;white-space:nowrap${active ? ";color:var(--text)" : ""}" data-sd-click="${_sdOn(((a0) => function (event) { _adminUnifiedSortBy(a0) })(String(c.key ?? "")))}" title="Sort by ${c.label}">${c.label}${arrow}</th>`;
  };
  const head = `<thead style="font-size:0.7rem;text-transform:uppercase;letter-spacing:0.04em;color:var(--muted)"><tr>${cols.map((c, i) => th(c, i)).join("")}</tr></thead>`;
  const body = rows.map(u => {
    const tds = cols.map((c, i) => `<td class="${i === 0 ? "au-sticky-col" : ""}" style="padding:0.3rem 0.5rem;text-align:${c.align};vertical-align:top;white-space:nowrap">${_adminUnifiedCell(u, c)}</td>`).join("");
    return `<tr style="border-top:1px solid var(--border)">${tds}</tr>`;
  }).join("");
  // Two horizontal scrollbars kept in sync: a slim one on TOP (so you don't
  // have to scroll to the bottom of a long table to pan sideways) and the
  // real one under the table. Both styled wide/visible via .au-scroll CSS.
  el.innerHTML = `<div class="au-scroll-top"><div class="au-scroll-top-spacer"></div></div>
    <div class="au-scroll" style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:0.78rem">${head}<tbody>${body}</tbody></table></div>
    <div style="font-size:0.72rem;color:var(--muted);margin-top:0.4rem">${rows.length} user${rows.length === 1 ? "" : "s"}${_adminUnifiedFilter ? ` (filtered from ${_adminUnifiedData.length})` : ""}</div>`;
  // Reapply the pre-render scroll position to the freshly-built scroller and
  // wire the top scrollbar to mirror the table's width + scroll position.
  const _newScroller = el.querySelector(".au-scroll");
  const _topScroller = el.querySelector(".au-scroll-top");
  const _spacer = el.querySelector(".au-scroll-top-spacer");
  if (_newScroller) {
    if (_spacer) _spacer.style.width = _newScroller.scrollWidth + "px";
    _newScroller.scrollLeft = _prevLeft;
    _newScroller.scrollTop = _prevTop;
    if (_topScroller) {
      _topScroller.scrollLeft = _prevLeft;
      let _syncing = false;
      _newScroller.addEventListener("scroll", () => {
        if (_syncing) return; _syncing = true; _topScroller.scrollLeft = _newScroller.scrollLeft; _syncing = false;
      });
      _topScroller.addEventListener("scroll", () => {
        if (_syncing) return; _syncing = true; _newScroller.scrollLeft = _topScroller.scrollLeft; _syncing = false;
      });
    }
  }
}


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
        loadAdminUsersUnified();   // refresh the merged users table
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
      <button class="admin-btn" data-sd-click="${_sdOn(function (event) { document.getElementById('admin-db-table-overlay')?.remove() })}" style="font-size:1.2rem;padding:0 0.6rem">×</button>
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
