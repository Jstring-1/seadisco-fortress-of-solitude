// Offline-access orchestration for SeaDisco.
//
// Glue between the service worker, IndexedDB, the page, and the
// Account-page UI. Opt-in: nothing here activates unless the user
// flips the toggle on. Default visit experience is unchanged.
//
// What this file does:
//
//   1. Reads localStorage flag `sd_offline_enabled`. If absent, only
//      the static online/offline detection runs (so the UI can react
//      to connection drops mid-session) and the install-prompt event
//      is suppressed but stashed in case the user enables later.
//
//   2. If enabled: registers /sw.js so the browser starts caching
//      the shell + any matched API responses + cover images.
//
//   3. Wraps `apiFetch` with `apiFetchWithCache`. Library endpoints
//      (collection / wantlist / inventory / lists / profile) get a
//      transparent IDB fallback when the network call fails.
//
//   4. Listens to the `online` / `offline` browser events and toggles
//      a `body.is-offline` class + the connection banner text.
//
//   5. Catches `beforeinstallprompt` and stashes it. The Account-page
//      "Install app" button consumes it on click. Keeps the browser's
//      auto-banner from ever firing.
//
//   6. Exposes helpers used by web/account.js to render the toggle,
//      drive sync-now / clear-cache, and report storage stats.

(function () {
  const FLAG_KEY = "sd_offline_enabled";
  // Endpoints whose responses we cache for offline reads. Keep
  // strictly to the user's own library — never cache search results
  // (they'd surface stale/wrong data) or anything authenticated for
  // a different user. NOTE: the real paths are under /api/user/…
  // (the earlier /api/collection etc. paths were wrong; nothing
  // was actually being cached, which is why the offline collection
  // view was empty).
  const LIBRARY_PATTERNS = [
    /^\/api\/user\/profile(?:\b|$)/,
    /^\/api\/user\/folders(?:\b|\?|$|\/)/,
    /^\/api\/user\/facets(?:\b|\?)/,
    /^\/api\/user\/collection(?:\b|\?)/,
    /^\/api\/user\/wantlist(?:\b|\?)/,
    /^\/api\/user\/inventory(?:\b|\?)/,
    /^\/api\/user\/lists(?:\b|\?|$|\/)/,
    /^\/api\/user\/favorites(?:\b|\?)/,
  ];

  let _deferredInstallPrompt = null;
  let _swReg = null;

  // Per-device "offline access is on for this device" flag.
  // Distinct from the SERVER-side preference (window.sdOffline.serverPref)
  // which tracks "this user wants offline on any device they sign in to."
  // The two combine like this:
  //   server pref true + local flag true  → fully on, sync as usual
  //   server pref true + local flag false → cross-device prompt: "Cache here?"
  //   server pref false                   → local flag is the only signal
  // _setEnabled writes the LOCAL flag only; updateServerPref writes the
  // server side. enable()/disable() do both.
  function isEnabled() {
    try { return localStorage.getItem(FLAG_KEY) === "1"; } catch { return false; }
  }
  function _setEnabled(on) {
    try {
      if (on) localStorage.setItem(FLAG_KEY, "1");
      else    localStorage.removeItem(FLAG_KEY);
    } catch {}
  }

  // Per-device flag: "user already saw the cross-device prompt and said
  // not now / declined." Suppresses the prompt for this device only.
  // They can still flip the toggle manually from Account.
  const PROMPT_SHOWN_KEY = "sd_offline_prompt_dismissed";
  function _wasPromptDismissed() {
    try { return localStorage.getItem(PROMPT_SHOWN_KEY) === "1"; } catch { return false; }
  }
  function _markPromptDismissed() {
    try { localStorage.setItem(PROMPT_SHOWN_KEY, "1"); } catch {}
  }

  // ── Server-side preference ─────────────────────────────────────────
  async function getServerPref() {
    if (!window.apiFetch) return null;
    try {
      const r = await window.apiFetch("/api/user/preferences");
      if (!r.ok) return null;
      const j = await r.json();
      return (j?.prefs && typeof j.prefs === "object") ? j.prefs : {};
    } catch { return null; }
  }
  async function setServerPref(patch) {
    if (!window.apiFetch) return null;
    try {
      const r = await window.apiFetch("/api/user/preferences", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ patch }),
      });
      if (!r.ok) return null;
      const j = await r.json();
      return j?.prefs ?? null;
    } catch { return null; }
  }

  function _isLibraryUrl(url) {
    try {
      const u = url.startsWith("http") ? new URL(url) : new URL(url, location.origin);
      if (u.origin !== location.origin) return false;
      return LIBRARY_PATTERNS.some(rx => rx.test(u.pathname + u.search));
    } catch { return false; }
  }
  function _normalizeUrl(url) {
    try {
      const u = url.startsWith("http") ? new URL(url) : new URL(url, location.origin);
      return u.pathname + u.search;
    } catch { return url; }
  }
  // Strip filter/sort params for an offline-fallback lookup against
  // the canonical synced URL. We keep page + per_page (these define
  // the synced URL); everything else gets dropped. Returns null if
  // the URL isn't a library endpoint we sync.
  function _trimToBaseLibraryUrl(url) {
    try {
      const u = url.startsWith("http") ? new URL(url) : new URL(url, location.origin);
      const path = u.pathname;
      // Keep the same set of synced paths the SYNC_ENDPOINTS uses.
      const isPaginatedLib =
        /^\/api\/user\/(?:collection|wantlist|inventory)$/i.test(path);
      const isFavorites = path === "/api/user/favorites";
      const isFacets    = path === "/api/user/facets";
      if (isPaginatedLib) {
        const page    = u.searchParams.get("page")     || "1";
        const perPage = u.searchParams.get("per_page") || "96";
        return `${path}?page=${page}&per_page=${perPage}`;
      }
      if (isFavorites) {
        // Synced as ?limit=200; fall back regardless of caller's limit.
        return `${path}?limit=200`;
      }
      if (isFacets) {
        const type = u.searchParams.get("type") || "";
        return `${path}?type=${encodeURIComponent(type)}`;
      }
      return null;
    } catch { return null; }
  }

  // ── Connection banner ───────────────────────────────────────────────
  function _ensureBanner() {
    let el = document.getElementById("offline-banner");
    if (el) return el;
    el = document.createElement("div");
    el.id = "offline-banner";
    el.className = "offline-banner";
    el.innerHTML = `
      <span class="offline-banner-dot" aria-hidden="true"></span>
      <span class="offline-banner-text">You're offline — showing your cached library.</span>
    `;
    document.body.appendChild(el);
    return el;
  }
  function _onConnectionChange(isOnline) {
    document.body.classList.toggle("is-offline", !isOnline);
    if (isEnabled()) {
      _ensureBanner();
    }
    // When coming back online, ping the views that read the library
    // so they refresh from the network. Each view defines its own
    // refresher (best-effort — we just call what's exposed).
    if (isOnline && isEnabled()) {
      try { window._sdOfflineRefreshOnReconnect?.(); } catch {}
    }
  }

  // ── Service worker register / unregister ────────────────────────────
  async function _registerSw() {
    if (!("serviceWorker" in navigator)) return null;
    try {
      const reg = await navigator.serviceWorker.register("/sw.js", { scope: "/" });
      _swReg = reg;
      // Ask for persistent storage so the browser doesn't evict our
      // cache during routine cleanup. Usually granted silently for
      // engaged sites.
      if (navigator.storage?.persist) {
        try { await navigator.storage.persist(); } catch {}
      }
      return reg;
    } catch (e) {
      console.warn("[offline] SW registration failed:", e);
      return null;
    }
  }
  async function _unregisterSw() {
    try {
      const regs = await navigator.serviceWorker?.getRegistrations?.() || [];
      await Promise.all(regs.map(r => r.unregister()));
      _swReg = null;
    } catch {}
  }
  async function _clearAllCaches() {
    // Ask the active SW to wipe its caches first (so it doesn't
    // race-keep entries), then nuke from the page side as a backstop.
    try {
      const ctrl = navigator.serviceWorker?.controller;
      if (ctrl) {
        await new Promise((resolve) => {
          const ch = new MessageChannel();
          ch.port1.onmessage = () => resolve();
          ctrl.postMessage({ type: "SD_CLEAR_CACHES" }, [ch.port2]);
          setTimeout(resolve, 1000); // safety timeout
        });
      }
    } catch {}
    try {
      const keys = await caches.keys();
      await Promise.all(keys.filter(k => k.startsWith("sd-")).map(k => caches.delete(k)));
    } catch {}
  }

  // ── Fetch wrapper with IDB fallback ────────────────────────────────
  // Wraps the existing window.apiFetch so the rest of the app doesn't
  // need to know about caching. Library reads transparently fall back
  // to IDB when the network fails. Successful library responses are
  // teed into IDB for next time.
  function _installFetchWrapper() {
    if (typeof window.apiFetch !== "function") return;
    if (window._sdApiFetchWrapped) return;
    const original = window.apiFetch;
    window.apiFetch = async function apiFetchWithCache(url, options) {
      const isLib = _isLibraryUrl(url);
      const method = (options?.method || "GET").toUpperCase();
      try {
        const res = await original(url, options);
        // Tee successful library GETs into IDB for offline reads.
        if (isLib && method === "GET" && res.ok && isEnabled() && window.sdIdb) {
          try {
            const cloned = res.clone();
            const json = await cloned.json();
            window.sdIdb.libraryPut(_normalizeUrl(url), json);
          } catch { /* not JSON — skip */ }
        }
        return res;
      } catch (err) {
        // Network failed entirely. If this was a library GET and the
        // user has enabled offline mode, serve from IDB.
        if (isLib && method === "GET" && isEnabled() && window.sdIdb) {
          // 1. Exact URL match (filters + sort + page identical to
          //    something we synced).
          let row = await window.sdIdb.libraryGet(_normalizeUrl(url));
          // 2. Fallback: drop optional filter/sort params and try
          //    the canonical synced URL. Wantlist / inventory pages
          //    accumulate `&sort=artist:asc` etc. on first render
          //    even when no filter is set, so the exact URL wouldn't
          //    match the synced default. The synced URL had only
          //    page + per_page; rebuild that and look it up.
          if (!row?.json) {
            const trimmed = _trimToBaseLibraryUrl(url);
            if (trimmed && trimmed !== _normalizeUrl(url)) {
              row = await window.sdIdb.libraryGet(trimmed);
            }
          }
          if (row?.json) {
            return new Response(JSON.stringify(row.json), {
              status: 200,
              headers: { "Content-Type": "application/json", "X-Sd-Offline": "cache" },
            });
          }
        }
        throw err;
      }
    };
    window._sdApiFetchWrapped = true;
  }

  // ── Initial sync ───────────────────────────────────────────────────
  // Walk through the user's library endpoints and warm both IDB and
  // the SW's API cache. Reports progress via the callback so the
  // Account UI can show a progress bar.
  // The exact URLs collection.js / etc. call. IDB cache lookup is
  // by URL, so what we sync MUST match what the page later requests.
  // Each library kind syncs page 1 (96 items) — enough for browsing
  // a typical collection offline. Power users on page 2+ get a
  // "no cache for this page" miss; re-syncing while online would be
  // a future enhancement (sync-all-pages loop).
  const SYNC_ENDPOINTS = [
    { url: "/api/user/profile",                       label: "Profile" },
    { url: "/api/user/folders",                       label: "Folders" },
    { url: "/api/user/facets?type=collection",        label: "Collection facets" },
    { url: "/api/user/facets?type=wantlist",          label: "Wantlist facets" },
    { url: "/api/user/facets?type=inventory",         label: "Inventory facets" },
    { url: "/api/user/collection?page=1&per_page=96", label: "Collection" },
    { url: "/api/user/wantlist?page=1&per_page=96",   label: "Wantlist" },
    { url: "/api/user/inventory?page=1&per_page=96",  label: "Inventory" },
    { url: "/api/user/favorites?limit=200",           label: "Favorites" },
    { url: "/api/user/lists",                         label: "Lists" },
  ];
  async function syncNow(onProgress) {
    if (!window.apiFetch) return { ok: false, reason: "no-fetch" };
    let done = 0;
    // Lists are dynamic — we sync the index then each list's items.
    // Pre-flight to figure out the actual total work units so the
    // progress bar reflects reality.
    let listIds = [];
    const total = SYNC_ENDPOINTS.length; // base; lists/items added below
    for (const ep of SYNC_ENDPOINTS) {
      onProgress?.({ done, total, label: ep.label });
      try {
        const r = await window.apiFetch(ep.url);
        if (r.ok && window.sdIdb) {
          try {
            const json = await r.clone().json();
            await window.sdIdb.libraryPut(_normalizeUrl(ep.url), json);
            // After /api/user/lists succeeds, capture its list IDs
            // so we can fetch each list's items in the loop below.
            if (ep.url === "/api/user/lists" && Array.isArray(json?.lists)) {
              listIds = json.lists.map(l => l.id).filter(Boolean);
            }
          } catch {}
        }
      } catch { /* skip — partial is fine */ }
      done++;
      onProgress?.({ done, total, label: ep.label });
    }
    // Fetch each list's items so list view works offline.
    for (const id of listIds) {
      const url = `/api/user/lists/${id}/items`;
      onProgress?.({ done, total: total + listIds.length, label: `List ${id}` });
      try {
        const r = await window.apiFetch(url);
        if (r.ok && window.sdIdb) {
          try {
            const json = await r.clone().json();
            await window.sdIdb.libraryPut(_normalizeUrl(url), json);
          } catch {}
        }
      } catch {}
      done++;
    }
    if (window.sdIdb) await window.sdIdb.metaSet("lastSyncAt", Date.now());
    return { ok: true, done, total: total + listIds.length };
  }

  // ── Storage stats ───────────────────────────────────────────────────
  async function getStorageStats() {
    const out = {
      enabled: isEnabled(),
      records: 0,
      libraryBytes: 0,    // IDB JSON for the user's library endpoints
      shellBytes: 0,      // Cache API: HTML/JS/CSS app shell
      apiCacheBytes: 0,   // Cache API: SW's API response cache
      imageBytes: 0,      // Cache API: cover images
      totalBytes: 0,      // Sum of the above (deterministic, our caches only)
      quotaBytes: null,   // Browser quota ceiling
      lastSyncAt: null,
    };
    if (window.sdIdb) {
      try {
        const lib = await window.sdIdb.librarySize();
        out.records = lib.count;
        out.libraryBytes = lib.bytes;
      } catch {}
      try { out.lastSyncAt = await window.sdIdb.metaGet("lastSyncAt"); } catch {}
    }
    // Sum bytes from each of OUR named caches. navigator.storage.estimate()
    // reports the whole origin (Clerk, GA, browser bookkeeping, etc.) and
    // drifts by hundreds of KB across calls — confusing on a status line
    // that's supposed to reflect "what offline mode is using." We measure
    // exactly the sd-* caches plus the IDB library, and only that.
    if (typeof caches !== "undefined") {
      try {
        const cacheNames = (await caches.keys()).filter(k => k.startsWith("sd-"));
        const sizes = await Promise.all(cacheNames.map(async (name) => {
          let bytes = 0;
          try {
            const cache = await caches.open(name);
            const reqs = await cache.keys();
            for (const req of reqs) {
              try {
                const r = await cache.match(req);
                if (!r) continue;
                const blob = await r.blob();
                bytes += blob.size;
              } catch {}
            }
          } catch {}
          return { name, bytes };
        }));
        for (const { name, bytes } of sizes) {
          if (name.startsWith("sd-shell-")) out.shellBytes += bytes;
          else if (name.startsWith("sd-api-")) out.apiCacheBytes += bytes;
          else if (name.startsWith("sd-img-")) out.imageBytes += bytes;
        }
      } catch {}
    }
    out.totalBytes = out.libraryBytes + out.shellBytes + out.apiCacheBytes + out.imageBytes;
    if (navigator.storage?.estimate) {
      try {
        const est = await navigator.storage.estimate();
        out.quotaBytes = est.quota || null;
      } catch {}
    }
    return out;
  }

  // ── Enable / disable ───────────────────────────────────────────────
  // enable() / disable() are user-initiated (Account toggle, prompt
  // accept). Both update the LOCAL flag immediately and the SERVER
  // preference in the background. Server-side write failure is non-
  // fatal; the local toggle still works for this device.
  async function enable(onProgress) {
    _setEnabled(true);
    setServerPref({ offlineEnabled: true }).catch(() => {});
    // Clear the prompt-dismissed marker — they're saying yes now.
    try { localStorage.removeItem(PROMPT_SHOWN_KEY); } catch {}
    await _registerSw();
    _ensureBanner();
    _onConnectionChange(navigator.onLine);
    return syncNow(onProgress);
  }
  async function disable() {
    _setEnabled(false);
    setServerPref({ offlineEnabled: false }).catch(() => {});
    await _clearAllCaches();
    if (window.sdIdb) {
      try { await window.sdIdb.libraryClear(); } catch {}
      try { await window.sdIdb.metaClear(); }    catch {}
    }
    await _unregisterSw();
    document.body.classList.remove("is-offline");
    const banner = document.getElementById("offline-banner");
    if (banner) banner.remove();
  }

  // ── Cross-device prompt ────────────────────────────────────────────
  // Called after Clerk has resolved a signed-in user, while online,
  // when offlineEnabled is true server-side but the local flag isn't
  // set. Renders a one-time non-blocking banner asking to cache on
  // this device. Stays out of the way if the user dismisses it.
  function _showCrossDevicePrompt() {
    if (document.getElementById("sd-offline-cross-prompt")) return;
    const wrap = document.createElement("div");
    wrap.id = "sd-offline-cross-prompt";
    wrap.className = "sd-offline-cross-prompt";
    wrap.innerHTML = `
      <span class="sd-offline-cross-msg">Offline access is on for your account. Cache your library on this device too?</span>
      <button type="button" class="sd-offline-cross-yes">Yes</button>
      <button type="button" class="sd-offline-cross-no">Not now</button>
    `;
    document.body.appendChild(wrap);
    const dismiss = (mark) => {
      if (mark) _markPromptDismissed();
      wrap.remove();
    };
    wrap.querySelector(".sd-offline-cross-yes").addEventListener("click", async () => {
      const btn = wrap.querySelector(".sd-offline-cross-yes");
      btn.disabled = true; btn.textContent = "Syncing…";
      try { await enable(); } catch {}
      dismiss(false);
      if (typeof window._sdOfflineRenderAccount === "function") {
        try { window._sdOfflineRenderAccount(); } catch {}
      }
    });
    wrap.querySelector(".sd-offline-cross-no").addEventListener("click", () => dismiss(true));
  }
  async function _maybePromptCrossDevice() {
    if (isEnabled()) return;          // already on locally
    if (_wasPromptDismissed()) return; // user said not now on this device
    if (!navigator.onLine) return;     // wait until back online
    if (!window._clerk?.user) return;  // need a real signed-in user
    if (window._sdOfflineMode) return; // already in offline-boot mode
    const prefs = await getServerPref();
    if (prefs?.offlineEnabled) _showCrossDevicePrompt();
  }

  // ── Install prompt ─────────────────────────────────────────────────
  // Catch the browser's "ready to install" event and stash it. The
  // Account-page button uses promptInstall() to fire it on demand.
  // Without this, Chrome on Android would auto-banner us, which the
  // user explicitly doesn't want.
  window.addEventListener("beforeinstallprompt", (e) => {
    e.preventDefault();
    _deferredInstallPrompt = e;
    // Re-render the offline section if it's visible — the install
    // button was disabled and now wants to enable.
    try { window._sdOfflineRenderAccount?.(); } catch {}
  });
  function canInstall() { return !!_deferredInstallPrompt; }
  async function promptInstall() {
    if (!_deferredInstallPrompt) return { outcome: "unavailable" };
    const evt = _deferredInstallPrompt;
    _deferredInstallPrompt = null; // single-use
    try {
      evt.prompt();
      const { outcome } = await evt.userChoice;
      return { outcome };
    } catch {
      return { outcome: "error" };
    } finally {
      try { window._sdOfflineRenderAccount?.(); } catch {}
    }
  }
  // True once the app is running as an installed PWA. iOS uses
  // navigator.standalone; Chrome/Edge use display-mode media query.
  function isInstalled() {
    try {
      if (window.matchMedia?.("(display-mode: standalone)").matches) return true;
      if (window.navigator.standalone === true) return true;
    } catch {}
    return false;
  }

  // ── Connection events ──────────────────────────────────────────────
  window.addEventListener("online",  () => _onConnectionChange(true));
  window.addEventListener("offline", () => _onConnectionChange(false));

  // ── Boot ───────────────────────────────────────────────────────────
  // Wrap apiFetch as soon as shared.js has defined it. Wrapping is
  // cheap and only changes behavior when offline mode is enabled.
  if (typeof window.apiFetch === "function") {
    _installFetchWrapper();
  } else {
    document.addEventListener("DOMContentLoaded", _installFetchWrapper);
  }

  // If the user previously opted in, re-register the SW on this load
  // (fresh tabs need it; otherwise the SW only takes over the *next*
  // tab).
  document.addEventListener("DOMContentLoaded", () => {
    if (isEnabled()) {
      _registerSw();
      _onConnectionChange(navigator.onLine);
    } else {
      // Just reflect the offline state visually without showing the
      // banner (which is reserved for when caching's actually on).
      document.body.classList.toggle("is-offline", !navigator.onLine);
    }
    // Cross-device prompt: poll briefly for Clerk to resolve, then
    // ask the server if this user has offlineEnabled set elsewhere.
    // If so, surface the "Cache on this device too?" banner. Skipped
    // entirely if the local flag is already on or the user has
    // dismissed the prompt before.
    if (!isEnabled() && !_wasPromptDismissed()) {
      let waited = 0;
      const iv = setInterval(() => {
        waited += 250;
        if (window._clerk?.user) {
          clearInterval(iv);
          _maybePromptCrossDevice().catch(() => {});
        } else if (waited >= 10000) {
          clearInterval(iv);
        }
      }, 250);
    }
  });

  // Public API used by web/account.js
  window.sdOffline = {
    isEnabled,
    enable,
    disable,
    syncNow,
    getStorageStats,
    canInstall,
    promptInstall,
    isInstalled,
  };
})();
