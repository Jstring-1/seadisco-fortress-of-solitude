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
  const LIBRARY_PATTERNS = [
    /^\/api\/collection(?:\b|\?)/,
    /^\/api\/wantlist(?:\b|\?)/,
    /^\/api\/inventory(?:\b|\?)/,
    /^\/api\/lists(?:\b|\?)/,
    /^\/api\/user\/profile(?:\b|$)/,
  ];

  let _deferredInstallPrompt = null;
  let _swReg = null;

  function isEnabled() {
    try { return localStorage.getItem(FLAG_KEY) === "1"; } catch { return false; }
  }
  function _setEnabled(on) {
    try {
      if (on) localStorage.setItem(FLAG_KEY, "1");
      else    localStorage.removeItem(FLAG_KEY);
    } catch {}
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
          const row = await window.sdIdb.libraryGet(_normalizeUrl(url));
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
  const SYNC_ENDPOINTS = [
    { url: "/api/user/profile",            label: "Profile" },
    { url: "/api/collection?per_page=500", label: "Collection" },
    { url: "/api/wantlist?per_page=500",   label: "Wantlist" },
    { url: "/api/inventory?per_page=500",  label: "Inventory" },
    { url: "/api/lists",                   label: "Lists" },
  ];
  async function syncNow(onProgress) {
    if (!window.apiFetch) return { ok: false, reason: "no-fetch" };
    let done = 0;
    const total = SYNC_ENDPOINTS.length;
    for (const ep of SYNC_ENDPOINTS) {
      onProgress?.({ done, total, label: ep.label });
      try {
        const r = await window.apiFetch(ep.url);
        if (r.ok && window.sdIdb) {
          try {
            const json = await r.clone().json();
            await window.sdIdb.libraryPut(_normalizeUrl(ep.url), json);
          } catch {}
        }
      } catch { /* skip — partial is fine */ }
      done++;
      onProgress?.({ done, total, label: ep.label });
    }
    if (window.sdIdb) await window.sdIdb.metaSet("lastSyncAt", Date.now());
    return { ok: true, done, total };
  }

  // ── Storage stats ───────────────────────────────────────────────────
  async function getStorageStats() {
    const out = {
      enabled: isEnabled(),
      records: 0,
      libraryBytes: 0,
      cacheBytes: 0,
      totalBytes: 0,
      lastSyncAt: null,
      quotaBytes: null,
    };
    if (window.sdIdb) {
      try {
        const lib = await window.sdIdb.librarySize();
        out.records = lib.count;
        out.libraryBytes = lib.bytes;
      } catch {}
      try { out.lastSyncAt = await window.sdIdb.metaGet("lastSyncAt"); } catch {}
    }
    // Cache API + total estimate via storage.estimate() — gives us
    // image cache size implicitly (everything else our SW caches is
    // tiny).
    if (navigator.storage?.estimate) {
      try {
        const est = await navigator.storage.estimate();
        out.totalBytes = est.usage || 0;
        out.quotaBytes = est.quota || null;
        out.cacheBytes = Math.max(0, (est.usage || 0) - out.libraryBytes);
      } catch {}
    }
    return out;
  }

  // ── Enable / disable ───────────────────────────────────────────────
  async function enable(onProgress) {
    _setEnabled(true);
    await _registerSw();
    _ensureBanner();
    _onConnectionChange(navigator.onLine);
    return syncNow(onProgress);
  }
  async function disable() {
    _setEnabled(false);
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
