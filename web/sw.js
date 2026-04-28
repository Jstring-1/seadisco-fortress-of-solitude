// SeaDisco service worker.
//
// Registered only when the user opts in to offline access from the
// Account page (web/offline.js drives the lifecycle). Never installed
// by default — the goal is for the default visitor experience to be
// identical to having no service worker at all.
//
// Strategy by request kind:
//
//   App shell (/, /*.js, /*.css, /*.svg, /icon-512.svg, manifest)
//     → cache-first, fall back to network. After install we have a
//       full snapshot; offline reload boots straight from cache.
//
//   User library (/api/collection, /api/wantlist, /api/inventory,
//                 /api/lists, /api/user/profile)
//     → network-first, cache fallback. Online: always fresh; offline:
//       last-known copy. The IDB layer (web/idb.js) holds a parallel
//       JSON copy used by the page-side code; this Cache API copy is
//       so the SW can answer the request without involving the page.
//
//   Cover images (i.discogs.com, st.discogs.com, archive.org images)
//     → cache-first, populate on first view. Capped by an LRU sweep
//       to keep storage bounded.
//
//   Everything else → passthrough. We don't cache search results,
//     marketplace, AI calls, queue endpoints, etc.

const SW_VERSION = "v1-20260427.2131";
const SHELL_CACHE = `sd-shell-${SW_VERSION}`;
const API_CACHE   = `sd-api-${SW_VERSION}`;
const IMG_CACHE   = `sd-img-${SW_VERSION}`;
const IMG_CACHE_MAX_BYTES = 250 * 1024 * 1024; // ~250 MB ceiling

// Files to pre-cache so the offline reload has the whole shell ready
// without any network. A miss falls back to network on first encounter.
const SHELL_URLS = [
  "/",
  "/index.html",
  "/site.webmanifest",
  "/icon-512.svg",
  "/style.css",
  "/shared.js",
  "/utils.js",
  "/search.js",
  "/collection.js",
  "/account.js",
  "/orders.js",
  "/modal.js",
  "/app.js",
  "/inventory-editor.js",
  "/loc.js",
  "/archive.js",
  "/youtube.js",
  "/queue.js",
  "/idb.js",
  "/offline.js",
];

// API endpoints whose responses we cache for offline read. Keep
// strictly to the user's own library — never cache search results
// (would surface stale/wrong data) or anything authenticated for a
// different user. Real paths are under /api/user/… (the earlier
// /api/collection etc. patterns matched nothing — the page actually
// requests /api/user/collection).
const API_CACHE_PATTERNS = [
  /^\/api\/user\/profile(?:\b|$)/,
  /^\/api\/user\/folders(?:\b|\?|$|\/)/,
  /^\/api\/user\/facets(?:\b|\?)/,
  /^\/api\/user\/collection(?:\b|\?)/,
  /^\/api\/user\/wantlist(?:\b|\?)/,
  /^\/api\/user\/inventory(?:\b|\?)/,
  /^\/api\/user\/lists(?:\b|\?|$|\/)/,
  /^\/api\/user\/favorites(?:\b|\?)/,
  /^\/api\/user\/preferences(?:\b|$)/,
];

const IMG_HOST_PATTERNS = [
  /(^|\.)discogs\.com$/i,
  /(^|\.)archive\.org$/i,
  /(^|\.)ytimg\.com$/i,
];

function _isApiCacheable(url) {
  if (url.origin !== self.location.origin) return false;
  return API_CACHE_PATTERNS.some(rx => rx.test(url.pathname + url.search));
}

function _isShellRequest(url) {
  if (url.origin !== self.location.origin) return false;
  if (url.pathname === "/" || url.pathname === "/index.html") return true;
  return /\.(?:js|css|svg|webmanifest|woff2?)$/i.test(url.pathname);
}

function _isImageHost(url) {
  return IMG_HOST_PATTERNS.some(rx => rx.test(url.hostname));
}

self.addEventListener("install", (event) => {
  event.waitUntil((async () => {
    const cache = await caches.open(SHELL_CACHE);
    // Best-effort. Some files might 404 in dev; don't block install.
    await Promise.all(SHELL_URLS.map(async (u) => {
      try { await cache.add(u); } catch { /* skip */ }
    }));
    self.skipWaiting();
  })());
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.map((k) => {
      if (![SHELL_CACHE, API_CACHE, IMG_CACHE].includes(k)) return caches.delete(k);
      return null;
    }));
    await self.clients.claim();
  })());
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;
  let url;
  try { url = new URL(req.url); } catch { return; }

  // Don't touch range requests, cross-origin auth flows, etc.
  if (req.headers.has("range")) return;

  // App shell — cache-first, network-revalidate behind.
  if (_isShellRequest(url)) {
    event.respondWith(_cacheFirst(req, SHELL_CACHE));
    return;
  }

  // User library — network-first, cache fallback.
  if (_isApiCacheable(url)) {
    event.respondWith(_networkFirst(req, API_CACHE));
    return;
  }

  // Cover images — cache-first, populate on first miss.
  if (_isImageHost(url) && /\.(?:jpe?g|png|webp|gif|svg)(?:\?|$)/i.test(url.pathname + url.search)) {
    event.respondWith(_cacheFirstWithLimit(req, IMG_CACHE, IMG_CACHE_MAX_BYTES));
    return;
  }

  // Everything else — passthrough. SW out of the way.
});

async function _cacheFirst(req, cacheName) {
  const cache = await caches.open(cacheName);
  // ignoreSearch: the page requests JS/CSS with a `?v=…` cache-bust
  // query that the pre-cached entries don't have. Without this, every
  // shell file would miss the cache and re-fetch on every load.
  const cached = await cache.match(req, { ignoreSearch: true });
  if (cached) {
    // Refresh in background so the next load gets newer code.
    fetch(req).then((res) => {
      if (res && res.ok) cache.put(req, res.clone());
    }).catch(() => {});
    return cached;
  }
  try {
    const res = await fetch(req);
    if (res && res.ok) cache.put(req, res.clone());
    return res;
  } catch (e) {
    // Last resort: try to return the index for SPA navigation when
    // offline and the requested file isn't cached. Avoids the
    // browser's "no internet" page on a deep-link offline.
    if (req.mode === "navigate") {
      const root = await cache.match("/", { ignoreSearch: true });
      if (root) return root;
    }
    throw e;
  }
}

async function _networkFirst(req, cacheName) {
  const cache = await caches.open(cacheName);
  try {
    const res = await fetch(req);
    if (res && res.ok) cache.put(req, res.clone());
    return res;
  } catch {
    const cached = await cache.match(req);
    if (cached) return cached;
    // No network, no cache — let the page handle the failure (it'll
    // try IDB next via the shared.js wrapper).
    return new Response(JSON.stringify({ offline: true }), {
      status: 503,
      headers: { "Content-Type": "application/json", "X-Sd-Offline": "1" },
    });
  }
}

async function _cacheFirstWithLimit(req, cacheName, maxBytes) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(req);
  if (cached) return cached;
  try {
    const res = await fetch(req);
    // Cross-origin images (Discogs CDN, archive.org thumbs) come back
    // with type: "opaque" and ok: false because the browser hides the
    // status code without CORS. They're still cacheable + usable as
    // <img src>, so accept them too.
    if (res && (res.ok || res.type === "opaque")) {
      cache.put(req, res.clone());
      // Best-effort LRU sweep — fire-and-forget, no await so the
      // caller doesn't wait on the bookkeeping.
      _trimImgCache(cache, maxBytes).catch(() => {});
    }
    return res;
  } catch (e) {
    // Image loads while offline — return a 1×1 transparent PNG so
    // the layout doesn't go ragged. 67-byte data URL.
    return new Response(
      Uint8Array.from(atob("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="), c => c.charCodeAt(0)),
      { status: 200, headers: { "Content-Type": "image/png" } }
    );
  }
}

// Crude LRU: count total bytes in the image cache; if over the cap,
// delete oldest entries (by Cache API insertion order — keys() returns
// in insertion order) until under. The Cache API doesn't expose
// per-entry size or atime, so this is approximate.
async function _trimImgCache(cache, maxBytes) {
  const keys = await cache.keys();
  if (keys.length === 0) return;
  // Estimate size by re-reading each response's blob length. This is
  // O(n) per sweep but only runs after a put when over cap, and we
  // bail early once we're under the limit again.
  let total = 0;
  const sizes = [];
  for (const req of keys) {
    try {
      const r = await cache.match(req);
      if (!r) { sizes.push(0); continue; }
      const blob = await r.blob();
      sizes.push(blob.size);
      total += blob.size;
    } catch { sizes.push(0); }
  }
  if (total <= maxBytes) return;
  for (let i = 0; i < keys.length && total > maxBytes; i++) {
    try { await cache.delete(keys[i]); } catch {}
    total -= sizes[i] || 0;
  }
}

// Custom messages from the page — used for "clear all caches" without
// having to unregister + re-register the SW.
self.addEventListener("message", async (event) => {
  if (!event.data) return;
  if (event.data.type === "SD_CLEAR_CACHES") {
    const keys = await caches.keys();
    await Promise.all(keys.filter(k => k.startsWith("sd-")).map(k => caches.delete(k)));
    if (event.ports && event.ports[0]) event.ports[0].postMessage({ ok: true });
  } else if (event.data.type === "SD_SKIP_WAITING") {
    self.skipWaiting();
  }
});
