const CACHE_NAME = "seadisco-v3";
const SHELL_FILES = [
  "/",
  "/style.css",
  "/shared.js",
  "/utils.js",
  "/search.js",
  "/collection.js",
  "/modal.js",
  "/drops.js",
  "/feed.js",
  "/gear.js",
  "/live.js",
  "/app.js",
  "/favicon.ico",
  "/favicon-16x16.png",
  "/favicon-32x32.png",
  "/apple-touch-icon.png",
  "/seadisco-logo.png",
  "/seadisco-logo.webp"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_FILES))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // API calls: network only (never cache)
  if (url.pathname.startsWith("/api/")) {
    return;
  }

  // Static assets: stale-while-revalidate
  event.respondWith(
    caches.match(event.request).then((cached) => {
      const fetchPromise = fetch(event.request)
        .then((response) => {
          if (response.ok) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
          }
          return response;
        })
        .catch(() => cached);
      return cached || fetchPromise;
    })
  );
});
