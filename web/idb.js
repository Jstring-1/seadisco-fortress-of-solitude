// Small IndexedDB wrapper for SeaDisco's offline cache.
//
// Schema:
//   db: "seadisco"  version 1
//     object store "library"   — keyPath: "url"
//       { url, json, fetchedAt, size }
//     object store "meta"      — keyPath: "key"
//       { key, value }
//
// `library` stores cached responses for the user's library endpoints
// (collection, wantlist, inventory, lists). `meta` stores singleton
// status fields (last sync time, schema version, install flag). Both
// are tiny and used by web/offline.js + the offline-fallback path in
// shared.js' apiFetch wrapper.
//
// All functions return Promises and resolve to plain JS objects (no
// IDBRequest leaks into callers). Errors fall back to "no-op" — IDB
// is a best-effort cache; the app should never break because IDB is
// unavailable (private mode, quota full, etc.).

(function () {
  const DB_NAME = "seadisco";
  const DB_VERSION = 1;
  let _dbPromise = null;

  function _openDb() {
    if (_dbPromise) return _dbPromise;
    _dbPromise = new Promise((resolve, reject) => {
      if (!window.indexedDB) { reject(new Error("indexedDB unavailable")); return; }
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains("library")) {
          db.createObjectStore("library", { keyPath: "url" });
        }
        if (!db.objectStoreNames.contains("meta")) {
          db.createObjectStore("meta", { keyPath: "key" });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror   = () => reject(req.error || new Error("indexedDB open failed"));
    });
    return _dbPromise;
  }

  function _tx(storeName, mode = "readonly") {
    return _openDb().then((db) => db.transaction(storeName, mode).objectStore(storeName));
  }

  function _wrap(req) {
    return new Promise((resolve, reject) => {
      req.onsuccess = () => resolve(req.result);
      req.onerror   = () => reject(req.error);
    });
  }

  // ── library store ───────────────────────────────────────────────────
  // Cached JSON for a fetched library endpoint. `url` is the request
  // URL (origin-relative, e.g. "/api/collection?folder=0"), so query
  // params count toward identity — different filters cache separately.
  async function libraryPut(url, json) {
    try {
      const store = await _tx("library", "readwrite");
      const size = JSON.stringify(json).length;
      await _wrap(store.put({ url, json, fetchedAt: Date.now(), size }));
      return true;
    } catch { return false; }
  }
  async function libraryGet(url) {
    try {
      const store = await _tx("library");
      const row = await _wrap(store.get(url));
      return row || null;
    } catch { return null; }
  }
  async function libraryAll() {
    try {
      const store = await _tx("library");
      return await _wrap(store.getAll()) || [];
    } catch { return []; }
  }
  async function libraryClear() {
    try {
      const store = await _tx("library", "readwrite");
      await _wrap(store.clear());
      return true;
    } catch { return false; }
  }
  async function librarySize() {
    const rows = await libraryAll();
    let bytes = 0, count = 0;
    for (const r of rows) { bytes += r.size || 0; count++; }
    return { bytes, count };
  }

  // ── meta store ──────────────────────────────────────────────────────
  async function metaSet(key, value) {
    try {
      const store = await _tx("meta", "readwrite");
      await _wrap(store.put({ key, value }));
      return true;
    } catch { return false; }
  }
  async function metaGet(key) {
    try {
      const store = await _tx("meta");
      const row = await _wrap(store.get(key));
      return row ? row.value : null;
    } catch { return null; }
  }
  async function metaClear() {
    try {
      const store = await _tx("meta", "readwrite");
      await _wrap(store.clear());
      return true;
    } catch { return false; }
  }

  // ── full nuke (called from "Clear cache" button) ────────────────────
  async function destroy() {
    try { _dbPromise = null; } catch {}
    return new Promise((resolve) => {
      try {
        const req = indexedDB.deleteDatabase(DB_NAME);
        req.onsuccess = req.onerror = req.onblocked = () => resolve(true);
      } catch { resolve(false); }
    });
  }

  window.sdIdb = {
    libraryPut, libraryGet, libraryAll, libraryClear, librarySize,
    metaSet, metaGet, metaClear,
    destroy,
  };
})();
