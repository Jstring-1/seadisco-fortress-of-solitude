// ── Project Gutenberg view ─────────────────────────────────────────────
//
// Lazy-loaded module. Powers the /?v=gutenberg tab in the Discovery
// group. Admin-only initially (server gates via requireGutenbergAccess).
//
// Architecture mirrors archive.js / loc.js:
//   - Two-tab view (Search / Library) toggled via _gutenbergSwitchTab.
//   - Search results render as cards; clicking a card opens the reader
//     overlay (#gutenberg-reader-overlay) which fetches the HTML body
//     from /api/gutenberg/book/:id and injects it into the reader.
//   - DB-as-cache server-side: first read fetches gutenberg.org, every
//     subsequent open serves directly from Postgres.
//   - Bookmarks: one auto-resume position per (user, book), plus N
//     manual bookmarks the user can pin from any scroll position.

// ── Module state ────────────────────────────────────────────────────
let _gutenbergSearchInflight = false;
let _gutenbergSearchPage     = 1;
let _gutenbergSearchQuery    = "";
let _gutenbergSearchLang     = "";
let _gutenbergSearchHasMore  = false;
let _gutenbergSavedIds       = new Set();
let _gutenbergSavedItems     = [];
// Active reader state — used by the auto-bookmark scroll handler.
let _gutenbergReaderBookId   = null;
let _gutenbergReaderTitle    = "";
let _gutenbergReaderBookmarks = { auto: null, manual: [] };
let _gutenbergScrollTimer    = null;
const _GUTENBERG_AUTO_SAVE_MS = 1500;   // debounce window for auto-bookmark

// ── View bootstrap ──────────────────────────────────────────────────
function initGutenbergView() {
  const view = document.getElementById("gutenberg-view");
  if (!view) return;
  // Run once per page lifecycle — repeat calls are idempotent.
  if (!view.dataset.initialized) {
    view.dataset.initialized = "1";
    // Default tab is Search. The HTML markup already has the search
    // panel visible and saved hidden, so no toggle needed on first
    // open. Run saved fetch in the background so the Library tab is
    // ready instantly on first switch.
    _gutenbergLoadSaved().catch(() => {});
  }
  // Focus the input on (re-)entry so the user can start typing
  // immediately. Mirrors LOC / Wiki behaviour.
  setTimeout(() => document.getElementById("gutenberg-q")?.focus(), 50);
}
window.initGutenbergView = initGutenbergView;

// ── Tab switching ───────────────────────────────────────────────────
function _gutenbergSwitchTab(tab) {
  const view = document.getElementById("gutenberg-view");
  if (!view) return;
  view.querySelectorAll(".loc-tab").forEach(b => {
    const tabKey = b.classList.contains("loc-tab-search") ? "search" : "saved";
    b.classList.toggle("active", tabKey === tab);
  });
  document.querySelector(".gutenberg-panel-search").style.display = tab === "search" ? "block" : "none";
  document.querySelector(".gutenberg-panel-saved").style.display  = tab === "saved"  ? "block" : "none";
  if (tab === "saved") _gutenbergRenderSaved();
}
window._gutenbergSwitchTab = _gutenbergSwitchTab;

// ── Search ──────────────────────────────────────────────────────────
async function runGutenbergSearch(q, opts) {
  const append = !!(opts && opts.append);
  q = String(q || "").trim();
  if (!q) {
    document.getElementById("gutenberg-results").innerHTML =
      `<div class="loc-empty">Type a title, author, or topic to search Project Gutenberg.</div>`;
    document.getElementById("gutenberg-pagination").innerHTML = "";
    return;
  }
  if (_gutenbergSearchInflight) return;
  _gutenbergSearchInflight = true;
  if (!append) {
    _gutenbergSearchPage = 1;
    _gutenbergSearchQuery = q;
    document.getElementById("gutenberg-results").innerHTML =
      `<div class="loc-empty">Searching…</div>`;
  } else {
    _gutenbergSearchPage += 1;
  }
  const langSel = document.getElementById("gutenberg-lang");
  _gutenbergSearchLang = langSel ? langSel.value : "";
  try {
    const params = new URLSearchParams();
    params.set("q", q);
    if (_gutenbergSearchPage > 1) params.set("page", String(_gutenbergSearchPage));
    if (_gutenbergSearchLang) params.set("lang", _gutenbergSearchLang);
    const r = await apiFetch(`/api/gutenberg/search?${params.toString()}`);
    if (!r.ok) {
      document.getElementById("gutenberg-results").innerHTML =
        `<div class="loc-empty">Search failed (HTTP ${r.status}).</div>`;
      return;
    }
    const j = await r.json();
    const items = Array.isArray(j?.items) ? j.items : [];
    _gutenbergSearchHasMore = !!j?.hasMore;
    if (!append && !items.length) {
      document.getElementById("gutenberg-results").innerHTML =
        `<div class="loc-empty">No matches.</div>`;
      document.getElementById("gutenberg-pagination").innerHTML = "";
      return;
    }
    const html = items.map(_gutenbergCardHtml).join("");
    const target = document.getElementById("gutenberg-results");
    if (append) target.insertAdjacentHTML("beforeend", html);
    else        target.innerHTML = html;
    _gutenbergRenderPagination();
  } catch (e) {
    console.warn("[gutenberg/search]", e);
    document.getElementById("gutenberg-results").innerHTML =
      `<div class="loc-empty">Search error.</div>`;
  } finally {
    _gutenbergSearchInflight = false;
  }
}
window.runGutenbergSearch = runGutenbergSearch;

function _gutenbergRenderPagination() {
  const pag = document.getElementById("gutenberg-pagination");
  if (!pag) return;
  pag.innerHTML = _gutenbergSearchHasMore
    ? `<button type="button" class="loc-submit" onclick="runGutenbergSearch(document.getElementById('gutenberg-q').value,{append:true})">Load more</button>`
    : "";
}

// ── Card rendering ──────────────────────────────────────────────────
function _gutenbergCardHtml(b) {
  const authors = Array.isArray(b.authors) && b.authors.length
    ? b.authors.map(a => escHtml(a?.name || "")).filter(Boolean).join(", ")
    : "Unknown author";
  const langs = Array.isArray(b.languages) && b.languages.length
    ? b.languages.map(l => escHtml(String(l).toUpperCase())).join(" / ")
    : "";
  const subjects = Array.isArray(b.subjects) && b.subjects.length
    ? b.subjects.slice(0, 3).map(s => `<span class="gutenberg-subject-chip">${escHtml(s)}</span>`).join("")
    : "";
  const isSaved = _gutenbergSavedIds.has(b.id);
  const saveBtn = `<button type="button" class="archive-btn" onclick="_gutenbergToggleSave(${b.id}, this)" title="${isSaved ? "Remove from Library" : "Save to Library"}">${isSaved ? "✓ Saved" : "+ Save"}</button>`;
  const cover = b.cover
    ? `<img class="gutenberg-cover" src="${escHtml(b.cover)}" alt="" loading="lazy" decoding="async">`
    : `<div class="gutenberg-cover gutenberg-cover-placeholder">📖</div>`;
  return `
    <div class="gutenberg-card" data-book-id="${b.id}">
      ${cover}
      <div class="gutenberg-card-body">
        <div class="gutenberg-card-title">${escHtml(b.title || `Book ${b.id}`)}</div>
        <div class="gutenberg-card-author">${authors}</div>
        ${langs ? `<div class="gutenberg-card-langs">${langs}</div>` : ""}
        ${subjects ? `<div class="gutenberg-card-subjects">${subjects}</div>` : ""}
        <div class="gutenberg-card-actions">
          <button type="button" class="archive-btn archive-btn-suggest" onclick="_gutenbergOpenReader(${b.id}, ${JSON.stringify(b.title || "").replace(/"/g, "&quot;")})">Read</button>
          ${saveBtn}
          <a href="https://www.gutenberg.org/ebooks/${b.id}" target="_blank" rel="noopener" class="archive-btn" style="text-decoration:none">Gutenberg ↗</a>
        </div>
      </div>
    </div>`;
}

// ── Save / unsave ───────────────────────────────────────────────────
async function _gutenbergToggleSave(bookId, btn) {
  bookId = Number(bookId);
  if (!Number.isFinite(bookId) || bookId <= 0) return;
  const currentlySaved = _gutenbergSavedIds.has(bookId);
  if (btn) btn.disabled = true;
  try {
    const method = currentlySaved ? "DELETE" : "POST";
    const r = await apiFetch("/api/user/gutenberg-saves", {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: bookId }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    if (currentlySaved) _gutenbergSavedIds.delete(bookId);
    else                _gutenbergSavedIds.add(bookId);
    if (btn) {
      btn.textContent = currentlySaved ? "+ Save" : "✓ Saved";
      btn.title = currentlySaved ? "Save to Library" : "Remove from Library";
    }
    // Refresh saved list metadata so the Library tab is current.
    await _gutenbergLoadSaved();
    if (typeof showToast === "function") {
      showToast(currentlySaved ? "Removed from Library" : "Saved to Library");
    }
  } catch (e) {
    console.warn("[gutenberg/save]", e);
    if (typeof showToast === "function") showToast("Save failed", "error");
  } finally {
    if (btn) btn.disabled = false;
  }
}
window._gutenbergToggleSave = _gutenbergToggleSave;

async function _gutenbergLoadSaved() {
  try {
    const r = await apiFetch("/api/user/gutenberg-saves");
    if (!r.ok) return;
    const j = await r.json();
    _gutenbergSavedItems = Array.isArray(j?.items) ? j.items : [];
    _gutenbergSavedIds = new Set(_gutenbergSavedItems.map(it => Number(it.id)));
  } catch { /* ignore */ }
}

function _gutenbergRenderSaved() {
  const target = document.getElementById("gutenberg-saved-results");
  if (!target) return;
  if (!_gutenbergSavedItems.length) {
    target.innerHTML = `<div class="loc-empty">No saved books yet — search and click + Save to add some.</div>`;
    return;
  }
  target.innerHTML = _gutenbergSavedItems.map(b => _gutenbergCardHtml({
    id: b.id,
    title: b.title,
    authors: b.authors,
    languages: b.languages,
    subjects: [],
    cover: "",
  })).join("");
  const count = document.getElementById("gutenberg-saved-count");
  if (count) count.textContent = String(_gutenbergSavedItems.length);
}

// ── Reader overlay ──────────────────────────────────────────────────
async function _gutenbergOpenReader(bookId, fallbackTitle) {
  bookId = Number(bookId);
  if (!Number.isFinite(bookId) || bookId <= 0) return;
  const overlay = document.getElementById("gutenberg-reader-overlay");
  const titleEl = document.getElementById("gutenberg-reader-title");
  const bodyEl  = document.getElementById("gutenberg-reader-body");
  const bmkList = document.getElementById("gutenberg-reader-bookmarks-list");
  if (!overlay || !bodyEl) return;
  overlay.classList.add("open");
  document.body.classList.add("gutenberg-reader-open");
  if (titleEl) titleEl.textContent = fallbackTitle || `Book ${bookId}`;
  bodyEl.innerHTML = `<div class="loc-empty" style="padding:3rem 1rem">Loading book… first read may take a few seconds while we fetch from Gutenberg.</div>`;
  if (bmkList) bmkList.innerHTML = "";
  _gutenbergReaderBookId = bookId;
  // Load body + bookmarks in parallel.
  try {
    const [bookRes, bmkRes] = await Promise.all([
      apiFetch(`/api/gutenberg/book/${bookId}`),
      apiFetch(`/api/user/gutenberg-bookmarks/${bookId}`),
    ]);
    if (!bookRes.ok) {
      bodyEl.innerHTML = `<div class="loc-empty" style="padding:3rem 1rem">Could not load book (HTTP ${bookRes.status}).</div>`;
      return;
    }
    const book = await bookRes.json();
    _gutenbergReaderTitle = book.title || fallbackTitle || `Book ${bookId}`;
    if (titleEl) titleEl.textContent = _gutenbergReaderTitle;
    bodyEl.innerHTML = book.html || "<div class=\"loc-empty\">Book body is empty.</div>";
    _gutenbergReaderBookmarks = bmkRes.ok
      ? await bmkRes.json()
      : { auto: null, manual: [] };
    _gutenbergRenderBookmarkList();
    // Resume scroll if we have an auto-bookmark from a previous session.
    if (_gutenbergReaderBookmarks.auto && Number(_gutenbergReaderBookmarks.auto.positionPct) > 0) {
      // Defer one frame so the body's height has stabilized after
      // innerHTML write.
      requestAnimationFrame(() => {
        _gutenbergScrollToPct(Number(_gutenbergReaderBookmarks.auto.positionPct));
      });
    } else {
      bodyEl.scrollTop = 0;
    }
  } catch (e) {
    console.warn("[gutenberg/reader]", e);
    bodyEl.innerHTML = `<div class="loc-empty" style="padding:3rem 1rem">Could not load book.</div>`;
  }
}
window._gutenbergOpenReader = _gutenbergOpenReader;

function _gutenbergCloseReader() {
  const overlay = document.getElementById("gutenberg-reader-overlay");
  if (!overlay) return;
  // Flush the auto-bookmark one last time on close so a quick read
  // (less than the debounce window) still saves its position.
  if (_gutenbergReaderBookId) _gutenbergSaveAutoBookmark(true);
  overlay.classList.remove("open");
  document.body.classList.remove("gutenberg-reader-open");
  // Empty the body so the next-book load doesn't briefly show the
  // previous book's pages.
  const bodyEl = document.getElementById("gutenberg-reader-body");
  if (bodyEl) bodyEl.innerHTML = "";
  _gutenbergReaderBookId = null;
  _gutenbergReaderTitle = "";
  _gutenbergReaderBookmarks = { auto: null, manual: [] };
}
window._gutenbergCloseReader = _gutenbergCloseReader;

// ── Bookmark sidebar rendering ──────────────────────────────────────
function _gutenbergRenderBookmarkList() {
  const list = document.getElementById("gutenberg-reader-bookmarks-list");
  if (!list) return;
  const bms = _gutenbergReaderBookmarks?.manual ?? [];
  if (!bms.length) {
    list.innerHTML = `<div class="loc-empty" style="font-size:0.8rem;padding:0.6rem">No bookmarks yet. Use the pin button below to save your spot.</div>`;
    return;
  }
  list.innerHTML = bms.map(b => `
    <div class="gutenberg-bookmark-row" data-id="${b.id}">
      <button type="button" class="gutenberg-bookmark-jump" onclick="_gutenbergJumpToBookmark(${b.id})" title="Jump to this bookmark">
        <span class="gutenberg-bookmark-label">${escHtml(b.label || `${Math.round(b.positionPct)}%`)}</span>
        <span class="gutenberg-bookmark-pct">${Math.round(b.positionPct)}%</span>
      </button>
      <button type="button" class="gutenberg-bookmark-del" onclick="_gutenbergDeleteBookmark(${b.id})" title="Delete bookmark">×</button>
    </div>`).join("");
}

function _gutenbergJumpToBookmark(bmkId) {
  const bm = (_gutenbergReaderBookmarks?.manual || []).find(b => b.id === bmkId);
  if (!bm) return;
  _gutenbergScrollToPct(Number(bm.positionPct) || 0);
}
window._gutenbergJumpToBookmark = _gutenbergJumpToBookmark;

async function _gutenbergDeleteBookmark(bmkId) {
  if (!confirm("Delete this bookmark?")) return;
  try {
    const r = await apiFetch("/api/user/gutenberg-bookmarks", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: bmkId }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _gutenbergReaderBookmarks.manual = _gutenbergReaderBookmarks.manual.filter(b => b.id !== bmkId);
    _gutenbergRenderBookmarkList();
  } catch (e) {
    console.warn("[gutenberg/bookmark-delete]", e);
    if (typeof showToast === "function") showToast("Delete failed", "error");
  }
}
window._gutenbergDeleteBookmark = _gutenbergDeleteBookmark;

async function _gutenbergPinHere() {
  if (!_gutenbergReaderBookId) return;
  const pct = _gutenbergCurrentScrollPct();
  const label = prompt(`Bookmark label (optional). Position: ${Math.round(pct)}%`, "") || null;
  try {
    const r = await apiFetch(`/api/user/gutenberg-bookmarks/${_gutenbergReaderBookId}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ kind: "manual", positionPct: pct, label }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    // Re-fetch to get the new row's id + canonical createdAt.
    const r2 = await apiFetch(`/api/user/gutenberg-bookmarks/${_gutenbergReaderBookId}`);
    if (r2.ok) {
      _gutenbergReaderBookmarks = await r2.json();
      _gutenbergRenderBookmarkList();
    }
    if (typeof showToast === "function") showToast("Bookmark pinned");
  } catch (e) {
    console.warn("[gutenberg/pin]", e);
    if (typeof showToast === "function") showToast("Pin failed", "error");
  }
}
window._gutenbergPinHere = _gutenbergPinHere;

// ── Scroll position tracking ────────────────────────────────────────
// The reader body is its own scrolling container (#gutenberg-reader-body
// has overflow-y:auto), not the page. Position is measured relative to
// the body's own scrollHeight so it's stable across font-size changes.
function _gutenbergCurrentScrollPct() {
  const el = document.getElementById("gutenberg-reader-body");
  if (!el) return 0;
  const max = el.scrollHeight - el.clientHeight;
  if (max <= 0) return 0;
  return Math.max(0, Math.min(100, (el.scrollTop / max) * 100));
}

function _gutenbergScrollToPct(pct) {
  const el = document.getElementById("gutenberg-reader-body");
  if (!el) return;
  const max = el.scrollHeight - el.clientHeight;
  if (max <= 0) return;
  el.scrollTop = max * (pct / 100);
}

function _gutenbergOnBodyScroll() {
  if (!_gutenbergReaderBookId) return;
  if (_gutenbergScrollTimer) clearTimeout(_gutenbergScrollTimer);
  _gutenbergScrollTimer = setTimeout(() => {
    _gutenbergScrollTimer = null;
    _gutenbergSaveAutoBookmark(false);
  }, _GUTENBERG_AUTO_SAVE_MS);
}
window._gutenbergOnBodyScroll = _gutenbergOnBodyScroll;

async function _gutenbergSaveAutoBookmark(isFinalFlush) {
  if (!_gutenbergReaderBookId) return;
  const pct = _gutenbergCurrentScrollPct();
  // Don't bother saving a position of ~0 unless this is the close-time
  // final flush — fresh opens are at 0% anyway.
  if (pct < 0.5 && !isFinalFlush) return;
  try {
    await apiFetch(`/api/user/gutenberg-bookmarks/${_gutenbergReaderBookId}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ kind: "auto", positionPct: pct }),
    });
    // Mirror locally so a subsequent re-open without a fetch sees the
    // freshest value.
    _gutenbergReaderBookmarks.auto = {
      positionPct: pct,
      updatedAt: new Date().toISOString(),
    };
  } catch (e) {
    // Silent — auto-save shouldn't surface errors mid-read.
    console.debug("[gutenberg/auto-bookmark]", e);
  }
}

// ── Font-size controls ──────────────────────────────────────────────
function _gutenbergAdjustFontSize(delta) {
  const body = document.getElementById("gutenberg-reader-body");
  if (!body) return;
  // Read from inline style first (set by prior clicks); fall back to
  // the computed-style default so the first delta lands on a known base.
  const current = parseFloat(body.style.fontSize) || 17;
  const next = Math.max(12, Math.min(28, current + delta));
  body.style.fontSize = `${next}px`;
  try { localStorage.setItem("sd_gutenberg_font_px", String(next)); } catch {}
}
window._gutenbergAdjustFontSize = _gutenbergAdjustFontSize;

// Restore preferred font size on module load.
(function _gutenbergRestoreFont() {
  try {
    const saved = parseFloat(localStorage.getItem("sd_gutenberg_font_px") || "");
    if (saved >= 12 && saved <= 28) {
      const body = document.getElementById("gutenberg-reader-body");
      if (body) body.style.fontSize = `${saved}px`;
    }
  } catch {}
})();
