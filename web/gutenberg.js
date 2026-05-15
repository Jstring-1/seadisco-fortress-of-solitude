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
// AbortController for the currently in-flight search. New searches
// abort the previous one rather than queuing — Gutendex can be slow
// for broad queries, and the old "if inflight, ignore" guard left
// users staring at stale results with no feedback when they retried.
let _gutenbergSearchAbort    = null;
let _gutenbergSearchPage     = 1;
let _gutenbergSearchQuery    = "";
let _gutenbergSearchLang     = "";
let _gutenbergSearchHasMore  = false;
let _gutenbergSavedIds       = new Set();
let _gutenbergSavedItems     = [];
// Lookup of currently-rendered book objects keyed by id. Populated as
// cards render (search + library both feed it) so the Save handler can
// pass title / authors / etc. through to the server — otherwise saving
// a never-read book would leave the gutenberg_books row absent and the
// Library list would render "Book NNNN / Unknown author".
const _gutenbergBookById = new Map();
// Active reader state — used by the auto-bookmark scroll handler.
let _gutenbergReaderBookId   = null;
let _gutenbergReaderTitle    = "";
let _gutenbergReaderBookmarks = { auto: null, manual: [] };
let _gutenbergReaderAnnotations = [];
let _gutenbergScrollTimer    = null;
const _GUTENBERG_AUTO_SAVE_MS = 1500;   // debounce window for auto-bookmark
// In-book find-as-you-type state.
let _gutenbergFindMatches    = [];      // [{node, start, end}] — descendants of #gutenberg-reader-body
let _gutenbergFindIndex      = -1;      // currently-focused match index
let _gutenbergFindDebounce   = null;

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
    // Mount the shared saved-search dropdown into the search row so
    // PG queries (q + subject) can be bookmarked alongside the rest
    // of the site's saved searches. Anon users skip — bookmarks are
    // per-account.
    if (typeof buildSavedSearchUI === "function" && window._clerk?.user) {
      const formRow = view.querySelector(".gutenberg-form-row");
      if (formRow && !formRow.querySelector(".saved-search-wrap")) {
        buildSavedSearchUI(
          "gutenberg",
          () => {
            // getParams — what gets persisted under the saved-search name.
            const q     = document.getElementById("gutenberg-q")?.value?.trim() ?? "";
            const topic = document.getElementById("gutenberg-topic")?.value?.trim() ?? "";
            const out = {};
            if (q)     out.q     = q;
            if (topic) out.topic = topic;
            return out;
          },
          (params) => {
            // apply — restore the form + fire the search.
            const qEl     = document.getElementById("gutenberg-q");
            const topicEl = document.getElementById("gutenberg-topic");
            const picker  = document.getElementById("gutenberg-topic-picker");
            if (qEl)     qEl.value     = params.q     ?? "";
            if (topicEl) topicEl.value = params.topic ?? "";
            if (picker) {
              // Sync the visible picker — match by value, else fall
              // through to "Any subject" so the dropdown isn't stale.
              const has = Array.from(picker.options).some(o => o.value === (params.topic ?? ""));
              picker.value = has ? (params.topic ?? "") : "";
            }
            _gutenbergSwitchTab("search");
            runGutenbergSearch(params.q ?? "");
          },
          formRow,
        );
      }
    }
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
    const tabKey = b.classList.contains("loc-tab-search")    ? "search"
                 : b.classList.contains("loc-tab-saved")     ? "saved"
                 : b.classList.contains("loc-tab-bookmarks") ? "bookmarks"
                 : "";
    b.classList.toggle("active", tabKey === tab);
  });
  document.querySelector(".gutenberg-panel-search").style.display    = tab === "search"    ? "block" : "none";
  document.querySelector(".gutenberg-panel-saved").style.display     = tab === "saved"     ? "block" : "none";
  document.querySelector(".gutenberg-panel-bookmarks").style.display = tab === "bookmarks" ? "block" : "none";
  if (tab === "saved")     _gutenbergRenderSaved();
  if (tab === "bookmarks") _gutenbergLoadAndRenderBookmarks();
}
window._gutenbergSwitchTab = _gutenbergSwitchTab;

// ── Bookmarks tab ───────────────────────────────────────────────────
// Cross-book listing of every manual bookmark the user has pinned.
// Click a row to open the reader at that position. Renders the
// server's joined book metadata so titles + authors come through
// even for books the user never explicitly Saved.
async function _gutenbergLoadAndRenderBookmarks() {
  const target = document.getElementById("gutenberg-bookmarks-results");
  const countEl = document.getElementById("gutenberg-bookmarks-count");
  if (!target) return;
  target.innerHTML = `<div class="loc-empty">Loading bookmarks…</div>`;
  try {
    const r = await apiFetch("/api/user/gutenberg-bookmarks");
    if (!r.ok) {
      target.innerHTML = `<div class="loc-empty">Could not load bookmarks (HTTP ${r.status}).</div>`;
      return;
    }
    const j = await r.json();
    const items = Array.isArray(j?.items) ? j.items : [];
    if (countEl) countEl.textContent = String(items.length);
    if (!items.length) {
      target.innerHTML = `<div class="loc-empty">No bookmarks yet. Open a book and use 📌 Pin to save your spot.</div>`;
      return;
    }
    target.innerHTML = items.map(b => {
      const authors = Array.isArray(b.bookAuthors) && b.bookAuthors.length
        ? b.bookAuthors.map(a => escHtml(a?.name || "")).filter(Boolean).join(", ")
        : "";
      const pct = Math.round(Number(b.positionPct) || 0);
      const labelTxt = b.label || `${pct}%`;
      const dateStr = b.createdAt ? new Date(b.createdAt).toLocaleDateString() : "";
      const titleSafe = JSON.stringify(b.bookTitle || "").replace(/"/g, "&quot;");
      return `
        <div class="gutenberg-bookmark-list-row" onclick="_gutenbergCloseAndOpenAt(${b.bookId}, ${titleSafe}, ${pct})">
          <div class="gutenberg-bookmark-list-main">
            <div class="gutenberg-bookmark-list-label">${escHtml(labelTxt)}</div>
            <div class="gutenberg-bookmark-list-book">${escHtml(b.bookTitle ?? `Book ${b.bookId}`)}${authors ? ` · <span class="gutenberg-bookmark-list-author">${authors}</span>` : ""}</div>
          </div>
          <div class="gutenberg-bookmark-list-meta">
            <span class="gutenberg-bookmark-list-pct">${pct}%</span>
            ${dateStr ? `<span class="gutenberg-bookmark-list-date">${escHtml(dateStr)}</span>` : ""}
            <button type="button" class="gutenberg-bookmark-del" onclick="event.stopPropagation();_gutenbergDeleteBookmarkFromList(${b.id})" title="Delete bookmark">×</button>
          </div>
        </div>`;
    }).join("");
  } catch (e) {
    console.warn("[gutenberg/bookmarks list]", e);
    target.innerHTML = `<div class="loc-empty">Could not load bookmarks.</div>`;
  }
}
window._gutenbergLoadAndRenderBookmarks = _gutenbergLoadAndRenderBookmarks;

// Open reader at the bookmark position from the Bookmarks tab. Wraps
// the existing reader opener with the optional start-position arg.
function _gutenbergCloseAndOpenAt(bookId, fallbackTitle, pct) {
  _gutenbergOpenReader(bookId, fallbackTitle, { startPositionPct: pct });
}
window._gutenbergCloseAndOpenAt = _gutenbergCloseAndOpenAt;

// Delete a bookmark from the Bookmarks tab (manual rows only). Then
// re-render the list so the row disappears immediately.
async function _gutenbergDeleteBookmarkFromList(bmkId) {
  if (!confirm("Delete this bookmark?")) return;
  try {
    const r = await apiFetch("/api/user/gutenberg-bookmarks", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: bmkId }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _gutenbergLoadAndRenderBookmarks();
  } catch (e) {
    console.warn("[gutenberg/bookmark-delete-from-list]", e);
    if (typeof showToast === "function") showToast("Delete failed", "error");
  }
}
window._gutenbergDeleteBookmarkFromList = _gutenbergDeleteBookmarkFromList;

// ── Search ──────────────────────────────────────────────────────────
async function runGutenbergSearch(q, opts) {
  const append = !!(opts && opts.append);
  q = String(q || "").trim();
  const topicInput = document.getElementById("gutenberg-topic");
  const topic = topicInput ? String(topicInput.value || "").trim() : "";
  // A bare topic (no q, no preset name) is still a meaningful query —
  // Gutendex's `topic=` filter alone returns the subject's full catalog
  // (downloads-desc by default). Same for a preset selection. Only
  // empty-everything renders the empty state.
  if (!q && !topic) {
    document.getElementById("gutenberg-results").innerHTML =
      `<div class="loc-empty">Type a title, author, or topic — or pick a subject from the dropdown — to search Project Gutenberg.</div>`;
    document.getElementById("gutenberg-pagination").innerHTML = "";
    return;
  }
  // Cancel-and-replace: any new search aborts the previous one. Gutendex
  // can hang for several seconds on broad substring queries; the old
  // "if inflight, ignore" guard meant a slow first attempt blocked every
  // retry with no UI feedback. Now the user can re-search at any time
  // and the stale request is abandoned.
  if (_gutenbergSearchAbort) {
    try { _gutenbergSearchAbort.abort(); } catch {}
  }
  const ctrl = new AbortController();
  _gutenbergSearchAbort = ctrl;
  if (!append) {
    _gutenbergSearchPage = 1;
    _gutenbergSearchQuery = q;
    // Paint "Searching…" synchronously so the user always sees that
    // their click registered, even if the network is slow.
    document.getElementById("gutenberg-results").innerHTML =
      `<div class="loc-empty">Searching…</div>`;
    document.getElementById("gutenberg-pagination").innerHTML = "";
  } else {
    _gutenbergSearchPage += 1;
  }
  // Language filter element was removed from the UI — _gutenbergSearchLang
  // stays as state-only (defaults to "" → any language) in case we want
  // to bring the picker back later.
  _gutenbergSearchLang = "";
  try {
    const params = new URLSearchParams();
    if (q) params.set("q", q);
    if (_gutenbergSearchPage > 1) params.set("page", String(_gutenbergSearchPage));
    if (_gutenbergSearchLang) params.set("lang", _gutenbergSearchLang);
    if (topic) params.set("topic", topic);
    const r = await apiFetch(`/api/gutenberg/search?${params.toString()}`, { signal: ctrl.signal });
    // If a newer search aborted us mid-flight, bail silently — the new
    // search has already replaced the "Searching…" message with its own
    // state, and we don't want to overwrite it.
    if (ctrl.signal.aborted) return;
    if (!r.ok) {
      document.getElementById("gutenberg-results").innerHTML =
        `<div class="loc-empty">Search failed (HTTP ${r.status}).</div>`;
      return;
    }
    const j = await r.json();
    if (ctrl.signal.aborted) return;
    const items = Array.isArray(j?.items) ? j.items : [];
    _gutenbergSearchHasMore = !!j?.hasMore;
    if (!append && !items.length) {
      document.getElementById("gutenberg-results").innerHTML =
        `<div class="loc-empty">No matches.</div>`;
      document.getElementById("gutenberg-pagination").innerHTML = "";
      return;
    }
    // Preset banner — server echoes back `preset` + `presetSubjects`
    // when the topic matched a known preset. Show a small chip above
    // the results so the user knows it's a merged set.
    let header = "";
    if (!append && j?.preset && Array.isArray(j?.presetSubjects)) {
      const subjectStr = j.presetSubjects.join(", ");
      header = `<div class="gutenberg-preset-banner" title="Merged across: ${escHtml(subjectStr)}">Preset: <b>${escHtml(j.preset)}</b> · ${j.presetSubjects.length} subjects · top ${items.length} by popularity</div>`;
    }
    const html = items.map(_gutenbergCardHtml).join("");
    const target = document.getElementById("gutenberg-results");
    if (append) target.insertAdjacentHTML("beforeend", html);
    else        target.innerHTML = header + html;
    _gutenbergRenderPagination();
  } catch (e) {
    // AbortError is expected when a new search supersedes us — don't
    // touch the DOM in that case (the new search owns it now).
    if (e && (e.name === "AbortError" || ctrl.signal.aborted)) return;
    console.warn("[gutenberg/search]", e);
    document.getElementById("gutenberg-results").innerHTML =
      `<div class="loc-empty">Search error.</div>`;
  } finally {
    // Only clear the module-level pointer if we're still the latest
    // search — a newer one may have already replaced it.
    if (_gutenbergSearchAbort === ctrl) _gutenbergSearchAbort = null;
  }
}
window.runGutenbergSearch = runGutenbergSearch;

// Preset chip click: just stash the preset name in the topic input.
// Doesn't run the search — the user often wants to add / edit the
// keyword first. They press Enter or click Search when ready. Focus
// the keyword field so the cursor lands where they're likely to type.
function _gutenbergRunPreset(presetName) {
  const topicInput = document.getElementById("gutenberg-topic");
  const qInput = document.getElementById("gutenberg-q");
  if (topicInput) topicInput.value = presetName;
  if (qInput) qInput.focus();
}
window._gutenbergRunPreset = _gutenbergRunPreset;

function _gutenbergRenderPagination() {
  const pag = document.getElementById("gutenberg-pagination");
  if (!pag) return;
  pag.innerHTML = _gutenbergSearchHasMore
    ? `<button type="button" class="loc-submit" onclick="runGutenbergSearch(document.getElementById('gutenberg-q').value,{append:true})">Load more</button>`
    : "";
}

// ── Card rendering ──────────────────────────────────────────────────
// Rich card. Everything that used to live in a separate "Info" popup
// now renders inline:
//   - All subjects (capped at 12 server-side) + bookshelves as chips
//   - Available formats (HTML / EPUB / Kindle / plain / PDF) as links
//   - 🎧 Audio rows (Play / Queue when directly playable, ↗ otherwise)
//   - Action row: Read, Save, Wikipedia search ↗, Google search ↗,
//                 Gutenberg ↗
// No more popup, no more per-card lazy fetches.
function _gutenbergCardHtml(b) {
  // Stash for save-time metadata pass-through (see _gutenbergToggleSave).
  if (b && b.id) _gutenbergBookById.set(Number(b.id), b);
  const authors = Array.isArray(b.authors) && b.authors.length
    ? b.authors.map(a => {
        const years = (a?.birth_year || a?.death_year)
          ? ` <span class="gutenberg-card-years">(${a.birth_year ?? "?"}–${a.death_year ?? "?"})</span>`
          : "";
        return `<span>${escHtml(a?.name || "Unknown")}</span>${years}`;
      }).join(", ")
    : "Unknown author";
  const langs = Array.isArray(b.languages) && b.languages.length
    ? b.languages.map(l => escHtml(String(l).toUpperCase())).join(" / ")
    : "";
  // Subjects: clickable, pivot to a fresh search filtered by that subject.
  const subjectChips = (Array.isArray(b.subjects) ? b.subjects : []).map(s =>
    `<span class="gutenberg-subject-chip" onclick="_gutenbergSearchBySubject(${JSON.stringify(s).replace(/"/g, "&quot;")})" style="cursor:pointer" title="Search for this subject">${escHtml(s)}</span>`
  ).join("");
  const shelfChips = (Array.isArray(b.bookshelves) ? b.bookshelves : []).map(s =>
    `<span class="gutenberg-shelf-chip">${escHtml(s)}</span>`
  ).join("");
  const downloadCount = b.download_count
    ? `<span class="gutenberg-card-stat"><span class="gutenberg-card-stat-label">↓</span> ${Number(b.download_count).toLocaleString()}</span>`
    : "";
  const isSaved = _gutenbergSavedIds.has(b.id);
  const saveBtn = `<button type="button" class="archive-btn" onclick="_gutenbergToggleSave(${b.id}, this)" title="${isSaved ? "Remove from Library" : "Save to Library"}">${isSaved ? "✓ Saved" : "+ Save"}</button>`;
  const cover = b.cover
    ? `<img class="gutenberg-cover" src="${escHtml(b.cover)}" alt="" loading="lazy" decoding="async">`
    : `<div class="gutenberg-cover gutenberg-cover-placeholder">📖</div>`;
  const titleSafe = JSON.stringify(b.title || "").replace(/"/g, "&quot;");
  const formatLinks = _gutenbergFormatLinksHtml(b.formats);
  const audioSection = _gutenbergAudioSectionHtml(b);
  // External search URLs — Wikipedia + Google, both with title +
  // primary author quoted for exact-phrase matching.
  const wikiUrl = _gutenbergWikipediaSearchUrl(b);
  const googleUrl = _gutenbergGoogleSearchUrl(b);
  return `
    <div class="gutenberg-card" data-book-id="${b.id}">
      ${cover}
      <div class="gutenberg-card-body">
        <div class="gutenberg-card-title">${escHtml(b.title || `Book ${b.id}`)}</div>
        <div class="gutenberg-card-author">by ${authors}</div>
        <div class="gutenberg-card-meta-row">
          ${langs ? `<span class="gutenberg-card-langs">${langs}</span>` : ""}
          ${downloadCount}
        </div>
        ${shelfChips ? `<div class="gutenberg-card-section"><span class="gutenberg-card-section-label">Bookshelves:</span><div class="gutenberg-card-chips">${shelfChips}</div></div>` : ""}
        ${subjectChips ? `<div class="gutenberg-card-section"><span class="gutenberg-card-section-label">Subjects:</span><div class="gutenberg-card-chips">${subjectChips}</div></div>` : ""}
        ${audioSection}
        ${formatLinks ? `<div class="gutenberg-card-section gutenberg-card-formats-section"><span class="gutenberg-card-section-label">Formats:</span><div class="gutenberg-card-formats">${formatLinks}</div></div>` : ""}
        <div class="gutenberg-card-actions">
          <button type="button" class="archive-btn archive-btn-suggest" onclick="_gutenbergOpenReader(${b.id}, ${titleSafe})">📖 Read</button>
          ${saveBtn}
          <a href="${escHtml(wikiUrl)}" target="_blank" rel="noopener" class="archive-btn" style="text-decoration:none" title="Search Wikipedia for this book">Wikipedia ↗</a>
          <a href="${escHtml(googleUrl)}" target="_blank" rel="noopener" class="archive-btn" style="text-decoration:none" title="Google search for this book">Google ↗</a>
          <a href="https://www.gutenberg.org/ebooks/${b.id}" target="_blank" rel="noopener" class="archive-btn" style="text-decoration:none">Gutenberg ↗</a>
        </div>
      </div>
    </div>`;
}

// Compact format-link row builder, extracted so the card render
// stays readable. Maps known reader-relevant MIME types to friendly
// labels in a stable order; drops zip-only variants.
function _gutenbergFormatLinksHtml(formats) {
  if (!formats || typeof formats !== "object") return "";
  const FORMAT_ORDER = [
    ["text/html",                       "HTML"],
    ["application/epub+zip",            "EPUB"],
    ["application/x-mobipocket-ebook",  "Kindle"],
    ["text/plain; charset=utf-8",       "Plain text"],
    ["text/plain",                      "Plain text"],
    ["application/pdf",                 "PDF"],
  ];
  const seen = new Set();
  return FORMAT_ORDER.map(([mime, label]) => {
    const key = Object.keys(formats).find(k => k.toLowerCase().startsWith(mime.toLowerCase()) && !/\.zip$/i.test(formats[k]));
    if (!key) return "";
    const url = formats[key];
    if (!url || seen.has(url)) return "";
    seen.add(url);
    return `<a href="${escHtml(url)}" target="_blank" rel="noopener" class="gutenberg-card-format-link">${label} ↗</a>`;
  }).filter(Boolean).join("");
}

function _gutenbergWikipediaSearchUrl(meta) {
  const title = String(meta?.title || "").trim();
  const primaryAuthor = (Array.isArray(meta?.authors) && meta.authors[0]?.name) ? String(meta.authors[0].name).trim() : "";
  const q = [title, primaryAuthor].filter(Boolean).join(" ") || title;
  return `https://en.wikipedia.org/wiki/Special:Search?search=${encodeURIComponent(q)}`;
}

// ── Subject pivot ───────────────────────────────────────────────────
// Used by subject chips on every card — drop into the topic input
// and run a fresh search for that subject. The info popup is gone
// (cards are rich enough to render everything inline), but this
// pivot remains useful when scanning results.
function _gutenbergSearchBySubject(subject) {
  if (!subject) return;
  const topicInput = document.getElementById("gutenberg-topic");
  const picker = document.getElementById("gutenberg-topic-picker");
  const qInput = document.getElementById("gutenberg-q");
  if (topicInput) topicInput.value = subject;
  // Sync the picker so its current selection matches the active filter
  // (when the subject matches a known option). Custom subjects fall
  // through silently — the picker keeps its previous selection but
  // the hidden input drives the actual query.
  if (picker) {
    const has = Array.from(picker.options).some(o => o.value === subject);
    if (has) picker.value = subject;
  }
  if (qInput) qInput.value = "";
  _gutenbergSwitchTab("search");
  runGutenbergSearch("");
}
window._gutenbergSearchBySubject = _gutenbergSearchBySubject;

// Subject picker change handler — single source of truth for the
// subject filter. Writes to the hidden #gutenberg-topic input that
// the runGutenbergSearch reads. The special "__custom__" value
// surfaces a prompt for an arbitrary subject; "" clears the filter.
function _gutenbergOnSubjectPick(selectEl) {
  if (!selectEl) return;
  const v = selectEl.value;
  const topicInput = document.getElementById("gutenberg-topic");
  if (!topicInput) return;
  if (v === "__custom__") {
    const custom = prompt("Custom subject (free text):", topicInput.value || "");
    if (custom !== null) topicInput.value = String(custom).trim();
    // Re-sync the picker — if the typed value matches a known option,
    // select it; otherwise fall back to "Any subject" so the dropdown
    // doesn't keep showing "Custom…".
    const matched = Array.from(selectEl.options).find(o => o.value && o.value === topicInput.value);
    selectEl.value = matched ? matched.value : "";
  } else {
    topicInput.value = v || "";
  }
}
window._gutenbergOnSubjectPick = _gutenbergOnSubjectPick;

// ── Removed: info popup machinery ──────────────────────────────────
// Cards inline everything that used to require a popup-time fetch:
// full subjects + bookshelves, formats, audio detection (Play/Queue/
// external), and the action row (Read, Save, Wikipedia ↗, Google ↗,
// Gutenberg ↗). Was: 8+ async fetches per popup open. Now: 0.


// ── Audio detection (Project Gutenberg human-read / TTS audio books) ─
// Gutendex exposes audio MIME types in the formats dict when an audio
// edition exists — usually `audio/mpeg`, `audio/ogg`, sometimes
// `audio/x-mpegurl`. URLs vary: a single .mp3 file (rare, but
// directly playable), a .m3u playlist, a directory listing, or a
// zipped archive. We classify each: directly playable streams get
// ▶ Play / ＋ Queue buttons that hook into the existing LOC-style
// engine; others fall back to an external download link.

// Returns the inner HTML of the audio section, or "" if no audio
// formats are advertised on this book.
function _gutenbergAudioSectionHtml(meta) {
  const formats = meta?.formats ?? {};
  const entries = Object.entries(formats)
    .filter(([mime]) => /^audio\//i.test(mime))
    // Drop image cover MIME variants that occasionally leak (e.g.
    // audio/jpeg from typos in upstream metadata).
    .filter(([_, url]) => typeof url === "string" && url);
  if (!entries.length) return "";
  const rows = entries.map(([mime, url]) => {
    const playable = _gutenbergAudioIsPlayable(mime, url);
    const labelText = _gutenbergAudioLabel(mime, url);
    const urlSafe = escHtml(url);
    const urlJs = JSON.stringify(url).replace(/"/g, "&quot;");
    const titleSafe = JSON.stringify(meta?.title || `Book ${meta?.id}`).replace(/"/g, "&quot;");
    const authorSafe = JSON.stringify((Array.isArray(meta?.authors) && meta.authors[0]?.name) ? meta.authors[0].name : "").replace(/"/g, "&quot;");
    const idJs = Number(meta?.id) || 0;
    const playBtn = playable
      ? `<button type="button" class="archive-btn archive-btn-suggest" onclick="_gutenbergAudioPlay(${idJs}, ${urlJs}, ${titleSafe}, ${authorSafe})" title="Play through the mini-player">▶ Play</button>
         <button type="button" class="archive-btn" onclick="_gutenbergAudioQueue(${idJs}, ${urlJs}, ${titleSafe}, ${authorSafe})" title="Add to queue">＋ Queue</button>`
      : `<span class="gutenberg-audio-note" title="Not a direct audio file — opens externally">archive / playlist</span>`;
    return `
      <div class="gutenberg-audio-row">
        <span class="gutenberg-audio-mime">${escHtml(labelText)}</span>
        <a href="${urlSafe}" target="_blank" rel="noopener" class="gutenberg-audio-url">${escHtml(_gutenbergAudioUrlDisplay(url))} ↗</a>
        <div class="gutenberg-audio-actions">${playBtn}</div>
      </div>`;
  }).join("");
  return `
    <div class="gutenberg-info-section">
      <div class="gutenberg-info-label">🎧 Audio</div>
      <div class="gutenberg-audio-list">${rows}</div>
    </div>`;
}

// Direct-stream detection. We consider a URL playable in-browser if
// it ends with a known audio extension. Anything else (directory
// listings, zip archives, m3u playlists pointing at chunked content)
// falls through to an external link — those need a real audiobook
// engine to handle chapter enumeration / HLS parsing.
function _gutenbergAudioIsPlayable(mime, url) {
  if (!url) return false;
  return /\.(?:mp3|ogg|wav|m4a|aac|flac|opus)(?:\?|#|$)/i.test(url);
}

// Human-readable label for the format row. Prefer the URL's file
// extension when present (more specific than the broad MIME type),
// else show the MIME.
function _gutenbergAudioLabel(mime, url) {
  const m = String(url).match(/\.(mp3|ogg|wav|m4a|aac|flac|opus|m3u|m3u8|zip)(?:\?|#|$)/i);
  if (m) return m[1].toUpperCase();
  const sub = String(mime).split("/")[1] || mime;
  return sub.toUpperCase();
}

// Trim the URL for display — full URLs are long and visually noisy.
// Show just the host + last path segment.
function _gutenbergAudioUrlDisplay(url) {
  try {
    const u = new URL(url);
    const tail = u.pathname.split("/").filter(Boolean).pop() || u.hostname;
    return tail.length > 36 ? tail.slice(0, 33) + "…" : tail;
  } catch {
    return url.length > 36 ? url.slice(0, 33) + "…" : url;
  }
}

// Play a Gutenberg audio URL through the existing LOC engine
// (window._locPlay). Synthesizes a minimal LOC-shaped item so the
// engine + mini-player can render title / artist on the persistent
// bar. id is prefixed "gutenberg-" so the play log + cross-source
// queue can tell PG audio apart from real LOC items.
function _gutenbergAudioPlay(bookId, streamUrl, title, authorName) {
  if (!streamUrl) return;
  const item = {
    id: `gutenberg-${bookId}`,
    title: title || `Book ${bookId}`,
    contributors: authorName ? [authorName] : [],
    streamUrl,
    streamType: _gutenbergGuessStreamType(streamUrl),
    image: "",
    year: "",
  };
  if (typeof window._locPlay === "function") {
    window._locPlay(item);
  } else if (typeof showToast === "function") {
    showToast("Audio engine not loaded — try again in a moment.", "error");
  }
}
window._gutenbergAudioPlay = _gutenbergAudioPlay;

function _gutenbergAudioQueue(bookId, streamUrl, title, authorName) {
  if (!streamUrl) return;
  const item = {
    id: `gutenberg-${bookId}-${_gutenbergQueueCounter++}`,
    title: title || `Book ${bookId}`,
    contributors: authorName ? [authorName] : [],
    streamUrl,
    streamType: _gutenbergGuessStreamType(streamUrl),
    image: "",
    year: "",
  };
  if (typeof window.queueAddLoc === "function") {
    window.queueAddLoc(item, { mode: "append" });
    if (typeof showToast === "function") showToast("Added to queue");
  } else if (typeof showToast === "function") {
    showToast("Queue not loaded — try again in a moment.", "error");
  }
}
window._gutenbergAudioQueue = _gutenbergAudioQueue;

// Counter so multiple ＋ Queue clicks on the same book produce
// distinct queue entries (queue dedupes by externalId).
let _gutenbergQueueCounter = 1;

function _gutenbergGuessStreamType(url) {
  if (/\.mp3(?:\?|#|$)/i.test(url)) return "mp3";
  if (/\.ogg(?:\?|#|$)/i.test(url)) return "ogg";
  if (/\.m4a(?:\?|#|$)/i.test(url)) return "m4a";
  if (/\.aac(?:\?|#|$)/i.test(url)) return "aac";
  if (/\.wav(?:\?|#|$)/i.test(url)) return "wav";
  if (/\.flac(?:\?|#|$)/i.test(url)) return "flac";
  if (/\.opus(?:\?|#|$)/i.test(url)) return "opus";
  if (/\.m3u8?(?:\?|#|$)/i.test(url)) return "hls";
  return "";
}

// Build a Google search URL for a book — title and primary author
// quoted so the search prefers exact-phrase matches. Falls back to
// just the title when no author is available.
function _gutenbergGoogleSearchUrl(meta) {
  const title = String(meta?.title || "").trim();
  const primaryAuthor = (Array.isArray(meta?.authors) && meta.authors[0]?.name) ? String(meta.authors[0].name).trim() : "";
  const parts = [];
  if (title) parts.push(`"${title}"`);
  if (primaryAuthor) parts.push(`"${primaryAuthor}"`);
  const q = parts.join(" ") || `Project Gutenberg ${meta?.id ?? ""}`;
  return `https://www.google.com/search?q=${encodeURIComponent(q)}`;
}

// — Second chunk of popup machinery DELETED here: _gutenbergFillInfoPopupExtras,
//   _gutenbergOpenInfoLinkChip, _gutenbergToggleSaveFromPopup, and the OLD
//   _gutenbergSearchBySubject (shadowed the live one defined above the
//   popup-removal banner). Nothing references these from outside any more —
//   the popup that called them is gone. Re-add from git history if needed.
//
//   Inline `/* swallow */` comments inside the original block ruled out a
//   single big /* */ wrapper, so the code is gone-gone rather than
//   commented out.
// ── Save / unsave ───────────────────────────────────────────────────
async function _gutenbergToggleSave(bookId, btn) {
  bookId = Number(bookId);
  if (!Number.isFinite(bookId) || bookId <= 0) return;
  const currentlySaved = _gutenbergSavedIds.has(bookId);
  if (btn) btn.disabled = true;
  try {
    const method = currentlySaved ? "DELETE" : "POST";
    // On Save, pass the metadata we already have for this book so the
    // server can seed a gutenberg_books row. Otherwise the Library
    // list would render "Book NNNN / Unknown author" for books that
    // were saved-without-read.
    const body = { id: bookId };
    if (method === "POST") {
      const cached = _gutenbergBookById.get(bookId);
      if (cached) {
        if (cached.title)     body.title     = cached.title;
        if (cached.authors)   body.authors   = cached.authors;
        if (cached.languages) body.languages = cached.languages;
        if (cached.subjects)  body.subjects  = cached.subjects;
      }
    }
    const r = await apiFetch("/api/user/gutenberg-saves", {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
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
async function _gutenbergOpenReader(bookId, fallbackTitle, opts) {
  bookId = Number(bookId);
  if (!Number.isFinite(bookId) || bookId <= 0) return;
  // Optional start position from caller (e.g. mention-panel deep-link).
  // Overrides the auto-bookmark resume — if a user clicks a specific
  // passage they want to land there, not their last-read spot.
  const startPct = (opts && Number.isFinite(Number(opts.startPositionPct)))
    ? Number(opts.startPositionPct)
    : null;
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
  _gutenbergResetFind();
  _gutenbergReaderBookId = bookId;
  // Hide/show admin-only 🔗 Link button based on current admin status.
  const linkBtn = document.getElementById("gutenberg-link-here-btn");
  if (linkBtn) linkBtn.style.display = window._isAdmin ? "" : "none";
  // Load body + bookmarks + annotations in parallel.
  try {
    const [bookRes, bmkRes, annRes] = await Promise.all([
      apiFetch(`/api/gutenberg/book/${bookId}`),
      apiFetch(`/api/user/gutenberg-bookmarks/${bookId}`),
      apiFetch(`/api/gutenberg/annotations?book_id=${bookId}`),
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
    _gutenbergReaderAnnotations = annRes.ok
      ? (await annRes.json()).items || []
      : [];
    _gutenbergRenderBookmarkList();
    _gutenbergRenderAnnotationList();
    // Defer marker injection one frame so the body's height has
    // stabilized after innerHTML write — _gutenbergInjectAnnotationMarkers
    // needs accurate scrollHeight to position the markers.
    requestAnimationFrame(() => {
      _gutenbergInjectAnnotationMarkers();
      // Caller-provided start position wins over the auto-bookmark.
      if (startPct !== null) {
        _gutenbergScrollToPct(startPct);
      } else if (_gutenbergReaderBookmarks.auto && Number(_gutenbergReaderBookmarks.auto.positionPct) > 0) {
        _gutenbergScrollToPct(Number(_gutenbergReaderBookmarks.auto.positionPct));
      } else {
        bodyEl.scrollTop = 0;
      }
    });
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
  _gutenbergReaderAnnotations = [];
  _gutenbergResetFind();
  const findInput = document.getElementById("gutenberg-reader-find");
  if (findInput) findInput.value = "";
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
  // the computed-style default so the first delta lands on a known
  // base. Default sync'd with the CSS default (15.5px).
  const current = parseFloat(body.style.fontSize) || 15.5;
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

// ── In-book text search ─────────────────────────────────────────────
// Walks all text nodes in the reader body, finds case-insensitive
// matches of the query, wraps each match in a <span class="gutenberg-
// find-hit"> so CSS can highlight and getBoundingClientRect can scroll
// matches into view. Active match additionally carries .is-active.
//
// Performance: for a 2MB book we have ~50-200k text nodes. Splitting
// each match into a span is O(matches) which is bounded by user input
// length (a 3-character query against "the" hits ~5000 times in a
// typical book). The render is fast enough on desktop; mobile may
// stutter for very common short queries. Reset on every input.

function _gutenbergResetFind() {
  // Undo all wrap spans by replacing them with their text content.
  // querySelectorAll snapshot is safe to walk while we mutate parents.
  const body = document.getElementById("gutenberg-reader-body");
  if (body) {
    body.querySelectorAll(".gutenberg-find-hit").forEach(span => {
      const text = span.textContent;
      const parent = span.parentNode;
      if (!parent) return;
      parent.replaceChild(document.createTextNode(text), span);
      parent.normalize();   // re-merge adjacent text nodes
    });
  }
  _gutenbergFindMatches = [];
  _gutenbergFindIndex = -1;
  _gutenbergUpdateFindUI();
}

function _gutenbergFindOnInput(input) {
  if (_gutenbergFindDebounce) clearTimeout(_gutenbergFindDebounce);
  // Debounce so a fast typist doesn't trigger a regex+wrap pass per
  // keystroke (the wrap pass mutates DOM and can lag for very common
  // short queries).
  _gutenbergFindDebounce = setTimeout(() => {
    _gutenbergFindDebounce = null;
    _gutenbergRunFind(input.value || "");
  }, 180);
}
window._gutenbergFindOnInput = _gutenbergFindOnInput;

function _gutenbergFindOnKeyDown(event) {
  if (event.key === "Enter") {
    event.preventDefault();
    _gutenbergFindStep(event.shiftKey ? -1 : 1);
  } else if (event.key === "Escape") {
    event.preventDefault();
    event.target.value = "";
    _gutenbergResetFind();
  }
}
window._gutenbergFindOnKeyDown = _gutenbergFindOnKeyDown;

function _gutenbergRunFind(query) {
  _gutenbergResetFind();
  query = String(query || "").trim();
  if (!query || query.length < 2) return;
  const body = document.getElementById("gutenberg-reader-body");
  if (!body) return;
  // Walk text nodes and wrap matches. Live NodeList from getElementsBy*
  // is dangerous mid-mutation; use a manual TreeWalker with a static
  // node array.
  const walker = document.createTreeWalker(body, NodeFilter.SHOW_TEXT, {
    acceptNode(node) {
      // Skip script / style / already-wrapped hits (defensive — reset
      // should have unwrapped them).
      const p = node.parentNode;
      if (!p) return NodeFilter.FILTER_REJECT;
      const tag = p.nodeName;
      if (tag === "SCRIPT" || tag === "STYLE") return NodeFilter.FILTER_REJECT;
      if (p.classList && p.classList.contains("gutenberg-find-hit")) return NodeFilter.FILTER_REJECT;
      return NodeFilter.FILTER_ACCEPT;
    },
  });
  const textNodes = [];
  let n;
  while ((n = walker.nextNode())) textNodes.push(n);
  const re = new RegExp(_gutenbergEscapeRegex(query), "gi");
  const matches = [];
  for (const node of textNodes) {
    const text = node.nodeValue || "";
    let m;
    let lastIndex = 0;
    const frag = document.createDocumentFragment();
    let any = false;
    re.lastIndex = 0;
    while ((m = re.exec(text)) !== null) {
      any = true;
      if (m.index > lastIndex) frag.appendChild(document.createTextNode(text.slice(lastIndex, m.index)));
      const span = document.createElement("span");
      span.className = "gutenberg-find-hit";
      span.textContent = m[0];
      frag.appendChild(span);
      matches.push(span);
      lastIndex = m.index + m[0].length;
      // Defensive against zero-width matches.
      if (m[0].length === 0) re.lastIndex++;
    }
    if (any) {
      if (lastIndex < text.length) frag.appendChild(document.createTextNode(text.slice(lastIndex)));
      node.parentNode.replaceChild(frag, node);
    }
  }
  _gutenbergFindMatches = matches;
  _gutenbergFindIndex = matches.length ? 0 : -1;
  if (_gutenbergFindIndex >= 0) _gutenbergActivateFindMatch(0);
  _gutenbergUpdateFindUI();
}

function _gutenbergFindStep(direction) {
  if (!_gutenbergFindMatches.length) return;
  const next = (_gutenbergFindIndex + direction + _gutenbergFindMatches.length) % _gutenbergFindMatches.length;
  _gutenbergActivateFindMatch(next);
}
window._gutenbergFindStep = _gutenbergFindStep;

function _gutenbergActivateFindMatch(idx) {
  if (_gutenbergFindIndex >= 0 && _gutenbergFindMatches[_gutenbergFindIndex]) {
    _gutenbergFindMatches[_gutenbergFindIndex].classList.remove("is-active");
  }
  _gutenbergFindIndex = idx;
  const el = _gutenbergFindMatches[idx];
  if (!el) return;
  el.classList.add("is-active");
  // scrollIntoView with block:center keeps the match visible without
  // jumping to the very top of the viewport.
  el.scrollIntoView({ behavior: "smooth", block: "center" });
  _gutenbergUpdateFindUI();
}

function _gutenbergUpdateFindUI() {
  const counter = document.getElementById("gutenberg-reader-find-count");
  const prev = document.getElementById("gutenberg-find-prev");
  const next = document.getElementById("gutenberg-find-next");
  const has = _gutenbergFindMatches.length > 0;
  if (counter) {
    counter.textContent = has
      ? `${_gutenbergFindIndex + 1}/${_gutenbergFindMatches.length}`
      : "";
  }
  if (prev) prev.disabled = !has;
  if (next) next.disabled = !has;
}

function _gutenbergEscapeRegex(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// ── Annotations (admin-curated cross-links to Discogs entities) ─────
// Sidebar rendering — one row per annotation, click jumps reader to
// that position and (for entity-id-bearing rows) double-click opens
// the Discogs entity popup.

function _gutenbergRenderAnnotationList() {
  const list = document.getElementById("gutenberg-reader-annotations-list");
  if (!list) return;
  const ann = _gutenbergReaderAnnotations || [];
  if (!ann.length) {
    list.innerHTML = `<div class="loc-empty" style="font-size:0.8rem;padding:0.6rem">No links yet.${window._isAdmin ? " Use 🔗 Link to add one." : ""}</div>`;
    return;
  }
  list.innerHTML = ann.map(a => {
    const kindBadge = a.entityType.charAt(0).toUpperCase();
    return `
      <div class="gutenberg-annotation-row" data-id="${a.id}">
        <button type="button" class="gutenberg-annotation-jump" onclick="_gutenbergJumpToAnnotation(${a.id})" title="${escHtml(a.label || a.entityName)} — jump here">
          <span class="gutenberg-annotation-kind" data-kind="${escHtml(a.entityType)}">${kindBadge}</span>
          <span class="gutenberg-annotation-name">${escHtml(a.entityName)}</span>
          <span class="gutenberg-annotation-pct">${Math.round(a.positionPct)}%</span>
        </button>
        <button type="button" class="gutenberg-annotation-open" onclick="_gutenbergOpenAnnotationEntity(${a.id})" title="Open ${escHtml(a.entityType)} in SeaDisco">↗</button>
        ${window._isAdmin ? `<button type="button" class="gutenberg-bookmark-del" onclick="_gutenbergDeleteAnnotation(${a.id})" title="Delete link">×</button>` : ""}
      </div>`;
  }).join("");
}

function _gutenbergJumpToAnnotation(annId) {
  const a = _gutenbergReaderAnnotations.find(x => x.id === annId);
  if (!a) return;
  _gutenbergScrollToPct(Number(a.positionPct) || 0);
  // Flash the inline marker so the user sees where it lives.
  const marker = document.querySelector(`.gutenberg-anno-marker[data-anno-id="${annId}"]`);
  if (marker) {
    marker.classList.add("flash");
    setTimeout(() => marker.classList.remove("flash"), 1500);
  }
}
window._gutenbergJumpToAnnotation = _gutenbergJumpToAnnotation;

function _gutenbergOpenAnnotationEntity(annId) {
  const a = _gutenbergReaderAnnotations.find(x => x.id === annId);
  if (!a) return;
  _gutenbergDispatchEntityOpen(a);
}
window._gutenbergOpenAnnotationEntity = _gutenbergOpenAnnotationEntity;

// Open a Discogs entity referenced by an annotation. For artists with
// no Discogs id we just run a SeaDisco artist search; for entities
// with ids we route through the existing modal opener fns when
// available, falling back to a search URL.
function _gutenbergDispatchEntityOpen(a) {
  if (!a) return;
  const id = a.entityId;
  if (a.entityType === "artist") {
    if (id && typeof window.openArtistModal === "function") {
      window.openArtistModal(Number(id), a.entityName);
      return;
    }
    // Fallback — search by name. Routes through index.html's main
    // search form for a consistent experience.
    location.href = `/?a=${encodeURIComponent(a.entityName)}`;
    return;
  }
  if (a.entityType === "master") {
    if (id && typeof window.openMasterModal === "function") {
      window.openMasterModal(Number(id));
      return;
    }
    location.href = `/master/${id ?? ""}`;
    return;
  }
  if (a.entityType === "release") {
    if (id && typeof window.openAlbumModal === "function") {
      window.openAlbumModal(Number(id));
      return;
    }
    location.href = `/release/${id ?? ""}`;
    return;
  }
  if (a.entityType === "label") {
    if (id && typeof window.openLabelModal === "function") {
      window.openLabelModal(Number(id), a.entityName);
      return;
    }
    location.href = `/?l=${encodeURIComponent(a.entityName)}`;
    return;
  }
}

async function _gutenbergDeleteAnnotation(annId) {
  if (!window._isAdmin) return;
  if (!confirm("Delete this link?")) return;
  try {
    const r = await apiFetch("/api/gutenberg/annotations", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: annId }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _gutenbergReaderAnnotations = _gutenbergReaderAnnotations.filter(a => a.id !== annId);
    _gutenbergRenderAnnotationList();
    _gutenbergInjectAnnotationMarkers();
  } catch (e) {
    console.warn("[gutenberg/annotation-delete]", e);
    if (typeof showToast === "function") showToast("Delete failed", "error");
  }
}
window._gutenbergDeleteAnnotation = _gutenbergDeleteAnnotation;

// Inject inline markers next to the closest element to each
// annotation's position_pct. Idempotent — clears existing markers
// before re-injecting (used by add/delete/initial-load paths).
function _gutenbergInjectAnnotationMarkers() {
  const body = document.getElementById("gutenberg-reader-body");
  if (!body) return;
  // Strip previous markers.
  body.querySelectorAll(".gutenberg-anno-marker").forEach(el => el.remove());
  const ann = _gutenbergReaderAnnotations || [];
  if (!ann.length) return;
  // Snapshot top-level children's offsets — we use these as the anchor
  // candidates. Working with first-level children only keeps it cheap
  // even for large books (a few hundred elements vs. tens of thousands
  // of text nodes).
  const children = Array.from(body.children);
  if (!children.length) return;
  const offsets = children.map(c => c.offsetTop);
  const maxScroll = body.scrollHeight;
  for (const a of ann) {
    const targetTop = (Number(a.positionPct) / 100) * maxScroll;
    // Pick the LAST child whose offsetTop <= targetTop. That's "where
    // the user was scrolled to". If targetTop is past the last child,
    // pin to the last child.
    let pick = 0;
    for (let i = 0; i < offsets.length; i++) {
      if (offsets[i] <= targetTop) pick = i;
      else break;
    }
    const anchor = children[pick];
    if (!anchor) continue;
    const marker = document.createElement("span");
    marker.className = "gutenberg-anno-marker";
    marker.dataset.annoId = String(a.id);
    marker.title = `${a.entityType}: ${a.entityName}${a.label ? " — " + a.label : ""}`;
    marker.textContent = "🔗";
    marker.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      _gutenbergDispatchEntityOpen(a);
    });
    // Prepend so the marker sits at the start of the paragraph/section
    // rather than after it — keeps it visually attached to the right
    // content even at large font sizes.
    anchor.insertBefore(marker, anchor.firstChild);
  }
}

// 🔗 Link button handler — admin captures current scroll position,
// picks entity type + name + (optional) Discogs id via a minimal
// prompt-driven flow, posts to /api/gutenberg/annotations, and
// refreshes the sidebar + markers.
//
// Prompt-driven for now to keep this small; a richer popup with type-
// ahead Discogs search would be Phase 3d.
async function _gutenbergLinkHereStart() {
  if (!window._isAdmin) {
    if (typeof showToast === "function") showToast("Linking is admin-only.", "info");
    return;
  }
  if (!_gutenbergReaderBookId) return;
  const pct = _gutenbergCurrentScrollPct();
  const entityType = (prompt("Link type? (artist / release / master / label)", "artist") || "")
    .trim().toLowerCase();
  if (!["artist", "release", "master", "label"].includes(entityType)) {
    if (entityType && typeof showToast === "function") showToast("Unknown link type — use artist, release, master, or label.", "error");
    return;
  }
  const entityName = prompt(`${entityType} name?`, "");
  if (!entityName || !entityName.trim()) return;
  const entityIdRaw = prompt(
    `Discogs ${entityType} id? (optional — leave blank to link by name only)`,
    "",
  );
  const entityId = entityIdRaw && /^\d+$/.test(entityIdRaw.trim()) ? Number(entityIdRaw.trim()) : null;
  const label = prompt("Short label / context (optional)", "") || null;
  // Capture a small snippet of nearby text for the reverse-side card.
  const snippet = _gutenbergGetSnippetAtPct(pct);
  try {
    const r = await apiFetch("/api/gutenberg/annotations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        bookId: _gutenbergReaderBookId,
        positionPct: pct,
        entityType,
        entityId,
        entityName: entityName.trim(),
        snippet,
        label,
      }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    // Push the new annotation into local state without a refetch.
    _gutenbergReaderAnnotations.push({
      id: j.id,
      bookId: _gutenbergReaderBookId,
      positionPct: pct,
      positionAnchor: null,
      entityType,
      entityId: entityId == null ? null : String(entityId),
      entityName: entityName.trim(),
      snippet,
      label,
      createdAt: j.createdAt,
    });
    // Keep the sidebar order canonical (sorted by position).
    _gutenbergReaderAnnotations.sort((a, b) => (a.positionPct - b.positionPct) || (a.id - b.id));
    _gutenbergRenderAnnotationList();
    _gutenbergInjectAnnotationMarkers();
    if (typeof showToast === "function") showToast("Link saved");
  } catch (e) {
    console.warn("[gutenberg/link]", e);
    if (typeof showToast === "function") showToast("Link failed", "error");
  }
}
window._gutenbergLinkHereStart = _gutenbergLinkHereStart;

// Grab ~160 chars of text near the current scroll position. Used as
// snippet for the reverse-side "Mentioned in books" card so the user
// gets a teaser excerpt without opening the book.
function _gutenbergGetSnippetAtPct(pct) {
  const body = document.getElementById("gutenberg-reader-body");
  if (!body) return null;
  const children = Array.from(body.children);
  if (!children.length) return null;
  const target = (pct / 100) * body.scrollHeight;
  let pick = children[0];
  for (const c of children) {
    if (c.offsetTop <= target) pick = c;
    else break;
  }
  const text = (pick.textContent || "").replace(/\s+/g, " ").trim();
  return text ? text.slice(0, 160) : null;
}
