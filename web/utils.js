// ── Config ─────────────────────────────────────────────────────────────────
const API = "";

// ── HTML strip helper (for eBay descriptions) ───────────────────────────
function stripHtml(html) {
  const div = document.createElement("div");
  div.innerHTML = html;
  // Convert <br>, <p>, <div>, <li> to newlines for readability
  div.querySelectorAll("br").forEach(el => el.replaceWith("\n"));
  div.querySelectorAll("p, div, li, tr, h1, h2, h3, h4, h5, h6").forEach(el => {
    el.prepend(document.createTextNode("\n"));
    el.append(document.createTextNode("\n"));
  });
  // Get text, collapse excessive whitespace
  let text = div.textContent || "";
  text = text.replace(/[ \t]+/g, " ");           // collapse horizontal space
  text = text.replace(/\n{3,}/g, "\n\n");         // max 2 consecutive newlines
  return text.trim();
}

// ── Auth helpers ─────────────────────────────────────────────────────────
async function apiFetch(url, options = {}) {
  const headers = { ...(options.headers ?? {}) };
  try {
    if (window._clerk?.session) {
      const t = await window._clerk.session.getToken();
      if (t) headers["Authorization"] = `Bearer ${t}`;
    }
  } catch { /* not signed in */ }
  return fetch(url, { ...options, headers });
}

// ── Shared state ─────────────────────────────────────────────────────────
let currentPage = 1;
let totalPages  = 1;
const itemCache = new Map();
let currentArtistId  = null;
let detectedArtist   = null;

// ── Genre → Style mapping ────────────────────────────────────────────────
const GENRE_STYLES = {
  "Blues":                 ["Acoustic","Boogie Woogie","Chicago Blues","Country Blues","Delta Blues","East Coast Blues","Electric Blues","Harmonica Blues","Hill Country Blues","Jump Blues","Louisiana Blues","Memphis Blues","Modern Electric Blues","Piano Blues","Piedmont Blues","Texas Blues"],
  "Brass & Military":      ["Brass Band","Concert Band","Dixieland","Marches","Military","Pipe & Drum"],
  "Children's":            ["Educational","Holiday","Lullaby","Nursery Rhymes","Story"],
  "Classical":             ["Baroque","Chamber Music","Choral","Contemporary","Electroacoustic","Impressionist","Medieval","Modern Classical","Musique Concrète","Neo-Classical","Opera","Renaissance","Romantic","Sound Art","Symphony"],
  "Electronic":            ["Acid","Acid House","Ambient","Ambient House","Bass Music","Berlin-School","Big Beat","Bitpop","Breakbeat","Breakcore","Chillwave","Chiptune","Coldwave","Cut-up/DJ","Dark Ambient","Dark Electro","Darkwave","Deep House","Doomcore","Downtempo","Drone","Drum n Bass","Dub Techno","Dubstep","EBM","Electro","Electro House","Eurodance","Euro House","Field Recording","Future Bass","Future House","Gabber","Glitch","Glitch Hop","Happy Hardcore","Hard House","Hard Techno","Hard Trance","Hardstyle","Hi NRG","House","IDM","Industrial","Italodance","Italo-Disco","Jersey Club","Jumpstyle","Leftfield","Minimal","Minimal Techno","New Beat","Noise","Nu-Disco","Progressive House","Progressive Trance","Psy-Trance","Rhythmic Noise","Schranz","Speedcore","Synthwave","Tech House","Techno","Trance","Trip Hop","Tropical House","UK Funky","UK Garage","Vaporwave","Witch House"],
  "Folk, World, & Country":["Aboriginal","African","Afrobeat","Antifolk","Appalachian Music","Bakersfield Sound","Basque Music","Bhangra","Bluegrass","Cajun","Calypso","Celtic","Country","Fado","Field Recording","Flamenco","Folk","Folk Rock","Galician Traditional","Hawaiian","Highlife","Hillbilly","Klezmer","Mouth Music","Neofolk","Nordic","Occitan","Pacific","Polka","Romani","Sea Shanties","Skiffle","Soca","Western Swing","Zydeco"],
  "Funk / Soul":            ["Bayou Funk","Boogie","Contemporary R&B","Disco","Free Funk","Funk","Funk Metal","Go-Go","Gospel","Jazz-Funk","Minneapolis Sound","Neo Soul","New Jack Swing","P.Funk","Rhythm & Blues","RnB/Swing","Soul","Swingbeat"],
  "Hip Hop":               ["Abstract","Boom Bap","Cloud Rap","Conscious","Crunk","Cut-up/DJ","Drill","G-Funk","Gangsta","Grime","Hardcore Hip-Hop","Hip-House","Horrorcore","Hyphy","Jazzy Hip-Hop","Miami Bass","Pop Rap","Ragga HipHop","Screw","Thug Rap","Trap","Turntablism"],
  "Jazz":                  ["Acid Jazz","Afro-Cuban Jazz","Avant-garde Jazz","Big Band","Bop","Bossa Nova","Cape Jazz","Contemporary Jazz","Cool Jazz","Dark Jazz","Dixieland","Free Improvisation","Free Jazz","Fusion","Future Jazz","Gypsy Jazz","Hard Bop","Jazz-Funk","Jazz-Rock","Latin Jazz","Modal","Post Bop","Smooth Jazz","Soul-Jazz","Spiritual Jazz","Swing"],
  "Latin":                 ["Afro-Cuban","Axé","Bachata","Baião","Bolero","Bossa Nova","Candombe","Cha-Cha","Chacarera","Chamamé","Charanga","Choro","Conjunto","Corrido","Cubano","Cumbia","Danzon","Descarga","Forró","Guaguancó","Guaracha","Guarania","Joropo","Latin Jazz","Latin Pop","Lambada","Mambo","Mariachi","Merengue","Milonga","MPB","Norteño","Nueva Cancion","Nueva Trova","Pachanga","Pasodoble","Porro","Ranchera","Reggaeton","Rumba","Salsa","Samba","Samba-Canção","Sertanejo","Son","Son Montuno","Tango","Tejano","Timba","Trova","Vallenato","Zamba"],
  "Non-Music":             ["Audiobook","Comedy","Dialogue","Dub Poetry","Educational","Erotic","Interview","Medical","Monolog","Novelty","Parody","Poetry","Political","Promotional","Public Broadcast","Public Service Announcement","Radioplay","Religious","Sermon","Sound Collage","Speech","Spoken Word","Story","Therapy"],
  "Pop":                   ["Alt-Pop","Anison","AOR","Ballad","Baroque Pop","Britpop","Bubblegum","Cantopop","City Pop","Dance-pop","Dream Pop","Easy Listening","Ethno-pop","Europop","Hyperpop","Indie Pop","J-pop","Jangle Pop","K-pop","Kayōkyoku","Lo-Fi","Lounge","Mandopop","New Wave","Power Pop","Schlager","Soft Rock","Sunshine Pop","Synth-pop","Vocal","Yé-Yé"],
  "Reggae":                ["Calypso","Dancehall","Dub","Dub Poetry","Lovers Rock","Mento","Ragga","Reggae Gospel","Reggae-Pop","Rocksteady","Roots Reggae","Ska"],
  "Rock":                  ["Acid Rock","Alternative Metal","Alternative Rock","Anatolian Rock","Arena Rock","Art Rock","Atmospheric Black Metal","Black Metal","Blues Rock","Classic Rock","Country Rock","Death Metal","Deathcore","Deathrock","Doom Metal","Emo","Folk Metal","Folk Rock","Funk Metal","Garage Rock","Glam","Goth Rock","Gothic Metal","Grunge","Hard Rock","Hardcore","Heavy Metal","Horror Rock","Industrial Metal","Indie Rock","J-Rock","Jazz-Rock","K-Rock","Krautrock","Lo-Fi","Math Rock","Metalcore","Mod","Neofolk","New Wave","Nintendocore","Noise Rock","No Wave","Nu Metal","Pop Punk","Post-Grunge","Post-Hardcore","Post-Metal","Post Rock","Post-Punk","Power Metal","Prog Rock","Psychedelic Rock","Psychobilly","Pub Rock","Punk","Rock & Roll","Rock Opera","Rockabilly","Shoegaze","Slowcore","Southern Rock","Space Rock","Speed Metal","Stoner Rock","Surf","Symphonic Metal","Symphonic Rock","Thrash","Viking Metal"],
  "Stage & Screen":        ["Ballet","Bollywood","Concert Film","Karaoke","Music Video","Musical","Opera","Radioplay","Rock Opera","Score","Soundtrack","Theme","Video Game Music"]
};

function populateStyles() {
  const genre = document.getElementById("f-genre").value;
  const styleSelect = document.getElementById("f-style");
  const styles = GENRE_STYLES[genre] ?? [];
  styleSelect.innerHTML = '<option value="">Any</option>' +
    styles.map(s => `<option value="${s}">${s}</option>`).join("");
  styleSelect.disabled = !styles.length;
  styleSelect.value = "";
}

// ── Utility functions ────────────────────────────────────────────────────
function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function setStatus(msg, isError = false) {
  const el = document.getElementById("status");
  el.textContent = msg;
  el.className = isError ? "content-narrow error" : "content-narrow";
  el.style.display = msg ? "block" : "none";
}

function stripDiscogsMarkup(text) {
  return text
    .replace(/\[a=([^\]]+)\]/g, '$1')
    .replace(/\[l=([^\]]+)\]/g, '$1')
    .replace(/\[url=[^\]]*\]([^\[]*)\[\/url\]/g, '$1')
    .replace(/\[[a-z]\]([^\[]*)\[\/[a-z]\]/g, '$1')
    .replace(/\[[^\]]+\]/g, '')
    .replace(/\s+([.,])/g, '$1')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function renderBioMarkup(text) {
  if (!text) return '';
  text = text.replace(/\[l=([^\]]+)\]/g, '$1');
  const parts = [];
  let lastIndex = 0;
  const re = /\[a=([^\]]+)\]/g;
  let match;
  while ((match = re.exec(text)) !== null) {
    if (match.index > lastIndex) {
      parts.push(escHtml(text.slice(lastIndex, match.index)));
    }
    const name = match[1];
    parts.push(`<a href="#" class="bio-artist-link" onclick="searchBioArtist(event,this)" data-artist="${escHtml(name)}">${escHtml(name)}</a>`);
    lastIndex = match.index + match[0].length;
  }
  if (lastIndex < text.length) parts.push(escHtml(text.slice(lastIndex)));
  let html = parts.join('');
  html = html.replace(/\[[^\]]+\]/g, '');
  html = html.replace(/\n/g, '<br>');
  return html;
}

function truncateRaw(rawText, plainLimit) {
  let plainCount = 0;
  let i = 0;
  let lastSpace = -1;
  while (i < rawText.length) {
    if (rawText[i] === '[') {
      const end = rawText.indexOf(']', i);
      if (end !== -1) {
        const tag = rawText.slice(i, end + 1);
        const nameMatch = tag.match(/^\[a=([^\]]+)\]$/);
        if (nameMatch) plainCount += nameMatch[1].length;
        i = end + 1;
        continue;
      }
    }
    if (rawText[i] === ' ' && plainCount <= plainLimit) lastSpace = i;
    if (plainCount >= plainLimit) {
      return lastSpace > 0 ? rawText.slice(0, lastSpace) : rawText.slice(0, i);
    }
    plainCount++;
    i++;
  }
  return rawText;
}

function artistNamesMatch(searched, returned) {
  if (!searched || !returned) return true;
  const words = s => s.toLowerCase().replace(/[^a-z0-9\s]/g, "").trim().split(/\s+/).filter(w => w.length > 3);
  const a = words(searched);
  const b = new Set(words(returned));
  if (a.length === 0 || b.size === 0) return true;
  return a.some(w => b.has(w));
}

function sharePopup(btn) {
  navigator.clipboard.writeText(window.location.href).then(() => {
    const orig = btn.textContent;
    btn.textContent = "copied!";
    btn.style.color = "var(--accent)";
    setTimeout(() => { btn.textContent = orig; btn.style.color = ""; }, 1800);
  }).catch(() => {
    prompt("Copy this link:", window.location.href);
  });
}

function copySearchUrl(el) {
  navigator.clipboard.writeText(window.location.href).then(() => {
    const orig = el.textContent;
    el.textContent = "link copied!";
    el.style.color = "var(--accent)";
    setTimeout(() => { el.textContent = orig; el.style.color = ""; }, 1800);
  }).catch(() => {
    prompt("Copy this link:", window.location.href);
  });
}

function toTitleCase(s) {
  return s.replace(/\w\S*/g, w => w.charAt(0).toUpperCase() + w.slice(1));
}

// normP — now in shared.js

function feedLabel(raw) {
  const p = normP(raw);
  const tc = s => toTitleCase(s);
  const parts = [];
  if (p.q && (!p.a || p.q.toLowerCase() !== p.a.toLowerCase())) parts.push(tc(p.q));
  if (p.a) parts.push(tc(p.a));
  if (p.r) parts.push(tc(p.r));
  if (p.l) parts.push(`${tc(p.l)} label`);
  if (p.g) parts.push(p.g);
  if (p.s) parts.push(p.s);
  if (p.f) parts.push(p.f);
  if (p.y) parts.push(p.y);
  const full = parts.join(" · ") || "Search";
  const short = full.length > 24 ? full.slice(0, 23) + "…" : full;
  return { full, short };
}

function feedApply(raw) {
  if (!raw) return;
  const p = normP(raw);
  try {
    switchView("search", true);
    document.getElementById("query").value     = p.q ?? "";
    document.getElementById("f-artist").value  = p.a ?? "";
    document.getElementById("f-release").value = p.r ?? "";
    document.getElementById("f-year").value    = p.y ?? "";
    document.getElementById("f-label").value   = p.l ?? "";
    document.getElementById("f-format").value  = p.f || "";
    document.getElementById("f-genre").value   = p.g ?? "";
    populateStyles();
    document.getElementById("f-style").value   = p.s ?? "";
    const typeVal = p.t ?? "";
    const radio = document.querySelector(`input[name="result-type"][value="${typeVal}"]`);
    if (radio) radio.checked = true;
    const feedSortEl = document.getElementById("f-sort");
    feedSortEl.value = p.o ?? "";
    if (!feedSortEl.value) feedSortEl.selectedIndex = 0;
    const needsAdv = p.a || p.r || p.y || p.l || p.g || p.s || p.f;
    if (needsAdv) toggleAdvanced(true);
    doSearch(1);
  } catch (e) {
    setStatus("Could not restore search: " + e.message, false);
  }
}

// ── Rel-popup overflow helpers ───────────────────────────────────────────
window._relPopups = {};
let _relPopupIdx = 0;
function _storeRelPopup(items, isLinks) {
  const key = "rp" + (++_relPopupIdx);
  window._relPopups[key] = { items, isLinks };
  return key;
}

function showRelPopup(event, key) {
  event.preventDefault();
  event.stopPropagation();
  const store = window._relPopups[key];
  if (!store) return;
  let popup = document.getElementById("rel-overflow-popup");
  if (!popup) {
    popup = document.createElement("div");
    popup.id = "rel-overflow-popup";
    popup.style.cssText = "position:absolute;z-index:600;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:0.6rem 0.85rem;max-width:260px;font-size:0.78rem;line-height:1.7;box-shadow:0 4px 20px rgba(0,0,0,0.6);display:none";
    popup.onclick = e => e.stopPropagation();
    document.body.appendChild(popup);
    document.addEventListener("click", () => { popup.style.display = "none"; });
  }
  if (store.isLinks) {
    popup.innerHTML = store.items.map(u =>
      `<div><a href="${escHtml(u)}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">${escHtml(u.replace(/^https?:\/\//, "").replace(/\/$/, ""))}</a></div>`
    ).join("");
  } else {
    popup.innerHTML = store.items.map(a => typeof a === "string"
      ? `<div>${escHtml(a)}</div>`
      : `<div><a href="#" class="bio-artist-link" onclick="searchBioArtist(event,this);document.getElementById('rel-overflow-popup').style.display='none'" data-artist="${escHtml(a.name)}"${a.id ? ` data-artist-id="${a.id}"` : ""}>${escHtml(a.name)}</a></div>`
    ).join("");
  }
  const rect = event.target.getBoundingClientRect();
  popup.style.display = "block";
  popup.style.top  = (rect.bottom + window.scrollY + 4) + "px";
  popup.style.left = Math.min(rect.left + window.scrollX, window.innerWidth - 270) + "px";
}

function renderArtistRelations(members = [], groups = [], aliases = [], namevariations = [], urls = [], parentLabel = null, sublabels = [], compact = false) {
  const LIMIT = 3;

  const moreBtn = (overflow, isLinks) => {
    if (!overflow.length) return "";
    const key = _storeRelPopup(overflow, isLinks);
    return ` <a href="#" style="font-size:0.72rem;color:var(--muted);white-space:nowrap;text-decoration:none" onclick="showRelPopup(event,'${key}')">+${overflow.length} more</a>`;
  };

  const row = (label, items) => {
    if (!items.length) return "";
    const visible  = compact ? items.slice(0, LIMIT) : items;
    const overflow = compact ? items.slice(LIMIT) : [];
    const links = visible.map(a =>
      `<a href="#" class="bio-artist-link" onclick="searchBioArtist(event,this)" data-artist="${escHtml(a.name)}"${a.id ? ` data-artist-id="${a.id}"` : ""}>${escHtml(a.name)}</a>`
    ).join('<span style="color:#555;margin:0 0.2em">·</span>');
    return `<div style="font-size:0.78rem;margin-top:0.55rem;line-height:1.6">
              <span style="color:#777;margin-right:0.4em">${label}:</span>${links}${moreBtn(overflow, false)}
            </div>`;
  };
  const urlRow = (label, items) => {
    const filtered = items.filter(u => !/facebook\.com|myspace\.com/i.test(u));
    if (!filtered.length) return "";
    const visible  = compact ? filtered.slice(0, LIMIT) : filtered;
    const overflow = compact ? filtered.slice(LIMIT) : [];
    const links = visible.map(u =>
      `<div><a href="${escHtml(u)}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none">${escHtml(u.replace(/^https?:\/\//, "").replace(/\/$/, ""))}</a></div>`
    ).join("");
    return `<div style="font-size:0.78rem;margin-top:0.55rem;line-height:1.8">
              <span style="color:#777;margin-right:0.4em">${label}:</span>${links}${moreBtn(overflow, true)}
            </div>`;
  };
  const akaRow = (label, items) => {
    if (!items.length || compact) return "";
    const links = items.map(n =>
      `<a href="#" class="bio-artist-link" onclick="searchBioArtist(event,this)" data-artist="${escHtml(n)}" style="color:var(--accent);text-decoration:none">${escHtml(n)}</a>`
    ).join('<span style="color:#555;margin:0 0.2em">·</span>');
    return `<div style="font-size:0.78rem;margin-top:0.55rem;line-height:1.6">
              <span style="color:#777;margin-right:0.4em">${label}:</span>${links}
            </div>`;
  };
  const html = row("Members", members)
    + row("Also in", groups)
    + row("Aliases", aliases)
    + akaRow("Also known as", namevariations)
    + (parentLabel ? row("Part of", [parentLabel]) : "")
    + row("Sub-labels", sublabels)
    + urlRow("Links", urls);
  return html ? `<div style="margin-top:0.6rem;padding-top:0.5rem;border-top:1px solid var(--border)">${html}</div>` : "";
}

// ── Toast notifications ──────────────────────────────────────────────────
function showToast(message, type = "error", duration = 4000) {
  let container = document.getElementById("toast-container");
  if (!container) {
    container = document.createElement("div");
    container.id = "toast-container";
    document.body.appendChild(container);
  }
  const toast = document.createElement("div");
  toast.className = `toast toast-${type}`;
  toast.textContent = message;
  container.appendChild(toast);
  setTimeout(() => {
    toast.classList.add("toast-removing");
    toast.addEventListener("animationend", () => toast.remove());
  }, duration);
}

// ── Loading skeleton helpers ─────────────────────────────────────────────
function renderSkeletonGrid(count = 16) {
  return Array.from({ length: count }, () =>
    `<div class="skeleton-card card-animate"><div class="skeleton-thumb"></div><div class="skeleton-line" style="margin-top:10px"></div><div class="skeleton-line short"></div><div class="skeleton-line shorter"></div></div>`
  ).join("");
}

function renderSkeletonRows(count = 4) {
  return Array.from({ length: count }, () =>
    `<div style="display:flex;gap:0.75rem;align-items:center;padding:0.6rem 0;border-bottom:1px solid var(--border)"><div class="skeleton" style="width:50px;height:50px;border-radius:6px;flex-shrink:0"></div><div style="flex:1"><div class="skeleton-line"></div><div class="skeleton-line short" style="margin-top:8px"></div></div></div>`
  ).join("");
}

function renderFeedSkeletonGrid(count = 16) {
  return Array.from({ length: count }, () =>
    `<div class="skeleton-card card-animate" style="display:flex;flex-direction:row;height:130px"><div class="skeleton" style="width:120px;height:100%;flex-shrink:0"></div><div style="flex:1;padding:0.5rem 0.6rem"><div class="skeleton-line" style="width:40%;margin-bottom:8px"></div><div class="skeleton-line"></div><div class="skeleton-line short" style="margin-top:8px"></div><div class="skeleton-line shorter" style="margin-top:8px"></div></div></div>`
  ).join("");
}

// ── Empty state helper ───────────────────────────────────────────────────
function renderEmptyState(icon, title, subtitle) {
  return `<div class="empty-state"><div class="empty-state-icon">${icon}</div><div class="empty-state-title">${escHtml(title)}</div><div class="empty-state-subtitle">${escHtml(subtitle)}</div></div>`;
}

// ── Scroll-to-top button ─────────────────────────────────────────────────
(function initScrollToTop() {
  const btn = document.createElement("button");
  btn.id = "scroll-top-btn";
  btn.setAttribute("aria-label", "Scroll to top");
  btn.innerHTML = "&#8593;";
  document.body.appendChild(btn);
  btn.addEventListener("click", () => window.scrollTo({ top: 0, behavior: "smooth" }));
  window.addEventListener("scroll", () => {
    btn.classList.toggle("visible", window.scrollY > 400);
  }, { passive: true });
})();

// ── Keyboard shortcuts ───────────────────────────────────────────────────
document.addEventListener("keydown", (e) => {
  // Don't fire when typing in an input/textarea
  const tag = document.activeElement?.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") {
    if (e.key === "Escape") document.activeElement.blur();
    return;
  }
  if (e.key === "Escape") {
    // Close any open modal first
    const overlay = document.getElementById("modal-overlay");
    if (overlay && overlay.style.display !== "none" && overlay.style.display !== "") {
      if (typeof closeModal === "function") closeModal();
      return;
    }
    const vOverlay = document.getElementById("version-overlay");
    if (vOverlay && vOverlay.style.display !== "none" && vOverlay.style.display !== "") {
      if (typeof closeVersionPopup === "function") closeVersionPopup();
      return;
    }
    // Clear search
    const q = document.getElementById("query");
    if (q && q.value) {
      q.value = "";
      q.focus();
    }
  }
  if (e.key === "/" && !e.ctrlKey && !e.metaKey) {
    e.preventDefault();
    const q = document.getElementById("query");
    if (q) { q.focus(); q.select(); }
  }
});

// ── Saved searches ──────────────────────────────────────────────────────
const _savedSearchCache = {};

async function loadSavedSearches(view) {
  try {
    const data = await apiFetch(`/api/user/saved-searches?view=${encodeURIComponent(view)}`).then(r => r.json());
    _savedSearchCache[view] = data.searches ?? [];
  } catch { _savedSearchCache[view] = []; }
  return _savedSearchCache[view];
}

function buildSavedSearchUI(view, getParamsFn, applyFn, containerEl) {
  if (!containerEl) return;
  const wrap = document.createElement("div");
  wrap.className = "saved-search-wrap";
  wrap.id = `saved-search-${view}`;
  wrap.innerHTML = `
    <button class="saved-search-toggle" id="ss-toggle-${view}" onclick="toggleSavedDropdown('${view}')" title="Saved searches">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>
    </button>
    <div class="saved-search-dropdown" id="ss-dropdown-${view}" style="display:none">
      <div class="ss-header">
        <span style="font-weight:600;font-size:0.72rem;color:#aaa">Saved Searches</span>
        <button class="ss-save-btn" id="ss-save-${view}" onclick="saveCurrentSearch('${view}')" title="Save current search">+ Save</button>
      </div>
      <div class="ss-list" id="ss-list-${view}"></div>
    </div>`;
  containerEl.appendChild(wrap);

  // Store callbacks on the element
  wrap._getParams = getParamsFn;
  wrap._apply = applyFn;

  // Load saved searches if logged in
  if (window._clerk?.user) {
    loadSavedSearches(view).then(searches => renderSavedList(view, searches));
  }
}

function toggleSavedDropdown(view) {
  const dd = document.getElementById(`ss-dropdown-${view}`);
  if (!dd) return;
  const showing = dd.style.display !== "none";
  // Close all other dropdowns first
  document.querySelectorAll(".saved-search-dropdown").forEach(d => d.style.display = "none");
  if (!showing) {
    dd.style.display = "";
    // Refresh list
    loadSavedSearches(view).then(searches => renderSavedList(view, searches));
  }
}

function renderSavedList(view, searches) {
  const list = document.getElementById(`ss-list-${view}`);
  if (!list) return;
  if (!searches.length) {
    list.innerHTML = '<div class="ss-empty">No saved searches</div>';
    return;
  }
  list.innerHTML = searches.map(s =>
    `<div class="ss-item" id="ss-item-${s.id}">
      <button class="ss-item-btn" onclick="applySavedSearch('${escHtml(view)}',${s.id})" title="${escHtml(JSON.stringify(s.params))}">${escHtml(s.label)}</button>
      <button class="ss-item-del" onclick="deleteSavedSearchItem(event,'${escHtml(view)}',${s.id})" title="Remove">&times;</button>
    </div>`
  ).join("");
}

async function saveCurrentSearch(view) {
  const wrap = document.getElementById(`saved-search-${view}`);
  if (!wrap?._getParams) return;
  const params = wrap._getParams();
  // Build a short label from the params
  const parts = [];
  for (const [k, v] of Object.entries(params)) {
    if (v && k !== "page" && k !== "per_page") parts.push(String(v));
  }
  const label = parts.join(" · ").slice(0, 200) || "Untitled";
  try {
    const r = await apiFetch("/api/user/saved-searches", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ view, label, params })
    }).then(r => r.json());
    if (r.ok) {
      showToast("Search saved");
      loadSavedSearches(view).then(searches => renderSavedList(view, searches));
    } else {
      showToast(r.error || "Failed to save", "error");
    }
  } catch { showToast("Failed to save search", "error"); }
}

function applySavedSearch(view, id) {
  const wrap = document.getElementById(`saved-search-${view}`);
  const searches = _savedSearchCache[view] ?? [];
  const s = searches.find(x => x.id === id);
  if (!s || !wrap?._apply) return;
  wrap._apply(s.params);
  // Close dropdown
  const dd = document.getElementById(`ss-dropdown-${view}`);
  if (dd) dd.style.display = "none";
}

async function deleteSavedSearchItem(event, view, id) {
  event.stopPropagation();
  try {
    await apiFetch(`/api/user/saved-searches/${id}`, { method: "DELETE" });
    const el = document.getElementById(`ss-item-${id}`);
    if (el) el.remove();
    _savedSearchCache[view] = (_savedSearchCache[view] ?? []).filter(s => s.id !== id);
    const list = document.getElementById(`ss-list-${view}`);
    if (list && !list.children.length) list.innerHTML = '<div class="ss-empty">No saved searches</div>';
    showToast("Search removed");
  } catch { showToast("Failed to remove", "error"); }
}

// Close dropdowns when clicking outside
document.addEventListener("click", e => {
  if (!e.target.closest(".saved-search-wrap")) {
    document.querySelectorAll(".saved-search-dropdown").forEach(d => d.style.display = "none");
  }
});
