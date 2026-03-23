// ── Config ─────────────────────────────────────────────────────────────────
// Use relative URLs so this works on any deployment (production, test, local)
const API = "";

// ── Auth helpers ─────────────────────────────────────────────────────────
// apiFetch wraps fetch() and automatically attaches the Clerk session token
// when the user is signed in, so the backend can identify them.
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

// ── State ─────────────────────────────────────────────────────────────────
let currentPage = 1;
let totalPages  = 1;
const itemCache = new Map(); // id → search result item
let currentArtistId  = null;  // Discogs artist ID when navigating from a known card/link
let detectedArtist   = null;  // artist name auto-detected from bio on page 1, used for consistent pagination

// ── Genre → Style mapping ─────────────────────────────────────────────────
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

// ── Advanced panel toggle ─────────────────────────────────────────────────
function toggleAdvanced(forceOpen) {
  const isAi = document.querySelector('input[name="result-type"]:checked')?.value === "ai";
  if (isAi) {
    // Show inline hint then fade it out
    const btn = document.getElementById("advanced-toggle");
    const existing = document.getElementById("ai-adv-hint");
    if (!existing) {
      const hint = document.createElement("span");
      hint.id = "ai-adv-hint";
      hint.textContent = "Not available in AI mode";
      hint.style.cssText = "font-size:0.72rem;color:#888;margin-left:0.5rem;transition:opacity 1.5s";
      btn.parentNode.insertBefore(hint, btn.nextSibling);
      setTimeout(() => hint.style.opacity = "0", 1500);
      setTimeout(() => hint.remove(), 3000);
    }
    return;
  }
  const panel = document.getElementById("advanced-panel");
  const arrow = document.getElementById("advanced-arrow");
  const open  = forceOpen !== undefined ? forceOpen : panel.dataset.open !== "true";
  panel.dataset.open = open ? "true" : "false";
  arrow.textContent  = open ? "▼" : "▶";
}

// ── URL state helpers ────────────────────────────────────────────────────
function pushSearchState(q, artistRaw, release, year, label, genre, sort, resultType, page) {
  const p = new URLSearchParams();
  if (q)          p.set("q",  q);
  if (artistRaw)  p.set("ar", artistRaw);
  if (release)    p.set("re", release);
  if (year)       p.set("yr", year);
  if (label)      p.set("lb", label);
  if (genre)      p.set("gn", genre);
  const style  = document.getElementById("f-style")?.value ?? "";
  const format = document.getElementById("f-format")?.value ?? "";
  if (style)      p.set("st", style);
  if (format) p.set("fm", format);
  if (sort)       p.set("sr", sort);
  if (resultType) p.set("rt", resultType);
  if (page > 1)   p.set("pg", String(page));
  history.pushState({}, "", p.toString() ? "?" + p.toString() : location.pathname);
}

function restoreFromParams(p) {
  document.getElementById("query").value     = p.get("q")  ?? "";
  document.getElementById("f-artist").value  = p.get("ar") ?? "";
  document.getElementById("f-release").value = p.get("re") ?? "";
  document.getElementById("f-year").value    = p.get("yr") ?? "";
  document.getElementById("f-label").value   = p.get("lb") ?? "";
  document.getElementById("f-genre").value   = p.get("gn") ?? "";
  const sortEl = document.getElementById("f-sort");
  sortEl.value = p.get("sr") ?? "";
  if (!sortEl.value) sortEl.selectedIndex = 0;
  document.getElementById("f-format").value  = p.get("fm") || "";
  populateStyles();
  document.getElementById("f-style").value   = p.get("st") ?? "";
  const rtype = p.get("rt") ?? "";
  const radio = document.querySelector(`input[name="result-type"][value="${rtype}"]`);
  if (radio) radio.checked = true;
  // Auto-open advanced panel if any advanced fields are in use
  const hasAdvanced = p.get("ar") || p.get("re") || p.get("yr") || p.get("lb") || p.get("gn") || p.get("st") || p.get("fm");
  if (hasAdvanced) toggleAdvanced(true);
}

function clearForm() {
  ["query","f-artist","f-release","f-year","f-label","f-genre","f-style"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });
  document.getElementById("f-sort").value = "";
  document.getElementById("f-format").value = "";
  document.getElementById("f-style").innerHTML = '<option value="">Any</option>';
  document.getElementById("f-style").disabled = true;
  document.querySelector('input[name="result-type"][value=""]').checked = true;
  
  document.getElementById("search-desc").textContent = "";
  document.getElementById("search-returned").textContent = "";
  document.getElementById("search-ai-summary").textContent = "";
  document.getElementById("search-info-block").style.display = "none";
}

async function doSearch(page = 1, skipPushState = false) {
  const q         = document.getElementById("query").value.trim();
  const advOpen   = document.getElementById("advanced-panel")?.dataset.open === "true";
  const artistRaw = advOpen ? document.getElementById("f-artist").value.trim() : "";
  const artist    = artistRaw.replace(/\s*\(\d+\)$/, ""); // stripped for search params
  const release   = advOpen ? document.getElementById("f-release").value.trim() : "";
  const year      = advOpen ? document.getElementById("f-year").value.trim() : "";
  const label     = advOpen ? document.getElementById("f-label").value.trim() : "";
  const genre     = advOpen ? document.getElementById("f-genre").value.trim() : "";
  const style     = advOpen ? (document.getElementById("f-style")?.value.trim() ?? "") : "";
  const format    = advOpen ? document.getElementById("f-format").value : "";
  const sort      = document.getElementById("f-sort").value;
  const resultType = document.querySelector('input[name="result-type"]:checked')?.value ?? "";

  if (resultType === "ai") { doAiSearch(q); return; }

  // Radio buttons are the ONLY thing that determines result type

  if (!q && !artist && !release && !year && !label && !genre) {
    setStatus("Enter a search term or fill in at least one filter.", false);
    return;
  }

  // Always ensure search view is active and navbar reflects it
  switchView("search", true);
  setActiveTab("search");

  if (!skipPushState) pushSearchState(q, artistRaw, release, year, label, genre, sort, resultType, page);

  // Clear auto-detected artist on every new page-1 search so it doesn't bleed into unrelated searches
  if (page === 1) detectedArtist = null;

  currentPage = page;
  document.getElementById("search-btn").disabled = true;
  document.getElementById("pagination").style.display = "none";
  document.getElementById("blurb").style.display = "none";
  document.getElementById("artist-alts").innerHTML = "";
  closeAltsPopup();
  setStatus("Searching…");
  document.getElementById("results").innerHTML = "";

  if (page === 1) {
    const parts = [];
    if (q)       parts.push(q);
    if (artist)  parts.push(`Artist: ${artist}`);
    if (release) parts.push(`Release: ${release}`);
    if (year)    parts.push(`Year: ${year}`);
    if (label)   parts.push(`Label: ${label}`);
    if (genre)   parts.push(`Genre: ${genre}`);
    if (style)   parts.push(`Style: ${style}`);
    if (format)  parts.push(`Format: ${format}`);
    const typeLabels = { "master":"Masters", "release":"Releases", "artist":"Artists", "label":"Labels" };
    const typeLabel = typeLabels[resultType] ?? "";
    const sortLabels = { "year:asc":"Year ↑", "year:desc":"Year ↓", "title:asc":"Title A→Z", "title:desc":"Title Z→A", "label:asc":"Label A→Z" };
    const sortLabel = sortLabels[sort] ?? "";
    const extras = [typeLabel, sortLabel].filter(Boolean).join(" · ");
    const searchTerms = parts.join(", ") + (extras ? "  ·  " + extras : "");
    const descEl = document.getElementById("search-desc");
    if (parts.length) {
      descEl.innerHTML = `Searched :: <span onclick="copySearchUrl(this)" title="Click to copy search link" style="cursor:pointer;border-bottom:1px dotted transparent;transition:border-color 0.2s" onmouseover="this.style.borderBottomColor='var(--accent)'" onmouseout="this.style.borderBottomColor='transparent'">${escHtml(searchTerms)}</span>`;
    } else {
      descEl.textContent = "";
    }
    document.getElementById("search-returned").textContent = "";
    document.getElementById("search-ai-summary").textContent = "";
    if (parts.length) document.getElementById("search-info-block").style.display = "";
  }

  const buildParams = (perPage) => {
    const effectiveArtist = artist || (page > 1 ? detectedArtist : null) || "";
    const p = new URLSearchParams({ page, per_page: perPage });
    // Only send q when there's an actual general search term
    if (q) p.set("q", q);
    if (resultType) p.set("type", resultType);
    if (effectiveArtist) p.set("artist", effectiveArtist);
    if (release) p.set("release_title", release);
    if (year)    p.set("year",          year);
    if (label)   p.set("label",         label);
    if (genre)   p.set("genre",         genre);
    if (style)   p.set("style",         style);
    if (format)  p.set("format",        format);
    if (sort) {
      const [sortField, sortOrder] = sort.split(":");
      p.set("sort",       sortField);
      p.set("sort_order", sortOrder);
    }
    return p;
  };

  try {
    let bioFetch = null;
    if (page === 1) {
      if (artistRaw) {
        const bioUrl = `${API}/artist-bio?name=${encodeURIComponent(artistRaw)}`
                     + (currentArtistId ? `&id=${currentArtistId}` : "");
        bioFetch = apiFetch(bioUrl).catch(() => null);
      } else if (label) {
        bioFetch = apiFetch(`${API}/label-bio?name=${encodeURIComponent(label)}`).catch(() => null);
      } else if (genre) {
        bioFetch = apiFetch(`${API}/genre-info?genre=${encodeURIComponent(genre)}`).catch(() => null);
      }
    }

    let items, totalPages_new, totalItems_new = 0;
    const [res, bioRes] = await Promise.all([
      apiFetch(`${API}/search?${buildParams(24)}`),
      bioFetch ?? Promise.resolve(null),
    ]);
    bioFetch = bioRes ? { json: () => bioRes.json() } : null;
    if (res.status === 401 || res.status === 429) {
      const errData = await res.json().catch(() => ({}));
      if (errData.error === "no_token") {
        document.getElementById("status").innerHTML =
          `<a href="/account" style="color:var(--accent)">Sign in and add your Discogs token</a> to start searching.`;
        return;
      }
      if (errData.error === "rate_limited") {
        document.getElementById("status").innerHTML =
          `You've used your 5 free searches for today. <a href="/account" style="color:var(--accent)">Add your Discogs token</a> for unlimited searches.`;
        return;
      }
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    items = data.results ?? [];
    totalPages_new = data.pagination?.pages ?? 1;
    totalItems_new = data.pagination?.items ?? items.length;
    totalPages = totalPages_new;

    const blurbEl = document.getElementById("blurb");
    if (page === 1) {
      let bioData = null;
      if (bioFetch) {
        bioData = await bioFetch.json().catch(() => null);
      }
      if (!bioData && !q && !artist && !release && !label && !genre && items.length > 0) {
        const firstMedia = items.find(it => (it.type === "release" || it.type === "master") && it.title?.includes(" - "));
        if (firstMedia) {
          const derivedArtist = firstMedia.title.slice(0, firstMedia.title.indexOf(" - "));
          bioData = await apiFetch(`${API}/artist-bio?name=${encodeURIComponent(derivedArtist)}`).then(r => r.json()).catch(() => null);
        }
      }
      // Store alternatives for the bio full popup (not shown on the bio card)
      const alts = (bioData?.alternatives ?? []).filter(a => a.name);
      document.getElementById("artist-alts").innerHTML = "";
      if (alts.length > 0) {
        const popupEl = document.getElementById("alts-popup");
        popupEl.innerHTML = `<h4>Other artists</h4>` +
          alts.map(a => `<a href="#" data-alt-name="${escHtml(a.name)}"${a.id ? ` data-alt-id="${a.id}"` : ""} onclick="selectAltArtist(event,this);closeAltsPopup()">${escHtml(a.name)}</a>`).join("");
      } else {
        document.getElementById("alts-popup").innerHTML = "<h4>Other artists</h4>";
      }

      const firstMediaItem = items.find(it => (it.type === "release" || it.type === "master") && it.title?.includes(" - "));
      const searchedArtistName = artistRaw || (!label && !genre && firstMediaItem
        ? firstMediaItem.title.slice(0, firstMediaItem.title.indexOf(" - ")) : "");
      if (bioData && artistNamesMatch(searchedArtistName, bioData.name) === false) {
        bioData = null;
      }

      // If bio was auto-detected on page 1, re-fetch constrained to that artist and save for pagination
      if (bioData?.name && !artistRaw && !release && !label && !genre && page === 1) {
        const constrainedArtist = bioData.name.replace(/\s*\(\d+\)$/, "").trim();
        detectedArtist = constrainedArtist; // saved so pages 2+ use it directly in buildParams
        try {
          const cp = new URLSearchParams({ q: q || constrainedArtist, page, per_page: 48, artist: constrainedArtist });
          if (resultType) cp.set("type", resultType);
          if (release)    cp.set("release_title", release);
          if (year)       cp.set("year", year);
          if (format)     cp.set("format", format);
          if (sort) { const [sf, so] = sort.split(":"); cp.set("sort", sf); cp.set("sort_order", so); }
          const cr = await apiFetch(`${API}/search?${cp}`);
          if (cr.ok) {
            const cd = await cr.json();
            if ((cd.results ?? []).length > 0) {
              items = cd.results;
              totalPages = cd.pagination?.pages ?? totalPages;
              totalItems_new = cd.pagination?.items ?? totalItems_new;
            }
          }
        } catch { /* keep original results */ }
      }

      if (bioData?.profile) {
        window._currentBio = {
          name: bioData.name, text: bioData.profile ?? null,
          members:        bioData.members        ?? [],
          groups:         bioData.groups         ?? [],
          aliases:        bioData.aliases        ?? [],
          namevariations: bioData.namevariations ?? [],
          urls:           bioData.urls           ?? [],
          parentLabel:    bioData.parentLabel    ?? null,
          sublabels:      bioData.sublabels      ?? [],
          discogsId: bioData.discogsId ?? null,
          alternatives:   alts,
        };

        const rawBioText  = bioData.profile ?? null;
        const displayText = rawBioText ? stripDiscogsMarkup(rawBioText) : "";
        const TRUNCATE = 552;
        const needsMore = displayText.length > TRUNCATE;
        const truncatedRaw = rawBioText
          ? (needsMore ? truncateRaw(rawBioText, TRUNCATE) + '\u2026' : rawBioText)
          : "";

        const readMore = needsMore
          ? ` <a href="#" onclick="openBioFull(event)" style="font-size:0.8rem;color:var(--accent);white-space:nowrap;text-decoration:none">read more</a>`
          : "";
        const heading = bioData.name
          ? `<strong style="display:block;margin-bottom:0.4rem;color:var(--accent)">${escHtml(bioData.name)}</strong>`
          : "";

        const relLinks = renderArtistRelations(
          bioData.members        ?? [], bioData.groups    ?? [], bioData.aliases  ?? [],
          bioData.namevariations ?? [], [],
          bioData.parentLabel    ?? null, bioData.sublabels ?? [], true
        );
        const bioHtml  = rawBioText ? renderBioMarkup(truncatedRaw) : escHtml(truncatedRaw);
        blurbEl.innerHTML = heading + bioHtml + readMore + relLinks;
        blurbEl.style.display = "block";

        // Mark this search as having a bio (for recent search cloud filtering)
        apiFetch("/api/user/mb", { method: "POST" }).catch(() => {});
        // Add bio marker to URL
        const bu = new URL(window.location.href);
        if (!bu.searchParams.has("b")) { bu.searchParams.set("b", "y"); history.replaceState({}, "", bu.toString()); }
      }
    }

    if (!items.length) {
      setStatus("");
      document.getElementById("search-ai-summary").innerHTML = "<i>Couldn't find any results at Discogs.</i>";
      document.getElementById("search-info-block").style.display = "";
      return;
    }

    setStatus("");
    const returnedMsg = `Returned :: ${totalItems_new.toLocaleString()} results — page ${currentPage} of ${totalPages}`;
    document.getElementById("search-returned").textContent = returnedMsg;
    document.getElementById("search-info-block").style.display = "";
    renderResults(items);
    renderPagination();

    // Async Claude quality phrase (fire-and-forget, every page)
    {
      const qualityQuery = [
        q,
        artist  ? `Artist: ${artist}`   : "",
        release ? `Release: ${release}` : "",
        year    ? `Year: ${year}`       : "",
        label   ? `Label: ${label}`     : "",
        genre   ? `Genre: ${genre}`     : "",
        style   ? `Style: ${style}`     : "",
      ].filter(Boolean).join(", ");
      const qualityTitles = items.slice(0, 6).map(it => it.title ?? it.name ?? "").filter(Boolean);
      if (qualityQuery && qualityTitles.length) {
        const aiEl = document.getElementById("search-ai-summary");
        if (aiEl) aiEl.textContent = "…";
        fetch("/api/result-quality", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query: qualityQuery, titles: qualityTitles }),
        }).then(r => r.json()).then(d => {
          if (aiEl) aiEl.textContent = d.phrase || "";
        }).catch(() => {
          if (aiEl) aiEl.textContent = "";
        });
      }
    }

    // GA4: track search as virtual page view + search event
    if (typeof gtag === "function") {
      gtag("event", "page_view", {
        page_location: window.location.href,
        page_path:     window.location.pathname + window.location.search,
        page_title:    document.title
      });
      gtag("event", "search", {
        search_term: [q, artist, label, genre].filter(Boolean).join(" | ") || "(filters only)"
      });
    }

    const urlP = new URLSearchParams(location.search);
    const openParam  = urlP.get("op");
    const videoParam = urlP.get("vd");
    if (openParam) {
      const colon = openParam.indexOf(":");
      const pType = openParam.slice(0, colon);
      const pId   = openParam.slice(colon + 1);
      const pUrl  = `https://www.discogs.com/${pType}/${pId}`;
      openModal(null, pId, pType, pUrl);
      const versionParam = urlP.get("vr");
      if (versionParam) {
        setTimeout(() => openVersionPopup(null, versionParam), 1200);
      }
      if (videoParam) {
        setTimeout(() => openVideo(null, `https://www.youtube.com/watch?v=${videoParam}`), 1200);
      }
    } else if (urlP.get("bi") === "1") {
      openBioFull(null);
    } else if (videoParam) {
      openVideo(null, `https://www.youtube.com/watch?v=${videoParam}`);
    }
  } catch (e) {
    setStatus("Search failed: " + e.message, true);
  } finally {
    document.getElementById("search-btn").disabled = false;
  }
}

// ── Render cards ──────────────────────────────────────────────────────────
function renderResults(items) {
  window._lastResults = items;
  const hideOwned = document.getElementById("hide-owned")?.checked;
  const filtered = hideOwned && window._collectionIds?.size
    ? items.filter(item => !window._collectionIds.has(Number(item.id)))
    : items;
  const grid = document.getElementById("results");
  grid.innerHTML = filtered.map(item => renderCard(item)).join("");
}

function renderCard(item) {
  const url  = item.uri ? `https://www.discogs.com${item.uri}` : "#";
  const type = item.type ?? "";

  let artist = "";
  let title  = item.title ?? "Unknown";
  if ((type === "release" || type === "master") && title.includes(" - ")) {
    const idx = title.indexOf(" - ");
    artist = title.slice(0, idx);
    title  = title.slice(idx + 3);
  }

  const label   = (item.label   ?? []).slice(0, 2).join(", ");
  const catno   = (type === "release" || type === "master") ? (item.catno ?? "") : "";
  const formats = (item.format  ?? []).slice(0, 3).join(" · ");
  const genre   = (item.genre   ?? []).slice(0, 1).join("");
  const country = item.country ?? "";
  const year    = item.year    ?? "";

  const metaParts = [year, type, country].filter(Boolean);

  const thumb = item.cover_image
    ? `<img src="${item.cover_image}" alt="${escHtml(title)}" loading="lazy" />`
    : `<div class="thumb-placeholder">♪</div>`;

  const isRelease = type === "release" || type === "master";
  const isArtist  = type === "artist";
  const isLabel   = type === "label";
  if (isRelease) itemCache.set(String(item.id), item);
  const typeClass = `card card-type-${type}`;
  const cardAttrs = isRelease
    ? `class="${typeClass}" href="#" onclick="openModal(event,'${item.id}','${type}','${url.replace(/'/g, "\\'")}')" `
    : (isArtist || isLabel)
      ? `class="${typeClass}" href="#" data-entity-type="${escHtml(type)}" data-entity-name="${escHtml(title)}" data-entity-id="${item.id}" onclick="searchByEntity(event,this)"`
      : `class="${typeClass}" href="${url}" target="_blank" rel="noopener"`;

  // Collection / wantlist badges
  let badges = "";
  const releaseId = item.id;
  if (releaseId) {
    if (window._collectionIds?.has(releaseId)) badges += `<span class="collection-badge" title="In your collection">✓</span>`;
    if (window._wantlistIds?.has(releaseId))   badges += `<span class="wantlist-badge" title="In your wantlist">♡</span>`;
  }

  const thumbWrap = `<div class="card-thumb-wrap">${thumb}${badges ? `<div class="card-thumb-badges">${badges}</div>` : ""}</div>`;

  return `
    <a ${cardAttrs}>
      ${thumbWrap}
      <div class="card-body">
        ${artist ? `<div class="card-artist">${escHtml(artist)}</div>` : ""}
        <div class="card-title">${escHtml(title)}</div>
        ${label   ? `<div class="card-sub">${escHtml(label)}</div>` : ""}
        ${formats ? `<div class="card-format">${escHtml(formats)}</div>` : ""}
        ${genre   ? `<div class="card-format">${escHtml(genre)}</div>`   : ""}
        ${catno   ? `<div class="card-catno-line">${escHtml(catno)}</div>` : ""}
        <div class="card-meta">${metaParts.map(escHtml).join(" · ")}</div>
      </div>
    </a>`;
}

// ── Collection / Wantlist tab state ───────────────────────────────────────
let _activeTab = "search"; // "search" | "collection" | "wantlist"
let _colPage = 1, _wlPage = 1;

function renderCardFromBasicInfo(basicInfo) {
  // Map Discogs basic_information format to what renderCard expects
  const artistName = (basicInfo.artists ?? []).map(a => a.name).join(", ");
  const labelStr   = (basicInfo.labels  ?? []).map(l => l.name).join(", ");
  const formatStr  = (basicInfo.formats ?? []).map(f => f.name + (f.descriptions?.length ? ` (${f.descriptions.join(", ")})` : "")).join(" · ");
  const genreStr   = (basicInfo.genres  ?? [])[0] ?? "";

  const syntheticItem = {
    id:           basicInfo.id,
    type:         "release",
    title:        artistName ? `${artistName} - ${basicInfo.title}` : basicInfo.title,
    cover_image:  basicInfo.cover_image || basicInfo.thumb || "",
    label:        (basicInfo.labels ?? []).map(l => l.name),
    format:       (basicInfo.formats ?? []).map(f => f.name),
    genre:        basicInfo.genres ?? [],
    year:         String(basicInfo.year ?? ""),
    country:      "",
    uri:          basicInfo.id ? `/release/${basicInfo.id}` : "",
  };
  return renderCard(syntheticItem);
}

function addNavTab(view) {
  // Tabs are always in the DOM; this just enables a greyed-out one
  const btn = document.querySelector(`#main-nav-tabs [data-view="${view}"]`);
  if (btn) { btn.classList.remove("nav-disabled"); btn.removeAttribute("title"); }
}

function toggleMobileNav() {
  document.getElementById("main-nav-tabs")?.classList.toggle("mobile-open");
}

// Close hamburger menu when clicking outside
document.addEventListener("click", e => {
  if (!e.target.closest("#main-nav-tabs") && !e.target.closest("#nav-hamburger")) {
    document.getElementById("main-nav-tabs")?.classList.remove("mobile-open");
  }
});

function switchView(view, skipPushState = false) {
  // Close mobile nav if open
  document.getElementById("main-nav-tabs")?.classList.remove("mobile-open");
  // Block disabled tabs
  const tabBtn = document.querySelector(`#main-nav-tabs [data-view="${view}"]`);
  if (tabBtn?.classList.contains("nav-disabled")) return;

  document.querySelectorAll(".main-nav-tab").forEach(btn =>
    btn.classList.toggle("active", btn.dataset.view === view)
  );
  const searchView = document.getElementById("search-view");
  const dropsView  = document.getElementById("drops-view");
  const infoView   = document.getElementById("info-view");
  if (!skipPushState) {
    if (view === "drops" || view === "collection" || view === "wantlist" || view === "info" || view === "wanted") {
      history.pushState({ view }, "", "?view=" + view);
    } else {
      history.pushState({}, "", location.pathname);
    }
  }
  if (typeof gtag === "function") {
    const titles = { drops: "Drops", info: "Info", collection: "Collection", wantlist: "Wantlist", wanted: "Wanted", search: "Search" };
    gtag("event", "page_view", {
      page_location: window.location.href,
      page_path:     window.location.pathname + window.location.search,
      page_title:    "SeaDisco – " + (titles[view] ?? view)
    });
  }
  // Hide all views first
  if (searchView) searchView.style.display = "none";
  if (dropsView)  dropsView.style.display  = "none";
  if (infoView)   infoView.style.display   = "none";

  // Toggle main search form vs collection/wantlist search bar
  const mainForm    = document.getElementById("main-search-form");
  const cwWrap      = document.getElementById("cw-search-wrap");
  const cwInput     = document.getElementById("cw-query");
  const wantedWrap  = document.getElementById("wanted-search-wrap");

  if (view === "drops") {
    if (dropsView) dropsView.style.display = "block";
    if (mainForm) mainForm.style.display = "";
    if (cwWrap) cwWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
  } else if (view === "info") {
    if (infoView) infoView.style.display = "block";
    if (mainForm) mainForm.style.display = "";
    if (cwWrap) cwWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
  } else if (view === "wanted") {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "none";
    if (cwWrap) cwWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "";
    document.getElementById("artist-alts").innerHTML = "";
    const feed = document.getElementById("recent-feed"); if (feed) feed.style.display = "none";
    loadWantedTab();
  } else if (view === "collection") {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "none";
    if (cwWrap) cwWrap.style.display = "";
    if (wantedWrap) wantedWrap.style.display = "none";
    if (cwInput) { cwInput.placeholder = "Search your collection…"; cwInput.value = ""; }
    clearCwFilters();
    _cwTab = "collection"; _cwQuery = "";
    document.getElementById("artist-alts").innerHTML = "";
    const feed = document.getElementById("recent-feed"); if (feed) feed.style.display = "none";
    loadCwFacets("collection");
    loadCollectionTab(1);
  } else if (view === "wantlist") {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "none";
    if (cwWrap) cwWrap.style.display = "";
    if (wantedWrap) wantedWrap.style.display = "none";
    if (cwInput) { cwInput.placeholder = "Search your wantlist…"; cwInput.value = ""; }
    clearCwFilters();
    _cwTab = "wantlist"; _cwQuery = "";
    document.getElementById("artist-alts").innerHTML = "";
    const feed = document.getElementById("recent-feed"); if (feed) feed.style.display = "none";
    loadCwFacets("wantlist");
    loadWantlistTab(1);
  } else {
    if (searchView) searchView.style.display = "";
    if (mainForm) mainForm.style.display = "";
    if (cwWrap) cwWrap.style.display = "none";
    if (wantedWrap) wantedWrap.style.display = "none";
    document.getElementById("results").innerHTML = "";
    document.getElementById("pagination").style.display = "none";
    setStatus("");
    document.getElementById("blurb").style.display = "none";
    const feed = document.getElementById("recent-feed"); if (feed) feed.style.display = "";
  }
}

// ── Collection / Wantlist local search ──
let _cwTab = "collection"; // which tab is active for CW search
let _cwQuery = "";         // current CW search query
let _cwAdvOpen = false;

function toggleCwAdvanced(forceOpen) {
  const panel = document.getElementById("cw-advanced-panel");
  const arrow = document.getElementById("cw-advanced-arrow");
  if (!panel) return;
  _cwAdvOpen = forceOpen === true ? true : forceOpen === false ? false : !_cwAdvOpen;
  panel.style.display = _cwAdvOpen ? "" : "none";
  if (arrow) arrow.textContent = _cwAdvOpen ? "▼" : "▶";
}

function getCwFilters() {
  const f = {};
  const q       = (document.getElementById("cw-query")?.value   ?? "").trim();
  const artist  = (document.getElementById("cw-artist")?.value  ?? "").trim();
  const release = (document.getElementById("cw-release")?.value ?? "").trim();
  const label   = (document.getElementById("cw-label")?.value   ?? "").trim();
  const year    = (document.getElementById("cw-year")?.value    ?? "").trim();
  const genre   = (document.getElementById("cw-genre")?.value   ?? "").trim();
  const style   = (document.getElementById("cw-style")?.value   ?? "").trim();
  const format  = (document.getElementById("cw-format")?.value  ?? "").trim();
  if (q)       f.q       = q;
  if (artist)  f.artist  = artist;
  if (release) f.release = release;
  if (label)   f.label   = label;
  if (year)    f.year    = year;
  if (genre)   f.genre   = genre;
  if (style)   f.style   = style;
  if (format)  f.format  = format;
  return f;
}

function doCwSearch(page = 1) {
  const filters = getCwFilters();
  _cwQuery = filters.q || "";
  if (_cwTab === "collection") {
    loadCollectionTab(page, filters);
  } else {
    loadWantlistTab(page, filters);
  }
}

function clearCwSearch() {
  document.getElementById("cw-query").value   = "";
  document.getElementById("cw-artist").value  = "";
  document.getElementById("cw-release").value = "";
  document.getElementById("cw-label").value   = "";
  document.getElementById("cw-year").value    = "";
  document.getElementById("cw-genre").value   = "";
  document.getElementById("cw-style").value   = "";
  document.getElementById("cw-format").value  = "";
  _cwQuery = "";
  doCwSearch(1);
}

async function loadCwFacets(type, genre) {
  try {
    let url = `/api/user/facets?type=${type}`;
    if (genre) url += `&genre=${encodeURIComponent(genre)}`;
    const r = await apiFetch(url);
    const data = await r.json();
    const genreEl = document.getElementById("cw-genre");
    const styleEl = document.getElementById("cw-style");
    // Only refresh genres on initial load (no genre filter), not when filtering styles
    if (!genre && genreEl) {
      genreEl.innerHTML = '<option value="">Any</option>' +
        (data.genres ?? []).map(g => `<option value="${g}">${g}</option>`).join("");
    }
    if (styleEl) {
      const prev = styleEl.value;
      styleEl.innerHTML = '<option value="">Any</option>' +
        (data.styles ?? []).map(s => `<option value="${s}">${s}</option>`).join("");
      // Restore previous selection if still available
      if (prev && [...styleEl.options].some(o => o.value === prev)) styleEl.value = prev;
    }
  } catch {}
}

function onCwGenreChange() {
  const genre = document.getElementById("cw-genre")?.value || "";
  // Reset style when genre changes
  document.getElementById("cw-style").value = "";
  loadCwFacets(_cwTab, genre || undefined);
  doCwSearch(1);
}

function clearCwFilters() {
  ["cw-query","cw-artist","cw-release","cw-label","cw-year","cw-genre","cw-style","cw-format"].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = "";
  });
  toggleCwAdvanced(false);
}

function setActiveTab(tab) {
  _activeTab = tab;
}

async function loadCollectionTab(page = 1, filters) {
  _colPage = page;
  const f = filters || getCwFilters();
  setActiveTab("collection");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  setStatus("Loading collection…");
  try {
    let url = `/api/user/collection?page=${page}&per_page=24`;
    if (f.q)       url += `&q=${encodeURIComponent(f.q)}`;
    if (f.artist)  url += `&artist=${encodeURIComponent(f.artist)}`;
    if (f.release) url += `&release=${encodeURIComponent(f.release)}`;
    if (f.label)   url += `&label=${encodeURIComponent(f.label)}`;
    if (f.year)    url += `&year=${encodeURIComponent(f.year)}`;
    if (f.genre)   url += `&genre=${encodeURIComponent(f.genre)}`;
    if (f.style)   url += `&style=${encodeURIComponent(f.style)}`;
    if (f.format)  url += `&format=${encodeURIComponent(f.format)}`;
    const r = await apiFetch(url);
    const data = await r.json();
    const items = data.items ?? [];
    const hasFilter = Object.keys(f).length > 0;
    const filterDesc = Object.values(f).join(" + ");
    if (!items.length) {
      setStatus(hasFilter ? `No collection items matching "${filterDesc}".` : "No collection items synced yet. Click 'Sync now' to fetch from Discogs.");
      return;
    }
    const prefix = hasFilter ? `${data.total} results for "${filterDesc}"` : `${data.total} items in collection`;
    setStatus(`${prefix} — page ${page} of ${data.pages}`);
    document.getElementById("results").innerHTML = items.map(renderCardFromBasicInfo).join("");
    totalPages = data.pages;
    currentPage = page;
    renderCollectionPagination("collection");
  } catch (e) {
    setStatus("Failed to load collection: " + e.message, true);
  }
}

async function loadWantlistTab(page = 1, filters) {
  _wlPage = page;
  const f = filters || getCwFilters();
  setActiveTab("wantlist");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  setStatus("Loading wantlist…");
  try {
    let url = `/api/user/wantlist?page=${page}&per_page=24`;
    if (f.q)       url += `&q=${encodeURIComponent(f.q)}`;
    if (f.artist)  url += `&artist=${encodeURIComponent(f.artist)}`;
    if (f.release) url += `&release=${encodeURIComponent(f.release)}`;
    if (f.label)   url += `&label=${encodeURIComponent(f.label)}`;
    if (f.year)    url += `&year=${encodeURIComponent(f.year)}`;
    if (f.genre)   url += `&genre=${encodeURIComponent(f.genre)}`;
    if (f.style)   url += `&style=${encodeURIComponent(f.style)}`;
    if (f.format)  url += `&format=${encodeURIComponent(f.format)}`;
    const r = await apiFetch(url);
    const data = await r.json();
    const items = data.items ?? [];
    const hasFilter = Object.keys(f).length > 0;
    const filterDesc = Object.values(f).join(" + ");
    if (!items.length) {
      setStatus(hasFilter ? `No wantlist items matching "${filterDesc}".` : "No wantlist items synced yet. Click 'Sync now' to fetch from Discogs.");
      return;
    }
    const prefix = hasFilter ? `${data.total} results for "${filterDesc}"` : `${data.total} items in wantlist`;
    setStatus(`${prefix} — page ${page} of ${data.pages}`);
    document.getElementById("results").innerHTML = items.map(renderCardFromBasicInfo).join("");
    totalPages = data.pages;
    currentPage = page;
    renderCollectionPagination("wantlist");
  } catch (e) {
    setStatus("Failed to load wantlist: " + e.message, true);
  }
}

// ── Community Wanted ──
let _wantedItems = null; // cached after first load

async function loadWantedTab() {
  setActiveTab("wanted");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  if (_wantedItems) { renderWantedItems(_wantedItems); return; }
  setStatus("Loading wanted items…");
  try {
    const r = await apiFetch("/api/wanted");
    const data = await r.json();
    _wantedItems = data.items ?? [];
    renderWantedItems(_wantedItems);
  } catch (e) {
    setStatus("Failed to load wanted items: " + e.message, true);
  }
}

function filterWantedItems() {
  if (!_wantedItems) return;
  const q = (document.getElementById("wanted-q")?.value ?? "").trim().toLowerCase();
  if (!q) { renderWantedItems(_wantedItems); return; }
  const filtered = _wantedItems.filter(item => {
    const artist = (item.artists ?? []).map(a => a.name).join(" ").toLowerCase();
    const title  = (item.title  ?? "").toLowerCase();
    const label  = (item.labels ?? []).map(l => l.name).join(" ").toLowerCase();
    const genre  = (item.genres ?? []).join(" ").toLowerCase();
    const style  = (item.styles ?? []).join(" ").toLowerCase();
    const year   = String(item.year ?? "");
    return `${artist} ${title} ${label} ${genre} ${style} ${year}`.includes(q);
  });
  renderWantedItems(filtered);
}

function renderWantedItems(items) {
  if (!items.length) {
    setStatus("No wanted items found.");
    document.getElementById("results").innerHTML = "";
    return;
  }
  const q = (document.getElementById("wanted-q")?.value ?? "").trim();
  setStatus(q ? `${items.length} wanted items matching "${q}"` : `${items.length} random community wantlist items`);
  document.getElementById("results").innerHTML = items.map(item => renderCardFromBasicInfo(item)).join("");
}

function renderCollectionPagination(tab) {
  if (totalPages <= 1) return;
  const pag = document.getElementById("pagination");
  pag.style.display = "flex";
  document.getElementById("page-info").textContent = `${currentPage} / ${totalPages}`;
  document.getElementById("prev-btn").disabled = currentPage <= 1;
  document.getElementById("next-btn").disabled = currentPage >= totalPages;
  document.getElementById("prev-btn").onclick = currentPage > 1
    ? () => { window.scrollTo({top:0,behavior:'smooth'}); tab === "collection" ? loadCollectionTab(currentPage - 1) : loadWantlistTab(currentPage - 1); }
    : null;
  document.getElementById("next-btn").onclick = currentPage < totalPages
    ? () => { window.scrollTo({top:0,behavior:'smooth'}); tab === "collection" ? loadCollectionTab(currentPage + 1) : loadWantlistTab(currentPage + 1); }
    : null;
}

async function showSyncStatus(type) {
  const el = document.getElementById("sync-status");
  if (!el) return;
  try {
    const r = await apiFetch("/api/user/collection?page=1&per_page=1");
    // We just need to display last synced time; fetch it from sync endpoint status
  } catch {}
  el.innerHTML = `<a href="#" onclick="triggerSync('${type}');return false;" style="color:var(--accent);text-decoration:none">Sync now</a>`;
}

let _mainSyncPoll = null;

async function triggerSync(type = "both") {
  const el = document.getElementById("sync-status");
  if (el) el.textContent = "Syncing in background…";
  try {
    const r = await apiFetch("/api/user/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type }),
    });
    const data = await r.json();
    if (data.skipped) {
      if (el) el.innerHTML = `Recently synced &nbsp;·&nbsp; <a href="#" onclick="triggerSync('${type}');return false;" style="color:var(--accent);text-decoration:none">Sync now</a>`;
      return;
    }
    // Poll for progress
    if (_mainSyncPoll) clearInterval(_mainSyncPoll);
    _mainSyncPoll = setInterval(async () => {
      try {
        const sr = await apiFetch("/api/user/sync-status");
        const sd = await sr.json();
        if (sd.syncStatus === "syncing") {
          const pct = sd.syncTotal ? Math.round((sd.syncProgress / sd.syncTotal) * 100) : 0;
          if (el) el.textContent = `Syncing… ${sd.syncProgress.toLocaleString()} / ${sd.syncTotal.toLocaleString()} (${pct}%)`;
        } else {
          clearInterval(_mainSyncPoll); _mainSyncPoll = null;
          if (el) el.innerHTML = `Sync complete &nbsp;·&nbsp; <a href="#" onclick="triggerSync('${type}');return false;" style="color:var(--accent);text-decoration:none">Sync now</a>`;
          await loadDiscogsIds();
          if (_activeTab === "collection") loadCollectionTab(1);
          else if (_activeTab === "wantlist") loadWantlistTab(1);
        }
      } catch { clearInterval(_mainSyncPoll); _mainSyncPoll = null; }
    }, 4000);
  } catch (e) {
    if (el) el.textContent = "Sync failed: " + e.message;
  }
}

async function loadDiscogsIds() {
  try {
    const r = await apiFetch("/api/user/discogs-ids");
    if (!r.ok) return;
    const data = await r.json();
    window._collectionIds = new Set(data.collectionIds ?? []);
    window._wantlistIds   = new Set(data.wantlistIds   ?? []);
    // Enable "Hide owned" checkbox for any logged-in user with a token
    const cb = document.getElementById("hide-owned");
    const lbl = document.getElementById("hide-owned-label");
    if (cb && cb.disabled) {
      cb.disabled = false; cb.style.opacity = "1"; cb.style.cursor = "pointer";
      cb.addEventListener("change", () => { if (window._lastResults) renderResults(window._lastResults); });
    }
    if (lbl) {
      lbl.style.color = "#aaa"; lbl.style.cursor = "pointer";
      lbl.title = window._collectionIds.size > 0 ? "Hide releases already in your collection" : "Sync your collection on the Account page to use this filter";
    }
  } catch { /* ignore */ }
}

// ── Pagination ────────────────────────────────────────────────────────────
function renderPagination() {
  if (totalPages <= 1) return;
  document.getElementById("pagination").style.display = "flex";
  document.getElementById("page-info").textContent = `${currentPage} / ${totalPages}`;
  document.getElementById("prev-btn").disabled = currentPage <= 1;
  document.getElementById("next-btn").disabled = currentPage >= totalPages;
}

// ── Modal ─────────────────────────────────────────────────────────────────
function openModal(event, id, type, discogsUrl) {
  if (event) event.preventDefault();
  const u = new URL(window.location.href);
  u.searchParams.set("op", `${type}:${id}`);
  history.replaceState({}, "", u.toString());
  const overlay = document.getElementById("modal-overlay");
  document.getElementById("album-info").innerHTML = "";
  document.getElementById("modal-loading").style.display = "block";
  overlay.classList.add("open");

  const cachedItem = itemCache.get(String(id)) ?? { type };
  const endpoint = type === "master" ? "master" : "release";
  Promise.all([
    apiFetch(`${API}/${endpoint}/${id}`).then(r => r.json()),
    apiFetch(`${API}/marketplace-stats/${id}?type=${type}`).then(r => r.json()).catch(() => null),
  ])
    .then(([d, stats]) => {
      document.getElementById("modal-loading").style.display = "none";
      renderAlbumInfo(d, cachedItem, discogsUrl, stats);
    })
    .catch(() => {
      document.getElementById("modal-loading").textContent = "Failed to load.";
    });
}

function closeModal() {
  document.getElementById("modal-overlay").classList.remove("open");
  const u = new URL(window.location.href);
  u.searchParams.delete("op");
  history.replaceState({}, "", u.toString());
}

// ── Image lightbox / carousel ─────────────────────────────────────────────
let _lbImages = [], _lbIdx = 0;

function openLightbox(images, startIdx) {
  _lbImages = images;
  _lbIdx = startIdx ?? 0;
  _renderLightbox();
  document.getElementById("lightbox-overlay").classList.add("open");
  document.addEventListener("keydown", _lbKey);
}

function closeLightbox() {
  document.getElementById("lightbox-overlay").classList.remove("open");
  document.removeEventListener("keydown", _lbKey);
}

function lightboxStep(e, dir) {
  e.stopPropagation();
  _lbIdx = Math.max(0, Math.min(_lbImages.length - 1, _lbIdx + dir));
  _renderLightbox();
}

function _lbKey(e) {
  if (e.key === "ArrowLeft")  { _lbIdx = Math.max(0, _lbIdx - 1); _renderLightbox(); }
  if (e.key === "ArrowRight") { _lbIdx = Math.min(_lbImages.length - 1, _lbIdx + 1); _renderLightbox(); }
  if (e.key === "Escape")     closeLightbox();
}

function _renderLightbox() {
  document.getElementById("lightbox-img").src = _lbImages[_lbIdx] ?? "";
  document.getElementById("lightbox-counter").textContent =
    _lbImages.length > 1 ? `${_lbIdx + 1} / ${_lbImages.length}` : "";
  document.getElementById("lightbox-prev").disabled = _lbIdx === 0;
  document.getElementById("lightbox-next").disabled = _lbIdx === _lbImages.length - 1;
  const single = _lbImages.length <= 1;
  document.getElementById("lightbox-prev").style.display = single ? "none" : "";
  document.getElementById("lightbox-next").style.display = single ? "none" : "";
}

document.getElementById("modal-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("modal-overlay")) closeModal();
});

// ── Version popup (layered over master popup) ─────────────────────────────
async function openVersionPopup(event, releaseId) {
  if (event) event.preventDefault();
  const overlay = document.getElementById("version-overlay");
  const info    = document.getElementById("version-info");
  const loading = document.getElementById("version-loading");
  info.innerHTML = "";
  loading.style.display = "block";
  overlay.classList.add("open");
  const u = new URL(window.location.href);
  u.searchParams.set("vr", releaseId);
  history.replaceState({}, "", u.toString());
  try {
    const discogsUrl = `https://www.discogs.com/release/${releaseId}`;
    const [d, stats] = await Promise.all([
      apiFetch(`${API}/release/${releaseId}`).then(r => r.json()),
      apiFetch(`${API}/marketplace-stats/${releaseId}?type=release`).then(r => r.json()).catch(() => null),
    ]);
    loading.style.display = "none";
    const fakeResult = { type: "release", id: releaseId, title: d.title ?? "", cover_image: d.images?.[0]?.uri ?? "", format: [], country: d.country ?? "", year: d.year ?? "" };
    renderAlbumInfo(d, fakeResult, discogsUrl, stats, "version-info");
  } catch(e) {
    loading.style.display = "none";
    info.innerHTML = `<div style="padding:1rem;color:var(--muted)">Failed to load release details.</div>`;
  }
}

function closeVersionPopup() {
  document.getElementById("version-overlay").classList.remove("open");
  const u = new URL(window.location.href);
  u.searchParams.delete("vr");
  history.replaceState({}, "", u.toString());
}

document.getElementById("version-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("version-overlay")) closeVersionPopup();
});

// ── Bio full popup ────────────────────────────────────────────────────────
function openBioFull(event) {
  if (event) event.preventDefault();
  const u = new URL(window.location.href);
  u.searchParams.set("bi", "1");
  history.replaceState({}, "", u.toString());
  const {
    name, text, discogsId = null,
    members = [], groups = [], aliases = [],
    namevariations = [], urls = [],
    parentLabel = null, sublabels = [],
    alternatives = [],
  } = window._currentBio ?? {};
  document.getElementById("bio-full-name").textContent = name ?? "";
  let html = renderBioMarkup(text ?? "");
  // Show alternatives ("Also:") in popup
  if (alternatives.length > 0) {
    const altLinks = alternatives.map(a =>
      `<a href="#" class="bio-artist-link" onclick="selectAltArtist(event,this);closeBioFull()" data-alt-name="${escHtml(a.name)}"${a.id ? ` data-alt-id="${a.id}"` : ""} style="color:var(--accent);text-decoration:none">${escHtml(a.name)}</a>`
    ).join('<span style="color:#555;margin:0 0.3em">·</span>');
    html += `<div style="font-size:0.78rem;margin-top:0.7rem;line-height:1.6"><span style="color:#777;margin-right:0.4em">Also:</span>${altLinks}</div>`;
  }
  const relLinks = renderArtistRelations(members, groups, aliases, namevariations, urls, parentLabel, sublabels);
  if (relLinks) html += relLinks;
  if (discogsId) {
    html += `<div style="margin-top:1.1rem"><a href="https://www.discogs.com/artist/${discogsId}" target="_blank" rel="noopener" style="font-size:0.75rem;color:#666;text-decoration:none">View profile on Discogs ↗</a></div>`;
  }
  document.getElementById("bio-full-text").innerHTML = html;
  document.getElementById("bio-full-overlay").classList.add("open");
}

async function doAiSearch(q) {
  if (!q) { setStatus("Enter a question or description to search with AI.", false); return; }
  switchView("search", true);
  setActiveTab("search");
  const blurbEl = document.getElementById("blurb");
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  document.getElementById("artist-alts").innerHTML = "";
  blurbEl.style.display = "none";
  setStatus("Asking Claude…");
  document.getElementById("search-btn").disabled = true;
  try {
    const r = await apiFetch("/api/ai-search", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ q }) });
    if (r.status === 401) { setStatus("Sign in and add your Discogs token to use AI search.", false); return; }
    if (!r.ok) { setStatus("AI search failed. Try again.", false); return; }
    const { recommendations, blurb } = await r.json();
    if (!recommendations?.length) { setStatus("No recommendations returned.", false); return; }
    setStatus("");

    blurbEl.innerHTML = `
      <div style="margin-bottom:1rem">
        <div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:0.1em;color:#666;margin-bottom:0.4rem">✦ AI Recommendations for "${escHtml(q)}"</div>
        ${blurb ? `<div style="font-size:0.85rem;color:var(--muted);font-style:italic;margin-bottom:0.75rem">${escHtml(blurb)}</div>` : ""}
        ${recommendations.map(rec => {
          const params = new URLSearchParams();
          const p = rec.discogsParams ?? {};
          // For artist-type recs use ar= directly; for releases use q= or re=
          if (rec.type === "artist" && p.artist) {
            params.set("ar", p.artist);
          } else if (rec.type === "release" && p.artist) {
            params.set("ar", p.artist);
            if (p.q) params.set("re", p.q);
          } else {
            if (p.q)      params.set("q",  p.q);
            if (p.artist) params.set("ar", p.artist);
          }
          if (p.label)  params.set("lb", p.label);
          if (p.genre)  params.set("gn", p.genre);
          if (p.style)  params.set("st", p.style);
          const yr = p.year ? String(p.year).split(/[-–]/)[0].trim() : "";
          // Note: AI recs use their own discogsParams format (artist/label/genre/style/year)
          if (/^\d{4}$/.test(yr)) params.set("yr", yr);
          const href = "/?" + params.toString();
          return `<div style="padding:0.75rem 0;border-bottom:1px solid #222">
            <div style="display:flex;justify-content:space-between;align-items:baseline;gap:1rem">
              <span style="color:var(--fg);font-weight:600">${rec.name}</span>
              <a href="${href}" style="font-size:0.78rem;color:var(--accent);white-space:nowrap;flex-shrink:0">New Search →</a>
            </div>
            <div style="font-size:0.83rem;color:var(--muted);margin-top:0.25rem">${rec.description}</div>
          </div>`;
        }).join("")}
      </div>`;
    blurbEl.style.display = "block";

    // GA4: track AI search
    if (typeof gtag === "function") {
      gtag("event", "page_view", {
        page_location: window.location.href,
        page_title: document.title
      });
      gtag("event", "ai_search", { search_term: q });
    }
  } catch (err) {
    setStatus("AI search error: " + err.message, false);
  } finally {
    document.getElementById("search-btn").disabled = false;
  }
}

function closeBioFull() {
  document.getElementById("bio-full-overlay").classList.remove("open");
  const u = new URL(window.location.href);
  u.searchParams.delete("bi");
  history.replaceState({}, "", u.toString());
}

document.getElementById("bio-full-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("bio-full-overlay")) closeBioFull();
});

// ── Video popup ────────────────────────────────────────────────────────────
let ytPlayer = null;
let _ytLoading = false;
window.onYouTubeIframeAPIReady = function() { window._ytAPIReady = true; };

function ensureYTAPI() {
  if (window._ytAPIReady || _ytLoading) return;
  _ytLoading = true;
  const s = document.createElement("script");
  s.src = "https://www.youtube.com/iframe_api";
  document.head.appendChild(s);
}

function setVideoUrl(id) {
  const u = new URL(window.location.href);
  u.searchParams.set("vd", id);
  history.replaceState({}, "", u.toString());
}

function loadYTVideo(id) {
  if (ytPlayer && typeof ytPlayer.loadVideoById === "function") {
    ytPlayer.loadVideoById(id);
    return;
  }
  if (window._ytAPIReady && typeof YT !== "undefined") {
    document.getElementById("video-player").innerHTML = "";
    ytPlayer = new YT.Player("video-player", {
      height: "100%", width: "100%", videoId: id,
      playerVars: { autoplay: 1, rel: 0 },
      events: { onStateChange: function(e) { if (e.data === 0) playNextVideo(); } }
    });
  } else {
    document.getElementById("video-player").innerHTML =
      `<iframe src="https://www.youtube.com/embed/${id}?autoplay=1&enablejsapi=1" style="width:100%;height:100%;border:none" allow="autoplay;encrypted-media" allowfullscreen></iframe>`;
  }
}

function updateVideoNavButtons() {
  const queue = window._videoQueue ?? [];
  const idx   = window._videoQueueIndex ?? 0;
  const prevBtn  = document.getElementById("video-prev");
  const nextBtn  = document.getElementById("video-next");
  const titleEl  = document.getElementById("video-title");
  if (prevBtn) prevBtn.disabled = idx <= 0;
  if (nextBtn) nextBtn.disabled = idx >= queue.length - 1;
  if (titleEl) {
    const meta = (window._videoQueueMeta ?? [])[idx];
    if (meta) {
      const parts = [];
      if (meta.track)  parts.push(`<span class="vt-track">${escHtml(meta.track)}</span>`);
      if (meta.album)  parts.push(`<span>${escHtml(meta.album)}</span>`);
      if (meta.artist) parts.push(`<span>${escHtml(meta.artist)}</span>`);
      titleEl.innerHTML = parts.join(`<span class="vt-sep">·</span>`);
    } else {
      titleEl.innerHTML = "";
    }
  }
}

function openVideo(event, url) {
  if (event) { event.preventDefault(); event.stopPropagation(); }
  ensureYTAPI();
  const id = extractYouTubeId(url);
  if (!id) { window.open(url, "_blank", "noopener"); return; }
  const trackLinks = [...document.querySelectorAll(".track-link[data-video]")];
  window._videoQueue      = trackLinks.map(a => a.dataset.video);
  window._videoQueueMeta  = trackLinks.map(a => ({
    track:  a.dataset.track  || "",
    album:  a.dataset.album  || "",
    artist: a.dataset.artist || "",
  }));
  window._videoQueueIndex = window._videoQueue.indexOf(url);
  if (window._videoQueueIndex === -1) window._videoQueueIndex = 0;
  setVideoUrl(id);
  document.getElementById("video-overlay").classList.add("open");
  loadYTVideo(id);
  updateVideoNavButtons();
}

function videoPrev() {
  const queue = window._videoQueue ?? [];
  let prev = (window._videoQueueIndex ?? 0) - 1;
  while (prev >= 0) {
    const id = extractYouTubeId(queue[prev]);
    if (id) {
      window._videoQueueIndex = prev;
      setVideoUrl(id);
      loadYTVideo(id);
      updateVideoNavButtons();
      return;
    }
    prev--;
  }
}

function playNextVideo() {
  const queue = window._videoQueue ?? [];
  let next = (window._videoQueueIndex ?? -1) + 1;
  while (next < queue.length) {
    const id = extractYouTubeId(queue[next]);
    if (id) {
      window._videoQueueIndex = next;
      setVideoUrl(id);
      loadYTVideo(id);
      updateVideoNavButtons();
      return;
    }
    next++;
  }
}

function closeVideo() {
  document.getElementById("video-overlay").classList.remove("open");
  if (ytPlayer && typeof ytPlayer.stopVideo === "function") {
    ytPlayer.stopVideo();
    ytPlayer.destroy();
    ytPlayer = null;
  }
  document.getElementById("video-player").innerHTML = "";
  const u = new URL(window.location.href);
  u.searchParams.delete("vd");
  history.replaceState({}, "", u.toString());
}

function extractYouTubeId(url) {
  try {
    const u = new URL(url);
    if (u.hostname === "youtu.be") return u.pathname.slice(1);
    return u.searchParams.get("v");
  } catch { return null; }
}

document.getElementById("video-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("video-overlay")) closeVideo();
});

document.addEventListener("keydown", e => {
  if (e.key === "Escape") {
    closeVideo();
    closeModal();
    closeBioFull();
  }
});

// ── Album info panel ──────────────────────────────────────────────────────
function renderAlbumInfo(d, searchResult, discogsUrl = "", stats = null, targetId = "album-info") {
  const el = document.getElementById(targetId);

  const rawTitle = d.title ?? searchResult.title ?? "";
  const title    = rawTitle.includes(" - ") && !d.title
                   ? rawTitle.slice(rawTitle.indexOf(" - ") + 3) : rawTitle;
  const artistNames = (d.artists ?? []).map(a => a.name);
  const artists  = artistNames.length ? artistNames
                   : ((searchResult.title ?? "").split(" - ")[0] ? [(searchResult.title ?? "").split(" - ")[0]] : []);
  const year     = d.year ?? searchResult.year ?? "";
  const labels   = (d.labels ?? []).map(l => l.name).slice(0, 2).join(", ")
                   || (searchResult.label ?? []).slice(0, 2).join(", ");
  const genres   = [...(d.genres ?? []), ...(d.styles ?? [])].slice(0, 4).join(" · ");
  const country  = d.country ?? searchResult.country ?? "";
  const allImages = (d.images ?? []).map(i => i.uri).filter(Boolean);
  if (allImages.length === 0 && searchResult.cover_image) allImages.push(searchResult.cover_image);
  const img      = allImages[0] ?? "";
  const released    = d.released_formatted ?? d.released ?? "";
  const formats     = (d.formats ?? []).map(f =>
    [f.name, ...(f.descriptions ?? [])].filter(Boolean).join(" · ")
  ).join("; ") || (searchResult.format ?? []).join(" · ");
  const credits     = (d.extraartists ?? [])
    .map(a => {
      const nameEl = a.id
        ? `<a href="#" data-alt-name="${escHtml(a.name)}" data-alt-id="${a.id}" onclick="selectAltArtist(event,this);closeModal()" style="color:var(--accent);text-decoration:none">${escHtml(a.name)}</a>`
        : escHtml(a.name);
      return `${nameEl}${a.role ? ` <span class="credit-role">(${escHtml(a.role)})</span>` : ""}`;
    })
    .join(" · ");
  const notes       = d.notes ? stripDiscogsMarkup(d.notes) : "";
  const catno     = (d.labels ?? [])[0]?.catno ?? "";
  const releaseId = d.id ?? searchResult.id ?? "";
  const typeLabel = targetId === "version-info"
    ? `Version: ${releaseId}`
    : searchResult.type === "master" ? "Master Release" : searchResult.type === "release" ? "Release" : "";

  const videoMap = new Map();
  for (const v of (d.videos ?? [])) {
    if (v.title && v.uri) videoMap.set(v.title.toLowerCase(), v.uri);
  }
  function findVideo(trackTitle) {
    const tl = trackTitle.toLowerCase();
    for (const [vt, uri] of videoMap) {
      if (vt.includes(tl) || tl.includes(vt)) return uri;
    }
    return null;
  }

  const identifierTypes = ["Barcode","Matrix / Runout","ASIN","Catalog Number","Label Code"];
  const identifierGroups = {};
  for (const i of (d.identifiers ?? [])) {
    if (i.value && identifierTypes.includes(i.type)) {
      identifierGroups[i.type] = identifierGroups[i.type]
        ? identifierGroups[i.type] + ", " + i.value
        : i.value;
    }
  }
  const identifierRows = identifierTypes
    .filter(t => identifierGroups[t])
    .map(t => `<span class="detail-label">${escHtml(t)}</span><span>${escHtml(identifierGroups[t])}</span>`)
    .join("");

  // Companies — group by entity type, show vinyl-relevant ones
  const companyTypes = ["Pressed By","Manufactured By","Mastered At","Recorded At","Mixed At","Distributed By","Licensed From","Phonographic Copyright (p)"];
  const companyGroups = {};
  for (const c of (d.companies ?? [])) {
    const t = c.entity_type_name;
    if (companyTypes.includes(t)) {
      if (!companyGroups[t]) companyGroups[t] = [];
      companyGroups[t].push(c.name);
    }
  }
  const companyRows = companyTypes
    .filter(t => companyGroups[t])
    .map(t => `<span class="detail-label">${escHtml(t)}</span><span>${escHtml(companyGroups[t].join(", "))}</span>`)
    .join("");

  const seriesText = (d.series ?? [])
    .map(s => s.catno ? `${s.name} (${s.catno})` : s.name)
    .filter(Boolean).join(", ");

  const isMaster = searchResult.type === "master";
  const detailRows = [
    labels  ? `<span class="detail-label">Label</span><span>${escHtml(labels)}</span>`   : "",
    (!isMaster && formats) ? `<span class="detail-label">Format</span><span>${escHtml(formats)}</span>` : "",
    country ? `<span class="detail-label">Country</span><span>${escHtml(country)}</span>` : "",
    genres  ? `<span class="detail-label">Genre</span><span>${escHtml(genres)}</span>`   : "",
    (!isMaster && catno) ? `<span class="detail-label">Cat#</span><span>${escHtml(catno)}</span>` : "",
    year    ? `<span class="detail-label">Year</span><span>${escHtml(String(year))}</span>` : "",
    seriesText ? `<span class="detail-label">Series</span><span>${escHtml(seriesText)}</span>` : "",
    companyRows,
    identifierRows,
  ].filter(Boolean).join("");

  const tracks = (d.tracklist ?? []).filter(t => t.type_ !== "heading");
  const trackHTML = tracks.length ? `
    <div class="album-tracklist">
      <div class="tracklist-heading">Tracklist</div>
      ${tracks.map(t => {
        const url = findVideo(t.title || "");
        const trackArtist = artists.length ? artists[0] : "";
        const ytQuery = encodeURIComponent(`${trackArtist} ${title} ${t.title || ""}`);
        const ytIcon = `<a class="yt-search" href="https://www.youtube.com/results?search_query=${ytQuery}" target="_blank" rel="noopener" title="Search on YouTube"><svg width="16" height="11" viewBox="0 0 16 11" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="16" height="11" rx="2.5" fill="#FF0000"/><path d="M6.5 3L11 5.5L6.5 8V3Z" fill="white"/></svg></a>`;
        const trackSearchQ = ('"' + trackArtist + ' ' + (t.title || '').trim() + '"').replace(/'/g, "\\'");
        const searchIcon = t.title ? ` <a class="track-search-icon" href="#" onclick="event.preventDefault();closeModal();document.getElementById('query').value='${escHtml(trackSearchQ)}';toggleAdvanced(false);document.querySelector('input[name=\\'result-type\\'][value=\\'\\']').checked=true;doSearch(1)" title="Search for other versions" style="text-decoration:none;color:var(--accent);font-size:0.85em">⌕</a>` : "";
        const titleEl = url
          ? `<a class="track-link" href="#" data-video="${escHtml(url)}" data-track="${escHtml(t.title || "")}" data-album="${escHtml(title)}" data-artist="${escHtml(trackArtist)}" onclick="openVideo(event,'${url.replace(/'/g, "\\'")}')">${escHtml(t.title || "")} ▶</a>${searchIcon}`
          : `${escHtml(t.title || "")}${ytIcon}${searchIcon}`;
        return `<div class="track">
          <span class="track-pos">${escHtml(t.position || "")}</span>
          <span class="track-title">${titleEl}</span>
          ${t.duration ? `<span class="track-dur">${escHtml(t.duration)}</span>` : ""}
        </div>`;
      }).join("")}
    </div>` : "";

  const metaRows = [
    released ? `<div class="album-meta-row"><span class="meta-label">Released</span><span>${escHtml(released)}</span></div>` : "",
    (!isMaster && formats) ? `<div class="album-meta-row"><span class="meta-label">Format</span><span>${escHtml(formats)}</span></div>` : "",
    credits  ? `<div class="album-meta-row"><span class="meta-label">Credits</span><span>${credits}</span></div>` : "",
    notes    ? `<div class="album-notes"><div class="tracklist-heading" style="margin-top:0.5rem">Notes</div>${escHtml(notes)}</div>` : "",
  ].filter(Boolean).join("");

  el.innerHTML = `
    <div class="album-header">
      ${img ? `<img class="album-cover" src="${img}" alt="${escHtml(title)}" loading="lazy"
               onclick="openLightbox(${escHtml(JSON.stringify(allImages))},0)"
               title="${allImages.length > 1 ? `View ${allImages.length} photos` : 'View photo'}" />`
             : `<div class="album-cover-placeholder">♪</div>`}
      <div class="album-meta">
        ${typeLabel ? `<div class="album-type-badge">${escHtml(typeLabel)}</div>` : ""}
        <h2>${escHtml(title)} <a href="#" class="album-title-search" onclick="event.preventDefault();closeModal();document.getElementById('query').value='${escHtml(title.replace(/'/g, "\\'"))}';toggleAdvanced(false);document.querySelector('input[name=\\'result-type\\'][value=\\'\\']').checked=true;doSearch(1)" title="Search for other versions">⌕</a>${window._collectionIds?.has(Number(releaseId)) ? ` <span class="collection-badge" title="In your collection">✓</span>` : ""}${window._wantlistIds?.has(Number(releaseId)) ? ` <span class="wantlist-badge" title="In your wantlist">♡</span>` : ""}</h2>
        ${artists.length ? `<div class="album-artist">${artists.map(n => `<a href="#" class="modal-artist-link" data-artist="${escHtml(n)}" onclick="searchArtistFromModal(event,this)">${escHtml(n)}</a>`).join(", ")}</div>` : ""}
        ${detailRows ? `<div class="album-detail-grid">${detailRows}</div>` : ""}
        ${(() => {
          const r = d.community?.rating;
          const have = d.community?.have;
          const want = d.community?.want;
          const parts = [];
          if (r?.count > 0) parts.push(`★ ${parseFloat(r.average).toFixed(2)} <span style="color:#555">(${r.count.toLocaleString()} ratings)</span>`);
          if (have || want) parts.push(`${(have ?? 0).toLocaleString()} have · ${(want ?? 0).toLocaleString()} want`);
          return parts.length ? `<div style="font-size:0.72rem;color:#888;margin-top:0.35rem">${parts.join('<span style="color:#444;margin:0 0.35em">·</span>')}</div>` : "";
        })()}
        ${discogsUrl ? `<a href="${discogsUrl}" target="_blank" rel="noopener" style="font-size:0.75rem;color:var(--accent);text-decoration:none;margin-top:0.5rem;display:inline-block">View on Discogs ↗</a>` : ""}
        ${stats?.numForSale > 0 && stats?.lowestPrice != null
          ? `<a href="https://www.discogs.com/sell/list?release_id=${escHtml(String(stats.releaseId))}" target="_blank" rel="noopener" style="font-size:0.75rem;color:#888;text-decoration:none;margin-top:0.2rem;display:block">${escHtml(String(stats.numForSale))} available from $${parseFloat(stats.lowestPrice).toFixed(2)}</a>`
          : (stats?.numForSale === 0 ? `<div style="font-size:0.75rem;color:#555;margin-top:0.2rem">Not currently available on Discogs marketplace</div>` : "")
        }
      </div>
    </div>
    ${trackHTML}
    ${metaRows ? `<div class="album-extra">${metaRows}</div>` : ""}
    ${isMaster ? `<div id="master-versions-list" style="padding:0.75rem 1rem 0.5rem;font-size:0.78rem;color:var(--muted)">Loading pressings…</div>` : ""}`;

  if (isMaster) loadMasterVersions(null, searchResult.id);
}

let _masterVersions = [];

function renderMasterVersions(filter) {
  const list = document.getElementById("master-versions-list");
  if (!list) return;
  const MEDIA = new Set(["Vinyl","CD","Cassette","DVD","Blu-ray","File","Box Set","Lathe Cut","Flexi-disc","Shellac","8-Track Cartridge","Reel-To-Reel","MiniDisc","SACD","Betamax","VHS"]);
  const getMedium = v => {
    const parts = (v.format ?? "").split(",").map(s => s.trim());
    return parts.find(p => MEDIA.has(p)) || (v.majorFormats ?? []).find(f => MEDIA.has(f)) || parts[0] || "";
  };
  const getDisplayFormat = v => {
    const fmt = (v.format ?? "").trim();
    const medium = (v.majorFormats ?? []).find(f => MEDIA.has(f));
    if (!fmt) return medium || "—";
    // If the format string already contains the medium type, show as-is
    if (!medium || fmt.split(",").map(s => s.trim()).includes(medium)) return fmt;
    // Otherwise prepend the medium so it's always visible
    return `${medium}, ${fmt}`;
  };
  const filtered = filter ? _masterVersions.filter(v => getMedium(v) === filter) : _masterVersions;

  // Update pill active states
  list.querySelectorAll(".mv-filter-pill").forEach(p => {
    p.style.background = p.dataset.filter === (filter ?? "") ? "var(--accent)" : "#2a2a2a";
    p.style.color      = p.dataset.filter === (filter ?? "") ? "#000" : "var(--fg)";
  });

  const grid = list.querySelector(".mv-grid");
  if (!grid) return;
  if (!filtered.length) { grid.innerHTML = `<span style="color:var(--muted);grid-column:1/-1">No pressings match this filter.</span>`; return; }
  grid.innerHTML = filtered.map(v => {
    const inCol  = window._collectionIds?.has(v.id);
    const inWant = window._wantlistIds?.has(v.id);
    const badge  = inCol  ? `<span class="collection-badge">✓</span>` :
                   inWant ? `<span class="wantlist-badge">♡</span>` : "";
    return `
      <span style="color:#888">${escHtml(!v.year || v.year === "0" ? "?" : String(v.year))}</span>
      <span style="color:#aaa">${escHtml(v.country || "?")}</span>
      <span style="color:#888">${escHtml(getDisplayFormat(v))}</span>
      <span style="color:#aaa">${escHtml(v.catno ?? "—")}</span>
      <span><a href="#" onclick="openVersionPopup(event,${v.id})" style="color:var(--accent);text-decoration:none">${escHtml(v.label ?? v.title ?? "—")}</a>${badge}</span>`;
  }).join("");
}

async function loadMasterVersions(event, masterId) {
  if (event) event.preventDefault();
  const list = document.getElementById("master-versions-list");
  if (!list) return;
  try {
    const data = await apiFetch(`${API}/master-versions/${masterId}`).then(r => r.json());
    _masterVersions = data.versions ?? [];
    if (!_masterVersions.length) { list.textContent = "No pressings found."; return; }

    // Build unique medium types for filter pills
    // Discogs format strings look like "CD, Compilation" or "Vinyl, LP, Album"
    // We want the medium type (Vinyl/CD/Cassette/etc.) not qualifiers
    const MEDIA2 = new Set(["Vinyl","CD","Cassette","DVD","Blu-ray","File","Box Set","Lathe Cut","Flexi-disc","Shellac","8-Track Cartridge","Reel-To-Reel","MiniDisc","SACD","Betamax","VHS"]);
    const getMedium2 = v => { const parts = (v.format ?? "").split(",").map(s => s.trim()); return parts.find(p => MEDIA2.has(p)) || (v.majorFormats ?? []).find(f => MEDIA2.has(f)) || parts[0] || ""; };
    const formatSet = new Set();
    _masterVersions.forEach(v => {
      const medium = getMedium2(v);
      if (medium) formatSet.add(medium);
    });
    const formats = [...formatSet].sort();
    const pillStyle = `cursor:pointer;border:none;border-radius:20px;padding:0.15rem 0.6rem;font-size:0.72rem;font-weight:600;transition:background 0.15s`;
    const pills = [
      `<button class="mv-filter-pill" data-filter="" onclick="renderMasterVersions('')" style="${pillStyle};background:var(--accent);color:#000">All</button>`,
      ...formats.map(f => `<button class="mv-filter-pill" data-filter="${escHtml(f)}" onclick="renderMasterVersions('${f.replace(/'/g,"\\'")}')\" style="${pillStyle};background:#2a2a2a;color:var(--fg)">${escHtml(f)}</button>`)
    ].join("");

    list.innerHTML = `
      <div style="font-size:0.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.4rem">Pressings / Versions</div>
      ${formats.length > 1 ? `<div style="display:flex;flex-wrap:wrap;gap:0.35rem;margin-bottom:0.6rem">${pills}</div>` : ""}
      <div class="mv-grid" style="display:grid;grid-template-columns:auto auto auto auto 1fr;gap:0.2rem 0.7rem;font-size:0.75rem"></div>`;
    renderMasterVersions("");
  } catch(e) {
    list.textContent = "Failed to load pressings.";
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────
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

function setStatus(msg, isError = false) {
  const el = document.getElementById("status");
  el.textContent = msg;
  el.className = isError ? "error" : "";
  el.style.display = msg ? "block" : "none";
}

// Store overflow items for rel-popup; keyed by auto-incrementing id
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
  // "Also known as" uses clickable links (search as artist), skipped in compact (bio card) mode
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

function renderBioMarkup(text) {
  if (!text) return '';
  // First flatten [l=Name] tags to plain text (label names, no link needed)
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

function searchBioArtist(event, el) {
  event.preventDefault();
  closeBioFull();
  switchView("search", true);
  document.getElementById("f-artist").value  = el.dataset.artist;
  document.getElementById("query").value     = "";
  document.getElementById("f-release").value = "";
  document.getElementById("f-year").value    = "";
  document.getElementById("f-label").value   = "";
  document.getElementById("f-genre").value   = "";
  currentArtistId = el.dataset.artistId || null;
  // Ensure advanced panel is open so doSearch reads the artist field
  toggleAdvanced(true);
  doSearch(1);
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

function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ── Alt artist link → new search ─────────────────────────────────────────
function searchArtistFromModal(event, el) {
  event.preventDefault();
  closeModal();
  document.getElementById("f-artist").value  = el.dataset.artist;
  document.getElementById("query").value     = "";
  document.getElementById("f-release").value = "";
  document.getElementById("f-year").value    = "";
  document.getElementById("f-label").value   = "";
  document.getElementById("f-genre").value   = "";
  toggleAdvanced(true);
  doSearch(1);
}

function selectAltArtist(event, el) {
  event.preventDefault();
  document.getElementById("f-artist").value = el.dataset.altName;
  document.getElementById("query").value = "";
  currentArtistId = el.dataset.altId || null;
  toggleAdvanced(true);
  doSearch(1);
}

function openAltsPopup(event) {
  event.preventDefault();
  event.stopPropagation();
  document.getElementById("alts-popup").classList.add("open");
  document.getElementById("alts-popup-backdrop").classList.add("open");
}

function closeAltsPopup() {
  document.getElementById("alts-popup").classList.remove("open");
  document.getElementById("alts-popup-backdrop").classList.remove("open");
}

// ── Entity card click → new search ───────────────────────────────────────
function searchByEntity(event, el) {
  event.preventDefault();
  switchView("search", true);
  const type = el.dataset.entityType;
  const name = el.dataset.entityName;
  document.getElementById("query").value = "";
  document.getElementById("f-artist").value  = "";
  document.getElementById("f-release").value = "";
  document.getElementById("f-year").value    = "";
  document.getElementById("f-label").value   = "";
  document.getElementById("f-genre").value   = "";
  if (type === "artist") {
    document.getElementById("f-artist").value = name;
    currentArtistId = el.dataset.entityId || null;
  }
  if (type === "label")  document.getElementById("f-label").value  = name;
  // Must open advanced panel so doSearch reads artist/label fields
  toggleAdvanced(true);
  doSearch(1);
}

// ── Browser back / forward ───────────────────────────────────────────────
window.addEventListener("popstate", () => {
  const p = new URLSearchParams(location.search);
  const view = p.get("view");
  if (view === "drops" || view === "collection" || view === "wantlist" || view === "info" || view === "wanted") {
    switchView(view, true);
  } else {
    switchView("search", true);
    restoreFromParams(p);
    if (p.toString()) {
      doSearch(parseInt(p.get("pg") ?? "1"), true);
    } else {
      document.getElementById("results").innerHTML = "";
      document.getElementById("blurb").style.display = "none";
      document.getElementById("artist-alts").innerHTML = "";
      document.getElementById("status").textContent = "";
      document.getElementById("pagination").style.display = "none";
      document.getElementById("search-desc").textContent = "";
      document.getElementById("search-returned").textContent = "";
      document.getElementById("search-ai-summary").textContent = "";
      document.getElementById("search-info-block").style.display = "none";
    }
  }
});

// ── Recent searches feed ─────────────────────────────────────────────────
function toTitleCase(s) {
  return s.replace(/\w\S*/g, w => w.charAt(0).toUpperCase() + w.slice(1));
}
// Normalize old full-name param keys to single letters (handles old DB entries)
function normP(p) {
  const m = { artist:"a", release_title:"r", label:"l", year:"y", genre:"g", style:"s", format:"f", type:"t", sort:"o" };
  const o = {};
  for (const [k, v] of Object.entries(p)) { if (v) o[m[k] ?? k] = v; }
  return o;
}

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

let _recentSearches = [];

async function loadRecentFeed() {
  try {
    const data = await fetch("/api/recent-searches").then(r => r.json());
    const searches = data.searches ?? [];
    const el = document.getElementById("recent-feed");
    if (!el) return;
    if (!searches.length) { el.style.display = "none"; return; }
    // Filter out searches with no meaningful label (would show as generic "Search")
    const filtered = searches.filter(s => {
      const { full } = feedLabel(s.params);
      return full !== "Search";
    });
    if (!filtered.length) { el.style.display = "none"; return; }
    _recentSearches = filtered;
    const pillsHtml = `<div class="feed-label">Recent Searches</div><div class="feed-pills">${
      filtered.map((s, i) => {
        const { full, short } = feedLabel(s.params);
        return `<span class="feed-pill" data-idx="${i}" title="${escHtml(full)}">${escHtml(short)}</span>`;
      }).join("")
    }</div>`;
    el.style.opacity = "0";
    el.innerHTML = pillsHtml;
    el.querySelectorAll(".feed-pill").forEach((pill, i) => {
      pill.addEventListener("click", () => feedApply(filtered[i].params));
    });
    requestAnimationFrame(() => { el.style.opacity = "1"; });
  } catch {
    const el = document.getElementById("recent-feed");
    if (el) el.style.display = "none";
  }
}

function renderFreshGrid(releases) {
  const grid = document.getElementById("fresh-releases-grid");
  if (!grid) return;
  if (!releases.length) {
    grid.innerHTML = `<div style="color:var(--muted);font-size:0.8rem;grid-column:1/-1;text-align:center;padding:2rem 0">No releases found for this tag.</div>`;
    return;
  }
  grid.innerHTML = releases.map(rel => {
    const img = rel.cover_url
      ? `<img src="${escHtml(rel.cover_url)}" alt="${escHtml(rel.release_name ?? '')}" loading="lazy" onerror="this.style.display='none'">`
      : `<div class="fresh-card-no-img">♪</div>`;

    const types = [rel.primary_type, rel.secondary_type].filter(Boolean).join(" · ");

    // Slice to YYYY-MM-DD string (handles both Date objects and strings from PG)
    const dateStr = rel.release_date ? String(rel.release_date).slice(0, 10) : "";
    const date = dateStr
      ? new Date(dateStr + "T12:00:00").toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" })
      : "";

    const googleQ   = [rel.artist_credit_name, rel.release_name, "new release"].filter(Boolean).join(" ");
    const googleUrl = `https://www.google.com/search?q=${encodeURIComponent(googleQ)}`;

    return `<div class="fresh-card">
      <div class="fresh-card-img">${img}</div>
      <div class="fresh-card-body">
        <div class="fresh-card-title">${escHtml(rel.release_name ?? "Unknown")}</div>
        <div class="fresh-card-artist">${escHtml(rel.artist_credit_name ?? "")}</div>
        ${date ? `<div class="fresh-card-date">${date}</div>` : ""}
        ${types ? `<div class="fresh-card-type">${escHtml(types)}</div>` : ""}
        <div class="fresh-card-links">
          <a href="${googleUrl}" target="_blank" rel="noopener">Google →</a>
        </div>
      </div>
    </div>`;
  }).join("");
}

let _freshActiveTag = "";

async function filterFreshByTag(tag) {
  const pills = document.querySelectorAll(".fresh-tag-pill");
  pills.forEach(p => p.classList.toggle("active", p.dataset.tag === tag));
  _freshActiveTag = tag;
  const url = tag ? `/api/fresh-releases?tag=${encodeURIComponent(tag)}` : "/api/fresh-releases";
  try {
    const data = await fetch(url).then(r => r.json());
    renderFreshGrid(data.releases ?? []);
  } catch { /* ignore */ }
}

async function loadFreshReleases() {
  try {
    const data = await fetch("/api/fresh-releases").then(r => r.json());
    const releases = data.releases ?? [];
    const topTags  = data.topTags  ?? [];
    if (!releases.length && !topTags.length) return;
    const tagCloud = document.getElementById("fresh-tag-cloud");
    if (tagCloud && topTags.length) {
      tagCloud.innerHTML =
        `<span class="fresh-tag-pill active" data-tag="" onclick="filterFreshByTag('')">All</span>` +
        topTags.map(t =>
          `<span class="fresh-tag-pill" data-tag="${escHtml(t.tag)}" onclick="filterFreshByTag('${escHtml(t.tag)}')">${escHtml(t.tag)}</span>`
        ).join("");
    }
    renderFreshGrid(releases);
  } catch { /* fresh releases unavailable */ }
}

// Auth-ready promise — declared here so the page-load IIFE below can await it
// before initAuth (which resolves it) is defined further down the file.
let _authReady;
const authReadyPromise = new Promise(res => { _authReady = res; });

// ── Restore from URL on page load ────────────────────────────────────────
(async function () {
  const p = new URLSearchParams(location.search);
  const view = p.get("view");
  if (view === "drops" || view === "info") {
    switchView(view, true);
  } else if (view === "collection" || view === "wantlist" || view === "wanted") {
    await authReadyPromise;
    switchView(view, true);
  } else if (p.toString()) {
    restoreFromParams(p);
    await authReadyPromise;
    doSearch(parseInt(p.get("pg") ?? "1"), true);
  }
  // Defer non-critical feeds until browser is idle
  const deferLoad = (fn) => typeof requestIdleCallback === "function" ? requestIdleCallback(fn) : setTimeout(fn, 200);
  deferLoad(() => loadRecentFeed());
  deferLoad(() => loadFreshReleases());
})();

// Submit on Enter (all text inputs)
["query", "f-artist", "f-release", "f-year", "f-label"].forEach(id => {
  document.getElementById(id).addEventListener("keydown", e => {
    if (e.key === "Enter") doSearch(1);
  });
});

// Grey out advanced toggle when AI mode is selected
document.querySelectorAll('input[name="result-type"]').forEach(radio => {
  radio.addEventListener("change", () => {
    const isAi = document.querySelector('input[name="result-type"]:checked')?.value === "ai";
    if (isAi) {
      // Close panel and grey out the toggle button
      const panel = document.getElementById("advanced-panel");
      const arrow = document.getElementById("advanced-arrow");
      panel.dataset.open = "false";
      arrow.textContent = "▶";
    }
    const toggleBtn = document.getElementById("advanced-toggle");
    if (toggleBtn) {
      toggleBtn.style.opacity  = isAi ? "0.35" : "";
      toggleBtn.style.cursor   = isAi ? "default" : "";
    }
  });
});

// ── Clerk auth init ───────────────────────────────────────────────────────
(async function initAuth() {
  try {
    const cfg = await fetch("/api/config").then(r => r.json()).catch(() => ({}));
    const pk = cfg.clerkPublishableKey;
    if (!pk) { _authReady(); return; }

    const frontendApi = atob(pk.replace(/^pk_(test|live)_/, "")).replace(/\$$/, "");
    await new Promise((resolve, reject) => {
      const s = document.createElement("script");
      s.src = `https://${frontendApi}/npm/@clerk/clerk-js@latest/dist/clerk.browser.js`;
      s.setAttribute("data-clerk-publishable-key", pk);
      s.setAttribute("crossorigin", "anonymous");
      s.onload = resolve; s.onerror = reject;
      document.head.appendChild(s);
    });

    await new Promise(r => setTimeout(r, 50));
    window._clerk = window.Clerk;
    if (!window._clerk) { _authReady(); return; }
    await window._clerk.load();

    const navBtn = document.getElementById("nav-auth-btn");
    if (navBtn) {
      if (window._clerk.user) {
        navBtn.textContent = "Account";
        navBtn.classList.remove("nav-signup-btn");
        const popup = document.getElementById("nav-auth-popup");
        if (popup) popup.remove();
      } else {
        navBtn.textContent = "Sign Up";
        navBtn.classList.add("nav-signup-btn");
      }
    }

    // If user is signed in, unlock Wanted tab (community feature, no token needed)
    if (window._clerk.user) {
      addNavTab("wanted");
      try {
        const tokenCheck = await apiFetch("/api/user/token");
        if (tokenCheck.ok) {
          const tokenData = await tokenCheck.json();
          if (tokenData.hasToken) {
            addNavTab("collection");
            addNavTab("wantlist");
            await loadDiscogsIds();
          }
        }
      } catch { /* collection tabs optional */ }
    }
  } catch { /* auth unavailable — site works fine without it */ }
  finally { _authReady(); }
})();
