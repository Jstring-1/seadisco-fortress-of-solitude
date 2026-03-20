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
  document.getElementById("type-desc").textContent = "";
  document.getElementById("type-pipe").style.display = "none";
  document.getElementById("sort-desc").textContent = "";
  document.getElementById("sort-pipe").style.display = "none";
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

  if (!q && !artist && !release && !year && !label && !genre) {
    setStatus("Enter a search term or fill in at least one filter.", false);
    return;
  }

  // Switch back to search tab if on collection/wantlist tab
  if (_activeTab !== "search") {
    setActiveTab("search");
  }

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
    const descText = parts.length ? "Search :: " + parts.join(", ") : "";
    
    document.getElementById("search-desc").textContent = descText;
    const typeLabels = { "master":"Masters", "release":"Releases", "artist":"Artists", "label":"Labels" };
    const typeLabel = typeLabels[resultType] ?? "";
    document.getElementById("type-desc").textContent = typeLabel ? `Results: ${typeLabel}` : "";
    document.getElementById("type-pipe").style.display = (descText && typeLabel) ? "inline" : "none";
    const sortLabels = { "year:asc":"Year ↑", "year:desc":"Year ↓", "title:asc":"Title A→Z", "title:desc":"Title Z→A", "label:asc":"Label A→Z" };
    const sortLabel = sortLabels[sort] ?? "";
    document.getElementById("sort-desc").textContent = sortLabel ? `Sort: ${sortLabel}` : "";
    document.getElementById("sort-pipe").style.display = (descText && sortLabel) ? "inline" : "none";
  }

  const isYearSort  = sort.startsWith("year:");
  const skipSort    = resultType === "artist" || resultType === "label";
  const dualFetch   = isYearSort && !skipSort && resultType === "";

  const baseParams = (typeOverride, perPage) => {
    // For pages 2+, use the auto-detected artist (saved from page 1) for consistent Discogs pagination
    const effectiveArtist = artist || (page > 1 ? detectedArtist : null) || "";
    const p = new URLSearchParams({ q: q || effectiveArtist || label || release, page, per_page: perPage });
    const t = typeOverride ?? resultType;
    if (t) p.set("type", t);
    if (effectiveArtist) p.set("artist", effectiveArtist);
    if (release) p.set("release_title", release);
    if (year)    p.set("year",          year);
    if (label)   p.set("label",         label);
    if (genre)   p.set("genre",         genre);
    if (style)   p.set("style",         style);
    if (format)  p.set("format",        format);
    if (sort && !skipSort) {
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
        bioFetch = fetch(bioUrl);
      } else if (label) {
        bioFetch = apiFetch(`${API}/label-bio?name=${encodeURIComponent(label)}`);
      } else if (genre) {
        bioFetch = apiFetch(`${API}/genre-info?genre=${encodeURIComponent(genre)}`);
      }
    }

    let items, totalPages_new;
    if (dualFetch) {
      const [masterRes, releaseRes, bioRes2] = await Promise.all([
        apiFetch(`${API}/search?${baseParams("master",  18)}`),
        apiFetch(`${API}/search?${baseParams("release",  6)}`),
        bioFetch ?? Promise.resolve(null),
      ]);
      bioFetch = bioRes2 ? { json: () => bioRes2.json() } : null;
      const [md, rd] = await Promise.all([masterRes.json(), releaseRes.json()]);
      const merged = [...(md.results ?? []), ...(rd.results ?? [])];
      const asc = sort === "year:asc";
      merged.sort((a, b) => {
        const ya = parseInt(a.year ?? "0") || 0;
        const yb = parseInt(b.year ?? "0") || 0;
        return asc ? ya - yb : yb - ya;
      });
      items = merged;
      totalPages_new = Math.max(md.pagination?.pages ?? 1, rd.pagination?.pages ?? 1);
    } else if (artistRaw && resultType === "label") {
      // Special case: artist + Labels radio → show labels that artist recorded on
      const rp = new URLSearchParams({ q: q || artist, artist, type: "master", per_page: 50, page: 1 });
      const [relRes, bioRes] = await Promise.all([
        apiFetch(`${API}/search?${rp}`),
        bioFetch ?? Promise.resolve(null),
      ]);
      bioFetch = bioRes ? { json: () => bioRes.json() } : null;
      const relData = await relRes.json();
      const labelNames = [...new Set(
        (relData.results ?? []).flatMap(r => r.label ?? []).filter(Boolean)
      )].slice(0, 12);
      const labelCards = await Promise.all(
        labelNames.map(name =>
          apiFetch(`${API}/search?q=${encodeURIComponent(name)}&type=label&per_page=1`)
            .then(r => r.json()).then(d => d.results?.[0]).catch(() => null)
        )
      );
      items = labelCards.filter(Boolean);
      totalPages_new = 1;
    } else {
      const [res, bioRes] = await Promise.all([
        apiFetch(`${API}/search?${baseParams(null, 24)}`),
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

      // If no results but a format filter is active, retry without format so the artist's
      // releases still appear (e.g. artist has no vinyl but does have CDs/masters)
      if (items.length === 0 && format && (artist || q)) {
        const fallbackP = baseParams(null, 48);
        fallbackP.delete("format");
        const fallbackRes = await apiFetch(`${API}/search?${fallbackP}`);
        if (fallbackRes.ok) {
          const fd = await fallbackRes.json();
          if ((fd.results ?? []).length > 0) {
            items = fd.results;
            totalPages_new = fd.pagination?.pages ?? 1;
            setStatus(`No ${format} releases found — showing all formats.`);
          }
        }
      }
    }
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
      const altsEl = document.getElementById("artist-alts");
      const alts = (bioData?.alternatives ?? []).filter(a => a.name);
      if (alts.length > 0) {
        const altLink = a => `<a href="#" data-alt-name="${escHtml(a.name)}"${a.id ? ` data-alt-id="${a.id}"` : ""} onclick="selectAltArtist(event,this)" style="color:#666;text-decoration:none;border-bottom:1px dotted #444">${escHtml(a.name)}</a>`;
        const shown  = alts.slice(0, 3);
        const hidden = alts.slice(3);
        const popupEl = document.getElementById("alts-popup");
        popupEl.innerHTML = `<h4>Other artists</h4>` +
          alts.map(a => `<a href="#" data-alt-name="${escHtml(a.name)}"${a.id ? ` data-alt-id="${a.id}"` : ""} onclick="selectAltArtist(event,this);closeAltsPopup()">${escHtml(a.name)}</a>`).join("");
        const moreBtn = hidden.length > 0
          ? ` <a href="#" onclick="openAltsPopup(event)" style="color:#666;font-size:0.75rem;text-decoration:none;white-space:nowrap">▾ ${hidden.length} more</a>`
          : "";
        altsEl.innerHTML = `<div style="font-size:0.78rem;color:#555;margin-bottom:0.6rem">Also: ${shown.map(altLink).join(' &middot; ')}${moreBtn}</div>`;
      } else {
        altsEl.innerHTML = "";
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
        detectedArtist = constrainedArtist; // saved so pages 2+ use it directly in baseParams
        try {
          const cp = new URLSearchParams({ q: q || constrainedArtist, page, per_page: 48, artist: constrainedArtist });
          if (resultType) cp.set("type", resultType);
          if (release)    cp.set("release_title", release);
          if (year)       cp.set("year", year);
          if (format)     cp.set("format", format);
          if (sort && !skipSort) { const [sf, so] = sort.split(":"); cp.set("sort", sf); cp.set("sort_order", so); }
          const cr = await apiFetch(`${API}/search?${cp}`);
          if (cr.ok) {
            const cd = await cr.json();
            if ((cd.results ?? []).length > 0) {
              items = cd.results;
              totalPages = cd.pagination?.pages ?? totalPages;
            }
          }
        } catch { /* keep original results */ }
      }

      if (bioData?.profile) {
        const entityType = artist ? 'artist' : label ? 'label' : genre ? 'genre' : 'artist';
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
        };

        const rawBioText  = bioData.profile ?? null;
        const displayText = rawBioText ? stripDiscogsMarkup(rawBioText) : "";
        const TRUNCATE = 600;
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

        let discogsHref = "";
        if (entityType === 'artist') discogsHref = `https://www.discogs.com/search/?q=${encodeURIComponent(bioData.name)}&type=artist`;
        else if (entityType === 'label') discogsHref = `https://www.discogs.com/search/?q=${encodeURIComponent(bioData.name)}&type=label`;
        const discogsLink = discogsHref
          ? `<div style="margin-top:0.3rem"><a href="${discogsHref}" target="_blank" rel="noopener" style="font-size:0.75rem;color:#666;text-decoration:none">View on Discogs ↗</a></div>`
          : "";

        const relLinks = renderArtistRelations(
          bioData.members        ?? [], bioData.groups    ?? [], bioData.aliases  ?? [],
          bioData.namevariations ?? [], bioData.urls      ?? [],
          bioData.parentLabel    ?? null, bioData.sublabels ?? [], true
        );
        const bioHtml  = rawBioText ? renderBioMarkup(truncatedRaw) : escHtml(truncatedRaw);
        blurbEl.innerHTML = heading + bioHtml + readMore + relLinks + discogsLink;
        blurbEl.style.display = "block";
      }
    }

    if (!items.length) { setStatus("No results found."); return; }

    const countMsg = `${items.length} results — page ${currentPage} of ${totalPages}`;
    setStatus(countMsg);
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
        const statusEl = document.getElementById("status");
        if (statusEl) statusEl.textContent = `${countMsg} · …`;
        fetch("/api/result-quality", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query: qualityQuery, titles: qualityTitles }),
        }).then(r => r.json()).then(d => {
          const el = document.getElementById("status");
          if (el) el.textContent = d.phrase ? `${countMsg} · ${d.phrase}` : countMsg;
        }).catch(() => {
          const el = document.getElementById("status");
          if (el) el.textContent = countMsg;
        });
      }
    }

    // GA4: track search as virtual page view + search event
    if (typeof gtag === "function") {
      gtag("event", "page_view", {
        page_location: window.location.href,
        page_title: document.title
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
  const grid = document.getElementById("results");
  grid.innerHTML = items.map(item => renderCard(item)).join("");
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
        ${label   ? `<div class="card-sub">${escHtml(label)}</div>`   : ""}
        ${formats ? `<div class="card-format">${escHtml(formats)}</div>` : ""}
        ${genre   ? `<div class="card-format">${escHtml(genre)}</div>`   : ""}
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

function addNavTab(view, label) {
  const container = document.getElementById("main-nav-tabs");
  if (!container || container.querySelector(`[data-view="${view}"]`)) return;
  const btn = document.createElement("button");
  btn.className = "main-nav-tab";
  btn.dataset.view = view;
  btn.textContent = label;
  btn.onclick = () => switchView(view);
  container.appendChild(btn);
}

function switchView(view) {
  document.querySelectorAll(".main-nav-tab").forEach(btn =>
    btn.classList.toggle("active", btn.dataset.view === view)
  );
  const searchView = document.getElementById("search-view");
  const dropsView  = document.getElementById("drops-view");
  if (view === "drops") {
    if (searchView) searchView.style.display = "none";
    if (dropsView)  dropsView.style.display  = "block";
  } else {
    if (searchView) searchView.style.display = "";
    if (dropsView)  dropsView.style.display  = "none";
    if (view === "collection") {
      loadCollectionTab(1);
    } else if (view === "wantlist") {
      loadWantlistTab(1);
    } else {
      // Search — clear any collection/wantlist content
      document.getElementById("results").innerHTML = "";
      document.getElementById("pagination").style.display = "none";
      setStatus("");
      document.getElementById("blurb").style.display = "none";
    }
  }
}

function setActiveTab(tab) {
  _activeTab = tab;
}

async function loadCollectionTab(page = 1) {
  _colPage = page;
  setActiveTab("collection");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  setStatus("Loading collection…");
  try {
    const r = await apiFetch(`/api/user/collection?page=${page}&per_page=24`);
    const data = await r.json();
    const items = data.items ?? [];
    if (!items.length) {
      setStatus("No collection items synced yet. Click 'Sync now' to fetch from Discogs.");
      return;
    }
    setStatus(`${data.total} items in collection — page ${page} of ${data.pages}`);
    document.getElementById("results").innerHTML = items.map(renderCardFromBasicInfo).join("");
    totalPages = data.pages;
    currentPage = page;
    renderCollectionPagination("collection");
  } catch (e) {
    setStatus("Failed to load collection: " + e.message, true);
  }
}

async function loadWantlistTab(page = 1) {
  _wlPage = page;
  setActiveTab("wantlist");
  document.getElementById("blurb").style.display = "none";
  document.getElementById("results").innerHTML = "";
  document.getElementById("pagination").style.display = "none";
  setStatus("Loading wantlist…");
  try {
    const r = await apiFetch(`/api/user/wantlist?page=${page}&per_page=24`);
    const data = await r.json();
    const items = data.items ?? [];
    if (!items.length) {
      setStatus("No wantlist items synced yet. Click 'Sync now' to fetch from Discogs.");
      return;
    }
    setStatus(`${data.total} items in wantlist — page ${page} of ${data.pages}`);
    document.getElementById("results").innerHTML = items.map(renderCardFromBasicInfo).join("");
    totalPages = data.pages;
    currentPage = page;
    renderCollectionPagination("wantlist");
  } catch (e) {
    setStatus("Failed to load wantlist: " + e.message, true);
  }
}

function renderCollectionPagination(tab) {
  if (totalPages <= 1) return;
  const prevFn = tab === "collection" ? `loadCollectionTab(${currentPage - 1})` : `loadWantlistTab(${currentPage - 1})`;
  const nextFn = tab === "collection" ? `loadCollectionTab(${currentPage + 1})` : `loadWantlistTab(${currentPage + 1})`;
  const pag = document.getElementById("pagination");
  pag.style.display = "flex";
  document.getElementById("page-info").textContent = `${currentPage} / ${totalPages}`;
  document.getElementById("prev-btn").disabled = currentPage <= 1;
  document.getElementById("next-btn").disabled = currentPage >= totalPages;
  document.getElementById("prev-btn").onclick = currentPage > 1
    ? () => { tab === "collection" ? loadCollectionTab(currentPage - 1) : loadWantlistTab(currentPage - 1); }
    : null;
  document.getElementById("next-btn").onclick = currentPage < totalPages
    ? () => { tab === "collection" ? loadCollectionTab(currentPage + 1) : loadWantlistTab(currentPage + 1); }
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

async function triggerSync(type = "both") {
  const el = document.getElementById("sync-status");
  if (el) el.textContent = "Syncing…";
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
    // Reload badge IDs after sync
    await loadDiscogsIds();
    if (_activeTab === "collection") loadCollectionTab(1);
    else if (_activeTab === "wantlist") loadWantlistTab(1);
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
  } = window._currentBio ?? {};
  document.getElementById("bio-full-name").textContent = name ?? "";
  let html = renderBioMarkup(text ?? "");
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
    const { recommendations } = await r.json();
    if (!recommendations?.length) { setStatus("No recommendations returned.", false); return; }
    setStatus("");

    blurbEl.innerHTML = `
      <div style="margin-bottom:1rem">
        <div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:0.1em;color:#666;margin-bottom:0.5rem">✦ AI Recommendations for "${q}"</div>
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
window.onYouTubeIframeAPIReady = function() { window._ytAPIReady = true; };

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
        const titleEl = url
          ? `<a class="track-link" href="#" data-video="${escHtml(url)}" data-track="${escHtml(t.title || "")}" data-album="${escHtml(title)}" data-artist="${escHtml(trackArtist)}" onclick="openVideo(event,'${url.replace(/'/g, "\\'")}')">${escHtml(t.title || "")} ▶</a>`
          : `${escHtml(t.title || "")}${ytIcon}`;
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
        <h2>${escHtml(title)}${window._collectionIds?.has(Number(releaseId)) ? ` <span class="collection-badge" title="In your collection">✓</span>` : ""}${window._wantlistIds?.has(Number(releaseId)) ? ` <span class="wantlist-badge" title="In your wantlist">♡</span>` : ""}</h2>
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

async function loadMasterVersions(event, masterId) {
  if (event) event.preventDefault();
  const list = document.getElementById("master-versions-list");
  if (!list) return;
  try {
    const data = await apiFetch(`${API}/master-versions/${masterId}`).then(r => r.json());
    const versions = data.versions ?? [];
    if (!versions.length) { list.textContent = "No pressings found."; return; }
    list.innerHTML = `
      <div style="font-size:0.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.5rem">Pressings / Versions</div>
      <div style="display:grid;grid-template-columns:auto auto auto auto 1fr;gap:0.2rem 0.7rem;font-size:0.75rem">
        ${versions.map(v => {
        const inCol  = window._collectionIds?.has(v.id);
        const inWant = window._wantlistIds?.has(v.id);
        const badge  = inCol  ? `<span class="collection-badge">✓</span>` :
                       inWant ? `<span class="wantlist-badge">♡</span>` : "";
        return `
          <span style="color:#888">${escHtml(!v.year || v.year === "0" ? "?" : String(v.year))}</span>
          <span style="color:#aaa">${escHtml(v.country || "?")}</span>
          <span style="color:#888">${escHtml(v.format ?? "—")}</span>
          <span style="color:#aaa">${escHtml(v.catno ?? "—")}</span>
          <span><a href="#" onclick="openVersionPopup(event,${v.id})" style="color:var(--accent);text-decoration:none">${escHtml(v.label ?? v.title ?? "—")}</a>${badge}</span>`;
      }).join("")}
      </div>`;
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
  const textRow = (label, items) => {
    if (!items.length) return "";
    const visible  = compact ? items.slice(0, LIMIT) : items;
    const overflow = compact ? items.slice(LIMIT) : [];
    return `<div style="font-size:0.78rem;margin-top:0.55rem;line-height:1.6">
              <span style="color:#777;margin-right:0.4em">${label}:</span>${escHtml(visible.join(" · "))}${moreBtn(overflow, false)}
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
  const html = row("Members", members)
    + row("Also in", groups)
    + row("Aliases", aliases)
    + textRow("Also known as", namevariations)
    + (parentLabel ? row("Part of", [parentLabel]) : "")
    + row("Sub-labels", sublabels)
    + urlRow("Links", urls);
  return html ? `<div style="margin-top:0.6rem;padding-top:0.5rem;border-top:1px solid var(--border)">${html}</div>` : "";
}

function renderBioMarkup(text) {
  if (!text) return '';
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
  doSearch(1);
}

function selectAltArtist(event, el) {
  event.preventDefault();
  document.getElementById("f-artist").value = el.dataset.altName;
  document.getElementById("query").value = "";
  currentArtistId = el.dataset.altId || null;
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
  doSearch(1);
}

// ── Browser back / forward ───────────────────────────────────────────────
window.addEventListener("popstate", () => {
  const p = new URLSearchParams(location.search);
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
    document.getElementById("type-desc").textContent = "";
    document.getElementById("type-pipe").style.display = "none";
    document.getElementById("sort-desc").textContent = "";
    document.getElementById("sort-pipe").style.display = "none";
    
  }
});

// ── Recent searches feed ─────────────────────────────────────────────────
function toTitleCase(s) {
  return s.replace(/\w\S*/g, w => w.charAt(0).toUpperCase() + w.slice(1));
}
function feedLabel(p) {
  const tc = s => toTitleCase(s);
  const parts = [];
  if (p.q && (!p.artist || p.q.toLowerCase() !== p.artist.toLowerCase())) parts.push(tc(p.q));
  if (p.artist)        parts.push(tc(p.artist));
  if (p.release_title) parts.push(`"${tc(p.release_title)}"`);
  if (p.label)         parts.push(`${tc(p.label)} label`);
  if (p.genre)         parts.push(p.genre);
  if (p.style)         parts.push(p.style);
  if (p.format) parts.push(p.format);
  if (p.year)          parts.push(p.year);
  const full = parts.join(" · ") || "Search";
  const short = full.length > 24 ? full.slice(0, 23) + "…" : full;
  return { full, short };
}

function feedApply(p) {
  document.getElementById("query").value     = p.q             ?? "";
  document.getElementById("f-artist").value  = p.artist        ?? "";
  document.getElementById("f-release").value = p.release_title ?? "";
  document.getElementById("f-year").value    = p.year          ?? "";
  document.getElementById("f-label").value   = p.label         ?? "";
  document.getElementById("f-format").value  = p.format        || "";
  document.getElementById("f-genre").value   = p.genre         ?? "";
  populateStyles();
  document.getElementById("f-style").value   = p.style         ?? "";
  const radio = document.querySelector(`input[name="result-type"][value="${p.type ?? ""}"]`);
  if (radio) radio.checked = true;
  const feedSortEl = document.getElementById("f-sort");
  feedSortEl.value = p.sort ?? "";
  if (!feedSortEl.value) feedSortEl.selectedIndex = 0;
  // Open advanced panel if any advanced fields are populated so doSearch picks them up
  const needsAdv = p.artist || p.release_title || p.year || p.label || p.genre || p.style || p.format;
  if (needsAdv) toggleAdvanced(true);
  doSearch(1);
}

let _recentSearches = [];

async function loadRecentFeed() {
  try {
    const data = await fetch("/api/recent-searches").then(r => r.json());
    const searches = data.searches ?? [];
    const el = document.getElementById("recent-feed");
    if (!el) return;
    if (!searches.length) { el.style.display = "none"; return; }
    _recentSearches = searches;
    const pillsHtml = `<div class="feed-pills">${
      searches.map((s, i) => {
        const { full, short } = feedLabel(s.params);
        return `<span class="feed-pill" data-idx="${i}" title="${escHtml(full)}">${escHtml(short)}</span>`;
      }).join("")
    }</div>`;
    el.style.opacity = "0";
    el.innerHTML = pillsHtml;
    el.querySelectorAll(".feed-pill").forEach((pill, i) => {
      pill.addEventListener("click", () => feedApply(searches[i].params));
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
          <a href="${googleUrl}" target="_blank" rel="noopener">Search →</a>
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
  if (p.toString()) {
    restoreFromParams(p);
    await authReadyPromise; // wait for Clerk so Bearer token is attached
    doSearch(parseInt(p.get("pg") ?? "1"), true);
  }
  loadRecentFeed();
  loadFreshReleases();
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

    const authBar = document.getElementById("auth-status");
    if (authBar) {
      if (window._clerk.user) {
        const email = window._clerk.user.primaryEmailAddress?.emailAddress ?? "";
        const [local, domain] = email.split("@");
        const truncated = email ? email.slice(0, 2) + "***" + email.slice(-2) : "account";
        authBar.innerHTML = `<a href="/account">ACCOUNT: ${truncated}</a>`;
      } else {
        authBar.innerHTML = `<a href="/account">Add your Discogs API Token for More Searches</a>`;
      }
    }

    // If user is signed in, check if they have a token and set up collection tabs
    if (window._clerk.user) {
      try {
        const tokenCheck = await apiFetch("/api/user/token");
        if (tokenCheck.ok) {
          const tokenData = await tokenCheck.json();
          if (tokenData.hasToken) {
            addNavTab("collection", "Collection");
            addNavTab("wantlist", "Wantlist");
            await loadDiscogsIds();
          }
        }
      } catch { /* collection tabs optional */ }
    }
  } catch { /* auth unavailable — site works fine without it */ }
  finally { _authReady(); }
})();
