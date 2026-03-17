// ── Config ────────────────────────────────────────────────────────────────
const API = "https://discogs-mcp-server-production-c794.up.railway.app";

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
  document.getElementById("f-sort").value    = p.get("sr") ?? "";
  document.getElementById("f-format").value  = p.get("fm") || "Vinyl";
  populateStyles();
  document.getElementById("f-style").value   = p.get("st") ?? "";
  const rtype = p.get("rt") ?? "";
  const radio = document.querySelector(`input[name="result-type"][value="${rtype}"]`);
  if (radio) radio.checked = true;
}

function clearForm() {
  ["query","f-artist","f-release","f-year","f-label","f-genre","f-style"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });
  document.getElementById("f-sort").value = "";
  document.getElementById("f-format").value = "Vinyl";
  document.getElementById("f-style").innerHTML = '<option value="">Any</option>';
  document.getElementById("f-style").disabled = true;
  document.querySelector('input[name="result-type"][value=""]').checked = true;
  document.getElementById("powered-by").style.display = "";
  document.getElementById("search-desc").textContent = "";
  document.getElementById("search-pipe").style.display = "none";
  document.getElementById("type-desc").textContent = "";
  document.getElementById("type-pipe").style.display = "none";
  document.getElementById("sort-desc").textContent = "";
  document.getElementById("sort-pipe").style.display = "none";
}

async function doSearch(page = 1, skipPushState = false) {
  const q         = document.getElementById("query").value.trim();
  const artistRaw = document.getElementById("f-artist").value.trim();
  const artist    = artistRaw.replace(/\s*\(\d+\)$/, ""); // stripped for search params
  const release   = document.getElementById("f-release").value.trim();
  const year      = document.getElementById("f-year").value.trim();
  const label     = document.getElementById("f-label").value.trim();
  const genre     = document.getElementById("f-genre").value.trim();
  const style     = document.getElementById("f-style")?.value.trim() ?? "";
  const format    = document.getElementById("f-format").value;
  const sort      = document.getElementById("f-sort").value;
  const resultType = document.querySelector('input[name="result-type"]:checked')?.value ?? "";

  if (!q && !artist && !release && !year && !label && !genre) {
    setStatus("Enter a search term or fill in at least one filter.", false);
    return;
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
    document.getElementById("powered-by").style.display = descText ? "none" : "";
    document.getElementById("search-desc").textContent = descText;
    document.getElementById("search-pipe").style.display = descText ? "inline" : "none";
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
    // Only use strict release_title filter when artist is also set;
    // without an artist it ranks by one title's popularity and one artist dominates
    if (release && effectiveArtist) p.set("release_title", release);
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
        bioFetch = fetch(`${API}/label-bio?name=${encodeURIComponent(label)}`);
      } else if (genre) {
        bioFetch = fetch(`${API}/genre-info?genre=${encodeURIComponent(genre)}`);
      }
    }

    let items, totalPages_new;
    if (dualFetch) {
      const [masterRes, releaseRes, bioRes2] = await Promise.all([
        fetch(`${API}/search?${baseParams("master",  18)}`),
        fetch(`${API}/search?${baseParams("release",  6)}`),
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
        fetch(`${API}/search?${rp}`),
        bioFetch ?? Promise.resolve(null),
      ]);
      bioFetch = bioRes ? { json: () => bioRes.json() } : null;
      const relData = await relRes.json();
      const labelNames = [...new Set(
        (relData.results ?? []).flatMap(r => r.label ?? []).filter(Boolean)
      )].slice(0, 12);
      const labelCards = await Promise.all(
        labelNames.map(name =>
          fetch(`${API}/search?q=${encodeURIComponent(name)}&type=label&per_page=1`)
            .then(r => r.json()).then(d => d.results?.[0]).catch(() => null)
        )
      );
      items = labelCards.filter(Boolean);
      totalPages_new = 1;
    } else {
      const [res, bioRes] = await Promise.all([
        fetch(`${API}/search?${baseParams(null, 12)}`),
        bioFetch ?? Promise.resolve(null),
      ]);
      bioFetch = bioRes ? { json: () => bioRes.json() } : null;
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      items = data.results ?? [];
      totalPages_new = data.pagination?.pages ?? 1;
    }
    totalPages = totalPages_new;

    const blurbEl = document.getElementById("blurb");
    if (page === 1) {
      let bioData = null;
      if (bioFetch) {
        bioData = await bioFetch.json().catch(() => null);
      }
      if (!bioData && !artist && !release && !label && !genre && items.length > 0) {
        const firstMedia = items.find(it => (it.type === "release" || it.type === "master") && it.title?.includes(" - "));
        if (firstMedia) {
          const derivedArtist = firstMedia.title.slice(0, firstMedia.title.indexOf(" - "));
          bioData = await fetch(`${API}/artist-bio?name=${encodeURIComponent(derivedArtist)}`).then(r => r.json()).catch(() => null);
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
          const cp = new URLSearchParams({ q: q || constrainedArtist, page, per_page: dualFetch ? 24 : 12, artist: constrainedArtist });
          if (resultType) cp.set("type", resultType);
          if (release)    cp.set("release_title", release);
          if (year)       cp.set("year", year);
          if (format)     cp.set("format", format);
          if (sort && !skipSort) { const [sf, so] = sort.split(":"); cp.set("sort", sf); cp.set("sort_order", so); }
          const cr = await fetch(`${API}/search?${cp}`);
          if (cr.ok) {
            const cd = await cr.json();
            if ((cd.results ?? []).length > 0) {
              items = cd.results;
              totalPages = cd.pagination?.pages ?? totalPages;
            }
          }
        } catch { /* keep original results */ }
      }

      if (bioData?.profile || bioData?.wikiExtract) {
        window._upcomingShows = { name: bioData.name, shows: [] };
        const entityType = artist ? 'artist' : label ? 'label' : genre ? 'genre' : 'artist';
        window._currentBio = {
          name: bioData.name, text: bioData.profile ?? null, wiki: bioData.wikiExtract ?? null,
          members: bioData.members ?? [], groups: bioData.groups ?? [], aliases: bioData.aliases ?? [],
          discogsId: bioData.discogsId ?? null,
        };

        // Use Discogs profile if available, otherwise fall back to Wikipedia extract
        const rawBioText  = bioData.profile ?? null;
        const wikiText    = bioData.wikiExtract ?? null;
        const displayText = rawBioText ? stripDiscogsMarkup(rawBioText) : (wikiText ?? "");
        const TRUNCATE = 300;
        const needsMore = displayText.length > TRUNCATE;
        const truncatedRaw = rawBioText
          ? (needsMore ? truncateRaw(rawBioText, TRUNCATE) + '\u2026' : rawBioText)
          : (needsMore ? displayText.slice(0, TRUNCATE) + '\u2026' : displayText);

        const showsArtist = bioData.name.replace(/\s*\(\d+\)$/, "").trim();
        const showsLink = ` <a href="#" data-artist="${escHtml(showsArtist)}" onclick="fetchAndShowUpcoming(event,this.dataset.artist)" style="font-size:0.75rem;color:#666;font-weight:400;margin-left:0.4rem;text-decoration:none">(upcoming shows)</a>`;
        const readMore = needsMore
          ? ` <a href="#" onclick="openBioFull(event)" style="font-size:0.8rem;color:var(--accent);white-space:nowrap;text-decoration:none">read more</a>`
          : "";
        const heading = bioData.name
          ? `<strong style="display:block;margin-bottom:0.4rem;color:var(--accent)">${escHtml(bioData.name)}${showsLink}</strong>`
          : "";

        let discogsHref = "";
        if (entityType === 'artist') discogsHref = `https://www.discogs.com/search/?q=${encodeURIComponent(bioData.name)}&type=artist`;
        else if (entityType === 'label') discogsHref = `https://www.discogs.com/search/?q=${encodeURIComponent(bioData.name)}&type=label`;
        const discogsLink = discogsHref
          ? `<div style="margin-top:0.3rem"><a href="${discogsHref}" target="_blank" rel="noopener" style="font-size:0.75rem;color:#666;text-decoration:none">View on Discogs ↗</a></div>`
          : "";

        const relLinks = renderArtistRelations(bioData.members ?? [], bioData.groups ?? [], bioData.aliases ?? []);
        const bioHtml  = rawBioText ? renderBioMarkup(truncatedRaw) : escHtml(truncatedRaw);
        blurbEl.innerHTML = heading + bioHtml + readMore + relLinks + discogsLink;
        blurbEl.style.display = "block";
      }
    }

    if (!items.length) { setStatus("No results found."); return; }

    setStatus(`${items.length} results — page ${currentPage} of ${totalPages}`);
    renderResults(items);
    renderPagination();

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
    } else if (urlP.get("sh") === "1") {
      renderShowsPopup();
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
  grid.innerHTML = items.map(item => {
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

    return `
      <a ${cardAttrs}>
        ${thumb}
        <div class="card-body">
          ${artist ? `<div class="card-artist">${escHtml(artist)}</div>` : ""}
          <div class="card-title">${escHtml(title)}</div>
          ${label   ? `<div class="card-sub">${escHtml(label)}</div>`   : ""}
          ${formats ? `<div class="card-format">${escHtml(formats)}</div>` : ""}
          ${genre   ? `<div class="card-format">${escHtml(genre)}</div>`   : ""}
          <div class="card-meta">${metaParts.map(escHtml).join(" · ")}</div>
        </div>
      </a>`;
  }).join("");
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
    fetch(`${API}/${endpoint}/${id}`).then(r => r.json()),
    fetch(`${API}/marketplace-stats/${id}?type=${type}`).then(r => r.json()).catch(() => null),
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
      fetch(`${API}/release/${releaseId}`).then(r => r.json()),
      fetch(`${API}/marketplace-stats/${releaseId}?type=release`).then(r => r.json()).catch(() => null),
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
  const { name, text, wiki, members = [], groups = [], aliases = [], discogsId = null } = window._currentBio ?? {};
  document.getElementById("bio-full-name").textContent = name ?? "";
  let html = renderBioMarkup(text ?? "");
  const relLinks = renderArtistRelations(members, groups, aliases);
  if (relLinks) html += relLinks;
  if (wiki) {
    html += `<hr style="border:none;border-top:1px solid var(--border);margin:1.25rem 0 1rem">
             <div style="font-size:0.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.6rem">Wikipedia</div>
             <div style="font-size:0.87rem;line-height:1.65">${escHtml(wiki)}</div>`;
  }
  if (discogsId) {
    html += `<div style="margin-top:1.1rem"><a href="https://www.discogs.com/artist/${discogsId}" target="_blank" rel="noopener" style="font-size:0.75rem;color:#666;text-decoration:none">View profile on Discogs ↗</a></div>`;
  }
  document.getElementById("bio-full-text").innerHTML = html;
  document.getElementById("bio-full-overlay").classList.add("open");
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

// ── Shows popup ───────────────────────────────────────────────────────────
async function fetchAndShowUpcoming(event, artistName) {
  event.preventDefault();
  const link = event.target;
  link.textContent = "(loading…)";
  try {
    const data = await fetch(`${API}/upcoming-shows?artist=${encodeURIComponent(artistName)}`).then(r => r.json());
    window._upcomingShows = { name: artistName, shows: data.shows ?? [] };
    link.textContent = "(upcoming shows)";
    renderShowsPopup();
  } catch(e) {
    link.textContent = "(upcoming shows)";
  }
}

function renderShowsPopup() {
  const { name, shows } = window._upcomingShows ?? { name: "", shows: [] };
  document.getElementById("shows-title").textContent = `Upcoming Shows — ${name}`;
  document.getElementById("shows-list").innerHTML = shows.length === 0
    ? `<p style="color:var(--muted);padding:0.5rem 0">No upcoming shows found on Ticketmaster.</p>`
    : shows.map(s => `
      <div class="show-row">
        <div class="show-date">${escHtml(s.date)}${s.time ? "<br>" + escHtml(s.time.slice(0,5)) : ""}</div>
        <div class="show-info">
          <div class="show-venue">${escHtml(s.venue)}</div>
          <div class="show-location">${[s.city, s.country].filter(Boolean).map(escHtml).join(", ")}</div>
        </div>
        ${s.url ? `<a href="${s.url}" target="_blank" rel="noopener" class="show-ticket">Tickets ↗</a>` : ""}
      </div>`).join("");
  const u = new URL(window.location.href);
  u.searchParams.set("sh", "1");
  history.replaceState({}, "", u.toString());
  document.getElementById("shows-overlay").classList.add("open");
}

function openShowsPopup(event) {
  event.preventDefault();
  renderShowsPopup();
}

function closeShows() {
  document.getElementById("shows-overlay").classList.remove("open");
  const u = new URL(window.location.href);
  u.searchParams.delete("sh");
  history.replaceState({}, "", u.toString());
}

document.getElementById("shows-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("shows-overlay")) closeShows();
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
    closeShows();
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
  const img      = d.images?.[0]?.uri ?? searchResult.cover_image ?? "";
  const released    = d.released ?? "";
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

  const isMaster = searchResult.type === "master";
  const detailRows = [
    labels  ? `<span class="detail-label">Label</span><span>${escHtml(labels)}</span>`   : "",
    (!isMaster && formats) ? `<span class="detail-label">Format</span><span>${escHtml(formats)}</span>` : "",
    country ? `<span class="detail-label">Country</span><span>${escHtml(country)}</span>` : "",
    genres  ? `<span class="detail-label">Genre</span><span>${escHtml(genres)}</span>`   : "",
    (!isMaster && catno) ? `<span class="detail-label">Cat#</span><span>${escHtml(catno)}</span>` : "",
    year    ? `<span class="detail-label">Year</span><span>${escHtml(String(year))}</span>` : "",
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
      ${img ? `<img class="album-cover" src="${img}" alt="${escHtml(title)}" loading="lazy" />`
             : `<div class="album-cover-placeholder">♪</div>`}
      <div class="album-meta">
        ${typeLabel ? `<div class="album-type-badge">${escHtml(typeLabel)}</div>` : ""}
        <h2>${escHtml(title)}</h2>
        ${artists.length ? `<div class="album-artist">${artists.map(n => `<a href="#" class="modal-artist-link" data-artist="${escHtml(n)}" onclick="searchArtistFromModal(event,this)">${escHtml(n)}</a>`).join(", ")}</div>` : ""}
        ${detailRows ? `<div class="album-detail-grid">${detailRows}</div>` : ""}
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
    const data = await fetch(`${API}/master-versions/${masterId}`).then(r => r.json());
    const versions = data.versions ?? [];
    if (!versions.length) { list.textContent = "No pressings found."; return; }
    list.innerHTML = `
      <div style="font-size:0.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.5rem">Pressings / Versions</div>
      <div style="display:grid;grid-template-columns:auto auto auto auto 1fr;gap:0.2rem 0.7rem;font-size:0.75rem">
        ${versions.map(v => `
          <span style="color:#888">${escHtml(!v.year || v.year === "0" ? "?" : String(v.year))}</span>
          <span style="color:#aaa">${escHtml(v.country || "?")}</span>
          <span style="color:#888">${escHtml(v.format ?? "—")}</span>
          <span style="color:#aaa">${escHtml(v.catno ?? "—")}</span>
          <span><a href="#" onclick="openVersionPopup(event,${v.id})" style="color:var(--accent);text-decoration:none">${escHtml(v.label ?? v.title ?? "—")}</a></span>
        `).join("")}
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

function setStatus(msg, isError = false) {
  const el = document.getElementById("status");
  el.textContent = msg;
  el.className = isError ? "error" : "";
}

function renderArtistRelations(members, groups, aliases) {
  const row = (label, items) => {
    if (!items.length) return "";
    const links = items.map(a =>
      `<a href="#" class="bio-artist-link" onclick="searchBioArtist(event,this)" data-artist="${escHtml(a.name)}"${a.id ? ` data-artist-id="${a.id}"` : ""}>${escHtml(a.name)}</a>`
    ).join('<span style="color:#555;margin:0 0.2em">·</span>');
    return `<div style="font-size:0.78rem;margin-top:0.55rem;line-height:1.6">
              <span style="color:#777;margin-right:0.4em">${label}:</span>${links}
            </div>`;
  };
  const html = row("Members", members) + row("Also in", groups) + row("Aliases", aliases);
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
    document.getElementById("search-pipe").style.display = "none";
    document.getElementById("type-desc").textContent = "";
    document.getElementById("type-pipe").style.display = "none";
    document.getElementById("sort-desc").textContent = "";
    document.getElementById("sort-pipe").style.display = "none";
    document.getElementById("powered-by").style.display = "";
  }
});

// ── Restore from URL on page load ────────────────────────────────────────
(function () {
  const p = new URLSearchParams(location.search);
  if (p.toString()) {
    restoreFromParams(p);
    doSearch(parseInt(p.get("pg") ?? "1"), true);
  }
})();

// Submit on Enter (all text inputs)
["query", "f-artist", "f-release", "f-year", "f-label"].forEach(id => {
  document.getElementById(id).addEventListener("keydown", e => {
    if (e.key === "Enter") doSearch(1);
  });
});
