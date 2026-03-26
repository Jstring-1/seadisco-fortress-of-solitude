// ── Live tab: concert/event search ───────────────────────────────────────
let _livePage = 0;
let _liveTotal = 0;

function _renderLiveEvents(events, artist) {
  let html = "";
  for (const ev of events) {
    const fmtDate = _liveFmtDate(ev.date);
    const fmtTime = _liveFmtTime(ev.time);
    const location = [ev.city, ev.region, ev.country].filter(Boolean).join(", ");
    const artistLine = !artist && ev.artist
      ? `<div class="live-event-artist"><a href="#" onclick="event.preventDefault();liveSearchArtist('${escHtml(ev.artist).replace(/'/g, "\\'")}')">${escHtml(ev.artist)}</a></div>`
      : "";
    const venueLink = ev.venueId
      ? `<a href="#" onclick="event.preventDefault();liveSearchVenue('${escHtml(ev.venueId).replace(/'/g, "\\'")}','${escHtml(ev.venue).replace(/'/g, "\\'")}')">${escHtml(ev.venue)}</a>`
      : `<a href="https://www.google.com/search?q=${encodeURIComponent(`${ev.venue} ${ev.city} concerts`)}" target="_blank" rel="noopener">${escHtml(ev.venue)}</a>`;
    html += `<div class="live-event">
      <div class="live-event-date">
        ${escHtml(fmtDate)}
        ${fmtTime ? `<span class="live-event-time">${escHtml(fmtTime)}</span>` : ""}
      </div>
      <div class="live-event-info">
        ${artistLine}
        <div class="live-event-name">${escHtml(ev.name)}</div>
        <div class="live-event-venue">
          ${venueLink}
          ${location ? ` — ${escHtml(location)}` : ""}
        </div>
      </div>
    </div>`;
  }
  return html;
}

async function doLiveSearch(append = false) {
  const artist = (document.getElementById("live-artist")?.value ?? "").trim();
  const city   = (document.getElementById("live-city")?.value ?? "").trim();
  const genre  = (document.getElementById("live-genre")?.value ?? "").trim();

  if (!artist && !city && !genre) {
    document.getElementById("live-status").textContent = "Enter an artist, city, or pick a genre.";
    return;
  }

  const statusEl  = document.getElementById("live-status");
  const resultsEl = document.getElementById("live-results");

  _hideLiveUpcoming();

  if (!append) {
    _livePage = 0;
    _liveTotal = 0;
    statusEl.textContent = "Searching…";
    resultsEl.innerHTML = "";
  } else {
    // Remove the "load more" link before fetching
    const moreEl = document.getElementById("live-load-more");
    if (moreEl) moreEl.textContent = "Loading…";
  }

  try {
    const params = new URLSearchParams();
    if (artist) params.set("artist", artist);
    if (city)   params.set("city", city);
    if (genre)  params.set("genre", genre);
    if (_livePage > 0) params.set("page", String(_livePage));

    const data = await fetch(`/api/concerts/search?${params}`).then(r => r.json());
    const events = data.events ?? [];
    const artistImage = data.artistImage ?? null;
    const hasMore = data.hasMore ?? false;

    if (!events.length && !append) {
      statusEl.textContent = "";
      resultsEl.innerHTML = `<div class="live-empty">No events found. Try broadening your search.</div>`;
      return;
    }

    _liveTotal += events.length;

    // Update URL for sharing
    if (!append) {
      const u = new URLSearchParams();
      u.set("view", "live");
      if (artist) u.set("la", artist);
      if (city)   u.set("lc", city);
      if (genre)  u.set("lg", genre);
      history.replaceState({}, "", "?" + u.toString());
    }

    // Save live search to DB only on first page and when results found
    if (!append && events.length) {
      const liveParams = {};
      if (artist) liveParams.artist = artist;
      if (city)   liveParams.city = city;
      if (genre)  liveParams.genre = genre;
      apiFetch("/api/user/live-search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ params: liveParams }),
      }).catch(() => {});
    }

    const parts = [];
    if (artist) parts.push(artist);
    if (city)   parts.push(city);
    if (genre)  parts.push(genre);
    const headingEl = document.getElementById("live-heading");
    const backLink = _livePrevSearch
      ? ` <a href="#" onclick="event.preventDefault();liveGoBack()" style="color:var(--accent);text-decoration:none;font-size:0.8rem;margin-left:0.4rem">← Back</a>`
      : "";
    if (headingEl) {
      headingEl.innerHTML = escHtml(parts.join(" · ")) + backLink;
      headingEl.style.display = "";
    }
    statusEl.textContent = `${_liveTotal} event${_liveTotal !== 1 ? "s" : ""}`;

    if (append) {
      // Remove old "load more" link
      const moreEl = document.getElementById("live-load-more");
      if (moreEl) moreEl.remove();
      // Append new events
      resultsEl.insertAdjacentHTML("beforeend", _renderLiveEvents(events, artist));
    } else {
      let html = "";
      if (artist && artistImage) {
        html += `<div class="live-artist-header">
          <img class="live-artist-img" src="${escHtml(artistImage)}" alt="" onerror="this.style.display='none'">
          <div class="live-artist-name"><a href="#" onclick="event.preventDefault();liveSearchArtist('${escHtml(artist).replace(/'/g, "\\'")}')">${escHtml(artist)}</a></div>
        </div>`;
      } else if (artist) {
        html += `<div class="live-artist-header">
          <div class="live-artist-name"><a href="#" onclick="event.preventDefault();liveSearchArtist('${escHtml(artist).replace(/'/g, "\\'")}')">${escHtml(artist)}</a></div>
        </div>`;
      }
      html += _renderLiveEvents(events, artist);
      resultsEl.innerHTML = html;
    }

    // Add "Load more" link if there are more pages
    if (hasMore) {
      resultsEl.insertAdjacentHTML("beforeend",
        `<div id="live-load-more" style="text-align:center;padding:1rem 0">
          <a href="#" onclick="event.preventDefault();_livePage++;doLiveSearch(true)" style="color:var(--accent);text-decoration:none;font-size:0.85rem">Load more events →</a>
        </div>`);
    }

  } catch (err) {
    statusEl.textContent = "";
    if (!append) resultsEl.innerHTML = `<div class="live-empty">Failed to load events.</div>`;
  }
}

let _livePrevSearch = null;

function liveSearchArtist(name) {
  // Save current fields so user can go back
  _livePrevSearch = {
    artist: document.getElementById("live-artist").value,
    city:   document.getElementById("live-city").value,
    genre:  document.getElementById("live-genre").value,
  };
  document.getElementById("live-artist").value = name;
  document.getElementById("live-genre").value = "";
  doLiveSearch();
}

function liveGoBack() {
  if (!_livePrevSearch) return;
  document.getElementById("live-artist").value = _livePrevSearch.artist;
  document.getElementById("live-city").value   = _livePrevSearch.city;
  document.getElementById("live-genre").value  = _livePrevSearch.genre;
  _livePrevSearch = null;
  doLiveSearch();
}

async function liveSearchVenue(venueId, venueName) {
  // Save current fields so user can go back
  _livePrevSearch = {
    artist: document.getElementById("live-artist").value,
    city:   document.getElementById("live-city").value,
    genre:  document.getElementById("live-genre").value,
  };
  const statusEl  = document.getElementById("live-status");
  const resultsEl = document.getElementById("live-results");
  statusEl.textContent = `Loading events at ${venueName}…`;
  resultsEl.innerHTML = "";

  try {
    const data = await fetch(`/api/concerts/venue/${encodeURIComponent(venueId)}`).then(r => r.json());
    const events = data.events ?? [];
    const name   = data.venueName || venueName;
    const loc    = data.location || "";

    if (!events.length) {
      statusEl.textContent = "";
      resultsEl.innerHTML = `<div class="live-empty">No upcoming events at ${escHtml(name)}</div>`;
      return;
    }

    const headingEl = document.getElementById("live-heading");
    if (headingEl) {
      headingEl.innerHTML = `${escHtml(name)}${loc ? ` <span style="font-size:0.8rem;color:#555">— ${escHtml(loc)}</span>` : ""} <a href="#" onclick="event.preventDefault();liveGoBack()" style="color:var(--accent);text-decoration:none;font-size:0.8rem;margin-left:0.4rem">← Back</a>`;
      headingEl.style.display = "";
    }
    statusEl.textContent = `${events.length} upcoming event${events.length !== 1 ? "s" : ""}`;

    let html = "";
    for (const ev of events) {
      const fmtDate = _liveFmtDate(ev.date);
      const fmtTime = _liveFmtTime(ev.time);
      const artistLine = ev.artist
        ? `<div class="live-event-artist"><a href="#" onclick="event.preventDefault();liveSearchArtist('${escHtml(ev.artist).replace(/'/g, "\\'")}')">${escHtml(ev.artist)}</a></div>`
        : "";
      html += `<div class="live-event">
        <div class="live-event-date">
          ${escHtml(fmtDate)}
          ${fmtTime ? `<span class="live-event-time">${escHtml(fmtTime)}</span>` : ""}
        </div>
        <div class="live-event-info">
          ${artistLine}
          <div class="live-event-name">${escHtml(ev.name)}</div>
        </div>
      </div>`;
    }
    resultsEl.innerHTML = html;
  } catch {
    statusEl.textContent = "";
    resultsEl.innerHTML = `<div class="live-empty">Failed to load venue events.</div>`;
  }
}

function clearLiveSearch() {
  document.getElementById("live-artist").value = "";
  document.getElementById("live-city").value = "";
  document.getElementById("live-genre").value = "";
  document.getElementById("live-status").textContent = "";
  document.getElementById("live-results").innerHTML = "";
  const h = document.getElementById("live-heading");
  if (h) { h.textContent = ""; h.style.display = "none"; }
}

function _liveFmtDate(d) {
  if (!d) return "";
  try {
    return new Date(d + "T12:00:00").toLocaleDateString("en-US", {
      weekday: "short", month: "short", day: "numeric"
    });
  } catch { return d; }
}

function _liveFmtTime(t) {
  if (!t) return "";
  try {
    const [h, m] = t.split(":");
    const hr = parseInt(h);
    return `${hr > 12 ? hr - 12 : hr}:${m} ${hr >= 12 ? "PM" : "AM"}`;
  } catch { return t; }
}

// ── Upcoming events (pre-search filler) ──────────────────────────────────
let _liveUpcomingLoaded = false;

let _liveUpcomingAll = [];
const LIVE_UPCOMING_PAGE = 8;

async function loadLiveUpcoming() {
  if (_liveUpcomingLoaded) return;
  _liveUpcomingLoaded = true;
  const wrap = document.getElementById("live-upcoming");
  const list = document.getElementById("live-upcoming-list");
  if (!wrap || !list) return;
  try {
    const data = await fetch("/api/live/upcoming").then(r => r.json());
    _liveUpcomingAll = data.events ?? [];
    if (!_liveUpcomingAll.length) { wrap.style.display = "none"; return; }
    _renderUpcomingSlice(LIVE_UPCOMING_PAGE);
    wrap.style.display = "";
  } catch {
    wrap.style.display = "none";
  }
}

function _renderUpcomingSlice(count) {
  const list = document.getElementById("live-upcoming-list");
  if (!list) return;
  const slice = _liveUpcomingAll.slice(0, count);
  let html = _renderLiveEvents(slice, "");
  if (count < _liveUpcomingAll.length) {
    html += `<div style="text-align:center;padding:0.5rem 0">
      <a href="#" onclick="event.preventDefault();_renderUpcomingSlice(${count + LIVE_UPCOMING_PAGE})" style="color:var(--accent);text-decoration:none;font-size:0.82rem">Load more events →</a>
    </div>`;
  }
  list.innerHTML = html;
}

function _hideLiveUpcoming() {
  const el = document.getElementById("live-upcoming");
  if (el) el.style.display = "none";
}

// ── Recent live search pill cloud ────────────────────────────────────────
let _liveRecentLoaded = false;

async function loadLiveRecentFeed() {
  loadLiveUpcoming();
  if (_liveRecentLoaded) return;
  _liveRecentLoaded = true;
  const el = document.getElementById("live-recent-feed");
  if (!el) return;
  try {
    const data = await fetch("/api/recent-live-searches").then(r => r.json());
    const searches = data.searches ?? [];
    if (!searches.length) { el.style.display = "none"; return; }
    // Shuffle client-side for variety on cached responses
    for (let i = searches.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [searches[i], searches[j]] = [searches[j], searches[i]];
    }
    const pills = searches.map((s, i) => {
      const p = s.params;
      const parts = [];
      if (p.artist) parts.push(p.artist);
      if (p.city)   parts.push(p.city);
      if (p.genre)  parts.push(p.genre);
      const full = parts.join(" · ");
      const short = full.length > 28 ? full.slice(0, 27) + "…" : full;
      return `<span class="pill feed-pill" data-live-idx="${i}" title="${escHtml(full)}">${escHtml(short)}</span>`;
    }).join("");
    el.innerHTML = `<div class="feed-label">Recent Live Searches</div><div class="feed-pills">${pills}</div>`;
    el.querySelectorAll(".feed-pill").forEach((pill, i) => {
      pill.addEventListener("click", () => {
        const p = searches[i].params;
        document.getElementById("live-artist").value = p.artist || "";
        document.getElementById("live-city").value   = p.city || "";
        document.getElementById("live-genre").value  = p.genre || "";
        doLiveSearch();
      });
    });
  } catch {
    if (el) el.style.display = "none";
  }
}
