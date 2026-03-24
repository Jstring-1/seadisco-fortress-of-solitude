// ── Live tab: concert/event search ───────────────────────────────────────

// Restore saved city on load
(function () {
  const saved = localStorage.getItem("seadisco_live_city");
  if (saved) {
    const el = document.getElementById("live-city");
    if (el) el.value = saved;
  }
})();

async function doLiveSearch() {
  const artist = (document.getElementById("live-artist")?.value ?? "").trim();
  const city   = (document.getElementById("live-city")?.value ?? "").trim();
  const genre  = (document.getElementById("live-genre")?.value ?? "").trim();

  if (!artist && !city && !genre) {
    document.getElementById("live-status").textContent = "Enter an artist, city, or pick a genre.";
    return;
  }

  // Save city for next visit
  if (city) {
    localStorage.setItem("seadisco_live_city", city);
  }

  const statusEl  = document.getElementById("live-status");
  const resultsEl = document.getElementById("live-results");
  statusEl.textContent = "Searching…";
  resultsEl.innerHTML = "";

  try {
    const params = new URLSearchParams();
    if (artist) params.set("artist", artist);
    if (city)   params.set("city", city);
    if (genre)  params.set("genre", genre);

    const data = await fetch(`/api/concerts/search?${params}`).then(r => r.json());
    const events = data.events ?? [];
    const artistImage = data.artistImage ?? null;

    if (!events.length) {
      statusEl.textContent = "";
      resultsEl.innerHTML = `<div class="live-empty">No events found. Try broadening your search.</div>`;
      return;
    }

    const parts = [];
    if (artist) parts.push(artist);
    if (city)   parts.push(city);
    if (genre)  parts.push(genre);
    statusEl.textContent = `${events.length} event${events.length !== 1 ? "s" : ""} — ${parts.join(" · ")}`;

    let html = "";

    // Artist header with image (only when artist was searched)
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

    for (const ev of events) {
      const fmtDate = _liveFmtDate(ev.date);
      const fmtTime = _liveFmtTime(ev.time);
      const location = [ev.city, ev.region, ev.country].filter(Boolean).join(", ");
      const googleQ = encodeURIComponent(`${ev.artist || artist} ${ev.venue} ${ev.city} concert tickets`);
      const googleUrl = `https://www.google.com/search?q=${googleQ}`;

      // Show artist name on each row when no artist was searched (city/genre mode)
      const artistLine = !artist && ev.artist
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
          <div class="live-event-venue">
            <a href="${googleUrl}" target="_blank" rel="noopener">${escHtml(ev.venue)}</a>
            ${location ? ` — ${escHtml(location)}` : ""}
          </div>
          <div class="live-event-source">${escHtml(ev.source)}</div>
        </div>
      </div>`;
    }

    resultsEl.innerHTML = html;
  } catch (err) {
    statusEl.textContent = "";
    resultsEl.innerHTML = `<div class="live-empty">Failed to load events.</div>`;
  }
}

function liveSearchArtist(name) {
  document.getElementById("live-artist").value = name;
  doLiveSearch();
}

function clearLiveSearch() {
  document.getElementById("live-artist").value = "";
  document.getElementById("live-city").value = "";
  document.getElementById("live-genre").value = "";
  document.getElementById("live-status").textContent = "";
  document.getElementById("live-results").innerHTML = "";
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
