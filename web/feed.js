// ── Feed tab — music news, reviews & videos ────────────────────────────
let _feedItems = [];
let _feedTotal = 0;
let _feedCategory = "all";
let _feedQuery = "";
let _feedOffset = 0;
let _feedLoading = false;
let _feedDebounce = null;
const FEED_PAGE_SIZE = 50;

const FEED_SOURCE_URLS = {
  "Pitchfork": "https://pitchfork.com",
  "Pitchfork News": "https://pitchfork.com",
  "Bandcamp Daily": "https://daily.bandcamp.com",
  "Stereogum": "https://www.stereogum.com",
  "The Vinyl Factory": "https://thevinylfactory.com",
  "Aquarium Drunkard": "https://aquariumdrunkard.com",
  "The Quietus": "https://thequietus.com",
  "Analog Planet": "https://www.analogplanet.com",
  "Vinyl Eyezz": "https://www.youtube.com/@VinylEyezz",
  "Techmoan": "https://www.youtube.com/@Techmoan",
  "The Vinyl Guide": "https://www.youtube.com/@TheVinylGuide",
};

function renderFeedCard(item) {
  const isVideo = item.content_type === "video";
  const img = item.image_url
    ? `<img src="${escHtml(item.image_url)}" alt="" loading="lazy" />`
    : `<div style="background:#1a1a1a;width:100%;aspect-ratio:16/9;display:flex;align-items:center;justify-content:center;color:#555;font-size:2rem">${isVideo ? "▶" : "📰"}</div>`;

  // Format published date
  let pubDate = "";
  if (item.published_at) {
    const d = new Date(item.published_at);
    pubDate = d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  }

  const sourceClass = item.source.toLowerCase().replace(/\s+/g, "-");
  const sourceUrl = FEED_SOURCE_URLS[item.source] || "#";
  const catLabel = (item.category || "news").charAt(0).toUpperCase() + (item.category || "news").slice(1);

  if (isVideo) {
    const videoId = item.source_url.match(/[?&]v=([^&]+)/)?.[1] || "";
    const videoAuthorLine = item.author
      ? `${escHtml(item.author)} via <a href="${escHtml(sourceUrl)}" target="_blank" rel="noopener" class="feed-src-${sourceClass}" onclick="event.stopPropagation()">${escHtml(item.source)}</a>`
      : `<a href="${escHtml(sourceUrl)}" target="_blank" rel="noopener" class="feed-src-${sourceClass}" onclick="event.stopPropagation()">${escHtml(item.source)}</a>`;
    return `<div class="feed-card feed-card-video" onclick="playFeedVideo('${videoId}', ${JSON.stringify(escHtml(item.title)).replace(/'/g, "\\'")})" style="cursor:pointer">
      <div class="feed-card-thumb">${img}<div class="feed-video-play">▶</div></div>
      <div class="feed-card-body">
        <div class="feed-card-category">${escHtml(catLabel)}</div>
        <div class="feed-card-title">${escHtml(item.title)}</div>
        <div class="feed-card-author">${videoAuthorLine}</div>
        ${pubDate ? `<div class="feed-card-date">${pubDate}</div>` : ""}
        ${item.summary ? `<div class="feed-card-summary">${escHtml(item.summary.length > 150 ? item.summary.slice(0, 147) + "…" : item.summary)}</div>` : ""}
      </div>
    </div>`;
  }

  // Article cards use <a> wrapper so source must be a <span>, not nested <a>
  const articleAuthorLine = item.author
    ? `${escHtml(item.author)} via <span class="feed-src-${sourceClass}">${escHtml(item.source)}</span>`
    : `<span class="feed-src-${sourceClass}">${escHtml(item.source)}</span>`;

  return `<a href="${escHtml(item.source_url)}" target="_blank" rel="noopener" class="feed-card feed-card-article">
    <div class="feed-card-thumb">${img}</div>
    <div class="feed-card-body">
      <div class="feed-card-category">${escHtml(catLabel)}</div>
      <div class="feed-card-title">${escHtml(item.title)}</div>
      <div class="feed-card-author">${articleAuthorLine}</div>
      ${pubDate ? `<div class="feed-card-date">${pubDate}</div>` : ""}
      ${item.summary ? `<div class="feed-card-summary">${escHtml(item.summary.length > 150 ? item.summary.slice(0, 147) + "…" : item.summary)}</div>` : ""}
    </div>
  </a>`;
}

function playFeedVideo(videoId, title) {
  if (typeof openVideoPlayer === "function") {
    openVideoPlayer(videoId, title);
  } else {
    window.open(`https://www.youtube.com/watch?v=${videoId}`, "_blank");
  }
}

function openVideoPlayer(videoId, title) {
  const overlay = document.getElementById("video-overlay");
  const player = document.getElementById("video-player");
  const titleEl = document.getElementById("video-title");
  if (!overlay || !player) {
    window.open(`https://www.youtube.com/watch?v=${videoId}`, "_blank");
    return;
  }
  player.innerHTML = `<iframe width="100%" height="100%" src="https://www.youtube.com/embed/${videoId}?autoplay=1" frameborder="0" allow="autoplay; encrypted-media" allowfullscreen></iframe>`;
  if (titleEl) titleEl.textContent = title || "";
  overlay.classList.add("visible");
  document.getElementById("video-prev")?.setAttribute("disabled", "true");
  document.getElementById("video-next")?.setAttribute("disabled", "true");
}

function renderFeedGrid() {
  const grid = document.getElementById("feed-results");
  if (!grid) return;
  grid.innerHTML = _feedItems.map((item, i) => renderFeedCard(item)).join("");

  const loadMoreBtn = document.getElementById("feed-load-more");
  if (loadMoreBtn) {
    loadMoreBtn.style.display = _feedItems.length < _feedTotal ? "" : "none";
  }
}

function setFeedCategory(cat) {
  _feedCategory = cat;
  _feedOffset = 0;
  _feedItems = [];
  document.querySelectorAll(".feed-cat-pill").forEach(p =>
    p.classList.toggle("active", p.dataset.cat === cat)
  );
  loadFeedArticles();
}

function onFeedSearch(val) {
  clearTimeout(_feedDebounce);
  _feedDebounce = setTimeout(() => {
    _feedQuery = val;
    _feedOffset = 0;
    _feedItems = [];
    loadFeedArticles();
  }, 350);
}

async function loadFeedArticles(append = false) {
  if (_feedLoading) return;
  _feedLoading = true;
  const status = document.getElementById("feed-status");

  try {
    const params = new URLSearchParams({
      category: _feedCategory,
      limit: String(FEED_PAGE_SIZE),
      offset: String(_feedOffset),
    });
    if (_feedQuery) params.set("q", _feedQuery);

    const r = await apiFetch(`/api/feed?${params}`);
    const data = await r.json();
    const newItems = data.items || [];
    _feedTotal = data.total || 0;

    if (append) {
      _feedItems = _feedItems.concat(newItems);
    } else {
      _feedItems = newItems;
    }

    if (status) {
      status.textContent = `${_feedTotal} articles`;
    }

    renderFeedGrid();
  } catch (e) {
    if (status) status.textContent = "Error loading feed";
  } finally {
    _feedLoading = false;
  }
}

function loadMoreFeed() {
  _feedOffset += FEED_PAGE_SIZE;
  loadFeedArticles(true);
}
