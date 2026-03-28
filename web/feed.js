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
  "Aquarium Drunkard": "https://aquariumdrunkard.com",
  "The Quietus": "https://thequietus.com",
  "BrooklynVegan": "https://www.brooklynvegan.com",
  "Resident Advisor": "https://ra.co",
  "Vinyl Eyezz": "https://www.youtube.com/@VinylEyezz",
  "Techmoan": "https://www.youtube.com/@Techmoan",
  "The Vinyl Guide": "https://www.youtube.com/@TheVinylGuide",
  "KEXP": "https://www.youtube.com/@kaboretv",
  "Tiny Desk (NPR)": "https://www.youtube.com/@nprmusic",
  "COLORS": "https://www.youtube.com/@COLORSxSTUDIOS",
  "Audiotree": "https://www.youtube.com/@Audiotree",
  "The Needle Drop": "https://www.youtube.com/@theneedledrop",
  "Deep Cuts": "https://www.youtube.com/@DeepCutsMusic",
};

function renderFeedCard(item, index) {
  const isVideo = item.content_type === "video";
  const img = item.image_url
    ? `<img src="${escHtml(item.image_url)}" alt="" loading="lazy" />`
    : `<div style="background:#1a1a1a;width:100%;aspect-ratio:16/9;display:flex;align-items:center;justify-content:center;color:#555;font-size:2rem">${isVideo ? "▶" : "📰"}</div>`;

  const sourceClass = item.source.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/-+$/, "");
  const catLabel = (item.category || "news").charAt(0).toUpperCase() + (item.category || "news").slice(1);
  const authorLine = item.author
    ? `${escHtml(item.author)} via <span class="feed-src-${sourceClass}">${escHtml(item.source)}</span>`
    : `<span class="feed-src-${sourceClass}">${escHtml(item.source)}</span>`;

  const animIdx = index != null ? Math.min(index, 20) : 0;
  return `<a href="${escHtml(item.source_url)}" target="_blank" rel="noopener" class="feed-card${isVideo ? " feed-card-video" : ""} card-animate" style="--i:${animIdx}">
    <div class="feed-card-thumb">${img}${isVideo ? '<div class="feed-video-play">▶</div>' : ""}</div>
    <div class="feed-card-body">
      <div class="feed-card-category">${escHtml(catLabel)}</div>
      <div class="feed-card-title">${escHtml(item.title)}</div>
      <div class="feed-card-author">${authorLine}</div>

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
  grid.innerHTML = _feedItems.map((item, i) => renderFeedCard(item, i)).join("");

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

async function loadFeedArticles(append = false, _retryCount = 0) {
  if (_feedLoading) return;
  _feedLoading = true;
  const status = document.getElementById("feed-status");

  if (!append) {
    document.getElementById("feed-results").innerHTML = renderFeedSkeletonGrid(8);
  }

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

    // Shuffle new items for variety
    for (let i = newItems.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [newItems[i], newItems[j]] = [newItems[j], newItems[i]];
    }
    if (append) {
      _feedItems = _feedItems.concat(newItems);
    } else {
      _feedItems = newItems;
    }

    if (status) {
      status.textContent = `${_feedTotal} articles`;
    }

    if (!_feedItems.length) {
      // Auto-retry once after 2s — the feed DB may not have been ready
      if (_retryCount < 1 && !_feedQuery) {
        _feedLoading = false;
        setTimeout(() => loadFeedArticles(false, _retryCount + 1), 2000);
        return;
      }
      document.getElementById("feed-results").innerHTML = renderEmptyState("📰", "No articles yet", "The feed updates every 4 hours with music news and reviews")
        + `<div style="text-align:center;margin-top:1rem"><button onclick="loadFeedArticles()" style="background:var(--accent);color:#fff;border:none;padding:0.5rem 1.2rem;border-radius:4px;cursor:pointer;font-size:0.85rem">Refresh Feed</button></div>`;
    } else {
      renderFeedGrid();
    }
  } catch (e) {
    // Auto-retry once on network/parse error
    if (_retryCount < 1) {
      _feedLoading = false;
      setTimeout(() => loadFeedArticles(append, _retryCount + 1), 2000);
      return;
    }
    if (status) status.textContent = "Error loading feed";
    showToast("Failed to load feed — please try again", "error");
  } finally {
    _feedLoading = false;
  }
}

function loadMoreFeed() {
  _feedOffset += FEED_PAGE_SIZE;
  loadFeedArticles(true);
}
