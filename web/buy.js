// ── Buy tab — eBay 12" vinyl LP auctions ──────────────────────────────
let _buyItems = [];
let _buyTotal = 0;
let _buyMinPrice = 0;
let _buySort = "ending";
let _buyQuery = "";
let _buyOffset = 0;
let _buyLoading = false;
let _buyDebounce = null;
const BUY_PAGE_SIZE = 200;

function renderBuyCard(item, idx) {
  const img = item.image_url
    ? `<img src="${escHtml(item.image_url)}" alt="${escHtml(item.title)}" loading="lazy" onerror="this.style.display='none'">`
    : `<div class="thumb-placeholder">🎵</div>`;

  const price = parseFloat(item.price);
  const priceStr = price.toLocaleString("en-US", { style: "currency", currency: item.currency || "USD" });

  const condition = item.condition || "";
  const loc = [item.location_city, item.location_state].filter(Boolean).join(", ");

  const bids = item.bid_count ?? 0;
  const bidStr = `${bids} bid${bids !== 1 ? "s" : ""}`;

  const specifics = item.item_specifics ?? {};
  const artist = specifics.Artist || specifics.artist || "";
  const label = specifics["Record Label"] || specifics.Label || "";
  const artistLabel = [artist, label].filter(Boolean).join(" · ");

  // Time remaining
  let timeLeft = "";
  if (item.item_end_date) {
    const ms = new Date(item.item_end_date).getTime() - Date.now();
    if (ms > 0) {
      const hrs = Math.floor(ms / 3600000);
      const mins = Math.floor((ms % 3600000) / 60000);
      if (hrs >= 24) { const d = Math.floor(hrs / 24); timeLeft = `${d}d ${hrs % 24}h left`; }
      else if (hrs > 0) timeLeft = `${hrs}h ${mins}m left`;
      else timeLeft = `${mins}m left`;
    }
  }

  const conditionShow = condition && condition !== "Used" ? condition : "";

  return `<div class="card buy-card card-animate" onclick="event.stopPropagation();openBuyPopup(${idx})" role="button" tabindex="0" style="--i:${Math.min(idx, 20)};cursor:pointer;-webkit-tap-highlight-color:transparent" title="${escHtml(item.title)}">
    <div class="card-thumb-wrap" style="pointer-events:none">${img}</div>
    <div class="card-body" style="pointer-events:none">
      <div class="card-title">${escHtml(item.title.length > 65 ? item.title.slice(0, 63) + "…" : item.title)}</div>
      ${artistLabel ? `<div class="card-sub" style="color:#aaa">${escHtml(artistLabel)}</div>` : ""}
      <div class="buy-price">${priceStr}</div>
      <div class="card-sub buy-bids">${escHtml(bidStr)}</div>
      ${conditionShow ? `<div class="card-meta">${escHtml(conditionShow)}</div>` : ""}
      ${loc ? `<div class="card-meta">${escHtml(loc)}</div>` : ""}
      ${timeLeft ? `<div class="card-meta buy-time-left">${timeLeft}</div>` : ""}
    </div>
  </div>`;
}

function openBuyPopup(idx) {
  const item = _buyItems[idx];
  if (!item) return;

  const price = parseFloat(item.price);
  const priceStr = price.toLocaleString("en-US", { style: "currency", currency: item.currency || "USD" });
  const condition = item.condition || "";
  const loc = [item.location_city, item.location_state, item.location_country].filter(Boolean).join(", ");
  const specifics = item.item_specifics ?? {};
  const allImages = item.all_images && item.all_images.length ? item.all_images : (item.image_url ? [item.image_url] : []);

  const buyType = (item.buying_options ?? []).includes("AUCTION")
    ? `Auction${item.bid_count > 0 ? ` · ${item.bid_count} bid${item.bid_count !== 1 ? "s" : ""}` : ""}`
    : "Buy Now";

  const endDate = item.item_end_date
    ? new Date(item.item_end_date).toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })
    : "";

  const seller = item.seller_name || "";
  const feedback = item.seller_feedback ? `(${item.seller_feedback}%)` : "";

  const specKeys = Object.keys(specifics).filter(k => specifics[k]);
  const specsHtml = specKeys.length
    ? `<div class="buy-popup-specs">${specKeys.map(k => `<div class="buy-spec-row"><span class="buy-spec-label">${escHtml(k)}</span> <span>${escHtml(String(specifics[k]))}</span></div>`).join("")}</div>`
    : "";

  const galleryHtml = allImages.length > 1
    ? `<div class="buy-popup-gallery">${allImages.map(u => `<img src="${escHtml(u)}" loading="lazy" onclick="this.parentElement.previousElementSibling.src='${escHtml(u)}'" onerror="this.style.display='none'">`).join("")}</div>`
    : "";

  const detailHtml = item.detail_html
    ? `<div class="buy-popup-description">${item.detail_html}</div>`
    : "";

  const overlay = document.createElement("div");
  overlay.className = "buy-popup-overlay";
  overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };

  overlay.innerHTML = `<div class="buy-popup">
    <button class="buy-popup-close" onclick="this.closest('.buy-popup-overlay').remove()">✕</button>
    ${allImages.length ? `<img class="buy-popup-main-img" src="${escHtml(allImages[0])}" onerror="this.style.display='none'">` : ""}
    ${galleryHtml}
    <div class="buy-popup-body">
      <h3 class="buy-popup-title">${escHtml(item.title)}</h3>
      <div class="buy-popup-price">${priceStr} <span style="font-size:0.78rem;font-weight:400;color:#aaa;margin-left:0.5rem">${escHtml(buyType)}</span></div>
      ${endDate ? `<div class="buy-popup-meta">Ends ${endDate}</div>` : ""}
      ${condition ? `<div class="buy-popup-meta">Condition: ${escHtml(condition)}</div>` : ""}
      ${loc ? `<div class="buy-popup-meta">Location: ${escHtml(loc)}</div>` : ""}
      ${seller ? `<div class="buy-popup-meta">Seller: ${escHtml(seller)} ${feedback}</div>` : ""}
      ${specsHtml}
      ${detailHtml}
      <a class="buy-popup-ebay-link" href="${escHtml(item.item_url)}" target="_blank" rel="noopener">View on eBay →</a>
    </div>
  </div>`;

  document.body.appendChild(overlay);
}

function renderBuyGrid() {
  const grid = document.getElementById("buy-results");
  if (!grid) return;
  if (!_buyItems.length) {
    grid.innerHTML = `<div style="color:var(--muted);font-size:0.8rem;grid-column:1/-1;text-align:center;padding:2rem 0">No vinyl auctions available yet. Check back soon!</div>`;
    return;
  }
  grid.innerHTML = _buyItems.map((item, idx) => renderBuyCard(item, idx)).join("");
}

function setBuyPriceFilter(minPrice) {
  _buyMinPrice = minPrice;
  _buyOffset = 0;
  _buyItems = [];
  document.querySelectorAll(".buy-price-pill").forEach(p =>
    p.classList.toggle("active", parseInt(p.dataset.min) === minPrice)
  );
  loadBuyListings();
}

function setBuySort(sort) {
  _buySort = sort;
  _buyOffset = 0;
  _buyItems = [];
  loadBuyListings();
}

function onBuySearch(val) {
  clearTimeout(_buyDebounce);
  _buyDebounce = setTimeout(() => {
    _buyQuery = val;
    _buyOffset = 0;
    _buyItems = [];
    loadBuyListings();
  }, 350);
}

async function loadBuyListings(append = false) {
  if (_buyLoading) return;
  _buyLoading = true;

  const status = document.getElementById("buy-status");
  const loadMore = document.getElementById("buy-load-more");

  if (!append) {
    _buyOffset = 0;
    _buyItems = [];
    if (status) status.textContent = "";
    document.getElementById("buy-results").innerHTML = renderSkeletonGrid(16);
  } else {
    if (loadMore) loadMore.textContent = "Loading…";
  }

  try {
    const qEnc = _buyQuery ? `&q=${encodeURIComponent(_buyQuery)}` : "";
    const url = `/api/vinyl?min_price=${_buyMinPrice}&sort=${_buySort}${qEnc}&limit=${BUY_PAGE_SIZE}&offset=${_buyOffset}`;
    const r = await fetch(url);
    const data = await r.json();
    const newItems = data.items ?? [];
    _buyTotal = data.total ?? 0;

    if (append) {
      _buyItems = _buyItems.concat(newItems);
    } else {
      _buyItems = newItems;
    }

    renderBuyGrid();

    if (status) {
      if (!_buyItems.length) {
        status.textContent = "";
        document.getElementById("buy-results").innerHTML = renderEmptyState("🎵", "No vinyl auctions found", "Try adjusting your filters or check back later");
      } else {
        const newest = newItems.reduce((max, i) => i.fetched_at > max ? i.fetched_at : max, "");
        let ago = "";
        if (newest) {
          const mins = Math.round((Date.now() - new Date(newest).getTime()) / 60000);
          if (mins < 1) ago = "just now";
          else if (mins < 60) ago = `${mins} minute${mins !== 1 ? "s" : ""} ago`;
          else { const hrs = Math.round(mins / 60); ago = `${hrs} hour${hrs !== 1 ? "s" : ""} ago`; }
        }
        status.textContent = `${_buyTotal.toLocaleString()} auctions · showing ${_buyItems.length}` +
          (ago ? ` · updated ${ago}` : "");
      }
    }

    if (loadMore) {
      if (_buyItems.length < _buyTotal) {
        loadMore.style.display = "";
        loadMore.textContent = `Load more (${(_buyTotal - _buyItems.length).toLocaleString()} remaining)`;
      } else {
        loadMore.style.display = "none";
      }
    }
  } catch (e) {
    if (status) status.textContent = "Failed to load vinyl listings.";
    showToast("Failed to load vinyl listings — please try again", "error");
  }
  _buyLoading = false;
}

function loadMoreBuy() {
  _buyOffset += BUY_PAGE_SIZE;
  loadBuyListings(true);
}
