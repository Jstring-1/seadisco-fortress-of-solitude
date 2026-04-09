// ── Buy tab — eBay 12" vinyl LP Buy-It-Now listings ───────────────────

/** Close buy popup and search main for a term (used by vinyl popup links) */
function searchFromBuyPopup(event, query) {
  event.preventDefault();
  const overlay = event.target.closest(".buy-popup-overlay");
  if (overlay) overlay.remove();
  if (typeof switchView === "function") switchView("search");
  if (typeof clearForm === "function") clearForm();
  const qField = document.getElementById("f-query");
  if (qField) qField.value = query;
  if (typeof doSearch === "function") doSearch(1);
}
/** Close buy popup and search artist field (used by vinyl popup artist links) */
function searchArtistFromBuyPopup(event, el) {
  event.preventDefault();
  const overlay = event.target.closest(".buy-popup-overlay");
  if (overlay) overlay.remove();
  if (typeof switchView === "function") switchView("search");
  if (typeof clearForm === "function") clearForm();
  const artistField = document.getElementById("f-artist");
  if (artistField) artistField.value = el.dataset.artist;
  if (typeof toggleAdvanced === "function") toggleAdvanced(true);
  if (typeof doSearch === "function") doSearch(1);
}
/** Close buy popup and search collection (used by vinyl popup ⌕ icons) */
function searchCollectionFromBuyPopup(event, field, value) {
  event.preventDefault();
  const overlay = event.target.closest(".buy-popup-overlay");
  if (overlay) overlay.remove();
  window._pendingCwSearch = { field, value };
  if (typeof switchView === "function") switchView("records");
}

let _buyItems = [];
let _buyTotal = 0;
let _buyMinPrice = 0;
let _buySort = "price_desc";
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

  const specifics = item.item_specifics ?? {};
  const artist = specifics.Artist || specifics.artist || "";
  const label = specifics["Record Label"] || specifics.Label || "";
  const artistLabel = [artist, label].filter(Boolean).join(" · ");

  const conditionShow = condition && condition !== "Used" ? condition : "";

  return `<div class="card buy-card card-animate" onclick="event.stopPropagation();openBuyPopup(${idx})" role="button" tabindex="0" style="--i:${Math.min(idx, 20)};cursor:pointer;-webkit-tap-highlight-color:transparent" title="${escHtml(item.title)}">
    <div class="card-thumb-wrap" style="pointer-events:none">${img}</div>
    <div class="card-body" style="pointer-events:none">
      <div class="card-title">${escHtml(item.title)}</div>
      ${artistLabel ? `<div class="card-sub" style="color:#aaa">${escHtml(artistLabel)}</div>` : ""}
      <div class="buy-price">${priceStr}</div>
      ${conditionShow ? `<div class="card-meta">${escHtml(conditionShow)}</div>` : ""}
      ${loc ? `<div class="card-meta">${escHtml(loc)}</div>` : ""}
    </div>
  </div>`;
}

function openBuyPopup(idx) {
  _openEbayPopup(_buyItems[idx], { vinyl: true });
}

function _openEbayPopup(item, opts) {
  if (!item) return;
  const isVinyl = opts?.vinyl || false;

  const thumbImg = item.image_url || (item.all_images && item.all_images.length ? item.all_images[0] : "");

  const overlay = document.createElement("div");
  overlay.className = "buy-popup-overlay";
  overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };

  // Show popup shell — spinner only for signed-in users (who get live eBay data)
  const willFetchLive = !!window._clerk?.user && !!(item.item_id || item.ebay_item_id);
  overlay.innerHTML = `<div class="buy-popup">
    <button class="buy-popup-close" onclick="this.closest('.buy-popup-overlay').remove()">✕</button>
    ${thumbImg ? `<img class="buy-popup-main-img" src="${escHtml(thumbImg)}" onerror="this.style.display='none'">` : ""}
    <div class="buy-popup-body">
      <h3 class="buy-popup-title">${escHtml(item.title)}</h3>
      ${willFetchLive ? `<div class="buy-popup-loading" style="text-align:center;padding:1.5rem 0;color:#666;font-size:0.85rem">
        <div class="ebay-spinner"></div>
        Loading live data from eBay…
      </div>` : ""}
      <a class="buy-popup-ebay-link" href="${escHtml(item.item_url)}" target="_blank" rel="noopener">View on eBay →</a>
    </div>
  </div>`;

  document.body.appendChild(overlay);

  // Only fetch live eBay detail for signed-in users (saves API calls)
  const isSignedIn = !!window._clerk?.user;
  const itemId = item.item_id || item.ebay_item_id || "";
  if (isSignedIn && itemId) {
    _fetchEbayDetail(itemId, overlay, item, opts);
  } else {
    // Signed-out or no item ID — show DB data immediately (no spinner)
    _populatePopupFromDb(overlay, item);
  }
}

async function _fetchEbayDetail(itemId, overlay, dbItem, opts) {
  const loadingEl = overlay.querySelector(".buy-popup-loading");
  try {
    const r = await fetch(`/api/ebay/item/${encodeURIComponent(itemId)}`);
    if (!overlay.isConnected) return;
    if (r.status === 429) {
      // Rate limited — fall back to DB data
      _populatePopupFromDb(overlay, dbItem, "Daily eBay request limit reached.");
      return;
    }
    if (!r.ok) {
      _populatePopupFromDb(overlay, dbItem);
      return;
    }
    const d = await r.json();

    // Build full popup content from live eBay data
    const price = d.price > 0 ? d.price : parseFloat(dbItem.price);
    const currency = d.currency || dbItem.currency || "USD";
    const priceStr = price.toLocaleString("en-US", { style: "currency", currency });
    const condDesc = d.conditionDescription ? ` — ${d.conditionDescription}` : "";
    const location = d.location || [dbItem.location_city, dbItem.location_state, dbItem.location_country].filter(Boolean).join(", ");
    const seller = d.seller || dbItem.seller_name || dbItem.seller_username || "";
    const feedback = d.sellerFeedbackPercent ? `(${d.sellerFeedbackPercent}%)` : "";
    const allImages = d.allImages?.length ? d.allImages : (dbItem.all_images?.length ? dbItem.all_images : (dbItem.image_url ? [dbItem.image_url] : []));
    const itemUrl = d.itemUrl || dbItem.item_url || "";

    // Update main image
    let mainImg = overlay.querySelector(".buy-popup-main-img");
    if (allImages.length) {
      if (mainImg) {
        mainImg.src = allImages[0];
      } else {
        const img = document.createElement("img");
        img.className = "buy-popup-main-img";
        img.src = allImages[0];
        img.onerror = () => { img.style.display = "none"; };
        const closeBtn = overlay.querySelector(".buy-popup-close");
        if (closeBtn) closeBtn.after(img);
        mainImg = img;
      }
      // Gallery
      if (allImages.length > 1 && mainImg && !overlay.querySelector(".buy-popup-gallery")) {
        const gal = document.createElement("div");
        gal.className = "buy-popup-gallery";
        gal.innerHTML = allImages.map(u => `<img src="${escHtml(u)}" loading="lazy" onclick="this.parentElement.previousElementSibling.src='${escHtml(u)}'" onerror="this.style.display='none'">`).join("");
        mainImg.after(gal);
      }
    }

    // Build body content
    let metaHtml = "";
    metaHtml += `<div class="buy-popup-price">${priceStr} <span style="font-size:0.78rem;font-weight:400;color:#aaa;margin-left:0.5rem">Buy Now</span></div>`;
    if (condition) metaHtml += `<div class="buy-popup-meta">Condition: ${escHtml(condition)}${escHtml(condDesc)}</div>`;
    if (location) metaHtml += `<div class="buy-popup-meta">Location: ${escHtml(location)}</div>`;
    if (seller) metaHtml += `<div class="buy-popup-meta">Seller: ${escHtml(seller)} ${escHtml(feedback)}</div>`;

    // Specs — make Artist and Release Title rows clickable with search links
    const specKeys = Object.keys(d.specifics || {}).filter(k => d.specifics[k]);
    if (specKeys.length) {
      const isVinyl = opts?.vinyl || false;
      metaHtml += `<div class="buy-popup-specs">${specKeys.map(k => {
        const val = String(d.specifics[k]);
        const isArtistRow = isVinyl && /^artist$/i.test(k);
        const isReleaseRow = isVinyl && /^(Release Title|Album\/EP Name)$/i.test(k);
        if (isArtistRow) {
          const safeA = val.replace(/'/g, "\\'");
          return `<div class="buy-spec-row"><span class="buy-spec-label">${escHtml(k)}</span> <span><a href="#" class="modal-artist-link" data-artist="${escHtml(val)}" onclick="searchArtistFromBuyPopup(event,this)" title="Search for ${escHtml(val)}">${escHtml(val)}</a> <a href="#" class="album-title-search" onclick="searchCollectionFromBuyPopup(event,'cw-artist','${escHtml(safeA)}')" title="Search your collection for ${escHtml(val)}">⌕</a></span></div>`;
        }
        if (isReleaseRow) {
          const safeR = val.replace(/'/g, "\\'");
          return `<div class="buy-spec-row"><span class="buy-spec-label">${escHtml(k)}</span> <span><a href="#" class="modal-title-link" onclick="searchFromBuyPopup(event,'${escHtml(safeR)}')" title="Search SeaDisco for ${escHtml(val)}">${escHtml(val)}</a> <a href="#" class="album-title-search" onclick="searchCollectionFromBuyPopup(event,'cw-release','${escHtml(safeR)}')" title="Search your collection for ${escHtml(val)}">⌕</a></span></div>`;
        }
        return `<div class="buy-spec-row"><span class="buy-spec-label">${escHtml(k)}</span> <span>${escHtml(val)}</span></div>`;
      }).join("")}</div>`;
    }

    // Description
    if (d.description) {
      metaHtml += `<div class="buy-popup-description">${escHtml(typeof stripHtml === "function" ? stripHtml(d.description) : d.description)}</div>`;
    }

    // Replace loading state with full content
    if (loadingEl) loadingEl.remove();
    const ebayLink = overlay.querySelector(".buy-popup-ebay-link");
    if (ebayLink) {
      ebayLink.insertAdjacentHTML("beforebegin", metaHtml);
      if (itemUrl) ebayLink.href = itemUrl;
    }

    // Update in-memory item and visible card
    _updateItemInMemory(itemId, d);
  } catch {
    // Network error — fall back to DB data
    _populatePopupFromDb(overlay, dbItem);
  }
}

/** Fill popup with cached DB data (fallback when live fetch fails) */
function _populatePopupFromDb(overlay, item, notice) {
  const loadingEl = overlay.querySelector(".buy-popup-loading");
  const price = parseFloat(item.price);
  const priceStr = price.toLocaleString("en-US", { style: "currency", currency: item.currency || "USD" });
  const condition = item.condition || "";
  const loc = [item.location_city, item.location_state, item.location_country].filter(Boolean).join(", ");
  const seller = item.seller_name || item.seller_username || "";
  const feedback = item.seller_feedback_percent
    ? `(${item.seller_feedback_percent}%)`
    : (item.seller_feedback ? `(${item.seller_feedback.toLocaleString()} reviews)` : "");

  let html = "";
  html += `<div class="buy-popup-price">${priceStr} <span style="font-size:0.78rem;font-weight:400;color:#aaa;margin-left:0.5rem">Buy Now</span></div>`;
  if (condition) html += `<div class="buy-popup-meta">Condition: ${escHtml(condition)}</div>`;
  if (loc) html += `<div class="buy-popup-meta">Location: ${escHtml(loc)}</div>`;
  if (seller) html += `<div class="buy-popup-meta">Seller: ${escHtml(seller)} ${feedback}</div>`;
  if (notice) html += `<div style="color:#e88;font-size:0.78rem;margin-top:0.5rem">${escHtml(notice)}</div>`;

  if (loadingEl) loadingEl.remove();
  const ebayLink = overlay.querySelector(".buy-popup-ebay-link");
  if (ebayLink) ebayLink.insertAdjacentHTML("beforebegin", html);
}

function _updateItemInMemory(itemId, detail) {
  for (const list of [_buyItems, (window._gearItems || [])]) {
    const item = list.find(i => (i.item_id || i.ebay_item_id) === itemId);
    if (!item) continue;
    if (detail.price > 0)        item.price = detail.price;
    if (detail.currency)         item.currency = detail.currency;
    if (detail.condition)        item.condition = detail.condition;
    if (detail.allImages?.length) item.all_images = detail.allImages;
    // Refresh the visible card
    const idx = list.indexOf(item);
    const isBuy = list === _buyItems;
    const gridId = isBuy ? "buy-results" : "gear-results";
    const grid = document.getElementById(gridId);
    if (grid && idx >= 0 && grid.children[idx]) {
      const renderFn = isBuy ? renderBuyCard : (typeof renderGearCard === "function" ? renderGearCard : null);
      if (renderFn) {
        const tmp = document.createElement("div");
        tmp.innerHTML = renderFn(item, idx);
        grid.children[idx].replaceWith(tmp.firstElementChild);
      }
    }
  }
}

function renderBuyGrid() {
  const grid = document.getElementById("buy-results");
  if (!grid) return;
  if (!_buyItems.length) {
    grid.innerHTML = `<div style="color:var(--muted);font-size:0.8rem;grid-column:1/-1;text-align:center;padding:2rem 0">No vinyl listings available yet. Check back soon!</div>`;
    return;
  }
  grid.innerHTML = _buyItems.map((item, idx) => renderBuyCard(item, idx)).join("");
}

function refreshBuyListings() {
  _buyOffset = 0;
  _buyItems = [];
  loadBuyListings(false, true);
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

function clearBuyFilter() {
  const el = document.getElementById("buy-filter-field");
  if (el) { el.value = ""; el.focus(); }
  onBuySearch("");
}

async function loadBuyListings(append = false, bustCache = false) {
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
    const bust = bustCache ? `&_t=${Date.now()}` : "";
    const url = `/api/vinyl?min_price=${_buyMinPrice}&sort=${_buySort}${qEnc}&limit=${BUY_PAGE_SIZE}&offset=${_buyOffset}${bust}`;
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
        document.getElementById("buy-results").innerHTML = renderEmptyState("🎵", "No vinyl listings found", "Try adjusting your filters or check back later");
      } else {
        const newest = newItems.reduce((max, i) => i.fetched_at > max ? i.fetched_at : max, "");
        let ago = "";
        if (newest) {
          const mins = Math.round((Date.now() - new Date(newest).getTime()) / 60000);
          if (mins < 1) ago = "just now";
          else if (mins < 60) ago = `${mins} minute${mins !== 1 ? "s" : ""} ago`;
          else { const hrs = Math.round(mins / 60); ago = `${hrs} hour${hrs !== 1 ? "s" : ""} ago`; }
        }
        status.textContent = `${_buyTotal.toLocaleString()} listings · showing ${_buyItems.length}` +
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
