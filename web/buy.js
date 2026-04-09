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
      <div class="buy-price">${priceStr}</div>
      <div class="card-title">${escHtml(item.title)}</div>
      ${artistLabel ? `<div class="card-sub" style="color:#aaa">${escHtml(artistLabel)}</div>` : ""}
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

  // For vinyl, make the title clickable to search SeaDisco for it
  const safeTitle = String(item.title || "").replace(/'/g, "\\'");
  const titleInner = isVinyl
    ? `<a href="#" class="modal-title-link" onclick="searchFromBuyPopup(event,'${escHtml(safeTitle)}')" title="Search SeaDisco for this">${escHtml(item.title)}</a>`
    : escHtml(item.title);

  // Show popup shell — spinner only for signed-in users (who get live eBay data)
  const willFetchLive = !!window._clerk?.user && !!(item.item_id || item.ebay_item_id);
  overlay.innerHTML = `<div class="buy-popup">
    <button class="buy-popup-close" onclick="this.closest('.buy-popup-overlay').remove()">✕</button>
    ${thumbImg ? `<img class="buy-popup-main-img" src="${escHtml(thumbImg)}" onerror="this.style.display='none'">` : ""}
    <div class="buy-popup-body">
      <h3 class="buy-popup-title">${titleInner}</h3>
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
    const condition = d.condition || dbItem.condition || "";
    const condDesc = d.conditionDescription ? ` — ${d.conditionDescription}` : "";
    const location = d.location || [dbItem.location_city, dbItem.location_state, dbItem.location_country].filter(Boolean).join(", ");
    const seller = d.seller || dbItem.seller_name || dbItem.seller_username || "";
    const feedback = d.sellerFeedbackPercent ? `(${d.sellerFeedbackPercent}%)` : "";
    const allImages = d.allImages?.length ? d.allImages : (dbItem.all_images?.length ? dbItem.all_images : (dbItem.image_url ? [dbItem.image_url] : []));
    const itemUrl = d.itemUrl || dbItem.item_url || "";

    // Shipping
    const shippingFree = d.shippingFree === true;
    const shippingCost = parseFloat(d.shippingCost ?? "0");
    const shippingCur  = d.shippingCurrency || currency;
    const shippingStr  = shippingFree
      ? "Free shipping"
      : (shippingCost > 0 ? `+${shippingCost.toLocaleString("en-US", { style: "currency", currency: shippingCur })} shipping` : "");

    // Returns
    const returnsAccepted = d.returnsAccepted === true;
    const returnPeriod = d.returnPeriod || "";
    const returnsStr = returnsAccepted
      ? (returnPeriod ? `${returnPeriod} returns` : "Returns accepted")
      : (d.returnsAccepted === false ? "No returns" : "");

    // Stock
    const qtyAvail = parseInt(d.quantityAvailable ?? "0");
    const qtySold  = parseInt(d.quantitySold ?? "0");

    // Listed date
    const listedDate = d.itemCreationDate
      ? new Date(d.itemCreationDate).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })
      : "";

    // Top-rated badge
    const topRated = d.topRatedBuyingExperience === true;

    // Category path (e.g. "Music > Records > Rock")
    const categoryPath = (d.categoryPath || "").replace(/\|/g, " › ");

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

    // Price block — inserted BEFORE the title so it appears at the top of the popup body
    const priceHtml = `<div class="buy-popup-price">${priceStr} <span style="font-size:0.78rem;font-weight:400;color:#aaa;margin-left:0.5rem">Buy Now</span>${shippingStr ? ` <span style="font-size:0.72rem;font-weight:400;color:#888;margin-left:0.4rem">${escHtml(shippingStr)}</span>` : ""}</div>`;

    // Build body content (price already inserted above title; this goes after title)
    let metaHtml = "";
    if (d.subtitle) metaHtml += `<div class="buy-popup-subtitle" style="color:#bbb;font-size:0.82rem;margin-top:-0.2rem;margin-bottom:0.4rem">${escHtml(d.subtitle)}</div>`;
    if (categoryPath) metaHtml += `<div class="buy-popup-meta" style="font-size:0.7rem;color:#667;letter-spacing:0.02em">${escHtml(categoryPath)}</div>`;
    if (condition) metaHtml += `<div class="buy-popup-meta">Condition: ${escHtml(condition)}${escHtml(condDesc)}</div>`;
    if (returnsStr) metaHtml += `<div class="buy-popup-meta">${escHtml(returnsStr)}</div>`;
    // Stock line — only if meaningful
    if (qtyAvail > 1 || qtySold > 0) {
      const stockParts = [];
      if (qtyAvail > 1) stockParts.push(`${qtyAvail} available`);
      if (qtySold > 0)  stockParts.push(`${qtySold} sold`);
      metaHtml += `<div class="buy-popup-meta">${escHtml(stockParts.join(" · "))}</div>`;
    }
    if (location) metaHtml += `<div class="buy-popup-meta">Location: ${escHtml(location)}</div>`;
    if (seller) {
      const topRatedBadge = topRated ? ` <span style="color:#daa520" title="eBay Top Rated Seller">⭐ Top Rated</span>` : "";
      metaHtml += `<div class="buy-popup-meta">Seller: ${escHtml(seller)} ${escHtml(feedback)}${topRatedBadge}</div>`;
    }
    if (listedDate) metaHtml += `<div class="buy-popup-meta" style="color:#667;font-size:0.7rem">Listed ${escHtml(listedDate)}</div>`;

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

    // Insert price ABOVE the title
    const titleEl = overlay.querySelector(".buy-popup-title");
    if (titleEl) titleEl.insertAdjacentHTML("beforebegin", priceHtml);

    // Insert rest of metadata BEFORE the "View on eBay" link (below the title)
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

  const priceHtml = `<div class="buy-popup-price">${priceStr} <span style="font-size:0.78rem;font-weight:400;color:#aaa;margin-left:0.5rem">Buy Now</span></div>`;

  let metaHtml = "";
  if (condition) metaHtml += `<div class="buy-popup-meta">Condition: ${escHtml(condition)}</div>`;
  if (loc) metaHtml += `<div class="buy-popup-meta">Location: ${escHtml(loc)}</div>`;
  if (seller) metaHtml += `<div class="buy-popup-meta">Seller: ${escHtml(seller)} ${feedback}</div>`;
  if (notice) metaHtml += `<div style="color:#e88;font-size:0.78rem;margin-top:0.5rem">${escHtml(notice)}</div>`;

  if (loadingEl) loadingEl.remove();

  // Price above title
  const titleEl = overlay.querySelector(".buy-popup-title");
  if (titleEl) titleEl.insertAdjacentHTML("beforebegin", priceHtml);

  // Rest of meta below title, before ebay link
  const ebayLink = overlay.querySelector(".buy-popup-ebay-link");
  if (ebayLink) ebayLink.insertAdjacentHTML("beforebegin", metaHtml);
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
