// ── Gear tab — eBay vintage electronics scroller ─────────────────────────
let _gearItems = [];
let _gearTotal = 0;
let _gearMinPrice = 0;
let _gearSort = "ending";
let _gearQuery = "";
let _gearOffset = 0;
let _gearLoading = false;
let _gearDebounce = null;
const GEAR_PAGE_SIZE = 200;

function renderGearCard(item, idx) {
  const img = item.image_url
    ? `<img src="${escHtml(item.image_url)}" alt="${escHtml(item.title)}" loading="lazy" onerror="this.style.display='none'">`
    : `<div class="thumb-placeholder">⚙</div>`;

  const price = parseFloat(item.price);
  const priceStr = price.toLocaleString("en-US", { style: "currency", currency: item.currency || "USD" });

  const condition = item.condition || "";
  const loc = [item.location_city, item.location_state].filter(Boolean).join(", ");

  const bids = item.bid_count ?? 0;
  const bidStr = `${bids} bid${bids !== 1 ? "s" : ""}`;

  const specifics = item.item_specifics ?? {};
  const brand = specifics.Brand || specifics.brand || "";
  const model = specifics.Model || specifics.model || "";
  const brandModel = [brand, model].filter(Boolean).join(" ");

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

  // Show condition only if it's notable (not just "Used")
  const conditionShow = condition && condition !== "Used" ? condition : "";

  return `<div class="card gear-card" onclick="event.stopPropagation();openGearPopup(${idx})" role="button" tabindex="0" style="cursor:pointer;-webkit-tap-highlight-color:transparent" title="${escHtml(item.title)}">
    <div class="card-thumb-wrap" style="pointer-events:none">${img}</div>
    <div class="card-body" style="pointer-events:none">
      <div class="card-title">${escHtml(item.title.length > 65 ? item.title.slice(0, 63) + "…" : item.title)}</div>
      ${brandModel ? `<div class="card-sub" style="color:#aaa">${escHtml(brandModel)}</div>` : ""}
      <div class="gear-price">${priceStr}</div>
      <div class="card-sub gear-bids">${escHtml(bidStr)}</div>
      ${conditionShow ? `<div class="card-meta">${escHtml(conditionShow)}</div>` : ""}
      ${loc ? `<div class="card-meta">${escHtml(loc)}</div>` : ""}
      ${timeLeft ? `<div class="card-meta gear-time-left">${timeLeft}</div>` : ""}
    </div>
  </div>`;
}

function openGearPopup(idx) {
  const item = _gearItems[idx];
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

  // Build specifics list
  const specKeys = Object.keys(specifics).filter(k => specifics[k]);
  const specsHtml = specKeys.length
    ? `<div class="gear-popup-specs">${specKeys.map(k => `<div class="gear-spec-row"><span class="gear-spec-label">${escHtml(k)}</span> <span>${escHtml(String(specifics[k]))}</span></div>`).join("")}</div>`
    : "";

  // Build image gallery
  const galleryHtml = allImages.length > 1
    ? `<div class="gear-popup-gallery">${allImages.map(u => `<img src="${escHtml(u)}" loading="lazy" onclick="this.parentElement.previousElementSibling.src='${escHtml(u)}'" onerror="this.style.display='none'">`).join("")}</div>`
    : "";

  // Detail HTML (from getItem)
  const detailHtml = item.detail_html
    ? `<div class="gear-popup-description">${item.detail_html}</div>`
    : "";

  const overlay = document.createElement("div");
  overlay.className = "gear-popup-overlay";
  overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };

  overlay.innerHTML = `<div class="gear-popup">
    <button class="gear-popup-close" onclick="this.closest('.gear-popup-overlay').remove()">✕</button>
    ${allImages.length ? `<img class="gear-popup-main-img" src="${escHtml(allImages[0])}" onerror="this.style.display='none'">` : ""}
    ${galleryHtml}
    <div class="gear-popup-body">
      <h3 class="gear-popup-title">${escHtml(item.title)}</h3>
      <div class="gear-popup-price">${priceStr} <span style="font-size:0.78rem;font-weight:400;color:#aaa;margin-left:0.5rem">${escHtml(buyType)}</span></div>
      ${endDate ? `<div class="gear-popup-meta">Ends ${endDate}</div>` : ""}
      ${condition ? `<div class="gear-popup-meta">Condition: ${escHtml(condition)}</div>` : ""}
      ${loc ? `<div class="gear-popup-meta">Location: ${escHtml(loc)}</div>` : ""}
      ${seller ? `<div class="gear-popup-meta">Seller: ${escHtml(seller)} ${feedback}</div>` : ""}
      ${specsHtml}
      ${detailHtml}
      <a class="gear-popup-ebay-link" href="${escHtml(item.item_url)}" target="_blank" rel="noopener">View on eBay →</a>
    </div>
  </div>`;

  document.body.appendChild(overlay);
}

function renderGearGrid() {
  const grid = document.getElementById("gear-results");
  if (!grid) return;
  if (!_gearItems.length) {
    grid.innerHTML = `<div style="color:var(--muted);font-size:0.8rem;grid-column:1/-1;text-align:center;padding:2rem 0">No gear listings available yet. Check back soon!</div>`;
    return;
  }
  grid.innerHTML = _gearItems.map((item, idx) => renderGearCard(item, idx)).join("");
}

function setGearPriceFilter(minPrice) {
  _gearMinPrice = minPrice;
  _gearOffset = 0;
  _gearItems = [];
  document.querySelectorAll(".gear-price-pill").forEach(p =>
    p.classList.toggle("active", parseInt(p.dataset.min) === minPrice)
  );
  loadGearListings();
}

function setGearSort(sort) {
  _gearSort = sort;
  _gearOffset = 0;
  _gearItems = [];
  loadGearListings();
}

function onGearSearch(val) {
  clearTimeout(_gearDebounce);
  _gearDebounce = setTimeout(() => {
    _gearQuery = val;
    _gearOffset = 0;
    _gearItems = [];
    loadGearListings();
  }, 350);
}

async function loadGearListings(append = false) {
  if (_gearLoading) return;
  _gearLoading = true;

  const status = document.getElementById("gear-status");
  const loadMore = document.getElementById("gear-load-more");

  if (!append) {
    _gearOffset = 0;
    _gearItems = [];
    if (status) status.textContent = "Loading gear…";
  } else {
    if (loadMore) loadMore.textContent = "Loading…";
  }

  try {
    const qEnc = _gearQuery ? `&q=${encodeURIComponent(_gearQuery)}` : "";
    const url = `/api/gear?min_price=${_gearMinPrice}&sort=${_gearSort}${qEnc}&limit=${GEAR_PAGE_SIZE}&offset=${_gearOffset}`;
    const r = await fetch(url);
    const data = await r.json();
    const newItems = data.items ?? [];
    _gearTotal = data.total ?? 0;

    if (append) {
      _gearItems = _gearItems.concat(newItems);
    } else {
      _gearItems = newItems;
    }

    renderGearGrid();

    if (status) {
      if (!_gearItems.length) {
        status.textContent = "No listings found at this price point.";
      } else {
        const newest = newItems.reduce((max, i) => i.fetched_at > max ? i.fetched_at : max, "");
        let ago = "";
        if (newest) {
          const mins = Math.round((Date.now() - new Date(newest).getTime()) / 60000);
          if (mins < 1) ago = "just now";
          else if (mins < 60) ago = `${mins} minute${mins !== 1 ? "s" : ""} ago`;
          else { const hrs = Math.round(mins / 60); ago = `${hrs} hour${hrs !== 1 ? "s" : ""} ago`; }
        }
        status.textContent = `${_gearTotal.toLocaleString()} auctions · showing ${_gearItems.length}` +
          (ago ? ` · updated ${ago}` : "");
      }
    }

    if (loadMore) {
      if (_gearItems.length < _gearTotal) {
        loadMore.style.display = "";
        loadMore.textContent = `Load more (${(_gearTotal - _gearItems.length).toLocaleString()} remaining)`;
      } else {
        loadMore.style.display = "none";
      }
    }
  } catch (e) {
    if (status) status.textContent = "Failed to load gear listings.";
  }
  _gearLoading = false;
}

function loadMoreGear() {
  _gearOffset += GEAR_PAGE_SIZE;
  loadGearListings(true);
}
