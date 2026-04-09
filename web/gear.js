// ── Gear tab — eBay vintage electronics Buy-It-Now listings ───────────
let _gearItems = [];
let _gearTotal = 0;
let _gearMinPrice = 0;
let _gearSort = "price_desc";
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

  // Show condition only if it's notable (not just "Used")
  const conditionShow = condition && condition !== "Used" ? condition : "";

  return `<div class="card gear-card card-animate" onclick="event.stopPropagation();openGearPopup(${idx})" role="button" tabindex="0" style="--i:${Math.min(idx, 20)};cursor:pointer;-webkit-tap-highlight-color:transparent" title="${escHtml(item.title)}">
    <div class="card-thumb-wrap" style="pointer-events:none">${img}</div>
    <div class="card-body" style="pointer-events:none">
      <div class="card-title">${escHtml(item.title)}</div>
      <div class="gear-price">${priceStr}</div>
      ${conditionShow ? `<div class="card-meta">${escHtml(conditionShow)}</div>` : ""}
      ${loc ? `<div class="card-meta">${escHtml(loc)}</div>` : ""}
    </div>
  </div>`;
}

function openGearPopup(idx) {
  const item = _gearItems[idx];
  if (!item) return;
  // Reuse the shared eBay popup (fetches live detail with images, specs, description)
  _openEbayPopup(item);
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

function refreshGearListings() {
  _gearOffset = 0;
  _gearItems = [];
  loadGearListings(false, true);
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

function clearGearFilter() {
  const el = document.querySelector(".gear-search-field");
  if (el) { el.value = ""; el.focus(); }
  onGearSearch("");
}

async function loadGearListings(append = false, bustCache = false) {
  if (_gearLoading) return;
  _gearLoading = true;

  const status = document.getElementById("gear-status");
  const loadMore = document.getElementById("gear-load-more");

  if (!append) {
    _gearOffset = 0;
    _gearItems = [];
    if (status) status.textContent = "";
    document.getElementById("gear-results").innerHTML = renderSkeletonGrid(16);
  } else {
    if (loadMore) loadMore.textContent = "Loading…";
  }

  try {
    const qEnc = _gearQuery ? `&q=${encodeURIComponent(_gearQuery)}` : "";
    const bust = bustCache ? `&_t=${Date.now()}` : "";
    const url = `/api/gear?min_price=${_gearMinPrice}&sort=${_gearSort}${qEnc}&limit=${GEAR_PAGE_SIZE}&offset=${_gearOffset}${bust}`;
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
        status.textContent = "";
        document.getElementById("gear-results").innerHTML = renderEmptyState("🎛️", "No gear listings found", "Try adjusting your filters or check back later");
      } else {
        const newest = newItems.reduce((max, i) => i.fetched_at > max ? i.fetched_at : max, "");
        let ago = "";
        if (newest) {
          const mins = Math.round((Date.now() - new Date(newest).getTime()) / 60000);
          if (mins < 1) ago = "just now";
          else if (mins < 60) ago = `${mins} minute${mins !== 1 ? "s" : ""} ago`;
          else { const hrs = Math.round(mins / 60); ago = `${hrs} hour${hrs !== 1 ? "s" : ""} ago`; }
        }
        status.textContent = `${_gearTotal.toLocaleString()} listings · showing ${_gearItems.length}` +
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
    showToast("Failed to load gear listings — please try again", "error");
  }
  _gearLoading = false;
}

function loadMoreGear() {
  _gearOffset += GEAR_PAGE_SIZE;
  loadGearListings(true);
}
