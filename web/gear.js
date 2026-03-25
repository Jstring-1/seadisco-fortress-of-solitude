// ── Gear tab — eBay vintage electronics scroller ─────────────────────────
let _gearItems = [];
let _gearTotal = 0;
let _gearMinPrice = 100;
let _gearOffset = 0;
let _gearLoading = false;
const GEAR_PAGE_SIZE = 200;

function renderGearCard(item) {
  const img = item.image_url
    ? `<img src="${escHtml(item.image_url)}" alt="${escHtml(item.title)}" loading="lazy" onerror="this.style.display='none'">`
    : `<div class="thumb-placeholder">⚙</div>`;

  const price = parseFloat(item.price);
  const priceStr = price.toLocaleString("en-US", { style: "currency", currency: item.currency || "USD" });

  const condition = item.condition || "";
  const loc = [item.location_city, item.location_state].filter(Boolean).join(", ");

  const buyType = (item.buying_options ?? []).includes("AUCTION")
    ? `Auction${item.bid_count > 0 ? ` · ${item.bid_count} bid${item.bid_count !== 1 ? "s" : ""}` : ""}`
    : "Buy Now";

  const specifics = item.item_specifics ?? {};
  const brand = specifics.Brand || specifics.brand || "";
  const model = specifics.Model || specifics.model || "";
  const brandModel = [brand, model].filter(Boolean).join(" ");

  const endDate = item.item_end_date
    ? new Date(item.item_end_date).toLocaleDateString("en-US", { month: "short", day: "numeric" })
    : "";

  return `<a class="card gear-card" href="${escHtml(item.item_url)}" target="_blank" rel="noopener" title="${escHtml(item.title)}">
    <div class="card-thumb-wrap">${img}</div>
    <div class="card-body">
      <div class="card-title">${escHtml(item.title.length > 65 ? item.title.slice(0, 63) + "…" : item.title)}</div>
      ${brandModel ? `<div class="card-sub" style="color:#aaa">${escHtml(brandModel)}</div>` : ""}
      <div class="gear-price">${priceStr}</div>
      <div class="card-sub">${escHtml(buyType)}</div>
      ${condition ? `<div class="card-meta">${escHtml(condition)}</div>` : ""}
      ${loc ? `<div class="card-meta">${escHtml(loc)}</div>` : ""}
      ${endDate ? `<div class="card-meta">Ends ${endDate}</div>` : ""}
    </div>
  </a>`;
}

function renderGearGrid() {
  const grid = document.getElementById("gear-results");
  if (!grid) return;
  if (!_gearItems.length) {
    grid.innerHTML = `<div style="color:var(--muted);font-size:0.8rem;grid-column:1/-1;text-align:center;padding:2rem 0">No gear listings available yet. Check back soon!</div>`;
    return;
  }
  grid.innerHTML = _gearItems.map(renderGearCard).join("");
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
    const url = `/api/gear?min_price=${_gearMinPrice}&limit=${GEAR_PAGE_SIZE}&offset=${_gearOffset}`;
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
