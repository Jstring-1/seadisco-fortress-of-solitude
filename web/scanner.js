// SeaDisco barcode scanner.
//
// Lazy-loaded by shared.js's _sdLoadModule when the user taps the
// ▮ scan button. Phase 1: admin-only, both camera + manual-entry
// available. Camera path uses the browser-native BarcodeDetector
// (Chromium / Android Chrome / Edge — no extra bundle bytes).
// Browsers without it (Safari, Firefox) fall through to manual-
// entry only; a ZXing fallback can be wired in Phase 2 if needed.
//
// Result handoff: on a successful scan / submit, _sdRunBarcodeSearch
// (defined in search.js) is invoked with the cleaned barcode digits.

let _sdScanStream = null;       // active MediaStream so we can stop tracks on close
let _sdScanRafId  = null;        // requestAnimationFrame handle for the detect loop
let _sdScanDetector = null;      // BarcodeDetector instance (when available)
let _sdScanLastHit  = 0;         // ms timestamp of the last successful match — debounce
let _sdScanActive   = false;     // overlay open flag

function _sdScanEl(id) { return document.getElementById(id); }

// Strip user-entered formatting from a barcode field. Discogs's
// /database/search?barcode= matches the literal stored string, but
// most barcodes are stored as digits-only. Trim spaces, dashes, and
// non-digit junk so a user can paste "0 75678 13192 9" or whatever
// and still get a hit.
function _sdScanCleanCode(raw) {
  if (!raw) return "";
  return String(raw).replace(/[^0-9]/g, "");
}

async function _sdOpenScanner() {
  const overlay = _sdScanEl("barcode-scan-overlay");
  if (!overlay) return;
  overlay.style.display = "flex";
  if (typeof _sdLockBodyScroll === "function") _sdLockBodyScroll("scanner");
  _sdScanActive = true;
  // Reset prior state so reopening the overlay doesn't carry stale
  // status text or input value.
  const input = _sdScanEl("barcode-scan-input");
  if (input) { input.value = ""; setTimeout(() => input.focus(), 50); }
  const status = _sdScanEl("barcode-scan-camera-status");
  if (status) status.textContent = "";
  // Try the camera path. If anything fails we silently leave the
  // camera UI hidden and the manual-entry input is still usable.
  await _sdScanStartCamera();
}
window._sdOpenScanner = _sdOpenScanner;

function _sdCloseScanner() {
  _sdScanActive = false;
  const overlay = _sdScanEl("barcode-scan-overlay");
  if (overlay) overlay.style.display = "none";
  if (typeof _sdUnlockBodyScroll === "function") _sdUnlockBodyScroll("scanner");
  _sdScanStopCamera();
}
window._sdCloseScanner = _sdCloseScanner;

async function _sdScanStartCamera() {
  const wrap = _sdScanEl("barcode-scan-camera-wrap");
  const video = _sdScanEl("barcode-scan-video");
  const status = _sdScanEl("barcode-scan-camera-status");
  if (!wrap || !video) return;
  // BarcodeDetector is the cheap path — Chromium-only but covers
  // most mobile traffic. If the browser lacks it, hide the camera
  // section entirely; the manual-entry input handles those cases.
  if (typeof window.BarcodeDetector !== "function") {
    wrap.style.display = "none";
    if (status) status.textContent = "Camera scan needs Chrome / Android Chrome / Edge. Type the digits below to look up.";
    return;
  }
  // navigator.mediaDevices is gated on a secure context (https or
  // localhost) — production seadisco.com is HTTPS so this should
  // always work. Belt-and-suspenders check anyway.
  if (!navigator.mediaDevices?.getUserMedia) {
    wrap.style.display = "none";
    if (status) status.textContent = "Camera unavailable in this browser.";
    return;
  }
  try {
    _sdScanDetector = new BarcodeDetector({
      // Vinyl / CD barcodes are typically EAN_13 or UPC_A. Include
      // the rest of the 1D set so unusual pressings (sticker
      // overlays, promo barcodes) still parse.
      formats: ["ean_13", "ean_8", "upc_a", "upc_e", "code_128", "code_39", "itf"],
    });
  } catch (e) {
    wrap.style.display = "none";
    if (status) status.textContent = "Scanner init failed.";
    console.warn("[scanner] BarcodeDetector init failed:", e);
    return;
  }
  try {
    _sdScanStream = await navigator.mediaDevices.getUserMedia({
      video: {
        facingMode: { ideal: "environment" },  // rear camera on phones
        width:  { ideal: 1280 },
        height: { ideal: 720 },
      },
      audio: false,
    });
  } catch (e) {
    wrap.style.display = "none";
    if (status) {
      if (e?.name === "NotAllowedError") {
        status.textContent = "Camera permission denied. You can still type the barcode below.";
      } else if (e?.name === "NotFoundError") {
        status.textContent = "No camera detected. Type the barcode below.";
      } else {
        status.textContent = "Camera unavailable. Type the barcode below.";
      }
    }
    console.warn("[scanner] getUserMedia failed:", e?.name || e);
    return;
  }
  video.srcObject = _sdScanStream;
  wrap.style.display = "";
  // Some browsers won't actually start playing on srcObject assignment
  // alone — a defensive .play() covers that and is a no-op when the
  // stream is already running.
  try { await video.play(); } catch {}
  _sdScanDetectLoop();
}

function _sdScanStopCamera() {
  if (_sdScanRafId) {
    cancelAnimationFrame(_sdScanRafId);
    _sdScanRafId = null;
  }
  if (_sdScanStream) {
    try { _sdScanStream.getTracks().forEach(t => t.stop()); } catch {}
    _sdScanStream = null;
  }
  const video = _sdScanEl("barcode-scan-video");
  if (video) video.srcObject = null;
  _sdScanDetector = null;
}

async function _sdScanDetectLoop() {
  if (!_sdScanActive || !_sdScanDetector) return;
  const video = _sdScanEl("barcode-scan-video");
  if (!video || video.readyState < 2) {
    _sdScanRafId = requestAnimationFrame(_sdScanDetectLoop);
    return;
  }
  try {
    const codes = await _sdScanDetector.detect(video);
    if (codes && codes.length) {
      // Debounce: ignore back-to-back hits within 1.5s so a stable
      // barcode doesn't fire ten searches per second.
      const now = Date.now();
      if (now - _sdScanLastHit > 1500) {
        _sdScanLastHit = now;
        const raw = codes[0]?.rawValue || "";
        const cleaned = _sdScanCleanCode(raw);
        if (cleaned) {
          // Light haptic feedback on supporting devices.
          try { navigator.vibrate?.(40); } catch {}
          _sdScanHandoff(cleaned);
          return;
        }
      }
    }
  } catch (e) {
    // Detection failures are noisy on some devices; don't toast.
    console.debug("[scanner] detect threw:", e);
  }
  _sdScanRafId = requestAnimationFrame(_sdScanDetectLoop);
}

// Manual-entry submit handler. Reads the input field, cleans it,
// and routes through the same handoff. Wired to the Search button +
// Enter on the input.
function _sdSubmitBarcodeManual() {
  const input = _sdScanEl("barcode-scan-input");
  if (!input) return;
  const cleaned = _sdScanCleanCode(input.value);
  const status = _sdScanEl("barcode-scan-camera-status");
  if (!cleaned) {
    if (status) status.textContent = "Enter at least one digit.";
    return;
  }
  _sdScanHandoff(cleaned);
}
window._sdSubmitBarcodeManual = _sdSubmitBarcodeManual;

// Common path for camera + manual submissions. Closes the overlay,
// runs the search through search.js's barcode entry point, and
// reflects the URL state. The search-side helper handles result
// rendering, empty states, and the heading.
function _sdScanHandoff(cleaned) {
  _sdCloseScanner();
  if (typeof window._sdRunBarcodeSearch === "function") {
    window._sdRunBarcodeSearch(cleaned);
  } else {
    // Defensive fallback — should never hit in practice since
    // search.js is eager-loaded.
    console.warn("[scanner] _sdRunBarcodeSearch missing");
  }
}

// Wire Enter on the manual-entry input to the Search button. Done
// at script-load time because the input lives in index.html and is
// always in the DOM.
document.addEventListener("DOMContentLoaded", () => {
  const input = _sdScanEl("barcode-scan-input");
  if (!input) return;
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      _sdSubmitBarcodeManual();
    }
  });
});
// In case the script loads after DOMContentLoaded (lazy-load path),
// fire the input wiring immediately.
if (document.readyState !== "loading") {
  const input = _sdScanEl("barcode-scan-input");
  if (input && !input.dataset.sdWired) {
    input.dataset.sdWired = "1";
    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        _sdSubmitBarcodeManual();
      }
    });
  }
}
