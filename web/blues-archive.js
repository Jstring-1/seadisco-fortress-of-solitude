// blues-archive.js — admin-only Discovery sub-view combining
// blues_artists, blues_lyrics, and the discogs_releases JSONB. Lazy-
// loaded by switchView('blues-archive'). All endpoints are gated by
// requireAdmin server-side; this module trusts that gate and just
// surfaces the responses.

let _baPage = 0;
const _BA_LIMIT = 100;
let _baTotal = 0;
let _baSearchTimer = null;
let _baCurrentArtistId = null;

function initBluesArchiveView() {
  // First entry — paint the list view, hide any leftover detail panel.
  const detail = document.getElementById("blues-archive-detail");
  if (detail) detail.style.display = "none";
  const list = document.querySelector("#blues-archive-view .blues-archive-list");
  if (list) list.style.display = "";
  _baLoadList();
}
window.initBluesArchiveView = initBluesArchiveView;

function _bluesArchiveDebouncedSearch() {
  if (_baSearchTimer) clearTimeout(_baSearchTimer);
  _baSearchTimer = setTimeout(() => { _baPage = 0; _baLoadList(); }, 280);
}
window._bluesArchiveDebouncedSearch = _bluesArchiveDebouncedSearch;

async function _baLoadList() {
  const rowsEl = document.getElementById("blues-archive-rows");
  const countEl = document.getElementById("blues-archive-count");
  if (!rowsEl) return;
  const q = (document.getElementById("blues-archive-search")?.value || "").trim();
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  params.set("limit", String(_BA_LIMIT));
  params.set("offset", String(_baPage * _BA_LIMIT));
  rowsEl.textContent = "Loading…";
  try {
    const r = await apiFetch(`/api/blues-archive/artists?${params}`);
    if (!r.ok) { rowsEl.innerHTML = `<p style="color:#e88">Failed: HTTP ${r.status}</p>`; return; }
    const { rows = [], total = 0 } = await r.json();
    _baTotal = total;
    if (countEl) countEl.textContent = total ? `${total.toLocaleString()} artist${total === 1 ? "" : "s"}` : "No artists yet.";
    if (!rows.length) {
      rowsEl.innerHTML = `<p style="color:var(--muted);padding:0.5rem 0">No matches.</p>`;
      _baRenderPager();
      return;
    }
    rowsEl.innerHTML = `
      <table class="api-log-table" style="font-size:0.86rem;width:100%">
        <thead><tr>
          <th>Name</th>
          <th>Dates</th>
          <th style="text-align:right">Lyrics</th>
          <th style="text-align:right">Releases</th>
        </tr></thead>
        <tbody>${rows.map(row => {
          const dates = [row.birth_date, row.death_date].filter(Boolean).join(" – ") || "—";
          return `<tr style="cursor:pointer" onclick="_baOpenArtist(${row.id})">
            <td style="font-weight:600;color:var(--text)">${escHtml(row.name || "")}</td>
            <td style="color:var(--muted);font-size:0.78rem">${escHtml(dates)}</td>
            <td style="text-align:right;color:${row.lyrics_count ? "var(--accent)" : "var(--muted)"}">${row.lyrics_count || ""}</td>
            <td style="text-align:right;color:${row.releases_count ? "var(--accent)" : "var(--muted)"}">${row.releases_count || ""}</td>
          </tr>`;
        }).join("")}</tbody>
      </table>`;
    _baRenderPager();
  } catch (e) {
    rowsEl.innerHTML = `<p style="color:#e88">Failed: ${escHtml(String(e?.message || e))}</p>`;
  }
}

function _baRenderPager() {
  const el = document.getElementById("blues-archive-pager");
  if (!el) return;
  const pageCount = Math.max(1, Math.ceil(_baTotal / _BA_LIMIT));
  const cur = _baPage + 1;
  if (pageCount <= 1) { el.innerHTML = ""; return; }
  el.innerHTML = `
    <button class="archive-btn" ${cur <= 1 ? "disabled" : ""} onclick="_baGoToPage(${_baPage - 1})">‹ Prev</button>
    <span style="color:var(--muted)">Page ${cur} / ${pageCount}</span>
    <button class="archive-btn" ${cur >= pageCount ? "disabled" : ""} onclick="_baGoToPage(${_baPage + 1})">Next ›</button>
  `;
}

function _baGoToPage(p) {
  _baPage = Math.max(0, p);
  _baLoadList();
}
window._baGoToPage = _baGoToPage;

async function _baOpenArtist(id) {
  _baCurrentArtistId = id;
  const detail = document.getElementById("blues-archive-detail");
  const list = document.querySelector("#blues-archive-view .blues-archive-list");
  if (!detail || !list) return;
  list.style.display = "none";
  detail.style.display = "block";
  detail.innerHTML = `<div style="padding:1rem;color:var(--muted)">Loading…</div>`;
  try {
    const r = await apiFetch(`/api/blues-archive/artists/${id}`);
    if (!r.ok) { detail.innerHTML = `<p style="color:#e88;padding:1rem">Failed: HTTP ${r.status}</p>`; return; }
    const a = await r.json();
    _baRenderArtistDetail(a);
  } catch (e) {
    detail.innerHTML = `<p style="color:#e88;padding:1rem">Failed: ${escHtml(String(e?.message || e))}</p>`;
  }
}
window._baOpenArtist = _baOpenArtist;

function _baRenderArtistDetail(a) {
  const detail = document.getElementById("blues-archive-detail");
  if (!detail) return;
  const dates = [a.birth_date, a.death_date].filter(Boolean).join(" – ");
  const bio = a.profile ? `<p style="font-size:0.86rem;line-height:1.5;color:var(--text);white-space:pre-wrap;margin:0.6rem 0">${escHtml(a.profile).slice(0, 4000)}</p>` : "";
  const photo = a.photo_url
    ? `<img src="${escHtml(a.photo_url)}" alt="" style="width:140px;height:140px;object-fit:cover;border-radius:4px;flex:0 0 auto" loading="lazy" />`
    : "";
  const meta = [
    a.birth_place ? `Born: ${escHtml(a.birth_place)}` : "",
    a.death_place ? `Died: ${escHtml(a.death_place)}` : "",
    a.hometown_region ? `From: ${escHtml(a.hometown_region)}` : "",
    a.first_recording_year ? `First recording: ${a.first_recording_year}` : "",
  ].filter(Boolean).join(" · ");
  const lyrics = Array.isArray(a.lyrics) ? a.lyrics : [];
  const releases = Array.isArray(a.releases) ? a.releases : [];
  const lyricsHtml = lyrics.length
    ? `<table class="api-log-table" style="font-size:0.84rem;width:100%">
        <thead><tr><th>Title</th><th>Tuning</th><th>Snippet</th></tr></thead>
        <tbody>${lyrics.map(l => `
          <tr style="cursor:pointer" onclick="_baOpenLyric(${l.id})">
            <td style="font-weight:600;color:var(--text);white-space:nowrap">${escHtml(l.page_title || "")}</td>
            <td style="white-space:nowrap;color:var(--accent)">${escHtml(l.tuning || "")}</td>
            <td style="font-size:0.76rem;color:#888">${escHtml((l.snippet || "").replace(/\s+/g, " ").slice(0, 140))}…</td>
          </tr>`).join("")}</tbody>
      </table>`
    : `<p style="color:var(--muted);font-style:italic;padding:0.4rem 0">No lyrics matched this artist's name. (Try Import from lyrics on the list page if you've just scraped.)</p>`;
  const releasesHtml = releases.length
    ? `<table class="api-log-table" style="font-size:0.84rem;width:100%">
        <thead><tr><th>Year</th><th>Title</th><th>Label</th><th>Type</th></tr></thead>
        <tbody>${releases.map(rel => {
          const type = String(rel.type || "release");
          const url  = `https://www.discogs.com/${type === "master" ? "master" : "release"}/${rel.id}`;
          const safeUrl = url.replace(/'/g, "\\'");
          return `<tr style="cursor:pointer" onclick="_baOpenRelease(${rel.id}, '${escHtml(type).replace(/'/g, "\\'")}', '${escHtml(safeUrl)}')">
            <td style="white-space:nowrap;color:var(--muted);font-variant-numeric:tabular-nums">${rel.year || "—"}</td>
            <td style="font-weight:600;color:var(--text)">${escHtml(rel.title || "")}</td>
            <td style="color:#888;font-size:0.78rem">${escHtml(rel.label || "")}</td>
            <td style="color:var(--accent);font-size:0.74rem;text-transform:uppercase">${escHtml(type)}</td>
          </tr>`;
        }).join("")}</tbody>
      </table>`
    : `<p style="color:var(--muted);font-style:italic;padding:0.4rem 0">No releases stored. Use the existing "Get all info from Discogs" button on the Blues DB tab to populate them.</p>`;
  detail.innerHTML = `
    <div style="display:flex;align-items:center;gap:0.6rem;margin-bottom:1rem;flex-wrap:wrap">
      <button class="archive-btn" onclick="_baBackToList()">‹ Back to list</button>
      <h2 style="margin:0;font-size:1.1rem">${escHtml(a.name || "")}</h2>
      <span style="color:var(--muted);font-size:0.82rem">${escHtml(dates)}</span>
    </div>
    <div style="display:flex;gap:1rem;margin-bottom:1.2rem;align-items:flex-start;flex-wrap:wrap">
      ${photo}
      <div style="flex:1;min-width:240px">
        ${meta ? `<div style="font-size:0.82rem;color:var(--muted);margin-bottom:0.4rem">${meta}</div>` : ""}
        ${bio}
      </div>
    </div>
    <h3 style="font-size:0.92rem;color:var(--accent);margin:1rem 0 0.5rem">Lyrics (${lyrics.length})</h3>
    ${lyricsHtml}
    <h3 style="font-size:0.92rem;color:var(--accent);margin:1.4rem 0 0.5rem">Releases — oldest to newest (${releases.length})</h3>
    ${releasesHtml}
  `;
}

function _baBackToList() {
  const detail = document.getElementById("blues-archive-detail");
  const list = document.querySelector("#blues-archive-view .blues-archive-list");
  if (detail) detail.style.display = "none";
  if (list) list.style.display = "";
  _baCurrentArtistId = null;
}
window._baBackToList = _baBackToList;

// Reuse the same lyric viewer the admin Lyrics tab uses. The admin
// view is on a different page (/admin), so we render a simple inline
// popup here using the existing chronam-style overlay pattern.
async function _baOpenLyric(id) {
  try {
    const r = await apiFetch(`/api/admin/lyrics/${id}`);
    if (!r.ok) return;
    const row = await r.json();
    let overlay = document.getElementById("ba-lyric-overlay");
    if (!overlay) {
      overlay = document.createElement("div");
      overlay.id = "ba-lyric-overlay";
      Object.assign(overlay.style, {
        position: "fixed", inset: "0", background: "rgba(0,0,0,0.78)",
        zIndex: "300", display: "flex", alignItems: "flex-start",
        justifyContent: "center", padding: "2rem 1rem", overflow: "auto",
      });
      overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
      document.body.appendChild(overlay);
    }
    overlay.innerHTML = `
      <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.2rem 1.4rem;width:min(820px,100%)">
        <div style="display:flex;justify-content:space-between;align-items:start;gap:0.6rem;margin-bottom:0.6rem">
          <div style="min-width:0">
            <h3 style="margin:0 0 0.25rem">${escHtml(row.page_title || "")}</h3>
            <div style="font-size:0.78rem;color:var(--muted)">${row.tuning ? `Tuning: ${escHtml(row.tuning)} · ` : ""}<a href="${escHtml(row.page_url || "")}" target="_blank" rel="noopener" style="color:var(--accent)">View on wiki ↗</a></div>
          </div>
          <button class="archive-btn" onclick="document.getElementById('ba-lyric-overlay')?.remove()" style="font-size:1.2rem;padding:0 0.6rem">×</button>
        </div>
        <pre style="white-space:pre-wrap;font-family:inherit;font-size:0.88rem;line-height:1.5;color:var(--text);max-height:60vh;overflow:auto;background:rgba(255,255,255,0.02);border:1px solid var(--border);border-radius:4px;padding:0.8rem 1rem;margin:0">${escHtml(row.plaintext || "(no plaintext)")}</pre>
      </div>
    `;
  } catch (e) {
    console.warn("[blues-archive] lyric open failed:", e);
  }
}
window._baOpenLyric = _baOpenLyric;

// Open a release in the same modal the main site uses (modal.js's
// openModal). Falls back to opening the Discogs page if the function
// isn't around.
function _baOpenRelease(id, type, discogsUrl) {
  if (typeof openModal === "function") {
    openModal(null, id, type, discogsUrl);
  } else {
    window.open(discogsUrl, "_blank", "noopener");
  }
}
window._baOpenRelease = _baOpenRelease;

// Admin button — import distinct lyrics-artist names that aren't yet
// in blues_artists. Idempotent (server checks LOWER(name) uniqueness).
async function bluesArchiveImport() {
  const btn = document.getElementById("blues-archive-import-btn");
  const statusEl = document.getElementById("blues-archive-import-status");
  if (!confirm("Walk the scraped lyrics and insert any artist names not already in the Blues DB?")) return;
  if (btn) btn.disabled = true;
  if (statusEl) statusEl.textContent = "Importing…";
  try {
    const r = await apiFetch("/api/blues-archive/import-from-lyrics", { method: "POST", timeoutMs: 60000 });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    if (statusEl) statusEl.innerHTML = `<span style="color:#4caf50">Imported</span> ${j.added} new of ${j.total} distinct (${j.existing} already in DB)`;
    _baLoadList();
  } catch (e) {
    if (statusEl) statusEl.textContent = `Import failed: ${e?.message || e}`;
  } finally {
    if (btn) btn.disabled = false;
  }
}
window.bluesArchiveImport = bluesArchiveImport;
