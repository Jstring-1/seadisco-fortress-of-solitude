// ── Admin coverage-sweep worker routes ───────────────────────────
//
// Start/stop/force-clear/status for the three background coverage
// workers (artist masters+ sweep, year×facet sweep, label upstream
// stats). Extracted verbatim from search-api.ts as the first slice of
// splitting that 17k-line file into per-domain routers. These handlers
// are pure pass-throughs to the imported worker modules, so they carry
// zero coupling to search-api's module state — `requireAdmin` is passed
// in (rather than imported) to avoid an import cycle.
//
// Register order is preserved by calling registerAdminWorkerSweepRoutes
// at the same position the routes used to occupy.
import express from "express";
import { startArtistSweep, requestArtistSweepStop, forceClearArtistSweep, getArtistSweepStatus, } from "../artist-sweep-worker.js";
import { startFacetedSweep, requestFacetedSweepStop, forceClearFacetedSweep, getFacetedSweepStatus, } from "../faceted-sweep-worker.js";
import { startLabelUpstreamStats, requestLabelUpstreamStatsStop, forceClearLabelUpstreamStats, getLabelUpstreamStatsStatus, } from "../label-upstream-stats-worker.js";
import { startBulkLabelSweep, requestBulkLabelSweepStop, forceClearBulkLabelSweep, getBulkLabelSweepStatus, } from "../label-bulk-sweep-worker.js";
export function registerAdminWorkerSweepRoutes(app, requireAdmin) {
    // ── Artist masters+ sweep ────────────────────────────────────────
    app.post("/api/admin/artist-sweep/start", express.json({ limit: "1kb" }), async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        const body = req.body || {};
        const yearMax = Number.isFinite(Number(body.yearMax)) ? Number(body.yearMax) : undefined;
        const resetCursor = !!body.resetCursor;
        try {
            const r = await startArtistSweep({ yearMax, resetCursor });
            if (!r.ok) {
                res.status(409).json(r);
                return;
            }
            res.json(r);
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.post("/api/admin/artist-sweep/stop", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            requestArtistSweepStop();
            res.json({ ok: true });
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.post("/api/admin/artist-sweep/force-clear", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            forceClearArtistSweep();
            res.json({ ok: true });
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.get("/api/admin/artist-sweep/status", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            res.json(getArtistSweepStatus());
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    // ── Year × facet (format / country) sweep ────────────────────────
    app.post("/api/admin/faceted-sweep/start", express.json({ limit: "2kb" }), async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        const body = req.body || {};
        const mode = String(body.mode || "").toLowerCase();
        if (mode !== "format" && mode !== "country") {
            res.status(400).json({ error: "mode must be 'format' or 'country'" });
            return;
        }
        const yearFrom = Number.isFinite(Number(body.yearFrom)) ? Number(body.yearFrom) : undefined;
        const yearTo = Number.isFinite(Number(body.yearTo)) ? Number(body.yearTo) : undefined;
        const values = Array.isArray(body.values) ? body.values.map(String).filter(Boolean) : undefined;
        const resetCursor = !!body.resetCursor;
        try {
            const r = await startFacetedSweep({ mode: mode, yearFrom, yearTo, values, resetCursor });
            if (!r.ok) {
                res.status(409).json(r);
                return;
            }
            res.json(r);
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.post("/api/admin/faceted-sweep/stop", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            requestFacetedSweepStop();
            res.json({ ok: true });
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.post("/api/admin/faceted-sweep/force-clear", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            forceClearFacetedSweep();
            res.json({ ok: true });
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.get("/api/admin/faceted-sweep/status", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            res.json(getFacetedSweepStatus());
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    // ── Label upstream stats ─────────────────────────────────────────
    // Background worker: for each known label (external_discography row
    // with a Discogs id), fetches /labels/{id}/releases?per_page=1 and
    // stores `pagination.items` in label_upstream_stats so the label
    // directory can show upstream totals before deciding what to sweep.
    app.post("/api/admin/label-upstream-stats/start", express.json({ limit: "1kb" }), async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        const body = req.body || {};
        const staleAfterDays = Number.isFinite(Number(body.staleAfterDays)) ? Number(body.staleAfterDays) : undefined;
        const resetCursor = !!body.resetCursor;
        try {
            const r = await startLabelUpstreamStats({ staleAfterDays, resetCursor });
            if (!r.ok) {
                res.status(409).json(r);
                return;
            }
            res.json(r);
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.post("/api/admin/label-upstream-stats/stop", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            requestLabelUpstreamStatsStop();
            res.json({ ok: true });
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.post("/api/admin/label-upstream-stats/force-clear", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            forceClearLabelUpstreamStats();
            res.json({ ok: true });
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.get("/api/admin/label-upstream-stats/status", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            res.json(getLabelUpstreamStatsStatus());
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    // ── Bulk label sweep ─────────────────────────────────────────────
    // Walks the label directory top-down by pad-row count, firing an
    // ad-hoc masters+ sweep at each label in turn. Owned by the
    // label-bulk-sweep-worker module.
    app.post("/api/admin/label-directory/bulk-sweep/start", express.json({ limit: "1kb" }), async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        const body = req.body || {};
        const minExternalCount = Number.isFinite(Number(body.minExternalCount))
            ? Number(body.minExternalCount) : 0;
        const skipSweptSinceDays = Number.isFinite(Number(body.skipSweptSinceDays))
            ? Number(body.skipSweptSinceDays) : undefined;
        const resetCursor = !!body.resetCursor;
        try {
            const result = await startBulkLabelSweep({ minExternalCount, resetCursor, skipSweptSinceDays });
            if (!result.ok) {
                res.status(409).json(result);
                return;
            }
            res.json(result);
        }
        catch (err) {
            console.error("[bulk-label-sweep start]", err);
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.post("/api/admin/label-directory/bulk-sweep/stop", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            requestBulkLabelSweepStop();
            res.json({ ok: true });
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.post("/api/admin/label-directory/bulk-sweep/force-clear", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            forceClearBulkLabelSweep();
            res.json({ ok: true });
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
    app.get("/api/admin/label-directory/bulk-sweep/status", async (req, res) => {
        if (!await requireAdmin(req, res))
            return;
        try {
            res.json(getBulkLabelSweepStatus());
        }
        catch (err) {
            res.status(500).json({ error: err?.message ?? String(err) });
        }
    });
}
