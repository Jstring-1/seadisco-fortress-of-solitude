// ── Admin split-cache projection routes ──────────────────────────
//
// Start/stop/force-clear/status for the one-shot projection backfill
// that copies release_cache into the split schema, plus the global
// split-cache-reader flag toggle. Extracted from search-api.ts (split
// 2/N). Pure pass-throughs to the worker module + db helpers — no
// coupling to search-api module state, so requireAdmin is passed in.

import express, { Express, Request, Response } from "express";
import {
  startCacheProjectionBackfill,
  requestCacheProjectionBackfillStop,
  forceClearCacheProjectionBackfill,
  getCacheProjectionBackfillStatus,
} from "../cache-projection-backfill-worker.js";
import {
  getProjectedCacheStats,
  isSplitCacheReaderEnabled,
  setSplitCacheReaderEnabled,
} from "../db.js";

type RequireAdmin = (req: Request, res: Response) => Promise<string | null>;

export function registerAdminCacheProjectionRoutes(app: Express, requireAdmin: RequireAdmin): void {
  // One-shot scan that projects every existing release_cache row into
  // the new discogs_cache_masters_plus / discogs_cache_pressings +
  // side-table schema. Idempotent; can be resumed / restarted.
  app.post("/api/admin/cache-projection/start", express.json({ limit: "1kb" }), async (req, res) => {
    if (!await requireAdmin(req, res)) return;
    const resetCursor = !!(req.body || {}).resetCursor;
    try {
      const result = await startCacheProjectionBackfill({ resetCursor });
      if (!result.ok) { res.status(409).json(result); return; }
      res.json(result);
    } catch (err: any) {
      console.error("[cache-projection start]", err);
      res.status(500).json({ error: err?.message ?? String(err) });
    }
  });
  app.post("/api/admin/cache-projection/stop", async (req, res) => {
    if (!await requireAdmin(req, res)) return;
    try { requestCacheProjectionBackfillStop(); res.json({ ok: true }); }
    catch (err: any) { res.status(500).json({ error: err?.message ?? String(err) }); }
  });
  app.post("/api/admin/cache-projection/force-clear", async (req, res) => {
    if (!await requireAdmin(req, res)) return;
    try { forceClearCacheProjectionBackfill(); res.json({ ok: true }); }
    catch (err: any) { res.status(500).json({ error: err?.message ?? String(err) }); }
  });
  app.get("/api/admin/cache-projection/status", async (req, res) => {
    if (!await requireAdmin(req, res)) return;
    try {
      const [worker, stats, splitReaders] = await Promise.all([
        Promise.resolve(getCacheProjectionBackfillStatus()),
        getProjectedCacheStats().catch(() => null),
        isSplitCacheReaderEnabled().catch(() => false),
      ]);
      res.json({ ...worker, stats, splitReadersEnabled: splitReaders });
    } catch (err: any) { res.status(500).json({ error: err?.message ?? String(err) }); }
  });

  // Global toggle for the split-cache reader path. When on, admin
  // panels (label directory today; cache analytics + feed later) read
  // from the projected schema instead of unrolling JSONB. Flip only
  // after the projection backfill has drained.
  app.post("/api/admin/cache-projection/set-reader-flag", express.json({ limit: "1kb" }), async (req, res) => {
    if (!await requireAdmin(req, res)) return;
    const enabled = !!(req.body || {}).enabled;
    try {
      await setSplitCacheReaderEnabled(enabled);
      res.json({ ok: true, enabled });
    } catch (err: any) { res.status(500).json({ error: err?.message ?? String(err) }); }
  });
}
