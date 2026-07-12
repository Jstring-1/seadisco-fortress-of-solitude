// ── Admin external-discography worker routes ─────────────────────
//
// Start/stop/status for the singleflight scraper worker (wirz /
// Abrams). Extracted from search-api.ts (split 4/N). Pure pass-throughs
// to the external-discography-worker module — no coupling to search-api
// module state, so requireAdmin is passed in. The xlsx-upload endpoint
// stays in search-api for now (it couples to the parser + db inserts).

import express, { Express, Request, Response } from "express";
import {
  getExternalDiscographyStatus,
  startExternalDiscographyRun,
  isExternalDiscographyRunning,
  requestExternalDiscographyStop,
} from "../external-discography-worker.js";

type RequireAdmin = (req: Request, res: Response) => Promise<string | null>;

export function registerAdminExternalDiscographyRoutes(app: Express, requireAdmin: RequireAdmin): void {
  // One singleflight worker for wirz / Abrams scrapes. Polite 2s delay
  // per page; cursor persists in app_settings so a Railway restart
  // resumes from the last completed seed.
  app.get("/api/admin/external-discography-worker/status", async (req, res) => {
    if (!await requireAdmin(req, res)) return;
    res.json(getExternalDiscographyStatus());
  });

  app.post("/api/admin/external-discography-worker/start", express.json({ limit: "8kb" }), async (req, res) => {
    if (!await requireAdmin(req, res)) return;
    const source = String((req.body || {}).source ?? "").trim().toLowerCase();
    if (source !== "wirz" && source !== "abrams") {
      res.status(400).json({ error: "source must be 'wirz' or 'abrams'" });
      return;
    }
    const result = await startExternalDiscographyRun(source as "wirz" | "abrams");
    if (!result.ok) { res.status(409).json(result); return; }
    res.json(result);
  });

  app.post("/api/admin/external-discography-worker/stop", async (req, res) => {
    if (!await requireAdmin(req, res)) return;
    if (!isExternalDiscographyRunning()) {
      res.json({ ok: true, message: "not running" });
      return;
    }
    requestExternalDiscographyStop();
    res.json({ ok: true, message: "stop requested" });
  });
}
