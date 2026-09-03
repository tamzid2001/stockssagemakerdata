import crypto from "node:crypto";
import type { Request, Response, Router } from "express";
import type admin from "firebase-admin";
import {
  PLATFORM_API_SCOPES,
  authenticatePlatformRequest,
  authorizeWorkspaceAction,
  createPersonalApiKey,
  listAccessibleWorkspaces,
  requireWorkspacePermission,
  requireScope,
  resolveWorkspaceAccess,
  validatePlatformScopes,
  writeApiAudit,
  type ApiPrincipal,
  type PlatformApiScope,
} from "./apiAccess";
import {
  ALL_WORKSPACE_PERMISSIONS,
  addWorkspaceCollaborator,
  createWorkspace,
  ensurePersonalWorkspace,
  listWorkspaceAudit,
  listWorkspaceCollaborators,
  publicWorkspace,
  removeWorkspaceCollaborator,
  updateWorkspace,
  updateWorkspaceCollaborator,
  writeWorkspaceAudit,
} from "./workspaces";
import {
  MAX_BULK_CSV_OPERATION,
  copyAuthorizedCsv,
  createManagedUploadedCsv,
  deleteAuthorizedCsv,
  findAuthorizedCsv,
  listAuthorizedCsvs,
  moveAuthorizedCsv,
  publicUploadedCsv,
  readUploadedCsvBytes,
  renameAuthorizedCsv,
} from "./uploadedCsv";
import { DATASET_CATALOG, datasetById } from "./datasetCatalog";
import { buildOpenApiDocument } from "./openapi";
import { PLAN_ENTITLEMENTS, publicPlanEntitlements } from "./planEntitlements";

type Options = { db: FirebaseFirestore.Firestore; auth: admin.auth.Auth; publicOrigin: string };
type JsonRecord = Record<string, unknown>;
type Handler = (req: Request, res: Response, principal: ApiPrincipal, requestId: string) => Promise<void>;

const API_KEYS = "quantura_api_keys";
const RATE_WINDOWS = "quantura_api_rate_windows";
// eslint-disable-next-line @typescript-eslint/no-var-requires
const AdmZip = require("adm-zip");

function text(value: unknown, max = 500): string { return String(value ?? "").trim().slice(0, max); }
function plain(value: unknown): JsonRecord { return value && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {}; }

function apiError(error: unknown): { status: number; code: string; message: string } {
  const raw = text((error as any)?.message || error, 200).toLowerCase();
  if (/missing|invalid|revoked|expired/.test(raw) && /api_key|token/.test(raw)) return { status: 401, code: raw.toUpperCase(), message: "Authentication failed." };
  if (/insufficient|forbidden|read_only|permission_denied|owner_required|escalation|upgrade_required/.test(raw)) return { status: 403, code: raw.toUpperCase(), message: "This token is not authorized for the requested operation." };
  if (/not_found/.test(raw)) return { status: 404, code: raw.toUpperCase(), message: "The requested resource was not found." };
  if (/rate_limit/.test(raw)) return { status: 429, code: "RATE_LIMITED", message: "Rate limit exceeded." };
  if (/csv_size_limit|csv_file_too_large|csv_zip_size_limit|payload_too_large/.test(raw)) return { status: 413, code: "PAYLOAD_TOO_LARGE", message: "The CSV operation exceeds the configured size limit." };
  if (/already_exists|immutable/.test(raw)) return { status: 409, code: raw.toUpperCase(), message: "The request conflicts with the current resource state." };
  if (/invalid|requires|limit_exceeded|unsupported|unclosed|duplicate_columns/.test(raw)) return { status: 422, code: raw.toUpperCase().replace(/[^A-Z0-9]+/g, "_"), message: "The request failed validation." };
  return { status: 400, code: raw.toUpperCase().replace(/[^A-Z0-9]+/g, "_") || "INVALID_REQUEST", message: "The request is invalid." };
}

function sendError(res: Response, error: unknown, requestId: string): void {
  const normalized = apiError(error);
  res.status(normalized.status).json({ error: { code: normalized.code, message: normalized.message, request_id: requestId } });
}

function sendData(res: Response, data: unknown, requestId: string, meta: JsonRecord = {}): void {
  res.setHeader("X-Request-ID", requestId);
  res.setHeader("X-Quantura-API-Version", "v1");
  res.setHeader("Cache-Control", "private, no-store");
  res.json({ data, meta: { api_version: "v1", ...meta } });
}

async function enforceRateLimit(options: Options, principal: ApiPrincipal): Promise<void> {
  const limit = PLAN_ENTITLEMENTS[principal.plan].apiReadPerMinute;
  const start = Math.floor(Date.now() / 60_000) * 60_000;
  const identity = principal.tokenId || `session_${principal.userId}`;
  const ref = options.db.collection(RATE_WINDOWS).doc(`${identity}_${start}`);
  await options.db.runTransaction(async (transaction) => {
    const snapshot = await transaction.get(ref);
    const count = Number(snapshot.data()?.count || 0);
    if (count >= limit) throw new Error("rate_limited");
    transaction.set(ref, { count: count + 1, identity, window_start: new Date(start).toISOString(), expires_at: new Date(start + 120_000).toISOString() }, { merge: true });
  });
}

function wrap(options: Options, handler: Handler): (req: Request, res: Response) => Promise<void> {
  return async (req, res) => {
    const requestId = crypto.randomUUID();
    const started = Date.now();
    let principal: ApiPrincipal | undefined;
    let status = 500;
    try {
      principal = await authenticatePlatformRequest(req, options);
      await enforceRateLimit(options, principal);
      await handler(req, res, principal, requestId);
      status = res.statusCode;
    } catch (error) {
      sendError(res, error, requestId);
      status = res.statusCode;
    } finally {
      void writeApiAudit(options.db, { principal, endpoint: req.path, method: req.method, status, requestId, latencyMs: Date.now() - started }).catch(() => undefined);
    }
  };
}

function cursorEncode(value: string): string { return Buffer.from(value, "utf8").toString("base64url"); }
function cursorDecode(value: unknown): string {
  const clean = text(value, 500);
  if (!clean) return "";
  try { return Buffer.from(clean, "base64url").toString("utf8"); } catch { throw new Error("cursor_invalid"); }
}

function parseCsvPage(records: Record<string, any>[], query: Request["query"], maximum = 500): { rows: Record<string, any>[]; nextCursor: string | null } {
  const limit = Math.min(Math.max(Number(query.limit) || 50, 1), maximum);
  const ticker = text(query.ticker, 20).toUpperCase();
  const source = text(query.source, 80).toLowerCase();
  const from = text(query.from, 80);
  const to = text(query.to, 80);
  const cursor = cursorDecode(query.cursor);
  const sort = text(query.sort || "uploaded_at", 40);
  const order = text(query.order || "desc", 10).toLowerCase() === "asc" ? 1 : -1;
  let filtered = records
    .filter((record) => !ticker || text(record.ticker, 20).toUpperCase() === ticker)
    .filter((record) => !source || text(record.source_type, 80).toLowerCase() === source)
    .filter((record) => !from || text(record.uploaded_at, 80) >= from)
    .filter((record) => !to || text(record.uploaded_at, 80) <= to);
  const field = sort === "display_filename" ? "display_filename" : sort === "size_bytes" ? "size_bytes" : "uploaded_at";
  filtered.sort((a, b) => {
    const left = field === "size_bytes" ? Number(a[field]) || 0 : text(a[field], 500).toLowerCase();
    const right = field === "size_bytes" ? Number(b[field]) || 0 : text(b[field], 500).toLowerCase();
    if (left < right) return -1 * order;
    if (left > right) return 1 * order;
    return a.id.localeCompare(b.id) * order;
  });
  const start = cursor ? Math.max(0, filtered.findIndex((record) => record.id === cursor) + 1) : 0;
  const rows = filtered.slice(start, start + limit);
  const nextCursor = start + limit < filtered.length && rows.length ? cursorEncode(rows.at(-1)!.id) : null;
  return { rows, nextCursor };
}

function csvManifest(records: Record<string, any>[]): Record<string, unknown>[] {
  return records.map((record) => ({
    id: record.id, workspace_id: record.workspace_id, filename: record.display_filename || record.original_filename,
    size_bytes: record.size_bytes ?? null, sha256: record.sha256 || null, ticker: record.ticker || null,
    uploaded_at: record.uploaded_at || null, row_count: record.row_count ?? null, column_count: record.column_count ?? null,
  }));
}

function manifestCsv(rows: Record<string, unknown>[]): string {
  const columns = ["id", "workspace_id", "filename", "size_bytes", "sha256", "ticker", "uploaded_at", "row_count", "column_count"];
  const quote = (value: unknown) => `"${String(value ?? "").replaceAll('"', '""')}"`;
  return [columns.join(","), ...rows.map((row) => columns.map((column) => quote(row[column])).join(","))].join("\n");
}

function publicResource(value: JsonRecord): JsonRecord {
  const blocked = /(^|_)(secret|credential|private_key|api_key|alpha|strategy|raw_prompt|system_prompt|storage_path|file_path|firestore_key|signed_url|download_url|api_text_path|s3_uri)($|_)/i;
  const walk = (input: unknown): unknown => {
    if (Array.isArray(input)) return input.map(walk);
    if (!input || typeof input !== "object") return input;
    return Object.fromEntries(Object.entries(input as JsonRecord).filter(([key]) => {
      const normalized = key.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
      return !blocked.test(normalized);
    }).map(([key, item]) => [key, walk(item)]));
  };
  return walk(value) as JsonRecord;
}

const workspaceResources: Record<string, { collection: string; scope: PlatformApiScope; permission: Parameters<typeof requireWorkspacePermission>[1] }> = {
  forecasts: { collection: "forecast_requests", scope: "forecasts:read", permission: "forecast.read" },
  "prediction-analyses": { collection: "prediction_uploads", scope: "predictions:read", permission: "analysis.read" },
  "screener-snapshots": { collection: "screener_runs", scope: "screener:read", permission: "screener.read" },
  datasets: { collection: "autopilot_requests", scope: "datasets:read", permission: "csv.read" },
  backtests: { collection: "forecast_backtests", scope: "backtests:read", permission: "forecast.read" },
};

function recordTime(value: Record<string, any>): number {
  const raw = value.createdAt || value.created_at || value.updatedAt || value.updated_at;
  const date = raw?.toDate?.() || raw;
  const parsed = date instanceof Date ? date.getTime() : Date.parse(String(date || ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

export function registerPlatformApiRoutes(router: Router, options: Options): void {
  router.get("/openapi.json", (_req, res) => {
    res.setHeader("Cache-Control", "public, max-age=300, stale-while-revalidate=3600");
    res.json(buildOpenApiDocument(options.publicOrigin));
  });
  router.get("/v1/plans", (_req, res) => res.json({ data: publicPlanEntitlements(), meta: { api_version: "v1" } }));

  router.post("/calendar/interactions", wrap(options, async (req, res, principal, requestId) => {
    if (principal.authMethod !== "firebase_session") throw new Error("session_required");
    const body = plain(req.body);
    const eventName = text(body.event_name, 80).toLowerCase().replace(/[^a-z0-9_:-]/g, "_");
    const allowed = new Set([
      "forecast_generated", "forecast_saved", "forecast_ai_analysis_generated", "screener_run_created",
      "screener_exported", "historical_data_downloaded", "options_history_downloaded",
      "prediction_market_dataset_downloaded", "prediction_csv_analyzed", "backtest_completed",
      "api_key_created", "api_key_revoked", "task_created", "task_updated", "task_completed"
    ]);
    if (!allowed.has(eventName)) throw new Error("interaction_not_recordable");
    const occurredRaw = text(body.occurred_at, 80);
    const occurredMs = Date.parse(occurredRaw || new Date().toISOString());
    if (!Number.isFinite(occurredMs) || occurredMs > Date.now() + 60_000) throw new Error("interaction_timestamp_invalid");
    const idempotencyKey = text(body.idempotency_key, 160) || crypto.createHash("sha256").update(`${principal.userId}:${eventName}:${occurredMs}`).digest("hex");
    const ref = options.db.collection("users").doc(principal.userId).collection("calendar_interactions").doc(idempotencyKey);
    await ref.create({
      event_name: eventName,
      title: text(body.title, 160) || eventName.replaceAll("_", " "),
      occurred_at: new Date(occurredMs).toISOString(),
      route: text(body.route, 240),
      resource_type: text(body.resource_type, 80) || null,
      resource_id: text(body.resource_id, 220) || null,
      workspace_id: text(body.workspace_id, 220) || principal.userId,
      created_at: new Date().toISOString(),
    }).catch((error: any) => {
      if (Number(error?.code) !== 6 && !String(error?.message || "").toLowerCase().includes("already exists")) throw error;
    });
    res.status(201);
    sendData(res, { id: ref.id, recorded: true }, requestId);
  }));

  router.get("/v1/me/access", wrap(options, async (_req, res, principal, requestId) => {
    requireScope(principal, "account:read");
    const workspaces = await listAccessibleWorkspaces(options.db, principal);
    sendData(res, {
      user_id: principal.userId,
      auth_method: principal.authMethod,
      token_name: principal.tokenName,
      token_scopes: principal.tokenScopes,
      personal_plan: principal.plan,
      accessible_workspaces: workspaces,
    }, requestId);
  }));

  router.get("/v1/capabilities", wrap(options, async (_req, res, principal, requestId) => {
    requireScope(principal, "account:read");
    const features = PLAN_ENTITLEMENTS[principal.plan].features;
    const workspaces = await listAccessibleWorkspaces(options.db, principal);
    const sharedWorkspaceRead = workspaces.some((workspace) => workspace.role === "viewer" || workspace.role === "editor");
    sendData(res, {
      forecasting: features.includes("meta_prophet"), screener: features.includes("screener"), historical_data: features.includes("historical_data"),
      options: features.includes("options"), sports: features.includes("prediction_markets"), api_dataset_download: features.includes("api") || features.includes("bulk_exports"),
      backtesting: features.includes("backtesting"), collaboration: features.includes("workspaces"), sagemaker: features.includes("sagemaker_exports"),
      shared_workspace_read: sharedWorkspaceRead,
      workspace_access: workspaces.map((workspace) => ({ workspace_id: workspace.workspaceId, role: workspace.role, capabilities: workspace.capabilities })),
    }, requestId);
  }));

  router.get("/v1/workspaces", wrap(options, async (_req, res, principal, requestId) => {
    requireScope(principal, "workspaces:read");
    const user = await options.auth.getUser(principal.userId).catch(() => null);
    await ensurePersonalWorkspace(options.db, principal, { name: user?.displayName, email: user?.email });
    const workspaces = await listAccessibleWorkspaces(options.db, principal);
    sendData(res, workspaces.map(publicWorkspace), requestId, { count: workspaces.length, next_cursor: null });
  }));

  router.post("/v1/workspaces", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "workspaces:write");
    const body = plain(req.body);
    const user = await options.auth.getUser(principal.userId).catch(() => null);
    await ensurePersonalWorkspace(options.db, principal, { name: user?.displayName, email: user?.email });
    const workspace = await createWorkspace(options.db, principal, { name: body.name, description: body.description });
    res.status(201);
    sendData(res, publicWorkspace(workspace), requestId);
  }));

  router.get("/v1/workspace-permissions", wrap(options, async (_req, res, principal, requestId) => {
    requireScope(principal, "workspaces:read");
    sendData(res, ALL_WORKSPACE_PERMISSIONS, requestId, { count: ALL_WORKSPACE_PERMISSIONS.length });
  }));

  router.get("/v1/workspaces/:workspaceId", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "workspaces:read");
    const access = await resolveWorkspaceAccess(options.db, principal, req.params.workspaceId);
    sendData(res, publicWorkspace(access), requestId);
  }));

  router.patch("/v1/workspaces/:workspaceId", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "workspaces:write");
    const body = plain(req.body);
    const workspace = await updateWorkspace(options.db, principal, req.params.workspaceId, { name: body.name, description: body.description, settings: body.settings });
    sendData(res, publicWorkspace(workspace), requestId);
  }));

  router.get("/v1/workspaces/:workspaceId/collaborators", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "workspaces:read");
    const collaborators = await listWorkspaceCollaborators(options.db, principal, req.params.workspaceId);
    sendData(res, collaborators, requestId, { count: collaborators.length, next_cursor: null });
  }));

  router.post("/v1/workspaces/:workspaceId/collaborators", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "workspaces:write");
    const body = plain(req.body);
    const collaborator = await addWorkspaceCollaborator(options.db, options.auth, principal, req.params.workspaceId, {
      email: body.email, role: body.role, permissions: body.permissions, resource_scope: body.resource_scope,
    });
    res.status(201);
    sendData(res, collaborator, requestId);
  }));

  router.patch("/v1/workspaces/:workspaceId/collaborators/:collaboratorId", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "workspaces:write");
    const body = plain(req.body);
    const collaborator = await updateWorkspaceCollaborator(options.db, principal, req.params.workspaceId, req.params.collaboratorId, {
      role: body.role, permissions: body.permissions, resource_scope: body.resource_scope, status: body.status,
    });
    sendData(res, collaborator, requestId);
  }));

  router.delete("/v1/workspaces/:workspaceId/collaborators/:collaboratorId", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "workspaces:write");
    const removed = await removeWorkspaceCollaborator(options.db, principal, req.params.workspaceId, req.params.collaboratorId);
    sendData(res, removed, requestId);
  }));

  router.get("/v1/workspaces/:workspaceId/audit-log", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "workspaces:read");
    const rows = await listWorkspaceAudit(options.db, principal, req.params.workspaceId, req.query.limit);
    sendData(res, rows, requestId, { count: rows.length, next_cursor: null });
  }));

  const listCsv = async (req: Request, res: Response, principal: ApiPrincipal, requestId: string, workspaceId?: string) => {
    const result = await listAuthorizedCsvs(options.db, principal, workspaceId || req.query.workspace_id);
    const page = parseCsvPage(result.records, req.query);
    sendData(res, page.rows.map((record) => publicUploadedCsv(record as any)), requestId, { count: page.rows.length, next_cursor: page.nextCursor, workspace_id: workspaceId || text(req.query.workspace_id, 220) || null });
  };

  const downloadCsv = async (req: Request, res: Response, principal: ApiPrincipal, requestId: string, workspaceId?: string) => {
    const { record, access } = await findAuthorizedCsv(options.db, principal, req.params.csvId, "csv.download", "datasets:read", workspaceId);
    const bytes = await readUploadedCsvBytes(options.db, record);
    const filename = text(record.display_filename || record.original_filename || `${record.id}.csv`, 240).replace(/[\r\n"\\/]/g, "-");
    await writeWorkspaceAudit(options.db, { workspaceId: access.workspaceId, actorUserId: principal.userId, action: "csv_downloaded", resourceType: "csv", resourceId: record.id });
    res.setHeader("X-Request-ID", requestId);
    res.setHeader("Cache-Control", "private, no-store");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.status(200).send(bytes);
  };

  const downloadCsvZip = async (req: Request, res: Response, principal: ApiPrincipal, requestId: string, workspaceId?: string) => {
    const result = await listAuthorizedCsvs(options.db, principal, workspaceId || req.query.workspace_id);
    const page = parseCsvPage(result.records, { ...req.query, limit: "1000" } as Request["query"], 1000);
    const zip = new AdmZip();
    const exported: Record<string, any>[] = [];
    const configuredLimit = Math.floor(Number(process.env.QUANTURA_CSV_ZIP_MAX_BYTES) || 100 * 1024 * 1024);
    const byteLimit = Math.min(Math.max(configuredLimit, 1024 * 1024), 250 * 1024 * 1024);
    let totalBytes = 0;
    for (const record of page.rows) {
      const access = await resolveWorkspaceAccess(options.db, principal, record.workspace_id);
      requireWorkspacePermission(access, "csv.download", record.id);
      const declaredSize = Math.max(0, Math.floor(Number(record.size_bytes) || 0));
      if (declaredSize && totalBytes + declaredSize > byteLimit) throw new Error("csv_zip_size_limit_exceeded");
      const bytes = await readUploadedCsvBytes(options.db, record as any, byteLimit - totalBytes);
      totalBytes += bytes.length;
      if (totalBytes > byteLimit) throw new Error("csv_zip_size_limit_exceeded");
      const filename = `${record.id}__${text(record.display_filename || record.original_filename, 180).replace(/[^A-Za-z0-9._-]/g, "-")}`;
      zip.addFile(filename, bytes);
      exported.push(record);
    }
    const manifest = csvManifest(exported);
    zip.addFile("manifest.json", Buffer.from(JSON.stringify(manifest, null, 2)));
    zip.addFile("manifest.csv", Buffer.from(manifestCsv(manifest)));
    zip.addFile("manifest_summary.json", Buffer.from(JSON.stringify({ generated_at: new Date().toISOString(), record_count: exported.length, workspace_id: workspaceId || null }, null, 2)));
    res.setHeader("X-Request-ID", requestId);
    res.setHeader("Cache-Control", "private, no-store");
    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename="quantura-uploaded-csvs-${new Date().toISOString().slice(0, 10)}.zip"`);
    res.status(200).send(zip.toBuffer());
  };

  router.get("/v1/uploads/csv", wrap(options, (req, res, principal, requestId) => listCsv(req, res, principal, requestId)));
  router.post("/v1/uploads/csv", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "datasets:write");
    const body = plain(req.body);
    const workspaceId = text(body.workspace_id, 220) || principal.userId;
    const rawBase64 = typeof body.content_base64 === "string" ? body.content_base64.trim() : "";
    const rawText = typeof body.csv_text === "string" ? body.csv_text : "";
    if (rawBase64 && (!/^[A-Za-z0-9+/]*={0,2}$/.test(rawBase64) || rawBase64.length % 4 !== 0)) throw new Error("csv_base64_invalid");
    const bytes = rawBase64 ? Buffer.from(rawBase64, "base64") : Buffer.from(rawText, "utf8");
    const created = await createManagedUploadedCsv(options.db, principal, workspaceId, text(body.filename, 240), bytes, plain(body.metadata));
    res.status(201);
    sendData(res, created, requestId);
  }));
  router.get("/v1/uploads/csv/download-all", wrap(options, (req, res, principal, requestId) => downloadCsvZip(req, res, principal, requestId)));
  router.get("/v1/uploads/csv/:csvId/download", wrap(options, (req, res, principal, requestId) => downloadCsv(req, res, principal, requestId)));
  router.get("/v1/uploads/csv/:csvId", wrap(options, async (req, res, principal, requestId) => {
    const { record } = await findAuthorizedCsv(options.db, principal, req.params.csvId, "csv.read");
    sendData(res, publicUploadedCsv(record), requestId);
  }));
  router.patch("/v1/uploads/csv/:csvId", wrap(options, async (req, res, principal, requestId) => {
    const updated = await renameAuthorizedCsv(options.db, principal, req.params.csvId, plain(req.body).display_filename);
    sendData(res, updated, requestId);
  }));
  router.delete("/v1/uploads/csv/:csvId", wrap(options, async (req, res, principal, requestId) => {
    sendData(res, await deleteAuthorizedCsv(options.db, principal, req.params.csvId), requestId);
  }));
  router.post("/v1/uploads/csv/:csvId/move", wrap(options, async (req, res, principal, requestId) => {
    sendData(res, await moveAuthorizedCsv(options.db, principal, req.params.csvId, plain(req.body).destination_workspace_id), requestId);
  }));
  router.post("/v1/uploads/csv/:csvId/copy", wrap(options, async (req, res, principal, requestId) => {
    const copied = await copyAuthorizedCsv(options.db, principal, req.params.csvId, plain(req.body).destination_workspace_id);
    res.status(201);
    sendData(res, copied, requestId);
  }));

  const bulkCsvAction = async (req: Request, res: Response, principal: ApiPrincipal, requestId: string, action: "move" | "copy") => {
    const body = plain(req.body);
    const ids = Array.isArray(body.csv_ids) ? [...new Set(body.csv_ids.map((id) => text(id, 220)).filter(Boolean))] : [];
    if (!ids.length || ids.length > MAX_BULK_CSV_OPERATION) throw new Error("csv_bulk_size_invalid");
    const destination = text(body.destination_workspace_id, 220);
    const successful: Record<string, unknown>[] = [];
    const failed: Record<string, unknown>[] = [];
    for (const id of ids) {
      try {
        const result = action === "move" ? await moveAuthorizedCsv(options.db, principal, id, destination) : await copyAuthorizedCsv(options.db, principal, id, destination);
        successful.push({ source_csv_id: id, csv: result });
      } catch (error) {
        failed.push({ csv_id: id, code: apiError(error).code });
      }
    }
    sendData(res, { successful, failed }, requestId, { requested: ids.length, successful: successful.length, failed: failed.length });
  };
  router.post("/v1/uploads/csv/bulk-move", wrap(options, (req, res, principal, requestId) => bulkCsvAction(req, res, principal, requestId, "move")));
  router.post("/v1/uploads/csv/bulk-copy", wrap(options, (req, res, principal, requestId) => bulkCsvAction(req, res, principal, requestId, "copy")));

  router.get("/v1/workspaces/:workspaceId/uploads/csv", wrap(options, (req, res, principal, requestId) => listCsv(req, res, principal, requestId, req.params.workspaceId)));
  router.get("/v1/workspaces/:workspaceId/uploads/csv/download-all", wrap(options, (req, res, principal, requestId) => downloadCsvZip(req, res, principal, requestId, req.params.workspaceId)));
  router.get("/v1/workspaces/:workspaceId/uploads/csv/:csvId/download", wrap(options, (req, res, principal, requestId) => downloadCsv(req, res, principal, requestId, req.params.workspaceId)));
  router.get("/v1/workspaces/:workspaceId/uploads/csv/:csvId", wrap(options, async (req, res, principal, requestId) => {
    const { record } = await findAuthorizedCsv(options.db, principal, req.params.csvId, "csv.read", "datasets:read", req.params.workspaceId);
    sendData(res, publicUploadedCsv(record), requestId);
  }));

  router.get("/v1/workspaces/:workspaceId/:resource", wrap(options, async (req, res, principal, requestId) => {
    const definition = workspaceResources[text(req.params.resource, 80)];
    if (!definition) throw new Error("resource_not_found");
    const access = await resolveWorkspaceAccess(options.db, principal, req.params.workspaceId);
    authorizeWorkspaceAction(principal, access, definition.scope, "read");
    requireWorkspacePermission(access, definition.permission);
    if (access.role === "owner" && principal.authMethod === "api_key" && !PLAN_ENTITLEMENTS[principal.plan].features.includes("api")) throw new Error("plan_upgrade_required");
    const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 200);
    const cursor = cursorDecode(req.query.cursor);
    const snapshots = await Promise.all([
      options.db.collection(definition.collection).where("workspaceId", "==", access.workspaceId).limit(401).get().catch(() => null),
      options.db.collection(definition.collection).where("userId", "==", access.workspaceId).limit(401).get().catch(() => null),
    ]);
    const records = [...new Map(snapshots.flatMap((snapshot) => snapshot?.docs || []).map((doc) => [doc.id, doc])).values()]
      .filter((doc) => !cursor || recordTime(doc.data() || {}) < Date.parse(cursor))
      .filter((doc) => req.params.resource !== "forecasts" || access.resourceScope.forecasts.mode === "all" || (access.resourceScope.forecasts.mode === "selected" && access.resourceScope.forecasts.ids.includes(doc.id)))
      .filter((doc) => req.params.resource !== "datasets" || access.resourceScope.csv.mode === "all" || (access.resourceScope.csv.mode === "selected" && access.resourceScope.csv.ids.includes(`legacy_${doc.id}`)))
      .sort((left, right) => recordTime(right.data() || {}) - recordTime(left.data() || {}) || left.id.localeCompare(right.id));
    const visible = records.slice(0, limit);
    const data = visible.map((doc) => ({ id: doc.id, ...publicResource(plain(doc.data())) }));
    const lastTime = visible.length ? recordTime(visible[visible.length - 1].data() || {}) : 0;
    const nextCursor = records.length > limit && lastTime ? cursorEncode(new Date(lastTime).toISOString()) : null;
    sendData(res, data, requestId, { count: data.length, next_cursor: nextCursor, workspace_id: access.workspaceId, workspace_role: access.role });
  }));

  router.get("/v1/datasets", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "datasets:read");
    if (principal.authMethod === "api_key" && !PLAN_ENTITLEMENTS[principal.plan].features.includes("api")) throw new Error("plan_upgrade_required");
    const assetClass = text(req.query.asset_class, 80).toLowerCase();
    const source = text(req.query.source, 80).toLowerCase();
    const derived = text(req.query.derived, 10).toLowerCase();
    const data = DATASET_CATALOG.filter((item) => !assetClass || item.assetClasses.includes(assetClass))
      .filter((item) => !source || item.sourceProvider.toLowerCase().includes(source))
      .filter((item) => !derived || String(item.derived) === derived);
    sendData(res, data, requestId, { count: data.length, next_cursor: null });
  }));
  router.get("/v1/datasets/:datasetId", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "datasets:read");
    if (principal.authMethod === "api_key" && !PLAN_ENTITLEMENTS[principal.plan].features.includes("api")) throw new Error("plan_upgrade_required");
    const dataset = datasetById(req.params.datasetId);
    if (!dataset) throw new Error("dataset_not_found");
    sendData(res, dataset, requestId);
  }));
  router.get("/v1/datasets/:datasetId/schema", wrap(options, async (req, res, principal, requestId) => {
    requireScope(principal, "datasets:read");
    if (principal.authMethod === "api_key" && !PLAN_ENTITLEMENTS[principal.plan].features.includes("api")) throw new Error("plan_upgrade_required");
    const dataset = datasetById(req.params.datasetId);
    if (!dataset) throw new Error("dataset_not_found");
    sendData(res, { dataset_id: dataset.id, schema: dataset.schema, forecast_quantiles: dataset.forecastQuantiles }, requestId);
  }));

  router.get("/account/api-keys/scopes", wrap(options, async (_req, res, principal, requestId) => {
    sendData(res, PLATFORM_API_SCOPES, requestId, { count: PLATFORM_API_SCOPES.length });
  }));
  router.get("/account/api-keys", wrap(options, async (_req, res, principal, requestId) => {
    if (principal.authMethod !== "firebase_session") throw new Error("session_required");
    const snapshot = await options.db.collection(API_KEYS).where("user_id", "==", principal.userId).limit(100).get();
    const keys = snapshot.docs.map((doc) => {
      const value = plain(doc.data());
      return { id: doc.id, name: value.name, prefix: value.prefix, scopes: value.scopes, created_at: value.created_at, last_used_at: value.last_used_at, expires_at: value.expires_at, revoked_at: value.revoked_at };
    });
    sendData(res, keys, requestId, { count: keys.length });
  }));
  router.post("/account/api-keys", wrap(options, async (req, res, principal, requestId) => {
    if (principal.authMethod !== "firebase_session") throw new Error("session_required");
    const body = plain(req.body);
    const created = await createPersonalApiKey(options.db, principal, { name: body.name, scopes: body.scopes, expiresAt: body.expires_at });
    res.status(201);
    sendData(res, { ...created, secret_shown_once: true }, requestId);
  }));
  router.post("/account/api-keys/:keyId/replace", wrap(options, async (req, res, principal, requestId) => {
    if (principal.authMethod !== "firebase_session") throw new Error("session_required");
    const ref = options.db.collection(API_KEYS).doc(text(req.params.keyId, 128));
    const snapshot = await ref.get();
    if (!snapshot.exists || text(snapshot.data()?.user_id, 220) !== principal.userId) throw new Error("api_key_not_found");
    const existing = plain(snapshot.data());
    const scopes = validatePlatformScopes(existing.scopes);
    const created = await createPersonalApiKey(options.db, principal, { name: `${text(existing.name, 100)} replacement`, scopes, expiresAt: existing.expires_at });
    await ref.set({ revoked_at: new Date().toISOString(), replaced_by: created.id }, { merge: true });
    res.status(201);
    sendData(res, { ...created, secret_shown_once: true }, requestId);
  }));
  router.delete("/account/api-keys/:keyId", wrap(options, async (req, res, principal, requestId) => {
    if (principal.authMethod !== "firebase_session") throw new Error("session_required");
    const ref = options.db.collection(API_KEYS).doc(text(req.params.keyId, 128));
    const snapshot = await ref.get();
    if (!snapshot.exists || text(snapshot.data()?.user_id, 220) !== principal.userId) throw new Error("api_key_not_found");
    await ref.set({ revoked_at: new Date().toISOString() }, { merge: true });
    sendData(res, { revoked: true, id: snapshot.id }, requestId);
  }));
}
