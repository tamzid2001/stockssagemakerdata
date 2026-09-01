import crypto from "node:crypto";
import type { Request, Response, Router } from "express";
import type admin from "firebase-admin";
import {
  PLATFORM_API_SCOPES,
  authenticatePlatformRequest,
  authorizeWorkspaceAction,
  createPersonalApiKey,
  listAccessibleWorkspaces,
  requireScope,
  resolveWorkspaceAccess,
  validatePlatformScopes,
  writeApiAudit,
  type ApiPrincipal,
  type PlatformApiScope,
} from "./apiAccess";
import { DATASET_CATALOG, datasetById } from "./datasetCatalog";
import { buildOpenApiDocument } from "./openapi";
import { PLAN_ENTITLEMENTS, publicPlanEntitlements } from "./planEntitlements";

type Options = { db: FirebaseFirestore.Firestore; auth: admin.auth.Auth; publicOrigin: string };
type JsonRecord = Record<string, unknown>;
type Handler = (req: Request, res: Response, principal: ApiPrincipal, requestId: string) => Promise<void>;

const API_KEYS = "quantura_api_keys";
const RATE_WINDOWS = "quantura_api_rate_windows";

function text(value: unknown, max = 500): string { return String(value ?? "").trim().slice(0, max); }
function plain(value: unknown): JsonRecord { return value && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {}; }

function apiError(error: unknown): { status: number; code: string; message: string } {
  const raw = text((error as any)?.message || error, 200).toLowerCase();
  if (/missing|invalid|revoked|expired/.test(raw) && /api_key|token/.test(raw)) return { status: 401, code: raw.toUpperCase(), message: "Authentication failed." };
  if (/insufficient|forbidden|read_only|owner_required|upgrade_required/.test(raw)) return { status: 403, code: raw.toUpperCase(), message: "This token is not authorized for the requested operation." };
  if (/not_found/.test(raw)) return { status: 404, code: raw.toUpperCase(), message: "The requested resource was not found." };
  if (/rate_limit/.test(raw)) return { status: 429, code: "RATE_LIMITED", message: "Rate limit exceeded." };
  if (/already_exists|immutable/.test(raw)) return { status: 409, code: raw.toUpperCase(), message: "The request conflicts with the current resource state." };
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

function publicResource(value: JsonRecord): JsonRecord {
  const blocked = /(^|_)(secret|credential|private_key|api_key|alpha|strategy|raw_prompt|system_prompt)($|_)/i;
  const walk = (input: unknown): unknown => {
    if (Array.isArray(input)) return input.map(walk);
    if (!input || typeof input !== "object") return input;
    return Object.fromEntries(Object.entries(input as JsonRecord).filter(([key]) => !blocked.test(key)).map(([key, item]) => [key, walk(item)]));
  };
  return walk(value) as JsonRecord;
}

const workspaceResources: Record<string, { collection: string; ownerField: string; scope: PlatformApiScope }> = {
  forecasts: { collection: "forecast_requests", ownerField: "userId", scope: "forecasts:read" },
  "prediction-analyses": { collection: "prediction_uploads", ownerField: "userId", scope: "predictions:read" },
  "screener-snapshots": { collection: "screener_runs", ownerField: "userId", scope: "screener:read" },
  datasets: { collection: "autopilot_requests", ownerField: "userId", scope: "datasets:read" },
  backtests: { collection: "forecast_backtests", ownerField: "userId", scope: "backtests:read" },
};

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
    const workspaces = await listAccessibleWorkspaces(options.db, principal);
    sendData(res, workspaces, requestId, { count: workspaces.length, next_cursor: null });
  }));

  router.get("/v1/workspaces/:workspaceId/:resource", wrap(options, async (req, res, principal, requestId) => {
    const definition = workspaceResources[text(req.params.resource, 80)];
    if (!definition) throw new Error("resource_not_found");
    const access = await resolveWorkspaceAccess(options.db, principal, req.params.workspaceId);
    authorizeWorkspaceAction(principal, access, definition.scope, "read");
    if (access.role === "owner" && principal.authMethod === "api_key" && !PLAN_ENTITLEMENTS[principal.plan].features.includes("api")) throw new Error("plan_upgrade_required");
    const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 200);
    const cursor = cursorDecode(req.query.cursor);
    let query: FirebaseFirestore.Query = options.db.collection(definition.collection).where(definition.ownerField, "==", access.workspaceId).orderBy("createdAt", "desc").limit(limit + 1);
    if (cursor) query = query.startAfter(new Date(cursor));
    const snapshot = await query.get();
    const visible = snapshot.docs.slice(0, limit);
    const data = visible.map((doc) => ({ id: doc.id, ...publicResource(plain(doc.data())) }));
    const last = visible[visible.length - 1];
    const lastDate = last?.data()?.createdAt?.toDate?.() as Date | undefined;
    const nextCursor = snapshot.size > limit && lastDate ? cursorEncode(lastDate.toISOString()) : null;
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
