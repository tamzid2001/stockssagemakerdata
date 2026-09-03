import crypto from "node:crypto";
import type { Request, Response, Router } from "express";
import type admin from "firebase-admin";
import {
  authenticatePlatformRequest,
  authorizeWorkspaceAction,
  requireWorkspacePermission,
  resolveWorkspaceAccess,
  writeApiAudit,
  type ApiPrincipal,
} from "./apiAccess";
import modelRegistry from "./ensembleModelRegistry.json";
import { fetchStockHistoryData } from "./marketDataRoutes";
import { PLAN_ENTITLEMENTS, type PlanKey } from "./planEntitlements";

type JsonRecord = Record<string, unknown>;
type Options = { db: FirebaseFirestore.Firestore; auth: admin.auth.Auth; publicOrigin: string };
type ModelId = "prophet" | "toto" | "granite" | "chronos" | "timesfm";
type Handler = (req: Request, res: Response, principal: ApiPrincipal, requestId: string) => Promise<void>;

const JOBS = "ensemble_forecast_jobs";
const RESULTS = "ensemble_forecast_results";
const PRESETS = "ensemble_forecast_presets";
const CACHE = "ensemble_forecast_cache";
const IDEMPOTENCY = "ensemble_forecast_idempotency";
const USAGE = "ensemble_forecast_usage";
const INPUT_CHUNKS = "input_chunks";
const APPROVED_MODELS: ModelId[] = ["prophet", "toto", "granite", "chronos", "timesfm"];
const MAX_HISTORY_ROWS = 10_000;
const INPUT_CHUNK_ROWS = 250;
const WORKER_SCHEMA_VERSION = "ensemble_forecast_job_v1";

function text(value: unknown, max = 500): string { return String(value ?? "").trim().slice(0, max); }
function plain(value: unknown): JsonRecord { return value && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {}; }
function finite(value: unknown): number | null { const result = Number(value); return Number.isFinite(result) ? result : null; }
function boolean(value: unknown, fallback = false): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "string" && /^(true|false)$/i.test(value.trim())) return value.trim().toLowerCase() === "true";
  return fallback;
}
function iso(value: unknown, fallback = ""): string {
  const raw = text(value, 100);
  const parsed = Date.parse(raw);
  if (!raw || !Number.isFinite(parsed)) return fallback;
  return new Date(parsed).toISOString();
}
function safeId(value: unknown, max = 220): string {
  return text(value, max).replace(/[^A-Za-z0-9._:-]+/g, "-").replace(/^-+|-+$/g, "");
}
function assertOnlyKeys(value: JsonRecord, allowed: readonly string[], context: string): void {
  const allow = new Set(allowed);
  if (Object.keys(value).some((key) => !allow.has(key))) throw new Error(`${context}_field_unsupported`);
}
function runtimeMode(): "production" | "development" | "test" {
  if (process.env.NODE_ENV === "test") return "test";
  return process.env.NODE_ENV === "production" ? "production" : "development";
}
function envTrue(name: string): boolean { return /^(1|true|yes|on)$/i.test(text(process.env[name], 20)); }

function apiError(error: unknown): { status: number; code: string; message: string } {
  const raw = text((error as any)?.message || error, 300).toLowerCase();
  const code = raw.toUpperCase().replace(/[^A-Z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "INVALID_REQUEST";
  if (/api_key_(missing|invalid|revoked|expired)|worker_token_invalid/.test(raw)) return { status: 401, code, message: "Authentication failed." };
  if (/insufficient_scope|workspace_(forbidden|read_only|permission_denied|resource_forbidden|owner_required)|plan_upgrade|required_entitlement|commercial_license/.test(raw)) return { status: 403, code, message: "This identity is not authorized for the requested operation." };
  if (/not_found/.test(raw)) return { status: 404, code, message: "The requested forecast resource was not found." };
  if (/already|idempotency_conflict|claim_conflict/.test(raw)) return { status: 409, code, message: "The request conflicts with the current forecast state." };
  if (/rate_limit|quota|concurrent/.test(raw)) return { status: 429, code, message: "The forecast compute limit has been reached." };
  if (/dispatch_not_configured|worker_unavailable/.test(raw)) return { status: 503, code, message: "Forecast workers are temporarily unavailable." };
  if (/unsupported|quantile|weight|prediction_length|context_length|source_|dataset_|transform|failure_policy|models_|history_/.test(raw)) {
    return { status: 422, code, message: "The forecast configuration is not supported." };
  }
  return { status: 400, code, message: "The request is invalid." };
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

function wrap(options: Options, handler: Handler): (req: Request, res: Response) => Promise<void> {
  return async (req, res) => {
    const requestId = crypto.randomUUID();
    const started = Date.now();
    let principal: ApiPrincipal | undefined;
    try {
      principal = await authenticatePlatformRequest(req, options);
      await handler(req, res, principal, requestId);
    } catch (error) {
      sendError(res, error, requestId);
    } finally {
      void writeApiAudit(options.db, {
        principal,
        workspaceId: text((req.body || {}).workspace_id || req.query.workspace_id, 220) || null,
        endpoint: req.path,
        method: req.method,
        status: res.statusCode,
        resource: text(req.params.forecastId || req.params.presetId, 220) || null,
        requestId,
        latencyMs: Date.now() - started,
      }).catch(() => undefined);
    }
  };
}

export function timesFmState(mode = runtimeMode()): { available: boolean; unavailable_reason: string | null; evaluation_only: boolean } {
  const access = envTrue("TIMESFM_HF_ACCESS_APPROVED");
  const commercial = envTrue("TIMESFM_COMMERCIAL_LICENSED");
  const evaluation = envTrue("ALLOW_NONCOMMERCIAL_TIMESFM") && mode !== "production";
  if (!access) return { available: false, unavailable_reason: "access_not_approved", evaluation_only: evaluation };
  if (mode === "production" && !commercial) return { available: false, unavailable_reason: "commercial_license_required", evaluation_only: false };
  if (!commercial && !evaluation) return { available: false, unavailable_reason: "commercial_license_required", evaluation_only: false };
  return { available: true, unavailable_reason: null, evaluation_only: !commercial };
}

function planAllowsModel(plan: PlanKey, modelId: ModelId): boolean {
  const features = PLAN_ENTITLEMENTS[plan].features;
  if (modelId === "prophet") return features.includes("meta_prophet") || features.includes("meta_prophet_limited");
  if (modelId === "chronos") return features.includes("chronos_2");
  return features.includes("forecast_ensemble");
}

export function publicModelCapabilities(plan: PlanKey = "free"): JsonRecord {
  const mode = runtimeMode();
  const timesfm = timesFmState(mode);
  const models = APPROVED_MODELS.map((id) => {
    const source = (modelRegistry.models as Record<string, any>)[id];
    const licensed = id !== "timesfm" || timesfm.available;
    return {
      id,
      name: source.name,
      checkpoint: source.checkpoint,
      available: licensed && planAllowsModel(plan, id),
      runtime_available: licensed,
      plan_available: planAllowsModel(plan, id),
      unavailable_reason: !licensed ? timesfm.unavailable_reason : !planAllowsModel(plan, id) ? "plan_upgrade_required" : null,
      evaluation_only: id === "timesfm" ? timesfm.evaluation_only : false,
      default_weight: source.defaultWeight,
      quantile_support: source.quantileSupport,
      max_prediction_length: source.maxPredictionLength,
      max_context_length: source.maxContextLength,
      default_device: source.defaultDevice,
      license: source.license || null,
    };
  });
  return {
    schema_version: modelRegistry.schemaVersion,
    default_quantiles: modelRegistry.defaultQuantiles,
    max_requested_quantiles: modelRegistry.maxRequestedQuantiles,
    runtime_mode: mode,
    models,
  };
}

function canonicalQuantile(value: number): string {
  return Number(value.toPrecision(12)).toString();
}

export function normalizeRequestedQuantiles(value: unknown): number[] {
  if (!Array.isArray(value) || !value.length) throw new Error("quantiles_required");
  const unique = new Map<string, number>();
  value.forEach((entry) => {
    const quantile = finite(entry);
    if (quantile === null || quantile <= 0 || quantile >= 1) throw new Error("quantile_invalid");
    unique.set(canonicalQuantile(quantile), quantile);
  });
  const output = [...unique.values()].sort((left, right) => left - right);
  if (output.length > Number(modelRegistry.maxRequestedQuantiles)) throw new Error("quantile_limit_exceeded");
  return output;
}

function modelSupportsQuantile(modelId: ModelId, quantile: number): boolean {
  const support = (modelRegistry.models as Record<string, any>)[modelId].quantileSupport;
  return !support.minimum || (quantile >= Number(support.minimum) && quantile <= Number(support.maximum));
}

type NormalizedConfiguration = {
  prediction_length: number;
  horizon_mode: "trading_sessions" | "calendar_days" | "frequency_periods";
  quantiles: number[];
  transform: "auto" | "log" | "none";
  context_length: number | null;
  failure_policy: "fail" | "renormalize";
  frequency: string;
  calendar: string;
  models: Record<ModelId, { enabled: boolean; weight: number }>;
  requested_weights: Record<ModelId, number>;
  effective_central_weights: Partial<Record<ModelId, number>>;
};

export function normalizeEnsembleConfiguration(body: JsonRecord, plan: PlanKey): NormalizedConfiguration {
  assertOnlyKeys(body, ["workspace_id", "source", "prediction_length", "horizon_mode", "quantiles", "transform", "context_length", "failure_policy", "model_failure_policy", "frequency", "calendar", "models"], "configuration");
  const predictionLength = Math.floor(Number(body.prediction_length ?? 30));
  const planMaximum: Record<PlanKey, number> = { free: 30, pro: 90, quant: 365, research: 512 };
  if (!Number.isFinite(predictionLength) || predictionLength < 1 || predictionLength > planMaximum[plan]) throw new Error("prediction_length_unsupported");
  const horizonModeRaw = text(body.horizon_mode || "trading_sessions", 40);
  if (!new Set(["trading_sessions", "calendar_days", "frequency_periods"]).has(horizonModeRaw)) throw new Error("horizon_mode_unsupported");
  const transformRaw = text(body.transform || "auto", 20);
  if (!new Set(["auto", "log", "none"]).has(transformRaw)) throw new Error("transform_unsupported");
  const failureRaw = text(body.model_failure_policy || body.failure_policy || "fail", 20);
  if (!new Set(["fail", "renormalize"]).has(failureRaw)) throw new Error("failure_policy_unsupported");
  const contextRaw = body.context_length;
  const contextLength = contextRaw === undefined || contextRaw === null || contextRaw === "" ? null : Math.floor(Number(contextRaw));
  if (contextLength !== null && (!Number.isFinite(contextLength) || contextLength < 40 || contextLength > 16384)) throw new Error("context_length_unsupported");
  const quantiles = normalizeRequestedQuantiles(body.quantiles || modelRegistry.defaultQuantiles);
  const modelsRaw = plain(body.models);
  assertOnlyKeys(modelsRaw, APPROVED_MODELS, "models");
  const models = {} as NormalizedConfiguration["models"];
  const requestedWeights = {} as Record<ModelId, number>;
  for (const modelId of APPROVED_MODELS) {
    const row = plain(modelsRaw[modelId]);
    assertOnlyKeys(row, ["enabled", "weight"], `${modelId}_configuration`);
    const enabled = boolean(row.enabled, modelId === "prophet");
    const weight = finite(row.weight ?? (enabled ? (modelRegistry.models as Record<string, any>)[modelId].defaultWeight : 0));
    if (weight === null || weight < 0) throw new Error(`${modelId}_weight_invalid`);
    if (enabled && !planAllowsModel(plan, modelId)) throw new Error(`${modelId}_required_entitlement`);
    if (enabled && predictionLength > Number((modelRegistry.models as Record<string, any>)[modelId].maxPredictionLength)) throw new Error(`${modelId}_prediction_length_unsupported`);
    if (enabled && contextLength && contextLength > Number((modelRegistry.models as Record<string, any>)[modelId].maxContextLength)) throw new Error(`${modelId}_context_length_unsupported`);
    if (enabled && modelId === "timesfm" && !timesFmState().available) throw new Error("timesfm_commercial_license_required");
    models[modelId] = { enabled, weight };
    requestedWeights[modelId] = weight;
  }
  const enabled = APPROVED_MODELS.filter((modelId) => models[modelId].enabled);
  if (!enabled.length) throw new Error("models_required");
  if (!enabled.some((modelId) => models[modelId].weight > 0)) throw new Error("models_positive_weight_required");
  quantiles.forEach((quantile) => {
    const supporting = enabled.filter((modelId) => models[modelId].weight > 0 && modelSupportsQuantile(modelId, quantile));
    if (!supporting.length) throw new Error(`quantile_${canonicalQuantile(quantile)}_unsupported`);
  });
  const central = enabled.filter((modelId) => models[modelId].weight > 0 && modelSupportsQuantile(modelId, 0.5));
  const centralTotal = central.reduce((sum, modelId) => sum + models[modelId].weight, 0);
  const effectiveCentralWeights = Object.fromEntries(central.map((modelId) => [modelId, models[modelId].weight / centralTotal]));
  return {
    prediction_length: predictionLength,
    horizon_mode: horizonModeRaw as NormalizedConfiguration["horizon_mode"],
    quantiles,
    transform: transformRaw as NormalizedConfiguration["transform"],
    context_length: contextLength,
    failure_policy: failureRaw as NormalizedConfiguration["failure_policy"],
    frequency: text(body.frequency || "1D", 30) || "1D",
    calendar: text(body.calendar || "NYSE", 30) || "NYSE",
    models,
    requested_weights: requestedWeights,
    effective_central_weights: effectiveCentralWeights,
  };
}

function normalizeSeriesRows(rows: unknown, timestampColumn: string, targetColumn: string): Array<{ timestamp: string; target: number }> {
  if (!Array.isArray(rows)) throw new Error("source_series_rows_required");
  if (rows.length > MAX_HISTORY_ROWS) throw new Error("history_row_limit_exceeded");
  const byTimestamp = new Map<string, number>();
  rows.forEach((entry) => {
    const row = plain(entry);
    const timestamp = iso(row[timestampColumn] ?? row.timestamp ?? row.date);
    const target = finite(row[targetColumn] ?? row.target ?? row.close ?? row.value);
    if (timestamp && target !== null) byTimestamp.set(timestamp, target);
  });
  const output = [...byTimestamp.entries()].sort(([left], [right]) => left.localeCompare(right)).map(([timestamp, target]) => ({ timestamp, target }));
  if (output.length < 40) throw new Error("history_minimum_rows_required");
  return output;
}

function parseCsv(textValue: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let quoted = false;
  for (let index = 0; index < textValue.length; index += 1) {
    const character = textValue[index];
    if (character === '"') {
      if (quoted && textValue[index + 1] === '"') { field += '"'; index += 1; }
      else quoted = !quoted;
    } else if (character === "," && !quoted) { row.push(field); field = ""; }
    else if ((character === "\n" || character === "\r") && !quoted) {
      if (character === "\r" && textValue[index + 1] === "\n") index += 1;
      row.push(field); field = "";
      if (row.some((value) => value.length)) rows.push(row);
      row = [];
    } else field += character;
  }
  row.push(field);
  if (row.some((value) => value.length)) rows.push(row);
  if (quoted) throw new Error("dataset_csv_malformed");
  return rows;
}

async function readFoundryArtifact(db: FirebaseFirestore.Firestore, runId: string, file: JsonRecord): Promise<string> {
  if (text(file.artifactStore, 40) !== "firestore") throw new Error("dataset_artifact_unavailable");
  const fileKey = safeId(file.firestoreKey || file.key, 80);
  if (!fileKey) throw new Error("dataset_artifact_unavailable");
  const ref = db.collection("autopilot_requests").doc(runId).collection("text_artifacts").doc(fileKey);
  const metadata = await ref.get();
  if (!metadata.exists) throw new Error("dataset_artifact_not_found");
  const count = Math.min(Math.max(Math.floor(Number(metadata.data()?.chunkCount || 0)), 0), 1000);
  const snapshots = await Promise.all(Array.from({ length: count }, (_, index) => ref.collection("chunks").doc(String(index).padStart(4, "0")).get()));
  const value = snapshots.map((snapshot) => text(snapshot.data()?.text, 950_000)).join("");
  if (!value || value.length > 8_000_000) throw new Error("dataset_artifact_invalid");
  return value;
}

async function materializeWorkspaceDataset(
  options: Options,
  principal: ApiPrincipal,
  workspaceId: string,
  source: JsonRecord
): Promise<{ rows: Array<{ timestamp: string; target: number }>; source: JsonRecord; frequency: string; timezone: string }> {
  const datasetId = safeId(source.dataset_id, 220);
  if (!datasetId) throw new Error("dataset_id_required");
  const access = await resolveWorkspaceAccess(options.db, principal, workspaceId);
  authorizeWorkspaceAction(principal, access, "forecasts:write", "write");
  const snapshot = await options.db.collection("autopilot_requests").doc(datasetId).get();
  if (!snapshot.exists) throw new Error("dataset_not_found");
  const data = plain(snapshot.data());
  const owner = text(data.workspaceId || data.userId, 220);
  if (owner !== workspaceId) throw new Error("dataset_workspace_forbidden");
  const timestampColumn = text(source.timestamp_column || "timestamp", 100) || "timestamp";
  const targetColumn = text(source.target_column || "target", 100) || "target";
  let rawRows: unknown = data.series || data.historicalRows || data.rows;
  if (!Array.isArray(rawRows)) {
    const files = plain(data.files);
    const file = plain(files.uploadedCsv || files.datasetCsv || files.predictionsCsv);
    const csvText = await readFoundryArtifact(options.db, datasetId, file);
    const csv = parseCsv(csvText);
    const headers = (csv.shift() || []).map((value) => value.trim());
    rawRows = csv.map((values) => Object.fromEntries(headers.map((header, index) => [header, values[index] ?? ""])));
  }
  return {
    rows: normalizeSeriesRows(rawRows, timestampColumn, targetColumn),
    source: { type: "workspace_dataset", dataset_id: datasetId, dataset_version: text(data.datasetVersion || data.updatedAt || data.createdAt, 120) || snapshot.updateTime?.toDate().toISOString() || "unknown" },
    frequency: text(source.frequency || data.frequency || "infer", 30) || "infer",
    timezone: text(source.timezone || data.timezone || "UTC", 60) || "UTC",
  };
}

async function materializeSource(
  options: Options,
  principal: ApiPrincipal,
  workspaceId: string,
  sourceValue: unknown
): Promise<{ rows: Array<{ timestamp: string; target: number }>; source: JsonRecord; frequency: string; timezone: string }> {
  const source = plain(sourceValue);
  const type = text(source.type || "ticker", 40);
  if (type === "ticker") {
    assertOnlyKeys(source, ["type", "symbol", "provider", "source", "start", "end", "field", "frequency", "adjustment", "session", "limit"], "source");
    const symbol = text(source.symbol, 30).toUpperCase();
    if (!/^(?:\^[A-Z0-9.\-]{1,23}|[A-Z0-9][A-Z0-9.^=\-]{0,23})$/.test(symbol)) throw new Error("source_symbol_invalid");
    const history = await fetchStockHistoryData({
      source: source.provider || source.source || "auto",
      symbol,
      start: source.start || "2000-01-01",
      end: source.end || new Date().toISOString(),
      timeframe: source.frequency || "1Day",
      adjustment: source.adjustment || "raw",
      session: source.session || "regular",
      limit: Math.min(Math.max(Math.floor(Number(source.limit) || 5000), 40), MAX_HISTORY_ROWS),
    });
    return {
      rows: normalizeSeriesRows(history.rows, "timestamp", text(source.field || "close", 50) || "close"),
      source: { type: "ticker", symbol, field: text(source.field || "close", 50), provider: history.provider, source_requested: history.sourceRequested, fallback_used: history.fallbackUsed, start: source.start || null, end: source.end || null },
      frequency: text(source.frequency || history.timeframe || "1D", 30),
      timezone: "UTC",
    };
  }
  if (type === "workspace_dataset") {
    assertOnlyKeys(source, ["type", "dataset_id", "timestamp_column", "target_column", "frequency", "timezone"], "source");
    return materializeWorkspaceDataset(options, principal, workspaceId, source);
  }
  if (type === "series") {
    assertOnlyKeys(source, ["type", "name", "rows", "timestamp_column", "target_column", "frequency", "timezone"], "source");
    return {
      rows: normalizeSeriesRows(source.rows, text(source.timestamp_column || "timestamp", 100), text(source.target_column || "target", 100)),
      source: { type: "series", name: text(source.name || "API historical series", 120) },
      frequency: text(source.frequency || "infer", 30),
      timezone: text(source.timezone || "UTC", 60),
    };
  }
  throw new Error("source_type_unsupported");
}

function datasetHash(rows: Array<{ timestamp: string; target: number }>, source: JsonRecord): string {
  return crypto.createHash("sha256").update(JSON.stringify({ rows, source })).digest("hex");
}

function requestHash(workspaceId: string, sourceHash: string, configuration: JsonRecord): string {
  return crypto.createHash("sha256").update(JSON.stringify({ workspaceId, sourceHash, configuration, registry: modelRegistry.schemaVersion })).digest("hex");
}

function approvedModelCheckpoints(configuration: NormalizedConfiguration): Record<ModelId, string | null> {
  return Object.fromEntries(APPROVED_MODELS
    .filter((modelId) => configuration.models[modelId].enabled && configuration.models[modelId].weight > 0)
    .map((modelId) => [modelId, (modelRegistry.models as Record<string, any>)[modelId].checkpoint || null])) as Record<ModelId, string | null>;
}

async function persistInputChunks(ref: FirebaseFirestore.DocumentReference, rows: Array<{ timestamp: string; target: number }>): Promise<void> {
  for (let offset = 0; offset < rows.length; offset += INPUT_CHUNK_ROWS) {
    await ref.collection(INPUT_CHUNKS).doc(String(offset / INPUT_CHUNK_ROWS).padStart(4, "0")).create({ rows: rows.slice(offset, offset + INPUT_CHUNK_ROWS) });
  }
}

async function loadInputRows(ref: FirebaseFirestore.DocumentReference): Promise<Array<{ timestamp: string; target: number }>> {
  const snapshots = await ref.collection(INPUT_CHUNKS).orderBy("__name__").limit(100).get();
  const rows = snapshots.docs.flatMap((doc) => Array.isArray(doc.data().rows) ? doc.data().rows : []);
  return normalizeSeriesRows(rows, "timestamp", "target");
}

async function enforceComputeQuota(options: Options, plan: PlanKey, workspaceId: string): Promise<void> {
  const date = new Date().toISOString().slice(0, 10);
  const limit = PLAN_ENTITLEMENTS[plan].forecastComputePerDay;
  const ref = options.db.collection(USAGE).doc(crypto.createHash("sha256").update(`${workspaceId}:${date}`).digest("hex"));
  await options.db.runTransaction(async (transaction) => {
    const snapshot = await transaction.get(ref);
    const count = Number(snapshot.data()?.count || 0);
    const active = Number(snapshot.data()?.active || 0);
    const concurrentLimit = plan === "research" ? 5 : plan === "quant" ? 3 : 1;
    if (count >= limit) throw new Error("forecast_daily_quota_exceeded");
    if (active >= concurrentLimit) throw new Error("forecast_concurrent_limit_exceeded");
    transaction.set(ref, { workspace_id: workspaceId, date, count: count + 1, active: active + 1, updated_at: new Date().toISOString() }, { merge: true });
  });
}

async function releaseComputeSlot(options: Options, workspaceId: string): Promise<void> {
  const date = new Date().toISOString().slice(0, 10);
  const ref = options.db.collection(USAGE).doc(crypto.createHash("sha256").update(`${workspaceId}:${date}`).digest("hex"));
  await options.db.runTransaction(async (transaction) => {
    const snapshot = await transaction.get(ref);
    transaction.set(ref, { active: Math.max(0, Number(snapshot.data()?.active || 0) - 1), updated_at: new Date().toISOString() }, { merge: true });
  }).catch(() => undefined);
}

async function dispatchWorker(jobId: string): Promise<void> {
  const mode = text(process.env.QUANTURA_ENSEMBLE_WORKER_MODE || "github_actions", 40);
  if (mode === "manual") {
    if (!envTrue("QUANTURA_ENSEMBLE_ALLOW_MANUAL_CLAIM")) throw new Error("worker_dispatch_not_configured");
    return;
  }
  if (mode !== "github_actions") throw new Error("worker_dispatch_not_configured");
  const token = text(process.env.GITHUB_ACTIONS_TOKEN, 1000);
  const owner = text(process.env.GITHUB_REPO_OWNER || "tamzid2001", 120);
  const repository = text(process.env.GITHUB_REPO_NAME || "stockssagemakerdata", 160);
  if (!token || !owner || !repository) throw new Error("worker_dispatch_not_configured");
  const response = await fetch(`https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repository)}/actions/workflows/ensemble-forecast.yml/dispatches`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "Content-Type": "application/json" },
    body: JSON.stringify({ ref: text(process.env.GITHUB_WORKFLOW_REF || "main", 120), inputs: { forecast_job_id: jobId, environment: runtimeMode(), smoke_mode: "real" } }),
    signal: AbortSignal.timeout(20_000),
  });
  if (!response.ok) throw new Error("worker_unavailable");
}

export function publicEnsembleJob(jobId: string, data: JsonRecord, result?: JsonRecord | null): JsonRecord {
  const output: JsonRecord = {
    forecast_id: jobId,
    status: data.status,
    created_at: data.created_at,
    started_at: data.started_at || null,
    completed_at: data.completed_at || null,
    workspace_id: data.workspace_id,
    source: data.source,
    prediction_length: plain(data.request).prediction_length,
    horizon_mode: plain(data.request).horizon_mode,
    quantiles: plain(data.request).quantiles,
    transform: plain(data.request).transform,
    context_length: plain(data.request).context_length,
    frequency: plain(data.request).frequency,
    calendar: plain(data.request).calendar,
    model_failure_policy: plain(data.request).failure_policy,
    models: plain(data.request).models,
    model_checkpoints: data.model_checkpoints,
    requested_weights: data.requested_weights,
    effective_central_weights: data.effective_central_weights,
    progress: data.progress || null,
    warnings: data.warnings || [],
    error: data.error || null,
    status_url: `/api/v1/ensemble-forecasts/${encodeURIComponent(jobId)}`,
    result_url: `/api/v1/ensemble-forecasts/${encodeURIComponent(jobId)}`,
    reproduces_forecast_id: data.reproduces_forecast_id || null,
    registry_version: data.registry_version,
  };
  if (result) {
    output.predictions = result.predictions;
    output.effective_weights_by_quantile = result.effective_weights_by_quantile;
    output.runtime_seconds = result.runtime_seconds;
    output.dataset_hash = result.dataset_hash;
    output.prepared_series_hash = result.prepared_series_hash;
    output.result_hash = result.result_hash;
    output.model_runtime = Array.isArray(result.models) ? result.models : [];
  }
  return output;
}

function validWorkerToken(req: Request): boolean {
  const configured = Buffer.from(text(process.env.QUANTURA_ENSEMBLE_WORKER_TOKEN, 2000));
  const supplied = Buffer.from((text(req.headers.authorization, 2200).match(/^Bearer\s+(.+)$/i)?.[1] || "").trim());
  return configured.length >= 32 && configured.length === supplied.length && crypto.timingSafeEqual(configured, supplied);
}

export function validateWorkerResult(body: JsonRecord, job: JsonRecord): { quantiles: number[]; predictions: JsonRecord[] } {
  const requested = normalizeRequestedQuantiles(plain(job.request).quantiles);
  const quantiles = normalizeRequestedQuantiles(body.quantiles);
  if (requested.map(canonicalQuantile).join(",") !== quantiles.map(canonicalQuantile).join(",")) throw new Error("forecast_result_quantiles_invalid");
  if (!Array.isArray(body.predictions) || !body.predictions.length) throw new Error("forecast_result_invalid");
  const predictions = body.predictions.map(plain);
  const requestedLength = Number(plain(job.request).prediction_length || 0);
  const horizonMode = text(plain(job.request).horizon_mode, 40);
  if (predictions.length > requestedLength || (horizonMode !== "calendar_days" && predictions.length !== requestedLength)) throw new Error("forecast_result_length_invalid");
  let priorTimestamp = "";
  for (const row of predictions) {
    const timestamp = iso(row.timestamp);
    if (!timestamp || timestamp <= priorTimestamp) throw new Error("forecast_result_timestamp_invalid");
    priorTimestamp = timestamp;
    const values = plain(row.quantiles);
    let previous = Number.NEGATIVE_INFINITY;
    for (const quantile of quantiles) {
      const value = finite(values[canonicalQuantile(quantile)]);
      if (value === null || value < previous) throw new Error("forecast_result_ordering_invalid");
      if (text(body.transform, 20) === "log" && value <= 0) throw new Error("forecast_result_transform_invalid");
      previous = value;
    }
  }
  const weights = plain(body.effective_weights_by_quantile);
  for (const quantile of quantiles) {
    const row = plain(weights[canonicalQuantile(quantile)]);
    const entries = Object.entries(row);
    if (!entries.length || entries.some(([modelId, weight]) => !APPROVED_MODELS.includes(modelId as ModelId) || finite(weight) === null || Number(weight) < 0)) throw new Error("forecast_result_weights_invalid");
    const total = entries.reduce((sum, [, weight]) => sum + Number(weight), 0);
    if (Math.abs(total - 1) > 1e-5) throw new Error("forecast_result_weights_invalid");
  }
  return { quantiles, predictions };
}

function internal(options: Options, handler: (req: Request, res: Response, requestId: string) => Promise<void>) {
  return async (req: Request, res: Response) => {
    const requestId = crypto.randomUUID();
    res.setHeader("X-Request-ID", requestId);
    res.setHeader("Cache-Control", "no-store");
    try {
      if (!validWorkerToken(req)) throw new Error("worker_token_invalid");
      await handler(req, res, requestId);
    } catch (error) {
      sendError(res, error, requestId);
    }
  };
}

export function registerEnsembleForecastRoutes(router: Router, options: Options): void {
  router.get("/v1/forecast/models", wrap(options, async (req, res, principal, requestId) => {
    const access = await resolveWorkspaceAccess(options.db, principal, text(req.query.workspace_id || principal.userId, 220));
    authorizeWorkspaceAction(principal, access, "forecasts:read", "read");
    requireWorkspacePermission(access, "forecast.read");
    sendData(res, publicModelCapabilities(access.plan), requestId);
  }));
  router.get("/v1/ensemble-forecasts/models", wrap(options, async (req, res, principal, requestId) => {
    const access = await resolveWorkspaceAccess(options.db, principal, text(req.query.workspace_id || principal.userId, 220));
    authorizeWorkspaceAction(principal, access, "forecasts:read", "read");
    requireWorkspacePermission(access, "forecast.read");
    sendData(res, publicModelCapabilities(access.plan), requestId);
  }));

  router.post("/v1/ensemble-forecasts", wrap(options, async (req, res, principal, requestId) => {
    const body = plain(req.body);
    assertOnlyKeys(body, ["workspace_id", "source", "prediction_length", "horizon_mode", "quantiles", "transform", "context_length", "failure_policy", "model_failure_policy", "frequency", "calendar", "models"], "request");
    const workspaceId = text(body.workspace_id || principal.userId, 220);
    const access = await resolveWorkspaceAccess(options.db, principal, workspaceId);
    authorizeWorkspaceAction(principal, access, "forecasts:write", "write");
    requireWorkspacePermission(access, "forecast.create");
    const configuration = normalizeEnsembleConfiguration(body, access.plan);
    const materialized = await materializeSource(options, principal, workspaceId, body.source);
    const sourceHash = datasetHash(materialized.rows, materialized.source);
    const normalizedRequest = {
      prediction_length: configuration.prediction_length,
      horizon_mode: configuration.horizon_mode,
      quantiles: configuration.quantiles,
      transform: configuration.transform,
      context_length: configuration.context_length,
      failure_policy: configuration.failure_policy,
      frequency: materialized.frequency,
      calendar: configuration.calendar,
      models: configuration.models,
    };
    const hash = requestHash(workspaceId, sourceHash, normalizedRequest);
    const idempotencyKey = text(req.headers["idempotency-key"], 180);
    if (idempotencyKey) {
      const idempotencyId = crypto.createHash("sha256").update(`${principal.userId}:${idempotencyKey}`).digest("hex");
      const existing = await options.db.collection(IDEMPOTENCY).doc(idempotencyId).get();
      if (existing.exists) {
        if (text(existing.data()?.request_hash, 128) !== hash) throw new Error("idempotency_conflict");
        const existingId = text(existing.data()?.forecast_id, 220);
        const existingJob = await options.db.collection(JOBS).doc(existingId).get();
        if (existingJob.exists) {
          res.status(202);
          sendData(res, publicEnsembleJob(existingId, plain(existingJob.data())), requestId, { reused: true });
          return;
        }
      }
    }
    const cacheId = crypto.createHash("sha256").update(`${workspaceId}:${hash}`).digest("hex");
    const cached = await options.db.collection(CACHE).doc(cacheId).get();
    if (cached.exists) {
      const cachedJobId = text(cached.data()?.forecast_id, 220);
      const [job, result] = await Promise.all([options.db.collection(JOBS).doc(cachedJobId).get(), options.db.collection(RESULTS).doc(cachedJobId).get()]);
      if (job.exists && result.exists && text(job.data()?.status, 40) === "completed") {
        sendData(res, publicEnsembleJob(cachedJobId, plain(job.data()), plain(result.data())), requestId, { cache_hit: true });
        return;
      }
    }
    await enforceComputeQuota(options, access.plan, workspaceId);
    const ref = options.db.collection(JOBS).doc();
    const now = new Date().toISOString();
    const job = {
      schema_version: WORKER_SCHEMA_VERSION,
      forecast_id: ref.id,
      user_id: principal.userId,
      workspace_id: workspaceId,
      api_key_id: principal.tokenId,
      request: normalizedRequest,
      requested_weights: configuration.requested_weights,
      effective_central_weights: configuration.effective_central_weights,
      source: materialized.source,
      dataset_hash: sourceHash,
      model_checkpoints: approvedModelCheckpoints(configuration),
      input_row_count: materialized.rows.length,
      input_timestamp_column: "timestamp",
      input_target_column: "target",
      input_timezone: materialized.timezone,
      request_hash: hash,
      registry_version: modelRegistry.schemaVersion,
      runtime_mode: runtimeMode(),
      status: "queued",
      progress: { completed_models: 0, total_models: Object.values(configuration.models).filter((model) => model.enabled && model.weight > 0).length, current_model: null },
      warnings: [],
      error: null,
      created_at: now,
      started_at: null,
      completed_at: null,
    };
    await ref.create(job);
    try {
      await persistInputChunks(ref, materialized.rows);
      if (idempotencyKey) {
        const idempotencyId = crypto.createHash("sha256").update(`${principal.userId}:${idempotencyKey}`).digest("hex");
        await options.db.collection(IDEMPOTENCY).doc(idempotencyId).create({ forecast_id: ref.id, request_hash: hash, created_at: now });
      }
      await dispatchWorker(ref.id);
      await ref.set({ dispatched_at: new Date().toISOString(), dispatch_backend: text(process.env.QUANTURA_ENSEMBLE_WORKER_MODE || "github_actions", 40) }, { merge: true });
    } catch (error) {
      await ref.set({ status: "failed", error: { code: "WORKER_DISPATCH_FAILED", retryable: true }, completed_at: new Date().toISOString() }, { merge: true });
      await releaseComputeSlot(options, workspaceId);
      throw error;
    }
    res.status(202);
    sendData(res, publicEnsembleJob(ref.id, job), requestId);
  }));

  router.post("/v1/ensemble-forecasts/:forecastId/reproduce", wrap(options, async (req, res, principal, requestId) => {
    const originalId = safeId(req.params.forecastId, 220);
    const originalRef = options.db.collection(JOBS).doc(originalId);
    const originalSnapshot = await originalRef.get();
    if (!originalSnapshot.exists) throw new Error("forecast_job_not_found");
    const original = plain(originalSnapshot.data());
    const workspaceId = text(original.workspace_id, 220);
    const access = await resolveWorkspaceAccess(options.db, principal, workspaceId);
    authorizeWorkspaceAction(principal, access, "forecasts:read", "read");
    requireWorkspacePermission(access, "forecast.read", originalId);
    authorizeWorkspaceAction(principal, access, "forecasts:write", "write");
    requireWorkspacePermission(access, "forecast.create");
    const configuration = normalizeEnsembleConfiguration(plain(original.request), access.plan);
    await enforceComputeQuota(options, access.plan, workspaceId);
    const rows = await loadInputRows(originalRef);
    const ref = options.db.collection(JOBS).doc();
    const now = new Date().toISOString();
    const checkpoints = plain(original.model_checkpoints);
    const job = {
      schema_version: WORKER_SCHEMA_VERSION,
      forecast_id: ref.id,
      user_id: principal.userId,
      workspace_id: workspaceId,
      api_key_id: principal.tokenId,
      request: original.request,
      requested_weights: configuration.requested_weights,
      effective_central_weights: configuration.effective_central_weights,
      source: original.source,
      dataset_hash: original.dataset_hash,
      input_row_count: rows.length,
      input_timestamp_column: "timestamp",
      input_target_column: "target",
      input_timezone: original.input_timezone || "UTC",
      request_hash: original.request_hash,
      registry_version: original.registry_version,
      model_checkpoints: Object.keys(checkpoints).length ? checkpoints : approvedModelCheckpoints(configuration),
      runtime_mode: runtimeMode(),
      status: "queued",
      progress: { completed_models: 0, total_models: Object.values(configuration.models).filter((model) => model.enabled && model.weight > 0).length, current_model: null },
      warnings: [],
      error: null,
      reproduces_forecast_id: originalId,
      created_at: now,
      started_at: null,
      completed_at: null,
    };
    try {
      await ref.create(job);
      await persistInputChunks(ref, rows);
      await dispatchWorker(ref.id);
      await ref.set({ dispatched_at: new Date().toISOString(), dispatch_backend: text(process.env.QUANTURA_ENSEMBLE_WORKER_MODE || "github_actions", 40) }, { merge: true });
    } catch (error) {
      await ref.set({ status: "failed", error: { code: "WORKER_DISPATCH_FAILED", retryable: true }, completed_at: new Date().toISOString() }, { merge: true }).catch(() => undefined);
      await releaseComputeSlot(options, workspaceId);
      throw error;
    }
    res.status(202);
    sendData(res, publicEnsembleJob(ref.id, job), requestId, { reproduced_from: originalId });
  }));

  router.get("/v1/ensemble-forecasts/:forecastId", wrap(options, async (req, res, principal, requestId) => {
    const forecastId = safeId(req.params.forecastId, 220);
    const job = await options.db.collection(JOBS).doc(forecastId).get();
    if (!job.exists) throw new Error("forecast_job_not_found");
    const data = plain(job.data());
    const access = await resolveWorkspaceAccess(options.db, principal, data.workspace_id);
    authorizeWorkspaceAction(principal, access, "forecasts:read", "read");
    requireWorkspacePermission(access, "forecast.read", forecastId);
    const result = text(data.status, 40) === "completed" ? await options.db.collection(RESULTS).doc(forecastId).get() : null;
    sendData(res, publicEnsembleJob(forecastId, data, result?.exists ? plain(result.data()) : null), requestId);
  }));

  router.get("/v1/ensemble-forecasts/:forecastId/download", wrap(options, async (req, res, principal, requestId) => {
    const forecastId = safeId(req.params.forecastId, 220);
    const [job, result] = await Promise.all([options.db.collection(JOBS).doc(forecastId).get(), options.db.collection(RESULTS).doc(forecastId).get()]);
    if (!job.exists || !result.exists || text(job.data()?.status, 40) !== "completed") throw new Error("forecast_result_not_found");
    const data = plain(job.data());
    const access = await resolveWorkspaceAccess(options.db, principal, data.workspace_id);
    authorizeWorkspaceAction(principal, access, "forecasts:read", "read");
    requireWorkspacePermission(access, "forecast.read", forecastId);
    const payload = plain(result.data());
    const format = text(req.query.format || "csv", 20).toLowerCase();
    const filename = `quantura-ensemble-${forecastId}`;
    if (format === "json") {
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.setHeader("Content-Disposition", `attachment; filename="${filename}.json"`);
      res.status(200).send(JSON.stringify(publicEnsembleJob(forecastId, data, payload), null, 2));
      return;
    }
    if (format !== "csv") throw new Error("download_format_unsupported");
    const quantiles = Array.isArray(payload.quantiles) ? payload.quantiles.map(Number) : [];
    const headers = ["timestamp", ...quantiles.map((quantile) => `q_${canonicalQuantile(quantile)}`)];
    const escape = (value: unknown) => /[",\r\n]/.test(String(value ?? "")) ? `"${String(value ?? "").replace(/"/g, '""')}"` : String(value ?? "");
    const rows = (Array.isArray(payload.predictions) ? payload.predictions : []).map((entry) => {
      const row = plain(entry);
      const structured = plain(row.quantiles);
      return [row.timestamp, ...quantiles.map((quantile) => structured[canonicalQuantile(quantile)])].map(escape).join(",");
    });
    res.setHeader("X-Request-ID", requestId);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}.csv"`);
    res.status(200).send([headers.join(","), ...rows].join("\n") + "\n");
  }));

  router.get("/v1/ensemble-forecast-presets", wrap(options, async (req, res, principal, requestId) => {
    const workspaceId = text(req.query.workspace_id || principal.userId, 220);
    const access = await resolveWorkspaceAccess(options.db, principal, workspaceId);
    authorizeWorkspaceAction(principal, access, "forecasts:read", "read");
    requireWorkspacePermission(access, "forecast.read");
    const snapshot = await options.db.collection(PRESETS).where("workspace_id", "==", workspaceId).limit(100).get();
    sendData(res, snapshot.docs.map((doc) => ({ preset_id: doc.id, ...plain(doc.data()) })), requestId, { count: snapshot.size });
  }));

  router.post("/v1/ensemble-forecast-presets", wrap(options, async (req, res, principal, requestId) => {
    const body = plain(req.body);
    const workspaceId = text(body.workspace_id || principal.userId, 220);
    const access = await resolveWorkspaceAccess(options.db, principal, workspaceId);
    authorizeWorkspaceAction(principal, access, "forecasts:write", "write");
    requireWorkspacePermission(access, "forecast.create");
    const name = text(body.name, 100);
    if (!name) throw new Error("preset_name_required");
    const configuration = normalizeEnsembleConfiguration(plain(body.configuration), access.plan);
    const ref = options.db.collection(PRESETS).doc();
    const now = new Date().toISOString();
    await ref.create({ workspace_id: workspaceId, user_id: principal.userId, name, configuration, created_at: now, updated_at: now });
    res.status(201);
    sendData(res, { preset_id: ref.id, name, configuration, created_at: now }, requestId);
  }));

  router.delete("/v1/ensemble-forecast-presets/:presetId", wrap(options, async (req, res, principal, requestId) => {
    const ref = options.db.collection(PRESETS).doc(safeId(req.params.presetId, 220));
    const snapshot = await ref.get();
    if (!snapshot.exists) throw new Error("preset_not_found");
    const access = await resolveWorkspaceAccess(options.db, principal, snapshot.data()?.workspace_id);
    authorizeWorkspaceAction(principal, access, "forecasts:write", "delete");
    requireWorkspacePermission(access, "forecast.delete");
    await ref.delete();
    sendData(res, { deleted: true, preset_id: snapshot.id }, requestId);
  }));

  router.post("/internal/ensemble-forecasts/:forecastId/claim", internal(options, async (req, res, requestId) => {
    const ref = options.db.collection(JOBS).doc(safeId(req.params.forecastId, 220));
    const job = await options.db.runTransaction(async (transaction) => {
      const snapshot = await transaction.get(ref);
      if (!snapshot.exists) throw new Error("forecast_job_not_found");
      const data = plain(snapshot.data());
      const status = text(data.status, 40);
      if (status !== "queued" && status !== "running") throw new Error("forecast_claim_conflict");
      if (status === "running" && iso(data.lease_expires_at) && Date.parse(String(data.lease_expires_at)) > Date.now()) throw new Error("forecast_claim_conflict");
      const now = new Date().toISOString();
      transaction.set(ref, { status: "running", started_at: data.started_at || now, lease_expires_at: new Date(Date.now() + 5 * 60 * 60_000).toISOString(), worker_claim_id: requestId }, { merge: true });
      return data;
    });
    const rows = await loadInputRows(ref);
    sendData(res, {
      forecast_id: ref.id,
      request: job.request,
      source: job.source,
      dataset_hash: job.dataset_hash,
      model_checkpoints: job.model_checkpoints,
      registry_version: job.registry_version,
      runtime_mode: job.runtime_mode,
      maximum_history_rows: MAX_HISTORY_ROWS,
      input: { rows, timestamp_column: job.input_timestamp_column, target_column: job.input_target_column, frequency: plain(job.request).frequency, timezone: job.input_timezone },
    }, requestId);
  }));

  router.post("/internal/ensemble-forecasts/:forecastId/progress", internal(options, async (req, res, requestId) => {
    const ref = options.db.collection(JOBS).doc(safeId(req.params.forecastId, 220));
    const snapshot = await ref.get();
    if (!snapshot.exists || text(snapshot.data()?.status, 40) !== "running") throw new Error("forecast_job_not_found");
    const body = plain(req.body);
    const total = Math.max(1, Math.min(Math.floor(Number(body.total_models) || 1), APPROVED_MODELS.length));
    const completed = Math.max(0, Math.min(Math.floor(Number(body.completed_models) || 0), total));
    const current = text(body.current_model, 40);
    if (current && !APPROVED_MODELS.includes(current as ModelId)) throw new Error("model_unsupported");
    await ref.set({ progress: { completed_models: completed, total_models: total, current_model: current || null }, lease_expires_at: new Date(Date.now() + 5 * 60 * 60_000).toISOString(), updated_at: new Date().toISOString() }, { merge: true });
    sendData(res, { updated: true }, requestId);
  }));

  router.post("/internal/ensemble-forecasts/:forecastId/complete", internal(options, async (req, res, requestId) => {
    const ref = options.db.collection(JOBS).doc(safeId(req.params.forecastId, 220));
    const snapshot = await ref.get();
    if (!snapshot.exists || text(snapshot.data()?.status, 40) !== "running") throw new Error("forecast_job_not_found");
    const job = plain(snapshot.data());
    const body = plain(req.body);
    if (text(body.dataset_hash, 128) !== text(job.dataset_hash, 128)) throw new Error("forecast_result_invalid");
    const validated = validateWorkerResult(body, job);
    const result = {
      forecast_id: ref.id,
      schema_version: WORKER_SCHEMA_VERSION,
      predictions: validated.predictions,
      quantiles: validated.quantiles,
      effective_weights_by_quantile: body.effective_weights_by_quantile,
      models: body.models,
      model_runs: body.model_runs,
      transform: body.transform,
      warnings: body.warnings,
      failures: body.failures,
      dataset_hash: body.dataset_hash,
      prepared_series_hash: body.prepared_series_hash,
      result_hash: body.result_hash,
      runtime_seconds: body.runtime_seconds,
      runtime: body.runtime,
      created_at: new Date().toISOString(),
    };
    await options.db.collection(RESULTS).doc(ref.id).create(result);
    const completedAt = new Date().toISOString();
    await ref.set({ status: "completed", completed_at: completedAt, lease_expires_at: null, warnings: body.warnings || [], progress: { completed_models: Array.isArray(body.models) ? body.models.length : 0, total_models: Array.isArray(body.models) ? body.models.length : 0, current_model: null } }, { merge: true });
    const cacheId = crypto.createHash("sha256").update(`${text(job.workspace_id, 220)}:${text(job.request_hash, 128)}`).digest("hex");
    await options.db.collection(CACHE).doc(cacheId).set({ forecast_id: ref.id, request_hash: job.request_hash, completed_at: completedAt }, { merge: false });
    await releaseComputeSlot(options, text(job.workspace_id, 220));
    sendData(res, { completed: true, forecast_id: ref.id }, requestId);
  }));

  router.post("/internal/ensemble-forecasts/:forecastId/fail", internal(options, async (req, res, requestId) => {
    const ref = options.db.collection(JOBS).doc(safeId(req.params.forecastId, 220));
    const snapshot = await ref.get();
    if (!snapshot.exists) throw new Error("forecast_job_not_found");
    const job = plain(snapshot.data());
    const body = plain(req.body);
    await ref.set({ status: "failed", completed_at: new Date().toISOString(), lease_expires_at: null, error: { code: text(body.code || "FORECAST_JOB_FAILED", 100), model: text(body.model, 40) || null, retryable: boolean(body.retryable) } }, { merge: true });
    await releaseComputeSlot(options, text(job.workspace_id, 220));
    sendData(res, { failed: true, forecast_id: ref.id }, requestId);
  }));
}
