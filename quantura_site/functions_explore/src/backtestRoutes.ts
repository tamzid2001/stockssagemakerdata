import crypto from "node:crypto";
import type { Request, Response, Router } from "express";
import type admin from "firebase-admin";
import { authorizeWorkspaceAction, requireScope, requireWorkspacePermission, resolveWorkspaceAccess } from "./apiAccess";
import { withPlatformAccess } from "./platformApiRoutes";
import { PLAN_ENTITLEMENTS } from "./planEntitlements";
import { fetchStockHistoryData } from "./marketDataRoutes";
import { PredictionMarketDataError, predictionForecastHistory } from "./predictionMarketData";
import { historySelection } from "./eventHistory";
import { approvedModelCheckpoints, approvedModelRevisions, normalizeEnsembleConfiguration, validateModelHistory } from "./ensembleForecastRoutes";
import { DEFAULT_BACKTEST_EXECUTION, validateBacktestExecution } from "./backtestEngine";
import { DEFAULT_QUANTILE_REPLAY, DEFAULT_QUANTILE_STRATEGY, QUANTILE_STRATEGY_SCHEMA,
  validateQuantileReplay, validateQuantileStrategy } from "./quantileBacktestConfig";

type Options = { db: FirebaseFirestore.Firestore; auth: admin.auth.Auth; publicOrigin: string;
  stockHistory?: typeof fetchStockHistoryData; marketHistory?: typeof predictionForecastHistory;
  dispatch?: (id: string) => Promise<void> };
type Source = { type: "ticker"; symbol: string; provider: "auto" | "alpaca" | "yahoo"; frequency: "1Day" | "1Hour" | "1Min"; end?: string }
  | { type: "prediction_market"; provider: "kalshi" | "polymarket_us"; symbol: string; contract_id: string;
    frequency: "1min" | "1h" | "1D"; history_phase: "both" | "pregame" | "in_game"; end?: string };
type Bar = { timestamp: string; target: number; open: number; close: number };

const COLLECTION = "forecast_backtests";
const RESULTS = "forecast_backtest_results";
const CHUNKS = "input_chunks";
const CHUNK_SIZE = 200;
const MAX_BARS = 2000;
const MAX_WINDOWS_STEPS = 240;

function invalid(message: string): never { throw new Error(`BACKTEST_REQUEST_INVALID: ${message}`); }
function object(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : invalid("Expected a JSON object.");
}
function onlyKeys(row: Record<string, unknown>, keys: readonly string[]): void {
  if (Object.keys(row).some(key => !keys.includes(key))) invalid("An unsupported field was supplied.");
}
function parseEnd(value: unknown): { end?: string } {
  if (value === undefined || value === null || value === "") return {};
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,3})?)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) invalid("End time must be an ISO-8601 timestamp with timezone.");
  const time = Date.parse(value);
  if (!Number.isFinite(time) || time > Date.now()) invalid("End time must be a valid past timestamp.");
  return { end: new Date(time).toISOString() };
}
function parseSource(value: unknown): Source {
  const row = object(value);
  if (row.type === "ticker") {
    onlyKeys(row, ["type", "symbol", "provider", "frequency", "end"]);
    const symbol = String(row.symbol || "").trim().toUpperCase();
    const provider = row.provider || "auto", frequency = row.frequency || "1Day";
    if (!/^[A-Z0-9.^=-]{1,20}$/.test(symbol) || !["auto", "alpaca", "yahoo"].includes(String(provider)) ||
        !["1Day", "1Hour", "1Min"].includes(String(frequency))) invalid("Choose a supported ticker, provider and observed interval.");
    return { type: "ticker", symbol, provider: provider as "auto" | "alpaca" | "yahoo",
      frequency: frequency as "1Day" | "1Hour" | "1Min", ...parseEnd(row.end) };
  }
  if (row.type === "prediction_market") {
    onlyKeys(row, ["type", "provider", "symbol", "contract_id", "frequency", "history_phase", "end"]);
    const provider = row.provider, symbol = String(row.symbol || "").trim(), contract_id = String(row.contract_id || "").trim();
    const frequency = row.frequency || "1min", history_phase = row.history_phase || "both";
    if (!["kalshi", "polymarket_us"].includes(String(provider)) || !/^[\w:./-]{1,180}$/.test(symbol) ||
        !/^[\w:./-]{1,240}$/.test(contract_id) || !["1min", "1h", "1D"].includes(String(frequency)) ||
        !["both", "pregame", "in_game"].includes(String(history_phase))) invalid("Select one supported event contract and interval.");
    return { type: "prediction_market", provider: provider as "kalshi" | "polymarket_us", symbol, contract_id,
      frequency: frequency as "1min" | "1h" | "1D", history_phase: history_phase as "both" | "pregame" | "in_game", ...parseEnd(row.end) };
  }
  return invalid("Choose a ticker or an individual Kalshi / Polymarket US contract.");
}
function normalizedFrequency(source: Source): "1D" | "1h" | "1min" {
  if (source.type === "prediction_market") return source.frequency;
  return source.frequency === "1Day" ? "1D" : source.frequency === "1Hour" ? "1h" : "1min";
}
function send(res: Response, requestId: string, data: unknown, status = 200): void {
  res.setHeader("X-Request-ID", requestId);
  res.setHeader("Cache-Control", "private, no-store");
  res.status(status).json({ data, meta: { api_version: "v1" } });
}
function publicRecord(record: Record<string, any>): Record<string, unknown> {
  const { user_id: _owner, api_key_id: _token, worker_claim_id: _claim, ...safe } = record;
  return safe;
}
function sendSafeError(res: Response, requestId: string, error: unknown): void {
  const message = String((error as Error)?.message || "");
  const validation = /^(BACKTEST_[A-Z_]+): (.{1,240})$/.exec(message);
  const quota = message === "BACKTEST_MONTHLY_LIMIT" || message === "BACKTEST_CONCURRENT_LIMIT";
  const providerError = error instanceof PredictionMarketDataError ? error : null;
  const settingCode = /^[a-z][a-z0-9_]{2,70}$/.test(message) ? message.toUpperCase() : null;
  res.status(quota ? 429 : (validation || settingCode || providerError) ? 422 : 503).json({ error: {
    code: quota ? message : validation?.[1] || providerError?.code.toUpperCase() || settingCode || "BACKTEST_DATA_UNAVAILABLE",
    message: quota ? "Backtest compute allowance reached." : validation?.[2] ||
      (providerError ? providerError.message : settingCode ? message.replaceAll("_", " ") : "Historical data or a backtest worker is unavailable. Try another interval or range."),
    request_id: requestId,
  } });
}
function normalizedBars(input: Bar[]): Bar[] {
  const byTimestamp = new Map<string, Bar>();
  for (const row of input) {
    const time = Date.parse(row.timestamp);
    if (!Number.isFinite(time) || ![row.target, row.open, row.close].every(Number.isFinite)) continue;
    byTimestamp.set(new Date(time).toISOString(), { timestamp: new Date(time).toISOString(), target: row.target, open: row.open, close: row.close });
  }
  return [...byTimestamp.values()].sort((a, b) => a.timestamp.localeCompare(b.timestamp)).slice(-MAX_BARS);
}
async function loadBars(options: Options, source: Source): Promise<{ bars: Bar[]; provider: string; timezone: string; fillModel: string }> {
  if (source.type === "ticker") {
    const history = await (options.stockHistory || fetchStockHistoryData)({ source: source.provider, symbol: source.symbol,
      timeframe: source.frequency, adjustment: "all", session: "regular", limit: MAX_BARS, ...(source.end ? { end: source.end } : {}) });
    const bars = normalizedBars(history.rows.map(row => ({ timestamp: row.timestamp, target: Number(row.close), open: Number(row.open), close: Number(row.close) })));
    if (bars.some(row => row.target <= 0 || row.open <= 0 || row.close <= 0)) invalid("Stock bars must have positive adjusted prices.");
    return { bars,
      provider: history.provider, timezone: history.exchangeTimezone || "UTC", fillModel: "next_observed_bar_open" };
  }
  const history = await (options.marketHistory || predictionForecastHistory)(source.provider, source.symbol, source.contract_id, source.frequency,
    { allowResolved: true, limit: MAX_BARS, minimumRows: 2, ...(source.end ? { until: Date.parse(source.end) } : {}),
      selection: historySelection({ history_phase: source.history_phase }) });
  const bars = normalizedBars(history.rows.map(row => ({ timestamp: row.timestamp, target: Number(row.target), open: Number(row.target), close: Number(row.target) })));
  if (bars.some(row => row.target < 0 || row.target > 1)) invalid("Contract display probabilities must be between zero and one.");
  return { bars,
    provider: source.provider, timezone: "UTC", fillModel: "next_observed_display_quote_proxy_not_executable" };
}
async function dispatchBacktestJob(id: string): Promise<void> {
  const mode = process.env.QUANTURA_ENSEMBLE_WORKER_MODE || "github_actions";
  if (mode === "manual" && /^(1|true)$/i.test(process.env.QUANTURA_ENSEMBLE_ALLOW_MANUAL_CLAIM || "")) return;
  if (mode !== "github_actions") throw new Error("BACKTEST_WORKER_UNAVAILABLE");
  const token = process.env.QUANTURA_ENSEMBLE_GITHUB_TOKEN || "";
  const owner = process.env.QUANTURA_ENSEMBLE_GITHUB_OWNER || "tamzid2001";
  const repository = process.env.QUANTURA_ENSEMBLE_GITHUB_REPO || "stockssagemakerdata";
  if (!token || !/^[A-Za-z0-9_.-]+$/.test(owner) || !/^[A-Za-z0-9_.-]+$/.test(repository)) throw new Error("BACKTEST_WORKER_UNAVAILABLE");
  const response = await fetch(`https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repository)}/actions/workflows/quantile-backtest.yml/dispatches`, {
    method: "POST", headers: { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json", "Content-Type": "application/json", "User-Agent": "Quantura-Backtest-Dispatch" },
    body: JSON.stringify({ ref: "main", inputs: { backtest_job_id: id, environment: process.env.NODE_ENV === "production" ? "production" : "development" } }),
    signal: AbortSignal.timeout(20_000),
  });
  if (!response.ok) throw new Error("BACKTEST_WORKER_UNAVAILABLE");
}
function workerAuthorized(req: Request): boolean {
  const configured = Buffer.from(String(process.env.QUANTURA_ENSEMBLE_WORKER_TOKEN || ""));
  const supplied = Buffer.from((String(req.headers.authorization || "").match(/^Bearer\s+(.+)$/i)?.[1] || "").trim());
  return configured.length >= 32 && configured.length === supplied.length && crypto.timingSafeEqual(configured, supplied);
}
function worker(options: Options, handler: (req: Request, res: Response, requestId: string) => Promise<void>) {
  return async (req: Request, res: Response) => {
    const requestId = crypto.randomUUID();
    res.setHeader("Cache-Control", "no-store");
    if (!workerAuthorized(req)) { res.status(401).json({ error: { code: "WORKER_AUTH_INVALID", request_id: requestId } }); return; }
    try { await handler(req, res, requestId); }
    catch (error) {
      const message = String((error as Error)?.message || "");
      res.status(/NOT_FOUND/.test(message) ? 404 : /CONFLICT/.test(message) ? 409 : 422).json({ error: { code: /^BACKTEST_[A-Z_]+$/.test(message) ? message : "BACKTEST_WORKER_REQUEST_INVALID", request_id: requestId } });
    }
  };
}
async function readInput(ref: FirebaseFirestore.DocumentReference): Promise<Bar[]> {
  const snapshots = await ref.collection(CHUNKS).orderBy("__name__").limit(20).get();
  return snapshots.docs.flatMap(doc => Array.isArray(doc.data().rows) ? doc.data().rows as Bar[] : []);
}
function validateWorkerResult(body: Record<string, unknown>, job: Record<string, any>): void {
  const fields = new Set(["metrics", "trades", "equity_curve", "decision_events", "assumptions", "fill_model", "forecast_windows",
    "data_hash", "strategy_schema_version", "live_eligible", "result_hash"]);
  if (Object.keys(body).some(key => !fields.has(key)) || body.fill_model !== job.fill_model ||
      typeof body.result_hash !== "string" || !/^[a-f0-9]{64}$/.test(body.result_hash)) throw new Error("BACKTEST_RESULT_INVALID");
  const metrics = object(body.metrics);
  const trades = body.trades, windows = body.forecast_windows, equity = body.equity_curve;
  if (body.data_hash !== job.data_hash || body.strategy_schema_version !== 2 || body.live_eligible !== false ||
      !Array.isArray(windows) || windows.length !== job.replay.evaluation_windows ||
      !Array.isArray(trades) || trades.length > MAX_WINDOWS_STEPS || !Array.isArray(equity) || equity.length > MAX_BARS ||
      !Number.isFinite(Number(metrics.net_pnl)) || !Number.isFinite(Number(metrics.max_drawdown)) ||
      !Number.isFinite(Number(metrics.trades)) || Number(metrics.trades) !== trades.length) throw new Error("BACKTEST_RESULT_INVALID");
  const serialized = JSON.stringify(body);
  if (serialized.length > 850_000 || JSON.parse(serialized).result_hash !== body.result_hash) throw new Error("BACKTEST_RESULT_INVALID");
  for (const window of windows) {
    const row = object(window);
    if (!Array.isArray(row.predictions) || row.predictions.length !== job.request.prediction_length) throw new Error("BACKTEST_RESULT_INVALID");
    if (!Number.isInteger(row.cutoff_index) || row.training_rows !== job.replay.context_rows ||
        typeof row.cutoff_at !== "string" || !Number.isFinite(Date.parse(row.cutoff_at))) throw new Error("BACKTEST_RESULT_INVALID");
    for (const prediction of row.predictions) {
      const forecastAt = Date.parse(String(object(prediction).timestamp || ""));
      if (!Number.isFinite(forecastAt) || forecastAt <= Date.parse(row.cutoff_at)) throw new Error("BACKTEST_RESULT_INVALID");
      const quantiles = object(object(prediction).quantiles);
      let prior = Number.NEGATIVE_INFINITY;
      for (const level of job.request.quantiles) {
        const raw = quantiles[String(Number(Number(level).toPrecision(12)))];
        const value = typeof raw === "number" ? raw : NaN;
        if (!Number.isFinite(value) || value < prior) throw new Error("BACKTEST_RESULT_INVALID");
        prior = value;
      }
    }
  }
}

/** New runs are asynchronous quantile-rule jobs. Legacy SMA records remain readable. */
export function registerBacktestRoutes(router: Router, options: Options): void {
  const wrap = (handler: Parameters<typeof withPlatformAccess>[1]) => withPlatformAccess(options, handler);
  router.get("/v1/backtests/strategy-schema", wrap(async (_req, res, principal, requestId) => {
    requireScope(principal, "backtests:read");
    send(res, requestId, { schema: QUANTILE_STRATEGY_SCHEMA, default_strategy: DEFAULT_QUANTILE_STRATEGY,
      default_replay: DEFAULT_QUANTILE_REPLAY, default_execution: DEFAULT_BACKTEST_EXECUTION,
      model_capabilities_url: "/api/v1/ensemble-forecasts/models", live_eligible: false });
  }));
  router.post("/v1/backtests", wrap(async (req, res, principal, requestId) => {
    try {
      const body = object(req.body);
      onlyKeys(body, ["workspace_id", "source", "forecast", "replay", "strategy", "execution"]);
      const workspaceId = typeof body.workspace_id === "string" && body.workspace_id ? body.workspace_id : principal.userId;
      const access = await resolveWorkspaceAccess(options.db, principal, workspaceId);
      authorizeWorkspaceAction(principal, access, "backtests:run", "write");
      requireWorkspacePermission(access, "forecast.create");
      const source = parseSource(body.source);
      const forecast = object(body.forecast);
      onlyKeys(forecast, ["prediction_length", "quantiles", "models", "toto_variant", "context_length", "failure_policy"]);
      const frequency = normalizedFrequency(source);
      const request = normalizeEnsembleConfiguration({ ...forecast, toto_variant: forecast.toto_variant ?? "4m",
        horizon_mode: source.type === "ticker" && frequency === "1D" ? "trading_sessions" : "frequency_periods",
        transform: source.type === "prediction_market" ? "logit" : "auto", frequency,
        calendar: source.type === "prediction_market" ? "NONE" : "NYSE" }, access.plan);
      if (request.prediction_length > 60) invalid("Backtests support 1–60 forecast steps per window.");
      const enabled = Object.values(request.models).filter(model => model.enabled && model.weight > 0).length;
      const replay = validateQuantileReplay(body.replay || DEFAULT_QUANTILE_REPLAY, enabled);
      if (replay.evaluation_windows * request.prediction_length > MAX_WINDOWS_STEPS) invalid("At most 240 forecasted steps may be backtested in one job.");
      validateModelHistory(request, replay.context_rows);
      const strategy = validateQuantileStrategy(body.strategy || DEFAULT_QUANTILE_STRATEGY, request.quantiles);
      const execution = validateBacktestExecution(body.execution || DEFAULT_BACKTEST_EXECUTION);
      const month = new Date().toISOString().slice(0, 7);
      const usageRef = options.db.collection("backtest_usage").doc(`${principal.userId}_${month}`);
      if (Number((await usageRef.get()).data()?.count || 0) >= PLAN_ENTITLEMENTS[access.plan].backtestsPerMonth) throw new Error("BACKTEST_MONTHLY_LIMIT");
      const active = await options.db.collection(COLLECTION).where("user_id", "==", principal.userId).limit(50).get();
      if (active.docs.some(doc => ["queued", "running"].includes(String(doc.data().status || "")) &&
          Date.parse(String(doc.data().created_at || "")) > Date.now() - 5 * 60 * 60_000)) throw new Error("BACKTEST_CONCURRENT_LIMIT");
      const { bars, provider, timezone, fillModel } = await loadBars(options, source);
      if (bars.length < replay.context_rows + replay.evaluation_windows * request.prediction_length) {
        invalid(`This source has ${bars.length} genuine bars; the selected context and windows require at least ${replay.context_rows + replay.evaluation_windows * request.prediction_length}. Reduce windows, context, or interval.`);
      }
      const createdAt = new Date().toISOString();
      const id = `bt_${crypto.randomUUID().replaceAll("-", "")}`;
      const ref = options.db.collection(COLLECTION).doc(id);
      const dataHash = crypto.createHash("sha256").update(JSON.stringify({ source, provider, bars })).digest("hex");
      const record = { id, schema_version: 2, status: "queued", progress: { completed_windows: 0, total_windows: replay.evaluation_windows, current_window: null },
        workspace_id: workspaceId, user_id: principal.userId, api_key_id: principal.tokenId || null, created_at: createdAt,
        source, provider, input_timezone: timezone, input_row_count: bars.length, data_hash: dataHash,
        request, replay, strategy, execution, fill_model: fillModel,
        model_checkpoints: approvedModelCheckpoints(request), model_revisions: approvedModelRevisions(request),
        runtime_mode: process.env.NODE_ENV === "production" ? "production" : "development", live_eligible: false };
      await options.db.runTransaction(async transaction => {
        const usage = await transaction.get(usageRef);
        const count = Number(usage.data()?.count || 0);
        if (count >= PLAN_ENTITLEMENTS[access.plan].backtestsPerMonth) throw new Error("BACKTEST_MONTHLY_LIMIT");
        transaction.set(usageRef, { count: count + 1, user_id: principal.userId, month, updated_at: createdAt }, { merge: true });
        transaction.create(ref, record);
      });
      try {
        for (let offset = 0; offset < bars.length; offset += CHUNK_SIZE) {
          await ref.collection(CHUNKS).doc(String(offset / CHUNK_SIZE).padStart(4, "0")).create({ rows: bars.slice(offset, offset + CHUNK_SIZE) });
        }
        await (options.dispatch || dispatchBacktestJob)(id);
        await ref.set({ dispatched_at: new Date().toISOString() }, { merge: true });
      } catch (error) {
        await ref.set({ status: "failed", error: { code: "BACKTEST_DISPATCH_FAILED", retryable: true }, completed_at: new Date().toISOString() }, { merge: true });
        throw error;
      }
      send(res, requestId, { backtest_id: id, status: "queued", created_at: createdAt,
        status_url: `/api/v1/backtests/${id}`, result_url: `/api/v1/backtests/${id}` }, 202);
    } catch (error) {
      if (/^(workspace_|insufficient_scope|plan_upgrade_required|[a-z]+_required_entitlement)/.test(String((error as Error)?.message || ""))) throw error;
      sendSafeError(res, requestId, error);
    }
  }));
  router.get("/v1/backtests", wrap(async (req, res, principal, requestId) => {
    const workspaceId = String(req.query.workspace_id || principal.userId);
    const access = await resolveWorkspaceAccess(options.db, principal, workspaceId);
    authorizeWorkspaceAction(principal, access, "backtests:read", "read");
    requireWorkspacePermission(access, "forecast.read");
    const snapshot = await options.db.collection(COLLECTION).where("workspace_id", "==", workspaceId).limit(150).get();
    const rows = snapshot.docs.map(doc => doc.data()).filter(row => {
      try { requireWorkspacePermission(access, "forecast.read", row.id); return true; } catch { return false; }
    }).sort((a, b) => String(b.created_at).localeCompare(String(a.created_at))).slice(0, 50)
      .map(row => ({ id: row.id, workspace_id: row.workspace_id, created_at: row.created_at, status: row.status || "completed",
        source: row.source, strategy: row.strategy, progress: row.progress || null, metrics: row.metrics || null, data_hash: row.data_hash, live_eligible: false }));
    send(res, requestId, rows);
  }));
  const read = wrap(async (req, res, principal, requestId) => {
    requireScope(principal, "backtests:read");
    const id = String(req.params.id || "");
    if (!/^bt_[a-f0-9]{32}$/.test(id)) { res.status(404).json({ error: { code: "BACKTEST_NOT_FOUND", request_id: requestId } }); return; }
    const snapshot = await options.db.collection(COLLECTION).doc(id).get();
    if (!snapshot.exists) { res.status(404).json({ error: { code: "BACKTEST_NOT_FOUND", request_id: requestId } }); return; }
    const record = snapshot.data()!;
    const access = await resolveWorkspaceAccess(options.db, principal, record.workspace_id);
    authorizeWorkspaceAction(principal, access, "backtests:read", "read");
    requireWorkspacePermission(access, "forecast.read", id);
    if (req.path.endsWith("/strategy")) {
      send(res, requestId, { schema_version: record.schema_version || 1, strategy: record.strategy, source: record.source,
        forecast: record.request || null, replay: record.replay || null, execution: record.execution, backtest_id: id,
        data_hash: record.data_hash, fill_model: record.fill_model, live_eligible: false,
        note: "Research configuration only. Live orders require a separately approved and verified execution workflow." });
      return;
    }
    if (record.schema_version !== 2) { send(res, requestId, publicRecord(record)); return; }
    const result = record.status === "completed" ? await options.db.collection(RESULTS).doc(id).get() : null;
    send(res, requestId, { ...publicRecord(record), ...(result?.exists ? result.data() : {}) });
  });
  router.get("/v1/backtests/:id/strategy", read);
  router.get("/v1/backtests/:id", read);

  router.post("/internal/backtests/:id/claim", worker(options, async (req, res, requestId) => {
    const id = String(req.params.id || "");
    if (!/^bt_[a-f0-9]{32}$/.test(id)) throw new Error("BACKTEST_NOT_FOUND");
    const ref = options.db.collection(COLLECTION).doc(id);
    const job = await options.db.runTransaction(async transaction => {
      const snapshot = await transaction.get(ref);
      if (!snapshot.exists) throw new Error("BACKTEST_NOT_FOUND");
      if (snapshot.data()?.status !== "queued") throw new Error("BACKTEST_CLAIM_CONFLICT");
      transaction.set(ref, { status: "running", started_at: new Date().toISOString(), worker_claim_id: requestId,
        lease_expires_at: new Date(Date.now() + 5 * 60 * 60_000).toISOString() }, { merge: true });
      return snapshot.data()!;
    });
    const rows = await readInput(ref);
    if (rows.length !== job.input_row_count || crypto.createHash("sha256").update(JSON.stringify({ source: job.source, provider: job.provider, bars: rows })).digest("hex") !== job.data_hash) {
      await ref.set({ status: "failed", error: { code: "BACKTEST_INPUT_INVALID", retryable: false }, completed_at: new Date().toISOString() }, { merge: true });
      throw new Error("BACKTEST_INPUT_INVALID");
    }
    send(res, requestId, { backtest_id: id, request: job.request, replay: job.replay, strategy: job.strategy, execution: job.execution,
      source: job.source, data_hash: job.data_hash, model_checkpoints: job.model_checkpoints, model_revisions: job.model_revisions,
      runtime_mode: job.runtime_mode, fill_model: job.fill_model, input: { rows, timezone: job.input_timezone } });
  }));
  router.post("/internal/backtests/:id/progress", worker(options, async (req, res, requestId) => {
    const ref = options.db.collection(COLLECTION).doc(String(req.params.id));
    const body = object(req.body);
    const total = Number(body.total_windows), completed = Number(body.completed_windows), current = body.current_window;
    await options.db.runTransaction(async transaction => {
      const snapshot = await transaction.get(ref);
      if (!snapshot.exists || snapshot.data()?.status !== "running") throw new Error("BACKTEST_CLAIM_CONFLICT");
      if (!Number.isInteger(total) || total !== snapshot.data()?.replay.evaluation_windows || !Number.isInteger(completed) || completed < 0 || completed > total ||
          !(current === null || (Number.isInteger(current) && Number(current) >= 1 && Number(current) <= total))) throw new Error("BACKTEST_PROGRESS_INVALID");
      transaction.set(ref, { progress: { completed_windows: completed, total_windows: total, current_window: current },
        lease_expires_at: new Date(Date.now() + 5 * 60 * 60_000).toISOString(), updated_at: new Date().toISOString() }, { merge: true });
    });
    send(res, requestId, { updated: true });
  }));
  router.post("/internal/backtests/:id/complete", worker(options, async (req, res, requestId) => {
    const id = String(req.params.id);
    const ref = options.db.collection(COLLECTION).doc(id), resultRef = options.db.collection(RESULTS).doc(id);
    const body = object(req.body);
    const alreadyCompleted = await options.db.runTransaction(async transaction => {
      const [snapshot, prior] = await Promise.all([transaction.get(ref), transaction.get(resultRef)]);
      if (snapshot.data()?.status === "completed" && prior.exists && prior.data()?.result_hash === body.result_hash) return true;
      if (!snapshot.exists || snapshot.data()?.status !== "running" || prior.exists) throw new Error("BACKTEST_CLAIM_CONFLICT");
      validateWorkerResult(body, snapshot.data()!);
      transaction.create(resultRef, { ...body, backtest_id: id, completed_at: new Date().toISOString() });
      transaction.set(ref, { status: "completed", progress: { completed_windows: snapshot.data()?.replay.evaluation_windows,
        total_windows: snapshot.data()?.replay.evaluation_windows, current_window: null }, completed_at: new Date().toISOString() }, { merge: true });
      return false;
    });
    send(res, requestId, { completed: true, already_completed: alreadyCompleted, backtest_id: id });
  }));
  router.post("/internal/backtests/:id/fail", worker(options, async (req, res, requestId) => {
    const ref = options.db.collection(COLLECTION).doc(String(req.params.id));
    const body = object(req.body);
    const code = typeof body.code === "string" && /^BACKTEST_[A-Z_]{1,70}$/.test(body.code) ? body.code : "BACKTEST_WORKER_FAILED";
    await options.db.runTransaction(async transaction => {
      const snapshot = await transaction.get(ref);
      if (!snapshot.exists) throw new Error("BACKTEST_NOT_FOUND");
      if (snapshot.data()?.status === "completed") throw new Error("BACKTEST_CLAIM_CONFLICT");
      if (!["queued", "running"].includes(String(snapshot.data()?.status))) return;
      if (body.only_if_queued && snapshot.data()?.status !== "queued") return;
      transaction.set(ref, { status: "failed", error: { code, retryable: body.retryable === true },
        completed_at: new Date().toISOString() }, { merge: true });
    });
    send(res, requestId, { failed: true, backtest_id: ref.id });
  }));
}
