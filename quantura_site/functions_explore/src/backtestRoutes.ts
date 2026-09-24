import crypto from "node:crypto";
import type { Router } from "express";
import type admin from "firebase-admin";
import { authorizeWorkspaceAction, requireScope, requireWorkspacePermission, resolveWorkspaceAccess } from "./apiAccess";
import { withPlatformAccess } from "./platformApiRoutes";
import { PLAN_ENTITLEMENTS } from "./planEntitlements";
import { fetchStockHistoryData } from "./marketDataRoutes";
import { predictionForecastHistory } from "./predictionMarketData";
import { historySelection } from "./eventHistory";
import { BACKTEST_STRATEGY_SCHEMA, DEFAULT_BACKTEST_EXECUTION, DEFAULT_BACKTEST_STRATEGY,
  runSmaBacktest, validateBacktestExecution, validateBacktestStrategy, type BacktestBar } from "./backtestEngine";

type Options = { db: FirebaseFirestore.Firestore; auth: admin.auth.Auth; publicOrigin: string;
  stockHistory?: typeof fetchStockHistoryData; marketHistory?: typeof predictionForecastHistory };
type Source = { type: "ticker"; symbol: string; provider: "auto" | "alpaca" | "yahoo"; frequency: "1Day" | "1Hour" | "1Min"; end?: string }
  | { type: "prediction_market"; provider: "kalshi" | "polymarket_us"; symbol: string; contract_id: string; frequency: "1min" | "1h" | "1D"; history_phase: "both" | "pregame" | "in_game"; end?: string };

function invalid(message: string): never { throw new Error(`BACKTEST_REQUEST_INVALID: ${message}`); }
function object(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : invalid("Expected a JSON object.");
}
function onlyKeys(row: Record<string, unknown>, keys: string[]): void {
  if (Object.keys(row).some(key => !keys.includes(key))) invalid("An unsupported field was supplied.");
}
function parseSource(value: unknown): Source {
  const row = object(value);
  const type = row.type;
  if (type === "ticker") {
    onlyKeys(row, ["type", "symbol", "provider", "frequency", "end"]);
    const symbol = String(row.symbol || "").trim().toUpperCase();
    const provider = row.provider || "auto";
    const frequency = row.frequency || "1Day";
    if (!/^[A-Z0-9.^=-]{1,20}$/.test(symbol) || !["auto", "alpaca", "yahoo"].includes(String(provider)) ||
        !["1Day", "1Hour", "1Min"].includes(String(frequency))) invalid("Choose a supported ticker, provider and interval.");
    return { type, symbol, provider: provider as "auto" | "alpaca" | "yahoo", frequency: frequency as "1Day" | "1Hour" | "1Min", ...parseEnd(row.end) };
  }
  if (type === "prediction_market") {
    onlyKeys(row, ["type", "provider", "symbol", "contract_id", "frequency", "history_phase", "end"]);
    const provider = row.provider;
    const symbol = String(row.symbol || "").trim();
    const contract_id = String(row.contract_id || "").trim();
    const frequency = row.frequency || "1min";
    const history_phase = row.history_phase || "both";
    if (!["kalshi", "polymarket_us"].includes(String(provider)) || !/^[\w:./-]{1,180}$/.test(symbol) ||
        !/^[\w:./-]{1,240}$/.test(contract_id) || !["1min", "1h", "1D"].includes(String(frequency)) ||
        !["both", "pregame", "in_game"].includes(String(history_phase))) invalid("Select one supported market contract and interval.");
    return { type, provider: provider as "kalshi" | "polymarket_us", symbol, contract_id,
      frequency: frequency as "1min" | "1h" | "1D", history_phase: history_phase as "both" | "pregame" | "in_game", ...parseEnd(row.end) };
  }
  return invalid("The backtest builder currently supports tickers and selected prediction-market contracts.");
}
function parseEnd(value: unknown): { end?: string } {
  if (value === undefined || value === null || value === "") return {};
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T/.test(value)) invalid("End time must be an ISO-8601 timestamp with timezone.");
  const time = Date.parse(value);
  if (!Number.isFinite(time) || time > Date.now()) invalid("End time must be a valid past timestamp.");
  return { end: new Date(time).toISOString() };
}
function send(res: any, requestId: string, data: unknown, status = 200): void {
  res.setHeader("X-Request-ID", requestId);
  res.setHeader("Cache-Control", "private, no-store");
  res.status(status).json({ data, meta: { api_version: "v1" } });
}
function publicRecord(record: Record<string, unknown>): Record<string, unknown> {
  const { user_id: _owner, api_key_id: _token, ...safe } = record;
  return safe;
}
function sendSafeError(res: any, requestId: string, error: unknown): void {
  const message = String((error as Error)?.message || "");
  const validation = /^(BACKTEST_[A-Z_]+): (.{1,240})$/.exec(message);
  const quota = message === "BACKTEST_MONTHLY_LIMIT";
  res.status(quota ? 429 : validation ? 422 : 503).json({ error: {
    code: quota ? message : validation?.[1] || "BACKTEST_DATA_UNAVAILABLE",
    message: quota ? "Monthly backtest allowance reached." : validation?.[2] || "Historical data is unavailable for this backtest. Try another interval or range.",
    request_id: requestId,
  } });
}

/** HTTP-safe simulation only. No trading client or order route is imported here. */
export function registerBacktestRoutes(router: Router, options: Options): void {
  const wrap = (handler: Parameters<typeof withPlatformAccess>[1]) => withPlatformAccess(options, handler);
  router.get("/v1/backtests/strategy-schema", wrap(async (_req, res, principal, requestId) => {
    requireScope(principal, "backtests:read");
    send(res, requestId, { schema: BACKTEST_STRATEGY_SCHEMA, default_strategy: DEFAULT_BACKTEST_STRATEGY,
      default_execution: DEFAULT_BACKTEST_EXECUTION, live_eligible: false });
  }));
  router.post("/v1/backtests", wrap(async (req, res, principal, requestId) => {
    try {
      const body = object(req.body);
      onlyKeys(body, ["workspace_id", "source", "strategy", "execution"]);
      const workspaceId = typeof body.workspace_id === "string" && body.workspace_id ? body.workspace_id : principal.userId;
      const access = await resolveWorkspaceAccess(options.db, principal, workspaceId);
      authorizeWorkspaceAction(principal, access, "backtests:run", "write");
      requireWorkspacePermission(access, "forecast.create");
      const source = parseSource(body.source);
      const strategy = validateBacktestStrategy(body.strategy || DEFAULT_BACKTEST_STRATEGY);
      const execution = validateBacktestExecution(body.execution || DEFAULT_BACKTEST_EXECUTION);
      const month = new Date().toISOString().slice(0, 7);
      const usageRef = options.db.collection("backtest_usage").doc(`${principal.userId}_${month}`);
      if (Number((await usageRef.get()).data()?.count || 0) >= PLAN_ENTITLEMENTS[access.plan].backtestsPerMonth) throw new Error("BACKTEST_MONTHLY_LIMIT");

      let bars: BacktestBar[];
      let provider: string;
      let fillModel: string;
      if (source.type === "ticker") {
        const history = await (options.stockHistory || fetchStockHistoryData)({ source: source.provider, symbol: source.symbol, timeframe: source.frequency,
          adjustment: "all", session: "regular", limit: 500, ...(source.end ? { end: source.end } : {}) });
        bars = history.rows.map(bar => ({ timestamp: bar.timestamp, open: bar.open, close: bar.close }));
        provider = history.provider;
        fillModel = "next_observed_bar_open";
      } else {
        const history = await (options.marketHistory || predictionForecastHistory)(source.provider, source.symbol, source.contract_id, source.frequency,
          { allowResolved: true, limit: 500, minimumRows: strategy.slow_period + 2,
            ...(source.end ? { until: Date.parse(source.end) } : {}), selection: historySelection({ history_phase: source.history_phase }) });
        bars = history.rows.map(row => ({ timestamp: row.timestamp, open: Number(row.target), close: Number(row.target) }));
        provider = source.provider;
        fillModel = "next_observed_display_quote_proxy_not_executable";
      }
      const result = runSmaBacktest(bars, strategy, execution);
      const createdAt = new Date().toISOString();
      const id = `bt_${crypto.randomUUID().replaceAll("-", "")}`;
      const dataHash = crypto.createHash("sha256").update(JSON.stringify(bars)).digest("hex");
      const record = { id, workspace_id: workspaceId, user_id: principal.userId, api_key_id: principal.tokenId,
        created_at: createdAt, source, provider, data_hash: dataHash, strategy, execution, fill_model: fillModel,
        live_eligible: false, ...result };
      await options.db.runTransaction(async transaction => {
        const usage = await transaction.get(usageRef);
        const count = Number(usage.data()?.count || 0);
        if (count >= PLAN_ENTITLEMENTS[access.plan].backtestsPerMonth) throw new Error("BACKTEST_MONTHLY_LIMIT");
        transaction.set(usageRef, { count: count + 1, user_id: principal.userId, month, updated_at: createdAt }, { merge: true });
        transaction.create(options.db.collection("forecast_backtests").doc(id), record);
      });
      send(res, requestId, publicRecord(record), 201);
    } catch (error) {
      if (/^(workspace_|insufficient_scope|plan_upgrade_required)/.test(String((error as Error)?.message || ""))) throw error;
      sendSafeError(res, requestId, error);
    }
  }));
  router.get("/v1/backtests", wrap(async (req, res, principal, requestId) => {
    const workspaceId = String(req.query.workspace_id || principal.userId);
    const access = await resolveWorkspaceAccess(options.db, principal, workspaceId);
    authorizeWorkspaceAction(principal, access, "backtests:read", "read");
    requireWorkspacePermission(access, "forecast.read");
    const snapshot = await options.db.collection("forecast_backtests").where("workspace_id", "==", workspaceId).limit(150).get();
    const rows = snapshot.docs.map(doc => doc.data()).filter(row => {
      try { requireWorkspacePermission(access, "forecast.read", row.id); return true; } catch { return false; }
    }).sort((a, b) => String(b.created_at).localeCompare(String(a.created_at))).slice(0, 50)
      .map(row => ({ id: row.id, workspace_id: row.workspace_id, created_at: row.created_at, source: row.source,
        strategy: row.strategy, metrics: row.metrics, data_hash: row.data_hash, live_eligible: false }));
    send(res, requestId, rows);
  }));
  const read = wrap(async (req, res, principal, requestId) => {
    requireScope(principal, "backtests:read");
    const id = String(req.params.id || "");
    if (!/^bt_[a-f0-9]{32}$/.test(id)) { res.status(404).json({ error: { code: "BACKTEST_NOT_FOUND", message: "Backtest not found.", request_id: requestId } }); return; }
    const snapshot = await options.db.collection("forecast_backtests").doc(id).get();
    if (!snapshot.exists) { res.status(404).json({ error: { code: "BACKTEST_NOT_FOUND", message: "Backtest not found.", request_id: requestId } }); return; }
    const record = snapshot.data()!;
    const access = await resolveWorkspaceAccess(options.db, principal, record.workspace_id);
    authorizeWorkspaceAction(principal, access, "backtests:read", "read");
    requireWorkspacePermission(access, "forecast.read", id);
    if (req.path.endsWith("/strategy")) {
      send(res, requestId, { schema_version: 1, strategy: record.strategy, source: record.source,
        execution: record.execution, backtest_id: id, data_hash: record.data_hash, fill_model: record.fill_model,
        live_eligible: false, note: "Export is a research configuration. Live orders require a separate approved execution workflow." });
    } else send(res, requestId, publicRecord(record));
  });
  router.get("/v1/backtests/:id/strategy", read);
  router.get("/v1/backtests/:id", read);
}
