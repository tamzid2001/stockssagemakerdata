import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { registerBacktestRoutes } from "./backtestRoutes";

function harness() {
  const values = new Map<string, any>();
  const dispatched: string[] = [];
  values.set("users/owner", { plan: "free" });
  const listing = (key: string) => [...values].filter(([path]) => path.startsWith(`${key}/`) && path.slice(key.length + 1).indexOf("/") === -1)
    .sort(([a], [b]) => a.localeCompare(b)).map(([path]) => ({ id: path.split("/").at(-1), data: () => values.get(path) }));
  const ref = (key: string): any => ({ path: key, id: key.split("/").at(-1),
    get: async () => ({ exists: values.has(key), id: key.split("/").at(-1), data: () => values.get(key) }),
    set: async (value: any, options?: any) => values.set(key, options?.merge ? { ...values.get(key), ...value } : value),
    create: async (value: any) => { if (values.has(key)) throw new Error("already_exists"); values.set(key, value); },
    collection: (child: string) => collection(`${key}/${child}`),
  });
  const collection = (key: string): any => ({ doc: (id: string) => ref(`${key}/${id}`),
    where: (field: string, _op: string, equal: any) => ({ limit: (size: number) => ({
      get: async () => ({ docs: listing(key).filter(doc => doc.data()?.[field] === equal).slice(0, size) }),
    }) }),
    orderBy: () => ({ limit: (size: number) => ({ get: async () => ({ docs: listing(key).slice(0, size) }) }) }),
  });
  const db: any = { collection, runTransaction: async (fn: any) => fn({ get: (item: any) => item.get(),
    set: (item: any, value: any, options?: any) => item.set(value, options), create: (item: any, value: any) => item.create(value) }) };
  const auth: any = { verifyIdToken: async (token: string) => token === "valid-session" ? { uid: "owner", firebase: { sign_in_provider: "password" } } : null };
  const app = express();
  app.use(express.json({ limit: "2mb" }));
  const router = express.Router();
  const now = Date.UTC(2026, 7, 1);
  const bars = Array.from({ length: 80 }, (_, index) => ({ timestamp: new Date(now + index * 86400000).toISOString(),
    open: 100 + index * .1, high: 101 + index * .1, low: 99 + index * .1, close: 100 + index * .1,
    volume: 100, tradeCount: null, vwap: null, session: "regular" as const }));
  registerBacktestRoutes(router, { db, auth, publicOrigin: "https://quantura.studio", dispatch: async id => { dispatched.push(id); },
    stockHistory: async () => ({ provider: "alpaca", sourceRequested: "auto", fallbackUsed: false, symbol: "SPY", timeframe: "1Day",
      feed: "iex", adjustment: "all", session: "regular", rows: bars }) });
  app.use("/api", router);
  return { app, values, dispatched, bars };
}

const body = { workspace_id: "owner", source: { type: "ticker", symbol: "SPY", provider: "auto", frequency: "1Day" },
  forecast: { prediction_length: 3, quantiles: [.01, .1, .5], models: { prophet: { enabled: true, weight: 1 } }, failure_policy: "fail" },
  replay: { context_rows: 40, evaluation_windows: 2 },
  strategy: { schema_version: 2, type: "quantile_rules", entry_logic: "all",
    rules: [{ id: "entry", kind: "entry", condition: "crosses_above", quantile: .1 },
      { id: "profit", kind: "take_profit", target_mode: "quantile", quantile: .5 },
      { id: "stop", kind: "stop_loss", target_mode: "quantile", quantile: .01 }] },
  execution: { starting_capital: 1000, position_fraction: 1, commission_bps: 0, slippage_bps: 0 } };

test("quantile jobs validate, claim once, and publish only ensemble results", async () => {
  const { app, values, dispatched, bars } = harness();
  const previousToken = process.env.QUANTURA_ENSEMBLE_WORKER_TOKEN;
  process.env.QUANTURA_ENSEMBLE_WORKER_TOKEN = "test-only-worker-token-of-at-least-32-chars";
  const server = app.listen(0, "127.0.0.1");
  await new Promise<void>(resolve => server.once("listening", resolve));
  const root = `http://127.0.0.1:${(server.address() as any).port}/api`;
  const base = `${root}/v1/backtests`;
  const headers = { Authorization: "Bearer valid-session", "Content-Type": "application/json" };
  const workerHeaders = { Authorization: `Bearer ${process.env.QUANTURA_ENSEMBLE_WORKER_TOKEN}`, "Content-Type": "application/json" };
  try {
    assert.equal((await fetch(base, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })).status, 401);
    assert.equal((await fetch(base, { method: "POST", headers, body: JSON.stringify({ ...body, workspace_id: "other" }) })).status, 403);
    const invalid = { ...body, strategy: { ...body.strategy, rules: [{ ...body.strategy.rules[0], quantile: .3 }, ...body.strategy.rules.slice(1)] } };
    assert.equal((await fetch(base, { method: "POST", headers, body: JSON.stringify(invalid) })).status, 422);
    const created = await fetch(base, { method: "POST", headers, body: JSON.stringify(body) });
    assert.equal(created.status, 202);
    const accepted: any = (await created.json()).data;
    assert.match(accepted.backtest_id, /^bt_[a-f0-9]{32}$/);
    assert.equal(accepted.status, "queued");
    assert.deepEqual(dispatched, [accepted.backtest_id]);
    const stored = values.get(`forecast_backtests/${accepted.backtest_id}`);
    assert.equal(stored.input_row_count, 80);
    assert.equal(stored.strategy.type, "quantile_rules");
    assert.equal(stored.live_eligible, false);
    assert.equal((await fetch(`${base}/${accepted.backtest_id}`, { headers })).status, 200);
    const exported: any = (await (await fetch(`${base}/${accepted.backtest_id}/strategy`, { headers })).json()).data;
    assert.equal(exported.strategy.schema_version, 2);
    assert.equal(exported.live_eligible, false);
    assert.equal((await (await fetch(base, { headers })).json()).data.length, 1);

    const internal = `${root}/internal/backtests/${accepted.backtest_id}`;
    assert.equal((await fetch(`${internal}/claim`, { method: "POST" })).status, 401);
    const claim = await fetch(`${internal}/claim`, { method: "POST", headers: workerHeaders });
    assert.equal(claim.status, 200);
    const claimed: any = (await claim.json()).data;
    assert.equal(claimed.input.rows.length, 80);
    assert.equal(claimed.data_hash, stored.data_hash);
    assert.equal((await fetch(`${internal}/claim`, { method: "POST", headers: workerHeaders })).status, 409);
    const predicted = (first: number) => Array.from({ length: 3 }, (_, step) => ({ timestamp: bars[first + step].timestamp,
      quantiles: { "0.01": 90, "0.1": 100, "0.5": 110 } }));
    const result = { data_hash: stored.data_hash, strategy_schema_version: 2, live_eligible: false,
      fill_model: "next_observed_bar_open", result_hash: "a".repeat(64),
      metrics: { trades: 0, net_pnl: 0, max_drawdown: 0 }, trades: [], equity_curve: [], decision_events: [], assumptions: [],
      forecast_windows: [{ number: 1, cutoff_index: 73, cutoff_at: bars[73].timestamp, training_rows: 40, predictions: predicted(74) },
        { number: 2, cutoff_index: 76, cutoff_at: bars[76].timestamp, training_rows: 40, predictions: predicted(77) }] };
    assert.equal((await fetch(`${internal}/complete`, { method: "POST", headers: workerHeaders, body: JSON.stringify(result) })).status, 200);
    assert.equal((await fetch(`${internal}/complete`, { method: "POST", headers: workerHeaders, body: JSON.stringify(result) })).status, 200);
    const fetched: any = (await (await fetch(`${base}/${accepted.backtest_id}`, { headers })).json()).data;
    assert.equal(fetched.status, "completed");
    assert.equal(fetched.forecast_windows.length, 2);
    assert.equal(fetched.user_id, undefined);
    assert.equal(fetched.input, undefined);
    assert.equal(fetched.live_eligible, false);
  } finally {
    if (previousToken === undefined) delete process.env.QUANTURA_ENSEMBLE_WORKER_TOKEN;
    else process.env.QUANTURA_ENSEMBLE_WORKER_TOKEN = previousToken;
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});
