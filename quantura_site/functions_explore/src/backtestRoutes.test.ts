import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { registerBacktestRoutes } from "./backtestRoutes";

function harness() {
  const values = new Map<string, any>();
  values.set("users/owner", { plan: "free" });
  const ref = (key: string): any => ({ path: key, id: key.split("/").at(-1),
    get: async () => ({ exists: values.has(key), id: key.split("/").at(-1), data: () => values.get(key) }),
    set: async (value: any, options?: any) => values.set(key, options?.merge ? { ...values.get(key), ...value } : value),
    create: async (value: any) => { if (values.has(key)) throw new Error("already_exists"); values.set(key, value); },
    collection: (child: string) => collection(`${key}/${child}`),
  });
  const collection = (key: string): any => ({ doc: (id: string) => ref(`${key}/${id}`),
    where: (field: string, _op: string, equal: any) => ({ limit: (size: number) => ({
      get: async () => ({ docs: [...values].filter(([path, row]) => path.startsWith(`${key}/`) && path.slice(key.length + 1).indexOf("/") === -1 && row[field] === equal)
        .slice(0, size).map(([path, row]) => ({ id: path.split("/").at(-1), data: () => row })) }),
    }) }),
  });
  const db: any = { collection, runTransaction: async (fn: any) => fn({ get: (item: any) => item.get(),
    set: (item: any, value: any, options?: any) => item.set(value, options), create: (item: any, value: any) => item.create(value) }) };
  const auth: any = { verifyIdToken: async (token: string) => token === "valid-session" ? { uid: "owner", firebase: { sign_in_provider: "password" } } : null };
  const app = express();
  app.use(express.json());
  const router = express.Router();
  const now = Date.UTC(2026, 8, 1);
  const bars = Array.from({ length: 50 }, (_, index) => ({ timestamp: new Date(now + index * 86400000).toISOString(),
    open: 100 + index * .1, high: 101 + index * .1, low: 99 + index * .1, close: 100 + index * .1,
    volume: 100, tradeCount: null, vwap: null, session: "regular" as const }));
  registerBacktestRoutes(router, { db, auth, publicOrigin: "https://quantura.studio", stockHistory: async () => ({
    provider: "alpaca", sourceRequested: "auto", fallbackUsed: false, symbol: "SPY", timeframe: "1Day", feed: "iex", adjustment: "all", session: "regular", rows: bars,
  }) });
  app.use("/api", router);
  return { app, values };
}

test("backtest API enforces session/workspace, returns immutable result and exported strategy, and applies monthly quota", async () => {
  const { app, values } = harness();
  const server = app.listen(0, "127.0.0.1");
  await new Promise<void>(resolve => server.once("listening", resolve));
  const port = (server.address() as any).port;
  const base = `http://127.0.0.1:${port}/api/v1/backtests`;
  const body = { workspace_id: "owner", source: { type: "ticker", symbol: "SPY", provider: "auto", frequency: "1Day" },
    strategy: { schema_version: 1, type: "sma_crossover", fast_period: 5, slow_period: 10 },
    execution: { starting_capital: 1000, position_fraction: 1, commission_bps: 0, slippage_bps: 0 } };
  const headers = { Authorization: "Bearer valid-session", "Content-Type": "application/json" };
  try {
    assert.equal((await fetch(base, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })).status, 401);
    assert.equal((await fetch(base, { method: "POST", headers, body: JSON.stringify({ ...body, workspace_id: "other" }) })).status, 403);
    assert.equal((await fetch(base, { method: "POST", headers, body: JSON.stringify({ ...body, strategy: { ...body.strategy, script: "buy()" } }) })).status, 422);
    const created = await fetch(base, { method: "POST", headers, body: JSON.stringify(body) });
    assert.equal(created.status, 201);
    const result: any = (await created.json()).data;
    assert.match(result.id, /^bt_[a-f0-9]{32}$/);
    assert.equal(result.live_eligible, false);
    assert.equal(result.fill_model, "next_observed_bar_open");
    assert.equal(result.metrics.observed_bars, 50);
    assert.equal(result.user_id, undefined);
    assert.equal(values.get(`forecast_backtests/${result.id}`).data_hash, result.data_hash);
    assert.equal((await fetch(`${base}/${result.id}`, { headers })).status, 200);
    const exported: any = (await (await fetch(`${base}/${result.id}/strategy`, { headers })).json()).data;
    assert.equal(exported.strategy.type, "sma_crossover");
    assert.equal(exported.live_eligible, false);
    assert.equal((await (await fetch(base, { headers })).json()).data.length, 1);
    for (let i = 0; i < 4; i++) assert.equal((await fetch(base, { method: "POST", headers, body: JSON.stringify(body) })).status, 201);
    assert.equal((await fetch(base, { method: "POST", headers, body: JSON.stringify(body) })).status, 429);
  } finally { await new Promise<void>(resolve => server.close(() => resolve())); }
});
