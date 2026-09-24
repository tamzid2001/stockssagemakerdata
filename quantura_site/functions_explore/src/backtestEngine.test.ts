import test from "node:test";
import assert from "node:assert/strict";
import { DEFAULT_BACKTEST_EXECUTION, runSmaBacktest, validateBacktestExecution, validateBacktestStrategy } from "./backtestEngine";

const bars = (closes: number[]) => closes.map((close, i) => ({ timestamp: new Date(Date.UTC(2026, 0, 1 + i)).toISOString(), open: close, close }));
const strategy = { schema_version: 1 as const, type: "sma_crossover" as const, fast_period: 2, slow_period: 3 };

test("strategy schema rejects unknown code, invalid windows and extra fields", () => {
  assert.deepEqual(validateBacktestStrategy(strategy), strategy);
  assert.throws(() => validateBacktestStrategy({ ...strategy, fast_period: 3 }), /BACKTEST_STRATEGY_INVALID/);
  assert.throws(() => validateBacktestStrategy({ ...strategy, script: "orders.buy()" }), /BACKTEST_STRATEGY_INVALID/);
  assert.throws(() => validateBacktestExecution({ ...DEFAULT_BACKTEST_EXECUTION, position_fraction: 2 }), /BACKTEST_EXECUTION_INVALID/);
});

test("signal at completed close executes at next observed open, never the same bar", () => {
  const result = runSmaBacktest(bars([10, 10, 12, 13, 11, 9]), strategy, { ...DEFAULT_BACKTEST_EXECUTION, commission_bps: 0, slippage_bps: 0 });
  assert.equal(result.trades.length, 1);
  assert.equal(result.trades[0].entry_at, bars([10, 10, 12, 13])[3].timestamp);
  assert.equal(result.trades[0].entry_price, 13);
  assert.equal(result.trades[0].exit_at, bars([10, 10, 12, 13, 11, 9])[5].timestamp);
  assert.equal(result.metrics.wins, 0);
  assert.equal(result.metrics.losses, 1);
});

test("final bar close liquidates an open position; no signal means no invented trades", () => {
  const rising = runSmaBacktest(bars([10, 10, 12, 13, 14]), strategy, { ...DEFAULT_BACKTEST_EXECUTION, commission_bps: 0, slippage_bps: 0 });
  assert.equal(rising.trades.at(-1)?.reason, "end_of_data");
  const flat = runSmaBacktest(bars([10, 10, 10, 10, 10]), strategy, DEFAULT_BACKTEST_EXECUTION);
  assert.equal(flat.metrics.trades, 0);
  assert.equal(flat.metrics.win_rate_pct, null);
  assert.equal(flat.metrics.net_pnl, 0);
});

test("costs reduce returns, drawdown and streaks are deterministic", () => {
  const series = bars([10, 10, 12, 13, 11, 9, 10, 12, 13, 14]);
  const free = runSmaBacktest(series, strategy, { ...DEFAULT_BACKTEST_EXECUTION, commission_bps: 0, slippage_bps: 0 });
  const costly = runSmaBacktest(series, strategy, DEFAULT_BACKTEST_EXECUTION);
  assert.ok(costly.metrics.net_pnl < free.metrics.net_pnl);
  assert.ok(costly.metrics.fees > 0);
  assert.ok(costly.metrics.max_drawdown >= 0);
  assert.ok(costly.metrics.longest_losing_streak >= 1);
  assert.deepEqual(costly, runSmaBacktest(series, strategy, DEFAULT_BACKTEST_EXECUTION));
});

test("invalid prices and insufficient genuine history are rejected", () => {
  assert.throws(() => runSmaBacktest(bars([1, 2, 3, 4]), strategy, DEFAULT_BACKTEST_EXECUTION), /BACKTEST_HISTORY_INVALID/);
  assert.throws(() => runSmaBacktest(bars([1, 2, 3, 4, 0]), strategy, DEFAULT_BACKTEST_EXECUTION), /BACKTEST_DATA_INVALID/);
});
