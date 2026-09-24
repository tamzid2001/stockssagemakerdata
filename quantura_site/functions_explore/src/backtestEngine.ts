/** Deterministic, bounded research backtest. Signals use completed bars only. */
export type BacktestBar = { timestamp: string; open: number; close: number };
export type BacktestStrategy = { schema_version: 1; type: "sma_crossover"; fast_period: number; slow_period: number };
export type BacktestExecution = { starting_capital: number; position_fraction: number; commission_bps: number; slippage_bps: number };

export const DEFAULT_BACKTEST_STRATEGY: BacktestStrategy = { schema_version: 1, type: "sma_crossover", fast_period: 10, slow_period: 30 };
export const DEFAULT_BACKTEST_EXECUTION: BacktestExecution = { starting_capital: 1000, position_fraction: 1, commission_bps: 10, slippage_bps: 5 };

export const BACKTEST_STRATEGY_SCHEMA = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  title: "Quantura backtest strategy v1",
  type: "object",
  additionalProperties: false,
  required: ["schema_version", "type", "fast_period", "slow_period"],
  properties: {
    schema_version: { const: 1 },
    type: { const: "sma_crossover", description: "Long while the completed-bar fast SMA exceeds the slow SMA; execute at the next observed bar." },
    fast_period: { type: "integer", minimum: 2, maximum: 100 },
    slow_period: { type: "integer", minimum: 3, maximum: 200 },
  },
} as const;

export function validateBacktestStrategy(value: unknown): BacktestStrategy {
  const row = value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
  if (Object.keys(row).some(key => !["schema_version", "type", "fast_period", "slow_period"].includes(key)) ||
      row.schema_version !== 1 || row.type !== "sma_crossover" ||
      !Number.isInteger(row.fast_period) || !Number.isInteger(row.slow_period) ||
      Number(row.fast_period) < 2 || Number(row.fast_period) > 100 ||
      Number(row.slow_period) < 3 || Number(row.slow_period) > 200 ||
      Number(row.fast_period) >= Number(row.slow_period)) {
    throw new Error("BACKTEST_STRATEGY_INVALID: Choose integer fast/slow periods (2–100 / 3–200), with fast below slow.");
  }
  return row as BacktestStrategy;
}

export function validateBacktestExecution(value: unknown): BacktestExecution {
  const row = value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
  if (Object.keys(row).some(key => !["starting_capital", "position_fraction", "commission_bps", "slippage_bps"].includes(key))) {
    throw new Error("BACKTEST_EXECUTION_INVALID: Unknown execution setting.");
  }
  const result = { ...DEFAULT_BACKTEST_EXECUTION, ...row } as BacktestExecution;
  if (!Number.isFinite(result.starting_capital) || result.starting_capital < 10 || result.starting_capital > 1_000_000 ||
      !Number.isFinite(result.position_fraction) || result.position_fraction <= 0 || result.position_fraction > 1 ||
      !Number.isFinite(result.commission_bps) || result.commission_bps < 0 || result.commission_bps > 1000 ||
      !Number.isFinite(result.slippage_bps) || result.slippage_bps < 0 || result.slippage_bps > 1000) {
    throw new Error("BACKTEST_EXECUTION_INVALID: Capital must be $10–$1m, fraction (0,1], and costs 0–1000 bps.");
  }
  return result;
}

function rounded(value: number): number { return Number(value.toFixed(6)); }

export function runSmaBacktest(input: BacktestBar[], strategy: BacktestStrategy, execution: BacktestExecution) {
  const ordered = new Map<number, BacktestBar>();
  for (const bar of input) {
    const time = Date.parse(bar.timestamp);
    if (!Number.isFinite(time) || !Number.isFinite(bar.open) || !Number.isFinite(bar.close) || bar.open <= 0 || bar.close <= 0) {
      throw new Error("BACKTEST_DATA_INVALID: Bars need valid timestamps and positive finite open/close prices.");
    }
    ordered.set(time, { timestamp: new Date(time).toISOString(), open: bar.open, close: bar.close });
  }
  const bars = [...ordered.entries()].sort((a, b) => a[0] - b[0]).map(([, bar]) => bar);
  if (bars.length < strategy.slow_period + 2 || bars.length > 500) {
    throw new Error("BACKTEST_HISTORY_INVALID: Choose at least slow period + 2 and at most 500 unique observed bars.");
  }
  const closes = bars.map(bar => bar.close);
  const prefix = [0];
  for (const close of closes) prefix.push(prefix.at(-1)! + close);
  const sma = (end: number, length: number) => (prefix[end + 1] - prefix[end + 1 - length]) / length;
  const trades: Array<Record<string, number | string>> = [];
  const equity_curve: Array<{ timestamp: string; equity: number }> = [];
  let cash = execution.starting_capital;
  let shares = 0;
  let entryCost = 0;
  let entryPrice = 0;
  let entryTime = "";
  let totalFees = 0;
  let peak = cash;
  let maxDrawdown = 0;
  let maxDrawdownPct = 0;
  let minCash = cash;
  let winningStreak = 0;
  let losingStreak = 0;
  let longestWinningStreak = 0;
  let longestLosingStreak = 0;
  const buy = (bar: BacktestBar) => {
    const price = bar.open * (1 + execution.slippage_bps / 10000);
    const count = Math.floor(cash * execution.position_fraction / (price * (1 + execution.commission_bps / 10000)));
    if (count < 1) return;
    const cost = count * price;
    const fee = cost * execution.commission_bps / 10000;
    shares = count;
    cash -= cost + fee;
    entryCost = cost + fee;
    entryPrice = price;
    entryTime = bar.timestamp;
    totalFees += fee;
  };
  const sell = (bar: BacktestBar, reason: string, useClose = false) => {
    if (!shares) return;
    const price = (useClose ? bar.close : bar.open) * (1 - execution.slippage_bps / 10000);
    const proceeds = shares * price;
    const fee = proceeds * execution.commission_bps / 10000;
    const pnl = proceeds - fee - entryCost;
    cash += proceeds - fee;
    totalFees += fee;
    trades.push({ entry_at: entryTime, exit_at: bar.timestamp, entry_price: rounded(entryPrice), exit_price: rounded(price), shares,
      pnl: rounded(pnl), return_pct: rounded(100 * pnl / entryCost), reason });
    if (pnl > 0) { winningStreak++; losingStreak = 0; longestWinningStreak = Math.max(longestWinningStreak, winningStreak); }
    else if (pnl < 0) { losingStreak++; winningStreak = 0; longestLosingStreak = Math.max(longestLosingStreak, losingStreak); }
    shares = 0;
    entryCost = 0;
  };
  for (let i = 0; i < bars.length; i++) {
    // At bar i open, only the completed close of i-1 and earlier may determine a trade.
    const previous = i - 1;
    if (previous >= strategy.slow_period - 1) {
      const longSignal = sma(previous, strategy.fast_period) > sma(previous, strategy.slow_period);
      if (longSignal && !shares) buy(bars[i]);
      else if (!longSignal && shares) sell(bars[i], "sma_exit");
    }
    if (i === bars.length - 1 && shares) sell(bars[i], "end_of_data", true);
    const equity = cash + shares * bars[i].close;
    peak = Math.max(peak, equity);
    maxDrawdown = Math.max(maxDrawdown, peak - equity);
    maxDrawdownPct = Math.max(maxDrawdownPct, peak > 0 ? 100 * (peak - equity) / peak : 0);
    minCash = Math.min(minCash, cash);
    equity_curve.push({ timestamp: bars[i].timestamp, equity: rounded(equity) });
  }
  const wins = trades.filter(trade => Number(trade.pnl) > 0).length;
  const losses = trades.filter(trade => Number(trade.pnl) < 0).length;
  return {
    metrics: {
      observed_bars: bars.length, trades: trades.length, wins, losses,
      win_rate_pct: trades.length ? rounded(100 * wins / trades.length) : null,
      starting_capital: execution.starting_capital, ending_equity: rounded(cash),
      net_pnl: rounded(cash - execution.starting_capital),
      return_pct: rounded(100 * (cash - execution.starting_capital) / execution.starting_capital),
      max_drawdown: rounded(maxDrawdown), max_drawdown_pct: rounded(maxDrawdownPct),
      min_cash: rounded(minCash), fees: rounded(totalFees),
      longest_winning_streak: longestWinningStreak, longest_losing_streak: longestLosingStreak,
    },
    trades, equity_curve,
    assumptions: ["Signals use completed closes; entries and signal exits use the next observed bar open.",
      "The final open position is liquidated at the final observed close; fee and slippage assumptions apply.",
      "This is historical simulation, not a validated live-trading strategy or guaranteed executable fill."],
  };
}
