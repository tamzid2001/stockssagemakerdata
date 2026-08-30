from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd


Signal = Literal["LONG", "SHORT", "HOLD"]


@dataclass(frozen=True)
class BacktestConfig:
    starting_capital: float = 100_000.0
    position_size: float = 1.0
    commission_bps: float = 1.0
    spread_bps: float = 2.0
    slippage_bps: float = 2.0
    short_borrow_bps_annual: float = 100.0
    long_enabled: bool = True
    short_enabled: bool = True
    execution_rule: str = "next_bar_open"


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def run_trade_backtest(frame: pd.DataFrame, signals: pd.Series, *, config: BacktestConfig = BacktestConfig()) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    if config.execution_rule != "next_bar_open":
        raise ValueError("only next_bar_open execution is supported")
    data = frame.copy().reset_index(drop=True)
    required = {"timestamp", "open", "close"}
    if not required.issubset(data.columns):
        raise ValueError("timestamp, open, and close columns are required")
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, errors="coerce")
    signal_values = signals.reindex(data.index).fillna("HOLD").astype(str).str.upper()
    capital = float(config.starting_capital)
    equity_rows = [{"timestamp": data["timestamp"].iloc[0], "equity": capital, "exposure": 0.0}]
    trades: list[dict] = []
    total_costs = 0.0
    for signal_index in range(len(data) - 1):
        signal = signal_values.iloc[signal_index]
        direction = 1 if signal == "LONG" and config.long_enabled else -1 if signal == "SHORT" and config.short_enabled else 0
        if direction == 0:
            equity_rows.append({"timestamp": data["timestamp"].iloc[signal_index + 1], "equity": capital, "exposure": 0.0})
            continue
        entry_index = signal_index + 1
        entry = float(data["open"].iloc[entry_index])
        exit_price = float(data["close"].iloc[entry_index])
        notional = capital * max(0.0, min(1.0, config.position_size))
        round_trip_bps = 2 * config.commission_bps + config.spread_bps + 2 * config.slippage_bps
        costs = notional * round_trip_bps / 10_000.0
        if direction < 0:
            costs += notional * config.short_borrow_bps_annual / 10_000.0 / 252.0
        gross = direction * notional * ((exit_price / entry) - 1.0)
        net = gross - costs
        capital += net
        total_costs += costs
        trades.append(
            {
                "signal_timestamp": data["timestamp"].iloc[signal_index],
                "entry_timestamp": data["timestamp"].iloc[entry_index],
                "signal": signal,
                "entry_price": entry,
                "exit_price": exit_price,
                "gross_pnl": gross,
                "costs": costs,
                "net_pnl": net,
            }
        )
        equity_rows.append({"timestamp": data["timestamp"].iloc[entry_index], "equity": capital, "exposure": abs(notional) / max(capital, 1e-9)})
    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_rows)
    returns = equity_df["equity"].pct_change(fill_method=None).dropna()
    running_max = equity_df["equity"].cummax()
    drawdown = equity_df["equity"] / running_max - 1.0
    wins = trades_df[trades_df["net_pnl"] > 0] if not trades_df.empty else trades_df
    losses = trades_df[trades_df["net_pnl"] < 0] if not trades_df.empty else trades_df
    gross_profit = float(wins["net_pnl"].sum()) if not wins.empty else 0.0
    gross_loss = abs(float(losses["net_pnl"].sum())) if not losses.empty else 0.0
    downside = returns[returns < 0]
    metrics = {
        "starting_capital": config.starting_capital,
        "ending_equity": capital,
        "total_return": capital / config.starting_capital - 1.0,
        "trades": int(len(trades_df)),
        "wins": int(len(wins)),
        "losses": int(len(losses)),
        "win_rate": _safe_ratio(len(wins), len(trades_df)),
        "average_win": float(wins["net_pnl"].mean()) if not wins.empty else None,
        "average_loss": float(losses["net_pnl"].mean()) if not losses.empty else None,
        "expectancy": float(trades_df["net_pnl"].mean()) if not trades_df.empty else 0.0,
        "profit_factor": _safe_ratio(gross_profit, gross_loss),
        "maximum_drawdown": float(drawdown.min()),
        "sharpe": float(np.sqrt(252) * returns.mean() / returns.std(ddof=0)) if len(returns) > 1 and returns.std(ddof=0) > 0 else None,
        "sortino": float(np.sqrt(252) * returns.mean() / downside.std(ddof=0)) if len(downside) > 1 and downside.std(ddof=0) > 0 else None,
        "exposure": float(equity_df["exposure"].mean()),
        "turnover": float(len(trades_df) * config.position_size),
        "costs": total_costs,
        "long_return": float(trades_df.loc[trades_df["signal"] == "LONG", "net_pnl"].sum()) if not trades_df.empty else 0.0,
        "short_return": float(trades_df.loc[trades_df["signal"] == "SHORT", "net_pnl"].sum()) if not trades_df.empty else 0.0,
        "execution_rule": config.execution_rule,
    }
    return trades_df, equity_df, metrics
