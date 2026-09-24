"""Bounded walk-forward research backtests of the final ensemble quantiles.

This module never calls an exchange execution client. A decision sees a
forecast made at an earlier cutoff and an observed completed close; a
simulated fill occurs at the *next* observed bar. Event-contract quotes are
display-price proxies, not proven executable bids or asks.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any, Callable, Mapping


def _timestamp(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("BACKTEST_TIMESTAMP_TIMEZONE_REQUIRED")
    return parsed.astimezone(timezone.utc)


def _slot(value: Any, frequency: str) -> str:
    parsed = _timestamp(value)
    if frequency == "1D":
        return parsed.date().isoformat()
    seconds = 60 if frequency == "1min" else 3600 if frequency == "1h" else 0
    if not seconds:
        raise ValueError("BACKTEST_FREQUENCY_UNSUPPORTED")
    return str(int(parsed.timestamp()) // seconds)


def _qkey(value: float) -> str:
    return format(float(value), ".12g")


def _finite(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("BACKTEST_NONFINITE_VALUE")
    return number


def _validate(job: Mapping[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]:
    request = dict(job.get("request") or {})
    replay = dict(job.get("replay") or {})
    strategy = dict(job.get("strategy") or {})
    execution = dict(job.get("execution") or {})
    rows = [dict(row) for row in (job.get("input") or {}).get("rows") or []]
    if strategy.get("schema_version") != 2 or strategy.get("type") != "quantile_rules":
        raise ValueError("BACKTEST_STRATEGY_VERSION_INVALID")
    if strategy.get("entry_logic") not in {"all", "any"} or not isinstance(strategy.get("rules"), list):
        raise ValueError("BACKTEST_STRATEGY_INVALID")
    kinds = [rule.get("kind") for rule in strategy["rules"] if isinstance(rule, dict)]
    if not (1 <= kinds.count("entry") <= 4 and any(kind in {"take_profit", "stop_loss", "trailing_stop"} for kind in kinds)):
        raise ValueError("BACKTEST_STRATEGY_INVALID")
    if len(rows) > 2000 or not rows:
        raise ValueError("BACKTEST_HISTORY_INVALID")
    last = None
    for row in rows:
        time = _timestamp(row["timestamp"])
        if last is not None and time <= last:
            raise ValueError("BACKTEST_HISTORY_ORDER_INVALID")
        last = time
        for field in ("target", "open", "close"):
            if field in row:
                _finite(row[field])
    context = replay.get("context_rows")
    windows = replay.get("evaluation_windows")
    horizon = request.get("prediction_length")
    if type(context) is not int or not 2 <= context <= 500 or type(windows) is not int or not 1 <= windows <= 8 or type(horizon) is not int or not 1 <= horizon <= 120:
        raise ValueError("BACKTEST_REPLAY_INVALID")
    if len(rows) < context + windows * horizon:
        raise ValueError("BACKTEST_HISTORY_INSUFFICIENT")
    return rows, request, replay, strategy


def _entry_matches(rule: Mapping[str, Any], current: float, previous: float | None,
                   quantiles: Mapping[str, Any], previous_quantiles: Mapping[str, Any] | None) -> bool:
    key = _qkey(float(rule["quantile"]))
    threshold = _finite(quantiles[key])
    condition = rule["condition"]
    if condition == "at_or_above":
        return current >= threshold
    if condition == "at_or_below":
        return current <= threshold
    if previous is None or previous_quantiles is None:
        return False
    prior_threshold = _finite(previous_quantiles[key])
    if condition == "crosses_above":
        return previous < prior_threshold and current >= threshold
    if condition == "crosses_below":
        return previous > prior_threshold and current <= threshold
    raise ValueError("BACKTEST_ENTRY_RULE_INVALID")


def _exit_matches(rule: Mapping[str, Any], close: float, quantiles: Mapping[str, Any],
                  entry_price: float, peak_close: float) -> bool:
    kind = rule["kind"]
    if kind == "trailing_stop":
        return close <= peak_close * (1 - _finite(rule["percent"]) / 100)
    if rule.get("target_mode") == "quantile":
        threshold = _finite(quantiles[_qkey(float(rule["quantile"]))])
    else:
        percent = _finite(rule["percent"]) / 100
        threshold = entry_price * (1 + percent if kind == "take_profit" else 1 - percent)
    if kind == "take_profit":
        return close >= threshold
    if kind == "stop_loss":
        return close <= threshold
    raise ValueError("BACKTEST_EXIT_RULE_INVALID")


def simulate_quantile_rules(
    bars: list[dict[str, Any]], windows: list[dict[str, Any]], strategy: Mapping[str, Any],
    execution: Mapping[str, Any], *, frequency: str, fill_model: str,
) -> dict[str, Any]:
    """Observe closes, then simulate whole-unit fills on the next bar open."""
    starting = _finite(execution["starting_capital"])
    fraction = _finite(execution["position_fraction"])
    commission = _finite(execution["commission_bps"]) / 10000
    slippage = _finite(execution["slippage_bps"]) / 10000
    if not (starting >= 10 and 0 < fraction <= 1 and 0 <= commission <= .1 and 0 <= slippage <= .1):
        raise ValueError("BACKTEST_EXECUTION_INVALID")
    entries = [rule for rule in strategy["rules"] if rule["kind"] == "entry"]
    exits = [rule for rule in strategy["rules"] if rule["kind"] != "entry"]
    priority = {"stop_loss": 0, "trailing_stop": 1, "take_profit": 2}
    exits = sorted(exits, key=lambda rule: priority[rule["kind"]])
    by_index: dict[int, tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None, float | None]] = {}
    matched = 0
    for window in windows:
        cutoff = int(window["cutoff_index"])
        forecasts = { _slot(row["timestamp"], frequency): row for row in window["predictions"] }
        previous_quantiles: dict[str, Any] | None = None
        previous_close: float | None = _finite(bars[cutoff]["close"])
        previous_index = cutoff
        aligned: list[dict[str, Any]] = []
        for index in range(cutoff + 1, min(cutoff + 1 + len(window["predictions"]), len(bars))):
            actual = bars[index]
            prediction = forecasts.get(_slot(actual["timestamp"], frequency))
            if prediction is None:
                previous_quantiles, previous_close, previous_index = None, None, index
                continue
            quantiles = dict(prediction.get("quantiles") or {})
            if previous_index == cutoff and previous_quantiles is None:
                # At the first future row, compare cutoff close against the
                # first forecast level; no future quantile was known at cutoff.
                prior = quantiles
            else:
                prior = previous_quantiles if previous_index == index - 1 else None
            if index in by_index:
                raise ValueError("BACKTEST_WINDOWS_OVERLAP")
            by_index[index] = (window, quantiles, prior, previous_close)
            aligned.append({"timestamp": actual["timestamp"], "close": _finite(actual["close"]), "quantiles": quantiles})
            matched += 1
            previous_quantiles, previous_close, previous_index = quantiles, _finite(actual["close"]), index
        window["observed"] = aligned
        window["matched_bars"] = len(aligned)

    cash = starting
    shares = 0
    entry_cost = 0.0
    entry_price = 0.0
    entry_time = ""
    entry_rule_ids: list[str] = []
    peak_close = 0.0
    fees = 0.0
    pending: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    max_drawdown = max_drawdown_pct = 0.0
    peak_equity = starting
    min_cash = starting
    win_streak = loss_streak = longest_win = longest_loss = 0
    first_cutoff = int(windows[0]["cutoff_index"])

    def sell(bar: Mapping[str, Any], reason: str, rule_id: str, *, final_close: bool = False) -> None:
        nonlocal cash, shares, fees, entry_cost, entry_price, win_streak, loss_streak, longest_win, longest_loss
        if not shares:
            return
        raw_price = _finite(bar["close"] if final_close else bar["open"])
        price = raw_price * (1 - slippage)
        proceeds = shares * price
        fee = proceeds * commission
        pnl = proceeds - fee - entry_cost
        cash += proceeds - fee
        fees += fee
        trades.append({"entry_at": entry_time, "exit_at": bar["timestamp"], "entry_price": round(entry_price, 6),
                       "exit_price": round(price, 6), "shares": shares, "pnl": round(pnl, 6),
                       "return_pct": round(100 * pnl / entry_cost, 6), "entry_rule_ids": list(entry_rule_ids),
                       "exit_rule_id": rule_id, "reason": reason})
        if pnl > 0:
            win_streak += 1
            loss_streak = 0
            longest_win = max(longest_win, win_streak)
        elif pnl < 0:
            loss_streak += 1
            win_streak = 0
            longest_loss = max(longest_loss, loss_streak)
        shares = 0
        entry_cost = 0.0
        entry_price = 0.0

    for index in range(first_cutoff + 1, len(bars)):
        bar = bars[index]
        open_price = _finite(bar["open"])
        close = _finite(bar["close"])
        if pending is not None:
            if pending["kind"] == "exit":
                sell(bar, pending["reason"], pending["rule_id"])
                events.append({"at": bar["timestamp"], "event": "exit_filled", "rule_id": pending["rule_id"]})
            elif pending["kind"] == "entry" and shares == 0 and open_price > 0:
                price = open_price * (1 + slippage)
                count = math.floor(cash * fraction / (price * (1 + commission)))
                if count > 0:
                    cost = count * price
                    fee = cost * commission
                    shares = count
                    cash -= cost + fee
                    fees += fee
                    entry_cost = cost + fee
                    entry_price = price
                    entry_time = str(bar["timestamp"])
                    entry_rule_ids = list(pending["rule_ids"])
                    peak_close = close
                    events.append({"at": bar["timestamp"], "event": "entry_filled", "rule_ids": entry_rule_ids, "shares": count})
                else:
                    events.append({"at": bar["timestamp"], "event": "entry_unfilled", "reason": "insufficient_capital"})
            pending = None
        if shares:
            peak_close = max(peak_close, close)
        aligned = by_index.get(index)
        if aligned is not None:
            window, quantiles, previous_quantiles, previous_close = aligned
            if shares:
                selected = next((rule for rule in exits if _exit_matches(rule, close, quantiles, entry_price, peak_close)), None)
                if selected is not None and index + 1 < len(bars):
                    pending = {"kind": "exit", "reason": selected["kind"], "rule_id": selected["id"]}
                    events.append({"at": bar["timestamp"], "event": "exit_signal", "rule_id": selected["id"], "window": window["number"]})
            else:
                matches = [rule["id"] for rule in entries if _entry_matches(rule, close, previous_close, quantiles, previous_quantiles)]
                qualified = len(matches) == len(entries) if strategy["entry_logic"] == "all" else bool(matches)
                if qualified and index + 1 < len(bars):
                    pending = {"kind": "entry", "rule_ids": matches}
                    events.append({"at": bar["timestamp"], "event": "entry_signal", "rule_ids": matches, "window": window["number"]})
        if index == len(bars) - 1 and shares:
            sell(bar, "end_of_test", "end_of_test", final_close=True)
        equity = cash + shares * close
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)
        max_drawdown_pct = max(max_drawdown_pct, 100 * (peak_equity - equity) / peak_equity)
        min_cash = min(min_cash, cash)
        equity_curve.append({"timestamp": bar["timestamp"], "equity": round(equity, 6)})

    wins = sum(trade["pnl"] > 0 for trade in trades)
    losses = sum(trade["pnl"] < 0 for trade in trades)
    return {"metrics": {"observed_bars": len(bars), "matched_forecast_bars": matched,
                         "forecast_windows": len(windows), "trades": len(trades), "wins": wins, "losses": losses,
                         "win_rate_pct": round(100 * wins / len(trades), 6) if trades else None,
                         "starting_capital": starting, "ending_equity": round(cash, 6),
                         "net_pnl": round(cash - starting, 6), "return_pct": round(100 * (cash - starting) / starting, 6),
                         "max_drawdown": round(max_drawdown, 6), "max_drawdown_pct": round(max_drawdown_pct, 6),
                         "min_cash": round(min_cash, 6), "fees": round(fees, 6),
                         "longest_winning_streak": longest_win, "longest_losing_streak": longest_loss},
            "trades": trades, "equity_curve": equity_curve, "decision_events": events,
            "assumptions": ["Each window is inferred only from bars through its cutoff; no observed future row enters model input.",
                            "Signals use completed observed closes and the forecast quantile for that timestamp; fills use the next observed open.",
                            "Exit blocks are OR-combined; stop-loss precedes trailing stop, then take-profit on simultaneous close signals.",
                            "An open position is liquidated at the final observed close with configured costs.",
                            "Event-market display quotes are not executable bid/ask fills; results are hypothetical, not live performance."],
            "fill_model": fill_model}


def run_quantile_replay(job: Mapping[str, Any], *, progress: Callable[[Mapping[str, Any]], None] | None = None,
                        forecast_fn: Callable[..., dict[str, Any]] | None = None) -> dict[str, Any]:
    from .worker import execute_job

    rows, request, replay, strategy = _validate(job)
    progress = progress or (lambda _payload: None)
    # The API already validates each enabled model's actual minimum context.
    # The general-purpose worker default of 40 rows is a public-forecast
    # policy, not a model capability, and must not reject a valid short replay.
    forecast_fn = forecast_fn or (lambda candidate: execute_job(candidate, minimum_history_rows=2))
    horizon = int(request["prediction_length"])
    count = int(replay["evaluation_windows"])
    first_cutoff = len(rows) - count * horizon - 1
    windows: list[dict[str, Any]] = []
    for number in range(count):
        cutoff = first_cutoff + number * horizon
        history = rows[cutoff + 1 - int(replay["context_rows"]):cutoff + 1]
        progress({"status": "running", "completed_windows": number, "total_windows": count,
                  "current_window": number + 1, "cutoff_at": rows[cutoff]["timestamp"]})
        candidate = {"source": job.get("source"), "request": request, "input": {
            "rows": [{"timestamp": row["timestamp"], "target": row["target"]} for row in history],
            "timestamp_column": "timestamp", "target_column": "target", "frequency": request["frequency"],
            "timezone": (job.get("input") or {}).get("timezone") or "UTC"},
            "model_checkpoints": job.get("model_checkpoints"), "model_revisions": job.get("model_revisions"),
            "runtime_mode": job.get("runtime_mode") or "production", "dataset_hash": job.get("data_hash")}
        output = forecast_fn(candidate)
        if len(output["predictions"]) != horizon:
            raise ValueError("BACKTEST_FORECAST_HORIZON_MISMATCH")
        windows.append({"number": number + 1, "cutoff_index": cutoff, "cutoff_at": rows[cutoff]["timestamp"],
                        "training_rows": len(history), "predictions": output["predictions"],
                        "effective_weights_by_quantile": output.get("effective_weights_by_quantile"),
                        "participating_models": output.get("models"), "warnings": output.get("warnings") or []})
    result = simulate_quantile_rules(rows, windows, strategy, dict(job.get("execution") or {}),
                                     frequency=str(request["frequency"]), fill_model=str(job.get("fill_model") or "unknown"))
    result["forecast_windows"] = windows
    result["data_hash"] = job["data_hash"]
    result["strategy_schema_version"] = 2
    result["live_eligible"] = False
    result["result_hash"] = hashlib.sha256(json.dumps(result, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    progress({"status": "running", "completed_windows": count, "total_windows": count, "current_window": None})
    return result
