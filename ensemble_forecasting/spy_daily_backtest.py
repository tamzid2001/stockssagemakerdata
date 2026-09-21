"""Resumable SPY-constituent daily-close five-model backtest.

The production study freezes current S&P 500 constituents plus SPY and Alpaca
IEX split-adjusted daily closes.  Every rolling origin receives exactly 500
prior closes.  The immediately following close is withheld and classified as
a buy below first-step P10 or a sell above first-step P90.

Long-running inference is checkpointed in the repository's private encrypted
research bucket.  This module never places broker orders.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import statistics
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from itertools import pairwise
from pathlib import Path
from typing import Any, ClassVar

from market_research.engine import digest
from market_research.recovery_cloud import Campaign, decode_catalog, encode_catalog
from scripts.quant_screener_pipeline import fetch_alpaca_histories, request_with_retry
from scripts.weekly_screener import ENGINE as WEEKLY_ENSEMBLE_ENGINE
from scripts.weekly_screener import session_schedule, weekly_configuration

from .capabilities import model_supports_quantile
from .kalshi_perp_backtest import validate_five_model_result
from .schemas import APPROVED_MODELS
from .worker import execute_job

SCHEMA_VERSION = "spy_daily_rolling_500_v1"
ROLLING_CONTEXT = 500
BACKTEST_ORIGINS = 500
DEFAULT_SHARDS = 16
INITIAL_CAPITAL = 10_000.0
SPY_HOLDINGS_URL = "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
FIVE_MODEL_IDS = tuple(APPROVED_MODELS)
QUANTILES = (0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99)
QUANTILE_KEYS = tuple(str(value).rstrip("0").rstrip(".") for value in QUANTILES)
ForecastFn = Callable[[list[dict[str, Any]], Mapping[str, Any]], Mapping[str, Any]]


def iso_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def stock_backtest_configuration() -> dict[str, Any]:
    """Return the production weekly profile used at every rolling origin."""
    weekly = weekly_configuration()
    return {
        "prediction_length": 7,
        "horizon_mode": "trading_sessions",
        "frequency": "1D",
        "calendar": "NYSE",
        "quantiles": list(weekly["quantiles"]),
        "context_length": int(weekly["context_length"]),
        "transform": str(weekly["transform"]),
        "failure_policy": "fail",
        "models": dict(weekly["models"]),
        "toto_variant": str(weekly["toto_variant"]),
        "model_checkpoints": dict(weekly["model_checkpoints"]),
        "model_revisions": dict(weekly["model_revisions"]),
    }


def stock_configuration_hash() -> str:
    return hashlib.sha256(
        json.dumps(
            stock_backtest_configuration(), sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()


def validate_stock_result(
    result: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    """Require the full requested surface and the correct equal supported weights."""
    predictions, completed = validate_five_model_result(result)
    effective = result.get("effective_weights_by_quantile")
    if not isinstance(effective, Mapping):
        raise TypeError("stock forecast effective weights are missing")
    for quantile in QUANTILES:
        key = str(quantile).rstrip("0").rstrip(".")
        supported = tuple(
            model_id
            for model_id in FIVE_MODEL_IDS
            if model_supports_quantile(model_id, quantile)
        )
        weights = effective.get(key)
        if not isinstance(weights, Mapping) or set(weights) != set(supported):
            raise ValueError(
                f"stock forecast has invalid effective models at quantile {key}"
            )
        expected = 1 / len(supported)
        if any(
            not math.isclose(
                float(weights[model_id]), expected, rel_tol=0, abs_tol=1e-12
            )
            for model_id in supported
        ):
            raise ValueError(
                f"stock forecast is not equally weighted at quantile {key}"
            )
        for prediction in predictions:
            if key not in dict(prediction.get("quantiles") or {}):
                raise ValueError(f"stock forecast is missing quantile {key}")
    return predictions, completed


def stock_worker_forecaster(*, mock: bool = False) -> ForecastFn:
    config = stock_backtest_configuration()
    request = {
        key: value
        for key, value in config.items()
        if key not in {"model_checkpoints", "model_revisions", "toto_variant"}
    }

    def forecast(
        history: list[dict[str, Any]], security: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        if len(history) != ROLLING_CONTEXT:
            raise ValueError(
                "every stock backtest origin requires exactly 500 prior closes"
            )
        result = execute_job(
            {
                "source": {
                    "type": "stock",
                    "provider": "alpaca_iex",
                    "symbol": security["symbol"],
                    "adjustment": "split",
                },
                "request": request,
                "runtime_mode": "test" if mock else "production",
                "model_checkpoints": config["model_checkpoints"],
                "model_revisions": config["model_revisions"],
                "maximum_history_rows": ROLLING_CONTEXT,
                "input": {"rows": history, "frequency": "1D", "timezone": "UTC"},
            },
            mock=mock,
            minimum_history_rows=ROLLING_CONTEXT,
        )
        validate_stock_result(result)
        return result

    return forecast


def normalize_stock_history(
    rows: Sequence[Mapping[str, Any]], *, as_of: str, maximum_rows: int = 1100
) -> list[dict[str, Any]]:
    """Normalize positive, unique completed daily closes without filling gaps."""
    if maximum_rows < ROLLING_CONTEXT + 1:
        raise ValueError("history retention cannot support one rolling origin")
    by_day: dict[str, float] = {}
    for row in rows:
        stamp = str(row.get("timestamp") or "")
        day = stamp[:10]
        try:
            close = float(row.get("close"))
            parsed = date.fromisoformat(day)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(close) or close <= 0 or parsed > date.fromisoformat(as_of):
            continue
        prior = by_day.get(day)
        if prior is not None and prior != close:
            raise ValueError(f"conflicting Alpaca close for {day}")
        by_day[day] = close
    return [
        {"timestamp": f"{day}T00:00:00Z", "target": by_day[day]}
        for day in sorted(by_day)[-maximum_rows:]
    ]


def parse_spy_holdings_frame(frame: Any) -> list[dict[str, Any]]:
    """Select only USD exchange tickers from State Street's fund holdings table."""
    required = {"Name", "Ticker", "Weight", "Shares Held", "Local Currency"}
    if not required.issubset(frame.columns):
        raise ValueError("State Street SPY holdings columns are missing")
    output: dict[str, dict[str, Any]] = {}
    for raw in frame.to_dict(orient="records"):
        symbol = str(raw.get("Ticker") or "").strip().upper()
        name = str(raw.get("Name") or "").strip()
        currency = str(raw.get("Local Currency") or "").strip().upper()
        try:
            weight = float(raw.get("Weight"))
            shares = float(raw.get("Shares Held"))
        except (TypeError, ValueError):
            continue
        if (
            not re.fullmatch(r"[A-Z][A-Z0-9]{0,5}(?:\.[A-Z])?", symbol)
            or currency != "USD"
            or not name
            or not math.isfinite(weight)
            or weight <= 0
            or not math.isfinite(shares)
            or shares <= 0
        ):
            continue
        record = {
            "symbol": symbol,
            "company_name": name,
            "is_etf": False,
            "sector": None,
            "industry": None,
            "holding_weight_pct": weight,
            "shares_held": shares,
        }
        if symbol in output and output[symbol] != record:
            raise ValueError(f"conflicting State Street holding for {symbol}")
        output[symbol] = record
    return [output[symbol] for symbol in sorted(output)]


def fetch_spy_holdings() -> tuple[list[dict[str, Any]], str, str]:
    """Download and validate the official daily State Street SPY workbook."""
    import pandas as pd

    response = request_with_retry(
        "GET",
        SPY_HOLDINGS_URL,
        headers={
            "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "User-Agent": "Quantura-SPY-Backtest/1.0",
        },
        timeout=60,
    )
    content = bytes(response.content)
    if len(content) > 5 * 1024 * 1024 or not content.startswith(b"PK"):
        raise ValueError("invalid State Street SPY holdings workbook")
    raw = pd.read_excel(
        io.BytesIO(content), sheet_name="holdings", header=None, nrows=12
    )
    header_rows = [
        index
        for index, row in raw.iterrows()
        if str(row.iloc[0]).strip() == "Name" and str(row.iloc[1]).strip() == "Ticker"
    ]
    if len(header_rows) != 1:
        raise ValueError("State Street SPY holdings header is ambiguous")
    frame = pd.read_excel(
        io.BytesIO(content), sheet_name="holdings", header=int(header_rows[0])
    )
    holdings = parse_spy_holdings_frame(frame)
    if not 450 <= len(holdings) <= 550:
        raise ValueError(
            "State Street SPY equity holding count is outside the validated range"
        )
    holding_labels = [
        str(value) for value in raw.stack().tolist() if str(value).startswith("As of ")
    ]
    if len(holding_labels) != 1:
        raise ValueError("State Street SPY holdings as-of date is missing")
    parsed_as_of = (
        datetime.strptime(holding_labels[0].removeprefix("As of "), "%d-%b-%Y")
        .replace(tzinfo=timezone.utc)
        .date()
        .isoformat()
    )
    return holdings, parsed_as_of, hashlib.sha256(content).hexdigest()


def eligible_cutoffs(
    rows: Sequence[Mapping[str, Any]],
    *,
    context: int = ROLLING_CONTEXT,
    maximum_origins: int = BACKTEST_ORIGINS,
) -> list[int]:
    """Return newest-first cutoffs with an observed close on the next NYSE session."""
    if context < 2 or maximum_origins < 1 or len(rows) <= context:
        return []
    first = str(rows[0]["timestamp"])[:10]
    last = str(rows[-1]["timestamp"])[:10]
    sessions = [row["date"] for row in session_schedule(first, last)]
    next_session = dict(pairwise(sessions))
    output: list[int] = []
    for cutoff in range(len(rows) - 2, context - 2, -1):
        cutoff_day = str(rows[cutoff]["timestamp"])[:10]
        actual_day = str(rows[cutoff + 1]["timestamp"])[:10]
        if next_session.get(cutoff_day) == actual_day:
            output.append(cutoff)
            if len(output) == maximum_origins:
                break
    return output


def _q(row: Mapping[str, Any], quantile: str) -> float:
    value = float((row.get("quantiles") or {})[quantile])
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"invalid forecast quantile {quantile}")
    return value


def evaluate_origin(
    security: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    cutoff_index: int,
    forecast: ForecastFn,
) -> dict[str, Any]:
    if cutoff_index < ROLLING_CONTEXT - 1 or cutoff_index >= len(rows) - 1:
        raise ValueError("rolling cutoff is outside the eligible history")
    history = [
        dict(row) for row in rows[cutoff_index - ROLLING_CONTEXT + 1 : cutoff_index + 1]
    ]
    if len(history) != ROLLING_CONTEXT:
        raise ValueError("rolling cutoff does not contain 500 prior closes")
    actual = dict(rows[cutoff_index + 1])
    result = forecast(history, security)
    predictions, completed_models = validate_stock_result(result)
    if str(predictions[0]["timestamp"])[:10] != str(actual["timestamp"])[:10]:
        raise ValueError(
            "first forecast session does not match the withheld Alpaca close"
        )
    first = predictions[0]
    actual_price = float(actual["target"])
    p10, p90 = _q(first, "0.1"), _q(first, "0.9")
    signal = "buy" if actual_price < p10 else "sell" if actual_price > p90 else "none"
    record: dict[str, Any] = {
        "symbol": security["symbol"],
        "cutoff_index": cutoff_index,
        "cutoff_at": history[-1]["timestamp"],
        "context_first_at": history[0]["timestamp"],
        "context_rows": len(history),
        "signal_index": cutoff_index + 1,
        "signal_at": actual["timestamp"],
        "actual": actual_price,
        "signal": signal,
        "ensemble_models": ",".join(completed_models),
        "forecast_result_hash": result.get("result_hash"),
    }
    for key in QUANTILE_KEYS:
        label = f"p{round(float(key) * 100):02d}"
        record[label] = _q(first, key)
        record[f"avg_{label}"] = sum(_q(row, key) for row in predictions) / len(
            predictions
        )
    return record


def _drawdown(equity: Sequence[float]) -> float:
    peak = 0.0
    maximum = 0.0
    for value in equity:
        peak = max(peak, float(value))
        if peak > 0:
            maximum = max(maximum, (peak - float(value)) / peak)
    return maximum


def _mean(values: Sequence[float | int]) -> float | None:
    return statistics.fmean(values) if values else None


def _median(values: Sequence[float | int]) -> float | None:
    return statistics.median(values) if values else None


def build_symbol_report(
    security: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    origin_results: Sequence[Mapping[str, Any]],
    *,
    initial_capital: float = INITIAL_CAPITAL,
) -> dict[str, Any]:
    """Reconstruct chronological signals, targets, trades, turnover and drawdown."""
    if initial_capital <= 0:
        raise ValueError("initial capital must be positive")
    records = sorted(
        (dict(row) for row in origin_results), key=lambda row: int(row["signal_index"])
    )
    if len({int(row["signal_index"]) for row in records}) != len(records):
        raise ValueError("duplicate rolling origin")
    events = [row for row in records if row["signal"] != "none"]
    events_by_index = {int(row["signal_index"]): row for row in events}

    hit_rows: list[dict[str, Any]] = []
    for event_index, event in enumerate(events):
        opposite = "sell" if event["signal"] == "buy" else "buy"
        stop = min(len(rows) - 1, int(event["signal_index"]) + 7)
        opposite_event = next(
            (
                candidate
                for candidate in events[event_index + 1 :]
                if int(candidate["signal_index"]) <= stop
                and candidate["signal"] == opposite
            ),
            None,
        )
        if opposite_event:
            stop = int(opposite_event["signal_index"]) - 1
        future = rows[int(event["signal_index"]) + 1 : stop + 1]
        if event["signal"] == "buy":
            crossed_p50 = any(
                float(row["target"]) >= float(event["avg_p50"]) for row in future
            )
            crossed_outer = any(
                float(row["target"]) >= float(event["avg_p90"]) for row in future
            )
            outer_name = "avg_p90"
        else:
            crossed_p50 = any(
                float(row["target"]) <= float(event["avg_p50"]) for row in future
            )
            crossed_outer = any(
                float(row["target"]) <= float(event["avg_p10"]) for row in future
            )
            outer_name = "avg_p10"
        hit_rows.append(
            {
                "symbol": security["symbol"],
                "signal_at": event["signal_at"],
                "signal": event["signal"],
                "crossed_avg_p50": crossed_p50,
                "crossed_outer": crossed_outer,
                "outer_target": outer_name,
                "observed_closes": len(future),
                "opposite_signal_at": opposite_event["signal_at"]
                if opposite_event
                else None,
            }
        )

    capital = initial_capital
    position: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    gross_notional = 0.0
    first_evaluation_index = int(records[0]["signal_index"]) if records else len(rows)
    last_evaluation_index = int(records[-1]["signal_index"]) if records else -1
    for index, raw in enumerate(rows):
        if index < first_evaluation_index or index > last_evaluation_index:
            continue
        price = float(raw["target"])
        event = events_by_index.get(index)
        if position:
            direction = 1 if position["side"] == "buy" else -1
            marked = float(position["entry_equity"]) * (
                1 + direction * (price / float(position["entry_price"]) - 1)
            )
            opposite = event is not None and event["signal"] != position["side"]
            expired = index >= int(position["deadline"])
            if opposite or expired or index == last_evaluation_index:
                capital = marked
                gross_notional += max(capital, 0.0)
                trades.append(
                    {
                        "symbol": security["symbol"],
                        "side": position["side"],
                        "entry_at": position["entry_at"],
                        "entry_price": position["entry_price"],
                        "exit_at": raw["timestamp"],
                        "exit_price": price,
                        "exit_reason": "opposite_signal"
                        if opposite
                        else "seven_closes"
                        if expired
                        else "end_of_study",
                        "return_pct": (capital / float(position["entry_equity"]) - 1)
                        * 100,
                        "profit": capital - float(position["entry_equity"]),
                        "holding_closes": index - int(position["entry_index"]),
                    }
                )
                position = None
            else:
                capital = marked
        if (
            position is None
            and event is not None
            and index < last_evaluation_index
            and capital > 0
        ):
            gross_notional += capital
            position = {
                "side": event["signal"],
                "entry_index": index,
                "entry_at": raw["timestamp"],
                "entry_price": price,
                "entry_equity": capital,
                "deadline": index + 7,
            }
        equity_curve.append({"timestamp": raw["timestamp"], "equity": capital})

    def hit_summary(side: str) -> dict[str, Any]:
        subset = [row for row in hit_rows if row["signal"] == side]
        p50_hits = sum(bool(row["crossed_avg_p50"]) for row in subset)
        outer_hits = sum(bool(row["crossed_outer"]) for row in subset)
        return {
            "signals": len(subset),
            "avg_p50_hits": p50_hits,
            "avg_p50_hit_rate": p50_hits / len(subset) if subset else None,
            "outer_quantile_hits": outer_hits,
            "outer_quantile_hit_rate": outer_hits / len(subset) if subset else None,
        }

    signal_gaps = [
        int(right["signal_index"]) - int(left["signal_index"])
        for left, right in pairwise(events)
    ]
    opposite_gaps = [
        int(right["signal_index"]) - int(left["signal_index"])
        for left, right in pairwise(events)
        if left["signal"] != right["signal"]
    ]
    side_switches = sum(
        left["signal"] != right["signal"] for left, right in pairwise(events)
    )
    equity_values = [initial_capital] + [float(row["equity"]) for row in equity_curve]
    average_equity = _mean(equity_values) or initial_capital
    evaluation_sessions = len(equity_curve)
    turnover = {
        "signals_per_100_sessions": len(events) / evaluation_sessions * 100
        if evaluation_sessions
        else None,
        "side_switches": side_switches,
        "side_switch_rate": side_switches / (len(events) - 1)
        if len(events) > 1
        else None,
        "mean_sessions_between_signals": _mean(signal_gaps),
        "median_sessions_between_signals": _median(signal_gaps),
        "mean_sessions_between_opposite_signals": _mean(opposite_gaps),
        "median_sessions_between_opposite_signals": _median(opposite_gaps),
        "mean_holding_closes": _mean([int(row["holding_closes"]) for row in trades]),
        "median_holding_closes": _median(
            [int(row["holding_closes"]) for row in trades]
        ),
        "gross_notional_traded": gross_notional,
        "gross_turnover_ratio": gross_notional / average_equity
        if average_equity
        else None,
        "annualized_gross_turnover_ratio": (
            gross_notional / average_equity * 252 / evaluation_sessions
            if average_equity and evaluation_sessions
            else None
        ),
        "annualized_one_way_turnover_ratio": (
            gross_notional / average_equity / 2 * 252 / evaluation_sessions
            if average_equity and evaluation_sessions
            else None
        ),
    }
    return {
        "symbol": security["symbol"],
        "company_name": security.get("company_name"),
        "is_etf": bool(security.get("is_etf")),
        "observations": len(rows),
        "origins_available": len(records),
        "full_500_origin_coverage": len(records) == BACKTEST_ORIGINS,
        "first_evaluation_close": records[0]["signal_at"] if records else None,
        "last_evaluation_close": records[-1]["signal_at"] if records else None,
        "signal_counts": {
            side: sum(row["signal"] == side for row in records)
            for side in ("buy", "sell", "none")
        },
        "hit_rates": {"buy": hit_summary("buy"), "sell": hit_summary("sell")},
        "initial_capital": initial_capital,
        "final_equity": capital,
        "profit": capital - initial_capital,
        "return_pct": (capital / initial_capital - 1) * 100,
        "max_drawdown_pct": _drawdown(equity_values) * 100,
        "turnover": turnover,
        "trades": trades,
        "origin_results": records,
        "signal_hit_results": hit_rows,
        "equity_curve": equity_curve,
    }


def aggregate_reports(reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    eligible = [
        dict(report)
        for report in reports
        if int(report.get("origins_available") or 0) > 0
    ]
    if not eligible:
        return {
            "symbols": len(reports),
            "eligible_symbols": 0,
            "initial_capital": 0,
            "final_equity": 0,
            "profit": 0,
            "return_pct": None,
            "max_drawdown_pct": None,
        }
    timestamps = sorted(
        {row["timestamp"] for report in eligible for row in report["equity_curve"]}
    )
    latest = {
        str(report["symbol"]): float(report["initial_capital"]) for report in eligible
    }
    curves = {
        str(report["symbol"]): {
            row["timestamp"]: float(row["equity"]) for row in report["equity_curve"]
        }
        for report in eligible
    }
    starting = sum(float(report["initial_capital"]) for report in eligible)
    portfolio_curve = [
        {"timestamp": timestamps[0] if timestamps else None, "equity": starting}
    ]
    for timestamp in timestamps:
        for symbol, curve in curves.items():
            if timestamp in curve:
                latest[symbol] = curve[timestamp]
        portfolio_curve.append({"timestamp": timestamp, "equity": sum(latest.values())})
    ending = sum(float(report["final_equity"]) for report in eligible)
    all_signal_gaps: list[float] = []
    all_opposite_gaps: list[float] = []
    for report in eligible:
        events = [row for row in report["origin_results"] if row["signal"] != "none"]
        all_signal_gaps.extend(
            int(b["signal_index"]) - int(a["signal_index"]) for a, b in pairwise(events)
        )
        all_opposite_gaps.extend(
            int(b["signal_index"]) - int(a["signal_index"])
            for a, b in pairwise(events)
            if a["signal"] != b["signal"]
        )
    gross_notional = sum(
        float((report.get("turnover") or {}).get("gross_notional_traded") or 0)
        for report in eligible
    )
    average_portfolio = _mean([row["equity"] for row in portfolio_curve]) or starting
    sessions = max(1, len(timestamps))
    hit_rates: dict[str, Any] = {}
    for side in ("buy", "sell"):
        parts = [(report.get("hit_rates") or {}).get(side, {}) for report in eligible]
        signals = sum(int(part.get("signals") or 0) for part in parts)
        p50_hits = sum(int(part.get("avg_p50_hits") or 0) for part in parts)
        outer_hits = sum(int(part.get("outer_quantile_hits") or 0) for part in parts)
        hit_rates[side] = {
            "signals": signals,
            "avg_p50_hits": p50_hits,
            "avg_p50_hit_rate": p50_hits / signals if signals else None,
            "outer_quantile_hits": outer_hits,
            "outer_quantile_hit_rate": outer_hits / signals if signals else None,
        }
    return {
        "symbols": len(reports),
        "eligible_symbols": len(eligible),
        "full_500_origin_symbols": sum(
            bool(report.get("full_500_origin_coverage")) for report in eligible
        ),
        "origins_evaluated": sum(
            int(report["origins_available"]) for report in eligible
        ),
        "initial_capital": starting,
        "final_equity": ending,
        "profit": ending - starting,
        "return_pct": (ending / starting - 1) * 100,
        "max_drawdown_pct": _drawdown([row["equity"] for row in portfolio_curve]) * 100,
        "closed_trades": sum(len(report["trades"]) for report in eligible),
        "hit_rates": hit_rates,
        "turnover": {
            "gross_notional_traded": gross_notional,
            "gross_turnover_ratio": gross_notional / average_portfolio
            if average_portfolio
            else None,
            "annualized_gross_turnover_ratio": gross_notional
            / average_portfolio
            * 252
            / sessions,
            "annualized_one_way_turnover_ratio": gross_notional
            / average_portfolio
            / 2
            * 252
            / sessions,
            "mean_sessions_between_signals": _mean(all_signal_gaps),
            "median_sessions_between_signals": _median(all_signal_gaps),
            "mean_sessions_between_opposite_signals": _mean(all_opposite_gaps),
            "median_sessions_between_opposite_signals": _median(all_opposite_gaps),
        },
        "portfolio_equity_curve": portfolio_curve,
    }


class SpyDailyCloud:
    """Immutable encrypted records plus a tiny mutable per-shard cursor."""

    KINDS: ClassVar[set[str]] = {"source", "origin", "symbol", "final"}

    def __init__(self, configuration: Mapping[str, Any], *, create: bool):
        self.configuration = dict(configuration)
        self.id = "p90-" + digest(["spy-daily-rolling-500", self.configuration])[:24]
        self.campaign = Campaign(
            self.id, "spy-daily-" + os.getenv("GITHUB_RUN_ID", "local")
        )
        existing = self.campaign.lease.ref.get()
        if existing.exists:
            data = existing.to_dict() or {}
            if (
                data.get("research_kind") != "spy-daily-rolling-500"
                or data.get("configuration") != self.configuration
            ):
                raise ValueError("SPY_DAILY_CONFIGURATION_CONFLICT")
        elif create:
            from google.api_core.exceptions import AlreadyExists

            try:
                self.campaign.lease.ref.create(
                    {
                        "configuration": self.configuration,
                        "research_kind": "spy-daily-rolling-500",
                        "paper_only": True,
                        "created_at": iso_now(),
                    }
                )
            except AlreadyExists:
                data = self.campaign.lease.ref.get().to_dict() or {}
                if data.get("configuration") != self.configuration:
                    raise ValueError("SPY_DAILY_CONFIGURATION_CONFLICT")
        else:
            raise ValueError("SPY_DAILY_CAMPAIGN_NOT_FOUND")

    @classmethod
    def resume(cls, identifier: str, code_sha: str) -> SpyDailyCloud:
        if not re.fullmatch(r"p90-[a-f0-9]{24}", identifier):
            raise ValueError("INVALID_SPY_DAILY_CAMPAIGN")
        campaign = Campaign(
            identifier, "spy-daily-resume-" + os.getenv("GITHUB_RUN_ID", "local")
        )
        root = campaign.lease.ref.get().to_dict() or {}
        config = root.get("configuration") or {}
        if (
            root.get("research_kind") != "spy-daily-rolling-500"
            or config.get("code_sha") != code_sha
        ):
            raise ValueError("SPY_DAILY_RESUME_REQUIRES_ORIGINAL_CODE")
        value = cls(config, create=False)
        if value.id != identifier:
            raise ValueError("SPY_DAILY_CAMPAIGN_ID_MISMATCH")
        return value

    def _ref(self, kind: str, key: str):
        if kind not in self.KINDS or not key or len(key) > 300:
            raise ValueError("INVALID_SPY_DAILY_RECORD_KEY")
        return self.campaign.lease.ref.collection("spy_daily_" + kind).document(
            digest([kind, key])
        )

    def get(self, kind: str, key: str) -> Any:
        record = self._ref(kind, key).get()
        if not record.exists:
            return None
        pointer = record.to_dict() or {}
        if pointer.get("key") != key:
            raise ValueError("SPY_DAILY_RECORD_KEY_MISMATCH")
        with tempfile.TemporaryDirectory(prefix="spy-daily-read-") as directory:
            path = Path(directory) / "record.enc"
            self.campaign.download(pointer["archive"], path)
            value = decode_catalog(path)
        if digest(value) != pointer.get("content_sha256"):
            raise ValueError("SPY_DAILY_CONTENT_HASH_MISMATCH")
        return value

    def put(self, kind: str, key: str, value: Any) -> None:
        ref = self._ref(kind, key)
        checksum = digest(value)
        existing = ref.get()
        if existing.exists:
            payload = existing.to_dict() or {}
            if payload.get("content_sha256") != checksum or payload.get("key") != key:
                raise ValueError("IMMUTABLE_SPY_DAILY_RECORD_CONFLICT")
            return
        with tempfile.TemporaryDirectory(prefix="spy-daily-write-") as directory:
            path = Path(directory) / "record.enc"
            encode_catalog(value, path)
            archive = self.campaign.upload(
                path, "catalog" if kind == "source" else "game"
            )
        metadata = {
            "key": key,
            "archive": archive,
            "content_sha256": checksum,
            "code_sha": self.configuration["code_sha"],
            "created_at": iso_now(),
        }
        from google.api_core.exceptions import AlreadyExists

        try:
            ref.create(metadata)
        except AlreadyExists:
            current = ref.get().to_dict() or {}
            if current.get("content_sha256") != checksum or current.get("key") != key:
                raise ValueError("IMMUTABLE_SPY_DAILY_RECORD_CONFLICT")

    def progress(self, shard: int) -> dict[str, Any]:
        ref = self.campaign.lease.ref.collection("spy_daily_progress").document(
            f"shard-{shard:03d}"
        )
        return ref.get().to_dict() or {
            "revision": 0,
            "symbol_index": 0,
            "origin_rank": 0,
            "complete": False,
        }

    def advance(
        self, shard: int, expected_revision: int, value: Mapping[str, Any]
    ) -> dict[str, Any]:
        ref = self.campaign.lease.ref.collection("spy_daily_progress").document(
            f"shard-{shard:03d}"
        )

        @self.campaign.lease.fs.transactional
        def update(tx):
            current = ref.get(transaction=tx).to_dict() or {"revision": 0}
            if int(current.get("revision") or 0) != expected_revision:
                raise RuntimeError("SPY_DAILY_PROGRESS_CONTENTION")
            payload = {
                **dict(value),
                "revision": expected_revision + 1,
                "updated_at": iso_now(),
            }
            tx.set(ref, payload)
            return payload

        return self.campaign.lease.transact(update)


def _completed_as_of(requested: str | None = None) -> str:
    now = datetime.now(timezone.utc)
    end = date.fromisoformat(requested) if requested else now.date()
    rows = session_schedule((end - timedelta(days=14)).isoformat(), end.isoformat())
    eligible = [row for row in rows if datetime.fromisoformat(row["close"]) <= now]
    if requested:
        eligible = [row for row in eligible if row["date"] <= requested]
    if not eligible:
        raise ValueError(
            "no completed NYSE session is available for the requested as-of date"
        )
    return eligible[-1]["date"]


def _code_sha() -> str:
    value = os.getenv("QUANTURA_CODE_SHA") or os.getenv("GITHUB_SHA") or ""
    if not re.fullmatch(r"[a-f0-9]{40}", value):
        raise ValueError("IMMUTABLE_CODE_SHA_REQUIRED")
    return value


def prepare_campaign(
    *,
    shard_count: int,
    as_of: str | None = None,
    maximum_symbols: int | None = None,
) -> SpyDailyCloud:
    if not 1 <= shard_count <= 64:
        raise ValueError("shard count must be between 1 and 64")
    completed_as_of = _completed_as_of(as_of)
    securities, holdings_as_of, holdings_sha256 = fetch_spy_holdings()
    securities.append(
        {
            "symbol": "SPY",
            "company_name": "SPDR S&P 500 ETF Trust",
            "is_etf": True,
            "sector": None,
            "industry": None,
        }
    )
    securities = sorted(
        {row["symbol"]: row for row in securities}.values(),
        key=lambda row: row["symbol"],
    )
    if maximum_symbols:
        if maximum_symbols < 1:
            raise ValueError("maximum symbols must be positive")
        securities = securities[:maximum_symbols]
        if "SPY" not in {row["symbol"] for row in securities}:
            securities[-1] = {
                "symbol": "SPY",
                "company_name": "SPDR S&P 500 ETF Trust",
                "is_etf": True,
                "sector": None,
                "industry": None,
            }
            securities.sort(key=lambda row: row["symbol"])
    source_start = (
        date.fromisoformat(completed_as_of) - timedelta(days=2200)
    ).isoformat()
    histories = fetch_alpaca_histories(
        [row["symbol"] for row in securities], source_start, completed_as_of
    )
    for security in securities:
        security["rows"] = normalize_stock_history(
            histories.get(security["symbol"]) or [], as_of=completed_as_of
        )
        security["eligible_cutoffs"] = eligible_cutoffs(security["rows"])
    universe_identity = [
        {
            key: row.get(key)
            for key in (
                "symbol",
                "company_name",
                "is_etf",
                "holding_weight_pct",
                "shares_held",
            )
        }
        for row in securities
    ]
    configuration = {
        "schema_version": SCHEMA_VERSION,
        "code_sha": _code_sha(),
        "forecast_configuration_hash": stock_configuration_hash(),
        "as_of_session": completed_as_of,
        "source_start": source_start,
        "data_provider": "Alpaca IEX daily bars",
        "adjustment": "split",
        "feed": str(os.getenv("ALPACA_DATA_FEED") or "iex"),
        "constituent_source": "State Street official SPY daily fund holdings plus SPY",
        "constituent_source_url": SPY_HOLDINGS_URL,
        "holdings_as_of": holdings_as_of,
        "holdings_workbook_sha256": holdings_sha256,
        "universe_hash": digest(universe_identity),
        "symbol_count": len(securities),
        "shard_count": shard_count,
        "rolling_context_closes": ROLLING_CONTEXT,
        "maximum_recent_origins": BACKTEST_ORIGINS,
        "origin_order": "newest_first_within_symbol",
        "ensemble_profile": WEEKLY_ENSEMBLE_ENGINE,
        "models": list(FIVE_MODEL_IDS),
        "raw_weights": {model_id: 0.2 for model_id in FIVE_MODEL_IDS},
        "quantiles": list(QUANTILES),
        "prediction_sessions": 7,
        "maximum_symbols": maximum_symbols,
    }
    cloud = SpyDailyCloud(configuration, create=True)
    shards = [[] for _ in range(shard_count)]
    for index, security in enumerate(securities):
        shards[index % shard_count].append(security)
    for shard, items in enumerate(shards):
        cloud.put(
            "source",
            f"shard-{shard:03d}",
            {
                "schema_version": SCHEMA_VERSION,
                "campaign_id": cloud.id,
                "configuration_hash": digest(configuration),
                "shard": shard,
                "shard_count": shard_count,
                "securities": items,
            },
        )
    print(
        json.dumps(
            {
                "event": "spy_daily_campaign_prepared",
                "campaign_id": cloud.id,
                "as_of": completed_as_of,
                "symbols": len(securities),
                "full_500_origin_symbols": sum(
                    len(row["eligible_cutoffs"]) == BACKTEST_ORIGINS
                    for row in securities
                ),
                "eligible_origins": sum(
                    len(row["eligible_cutoffs"]) for row in securities
                ),
                "shards": shard_count,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return cloud


def _origin_key(shard: int, symbol: str, cutoff_index: int) -> str:
    return f"shard-{shard:03d}:{symbol}:{cutoff_index}"


def run_shard(
    cloud: SpyDailyCloud,
    shard: int,
    *,
    budget_minutes: int,
    mock: bool = False,
    maximum_new_origins: int | None = None,
) -> dict[str, Any]:
    if not 1 <= budget_minutes <= 300:
        raise ValueError("budget minutes must be between 1 and 300")
    if shard < 0 or shard >= int(cloud.configuration["shard_count"]):
        raise ValueError("invalid shard")
    source = cloud.get("source", f"shard-{shard:03d}")
    if not source or source.get("configuration_hash") != digest(cloud.configuration):
        raise ValueError("SPY_DAILY_SOURCE_SNAPSHOT_REQUIRED")
    securities = list(source["securities"])
    progress = cloud.progress(shard)
    if progress.get("complete"):
        return progress
    started = time.monotonic()
    deadline = started + budget_minutes * 60
    completed_now = 0
    forecast = stock_worker_forecaster(mock=mock)
    while int(progress.get("symbol_index") or 0) < len(securities):
        symbol_index = int(progress.get("symbol_index") or 0)
        security = securities[symbol_index]
        cutoffs = list(security.get("eligible_cutoffs") or [])
        rank = int(progress.get("origin_rank") or 0)
        if rank < len(cutoffs):
            if maximum_new_origins is not None and completed_now >= maximum_new_origins:
                break
            if time.monotonic() >= deadline - 600:
                break
            cutoff = int(cutoffs[rank])
            record = evaluate_origin(security, security["rows"], cutoff, forecast)
            cloud.put("origin", _origin_key(shard, security["symbol"], cutoff), record)
            progress = cloud.advance(
                shard,
                int(progress.get("revision") or 0),
                {
                    "symbol_index": symbol_index,
                    "origin_rank": rank + 1,
                    "complete": False,
                    "last_symbol": security["symbol"],
                    "last_cutoff_at": record["cutoff_at"],
                    "completed_origins": int(progress.get("completed_origins") or 0)
                    + 1,
                },
            )
            completed_now += 1
            print(
                json.dumps(
                    {
                        "event": "spy_daily_origin_checkpointed",
                        "campaign_id": cloud.id,
                        "shard": shard,
                        "symbol": security["symbol"],
                        "origin_rank": rank + 1,
                        "origins_for_symbol": len(cutoffs),
                        "signal_at": record["signal_at"],
                        "signal": record["signal"],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            continue

        if cutoffs and time.monotonic() >= deadline - 300:
            break
        origin_results = [
            cloud.get("origin", _origin_key(shard, security["symbol"], int(cutoff)))
            for cutoff in cutoffs
        ]
        if any(row is None for row in origin_results):
            raise RuntimeError("SPY_DAILY_ORIGIN_CHECKPOINT_MISSING")
        report = build_symbol_report(security, security["rows"], origin_results)
        cloud.put("symbol", security["symbol"], report)
        progress = cloud.advance(
            shard,
            int(progress.get("revision") or 0),
            {
                "symbol_index": symbol_index + 1,
                "origin_rank": 0,
                "complete": symbol_index + 1 == len(securities),
                "last_symbol": security["symbol"],
                "completed_origins": int(progress.get("completed_origins") or 0),
                "completed_symbols": symbol_index + 1,
            },
        )
        print(
            json.dumps(
                {
                    "event": "spy_daily_symbol_complete",
                    "campaign_id": cloud.id,
                    "shard": shard,
                    "symbol": security["symbol"],
                    "origins": len(cutoffs),
                    "return_pct": report["return_pct"],
                    "max_drawdown_pct": report["max_drawdown_pct"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
    if int(progress.get("symbol_index") or 0) >= len(securities) and not progress.get(
        "complete"
    ):
        progress = cloud.advance(
            shard,
            int(progress.get("revision") or 0),
            {**progress, "complete": True, "completed_symbols": len(securities)},
        )
    print(
        json.dumps(
            {
                "event": "spy_daily_shard_checkpoint",
                "campaign_id": cloud.id,
                "shard": shard,
                "complete": bool(progress.get("complete")),
                "completed_symbols": int(progress.get("completed_symbols") or 0),
                "symbols": len(securities),
                "completed_origins": int(progress.get("completed_origins") or 0),
                "new_origins": completed_now,
                "runtime_seconds": round(time.monotonic() - started, 2),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return progress


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not rows:
            return
        fields: list[str] = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_final_reports(
    output: Path, cloud: SpyDailyCloud, reports: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    aggregate = aggregate_reports(reports)
    compact_symbols = [
        {
            key: value
            for key, value in report.items()
            if key
            not in {"origin_results", "trades", "signal_hit_results", "equity_curve"}
        }
        for report in reports
    ]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": iso_now(),
        "campaign_id": cloud.id,
        "configuration": cloud.configuration,
        "methodology": {
            "paper_only": True,
            "rolling_context": "exactly 500 completed closes at every origin",
            "origins": "up to the 500 most recent eligible one-session cutoffs per symbol, computed newest first",
            "withheld_close": "the immediately following completed NYSE-session close is excluded from inference",
            "signal": "withheld close below first-step P10 buys; above first-step P90 shorts; strict inequalities",
            "forecast": "seven NYSE sessions; P1/P10/P25/P50/P75/P90/P99",
            "ensemble": "Prophet, Toto 4M, Granite, Chronos-2, TimesFM; raw weights 0.20 each; fail closed",
            "tails": "unsupported model tails are excluded only at those quantiles; supported models retain equal effective weight",
            "trade_exit": "first opposite signal, seven completed closes, or end of evaluated data",
            "sizing": "one fully invested non-overlapping long/short position per symbol; equal $10,000 initial allocation",
            "costs": "fees, slippage, short borrow, dividends and taxes excluded",
            "bias": "current constituents create survivorship bias; split-adjusted close returns are not total returns",
            "checkpoint_training_cutoff": "foundation-model pretraining cutoffs are not independently audited against every historical origin",
        },
        "aggregate": {
            key: value
            for key, value in aggregate.items()
            if key != "portfolio_equity_curve"
        },
        "symbols": compact_symbols,
    }
    (output / "report.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    origins = [row for report in reports for row in report["origin_results"]]
    trades = [row for report in reports for row in report["trades"]]
    hits = [row for report in reports for row in report["signal_hit_results"]]
    _write_csv(output / "symbols.csv", compact_symbols)
    _write_csv(output / "origins.csv", origins)
    _write_csv(output / "trades.csv", trades)
    _write_csv(output / "signal_hits.csv", hits)
    _write_csv(output / "portfolio_equity.csv", aggregate["portfolio_equity_curve"])
    turnover = aggregate.get("turnover") or {}
    lines = [
        "# SPY constituents rolling daily-close backtest",
        "",
        f"Campaign: `{cloud.id}`  ",
        f"Alpaca completed through: {cloud.configuration['as_of_session']}  ",
        f"Code: `{cloud.configuration['code_sha']}`",
        "",
        "Every forecast uses exactly 500 prior split-adjusted Alpaca IEX closes and withholds the following close. No orders were sent.",
        "",
        f"- Symbols in frozen universe: {aggregate['symbols']}",
        f"- Symbols with at least one eligible origin: {aggregate['eligible_symbols']}",
        f"- Symbols with all 500 origins: {aggregate['full_500_origin_symbols']}",
        f"- Forecast origins: {aggregate['origins_evaluated']:,}",
        f"- Closed trades: {aggregate['closed_trades']:,}",
        f"- Equal-weight portfolio start: ${aggregate['initial_capital']:,.2f}",
        f"- Final equity: ${aggregate['final_equity']:,.2f}",
        f"- Profit: ${aggregate['profit']:,.2f}",
        f"- Return: {aggregate['return_pct']:.4f}%",
        f"- Maximum drawdown: {aggregate['max_drawdown_pct']:.4f}%",
        f"- Annualized gross turnover: {turnover.get('annualized_gross_turnover_ratio', 0):.4f}x",
        f"- Annualized one-way turnover: {turnover.get('annualized_one_way_turnover_ratio', 0):.4f}x",
        f"- Mean sessions between signals: {turnover.get('mean_sessions_between_signals')}",
        f"- Median sessions between signals: {turnover.get('median_sessions_between_signals')}",
        f"- Mean sessions between opposite signals: {turnover.get('mean_sessions_between_opposite_signals')}",
        "",
    ]
    for side, target in (("buy", "P90"), ("sell", "P10")):
        summary = aggregate["hit_rates"][side]
        lines.append(
            f"- {side.title()} signals: {summary['signals']:,}; average P50 hits: {summary['avg_p50_hits']:,} "
            f"({summary['avg_p50_hit_rate'] if summary['avg_p50_hit_rate'] is not None else 'n/a'}); "
            f"average {target} hits: {summary['outer_quantile_hits']:,} "
            f"({summary['outer_quantile_hit_rate'] if summary['outer_quantile_hit_rate'] is not None else 'n/a'})"
        )
    lines += [
        "",
        "Limitations: current-constituent survivorship bias; split-adjusted prices omit dividends; foundation-model pretraining cutoffs are unaudited at each historical origin; fees, slippage, borrow and taxes are excluded. This is retrospective research, not investment advice.",
    ]
    (output / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def aggregate_campaign(
    cloud: SpyDailyCloud, output: Path
) -> tuple[bool, dict[str, Any]]:
    incomplete: list[dict[str, Any]] = []
    all_progress: list[dict[str, Any]] = []
    securities: list[dict[str, Any]] = []
    for shard in range(int(cloud.configuration["shard_count"])):
        source = cloud.get("source", f"shard-{shard:03d}")
        if not source:
            raise ValueError("SPY_DAILY_SOURCE_SHARD_MISSING")
        securities.extend(source["securities"])
        progress = cloud.progress(shard)
        all_progress.append(progress)
        if not progress.get("complete"):
            incomplete.append(
                {"shard": shard, **progress, "symbols": len(source["securities"])}
            )
    if incomplete:
        output.mkdir(parents=True, exist_ok=True)
        progress_report = {
            "schema_version": SCHEMA_VERSION,
            "campaign_id": cloud.id,
            "complete": False,
            "configuration": cloud.configuration,
            "incomplete_shards": incomplete,
            "completed_origins": sum(
                int(row.get("completed_origins") or 0) for row in all_progress
            ),
        }
        (output / "progress.json").write_text(
            json.dumps(progress_report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (output / "summary.md").write_text(
            "# SPY daily backtest progress\n\n"
            f"Campaign `{cloud.id}` remains checkpointed and incomplete. "
            f"{len(incomplete)} of {cloud.configuration['shard_count']} shards still need work.\n",
            encoding="utf-8",
        )
        return False, progress_report
    reports = []
    for security in sorted(securities, key=lambda row: row["symbol"]):
        report = cloud.get("symbol", security["symbol"])
        if report is None:
            raise ValueError(f"SPY_DAILY_SYMBOL_REPORT_MISSING:{security['symbol']}")
        reports.append(report)
    payload = write_final_reports(output, cloud, reports)
    cloud.put("final", "portfolio", payload)
    return True, payload


def _write_output(name: str, value: Any) -> None:
    path = os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(f"{name}={value}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--shards", type=int, default=DEFAULT_SHARDS)
    prepare.add_argument("--as-of")
    prepare.add_argument("--max-symbols", type=int)
    resume = sub.add_parser("resume")
    resume.add_argument("--campaign", required=True)
    worker = sub.add_parser("run-shard")
    worker.add_argument("--campaign", required=True)
    worker.add_argument("--shard", type=int, required=True)
    worker.add_argument("--budget-minutes", type=int, default=270)
    worker.add_argument("--max-new-origins", type=int)
    worker.add_argument("--mock", action="store_true")
    aggregate = sub.add_parser("aggregate")
    aggregate.add_argument("--campaign", required=True)
    aggregate.add_argument(
        "--output", type=Path, default=Path("artifacts/spy-daily-backtest")
    )
    args = parser.parse_args()
    if args.command == "prepare":
        cloud = prepare_campaign(
            shard_count=args.shards, as_of=args.as_of, maximum_symbols=args.max_symbols
        )
        _write_output("campaign_id", cloud.id)
        _write_output(
            "matrix",
            json.dumps({"shard": list(range(args.shards))}, separators=(",", ":")),
        )
        return
    cloud = SpyDailyCloud.resume(args.campaign, _code_sha())
    if args.command == "resume":
        _write_output("campaign_id", cloud.id)
        _write_output(
            "matrix",
            json.dumps(
                {"shard": list(range(int(cloud.configuration["shard_count"])))},
                separators=(",", ":"),
            ),
        )
    elif args.command == "run-shard":
        progress = run_shard(
            cloud,
            args.shard,
            budget_minutes=args.budget_minutes,
            mock=args.mock,
            maximum_new_origins=args.max_new_origins,
        )
        _write_output("complete", str(bool(progress.get("complete"))).lower())
    elif args.command == "aggregate":
        complete, report = aggregate_campaign(cloud, args.output)
        _write_output("complete", str(complete).lower())
        print(
            json.dumps(
                {
                    "campaign_id": cloud.id,
                    "complete": complete,
                    "report": report.get("aggregate"),
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
