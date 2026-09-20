"""Strict daily Kalshi perpetual forecast backtest and GitHub Actions report.

Every origin supplies only observations at or before its cutoff to the forecast
engine. The next completed daily close is withheld and becomes the signal price.
This module is research reporting only; it never calls trading endpoints.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from scripts.weekly_screener import ENGINE as WEEKLY_ENSEMBLE_ENGINE
from scripts.weekly_screener import weekly_configuration

from .capabilities import MODEL_REGISTRY
from .schemas import APPROVED_MODELS
from .worker import execute_job

KALSHI_ORIGIN = "https://api.elections.kalshi.com/trade-api/v2"
FIVE_MODEL_IDS = tuple(APPROVED_MODELS)
FIVE_MODEL_MINIMUM_HISTORY = max(
    2,
    *(int(MODEL_REGISTRY["models"][model_id].get("minimumObservedContext") or 2) for model_id in FIVE_MODEL_IDS),
)
ForecastFn = Callable[[list[dict[str, Any]], Mapping[str, Any]], Mapping[str, Any]]


def five_model_backtest_configuration() -> dict[str, Any]:
    """Return the pinned weekly ensemble profile on a continuous daily horizon."""
    weekly = weekly_configuration()
    return {
        "prediction_length": 7,
        "horizon_mode": "frequency_periods",
        "frequency": "1D",
        "calendar": "NONE",
        "quantiles": list(weekly["quantiles"]),
        "context_length": int(weekly["context_length"]),
        "transform": str(weekly["transform"]),
        "failure_policy": "fail",
        "models": dict(weekly["models"]),
        "toto_variant": str(weekly["toto_variant"]),
        "model_checkpoints": dict(weekly["model_checkpoints"]),
        "model_revisions": dict(weekly["model_revisions"]),
    }


def five_model_configuration_hash() -> str:
    payload = json.dumps(five_model_backtest_configuration(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def validate_five_model_result(result: Mapping[str, Any]) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    models = result.get("models")
    failures = result.get("failures")
    if not isinstance(models, list) or not isinstance(failures, list):
        raise ValueError("five-model forecast metadata is missing")
    completed = tuple(
        str(row.get("id")) for row in models
        if isinstance(row, Mapping) and row.get("status") == "completed"
    )
    if failures or completed != FIVE_MODEL_IDS or len(models) != len(FIVE_MODEL_IDS):
        raise ValueError("all five weekly-ensemble models must complete at every backtest origin")
    effective = result.get("effective_weights_by_quantile")
    for quantile in ("0.1", "0.5", "0.9"):
        weights = effective.get(quantile) if isinstance(effective, Mapping) else None
        if not isinstance(weights, Mapping) or set(weights) != set(FIVE_MODEL_IDS) or any(
            not math.isclose(float(weights[model_id]), 0.2, rel_tol=0, abs_tol=1e-12)
            for model_id in FIVE_MODEL_IDS
        ):
            raise ValueError(f"five-model forecast requires equal effective weights at P{int(float(quantile) * 100)}")
    predictions = result.get("predictions")
    if not isinstance(predictions, list) or len(predictions) != 7 or not all(isinstance(row, dict) for row in predictions):
        raise ValueError("five-model forecast must return exactly seven daily predictions")
    return predictions, completed


def underlying_units(market: Mapping[str, Any]) -> float:
    units = float(market["contract_size"]) * float(market["underlying_multiplier"])
    if not math.isfinite(units) or units <= 0:
        raise ValueError("invalid Kalshi perpetual contract exposure")
    return units


def normalize_daily_candles(candles: Sequence[Mapping[str, Any]], market: Mapping[str, Any]) -> list[dict[str, Any]]:
    units = underlying_units(market)
    unique: dict[int, dict[str, Any]] = {}
    for candle in candles:
        timestamp = candle.get("end_period_ts")
        close = (candle.get("price") or {}).get("close")
        if not isinstance(timestamp, int) or close is None:
            continue
        value = float(close) / units
        if not math.isfinite(value) or value <= 0:
            continue
        row = {"timestamp": datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z"), "target": value}
        if timestamp in unique and unique[timestamp] != row:
            raise ValueError("conflicting Kalshi daily candle")
        unique[timestamp] = row
    return [unique[key] for key in sorted(unique)]


def worker_forecaster(*, mock: bool = False) -> ForecastFn:
    config = five_model_backtest_configuration()
    request = {key: value for key, value in config.items() if key not in {"model_checkpoints", "model_revisions", "toto_variant"}}

    def forecast(history: list[dict[str, Any]], market: Mapping[str, Any]) -> Mapping[str, Any]:
        result = execute_job({
            "source": {"type": "kalshi_perp", "provider": "kalshi_perps", "symbol": market["ticker"]},
            "request": request,
            "runtime_mode": "test" if mock else "production",
            "model_checkpoints": config["model_checkpoints"],
            "model_revisions": config["model_revisions"],
            "input": {"rows": history, "frequency": "1D", "timezone": "UTC"},
        }, mock=mock, minimum_history_rows=FIVE_MODEL_MINIMUM_HISTORY)
        validate_five_model_result(result)
        return result
    return forecast


def _q(row: Mapping[str, Any], quantile: str) -> float:
    return float((row.get("quantiles") or {})[quantile])


def _drawdown(equity: Sequence[float]) -> float:
    peak = 0.0
    maximum = 0.0
    for value in equity:
        peak = max(peak, value)
        if peak > 0:
            maximum = max(maximum, (peak - value) / peak)
    return maximum


def evaluate_market(
    market: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], forecast: ForecastFn, *,
    minimum_history: int = FIVE_MODEL_MINIMUM_HISTORY, maximum_origins: int | None = None, initial_capital: float = 10_000,
) -> dict[str, Any]:
    if minimum_history < FIVE_MODEL_MINIMUM_HISTORY or initial_capital <= 0:
        raise ValueError("invalid backtest configuration")
    observations = [{"timestamp": str(row["timestamp"]), "target": float(row["target"])} for row in rows]
    origins = list(range(minimum_history - 1, len(observations) - 1))
    if maximum_origins:
        origins = origins[-maximum_origins:]
    records: list[dict[str, Any]] = []
    for cutoff_index in origins:
        history = [dict(row) for row in observations[:cutoff_index + 1]]
        forecast_result = forecast(history, market)
        predictions, completed_models = validate_five_model_result(forecast_result)
        first = predictions[0]
        actual_index = cutoff_index + 1
        actual = observations[actual_index]
        p10, p50, p90 = _q(first, "0.1"), _q(first, "0.5"), _q(first, "0.9")
        signal = "buy" if actual["target"] < p10 else "sell" if actual["target"] > p90 else "none"
        records.append({
            "symbol": market["ticker"], "cutoff_index": cutoff_index, "cutoff_at": history[-1]["timestamp"],
            "signal_index": actual_index, "signal_at": actual["timestamp"], "actual": actual["target"], "signal": signal,
            "p10": p10, "p50": p50, "p90": p90,
            "avg_p10": sum(_q(row, "0.1") for row in predictions) / 7,
            "avg_p50": sum(_q(row, "0.5") for row in predictions) / 7,
            "avg_p90": sum(_q(row, "0.9") for row in predictions) / 7,
            "ensemble_models": ",".join(completed_models),
            "forecast_result_hash": forecast_result.get("result_hash"),
        })

    events = [record for record in records if record["signal"] != "none"]
    events_by_index = {record["signal_index"]: record for record in events}
    hit_rows: list[dict[str, Any]] = []
    for event in events:
        opposite = "sell" if event["signal"] == "buy" else "buy"
        stop = min(len(observations) - 1, event["signal_index"] + 7)
        opposite_event = next((candidate for candidate in events if candidate["signal_index"] > event["signal_index"] and candidate["signal_index"] <= stop and candidate["signal"] == opposite), None)
        if opposite_event:
            stop = opposite_event["signal_index"] - 1
        future = observations[event["signal_index"] + 1:stop + 1]
        if event["signal"] == "buy":
            crossed_p50 = any(row["target"] >= event["avg_p50"] for row in future)
            crossed_outer = any(row["target"] >= event["avg_p90"] for row in future)
            outer_name = "avg_p90"
        else:
            crossed_p50 = any(row["target"] <= event["avg_p50"] for row in future)
            crossed_outer = any(row["target"] <= event["avg_p10"] for row in future)
            outer_name = "avg_p10"
        hit_rows.append({**event, "crossed_avg_p50": crossed_p50, "crossed_outer": crossed_outer,
                         "outer_target": outer_name, "observed_closes": len(future),
                         "opposite_signal_at": opposite_event["signal_at"] if opposite_event else None})

    capital = initial_capital
    position: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    first_signal_index = events[0]["signal_index"] if events else len(observations)
    for index, row in enumerate(observations):
        event = events_by_index.get(index)
        if position:
            direction = 1 if position["side"] == "buy" else -1
            marked = position["entry_equity"] * (1 + direction * (row["target"] / position["entry_price"] - 1))
            opposite = event is not None and event["signal"] != position["side"]
            expired = index >= position["deadline"]
            if opposite or expired or index == len(observations) - 1:
                capital = marked
                trades.append({
                    "symbol": market["ticker"], "side": position["side"], "entry_at": position["entry_at"],
                    "entry_price": position["entry_price"], "exit_at": row["timestamp"], "exit_price": row["target"],
                    "exit_reason": "opposite_signal" if opposite else "seven_closes" if expired else "end_of_data",
                    "return_pct": (capital / position["entry_equity"] - 1) * 100,
                    "profit": capital - position["entry_equity"], "holding_closes": index - position["entry_index"],
                })
                position = None
            else:
                capital = marked
        if position is None and event is not None and index < len(observations) - 1:
            position = {"side": event["signal"], "entry_index": index, "entry_at": row["timestamp"],
                        "entry_price": row["target"], "entry_equity": capital, "deadline": index + 7}
        if index >= first_signal_index:
            equity_curve.append({"timestamp": row["timestamp"], "equity": capital})

    def hit_summary(side: str) -> dict[str, Any]:
        subset = [row for row in hit_rows if row["signal"] == side]
        count = len(subset)
        p50_hits = sum(bool(row["crossed_avg_p50"]) for row in subset)
        outer_hits = sum(bool(row["crossed_outer"]) for row in subset)
        return {"signals": count, "avg_p50_hits": p50_hits, "avg_p50_hit_rate": p50_hits / count if count else None,
                "outer_quantile_hits": outer_hits, "outer_quantile_hit_rate": outer_hits / count if count else None}

    equity_values = [initial_capital] + [row["equity"] for row in equity_curve]
    return {
        "symbol": market["ticker"], "title": market.get("title"), "observations": len(observations),
        "first_observation": observations[0]["timestamp"] if observations else None,
        "last_observation": observations[-1]["timestamp"] if observations else None,
        "origins_evaluated": len(records), "signal_counts": {side: sum(row["signal"] == side for row in records) for side in ("buy", "sell", "none")},
        "hit_rates": {"buy": hit_summary("buy"), "sell": hit_summary("sell")},
        "initial_capital": initial_capital, "final_equity": capital,
        "profit": capital - initial_capital, "return_pct": (capital / initial_capital - 1) * 100,
        "max_drawdown_pct": _drawdown(equity_values) * 100, "trades": trades,
        "origin_results": records, "signal_hit_results": hit_rows, "equity_curve": equity_curve,
    }


def aggregate_reports(reports: Sequence[Mapping[str, Any]], initial_capital: float) -> dict[str, Any]:
    if not reports:
        return {"markets": 0, "initial_capital": 0, "final_equity": 0, "profit": 0, "return_pct": None, "max_drawdown_pct": None}
    timestamps = sorted({row["timestamp"] for report in reports for row in report["equity_curve"]})
    latest = {str(report["symbol"]): initial_capital for report in reports}
    curves = {str(report["symbol"]): {row["timestamp"]: row["equity"] for row in report["equity_curve"]} for report in reports}
    portfolio: list[float] = [initial_capital * len(reports)]
    for timestamp in timestamps:
        for symbol, curve in curves.items():
            if timestamp in curve:
                latest[symbol] = curve[timestamp]
        portfolio.append(sum(latest.values()))
    starting = initial_capital * len(reports)
    ending = sum(float(report["final_equity"]) for report in reports)
    hit_rates: dict[str, Any] = {}
    for side in ("buy", "sell"):
        summaries = [(report.get("hit_rates") or {}).get(side, {}) for report in reports]
        signals = sum(int(summary.get("signals") or 0) for summary in summaries)
        p50_hits = sum(int(summary.get("avg_p50_hits") or 0) for summary in summaries)
        outer_hits = sum(int(summary.get("outer_quantile_hits") or 0) for summary in summaries)
        hit_rates[side] = {"signals": signals, "avg_p50_hits": p50_hits,
                           "avg_p50_hit_rate": p50_hits / signals if signals else None,
                           "outer_quantile_hits": outer_hits,
                           "outer_quantile_hit_rate": outer_hits / signals if signals else None}
    return {"markets": len(reports), "initial_capital": starting, "final_equity": ending, "profit": ending - starting,
            "return_pct": (ending / starting - 1) * 100, "max_drawdown_pct": _drawdown(portfolio) * 100,
            "origins_evaluated": sum(int(report["origins_evaluated"]) for report in reports),
            "trades": sum(len(report["trades"]) for report in reports), "hit_rates": hit_rates}


def fetch_markets_and_history(
    *, days: int = 365, maximum_markets: int | None = None, market_ticker: str | None = None,
) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    end = int(datetime.now(timezone.utc).timestamp())
    start = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    with httpx.Client(base_url=KALSHI_ORIGIN, timeout=30, headers={"Accept": "application/json", "User-Agent": "Quantura-Perpetual-Backtest/1.0"}) as client:
        response = client.get("/margin/markets")
        response.raise_for_status()
        markets = [dict(row) for row in response.json().get("markets", []) if row.get("status") == "active"]
        if market_ticker:
            markets = [row for row in markets if row.get("ticker") == market_ticker]
            if not markets:
                raise ValueError(f"active Kalshi perpetual not found: {market_ticker}")
        if maximum_markets:
            markets = markets[:maximum_markets]
        output = []
        for market in markets:
            response = client.get(f"/margin/markets/{market['ticker']}/candlesticks", params={"start_ts": start, "end_ts": end, "period_interval": 1440, "include_latest_before_start": "false"})
            response.raise_for_status()
            output.append((market, normalize_daily_candles(response.json().get("candlesticks", []), market)))
        return output


def _write_reports(output: Path, report: dict[str, Any]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    origins = [row for market in report["market_results"] for row in market["origin_results"]]
    signal_hits = [row for market in report["market_results"] for row in market["signal_hit_results"]]
    trades = [row for market in report["market_results"] for row in market["trades"]]
    for filename, rows in (("origins.csv", origins), ("signal_hits.csv", signal_hits), ("trades.csv", trades)):
        with (output / filename).open("w", encoding="utf-8", newline="") as handle:
            if rows:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
    aggregate = report["aggregate"]
    lines = ["# Kalshi perpetual daily backtest", "", f"Generated: {report['generated_at']}", "",
             "Prices are normalized to USD per underlying unit. Every origin withholds the next close; no future observation is supplied to forecasting.",
             "Every cutoff requires the pinned equal-weight weekly ensemble: Prophet, Toto 4M, Granite, Chronos-2, and TimesFM. A missing model fails the run; weights are never silently renormalized.", "",
             f"- Markets: {aggregate['markets']}", f"- Forecast origins: {aggregate.get('origins_evaluated', 0)}",
             f"- Closed trades: {aggregate.get('trades', 0)}", f"- Profit on equal ${report['methodology']['initial_capital_per_market']:,.0f} allocations: ${aggregate['profit']:,.2f}",
             f"- Portfolio return: {aggregate['return_pct']:.2f}%" if aggregate["return_pct"] is not None else "- Portfolio return: unavailable",
             f"- Maximum drawdown: {aggregate['max_drawdown_pct']:.2f}%" if aggregate["max_drawdown_pct"] is not None else "- Maximum drawdown: unavailable", ""]
    for side, outer in (("buy", "P90"), ("sell", "P10")):
        hits=aggregate.get("hit_rates",{}).get(side,{})
        lines += [f"- {side.title()} signals: {hits.get('signals',0)}; average P50 hits: {hits.get('avg_p50_hits',0)}; average {outer} hits: {hits.get('outer_quantile_hits',0)}"]
    lines.append("")
    for market in report["market_results"]:
        lines += [f"## {market['symbol']}", "", f"{market['observations']} closes; {market['origins_evaluated']} origins; {len(market['trades'])} trades; return {market['return_pct']:.2f}%; max drawdown {market['max_drawdown_pct']:.2f}%.", "",
                  f"Buy signals: {market['hit_rates']['buy']['signals']} (P50 hits {market['hit_rates']['buy']['avg_p50_hits']}, P90 hits {market['hit_rates']['buy']['outer_quantile_hits']}).",
                  f"Sell signals: {market['hit_rates']['sell']['signals']} (P50 hits {market['hit_rates']['sell']['avg_p50_hits']}, P10 hits {market['hit_rates']['sell']['outer_quantile_hits']}).", ""]
    (output / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def combine_component_reports(paths: Sequence[Path], output: Path) -> dict[str, Any]:
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(paths)]
    if not reports:
        raise ValueError("no component backtest reports found")
    first = reports[0]
    identity = (
        first.get("schema_version"), first.get("mode"),
        (first.get("methodology") or {}).get("configuration_hash"),
        (first.get("methodology") or {}).get("minimum_history"),
        (first.get("methodology") or {}).get("initial_capital_per_market"),
    )
    if any((
        report.get("schema_version"), report.get("mode"),
        (report.get("methodology") or {}).get("configuration_hash"),
        (report.get("methodology") or {}).get("minimum_history"),
        (report.get("methodology") or {}).get("initial_capital_per_market"),
    ) != identity for report in reports):
        raise ValueError("component backtest reports do not share one immutable configuration")
    market_results = [market for report in reports for market in report.get("market_results", [])]
    symbols = [market.get("symbol") for market in market_results]
    if len(symbols) != len(set(symbols)):
        raise ValueError("component backtest reports contain duplicate markets")
    initial_capital = float(first["methodology"]["initial_capital_per_market"])
    combined = {
        **{key: first[key] for key in ("schema_version", "mode", "data_provider", "methodology")},
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "coverage": {
            "catalog_markets": sum(int(report["coverage"]["catalog_markets"]) for report in reports),
            "reported_markets": len(market_results),
            "skipped_for_insufficient_history": sorted({
                symbol for report in reports for symbol in report["coverage"]["skipped_for_insufficient_history"]
            }),
            "component_reports": len(reports),
        },
        "aggregate": aggregate_reports(market_results, initial_capital),
        "market_results": market_results,
    }
    _write_reports(output, combined)
    return combined


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("artifacts/kalshi-perpetual-backtest"))
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--max-markets", type=int)
    parser.add_argument("--market", help="Evaluate one exact active perpetual ticker (used by the Actions matrix).")
    parser.add_argument("--max-origins", type=int)
    parser.add_argument("--minimum-history", type=int, default=FIVE_MODEL_MINIMUM_HISTORY)
    parser.add_argument("--initial-capital", type=float, default=10_000)
    parser.add_argument("--mock", action="store_true", help="Use deterministic test adapters; never use for reported production results.")
    parser.add_argument("--combine-dir", type=Path, help="Combine per-market report.json files without running inference.")
    args = parser.parse_args()
    if args.minimum_history < FIVE_MODEL_MINIMUM_HISTORY:
        parser.error(f"--minimum-history must be at least {FIVE_MODEL_MINIMUM_HISTORY} for the five-model weekly ensemble")
    if args.combine_dir:
        paths = [path for path in args.combine_dir.rglob("report.json") if args.output not in path.parents]
        combined = combine_component_reports(paths, args.output)
        print(json.dumps({"output": str(args.output), "aggregate": combined["aggregate"], "coverage": combined["coverage"]}, indent=2))
        return
    data = fetch_markets_and_history(days=args.days, maximum_markets=args.max_markets, market_ticker=args.market)
    forecast = worker_forecaster(mock=args.mock)
    results = [evaluate_market(market, rows, forecast, minimum_history=args.minimum_history, maximum_origins=args.max_origins, initial_capital=args.initial_capital) for market, rows in data if len(rows) > args.minimum_history]
    report = {
        "schema_version": "kalshi_perpetual_backtest_v2", "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "five_model_weekly_ensemble_mock" if args.mock else "five_model_weekly_ensemble_real",
        "data_provider": "Kalshi public margin market data",
        "methodology": {"frequency": "1D", "prediction_length": 7, "quantiles": five_model_backtest_configuration()["quantiles"],
            "ensemble_profile": WEEKLY_ENSEMBLE_ENGINE, "models": list(FIVE_MODEL_IDS),
            "raw_weights": {model_id: 0.2 for model_id in FIVE_MODEL_IDS}, "toto_variant": "4m",
            "model_failure_policy": "fail", "configuration_hash": five_model_configuration_hash(),
            "signal": "withheld next close < P10 buy; > P90 sell (strict)",
            "target_hits": "next seven completed closes, stopping before the first opposite signal; buy tests average P50/P90, sell tests average P50/P10",
            "trade_exit": "first opposite signal, seven completed closes, or end of available data",
            "sizing": "one fully invested, non-overlapping position per market; equal initial capital per market; no fees, slippage or funding",
            "initial_capital_per_market": args.initial_capital, "minimum_history": args.minimum_history},
        "coverage": {"catalog_markets": len(data), "reported_markets": len(results), "skipped_for_insufficient_history": [market["ticker"] for market, rows in data if len(rows) <= args.minimum_history]},
        "aggregate": aggregate_reports(results, args.initial_capital), "market_results": results,
    }
    _write_reports(args.output, report)
    print(json.dumps({"output": str(args.output), "aggregate": report["aggregate"], "coverage": report["coverage"]}, indent=2))


if __name__ == "__main__":
    main()
