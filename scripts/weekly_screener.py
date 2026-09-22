"""Shared five-model screener specification and exchange-session input selection.

No drift/synthetic fallback. Model failures leave the prior validated release intact.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import base64
import gzip
from typing import Any, Callable, Mapping, Sequence

QUANTILES = (0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99)
QUANTILE_NAMES = ("p01", "p10", "p25", "p50", "p75", "p90", "p99")
ENGINE = "quantura_weekly_ensemble_v2"


def weekly_configuration() -> dict[str, Any]:
    from ensemble_forecasting.capabilities import MODEL_REGISTRY
    models = MODEL_REGISTRY["models"]
    toto = next(item for item in models["toto"]["variants"] if item["id"] == "4m")
    return {
        "prediction_length": 7, "horizon_mode": "trading_sessions", "frequency": "1D", "calendar": "NYSE",
        "quantiles": list(QUANTILES), "context_length": 512, "transform": "log", "model_failure_policy": "fail",
        "models": {name: {"enabled": True, "weight": 0.2} for name in models},
        "toto_variant": "4m", "history_lag_sessions": 0, "adjustment": "split",
        "model_checkpoints": {name: (toto if name == "toto" else model).get("checkpoint") for name, model in models.items()},
        "model_revisions": {name: (toto if name == "toto" else model)["checkpointRevision"] for name, model in models.items() if (toto if name == "toto" else model).get("checkpointRevision")},
    }


def configuration_hash() -> str:
    return hashlib.sha256(json.dumps(weekly_configuration(), sort_keys=True).encode()).hexdigest()


def session_schedule(start: str, end: str) -> list[dict[str, str]]:
    import pandas_market_calendars as mcal
    schedule = mcal.get_calendar("NYSE").schedule(start_date=start, end_date=end)
    return [{"date": str(day.date()), "open": row.market_open.isoformat(), "close": row.market_close.isoformat()}
            for day, row in schedule.iterrows()]


def completed_history(history: Sequence[Mapping[str, Any]], now: dt.datetime) -> list[dict[str, Any]]:
    """Do not train on today's partial daily bar or invent weekday/holiday rows."""
    if not history:
        return []
    days = {str(row["timestamp"])[:10]: row for row in history}
    sessions = session_schedule(min(days), now.date().isoformat())
    return [{"timestamp": f"{session['date']}T00:00:00Z", "close": float(days[session["date"]]["close"])}
            for session in sessions if session["date"] in days and dt.datetime.fromisoformat(session["close"]) <= now]


def build_weekly_forecast(history: Sequence[Mapping[str, Any]], *, now: dt.datetime | None = None,
                          execute: Callable[..., dict[str, Any]] | None = None) -> dict[str, Any] | None:
    from ensemble_forecasting.worker import execute_job
    now = now or dt.datetime.now(dt.timezone.utc)
    complete = completed_history(history, now)
    # Include the latest completed daily close. Partial sessions were excluded
    # above; no one-day lag and no intraday quote substitution.
    training = complete[-512:]
    if len(training) < 32:
        return None
    config = weekly_configuration()
    job = {"request": config, "runtime_mode": "production",
           "model_checkpoints": config["model_checkpoints"], "model_revisions": config["model_revisions"],
           "input": {"rows": [{"timestamp": r["timestamp"], "target": r["close"]} for r in training], "frequency": "1D", "timezone": "UTC"}}
    result = (execute or execute_job)(job, minimum_history_rows=32)
    if result.get("runtime", {}).get("mock") and execute is None:
        raise ValueError("mock_screener_output_forbidden")
    predictions = result["predictions"]
    if len(predictions) != 7:
        raise ValueError("weekly_screener_horizon_mismatch")
    sessions = session_schedule(predictions[0]["timestamp"][:10], predictions[-1]["timestamp"][:10])
    by_date = {r["date"]: r for r in sessions}
    rows = [{"date": p["timestamp"][:10], "timestamp": p["timestamp"],
             "session_open": by_date[p["timestamp"][:10]]["open"], "session_close": by_date[p["timestamp"][:10]]["close"],
             **{name: float(p["quantiles"][str(q).rstrip("0").rstrip(".")]) for name, q in zip(QUANTILE_NAMES, QUANTILES)}}
            for p in predictions]
    stats = {name: {"min": min(r[name] for r in rows), "max": max(r[name] for r in rows),
                    "avg": sum(r[name] for r in rows) / len(rows)} for name in QUANTILE_NAMES}
    return {**{q: rows[-1][q] for q in QUANTILE_NAMES}, "rows": rows, "quantile_stats": stats,
            "forecast_date": rows[-1]["date"], "last_forecast_update": now.isoformat(),
            "forecast_engine": ENGINE, "forecast_history_points": len(training),
            "forecast_config": config, "forecast_config_hash": configuration_hash(),
            "history_cutoff_at": training[-1]["timestamp"], "withheld_session": None,
            "daily_close_at": session_schedule(training[-1]["timestamp"][:10], training[-1]["timestamp"][:10])[0]["close"],
            "buy_price_target": rows[-1]["p99"], "signal_policy": "daily_close_above_first_p99_v2",
            "forecast_models": result["models"], "effective_weights_by_quantile": result["effective_weights_by_quantile"],
            "dataset_hash": result["dataset_hash"],
            "forecast_input_gzip": base64.b64encode(gzip.compress(json.dumps([[r["timestamp"], r["close"]] for r in training], separators=(",", ":")).encode(), mtime=0)).decode(),
            "forecast_provenance": {key: result.get(key) for key in ("result_hash", "prepared_series_hash", "runtime_seconds", "runtime", "warnings")},
            "general_bias": None, "unusual_p50_count": 0, "p10_signal_active": False}
