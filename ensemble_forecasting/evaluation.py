"""One bounded chronological holdout, separate from the future forecast.

This is historical validation, not training fit or a walk-forward backtest.
Explicit offline utility only. Normal forecast jobs never call this module.
Research collectors/paper traders do not implicitly incur a second model pass.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

import numpy as np
import pandas as pd

from .capabilities import MODEL_REGISTRY
from .schemas import ForecastRequest, PreparedSeries, canonical_quantile_string

HISTORICAL_VALIDATION_POLICY = "chronological_holdout_v1"


def score_predictions(predictions: list[dict], actuals: Mapping[str, float], quantiles: tuple[float, ...],
                      key: Callable[[str, bool], str] | None = None) -> dict[str, Any]:
    """Exact timestamp/session matches only; never fill missing outcomes."""
    key = key or (lambda stamp, _actual: pd.Timestamp(stamp).isoformat())
    observed = {key(stamp, True): value for stamp, value in actuals.items()}
    matched = [(row, observed[key(row["timestamp"], False)]) for row in predictions
               if key(row["timestamp"], False) in observed]
    median = [(float(row["quantiles"]["0.5"]), value) for row, value in matched if "0.5" in row["quantiles"]]
    errors = np.array([pred - actual for pred, actual in median], dtype=np.float64)
    smape = [2 * abs(pred - actual) / (abs(pred) + abs(actual)) if abs(pred) + abs(actual) else 0.
             for pred, actual in median]
    denominator = sum(abs(actual) for _, actual in matched)
    losses = []
    for q in quantiles:
        residuals = [actual - float(row["quantiles"][canonical_quantile_string(q)]) for row, actual in matched]
        loss = 2 * sum(q * error if error >= 0 else (q - 1) * error for error in residuals)
        losses.append({"q": q, "count": len(matched), "wql": loss / denominator if denominator else None})
    return {"count": len(matched), "point_count": len(median),
            "mae": float(np.mean(np.abs(errors))) if median else None,
            "rmse": float(np.sqrt(np.mean(errors ** 2))) if median else None,
            "smape": float(np.mean(smape)) if median else None,
            "average_wql": float(np.mean([row["wql"] for row in losses])) if denominator else None,
            "quantiles": losses}


def evaluate_history(job: Mapping[str, Any], series: PreparedSeries, request: ForecastRequest,
                     *, execute: Callable[..., dict], progress: Callable, mock: bool) -> dict[str, Any]:
    enabled = [name for name, selection in request.models.items() if selection.enabled and selection.weight > 0]
    minimum = max(2, *(int(MODEL_REGISTRY["models"][name].get("minimumObservedContext", 2)) for name in enabled))
    # Never consume most of a short series; never run a huge implicit backtest.
    holdout = min(30, request.prediction_length, max(1, len(series.values) // 5), len(series.values) - minimum)
    report: dict[str, Any] = {"policy": HISTORICAL_VALIDATION_POLICY, "method": "chronological_holdout",
                              "status": "insufficient_history", "minimum_training_rows": minimum,
                              "requested_models": enabled, "metrics": None}
    if holdout < 1:
        return report
    split = len(series.values) - holdout
    training = [{"timestamp": stamp, "target": float(value)}
                for stamp, value in zip(series.timestamps[:split], series.values[:split])]
    actuals = dict(zip(series.timestamps[split:], map(float, series.values[split:])))
    report.update(training_rows=split, holdout_rows=holdout, training_end_at=series.timestamps[split - 1],
                  validation_start_at=series.timestamps[split], validation_end_at=series.timestamps[-1],
                  frequency=series.frequency, evaluated_at=datetime.now(timezone.utc).isoformat(),
                  actuals_hash=hashlib.sha256(json.dumps(actuals, sort_keys=True).encode()).hexdigest())
    # Validation derives transforms/frequency from the training prefix, never from
    # held-out target values. Copy the original model configuration and pins.
    evaluation_request = dict(job["request"])
    evaluation_request.update(prediction_length=holdout, failure_policy="fail")
    if request.horizon_mode == "calendar_days":
        evaluation_request["horizon_mode"] = "trading_sessions"
    evaluation_job = {**job, "request": evaluation_request,
                      "input": {**dict(job["input"]), "rows": training, "timestamp_column": "timestamp", "target_column": "target"}}
    evaluation_job.pop("evaluation_policy", None)  # no recursion
    evaluation_job.pop("dataset_hash", None)  # prefix gets its own immutable hash
    try:
        result = execute(evaluation_job, progress=lambda payload: progress({**payload, "phase": "historical_validation"}),
                         mock=mock, minimum_history_rows=minimum)
        source = dict(job.get("source") or {})

        def time_key(stamp: str, actual: bool) -> str:
            # NYSE predictions are session-date labels; provider daily bars may
            # instead be labeled at midnight/open in the exchange timezone.
            if source.get("type") == "ticker" and series.frequency == "1D" and request.horizon_mode != "frequency_periods":
                if not actual or source.get("daily_timestamp_convention") == "session_date":
                    return stamp[:10]
                return str(pd.Timestamp(stamp).tz_convert(source.get("exchange_timezone") or "America/New_York").date())
            return pd.Timestamp(stamp).isoformat()

        scores = score_predictions(result["predictions"], actuals, request.quantiles, time_key)
        json.dumps(scores, allow_nan=False)
        report.update(status="completed" if scores["count"] else "no_matching_outcomes", metrics=scores,
                      prediction_length=result["prediction_length"], transform=result["transform"],
                      prepared_series_hash=result["prepared_series_hash"], result_hash=result["result_hash"],
                      effective_weights_by_quantile=result["effective_weights_by_quantile"],
                      model_runs=result["model_runs"], runtime_seconds=result["runtime_seconds"])
        # Small, final-ensemble-only evidence; server persists but does not expose
        # it in the normal public result, which remains the future forecast.
        report["evidence"] = {"predictions": result["predictions"], "actuals": actuals}
    except Exception:
        # Validation failing must not destroy an otherwise completed forecast.
        # Do not silently replace it with a reduced-model or in-sample score.
        report.update(status="failed", error_code="HISTORICAL_VALIDATION_FAILED")
        logging.getLogger("quantura.ensemble.worker").warning("historical validation failed: code=HISTORICAL_VALIDATION_FAILED")
    return report
