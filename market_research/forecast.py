"""Reuse the production five-adapter registry and per-quantile ensemble engine."""

from __future__ import annotations

import time
import numpy as np

from ensemble_forecasting.worker import execute_job
from ensemble_forecasting.capabilities import MODEL_REGISTRY
from .engine import QUANTILES, Quote, digest, iso


def forecast_window(
    window: list[Quote],
    horizon: int,
    models: tuple[str, ...] = ("prophet", "toto", "granite", "chronos"),
) -> dict:
    if (
        not models
        or len(set(models)) != len(models)
        or set(models) - set(MODEL_REGISTRY["models"])
    ):
        raise ValueError("unsupported_models")
    source = [
        {"timestamp": iso(q.timestamp), "target": q.ask, "observed": q.observed}
        for q in window
    ]
    # Epsilon is used only to make log-odds finite at the boundaries.
    logits = [
        {
            **r,
            "target": float(
                np.log(
                    np.clip(r["target"], 1e-6, 1 - 1e-6)
                    / (1 - np.clip(r["target"], 1e-6, 1 - 1e-6))
                )
            ),
        }
        for r in source
    ]
    configuration = {
        "prediction_length": horizon,
        "horizon_mode": "frequency_periods",
        "frequency": "1min",
        "calendar": "NONE",
        "transform": "none",
        "context_length": 500,
        "failure_policy": "renormalize",
        "quantiles": list(QUANTILES),
        "models": {m: {"enabled": True, "weight": 1} for m in models},
    }
    seed = int(digest(source)[:8], 16)
    np.random.seed(seed)
    started = time.monotonic()
    result = execute_job(
        {
            "request": configuration,
            "input": {"rows": logits, "frequency": "1min"},
            "runtime_mode": "production",
        }
    )
    if sum(m["status"] == "completed" for m in result["models"]) < 2:
        raise RuntimeError("INSUFFICIENT_ENSEMBLE_MEMBERS")
    rows = []
    for row in result["predictions"]:
        values = {}
        for q in QUANTILES:
            x = float(row["quantiles"][str(q)])
            values[str(q)] = (
                float(1 / (1 + np.exp(-x)))
                if x >= 0
                else float(np.exp(x) / (1 + np.exp(x)))
            )
        from .engine import stamp

        rows.append({"timestamp": stamp(row["timestamp"]), "quantiles": values})
    versions = [
        {
            k: run.get(k)
            for k in (
                "model",
                "checkpoint",
                "package_versions",
                "available_quantiles",
                "quantile_provenance",
                "status",
            )
        }
        for run in result["model_runs"]
    ]
    return {
        "forecast_id": digest(
            {
                "source": source,
                "config": configuration,
                "versions": versions,
                "rows": rows,
                "seed": seed,
            }
        ),
        "origin": window[-1].timestamp,
        "history_start": window[0].timestamp,
        "history_count": sum(q.observed for q in window),
        "model_context_steps": len(window),
        "imputed_context_steps": sum(not q.observed for q in window),
        "imputation": "causal_previous_quote_max_5_minutes_model_input_only",
        "history_requested_minutes": 500,
        "source_hash": digest(source),
        "seed": seed,
        "input_snapshot": source,
        "model_versions": versions,
        "configuration": configuration,
        "transform": "logit_inverse_logit",
        "epsilon": 1e-6,
        "rows": rows,
        "weights": result["effective_weights_by_quantile"],
        "models": result.get("models"),
        "warnings": result.get("warnings", []),
        "failures": result.get("failures", []),
        "duration_seconds": time.monotonic() - started,
    }
