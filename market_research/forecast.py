"""Reuse the production five-adapter registry and per-quantile ensemble engine."""

from __future__ import annotations

import time
import logging
import numpy as np

from ensemble_forecasting.worker import execute_job
from ensemble_forecasting.capabilities import MODEL_REGISTRY, timesfm_availability
from .engine import QUANTILES, Quote, digest, iso, history_quality


def default_research_models() -> tuple[str, ...]:
    models = ("prophet", "toto", "granite", "chronos")
    return (*models, "timesfm") if timesfm_availability("production")[0] else models


def forecast_window(
    window: list[Quote],
    horizon: int,
    models: tuple[str, ...] | None = None,
    quantiles: tuple[float, ...] = QUANTILES,
    *, single_point_research: bool = False, failure_policy: str | None = None,
    btc_two_point_research: bool = False,
) -> dict:
    models = default_research_models() if models is None else models
    if failure_policy not in (None, 'fail', 'renormalize'):
        raise ValueError('INVALID_RESEARCH_FAILURE_POLICY')
    if single_point_research and failure_policy == 'renormalize':
        raise ValueError('SINGLE_POINT_REQUIRES_STRICT_MODELS')
    if (
        not models
        or len(set(models)) != len(models)
        or set(models) - set(MODEL_REGISTRY["models"])
    ):
        raise ValueError("unsupported_models")
    if single_point_research and (len(window) != 1 or horizon != 14 or set(models) != {'granite','chronos','timesfm'}):
        raise ValueError('SINGLE_POINT_RESEARCH_CONFIGURATION_REQUIRED')
    if btc_two_point_research and (single_point_research or len(window)!=2 or horizon!=13
            or window[1].timestamp-window[0].timestamp!=60
            or not set(models)<= {'prophet','granite','chronos','timesfm'}):
        raise ValueError('BTC_TWO_POINT_RESEARCH_CONFIGURATION_REQUIRED')
    if len(window) < (1 if single_point_research else 2) or any(not q.observed for q in window):
        raise ValueError("TWO_GENUINE_OBSERVATIONS_REQUIRED")
    if any(b.timestamp <= a.timestamp for a, b in zip(window, window[1:])):
        raise ValueError("NON_CHRONOLOGICAL_CONTEXT")
    quality = history_quality(window)
    if quality["forecast_blocked"] and not btc_two_point_research:
        raise ValueError("HISTORY_FLAT_WINDOW")
    gaps = sum(b.timestamp - a.timestamp > 60 for a, b in zip(window, window[1:]))
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
        "failure_policy": "fail" if single_point_research else (failure_policy or "renormalize"),
        "quantiles": list(quantiles),
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
        },
        minimum_history_rows=1 if single_point_research else 2,
        **({'single_point_research': True} if single_point_research else {}),
        progress=lambda progress: logging.getLogger("quantura.research").warning(
            "ensemble_progress model=%s completed=%s total=%s observed_rows=%s",
            progress.get("current_model"),
            progress.get("completed_models"),
            progress.get("total_models"),
            len(window),
        ),
    )
    if sum(m["status"] == "completed" for m in result["models"]) < 2:
        raise RuntimeError("INSUFFICIENT_ENSEMBLE_MEMBERS")
    if configuration['failure_policy'] == 'fail' and (
        len(result['models']) != len(models) or any(m['status'] != 'completed' for m in result['models'])
    ):
        raise RuntimeError('STRICT_RESEARCH_ENSEMBLE_INCOMPLETE')
    rows = []
    for row in result["predictions"]:
        values = {}
        for q in quantiles:
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
        "imputation": "none_observed_values_only",
        "history_gap_count": gaps,
        "history_elapsed_minutes": (window[-1].timestamp - window[0].timestamp) / 60,
        "history_requested_minutes": 500,
        "source_hash": digest(source),
        "seed": seed,
        "input_snapshot": source,
        "model_versions": versions,
        "history_quality": quality,
        "btc_two_point_research": btc_two_point_research,
        "configuration": configuration,
        "transform": "logit_inverse_logit",
        "epsilon": 1e-6,
        "rows": rows,
        "weights": result["effective_weights_by_quantile"],
        "models": result.get("models"),
        "warnings": result.get("warnings", [])
        + (["UNCHANGED_TWO_POINT_CONTEXT: two genuine consecutive BTC minute quotes were equal; no synthetic variation added; reliability unvalidated."] if btc_two_point_research and quality['forecast_blocked'] else [])
        + (["IRREGULAR_HISTORY: missing minutes retained; foundation-model observation steps are not equal elapsed time. Reliability requires validation."] if gaps else [])
        + (
            [
                f"SHORT_HISTORY: only {len(window)} genuine minute observations; forecast reliability is unvalidated."
            ]
            if len(window) < 500
            else []
        ),
        "failures": result.get("failures", []),
        "duration_seconds": time.monotonic() - started,
    }
