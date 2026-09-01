from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import time
from dataclasses import replace
from typing import Any, Callable, Mapping

import httpx

from .adapters import ChronosAdapter, GraniteAdapter, ProphetAdapter, TimesFMAdapter, TotoAdapter
from .adapters.base import ForecastAdapter, ModelExecutionError, cleanup_memory
from .adapters.mock import MockAdapter
from .calendars import build_future_timestamps
from .capabilities import MODEL_REGISTRY, validate_request_capabilities
from .ensemble import build_ensemble
from .preprocessing import prepare_series
from .schemas import APPROVED_MODELS, ForecastRequest, ModelId

LOGGER = logging.getLogger("quantura.ensemble.worker")


class WorkerApi:
    def __init__(self, base_url: str, token: str, job_id: str):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.job_id = job_id
        self.client = httpx.Client(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {token}", "User-Agent": "Quantura-Ensemble-Worker/1.0"},
            timeout=60.0,
        )

    def claim(self) -> dict[str, Any]:
        response = self.client.post(f"/api/internal/ensemble-forecasts/{self.job_id}/claim")
        response.raise_for_status()
        return response.json()["data"]

    def progress(self, payload: Mapping[str, Any]) -> None:
        response = self.client.post(f"/api/internal/ensemble-forecasts/{self.job_id}/progress", json=dict(payload))
        response.raise_for_status()

    def complete(self, payload: Mapping[str, Any]) -> None:
        response = self.client.post(f"/api/internal/ensemble-forecasts/{self.job_id}/complete", json=dict(payload))
        response.raise_for_status()

    def fail(self, payload: Mapping[str, Any]) -> None:
        response = self.client.post(f"/api/internal/ensemble-forecasts/{self.job_id}/fail", json=dict(payload))
        response.raise_for_status()


def adapter_factory(model_id: ModelId, *, mock: bool = False) -> ForecastAdapter:
    if mock:
        return MockAdapter(model_id)
    factories: dict[ModelId, Callable[[], ForecastAdapter]] = {
        "prophet": ProphetAdapter,
        "toto": TotoAdapter,
        "granite": GraniteAdapter,
        "chronos": ChronosAdapter,
        "timesfm": TimesFMAdapter,
    }
    return factories[model_id]()


def execute_job(
    job: Mapping[str, Any],
    *,
    progress: Callable[[Mapping[str, Any]], None] | None = None,
    mock: bool = False,
) -> dict[str, Any]:
    progress = progress or (lambda _payload: None)
    request_payload = dict(job.get("request") or {})
    request_payload["model_checkpoints"] = dict(job.get("model_checkpoints") or {})
    request_payload["runtime_mode"] = str(job.get("runtime_mode") or request_payload.get("runtime_mode") or "production")
    request_payload["max_quantiles"] = int(MODEL_REGISTRY["maxRequestedQuantiles"])
    request = ForecastRequest.from_dict(request_payload)
    validate_request_capabilities(request)
    source = dict(job.get("input") or {})
    rows = source.get("rows")
    if not isinstance(rows, list):
        raise ValueError("worker input rows are missing")
    series = prepare_series(
        rows,
        timestamp_column=str(source.get("timestamp_column") or "timestamp"),
        target_column=str(source.get("target_column") or "target"),
        transform=request.transform,
        frequency=str(source.get("frequency") or request.frequency),
        timezone=str(source.get("timezone") or "UTC"),
        maximum_rows=int(job.get("maximum_history_rows") or 10_000),
    )
    # `dataset_hash` is the API's immutable snapshot identifier and includes
    # source/provider metadata. `series.dataset_hash` is a second, worker-side
    # hash of the normalized numerical series. They intentionally use distinct
    # domains and are persisted together instead of being compared as if they
    # were the same digest.
    snapshot_hash = str(job.get("dataset_hash") or series.dataset_hash)
    timestamps = build_future_timestamps(
        series.timestamps[-1],
        prediction_length=request.prediction_length,
        horizon_mode=request.horizon_mode,
        frequency=series.frequency,
        calendar=request.calendar,
    )
    if not timestamps:
        raise ValueError("forecast horizon produced no valid future timestamps")
    inference_request = request if len(timestamps) == request.prediction_length else replace(request, prediction_length=len(timestamps))
    enabled: list[ModelId] = [
        model_id for model_id in APPROVED_MODELS if inference_request.models[model_id].enabled and inference_request.models[model_id].weight > 0
    ]
    forecasts = {}
    model_runs: list[dict[str, Any]] = []
    warnings: list[str] = []
    failures: list[dict[str, Any]] = []
    total_started = time.monotonic()
    for index, model_id in enumerate(enabled):
        progress(
            {
                "status": "running",
                "completed_models": index,
                "total_models": len(enabled),
                "current_model": model_id,
            }
        )
        model_started = time.monotonic()
        try:
            forecast = adapter_factory(model_id, mock=mock).forecast(series, timestamps, inference_request)
            forecast.validate()
            forecasts[model_id] = forecast
            model_runs.append(
                {
                    "model": model_id,
                    "checkpoint": forecast.checkpoint,
                    "status": "completed",
                    "duration_seconds": forecast.duration_seconds,
                    "device": forecast.device,
                    "available_quantiles": list(forecast.available_quantiles),
                    "quantile_provenance": forecast.quantile_provenance,
                    "package_versions": forecast.package_versions,
                    "warnings": forecast.warnings,
                }
            )
            warnings.extend(forecast.warnings)
        except Exception as exc:
            failure = {
                "model": model_id,
                "code": exc.code if isinstance(exc, ModelExecutionError) else "MODEL_INFERENCE_FAILED",
                "retryable": bool(exc.retryable) if isinstance(exc, ModelExecutionError) else False,
                "duration_seconds": time.monotonic() - model_started,
            }
            failures.append(failure)
            model_runs.append({**failure, "status": "failed"})
            if inference_request.failure_policy == "fail":
                raise ModelExecutionError(model_id, failure["code"], retryable=failure["retryable"]) from exc
            warnings.append(f"{model_id} failed and was excluded under the explicit renormalize policy.")
        finally:
            cleanup_memory()
    ensemble = build_ensemble(inference_request, forecasts, timestamps, chosen_transform=series.transform)
    warnings.extend(ensemble.warnings)
    result = ensemble.to_dict()
    result.update(
        {
            "dataset_hash": snapshot_hash,
            "prepared_series_hash": series.dataset_hash,
            "requested_prediction_length": request.prediction_length,
            "source": job.get("source"),
            "models": [
                {
                    "id": row["model"],
                    "checkpoint": row.get("checkpoint"),
                    "status": row["status"],
                    "device": row.get("device"),
                    "duration_seconds": row.get("duration_seconds"),
                }
                for row in model_runs
            ],
            "warnings": warnings,
            "runtime_seconds": time.monotonic() - total_started,
            "runtime": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "mock": mock,
            },
            "failures": failures,
            "model_runs": model_runs,
        }
    )
    digest = hashlib.sha256(json.dumps(result, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    result["result_hash"] = digest
    progress(
        {
            "status": "running",
            "completed_models": len(enabled),
            "total_models": len(enabled),
            "current_model": None,
        }
    )
    return result


def run_remote_job(job_id: str, *, mock: bool = False) -> dict[str, Any]:
    base_url = os.environ.get("QUANTURA_WORKER_API_BASE", "https://quantura.studio").strip()
    token = os.environ.get("QUANTURA_ENSEMBLE_WORKER_TOKEN", "").strip()
    if not token:
        raise RuntimeError("QUANTURA_ENSEMBLE_WORKER_TOKEN is required")
    api = WorkerApi(base_url, token, job_id)
    job = api.claim()
    try:
        result = execute_job(job, progress=api.progress, mock=mock)
        api.complete(result)
        return result
    except Exception as exc:
        failure = {
            "code": exc.code if isinstance(exc, ModelExecutionError) else "FORECAST_JOB_FAILED",
            "model": exc.model if isinstance(exc, ModelExecutionError) else None,
            "retryable": bool(exc.retryable) if isinstance(exc, ModelExecutionError) else False,
        }
        try:
            api.fail(failure)
        except Exception:
            LOGGER.exception("failed to report job failure", extra={"forecast_id": job_id})
        raise
