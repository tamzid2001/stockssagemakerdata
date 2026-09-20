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
        self._write("progress", payload)

    def complete(self, payload: Mapping[str, Any]) -> None:
        self._write("complete", payload)

    def fail(self, payload: Mapping[str, Any]) -> None:
        self._write("fail", payload)

    def _write(self, action: str, payload: Mapping[str, Any]) -> None:
        # Only idempotent status/result callbacks are retried. Claim and model
        # execution are never repeated because of a lost network response.
        for attempt in range(4):
            try:
                response = self.client.post(f"/api/internal/ensemble-forecasts/{self.job_id}/{action}", json=dict(payload))
                response.raise_for_status()
                return
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                transient = isinstance(exc, httpx.TransportError) or exc.response.status_code in {408, 429, 500, 502, 503, 504}
                if not transient or attempt == 3:
                    raise
                LOGGER.warning("worker callback retry: forecast_id=%s action=%s attempt=%s", self.job_id, action, attempt + 1)
                time.sleep(2 ** attempt)


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


def _recent_signal_search(
    job: Mapping[str, Any],
    *,
    progress: Callable[[Mapping[str, Any]], None],
    mock: bool,
    minimum_history_rows: int,
) -> dict[str, Any]:
    """Walk recent immutable cutoffs backward until the next close breaches P10/P90.

    The observation immediately after each cutoff is withheld from inference and
    used exactly once for classification. Search is bounded because every
    cutoff reruns the configured models rather than recycling future output.
    """
    request = dict(job.get("request") or {})
    if int(request.get("prediction_length") or 0) != 7:
        raise ValueError("RECENT_SIGNAL_SEARCH_REQUIRES_SEVEN_STEPS")
    requested = {round(float(value), 10) for value in request.get("quantiles") or []}
    if not {0.1, 0.5, 0.9}.issubset(requested):
        raise ValueError("RECENT_SIGNAL_SEARCH_REQUIRES_P10_P50_P90")
    try:
        maximum = int(request.get("search_max_cutoffs", 20))
    except (TypeError, ValueError) as exc:
        raise ValueError("RECENT_SIGNAL_SEARCH_LIMIT_INVALID") from exc
    if maximum < 1 or maximum > 30:
        raise ValueError("RECENT_SIGNAL_SEARCH_LIMIT_INVALID")
    source = dict(job.get("input") or {})
    rows = source.get("rows")
    if not isinstance(rows, list):
        raise ValueError("worker input rows are missing")
    enabled = [
        model_id for model_id, selection in dict(request.get("models") or {}).items()
        if isinstance(selection, Mapping) and selection.get("enabled") and float(selection.get("weight") or 0) > 0
    ]
    source_type = (job.get("source") or {}).get("type")
    source_minimum = 2 if source_type in {"prediction_market", "kalshi_perp"} else minimum_history_rows
    model_minimum = max(
        [source_minimum]
        + [int(MODEL_REGISTRY["models"].get(model_id, {}).get("minimumObservedContext") or 2) for model_id in enabled]
    )
    if len(rows) <= model_minimum:
        raise ValueError(f"RECENT_SIGNAL_SEARCH_REQUIRES_{model_minimum + 1}_ROWS")
    single_request = {key: value for key, value in request.items() if key not in {"analysis_mode", "search_max_cutoffs"}}
    examined = 0
    selected: dict[str, Any] | None = None
    selected_history: list[dict[str, Any]] = []
    selected_signal = "none"
    selected_actual: dict[str, Any] | None = None
    thresholds: dict[str, float] = {}
    total_started = time.monotonic()
    oldest_index = max(model_minimum - 1, len(rows) - 1 - maximum)
    for cutoff_index in range(len(rows) - 2, oldest_index - 1, -1):
        history = [dict(row) for row in rows[: cutoff_index + 1]]
        actual = dict(rows[cutoff_index + 1])
        candidate_job = {
            **dict(job),
            "request": single_request,
            "input": {**source, "rows": history},
        }
        candidate = execute_job(
            candidate_job,
            progress=progress,
            mock=mock,
            minimum_history_rows=model_minimum,
        )
        examined += 1
        first = dict(candidate["predictions"][0])
        quantiles = dict(first.get("quantiles") or {})
        lower, median, upper = (float(quantiles[key]) for key in ("0.1", "0.5", "0.9"))
        observed = float(actual["target"])
        signal = "buy" if observed < lower else "sell" if observed > upper else "none"
        selected, selected_history, selected_signal = candidate, history, signal
        selected_actual = {"timestamp": str(actual["timestamp"]), "price": observed}
        thresholds = {"p10": lower, "p50": median, "p90": upper}
        if signal != "none":
            break
    if selected is None or selected_actual is None:
        raise ValueError("RECENT_SIGNAL_SEARCH_NO_ELIGIBLE_CUTOFF")
    selected["recent_signal_search"] = {
        "status": "found" if selected_signal != "none" else "not_found",
        "signal": selected_signal,
        "cutoffs_examined": examined,
        "max_cutoffs": maximum,
        "history_cutoff_at": str(selected_history[-1]["timestamp"]),
        "history_row_count": len(selected_history),
        "next_observation": selected_actual,
        "thresholds": thresholds,
        "rule": "next observed close below P10 = buy; above P90 = sell; strict boundaries",
    }
    # Firestore result documents stay bounded; the count still proves which
    # prefix was used, while the chart receives the most recent 500 inputs.
    selected["selected_history"] = selected_history[-500:]
    selected["runtime_seconds"] = time.monotonic() - total_started
    selected["warnings"] = list(selected.get("warnings") or []) + [
        f"Recent signal search examined {examined} strictly out-of-sample cutoff(s); later observations were not passed to inference."
    ]
    digest_payload = {key: value for key, value in selected.items() if key != "result_hash"}
    selected["result_hash"] = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return selected


def execute_job(
    job: Mapping[str, Any],
    *,
    progress: Callable[[Mapping[str, Any]], None] | None = None,
    mock: bool = False,
    minimum_history_rows: int = 40,
    single_point_research: bool = False,
) -> dict[str, Any]:
    # Trusted Python caller option, deliberately not a client request field.
    if type(single_point_research) is not bool or type(minimum_history_rows) is not int or not (1 if single_point_research else 2) <= minimum_history_rows <= 10_000:
        raise ValueError("invalid minimum history rows")
    progress = progress or (lambda _payload: None)
    if (job.get("request") or {}).get("analysis_mode") == "recent_signal_search":
        if single_point_research:
            raise ValueError("RECENT_SIGNAL_SEARCH_CONFIGURATION_INVALID")
        return _recent_signal_search(
            job, progress=progress, mock=mock, minimum_history_rows=minimum_history_rows
        )
    request_payload = dict(job.get("request") or {})
    request_payload["model_checkpoints"] = dict(job.get("model_checkpoints") or {})
    request_payload["model_revisions"] = dict(job.get("model_revisions") or {})
    request_payload["runtime_mode"] = str(job.get("runtime_mode") or request_payload.get("runtime_mode") or "production")
    request_payload["max_quantiles"] = int(MODEL_REGISTRY["maxRequestedQuantiles"])
    request = ForecastRequest.from_dict(request_payload)
    if single_point_research and (minimum_history_rows != 1 or job.get('source') or
            request.prediction_length != 14 or request.frequency != '1min' or
            request.failure_policy != 'fail' or
            {m for m, s in request.models.items() if s.enabled} != {'granite', 'chronos', 'timesfm'}):
        raise ValueError('SINGLE_POINT_RESEARCH_CONFIGURATION_REQUIRED')
    validate_request_capabilities(request)
    source = dict(job.get("input") or {})
    # The authenticated API verifies provider identity, side and real bars before
    # setting this immutable source. This is not an arbitrary client min-rows flag.
    if (job.get("source") or {}).get("type") == "prediction_market":
        minimum_history_rows = 2
        if request.transform != "logit":
            raise ValueError("prediction-market jobs require bounded logit forecasting")
    elif (job.get("source") or {}).get("type") == "kalshi_perp":
        minimum_history_rows = 2
        if request.transform == "logit" or request.calendar != "NONE" or request.horizon_mode != "frequency_periods":
            raise ValueError("perpetual jobs require price transforms and continuous frequency periods")
    rows = source.get("rows")
    if not isinstance(rows, list):
        raise ValueError("worker input rows are missing")
    if single_point_research and len(rows) != 1:
        raise ValueError('EXACTLY_ONE_REAL_OBSERVATION_REQUIRED')
    series = prepare_series(
        rows,
        timestamp_column=str(source.get("timestamp_column") or "timestamp"),
        target_column=str(source.get("target_column") or "target"),
        transform=request.transform,
        frequency=str(source.get("frequency") or request.frequency),
        timezone=str(source.get("timezone") or "UTC"),
        maximum_rows=int(job.get("maximum_history_rows") or 10_000),
        minimum_rows=minimum_history_rows,
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
    warnings: list[str] = list((job.get("source") or {}).get("warnings") or [])
    if series.transform == "logit":
        warnings.append("Probability targets use logit (epsilon 1e-6), ensemble in transformed space, then inverse-logit; no extrapolated probability clipping.")
    if len(series.values) < 40:
        warnings.append("Very short history: numerical execution does not establish historical forecasting reliability.")
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
                    "checkpoint_revision": forecast.checkpoint_revision,
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
            LOGGER.warning("model execution failed: model=%s code=%s retryable=%s", model_id, failure["code"], failure["retryable"])
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
                    "checkpoint_revision": row.get("checkpoint_revision"),
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
    # A forecast job runs only the requested horizon. Historical validation is
    # an explicit offline operation, never a second inference pass here (even
    # when an older queued record carries an evaluation_policy).
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
