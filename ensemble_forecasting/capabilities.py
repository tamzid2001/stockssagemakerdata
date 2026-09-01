from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from .schemas import ForecastRequest, ModelId


def _registry_path() -> Path:
    override = os.environ.get("QUANTURA_ENSEMBLE_MODEL_REGISTRY", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parents[1] / "quantura_site" / "functions_explore" / "src" / "ensembleModelRegistry.json"


@lru_cache(maxsize=1)
def load_registry() -> dict[str, Any]:
    path = _registry_path()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload.get("models"), dict):
        raise RuntimeError("ensemble model registry is invalid")
    return payload


MODEL_REGISTRY = load_registry()


def env_true(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def timesfm_availability(runtime_mode: str) -> tuple[bool, str | None, bool]:
    access = env_true("TIMESFM_HF_ACCESS_APPROVED")
    commercial = env_true("TIMESFM_COMMERCIAL_LICENSED")
    evaluation = env_true("ALLOW_NONCOMMERCIAL_TIMESFM") and runtime_mode in {"development", "test"}
    if not access:
        return False, "access_not_approved", evaluation
    if runtime_mode == "production" and not commercial:
        return False, "commercial_license_required", False
    if not commercial and not evaluation:
        return False, "commercial_license_required", False
    return True, None, not commercial


def model_supports_quantile(model_id: ModelId, quantile: float) -> bool:
    support = MODEL_REGISTRY["models"][model_id]["quantileSupport"]
    if support["type"] in {"requested", "requested_verified_native_range"}:
        return True
    return float(support["minimum"]) <= quantile <= float(support["maximum"])


def validate_request_capabilities(request: ForecastRequest) -> None:
    for model_id, selection in request.models.items():
        if not selection.enabled:
            continue
        model = MODEL_REGISTRY["models"][model_id]
        if request.prediction_length > int(model["maxPredictionLength"]):
            raise ValueError(f"{model_id} supports at most {model['maxPredictionLength']} forecast steps")
        if request.context_length and request.context_length > int(model["maxContextLength"]):
            raise ValueError(f"{model_id} supports at most {model['maxContextLength']} context rows")
        if model_id == "timesfm":
            available, reason, _ = timesfm_availability(request.runtime_mode)
            if not available:
                raise ValueError(f"timesfm_unavailable:{reason}")
    for quantile in request.quantiles:
        supported = [
            model_id
            for model_id, selection in request.models.items()
            if selection.enabled and selection.weight > 0 and model_supports_quantile(model_id, quantile)
        ]
        if not supported:
            raise ValueError(f"no enabled positive-weight model supports quantile {quantile:g}")


def public_capabilities(runtime_mode: str = "production") -> dict[str, Any]:
    models = []
    for model_id, value in MODEL_REGISTRY["models"].items():
        available = True
        unavailable_reason = None
        evaluation_only = False
        if model_id == "timesfm":
            available, unavailable_reason, evaluation_only = timesfm_availability(runtime_mode)
        models.append(
            {
                "id": model_id,
                "name": value["name"],
                "checkpoint": value.get("checkpoint"),
                "available": available,
                "unavailable_reason": unavailable_reason,
                "evaluation_only": evaluation_only,
                "default_weight": value["defaultWeight"],
                "quantile_support": value["quantileSupport"],
                "max_prediction_length": value["maxPredictionLength"],
                "max_context_length": value["maxContextLength"],
                "default_device": value["defaultDevice"],
                "license": value.get("license"),
            }
        )
    return {
        "schema_version": MODEL_REGISTRY["schemaVersion"],
        "default_quantiles": MODEL_REGISTRY["defaultQuantiles"],
        "max_requested_quantiles": MODEL_REGISTRY["maxRequestedQuantiles"],
        "models": models,
    }
