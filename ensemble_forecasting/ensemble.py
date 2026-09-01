from __future__ import annotations

from typing import Mapping

import numpy as np

from .capabilities import model_supports_quantile
from .schemas import EnsembleResult, ForecastRequest, ModelForecast, canonical_quantile_string
from .validation import monotonic_rearrangement, validate_monotonic


def effective_weights_by_quantile(
    request: ForecastRequest,
    forecasts: Mapping[str, ModelForecast] | None = None,
) -> dict[str, dict[str, float]]:
    output: dict[str, dict[str, float]] = {}
    for quantile in request.quantiles:
        available = []
        for model_id, selection in request.models.items():
            if not selection.enabled or selection.weight <= 0 or not model_supports_quantile(model_id, quantile):
                continue
            if forecasts is not None:
                forecast = forecasts.get(model_id)
                if forecast is None or quantile not in forecast.available_quantiles:
                    continue
            available.append((model_id, selection.weight))
        total = sum(weight for _, weight in available)
        if total <= 0:
            raise ValueError(f"no enabled positive-weight model supports quantile {quantile:g}")
        output[canonical_quantile_string(quantile)] = {
            model_id: weight / total for model_id, weight in available
        }
    return output


def build_ensemble(
    request: ForecastRequest,
    forecasts: Mapping[str, ModelForecast],
    timestamps: tuple[str, ...],
    *,
    chosen_transform: str,
) -> EnsembleResult:
    weights = effective_weights_by_quantile(request, forecasts)
    combined = np.empty((len(request.quantiles), request.prediction_length), dtype=np.float64)
    for q_index, quantile in enumerate(request.quantiles):
        row = np.zeros(request.prediction_length, dtype=np.float64)
        for model_id, weight in weights[canonical_quantile_string(quantile)].items():
            model = forecasts[model_id]
            row += weight * model.quantile_matrix[q_index]
        combined[q_index] = row
    repaired_transformed = monotonic_rearrangement(combined)
    validate_monotonic(repaired_transformed)
    warnings = []
    if not np.array_equal(repaired_transformed, combined):
        warnings.append("Quantile crossing was repaired with deterministic monotonic rearrangement.")
    if chosen_transform == "log":
        repaired = np.exp(repaired_transformed)
    elif chosen_transform == "none":
        repaired = repaired_transformed
    else:
        raise ValueError("chosen_transform must be log or none")
    if not np.isfinite(repaired).all():
        raise ValueError("inverse-transformed ensemble contains NaN/inf")
    return EnsembleResult(
        timestamps=timestamps,
        quantiles=request.quantiles,
        quantile_matrix=repaired,
        effective_weights=weights,
        transform=chosen_transform,
        warnings=tuple(warnings),
    )
