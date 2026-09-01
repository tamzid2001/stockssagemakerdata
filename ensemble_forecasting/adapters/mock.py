from __future__ import annotations

import numpy as np

from ..capabilities import MODEL_REGISTRY, model_supports_quantile
from ..schemas import ForecastRequest, ModelForecast, ModelId, PreparedSeries, canonical_quantile_string
from .base import ForecastAdapter


class MockAdapter(ForecastAdapter):
    """Deterministic CI adapter. Never selected outside explicit mock mode."""

    def __init__(self, model_id: ModelId, *, fail: bool = False):
        self.model_id = model_id
        self.fail = fail

    def forecast(self, series: PreparedSeries, timestamps: tuple[str, ...], request: ForecastRequest) -> ModelForecast:
        del timestamps
        if self.fail:
            raise RuntimeError(f"{self.model_id}:mock_failure")
        center = float(series.transformed_values[-1])
        matrix = np.full((len(request.quantiles), request.prediction_length), np.nan, dtype=np.float64)
        supported = []
        provenance = {}
        for q_index, quantile in enumerate(request.quantiles):
            if not model_supports_quantile(self.model_id, quantile):
                provenance[canonical_quantile_string(quantile)] = "unavailable_outside_native_range"
                continue
            supported.append(quantile)
            offset = (quantile - 0.5) * 0.08
            matrix[q_index] = center + offset + np.arange(1, request.prediction_length + 1) * 0.001
            provenance[canonical_quantile_string(quantile)] = "mock_native"
        result = ModelForecast(
            model_id=self.model_id,
            checkpoint=MODEL_REGISTRY["models"][self.model_id].get("checkpoint"),
            prediction_length=request.prediction_length,
            requested_quantiles=request.quantiles,
            available_quantiles=tuple(supported),
            quantile_matrix=matrix,
            quantile_provenance=provenance,
            device="mock",
            duration_seconds=0.0,
            package_versions={"mock": "1"},
        )
        result.validate()
        return result
