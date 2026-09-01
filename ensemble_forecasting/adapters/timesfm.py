from __future__ import annotations

import time

import numpy as np

from ..capabilities import MODEL_REGISTRY, timesfm_availability
from ..schemas import ForecastRequest, ModelForecast, PreparedSeries
from ..validation import coerce_quantile_horizon, interpolate_native_quantiles
from .base import (
    ForecastAdapter,
    ModelExecutionError,
    cleanup_memory,
    device_name,
    is_cuda_oom,
    package_versions,
)

NATIVE_LEVELS = tuple(round(value / 10, 1) for value in range(1, 10))


class TimesFMAdapter(ForecastAdapter):
    model_id = "timesfm"

    def _run(self, context: np.ndarray, prediction_length: int, device: str, checkpoint: str) -> np.ndarray:
        from timesfm3 import ModelConfig, TimesFM3Evaluator

        evaluator = None
        try:
            evaluator = TimesFM3Evaluator(
                ModelConfig(checkpoint_path=checkpoint, per_core_batch_size=1, device=device)
            )
            outputs = list(
                evaluator.predict_batch(
                    [context],
                    horizon=prediction_length,
                    return_quantiles=True,
                    use_symmetric_averaging=False,
                )
            )
            if len(outputs) != 1:
                raise ModelExecutionError(self.model_id, "INVALID_MODEL_OUTPUT")
            return np.asarray(outputs[0].quantiles, dtype=np.float64)
        finally:
            if evaluator is not None:
                del evaluator
            cleanup_memory()

    def forecast(self, series: PreparedSeries, timestamps: tuple[str, ...], request: ForecastRequest) -> ModelForecast:
        del timestamps
        available, reason, evaluation_only = timesfm_availability(request.runtime_mode)
        if not available:
            raise ModelExecutionError(self.model_id, f"LICENSE_{str(reason).upper()}")
        started = time.monotonic()
        try:
            import timesfm3  # noqa: F401
        except ImportError as exc:
            raise ModelExecutionError(self.model_id, "MODEL_DEPENDENCY_MISSING") from exc
        context_length = min(
            request.context_length or 512,
            int(MODEL_REGISTRY["models"]["timesfm"]["maxContextLength"]),
            len(series.transformed_values),
        )
        context = series.transformed_values[-context_length:].astype(np.float32).copy()
        checkpoint = request.model_checkpoints.get("timesfm") or MODEL_REGISTRY["models"]["timesfm"]["checkpoint"]
        device = device_name("auto")
        try:
            raw = self._run(context, request.prediction_length, device, checkpoint)
        except RuntimeError as exc:
            if device != "cuda" or not is_cuda_oom(exc):
                raise ModelExecutionError(self.model_id, "MODEL_INFERENCE_FAILED", retryable=True) from exc
            device = "cpu"
            raw = self._run(context, request.prediction_length, device, checkpoint)
        native = coerce_quantile_horizon(
            raw,
            quantile_count=len(NATIVE_LEVELS),
            prediction_length=request.prediction_length,
            model_name="TimesFM 3.0",
        )
        matrix, supported, provenance = interpolate_native_quantiles(
            native,
            native_levels=NATIVE_LEVELS,
            requested_levels=request.quantiles,
        )
        warnings = ["TimesFM 3.0 is running under non-commercial evaluation terms."] if evaluation_only else []
        result = ModelForecast(
            model_id="timesfm",
            checkpoint=checkpoint,
            prediction_length=request.prediction_length,
            requested_quantiles=request.quantiles,
            available_quantiles=supported,
            quantile_matrix=matrix,
            quantile_provenance=provenance,
            device=device,
            duration_seconds=time.monotonic() - started,
            warnings=warnings,
            package_versions=package_versions(["timesfm"]),
        )
        result.validate()
        return result
