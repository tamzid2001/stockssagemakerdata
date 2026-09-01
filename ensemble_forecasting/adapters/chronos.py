from __future__ import annotations

import time

import numpy as np

from ..capabilities import MODEL_REGISTRY
from ..schemas import ForecastRequest, ModelForecast, PreparedSeries, canonical_quantile_string
from ..validation import coerce_quantile_horizon, monotonic_rearrangement
from .base import ForecastAdapter, ModelExecutionError, cleanup_memory, device_name, hf_token, package_versions


class ChronosAdapter(ForecastAdapter):
    model_id = "chronos"

    def forecast(self, series: PreparedSeries, timestamps: tuple[str, ...], request: ForecastRequest) -> ModelForecast:
        del timestamps
        started = time.monotonic()
        try:
            import torch
            from chronos import Chronos2Pipeline
        except ImportError as exc:
            raise ModelExecutionError(self.model_id, "MODEL_DEPENDENCY_MISSING") from exc
        pipeline = None
        try:
            checkpoint = request.model_checkpoints.get("chronos") or MODEL_REGISTRY["models"]["chronos"]["checkpoint"]
            device = device_name("auto")
            pipeline = Chronos2Pipeline.from_pretrained(checkpoint, device_map=device, token=hf_token())
            trained = tuple(float(value) for value in getattr(pipeline, "quantiles", []))
            if not trained:
                raise ModelExecutionError(self.model_id, "MODEL_CAPABILITY_UNAVAILABLE")
            minimum, maximum = min(trained), max(trained)
            unsupported = [value for value in request.quantiles if value < minimum or value > maximum]
            if unsupported:
                raise ModelExecutionError(self.model_id, "REQUESTED_QUANTILE_OUTSIDE_TRAINED_RANGE")
            context_length = min(
                request.context_length or 512,
                int(getattr(pipeline, "model_context_length", MODEL_REGISTRY["models"]["chronos"]["maxContextLength"])),
                len(series.transformed_values),
            )
            context = torch.tensor(series.transformed_values[-context_length:], dtype=torch.float32)
            with torch.inference_mode():
                quantiles, _point = pipeline.predict_quantiles(
                    inputs=[context],
                    prediction_length=request.prediction_length,
                    quantile_levels=list(request.quantiles),
                    batch_size=1,
                    context_length=context_length,
                )
            if len(quantiles) != 1:
                raise ModelExecutionError(self.model_id, "INVALID_MODEL_OUTPUT")
            raw = quantiles[0]
            if torch.is_tensor(raw):
                raw = raw.detach().float().cpu().numpy()
            matrix = monotonic_rearrangement(
                coerce_quantile_horizon(
                    raw,
                    quantile_count=len(request.quantiles),
                    prediction_length=request.prediction_length,
                    model_name="Chronos-2",
                )
            )
            provenance = {
                canonical_quantile_string(q): "native" if q in trained else "interpolated_inside_trained_range"
                for q in request.quantiles
            }
            result = ModelForecast(
                model_id="chronos",
                checkpoint=checkpoint,
                prediction_length=request.prediction_length,
                requested_quantiles=request.quantiles,
                available_quantiles=request.quantiles,
                quantile_matrix=matrix,
                quantile_provenance=provenance,
                device=device,
                duration_seconds=time.monotonic() - started,
                package_versions=package_versions(["chronos-forecasting", "torch"]),
            )
            result.validate()
            return result
        except ModelExecutionError:
            raise
        except Exception as exc:
            raise ModelExecutionError(self.model_id, "MODEL_INFERENCE_FAILED", retryable=True) from exc
        finally:
            if pipeline is not None:
                del pipeline
            cleanup_memory()
