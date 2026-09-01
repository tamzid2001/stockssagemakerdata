from __future__ import annotations

import time

import numpy as np

from ..capabilities import MODEL_REGISTRY
from ..schemas import ForecastRequest, ModelForecast, PreparedSeries
from ..validation import coerce_quantile_horizon, interpolate_native_quantiles
from .base import ForecastAdapter, ModelExecutionError, cleanup_memory, device_name, hf_token, package_versions

NATIVE_LEVELS = tuple(round(value / 10, 1) for value in range(1, 10))


class TotoAdapter(ForecastAdapter):
    model_id = "toto"

    def forecast(self, series: PreparedSeries, timestamps: tuple[str, ...], request: ForecastRequest) -> ModelForecast:
        del timestamps
        started = time.monotonic()
        try:
            import torch
            from toto2 import Toto2Model
        except ImportError as exc:
            raise ModelExecutionError(self.model_id, "MODEL_DEPENDENCY_MISSING") from exc
        device = device_name("auto")
        model = None
        try:
            checkpoint = request.model_checkpoints.get("toto") or MODEL_REGISTRY["models"]["toto"]["checkpoint"]
            model = Toto2Model.from_pretrained(checkpoint, token=hf_token()).to(device).eval()
            context_length = min(
                request.context_length or 512,
                int(MODEL_REGISTRY["models"]["toto"]["maxContextLength"]),
                len(series.transformed_values),
            )
            target = torch.tensor(series.transformed_values[-context_length:], dtype=torch.float32, device=device).view(1, 1, -1)
            mask = torch.ones_like(target, dtype=torch.bool)
            series_ids = torch.zeros((1, 1), dtype=torch.long, device=device)
            with torch.inference_mode():
                output = model.forecast(
                    {"target": target, "target_mask": mask, "series_ids": series_ids},
                    horizon=request.prediction_length,
                    decode_block_size=None,
                    has_missing_values=True,
                )
            native = coerce_quantile_horizon(
                output.detach().float().cpu().numpy(),
                quantile_count=len(NATIVE_LEVELS),
                prediction_length=request.prediction_length,
                model_name="Toto",
            )
            matrix, available, provenance = interpolate_native_quantiles(
                native,
                native_levels=NATIVE_LEVELS,
                requested_levels=request.quantiles,
            )
            result = ModelForecast(
                model_id="toto",
                checkpoint=checkpoint,
                prediction_length=request.prediction_length,
                requested_quantiles=request.quantiles,
                available_quantiles=available,
                quantile_matrix=matrix,
                quantile_provenance=provenance,
                device=device,
                duration_seconds=time.monotonic() - started,
                package_versions=package_versions(["toto-2", "torch"]),
            )
            result.validate()
            return result
        except ModelExecutionError:
            raise
        except Exception as exc:
            raise ModelExecutionError(self.model_id, "MODEL_INFERENCE_FAILED", retryable=True) from exc
        finally:
            if model is not None:
                del model
            cleanup_memory()
