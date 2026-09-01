from __future__ import annotations

import time

import numpy as np

from ..capabilities import MODEL_REGISTRY
from ..schemas import ForecastRequest, ModelForecast, PreparedSeries, canonical_quantile_string
from ..validation import coerce_quantile_horizon, monotonic_rearrangement
from .base import ForecastAdapter, ModelExecutionError, cleanup_memory, hf_token, package_versions


class GraniteAdapter(ForecastAdapter):
    model_id = "granite"

    def forecast(self, series: PreparedSeries, timestamps: tuple[str, ...], request: ForecastRequest) -> ModelForecast:
        del timestamps
        started = time.monotonic()
        try:
            import torch
            from tsfm_public import PatchTSTFMForPrediction
        except ImportError as exc:
            raise ModelExecutionError(self.model_id, "MODEL_DEPENDENCY_MISSING") from exc
        model = None
        try:
            checkpoint = request.model_checkpoints.get("granite") or MODEL_REGISTRY["models"]["granite"]["checkpoint"]
            model = PatchTSTFMForPrediction.from_pretrained(
                checkpoint,
                token=hf_token(),
                low_cpu_mem_usage=True,
            ).to("cpu").eval()
            maximum = int(getattr(model.config, "context_length", MODEL_REGISTRY["models"]["granite"]["maxContextLength"]))
            context_length = min(request.context_length or 512, maximum, len(series.transformed_values))
            values = torch.tensor(series.transformed_values[-context_length:], dtype=torch.float32, device="cpu").unsqueeze(0)
            with torch.inference_mode():
                output = model(
                    past_values=values,
                    prediction_length=request.prediction_length,
                    quantile_levels=list(request.quantiles),
                )
            raw = output.quantile_outputs[0] if isinstance(output.quantile_outputs, (list, tuple)) else output.quantile_outputs
            if torch.is_tensor(raw):
                raw = raw.detach().float().cpu().numpy()
            matrix = monotonic_rearrangement(
                coerce_quantile_horizon(
                    raw,
                    quantile_count=len(request.quantiles),
                    prediction_length=request.prediction_length,
                    model_name="Granite",
                )
            )
            result = ModelForecast(
                model_id="granite",
                checkpoint=checkpoint,
                prediction_length=request.prediction_length,
                requested_quantiles=request.quantiles,
                available_quantiles=request.quantiles,
                quantile_matrix=matrix,
                quantile_provenance={canonical_quantile_string(q): "requested_model_quantile" for q in request.quantiles},
                device="cpu",
                duration_seconds=time.monotonic() - started,
                package_versions=package_versions(["granite-tsfm", "torch"]),
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
