from __future__ import annotations

import time

import numpy as np
import pandas as pd

from ..schemas import (
    ForecastRequest,
    ModelForecast,
    PreparedSeries,
    canonical_quantile_string,
)
from ..validation import coerce_quantile_horizon, monotonic_rearrangement
from .base import ForecastAdapter, ModelExecutionError, package_versions


class ProphetAdapter(ForecastAdapter):
    model_id = "prophet"

    def forecast(
        self,
        series: PreparedSeries,
        timestamps: tuple[str, ...],
        request: ForecastRequest,
    ) -> ModelForecast:
        started = time.monotonic()
        try:
            from prophet import Prophet
        except ImportError as exc:
            raise ModelExecutionError(
                self.model_id, "MODEL_DEPENDENCY_MISSING"
            ) from exc
        train_timestamps = pd.to_datetime(list(series.timestamps), utc=True).tz_convert(
            None
        )
        train = pd.DataFrame(
            {"ds": train_timestamps, "y": series.transformed_values.astype(np.float64)}
        )
        span_days = (train_timestamps[-1] - train_timestamps[0]).total_seconds() / 86400
        model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=span_days >= 14,
            yearly_seasonality=span_days >= 365,
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=10.0,
            uncertainty_samples=500,
            mcmc_samples=0,
        )
        if request.calendar in {"NYSE", "XNYS", "NASDAQ", "XNAS"}:
            model.add_country_holidays(country_name="US")
        if span_days >= 90:
            model.add_seasonality(name="monthly", period=30.5, fourier_order=5)
        fit_options = {}
        warnings = []
        if len(train) == 2:
            # Newton can drive sigma_obs to zero for a two-point exact line,
            # overflow Stan's GLM, or run indefinitely. Keep the real inputs
            # and Prophet posterior; use the converged BFGS optimizer instead.
            # Do not accept unconverged fits or fall back to the failing Newton
            # path. Longer histories retain their existing optimizer policy.
            model.stan_backend.set_options(newton_fallback=False)
            fit_options = {"algorithm": "BFGS", "seed": int(series.dataset_hash[:8], 16),
                           "timeout": 20, "require_converged": True}
            warnings.append("PROPHET_TWO_POINT_BFGS: two observations cannot establish calibrated uncertainty; short-history research only.")
        try:
            model.fit(train, **fit_options)
            future = pd.DataFrame(
                {"ds": pd.to_datetime(list(timestamps), utc=True).tz_convert(None)}
            )
            samples = np.asarray(
                model.predictive_samples(future)["yhat"], dtype=np.float64
            )
        except Exception as exc:
            raise ModelExecutionError(
                self.model_id, "MODEL_INFERENCE_FAILED", retryable=False
            ) from exc
        if samples.ndim != 2:
            raise ModelExecutionError(self.model_id, "INVALID_MODEL_OUTPUT")
        if (
            samples.shape[0] != request.prediction_length
            and samples.shape[1] == request.prediction_length
        ):
            samples = samples.T
        if samples.shape[0] != request.prediction_length:
            raise ModelExecutionError(self.model_id, "INVALID_MODEL_OUTPUT")
        matrix = np.quantile(samples, q=request.quantiles, axis=1)
        matrix = monotonic_rearrangement(
            coerce_quantile_horizon(
                matrix,
                quantile_count=len(request.quantiles),
                prediction_length=request.prediction_length,
                model_name="Prophet",
            )
        )
        return ModelForecast(
            model_id="prophet",
            checkpoint=None,
            prediction_length=request.prediction_length,
            requested_quantiles=request.quantiles,
            available_quantiles=request.quantiles,
            quantile_matrix=matrix,
            quantile_provenance={
                canonical_quantile_string(q): "posterior_predictive_sample"
                for q in request.quantiles
            },
            device="cpu",
            duration_seconds=time.monotonic() - started,
            package_versions=package_versions(["prophet", "numpy"]),
            warnings=warnings,
        )
