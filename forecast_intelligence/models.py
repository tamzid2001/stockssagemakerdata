from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import NormalDist
from typing import Protocol

import numpy as np
import pandas as pd

from .calendar import ResolvedHorizon
from .types import (
    ForecastPoint,
    ModelForecast,
    ModelName,
    QuantileName,
    QuantileValue,
    validate_quantile_values,
)


class ModelUnavailable(RuntimeError):
    pass


class ForecastModel(Protocol):
    name: ModelName

    def forecast(
        self,
        frame: pd.DataFrame,
        *,
        ticker: str,
        as_of: str,
        horizon: ResolvedHorizon,
    ) -> ModelForecast: ...


def _as_of_history(frame: pd.DataFrame, as_of: str) -> pd.DataFrame:
    if "timestamp" not in frame or "close" not in frame:
        raise ValueError("timestamp and close columns are required")
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, errors="coerce")
    cutoff = pd.Timestamp(as_of)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    data = data[data["timestamp"].notna() & (data["timestamp"] <= cutoff)].sort_values("timestamp")
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    data = data[data["close"].notna() & (data["close"] > 0)]
    if len(data) < 40:
        raise ValueError("at least 40 point-in-time history rows are required")
    return data


def _build_forecast(
    *,
    model: ModelName,
    ticker: str,
    as_of: str,
    horizon: ResolvedHorizon,
    current_price: float,
    rows: list[dict[QuantileName, QuantileValue]],
    metadata: dict,
) -> ModelForecast:
    points: list[ForecastPoint] = []
    for timestamp, quantiles in zip(horizon.session_dates, rows):
        validate_quantile_values(quantiles)
        points.append(ForecastPoint(timestamp=timestamp, quantiles=quantiles))
    return ModelForecast(
        model=model,
        as_of=pd.Timestamp(as_of).isoformat(),
        target_date=horizon.target_date,
        horizon=horizon.label,
        sessions=horizon.sessions,
        current_price=current_price,
        points=points,
        metadata=metadata,
    )


@dataclass
class StatisticalBaselineModel:
    """Explicit baseline; never presented as Prophet or Chronos."""

    name: ModelName = ModelName.BASELINE
    lookback_returns: int = 252

    def forecast(self, frame: pd.DataFrame, *, ticker: str, as_of: str, horizon: ResolvedHorizon) -> ModelForecast:
        data = _as_of_history(frame, as_of)
        close = data["close"].to_numpy(dtype=float)
        returns = np.diff(np.log(close))[-self.lookback_returns :]
        drift = float(np.mean(returns))
        volatility = max(float(np.std(returns, ddof=0)), 1e-6)
        current = float(close[-1])
        normal = NormalDist()
        rows: list[dict[QuantileName, QuantileValue]] = []
        for step in range(1, horizon.sessions + 1):
            mean_log = math.log(current) + drift * step
            sigma = volatility * math.sqrt(step)
            values = {
                name: QuantileValue(
                    value=float(math.exp(mean_log + normal.inv_cdf(level) * sigma)),
                    provenance="statistical_baseline",
                    native_level=level,
                )
                for name, level in {
                    QuantileName.P1: 0.01,
                    QuantileName.P25: 0.25,
                    QuantileName.P50: 0.50,
                    QuantileName.P75: 0.75,
                    QuantileName.P99: 0.99,
                }.items()
            }
            rows.append(values)
        return _build_forecast(
            model=self.name,
            ticker=ticker,
            as_of=as_of,
            horizon=horizon,
            current_price=current,
            rows=rows,
            metadata={"drift": drift, "volatility": volatility, "identity": "baseline_not_prophet"},
        )


@dataclass
class ProphetModel:
    name: ModelName = ModelName.PROPHET
    uncertainty_samples: int = 1000
    changepoint_prior_scale: float = 0.05
    seasonality_prior_scale: float = 10.0

    def forecast(self, frame: pd.DataFrame, *, ticker: str, as_of: str, horizon: ResolvedHorizon) -> ModelForecast:
        try:
            from prophet import Prophet
        except ImportError as exc:
            raise ModelUnavailable("Prophet is not installed") from exc

        data = _as_of_history(frame, as_of)
        train = pd.DataFrame(
            {
                "ds": data["timestamp"].dt.tz_convert(None),
                "y": np.log(data["close"].to_numpy(dtype=float)),
            }
        )
        model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=True,
            yearly_seasonality=len(train) >= 252,
            changepoint_prior_scale=self.changepoint_prior_scale,
            seasonality_prior_scale=self.seasonality_prior_scale,
            uncertainty_samples=self.uncertainty_samples,
            interval_width=0.98,
        )
        model.add_country_holidays(country_name="US")
        if len(train) >= 90:
            model.add_seasonality(name="monthly", period=30.5, fourier_order=5)
        model.fit(train)
        future = pd.DataFrame({"ds": pd.to_datetime(list(horizon.session_dates))})
        samples = model.predictive_samples(future).get("yhat")
        if samples is None:
            raise ModelUnavailable("Prophet did not return posterior predictive yhat samples")
        matrix = np.asarray(samples, dtype=float)
        if matrix.ndim != 2 or matrix.shape[0] != horizon.sessions:
            raise ModelUnavailable("Prophet posterior predictive sample shape is invalid")
        levels = [0.01, 0.25, 0.50, 0.75, 0.99]
        sample_quantiles = np.quantile(matrix, levels, axis=1).T
        rows: list[dict[QuantileName, QuantileValue]] = []
        for row in sample_quantiles:
            values = {
                name: QuantileValue(
                    value=max(1e-8, float(math.exp(value))),
                    provenance="prophet_posterior_predictive",
                    native_level=level,
                )
                for name, level, value in zip(QuantileName, levels, row)
            }
            rows.append(values)
        return _build_forecast(
            model=self.name,
            ticker=ticker,
            as_of=as_of,
            horizon=horizon,
            current_price=float(data["close"].iloc[-1]),
            rows=rows,
            metadata={
                "method": "log_price_prophet_posterior_predictive_samples",
                "uncertainty_samples": self.uncertainty_samples,
                "training_end": data["timestamp"].iloc[-1].isoformat(),
                "training_rows": int(len(data)),
            },
        )


def _empirical_tail_offsets(close: np.ndarray, step: int) -> tuple[float, float]:
    log_close = np.log(close)
    if len(log_close) <= step + 10:
        daily = np.diff(log_close)
        scale = max(float(np.std(daily, ddof=0)), 1e-6) * math.sqrt(step)
        return NormalDist().inv_cdf(0.01) * scale, NormalDist().inv_cdf(0.99) * scale
    returns = log_close[step:] - log_close[:-step]
    median = float(np.quantile(returns, 0.50))
    return float(np.quantile(returns, 0.01) - median), float(np.quantile(returns, 0.99) - median)


@dataclass
class Chronos2Model:
    name: ModelName = ModelName.CHRONOS
    checkpoint: str = "amazon/chronos-2"
    device_map: str = "cpu"
    finetune_mode: str = "none"
    pipeline: object | None = None

    def _load(self):
        if self.pipeline is not None:
            return self.pipeline
        try:
            from chronos import Chronos2Pipeline
        except ImportError as exc:
            raise ModelUnavailable("chronos-forecasting is not installed") from exc
        self.pipeline = Chronos2Pipeline.from_pretrained(self.checkpoint, device_map=self.device_map)
        return self.pipeline

    def forecast(self, frame: pd.DataFrame, *, ticker: str, as_of: str, horizon: ResolvedHorizon) -> ModelForecast:
        if self.finetune_mode not in {"none", "lora", "full"}:
            raise ValueError("chronos_finetune_mode must be none, lora, or full")
        data = _as_of_history(frame, as_of)
        pipe = self._load()
        context = pd.DataFrame(
            {
                "item_id": ticker,
                "timestamp": data["timestamp"].dt.tz_convert(None),
                "target": data["close"].to_numpy(dtype=float),
            }
        )
        # Only deterministic calendar fields are supplied into the future. Price,
        # volume, volatility, RSI, and MACD remain past-only and are never filled.
        context["day_of_week"] = context["timestamp"].dt.dayofweek.astype(float)
        future = pd.DataFrame(
            {
                "item_id": ticker,
                "timestamp": pd.to_datetime(list(horizon.session_dates)),
                "day_of_week": pd.to_datetime(list(horizon.session_dates)).dayofweek.astype(float),
            }
        )
        if self.finetune_mode != "none":
            pipe = pipe.fit(context, prediction_length=horizon.sessions, finetune_mode=self.finetune_mode)
        prediction = pipe.predict_df(
            context,
            future_df=future,
            prediction_length=horizon.sessions,
            quantile_levels=[0.25, 0.50, 0.75],
            id_column="item_id",
            timestamp_column="timestamp",
            target="target",
            validate_inputs=True,
        )
        close = data["close"].to_numpy(dtype=float)
        rows: list[dict[QuantileName, QuantileValue]] = []
        for index, predicted in prediction.reset_index(drop=True).iterrows():
            p25 = float(predicted["0.25"])
            p50 = float(predicted["0.5"])
            p75 = float(predicted["0.75"])
            low_offset, high_offset = _empirical_tail_offsets(close, index + 1)
            p1 = min(p25, p50 * math.exp(low_offset))
            p99 = max(p75, p50 * math.exp(high_offset))
            values = {
                QuantileName.P1: QuantileValue(p1, "chronos_external_empirical_tail", 0.01, True),
                QuantileName.P25: QuantileValue(p25, "chronos_interpolated_trained_range", 0.25),
                QuantileName.P50: QuantileValue(p50, "chronos_native", 0.50),
                QuantileName.P75: QuantileValue(p75, "chronos_interpolated_trained_range", 0.75),
                QuantileName.P99: QuantileValue(p99, "chronos_external_empirical_tail", 0.99, True),
            }
            rows.append(values)
        return _build_forecast(
            model=self.name,
            ticker=ticker,
            as_of=as_of,
            horizon=horizon,
            current_price=float(close[-1]),
            rows=rows,
            metadata={
                "checkpoint": self.checkpoint,
                "finetune_mode": self.finetune_mode,
                "native_quantile_range": [0.1, 0.9],
                "requested_central_quantiles": [0.25, 0.5, 0.75],
                "tails": "point_in_time_empirical_log_return_calibration",
                "future_covariates": ["day_of_week"],
                "past_only_covariates": ["target"],
                "training_end": data["timestamp"].iloc[-1].isoformat(),
            },
        )
