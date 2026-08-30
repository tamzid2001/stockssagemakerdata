from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

import numpy as np

from .types import ForecastMetrics, QuantileName, QUANTILE_LEVELS


@dataclass(frozen=True)
class EvaluationRow:
    actual: float
    origin_price: float
    quantiles: Mapping[QuantileName, float]


def pinball_loss(actual: float, forecast: float, quantile: float) -> float:
    error = actual - forecast
    return max(quantile * error, (quantile - 1.0) * error)


def evaluate_forecasts(rows: Iterable[EvaluationRow], *, seasonal_period: int = 1) -> ForecastMetrics:
    items = list(rows)
    if not items:
        return ForecastMetrics()
    actual = np.array([row.actual for row in items], dtype=float)
    median = np.array([row.quantiles[QuantileName.P50] for row in items], dtype=float)
    origin = np.array([row.origin_price for row in items], dtype=float)
    errors = actual - median
    abs_errors = np.abs(errors)
    mae = float(np.mean(abs_errors))
    rmse = float(np.sqrt(np.mean(errors**2)))
    scale = np.abs(actual[seasonal_period:] - actual[:-seasonal_period]) if len(actual) > seasonal_period else np.array([])
    mase_denominator = float(np.mean(scale)) if scale.size and float(np.mean(scale)) > 0 else None
    mase = mae / mase_denominator if mase_denominator else None
    denominator = np.abs(actual) + np.abs(median)
    smape = float(np.mean(np.where(denominator > 0, 2 * abs_errors / denominator, 0.0)))

    losses: list[float] = []
    empirical: dict[str, float] = {}
    calibration: dict[str, float] = {}
    for name, level in QUANTILE_LEVELS.items():
        forecasts = np.array([row.quantiles[name] for row in items], dtype=float)
        losses.extend(pinball_loss(a, f, level) for a, f in zip(actual, forecasts))
        coverage = float(np.mean(actual <= forecasts))
        empirical[name.value] = coverage
        calibration[name.value] = coverage - level

    actual_direction = np.sign(actual - origin)
    predicted_direction = np.sign(median - origin)
    directional_accuracy = float(np.mean(actual_direction == predicted_direction))
    predicted_up = predicted_direction > 0
    predicted_down = predicted_direction < 0
    up_precision = float(np.mean(actual_direction[predicted_up] > 0)) if predicted_up.any() else None
    down_precision = float(np.mean(actual_direction[predicted_down] < 0)) if predicted_down.any() else None
    p1 = np.array([row.quantiles[QuantileName.P1] for row in items], dtype=float)
    p25 = np.array([row.quantiles[QuantileName.P25] for row in items], dtype=float)
    p75 = np.array([row.quantiles[QuantileName.P75] for row in items], dtype=float)
    p99 = np.array([row.quantiles[QuantileName.P99] for row in items], dtype=float)
    return ForecastMetrics(
        mae=mae,
        rmse=rmse,
        mase=mase,
        smape=smape,
        weighted_quantile_loss=float(np.mean(losses)),
        directional_accuracy=directional_accuracy,
        up_precision=up_precision,
        down_precision=down_precision,
        interval_coverage_central=float(np.mean((actual >= p25) & (actual <= p75))),
        interval_coverage_extreme=float(np.mean((actual >= p1) & (actual <= p99))),
        interval_width_central=float(np.mean(p75 - p25)),
        interval_width_extreme=float(np.mean(p99 - p1)),
        calibration_error=calibration,
        empirical_coverage=empirical,
        sample_size=len(items),
    )


def validation_objective(metrics: ForecastMetrics) -> float:
    if metrics.sample_size <= 0:
        return math.inf
    wql = metrics.weighted_quantile_loss if metrics.weighted_quantile_loss is not None else math.inf
    mase = metrics.mase if metrics.mase is not None else (metrics.mae or math.inf)
    calibration = float(np.mean(np.abs(list(metrics.calibration_error.values())))) if metrics.calibration_error else 1.0
    return float(wql + mase + calibration)
