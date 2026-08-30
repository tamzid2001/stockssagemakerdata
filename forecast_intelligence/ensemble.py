from __future__ import annotations

from dataclasses import dataclass

from .metrics import EvaluationRow, evaluate_forecasts, validation_objective
from .types import ForecastPoint, ModelForecast, ModelName, QuantileName, QuantileValue, validate_quantile_values


@dataclass(frozen=True)
class EnsembleSelection:
    prophet_weight: float
    chronos_weight: float
    objective: float
    sample_size: int
    metric: str = "wql_plus_mase_plus_calibration_error"


def combine_forecasts(prophet: ModelForecast, chronos: ModelForecast, *, prophet_weight: float) -> ModelForecast:
    if prophet.sessions != chronos.sessions or prophet.target_date != chronos.target_date:
        raise ValueError("ensemble forecasts must have identical horizons")
    weight = max(0.0, min(1.0, float(prophet_weight)))
    points: list[ForecastPoint] = []
    for prophet_point, chronos_point in zip(prophet.points, chronos.points):
        if prophet_point.timestamp != chronos_point.timestamp:
            raise ValueError("ensemble forecast timestamps do not align")
        quantiles = {}
        for name in QuantileName:
            left = prophet_point.quantiles[name]
            right = chronos_point.quantiles[name]
            is_tail = name in {QuantileName.P1, QuantileName.P99}
            quantiles[name] = QuantileValue(
                value=weight * left.value + (1.0 - weight) * right.value,
                provenance="calibrated_ensemble_tail" if is_tail else "validated_weighted_ensemble",
                native_level=None,
                calibrated=is_tail or left.calibrated or right.calibrated,
            )
        validate_quantile_values(quantiles)
        points.append(ForecastPoint(prophet_point.timestamp, quantiles))
    return ModelForecast(
        model=ModelName.ENSEMBLE,
        as_of=prophet.as_of,
        target_date=prophet.target_date,
        horizon=prophet.horizon,
        sessions=prophet.sessions,
        current_price=prophet.current_price,
        points=points,
        metadata={
            "weights": {"prophet": weight, "chronos_2": 1.0 - weight},
            "tail_provenance": "weighted_prophet_posterior_and_chronos_empirical_calibration",
        },
    )


def select_ensemble_weight(
    prophet_rows: list[EvaluationRow],
    chronos_rows: list[EvaluationRow],
    *,
    candidate_weights: tuple[float, ...] = tuple(index / 10 for index in range(11)),
) -> EnsembleSelection:
    if len(prophet_rows) != len(chronos_rows) or not prophet_rows:
        raise ValueError("aligned non-empty Prophet and Chronos validation rows are required")
    best: EnsembleSelection | None = None
    for weight in candidate_weights:
        rows: list[EvaluationRow] = []
        for prophet, chronos in zip(prophet_rows, chronos_rows):
            if prophet.actual != chronos.actual or prophet.origin_price != chronos.origin_price:
                raise ValueError("validation targets are not aligned")
            quantiles = {
                name: weight * prophet.quantiles[name] + (1.0 - weight) * chronos.quantiles[name]
                for name in QuantileName
            }
            rows.append(EvaluationRow(prophet.actual, prophet.origin_price, quantiles))
        metrics = evaluate_forecasts(rows)
        candidate = EnsembleSelection(weight, 1.0 - weight, validation_objective(metrics), len(rows))
        if best is None or candidate.objective < best.objective:
            best = candidate
    if best is None:
        raise ValueError("no ensemble weight could be selected")
    return best
