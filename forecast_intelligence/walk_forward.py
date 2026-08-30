from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .calendar import resolve_horizon
from .metrics import EvaluationRow, evaluate_forecasts
from .models import ForecastModel, ModelUnavailable
from .types import ModelForecast, QuantileName


@dataclass
class WalkForwardResult:
    model: str
    rows: list[EvaluationRow]
    origins: list[str]
    failures: list[dict[str, str]]
    metrics: object


def walk_forward_evaluate(
    frame: pd.DataFrame,
    model: ForecastModel,
    *,
    ticker: str,
    timeframe: str,
    min_history: int = 80,
    step: int = 5,
    max_origins: int | None = None,
) -> WalkForwardResult:
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, errors="coerce")
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    data = data[data["timestamp"].notna() & data["close"].notna()].sort_values("timestamp").reset_index(drop=True)
    sessions = resolve_horizon(timeframe, data["timestamp"].iloc[min_history - 1]).sessions
    last_origin = len(data) - sessions - 1
    origin_indexes = list(range(min_history - 1, last_origin + 1, max(1, step)))
    if max_origins and len(origin_indexes) > max_origins:
        origin_indexes = origin_indexes[-max_origins:]
    rows: list[EvaluationRow] = []
    origins: list[str] = []
    failures: list[dict[str, str]] = []
    for origin_index in origin_indexes:
        as_of = data["timestamp"].iloc[origin_index].isoformat()
        horizon = resolve_horizon(timeframe, as_of)
        # The model receives the full frame intentionally; every adapter must enforce
        # as_of internally. Tests assert that training/context ends at this origin.
        try:
            forecast: ModelForecast = model.forecast(data, ticker=ticker, as_of=as_of, horizon=horizon)
            actual = float(data["close"].iloc[origin_index + sessions])
            origin_price = float(data["close"].iloc[origin_index])
            quantiles = {name: forecast.final.quantiles[name].value for name in QuantileName}
            rows.append(EvaluationRow(actual, origin_price, quantiles))
            origins.append(as_of)
        except (ValueError, ModelUnavailable) as exc:
            failures.append({"as_of": as_of, "error": str(exc)[:240]})
    return WalkForwardResult(model.name.value, rows, origins, failures, evaluate_forecasts(rows))


def chronological_splits(length: int, *, horizon: int, development_fraction: float = 0.60, validation_fraction: float = 0.20) -> dict[str, range]:
    if length < horizon * 6:
        raise ValueError("insufficient samples for purged chronological splits")
    development_end = int(length * development_fraction)
    validation_end = int(length * (development_fraction + validation_fraction))
    embargo = max(1, int(horizon))
    validation_start = development_end + embargo
    holdout_start = validation_end + embargo
    if validation_start >= validation_end or holdout_start >= length:
        raise ValueError("purge/embargo leaves an empty split")
    return {
        "development": range(0, development_end),
        "validation": range(validation_start, validation_end),
        "final_holdout": range(holdout_start, length),
    }
