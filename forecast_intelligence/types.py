from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Mapping


class AnalysisMode(str, Enum):
    LIVE = "live"
    BACKTEST = "backtest"


class ModelName(str, Enum):
    PROPHET = "prophet"
    CHRONOS = "chronos_2"
    ENSEMBLE = "ensemble"
    BASELINE = "statistical_baseline"


class QuantileName(str, Enum):
    P1 = "P1"
    P25 = "P25"
    P50 = "P50"
    P75 = "P75"
    P99 = "P99"


QUANTILE_LEVELS: dict[QuantileName, float] = {
    QuantileName.P1: 0.01,
    QuantileName.P25: 0.25,
    QuantileName.P50: 0.50,
    QuantileName.P75: 0.75,
    QuantileName.P99: 0.99,
}


@dataclass(frozen=True)
class QuantileValue:
    value: float
    provenance: str
    native_level: float | None = None
    calibrated: bool = False


@dataclass(frozen=True)
class ForecastPoint:
    timestamp: str
    quantiles: Mapping[QuantileName, QuantileValue]


@dataclass
class ForecastMetrics:
    mae: float | None = None
    rmse: float | None = None
    mase: float | None = None
    smape: float | None = None
    weighted_quantile_loss: float | None = None
    directional_accuracy: float | None = None
    up_precision: float | None = None
    down_precision: float | None = None
    interval_coverage_central: float | None = None
    interval_coverage_extreme: float | None = None
    interval_width_central: float | None = None
    interval_width_extreme: float | None = None
    calibration_error: dict[str, float] = field(default_factory=dict)
    empirical_coverage: dict[str, float] = field(default_factory=dict)
    sample_size: int = 0


@dataclass
class ModelForecast:
    model: ModelName
    as_of: str
    target_date: str
    horizon: str
    sessions: int
    current_price: float
    points: list[ForecastPoint]
    metrics: ForecastMetrics = field(default_factory=ForecastMetrics)
    available: bool = True
    unavailable_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def final(self) -> ForecastPoint:
        if not self.points:
            raise ValueError(f"{self.model.value} forecast has no points")
        return self.points[-1]


@dataclass(frozen=True)
class Scenario:
    label: str
    quantile: QuantileName
    target: float
    dollar_change: float
    percent_change: float
    target_date: str
    provenance: str


@dataclass
class ForecastResult:
    ticker: str
    mode: AnalysisMode
    as_of: str
    timeframe: str
    target_date: str
    current_price: float
    selected_model: ModelName
    selection_metric: str
    selection_reason: str
    model_forecasts: dict[str, ModelForecast]
    scenarios: dict[str, Scenario]
    indicators: dict[str, Any]
    model_agreement: dict[str, Any]
    validation: dict[str, Any]
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def percent_change(target: float, current: float) -> float:
    if not (current > 0):
        raise ValueError("current price must be positive")
    return ((float(target) - float(current)) / float(current)) * 100.0


def validate_quantile_values(values: Mapping[QuantileName, QuantileValue]) -> None:
    missing = [name.value for name in QuantileName if name not in values]
    if missing:
        raise ValueError(f"missing forecast quantiles: {', '.join(missing)}")
    ordered = [float(values[name].value) for name in QuantileName]
    if any(not (value == value and abs(value) != float("inf")) for value in ordered):
        raise ValueError("forecast quantiles must be finite")
    if any(left > right for left, right in zip(ordered, ordered[1:])):
        raise ValueError("forecast quantiles violate P1 <= P25 <= P50 <= P75 <= P99")
