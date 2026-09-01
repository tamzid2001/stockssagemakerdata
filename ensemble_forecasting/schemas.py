from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Mapping

import numpy as np

ModelId = Literal["prophet", "toto", "granite", "chronos", "timesfm"]
Transform = Literal["auto", "log", "none"]
HorizonMode = Literal["trading_sessions", "calendar_days", "frequency_periods"]
FailurePolicy = Literal["fail", "renormalize"]

APPROVED_MODELS: tuple[ModelId, ...] = ("prophet", "toto", "granite", "chronos", "timesfm")


@dataclass(frozen=True)
class ModelSelection:
    enabled: bool = False
    weight: float = 0.0

    def validate(self, model_id: str) -> None:
        if model_id not in APPROVED_MODELS:
            raise ValueError(f"unsupported model: {model_id}")
        if not np.isfinite(self.weight) or self.weight < 0:
            raise ValueError(f"{model_id} weight must be finite and >= 0")


@dataclass(frozen=True)
class ForecastRequest:
    prediction_length: int
    horizon_mode: HorizonMode
    quantiles: tuple[float, ...]
    transform: Transform
    context_length: int | None
    models: Mapping[ModelId, ModelSelection]
    model_checkpoints: Mapping[ModelId, str | None] = field(default_factory=dict)
    failure_policy: FailurePolicy = "fail"
    frequency: str = "1D"
    calendar: str = "NYSE"
    runtime_mode: Literal["production", "development", "test"] = "production"
    max_quantiles: int = 21

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ForecastRequest":
        models_raw = payload.get("models")
        if not isinstance(models_raw, Mapping):
            raise ValueError("models must be an object")
        models: dict[ModelId, ModelSelection] = {}
        for model_id in APPROVED_MODELS:
            raw = models_raw.get(model_id, {})
            if not isinstance(raw, Mapping):
                raise ValueError(f"{model_id} configuration must be an object")
            models[model_id] = ModelSelection(
                enabled=bool(raw.get("enabled", False)),
                weight=float(raw.get("weight", 0.0)),
            )
        quantiles = normalize_quantiles(payload.get("quantiles", ()), int(payload.get("max_quantiles", 21)))
        context = payload.get("context_length")
        checkpoint_payload = payload.get("model_checkpoints") or {}
        if not isinstance(checkpoint_payload, Mapping):
            raise ValueError("model_checkpoints must be an object")
        unknown_checkpoints = set(checkpoint_payload) - set(APPROVED_MODELS)
        if unknown_checkpoints:
            raise ValueError("model_checkpoints contains an unsupported model")
        model_checkpoints = {
            model_id: None if checkpoint_payload.get(model_id) in (None, "") else str(checkpoint_payload[model_id])
            for model_id in APPROVED_MODELS
            if model_id in checkpoint_payload
        }
        request = cls(
            prediction_length=int(payload.get("prediction_length", 30)),
            horizon_mode=str(payload.get("horizon_mode", "trading_sessions")),  # type: ignore[arg-type]
            quantiles=quantiles,
            transform=str(payload.get("transform", "auto")),  # type: ignore[arg-type]
            context_length=None if context in (None, "") else int(context),
            models=models,
            model_checkpoints=model_checkpoints,
            failure_policy=str(payload.get("failure_policy", "fail")),  # type: ignore[arg-type]
            frequency=str(payload.get("frequency", "1D")),
            calendar=str(payload.get("calendar", "NYSE")),
            runtime_mode=str(payload.get("runtime_mode", "production")),  # type: ignore[arg-type]
            max_quantiles=int(payload.get("max_quantiles", 21)),
        )
        request.validate()
        return request

    def validate(self) -> None:
        if self.prediction_length < 1 or self.prediction_length > 1024:
            raise ValueError("prediction_length must be between 1 and 1024")
        if self.horizon_mode not in {"trading_sessions", "calendar_days", "frequency_periods"}:
            raise ValueError("unsupported horizon_mode")
        if self.transform not in {"auto", "log", "none"}:
            raise ValueError("unsupported transform")
        if self.failure_policy not in {"fail", "renormalize"}:
            raise ValueError("unsupported model_failure_policy")
        if self.runtime_mode not in {"production", "development", "test"}:
            raise ValueError("unsupported runtime_mode")
        if self.context_length is not None and not 40 <= self.context_length <= 16384:
            raise ValueError("context_length must be between 40 and 16384")
        for model_id, selection in self.models.items():
            selection.validate(model_id)
        for model_id, checkpoint in self.model_checkpoints.items():
            if model_id not in APPROVED_MODELS or checkpoint is not None and (not checkpoint or len(checkpoint) > 240):
                raise ValueError("model_checkpoints is invalid")
        enabled = {name: value for name, value in self.models.items() if value.enabled}
        if not enabled:
            raise ValueError("at least one model must be enabled")
        if not any(value.weight > 0 for value in enabled.values()):
            raise ValueError("at least one enabled model must have a positive weight")


@dataclass(frozen=True)
class PreparedSeries:
    timestamps: tuple[str, ...]
    values: np.ndarray
    transformed_values: np.ndarray
    transform: Literal["log", "none"]
    frequency: str
    timezone: str
    dataset_hash: str


@dataclass
class ModelForecast:
    model_id: ModelId
    checkpoint: str | None
    prediction_length: int
    requested_quantiles: tuple[float, ...]
    available_quantiles: tuple[float, ...]
    quantile_matrix: np.ndarray
    quantile_provenance: dict[str, str]
    device: str
    duration_seconds: float
    warnings: list[str] = field(default_factory=list)
    package_versions: dict[str, str] = field(default_factory=dict)

    def validate(self) -> None:
        expected = (len(self.requested_quantiles), self.prediction_length)
        if self.quantile_matrix.shape != expected:
            raise ValueError(f"{self.model_id}: quantile matrix {self.quantile_matrix.shape} != {expected}")
        available = set(self.available_quantiles)
        for index, quantile in enumerate(self.requested_quantiles):
            row = self.quantile_matrix[index]
            if quantile in available and not np.isfinite(row).all():
                raise ValueError(f"{self.model_id}: available quantile {quantile} contains NaN/inf")
            if quantile not in available and not np.isnan(row).all():
                raise ValueError(f"{self.model_id}: unavailable quantile {quantile} must be NaN")


@dataclass(frozen=True)
class EnsembleResult:
    timestamps: tuple[str, ...]
    quantiles: tuple[float, ...]
    quantile_matrix: np.ndarray
    effective_weights: dict[str, dict[str, float]]
    transform: str
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        for step, timestamp in enumerate(self.timestamps):
            structured = {canonical_quantile_string(q): float(self.quantile_matrix[index, step]) for index, q in enumerate(self.quantiles)}
            row: dict[str, Any] = {"timestamp": timestamp, "quantiles": structured}
            for q, value in zip(self.quantiles, self.quantile_matrix[:, step]):
                common = common_quantile_key(q)
                if common:
                    row[common] = float(value)
            rows.append(row)
        return {
            "prediction_length": len(self.timestamps),
            "quantiles": list(self.quantiles),
            "predictions": rows,
            "effective_weights_by_quantile": self.effective_weights,
            "transform": self.transform,
            "warnings": list(self.warnings),
        }


def normalize_quantiles(values: Any, maximum: int = 21) -> tuple[float, ...]:
    if not isinstance(values, (list, tuple)) or not values:
        raise ValueError("at least one quantile is required")
    normalized: dict[str, float] = {}
    for raw in values:
        value = float(raw)
        if not np.isfinite(value) or not 0 < value < 1:
            raise ValueError("quantiles must be finite and strictly between 0 and 1")
        key = canonical_quantile_string(value)
        normalized[key] = value
    result = tuple(sorted(normalized.values()))
    if len(result) > maximum:
        raise ValueError(f"at most {maximum} quantiles may be requested")
    return result


def canonical_quantile_string(value: float) -> str:
    return np.format_float_positional(float(value), trim="-")


def common_quantile_key(value: float) -> str | None:
    basis_points = round(float(value) * 100)
    if abs(float(value) - basis_points / 100) > 1e-10:
        return None
    return f"p{basis_points:02d}" if basis_points < 100 else "p100"


def serialize_model_forecast(value: ModelForecast) -> dict[str, Any]:
    payload = asdict(value)
    payload["quantile_matrix"] = value.quantile_matrix.tolist()
    return payload
