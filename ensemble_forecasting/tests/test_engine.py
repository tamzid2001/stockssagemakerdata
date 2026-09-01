from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from ensemble_forecasting.adapters.mock import MockAdapter
from ensemble_forecasting.calendars import build_future_timestamps
from ensemble_forecasting.capabilities import timesfm_availability, validate_request_capabilities
from ensemble_forecasting.ensemble import build_ensemble, effective_weights_by_quantile
from ensemble_forecasting.preprocessing import prepare_series
from ensemble_forecasting.schemas import ForecastRequest, ModelSelection, normalize_quantiles
from ensemble_forecasting.validation import interpolate_native_quantiles, monotonic_rearrangement
from ensemble_forecasting.worker import execute_job


DEFAULT_MODELS = {
    name: {"enabled": True, "weight": 0.2}
    for name in ("prophet", "toto", "granite", "chronos", "timesfm")
}


def request(**updates) -> ForecastRequest:
    payload = {
        "prediction_length": 3,
        "horizon_mode": "frequency_periods",
        "frequency": "1D",
        "quantiles": [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99],
        "transform": "auto",
        "context_length": 64,
        "runtime_mode": "test",
        "failure_policy": "fail",
        "models": DEFAULT_MODELS,
    }
    payload.update(updates)
    return ForecastRequest.from_dict(payload)


def series_rows(count: int = 80) -> list[dict]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        {"timestamp": (start + timedelta(days=index)).isoformat(), "target": 100 + index * 0.25}
        for index in range(count)
    ]


def enable_timesfm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESFM_HF_ACCESS_APPROVED", "true")
    monkeypatch.setenv("ALLOW_NONCOMMERCIAL_TIMESFM", "true")
    monkeypatch.setenv("TIMESFM_COMMERCIAL_LICENSED", "false")


def test_model_toggle_validation() -> None:
    with pytest.raises(ValueError, match="at least one model"):
        request(models={name: {"enabled": False, "weight": 0} for name in DEFAULT_MODELS})


@pytest.mark.parametrize("weight", [-1, float("nan"), float("inf")])
def test_invalid_weights(weight: float) -> None:
    models = {**DEFAULT_MODELS, "prophet": {"enabled": True, "weight": weight}}
    with pytest.raises(ValueError, match="finite and >= 0"):
        request(models=models)


def test_default_central_weights_and_tail_reweighting(monkeypatch: pytest.MonkeyPatch) -> None:
    enable_timesfm(monkeypatch)
    value = request()
    validate_request_capabilities(value)
    weights = effective_weights_by_quantile(value)
    assert weights["0.5"] == pytest.approx({name: 0.2 for name in DEFAULT_MODELS})
    assert weights["0.01"] == pytest.approx({"prophet": 1 / 3, "granite": 1 / 3, "chronos": 1 / 3})
    assert "toto" not in weights["0.99"]
    assert "timesfm" not in weights["0.99"]


@pytest.mark.parametrize("model_id", ["toto", "timesfm"])
def test_native_interpolation_p25_p75(model_id: str) -> None:
    native = np.vstack([np.full(2, value) for value in range(1, 10)])
    matrix, available, provenance = interpolate_native_quantiles(
        native,
        native_levels=tuple(value / 10 for value in range(1, 10)),
        requested_levels=(0.01, 0.25, 0.75, 0.99),
    )
    assert available == (0.25, 0.75)
    assert matrix[1] == pytest.approx([2.5, 2.5])
    assert matrix[2] == pytest.approx([7.5, 7.5])
    assert np.isnan(matrix[0]).all() and np.isnan(matrix[3]).all()
    assert provenance["0.25"] == "interpolated_inside_native_range"


def test_unsupported_tail_with_toto_timesfm_only(monkeypatch: pytest.MonkeyPatch) -> None:
    enable_timesfm(monkeypatch)
    models = {
        name: {"enabled": name in {"toto", "timesfm"}, "weight": 1 if name in {"toto", "timesfm"} else 0}
        for name in DEFAULT_MODELS
    }
    value = request(models=models, quantiles=[0.01])
    with pytest.raises(ValueError, match="no enabled positive-weight model"):
        validate_request_capabilities(value)


def test_custom_quantile_sorting_and_deduplication() -> None:
    assert normalize_quantiles([0.75, 0.123456, 0.5, 0.123456, 0.1]) == (0.1, 0.123456, 0.5, 0.75)


def test_monotonic_rearrangement() -> None:
    repaired = monotonic_rearrangement(np.array([[3.0, 1.0], [1.0, 2.0], [2.0, 3.0]]))
    assert repaired.tolist() == [[1.0, 1.0], [2.0, 2.0], [3.0, 3.0]]


def test_nyse_trading_sessions_exclude_independence_day() -> None:
    dates = build_future_timestamps(
        "2026-07-02T00:00:00Z",
        prediction_length=2,
        horizon_mode="trading_sessions",
        frequency="1D",
        calendar="NYSE",
    )
    assert dates[0].startswith("2026-07-06")
    assert dates[1].startswith("2026-07-07")


def test_prediction_length_validation() -> None:
    with pytest.raises(ValueError, match="prediction_length"):
        request(prediction_length=0)


def test_timesfm_production_license_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESFM_HF_ACCESS_APPROVED", "true")
    monkeypatch.delenv("TIMESFM_COMMERCIAL_LICENSED", raising=False)
    monkeypatch.setenv("ALLOW_NONCOMMERCIAL_TIMESFM", "true")
    assert timesfm_availability("production") == (False, "commercial_license_required", False)
    assert timesfm_availability("test") == (True, None, True)


def test_per_quantile_ensemble_and_inverse_log(monkeypatch: pytest.MonkeyPatch) -> None:
    enable_timesfm(monkeypatch)
    value = request()
    prepared = prepare_series(series_rows(), transform="log", frequency="1D")
    timestamps = build_future_timestamps(
        prepared.timestamps[-1], prediction_length=3, horizon_mode="frequency_periods", frequency="1D"
    )
    forecasts = {name: MockAdapter(name).forecast(prepared, timestamps, value) for name in DEFAULT_MODELS}
    result = build_ensemble(value, forecasts, timestamps, chosen_transform="log")
    assert result.quantile_matrix.shape == (7, 3)
    assert np.all(result.quantile_matrix > 0)
    assert result.effective_weights["0.01"] == pytest.approx({"prophet": 1 / 3, "granite": 1 / 3, "chronos": 1 / 3})


def test_mock_worker_serialization_and_model_failure_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    enable_timesfm(monkeypatch)
    prepared = prepare_series(series_rows(), transform="auto", frequency="1D")
    job = {
        "runtime_mode": "test",
        "dataset_hash": "server-snapshot-hash",
        "request": {
            "prediction_length": 2,
            "horizon_mode": "frequency_periods",
            "frequency": "1D",
            "quantiles": [0.01, 0.25, 0.5, 0.75, 0.99],
            "transform": "auto",
            "context_length": 64,
            "failure_policy": "fail",
            "models": DEFAULT_MODELS,
        },
        "input": {"rows": series_rows(), "timestamp_column": "timestamp", "target_column": "target", "frequency": "1D"},
    }
    result = execute_job(job, mock=True)
    assert result["prediction_length"] == 2
    assert list(result["predictions"][0])[:2] == ["timestamp", "quantiles"]
    assert result["predictions"][0]["p01"] > 0
    assert result["runtime"]["mock"] is True
    assert result["dataset_hash"] == "server-snapshot-hash"
    assert result["prepared_series_hash"] == prepared.dataset_hash


def test_calendar_day_worker_uses_only_exchange_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    enable_timesfm(monkeypatch)
    rows = series_rows(183)
    job = {
        "runtime_mode": "test",
        "dataset_hash": "calendar-snapshot",
        "request": {
            "prediction_length": 10,
            "horizon_mode": "calendar_days",
            "frequency": "1D",
            "calendar": "NYSE",
            "quantiles": [0.25, 0.5, 0.75],
            "transform": "auto",
            "context_length": 64,
            "failure_policy": "fail",
            "models": {name: {"enabled": name == "prophet", "weight": 1 if name == "prophet" else 0} for name in DEFAULT_MODELS},
        },
        "input": {"rows": rows, "timestamp_column": "timestamp", "target_column": "target", "frequency": "1D"},
    }
    result = execute_job(job, mock=True)
    assert result["requested_prediction_length"] == 10
    assert 1 <= result["prediction_length"] < 10
    assert len(result["predictions"]) == result["prediction_length"]


def test_prepare_series_duplicate_and_hash_stability() -> None:
    rows = series_rows()
    rows.append({**rows[-1], "target": 123.45})
    first = prepare_series(rows, transform="auto", frequency="1D")
    second = prepare_series(rows, transform="auto", frequency="1D")
    assert first.dataset_hash == second.dataset_hash
    assert len(first.timestamps) == 80
    assert first.values[-1] == pytest.approx(123.45)
    assert first.transform == "log"


def test_log_transform_rejects_nonpositive() -> None:
    rows = series_rows()
    rows[-1]["target"] = 0
    with pytest.raises(ValueError, match="strictly positive"):
        prepare_series(rows, transform="log", frequency="1D")
