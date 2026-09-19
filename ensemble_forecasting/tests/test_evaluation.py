import numpy as np
import pandas as pd
import pytest

from ensemble_forecasting.evaluation import HISTORICAL_VALIDATION_POLICY, score_predictions
from ensemble_forecasting.worker import execute_job
from ensemble_forecasting.adapters.mock import MockAdapter


def job(count=80):
    return {"evaluation_policy": HISTORICAL_VALIDATION_POLICY, "source": {"type": "series"},
            "request": {"prediction_length": 7, "horizon_mode": "frequency_periods", "frequency": "1D",
                        "calendar": "NONE", "transform": "auto", "quantiles": [.1, .5, .9],
                        "models": {"prophet": {"enabled": True, "weight": 1}}},
            "input": {"frequency": "1D", "rows": [{"timestamp": t.isoformat(), "target": 10. + n}
                         for n, t in enumerate(pd.date_range("2026-01-01", periods=count, tz="UTC"))]}}


def test_four_metrics_hand_calculation():
    rows = [{"timestamp": f"2026-01-0{n}T00:00:00Z", "quantiles": {"0.1": n+8, "0.5": n+10, "0.9": n+12}} for n in (1, 2, 3)]
    values = {row["timestamp"]: value for row, value in zip(rows, [10, 14, 12])}
    result = score_predictions(rows, values, (.1, .5, .9))
    assert result["mae"] == pytest.approx(4/3)
    assert result["rmse"] == pytest.approx(np.sqrt(2))
    assert result["smape"] == pytest.approx((2/21+4/26+2/25)/3)
    assert result["average_wql"] == pytest.approx((1.2+4+1.2)/36/3)


def test_prefix_only_models_and_final_forecast_unchanged(monkeypatch):
    calls = []
    def factory(name, **_):
        adapter = MockAdapter(name)
        original = adapter.forecast
        def forecast(series, timestamps, request):
            calls.append((series, timestamps, request))
            return original(series, timestamps, request)
        adapter.forecast = forecast
        return adapter
    monkeypatch.setattr("ensemble_forecasting.worker.adapter_factory", factory)
    data = job()
    result = execute_job(data)
    assert [len(c[0].values) for c in calls] == [80, 73]
    assert calls[1][0].timestamps[-1] < calls[1][1][0]
    assert calls[0][2].models == calls[1][2].models
    assert calls[0][2].quantiles == calls[1][2].quantiles
    assert result["historical_validation"]["status"] == "completed"
    assert result["historical_validation"]["metrics"]["count"] == 7
    assert result["historical_validation"]["prepared_series_hash"] != result["prepared_series_hash"]
    assert "historical_validation" not in result["historical_validation"]
    del data["evaluation_policy"]
    ordinary = execute_job(data)
    assert ordinary["predictions"] == result["predictions"]
    assert "historical_validation" not in ordinary


def test_holdout_target_changes_cannot_change_its_predictions():
    data = job()
    first = execute_job(data, mock=True)["historical_validation"]
    # Even transform=auto must look at the prefix, not withheld negative targets.
    for row in data["input"]["rows"][-7:]:
        row["target"] = -999
    second = execute_job(data, mock=True)["historical_validation"]
    assert first["evidence"]["predictions"] == second["evidence"]["predictions"]
    assert first["metrics"] != second["metrics"]


def test_all_models_tail_renormalization_and_progress(monkeypatch):
    monkeypatch.setenv("TIMESFM_HF_ACCESS_APPROVED", "true")
    monkeypatch.setenv("TIMESFM_COMMERCIAL_LICENSED", "true")
    data = job()
    data["request"]["models"] = {name: {"enabled": True, "weight": .2} for name in ("prophet", "toto", "granite", "chronos", "timesfm")}
    data["request"]["quantiles"] = [.01, .1, .25, .5, .75, .9, .99]
    progress = []
    report = execute_job(data, mock=True, progress=progress.append)["historical_validation"]
    assert report["effective_weights_by_quantile"]["0.5"] == {name: .2 for name in data["request"]["models"]}
    assert report["effective_weights_by_quantile"]["0.01"] == {name: 1/3 for name in ("prophet", "granite", "chronos")}
    assert any(row.get("phase") == "historical_validation" for row in progress)


def test_failed_validation_does_not_destroy_future_forecast_or_renormalize(monkeypatch):
    calls = 0
    def factory(name, **_):
        nonlocal calls
        calls += 1
        return MockAdapter(name, fail=calls > 1)
    monkeypatch.setattr("ensemble_forecasting.worker.adapter_factory", factory)
    data = job()
    data["request"]["failure_policy"] = "renormalize"
    result = execute_job(data)
    assert len(result["predictions"]) == 7
    assert result["historical_validation"]["status"] == "failed"
    assert result["historical_validation"]["metrics"] is None


def test_short_and_flat_history_are_explicit_not_fabricated():
    data = job(2)
    result = execute_job(data, mock=True, minimum_history_rows=2)
    assert result["historical_validation"]["status"] == "insufficient_history"
    data = job(40)
    for row in data["input"]["rows"]: row["target"] = 0
    result = execute_job(data, mock=True)
    report = result["historical_validation"]
    assert report["status"] == "completed"
    assert report["metrics"]["average_wql"] is None
    assert np.isfinite(report["metrics"]["mae"])


def test_no_p50_custom_quantiles_and_zero_smape():
    stamp = "2026-01-01T00:00:00Z"
    report = score_predictions([{"timestamp": stamp, "quantiles": {"0.123": 0, "0.5": 0}}], {stamp: 0}, (.123, .5))
    assert report["smape"] == 0
    assert report["average_wql"] is None
    data = job()
    data["request"]["quantiles"] = [.123, .876]
    report = execute_job(data, mock=True)["historical_validation"]
    assert report["metrics"]["mae"] is None
    assert report["metrics"]["average_wql"] is not None


def test_missing_timestamps_are_not_filled_or_shifted():
    data = job()
    for row in data["input"]["rows"][-7:]:
        row["timestamp"] = (pd.Timestamp(row["timestamp"]) + pd.Timedelta(days=20)).isoformat()
    result = execute_job(data, mock=True)["historical_validation"]
    assert result["status"] == "no_matching_outcomes"
    assert result["metrics"]["count"] == 0


def test_daily_equity_provider_open_labels_match_exchange_sessions():
    import pandas_market_calendars as mcal
    dates = mcal.get_calendar("NYSE").schedule("2026-04-01", "2026-08-01").iloc[:80]
    data = job()
    data["source"] = {"type": "ticker", "exchange_timezone": "America/New_York"}
    data["request"].update(calendar="NYSE", horizon_mode="trading_sessions")
    data["input"]["rows"] = [{"timestamp": stamp.isoformat(), "target": 100.+i} for i, stamp in enumerate(dates.market_open)]
    result = execute_job(data, mock=True)["historical_validation"]
    assert result["status"] == "completed"
    assert result["metrics"]["count"] == 7
    assert all(row["timestamp"][:10] != "2026-07-03" for row in result["evidence"]["predictions"])


def test_validation_is_bounded_and_respects_toto_minimum():
    data = job(40)
    data["request"].update(prediction_length=365, models={"toto": {"enabled": True, "weight": 1}})
    report = execute_job(data, mock=True)["historical_validation"]
    assert report["training_rows"] == 32 and report["holdout_rows"] == 8
    data = job(500)
    data["request"]["prediction_length"] = 365
    assert execute_job(data, mock=True)["historical_validation"]["holdout_rows"] == 30


def test_real_prophet_holdout_smoke():
    import os
    if os.environ.get("QUANTURA_REAL_VALIDATION_SMOKE") != "1":
        pytest.skip("real-model evaluation is explicitly gated")
    data = job(80)
    result = execute_job(data)
    report = result["historical_validation"]
    assert report["status"] == "completed"
    assert report["metrics"]["count"] == 7
    assert all(np.isfinite(report["metrics"][key]) for key in ("mae", "rmse", "smape", "average_wql"))
    assert report["model_runs"][0]["device"] == "cpu"
    assert result["runtime"]["mock"] is False
