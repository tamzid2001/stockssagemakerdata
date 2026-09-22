import datetime as dt
import functools
import gzip
import base64
import json
import pytest
from scripts.weekly_screener import build_weekly_forecast, weekly_configuration, completed_history, session_schedule
from ensemble_forecasting.worker import execute_job
from ensemble_forecasting.schemas import ForecastRequest

NOW = dt.datetime(2026, 9, 8, 22, tzinfo=dt.timezone.utc)

def history():
    return [{"timestamp":r["date"]+"T00:00:00Z", "close":100+i/10} for i,r in enumerate(session_schedule("2026-01-01", "2026-09-08"))]

def test_weekly_config_is_worker_valid_small_toto_equal_weights():
    request=ForecastRequest.from_dict(weekly_configuration())
    assert request.prediction_length == 7
    assert request.model_checkpoints["toto"] == "Datadog/Toto-2.0-4m"
    assert len(request.models)==5
    assert all(model.enabled and model.weight == .2 for model in request.models.values())

def test_real_shared_math_with_mock_adapters_in_tests_only(monkeypatch):
    monkeypatch.setenv("TIMESFM_HF_ACCESS_APPROVED", "true")
    monkeypatch.setenv("TIMESFM_COMMERCIAL_LICENSED", "true")
    result=build_weekly_forecast(history(),now=NOW,execute=functools.partial(execute_job,mock=True))
    assert result["history_cutoff_at"].startswith("2026-09-08")
    assert result["withheld_session"] is None
    assert len(result["rows"]) == 7
    assert result["rows"][0]["date"] == "2026-09-09"
    assert set(result["effective_weights_by_quantile"]["0.01"]) == {"prophet","granite","chronos"}
    assert len(result["effective_weights_by_quantile"]["0.5"]) == 5
    assert result["forecast_config"]["toto_variant"] == "4m"
    assert result["forecast_config"]["history_lag_sessions"] == 0
    assert result["buy_price_target"] == result["rows"][-1]["p99"]
    assert dt.datetime.fromisoformat(result["daily_close_at"]) == dt.datetime(2026,9,8,20,tzinfo=dt.timezone.utc)
    restored=json.loads(gzip.decompress(base64.b64decode(result["forecast_input_gzip"])))
    assert restored[-1][0] == result["history_cutoff_at"]
    assert len(restored) == result["forecast_history_points"]
    assert all(row["p01"]<=row["p10"]<=row["p50"]<=row["p90"]<=row["p99"] for row in result["rows"])

def test_partial_session_is_never_withheld_as_completed():
    rows=completed_history(history(), NOW.replace(hour=15))
    assert rows[-1]["timestamp"].startswith("2026-09-04")

def test_insufficient_history_returns_missing_not_drift():
    assert build_weekly_forecast(history()[:20],now=NOW) is None

def test_failure_propagates_and_cannot_publish_fabricated_forecast():
    def broken(*_args,**_kwargs): raise RuntimeError("model failed")
    with pytest.raises(RuntimeError):build_weekly_forecast(history(),now=NOW,execute=broken)
