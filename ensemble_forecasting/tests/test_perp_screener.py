from copy import deepcopy
from datetime import datetime, timedelta, timezone
import pytest
from ensemble_forecasting.kalshi_perp_screener import completed_daily, build_snapshot, configuration


def fixture():
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    history = [{"timestamp": (start + timedelta(days=i)).isoformat(), "target": 100.0} for i in range(40)]
    config = configuration()
    result = {"runtime": {"mock": False}, "models": [{"id": k, "status": "completed"} for k in config["models"]],
              "result_hash": "result", "dataset_hash": "dataset", "effective_weights_by_quantile": {},
              "predictions": [{"timestamp": (start + timedelta(days=40+i)).isoformat(), "quantiles": {str(q): 90+10*q for q in config["quantiles"]}} for i in range(7)]}
    return history, result


def test_completed_trade_closes_use_underlying_units_without_empty_bar_filling():
    candles = [{"end_period_ts": 86400, "price": {"close": "8.1"}}, {"end_period_ts": 172800, "price": {"close": None}}, {"end_period_ts": 259200, "price": {"close": "8.2"}}]
    rows = completed_daily(candles, {"contract_size": ".0001", "underlying_multiplier": "1"}, 172800)
    assert len(rows) == 1 and rows[0]["target"] == pytest.approx(81000)


def test_publication_requires_real_five_model_ordered_daily_predictions():
    history, result = fixture()
    snapshot = build_snapshot({"ticker": "KXBTCPERP"}, history, result, history[-1]["timestamp"])
    assert len(snapshot["forecast_rows"]) == 7
    assert set(snapshot["quantile_stats"]) == {"p01", "p25", "p50", "p75", "p90", "p99"}
    for change in ("mock", "missing_model", "hour_gap", "crossed_quantiles"):
        broken = deepcopy(result)
        if change == "mock": broken["runtime"]["mock"] = True
        elif change == "missing_model": broken["models"].pop()
        elif change == "hour_gap": broken["predictions"][0]["timestamp"] = broken["predictions"][1]["timestamp"]
        else: broken["predictions"][0]["quantiles"]["0.01"] = 200
        with pytest.raises(ValueError): build_snapshot({"ticker": "KXBTCPERP"}, history, broken, history[-1]["timestamp"])
