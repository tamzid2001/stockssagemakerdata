"""Offline point-in-time tests; no provider traffic or exchange orders."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ensemble_forecasting.quantile_backtest import run_quantile_replay, simulate_quantile_rules


BASE = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)


def bars(closes):
    return [{"timestamp": (BASE + timedelta(minutes=index)).isoformat(),
             "target": float(close), "open": float(close), "close": float(close)}
            for index, close in enumerate(closes)]


def forecast_rows(actual, first, count, levels):
    return [{"timestamp": actual[index]["timestamp"],
             "quantiles": {str(q): value for q, value in levels.items()}}
            for index in range(first, first + count)]


EXECUTION = {"starting_capital": 1000, "position_fraction": 1,
             "commission_bps": 0, "slippage_bps": 0}


def strategy(*rules, entry_logic="all"):
    return {"schema_version": 2, "type": "quantile_rules",
            "entry_logic": entry_logic, "rules": list(rules)}


def entry(identifier="entry", condition="crosses_above", quantile=.1):
    return {"id": identifier, "kind": "entry", "condition": condition, "quantile": quantile}


def target(identifier, kind, quantile):
    return {"id": identifier, "kind": kind, "target_mode": "quantile", "quantile": quantile}


def test_completed_close_entry_and_quantile_profit_fill_on_next_bar():
    actual = bars([100, 90, 95, 110, 115, 100, 101])
    window = {"number": 1, "cutoff_index": 1,
              "predictions": forecast_rows(actual, 2, 5, {.01: 80, .1: 92, .5: 108})}
    result = simulate_quantile_rules(actual, [window], strategy(
        entry(), target("profit", "take_profit", .5),
        target("stop", "stop_loss", .01)), EXECUTION,
        frequency="1min", fill_model="next_observed_bar_open")
    assert result["metrics"]["forecast_windows"] == 1
    assert result["metrics"]["matched_forecast_bars"] == 5
    assert result["metrics"]["trades"] == 1
    trade = result["trades"][0]
    assert trade["entry_at"] == actual[3]["timestamp"]
    assert trade["entry_price"] == 110
    assert trade["exit_at"] == actual[4]["timestamp"]
    assert trade["exit_price"] == 115
    assert trade["exit_rule_id"] == "profit"


def test_entry_blocks_stack_with_all_or_any_and_duplicate_positions_are_not_opened():
    actual = bars([100, 90, 95, 100, 101, 102, 103])
    window = {"number": 1, "cutoff_index": 1,
              "predictions": forecast_rows(actual, 2, 5, {.01: 80, .1: 92, .25: 93, .5: 200})}
    required = strategy(entry("cross"), entry("level", "at_or_below", .25),
                        target("profit", "take_profit", .5), entry_logic="all")
    rejected = simulate_quantile_rules(actual, [window.copy()], required, EXECUTION,
                                       frequency="1min", fill_model="quote_proxy")
    assert rejected["metrics"]["trades"] == 0
    optional = {**required, "entry_logic": "any"}
    accepted = simulate_quantile_rules(actual, [window.copy()], optional, EXECUTION,
                                       frequency="1min", fill_model="quote_proxy")
    assert accepted["metrics"]["trades"] == 1
    assert accepted["trades"][0]["reason"] == "end_of_test"


def test_stop_precedes_trailing_stop_when_both_match_same_completed_close():
    actual = bars([100, 90, 102, 105, 80, 78, 79])
    window = {"number": 1, "cutoff_index": 1,
              "predictions": forecast_rows(actual, 2, 5, {.1: 95, .5: 100, .9: 120})}
    rules = strategy(entry(quantile=.5), target("stop", "stop_loss", .1),
                     {"id": "trail", "kind": "trailing_stop", "percent": 5},
                     target("profit", "take_profit", .9))
    result = simulate_quantile_rules(actual, [window], rules, EXECUTION,
                                     frequency="1min", fill_model="quote_proxy")
    assert result["trades"][0]["exit_rule_id"] == "stop"
    assert result["trades"][0]["exit_at"] == actual[5]["timestamp"]


def test_rolling_inference_never_sees_the_future_and_keeps_flat_windows():
    actual = bars([.5] * 12)
    seen = []

    def infer(candidate):
        history = candidate["input"]["rows"]
        seen.append([row["timestamp"] for row in history])
        last = datetime.fromisoformat(history[-1]["timestamp"])
        return {"predictions": [{"timestamp": (last + timedelta(minutes=step)).isoformat(),
                                  "quantiles": {"0.1": .45, "0.5": .5, "0.9": .55}}
                                 for step in (1, 2, 3)],
                "models": [{"id": "chronos", "status": "completed"}]}

    job = {"source": {"type": "prediction_market"},
           "input": {"rows": actual, "timezone": "UTC"},
           "request": {"prediction_length": 3, "frequency": "1min", "quantiles": [.1, .5, .9]},
           "replay": {"context_rows": 4, "evaluation_windows": 2},
           "strategy": strategy(entry(condition="at_or_below"), target("profit", "take_profit", .9)),
           "execution": EXECUTION, "data_hash": "f" * 64, "fill_model": "quote_proxy"}
    output = run_quantile_replay(job, forecast_fn=infer)
    assert seen == [[row["timestamp"] for row in actual[2:6]],
                    [row["timestamp"] for row in actual[5:9]]]
    assert output["metrics"]["forecast_windows"] == 2
    assert output["metrics"]["matched_forecast_bars"] == 6
    assert all(window["predictions"][0]["timestamp"] > window["cutoff_at"]
               for window in output["forecast_windows"])
    assert output["live_eligible"] is False


def test_insufficient_real_rows_never_synthesizes_history():
    job = {"source": {"type": "prediction_market"}, "input": {"rows": bars([.5] * 5)},
           "request": {"prediction_length": 3, "frequency": "1min"},
           "replay": {"context_rows": 4, "evaluation_windows": 1},
           "strategy": strategy(entry(), target("profit", "take_profit", .5)),
           "execution": EXECUTION}
    with pytest.raises(ValueError, match="BACKTEST_HISTORY_INSUFFICIENT"):
        run_quantile_replay(job, forecast_fn=lambda candidate: {})
