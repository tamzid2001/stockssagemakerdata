from __future__ import annotations

import json
import math

from ensemble_forecasting.kalshi_perp_backtest import (
    FIVE_MODEL_IDS,
    FIVE_MODEL_MINIMUM_HISTORY,
    aggregate_reports,
    combine_component_reports,
    evaluate_market,
    five_model_backtest_configuration,
    normalize_daily_candles,
    underlying_units,
)


def ensemble_result(predictions):
    return {
        "predictions": predictions,
        "models": [{"id": model_id, "status": "completed"} for model_id in FIVE_MODEL_IDS],
        "failures": [],
        "effective_weights_by_quantile": {
            quantile: {model_id: 0.2 for model_id in FIVE_MODEL_IDS}
            for quantile in ("0.1", "0.5", "0.9")
        },
        "result_hash": "test-forecast-hash",
    }


def test_contract_prices_are_normalized_to_underlying_spot_scale() -> None:
    market = {"ticker": "KXGOLDPERP", "contract_size": "0.001", "underlying_multiplier": "1"}
    rows = normalize_daily_candles([
        {"end_period_ts": 1_700_000_000, "price": {"close": "4.375"}},
        {"end_period_ts": 1_700_086_400, "price": {"close": None}},
    ], market)
    assert underlying_units(market) == 0.001
    assert rows[0]["target"] == 4375
    assert len(rows) == 1


def test_backtest_uses_the_pinned_equal_weight_five_model_weekly_profile() -> None:
    config = five_model_backtest_configuration()
    assert FIVE_MODEL_IDS == ("prophet", "toto", "granite", "chronos", "timesfm")
    assert FIVE_MODEL_MINIMUM_HISTORY == 32
    assert config["toto_variant"] == "4m"
    assert config["failure_policy"] == "fail"
    assert config["horizon_mode"] == "frequency_periods"
    assert config["calendar"] == "NONE"
    assert config["models"] == {
        model_id: {"enabled": True, "weight": 0.2} for model_id in FIVE_MODEL_IDS
    }
    assert set(config["model_checkpoints"]) == set(FIVE_MODEL_IDS)


def test_history_shorter_than_the_five_model_minimum_has_no_eligible_origin() -> None:
    prices = [100, 100, 80, 100, 115, 100, 85, 95, 105]
    rows = [{"timestamp": f"2026-09-{index + 1:02d}T00:00:00Z", "target": price} for index, price in enumerate(prices)]
    seen: list[list[dict[str, object]]] = []

    def forecast(history, _market):
        seen.append([dict(row) for row in history])
        return ensemble_result([
            {"timestamp": f"2026-10-{step + 1:02d}T00:00:00Z", "quantiles": {"0.1": 90, "0.5": 100, "0.9": 110}}
            for step in range(7)
        ])

    report = evaluate_market(
        {"ticker": "KXTESTPERP", "title": "Test"}, rows, forecast,
        minimum_history=FIVE_MODEL_MINIMUM_HISTORY,
    )
    assert len(seen) == 0
    assert report["origins_evaluated"] == 0


def test_each_origin_withholds_the_next_close_and_reports_hits_with_five_model_metadata() -> None:
    prices = [100] * 31 + [100, 80, 100, 115, 100, 85, 95, 105]
    rows = [{"timestamp": f"2026-{1 + index // 28:02d}-{1 + index % 28:02d}T00:00:00Z", "target": price} for index, price in enumerate(prices)]
    seen: list[list[dict[str, object]]] = []

    def forecast(history, _market):
        seen.append([dict(row) for row in history])
        return ensemble_result([
            {"timestamp": f"2026-10-{step + 1:02d}T00:00:00Z", "quantiles": {"0.1": 90, "0.5": 100, "0.9": 110}}
            for step in range(7)
        ])

    report = evaluate_market({"ticker": "KXTESTPERP", "title": "Test"}, rows, forecast)
    assert len(seen) == len(rows) - FIVE_MODEL_MINIMUM_HISTORY
    assert all(history == rows[:len(history)] for history in seen)
    assert report["signal_counts"] == {"buy": 2, "sell": 1, "none": 4}
    assert report["hit_rates"]["buy"]["avg_p50_hits"] == 2
    assert report["hit_rates"]["buy"]["outer_quantile_hits"] == 0
    assert report["hit_rates"]["sell"]["avg_p50_hits"] == 1
    assert [trade["exit_reason"] for trade in report["trades"]] == ["opposite_signal", "opposite_signal", "end_of_data"]
    assert report["profit"] > 0
    assert report["max_drawdown_pct"] >= 0
    assert math.isclose(report["final_equity"], report["initial_capital"] + report["profit"])
    assert all(row["ensemble_models"] == ",".join(FIVE_MODEL_IDS) for row in report["origin_results"])


def test_backtest_rejects_partial_ensemble_output() -> None:
    rows = [{"timestamp": f"2026-{1 + index // 28:02d}-{1 + index % 28:02d}T00:00:00Z", "target": 100} for index in range(33)]

    def forecast(_history, _market):
        return {
            **ensemble_result([{"quantiles": {"0.1": 90, "0.5": 100, "0.9": 110}}] * 7),
            "models": [{"id": "prophet", "status": "completed"}],
        }

    try:
        evaluate_market({"ticker": "KXTESTPERP"}, rows, forecast)
    except ValueError as exc:
        assert "all five" in str(exc)
    else:
        raise AssertionError("partial ensemble output must fail the backtest")


def test_portfolio_aggregation_uses_equal_starting_allocations() -> None:
    reports = [
        {"symbol": "A", "final_equity": 11_000, "origins_evaluated": 3, "trades": [{}], "equity_curve": [{"timestamp": "2026-01-01T00:00:00Z", "equity": 9_000}, {"timestamp": "2026-01-02T00:00:00Z", "equity": 11_000}]},
        {"symbol": "B", "final_equity": 10_000, "origins_evaluated": 2, "trades": [], "equity_curve": []},
    ]
    aggregate = aggregate_reports(reports, 10_000)
    assert aggregate["profit"] == 1_000
    assert math.isclose(aggregate["return_pct"], 5)
    assert math.isclose(aggregate["max_drawdown_pct"], 5)


def test_component_reports_are_combined_only_under_one_configuration(tmp_path) -> None:
    component = {
        "schema_version": "kalshi_perpetual_backtest_v2",
        "generated_at": "2026-09-20T00:00:00Z",
        "mode": "five_model_weekly_ensemble_mock",
        "data_provider": "Kalshi public margin market data",
        "methodology": {
            "configuration_hash": "immutable",
            "minimum_history": 32,
            "initial_capital_per_market": 10_000,
        },
        "coverage": {
            "catalog_markets": 1,
            "reported_markets": 0,
            "skipped_for_insufficient_history": ["KXSHORTPERP"],
        },
        "aggregate": aggregate_reports([], 10_000),
        "market_results": [],
    }
    component_path = tmp_path / "component" / "report.json"
    component_path.parent.mkdir()
    component_path.write_text(json.dumps(component), encoding="utf-8")
    combined = combine_component_reports([component_path], tmp_path / "combined")
    assert combined["coverage"] == {
        "catalog_markets": 1,
        "reported_markets": 0,
        "skipped_for_insufficient_history": ["KXSHORTPERP"],
        "component_reports": 1,
    }
