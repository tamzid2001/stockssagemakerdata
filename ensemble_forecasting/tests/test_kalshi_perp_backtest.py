from __future__ import annotations

import math

from ensemble_forecasting.kalshi_perp_backtest import (
    aggregate_reports,
    evaluate_market,
    normalize_daily_candles,
    underlying_units,
)


def test_contract_prices_are_normalized_to_underlying_spot_scale() -> None:
    market = {"ticker": "KXGOLDPERP", "contract_size": "0.001", "underlying_multiplier": "1"}
    rows = normalize_daily_candles([
        {"end_period_ts": 1_700_000_000, "price": {"close": "4.375"}},
        {"end_period_ts": 1_700_086_400, "price": {"close": None}},
    ], market)
    assert underlying_units(market) == 0.001
    assert rows[0]["target"] == 4375
    assert len(rows) == 1


def test_each_origin_withholds_the_next_close_and_reports_hits_returns_and_drawdown() -> None:
    prices = [100, 100, 80, 100, 115, 100, 85, 95, 105]
    rows = [{"timestamp": f"2026-09-{index + 1:02d}T00:00:00Z", "target": price} for index, price in enumerate(prices)]
    seen: list[list[dict[str, object]]] = []

    def forecast(history, _market):
        seen.append([dict(row) for row in history])
        return [
            {"timestamp": f"2026-10-{step + 1:02d}T00:00:00Z", "quantiles": {"0.1": 90, "0.5": 100, "0.9": 110}}
            for step in range(7)
        ]

    report = evaluate_market({"ticker": "KXTESTPERP", "title": "Test"}, rows, forecast)
    assert len(seen) == len(rows) - 2
    assert all(history == rows[:len(history)] for history in seen)
    assert report["signal_counts"] == {"buy": 2, "sell": 1, "none": 4}
    assert report["hit_rates"]["buy"]["avg_p50_hits"] == 2
    assert report["hit_rates"]["buy"]["outer_quantile_hits"] == 0
    assert report["hit_rates"]["sell"]["avg_p50_hits"] == 1
    assert [trade["exit_reason"] for trade in report["trades"]] == ["opposite_signal", "opposite_signal", "end_of_data"]
    assert report["profit"] > 0
    assert report["max_drawdown_pct"] >= 0
    assert math.isclose(report["final_equity"], report["initial_capital"] + report["profit"])


def test_portfolio_aggregation_uses_equal_starting_allocations() -> None:
    reports = [
        {"symbol": "A", "final_equity": 11_000, "origins_evaluated": 3, "trades": [{}], "equity_curve": [{"timestamp": "2026-01-01T00:00:00Z", "equity": 9_000}, {"timestamp": "2026-01-02T00:00:00Z", "equity": 11_000}]},
        {"symbol": "B", "final_equity": 10_000, "origins_evaluated": 2, "trades": [], "equity_curve": []},
    ]
    aggregate = aggregate_reports(reports, 10_000)
    assert aggregate["profit"] == 1_000
    assert math.isclose(aggregate["return_pct"], 5)
    assert math.isclose(aggregate["max_drawdown_pct"], 5)
