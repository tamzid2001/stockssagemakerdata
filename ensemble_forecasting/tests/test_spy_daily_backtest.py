from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
import yaml

from ensemble_forecasting.capabilities import model_supports_quantile
from ensemble_forecasting.spy_daily_backtest import (
    BACKTEST_ORIGINS,
    FIVE_MODEL_IDS,
    QUANTILES,
    ROLLING_CONTEXT,
    aggregate_reports,
    build_symbol_report,
    eligible_cutoffs,
    evaluate_origin,
    normalize_stock_history,
    parse_spy_holdings_frame,
    run_shard,
    stock_backtest_configuration,
    validate_stock_result,
)
from market_research.engine import digest
from scripts.weekly_screener import session_schedule


def exchange_rows(count: int, start: str = "2019-01-02") -> list[dict]:
    end = (date.fromisoformat(start) + timedelta(days=count * 2)).isoformat()
    sessions = session_schedule(start, end)[:count]
    assert len(sessions) == count
    return [
        {"timestamp": f"{session['date']}T00:00:00Z", "target": 100 + index / 10}
        for index, session in enumerate(sessions)
    ]


def result_for(
    timestamp: str, *, p10: float = 90, p50: float = 100, p90: float = 110
) -> dict:
    rows = []
    for step in range(7):
        quantiles = {
            "0.01": p10 - 5,
            "0.1": p10,
            "0.25": (p10 + p50) / 2,
            "0.5": p50,
            "0.75": (p50 + p90) / 2,
            "0.9": p90,
            "0.99": p90 + 5,
        }
        rows.append(
            {
                "timestamp": timestamp
                if step == 0
                else f"2099-01-{step + 1:02d}T00:00:00Z",
                "quantiles": quantiles,
            }
        )
    effective = {}
    for quantile in QUANTILES:
        key = str(quantile).rstrip("0").rstrip(".")
        supported = [
            model
            for model in FIVE_MODEL_IDS
            if model_supports_quantile(model, quantile)
        ]
        effective[key] = {model: 1 / len(supported) for model in supported}
    return {
        "predictions": rows,
        "models": [{"id": model, "status": "completed"} for model in FIVE_MODEL_IDS],
        "failures": [],
        "effective_weights_by_quantile": effective,
        "result_hash": "abc",
    }


def test_configuration_is_exact_weekly_five_model_profile():
    config = stock_backtest_configuration()
    assert config["prediction_length"] == 7
    assert config["horizon_mode"] == "trading_sessions"
    assert config["calendar"] == "NYSE"
    assert config["quantiles"] == list(QUANTILES)
    assert config["transform"] == "log"
    assert config["failure_policy"] == "fail"
    assert set(config["models"]) == set(FIVE_MODEL_IDS)
    assert {
        model: selection["weight"] for model, selection in config["models"].items()
    } == {model: 0.2 for model in FIVE_MODEL_IDS}
    assert config["toto_variant"] == "4m"


def test_latest_500_cutoffs_each_have_exact_500_prior_closes():
    rows = exchange_rows(1005)
    cutoffs = eligible_cutoffs(rows)
    assert len(cutoffs) == BACKTEST_ORIGINS
    assert cutoffs[0] == 1003
    assert cutoffs[-1] == 504
    for cutoff in cutoffs:
        assert len(rows[cutoff - ROLLING_CONTEXT + 1 : cutoff + 1]) == ROLLING_CONTEXT
        assert rows[cutoff + 1]["timestamp"] > rows[cutoff]["timestamp"]


def test_gap_is_not_mislabeled_as_a_one_session_cutoff():
    rows = exchange_rows(502)
    rows.pop(500)
    assert eligible_cutoffs(rows) == []


def test_origin_never_receives_withheld_or_older_extra_rows():
    rows = exchange_rows(501)
    captured = []

    def forecast(history, _security):
        captured.extend(history)
        return result_for(rows[-1]["timestamp"], p10=1000, p50=1100, p90=1200)

    record = evaluate_origin({"symbol": "SPY"}, rows, 499, forecast)
    assert len(captured) == 500
    assert captured[0] == rows[0]
    assert captured[-1] == rows[499]
    assert rows[500] not in captured
    assert record["actual"] == rows[500]["target"]
    assert record["signal"] == "buy"
    assert record["context_rows"] == 500


def test_tail_weights_must_be_equal_over_only_supported_models():
    value = result_for("2026-09-18T00:00:00Z")
    predictions, models = validate_stock_result(value)
    assert len(predictions) == 7 and models == FIVE_MODEL_IDS
    value["effective_weights_by_quantile"]["0.01"][
        next(iter(value["effective_weights_by_quantile"]["0.01"]))
    ] = 0.9
    with pytest.raises(ValueError, match="equally weighted"):
        validate_stock_result(value)


def test_returns_drawdown_and_turnover_are_chronological():
    rows = exchange_rows(510)
    records = [
        {
            "symbol": "SPY",
            "cutoff_index": 499,
            "signal_index": 500,
            "cutoff_at": rows[499]["timestamp"],
            "signal_at": rows[500]["timestamp"],
            "actual": rows[500]["target"],
            "signal": "buy",
            "avg_p10": 140,
            "avg_p50": 151,
            "avg_p90": 152,
        },
        {
            "symbol": "SPY",
            "cutoff_index": 501,
            "signal_index": 502,
            "cutoff_at": rows[501]["timestamp"],
            "signal_at": rows[502]["timestamp"],
            "actual": rows[502]["target"],
            "signal": "sell",
            "avg_p10": 140,
            "avg_p50": 149,
            "avg_p90": 150,
        },
        {
            "symbol": "SPY",
            "cutoff_index": 504,
            "signal_index": 505,
            "cutoff_at": rows[504]["timestamp"],
            "signal_at": rows[505]["timestamp"],
            "actual": rows[505]["target"],
            "signal": "buy",
            "avg_p10": 140,
            "avg_p50": 151,
            "avg_p90": 152,
        },
    ]
    report = build_symbol_report(
        {"symbol": "SPY", "is_etf": True}, rows, list(reversed(records))
    )
    assert report["signal_counts"] == {"buy": 2, "sell": 1, "none": 0}
    assert report["turnover"]["side_switches"] == 2
    assert report["turnover"]["mean_sessions_between_signals"] == 2.5
    assert report["turnover"]["mean_sessions_between_opposite_signals"] == 2.5
    # The final close can classify a signal, but cannot open a trade with no
    # subsequent close available for mark-to-market or exit.
    assert [trade["side"] for trade in report["trades"]] == ["buy", "sell"]
    assert report["max_drawdown_pct"] >= 0
    aggregate = aggregate_reports([report])
    assert aggregate["origins_evaluated"] == 3
    assert aggregate["closed_trades"] == 2
    assert aggregate["turnover"]["gross_notional_traded"] > 0


def test_normalization_deduplicates_and_never_pads_history():
    source = [
        {"timestamp": "2026-09-17T04:00:00Z", "close": 100},
        {"timestamp": "2026-09-17T20:00:00Z", "close": 100},
        {"timestamp": "2026-09-18T04:00:00Z", "close": 101},
        {"timestamp": "2026-09-19T04:00:00Z", "close": -1},
    ]
    assert normalize_stock_history(source, as_of="2026-09-18") == [
        {"timestamp": "2026-09-17T00:00:00Z", "target": 100},
        {"timestamp": "2026-09-18T00:00:00Z", "target": 101},
    ]
    with pytest.raises(ValueError, match="conflicting"):
        normalize_stock_history(
            [source[0], {**source[1], "close": 99}], as_of="2026-09-18"
        )


def test_official_spy_holdings_parser_excludes_cash_and_non_ticker_rows():
    frame = pd.DataFrame(
        [
            {
                "Name": "BERKSHIRE HATHAWAY",
                "Ticker": "BRK.B",
                "Weight": 1.5,
                "Shares Held": 10,
                "Local Currency": "USD",
            },
            {
                "Name": "APPLE INC",
                "Ticker": "AAPL",
                "Weight": 7.0,
                "Shares Held": 20,
                "Local Currency": "USD",
            },
            {
                "Name": "US DOLLAR",
                "Ticker": "-",
                "Weight": 0.2,
                "Shares Held": 1000,
                "Local Currency": "USD",
            },
            {
                "Name": "BAD FOREIGN",
                "Ticker": "BAD",
                "Weight": 0.1,
                "Shares Held": 2,
                "Local Currency": "EUR",
            },
        ]
    )
    rows = parse_spy_holdings_frame(frame)
    assert [row["symbol"] for row in rows] == ["AAPL", "BRK.B"]
    assert rows[0]["holding_weight_pct"] == 7.0


def test_shard_resumes_after_each_durable_origin(monkeypatch):
    rows = exchange_rows(502)
    security = {
        "symbol": "SPY",
        "company_name": "SPY",
        "is_etf": True,
        "rows": rows,
        "eligible_cutoffs": [500, 499],
    }

    class MemoryCloud:
        id = "p90-" + "1" * 24

        def __init__(self):
            self.configuration = {"shard_count": 1}
            self.records = {
                ("source", "shard-000"): {
                    "configuration_hash": digest(self.configuration),
                    "securities": [security],
                }
            }
            self.state = {
                "revision": 0,
                "symbol_index": 0,
                "origin_rank": 0,
                "complete": False,
            }

        def get(self, kind, key):
            return self.records.get((kind, key))

        def put(self, kind, key, value):
            previous = self.records.get((kind, key))
            assert previous is None or previous == value
            self.records[kind, key] = value

        def progress(self, _shard):
            return dict(self.state)

        def advance(self, _shard, expected_revision, value):
            assert self.state["revision"] == expected_revision
            self.state = {**dict(value), "revision": expected_revision + 1}
            return dict(self.state)

    monkeypatch.setenv("TIMESFM_HF_ACCESS_APPROVED", "true")
    monkeypatch.setenv("ALLOW_NONCOMMERCIAL_TIMESFM", "true")
    cloud = MemoryCloud()
    first = run_shard(cloud, 0, budget_minutes=11, mock=True, maximum_new_origins=1)
    assert first["origin_rank"] == 1 and not first["complete"]
    assert len([key for key in cloud.records if key[0] == "origin"]) == 1
    second = run_shard(cloud, 0, budget_minutes=11, mock=True, maximum_new_origins=1)
    assert second["complete"] and second["completed_symbols"] == 1
    report = cloud.records["symbol", "SPY"]
    assert report["origins_available"] == 2


def test_workflow_is_continuous_pinned_and_real_only():
    workflow = yaml.safe_load(
        Path(".github/workflows/spy-daily-500-backtest.yml").read_text()
    )
    assert workflow["jobs"]["worker"]["timeout-minutes"] == 350
    assert workflow["jobs"]["worker"]["strategy"]["matrix"]["shard"] == list(range(16))
    assert workflow["concurrency"]["cancel-in-progress"] is False
    worker_steps = workflow["jobs"]["worker"]["steps"]
    run = next(
        step
        for step in worker_steps
        if step.get("name") == "Run checkpointed real five-model origins"
    )
    assert "--budget-minutes 270" in run["run"]
    handoff = next(
        step
        for step in workflow["jobs"]["aggregate"]["steps"]
        if step.get("name") == "Continue unfinished campaign"
    )
    assert "code_ref:process.env.QUANTURA_CODE_SHA" in handoff["with"]["script"]
    assert "smoke" not in str(workflow).lower()
