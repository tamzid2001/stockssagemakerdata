from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from forecast_intelligence.artifacts import ARTIFACT_FILES, write_artifacts
from forecast_intelligence.backtest import BacktestConfig, run_trade_backtest
from forecast_intelligence.calendar import HORIZON_SESSIONS, resolve_horizon
from forecast_intelligence.ensemble import combine_forecasts, select_ensemble_weight
from forecast_intelligence.gpt import MockAnalysisClient, apply_output_guardrails, confidence_cap, load_prompt
from forecast_intelligence.indicators import point_in_time_indicators
from forecast_intelligence.metrics import EvaluationRow, evaluate_forecasts, pinball_loss
from forecast_intelligence.models import Chronos2Model, StatisticalBaselineModel
from forecast_intelligence.pipeline import ForecastIntelligencePipeline, PipelineConfig, build_gpt_payload
from forecast_intelligence.rag import PointInTimeRetriever, RagDocument
from forecast_intelligence.tools import ApprovedToolRegistry, ToolDefinition, ToolPolicyError
from forecast_intelligence.types import (
    AnalysisMode,
    ForecastPoint,
    ModelForecast,
    ModelName,
    QuantileName,
    QuantileValue,
    validate_quantile_values,
)
from forecast_intelligence.walk_forward import chronological_splits, walk_forward_evaluate


def market_frame(rows: int = 320) -> pd.DataFrame:
    index = pd.bdate_range("2024-01-02", periods=rows, tz="UTC")
    trend = np.arange(rows) * 0.08
    close = 100 + trend + np.sin(np.arange(rows) / 7) * 2
    return pd.DataFrame(
        {
            "timestamp": index,
            "open": close - 0.2,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": 1_000_000 + np.arange(rows) * 100,
        }
    )


class DeterministicModel:
    def __init__(self, name: ModelName, bias: float = 0.0):
        self.name = name
        self.bias = bias
        self.seen_training_ends: list[pd.Timestamp] = []

    def forecast(self, frame, *, ticker, as_of, horizon):
        cutoff = pd.Timestamp(as_of)
        cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
        data = frame[pd.to_datetime(frame["timestamp"], utc=True) <= cutoff]
        self.seen_training_ends.append(pd.to_datetime(data["timestamp"], utc=True).max())
        current = float(data["close"].iloc[-1])
        points = []
        for step, timestamp in enumerate(horizon.session_dates, 1):
            center = current + self.bias + step * 0.05
            quantiles = {
                QuantileName.P1: QuantileValue(center - 8, f"{self.name.value}_test"),
                QuantileName.P25: QuantileValue(center - 2, f"{self.name.value}_test"),
                QuantileName.P50: QuantileValue(center, f"{self.name.value}_test"),
                QuantileName.P75: QuantileValue(center + 2, f"{self.name.value}_test"),
                QuantileName.P99: QuantileValue(center + 8, f"{self.name.value}_test"),
            }
            points.append(ForecastPoint(timestamp, quantiles))
        return ModelForecast(self.name, pd.Timestamp(as_of).isoformat(), horizon.target_date, horizon.label, horizon.sessions, current, points)


def test_all_required_horizons_resolve_to_exchange_sessions():
    for label, sessions in HORIZON_SESSIONS.items():
        result = resolve_horizon(label, "2026-08-28T20:00:00Z")
        assert result.sessions == sessions
        assert len(result.session_dates) == sessions
        assert all(pd.Timestamp(value).dayofweek < 5 for value in result.session_dates)


def test_horizon_aliases_change_target_date():
    one = resolve_horizon("1d", "2026-08-28T20:00:00Z")
    month = resolve_horizon("1m", "2026-08-28T20:00:00Z")
    assert month.target_date > one.target_date
    assert one.sessions == 1 and month.sessions == 21


def test_indicators_ignore_post_asof_rows():
    frame = market_frame()
    as_of = frame["timestamp"].iloc[200].isoformat()
    before = point_in_time_indicators(frame, as_of=as_of)
    mutated = frame.copy()
    mutated.loc[201:, "close"] = 1_000_000
    after = point_in_time_indicators(mutated, as_of=as_of)
    assert before == after
    assert before["history_points"] == 201


def test_indicator_snapshot_contains_raw_values_and_regimes():
    result = point_in_time_indicators(market_frame(), as_of=market_frame()["timestamp"].iloc[-1])
    assert isinstance(result["rsi"]["value"], float)
    assert result["rsi"]["regime"] in {"oversold", "neutral", "overbought"}
    assert {"value", "signal", "histogram", "regime"}.issubset(result["macd"])


def test_baseline_quantiles_are_ordered_and_explicitly_not_prophet():
    frame = market_frame()
    model = StatisticalBaselineModel()
    forecast = model.forecast(frame, ticker="AAPL", as_of=frame["timestamp"].iloc[-1].isoformat(), horizon=resolve_horizon("5d", frame["timestamp"].iloc[-1]))
    validate_quantile_values(forecast.final.quantiles)
    assert forecast.model is ModelName.BASELINE
    assert forecast.metadata["identity"] == "baseline_not_prophet"


class FakeChronosPipeline:
    def __init__(self):
        self.context = None
        self.future = None
        self.levels = None

    def predict_df(self, context, *, future_df, prediction_length, quantile_levels, **kwargs):
        self.context = context.copy()
        self.future = future_df.copy()
        self.levels = quantile_levels
        base = float(context["target"].iloc[-1])
        return pd.DataFrame({"0.25": [base - 1] * prediction_length, "0.5": [base + 1] * prediction_length, "0.75": [base + 3] * prediction_length})


def test_chronos_uses_only_calendar_future_covariates_and_tracks_tail_provenance():
    frame = market_frame()
    fake = FakeChronosPipeline()
    model = Chronos2Model(pipeline=fake)
    forecast = model.forecast(frame, ticker="AAPL", as_of=frame["timestamp"].iloc[-1].isoformat(), horizon=resolve_horizon("5d", frame["timestamp"].iloc[-1]))
    assert list(fake.future.columns) == ["item_id", "timestamp", "day_of_week"]
    assert fake.levels == [0.25, 0.5, 0.75]
    assert forecast.final.quantiles[QuantileName.P50].provenance == "chronos_native"
    assert forecast.final.quantiles[QuantileName.P1].provenance == "chronos_external_empirical_tail"
    assert forecast.final.quantiles[QuantileName.P99].calibrated is True


def test_quantile_validation_rejects_crossing():
    values = {name: QuantileValue(value, "test") for name, value in zip(QuantileName, [80, 90, 89, 110, 120])}
    with pytest.raises(ValueError, match="P1 <= P25"):
        validate_quantile_values(values)


def test_pinball_and_full_forecast_metrics():
    assert pinball_loss(110, 100, 0.75) == 7.5
    rows = [
        EvaluationRow(101 + index, 100 + index, {
            QuantileName.P1: 90 + index,
            QuantileName.P25: 98 + index,
            QuantileName.P50: 101 + index,
            QuantileName.P75: 104 + index,
            QuantileName.P99: 112 + index,
        }) for index in range(20)
    ]
    metrics = evaluate_forecasts(rows)
    assert metrics.mae == 0
    assert metrics.directional_accuracy == 1
    assert metrics.sample_size == 20
    assert set(metrics.empirical_coverage) == {name.value for name in QuantileName}


def test_walk_forward_context_ends_at_each_origin():
    frame = market_frame()
    model = DeterministicModel(ModelName.PROPHET)
    result = walk_forward_evaluate(frame, model, ticker="AAPL", timeframe="5d", min_history=100, step=20, max_origins=5)
    assert len(result.rows) == 5
    for origin, training_end in zip(result.origins, model.seen_training_ends):
        assert training_end == pd.Timestamp(origin)


def test_ensemble_selection_uses_validation_accuracy_not_upside():
    actual = [100, 101, 102, 103, 104, 105]
    prophet = []
    chronos = []
    for value in actual:
        prophet.append(EvaluationRow(value, value - 1, {name: value + offset for name, offset in zip(QuantileName, [-5, -1, 0, 1, 5])}))
        chronos.append(EvaluationRow(value, value - 1, {name: value + 20 + offset for name, offset in zip(QuantileName, [-5, -1, 0, 1, 5])}))
    selected = select_ensemble_weight(prophet, chronos)
    assert selected.prophet_weight == 1.0


def test_combined_forecast_tracks_weight_and_tail_provenance():
    frame = market_frame()
    horizon = resolve_horizon("1d", frame["timestamp"].iloc[-1])
    left = DeterministicModel(ModelName.PROPHET).forecast(frame, ticker="AAPL", as_of=frame["timestamp"].iloc[-1].isoformat(), horizon=horizon)
    right = DeterministicModel(ModelName.CHRONOS, bias=2).forecast(frame, ticker="AAPL", as_of=frame["timestamp"].iloc[-1].isoformat(), horizon=horizon)
    result = combine_forecasts(left, right, prophet_weight=0.75)
    assert result.metadata["weights"] == {"prophet": 0.75, "chronos_2": 0.25}
    assert result.final.quantiles[QuantileName.P1].provenance == "calibrated_ensemble_tail"


def test_purged_splits_have_horizon_embargo():
    splits = chronological_splits(300, horizon=20)
    assert splits["validation"].start - splits["development"].stop >= 20
    assert splits["final_holdout"].start - splits["validation"].stop >= 20


def test_rag_excludes_future_publication_and_ingestion_in_backtest():
    docs = [
        RagDocument("old", "AAPL earnings context", "2025-01-01T10:00:00Z", "2025-01-01T11:00:00Z", "filing", "AAPL"),
        RagDocument("future", "AAPL earnings surprise", "2025-01-03T10:00:00Z", "2025-01-03T11:00:00Z", "news", "AAPL"),
    ]
    result = PointInTimeRetriever(docs).retrieve("AAPL earnings", ticker="AAPL", as_of="2025-01-02T00:00:00Z", mode=AnalysisMode.BACKTEST)
    assert [row["document_id"] for row in result] == ["old"]


def test_live_only_tool_is_blocked_in_backtest():
    registry = ApprovedToolRegistry([ToolDefinition("price", "live price", lambda args: {"price": 100})])
    with pytest.raises(ToolPolicyError, match="blocked during backtest"):
        registry.call("price", {"ticker": "AAPL", "as_of_timestamp": "2025-01-01T00:00:00Z"}, mode=AnalysisMode.BACKTEST, as_of="2025-01-01T00:00:00Z")


def test_point_in_time_tool_requires_exact_asof_and_caches_calls():
    calls = []
    registry = ApprovedToolRegistry([
        ToolDefinition("history", "as-of history", lambda args: calls.append(args) or {"close": 100}, backtest_allowed=True, point_in_time_aware=True)
    ], max_calls=1)
    args = {"ticker": "AAPL", "as_of_timestamp": "2025-01-01T00:00:00Z"}
    assert registry.call("history", args, mode=AnalysisMode.BACKTEST, as_of=args["as_of_timestamp"]) == {"close": 100}
    assert registry.call("history", args, mode=AnalysisMode.BACKTEST, as_of=args["as_of_timestamp"]) == {"close": 100}
    assert len(calls) == 1


def test_prompt_requires_numerical_targets_and_both_sides():
    prompt = load_prompt("v1-adversarial")
    assert "never invent or revise a target" in prompt.lower()
    assert "p75 bull case" in prompt.lower()
    assert "p25 bear case" in prompt.lower()
    assert "p1/p99" in prompt.lower()


def test_gpt_guardrails_restore_targets_and_cap_confidence():
    payload = {
        "scenarios": {
            "bear": {"target": 90, "percent_change": -10},
            "base": {"target": 100, "percent_change": 0},
            "bull": {"target": 110, "percent_change": 10},
        },
        "model_agreement": {"level": "conflicting"},
        "selected_forecast": {"validation_metrics": {"sample_size": 10, "mean_absolute_calibration_error": 0.25}},
        "ranges": {"extreme": {"percent_of_current": 60}},
    }
    output = {"signal": "LONG", "confidence": 0.99, "bear_case": {}, "base_case": {}, "bull_case": {"target": 999}}
    guarded = apply_output_guardrails(output, payload)
    assert guarded["bull_case"]["target"] == 110
    assert guarded["bear_case"]["change_pct"] == -10
    assert guarded["confidence"] <= 0.58


def test_backtest_executes_only_at_next_bar_open_and_deducts_costs():
    frame = market_frame(10)
    signals = pd.Series(["LONG"] + ["HOLD"] * 9)
    trades, _, metrics = run_trade_backtest(frame, signals, config=BacktestConfig())
    assert trades.iloc[0]["signal_timestamp"] == frame.iloc[0]["timestamp"]
    assert trades.iloc[0]["entry_timestamp"] == frame.iloc[1]["timestamp"]
    assert trades.iloc[0]["entry_price"] == frame.iloc[1]["open"]
    assert metrics["costs"] > 0


@pytest.mark.parametrize("timeframe", ["1d", "5d", "2w", "1m"])
def test_pipeline_updates_horizon_and_builds_all_scenarios(timeframe):
    frame = market_frame()
    config = PipelineConfig(run_prophet=True, run_chronos=True, walk_forward_min_history=100, walk_forward_step=30, max_validation_origins=6, min_selection_samples=3)
    pipeline = ForecastIntelligencePipeline(config, prophet_model=DeterministicModel(ModelName.PROPHET), chronos_model=DeterministicModel(ModelName.CHRONOS, bias=0.5))
    result, _, _ = pipeline.run(frame, ticker="AAPL", timeframe=timeframe, as_of=frame["timestamp"].iloc[-1].isoformat())
    assert result.target_date == resolve_horizon(timeframe, frame["timestamp"].iloc[-1]).target_date
    assert set(result.scenarios) == {"extreme_bear", "bear", "base", "bull", "extreme_bull"}
    assert result.selection_metric == "validation_wql_plus_mase_plus_calibration_error"


def test_gpt_payload_and_mock_analysis_are_complete_and_button_safe():
    frame = market_frame()
    pipeline = ForecastIntelligencePipeline(PipelineConfig(run_prophet=True, run_chronos=False, walk_forward_min_history=100, walk_forward_step=30, max_validation_origins=5, min_selection_samples=3), prophet_model=DeterministicModel(ModelName.PROPHET))
    result, _, _ = pipeline.run(frame, ticker="AAPL", timeframe="5d", as_of=frame["timestamp"].iloc[-1].isoformat())
    payload = build_gpt_payload(result)
    assert set(payload["selected_forecast"]["quantiles"]) == {name.value for name in QuantileName}
    analysis = MockAnalysisClient().analyze(payload, reasoning_effort="medium")
    assert analysis["bear_case"]["target"] == result.scenarios["bear"].target
    assert analysis["base_case"]["target"] == result.scenarios["base"].target
    assert analysis["bull_case"]["target"] == result.scenarios["bull"].target
    assert "BOTH_SIDES_REVIEWED" in analysis["reason_codes"]


def test_artifact_writer_creates_every_required_file(tmp_path: Path):
    frame = market_frame()
    pipeline = ForecastIntelligencePipeline(PipelineConfig(run_prophet=True, run_chronos=False, walk_forward_min_history=100, walk_forward_step=30, max_validation_origins=5, min_selection_samples=3), prophet_model=DeterministicModel(ModelName.PROPHET))
    result, validation, selection = pipeline.run(frame, ticker="AAPL", timeframe="5d", as_of=frame["timestamp"].iloc[-1].isoformat())
    paths = write_artifacts(tmp_path, result=result, validation=validation, ensemble_selection=selection)
    assert set(paths) == set(ARTIFACT_FILES)
    assert all(Path(path).exists() for path in paths.values())
    assert "AAPL" in (tmp_path / "final_prediction.json").read_text()
