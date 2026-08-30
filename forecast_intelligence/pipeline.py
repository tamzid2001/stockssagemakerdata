from __future__ import annotations

from dataclasses import asdict, dataclass, field
from statistics import mean
from typing import Any

import pandas as pd

from .calendar import resolve_horizon
from .ensemble import EnsembleSelection, combine_forecasts, select_ensemble_weight
from .indicators import point_in_time_indicators
from .metrics import EvaluationRow, validation_objective
from .models import Chronos2Model, ForecastModel, ModelUnavailable, ProphetModel, StatisticalBaselineModel
from .types import (
    AnalysisMode,
    ForecastResult,
    ModelForecast,
    ModelName,
    QuantileName,
    Scenario,
    percent_change,
)
from .walk_forward import WalkForwardResult, walk_forward_evaluate


@dataclass
class PipelineConfig:
    run_prophet: bool = True
    run_chronos: bool = True
    chronos_checkpoint: str = "amazon/chronos-2"
    chronos_finetune_mode: str = "none"
    chronos_device_map: str = "cpu"
    walk_forward_min_history: int = 120
    walk_forward_step: int = 10
    max_validation_origins: int = 12
    min_selection_samples: int = 5
    calendar_name: str = "XNYS"
    allow_statistical_baseline: bool = True
    prompt_version: str = "v1-adversarial"
    fewshot_version: str = "v1"
    tool_policy_version: str = "v1"
    rag_policy_version: str = "v1"
    reasoning_effort: str = "medium"
    gpt_model: str = "gpt-5.6-luna"


def _scenario(label: str, quantile: QuantileName, forecast: ModelForecast) -> Scenario:
    value = forecast.final.quantiles[quantile]
    return Scenario(
        label=label,
        quantile=quantile,
        target=value.value,
        dollar_change=value.value - forecast.current_price,
        percent_change=percent_change(value.value, forecast.current_price),
        target_date=forecast.target_date,
        provenance=value.provenance,
    )


def build_scenarios(forecast: ModelForecast) -> dict[str, Scenario]:
    return {
        "extreme_bear": _scenario("Extreme Bear", QuantileName.P1, forecast),
        "bear": _scenario("Bear", QuantileName.P25, forecast),
        "base": _scenario("Base", QuantileName.P50, forecast),
        "bull": _scenario("Bull", QuantileName.P75, forecast),
        "extreme_bull": _scenario("Extreme Bull", QuantileName.P99, forecast),
    }


def _range_summary(forecast: ModelForecast, low: QuantileName, high: QuantileName) -> dict[str, Any]:
    widths = [point.quantiles[high].value - point.quantiles[low].value for point in forecast.points]
    current = forecast.current_price
    change = ((widths[-1] / widths[0]) - 1) * 100 if widths[0] > 0 else 0.0
    direction = "expanding" if change > 5 else "contracting" if change < -5 else "stable"
    return {
        "lower": low.value,
        "upper": high.value,
        "start": widths[0],
        "final": widths[-1],
        "average": mean(widths),
        "percent_of_current": widths[-1] / current * 100,
        "change_percent": change,
        "direction": direction,
    }


def _agreement(forecasts: dict[ModelName, ModelForecast]) -> dict[str, Any]:
    medians = {model.value: forecast.final.quantiles[QuantileName.P50].value for model, forecast in forecasts.items()}
    if len(medians) < 2:
        return {"level": "weak", "median_targets": medians, "dispersion_pct": None, "reason": "Only one numerical model is available."}
    values = list(medians.values())
    midpoint = max(mean(values), 1e-9)
    dispersion = (max(values) - min(values)) / midpoint * 100
    level = "strong" if dispersion <= 2 else "moderate" if dispersion <= 5 else "weak" if dispersion <= 10 else "conflicting"
    return {"level": level, "median_targets": medians, "dispersion_pct": dispersion}


def _metrics_dict(result: WalkForwardResult) -> dict[str, Any]:
    return asdict(result.metrics)


class ForecastIntelligencePipeline:
    def __init__(
        self,
        config: PipelineConfig | None = None,
        *,
        prophet_model: ForecastModel | None = None,
        chronos_model: ForecastModel | None = None,
    ):
        config = config or PipelineConfig()
        self.config = config
        self.prophet_model = prophet_model or ProphetModel()
        self.chronos_model = chronos_model or Chronos2Model(
            checkpoint=config.chronos_checkpoint,
            device_map=config.chronos_device_map,
            finetune_mode=config.chronos_finetune_mode,
        )
        self.baseline_model = StatisticalBaselineModel()

    def _enabled_models(self) -> list[ForecastModel]:
        models: list[ForecastModel] = []
        if self.config.run_prophet:
            models.append(self.prophet_model)
        if self.config.run_chronos:
            models.append(self.chronos_model)
        return models

    def run(
        self,
        frame: pd.DataFrame,
        *,
        ticker: str,
        timeframe: str,
        as_of: str,
        mode: AnalysisMode = AnalysisMode.LIVE,
        run_validation: bool = True,
    ) -> tuple[ForecastResult, dict[str, WalkForwardResult], EnsembleSelection | None]:
        horizon = resolve_horizon(timeframe, as_of, calendar_name=self.config.calendar_name)
        indicators = point_in_time_indicators(frame, as_of=as_of)
        forecasts: dict[ModelName, ModelForecast] = {}
        validation: dict[str, WalkForwardResult] = {}
        unavailable: dict[str, str] = {}
        enabled = self._enabled_models()
        for model in enabled:
            try:
                forecasts[model.name] = model.forecast(frame, ticker=ticker, as_of=as_of, horizon=horizon)
                if run_validation:
                    validation[model.name.value] = walk_forward_evaluate(
                        frame,
                        model,
                        ticker=ticker,
                        timeframe=horizon.label,
                        min_history=self.config.walk_forward_min_history,
                        step=self.config.walk_forward_step,
                        max_origins=self.config.max_validation_origins,
                    )
                    forecasts[model.name].metrics = validation[model.name.value].metrics
            except (ModelUnavailable, ValueError) as exc:
                unavailable[model.name.value] = str(exc)

        if not forecasts and self.config.allow_statistical_baseline:
            forecasts[ModelName.BASELINE] = self.baseline_model.forecast(frame, ticker=ticker, as_of=as_of, horizon=horizon)
            if run_validation:
                validation[ModelName.BASELINE.value] = walk_forward_evaluate(
                    frame,
                    self.baseline_model,
                    ticker=ticker,
                    timeframe=horizon.label,
                    min_history=self.config.walk_forward_min_history,
                    step=self.config.walk_forward_step,
                    max_origins=self.config.max_validation_origins,
                )
                forecasts[ModelName.BASELINE].metrics = validation[ModelName.BASELINE.value].metrics
        if not forecasts:
            raise ModelUnavailable("No numerical forecasting model is available")

        ensemble_selection: EnsembleSelection | None = None
        if ModelName.PROPHET in forecasts and ModelName.CHRONOS in forecasts:
            prophet_validation = validation.get(ModelName.PROPHET.value)
            chronos_validation = validation.get(ModelName.CHRONOS.value)
            if prophet_validation and chronos_validation and len(prophet_validation.rows) == len(chronos_validation.rows) and len(prophet_validation.rows) >= self.config.min_selection_samples:
                ensemble_selection = select_ensemble_weight(prophet_validation.rows, chronos_validation.rows)
                ensemble = combine_forecasts(forecasts[ModelName.PROPHET], forecasts[ModelName.CHRONOS], prophet_weight=ensemble_selection.prophet_weight)
                ensemble_rows = [
                    EvaluationRow(
                        left.actual,
                        left.origin_price,
                        {
                            name: ensemble_selection.prophet_weight * left.quantiles[name] + ensemble_selection.chronos_weight * right.quantiles[name]
                            for name in QuantileName
                        },
                    )
                    for left, right in zip(prophet_validation.rows, chronos_validation.rows)
                ]
                from .metrics import evaluate_forecasts

                ensemble.metrics = evaluate_forecasts(ensemble_rows)
                forecasts[ModelName.ENSEMBLE] = ensemble

        eligible = [
            forecast for forecast in forecasts.values()
            if forecast.metrics.sample_size >= self.config.min_selection_samples
        ]
        if eligible:
            selected = min(eligible, key=lambda forecast: validation_objective(forecast.metrics))
            selection_metric = "validation_wql_plus_mase_plus_calibration_error"
            selection_reason = f"Lowest locked validation objective for {horizon.label} across {selected.metrics.sample_size} origins."
        else:
            priority = [ModelName.ENSEMBLE, ModelName.PROPHET, ModelName.CHRONOS, ModelName.BASELINE]
            selected = next(forecasts[name] for name in priority if name in forecasts)
            selection_metric = "availability_fallback_unvalidated"
            selection_reason = "Insufficient walk-forward samples; the highest-priority available numerical model is shown without a best-validated claim."

        scenarios = build_scenarios(selected)
        model_agreement = _agreement({name: value for name, value in forecasts.items() if name is not ModelName.ENSEMBLE})
        validation_payload = {
            name: {
                "metrics": _metrics_dict(result),
                "origins": result.origins,
                "failures": result.failures,
            }
            for name, result in validation.items()
        }
        if ModelName.ENSEMBLE in forecasts:
            validation_payload[ModelName.ENSEMBLE.value] = {"metrics": asdict(forecasts[ModelName.ENSEMBLE].metrics)}
        result = ForecastResult(
            ticker=ticker.upper(),
            mode=mode,
            as_of=pd.Timestamp(as_of).isoformat(),
            timeframe=horizon.label,
            target_date=horizon.target_date,
            current_price=selected.current_price,
            selected_model=selected.model,
            selection_metric=selection_metric,
            selection_reason=selection_reason,
            model_forecasts={name.value: forecast for name, forecast in forecasts.items()},
            scenarios=scenarios,
            indicators=indicators,
            model_agreement=model_agreement,
            validation=validation_payload,
            metadata={
                "calendar": horizon.calendar,
                "sessions": horizon.sessions,
                "frequency": horizon.frequency,
                "unavailable_models": unavailable,
                "ensemble_selection": asdict(ensemble_selection) if ensemble_selection else None,
                "prompt_version": self.config.prompt_version,
                "fewshot_version": self.config.fewshot_version,
                "tool_policy_version": self.config.tool_policy_version,
                "rag_policy_version": self.config.rag_policy_version,
                "gpt_model": self.config.gpt_model,
                "gpt_optimization_method": "prompt_fewshot_rag_tools",
            },
        )
        result.metadata["ranges"] = {
            "central": _range_summary(selected, QuantileName.P25, QuantileName.P75),
            "extreme": _range_summary(selected, QuantileName.P1, QuantileName.P99),
        }
        return result, validation, ensemble_selection


def build_gpt_payload(result: ForecastResult, *, retrieved_context: list[dict] | None = None, tool_disclosure: list[str] | None = None) -> dict[str, Any]:
    selected = result.model_forecasts[result.selected_model.value]
    final = selected.final
    selected_metrics = asdict(selected.metrics)
    calibration_values = list(selected.metrics.calibration_error.values())
    selected_metrics["mean_absolute_calibration_error"] = mean(abs(value) for value in calibration_values) if calibration_values else None
    return {
        "ticker": result.ticker,
        "as_of": result.as_of,
        "mode": result.mode.value,
        "timeframe": result.timeframe,
        "target_date": result.target_date,
        "current_price": result.current_price,
        "selected_forecast": {
            "model": result.selected_model.value,
            "selection_metric": result.selection_metric,
            "selection_reason": result.selection_reason,
            "quantiles": {
                name.value: {
                    "value": final.quantiles[name].value,
                    "provenance": final.quantiles[name].provenance,
                }
                for name in QuantileName
            },
            "validation_metrics": selected_metrics,
        },
        "models": {
            model_name: {
                "final_quantiles": {name.value: forecast.final.quantiles[name].value for name in QuantileName},
                "provenance": {name.value: forecast.final.quantiles[name].provenance for name in QuantileName},
                "validation_metrics": asdict(forecast.metrics),
            }
            for model_name, forecast in result.model_forecasts.items()
        },
        "scenarios": {key: asdict(value) for key, value in result.scenarios.items()},
        "ranges": result.metadata["ranges"],
        "indicators": result.indicators,
        "model_agreement": result.model_agreement,
        "retrieved_context": retrieved_context or [],
        "tool_disclosure": tool_disclosure or [],
        "versions": {
            "prompt": result.metadata["prompt_version"],
            "fewshot": result.metadata["fewshot_version"],
            "tool_policy": result.metadata["tool_policy_version"],
            "rag_policy": result.metadata["rag_policy_version"],
        },
    }
