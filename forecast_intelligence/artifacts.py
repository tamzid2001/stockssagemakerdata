from __future__ import annotations

import json
import subprocess
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from .ensemble import EnsembleSelection
from .types import ForecastResult, QuantileName
from .walk_forward import WalkForwardResult


ARTIFACT_FILES = (
    "forecast_model_comparison.csv",
    "forecast_calibration.csv",
    "ensemble_weights.json",
    "prompt_evaluation.csv",
    "rag_ablation.csv",
    "tool_ablation.csv",
    "gpt_signals.csv",
    "trades.csv",
    "equity_curve.csv",
    "final_prediction.json",
    "final_analysis.json",
    "final_holdout_report.md",
    "run_metadata.json",
)


def git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, default=str, sort_keys=True) + "\n", encoding="utf-8")


def write_artifacts(
    output_dir: str | Path,
    *,
    result: ForecastResult,
    validation: dict[str, WalkForwardResult],
    ensemble_selection: EnsembleSelection | None,
    gpt_analysis: dict[str, Any] | None = None,
    trades: pd.DataFrame | None = None,
    equity_curve: pd.DataFrame | None = None,
    ablations: dict[str, list[dict[str, Any]]] | None = None,
    final_holdout: bool = False,
) -> dict[str, str]:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    comparison_rows = []
    calibration_rows = []
    for model_name, forecast in result.model_forecasts.items():
        metrics = asdict(forecast.metrics)
        comparison_rows.append({"model": model_name, **{key: value for key, value in metrics.items() if not isinstance(value, dict)}})
        for quantile in QuantileName:
            calibration_rows.append(
                {
                    "model": model_name,
                    "quantile": quantile.value,
                    "nominal": {"P1": 0.01, "P25": 0.25, "P50": 0.5, "P75": 0.75, "P99": 0.99}[quantile.value],
                    "observed": forecast.metrics.empirical_coverage.get(quantile.value),
                    "calibration_difference": forecast.metrics.calibration_error.get(quantile.value),
                }
            )
    pd.DataFrame(comparison_rows).to_csv(target / "forecast_model_comparison.csv", index=False)
    pd.DataFrame(calibration_rows).to_csv(target / "forecast_calibration.csv", index=False)
    _write_json(target / "ensemble_weights.json", asdict(ensemble_selection) if ensemble_selection else {"available": False})
    ablations = ablations or {}
    pd.DataFrame(ablations.get("prompt", [])).to_csv(target / "prompt_evaluation.csv", index=False)
    pd.DataFrame(ablations.get("rag", [])).to_csv(target / "rag_ablation.csv", index=False)
    pd.DataFrame(ablations.get("tools", [])).to_csv(target / "tool_ablation.csv", index=False)
    pd.DataFrame([gpt_analysis] if gpt_analysis else []).to_csv(target / "gpt_signals.csv", index=False)
    (trades if trades is not None else pd.DataFrame()).to_csv(target / "trades.csv", index=False)
    (equity_curve if equity_curve is not None else pd.DataFrame()).to_csv(target / "equity_curve.csv", index=False)
    _write_json(target / "final_prediction.json", result.to_dict())
    _write_json(target / "final_analysis.json", gpt_analysis or {"generated": False})
    holdout_label = "FINAL HOLDOUT — CONFIGURATION LOCKED" if final_holdout else "Development / validation run (final holdout not executed)"
    (target / "final_holdout_report.md").write_text(
        f"# {holdout_label}\n\nTicker: `{result.ticker}`\n\nTimeframe: `{result.timeframe}`\n\nSelected model: `{result.selected_model.value}`\n\nSimulated historical results do not guarantee future performance.\n",
        encoding="utf-8",
    )
    _write_json(
        target / "run_metadata.json",
        {
            **result.metadata,
            "ticker": result.ticker,
            "timeframe": result.timeframe,
            "as_of": result.as_of,
            "target_date": result.target_date,
            "git_commit": git_commit(),
            "final_holdout": final_holdout,
            "configuration_locked": final_holdout,
            "artifacts": list(ARTIFACT_FILES),
        },
    )
    return {name: str(target / name) for name in ARTIFACT_FILES}
