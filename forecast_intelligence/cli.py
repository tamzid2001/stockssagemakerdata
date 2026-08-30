from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from .artifacts import write_artifacts
from .backtest import BacktestConfig, run_trade_backtest
from .gpt import MockAnalysisClient, OpenAIAnalysisClient
from .pipeline import ForecastIntelligencePipeline, PipelineConfig, build_gpt_payload
from .types import AnalysisMode, ModelName, QuantileName


def parse_bool(value: str) -> bool:
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean: {value}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run point-in-time Forecast Intelligence analysis.")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--timeframe", default="5_trading_days")
    parser.add_argument("--frequency", default="1d", choices=["1d"])
    parser.add_argument("--input-csv")
    parser.add_argument("--as-of")
    parser.add_argument("--output-dir", default="artifacts/forecast-intelligence")
    parser.add_argument("--run-prophet", type=parse_bool, default=True)
    parser.add_argument("--run-chronos", type=parse_bool, default=False)
    parser.add_argument("--chronos-finetune-mode", choices=["none", "lora", "full"], default="none")
    parser.add_argument("--run-gpt", type=parse_bool, default=False)
    parser.add_argument("--live-gpt", action="store_true", help="Use the paid OpenAI API instead of the deterministic CI client.")
    parser.add_argument("--enable-rag", type=parse_bool, default=False)
    parser.add_argument("--enable-tools", type=parse_bool, default=False)
    parser.add_argument("--reasoning-effort", choices=["none", "low", "medium", "high", "xhigh", "max"], default="medium")
    parser.add_argument("--prompt-version", default="v1-adversarial")
    parser.add_argument("--run-backtest", type=parse_bool, default=True)
    parser.add_argument("--run-final-holdout", type=parse_bool, default=False)
    parser.add_argument("--max-validation-origins", type=int, default=8)
    return parser.parse_args()


def load_market_data(args: argparse.Namespace) -> pd.DataFrame:
    if args.input_csv:
        raw = pd.read_csv(args.input_csv)
    else:
        import yfinance as yf

        raw = yf.download(args.ticker, period="5y", interval="1d", auto_adjust=False, progress=False, threads=False)
        if raw.empty:
            raise RuntimeError("No market history returned")
        raw = raw.reset_index()
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = [str(value[0]) for value in raw.columns]
    columns = {str(column).strip().lower().replace(" ", "_"): column for column in raw.columns}
    timestamp = columns.get("timestamp") or columns.get("date") or columns.get("datetime")
    close = columns.get("close") or columns.get("adj_close")
    if timestamp is None or close is None:
        raise ValueError("market data requires timestamp/date and close columns")
    out = pd.DataFrame({"timestamp": raw[timestamp], "close": raw[close]})
    for name in ("open", "high", "low", "volume"):
        source = columns.get(name)
        out[name] = raw[source] if source is not None else raw[close] if name != "volume" else 0
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    for name in ("open", "high", "low", "close", "volume"):
        out[name] = pd.to_numeric(out[name], errors="coerce")
    return out.dropna(subset=["timestamp", "close"]).sort_values("timestamp").reset_index(drop=True)


def build_backtest_signals(frame: pd.DataFrame, validation: dict[str, Any], selected_model: str) -> pd.Series:
    signals = pd.Series("HOLD", index=frame.index, dtype="object")
    evaluation = validation.get(selected_model)
    if evaluation is None:
        return signals
    indexed = {pd.Timestamp(value).isoformat(): index for index, value in enumerate(pd.to_datetime(frame["timestamp"], utc=True))}
    for origin, row in zip(evaluation.origins, evaluation.rows):
        index = indexed.get(pd.Timestamp(origin).isoformat())
        if index is None:
            continue
        median = row.quantiles[QuantileName.P50]
        signals.iloc[index] = "LONG" if median > row.origin_price * 1.005 else "SHORT" if median < row.origin_price * 0.995 else "HOLD"
    return signals


def main() -> int:
    args = parse_args()
    frame = load_market_data(args)
    as_of = args.as_of or pd.Timestamp(frame["timestamp"].iloc[-1]).isoformat()
    config = PipelineConfig(
        run_prophet=args.run_prophet,
        run_chronos=args.run_chronos,
        chronos_finetune_mode=args.chronos_finetune_mode,
        max_validation_origins=max(1, args.max_validation_origins),
        prompt_version=args.prompt_version,
        reasoning_effort=args.reasoning_effort,
    )
    pipeline = ForecastIntelligencePipeline(config)
    result, validation, ensemble_selection = pipeline.run(
        frame,
        ticker=args.ticker,
        timeframe=args.timeframe,
        as_of=as_of,
        mode=AnalysisMode.LIVE,
        run_validation=True,
    )
    gpt_analysis = None
    if args.run_gpt:
        payload = build_gpt_payload(
            result,
            retrieved_context=[],
            tool_disclosure=["quantitative_data_only"] if not args.enable_rag and not args.enable_tools else [],
        )
        if args.live_gpt:
            if not os.environ.get("OPENAI_API_KEY"):
                raise RuntimeError("OPENAI_API_KEY is required for --live-gpt")
            client = OpenAIAnalysisClient(prompt_version=args.prompt_version)
        else:
            client = MockAnalysisClient()
        gpt_analysis = client.analyze(payload, reasoning_effort=args.reasoning_effort)

    trades = pd.DataFrame()
    equity = pd.DataFrame()
    backtest_metrics: dict[str, Any] = {}
    if args.run_backtest:
        signals = build_backtest_signals(frame, validation, result.selected_model.value)
        trades, equity, backtest_metrics = run_trade_backtest(frame, signals, config=BacktestConfig())
        result.metadata["backtest"] = backtest_metrics
    if args.run_final_holdout:
        print("FINAL HOLDOUT — CONFIGURATION LOCKED")
    artifacts = write_artifacts(
        args.output_dir,
        result=result,
        validation=validation,
        ensemble_selection=ensemble_selection,
        gpt_analysis=gpt_analysis,
        trades=trades,
        equity_curve=equity,
        final_holdout=args.run_final_holdout,
    )
    print(json.dumps({
        "ticker": result.ticker,
        "timeframe": result.timeframe,
        "target_date": result.target_date,
        "selected_model": result.selected_model.value,
        "selection_metric": result.selection_metric,
        "artifacts": artifacts,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
