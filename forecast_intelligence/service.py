from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Literal

import pandas as pd
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .gpt import OpenAIAnalysisClient
from .pipeline import ForecastIntelligencePipeline, PipelineConfig, build_gpt_payload
from .types import AnalysisMode, QuantileName


class HistoryRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    timestamp: datetime
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float
    volume: float | None = None


class ForecastRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: str = Field(min_length=1, max_length=12, pattern=r"^[A-Za-z][A-Za-z0-9.\-]{0,11}$")
    timeframe: Literal[
        "1_trading_day",
        "3_trading_days",
        "5_trading_days",
        "10_trading_days",
        "2_weeks",
        "1_month",
        "3_months",
    ]
    as_of: datetime
    history: list[HistoryRow] = Field(min_length=40, max_length=5000)
    run_prophet: bool = True
    run_chronos: bool = True
    chronos_finetune_mode: Literal["none", "lora", "full"] = "none"
    run_validation: bool = True
    max_validation_origins: int = Field(default=8, ge=1, le=50)

    @field_validator("ticker")
    @classmethod
    def normalize_ticker(cls, value: str) -> str:
        return value.strip().upper()


class AnalysisRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payload: dict[str, Any]
    reasoning_effort: Literal["none", "low", "medium", "high", "xhigh", "max"] = "medium"
    prompt_version: Literal["v1-minimal", "v1-adversarial", "v1-contextual"] = "v1-adversarial"
    fewshot_version: Literal["v1"] = "v1"


def _require_service_token(authorization: str | None = Header(default=None)) -> None:
    expected = os.environ.get("FORECAST_INTELLIGENCE_SERVICE_TOKEN", "").strip()
    if not expected:
        if os.environ.get("FORECAST_INTELLIGENCE_ALLOW_UNAUTHENTICATED", "").lower() == "true":
            return
        raise HTTPException(status_code=503, detail="service_auth_not_configured")
    supplied = (authorization or "").removeprefix("Bearer ").strip()
    import hmac

    if not supplied or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="unauthorized")


def _frame(rows: list[HistoryRow]) -> pd.DataFrame:
    frame = pd.DataFrame([row.model_dump() for row in rows])
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    for name in ("open", "high", "low", "close", "volume"):
        frame[name] = pd.to_numeric(frame[name], errors="coerce")
    frame = frame.dropna(subset=["timestamp", "close"])
    frame = frame[frame["close"] > 0].sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    if len(frame) < 40:
        raise HTTPException(status_code=400, detail="insufficient_point_in_time_history")
    return frame.reset_index(drop=True)


def _forecast_series(result: Any) -> list[dict[str, Any]]:
    selected = result.model_forecasts[result.selected_model.value]
    return [
        {
            "ds": point.timestamp,
            **{name.value.lower(): point.quantiles[name].value for name in QuantileName},
            "provenance": {name.value: point.quantiles[name].provenance for name in QuantileName},
        }
        for point in selected.points
    ]


app = FastAPI(title="Quantura Forecast Intelligence", version="1.0.0")


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "forecast-intelligence",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "prophet_configured": True,
        "chronos_checkpoint": os.environ.get("CHRONOS_CHECKPOINT", "amazon/chronos-2"),
    }


@app.post("/v1/forecast", dependencies=[Depends(_require_service_token)])
def forecast(request: ForecastRequest) -> dict[str, Any]:
    frame = _frame(request.history)
    cutoff = pd.Timestamp(request.as_of)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    frame = frame[frame["timestamp"] <= cutoff]
    if len(frame) < 40:
        raise HTTPException(status_code=400, detail="insufficient_as_of_history")
    config = PipelineConfig(
        run_prophet=request.run_prophet,
        run_chronos=request.run_chronos,
        chronos_checkpoint=os.environ.get("CHRONOS_CHECKPOINT", "amazon/chronos-2"),
        chronos_finetune_mode=request.chronos_finetune_mode,
        chronos_device_map=os.environ.get("CHRONOS_DEVICE_MAP", "cpu"),
        max_validation_origins=request.max_validation_origins,
        allow_statistical_baseline=False,
    )
    try:
        result, _validation, ensemble = ForecastIntelligencePipeline(config).run(
            frame,
            ticker=request.ticker,
            timeframe=request.timeframe,
            as_of=cutoff.isoformat(),
            mode=AnalysisMode.LIVE,
            run_validation=request.run_validation,
        )
    except Exception as exc:
        # Do not return provider internals, model paths, or upstream payloads.
        error_name = type(exc).__name__
        if error_name in {"ModelUnavailable", "ModuleNotFoundError", "ImportError"}:
            raise HTTPException(status_code=503, detail="numerical_model_unavailable") from exc
        raise HTTPException(status_code=422, detail="forecast_generation_failed") from exc

    payload = build_gpt_payload(result, retrieved_context=[], tool_disclosure=["quantitative_data_only"])
    return jsonable_encoder(
        {
            "ok": True,
            "result": result.to_dict(),
            "forecast_series": _forecast_series(result),
            "gpt_payload": payload,
            "ensemble_selection": asdict(ensemble) if ensemble else None,
        }
    )


@app.post("/v1/analyze", dependencies=[Depends(_require_service_token)])
def analyze(request: AnalysisRequest) -> dict[str, Any]:
    if not os.environ.get("OPENAI_API_KEY", "").strip():
        raise HTTPException(status_code=503, detail="openai_not_configured")
    try:
        client = OpenAIAnalysisClient(
            model=os.environ.get("FORECAST_GPT_MODEL", "gpt-5.6-luna"),
            prompt_version=request.prompt_version,
            fewshot_version=request.fewshot_version,
            max_gpt_calls=min(max(int(os.environ.get("MAX_GPT_CALLS", "1")), 1), 3),
        )
        result = client.analyze(request.payload, reasoning_effort=request.reasoning_effort)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail="analysis_service_unavailable") from exc
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail="invalid_analysis_output") from exc
    return {"ok": True, "analysis": jsonable_encoder(result), "model": client.model}
