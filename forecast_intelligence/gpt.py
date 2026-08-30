from __future__ import annotations

import json
import os
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Protocol


PROMPT_DIR = Path(__file__).resolve().parent / "prompts"
ALLOWED_SIGNALS = {"LONG", "SHORT", "HOLD"}


GPT_RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "overall_bias": {
            "type": "string",
            "enum": ["strongly_bullish", "moderately_bullish", "neutral_mixed", "moderately_bearish", "strongly_bearish"],
        },
        "signal": {"type": "string", "enum": ["LONG", "SHORT", "HOLD"]},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "forecast_distribution": {"type": "string"},
        "bull_case": {"$ref": "#/$defs/case"},
        "base_case": {"$ref": "#/$defs/case"},
        "bear_case": {"$ref": "#/$defs/case"},
        "extreme_bear_case": {"type": "string"},
        "extreme_bull_case": {"type": "string"},
        "uncertainty": {"type": "string"},
        "risk_level": {"type": "string", "enum": ["low", "medium", "high"]},
        "model_agreement": {"type": "string", "enum": ["strong", "moderate", "weak", "conflicting"]},
        "quantitative_evidence": {"type": "array", "items": {"type": "string"}, "maxItems": 12},
        "contextual_evidence": {"type": "array", "items": {"type": "string"}, "maxItems": 12},
        "conflicting_evidence": {"type": "array", "items": {"type": "string"}, "maxItems": 12},
        "reason_codes": {"type": "array", "items": {"type": "string"}, "maxItems": 16},
        "tool_disclosure": {"type": "array", "items": {"type": "string"}, "maxItems": 12},
    },
    "required": [
        "overall_bias", "signal", "confidence", "forecast_distribution", "bull_case", "base_case", "bear_case",
        "extreme_bear_case", "extreme_bull_case", "uncertainty", "risk_level", "model_agreement",
        "quantitative_evidence", "contextual_evidence", "conflicting_evidence", "reason_codes", "tool_disclosure",
    ],
    "$defs": {
        "case": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "target": {"type": "number"},
                "change_pct": {"type": "number"},
                "drivers": {"type": "array", "items": {"type": "string"}, "maxItems": 8},
            },
            "required": ["target", "change_pct", "drivers"],
        }
    },
}


class AnalysisClient(Protocol):
    def analyze(self, payload: dict[str, Any], *, reasoning_effort: str) -> dict[str, Any]: ...


def load_prompt(version: str) -> str:
    path = PROMPT_DIR / f"system-{version}.txt"
    if not path.exists():
        raise ValueError(f"unknown prompt version: {version}")
    return path.read_text(encoding="utf-8").strip()


def load_fewshots(version: str) -> list[dict[str, Any]]:
    path = PROMPT_DIR / f"fewshots-{version}.json"
    if not path.exists():
        raise ValueError(f"unknown few-shot version: {version}")
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError("few-shot prompt file must contain a list")
    return rows


def confidence_cap(payload: dict[str, Any]) -> float:
    agreement = str(payload.get("model_agreement", {}).get("level", "weak"))
    validation = payload.get("selected_forecast", {}).get("validation_metrics", {})
    sample_size = int(validation.get("sample_size") or 0)
    calibration = float(validation.get("mean_absolute_calibration_error") or 1.0)
    extreme_range_pct = float(payload.get("ranges", {}).get("extreme", {}).get("percent_of_current") or 100.0)
    cap = 0.90
    if agreement in {"weak", "conflicting"}:
        cap = min(cap, 0.62)
    if sample_size < 30:
        cap = min(cap, 0.58)
    elif sample_size < 100:
        cap = min(cap, 0.72)
    if calibration > 0.15:
        cap = min(cap, 0.60)
    if extreme_range_pct > 40:
        cap = min(cap, 0.64)
    return cap


def apply_output_guardrails(output: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    if output.get("signal") not in ALLOWED_SIGNALS:
        raise ValueError("GPT returned an invalid signal")
    scenarios = payload["scenarios"]
    expected = {"bear_case": scenarios["bear"], "base_case": scenarios["base"], "bull_case": scenarios["bull"]}
    guarded = dict(output)
    for key, scenario in expected.items():
        value = dict(guarded.get(key) or {})
        value["target"] = scenario["target"]
        value["change_pct"] = scenario["percent_change"]
        value["drivers"] = list(value.get("drivers") or [])[:8]
        guarded[key] = value
    guarded["confidence"] = min(max(float(guarded.get("confidence", 0)), 0.0), confidence_cap(payload))
    guarded["guardrails"] = {
        "targets_locked_to_numerical_forecast": True,
        "confidence_cap": confidence_cap(payload),
        "gpt_optimization_method": "prompt_fewshot_rag_tools",
    }
    return guarded


@dataclass
class OpenAIAnalysisClient:
    model: str = "gpt-5.6-luna"
    prompt_version: str = "v1-adversarial"
    fewshot_version: str = "v1"
    max_gpt_calls: int = 1
    _calls: int = 0

    def analyze(self, payload: dict[str, Any], *, reasoning_effort: str = "medium") -> dict[str, Any]:
        if self._calls >= self.max_gpt_calls:
            raise RuntimeError("MAX_GPT_CALLS budget exceeded")
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise RuntimeError("OpenAI SDK is not installed") from exc
        self._calls += 1
        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        input_items: list[dict[str, Any]] = []
        for example in load_fewshots(self.fewshot_version):
            input_items.extend(
                [
                    {"role": "user", "content": json.dumps(example["input"], separators=(",", ":"))},
                    {"role": "assistant", "content": json.dumps(example["output"], separators=(",", ":"))},
                ]
            )
        input_items.append({"role": "user", "content": json.dumps(payload, separators=(",", ":"), default=str)})
        response = client.responses.create(
            model=self.model,
            instructions=load_prompt(self.prompt_version),
            input=input_items,
            reasoning={"effort": reasoning_effort},
            text={
                "format": {
                    "type": "json_schema",
                    "name": "forecast_intelligence_analysis",
                    "strict": True,
                    "schema": GPT_RESPONSE_SCHEMA,
                }
            },
            prompt_cache_key=f"forecast-intelligence:{self.prompt_version}:{self.fewshot_version}",
        )
        parsed = json.loads(response.output_text)
        return apply_output_guardrails(parsed, payload)


class MockAnalysisClient:
    """Deterministic CI client; never makes a paid API call."""

    def analyze(self, payload: dict[str, Any], *, reasoning_effort: str = "medium") -> dict[str, Any]:
        scenarios = payload["scenarios"]
        base_change = float(scenarios["base"]["percent_change"])
        bias = "moderately_bullish" if base_change > 1 else "moderately_bearish" if base_change < -1 else "neutral_mixed"
        signal = "LONG" if base_change > 2 else "SHORT" if base_change < -2 else "HOLD"
        output = {
            "overall_bias": bias,
            "signal": signal,
            "confidence": 0.7,
            "forecast_distribution": "The complete numerical distribution and current-price position were evaluated.",
            "bull_case": {"target": 0, "change_pct": 0, "drivers": ["P75 numerical scenario"]},
            "base_case": {"target": 0, "change_pct": 0, "drivers": ["P50 median scenario"]},
            "bear_case": {"target": 0, "change_pct": 0, "drivers": ["P25 numerical scenario"]},
            "extreme_bear_case": "P1 is a probabilistic lower-tail scenario, not a guaranteed outcome.",
            "extreme_bull_case": "P99 is a probabilistic upper-tail scenario, not a guaranteed outcome.",
            "uncertainty": "Both P25-P75 and P1-P99 ranges were considered.",
            "risk_level": "medium",
            "model_agreement": payload.get("model_agreement", {}).get("level", "weak"),
            "quantitative_evidence": ["Validated numerical forecasts supplied the targets."],
            "contextual_evidence": [item.get("source", "context") for item in payload.get("retrieved_context", [])],
            "conflicting_evidence": ["Bull and bear cases remain probabilistic."],
            "reason_codes": ["NUMERICAL_TARGETS_LOCKED", "BOTH_SIDES_REVIEWED"],
            "tool_disclosure": list(payload.get("tool_disclosure", [])),
        }
        return apply_output_guardrails(output, payload)


def analysis_cache_key(payload: dict[str, Any], *, model: str, prompt_version: str, fewshot_version: str, tool_policy_version: str, rag_policy_version: str) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(f"{model}:{prompt_version}:{fewshot_version}:{tool_policy_version}:{rag_policy_version}:{canonical}".encode()).hexdigest()
