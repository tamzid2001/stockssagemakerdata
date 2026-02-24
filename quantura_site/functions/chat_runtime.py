from __future__ import annotations

import json
from typing import Any


def normalize_allowed_models(models: list[str] | tuple[str, ...] | None) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in models or []:
        model_id = str(raw or "").strip()
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        ordered.append(model_id)
    return ordered


def select_model_for_request(
    *,
    requested_model: str,
    allowed_models: list[str] | tuple[str, ...] | None,
    default_model: str,
) -> dict[str, Any]:
    allowed = normalize_allowed_models(allowed_models)
    fallback = str(default_model or "gpt-5-mini").strip() or "gpt-5-mini"
    if not allowed:
        allowed = [fallback]

    requested = str(requested_model or "").strip()
    allowed_set = set(allowed)
    selected = requested if requested in allowed_set else (fallback if fallback in allowed_set else allowed[0])
    return {
        "requested_model": requested,
        "selected_model": selected,
        "requested_allowed": bool(requested and requested in allowed_set),
        "allowed_models": allowed,
    }


def build_chat_prompt_messages(
    *,
    stable_prefix: str,
    language_label: str,
    ticker: str,
    question: str,
    context_payload: dict[str, Any],
) -> dict[str, str]:
    prefix = str(stable_prefix or "").strip()
    language = str(language_label or "English").strip() or "English"
    ticker_norm = str(ticker or "").strip().upper()
    query = str(question or "").strip()
    payload = context_payload if isinstance(context_payload, dict) else {}

    system_prompt = f"{prefix}\nRespond in {language}. Keep the language consistent in all sections."
    dynamic_user_prompt = (
        f"Ticker: {ticker_norm}\n"
        f"Question: {query}\n"
        f"Language preference: {language}\n"
        "Ticker context payload:\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )
    return {
        "system_prompt": system_prompt,
        "dynamic_user_prompt": dynamic_user_prompt,
    }
