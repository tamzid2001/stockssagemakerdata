from __future__ import annotations

import json
import re
from typing import Any

from .models import SocialDraft, SocialSource
from .secrets import SecretResolver


def _normalize_hashtag(tag: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9]+", "", str(tag or "").strip())
    return clean[:28] if clean else ""


def _truncate(text: str, max_length: int) -> str:
    clean = re.sub(r"\s+", " ", str(text or "").strip())
    if len(clean) <= max_length:
        return clean
    shortened = clean[: max_length - 3].rstrip()
    return f"{shortened}..."


def _template_draft(source: SocialSource, channel_config: dict[str, Any], cta_url: str) -> tuple[str, str, tuple[str, ...]]:
    max_body_length = int(channel_config.get("maxBodyLength") or 280)
    max_hashtags = int(channel_config.get("maxHashtags") or 2)
    tags = tuple(filter(None, (_normalize_hashtag(tag) for tag in source.tags)))[:max_hashtags]
    hashtag_text = " ".join(f"#{tag}" for tag in tags)
    url = str(source.canonical_url or cta_url or "").strip()
    segments = [source.title.strip()]
    if source.summary.strip():
        segments.append(source.summary.strip())
    if hashtag_text:
        segments.append(hashtag_text)
    if url:
        segments.append(url)
    body = _truncate(" ".join(segment for segment in segments if segment), max_body_length)
    headline = _truncate(source.title.strip(), 120)
    return headline, body, tags


def _extract_json_object(text: str) -> dict[str, Any]:
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(0))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _ai_draft(
    *,
    source: SocialSource,
    channel_config: dict[str, Any],
    resolver: SecretResolver,
    objective: str,
    audience: str,
    tone: str,
    cta_url: str,
) -> tuple[str, str, tuple[str, ...], str]:
    api_key = resolver.get("OPENAI_API_KEY", "")
    if not api_key:
        return "", "", (), ""
    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        return "", "", (), ""

    channel = str(channel_config.get("channel") or "social").strip()
    max_body_length = int(channel_config.get("maxBodyLength") or 280)
    max_hashtags = int(channel_config.get("maxHashtags") or 2)
    model = str(channel_config.get("model") or "gpt-5-mini").strip()
    defaults = channel_config.get("defaults") or {}

    system_prompt = (
        "You are writing a production-ready social media post for Quantura. "
        "Return JSON only with keys headline, body, hashtags, cta. "
        f"Keep the body under {max_body_length} characters, keep language compliant and risk-aware, "
        "avoid promises or investment guarantees, and make the post feel native to the requested platform."
    )
    user_prompt = {
        "channel": channel,
        "objective": objective or defaults.get("objective") or "",
        "audience": audience or defaults.get("audience") or "",
        "tone": tone or defaults.get("tone") or "",
        "ctaUrl": cta_url,
        "source": {
            "kind": source.kind,
            "title": source.title,
            "summary": source.summary,
            "canonicalUrl": source.canonical_url,
            "tags": list(source.tags),
            "topic": source.topic,
        },
        "constraints": {
            "maxBodyLength": max_body_length,
            "maxHashtags": max_hashtags,
            "useNoMoreThanOneCTA": True,
            "includeRiskAwareLanguage": True,
        },
    }

    client = OpenAI(api_key=api_key)
    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=True)},
        ],
        max_output_tokens=400,
    )
    output_text = str(getattr(response, "output_text", "") or "").strip()
    payload = _extract_json_object(output_text)
    if not payload:
        return "", "", (), ""
    headline = _truncate(str(payload.get("headline") or source.title), 120)
    body = _truncate(str(payload.get("body") or ""), max_body_length)
    hashtags = tuple(
        tag
        for tag in (_normalize_hashtag(item) for item in (payload.get("hashtags") or []))
        if tag
    )[:max_hashtags]
    return headline, body, hashtags, model


def build_social_draft(
    *,
    channel_config: dict[str, Any],
    source: SocialSource,
    resolver: SecretResolver,
    objective: str,
    audience: str,
    tone: str,
    cta_url: str,
) -> SocialDraft:
    headline, body, hashtags, model_used = _ai_draft(
        source=source,
        channel_config=channel_config,
        resolver=resolver,
        objective=objective,
        audience=audience,
        tone=tone,
        cta_url=cta_url,
    )
    used_ai = bool(body and model_used)
    if not body:
        headline, body, hashtags = _template_draft(source, channel_config, cta_url)
    cta = str(channel_config.get("cta") or "Explore the workflow").strip()
    media_url = str(source.media_url or channel_config.get("defaultMediaUrl") or "").strip()
    return SocialDraft(
        channel=str(channel_config.get("channel") or "").strip(),
        source=source,
        headline=headline,
        body=body,
        hashtags=hashtags,
        cta=cta,
        cta_url=str(cta_url or source.canonical_url or "").strip(),
        media_url=media_url,
        used_ai=used_ai,
        model_used=model_used,
        metadata={"maxBodyLength": channel_config.get("maxBodyLength")},
    )
