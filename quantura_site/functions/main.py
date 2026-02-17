from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from io import BytesIO, StringIO
from statistics import NormalDist
from typing import Any

import requests

import firebase_admin
from firebase_admin import credentials, firestore, messaging as admin_messaging, storage as admin_storage
from firebase_functions import https_fn, scheduler_fn
from firebase_functions.options import set_global_options

try:
    from firebase_admin import remote_config as admin_remote_config  # type: ignore
except Exception:  # pragma: no cover - optional dependency until firebase-admin>=7.x
    admin_remote_config = None

set_global_options(max_instances=10)

SERVICE_ACCOUNT_PATH = os.environ.get(
    "SERVICE_ACCOUNT_PATH",
    os.path.join(os.path.dirname(__file__), "serviceAccountKey.json"),
)
STORAGE_BUCKET = (
    os.environ.get("STORAGE_BUCKET")
    or os.environ.get("FIREBASE_STORAGE_BUCKET")
    or "quantura-e2e3d.firebasestorage.app"
)
PUBLIC_ORIGIN = (os.environ.get("PUBLIC_ORIGIN") or "https://quantura-e2e3d.web.app").rstrip("/")
ADMIN_EMAIL = "tamzid257@gmail.com"
ALLOWED_STATUSES = {"pending", "in_progress", "fulfilled", "cancelled"}
CONTACT_REQUIRED_FIELDS = {"name", "email", "message"}
FORECAST_SERVICES = {"prophet", "ibm_timemixer"}
BACKTEST_SOURCE_FORMATS = {"python", "tradingview", "metatrader5", "tradelocker"}
REPORT_AGENT_BATCH_SIZE = max(1, min(int(os.environ.get("REPORT_AGENT_BATCH_SIZE", "8") or 8), 40))

ALPACA_API_BASE = os.environ.get("ALPACA_API_BASE", "https://paper-api.alpaca.markets")
ALPACA_DATA_BASE = os.environ.get("ALPACA_DATA_BASE", "https://data.alpaca.markets")
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY") or os.environ.get("ALPACAAPIKEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("ALPACASECRETKEY")

IBM_TIMEMIXER_MODEL_ID = os.environ.get("IBM_TIMEMIXER_MODEL_ID", "ibm-granite/granite-timeseries-ttm-r2")
IBM_TIMEMIXER_ENDPOINT = os.environ.get("IBM_TIMEMIXER_ENDPOINT", "").strip()
IBM_TIMEMIXER_API_KEY = os.environ.get("IBM_TIMEMIXER_API_KEY", "").strip()
HUGGINGFACEHUB_API_TOKEN = os.environ.get("HUGGINGFACEHUB_API_TOKEN", "").strip()

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
FCM_WEB_VAPID_KEY = os.environ.get("FCM_WEB_VAPID_KEY", "").strip()
STRIPE_PUBLIC_KEY = os.environ.get("STRIPE_PUBLIC_KEY", "").strip()
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
RISK_FREE_RATE = float(os.environ.get("RISK_FREE_RATE", "0.045") or 0.045)
TRENDING_URL = "https://query1.finance.yahoo.com/v1/finance/trending/US"
YAHOO_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"
DEFAULT_FORECAST_PRICE = 349
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
SOCIAL_CONTENT_MODEL = (os.environ.get("SOCIAL_CONTENT_MODEL") or "gpt-4o-mini").strip()
SOCIAL_AUTOMATION_ENABLED = str(os.environ.get("SOCIAL_AUTOMATION_ENABLED") or "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
SOCIAL_AUTOMATION_TIMEZONE = str(os.environ.get("SOCIAL_AUTOMATION_TIMEZONE") or "America/New_York").strip()
SOCIAL_DISPATCH_BATCH_SIZE = max(1, min(int(os.environ.get("SOCIAL_DISPATCH_BATCH_SIZE", "30") or 30), 100))
SOCIAL_DEFAULT_CTA_URL = str(os.environ.get("SOCIAL_DEFAULT_CTA_URL") or PUBLIC_ORIGIN).strip()
SOCIAL_POPULAR_CHANNELS = [
    "x",
    "linkedin",
    "facebook",
    "instagram",
    "threads",
    "reddit",
    "tiktok",
    "youtube",
    "pinterest",
]
SOCIAL_CHANNEL_WEBHOOK_ENV = {
    "x": "SOCIAL_WEBHOOK_X",
    "linkedin": "SOCIAL_WEBHOOK_LINKEDIN",
    "facebook": "SOCIAL_WEBHOOK_FACEBOOK",
    "instagram": "SOCIAL_WEBHOOK_INSTAGRAM",
    "threads": "SOCIAL_WEBHOOK_THREADS",
    "reddit": "SOCIAL_WEBHOOK_REDDIT",
    "tiktok": "SOCIAL_WEBHOOK_TIKTOK",
    "youtube": "SOCIAL_WEBHOOK_YOUTUBE",
    "pinterest": "SOCIAL_WEBHOOK_PINTEREST",
}
META_PIXEL_ID = str(os.environ.get("META_PIXEL_ID") or "1643823927053003").strip()
META_CAPI_ACCESS_TOKEN = str(os.environ.get("META_CAPI_ACCESS_TOKEN") or "").strip()
META_GRAPH_API_VERSION = str(os.environ.get("META_GRAPH_API_VERSION") or "v21.0").strip() or "v21.0"
META_ALLOWED_ORIGINS = {
    PUBLIC_ORIGIN,
    "https://quantura-e2e3d.web.app",
    "https://quantura-e2e3d.firebaseapp.com",
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
}

if not firebase_admin._apps:
    options: dict[str, Any] = {}
    if STORAGE_BUCKET:
        options["storageBucket"] = STORAGE_BUCKET
    if os.path.exists(SERVICE_ACCOUNT_PATH):
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred, options or None)
    else:
        # IMPORTANT: initialize_app() expects (credential, options). When running in Cloud
        # Functions, we rely on Application Default Credentials and only pass options.
        firebase_admin.initialize_app(options=options or None)

db = firestore.client()

_REMOTE_CONFIG_CACHE: dict[str, Any] = {"template": None, "loadedAt": 0.0}

DEFAULT_REMOTE_CONFIG: dict[str, str] = {
    "welcome_message": "Welcome to Quantura",
    "watchlist_enabled": "true",
    "forecast_prophet_enabled": "true",
    "forecast_timemixer_enabled": "true",
    "enable_social_leaderboard": "true",
    "forecast_model_primary": "Quantura Horizon",
    "promo_banner_text": "",
    "maintenance_mode": "false",
    "volatility_threshold": "0.05",
    "ai_usage_tiers": json.dumps(
        {
            "free": {
                "allowed_models": ["gpt-4o-mini", "gemini-1.5-flash"],
                "daily_limit": 5,
                "volatility_alerts": False,
            },
            "premium": {
                "allowed_models": ["gpt-4o", "claude-3-5-sonnet", "gemini-1.5-pro", "o1-preview"],
                "daily_limit": 50,
                "volatility_alerts": True,
            },
        }
    ),
    "push_notifications_enabled": "true",
    "webpush_vapid_key": "",
    "stripe_checkout_enabled": "true",
    "stripe_public_key": "",
    "holiday_promo": "false",
    "backtesting_enabled": "true",
    "backtesting_free_daily_limit": "1",
    "backtesting_pro_daily_limit": "25",
}


def _get_remote_config_template(max_age_seconds: int = 300) -> Any | None:
    """Loads a Remote Config ServerTemplate and keeps it cached between invocations."""
    if admin_remote_config is None:
        return None
    now = time.time()
    cached = _REMOTE_CONFIG_CACHE.get("template")
    loaded_at = float(_REMOTE_CONFIG_CACHE.get("loadedAt") or 0.0)

    if cached is None:
        try:
            cached = admin_remote_config.init_server_template(default_config=DEFAULT_REMOTE_CONFIG)
            _REMOTE_CONFIG_CACHE["template"] = cached
        except Exception:
            return None

    if loaded_at and (now - loaded_at) < max_age_seconds:
        return cached

    try:
        cached.load()
        _REMOTE_CONFIG_CACHE["loadedAt"] = now
    except Exception:
        # Keep the old template cache (if any).
        pass
    return cached


def _remote_config_context(
    req: https_fn.CallableRequest | None,
    token: dict[str, Any] | None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Builds a stable Remote Config evaluation context.

    Note: Python ServerTemplate percent conditions expect `randomization_id`.
    """
    meta = meta or {}
    token = token or {}
    context: dict[str, Any] = {}

    randomization_id = ""
    if req and req.auth:
        randomization_id = req.auth.uid
    if not randomization_id and isinstance(meta.get("sessionId"), str) and meta.get("sessionId"):
        # For unauthenticated calls, fall back to a stable per-device session id.
        randomization_id = meta["sessionId"]
    if randomization_id:
        context["randomization_id"] = randomization_id

    email = token.get("email")
    if isinstance(email, str) and email:
        context["email"] = email.lower()
    if isinstance(meta.get("pagePath"), str) and meta.get("pagePath"):
        context["page_path"] = meta["pagePath"]
    if isinstance(meta.get("timezone"), str) and meta.get("timezone"):
        context["timezone"] = meta["timezone"]
    return context


def _remote_config_param(key: str, default: str = "", context: dict[str, Any] | None = None) -> str:
    template = _get_remote_config_template()
    if template is None:
        return default
    try:
        config = template.evaluate(context or {})
        value = config.get_string(key)
        if value is None:
            return default
        value_str = str(value)
        return value_str if value_str != "" else default
    except Exception:
        return default


def _remote_config_bool(key: str, default: bool = False, context: dict[str, Any] | None = None) -> bool:
    template = _get_remote_config_template()
    if template is None:
        return default
    try:
        config = template.evaluate(context or {})
        return bool(config.get_boolean(key))
    except Exception:
        raw = _remote_config_param(key, "", context=context)
        if not raw:
            return default
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        return default


def _remote_config_int(key: str, default: int = 0, context: dict[str, Any] | None = None) -> int:
    raw = _remote_config_param(key, str(default), context=context)
    if raw == "":
        return default
    try:
        return int(float(str(raw).strip()))
    except Exception:
        return default


def _remote_config_float(key: str, default: float = 0.0, context: dict[str, Any] | None = None) -> float:
    raw = _remote_config_param(key, str(default), context=context)
    if raw == "":
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def _remote_config_json_param(key: str, default: dict[str, Any], context: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = _remote_config_param(key, "", context=context)
    if not raw:
        return default
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else default
    except Exception:
        return default


def _raise_structured_error(
    code: https_fn.FunctionsErrorCode,
    error_key: str,
    detail: str,
    extras: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {"error": error_key, "detail": detail}
    if extras:
        payload.update(extras)
    raise https_fn.HttpsError(code, detail, payload)


def _get_ai_usage_tiers(context: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback = {
        "free": {
            "allowed_models": ["gpt-4o-mini", "gemini-1.5-flash"],
            "daily_limit": 5,
            "volatility_alerts": False,
        },
        "premium": {
            "allowed_models": ["gpt-4o", "claude-3-5-sonnet", "gemini-1.5-pro", "o1-preview"],
            "daily_limit": 50,
            "volatility_alerts": True,
        },
    }
    parsed = _remote_config_json_param("ai_usage_tiers", fallback, context=context)
    if not isinstance(parsed, dict):
        return fallback
    return parsed


def _resolve_ai_tier(
    uid: str,
    token: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    tiers = _get_ai_usage_tiers(context=context)
    is_premium = token.get("email") == ADMIN_EMAIL or _is_paid_user(uid)
    tier_key = "premium" if is_premium else "free"
    tier = tiers.get(tier_key) if isinstance(tiers.get(tier_key), dict) else {}
    if not tier:
        tier = {"allowed_models": [], "daily_limit": 5, "volatility_alerts": bool(is_premium)}
    return tier_key, tier


def _yahoo_headers() -> dict[str, str]:
    # Yahoo endpoints frequently rate-limit requests without a browser-like UA.
    return {
        "User-Agent": "Mozilla/5.0 (Quantura; +https://quantura-e2e3d.web.app)",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }


def _force_remove_second_line(csv_text: str) -> str:
    lines = csv_text.splitlines(True)
    if len(lines) > 1:
        del lines[1]
    return "".join(lines)


def _social_channel_webhooks() -> dict[str, str]:
    out: dict[str, str] = {}
    for channel, env_key in SOCIAL_CHANNEL_WEBHOOK_ENV.items():
        out[channel] = str(os.environ.get(env_key) or "").strip()
    return out


def _normalize_social_channels(raw: Any) -> list[str]:
    if raw is None:
        return list(SOCIAL_POPULAR_CHANNELS)
    if isinstance(raw, str):
        parts = [part.strip().lower() for part in raw.split(",")]
    elif isinstance(raw, list):
        parts = [str(part).strip().lower() for part in raw]
    else:
        parts = []
    channels = [channel for channel in parts if channel in SOCIAL_POPULAR_CHANNELS]
    if not channels:
        return list(SOCIAL_POPULAR_CHANNELS)
    return list(dict.fromkeys(channels))


def _as_utc_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.astimezone(timezone.utc) if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if hasattr(raw, "to_datetime"):
        try:
            dt = raw.to_datetime()  # Firestore Timestamp
            if isinstance(dt, datetime):
                return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    try:
        text = str(raw).strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _extract_json_object(raw_text: str) -> dict[str, Any]:
    text = str(raw_text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    fenced = re.search(r"```(?:json)?\\s*(\\{.*\\})\\s*```", text, flags=re.DOTALL)
    if fenced:
        try:
            parsed = json.loads(fenced.group(1))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _default_social_copy(
    topic: str,
    objective: str,
    audience: str,
    tone: str,
    channels: list[str],
    posts_per_channel: int,
    cta_url: str,
) -> dict[str, list[dict[str, Any]]]:
    base_objective = objective.strip() or "Drive qualified leads to Quantura"
    base_audience = audience.strip() or "active investors and data-driven operators"
    base_tone = tone.strip() or "confident, concise, practical"
    today = datetime.now(timezone.utc).date().isoformat()
    out: dict[str, list[dict[str, Any]]] = {}

    for channel in channels:
        channel_posts: list[dict[str, Any]] = []
        for idx in range(posts_per_channel):
            headline = f"{topic} insight #{idx + 1}"
            body = (
                f"{topic}: clear signal, clear execution. "
                f"We built this for {base_audience}. "
                f"Objective: {base_objective}. "
                f"Tone: {base_tone}."
            )
            channel_posts.append(
                {
                    "headline": headline,
                    "body": body,
                    "hashtags": ["#Quantura", "#Stocks", "#DataScience", f"#{channel.capitalize()}"],
                    "cta": "Explore the platform",
                    "ctaUrl": cta_url,
                    "suggestedPostTime": f"{today}T14:{10 + idx:02d}:00Z",
                }
            )
        out[channel] = channel_posts
    return out


def _generate_social_copy_with_openai(
    *,
    topic: str,
    objective: str,
    audience: str,
    tone: str,
    channels: list[str],
    posts_per_channel: int,
    cta_url: str,
) -> tuple[dict[str, list[dict[str, Any]]], str]:
    if not OPENAI_API_KEY:
        drafts = _default_social_copy(topic, objective, audience, tone, channels, posts_per_channel, cta_url)
        return drafts, "template_fallback"

    system_prompt = (
        "You are a social media strategist for a financial analytics company. "
        "Write concise, compliant, high-conversion posts. "
        "Avoid guaranteed-return language. Keep content useful and brand-safe. "
        "Return strict JSON only."
    )
    user_prompt = (
        "Generate social drafts with this exact JSON shape: "
        '{"channels":{"x":[{"headline":"","body":"","hashtags":[],"cta":"","ctaUrl":"","suggestedPostTime":""}]}}. '
        f"Topic: {topic}. Objective: {objective}. Audience: {audience}. Tone: {tone}. "
        f"Channels: {', '.join(channels)}. Posts per channel: {posts_per_channel}. "
        f"Use CTA URL: {cta_url}. Keep each body optimized for the channel and under typical limits."
    )

    payload = {
        "model": SOCIAL_CONTENT_MODEL,
        "temperature": 0.7,
        "max_tokens": 1200,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
    }

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=35,
        )
        response.raise_for_status()
        body = response.json()
        content = (
            body.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "{}")
        )
        parsed = _extract_json_object(content)
        channels_obj = parsed.get("channels") if isinstance(parsed, dict) else None
        if not isinstance(channels_obj, dict):
            raise ValueError("Model response missing channels object.")

        normalized: dict[str, list[dict[str, Any]]] = {}
        for channel in channels:
            rows = channels_obj.get(channel) if isinstance(channels_obj, dict) else None
            if not isinstance(rows, list):
                rows = []
            mapped: list[dict[str, Any]] = []
            for row in rows[:posts_per_channel]:
                if not isinstance(row, dict):
                    continue
                hashtags_raw = row.get("hashtags")
                if isinstance(hashtags_raw, list):
                    hashtags = [str(tag).strip() for tag in hashtags_raw if str(tag).strip()]
                elif isinstance(hashtags_raw, str):
                    hashtags = [tag.strip() for tag in hashtags_raw.split(",") if tag.strip()]
                else:
                    hashtags = []
                mapped.append(
                    {
                        "headline": str(row.get("headline") or "").strip(),
                        "body": str(row.get("body") or "").strip(),
                        "hashtags": hashtags,
                        "cta": str(row.get("cta") or "Learn more").strip(),
                        "ctaUrl": str(row.get("ctaUrl") or cta_url).strip() or cta_url,
                        "suggestedPostTime": str(row.get("suggestedPostTime") or "").strip(),
                    }
                )
            if mapped:
                normalized[channel] = mapped

        if not normalized:
            raise ValueError("No valid posts returned from model.")

        for channel in channels:
            if channel not in normalized:
                normalized[channel] = _default_social_copy(
                    topic,
                    objective,
                    audience,
                    tone,
                    [channel],
                    posts_per_channel,
                    cta_url,
                ).get(channel, [])
        return normalized, SOCIAL_CONTENT_MODEL
    except Exception:
        drafts = _default_social_copy(topic, objective, audience, tone, channels, posts_per_channel, cta_url)
        return drafts, "template_fallback"


def _enqueue_social_posts(
    *,
    campaign_id: str,
    user_id: str,
    user_email: str,
    channels: list[str],
    drafts: dict[str, list[dict[str, Any]]],
    scheduled_for: datetime,
    meta: dict[str, Any],
) -> dict[str, Any]:
    created_ids: list[str] = []
    total = 0
    scheduled_utc = scheduled_for.astimezone(timezone.utc)
    for channel in channels:
        posts = drafts.get(channel) if isinstance(drafts, dict) else None
        if not isinstance(posts, list):
            continue
        for rank, post in enumerate(posts):
            if not isinstance(post, dict):
                continue
            body = str(post.get("body") or "").strip()
            headline = str(post.get("headline") or "").strip()
            if not body and not headline:
                continue
            doc = {
                "campaignId": campaign_id,
                "userId": user_id,
                "userEmail": user_email,
                "platform": channel,
                "headline": headline,
                "body": body,
                "hashtags": post.get("hashtags") if isinstance(post.get("hashtags"), list) else [],
                "cta": str(post.get("cta") or "").strip(),
                "ctaUrl": str(post.get("ctaUrl") or SOCIAL_DEFAULT_CTA_URL).strip(),
                "scheduledFor": scheduled_utc,
                "status": "queued",
                "attempts": 0,
                "orderIndex": rank,
                "createdAt": firestore.SERVER_TIMESTAMP,
                "updatedAt": firestore.SERVER_TIMESTAMP,
                "meta": meta or {},
            }
            ref = db.collection("social_queue").document()
            ref.set(doc)
            created_ids.append(ref.id)
            total += 1
    return {"count": total, "queueIds": created_ids}


def _post_to_social_channel(
    *,
    platform: str,
    body: str,
    headline: str,
    hashtags: list[str],
    cta: str,
    cta_url: str,
    campaign_id: str,
    queue_id: str,
) -> dict[str, Any]:
    webhooks = _social_channel_webhooks()
    webhook = str(webhooks.get(platform) or "").strip()
    if not webhook:
        return {"ok": False, "pendingCredentials": True, "error": f"Missing webhook for channel '{platform}'."}

    tags = [str(tag).strip() for tag in hashtags if str(tag).strip()]
    rendered_text = body.strip()
    if headline.strip():
        rendered_text = f"{headline.strip()}\n\n{rendered_text}".strip()
    if tags:
        rendered_text = f"{rendered_text}\n\n{' '.join(tags)}".strip()
    if cta.strip() and cta_url.strip():
        rendered_text = f"{rendered_text}\n\n{cta.strip()}: {cta_url.strip()}".strip()

    payload = {
        "platform": platform,
        "text": rendered_text,
        "headline": headline.strip(),
        "body": body.strip(),
        "hashtags": tags,
        "cta": cta.strip(),
        "ctaUrl": cta_url.strip(),
        "campaignId": campaign_id,
        "queueId": queue_id,
    }
    response = requests.post(webhook, json=payload, timeout=20)
    if response.status_code >= 400:
        return {"ok": False, "pendingCredentials": False, "error": f"{response.status_code} {response.text}"}
    external_id = ""
    try:
        response_body = response.json()
        external_id = str(response_body.get("id") or response_body.get("postId") or "").strip()
    except Exception:
        external_id = ""
    return {"ok": True, "externalId": external_id, "statusCode": response.status_code}


def _dispatch_due_social_posts(*, max_posts: int, trigger: str, actor_uid: str = "") -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    snapshot = (
        db.collection("social_queue")
        .where("status", "in", ["queued", "retry"])
        .limit(max(1, min(max_posts * 4, 200)))
        .stream()
    )

    due_docs: list[tuple[str, dict[str, Any]]] = []
    for doc in snapshot:
        data = doc.to_dict() or {}
        scheduled_for = _as_utc_datetime(data.get("scheduledFor"))
        if scheduled_for is None or scheduled_for <= now_utc:
            due_docs.append((doc.id, data))
        if len(due_docs) >= max_posts:
            break

    posted = 0
    waiting_credentials = 0
    failed = 0
    retried = 0
    processed_ids: list[str] = []

    for queue_id, item in due_docs:
        platform = str(item.get("platform") or "").strip().lower()
        attempts = int(item.get("attempts") or 0)
        result = _post_to_social_channel(
            platform=platform,
            body=str(item.get("body") or ""),
            headline=str(item.get("headline") or ""),
            hashtags=item.get("hashtags") if isinstance(item.get("hashtags"), list) else [],
            cta=str(item.get("cta") or ""),
            cta_url=str(item.get("ctaUrl") or SOCIAL_DEFAULT_CTA_URL),
            campaign_id=str(item.get("campaignId") or ""),
            queue_id=queue_id,
        )

        ref = db.collection("social_queue").document(queue_id)
        if result.get("ok"):
            ref.set(
                {
                    "status": "posted",
                    "attempts": attempts + 1,
                    "postedAt": firestore.SERVER_TIMESTAMP,
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                    "externalId": result.get("externalId") or "",
                    "lastError": "",
                },
                merge=True,
            )
            posted += 1
        elif result.get("pendingCredentials"):
            ref.set(
                {
                    "status": "waiting_credentials",
                    "attempts": attempts + 1,
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                    "lastError": str(result.get("error") or "Missing credentials"),
                },
                merge=True,
            )
            waiting_credentials += 1
        else:
            next_attempts = attempts + 1
            exhausted = next_attempts >= 3
            ref.set(
                {
                    "status": "failed" if exhausted else "retry",
                    "attempts": next_attempts,
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                    "lastError": str(result.get("error") or "Publish failed"),
                },
                merge=True,
            )
            if exhausted:
                failed += 1
            else:
                retried += 1
        processed_ids.append(queue_id)

    db.collection("social_dispatch_logs").add(
        {
            "trigger": trigger,
            "actorUid": actor_uid,
            "scheduledAt": firestore.SERVER_TIMESTAMP,
            "processedCount": len(processed_ids),
            "postedCount": posted,
            "waitingCredentialsCount": waiting_credentials,
            "retryCount": retried,
            "failedCount": failed,
            "queueIds": processed_ids,
        }
    )

    return {
        "processed": len(processed_ids),
        "posted": posted,
        "waitingCredentials": waiting_credentials,
        "retry": retried,
        "failed": failed,
        "trigger": trigger,
    }


def _require_auth(req: https_fn.CallableRequest) -> dict[str, Any]:
    if req.auth is None:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.UNAUTHENTICATED,
            "Sign in is required to perform this action.",
        )
    return req.auth.token or {}


def _require_admin(token: dict[str, Any]) -> None:
    if token.get("email") != ADMIN_EMAIL:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.PERMISSION_DENIED,
            "Admin access required.",
        )


def _collaborator_snapshot(workspace_id: str, uid: str):
    return (
        db.collection("users")
        .document(workspace_id)
        .collection("collaborators")
        .document(uid)
        .get()
    )


def _can_read_workspace(workspace_id: str, uid: str, token: dict[str, Any]) -> bool:
    if token.get("email") == ADMIN_EMAIL:
        return True
    if workspace_id == uid:
        return True
    shared = _collaborator_snapshot(workspace_id, uid)
    return bool(shared.exists)


def _can_edit_workspace(workspace_id: str, uid: str, token: dict[str, Any]) -> bool:
    if token.get("email") == ADMIN_EMAIL:
        return True
    if workspace_id == uid:
        return True
    shared = _collaborator_snapshot(workspace_id, uid)
    if not shared.exists:
        return False
    role = str((shared.to_dict() or {}).get("role") or "viewer").strip().lower()
    return role == "editor"


def _require_workspace_access(workspace_id: str, uid: str, token: dict[str, Any]) -> None:
    if not _can_read_workspace(workspace_id, uid, token):
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.PERMISSION_DENIED,
            "Workspace access required.",
        )


def _require_workspace_editor(workspace_id: str, uid: str, token: dict[str, Any]) -> None:
    if not _can_edit_workspace(workspace_id, uid, token):
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.PERMISSION_DENIED,
            "Editor access required for this workspace.",
        )


def _alpaca_headers() -> dict[str, str]:
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "Missing Alpaca API credentials.",
        )
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Content-Type": "application/json",
    }


def _stripe_module():
    try:
        import stripe  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "Stripe SDK is not available in this deployment.",
        ) from exc

    if not STRIPE_SECRET_KEY:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "Stripe is not configured (missing STRIPE_SECRET_KEY).",
        )
    stripe.api_key = STRIPE_SECRET_KEY
    return stripe


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def _meta_hash_sha256(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _meta_normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits if len(digits) >= 7 else ""


def _meta_header(req: https_fn.CallableRequest, key: str) -> str:
    raw_request = getattr(req, "raw_request", None)
    headers = getattr(raw_request, "headers", None)
    if headers is None:
        return ""
    try:
        return str(headers.get(key, "") or "").strip()
    except Exception:
        return ""


def _meta_client_ip(req: https_fn.CallableRequest, data: dict[str, Any]) -> str:
    explicit = str(data.get("clientIpAddress") or data.get("client_ip_address") or "").strip()
    if explicit:
        return explicit.split(",")[0].strip()
    forwarded = _meta_header(req, "x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    raw_request = getattr(req, "raw_request", None)
    remote_addr = getattr(raw_request, "remote_addr", "")
    return str(remote_addr or "").strip()


def _meta_build_user_data(req: https_fn.CallableRequest, data: dict[str, Any]) -> dict[str, Any]:
    user_data: dict[str, Any] = {}

    user_agent = str(data.get("userAgent") or data.get("user_agent") or _meta_header(req, "user-agent") or "").strip()
    client_ip = _meta_client_ip(req, data)
    if user_agent:
        user_data["client_user_agent"] = user_agent[:512]
    if client_ip:
        user_data["client_ip_address"] = client_ip

    email = str(data.get("email") or "").strip()
    if not email and getattr(req, "auth", None) and isinstance(req.auth.token, dict):
        email = str(req.auth.token.get("email") or "").strip()
    email_hash = _meta_hash_sha256(email)
    if email_hash:
        user_data["em"] = [email_hash]

    phone = _meta_normalize_phone(data.get("phone"))
    phone_hash = _meta_hash_sha256(phone)
    if phone_hash:
        user_data["ph"] = [phone_hash]

    external_id = str(data.get("externalId") or "").strip()
    if not external_id and getattr(req, "auth", None):
        external_id = str(getattr(req.auth, "uid", "") or "").strip()
    external_id_hash = _meta_hash_sha256(external_id)
    if external_id_hash:
        user_data["external_id"] = [external_id_hash]

    fbc = str(data.get("fbc") or "").strip()
    fbp = str(data.get("fbp") or "").strip()
    if fbc:
        user_data["fbc"] = fbc[:256]
    if fbp:
        user_data["fbp"] = fbp[:256]

    return user_data


def _meta_build_custom_data(params: Any) -> dict[str, Any]:
    if not isinstance(params, dict):
        return {}
    out: dict[str, Any] = {}
    allowed_keys = [
        "currency",
        "value",
        "content_name",
        "content_category",
        "search_string",
        "ticker",
        "workspace_id",
        "order_id",
        "status",
    ]
    for key in allowed_keys:
        if key not in params:
            continue
        value = params.get(key)
        if value is None:
            continue
        if key == "value":
            numeric = _safe_float(value)
            if numeric is None:
                continue
            out[key] = round(float(numeric), 6)
            continue
        if isinstance(value, bool):
            out[key] = value
            continue
        if isinstance(value, (int, float)):
            out[key] = value
            continue
        if isinstance(value, str):
            text = value.strip()
            if text:
                out[key] = text[:256]
    return out


def _audit_event(uid: str, email: str | None, event_type: str, payload: dict[str, Any]) -> None:
    db.collection("user_events").add(
        {
            "userId": uid,
            "userEmail": email,
            "eventType": event_type,
            "payload": payload,
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )


def _token_doc_id(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _active_notification_tokens_for_user(user_id: str) -> list[str]:
    docs = (
        db.collection("notification_tokens")
        .where("userId", "==", user_id)
        .where("active", "==", True)
        .limit(500)
        .stream()
    )
    tokens: list[str] = []
    for doc in docs:
        token_value = (doc.to_dict() or {}).get("token")
        if isinstance(token_value, str) and token_value:
            tokens.append(token_value)
    return list(dict.fromkeys(tokens))


def _send_push_tokens(tokens: list[str], title: str, body: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    tokens = [str(token).strip() for token in tokens if str(token).strip()]
    if not tokens:
        return {"successCount": 0, "failureCount": 0, "failed": [], "staleTokenHashes": []}

    payload_data = {str(key): str(value) for key, value in (data or {}).items() if value is not None}
    target = payload_data.get("url") or "/dashboard"
    link = target if target.startswith("http") else f"{PUBLIC_ORIGIN}{target}"
    icon = f"{PUBLIC_ORIGIN}/assets/quantura-icon.svg"
    message = admin_messaging.MulticastMessage(
        tokens=tokens[:500],
        notification=admin_messaging.Notification(title=title, body=body),
        data=payload_data,
        webpush=admin_messaging.WebpushConfig(
            notification=admin_messaging.WebpushNotification(
                title=title,
                body=body,
                icon=icon,
                badge=icon,
            ),
            fcm_options=admin_messaging.WebpushFCMOptions(link=link),
            headers={"Urgency": "high"},
        ),
    )

    try:
        response = admin_messaging.send_each_for_multicast(message)
    except Exception as exc:
        # Avoid surfacing an opaque "internal error" to the caller. We still audit via _notify_user().
        return {
            "successCount": 0,
            "failureCount": len(tokens[:500]),
            "failed": [{"tokenHash": _token_doc_id(t), "error": str(exc)} for t in tokens[:5]],
            "staleTokenHashes": [],
            "error": str(exc),
        }
    failed: list[dict[str, str]] = []
    stale_token_hashes: list[str] = []

    for idx, resp in enumerate(response.responses):
        if resp.success:
            continue
        token = tokens[idx]
        error_text = str(resp.exception or "unknown_error")
        failed.append({"tokenHash": _token_doc_id(token), "error": error_text})
        normalized = error_text.lower()
        if (
            "unregistered" in normalized
            or "registration-token-not-registered" in normalized
            or "invalid registration token" in normalized
            or "requested entity was not found" in normalized
        ):
            stale_token_hashes.append(_token_doc_id(token))

    return {
        "successCount": response.success_count,
        "failureCount": response.failure_count,
        "failed": failed,
        "staleTokenHashes": stale_token_hashes,
    }


def _notify_user(
    user_id: str,
    user_email: str | None,
    title: str,
    body: str,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tokens = _active_notification_tokens_for_user(user_id)
    result = _send_push_tokens(tokens, title=title, body=body, data=data)
    for stale_hash in result.get("staleTokenHashes", []):
        db.collection("notification_tokens").document(str(stale_hash)).set(
            {
                "active": False,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
    _audit_event(
        user_id,
        user_email,
        "push_notification_attempted",
        {
            "title": title,
            "successCount": result.get("successCount", 0),
            "failureCount": result.get("failureCount", 0),
        },
    )
    return result


def _serialize_for_firestore(value: Any) -> Any:
    try:
        import numpy as np  # type: ignore

        if isinstance(value, (np.floating, np.integer)):
            return value.item()
        if isinstance(value, np.ndarray):
            return [_serialize_for_firestore(item) for item in value.tolist()]
    except Exception:
        pass

    try:
        import pandas as pd  # type: ignore

        if isinstance(value, pd.Timestamp):
            return value.isoformat()
    except Exception:
        pass

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, list):
        return [_serialize_for_firestore(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_for_firestore(item) for key, item in value.items()}
    return value


def _trending_snapshots(tickers: list[str], max_rows: int = 18) -> dict[str, dict[str, Any]]:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

    tickers = [str(t).upper().strip() for t in (tickers or []) if str(t).strip()]
    tickers = list(dict.fromkeys([t for t in tickers if t]))[: max(1, int(max_rows or 18))]
    if not tickers:
        return {}

    symbols = " ".join(tickers)
    try:
        frame = yf.download(
            symbols,
            period="14d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
            auto_adjust=False,
        )
    except Exception:
        frame = pd.DataFrame()

    if frame.empty:
        return {}

    snapshots: dict[str, dict[str, Any]] = {}

    def _snap_from_close(series: pd.Series) -> dict[str, Any] | None:
        close = pd.to_numeric(series, errors="coerce").dropna()
        if close.empty:
            return None
        last = float(close.iloc[-1])
        prev = float(close.iloc[-2]) if len(close) >= 2 else None
        change = round(last - prev, 4) if prev else None
        change_pct = round((last - prev) / prev * 100.0, 4) if prev else None
        return {"lastClose": round(last, 4), "prevClose": round(prev, 4) if prev else None, "change": change, "changePct": change_pct}

    if isinstance(frame.columns, pd.MultiIndex):
        # shape: (field, ticker) or (ticker, field)
        level0 = list(frame.columns.get_level_values(0))
        is_ticker_level0 = any(t in set(level0) for t in tickers)
        for ticker in tickers:
            try:
                sub = frame[ticker] if is_ticker_level0 else frame.xs(ticker, axis=1, level=1)
            except Exception:
                continue
            if isinstance(sub.columns, pd.MultiIndex):
                sub.columns = sub.columns.get_level_values(-1)
            if "Close" not in sub.columns:
                continue
            snap = _snap_from_close(sub["Close"])
            if snap:
                snapshots[ticker] = snap
        return snapshots

    if "Close" in frame.columns and len(tickers) == 1:
        snap = _snap_from_close(frame["Close"])
        if snap:
            snapshots[tickers[0]] = snap
    return snapshots


def _parse_quantiles(raw: Any) -> list[float]:
    if raw is None:
        return [0.1, 0.5, 0.9]

    values: list[float] = []
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if not parts:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Quantiles must be comma-delimited values between 0 and 1.")
        for part in parts:
            try:
                values.append(float(part))
            except Exception:
                raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, f"Invalid quantile value: {part}")
    elif isinstance(raw, (list, tuple)):
        if not raw:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "At least one quantile is required.")
        for item in raw:
            try:
                values.append(float(item))
            except Exception:
                raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, f"Invalid quantile value: {item}")
    else:
        try:
            values = [float(raw)]
        except Exception:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid quantile value.")

    cleaned: list[float] = []
    seen: set[int] = set()
    for q in values:
        if not (0.0 < float(q) < 1.0):
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Quantiles must be between 0 and 1 (exclusive).")
        key = int(round(float(q) * 10000))
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(float(q))

    if len(cleaned) > 12:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Too many quantiles requested (max 12).")

    return cleaned


def _load_history(ticker: str, start: str | None, interval: str) -> pd.DataFrame:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

    period = "730d" if interval == "1h" else "10y"
    frame = yf.download(
        ticker,
        start=start or None,
        period=None if start else period,
        interval=interval,
        progress=False,
        auto_adjust=False,
    )
    if frame.empty:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "No market data found for ticker.")

    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)

    if "Close" not in frame.columns:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Close prices unavailable for ticker.")

    frame = frame.dropna(subset=["Close"])
    if frame.empty:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "No valid close values for ticker.")

    return frame


def _latest_close_prices(tickers: list[str]) -> dict[str, float]:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

    tickers = [str(t).upper().strip() for t in (tickers or []) if str(t).strip()]
    tickers = list(dict.fromkeys([t for t in tickers if t]))
    if not tickers:
        return {}

    symbols = " ".join(tickers)
    try:
        frame = yf.download(
            symbols,
            period="7d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
            auto_adjust=False,
        )
    except Exception:
        frame = pd.DataFrame()

    prices: dict[str, float] = {}
    if frame.empty:
        return prices

    if isinstance(frame.columns, pd.MultiIndex):
        # shape: (field, ticker) or (ticker, field) depending on yfinance version
        level0 = list(frame.columns.get_level_values(0))
        level1 = list(frame.columns.get_level_values(1))
        is_ticker_level0 = any(t in set(level0) for t in tickers)
        for ticker in tickers:
            try:
                sub = frame[ticker] if is_ticker_level0 else frame.xs(ticker, axis=1, level=1)
            except Exception:
                continue
            if isinstance(sub.columns, pd.MultiIndex):
                sub.columns = sub.columns.get_level_values(-1)
            if "Close" not in sub.columns:
                continue
            close = pd.to_numeric(sub["Close"], errors="coerce").dropna()
            if close.empty:
                continue
            prices[ticker] = float(close.iloc[-1])
        return prices

    if "Close" in frame.columns and len(tickers) == 1:
        close = pd.to_numeric(frame["Close"], errors="coerce").dropna()
        if not close.empty:
            prices[tickers[0]] = float(close.iloc[-1])
    return prices


def _generate_quantile_forecast(
    close_series: pd.Series,
    horizon: int,
    quantiles: list[float],
    interval: str,
) -> dict[str, Any]:
    import numpy as np  # type: ignore
    import pandas as pd  # type: ignore

    quantiles = sorted({float(q) for q in (quantiles or []) if 0 < float(q) < 1})
    if not quantiles:
        quantiles = [0.5]

    horizon = max(1, min(horizon, 365 if interval == "1d" else 240))

    values = close_series.astype(float).dropna().values
    if len(values) < 15:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "Not enough history to generate forecast.",
        )

    log_returns = np.diff(np.log(values))
    recent = log_returns[-min(len(log_returns), 252) :]
    drift = float(np.mean(recent[-min(len(recent), 40) :])) if len(recent) else 0.0
    vol = float(np.std(recent[-min(len(recent), 120) :])) if len(recent) else 0.01
    vol = max(vol, 0.0008)

    rng = np.random.default_rng(42)
    sims = rng.normal(loc=drift, scale=vol, size=(1500, horizon)).cumsum(axis=1)
    sim_prices = values[-1] * np.exp(sims)

    freq = "H" if interval == "1h" else "B"
    dates = pd.date_range(close_series.index[-1], periods=horizon + 1, freq=freq)[1:]

    forecast_rows: list[dict[str, Any]] = []
    for idx, ts in enumerate(dates):
        row = {"ds": ts.isoformat()}
        for quantile in quantiles:
            row[f"q{int(round(quantile * 100)):02d}"] = round(float(np.quantile(sim_prices[:, idx], quantile)), 4)
        forecast_rows.append(row)

    ref_q = min(quantiles, key=lambda q: abs(q - 0.5)) if quantiles else 0.5
    median_key = f"q{int(round(ref_q * 100)):02d}"
    median_path = np.array([row.get(median_key, values[-1]) for row in forecast_rows], dtype=float)
    last_actual = float(values[-1])
    mae_recent = float(np.mean(np.abs(np.diff(values[-min(len(values), 90) :])))) if len(values) > 2 else 0.0

    return {
        "engine": "statistical_fallback",
        "status": "completed",
        "serviceMessage": "Fallback Monte Carlo quantile model used.",
        "forecastRows": forecast_rows,
        "metrics": {
            "lastClose": round(last_actual, 4),
            "mae": round(mae_recent, 4),
            "horizon": horizon,
            "medianEnd": round(float(median_path[-1]), 4),
            "drift": round(drift, 6),
            "volatility": round(vol, 6),
            "historyPoints": int(len(values)),
            "historyStart": str(close_series.index[0].isoformat()) if len(close_series.index) else "",
            "historyEnd": str(close_series.index[-1].isoformat()) if len(close_series.index) else "",
        },
    }


def _run_prophet_engine(close_series: pd.Series, horizon: int, quantiles: list[float], interval: str) -> dict[str, Any]:
    try:
        from prophet import Prophet  # type: ignore
    except Exception:
        return _generate_quantile_forecast(close_series, horizon, quantiles, interval)

    import numpy as np  # type: ignore
    import pandas as pd  # type: ignore

    forecast_core = _generate_quantile_forecast(close_series, horizon, quantiles, interval)
    try:
        df = pd.DataFrame({"ds": close_series.index.tz_localize(None), "y": close_series.values.astype(float)})
        is_hourly = interval == "1h"
        model = Prophet(
            daily_seasonality=is_hourly,
            weekly_seasonality=True,
            yearly_seasonality=not is_hourly,
            changepoint_prior_scale=0.08,
            seasonality_prior_scale=12.0,
            interval_width=0.8,
        )
        model.add_country_holidays(country_name="US")
        if not is_hourly:
            model.add_seasonality(name="monthly", period=30.5, fourier_order=5)

        model.fit(df)

        # In-sample diagnostics (tail window) so users can compare engines on fit quality.
        try:
            in_sample = model.predict(df[["ds"]])
            tail = min(len(df), 90 if interval != "1h" else 240)
            if tail >= 5:
                actual = df["y"].to_numpy(dtype=float)
                yhat = in_sample["yhat"].to_numpy(dtype=float)
                abs_err = np.abs(actual - yhat)
                mae = float(np.mean(abs_err[-tail:]))
                rmse = float(np.sqrt(np.mean((actual[-tail:] - yhat[-tail:]) ** 2)))
                denom = np.maximum(np.abs(actual[-tail:]), 1e-9)
                mape = float(np.mean(np.abs((actual[-tail:] - yhat[-tail:]) / denom)))

                lower = in_sample["yhat_lower"].to_numpy(dtype=float)
                upper = in_sample["yhat_upper"].to_numpy(dtype=float)
                coverage = float(np.mean((actual[-tail:] >= lower[-tail:]) & (actual[-tail:] <= upper[-tail:])))

                forecast_core["metrics"].update(
                    {
                        "mae": round(mae, 4),
                        "rmse": round(rmse, 4),
                        "mape": round(mape, 6),
                        "coverage10_90": round(coverage, 4),
                        "historyPoints": int(len(df)),
                        "historyStart": str(df["ds"].iloc[0].isoformat()) if len(df) else "",
                        "historyEnd": str(df["ds"].iloc[-1].isoformat()) if len(df) else "",
                        "diagnosticWindow": int(tail),
                    }
                )
        except Exception:
            pass

        freq = "H" if is_hourly else "B"
        periods = max(1, min(horizon, 365 if not is_hourly else 240))
        future = model.make_future_dataframe(periods=periods, freq=freq, include_history=False)
        forecast = model.predict(future)

        normal = NormalDist()
        z_80 = normal.inv_cdf(0.9)
        if z_80 == 0:
            z_80 = 1.28155

        rows: list[dict[str, Any]] = []
        quantiles = sorted({float(q) for q in (quantiles or []) if 0 < float(q) < 1})
        if not quantiles:
            quantiles = [0.5]

        for _, row in forecast.iterrows():
            yhat = float(row["yhat"])
            lower = float(row["yhat_lower"])
            upper = float(row["yhat_upper"])
            sigma = max((upper - lower) / (2 * z_80), 1e-6)
            out_row: dict[str, Any] = {"ds": row["ds"].isoformat()}
            for q in quantiles:
                z = normal.inv_cdf(q)
                out_row[f"q{int(round(q * 100)):02d}"] = round(yhat + sigma * z, 4)
            rows.append(out_row)

        forecast_core["engine"] = "prophet"
        forecast_core["serviceMessage"] = "Forecast generated with Quantura Horizon, quantiles derived from model uncertainty."
        forecast_core["forecastRows"] = rows
        ref_q = min(quantiles, key=lambda q: abs(q - 0.5)) if quantiles else 0.5
        ref_key = f"q{int(round(ref_q * 100)):02d}"
        forecast_core["metrics"]["medianEnd"] = rows[-1].get(ref_key) if rows else forecast_core["metrics"].get("medianEnd")

        return forecast_core
    except Exception:
        # Prophet is best-effort: keep the product usable by returning the fallback model output.
        return forecast_core


def _run_timemixer_engine(close_series: pd.Series, horizon: int, quantiles: list[float], interval: str) -> dict[str, Any]:
    payload = {
        "model_id": IBM_TIMEMIXER_MODEL_ID,
        "history": [round(float(value), 6) for value in close_series.tail(2048).tolist()],
        "horizon": int(horizon),
        "quantiles": quantiles,
        "interval": interval,
    }

    headers = {"Content-Type": "application/json"}
    if IBM_TIMEMIXER_API_KEY:
        headers["Authorization"] = f"Bearer {IBM_TIMEMIXER_API_KEY}"

    if IBM_TIMEMIXER_ENDPOINT:
        try:
            response = requests.post(IBM_TIMEMIXER_ENDPOINT, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            body = response.json()
            if isinstance(body, dict) and isinstance(body.get("forecastRows"), list):
                result = _generate_quantile_forecast(close_series, horizon, quantiles, interval)
                result["engine"] = "ibm_timemixer_endpoint"
                result["serviceMessage"] = "Forecast generated by configured IBM TimeMixer endpoint."
                result["forecastRows"] = body["forecastRows"]
                metrics = body.get("metrics") or {}
                result["metrics"].update({k: _serialize_for_firestore(v) for k, v in metrics.items()})
                return result
        except Exception:
            pass

    if HUGGINGFACEHUB_API_TOKEN:
        try:
            hf_headers = {
                "Authorization": f"Bearer {HUGGINGFACEHUB_API_TOKEN}",
                "Content-Type": "application/json",
            }
            hf_response = requests.post(
                f"https://api-inference.huggingface.co/models/{IBM_TIMEMIXER_MODEL_ID}",
                headers=hf_headers,
                json={"inputs": payload},
                timeout=45,
            )
            if hf_response.status_code < 400:
                body = hf_response.json()
                if isinstance(body, dict) and isinstance(body.get("forecastRows"), list):
                    result = _generate_quantile_forecast(close_series, horizon, quantiles, interval)
                    result["engine"] = "ibm_timemixer_hf"
                    result["serviceMessage"] = "Forecast generated via IBM TimeMixer Hugging Face endpoint."
                    result["forecastRows"] = body["forecastRows"]
                    return result
        except Exception:
            pass

    result = _generate_quantile_forecast(close_series, horizon, quantiles, interval)
    result["engine"] = "ibm_timemixer_proxy"
    result["serviceMessage"] = (
        "IBM TimeMixer service selected (model ibm-granite/granite-timeseries-ttm-r2). "
        "Proxy fallback model executed because no external TimeMixer endpoint is configured."
    )
    return result


def _run_forecast_service(
    service: str,
    close_series: pd.Series,
    horizon: int,
    quantiles: list[float],
    interval: str,
) -> dict[str, Any]:
    if service == "ibm_timemixer":
        return _run_timemixer_engine(close_series, horizon, quantiles, interval)
    return _run_prophet_engine(close_series, horizon, quantiles, interval)


def _forecast_preview_rows(rows: list[dict[str, Any]], max_rows: int = 12) -> list[dict[str, Any]]:
    return rows[:max_rows]


def _forecast_quantile_entries(rows: list[dict[str, Any]]) -> list[tuple[float, str]]:
    if not rows:
        return []
    keys = set()
    for row in rows[: min(len(rows), 200)]:
        for key in (row or {}).keys():
            if re.match(r"^q\d\d$", str(key)):
                keys.add(str(key))
    entries: list[tuple[float, str]] = []
    for key in sorted(keys):
        try:
            q = int(key[1:]) / 100.0
            entries.append((q, key))
        except Exception:
            continue
    return sorted(entries, key=lambda item: item[0])


def _build_forecast_trade_rationale(
    service: str,
    horizon: int,
    metrics: dict[str, Any] | None,
    forecast_rows: list[dict[str, Any]] | None,
) -> str:
    metrics = metrics or {}
    rows = forecast_rows or []
    last_close = _safe_float(metrics.get("lastClose")) or 0.0
    median_end = _safe_float(metrics.get("medianEnd"))
    drift = _safe_float(metrics.get("drift")) or 0.0
    coverage = _safe_float(metrics.get("coverage10_90"))
    volatility = _safe_float(metrics.get("volatility"))

    implied_return = None
    if last_close > 0 and median_end is not None:
        implied_return = (median_end - last_close) / last_close

    trend_phrase = "balanced trend profile"
    if implied_return is not None and implied_return > 0.08:
        trend_phrase = "strong upward trend profile"
    elif implied_return is not None and implied_return > 0:
        trend_phrase = "moderately positive trend profile"
    elif implied_return is not None and implied_return < -0.05:
        trend_phrase = "defensive/negative trend profile"
    elif drift > 0:
        trend_phrase = "positive drift profile"

    service_label = "Quantura Horizon" if str(service).strip().lower() == "prophet" else "forecast model"
    sentence_one = (
        f"{service_label} indicates a {trend_phrase} over the next {int(horizon)} periods "
        f"with median-path projection anchored around recent price behavior."
    )

    coverage_text = "coverage unavailable"
    if coverage is not None:
        coverage_text = f"{coverage * 100:.1f}% coverage in the 10-90% band"
    vol_text = "volatility regime unavailable"
    if volatility is not None:
        vol_text = f"volatility near {volatility * 100:.2f}%"

    sentence_two = f"Confidence framing is based on {coverage_text} and {vol_text}."
    if rows:
        sentence_two += " Decision trigger should be tied to the median path versus lower-band support."
    return f"{sentence_one} {sentence_two}"


def _extract_forecast_key_levels(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    entries = _forecast_quantile_entries(rows)
    if not rows or not entries:
        return {"support": None, "median": None, "resistance": None}
    last_row = rows[-1] or {}
    low_key = entries[0][1]
    high_key = entries[-1][1]
    mid_key = min(entries, key=lambda item: abs(item[0] - 0.5))[1]
    return {
        "support": _safe_float(last_row.get(low_key)),
        "median": _safe_float(last_row.get(mid_key)),
        "resistance": _safe_float(last_row.get(high_key)),
    }


def _generate_forecast_chart_png(forecast_doc: dict[str, Any]) -> bytes:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # type: ignore
    import pandas as pd  # type: ignore

    rows = forecast_doc.get("forecastRows") if isinstance(forecast_doc.get("forecastRows"), list) else []
    if not rows:
        raise ValueError("No forecast rows are available to chart.")

    entries = _forecast_quantile_entries(rows)
    if not entries:
        raise ValueError("No quantile columns detected in forecast rows.")

    low_key = entries[0][1]
    high_key = entries[-1][1]
    mid_key = min(entries, key=lambda item: abs(item[0] - 0.5))[1]

    x_vals = []
    for idx, row in enumerate(rows):
        ds = row.get("ds")
        if isinstance(ds, str):
            try:
                x_vals.append(pd.to_datetime(ds))
                continue
            except Exception:
                pass
        x_vals.append(pd.Timestamp.utcnow() + pd.Timedelta(days=idx))

    low = [float(row.get(low_key) or 0.0) for row in rows]
    high = [float(row.get(high_key) or 0.0) for row in rows]
    median = [float(row.get(mid_key) or 0.0) for row in rows]

    fig = plt.figure(figsize=(11.2, 5.2), dpi=170, facecolor="#0f172a")
    ax = fig.add_subplot(111)
    ax.set_facecolor("#0f172a")
    ax.plot(x_vals, median, color="#3b82f6", linewidth=2.2, label=f"Median ({mid_key})")
    ax.fill_between(x_vals, low, high, color="#10b981", alpha=0.18, label=f"Band {low_key}-{high_key}")
    ax.plot(x_vals, low, color="#ef4444", linewidth=1.2, alpha=0.9, label=f"Lower ({low_key})")
    ax.plot(x_vals, high, color="#10b981", linewidth=1.2, alpha=0.9, label=f"Upper ({high_key})")

    ticker = str(forecast_doc.get("ticker") or "Ticker")
    service = str(forecast_doc.get("service") or "Forecast")
    service_label = "Quantura Horizon" if service.lower() == "prophet" else service
    ax.set_title(f"{ticker} • {service_label}", color="#f8fafc", fontsize=13, fontweight="bold")
    ax.grid(alpha=0.22, color="#93c5fd", linewidth=0.5)
    ax.tick_params(colors="#e2e8f0")
    for spine in ax.spines.values():
        spine.set_color("#334155")
    legend = ax.legend(loc="upper left", frameon=False, fontsize=8)
    for txt in legend.get_texts():
        txt.set_color("#e2e8f0")

    fig.tight_layout()
    out = BytesIO()
    fig.savefig(out, format="png", bbox_inches="tight", transparent=True)
    plt.close(fig)
    return out.getvalue()


def _render_forecast_report_html(
    forecast_doc: dict[str, Any],
    rationale: str,
    chart_png_b64: str,
) -> str:
    ticker = str(forecast_doc.get("ticker") or "Ticker")
    horizon = int(forecast_doc.get("horizon") or 0)
    interval = str(forecast_doc.get("interval") or "1d")
    created = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    metrics = forecast_doc.get("metrics") if isinstance(forecast_doc.get("metrics"), dict) else {}
    key_levels = _extract_forecast_key_levels(forecast_doc.get("forecastRows") or [])
    service = str(forecast_doc.get("service") or "prophet")
    service_label = "Quantura Horizon" if service.lower() == "prophet" else service

    rows = [
        ("Last Close", metrics.get("lastClose")),
        ("Median End", metrics.get("medianEnd")),
        ("Coverage 10-90", metrics.get("coverage10_90")),
        ("Support", key_levels.get("support")),
        ("Resistance", key_levels.get("resistance")),
    ]
    metric_rows = "".join(
        f"<tr><td>{label}</td><td>{value if value is not None else '—'}</td></tr>"
        for label, value in rows
    )

    return f"""
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <style>
      body {{
        margin: 0;
        font-family: Inter, Roboto, sans-serif;
        background: #0f172a;
        color: #e2e8f0;
        padding: 28px;
      }}
      .wrap {{
        border: 1px solid rgba(148, 163, 184, 0.35);
        border-radius: 18px;
        padding: 22px;
        background: linear-gradient(180deg, rgba(15, 23, 42, 0.98) 0%, rgba(30, 41, 59, 0.92) 100%);
      }}
      .brand {{
        font-weight: 800;
        letter-spacing: 0.08em;
        color: #93c5fd;
        margin-bottom: 8px;
      }}
      h1 {{
        margin: 0 0 4px;
        font-size: 24px;
      }}
      .meta {{
        margin: 0 0 16px;
        color: #cbd5e1;
        font-size: 13px;
      }}
      .mono {{
        font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, monospace;
      }}
      img {{
        width: 100%;
        border-radius: 12px;
        border: 1px solid rgba(148, 163, 184, 0.35);
        background: rgba(15, 23, 42, 0.6);
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        margin-top: 14px;
      }}
      td {{
        padding: 8px 10px;
        border-bottom: 1px solid rgba(148, 163, 184, 0.25);
        font-size: 13px;
      }}
      td:first-child {{
        color: #93c5fd;
      }}
      .rationale {{
        margin-top: 16px;
        background: rgba(30, 41, 59, 0.7);
        border: 1px solid rgba(59, 130, 246, 0.28);
        border-radius: 12px;
        padding: 12px;
        line-height: 1.5;
      }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="brand">QUANTURA EXECUTIVE BRIEF</div>
      <h1>{ticker} • {service_label}</h1>
      <div class="meta">Generated: {created} • Horizon: {horizon} • Interval: {interval}</div>
      <img src="data:image/png;base64,{chart_png_b64}" alt="Forecast chart" />
      <table>
        {metric_rows}
      </table>
      <div class="rationale"><strong>AI Rationale:</strong> {rationale}</div>
    </div>
  </body>
</html>
""".strip()


def _generate_forecast_pdf_bytes(html: str, fallback_title: str, rationale: str) -> bytes:
    try:
        from weasyprint import HTML  # type: ignore

        return HTML(string=html, base_url=PUBLIC_ORIGIN).write_pdf()
    except Exception:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # type: ignore

        fig = plt.figure(figsize=(11.0, 8.5), dpi=170, facecolor="#ffffff")
        ax = fig.add_subplot(111)
        ax.axis("off")
        ax.text(0.02, 0.94, "Quantura Executive Brief", fontsize=16, fontweight="bold")
        ax.text(0.02, 0.89, fallback_title, fontsize=12)
        ax.text(0.02, 0.83, rationale, fontsize=10, wrap=True)
        out = BytesIO()
        fig.savefig(out, format="pdf", bbox_inches="tight")
        plt.close(fig)
        return out.getvalue()


def _generate_forecast_pptx_bytes(
    forecast_doc: dict[str, Any],
    chart_png: bytes,
    rationale: str,
) -> bytes:
    from pptx import Presentation  # type: ignore
    from pptx.util import Inches, Pt  # type: ignore

    ticker = str(forecast_doc.get("ticker") or "Ticker")
    created = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    metrics = forecast_doc.get("metrics") if isinstance(forecast_doc.get("metrics"), dict) else {}
    rows = forecast_doc.get("forecastRows") if isinstance(forecast_doc.get("forecastRows"), list) else []
    key_levels = _extract_forecast_key_levels(rows)

    prs = Presentation()

    # Slide 1: Title.
    slide = prs.slides.add_slide(prs.slide_layouts[5])
    slide.shapes.title.text = f"{ticker} • Quantura Horizon Brief"
    subtitle = slide.shapes.add_textbox(Inches(0.8), Inches(1.6), Inches(11.5), Inches(1.2))
    tf = subtitle.text_frame
    tf.text = f"Date: {created}\nCurrent Price: {metrics.get('lastClose', '—')}"
    tf.paragraphs[0].font.size = Pt(20)

    # Slide 2: Analysis chart + key levels.
    slide2 = prs.slides.add_slide(prs.slide_layouts[5])
    slide2.shapes.title.text = "Forecast Analysis"
    slide2.shapes.add_picture(BytesIO(chart_png), Inches(0.6), Inches(1.2), width=Inches(8.4))
    box = slide2.shapes.add_textbox(Inches(9.1), Inches(1.4), Inches(3.0), Inches(3.8))
    t2 = box.text_frame
    t2.text = "Key Levels"
    p = t2.add_paragraph()
    p.text = f"Support: {key_levels.get('support') if key_levels.get('support') is not None else '—'}"
    p.level = 1
    p = t2.add_paragraph()
    p.text = f"Median: {key_levels.get('median') if key_levels.get('median') is not None else '—'}"
    p.level = 1
    p = t2.add_paragraph()
    p.text = f"Resistance: {key_levels.get('resistance') if key_levels.get('resistance') is not None else '—'}"
    p.level = 1

    # Slide 3: Rationale.
    slide3 = prs.slides.add_slide(prs.slide_layouts[5])
    slide3.shapes.title.text = "Trade Rationale"
    rationale_box = slide3.shapes.add_textbox(Inches(0.8), Inches(1.4), Inches(11.3), Inches(4.6))
    t3 = rationale_box.text_frame
    t3.word_wrap = True
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", rationale) if s.strip()]
    if not sentences:
        sentences = [rationale.strip() or "No rationale available."]
    t3.text = sentences[0]
    for sentence in sentences[1:]:
        p = t3.add_paragraph()
        p.text = sentence
        p.level = 0

    out = BytesIO()
    prs.save(out)
    return out.getvalue()


def _generate_and_store_forecast_report_assets(
    forecast_id: str,
    forecast_doc: dict[str, Any],
) -> dict[str, Any]:
    workspace_id = str(forecast_doc.get("userId") or forecast_doc.get("createdByUid") or "").strip() or "workspace"
    ticker = str(forecast_doc.get("ticker") or "ticker").upper()
    service = str(forecast_doc.get("service") or "prophet")
    service_label = "Quantura Horizon" if service.lower() == "prophet" else service

    chart_png = _generate_forecast_chart_png(forecast_doc)
    rationale = str(forecast_doc.get("tradeRationale") or "").strip()
    if not rationale:
        rationale = _build_forecast_trade_rationale(
            service=service,
            horizon=int(forecast_doc.get("horizon") or 0),
            metrics=forecast_doc.get("metrics") if isinstance(forecast_doc.get("metrics"), dict) else {},
            forecast_rows=forecast_doc.get("forecastRows") if isinstance(forecast_doc.get("forecastRows"), list) else [],
        )

    chart_b64 = base64.b64encode(chart_png).decode("utf-8")
    html = _render_forecast_report_html(forecast_doc, rationale, chart_b64)
    pdf_bytes = _generate_forecast_pdf_bytes(html, f"{ticker} • {service_label}", rationale)
    pptx_bytes = _generate_forecast_pptx_bytes(forecast_doc, chart_png, rationale)

    safe_prefix = re.sub(r"[^A-Z0-9_-]+", "_", f"{ticker}_{forecast_id}".upper())
    base_path = f"forecast_reports/{workspace_id}"
    chart_path = f"{base_path}/{safe_prefix}_chart.png"
    pdf_path = f"{base_path}/{safe_prefix}_executive_brief.pdf"
    pptx_path = f"{base_path}/{safe_prefix}_slide_deck.pptx"

    bucket = admin_storage.bucket(STORAGE_BUCKET)
    bucket.blob(chart_path).upload_from_string(chart_png, content_type="image/png")
    bucket.blob(pdf_path).upload_from_string(pdf_bytes, content_type="application/pdf")
    bucket.blob(pptx_path).upload_from_string(
        pptx_bytes,
        content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
    )

    return {
        "tradeRationale": rationale,
        "reportAssets": {
            "chartPath": chart_path,
            "pdfPath": pdf_path,
            "pptxPath": pptx_path,
        },
    }


@https_fn.on_call()
def create_order(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}

    product = str(data.get("product") or "Deep Forecast")
    currency = str(data.get("currency") or "USD")
    price = data.get("price")
    if not isinstance(price, (int, float)):
        price = DEFAULT_FORECAST_PRICE
    meta = data.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}

    order = {
        "userId": req.auth.uid,
        "userEmail": token.get("email"),
        "product": product,
        "price": price,
        "currency": currency,
        "status": "pending",
        "paymentProvider": "stripe",
        "paymentStatus": "unpaid",
        "stripeCheckoutSessionId": "",
        "stripePaymentIntentId": "",
        "stripeSubscriptionId": "",
        "fulfillmentNotes": "",
        "meta": meta,
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    doc_ref = db.collection("orders").document()
    doc_ref.set(order)
    _audit_event(req.auth.uid, token.get("email"), "order_created", {"orderId": doc_ref.id, "product": product})

    return {"orderId": doc_ref.id}


@https_fn.on_call()
def create_stripe_checkout_session(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    order_id = str(data.get("orderId") or "").strip()
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}

    if not order_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Order ID is required.")

    context = _remote_config_context(req, token, meta)
    if not _remote_config_bool("stripe_checkout_enabled", True, context=context):
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "Checkout is temporarily disabled.",
        )

    stripe = _stripe_module()
    order_ref = db.collection("orders").document(order_id)
    snapshot = order_ref.get()
    if not snapshot.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Order not found.")

    order = snapshot.to_dict() or {}
    owner_id = str(order.get("userId") or "")
    if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Order access denied.")

    payment_status = str(order.get("paymentStatus") or "unpaid").strip().lower()
    if payment_status in {"paid", "succeeded"}:
        return {"orderId": order_id, "status": "paid"}

    product = str(order.get("product") or "Deep Forecast").strip() or "Deep Forecast"
    currency = str(order.get("currency") or "USD").strip().lower() or "usd"
    price = _safe_float(order.get("price"))
    if price is None:
        price = float(DEFAULT_FORECAST_PRICE)
    amount_cents = max(50, int(round(float(price) * 100)))

    normalized_name = product.lower()
    mode = "payment" if "forecast" in normalized_name else "subscription"
    recurring = {"interval": "month"} if mode == "subscription" else None

    price_data: dict[str, Any] = {
        "currency": currency,
        "unit_amount": amount_cents,
        "product_data": {"name": product},
    }
    if recurring:
        price_data["recurring"] = recurring

    success_url = f"{PUBLIC_ORIGIN}/purchase?checkout=success&orderId={order_id}&session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = f"{PUBLIC_ORIGIN}/purchase?checkout=cancel&orderId={order_id}"

    session_kwargs: dict[str, Any] = {
        "mode": mode,
        "line_items": [{"price_data": price_data, "quantity": 1}],
        "success_url": success_url,
        "cancel_url": cancel_url,
        "client_reference_id": order_id,
        "metadata": {
            "orderId": order_id,
            "userId": owner_id,
            "product": product,
        },
    }

    email = token.get("email")
    if isinstance(email, str) and email.strip():
        session_kwargs["customer_email"] = email.strip()

    try:
        session = stripe.checkout.Session.create(**session_kwargs)
    except Exception as exc:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INTERNAL,
            "Unable to start checkout session.",
            {"error": str(exc)},
        )

    order_ref.set(
        {
            "paymentProvider": "stripe",
            "paymentStatus": "checkout_created",
            "stripeCheckoutSessionId": str(getattr(session, "id", "") or ""),
            "stripeMode": mode,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    _audit_event(
        req.auth.uid,
        token.get("email"),
        "stripe_checkout_session_created",
        {"orderId": order_id, "mode": mode, "currency": currency, "amountCents": amount_cents},
    )

    return {
        "orderId": order_id,
        "status": "created",
        "mode": mode,
        "sessionId": str(getattr(session, "id", "") or ""),
        "url": str(getattr(session, "url", "") or ""),
    }


@https_fn.on_call()
def confirm_stripe_checkout(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    session_id = str(data.get("sessionId") or data.get("session_id") or "").strip()
    order_hint = str(data.get("orderId") or "").strip()

    if not session_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Stripe session ID is required.")

    stripe = _stripe_module()
    try:
        session = stripe.checkout.Session.retrieve(session_id, expand=["payment_intent", "subscription"])
    except Exception as exc:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.NOT_FOUND,
            "Stripe session could not be loaded.",
            {"error": str(exc)},
        )

    session_dict: dict[str, Any] = session.to_dict() if hasattr(session, "to_dict") else dict(session)
    order_id = (
        str(session_dict.get("client_reference_id") or "").strip()
        or str((session_dict.get("metadata") or {}).get("orderId") or "").strip()
        or order_hint
    )
    if not order_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Order ID missing from Stripe session.")

    order_ref = db.collection("orders").document(order_id)
    snapshot = order_ref.get()
    if not snapshot.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Order not found.")

    order = snapshot.to_dict() or {}
    owner_id = str(order.get("userId") or "")
    if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Order access denied.")

    mode = str(session_dict.get("mode") or "")
    payment_status = str(session_dict.get("payment_status") or "").lower()
    session_status = str(session_dict.get("status") or "").lower()
    subscription_id = session_dict.get("subscription")
    payment_intent = session_dict.get("payment_intent")

    is_paid = False
    if mode == "payment":
        is_paid = payment_status == "paid"
    elif mode == "subscription":
        is_paid = session_status == "complete" or bool(subscription_id)
    else:
        is_paid = payment_status == "paid"

    payment_intent_id = ""
    if isinstance(payment_intent, dict):
        payment_intent_id = str(payment_intent.get("id") or "")
    elif isinstance(payment_intent, str):
        payment_intent_id = payment_intent

    subscription_id_str = ""
    if isinstance(subscription_id, dict):
        subscription_id_str = str(subscription_id.get("id") or "")
    elif isinstance(subscription_id, str):
        subscription_id_str = subscription_id

    update_payload: dict[str, Any] = {
        "paymentProvider": "stripe",
        "stripeCheckoutSessionId": session_id,
        "stripePaymentIntentId": payment_intent_id,
        "stripeSubscriptionId": subscription_id_str,
        "stripeMode": mode,
        "stripePaymentStatus": payment_status,
        "stripeSessionStatus": session_status,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    if is_paid:
        update_payload["paymentStatus"] = "paid"
        update_payload["paidAt"] = firestore.SERVER_TIMESTAMP
    else:
        update_payload["paymentStatus"] = "unpaid"

    order_ref.set(update_payload, merge=True)
    _audit_event(
        req.auth.uid,
        token.get("email"),
        "stripe_checkout_confirmed",
        {"orderId": order_id, "paid": bool(is_paid), "mode": mode, "sessionId": session_id},
    )

    return {
        "orderId": order_id,
        "paid": bool(is_paid),
        "mode": mode,
        "paymentStatus": update_payload["paymentStatus"],
        "product": order.get("product") or "",
        "currency": order.get("currency") or "USD",
        "price": order.get("price") or 0,
    }


@https_fn.on_request()
def stripe_webhook(req: https_fn.Request) -> tuple[str, int]:
    if req.method != "POST":
        return ("Method not allowed", 405)

    if not STRIPE_WEBHOOK_SECRET:
        return ("Stripe webhook secret is not configured.", 500)

    stripe = _stripe_module()
    payload = req.get_data(cache=False, as_text=False)
    sig = req.headers.get("Stripe-Signature", "")

    try:
        event = stripe.Webhook.construct_event(payload=payload, sig_header=sig, secret=STRIPE_WEBHOOK_SECRET)
    except Exception:
        return ("Invalid signature", 400)

    try:
        event_type = str(event.get("type") or "")
        if event_type == "checkout.session.completed":
            session_obj = (event.get("data") or {}).get("object") or {}
            order_id = str(session_obj.get("client_reference_id") or "").strip()
            if not order_id:
                order_id = str((session_obj.get("metadata") or {}).get("orderId") or "").strip()

            if order_id:
                payment_status = str(session_obj.get("payment_status") or "").lower()
                session_status = str(session_obj.get("status") or "").lower()
                mode = str(session_obj.get("mode") or "")

                is_paid = payment_status == "paid" or session_status == "complete"
                db.collection("orders").document(order_id).set(
                    {
                        "paymentProvider": "stripe",
                        "paymentStatus": "paid" if is_paid else "unpaid",
                        "stripeCheckoutSessionId": str(session_obj.get("id") or ""),
                        "stripePaymentIntentId": str(session_obj.get("payment_intent") or ""),
                        "stripeSubscriptionId": str(session_obj.get("subscription") or ""),
                        "stripeMode": mode,
                        "stripePaymentStatus": payment_status,
                        "stripeSessionStatus": session_status,
                        "paidAt": firestore.SERVER_TIMESTAMP if is_paid else firestore.DELETE_FIELD,
                        "updatedAt": firestore.SERVER_TIMESTAMP,
                    },
                    merge=True,
                )
    except Exception:
        # Webhook processing is best-effort; Stripe will retry on non-2xx, so keep this 200 unless signature fails.
        pass

    return ("ok", 200)


@https_fn.on_call()
def update_order_status(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)

    data = req.data or {}
    order_id = data.get("orderId")
    status = data.get("status")
    notes = data.get("notes", "")

    if not order_id:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Order ID is required.",
        )

    if status not in ALLOWED_STATUSES:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Invalid order status.",
        )

    order_ref = db.collection("orders").document(order_id)
    snapshot = order_ref.get()
    if not snapshot.exists:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.NOT_FOUND,
            "Order not found.",
        )
    order_data = snapshot.to_dict() or {}

    update_payload: dict[str, Any] = {
        "status": status,
        "fulfillmentNotes": notes,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    if status == "fulfilled":
        update_payload["fulfilledAt"] = firestore.SERVER_TIMESTAMP

    order_ref.update(update_payload)
    _audit_event(req.auth.uid, token.get("email"), "order_status_updated", {"orderId": order_id, "status": status})

    order_user_id = order_data.get("userId")
    order_user_email = order_data.get("userEmail")
    if isinstance(order_user_id, str) and order_user_id:
        product = str(order_data.get("product") or "Deep Forecast")
        human_status = status.replace("_", " ").title()
        _notify_user(
            order_user_id,
            order_user_email if isinstance(order_user_email, str) else None,
            title="Quantura order update",
            body=f"{product} order {order_id} is now {human_status}.",
            data={
                "type": "order_status",
                "orderId": order_id,
                "status": status,
                "url": "/dashboard",
            },
        )

    return {"orderId": order_id, "status": status}


@https_fn.on_call()
def submit_contact(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    payload = {
        "name": str(data.get("name") or "").strip(),
        "email": str(data.get("email") or "").strip(),
        "company": str(data.get("company") or "").strip(),
        "role": str(data.get("role") or "").strip(),
        "message": str(data.get("message") or "").strip(),
        "sourcePage": str(data.get("sourcePage") or "").strip(),
        "utm": data.get("utm") or {},
        "meta": data.get("meta") or {},
        "createdAt": firestore.SERVER_TIMESTAMP,
    }

    missing = [field for field in CONTACT_REQUIRED_FIELDS if not payload[field]]
    if missing:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Missing required contact fields.",
            {"missing": missing},
        )

    doc_ref = db.collection("contacts").document()
    doc_ref.set(payload)

    return {"contactId": doc_ref.id}


@https_fn.on_call()
def create_collab_invite(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    to_email_raw = str(data.get("email") or "").strip()
    to_email_norm = _normalize_email(to_email_raw)
    if not to_email_norm or "@" not in to_email_norm:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Valid email is required.")

    role = str(data.get("role") or "viewer").strip().lower()
    if role not in {"viewer", "editor"}:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Role must be viewer or editor.")

    from_email = _normalize_email(token.get("email"))
    invite_doc = {
        "fromUserId": req.auth.uid,
        "fromEmail": from_email,
        "workspaceUserId": req.auth.uid,
        "workspaceEmail": from_email,
        "toEmail": to_email_raw,
        "toEmailNorm": to_email_norm,
        "toUserId": "",
        "role": role,
        "status": "pending",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    doc_ref = db.collection("collab_invites").document()
    doc_ref.set(invite_doc)
    _audit_event(
        req.auth.uid,
        token.get("email"),
        "collab_invite_created",
        {"inviteId": doc_ref.id, "toEmail": to_email_norm, "role": role},
    )
    return {"inviteId": doc_ref.id, "status": "pending"}


@https_fn.on_call()
def list_collab_invites(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    email_norm = _normalize_email(token.get("email"))
    if not email_norm:
        return {"invites": []}

    docs = (
        db.collection("collab_invites")
        .where("toEmailNorm", "==", email_norm)
        .limit(100)
        .stream()
    )

    invites: list[dict[str, Any]] = []
    for doc in docs:
        data = doc.to_dict() or {}
        if data.get("status") != "pending":
            continue
        invites.append(
            {
                "inviteId": doc.id,
                "fromEmail": data.get("fromEmail"),
                "workspaceUserId": data.get("workspaceUserId"),
                "workspaceEmail": data.get("workspaceEmail"),
                "role": data.get("role", "viewer"),
                "status": data.get("status", "pending"),
                "createdAt": data.get("createdAt"),
            }
        )

    return {"invites": _serialize_for_firestore(invites)}


@https_fn.on_call()
def accept_collab_invite(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    invite_id = str(data.get("inviteId") or "").strip()
    if not invite_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invite ID is required.")

    email_norm = _normalize_email(token.get("email"))
    invite_ref = db.collection("collab_invites").document(invite_id)
    snapshot = invite_ref.get()
    if not snapshot.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Invite not found.")

    invite = snapshot.to_dict() or {}
    if invite.get("status") != "pending":
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, "Invite is not pending.")
    if _normalize_email(invite.get("toEmailNorm")) != email_norm:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Invite is not addressed to you.")

    workspace_user_id = str(invite.get("workspaceUserId") or invite.get("fromUserId") or "").strip()
    workspace_email = _normalize_email(invite.get("workspaceEmail") or invite.get("fromEmail"))
    if not workspace_user_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INTERNAL, "Invite is missing workspace info.")

    role = str(invite.get("role") or "viewer").strip().lower()
    if role not in {"viewer", "editor"}:
        role = "viewer"

    collab_uid = req.auth.uid
    collab_email = _normalize_email(token.get("email"))

    batch = db.batch()
    batch.set(
        db.collection("users").document(workspace_user_id).collection("collaborators").document(collab_uid),
        {
            "userId": collab_uid,
            "email": collab_email,
            "role": role,
            "addedAt": firestore.SERVER_TIMESTAMP,
            "addedBy": workspace_user_id,
        },
        merge=True,
    )
    batch.set(
        db.collection("users").document(collab_uid).collection("shared_workspaces").document(workspace_user_id),
        {
            "workspaceUserId": workspace_user_id,
            "workspaceEmail": workspace_email,
            "role": role,
            "addedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    batch.update(
        invite_ref,
        {
            "status": "accepted",
            "toUserId": collab_uid,
            "acceptedAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
    )
    batch.commit()

    _audit_event(
        collab_uid,
        token.get("email"),
        "collab_invite_accepted",
        {"inviteId": invite_id, "workspaceUserId": workspace_user_id, "role": role},
    )
    return {"status": "accepted", "workspaceUserId": workspace_user_id, "workspaceEmail": workspace_email, "role": role}


@https_fn.on_call()
def revoke_collab_invite(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    invite_id = str(data.get("inviteId") or "").strip()
    if not invite_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invite ID is required.")

    invite_ref = db.collection("collab_invites").document(invite_id)
    snapshot = invite_ref.get()
    if not snapshot.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Invite not found.")
    invite = snapshot.to_dict() or {}
    if invite.get("fromUserId") != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Only the inviter can revoke.")
    if invite.get("status") != "pending":
        return {"inviteId": invite_id, "status": invite.get("status")}

    invite_ref.update({"status": "revoked", "updatedAt": firestore.SERVER_TIMESTAMP})
    _audit_event(req.auth.uid, token.get("email"), "collab_invite_revoked", {"inviteId": invite_id})
    return {"inviteId": invite_id, "status": "revoked"}


@https_fn.on_call()
def list_collaborators(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    docs = db.collection("users").document(req.auth.uid).collection("collaborators").limit(200).stream()
    collaborators: list[dict[str, Any]] = []
    for doc in docs:
        data = doc.to_dict() or {}
        collaborators.append(
            {
                "userId": doc.id,
                "email": data.get("email"),
                "role": data.get("role", "viewer"),
                "addedAt": data.get("addedAt"),
            }
        )
    return {"collaborators": _serialize_for_firestore(collaborators)}


@https_fn.on_call()
def remove_collaborator(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    collaborator_user_id = str(data.get("collaboratorUserId") or "").strip()
    if not collaborator_user_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Collaborator user ID is required.")

    owner_id = req.auth.uid
    batch = db.batch()
    batch.delete(db.collection("users").document(owner_id).collection("collaborators").document(collaborator_user_id))
    batch.delete(db.collection("users").document(collaborator_user_id).collection("shared_workspaces").document(owner_id))
    batch.commit()
    _audit_event(owner_id, token.get("email"), "collaborator_removed", {"collaboratorUserId": collaborator_user_id})
    return {"status": "removed", "collaboratorUserId": collaborator_user_id}


@https_fn.on_call()
def track_meta_conversion_event(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    if not isinstance(data, dict):
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Event payload must be an object.",
        )

    if not META_PIXEL_ID or not META_CAPI_ACCESS_TOKEN:
        return {"ok": False, "skipped": "meta_capi_not_configured"}

    origin = _meta_header(req, "origin")
    if origin and origin not in META_ALLOWED_ORIGINS:
        return {"ok": False, "skipped": "origin_not_allowed"}

    source_event_name = str(data.get("sourceEventName") or data.get("eventName") or "").strip()
    if not source_event_name:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "eventName is required.",
        )
    source_event_name = re.sub(r"[^A-Za-z0-9_\\- ]+", "", source_event_name)[:80] or "custom_event"

    event_name = str(data.get("eventName") or source_event_name).strip()
    event_name = "PageView" if event_name == "page_view" else event_name
    event_name = re.sub(r"[^A-Za-z0-9_\\- ]+", "", event_name)[:80] or "custom_event"

    now_ts = int(time.time())
    event_time = int(_safe_float(data.get("eventTime")) or now_ts)
    if event_time < 946684800 or event_time > now_ts + 3600:
        event_time = now_ts

    event_id = str(data.get("eventId") or "").strip()
    if not event_id:
        seed = f"{source_event_name}:{event_time}:{time.time_ns()}"
        event_id = f"q_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:24]}"
    event_id = re.sub(r"[^A-Za-z0-9_\\-]+", "", event_id)[:100] or f"q_{int(time.time() * 1000)}"

    action_source = str(data.get("actionSource") or "website").strip().lower()
    valid_action_sources = {
        "website",
        "app",
        "phone_call",
        "chat",
        "physical_store",
        "system_generated",
        "business_messaging",
        "other",
    }
    if action_source not in valid_action_sources:
        action_source = "website"

    event_source_url = str(data.get("eventSourceUrl") or "").strip()
    if event_source_url and not event_source_url.startswith("http"):
        event_source_url = f"{PUBLIC_ORIGIN}{event_source_url if event_source_url.startswith('/') else '/' + event_source_url}"
    if not event_source_url:
        event_source_url = PUBLIC_ORIGIN

    user_data = _meta_build_user_data(req, data)
    custom_data = _meta_build_custom_data(data.get("params"))

    event_payload: dict[str, Any] = {
        "event_name": event_name,
        "event_time": event_time,
        "event_id": event_id,
        "event_source_url": event_source_url,
        "action_source": action_source,
        "user_data": user_data,
        "original_event_data": {
            "event_name": source_event_name,
            "event_time": event_time,
        },
    }
    if custom_data:
        event_payload["custom_data"] = custom_data

    payload = {"data": [event_payload]}
    endpoint = f"https://graph.facebook.com/{META_GRAPH_API_VERSION}/{META_PIXEL_ID}/events"

    try:
        response = requests.post(
            endpoint,
            params={"access_token": META_CAPI_ACCESS_TOKEN},
            json=payload,
            timeout=10,
        )
    except Exception as exc:
        return {"ok": False, "eventId": event_id, "error": f"request_failed: {exc}"}

    response_text = response.text[:1200]
    try:
        response_json = response.json()
    except Exception:
        response_json = {"raw": response_text}

    if response.status_code >= 400:
        return {
            "ok": False,
            "eventId": event_id,
            "status": response.status_code,
            "error": response_json,
        }

    return {
        "ok": True,
        "eventId": event_id,
        "status": response.status_code,
        "meta": _serialize_for_firestore(response_json),
    }


@https_fn.on_call()
def get_web_push_config(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    meta = req.data.get("meta") if isinstance(req.data, dict) else {}
    context = _remote_config_context(req, token, meta if isinstance(meta, dict) else None)
    vapid_key = FCM_WEB_VAPID_KEY or _remote_config_param("webpush_vapid_key", "", context=context)
    if not vapid_key:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "FCM_WEB_VAPID_KEY is not configured.",
        )
    return {"vapidKey": vapid_key}


@https_fn.on_call()
def get_feature_flags(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    meta = req.data.get("meta") if isinstance(req.data, dict) else {}
    context = _remote_config_context(req, token, meta if isinstance(meta, dict) else None)
    tier_key, tier = _resolve_ai_tier(req.auth.uid, token, context=context)
    usage_tiers = _get_ai_usage_tiers(context=context)
    return {
        "forecastProphetEnabled": _remote_config_bool("forecast_prophet_enabled", True, context=context),
        "forecastTimeMixerEnabled": _remote_config_bool("forecast_timemixer_enabled", True, context=context),
        "watchlistEnabled": _remote_config_bool("watchlist_enabled", True, context=context),
        "enableSocialLeaderboard": _remote_config_bool("enable_social_leaderboard", True, context=context),
        "pushEnabled": _remote_config_bool("push_notifications_enabled", True, context=context),
        "webPushVapidKey": _remote_config_param("webpush_vapid_key", "", context=context),
        "forecastModelPrimary": _remote_config_param("forecast_model_primary", "Quantura Horizon", context=context),
        "promoBannerText": _remote_config_param("promo_banner_text", "", context=context),
        "maintenanceMode": _remote_config_bool("maintenance_mode", False, context=context),
        "volatilityThreshold": _remote_config_float("volatility_threshold", 0.05, context=context),
        "aiUsageTiers": _serialize_for_firestore(usage_tiers),
        "aiUsageTierKey": tier_key,
        "aiUsageTier": _serialize_for_firestore(tier),
    }


@https_fn.on_call()
def register_notification_token(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    registration_token = str(data.get("token") or "").strip()
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    if len(registration_token) < 20:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Valid notification token is required.",
        )

    doc_id = _token_doc_id(registration_token)
    doc_ref = db.collection("notification_tokens").document(doc_id)
    doc_ref.set(
        {
            "token": registration_token,
            "tokenHash": doc_id,
            "active": True,
            "userId": req.auth.uid,
            "userEmail": token.get("email"),
            "userAgent": str(meta.get("userAgent") or ""),
            "meta": meta,
            "forceRefresh": bool(data.get("forceRefresh")),
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "lastSeenAt": firestore.SERVER_TIMESTAMP,
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    _audit_event(req.auth.uid, token.get("email"), "notification_token_registered", {"tokenHash": doc_id})
    return {"tokenHash": doc_id, "active": True}


@https_fn.on_call()
def unregister_notification_token(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    registration_token = str(data.get("token") or "").strip()
    if len(registration_token) < 20:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Valid notification token is required.",
        )

    doc_id = _token_doc_id(registration_token)
    db.collection("notification_tokens").document(doc_id).set(
        {
            "active": False,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    _audit_event(req.auth.uid, token.get("email"), "notification_token_unregistered", {"tokenHash": doc_id})
    return {"tokenHash": doc_id, "active": False}


@https_fn.on_call()
def send_test_notification(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    title = str(data.get("title") or "Quantura notification test")
    body = str(data.get("body") or "Your web push notifications are active.")
    payload_data = data.get("data") if isinstance(data.get("data"), dict) else {}

    result = _notify_user(
        req.auth.uid,
        token.get("email"),
        title=title,
        body=body,
        data={
            **payload_data,
            "type": "test",
            "url": "/dashboard",
        },
    )
    return result


@https_fn.on_call()
def check_price_alerts(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    workspace_id = str(data.get("workspaceId") or req.auth.uid or "").strip()
    if not workspace_id:
        workspace_id = req.auth.uid

    # Allow scanning your own workspace, or a shared workspace you have access to.
    if workspace_id != req.auth.uid and token.get("email") != ADMIN_EMAIL:
        shared = (
            db.collection("users")
            .document(workspace_id)
            .collection("collaborators")
            .document(req.auth.uid)
            .get()
        )
        if not shared.exists:
            raise https_fn.HttpsError(
                https_fn.FunctionsErrorCode.PERMISSION_DENIED,
                "Workspace access required to scan alerts.",
            )

    alerts_ref = db.collection("users").document(workspace_id).collection("price_alerts")
    docs = (
        alerts_ref.where("createdByUid", "==", req.auth.uid)
        .where("active", "==", True)
        .limit(200)
        .stream()
    )
    alert_docs = [(doc, doc.to_dict() or {}) for doc in docs]
    if not alert_docs:
        return {"workspaceId": workspace_id, "checked": 0, "triggered": 0, "triggeredAlerts": []}

    alerts: list[dict[str, Any]] = []
    tickers: list[str] = []
    for doc, payload in alert_docs:
        ticker = str(payload.get("ticker") or "").upper().strip()
        if ticker:
            tickers.append(ticker)
        alerts.append({"ref": doc.reference, "id": doc.id, "data": payload, "ticker": ticker})

    price_map = _latest_close_prices(tickers)
    batch = db.batch()
    triggered: list[dict[str, Any]] = []

    for alert in alerts:
        ref = alert["ref"]
        payload = alert["data"] or {}
        ticker = alert["ticker"]
        condition = str(payload.get("condition") or "above").strip().lower()
        target = _safe_float(payload.get("targetPrice") or payload.get("target") or payload.get("price"))
        current = price_map.get(ticker)

        update_payload: dict[str, Any] = {
            "lastCheckedAt": firestore.SERVER_TIMESTAMP,
            "lastPrice": current,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }

        fired = False
        if current is not None and target is not None:
            if condition == "below" and current <= target:
                fired = True
            if condition != "below" and current >= target:
                fired = True

        if fired:
            update_payload.update(
                {
                    "active": False,
                    "status": "triggered",
                    "triggeredAt": firestore.SERVER_TIMESTAMP,
                    "triggeredPrice": current,
                }
            )
            triggered.append(
                {
                    "alertId": alert["id"],
                    "workspaceId": workspace_id,
                    "ticker": ticker,
                    "condition": condition,
                    "targetPrice": target,
                    "currentPrice": current,
                }
            )
            event_ref = db.collection("price_alert_events").document()
            batch.set(
                event_ref,
                {
                    "alertId": alert["id"],
                    "workspaceId": workspace_id,
                    "userId": req.auth.uid,
                    "userEmail": token.get("email"),
                    "ticker": ticker,
                    "condition": condition,
                    "targetPrice": target,
                    "currentPrice": current,
                    "createdAt": firestore.SERVER_TIMESTAMP,
                    "meta": data.get("meta") or {},
                },
            )

        batch.set(ref, update_payload, merge=True)

    batch.commit()

    if triggered:
        lines = []
        for item in triggered[:6]:
            t = item.get("ticker") or "?"
            cond = ">= " if item.get("condition") != "below" else "<= "
            lines.append(f"{t} {cond}{item.get('targetPrice')} (now {item.get('currentPrice')})")
        body = "Triggered: " + "; ".join(lines)
        _notify_user(
            req.auth.uid,
            token.get("email"),
            title="Quantura price alert",
            body=body,
            data={"type": "price_alert", "count": len(triggered), "url": "/dashboard#watchlist"},
        )

    _audit_event(
        req.auth.uid,
        token.get("email"),
        "price_alerts_checked",
        {"workspaceId": workspace_id, "checked": len(alerts), "triggered": len(triggered)},
    )

    return {
        "workspaceId": workspace_id,
        "checked": len(alerts),
        "triggered": len(triggered),
        "triggeredAlerts": _serialize_for_firestore(triggered),
        "prices": _serialize_for_firestore(price_map),
    }


def _handle_forecast_request(req: https_fn.CallableRequest, forced_service: str | None = None) -> dict[str, Any]:
    token = _require_auth(req)
    data = dict(req.data or {})
    if forced_service:
        data["service"] = forced_service

    workspace_id = str(data.get("workspaceId") or req.auth.uid or "").strip()
    if not workspace_id:
        workspace_id = req.auth.uid
    if workspace_id != req.auth.uid:
        _require_workspace_editor(workspace_id, req.auth.uid, token)

    ticker = str(data.get("ticker") or "").upper().strip()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    service = str(data.get("service") or "prophet").strip().lower()
    if service not in FORECAST_SERVICES:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Invalid forecast service.",
            {"allowed": sorted(FORECAST_SERVICES)},
        )
    context = _remote_config_context(req, token, data.get("meta") if isinstance(data.get("meta"), dict) else None)
    if _remote_config_bool("maintenance_mode", False, context=context):
        _raise_structured_error(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "maintenance_mode",
            "Quantura is temporarily in maintenance mode.",
        )
    if service == "prophet" and not _remote_config_bool("forecast_prophet_enabled", True, context=context):
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "Quantura Horizon forecasting is currently disabled.",
        )
    if service == "ibm_timemixer" and not _remote_config_bool("forecast_timemixer_enabled", True, context=context):
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "IBM TimeMixer forecasting is currently disabled.",
        )

    interval = str(data.get("interval") or "1d")
    if interval not in {"1d", "1h"}:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Interval must be 1d or 1h.")

    horizon = int(data.get("horizon") or 90)
    quantiles = _parse_quantiles(data.get("quantiles"))
    start = data.get("start")

    try:
        history = _load_history(ticker=ticker, start=start, interval=interval)
    except https_fn.HttpsError:
        raise
    except Exception as exc:
        _raise_structured_error(
            https_fn.FunctionsErrorCode.NOT_FOUND,
            "ticker_not_found",
            "Unable to load market data for ticker.",
            {"ticker": ticker, "raw": str(exc)},
        )
    close_series = history["Close"].copy()

    try:
        result = _run_forecast_service(service, close_series, horizon, quantiles, interval)
    except Exception:
        try:
            result = _generate_quantile_forecast(close_series, horizon, quantiles, interval)
            result["serviceMessage"] = "Forecast service failed; fallback model executed."
        except Exception as exc:
            _raise_structured_error(
                https_fn.FunctionsErrorCode.INTERNAL,
                "forecast_failed",
                "Forecast generation failed.",
                {"ticker": ticker, "service": service, "raw": str(exc)},
            )

    trade_rationale = _build_forecast_trade_rationale(
        service=service,
        horizon=horizon,
        metrics=result.get("metrics") if isinstance(result.get("metrics"), dict) else {},
        forecast_rows=result.get("forecastRows") if isinstance(result.get("forecastRows"), list) else [],
    )

    request_doc = {
        "userId": workspace_id,
        "userEmail": token.get("email"),
        "createdByUid": req.auth.uid,
        "createdByEmail": token.get("email"),
        "ticker": ticker,
        "interval": interval,
        "horizon": horizon,
        "start": start,
        "quantiles": quantiles,
        "service": service,
        "engine": result.get("engine"),
        "status": result.get("status", "completed"),
        "serviceMessage": result.get("serviceMessage"),
        "metrics": _serialize_for_firestore(result.get("metrics") or {}),
        "forecastPreview": _serialize_for_firestore(_forecast_preview_rows(result.get("forecastRows") or [])),
        "forecastRows": _serialize_for_firestore(result.get("forecastRows") or []),
        "tradeRationale": trade_rationale,
        "reportStatus": "queued",
        "reportAssets": {},
        "meta": data.get("meta") or {},
        "utm": data.get("utm") or {},
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    doc_ref = db.collection("forecast_requests").document()
    doc_ref.set(request_doc)

    _audit_event(
        req.auth.uid,
        token.get("email"),
        "forecast_requested",
        {
            "requestId": doc_ref.id,
            "ticker": ticker,
            "service": service,
            "engine": result.get("engine"),
            "workspaceId": workspace_id,
        },
    )

    metrics = result.get("metrics") or {}
    return {
        "requestId": doc_ref.id,
        "status": request_doc["status"],
        "service": service,
        "engine": request_doc["engine"],
        "serviceMessage": request_doc["serviceMessage"],
        "lastClose": metrics.get("lastClose"),
        "mae": metrics.get("mae"),
        "coverage10_90": metrics.get("coverage10_90", "n/a"),
        "forecastPreview": request_doc["forecastPreview"],
        "tradeRationale": trade_rationale,
        "reportStatus": "queued",
    }


@https_fn.on_call()
def run_timeseries_forecast(req: https_fn.CallableRequest) -> dict[str, Any]:
    return _handle_forecast_request(req)


@https_fn.on_call()
def run_prophet_forecast(req: https_fn.CallableRequest) -> dict[str, Any]:
    return _handle_forecast_request(req, forced_service="prophet")


@https_fn.on_call()
def delete_forecast_request(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    forecast_id = str(data.get("forecastId") or data.get("id") or "").strip()
    if not forecast_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Forecast ID is required.")

    ref = db.collection("forecast_requests").document(forecast_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Forecast not found.")

    doc = snap.to_dict() or {}
    workspace_id = str(doc.get("userId") or "").strip()
    if not workspace_id:
        if token.get("email") != ADMIN_EMAIL:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")
    else:
        _require_workspace_editor(workspace_id, req.auth.uid, token)

    ref.delete()
    _audit_event(req.auth.uid, token.get("email"), "forecast_deleted", {"forecastId": forecast_id, "workspaceId": workspace_id})
    return {"deleted": True, "forecastId": forecast_id}


@https_fn.on_call()
def generate_forecast_report_assets(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    forecast_id = str(data.get("forecastId") or data.get("id") or "").strip()
    force = bool(data.get("force"))
    if not forecast_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Forecast ID is required.")

    ref = db.collection("forecast_requests").document(forecast_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Forecast not found.")

    doc = snap.to_dict() or {}
    workspace_id = str(doc.get("userId") or "").strip()
    if workspace_id:
        _require_workspace_editor(workspace_id, req.auth.uid, token)
    elif token.get("email") != ADMIN_EMAIL:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")

    existing_assets = doc.get("reportAssets") if isinstance(doc.get("reportAssets"), dict) else {}
    existing_status = str(doc.get("reportStatus") or "").strip().lower()
    if not force and existing_status == "ready" and existing_assets:
        return {
            "forecastId": forecast_id,
            "reportStatus": "ready",
            "reportAssets": _serialize_for_firestore(existing_assets),
            "tradeRationale": str(doc.get("tradeRationale") or "").strip(),
        }

    ref.set(
        {
            "reportStatus": "generating",
            "reportError": "",
            "reportUpdatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    try:
        generated = _generate_and_store_forecast_report_assets(forecast_id, doc)
        ref.set(
            {
                "reportStatus": "ready",
                "reportAssets": generated.get("reportAssets") or {},
                "tradeRationale": generated.get("tradeRationale") or str(doc.get("tradeRationale") or "").strip(),
                "reportUpdatedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        _audit_event(req.auth.uid, token.get("email"), "forecast_report_generated", {"forecastId": forecast_id, "workspaceId": workspace_id})
        return {
            "forecastId": forecast_id,
            "reportStatus": "ready",
            "reportAssets": _serialize_for_firestore(generated.get("reportAssets") or {}),
            "tradeRationale": str(generated.get("tradeRationale") or "").strip(),
        }
    except Exception as error:
        ref.set(
            {
                "reportStatus": "failed",
                "reportError": str(error)[:700],
                "reportUpdatedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INTERNAL, f"Unable to generate report assets: {error}")


@scheduler_fn.on_schedule(
    schedule="*/30 * * * *",
    timezone=scheduler_fn.Timezone(SOCIAL_AUTOMATION_TIMEZONE),
)
def forecast_report_agent_scheduler(event: scheduler_fn.ScheduledEvent) -> None:
    del event
    docs = list(db.collection("forecast_requests").where("reportStatus", "==", "queued").limit(REPORT_AGENT_BATCH_SIZE).stream())
    for item in docs:
        ref = db.collection("forecast_requests").document(item.id)
        data = item.to_dict() or {}
        try:
            ref.set(
                {
                    "reportStatus": "generating",
                    "reportError": "",
                    "reportUpdatedAt": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )
            generated = _generate_and_store_forecast_report_assets(item.id, data)
            ref.set(
                {
                    "reportStatus": "ready",
                    "reportAssets": generated.get("reportAssets") or {},
                    "tradeRationale": generated.get("tradeRationale") or str(data.get("tradeRationale") or "").strip(),
                    "reportUpdatedAt": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )
        except Exception as error:
            ref.set(
                {
                    "reportStatus": "failed",
                    "reportError": str(error)[:700],
                    "reportUpdatedAt": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )


@https_fn.on_call()
def delete_screener_run(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    run_id = str(data.get("runId") or data.get("id") or "").strip()
    if not run_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Run ID is required.")

    ref = db.collection("screener_runs").document(run_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Screener run not found.")

    doc = snap.to_dict() or {}
    workspace_id = str(doc.get("userId") or "").strip()
    if not workspace_id:
        if token.get("email") != ADMIN_EMAIL:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")
    else:
        _require_workspace_editor(workspace_id, req.auth.uid, token)

    ref.delete()
    _audit_event(req.auth.uid, token.get("email"), "screener_deleted", {"runId": run_id, "workspaceId": workspace_id})
    return {"deleted": True, "runId": run_id}


@https_fn.on_call()
def rename_screener_run(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    run_id = str(data.get("runId") or data.get("id") or "").strip()
    title = str(data.get("title") or "").strip()
    if not run_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Run ID is required.")
    if not title:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Title is required.")
    if len(title) > 180:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Title is too long.")

    ref = db.collection("screener_runs").document(run_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Screener run not found.")

    doc = snap.to_dict() or {}
    workspace_id = str(doc.get("userId") or "").strip()
    if not workspace_id:
        if token.get("email") != ADMIN_EMAIL:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")
    else:
        _require_workspace_editor(workspace_id, req.auth.uid, token)

    ref.set({"title": title, "updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)
    _audit_event(req.auth.uid, token.get("email"), "screener_renamed", {"runId": run_id, "workspaceId": workspace_id})
    return {"updated": True, "runId": run_id, "title": title}


@https_fn.on_call()
def upsert_ai_agent_social_action(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}

    workspace_id = str(data.get("workspaceId") or req.auth.uid or "").strip() or req.auth.uid
    if workspace_id != req.auth.uid:
        _require_workspace_editor(workspace_id, req.auth.uid, token)

    agent_id = str(data.get("agentId") or "").strip()
    action = str(data.get("action") or "").strip().lower()
    active = bool(data.get("active"))
    if not agent_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Agent ID is required.")
    if action not in {"follow", "like"}:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Action must be follow or like.")

    social_collection = "ai_agent_followers" if action == "follow" else "ai_agent_likes"
    count_field = "followersCount" if action == "follow" else "likesCount"
    user_doc_id = f"{agent_id}__{req.auth.uid}"

    agent_ref = db.collection("users").document(workspace_id).collection("ai_agents").document(agent_id)
    social_ref = db.collection("users").document(workspace_id).collection(social_collection).document(user_doc_id)

    txn = db.transaction()
    agent_snap = agent_ref.get(transaction=txn)
    if not agent_snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "AI Agent not found.")
    social_snap = social_ref.get(transaction=txn)

    agent_doc = agent_snap.to_dict() or {}
    current_count = int(agent_doc.get(count_field) or 0)
    now = firestore.SERVER_TIMESTAMP

    changed = False
    next_active = social_snap.exists
    if active and not social_snap.exists:
        txn.set(
            social_ref,
            {
                "agentId": agent_id,
                "workspaceId": workspace_id,
                "userId": req.auth.uid,
                "userEmail": token.get("email"),
                "action": action,
                "createdAt": now,
                "updatedAt": now,
                "meta": data.get("meta") if isinstance(data.get("meta"), dict) else {},
            },
            merge=True,
        )
        next_active = True
        changed = True
    elif not active and social_snap.exists:
        txn.delete(social_ref)
        next_active = False
        changed = True

    next_count = current_count
    if changed:
        next_count = current_count + 1 if next_active else max(0, current_count - 1)
        txn.set(agent_ref, {count_field: next_count, "updatedAt": now}, merge=True)

    txn.commit()

    # Follow action auto-enables default volatility alerts on holdings.
    if action == "follow" and next_active:
        holdings = agent_doc.get("holdings") if isinstance(agent_doc.get("holdings"), list) else []
        symbols: list[str] = []
        for item in holdings[:20]:
            if isinstance(item, str):
                symbol = str(item).upper().strip()
            elif isinstance(item, dict):
                symbol = str(item.get("symbol") or "").upper().strip()
            else:
                symbol = ""
            if symbol and re.match(r"^[A-Z0-9.\-]{1,12}$", symbol):
                symbols.append(symbol)
        symbols = list(dict.fromkeys(symbols))
        for symbol in symbols:
            alert_ref = db.collection("users").document(workspace_id).collection("price_alerts").document(
                f"volatility_follow_{agent_id}_{symbol}"
            )
            alert_ref.set(
                {
                    "workspaceId": workspace_id,
                    "userId": req.auth.uid,
                    "createdByUid": req.auth.uid,
                    "userEmail": token.get("email"),
                    "ticker": symbol,
                    "condition": "volatility",
                    "thresholdPercent": 0.05,
                    "notes": "Auto-enabled from AI Agent follow action.",
                    "active": True,
                    "triggered": False,
                    "createdAt": now,
                    "updatedAt": now,
                    "source": "ai_agent_follow",
                    "meta": data.get("meta") if isinstance(data.get("meta"), dict) else {},
                },
                merge=True,
            )

    _audit_event(
        req.auth.uid,
        token.get("email"),
        "ai_agent_social_action",
        {"workspaceId": workspace_id, "agentId": agent_id, "action": action, "active": next_active, "changed": changed},
    )

    return {
        "workspaceId": workspace_id,
        "agentId": agent_id,
        "action": action,
        "active": next_active,
        "countField": count_field,
        "count": next_count,
        "changed": changed,
    }


@https_fn.on_call()
def rename_prediction_upload(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    upload_id = str(data.get("uploadId") or data.get("id") or "").strip()
    title = str(data.get("title") or "").strip()
    if not upload_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Upload ID is required.")
    if not title:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Title is required.")
    if len(title) > 180:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Title is too long.")

    ref = db.collection("prediction_uploads").document(upload_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Upload not found.")

    doc = snap.to_dict() or {}
    owner_id = str(doc.get("userId") or "").strip()
    if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")

    ref.set({"title": title, "updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)
    _audit_event(req.auth.uid, token.get("email"), "predictions_renamed", {"uploadId": upload_id})
    return {"updated": True, "uploadId": upload_id, "title": title}


@https_fn.on_call()
def delete_prediction_upload(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    upload_id = str(data.get("uploadId") or data.get("id") or "").strip()
    if not upload_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Upload ID is required.")

    ref = db.collection("prediction_uploads").document(upload_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Upload not found.")

    doc = snap.to_dict() or {}
    owner_id = str(doc.get("userId") or "").strip()
    if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")

    file_path = str(doc.get("filePath") or "").strip()
    if file_path:
        try:
            bucket = admin_storage.bucket(STORAGE_BUCKET)
            bucket.blob(file_path).delete()
        except Exception:
            # Best-effort cleanup; still delete Firestore metadata.
            pass

    ref.delete()
    _audit_event(req.auth.uid, token.get("email"), "predictions_deleted", {"uploadId": upload_id})
    return {"deleted": True, "uploadId": upload_id}


@https_fn.on_call()
def get_prediction_upload_csv(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    upload_id = str(data.get("uploadId") or data.get("id") or "").strip()
    if not upload_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Upload ID is required.")

    max_bytes_raw = data.get("maxBytes")
    max_bytes = 2_000_000
    if max_bytes_raw is not None:
        try:
            max_bytes = int(max_bytes_raw)
        except Exception:
            max_bytes = 2_000_000
    max_bytes = max(25_000, min(max_bytes, 5_000_000))

    ref = db.collection("prediction_uploads").document(upload_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Upload not found.")

    doc = snap.to_dict() or {}
    owner_id = str(doc.get("userId") or "").strip()
    if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")

    file_path = str(doc.get("filePath") or "").strip()
    if not file_path:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, "Upload is missing a storage path.")

    try:
        bucket = admin_storage.bucket(STORAGE_BUCKET)
        blob = bucket.blob(file_path)
        if not blob.exists():
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "CSV file not found in storage.")

        truncated = False
        size = 0
        try:
            blob.reload()
            size = int(blob.size or 0)
        except Exception:
            size = 0

        data_bytes = b""
        if size and size > max_bytes:
            truncated = True
            try:
                with blob.open("rb") as handle:
                    data_bytes = handle.read(max_bytes)
            except Exception:
                data_bytes = blob.download_as_bytes()[:max_bytes]
        else:
            data_bytes = blob.download_as_bytes()
            if len(data_bytes) > max_bytes:
                truncated = True
                data_bytes = data_bytes[:max_bytes]

        text = data_bytes.decode("utf-8", errors="replace")
        _audit_event(req.auth.uid, token.get("email"), "predictions_csv_fetched", {"uploadId": upload_id, "truncated": truncated})
        return {
            "uploadId": upload_id,
            "title": doc.get("title") or "",
            "filePath": file_path,
            "bytes": len(data_bytes),
            "truncated": truncated,
            "csv": text,
        }
    except https_fn.HttpsError:
        raise
    except Exception:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INTERNAL, "Unable to fetch CSV.")


def _prediction_agent_fallback(summary: dict[str, Any], ticker: str) -> str:
    quantile = str(summary.get("selectedQuantileLabel") or summary.get("selectedQuantile") or "N/A")
    point = summary.get("pointForecastValue")
    first_date = str(summary.get("firstRowDate") or "")
    last_date = str(summary.get("lastRowDate") or "")
    relation = str(summary.get("relation") or "").strip()
    warning = str(summary.get("warningText") or "").strip()
    pieces = [
        f"Ticker: {ticker or 'N/A'}.",
        f"Selected quantile: {quantile}.",
        f"Point forecast on last weekday row: {point}.",
    ]
    if first_date and last_date:
        pieces.append(f"Prediction window: {first_date} -> {last_date}.")
    if relation:
        pieces.append(relation)
    if warning:
        pieces.append(f"Weekday warning: {warning}")
    pieces.append("OpenAI commentary is unavailable; this is the deterministic fallback summary.")
    return " ".join(pieces)


@https_fn.on_call()
def run_prediction_upload_agent(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    upload_id = str(data.get("uploadId") or data.get("id") or "").strip()
    if not upload_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Upload ID is required.")

    ref = db.collection("prediction_uploads").document(upload_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Upload not found.")

    doc = snap.to_dict() or {}
    owner_id = str(doc.get("userId") or "").strip()
    if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")

    mapping_summary = data.get("mappingSummary") if isinstance(data.get("mappingSummary"), dict) else {}
    ticker = str(data.get("ticker") or mapping_summary.get("ticker") or doc.get("ticker") or "").upper().strip()
    if not ticker:
        ticker = "UNKNOWN"

    # Trim very long cells before sending to model.
    compact_summary = dict(mapping_summary or {})
    for key in ("firstRowText", "lastRowText", "relation", "warningText"):
        if key in compact_summary and compact_summary[key] is not None:
            compact_summary[key] = str(compact_summary[key])[:1200]

    fallback_text = _prediction_agent_fallback(compact_summary, ticker)
    analysis = fallback_text
    model_used = "template_fallback"
    provider = "fallback"

    if OPENAI_API_KEY:
        model_used = str(os.environ.get("PREDICTION_AGENT_MODEL") or "gpt-4o-mini").strip() or "gpt-4o-mini"
        system_prompt = (
            "You are a market data QA assistant for Quantura. "
            "Given CSV-derived quantile mapping metadata, write a concise analyst note. "
            "Do not provide financial advice, guarantees, or return promises. "
            "Output plain text only."
        )
        user_prompt = (
            "Write a compact report with these headings:\n"
            "1) Mapping result\n"
            "2) Weekday checks\n"
            "3) Risk notes\n"
            "4) Next validation step\n\n"
            f"Ticker: {ticker}\n"
            f"Upload title: {doc.get('title') or 'predictions.csv'}\n"
            f"Summary JSON: {json.dumps(compact_summary, ensure_ascii=True)}"
        )
        payload = {
            "model": model_used,
            "temperature": 0.2,
            "max_tokens": 450,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=30,
            )
            response.raise_for_status()
            body = response.json()
            message = body.get("choices", [{}])[0].get("message", {})
            content = str(message.get("content") or "").strip()
            if content:
                analysis = content
                provider = "openai"
            else:
                analysis = fallback_text
                provider = "fallback"
        except Exception:
            analysis = fallback_text
            provider = "fallback"

    _audit_event(
        req.auth.uid,
        token.get("email"),
        "predictions_agent_run",
        {"uploadId": upload_id, "provider": provider, "model": model_used, "ticker": ticker},
    )
    return {
        "uploadId": upload_id,
        "ticker": ticker,
        "analysis": analysis,
        "provider": provider,
        "model": model_used,
    }


def _is_paid_user(uid: str) -> bool:
    try:
        docs = db.collection("orders").where("userId", "==", uid).limit(25).stream()
        for doc in docs:
            data = doc.to_dict() or {}
            status = str(data.get("paymentStatus") or "").strip().lower()
            if status in {"paid", "succeeded"}:
                return True
    except Exception:
        return False
    return False


def _enforce_daily_usage(uid: str, feature_key: str, limit: int, limit_message: str | None = None) -> None:
    if limit <= 0:
        return
    day_key = datetime.now(timezone.utc).date().isoformat()
    ref = db.collection("usage_daily").document(f"{uid}_{day_key}")
    transaction = db.transaction()
    snap = ref.get(transaction=transaction)
    existing = snap.to_dict() if snap.exists else {}
    count = int(existing.get(feature_key) or 0)
    if count >= limit:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.RESOURCE_EXHAUSTED,
            limit_message or "Daily usage limit reached.",
        )
    transaction.set(
        ref,
        {
            "userId": uid,
            "date": day_key,
            feature_key: count + 1,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    transaction.commit()


def _generate_backtest_code(payload: dict[str, Any]) -> str:
    ticker = str(payload.get("ticker") or "").upper().strip()
    interval = str(payload.get("interval") or "1d").strip()
    lookback_days = int(payload.get("lookbackDays") or 730)
    strategy = str(payload.get("strategy") or "sma_cross").strip().lower()
    params = payload.get("params") or {}
    cash = float(payload.get("cash") or 10_000)
    commission = float(payload.get("commission") or 0.0)

    # Keep emitted code self-contained so users can run it locally.
    fast = int(params.get("fast") or 20)
    slow = int(params.get("slow") or 50)
    rsi_period = int(params.get("rsiPeriod") or 14)
    oversold = float(params.get("oversold") or 30)
    exit_above = float(params.get("exitAbove") or 55)
    sl_pct = max(0.0, min(float(params.get("slPct") or params.get("sl") or 0.0), 0.5))
    tp_pct = max(0.0, min(float(params.get("tpPct") or params.get("tp") or 0.0), 0.5))
    strategy_class = "RsiReversion" if strategy == "rsi_reversion" else "SmaCross"

    if strategy == "rsi_reversion":
        strategy_code = f"""
class RsiReversion(Strategy):
    rsi_period = {rsi_period}
    oversold = {oversold}
    exit_above = {exit_above}
    sl_pct = {sl_pct}
    tp_pct = {tp_pct}

    def init(self):
        self.rsi = self.I(calc_rsi, self.data.Close, self.rsi_period)

    def _risk_args(self):
        last_close = float(self.data.Close[-1])
        out = {{}}
        if self.sl_pct > 0:
            out["sl"] = last_close * (1 - self.sl_pct)
        if self.tp_pct > 0:
            out["tp"] = last_close * (1 + self.tp_pct)
        return out

    def next(self):
        if not self.position and self.rsi[-1] < self.oversold:
            self.buy(**self._risk_args())
        elif self.position and self.rsi[-1] > self.exit_above:
            self.position.close()
"""
    else:
        strategy_code = f"""
class SmaCross(Strategy):
    fast = {fast}
    slow = {slow}
    sl_pct = {sl_pct}
    tp_pct = {tp_pct}

    def init(self):
        price = self.data.Close
        self.sma_fast = self.I(SMA, price, self.fast)
        self.sma_slow = self.I(SMA, price, self.slow)

    def _risk_args(self):
        last_close = float(self.data.Close[-1])
        out = {{}}
        if self.sl_pct > 0:
            out["sl"] = last_close * (1 - self.sl_pct)
        if self.tp_pct > 0:
            out["tp"] = last_close * (1 + self.tp_pct)
        return out

    def next(self):
        if crossover(self.sma_fast, self.sma_slow):
            self.position.close()
            self.buy(**self._risk_args())
        elif crossover(self.sma_slow, self.sma_fast):
            self.position.close()
"""

    return (
        f"""# Quantura backtest export (generated)
# pip install yfinance backtesting pandas matplotlib

from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import SMA


def calc_rsi(series, period=14):
    s = pd.Series(series)
    delta = s.diff()
    up = delta.clip(lower=0).rolling(period).mean()
    down = (-delta.clip(upper=0)).rolling(period).mean()
    rs = up / down
    return 100 - (100 / (1 + rs))


{strategy_code.strip()}


def main():
    ticker = {ticker!r}
    interval = {interval!r}
    lookback_days = {lookback_days}
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback_days)

    df = yf.download(
        ticker,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval=interval,
        progress=False,
        auto_adjust=False,
        threads=False,
    )
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.dropna()
    df.index = pd.to_datetime(df.index, errors="coerce")
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)

    bt = Backtest(df, {strategy_class}, cash={cash}, commission={commission})
    stats = bt.run()
    print(stats)
    bt.plot()


if __name__ == "__main__":
    main()
"""
    ).strip()


def _render_backtest_tradingview_code(payload: dict[str, Any]) -> str:
    ticker = str(payload.get("ticker") or "").upper().strip()
    strategy = str(payload.get("strategy") or "sma_cross").strip().lower()
    params = payload.get("params") or {}
    cash = float(payload.get("cash") or 10_000)
    commission = float(payload.get("commission") or 0.0)

    fast = int(params.get("fast") or 20)
    slow = int(params.get("slow") or 50)
    rsi_period = int(params.get("rsiPeriod") or 14)
    oversold = float(params.get("oversold") or 30)
    exit_above = float(params.get("exitAbove") or 55)
    sl_pct = max(0.0, min(float(params.get("slPct") or params.get("sl") or 0.0), 0.5))
    tp_pct = max(0.0, min(float(params.get("tpPct") or params.get("tp") or 0.0), 0.5))

    label = "RSI Reversion" if strategy == "rsi_reversion" else "SMA Crossover"
    lines = [
        "//@version=5",
        f"strategy(\"Quantura {label} ({ticker})\", overlay=true, initial_capital={int(cash)}, commission_type=strategy.commission.percent, commission_value={commission * 100.0:.6f})",
        "",
        f"slPct = input.float({sl_pct:.6f}, \"Stop loss (decimal)\", step=0.001, minval=0.0)",
        f"tpPct = input.float({tp_pct:.6f}, \"Take profit (decimal)\", step=0.001, minval=0.0)",
    ]

    if strategy == "rsi_reversion":
        lines.extend(
            [
                f"rsiPeriod = input.int({rsi_period}, \"RSI Period\", minval=2, maxval=60)",
                f"oversold = input.float({oversold:.2f}, \"Oversold\", minval=1, maxval=49)",
                f"exitAbove = input.float({exit_above:.2f}, \"Exit Above\", minval=50, maxval=95)",
                "rsiValue = ta.rsi(close, rsiPeriod)",
                "longCondition = rsiValue < oversold",
                "exitCondition = rsiValue > exitAbove",
            ]
        )
    else:
        lines.extend(
            [
                f"fastSmaLen = input.int({fast}, \"Fast SMA\", minval=2, maxval=200)",
                f"slowSmaLen = input.int({slow}, \"Slow SMA\", minval=5, maxval=400)",
                "fastSma = ta.sma(close, fastSmaLen)",
                "slowSma = ta.sma(close, slowSmaLen)",
                "longCondition = ta.crossover(fastSma, slowSma)",
                "exitCondition = ta.crossunder(fastSma, slowSma)",
                "plot(fastSma, color=color.new(color.aqua, 0), title=\"Fast SMA\")",
                "plot(slowSma, color=color.new(color.orange, 0), title=\"Slow SMA\")",
            ]
        )

    lines.extend(
        [
            "",
            "if longCondition and strategy.position_size <= 0",
            "    strategy.entry(\"Long\", strategy.long)",
            "",
            "if strategy.position_size > 0",
            "    stopPrice = slPct > 0 ? strategy.position_avg_price * (1 - slPct) : na",
            "    takePrice = tpPct > 0 ? strategy.position_avg_price * (1 + tpPct) : na",
            "    strategy.exit(\"Risk Exit\", from_entry=\"Long\", stop=stopPrice, limit=takePrice)",
            "",
            "if exitCondition and strategy.position_size > 0",
            "    strategy.close(\"Long\")",
            "",
            "// Generated by Quantura Export Strategy Logic",
        ]
    )
    return "\n".join(lines).strip()


def _render_backtest_mq5_code(payload: dict[str, Any]) -> str:
    ticker = str(payload.get("ticker") or "").upper().strip()
    strategy = str(payload.get("strategy") or "sma_cross").strip().lower()
    params = payload.get("params") or {}
    commission = float(payload.get("commission") or 0.0)
    lookback_days = int(payload.get("lookbackDays") or 730)

    fast = int(params.get("fast") or 20)
    slow = int(params.get("slow") or 50)
    rsi_period = int(params.get("rsiPeriod") or 14)
    oversold = float(params.get("oversold") or 30)
    exit_above = float(params.get("exitAbove") or 55)
    sl_pct = max(0.0, min(float(params.get("slPct") or params.get("sl") or 0.0), 0.5))
    tp_pct = max(0.0, min(float(params.get("tpPct") or params.get("tp") or 0.0), 0.5))

    lines = [
        "#property strict",
        "#include <Trade/Trade.mqh>",
        "",
        f"// Quantura template for {ticker} ({strategy})",
        f"// Backtest lookback used in Quantura: {lookback_days} days",
        f"// Commission in Quantura run: {commission:.6f}",
        "input double LotSize = 0.10;",
        f"input double StopLossPct = {sl_pct:.6f};",
        f"input double TakeProfitPct = {tp_pct:.6f};",
        "",
        "CTrade trade;",
        "double ReadBuffer(int handle, int shift)",
        "{",
        "  double values[];",
        "  if(CopyBuffer(handle, 0, shift, 1, values) <= 0) return EMPTY_VALUE;",
        "  return values[0];",
        "}",
        "",
        "void ApplyRisk()",
        "{",
        "  if(!PositionSelect(_Symbol)) return;",
        "  double openPrice = PositionGetDouble(POSITION_PRICE_OPEN);",
        "  double sl = 0.0;",
        "  double tp = 0.0;",
        "  if(StopLossPct > 0.0) sl = NormalizeDouble(openPrice * (1.0 - StopLossPct), _Digits);",
        "  if(TakeProfitPct > 0.0) tp = NormalizeDouble(openPrice * (1.0 + TakeProfitPct), _Digits);",
        "  trade.PositionModify(_Symbol, sl, tp);",
        "}",
        "",
    ]

    if strategy == "rsi_reversion":
        lines.extend(
            [
                f"input int RsiPeriod = {rsi_period};",
                f"input double Oversold = {oversold:.2f};",
                f"input double ExitAbove = {exit_above:.2f};",
                "int rsiHandle = INVALID_HANDLE;",
                "",
                "int OnInit()",
                "{",
                "  rsiHandle = iRSI(_Symbol, PERIOD_CURRENT, RsiPeriod, PRICE_CLOSE);",
                "  if(rsiHandle == INVALID_HANDLE) return(INIT_FAILED);",
                "  return(INIT_SUCCEEDED);",
                "}",
                "",
                "void OnDeinit(const int reason)",
                "{",
                "  if(rsiHandle != INVALID_HANDLE) IndicatorRelease(rsiHandle);",
                "}",
                "",
                "void OnTick()",
                "{",
                "  double rsiNow = ReadBuffer(rsiHandle, 0);",
                "  if(rsiNow == EMPTY_VALUE) return;",
                "  bool hasPosition = PositionSelect(_Symbol);",
                "  if(!hasPosition && rsiNow < Oversold)",
                "  {",
                "    trade.Buy(LotSize, _Symbol);",
                "    ApplyRisk();",
                "  }",
                "  if(hasPosition && rsiNow > ExitAbove)",
                "  {",
                "    trade.PositionClose(_Symbol);",
                "  }",
                "}",
            ]
        )
    else:
        lines.extend(
            [
                f"input int FastSma = {fast};",
                f"input int SlowSma = {slow};",
                "int fastHandle = INVALID_HANDLE;",
                "int slowHandle = INVALID_HANDLE;",
                "",
                "int OnInit()",
                "{",
                "  fastHandle = iMA(_Symbol, PERIOD_CURRENT, FastSma, 0, MODE_SMA, PRICE_CLOSE);",
                "  slowHandle = iMA(_Symbol, PERIOD_CURRENT, SlowSma, 0, MODE_SMA, PRICE_CLOSE);",
                "  if(fastHandle == INVALID_HANDLE || slowHandle == INVALID_HANDLE) return(INIT_FAILED);",
                "  return(INIT_SUCCEEDED);",
                "}",
                "",
                "void OnDeinit(const int reason)",
                "{",
                "  if(fastHandle != INVALID_HANDLE) IndicatorRelease(fastHandle);",
                "  if(slowHandle != INVALID_HANDLE) IndicatorRelease(slowHandle);",
                "}",
                "",
                "void OnTick()",
                "{",
                "  double fastNow = ReadBuffer(fastHandle, 0);",
                "  double fastPrev = ReadBuffer(fastHandle, 1);",
                "  double slowNow = ReadBuffer(slowHandle, 0);",
                "  double slowPrev = ReadBuffer(slowHandle, 1);",
                "  if(fastNow == EMPTY_VALUE || fastPrev == EMPTY_VALUE || slowNow == EMPTY_VALUE || slowPrev == EMPTY_VALUE) return;",
                "  bool crossUp = fastPrev <= slowPrev && fastNow > slowNow;",
                "  bool crossDown = fastPrev >= slowPrev && fastNow < slowNow;",
                "  bool hasPosition = PositionSelect(_Symbol);",
                "  if(crossUp && !hasPosition)",
                "  {",
                "    trade.Buy(LotSize, _Symbol);",
                "    ApplyRisk();",
                "  }",
                "  if(crossDown && hasPosition)",
                "  {",
                "    trade.PositionClose(_Symbol);",
                "  }",
                "}",
            ]
        )

    return "\n".join(lines).strip()


def _render_tradelocker_payload(payload: dict[str, Any]) -> str:
    ticker = str(payload.get("ticker") or "").upper().strip()
    interval = str(payload.get("interval") or "1d").strip()
    strategy = str(payload.get("strategy") or "sma_cross").strip().lower()
    params = payload.get("params") or {}
    cash = float(payload.get("cash") or 10_000)
    commission = float(payload.get("commission") or 0.0)
    sl_pct = max(0.0, min(float(params.get("slPct") or params.get("sl") or 0.0), 0.5))
    tp_pct = max(0.0, min(float(params.get("tpPct") or params.get("tp") or 0.0), 0.5))

    if strategy == "rsi_reversion":
        entry_rules = [f"RSI({int(params.get('rsiPeriod') or 14)}) < {float(params.get('oversold') or 30):.2f}"]
        exit_rules = [f"RSI({int(params.get('rsiPeriod') or 14)}) > {float(params.get('exitAbove') or 55):.2f}"]
    else:
        entry_rules = [f"SMA({int(params.get('fast') or 20)}) crosses above SMA({int(params.get('slow') or 50)})"]
        exit_rules = [f"SMA({int(params.get('fast') or 20)}) crosses below SMA({int(params.get('slow') or 50)})"]

    doc = {
        "provider": "tradelocker",
        "version": "1.0",
        "strategyName": f"quantura_{strategy}",
        "symbol": ticker,
        "interval": interval,
        "execution": {
            "side": "buy",
            "orderType": "market",
            "quantityType": "notional",
            "notionalUsd": round(cash, 2),
            "commissionRate": round(commission, 6),
        },
        "risk": {
            "stopLossPct": round(sl_pct, 6),
            "takeProfitPct": round(tp_pct, 6),
        },
        "entryRules": entry_rules,
        "exitRules": exit_rules,
        "notes": [
            "Generated by Quantura Export Strategy Logic.",
            "Map these rules into your TradeLocker webhook bot or strategy runner.",
        ],
    }
    return json.dumps(doc, indent=2, sort_keys=False)


def _build_backtest_export_sources(payload: dict[str, Any]) -> dict[str, dict[str, str]]:
    ticker = str(payload.get("ticker") or "backtest").upper().strip() or "backtest"
    strategy = str(payload.get("strategy") or "strategy").strip().lower()
    safe_prefix = re.sub(r"[^A-Z0-9_-]+", "_", f"{ticker}_{strategy}")
    python_code = _generate_backtest_code(payload)

    return {
        "python": {
            "label": "Python (.py)",
            "filename": f"{safe_prefix}.py",
            "mimeType": "text/x-python",
            "content": python_code,
        },
        "tradingview": {
            "label": "TradingView (.pine)",
            "filename": f"{safe_prefix}.pine",
            "mimeType": "text/plain",
            "content": _render_backtest_tradingview_code(payload),
        },
        "metatrader5": {
            "label": "MetaTrader 5 (.mq5)",
            "filename": f"{safe_prefix}.mq5",
            "mimeType": "text/plain",
            "content": _render_backtest_mq5_code(payload),
        },
        "tradelocker": {
            "label": "TradeLocker (JSON)",
            "filename": f"{safe_prefix}_tradelocker.json",
            "mimeType": "application/json",
            "content": _render_tradelocker_payload(payload),
        },
    }


@https_fn.on_call()
def run_backtest(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    context = _remote_config_context(req, token, meta)

    if not _remote_config_bool("backtesting_enabled", True, context=context):
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, "Backtesting is temporarily disabled.")

    ticker = str(data.get("ticker") or "").upper().strip()
    if not ticker or len(ticker) > 12 or not all(ch.isalnum() or ch in {".", "-"} for ch in ticker):
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    interval = str(data.get("interval") or "1d").strip()
    if interval not in {"1d", "1h"}:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid interval.")

    strategy = str(data.get("strategy") or "sma_cross").strip().lower()
    if strategy not in {"sma_cross", "rsi_reversion"}:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid strategy.")

    lookback_days = int(data.get("lookbackDays") or data.get("lookback") or 730)
    if lookback_days < 30 or lookback_days > 5000:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Lookback must be between 30 and 5000 days.")

    cash = float(data.get("cash") or 10_000)
    commission = float(data.get("commission") or 0.0)
    if cash <= 0 or cash > 10_000_000:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid cash amount.")
    if commission < 0 or commission > 0.05:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid commission.")

    params_raw = data.get("params") if isinstance(data.get("params"), dict) else {}
    sl_pct = float(params_raw.get("slPct") or params_raw.get("sl") or 0.0)
    tp_pct = float(params_raw.get("tpPct") or params_raw.get("tp") or 0.0)
    if sl_pct < 0 or sl_pct > 0.5:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Stop-loss percent must be between 0 and 0.5.")
    if tp_pct < 0 or tp_pct > 0.5:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Take-profit percent must be between 0 and 0.5.")

    params: dict[str, Any] = {}
    if strategy == "sma_cross":
        fast = int(params_raw.get("fast") or 20)
        slow = int(params_raw.get("slow") or 50)
        if fast < 2 or slow < 5 or fast >= slow or slow > 400:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid SMA parameters.")
        params = {"fast": fast, "slow": slow, "slPct": sl_pct, "tpPct": tp_pct}
    else:
        rsi_period = int(params_raw.get("rsiPeriod") or params_raw.get("period") or 14)
        oversold = float(params_raw.get("oversold") or 30)
        exit_above = float(params_raw.get("exitAbove") or 55)
        if rsi_period < 2 or rsi_period > 60:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid RSI period.")
        if oversold <= 0 or oversold >= 50:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid RSI oversold threshold.")
        if exit_above <= 50 or exit_above >= 95:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid RSI exit threshold.")
        params = {"rsiPeriod": rsi_period, "oversold": oversold, "exitAbove": exit_above, "slPct": sl_pct, "tpPct": tp_pct}

    is_pro = token.get("email") == ADMIN_EMAIL or _is_paid_user(req.auth.uid)
    free_limit = _remote_config_int("backtesting_free_daily_limit", 1, context=context)
    pro_limit = _remote_config_int("backtesting_pro_daily_limit", 25, context=context)
    _enforce_daily_usage(
        req.auth.uid,
        "backtestRuns",
        pro_limit if is_pro else free_limit,
        "Daily usage limit reached. Upgrade your plan to run more backtests.",
    )

    from datetime import timedelta  # local import to reduce cold start
    import io

    try:
        import pandas as pd  # type: ignore
        import yfinance as yf  # type: ignore
        from backtesting import Backtest, Strategy  # type: ignore
        from backtesting.lib import crossover  # type: ignore
        from backtesting.test import SMA  # type: ignore
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as exc:
        _raise_structured_error(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "backtest_dependency_error",
            "Backtest dependencies are unavailable.",
            {"raw": str(exc)},
        )

    def calc_rsi(series, period: int = 14):
        s = pd.Series(series)
        delta = s.diff()
        up = delta.clip(lower=0).rolling(period).mean()
        down = (-delta.clip(upper=0)).rolling(period).mean()
        rs = up / down
        return 100 - (100 / (1 + rs))

    if strategy == "rsi_reversion":

        class RsiReversion(Strategy):  # type: ignore
            rsi_period = int(params["rsiPeriod"])
            oversold = float(params["oversold"])
            exit_above = float(params["exitAbove"])
            sl_pct = float(params.get("slPct") or 0.0)
            tp_pct = float(params.get("tpPct") or 0.0)

            def _risk_args(self):
                last_close = float(self.data.Close[-1])
                out: dict[str, float] = {}
                if self.sl_pct > 0:
                    out["sl"] = last_close * (1 - self.sl_pct)
                if self.tp_pct > 0:
                    out["tp"] = last_close * (1 + self.tp_pct)
                return out

            def init(self):
                self.rsi = self.I(calc_rsi, self.data.Close, self.rsi_period)

            def next(self):
                if not self.position and self.rsi[-1] < self.oversold:
                    self.buy(**self._risk_args())
                elif self.position and self.rsi[-1] > self.exit_above:
                    self.position.close()

        strategy_cls = RsiReversion
        strategy_name = "RSI reversion"
    else:

        class SmaCross(Strategy):  # type: ignore
            fast = int(params["fast"])
            slow = int(params["slow"])
            sl_pct = float(params.get("slPct") or 0.0)
            tp_pct = float(params.get("tpPct") or 0.0)

            def _risk_args(self):
                last_close = float(self.data.Close[-1])
                out: dict[str, float] = {}
                if self.sl_pct > 0:
                    out["sl"] = last_close * (1 - self.sl_pct)
                if self.tp_pct > 0:
                    out["tp"] = last_close * (1 + self.tp_pct)
                return out

            def init(self):
                price = self.data.Close
                self.sma_fast = self.I(SMA, price, self.fast)
                self.sma_slow = self.I(SMA, price, self.slow)

            def next(self):
                if crossover(self.sma_fast, self.sma_slow):
                    self.position.close()
                    self.buy(**self._risk_args())
                elif crossover(self.sma_slow, self.sma_fast):
                    self.position.close()

        strategy_cls = SmaCross
        strategy_name = "SMA crossover"

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback_days)
    try:
        df = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval=interval,
            progress=False,
            auto_adjust=False,
            threads=False,
        )
    except Exception as exc:
        _raise_structured_error(
            https_fn.FunctionsErrorCode.NOT_FOUND,
            "ticker_not_found",
            "Unable to load ticker history for backtest.",
            {"ticker": ticker, "raw": str(exc)},
        )
    if df is None or getattr(df, "empty", True):
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, "No price history returned.")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.dropna()
    df.index = pd.to_datetime(df.index, errors="coerce")
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)

    for col in ["Open", "High", "Low", "Close"]:
        if col not in df.columns:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, f"Missing column: {col}")

    try:
        bt = Backtest(df, strategy_cls, cash=cash, commission=commission)
        stats = bt.run()
        equity = None
        try:
            equity = stats.get("_equity_curve") if hasattr(stats, "get") else None
        except Exception:
            equity = None

        fig = plt.figure(figsize=(10.5, 4.4), dpi=160)
        ax = fig.add_subplot(111)
        ax.set_title(f"{ticker} backtest: {strategy_name}")
        ax.set_xlabel("Date")
        ax.set_ylabel("Equity (USD)")
        ax.grid(True, alpha=0.22)
        if equity is not None and hasattr(equity, "__getitem__") and "Equity" in equity:
            ax.plot(equity.index, equity["Equity"], linewidth=1.8)
        else:
            # Fall back to close price if equity curve is unavailable.
            ax.plot(df.index, df["Close"], linewidth=1.6)
            ax.set_ylabel("Close (USD)")
        fig.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        png_bytes = buf.getvalue()
    except https_fn.HttpsError:
        raise
    except Exception as exc:
        _raise_structured_error(
            https_fn.FunctionsErrorCode.INTERNAL,
            "backtest_execution_error",
            "Backtest execution failed.",
            {"ticker": ticker, "strategy": strategy, "raw": str(exc)},
        )

    export_payload = {
        "ticker": ticker,
        "interval": interval,
        "lookbackDays": lookback_days,
        "strategy": strategy,
        "params": params,
        "cash": cash,
        "commission": commission,
    }
    export_sources = _build_backtest_export_sources(export_payload)
    code = export_sources.get("python", {}).get("content") or ""

    doc_ref = db.collection("backtests").document()
    backtest_id = doc_ref.id
    image_path = f"backtests/{req.auth.uid}/{backtest_id}.png"

    try:
        bucket = admin_storage.bucket(STORAGE_BUCKET)
        bucket.blob(image_path).upload_from_string(png_bytes, content_type="image/png")
    except Exception:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INTERNAL, "Unable to store backtest chart.")

    metrics = {
        "ReturnPct": float(stats.get("Return [%]", 0.0)) if hasattr(stats, "get") else 0.0,
        "Sharpe": float(stats.get("Sharpe Ratio", 0.0)) if hasattr(stats, "get") else 0.0,
        "MaxDrawdownPct": float(stats.get("Max. Drawdown [%]", 0.0)) if hasattr(stats, "get") else 0.0,
        "Trades": int(stats.get("# Trades", 0)) if hasattr(stats, "get") else 0,
        "WinRatePct": float(stats.get("Win Rate [%]", 0.0)) if hasattr(stats, "get") else 0.0,
    }

    title = str(data.get("title") or f"{ticker} · {strategy_name}").strip()
    if len(title) > 180:
        title = title[:180]

    doc_ref.set(
        {
            "userId": req.auth.uid,
            "title": title,
            "ticker": ticker,
            "interval": interval,
            "lookbackDays": lookback_days,
            "strategy": strategy,
            "params": params,
            "cash": cash,
            "commission": commission,
            "status": "completed",
            "metrics": metrics,
            "imagePath": image_path,
            "code": code,
            "exportSources": export_sources,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "meta": meta,
        }
    )

    _audit_event(req.auth.uid, token.get("email"), "backtest_completed", {"backtestId": backtest_id, "ticker": ticker, "strategy": strategy})
    return {
        "backtestId": backtest_id,
        "metrics": metrics,
        "imagePath": image_path,
        "title": title,
        "exportFormats": sorted(BACKTEST_SOURCE_FORMATS),
    }


@https_fn.on_call()
def rename_backtest(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    backtest_id = str(data.get("backtestId") or data.get("id") or "").strip()
    title = str(data.get("title") or "").strip()
    if not backtest_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Backtest ID is required.")
    if not title:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Title is required.")
    if len(title) > 180:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Title is too long.")

    ref = db.collection("backtests").document(backtest_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Backtest not found.")
    doc = snap.to_dict() or {}
    owner_id = str(doc.get("userId") or "").strip()
    if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")

    ref.set({"title": title, "updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)
    _audit_event(req.auth.uid, token.get("email"), "backtest_renamed", {"backtestId": backtest_id})
    return {"updated": True, "backtestId": backtest_id, "title": title}


@https_fn.on_call()
def delete_backtest(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    backtest_id = str(data.get("backtestId") or data.get("id") or "").strip()
    if not backtest_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Backtest ID is required.")

    ref = db.collection("backtests").document(backtest_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Backtest not found.")
    doc = snap.to_dict() or {}
    owner_id = str(doc.get("userId") or "").strip()
    if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")

    image_path = str(doc.get("imagePath") or "").strip()
    if image_path:
        try:
            bucket = admin_storage.bucket(STORAGE_BUCKET)
            bucket.blob(image_path).delete()
        except Exception:
            pass

    ref.delete()
    _audit_event(req.auth.uid, token.get("email"), "backtest_deleted", {"backtestId": backtest_id})
    return {"deleted": True, "backtestId": backtest_id}


@https_fn.on_call()
def create_share_link(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    kind = str(data.get("kind") or "").strip().lower()
    source_id = str(data.get("id") or "").strip()
    if kind not in {"forecast", "screener", "upload"}:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid share kind.")
    if not source_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Source ID is required.")

    collection = "forecast_requests" if kind == "forecast" else "screener_runs" if kind == "screener" else "prediction_uploads"
    snap = db.collection(collection).document(source_id).get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Source document not found.")

    doc = snap.to_dict() or {}
    owner_id = str(doc.get("userId") or "").strip()
    if kind in {"forecast", "screener"}:
        if not owner_id:
            _require_admin(token)
        else:
            _require_workspace_access(owner_id, req.auth.uid, token)
    else:
        if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")

    share_ref = db.collection("shares").document()
    share_doc = {
        "kind": kind,
        "sourceCollection": collection,
        "sourceId": source_id,
        "sourceUserId": owner_id,
        "createdByUid": req.auth.uid,
        "createdByEmail": token.get("email"),
        "title": doc.get("title") or doc.get("ticker") or "",
        "ticker": doc.get("ticker") or "",
        "createdAt": firestore.SERVER_TIMESTAMP,
    }
    share_ref.set(share_doc)

    path = "/forecasting" if kind == "forecast" else "/screener" if kind == "screener" else "/uploads"
    share_url = f"{PUBLIC_ORIGIN}{path}?share={share_ref.id}"
    _audit_event(req.auth.uid, token.get("email"), "share_created", {"shareId": share_ref.id, "kind": kind})
    return {"shareId": share_ref.id, "shareUrl": share_url, "kind": kind}


@https_fn.on_call()
def import_shared_item(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    share_id = str(data.get("shareId") or "").strip()
    if not share_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Share ID is required.")

    share_snap = db.collection("shares").document(share_id).get()
    if not share_snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Share link not found.")

    share_doc = share_snap.to_dict() or {}
    kind = str(share_doc.get("kind") or "").strip().lower()
    source_collection = str(share_doc.get("sourceCollection") or "").strip()
    source_id = str(share_doc.get("sourceId") or "").strip()
    if kind not in {"forecast", "screener", "upload"} or not source_collection or not source_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, "Share link is invalid.")

    destination_workspace_id = str(data.get("workspaceId") or req.auth.uid or "").strip() or req.auth.uid
    if destination_workspace_id != req.auth.uid:
        _require_workspace_editor(destination_workspace_id, req.auth.uid, token)

    import_key = f"{destination_workspace_id}_{share_id}"
    import_ref = db.collection("share_imports").document(import_key)
    existing = import_ref.get()
    if existing.exists:
        payload = existing.to_dict() or {}
        imported_id = str(payload.get("importedId") or "").strip()
        if imported_id:
            return {"kind": kind, "importedId": imported_id, "shareId": share_id}

    source_snap = db.collection(source_collection).document(source_id).get()
    if not source_snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Source document no longer exists.")

    source = source_snap.to_dict() or {}
    imported_ref = db.collection(source_collection).document()
    imported_doc: dict[str, Any] = {
        "userId": destination_workspace_id,
        "userEmail": token.get("email"),
        "createdByUid": req.auth.uid,
        "createdByEmail": token.get("email"),
        "status": source.get("status") or "completed",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "sourceShareId": share_id,
        "sourceId": source_id,
        "sourceUserId": str(source.get("userId") or ""),
        "meta": source.get("meta") or {},
    }

    if kind == "forecast":
        for key in [
            "ticker",
            "interval",
            "horizon",
            "start",
            "quantiles",
            "service",
            "engine",
            "serviceMessage",
            "metrics",
            "forecastPreview",
            "forecastRows",
        ]:
            if key in source:
                imported_doc[key] = source.get(key)
        imported_doc["title"] = source.get("title") or f"{source.get('ticker') or ''} forecast".strip()
    elif kind == "screener":
        for key in ["market", "universe", "maxNames", "results", "notes"]:
            if key in source:
                imported_doc[key] = source.get(key)
        imported_doc["title"] = source.get("title") or "Screener run"
    else:
        title = str(source.get("title") or "predictions.csv")
        notes = str(source.get("notes") or "")
        file_path = str(source.get("filePath") or "").strip()
        new_path = ""
        if file_path:
            try:
                bucket = admin_storage.bucket(STORAGE_BUCKET)
                src_blob = bucket.blob(file_path)
                if src_blob.exists():
                    base = os.path.basename(file_path)
                    new_path = f"predictions/{destination_workspace_id}/{int(time.time()*1000)}_{base}"
                    bucket.copy_blob(src_blob, bucket, new_path)
            except Exception:
                new_path = ""

        imported_doc.update(
            {
                "title": title,
                "notes": notes,
                "ticker": str(source.get("ticker") or "").upper(),
                "status": "uploaded",
                "filePath": new_path or file_path,
                "fileUrl": "",
            }
        )

    imported_ref.set(imported_doc)
    import_ref.set(
        {
            "shareId": share_id,
            "kind": kind,
            "importedCollection": source_collection,
            "importedId": imported_ref.id,
            "workspaceId": destination_workspace_id,
            "createdByUid": req.auth.uid,
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )

    _audit_event(req.auth.uid, token.get("email"), "share_imported", {"shareId": share_id, "kind": kind})
    return {"kind": kind, "importedId": imported_ref.id, "shareId": share_id}


@https_fn.on_call()
def get_ticker_history(req: https_fn.CallableRequest) -> dict[str, Any]:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    interval = str(data.get("interval") or "1d")
    start = data.get("start")
    end = data.get("end")

    try:
        history = yf.download(ticker, start=start, end=end, interval=interval, progress=False)
    except Exception as exc:
        _raise_structured_error(
            https_fn.FunctionsErrorCode.NOT_FOUND,
            "ticker_not_found",
            "Unable to load market history for ticker.",
            {"ticker": ticker, "raw": str(exc)},
        )
    if isinstance(history.columns, pd.MultiIndex):
        history.columns = history.columns.get_level_values(0)
    history = history.dropna().reset_index()
    if history.empty:
        _raise_structured_error(
            https_fn.FunctionsErrorCode.NOT_FOUND,
            "ticker_not_found",
            "No history returned for ticker.",
            {"ticker": ticker},
        )

    date_col = "Datetime" if "Datetime" in history.columns else "Date"
    if date_col in history.columns:
        history[date_col] = history[date_col].astype(str)

    rows = history.to_dict(orient="records")
    return {"rows": _serialize_for_firestore(rows)}


@https_fn.on_call()
def download_price_csv(req: https_fn.CallableRequest) -> dict[str, Any]:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

    _require_auth(req)
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper().strip()
    interval = str(data.get("interval") or "1d").strip().lower()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")
    if interval not in {"1d", "1h"}:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Interval must be 1d or 1h.")

    end_raw = str(data.get("end") or "").strip()
    start_raw = str(data.get("start") or "").strip()

    def _parse_iso_date(raw: str, fallback: date) -> date:
        raw = (raw or "").strip()
        if not raw:
            return fallback
        try:
            return datetime.fromisoformat(raw[:10]).date()
        except Exception:
            return fallback

    end_date = _parse_iso_date(end_raw, date.today())
    if interval == "1h":
        min_start = end_date - timedelta(days=729)
        start_date = _parse_iso_date(start_raw, min_start)
        if start_date < min_start:
            start_date = min_start
    else:
        start_date = _parse_iso_date(start_raw, date(1900, 1, 1))

    # yfinance treats end as exclusive; bump one day so user-selected end is included.
    end_exclusive = end_date + timedelta(days=1)

    history = yf.download(
        ticker,
        start=start_date.isoformat(),
        end=end_exclusive.isoformat(),
        interval=interval,
        progress=False,
    )
    if isinstance(history.columns, pd.MultiIndex):
        history.columns = history.columns.get_level_values(0)
    history = history.dropna()
    if history.empty or "Close" not in history.columns:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "No data returned for the requested range.")

    out_df = history[["Close"]].rename(columns={"Close": "Price"}).copy()
    out_df.reset_index(inplace=True)
    date_col = "Datetime" if "Datetime" in out_df.columns else "Date"
    if date_col != "Date" and date_col in out_df.columns:
        out_df.rename(columns={date_col: "Date"}, inplace=True)
    if "Date" in out_df.columns:
        out_df["Date"] = out_df["Date"].astype(str)
    out_df.insert(0, "Item_Id", ticker.lower())

    buffer = StringIO()
    out_df.to_csv(buffer, index=False)
    buffer.seek(0)
    csv_text = _force_remove_second_line(buffer.getvalue())

    filename = f"{ticker}_{start_date.isoformat()}_{end_date.isoformat()}_{interval}.csv"
    return {
        "ticker": ticker,
        "interval": interval,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "rowCount": int(len(out_df)),
        "filename": filename,
        "csv": csv_text,
    }


@https_fn.on_call()
def get_trending_tickers(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    force = bool(data.get("force"))
    now = time.time()

    cache_ref = db.collection("cache").document("trending_us")
    ttl_seconds = 24 * 60 * 60

    if not force:
        try:
            cached_snap = cache_ref.get()
            if cached_snap.exists:
                cached = cached_snap.to_dict() or {}
                updated_epoch = float(cached.get("updatedAtEpoch") or 0.0)
                tickers_cached = cached.get("tickers") or []
                items_cached = cached.get("items") or []
                if updated_epoch and (now - updated_epoch) < ttl_seconds and (tickers_cached or items_cached):
                    return {
                        "tickers": tickers_cached,
                        "items": items_cached,
                        "cached": True,
                        "updatedAtEpoch": updated_epoch,
                    }
        except Exception:
            # Cache is best-effort; fall back to live fetch.
            pass

    tickers: list[str] = []
    try:
        response = requests.get(TRENDING_URL, headers=_yahoo_headers(), timeout=10)
        response.raise_for_status()
        data = response.json()
        quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        tickers = [item.get("symbol") for item in quotes if item.get("symbol")]
    except Exception:
        # Keep the UI functional even when Yahoo throttles.
        tickers = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "TSLA", "META"]

    tickers = [str(t).upper().strip() for t in (tickers or []) if str(t).strip()]
    tickers = list(dict.fromkeys(tickers))

    snapshots = _trending_snapshots(tickers, max_rows=18)
    items: list[dict[str, Any]] = []
    for symbol in tickers[:18]:
        snap = snapshots.get(symbol) or {}
        items.append({"symbol": symbol, **snap})

    payload = {
        "tickers": tickers,
        "items": _serialize_for_firestore(items),
        "cached": False,
        "updatedAtEpoch": int(now),
    }

    try:
        cache_ref.set(
            {
                "tickers": tickers,
                "items": payload["items"],
                "updatedAtEpoch": payload["updatedAtEpoch"],
                "updatedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
    except Exception:
        pass

    return payload


@https_fn.on_call()
def get_ticker_intel(req: https_fn.CallableRequest) -> dict[str, Any]:
    import numpy as np  # type: ignore
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper().strip()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    def _as_float(value: Any) -> float | None:
        try:
            num = float(value)
            return num if math.isfinite(num) else None
        except Exception:
            return None

    def _statement_value(frame: Any, labels: list[str]) -> float | None:
        if frame is None or getattr(frame, "empty", True):
            return None
        try:
            for label in labels:
                if label in frame.index:
                    series = frame.loc[label]
                    if hasattr(series, "dropna"):
                        series = series.dropna()
                    if getattr(series, "empty", True):
                        continue
                    return _as_float(series.iloc[0])
        except Exception:
            return None
        return None

    def _score_by_range(value: float | None, low: float, high: float, invert: bool = False) -> float | None:
        if value is None or high <= low:
            return None
        clamped = max(low, min(high, value))
        pct = (clamped - low) / (high - low)
        if invert:
            pct = 1 - pct
        return round(max(0.0, min(1.0, pct)) * 100, 1)

    ticker_obj = yf.Ticker(ticker)

    info: dict[str, Any] = {}
    try:
        info = ticker_obj.info or {}
        if not isinstance(info, dict):
            info = {}
    except Exception:
        info = {}

    summary = str(info.get("longBusinessSummary") or "").strip()
    if len(summary) > 900:
        summary = summary[:897].rstrip() + "..."

    profile = {
        "name": info.get("longName") or info.get("shortName") or ticker,
        "sector": info.get("sector") or "",
        "industry": info.get("industry") or "",
        "website": info.get("website") or "",
        "country": info.get("country") or "",
        "exchange": info.get("fullExchangeName") or info.get("exchange") or "",
        "currency": info.get("currency") or "",
        "summary": summary,
        "marketCap": info.get("marketCap"),
        "beta": info.get("beta"),
        "trailingPE": info.get("trailingPE"),
        "forwardPE": info.get("forwardPE"),
        "dividendYield": info.get("dividendYield"),
        "fiftyTwoWeekLow": info.get("fiftyTwoWeekLow"),
        "fiftyTwoWeekHigh": info.get("fiftyTwoWeekHigh"),
        "debtToEquity": info.get("debtToEquity"),
        "currentRatio": info.get("currentRatio"),
        "quickRatio": info.get("quickRatio"),
        "returnOnEquity": info.get("returnOnEquity"),
        "returnOnAssets": info.get("returnOnAssets"),
    }

    income_stmt = None
    balance_sheet = None
    cashflow = None
    try:
        income_stmt = ticker_obj.quarterly_income_stmt
    except Exception:
        income_stmt = None
    try:
        balance_sheet = ticker_obj.quarterly_balance_sheet
    except Exception:
        balance_sheet = None
    try:
        cashflow = ticker_obj.quarterly_cashflow
    except Exception:
        cashflow = None

    total_revenue = _statement_value(income_stmt, ["Total Revenue", "Revenue"])
    gross_profit = _statement_value(income_stmt, ["Gross Profit"])
    operating_income = _statement_value(income_stmt, ["Operating Income"])
    net_income = _statement_value(income_stmt, ["Net Income", "Net Income Common Stockholders"])
    free_cash_flow = _statement_value(cashflow, ["Free Cash Flow"])

    net_margin = (net_income / total_revenue) if net_income is not None and total_revenue and total_revenue > 0 else None
    roi = _as_float(info.get("returnOnEquity"))
    if roi is None:
        roi = _as_float(info.get("returnOnAssets"))
    debt_to_equity = _as_float(info.get("debtToEquity"))
    if debt_to_equity is not None and debt_to_equity > 10:
        debt_to_equity = debt_to_equity / 100.0
    current_ratio = _as_float(info.get("currentRatio"))
    beta = _as_float(info.get("beta"))

    dividend_yield = _as_float(info.get("dividendYield"))
    payout_ratio = _as_float(info.get("payoutRatio"))
    shares_outstanding = _as_float(info.get("sharesOutstanding"))
    float_shares = _as_float(info.get("floatShares"))
    target_12m = _as_float(info.get("targetMeanPrice"))
    liquidity_cash = _as_float(info.get("totalCash"))
    liquidity_debt = _as_float(info.get("totalDebt"))

    dividend_policy = "No regular dividend policy disclosed."
    if dividend_yield is not None and dividend_yield > 0:
        payout_text = f"{payout_ratio * 100:.1f}%" if payout_ratio is not None and payout_ratio > 0 else "n/a"
        dividend_policy = f"Dividend yield near {dividend_yield * 100:.2f}% with payout ratio {payout_text}."

    buyback_summary = "Share buyback signal not explicit in current data."
    if shares_outstanding and float_shares and shares_outstanding > 0 and float_shares > 0:
        ratio = float_shares / shares_outstanding
        if ratio < 0.9:
            buyback_summary = "Share count structure suggests periodic repurchases versus total outstanding base."
        elif ratio > 0.98:
            buyback_summary = "Limited buyback footprint detected in the current share structure."

    risk_mitigation = []
    if current_ratio is not None:
        risk_mitigation.append(f"Current ratio {current_ratio:.2f}")
    if debt_to_equity is not None:
        risk_mitigation.append(f"Debt-to-equity {debt_to_equity:.2f}")
    if liquidity_cash is not None and liquidity_cash > 0:
        risk_mitigation.append(f"Cash buffer ${liquidity_cash:,.0f}")
    risk_mitigation_text = ", ".join(risk_mitigation) if risk_mitigation else "Liquidity and hedging data is limited for this symbol."

    env_score = _as_float(info.get("environmentScore"))
    social_score = _as_float(info.get("socialScore"))
    governance_score = _as_float(info.get("governanceScore"))
    total_esg = _as_float(info.get("totalEsg"))
    if total_esg is None:
        esg_parts = [x for x in [env_score, social_score, governance_score] if x is not None]
        total_esg = float(np.mean(esg_parts)) if esg_parts else None

    heatmap = [
        {
            "label": "Liquidity",
            "value": current_ratio,
            "score": _score_by_range(current_ratio, 0.5, 3.0),
            "hint": "Current ratio",
        },
        {
            "label": "Leverage",
            "value": debt_to_equity,
            "score": _score_by_range(debt_to_equity, 0.0, 3.0, invert=True),
            "hint": "Debt / equity",
        },
        {
            "label": "Net margin",
            "value": None if net_margin is None else net_margin * 100,
            "score": _score_by_range(net_margin, -0.05, 0.30),
            "hint": "Profitability",
        },
        {
            "label": "ROI",
            "value": None if roi is None else roi * 100,
            "score": _score_by_range(roi, -0.05, 0.25),
            "hint": "Return on capital",
        },
        {
            "label": "Cash conversion",
            "value": (free_cash_flow / total_revenue) * 100
            if free_cash_flow is not None and total_revenue and total_revenue > 0
            else None,
            "score": _score_by_range(
                (free_cash_flow / total_revenue) if free_cash_flow is not None and total_revenue and total_revenue > 0 else None,
                -0.05,
                0.25,
            ),
            "hint": "FCF / revenue",
        },
        {
            "label": "Volatility control",
            "value": beta,
            "score": _score_by_range(beta, 0.6, 2.0, invert=True),
            "hint": "Beta stability",
        },
    ]

    sector_peer_map: dict[str, list[str]] = {
        "Technology": ["MSFT", "AAPL", "NVDA", "AMD", "ORCL", "CRM"],
        "Financial Services": ["JPM", "BAC", "GS", "MS", "C", "WFC"],
        "Healthcare": ["LLY", "UNH", "JNJ", "PFE", "MRK", "ABBV"],
        "Energy": ["XOM", "CVX", "COP", "SLB", "EOG"],
        "Consumer Defensive": ["KO", "PEP", "WMT", "COST", "PG"],
        "Consumer Cyclical": ["AMZN", "TSLA", "HD", "MCD", "NKE"],
        "Industrials": ["CAT", "DE", "BA", "HON", "GE"],
    }
    sector_key = str(info.get("sector") or "")
    peer_candidates = [sym for sym in sector_peer_map.get(sector_key, ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]) if sym != ticker]
    peer_symbols = peer_candidates[:3]
    peer_rows: list[dict[str, Any]] = []
    for peer in peer_symbols:
        peer_info: dict[str, Any] = {}
        try:
            peer_info = yf.Ticker(peer).info or {}
            if not isinstance(peer_info, dict):
                peer_info = {}
        except Exception:
            peer_info = {}

        peer_pe = _as_float(peer_info.get("trailingPE"))
        peer_dte = _as_float(peer_info.get("debtToEquity"))
        if peer_dte is not None and peer_dte > 10:
            peer_dte = peer_dte / 100.0

        peer_sharpe = None
        try:
            hist = yf.download(peer, period="6mo", interval="1d", progress=False, auto_adjust=False)
            if hist is not None and getattr(hist, "empty", True) is False and "Close" in hist.columns:
                close = hist["Close"].astype(float).dropna()
                if len(close) > 15:
                    ret = close.pct_change().dropna()
                    vol = float(ret.std())
                    if vol > 0:
                        peer_sharpe = float((ret.mean() / vol) * math.sqrt(252))
        except Exception:
            peer_sharpe = None

        peer_rows.append(
            {
                "ticker": peer,
                "pe": None if peer_pe is None else round(peer_pe, 2),
                "debtToEquity": None if peer_dte is None else round(peer_dte, 2),
                "sharpeRatio": None if peer_sharpe is None else round(peer_sharpe, 2),
            }
        )

    calendar_raw: dict[str, Any] = {}
    try:
        cal = ticker_obj.calendar or {}
        if isinstance(cal, dict):
            calendar_raw = cal
    except Exception:
        calendar_raw = {}

    events: list[dict[str, Any]] = []
    for key, value in (calendar_raw or {}).items():
        if value is None:
            continue
        events.append({"label": str(key), "value": _serialize_for_firestore(value)})

    analyst = {
        "recommendationKey": info.get("recommendationKey"),
        "recommendationMean": info.get("recommendationMean"),
        "analystOpinions": info.get("numberOfAnalystOpinions"),
        "targetLowPrice": info.get("targetLowPrice"),
        "targetMeanPrice": info.get("targetMeanPrice"),
        "targetHighPrice": info.get("targetHighPrice"),
    }

    recommendation_trend: list[dict[str, Any]] = []
    try:
        rec = ticker_obj.recommendations
        if rec is not None and getattr(rec, "empty", True) is False:
            rec_rows = rec.reset_index(drop=True).head(6)
            for _, row in rec_rows.iterrows():
                recommendation_trend.append(
                    {
                        "period": str(row.get("period") or ""),
                        "strongBuy": int(row.get("strongBuy") or 0),
                        "buy": int(row.get("buy") or 0),
                        "hold": int(row.get("hold") or 0),
                        "sell": int(row.get("sell") or 0),
                        "strongSell": int(row.get("strongSell") or 0),
                    }
                )
    except Exception:
        recommendation_trend = []

    executive_summary = {
        "ticker": ticker,
        "exchange": profile.get("exchange"),
        "sector": profile.get("sector"),
        "marketCap": profile.get("marketCap"),
        "priceTarget12m": target_12m,
    }
    fundamental_deep_dive = {
        "revenueMechanics": {
            "totalRevenue": total_revenue,
            "grossProfit": gross_profit,
            "operatingIncome": operating_income,
            "segmentBreakdown": "Segment-level disclosure varies by issuer; this view highlights top-line and operating mechanics from reported statements.",
        },
        "profitability": {
            "netMargin": net_margin,
            "roi": roi,
            "returnOnAssets": _as_float(info.get("returnOnAssets")),
        },
        "capitalAllocation": {
            "dividendPolicy": dividend_policy,
            "shareBuybacks": buyback_summary,
            "freeCashFlow": free_cash_flow,
        },
    }
    risk_and_esg = {
        "riskMitigation": risk_mitigation_text,
        "liquidity": {
            "totalCash": liquidity_cash,
            "totalDebt": liquidity_debt,
            "currentRatio": current_ratio,
            "quickRatio": _as_float(info.get("quickRatio")),
        },
        "esg": {
            "environmental": env_score,
            "social": social_score,
            "governance": governance_score,
            "overall": total_esg,
        },
    }

    return {
        "ticker": ticker,
        "profile": _serialize_for_firestore(profile),
        "events": _serialize_for_firestore(events),
        "analyst": _serialize_for_firestore(analyst),
        "recommendationTrend": _serialize_for_firestore(recommendation_trend),
        "executiveSummary": _serialize_for_firestore(executive_summary),
        "fundamentalDeepDive": _serialize_for_firestore(fundamental_deep_dive),
        "riskAndEsg": _serialize_for_firestore(risk_and_esg),
        "balanceSheetHeatmap": _serialize_for_firestore(heatmap),
        "peerComparison": _serialize_for_firestore(peer_rows),
    }


@https_fn.on_call()
def get_ticker_news(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    seen: set[str] = set()

    def _extract_thumbnail(item: dict[str, Any]) -> str:
        thumb = item.get("thumbnail")
        if isinstance(thumb, dict):
            resolutions = thumb.get("resolutions")
            if isinstance(resolutions, list) and resolutions:
                # Prefer the largest image available.
                best = None
                for res in resolutions:
                    if not isinstance(res, dict):
                        continue
                    url = res.get("url")
                    width = res.get("width") or 0
                    if not url:
                        continue
                    if best is None or int(width or 0) > int(best.get("width") or 0):
                        best = {"url": str(url), "width": int(width or 0)}
                if best and best.get("url"):
                    return str(best["url"])
        return ""

    def _append_item(item: dict[str, Any]) -> None:
        title = str(item.get("title") or "").strip()
        link = str(item.get("link") or item.get("url") or "").strip()
        if not title:
            return
        key = (link or title).strip().lower()
        if key in seen:
            return
        seen.add(key)
        publisher = str(item.get("publisher") or item.get("source") or "").strip()
        published = item.get("publishedAt") or item.get("providerPublishTime")
        summary = str(item.get("summary") or item.get("description") or "").strip()
        news_items.append(
            {
                "title": title,
                "publisher": publisher,
                "link": link,
                "publishedAt": published,
                "summary": summary,
                "thumbnailUrl": _extract_thumbnail(item),
            }
        )

    news_items: list[dict[str, Any]] = []
    # 1) Prefer Yahoo search news (more reliable than yfinance .news)
    try:
        resp = requests.get(
            YAHOO_SEARCH_URL,
            headers=_yahoo_headers(),
            params={"q": ticker, "newsCount": 12, "quotesCount": 0, "listsCount": 0},
            timeout=10,
        )
        if resp.status_code < 400:
            payload = resp.json() if resp.text else {}
            for item in (payload.get("news") or [])[:12]:
                if isinstance(item, dict):
                    _append_item(item)
    except Exception:
        pass

    # 2) Fallback: yfinance news
    if not news_items:
        try:
            import yfinance as yf  # type: ignore

            ticker_obj = yf.Ticker(ticker)
            for item in (ticker_obj.news or [])[:10]:
                if not isinstance(item, dict):
                    continue
                _append_item(item)
        except Exception:
            pass

    # 3) Optional: RSS fallback (lightweight parse)
    if not news_items:
        try:
            import xml.etree.ElementTree as ET

            rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
            rss = requests.get(rss_url, headers=_yahoo_headers(), timeout=10)
            if rss.status_code < 400 and rss.text:
                root = ET.fromstring(rss.text)
                for item in root.findall(".//item")[:10]:
                    title = (item.findtext("title") or "").strip()
                    link = (item.findtext("link") or "").strip()
                    pub_date = (item.findtext("pubDate") or "").strip()
                    if title:
                        key = (link or title).strip().lower()
                        if key in seen:
                            continue
                        seen.add(key)
                        news_items.append(
                            {
                                "title": title,
                                "publisher": "Yahoo Finance",
                                "link": link,
                                "publishedAt": pub_date,
                                "summary": "",
                                "thumbnailUrl": "",
                            }
                        )
        except Exception:
            pass

    return {"news": news_items}


@https_fn.on_call()
def get_options_chain(req: https_fn.CallableRequest) -> dict[str, Any]:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

    _require_auth(req)
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper().strip()
    expiration = str(data.get("expiration") or "").strip()
    limit = int(data.get("limit") or 24)
    limit = max(10, min(limit, 200))
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    ticker_obj = yf.Ticker(ticker)
    expirations = []
    try:
        expirations = list(ticker_obj.options or [])
    except Exception:
        expirations = []

    if not expirations:
        return {"ticker": ticker, "underlyingPrice": None, "expirations": [], "selectedExpiration": "", "calls": [], "puts": []}

    selected = expiration if expiration in expirations else expirations[0]

    underlying_price = None
    try:
        fast_info = getattr(ticker_obj, "fast_info", None) or {}
        underlying_price = _safe_float(fast_info.get("last_price") or fast_info.get("lastPrice"))
    except Exception:
        underlying_price = None
    if underlying_price is None:
        try:
            hist = ticker_obj.history(period="5d", interval="1d")
            if not hist.empty and "Close" in hist.columns:
                underlying_price = float(hist["Close"].dropna().iloc[-1])
        except Exception:
            underlying_price = None

    # Time to expiry in years (approx). If we cannot parse, probabilities will be null.
    T = None
    try:
        exp_dt = datetime.strptime(selected, "%Y-%m-%d").date()
        today = datetime.now(tz=timezone.utc).date()
        days = max((exp_dt - today).days, 0)
        T = days / 365.0
    except Exception:
        T = None

    normal = NormalDist()

    def _option_metrics(
        strike: float | None, iv: float | None, is_call: bool
    ) -> tuple[float, float, float, float] | None:
        if underlying_price is None or T is None or T <= 0:
            return None
        if strike is None or strike <= 0:
            return None
        if iv is None or iv <= 0:
            return None
        S = float(underlying_price)
        K = float(strike)
        sigma = float(iv)
        # Protect the probability math from extreme / bad IV values.
        if sigma > 5:
            return None
        denom = sigma * math.sqrt(T)
        if denom <= 0:
            return None
        d1 = (math.log(S / K) + (RISK_FREE_RATE + 0.5 * sigma * sigma) * T) / denom
        d2 = d1 - sigma * math.sqrt(T)
        p_call = normal.cdf(d2)
        prob_itm = p_call if is_call else normal.cdf(-d2)
        delta = normal.cdf(d1) if is_call else normal.cdf(d1) - 1.0
        return (prob_itm, delta, d1, d2)

    def _format_chain(df: pd.DataFrame, opt_type: str) -> list[dict[str, Any]]:
        if df is None or df.empty:
            return []
        frame = df.copy()
        if "strike" in frame.columns:
            frame["strike"] = pd.to_numeric(frame["strike"], errors="coerce")
        for col in ["openInterest", "volume"]:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0)
        if "strike" in frame.columns:
            frame = frame.dropna(subset=["strike"])

        # Select a compact window around the underlying, then return results ordered by strike.
        if underlying_price is not None and "strike" in frame.columns:
            frame["distance"] = (frame["strike"] - float(underlying_price)).abs()
            sort_cols: list[str] = ["distance"]
            ascending: list[bool] = [True]
            if "openInterest" in frame.columns:
                sort_cols.append("openInterest")
                ascending.append(False)
            elif "volume" in frame.columns:
                sort_cols.append("volume")
                ascending.append(False)
            frame = frame.sort_values(by=sort_cols, ascending=ascending)
        elif "strike" in frame.columns:
            frame = frame.sort_values(by="strike", ascending=True)
        else:
            sort_col = "openInterest" if "openInterest" in frame.columns else ("volume" if "volume" in frame.columns else None)
            if sort_col:
                frame = frame.sort_values(by=sort_col, ascending=False)

        frame = frame.head(limit)
        if "strike" in frame.columns:
            frame = frame.sort_values(by="strike", ascending=True)

        items: list[dict[str, Any]] = []
        for _, row in frame.iterrows():
            strike = _safe_float(row.get("strike"))
            iv = _safe_float(row.get("impliedVolatility"))
            metrics = _option_metrics(strike, iv, is_call=(opt_type == "call"))
            p_itm = metrics[0] if metrics else None
            delta = metrics[1] if metrics else None
            bid = _safe_float(row.get("bid"))
            ask = _safe_float(row.get("ask"))
            mid = None
            if bid is not None and ask is not None and bid >= 0 and ask >= 0:
                mid = (bid + ask) / 2.0
            items.append(
                {
                    "contractSymbol": row.get("contractSymbol"),
                    "type": opt_type,
                    "strike": strike,
                    "lastPrice": _safe_float(row.get("lastPrice")),
                    "bid": bid,
                    "ask": ask,
                    "mid": None if mid is None else round(mid, 4),
                    "impliedVolatility": None if iv is None else round(iv, 4),
                    "delta": None if delta is None else round(delta, 4),
                    "volume": int(row.get("volume") or 0),
                    "openInterest": int(row.get("openInterest") or 0),
                    "inTheMoney": bool(row.get("inTheMoney")) if "inTheMoney" in row else None,
                    "probabilityITM": None if p_itm is None else round(p_itm * 100.0, 2),
                }
            )
        return items

    try:
        chain = ticker_obj.option_chain(selected)
        calls = _format_chain(chain.calls, "call")
        puts = _format_chain(chain.puts, "put")
    except Exception:
        calls = []
        puts = []

    return {
        "ticker": ticker,
        "underlyingPrice": underlying_price,
        "timeToExpiryYears": None if T is None else round(float(T), 6),
        "riskFreeRate": RISK_FREE_RATE,
        "expirations": expirations,
        "selectedExpiration": selected,
        "calls": _serialize_for_firestore(calls),
        "puts": _serialize_for_firestore(puts),
    }


@https_fn.on_call()
def get_technicals(req: https_fn.CallableRequest) -> dict[str, Any]:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore
    from finta import TA  # type: ignore

    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    interval = str(data.get("interval") or "1d")
    lookback = int(data.get("lookback") or 120)
    indicators = data.get("indicators") or ["RSI", "MACD"]
    include_series = bool(data.get("includeSeries"))
    max_points = int(data.get("maxPoints") or (240 if interval == "1h" else 260))
    max_points = max(30, min(max_points, 900))

    history = yf.download(ticker, period=f"{lookback}d", interval=interval, progress=False)
    if isinstance(history.columns, pd.MultiIndex):
        history.columns = history.columns.get_level_values(0)
    history = history.dropna()
    if history.empty:
        return {"latest": [], "series": {"dates": [], "items": []} if include_series else None}

    history = history.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    def _ta(name: str, frame: pd.DataFrame):
        func = getattr(TA, name, None)
        if not callable(func):
            return None
        return func(frame)

    latest_values = []
    series_items: list[dict[str, Any]] = []
    dates_index = history.index[-max_points:]
    dates_out = [pd.Timestamp(ts).isoformat() for ts in dates_index]
    indicator_map = {
        "RSI": lambda frame: _ta("RSI", frame),
        "MACD": lambda frame: TA.MACD(frame).get("MACD"),
        "SMA": lambda frame: TA.SMA(frame, 20),
        "EMA": lambda frame: TA.EMA(frame, 20),
        "BBANDS": lambda frame: _ta("BBANDS", frame),
        "ATR": lambda frame: TA.ATR(frame, 14),
        "ADX": lambda frame: _ta("ADX", frame),
        "CCI": lambda frame: _ta("CCI", frame),
        "MFI": lambda frame: _ta("MFI", frame),
        "OBV": lambda frame: _ta("OBV", frame),
        "ROC": lambda frame: _ta("ROC", frame),
        "STOCH": lambda frame: _ta("STOCH", frame),
        "WILLR": lambda frame: _ta("WILLIAMS", frame),
    }

    for indicator in indicators:
        func = indicator_map.get(indicator)
        if not func:
            continue
        try:
            series = func(history)
            if series is None or len(series) == 0:
                continue
            if indicator == "BBANDS" and isinstance(series, pd.DataFrame):
                for key, label in [("BB_UPPER", "BBANDS_UPPER"), ("BB_MIDDLE", "BBANDS_MIDDLE"), ("BB_LOWER", "BBANDS_LOWER")]:
                    band = series.get(key)
                    if band is None or len(band) == 0:
                        continue
                    value = band.dropna().iloc[-1]
                    latest_values.append({"name": label, "value": round(float(value), 4)})
                    if include_series:
                        aligned = band.reindex(dates_index)
                        series_items.append(
                            {
                                "name": label,
                                "values": [None if pd.isna(v) else round(float(v), 6) for v in aligned.tolist()],
                            }
                        )
                continue

            if isinstance(series, pd.DataFrame):
                series = series.iloc[:, 0]
            value = series.dropna().iloc[-1]
            latest_values.append({"name": indicator, "value": round(float(value), 4)})
            if include_series:
                aligned = series.reindex(dates_index)
                series_items.append(
                    {
                        "name": indicator,
                        "values": [None if pd.isna(v) else round(float(v), 6) for v in aligned.tolist()],
                    }
                )
        except Exception:
            continue

    out: dict[str, Any] = {"latest": latest_values}
    if include_series:
        out["series"] = {"dates": dates_out, "items": series_items}
    return out


@https_fn.on_call()
def queue_screener_run(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    payload = {
        "userId": req.auth.uid,
        "userEmail": token.get("email"),
        "universe": data.get("universe"),
        "market": data.get("market"),
        "minCap": data.get("minCap"),
        "maxNames": data.get("maxNames"),
        "notes": data.get("notes"),
        "status": "queued",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "meta": data.get("meta") or {},
    }

    doc_ref = db.collection("screener_runs").document()
    doc_ref.set(payload)
    _audit_event(req.auth.uid, token.get("email"), "screener_queued", {"runId": doc_ref.id})
    return {"runId": doc_ref.id}


@https_fn.on_call()
def run_quick_screener(req: https_fn.CallableRequest) -> dict[str, Any]:
    try:
        import numpy as np  # type: ignore
        import pandas as pd  # type: ignore
        import yfinance as yf  # type: ignore
    except Exception as exc:
        _raise_structured_error(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "screener_dependency_error",
            "Screener dependencies are unavailable.",
            {"raw": str(exc)[:400]},
        )

    token = _require_auth(req)
    data = req.data or {}

    workspace_id = str(data.get("workspaceId") or req.auth.uid or "").strip()
    if not workspace_id:
        workspace_id = req.auth.uid

    # Allow writing into your own workspace, or an editor-level shared workspace.
    if workspace_id != req.auth.uid and token.get("email") != ADMIN_EMAIL:
        shared = (
            db.collection("users")
            .document(workspace_id)
            .collection("collaborators")
            .document(req.auth.uid)
            .get()
        )
        if not shared.exists:
            raise https_fn.HttpsError(
                https_fn.FunctionsErrorCode.PERMISSION_DENIED,
                "Workspace access required to run a screener for this workspace.",
            )
        role = str((shared.to_dict() or {}).get("role") or "viewer").strip().lower()
        if role != "editor":
            raise https_fn.HttpsError(
                https_fn.FunctionsErrorCode.PERMISSION_DENIED,
                "Editor access is required to run a screener for this workspace.",
            )

    context = _remote_config_context(req, token, data.get("meta") if isinstance(data.get("meta"), dict) else None)
    tier_key, tier = _resolve_ai_tier(req.auth.uid, token, context=context)
    allowed_models_raw = tier.get("allowed_models") if isinstance(tier, dict) else []
    allowed_models = [str(item).strip() for item in (allowed_models_raw or []) if str(item).strip()]
    daily_limit = int(tier.get("daily_limit") or (50 if tier_key == "premium" else 5))

    selected_model = str(data.get("model") or data.get("selectedModel") or "gpt-4o-mini").strip()
    if allowed_models and selected_model not in allowed_models:
        _raise_structured_error(
            https_fn.FunctionsErrorCode.PERMISSION_DENIED,
            "model_locked",
            "Selected model is not available for your current tier.",
            {"selectedModel": selected_model, "allowedModels": allowed_models, "tier": tier_key},
        )

    _enforce_daily_usage(
        req.auth.uid,
        "aiScreenerRuns",
        max(1, daily_limit),
        "Daily AI screener limit reached. Upgrade to Premium for higher throughput.",
    )

    market = str(data.get("market") or "us").lower()
    try:
        max_names = int(data.get("maxNames") or 25)
    except Exception:
        max_names = 25
    max_names = max(5, min(max_names, 50))

    market_cap_filter_raw = data.get("marketCapFilter") if isinstance(data.get("marketCapFilter"), dict) else {}
    filter_mode = str(market_cap_filter_raw.get("type") or "greater_than").strip().lower() or "greater_than"
    filter_value_raw = market_cap_filter_raw.get("value")
    if filter_value_raw in (None, ""):
        filter_value_raw = data.get("minCap")

    market_cap_target_abs: float | None = None
    try:
        if filter_value_raw not in (None, ""):
            parsed = float(filter_value_raw)
            # Backward compatibility: historical API used billions.
            market_cap_target_abs = parsed * 1_000_000_000 if parsed < 1_000_000_000 else parsed
    except Exception:
        _raise_structured_error(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "invalid_market_cap_filter",
            "Market cap filter value is invalid.",
        )

    if filter_mode not in {"greater_than", "less_than", "any"}:
        filter_mode = "greater_than"

    universe_key = str(data.get("universe") or "trending").strip().lower()
    universe_map: dict[str, list[str]] = {
        "large-cap": [
            "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO", "JPM", "V",
            "MA", "WMT", "COST", "XOM", "LLY", "UNH", "JNJ", "PG", "HD", "BAC",
            "KO", "PEP", "ORCL", "CRM", "NFLX", "ADBE", "AMD", "CSCO", "CVX", "MRK",
        ],
        "mid-cap": [
            "HUBS", "ROST", "VRSK", "ANSS", "TTWO", "PAYC", "NVR", "RJF", "PODD", "WAB",
            "FDS", "MGM", "DKNG", "GL", "MTCH", "OKTA", "ZS", "ULTA", "ETSY", "ON",
            "COIN", "RBLX", "MELI", "CFLT", "NET", "SQ", "DDOG", "SNAP", "SHOP", "U",
        ],
        "small-cap": [
            "IWM", "CROX", "RXRX", "CRSR", "SMR", "SOFI", "OPEN", "RUN", "TOST", "AI",
            "SOUN", "UPST", "LMND", "RKLB", "ASTS", "IONQ", "DNA", "BBAI", "ENVX", "ARRY",
            "CHPT", "QS", "BMBL", "RIVN", "NOVA", "S", "PATH", "JOBY", "ACHR", "PL",
        ],
    }

    trending: list[str] = []
    if universe_key == "trending" or universe_key not in universe_map:
        try:
            response = requests.get(TRENDING_URL, headers=_yahoo_headers(), timeout=10)
            response.raise_for_status()
            payload = response.json()
            quotes = payload.get("finance", {}).get("result", [{}])[0].get("quotes", [])
            trending = [str(item.get("symbol") or "").upper() for item in quotes if item.get("symbol")]
        except Exception:
            trending = []

    fallback_diverse = universe_map["large-cap"]
    base_tickers = universe_map.get(universe_key) if universe_key in universe_map else trending
    if not base_tickers:
        base_tickers = trending or fallback_diverse

    candidate_pool = max(20, min(max_names * 3, 90))
    tickers = list(dict.fromkeys([str(t).upper().strip() for t in base_tickers if str(t).strip()]))[:candidate_pool]

    results: list[dict[str, Any]] = []
    market_cap_cache: dict[str, float | None] = {}

    def _fmt_market_cap(value: float | None) -> str:
        if value is None:
            return "—"
        abs_value = abs(float(value))
        if abs_value >= 1_000_000_000_000:
            return f"${value / 1_000_000_000_000:.2f}T"
        if abs_value >= 1_000_000_000:
            return f"${value / 1_000_000_000:.2f}B"
        if abs_value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
        return f"${value:,.0f}"

    def _market_cap_for_symbol(symbol: str) -> float | None:
        if symbol in market_cap_cache:
            return market_cap_cache[symbol]
        cap: float | None = None
        try:
            tk = yf.Ticker(symbol)
            fast_info = getattr(tk, "fast_info", None)
            if fast_info is not None:
                cap_raw = None
                try:
                    cap_raw = fast_info.get("market_cap")
                except Exception:
                    cap_raw = getattr(fast_info, "market_cap", None)
                if cap_raw is not None:
                    cap_num = float(cap_raw)
                    if math.isfinite(cap_num) and cap_num > 0:
                        cap = cap_num
            if cap is None:
                info = tk.info or {}
                cap_raw = info.get("marketCap")
                if cap_raw is not None:
                    cap_num = float(cap_raw)
                    if math.isfinite(cap_num) and cap_num > 0:
                        cap = cap_num
        except Exception:
            cap = None
        market_cap_cache[symbol] = cap
        return cap

    def _compute_rsi(close_series: Any, period: int = 14) -> float | None:
        try:
            series = pd.Series(close_series, dtype="float64").dropna()
            if len(series) < period + 1:
                return None
            delta = series.diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
            avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
            rs = avg_gain / avg_loss.replace(0, np.nan)
            rsi = 100 - (100 / (1 + rs))
            clean = rsi.dropna()
            if clean.empty:
                return None
            return float(clean.iloc[-1])
        except Exception:
            return None

    if tickers:
        try:
            history = yf.download(
                " ".join(tickers),
                period="6mo",
                interval="1d",
                group_by="ticker",
                progress=False,
                threads=True,
                auto_adjust=False,
            )
        except Exception:
            history = pd.DataFrame()

        per_symbol_history: dict[str, Any] = {}

        def _extract(history_frame: Any, symbol: str) -> Any:
            if isinstance(history_frame, pd.DataFrame) and not history_frame.empty:
                if not isinstance(history_frame.columns, pd.MultiIndex):
                    return history_frame.copy()
                if symbol in history_frame.columns.get_level_values(0):
                    frame = history_frame[symbol].copy()
                    if isinstance(frame.columns, pd.MultiIndex):
                        frame.columns = frame.columns.get_level_values(-1)
                    return frame
                if symbol in history_frame.columns.get_level_values(1):
                    frame = history_frame.xs(symbol, level=1, axis=1).copy()
                    if isinstance(frame.columns, pd.MultiIndex):
                        frame.columns = frame.columns.get_level_values(-1)
                    return frame
            cached = per_symbol_history.get(symbol)
            if cached is not None:
                return cached
            try:
                frame = yf.download(
                    symbol,
                    period="6mo",
                    interval="1d",
                    progress=False,
                    auto_adjust=False,
                    threads=False,
                )
            except Exception:
                frame = pd.DataFrame()
            per_symbol_history[symbol] = frame
            return frame

        for symbol in tickers:
            market_cap = _market_cap_for_symbol(symbol)
            if market_cap_target_abs is not None and filter_mode != "any":
                if market_cap is None:
                    continue
                if filter_mode == "greater_than" and not (market_cap >= market_cap_target_abs):
                    continue
                if filter_mode == "less_than" and not (market_cap <= market_cap_target_abs):
                    continue

            frame = _extract(history, symbol)
            if frame is None or getattr(frame, "empty", True) or "Close" not in frame.columns:
                continue
            frame = frame.dropna()
            if frame.empty:
                continue

            close = frame["Close"].astype(float).dropna()
            if len(close) < 30:
                continue

            last_close = float(close.iloc[-1])
            ret_1m = (last_close / float(close.iloc[-21]) - 1.0) if len(close) >= 22 else None
            ret_3m = (last_close / float(close.iloc[-63]) - 1.0) if len(close) >= 64 else None

            returns = close.pct_change().dropna()
            vol_ann = float(returns.std() * np.sqrt(252)) if len(returns) else None
            rsi_val = _compute_rsi(close, period=14)

            score = 0.0
            if isinstance(ret_3m, float):
                score += 0.65 * ret_3m
            if isinstance(ret_1m, float):
                score += 0.35 * ret_1m
            if isinstance(rsi_val, float):
                score += 0.05 * ((rsi_val - 50.0) / 50.0)

            results.append(
                {
                    "symbol": symbol,
                    "lastClose": round(last_close, 4),
                    "return1m": None if ret_1m is None else round(ret_1m * 100.0, 2),
                    "return3m": None if ret_3m is None else round(ret_3m * 100.0, 2),
                    "rsi14": None if rsi_val is None else round(rsi_val, 2),
                    "volatility": None if vol_ann is None else round(vol_ann, 4),
                    "score": round(score, 6),
                    "marketCap": None if market_cap is None else int(round(market_cap)),
                    "marketCapLabel": _fmt_market_cap(market_cap),
                }
            )

    results.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    results = results[:max_names]
    run_title = str(data.get("title") or "").strip()
    if not run_title:
        run_title = f"{str(data.get('universe') or 'Trending').title()} screener"

    doc_ref = db.collection("screener_runs").document()
    run_doc = {
        "userId": workspace_id,
        "userEmail": token.get("email"),
        "createdByUid": req.auth.uid,
        "createdByEmail": token.get("email"),
        "market": market,
        "universe": str(data.get("universe") or "trending"),
        "maxNames": max_names,
        "status": "completed",
        "title": run_title,
        "results": _serialize_for_firestore(results),
        "notes": str(data.get("notes") or ""),
        "modelUsed": selected_model,
        "modelTier": tier_key,
        "allowedModels": allowed_models,
        "dailyLimit": daily_limit,
        "marketCapFilter": {
            "type": filter_mode,
            "value": market_cap_target_abs,
        },
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "meta": data.get("meta") or {},
    }
    doc_ref.set(run_doc)
    _audit_event(req.auth.uid, token.get("email"), "screener_completed", {"runId": doc_ref.id, "count": len(results)})

    return {
        "runId": doc_ref.id,
        "status": "completed",
        "title": run_title,
        "results": results,
        "workspaceId": workspace_id,
        "modelUsed": selected_model,
        "modelTier": tier_key,
        "dailyLimit": daily_limit,
        "allowedModels": allowed_models,
        "resultsFound": len(results),
        "marketCapFilter": {"type": filter_mode, "value": market_cap_target_abs},
    }


@https_fn.on_call()
def queue_autopilot_run(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    payload = {
        "userId": req.auth.uid,
        "userEmail": token.get("email"),
        "ticker": data.get("ticker"),
        "horizon": data.get("horizon"),
        "quantiles": data.get("quantiles"),
        "interval": data.get("interval"),
        "notes": data.get("notes"),
        "status": "queued",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "meta": data.get("meta") or {},
    }

    doc_ref = db.collection("autopilot_requests").document()
    doc_ref.set(payload)
    _audit_event(req.auth.uid, token.get("email"), "autopilot_queued", {"requestId": doc_ref.id})
    return {"requestId": doc_ref.id}


@https_fn.on_call()
def delete_autopilot_request(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    request_id = str(data.get("requestId") or data.get("id") or "").strip()
    if not request_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Request ID is required.")

    ref = db.collection("autopilot_requests").document(request_id)
    snap = ref.get()
    if not snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Autopilot request not found.")

    doc = snap.to_dict() or {}
    owner_id = str(doc.get("userId") or "").strip()
    if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")

    ref.delete()
    _audit_event(req.auth.uid, token.get("email"), "autopilot_deleted", {"requestId": request_id})
    return {"deleted": True, "requestId": request_id}


@https_fn.on_call()
def submit_feedback(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    message = str(data.get("message") or "").strip()
    if not message:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Feedback message is required.")

    rating = data.get("rating")
    page_path = str(data.get("pagePath") or "").strip()
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}

    user_id = req.auth.uid if req.auth else ""
    user_email = ""
    if req.auth and req.auth.token:
        user_email = str(req.auth.token.get("email") or "")
    if not user_email:
        user_email = str(data.get("email") or "").strip()

    doc_ref = db.collection("feedback").document()
    doc_ref.set(
        {
            "userId": user_id,
            "userEmail": user_email,
            "rating": rating,
            "message": message,
            "pagePath": page_path or str(meta.get("pagePath") or ""),
            "meta": meta,
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )

    if user_id:
        _audit_event(user_id, user_email, "feedback_submitted", {"feedbackId": doc_ref.id})

    return {"feedbackId": doc_ref.id}


@https_fn.on_call()
def generate_social_campaign_drafts(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}

    topic = str(data.get("topic") or "").strip() or "Weekly market positioning update"
    objective = str(data.get("objective") or "").strip() or "Drive qualified leads to Quantura"
    audience = str(data.get("audience") or "").strip() or "active investors and growth-focused teams"
    tone = str(data.get("tone") or "").strip() or "confident, concise, practical"
    channels = _normalize_social_channels(data.get("channels"))
    posts_per_channel = max(1, min(int(data.get("postsPerChannel") or 2), 5))
    cta_url = str(data.get("ctaUrl") or SOCIAL_DEFAULT_CTA_URL).strip() or SOCIAL_DEFAULT_CTA_URL
    title = str(data.get("title") or f"{topic} campaign").strip()
    save_draft = bool(data.get("saveDraft", True))

    drafts, model_used = _generate_social_copy_with_openai(
        topic=topic,
        objective=objective,
        audience=audience,
        tone=tone,
        channels=channels,
        posts_per_channel=posts_per_channel,
        cta_url=cta_url,
    )

    campaign_id = ""
    if save_draft:
        campaign_ref = db.collection("social_campaigns").document()
        campaign_ref.set(
            {
                "userId": req.auth.uid,
                "userEmail": token.get("email"),
                "title": title,
                "topic": topic,
                "objective": objective,
                "audience": audience,
                "tone": tone,
                "channels": channels,
                "postsPerChannel": posts_per_channel,
                "ctaUrl": cta_url,
                "drafts": _serialize_for_firestore(drafts),
                "model": model_used,
                "status": "draft",
                "createdAt": firestore.SERVER_TIMESTAMP,
                "updatedAt": firestore.SERVER_TIMESTAMP,
                "meta": data.get("meta") or {},
            }
        )
        campaign_id = campaign_ref.id

    _audit_event(
        req.auth.uid,
        token.get("email"),
        "social_drafts_generated",
        {"campaignId": campaign_id, "channels": channels, "postsPerChannel": posts_per_channel, "model": model_used},
    )

    return {
        "campaignId": campaign_id,
        "title": title,
        "topic": topic,
        "channels": channels,
        "postsPerChannel": posts_per_channel,
        "model": model_used,
        "drafts": drafts,
    }


@https_fn.on_call()
def queue_social_campaign_posts(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    campaign_id = str(data.get("campaignId") or "").strip()
    if not campaign_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "campaignId is required.")

    campaign_ref = db.collection("social_campaigns").document(campaign_id)
    campaign_snap = campaign_ref.get()
    if not campaign_snap.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Campaign not found.")

    campaign = campaign_snap.to_dict() or {}
    owner_id = str(campaign.get("userId") or "").strip()
    if token.get("email") != ADMIN_EMAIL and owner_id != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Access denied.")

    drafts = campaign.get("drafts") if isinstance(campaign.get("drafts"), dict) else {}
    if not drafts:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, "Campaign has no drafts to queue.")

    requested_channels = _normalize_social_channels(data.get("channels"))
    channels = [channel for channel in requested_channels if channel in drafts] or list(drafts.keys())
    if not channels:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, "No channels available to queue.")

    scheduled_for = _as_utc_datetime(data.get("scheduledFor")) or datetime.now(timezone.utc)
    if scheduled_for < datetime.now(timezone.utc) - timedelta(minutes=2):
        scheduled_for = datetime.now(timezone.utc)

    enqueue = _enqueue_social_posts(
        campaign_id=campaign_id,
        user_id=owner_id or req.auth.uid,
        user_email=str(campaign.get("userEmail") or token.get("email") or ""),
        channels=channels,
        drafts=drafts,
        scheduled_for=scheduled_for,
        meta=data.get("meta") if isinstance(data.get("meta"), dict) else {},
    )

    campaign_ref.set(
        {
            "status": "queued",
            "queuedAt": firestore.SERVER_TIMESTAMP,
            "queuedCount": enqueue["count"],
            "queuedChannels": channels,
            "scheduledFor": scheduled_for,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    _audit_event(
        req.auth.uid,
        token.get("email"),
        "social_posts_queued",
        {"campaignId": campaign_id, "queuedCount": enqueue["count"], "channels": channels},
    )
    return {
        "campaignId": campaign_id,
        "queuedCount": enqueue["count"],
        "queueIds": enqueue["queueIds"],
        "scheduledFor": scheduled_for.isoformat(),
        "channels": channels,
    }


@https_fn.on_call()
def list_social_campaigns(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    limit = max(1, min(int((req.data or {}).get("limit") or 40), 100))
    query = db.collection("social_campaigns")
    if token.get("email") != ADMIN_EMAIL:
        query = query.where("userId", "==", req.auth.uid)

    docs = query.order_by("createdAt", direction=firestore.Query.DESCENDING).limit(limit).stream()
    items = []
    for doc in docs:
        row = doc.to_dict() or {}
        row["id"] = doc.id
        items.append(row)
    return {"campaigns": _serialize_for_firestore(items)}


@https_fn.on_call()
def list_social_queue(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    limit = max(1, min(int(data.get("limit") or 80), 200))
    status_filter = str(data.get("status") or "").strip().lower()
    query = db.collection("social_queue")
    if token.get("email") != ADMIN_EMAIL:
        query = query.where("userId", "==", req.auth.uid)
    if status_filter:
        query = query.where("status", "==", status_filter)

    docs = query.order_by("createdAt", direction=firestore.Query.DESCENDING).limit(limit).stream()
    items = []
    for doc in docs:
        row = doc.to_dict() or {}
        row["id"] = doc.id
        items.append(row)
    return {"queue": _serialize_for_firestore(items)}


@https_fn.on_call()
def publish_social_queue_now(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)
    if not SOCIAL_AUTOMATION_ENABLED:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, "Social automation is disabled.")
    data = req.data or {}
    max_posts = max(1, min(int(data.get("maxPosts") or SOCIAL_DISPATCH_BATCH_SIZE), 100))
    summary = _dispatch_due_social_posts(max_posts=max_posts, trigger="manual", actor_uid=req.auth.uid)
    _audit_event(req.auth.uid, token.get("email"), "social_dispatch_manual", summary)
    return summary


@scheduler_fn.on_schedule(
    schedule="0 * * * *",
    timezone=scheduler_fn.Timezone(SOCIAL_AUTOMATION_TIMEZONE),
)
def social_dispatch_scheduler(event: scheduler_fn.ScheduledEvent) -> None:
    del event
    if not SOCIAL_AUTOMATION_ENABLED:
        return
    _dispatch_due_social_posts(max_posts=SOCIAL_DISPATCH_BATCH_SIZE, trigger="scheduler")


@https_fn.on_call()
def alpaca_get_options(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    expiration = data.get("expiration")
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    options = []
    try:
        headers = _alpaca_headers()
        params = {"underlying_symbols": ticker, "limit": 30}
        if expiration:
            params["expiration_date"] = expiration
        response = requests.get(
            f"{ALPACA_DATA_BASE}/v1beta1/options/contracts",
            headers=headers,
            params=params,
            timeout=12,
        )
        response.raise_for_status()
        contracts = response.json().get("option_contracts", [])
        for contract in contracts[:30]:
            delta = _safe_float(contract.get("delta"))
            implied_prob = None
            if delta is not None:
                implied_prob = max(0.0, min(abs(delta), 1.0))
            options.append(
                {
                    "symbol": contract.get("symbol"),
                    "type": contract.get("type"),
                    "strike": contract.get("strike_price"),
                    "expiration": contract.get("expiration_date"),
                    "lastPrice": contract.get("close_price"),
                    "delta": None if delta is None else round(delta, 4),
                    "impliedProbability": None if implied_prob is None else round(implied_prob * 100, 2),
                }
            )
    except Exception:
        try:
            ticker_obj = yf.Ticker(ticker)
            expirations = ticker_obj.options
            if expirations:
                exp = expiration or expirations[0]
                chain = ticker_obj.option_chain(exp)
                for _, row in chain.calls.head(10).iterrows():
                    options.append(
                        {
                            "symbol": row.get("contractSymbol"),
                            "type": "call",
                            "strike": row.get("strike"),
                            "expiration": exp,
                            "lastPrice": row.get("lastPrice"),
                            "delta": None,
                            "impliedProbability": None,
                        }
                    )
                for _, row in chain.puts.head(10).iterrows():
                    options.append(
                        {
                            "symbol": row.get("contractSymbol"),
                            "type": "put",
                            "strike": row.get("strike"),
                            "expiration": exp,
                            "lastPrice": row.get("lastPrice"),
                            "delta": None,
                            "impliedProbability": None,
                        }
                    )
        except Exception:
            options = []

    return {"options": options}


@https_fn.on_call()
def alpaca_place_order(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)
    data = req.data or {}

    symbol = str(data.get("symbol") or "").upper().strip()
    if not symbol:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Symbol is required.")

    order_payload: dict[str, Any] = {
        "symbol": symbol,
        "qty": int(data.get("qty") or 1),
        "side": str(data.get("side") or "buy"),
        "type": str(data.get("type") or "market"),
        "time_in_force": str(data.get("timeInForce") or "day"),
    }

    limit_price = _safe_float(data.get("limitPrice"))
    if order_payload["type"] == "limit" and limit_price:
        order_payload["limit_price"] = limit_price

    response = requests.post(
        f"{ALPACA_API_BASE}/v2/orders",
        headers=_alpaca_headers(),
        json=order_payload,
        timeout=12,
    )

    if response.status_code >= 400:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            f"Alpaca error: {response.text}",
        )

    order_data = response.json()
    doc_ref = db.collection("alpaca_orders").document()
    doc_ref.set(
        {
            "userId": req.auth.uid,
            "userEmail": token.get("email"),
            "symbol": symbol,
            "side": order_payload["side"],
            "type": order_payload["type"],
            "qty": order_payload["qty"],
            "status": order_data.get("status"),
            "alpacaId": order_data.get("id"),
            "response": _serialize_for_firestore(order_data),
            "createdAt": firestore.SERVER_TIMESTAMP,
            "meta": data.get("meta") or {},
        }
    )

    _audit_event(
        req.auth.uid,
        token.get("email"),
        "alpaca_order_placed",
        {"orderId": doc_ref.id, "alpacaId": order_data.get("id"), "symbol": symbol},
    )

    return {"orderId": doc_ref.id, "alpacaId": order_data.get("id")}


@https_fn.on_call()
def alpaca_get_account(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)
    response = requests.get(f"{ALPACA_API_BASE}/v2/account", headers=_alpaca_headers(), timeout=12)
    if response.status_code >= 400:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, response.text)
    account = response.json()
    return {
        "account": {
            "id": account.get("id"),
            "status": account.get("status"),
            "currency": account.get("currency"),
            "buying_power": account.get("buying_power"),
            "equity": account.get("equity"),
            "cash": account.get("cash"),
            "portfolio_value": account.get("portfolio_value"),
            "pattern_day_trader": account.get("pattern_day_trader"),
        }
    }


@https_fn.on_call()
def alpaca_get_positions(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)
    response = requests.get(f"{ALPACA_API_BASE}/v2/positions", headers=_alpaca_headers(), timeout=12)
    if response.status_code >= 400:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, response.text)
    positions = response.json()
    clean_positions = [
        {
            "symbol": item.get("symbol"),
            "qty": item.get("qty"),
            "market_value": item.get("market_value"),
            "cost_basis": item.get("cost_basis"),
            "unrealized_pl": item.get("unrealized_pl"),
            "unrealized_plpc": item.get("unrealized_plpc"),
            "side": item.get("side"),
        }
        for item in positions
    ]
    return {"positions": clean_positions}


@https_fn.on_call()
def alpaca_list_orders(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)
    data = req.data or {}
    status = str(data.get("status") or "all")
    limit = int(data.get("limit") or 50)
    params = {"status": status, "limit": max(1, min(limit, 100)), "direction": "desc"}

    response = requests.get(f"{ALPACA_API_BASE}/v2/orders", headers=_alpaca_headers(), params=params, timeout=12)
    if response.status_code >= 400:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, response.text)

    orders = response.json()
    clean_orders = [
        {
            "id": item.get("id"),
            "symbol": item.get("symbol"),
            "status": item.get("status"),
            "side": item.get("side"),
            "type": item.get("type"),
            "qty": item.get("qty"),
            "filled_qty": item.get("filled_qty"),
            "filled_avg_price": item.get("filled_avg_price"),
            "created_at": item.get("created_at"),
        }
        for item in orders
    ]
    return {"orders": clean_orders}


@https_fn.on_call()
def alpaca_cancel_order(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)
    data = req.data or {}
    order_id = str(data.get("orderId") or "").strip()
    if not order_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "orderId is required.")

    response = requests.delete(f"{ALPACA_API_BASE}/v2/orders/{order_id}", headers=_alpaca_headers(), timeout=12)
    if response.status_code >= 400:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, response.text)

    _audit_event(req.auth.uid, token.get("email"), "alpaca_order_cancelled", {"alpacaId": order_id})
    return {"cancelled": True, "orderId": order_id}


@https_fn.on_call()
def send_slack_test_message(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)

    if not SLACK_WEBHOOK_URL:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "SLACK_WEBHOOK_URL is not configured.",
        )

    data = req.data or {}
    text = str(data.get("text") or "Quantura Slack webhook test from Firebase function.")
    payload = {"text": text}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    if response.status_code >= 400:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INTERNAL,
            f"Slack webhook failed: {response.status_code} {response.text}",
        )

    _audit_event(req.auth.uid, token.get("email"), "slack_test_sent", {"message": text})
    return {"ok": True}
