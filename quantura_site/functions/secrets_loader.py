from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Final


@dataclass(frozen=True)
class SecretSpec:
    key: str
    aliases: tuple[str, ...] = ()
    usage: str = ""


SOCIAL_WEBHOOK_SECRET_NAMES: Final[tuple[str, ...]] = (
    "SOCIAL_WEBHOOK_X",
    "SOCIAL_WEBHOOK_LINKEDIN",
    "SOCIAL_WEBHOOK_FACEBOOK",
    "SOCIAL_WEBHOOK_INSTAGRAM",
    "SOCIAL_WEBHOOK_THREADS",
    "SOCIAL_WEBHOOK_REDDIT",
    "SOCIAL_WEBHOOK_TIKTOK",
    "SOCIAL_WEBHOOK_YOUTUBE",
    "SOCIAL_WEBHOOK_PINTEREST",
)

DEFAULT_SECRET_BINDING_KEYS: Final[tuple[str, ...]] = (
    "OPENAI_API_KEY",
    "GEMINI_API_KEY",
    "MISTRAL_API_KEY",
    "PERPLEXITY_API_KEY",
    "MODEL_COUNCIL_OTHER_API_KEY",
    "AMAZON_NOVA_API_KEY",
    "ALPACA_API_KEY",
    "ALPACA_SECRET_KEY",
    "SLACK_WEBHOOK_URL",
    "FCM_WEB_VAPID_KEY",
    "STRIPE_SECRET_KEY",
    "STRIPE_WEBHOOK_SECRET",
    "UNSPLASH_ACCESS_KEY",
    "TWITTER_BEARER_TOKEN",
)

# Prefer aliases that already exist in this project.
SECRET_BINDING_ALIAS_OVERRIDES: Final[dict[str, tuple[str, ...]]] = {
    "AMAZON_NOVA_API_KEY": ("AMAZON_NOVA_KEY",),
    "STRIPE_SECRET_KEY": ("STRIPE_PRIVATE_KEY",),
    "TWITTER_BEARER_TOKEN": ("X_BEARER_TOKEN",),
}


_SECRET_SPECS: dict[str, SecretSpec] = {
    "OPENAI_API_KEY": SecretSpec("OPENAI_API_KEY", usage="OpenAI completions and agent analysis"),
    "GEMINI_API_KEY": SecretSpec("GEMINI_API_KEY", usage="Google Gemini provider routing"),
    "MISTRAL_API_KEY": SecretSpec("MISTRAL_API_KEY", usage="Mistral provider routing"),
    "PERPLEXITY_API_KEY": SecretSpec("PERPLEXITY_API_KEY", usage="Perplexity Sonar provider routing"),
    "MODEL_COUNCIL_OTHER_API_KEY": SecretSpec("MODEL_COUNCIL_OTHER_API_KEY", usage="Custom Model Council provider routing"),
    "AMAZON_NOVA_API_KEY": SecretSpec(
        "AMAZON_NOVA_API_KEY",
        aliases=("AMAZON_NOVA_KEY",),
        usage="Amazon Nova provider routing",
    ),
    "SAGEMAKER_CANVAS_API_KEY": SecretSpec("SAGEMAKER_CANVAS_API_KEY", usage="SageMaker Canvas endpoint auth"),
    "HUGGINGFACEHUB_API_TOKEN": SecretSpec("HUGGINGFACEHUB_API_TOKEN", usage="Hugging Face inference fallback"),
    "ALPACA_API_KEY": SecretSpec("ALPACA_API_KEY", aliases=("ALPACAAPIKEY",), usage="Alpaca trading API"),
    "ALPACA_SECRET_KEY": SecretSpec("ALPACA_SECRET_KEY", aliases=("ALPACASECRETKEY",), usage="Alpaca trading API"),
    "SLACK_WEBHOOK_URL": SecretSpec("SLACK_WEBHOOK_URL", usage="Ops alerts"),
    "FCM_WEB_VAPID_KEY": SecretSpec("FCM_WEB_VAPID_KEY", usage="Web push token generation"),
    "STRIPE_SECRET_KEY": SecretSpec(
        "STRIPE_SECRET_KEY",
        aliases=("STRIPE_PRIVATE_KEY",),
        usage="Stripe server API",
    ),
    "STRIPE_WEBHOOK_SECRET": SecretSpec("STRIPE_WEBHOOK_SECRET", usage="Stripe webhook verification"),
    "UNSPLASH_ACCESS_KEY": SecretSpec(
        "UNSPLASH_ACCESS_KEY",
        aliases=("UNSPLASH_APPLICATION_ID",),
        usage="Unsplash media API",
    ),
    "TWITTER_BEARER_TOKEN": SecretSpec("TWITTER_BEARER_TOKEN", aliases=("X_BEARER_TOKEN",), usage="X read APIs"),
    "X_USER_OAUTH2_TOKEN": SecretSpec(
        "X_USER_OAUTH2_TOKEN",
        aliases=("X_USER_BEARER_TOKEN", "TWITTER_USER_OAUTH2_TOKEN"),
        usage="X user-context posting",
    ),
    "TWITTER_API_KEY": SecretSpec(
        "TWITTER_API_KEY",
        aliases=("X_API_KEY", "X_CLIENT_KEY", "TWITTER_CONSUMER_KEY"),
        usage="X OAuth1 posting",
    ),
    "TWITTER_API_SECRET": SecretSpec(
        "TWITTER_API_SECRET",
        aliases=("X_API_SECRET", "X_CLIENT_SECRET", "TWITTER_CONSUMER_SECRET"),
        usage="X OAuth1 posting",
    ),
    "TWITTER_ACCESS_TOKEN": SecretSpec(
        "TWITTER_ACCESS_TOKEN",
        aliases=("X_ACCESS_TOKEN", "X_SECRET_KEY"),
        usage="X OAuth1 posting",
    ),
    "TWITTER_ACCESS_TOKEN_SECRET": SecretSpec(
        "TWITTER_ACCESS_TOKEN_SECRET",
        aliases=("X_ACCESS_TOKEN_SECRET", "X_CLIENT_SECRET_ID"),
        usage="X OAuth1 posting",
    ),
    "LINKEDIN_ACCESS_TOKEN": SecretSpec("LINKEDIN_ACCESS_TOKEN", usage="LinkedIn posting"),
    "FACEBOOK_PAGE_ACCESS_TOKEN": SecretSpec("FACEBOOK_PAGE_ACCESS_TOKEN", usage="Facebook page posting"),
    "INSTAGRAM_ACCESS_TOKEN": SecretSpec("INSTAGRAM_ACCESS_TOKEN", usage="Instagram graph posting"),
    "TIKTOK_ACCESS_TOKEN": SecretSpec("TIKTOK_ACCESS_TOKEN", usage="TikTok direct posting"),
    "META_CAPI_ACCESS_TOKEN": SecretSpec("META_CAPI_ACCESS_TOKEN", usage="Meta conversion API"),
}

for _name in SOCIAL_WEBHOOK_SECRET_NAMES:
    _SECRET_SPECS[_name] = SecretSpec(_name, usage="Social channel webhook routing")


def secret_bindings() -> list[str]:
    """Secrets declared for Firebase Functions runtime binding.

    Optional integrations are loaded lazily in code and should not block deploys.
    Use QUANTURA_SECRET_BINDINGS to override (comma-separated secret names).
    """
    override = str(os.environ.get("QUANTURA_SECRET_BINDINGS", "") or "").strip()
    if override:
        custom = [item.strip() for item in override.split(",") if item.strip()]
        seen_custom: set[str] = set()
        out_custom: list[str] = []
        for item in custom:
            if item in seen_custom:
                continue
            seen_custom.add(item)
            out_custom.append(item)
        return out_custom

    bindings: list[str] = []
    for key in DEFAULT_SECRET_BINDING_KEYS:
        spec = _SECRET_SPECS.get(key)
        candidates: list[str] = list(SECRET_BINDING_ALIAS_OVERRIDES.get(key, ()))
        candidates.append(key)
        if spec:
            for alias in spec.aliases:
                if alias not in candidates:
                    candidates.append(alias)

        chosen = ""
        for candidate in candidates:
            if str(os.environ.get(candidate) or "").strip():
                chosen = candidate
                break
        if not chosen:
            chosen = candidates[0]
        bindings.append(chosen)

    seen: set[str] = set()
    out: list[str] = []
    for binding in bindings:
        if binding in seen:
            continue
        seen.add(binding)
        out.append(binding)
    return out


@lru_cache(maxsize=None)
def _lookup_secret(key: str) -> str:
    spec = _SECRET_SPECS.get(key)
    candidates = [key]
    if spec:
        candidates.extend(spec.aliases)
    for candidate in candidates:
        raw = os.environ.get(candidate)
        if raw is None:
            continue
        value = str(raw).strip()
        if value:
            return value
    return ""


def get_secret(key: str, default: str = "") -> str:
    value = _lookup_secret(key)
    return value if value else default


def require_secret(key: str, feature: str) -> str:
    value = get_secret(key, "")
    if value:
        return value
    raise RuntimeError(
        f"Missing required secret '{key}' for {feature}. Configure it in Google Secret Manager and redeploy."
    )
