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


_SECRET_SPECS: dict[str, SecretSpec] = {
    "OPENAI_API_KEY": SecretSpec("OPENAI_API_KEY", usage="OpenAI completions and agent analysis"),
    "AMAZON_NOVA_API_KEY": SecretSpec("AMAZON_NOVA_API_KEY", usage="Amazon Nova provider routing"),
    "IBM_TIMEMIXER_API_KEY": SecretSpec("IBM_TIMEMIXER_API_KEY", usage="IBM TimeMixer endpoint auth"),
    "HUGGINGFACEHUB_API_TOKEN": SecretSpec("HUGGINGFACEHUB_API_TOKEN", usage="Hugging Face inference fallback"),
    "ALPACA_API_KEY": SecretSpec("ALPACA_API_KEY", aliases=("ALPACAAPIKEY",), usage="Alpaca trading API"),
    "ALPACA_SECRET_KEY": SecretSpec("ALPACA_SECRET_KEY", aliases=("ALPACASECRETKEY",), usage="Alpaca trading API"),
    "SLACK_WEBHOOK_URL": SecretSpec("SLACK_WEBHOOK_URL", usage="Ops alerts"),
    "FCM_WEB_VAPID_KEY": SecretSpec("FCM_WEB_VAPID_KEY", usage="Web push token generation"),
    "STRIPE_SECRET_KEY": SecretSpec("STRIPE_SECRET_KEY", usage="Stripe server API"),
    "STRIPE_WEBHOOK_SECRET": SecretSpec("STRIPE_WEBHOOK_SECRET", usage="Stripe webhook verification"),
    "MASSIVE_API_KEY": SecretSpec("MASSIVE_API_KEY", usage="Massive market data"),
    "MASSIVE_BASE_URL": SecretSpec("MASSIVE_BASE_URL", usage="Massive API base URL override"),
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
        aliases=("X_API_KEY", "TWITTER_CONSUMER_KEY"),
        usage="X OAuth1 posting",
    ),
    "TWITTER_API_SECRET": SecretSpec(
        "TWITTER_API_SECRET",
        aliases=("X_API_SECRET", "TWITTER_CONSUMER_SECRET"),
        usage="X OAuth1 posting",
    ),
    "TWITTER_ACCESS_TOKEN": SecretSpec("TWITTER_ACCESS_TOKEN", aliases=("X_ACCESS_TOKEN",), usage="X OAuth1 posting"),
    "TWITTER_ACCESS_TOKEN_SECRET": SecretSpec(
        "TWITTER_ACCESS_TOKEN_SECRET",
        aliases=("X_ACCESS_TOKEN_SECRET",),
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
    """Secrets declared for Firebase Functions runtime binding."""
    return sorted(_SECRET_SPECS.keys())


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
