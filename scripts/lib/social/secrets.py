from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable


SOCIAL_SECRET_ALIASES: dict[str, tuple[str, ...]] = {
    "OPENAI_API_KEY": ("OPENAI_API_KEY",),
    "TWITTER_API_KEY": ("TWITTER_API_KEY", "X_API_KEY", "TWITTER_CONSUMER_KEY"),
    "TWITTER_API_SECRET": ("TWITTER_API_SECRET", "X_API_SECRET", "TWITTER_CONSUMER_SECRET"),
    "TWITTER_ACCESS_TOKEN": ("TWITTER_ACCESS_TOKEN", "X_ACCESS_TOKEN"),
    "TWITTER_ACCESS_TOKEN_SECRET": ("TWITTER_ACCESS_TOKEN_SECRET", "X_ACCESS_TOKEN_SECRET"),
    "X_USER_OAUTH2_TOKEN": ("X_USER_OAUTH2_TOKEN", "TWITTER_USER_OAUTH2_TOKEN", "X_USER_BEARER_TOKEN"),
    "LINKEDIN_ACCESS_TOKEN": ("LINKEDIN_ACCESS_TOKEN",),
    "LINKEDIN_AUTHOR_URN": ("LINKEDIN_AUTHOR_URN", "LINKEDIN_ORGANIZATION_URN"),
    "FACEBOOK_PAGE_ID": ("FACEBOOK_PAGE_ID",),
    "FACEBOOK_PAGE_ACCESS_TOKEN": ("FACEBOOK_PAGE_ACCESS_TOKEN",),
    "INSTAGRAM_BUSINESS_ACCOUNT_ID": ("INSTAGRAM_BUSINESS_ACCOUNT_ID",),
    "INSTAGRAM_ACCESS_TOKEN": ("INSTAGRAM_ACCESS_TOKEN", "FACEBOOK_PAGE_ACCESS_TOKEN"),
    "INSTAGRAM_DEFAULT_IMAGE_URL": ("INSTAGRAM_DEFAULT_IMAGE_URL",),
    "TIKTOK_ACCESS_TOKEN": ("TIKTOK_ACCESS_TOKEN",),
    "TIKTOK_OPEN_ID": ("TIKTOK_OPEN_ID",),
    "TIKTOK_DEFAULT_MEDIA_URL": ("TIKTOK_DEFAULT_MEDIA_URL",),
    "TIKTOK_PRIVACY_LEVEL": ("TIKTOK_PRIVACY_LEVEL",),
}


@dataclass(frozen=True)
class SecretValidationResult:
    missing: tuple[str, ...]
    found: tuple[str, ...]


class SecretResolver:
    def __init__(self, project_id: str = "") -> None:
        self.project_id = str(project_id or "").strip()
        self._client = None
        if self.project_id:
            try:
                from google.cloud.secretmanager import SecretManagerServiceClient  # type: ignore

                self._client = SecretManagerServiceClient()
            except Exception:
                self._client = None

    @lru_cache(maxsize=None)
    def get(self, secret_name: str, default: str = "") -> str:
        names = SOCIAL_SECRET_ALIASES.get(secret_name, (secret_name,))
        for name in names:
            value = str(os.environ.get(name) or "").strip()
            if value:
                return value

        if not self._client or not self.project_id:
            return default

        for name in names:
            try:
                resource = f"projects/{self.project_id}/secrets/{name}/versions/latest"
                response = self._client.access_secret_version(name=resource)
                payload = response.payload.data.decode("utf-8").strip()
                if payload:
                    return payload
            except Exception:
                continue
        return default

    def require(self, secret_name: str, feature: str) -> str:
        value = self.get(secret_name, "")
        if value:
            return value
        raise RuntimeError(
            f"Missing required secret '{secret_name}' for {feature}. "
            f"Store it in Google Cloud Secret Manager for project '{self.project_id or 'unset-project'}'."
        )

    def validate_required(self, secret_names: Iterable[str]) -> SecretValidationResult:
        missing: list[str] = []
        found: list[str] = []
        for secret_name in secret_names:
            if self.get(secret_name, ""):
                found.append(secret_name)
            else:
                missing.append(secret_name)
        return SecretValidationResult(tuple(missing), tuple(found))
