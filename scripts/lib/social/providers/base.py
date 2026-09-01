from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from ..models import PublishResult, SocialDraft
from ..secrets import SecretResolver


class ProviderError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 0, retryable: bool = False, raw: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
        self.raw = raw or {}


@dataclass
class ProviderContext:
    channel_config: dict[str, Any]
    resolver: SecretResolver
    dry_run: bool = False


class SocialProvider:
    provider_name = "base"

    def __init__(self, ctx: ProviderContext) -> None:
        self.ctx = ctx

    def publish(self, draft: SocialDraft) -> PublishResult:
        raise NotImplementedError

    def _result(self, *, ok: bool, channel: str, status: str, message: str = "", external_id: str = "", external_url: str = "", raw: dict[str, Any] | None = None, retryable: bool = False) -> PublishResult:
        return PublishResult(
            ok=ok,
            channel=channel,
            status=status,
            message=message,
            external_id=external_id,
            external_url=external_url,
            raw=raw or {},
            retryable=retryable,
        )

    def _request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        auth: Any = None,
        timeout: int = 30,
    ):
        import re
        import requests
        from urllib.parse import urlparse

        try:
            if "/../" in url or re.search(r"/%2e%2e/", url, re.IGNORECASE):
                raise ValueError("Invalid path")
            parsed = urlparse(url)
            if parsed.scheme not in ("http", "https"):
                raise ValueError("Invalid protocol")
            if not parsed.hostname:
                raise ValueError("Invalid host")
            allowed_domains = ["graph.facebook.com", "api.linkedin.com", "api.x.com", "open.tiktokapis.com"]
            if parsed.hostname.lower() not in allowed_domains:
                raise ValueError("Invalid host")
        except Exception:
            raise ValueError("Invalid URL")

        response = requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=params,
            json=json_body,
            data=data,
            auth=auth,
            timeout=timeout,
        )
        if response.status_code in {429, 500, 502, 503, 504}:
            raise ProviderError(
                f"Transient provider error {response.status_code}",
                status_code=response.status_code,
                retryable=True,
                raw={"text": response.text[:2000]},
            )
        if response.status_code >= 400:
            raise ProviderError(
                f"Provider error {response.status_code}",
                status_code=response.status_code,
                retryable=False,
                raw={"text": response.text[:2000]},
            )
        return response


def _import_provider(name: str):
    if name == "x":
        from .x import XProvider

        return XProvider
    if name == "linkedin":
        from .linkedin import LinkedInProvider

        return LinkedInProvider
    if name == "facebook":
        from .facebook import FacebookProvider

        return FacebookProvider
    if name == "instagram":
        from .instagram import InstagramProvider

        return InstagramProvider
    if name == "tiktok":
        from .tiktok import TikTokProvider

        return TikTokProvider
    raise ValueError(f"Unsupported social provider '{name}'.")


def get_provider(provider_name: str, ctx: ProviderContext) -> SocialProvider:
    provider_cls = _import_provider(str(provider_name or "").strip().lower())
    return provider_cls(ctx)


def execute_with_retry(
    *,
    action,
    provider_name: str,
    max_attempts: int,
    base_delay_seconds: int,
) -> PublishResult:
    attempt = 0
    while True:
        attempt += 1
        try:
            return action()
        except ProviderError as error:
            if attempt >= max_attempts or not error.retryable:
                return PublishResult(
                    ok=False,
                    channel=provider_name,
                    status="failed",
                    message=str(error),
                    retryable=error.retryable,
                    raw=error.raw,
                )
            sleep_for = max(1, base_delay_seconds) * attempt
            print(
                json.dumps(
                    {
                        "event": "provider_retry",
                        "provider": provider_name,
                        "attempt": attempt,
                        "sleepSeconds": sleep_for,
                        "reason": str(error),
                    },
                    ensure_ascii=True,
                )
            )
            time.sleep(sleep_for)
