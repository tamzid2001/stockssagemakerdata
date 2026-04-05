from __future__ import annotations

from ..models import SocialDraft
from ..secrets import SecretResolver
from .base import ProviderContext, SocialProvider


class XProvider(SocialProvider):
    provider_name = "x"

    def __init__(self, ctx: ProviderContext) -> None:
        super().__init__(ctx)
        self.resolver: SecretResolver = ctx.resolver

    def publish(self, draft: SocialDraft):
        if self.ctx.dry_run:
            return self._result(ok=True, channel=draft.channel, status="dry_run", message="Dry-run preview only.")

        oauth2_token = self.resolver.get("X_USER_OAUTH2_TOKEN", "")
        headers = {"Content-Type": "application/json"}
        auth = None
        if oauth2_token:
            headers["Authorization"] = f"Bearer {oauth2_token}"
        else:
            from requests_oauthlib import OAuth1  # type: ignore

            auth = OAuth1(
                self.resolver.require("TWITTER_API_KEY", "X publishing"),
                client_secret=self.resolver.require("TWITTER_API_SECRET", "X publishing"),
                resource_owner_key=self.resolver.require("TWITTER_ACCESS_TOKEN", "X publishing"),
                resource_owner_secret=self.resolver.require("TWITTER_ACCESS_TOKEN_SECRET", "X publishing"),
            )

        response = self._request(
            method="POST",
            url="https://api.x.com/2/tweets",
            headers=headers,
            json_body={"text": draft.body},
            auth=auth,
        )
        payload = response.json() if response.text else {}
        post_id = str((payload.get("data") or {}).get("id") or "")
        external_url = f"https://x.com/i/web/status/{post_id}" if post_id else ""
        return self._result(
            ok=True,
            channel=draft.channel,
            status="published",
            message="Published to X.",
            external_id=post_id,
            external_url=external_url,
            raw=payload,
        )
