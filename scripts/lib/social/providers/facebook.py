from __future__ import annotations

from ..models import SocialDraft
from ..secrets import SecretResolver
from .base import SocialProvider


class FacebookProvider(SocialProvider):
    provider_name = "facebook"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.resolver: SecretResolver = ctx.resolver

    def publish(self, draft: SocialDraft):
        if self.ctx.dry_run:
            return self._result(ok=True, channel=draft.channel, status="dry_run", message="Dry-run preview only.")

        page_id = self.resolver.require("FACEBOOK_PAGE_ID", "Facebook publishing")
        access_token = self.resolver.require("FACEBOOK_PAGE_ACCESS_TOKEN", "Facebook publishing")
        api_version = str(self.ctx.channel_config.get("providerOptions", {}).get("graphApiVersion") or "v23.0")
        response = self._request(
            method="POST",
            url=f"https://graph.facebook.com/{api_version}/{page_id}/feed",
            data={
                "message": draft.body,
                "link": draft.cta_url or draft.source.canonical_url,
                "access_token": access_token,
            },
        )
        payload = response.json() if response.text else {}
        external_id = str(payload.get("id") or "")
        external_url = f"https://www.facebook.com/{external_id}" if external_id else ""
        return self._result(
            ok=True,
            channel=draft.channel,
            status="published",
            message="Published to Facebook.",
            external_id=external_id,
            external_url=external_url,
            raw=payload,
        )
