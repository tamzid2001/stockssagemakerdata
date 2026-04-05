from __future__ import annotations

from ..models import SocialDraft
from ..secrets import SecretResolver
from .base import SocialProvider


class LinkedInProvider(SocialProvider):
    provider_name = "linkedin"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.resolver: SecretResolver = ctx.resolver

    def publish(self, draft: SocialDraft):
        if self.ctx.dry_run:
            return self._result(ok=True, channel=draft.channel, status="dry_run", message="Dry-run preview only.")

        author_urn = self.resolver.require("LINKEDIN_AUTHOR_URN", "LinkedIn publishing")
        access_token = self.resolver.require("LINKEDIN_ACCESS_TOKEN", "LinkedIn publishing")
        version = str(self.ctx.channel_config.get("providerOptions", {}).get("linkedinVersion") or "202504")
        commentary = draft.body
        if draft.cta_url and draft.cta_url not in commentary:
            commentary = f"{commentary}\n{draft.cta_url}".strip()
        response = self._request(
            method="POST",
            url="https://api.linkedin.com/rest/posts",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "LinkedIn-Version": version,
                "X-Restli-Protocol-Version": "2.0.0",
            },
            json_body={
                "author": author_urn,
                "commentary": commentary,
                "visibility": "PUBLIC",
                "distribution": {
                    "feedDistribution": "MAIN_FEED",
                    "targetEntities": [],
                    "thirdPartyDistributionChannels": [],
                },
                "lifecycleState": "PUBLISHED",
                "isReshareDisabledByAuthor": False,
            },
        )
        external_id = str(response.headers.get("x-restli-id") or "")
        payload = response.json() if response.text else {}
        external_url = ""
        return self._result(
            ok=True,
            channel=draft.channel,
            status="published",
            message="Published to LinkedIn.",
            external_id=external_id,
            external_url=external_url,
            raw=payload,
        )
