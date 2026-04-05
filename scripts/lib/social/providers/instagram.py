from __future__ import annotations

import time

from ..models import SocialDraft
from ..secrets import SecretResolver
from .base import SocialProvider


class InstagramProvider(SocialProvider):
    provider_name = "instagram"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.resolver: SecretResolver = ctx.resolver

    def publish(self, draft: SocialDraft):
        if self.ctx.dry_run:
            return self._result(ok=True, channel=draft.channel, status="dry_run", message="Dry-run preview only.")

        account_id = self.resolver.require("INSTAGRAM_BUSINESS_ACCOUNT_ID", "Instagram publishing")
        access_token = self.resolver.require("INSTAGRAM_ACCESS_TOKEN", "Instagram publishing")
        image_url = draft.media_url or self.resolver.get("INSTAGRAM_DEFAULT_IMAGE_URL", "")
        if not image_url:
            raise RuntimeError(
                "Instagram publishing requires a public image URL. Configure INSTAGRAM_DEFAULT_IMAGE_URL in Secret Manager."
            )
        api_version = str(self.ctx.channel_config.get("providerOptions", {}).get("graphApiVersion") or "v23.0")
        create_response = self._request(
            method="POST",
            url=f"https://graph.facebook.com/{api_version}/{account_id}/media",
            data={
                "image_url": image_url,
                "caption": draft.body,
                "access_token": access_token,
            },
        )
        create_payload = create_response.json() if create_response.text else {}
        creation_id = str(create_payload.get("id") or "")
        time.sleep(8)
        publish_response = self._request(
            method="POST",
            url=f"https://graph.facebook.com/{api_version}/{account_id}/media_publish",
            data={
                "creation_id": creation_id,
                "access_token": access_token,
            },
        )
        payload = publish_response.json() if publish_response.text else {}
        external_id = str(payload.get("id") or creation_id)
        return self._result(
            ok=True,
            channel=draft.channel,
            status="published",
            message="Published to Instagram.",
            external_id=external_id,
            raw={"create": create_payload, "publish": payload},
        )
