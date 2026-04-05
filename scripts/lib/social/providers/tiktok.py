from __future__ import annotations

from ..models import SocialDraft
from ..secrets import SecretResolver
from .base import SocialProvider


class TikTokProvider(SocialProvider):
    provider_name = "tiktok"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.resolver: SecretResolver = ctx.resolver

    def publish(self, draft: SocialDraft):
        if self.ctx.dry_run:
            return self._result(ok=True, channel=draft.channel, status="dry_run", message="Dry-run preview only.")

        access_token = self.resolver.require("TIKTOK_ACCESS_TOKEN", "TikTok publishing")
        open_id = self.resolver.require("TIKTOK_OPEN_ID", "TikTok publishing")
        media_url = draft.media_url or self.resolver.get("TIKTOK_DEFAULT_MEDIA_URL", "")
        if not media_url:
            raise RuntimeError(
                "TikTok publishing requires a publicly reachable media URL. Configure TIKTOK_DEFAULT_MEDIA_URL in Secret Manager."
            )

        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        creator_info_response = self._request(
            method="POST",
            url="https://open.tiktokapis.com/v2/post/publish/creator_info/query/",
            headers=headers,
            json_body={"open_id": open_id},
        )
        creator_info = creator_info_response.json() if creator_info_response.text else {}
        creator_options = ((creator_info.get("data") or {}).get("privacy_level_options") or [])
        privacy_level = self.resolver.get("TIKTOK_PRIVACY_LEVEL", "") or str(
            self.ctx.channel_config.get("providerOptions", {}).get("privacyLevel") or ""
        )
        if privacy_level and creator_options and privacy_level not in creator_options:
            privacy_level = str(creator_options[0])
        if not privacy_level:
            privacy_level = str(creator_options[0] if creator_options else "SELF_ONLY")

        init_response = self._request(
            method="POST",
            url="https://open.tiktokapis.com/v2/post/publish/content/init/",
            headers=headers,
            json_body={
                "post_info": {
                    "title": draft.headline[:90],
                    "description": draft.body[:4000],
                    "privacy_level": privacy_level,
                    "disable_comment": False,
                    "auto_add_music": True,
                    "brand_content_toggle": False,
                    "brand_organic_toggle": False,
                },
                "source_info": {
                    "source": "PULL_FROM_URL",
                    "photo_images": [media_url],
                    "photo_cover_index": 1,
                },
                "post_mode": "DIRECT_POST",
                "media_type": "PHOTO",
            },
        )
        payload = init_response.json() if init_response.text else {}
        publish_id = str((payload.get("data") or {}).get("publish_id") or "")
        return self._result(
            ok=True,
            channel=draft.channel,
            status="published",
            message=f"Submitted to TikTok with privacy level {privacy_level}.",
            external_id=publish_id,
            raw={"creatorInfo": creator_info, "publish": payload},
        )
