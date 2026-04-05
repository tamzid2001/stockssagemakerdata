from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import REPO_ROOT
from .models import SocialSource
from .store import FirestoreStore


BLOG_MANIFEST_PATH = REPO_ROOT / "quantura_site" / "public" / "blog" / "posts.manifest.json"
SITE_ORIGIN = "https://quantura.studio"
DEFAULT_VISUAL_URL = f"{SITE_ORIGIN}/web-app-manifest-512x512.png?v=20260405b"


def _slugify(value: str, max_length: int = 64) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text[:max_length] or "item"


def _absolute_site_url(url: str) -> str:
    clean = str(url or "").strip()
    if not clean:
        return ""
    if clean.startswith("http://") or clean.startswith("https://"):
        return clean
    if not clean.startswith("/"):
        clean = f"/{clean}"
    return f"{SITE_ORIGIN}{clean}"


def _load_blog_posts() -> list[dict[str, Any]]:
    if not BLOG_MANIFEST_PATH.exists():
        return []
    payload = json.loads(BLOG_MANIFEST_PATH.read_text(encoding="utf-8"))
    posts = payload.get("posts") or []
    if not isinstance(posts, list):
        return []
    return [item for item in posts if isinstance(item, dict)]


def choose_social_source(
    *,
    channel: str,
    source_mode: str,
    store: FirestoreStore,
    topic: str,
    cta_url: str,
    default_media_url: str,
) -> SocialSource:
    mode = str(source_mode or "auto").strip().lower()
    if mode in {"manual", "topic"} and topic.strip():
        return SocialSource(
            kind="manual_topic",
            source_id=f"manual-{channel}-{_slugify(topic)}",
            title=topic.strip(),
            summary=f"Manual social publishing run for {channel}: {topic.strip()}",
            canonical_url=str(cta_url or "").strip(),
            tags=("quantura", channel),
            media_url=default_media_url or DEFAULT_VISUAL_URL,
            metadata={"sourceMode": mode},
        )

    posts = _load_blog_posts()
    for post in posts:
        source_id = f"blog:{post.get('slug', '')}"
        if not source_id.endswith(":") and not store.was_published(channel, source_id):
            return SocialSource(
                kind="blog_post",
                source_id=source_id,
                title=str(post.get("rawTitle") or post.get("title") or "Quantura update").strip(),
                summary=str(post.get("description") or post.get("excerpt") or "").strip(),
                canonical_url=_absolute_site_url(str(post.get("canonical") or post.get("slug") or cta_url)),
                tags=tuple(str(tag).strip() for tag in (post.get("tags") or []) if str(tag).strip()),
                topic=str(post.get("topic") or "").strip(),
                published_at=str(post.get("dateIso") or "").strip(),
                media_url=_absolute_site_url(str(post.get("heroImage") or default_media_url or DEFAULT_VISUAL_URL)),
                metadata={"weekIndex": post.get("weekIndex"), "sourceMode": "blog_manifest"},
            )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    evergreen_title = topic.strip() or f"Quantura market pulse for {today}"
    evergreen_summary = (
        "Quantura workflow update: forecasting, research, and execution notes built for active market operators."
    )
    return SocialSource(
        kind="evergreen_market_pulse",
        source_id=f"evergreen:{channel}:{today}:{_slugify(evergreen_title)}",
        title=evergreen_title,
        summary=evergreen_summary,
        canonical_url=str(cta_url or f"{SITE_ORIGIN}/forecasting").strip(),
        tags=("quantura", "forecasting", "research"),
        media_url=default_media_url or DEFAULT_VISUAL_URL,
        metadata={"sourceMode": "evergreen_fallback"},
    )
