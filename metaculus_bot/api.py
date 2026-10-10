from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import requests

BASE = "https://www.metaculus.com/api"
PERMISSIONS = {"forecaster", "curator", "admin", "creator"}


class ApiError(RuntimeError):
    def __init__(self, service: str, status: int | None = None):
        self.service, self.status = service, status
        super().__init__(f"{service}_{status or 'UNAVAILABLE'}")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def date(value: str | float | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (float, int)):
        return datetime.fromtimestamp(value, timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


class Metaculus:
    def __init__(self, token: str, session=None):
        self.session = session or requests.Session()
        self.session.headers.update({"Authorization": f"Token {token}", "User-Agent": "Quantura-Metaculus/1.0"})

    def get(self, path: str, **params) -> Any:
        # Retry only reads. Writes are reconciled through the authoritative API.
        for attempt in range(3):
            try:
                r = self.session.get(f"{BASE}/{path.lstrip('/')}", params=params, timeout=(10, 45))
            except requests.RequestException:
                if attempt == 2:
                    raise ApiError("METACULUS") from None
            else:
                if r.status_code == 200:
                    return r.json()
                if r.status_code not in {408, 429, 500, 502, 503, 504} or attempt == 2:
                    raise ApiError("METACULUS", r.status_code)
            time.sleep(2 ** attempt)
        raise ApiError("METACULUS")

    def post(self, path: str, payload) -> Any:
        try:
            r = self.session.post(f"{BASE}/{path.lstrip('/')}", json=payload, timeout=(10, 45))
        except requests.RequestException:
            raise ApiError("METACULUS_WRITE_UNCERTAIN") from None
        if not 200 <= r.status_code < 300:
            raise ApiError("METACULUS_WRITE", r.status_code)
        return r.json() if r.content else None

    def identity(self) -> dict:
        user = self.get("users/me/")
        if not user.get("is_bot") or not user.get("is_active"):
            raise RuntimeError("ACTIVE_BOT_ACCOUNT_REQUIRED")
        if user.get("api_forecasting_access") is False:
            raise RuntimeError("BOT_API_FORECASTING_ACCESS_REQUIRED")
        return {"id": user["id"], "username": user["username"]}

    def competitions(self, now=None) -> list[dict]:
        now = now or utcnow()
        projects = self.get("projects/tournaments/") + self.get("projects/minibenches/")
        # Include requires explicit bot inclusion in the leaderboard; exclude/show
        # human tournaments aren't inferred eligible from ordinary write permission.
        unique = {}
        for project in projects:
            end = date(project.get("forecasting_end_date") or project.get("close_date"))
            start = date(project.get("start_date"))
            if (project.get("bot_leaderboard_status") in {"bots_only", "include"}
                    and project.get("user_permission") in PERMISSIONS
                    and project.get("is_ongoing")
                    and (not start or start <= now) and (not end or end > now)):
                unique[project["id"]] = project
        return list(unique.values())

    def posts(self, tournament) -> list[dict]:
        rows, offset = [], 0
        while True:
            data = self.get("posts/", tournaments=tournament, statuses="open", include_description="true",
                            include_conditional_cps="false", limit=100, offset=offset, order_by="-published_time")
            page = data["results"]
            rows.extend(page)
            # Metaculus uses a countless paginator: `next` can be present even
            # for a short final page. Advancing by the page's length duplicates
            # items and eventually mistakes the empty terminal page for failure.
            if len(page) < 100 or not data.get("next"):
                return rows
            offset += 100

    def comments(self, post_id: int, author_id: int) -> list[dict]:
        rows, offset = [], 0
        while True:
            data = self.get("comments/", post=post_id, author=author_id, is_private="true", limit=100, offset=offset)
            page = data["results"] if isinstance(data, dict) else data
            rows.extend(page)
            if isinstance(data, list) or len(page) < 100 or not data.get("next"):
                return rows
            offset += 100
