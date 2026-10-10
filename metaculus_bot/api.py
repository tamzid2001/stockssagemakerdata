from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests

BASE = "https://www.metaculus.com/api"
PERMISSIONS = {"forecaster", "curator", "admin", "creator"}
BOT_FRIENDLY_SLUGS = {"ai-2027"}


class ApiError(RuntimeError):
    def __init__(self, service: str, status: int | None = None):
        self.service, self.status = service, status
        super().__init__(f"{service}_{status or 'UNAVAILABLE'}")


class RateLimited(ApiError):
    def __init__(self, retry_after: float):
        self.retry_after = retry_after
        super().__init__("METACULUS_RATE_LIMIT", 429)


def retry_seconds(value, now=None) -> float | None:
    """Retry-After can be seconds or an HTTP date. Never retry before it."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        try:
            return max(0.0, (parsedate_to_datetime(value) - (now or utcnow())).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


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
    def __init__(self, token: str, session=None, *, interval=5.0, clock=time.monotonic, sleep=time.sleep):
        self.session = session or requests.Session()
        self.session.headers.update({"Authorization": f"Token {token}", "User-Agent": "Quantura-Metaculus/1.0"})
        self.interval, self.clock, self.sleep = interval, clock, sleep
        self.next_request = 0.0

    def wait(self):
        remaining = self.next_request - self.clock()
        if remaining > 120:
            raise RateLimited(remaining)
        if remaining > 0:
            self.sleep(remaining)
        self.next_request = self.clock() + self.interval

    def cooldown(self, response, attempt=0):
        delay = retry_seconds(getattr(response, "headers", {}).get("Retry-After"))
        delay = max(self.interval, delay if delay is not None else 30 * 2 ** attempt)
        self.next_request = max(self.next_request, self.clock() + delay)
        print(json.dumps({"event": "metaculus_api_backoff", "upstream_status": response.status_code,
                          "retry_after_seconds": round(delay, 1)}), flush=True)
        return delay

    def get(self, path: str, **params) -> Any:
        # Retry only reads. Writes are reconciled through the authoritative API.
        for attempt in range(3):
            self.wait()
            try:
                r = self.session.get(f"{BASE}/{path.lstrip('/')}", params=params, timeout=(10, 45))
            except requests.RequestException:
                if attempt == 2:
                    raise ApiError("METACULUS") from None
            else:
                if r.status_code == 200:
                    return r.json()
                if r.status_code == 429:
                    delay = self.cooldown(r, attempt)
                    if attempt == 2 or delay > 120:
                        raise RateLimited(delay)
                    continue
                if r.status_code not in {408, 429, 500, 502, 503, 504} or attempt == 2:
                    raise ApiError("METACULUS", r.status_code)
            self.next_request = max(self.next_request, self.clock() + 2 ** attempt)
        raise ApiError("METACULUS")

    def post(self, path: str, payload) -> Any:
        self.wait()
        try:
            r = self.session.post(f"{BASE}/{path.lstrip('/')}", json=payload, timeout=(10, 45))
        except requests.RequestException:
            raise ApiError("METACULUS_WRITE_UNCERTAIN") from None
        if r.status_code == 429:
            raise RateLimited(self.cooldown(r))
        if not 200 <= r.status_code < 300:
            raise ApiError("METACULUS_WRITE", r.status_code)
        return r.json() if r.content else None

    def identity(self) -> dict:
        user = self.get("users/me/")
        if not user.get("is_bot") or not user.get("is_active"):
            raise RuntimeError("ACTIVE_BOT_ACCOUNT_REQUIRED")
        if user.get("api_forecasting_access") in {False, "disabled"}:
            raise RuntimeError("BOT_API_FORECASTING_ACCESS_REQUIRED")
        return {"id": user["id"], "username": user["username"]}

    def competitions(self, now=None) -> list[dict]:
        now = now or utcnow()
        projects = self.get("projects/tournaments/") + self.get("projects/minibenches/")
        # Include requires explicit bot inclusion in the leaderboard; exclude/show
        # human tournaments aren't inferred eligible from ordinary write permission.
        unique = {}
        for project in projects:
            if eligible_project(project, now) and project.get("user_permission") in PERMISSIONS:
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

    def own_forecasts(self, question_ids: list[int]) -> set[int]:
        found = set()
        for start in range(0, len(question_ids), 100):
            data = self.get("questions/bulk-forecast-read/", question_ids=question_ids[start:start + 100])
            found.update(row["question_id"] for row in data["results"] if row["forecasts"])
        return found

    def comments(self, post_id: int | None, author_id: int) -> list[dict]:
        rows, offset = [], 0
        while True:
            data = self.get("comments/", post=post_id, author=author_id, is_private="true", limit=100, offset=offset)
            page = data["results"] if isinstance(data, dict) else data
            rows.extend(page)
            if isinstance(data, list) or len(page) < 100 or not data.get("next"):
                return rows
            offset += 100


def eligible_project(project: dict, now=None) -> bool:
    now = now or utcnow()
    end = date(project.get("forecasting_end_date") or project.get("close_date"))
    start = date(project.get("start_date"))
    return ((project.get("bot_leaderboard_status") in {"bots_only", "include"}
             or project.get("slug") in BOT_FRIENDLY_SLUGS
             or (project.get("slug") or "").startswith("metaculus-cup-"))
            and project.get("is_ongoing") and (not start or start <= now) and (not end or end > now))


def eligible_post(post: dict, scope: str) -> bool:
    projects = post.get("projects") or {}
    members = [p for rows in projects.values() for p in (rows if isinstance(rows, list) else [rows]) if isinstance(p, dict)]
    if scope == "test":
        return any(p.get("slug") == "bot-testing-area" for p in members)
    return any(eligible_project(p) for p in members)
