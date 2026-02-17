#!/usr/bin/env python3
"""Channel probe for Quantura social integrations.

Runs lightweight checks for X, Reddit, Facebook, Instagram, LinkedIn, and TikTok
using configured credentials, then falls back to web search snippets where possible.
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import requests


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and (key not in os.environ or not str(os.environ.get(key) or "").strip()):
            os.environ[key] = value


def env(*keys: str) -> str:
    for key in keys:
        value = str(os.environ.get(key) or "").strip()
        if value:
            return value
    return ""


@dataclass
class ProbeResult:
    channel: str
    status: str
    detail: str
    count: int = 0


def web_fallback(query: str, domain: str, limit: int = 5) -> tuple[int, str]:
    try:
        q = quote_plus(f"site:{domain} {query}")
        resp = requests.get(
            f"https://duckduckgo.com/html/?q={q}",
            headers={"User-Agent": "Mozilla/5.0 (Quantura)"},
            timeout=12,
        )
        if resp.status_code >= 400:
            return 0, f"fallback_http_{resp.status_code}"
        html = resp.text or ""
    except Exception as exc:
        return 0, f"fallback_error:{str(exc)[:120]}"

    pattern = re.compile(
        r'<a[^>]+class="result__a"[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    seen: list[str] = []
    for match in pattern.finditer(html):
        href = unescape(re.sub(r"<[^>]+>", "", match.group("href") or "")).strip()
        if "uddg=" in href:
            try:
                parsed = urlparse(href)
                redirected = parse_qs(parsed.query).get("uddg", [""])[0]
                if redirected:
                    href = unquote(redirected)
            except Exception:
                pass
        if domain in href and href not in seen:
            seen.append(href)
        if len(seen) >= limit:
            break
    if seen:
        return len(seen), "fallback_ok"
    return 0, "fallback_empty"


def probe_x(query: str, limit: int) -> ProbeResult:
    token = env("TWITTER_BEARER_TOKEN", "X_BEARER_TOKEN")
    if not token:
        count, note = web_fallback(query, "x.com", limit)
        return ProbeResult("x", "fallback" if count else "missing_credentials", f"bearer_missing ({note})", count)

    # Prefer X News Search (AI-generated stories) when available.
    news_params = {
        "query": query,
        "max_results": max(5, min(limit, 10)),
        "max_age_hours": 72,
        "news.fields": "category,contexts,hook,name,summary,updated_at",
    }
    try:
        resp = requests.get(
            "https://api.x.com/2/news/search",
            headers={"Authorization": f"Bearer {token}"},
            params=news_params,
            timeout=12,
        )
        if resp.status_code < 400:
            payload = resp.json() if resp.text else {}
            rows = payload.get("data") or []
            if rows:
                return ProbeResult("x", "ok", "news_search_ok", min(len(rows), limit))
        # If news search is not available, fall back to post search.
    except Exception:
        pass

    params = {
        "query": f"({query}) -is:retweet",
        "max_results": max(10, min(limit * 2, 40)),
        "tweet.fields": "created_at,lang",
    }
    try:
        resp = requests.get(
            "https://api.twitter.com/2/tweets/search/recent",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=12,
        )
        if resp.status_code >= 400:
            count, note = web_fallback(query, "x.com", limit)
            return ProbeResult("x", "fallback" if count else "error", f"api_http_{resp.status_code} ({note})", count)
        payload = resp.json() if resp.text else {}
        rows = payload.get("data") or []
        if rows:
            return ProbeResult("x", "ok", "api_ok", min(len(rows), limit))
        count, note = web_fallback(query, "x.com", limit)
        return ProbeResult("x", "fallback" if count else "empty", f"api_empty ({note})", count)
    except Exception as exc:
        count, note = web_fallback(query, "x.com", limit)
        return ProbeResult("x", "fallback" if count else "error", f"api_error:{str(exc)[:120]} ({note})", count)


def probe_reddit(query: str, limit: int) -> ProbeResult:
    try:
        resp = requests.get(
            "https://www.reddit.com/search.json",
            params={"q": query, "sort": "hot", "limit": max(1, min(limit, 25))},
            headers={"User-Agent": env("REDDIT_USER_AGENT") or "quantura/1.0"},
            timeout=12,
        )
        if resp.status_code >= 400:
            count, note = web_fallback(query, "reddit.com", limit)
            return ProbeResult("reddit", "fallback" if count else "error", f"api_http_{resp.status_code} ({note})", count)
        payload = resp.json() if resp.text else {}
        rows = (payload.get("data") or {}).get("children") or []
        if rows:
            return ProbeResult("reddit", "ok", "api_ok", min(len(rows), limit))
        count, note = web_fallback(query, "reddit.com", limit)
        return ProbeResult("reddit", "fallback" if count else "empty", f"api_empty ({note})", count)
    except Exception as exc:
        count, note = web_fallback(query, "reddit.com", limit)
        return ProbeResult("reddit", "fallback" if count else "error", f"api_error:{str(exc)[:120]} ({note})", count)


def probe_facebook(query: str, limit: int) -> ProbeResult:
    page_id = env("FACEBOOK_PAGE_ID")
    token = env("FACEBOOK_PAGE_ACCESS_TOKEN")
    if not page_id or not token:
        count, note = web_fallback(query, "facebook.com", limit)
        return ProbeResult("facebook", "fallback" if count else "missing_credentials", f"page_credentials_missing ({note})", count)

    try:
        resp = requests.get(
            f"https://graph.facebook.com/v21.0/{page_id}/posts",
            params={
                "access_token": token,
                "fields": "id,message,created_time",
                "limit": max(5, min(limit * 2, 30)),
            },
            timeout=12,
        )
        if resp.status_code >= 400:
            count, note = web_fallback(query, "facebook.com", limit)
            return ProbeResult("facebook", "fallback" if count else "error", f"api_http_{resp.status_code} ({note})", count)
        payload = resp.json() if resp.text else {}
        rows = payload.get("data") or []
        if rows:
            return ProbeResult("facebook", "ok", "api_ok", min(len(rows), limit))
        count, note = web_fallback(query, "facebook.com", limit)
        return ProbeResult("facebook", "fallback" if count else "empty", f"api_empty ({note})", count)
    except Exception as exc:
        count, note = web_fallback(query, "facebook.com", limit)
        return ProbeResult("facebook", "fallback" if count else "error", f"api_error:{str(exc)[:120]} ({note})", count)


def probe_instagram(query: str, limit: int) -> ProbeResult:
    acct = env("INSTAGRAM_BUSINESS_ACCOUNT_ID")
    token = env("INSTAGRAM_ACCESS_TOKEN", "FACEBOOK_PAGE_ACCESS_TOKEN")
    if not acct or not token:
        count, note = web_fallback(query, "instagram.com", limit)
        return ProbeResult("instagram", "fallback" if count else "missing_credentials", f"business_credentials_missing ({note})", count)

    try:
        resp = requests.get(
            f"https://graph.facebook.com/v21.0/{acct}/media",
            params={"access_token": token, "fields": "id,caption,timestamp", "limit": max(5, min(limit * 2, 30))},
            timeout=12,
        )
        if resp.status_code >= 400:
            count, note = web_fallback(query, "instagram.com", limit)
            return ProbeResult("instagram", "fallback" if count else "error", f"api_http_{resp.status_code} ({note})", count)
        payload = resp.json() if resp.text else {}
        rows = payload.get("data") or []
        if rows:
            return ProbeResult("instagram", "ok", "api_ok", min(len(rows), limit))
        count, note = web_fallback(query, "instagram.com", limit)
        return ProbeResult("instagram", "fallback" if count else "empty", f"api_empty ({note})", count)
    except Exception as exc:
        count, note = web_fallback(query, "instagram.com", limit)
        return ProbeResult("instagram", "fallback" if count else "error", f"api_error:{str(exc)[:120]} ({note})", count)


def probe_linkedin() -> ProbeResult:
    token = env("LINKEDIN_ACCESS_TOKEN")
    if not token:
        return ProbeResult("linkedin", "missing_credentials", "access_token_missing", 0)
    try:
        resp = requests.get(
            "https://api.linkedin.com/v2/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if resp.status_code >= 400:
            return ProbeResult("linkedin", "error", f"api_http_{resp.status_code}", 0)
        return ProbeResult("linkedin", "ok", "token_valid", 1)
    except Exception as exc:
        return ProbeResult("linkedin", "error", f"api_error:{str(exc)[:120]}", 0)


def probe_tiktok() -> ProbeResult:
    token = env("TIKTOK_ACCESS_TOKEN")
    open_id = env("TIKTOK_OPEN_ID")
    if not token or not open_id:
        return ProbeResult("tiktok", "missing_credentials", "access_token_or_open_id_missing", 0)
    try:
        resp = requests.post(
            "https://open.tiktokapis.com/v2/user/info/",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"open_id": open_id, "fields": ["display_name", "profile_deep_link", "avatar_url"]},
            timeout=10,
        )
        if resp.status_code in {200, 201}:
            return ProbeResult("tiktok", "ok", "token_valid", 1)
        return ProbeResult("tiktok", "error", f"api_http_{resp.status_code}", 0)
    except Exception as exc:
        return ProbeResult("tiktok", "error", f"api_error:{str(exc)[:120]}", 0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe social integrations and fallback feeds.")
    parser.add_argument("--query", default="US stock market top headlines today")
    parser.add_argument("--limit", type=int, default=6)
    return parser.parse_args()


def main() -> int:
    load_env_file(Path(".env").resolve())
    load_env_file(Path("quantura_site/functions/.env").resolve())
    args = parse_args()

    probes = [
        probe_x(args.query, args.limit),
        probe_reddit(args.query, args.limit),
        probe_facebook(args.query, args.limit),
        probe_instagram(args.query, args.limit),
        probe_linkedin(),
        probe_tiktok(),
    ]

    print("social_channel_probe summary")
    print("=" * 72)
    for row in probes:
        print(f"{row.channel:10} status={row.status:20} count={row.count:<3} detail={row.detail}")

    hard_fail = [row for row in probes if row.status == "error"]
    return 1 if hard_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
