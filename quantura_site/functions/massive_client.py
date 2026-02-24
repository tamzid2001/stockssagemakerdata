from __future__ import annotations

import copy
import json
import random
import time
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    import requests
except Exception:  # pragma: no cover - requests may be absent in minimal test runtimes
    requests = None  # type: ignore


class MassiveApiError(RuntimeError):
    def __init__(self, status_code: int, message: str, *, body: Any = None) -> None:
        super().__init__(message)
        self.status_code = int(status_code or 500)
        self.body = body


def is_blocked_massive_path(path: str) -> bool:
    clean = "/" + str(path or "").strip().lstrip("/")
    lowered = clean.lower()
    return "balance-sheets" in lowered


class MassiveClient:
    """
    Thin Massive REST wrapper for shared retries, cursor pagination, and TTL cache.
    """

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.massive.com",
        timeout_seconds: float = 18.0,
        max_retries: int = 3,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.base_url = str(base_url or "https://api.massive.com").strip().rstrip("/")
        self.timeout_seconds = max(1.0, float(timeout_seconds or 18.0))
        self.max_retries = max(0, int(max_retries or 0))
        self._cache: dict[str, tuple[float, Any]] = {}
        self._cache_lock = Lock()

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _assert_allowed_path(self, path: str) -> str:
        clean = "/" + str(path or "").strip().lstrip("/")
        if is_blocked_massive_path(clean):
            raise MassiveApiError(
                400,
                "Massive balance-sheets endpoint is blocked by policy and cannot be requested.",
            )
        return clean

    @staticmethod
    def _extract_cursor(payload: dict[str, Any]) -> str | None:
        direct = payload.get("next_cursor") or payload.get("nextCursor")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        url_like = payload.get("next_url") or payload.get("nextUrl") or payload.get("next")
        if isinstance(url_like, str) and url_like.strip():
            parsed = urlparse(url_like.strip())
            cursor_values = parse_qs(parsed.query).get("cursor") or []
            if cursor_values and str(cursor_values[0]).strip():
                return str(cursor_values[0]).strip()
        return None

    @staticmethod
    def _stable_params(params: dict[str, Any] | None) -> list[tuple[str, str]]:
        if not isinstance(params, dict):
            return []
        stable: list[tuple[str, str]] = []
        for key in sorted(params.keys()):
            if params[key] is None:
                continue
            stable.append((str(key), str(params[key])))
        return stable

    def _cache_get(self, key: str) -> Any | None:
        with self._cache_lock:
            entry = self._cache.get(key)
            if not entry:
                return None
            expires_at, payload = entry
            if expires_at <= time.time():
                self._cache.pop(key, None)
                return None
            return copy.deepcopy(payload)

    def _cache_set(self, key: str, payload: Any, ttl_seconds: int) -> None:
        if ttl_seconds <= 0:
            return
        with self._cache_lock:
            self._cache[key] = (time.time() + int(ttl_seconds), copy.deepcopy(payload))

    def request_json(
        self,
        *,
        path: str,
        params: dict[str, Any] | None = None,
        method: str = "GET",
        timeout_seconds: float | None = None,
        cache_ttl_seconds: int = 0,
    ) -> dict[str, Any]:
        if not self.api_key:
            raise MassiveApiError(503, "Massive API key is not configured.")
        if requests is None:
            raise MassiveApiError(503, "requests dependency is unavailable.")
        clean_path = self._assert_allowed_path(path)
        timeout = max(1.0, float(timeout_seconds or self.timeout_seconds))
        method_norm = str(method or "GET").upper()
        cache_key = f"{method_norm}:{clean_path}:{json.dumps(self._stable_params(params), ensure_ascii=True)}"
        if cache_ttl_seconds > 0:
            cached = self._cache_get(cache_key)
            if isinstance(cached, dict):
                return cached

        url = f"{self.base_url}{clean_path}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "X-API-Key": self.api_key,
            "Accept": "application/json",
            "User-Agent": "Quantura/1.0 (+https://quantura-e2e3d.web.app)",
        }

        attempts = self.max_retries + 1
        last_status = 503
        for attempt in range(attempts):
            try:
                response = requests.request(
                    method_norm,
                    url,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                )
            except requests.RequestException as exc:
                last_status = 503
                if attempt >= attempts - 1:
                    raise MassiveApiError(503, f"Massive request failed: {str(exc)[:220]}") from exc
                delay = min(8.0, 0.4 * (2 ** attempt)) + random.uniform(0.0, 0.25)
                time.sleep(delay)
                continue

            last_status = int(response.status_code or 500)
            if last_status in {429, 500, 502, 503, 504} and attempt < attempts - 1:
                delay = min(8.0, 0.4 * (2 ** attempt)) + random.uniform(0.0, 0.25)
                time.sleep(delay)
                continue

            if last_status >= 400:
                body_text = ""
                try:
                    body_text = response.text[:500]
                except Exception:
                    body_text = ""
                raise MassiveApiError(
                    last_status,
                    f"Massive request failed with status {last_status}.",
                    body=body_text,
                )

            try:
                payload = response.json() if response.text else {}
            except Exception as exc:
                raise MassiveApiError(502, "Massive response was not valid JSON.") from exc
            if not isinstance(payload, dict):
                payload = {"results": payload}
            if cache_ttl_seconds > 0:
                self._cache_set(cache_key, payload, int(cache_ttl_seconds))
            return payload

        raise MassiveApiError(last_status, f"Massive request failed with status {last_status}.")

    def fetch_cursor_pages(
        self,
        *,
        path: str,
        params: dict[str, Any] | None = None,
        cursor_param: str = "cursor",
        max_pages: int = 20,
        cache_ttl_seconds: int = 0,
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        next_cursor: str | None = None
        pages = max(1, min(int(max_pages or 20), 100))
        for _ in range(pages):
            request_params = dict(params or {})
            if next_cursor:
                request_params[cursor_param] = next_cursor
            payload = self.request_json(
                path=path,
                params=request_params,
                cache_ttl_seconds=cache_ttl_seconds,
            )
            rows = payload.get("results")
            if isinstance(rows, list):
                output.extend([row for row in rows if isinstance(row, dict)])
            next_cursor = self._extract_cursor(payload)
            if not next_cursor:
                break
        return output
