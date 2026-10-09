"""Dependency-free Quantura HTTP client. Mutations are never auto-retried."""
from __future__ import annotations

import json
import os
import time
import uuid
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit, quote, unquote
from urllib.request import HTTPRedirectHandler, Request, build_opener

DEFAULT_BASE_URL = "https://quantura.studio/api/v1"


class QuanturaError(Exception):
    def __init__(self, message, *, status=0, code="REQUEST_FAILED", request_id=None):
        super().__init__(message)
        self.status, self.code, self.request_id = status, code, request_id


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def api_base(value):
    parsed = urlsplit(value)
    if (parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1", "::1"))):
        raise ValueError("Use an HTTPS API URL, or loopback HTTP for development.")
    return value.rstrip("/") + ("/api/v1" if parsed.path in ("", "/") else "")


class Quantura:
    def __init__(self, token=None, *, base_url=DEFAULT_BASE_URL, timeout=60, opener=None):
        self.base_url, self.timeout = api_base(base_url), timeout
        self.token = token or (lambda: os.environ.get("QUANTURA_API_KEY") or os.environ.get("QUANTURA_ACCESS_TOKEN", ""))
        self.opener = opener or build_opener(_NoRedirect())

    def request(self, path, *, method="GET", body=None, query=None, idempotency_key=None, raw=False):
        if not path or path.startswith("/") or ":" in path or any(part in (".", "..") for part in unquote(path).split("/")):
            raise ValueError("Use a relative Quantura API path.")
        token = self.token() if callable(self.token) else self.token
        if not token:
            raise QuanturaError("Run quantura login or set QUANTURA_API_KEY.", code="AUTH_REQUIRED")
        url = self.base_url + "/" + path
        if query:
            url += "?" + urlencode({key: value for key, value in query.items() if value is not None})
        headers = {"Authorization": "Bearer " + token, "Accept": "text/csv" if raw else "application/json"}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode("utf-8")
        req = Request(url, data=data, headers=headers, method=method)
        try:
            with self.opener.open(req, timeout=self.timeout) as response:
                payload = response.read()
        except HTTPError as error:
            try:
                value = json.loads(error.read())
                details = value.get("error", {})
                if not isinstance(details, dict):
                    details = {"code": details, "message": value.get("message")}
            except (ValueError, AttributeError):
                details = {}
            raise QuanturaError(details.get("message") or f"Quantura request failed ({error.code}).", status=error.code,
                                code=details.get("code") or "REQUEST_FAILED",
                                request_id=details.get("request_id") or error.headers.get("X-Request-ID")) from None
        if raw:
            return payload
        try:
            return json.loads(payload)
        except ValueError:
            raise QuanturaError("The API did not return JSON.", code="INVALID_RESPONSE") from None

    def search(self, q, *, source="auto", limit=8, mode="open"):
        return self.request("market-search", query={"q": q, "source": source, "limit": limit, "mode": mode})

    def resolve(self, url):
        return self.request("market-search/resolve", query={"url": url})

    def models(self):
        return self.request("forecast/models")

    def access(self):
        return self.request("me/access")

    def create_forecast(self, request, *, idempotency_key=None):
        return self.request("ensemble-forecasts", method="POST", body=request, idempotency_key=idempotency_key or str(uuid.uuid4()))

    def get_forecast(self, forecast_id):
        return self.request("ensemble-forecasts/" + quote(forecast_id, safe=""))

    def download_forecast(self, forecast_id):
        return self.request("ensemble-forecasts/" + quote(forecast_id, safe="") + "/download", raw=True)

    def stamp_forecast(self, forecast_id):
        return self.request("ensemble-forecasts/" + quote(forecast_id, safe="") + "/proof", method="POST", body={})

    def verify_forecast(self, forecast_id):
        return self.request("ensemble-forecasts/" + quote(forecast_id, safe="") + "/proof", query={"verify": "true"})

    def download_forecast_proof(self, forecast_id):
        return self.request("ensemble-forecasts/" + quote(forecast_id, safe="") + "/proof", raw=True)

    def history(self, request):
        return self.request("market-data/stocks/history", method="POST", body=request, raw=request.get("format") == "csv")

    def history_pages(self, request, *, max_pages=1000):
        seen, cursor = set(), request.get("cursor")
        for _ in range(max_pages):
            body = {**request, "format": "json", "page_mode": True}
            if cursor:
                body["cursor"] = cursor
            page = self.history(body)
            yield page
            next_cursor = page.get("next_cursor") or page.get("data", {}).get("next_cursor")
            if not next_cursor:
                return
            if not isinstance(next_cursor, str) or next_cursor in seen or next_cursor == cursor:
                raise QuanturaError("History pagination stalled.", code="PAGINATION_STALLED")
            seen.add(next_cursor)
            cursor = next_cursor
        raise QuanturaError("History exceeds max_pages. Continue from the last cursor.", code="PAGE_LIMIT")

    def ask_scout(self, context, question, *, conversation_id=None, turn_id=None):
        body = {"context": context, "question": question, "turn_id": turn_id or str(uuid.uuid4())}
        if conversation_id:
            body["conversation_id"] = conversation_id
        return self.request("jev/forecast-questions", method="POST", body=body)

    def wait_for_forecast(self, forecast_id, *, interval=5, timeout=900):
        if interval < 1 or timeout <= 0:
            raise ValueError("Use interval >= 1 and a positive timeout.")
        deadline = time.monotonic() + timeout
        while True:
            value = self.get_forecast(forecast_id)
            job = value.get("data", value)
            if job.get("status") == "completed":
                return value
            if job.get("status") in ("failed", "cancelled", "canceled"):
                raise QuanturaError("Forecast failed. Inspect its error and reference.", code=(job.get("error") or {}).get("code", "FORECAST_FAILED"))
            if time.monotonic() + interval >= deadline:
                raise QuanturaError("Forecast is still running. Poll its ID again.", code="FORECAST_TIMEOUT")
            time.sleep(interval)
