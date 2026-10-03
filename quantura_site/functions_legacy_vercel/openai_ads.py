"""Bounded, server-only conversion reporting after a contact has been saved."""
from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlsplit

import requests

CANONICAL_ORIGIN = "https://quantura.studio"
TRUSTED_ORIGINS = {CANONICAL_ORIGIN, "https://www.quantura.studio"}


def sanitized_source_url(value: Any) -> str:
    try:
        parsed = urlsplit(str(value or ""))
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if origin in TRUSTED_ORIGINS and not parsed.username and not parsed.password:
            return origin + (parsed.path or "/")
    except (ValueError, TypeError):
        pass
    return CANONICAL_ORIGIN + "/contact"


def _raw_cookie(header: str, name: str) -> str:
    prefix = name + "="
    for part in str(header or "").split(";"):
        part = part.strip()
        if part.startswith(prefix):
            return part[len(prefix):]
    return ""


def report_contact_lead(contact_id: str, context: Any, headers: Any, get_secret: Any) -> str | None:
    # Construction and secret retrieval are inside the failure boundary. Never
    # retry here or log identifiers, contact contents, credentials, or responses.
    try:
        if not isinstance(context, dict) or context.get("consent") != "granted":
            return None
        if str(headers.get("Sec-GPC", "")) == "1":
            return None
        key = get_secret("OPENAI_ADS_CONVERSIONS_API_KEY")
        pixel_id = get_secret("OPENAI_ADS_PIXEL_ID")
        if not key or not pixel_id:
            return None
        event_id = "lead_" + str(contact_id)
        event: dict[str, Any] = {
            "id": event_id, "type": "lead_created", "timestamp_ms": int(time.time() * 1000),
            "source_url": sanitized_source_url(context.get("sourceUrl")), "action_source": "web",
            "opt_out": True, "data": {"type": "customer_action"},
        }
        cookies = headers.get("Cookie", "")
        oppref = _raw_cookie(cookies, "__oppref") or context.get("oppref")
        obref = _raw_cookie(cookies, "__obref") or context.get("obref")
        if isinstance(oppref, str) and 0 < len(oppref) <= 4096:
            event["oppref"] = oppref
        if isinstance(obref, str) and 0 < len(obref) <= 4096:
            event["user"] = {"obref": obref}
        # Analytics consent does not separately authorize contact identity matching.
        requests.post("https://bzr.openai.com/v1/events", params={"pid": pixel_id},
                      headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
                      json={"validate_only": False, "events": [event]}, timeout=(0.5, 0.5))
        return event_id
    except Exception:
        return None
