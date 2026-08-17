from __future__ import annotations

import json
import os
import secrets
from datetime import datetime, timezone
from typing import Any

import firebase_admin
from firebase_admin import auth, credentials, firestore
from flask import Request, Response

from newsletter_mailer import (
    DEFAULT_MAX_SEND,
    maybe_get_secret,
    render_unsubscribe_html,
    send_email_campaign_batch,
    unsubscribe_with_token,
)

if not firebase_admin._apps:
    raw_service_account = str(os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON") or "").strip()
    if raw_service_account:
        firebase_admin.initialize_app(credentials.Certificate(json.loads(raw_service_account)))
    else:
        firebase_admin.initialize_app()

db = firestore.client()

DEFAULT_ADMIN_EMAILS = {"tamzid257@gmail.com"}
_ADMIN_KEY_CACHE: str | None = None


def _json_response(payload: dict[str, Any], status: int = 200) -> Response:
    return Response(json.dumps(payload), status=status, mimetype="application/json")


def _cors_headers() -> dict[str, str]:
    origin = str(os.environ.get("PUBLIC_SITE_ORIGIN") or "https://quantura.studio").rstrip("/")
    return {
        "Access-Control-Allow-Origin": origin or "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, X-Newsletter-Admin-Key",
        "Access-Control-Max-Age": "3600",
    }


def _preflight_response() -> Response:
    headers = _cors_headers()
    return Response("", status=204, headers=headers)


def _admin_emails() -> set[str]:
    env = str(os.environ.get("NEWSLETTER_ADMIN_EMAILS") or "").strip()
    if not env:
        return set(DEFAULT_ADMIN_EMAILS)
    out = set(DEFAULT_ADMIN_EMAILS)
    for item in env.split(","):
        value = str(item or "").strip().lower()
        if value:
            out.add(value)
    return out


def _get_expected_admin_key() -> str:
    global _ADMIN_KEY_CACHE
    if _ADMIN_KEY_CACHE is not None:
        return _ADMIN_KEY_CACHE
    env_key = str(os.environ.get("NEWSLETTER_ADMIN_KEY") or "").strip()
    if env_key:
        _ADMIN_KEY_CACHE = env_key
        return _ADMIN_KEY_CACHE
    _ADMIN_KEY_CACHE = maybe_get_secret("NEWSLETTER_ADMIN_KEY")
    return _ADMIN_KEY_CACHE or ""


def _extract_bearer_token(request: Request) -> str:
    auth_header = str(request.headers.get("Authorization") or "").strip()
    if not auth_header.lower().startswith("bearer "):
        return ""
    return auth_header[7:].strip()


def _is_admin_request(request: Request) -> bool:
    provided_key = str(request.headers.get("X-Newsletter-Admin-Key") or request.args.get("adminKey") or "").strip()
    expected_key = _get_expected_admin_key()
    if expected_key and provided_key and secrets.compare_digest(provided_key, expected_key):
        return True

    token = _extract_bearer_token(request)
    if not token:
        return False
    try:
        claims = auth.verify_id_token(token, check_revoked=False)
    except Exception:
        return False
    if bool(claims.get("admin")):
        return True
    email = str(claims.get("email") or "").strip().lower()
    return email in _admin_emails()


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _default_weekly_payload() -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "title": "Weekly market workflow update",
        "subject": f"Week of {now.date().isoformat()}",
        "summary": "Macro context, model outputs, and watchlist execution checkpoints from this week.",
        "highlights": [
            "Review top watchlist movers and model-confidence shifts.",
            "Confirm scenario invalidation levels before changing size.",
            "Escalate high-uncertainty names through Model Council verification.",
        ],
        "ctaUrl": "https://quantura.studio/forecasting",
    }


def send_newsletter_daily_http(request: Request) -> Response:
    if request.method == "OPTIONS":
        return _preflight_response()

    headers = _cors_headers()
    if request.method != "POST":
        return Response("Method Not Allowed", status=405, headers=headers)

    if not _is_admin_request(request):
        return Response("Forbidden", status=403, headers=headers)

    body = request.get_json(silent=True)
    payload = body if isinstance(body, dict) else {}

    mode = str(payload.get("mode") or "newsletter").strip().lower()
    campaign_payload = payload.get("campaign") if isinstance(payload.get("campaign"), dict) else payload
    recipients = payload.get("recipients") if isinstance(payload.get("recipients"), list) else None
    max_to_send = _to_int(payload.get("maxToSend"), _to_int(os.environ.get("NEWSLETTER_MAX_SEND"), DEFAULT_MAX_SEND))
    max_to_send = max(1, min(max_to_send, 2000))
    dry_run = _to_bool(payload.get("dryRun"), False)

    result = send_email_campaign_batch(
        db,
        mode=mode,
        max_to_send=max_to_send,
        payload=campaign_payload,
        recipients=recipients,
        dry_run=dry_run,
    )
    response = _json_response(result, status=200)
    response.headers.update(headers)
    return response


def send_newsletter_weekly_scheduler(cloud_event: Any) -> None:
    del cloud_event
    max_to_send = _to_int(os.environ.get("NEWSLETTER_WEEKLY_MAX_SEND"), 1500)
    max_to_send = max(1, min(max_to_send, 2000))

    result = send_email_campaign_batch(
        db,
        mode="newsletter",
        max_to_send=max_to_send,
        payload=_default_weekly_payload(),
        recipients=None,
        dry_run=False,
    )
    print(json.dumps({"event": "newsletter_weekly_scheduler", "result": result}, default=str))


def email_unsubscribe_http(request: Request) -> Response:
    token = str(request.args.get("token") or "").strip()
    if not token and request.method == "POST":
        token = str(request.form.get("token") or "").strip()

    result = unsubscribe_with_token(db, token)
    html_response = render_unsubscribe_html(result)
    status = 200 if bool(result.get("ok")) else 400
    return Response(html_response, status=status, mimetype="text/html")
