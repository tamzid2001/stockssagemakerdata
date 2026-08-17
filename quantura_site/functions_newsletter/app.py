from __future__ import annotations

import hmac
import os

from flask import Flask, Response, request

import main

app = Flask(__name__)


def _cron_authorized() -> bool:
    expected = str(os.environ.get("CRON_SECRET") or "").strip()
    supplied = str(request.headers.get("Authorization") or "").strip()
    if supplied.lower().startswith("bearer "):
        supplied = supplied[7:].strip()
    if not supplied:
        supplied = str(request.headers.get("X-Cron-Secret") or "").strip()
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


@app.get("/api/health")
def health() -> dict[str, object]:
    return {"ok": True, "service": "quantura-newsletter"}


@app.route("/api/email/send-newsletter-daily", methods=["POST", "OPTIONS"])
@app.route("/api/email/send-campaign", methods=["POST", "OPTIONS"])
@app.route("/api/email/send-newsletter-weekly", methods=["POST", "OPTIONS"])
def send_campaign() -> Response:
    return main.send_newsletter_daily_http(request)


@app.route("/email/unsubscribe", methods=["GET", "POST"])
def unsubscribe() -> Response:
    return main.email_unsubscribe_http(request)


@app.route("/api/internal/cron/newsletter-weekly", methods=["GET", "POST"])
def newsletter_weekly() -> tuple[dict[str, object], int] | dict[str, object]:
    if not _cron_authorized():
        return {"ok": False, "error": "unauthorized"}, 401
    main.send_newsletter_weekly_scheduler({})
    return {"ok": True, "job": "newsletter-weekly"}
