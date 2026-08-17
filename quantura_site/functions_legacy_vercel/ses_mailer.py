from __future__ import annotations

import html
import json
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from firebase_admin import firestore
from google.cloud import secretmanager

DEFAULT_TEMPLATE_NAME = "quantura_newsletter_v1"
DEFAULT_SITE_ORIGIN = "https://quantura.studio"
LEGACY_SITE_ORIGIN = "https://quantura-e2e3d.web.app"
DAILY_SEND_CAP = 200

_SECRET_CACHE: dict[str, tuple[float, str]] = {}
_SECRET_CACHE_TTL_SECONDS = 300


def _resolve_project_id() -> str:
    candidates = [
        os.environ.get("GCP_PROJECT"),
        os.environ.get("GCLOUD_PROJECT"),
        os.environ.get("GOOGLE_CLOUD_PROJECT"),
    ]
    for candidate in candidates:
        value = str(candidate or "").strip()
        if value:
            return value

    firebase_config_raw = str(os.environ.get("FIREBASE_CONFIG") or "").strip()
    if firebase_config_raw:
        try:
            firebase_config = json.loads(firebase_config_raw)
            project_id = str(firebase_config.get("projectId") or "").strip()
            if project_id:
                return project_id
        except Exception:
            pass
    raise RuntimeError("Unable to resolve GCP project id for Secret Manager access.")


def get_secret(name: str) -> str:
    key = str(name or "").strip()
    if not key:
        raise ValueError("Secret name is required.")

    env_value = str(os.environ.get(key) or "").strip()
    if env_value:
        return env_value

    cached = _SECRET_CACHE.get(key)
    now = time.time()
    if cached and cached[0] > now:
        return cached[1]

    project_id = _resolve_project_id()
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{key}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    value = response.payload.data.decode("utf-8").strip()
    if not value:
        raise RuntimeError(f"Secret {key} is empty.")

    _SECRET_CACHE[key] = (now + _SECRET_CACHE_TTL_SECONDS, value)
    return value


def init_ses_client() -> tuple[Any, dict[str, str]]:
    access_key = get_secret("AWS_ACCESS_KEY_ID")
    secret_key = get_secret("AWS_SECRET_ACCESS_KEY")
    region = get_secret("AWS_REGION")
    from_email = get_secret("SES_FROM_EMAIL")

    config_set = ""
    try:
        config_set = get_secret("SES_CONFIG_SET")
    except Exception:
        config_set = ""

    template_name = DEFAULT_TEMPLATE_NAME
    try:
        template_name = get_secret("NEWSLETTER_TEMPLATE_NAME")
    except Exception:
        template_name = DEFAULT_TEMPLATE_NAME

    client = boto3.client(
        "sesv2",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return client, {
        "region": region,
        "from_email": from_email,
        "config_set": config_set,
        "template_name": template_name or DEFAULT_TEMPLATE_NAME,
    }


def _template_content() -> dict[str, str]:
    subject = "Quantura Daily Brief: {{top_story_title}}"
    html_part = """
<!doctype html>
<html>
  <body style="margin:0;padding:0;background:#0b1220;color:#e5edf8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#0b1220;padding:24px 12px;">
      <tr>
        <td align="center">
          <table role="presentation" width="640" cellspacing="0" cellpadding="0" style="max-width:640px;background:#121c2f;border:1px solid rgba(255,255,255,0.12);border-radius:16px;overflow:hidden;">
            <tr>
              <td style="padding:22px 24px;background:linear-gradient(135deg,#1d4ed8 0%,#0f172a 65%);">
                <div style="font-size:12px;letter-spacing:.08em;text-transform:uppercase;opacity:.82;">Quantura Daily Brief</div>
                <h1 style="margin:10px 0 0;font-size:24px;line-height:1.25;color:#f8fbff;">{{top_story_title}}</h1>
              </td>
            </tr>
            <tr>
              <td style="padding:20px 24px;">
                <p style="margin:0 0 12px;font-size:15px;line-height:1.65;color:#d6e2f2;">{{top_story_summary}}</p>
                <p style="margin:0 0 18px;"><a href="{{top_story_url}}" style="display:inline-block;padding:10px 14px;background:#1d4ed8;color:#ffffff;text-decoration:none;border-radius:999px;font-weight:600;">Read Top Story</a></p>
                <h2 style="margin:0 0 10px;font-size:17px;color:#f3f8ff;">Market Focus</h2>
                <p style="margin:0 0 16px;font-size:14px;line-height:1.65;color:#d6e2f2;">{{markets_bullets}}</p>
                <p style="margin:0 0 18px;"><a href="{{cta_url}}" style="display:inline-block;padding:10px 14px;border:1px solid rgba(255,255,255,0.25);color:#dbeafe;text-decoration:none;border-radius:999px;font-weight:600;">Open Quantura</a></p>
                <p style="margin:0;font-size:12px;line-height:1.6;color:#9eb4d4;">Informational content only. Not investment advice.</p>
              </td>
            </tr>
            <tr>
              <td style="padding:14px 24px;border-top:1px solid rgba(255,255,255,0.12);font-size:12px;line-height:1.7;color:#9eb4d4;">
                <a href="{{preferences_url}}" style="color:#93c5fd;text-decoration:none;">Email preferences</a>
                &nbsp;•&nbsp;
                <a href="{{unsubscribe_url}}" style="color:#93c5fd;text-decoration:none;">Unsubscribe</a>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
""".strip()
    text_part = (
        "Quantura Daily Brief\n"
        "Top story: {{top_story_title}}\n"
        "{{top_story_summary}}\n"
        "Read more: {{top_story_url}}\n\n"
        "Market focus: {{markets_bullets}}\n\n"
        "Open Quantura: {{cta_url}}\n"
        "Email preferences: {{preferences_url}}\n"
        "Unsubscribe: {{unsubscribe_url}}\n"
        "Informational content only. Not investment advice."
    )
    return {
        "subject": subject,
        "html": html_part,
        "text": text_part,
    }


def ensure_template_exists(client: Any, template_name: str | None = None) -> str:
    name = str(template_name or DEFAULT_TEMPLATE_NAME).strip() or DEFAULT_TEMPLATE_NAME
    content = _template_content()

    desired = {
        "Subject": content["subject"],
        "Html": content["html"],
        "Text": content["text"],
    }

    try:
        current = client.get_email_template(TemplateName=name)
        existing = (current.get("TemplateContent") if isinstance(current, dict) else {}) or {}
        if (
            str(existing.get("Subject") or "") != desired["Subject"]
            or str(existing.get("Html") or "") != desired["Html"]
            or str(existing.get("Text") or "") != desired["Text"]
        ):
            client.update_email_template(TemplateName=name, TemplateContent=desired)
        return name
    except Exception:
        client.create_email_template(TemplateName=name, TemplateContent=desired)
        return name


def _coerce_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if hasattr(raw, "to_datetime"):
        try:
            value = raw.to_datetime()
            if isinstance(value, datetime):
                return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _reserve_send_quota(db: Any, desired: int) -> dict[str, int]:
    desired_count = max(0, int(desired))
    now = datetime.now(timezone.utc)
    today_key = f"daily_{now.date().isoformat()}"
    yesterday_key = f"daily_{(now.date() - timedelta(days=1)).isoformat()}"

    sends_col = db.collection("email_sends")
    today_ref = sends_col.document(today_key)
    yesterday_ref = sends_col.document(yesterday_key)

    transaction = db.transaction()

    @firestore.transactional
    def _run(txn: Any) -> dict[str, int]:
        today_snap = today_ref.get(transaction=txn)
        yesterday_snap = yesterday_ref.get(transaction=txn)

        rolling_count = 0
        for snap in [today_snap, yesterday_snap]:
            if not snap.exists:
                continue
            data = snap.to_dict() or {}
            count = int(data.get("count") or 0)
            updated_at = _coerce_datetime(data.get("updatedAt"))
            window_start = _coerce_datetime(data.get("windowStartTs"))
            anchor = updated_at or window_start
            if not anchor:
                continue
            if now - anchor <= timedelta(hours=24):
                rolling_count += max(0, count)

        remaining = max(0, DAILY_SEND_CAP - rolling_count)
        reserved = min(remaining, desired_count)
        if reserved <= 0:
            return {
                "reserved": 0,
                "rolling24hCount": rolling_count,
                "remaining": remaining,
            }

        today_count = 0
        today_window_start = now
        if today_snap.exists:
            today_data = today_snap.to_dict() or {}
            today_count = int(today_data.get("count") or 0)
            today_window_start = _coerce_datetime(today_data.get("windowStartTs")) or now

        txn.set(
            today_ref,
            {
                "count": today_count + reserved,
                "windowStartTs": today_window_start,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )

        return {
            "reserved": reserved,
            "rolling24hCount": rolling_count,
            "remaining": remaining,
        }

    return _run(transaction)


def _load_subscribers(db: Any, *, max_candidates: int = 1200) -> list[dict[str, str]]:
    subscribers: list[dict[str, str]] = []
    query = db.collection("users").where("emailPrefs.newsletter", "==", True).limit(max_candidates)

    for snap in query.stream():
        doc = snap.to_dict() or {}
        email = str(doc.get("email") or "").strip().lower()
        if not email:
            continue
        if doc.get("emailVerified") is False:
            continue
        subscribers.append(
            {
                "uid": snap.id,
                "email": email,
            }
        )
    return subscribers


def _create_unsubscribe_token(db: Any, *, uid: str, email: str, topic: str) -> str:
    token = secrets.token_urlsafe(24)
    db.collection("email_unsubscribe_tokens").document(token).set(
        {
            "uid": uid,
            "email": email,
            "topic": topic,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "usedAt": None,
        },
        merge=True,
    )
    return token


def _render_markets_bullets(markets: list[dict[str, Any]] | list[str] | None) -> str:
    entries: list[str] = []
    for item in list(markets or [])[:8]:
        if isinstance(item, str):
            text = item.strip()
            if text:
                entries.append(text)
            continue
        if isinstance(item, dict):
            symbol = str(item.get("symbol") or item.get("ticker") or "").strip().upper()
            headline = str(item.get("headline") or item.get("title") or item.get("summary") or "").strip()
            if symbol and headline:
                entries.append(f"{symbol}: {headline}")
            elif symbol:
                entries.append(symbol)
            elif headline:
                entries.append(headline)
    if not entries:
        entries = [
            "Watchlist momentum and risk updates are live in your workspace.",
            "Forecast and AI signal activity can be reviewed inside the terminal.",
            "Use quantile bands and event windows to frame scenario ranges.",
        ]
    return " • ".join(entries)


def _compose_newsletter_content(
    *,
    payload: dict[str, Any] | None,
    unsubscribe_url: str,
    preferences_url: str,
    cta_url: str,
) -> dict[str, str]:
    data = payload or {}
    top_story = data.get("topStory") if isinstance(data.get("topStory"), dict) else {}

    top_story_title = str(top_story.get("title") or data.get("subject") or "Market setup update").strip() or "Market setup update"
    top_story_summary = str(
        top_story.get("summary")
        or data.get("summary")
        or "Your Quantura workspace has new context on forecasts, watchlist momentum, and AI insights."
    ).strip()
    top_story_url = str(top_story.get("url") or data.get("storyUrl") or cta_url).strip() or cta_url

    markets_bullets = _render_markets_bullets(data.get("markets") if isinstance(data.get("markets"), list) else None)

    subject = f"Quantura Daily Brief: {top_story_title}"

    html_body = f"""
<!doctype html>
<html>
  <body style="margin:0;padding:0;background:#0b1220;color:#e5edf8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#0b1220;padding:24px 12px;">
      <tr>
        <td align="center">
          <table role="presentation" width="640" cellspacing="0" cellpadding="0" style="max-width:640px;background:#121c2f;border:1px solid rgba(255,255,255,0.12);border-radius:16px;overflow:hidden;">
            <tr>
              <td style="padding:22px 24px;background:linear-gradient(135deg,#1d4ed8 0%,#0f172a 65%);">
                <div style="font-size:12px;letter-spacing:.08em;text-transform:uppercase;opacity:.82;">Quantura Daily Brief</div>
                <h1 style="margin:10px 0 0;font-size:24px;line-height:1.25;color:#f8fbff;">{html.escape(top_story_title)}</h1>
              </td>
            </tr>
            <tr>
              <td style="padding:20px 24px;">
                <p style="margin:0 0 12px;font-size:15px;line-height:1.65;color:#d6e2f2;">{html.escape(top_story_summary)}</p>
                <p style="margin:0 0 18px;"><a href="{html.escape(top_story_url)}" style="display:inline-block;padding:10px 14px;background:#1d4ed8;color:#ffffff;text-decoration:none;border-radius:999px;font-weight:600;">Read Top Story</a></p>
                <h2 style="margin:0 0 10px;font-size:17px;color:#f3f8ff;">Market Focus</h2>
                <p style="margin:0 0 16px;font-size:14px;line-height:1.65;color:#d6e2f2;">{html.escape(markets_bullets)}</p>
                <p style="margin:0 0 18px;"><a href="{html.escape(cta_url)}" style="display:inline-block;padding:10px 14px;border:1px solid rgba(255,255,255,0.25);color:#dbeafe;text-decoration:none;border-radius:999px;font-weight:600;">Open Quantura</a></p>
                <p style="margin:0;font-size:12px;line-height:1.6;color:#9eb4d4;">Informational content only. Not investment advice.</p>
              </td>
            </tr>
            <tr>
              <td style="padding:14px 24px;border-top:1px solid rgba(255,255,255,0.12);font-size:12px;line-height:1.7;color:#9eb4d4;">
                <a href="{html.escape(preferences_url)}" style="color:#93c5fd;text-decoration:none;">Email preferences</a>
                &nbsp;•&nbsp;
                <a href="{html.escape(unsubscribe_url)}" style="color:#93c5fd;text-decoration:none;">Unsubscribe</a>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
""".strip()

    text_body = (
        "Quantura Daily Brief\n"
        f"Top story: {top_story_title}\n"
        f"{top_story_summary}\n"
        f"Read more: {top_story_url}\n\n"
        f"Market focus: {markets_bullets}\n\n"
        f"Open Quantura: {cta_url}\n"
        f"Email preferences: {preferences_url}\n"
        f"Unsubscribe: {unsubscribe_url}\n"
        "Informational content only. Not investment advice."
    )

    return {
        "subject": subject,
        "html": html_body,
        "text": text_body,
    }


def send_newsletter_batch(
    db: Any,
    *,
    max_to_send: int = DAILY_SEND_CAP,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ses_client, ses_cfg = init_ses_client()
    template_name = ensure_template_exists(ses_client, ses_cfg.get("template_name"))

    subscribers = _load_subscribers(db, max_candidates=1200)
    desired = min(max(0, int(max_to_send)), len(subscribers), DAILY_SEND_CAP)
    reservation = _reserve_send_quota(db, desired)
    reserved = int(reservation.get("reserved") or 0)

    if reserved <= 0:
        return {
            "template": template_name,
            "reserved": 0,
            "sent": 0,
            "failed": 0,
            "rolling24hCount": int(reservation.get("rolling24hCount") or 0),
            "remaining": int(reservation.get("remaining") or 0),
            "attempted": 0,
            "status": "cap_reached",
        }

    site_origin = str(os.environ.get("PUBLIC_SITE_ORIGIN") or DEFAULT_SITE_ORIGIN).rstrip("/")
    if not site_origin:
        site_origin = DEFAULT_SITE_ORIGIN
    cta_url = site_origin
    preferences_url = f"{site_origin}/account"

    sent = 0
    failed = 0
    attempted = 0

    for subscriber in subscribers[:reserved]:
        attempted += 1
        uid = str(subscriber.get("uid") or "").strip()
        email = str(subscriber.get("email") or "").strip().lower()
        if not uid or not email:
            failed += 1
            continue

        unsubscribe_token = _create_unsubscribe_token(db, uid=uid, email=email, topic="newsletter")
        unsubscribe_url = f"{site_origin}/email/unsubscribe?token={unsubscribe_token}"
        content = _compose_newsletter_content(
            payload=payload,
            unsubscribe_url=unsubscribe_url,
            preferences_url=preferences_url,
            cta_url=cta_url,
        )

        status = "sent"
        error_text = ""
        message_id = ""

        try:
            send_kwargs: dict[str, Any] = {
                "FromEmailAddress": ses_cfg["from_email"],
                "Destination": {"ToAddresses": [email]},
                "Content": {
                    "Simple": {
                        "Subject": {"Data": content["subject"], "Charset": "UTF-8"},
                        "Body": {
                            "Html": {"Data": content["html"], "Charset": "UTF-8"},
                            "Text": {"Data": content["text"], "Charset": "UTF-8"},
                        },
                        "Headers": [
                            {"Name": "List-Unsubscribe", "Value": f"<{unsubscribe_url}>"},
                            {"Name": "List-Unsubscribe-Post", "Value": "List-Unsubscribe=One-Click"},
                        ],
                    }
                },
                "EmailTags": [
                    {"Name": "template", "Value": template_name},
                    {"Name": "topic", "Value": "newsletter"},
                    {"Name": "uid", "Value": uid[:120]},
                ],
            }
            if ses_cfg.get("config_set"):
                send_kwargs["ConfigurationSetName"] = ses_cfg["config_set"]

            result = ses_client.send_email(**send_kwargs)
            message_id = str(result.get("MessageId") or "")
            sent += 1
        except Exception as exc:
            status = "failed"
            error_text = str(exc)[:500]
            failed += 1

        db.collection("email_logs").document().set(
            {
                "uid": uid,
                "email": email,
                "template": template_name,
                "topic": "newsletter",
                "ts": firestore.SERVER_TIMESTAMP,
                "status": status,
                "error": error_text,
                "messageId": message_id,
            },
            merge=True,
        )

    return {
        "template": template_name,
        "reserved": reserved,
        "sent": sent,
        "failed": failed,
        "rolling24hCount": int(reservation.get("rolling24hCount") or 0),
        "remaining": int(reservation.get("remaining") or 0),
        "attempted": attempted,
        "status": "ok",
    }


def unsubscribe_with_token(db: Any, token: str) -> dict[str, Any]:
    clean_token = str(token or "").strip()
    if not clean_token:
        return {"ok": False, "error": "Missing token."}

    token_ref = db.collection("email_unsubscribe_tokens").document(clean_token)
    token_snap = token_ref.get()
    if not token_snap.exists:
        return {"ok": False, "error": "Invalid unsubscribe token."}

    token_doc = token_snap.to_dict() or {}
    uid = str(token_doc.get("uid") or "").strip()
    topic = str(token_doc.get("topic") or "newsletter").strip() or "newsletter"

    if not uid:
        return {"ok": False, "error": "Token missing user mapping."}

    pref_path = f"emailPrefs.{topic}"
    db.collection("users").document(uid).set(
        {
            pref_path: False,
            "emailPrefs.updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    token_ref.set({"usedAt": firestore.SERVER_TIMESTAMP}, merge=True)

    return {"ok": True, "uid": uid, "topic": topic}


def render_unsubscribe_html(result: dict[str, Any]) -> str:
    ok = bool(result.get("ok"))
    title = "You are unsubscribed" if ok else "Unsubscribe failed"
    message = (
        "You will no longer receive Quantura newsletter emails for this topic."
        if ok
        else str(result.get("error") or "The unsubscribe token is invalid or expired.")
    )
    return f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>{html.escape(title)}</title>
    <style>
      body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:#0b1220; color:#e5edf8; }}
      .wrap {{ min-height:100vh; display:grid; place-items:center; padding:24px; }}
      .card {{ width:min(560px,100%); border:1px solid rgba(255,255,255,0.18); border-radius:16px; padding:22px; background:#121c2f; }}
      h1 {{ margin:0 0 10px; font-size:1.35rem; }}
      p {{ margin:0; line-height:1.65; color:#c8d6eb; }}
      a {{ color:#93c5fd; text-decoration:none; }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <h1>{html.escape(title)}</h1>
        <p>{html.escape(message)}</p>
        <p style="margin-top:14px;"><a href="{DEFAULT_SITE_ORIGIN}">Return to Quantura</a> · <a href="{LEGACY_SITE_ORIGIN}">Legacy site</a></p>
      </div>
    </div>
  </body>
</html>
""".strip()
