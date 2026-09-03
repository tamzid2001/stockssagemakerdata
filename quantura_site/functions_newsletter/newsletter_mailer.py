from __future__ import annotations

import hashlib
import html
import json
import os
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import requests
from firebase_admin import firestore
from google.cloud import secretmanager

DEFAULT_SITE_ORIGIN = "https://quantura.studio"
DEFAULT_SUPPORT_EMAIL = "hello@quantura.studio"
DEFAULT_MAILING_ADDRESS = ""
DEFAULT_OPENAI_MODEL = "gpt-5-mini"
DEFAULT_MAX_SEND = 500
DAILY_SEND_CAP = 2000

_SECRET_CACHE: dict[str, tuple[float, str]] = {}
_SECRET_CACHE_TTL_SECONDS = 300

_OPENAI_TIMEOUT_SECONDS = 30
_OPENAI_OUTPUT_CHARS_LIMIT = 18000


@dataclass(frozen=True)
class CampaignModeConfig:
    mode: str
    topic: str
    label: str
    fallback_subject_prefix: str
    fallback_headline: str


MODE_CONFIGS: dict[str, CampaignModeConfig] = {
    "newsletter": CampaignModeConfig(
        mode="newsletter",
        topic="newsletter",
        label="Quantura Weekly Intelligence",
        fallback_subject_prefix="Quantura Weekly Brief",
        fallback_headline="Your weekly market workflow briefing",
    ),
    "cold_email": CampaignModeConfig(
        mode="cold_email",
        topic="marketing",
        label="Quantura Intro",
        fallback_subject_prefix="Quantura Intro",
        fallback_headline="A faster institutional workflow for market research",
    ),
    "pitch": CampaignModeConfig(
        mode="pitch",
        topic="marketing",
        label="Quantura Enterprise Pitch",
        fallback_subject_prefix="Quantura Enterprise",
        fallback_headline="Execution-ready market intelligence for teams",
    ),
}


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


def maybe_get_secret(name: str) -> str:
    try:
        return get_secret(name)
    except Exception:
        return ""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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


def _normalize_mode(value: Any) -> str:
    mode = str(value or "newsletter").strip().lower()
    if mode not in MODE_CONFIGS:
        return "newsletter"
    return mode


def _normalize_email(value: Any) -> str:
    email = str(value or "").strip().lower()
    if not email:
        return ""
    if "@" not in email or "." not in email:
        return ""
    if len(email) > 320:
        return ""
    return email


def _normalize_url(value: Any, *, fallback: str) -> str:
    url = str(value or "").strip()
    if not url:
        return fallback
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return f"{fallback.rstrip('/')}{url}"
    return fallback


def _normalize_recipient_uid(email: str, uid: str | None) -> str:
    clean_uid = str(uid or "").strip()
    if clean_uid:
        return clean_uid[:180]
    digest = hashlib.sha256(email.encode("utf-8")).hexdigest()[:32]
    return f"lead:{digest}"


def _email_hash(email: str) -> str:
    return hashlib.sha256(email.encode("utf-8")).hexdigest()


def _extract_responses_output_text(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""

    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    output = payload.get("output")
    if not isinstance(output, list):
        return ""

    chunks: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            part_text = part.get("text")
            if isinstance(part_text, str) and part_text.strip():
                chunks.append(part_text.strip())
    return "\n".join(chunks).strip()


def _parse_json_block(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    if raw.startswith("```"):
        raw = raw.strip("`")
        first_newline = raw.find("\n")
        if first_newline >= 0:
            raw = raw[first_newline + 1 :]
    raw = raw.strip()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        snippet = raw[start : end + 1]
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _generate_openai_campaign_copy(mode: str, payload: dict[str, Any]) -> dict[str, Any]:
    api_key = str(os.environ.get("OPENAI_API_KEY") or "").strip() or maybe_get_secret("OPENAI_API_KEY")
    if not api_key:
        return {}

    model = str(os.environ.get("NEWSLETTER_OPENAI_MODEL") or DEFAULT_OPENAI_MODEL).strip() or DEFAULT_OPENAI_MODEL
    mode_cfg = MODE_CONFIGS[_normalize_mode(mode)]

    system_prompt = (
        "You write production-grade financial marketing emails for Quantura.\n"
        "Tone: concise, credible, institutional.\n"
        "No hype, no guarantees, no fabricated performance claims.\n"
        "Return valid JSON only with keys: "
        "subject, preheader, headline, subheadline, intro, sections, ctaLabel, ctaUrl, signoff.\n"
        "sections must be an array of 2-4 objects with keys: title, body, bullets, linkLabel, linkUrl.\n"
        "bullets must be 2-4 concise strings."
    )
    user_prompt = (
        f"Mode: {mode_cfg.mode}\n"
        f"Campaign label: {mode_cfg.label}\n"
        f"Campaign context JSON:\n{json.dumps(payload, ensure_ascii=True)}\n"
        "Return JSON only."
    )

    body = {
        "model": model,
        "input": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_output_tokens": 1200,
        "temperature": 0.7,
    }

    try:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=_OPENAI_TIMEOUT_SECONDS,
        )
        if response.status_code >= 400:
            return {}
        parsed = response.json()
        text = _extract_responses_output_text(parsed)
        if not text:
            return {}
        if len(text) > _OPENAI_OUTPUT_CHARS_LIMIT:
            text = text[:_OPENAI_OUTPUT_CHARS_LIMIT]
        return _parse_json_block(text)
    except Exception:
        return {}


def _build_sections(raw_sections: Any, payload: dict[str, Any]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    if isinstance(raw_sections, list):
        for item in raw_sections[:4]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()[:120]
            body = str(item.get("body") or "").strip()[:700]
            bullets_raw = item.get("bullets")
            bullets: list[str] = []
            if isinstance(bullets_raw, list):
                for bullet in bullets_raw[:4]:
                    bullet_text = str(bullet or "").strip()
                    if bullet_text:
                        bullets.append(bullet_text[:180])
            link_label = str(item.get("linkLabel") or "").strip()[:100]
            link_url = str(item.get("linkUrl") or "").strip()[:500]
            if title or body:
                sections.append(
                    {
                        "title": title or "Update",
                        "body": body or "Fresh intelligence is available in your Quantura workspace.",
                        "bullets": bullets,
                        "linkLabel": link_label,
                        "linkUrl": link_url,
                    }
                )

    if sections:
        return sections

    highlights = payload.get("highlights")
    default_bullets: list[str] = []
    if isinstance(highlights, list):
        for item in highlights[:4]:
            line = str(item or "").strip()
            if line:
                default_bullets.append(line[:180])

    return [
        {
            "title": "What changed this week",
            "body": str(payload.get("summary") or "Market context, forecasts, and execution signals were updated this week.").strip()[:700],
            "bullets": default_bullets
            or [
                "Macro and rates context updated for active watchlists.",
                "Forecast and indicator workflows refreshed in Terminal.",
                "Model Council summaries ready for verification and escalation.",
            ],
            "linkLabel": "Open Terminal",
            "linkUrl": "/forecasting",
        },
        {
            "title": "Execution checklist",
            "body": "Turn signals into decision-ready actions with explicit invalidation levels and owner routing.",
            "bullets": [
                "Review current scenario bands before changing size.",
                "Track catalyst timing and thesis invalidation triggers.",
                "Publish concise notes to preserve decision audit trail.",
            ],
            "linkLabel": "Open Dashboard",
            "linkUrl": "/dashboard",
        },
    ]


def _build_campaign_copy(
    mode: str,
    payload: dict[str, Any],
    *,
    site_origin: str,
) -> dict[str, Any]:
    mode_cfg = MODE_CONFIGS[_normalize_mode(mode)]
    ai_copy = _generate_openai_campaign_copy(mode_cfg.mode, payload)

    fallback_title = str(payload.get("title") or payload.get("subject") or mode_cfg.fallback_headline).strip()
    subject = str(ai_copy.get("subject") or f"{mode_cfg.fallback_subject_prefix}: {fallback_title}").strip()[:180]
    preheader = str(ai_copy.get("preheader") or payload.get("preheader") or "Signal → scenario → execution, summarized for your workflow.").strip()[:220]
    headline = str(ai_copy.get("headline") or fallback_title or mode_cfg.fallback_headline).strip()[:180]
    subheadline = str(
        ai_copy.get("subheadline")
        or payload.get("subheadline")
        or "Concise market context and actionable next steps for your Quantura workflow."
    ).strip()[:240]
    intro = str(
        ai_copy.get("intro")
        or payload.get("intro")
        or payload.get("summary")
        or "This briefing captures this week’s market context so your team can move from signal to execution with less friction."
    ).strip()[:900]

    cta_label = str(ai_copy.get("ctaLabel") or payload.get("ctaLabel") or "Open Quantura").strip()[:80] or "Open Quantura"
    cta_url = _normalize_url(ai_copy.get("ctaUrl") or payload.get("ctaUrl"), fallback=site_origin)
    signoff = str(ai_copy.get("signoff") or payload.get("signoff") or "Quantura Research & Product Team").strip()[:120]

    sections = _build_sections(ai_copy.get("sections"), payload)
    return {
        "subject": subject,
        "preheader": preheader,
        "headline": headline,
        "subheadline": subheadline,
        "intro": intro,
        "sections": sections,
        "ctaLabel": cta_label,
        "ctaUrl": cta_url,
        "signoff": signoff,
        "modeLabel": mode_cfg.label,
        "topic": mode_cfg.topic,
    }


def _render_campaign_email(
    campaign: dict[str, Any],
    *,
    site_origin: str,
    unsubscribe_url: str,
    preferences_url: str,
    support_email: str,
    mailing_address: str,
) -> dict[str, str]:
    safe_mode_label = html.escape(str(campaign.get("modeLabel") or "Quantura"))
    safe_preheader = html.escape(str(campaign.get("preheader") or ""))
    safe_headline = html.escape(str(campaign.get("headline") or "Market briefing"))
    safe_subheadline = html.escape(str(campaign.get("subheadline") or ""))
    safe_intro = html.escape(str(campaign.get("intro") or ""))
    safe_cta_label = html.escape(str(campaign.get("ctaLabel") or "Open Quantura"))
    safe_cta_url = html.escape(str(campaign.get("ctaUrl") or DEFAULT_SITE_ORIGIN))
    safe_signoff = html.escape(str(campaign.get("signoff") or "Quantura Team"))
    safe_preferences_url = html.escape(preferences_url)
    safe_unsubscribe_url = html.escape(unsubscribe_url)
    safe_support_email = html.escape(support_email)
    safe_support_href = html.escape(f"mailto:{support_email}")
    safe_address = html.escape(mailing_address)

    section_html_parts: list[str] = []
    section_text_parts: list[str] = []
    sections = campaign.get("sections") if isinstance(campaign.get("sections"), list) else []
    for section in sections[:4]:
        if not isinstance(section, dict):
            continue
        title = html.escape(str(section.get("title") or "Update"))
        body = html.escape(str(section.get("body") or ""))
        bullets_raw = section.get("bullets")
        bullets: list[str] = []
        if isinstance(bullets_raw, list):
            for item in bullets_raw[:4]:
                text = str(item or "").strip()
                if text:
                    bullets.append(text)

        link_label = str(section.get("linkLabel") or "").strip()
        link_url = str(section.get("linkUrl") or "").strip()
        if link_url and link_url.startswith("/"):
            link_url = f"{site_origin.rstrip('/')}{link_url}"

        bullet_html = ""
        bullet_text = ""
        if bullets:
            bullet_html = "<ul style=\"margin:10px 0 0 18px;padding:0;color:#c8d6eb;\">" + "".join(
                f"<li style=\"margin:0 0 8px 0;\">{html.escape(item)}</li>" for item in bullets
            ) + "</ul>"
            bullet_text = "\n".join([f"- {item}" for item in bullets])

        link_html = ""
        if link_label and link_url:
            link_html = (
                f"<p style=\"margin:12px 0 0 0;\"><a href=\"{html.escape(link_url)}\" "
                "style=\"color:#93c5fd;text-decoration:none;font-weight:600;\">"
                f"{html.escape(link_label)} ↗</a></p>"
            )

        section_html_parts.append(
            "<tr><td style=\"padding:18px 24px;border-bottom:1px solid rgba(255,255,255,0.08);\">"
            f"<h2 style=\"margin:0 0 8px 0;font-size:18px;line-height:1.35;color:#f3f8ff;\">{title}</h2>"
            f"<p style=\"margin:0;font-size:14px;line-height:1.7;color:#d7e4f5;\">{body}</p>"
            f"{bullet_html}{link_html}"
            "</td></tr>"
        )

        section_text_parts.append(
            f"{str(section.get('title') or 'Update')}\n"
            f"{str(section.get('body') or '').strip()}\n"
            f"{bullet_text}\n"
            + (f"{link_label}: {link_url}\n" if link_label and link_url else "")
        )

    sections_html = "".join(section_html_parts) if section_html_parts else ""

    html_body = f"""
<!doctype html>
<html>
  <body style="margin:0;padding:0;background:#070c16;color:#e5edf8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
    <div style="display:none;max-height:0;overflow:hidden;opacity:0;">{safe_preheader}</div>
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#070c16;padding:20px 10px;">
      <tr>
        <td align="center">
          <table role="presentation" width="680" cellspacing="0" cellpadding="0" style="max-width:680px;background:#0f1829;border:1px solid rgba(255,255,255,0.12);border-radius:18px;overflow:hidden;">
            <tr>
              <td style="padding:20px 24px;background:linear-gradient(135deg,#173a7a 0%,#0b1220 70%);border-bottom:1px solid rgba(255,255,255,0.14);">
                <div style="font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:#b7cae8;">{safe_mode_label}</div>
                <h1 style="margin:10px 0 6px 0;font-size:28px;line-height:1.2;color:#f8fbff;">{safe_headline}</h1>
                <p style="margin:0;font-size:14px;line-height:1.6;color:#c5d6ef;">{safe_subheadline}</p>
              </td>
            </tr>
            <tr>
              <td style="padding:20px 24px 14px 24px;">
                <p style="margin:0;font-size:15px;line-height:1.75;color:#d7e4f5;">{safe_intro}</p>
                <p style="margin:16px 0 4px 0;">
                  <a href="{safe_cta_url}" style="display:inline-block;padding:11px 16px;background:#2563eb;color:#ffffff;text-decoration:none;border-radius:999px;font-weight:700;">{safe_cta_label}</a>
                </p>
              </td>
            </tr>
            {sections_html}
            <tr>
              <td style="padding:16px 24px;border-top:1px solid rgba(255,255,255,0.12);">
                <p style="margin:0;font-size:13px;line-height:1.7;color:#9db1d1;">
                  {safe_signoff}<br/>
                  Support: <a href="{safe_support_href}" style="color:#93c5fd;text-decoration:none;">{safe_support_email}</a><br/>
                  {safe_address}
                </p>
                <p style="margin:12px 0 0 0;font-size:12px;line-height:1.7;color:#8ca3c7;">
                  <a href="{safe_preferences_url}" style="color:#93c5fd;text-decoration:none;">Manage preferences</a>
                  &nbsp;•&nbsp;
                  <a href="{safe_unsubscribe_url}" style="color:#93c5fd;text-decoration:none;">Unsubscribe</a>
                </p>
                <p style="margin:8px 0 0 0;font-size:11px;line-height:1.6;color:#7e95ba;">
                  Informational content only. Not investment advice.
                </p>
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
        f"{campaign.get('modeLabel', 'Quantura')}\n"
        f"{campaign.get('headline', '')}\n"
        f"{campaign.get('subheadline', '')}\n\n"
        f"{campaign.get('intro', '')}\n\n"
        + "\n".join(section_text_parts)
        + "\n"
        + f"\n{campaign.get('ctaLabel', 'Open Quantura')}: {campaign.get('ctaUrl', DEFAULT_SITE_ORIGIN)}\n"
        + f"Support: {support_email}\n"
        + f"Address: {mailing_address}\n"
        + f"Manage preferences: {preferences_url}\n"
        + f"Unsubscribe: {unsubscribe_url}\n"
        + "Informational content only. Not investment advice.\n"
    )

    return {
        "subject": str(campaign.get("subject") or "Quantura update")[:180],
        "html": html_body,
        "text": text_body[:20000],
    }


def init_ses_client() -> tuple[Any, dict[str, str]]:
    access_key = maybe_get_secret("AWS_ACCESS_KEY_ID")
    secret_key = maybe_get_secret("AWS_SECRET_ACCESS_KEY")
    region = maybe_get_secret("AWS_REGION")
    from_email = maybe_get_secret("SES_FROM_EMAIL")

    if not (access_key and secret_key and region and from_email):
        raise RuntimeError("Missing AWS SES credentials in Secret Manager.")

    config_set = maybe_get_secret("SES_CONFIG_SET")
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
    }


def _reserve_send_quota(db: Any, desired: int) -> dict[str, int]:
    desired_count = max(0, int(desired))
    now = _utc_now()
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


def _load_newsletter_subscribers(db: Any, *, max_candidates: int) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    query = db.collection("users").select(["email", "emailVerified", "isAnonymous", "emailPrefs"]).limit(max_candidates)
    for snap in query.stream():
        doc = snap.to_dict() or {}
        email = _normalize_email(doc.get("email"))
        if not email:
            continue
        if doc.get("isAnonymous") is True:
            continue
        if doc.get("emailVerified") is False:
            continue
        prefs = doc.get("emailPrefs") if isinstance(doc.get("emailPrefs"), dict) else {}
        if prefs.get("global") is False:
            continue
        if prefs.get("newsletter") is False:
            continue
        out.append({"uid": snap.id, "email": email})
        seen.add(email)
    remaining = max(0, max_candidates - len(out))
    if remaining:
        leads = db.collection("promo_subscribers").where("consent", "==", True).limit(remaining)
        for snap in leads.stream():
            doc = snap.to_dict() or {}
            email = _normalize_email(doc.get("email"))
            prefs = doc.get("emailPrefs") if isinstance(doc.get("emailPrefs"), dict) else {}
            if not email or email in seen or prefs.get("newsletter") is False:
                continue
            out.append({"uid": f"lead:{snap.id}", "email": email})
            seen.add(email)
    return out


def _normalize_manual_recipients(raw: Any) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        return []
    recipients: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in raw[:2000]:
        if isinstance(item, str):
            email = _normalize_email(item)
            uid = ""
        elif isinstance(item, dict):
            email = _normalize_email(item.get("email"))
            uid = str(item.get("uid") or "").strip()
        else:
            continue
        if not email or email in seen:
            continue
        seen.add(email)
        recipients.append({"uid": uid[:180], "email": email})
    return recipients


def _is_opted_out(db: Any, *, email: str, topic: str, cache: dict[str, bool]) -> bool:
    key = f"{email}:{topic}"
    if key in cache:
        return cache[key]
    email_hash = _email_hash(email)
    snap = db.collection("email_opt_outs").document(email_hash).get()
    if not snap.exists:
        cache[key] = False
        return False
    data = snap.to_dict() or {}
    topics = data.get("topics") if isinstance(data.get("topics"), dict) else {}
    opted = bool(topics.get(topic))
    cache[key] = opted
    return opted


def _set_opt_out(db: Any, *, email: str, topic: str, source: str) -> None:
    email_hash = _email_hash(email)
    db.collection("email_opt_outs").document(email_hash).set(
        {
            "email": email,
            "topics": {topic: True},
            "source": source,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )


def _create_unsubscribe_token(
    db: Any,
    *,
    uid: str,
    email: str,
    topic: str,
    mode: str,
) -> str:
    token = secrets.token_urlsafe(24)
    db.collection("email_unsubscribe_tokens").document(token).set(
        {
            "uid": uid,
            "email": email,
            "topic": topic,
            "mode": mode,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "usedAt": None,
        },
        merge=True,
    )
    return token


def _record_lead_profile(db: Any, *, uid: str, email: str) -> None:
    if not uid.startswith("lead:"):
        return
    db.collection("email_leads").document(uid).set(
        {
            "uid": uid,
            "email": email,
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )


def send_email_campaign_batch(
    db: Any,
    *,
    mode: str = "newsletter",
    max_to_send: int = DEFAULT_MAX_SEND,
    payload: dict[str, Any] | None = None,
    recipients: list[dict[str, str]] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    clean_mode = _normalize_mode(mode)
    cfg = MODE_CONFIGS[clean_mode]
    site_origin = str(os.environ.get("PUBLIC_SITE_ORIGIN") or DEFAULT_SITE_ORIGIN).rstrip("/") or DEFAULT_SITE_ORIGIN
    support_email = str(os.environ.get("NEWSLETTER_SUPPORT_EMAIL") or DEFAULT_SUPPORT_EMAIL).strip() or DEFAULT_SUPPORT_EMAIL
    mailing_address = str(os.environ.get("NEWSLETTER_MAILING_ADDRESS") or DEFAULT_MAILING_ADDRESS).strip()
    if not mailing_address:
        raise RuntimeError(
            "NEWSLETTER_MAILING_ADDRESS must be configured with an approved business mailing address before sending campaigns."
        )
    campaign_payload = payload if isinstance(payload, dict) else {}

    if recipients:
        target_recipients = _normalize_manual_recipients(recipients)
    elif clean_mode == "newsletter":
        max_candidates = max(200, min(5000, int(max_to_send) * 5))
        target_recipients = _load_newsletter_subscribers(db, max_candidates=max_candidates)
    else:
        target_recipients = []

    target_recipients = target_recipients[: max(0, int(max_to_send))]
    campaign = _build_campaign_copy(clean_mode, campaign_payload, site_origin=site_origin)

    preview_email = _render_campaign_email(
        campaign,
        site_origin=site_origin,
        unsubscribe_url=f"{site_origin}/email/unsubscribe?token=preview",
        preferences_url=f"{site_origin}/account",
        support_email=support_email,
        mailing_address=mailing_address,
    )

    if dry_run:
        return {
            "mode": clean_mode,
            "topic": cfg.topic,
            "status": "dry_run",
            "recipientCandidates": len(target_recipients),
            "subject": preview_email["subject"],
            "previewHtml": preview_email["html"],
            "previewText": preview_email["text"],
        }

    if not target_recipients:
        return {
            "mode": clean_mode,
            "topic": cfg.topic,
            "status": "no_recipients",
            "recipientCandidates": 0,
            "sent": 0,
            "failed": 0,
        }

    desired = min(max(0, int(max_to_send)), len(target_recipients), DAILY_SEND_CAP)
    reservation = _reserve_send_quota(db, desired)
    reserved = int(reservation.get("reserved") or 0)
    if reserved <= 0:
        return {
            "mode": clean_mode,
            "topic": cfg.topic,
            "status": "cap_reached",
            "recipientCandidates": len(target_recipients),
            "sent": 0,
            "failed": 0,
            "reserved": 0,
            "rolling24hCount": int(reservation.get("rolling24hCount") or 0),
            "remaining": int(reservation.get("remaining") or 0),
        }

    ses_client, ses_cfg = init_ses_client()

    sent = 0
    failed = 0
    skipped_opt_out = 0
    attempted = 0
    opt_out_cache: dict[str, bool] = {}

    for recipient in target_recipients[:reserved]:
        email = _normalize_email(recipient.get("email"))
        if not email:
            failed += 1
            continue
        uid = _normalize_recipient_uid(email, recipient.get("uid"))
        _record_lead_profile(db, uid=uid, email=email)

        if _is_opted_out(db, email=email, topic=cfg.topic, cache=opt_out_cache):
            skipped_opt_out += 1
            continue

        attempted += 1
        unsubscribe_token = _create_unsubscribe_token(
            db,
            uid=uid,
            email=email,
            topic=cfg.topic,
            mode=clean_mode,
        )
        unsubscribe_url = f"{site_origin}/email/unsubscribe?token={unsubscribe_token}"
        preferences_url = f"{site_origin}/account"

        rendered = _render_campaign_email(
            campaign,
            site_origin=site_origin,
            unsubscribe_url=unsubscribe_url,
            preferences_url=preferences_url,
            support_email=support_email,
            mailing_address=mailing_address,
        )

        send_status = "sent"
        send_error = ""
        message_id = ""
        try:
            send_kwargs: dict[str, Any] = {
                "FromEmailAddress": ses_cfg["from_email"],
                "Destination": {"ToAddresses": [email]},
                "Content": {
                    "Simple": {
                        "Subject": {"Data": rendered["subject"], "Charset": "UTF-8"},
                        "Body": {
                            "Html": {"Data": rendered["html"], "Charset": "UTF-8"},
                            "Text": {"Data": rendered["text"], "Charset": "UTF-8"},
                        },
                        "Headers": [
                            {"Name": "List-Unsubscribe", "Value": f"<{unsubscribe_url}>"},
                            {"Name": "List-Unsubscribe-Post", "Value": "List-Unsubscribe=One-Click"},
                        ],
                    }
                },
                "EmailTags": [
                    {"Name": "topic", "Value": cfg.topic},
                    {"Name": "mode", "Value": clean_mode},
                    {"Name": "uid", "Value": uid[:120]},
                ],
            }
            if ses_cfg.get("config_set"):
                send_kwargs["ConfigurationSetName"] = ses_cfg["config_set"]

            response = ses_client.send_email(**send_kwargs)
            message_id = str(response.get("MessageId") or "")
            sent += 1
        except Exception as exc:
            send_status = "failed"
            send_error = str(exc)[:500]
            failed += 1

        db.collection("email_logs").document().set(
            {
                "uid": uid,
                "email": email,
                "topic": cfg.topic,
                "mode": clean_mode,
                "subject": rendered["subject"],
                "status": send_status,
                "error": send_error,
                "messageId": message_id,
                "ts": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )

    return {
        "mode": clean_mode,
        "topic": cfg.topic,
        "status": "ok",
        "recipientCandidates": len(target_recipients),
        "reserved": reserved,
        "attempted": attempted,
        "sent": sent,
        "failed": failed,
        "skippedOptOut": skipped_opt_out,
        "rolling24hCount": int(reservation.get("rolling24hCount") or 0),
        "remaining": int(reservation.get("remaining") or 0),
        "subject": campaign.get("subject"),
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
    email = _normalize_email(token_doc.get("email"))
    topic = str(token_doc.get("topic") or "newsletter").strip() or "newsletter"

    if not email:
        return {"ok": False, "error": "Token does not contain an email."}

    if uid.startswith("lead:") or not uid:
        _set_opt_out(db, email=email, topic=topic, source="unsubscribe_token")
    else:
        pref_path = f"emailPrefs.{topic}"
        db.collection("users").document(uid).set(
            {
                pref_path: False,
                "emailPrefs.updatedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        _set_opt_out(db, email=email, topic=topic, source="unsubscribe_token_user")

    token_ref.set({"usedAt": firestore.SERVER_TIMESTAMP}, merge=True)
    return {"ok": True, "uid": uid, "topic": topic, "email": email}


def render_unsubscribe_html(result: dict[str, Any]) -> str:
    ok = bool(result.get("ok"))
    title = "You are unsubscribed" if ok else "Unsubscribe failed"
    message = (
        "You will no longer receive this type of Quantura email at your address."
        if ok
        else str(result.get("error") or "The unsubscribe token is invalid or expired.")
    )
    support_email = str(os.environ.get("NEWSLETTER_SUPPORT_EMAIL") or DEFAULT_SUPPORT_EMAIL).strip() or DEFAULT_SUPPORT_EMAIL
    return f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>{html.escape(title)}</title>
    <style>
      body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:#070c16; color:#e5edf8; }}
      .wrap {{ min-height:100vh; display:grid; place-items:center; padding:24px; }}
      .card {{ width:min(560px,100%); border:1px solid rgba(255,255,255,0.18); border-radius:16px; padding:24px; background:#111b2d; }}
      h1 {{ margin:0 0 12px; font-size:1.35rem; line-height:1.3; }}
      p {{ margin:0; line-height:1.7; color:#c8d6eb; }}
      a {{ color:#93c5fd; text-decoration:none; }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <h1>{html.escape(title)}</h1>
        <p>{html.escape(message)}</p>
        <p style="margin-top:14px;">
          <a href="{DEFAULT_SITE_ORIGIN}">Return to Quantura</a>
          &nbsp;•&nbsp;
          <a href="mailto:{html.escape(support_email)}">Contact support</a>
        </p>
      </div>
    </div>
  </body>
</html>
""".strip()
