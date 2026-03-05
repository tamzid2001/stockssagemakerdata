# Newsletter + Campaign Emailing

This service sends:

- weekly newsletters to signed-in users with a valid email (unless opted out),
- cold outreach emails,
- enterprise pitch emails.

## Functions

- `send_newsletter_daily_http` (HTTP)
  - route: `/api/email/send-newsletter-daily`
  - aliases: `/api/email/send-campaign`, `/api/email/send-newsletter-weekly`
- `send_newsletter_weekly_scheduler` (Pub/Sub trigger topic: `quantura-newsletter-weekly`)
- `email_unsubscribe_http` (HTTP)
  - route: `/email/unsubscribe?token=...`

## Manual send payload

```json
{
  "mode": "newsletter",
  "dryRun": true,
  "maxToSend": 500,
  "campaign": {
    "title": "Weekly market workflow update",
    "summary": "Macro + execution context for the week.",
    "highlights": [
      "Watchlist momentum shifts",
      "Forecast scenario updates",
      "Model Council escalation queue"
    ],
    "ctaUrl": "https://quantura.studio/forecasting"
  }
}
```

For cold emails or pitches, pass explicit recipients:

```json
{
  "mode": "pitch",
  "maxToSend": 100,
  "recipients": [
    { "email": "team@fund.com" },
    { "email": "ops@desk.com", "uid": "lead_123" }
  ],
  "campaign": {
    "title": "Institutional workflow pilot",
    "summary": "We can compress signal-to-decision cycles for your desk.",
    "ctaUrl": "https://quantura.studio/pricing"
  }
}
```

## Auth for manual sends

`send_newsletter_daily_http` requires admin auth:

- `X-Newsletter-Admin-Key: <NEWSLETTER_ADMIN_KEY>` header, or
- Firebase ID token with `admin=true` claim or an admin email.

## Unsubscribe compliance

Every sent email includes:

- `List-Unsubscribe` and `List-Unsubscribe-Post` headers,
- footer links for preferences + unsubscribe,
- support contact and physical mailing address.

Unsubscribe updates:

- user `emailPrefs.<topic>=false` (known users), and
- `email_opt_outs` (lead/outreach recipients).
