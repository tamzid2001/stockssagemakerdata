# Quantura Platform

Quantura is a Firebase-hosted market intelligence web app with:
- Multi-page SSR-rendered marketing + app routes
- Authenticated dashboard workflows
- Forecasting, screener, options/news/technicals, uploads, backtesting
- Admin fulfillment operations
- Remote Config + Analytics + Messaging integrations
- Python Cloud Functions for data and workflow automation

This README documents architecture, setup, deployment, and operations.

## 1) Architecture

## Frontend
- Source pages: `quantura_site/pages/*.html`
- Client logic: `quantura_site/public/app.js`
- Shared styling: `quantura_site/public/styles.css`
- Static assets + SEO files: `quantura_site/public/*`

## SSR layer (Firebase Functions, Node)
- Entry: `quantura_site/functions_ssr/index.js`
- Template sync script: `quantura_site/functions_ssr/scripts/sync-templates.js`
- Templates target: `quantura_site/functions_ssr/templates/`

## Backend (Firebase Functions, Python)
- Entry: `quantura_site/functions/main.py`
- Requirements: `quantura_site/functions/requirements.txt`

## Firebase resources
- Firestore rules/indexes: `quantura_site/firestore.rules`, `quantura_site/firestore.indexes.json`
- Storage rules: `quantura_site/storage.rules`
- Hosting + rewrites: `quantura_site/firebase.json`

## CI/CD
- Workflows: `.github/workflows/*.yml`

---

## 2) Core Product Features

## Authentication + session
- Firebase Email/Password + Google sign-in
- Persistent auth state across routes
- Admin account gate for `tamzid257@gmail.com`

## Forecasting
- Meta Prophet and IBM TimeMixer options
- Quantile forecasting input and saved runs
- Plotly chart visualization and replay from saved runs

## Screener
- Run, save, rename, delete screener runs
- Load historical runs from workspace

## Uploads / predictions.csv
- Upload CSV with ticker metadata
- Plot uploaded CSV interactively
- Pagination controls in preview (`25/50/100/250/500` rows)
- CRUD actions: rename, delete, download, plot
- OpenAI CSV Agent (`run_prediction_upload_agent`) for analyst-style commentary

## Dashboard productivity
- Watchlist, price alerts, collaboration invites, tasks
- Autopilot request queue + CRUD
- Notifications controls and token registration flow

## Pricing + purchase
- Monthly pricing plans in `/pricing`
- Deep Forecast request integrated into pricing page
- `/purchase` redirected to `/pricing`

## Admin fulfillment
- Order status updates
- Upload fulfillment files and notes
- Admin-only data visibility

---

## 3) Routes

## Public
- `/`
- `/forecasting`
- `/screener`
- `/pricing`
- `/contact`
- `/blog`
- `/blog/posts/:slug`
- `/terms`
- `/privacy`

## Auth-gated app
- `/dashboard`
- `/account`
- `/watchlist`
- `/productivity`
- `/collaboration`
- `/uploads`
- `/autopilot`
- `/notifications`

## Admin
- `/admin`

---

## 4) Remote Config Parameters

Client and SSR use Remote Config with fallbacks. Current keys in use:
- `welcome_message` (string)
- `watchlist_enabled` (bool-ish)
- `forecast_prophet_enabled` (bool-ish)
- `forecast_timemixer_enabled` (bool-ish)
- `push_notifications_enabled` (bool-ish)
- `webpush_vapid_key` (string)
- `stripe_checkout_enabled` (bool-ish)
- `stripe_public_key` (string)
- `holiday_promo` (bool-ish)
- `backtesting_enabled` (bool-ish)
- `backtesting_free_daily_limit` (number)
- `backtesting_pro_daily_limit` (number)

Server evaluation happens in:
- SSR: `quantura_site/functions_ssr/index.js`
- Python functions: `quantura_site/functions/main.py`

---

## 5) Environment Variables

Use local `.env` files for local development only. Never commit secrets.

## Root `.env` (automation/scripts)
See `.env.example` for full template.

## Functions `.env`
Typical keys:
- Firebase/service: `SERVICE_ACCOUNT_PATH`, `STORAGE_BUCKET`, `PUBLIC_ORIGIN`
- OpenAI: `OPENAI_API_KEY`, `SOCIAL_CONTENT_MODEL`, `PREDICTION_AGENT_MODEL`
- Stripe: `STRIPE_PUBLIC_KEY`, `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`
- Messaging: `FCM_WEB_VAPID_KEY`
- Alpaca: `ALPACA_API_KEY`/`ALPACAAPIKEY`, `ALPACA_SECRET_KEY`/`ALPACASECRETKEY`
- TimeMixer/HF: `IBM_TIMEMIXER_*`, `HUGGINGFACEHUB_API_TOKEN`
- Slack/social: `SLACK_WEBHOOK_URL`, `SOCIAL_WEBHOOK_*`

---

## 6) Local Development

From repo root:
```bash
cd quantura_site
npm ci
```

If you use Python tests:
```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata
python3 -m venv .venv-test
source .venv-test/bin/activate
pip install -r requirements.txt pytest
pytest -q quantura_site/tests/test_public_assets.py
```

Optional frontend sanity checks:
```bash
node --check quantura_site/public/app.js
node --check quantura_site/functions_ssr/index.js
```

---

## 7) Deploy

Deploy from `quantura_site`:
```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site
firebase deploy
```

Or scoped deploys:
```bash
firebase deploy --only hosting
firebase deploy --only functions
firebase deploy --only functions:ssr
firebase deploy --only firestore:rules,firestore:indexes,storage
```

---

## 8) CI/CD Workflows

Key workflows:
- `firebase-hosting-merge.yml`: Hosting deploy on merge
- `firebase-hosting-pull-request.yml`: Preview deploy on PR
- `daily-quantura-tests.yml`: static/tests
- `mlops-data.yml`: market data + optional S3 upload
- `autopilot_timeseries.yml`: scheduled Autopilot job

Recent hardening:
- `mlops-data.yml` now skips AWS upload steps when AWS secrets are missing.
- `autopilot_timeseries.yml` now skips run cleanly when required secrets are missing.
- `scripts/run_autopilot_timeseries.py` fixed import path + `args.s3_prefix` bug.

Required repo secrets for AWS jobs:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AUTOPILOT_ROLE_ARN`
- `AUTOPILOT_S3_BUCKET`
- `AUTOPILOT_TICKERS`

---

## 9) Social Automation

Backend functions:
- `generate_social_campaign_drafts`
- `queue_social_campaign_posts`
- `list_social_campaigns`
- `list_social_queue`
- `publish_social_queue_now`
- `social_dispatch_scheduler`

Setup guide:
- `quantura_site/docs/social_media_setup.md`

---

## 10) Legal + Compliance Text

Footer now includes:
- Terms and Conditions link (`/terms`)
- Privacy Policy link (`/privacy`)
- Financial disclaimer

---

## 11) Troubleshooting

## "I uploaded CSV but cannot plot"
- Confirm upload owner matches signed-in user
- Verify `get_prediction_upload_csv` callable is deployed
- Check Storage path exists in upload doc

## "Agent button shows fallback"
- `OPENAI_API_KEY` is missing or unreachable
- Fallback deterministic summary is expected in this case

## "Auth button still says Sign in"
- Clear stale cache and hard reload
- Confirm latest `public/app.js` is deployed

## "Pagination not visible"
- Ensure latest `public/app.js` + `public/styles.css` are deployed
- Preview panel must have loaded CSV rows first

---

## 12) Release

First tagged release:
- Tag: `v1.0.0`
- Release: <https://github.com/tamzid2001/stockssagemakerdata/releases/tag/v1.0.0>
