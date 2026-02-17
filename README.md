# Quantura Monorepo

<p align="center">
  <strong>Execution-grade market intelligence platform + data automation toolkit</strong><br/>
  Forecasting, screening, portfolio workflows, social intelligence, and MLOps pipelines in one repository.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/stack-Firebase%20%7C%20Python%20%7C%20Node%20%7C%20JS-0f172a" alt="Stack" />
  <img src="https://img.shields.io/badge/domain-FinTech%20%26%20Market%20Intelligence-0ea5e9" alt="Domain" />
  <img src="https://img.shields.io/badge/deploy-Firebase%20Hosting%20%2B%20Cloud%20Functions-16a34a" alt="Deploy" />
</p>

---

## Table of Contents

- [1. Project Overview](#1-project-overview)
- [2. Problem Statement](#2-problem-statement)
- [3. Core Use Cases](#3-core-use-cases)
- [4. System Architecture](#4-system-architecture)
- [5. Repository Structure](#5-repository-structure)
- [6. Frontend Module Map](#6-frontend-module-map)
- [7. Backend Function Map](#7-backend-function-map)
- [8. Data Model (Firestore)](#8-data-model-firestore)
- [9. Configuration](#9-configuration)
- [10. Local Development](#10-local-development)
- [11. Deployments](#11-deployments)
- [12. Testing and Validation](#12-testing-and-validation)
- [13. Security and Public-Ready Checklist](#13-security-and-public-ready-checklist)
- [14. Roadmap](#14-roadmap)

---

## 1. Project Overview

Quantura is a full-stack market intelligence system with:

- A Firebase-hosted web platform (`quantura_site`) for forecasting, screening, backtesting, alerts, and collaboration
- Python Cloud Functions for market analytics, AI-assisted workflows, billing, social automation, and data APIs
- SSR layer for SEO-friendly pages and Remote Config hydration
- Supporting scripts for stock data ingestion, S3 workflows, weekday-aware prediction utilities, and scheduled automation
- iOS/Android app folders for mobile clients (`quantura_ios`, `quantura_android`)

This monorepo is designed to support both:

1. Productized SaaS workflows (dashboard, subscriptions, sharing, automation)
2. Operator workflows (data exports, quant research loops, scheduled jobs)

---

## 2. Problem Statement

Most retail and semi-professional investing workflows are fragmented:

- Charting in one tab
- News and social sentiment in others
- Forecast assumptions tracked manually
- Backtest artifacts disconnected from live workflows

Quantura consolidates these into one execution surface, so users can:

- Move from thesis to forecast to backtest faster
- Track portfolio/watchlist signals in context
- Persist and share outputs across teams/workspaces
- Control model access, billing, and feature rollout centrally

---

## 3. Core Use Cases

### A. Forecasting Desk
- Input ticker + interval + horizon
- Generate quantile forecasts
- Save runs and regenerate report assets (PDF/PPT style outputs)

### B. AI Portfolio Screener
- Apply descriptive/fundamental/technical filters
- Run ranked candidate generation
- Publish or keep private AI agent portfolios

### C. Backtesting + Technical Guidance
- Run rule-based strategy backtests
- Export backtest source templates (Python/TradingView/MT5/JSON)
- Generate technical setup suggestions from recent indicators

### D. Social and News Intelligence
- Pull market headlines by country
- Integrate social pulse feeds (X/Reddit/Facebook/Instagram)
- Generate and queue social campaign drafts

### E. Collaboration + Monetization
- Workspace collaborators/invites
- Stripe checkout, billing portal, creator support/subscriptions
- Share links for forecasts/screeners/uploads

---

## 4. System Architecture

```mermaid
flowchart LR
    U["User Browser"] --> H["Firebase Hosting (SPA + static assets)"]
    H --> SSR["SSR Function (Node.js)"]
    H --> JS["public/app.js"]

    JS --> PY["Cloud Functions (Python)"]
    JS --> FS["Firestore"]
    JS --> ST["Cloud Storage"]

    PY --> FS
    PY --> ST
    PY --> RC["Firebase Remote Config"]
    PY --> Stripe["Stripe APIs"]
    PY --> YF["Yahoo Finance / yfinance"]
    PY --> MX["Massive (US corporate events)"]
    PY --> OAI["OpenAI APIs"]
    PY --> NOVA["Amazon Nova Gateway (optional)"]
    PY --> SM["Social Platform APIs / Webhooks"]

    GH["GitHub Actions"] --> H
    GH --> PY
    GH --> Data["Data Jobs / Artifacts"]
```

### Runtime Sequence (Screener Path)

```mermaid
sequenceDiagram
    participant UI as Forecasting UI
    participant Fn as run_quick_screener
    participant RC as Remote Config
    participant Mkt as yfinance/Yahoo
    participant AI as OpenAI or Amazon Nova
    participant DB as Firestore

    UI->>Fn: submit screener payload (filters, model, notes)
    Fn->>RC: resolve tier + llm_allowed_models
    Fn->>Mkt: fetch histories/info
    Fn->>AI: optional note->ticker/query enrichment
    Fn->>DB: persist screener run + metadata
    Fn-->>UI: ranked results + runId + modelUsed
```

---

## 5. Repository Structure

```text
stockssagemakerdata/
├── quantura_site/
│   ├── pages/                 # HTML pages used by Hosting + SSR template sync
│   ├── public/                # app.js, styles.css, assets, blog output
│   ├── functions/             # Python Cloud Functions backend
│   ├── functions_ssr/         # Node SSR function and template hydration
│   ├── firestore.rules
│   ├── storage.rules
│   └── firebase.json
├── quantura_android/          # Android client workspace
├── quantura_ios/              # iOS client workspace
├── scripts/                   # automation/helpers
├── fetch_data.py              # local stock fetch utility
├── fetch_data_s3.py           # fetch + S3 upload utility
├── predictions.py             # prediction CSV helper
├── combined_stock_screener.py # standalone screener CSV + Slack tool
├── Makefile
└── .github/workflows/         # CI/CD + scheduled jobs
```

---

## 6. Frontend Module Map

Primary client entry: `quantura_site/public/app.js`

| Module Area | Key Responsibilities |
|---|---|
| Auth + Session | Firebase Auth, guest/session states, protected flow guards |
| Remote Config | SSR hydration + live refresh, feature toggles, tier policy |
| Forecasting UI | Chart controls, saved forecasts, overlays, ticker sync |
| Screener UI | Filter groups, model/personality select, run/load/publish |
| Backtesting UI | Strategy forms, export helpers, technical suggestion flow |
| Alerts + Notifications | FCM web token registration and test notification path |
| Collaboration | Workspace switching, collaborator invitations, shared links |
| Leaderboard + Social | AI portfolio cards, follows/likes, profile identity/social links |
| Billing UX | Stripe checkout + billing portal actions |

---

## 7. Backend Function Map

Primary backend: `quantura_site/functions/main.py`

### Forecasting
- `run_timeseries_forecast`
- `run_prophet_forecast`
- `delete_forecast_request`
- `generate_forecast_report_assets`
- `forecast_report_agent_scheduler` (scheduled)

### Screening + AI Portfolio
- `queue_screener_run`
- `run_quick_screener`
- `rename_screener_run`
- `delete_screener_run`
- `set_screener_public_visibility`
- `upsert_ai_agent_social_action`

### Backtesting
- `run_backtest`
- `rename_backtest`
- `delete_backtest`

### Ticker Intelligence and Market Data
- `get_ticker_history`
- `download_price_csv`
- `get_trending_tickers`
- `get_ticker_intel`
- `get_ticker_news`
- `get_ticker_x_trends`
- `get_corporate_events_calendar`
- `query_ticker_insight`
- `get_market_headlines_feed`
- `get_options_chain`
- `get_technicals`

### Uploads and Admin
- `run_prediction_upload_agent`
- `rename_prediction_upload`
- `delete_prediction_upload`
- `get_prediction_upload_csv`

### Collaboration + Sharing
- `create_collab_invite`
- `list_collab_invites`
- `accept_collab_invite`
- `revoke_collab_invite`
- `list_collaborators`
- `remove_collaborator`
- `create_share_link`
- `import_shared_item`

### Billing + Stripe
- `create_order`
- `create_stripe_checkout_session`
- `confirm_stripe_checkout`
- `create_stripe_billing_portal_session`
- `create_stripe_connect_onboarding_link`
- `create_creator_support_checkout`
- `update_order_status`

### Notifications + Feature Control
- `get_feature_flags`
- `get_web_push_config`
- `register_notification_token`
- `unregister_notification_token`
- `send_test_notification`
- `check_price_alerts`

### Product Feedback + Voting
- `submit_feedback`
- `submit_feature_vote`
- `get_feature_vote_summary`

### Social Automation
- `generate_social_campaign_drafts`
- `queue_social_campaign_posts`
- `list_social_campaigns`
- `list_social_queue`
- `publish_social_queue_now`
- `schedule_social_autopilot_now`
- `social_dispatch_scheduler` (scheduled)
- `social_daily_planner_scheduler` (scheduled)

### Broker/Execution Integrations
- `alpaca_get_options`
- `alpaca_place_order`
- `alpaca_get_account`
- `alpaca_get_positions`
- `alpaca_list_orders`
- `alpaca_cancel_order`

---

## 8. Data Model (Firestore)

Representative collections used by the app:

- `users/{uid}`
  - profile, plan state, metadata
- `forecast_requests/{id}`
- `screener_runs/{id}`
- `backtests/{id}`
- `prediction_uploads/{id}`
- `orders/{id}`
- `notification_tokens/{tokenHash}`
- `feature_votes/{feature_user}`
- `feature_vote_totals/{feature}`
- `share_links/{shareId}`
- `social_campaigns/{id}` and queue subcollections

> Exact access control is enforced by `quantura_site/firestore.rules`.

---

## 9. Configuration

### Remote Config (important keys)

- `watchlist_enabled` (boolean)
- `forecast_prophet_enabled` (boolean)
- `forecast_timemixer_enabled` (boolean)
- `enable_social_leaderboard` (boolean)
- `maintenance_mode` (boolean)
- `promo_banner_text` (string)
- `volatility_threshold` (number)
- `ai_usage_tiers` (JSON string)
- `llm_allowed_models` (CSV/JSON string)  **new server-side model gate**
- `push_notifications_enabled` (boolean)
- `webpush_vapid_key` (string)
- `stripe_checkout_enabled` (boolean)
- `stripe_public_key` (string)
- `backtesting_enabled` (boolean)
- `backtesting_free_daily_limit` (number)
- `backtesting_pro_daily_limit` (number)

### Environment Variables

Use `.env.example` as baseline. Critical categories:

- AI providers:
  - `OPENAI_API_KEY`
  - `AMAZON_NOVA_API_KEY` (optional)
  - `AMAZON_NOVA_API_ENDPOINT` (optional)
  - `AMAZON_NOVA_DEFAULT_MODEL` (optional)
  - `LLM_ALLOWED_MODELS` (fallback allowlist if Remote Config value is absent)
- Billing:
  - Stripe keys and webhook secret
- Messaging:
  - `FCM_WEB_VAPID_KEY`
- Data providers:
  - Massive, Alpaca, yfinance dependencies
- Social APIs/webhooks:
  - X/LinkedIn/Facebook/Instagram/etc

---

## 10. Local Development

### Prerequisites
- Node 18+
- Python 3.10+
- Firebase CLI

### Setup

```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cd /Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site
npm ci
```

### Quick validation

```bash
node --check /Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site/public/app.js
node --check /Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site/functions_ssr/index.js
python3 -m py_compile /Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site/functions/main.py
```

---

## 11. Deployments

Deploy from `quantura_site`:

```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site
firebase deploy
```

Scoped deploys:

```bash
firebase deploy --only hosting
firebase deploy --only functions
firebase deploy --only functions:ssr
firebase deploy --only firestore:rules,firestore:indexes,storage
```

---

## 12. Testing and Validation

- Static checks:
  - JS syntax: `node --check`
  - Python compile checks: `py_compile`
- Targeted tests:
  - `quantura_site/tests/test_public_assets.py`
- Runtime validation:
  - Forecasting run
  - Screener run with tier/model policy
  - Backtest run + export
  - Stripe checkout + billing portal
  - Notification token registration + test push

---

## 13. Security and Public-Ready Checklist

Before making the repo public:

1. Remove any committed secrets and rotate exposed keys.
2. Keep API keys in secret managers / environment variables only.
3. Verify `.env`, service accounts, and token files are gitignored.
4. Keep these files local-only (never committed):
   - `quantura_site/functions/serviceAccountKey.json`
   - `quantura_ios/quantura_ios/GoogleService-Info.plist`
   - `quantura_android/app/google-services.json`
5. Bootstrap local placeholders with:
   - `./scripts/setup_local_firebase_credentials.sh`
6. If key exposure occurred, revoke/rotate immediately and purge Git history for leaked files.
7. Re-check Firestore and Storage rules for least privilege.
8. Confirm admin-only features are enforced server-side (not UI-only).
9. Validate Stripe webhook signature handling and callback allowlists.
10. Verify social integrations fail safely when credentials are missing.
11. Keep `llm_allowed_models` constrained to approved models for cost/control.

### Immediate GCP/Firebase leak response

1. Disable and recreate exposed API keys in Google Cloud Console.
2. Restrict replacement API keys:
   - iOS key: restrict by bundle ID + only required APIs.
   - Android key: restrict by package name + SHA-1 + only required APIs.
   - Web key: restrict by HTTP referrers + only required APIs.
3. Disable compromised service-account keys and create new credentials only if required.
4. Prefer Workload Identity / Application Default Credentials over downloadable key files.
5. Purge leaked files from all Git history and force-push rewritten history.
6. Invalidate any copied secrets from CI/CD systems and re-seed them as repository/environment secrets.

---

## 14. Roadmap

- Expand model orchestration policy with per-feature model routing
- Add deeper explainability traces on screener ranking decisions
- Add benchmark backtest suites and reproducibility snapshots
- Extend international market coverage for events and social intelligence
- Harden observability dashboards for function memory/latency/error budgets

---

### Maintainer Note

If you are onboarding as a new contributor, start with:

1. `quantura_site/public/app.js` (client orchestration)
2. `quantura_site/functions/main.py` (business logic and integrations)
3. `quantura_site/functions_ssr/index.js` (SSR and Remote Config hydration)
4. `quantura_site/firebase.json` + rules files (deployment and access control)
