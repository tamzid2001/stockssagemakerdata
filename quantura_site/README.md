# Quantura Website

Flask website for the Quantura weekly investing newsletter and Demand Forecasting™ upsell.

## Run locally

```bash
cd quantura_site
python3 -m venv .venv
source .venv/bin/activate
pip install flask
python app.py
```

Open http://127.0.0.1:5000

## Social Automation backend

Quantura now includes Firebase Functions for low-cost social drafting and multi-channel posting automation.

- Draft generator: `generate_social_campaign_drafts`
- Queue campaign posts: `queue_social_campaign_posts`
- List campaigns: `list_social_campaigns`
- List queue: `list_social_queue`
- Manual dispatch (admin): `publish_social_queue_now`
- Hourly scheduler: `social_dispatch_scheduler` (Cloud Scheduler via Firebase scheduled function)

Environment variables are documented in `/Users/tamzidullah/Desktop/stockssagemakerdata/.env.example` under **Social media automation**.

Full setup and channel sign-up checklist:
- `/Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site/docs/social_media_setup.md`
