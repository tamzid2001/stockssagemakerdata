# Quantura Social Media Automation Setup

This guide covers:
- how to create high-conversion social pages,
- how to connect API credentials later,
- how to use the new Firebase backend automation for low-cost posting.

## 1) Create motivated social pages (brand + positioning)

Use this identity on every channel:
- Brand name: `Quantura`
- Tagline: `Data Analytics + ML/AI for active market decisions`
- Primary CTA: `https://quantura-e2e3d.web.app`
- Proof points: forecasting bands, indicator overlays, saved workspaces, collaboration.

Profile checklist (all channels):
- Handle: reserve `@quantura` or nearest available variant.
- Logo: upload your transparent Quantura logo.
- Banner: value proposition + CTA URL.
- Bio formula: `Who you help + what outcome + social proof + CTA`.
- Link-in-bio: home page + pricing + forecasting page.
- Pinned post: short intro + 3 key outcomes + CTA.

## 2) Channels to open first

Prioritize this order:
1. LinkedIn Company Page (B2B trust + organic reach for analytics/AI buyers)
2. X (market commentary and fast distribution)
3. YouTube (short explainers and demos)
4. Reddit (community feedback and intent traffic)
5. Facebook + Instagram + Threads + TikTok + Pinterest

## 3) Content pillars (for conversion, not vanity)

Publish around 4 pillars:
- `Market Context`: what moved, why it matters, what to watch next.
- `Product Value`: forecast examples, saved run workflows, collaboration use cases.
- `Education`: indicators, quantiles, risk framing, repeatable research process.
- `Proof`: user outcomes, before/after workflows, benchmark snapshots.

Recommended cadence:
- Daily short post: 1-2 platform-native messages.
- Weekly anchor post: one deeper insight thread/article/video.
- Weekly recap: wins, lessons, next week watchlist.

## 4) Backend automation now available

Implemented in:
- `/Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site/functions/main.py`

Functions:
- `generate_social_campaign_drafts`
- `queue_social_campaign_posts`
- `list_social_campaigns`
- `list_social_queue`
- `publish_social_queue_now` (admin)
- `social_dispatch_scheduler` (hourly)
- `schedule_social_autopilot_now` (admin, manual trigger)
- `social_daily_planner_scheduler` (daily campaign planner)

Model:
- Default low-cost model: `gpt-5-mini` via `SOCIAL_CONTENT_MODEL`.
- If `OPENAI_API_KEY` is missing, template fallback is used so automation still works.
- GPT-5 generation uses OpenAI Responses API with channel-by-channel drafting to avoid truncation.

Tier model access used by Quantura AI screeners:
- `free`: `gpt-5-nano`, `gpt-5-mini` (3 weekly runs)
- `pro`: `gpt-5-mini`, `gpt-5`, `gpt-5.1` (25 weekly runs)
- `desk`: `gpt-5-nano`, `gpt-5-mini`, `gpt-5`, `gpt-5.1`, `gpt-5.2` (75 weekly runs)

## 5) Environment variables

Add these in your Functions environment (or `.env` for local emulator):
- `OPENAI_API_KEY`
- `SOCIAL_AUTOMATION_ENABLED=true`
- `SOCIAL_CONTENT_MODEL=gpt-5-mini`
- `SOCIAL_AUTOMATION_TIMEZONE=America/New_York`
- `SOCIAL_DISPATCH_BATCH_SIZE=30`
- `SOCIAL_DEFAULT_CTA_URL=https://quantura-e2e3d.web.app`
- `SOCIAL_WEBHOOK_X`
- `SOCIAL_WEBHOOK_LINKEDIN`
- `SOCIAL_WEBHOOK_FACEBOOK`
- `SOCIAL_WEBHOOK_INSTAGRAM`
- `SOCIAL_WEBHOOK_THREADS`
- `SOCIAL_WEBHOOK_REDDIT`
- `SOCIAL_WEBHOOK_TIKTOK`
- `SOCIAL_WEBHOOK_YOUTUBE`
- `SOCIAL_WEBHOOK_PINTEREST`
- `SOCIAL_POSTING_TIMEZONE=America/New_York`
- `SOCIAL_AUTOPILOT_ENABLED=true`
- `SOCIAL_AUTOPILOT_CHANNELS=x,linkedin,facebook,instagram,tiktok`
- `SOCIAL_AUTOPILOT_POSTS_PER_CHANNEL=3`
- `SOCIAL_AUTOPILOT_USER_ID=quantura_system`
- `SOCIAL_AUTOPILOT_USER_EMAIL=system@quantura.ai`
- `SOCIAL_AUTOPILOT_TOPIC=Daily Quantura market pulse: top catalysts, risk posture, and setup watchlist`
- `SOCIAL_AUTOPILOT_OBJECTIVE=Drive qualified users to Quantura forecasting workflows`
- `SOCIAL_AUTOPILOT_AUDIENCE=active investors, analysts, and portfolio operators`
- `SOCIAL_AUTOPILOT_TONE=institutional, concise, actionable`

Direct provider posting credentials (optional alternative to webhooks):
- `TWITTER_BEARER_TOKEN` (read/search) plus user-write credentials `TWITTER_API_KEY` + `TWITTER_API_SECRET` + `TWITTER_ACCESS_TOKEN` + `TWITTER_ACCESS_TOKEN_SECRET` for X posting
- `LINKEDIN_ACCESS_TOKEN` + `LINKEDIN_AUTHOR_URN`
- `FACEBOOK_PAGE_ID` + `FACEBOOK_PAGE_ACCESS_TOKEN`
- `INSTAGRAM_BUSINESS_ACCOUNT_ID` + `INSTAGRAM_ACCESS_TOKEN` + `INSTAGRAM_DEFAULT_IMAGE_URL`
- `TIKTOK_ACCESS_TOKEN` + `TIKTOK_OPEN_ID` (or webhook integration)

Current adapter pattern:
- tries direct official API integration first for X/LinkedIn/Facebook/Instagram/TikTok.
- falls back to per-channel webhook when direct credentials are unavailable.

## 6) Example automation flow (client to backend)

1. Generate drafts:

```js
const gen = functions.httpsCallable("generate_social_campaign_drafts");
const draftRes = await gen({
  title: "Quantura weekly market pulse",
  topic: "How uncertainty bands improve position sizing",
  objective: "Drive qualified signups",
  audience: "Active investors and fintech operators",
  tone: "confident, concise, practical",
  channels: ["x", "linkedin", "youtube", "reddit"],
  postsPerChannel: 2,
  ctaUrl: "https://quantura-e2e3d.web.app/pricing",
  saveDraft: true
});
```

2. Queue posts:

```js
const queue = functions.httpsCallable("queue_social_campaign_posts");
await queue({
  campaignId: draftRes.data.campaignId,
  scheduledFor: "2026-02-16T14:00:00Z"
});
```

3. Dispatch:
- Automatic: hourly via `social_dispatch_scheduler`.
- Manual (admin): call `publish_social_queue_now`.

## 6.1) CLI smoke test and strategic queue

Run this locally from repo root:

```bash
quantura_site/functions/venv/bin/python scripts/social_poster_runner.py --send-now --queue-strategic
```

- `--send-now` attempts immediate posting once per selected channel.
- `--queue-strategic` saves queue rows with dayparted `suggestedPostTime`.
- `--schedule-autopilot` triggers the daily planner path manually.

Health probe (reads/fallback checks across X, Reddit, Facebook, Instagram, LinkedIn, TikTok):

```bash
python scripts/social_channel_probe.py --query "US stock market top headlines today"
```

## 6.2) GitHub automations

Use these workflows:
- `.github/workflows/social-media-automation.yml` for recurring social drafting/posting automation.
- `.github/workflows/weekly-blog-post.yml` for weekly blog generation and auto-commit.

`social-media-automation.yml` supports two modes:
- health probe only (no Firebase service account secret)
- full queue + dispatch mode when `FIREBASE_SERVICE_ACCOUNT_JSON` is configured.

The workflow also accepts `FIREBASE_SERVICE_ACCOUNT_QUANTURA_E2E3D` as a fallback secret name.
To ensure immediate feed activity each run, it performs:
- direct `--send-now` publish attempts,
- strategic queueing,
- `--publish-now-first` so at least one queued post per routable channel dispatches immediately.

### One-command secret sync (local -> GitHub Actions)

Use:

```bash
python scripts/sync_github_social_secrets.py
```

Optional (only selected keys):

```bash
python scripts/sync_github_social_secrets.py --only OPENAI_API_KEY,TWITTER_API_KEY,TWITTER_API_SECRET,TWITTER_ACCESS_TOKEN,TWITTER_ACCESS_TOKEN_SECRET
```

## 7) Launch plan for first 14 days

Day 1-2:
- Create pages, complete bios, pin intro post, add CTA links.

Day 3-5:
- Publish 1 post/day from two pillars (Market Context + Product Value).

Day 6-10:
- Add educational carousels/threads and one short video demo.

Day 11-14:
- Review clicks, saves, comments, and signups.
- Keep top-performing hooks and remove low-signal formats.

## 8) Compliance guardrails

Use in every post process:
- no guaranteed return language,
- include risk-aware phrasing,
- avoid personalized investment advice in public copy,
- keep claims evidence-backed.
