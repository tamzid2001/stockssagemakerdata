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

Model:
- Default low-cost model: `gpt-4o-mini` via `SOCIAL_CONTENT_MODEL`.
- If `OPENAI_API_KEY` is missing, template fallback is used so automation still works.

## 5) Environment variables

Add these in your Functions environment (or `.env` for local emulator):
- `OPENAI_API_KEY`
- `SOCIAL_AUTOMATION_ENABLED=true`
- `SOCIAL_CONTENT_MODEL=gpt-4o-mini`
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

Current adapter pattern:
- one webhook per channel.
- later you can swap each webhook to direct official API integrations.

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

