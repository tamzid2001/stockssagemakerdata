# Quantura Social Publishing via GitHub Actions

This document describes the production social automation architecture now used by Quantura.

The old Firebase-backed social automation flow is retired. Scheduled social publishing is now driven by GitHub Actions, with Google Cloud Secret Manager as the source of truth for credentials and Firestore used for deduplication and dispatch logs.

## What changed

- Social automation no longer depends on the retired Python backend shim in `quantura_site/functions/main.py`.
- Each live social channel now has its own scheduled GitHub Actions workflow:
  - `.github/workflows/social-x.yml`
  - `.github/workflows/social-linkedin.yml`
  - `.github/workflows/social-facebook.yml`
  - `.github/workflows/social-instagram.yml`
  - `.github/workflows/social-tiktok.yml`
- All of those wrappers call the shared reusable runner:
  - `.github/workflows/social-channel-runner.yml`
- Shared publish logic lives in:
  - `scripts/lib/social/`
- The workflow entrypoint is:
  - `scripts/social_workflow_runner.py`

## Current supported channels

Production-grade scheduled publishing is implemented for the channels that had real runnable automation paths in the previous repo flow:

- X
- LinkedIn
- Facebook Pages
- Instagram Business / Creator
- TikTok

Legacy webhook placeholders such as Threads, Reddit, YouTube, and Pinterest were not production-grade in the audited repo state. They are not scheduled by default in this migration.

## Architecture

### Shared runtime

The shared runtime handles:

- source selection
- optional AI draft generation
- platform formatting
- duplicate protection
- Firestore campaign / queue / dispatch logging
- retry behavior
- per-platform provider execution

Directory layout:

```text
scripts/lib/social/
├── config.py
├── content.py
├── drafting.py
├── models.py
├── pipeline.py
├── secrets.py
├── store.py
├── config/channels/
│   ├── x.json
│   ├── linkedin.json
│   ├── facebook.json
│   ├── instagram.json
│   └── tiktok.json
└── providers/
    ├── base.py
    ├── x.py
    ├── linkedin.py
    ├── facebook.py
    ├── instagram.py
    └── tiktok.py
```

### Content source strategy

Default scheduled content selection works like this:

1. Pick the newest unpublished blog entry from `quantura_site/public/blog/posts.manifest.json` for the channel.
2. If all current blog posts were already published to that channel, generate an evergreen Quantura market pulse fallback.
3. If a manual topic is supplied via `workflow_dispatch`, use that instead.

### Duplicate protection

Duplicate protection is enforced through Firestore:

- `social_publications`
- `social_campaigns`
- `social_queue`
- `social_dispatch_logs`

Each publish attempt reserves an idempotency record based on:

- channel
- source ID

That prevents scheduled reruns or overlapping workflows from posting the same source twice unless `force=true` is explicitly used.

## Google Cloud Secret Manager

Social publishing credentials must live in Google Cloud Secret Manager for the Quantura project.

Recommended project:

- `quantura-e2e3d`

### Required secrets by channel

#### Shared

- `OPENAI_API_KEY` (optional but recommended for AI-generated copy)

#### X

- `TWITTER_API_KEY`
- `TWITTER_API_SECRET`
- `TWITTER_ACCESS_TOKEN`
- `TWITTER_ACCESS_TOKEN_SECRET`

Optional:

- `X_USER_OAUTH2_TOKEN`

If `X_USER_OAUTH2_TOKEN` is present, the X adapter prefers it. Otherwise it uses OAuth 1.0a user-context credentials.

#### LinkedIn

- `LINKEDIN_ACCESS_TOKEN`
- `LINKEDIN_AUTHOR_URN`

#### Facebook

- `FACEBOOK_PAGE_ID`
- `FACEBOOK_PAGE_ACCESS_TOKEN`

#### Instagram

- `INSTAGRAM_BUSINESS_ACCOUNT_ID`
- `INSTAGRAM_ACCESS_TOKEN`

Optional override:

- `INSTAGRAM_DEFAULT_IMAGE_URL`

If `INSTAGRAM_DEFAULT_IMAGE_URL` is not set, the workflow uses Quantura’s public app icon as the media source.

#### TikTok

- `TIKTOK_ACCESS_TOKEN`
- `TIKTOK_OPEN_ID`

Optional but strongly recommended:

- `TIKTOK_DEFAULT_MEDIA_URL`
- `TIKTOK_PRIVACY_LEVEL`

If `TIKTOK_DEFAULT_MEDIA_URL` is not set, the workflow uses Quantura’s public app icon. The domain serving that media must be allowed by TikTok for pull-from-URL posting.

## GitHub repository settings

### Recommended auth model

Use GitHub Actions OIDC with Google Workload Identity Federation.

Required GitHub repository variables:

- `GCP_PROJECT_ID`
- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_SERVICE_ACCOUNT_EMAIL`

Optional fallback secret:

- `GCP_SERVICE_ACCOUNT_KEY_JSON`

The reusable workflow prefers OIDC. The service-account-JSON fallback is only there for environments that have not finished OIDC setup yet.

### Workflow permissions

The reusable social workflow requires:

- `contents: read`
- `id-token: write`

### Service account capabilities

The Google service account used by GitHub Actions should have the minimum roles needed to:

- read Secret Manager secrets
- write Firestore documents for publish logs and idempotency

In practice, that usually means:

- Secret Manager Secret Accessor
- Firestore User or another narrowly scoped Firestore write role

## Manual usage

Each platform workflow supports `workflow_dispatch`.

Manual inputs:

- `dry_run`
- `force`
- `source_mode`
- `topic`
- `cta_url`

### Dry-run

Dry-run builds the source, formats the post, records a run artifact, and writes a workflow summary without publishing to the provider.

### Force

`force=true` bypasses duplicate-post protection for a single run.

## Local usage

For local testing:

```bash
python scripts/social_poster_runner.py --channels x,linkedin --dry-run
```

To publish locally instead of previewing:

```bash
python scripts/social_poster_runner.py --channels x --send-now
```

To sync local env values into Google Cloud Secret Manager:

```bash
python scripts/sync_social_secrets_to_secret_manager.py --project-id quantura-e2e3d
```

The old helper `scripts/sync_github_social_secrets.py` is deprecated and should not be used for social publishing credentials anymore.

## Schedule map

The current default schedules are:

- X: weekdays at `13:15 UTC`
- LinkedIn: weekdays at `14:20 UTC`
- Facebook: Monday / Wednesday / Friday at `15:35 UTC`
- Instagram: Tuesday / Thursday / Saturday at `16:40 UTC`
- TikTok: Monday / Wednesday / Friday at `17:50 UTC`

These are intentionally separated so one platform’s failure or rate-limit event does not block the others.

## Platform implementation summary

### X

Implemented:

- scheduled and manual publish workflow
- duplicate protection
- OAuth user-context publishing through `POST /2/tweets`
- retry on transient failures
- text-first formatting with native URL inclusion

Constraints:

- text-first automation only in this migration
- media upload is intentionally not attempted in the scheduled path
- character limit handling is enforced in channel config

Official references used:

- [X API docs](https://docs.x.com/)

### LinkedIn

Implemented:

- scheduled and manual publish workflow
- Posts API publishing to member or organization URNs
- versioned REST headers
- duplicate protection and retry handling

Constraints:

- uses a text-first post body
- does not depend on LinkedIn scraping article preview behavior
- requires a valid `LINKEDIN_AUTHOR_URN`

Official references used:

- [LinkedIn community management docs on Microsoft Learn](https://learn.microsoft.com/en-us/linkedin/marketing/community-management/)

### Facebook

Implemented:

- scheduled and manual publish workflow
- Page feed publishing with message plus canonical link
- duplicate protection and retry handling

Constraints:

- targets Facebook Pages, not personal profiles
- requires Page-scoped token and relevant app permissions/review

Official references used:

- [Meta for Developers](https://developers.facebook.com/docs/)

### Instagram

Implemented:

- scheduled and manual publish workflow
- image-first publishing through Graph API media container creation and `media_publish`
- duplicate protection and retry handling

Constraints:

- Instagram feed publishing is not text-only; a public media URL is required
- requires a Business or Creator account linked through Meta Graph permissions

Official references used:

- [Instagram Graph API content publishing docs](https://developers.facebook.com/docs/instagram-api/guides/content-publishing)

### TikTok

Implemented:

- scheduled and manual publish workflow
- Content Posting API direct-post flow using pull-from-URL media
- creator privacy-option query before publish
- duplicate protection and retry handling

Constraints:

- requires a valid access token and `open_id`
- public publish availability depends on TikTok app approval and creator privacy options
- domain/media URL must be acceptable for pull-from-URL posting
- current automation uses photo posting for operational simplicity

Official references used:

- [TikTok Content Posting API](https://developers.tiktok.com/doc/content-posting-api-reference-direct-post?enter_method=left_navigation&from_seo_redirect=1)
- [TikTok User Data API](https://developers.tiktok.com/doc/minis-user-data)

## Operational guidance

- Run new channels in `dry_run=true` first.
- Confirm provider credentials and app review state before turning on scheduled publishing.
- Verify the Firestore idempotency log after the first real run.
- Use `force=true` only for deliberate reposts.
- Avoid changing schedules and provider configs in the same deploy when possible.

## Failure handling

Each channel workflow:

- logs the exact failing step in GitHub Actions
- writes a JSON artifact with the draft, source, and result payload
- retries transient provider failures
- fails early when required secrets are missing

That makes failures visible without leaking secrets into logs.
