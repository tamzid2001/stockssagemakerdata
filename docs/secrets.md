# Quantura Server Secrets

This repo keeps secrets on the server side only. Client bundles must never embed private keys or tokens.

## Runtime loader

- Server module: `quantura_site/functions/secrets_loader.py`
- Functions entrypoint: `quantura_site/functions/main.py`
- Firebase Functions global bindings: `set_global_options(..., secrets=secrets_loader.secret_bindings())`

All secret reads are centralized through `secrets_loader.get_secret(...)` with alias support and cached lookup.

## Secret names (no values)

### LLM and forecasting providers

- `OPENAI_API_KEY`: OpenAI completions and agent analysis.
- `AMAZON_NOVA_API_KEY`: Amazon Nova provider routing.
- `IBM_TIMEMIXER_API_KEY`: IBM TimeMixer auth.
- `HUGGINGFACEHUB_API_TOKEN`: Hugging Face fallback inference auth.

### Market and data providers

- `ALPACA_API_KEY`: Alpaca trading/data auth.
- `ALPACA_SECRET_KEY`: Alpaca trading/data auth.
- `MASSIVE_API_KEY`: Massive market data auth.
- `UNSPLASH_ACCESS_KEY`: Unsplash API auth.

### Billing and monetization

- `STRIPE_SECRET_KEY`: Stripe server API key.
- `STRIPE_WEBHOOK_SECRET`: Stripe webhook signature verification.

### Push and messaging

- `FCM_WEB_VAPID_KEY`: Web push token generation.
- `SLACK_WEBHOOK_URL`: operational notifications.

### Social APIs and publishing

- `TWITTER_BEARER_TOKEN`
- `X_USER_OAUTH2_TOKEN`
- `TWITTER_API_KEY`
- `TWITTER_API_SECRET`
- `TWITTER_ACCESS_TOKEN`
- `TWITTER_ACCESS_TOKEN_SECRET`
- `LINKEDIN_ACCESS_TOKEN`
- `FACEBOOK_PAGE_ACCESS_TOKEN`
- `INSTAGRAM_ACCESS_TOKEN`
- `TIKTOK_ACCESS_TOKEN`
- `META_CAPI_ACCESS_TOKEN`

### Optional channel webhook overrides

- `SOCIAL_WEBHOOK_X`
- `SOCIAL_WEBHOOK_LINKEDIN`
- `SOCIAL_WEBHOOK_FACEBOOK`
- `SOCIAL_WEBHOOK_INSTAGRAM`
- `SOCIAL_WEBHOOK_THREADS`
- `SOCIAL_WEBHOOK_REDDIT`
- `SOCIAL_WEBHOOK_TIKTOK`
- `SOCIAL_WEBHOOK_YOUTUBE`
- `SOCIAL_WEBHOOK_PINTEREST`

## Fail-fast behavior

Functions that require a missing secret return `FAILED_PRECONDITION` with explicit server-side error messages (for example Stripe and Web Push config), and provider features short-circuit with clear missing-credential responses.

## Local development

Use a local pull script (ignored by git) to materialize secrets into `.env.local` for local emulators only. Do not commit `.env`, `.env.local`, or any secret values.

