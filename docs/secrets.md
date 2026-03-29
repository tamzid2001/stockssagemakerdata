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
- `SAGEMAKER_CANVAS_API_KEY`: SageMaker Canvas auth.
- `HUGGINGFACEHUB_API_TOKEN`: Hugging Face fallback inference auth.

### Market and data providers

- `ALPACA_API_KEY`: Alpaca trading/data auth.
- `ALPACA_SECRET_KEY`: Alpaca trading/data auth.
- `FMP_API_KEY`: Financial Modeling Prep earnings calendar auth (server-side only).
- `UNSPLASH_ACCESS_KEY`: Unsplash API auth.

Treasury Fiscal Data API does not require authentication.

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

Use the local bootstrap script to materialize ignored Firebase files from Google Secret Manager:

```bash
./scripts/setup_local_firebase_credentials.sh
```

The script looks for these secret names by default:

- `FIREBASE_SERVICE_ACCOUNT_JSON`
- `FIREBASE_IOS_GOOGLE_SERVICE_INFO_PLIST`
- `FIREBASE_ANDROID_GOOGLE_SERVICES_JSON`

You can override the project or secret names with:

- `GOOGLE_CLOUD_PROJECT`
- `FIREBASE_SERVICE_ACCOUNT_SECRET_NAME`
- `FIREBASE_IOS_CONFIG_SECRET_NAME`
- `FIREBASE_ANDROID_CONFIG_SECRET_NAME`

If Secret Manager access is not available, the script creates placeholder example files instead. Do not commit `.env`, `.env.local`, or hydrated local credential files.
