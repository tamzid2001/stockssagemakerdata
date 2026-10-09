# Quantura Server Secrets

This repo keeps secrets on the server side only. Client bundles must never embed private keys or tokens.

## Runtime loader

- The public runtime is Vercel. Use sensitive environment variables for production/preview credentials; redeploy the consuming project after updates. Never use public frontend aliases for private credentials.
- Vercel compatibility loader: `quantura_site/functions_legacy_vercel/secrets_loader.py`.
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
- `MOZILLA_DATA_COLLECTIVE_API_KEY`: Mozilla Data Collective authenticated dataset API. Stored in Secret Manager and the production `quantura-api` Secret environment variable. Load lazily on the server; do not add it to every Firebase function's secret bindings. The API uses `Authorization: Bearer` at `https://mozilladatacollective.com/api`. Catalog search and metadata are public; downloads require the dataset's terms/access requirements to have been satisfied. Adding this credential does not itself add a forecasting provider or authorize buying datasets.

Treasury Fiscal Data API does not require authentication.

### Billing and monetization

- `STRIPE_SECRET_KEY`: Stripe server API key.
- `STRIPE_WEBHOOK_SECRET`: Stripe webhook signature verification.

### Push and messaging

- `FCM_WEB_VAPID_KEY`: Web push token generation.
- `RESEND_API_KEY`: server-side transactional email delivery for automation and forecast-boundary alerts.
- `SLACK_WEBHOOK_URL`: operational notifications.

`FORECAST_ALERT_EMAIL_FROM` is a non-secret sender address override. Prediction alerts are sent only to the authenticated user's verified Firebase email; recipient addresses are never accepted from an alert API request.

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

### Partner setup

- `OPENAI_ADS_PIXEL_ID`: public pixel identifier; browser and server must match.
- `OPENAI_ADS_CONVERSIONS_API_KEY`: server-only conversion reporting.
- `OPENAI_ADS_MANAGEMENT_API_KEY`: separate management credential, not a pixel or conversion key.
- `PINTEREST_APP_ID` / `PINTEREST_APP_SECRET`: Pinterest app configuration.
- `TIKTOK_CLIENT_KEY` / `TIKTOK_CLIENT_SECRET`: TikTok app configuration.
- `GEMINI_PREDICTION_API_KEY` / `GEMINI_PREDICTION_API_SECRET`: prediction-provider credentials, separate from Google's `GEMINI_API_KEY`.
- `CALENDLY_ACCESS_TOKEN`: server-only Calendly access.

See [measurement setup and coverage](partner-measurement.md). Provisioning credentials does not activate OAuth publishing, trading or booking flows.

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
