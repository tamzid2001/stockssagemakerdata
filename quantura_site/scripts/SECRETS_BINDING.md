# Firebase Functions – Secret Manager Binding

Secrets used by `functions/main.py` are declared in `set_global_options(secrets=[...])` and are loaded from Google Cloud Secret Manager at deploy time.

## Secret → Env mapping

| Secret Manager Name | Code Variable       | Notes                          |
|---------------------|---------------------|--------------------------------|
| ALPACA_API_KEY      | ALPACA_API_KEY      | Direct match                   |
| ALPACA_SECRET_KEY   | ALPACA_SECRET_KEY   | Direct match                   |
| AMAZON_NOVA_KEY     | AMAZON_NOVA_API_KEY | Mapped in code                 |
| FCM_WEB_VAPID_KEY   | FCM_WEB_VAPID_KEY   | Direct match                   |
| OPENAI_API_KEY      | OPENAI_API_KEY      | Direct match                   |
| SLACK_WEBHOOK_URL   | SLACK_WEBHOOK_URL   | Direct match                   |
| STRIPE_PRIVATE_KEY  | STRIPE_SECRET_KEY   | Mapped in code                 |
| STRIPE_PUBLIC_KEY   | STRIPE_PUBLIC_KEY   | Direct match                   |
| STRIPE_WEBHOOK_SECRET | STRIPE_WEBHOOK_SECRET | Direct match (subscriptions at /webhook) |
| STRIPE_WEBHOOK_SECRET_CONNECT | STRIPE_WEBHOOK_SECRET_CONNECT | Direct match (Connect at /connect) |
| UNSPLASH_ACCESS_KEY | UNSPLASH_ACCESS_KEY | Direct match                   |
| UNSPLASH_APPLICATION_ID | UNSPLASH_ACCESS_KEY | Code uses either for client ID |
| UNSPLASH_SECRET_KEY | UNSPLASH_SECRET_KEY | Direct match                   |

## Step 3: Bind and deploy

1. Verify secrets exist:

   ```bash
   cd quantura_site
   chmod +x scripts/verify-firebase-secrets.sh
   ./scripts/verify-firebase-secrets.sh
   ```

2. Create any missing secrets (interactive – prompts for value). If you add `STRIPE_WEBHOOK_SECRET` to Secret Manager, add it to `secrets=[]` in `main.py` and re-deploy:

   ```bash
   firebase functions:secrets:set SECRET_NAME
   ```

3. Deploy functions via gcloud, then deploy hosting:

   ```bash
   cd ..
   CLOUDSDK_PYTHON=/opt/homebrew/bin/python3.13 ./deploy.sh
   ```

## Secrets not used by Firebase Functions

These live in Secret Manager but are used by GitHub Actions, scripts, or future code. They are not declared in `secrets=[]`:

- AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_ACCOUNT_ID, AWS_REGION → scripts, CI
- S3_BUCKET → scripts, CI
- CLAUDE_API_KEY, DEEPSEEK_API_KEY, GEMINI_API_KEY, MISTRAL_API_KEY, QWEN_API_KEY → future LLM providers
- FACEBOOK_APP_SECRET, FACEBOOK_CLIENT_ID → OAuth (not yet used)
- TIKTOK_CLIENT_KEY, TIKTOK_CLIENT_SECRET → OAuth (not yet used)
- SLACK_APP_ID, SLACK_CLIENT_ID, SLACK_CLIENT_SECRET, SLACK_SIGNING_SECRET → Slack app (not yet used)
