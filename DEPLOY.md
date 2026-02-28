# Deployment Policy (Backend + Hosting)

Use only these deployment paths:

1. Backend functions: `gcloud functions deploy ...`
2. Frontend hosting: `firebase deploy --only hosting`

Do not use `firebase deploy --only functions`.

## One-command deploy

From repo root:

```bash
./deploy.sh
```

`deploy.sh` always runs both stages in order:

1. `gcloud functions deploy` for the Explore API + Firestore trigger functions.
2. `firebase deploy --only hosting` for frontend.

Deployment is only considered complete after both stages succeed.

## Required environment / config

`deploy.sh` resolves `PROJECT_ID` in this order:

1. `PROJECT_ID` environment variable.
2. `quantura_site/.firebaserc` default project.

If neither is available, export `PROJECT_ID` before deploy.

Example:

```bash
export PROJECT_ID=<PROJECT_ID>
```

Optional overrides:

```bash
export PROJECT_ID=<PROJECT_ID>
export REGION=us-central1
export FIRESTORE_TRIGGER_LOCATION=nam5
export FUNCTIONS_RUNTIME=nodejs24
export PUBLIC_ORIGIN=https://quantura.studio
export PLAY_INTEGRITY_ANDROID_PACKAGE=com.quantura.quanturaapp
export REQUIRE_PLAY_INTEGRITY=false
export LOCAL_FUNCTIONS_BUILD=false
export CLOUDSDK_PYTHON=/usr/bin/python3
```

By default `LOCAL_FUNCTIONS_BUILD=false` to avoid local Node-version/tooling drift; Cloud Build compiles functions during `gcloud functions deploy`. Set `LOCAL_FUNCTIONS_BUILD=true` only if you want a local pre-deploy compile step.

`deploy.sh` also defaults `CLOUDSDK_PYTHON` to `/usr/bin/python3` when unset, because some Homebrew Python runtimes can stall with this Cloud SDK install.

## Secrets

Never commit API keys or shared secrets to git.

If secret values are needed during deploy, bind them through Secret Manager:

```bash
export GCLOUD_SET_SECRETS="OPENAI_API_KEY=projects/<PROJECT_ID>/secrets/OPENAI_API_KEY:latest,GEMINI_API_KEY=projects/<PROJECT_ID>/secrets/GEMINI_API_KEY:latest,MISTRAL_API_KEY=projects/<PROJECT_ID>/secrets/MISTRAL_API_KEY:latest,PERPLEXITY_API_KEY=projects/<PROJECT_ID>/secrets/PERPLEXITY_API_KEY:latest,FMP_API_KEY=projects/<PROJECT_ID>/secrets/FMP_API_KEY:latest,IOS_IAP_WEBHOOK_SECRET=projects/<PROJECT_ID>/secrets/IOS_IAP_WEBHOOK_SECRET:latest,APPLE_NOTIFICATIONS_WEBHOOK_SECRET=projects/<PROJECT_ID>/secrets/APPLE_NOTIFICATIONS_WEBHOOK_SECRET:latest,ADMOB_SSV_WEBHOOK_SECRET=projects/<PROJECT_ID>/secrets/ADMOB_SSV_WEBHOOK_SECRET:latest"
./deploy.sh
```

If any secret was previously committed in git history, rotate it.

## Smoke checks

```bash
curl -sS https://quantura.studio/api/health

curl -sS -X POST https://quantura.studio/api/analytics/ad-impression \
  -H "Content-Type: application/json" \
  -d '{"platform":"android","adPlatform":"admob","adFormat":"rewarded","adUnitId":"test-unit"}'
```
