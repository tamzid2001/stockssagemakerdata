# Quantura Explore Deploy (Hosting + gcloud Functions Gen2)

## 1) Prerequisites

```bash
gcloud auth login
gcloud config set project <YOUR_GCP_PROJECT_ID>
gcloud services enable cloudfunctions.googleapis.com eventarc.googleapis.com run.googleapis.com firestore.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com
```

If you use Secret Manager for web push config:

```bash
echo -n "<FCM_WEB_VAPID_PUBLIC_KEY>" | gcloud secrets create fcm-web-vapid-key --data-file=-
```

## 2) Deploy Explore Backend (Gen2 via gcloud)

```bash
cd quantura_site/functions_explore
npm install
npm run build
```

### HTTP API

```bash
gcloud functions deploy quanturaExploreApi \
  --gen2 \
  --runtime=nodejs24 \
  --region=us-central1 \
  --source=. \
  --entry-point=quanturaExploreApi \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars=FCM_WEB_VAPID_KEY=<FCM_WEB_VAPID_PUBLIC_KEY>
```

### Firestore Create Triggers

```bash
gcloud functions deploy onForecastCreated \
  --gen2 \
  --runtime=nodejs24 \
  --region=us-central1 \
  --source=. \
  --entry-point=onForecastCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='forecast_requests/{requestId}'

gcloud functions deploy onBacktestCreated \
  --gen2 \
  --runtime=nodejs24 \
  --region=us-central1 \
  --source=. \
  --entry-point=onBacktestCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='backtests/{backtestId}'

gcloud functions deploy onScreenerRunCreated \
  --gen2 \
  --runtime=nodejs24 \
  --region=us-central1 \
  --source=. \
  --entry-point=onScreenerRunCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='screener_runs/{runId}'

gcloud functions deploy onAgentRunCreated \
  --gen2 \
  --runtime=nodejs24 \
  --region=us-central1 \
  --source=. \
  --entry-point=onAgentRunCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='agent_runs/{runId}'

gcloud functions deploy refreshFiscaldataDefaults \
  --gen2 \
  --runtime=nodejs24 \
  --region=us-central1 \
  --source=. \
  --entry-point=refreshFiscaldataDefaults \
  --trigger-topic=fiscaldata-refresh
```

## 3) Deploy Firestore Rules and Indexes

```bash
cd ../
firebase deploy --only firestore:rules,firestore:indexes
```

## 4) Deploy Frontend (Firebase Hosting only)

```bash
firebase deploy --only hosting
```

## 5) Verify Rewrites

`firebase.json` routes `/api/explore*`, `/api/posts/**`, `/api/profile/**`, `/api/me/**`, `/api/follows/**`, `/api/watch-tickers/**`, `/api/notifications/**`, and `/api/fiscaldata/**` to `quanturaExploreApi`.

## 6) Optional local run

```bash
cd quantura_site/functions_explore
npm install
npm run dev
# API listens on :8080 (functions-framework)
```
