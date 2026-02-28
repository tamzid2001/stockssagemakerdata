#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SITE_DIR="$ROOT_DIR/quantura_site"
FUNCTIONS_SRC="$SITE_DIR/functions_explore"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
PROJECT_ID="$(echo "${PROJECT_ID}" | tr -d '[:space:]')"
REGION="${REGION:-us-central1}"
FUNCTIONS_RUNTIME="${FUNCTIONS_RUNTIME:-nodejs24}"
PUBLIC_ORIGIN="${PUBLIC_ORIGIN:-https://quantura.studio}"
PLAY_INTEGRITY_ANDROID_PACKAGE="${PLAY_INTEGRITY_ANDROID_PACKAGE:-com.quantura.quanturaapp}"
REQUIRE_PLAY_INTEGRITY="${REQUIRE_PLAY_INTEGRITY:-false}"

if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "PROJECT_ID is not set. Run 'gcloud config set project <project-id>' or export PROJECT_ID."
  exit 1
fi

EXTRA_FLAGS=()
if [[ -n "${GCLOUD_SET_SECRETS:-}" ]]; then
  EXTRA_FLAGS+=(--set-secrets="${GCLOUD_SET_SECRETS}")
fi

echo "==> Building functions_explore"
pushd "${FUNCTIONS_SRC}" >/dev/null
npm install
npm run build
popd >/dev/null

echo "==> Deploying quanturaExploreApi (Gen2)"
gcloud functions deploy quanturaExploreApi \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=quanturaExploreApi \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="PUBLIC_ORIGIN=${PUBLIC_ORIGIN},PLAY_INTEGRITY_ANDROID_PACKAGE=${PLAY_INTEGRITY_ANDROID_PACKAGE},REQUIRE_PLAY_INTEGRITY=${REQUIRE_PLAY_INTEGRITY}" \
  "${EXTRA_FLAGS[@]}"

echo "==> Deploying Firestore trigger: onForecastCreated"
gcloud functions deploy onForecastCreated \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=onForecastCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='forecast_requests/{requestId}' \
  "${EXTRA_FLAGS[@]}"

echo "==> Deploying Firestore trigger: onBacktestCreated"
gcloud functions deploy onBacktestCreated \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=onBacktestCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='backtests/{backtestId}' \
  "${EXTRA_FLAGS[@]}"

echo "==> Deploying Firestore trigger: onScreenerRunCreated"
gcloud functions deploy onScreenerRunCreated \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=onScreenerRunCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='screener_runs/{runId}' \
  "${EXTRA_FLAGS[@]}"

echo "==> Deploying Firestore trigger: onAgentRunCreated"
gcloud functions deploy onAgentRunCreated \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=onAgentRunCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='agent_runs/{runId}' \
  "${EXTRA_FLAGS[@]}"

echo "==> Deploying frontend hosting"
pushd "${SITE_DIR}" >/dev/null
firebase deploy --only hosting
popd >/dev/null

echo "Deployment complete: backend (gcloud functions) + frontend (firebase hosting)."
