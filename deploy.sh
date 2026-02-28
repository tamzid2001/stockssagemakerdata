#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SITE_DIR="$ROOT_DIR/quantura_site"
FUNCTIONS_SRC="$SITE_DIR/functions_explore"

FIREBASERC_PROJECT="$(
  node -e "try{const fs=require('fs');const p=JSON.parse(fs.readFileSync(process.argv[1],'utf8'));process.stdout.write((p.projects&&p.projects.default)||'');}catch(_e){}" \
    "$SITE_DIR/.firebaserc" 2>/dev/null || true
)"
PROJECT_ID="${PROJECT_ID:-$FIREBASERC_PROJECT}"
PROJECT_ID="$(echo "${PROJECT_ID}" | tr -d '[:space:]')"
REGION="${REGION:-us-central1}"
FIRESTORE_TRIGGER_LOCATION="${FIRESTORE_TRIGGER_LOCATION:-nam5}"
FUNCTIONS_RUNTIME="${FUNCTIONS_RUNTIME:-nodejs24}"
PUBLIC_ORIGIN="${PUBLIC_ORIGIN:-https://quantura.studio}"
PLAY_INTEGRITY_ANDROID_PACKAGE="${PLAY_INTEGRITY_ANDROID_PACKAGE:-com.quantura.quanturaapp}"
REQUIRE_PLAY_INTEGRITY="${REQUIRE_PLAY_INTEGRITY:-false}"
LOCAL_FUNCTIONS_BUILD="${LOCAL_FUNCTIONS_BUILD:-false}"

# Homebrew Python 3.11+ can hang with this local Cloud SDK install.
# Default to macOS system Python unless caller overrides CLOUDSDK_PYTHON explicitly.
if [[ -z "${CLOUDSDK_PYTHON:-}" && -x "/usr/bin/python3" ]]; then
  export CLOUDSDK_PYTHON="/usr/bin/python3"
fi

if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "PROJECT_ID is not set. Export PROJECT_ID or set quantura_site/.firebaserc default project."
  exit 1
fi

EXTRA_FLAGS=()
if [[ -n "${GCLOUD_SET_SECRETS:-}" ]]; then
  EXTRA_FLAGS+=(--set-secrets="${GCLOUD_SET_SECRETS}")
fi

if [[ "${LOCAL_FUNCTIONS_BUILD}" == "true" ]]; then
  echo "==> Building functions_explore locally (LOCAL_FUNCTIONS_BUILD=true)"
  pushd "${FUNCTIONS_SRC}" >/dev/null
  npm install
  npm run build
  popd >/dev/null
else
  echo "==> Skipping local functions build (Cloud Build handles function build during deploy)"
fi

echo "==> Deploying quanturaExploreApi (Gen2)"
gcloud functions deploy quanturaExploreApi \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=quanturaExploreApi \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="PUBLIC_ORIGIN=${PUBLIC_ORIGIN},PLAY_INTEGRITY_ANDROID_PACKAGE=${PLAY_INTEGRITY_ANDROID_PACKAGE},REQUIRE_PLAY_INTEGRITY=${REQUIRE_PLAY_INTEGRITY}" \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Deploying Firestore trigger: onForecastCreated"
gcloud functions deploy onForecastCreated \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --trigger-location="${FIRESTORE_TRIGGER_LOCATION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=onForecastCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='forecast_requests/{requestId}' \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Deploying Firestore trigger: onBacktestCreated"
gcloud functions deploy onBacktestCreated \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --trigger-location="${FIRESTORE_TRIGGER_LOCATION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=onBacktestCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='backtests/{backtestId}' \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Deploying Firestore trigger: onScreenerRunCreated"
gcloud functions deploy onScreenerRunCreated \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --trigger-location="${FIRESTORE_TRIGGER_LOCATION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=onScreenerRunCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='screener_runs/{runId}' \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Deploying Firestore trigger: onAgentRunCreated"
gcloud functions deploy onAgentRunCreated \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --trigger-location="${FIRESTORE_TRIGGER_LOCATION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=onAgentRunCreated \
  --trigger-event-filters=type=google.cloud.firestore.document.v1.created \
  --trigger-event-filters=database='(default)' \
  --trigger-event-filters-path-pattern=document='agent_runs/{runId}' \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Deploying frontend hosting"
pushd "${SITE_DIR}" >/dev/null
firebase deploy --only hosting --non-interactive
popd >/dev/null

echo "Deployment complete: backend (gcloud functions) + frontend (firebase hosting)."
