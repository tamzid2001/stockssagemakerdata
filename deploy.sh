#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SITE_DIR="$ROOT_DIR/quantura_site"
FUNCTIONS_SRC="$SITE_DIR/functions_explore"
SSR_FUNCTIONS_SRC="$SITE_DIR/functions_ssr"
NEWSLETTER_FUNCTIONS_SRC="$SITE_DIR/functions_newsletter"

FIREBASERC_PROJECT="$(
  node -e "try{const fs=require('fs');const p=JSON.parse(fs.readFileSync(process.argv[1],'utf8'));process.stdout.write((p.projects&&p.projects.default)||'');}catch(_e){}" \
    "$SITE_DIR/.firebaserc" 2>/dev/null || true
)"
PROJECT_ID="${PROJECT_ID:-$FIREBASERC_PROJECT}"
PROJECT_ID="$(echo "${PROJECT_ID}" | tr -d '[:space:]')"
REGION="${REGION:-us-central1}"
FIRESTORE_TRIGGER_LOCATION="${FIRESTORE_TRIGGER_LOCATION:-nam5}"
FUNCTIONS_RUNTIME="${FUNCTIONS_RUNTIME:-nodejs24}"
PYTHON_FUNCTIONS_RUNTIME="${PYTHON_FUNCTIONS_RUNTIME:-python313}"
PUBLIC_ORIGIN="${PUBLIC_ORIGIN:-https://quantura.studio}"
PLAY_INTEGRITY_ANDROID_PACKAGE="${PLAY_INTEGRITY_ANDROID_PACKAGE:-com.quantura.quanturaapp}"
REQUIRE_PLAY_INTEGRITY="${REQUIRE_PLAY_INTEGRITY:-false}"
FISCALDATA_REFRESH_TOPIC="${FISCALDATA_REFRESH_TOPIC:-fiscaldata-refresh}"
NEWSLETTER_TOPIC="${NEWSLETTER_TOPIC:-quantura-newsletter-weekly}"
NEWSLETTER_SCHEDULER_JOB="${NEWSLETTER_SCHEDULER_JOB:-quantura-newsletter-weekly}"
SCHEDULER_LOCATION="${SCHEDULER_LOCATION:-us-central1}"
NEWSLETTER_WEEKLY_CRON="${NEWSLETTER_WEEKLY_CRON:-0 9 * * MON}"
NEWSLETTER_TIMEZONE="${NEWSLETTER_TIMEZONE:-America/New_York}"
LOCAL_FUNCTIONS_BUILD="${LOCAL_FUNCTIONS_BUILD:-false}"
GCLOUD_BIN="${GCLOUD_BIN:-}"
REMOVE_SECRETS_KEYS="${REMOVE_SECRETS_KEYS:-}"

# Prefer Homebrew gcloud on macOS if present, then fallback to PATH.
if [[ -z "${GCLOUD_BIN}" ]]; then
  if [[ -x "/opt/homebrew/bin/gcloud" ]]; then
    GCLOUD_BIN="/opt/homebrew/bin/gcloud"
  elif [[ -x "/usr/local/bin/gcloud" ]]; then
    GCLOUD_BIN="/usr/local/bin/gcloud"
  else
    GCLOUD_BIN="$(command -v gcloud || true)"
  fi
fi

if [[ -z "${GCLOUD_BIN}" ]]; then
  echo "gcloud CLI not found. Install Google Cloud CLI and retry."
  exit 1
fi

resolve_cloudsdk_python() {
  local candidate=""
  local major=""
  local minor=""
  local version=""

  # Prefer newest supported versions first.
  for candidate in \
    "${CLOUDSDK_PYTHON:-}" \
    "$(command -v python3.14 2>/dev/null || true)" \
    "$(command -v python3.13 2>/dev/null || true)" \
    "$(command -v python3.12 2>/dev/null || true)" \
    "$(command -v python3.11 2>/dev/null || true)" \
    "$(command -v python3.10 2>/dev/null || true)" \
    "/opt/homebrew/bin/python3.14" \
    "/opt/homebrew/bin/python3.13" \
    "/opt/homebrew/bin/python3.12" \
    "/opt/homebrew/bin/python3.11" \
    "/opt/homebrew/bin/python3.10" \
    "/usr/local/bin/python3.14" \
    "/usr/local/bin/python3.13" \
    "/usr/local/bin/python3.12" \
    "/usr/local/bin/python3.11" \
    "/usr/local/bin/python3.10" \
    "$(command -v python3 2>/dev/null || true)" \
    ; do
    [[ -n "${candidate}" ]] || continue
    [[ -x "${candidate}" ]] || continue
    version="$("${candidate}" -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")' 2>/dev/null || true)"
    major="${version%%.*}"
    minor="${version#*.}"
    if [[ "${major}" == "3" && "${minor}" =~ ^[0-9]+$ && "${minor}" -ge 10 && "${minor}" -le 14 ]]; then
      echo "${candidate}"
      return 0
    fi
  done
  return 1
}

if [[ -z "${CLOUDSDK_PYTHON:-}" ]]; then
  if CLOUDSDK_PYTHON="$(resolve_cloudsdk_python)"; then
    export CLOUDSDK_PYTHON
  fi
fi

if [[ -n "${CLOUDSDK_PYTHON:-}" ]]; then
  echo "==> Using CLOUDSDK_PYTHON=${CLOUDSDK_PYTHON} ($("${CLOUDSDK_PYTHON}" --version 2>/dev/null || true))"
else
  echo "==> CLOUDSDK_PYTHON not explicitly set; gcloud default interpreter will be used."
fi

if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "PROJECT_ID is not set. Export PROJECT_ID or set quantura_site/.firebaserc default project."
  exit 1
fi

EXTRA_FLAGS=()
if [[ -n "${GCLOUD_SET_SECRETS:-}" ]]; then
  EXTRA_FLAGS+=(--set-secrets="${GCLOUD_SET_SECRETS}")
fi
if [[ -n "${REMOVE_SECRETS_KEYS}" ]]; then
  EXTRA_FLAGS+=(--remove-secrets="${REMOVE_SECRETS_KEYS}")
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

echo "==> Syncing SSR HTML templates"
node "${SSR_FUNCTIONS_SRC}/scripts/sync-templates.js"

echo "==> Deploying quanturaExploreApi (Gen2)"
"${GCLOUD_BIN}" functions deploy quanturaExploreApi \
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

echo "==> Deploying shopApi (Gen2)"
"${GCLOUD_BIN}" functions deploy shopApi \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=shopApi \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="PUBLIC_ORIGIN=${PUBLIC_ORIGIN}" \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Deploying Firestore trigger: onForecastCreated"
"${GCLOUD_BIN}" functions deploy onForecastCreated \
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
"${GCLOUD_BIN}" functions deploy onBacktestCreated \
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
"${GCLOUD_BIN}" functions deploy onScreenerRunCreated \
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
"${GCLOUD_BIN}" functions deploy onAgentRunCreated \
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

echo "==> Deploying Pub/Sub trigger: refreshFiscaldataDefaults"
"${GCLOUD_BIN}" functions deploy refreshFiscaldataDefaults \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=refreshFiscaldataDefaults \
  --trigger-topic="${FISCALDATA_REFRESH_TOPIC}" \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Deploying newsletter HTTP function: send_newsletter_daily_http"
"${GCLOUD_BIN}" functions deploy send_newsletter_daily_http \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${PYTHON_FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${NEWSLETTER_FUNCTIONS_SRC}" \
  --entry-point=send_newsletter_daily_http \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="PUBLIC_SITE_ORIGIN=${PUBLIC_ORIGIN}" \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Deploying newsletter HTTP function: email_unsubscribe_http"
"${GCLOUD_BIN}" functions deploy email_unsubscribe_http \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${PYTHON_FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${NEWSLETTER_FUNCTIONS_SRC}" \
  --entry-point=email_unsubscribe_http \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="PUBLIC_SITE_ORIGIN=${PUBLIC_ORIGIN}" \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Ensuring Pub/Sub topic exists: ${NEWSLETTER_TOPIC}"
"${GCLOUD_BIN}" pubsub topics create "${NEWSLETTER_TOPIC}" --project="${PROJECT_ID}" >/dev/null 2>&1 || true

echo "==> Deploying newsletter weekly scheduler function: send_newsletter_weekly_scheduler"
"${GCLOUD_BIN}" functions deploy send_newsletter_weekly_scheduler \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${PYTHON_FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${NEWSLETTER_FUNCTIONS_SRC}" \
  --entry-point=send_newsletter_weekly_scheduler \
  --trigger-topic="${NEWSLETTER_TOPIC}" \
  --set-env-vars="PUBLIC_SITE_ORIGIN=${PUBLIC_ORIGIN}" \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Ensuring Cloud Scheduler weekly newsletter job"
if "${GCLOUD_BIN}" scheduler jobs describe "${NEWSLETTER_SCHEDULER_JOB}" --location="${SCHEDULER_LOCATION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  if ! "${GCLOUD_BIN}" scheduler jobs update pubsub "${NEWSLETTER_SCHEDULER_JOB}" \
    --project="${PROJECT_ID}" \
    --location="${SCHEDULER_LOCATION}" \
    --schedule="${NEWSLETTER_WEEKLY_CRON}" \
    --time-zone="${NEWSLETTER_TIMEZONE}" \
    --topic="${NEWSLETTER_TOPIC}" \
    --message-body='{"trigger":"weekly_newsletter"}'; then
    echo "WARNING: Unable to update scheduler job ${NEWSLETTER_SCHEDULER_JOB}. Check Cloud Scheduler permissions/config."
  fi
else
  if ! "${GCLOUD_BIN}" scheduler jobs create pubsub "${NEWSLETTER_SCHEDULER_JOB}" \
    --project="${PROJECT_ID}" \
    --location="${SCHEDULER_LOCATION}" \
    --schedule="${NEWSLETTER_WEEKLY_CRON}" \
    --time-zone="${NEWSLETTER_TIMEZONE}" \
    --topic="${NEWSLETTER_TOPIC}" \
    --message-body='{"trigger":"weekly_newsletter"}'; then
    echo "WARNING: Unable to create scheduler job ${NEWSLETTER_SCHEDULER_JOB}. Check Cloud Scheduler permissions/config."
  fi
fi

echo "==> Deploying ssr (Gen2)"
"${GCLOUD_BIN}" functions deploy ssr \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${SSR_FUNCTIONS_SRC}" \
  --entry-point=ssr \
  --trigger-http \
  --allow-unauthenticated \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Deploying frontend hosting"
pushd "${SITE_DIR}" >/dev/null
firebase deploy --only hosting --non-interactive
popd >/dev/null

echo "Deployment complete: backend (gcloud functions) + frontend (firebase hosting)."
