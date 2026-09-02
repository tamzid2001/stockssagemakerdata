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
SHOP_ALLOWED_ORIGINS="${SHOP_ALLOWED_ORIGINS:-https://quantura.studio,https://www.quantura.studio,https://quantura-e2e3d.web.app,https://quantura-e2e3d.firebaseapp.com}"
PLAY_INTEGRITY_ANDROID_PACKAGE="${PLAY_INTEGRITY_ANDROID_PACKAGE:-com.quantura.quanturaapp}"
REQUIRE_PLAY_INTEGRITY="${REQUIRE_PLAY_INTEGRITY:-false}"
AIKIDO_BLOCK="${AIKIDO_BLOCK:-false}"
AIKIDO_NODE_OPTIONS="${AIKIDO_NODE_OPTIONS:--r @aikidosec/firewall/instrument}"
AMAZON_NOVA_BASE_URL="${AMAZON_NOVA_BASE_URL:-}"
FISCALDATA_REFRESH_TOPIC="${FISCALDATA_REFRESH_TOPIC:-fiscaldata-refresh}"
AUTOPILOT_RECONCILE_TOPIC="${AUTOPILOT_RECONCILE_TOPIC:-quantura-autopilot-reconcile}"
AUTOPILOT_RECONCILE_JOB="${AUTOPILOT_RECONCILE_JOB:-quantura-autopilot-reconcile}"
AUTOPILOT_RECONCILE_CRON="${AUTOPILOT_RECONCILE_CRON:-*/15 * * * *}"
AUTOPILOT_RECONCILE_TIMEZONE="${AUTOPILOT_RECONCILE_TIMEZONE:-America/New_York}"
FORECAST_ALERT_TOPIC="${FORECAST_ALERT_TOPIC:-quantura-forecast-boundary-alerts}"
FORECAST_ALERT_JOB="${FORECAST_ALERT_JOB:-quantura-forecast-boundary-alerts}"
FORECAST_ALERT_CRON="${FORECAST_ALERT_CRON:-*/5 4-19 * * MON-FRI}"
FORECAST_ALERT_TIMEZONE="${FORECAST_ALERT_TIMEZONE:-America/New_York}"
FORECAST_ALERT_ALPACA_FEED="${FORECAST_ALERT_ALPACA_FEED:-iex}"
FORECAST_ALERT_COOLDOWN_MINUTES="${FORECAST_ALERT_COOLDOWN_MINUTES:-15}"
FORECAST_ALERT_MAX_PRICE_AGE_MINUTES="${FORECAST_ALERT_MAX_PRICE_AGE_MINUTES:-30}"
NEWSLETTER_TOPIC="${NEWSLETTER_TOPIC:-quantura-newsletter-weekly}"
NEWSLETTER_SCHEDULER_JOB="${NEWSLETTER_SCHEDULER_JOB:-quantura-newsletter-weekly}"
SCHEDULER_LOCATION="${SCHEDULER_LOCATION:-us-central1}"
NEWSLETTER_WEEKLY_CRON="${NEWSLETTER_WEEKLY_CRON:-0 9 * * MON}"
NEWSLETTER_TIMEZONE="${NEWSLETTER_TIMEZONE:-America/New_York}"
LOCAL_FUNCTIONS_BUILD="${LOCAL_FUNCTIONS_BUILD:-false}"
GCLOUD_BIN="${GCLOUD_BIN:-}"
REMOVE_SECRETS_KEYS="${REMOVE_SECRETS_KEYS:-}"
DEPLOY_PROVIDER="${DEPLOY_PROVIDER:-vercel}"
DEPLOY_DRY_RUN="${DEPLOY_DRY_RUN:-false}"
VERCEL_CLI_VERSION="${VERCEL_CLI_VERSION:-59.10.0}"
VERCEL_SCOPE="${VERCEL_SCOPE:-tamzid-ullahs-projects}"
DEPLOY_COMMIT_SHA="$(git -C "${ROOT_DIR}" rev-parse HEAD)"

deploy_vercel_project() {
  local label="$1"
  local project_name="$2"

  echo "==> Deploying ${label} to Vercel production"
  if [[ "${DEPLOY_DRY_RUN}" == "true" ]]; then
    echo "DRY RUN: vercel deploy --prod --yes --scope ${VERCEL_SCOPE} --project ${project_name} --cwd <committed-snapshot> --env GITLAB_SERVICE_VERSION=${DEPLOY_COMMIT_SHA}"
    return 0
  fi

  # Vercel project Root Directories are repository-relative. Deploy from a
  # clean, repository-root snapshot and select the project explicitly so the
  # configured root is applied exactly once. The snapshot also prevents local
  # ignored/untracked files from entering a production deployment.
  npm_config_cache="${VERCEL_NPM_CACHE}" npx --yes "vercel@${VERCEL_CLI_VERSION}" \
    deploy --prod --yes --scope "${VERCEL_SCOPE}" --project "${project_name}" --cwd "${VERCEL_SOURCE_DIR}" \
    --env "GITLAB_SERVICE_VERSION=${DEPLOY_COMMIT_SHA}" --meta "gitCommitSha=${DEPLOY_COMMIT_SHA}"
}

if [[ "${DEPLOY_PROVIDER}" == "vercel" ]]; then
  VERCEL_NPM_CACHE="${VERCEL_NPM_CACHE:-$(mktemp -d)}"
  export VERCEL_NPM_CACHE

  if [[ "${DEPLOY_DRY_RUN}" != "true" ]]; then
    VERCEL_SOURCE_DIR="$(mktemp -d)"
    cleanup_vercel_snapshot() {
      rm -rf -- "${VERCEL_SOURCE_DIR}"
    }
    trap cleanup_vercel_snapshot EXIT
    git -C "${ROOT_DIR}" archive HEAD | tar -x -C "${VERCEL_SOURCE_DIR}"
  else
    VERCEL_SOURCE_DIR="${ROOT_DIR}"
  fi
  export VERCEL_SOURCE_DIR

  # Vercel is the authoritative public web/API runtime for quantura.studio.
  # Deploy request-serving backends first and the public site last.
  deploy_vercel_project "Quantura API" "quantura-api"
  deploy_vercel_project "Quantura legacy API compatibility service" "quantura-legacy-api"
  deploy_vercel_project "Quantura newsletter service" "quantura-newsletter"
  deploy_vercel_project "Quantura SSR service" "quantura-ssr"
  deploy_vercel_project "Quantura web application" "quantura"
  echo "==> Vercel production deployment complete"
  exit 0
fi

if [[ "${DEPLOY_PROVIDER}" != "google-legacy" ]]; then
  echo "Unsupported DEPLOY_PROVIDER=${DEPLOY_PROVIDER}. Use vercel or google-legacy."
  exit 1
fi

echo "WARNING: running archived Google Cloud/Firebase deployment workflow."
echo "The public quantura.studio web and API runtime is Vercel."

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
AIKIDO_SECRET_BINDING=""

if [[ -z "${GCLOUD_SET_SECRETS:-}" ]]; then
  AUTO_SECRET_BINDINGS=()
  add_secret_binding() {
    local env_name="$1"
    shift
    local candidate=""
    for candidate in "$@"; do
      [[ -n "${candidate}" ]] || continue
      if "${GCLOUD_BIN}" secrets describe "${candidate}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
        AUTO_SECRET_BINDINGS+=("${env_name}=projects/${PROJECT_ID}/secrets/${candidate}:latest")
        return 0
      fi
    done
    return 1
  }

  # Secret Manager to runtime env mapping:
  # - Billing: STRIPE_SECRET_KEY / STRIPE_PRIVATE_KEY, STRIPE_WEBHOOK_SECRET / STRIPE_WEBHOOK_SECRET_CONNECT
  # - LLM providers: OPENAI_API_KEY, CLAUDE_API_KEY, GEMINI_API_KEY, DEEPSEEK_API_KEY, MISTRAL_API_KEY, PERPLEXITY_API_KEY, QWEN_API_KEY, AMAZON_NOVA_API_KEY
  # - Data providers: FMP_API_KEY
  # - GitHub workflow bridge: GITHUB_ACTIONS_TOKEN
  # - Webhook security: IOS_IAP_WEBHOOK_SECRET, APPLE_NOTIFICATIONS_WEBHOOK_SECRET, ADMOB_SSV_WEBHOOK_SECRET
  # - Newsletter email pipeline: NEWSLETTER_ADMIN_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, SES_FROM_EMAIL, SES_CONFIG_SET
  add_secret_binding "STRIPE_SECRET_KEY" "STRIPE_SECRET_KEY" "STRIPE_PRIVATE_KEY" "STRIPE_SECRET" "STRIPE_API_KEY" || true
  add_secret_binding "STRIPE_PRIVATE_KEY" "STRIPE_PRIVATE_KEY" "STRIPE_SECRET_KEY" "STRIPE_SECRET" "STRIPE_API_KEY" || true
  add_secret_binding "STRIPE_WEBHOOK_SECRET" "STRIPE_WEBHOOK_SECRET" "STRIPE_WEBHOOK_SECRET_CONNECT" "STRIPE_SIGNING_SECRET" || true
  add_secret_binding "STRIPE_WEBHOOK_SECRET_CONNECT" "STRIPE_WEBHOOK_SECRET_CONNECT" "STRIPE_WEBHOOK_SECRET" "STRIPE_SIGNING_SECRET" || true

  add_secret_binding "OPENAI_API_KEY" "OPENAI_API_KEY" "OPENAI_KEY" "OPENAI_SECRET_KEY" || true
  add_secret_binding "CLAUDE_API_KEY" "CLAUDE_API_KEY" "ANTHROPIC_API_KEY" || true
  add_secret_binding "GEMINI_API_KEY" "GEMINI_API_KEY" "GOOGLE_GENAI_API_KEY" || true
  add_secret_binding "DEEPSEEK_API_KEY" "DEEPSEEK_API_KEY" "DEEPSEEK_SECRET_KEY" || true
  add_secret_binding "MISTRAL_API_KEY" "MISTRAL_API_KEY" "MISTRAL_SECRET_KEY" || true
  add_secret_binding "PERPLEXITY_API_KEY" "PERPLEXITY_API_KEY" "PERPLEXITY_SECRET_KEY" || true
  add_secret_binding "QWEN_API_KEY" "QWEN_API_KEY" "QWEN_SECRET_KEY" || true
  add_secret_binding "AMAZON_NOVA_API_KEY" "AMAZON_NOVA_API_KEY" "BEDROCK_API_KEY" || true
  add_secret_binding "MODEL_COUNCIL_OTHER_API_KEY" "MODEL_COUNCIL_OTHER_API_KEY" "MODEL_COUNCIL_OTHER_KEY" || true
  add_secret_binding "GITHUB_ACTIONS_TOKEN" "GITHUB_ACTIONS_TOKEN" "GITHUB_TOKEN" "GH_TOKEN" || true
  add_secret_binding "IOS_IAP_WEBHOOK_SECRET" "IOS_IAP_WEBHOOK_SECRET" || true
  add_secret_binding "APPLE_NOTIFICATIONS_WEBHOOK_SECRET" "APPLE_NOTIFICATIONS_WEBHOOK_SECRET" || true
  add_secret_binding "ADMOB_SSV_WEBHOOK_SECRET" "ADMOB_SSV_WEBHOOK_SECRET" || true
  add_secret_binding "NEWSLETTER_ADMIN_KEY" "NEWSLETTER_ADMIN_KEY" || true
  add_secret_binding "AWS_ACCESS_KEY_ID" "AWS_ACCESS_KEY_ID" || true
  add_secret_binding "AWS_SECRET_ACCESS_KEY" "AWS_SECRET_ACCESS_KEY" || true
  add_secret_binding "AWS_SESSION_TOKEN" "AWS_SESSION_TOKEN" || true
  add_secret_binding "AWS_REGION" "AWS_REGION" || true
  add_secret_binding "AUTOPILOT_ROLE_ARN" "AUTOPILOT_ROLE_ARN" "SAGEMAKER_EXECUTION_ROLE_ARN" || true
  add_secret_binding "AUTOPILOT_S3_BUCKET" "AUTOPILOT_S3_BUCKET" "SAGEMAKER_AUTOPILOT_S3_BUCKET" || true
  add_secret_binding "SES_FROM_EMAIL" "SES_FROM_EMAIL" || true
  add_secret_binding "SES_CONFIG_SET" "SES_CONFIG_SET" || true
  add_secret_binding "ALPACA_API_KEY" "ALPACA_API_KEY" || true
  add_secret_binding "ALPACA_SECRET_KEY" "ALPACA_SECRET_KEY" || true
  add_secret_binding "RESEND_API_KEY" "RESEND_API_KEY" || true
  add_secret_binding "QUANTURA_FORECAST_API_KEY_PEPPER" "QUANTURA_FORECAST_API_KEY_PEPPER" || true

  if ! add_secret_binding "FMP_API_KEY" "FMP_API_KEY" "FMP_SECRET_KEY" "FMP_KEY"; then
    FMP_FALLBACK_SECRET="$("${GCLOUD_BIN}" secrets list --project="${PROJECT_ID}" --format='value(name)' --filter='name~^FMP_.*_KEY$' 2>/dev/null | head -n 1 | tr -d '[:space:]')"
    if [[ -n "${FMP_FALLBACK_SECRET}" ]]; then
      AUTO_SECRET_BINDINGS+=("FMP_API_KEY=projects/${PROJECT_ID}/secrets/${FMP_FALLBACK_SECRET}:latest")
    fi
  fi

  if [[ ${#AUTO_SECRET_BINDINGS[@]} -gt 0 ]]; then
    GCLOUD_SET_SECRETS="$(IFS=,; echo "${AUTO_SECRET_BINDINGS[*]}")"
    export GCLOUD_SET_SECRETS
    # Secret env var mapping (values remain in Secret Manager):
    # - LLM providers: OPENAI_API_KEY, CLAUDE_API_KEY, GEMINI_API_KEY, DEEPSEEK_API_KEY, MISTRAL_API_KEY, PERPLEXITY_API_KEY, QWEN_API_KEY, AMAZON_NOVA_API_KEY
    # - Market data: FMP_API_KEY
    # - Billing/webhooks: STRIPE_SECRET_KEY, STRIPE_PRIVATE_KEY, STRIPE_WEBHOOK_SECRET, STRIPE_WEBHOOK_SECRET_CONNECT
    # - GitHub workflow bridge: GITHUB_ACTIONS_TOKEN
    # - Native/webhook security: IOS_IAP_WEBHOOK_SECRET, APPLE_NOTIFICATIONS_WEBHOOK_SECRET, ADMOB_SSV_WEBHOOK_SECRET
    # - Newsletter/email: NEWSLETTER_ADMIN_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, SES_FROM_EMAIL, SES_CONFIG_SET
    echo "==> Auto-discovered Secret Manager bindings for deploy: ${GCLOUD_SET_SECRETS}"
  fi
fi

# Zen protects the Express HTTP functions only. Keep its runtime token out of
# unrelated triggers, SSR, and newsletter services, including when callers
# provide their own GCLOUD_SET_SECRETS override.
if "${GCLOUD_BIN}" secrets describe "AIKIDO_TOKEN" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  AIKIDO_SECRET_BINDING="AIKIDO_TOKEN=projects/${PROJECT_ID}/secrets/AIKIDO_TOKEN:latest"
fi

if [[ -n "${GCLOUD_SET_SECRETS:-}" ]]; then
  EXTRA_FLAGS+=(--set-secrets="${GCLOUD_SET_SECRETS}")
fi
if [[ -n "${REMOVE_SECRETS_KEYS}" ]]; then
  EXTRA_FLAGS+=(--remove-secrets="${REMOVE_SECRETS_KEYS}")
fi

EXPRESS_EXTRA_FLAGS=()
EXPRESS_SET_SECRETS="${GCLOUD_SET_SECRETS:-}"
if [[ -n "${AIKIDO_SECRET_BINDING}" ]]; then
  if [[ -n "${EXPRESS_SET_SECRETS}" ]]; then
    EXPRESS_SET_SECRETS="${EXPRESS_SET_SECRETS},${AIKIDO_SECRET_BINDING}"
  else
    EXPRESS_SET_SECRETS="${AIKIDO_SECRET_BINDING}"
  fi
fi
if [[ -n "${EXPRESS_SET_SECRETS}" ]]; then
  EXPRESS_EXTRA_FLAGS+=(--set-secrets="${EXPRESS_SET_SECRETS}")
fi
if [[ -n "${REMOVE_SECRETS_KEYS}" ]]; then
  EXPRESS_EXTRA_FLAGS+=(--remove-secrets="${REMOVE_SECRETS_KEYS}")
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

echo "==> Building React Native Web injection bundle"
pushd "${SITE_DIR}" >/dev/null
if [[ ! -d node_modules ]]; then
  npm install --no-audit --no-fund
fi
npm run build:rnweb
popd >/dev/null

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
  --set-env-vars="PUBLIC_ORIGIN=${PUBLIC_ORIGIN},PLAY_INTEGRITY_ANDROID_PACKAGE=${PLAY_INTEGRITY_ANDROID_PACKAGE},REQUIRE_PLAY_INTEGRITY=${REQUIRE_PLAY_INTEGRITY},AMAZON_NOVA_BASE_URL=${AMAZON_NOVA_BASE_URL},AIKIDO_BLOCK=${AIKIDO_BLOCK},NODE_OPTIONS=${AIKIDO_NODE_OPTIONS}" \
  ${EXPRESS_EXTRA_FLAGS[@]+"${EXPRESS_EXTRA_FLAGS[@]}"}

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
  --set-env-vars="^##^PUBLIC_ORIGIN=${PUBLIC_ORIGIN}##SHOP_ALLOWED_ORIGINS=${SHOP_ALLOWED_ORIGINS}##AIKIDO_BLOCK=${AIKIDO_BLOCK}##NODE_OPTIONS=${AIKIDO_NODE_OPTIONS}" \
  ${EXPRESS_EXTRA_FLAGS[@]+"${EXPRESS_EXTRA_FLAGS[@]}"}

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

echo "==> Ensuring Pub/Sub topic exists: ${AUTOPILOT_RECONCILE_TOPIC}"
"${GCLOUD_BIN}" pubsub topics create "${AUTOPILOT_RECONCILE_TOPIC}" --project="${PROJECT_ID}" >/dev/null 2>&1 || true

echo "==> Deploying Pub/Sub trigger: reconcileAutopilotRuns"
"${GCLOUD_BIN}" functions deploy reconcileAutopilotRuns \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=reconcileAutopilotRuns \
  --trigger-topic="${AUTOPILOT_RECONCILE_TOPIC}" \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}

echo "==> Ensuring Pub/Sub topic exists: ${FORECAST_ALERT_TOPIC}"
"${GCLOUD_BIN}" pubsub topics create "${FORECAST_ALERT_TOPIC}" --project="${PROJECT_ID}" >/dev/null 2>&1 || true

echo "==> Deploying Pub/Sub trigger: monitorForecastBoundaryAlerts"
"${GCLOUD_BIN}" functions deploy monitorForecastBoundaryAlerts \
  --quiet \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime="${FUNCTIONS_RUNTIME}" \
  --region="${REGION}" \
  --source="${FUNCTIONS_SRC}" \
  --entry-point=monitorForecastBoundaryAlerts \
  --trigger-topic="${FORECAST_ALERT_TOPIC}" \
  --set-env-vars="FORECAST_ALERT_ALPACA_FEED=${FORECAST_ALERT_ALPACA_FEED},FORECAST_ALERT_COOLDOWN_MINUTES=${FORECAST_ALERT_COOLDOWN_MINUTES},FORECAST_ALERT_MAX_PRICE_AGE_MINUTES=${FORECAST_ALERT_MAX_PRICE_AGE_MINUTES}" \
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

echo "==> Ensuring Cloud Scheduler Autopilot reconcile job"
if "${GCLOUD_BIN}" scheduler jobs describe "${AUTOPILOT_RECONCILE_JOB}" --location="${SCHEDULER_LOCATION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  if ! "${GCLOUD_BIN}" scheduler jobs update pubsub "${AUTOPILOT_RECONCILE_JOB}" \
    --project="${PROJECT_ID}" \
    --location="${SCHEDULER_LOCATION}" \
    --schedule="${AUTOPILOT_RECONCILE_CRON}" \
    --time-zone="${AUTOPILOT_RECONCILE_TIMEZONE}" \
    --topic="${AUTOPILOT_RECONCILE_TOPIC}" \
    --message-body='{"trigger":"autopilot_reconcile"}'; then
    echo "WARNING: Unable to update scheduler job ${AUTOPILOT_RECONCILE_JOB}. Check Cloud Scheduler permissions/config."
  fi
else
  if ! "${GCLOUD_BIN}" scheduler jobs create pubsub "${AUTOPILOT_RECONCILE_JOB}" \
    --project="${PROJECT_ID}" \
    --location="${SCHEDULER_LOCATION}" \
    --schedule="${AUTOPILOT_RECONCILE_CRON}" \
    --time-zone="${AUTOPILOT_RECONCILE_TIMEZONE}" \
    --topic="${AUTOPILOT_RECONCILE_TOPIC}" \
    --message-body='{"trigger":"autopilot_reconcile"}'; then
    echo "WARNING: Unable to create scheduler job ${AUTOPILOT_RECONCILE_JOB}. Check Cloud Scheduler permissions/config."
  fi
fi

echo "==> Ensuring Cloud Scheduler forecast-boundary alert job"
if "${GCLOUD_BIN}" scheduler jobs describe "${FORECAST_ALERT_JOB}" --location="${SCHEDULER_LOCATION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  if ! "${GCLOUD_BIN}" scheduler jobs update pubsub "${FORECAST_ALERT_JOB}" \
    --project="${PROJECT_ID}" \
    --location="${SCHEDULER_LOCATION}" \
    --schedule="${FORECAST_ALERT_CRON}" \
    --time-zone="${FORECAST_ALERT_TIMEZONE}" \
    --topic="${FORECAST_ALERT_TOPIC}" \
    --message-body='{"trigger":"forecast_boundary_alerts"}'; then
    echo "WARNING: Unable to update scheduler job ${FORECAST_ALERT_JOB}. Check Cloud Scheduler permissions/config."
  fi
else
  if ! "${GCLOUD_BIN}" scheduler jobs create pubsub "${FORECAST_ALERT_JOB}" \
    --project="${PROJECT_ID}" \
    --location="${SCHEDULER_LOCATION}" \
    --schedule="${FORECAST_ALERT_CRON}" \
    --time-zone="${FORECAST_ALERT_TIMEZONE}" \
    --topic="${FORECAST_ALERT_TOPIC}" \
    --message-body='{"trigger":"forecast_boundary_alerts"}'; then
    echo "WARNING: Unable to create scheduler job ${FORECAST_ALERT_JOB}. Check Cloud Scheduler permissions/config."
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
