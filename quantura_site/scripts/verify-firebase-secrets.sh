#!/usr/bin/env bash
# Verify Firebase Functions secrets exist in Secret Manager and document deploy steps.
# Run from quantura_site/: ./scripts/verify-firebase-secrets.sh
#
# Prerequisites: gcloud CLI, firebase CLI, and gcloud auth configured for project quantura-e2e3d

set -e
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-quantura-e2e3d}"
SECRETS=(
  ALPACA_API_KEY
  ALPACA_SECRET_KEY
  AMAZON_NOVA_KEY
  FCM_WEB_VAPID_KEY
  OPENAI_API_KEY
  SLACK_WEBHOOK_URL
  STRIPE_PRIVATE_KEY
  STRIPE_PUBLIC_KEY
  STRIPE_WEBHOOK_SECRET
  STRIPE_WEBHOOK_SECRET_CONNECT
  UNSPLASH_ACCESS_KEY
  UNSPLASH_APPLICATION_ID
  UNSPLASH_SECRET_KEY
  X_BEARER_TOKEN
  X_CLIENT_KEY
  X_CLIENT_SECRET
  X_CLIENT_SECRET_ID
  X_SECRET_KEY
)

echo "Project: $PROJECT_ID"
echo "Checking Secret Manager for ${#SECRETS[@]} secrets..."
echo ""

MISSING=0
for name in "${SECRETS[@]}"; do
  if gcloud secrets describe "$name" --project="$PROJECT_ID" &>/dev/null; then
    echo "  OK   $name"
  else
    echo "  MISS $name"
    ((MISSING++)) || true
  fi
done

echo ""
if [ "$MISSING" -gt 0 ]; then
  echo "Create missing secrets with:"
  echo "  gcloud secrets create SECRET_NAME --data-file=-"
  echo ""
fi

echo "Deploy with required flow:"
echo "  CLOUDSDK_PYTHON=/opt/homebrew/bin/python3.13 ./deploy.sh"
echo ""
echo "Ensure gcloud deploy includes --set-secrets for required keys."
