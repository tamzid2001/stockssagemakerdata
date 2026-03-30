#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-quantura-e2e3d}"
FORCE_REFRESH="${FORCE_REFRESH:-0}"
FIREBASE_SERVICE_ACCOUNT_SECRET_NAME="${FIREBASE_SERVICE_ACCOUNT_SECRET_NAME:-FIREBASE_SERVICE_ACCOUNT_JSON}"

copy_example_if_missing() {
  local target="$1"
  local example="$2"
  if [[ -f "$target" ]]; then
    echo "exists: $target"
    return
  fi
  if [[ ! -f "$example" ]]; then
    echo "missing example: $example"
    return
  fi
  cp "$example" "$target"
  echo "created: $target"
}

fetch_secret_if_available() {
  local secret_name="$1"
  local target="$2"

  if ! command -v gcloud >/dev/null 2>&1; then
    return 1
  fi
  if ! gcloud secrets describe "$secret_name" --project="$PROJECT_ID" >/dev/null 2>&1; then
    return 1
  fi

  mkdir -p "$(dirname "$target")"
  local tmp_file
  tmp_file="$(mktemp)"
  gcloud secrets versions access latest --secret="$secret_name" --project="$PROJECT_ID" > "$tmp_file"
  install -m 600 "$tmp_file" "$target"
  rm -f "$tmp_file"
  echo "fetched: $target"
}

materialize_local_credential() {
  local secret_name="$1"
  local target="$2"
  local example="$3"

  if [[ -f "$target" && "$FORCE_REFRESH" != "1" ]]; then
    echo "exists: $target"
    return
  fi
  if fetch_secret_if_available "$secret_name" "$target"; then
    return
  fi
  copy_example_if_missing "$target" "$example"
}

materialize_local_credential \
  "$FIREBASE_SERVICE_ACCOUNT_SECRET_NAME" \
  "$ROOT_DIR/quantura_site/functions/serviceAccountKey.json" \
  "$ROOT_DIR/quantura_site/functions/serviceAccountKey.example.json"

echo "Done. Local Firebase credential files stay ignored by git."
echo "Project: $PROJECT_ID"
echo "Secrets used:"
echo "  service account: $FIREBASE_SERVICE_ACCOUNT_SECRET_NAME"
echo "If Secret Manager access is unavailable, placeholder example files are created instead."
