#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

copy_if_missing() {
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

copy_if_missing \
  "$ROOT_DIR/quantura_site/functions/serviceAccountKey.json" \
  "$ROOT_DIR/quantura_site/functions/serviceAccountKey.example.json"

copy_if_missing \
  "$ROOT_DIR/quantura_ios/quantura_ios/GoogleService-Info.plist" \
  "$ROOT_DIR/quantura_ios/quantura_ios/GoogleService-Info.plist.example"

copy_if_missing \
  "$ROOT_DIR/quantura_android/app/google-services.json" \
  "$ROOT_DIR/quantura_android/app/google-services.json.example"

echo "Done. Replace placeholder values with real local credentials before running mobile/Firebase-dependent flows."
