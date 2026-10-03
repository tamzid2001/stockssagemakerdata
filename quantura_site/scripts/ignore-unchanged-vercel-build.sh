#!/usr/bin/env bash
# Vercel: exit 0 skips a Git build; exit 1 builds. CLI/env-only deploys always build.
set -u
project="${1:-}"
previous="${VERCEL_GIT_PREVIOUS_SHA:-}"
current="${VERCEL_GIT_COMMIT_SHA:-}"
if [[ ! "$previous" =~ ^[a-fA-F0-9]{40}$ || ! "$current" =~ ^[a-fA-F0-9]{40}$ ]]; then exit 1; fi
repository="$(git rev-parse --show-toplevel 2>/dev/null)" || exit 1
git -C "$repository" cat-file -e "${previous}^{commit}" 2>/dev/null || exit 1
case "$project" in
  api) paths=(quantura_site/functions_explore) ;;
  legacy) paths=(quantura_site/functions_legacy_vercel) ;;
  newsletter) paths=(quantura_site/functions_newsletter) ;;
  ssr) paths=(quantura_site/functions_ssr quantura_site/pages) ;;
  *) exit 1 ;;
esac
paths+=(quantura_site/scripts/ignore-unchanged-vercel-build.sh)
if git -C "$repository" diff --quiet "$previous" "$current" -- "${paths[@]}"; then
  echo "No runtime changes for ${project}; skipping duplicate function bundle."
  exit 0
fi
exit 1
