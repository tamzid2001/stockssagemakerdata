# Quantura Troubleshooting

## Firebase and local credentials

- If local server-side tooling fails on missing Firebase credentials, run `./scripts/setup_local_firebase_credentials.sh`.
- Confirm `quantura_site/functions/serviceAccountKey.json` exists locally when a workflow explicitly depends on it.
- If Secret Manager access is unavailable, confirm the example credential file exists and has been filled with local-only placeholder values where appropriate.

## SSR template drift

- If a page looks correct in `quantura_site/pages/` but not in SSR output, run `node quantura_site/functions_ssr/scripts/sync-templates.js`.
- Check for unintended diffs under `quantura_site/functions_ssr/templates/` after page updates.

## Hosting and dashboard regressions

- Run `node --check quantura_site/public/app.js` after touching dashboard or shared client logic.
- Verify both logged-out and logged-in routes when changing dashboard, Explore, or account flows.
- Re-test empty, loading, and error states when adding new workflow panels.

## Forecasting flow issues

- If Forecast Foundry or sports forecasting requests fail, verify the required backend env vars and Secret Manager bindings are available before debugging the UI.
- Reopen saved forecast outputs after backend changes to confirm persistence and reload behavior still work.
- Check live API health with `curl -sS https://quantura.studio/api/health` after deploys that touch `functions_explore`.

## Deploy issues

- Use `./deploy.sh` for production releases instead of ad hoc function-only deploys.
- If a deploy fails, check that `PROJECT_ID` resolves correctly and that the Google Cloud CLI and Firebase CLI are authenticated to the expected project.
- Re-run the documented smoke checks in `DEPLOY.md` before considering the release complete.
