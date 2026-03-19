# Security Policy

## Supported surfaces

This repository is actively maintained for:

| Surface | Status |
| --- | --- |
| `main` branch | Supported |
| `quantura.studio` production web app | Supported |
| `quantura-e2e3d.web.app` Firebase-hosted app | Supported |
| GitHub Actions workflows in `.github/workflows/` | Supported |

Older feature branches, stale forks, and locally modified deployments may not receive fixes.

## Reporting a vulnerability

Do not open a public issue, discussion, or pull request for a security problem.

Use one of these private channels instead:

- GitHub private vulnerability reporting, if it is enabled for this repository
- Email: `tamzid257@gmail.com`

Please include:

- affected route, file, workflow, or API surface
- impact summary
- clear reproduction steps
- proof-of-concept details, logs, or screenshots
- any suggested mitigation or patch direction

If the report includes credentials or private data, send the minimum material needed to reproduce the issue safely.

## Response targets

Current best-effort targets:

- acknowledgement within 3 business days
- initial triage within 7 business days
- status update after reproduction and scope review

Complex infrastructure or vendor-dependent issues may take longer, but reporters should still receive status updates.

## Scope guidance

High-value areas in this repo include:

- authentication, account linking, and anonymous-user upgrade flows
- billing and checkout integrations
- Explore publishing, comments, and private/public sharing controls
- dashboard data exposure and admin-only surfaces
- Gen2 Cloud Functions in `quantura_site/functions_explore/`
- SSR routing and template hydration in `quantura_site/functions_ssr/`
- GitHub Actions that write artifacts or touch production resources
- secret handling, service-account usage, and deployment scripts

## Secret handling rules

- Never commit service-account JSON files, `.env` files, OAuth secrets, API tokens, or mobile Firebase config files.
- Use Google Secret Manager, GitHub Actions secrets, or local ignored files for sensitive values.
- Treat logs, screenshots, artifacts, and issue comments as potentially public unless they are in a private vulnerability report.

Known local-only credential paths that must stay out of Git:

- `quantura_site/functions/serviceAccountKey.json`
- `quantura_ios/quantura_ios/GoogleService-Info.plist`
- `quantura_android/app/google-services.json`

## Incident response checklist

1. Revoke or rotate compromised credentials immediately.
2. Remove exposed material from runtime config, CI secrets, and local developer copies.
3. Patch the vulnerable path and add a regression check when practical.
4. Review logs, billing activity, and storage access for abuse.
5. If secrets were committed, clean the Git history and force-push only after coordinating with maintainers.
6. Publish a user-facing advisory if the issue affected deployed systems or exposed user data.

## Safe disclosure expectations

Good-faith security research is welcome. Please:

- avoid privacy violations, destructive testing, or service disruption
- avoid accessing data that does not belong to you
- give the maintainers reasonable time to remediate before public disclosure

## Related files

- [README.md](README.md)
- [CONTRIBUTING.md](CONTRIBUTING.md)
- [deploy.sh](deploy.sh)
- [quantura_site/firestore.rules](quantura_site/firestore.rules)
