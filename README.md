# Quantura

Quantura is a market-intelligence and forecasting monorepo that powers the live web app at [quantura.studio](https://quantura.studio) and the Firebase-hosted environment at [quantura-e2e3d.web.app](https://quantura-e2e3d.web.app).

The repository combines:

- the Quantura web product and dashboard
- server-side APIs and SSR rendering
- scheduled research and stock-screening automation
- mobile client workspaces
- deployment and operational tooling

## What is in the product

The current web platform includes:

- public research, pricing, blog, market-news, and product pages
- authenticated dashboard workflows for watchlists, notifications, productivity, collaboration, and billing
- Forecast Foundry for SageMaker Autopilot-backed time-series workflows and prediction analysis
- historical data download and research tooling
- Explore publishing and Model Council output review
- admin fulfillment and operational panels

## Repository layout

```text
stockssagemakerdata/
├── quantura_site/
│   ├── pages/                 # Source HTML pages for the web app and SSR
│   ├── public/                # Client JS, CSS, assets, service worker, public pages
│   ├── functions_explore/     # Gen2 Node API for app workflows and AWS/SageMaker integration
│   ├── functions_ssr/         # Gen2 Node SSR renderer + synced templates
│   ├── functions_newsletter/  # Python newsletter jobs and unsubscribe handlers
│   ├── firestore.rules
│   ├── storage.rules
│   └── firebase.json
├── quantura_android/          # Android client workspace
├── quantura_ios/              # iOS client workspace
├── .github/workflows/         # CI, smoke tests, sitemap refresh, stock screener
├── daily_prophet_signal_tracker.py
├── combined_stock_screener.py
├── deploy.sh
└── requirements.txt
```

## Current stack

- Frontend: HTML, CSS, vanilla JS, React Native Web bundle injection
- App backend: Node.js Gen2 Cloud Functions in `quantura_site/functions_explore`
- SSR: Node.js Gen2 Cloud Function in `quantura_site/functions_ssr`
- Scheduled automation: Python scripts and GitHub Actions
- Data/storage: Firebase Hosting, Firestore, Cloud Storage, Remote Config
- Forecasting integrations: AWS SageMaker Autopilot, Yahoo Finance, fiscal/macro data sources

## GitHub workflows

The repo currently ships these workflows:

- `quantura-app-ci.yml` for app validation, SSR template sync checks, and legal-link checks
- `quantura-live-smoke.yml` for scheduled probes of key live routes
- `quantura-sitemap.yml` for sitemap regeneration
- `stock-screener.yml` for the daily Prophet-based large-cap stock screener
- `workflow-readiness-report.yml` for repo secret and integration readiness snapshots

## Local development

Prerequisites:

- Node.js 20+ for local site tooling
- Python 3.11+ for scripts and tests
- Firebase CLI and Google Cloud CLI for deploy operations

Install the web dependencies:

```bash
cd quantura_site
npm install
```

Install the Python dependencies:

```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata
python3 -m pip install -r requirements.txt
```

Useful checks:

```bash
node --check quantura_site/public/app.js
node --check quantura_site/functions_ssr/index.js
pytest -q quantura_site/tests
python daily_prophet_signal_tracker.py --help
```

If you update files in `quantura_site/pages/`, sync SSR templates before shipping:

```bash
node quantura_site/functions_ssr/scripts/sync-templates.js
```

## Deployment

The canonical production deploy path is the repo-level script:

```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata
./deploy.sh
```

That script currently handles:

- SSR template sync
- React Native Web bundle build
- Gen2 function deployment
- scheduler/topic setup
- Firebase Hosting release

If you are only working inside the web app, start with [quantura_site/README.md](quantura_site/README.md).

## Stock screener automation

The repo includes a scheduled daily large-cap tracker in [daily_prophet_signal_tracker.py](daily_prophet_signal_tracker.py).

It currently:

- scans S&P 500 plus Nasdaq symbols
- filters to names above a configurable market-cap floor
- uses Prophet quantile bands to detect `below_p10` and `above_p90` breaches
- writes artifacts for active signals, resolved transitions, and state snapshots
- runs from GitHub Actions on business days near market close

## Documentation and community files

This repo now includes the baseline open-source project files recommended by Open Source Guides:

- [README.md](README.md)
- [CONTRIBUTING.md](CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [SECURITY.md](SECURITY.md)
- [LICENSE](LICENSE)
- issue templates in [`.github/ISSUE_TEMPLATE/`](.github/ISSUE_TEMPLATE)
- PR template in [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md)

## Security

Please do not report vulnerabilities in public issues or public pull requests. Follow [SECURITY.md](SECURITY.md) for coordinated disclosure.

## License

This repository is licensed under the MIT License. See [LICENSE](LICENSE).
