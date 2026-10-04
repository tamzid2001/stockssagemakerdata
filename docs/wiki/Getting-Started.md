# Getting started

## Use Quantura

1. Find a provider instrument or exact market outcome in [Search](https://quantura.studio/forecasting#market-search-title).
2. Inspect real observations and units in [Download](https://quantura.studio/historical-data).
3. Set the observed-bar interval, cutoff, model mix and horizon in [Forecast](https://quantura.studio/forecasting).
4. Review quantiles and historical overlays. Sign in to retain forecast and download requests. Uploaded CSV previews can be saved explicitly to Requests and reopened later.
   Profile uses Clerk account and billing components; Requests includes forecasts, downloads, uploaded CSVs and Jev responses.
5. Compare opportunities in [Screener](https://quantura.studio/screener).

Secure guest forecasts have fair-use limits. Sign-in uses Clerk with Google or email/password. Pro includes a 14-day website trial. Programmatic API access, personal keys and the OpenAPI reference require an active server-owned enterprise grant; browser-editable plans cannot grant access.

## Develop locally

Use Node.js 24 and Python 3.12. Clone the repository and install web/backend dependencies:

```bash
git clone https://github.com/tamzid2001/stockssagemakerdata.git
cd stockssagemakerdata
npm ci --prefix quantura_site
npm ci --prefix quantura_site/functions_explore
npm run lint --prefix quantura_site
npm run test:ui --prefix quantura_site
npm test --prefix quantura_site/functions_explore
npm run build --prefix quantura_site
node quantura_site/functions_ssr/scripts/sync-templates.js
```

For lightweight engine checks:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install numpy pandas pandas-market-calendars httpx pytest pyyaml==6.0.3
.venv/bin/python -m pytest -q ensemble_forecasting/tests
```

Real checkpoints require the locked inference dependencies, model access and applicable licensing. Operator credentials remain in private runtime/CI stores. Routine tests use mock fixtures; no broker orders are involved.

[Contribution guide](https://github.com/tamzid2001/stockssagemakerdata/blob/main/CONTRIBUTING.md) · [Architecture](Architecture-and-Operations) · [Troubleshooting](https://github.com/tamzid2001/stockssagemakerdata/blob/main/TROUBLESHOOTING.md)
