# Fiscal Data Macros

## Base API
- Base URL: `https://api.fiscaldata.treasury.gov/services/api/fiscal_service`
- Query pattern:
  - `{BASE_URL}{ENDPOINT}?fields=...&filter=...&sort=...&format=json&page[number]=...&page[size]=...`

No auth token is required.

## Registry Source of Truth
- Registry file: [`src/lib/fiscaldata/endpoints.registry.json`](/Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site/src/lib/fiscaldata/endpoints.registry.json)
- Each card in the Macro Dashboard is defined by one registry entry.
- Backend allowlist validation uses this registry model and only permits known endpoints.

## Add a New Macro Card
1. Add a new object to `endpoints.registry.json`:
   - `id`
   - `endpoint` (must start with `/v1/` or `/v2/`)
   - `title`
   - `category`
   - `defaultQuery.fields/sort/page`
   - `updateCadence`
   - `ttlSeconds`
2. Run the validator script:
   - `node --loader ts-node/esm scripts/verify-fiscaldata-registry.ts`
   - Optional endpoint ping: add `--ping`
3. Deploy backend + hosting.

## Filter Examples
- `record_date:gte:2015-01-01`
- `record_date:lte:2026-12-31`
- `country_currency_desc:in:(Canada-Dollar,Mexico-Peso)`

## Meta-driven rendering
UI rendering uses the Fiscal Data API `meta` object:
- `meta.labels` for table headers
- `meta.dataTypes` for type inference and formatting
- `meta.dataFormats` for display hints
- `meta.total-count` + `meta.total-pages` + `links.next` for pagination
