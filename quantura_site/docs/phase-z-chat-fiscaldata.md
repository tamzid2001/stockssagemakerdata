# Phase Z Fiscal Data Notes

## Macro data provider

- Macro cards are now sourced from U.S. Treasury Fiscal Data API only.
- Base API:
  - `https://api.fiscaldata.treasury.gov/services/api/fiscal_service`
- Proxy endpoint:
  - `GET /api/fiscaldata`
- Registry endpoint:
  - `GET /api/fiscaldata/registry`

## Query surface

- `fields=...`
- `filter=...` (supports `gt`, `lt`, `gte`, `lte`, `eq`, `in`)
- `sort=...` (`-` prefix for desc)
- `format=json` (always enforced)
- `page[number]`, `page[size]`

## Caching

- Firestore cache collection: `fiscaldata_cache`
- Cache key: stable hash of endpoint + normalized query
- Cache doc fields:
  - `endpoint`
  - `query`
  - `fetchedAt`
  - `ttlSeconds`
  - `payload`

## Refresh job

- Pub/Sub triggered function:
  - `refreshFiscaldataDefaults`
- Refreshes default queries from registry into `fiscaldata_cache`.
