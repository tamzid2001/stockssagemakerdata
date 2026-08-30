# Forecast Intelligence API v1

Base URL:

`https://quantura.studio/api/v1`

## Authentication

Send the API key in a header. Query-string credentials are not supported.

```bash
curl \
  -H "Authorization: Bearer $QUANTURA_API_KEY" \
  "https://quantura.studio/api/v1/forecasts?category=earnings&status=pending&min_probability=0.65"
```

`X-API-Key: $QUANTURA_API_KEY` is also supported. Keys are returned once at creation; Quantura stores only a keyed HMAC digest and prefix. Keys may have an expiry and can be revoked immediately.

Scopes:

| Scope | Access |
| --- | --- |
| `forecasts:read` | Current public forecast records, feed, categories and entities |
| `forecasts:history` | Append-only probability revisions |
| `forecasts:resolved` | Resolution metadata, scoring, calibration and performance |
| `forecasts:bulk` | Trajectory endpoint and immutable dataset releases |
| `forecasts:admin` | All API scopes; issuance is restricted to internal administrators |

## Envelope and errors

Successful response:

```json
{
  "data": [],
  "meta": {
    "api_version": "v1",
    "schema_version": "quantura_forecast_v1",
    "count": 0,
    "next_cursor": null
  }
}
```

Error response:

```json
{
  "error": {
    "code": "RATE_LIMITED",
    "message": "Rate limit exceeded.",
    "request_id": "..."
  }
}
```

Responses include `X-Request-ID`, `X-Quantura-API-Version` and rate-limit headers. Default list limit is 50; maximum is 500. Pagination uses opaque cursors from `meta.next_cursor`.

## Endpoints

### `GET /forecasts`

Filters: `category`, `subcategory`, `entity`, `ticker`, `status`, `min_probability`, `max_probability`, `created_after`, `created_before`, `resolves_after`, `resolves_before`, `model`, `limit`, `cursor`.

### `GET /forecasts/{forecast_id}`

Returns the current enterprise-safe forecast projection. Private strategy and alpha fields are excluded.

### `GET /forecasts/{forecast_id}/history`

Requires `forecasts:history`. Returns ordered, immutable revisions.

### `GET /forecasts/{forecast_id}/resolution`

Requires `forecasts:resolved`. Returns the frozen resolution rule, outcome evidence and scores.

### `GET /forecasts/resolved`

Requires `forecasts:resolved`. Returns resolved public forecasts.

### `GET /entities/{entity}/forecasts`

Returns forecasts for an exact normalized entity ID/name.

### `GET /calibration` and `GET /performance`

Require `forecasts:resolved`. Filters may include category, model and date range. Calibration rows include probability bucket, average prediction, actual frequency, sample count and Brier score.

### `GET /categories` and `GET /forecast-feed`

Return supported category identifiers and current unresolved feed records.

### `GET /datasets/forecast-trajectories`

Requires `forecasts:bulk`. Returns the prospective training-data projection with probability history and resolution labels, excluding private Quantura strategy fields.

### `GET /datasets/releases/{dataset_version}`

Returns an immutable manifest. Add `?format=jsonl` or `?format=csv` for a short-lived authenticated download URL.

## Examples

Python:

```python
import os
import requests

response = requests.get(
    "https://quantura.studio/api/v1/forecasts",
    headers={"Authorization": f"Bearer {os.environ['QUANTURA_API_KEY']}"},
    params={"category": "earnings", "status": "pending", "limit": 50},
    timeout=30,
)
response.raise_for_status()
for forecast in response.json()["data"]:
    print(forecast["forecast_id"], forecast["probability"], forecast["question"])
```

JavaScript/TypeScript:

```ts
const response = await fetch("https://quantura.studio/api/v1/forecasts?status=pending", {
  headers: { Authorization: `Bearer ${process.env.QUANTURA_API_KEY}` },
});
if (!response.ok) throw new Error(`Forecast API failed: ${response.status}`);
const { data, meta } = await response.json();
```

Cursor pagination:

```bash
curl \
  -H "X-API-Key: $QUANTURA_API_KEY" \
  "https://quantura.studio/api/v1/forecasts?limit=100&cursor=$NEXT_CURSOR"
```

## Rate limits and audit records

Limits are configured per key/tier, enforced per minute, and are not tied to hard-coded commercial pricing. Audit records contain key/customer identifiers, endpoint, status, record count, latency and request ID. They never contain the raw key.

## Dataset schema and guarantees

Each trajectory record includes forecast ID, creation and cutoff timestamps, category/entity, formal question, possible future headline, probability, public reasoning, evidence references, complete probability history, resolution deadline, status, actual outcome and scores. Dataset releases record `dataset_version`, `schema_version`, `generated_at`, `source_cutoff`, `record_count`, checksums, category/time coverage and licensing metadata. Released versions are immutable.

JSONL and CSV are currently supported. Parquet is not advertised until an approved server-side Parquet writer and compatibility test are deployed.
