# Quantura on RapidAPI

[Subscribe](https://rapidapi.com/tamzid2001/api/quantura2/pricing) · [API listing](https://rapidapi.com/tamzid2001/api/quantura2)

## Billing

| Plan | Base price | Requests | New forecast jobs |
|---|---:|---|---|
| BASIC · Pay per Use | $0 | $0.0001 each | $0.67 each |
| PRO | $200/month | 3,000,000/month, hard quota | Included |

Both plans permit 60 HTTP requests/minute, three concurrent forecasts and three forecast starts/minute. RapidAPI displays its additional bandwidth platform fee. Existing subscribers retain their agreed plan version.

A Forecasts unit is charged when the API accepts and dispatches a new job, including reproduction. It is **not** conditional on eventual model success. Polling, downloads, cached results, validation failures, dispatch failures and replaying an existing idempotency key add zero Forecasts units. The separate Requests charge still applies to HTTP calls under RapidAPI's gateway rules. Use an `Idempotency-Key` for retried creation requests and poll sparingly.

RapidAPI subscriptions provide the published endpoints using RapidAPI application keys. They are separate from Quantura website/Clerk subscriptions and MCP OAuth. Website trials, WELCOME50 and website usage budgets do not apply to marketplace subscriptions.

## Connect

Use the base URL and host shown in the Hub code examples, with `X-RapidAPI-Key` and `X-RapidAPI-Host`. Never send a RapidAPI application key directly to quantura.studio as an origin credential. The gateway authenticates the application and subscription before supplying its private origin credential. Each marketplace user has isolated forecast ownership.

All paths below are relative to the RapidAPI base URL. Responses from forecast endpoints use `{ "data": ..., "meta": ... }`; data downloads use `{ "ok": true, "rows": ... }`.

## 1. Find a market

`GET /v1/market-search?q=bitcoin&source=kalshi_perps&limit=8`

Search stocks, Dukascopy instruments, Kalshi perpetuals and Kalshi/Polymarket US events. For a market URL use `/v1/market-search/resolve?url=...`; select an explicit returned contract and side. Use `/v1/market-search/capabilities` for provider coverage. Search results are bounded, not an exhaustive index.

## 2. Download observed history

`POST /v1/market-data/history`

```json
{"source":"kalshi_perps","symbol":"KXBTCPERP","frequency":"1h","limit":500,"format":"json"}
```

Use `source=alpaca` and `symbol=AAPL` for stocks, or `source=dukascopy` and `symbol=XAUUSD` for supported instruments. Choose `format=csv` for an attachment. Perpetuals use the same endpoint: prices are normalized to USD per underlying unit, with contract scaling included in metadata. Perpetual limits are 5,000 rows and 20,000 intervals per date range.

Event history uses `source=kalshi` or `polymarket_us`, full verified `contracts` from Search, `start` and `end` ISO timestamps, and a `frequency`. Choose the side explicitly; IDs are reverified by the provider. Dukascopy large-range JSON downloads support `page_mode=true`; follow `next_cursor` with identical settings. Missing observations are not fabricated.

## 3. Create a forecast

First call `GET /v1/forecast/models`. It returns current model availability, minimum history and supported quantiles. Enable only eligible models; requesting unsupported quantiles or insufficient history returns a validation error.

`POST /v1/ensemble-forecasts` with an `Idempotency-Key` header:

```json
{
  "source":{"type":"kalshi_perp","symbol":"KXBTCPERP","frequency":"1h","limit":500},
  "frequency":"1h",
  "prediction_length":24,
  "horizon_mode":"frequency_periods",
  "calendar":"NONE",
  "quantiles":[0.01,0.25,0.5,0.75,0.9,0.99],
  "models":{"prophet":{"enabled":true,"weight":1}}
}
```

This single forecast endpoint also handles `type=ticker` for stock/Dukascopy instruments and `type=prediction_market` for verified event contracts. Eligible ensembles can use Prophet, Chronos, Granite, Toto and TimesFM; availability and quantile support vary. Perpetuals use price transforms, not binary-probability logit transforms. Daily stock forecasts support trading-session horizons; perpetuals use frequency periods.

Supported intervals: 1m, 5m, 15m, 30m, 1h, 4h, daily, weekly and monthly. Read capabilities for canonical interval names and provider support. `history_cutoff_at` supports an explicit ISO timestamp; `history_lag_minutes` supports historical replay. Data returned after a historical cutoff cannot enter the model inputs.

## 4. Read the result

Creation returns HTTP 202 and `data.forecast_id`. Poll `GET /v1/ensemble-forecasts/{forecast_id}` every 10–30 seconds with backoff until `completed` or `failed`. Do not poll more aggressively after 429; honor Retry-After when supplied.

Download with `GET /v1/ensemble-forecasts/{forecast_id}/download?format=csv` or `format=json`. Use `/observations` for actual outcomes, and `/proof` for available provenance. Historical-validation metrics are separate from later realized forecast error. Research forecasts are not guaranteed returns.

Published screener ranges are available at `GET /v1/screener/forecasts/{ticker}`. Opening a published result does not create a custom model job.

## Integration notes

- Keep keys on your server; avoid exposing them in a browser bundle.
- Cache capabilities and completed forecasts; never repeat creation to poll.
- A reused idempotency key with a different payload returns a conflict.
- Treat 401/403 as an access issue, 422 as invalid configuration, and 429 as a rate or compute-admission limit. Back off for transient 5xx failures using the same idempotency key.
- Provider licensing and history coverage govern redistribution; no live trading endpoint is exposed.
