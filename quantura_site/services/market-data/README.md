# Quantura Market Data Service

FastAPI service that centralizes stock + FX market data behind a stable schema for Quantura web and native clients.

## Endpoints
- `GET /health`
- `GET /fx/convert?amount=&from=&to=`
- `GET /stocks/quote?tickers=AAPL,MSFT&mode=fast|full`
- `POST /stocks/screener`

## Design Notes
- Source: Yahoo Finance via `yfinance`.
- `mode=fast` uses `fast_info` + batched `yf.download`.
- `mode=full` includes all normalized sections + `raw` with full `yfinance.info` dump.
- Caching:
  - Quote/FX: 30-120 seconds (default 90s).
  - Fast info: 1-30 minutes (default 5m).
  - Full info fields: 5-30 minutes (default 15m).
- Rate protection:
  - Global Yahoo concurrency semaphore.
  - Capped thread pool for per-symbol info fetches.
  - Batch price fetch with `yf.download`.

`yfinance.info` is slower and more brittle than `fast_info`; this service keeps the initial path fast and only fetches full info when explicitly requested.

## Stable Quote Schema
Every quote item returns:

```json
{
  "symbol": "AAPL",
  "asOf": "2026-02-24T18:20:00+00:00",
  "source": "yfinance",
  "price": {
    "last": 187.2,
    "prevClose": 186.9,
    "open": 187.0,
    "dayHigh": 188.2,
    "dayLow": 186.3,
    "volume": 55212211,
    "currency": "USD"
  },
  "valuation": {
    "marketCap": 3000000000000,
    "trailingPE": 31.2,
    "forwardPE": 28.9,
    "priceToBook": 42.0,
    "enterpriseValue": 3020000000000,
    "sharesOutstanding": 15500000000
  },
  "fundamentals": {
    "revenueTTM": 380000000000,
    "grossMargins": 0.44,
    "profitMargins": 0.25,
    "operatingMargins": 0.31,
    "ebitdaMargins": 0.35,
    "returnOnAssets": 0.22,
    "returnOnEquity": 1.33
  },
  "profile": {
    "longName": "Apple Inc.",
    "sector": "Technology",
    "industry": "Consumer Electronics",
    "country": "United States",
    "website": "https://www.apple.com",
    "longBusinessSummary": "..."
  },
  "risk": {
    "beta": 1.2,
    "shortRatio": 1.4
  },
  "dividends": {
    "dividendRate": 0.96,
    "dividendYield": 0.005,
    "exDividendDate": "2026-02-07",
    "payoutRatio": 0.15,
    "fiveYearAvgDividendYield": 0.007
  },
  "raw": {}
}
```

`raw` is present only for `mode=full` and contains the original `yfinance.info` object.

## Screener Usage
Preset mode:

```bash
curl -X POST http://127.0.0.1:8090/stocks/screener \
  -H "Content-Type: application/json" \
  -d '{"preset":"most_actives","size":25,"offset":0}'
```

Custom mode uses `operator/operands` shape compatible with `yfinance.EquityQuery`:

```json
{
  "query": {
    "operator": "and",
    "operands": [
      {"operator": "eq", "operands": ["region", "us"]},
      {"operator": "gt", "operands": ["dayvolume", 1000000]}
    ]
  },
  "size": 25,
  "offset": 0,
  "sortField": "dayvolume",
  "sortAsc": false
}
```

Supported screener fields/values are documented in `yfinance.EquityQuery.valid_fields` and `yfinance.EquityQuery.valid_values`.

## Local Run

```bash
cd quantura_site/services/market-data
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8090 --reload
```

## Smoke Tests

```bash
cd quantura_site/services/market-data
source .venv/bin/activate
pytest -q
```

## Deploy (Cloud Run)

```bash
cd quantura_site/services/market-data
gcloud run deploy quantura-market-data \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

Recommended runtime env settings:
- `MARKET_DATA_YAHOO_CONCURRENCY=4`
- `MARKET_DATA_INFO_WORKERS=4`
- `MARKET_DATA_QUOTE_TTL_SECONDS=90`
- `MARKET_DATA_FULL_INFO_TTL_SECONDS=900`

This service requires no API keys and should not store secrets.
