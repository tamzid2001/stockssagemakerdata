# Quantitative screener pipeline

The production screener is built by `.github/workflows/stock-screener.yml` and `scripts/quant_screener_pipeline.py`. It is separate from browser requests: the website reads only a validated rolling release, so a partial or failed scan cannot replace the last complete dataset.

## Universe and metadata

- The S&P 500 constituent table is fetched from Wikipedia at scan time.
- Eligible Nasdaq-listed common stocks come from Nasdaq Trader's current symbol directory. Test issues, ETFs, warrants, rights, units, preferred instruments, and similar unsupported securities are excluded.
- Symbols in both sources are de-duplicated while retaining both memberships.
- SPY is added explicitly as an ETF and is not assigned an equity market-cap class or earnings date.
- One Nasdaq stock-screener snapshot enriches company, sector, industry, and market-cap data. Missing provider values remain unavailable.
- The Nasdaq earnings calendar is requested once for each business date in the next 30 calendar days during universe preparation. Results are joined to the universe; the website never makes per-row earnings calls.

Market-cap presets use these boundaries: Mega at least $200 billion, Large from $10 billion to $200 billion, Mid from $2 billion to $10 billion, Small from $300 million to $2 billion, and Micro below $300 million. ETFs are unclassified.

## Scan and forecast model

The matrix assigns every symbol to exactly one deterministic SHA-256 chunk. Chunks prefer batched Alpaca IEX daily bars using server-only `ALPACA_API_KEY`, `ALPACA_SECRET_KEY`, `ALPACA_DATA_URL`, and `ALPACA_DATA_FEED`. When credentials or the provider are unavailable, the chunk uses batched adjusted Yahoo daily closes and records that fallback in its artifact.

Each symbol with sufficient history uses the same `quantura_quantile_drift_v1` methodology as the application: historical log-return drift and volatility-scaled P10/P50/P90 boundaries for a ten-business-day horizon. Actual price is the most recent adjusted completed daily close. Missing market data, insufficient prediction history, unsupported instruments, and per-symbol failures are retained as explicit statuses rather than silently discarded.

## Coverage and publication

Every chunk uploads an immutable artifact and has a same-day/universe-hash cache key. Aggregation reconstructs missing chunk rows as failures, reports every status category, and computes:

`coverage = successfully evaluated symbols / eligible symbols`

The scheduled threshold is 90 percent. Below that threshold the Action fails after uploading diagnostics and preserves the previous `screener-latest` release. Passing runs publish JSON, CSV, and a run manifest. The backend applies validated server-side filtering, sorting, and pagination to the JSON asset and proxies the CSV without exposing a GitHub or Alpaca credential.

## Safe representative validation

Use workflow dispatch with a deterministic cap and publishing disabled before a full run:

```text
max_tickers: 24
chunk_count: 4
coverage_threshold: 0.90
publish_results: false
```

The complete scheduled workflow leaves `max_tickers` blank, uses 16 chunks, and publishes only after coverage passes. Secret values belong in GitHub Actions Secrets or the deployment secret store and must never be placed in the workflow, release assets, logs, or frontend code.
