# Market data and downloads

Search keeps provider identity with the selected instrument, contract and side. Downloads preserve price basis and timestamps for reproducible research.

| Source | Data | Notes |
| --- | --- | --- |
| Alpaca / Yahoo | Stocks, ETFs and supported market history | Provider limits and entitlements; effective fallback, adjustment and exchange timezone retained |
| Dukascopy | Full published FX, metals, index and other CFD catalog | Bid/ask quotes, instrument-specific decimal scales and provider history bounds |
| Kalshi / Polymarket US | Exact binary outcomes | Decimal probability; separate quote/trade fields and verified event timing |
| Kalshi perpetuals | Margin-contract history | Price normalized by full underlying exposure; separate from binary outcomes |
| CSV / workspace | User-supplied series | Explicit timestamps, numeric target and validated frequency |

## Dukascopy

The checked-in catalog contains 1,504 instruments. The service refreshes it and marks snapshot fallback. Select instruments from Search instead of assuming every stock has a provider CFD.

Native minute/hour/daily candle archives avoid fetching every hourly tick file for long ranges. Weekly/monthly periods aggregate genuine daily closes where available. Downloads identify bid/ask side, scale, source, available-from date, completed files and range cutoff. Empty periods are not filled.

For long downloads use `page_mode` and follow every `next_cursor` with unchanged settings. The cutoff stays frozen across pages. Weeks crossing annual archive pages retain their observations without duplicate weekly rows.

CFD quotes are not exchange share prices. Provider quote volume is not exchange-traded share volume. Corporate-action adjustments are not supported for Dukascopy quotes.

## Retention and rate limits

Selecting a frequency does not guarantee 500 bars. Providers can return fewer genuine observations, reject unsupported ranges or rate limit requests. Yahoo caching/coalescing and verified fallback retain provenance. Kalshi discovery honors retry instructions and marks partial coverage; missing data is reported rather than invented.

[Download guide](https://quantura.mintlify.app/docs/q-download) · [Data provenance](https://quantura.mintlify.app/docs/data-provenance) · [Forecast intervals](Forecasting)
