# Event-history integrity audit — September 13, 2026

Scope: Polymarket US / Kalshi source history for Q Forecast, downloads,
historical research, and paper workers. No exchange orders were submitted.

## Reproduced incident

Market `aec-mlb-tex-az-2026-09-12`, Diamondbacks / short. Provider-recorded
game start: `2026-09-13T00:10:00Z`. Replay cutoff: `2026-09-13T02:03:00Z`,
matching the last input displayed in saved forecast `3oR376mqObdt535rBNFM`.

The shared adapter, against actual provider data, returned:

| Selection | Bars | Pregame | In-game | Price changes |
| --- | ---: | ---: | ---: | ---: |
| Both, last 500 available | 500 | 395 | 105 | 88 |
| In-game, last 60 elapsed minutes | 57 | 0 | 57 | 50 |

The 500-bar window starts at `2026-09-12T13:56:00Z` with 0.545 and ends
at the cutoff with 0.08. Its longest unchanged stretch spans 455 minutes.
Pregame-only input is rejected by the new flat-window guard because its
recent quotes remain unchanged. Downloads still preserve original values.
This demonstrates that pregame data exists and the flat stretch is in the
upstream display quotes; it does not prove individual trades or liquidity.

## Corrections

- Explicit custom-range pregame/in-game retrieval and chronological merge.
  No `INTERVAL_LIVE` substitution, coarse-series expansion, or new filling.
- Provider-confirmed event start, phase selection, elapsed lookback before
  selecting the last 500 bars; gaps remain gaps.
- Flat input guard before inference, on the API and shared Python research
  path: reject all-constant windows, or at least 30 final observations
  unchanged over 120 minutes. Do not mechanically discard repeated quotes.
- Persist quality counts and history selection in new immutable records.
  Old forecasts and datasets are not rewritten.
- Correct Kalshi end-stamped candle aggregation: round toward the completed
  target-interval end, not an earlier time that could leak future prices.
- Source-responsive ticker intervals and cutoff units; compact controls.
- Research methodology version incremented; do not combine old and new
  performance reports without identifying the version.

## Sources and limits

[Polymarket's price-history documentation](https://docs.polymarket.us/api-reference/price-history/get-price-history)
defines custom ranges of up to 24 hours, approximate observation cadence,
and book-derived display prices (not trades). Each range is bounded and
responses are cached for 30 seconds. These public gateway observations
cannot establish venue fills, available depth, or every historical update.

[Kalshi historical candles](https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks)
provide interval-end timestamps. No artificial minute bars are generated
from longer intervals. The point-in-time phase boundary uses recorded
provider metadata; actual delayed starts may differ from scheduled times.

Hourly/P1 representative jobs before this correction produced zero qualifying
Polymarket forecasts, not measured strategy win rates. Recurring rollout must
remain disabled until the revised pipeline passes real-data validation.
