# Market data and saved game forecasts

Dukascopy Search exposes the complete published instrument catalog (1,504 instruments in the September 26, 2026 snapshot). Availability depends on each instrument's published history. Unsupported or unavailable instruments return an explicit error; they do not receive fabricated prices.

The downloader uses Dukascopy's current official historical export API, rather than fetching every tick hour. H1/H4/D1 use monthly hour candles where the instrument's session aligns to UTC hours, otherwise daily minute files preserve session offsets. M1/M5/M15/M30 use minute files. Prices retain the instrument's published scale and selected bid/ask side. UTC buckets include only completed observed candles; gaps remain gaps. Equity instruments are CFDs, not exchange shares.

Browser exports paginate bounded requests, freeze the original cutoff, support cancellation, and publish only complete exports. CSV output contains timestamps and closes. Forecast inputs timestamp closes at bucket ends, preserve quote provenance, and use frequency periods rather than an equity calendar.

Measured locally, the user's XAUUSD H1 BID example from January 1, 2023 through January 3, 2026 returned 17,764 candles from 37 monthly files in 2.08 seconds. This is one observed request, not a latency guarantee for every instrument.

Today's games open saved Forecast pages via `gameForecastId`. They show P01/P25/P50/P75/P90/P99, genuine hourly observation counts, provider-verified kickoff, and the forecast through kickoff plus four hours. No inference is triggered by viewing a saved result.

Pregame workers recheck kickoff with the first-party provider before and after inference. Listing dates and settlement dates are never treated as kickoff. Four real models are required; Toto joins at 32 genuine observations, making five when approved TimesFM access is available. Models without native extreme quantiles do not supply P01/P99. There is no fabricated history to meet a context requirement.

The hourly workflow shards both providers across four jobs. Live updates stop at the hour containing kickoff. Manual `refresh_published` recomputes existing forecasts using their original pregame cutoff and labels them retrospective, preserving the original generation time. It does not present postgame observations as pregame data.

Company marks are provided by the attributed AllInvestView ticker logo CDN, with exact ticker-to-domain matches and visible fallback initials. Kalshi and Polymarket use their official local brand assets. Logo lookup never makes Yahoo requests.
