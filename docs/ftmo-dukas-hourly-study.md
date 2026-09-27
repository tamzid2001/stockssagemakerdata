# FTMO US hourly long/short research

Run **FTMO Dukascopy one-year long and short baskets** in GitHub Actions. Defaults cover September 26, 2025 origins through September 25, 2026 origins, each at 18:00 UTC through 17:00 UTC the next day. Ten approved `.sim` symbols have separate long and short reports.

## Agreed rules

| Instruments | Averaging spacing and basket target |
|---|---:|
| EURUSD, GBPUSD, USDCAD | 0.0010 (10 pips) |
| USDJPY, GBPJPY | 0.10 (10 pips) |
| US500, US30, US100 | 10 index points |
| XAUUSD | $1 quoted gold price |
| BTCUSD | $100 quoted BTC price |

- Each addition uses 0.01 lot. No 2.5× sizing.
- Long: initial buy strictly below P01; subsequent buys below current P01 and at least the spacing below the last execution. Sell the whole basket when executable bid reaches average ask entry plus the target.
- Short: initial sell strictly above P99; subsequent sells above current P99 and at least the spacing above the last execution. Buy back when executable ask reaches average bid entry minus the target.
- At most one addition per genuinely observed H1 bar. No re-entry in the exit bar. Carry until target; mark open liabilities at study end.
- Quantile limit levels and synthetic executable prices respect the published symbol digits. Targets are rounded outward to the executable price grid.

## Source and forecasts

The source matrix downloads frozen bid and ask candles once per instrument, including 90 days of warmup and USDJPY/USDCAD for USD conversion. Completed monthly native H1 archives are fast. Dukascopy's current-month native H1 endpoint rejects the incomplete monthly archive; that month uses completed daily M1 candles aggregated into UTC H1. No annual tick downloads or invented bars are used. Each source records raw response hashes and a frozen row hash.

Forecast chunks contain at most 75 origin dates, with four concurrent jobs and a 300-minute limit. Source jobs are limited to two concurrent jobs with paced requests and bounded retries honoring rate limits. Every forecast requires Prophet, Toto, Granite, Chronos and TimesFM to complete. Quantile provenance comes from the existing ensemble engine: a model lacking native P01/P99 does not receive fabricated tail values.

At each origin, only the last 500 genuinely completed hourly mid closes are supplied. Real calendar gaps stay in timestamps. Value-sequence foundation models consume observed values; Prophet uses actual times. If the last observation predates the origin, forecast the gap too, then retain the exact 23 future UTC hours. Inference duration is measured and new entries wait for the next full hourly bar. Partial-hour highs/lows are never assumed to occur after publication.

## Costs and practical limits

The planner captures the exact active FTMO US `.sim` specifications from the [first-party endpoint](https://ftmo.oanda.com/wp-json/ftmo/symbols), including contract size, profit currency, commission, swaps, leverage, volume caps and current weekly sessions. It saves one cost snapshot shared by all matrix jobs.

[FTMO US confirms](https://ftmo.oanda.com/blog/a-refined-trading-setup-for-the-us-market/) FX commission is $5 per lot round-trip. The current gold specification is **0.0014%**, which the compact ticker rounds to 0.001%; do not substitute the rounded display. Crypto is 0.065%. The public specification does not unambiguously document per-side versus round-trip percentage basis, so both interpretations are reported. The reference takes the higher, per-side cost.

Swaps are modeled as annual percentage of current notional divided by 360, following [MT5 interest/current](https://www.mql5.com/en/docs/constants/environment_state/marketinfoconstants). Current long/short rates are frozen; exact historical daily swap rates are not published by this endpoint. Triple-swap weekdays are assumptions, not verified FTMO historical data: Wednesday for FX/metals, Friday for indices, daily for crypto. A separate all-daily rollover sensitivity remains visible.

Observed Dukascopy bid/ask prices retain their spread. The replay widens that spread to at least the user's quoted FTMO snapshot. It does not charge that spread again as a commission. Stress doubles the spread floor, increases negative swaps 50%, and removes positive swap credits. Commissions are charged per entry and exit; swaps are assigned to the individual held legs. Closed trades can lose after fees even when their gross price target was reached.

JPY/CAD profits are converted with real USDJPY/USDCAD quotes, conservatively using bid or ask according to cash-flow sign. Intrahour fills use known hour-open conversion rates, an approximation. No uncompleted future FX close is used to decide a trade.

The [January 30, 2026 rolling-contract launch](https://ftmo.oanda.com/blog/trading-updates/trading-update-jan-28-2026/) changed contract sizes and costs for indices, gold and BTC. Full-year results are price-proxy research under current costs. For those five instruments, a separate post-launch baseline starts flat after January 30. Current minimum volume/step is absent from the public endpoint; the launch table showed minimum units of 0.1 for indices and BTC. The user-requested 0.01-lot study therefore does not establish executable sizing for every symbol.

Historical maintenance, holiday sessions, dividend-adjusted swaps, percentage fee basis and actual rollover rules need platform exports for exact replication. The projected GMT+2/+3 server clock uses Europe/Helsinki. The FTMO risk-day boundary uses Europe/Prague. Available margin and published volume limits block new additions; margin failures are reported. $100,000 account, $5,000 daily and $10,000 overall losses are diagnostic limits, retaining the user's prior cap-selection convention. They do not silently stop the research basket.

Daily-loss checks compare equity, including unrealized losses, fees and swaps, with midnight **balance**, not midnight equity. Closing only at profit cannot hide carried losses in this check.

Drawdown follows the assumed sequence of executable hourly opens, highs, lows and closes, including intrahour equity peaks before a loss. Session W/L measures equity changes from 18:00 UTC to the next 18:00 UTC (the final session ends at 17:00); carried targets and swaps remain active during the one-hour gap between forecast windows.

## Selection and validation

Nine fixed candidates: unfiltered baseline, median direction, median plus 3-hour momentum, median plus 6-hour momentum, median with a three-entry cap, 3-hour momentum with the cap, median with a three-hour entry delay, median with stable volatility, and median with a commission/one-night-swap feasibility check. All averaging obeys current P01/P99 and spacing rules.

Selection uses only the first 274 origin dates. A candidate needs at least 20 closed baskets and 20 exposed sessions; positive net equity profit retaining at least half of positive baseline profit; no worse drawdown, losing-session count or exposed-session win rate; and no development-period loss-limit/margin breach. Rank by fewest losing sessions, then drawdown, then profit. If no candidate qualifies, baseline remains selected.

The selected rule is frozen. Later validation starts flat and compares it with a fresh baseline; later performance cannot select a rule. Full-year alternative results are diagnostics. Both high-first and low-first OHLC paths are retained, reflecting unknown intrahour order rather than tick-level fills or confidence intervals. Model training data and source revisions limit retrospective validation claims.

Outputs include every rule/cost/path summary, fresh later validation, fresh post-launch baselines, closed entry/basket W/L, exposed-session W/L, commissions, swaps, open liabilities, maximum lots/margin, drawdown and limit breaches. Baseline and selected trade logs are retained. Missing forecasts produce an explicit incomplete report instead of silently skipping losing days.

The workflow has no broker credentials or order endpoints. Intermediate artifacts expire in seven days; the report expires in fourteen days.
