# Research and backtesting

A forecast provides a distribution; a strategy supplies entry, sizing, fill, exit and risk rules. Keep publication/inference time separate from the data cutoff to avoid backdated decisions.

## Paid backtest API

`POST /api/v1/backtests` implements bounded, long-only, one-position quantile rules. Completed-bar signals fill at the next observed open. Exported rules remain `live_eligible: false`.

The API does not implement short averaging ladders, broker lots, basket-average targets, swap accounting or median-direction filters. Inspect `GET /api/v1/backtests/strategy-schema` before creating rules. The [algorithmic strategy guide](https://quantura.studio/developers/api#algorithmic-strategy) includes a valid P01-entry/P50-target API example.

## Dedicated averaged-basket studies

Repository workflows separately evaluate SPY, ten mega-cap stocks and FTMO-style assets. The latter use frozen Dukascopy bid/ask data, current-cost snapshots and explicit executable quote assumptions.

| Rule | Long | Short |
| --- | --- | --- |
| Initial signal | Below P01 | Above P99 |
| Additional legs | Lower only with spacing and eligible quantile | Higher only with spacing and eligible quantile |
| Research size | 0.01 lot per leg | 0.01 lot per leg |
| Target | Average ask entry plus instrument distance, exited at bid | Average bid entry minus instrument distance, exited at ask |
| Unfinished baskets | Carry and mark floating liability | Carry and mark floating liability |

FX starting spacing/target is 10 pips; index spacing/target is 10 points. Gold and BTC have separate dollar distances. Long/short statistics remain separate. Broker minimum volumes and dated specifications determine whether a research size is executable.

The triggered variant covers eight FX/index assets: a real minute P01/P99 breach prompts a second forecast from 500 completed H1 observations through 17:00 UTC. Additions wait for the refreshed extreme and a full actionable H1 bar after measured inference; targets remain active while carried.

## Interpret results

Report realized and floating P&L, commissions/swaps, entry-leg and basket W/L, losing sessions, ladder size, time in market, margin and drawdown. A high closed-basket win rate can leave losing baskets open. Current swaps, historical specifications and unknown intrahour price order are material assumptions.

Select padding, median filters and ladder caps only on the development period. Freeze selection and evaluate a later period starting flat, with a baseline and both hourly OHLC path orders. Full-year optimization is not proof of an out-of-sample edge.

The $100,000 2-Step comparison uses the static $90,000 equity floor and a $5,000 daily diagnostic referenced to midnight balance in Europe/Prague, including costs/open P&L. These diagnostics do not establish an FTMO challenge pass. [Official FTMO objectives](https://ftmo.com/en/trading-objectives/).

Kalshi coin automation is paper only. Research workflows do not carry broker credentials or order endpoints.

[FTMO methodology](https://github.com/tamzid2001/stockssagemakerdata/blob/main/docs/ftmo-dukas-hourly-study.md) · [SPY methodology](https://github.com/tamzid2001/stockssagemakerdata/blob/main/docs/spy-hourly-research.md) · [Backtest API](https://quantura.studio/developers/api#backtesting)
