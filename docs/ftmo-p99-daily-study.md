# FTMO daily P99 ladder study

Run [FTMO daily P99 seven-session yearly grid study](../.github/workflows/ftmo-p99-daily-year.yml) to compare ten assets over up to 365 completed calendar days. This research workflow sends no orders and writes no Firestore documents.

## Forecasts and signals

- EURUSD, USDJPY, GBPUSD, GBPJPY, USDCAD, US500, US30, US100, XAUUSD and BTCUSD `.sim` instruments.
- A separate $100,000 USD starting account for each asset. Standalone account returns are not a shared portfolio result.
- One forecast at 18:00 UTC after an observed 18:00–17:00 UTC session finishes. The hour from 17:00 to 18:00 is outside that source session.
- Exactly 500 genuine completed daily session candles, aggregated from paired Dukascopy hourly observations. Missing candles and holidays are not filled. Daily closes are bid/ask midpoints; source coverage remains recorded.
- Seven future weekday sessions for FX/indices/metals; seven daily sessions for BTC. These are explicit UTC CFD session labels, not NYSE calendars or a guarantee of future broker holiday hours.
- Prophet, Toto 2.0 4m, Granite, Chronos and TimesFM all execute, with checkpoints/revisions pinned by Quantura's registry. A failed model invalidates that origin.
- P01, P25, P50, P75, P90 and P99. Toto and TimesFM's native ranges do not include P01/P99: the supported models' tail weights renormalize without extrapolating unsupported tails.
- Initial buy signal: the observed cutoff close is strictly above the **first** predicted P99. Entry uses the next available executable ask after measured inference latency.

## Authenticated history source

### Replaying other entry rules

The workflow accepts `entry_rule=above_p99` (the original), `above_p90`,
`below_p99`, or `daily_buy`. The last option starts a basket at the first
executable ask after each completed daily forecast whenever flat; it has no
cutoff-price quantile condition. After closing, it waits for a later forecast.
`averaging_gate=grid` removes the first-add P90 condition, so every addition
uses a lower grid level. The fixed final triggering P01 stop and final
P99−P01 risk reference remain unchanged. Entries at or below the fixed stop
and entries below the risk-sized minimum lot are rejected.

Set `replay_only=true` and a completed `resume_run_id` to reuse its frozen
plan, broker snapshot, source prices and model forecasts. This skips provider
downloads and model inference. Each replay records the entry and averaging
rules; aggregation rejects results from a different rule. The original
forecast artifacts are not altered or uploaded again.

This study uses Dukascopy's [documented S3 bulk source](https://www.dukascopy.com/wiki/en/development/data-export/), `cfg-public-proper-wallaby` in `eu-west-1`, with authenticated Requester Pays reads. It downloads native daily **M1 candle archives**, not tick archives or the entire bucket. At most eight daily downloads per job and two asset jobs run concurrently. Minute candles are aggregated into hourly observations and daily sessions; only the replay year's minute rows are retained. This keeps warmup memory bounded and avoids the annual hourly tick-download loop.

The big-endian archive records are decoded as seconds, open, close, low, high and volume, with the checked-in first-party instrument catalog's price scale. Both bid and ask are required. Flat zero-volume minutes on both sides are discarded as stale placeholders; no missing intervals are filled. Every source records object versions, ETags, compressed SHA-256 hashes, absent archive days and excluded placeholders. Frozen row hashes are checked before forecasts and replays.

GitHub obtains temporary credentials through OIDC. The configured `QuanturaDukascopyHistoryReader` role permits only `s3:GetObject` on the ten approved symbols' BID/ASK minute candle files. It permits no uploads, deletions, tick downloads or bucket enumeration. Its trust is restricted to this repository's `main` branch. Local AWS credentials are never copied into GitHub or artifacts.

Repository variable `DUKASCOPY_HISTORY_ROLE_ARN` selects the role. Reviewable configuration is in [the trust policy](../market_research/infra/dukascopy-history-trust.json) and [the read policy](../market_research/infra/dukascopy-history-read.json). AWS Requester Pays charges apply to these bounded reads. The provider-access job reads and validates the latest replay weekday's paired EURUSD archive before scheduling the source matrix.

For local diagnostics, the downloader accepts `--profile default` after AWS CLI authentication:

```sh
python -m market_research.dukascopy_s3_source \
  --symbol EURUSD.sim --start 2025-10-07 --end 2026-10-06 \
  --output /tmp/ftmo-p99/EURUSD.sim --profile default
```

Install the hash-locked `market_research/requirements-dukas-s3.lock` first. Its CRT dependency supports AWS CLI login profiles locally; Actions uses OIDC credentials. The study's source wrapper additionally requires at least 500 genuine completed warmup sessions before proceeding.

## Fixed stop and equity risk

The triggering forecast's **seventh/final P01** is the basket stop until exit, rounded down to the instrument's executable price tick. Later forecasts do not move it. Its **final P99 minus final P01** defines the sizing reference range:

```text
E = USD account equity immediately before the initial fill
B = 0.01 × E                         # $1,000 on the first $100k basket
S = triggering forecast's final P01  # fixed stop
W = final P99 − final P01            # fixed sizing reference

loss_per_lot[k] = USD(contract_size × max(entry_price[k] − S, W))
                + entry_commission_per_lot[k]
                + stop_exit_commission_per_lot[k]
                + seven_day_adverse_swap_reserve_per_lot[k]

remaining_budget = max(0, B − existing_entries_reserved_stop_risk)
scale = remaining_budget / Σ(weight[k] × loss_per_lot[k])
next_lots = floor_to_volume_step(scale × next_weight)
```

The `max()` prevents an entry above final P99 from being undersized for its larger actual loss to the stop. USD conversion uses the appropriate quoted currency and conservative conversion side. Lot quantities round **down**, are bounded by margin and broker maximum volume, and are skipped if below the assumed minimum. The entire planned ladder shares B; each entry does not receive another 1% budget.

The budget stays fixed for the basket. The next basket compounds from its new account equity. Currency conversion moves, gap fills and carrying beyond the swap reserve can produce losses beyond the planned budget; reported equity includes these effects.

The public FTMO endpoint does not publish minimum lot/step fields. These runs assume 0.01 minimum/step and explicitly mark execution feasibility unverified. Current advertised contract sizes, costs, leverage and maximum volumes are frozen in the plan; they are not verified historical broker specifications. Some `.sim` products launched during the replay year, so pre-launch results are counterfactual research, not a claim they were executable then.

## Averaging and exits

1. The first averaging buy must be at least one grid below the lowest existing fill and strictly below the latest available daily forecast's first P90. The P90 gate can make that first gap larger than one grid.
2. After that averaging fill, subsequent entries need only lower grid levels. P90 no longer gates the basket.
3. A completed minute must confirm the lower-price breach before a limit order exists. That order is eligible from the next observed minute. No retrospective fill at the low that created the order.
4. A resting buy limit above the stop fills before the stop on a continuous descent; a gap below the stop closes the basket at the actual available bid before adding exposure.
5. Trailing requires both positive basket P&L (including entry/exit commissions and accrued swaps) and executable bid at or above `lowest fill + trailing distance`. A single $666.82 entry with a $2 grid therefore cannot arm its trail before bid reaches $668.82. The fixed final P01 remains active while trailing is unarmed.
6. With at least two open entries, trailing distance is `0.75 × (highest fill − lowest fill)`. With one entry it is one grid. Once armed, the stop follows the favorable quote minus that distance, stays at least one executable tick above the lowest fill, and never loosens. It must remain at least one tick behind the quote; very small spans may need a larger move to form a valid stop. The more protective of the trail and fixed P01 closes the basket. Short research mirrors the rule below the highest fill.
7. After exit, reentry requires a later daily forecast with a new above-first-P99 signal. The same origin cannot reopen the basket.
8. Baskets carry until stop/trail. At the end of the year, open liabilities remain bid-marked, including estimated exit commission.

Earlier reports used immediate positive-P&L activation and can stop a one-leg basket before its first averaging level. That behavior is retained only as the explicit `legacy_basket_profit` replay option for reproducibility. Corrected results record `trailing_rule=extreme_entry_distance`; prior returns must not be presented as results of the corrected rule.

## Grid and lot comparisons

Each asset has six prespecified grids and three profiles: equal size, larger deeper entries, and smaller deeper entries. Profile weights range from 1 to 2; they are not martingale multipliers. All share the same 1% ceiling.

| Asset | Price grids |
|---|---|
| EURUSD, GBPUSD, USDCAD | 0.0005, 0.001, 0.002, 0.005, 0.01, 1 |
| USDJPY, GBPJPY | 0.05, 0.1, 0.2, 0.5, 1, 2 |
| US500, US30, US100 | 1, 5, 10, 20, 50, 100 |
| XAUUSD | 0.25, 0.5, 1, 2, 5, 10 |
| BTCUSD | 1, 25, 50, 100, 250, 500 |

The literal $1 grid is retained for every asset; FX pip distances are included because a raw one-dollar FX move is usually an impractical ladder interval. Very dense ladders may have insufficient budget to execute even the minimum lot: those blocked entries remain visible.

## Validation and reporting

Genuine paired M1 bid/ask candles execute the replay. Both low-first and high-first minute OHLC paths are reported because OHLC does not reveal tick order or the simultaneity of bid/ask extrema. Model runtime delays are measured and no source data after the cutoff enters training.

Candidate selection uses the first nine months only, maximizing the worse-path development net return among candidates with at least five closed baskets and no diagnosed daily, static overall or margin breach. The last quarter is then replayed from flat without selecting again. Every candidate's full-year result is preserved; an ineligible selection remains explicit.

Diagnostics include the standard 2-Step $90,000 static account floor and $5,000 daily loss relative to midnight CE(S)T balance, including floating results. These are reported breaches, not simulated automatic liquidation rules. Commission and adverse spread/swap stress results accompany the selected candidate.

Reports include net and percentage returns, fees, swaps, realized entry/basket W/L, win rate, longest W/L streaks, weighted average entry, duration, average/maximum ladder, maximum lots/margin, open liabilities, bid equity drawdown and daily loss.

All inputs, configuration, source checksums, daily forecasts and reports are GitHub artifacts. Model jobs are split into 21-calendar-day shards. `resume_run_id` can reuse a prior run's complete source artifacts and finished forecast records while retaining the original cost snapshot. Missing forecasts prevent an optimization report. Source access challenges and malformed/empty provider responses fail explicitly; access controls are not bypassed.
