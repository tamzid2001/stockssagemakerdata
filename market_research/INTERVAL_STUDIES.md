# Fifteen-minute markets: comparable minute-origin research

`kalshi-interval-minutes.yml` collects paired completed-minute YES/NO bid/ask
quotes independently of model inference. Discovery checks actual open binary
contracts with exactly 900 seconds between open and close, in Kalshi Crypto or
Commodities series with `frequency=fifteen_min`. Coin Race/relative-performance
series are excluded from directional price studies. Inactive or unavailable
series are reported, not populated with invented records.

`kalshi-interval-studies.yml` runs the same three independent strategies for each
series at each origin: first 1–12 completed minutes → remaining 14–3 minutes.
Each source market is downloaded once and frozen. Every origin contains exactly
the first N genuine minute observations; missing observations do not slide the
window forward. Flat observed windows are allowed without added variation.

## Models and strategies

First minute: Granite PatchTST-FM, Chronos-2, TimesFM, equally weighted. Origins
2–12 additionally include Prophet. Toto is excluded. All selected models must
succeed; TimesFM's production access and commercial-license flags still apply.
Equal raw weights are renormalized per quantile. TimesFM participates only within
P10–P90; P1/P99 use Granite + Chronos at the one-minute origin and additionally
Prophet at subsequent origins. Unsupported tails are never extrapolated.

- **First P90:** first unambiguous completed-minute bid at/above that minute's
  forecast P90, then the following minute's ask, hold to official settlement.
- **P90 + sticky direction:** the same first signal must agree with the opposite
  of the latest confirmed settlement **in that series**. Every settlement updates
  direction, including markets without a forecast or trade. No later P90 signal
  substitutes for a first signal that disagreed.
- **First quote below P10:** compare the first completed minute after both
  forecasts are available with the frozen first forecast row. Strict ask < P10
  selects that side; entry uses the following minute's ask, then settlement.

Missing paired signal quotes and simultaneous two-side signals are explicit
misses, not filled using a later or more profitable side. These hold-only studies
do not switch at P10 or use ladders. The sports switch experiment is a separate
workflow and result population.

Fixed-one-contract and 2.5× recovery results are independent scenarios. Recovery
starts at 1; after a confirmed loss while cumulative cycle P&L remains negative,
the next size is floor(previous size × 2.5), capped at 100. It resets only when
cycle P&L recovers to zero. No cross-series or cross-origin pooling occurs.

## Timing and honest coverage

Historical inference is not presented as prospective trading. Publication uses
the measured sequential inference duration plus a five-second quote-delay
assumption. Later model completion never allows a backdated entry. Settlement
knowledge uses official settlement time when available, with an explicit
60-second post-close floor. First-received live minute timestamps and later
provider revisions are stored separately by the independent collector.

Results distinguish signal count, missed entries, settled wins/losses, pending
positions, and model/data failures. No trades means unavailable win rate, not
zero percent. Fee-adjusted P&L is a sensitivity using the observed current
series quadratic fee schedule, not a claim of historical or live fills. Unknown
fees suppress net-return scenarios. Quote snapshots do not prove orderbook depth,
execution latency, partial fills, or profitability at 100 contracts.

## Storage and continuation

The existing private encrypted research bucket stores immutable source markets,
forecast pairs, strategy ledgers, and reports. Firestore contains only small
hashes, generation-pinned pointers, and worker leases. No full research database
is uploaded to GitHub artifacts. The collector snapshots its active working set
every five minutes and archives completed markets separately before pruning
their local copies. It retains pending markets and missing-minute metadata.

Collector runs use a 330-minute budget, explicit successor dispatch, and an
hourly recovery schedule gated by `KALSHI_INTERVAL_RESEARCH_ENABLED=true`.
GitHub scheduling/worker failures can leave gaps; uninterrupted exchange capture
is not guaranteed. A persistent collector is preferable if gap-free coverage is
required.

Studies freeze the series, cutoff, market count and 40-character code SHA. Two
CPU jobs run concurrently. Incomplete jobs continue from immutable per-origin
checkpoints, not from newly sampled cohorts. Start with `max_markets=2` for a
real-model smoke cohort; this is not all-history coverage. Saved failures and
missed deadlines remain visible, never silently counted as successful forecasts.

Official provider references: [series discovery](https://docs.kalshi.com/api-reference/market/get-series-list),
[minute candlesticks](https://docs.kalshi.com/api-reference/market/get-market-candlesticks),
[historical tier](https://docs.kalshi.com/getting_started/historical_data).
Provider redistribution permissions remain `review_required`.
