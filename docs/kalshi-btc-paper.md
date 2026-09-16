# Kalshi BTC first-two-minute paper research

This is private, read-only research, not a real-money trader. No exchange order
endpoint or account key is available to this worker.

See the newer [sticky-direction taker/recursive ladder tracking](kalshi-btc-sticky-tracking.md)
for independent minute/settlement collection, bounded retries and checkpoint-safe
upgrades. Legacy scenarios below keep their original fee assumptions.

## Exact prospective window

The provider adapter discovers `KXBTC15M`, verifies binary contracts whose official
open/close times are exactly 15 minutes apart, and requires observed candle closes
at **open + 1 minute** and **open + 2 minutes**. Missing or synthetic candles are
not substituted. Both YES and NO use the same two completed minutes. NO ask is
1 minus YES bid; NO bid is 1 minus YES ask. Current fixed-point dollar fields and
archived dollar fields are normalized without mixing cents and probabilities.

The existing ensemble service forecasts the remaining **13 minutes**, sequentially
using compatible approved models. Toto's genuine-history minimum is 32, so it is
excluded, not padded. TimesFM still requires the configured commercial/access
flags. Actual participants, failures, per-quantile weights, versions and input
snapshots are saved. Two-point forecasts are numerically experimental and have
no established predictive reliability.

Both sides must finish before their common publication timestamp. Paper entries
can occur only on later observed quotes. Starting after the third minute records
`missed_start`, not a backdated forecast. Inference exceeding market close records
`missed_deadline`. A crash before atomic publication records an interrupted origin
on recovery instead of reusing a fictitious earlier availability time.

## Independent paper comparisons

For each game and frozen forecast origin, compare:

- Ask strictly below P1.
- Ask strictly above P1 and strictly below P10 (exclusive middle bucket).
- Ask strictly below P10 (includes the below-P1 bucket; do not add their counts).

Each entry bucket has independent P90 and P99 exits. Buy the first qualifying
observed ask, one contract, then exit when a **later bid** reaches the time-aligned
forecast target above entry cost. Only one side may be held per game/origin/
experiment. Simultaneous opposite-side signals are excluded as ambiguous. No
same-bar entry/exit or interpolated fills. Every signal is counted, not only
signals later followed by a successful rally.

Compare no stop against an **absolute $0.51 stop**. The latter requires the bid
already above $0.51 at entry. Signals at/below that price are not valid stop-entry
setups. A later bid at/below $0.51 exits at that observed bid, including adverse
gaps—not an assumed $0.51 fill. This is not a $0.51 loss from entry.

If the target is not hit, a genuine final-minute bid provides the defined time
exit. Missing final quotes leave the position censored with its available mark.
The comparison assumes 1% of notional on each side as a **research cost parameter**,
not Kalshi's verified fee schedule, and does not model depth or queue priority.

Reports distinguish target-hit rate, net-profitable-trade rate, net P&L/closed
entry cost, open/censored marks, and provider-confirmed eventual YES/NO wins after
a target hit. Resolved game-side counts deduplicate overlapping forecasts.
Independent comparisons are not a combined portfolio return. Historical profits
do not guarantee future results.

## Shared Polymarket experiment

`quantile_paths.py` also evaluates the same comparisons against the immutable
five-model Polymarket 5/15/30-minute forecasts and observation tape. These are
separate from the P1 minute-accumulation portfolio. Each refreshed forecast is a
new frozen experiment; the P1 accumulation book retains its existing hold policy.
Two-side and six-side soccer games require the complete side set. YES/NO contract
wins are not conflated with a particular team's outright victory.

The live paper artifact rebuilds its chronological ledger from immutable observed
quotes every checkpoint. These are simulated bid/ask executions, not exchange
orders or guaranteed real-time fills. Original input and forecast data remain
unchanged. Outcomes are collected after forecast publication, never model inputs.

## Workflows and secure recovery

`kalshi-btc-paper.yml` supports manual live or bounded recent-history runs, mock
safety tests (no model downloads), and actual inference. It runs on a GitHub-hosted
CPU with a locked Python 3.12 environment. No GPU is assumed. Inputs contain only
enums, bounded counts, checkpoint IDs and validated code references.

The worker uses the existing `LocalStore` SQLite, `node/run.mjs` artifact uploader,
and AES-256-GCM encrypted ZIP format. No Firestore persistence is added. Immutable
forecasts, input snapshots, quote tape, resolution evidence and progress survive
recovery. Reports include `btc_forecast_quantiles.csv.gz`,
`quantile_path_trades.csv.gz`, `quantile_path_report.json` and the SQLite record.
Every file has a manifest checksum. Secrets/model caches are excluded.

Secrets: existing `HF_TOKEN` and `QUANTURA_RESEARCH_ARTIFACT_KEY`. Model flags:
`TIMESFM_COMMERCIAL_LICENSED`, `TIMESFM_HF_ACCESS_APPROVED`. Enable automatic
continuation using repository variable `KALSHI_BTC_PAPER_ENABLED=true` only after
validating a manual live lineage. `continuous=true` requests a checkpoint-confirmed
successor after the bounded worker. `kalshi-btc-paper-watchdog.yml` checks every
15 minutes and resumes the exact checkpoint code, never silently starting over.

Checkpoints upload every five minutes and at exit, retaining the latest two per
run for three days. Runner setup, model loading, provider failures, GitHub queues,
scheduled-job delays and handoffs can create gaps. **Uninterrupted 24/7 execution
is not guaranteed.** The local 20 MiB guard stops safely for a new archival shard;
it does not erase history. Keep required artifacts before their retention expires.

Official sources: [Kalshi candles](https://docs.kalshi.com/api-reference/market/get-market-candlesticks),
[market metadata and result](https://docs.kalshi.com/api-reference/market/get-market),
[historical tiers](https://docs.kalshi.com/getting_started/historical_data).
Provider redistribution rights still require separate review.
