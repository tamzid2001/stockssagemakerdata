# Five Kalshi opposite-dollar strategies

The continuous portfolio replaces the BTC strategy with the five selected timings. Each series runs independently, so all five can hold positions simultaneously. The initial stake is $1 of contract notional per 15-minute market, before fees. Quantities are floored to 0.01 contracts. There is no recovery multiplier and no price stop: positions hold until official settlement.

| Series | Genuine opening minutes | Forecast remaining minutes |
|---|---:|---:|
| Silver · KXSILVER15M | 8 | 7 |
| SOL · KXSOL15M | 7 | 8 |
| WTI · KXWTI15M | 7 | 8 |
| DOGE · KXDOGE15M | 3 | 12 |
| HYPE · KXHYPE15M | 9 | 6 |

Every forecast uses Prophet, Granite, Chronos and TimesFM, the original study's quantile grid and first N genuine completed-minute bid/ask receipts. Receipts more than 30 seconds after the minute close cannot supply opening history or signal/initial entry quotes. The whole YES/NO forecast pair becomes available at actual inference completion. Failed models, missing history and late inference are recorded; missing bars are never invented.

The first unambiguous completed-minute bid at or above that side's P90 is latched. It must agree with sticky direction, which is the opposite of the latest prior official binary settlement known before the signal. The trader then buys the opposite side at the next minute's current ask. A disagreement excludes the market; later signals cannot replace its first decision. For example, an agreeing YES P90 signal buys NO. Its executable ask is derived from the current market response, rather than assuming 1 minus the YES ask.

Live entries use V2 IOC limit orders at the current price, refreshed after account admission. The worker targets a one-second reconciliation/retry cadence while the market remains open. Network delay, rate limiting and read-model delay may lengthen that cadence. Every retry follows a terminal reconciliation of the previous order, has a distinct deterministic client ID, and sizes only the unspent portion of the $1 notional budget. An unknown delivery is retained and reconciled; it cannot cause a duplicate order. Fractional quantities and dynamic `price_ranges` are validated. IOC limits can still go unfilled; retries end at market close.

Durable intents distinguish `prepared` (no POST can have occurred) from `submitting` (delivery may be uncertain). Recovery may clear a prepared intent; submitting and older unstaged intents require exchange reconciliation. The worker checks lock ownership and the market cutoff again immediately before submission. Known pre-submission failures clear the unsent intent when fenced storage remains writable.

A shutdown signal blocks further IOC submissions and saves the recovery buffer. A worker releasing an expired account gate cannot overwrite a successor's lease. Confirmed fills are applied once and official settlement accounting is idempotent. Heartbeats include the execution SHA so the running approved version is visible.

Paper entries use observed current asks and modeled series taker fees; depth and queue priority are not execution verified. Live P&L uses authenticated terminal order cost/fees and matching account settlement records. Paper and live entries share each causal signal and keep separate accounting.

## Durable operation

`kalshi-five-opposite-trader.yml` runs one selected series for up to 300 minutes. Each series queues its own successor after saving and releasing its journal. The five series have independent workflow concurrency and encrypted GCS leases. A shared GCS account admission gate verifies exact ownership of every position across all five series before each live submission. Unexplained positions, resting orders or ambiguous fills block new exposure. Journal generations fence stale writers. Intents are durable before POST. Completed receipts and execution evidence are archived before the recovery buffer is retired. The new worker has no Firestore reads or writes.

Approval requires `QUANTURA_KALSHI_DOLLAR_LIVE_ENABLED=true`, the exact approved execution SHA and the selected portfolio configuration fingerprint. `QUANTURA_KALSHI_DOLLAR_ENABLED=true` controls workers/handoffs; `QUANTURA_KALSHI_DOLLAR_WATCHDOG_MODE=both` selects concurrent paper and live accounting. Existing BTC/coin live gates and watchdogs remain disabled when this portfolio is enabled. The separate BTC minute research flag is disabled during replacement; its existing research archives remain available.

Hosted-runner setup and handoffs can miss opening minutes. Continuous recovery does not guarantee uninterrupted exchange observation. The collector and execution loop continue while the numerical child performs inference, and official receipt times prevent historical observations being mislabeled as live.

Concurrent storage reads retry fresh metadata and a generation-matched download if another worker replaces the object between those requests. An obsolete-generation 404 is never treated as an empty journal. Repeated contention pauses admission with `TRADER_SNAPSHOT_BUSY`; ownership checks, generation-matched writes and pending order reconciliation still apply.

## cron-job.org recovery settings

The built-in recovery workflow runs every five minutes. An external cron can invoke the same idempotent recovery check:

- URL: `https://api.github.com/repos/tamzid2001/stockssagemakerdata/actions/workflows/kalshi-five-opposite-watchdog.yml/dispatches`
- Method: `POST`
- Schedule: every five minutes (`*/5 * * * *`), UTC or any timezone; it runs around the clock.
- Headers: `Authorization: Bearer YOUR_GITHUB_TOKEN`, `Accept: application/vnd.github+json`, `Content-Type: application/json`, `X-GitHub-Api-Version: 2022-11-28`.
- Body: `{"ref":"main"}`
- Timeout: 30 seconds.
- Expected successful HTTP response: `204 No Content`.

Use a repository-scoped GitHub fine-grained token with Actions read/write access to `tamzid2001/stockssagemakerdata`. The Kalshi private key is never supplied to cron-job.org. A 204 means the watchdog was queued, not that a trade filled. The watchdog checks each series separately and dispatches only when it has no active or queued worker. Duplicate checks are serialized. Disabling the portfolio gate makes recovery a no-op.

## Original selected-cohort replay

`kalshi-five-opposite-report.yml` authenticates the original immutable campaign manifests, report hashes and encrypted per-market quote archives. It uses the original selected timings without rerunning models. Entry cost/fees are debited when observed and settlement proceeds credited only at the recorded official confirmation. Entries debit before credits on timestamp ties. The report recomputes combined bid equity and cash across the five overlapping series.

For the September 16–19, 2026 cohort: 526 trades, 159 wins / 367 losses, 30.23% win rate; +$435.6409 net. Settled P&L drawdown was $33.2081; observed-minute bid equity drawdown $33.98929; cash drawdown including locked positions $35.75. The historical starting cash needed to fund that sequence was $21.04, with profits reinvested and fees included. Up to five positions were open together. Intraminute drawdown and actual ask-fill depth are unknown. Timings were selected in sample. These values are historical sample results, not a guaranteed future bankroll or live return.

Protocol references: [V2 orders](https://docs.kalshi.com/api-reference/orders/create-order-v2), [fixed-point quantities and price grids](https://docs.kalshi.com/getting_started/fixed_point_migration), [order direction](https://docs.kalshi.com/getting_started/order_direction), [rate limits](https://docs.kalshi.com/getting_started/rate_limits).
