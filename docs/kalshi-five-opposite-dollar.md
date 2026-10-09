# Five Kalshi opposite-dollar strategies

**Operational status, October 9, 2026:** the owner paused all five traders and their recovery workflow before permanently deleting Cloud Storage files, including journals and archives. Existing exchange positions remain at the exchange. `QUANTURA_KALSHI_DOLLAR_STORAGE_PURGED=true` blocks a restart until exchange reconciliation and durable state recovery are reviewed. The design below describes the strategy; it is not an assertion that a worker or its deleted history is currently available. See [cloud cost controls](cloud-cost-controls.mdx).

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

P90 is the 90th percentile of the forecast price, not a fixed 90-cent quote or a win probability. A 20-cent source bid can exceed an 18-cent P90; its opposite ask can then be 80 cents. Prices can also change between the signal minute, the next-minute entry and later IOC retries. Buying the opposite side does not impose a low-price ceiling. The existing 99-cent order guard remains in force. The manual `kalshi-five-entry-audit.yml` workflow reads the encrypted current journals and recent immutable archives without acquiring leases or receiving an exchange private key. It reports the signal bid/P90, sticky/source/traded sides, signal-price complement, next-minute opposite ask and actual cost-per-contract fill average.

Live entries use V2 IOC limit orders at the current price, refreshed after account admission. The worker targets a one-second reconciliation/retry cadence while the market remains open. Network delay, rate limiting and read-model delay may lengthen that cadence. Every retry follows a terminal reconciliation of the previous order, has a distinct deterministic client ID, and sizes only the unspent portion of the $1 notional budget. An unknown delivery is retained and reconciled; it cannot cause a duplicate order. Fractional quantities and dynamic `price_ranges` are validated. IOC limits can still go unfilled; retries end at market close.

Durable intents distinguish `prepared` (no POST can have occurred) from `submitting` (delivery may be uncertain). Recovery may clear a prepared intent; submitting and older unstaged intents require exchange reconciliation. The worker checks lock ownership and the market cutoff again immediately before submission. Known pre-submission failures clear the unsent intent when fenced storage remains writable.

A shutdown signal blocks further IOC submissions and saves the recovery buffer. A worker releasing an expired account gate cannot overwrite a successor's lease. Confirmed fills are applied once and official settlement accounting is idempotent. Heartbeats include the execution SHA so the running approved version is visible.

Startup waits up to three minutes for a canceled worker's unexpired journal lease, without overriding it. Every acquisition uses fresh metadata and a generation-matched write. Separate run attempts have separate holder identities. A worker that still owns a valid lease cannot be evicted by a replacement.

Paper entries use observed current asks and modeled series taker fees; depth and queue priority are not execution verified. Live P&L uses authenticated terminal order cost/fees and matching account settlement records. Paper and live entries share each causal signal and keep separate accounting.

## Per-asset performance logs

Every minute, each of the five workers emits a JSON heartbeat with independent `performance.paper` and `performance.live` results, followed by a readable summary for each mode. Each filled settlement also emits `opposite_dollar_trade_settled` with its side, contracts, average fill price, cost, fees, payout, net result and duration.

| Metric | Meaning |
|---|---|
| Closed trades, wins/losses, win rate | Filled positions with confirmed settlement; wins use net P&L after fees. Zero-fill attempts are excluded and breakevens are separate. |
| Net/gross returns and fees | Lifetime closed-trade cash flows for that asset and mode. Live uses authenticated exchange records; paper uses the modeled fill. |
| Return on closed entry spend | Net profit divided by cumulative closed-trade entry cost plus fees. This measures turnover, rather than account investment return. |
| Current/maximum W/L streaks | Consecutive confirmed positive/negative net settlements. Breakevens reset both streaks. |
| Maximum realized drawdown | Peak-to-trough decline of cumulative closed-trade net P&L, including the initial zero baseline. |
| Sampled bid equity drawdown | Decline observed at complete minute heartbeat snapshots since statistics tracking began; includes open entry cost and fees. Historic and intraminute equity drawdown cannot be inferred from settled P&L. |
| Average entry | Mean trade fill cost divided by contracts, in cents. A separate volume-weighted average uses total cost divided by total contracts. |
| Duration | Paper entry receipt or first live fill confirmation to confirmed settlement. Legacy trades without fill receipts use the entry-intent timestamp, explicitly identified in `duration_sources`. |
| Open exposure | Position count, contracts, entry cost, fees, average entry, fresh-bid unrealized P&L and pending order intents. Missing or stale marks are reported as unavailable. |

At the first upgrade, immutable encrypted archives and the current journal rebuild the lifetime statistics, deduplicated by market and mode. The result must match the existing lifetime trade/net/fee totals before historical averages, streaks or drawdown are presented as complete. Missing history is labeled incomplete; known lifetime totals remain visible. Compact aggregates then persist in the same fenced journal write as settlement accounting. Worker handoffs, partial fills and duplicate settlement checks cannot reset or double-count performance. No account balance, key or credential is logged; no Firestore requests are introduced.

## Durable operation

`kalshi-five-opposite-trader.yml` runs one selected series for up to 300 minutes. Each series queues its own successor after saving and releasing its journal. The five series have independent workflow concurrency and encrypted GCS leases. A shared GCS account admission gate verifies exact ownership of every position across all five series before each live submission. Unexplained positions, resting orders or ambiguous fills block new exposure. Journal generations fence stale writers. Intents are durable before POST. Completed receipts and execution evidence are archived before the recovery buffer is retired. The new worker has no Firestore reads or writes.

Approval requires `QUANTURA_KALSHI_DOLLAR_LIVE_ENABLED=true`, the exact approved execution SHA and the selected portfolio configuration fingerprint. `QUANTURA_KALSHI_DOLLAR_ENABLED=true` controls workers/handoffs; `QUANTURA_KALSHI_DOLLAR_WATCHDOG_MODE=both` selects concurrent paper and live accounting. Existing BTC/coin live gates and watchdogs remain disabled when this portfolio is enabled. The separate BTC minute research flag is disabled during replacement; its existing research archives remain available.

Hosted-runner setup and handoffs can miss opening minutes. Continuous recovery does not guarantee uninterrupted exchange observation. The collector and execution loop continue while the numerical child performs inference, and official receipt times prevent historical observations being mislabeled as live.

Concurrent storage reads retry fresh metadata and a generation-matched download if another worker replaces the object between those requests. An obsolete-generation 404 is never treated as an empty journal. Repeated contention pauses admission with `TRADER_SNAPSHOT_BUSY`; ownership checks, generation-matched writes and pending order reconciliation still apply.

Storage requests use a three-second request timeout and a five-second SDK retry window, rather than the SDK's default 120-second retry window, which can exhaust the journal lease. The final request can extend beyond that retry window. Each upload attaches a random receipt to its atomic object metadata. If the response is lost, fresh metadata must confirm that exact receipt before the worker adopts the committed generation. A different worker's receipt cannot acknowledge an upload; generation preconditions remain mandatory.

Short upload failures get at most one additional upload after fresh metadata proves the original generation is unchanged (or a create-only object remains absent). Both attempts use identical encrypted bytes, receipt and generation precondition. The recovery scheduling budget is 12 seconds, with request and SDK retry limits reduced to the remaining budget. An in-flight request can extend past that budget; it is not a hard process deadline. An expired owner lease blocks the additional attempt. An unknown commit, a successor's generation, an authorization error or an exhausted budget stops admission. Recovery logs include a safe upstream exception class and `failure_kind` (`timeout`, `connection`, `http`, `retry_exhausted` or `unknown`); a network error can legitimately have no HTTP status. Raw provider error text is never logged. This follows [Cloud Storage's conditional retry guidance](https://docs.cloud.google.com/storage/docs/retry-strategy).

When storage stays unavailable or a write cannot be confirmed, the worker stops new orders and exits with `TRADER_STORAGE_UNAVAILABLE`, the operation and upstream status. It stops collection/inference and closes the broker client. Aborted transactions do not perform another shutdown checkpoint or unlock, and cleanup errors cannot replace the original error. The lease expires naturally; the watchdog starts a successor that restores the durable journal and reconciles any submitted intent before another order. An outage can therefore interrupt trading, but it cannot authorize trading without durable ownership or reset the existing lifetime statistics. Uploads confirmed after a lost response log `opposite_dollar_storage_ack_recovered`.

The `verify` workflow mode tests the current main commit without sending orders. Paper/live workers continue to check out the immutable approved SHA until the verified revision is approved for execution.

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
