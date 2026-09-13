# Live game research and watchdogs

These workers simulate orders. They never connect to an exchange order API.
Neither forecast quantiles nor paper fills establish a profitable live strategy.

## P1 OCO Polymarket paper worker

`polymarket-live-paper.yml`, strategy `p1_oco`, provider `polymarket_us`, lag `0`,
horizon `5`, `15`, or `30`, real mode, uses the existing SQLite/encrypted-artifact research store.
Each game requires both moneylines (all six YES/NO sides for drawable soccer),
**at least 32 genuine observations per side**, using the latest available up to 500, and all
five model participants. The shared ensemble uses equal base weights and
renormalizes tails without Toto/TimesFM. Missing observations are not fabricated.

For each independent P10/P25/P50/P75/P90/P99 exit experiment:

- Use the terminal P1 of the selected 5/15/30-minute forecast as a buy limit, rounded
  down to 0.0001 probability units (one hundredth of a cent).
- Add **one new tranche every observed minute** while the observed ask is strictly above that limit. Buy at or below
  the limit on a later observed minute, conservatively charging the limit price.
- First fill cancels opposing buys. Hold only one side per game per experiment.
- Subsequent minute limits are only for the held side; filled limits add to the
  quantity and update volume-weighted average cost. Still-pending same-side
  tranches remain eligible until replaced by a forecast or until its horizon ends.
- Default tranche is one share; filled plus pending shares cannot exceed 100 on
  the same side. Opposing pending alternatives are mutually exclusive paper OCO.
- Same-minute opposing fills are ambiguous and excluded, never cherry-picked.
- Place a sell at the experiment's actual terminal quantile, rounded up. Both
  that quantile and its rounded limit must exceed weighted average entry cost.
  Otherwise suspend the sell until a newer forecast supplies a valid target;
  never manufacture a target by raising it to cost plus a tick. The modeled
  fee is 1% of entry/exit notional (an assumption, not an exchange fee claim).
  Fees can still make an above-cost closed trade unprofitable.
- No stop loss or forced horizon liquidation. Carry the position into the next
  forecast and update its sell. After exit, obtain fresh paired forecasts before
  rearming. No same-bar exit/re-entry.

Forecasts refresh after exits or every selected horizon, subject to actual sequential
model latency. An independent minute observer checks already armed books while
the five models run sequentially. P1 tranches are added each observed minute within each active
forecast and expire at its original target timestamp. Minute quotes have no queue priority, depth, tick-by-tick ordering,
partial-fill or guaranteed OCO information. These are quote-triggered simulations,
not verified exchange fills. Closed games without a simulated exit stay open /
unresolved rather than receiving invented settlement proceeds.

P1 books, input snapshots, forecasts, chronological orders/fills, and reports are
stored in runner-local SQLite, **not Firestore**. State and corresponding trade
transitions commit atomically. Online backups are encrypted using
`QUANTURA_RESEARCH_ARTIFACT_KEY` and uploaded after the first minute, every five
minutes, and at completion/failure. The latest two verified checkpoints per run
are retained for three days; never upload plaintext or model caches.

Each encrypted ZIP contains a SHA-256 manifest, `research.sqlite3`,
`p1_forecast_quantiles.csv.gz`, `p1_orders_and_fills.csv.gz`, and `p1_summary.json`.
The CSVs are rebuilt from the consistent database backup, not copied mid-write.
`p1_path_outcomes.json` separately tracks at/below-P1 and strictly-below-P10
entries followed by P90/P99 bid reaches **inside that same forecast horizon**.
Targets are frozen at forecast publication; later forecasts do not move an
episode's target after observing the path. Cohorts are split by 5/15/30 minutes.
Repeated rolling signals are reported as episodes and also deduplicated by
game/contract for the eventual-win denominator. A short soccer contract resolving
YES means "not this outcome", not necessarily that the opposing team won.

Only provider-confirmed `MARKET_STATUS_RESOLVED`, matching side identity and
binary [settlement evidence](https://docs.polymarket.us/api-reference/markets/get-market-settlement)
can label a selected contract a winner. Pending, void/partial, unavailable and
conflicting outcomes stay unknown. No final quote or P99 reach establishes a win.
Reports show open shares/cost/available bid marks as well as closed win rates.
`net_return_on_closed_cost` is net realized P&L divided by closed-entry cost,
**not a portfolio return**; it excludes remaining open risk. No trades means
null win rate and null return, not zero or a successful trading sample.

To resume, dispatch the workflow with `resume_artifact_id` from its checkpoint
link and `code_ref` equal to that checkpoint's immutable commit. Model/data
records and pending limits are restored; replayed observations cannot count a
fill twice. `resume_artifact_id=0` explicitly starts a new lineage. The old v1
non-averaging Firestore experiment is not silently combined with this version.

## Hourly screeners

### Historical P1 replay

Use `historical-p10-replay.yml` with `strategy=p1_oco`, `provider=polymarket_us`,
`lag_minutes=0` and `horizon=5`, `15`, or `30`. It runs the same minute-order,
average-cost and fixed-quantile exit functions, rolling by the chosen horizon.
Legacy dispatcher `roll_minutes` and `loss_multiplier` are not applied to this
fixed-tranche preset. Only previous observations enter each forecast. Measured
inference latency delays simulated order publication; busy/expired origins are
not silently traded retroactively. Quantiles and resumable books stay in the
encrypted artifact. `resume_artifact_id` and original `code_ref` continue a replay.
Counts explicitly distinguish discovered, selected, skipped, incomplete and
processed games. A bounded sample does not establish performance across all games.

`hourly-game-screener.yml` runs at minute 3 of every hour, with independent
Polymarket and Kalshi jobs (`fail-fast: false`). It uses the last 500 observed
inputs ending 15 minutes before the analysis time and forecasts 75 minutes from
that cutoff. The first 15 minutes are replay overlap, not unseen outcomes.

Both jobs run on GitHub-hosted CPU machines, not dedicated GPUs. A 45-minute
processing budget / 60-minute job timeout bounds each scan; up to 500 eligible
sides are considered. Coverage and skipped sides are reported explicitly.
Full all-game coverage within an hour is not guaranteed by this infrastructure.
No successful qualifying forecasts makes the job fail visibly.

Each output is a versioned encrypted ZIP artifact containing a checksum manifest,
SQLite checkpoints and compressed forecast CSV. Retention is three days; download
before expiry. Raw provider redistribution remains subject to licensing review.

## Scheduling and external recovery

Repository variables:

- `POLYMARKET_PAPER_ENABLED=true` enables P1 OCO handoff/recovery.
- `HOURLY_GAME_SCREENER_ENABLED=true` enables hourly scheduled scans.
- `KALSHI_PAPER_ENABLED` controls the existing separate Kalshi paper monitor.

The Polymarket process runs up to 345 minutes, uploads its final checkpoint,
then dispatches a successor pinned to that checkpoint's code and configuration.
The recovery workflow checks every ten minutes and requires a verified P1
artifact before resuming; it does not silently reset missing/expired state.
The encrypted artifact budget is 25 MiB (20 MiB SQLite stop threshold). Reaching
that limit stops visibly for archival/sharding instead of deleting prior results.
Runner acquisition and model installation take time: this is continuously
rescheduled operation, **not guaranteed gap-free 24/7 execution**.

External cron service configuration (no credentials in URL):

| Purpose | Method / URL | Schedule | Expected response |
| --- | --- | --- | --- |
| Website/API health | GET `https://quantura.studio/api/health/watchdog` | Every 5 minutes | 200, otherwise alert |
| Research workflow health | GET `https://quantura.studio/api/health/market-research` | Every 5 minutes | 200, otherwise alert |
| Recover missing workers | POST `https://quantura.studio/api/internal/market-research/watchdog` | Every 10 minutes | 202 |

Recovery requires `Authorization: Bearer <QUANTURA_RESEARCH_WATCHDOG_TOKEN>`.
Use the dedicated server-side value, not a personal API key or an exchange key.
Store it only in the cron service's secret header setting. Empty body, 30-second
timeout, no query-string credentials. Recovery cannot choose arbitrary workflows,
checkpoints, model IDs or order endpoints; it dispatches only approved paper jobs.
The health endpoint measures workflow liveness, not market coverage or trade quality.
