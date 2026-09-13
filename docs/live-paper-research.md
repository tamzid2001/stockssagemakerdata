# Live game research and watchdogs

These workers simulate orders. They never connect to an exchange order API.
Neither forecast quantiles nor paper fills establish a profitable live strategy.

## P1 OCO Polymarket paper worker

`polymarket-live-paper.yml`, strategy `p1_oco`, provider `polymarket_us`, lag `0`,
horizon `30`, real mode, runs through the existing fenced durable worker store.
Each game requires both moneylines, 500 genuine observations per side, and all
five model participants. The shared ensemble uses equal base weights and
renormalizes tails without Toto/TimesFM. Missing observations are not fabricated.

For each independent P10/P25/P50/P75/P90/P99 exit experiment:

- Use the terminal P1 of the latest 30-minute forecast as a buy limit, rounded
  down to 0.0001 probability units (one hundredth of a cent).
- Arm only while the observed ask is strictly above that limit. Buy at or below
  the limit on a later observed minute, conservatively charging the limit price.
- First fill cancels opposing buys. Hold only one side per game per experiment.
- Same-minute opposing fills are ambiguous and excluded, never cherry-picked.
- Place a sell at the experiment's terminal quantile, rounded up. It must exceed
  average entry cost by at least one simulated tick, even if the next forecast
  drops. Fees can still make a closed trade unprofitable.
- No stop loss or forced horizon liquidation. Carry the position into the next
  forecast and update its sell. After exit, obtain fresh paired forecasts before
  rearming. No same-bar exit/re-entry.

Limits refresh after exits or every 30 minutes, subject to actual sequential
model latency. Minute quotes have no queue priority, depth, tick-by-tick ordering,
partial-fill or guaranteed OCO information. These are quote-triggered simulations,
not verified exchange fills. Closed games without a simulated exit stay open /
unresolved rather than receiving invented settlement proceeds.

The worker checkpoints private paper state and transitions in the existing
Firestore research collections. Historical backtests and hourly forecast corpora
continue to use encrypted GitHub artifacts, not historical Firestore storage.

## Hourly screeners

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

The Polymarket process runs up to 345 minutes, checkpoints and releases its lease,
then dispatches a successor. The recovery workflow checks every ten minutes.
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
