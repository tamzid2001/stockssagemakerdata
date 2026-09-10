# Polymarket US ensemble paper research

This is a quote-based **paper simulation**, never an order-executing bot. It reuses
Quantura's Python adapters and per-quantile ensemble engine. No exchange trading
credentials or order endpoints are available to these workers.

## Forecast methodology

Default models: Prophet, Toto 2.0 4M, Granite PatchTST-FM-r2 (CPU), Chronos-2.
Each selected model receives raw weight 1. For each requested quantile, successful
capable models receive equal normalized weights. At least two models must succeed;
a lone Prophet result is not reported as an ensemble. Model failures are recorded.
TimesFM is not selected by these workflows: commercial licensing remains required.

Quantiles: P1, P10, P20, P30, P40, P50, P60, P70, P80, P90, P99. Toto contributes
only P10–P90; Chronos contributes only inside its loaded checkpoint's genuine
quantile range. Unsupported tails are unavailable, never clamped or extrapolated.
Forecasting and aggregation use log-odds with inverse-logit output. Tiny epsilon
is solely for transform stability. The input, seed, effective weights, package
versions and actual returned model metadata are preserved privately.

Use up to 500 completed, observed minutes before each origin, including pregame
and in-game observations. Fewer than 500 are accepted (minimum 40, matching the
existing engine). Only a contiguous observed suffix is used for a minute-step
foundation model; missing minutes are not compressed into fake one-minute bars.
Actual history counts and unavailable origins are reported. Data gaps can reduce
coverage substantially; 500 requested minutes does not imply 500 actual quotes.

Forecast horizon and rolling refresh cadence are independently selectable runs
of either 30 or 60 minutes. No final outcome or post-origin price enters inference.
For prospective monitoring, the durable publication timestamp also gates entry:
inference latency never creates a retrospectively available live signal.

## Price and execution semantics

The official [price-history endpoint](https://docs.polymarket.us/api-reference/price-history/get-price-history)
returns display quotes, not trades. YES ask is longPrice; NO ask is shortPrice.
YES bid = 1 − shortPrice; NO bid = 1 − longPrice. Spread is preserved: the two
asks need not sum to one. Both sides are tracked, not collapsed into complements.
Requests use genuine one-minute custom ranges, chunked into at most 24 hours and
cached for 30 seconds. Historical research requests a bounded window around each
event (maximum 48 hours; unresolved finish metadata falls back to start + 6 hours,
which can truncate long games and is not a verified resolution timestamp).

Either P10 crossing direction can trigger a paper limit intent. Entry cannot fill
on the signal observation. A later ask plus configured slippage must be at or
below the frozen P10 limit. Stop is frozen P1; targets independently test P20–P90.
Exit uses observed bid, with stop-first ordering and no invented intraminute path.
Quotes provide no depth/queue priority: these are assumptions, not verified fills.

Default assumptions: base size 1, loss multiplier 2.5, maximum size 100, reset after
a profitable trade, fee 0.01 per contract per side, slippage 0.005 per side.
Fees are configurable simulation assumptions, **not a claim about actual exchange
fees**. Each target is an independent experiment; never sum them as one portfolio.

Trigger-to-target outcomes and filled paper-trade results are distinct. Reports
include target hits, stops, censored horizon outcomes, observed win rate, net/gross
P&L, fees/slippage, average entry/exit/size, profit factor, realized drawdown and
realized equity curve. Unfinished triggers/positions are disclosed, not wins.
Realized drawdown is not mark-to-market portfolio drawdown. Historic resolved-game
selection has survivorship/coverage limitations and is not a profitability guarantee.

## Workflows and handoff

* `polymarket-live-paper.yml`: manual mock/real modes; bounded single-cycle test or
  up to 345 minutes monitoring, then checkpoint-confirmed successor dispatch.
* `polymarket-paper-watchdog.yml`: ten-minute recovery check; only dispatches if
  no paper monitor is running/queued and `POLYMARKET_PAPER_ENABLED=true`.
* `polymarket-historical-backtest.yml`: independent manual mock/real workflow;
  bounded contract and origin budgets, and 345-minute total compute budget.

These are logically separate Ubuntu CPU jobs, not dedicated GPU machines. They
cannot guarantee uninterrupted 24/7 execution: GitHub queue delays, installation,
provider outages and rate limits can cause gaps. The worker reserves time before
GitHub's six-hour hard timeout; slow setup can shorten its 345-minute work period.
Gap duration and delayed recovery are recorded. Use persistent managed workers
for low-latency/availability requirements; the engine does not rely on Actions APIs.

The current `main` branch is checked out for every successor. Private Firestore
leases have 120-second expiry, 25-second renewal, monotonic fencing and config
binding. Each outcome/checkpoint transition is atomic. A stale worker cannot
commit after its lease expires or another worker takes over. A successor is
dispatched only after a report and checkpoint have been persisted and ownership
released. Hard-killed workers recover through the watchdog after lease expiry.

## Storage, credentials and configuration

Existing secrets: `HF_TOKEN`, `FIREBASE_SERVICE_ACCOUNT_JSON`; automatic
`GITHUB_TOKEN` is scoped to contents read and Actions dispatch. No exchange
trading secrets are used. Repository variables:
`POLYMARKET_PAPER_ENABLED` (default false), `POLYMARKET_PAPER_HORIZON` (30 or 60).
Disable the variable to prevent handoffs/recovery; cancel a running job to stop it.

Private collections: `market_research_sessions` (contract checkpoints),
`market_research_forecasts` (immutable inputs/outputs), `market_research_trades`
(idempotent outcomes), `market_research_runs` (run reports). Browser Firestore
access is denied. These internal research records are not a new public API or
licensed bulk release. Raw provider redistribution remains `review_required`.

No raw datasets, forecasts, trading statistics, secrets, model caches or large
artifacts are uploaded to GitHub. Public Actions summaries expose only coverage
counts and safe execution receipts. This avoids increasing GitHub artifact storage.
The Linux/Python 3.12 CPU lock is generated from `market_research/requirements.in`;
dependencies install during worker setup, never inside a web request.

```bash
# No paid calls or model downloads
python -m pytest -q market_research/tests ensemble_forecasting/tests

# Actual historical data and real models; credentials must be in the environment
python -m market_research.worker --mode historical --horizon 30 \
  --max-contracts 20 --max-origins 20 --duration-minutes 345
```

This worker release does not claim that the larger unified global market index,
consumer backtest UI, all-sports full coverage, or real-money trading is complete.
