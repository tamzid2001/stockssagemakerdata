# SPY constituents rolling 500-close backtest

The `SPY constituents rolling 500-close five-model backtest` workflow runs a
paper-only, resumable historical study over the current official SPY equity
holdings plus SPY itself.

## Frozen inputs

A new campaign downloads State Street's official daily SPY holdings workbook,
rejects cash and non-exchange identifiers, records the workbook SHA-256 and
holdings date, and adds SPY. It then freezes Alpaca IEX split-adjusted daily
closes through the latest completed NYSE session. There is no Yahoo or synthetic
data fallback.

The immutable campaign records:

- the exact code commit;
- State Street workbook hash, holdings date, and ticker set;
- Alpaca feed, adjustment, source range, and completed as-of session;
- the weekly ensemble configuration hash and pinned model checkpoints;
- all source rows and every completed origin forecast in encrypted private
  research storage.

## Rolling-origin rule

Each forecast receives exactly 500 closes ending at cutoff `t`. Close `t+1` is
withheld and is never passed to inference. A close below first-step P10 is a buy
signal; a close above first-step P90 is a short/sell signal. Strict inequalities
are used. The runner evaluates up to the 500 most recent eligible cutoffs per
symbol and computes them newest first. A missing close on the immediately next
NYSE session is not treated as a one-session cutoff.

Every origin requests seven NYSE sessions and P1/P10/P25/P50/P75/P90/P99 from
Prophet, Toto 4M, Granite, Chronos-2, and TimesFM. All five have raw weight 0.20.
All five must finish. At tail quantiles that a model does not natively support,
only supporting models are used and retain equal effective weight.

## Trading and statistics

Each eligible symbol starts with a separate $10,000 allocation. One fully
invested, non-overlapping long or short can be open per symbol. It exits on the
first opposite signal, after seven completed closes, or at the end of evaluated
data. Results exclude fees, slippage, borrow cost, dividends, and taxes.

The final Actions artifact contains the aggregate and per-symbol return,
profit, maximum drawdown, signal and target-hit rates, gross and one-way
annualized turnover, time between any and opposite signals, holding time,
trades, every rolling origin, and the portfolio equity curve.

## Continuation

GitHub-hosted jobs cannot run indefinitely. Sixteen independent shards run for
a bounded inference window and checkpoint every successful origin. If every
shard exits cleanly but work remains, the workflow dispatches a successor with
the same campaign ID and exact original commit. A failed model or failed shard
stops automatic handoff instead of silently changing the ensemble.

To start the full study, dispatch the workflow with `resume_campaign` blank,
`code_ref=main`, `max_symbols=0`, `max_new_origins=0`, and `continuous=true`.
To resume manually, use the logged campaign ID and its recorded 40-character
code SHA.

This is retrospective research. The current-holdings universe has survivorship
bias, split-adjusted prices are not total returns, and the foundation-model
pretraining cutoff cannot be independently certified against every historical
origin.
