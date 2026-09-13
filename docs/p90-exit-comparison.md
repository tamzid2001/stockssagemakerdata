# P90 entry: take-profit and trailing-stop research

Private paper experiments only. No exchange orders are submitted. This is not a
recommendation to use loss-recovery sizing or evidence of a profitable edge.

The comparison reuses each saved forecast and completed one-minute bid/ask tape.
It does not rerun models or choose forecast versions based on later results.
The baseline remains P90 buy, held-side P10 sell-and-reverse, or opposite-side
P90 reversal. Twenty-four additional variants use fixed take-profit or trailing
stops at **1%, 5%, 10%, 20%, 30%, 40%, 50%, 60%, 70%, 80%, 90%, 100%**.

Percentages are relative price changes, not probability percentage points.
Take-profit threshold = entry ask × (1 + percentage). A target above $1 is
unreachable for a binary contract; it is counted explicitly and never clamped.
Trailing threshold = highest completed-minute bid since entry × (1 − percentage).
Trailing protection starts at entry, not only after a profitable move. A 100%
trail reaches zero and is not meaningful downside protection in most paths.

All variants retain the baseline P10/opposite-P90 reversals until a percentage
exit occurs. Percentage exits go flat and require a fresh P90 crossing to reenter.
Existing pending orders have priority, then percentage exits, held-side P10,
then opposite P90. Percentage exits remain armed between forecast windows.
Signals fill at the next genuine one-minute bid/ask within 120 seconds; missing
quotes expire pending fills. Intraminute highs/lows and stream ticks are ignored.
Settlement comes only from authoritative stored evidence.

Sizing starts at one share, increases 2.5× while the same game's cumulative
realized net P&L is negative, caps at 100, and resets to one only upon recovery.
A profitable trade that does not recover the game's prior losses does not reset.
Variants have independent books; never add their returns together.

Reports retain every variant, chronological trades, per-game results, win/loss
counts, streaks, fees, closed-entry notional, net P&L, realized drawdown, open
position marks and unreachable targets. Return means net / closed-entry notional,
not bankroll ROI. Open positions must not be ignored when comparing settings.
The assumed fee is 1% of entry and exit notional, not a verified exchange fee.
A 1% gross take-profit can therefore lose money even before spread/slippage.
Liquidity, queue position, capital constraints and fractional-contract rules are
not modeled. Extensive parameter sweeps introduce selection bias: reserve later
games for untouched forward evaluation before considering deployment.

## Frozen end-of-horizon quantile targets

Two additional path studies detect strict P90-upward and P10-downward crossings
from consecutive one-minute bids. The buy crossing tracks the original forecast's
final P99; the sell crossing tracks its final P1. Targets and deadlines are frozen
at the signal, never replaced by a subsequent forecast. Already-met targets are
reported separately. Observed later hits count; no-hit paths missing required
minutes are censored, not counted as clean misses. Pending paths remain pending.
Reports show hits/misses, missing/pending cases, completed-path hit rate, and
all-eligible lower/upper bounds. Repeated signals are correlated, not independent
games. These are quote-path probabilities, not trade P&L or short-selling results.
`signal_target_paths.csv.gz` records each signal, frozen target and hit timestamp.

## Execution and storage

`.github/workflows/p90-exit-comparison.yml` runs manually or every 30 minutes when
`P90_EXIT_COMPARISON_ENABLED=true`. It is a comparison sidecar, not a new forecast
engine: it consumes completed Polymarket/Kalshi sports game archives and the latest
verified BTC and Polymarket live-paper checkpoints. Failed/empty input runs are
not called complete coverage. Twenty-five new games per campaign are processed
per invocation; already analyzed source hashes are skipped. Source forecasts,
paper books and their pinned code are never changed.

Outputs are authenticated-encrypted ZIPs in the existing private research bucket,
containing `report-exit-comparison.json`, `exit_comparison_summary.csv`, and
`exit_comparison_trades.csv.gz`. Small Firestore pointers identify source hashes,
analysis commit, coverage and summaries. It does not consume GitHub artifact
storage or download model checkpoints. Cloud storage/operation charges still apply.
Each new live snapshot produces a new immutable report; only its latest pointer
changes. Overlapping live snapshots and historical/live cohorts are not summed.
