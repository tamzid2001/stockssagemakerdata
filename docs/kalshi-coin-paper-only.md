# Kalshi coin traders: paper mode

BTC, BNB, DOGE, ETH, NEAR and ZEC live gates were disabled and their running live
jobs cancelled on September 26, 2026 at the user's request. Live continuation was
paused before cancellation. Exchange positions and orders must be checked with
the read-only diagnostic; cancelling an Actions job does not liquidate a fill.

The existing coin workflows now offer `paper`. This keeps the same signal,
sticky-direction and recovery settings, simulates the timely observed ask entry,
and records a later observed bid exit at the five-cent threshold or official
binary settlement. It never calls an order-write endpoint. Full fills are a paper
assumption; available depth, partial fills and queue priority are not modeled.
The general M=1 taker fee formula is modeled conservatively with whole-cent
rounding of total cost plus fees. Actual product fees may differ.

Paper journals use a separate `:paper-v1` account identity and `paper_only=true`;
they do not modify the live journal, cash balance or existing exchange positions.
Paper results start a fresh recovery cycle. Private encrypted forecast evidence
and checkpoints remain in the existing restricted collections/bucket.

Set every coin's `WATCHDOG_MODE=paper`, `CONTINUOUS=true`, and `LIVE_ENABLED=false`
for paper continuation. The watchdog resolves current main for paper workers;
live mode still requires the independently pinned approval and enabled gate.
The legacy BTC research workflow remains off to avoid duplicate experiments.
