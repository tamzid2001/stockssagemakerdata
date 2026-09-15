# BTC first-P90 hold: first 3–12 minutes, next 12–3 minutes

This historical **paper-only** study complements the existing first-one-minute
→ next14 and first-two-minute → next13 prospective trackers. It does not change
their configurations, checkpoints, or trading rules.

## Forecast and strategy

Each arm uses exactly the first N completed one-minute bid/ask candles, N=3…12,
then forecasts the remaining 15−N minutes for both YES and NO. Missing opening
minutes exclude that market; later bars cannot replace an opening observation.
Inputs are frozen before inference. Outcomes and subsequent candles never enter
model inputs.

Prophet, Granite, Chronos-2, and TimesFM must all succeed. Equal raw weights give
each 25% at P90. Toto is deliberately excluded: fewer than 32 genuine observations
do not meet its minimum context. Tail weights follow the existing capability
registry; no unsupported tails are invented. TimesFM's existing production
access/license gates still apply.

The first unambiguous completed-minute **bid at or above its own P90** latches
one side. This is a threshold signal, not necessarily a fresh crossing from below.
Submit a simulated post-only limit at ask minus one cent, repricing the same side
each minute until filled or expired. Only a strict next-minute ask trade-through
qualifies as a candidate fill; touching a limit does not prove a fill. Hold the
selected side to official settlement. **No ladder, stop, switching, or re-entry.**

Report one whole contract separately from 2.5× recovery sizing across successive
markets (100-contract cap). Whole-contract sizes round down, e.g. 1, 2, 5, 12,
30, 75, 100. A partial win retains size; full cycle recovery resets to one.
Only outcome confirmations before the next entry may affect that entry's size.

## Honest replay clocks and coverage

Quote availability is assumed five seconds after each minute. Both sides become
available only after their combined measured sequential inference duration plus
that delay. Markets with inference finishing after close are reported as missed
deadlines, not retroactively tradable forecasts. Remote checkpoint upload latency
is not part of this model-runtime proxy. CPU historical replay is not proof that
the same model pair can publish in time in production.

Official settlement timestamps are used when present, with a 60-second-after-close
floor; otherwise that 60-second notification delay is an explicit assumption.
The report assumes 1% entry-notional fees, not verified maker fees or liquidity.
Return divides closed net P&L by cumulative entry notional, not account equity.
Drawdown is realized dollar drawdown, not a complete mark-to-market risk measure.

The cohort is the latest bounded set of exact-15-minute BTC contracts from one
current-tier discovery page before a fixed close cutoff. Selection does not use
winner, forecast success, or history availability. Coverage reports attempted,
usable, missing, failed and missed-deadline markets. It never claims all historical
markets. Compare a common usable subset before ranking horizons; few observations
or many attempted strategies can produce misleading apparent skill.

## Running and resuming

Workflow: `.github/workflows/kalshi-btc-horizon-backtest.yml`.
First use `smoke_mode=mock` (no model downloads). For actual research use
`smoke_mode=real`, `history_minutes=all`, a past UNIX-seconds `as_of`, and bounded
`max_markets`. GitHub-hosted CPU runners are logically separate arms, not GPUs.
Each arm has an independent concurrency group, immutable code SHA and campaign ID.
An unfinished arm dispatches its own continuation with identical parameters.
The wall-clock budget is 270 minutes within a 330-minute job timeout.

Source snapshots, each side's numerical output, orders, trades, and reports are
compressed and AES-GCM encrypted into the existing private research bucket.
Firestore contains small hashes/pointers/configuration only. A failed second-side
fit can resume from the first side's saved forecast. Completed records are never
silently overwritten. A terminal failed-market record remains visible; changing
code/configuration creates a distinct study instead of rewriting history.

An optional encrypted three-day GitHub summary artifact is not the durable store.
GitHub artifact quota exhaustion cannot erase completed private checkpoints.
Required existing secrets: `FIREBASE_SERVICE_ACCOUNT_JSON`,
`QUANTURA_RESEARCH_ARTIFACT_KEY`, `HF_TOKEN`; existing repository variables
`TIMESFM_HF_ACCESS_APPROVED=true` and `TIMESFM_COMMERCIAL_LICENSED=true`.

## Quote-count experiments are distinct

The requested first32/60/90/120 **exchange quote updates** are not minute candles
and not synthetic one-second bars. Their timestamps, sequence and provenance must
be preserved. One quiet second does not supply a new observation; multiple updates
inside one second must not be collapsed simply to achieve a convenient model grid.
Historical minute candles cannot reconstruct this tape. The five-model quote-count
study must not reuse the four-model minute results under a different label.
