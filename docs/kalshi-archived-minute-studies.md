# Archived Kalshi minute strategy studies

The all-series minute collector and the forecasting study are separate jobs.
The collector alone does **not** produce forecasts or strategy returns.

Run the existing `kalshi-interval-studies.yml` workflow with `source=minute_archive`,
`series=all`, `max_markets=1000`, `continuous=true`, and a pinned `code_ref` SHA.
This includes the 14 verified directional crypto/commodity series, even when a
commodity session is currently closed. `provider_replay` preserves the older
upstream-candlestick study. No exchange orders are sent by either mode.

The archive mode freezes existing encrypted source generations. It reads exact
first-received paired bid/ask minute closes and official settlement receipts.
Delayed backfills, price revisions, missing minutes and invalid quotes do not
become timely prices. Flat windows are valid. Every settlement updates that
series' sticky direction, including markets with no trade or failed forecast.

Each market is evaluated after observations 1 through 12, forecasting the
remaining 14 through 3 minutes. The first origin uses Granite, Chronos and
TimesFM; later origins add Prophet. Toto is excluded. Shared production adapters
and per-quantile equal weights remain authoritative. These are retrospective
fits: stored receipt time plus measured model runtime is a **publication proxy**,
not proof the forecast was available live. Live-model queue delay is not modeled.

## Independent comparisons

- First unambiguous one-minute bid at or above the aligned P90, hold to settlement.
- The same first P90 signal, only when it agrees with that series' sticky direction.
- First-quote-below-P10 comparison, preserved from the existing study.

Entries use the next observed minute's ask, never the signal price. Each origin,
series and strategy has its own fixed-one-contract and 2.5× recovery ledger,
rounded down to whole contracts, capped at 100. Recovery resets only when its
cumulative loss is recovered. Actual fills, depth and historical fee terms are
not verified; current quadratic fees are an explicitly labeled sensitivity.

## Later-forecast exits

The baseline is P90 + sticky after **two observed minutes → thirteen forecast
minutes**, not a two-minute horizon. Later origins 3 through 11 are assessed at
their first completed minute after modeled publication, only when publication
occurs after entry. Exits use the following observed minute's **bid**, including
exit fees. Two separate conditions are reported:

1. `opposite_p90`: an unambiguous P90 signal on the opposite side. Neutral does
   not exit; the opposite signal need not also match the sticky direction.
2. `no_longer_agrees`: the held side no longer has unambiguous P90 + sticky
   agreement, including neutral or ambiguous signals.

Sticky usually remains unchanged during a market, so an opposite P90 signal is
not automatically an opposite **P90 + sticky** signal. Missing quotes or failed
forecasts are never interpreted as disagreement. Each cutoff from 3 through 11
is a paired comparison against the same baseline entries. No re-entry or side
switch is introduced into this early-exit experiment.

## Persistence and coverage

Each model side/origin is checkpointed, then each complete market is summarized
without repeating inference on resume. Reports are split by origin and exit
policy to avoid large GitHub artifacts. Encrypted bodies live in private Cloud
Storage; Firestore contains small immutable hashes/pointers only. A run budgets
270 minutes and dispatches the same frozen cohort/code when incomplete.
Completion does not automatically start another future cohort. Minute collection
continues independently; launch another frozen cohort to analyze later archives.

Report `selected_markets`, `analyzed_markets`, failed/missed origins and usable
paired minutes alongside results. Market windows overlap across origins; adding
their trade counts does not create independent samples. Select best timings only
as exploratory, in-sample findings, not established profitable opportunities.
