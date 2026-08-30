# Quantura Forecasts methodology

## What a forecast is

A Quantura Forecast is a prospective probability assigned to a formal question whose outcome is not yet known. The possible future headline is explanatory display text. It is never a claim that the event already happened.

Every published forecast includes:

- a formal yes/no or explicitly partial-resolution proposition;
- a forecast timestamp and input-data cutoff;
- a resolution deadline;
- an objective resolution rule and authoritative source class;
- a probability between 0 and 1;
- model/provider version metadata;
- a frozen evidence snapshot;
- bull, base and bear reasoning;
- an append-only probability trajectory.

## Probability and uncertainty

A probability of `0.70` means the methodology assigned a 70% probability at that timestamp. It does not mean the event is guaranteed. Probabilities can change as new evidence becomes available; each change creates a new revision and leaves earlier values intact.

The generation interface supports structured statistical, market-data, time-series and domain models. A weighted ensemble normalizes configured provider weights and records every contribution. At least one structured numerical provider is required. An LLM may synthesize supplied evidence into concise explanations, but it cannot be the only numerical provider, rewrite the formal question or resolution rule, or cite evidence absent from the frozen source record.

## Resolution

Resolution uses category-specific deterministic adapters backed by authoritative structured data. The original rule is fixed before publication. If approved sources conflict or the result cannot be established, the forecast is marked disputed or remains unresolved instead of being forced to yes/no. An LLM does not independently declare objective outcomes.

## Scoring

Binary forecasts are scored using the last published probability available before resolution.

`Brier score = (probability - outcome)^2`

where outcome is `1` for yes and `0` for no. Lower Brier scores are better. Quantura also stores logarithmic score and aggregates calibration buckets comparing average predicted probability with actual event frequency. Partial, void and disputed outcomes are not assigned a binary Brier score.

## Temporal integrity

Evidence timestamps are validated against `input_cutoff_at`. Evidence published or observed after that cutoff is rejected from the forecast snapshot. Published initial snapshots and probability-history revisions are append-only. Corrections are separate amendments; they do not rewrite historical probabilities.

## Politics and public trust

Political forecasts are limited to objectively resolvable public actions such as elections, legislation, policy, tariffs, appointments, confirmations, budgets and regulatory action. Quantura rejects forecasts framed around unverified crimes, medical diagnoses, sexual/private behavior, secret illegal acts, deaths or serious harm. All unresolved political forecasts display that the event has not occurred.

## Limitations

Forecasts can be wrong. Model inputs can be incomplete, regimes can change, source data can be delayed, and rare events are difficult to calibrate. Category/model/entity performance should be interpreted only when sample sizes are sufficient. Historical scoring does not guarantee future predictive performance, investment returns, political outcomes, product announcements or sports results.
