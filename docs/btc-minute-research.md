# BTC minute forecast research

**Operational status, October 9, 2026:** continuous collection remains disabled. At the owner's request, Cloud Storage research archives and recovery files were permanently deleted. Only separately saved local reports or GitHub artifacts remain available; the cloud-dependent replay commands below cannot recover deleted data. Whole cloud paper snapshots are now an opt-in rather than a workflow default. See [cloud cost controls](cloud-cost-controls.mdx).

The dedicated `kalshi-btc-minute-forecasts.yml` workflow collects **Kalshi KXBTC15M binary-market bid/ask candle closes**, rather than Bitcoin spot prices. Collection runs in a separate thread every 15 seconds. Each completed-minute close is saved with its first actual receipt time; later price revisions are separate records.

For every market, origins 1 through 14 forecast its remaining 14 through 1 minutes. The first origin uses Granite, Chronos, and TimesFM under the explicit single-observation research policy. Later origins add Prophet. Missing history is never filled. Equal ensemble weights are normalized among models that support each requested quantile.

Whole YES/NO pairs record their actual inference-completion time. Only pairs completed within their own origin minute are usable by paper trading; slow or missed origins remain visible. The first unambiguous completed-minute bid at or above its time-aligned P90 may trigger a simulated **next-minute ask** entry, provided its quote was received within 30 seconds and before market close. A second scenario requires agreement with sticky direction: the opposite of the latest previously confirmed settlement. Each scenario holds **one contract** through official settlement, includes the observed quadratic fee schedule as a sensitivity, and uses no recovery multiplier. Timings overlap and returns must not be summed.

## Continuous operation

Enable only repository variable `KALSHI_BTC_MINUTE_FORECAST_ENABLED=true`, then dispatch the dedicated workflow with `duration_minutes=330` and `continuous=true`. A successful run saves encrypted state, then queues its successor. The separate recovery workflow checks every 15 minutes and resumes only when no research run is active or queued. Global concurrency and GCS generation preconditions prevent concurrent checkpoint overwrites.

GitHub-hosted runners have setup and scheduling delays, so this attempts continuous operation **without claiming gap-free 24/7 coverage**. Cold model loading, API delays, and inference deadlines can also create gaps. Reports distinguish published, failed, missed, and late unusable origins. Disabling the repository variable stops new workers and successors. Cancel an active research run to stop it immediately.

Quotes, forecasts, and individual paper trades remain AES-GCM encrypted in the existing private research bucket. Durable per-market archives are content-addressed; only after archiving are completed markets removed from the bounded local buffer. Checkpoints are saved every five minutes and at successful shutdown. Public GitHub artifacts contain only summary statistics. No Firestore documents are written and no exchange order methods, trading keys, or live-trader flags are used. The existing coin traders remain disabled.

## Exact original replay and opposite-side comparison

`kalshi-btc-fixed-report.yml` authenticates the original immutable 291-market / 3,492-origin replay and exports aggregate entry averages and fixed-one results. Its opposite-side comparison preserves the original sticky + P90 signals and next-minute timing, flips the settlement outcome, and spends up to $1 per entry with sizes floored to 0.01 contracts. It reports both a $1 notional budget plus fees and a $1 fee-inclusive budget.

Recorded opposite asks are the spread-aware alternative. The requested `1 - original ask` is the opposite **bid**, so that separate ideal-price sensitivity is not claimed as a verified executable ask fill. Direct four-decimal and cent balance-rounding sensitivities are exported. Historical fractional eligibility, liquidity, partial fills, and slippage are unverified. Choosing the best of 12 overlapping timings is an in-sample comparison, not independent validation.
