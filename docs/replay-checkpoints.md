# Resumable P10 historical research

`historical-p10-replay.yml` runs the shared quote engine against `kalshi` or `polymarket_us`. It is paper research only: no exchange orders, no Firestore backtest storage. Configure a 30/60-minute horizon, contract-side budget, rolling-origin budget and optional Kalshi Sports series. Blank series discovers game/match series, not all sports propositions indiscriminately.

Each origin uses up to 500 genuine previous observations, including gaps, and equal requested model weights. Toto requires 32 observed values. All five registered adapters are requested when TimesFM production gates are configured; failures and actual participation are preserved. No failed model is counted as participating. Quantiles are P01, P10–P90 in 10-point increments, and P99; native-only models do not manufacture tails.

P10 crossings from either direction are measured separately. Nine independent exit experiments target P20–P90/P99 with P01 stops. A signal cannot fill on the same observation. Missing-minute crossings are not inferred, fills use later actual asks, exits use bids, costs apply, and sizes are capped. Reported target-before-stop rates exclude censored outcomes and are distinct from filled-trade win rates. These lanes are not one combined portfolio.

## Recovery

- Every forecast is persisted before future observations are revealed. Replay transitions and outcomes are committed transactionally to runner-local SQLite.
- An interrupted published forecast resumes from its saved replay state, without re-fitting or double-counting trades.
- Online SQLite backups are AES-256-GCM encrypted ZIP artifacts. An initial checkpoint is attempted after one minute, then every 45 minutes and on worker exit. Only the newest two checkpoints from the current run are retained; superseded copies from that run are removed after a new upload succeeds.
- A runner loss can lose work since the last successful remote upload. Neither GitHub scheduling nor artifact uploads guarantee zero downtime or zero recovery loss.
- The worker stops early enough for a final upload before GitHub's six-hour limit. `remaining_handoffs` (0–8) can dispatch a successor with the exact commit and verified checkpoint ID. Configuration mismatches fail closed. Manual recovery uses `resume_artifact_id` plus the original `code_ref` and settings.
- A 20-MiB database/25-MiB encrypted payload limit prevents unbounded repository artifact costs; hitting it stops automatic continuation explicitly. Retention is **three days**, not permanent archival. Download required research before expiry.
- Coverage reports distinguish configured bounds, unavailable data, completed sides, and unfinished work. No run claims all-history coverage merely because its job succeeded.

Metrics are computed on timestamp-matched future observed asks: MAE, RMSE, bias, quantile pinball loss and empirical coverage. They are out-of-sample simulated research, not in-sample Prophet metrics or guarantees of future returns.

## Median-crossing experiment

Select `strategy=median_cross` on the same workflow for either provider. It uses the same real data adapters, five-model ensemble and encrypted checkpoints, not a separate trading service.

- Buy below the available P50: signal at a genuine ask below the median, then require a **later** observed ask plus slippage below that frozen limit. Sell above P50: signal when the observed bid is above the available median, then require a **later** bid minus slippage above that frozen sell limit. One position per side, no same-observation entry/exit.
- Every observed crossing of P50 from either direction requests another ensemble using only the latest up-to-500 observations available then. Crossings compare adjacent real minute observations against the same saved curve; gaps do not fabricate crossing events. Initial/expired forecasts also refresh.
- Recalculation is not instantaneous. Each forecast records measured execution time; simulated serial publication is rounded up to a minute. Crossing requests can queue, and a forecast can expire before it becomes available. Such latency is counted, not hidden. No refreshed curve rewrites the crossing or its prior orders.
- Retains the P01 protective stop fixed at entry signal and the entry forecast's horizon timeout. An unfinished position at the end of available history remains open/censored. A median exit is not necessarily profitable after spread, costs and forecast changes.
- `loss_multiplier` defaults to 2.5 and `max_shares` to 100; start with 1 share and reset after a net profitable trade. Setting multiplier to 1 supplies a fixed-size comparison. Cap applies separately per contract side, **not** as a global portfolio risk limit. Increasing size after losses increases risk; it does not guarantee recovery.
- The common cost assumptions remain $0.01 per contract per side and $0.005 adverse slippage per side. They are illustrative sensitivity inputs, not asserted venue fees or proof of order-book depth.
- Forecast budget counts every attempted refresh, not just successful forecasts. Hitting the budget stops that side and is reported as `forecast_budget`, not full historical coverage. Results separate closed-trade win rate, net P&L, open positions, crossings, actual model participation and ML metrics after simulated publication.

P10 and median experiments have separate provider/strategy concurrency groups. Existing pinned P10 continuations keep their original code and configuration. Historical artifacts remain private encrypted ZIPs with three-day retention; there is no live-order execution in this workflow.
