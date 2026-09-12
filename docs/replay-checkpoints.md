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
