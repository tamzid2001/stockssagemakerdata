# Forecasting

Choose an instrument or dataset, then configure **Observed bars**, **Last N observations**, **Data cutoff**, **Forecast until**, models and quantiles. The selected interval governs both input observations and each predicted step.

## Supported market intervals

| Selection | Canonical API/worker frequency |
| --- | --- |
| Minute | `1min` |
| 5 minutes | `5min` |
| 15 minutes | `15min` |
| 30 minutes | `30min` |
| Hourly | `1h` |
| 4 hours | `4h` |
| Daily | `1D` |
| Weekly | `1W-MON` |
| Monthly | `1MS` |

Aliases include `5m`, `15m`, `30m`, `4h`, `1w` and `1Month`; use `1Month` rather than ambiguous uppercase `1M`. Weeks begin Monday 00:00 UTC and months begin on the first UTC day. Stock daily bars group by exchange session date. Open aggregate periods and empty buckets are excluded, and prices are never invented to meet a model's minimum context.

`prediction_length` counts bars: 12 at 5 minutes is one hour; three monthly steps are three calendar boundaries. For a precise end use `prediction_end_at` with an explicit timezone. Saved presets restore bar counts and selected intervals.

Daily US-stock session horizons use NYSE holidays. Other market intervals use a UTC frequency grid; forecast timestamps during a market closure are not executable quotes.

## Models and quantiles

Prophet, Toto, Granite, Chronos and TimesFM participate according to context, horizon, native quantile support and licensing. Requested tail values only use models supporting those tails. Inspect `GET /api/v1/forecast/models` for effective availability and the interval list.

Historical overlays distinguish the original input series from subsequently observed prices. A first prediction is compared only with its matching completed interval; shorter or later observations cannot replace it.

## Latest and deep historical cutoffs

Select latest observations or a calendar/relative cutoff. For 120/180 days use 172800/259200 top-level `history_lag_minutes`; there is no fixed 90-day cutoff-age cap. Available observations still depend on provider retention. The cutoff is applied before the N-bar limit, and replays are generated now. Optional consented website telemetry is separated from model configuration so it cannot block latest or historical requests.

[Historical forecast guide](https://quantura.studio/developers/api#historical-forecasts).

Stock-price overlays can use retained history beyond 90 days. Recent minute quotes remain bounded to seven days; prediction-market/perpetual live overlays have a separate 90-day availability window. Saved predictions are never revised by overlay retrieval.

## API workflow

1. Resolve provider identity and freeze source metadata/cutoff.
2. Submit `POST /api/v1/ensemble-forecasts` with an authorized session/key and an `Idempotency-Key`.
3. Poll the job until completed or failed; keep the result hash and configuration.
4. Inspect or export the quantile series. Do not treat a backdated cutoff as a live publication time.

[Interval guide](https://quantura.studio/developers/api#forecast-frequencies) · [API guide](https://quantura.studio/developers/api#ensemble-api) · [Strategy research](Research-and-Backtesting)
