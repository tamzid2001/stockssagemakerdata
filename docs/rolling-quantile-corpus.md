# Rolling quantiles before choosing a strategy

The `quantiles` experiment in `historical-p10-replay.yml` stores model outputs
without taking simulated or real positions. Use this archive to design strategies
later, then reserve separate games/dates for validation: selecting a strategy on
the same corpus used to measure its returns is not independent validation.

## Timing

At decision time T, choose `lag_minutes` 15 or 30. The model receives the latest
**up to 500 genuine observations on or before T − lag**, not 500 observations
followed by deleting the last 30. Missing minutes are retained as gaps; short
history is recorded. The horizon is 45 or 60 minutes after the last input quote.
If that quote predates the requested cutoff, the actual forecast dates reflect
that staleness. A completely expired forecast is rejected.

`roll_minutes` independently advances the next decision (default 30 minutes).
All four lag/horizon combinations are supported. Historical origins begin at the
known game start, with pregame history used as input; without a reliable start,
they begin after 500 available observations, or two when fewer are available.
Downloads request up to seven additional days of pregame observations from the
existing provider adapter. This is not a guarantee that every market has 500 rows.

Each immutable forecast stores:

- provider, event, market, contract side and outcome;
- decision time, requested cutoff, actual last input time, generation time;
- exact timestamped input snapshot, input hash/count and gap count;
- P1, P10…P90 in ten-point increments, P99 for every future minute;
- equal requested model weights, effective per-quantile weights, model versions,
  actual participation, failures and runtime;
- separate timestamp-matched actual bid/ask observations, with missing values null;
- whether an actual was already visible at T or was observed after T.

Past-cutoff overlays never enter inference. Historical results are explicitly
replays generated now, not claims of forecasts published before those outcomes.
The headline forecast metrics score observations after decision time T; the
earlier overlay remains available for analysis in the artifact. No win-rate or
profitability claim is made by this strategy-free experiment.

## Run and recover

```sh
gh workflow run historical-p10-replay.yml --ref main \
  -f provider=polymarket_us -f strategy=quantiles -f smoke_mode=real \
  -f lag_minutes=30 -f horizon=60 -f roll_minutes=30 \
  -f max_contracts=20 -f max_origins=100 -f remaining_handoffs=1
```

Repeat with `provider=kalshi`, lag 15 and horizon 45 as required. Workflows are
isolated by provider, experiment, lag and horizon. They run on GitHub-hosted CPU
workers, not dedicated GPUs. Model inference is sequential. Registered models are
Prophet, Toto, Granite, Chronos and TimesFM; successful participation must be read
from each forecast rather than assumed. Toto/TimesFM do not supply outer tails.
TimesFM requires the existing production license and checkpoint-access flags.

The database, snapshots, quantiles and report are in AES-256-GCM encrypted ZIP
GitHub artifacts, **not Firestore**. Online checkpoints are published after the
first minute, every 45 minutes and at shutdown. A saved origin and its cursor are
atomic. Continuations restore the complete archive on the exact original code.
Only this run's newest two checkpoints are retained; artifacts expire in three
days. Download the encrypted artifact before expiry for long-term retention.
The local database has a 20 MiB safety cap (25 MiB artifact ceiling); reaching it
is reported as partial coverage, not a completed full-market study.

Final archives also contain `forecast_quantiles.csv.gz` (one row per forecast
minute, both quantiles and separately marked actuals) and `quantile_manifest.json`
with schema/configuration, counts and SHA-256 checksum. These exports remain
inside the encrypted archive. Intermediate recovery checkpoints may contain
only SQLite; all completed quantiles remain recoverable there.

## Live paper workers

`polymarket-live-paper.yml` is the existing shared worker workflow (path retained
for compatibility); it now accepts `provider=polymarket_us|kalshi`, `lag_minutes`,
`roll_minutes`, 45/60-minute horizons, and `forecast_only=true`. The default is
forecast-only: no strategy is selected. Prospective signals, if explicitly
enabled later, can use quotes only after the actual model-publication timestamp,
never the already-known overlay. Inference that consumes the remaining forecast
window is disclosed as expired, not a usable signal.

The existing live worker's private fenced persistence remains separate from the
artifact-only historical corpus. Per-provider watchdogs and handoffs preserve
the rolling configuration. Kalshi live discovery uses open Sports moneyline
events with a known start in the last 18 hours, **not confirmed live scores**;
events with unknown starts are excluded. GitHub scheduling/inference delays mean
continuous execution or exact refresh timing cannot be guaranteed. No exchange
order client or order credentials are used.

Kalshi request parameters were checked against the official
[events API](https://docs.kalshi.com/api-reference/events/get-events).
Source redistribution remains subject to provider terms; a private research
archive does not establish a right to sell raw provider data.
