# In-game P90 entry / P10 reversal research

This is an independent paper strategy, not the earlier buy-below-P1 experiment.
No exchange orders, wallet access or exchange trading credentials are used.

## Frozen rules

* Download genuine **in-game only** observations. Reject missing/conflicting
  game-start timestamps. Start after 32 elapsed minutes AND 32 observed bars on
  every side. Use up to 500 observations; do not synthesize missing minutes.
* Forecast every 15 or 30 minutes with the five approved adapters and equal
  requested weights. A failed member excludes the entire origin. Tail weights
  still exclude Toto/TimesFM where they cannot supply the requested quantile.
* First entry: observed bid crosses from at/below P90 to strictly above P90
  within a published forecast. A below-P1/P10 precondition is **not** required.
* Start with one share. Only one position per game. Ignore other P90 signals
  while holding the selected side. Hold across forecast horizons.
* When the held side's bid reaches or falls below the current published P10,
  sell it and buy the opposite side of the **same binary contract**, immediately
  after the simulated exit. The opposite does not need its own P90 signal.
* Each P10 exit multiplies quantity by 2.5, capped at 100. The sequence is
  1, 2.5, 6.25, 15.625, 39.0625, 97.65625, 100. Sizing is per game, not global.
* Soccer can contain six YES/NO sides. Opposite means paired YES/NO, not an
  arbitrary alternative outcome. Simultaneous first-entry signals are excluded
  when their ordering is ambiguous.
* There is **no fixed-price stop** and no time/horizon liquidation. P10 remains
  an explicit dynamic exit rule. Verified official settlement ends a position;
  unknown settlement remains open/censored, with available marks shown.

## Execution and temporal limitations

Signals use only already-published curves and observed quotes. Measured model
runtime delays publication. A curve revision cannot itself create a crossing.
Execution uses the next genuine quote within 120 seconds: sell at its bid and
buy at its ask, never at a retrospectively chosen quantile price. A reversal
requires quotes for both sides. Missing quotes do not manufacture fills.

The base cost assumption is 1% of entry plus exit notional, **not a verified
Polymarket fee schedule**. Spread is included through bid/ask. Queue position,
depth, account capital constraints and live execution quality are not modeled.
Fractional paper quantities are permitted. Loss escalation can increase losses
rapidly; a share cap is not a guarantee against loss.

Reports contain chronological trades, entry/exit reasons, trigger timestamps,
forecast IDs, quantities, W/L/breakeven, within-game streaks, fees, net P&L,
open marks, and net return on closed-entry notional. That last figure is **not
portfolio ROI**. Outcomes after an old P1/P10 recovery are not returns for this
new P90-entry strategy. Historical performance does not guarantee future results.

## Durable campaign execution

Workflow: `.github/workflows/polymarket-p90-ingame-backtest.yml`.
Run two independent jobs with `horizon=15` and `horizon=30`, `smoke_mode=real`,
`campaign_id=new`, `code_ref=main`, `continuous=true`.

Each GitHub-hosted CPU job runs for at most 330 inference minutes, then dispatches
a successor with the original commit and campaign ID. There is no fixed number
of successors. The 15-minute watchdog resumes unfinished campaigns after errors
or runner interruption; it never silently creates a new campaign.
GitHub queues, provider outages and rate limits can delay execution, so this is
durable 24/7 continuation, not a promise of uninterrupted CPU service.

The campaign paginates accessible historical metadata, deduplicates completed
games, and freezes its as-of cutoff. Coverage refers to provider-accessible
history, not a claim that every historical game has downloadable prices.

### Storage

The old 20 MiB all-games SQLite checkpoint is **not** used by this workflow.
Only one active game's data is retained locally. Every completed game is
compressed and AES-256-GCM encrypted, then uploaded to the existing bucket
`quantura-e2e3d.firebasestorage.app` under a private, content-addressed prefix.
Completed archives cannot be overwritten. Five-minute active-game checkpoints
support restart; they include immutable inputs, quantiles, and the trade ledger.

Firestore `market_research_sessions` holds only small campaign/lease/cursor
metadata and per-game summaries/archive references. No quote arrays, CSV bytes
or model matrices are stored in Firestore. Existing client rules deny access to
these documents and the `private-research` Storage prefix. No public download
token or signed permanent URL is created. One game's archive has a separate
1 GiB safety bound, not a total-campaign storage ceiling.

Archives do not depend on GitHub's three-day artifact expiration. Bucket IAM,
backup and retention policies remain operational responsibilities; storage and
Firestore usage incur normal cloud charges. Original GitHub-only experiments
remain untouched and retain their existing retention policy.

Existing secrets: `FIREBASE_SERVICE_ACCOUNT_JSON`, `QUANTURA_RESEARCH_ARTIFACT_KEY`,
`HF_TOKEN`. Existing TimesFM license/access variables still apply. Set
`POLYMARKET_RECOVERY_ENABLED=true` to enable scheduled recovery. No new secret is
required. The service account needs the existing project's Firestore access and
private object read/create access; use a dedicated least-privilege worker identity
when separating production service accounts.

Tests run without model downloads. Real runs must verify archive uploads,
Firestore fencing, actual model participation, and successful continuation.
