# BTC first minute → next 14 minutes: paper research

This is a separately versioned experiment, not a live exchange-order system or
a replacement for saved `kalshi_btc_first2_next13_v1` results. Never combine the
two cohorts when reporting strategy performance.

## Forecast origin and model policy

For each binary `KXBTC15M` market of exactly 900 seconds, use only the genuine
completed-minute bid/ask candle timestamped `open_time + 60 seconds`. YES uses
the observed ask; NO uses the complementary ask `1 - YES bid`. Do not repeat the
point, substitute the second minute, fill missing observations, or use settlement
information. Exactly two forecasts are published atomically, one per side.

The internal research-only one-point override permits exactly Granite, Chronos-2
and TimesFM, horizon 14, frequency 1min and strict model-failure policy. It is not
a public API parameter and does not lower website/upload minimum history limits.
All three must succeed. Requested weights are equal; unsupported tail quantiles
are excluded per model and the remaining weights renormalized. TimesFM licensing
and HF access checks remain enforced. Real inference must be smoke-tested before
enabling this workflow; successful numerical execution is not forecasting skill.

The first forecast timestamp is market open + 120s; the last is market close.
Actual publication follows inference completion, not a fictional instantaneous
60-second forecast. Quotes from before publication cannot trigger a trade. A
late-start or missed-origin market is reported, not backdated. The worker polls
every 15 seconds for completed-minute provider data; no websocket ticks enter
signals. Quotes with the same minute timestamp are immutable.

## Post-only −1¢ limit paper proxy

- When flat, a P90 touch/above signals that side; a P10 touch/below signals the
  paired opposite side. Conflicting same-minute candidates are excluded.
- Post a buy at the current best ask minus $0.01, rounded down to a cent, only
  when strictly between zero and the current ask. Opening orders are not
  reduce-only: that flag is for reducing existing exposure.
- Hold one side. A held-side P10 or opposite-side P90 triggers an exit. The first
  observed minute wins; held P10 has precedence for an indistinguishable tie.
- The sell limit is current bid + $0.01, rounded up to a cent, post-only and
  reduce-only. Do not post the opposite buy until the sell has a candidate fill.
- Pending intent is cancelled/repriced on each completed minute. Only a strict
  trade-through at the NEXT consecutive observed minute qualifies as a candidate
  fill. Touches, missing-minute gaps and intraminute extrema cannot fill an order.
- Base size 1, multiply after held P10 exit by 2.5 if cumulative market net P&L
  remains negative, cap 100, reset after cumulative recovery. Other-side P90
  switches do not independently multiply size. Sizing never crosses markets.
- No percentage take-profit or fixed stop. Hold the remaining position through
  official settlement. Unfilled entries expire at close; no position is forced
  merely to claim every market traded or settled with a holding.

Minute quotes do NOT prove maker fills, queue position, order acceptance, fees or
available size. Every proxy fill is marked `execution_verified=false` and all
unfilled/repriced/expired orders are retained. Zero maker fees are a sensitivity
assumption, not a verified historical fee schedule. Settlement payout is not
charged a sell-order fee. An immediate reversal and a guaranteed maker fill are
incompatible guarantees; neither is promised here.

## Execution and recovery

Use `kalshi-btc-paper.yml`, `history_minutes=1`, a fresh lineage
(`resume_artifact_id=0`) and pinned code. First run mock checks, then a bounded
real historical smoke, then live. The existing 2→13 mode remains available via
`history_minutes=2` and its original checkpoint/code. Never restore a 2→13
checkpoint into the 1→14 configuration.

The watchdog independently checks both history-minute cohorts once their
verified checkpoints exist. Five-minute AES-GCM encrypted SQLite
snapshots use the existing artifact mechanism; newest two per run, 3-day retention.
Production paper checkpoints now upload full authenticated-encrypted snapshots
to the existing private research bucket, with only a small AES-GCM authenticated
recovery reference in GitHub. The wrapper restores the exact object generation
and verifies checksum and SQLite integrity before starting. Existing embedded
SQLite artifacts remain readable. No historical records or positions are reset.
The local safety limit is 512 MiB, cloud object limit 1 GiB; this is not unlimited
retention. Immutable cloud snapshots require monitored storage lifecycle planning
and normal storage charges. Source research records remain outside Firestore.
GitHub-hosted CPU workers retain the 345-minute handoff budget; no GPU or uptime
guarantee is claimed. Keep the previous lineage's archives for audit.

The comparison observer reuses the same forecast tape without rerunning models.
It retains the original 25 percentage-exit benchmarks, two new quote-fee
sensitivity benchmarks, and the separately named post-only candidate-fill proxy.
The encrypted report includes order/trade CSVs and complete configuration.

## Report definitions

Win rate = positive-net closed trades / all closed trades, not market settlement
accuracy. Return = net P&L / closed entry notional, not bankroll ROI. Open marked
P&L, coverage, missed starts, failed models and unfilled orders must accompany
results. Return rankings on a reused tape are exploratory, in-sample comparisons;
selecting the highest result among many variants increases selection bias.

## First-P90 hold monitoring across 13- and 14-minute cohorts

The comparison observer now keeps the first-P90/hold-to-official-settlement
experiment separately for first-two-minutes → 13 minutes and first-one-minute
→ 14 minutes. The watchdog checks both independently and will not silently
start a fresh lineage when a verified recovery checkpoint is missing.

Each cohort reports three sizing policies: one contract; 2.5x after a loss,
reset after a win; and 2.5x after a loss until the cumulative recovery cycle is
nonnegative. All are capped at 100. Only an outcome whose confirmation was
received strictly before the next entry can change that entry's size.
Legacy fractional sizing remains a benchmark. New execution-oriented scenarios
round down to whole contracts (1, 2, 5, 12, 30, 75, 100); fractional contract
eligibility is not assumed.

There are three separate execution datasets, never pooled:

- The preserved quote benchmark, for continuity with earlier reports.
- Prospectively collected completed-minute quotes received within 30 seconds;
  late/backfilled or previously untimestamped observations are excluded.
- Post-only candidate entries: ask minus one cent, refreshed each minute on
  the first selected side only, and held to official settlement if filled.
  Strict later quote trade-through is a candidate, **not verified maker
  execution**. Mere touches, missing minutes, and unfilled orders are reported.

The order ledger is unit-size candidate evidence, with sizing as a separate
analytical overlay. Actual exchange acceptance, queued size, liquidity,
market-specific fees/tick rules, and fills remain unverified. The existing 1%
entry-notional fee is disclosed as a sensitivity assumption, not a maker-fee
claim. This does not enable real trading or establish readiness for live funds.

New observations record collection time and mode without rewriting old records.
Official settlement records retain first-confirmed time. Encrypted artifacts
include the full analysis and separate hold trade/order CSVs. Firestore holds
small summary/pointer metadata, not quote histories or forecast arrays.
