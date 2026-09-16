# BTC P90 + sticky direction: next-minute taker research

This is a separate, versioned **paper** comparison for the existing 1→14 and
2→13 minute forecast lineages. It does not replace historical maker or flat-fee
benchmarks, place orders, or prove executable returns.

## Direction is independent of quotes and forecasts

The binary sticky rule is equivalent to choosing the opposite of the latest
confirmed settled outcome. If that choice is wrong, it is still the opposite
of the new outcome; if correct, it flips. Every settlement updates this state,
including markets without a forecast, P90 signal, or trade. The collector reads
settled market metadata separately from model execution.

Record the actual first receipt of each confirmed result. A later historical
retrieval is **not** knowledge available earlier. Revisions are separate events;
they do not rewrite earlier decisions. An unconfirmed result cannot change the
next trade size. No prior confirmed outcome means no agreement-filtered entry.

## Minute archive

An independent network collector retains completed-minute YES bid/ask and NO
bid/ask from market open through close, even if forecasting fails or is slow.
Kalshi's binary book gives NO ask = 1 − YES bid and NO bid = 1 − YES ask; this is
explicitly labeled, not presented as a second independent feed. The collector
does not interpolate missing minutes. It stores first receipt, source, missing
timestamps and provider corrections separately. Only live minute records
received within 30 seconds qualify for the prospective comparison. Older candle
backfills remain useful research data but are not promoted to live evidence.

## Entry, sizing and ladder

- Latch the first unambiguous observed minute bid at or above its time-aligned
  P90 after both forecasts were published. If that side disagrees with the
  directional state, skip the market rather than shop for a later signal.
- Use the **next consecutive minute's ask** for the initial taker-fill proxy.
  Missing, late, closing-minute and untradeable quotes do not create fills.
- Hold to the official settlement. There is no P10 switch, TP or stop loss in
  these variants.
- Compare fixed one contract with recovery sizing: start at 1, multiply by 2.5
  and round down to whole contracts after a confirmed net loss, cap at 100.
  Keep that size after a partial recovery win; reset when the recovery cycle
  reaches nonnegative net P&L. Never share sizing state across forecast cohorts.
- A **separate** recursive ladder starts with one contract and uses 2/4/8/16…
  contracts at 10¢ increments below the initial ask, stopping before levels
  below 10¢. Total filled exposure cannot exceed 100. Only one rung is active.
  A minute ask at/below the rung triggers a next-minute marketable-limit proxy,
  filled only if that next ask still fits the rung price cap. A fill arms the
  next rung; the same candle can never fill multiple rungs. No cross-market
  recovery multiplier is applied on top of this ladder.

Snapshot asks do not establish depth, queue position, actual fills or account
fees. Reports use the fetched series quadratic taker fee and both 4-decimal and
cent balance-rounding sensitivities. Each fill is treated as one aggregate fill;
partial-fill fee accumulators are not modeled. Current fee metadata applied to
older trades is explicitly a sensitivity, not a historically verified schedule.
Missing fee metadata suppresses financial results instead of assuming zero fees.

Sources: [fee rounding](https://docs.kalshi.com/getting_started/fee_rounding),
[fee schedule](https://kalshi.com/docs/kalshi-fee-schedule.pdf),
[candlesticks](https://docs.kalshi.com/api-reference/market/get-market-candlesticks).

## Forecast recovery and rollout

Live transient inference failures get at most one retry before the market
deadline, using the original input snapshot. Publication is the real completion
time, never the original cutoff. Validation/license failures are not bypassed.
Only a complete YES/NO forecast pair is published. A retry cannot promise a
forecast before close; failed/missed deadlines remain visible in the ledger.
Toto remains excluded from these short-context experiments.

The existing encrypted checkpoint includes `btc_paired_minutes.csv.gz`,
`btc_sticky_tracking.json.gz`, all minute/settlement records, and attempt history
in SQLite. Price arrays are not placed in Firestore. The wrapper continues to
put the large encrypted snapshot in private storage and only the small recovery
pointer in GitHub artifacts.

For a code upgrade while a worker is running, dispatch the same horizon with
`resume_run_id` set to that running job and `resume_artifact_id=0`. The concurrency
group waits for it to finish. Only then is its newest verified checkpoint
resolved. A missing checkpoint or wrong-horizon source fails closed. Normal
handoffs continue using pinned code and checkpoint IDs. GitHub queue/setup times
can still create collection gaps; this is not a zero-downtime guarantee.
