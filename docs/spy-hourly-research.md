# SPY weekly hourly ensemble research

Read-only, retrospective research. No broker orders or change to production
forecasting. Run `python -m market_research.spy_weekly --output /private/research
--end-friday YYYY-MM-DD --weeks 4` in the locked inference environment, or use
the **SPY Weekly Hourly Ensemble Research** manual GitHub workflow.

Each Friday after the regular-session close, fit the existing five-model
ensemble to the most recent 500 completed hourly observations. Forecast each
regular-session bar until the next Friday close. The NYSE calendar excludes
holidays. Hours start at 09:30 New York time; the final 15:30–16:00 bin is a
30-minute partial hour. A holiday Friday is rejected instead of silently moved.

All five models must succeed: Prophet, Toto, Granite, Chronos, TimesFM. Equal
central weights; unsupported tails excluded with per-quantile renormalization.
Uses existing production adapters and log-space ensemble, not a new estimator.
Foundation model weights are current, so this is not a claim that these exact
forecasts or model training cutoffs existed before the historical outcomes.

The existing Quantura market-history service obtains raw regular-session
Alpaca IEX minute OHLC data. The study aggregates those real minutes; it never
invents missing observations. Missing opening/closing minutes invalidate a
bin. Interior missing IEX minutes are counted, not forward-filled. IEX is not
consolidated SIP data. Raw data and forecasts stay inside encrypted artifacts;
no third-party redistribution right is implied.

## Trading rule

- Hourly close ≥ its time-aligned P90: long one share.
- Hourly close ≤ its time-aligned P10: short one share.
- Reverse at the **next scheduled bar's open**, not the already observed signal
  price. Missing execution bars do not receive a later invented fill.
- Otherwise hold, including overnight and across weekly forecast refreshes.
- End-of-study liquidation at the last close is a labeled simulation assumption.
- Show gross and 1/5-basis-point-per-fill sensitivity; borrow, dividends,
  financing, taxes, spread, liquidity and real order execution remain unverified.

Returns use initial one-share SPY capital; they are not returns on margin or
annualized performance. Long/short outcomes and hourly marked drawdown are
separate. Signals without enough remaining weekly observations are censored.

## Excursions, not assumed target fills

After each new P90/P10 signal region, measure subsequent highs/lows through
Friday. Exclude the already completed signal bar. Report the frozen P99/P1
target at the signal and the time-varying forecast-tail path separately, along
with dollar gap closed. A high/low reaching a threshold is not proof a resting
order filled. Signal directional outcomes and actual simulated trade win rates
are distinct. Four weeks is exploratory and too small to establish an edge.

Every input, configuration, weekly quantile array, package version, seed,
simulation ledger and final report is immutable. Model failures stop the study;
partial output is encrypted with the existing research-artifact mechanism.

## Reversed signals and trailing-stop sweep

`python -m market_research.spy_exit_sweep --archive /private/study.zip
--output /private/new-study` reuses an authenticated/decrypted original archive.
It verifies the manifest, source hashes, five-model membership, 500-row training
hashes, and exact original baseline before comparing exit settings. There is
no new inference, source-data download, or change to production trading.

- Contrarian entries: short at P90 or higher, long at P10 or lower.
- Hourly close signals still fill at the next scheduled bar open.
- Primary trailing study monitors genuine minute closes; an hourly-close
  sensitivity is reported separately. No extrapolated tick path or ideal stop
  fill is assumed. Missing execution bars cannot fill retrospectively.
- Trails: none, 0.1%, 0.25%, 0.5%, 0.75%, 1%, 1.5%, 2%, 3%, 5%, 10%.
- Long trails follow the highest completed close; short trails follow the
  lowest completed close. Both start at entry and persist over weekly refreshes.
- Opposite hourly signal reversals take priority over simultaneous stop hits.
  Following a trailing exit, remain flat until a fresh hourly signal region.
- Fixed one share, no recovery multiplier. Every setting reports 0/1/5 bp costs,
  trade ledger, monitored drawdown, skipped bars, and the full equity path.

This chooses a best setting **in sample**, not the most profitable future live
strategy. Wide stops that never activate are ties with the no-stop baseline.
The CLI writes immutable JSON and a comparison CSV into a new output directory.
Use the existing encrypted artifact packager before uploading private reports.

## Crossing prices, averages, and options

`python -m market_research.spy_diagnostics --archive /private/study.zip
--output /private/new-study` exports weekly averages and a crossing ledger.
It distinguishes the first future P50, average P50, Friday actual close,
time-aligned crossing threshold, signal close, and next-bar fill. A first
forecast bar already outside its band is labeled separately from a subsequent
region entry. Not every signal adds a trade to an already-held position.

Quantile averages weight each forecast row equally, including partial final
session bars. A mean of quantile levels is not the quantile of a weekly average
or of the expiry price. Dollar/percentage distance is not an options Greek.

`python -m market_research.spy_options --diagnostics
/private/new-study/report-spy-crossings.json --output /private/new-study`
uses the existing Quantura Alpaca option-history endpoint. It requests a call
and put at the nearest $1 strikes to frozen P90/P10 averages, expiring the next
Friday. Actual returned history confirms contract existence. It does not claim
these are the nearest available strikes among every strike increment.

Compare the first common trade minute from 16:05–16:14 ET Friday with the first
common minute from 09:30–09:40 next NYSE session. Both use Friday's unchanged
forecast; the latter can be Tuesday after a holiday. The five-minute delay
avoids claiming a forecast using the 16:00 close is instantly executable at
16:00, but does not establish actual historical model completion times.

Premiums use option-bar opens, **not bid/ask quotes or verified fills**. The
provider reports an entitlement-default feed; do not call it certified OPRA.
Terminal value is a cash-equivalent intrinsic calculation from the archived
SPY IEX Friday close, not verified settlement/exercise or real closing orders.
One standard 100-share call plus one put is assumed, with no multiplication.
The result is a trade-bar pricing sensitivity, not an executable options
performance record. Bid/ask, fees, official exercise references and actual
execution are needed for that. Strike touches do not establish profitability;
both entry premiums must be deducted. Early profit-taking is a different test.

### Touch exits: separate legs versus whole pair

`python -m market_research.spy_option_targets --archive /private/study.zip
--diagnostics /private/new-study/report-spy-crossings.json
--output /private/new-study` downloads and checkpoints full option trade paths
through the expiry Friday. It compares both entry windows above with both exit
rules, on the same frozen forecast and verified historical contract symbols:

1. Sell the call when SPY reaches **average P90**; sell the put when SPY reaches
   **average P10**, independently.
2. Close both legs together when either average level is first touched.

The target is the exact average forecast level, not its rounded option strike,
and not a target on the option premium. Underlying regular-session minute
highs/lows establish a touch only after that minute is complete. Ignore the
entry minute because independent option/underlying OHLC cannot establish the
ordering of their trades. Reprice from the first subsequently observed option
bar open within five minutes, never the trigger bar or an ideal target fill.
Paired exits require both option bars in the same minute.

Any remaining leg receives an explicit Friday 15:55 time exit, with observed
pricing no later than 15:59. This avoids holding through expiry. Missing exit
prices are incomplete, not zero-priced or intrinsic-valued substitutions.
Reports show attempted/complete weeks and disallow full-sample rankings with
missing weeks. No overnight or after-hours target monitoring is claimed.

Full trade-bar paths, source checksums, every target/timed exit, per-leg prices,
and aggregate pair P&L are retained privately. The options bid/ask and execution
limitations above still apply: this is a price-bar approximation, not a record
of maker/taker fills or an investment recommendation. A $0.65 per contract per
fill sensitivity is labeled separately from before-cost results.
