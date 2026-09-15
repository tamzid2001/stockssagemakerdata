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
