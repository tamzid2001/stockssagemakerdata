# SPY Alpaca daily P99 share-ladder yearly study

The manual `spy-p99-share-year.yml` workflow freezes Alpaca data once, forecasts
each completed daily origin and replays a $100,000 unlevered share account.
The default forecast origins are October 7, 2025 through October 6, 2026;
execution observations extend through the completed October 7, 2026 session.

## Data and models

- Exactly 500 genuine, completed, split-adjusted Alpaca daily SPY candles before
  each origin; missing daily history invalidates the source.
- Seven future NYSE sessions, once per completed trading day, using Prophet,
  Toto 2.0 4m, Granite, Chronos and TimesFM with registry-pinned checkpoints.
- P01, P10, P25, P50, P75, P90 and P99. The models supporting tails supply
  P01/P99; unsupported native tails are not extrapolated.
- Historical SIP is the default; IEX can be chosen explicitly. Authentication
  or entitlement failures stop the job without changing the feed.
- Alpaca's exchange calendar supplies market hours, holidays and early closes.
  Minute bars outside regular hours or after the exchange close are excluded.
  Model inputs have daily date labels; actual close timestamps and measured
  inference latency control availability. A forecast starts 15 minutes after
  the close and can trade only on a later observed regular-session minute.
- Raw and split-adjusted closes in the execution year must agree. A split
  requiring a share-count rebase invalidates this version of the study.

Alpaca documents the [historical feeds](https://docs.alpaca.markets/us/docs/historical-stock-data-1)
and [stock bars](https://docs.alpaca.markets/us/reference/stockbarsingle-1).

The existing FTMO workflow can also call this isolated study using
`spy_shares=true` and `stock_feed=sip`; it skips its FTMO jobs in that mode.

## Strategy

The initial signal is **completed cutoff close strictly above first predicted
P99**. Equal prices do not qualify. At the next executable regular-session
minute, buy whole shares, with one share as the minimum and share step.

The ladder shares one percent of account equity. Its reference width is the
**final P99 minus final P01** of the triggering seven-session forecast. The
stop remains that forecast's final P01 for the life of the basket. Planned
lower levels share the budget; sizing rounds down and is additionally capped
by available unlevered buying power. An entry below the stop is rejected.

Average only lower, with no P90 gate. An observed grid breach creates a limit
eligible from the next observed minute. Activate trailing only when the basket
has positive modeled net P&L. The distance is one grid for a single entry or
75% of the highest-minus-lowest filled entry range for multiple entries.
Carry positions until exit; after exit, wait for a later qualifying forecast.

Grids are $0.25, $0.50, $1, $2, $5 and $10, each with equal, larger-deeper and
smaller-deeper quantity profiles. Both low-first and high-first minute OHLC
paths are preserved. Selection uses the first nine months; the final quarter
is a fresh-flat holdout. All configurations remain in the yearly artifact.

## Execution assumptions and outputs

These are simulated share fills from trade bars. Trade OHLC is rounded up to
cents as an ask proxy; modeled bid is ask minus an assumed one-cent spread,
with a two-cent spread sensitivity. The source contains no historical NBBO,
order-book queue or exact tick ordering. Commission is modeled as zero;
regulatory fees, dividends and interest are excluded. Reported returns are
price P&L under these assumptions, not verified broker total returns.

The report includes net price P&L, peak-to-trough equity drawdown, basket and
entry wins/losses, win/loss streaks, entry price, share exposure, ladder depth,
duration, minimum-share rejections and marked open positions. Missing model
origins prevent a complete report. Source and forecast identities are checked
before replay. Data and results use GitHub artifacts; there are no Firestore
writes, brokerage orders or connections to the live traders.
