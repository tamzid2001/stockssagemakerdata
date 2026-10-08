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
has positive modeled net P&L AND executable bid reaches the lowest fill plus
the trailing distance. The distance is one grid for a single entry or 75% of
the highest-minus-lowest filled entry range for multiple entries. A $666.82
single entry with a $2 grid arms at $668.82 bid. Its trailing stop is floored
one cent above the lowest fill and never loosens; the fixed P01 remains active
before trailing arms.
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

## Sell-only comparison with frozen forecasts

The replay CLI accepts `--side short`. Keep the **same above-first-P99 signal**,
sell whole shares at the modeled bid, and cover at the modeled ask. The fixed
stop is the triggering forecast's **final P99**, rounded upward to cents;
the final P99-minus-P01 width still sizes the entire 1% basket budget. Reject
entry when the fixed stop is at or below the sell price. Average only higher,
using the same causal next-minute limit rule. A profitable short's trail follows
the lowest cover ask plus one grid, or 75% of the filled-entry range for multiple
legs; it only ratchets downward. Both physical OHLC path orders are replayed.

Reserve 100% gross notional as collateral, with no extra capacity from short-sale
proceeds. Historical availability to borrow, locate/borrow fees and dividend
payments owed by short sellers are not established by the saved bars. This is
a hypothetical short price-P&L comparison, not a broker execution claim. See
Alpaca's [short-selling documentation](https://docs.alpaca.markets/us/docs/margin-and-short-selling).

Download the source and all forecast artifacts from run `37826329337`, then:

```sh
python -m market_research.spy_p99_share_study replay \
  --side short --source source --forecasts forecasts --output short-report
```

This does not rerun the models or submit orders. The report distinguishes initial
entries blocked by an invalid fixed upper stop from allocations below one share.
Use the original first-nine-month selection and fresh-flat holdout methodology;
do not select a short profile using the completed full-year outcomes.

## Offline drawdown and sizing sensitivity

This section preserves the superseded immediate-profit trailing rule for
historical reproducibility. Its returns are not corrected-strategy results.
Use `market_research.spy_trailing_audit` to compare corrected $1/$2 grids
against the exactly reproduced legacy $2 baseline from frozen artifacts.

`market_research.spy_drawdown_sizing` reuses the original SPY source and all
251 saved forecasts for the $2 equal long grid. It checks source hashes and
requires the multiplier-1 replay to reproduce every original result field.
It does not rerun models or submit orders.

```sh
python -m market_research.spy_drawdown_sizing \
  --source source --forecasts forecasts \
  --baseline original-report/results.json.gz --output sizing-report
```

The original replay closed 82 baskets with 82 entry orders: every basket filled
one purchase, with no subsequent averaging purchase. A purchase can contain
multiple shares. The $2 grid schedules potential lower additions; an observed
breach creates a limit that is eligible on the next observed minute. It does
not guarantee that an additional purchase fills. Both minute paths produced
80 trailing-stop exits and two gap-stop exits.

The experiment varies the planned whole-ladder risk budget independently of
actual account equity. Only multiplier 1 retains the original 1% budget. All
cases keep whole shares and cash buying power capped to 1x actual equity.
Existing workflow and API callers retain multiplier 1; this experimental
parameter is not exposed in their configuration.

Arithmetic scaling of the original trades to $10,000 historical drawdown
suggests $11,424.46-$11,568.45 price P&L, but requires about $1.26 million peak
gross SPY exposure. It is not executable in the $100,000 cash-only account.
Among the tested larger-budget replays, the highest full-year worst-path profit
was $2,420.79-$2,516.00, with $2,818.20 worst equity drawdown and $300.76 worst
daily loss. This case used a 100% *theoretical planned-ladder budget*, constrained
by actual cash. It changes the original 1% risk rule and is an aggressive
retrospective sensitivity, not a recommended allocation. A two-cent spread
reduces that case to $2,309.58-$2,403.30.

The $10,000 peak drawdown ceiling, $5,000 Prague-midnight daily loss ceiling,
$90,000 static equity floor and buying-power constraints reject cases after
replay. They do not cause forced liquidation or stop trading. Selection uses
the whole year and has no fresh holdout. Historical drawdown does not bound
future losses. The original forecast calibration and cost limitations remain;
these SPY share results cannot be transferred directly to FTMO US500 CFDs.
