# Sports first-row replay

`sports-first-row-backtest.yml` is a separate paper-only population from the
15-minute crypto/commodity studies and archived P1 strategies. It uses the
existing dataset-download service and encrypted per-game checkpoint store.

Forecast every 30 minutes using up to 500 genuine completed-minute observations,
with a minimum of 32. Use pregame plus in-game history initially, then in-game
only after 32 elapsed in-game minutes. Missing rows are not filled; insufficient
in-game rows produce a recorded skip. All five configured models must succeed.

Freeze the first predicted row. Compare the first completed minute after model
publication: ask below P10 selects that position; ask above P90 selects the
complement in the **same binary market**. Otherwise remain neutral. Simulated
execution uses the next minute's book. Hold to settlement or switch on a new
opposing signal; do not backdate entries to an unavailable forecast.

## Kalshi identity and settlement

The catalog may represent a game with one, two, or three binary markets: two,
four, or six YES/NO positions. Preserve every full ticker and side. Validate
complete pairs and event membership; never silently discard all Kalshi games
because their sides are not Polymarket's `long/short` labels. A NO position is
not automatically renamed as another team, especially in a three-way event.
Conflicting target positions are explicitly skipped rather than selected by
looking at subsequent outcomes.

Verify payouts using the official [market detail](https://docs.kalshi.com/api-reference/market/get-market)
or [historical detail](https://docs.kalshi.com/api-reference/historical/get-historical-market).
Check ticker, event, binary type, settled status, result, settlement value when
provided, and settlement timestamp. Provisional, partial or conflicting outcomes
remain unverified, never inferred from the last price. YES and NO payouts are
complementary; only final confirmed outcomes close a simulated settlement trade.

## Limitations and costs

The 1% fee in this sports campaign is an explicit sensitivity assumption, not
verified Kalshi/Polymarket execution fees. Minute books do not prove fills or
depth at the requested size. Recovery sizing starts at one contract, multiplies
by 2.5 and floors to whole contracts after a loss, caps at 100, and resets when
cumulative game recovery P&L reaches zero. Quote/forecast archives are immutable
and encrypted; Firestore stores small manifests and leases, not forecast arrays.
Provider redistribution status remains review required.
