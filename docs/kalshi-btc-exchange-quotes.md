# Genuine exchange quotes for BTC first32/60/90/120 research

This new tape is **not** the existing minute-candle tracker. It captures each
exchange best-bid/ask price or displayed-size change, retaining millisecond
timestamps, receipt times, and subscription sequence. Both YES and NO books are
derived from the same binary contract's official bid ladders. No polling snapshot
is promoted into a fresh exchange event.

- Multiple updates in one second remain separate.
- A quiet second creates zero observations, not a repeated price.
- Deep-book-only changes update the book without inflating the BBO quote count.
- An initial snapshot establishes book state but does not count as quote number1.
- The connection must receive its snapshot before opening. Late joins, sequence
  gaps, replays and out-of-order timestamps are recorded and cannot supply a
  complete opening prefix. An interrupted market is not silently restarted as
  if later updates were its first quotes.
- Actual first32/60/90/120 prefixes freeze independently for each side. They carry
  the elapsed seconds and seconds remaining, not a claim that N quotes=N seconds.
- Raw private account fields, signing keys, and auth headers are never archived.

## Run the collector

`.github/workflows/kalshi-btc-quote-tape.yml` supports `mode=mock` or `collect` and
30/60/120-minute bounded runs. It subscribes before upcoming BTC contracts open
and keeps capturing their BBO updates until market close or the run deadline.
There is no scheduled collector or unlimited handoff enabled by this change.

Set **Actions secrets**, never tracked files:

* `KALSHI_PROD_API_KEY` — API-key ID
* `KALSHI_PRIVATE_KEY` — RSA PEM signing key; a read-only key is preferable
* Existing `FIREBASE_SERVICE_ACCOUNT_JSON`, `QUANTURA_RESEARCH_ARTIFACT_KEY`

Vercel sensitive variables cannot be pulled back as plaintext. Their existence
does not configure a GitHub runner. WebSocket handshake and live first-N collection
must be tested with the actual configured credentials before claiming success.

The collector uses existing SQLite research storage and authenticated encrypted
private cloud backups. Each-minute checkpoints store full data privately; GitHub
receives small encrypted pointers, with at most the latest two from this run and
three-day retention. Collection does not need HF downloads or model inference.

## Forecasting and trading boundary

Frozen quote windows explicitly say `not_run_event_time_model_validation_required`.
They are data inputs, **not mock forecasts**. This workflow places no orders and
reports no win rate. The intended follow-up is all five approved models with equal
P90 weights, first-P90 entry and hold to settlement, with no ladder or stop.

Those models interpret regularly spaced steps; irregular exchange quotes need a
validated time mapping before “forecast to market close” is accurate. Do not insert
repeated prices to manufacture a one-second grid or silently label predicted quote
steps as seconds. Current production horizon guards also limit some adapters to
fewer than the seconds remaining in a 15-minute market. The quote-to-wall-time and
long-horizon methodology must be verified before running the five-model study.
Historical minute candlesticks cannot retrospectively reconstruct this quote tape.

Primary protocol references:
[authenticated WebSocket handshake](https://docs.kalshi.com/getting_started/quick_start_websockets)
and [orderbook snapshots/deltas](https://docs.kalshi.com/websockets/orderbook-updates).
