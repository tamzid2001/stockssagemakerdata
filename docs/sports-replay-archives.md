# Private sports replay archives

Both download workers reuse Quantura's existing prediction-market catalog and
CSV/JSON export service. They do not submit exchange orders. They do not run
models or treat dataset availability as evidence of profitability.

## Data integrity

- Polymarket US: each game's original moneyline display quotes retain both
  `long_price` and `short_price`. The catalog retains the two side IDs and source
  outcome labels. These are book-derived display prices, not historical trades.
- Kalshi: each team's YES market remains a separate market ticker under its
  event. A NO contract is **not** relabeled as the opposing team; draws and
  multi-outcome games make that unsafe.
- Missing minutes remain missing. Original timestamps, available bid/ask OHLC,
  trade OHLC/mean/previous, fractional volume and open interest are preserved.
  Prices normalize to decimal units, supporting current dollar strings and
  legacy integer-cent candle fields. Unknown values remain null.
- Settlement result/value/time are catalog annotations, separate from historical
  forecast inputs. A final settlement result must never become a known-future
  covariate or determine a historical entry.
- Polymarket replay evaluation currently requires two consecutive genuine
  minute observations at an origin. Gaps restart model context; they do not erase
  the downloaded source archive. This is a stricter evaluation rule than download
  retention. Two points are not enough to establish model reliability.

## Discovery and resumption

`polymarket-replay-archive.yml` and `kalshi-replay-archive.yml` are independent
GitHub-hosted CPU jobs/concurrency groups. Both default to mock safety tests;
`smoke_mode=real` enables actual private downloads. No public data artifacts or
GitHub model caches are produced. Results are now **encrypted compressed GitHub
artifacts**, not Firestore documents.

The collector now persists and downloads one discovery page before requesting
the next. Each page has an immutable catalog and run report; each market attempt
has an immutable result and progress record in runner-local SQLite. A failed
request cannot invalidate previously stored data; the always-run artifact step
preserves available partial results. Existing Firestore history is preserved
pending verified migration, not silently removed.

To resume an incomplete page, supply its `catalog_id` and `market_offset` from
the report, plus its retained `resume_artifact_id`. Restoration authenticates the
encrypted ZIP and checksum, retains the catalog/configuration in a new bounded
shard, and leaves prior results in the original ZIP. For another page, leave the catalog blank and supply
the exact `next_cursor`. Polymarket cursors are offsets; Kalshi cursors are JSON
objects containing the Sports series and provider event cursor. Inputs are passed
through environment variables and validated, never interpolated into shell code.

Kalshi optionally accepts `series_ticker` for a representative or explicitly
selected Sports series. Blank means the conservatively classified full-game
`GAME`, `MATCH`, or `MONEYLINE` series, excluding period/prop identifiers. The
coverage report includes all Sports series discovered and the eligible count.
It does **not** claim coverage of every sports proposition. One event catalog
page is fetched at a time, including settled events whose nested markets have
moved to the historical tier. Historical markets are joined by event ticker.
An anomalous event exceeding 1,000 archived markets fails explicitly rather than
being silently truncated. Long ranges exceeding the download service's 90-day
bound require partitioning; normal game windows remain bounded to 48 hours.

Provider absence, retention gaps, unavailable game starts and failed downloads
are reported separately. Completed discovery does not mean complete usable
history. Jobs stop before the GitHub runtime limit; continuation is resumable,
not a guarantee that every game is downloaded in one run.

## Storage and access

Historical backtests and both replay archive workflows have **no Firebase
credentials**. They use runner-local SQLite (compressed records), then ZIP and
authenticated AES-256-GCM encryption before GitHub upload. Only `research.sqlite3`,
`report-*.json` and a checksum manifest enter the inner ZIP. No raw credential,
environment file or Hugging Face cache is allowed. Provider-specific keys prevent
symbol collisions. Live paper monitoring still uses small durable Firestore
checkpoints; this change does not remove its handoff state.

The encryption key is the GitHub Actions Secret
`QUANTURA_RESEARCH_ARTIFACT_KEY`, backed up in the owner's macOS Keychain under
service `Quantura Research Artifact Encryption`, account
`tamzid2001/stockssagemakerdata`. Never print or commit it. To decrypt, load that
key into the same-named environment variable and run:

```bash
python -m market_research.artifact decrypt --source research.qra.enc --output research.zip
```

Artifacts expire after **three days**. Download them before expiry; these are
short-lived research outputs, not permanent archival guarantees. Each encrypted
payload is capped at **25 MiB**; collection pauses at approximately 20 MiB of local
database size so reports fit. Resume into another shard, keeping earlier ZIPs.
Public-repository readers may download artifacts, but cannot decrypt them without
the key. Immutable GitHub uploads do not overwrite earlier releases. Short retention
and compression reduce storage use but do not guarantee zero GitHub storage cost.

The website's existing Sports/Prediction Markets CSV/JSON download flow remains
the consumer interface for selected contracts. The all-history collector is an
administrative worker, **not a new public one-click bulk-export entitlement**.
Raw archive redistribution remains `review_required`; API availability does not
grant commercial redistribution rights. Historical outcome metadata must be
excluded from SageMaker target/covariate input until its outcome timestamp.

## Official endpoint review (2026-09-10)

- [Sports series](https://docs.kalshi.com/api-reference/market/get-series-list)
- [Events, including archived-market events](https://docs.kalshi.com/api-reference/events/get-events)
- [Historical markets and cursor filters](https://docs.kalshi.com/api-reference/historical/get-historical-markets)
- [Historical candles: 1, 60 or 1440 minutes](https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks)
- [Tier cutoff semantics](https://docs.kalshi.com/getting_started/historical_data)

Market/candle tiering follows **market settlement time**, not a split at each
candle timestamp. Recent markets use the live candle path; absent live history
falls back to the archived candle path. Trades have a separate creation-time
cutoff. Event start is derived only from verified linked sports milestones, never
from expiration as a substitute.
