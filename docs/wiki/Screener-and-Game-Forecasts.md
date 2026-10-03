# Screener and game forecasts

Screener combines stocks, perpetual contracts and **today's available game forecasts** through its data-source selector. Use its integrated search and filters rather than a separate Search widget.

## Stocks and perpetuals

Stock filters compare the latest observed price with the selected quantile or forecast range. Keep the distinction between the maximum P50 across a horizon and the P50 at a selected timestamp. Missing prices/quantiles cannot qualify for price comparisons. Homepage stock previews prioritize mega-cap names.

Perpetual prices are normalized to underlying units. A market reference quote is not a completed trade or fabricated forecast; unavailable quantiles remain unavailable.

Stock scans run after the latest completed NYSE close. Hourly recovery checks compare that exchange session with the last validated publication, so delayed GitHub runs and UTC midnight do not skip or misdate updates. Every chunk uses that same close; incomplete coverage preserves the previous publication and remains eligible for retry.

Aggregation reads the checked-in model registry directly and is tested without Python site packages, keeping publication independent of inference dependencies. The October 3 recovery republished genuine October 2 inputs: 3,568 valid five-model stock forecasts out of 3,607 stocks (98.92% coverage), with 39 unavailable histories/predictions explicitly retained as unavailable.

## Today's games

- Filter Kalshi-only or Polymarket US-only cards, or inspect matched game outcomes from both providers.
- Select the actual Yes/No or named outcome. Quotes and quantiles belong to that specific side.
- Open the card's market link to inspect the actual provider market.
- **View forecast** opens the saved distribution and historical overlay on Forecast. Viewing a game does not lock the general form; users can select another market or stock.
- Signed-in saved requests appear in the profile's My requests area alongside account workflows.

The daily game view uses verified game starts and today's America/New_York date, not contract expiration dates. Hourly GitHub Actions runs before the game-start hour and stops adding pregame forecasts at that hour. The last available forecast remains viewable today with a horizon through **kickoff plus four hours**.

## Ensemble and coverage

Eligible game forecasts use Prophet, Granite and Chronos plus available TimesFM/Toto participants, for a genuine four- or five-model ensemble. Context and licensing gates apply. P01/P25/P50/P75/P90/P99 are combined with per-quantile support; unsupported tails are not fabricated.

Sparse or insufficient-history markets can be excluded with explicit coverage information. Partial discovery during provider rate limits is not a complete census of today's games. A valid summary/artifact is produced even when no games qualify.

[Pregame methodology](https://github.com/tamzid2001/stockssagemakerdata/blob/main/docs/pregame-game-forecasts.md) · [Forecasting](Forecasting)
