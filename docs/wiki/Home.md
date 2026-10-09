# Quantura documentation

Quantura connects market observations, probabilistic forecasts and reproducible strategy research. Start with the [website](https://quantura.studio) or the [API documentation](https://quantura.studio/developers/api).

## Choose a guide

| Guide | Use it to |
| --- | --- |
| [Getting started](Getting-Started) | Install the project, run checks and navigate the product |
| [Forecasting](Forecasting) | Choose intervals, models, quantiles, cutoffs and horizons |
| [Market data and downloads](Market-Data-and-Downloads) | Select providers, quote sides and genuine observations |
| [Screener and game forecasts](Screener-and-Game-Forecasts) | Filter stocks/perpetuals and today's Kalshi/Polymarket games |
| [Strategy research](Research-and-Backtesting) | Understand public backtests, averaged baskets, costs and validation |
| [Architecture and operations](Architecture-and-Operations) | Locate code, deploy and operate private workers |
| [Release history](Release-History) | Review versioned changes and compatibility |

## v2.2.0

Clerk accounts and Pro (including an active 14-day trial)/enterprise/admin API permissions, Gemini/World Bank/Treasury/BigQuery data, CSV previews and named uploads, concise Research/About pages, three blog navigation controls, distinct post images, Bluesky posts and domain-verification assets are included. Firestore polling and unchanged billing writes are reduced. [Read the release](https://github.com/tamzid2001/stockssagemakerdata/releases/tag/v2.2.0).

## Earlier v2.1.0

Forecast now supports **1, 5, 15 and 30 minutes; 1 and 4 hours; daily, weekly and monthly** observations. API schemas, interval capabilities, worker calendars, downloads and saved configurations use the same frequencies. Weekly periods begin Monday UTC; monthly periods follow real calendar boundaries.

The release also brings the full Dukascopy instrument catalog, today's pregame ensembles, research matrices, product presentation and worker reliability fixes since v2.0.0. [Read the release](https://github.com/tamzid2001/stockssagemakerdata/releases/tag/v2.1.0).

The tracked source for this wiki lives in [`docs/wiki`](https://github.com/tamzid2001/stockssagemakerdata/tree/main/docs/wiki). The API reference is generated from [live OpenAPI](https://quantura.studio/api/openapi.json).
