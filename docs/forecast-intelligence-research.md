# Forecast Intelligence Research and Architecture Decisions

Last reviewed: 2026-08-29

## Objective

Forecast Intelligence must answer a narrowly defined question: given only information available at an analysis timestamp, which validated numerical forecast is best for the requested trading-session horizon, what distribution scenarios does it imply, and how did the locked methodology perform out of sample?

GPT-5.6 Luna is the interpretation layer. It does not generate or alter numerical targets. Prophet, Chronos-2, calibration, ensemble selection, point-in-time indicators, and deterministic evaluation produce the quantitative evidence.

## Existing repository audit

The authoritative deployment branch is `main`. The existing web application already has:

- a five-quantile Meta Prophet contract (`P1`, `P25`, `P50`, `P75`, `P99`), chart transformations, deterministic scenarios, and optional cached GPT analysis;
- point-in-time rolling indicator calculations for RSI, MACD, SMA, EMA, Bollinger Bands, ATR, realized volatility, ROC/momentum, OBV and related indicators;
- Yahoo historical-price retrieval and secure server-side Alpaca integration;
- Firestore storage for forecast requests, cached AI analyses, and backtests;
- an actual Python Prophet implementation in the legacy Python service;
- a TypeScript `/forecast/run` implementation currently named as Prophet but implemented as a closed-form lognormal drift/volatility model.

The last item is the main model-integrity problem. The statistical fallback remains useful as a baseline, but must not be labeled Prophet. This redesign gives every model and quantile an explicit provenance and makes unavailable models fail transparently instead of silently changing identity.

No existing production Chronos-2 implementation or genuine walk-forward model comparison was found. Existing diagnostic numbers in the TypeScript fallback are one-step, in-sample-style summaries and are not sufficient to select a best model.

## Official implementation research

### Prophet

Prophet documents simulated historical forecasts as repeated fits at historical cutoffs, using only data through each cutoff. Its uncertainty intervals are quantiles of a posterior predictive distribution estimated with Monte Carlo sampling. Therefore:

- the Prophet adapter fits log price using data through the supplied `as_of` timestamp;
- future dates come from the exchange calendar;
- `P1`, `P25`, `P50`, `P75`, and `P99` are computed directly from Prophet posterior predictive `yhat` samples, then transformed back from log-price space;
- rows are rejected if finite ordered quantiles cannot be produced;
- validation uses explicit historical origins rather than the model's in-sample fitted values.

Sources:

- [Prophet diagnostics and historical cutoff cross-validation](https://facebook.github.io/prophet/docs/diagnostics.html)
- [Prophet uncertainty intervals](https://facebook.github.io/prophet/docs/uncertainty_intervals.html)

### Chronos-2

Amazon's official `amazon/chronos-2` checkpoint is a 120M-parameter model exposed through `Chronos2Pipeline`. It supports zero-shot univariate, multivariate, past-covariate, and known-future-covariate forecasting. The official implementation also exposes LoRA and full fine-tuning.

The checkpoint's default trained quantiles are central deciles. The official pipeline warns that requested quantiles outside its trained range are set to the minimum or maximum trained level. Consequently:

- the default checkpoint is `amazon/chronos-2`;
- normal operation uses zero-shot mode;
- historical price and point-in-time indicators may be past-only covariates;
- only deterministic calendar/session fields may be known-future covariates;
- future RSI, MACD, volume, volatility, and price are never supplied as known-future values;
- Chronos P25/P50/P75 are retained with model provenance; interpolated central quantiles are identified as such;
- clamped P1/P99 outputs are never presented as native Chronos tail forecasts;
- final ensemble tail scenarios come from genuine Prophet posterior tails and/or validation-calibrated ensemble residuals, with provenance;
- `chronos_finetune_mode` supports `none`, `lora`, and `full`, defaulting to `none`.

Sources:

- [Amazon Chronos forecasting repository](https://github.com/amazon-science/chronos-forecasting)
- [Official Chronos-2 pipeline implementation](https://github.com/amazon-science/chronos-forecasting/blob/main/src/chronos/chronos2/pipeline.py)
- [Amazon Chronos-2 model card](https://huggingface.co/amazon/chronos-2)
- [Chronos-2 technical report](https://arxiv.org/abs/2510.15821)

### GPT-5.6 Luna

OpenAI documents `gpt-5.6-luna` as supporting the Responses API, configurable reasoning effort, function calling, and Structured Outputs. Standard fine-tuning is not supported. OpenAI recommends schema-enforced outputs, bounded tool policies, representative evals, and measuring quality, latency, tokens, and cost rather than assuming maximum reasoning is best.

Decisions:

- model ID is configurable and defaults to `gpt-5.6-luna`;
- optimization metadata is `prompt_fewshot_rag_tools`, never fine-tuning;
- output uses strict JSON Schema;
- prompt, few-shot examples, RAG policy, and tool policy are independently versioned;
- reasoning effort is configurable and selected on validation evidence;
- live paid calls are opt-in; normal CI uses a deterministic mock;
- numerical targets are copied from the selected numerical forecast and enforced again after model output parsing;
- confidence is capped by model agreement, interval width, calibration, and validation sample size.

Sources:

- [OpenAI model comparison: GPT-5.6 Luna capabilities](https://developers.openai.com/api/docs/models/compare)
- [OpenAI GPT-5.6 model guidance](https://developers.openai.com/api/docs/guides/latest-model)
- [OpenAI Responses API and Structured Outputs](https://developers.openai.com/api/reference/cli/resources/beta/subresources/responses)
- [OpenAI Evals API](https://developers.openai.com/api/reference/java/resources/evals/methods/create)

### Trading calendars and chronological validation

`exchange_calendars` is the authoritative session generator for supported exchanges. Daily horizons are session counts, not calendar-day offsets. Historical splits are chronological, with a purge/embargo at least as large as the target horizon when outcome windows overlap. Scikit-learn's `TimeSeriesSplit(gap=...)` documents the same ordering and gap principle; this project implements an explicit origin generator because forecasting targets vary by horizon.

Sources:

- [exchange_calendars implementation](https://github.com/gerrymanoim/exchange_calendars)
- [scikit-learn TimeSeriesSplit](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html)

## Canonical pipeline

```text
As-of OHLCV market data
  -> point-in-time indicators
  -> Prophet posterior forecast + Chronos-2 forecast
  -> per-model walk-forward scores and quantile calibration
  -> validation-selected, horizon-specific ensemble
  -> best validated numerical forecast
  -> optional as-of-safe RAG and approved tools
  -> GPT-5.6 Luna structured synthesis
  -> deterministic confidence/target guardrails
  -> next-bar deterministic trade backtest
```

## Horizon model

Supported labels map to XNYS sessions:

| Label | Sessions |
| --- | ---: |
| `1_trading_day` | 1 |
| `3_trading_days` | 3 |
| `5_trading_days` | 5 |
| `10_trading_days` | 10 |
| `2_weeks` | 10 |
| `1_month` | 21 |
| `3_months` | 63 |

The actual target date is the Nth exchange session after the latest permissible observation. Hourly support remains explicit and uses executable bars; it is not inferred by multiplying daily horizons by 24.

## Quantile calibration and ensemble selection

For each model, horizon, and quantile `q`, evaluation stores pinball loss and empirical coverage `mean(actual <= forecast_q)`. Calibration error is `observed_coverage - q`.

Candidate Prophet/Chronos weights are selected on validation origins using a combined objective composed of normalized weighted quantile loss, MASE, and absolute calibration error. The selected weight is locked before final holdout. Candidate weights are never selected using final-period returns.

Central Chronos quantiles and Prophet quantiles are linearly combined only when both are valid. Ensemble P1/P99 use validation-calibrated tail residuals when Chronos lacks genuine tail support. Each output contains quantile-level provenance (`prophet`, `chronos_native`, `chronos_interpolated`, or `calibrated_ensemble`). Quantile rows must satisfy `P1 <= P25 <= P50 <= P75 <= P99`; invalid rows fail validation.

## Point-in-time and leakage policy

- Every snapshot has an explicit mode: `live` or `backtest`.
- Market rows after `as_of` are rejected before features are computed.
- All rolling indicators are trailing-only.
- Prophet training and Chronos context end at the origin.
- RAG documents require publication and ingestion timestamps; backtests filter both to `<= as_of`.
- Backtest tools must declare point-in-time support and receive `as_of_timestamp`; live-only tools are rejected.
- Signals computed with a close execute no earlier than the next bar open.
- final holdout runs require a locked configuration marker and do not tune parameters.

## GPT prompt and context policy

The static prompt and few-shot prefix precede dynamic input to improve caching. Curated examples cover bullish, bearish, neutral, model disagreement, wide/narrow uncertainty, indicator conflict, and contextual contradiction without revealing realized future outcomes in example inputs.

Tool results are typed and bounded. Repeated calls are cached, identical calls are deduplicated, and `MAX_TOOL_CALLS_PER_ANALYSIS` stops loops. RAG results are capped by `MAX_RAG_RESULTS`. GPT requests are capped by `MAX_GPT_CALLS`. Budget exhaustion produces a structured safe failure without blocking the numerical forecast.

## Backtest assumptions

Strategies are evaluated separately: buy-and-hold, indicator-only, Prophet-only, Chronos-only, ensemble-only, GPT quantitative-only, and GPT with RAG/tools. Entries occur at the next bar open. Commission, half-spread, slippage, and short-borrow assumptions are centralized in `config/backtest.yaml`. Reported results are net simulated historical results, never profitability guarantees.

## Production deployment boundary

The web API consumes a typed Forecast Intelligence service result. Local and GitHub workflow execution use the Python engine directly. Chronos model weights are never bundled into the browser or Vercel function. Production Chronos should run on a configured CPU/GPU inference host or SageMaker endpoint. If no inference endpoint is provisioned, the service reports Chronos unavailable and does not relabel another model as Chronos.
