# Quantura Metaculus bot

The Quantura bot discovers open questions in active competitions where Metaculus
explicitly includes bots and the account has forecast permission. It scans every
page, unpacks group questions, handles conditional outcomes separately, and checks
each question's own opening/closing times. Newly eligible competitions are picked
up automatically. The officially documented bot-friendly Metaculus Cup and AI 2027
tournaments are included too; bots can forecast there but do not qualify for prizes.
Other human tournaments that exclude bots from competition are excluded.

## Operation

- Workflow: [Quantura Metaculus Bot](https://github.com/tamzid2001/stockssagemakerdata/actions/workflows/metaculus-bot.yml).
- Schedule: minutes 7, 27 and 47 of every hour, including weekends. GitHub can delay
  or drop scheduled runs; this is periodic automation, not a continuously running
  process or a guarantee to meet every short deadline.
- Secrets: `METACULUS_TOKEN` and `OPENROUTER_API_KEY` in GitHub Actions. Never put
  either in source files, browser code, inputs, or artifacts.
- Enable: repository variable `METACULUS_BOT_ENABLED=true`.
- Participation form: `METACULUS_PARTICIPATION_FORM_CONFIRMED=true` after submission.
- Pause: set `METACULUS_BOT_ENABLED=false` or disable the workflow. Existing forecasts
  remain on Metaculus; pausing does not erase them.

Each question receives one forecast. Subsequent runs skip authoritative own
forecast history. A private reasoning note records the exact generated answer
before submission, allowing a restart to reuse it without regenerating a different
answer. Uncertain writes are checked against Metaculus; writes aren't blindly
retried. Notes include engine, model, UTC time, real source links and data hashes.
Metaculus may publish private tournament notes after questions close, as its rules
require. Actions artifacts contain operational summaries, not private reasoning.

The unscored **bot-testing-area** supports a manual smoke test. Validate there
before running `scope=competitions, submit=true`. Follow the
[competition rules](https://www.metaculus.com/notebooks/38928/ai-benchmark-resources/):
no human adjustment of individual forecasts, no rerunning a forecast because its
probability is disliked, and no tuning against previews of open tournament forecasts.

## Free models and capacity

The router checks the current OpenRouter catalog and permits only explicitly free
entries with zero listed prices. It checks the key's free daily quota before each
generation. It sends no paid search plugins, tool calls, or paid fallback models.
If quota is exhausted or the service is unavailable, work is deferred. As configured
on October 10, 2026, the account allows **50 free model requests/day**; the actual
API quota is authoritative and may change. The default batch is four new LLM
questions and up to four numerical time-series questions per run. Invalid answers
and abstentions are reported. Complete coverage cannot be promised when free
capacity is lower than the question backlog or questions close before processing.

Binary and multiple-choice probabilities are validated. Numeric, discrete and date
forecasts use the official `forecasting-tools` distribution conversion, including
logarithmic scales, open/closed bounds and the question's exact CDF size. Numeric
answers use original units; dates use UTC timestamps.

## Quantura time-series route

The time-series job reuses `ensemble_forecasting.worker.execute_job` and its
production Prophet, Toto, Granite and Chronos adapters; TimesFM is included only
when the existing access/licensing configuration permits it. It logs the models
that actually completed and declines an ensemble with fewer than two models.
Supported tails are combined with the existing per-quantile weights. It uses up to
500 genuine observations and forecasts the **date named in the resolution criteria**,
not the later date when Metaculus plans to resolve the question.

The first automatic source adapter covers Silver Bulletin's publicly downloadable
Trump approval series when that exact source and a clear target date are named in
the question's criteria. Approval and net approval are distinguished. This public
series is fetched from its current Datawrapper chart; archived chart versions,
future rows, stale data and duplicate/out-of-order dates are rejected. Paywalled
material is not accessed. General event questions are labeled `free_llm`; they are
not presented as time-series ensemble forecasts.

Other exact public CSV mappings can be reviewed in `metaculus_bot/series_registry.json`:

```json
{
  "question_id": 12345,
  "resolution_sha256": "SHA256 of the exact resolution criteria",
  "kind": "public_csv",
  "url": "https://example.org/public-series.csv",
  "time_column": "date",
  "value_column": "value",
  "unit": "original resolution units",
  "frequency": "1D",
  "calendar": "NONE",
  "target_date": "2027-01-01",
  "max_history_age_days": 3
}
```

Match the source definition, units, aggregation period and target date before
adding a mapping. An asset's market price is not a substitute for its revenue,
earnings, opinion polls, or any unrelated KPI. Arbitrary untrusted question URLs
cannot access internal hosts, credentials, metadata endpoints or authenticated
provider sessions.

No Firestore or Cloud Storage is used. State lives in Metaculus own forecast history
and private notes, with seven-day GitHub operational artifacts. Existing paused
Kalshi traders and storage-purge controls are independent and remain paused.

## Local tests

```bash
python -m pip install --require-hashes -r metaculus_bot/requirements.lock
python -m pytest -q metaculus_bot/tests
python -m metaculus_bot.runner --scope test --max-questions 1
# Explicitly submit to the unscored testing area:
python -m metaculus_bot.runner --scope test --submit --max-questions 1
```

Time-series inference uses a separate virtual environment with the existing
`requirements-ensemble-forecast.lock`; the SDK environment cannot replace its
locked numerical/model dependencies. Gated model weights are never published to a
public Actions cache. Operational Actions logs show discovered questions, coverage,
submitted/deferred/abstained counts, engine and submission URLs. Forecast outcomes
and accuracy are only knowable after the questions resolve.
