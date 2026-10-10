# Quantura Metaculus bot

The Quantura bot discovers open questions in active public tournaments and indexes
where the account has forecast permission, plus bot-oriented question series such
as MiniBench and AI 2027. It scans every
page, unpacks group questions, handles conditional outcomes separately, and checks
each question's own opening/closing times. Newly eligible competitions are picked
up automatically. Coverage includes Labor Automation, Global Health, US Midterms,
POTUS, Democracy Threat, Horizon Scanning, Respiratory Outlook, AI Pathways, Sagan,
ACX, Cultured Meat, Chinese AI Chips and Climate Tipping Points when active.

Forecast permission and prize eligibility are separate. Metaculus's
[`bot_leaderboard_status` definition](https://github.com/Metaculus/metaculus/blob/main/projects/models.py)
describes `exclude_and_show` as allowing a leaderboard display while excluding
bots from ranks, prizes and medals. It does not revoke forecast permission.
`exclude_and_hide` also affects visibility rather than forecast permission.
The worker retains its registered bot identity and reports both the leaderboard
status and prize eligibility. `include` or `bots_only` with a prize pool is labeled
`subject_to_tournament_rules`, rather than promising eligibility for this account;
other competition-specific entry restrictions still apply. Under the general
[tournament rules](https://www.metaculus.com/tournament-rules/), bots are ineligible
for prizes unless a competition explicitly permits them. Unknown policy states,
closed/future competitions, and accounts without forecast permission are excluded.

## Operation

- Workflow: [Quantura Metaculus Bot](https://github.com/tamzid2001/stockssagemakerdata/actions/workflows/metaculus-bot.yml).
- Schedule: minutes 7, 27 and 47 of every hour, including weekends. GitHub can delay
  or drop scheduled runs; this is periodic automation, not a continuously running
  process or a guarantee to meet every short deadline.
- Secrets: `METACULUS_TOKEN` and `OPENROUTER_API_KEY` in GitHub Actions. Never put
  either in source files, browser code, inputs, or artifacts.
- Enable: repository variable `METACULUS_BOT_ENABLED=true`.
- Submission approval: `METACULUS_SUBMISSIONS_APPROVED=true` only for an account
  permitted by Metaculus to submit. It defaults to false, including manual runs.
- Account binding: `METACULUS_BOT_ID` must match the authenticated bot. Recovery
  artifacts are namespaced as `metaculus-checkpoint-bot-<id>-...`; another bot's
  generations and comments are never imported.
- Participation form: `METACULUS_PARTICIPATION_FORM_CONFIRMED=true` after submission.
- Pause: set `METACULUS_BOT_ENABLED=false` or disable the workflow. Existing forecasts
  remain on Metaculus; pausing does not erase them.

Each question receives one forecast. Subsequent runs skip authoritative own
forecast history through `questions/bulk-forecast-read/`; feed metadata alone is
not treated as a submission receipt. An encrypted recovery artifact preserves the
exact generated answer before submission. The private reasoning note contains a
brief explanation, model attribution, real source links and a compact answer hash;
it contains no raw answer JSON, data hashes or duplicated recovery payload.
Uncertain writes are checked against Metaculus; writes aren't blindly retried.
If a note exists without its exact recovery answer, the worker defers rather than
generating a different prediction or posting another comment.
Metaculus requests are spaced at least five seconds apart. Read retries honor
`Retry-After` in seconds or HTTP-date format. Exhausted quotas defer work instead
of crashing the worker, and a restored cooldown prevents requests before it expires.
Own comments are read once per job, saved abstentions do not consume new-generation
capacity, and the time-series job fetches only the posts selected during discovery.
Pending exact answers are recovered first, then questions closing within two hours
take priority. The remaining backlog is balanced across tournaments by processed
question counts, with the earliest deadline first within each tournament. Questions
shared by tournaments receive one forecast and count toward each tournament's
coverage. Selection never depends on forecast probabilities or leaderboard scores.
Each Actions summary shows open questions, previous submissions, new confirmed
submissions and new reasoning notes by tournament, including tournaments with no
open questions. A zero is visible rather than presented as a submission.

Every submitted forecast has an automated private reasoning note. Metaculus
requires bots to leave private comments and publishes FutureEval notes itself at
regular intervals; main-site notes remain private unless it grants permission.
New LLM explanations target 60–100 words. Posted reasoning is capped at 100 words
and 1,000 characters, including old pending generations. Source links are limited
to three retrieved URLs. Distribution validation happens before any comment;
abstentions and invalid forecasts produce no comments. The worker does not publish
promotional or duplicate comments. Comments explain
the forecast; leaderboard scores depend on resolved forecasting performance.
Actions artifacts contain operational summaries, not private reasoning.

As a conservative restart policy, at most **two new private-note attempts per job**
and **twelve attempts per UTC day** are permitted. The encrypted checkpoint reserves
capacity before each network request, including ambiguous writes. These are
Quantura's own limits, not a claim about Metaculus's permitted posting volume.
Unconfirmed note writes are not repeated: if their receipt is still missing after
a restart, the original answer is held for review. A 401/403 during any question
stops the entire batch immediately.

### Moderation restrictions

As of October 10, 2026, submissions are paused after the owner reported that the
accounts were labeled spam. A new token returned HTTP 403; the latest replacement
key authenticates bot `quantura3` (310227), but technical API access does not establish
permission to bypass a moderation restriction. Metaculus must restore access or
explicitly approve a replacement before submission approval and automation are
enabled. Register the approved bot through the participation process; the previous
account's form confirmation is not reused. Do not rotate accounts to evade a block.
The [community guidelines](https://www.metaculus.com/help/guidelines/) prohibit spam
and describe temporary suspensions and permanent bans. A 401/403 is reported as
requiring operator review, with no immediate request retry.

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

No Firestore or Cloud Storage is used. Confirmed submission history lives in
Metaculus. An encrypted seven-day GitHub recovery artifact preserves pending
generations, abstentions and API cooldowns between runs. Exact answers remain in
recovery state until their forecasts are confirmed; private notes only identify
their answer hashes. Its
encryption key is derived from the secret Metaculus token with a dedicated context;
token rotation requires draining pending generations before changing the token.
Restoring or decrypting recovery state must succeed before new work starts.
Operational summary artifacts contain no private forecast values. Existing paused
Kalshi traders and storage-purge controls are independent and remain paused.

## Local tests

```bash
python -m pip install --require-hashes -r metaculus_bot/requirements.lock
python -m pytest -q metaculus_bot/tests
python -m metaculus_bot.runner --scope test --max-questions 1
# Explicitly submit to the unscored testing area:
python -m metaculus_bot.runner --scope test --submit --max-questions 1 --checkpoint artifacts/metaculus-checkpoint.enc
```

Time-series inference uses a separate virtual environment with the existing
`requirements-ensemble-forecast.lock`; the SDK environment cannot replace its
locked numerical/model dependencies. Gated model weights are never published to a
public Actions cache. Operational Actions logs show discovered questions, coverage,
submitted/deferred/abstained counts, engine and submission URLs. Forecast outcomes
and accuracy are only knowable after the questions resolve.
