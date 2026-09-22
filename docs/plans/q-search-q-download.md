# Q Search and Q Download

Status: implemented in the release branch; deployment is verified separately.

Recovery: tag `backup/pre-q-search-download-20260921` preserves pre-change code.
Use a reviewed forward revert; never reset main or restore older databases.
Existing CSV endpoints and stored datasets remain compatible.

Scope: one Auto selector, bounded Jev ranking, paginated provider event branches,
one snapshot-based download tab. Discovery is not exhaustive. The daily
screener uses latest close > first P99; target is final P99.
Audited baseline: main at `9ec9fd8`, 21 September 2026.

## Product decision

One compact **Q Search** bar selects a verified market across the terminal.
**Source is always Auto**; provider selectors disappear from the normal UI.
One **Q Download** tab contains market-history preview and download, including
supported options histories and multi-contract prediction-market selections.
Q Forecast, Q Screener and Q Download use the same selected-market identity.
Do not create a second terminal, search index or forecasting engine.

```text
Q Search: ticker, company, team, event, contract or supported market URL
  → verified candidates, grouped by asset class / venue
  → explicit selection of security or outcome/side
  → Q Forecast | Q Screener | Q Download
```

“Auto” is a routing policy, not an asset identifier. Apple on two exchanges is
not one security, and opposing teams, YES/NO, draws and similarly named contracts
remain distinct. Never substitute a Kalshi contract for a Polymarket contract.
The resolved venue and actual data supplier remain visible as metadata.

## Current implementation to reuse

- `quantura_site/public/market-search.js`: existing universal search, keyboard
  navigation, outside-click dismissal, debouncing and selected-market events.
- `functions_explore/src/marketSearch.ts`: provider capabilities and search;
  current endpoints `/api/market-search`, `/api/market-search/resolve` and
  `/api/market-search/capabilities` remain compatibility entry points.
- `marketLink.ts`: allowlisted URL parsing, not arbitrary URL fetching.
- `predictionMarketData.ts`, `eventHistory.ts`, `kalshiMinuteHistory.ts`,
  `kalshiPerps.ts`, `alpacaClient.ts`, `yahooMarketData.ts` and
  `marketDataRoutes.ts`: existing discovery and actual history implementations.
- `public/data-integrations.js`: prediction-market discovery, selected outcomes,
  preview/export, live/historical routing and options controls.
- `public/app.js`: existing forecast job submission, quote overlays and equity
  history/options handlers. Extract shared state gradually; do not rewrite it.
- `functions_explore/src/supportChat.ts`: existing Jev typed-choice client pattern.
  Extract a reusable server-only client; preserve Q Support's fixed answers.
- `pages/forecasting.html`, `pages/historical-data.html` and SSR templates:
  currently expose overlapping market searches and download panels.

## Q Search

### Interaction

- Persistent, compact search above the terminal tabs. No separate source menu,
  duplicate ticker field, prediction-market search or paste-link input.
- Accept tickers, company/team names, market identifiers and approved market URLs.
- Default to all supported market categories. Optional compact chips narrow to
  Live, Upcoming, Historical, Stocks, Contracts, Perpetuals or Options; filters
  never become a requirement to discover a market.
- Show symbol/full title, exchange, provider, event/date, outcome and YES/NO when
  relevant. Nicknames must retain full team/event context. Soccer can expose six
  distinct outcome/side contracts; do not silently choose one.
- Selecting an event expands its actual available contracts; selecting a contract
  updates the one shared selection. Show a selected-market chip with Clear/Change.
- Results overlay content, close on selection/Escape/outside click/tab change,
  ignore stale network responses and retain keyboard/touch accessibility at 320px.
- On Q Forecast, selection configures forecastable history without executing a job.
  On Q Download, it configures preview/export. Selecting alone must not trigger
  compute, an export, an order or an account mutation.

### Auto routing and provenance

- Equities: use existing configured Alpaca/Yahoo availability rules, exchange and
  asset support, entitlements, interval/range limits, split adjustment and freshness.
  Fallback only between providers representing the exact same verified security.
- Polymarket US, Kalshi and Kalshi perpetuals: resolve the selected provider's own
  stable IDs. Kalshi perpetuals retain the documented underlying spot-price scale.
- Options: choose underlying → expiry → specific call/put contract in the same
  selector, with only real provider-supported expirations and histories.
- Private uploaded CSVs can appear as an authenticated “My datasets” result group;
  use current workspace authorization. Never mix them into public cached search.
  “Upload CSV” remains an explicit import action, not a pretend market provider.
- Return actual provider, price field, interval, timezone, data coverage, gaps and
  adjustment basis. No fabricated bars, no cross-venue stitching and no hidden
  resampling from daily to minute data.

### Jev's precise role

Use TypeSafe's structured decision API, not generated prose. It exposes typed
Choice, Noul and Score questions; it is not a market database or forecasting model.

1. Exact symbols, IDs and allowlisted links take the deterministic fast path.
2. Provider/catalog search retrieves a bounded, verified candidate set.
3. For ambiguous natural-language queries, Jev may classify the intent and rank
   **only those candidate IDs**, with an explicit “none/uncertain” option.
4. Validate model, allowed choices and probability ranges before using rankings.
   Low confidence leaves multiple choices visible; it never silently selects a side.
5. Timeout/unavailable Jev falls back to deterministic search—not GPT and not an
   empty market list. Jev cannot create symbols, histories, prices or capabilities.

Reuse the server-only `TYPESAFE_API_KEY`; no browser credentials. Send only the
query and public candidate metadata, not uploads/account records. Cache normalized
queries/candidate revisions, cap candidate count and request volume, debounce
typing and avoid an AI call for each keystroke. Preserve the pinned Jev model until
a separately tested upgrade is justified. Do not imply ranking confidence is a
market probability or a trading win rate.

Official API: https://docs.typesafe.ai/api

### Service contract

Normalize the existing resource fields into a shared selection contract containing
stable ID, asset class, symbol, provider, provider-specific IDs, event, outcome/side,
exchange, status, unit, currency, timezone and supported actions/intervals.
Keep provider-specific metadata rather than forcing contracts into equity schemas.

The frontend always requests `source=auto`; legacy API clients may keep explicit
providers. Server resolution must verify any client-selected ID. Authentication,
dataset ownership and compute quotas stay authoritative on the backend.

Keep bounded pagination and partial-provider errors. Extend the existing catalog
and synchronization facilities where present; current search is bounded discovery,
not proof of an exhaustive global/live/historical index. Publish actual coverage.

## Q Download

Replace separate stock-history and prediction-market download experiences with
one tab, one selected-market summary and one shared preview/export lifecycle.
Options are a capability-specific subsection of the same tab, not a third tool.

Compact default controls:

- Selected market(s), with event-side multi-select where available.
- Interval: only supported minute/hour/day choices.
- Localized start/end calendar controls or Latest N observations.
- Preview, Download CSV and Download JSON when supported by the existing endpoint.

Advanced controls appear only when applicable:

- Stocks: regular/extended sessions and split-adjustment policy.
- Event contracts: Auto / pregame / in-game / both; Auto changes to in-game only
  after 32 elapsed game minutes, while still reporting available observations.
- Options: selected contract, expiration and actual provider history coverage.
- Dataset exports: existing workspace permissions and safe filename handling.

One preview shows actual row count, selected field, first/last timestamps, missing
intervals, provider and price units. Export uses the same validated query/snapshot
as preview, not whatever happens to remain in a stale hidden form. Changing a
selection or date/interval invalidates prior preview/download links.

Large existing export jobs stay asynchronous; retain limits and cancellation.
Multi-market exports identify each contract in every row or separate files with a
manifest. Never combine two teams or opposing YES/NO prices into an unlabeled series.

## Compatibility and delivery sequence

1. Shared selection and capability tests; extract the Jev client without changing
   Q Support. Record baseline search/download/forecast tests and responsive layout.
2. Implement automatic Q Search over the existing providers, strict IDs and URL
   resolution. Add optional Jev reranking behind bounded server-side behavior.
3. Connect the common selection to Q Forecast and verify one-model and multi-model
   requests. Models/weights remain in Advanced; no all-five-model requirement.
4. Consolidate Q Download, preserving every supported export and provider-specific
   option. Remove duplicated source/search controls only once replacement paths pass.
5. Map old download URLs/panel links to Q Download while preserving validated
   symbol/contract/date selections. Do not break saved forecast deep links.
6. Sync source/SSR templates, OpenAPI and Mintlify, run tests/builds, deploy matched
   backend/SSR/assets from reviewed current main, then verify production paths.

Do not roll back newer code, silently migrate saved strategy behavior or touch live
trading workflows. The measurement retry remains an unrelated local process.

## Acceptance tests

- One visible search, no normal source selector; Auto on every search request.
- Exact ticker, duplicate symbol on exchanges, team nickname/full name, live game,
  settled event, supported URL, perpetual and option contract resolve correctly.
- All six soccer outcome/side selections remain distinct throughout download and
  forecasting. Selection survives tab changes without stale-provider leakage.
- Jev malformed answer, unknown ID, low confidence and timeout never fabricate a
  market or disable deterministic discovery. Secrets remain server-side.
- Download preview and CSV/JSON agree on rows, IDs, interval, units and coverage.
- Rapid search/selection changes, closing dropdowns and switching tabs cannot
  resurrect obsolete requests or obscure the Run forecast button.
- Single-model and arbitrary supported subsets work; missing tail capability
  produces clear validation, not fabricated quantiles or automatic model enabling.
- Prior requested P01/P99 filters and cutoff-close-above-first-P99 Buy indicator
  remain separate from the later first-observed-quote P10/P90 signal. No new
  Sell-below-P01 rule is introduced.
- Guest public usage, authenticated save, workspace CSV authorization and limits
  remain enforced. Responsive checks at 320/390/768/1440px in light/dark themes.
- Lint, typecheck, backend/unit/UI tests and production builds pass; provider/live
  smoke tests are reported separately from mocked unit fixtures.
