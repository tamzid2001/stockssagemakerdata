## Stock Screener Roadmap

### Must-do (short term)
- [ ] Verify Slack delivery using both webhook and bot-token upload paths.
- [ ] Confirm Linear workflow secrets (`LINEAR_API_KEY`, `LINEAR_TEAM_ID`) and validate issue creation.
- [ ] Expand headlines collection with additional sources if Yahoo Finance coverage is thin.
- [ ] Add unit tests for ticker loading, Slack payloads, and JSON schema validation.
- [ ] Add CSV schema validation for downstream consumers (e.g., BI dashboards).

### Should-do (mid term)
- [ ] Add caching for fundamentals/headlines to reduce API calls.
- [ ] Build a lightweight dashboard to visualize screening results.
- [ ] Add market-cap weighting to rankings and scoring.
- [ ] Add optional S3 upload path for combined screener output.
- [ ] Add configurable minimum ticker count check (enforced in CI).

### Nice-to-have
- [ ] Add sector-level summaries in the Slack message.
- [ ] Support sector-specific models or prompt templates.
- [ ] Add retry logic and circuit breakers for OpenAI/YFinance requests.
