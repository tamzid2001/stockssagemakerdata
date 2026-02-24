# Phase Z Implementation Notes

## OpenAI chat model + caching

- Model picker endpoint: `GET /api/openai/models`
- Streaming chat endpoint: `POST /api/chat`
- Server-side model validation is enforced with an allowlist before request execution.
- Prompt caching is enabled via `prompt_cache_retention` with env override:
  - `OPENAI_PROMPT_CACHE_RETENTION` = `in_memory` (default) or `24h`
- Usage telemetry captured on server and surfaced in UI:
  - `prompt_tokens`
  - `completion_tokens`
  - `cached_tokens`

## Massive integration

Shared client:

- `functions/massive_client.py`
- Adds retry with exponential backoff + jitter for `429/5xx`
- Adds cursor-pagination helper
- Adds TTL cache support
- Enforces deny-list for `balance-sheets` paths before any network call

Capability audit endpoint:

- `GET /api/massive/capabilities`
- Probes:
  - `/economy/treasury-yields`
  - `/economy/inflation`
  - `/economy/inflation-expectations`
  - `/economy/labor-market`
  - `/stocks/corporate-actions/ipos`
  - `/options/contracts/all-contracts`
- Status mapping:
  - `200` -> `AVAILABLE`
  - `401` -> `UNAUTHORIZED`
  - `402/403` -> `FORBIDDEN_OR_NOT_IN_PLAN`
  - other -> `ERROR`

Economy + IPO routes:

- `GET /api/massive/economy/treasury-yields`
- `GET /api/massive/economy/inflation`
- `GET /api/massive/economy/inflation-expectations`
- `GET /api/massive/economy/labor-market`
- `GET /api/massive/stocks/ipos`

Options contracts route + fallback:

- `GET /api/massive/options/contracts`
- `get_options_chain` fallback path:
  - try yfinance first
  - if yfinance expirations/chain unavailable, fallback to Massive contracts
  - return reference-only chain when quote-level data is unavailable

## References

- OpenAI Prompt Caching guide: https://platform.openai.com/docs/guides/prompt-caching
- OpenAI Models API (List models): https://platform.openai.com/docs/api-reference/models/list
- Massive economy docs: https://massive.com/docs/rest/economy/overview
- Massive IPO docs: https://massive.com/docs/rest/stocks/corporate-actions/ipos
- Massive options contracts docs: https://massive.com/docs/rest/options/contracts/all-contracts

## Policy reminder

- Massive `balance-sheets` endpoint is explicitly blocked and must not be used.
