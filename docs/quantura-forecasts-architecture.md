# Quantura Forecasts architecture

## Product boundary

Quantura Forecasts publishes prospective, objectively resolvable propositions. A possible future headline is presentation text attached to a formal question; it is never treated as evidence that an event occurred. Every unresolved public surface must render `FORECAST` and `THIS EVENT HAS NOT OCCURRED`.

The implementation reuses the existing Firebase architecture:

- Firestore is the system of record.
- `functions_explore` owns validation, authentication, scoped APIs, jobs, resolution and scoring.
- `functions_ssr` renders indexable forecast hubs and detail metadata.
- GitHub Actions invokes idempotent, secret-protected cron endpoints.
- Firebase Hosting routes public pages and versioned APIs to their existing functions.

No separate frontend framework or second database is introduced.

## Immutable record model

Collections:

| Collection | Purpose | Mutability |
| --- | --- | --- |
| `quantura_forecasts` | Current materialized view and immutable initial published snapshot | Limited mutable status/current fields |
| `quantura_forecasts/{id}/probability_history` | Append-only probability revisions | Create-only |
| `quantura_forecasts/{id}/amendments` | Transparent editorial corrections | Create-only |
| `quantura_forecast_api_keys` | HMAC-hashed enterprise credentials and scopes | Revocation/expiry metadata only |
| `quantura_forecast_api_usage` | Per-request audit records without raw credentials | Append-only |
| `quantura_forecast_rate_windows` | Transactional per-key rate windows | Ephemeral counters |
| `quantura_forecast_dataset_releases` | Immutable export manifests | Create-only |
| `quantura_forecast_job_runs` | Idempotency, progress and error summaries | One document per job/idempotency key |

The root forecast stores `initial_probability`, `initial_snapshot_hash`, `published_at`, and the original resolution rule. A revision updates only the materialized current probability and appends a history document. It never changes the initial snapshot. Resolution writes the actual outcome and proper scores in a transaction while retaining every pre-outcome revision.

## Temporal integrity

All evidence has an `observed_at` or `published_at` timestamp. Publication and revision validation rejects evidence newer than `input_cutoff_at`. Resolvers may add post-cutoff resolution evidence, but that evidence is stored separately and cannot modify forecast-time evidence.

Candidate discovery and generation jobs create drafts. Human/admin approval is required before publication. Forecast generation is provider-based and may combine structured model probabilities; an LLM may summarize supplied evidence but cannot be the only numerical provider or invent citations.

## API security

Enterprise routes use an opaque key only in `Authorization: Bearer` or `X-API-Key`. The raw key is returned once at creation. Firestore stores only an HMAC-SHA-256 digest, key prefix, customer identifier, scopes, tier, expiry and revocation metadata. `QUANTURA_FORECAST_API_KEY_PEPPER` is server-side only.

Supported scopes:

- `forecasts:read`
- `forecasts:history`
- `forecasts:resolved`
- `forecasts:bulk`
- `forecasts:admin`

Rate limits are enforced per key/tier and every request produces a redacted audit record with request ID, endpoint, status, latency and returned record count. Public website routes return an explicit public projection. Enterprise serializers exclude `private_strategy_json`, alpha features, raw provider secrets and private model internals.

## Resolution and scoring

Resolvers are registered by category. A resolver returns `yes`, `no`, `partial`, `void`, `disputed`, or `unresolved` plus structured source metadata. The generic engine never lets an LLM independently resolve an objective event. Conflicting or insufficient evidence produces `disputed` or remains unresolved.

Binary scores use:

`Brier = (p - outcome)^2`

`Log score = -(outcome * ln(p) + (1 - outcome) * ln(1 - p))`

using the final probability revision created before resolution. Partial/void/disputed records do not receive a binary score.

## Jobs

The existing protected cron route dispatches retry-safe stages:

1. expire overdue pending forecasts;
2. run registered resolvers for due forecasts;
3. score newly resolved forecasts;
4. materialize calibration summaries;
5. refresh public feed metadata;
6. create an explicitly requested immutable dataset release.

Every stage uses an idempotency key and transactions. Automatic candidate discovery and probability generation remain disabled until approved provider-specific inputs and licenses are provisioned; the architecture does not fabricate events.

## Dataset releases and recovery

Dataset manifests contain schema version, source cutoff, generated timestamp, record count, category/time coverage, object paths and SHA-256 checksums. A released version cannot be overwritten. JSONL and CSV are supported initially; Parquet requires an approved server-side writer before being advertised.

Backups and recovery use managed Firestore export and object-storage versioning procedures documented in `docs/quantura-forecasts-operations.md`. No RPO/RTO guarantee is claimed until the deployed infrastructure and support tier are verified.
