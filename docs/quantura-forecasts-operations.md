# Quantura Forecasts operations and continuity

## Required environment variables

- `QUANTURA_FORECAST_API_KEY_PEPPER`: random server-only secret used to HMAC enterprise API keys; at least 16 characters.
- `CRON_SECRET`: runtime scheduler authentication secret. The GitHub workflow provisions it from the existing `QUANTURA_CRON_SECRET` Actions secret.
- Existing Firebase Admin configuration and storage-bucket configuration used by the Quantura API.

Optional provider credentials belong in the existing server secret store and must never use a `NEXT_PUBLIC_`/browser-visible name. No new generation or resolver provider is enabled merely because a key exists; it must be registered and tested explicitly.

## Backups and recovery

Production operators should configure managed scheduled Firestore exports and object-storage versioning/retention for the forecast dataset prefix. Recovery procedure:

1. suspend forecast publication/resolution jobs;
2. export the current damaged database for forensic retention;
3. restore the most recent verified Firestore export into an isolated project;
4. verify initial snapshot hashes, probability-history counts, slug mappings and dataset manifests;
5. restore affected collections or promote the recovered project using the platform runbook;
6. re-run idempotent lifecycle jobs;
7. compare dataset-release checksums before restoring API traffic.

No numerical RPO or RTO is promised by the codebase. Those values depend on the deployed backup schedule, cloud plan and on-call process and must be documented only after infrastructure verification.

## Job replay and idempotency

`forecast-lifecycle` is called by the existing protected scheduler. Each stage uses hourly idempotency keys and Firestore transactions. Expiry checks re-read the current status before writing. Resolution adapters must use stable source IDs and are expected to return the same result on replay. Dataset versions use create-only manifests and cannot be overwritten.

Candidate discovery, automatic forecast generation, and automatic resolver adapters are disabled until approved real providers and source licenses are configured. This is an intentional integrity safeguard.

## API-key recovery

Raw keys cannot be recovered because they are not stored. Create a replacement key, distribute it through the approved secret channel, verify it, then revoke the old key. Revocation is checked on every request and takes effect immediately at the API layer.

## Monitoring

Monitor structured logs for:

- API authentication failures and 429 rates;
- latency/error rate by versioned endpoint;
- generation/resolver provider failures;
- lifecycle job completion and retry counts;
- forecasts remaining pending past deadline;
- scoring and calibration counts;
- dataset release count/checksum failures;
- audit-log write failures.

Public feeds may be cached briefly, but forecast detail responses use short TTLs while pending. API-key and probability-history responses are `private, no-store`. Publication, revision and resolution should invalidate any external CDN cache if one is added later.
