# Quantura production watchdog

Quantura exposes a read-only watchdog endpoint for external availability monitoring:

```text
GET https://quantura.studio/api/health/watchdog
```

The route performs a bounded server-side Firestore read and returns HTTP `200` only when both the Vercel API function and Firestore are reachable. It never returns credentials, customer data, provider payloads, or environment-variable values. Responses are not cached or indexed.

Expected healthy response shape:

```json
{
  "ok": true,
  "service": "quantura-api",
  "dependencies": {
    "firestore": "available"
  },
  "checkedAt": "2026-08-31T00:00:00.000Z",
  "deployment": "commit-or-unknown"
}
```

## cron-job.org registration

1. Create an HTTPS monitor/job for `https://quantura.studio/api/health/watchdog`.
2. Use `GET`; do not add API keys or secrets.
3. Run every 5–15 minutes, according to the desired alert sensitivity and the account's allowed frequency.
4. Set a request timeout of at least 10 seconds.
5. Treat any non-`200` response, network failure, or response body without `"ok":true` as a failure.
6. Configure failure notifications in cron-job.org and require more than one consecutive failure if avoiding transient-alert noise is preferred.

The repository also runs `.github/workflows/quantura-live-smoke.yml` daily. That workflow verifies major HTML routes, the lightweight API health endpoint, and the dependency-aware watchdog. External monitoring and GitHub Actions are intentionally independent.

This endpoint is an availability signal, not a formal uptime SLA. Provider-specific integrations can be checked separately through authenticated operational tooling without exposing credentials.
