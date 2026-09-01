---
title: "Ensemble forecasting architecture"
description: "Production architecture, model capabilities, quantile weighting, and worker operations."
---

# Quantura probabilistic ensemble forecasting

## Production architecture

The ensemble extends Quantura's existing Forecasting workspace and authenticated Express API. Heavy Python inference never runs in a browser or inside a synchronous Vercel request.

```text
Forecasting UI or API client
  -> POST /api/v1/ensemble-forecasts
  -> authenticate, authorize workspace, validate plan and capabilities
  -> snapshot normalized input and immutable request in Firestore
  -> dispatch only forecast_job_id to the private worker
  -> run enabled model adapters sequentially
  -> combine requested quantiles in transformed space
  -> persist final ensemble and auditable model-run metadata
  -> poll or download the workspace-authorized result
```

GitHub Actions is the configured dispatch backend until a persistent Quantura inference worker is provisioned. It is not a public API. The workflow receives a server-generated job identifier, then claims the authorized input through a signed internal endpoint. A dedicated GPU-capable self-hosted runner can be selected with `ENSEMBLE_FORECAST_RUNNER_LABELS`; the current fallback is a GitHub-hosted CPU runner and is suitable for mock validation, not a low-latency five-foundation-model SLA.

## Supported models

| Stable ID | Model/checkpoint | Quantile behavior | Default device |
| --- | --- | --- | --- |
| `prophet` | Meta Prophet | Requested quantiles from one posterior predictive sample distribution | CPU |
| `toto` | `Datadog/Toto-2.0-4m` | Native P10–P90 in 0.10 steps; interpolation only inside that interval | Auto |
| `granite` | `ibm-granite/granite-timeseries-patchtst-fm-r2` | Requested quantiles | CPU |
| `chronos` | `amazon/chronos-2` | Requested quantiles after verifying the loaded checkpoint's trained range | Auto |
| `timesfm` | `google/timesfm-3.0-pytorch` | Native P10–P90 in 0.10 steps; interpolation only inside that interval | CUDA with CPU OOM fallback |

Clients select only these stable IDs. Arbitrary checkpoints are rejected. The authoritative registry is `quantura_site/functions_explore/src/ensembleModelRegistry.json` and is loaded by both the TypeScript API and Python capability layer.

## Numerical method

The worker validates and chronologically sorts the immutable historical series, resolves duplicate timestamps by keeping the final observation, and converts the target to `float32` for foundation-model adapters.

Transforms:

- `auto`: log transform when every target is strictly positive; otherwise no transform.
- `log`: reject zero or negative targets.
- `none`: preserve the original scale.

Each enabled adapter forecasts the same transformed history and exact future timestamps. The worker retains only small quantile matrices between model executions and explicitly releases model, tensor, CPU, and CUDA memory after every adapter.

For every requested quantile `q`, the ensemble:

1. selects enabled positive-weight models that genuinely produce `q`;
2. removes unavailable model/quantile pairs;
3. normalizes the remaining requested weights to sum to one;
4. combines the quantile values in transformed space;
5. performs deterministic monotonic rearrangement across quantiles;
6. inverse-transforms the final matrix to the source scale.

This means equal 20% requested weights become 20% each at P50 when all five models participate, while P01/P99 use one-third each across Prophet, Granite, and Chronos. Toto and TimesFM tails are not extrapolated or manufactured. A request is rejected with HTTP 422 when no enabled positive-weight model can produce a requested quantile.

For US equities, `trading_sessions` uses the NYSE exchange calendar rather than weekdays, so exchange holidays are excluded. Uploaded datasets can use inferred or explicit pandas-compatible frequencies and retain timestamp precision/timezone.

## Persistence and reproducibility

Server-only Firestore collections store jobs, chunked immutable inputs, results, presets, cache mappings, idempotency mappings, and usage counters. Each job records its source version/hash, requested and effective weights, quantiles, horizon, transform, model checkpoints, runtime mode, progress, warnings, structured failures, timestamps, and result hash. The standard public response includes final ensemble rows and concise runtime metadata but excludes raw inputs and component prediction arrays.

`Idempotency-Key` protects client retries. The deterministic request hash also permits reuse of an identical completed result only when the normalized source snapshot, request, registry, and model versions match.

The default failure policy is `fail`. Explicit `renormalize` may continue only when every requested quantile is still supported; failed models are identified and never represented as participants.

## Authentication and quotas

Creation requires the existing `forecasts:write` scope and workspace write permission. Reading and downloading require `forecasts:read`. Workspace Viewers may read authorized results but cannot launch compute. Dataset IDs are resolved and authorized on the server; a client-supplied `workspace_id` never grants access by itself.

Plan-aware controls enforce model access, concurrent jobs, daily jobs, history rows, prediction length, context length, and requested quantile count. The API returns structured 401/403/422/429/503 errors and does not expose stack traces.

## TimesFM licensing gate

The three TimesFM flags are deliberately independent:

- `TIMESFM_HF_ACCESS_APPROVED`: the gated checkpoint is accessible.
- `TIMESFM_COMMERCIAL_LICENSED`: Quantura has separately established commercial-production rights.
- `ALLOW_NONCOMMERCIAL_TIMESFM`: explicit development-only evaluation; ignored in production.

Production availability requires both Hugging Face access and `TIMESFM_COMMERCIAL_LICENSED=true`. Merely accepting gated repository terms does not enable TimesFM for public or paid Quantura users.

## Secret and runtime configuration

Server/deployment variables:

- `QUANTURA_ENSEMBLE_WORKER_TOKEN` — at least 32 random bytes; identical Vercel API secret and GitHub Actions secret.
- `QUANTURA_ENSEMBLE_WORKER_MODE=github_actions`
- `GITHUB_ACTIONS_TOKEN` — least-privilege workflow-dispatch token for the API project.
- `GITHUB_REPO_OWNER`, `GITHUB_REPO_NAME`, `GITHUB_WORKFLOW_REF`
- the three TimesFM flags above.

Private worker variables/secrets:

- `HF_TOKEN`
- `QUANTURA_ENSEMBLE_WORKER_TOKEN`
- `QUANTURA_WORKER_API_BASE`
- `ENSEMBLE_FORECAST_RUNNER_LABELS` (GitHub variable containing a JSON runner-label array)

Never prefix these values with `NEXT_PUBLIC_`, commit them, echo them, attach them to artifacts, or write them to generated forecast files.

The immutable inference container is defined by `Dockerfile.ensemble-forecast` and `requirements-ensemble-forecast.lock`. Production requests never install packages dynamically.

## Operational limitations

- A GitHub-hosted runner does not imply CUDA or persistent low-latency service. Real five-model public workloads require a suitable persistent/GPU worker or self-hosted runner.
- TimesFM remains disabled in production until commercial rights are explicitly configured.
- Full historical performance metrics require a separate point-in-time walk-forward ensemble backtest. Prophet in-sample diagnostics must not be labeled ensemble accuracy.
- Large model checkpoint downloads are cached only in the private runner cache and never committed to Git.
