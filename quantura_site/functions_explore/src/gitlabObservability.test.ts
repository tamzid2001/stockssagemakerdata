import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizeOtlpTraceEndpoint,
  resolveGitLabObservabilityStatus,
} from "./gitlabObservability";

test("GitLab observability safely disables when its endpoint is absent", () => {
  const status = resolveGitLabObservabilityStatus({ GITLAB_OBSERVABILITY_ENABLED: "true" });
  assert.equal(status.enabled, false);
  assert.equal(status.reason, "endpoint_missing");
  assert.equal(status.traceEndpoint, null);
});

test("OTLP HTTP base endpoints normalize to the traces endpoint", () => {
  assert.equal(
    normalizeOtlpTraceEndpoint("https://140928869.otel.gitlab-o11y.com:14318"),
    "https://140928869.otel.gitlab-o11y.com:14318/v1/traces"
  );
  assert.equal(
    normalizeOtlpTraceEndpoint("https://140928869.otel.gitlab-o11y.com:14318/v1/traces"),
    "https://140928869.otel.gitlab-o11y.com:14318/v1/traces"
  );
  assert.throws(() => normalizeOtlpTraceEndpoint("http://example.com:4318"), /must_use_https/);
});

test("GitLab observability status carries recommended correlation attributes", () => {
  const status = resolveGitLabObservabilityStatus({
    GITLAB_OBSERVABILITY_ENABLED: "true",
    GITLAB_OTEL_HTTP_ENDPOINT: "https://140928869.otel.gitlab-o11y.com:14318",
    GITLAB_PROJECT_ID: "85909892",
    GITLAB_PROJECT_NAME: "stockssagemakerdata",
    OTEL_SERVICE_NAME: "quantura-api",
    GITLAB_SERVICE_VERSION: "abc123",
    VERCEL_ENV: "production",
  });

  assert.equal(status.enabled, true);
  assert.equal(status.reason, "ready");
  assert.equal(status.gitlabProjectId, "85909892");
  assert.equal(status.serviceVersion, "abc123");
  assert.equal(status.deploymentEnvironment, "production");
  assert.equal("headers" in status, false);
});
