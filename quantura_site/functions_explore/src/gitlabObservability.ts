import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { registerInstrumentations } from "@opentelemetry/instrumentation";
import { ExpressInstrumentation } from "@opentelemetry/instrumentation-express";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BatchSpanProcessor, NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import {
  ATTR_DEPLOYMENT_ENVIRONMENT_NAME,
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";

export type GitLabObservabilityStatus = {
  enabled: boolean;
  reason: "ready" | "disabled_by_config" | "endpoint_missing" | "invalid_endpoint";
  traceEndpoint: string | null;
  serviceName: string;
  serviceVersion: string;
  deploymentEnvironment: string;
  gitlabProjectId: string;
  gitlabProjectName: string;
};

let provider: NodeTracerProvider | null = null;
let initializationAttempted = false;

function isEnabled(value: string | undefined): boolean {
  return String(value || "").trim().toLowerCase() === "true";
}

export function normalizeOtlpTraceEndpoint(value: string): string {
  const parsed = new URL(value.trim());
  if (parsed.protocol !== "https:") {
    throw new Error("gitlab_observability_endpoint_must_use_https");
  }
  parsed.username = "";
  parsed.password = "";
  parsed.search = "";
  parsed.hash = "";
  const basePath = parsed.pathname.replace(/\/+$/, "");
  parsed.pathname = basePath.endsWith("/v1/traces") ? basePath : `${basePath}/v1/traces`;
  return parsed.toString();
}

export function resolveGitLabObservabilityStatus(
  env: NodeJS.ProcessEnv = process.env
): GitLabObservabilityStatus {
  const base = {
    serviceName: String(env.OTEL_SERVICE_NAME || "quantura-api").trim(),
    serviceVersion: String(env.GITLAB_SERVICE_VERSION || env.VERCEL_GIT_COMMIT_SHA || env.CI_COMMIT_SHA || env.npm_package_version || "unknown").trim(),
    deploymentEnvironment: String(env.VERCEL_ENV || env.CI_ENVIRONMENT_NAME || env.NODE_ENV || "development").trim(),
    gitlabProjectId: String(env.GITLAB_PROJECT_ID || env.CI_PROJECT_ID || "").trim(),
    gitlabProjectName: String(env.GITLAB_PROJECT_NAME || env.CI_PROJECT_NAME || "stockssagemakerdata").trim(),
  };

  if (!isEnabled(env.GITLAB_OBSERVABILITY_ENABLED)) {
    return { enabled: false, reason: "disabled_by_config", traceEndpoint: null, ...base };
  }
  if (!String(env.GITLAB_OTEL_HTTP_ENDPOINT || "").trim()) {
    return { enabled: false, reason: "endpoint_missing", traceEndpoint: null, ...base };
  }

  try {
    return {
      enabled: true,
      reason: "ready",
      traceEndpoint: normalizeOtlpTraceEndpoint(String(env.GITLAB_OTEL_HTTP_ENDPOINT)),
      ...base,
    };
  } catch {
    return { enabled: false, reason: "invalid_endpoint", traceEndpoint: null, ...base };
  }
}

export function startGitLabObservability(env: NodeJS.ProcessEnv = process.env): GitLabObservabilityStatus {
  const status = resolveGitLabObservabilityStatus(env);
  if (!status.enabled || initializationAttempted) return status;
  initializationAttempted = true;

  try {
    const resource = resourceFromAttributes({
      [ATTR_SERVICE_NAME]: status.serviceName,
      [ATTR_SERVICE_VERSION]: status.serviceVersion,
      [ATTR_DEPLOYMENT_ENVIRONMENT_NAME]: status.deploymentEnvironment,
      "gitlab.project.id": status.gitlabProjectId,
      "gitlab.project.name": status.gitlabProjectName,
    });
    const exporter = new OTLPTraceExporter({
      url: status.traceEndpoint || undefined,
      timeoutMillis: 10_000,
      concurrencyLimit: 2,
    });
    provider = new NodeTracerProvider({
      resource,
      spanProcessors: [new BatchSpanProcessor(exporter, {
        scheduledDelayMillis: 1_000,
        exportTimeoutMillis: 10_000,
        maxQueueSize: 512,
        maxExportBatchSize: 64,
      })],
    });
    provider.register();
    registerInstrumentations({
      tracerProvider: provider,
      instrumentations: [new HttpInstrumentation(), new ExpressInstrumentation()],
    });
    console.info(JSON.stringify({
      level: "info",
      message: "GitLab OpenTelemetry tracing initialized",
      service: status.serviceName,
      service_version: status.serviceVersion,
      deployment_environment: status.deploymentEnvironment,
      gitlab_project_id: status.gitlabProjectId,
    }));
  } catch (error) {
    provider = null;
    console.error(JSON.stringify({
      level: "error",
      message: "GitLab OpenTelemetry tracing initialization failed",
      error: error instanceof Error ? error.message : "unknown_error",
    }));
  }
  return status;
}

startGitLabObservability();
