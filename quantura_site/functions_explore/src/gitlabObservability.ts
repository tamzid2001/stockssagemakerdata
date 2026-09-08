import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { context, trace, SpanKind, SpanStatusCode } from "@opentelemetry/api";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-http";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-http";
import { MeterProvider, PeriodicExportingMetricReader } from "@opentelemetry/sdk-metrics";
import { LoggerProvider, BatchLogRecordProcessor } from "@opentelemetry/sdk-logs";
import { SeverityNumber } from "@opentelemetry/api-logs";
import { waitUntil } from "@vercel/functions";
import { randomUUID } from "node:crypto";
import type { RequestHandler } from "express";
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
let meterProvider: MeterProvider | null = null;
let loggerProvider: LoggerProvider | null = null;
let flushPromise: Promise<void> | null = null;
let flushRequested = false;
let initializationAttempted = false;

function isEnabled(value: string | undefined): boolean {
  return String(value || "").trim().toLowerCase() === "true";
}

export function normalizeOtlpTraceEndpoint(value: string): string {
  return normalizeOtlpEndpoint(value, "traces");
}

export function normalizeOtlpEndpoint(value: string, signal: "traces" | "metrics" | "logs"): string {
  const parsed = new URL(value.trim());
  if (parsed.protocol !== "https:") {
    throw new Error("gitlab_observability_endpoint_must_use_https");
  }
  parsed.username = "";
  parsed.password = "";
  parsed.search = "";
  parsed.hash = "";
  const basePath = parsed.pathname.replace(/\/+$/, "").replace(/\/v1\/(traces|metrics|logs)$/, "");
  parsed.pathname = `${basePath}/v1/${signal}`;
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
    meterProvider = new MeterProvider({
      resource,
      readers: [new PeriodicExportingMetricReader({
        exporter: new OTLPMetricExporter({
          url: normalizeOtlpEndpoint(status.traceEndpoint!, "metrics"),
          timeoutMillis: 5_000,
        }),
        exportIntervalMillis: 30_000,
        exportTimeoutMillis: 5_000,
      })],
    });
    loggerProvider = new LoggerProvider({
      resource,
      processors: [new BatchLogRecordProcessor({ exporter: new OTLPLogExporter({
        url: normalizeOtlpEndpoint(status.traceEndpoint!, "logs"), timeoutMillis: 5_000,
      }), scheduledDelayMillis: 1_000, maxQueueSize: 512, maxExportBatchSize: 64 })],
    });
    console.info(JSON.stringify({
      level: "info",
      message: "GitLab OpenTelemetry traces, metrics and request logs initialized",
      service: status.serviceName,
      service_version: status.serviceVersion,
      deployment_environment: status.deploymentEnvironment,
      gitlab_project_id: status.gitlabProjectId,
    }));
  } catch {
    provider = null;
    console.error(JSON.stringify({
      level: "error",
      message: "GitLab OpenTelemetry tracing initialization failed",
      error: "telemetry_initialization_failed",
    }));
  }
  return status;
}

/** Coalesce concurrent flushes; Vercel must keep this work alive after response. */
export function flushGitLabObservability(): Promise<void> {
  flushRequested = true;
  if (!flushPromise) {
    flushPromise = (async () => {
      while (flushRequested) {
        flushRequested = false;
        const results = await Promise.allSettled([
          provider?.forceFlush(), meterProvider?.forceFlush(), loggerProvider?.forceFlush(),
        ]);
        if (results.some((result) => result.status === "rejected")) {
          console.warn(JSON.stringify({ level: "warn", code: "TELEMETRY_EXPORT_FAILED" }));
        }
      }
    })().finally(() => { flushPromise = null; });
  }
  return flushPromise;
}

/** Only server-declared route templates enter telemetry; never raw URLs/IDs/query strings. */
export function safeRequestDimensions(method: string, route: unknown, status: number) {
  return {
    "http.request.method": ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"].includes(method) ? method : "OTHER",
    "http.route": typeof route === "string" ? route.slice(0, 200) : "unmatched",
    "http.response.status_code": status,
  };
}

export const observeGitLabRequest: RequestHandler = (req, res, next) => {
  if (!provider || !meterProvider || !loggerProvider) return next();
  const start = performance.now();
  const requestId = randomUUID();
  const meter = meterProvider.getMeter("quantura.http", "1");
  const count = meter.createCounter("http.server.request.count", { description: "Completed API requests" });
  const duration = meter.createHistogram("http.server.request.duration", { unit: "s" });
  const span = provider.getTracer("quantura.http", "1").startSpan("HTTP request", { kind: SpanKind.SERVER });
  const requestContext = trace.setSpan(context.active(), span);
  let completed = false;
  const finish = () => {
    if (completed) return;
    completed = true;
    const dimensions = safeRequestDimensions(req.method, req.route?.path, res.writableFinished ? res.statusCode : 499);
    const elapsed = (performance.now() - start) / 1000;
    span.updateName(`${dimensions["http.request.method"]} ${dimensions["http.route"]}`);
    span.setAttributes({ ...dimensions, "request.id": requestId });
    if (dimensions["http.response.status_code"] >= 500) span.setStatus({ code: SpanStatusCode.ERROR });
    count.add(1, dimensions);
    duration.record(elapsed, dimensions);
    loggerProvider!.getLogger("quantura.http", "1").emit({
      context: requestContext,
      severityNumber: dimensions["http.response.status_code"] >= 500 ? SeverityNumber.ERROR : SeverityNumber.INFO,
      body: "HTTP request completed",
      attributes: { ...dimensions, "request.id": requestId, "duration_ms": Math.round(elapsed * 1000) },
    });
    span.end();
    waitUntil(flushGitLabObservability());
  };
  res.once("finish", finish);
  res.once("close", finish);
  context.with(requestContext, next);
};

startGitLabObservability();
