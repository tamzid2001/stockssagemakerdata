const errorSchema = {
  type: "object",
  required: ["error"],
  properties: {
    error: {
      type: "object",
      required: ["code", "message", "request_id"],
      properties: {
        code: { type: "string" },
        message: { type: "string" },
        request_id: { type: "string", format: "uuid" },
      },
    },
  },
};

export function buildOpenApiDocument(origin = "https://quantura.studio"): Record<string, unknown> {
  const forecastFilters = [
    { name: "category", in: "query", required: false, description: "Forecast category.", schema: { type: "string", enum: ["markets", "earnings", "corporate", "products_technology", "politics_policy", "economics", "sports"] } },
    { name: "subcategory", in: "query", required: false, description: "Provider-defined subcategory.", schema: { type: "string", maxLength: 120 } },
    { name: "entity", in: "query", required: false, description: "Exact entity id or name.", schema: { type: "string", maxLength: 220 } },
    { name: "ticker", in: "query", required: false, description: "Exact normalized ticker.", schema: { type: "string", maxLength: 16 } },
    { name: "status", in: "query", required: false, description: "Forecast lifecycle status.", schema: { type: "string", enum: ["draft", "pending", "resolved_yes", "resolved_no", "resolved_partial", "void", "expired", "disputed"] } },
    { name: "min_probability", in: "query", required: false, description: "Inclusive minimum decimal probability.", schema: { type: "number", minimum: 0, maximum: 1 } },
    { name: "max_probability", in: "query", required: false, description: "Inclusive maximum decimal probability.", schema: { type: "number", minimum: 0, maximum: 1 } },
    { name: "created_after", in: "query", required: false, description: "Inclusive ISO-8601 creation timestamp.", schema: { type: "string", format: "date-time" } },
    { name: "created_before", in: "query", required: false, description: "Inclusive ISO-8601 creation timestamp.", schema: { type: "string", format: "date-time" } },
    { name: "resolves_after", in: "query", required: false, description: "Inclusive ISO-8601 resolution deadline.", schema: { type: "string", format: "date-time" } },
    { name: "resolves_before", in: "query", required: false, description: "Inclusive ISO-8601 resolution deadline.", schema: { type: "string", format: "date-time" } },
    { name: "model", in: "query", required: false, description: "Exact model name.", schema: { type: "string", maxLength: 160 } },
    { name: "limit", in: "query", required: false, description: "Page size.", schema: { type: "integer", minimum: 1, maximum: 500, default: 50 } },
    { name: "cursor", in: "query", required: false, description: "Opaque cursor returned by the previous page.", schema: { type: "string" } },
  ];
  const idParameter = { name: "forecast_id", in: "path", required: true, description: "Forecast id or unique slug.", schema: { type: "string" } };
  const commonErrors = {
    "401": { description: "Authentication failed", content: { "application/json": { schema: errorSchema } } },
    "403": { description: "Scope, plan, or workspace authorization denied", content: { "application/json": { schema: errorSchema } } },
    "429": { description: "Rate limit exceeded", content: { "application/json": { schema: errorSchema } } },
  };
  return {
    openapi: "3.1.0",
    info: {
      title: "Quantura Forecast Intelligence API",
      version: "1.0.0",
      description: "Versioned access to Quantura forecasts, collaborator-aware workspaces, screeners, datasets, and provenance metadata.",
      contact: { name: "Quantura", url: `${origin}/contact` },
      license: { name: "Quantura API Terms", url: `${origin}/terms` },
    },
    servers: [{ url: `${origin}/api/v1` }],
    security: [{ bearerAuth: [] }],
    tags: [
      { name: "Access", description: "Identity, token scopes, plans, and effective workspace access." },
      { name: "Workspaces", description: "Resources are authorized against current membership on every request." },
      { name: "Datasets", description: "Licensing-aware catalog and schema metadata." },
      { name: "Forecasts", description: "Prospective forecasts and immutable probability trajectories." },
      { name: "Ensemble Forecasts", description: "Asynchronous probabilistic time-series ensemble jobs and reproducible presets." },
    ],
    paths: {
      "/me/access": {
        get: {
          tags: ["Access"], summary: "Get effective API access", operationId: "getMyAccess",
          description: "Returns token scopes, personal plan, and current owner/editor/viewer membership. Membership is evaluated on every request.",
          "x-quantura-scope": "account:read", "x-plan-entitlement": "All authenticated users; free collaborators may inspect shared access.",
          responses: { "200": { description: "Effective identity, scopes, plan, and workspaces." }, ...commonErrors },
        },
      },
      "/capabilities": {
        get: { tags: ["Access"], summary: "Get effective product capabilities", operationId: "getCapabilities", "x-quantura-scope": "account:read", responses: { "200": { description: "Personal and workspace-scoped capability map." }, ...commonErrors } },
      },
      "/workspaces": {
        get: { tags: ["Workspaces"], summary: "List accessible workspaces", operationId: "listWorkspaces", "x-quantura-scope": "workspaces:read", responses: { "200": { description: "Owner and current collaborator workspaces." }, ...commonErrors } },
      },
      "/workspaces/{workspace_id}/{resource}": {
        get: {
          tags: ["Workspaces"], summary: "List authorized workspace resources", operationId: "listWorkspaceResources",
          parameters: [
            { name: "workspace_id", in: "path", required: true, schema: { type: "string" } },
            { name: "resource", in: "path", required: true, schema: { type: "string", enum: ["forecasts", "prediction-analyses", "screener-snapshots", "datasets", "backtests"] } },
            { name: "limit", in: "query", schema: { type: "integer", minimum: 1, maximum: 200, default: 50 } },
            { name: "cursor", in: "query", schema: { type: "string" } },
            { name: "sort", in: "query", schema: { type: "string", enum: ["created_at", "updated_at"] } },
            { name: "order", in: "query", schema: { type: "string", enum: ["asc", "desc"], default: "desc" } },
          ],
          responses: { "200": { description: "Workspace-scoped resources." }, "403": { description: "Membership, role, plan, or scope denied", content: { "application/json": { schema: errorSchema } } } },
          "x-quantura-scope": "Resource-specific read scope", "x-workspace-behavior": "Viewer and editor collaborators may read. Membership removal takes effect on the next request.",
        },
      },
      "/datasets": {
        get: {
          tags: ["Datasets"], summary: "List the licensing-aware dataset catalog", operationId: "listDatasets",
          parameters: [
            { name: "asset_class", in: "query", schema: { type: "string" } },
            { name: "source", in: "query", schema: { type: "string" } },
            { name: "derived", in: "query", schema: { type: "boolean" } },
            { name: "limit", in: "query", schema: { type: "integer", minimum: 1, maximum: 200, default: 50 } },
          ],
          responses: { "200": { description: "Dataset catalog entries with provenance and redistribution status." }, ...commonErrors },
          "x-quantura-scope": "datasets:read", "x-plan-entitlement": "Quant or Research for standalone API use; shared workspace reads remain resource-scoped.",
        },
      },
      "/datasets/{dataset_id}": {
        get: { tags: ["Datasets"], summary: "Get dataset metadata", operationId: "getDataset", "x-quantura-scope": "datasets:read", parameters: [{ name: "dataset_id", in: "path", required: true, schema: { type: "string" } }], responses: { "200": { description: "Dataset metadata." }, "404": { description: "Dataset not found." }, ...commonErrors } },
      },
      "/datasets/{dataset_id}/schema": {
        get: { tags: ["Datasets"], summary: "Get dataset column schema", operationId: "getDatasetSchema", "x-quantura-scope": "datasets:read", parameters: [{ name: "dataset_id", in: "path", required: true, schema: { type: "string" } }], responses: { "200": { description: "Dataset schema." }, ...commonErrors } },
      },
      "/forecasts": {
        get: {
          tags: ["Forecasts"], summary: "List published forecasts", operationId: "listForecasts",
          description: "Returns prospective forecasts. Probabilities use decimal 0–1 units; private Quantura alpha fields are excluded.",
          "x-quantura-scope": "forecasts:read", "x-plan-entitlement": "Quant or Research API entitlement.",
          parameters: forecastFilters,
          responses: { "200": { description: "Forecast results and cursor metadata." }, ...commonErrors },
        },
      },
      "/forecast/models": {
        get: {
          tags: ["Ensemble Forecasts"], summary: "Get ensemble model capabilities", operationId: "getEnsembleModelCapabilities",
          description: "Returns plan, runtime, quantile, horizon, device, checkpoint, and TimesFM licensing availability. No secret values are returned.",
          "x-quantura-scope": "Authenticated session or API key",
          parameters: [{ name: "workspace_id", in: "query", required: false, schema: { type: "string" }, description: "Resolve effective plan and model availability for this currently authorized workspace." }],
          responses: { "200": { description: "Effective model capabilities.", content: { "application/json": { schema: { $ref: "#/components/schemas/ModelCapabilitiesEnvelope" } } } }, ...commonErrors },
        },
      },
      "/ensemble-forecasts": {
        post: {
          tags: ["Ensemble Forecasts"], summary: "Create an asynchronous ensemble forecast", operationId: "createEnsembleForecast",
          description: "Validates the source and immutable configuration, snapshots authorized input data, enqueues a durable worker, and returns HTTP 202. Component-model arrays are not exposed in the standard result.",
          "x-quantura-scope": "forecasts:write", "x-workspace-behavior": "Owner and Editor may create. Viewer is read-only.",
          parameters: [{ name: "Idempotency-Key", in: "header", required: false, schema: { type: "string", maxLength: 180 }, description: "Replays the same normalized request without launching duplicate compute." }],
          requestBody: { required: true, content: { "application/json": { schema: { $ref: "#/components/schemas/EnsembleForecastRequest" } } } },
          responses: {
            "202": { description: "Forecast job queued.", content: { "application/json": { schema: { $ref: "#/components/schemas/EnsembleForecastEnvelope" } } } },
            "422": { description: "Unsupported model, quantile, horizon, dataset, or transform configuration.", content: { "application/json": { schema: errorSchema } } },
            ...commonErrors,
          },
        },
      },
      "/ensemble-forecasts/{forecast_id}": {
        get: {
          tags: ["Ensemble Forecasts"], summary: "Get forecast job status or final ensemble", operationId: "getEnsembleForecast",
          "x-quantura-scope": "forecasts:read", "x-workspace-behavior": "Current Owner, Editor, and Viewer membership is evaluated on every request.",
          parameters: [idParameter],
          responses: { "200": { description: "Queued/running progress or completed ensemble predictions.", content: { "application/json": { schema: { $ref: "#/components/schemas/EnsembleForecastEnvelope" } } } }, "404": { description: "Forecast job not found." }, ...commonErrors },
        },
      },
      "/ensemble-forecasts/{forecast_id}/reproduce": {
        post: {
          tags: ["Ensemble Forecasts"], summary: "Reproduce an immutable ensemble forecast", operationId: "reproduceEnsembleForecast",
          description: "Creates a new durable job from the original authorized input snapshot, exact model checkpoints, and stored configuration. Current workspace permissions, plan, and license gates are re-evaluated.",
          "x-quantura-scope": "forecasts:write", "x-workspace-behavior": "Owner and Editor may reproduce. Viewer is read-only.",
          parameters: [idParameter],
          responses: { "202": { description: "Reproduction job queued.", content: { "application/json": { schema: { $ref: "#/components/schemas/EnsembleForecastEnvelope" } } } }, "404": { description: "Original forecast job not found." }, ...commonErrors },
        },
      },
      "/ensemble-forecasts/{forecast_id}/download": {
        get: {
          tags: ["Ensemble Forecasts"], summary: "Download a completed ensemble", operationId: "downloadEnsembleForecast",
          "x-quantura-scope": "forecasts:read", parameters: [idParameter, { name: "format", in: "query", required: false, schema: { type: "string", enum: ["csv", "json"], default: "csv" } }],
          responses: { "200": { description: "CSV or JSON attachment containing final ensemble values only." }, "404": { description: "Completed result not found." }, ...commonErrors },
        },
      },
      "/ensemble-forecast-presets": {
        get: {
          tags: ["Ensemble Forecasts"], summary: "List workspace ensemble presets", operationId: "listEnsembleForecastPresets", "x-quantura-scope": "forecasts:read",
          parameters: [{ name: "workspace_id", in: "query", required: false, schema: { type: "string" } }], responses: { "200": { description: "Authorized presets." }, ...commonErrors },
        },
        post: {
          tags: ["Ensemble Forecasts"], summary: "Save an ensemble preset", operationId: "createEnsembleForecastPreset", "x-quantura-scope": "forecasts:write",
          requestBody: { required: true, content: { "application/json": { schema: { type: "object", required: ["name", "configuration"], properties: { name: { type: "string", maxLength: 100 }, workspace_id: { type: "string" }, configuration: { $ref: "#/components/schemas/EnsembleConfiguration" } } } } } },
          responses: { "201": { description: "Preset created." }, ...commonErrors },
        },
      },
      "/ensemble-forecast-presets/{preset_id}": {
        delete: {
          tags: ["Ensemble Forecasts"], summary: "Delete an owned ensemble preset", operationId: "deleteEnsembleForecastPreset", "x-quantura-scope": "forecasts:write",
          parameters: [{ name: "preset_id", in: "path", required: true, schema: { type: "string" } }], responses: { "200": { description: "Preset deleted." }, ...commonErrors },
        },
      },
      "/forecasts/resolved": {
        get: { tags: ["Forecasts"], summary: "List resolved forecasts", operationId: "listResolvedForecasts", "x-quantura-scope": "forecasts:resolved (forecasts:read alias supported)", parameters: forecastFilters, responses: { "200": { description: "Resolved forecasts with outcome scoring." }, ...commonErrors } },
      },
      "/forecasts/{forecast_id}": {
        get: { tags: ["Forecasts"], summary: "Get current forecast", operationId: "getForecast", "x-quantura-scope": "forecasts:read", parameters: [idParameter], responses: { "200": { description: "Complete current public forecast and amendments." }, "404": { description: "Forecast not found." }, ...commonErrors } },
      },
      "/forecasts/{forecast_id}/history": {
        get: { tags: ["Forecasts"], summary: "Get immutable probability history", operationId: "getForecastHistory", "x-quantura-scope": "forecasts:history (forecasts:read alias supported)", parameters: [idParameter], responses: { "200": { description: "Timestamped probability revisions." }, "404": { description: "Forecast not found." }, ...commonErrors } },
      },
      "/forecasts/{forecast_id}/resolution": {
        get: { tags: ["Forecasts"], summary: "Get resolution and scoring", operationId: "getForecastResolution", "x-quantura-scope": "forecasts:resolved (forecasts:read alias supported)", parameters: [idParameter], responses: { "200": { description: "Resolution rule, evidence, actual outcome, Brier score, and log score." }, "404": { description: "Forecast not found." }, ...commonErrors } },
      },
      "/entities/{entity}/forecasts": {
        get: { tags: ["Forecasts"], summary: "List forecasts for an entity", operationId: "listEntityForecasts", "x-quantura-scope": "forecasts:read", parameters: [{ name: "entity", in: "path", required: true, description: "Exact entity id or name.", schema: { type: "string" } }, ...forecastFilters.filter((parameter) => parameter.name !== "entity")], responses: { "200": { description: "Entity forecast page." }, ...commonErrors } },
      },
      "/categories": {
        get: { tags: ["Forecasts"], summary: "List forecast categories", operationId: "listForecastCategories", "x-quantura-scope": "forecasts:read", responses: { "200": { description: "Canonical extensible category names." }, ...commonErrors } },
      },
      "/forecast-feed": {
        get: { tags: ["Forecasts"], summary: "Get current forecast feed", operationId: "getForecastFeed", "x-quantura-scope": "forecasts:read", parameters: forecastFilters, responses: { "200": { description: "Latest pending forecast cards." }, ...commonErrors } },
      },
      "/calibration": {
        get: { tags: ["Forecasts"], summary: "Get calibration buckets", operationId: "getCalibration", "x-quantura-scope": "forecasts:resolved (forecasts:read alias supported)", parameters: forecastFilters, responses: { "200": { description: "Nominal probability and observed event frequency buckets." }, ...commonErrors } },
      },
      "/performance": {
        get: { tags: ["Forecasts"], summary: "Get aggregate forecast performance", operationId: "getForecastPerformance", "x-quantura-scope": "forecasts:resolved (forecasts:read alias supported)", parameters: forecastFilters, responses: { "200": { description: "Brier, log-score, probability, and calibration aggregates." }, ...commonErrors } },
      },
      "/datasets/forecast-trajectories": {
        get: { tags: ["Datasets"], summary: "Get forecast training trajectories", operationId: "getForecastTrajectories", "x-quantura-scope": "forecasts:bulk (datasets:read alias supported)", "x-plan-entitlement": "Quant or Research API entitlement.", parameters: forecastFilters, responses: { "200": { description: "Forecasts with immutable probability histories and resolved outcomes where available." }, ...commonErrors } },
      },
      "/dataset-releases/{version}": {
        get: { tags: ["Datasets"], summary: "Get immutable dataset release", operationId: "getDatasetRelease", "x-quantura-scope": "forecasts:bulk (datasets:read alias supported)", parameters: [{ name: "version", in: "path", required: true, description: "Immutable dataset version.", schema: { type: "string" } }, { name: "format", in: "query", required: false, description: "When provided, return a short-lived authorized download URL.", schema: { type: "string", enum: ["jsonl", "csv"] } }], responses: { "200": { description: "Release manifest or authorized download URL." }, "404": { description: "Release not found." }, ...commonErrors } },
      },
    },
    components: {
      securitySchemes: {
        bearerAuth: { type: "http", scheme: "bearer", bearerFormat: "Quantura API key" },
      },
      schemas: {
        ModelSelection: {
          type: "object", additionalProperties: false, required: ["enabled", "weight"],
          properties: { enabled: { type: "boolean" }, weight: { type: "number", minimum: 0 } },
        },
        EnsembleConfiguration: {
          type: "object",
          required: ["prediction_length", "horizon_mode", "quantiles", "models"],
          properties: {
            prediction_length: { type: "integer", minimum: 1, maximum: 512, default: 30 },
            horizon_mode: { type: "string", enum: ["trading_sessions", "calendar_days", "frequency_periods"] },
            quantiles: { type: "array", minItems: 1, maxItems: 21, uniqueItems: true, items: { type: "number", exclusiveMinimum: 0, exclusiveMaximum: 1 } },
            transform: { type: "string", enum: ["auto", "log", "none"], default: "auto" },
            context_length: { type: ["integer", "null"], minimum: 40, maximum: 16384, default: 512 },
            model_failure_policy: { type: "string", enum: ["fail", "renormalize"], default: "fail" },
            frequency: { type: "string", default: "1D" },
            calendar: { type: "string", default: "NYSE" },
            models: {
              type: "object", additionalProperties: false,
              properties: {
                prophet: { $ref: "#/components/schemas/ModelSelection" }, toto: { $ref: "#/components/schemas/ModelSelection" },
                granite: { $ref: "#/components/schemas/ModelSelection" }, chronos: { $ref: "#/components/schemas/ModelSelection" }, timesfm: { $ref: "#/components/schemas/ModelSelection" },
              },
            },
          },
        },
        EnsembleForecastRequest: {
          unevaluatedProperties: false,
          allOf: [
            { $ref: "#/components/schemas/EnsembleConfiguration" },
            {
              type: "object", required: ["source"], properties: {
                workspace_id: { type: "string" },
                source: {
                  oneOf: [
                    { type: "object", required: ["type", "symbol"], properties: { type: { const: "ticker" }, symbol: { type: "string" }, provider: { type: "string", enum: ["auto", "alpaca", "yahoo"] }, start: { type: "string", format: "date" }, field: { type: "string", enum: ["open", "high", "low", "close", "volume"] } } },
                    { type: "object", required: ["type", "dataset_id", "timestamp_column", "target_column"], properties: { type: { const: "workspace_dataset" }, dataset_id: { type: "string" }, timestamp_column: { type: "string" }, target_column: { type: "string" }, frequency: { type: "string" }, timezone: { type: "string" } } },
                    { type: "object", required: ["type", "rows"], properties: { type: { const: "series" }, rows: { type: "array", maxItems: 10000, items: { type: "object" } }, timestamp_column: { type: "string" }, target_column: { type: "string" }, frequency: { type: "string" }, timezone: { type: "string" } } },
                  ],
                },
              },
            },
          ],
        },
        EnsemblePrediction: {
          type: "object", required: ["timestamp", "quantiles"], properties: {
            timestamp: { type: "string", format: "date-time" },
            quantiles: { type: "object", additionalProperties: { type: "number" }, description: "Canonical decimal quantile string to final ensemble value." },
          },
        },
        EnsembleForecastEnvelope: {
          type: "object", required: ["data", "meta"], properties: {
            data: { type: "object", required: ["forecast_id", "status"], properties: { forecast_id: { type: "string" }, status: { type: "string", enum: ["queued", "running", "completed", "failed"] }, progress: { type: ["object", "null"] }, predictions: { type: "array", items: { $ref: "#/components/schemas/EnsemblePrediction" } }, effective_weights_by_quantile: { type: "object" }, status_url: { type: "string" }, result_url: { type: "string" } } },
            meta: { type: "object", properties: { api_version: { const: "v1" } } },
          },
        },
        ModelCapabilitiesEnvelope: { type: "object", required: ["data", "meta"], properties: { data: { type: "object", properties: { models: { type: "array", items: { type: "object" } } } }, meta: { type: "object" } } },
      },
    },
  };
}
