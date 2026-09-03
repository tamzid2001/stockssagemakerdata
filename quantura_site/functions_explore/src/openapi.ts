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

const OPERATION_TITLES: Record<string, string> = {
  getMyAccess: "Get the Authenticated User's Effective API, Plan, and Workspace Access",
  getCapabilities: "Get the Authenticated User's Effective Quantura Product Capabilities",
  listWorkspaces: "List Every Workspace the Authenticated User Can Currently Access",
  createWorkspace: "Create a New Workspace Owned by the Authenticated User",
  listWorkspacePermissions: "List Every Supported Fine-Grained Workspace Permission Identifier",
  getWorkspace: "Get One Workspace with the Caller's Current Role and Effective Permissions",
  updateWorkspace: "Update the Name, Description, or Settings of an Authorized Workspace",
  listWorkspaceCollaborators: "List the Current Collaborators and Permissions for One Workspace",
  addWorkspaceCollaborator: "Add an Existing Quantura User as a Workspace Collaborator",
  updateWorkspaceCollaborator: "Update a Workspace Collaborator's Role, Permissions, or Resource Scope",
  removeWorkspaceCollaborator: "Remove a Collaborator and Immediately Revoke Their Workspace Access",
  listWorkspaceAudit: "List Recent Administrative and CSV Audit Events for One Workspace",
  listUploadedCsvs: "List Metadata for Every Uploaded CSV the Caller Is Authorized to Access",
  uploadCsv: "Upload and Analyze a CSV in an Authorized Quantura Workspace",
  downloadAllUploadedCsvs: "Download Every Authorized Uploaded CSV as a Manifested ZIP Archive",
  getUploadedCsv: "Get Safe Metadata and Provenance for One Authorized Uploaded CSV",
  renameUploadedCsv: "Rename the Display Filename of One Authorized Uploaded CSV",
  deleteUploadedCsv: "Delete One Authorized Uploaded CSV and Its Managed Storage Object",
  downloadUploadedCsv: "Download the Original Bytes of One Authorized Uploaded CSV",
  moveUploadedCsv: "Move One Uploaded CSV from Its Current Workspace to Another Workspace",
  copyUploadedCsv: "Copy One Uploaded CSV into Another Authorized Workspace",
  bulkMoveUploadedCsvs: "Move Multiple Authorized Uploaded CSVs to Another Workspace",
  bulkCopyUploadedCsvs: "Copy Multiple Authorized Uploaded CSVs to Another Workspace",
  listWorkspaceUploadedCsvs: "List Authorized Uploaded CSV Metadata Within One Specific Workspace",
  getWorkspaceUploadedCsv: "Get Authorized Metadata for One Uploaded CSV Within a Specific Workspace",
  downloadWorkspaceUploadedCsv: "Download One Authorized Uploaded CSV from a Specific Workspace",
  downloadAllWorkspaceUploadedCsvs: "Download All Authorized CSVs from One Workspace as a ZIP Archive",
  listWorkspaceResources: "List Authorized Forecast, Analysis, Dataset, or Backtest Resources in One Workspace",
  listDatasets: "List the Quantura Dataset Catalog with Provenance and Licensing Metadata",
  getDataset: "Get Metadata, Availability, and Provenance for One Quantura Dataset",
  getDatasetSchema: "Get the Machine-Readable Column Schema for One Quantura Dataset",
  listForecasts: "List Quantura Event Forecasts Matching the Requested Filters",
  getEnsembleModelCapabilities: "List Available Ensemble Models, Quantile Support, and License Constraints",
  createEnsembleForecast: "Create an Asynchronous Multi-Model Quantura Forecast Job",
  getEnsembleForecast: "Get an Ensemble Forecast Job's Progress or Completed Quantile Results",
  reproduceEnsembleForecast: "Create a New Forecast Job from an Immutable Historical Configuration",
  downloadEnsembleForecast: "Download a Completed Ensemble Forecast as CSV or JSON",
  listEnsembleForecastPresets: "List Reusable Ensemble Forecast Configurations for an Authorized Workspace",
  createEnsembleForecastPreset: "Save a Reusable Ensemble Forecast Configuration in a Workspace",
  deleteEnsembleForecastPreset: "Delete One Reusable Ensemble Forecast Configuration from a Workspace",
  listResolvedForecasts: "List Resolved Quantura Event Forecasts with Outcome Scoring",
  getForecast: "Get the Current Published Revision of One Quantura Event Forecast",
  getForecastHistory: "Get the Immutable Probability Revision History for One Event Forecast",
  getForecastResolution: "Get Resolution Evidence, Outcome, and Probabilistic Scores for One Forecast",
  listEntityForecasts: "List Quantura Event Forecasts Associated with One Entity",
  listForecastCategories: "List the Canonical Categories Supported by Quantura Event Forecasts",
  getForecastFeed: "Get the Current Ranked Feed of Unresolved Quantura Event Forecasts",
  getCalibration: "Get Forecast Calibration Buckets and Observed Event Frequencies",
  getForecastPerformance: "Get Aggregate Probabilistic Performance Metrics for Resolved Forecasts",
  getForecastTrajectories: "Get Authorized Forecast Trajectories for Research or Training Datasets",
  getDatasetRelease: "Get an Immutable Enterprise Dataset Release Manifest or Download",
};

const successMeta = (count?: number) => ({
  api_version: "v1",
  ...(typeof count === "number" ? { count, next_cursor: null } : {}),
});

function successExample(operationId: string): unknown {
  const workspace = {
    id: "ws_01JEXAMPLE9Y6Z",
    name: "Institutional Research",
    slug: "institutional-research",
    owner_user_id: "usr_example_owner",
    role: "owner",
    permissions: ["workspace.read", "csv.list", "csv.read", "csv.download"],
    resource_scope: { csv: { mode: "all", ids: [] }, forecasts: { mode: "all", ids: [] } },
    capabilities: ["csv.read", "forecast.read"],
    legacy_personal: false,
    created_at: "2026-09-03T14:30:00.000Z",
    updated_at: "2026-09-03T14:30:00.000Z",
  };
  const csv = {
    id: "csv_01JEXAMPLE8Q4M",
    workspace_id: workspace.id,
    original_filename: "aapl-forecast.csv",
    display_filename: "AAPL Quantura Forecast.csv",
    content_type: "text/csv",
    size_bytes: 12345,
    sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    uploaded_by_user_id: "usr_example_owner",
    uploaded_at: "2026-09-03T14:35:00.000Z",
    updated_at: "2026-09-03T14:35:00.000Z",
    ticker: "AAPL",
    asset_class: "equity",
    source_type: "prediction_csv",
    forecast_brand: "Quantura Forecast",
    forecast_kind: "ensemble",
    row_count: 100,
    column_count: 6,
    data_cell_count: 600,
    populated_data_cell_count: 598,
    columns: ["date", "p01", "p25", "p50", "p75", "p99"],
    date_column: "date",
    first_timestamp: "2026-09-04T00:00:00.000Z",
    last_timestamp: "2027-01-22T00:00:00.000Z",
    inferred_granularity: "1d",
    quantile_columns: ["p01", "p25", "p50", "p75", "p99"],
    metadata_status: "complete",
    metadata_warnings: [],
    download_endpoint: "/api/v1/uploads/csv/csv_01JEXAMPLE8Q4M/download",
  };
  const forecast = {
    forecast_id: "qf_01JEXAMPLE4K2P",
    category: "earnings",
    entity: { type: "company", name: "Example Corporation", ticker: "EXM" },
    question: "Will Example Corporation report revenue above its stored consensus estimate?",
    possible_future_headline: "Example Corporation may report revenue above the stored consensus estimate.",
    probability: 0.67,
    status: "pending",
    created_at: "2026-09-03T14:40:00.000Z",
    input_cutoff_at: "2026-09-03T14:30:00.000Z",
    resolution_deadline: "2026-11-18T21:00:00.000Z",
  };
  const ensemble = {
    forecast_id: "ens_01JEXAMPLE7M6N",
    status: "queued",
    progress: { completed_models: 0, total_models: 4, current_model: null },
    status_url: "/api/v1/ensemble-forecasts/ens_01JEXAMPLE7M6N",
    result_url: "/api/v1/ensemble-forecasts/ens_01JEXAMPLE7M6N",
  };
  const membership = {
    id: "member_01JEXAMPLE",
    user_id: "usr_example_analyst",
    email: "analyst@example.com",
    display_name: "Example Analyst",
    role: "analyst",
    permissions: ["workspace.read", "csv.list", "csv.read", "csv.download", "forecast.read", "forecast.create"],
    resource_scope: { csv: { mode: "all", ids: [] }, forecasts: { mode: "all", ids: [] } },
    status: "active",
    added_at: "2026-09-03T15:00:00.000Z",
    updated_at: "2026-09-03T15:00:00.000Z",
  };

  switch (operationId) {
    case "getMyAccess": return { data: { user: { id: "usr_example", email: "developer@example.com" }, token_scopes: ["account:read", "workspaces:read", "datasets:read"], personal_plan: "pro", workspaces: [workspace] }, meta: successMeta() };
    case "getCapabilities": return { data: { forecasting: true, screener: true, historical_data: true, options: true, sports: true, dataset_download: true, backtesting: false, collaboration: true, sagemaker: true }, meta: successMeta() };
    case "listWorkspaces": return { data: [workspace], meta: successMeta(1) };
    case "createWorkspace": case "getWorkspace": case "updateWorkspace": return { data: workspace, meta: successMeta() };
    case "listWorkspacePermissions": return { data: ["workspace.read", "workspace.settings.read", "csv.list", "csv.read", "csv.download"], meta: successMeta(5) };
    case "listWorkspaceCollaborators": return { data: [membership], meta: { api_version: "v1", count: 1 } };
    case "addWorkspaceCollaborator": case "updateWorkspaceCollaborator": return { data: membership, meta: successMeta() };
    case "removeWorkspaceCollaborator": return { data: { collaborator_id: membership.id, workspace_id: workspace.id, removed: true }, meta: successMeta() };
    case "listWorkspaceAudit": return { data: [{ id: "audit_01JEXAMPLE", action: "csv_downloaded", actor_user_id: "usr_example", resource_type: "csv", resource_id: csv.id, timestamp: "2026-09-03T15:10:00.000Z" }], meta: successMeta(1) };
    case "listUploadedCsvs": case "listWorkspaceUploadedCsvs": return { data: [csv], meta: { ...successMeta(1), workspace_id: workspace.id } };
    case "uploadCsv": case "getUploadedCsv": case "getWorkspaceUploadedCsv": case "renameUploadedCsv": return { data: csv, meta: successMeta() };
    case "deleteUploadedCsv": return { data: { csv_id: csv.id, deleted: true }, meta: successMeta() };
    case "moveUploadedCsv": return { data: { ...csv, workspace_id: "ws_01JDESTINATION" }, meta: successMeta() };
    case "copyUploadedCsv": return { data: { ...csv, id: "csv_01JCOPYEXAMPLE", workspace_id: "ws_01JDESTINATION", copied_from_csv_id: csv.id, copied_from_workspace_id: workspace.id, copied_at: "2026-09-03T15:20:00.000Z" }, meta: successMeta() };
    case "bulkMoveUploadedCsvs": case "bulkCopyUploadedCsvs": return { data: { successful: [{ csv_id: csv.id, destination_workspace_id: "ws_01JDESTINATION" }], failed: [] }, meta: successMeta() };
    case "listWorkspaceResources": return { data: [{ id: "resource_01JEXAMPLE", type: "forecast", workspace_id: workspace.id, created_at: "2026-09-03T15:30:00.000Z" }], meta: successMeta(1) };
    case "listDatasets": return { data: [{ id: "quantura-forecast-trajectories", name: "Quantura Forecast Trajectories", derived: true, source_provider: "Quantura", asset_class: "multi_asset", license_status: "quantura_owned", api_available: true }], meta: successMeta(1) };
    case "getDataset": return { data: { id: "quantura-forecast-trajectories", name: "Quantura Forecast Trajectories", description: "Immutable prospective forecast revisions and resolved outcomes.", derived: true, license_status: "quantura_owned", last_updated: "2026-09-03T15:30:00.000Z" }, meta: successMeta() };
    case "getDatasetSchema": return { data: { dataset_id: "quantura-forecast-trajectories", schema_version: "1.0.0", columns: [{ name: "forecast_id", type: "string", nullable: false }, { name: "probability", type: "number", nullable: false }] }, meta: successMeta() };
    case "listForecasts": case "listResolvedForecasts": case "listEntityForecasts": case "getForecastFeed": return { data: [forecast], meta: successMeta(1) };
    case "getForecast": return { data: forecast, meta: successMeta() };
    case "getForecastHistory": return { data: [{ forecast_id: forecast.forecast_id, probability: 0.67, reasoning_delta: "Updated structured evidence.", model_version: "qf-1.0.0", input_cutoff_at: forecast.input_cutoff_at, created_at: forecast.created_at }], meta: successMeta(1) };
    case "getForecastResolution": return { data: { forecast_id: forecast.forecast_id, status: "resolved_yes", actual_outcome: "The formal proposition resolved YES.", resolved_at: "2026-11-18T21:15:00.000Z", brier_score: 0.1089, log_score: 0.4005, resolution_source: "authoritative-source" }, meta: successMeta() };
    case "listForecastCategories": return { data: ["markets", "earnings", "corporate", "products_technology", "politics_policy", "economics", "sports"], meta: successMeta(7) };
    case "getCalibration": return { data: [{ bucket: "60-70%", predicted_average_probability: 0.66, actual_event_frequency: 0.64, forecast_count: 125 }], meta: successMeta(1) };
    case "getForecastPerformance": return { data: { resolved_forecasts: 125, average_brier_score: 0.192, average_log_score: 0.574, average_probability: 0.66 }, meta: successMeta() };
    case "getForecastTrajectories": return { data: [{ ...forecast, probability_history: [{ probability: 0.61, created_at: "2026-08-28T14:30:00.000Z" }, { probability: 0.67, created_at: forecast.created_at }], actual_outcome: null, brier_score: null }], meta: successMeta(1) };
    case "getDatasetRelease": return { data: { dataset_version: "quantura-forecast-trajectories-2026-09", schema_version: "1.0.0", generated_at: "2026-09-03T16:00:00.000Z", source_cutoff: "2026-09-03T15:59:59.000Z", record_count: 125, checksum: "sha256:0123456789abcdef", formats: ["jsonl", "csv"] }, meta: successMeta() };
    case "getEnsembleModelCapabilities": return { data: { models: [{ id: "prophet", name: "Meta Prophet", available: true, default_weight: 0.2, quantile_support: { type: "requested" } }, { id: "toto", name: "Toto 2.0", available: true, default_weight: 0.2, quantile_support: { min: 0.1, max: 0.9, interpolation_inside_range: true } }] }, meta: successMeta() };
    case "createEnsembleForecast": case "reproduceEnsembleForecast": case "getEnsembleForecast": return { data: ensemble, meta: successMeta() };
    case "listEnsembleForecastPresets": return { data: [{ id: "preset_01JEXAMPLE", name: "Balanced Ensemble", workspace_id: workspace.id, configuration: { prediction_length: 30, horizon_mode: "trading_sessions", quantiles: [0.01, 0.25, 0.5, 0.75, 0.99] } }], meta: successMeta(1) };
    case "createEnsembleForecastPreset": return { data: { id: "preset_01JEXAMPLE", name: "Balanced Ensemble", workspace_id: workspace.id }, meta: successMeta() };
    case "deleteEnsembleForecastPreset": return { data: { preset_id: "preset_01JEXAMPLE", deleted: true }, meta: successMeta() };
    default: return { data: { success: true }, meta: successMeta() };
  }
}

function addDocumentationExamples(document: any): void {
  const downloadExamples: Record<string, { mediaType: string; value: string }> = {
    downloadAllUploadedCsvs: { mediaType: "application/zip", value: "<binary ZIP archive containing authorized CSVs and manifests>" },
    downloadAllWorkspaceUploadedCsvs: { mediaType: "application/zip", value: "<binary ZIP archive containing authorized workspace CSVs and manifests>" },
    downloadUploadedCsv: { mediaType: "text/csv", value: "date,p01,p25,p50,p75,p99\n2026-09-04,135.10,140.25,144.80,149.30,157.90\n" },
    downloadWorkspaceUploadedCsv: { mediaType: "text/csv", value: "date,p01,p25,p50,p75,p99\n2026-09-04,135.10,140.25,144.80,149.30,157.90\n" },
    downloadEnsembleForecast: { mediaType: "text/csv", value: "timestamp,p01,p25,p50,p75,p99\n2026-09-04T00:00:00.000Z,135.10,140.25,144.80,149.30,157.90\n" },
  };
  const methods = ["get", "post", "put", "patch", "delete"];
  for (const pathItem of Object.values(document.paths || {}) as any[]) {
    for (const method of methods) {
      const operation = pathItem?.[method];
      if (!operation?.operationId) continue;
      operation.summary = OPERATION_TITLES[operation.operationId] || operation.summary;
      const example = successExample(operation.operationId);
      for (const [status, responseValue] of Object.entries(operation.responses || {})) {
        if (!/^2\d\d$/.test(status)) continue;
        const response = responseValue as any;
        const download = downloadExamples[operation.operationId];
        if (download) {
          response.content ||= {};
          response.content[download.mediaType] ||= { schema: { type: "string", format: "binary" } };
          response.content[download.mediaType].example ||= download.value;
          continue;
        }
        response.content ||= { "application/json": { schema: { type: "object" } } };
        for (const [mediaType, mediaValue] of Object.entries(response.content) as [string, any][]) {
          if (mediaType.includes("json")) mediaValue.example ||= example;
        }
      }
    }
  }
}

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
  const document: any = {
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
      { name: "Uploaded CSVs", description: "Workspace-isolated CSV metadata, organization, and authorized exports." },
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
          "x-mint": { mcp: { enabled: true, name: "quantura_get_my_access", description: "Inspect the current Quantura identity, API-key scopes, plan, and live workspace access without returning secrets." } },
          responses: { "200": { description: "Effective identity, scopes, plan, and workspaces." }, ...commonErrors },
        },
      },
      "/capabilities": {
        get: { tags: ["Access"], summary: "Get effective product capabilities", operationId: "getCapabilities", "x-quantura-scope": "account:read", responses: { "200": { description: "Personal and workspace-scoped capability map." }, ...commonErrors } },
      },
      "/workspaces": {
        get: {
          tags: ["Workspaces"], summary: "List accessible workspaces", operationId: "listWorkspaces", "x-quantura-scope": "workspaces:read",
          "x-mint": { mcp: { enabled: true, name: "quantura_list_workspaces", description: "List the workspaces the authenticated Quantura user can currently access and their effective roles and permissions." } },
          responses: { "200": { description: "Owner and current collaborator workspaces.", content: { "application/json": { schema: { $ref: "#/components/schemas/WorkspaceListEnvelope" } } } }, ...commonErrors },
        },
        post: {
          tags: ["Workspaces"], summary: "Create a workspace", operationId: "createWorkspace", "x-quantura-scope": "workspaces:write",
          requestBody: { required: true, content: { "application/json": { schema: { type: "object", additionalProperties: false, required: ["name"], properties: { name: { type: "string", minLength: 2, maxLength: 80 }, description: { type: "string", maxLength: 500 } } } } } },
          responses: { "201": { description: "Workspace created.", content: { "application/json": { schema: { $ref: "#/components/schemas/WorkspaceEnvelope" } } } }, "422": { description: "Workspace name or plan limit validation failed.", content: { "application/json": { schema: errorSchema } } }, ...commonErrors },
        },
      },
      "/workspace-permissions": {
        get: {
          tags: ["Workspaces"], summary: "List workspace permission identifiers", operationId: "listWorkspacePermissions", "x-quantura-scope": "workspaces:read",
          responses: { "200": { description: "Canonical permission identifiers and count." }, ...commonErrors },
        },
      },
      "/workspaces/{workspace_id}": {
        get: {
          tags: ["Workspaces"], summary: "Get a workspace", operationId: "getWorkspace", "x-quantura-scope": "workspaces:read",
          parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }],
          responses: { "200": { description: "Effective workspace access.", content: { "application/json": { schema: { $ref: "#/components/schemas/WorkspaceEnvelope" } } } }, "404": { description: "Workspace not found." }, ...commonErrors },
        },
        patch: {
          tags: ["Workspaces"], summary: "Update workspace settings", operationId: "updateWorkspace", "x-quantura-scope": "workspaces:write",
          parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }],
          requestBody: { required: true, content: { "application/json": { schema: { type: "object", additionalProperties: false, properties: { name: { type: "string", minLength: 2, maxLength: 80 }, description: { type: "string", maxLength: 500 }, settings: { type: "object", additionalProperties: false, properties: { default_csv_visibility: { type: "string", enum: ["workspace", "private"] }, allow_member_invites: { type: "boolean" } } } } } } } },
          responses: { "200": { description: "Workspace updated." }, ...commonErrors },
        },
      },
      "/workspaces/{workspace_id}/collaborators": {
        get: {
          tags: ["Workspaces"], summary: "List workspace collaborators", operationId: "listWorkspaceCollaborators", "x-quantura-scope": "workspaces:read",
          parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }],
          responses: { "200": { description: "Current active collaborator memberships.", content: { "application/json": { schema: { $ref: "#/components/schemas/WorkspaceMembershipListEnvelope" } } } }, ...commonErrors },
        },
        post: {
          tags: ["Workspaces"], summary: "Add an existing Quantura user", operationId: "addWorkspaceCollaborator", "x-quantura-scope": "workspaces:write",
          parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }],
          requestBody: { required: true, content: { "application/json": { schema: { $ref: "#/components/schemas/WorkspaceMembershipMutation" } } } },
          responses: { "201": { description: "Collaborator added." }, "409": { description: "Membership conflict." }, "422": { description: "Invalid or ungrantable role, permission, or resource scope." }, ...commonErrors },
        },
      },
      "/workspaces/{workspace_id}/collaborators/{collaborator_id}": {
        patch: {
          tags: ["Workspaces"], summary: "Update collaborator access", operationId: "updateWorkspaceCollaborator", "x-quantura-scope": "workspaces:write",
          parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }, { name: "collaborator_id", in: "path", required: true, schema: { type: "string" } }],
          requestBody: { required: true, content: { "application/json": { schema: { $ref: "#/components/schemas/WorkspaceMembershipMutation" } } } },
          responses: { "200": { description: "Collaborator access updated immediately." }, ...commonErrors },
        },
        delete: {
          tags: ["Workspaces"], summary: "Remove a collaborator", operationId: "removeWorkspaceCollaborator", "x-quantura-scope": "workspaces:write",
          description: "Immediately removes this workspace membership without revoking the collaborator's personal account or API keys.",
          parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }, { name: "collaborator_id", in: "path", required: true, schema: { type: "string" } }],
          responses: { "200": { description: "Collaborator removed." }, ...commonErrors },
        },
      },
      "/workspaces/{workspace_id}/audit-log": {
        get: {
          tags: ["Workspaces"], summary: "List workspace audit events", operationId: "listWorkspaceAudit", "x-quantura-scope": "workspaces:read",
          parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }, { name: "limit", in: "query", schema: { type: "integer", minimum: 1, maximum: 200, default: 50 } }],
          responses: { "200": { description: "Newest workspace administration and CSV events." }, ...commonErrors },
        },
      },
      "/uploads/csv": {
        get: {
          tags: ["Uploaded CSVs"], summary: "List authorized uploaded CSVs", operationId: "listUploadedCsvs", "x-quantura-scope": "datasets:read",
          description: "Lists only files allowed by the caller's current workspace membership, csv.list permission, and resource scope.",
          "x-mint": { mcp: { enabled: true, name: "quantura_list_uploaded_csvs", description: "List safe metadata for CSV files the authenticated Quantura user may currently access." } },
          parameters: [
            { name: "workspace_id", in: "query", schema: { type: "string" } }, { name: "ticker", in: "query", schema: { type: "string", maxLength: 20 } },
            { name: "source", in: "query", schema: { type: "string", maxLength: 80 } }, { name: "from", in: "query", schema: { type: "string", format: "date-time" } },
            { name: "to", in: "query", schema: { type: "string", format: "date-time" } }, { name: "sort", in: "query", schema: { type: "string", enum: ["uploaded_at", "display_filename", "size_bytes"], default: "uploaded_at" } },
            { name: "order", in: "query", schema: { type: "string", enum: ["asc", "desc"], default: "desc" } }, { name: "limit", in: "query", schema: { type: "integer", minimum: 1, maximum: 500, default: 50 } },
            { name: "cursor", in: "query", schema: { type: "string" } },
          ],
          responses: { "200": { description: "Authorized CSV metadata and cursor.", content: { "application/json": { schema: { $ref: "#/components/schemas/UploadedCsvListEnvelope" } } } }, ...commonErrors },
        },
        post: {
          tags: ["Uploaded CSVs"], summary: "Upload a CSV", operationId: "uploadCsv", "x-quantura-scope": "datasets:write",
          description: "Uploads one bounded UTF-8 CSV by text or base64. The destination workspace must grant csv.upload.",
          requestBody: { required: true, content: { "application/json": { schema: { type: "object", additionalProperties: false, required: ["filename"], properties: { workspace_id: { type: "string" }, filename: { type: "string", maxLength: 240 }, csv_text: { type: "string" }, content_base64: { type: "string", contentEncoding: "base64" }, metadata: { type: "object" } }, oneOf: [{ required: ["csv_text"] }, { required: ["content_base64"] }] } } } },
          responses: { "201": { description: "CSV stored and metadata derived from its actual bytes." }, "413": { description: "CSV exceeds configured limits." }, "422": { description: "CSV parsing or validation failed." }, ...commonErrors },
        },
      },
      "/uploads/csv/download-all": {
        get: {
          tags: ["Uploaded CSVs"], summary: "Download all authorized CSVs", operationId: "downloadAllUploadedCsvs", "x-quantura-scope": "datasets:read",
          description: "Returns a ZIP containing only CSVs authorized by current membership/resource scope plus JSON and CSV manifests.",
          parameters: [{ name: "workspace_id", in: "query", schema: { type: "string" } }, { name: "ticker", in: "query", schema: { type: "string" } }, { name: "source", in: "query", schema: { type: "string" } }],
          responses: { "200": { description: "ZIP archive.", content: { "application/zip": { schema: { type: "string", format: "binary" } } } }, ...commonErrors },
        },
      },
      "/uploads/csv/{csv_id}": {
        get: {
          tags: ["Uploaded CSVs"], summary: "Get uploaded CSV metadata", operationId: "getUploadedCsv", "x-quantura-scope": "datasets:read",
          "x-mint": { mcp: { enabled: true, name: "quantura_get_uploaded_csv", description: "Get safe metadata and provenance for one authorized Quantura CSV without exposing its raw contents or storage path." } },
          parameters: [{ name: "csv_id", in: "path", required: true, schema: { type: "string" } }],
          responses: { "200": { description: "CSV metadata.", content: { "application/json": { schema: { $ref: "#/components/schemas/UploadedCsvEnvelope" } } } }, "404": { description: "CSV not found or not visible." }, ...commonErrors },
        },
        patch: {
          tags: ["Uploaded CSVs"], summary: "Rename a CSV", operationId: "renameUploadedCsv", "x-quantura-scope": "datasets:write",
          parameters: [{ name: "csv_id", in: "path", required: true, schema: { type: "string" } }], requestBody: { required: true, content: { "application/json": { schema: { type: "object", additionalProperties: false, required: ["display_filename"], properties: { display_filename: { type: "string", maxLength: 240 } } } } } },
          responses: { "200": { description: "CSV display name updated." }, ...commonErrors },
        },
        delete: {
          tags: ["Uploaded CSVs"], summary: "Delete a CSV", operationId: "deleteUploadedCsv", "x-quantura-scope": "datasets:write",
          parameters: [{ name: "csv_id", in: "path", required: true, schema: { type: "string" } }], responses: { "200": { description: "CSV deleted." }, ...commonErrors },
        },
      },
      "/uploads/csv/{csv_id}/download": {
        get: {
          tags: ["Uploaded CSVs"], summary: "Download one uploaded CSV", operationId: "downloadUploadedCsv", "x-quantura-scope": "datasets:read",
          parameters: [{ name: "csv_id", in: "path", required: true, schema: { type: "string" } }],
          responses: { "200": { description: "Original authorized CSV bytes.", content: { "text/csv": { schema: { type: "string", format: "binary" } } } }, "404": { description: "CSV not found or not visible." }, ...commonErrors },
        },
      },
      "/uploads/csv/{csv_id}/move": {
        post: { tags: ["Uploaded CSVs"], summary: "Move a CSV between workspaces", operationId: "moveUploadedCsv", "x-quantura-scope": "datasets:write", parameters: [{ name: "csv_id", in: "path", required: true, schema: { type: "string" } }], requestBody: { required: true, content: { "application/json": { schema: { $ref: "#/components/schemas/CsvDestination" } } } }, responses: { "200": { description: "CSV moved transactionally." }, ...commonErrors } },
      },
      "/uploads/csv/{csv_id}/copy": {
        post: { tags: ["Uploaded CSVs"], summary: "Copy a CSV into another workspace", operationId: "copyUploadedCsv", "x-quantura-scope": "datasets:write", parameters: [{ name: "csv_id", in: "path", required: true, schema: { type: "string" } }], requestBody: { required: true, content: { "application/json": { schema: { $ref: "#/components/schemas/CsvDestination" } } } }, responses: { "201": { description: "New destination-owned CSV record created." }, ...commonErrors } },
      },
      "/uploads/csv/bulk-move": {
        post: { tags: ["Uploaded CSVs"], summary: "Move multiple CSVs", operationId: "bulkMoveUploadedCsvs", "x-quantura-scope": "datasets:write", requestBody: { required: true, content: { "application/json": { schema: { $ref: "#/components/schemas/CsvBulkDestination" } } } }, responses: { "200": { description: "Per-resource move results; unauthorized files are never transferred." }, ...commonErrors } },
      },
      "/uploads/csv/bulk-copy": {
        post: { tags: ["Uploaded CSVs"], summary: "Copy multiple CSVs", operationId: "bulkCopyUploadedCsvs", "x-quantura-scope": "datasets:write", requestBody: { required: true, content: { "application/json": { schema: { $ref: "#/components/schemas/CsvBulkDestination" } } } }, responses: { "200": { description: "Per-resource copy results; unauthorized files are never transferred." }, ...commonErrors } },
      },
      "/workspaces/{workspace_id}/uploads/csv": {
        get: { tags: ["Uploaded CSVs"], summary: "List CSVs in one workspace", operationId: "listWorkspaceUploadedCsvs", "x-quantura-scope": "datasets:read", parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }], responses: { "200": { description: "Authorized CSV metadata for this workspace." }, ...commonErrors } },
      },
      "/workspaces/{workspace_id}/uploads/csv/{csv_id}": {
        get: { tags: ["Uploaded CSVs"], summary: "Get workspace CSV metadata", operationId: "getWorkspaceUploadedCsv", "x-quantura-scope": "datasets:read", parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }, { name: "csv_id", in: "path", required: true, schema: { type: "string" } }], responses: { "200": { description: "Authorized CSV metadata." }, ...commonErrors } },
      },
      "/workspaces/{workspace_id}/uploads/csv/{csv_id}/download": {
        get: { tags: ["Uploaded CSVs"], summary: "Download one workspace CSV", operationId: "downloadWorkspaceUploadedCsv", "x-quantura-scope": "datasets:read", parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }, { name: "csv_id", in: "path", required: true, schema: { type: "string" } }], responses: { "200": { description: "Original authorized CSV bytes." }, ...commonErrors } },
      },
      "/workspaces/{workspace_id}/uploads/csv/download-all": {
        get: { tags: ["Uploaded CSVs"], summary: "Download all authorized CSVs in a workspace", operationId: "downloadAllWorkspaceUploadedCsvs", "x-quantura-scope": "datasets:read", parameters: [{ name: "workspace_id", in: "path", required: true, schema: { type: "string" } }], responses: { "200": { description: "Authorized ZIP archive." }, ...commonErrors } },
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
        WorkspaceResourceScope: {
          type: "object",
          additionalProperties: false,
          properties: {
            csv: { type: "object", additionalProperties: false, required: ["mode", "ids"], properties: { mode: { type: "string", enum: ["all", "selected", "none"] }, ids: { type: "array", uniqueItems: true, maxItems: 1000, items: { type: "string" } } } },
            forecasts: { type: "object", additionalProperties: false, required: ["mode", "ids"], properties: { mode: { type: "string", enum: ["all", "selected", "none"] }, ids: { type: "array", uniqueItems: true, maxItems: 1000, items: { type: "string" } } } },
          },
        },
        Workspace: {
          type: "object", required: ["id", "name", "owner_user_id", "role", "permissions", "resource_scope"],
          properties: {
            id: { type: "string" }, name: { type: "string" }, slug: { type: "string" }, description: { type: "string" }, owner_user_id: { type: "string" },
            role: { type: "string", enum: ["owner", "admin", "editor", "analyst", "viewer", "custom"] },
            permissions: { type: "array", uniqueItems: true, items: { type: "string" } }, resource_scope: { $ref: "#/components/schemas/WorkspaceResourceScope" },
            capabilities: { type: "array", items: { type: "string" } }, legacy_personal: { type: "boolean" }, created_at: { type: ["string", "null"], format: "date-time" }, updated_at: { type: ["string", "null"], format: "date-time" },
            settings: { type: "object", properties: { default_csv_visibility: { type: "string", enum: ["workspace", "private"] }, allow_member_invites: { type: "boolean" } } },
          },
        },
        WorkspaceMembership: {
          type: "object", required: ["id", "user_id", "role", "permissions", "resource_scope", "status"],
          properties: {
            id: { type: "string" }, user_id: { type: "string" }, email: { type: ["string", "null"], format: "email" }, display_name: { type: ["string", "null"] },
            role: { type: "string", enum: ["admin", "editor", "analyst", "viewer", "custom"] }, permissions: { type: "array", uniqueItems: true, items: { type: "string" } },
            resource_scope: { $ref: "#/components/schemas/WorkspaceResourceScope" }, status: { type: "string", enum: ["active", "removed"] },
            added_at: { type: ["string", "null"], format: "date-time" }, updated_at: { type: ["string", "null"], format: "date-time" },
          },
        },
        WorkspaceMembershipMutation: {
          type: "object", additionalProperties: false,
          properties: {
            email: { type: "string", format: "email" }, role: { type: "string", enum: ["admin", "editor", "analyst", "viewer", "custom"], default: "viewer" },
            permissions: { type: "array", uniqueItems: true, items: { type: "string" } }, resource_scope: { $ref: "#/components/schemas/WorkspaceResourceScope" },
            status: { type: "string", enum: ["active", "removed"] },
          },
        },
        WorkspaceEnvelope: { type: "object", required: ["data", "meta"], properties: { data: { $ref: "#/components/schemas/Workspace" }, meta: { type: "object", properties: { api_version: { const: "v1" } } } } },
        WorkspaceListEnvelope: { type: "object", required: ["data", "meta"], properties: { data: { type: "array", items: { $ref: "#/components/schemas/Workspace" } }, meta: { type: "object", properties: { api_version: { const: "v1" }, count: { type: "integer" }, next_cursor: { type: ["string", "null"] } } } } },
        WorkspaceMembershipListEnvelope: { type: "object", required: ["data", "meta"], properties: { data: { type: "array", items: { $ref: "#/components/schemas/WorkspaceMembership" } }, meta: { type: "object", properties: { api_version: { const: "v1" }, count: { type: "integer" } } } } },
        UploadedCsv: {
          type: "object", required: ["id", "workspace_id", "original_filename", "display_filename", "content_type", "size_bytes", "sha256", "uploaded_at", "row_count", "column_count", "columns", "metadata_status", "metadata_warnings", "download_endpoint"],
          properties: {
            id: { type: "string" }, workspace_id: { type: "string" }, original_filename: { type: "string" }, display_filename: { type: "string" }, content_type: { const: "text/csv" },
            size_bytes: { type: "integer", minimum: 1 }, sha256: { type: "string", pattern: "^[a-f0-9]{64}$" }, uploaded_by_user_id: { type: "string" }, uploaded_at: { type: "string", format: "date-time" }, updated_at: { type: "string", format: "date-time" },
            ticker: { type: ["string", "null"] }, asset_class: { type: ["string", "null"] }, source_type: { type: ["string", "null"] }, forecast_brand: { type: ["string", "null"] }, forecast_kind: { type: ["string", "null"] },
            row_count: { type: "integer", minimum: 0 }, column_count: { type: "integer", minimum: 1 }, data_cell_count: { type: "integer", minimum: 0 }, populated_data_cell_count: { type: "integer", minimum: 0 },
            columns: { type: "array", items: { type: "string" } }, date_column: { type: ["string", "null"] }, first_timestamp: { type: ["string", "null"], format: "date-time" }, last_timestamp: { type: ["string", "null"], format: "date-time" }, inferred_granularity: { type: ["string", "null"] },
            quantile_columns: { type: "array", items: { type: "string" } }, metadata_status: { type: "string" }, metadata_warnings: { type: "array", items: { type: "string" } }, download_endpoint: { type: "string" },
            copied_from_csv_id: { type: ["string", "null"] }, copied_from_workspace_id: { type: ["string", "null"] }, copied_at: { type: ["string", "null"], format: "date-time" },
          },
        },
        UploadedCsvEnvelope: { type: "object", required: ["data", "meta"], properties: { data: { $ref: "#/components/schemas/UploadedCsv" }, meta: { type: "object", properties: { api_version: { const: "v1" } } } } },
        UploadedCsvListEnvelope: { type: "object", required: ["data", "meta"], properties: { data: { type: "array", items: { $ref: "#/components/schemas/UploadedCsv" } }, meta: { type: "object", properties: { api_version: { const: "v1" }, count: { type: "integer" }, next_cursor: { type: ["string", "null"] }, workspace_id: { type: ["string", "null"] } } } } },
        CsvDestination: { type: "object", additionalProperties: false, required: ["destination_workspace_id"], properties: { destination_workspace_id: { type: "string" } } },
        CsvBulkDestination: { type: "object", additionalProperties: false, required: ["csv_ids", "destination_workspace_id"], properties: { csv_ids: { type: "array", minItems: 1, maxItems: 100, uniqueItems: true, items: { type: "string" } }, destination_workspace_id: { type: "string" } } },
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
  addDocumentationExamples(document);
  return document;
}
