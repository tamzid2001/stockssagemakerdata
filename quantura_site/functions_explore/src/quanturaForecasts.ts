import crypto from "crypto";

export const FORECAST_CATEGORIES = [
  "markets",
  "earnings",
  "corporate",
  "products_technology",
  "politics_policy",
  "economics",
  "sports",
] as const;

export const FORECAST_STATUSES = [
  "draft",
  "pending",
  "resolved_yes",
  "resolved_no",
  "resolved_partial",
  "void",
  "expired",
  "disputed",
] as const;

export const FORECAST_API_SCOPES = [
  "forecasts:read",
  "forecasts:history",
  "forecasts:resolved",
  "forecasts:bulk",
  "forecasts:admin",
] as const;

export type ForecastCategory = (typeof FORECAST_CATEGORIES)[number];
export type ForecastStatus = (typeof FORECAST_STATUSES)[number];
export type ForecastApiScope = (typeof FORECAST_API_SCOPES)[number];
export type BinaryOutcome = "yes" | "no";

export interface ForecastEvidence {
  source: string;
  source_id?: string;
  url?: string;
  title?: string;
  description?: string;
  observed_at: string;
  published_at?: string;
  data_class?: "structured" | "primary_source" | "approved_context" | "model_output";
}

export interface ForecastDraftInput {
  slug: string;
  category: ForecastCategory;
  subcategory?: string;
  entity_type: string;
  entity_id: string;
  entity_name: string;
  ticker?: string;
  league?: string;
  team?: string;
  politician?: string;
  organization?: string;
  question: string;
  possible_future_headline: string;
  short_summary: string;
  probability: number;
  bull_case: string;
  base_case: string;
  bear_case: string;
  input_cutoff_at: string;
  resolution_deadline: string;
  model_provider: string;
  model_name: string;
  model_version: string;
  forecast_method: string;
  reasoning_summary: string;
  structured_reasoning?: Record<string, unknown>;
  evidence: ForecastEvidence[];
  source_metadata?: Record<string, unknown>;
  resolution_rule: string;
  resolution_source: string;
  created_by: string;
  review_status?: "needs_review" | "approved" | "rejected";
  private_strategy_json?: Record<string, unknown>;
}

export interface ProbabilityRevisionInput {
  probability: number;
  reasoning_delta: string;
  input_cutoff_at: string;
  model_provider: string;
  model_name: string;
  model_version: string;
  evidence: ForecastEvidence[];
}

export interface ForecastAmendmentInput {
  field: string;
  reason: string;
  corrected_display_value?: string;
  note: string;
}

export interface CalibrationRow {
  bucket: string;
  lower: number;
  upper: number;
  forecast_count: number;
  predicted_average_probability: number | null;
  actual_event_frequency: number | null;
  average_brier_score: number | null;
}

const STATUS_TRANSITIONS: Record<ForecastStatus, ReadonlySet<ForecastStatus>> = {
  draft: new Set(["pending", "void"]),
  pending: new Set(["resolved_yes", "resolved_no", "resolved_partial", "void", "expired", "disputed"]),
  resolved_yes: new Set(["disputed"]),
  resolved_no: new Set(["disputed"]),
  resolved_partial: new Set(["disputed"]),
  void: new Set(),
  expired: new Set(["void", "disputed"]),
  disputed: new Set(["resolved_yes", "resolved_no", "resolved_partial", "void"]),
};

const POLITICAL_SAFETY_PATTERNS: RegExp[] = [
  /\b(secret(ly)?|private)\b.{0,80}\b(crime|illegal act|sexual behavior|affair)\b/i,
  /\b(murder|suicide|assassinat|death|die|terminal illness)\b/i,
  /\b(medical diagnosis|has cancer|has dementia|mental illness)\b/i,
  /\bsexual (conduct|behavior|relationship)\b/i,
];

function text(value: unknown, max = 5000): string {
  return String(value ?? "").trim().slice(0, max);
}

function iso(value: unknown, field: string): string {
  const raw = text(value, 80);
  const timestamp = Date.parse(raw);
  if (!raw || !Number.isFinite(timestamp)) throw new Error(`${field}_invalid`);
  return new Date(timestamp).toISOString();
}

export function validateProbability(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) throw new Error("probability_must_be_between_zero_and_one");
  return parsed;
}

export function confidenceLabel(probability: number): "low" | "medium" | "high" | "very_high" {
  const directionalConfidence = Math.max(probability, 1 - probability);
  if (directionalConfidence >= 0.9) return "very_high";
  if (directionalConfidence >= 0.75) return "high";
  if (directionalConfidence >= 0.6) return "medium";
  return "low";
}

export function normalizeSlug(value: unknown): string {
  const slug = text(value, 180)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (slug.length < 6) throw new Error("forecast_slug_invalid");
  return slug;
}

export function buildForecastSearchTokens(values: unknown[]): string[] {
  const tokens = values
    .flatMap((value) => text(value, 4000).toLowerCase().replace(/[^a-z0-9.-]+/g, " ").split(/\s+/))
    .map((value) => value.trim())
    .filter((value) => value.length >= 2 && value.length <= 80);
  return [...new Set(tokens)].slice(0, 100);
}

export function validateEvidenceCutoff(evidence: ForecastEvidence[], inputCutoffAt: string): ForecastEvidence[] {
  const cutoff = Date.parse(iso(inputCutoffAt, "input_cutoff_at"));
  if (!Array.isArray(evidence) || evidence.length === 0) throw new Error("forecast_evidence_required");
  return evidence.slice(0, 100).map((item, index) => {
    const source = text(item?.source, 160);
    if (!source) throw new Error(`evidence_source_required:${index}`);
    const observedAt = iso(item?.observed_at, `evidence_observed_at:${index}`);
    const publishedAt = item?.published_at ? iso(item.published_at, `evidence_published_at:${index}`) : undefined;
    if (Date.parse(observedAt) > cutoff || (publishedAt && Date.parse(publishedAt) > cutoff)) {
      throw new Error(`evidence_after_input_cutoff:${index}`);
    }
    let url = text(item?.url, 1000);
    if (url) {
      const parsed = new URL(url);
      if (!/^https?:$/.test(parsed.protocol)) throw new Error(`evidence_url_invalid:${index}`);
      parsed.username = "";
      parsed.password = "";
      url = parsed.toString();
    }
    const normalized: ForecastEvidence = {
      source,
      observed_at: observedAt,
      data_class: item?.data_class || "structured",
    };
    const sourceId = text(item?.source_id, 220);
    const title = text(item?.title, 300);
    const description = text(item?.description, 600);
    if (sourceId) normalized.source_id = sourceId;
    if (url) normalized.url = url;
    if (title) normalized.title = title;
    if (description) normalized.description = description;
    if (publishedAt) normalized.published_at = publishedAt;
    return normalized;
  });
}

export function validatePoliticsSafety(input: Pick<ForecastDraftInput, "category" | "question" | "possible_future_headline" | "short_summary">): void {
  if (input.category !== "politics_policy") return;
  const combined = `${input.question} ${input.possible_future_headline} ${input.short_summary}`;
  if (POLITICAL_SAFETY_PATTERNS.some((pattern) => pattern.test(combined))) {
    throw new Error("politics_forecast_disallowed_claim");
  }
}

export function normalizeForecastDraft(input: ForecastDraftInput, now = new Date()): Record<string, unknown> {
  if (!FORECAST_CATEGORIES.includes(input.category)) throw new Error("forecast_category_invalid");
  const probability = validateProbability(input.probability);
  const inputCutoffAt = iso(input.input_cutoff_at, "input_cutoff_at");
  const resolutionDeadline = iso(input.resolution_deadline, "resolution_deadline");
  if (Date.parse(resolutionDeadline) <= Date.parse(inputCutoffAt)) throw new Error("resolution_deadline_must_follow_input_cutoff");
  if (Date.parse(inputCutoffAt) > now.getTime() + 60_000) throw new Error("input_cutoff_cannot_be_in_future");
  const question = text(input.question, 1000);
  if (question.length < 20 || !question.endsWith("?")) throw new Error("formal_forecast_question_required");
  const resolutionRule = text(input.resolution_rule, 3000);
  const resolutionSource = text(input.resolution_source, 1000);
  if (resolutionRule.length < 30 || resolutionSource.length < 3) throw new Error("objective_resolution_rule_required");
  const possibleHeadline = text(input.possible_future_headline, 400);
  if (possibleHeadline.length < 12) throw new Error("possible_future_headline_required");
  validatePoliticsSafety({ ...input, question, possible_future_headline: possibleHeadline });
  const evidence = validateEvidenceCutoff(input.evidence, inputCutoffAt);
  const createdAt = now.toISOString();
  const entityName = text(input.entity_name, 240);
  const ticker = text(input.ticker, 16).toUpperCase();
  const subcategory = text(input.subcategory, 120);
  const league = text(input.league, 80);
  const team = text(input.team, 160);
  return {
    schema_version: "quantura_forecast_v1",
    slug: normalizeSlug(input.slug),
    category: input.category,
    subcategory,
    entity_type: text(input.entity_type, 80),
    entity_id: text(input.entity_id, 220),
    entity_name: entityName,
    ticker,
    league,
    team,
    politician: text(input.politician, 200),
    organization: text(input.organization, 240),
    question,
    possible_future_headline: possibleHeadline,
    short_summary: text(input.short_summary, 700),
    search_tokens: buildForecastSearchTokens([
      input.category,
      subcategory,
      entityName,
      input.entity_id,
      ticker,
      league,
      team,
      question,
      possibleHeadline,
    ]),
    current_probability: probability,
    initial_probability: null,
    confidence_label: confidenceLabel(probability),
    bull_case: text(input.bull_case, 1200),
    base_case: text(input.base_case, 1200),
    bear_case: text(input.bear_case, 1200),
    created_at: createdAt,
    updated_at: createdAt,
    published_at: null,
    input_cutoff_at: inputCutoffAt,
    resolution_deadline: resolutionDeadline,
    forecast_horizon_seconds: Math.floor((Date.parse(resolutionDeadline) - Date.parse(inputCutoffAt)) / 1000),
    status: "draft",
    model_provider: text(input.model_provider, 120),
    model_name: text(input.model_name, 160),
    model_version: text(input.model_version, 160),
    forecast_method: text(input.forecast_method, 300),
    reasoning_summary: text(input.reasoning_summary, 2500),
    structured_reasoning: input.structured_reasoning || {},
    evidence_json: evidence,
    source_metadata_json: input.source_metadata || {},
    resolution_rule: resolutionRule,
    resolution_source: resolutionSource,
    resolution_notes: "",
    resolved_at: null,
    actual_outcome: null,
    brier_score: null,
    log_score: null,
    calibration_bucket: calibrationBucket(probability),
    previous_probability: null,
    probability_delta: 0,
    current_revision: 0,
    created_by: text(input.created_by, 220),
    review_status: input.review_status || "needs_review",
    is_public: false,
    immutable_published_snapshot: null,
    initial_snapshot_hash: null,
    private_strategy_json: input.private_strategy_json || {},
    view_count: 0,
  };
}

export function assertStatusTransition(from: ForecastStatus, to: ForecastStatus): void {
  if (!FORECAST_STATUSES.includes(from) || !FORECAST_STATUSES.includes(to) || !STATUS_TRANSITIONS[from].has(to)) {
    throw new Error(`invalid_forecast_status_transition:${from}:${to}`);
  }
}

export function calibrationBucket(probability: number): string {
  const value = validateProbability(probability);
  const lower = Math.min(90, Math.floor(value * 10) * 10);
  const upper = lower + 10;
  return `${lower}-${upper}`;
}

export function brierScore(probability: number, outcome: BinaryOutcome): number {
  const p = validateProbability(probability);
  return (p - (outcome === "yes" ? 1 : 0)) ** 2;
}

export function logScore(probability: number, outcome: BinaryOutcome): number {
  const p = Math.min(1 - 1e-15, Math.max(1e-15, validateProbability(probability)));
  const y = outcome === "yes" ? 1 : 0;
  return -(y * Math.log(p) + (1 - y) * Math.log(1 - p));
}

export function immutableSnapshotHash(value: Record<string, unknown>): string {
  const canonicalize = (input: unknown): string => {
    if (Array.isArray(input)) return `[${input.map(canonicalize).join(",")}]`;
    if (input && typeof input === "object") {
      const record = input as Record<string, unknown>;
      return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${canonicalize(record[key])}`).join(",")}}`;
    }
    return JSON.stringify(input);
  };
  const canonical = canonicalize(value);
  return crypto.createHash("sha256").update(canonical).digest("hex");
}

export function buildPublishedSnapshot(forecastId: string, value: Record<string, unknown>, publishedAt: string): Record<string, unknown> {
  if (value.status !== "draft") throw new Error("only_draft_forecasts_can_be_published");
  if (value.review_status !== "approved") throw new Error("forecast_review_approval_required");
  const probability = validateProbability(value.current_probability);
  const snapshot = {
    forecast_id: forecastId,
    slug: value.slug,
    question: value.question,
    possible_future_headline: value.possible_future_headline,
    probability,
    input_cutoff_at: value.input_cutoff_at,
    published_at: iso(publishedAt, "published_at"),
    resolution_deadline: value.resolution_deadline,
    resolution_rule: value.resolution_rule,
    resolution_source: value.resolution_source,
    model_provider: value.model_provider,
    model_name: value.model_name,
    model_version: value.model_version,
    evidence_json: value.evidence_json,
  };
  return { ...snapshot, snapshot_hash: immutableSnapshotHash(snapshot) };
}

export function normalizeProbabilityRevision(input: ProbabilityRevisionInput, current: Record<string, unknown>, now = new Date()): Record<string, unknown> {
  if (current.status !== "pending") throw new Error("only_pending_forecasts_accept_probability_revisions");
  const probability = validateProbability(input.probability);
  const previous = validateProbability(current.current_probability);
  const inputCutoffAt = iso(input.input_cutoff_at, "input_cutoff_at");
  if (Date.parse(inputCutoffAt) < Date.parse(String(current.input_cutoff_at))) throw new Error("revision_cutoff_cannot_move_backwards");
  if (Date.parse(inputCutoffAt) > now.getTime() + 60_000) throw new Error("input_cutoff_cannot_be_in_future");
  const evidence = validateEvidenceCutoff(input.evidence, inputCutoffAt);
  const revision = Number(current.current_revision || 0) + 1;
  return {
    revision,
    probability,
    previous_probability: previous,
    probability_delta: probability - previous,
    reasoning_delta: text(input.reasoning_delta, 2000),
    created_at: now.toISOString(),
    input_cutoff_at: inputCutoffAt,
    model_provider: text(input.model_provider, 120),
    model_name: text(input.model_name, 160),
    model_version: text(input.model_version, 160),
    evidence_json: evidence,
    immutable: true,
  };
}

export function normalizeForecastAmendment(
  input: ForecastAmendmentInput,
  current: Record<string, unknown>,
  createdBy: string,
  now = new Date()
): Record<string, unknown> {
  if (current.status === "draft") throw new Error("draft_forecasts_should_be_corrected_before_publication");
  const field = text(input.field, 120);
  const protectedFields = new Set([
    "probability",
    "current_probability",
    "initial_probability",
    "question",
    "resolution_rule",
    "resolution_deadline",
    "input_cutoff_at",
    "status",
  ]);
  if (!field || protectedFields.has(field)) throw new Error("amendment_cannot_rewrite_forecast_history");
  const reason = text(input.reason, 1000);
  const note = text(input.note, 3000);
  if (reason.length < 10 || note.length < 10) throw new Error("amendment_reason_and_note_required");
  return {
    field,
    reason,
    corrected_display_value: text(input.corrected_display_value, 1000) || null,
    note,
    created_at: now.toISOString(),
    created_by: text(createdBy, 220),
    immutable: true,
  };
}

export function publicForecastProjection(forecastId: string, value: Record<string, unknown>): Record<string, unknown> {
  const status = String(value.status || "draft");
  const unresolved = status === "pending";
  return {
    forecast_id: forecastId,
    slug: value.slug,
    category: value.category,
    subcategory: value.subcategory,
    entity: {
      type: value.entity_type,
      id: value.entity_id,
      name: value.entity_name,
      ticker: value.ticker || null,
      league: value.league || null,
      team: value.team || null,
      organization: value.organization || null,
    },
    question: value.question,
    possible_future_headline: value.possible_future_headline,
    short_summary: value.short_summary,
    probability: value.current_probability,
    initial_probability: value.initial_probability,
    confidence_label: value.confidence_label,
    probability_delta: value.probability_delta || 0,
    bull_case: value.bull_case,
    base_case: value.base_case,
    bear_case: value.bear_case,
    created_at: value.created_at,
    published_at: value.published_at,
    input_cutoff_at: value.input_cutoff_at,
    resolution_deadline: value.resolution_deadline,
    forecast_horizon_seconds: value.forecast_horizon_seconds,
    status,
    forecast_label: "QUANTURA FORECAST",
    event_has_not_occurred: unresolved,
    disclosure: unresolved ? "THIS EVENT HAS NOT OCCURRED" : "This forecast has been resolved or closed.",
    model: {
      provider: value.model_provider,
      name: value.model_name,
      version: value.model_version,
      method: value.forecast_method,
    },
    reasoning_summary: value.reasoning_summary,
    evidence: value.evidence_json || [],
    resolution: {
      rule: value.resolution_rule,
      source: value.resolution_source,
      notes: value.resolution_notes || null,
      actual_outcome: value.actual_outcome || null,
      resolved_at: value.resolved_at || null,
      brier_score: value.brier_score ?? null,
      log_score: value.log_score ?? null,
    },
  };
}

export function enterpriseForecastProjection(forecastId: string, value: Record<string, unknown>): Record<string, unknown> {
  return {
    ...publicForecastProjection(forecastId, value),
    schema_version: value.schema_version,
    previous_probability: value.previous_probability,
    current_revision: value.current_revision,
    structured_reasoning: value.structured_reasoning || {},
    source_metadata: value.source_metadata_json || {},
    calibration_bucket: value.calibration_bucket,
    review_status: value.review_status,
    provenance: {
      input_cutoff_at: value.input_cutoff_at,
      model_provider: value.model_provider,
      model_name: value.model_name,
      model_version: value.model_version,
      initial_snapshot_hash: value.initial_snapshot_hash,
    },
  };
}

export function buildCalibrationRows(records: Array<Record<string, unknown>>): CalibrationRow[] {
  return Array.from({ length: 10 }, (_, index) => {
    const lower = index / 10;
    const upper = (index + 1) / 10;
    const rows = records.filter((item) => {
      const probability = Number(item.scored_probability ?? item.current_probability);
      return Number.isFinite(probability) && probability >= lower && (index === 9 ? probability <= upper : probability < upper);
    });
    const outcomes = rows
      .map((item): 0 | 1 | null => (item.status === "resolved_yes" ? 1 : item.status === "resolved_no" ? 0 : null))
      .filter((item): item is 0 | 1 => item !== null);
    const probabilities = rows.map((item) => Number(item.scored_probability ?? item.current_probability)).filter(Number.isFinite);
    const brier = rows.map((item) => Number(item.brier_score)).filter(Number.isFinite);
    return {
      bucket: `${Math.round(lower * 100)}-${Math.round(upper * 100)}%`,
      lower,
      upper,
      forecast_count: rows.length,
      predicted_average_probability: probabilities.length ? probabilities.reduce((a, b) => a + b, 0) / probabilities.length : null,
      actual_event_frequency: outcomes.length ? outcomes.reduce<number>((a, b) => a + b, 0) / outcomes.length : null,
      average_brier_score: brier.length ? brier.reduce((a, b) => a + b, 0) / brier.length : null,
    };
  });
}

export function encodeCursor(createdAt: string, id: string): string {
  return Buffer.from(JSON.stringify({ created_at: iso(createdAt, "cursor_created_at"), id: text(id, 220) }), "utf8").toString("base64url");
}

export function decodeCursor(cursor: unknown): { created_at: string; id: string } | null {
  const raw = text(cursor, 1000);
  if (!raw) return null;
  try {
    const parsed = JSON.parse(Buffer.from(raw, "base64url").toString("utf8"));
    const id = text(parsed?.id, 220);
    if (!id) throw new Error("invalid");
    return { created_at: iso(parsed?.created_at, "cursor_created_at"), id };
  } catch {
    throw new Error("cursor_invalid");
  }
}

export function createForecastApiKey(): { rawKey: string; prefix: string } {
  const secret = crypto.randomBytes(32).toString("base64url");
  const rawKey = `qf_live_${secret}`;
  return { rawKey, prefix: rawKey.slice(0, 16) };
}

export function hashForecastApiKey(rawKey: string, pepper: string): string {
  const clean = text(rawKey, 500);
  if (!/^qf_(live|test)_[A-Za-z0-9_-]{30,}$/.test(clean)) throw new Error("api_key_invalid");
  if (text(pepper, 1000).length < 16) throw new Error("api_key_pepper_not_configured");
  return crypto.createHmac("sha256", pepper).update(clean).digest("hex");
}

export function validateScopes(scopes: unknown): ForecastApiScope[] {
  if (!Array.isArray(scopes) || !scopes.length) throw new Error("api_key_scopes_required");
  const unique = [...new Set(scopes.map((item) => text(item, 80) as ForecastApiScope))];
  if (unique.some((scope) => !FORECAST_API_SCOPES.includes(scope))) throw new Error("api_key_scope_invalid");
  return unique;
}

export function hasRequiredScope(granted: readonly string[], required: ForecastApiScope): boolean {
  return granted.includes("forecasts:admin") || granted.includes(required);
}

export function validateApiKeyRecord(
  record: Record<string, unknown>,
  requiredScope?: ForecastApiScope,
  now = new Date()
): ForecastApiScope[] {
  if (record.revoked_at) throw new Error("api_key_revoked");
  if (record.expires_at && Date.parse(String(record.expires_at)) <= now.getTime()) throw new Error("api_key_expired");
  const scopes = validateScopes(record.scopes);
  if (requiredScope && !hasRequiredScope(scopes, requiredScope)) throw new Error("insufficient_scope");
  return scopes;
}

export function sanitizeDatasetRecord(forecastId: string, value: Record<string, unknown>, history: Record<string, unknown>[]): Record<string, unknown> {
  const enterprise = enterpriseForecastProjection(forecastId, value);
  return {
    forecast_id: forecastId,
    schema_version: enterprise.schema_version,
    created_at: enterprise.created_at,
    input_cutoff_at: enterprise.input_cutoff_at,
    category: enterprise.category,
    entity: enterprise.entity,
    question: enterprise.question,
    possible_future_headline: enterprise.possible_future_headline,
    probability: enterprise.probability,
    initial_probability: enterprise.initial_probability,
    reasoning_summary: enterprise.reasoning_summary,
    evidence: enterprise.evidence,
    probability_history: history.map((item) => ({
      revision: item.revision,
      probability: item.probability,
      previous_probability: item.previous_probability,
      probability_delta: item.probability_delta,
      reasoning_delta: item.reasoning_delta,
      created_at: item.created_at,
      input_cutoff_at: item.input_cutoff_at,
      model_version: item.model_version,
    })),
    resolution_deadline: enterprise.resolution_deadline,
    status: enterprise.status,
    actual_outcome: (enterprise.resolution as Record<string, unknown>).actual_outcome,
    resolved_at: (enterprise.resolution as Record<string, unknown>).resolved_at,
    brier_score: (enterprise.resolution as Record<string, unknown>).brier_score,
    log_score: (enterprise.resolution as Record<string, unknown>).log_score,
  };
}
