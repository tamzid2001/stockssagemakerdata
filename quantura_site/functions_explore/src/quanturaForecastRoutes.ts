import crypto from "crypto";
import { Router, Request, Response } from "express";
import admin from "firebase-admin";

import {
  FORECAST_API_SCOPES,
  FORECAST_CATEGORIES,
  FORECAST_STATUSES,
  ForecastApiScope,
  ForecastStatus,
  assertStatusTransition,
  brierScore,
  buildCalibrationRows,
  buildForecastSearchTokens,
  buildPublishedSnapshot,
  createForecastApiKey,
  decodeCursor,
  encodeCursor,
  enterpriseForecastProjection,
  hashForecastApiKey,
  hasRequiredScope,
  logScore,
  normalizeForecastAmendment,
  normalizeForecastDraft,
  normalizeProbabilityRevision,
  publicForecastProjection,
  sanitizeDatasetRecord,
  validateProbability,
  validateApiKeyRecord,
  validateScopes,
} from "./quanturaForecasts";

const FORECASTS = "quantura_forecasts";
const API_KEYS = "quantura_forecast_api_keys";
const API_USAGE = "quantura_forecast_api_usage";
const RATE_WINDOWS = "quantura_forecast_rate_windows";
const DATASET_RELEASES = "quantura_forecast_dataset_releases";
const JOB_RUNS = "quantura_forecast_job_runs";
const AGGREGATES = "quantura_forecast_aggregates";
const SLUGS = "quantura_forecast_slugs";
const AMENDMENTS = "amendments";
const API_VERSION = "v1";
const SCHEMA_VERSION = "quantura_forecast_v1";
const DATASET_SCHEMA_VERSION = "forecast_trajectories_v1";
const MAX_BULK_RECORDS = 20_000;

type JsonRecord = Record<string, unknown>;

type ApiPrincipal = {
  keyId: string;
  customerId: string;
  scopes: ForecastApiScope[];
  tier: string;
  rateLimitPerMinute: number;
};

type RouteOptions = {
  db: FirebaseFirestore.Firestore;
  auth: admin.auth.Auth;
  adminEmails: string[];
  publicOrigin: string;
};

type EnterpriseHandler = (
  req: Request,
  res: Response,
  principal: ApiPrincipal,
  requestId: string
) => Promise<number | void>;

function text(value: unknown, max = 1000): string {
  return String(value ?? "").trim().slice(0, max);
}

function plain(value: unknown): JsonRecord {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {};
}

function finite(value: unknown): number | null {
  if (value === "" || value === null || value === undefined) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseIsoFilter(value: unknown, field: string): string | null {
  const raw = text(value, 80);
  if (!raw) return null;
  const parsed = Date.parse(raw);
  if (!Number.isFinite(parsed)) throw new Error(`${field}_invalid`);
  return new Date(parsed).toISOString();
}

function limitFrom(value: unknown, maximum = 500): number {
  const parsed = Math.floor(Number(value || 50));
  if (!Number.isFinite(parsed) || parsed < 1) throw new Error("limit_invalid");
  return Math.min(parsed, maximum);
}

function errorStatus(error: unknown): number {
  const message = text((error as any)?.message || error, 300);
  if (/unauthenticated|api_key_(missing|invalid|revoked|expired)/.test(message)) return 401;
  if (/insufficient_scope/.test(message)) return 403;
  if (/not_found/.test(message)) return 404;
  if (/already_exists|immutable|transition|only_draft|only_pending/.test(message)) return 409;
  if (/rate_limit/.test(message)) return 429;
  if (/not_configured/.test(message)) return 503;
  return 400;
}

function errorCode(error: unknown): string {
  return text((error as any)?.message || "request_failed", 120)
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "REQUEST_FAILED";
}

function sendError(res: Response, error: unknown, requestId: string, status = errorStatus(error)): void {
  const code = errorCode(error);
  const safeMessages: Record<string, string> = {
    API_KEY_MISSING: "An API key is required.",
    API_KEY_INVALID: "The API key is invalid.",
    API_KEY_REVOKED: "The API key has been revoked.",
    API_KEY_EXPIRED: "The API key has expired.",
    INSUFFICIENT_SCOPE: "The API key does not have the required scope.",
    RATE_LIMITED: "Rate limit exceeded.",
    FORECAST_NOT_FOUND: "Forecast not found.",
    CURSOR_INVALID: "The pagination cursor is invalid.",
  };
  res.status(status).json({
    error: {
      code,
      message: safeMessages[code] || (status < 500 ? "The request is invalid." : "The service is temporarily unavailable."),
      request_id: requestId,
    },
  });
}

function setApiHeaders(res: Response, requestId: string, limit?: number, remaining?: number, resetAt?: string): void {
  res.setHeader("X-Request-ID", requestId);
  res.setHeader("X-Quantura-API-Version", API_VERSION);
  res.setHeader("Cache-Control", "private, no-store");
  if (limit !== undefined) res.setHeader("X-RateLimit-Limit", String(limit));
  if (remaining !== undefined) res.setHeader("X-RateLimit-Remaining", String(Math.max(0, remaining)));
  if (resetAt) res.setHeader("X-RateLimit-Reset", resetAt);
}

function extractApiKey(req: Request): string {
  const xApiKey = text(req.headers["x-api-key"], 500);
  if (xApiKey) return xApiKey;
  const authorization = text(req.headers.authorization, 700);
  return authorization.match(/^Bearer\s+(.+)$/i)?.[1]?.trim() || "";
}

async function authenticateApiKey(req: Request, options: RouteOptions): Promise<ApiPrincipal> {
  const rawKey = extractApiKey(req);
  if (!rawKey) throw new Error("api_key_missing");
  const pepper = text(process.env.QUANTURA_FORECAST_API_KEY_PEPPER, 2000);
  const keyId = hashForecastApiKey(rawKey, pepper);
  const snapshot = await options.db.collection(API_KEYS).doc(keyId).get();
  if (!snapshot.exists) throw new Error("api_key_invalid");
  const value = plain(snapshot.data());
  const scopes = validateApiKeyRecord(value);
  return {
    keyId,
    customerId: text(value.customer_id, 220),
    scopes,
    tier: text(value.tier, 80) || "developer",
    rateLimitPerMinute: Math.min(Math.max(Math.floor(Number(value.rate_limit_per_minute) || 60), 1), 10_000),
  };
}

async function enforceRateLimit(options: RouteOptions, principal: ApiPrincipal): Promise<{ remaining: number; resetAt: string }> {
  const now = Date.now();
  const minuteStart = Math.floor(now / 60_000) * 60_000;
  const resetAt = new Date(minuteStart + 60_000).toISOString();
  const windowId = `${principal.keyId}_${minuteStart}`;
  const ref = options.db.collection(RATE_WINDOWS).doc(windowId);
  const count = await options.db.runTransaction(async (transaction) => {
    const snapshot = await transaction.get(ref);
    const current = Number(snapshot.data()?.count || 0);
    if (current >= principal.rateLimitPerMinute) throw new Error("rate_limited");
    const next = current + 1;
    transaction.set(ref, {
      key_id: principal.keyId,
      customer_id: principal.customerId,
      minute_start: new Date(minuteStart).toISOString(),
      expires_at: new Date(minuteStart + 2 * 60_000),
      count: next,
    }, { merge: true });
    return next;
  });
  return { remaining: principal.rateLimitPerMinute - count, resetAt };
}

async function writeAudit(
  options: RouteOptions,
  input: {
    principal?: ApiPrincipal;
    endpoint: string;
    method: string;
    status: number;
    records: number;
    requestId: string;
    latencyMs: number;
    authFailure?: boolean;
  }
): Promise<void> {
  await options.db.collection(API_USAGE).doc(input.requestId).set({
    request_id: input.requestId,
    api_key_id: input.principal?.keyId || null,
    customer_id: input.principal?.customerId || null,
    endpoint: input.endpoint,
    method: input.method,
    timestamp: new Date().toISOString(),
    response_status: input.status,
    records_returned: input.records,
    latency_ms: input.latencyMs,
    failed_authentication: Boolean(input.authFailure),
  }, { merge: false });
}

function enterprise(options: RouteOptions, scope: ForecastApiScope, handler: EnterpriseHandler) {
  return async (req: Request, res: Response) => {
    const started = Date.now();
    const requestId = crypto.randomUUID();
    let principal: ApiPrincipal | undefined;
    let records = 0;
    try {
      principal = await authenticateApiKey(req, options);
      if (!hasRequiredScope(principal.scopes, scope)) throw new Error("insufficient_scope");
      const rate = await enforceRateLimit(options, principal);
      setApiHeaders(res, requestId, principal.rateLimitPerMinute, rate.remaining, rate.resetAt);
      records = Number(await handler(req, res, principal, requestId)) || 0;
    } catch (error) {
      if (!res.headersSent) {
        setApiHeaders(res, requestId);
        sendError(res, error, requestId);
      }
    } finally {
      await writeAudit(options, {
        principal,
        endpoint: req.path,
        method: req.method,
        status: res.statusCode,
        records,
        requestId,
        latencyMs: Date.now() - started,
        authFailure: !principal,
      }).catch((error) => console.error("[QuanturaForecasts] audit write failed", { requestId, error: (error as Error).message }));
    }
  };
}

async function requireAdmin(req: Request, options: RouteOptions): Promise<admin.auth.DecodedIdToken> {
  const authorization = text(req.headers.authorization, 700);
  const token = authorization.match(/^Bearer\s+(.+)$/i)?.[1]?.trim();
  if (!token) throw new Error("unauthenticated");
  const decoded = await options.auth.verifyIdToken(token).catch(() => null);
  if (!decoded) throw new Error("unauthenticated");
  const isAdmin = decoded.admin === true || options.adminEmails.includes(text(decoded.email, 320).toLowerCase());
  if (!isAdmin) throw new Error("insufficient_scope");
  return decoded;
}

async function findForecast(options: RouteOptions, idOrSlug: string): Promise<{ id: string; value: JsonRecord } | null> {
  const clean = text(idOrSlug, 220);
  if (!clean) return null;
  const direct = await options.db.collection(FORECASTS).doc(clean).get();
  if (direct.exists) return { id: direct.id, value: plain(direct.data()) };
  const slug = await options.db.collection(SLUGS).doc(clean).get();
  const forecastId = text(slug.data()?.forecast_id, 220);
  if (!forecastId) return null;
  const forecast = await options.db.collection(FORECASTS).doc(forecastId).get();
  return forecast.exists ? { id: forecast.id, value: plain(forecast.data()) } : null;
}

function matchesFilters(value: JsonRecord, query: JsonRecord): boolean {
  const category = text(query.category, 80);
  const subcategory = text(query.subcategory, 120);
  const entity = text(query.entity, 220).toLowerCase();
  const ticker = text(query.ticker, 16).toUpperCase();
  const status = text(query.status, 80);
  const model = text(query.model, 160).toLowerCase();
  if (category && value.category !== category) return false;
  if (subcategory && value.subcategory !== subcategory) return false;
  if (entity && ![value.entity_id, value.entity_name].some((item) => text(item, 240).toLowerCase() === entity)) return false;
  if (ticker && value.ticker !== ticker) return false;
  if (status && value.status !== status) return false;
  if (model && text(value.model_name, 160).toLowerCase() !== model) return false;
  const probability = Number(value.current_probability);
  const minProbability = finite(query.min_probability);
  const maxProbability = finite(query.max_probability);
  if (minProbability !== null && probability < validateProbability(minProbability)) return false;
  if (maxProbability !== null && probability > validateProbability(maxProbability)) return false;
  const dateFilters: Array<[string, unknown, (left: number, right: number) => boolean]> = [
    ["created_at", query.created_after, (left, right) => left >= right],
    ["created_at", query.created_before, (left, right) => left <= right],
    ["resolution_deadline", query.resolves_after, (left, right) => left >= right],
    ["resolution_deadline", query.resolves_before, (left, right) => left <= right],
  ];
  for (const [field, raw, predicate] of dateFilters) {
    const filter = parseIsoFilter(raw, field);
    if (filter && !predicate(Date.parse(String(value[field])), Date.parse(filter))) return false;
  }
  return true;
}

function validateForecastQuery(query: JsonRecord): void {
  const category = text(query.category, 80);
  const status = text(query.status, 80);
  if (category && !FORECAST_CATEGORIES.includes(category as any)) throw new Error("category_invalid");
  if (status && !FORECAST_STATUSES.includes(status as any)) throw new Error("status_invalid");
  const minimum = finite(query.min_probability);
  const maximum = finite(query.max_probability);
  if (minimum !== null) validateProbability(minimum);
  if (maximum !== null) validateProbability(maximum);
  if (minimum !== null && maximum !== null && minimum > maximum) throw new Error("probability_range_invalid");
  parseIsoFilter(query.created_after, "created_after");
  parseIsoFilter(query.created_before, "created_before");
  parseIsoFilter(query.resolves_after, "resolves_after");
  parseIsoFilter(query.resolves_before, "resolves_before");
  limitFrom(query.limit);
  decodeCursor(query.cursor);
}

async function listForecasts(options: RouteOptions, queryInput: JsonRecord, includePrivate = false): Promise<{ rows: Array<{ id: string; value: JsonRecord }>; nextCursor: string | null }> {
  validateForecastQuery(queryInput);
  const limit = limitFrom(queryInput.limit);
  let cursor = decodeCursor(queryInput.cursor);
  const pageLimit = Math.min(Math.max(limit * 3, 100), 500);
  const maxScan = 2000;
  let query: FirebaseFirestore.Query = options.db.collection(FORECASTS);
  if (!includePrivate) query = query.where("is_public", "==", true);
  const category = text(queryInput.category, 80);
  const status = text(queryInput.status, 80);
  const ticker = text(queryInput.ticker, 16).toUpperCase();
  const search = safeSearchToken(queryInput.search);
  if (search) {
    query = query.where("search_tokens", "array-contains", search);
  } else if (category) {
    query = query.where("category", "==", category);
  } else if (status) {
    query = query.where("status", "==", status);
  } else if (ticker) query = query.where("ticker", "==", ticker);
  const orderedQuery = query.orderBy("created_at", "desc").orderBy(admin.firestore.FieldPath.documentId(), "desc");
  const rows: Array<{ id: string; value: JsonRecord }> = [];
  let lastProcessed: { id: string; createdAt: string } | null = null;
  let scanned = 0;
  let hasMore = false;
  while (rows.length < limit && scanned < maxScan) {
    let pageQuery = orderedQuery;
    if (cursor) pageQuery = pageQuery.startAfter(cursor.created_at, cursor.id);
    const batchLimit = Math.min(pageLimit, maxScan - scanned);
    const snapshot = await pageQuery.limit(batchLimit).get();
    if (snapshot.empty) break;
    hasMore = snapshot.size === batchLimit;
    for (let index = 0; index < snapshot.docs.length; index += 1) {
      const doc = snapshot.docs[index];
      const item = { id: doc.id, value: plain(doc.data()) };
      lastProcessed = { id: item.id, createdAt: String(item.value.created_at) };
      scanned += 1;
      if (matchesFilters(item.value, queryInput)) rows.push(item);
      if (rows.length >= limit || scanned >= maxScan) {
        hasMore = index < snapshot.docs.length - 1 || snapshot.size === batchLimit;
        break;
      }
    }
    if (rows.length >= limit || scanned >= maxScan) break;
    if (snapshot.size < batchLimit || !lastProcessed) break;
    cursor = { created_at: lastProcessed.createdAt, id: lastProcessed.id };
    hasMore = true;
  }
  return {
    rows,
    nextCursor: hasMore && lastProcessed ? encodeCursor(lastProcessed.createdAt, lastProcessed.id) : null,
  };
}

async function historyFor(options: RouteOptions, forecastId: string): Promise<JsonRecord[]> {
  const snapshot = await options.db.collection(FORECASTS).doc(forecastId).collection("probability_history").orderBy("revision", "asc").limit(1000).get();
  return snapshot.docs.map((doc) => ({ id: doc.id, ...plain(doc.data()) }));
}

async function amendmentsFor(options: RouteOptions, forecastId: string): Promise<JsonRecord[]> {
  const snapshot = await options.db.collection(FORECASTS).doc(forecastId).collection(AMENDMENTS).orderBy("created_at", "asc").limit(500).get();
  return snapshot.docs.map((doc) => ({ amendment_id: doc.id, ...plain(doc.data()) }));
}

function publicMeta(count: number, nextCursor: string | null = null): JsonRecord {
  return { api_version: API_VERSION, schema_version: SCHEMA_VERSION, count, next_cursor: nextCursor };
}

function safeSearchToken(value: unknown): string {
  return text(value, 100).toLowerCase().replace(/[^a-z0-9.-]+/g, " ").trim().split(/\s+/)[0] || "";
}

function escapeCsv(value: unknown): string {
  const raw = typeof value === "object" && value !== null ? JSON.stringify(value) : String(value ?? "");
  return /[",\r\n]/.test(raw) ? `"${raw.replace(/"/g, '""')}"` : raw;
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value as JsonRecord).sort().map((key) => `${JSON.stringify(key)}:${stableStringify((value as JsonRecord)[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

export async function createDatasetRelease(options: RouteOptions, input: JsonRecord, actorUid: string): Promise<JsonRecord> {
  const version = text(input.dataset_version, 120);
  if (!/^[a-z0-9][a-z0-9._-]{5,119}$/.test(version)) throw new Error("dataset_version_invalid");
  const releaseRef = options.db.collection(DATASET_RELEASES).doc(version);
  if ((await releaseRef.get()).exists) throw new Error("dataset_version_immutable_already_exists");
  const sourceCutoff = parseIsoFilter(input.source_cutoff, "source_cutoff") || new Date().toISOString();
  const forecastSnapshot = await options.db.collection(FORECASTS)
    .where("is_public", "==", true)
    .where("created_at", "<=", sourceCutoff)
    .orderBy("created_at", "asc")
    .limit(MAX_BULK_RECORDS)
    .get();
  const records: JsonRecord[] = [];
  for (const doc of forecastSnapshot.docs) {
    records.push(sanitizeDatasetRecord(doc.id, plain(doc.data()), await historyFor(options, doc.id)));
  }
  const generatedAt = new Date().toISOString();
  const jsonl = `${records.map((record) => JSON.stringify(record)).join("\n")}\n`;
  const csvColumns = ["forecast_id", "created_at", "input_cutoff_at", "category", "question", "possible_future_headline", "probability", "resolution_deadline", "status", "actual_outcome", "resolved_at", "brier_score"];
  const csv = `${csvColumns.join(",")}\n${records.map((record) => csvColumns.map((column) => escapeCsv(record[column])).join(",")).join("\n")}\n`;
  const jsonlChecksum = crypto.createHash("sha256").update(jsonl).digest("hex");
  const csvChecksum = crypto.createHash("sha256").update(csv).digest("hex");
  const prefix = `quantura-forecast-datasets/${version}`;
  const bucket = admin.storage().bucket();
  await bucket.file(`${prefix}/forecast-trajectories.jsonl`).save(jsonl, {
    resumable: false,
    contentType: "application/x-ndjson",
    metadata: { cacheControl: "private, max-age=0, no-store", metadata: { checksum: jsonlChecksum } },
  });
  await bucket.file(`${prefix}/forecast-trajectories.csv`).save(csv, {
    resumable: false,
    contentType: "text/csv; charset=utf-8",
    metadata: { cacheControl: "private, max-age=0, no-store", metadata: { checksum: csvChecksum } },
  });
  const manifest: JsonRecord = {
    dataset_version: version,
    schema_version: DATASET_SCHEMA_VERSION,
    generated_at: generatedAt,
    source_cutoff: sourceCutoff,
    record_count: records.length,
    checksums: { jsonl: jsonlChecksum, csv: csvChecksum },
    objects: {
      jsonl: `${prefix}/forecast-trajectories.jsonl`,
      csv: `${prefix}/forecast-trajectories.csv`,
    },
    formats: ["jsonl", "csv"],
    licensing_metadata: plain(input.licensing_metadata),
    category_coverage: [...new Set(records.map((record) => String(record.category)))].sort(),
    time_coverage: {
      earliest: records[0]?.created_at || null,
      latest: records.at(-1)?.created_at || null,
    },
    created_by: actorUid,
    immutable: true,
    manifest_checksum: "",
  };
  manifest.manifest_checksum = crypto.createHash("sha256").update(stableStringify(manifest)).digest("hex");
  await releaseRef.create(manifest);
  return manifest;
}

export async function runForecastLifecycleJob(options: RouteOptions, jobName: string, now = new Date()): Promise<JsonRecord> {
  const normalized = text(jobName, 100);
  const supported = ["expire", "resolve", "score", "calibration", "feed", "backfill-search", "all"];
  if (!supported.includes(normalized)) throw new Error("forecast_job_not_found");
  const dateKey = now.toISOString().slice(0, 13);
  const runId = `${normalized}_${dateKey}`;
  const runRef = options.db.collection(JOB_RUNS).doc(runId);
  const existing = await runRef.get();
  if (existing.exists && existing.data()?.status === "completed") return { ...plain(existing.data()), reused: true };
  await runRef.set({ job: normalized, status: "running", started_at: now.toISOString(), idempotency_key: runId }, { merge: true });
  let expired = 0;
  let calibrationRecords = 0;
  let feedRecords = 0;
  let searchRecords = 0;
  if (["expire", "all"].includes(normalized)) {
    const due = await options.db.collection(FORECASTS)
      .where("status", "==", "pending")
      .where("resolution_deadline", "<=", now.toISOString())
      .orderBy("resolution_deadline", "asc")
      .limit(250)
      .get();
    for (const doc of due.docs) {
      await options.db.runTransaction(async (transaction) => {
        const fresh = await transaction.get(doc.ref);
        if (fresh.data()?.status !== "pending") return;
        transaction.update(doc.ref, { status: "expired", updated_at: now.toISOString(), expired_at: now.toISOString() });
        expired += 1;
      });
    }
  }
  if (["calibration", "score", "all"].includes(normalized)) {
    const resolved = await options.db.collection(FORECASTS).where("is_public", "==", true).limit(5000).get();
    const records = resolved.docs.map((doc) => plain(doc.data())).filter((item) => ["resolved_yes", "resolved_no"].includes(String(item.status)));
    const rows = buildCalibrationRows(records);
    calibrationRecords = records.length;
    await options.db.collection(AGGREGATES).doc("calibration_public").set({
      rows,
      resolved_count: records.length,
      average_brier_score: records.length ? records.reduce((sum, item) => sum + Number(item.brier_score || 0), 0) / records.length : null,
      updated_at: now.toISOString(),
    }, { merge: false });
  }
  if (["feed", "all"].includes(normalized)) {
    const feed = await options.db.collection(FORECASTS)
      .where("is_public", "==", true)
      .where("status", "==", "pending")
      .orderBy("updated_at", "desc")
      .limit(100)
      .get();
    feedRecords = feed.size;
    await options.db.collection(AGGREGATES).doc("public_feed").set({
      forecast_ids: feed.docs.map((doc) => doc.id),
      updated_at: now.toISOString(),
    }, { merge: false });
  }
  if (["backfill-search", "all"].includes(normalized)) {
    const forecasts = await options.db.collection(FORECASTS).limit(500).get();
    const writer = options.db.bulkWriter();
    for (const doc of forecasts.docs) {
      const value = plain(doc.data());
      const tokens = buildForecastSearchTokens([
        value.category,
        value.subcategory,
        value.entity_name,
        value.entity_id,
        value.ticker,
        value.league,
        value.team,
        value.question,
        value.possible_future_headline,
      ]);
      writer.update(doc.ref, { search_tokens: tokens });
      searchRecords += 1;
    }
    await writer.close();
  }
  const result = {
    job: normalized,
    status: "completed",
    started_at: existing.data()?.started_at || now.toISOString(),
    completed_at: new Date().toISOString(),
    expired,
    calibration_records: calibrationRecords,
    feed_records: feedRecords,
    search_records: searchRecords,
    resolver_status: ["resolve", "all"].includes(normalized) ? "no_category_resolvers_registered" : "not_requested",
    reused: false,
  };
  await runRef.set(result, { merge: true });
  return result;
}

export function registerQuanturaForecastRoutes(router: Router, options: RouteOptions): void {
  router.get("/forecasts/public/categories", (_req, res) => {
    res.setHeader("Cache-Control", "public, max-age=300, stale-while-revalidate=900");
    res.status(200).json({ data: FORECAST_CATEGORIES, meta: publicMeta(FORECAST_CATEGORIES.length) });
  });

  router.get("/forecasts/public/feed", async (req, res) => {
    try {
      const query = { ...req.query, limit: Math.min(limitFrom(req.query.limit, 100), 100) };
      const result = await listForecasts(options, query, false);
      const search = safeSearchToken(req.query.search);
      const rows = result.rows
        .filter((item) => !search || [item.value.question, item.value.possible_future_headline, item.value.entity_name, item.value.ticker, item.value.league, item.value.team]
          .some((field) => text(field, 1000).toLowerCase().includes(search)))
        .map((item) => publicForecastProjection(item.id, item.value));
      res.setHeader("Cache-Control", "public, max-age=60, stale-while-revalidate=300");
      res.status(200).json({ data: rows, meta: publicMeta(rows.length, result.nextCursor) });
    } catch (error) {
      sendError(res, error, crypto.randomUUID());
    }
  });

  router.get("/forecasts/public/calibration", async (_req, res) => {
    const cached = await options.db.collection(AGGREGATES).doc("calibration_public").get();
    const data = cached.exists ? plain(cached.data()) : { rows: [], resolved_count: 0, average_brier_score: null, updated_at: null };
    const pending = await options.db.collection(FORECASTS).where("is_public", "==", true).where("status", "==", "pending").count().get().catch(() => null);
    res.setHeader("Cache-Control", "public, max-age=120, stale-while-revalidate=300");
    res.status(200).json({ data: { ...data, pending_count: pending?.data().count ?? null }, meta: publicMeta(1) });
  });

  router.get("/forecasts/public/:idOrSlug", async (req, res) => {
    const found = await findForecast(options, req.params.idOrSlug);
    if (!found || found.value.is_public !== true) {
      res.status(404).json({ error: { code: "FORECAST_NOT_FOUND", message: "Forecast not found.", request_id: crypto.randomUUID() } });
      return;
    }
    const [history, amendments] = await Promise.all([historyFor(options, found.id), amendmentsFor(options, found.id)]);
    res.setHeader("Cache-Control", found.value.status === "pending" ? "public, max-age=30" : "public, max-age=300");
    res.status(200).json({
      data: { ...publicForecastProjection(found.id, found.value), probability_history: history, amendments },
      meta: publicMeta(1),
    });
  });

  router.get("/v1/categories", enterprise(options, "forecasts:read", async (_req, res) => {
    res.status(200).json({ data: FORECAST_CATEGORIES, meta: publicMeta(FORECAST_CATEGORIES.length) });
    return FORECAST_CATEGORIES.length;
  }));

  router.get("/v1/forecast-feed", enterprise(options, "forecasts:read", async (req, res) => {
    const result = await listForecasts(options, { ...req.query, status: req.query.status || "pending" }, false);
    const rows = result.rows.map((item) => enterpriseForecastProjection(item.id, item.value));
    res.status(200).json({ data: rows, meta: publicMeta(rows.length, result.nextCursor) });
    return rows.length;
  }));

  router.get("/v1/forecasts/resolved", enterprise(options, "forecasts:resolved", async (req, res) => {
    const limit = limitFrom(req.query.limit);
    const snapshot = await options.db.collection(FORECASTS).where("is_public", "==", true).limit(Math.min(limit * 5, 1000)).get();
    const rows = snapshot.docs
      .map((doc) => ({ id: doc.id, value: plain(doc.data()) }))
      .filter((item) => ["resolved_yes", "resolved_no", "resolved_partial"].includes(String(item.value.status)))
      .filter((item) => matchesFilters(item.value, plain(req.query)))
      .sort((a, b) => String(b.value.resolved_at).localeCompare(String(a.value.resolved_at)))
      .slice(0, limit)
      .map((item) => enterpriseForecastProjection(item.id, item.value));
    res.status(200).json({ data: rows, meta: publicMeta(rows.length) });
    return rows.length;
  }));

  router.get("/v1/forecasts", enterprise(options, "forecasts:read", async (req, res) => {
    const result = await listForecasts(options, plain(req.query), false);
    const rows = result.rows.map((item) => enterpriseForecastProjection(item.id, item.value));
    res.status(200).json({ data: rows, meta: publicMeta(rows.length, result.nextCursor) });
    return rows.length;
  }));

  router.get("/v1/forecasts/:forecastId/history", enterprise(options, "forecasts:history", async (req, res) => {
    const found = await findForecast(options, req.params.forecastId);
    if (!found || found.value.is_public !== true) throw new Error("forecast_not_found");
    const rows = await historyFor(options, found.id);
    res.status(200).json({ data: rows, meta: publicMeta(rows.length) });
    return rows.length;
  }));

  router.get("/v1/forecasts/:forecastId/resolution", enterprise(options, "forecasts:resolved", async (req, res) => {
    const found = await findForecast(options, req.params.forecastId);
    if (!found || found.value.is_public !== true) throw new Error("forecast_not_found");
    const value = found.value;
    const data = {
      forecast_id: found.id,
      status: value.status,
      resolution_rule: value.resolution_rule,
      resolution_source: value.resolution_source,
      resolution_notes: value.resolution_notes,
      resolution_evidence: value.resolution_evidence_json || [],
      actual_outcome: value.actual_outcome,
      resolved_at: value.resolved_at,
      scored_probability: value.scored_probability,
      brier_score: value.brier_score,
      log_score: value.log_score,
    };
    res.status(200).json({ data, meta: publicMeta(1) });
    return 1;
  }));

  router.get("/v1/forecasts/:forecastId", enterprise(options, "forecasts:read", async (req, res) => {
    const found = await findForecast(options, req.params.forecastId);
    if (!found || found.value.is_public !== true) throw new Error("forecast_not_found");
    const amendments = await amendmentsFor(options, found.id);
    res.status(200).json({ data: { ...enterpriseForecastProjection(found.id, found.value), amendments }, meta: publicMeta(1) });
    return 1;
  }));

  router.get("/v1/entities/:entity/forecasts", enterprise(options, "forecasts:read", async (req, res) => {
    const result = await listForecasts(options, { ...req.query, entity: req.params.entity }, false);
    const rows = result.rows.map((item) => enterpriseForecastProjection(item.id, item.value));
    res.status(200).json({ data: rows, meta: publicMeta(rows.length, result.nextCursor) });
    return rows.length;
  }));

  router.get("/v1/calibration", enterprise(options, "forecasts:resolved", async (req, res) => {
    const snapshot = await options.db.collection(FORECASTS).where("is_public", "==", true).limit(5000).get();
    const records = snapshot.docs.map((doc) => plain(doc.data()))
      .filter((item) => ["resolved_yes", "resolved_no"].includes(String(item.status)))
      .filter((item) => matchesFilters(item, plain(req.query)));
    const rows = buildCalibrationRows(records);
    res.status(200).json({ data: rows, meta: { ...publicMeta(rows.length), resolved_forecasts: records.length } });
    return rows.length;
  }));

  router.get("/v1/performance", enterprise(options, "forecasts:resolved", async (req, res) => {
    const snapshot = await options.db.collection(FORECASTS).where("is_public", "==", true).limit(5000).get();
    const records = snapshot.docs.map((doc) => plain(doc.data()))
      .filter((item) => ["resolved_yes", "resolved_no"].includes(String(item.status)))
      .filter((item) => matchesFilters(item, plain(req.query)));
    const average = (field: string) => records.length ? records.reduce((sum, item) => sum + Number(item[field] || 0), 0) / records.length : null;
    const data = {
      resolved_forecasts: records.length,
      average_brier_score: average("brier_score"),
      average_log_score: average("log_score"),
      average_forecast_probability: average("scored_probability"),
      calibration: buildCalibrationRows(records),
    };
    res.status(200).json({ data, meta: publicMeta(1) });
    return 1;
  }));

  router.get("/v1/datasets/forecast-trajectories", enterprise(options, "forecasts:bulk", async (req, res) => {
    const result = await listForecasts(options, plain(req.query), false);
    const rows: JsonRecord[] = [];
    for (const item of result.rows) rows.push(sanitizeDatasetRecord(item.id, item.value, await historyFor(options, item.id)));
    res.status(200).json({ data: rows, meta: { ...publicMeta(rows.length, result.nextCursor), dataset_schema_version: DATASET_SCHEMA_VERSION } });
    return rows.length;
  }));

  router.get("/v1/datasets/releases/:version", enterprise(options, "forecasts:bulk", async (req, res) => {
    const version = text(req.params.version, 120);
    const snapshot = await options.db.collection(DATASET_RELEASES).doc(version).get();
    if (!snapshot.exists) throw new Error("dataset_release_not_found");
    const manifest = plain(snapshot.data());
    const format = text(req.query.format, 20).toLowerCase();
    if (format === "jsonl" || format === "csv") {
      const objectPath = text(plain(manifest.objects)[format], 500);
      const [url] = await admin.storage().bucket().file(objectPath).getSignedUrl({ action: "read", expires: Date.now() + 10 * 60 * 1000 });
      res.status(200).json({ data: { dataset_version: version, format, download_url: url, expires_in_seconds: 600 }, meta: publicMeta(1) });
      return 1;
    }
    res.status(200).json({ data: manifest, meta: publicMeta(1) });
    return 1;
  }));

  router.post("/forecasts/admin", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      const actor = await requireAdmin(req, options);
      const normalized = normalizeForecastDraft({ ...plain(req.body), created_by: actor.uid } as any);
      const forecastId = `qf_${crypto.randomUUID().replace(/-/g, "")}`;
      const slug = String(normalized.slug);
      await options.db.runTransaction(async (transaction) => {
        const slugRef = options.db.collection(SLUGS).doc(slug);
        if ((await transaction.get(slugRef)).exists) throw new Error("forecast_slug_already_exists");
        transaction.create(options.db.collection(FORECASTS).doc(forecastId), normalized);
        transaction.create(slugRef, { forecast_id: forecastId, created_at: normalized.created_at, immutable: true });
      });
      res.status(201).json({ data: { forecast_id: forecastId, ...normalized }, meta: publicMeta(1) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.get("/forecasts/admin/list", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      await requireAdmin(req, options);
      const result = await listForecasts(options, { ...plain(req.query), limit: Math.min(limitFrom(req.query.limit, 100), 100) }, true);
      const rows = result.rows.map((item) => enterpriseForecastProjection(item.id, item.value));
      res.status(200).json({ data: rows, meta: publicMeta(rows.length, result.nextCursor) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.put("/forecasts/admin/:forecastId/draft", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      const actor = await requireAdmin(req, options);
      const body = plain(req.body);
      const ref = options.db.collection(FORECASTS).doc(text(req.params.forecastId, 220));
      await options.db.runTransaction(async (transaction) => {
        const snapshot = await transaction.get(ref);
        if (!snapshot.exists) throw new Error("forecast_not_found");
        const current = plain(snapshot.data());
        if (current.status !== "draft") throw new Error("published_forecast_is_immutable");
        const choose = (field: string) => body[field] === undefined ? current[field] : body[field];
        const normalized = normalizeForecastDraft({
          slug: current.slug,
          category: choose("category"),
          subcategory: choose("subcategory"),
          entity_type: choose("entity_type"),
          entity_id: choose("entity_id"),
          entity_name: choose("entity_name"),
          ticker: choose("ticker"),
          league: choose("league"),
          team: choose("team"),
          politician: choose("politician"),
          organization: choose("organization"),
          question: choose("question"),
          possible_future_headline: choose("possible_future_headline"),
          short_summary: choose("short_summary"),
          probability: choose("current_probability"),
          bull_case: choose("bull_case"),
          base_case: choose("base_case"),
          bear_case: choose("bear_case"),
          input_cutoff_at: choose("input_cutoff_at"),
          resolution_deadline: choose("resolution_deadline"),
          model_provider: choose("model_provider"),
          model_name: choose("model_name"),
          model_version: choose("model_version"),
          forecast_method: choose("forecast_method"),
          reasoning_summary: choose("reasoning_summary"),
          structured_reasoning: plain(choose("structured_reasoning")),
          evidence: (body.evidence === undefined ? current.evidence_json : body.evidence) as any,
          source_metadata: plain(body.source_metadata === undefined ? current.source_metadata_json : body.source_metadata),
          resolution_rule: choose("resolution_rule"),
          resolution_source: choose("resolution_source"),
          created_by: text(current.created_by, 220) || actor.uid,
          review_status: choose("review_status"),
          private_strategy_json: plain(current.private_strategy_json),
        } as any);
        transaction.set(ref, {
          ...normalized,
          created_at: current.created_at,
          updated_at: new Date().toISOString(),
          view_count: current.view_count || 0,
          reviewed_by: actor.uid,
        }, { merge: false });
      });
      const result = await ref.get();
      res.status(200).json({ data: enterpriseForecastProjection(result.id, plain(result.data())), meta: publicMeta(1) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.post("/forecasts/admin/:forecastId/publish", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      const actor = await requireAdmin(req, options);
      const ref = options.db.collection(FORECASTS).doc(text(req.params.forecastId, 220));
      const now = new Date().toISOString();
      await options.db.runTransaction(async (transaction) => {
        const snapshot = await transaction.get(ref);
        if (!snapshot.exists) throw new Error("forecast_not_found");
        const value = plain(snapshot.data());
        const frozen = buildPublishedSnapshot(snapshot.id, value, now);
        const historyRef = ref.collection("probability_history").doc("rev_000001");
        if ((await transaction.get(historyRef)).exists) throw new Error("published_history_already_exists");
        transaction.create(historyRef, {
          forecast_id: snapshot.id,
          revision: 1,
          probability: frozen.probability,
          previous_probability: null,
          probability_delta: 0,
          reasoning_delta: "Initial published forecast.",
          created_at: now,
          input_cutoff_at: frozen.input_cutoff_at,
          model_provider: frozen.model_provider,
          model_name: frozen.model_name,
          model_version: frozen.model_version,
          evidence_json: frozen.evidence_json,
          immutable: true,
        });
        transaction.update(ref, {
          status: "pending",
          is_public: true,
          published_at: now,
          updated_at: now,
          initial_probability: frozen.probability,
          current_probability: frozen.probability,
          current_revision: 1,
          immutable_published_snapshot: frozen,
          initial_snapshot_hash: frozen.snapshot_hash,
          published_by: actor.uid,
        });
      });
      const result = await ref.get();
      res.status(200).json({ data: enterpriseForecastProjection(result.id, plain(result.data())), meta: publicMeta(1) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.post("/forecasts/admin/:forecastId/revisions", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      const actor = await requireAdmin(req, options);
      const ref = options.db.collection(FORECASTS).doc(text(req.params.forecastId, 220));
      let revisionValue: JsonRecord = {};
      await options.db.runTransaction(async (transaction) => {
        const snapshot = await transaction.get(ref);
        if (!snapshot.exists) throw new Error("forecast_not_found");
        const value = plain(snapshot.data());
        revisionValue = normalizeProbabilityRevision(plain(req.body) as any, value);
        const revision = Number(revisionValue.revision);
        const historyRef = ref.collection("probability_history").doc(`rev_${String(revision).padStart(6, "0")}`);
        if ((await transaction.get(historyRef)).exists) throw new Error("forecast_revision_already_exists");
        transaction.create(historyRef, { forecast_id: snapshot.id, ...revisionValue, created_by: actor.uid });
        transaction.update(ref, {
          current_probability: revisionValue.probability,
          confidence_label: Number(revisionValue.probability) >= 0.75 || Number(revisionValue.probability) <= 0.25 ? "high" : "medium",
          previous_probability: revisionValue.previous_probability,
          probability_delta: revisionValue.probability_delta,
          current_revision: revision,
          updated_at: revisionValue.created_at,
          input_cutoff_at: revisionValue.input_cutoff_at,
          model_provider: revisionValue.model_provider,
          model_name: revisionValue.model_name,
          model_version: revisionValue.model_version,
        });
      });
      res.status(201).json({ data: revisionValue, meta: publicMeta(1) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.post("/forecasts/admin/:forecastId/amendments", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      const actor = await requireAdmin(req, options);
      const ref = options.db.collection(FORECASTS).doc(text(req.params.forecastId, 220));
      const amendmentRef = ref.collection(AMENDMENTS).doc(`amend_${crypto.randomUUID().replace(/-/g, "")}`);
      let amendment: JsonRecord = {};
      await options.db.runTransaction(async (transaction) => {
        const snapshot = await transaction.get(ref);
        if (!snapshot.exists) throw new Error("forecast_not_found");
        amendment = normalizeForecastAmendment(plain(req.body) as any, plain(snapshot.data()), actor.uid);
        transaction.create(amendmentRef, { forecast_id: snapshot.id, ...amendment });
        transaction.update(ref, { latest_amendment_at: amendment.created_at, updated_at: amendment.created_at });
      });
      res.status(201).json({ data: { amendment_id: amendmentRef.id, ...amendment }, meta: publicMeta(1) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.post("/forecasts/admin/:forecastId/resolve", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      const actor = await requireAdmin(req, options);
      const body = plain(req.body);
      const outcome = text(body.outcome, 20).toLowerCase();
      if (!["yes", "no", "partial", "void", "disputed"].includes(outcome)) throw new Error("resolution_outcome_invalid");
      const evidence = Array.isArray(body.evidence) ? body.evidence.slice(0, 50) : [];
      if (["yes", "no", "partial"].includes(outcome) && !evidence.length) throw new Error("resolution_evidence_required");
      const ref = options.db.collection(FORECASTS).doc(text(req.params.forecastId, 220));
      await options.db.runTransaction(async (transaction) => {
        const snapshot = await transaction.get(ref);
        if (!snapshot.exists) throw new Error("forecast_not_found");
        const value = plain(snapshot.data());
        const nextStatus = (outcome === "yes" ? "resolved_yes" : outcome === "no" ? "resolved_no" : outcome === "partial" ? "resolved_partial" : outcome) as ForecastStatus;
        assertStatusTransition(String(value.status) as ForecastStatus, nextStatus);
        const probability = validateProbability(value.current_probability);
        const now = new Date().toISOString();
        transaction.update(ref, {
          status: nextStatus,
          actual_outcome: outcome,
          resolution_notes: text(body.notes, 3000),
          resolution_evidence_json: evidence,
          resolved_at: ["yes", "no", "partial"].includes(outcome) ? now : null,
          scored_probability: ["yes", "no"].includes(outcome) ? probability : null,
          brier_score: outcome === "yes" || outcome === "no" ? brierScore(probability, outcome) : null,
          log_score: outcome === "yes" || outcome === "no" ? logScore(probability, outcome) : null,
          updated_at: now,
          resolved_by: actor.uid,
        });
      });
      const result = await ref.get();
      res.status(200).json({ data: enterpriseForecastProjection(result.id, plain(result.data())), meta: publicMeta(1) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.post("/forecasts/admin/api-keys", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      const actor = await requireAdmin(req, options);
      const body = plain(req.body);
      const scopes = validateScopes(body.scopes);
      const pepper = text(process.env.QUANTURA_FORECAST_API_KEY_PEPPER, 2000);
      const created = createForecastApiKey();
      const keyId = hashForecastApiKey(created.rawKey, pepper);
      const expiresAt = parseIsoFilter(body.expires_at, "expires_at");
      if (expiresAt && Date.parse(expiresAt) <= Date.now()) throw new Error("api_key_expiration_invalid");
      await options.db.collection(API_KEYS).doc(keyId).create({
        key_id: keyId,
        key_prefix: created.prefix,
        customer_id: text(body.customer_id, 220),
        label: text(body.label, 200),
        scopes,
        tier: text(body.tier, 80) || "developer",
        rate_limit_per_minute: Math.min(Math.max(Math.floor(Number(body.rate_limit_per_minute) || 60), 1), 10_000),
        created_at: new Date().toISOString(),
        created_by: actor.uid,
        expires_at: expiresAt,
        revoked_at: null,
      });
      res.status(201).json({
        data: { api_key: created.rawKey, key_id: keyId, key_prefix: created.prefix, customer_id: body.customer_id, scopes, expires_at: expiresAt },
        meta: { ...publicMeta(1), notice: "Store this key securely. It will not be shown again." },
      });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.get("/forecasts/admin/api-keys", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      await requireAdmin(req, options);
      const snapshot = await options.db.collection(API_KEYS).orderBy("created_at", "desc").limit(200).get();
      const rows = snapshot.docs.map((doc) => {
        const value = plain(doc.data());
        return {
          key_id: doc.id,
          key_prefix: value.key_prefix,
          customer_id: value.customer_id,
          label: value.label,
          scopes: value.scopes,
          tier: value.tier,
          rate_limit_per_minute: value.rate_limit_per_minute,
          created_at: value.created_at,
          expires_at: value.expires_at || null,
          revoked_at: value.revoked_at || null,
        };
      });
      res.status(200).json({ data: rows, meta: publicMeta(rows.length) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.post("/forecasts/admin/api-keys/:keyId/revoke", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      const actor = await requireAdmin(req, options);
      const ref = options.db.collection(API_KEYS).doc(text(req.params.keyId, 128));
      if (!(await ref.get()).exists) throw new Error("api_key_not_found");
      await ref.update({ revoked_at: new Date().toISOString(), revoked_by: actor.uid });
      res.status(200).json({ data: { revoked: true, key_id: ref.id }, meta: publicMeta(1) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.post("/forecasts/admin/dataset-releases", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      const actor = await requireAdmin(req, options);
      const manifest = await createDatasetRelease(options, plain(req.body), actor.uid);
      res.status(201).json({ data: manifest, meta: publicMeta(1) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });

  router.post("/forecasts/admin/jobs/:jobName", async (req, res) => {
    const requestId = crypto.randomUUID();
    try {
      await requireAdmin(req, options);
      const result = await runForecastLifecycleJob(options, req.params.jobName);
      res.status(200).json({ data: result, meta: publicMeta(1) });
    } catch (error) {
      sendError(res, error, requestId);
    }
  });
}
