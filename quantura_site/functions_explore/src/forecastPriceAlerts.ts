import admin from "firebase-admin";
import crypto from "node:crypto";

export const FORECAST_ALERT_COLLECTION = "forecast_price_alerts";
export const FORECAST_ALERT_PRICE_SOURCE = "Alpaca latest completed 1-minute bar close";
export const DEFAULT_FORECAST_ALERT_BOUNDARIES = Object.freeze(["P10", "P50", "P90"]);

export type ForecastMarketSession = "premarket" | "regular" | "after_hours" | "overnight";
export type ForecastSessionMode = "regular" | "extended";
export type BoundarySide = "above" | "below" | "at";
export type CrossingDirection = "above" | "below";

export type ForecastBoundaryScheduleRow = {
  timestamp: string;
  dateKey: string;
  values: Record<string, number>;
};

export type ForecastBoundaryState = {
  forecastTimestamp: string;
  boundaryValue: number;
  previousPrice: number;
  side: BoundarySide;
  lastDirection?: CrossingDirection;
  lastCrossingAt?: string;
  lastNotificationAt?: string;
};

export type ForecastCrossing = {
  boundary: string;
  boundaryValue: number;
  direction: CrossingDirection;
  previousPrice: number;
  currentPrice: number;
  priceTimestamp: string;
  forecastTimestamp: string;
  forecastDate: string;
};

export type ForecastPriceQuote = {
  symbol: string;
  price: number;
  timestamp: string;
  session: ForecastMarketSession;
};

export type ForecastAlertRecord = {
  id: string;
  userId: string;
  runId: string;
  ticker: string;
  email: string;
  emailVerified: boolean;
  enabled: boolean;
  globalEnabled: boolean;
  status: "active" | "disabled" | "expired" | "configuration_required" | "error";
  sessionMode: ForecastSessionMode;
  monitoredBoundaries: string[];
  availableBoundaries: string[];
  schedule: ForecastBoundaryScheduleRow[];
  boundaryStates: Record<string, ForecastBoundaryState>;
  horizonStart: string;
  horizonEnd: string;
  analysisUrl: string;
  lastCheckedAt?: string;
  lastPrice?: number;
  lastPriceAt?: string;
  lastCrossing?: ForecastCrossing;
  lastNotificationAt?: string;
  lastError?: { code: string; message: string; at: string } | null;
};

export type ForecastAlertEvent = {
  id: string;
  alertId: string;
  userId: string;
  ticker: string;
  email: string;
  crossings: ForecastCrossing[];
  analysisUrl: string;
  status: "pending" | "sent" | "failed";
  detectedAt: string;
  notBeforeMs: number;
  attempts: number;
  lastAttemptAt?: string;
  sentAt?: string;
  providerMessageId?: string;
  errorCode?: string;
};

export type AlertEvaluation = {
  status: ForecastAlertRecord["status"];
  reason: "checked" | "disabled" | "expired" | "missing_forecast_row" | "outside_session" | "stale_price";
  boundaryStates: Record<string, ForecastBoundaryState>;
  crossings: ForecastCrossing[];
  lastCheckedAt: string;
  lastPrice?: number;
  lastPriceAt?: string;
  lastCrossing?: ForecastCrossing;
};

export interface ForecastAlertRepository {
  listActiveAlerts(): Promise<ForecastAlertRecord[]>;
  updateAlert(alertId: string, patch: Partial<ForecastAlertRecord>): Promise<void>;
  createEventIfAbsent(event: ForecastAlertEvent): Promise<boolean>;
  listPendingEvents(alertId: string, nowMs: number): Promise<ForecastAlertEvent[]>;
  updateEvent(alertId: string, eventId: string, patch: Partial<ForecastAlertEvent>): Promise<void>;
}

export interface ForecastAlertPriceSource {
  getLatestPrices(symbols: string[]): Promise<Map<string, ForecastPriceQuote>>;
}

export interface ForecastAlertEmailProvider {
  sendBoundaryCrossing(event: ForecastAlertEvent): Promise<{ messageId: string }>;
}

export type ForecastMonitorResult = {
  checkedAlerts: number;
  fetchedTickers: number;
  crossings: number;
  emailsSent: number;
  emailFailures: number;
  missingPrices: number;
  expiredAlerts: number;
  errors: number;
};

function cleanText(value: unknown, maxLength = 500): string {
  return String(value ?? "")
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
}

function finite(value: unknown): number | null {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function dateKeyInNewYork(value: Date): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(value);
  const map = new Map(parts.map((part) => [part.type, part.value]));
  return `${map.get("year")}-${map.get("month")}-${map.get("day")}`;
}

export function normalizeBoundaryLabel(value: unknown): string {
  const raw = cleanText(value, 24).toUpperCase().replace(/[^A-Z0-9.]/g, "");
  const match = raw.match(/^(?:P|Q)?(\d+(?:\.\d+)?)$/);
  if (!match) return "";
  let numeric = Number(match[1]);
  if (!Number.isFinite(numeric)) return "";
  if (raw.startsWith("Q") && numeric <= 1) numeric *= 100;
  if (numeric <= 1 && !raw.startsWith("P")) numeric *= 100;
  if (numeric <= 0 || numeric >= 100) return "";
  const rounded = Number(numeric.toFixed(4));
  return `P${String(rounded).replace(/\.0+$/, "")}`;
}

export function normalizeBoundarySelection(values: unknown, available: string[]): string[] {
  const availableSet = new Set(available.map(normalizeBoundaryLabel).filter(Boolean));
  const requested = Array.isArray(values) ? values : DEFAULT_FORECAST_ALERT_BOUNDARIES;
  return [...new Set(requested.map(normalizeBoundaryLabel).filter((label) => availableSet.has(label)))];
}

export function sideOfBoundary(price: number, boundary: number): BoundarySide {
  if (price > boundary) return "above";
  if (price < boundary) return "below";
  return "at";
}

function parseScheduleTimestamp(value: string): number {
  const clean = cleanText(value, 64);
  if (!clean) return Number.NaN;
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(clean) ? `${clean}T00:00:00.000Z` : clean.replace(" ", "T") + (/Z$|[+-]\d\d:?\d\d$/.test(clean) ? "" : "Z");
  return Date.parse(normalized);
}

export function selectApplicableBoundaryRow(
  schedule: ForecastBoundaryScheduleRow[],
  now: Date
): ForecastBoundaryScheduleRow | null {
  const dateKey = dateKeyInNewYork(now);
  const nowMs = now.getTime();
  const candidates = schedule
    .filter((row) => row.dateKey === dateKey)
    .filter((row) => {
      const timestampMs = parseScheduleTimestamp(row.timestamp);
      return Number.isFinite(timestampMs) && timestampMs <= nowMs;
    })
    .sort((left, right) => parseScheduleTimestamp(left.timestamp) - parseScheduleTimestamp(right.timestamp));
  return candidates[candidates.length - 1] || null;
}

function isPriceFresh(quote: ForecastPriceQuote, now: Date, maxAgeMinutes: number): boolean {
  const timestamp = Date.parse(quote.timestamp);
  if (!Number.isFinite(timestamp)) return false;
  const ageMs = now.getTime() - timestamp;
  return ageMs >= -60_000 && ageMs <= maxAgeMinutes * 60_000;
}

function sessionAllowed(mode: ForecastSessionMode, session: ForecastMarketSession): boolean {
  if (mode === "regular") return session === "regular";
  return session === "premarket" || session === "regular" || session === "after_hours";
}

export function evaluateForecastAlert(
  alert: ForecastAlertRecord,
  quote: ForecastPriceQuote,
  options: { now?: Date; maxPriceAgeMinutes?: number } = {}
): AlertEvaluation {
  const now = options.now || new Date();
  const checkedAt = now.toISOString();
  const states = { ...(alert.boundaryStates || {}) };
  if (!alert.enabled || !alert.globalEnabled) {
    return { status: "disabled", reason: "disabled", boundaryStates: states, crossings: [], lastCheckedAt: checkedAt };
  }
  const currentDate = dateKeyInNewYork(now);
  if (alert.horizonEnd && currentDate > alert.horizonEnd) {
    return { status: "expired", reason: "expired", boundaryStates: states, crossings: [], lastCheckedAt: checkedAt };
  }
  if (!sessionAllowed(alert.sessionMode, quote.session)) {
    return { status: "active", reason: "outside_session", boundaryStates: states, crossings: [], lastCheckedAt: checkedAt };
  }
  if (!isPriceFresh(quote, now, Math.max(2, options.maxPriceAgeMinutes || 30))) {
    return { status: "active", reason: "stale_price", boundaryStates: states, crossings: [], lastCheckedAt: checkedAt };
  }
  const row = selectApplicableBoundaryRow(alert.schedule || [], now);
  if (!row) {
    return { status: "active", reason: "missing_forecast_row", boundaryStates: states, crossings: [], lastCheckedAt: checkedAt };
  }

  const crossings: ForecastCrossing[] = [];
  for (const boundary of alert.monitoredBoundaries) {
    const boundaryValue = finite(row.values?.[boundary]);
    if (boundaryValue === null) continue;
    const existing = states[boundary];
    const sameForecastBoundary = Boolean(
      existing &&
      existing.forecastTimestamp === row.timestamp &&
      Math.abs(existing.boundaryValue - boundaryValue) < 1e-10
    );
    const currentSide = sideOfBoundary(quote.price, boundaryValue);
    if (!sameForecastBoundary) {
      states[boundary] = {
        forecastTimestamp: row.timestamp,
        boundaryValue,
        previousPrice: quote.price,
        side: currentSide,
      };
      continue;
    }

    const crossedUp = existing.previousPrice <= boundaryValue && quote.price > boundaryValue;
    const crossedDown = existing.previousPrice >= boundaryValue && quote.price < boundaryValue;
    const direction: CrossingDirection | null = crossedUp ? "above" : crossedDown ? "below" : null;
    const crossing = direction
      ? {
          boundary,
          boundaryValue,
          direction,
          previousPrice: existing.previousPrice,
          currentPrice: quote.price,
          priceTimestamp: quote.timestamp,
          forecastTimestamp: row.timestamp,
          forecastDate: row.dateKey,
        }
      : null;
    states[boundary] = {
      ...existing,
      previousPrice: quote.price,
      side: currentSide,
      ...(crossing
        ? {
            lastDirection: direction!,
            lastCrossingAt: checkedAt,
          }
        : {}),
    };
    if (crossing) crossings.push(crossing);
  }

  return {
    status: "active",
    reason: "checked",
    boundaryStates: states,
    crossings,
    lastCheckedAt: checkedAt,
    lastPrice: quote.price,
    lastPriceAt: quote.timestamp,
    lastCrossing: crossings[crossings.length - 1] || alert.lastCrossing,
  };
}

export function buildForecastAlertEvent(alert: ForecastAlertRecord, crossings: ForecastCrossing[], now: Date): ForecastAlertEvent {
  const identity = [alert.id, crossings.map((item) => `${item.boundary}:${item.direction}`).join(","), crossings[0]?.priceTimestamp || now.toISOString()].join("|");
  const id = crypto.createHash("sha256").update(identity).digest("hex").slice(0, 32);
  return {
    id,
    alertId: alert.id,
    userId: alert.userId,
    ticker: alert.ticker,
    email: alert.email,
    crossings,
    analysisUrl: alert.analysisUrl,
    status: "pending",
    detectedAt: now.toISOString(),
    notBeforeMs: now.getTime(),
    attempts: 0,
  };
}

export function safeMonitorError(error: unknown): { code: string; message: string } {
  const candidate = error as { code?: unknown; name?: unknown };
  const rawCode = cleanText(candidate?.code || candidate?.name || "monitor_error", 80).toLowerCase().replace(/[^a-z0-9_-]/g, "_");
  const code = rawCode || "monitor_error";
  const messages: Record<string, string> = {
    authentication: "Market-data authentication failed.",
    entitlement: "The configured market-data feed is not available.",
    rate_limit: "Market-data rate limit reached; monitoring will retry.",
    network: "Market-data service could not be reached; monitoring will retry.",
    no_data: "No current market price was available.",
    email_configuration: "Forecast alert email delivery is not configured.",
    email_network: "The email service could not be reached; delivery will retry.",
    email_rate_limit: "The email service rate limit was reached; delivery will retry.",
    email_provider: "The email service rejected this notification; delivery will retry.",
    invalid_recipient: "The verified alert email is unavailable.",
  };
  return { code, message: messages[code] || "The alert monitor could not complete this check and will retry." };
}

export async function runForecastAlertMonitor(
  dependencies: {
    repository: ForecastAlertRepository;
    priceSource: ForecastAlertPriceSource;
    emailProvider: ForecastAlertEmailProvider;
    now?: Date;
    cooldownMinutes?: number;
    maxPriceAgeMinutes?: number;
  }
): Promise<ForecastMonitorResult> {
  const now = dependencies.now || new Date();
  const cooldownMs = Math.max(0, dependencies.cooldownMinutes ?? 15) * 60_000;
  const result: ForecastMonitorResult = {
    checkedAlerts: 0,
    fetchedTickers: 0,
    crossings: 0,
    emailsSent: 0,
    emailFailures: 0,
    missingPrices: 0,
    expiredAlerts: 0,
    errors: 0,
  };
  const alerts = await dependencies.repository.listActiveAlerts();
  const currentDate = dateKeyInNewYork(now);
  const tickers = [...new Set(alerts
    .filter((alert) => alert.enabled && alert.globalEnabled && (!alert.horizonEnd || currentDate <= alert.horizonEnd))
    .map((alert) => alert.ticker)
    .filter(Boolean))];
  let quotes = new Map<string, ForecastPriceQuote>();
  try {
    quotes = tickers.length ? await dependencies.priceSource.getLatestPrices(tickers) : quotes;
    result.fetchedTickers = quotes.size;
  } catch (error) {
    const safe = safeMonitorError(error);
    result.errors += tickers.length || 1;
    await Promise.all(alerts.map((alert) => dependencies.repository.updateAlert(alert.id, {
      lastCheckedAt: now.toISOString(),
      lastError: { ...safe, at: now.toISOString() },
    })));
    return result;
  }

  for (const alert of alerts) {
    result.checkedAlerts += 1;
    if (alert.horizonEnd && currentDate > alert.horizonEnd) {
      result.expiredAlerts += 1;
      await dependencies.repository.updateAlert(alert.id, {
        status: "expired",
        enabled: false,
        lastCheckedAt: now.toISOString(),
        lastError: null,
      });
      continue;
    }
    if (!alert.globalEnabled) {
      await dependencies.repository.updateAlert(alert.id, {
        status: "disabled",
        lastCheckedAt: now.toISOString(),
        lastError: null,
      });
      continue;
    }
    const quote = quotes.get(alert.ticker);
    if (!quote) {
      result.missingPrices += 1;
      await dependencies.repository.updateAlert(alert.id, {
        lastCheckedAt: now.toISOString(),
        lastError: { code: "no_data", message: "No current Alpaca bar was available for this ticker.", at: now.toISOString() },
      });
      continue;
    }
    const evaluation = evaluateForecastAlert(alert, quote, { now, maxPriceAgeMinutes: dependencies.maxPriceAgeMinutes });
    if (evaluation.status === "expired") result.expiredAlerts += 1;
    result.crossings += evaluation.crossings.length;
    await dependencies.repository.updateAlert(alert.id, {
      status: evaluation.status,
      enabled: evaluation.status === "expired" ? false : alert.enabled,
      boundaryStates: evaluation.boundaryStates,
      lastCheckedAt: evaluation.lastCheckedAt,
      lastPrice: evaluation.lastPrice,
      lastPriceAt: evaluation.lastPriceAt,
      lastCrossing: evaluation.lastCrossing,
      lastError: evaluation.reason === "stale_price"
        ? { code: "stale_price", message: "The latest Alpaca bar was too old to evaluate safely.", at: now.toISOString() }
        : evaluation.reason === "missing_forecast_row"
          ? { code: "missing_forecast_row", message: "No forecast boundary is applicable to this monitoring date.", at: now.toISOString() }
          : null,
    });
    alert.boundaryStates = evaluation.boundaryStates;
    if (evaluation.crossings.length && alert.emailVerified && alert.email) {
      const created = await dependencies.repository.createEventIfAbsent(buildForecastAlertEvent(alert, evaluation.crossings, now));
      if (!created) continue;
    }

    const pending = await dependencies.repository.listPendingEvents(alert.id, now.getTime());
    for (const event of pending.slice(0, 3)) {
      const alertLastSentMs = Date.parse(alert.lastNotificationAt || "");
      if (Number.isFinite(alertLastSentMs) && now.getTime() - alertLastSentMs < cooldownMs) {
        await dependencies.repository.updateEvent(alert.id, event.id, { notBeforeMs: alertLastSentMs + cooldownMs });
        continue;
      }
      try {
        const sent = await dependencies.emailProvider.sendBoundaryCrossing(event);
        const sentAt = now.toISOString();
        await dependencies.repository.updateEvent(alert.id, event.id, {
          status: "sent",
          sentAt,
          providerMessageId: cleanText(sent.messageId, 220),
          attempts: event.attempts + 1,
          lastAttemptAt: sentAt,
          errorCode: "",
        });
        const boundaryStates = { ...alert.boundaryStates };
        event.crossings.forEach((crossing) => {
          const state = boundaryStates[crossing.boundary];
          if (state) boundaryStates[crossing.boundary] = { ...state, lastNotificationAt: sentAt };
        });
        await dependencies.repository.updateAlert(alert.id, { boundaryStates, lastNotificationAt: sentAt, lastError: null });
        alert.boundaryStates = boundaryStates;
        alert.lastNotificationAt = sentAt;
        result.emailsSent += 1;
      } catch (error) {
        const safe = safeMonitorError(error);
        await dependencies.repository.updateEvent(alert.id, event.id, {
          status: "failed",
          attempts: event.attempts + 1,
          lastAttemptAt: now.toISOString(),
          notBeforeMs: now.getTime() + Math.min(60, 5 * (event.attempts + 1)) * 60_000,
          errorCode: safe.code,
        });
        await dependencies.repository.updateAlert(alert.id, { lastError: { ...safe, at: now.toISOString() } });
        result.emailFailures += 1;
      }
    }
  }
  return result;
}

function timestampToIso(value: unknown): string {
  if (typeof value === "string") return cleanText(value, 80);
  if (value && typeof value === "object" && typeof (value as { toDate?: unknown }).toDate === "function") {
    return ((value as { toDate: () => Date }).toDate()).toISOString();
  }
  return "";
}

function deserializeAlert(id: string, data: Record<string, unknown>): ForecastAlertRecord {
  const scheduleRaw = Array.isArray(data.schedule) ? data.schedule : [];
  const schedule = scheduleRaw.slice(0, 5000).map((entry) => {
    const row = (entry || {}) as Record<string, unknown>;
    const valuesRaw = row.values && typeof row.values === "object" ? row.values as Record<string, unknown> : {};
    const values: Record<string, number> = {};
    Object.entries(valuesRaw).forEach(([key, value]) => {
      const numeric = finite(value);
      if (numeric !== null) values[normalizeBoundaryLabel(key)] = numeric;
    });
    return { timestamp: cleanText(row.timestamp, 80), dateKey: cleanText(row.dateKey, 10), values };
  }).filter((row) => row.timestamp && row.dateKey && Object.keys(row.values).length);
  const boundaryStates = data.boundaryStates && typeof data.boundaryStates === "object"
    ? data.boundaryStates as Record<string, ForecastBoundaryState>
    : {};
  return {
    id,
    userId: cleanText(data.userId, 220),
    runId: cleanText(data.runId, 220),
    ticker: cleanText(data.ticker, 24).toUpperCase(),
    email: cleanText(data.email, 320).toLowerCase(),
    emailVerified: data.emailVerified === true,
    enabled: data.enabled === true,
    globalEnabled: data.globalEnabled !== false,
    status: (cleanText(data.status, 40) || "disabled") as ForecastAlertRecord["status"],
    sessionMode: data.sessionMode === "extended" ? "extended" : "regular",
    monitoredBoundaries: Array.isArray(data.monitoredBoundaries) ? data.monitoredBoundaries.map(normalizeBoundaryLabel).filter(Boolean) : [],
    availableBoundaries: Array.isArray(data.availableBoundaries) ? data.availableBoundaries.map(normalizeBoundaryLabel).filter(Boolean) : [],
    schedule,
    boundaryStates,
    horizonStart: cleanText(data.horizonStart, 10),
    horizonEnd: cleanText(data.horizonEnd, 10),
    analysisUrl: cleanText(data.analysisUrl, 1000),
    lastCheckedAt: timestampToIso(data.lastCheckedAt),
    lastPrice: finite(data.lastPrice) ?? undefined,
    lastPriceAt: cleanText(data.lastPriceAt, 80),
    lastCrossing: data.lastCrossing && typeof data.lastCrossing === "object" ? data.lastCrossing as ForecastCrossing : undefined,
    lastNotificationAt: timestampToIso(data.lastNotificationAt),
    lastError: data.lastError && typeof data.lastError === "object" ? data.lastError as ForecastAlertRecord["lastError"] : null,
  };
}

function deserializeEvent(id: string, data: Record<string, unknown>): ForecastAlertEvent {
  return {
    id,
    alertId: cleanText(data.alertId, 220),
    userId: cleanText(data.userId, 220),
    ticker: cleanText(data.ticker, 24),
    email: cleanText(data.email, 320),
    crossings: Array.isArray(data.crossings) ? data.crossings as ForecastCrossing[] : [],
    analysisUrl: cleanText(data.analysisUrl, 1000),
    status: (cleanText(data.status, 20) || "pending") as ForecastAlertEvent["status"],
    detectedAt: timestampToIso(data.detectedAt) || cleanText(data.detectedAt, 80),
    notBeforeMs: finite(data.notBeforeMs) || 0,
    attempts: Math.max(0, Math.floor(finite(data.attempts) || 0)),
    lastAttemptAt: timestampToIso(data.lastAttemptAt),
    sentAt: timestampToIso(data.sentAt),
    providerMessageId: cleanText(data.providerMessageId, 220),
    errorCode: cleanText(data.errorCode, 80),
  };
}

export class FirestoreForecastAlertRepository implements ForecastAlertRepository {
  constructor(private readonly db: admin.firestore.Firestore) {}

  async listActiveAlerts(): Promise<ForecastAlertRecord[]> {
    const alerts: ForecastAlertRecord[] = [];
    let cursor: admin.firestore.QueryDocumentSnapshot | null = null;
    do {
      let query: admin.firestore.Query = this.db.collection(FORECAST_ALERT_COLLECTION)
        .where("enabled", "==", true)
        .orderBy(admin.firestore.FieldPath.documentId())
        .limit(500);
      if (cursor) query = query.startAfter(cursor);
      const snap = await query.get();
      snap.docs.forEach((doc) => alerts.push(deserializeAlert(doc.id, (doc.data() || {}) as Record<string, unknown>)));
      cursor = snap.size === 500 ? snap.docs[snap.docs.length - 1] : null;
    } while (cursor);
    return alerts;
  }

  async updateAlert(alertId: string, patch: Partial<ForecastAlertRecord>): Promise<void> {
    const safePatch: Record<string, unknown> = { ...patch, updatedAt: admin.firestore.FieldValue.serverTimestamp() };
    Object.keys(safePatch).forEach((key) => safePatch[key] === undefined && delete safePatch[key]);
    const alertRef = this.db.collection(FORECAST_ALERT_COLLECTION).doc(alertId);
    const snap = await alertRef.get();
    const runId = cleanText((snap.data() || {}).runId, 220);
    const batch = this.db.batch();
    batch.set(alertRef, safePatch, { merge: true });
    if (runId) batch.set(this.db.collection("autopilot_requests").doc(runId), { priceAlert: publicForecastAlert({ ...deserializeAlert(alertId, (snap.data() || {}) as Record<string, unknown>), ...patch } as ForecastAlertRecord), updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    await batch.commit();
  }

  async createEventIfAbsent(event: ForecastAlertEvent): Promise<boolean> {
    const ref = this.db.collection(FORECAST_ALERT_COLLECTION).doc(event.alertId).collection("events").doc(event.id);
    return this.db.runTransaction(async (transaction) => {
      const snap = await transaction.get(ref);
      if (snap.exists) return false;
      transaction.create(ref, { ...event, createdAt: admin.firestore.FieldValue.serverTimestamp() });
      return true;
    });
  }

  async listPendingEvents(alertId: string, nowMs: number): Promise<ForecastAlertEvent[]> {
    const snap = await this.db.collection(FORECAST_ALERT_COLLECTION).doc(alertId).collection("events")
      .where("status", "in", ["pending", "failed"])
      .limit(20)
      .get();
    return snap.docs
      .map((doc) => deserializeEvent(doc.id, (doc.data() || {}) as Record<string, unknown>))
      .filter((event) => event.notBeforeMs <= nowMs)
      .sort((left, right) => left.notBeforeMs - right.notBeforeMs);
  }

  async updateEvent(alertId: string, eventId: string, patch: Partial<ForecastAlertEvent>): Promise<void> {
    const safePatch: Record<string, unknown> = { ...patch, updatedAt: admin.firestore.FieldValue.serverTimestamp() };
    Object.keys(safePatch).forEach((key) => safePatch[key] === undefined && delete safePatch[key]);
    await this.db.collection(FORECAST_ALERT_COLLECTION).doc(alertId).collection("events").doc(eventId).set(safePatch, { merge: true });
  }
}

export function publicForecastAlert(alert: ForecastAlertRecord): Record<string, unknown> {
  return {
    id: alert.id,
    runId: alert.runId,
    ticker: alert.ticker,
    enabled: alert.enabled,
    globalEnabled: alert.globalEnabled,
    status: alert.status,
    sessionMode: alert.sessionMode,
    monitoredBoundaries: alert.monitoredBoundaries,
    availableBoundaries: alert.availableBoundaries,
    horizonStart: alert.horizonStart,
    horizonEnd: alert.horizonEnd,
    emailVerified: alert.emailVerified,
    priceSource: FORECAST_ALERT_PRICE_SOURCE,
    lastCheckedAt: alert.lastCheckedAt || "",
    lastPrice: alert.lastPrice ?? null,
    lastPriceAt: alert.lastPriceAt || "",
    lastCrossing: alert.lastCrossing || null,
    lastNotificationAt: alert.lastNotificationAt || "",
    lastError: alert.lastError || null,
  };
}

export function assertForecastAlertOwner(alert: Pick<ForecastAlertRecord, "userId">, userId: string): void {
  if (!userId || alert.userId !== userId) {
    const error = new Error("forecast_alert_not_found") as Error & { code?: string };
    error.code = "unauthorized";
    throw error;
  }
}

export function alertFromSnapshot(id: string, data: Record<string, unknown>): ForecastAlertRecord {
  return deserializeAlert(id, data);
}
