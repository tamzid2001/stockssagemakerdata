import assert from "node:assert/strict";
import test from "node:test";
import { buildForecastBoundaryEmail, ResendForecastAlertEmailProvider } from "./forecastAlertEmail";
import {
  assertForecastAlertOwner,
  buildForecastAlertEvent,
  evaluateForecastAlert,
  runForecastAlertMonitor,
  type ForecastAlertEmailProvider,
  type ForecastAlertEvent,
  type ForecastAlertPriceSource,
  type ForecastAlertRecord,
  type ForecastAlertRepository,
  type ForecastPriceQuote,
} from "./forecastPriceAlerts";

const REGULAR_NOW = new Date("2026-08-24T15:00:00.000Z");

function alert(overrides: Partial<ForecastAlertRecord> = {}): ForecastAlertRecord {
  return {
    id: "run-1",
    userId: "user-1",
    runId: "run-1",
    ticker: "PLTR",
    email: "verified@example.test",
    emailVerified: true,
    enabled: true,
    globalEnabled: true,
    status: "active",
    sessionMode: "regular",
    monitoredBoundaries: ["P10", "P50", "P90"],
    availableBoundaries: ["P10", "P50", "P90"],
    schedule: [{ timestamp: "2026-08-24T00:00:00.000Z", dateKey: "2026-08-24", values: { P10: 90, P50: 100, P90: 110 } }],
    boundaryStates: {},
    horizonStart: "2026-08-24",
    horizonEnd: "2026-08-28",
    analysisUrl: "https://quantura.studio/autopilot?runId=run-1",
    ...overrides,
  };
}

function quote(price: number, overrides: Partial<ForecastPriceQuote> = {}): ForecastPriceQuote {
  return { symbol: "PLTR", price, timestamp: "2026-08-24T14:59:00.000Z", session: "regular", ...overrides };
}

function withBaseline(price: number, overrides: Partial<ForecastAlertRecord> = {}): ForecastAlertRecord {
  const first = evaluateForecastAlert(alert(overrides), quote(price), { now: REGULAR_NOW });
  return alert({ ...overrides, boundaryStates: first.boundaryStates, lastPrice: price, lastPriceAt: quote(price).timestamp });
}

test("price stays below a boundary without creating a crossing", () => {
  const result = evaluateForecastAlert(withBaseline(80, { monitoredBoundaries: ["P10"] }), quote(85), { now: REGULAR_NOW });
  assert.equal(result.crossings.length, 0);
});

test("price stays above a boundary without creating a crossing", () => {
  const result = evaluateForecastAlert(withBaseline(120, { monitoredBoundaries: ["P90"] }), quote(115), { now: REGULAR_NOW });
  assert.equal(result.crossings.length, 0);
});

test("below to above creates an upward P50 crossing", () => {
  const result = evaluateForecastAlert(withBaseline(99, { monitoredBoundaries: ["P50"] }), quote(101), { now: REGULAR_NOW });
  assert.deepEqual(result.crossings.map((item) => [item.boundary, item.direction]), [["P50", "above"]]);
});

test("above to below creates a downward P50 crossing", () => {
  const result = evaluateForecastAlert(withBaseline(101, { monitoredBoundaries: ["P50"] }), quote(99), { now: REGULAR_NOW });
  assert.deepEqual(result.crossings.map((item) => [item.boundary, item.direction]), [["P50", "below"]]);
});

test("repeated check on the same side creates no duplicate crossing", () => {
  const source = withBaseline(99, { monitoredBoundaries: ["P50"] });
  const crossed = evaluateForecastAlert(source, quote(101), { now: REGULAR_NOW });
  const next = alert({ monitoredBoundaries: ["P50"], boundaryStates: crossed.boundaryStates });
  assert.equal(evaluateForecastAlert(next, quote(102), { now: new Date("2026-08-24T15:05:00Z") }).crossings.length, 0);
});

for (const boundary of ["P10", "P50", "P90"]) {
  test(`${boundary} crossing is detected independently`, () => {
    const level = { P10: 90, P50: 100, P90: 110 }[boundary] as number;
    const result = evaluateForecastAlert(withBaseline(level - 1, { monitoredBoundaries: [boundary] }), quote(level + 1), { now: REGULAR_NOW });
    assert.equal(result.crossings[0]?.boundary, boundary);
  });
}

test("one price move may cross multiple monitored boundaries", () => {
  const result = evaluateForecastAlert(withBaseline(80), quote(120), { now: REGULAR_NOW });
  assert.deepEqual(result.crossings.map((item) => item.boundary), ["P10", "P50", "P90"]);
});

test("disabled alert does not evaluate crossings", () => {
  const result = evaluateForecastAlert(withBaseline(99, { monitoredBoundaries: ["P50"], enabled: false }), quote(101), { now: REGULAR_NOW });
  assert.equal(result.reason, "disabled");
  assert.equal(result.crossings.length, 0);
});

test("global email setting disables crossing evaluation", () => {
  const result = evaluateForecastAlert(withBaseline(99, { monitoredBoundaries: ["P50"], globalEnabled: false }), quote(101), { now: REGULAR_NOW });
  assert.equal(result.reason, "disabled");
  assert.equal(result.crossings.length, 0);
});

test("expired forecast is stopped", () => {
  const result = evaluateForecastAlert(alert({ horizonEnd: "2026-08-21" }), quote(101), { now: REGULAR_NOW });
  assert.equal(result.status, "expired");
});

test("regular-hours alert rejects a premarket quote while extended-hours alert accepts it", () => {
  const now = new Date("2026-08-24T12:00:00.000Z");
  const premarket = quote(99, { timestamp: "2026-08-24T11:59:00.000Z", session: "premarket" });
  assert.equal(evaluateForecastAlert(alert({ sessionMode: "regular" }), premarket, { now }).reason, "outside_session");
  assert.equal(evaluateForecastAlert(alert({ sessionMode: "extended" }), premarket, { now }).reason, "checked");
});

test("a changed forecast-date boundary establishes a new baseline rather than a false crossing", () => {
  const existing = withBaseline(99, { monitoredBoundaries: ["P50"] });
  const nextDay = alert({
    monitoredBoundaries: ["P50"],
    boundaryStates: existing.boundaryStates,
    schedule: [{ timestamp: "2026-08-25T00:00:00.000Z", dateKey: "2026-08-25", values: { P50: 105 } }],
  });
  const result = evaluateForecastAlert(nextDay, quote(106, { timestamp: "2026-08-25T14:59:00Z" }), { now: new Date("2026-08-25T15:00:00Z") });
  assert.equal(result.crossings.length, 0);
  assert.equal(result.boundaryStates.P50.boundaryValue, 105);
});

class MemoryRepository implements ForecastAlertRepository {
  readonly alerts = new Map<string, ForecastAlertRecord>();
  readonly events = new Map<string, ForecastAlertEvent>();
  constructor(records: ForecastAlertRecord[]) { records.forEach((record) => this.alerts.set(record.id, structuredClone(record))); }
  async listActiveAlerts() { return [...this.alerts.values()].filter((record) => record.enabled); }
  async updateAlert(id: string, patch: Partial<ForecastAlertRecord>) { this.alerts.set(id, { ...this.alerts.get(id)!, ...patch }); }
  async createEventIfAbsent(event: ForecastAlertEvent) { if (this.events.has(event.id)) return false; this.events.set(event.id, structuredClone(event)); return true; }
  async listPendingEvents(alertId: string, nowMs: number) { return [...this.events.values()].filter((event) => event.alertId === alertId && event.status !== "sent" && event.notBeforeMs <= nowMs); }
  async updateEvent(_alertId: string, id: string, patch: Partial<ForecastAlertEvent>) { this.events.set(id, { ...this.events.get(id)!, ...patch }); }
}

const priceSource = (value: ForecastPriceQuote | null): ForecastAlertPriceSource => ({
  getLatestPrices: async () => value ? new Map([[value.symbol, value]]) : new Map(),
});

test("missing Alpaca price is recorded without email", async () => {
  const repository = new MemoryRepository([alert()]);
  let sends = 0;
  const result = await runForecastAlertMonitor({ repository, priceSource: priceSource(null), emailProvider: { sendBoundaryCrossing: async () => { sends += 1; return { messageId: "x" }; } }, now: REGULAR_NOW });
  assert.equal(result.missingPrices, 1);
  assert.equal(sends, 0);
  assert.equal(repository.alerts.get("run-1")?.lastError?.code, "no_data");
});

test("expired forecast is disabled without requiring an Alpaca quote", async () => {
  const repository = new MemoryRepository([alert({ horizonEnd: "2026-08-21" })]);
  let priceRequests = 0;
  const result = await runForecastAlertMonitor({
    repository,
    priceSource: { getLatestPrices: async () => { priceRequests += 1; return new Map(); } },
    emailProvider: { sendBoundaryCrossing: async () => ({ messageId: "unused" }) },
    now: REGULAR_NOW,
  });
  assert.equal(result.expiredAlerts, 1);
  assert.equal(result.missingPrices, 0);
  assert.equal(priceRequests, 0);
  assert.equal(repository.alerts.get("run-1")?.status, "expired");
  assert.equal(repository.alerts.get("run-1")?.enabled, false);
});

test("Alpaca failure is classified safely and monitoring remains retryable", async () => {
  const repository = new MemoryRepository([alert()]);
  const failingSource: ForecastAlertPriceSource = { getLatestPrices: async () => { const error = new Error("provider diagnostic") as Error & { code?: string }; error.code = "rate_limit"; throw error; } };
  const result = await runForecastAlertMonitor({ repository, priceSource: failingSource, emailProvider: { sendBoundaryCrossing: async () => ({ messageId: "x" }) }, now: REGULAR_NOW });
  assert.equal(result.errors, 1);
  assert.equal(repository.alerts.get("run-1")?.lastError?.message, "Market-data rate limit reached; monitoring will retry.");
});

test("email provider error keeps an event retryable", async () => {
  const crossedAlert = withBaseline(99, { monitoredBoundaries: ["P50"] });
  const repository = new MemoryRepository([crossedAlert]);
  const failingEmail: ForecastAlertEmailProvider = { sendBoundaryCrossing: async () => { const error = new Error("secret provider body") as Error & { code?: string }; error.code = "email_provider"; throw error; } };
  const result = await runForecastAlertMonitor({ repository, priceSource: priceSource(quote(101)), emailProvider: failingEmail, now: REGULAR_NOW });
  assert.equal(result.emailFailures, 1);
  assert.equal([...repository.events.values()][0].status, "failed");
});

test("successful email records a notification timestamp on each crossed boundary", async () => {
  const repository = new MemoryRepository([withBaseline(80)]);
  const result = await runForecastAlertMonitor({
    repository,
    priceSource: priceSource(quote(120)),
    emailProvider: { sendBoundaryCrossing: async () => ({ messageId: "message-1" }) },
    now: REGULAR_NOW,
  });
  assert.equal(result.emailsSent, 1);
  assert.equal(repository.alerts.get("run-1")?.boundaryStates.P10.lastNotificationAt, REGULAR_NOW.toISOString());
  assert.equal(repository.alerts.get("run-1")?.boundaryStates.P50.lastNotificationAt, REGULAR_NOW.toISOString());
  assert.equal(repository.alerts.get("run-1")?.boundaryStates.P90.lastNotificationAt, REGULAR_NOW.toISOString());
});

test("ownership check rejects another authenticated user", () => {
  assert.throws(() => assertForecastAlertOwner(alert(), "user-2"), /not_found/);
  assert.doesNotThrow(() => assertForecastAlertOwner(alert(), "user-1"));
});

test("professional email includes crossing values and private analysis link", () => {
  const event = buildForecastAlertEvent(alert(), evaluateForecastAlert(withBaseline(99, { monitoredBoundaries: ["P50"] }), quote(101), { now: REGULAR_NOW }).crossings, REGULAR_NOW);
  const email = buildForecastBoundaryEmail(event);
  assert.match(email.subject, /PLTR crossed above P50/);
  assert.match(email.text, /Previous price: \$99\.00/);
  assert.match(email.html, /Open private analysis/);
  assert.doesNotMatch(email.text + email.html, /ALPACA_API_KEY|RESEND_API_KEY/);
});

test("Resend provider sends with an idempotency key and does not expose its credential in the request body", async () => {
  let headers: HeadersInit | undefined;
  let body = "";
  const provider = new ResendForecastAlertEmailProvider({
    apiKey: "unit-test-resend-credential",
    fromEmail: "alerts@example.test",
    fetchImpl: (async (_url, init) => { headers = init?.headers; body = String(init?.body || ""); return Response.json({ id: "message-1" }); }) as typeof fetch,
  });
  const crossing = evaluateForecastAlert(withBaseline(99, { monitoredBoundaries: ["P50"] }), quote(101), { now: REGULAR_NOW }).crossings;
  const event = buildForecastAlertEvent(alert(), crossing, REGULAR_NOW);
  assert.equal((await provider.sendBoundaryCrossing(event)).messageId, "message-1");
  assert.match(JSON.stringify(headers), /Idempotency-Key/);
  assert.doesNotMatch(body, /unit-test-resend-credential/);
});
