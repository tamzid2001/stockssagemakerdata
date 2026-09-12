import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizeEnsembleConfiguration,
  normalizeRequestedQuantiles,
  publicEnsembleJob,
  publicModelCapabilities,
  timesFmState,
  validateWorkerResult,
  apiError,
  validateModelHistory,
} from "./ensembleForecastRoutes";
import { AlpacaError } from "./alpacaClient";
import { parseMarketLink } from "./marketLink";
import { forecastObservationWindow, PredictionMarketDataError, isMoneyline, gameTiming, resolveMarketLink } from "./predictionMarketData";

test("market links reject SSRF, lookalike origins, credentials, ports and traversal", () => {
  assert.deepEqual(parseMarketLink("https://kalshi.com/markets/kxmlbgame/mlb/kxmlbgame-26sep11abc"), { source: "kalshi", identifier: "KXMLBGAME-26SEP11ABC", kind: "either" });
  assert.equal(parseMarketLink("https://www.polymarket.us/event/game-123?utm_source=test").identifier, "game-123");
  for (const url of ["http://polymarket.us/event/game-1", "https://polymarket.us.evil.test/event/x", "https://localhost/event/x", "https://polymarket.com/event/game-1", "https://x:y@kalshi.com/markets/test", "https://kalshi.com:444/markets/test", "https://polymarket.us/event/%2e%2e%2fsecret"]) assert.throws(() => parseMarketLink(url));
});

test("observed forecast window caps 500 bars, retains both endpoints, never fills gaps or future observations", () => {
  const now = Date.parse("2026-09-11T12:00:00Z");
  const rows = Array.from({ length: 600 }, (_, i) => ({ timestamp: new Date(now - (599-i)*60_000).toISOString(), price: i % 2 ? 1 : 0 }));
  const full = forecastObservationWindow(rows, 60_000, now);
  assert.equal(full.rows.length, 500);
  assert.equal(full.rows.at(-1)?.timestamp, new Date(now).toISOString());
  assert.equal(full.rows[0].target, 0);
  const gaps = [...rows.slice(0, -3), ...rows.slice(-2), { timestamp: new Date(now+60_000).toISOString(), price: .5 }];
  assert.equal(forecastObservationWindow(gaps, 60_000, now).rows.length, 500);
  assert.equal(forecastObservationWindow(gaps, 60_000, now).gap_count, 1);
  assert.throws(() => forecastObservationWindow([{ timestamp: new Date(now).toISOString(), price: null }], 60_000, now), /two observed/);
});

test("open Kalshi is not a live game without an official start; props are not moneylines", () => {
  const contract = { source: "kalshi", status: "open", eventStart: null, league: "KXMLBGAME" } as any;
  assert.equal(gameTiming(contract), "open"); assert.equal(isMoneyline(contract), true);
  assert.equal(isMoneyline({ ...contract, league: "KXMLBTOTAL" }), false);
  assert.equal(gameTiming({ ...contract, eventStart: new Date(Date.now()-60_000).toISOString() }), "in_progress");
  assert.equal(gameTiming({ ...contract, status: "settled", live: true }), "closed");
});

test("safe provider errors retain actionable HTTP status instead of generic invalid request", () => {
  assert.deepEqual(apiError(new AlpacaError("rate_limit", "Provider rate limited. Retry later.", 429)), { status: 429, code: "RATE_LIMIT", message: "Provider rate limited. Retry later." });
  assert.equal(apiError(new PredictionMarketDataError("contract_not_found", "Select the side again.", 422)).message, "Select the side again.");
});

test("event URL resolution preserves team sides and selects moneylines over unrelated props", async () => {
  const original = globalThis.fetch;
  const urls: string[] = [];
  globalThis.fetch = (async (input: any) => {
    urls.push(String(input));
    return Response.json({ event: { id: "event-fixture", title: "A vs B", markets: [
      { id: "m1", slug: "game-fixture", sportsMarketTypeV2: "SPORTS_MARKET_TYPE_MONEYLINE", marketSides: [{ id: "side-a", long: true, description: "A" }, { id: "side-b", long: false, description: "B" }] },
      { id: "m2", slug: "prop-fixture", sportsMarketTypeV2: "SPORTS_MARKET_TYPE_PROP", marketSides: [{ id: "prop", long: true, description: "Prop" }] },
    ] } });
  }) as typeof fetch;
  try {
    const rows = await resolveMarketLink("https://polymarket.us/event/test-event-fixture");
    assert.deepEqual(rows.map(c => [c.contractId, c.side]), [["side-a", "long"], ["side-b", "short"]]);
    assert.equal(new URL(urls[0]).origin, "https://gateway.polymarket.us");
    assert.equal(rows[0].eventId, "event-fixture");
  } finally { globalThis.fetch = original; }
});

const balancedModels = Object.fromEntries(
  ["prophet", "toto", "granite", "chronos", "timesfm"].map((id) => [id, { enabled: true, weight: 0.2 }])
);

test("short observed histories fail before dispatch unless the user permits a viable reduced ensemble", () => {
  const models = { prophet: { enabled: true, weight: 1 }, toto: { enabled: true, weight: 1 } };
  const strict = normalizeEnsembleConfiguration({ models }, "quant");
  assert.throws(() => validateModelHistory(strict, 2), (error: unknown) => {
    const response = apiError(error);
    return response.status === 422 && response.code === "MODEL_CONTEXT_TOO_SHORT" && response.message.includes("32 observed values");
  });
  assert.doesNotThrow(() => validateModelHistory(strict, 32));
  const flexible = normalizeEnsembleConfiguration({ models, failure_policy: "renormalize" }, "quant");
  assert.doesNotThrow(() => validateModelHistory(flexible, 2));
  assert.equal(flexible.models.toto.enabled, true, "preflight must not silently rewrite the requested models");
  const onlyToto = normalizeEnsembleConfiguration({ models: { prophet: { enabled: false }, toto: { enabled: true, weight: 1 } }, quantiles: [.1, .5, .9], failure_policy: "renormalize" }, "quant");
  assert.throws(() => validateModelHistory(onlyToto, 2), /No enabled positive-weight model/);
  const zeroToto = normalizeEnsembleConfiguration({ models: { ...models, toto: { enabled: true, weight: 0 } } }, "quant");
  assert.doesNotThrow(() => validateModelHistory(zeroToto, 2));
  assert.equal((publicModelCapabilities("quant").models as any[]).find(m => m.id === "toto").minimum_observed_context, 32);
});

test("custom quantiles are sorted and deduplicated without rounding collisions", () => {
  assert.deepEqual(normalizeRequestedQuantiles([0.75, 0.123456, 0.1, 0.123456, 0.5]), [0.1, 0.123456, 0.5, 0.75]);
  assert.throws(() => normalizeRequestedQuantiles([0, 0.5]), /quantile_invalid/);
});

test("balanced central weights normalize to 20 percent each", () => {
  process.env.TIMESFM_HF_ACCESS_APPROVED = "true";
  process.env.TIMESFM_COMMERCIAL_LICENSED = "true";
  const config = normalizeEnsembleConfiguration({ models: balancedModels, quantiles: [0.01, 0.5, 0.99], prediction_length: 30 }, "quant");
  assert.deepEqual(config.effective_central_weights, {
    prophet: 0.2,
    toto: 0.2,
    granite: 0.2,
    chronos: 0.2,
    timesfm: 0.2,
  });
});

test("Toto and TimesFM alone cannot satisfy a P01 request", () => {
  process.env.TIMESFM_HF_ACCESS_APPROVED = "true";
  process.env.TIMESFM_COMMERCIAL_LICENSED = "true";
  const models = Object.fromEntries(
    ["prophet", "toto", "granite", "chronos", "timesfm"].map((id) => [id, { enabled: id === "toto" || id === "timesfm", weight: 1 }])
  );
  assert.throws(
    () => normalizeEnsembleConfiguration({ models, quantiles: [0.01], prediction_length: 5 }, "quant"),
    /quantile_0.01_unsupported/
  );
});

test("weights reject negative and NaN values", () => {
  assert.throws(
    () => normalizeEnsembleConfiguration({ models: { prophet: { enabled: true, weight: -1 } }, quantiles: [0.5] }, "free"),
    /prophet_weight_invalid/
  );
  assert.throws(
    () => normalizeEnsembleConfiguration({ models: { prophet: { enabled: true, weight: Number.NaN } }, quantiles: [0.5] }, "free"),
    /prophet_weight_invalid/
  );
  assert.throws(
    () => normalizeEnsembleConfiguration({ models: { prophet: { enabled: true, weight: 1 } }, quantiles: [0.5], model_checkpoints: { prophet: "unapproved/checkpoint" } }, "free"),
    /configuration_field_unsupported/
  );
});

test("foundation models are plan gated", () => {
  assert.throws(
    () => normalizeEnsembleConfiguration({ models: { toto: { enabled: true, weight: 1 } }, quantiles: [0.5] }, "pro"),
    /toto_required_entitlement/
  );
});

test("TimesFM production license flag is independent from access approval", () => {
  const originalNodeEnv = process.env.NODE_ENV;
  process.env.NODE_ENV = "production";
  process.env.TIMESFM_HF_ACCESS_APPROVED = "true";
  delete process.env.TIMESFM_COMMERCIAL_LICENSED;
  process.env.ALLOW_NONCOMMERCIAL_TIMESFM = "true";
  assert.deepEqual(timesFmState("production"), { available: false, unavailable_reason: "commercial_license_required", evaluation_only: false });
  const capability = publicModelCapabilities("research");
  const timesfm = (capability.models as Array<Record<string, unknown>>).find((model) => model.id === "timesfm");
  assert.equal(timesfm?.available, false);
  assert.equal(timesfm?.unavailable_reason, "commercial_license_required");
  process.env.NODE_ENV = originalNodeEnv;
});

test("public job schema exposes ensemble output without component arrays or inputs", () => {
  const job = publicEnsembleJob(
    "job_1",
    {
      status: "completed",
      workspace_id: "workspace_1",
      request: { prediction_length: 2, horizon_mode: "trading_sessions", quantiles: [0.25, 0.5, 0.75], transform: "log", models: balancedModels },
      source: { type: "ticker", symbol: "AAPL" },
      requested_weights: {},
      effective_central_weights: {},
    },
    { predictions: [{ timestamp: "2026-09-02", quantiles: { "0.5": 100 } }], effective_weights_by_quantile: {}, models: [{ id: "prophet" }] }
  );
  assert.ok(Array.isArray(job.predictions));
  assert.equal("input" in job, false);
  assert.equal("model_runs" in job, false);
});

test("worker result validation rejects crossed quantiles and invalid effective weights", () => {
  const job = { request: { prediction_length: 1, horizon_mode: "trading_sessions", quantiles: [0.25, 0.5, 0.75] } };
  const valid = {
    quantiles: [0.25, 0.5, 0.75],
    transform: "none",
    predictions: [{ timestamp: "2026-09-02T00:00:00Z", quantiles: { "0.25": 90, "0.5": 100, "0.75": 110 } }],
    effective_weights_by_quantile: {
      "0.25": { prophet: 1 }, "0.5": { prophet: 1 }, "0.75": { prophet: 1 },
    },
  };
  assert.equal(validateWorkerResult(valid, job).predictions.length, 1);
  assert.throws(() => validateWorkerResult({ ...valid, predictions: [{ timestamp: "2026-09-02T00:00:00Z", quantiles: { "0.25": 110, "0.5": 100, "0.75": 90 } }] }, job), /ordering/);
  assert.throws(() => validateWorkerResult({ ...valid, effective_weights_by_quantile: { ...valid.effective_weights_by_quantile, "0.5": { prophet: 0.8 } } }, job), /weights/);
});
