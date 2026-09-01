import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizeEnsembleConfiguration,
  normalizeRequestedQuantiles,
  publicEnsembleJob,
  publicModelCapabilities,
  timesFmState,
  validateWorkerResult,
} from "./ensembleForecastRoutes";

const balancedModels = Object.fromEntries(
  ["prophet", "toto", "granite", "chronos", "timesfm"].map((id) => [id, { enabled: true, weight: 0.2 }])
);

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
