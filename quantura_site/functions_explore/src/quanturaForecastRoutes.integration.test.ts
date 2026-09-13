import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import admin from "firebase-admin";
import type { Server } from "node:http";

import { registerQuanturaForecastRoutes } from "./quanturaForecastRoutes";
import { hashForecastApiKey, normalizeForecastDraft } from "./quanturaForecasts";
import { quanturaExploreApi } from "./index";
import { registerEnsembleForecastRoutes } from "./ensembleForecastRoutes";
import { generatePlatformApiKey, hashPlatformApiKey, workspaceMembershipId } from "./apiAccess";

const emulatorAvailable = Boolean(process.env.FIRESTORE_EMULATOR_HOST);

test("ensemble job persists inputs, claims two-bar market history, downloads, and checks current membership", { skip: !emulatorAvailable }, async () => {
  const firebaseApp = admin.initializeApp({ projectId: "quantura-forecast-integration" }, `ensemble-route-test-${Date.now()}`);
  const db = firebaseApp.firestore();
  const workspace = `ws_ensemble_${Date.now()}`;
  const owner = `${workspace}_owner`, viewer = `${workspace}_viewer`;
  const oldEnv = { ...process.env };
  process.env.QUANTURA_API_KEY_PEPPER = "integration-only-pepper-with-at-least-32-characters";
  process.env.QUANTURA_ENSEMBLE_WORKER_MODE = "manual";
  process.env.QUANTURA_ENSEMBLE_ALLOW_MANUAL_CLAIM = "true";
  process.env.TIMESFM_HF_ACCESS_APPROVED = "true";
  process.env.TIMESFM_COMMERCIAL_LICENSED = "true";
  const workerToken = "integration-only-worker-token-with-32-characters";
  process.env.QUANTURA_ENSEMBLE_WORKER_TOKEN = workerToken;
  const keys = [generatePlatformApiKey().rawKey, generatePlatformApiKey().rawKey];
  for (const [i, user] of [owner, viewer].entries()) {
    await db.collection("users").doc(user).set({ plan: i === 0 ? "free" : "quant" });
    await db.collection("quantura_api_keys").doc(hashPlatformApiKey(keys[i])).set({ user_id: user, name: "Integration only", scopes: ["forecasts:read", "forecasts:write"] });
  }
  await db.collection("workspaces").doc(workspace).set({ owner_user_id: owner, name: "Integration only" });
  const member = db.collection("workspace_memberships").doc(workspaceMembershipId(workspace, viewer));
  await member.set({ role: "viewer", status: "active" });
  const app = express(); app.use(express.json());
  const identity = { verifyIdToken: async () => { throw new Error("invalid_test_token"); }, getUser: async (uid: string) => ({ uid, email: uid === owner ? "administrator@example.test" : "viewer@example.test", emailVerified: true, disabled: false }) } as any;
  const router = express.Router(); registerEnsembleForecastRoutes(router, { db, auth: identity, adminEmails: ["administrator@example.test"], publicOrigin: "http://localhost" }); app.use("/api", router);
  const server = await new Promise<Server>(resolve => { const listening = app.listen(0, "127.0.0.1", () => resolve(listening)); });
  const address = server.address() as { port: number };
  const call = (path: string, token = keys[0], body?: unknown) => fetch(`http://127.0.0.1:${address.port}/api${path}`, { method: body === undefined ? "GET" : "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
  try {
    const capabilities = await call(`/v1/forecast/models?workspace_id=${workspace}`);
    assert.equal(capabilities.status, 200);
    assert.equal((await capabilities.json()).data.models.filter((model: any) => model.available).length, 5);
    assert.equal((await db.collection("users").doc(owner).get()).data()?.plan, "free", "admin access does not mutate billing");
    const request = { workspace_id: workspace, source: { type: "series", frequency: "1min", rows: Array.from({ length: 40 }, (_, i) => ({ timestamp: new Date(Date.UTC(2026, 0, 1, 0, i)).toISOString(), target: .4 })) }, prediction_length: 2, horizon_mode: "frequency_periods", quantiles: [.1, .5, .9], models: { prophet: { enabled: true, weight: 1 } } };
    assert.equal((await call("/v1/ensemble-forecasts", keys[1], request)).status, 403);
    assert.equal((await call("/v1/ensemble-forecasts", "invalid", request)).status, 401);
    const created = await call("/v1/ensemble-forecasts", keys[0], request);
    assert.equal(created.status, 202, await created.clone().text());
    const id = (await created.json()).data.forecast_id;
    const requestIndex = db.collection("users").doc(owner).collection("requests").doc(`ensemble__${id}`);
    assert.equal((await requestIndex.get()).data()?.sourceRef.id,id);
    await requestIndex.set({title:"My custom saved forecast",titleEdited:true},{merge:true});
    const ref = db.collection("ensemble_forecast_jobs").doc(id);
    assert.equal((await ref.collection("input_chunks").doc("0000").get()).data()?.rows.length, 40);
    // A server-verified prediction-market fixture exercises the trusted two-bar
    // claim boundary, independently of upstream provider availability in CI.
    await ref.update({ source: { type: "prediction_market", provider: "kalshi" }, "request.transform": "logit" });
    await ref.collection("input_chunks").doc("0000").set({ rows: request.source.rows.slice(-2) });
    const claimed = await call(`/internal/ensemble-forecasts/${id}/claim`, workerToken, {});
    assert.equal(claimed.status, 200, await claimed.clone().text());
    const job = (await claimed.json()).data;
    assert.equal(job.input.rows.length, 2); assert.equal(job.request.transform, "logit");
    assert.equal((await call(`/internal/ensemble-forecasts/${id}/claim`, workerToken, {})).status, 409);
    const result = { quantiles: [.1, .5, .9], predictions: [40, 41].map(i => ({ timestamp: new Date(Date.UTC(2026, 0, 1, 0, i)).toISOString(), quantiles: { "0.1": .2, "0.5": .4, "0.9": .6 } })), effective_weights_by_quantile: { "0.1": { prophet: 1 }, "0.5": { prophet: 1 }, "0.9": { prophet: 1 } }, models: ["prophet"], model_runs: [], transform: "logit", warnings: [], failures: [], dataset_hash: job.dataset_hash, prepared_series_hash: "fixture", result_hash: "fixture", runtime_seconds: 1, runtime: { test: true } };
    assert.equal((await call(`/internal/ensemble-forecasts/${id}/complete`, workerToken, result)).status, 200);
    assert.equal((await call(`/internal/ensemble-forecasts/${id}/complete`, workerToken, result)).status, 200, "lost completion response is safely retryable");
    const lateFailure = await call(`/internal/ensemble-forecasts/${id}/fail`, workerToken, {code:"LATE_CALLBACK",retryable:true});
    assert.equal((await lateFailure.json()).data.failed,false,"late failure cannot overwrite success");
    assert.equal((await call(`/internal/ensemble-forecasts/${id}/complete`, workerToken, {...result,result_hash:"different"})).status,409);
    assert.equal((await call(`/internal/ensemble-forecasts/${id}/progress`, workerToken, {completed_models:0,total_models:1})).status,409);
    const usage = await db.collection("ensemble_forecast_usage").where("workspace_id","==",workspace).get();
    assert.equal(usage.docs[0].data().active,0,"callbacks release the quota exactly once");
    assert.equal((await requestIndex.get()).data()?.title,"My custom saved forecast");
    assert.equal((await requestIndex.get()).data()?.outputsMeta.status,"completed");
    const saved = (await (await call(`/v1/ensemble-forecasts/${id}`,keys[0])).json()).data;
    assert.equal(saved.history.length,2);
    const downloaded = await call(`/v1/ensemble-forecasts/${id}/download?format=csv`, keys[1]);
    assert.equal(downloaded.status, 200); assert.match(await downloaded.text(), /timestamp,q_0.1,q_0.5,q_0.9/);
    const json = await call(`/v1/ensemble-forecasts/${id}/download?format=json`, keys[1]);
    assert.equal((await json.json()).predictions.length, 2);
    await member.update({ status: "removed" });
    assert.equal((await call(`/v1/ensemble-forecasts/${id}/observations`, keys[1])).status, 403);
    assert.equal((await call(`/v1/ensemble-forecasts/${id}`, keys[1])).status, 403);
    assert.equal((await call(`/v1/ensemble-forecasts/${id}/download`, keys[1])).status, 403);
    assert.equal((await call(`/v1/forecast/models?workspace_id=${viewer}`, keys[1])).status, 200);
    assert.equal((await call(`/v1/ensemble-forecasts/${id}`, keys[0])).status, 200);
    // Simulate a pre-migration partial write: the already persisted forecast
    // recovers on an authorized read without changing its numerical values.
    await ref.update({status:"running",lease_expires_at:new Date(Date.now()-1000).toISOString()});
    assert.equal((await (await call(`/v1/ensemble-forecasts/${id}`)).json()).data.status,"completed");
    assert.equal((await db.collection("ensemble_forecast_results").doc(id).get()).data()?.result_hash,"fixture");
    const expired = db.collection("ensemble_forecast_jobs").doc(`${id}_expired`);
    await expired.set({...(await ref.get()).data(),status:"running",lease_expires_at:new Date(Date.now()-1000).toISOString()});
    assert.equal((await (await call(`/v1/ensemble-forecasts/${expired.id}`)).json()).data.error.code,"WORKER_LEASE_EXPIRED");
    assert.equal((await call(`/internal/ensemble-forecasts/${expired.id}/claim`,workerToken,{})).status,409);
  } finally {
    await new Promise<void>(resolve => server.close(() => resolve()));
    for (const name of ["QUANTURA_API_KEY_PEPPER", "QUANTURA_ENSEMBLE_WORKER_MODE", "QUANTURA_ENSEMBLE_ALLOW_MANUAL_CLAIM", "QUANTURA_ENSEMBLE_WORKER_TOKEN", "TIMESFM_HF_ACCESS_APPROVED", "TIMESFM_COMMERCIAL_LICENSED"]) { if (oldEnv[name] === undefined) delete process.env[name]; else process.env[name] = oldEnv[name]; }
    await firebaseApp.delete();
  }
});

function assertSecurityHeaders(response: Response): void {
  assert.ok(response.headers.get("content-security-policy"));
  assert.ok(response.headers.get("strict-transport-security"));
  assert.equal(response.headers.get("x-content-type-options"), "nosniff");
  assert.equal(response.headers.get("cross-origin-resource-policy"), "same-origin");
  assert.equal(response.headers.get("x-powered-by"), null);
}

test("main API emits Helmet headers without breaking CORS or error responses", async () => {
  const server: Server = await new Promise((resolve) => {
    const listening = quanturaExploreApi.listen(0, "127.0.0.1", () => resolve(listening));
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("security_header_server_address_invalid");
  const origin = `http://127.0.0.1:${address.port}`;
  try {
    const health = await fetch(`${origin}/api/health`, {
      headers: { Origin: "https://quantura.studio" },
    });
    assert.equal(health.status, 200);
    assertSecurityHeaders(health);
    assert.equal(health.headers.get("access-control-allow-origin"), "https://quantura.studio");

    const missing = await fetch(`${origin}/definitely-missing`);
    assert.equal(missing.status, 404);
    assertSecurityHeaders(missing);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

async function waitForAuditRecords(
  collection: FirebaseFirestore.CollectionReference,
  minimum: number,
  timeoutMs = 3_000,
): Promise<FirebaseFirestore.QuerySnapshot> {
  const deadline = Date.now() + timeoutMs;
  let snapshot = await collection.get();
  while (snapshot.size < minimum && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25));
    snapshot = await collection.get();
  }
  return snapshot;
}

test("versioned API enforces authentication, scopes, revocation and public redaction", { skip: !emulatorAvailable }, async () => {
  const projectId = process.env.GOOGLE_CLOUD_PROJECT || "quantura-forecast-integration";
  const firebaseApp = admin.initializeApp({ projectId }, `forecast-route-test-${Date.now()}`);
  const db = firebaseApp.firestore();
  const pepper = "test-only-route-pepper-value";
  process.env.QUANTURA_FORECAST_API_KEY_PEPPER = pepper;
  const rawKey = `qf_test_${"a".repeat(44)}`;
  const keyId = hashForecastApiKey(rawKey, pepper);
  await db.collection("quantura_forecast_api_keys").doc(keyId).set({
    customer_id: "integration-customer",
    label: "Integration test",
    scopes: ["forecasts:read"],
    tier: "test",
    rate_limit_per_minute: 20,
    created_at: "2026-08-30T12:00:00.000Z",
    expires_at: null,
    revoked_at: null,
  });
  const normalized = normalizeForecastDraft({
    slug: "will-integration-company-beat-stored-consensus",
    category: "earnings",
    entity_type: "company",
    entity_id: "integration-company",
    entity_name: "Integration Company",
    ticker: "INTG",
    question: "Will Integration Company report revenue above its frozen consensus threshold?",
    possible_future_headline: "Integration Company may report revenue above stored consensus",
    short_summary: "A route-level prospective forecast fixture.",
    probability: 0.67,
    bull_case: "Demand is stronger than the frozen threshold.",
    base_case: "Results remain close to the threshold.",
    bear_case: "Reported demand misses the frozen threshold.",
    input_cutoff_at: "2026-08-30T10:00:00.000Z",
    resolution_deadline: "2026-11-30T23:59:59.000Z",
    model_provider: "quantura",
    model_name: "route-test",
    model_version: "1",
    forecast_method: "structured_test_fixture",
    reasoning_summary: "The fixture tests transport and redaction, not prediction quality.",
    evidence: [{ source: "Frozen fixture", source_id: "fixture-1", observed_at: "2026-08-30T09:00:00.000Z" }],
    resolution_rule: "YES if the official release reports revenue above the frozen threshold; NO otherwise.",
    resolution_source: "Official issuer release",
    created_by: "integration-test",
    review_status: "approved",
    private_strategy_json: { never_expose: "private-alpha" },
  }, new Date("2026-08-30T12:00:00.000Z"));
  const forecastId = "qf_integration";
  await db.collection("quantura_forecasts").doc(forecastId).set({
    ...normalized,
    status: "pending",
    is_public: true,
    published_at: "2026-08-30T12:00:00.000Z",
    initial_probability: 0.67,
    current_revision: 1,
  });
  await db.collection("quantura_forecast_slugs").doc(String(normalized.slug)).set({ forecast_id: forecastId });

  const app = express();
  app.use(express.json());
  const router = express.Router();
  registerQuanturaForecastRoutes(router, { db, auth: firebaseApp.auth(), adminEmails: [], publicOrigin: "http://127.0.0.1" });
  app.use("/api", router);
  const server: Server = await new Promise((resolve) => {
    const listening = app.listen(0, "127.0.0.1", () => resolve(listening));
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("integration_server_address_invalid");
  const origin = `http://127.0.0.1:${address.port}`;
  try {
    const publicCategories = await fetch(`${origin}/api/forecasts/public/categories`);
    assert.equal(publicCategories.status, 200);

    const publicForecast = await fetch(`${origin}/api/forecasts/public/${normalized.slug}`);
    const publicPayload = await publicForecast.json() as any;
    assert.equal(publicForecast.status, 200);
    assert.equal(publicPayload.data.disclosure, "THIS EVENT HAS NOT OCCURRED");
    assert.equal(JSON.stringify(publicPayload).includes("private-alpha"), false);

    const unauthorized = await fetch(`${origin}/api/v1/categories`);
    assert.equal(unauthorized.status, 401);
    assert.equal((await unauthorized.json() as any).error.code, "API_KEY_MISSING");

    const authorized = await fetch(`${origin}/api/v1/forecasts?status=pending`, { headers: { Authorization: `Bearer ${rawKey}` } });
    const authorizedPayload = await authorized.json() as any;
    assert.equal(authorized.status, 200);
    assert.equal(authorizedPayload.data.length, 1);
    assert.equal(JSON.stringify(authorizedPayload).includes("private-alpha"), false);

    const insufficient = await fetch(`${origin}/api/v1/calibration`, { headers: { "X-API-Key": rawKey } });
    assert.equal(insufficient.status, 403);
    assert.equal((await insufficient.json() as any).error.code, "INSUFFICIENT_SCOPE");

    await db.collection("quantura_forecast_api_keys").doc(keyId).update({ revoked_at: new Date().toISOString() });
    const revoked = await fetch(`${origin}/api/v1/categories`, { headers: { Authorization: `Bearer ${rawKey}` } });
    assert.equal(revoked.status, 401);
    assert.equal((await revoked.json() as any).error.code, "API_KEY_REVOKED");

    const audits = await waitForAuditRecords(db.collection("quantura_forecast_api_usage"), 4);
    assert.ok(audits.size >= 4);
    assert.equal(JSON.stringify(audits.docs.map((doc) => doc.data())).includes(rawKey), false);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await firebaseApp.delete();
    delete process.env.QUANTURA_FORECAST_API_KEY_PEPPER;
  }
});
