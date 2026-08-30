import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import admin from "firebase-admin";
import type { Server } from "node:http";

import { registerQuanturaForecastRoutes } from "./quanturaForecastRoutes";
import { hashForecastApiKey, normalizeForecastDraft } from "./quanturaForecasts";

const emulatorAvailable = Boolean(process.env.FIRESTORE_EMULATOR_HOST);

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

    const audits = await db.collection("quantura_forecast_api_usage").get();
    assert.ok(audits.size >= 4);
    assert.equal(JSON.stringify(audits.docs.map((doc) => doc.data())).includes(rawKey), false);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await firebaseApp.delete();
    delete process.env.QUANTURA_FORECAST_API_KEY_PEPPER;
  }
});
