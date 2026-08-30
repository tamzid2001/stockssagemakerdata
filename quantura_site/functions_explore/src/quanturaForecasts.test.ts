import assert from "node:assert/strict";
import test from "node:test";

import {
  assertStatusTransition,
  brierScore,
  buildForecastSearchTokens,
  buildCalibrationRows,
  buildPublishedSnapshot,
  createForecastApiKey,
  decodeCursor,
  encodeCursor,
  enterpriseForecastProjection,
  hashForecastApiKey,
  hasRequiredScope,
  normalizeForecastDraft,
  normalizeForecastAmendment,
  normalizeProbabilityRevision,
  publicForecastProjection,
  sanitizeDatasetRecord,
  validateEvidenceCutoff,
  validateApiKeyRecord,
  validateProbability,
  validateScopes,
} from "./quanturaForecasts";

const NOW = new Date("2026-08-30T12:00:00.000Z");
const evidence = [{ source: "Issuer release", observed_at: "2026-08-30T10:00:00.000Z", url: "https://example.com/source" }];

function draft(overrides: Record<string, unknown> = {}) {
  return normalizeForecastDraft({
    slug: "will-example-company-beat-stored-consensus",
    category: "earnings",
    entity_type: "company",
    entity_id: "example-company",
    entity_name: "Example Company",
    ticker: "EXM",
    question: "Will Example Company report revenue above the consensus snapshot stored at creation?",
    possible_future_headline: "Example Company may report revenue above stored consensus",
    short_summary: "A prospective earnings forecast based on a frozen consensus snapshot.",
    probability: 0.71,
    bull_case: "Demand exceeds the stored consensus assumptions.",
    base_case: "Reported revenue remains near the stored consensus.",
    bear_case: "Demand or delivery timing falls short.",
    input_cutoff_at: "2026-08-30T11:00:00.000Z",
    resolution_deadline: "2026-11-30T23:59:59.000Z",
    model_provider: "quantura",
    model_name: "event-ensemble",
    model_version: "1.0.0",
    forecast_method: "weighted_structured_provider_ensemble",
    reasoning_summary: "Structured earnings evidence supports a probability above one half.",
    evidence,
    resolution_rule: "YES if the official issuer earnings release reports revenue above the frozen consensus value; NO otherwise.",
    resolution_source: "Official issuer earnings release",
    created_by: "admin-user",
    review_status: "approved",
    ...overrides,
  } as never, NOW);
}

test("probability and Brier score use the 0-1 scale", () => {
  assert.equal(validateProbability(0.71), 0.71);
  assert.throws(() => validateProbability(71), /between_zero_and_one/);
  assert.ok(Math.abs(brierScore(0.7, "yes") - 0.09) < 1e-12);
  assert.ok(Math.abs(brierScore(0.7, "no") - 0.49) < 1e-12);
});

test("draft requires a formal question and objective resolution rule", () => {
  assert.throws(() => draft({ question: "Likely headline" }), /formal_forecast_question_required/);
  assert.throws(() => draft({ resolution_rule: "Check later" }), /objective_resolution_rule_required/);
});

test("forecast evidence cannot be newer than the input cutoff", () => {
  assert.throws(
    () => validateEvidenceCutoff([{ source: "Future", observed_at: "2026-08-31T00:00:00Z" }], "2026-08-30T11:00:00Z"),
    /evidence_after_input_cutoff/
  );
});

test("normalized evidence is Firestore-safe and omits undefined optional fields", () => {
  const [normalized] = validateEvidenceCutoff([{ source: "Source", observed_at: "2026-08-30T10:00:00Z" }], "2026-08-30T11:00:00Z");
  assert.equal(Object.values(normalized).includes(undefined), false);
  assert.equal("url" in normalized, false);
});

test("search tokens normalize forecast entities without exposing full blobs", () => {
  assert.deepEqual(buildForecastSearchTokens(["NVIDIA Corporation", "NVDA", "Products & Technology"]), [
    "nvidia",
    "corporation",
    "nvda",
    "products",
    "technology",
  ]);
  assert.ok((draft().search_tokens as string[]).includes("example"));
});

test("political forecasts reject damaging private allegations", () => {
  assert.throws(
    () => draft({
      category: "politics_policy",
      question: "Will an official secretly commit an illegal act before the policy deadline?",
    }),
    /politics_forecast_disallowed_claim/
  );
});

test("published snapshot freezes initial probability and resolution text", () => {
  const value = draft();
  const snapshot = buildPublishedSnapshot("qf_test", value, "2026-08-30T12:00:00Z");
  assert.equal(snapshot.probability, 0.71);
  assert.equal(typeof snapshot.snapshot_hash, "string");
  assert.throws(() => buildPublishedSnapshot("qf_test", { ...value, review_status: "needs_review" }, NOW.toISOString()), /approval_required/);
});

test("probability revisions are append-only deltas with monotonic cutoffs", () => {
  const current = { ...draft(), status: "pending", current_revision: 1, current_probability: 0.71 };
  const revision = normalizeProbabilityRevision({
    probability: 0.77,
    reasoning_delta: "New structured guidance increased the probability.",
    input_cutoff_at: "2026-08-30T11:30:00Z",
    model_provider: "quantura",
    model_name: "event-ensemble",
    model_version: "1.0.1",
    evidence: [{ source: "Guidance", observed_at: "2026-08-30T11:15:00Z" }],
  }, current, NOW);
  assert.equal(revision.revision, 2);
  assert.ok(Math.abs(Number(revision.probability_delta) - 0.06) < 1e-12);
  assert.equal(current.current_probability, 0.71);
});

test("published corrections are transparent amendments and cannot rewrite core history", () => {
  const current = { ...draft(), status: "pending" };
  const amendment = normalizeForecastAmendment({
    field: "short_summary",
    reason: "Clarify a display-only wording issue.",
    corrected_display_value: "Clarified summary",
    note: "The original probability and resolution rule remain unchanged.",
  }, current, "admin-user", NOW);
  assert.equal(amendment.immutable, true);
  assert.equal(amendment.created_by, "admin-user");
  assert.throws(() => normalizeForecastAmendment({ field: "probability", reason: "Change old number", note: "Rewrite historical probability" }, current, "admin-user", NOW), /cannot_rewrite/);
});

test("status transitions prevent rewriting resolved forecasts", () => {
  assert.doesNotThrow(() => assertStatusTransition("pending", "resolved_yes"));
  assert.throws(() => assertStatusTransition("resolved_yes", "pending"), /invalid_forecast_status_transition/);
  assert.throws(() => assertStatusTransition("void", "pending"), /invalid_forecast_status_transition/);
});

test("public and enterprise serializers exclude private strategy fields", () => {
  const value = { ...draft({ private_strategy_json: { alpha: "secret" } }), status: "pending" };
  const publicValue = publicForecastProjection("qf_test", value);
  const enterpriseValue = enterpriseForecastProjection("qf_test", value);
  assert.equal(publicValue.event_has_not_occurred, true);
  assert.equal(publicValue.disclosure, "THIS EVENT HAS NOT OCCURRED");
  assert.equal("private_strategy_json" in publicValue, false);
  assert.equal("private_strategy_json" in enterpriseValue, false);
});

test("API scopes are validated and admin implies every scope", () => {
  assert.deepEqual(validateScopes(["forecasts:read", "forecasts:read"]), ["forecasts:read"]);
  assert.throws(() => validateScopes(["billing:write"]), /scope_invalid/);
  assert.equal(hasRequiredScope(["forecasts:admin"], "forecasts:bulk"), true);
  assert.equal(hasRequiredScope(["forecasts:read"], "forecasts:history"), false);
});

test("revoked and expired API-key records lose access immediately", () => {
  const active = { scopes: ["forecasts:read"], expires_at: "2026-08-31T12:00:00Z", revoked_at: null };
  assert.deepEqual(validateApiKeyRecord(active, "forecasts:read", NOW), ["forecasts:read"]);
  assert.throws(() => validateApiKeyRecord({ ...active, revoked_at: "2026-08-30T11:00:00Z" }, "forecasts:read", NOW), /revoked/);
  assert.throws(() => validateApiKeyRecord({ ...active, expires_at: "2026-08-30T11:59:59Z" }, "forecasts:read", NOW), /expired/);
  assert.throws(() => validateApiKeyRecord(active, "forecasts:bulk", NOW), /insufficient_scope/);
});

test("API keys are opaque and keyed HMAC hashes are deterministic", () => {
  const created = createForecastApiKey();
  assert.match(created.rawKey, /^qf_live_/);
  const first = hashForecastApiKey(created.rawKey, "test-only-pepper-value");
  const second = hashForecastApiKey(created.rawKey, "test-only-pepper-value");
  assert.equal(first, second);
  assert.notEqual(first, created.rawKey);
});

test("cursor pagination round trips and rejects malformed cursors", () => {
  const cursor = encodeCursor("2026-08-30T12:00:00Z", "qf_test");
  assert.deepEqual(decodeCursor(cursor), { created_at: "2026-08-30T12:00:00.000Z", id: "qf_test" });
  assert.throws(() => decodeCursor("not-a-cursor"), /cursor_invalid/);
});

test("calibration buckets report predicted and realized frequencies", () => {
  const rows = buildCalibrationRows([
    { current_probability: 0.72, status: "resolved_yes", brier_score: 0.0784 },
    { current_probability: 0.78, status: "resolved_no", brier_score: 0.6084 },
  ]);
  const bucket = rows.find((item) => item.bucket === "70-80%")!;
  assert.equal(bucket.forecast_count, 2);
  assert.equal(bucket.predicted_average_probability, 0.75);
  assert.equal(bucket.actual_event_frequency, 0.5);
});

test("training dataset projection preserves trajectory but excludes alpha", () => {
  const value = { ...draft({ private_strategy_json: { feature_weights: [1, 2] } }), status: "resolved_yes", actual_outcome: "yes" };
  const record = sanitizeDatasetRecord("qf_test", value, [{ revision: 1, probability: 0.71, created_at: NOW.toISOString() }]);
  assert.equal((record.probability_history as unknown[]).length, 1);
  assert.equal("private_strategy_json" in record, false);
});
