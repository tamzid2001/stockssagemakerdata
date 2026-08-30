import assert from "node:assert/strict";
import test from "node:test";

import { StaticStructuredForecastProvider, WeightedForecastEnsemble } from "./quanturaForecastGeneration";
import { candidateIdempotencyKey, runCandidateGenerationJob, runResolutionBatch, validateCandidate } from "./quanturaForecastJobs";
import { StructuredThresholdResolver } from "./quanturaForecastResolvers";

const candidate = {
  providerId: "earnings-calendar",
  sourceCandidateId: "issuer-q3-2027",
  category: "earnings" as const,
  question: "Will Example Company report revenue above the frozen consensus threshold?",
  entity: { type: "company", id: "example", name: "Example Company", ticker: "EXM" },
  inputCutoffAt: "2026-08-30T10:00:00Z",
  resolutionDeadline: "2026-11-30T23:59:59Z",
  resolutionRule: "YES if the official issuer release reports revenue above the frozen consensus threshold; NO otherwise.",
  resolutionSource: "Official issuer release",
  evidence: [{ source: "Consensus snapshot", source_id: "consensus-1", observed_at: "2026-08-30T09:00:00Z" }],
  structuredInputs: { consensus: 100 },
};

test("candidate validation enforces point-in-time evidence and stable idempotency", () => {
  assert.equal(validateCandidate(candidate, "2026-08-30T12:00:00Z").inputCutoffAt, "2026-08-30T10:00:00.000Z");
  assert.equal(candidateIdempotencyKey(candidate), candidateIdempotencyKey({ ...candidate }));
  assert.throws(() => validateCandidate({ ...candidate, inputCutoffAt: "2026-08-31T00:00:00Z" }, "2026-08-30T12:00:00Z"), /cutoff_after/);
});

test("generation job is retry-safe and requires the numerical ensemble", async () => {
  const claims = new Set<string>();
  const store = {
    async claim(key: string) { if (claims.has(key)) return false; claims.add(key); return true; },
    async complete() {},
    async fail() {},
  };
  const ensemble = new WeightedForecastEnsemble([
    new StaticStructuredForecastProvider({ id: "model", providerType: "structured_model", numerical: true, probability: 0.62, weight: 1, modelName: "event-model", modelVersion: "1", method: "point_in_time", reasoning: "Frozen structured data.", evidenceIds: ["consensus-1"] }),
  ]);
  const created: string[] = [];
  const writer = { async createGeneratedDraft() { created.push("qf_generated"); return "qf_generated"; } };
  const providers = [{ id: "earnings-calendar", async discover() { return [candidate]; } }];
  const buildNarrative = async () => ({ question: candidate.question, possible_future_headline: "Example Company may report revenue above stored consensus", short_summary: "Prospective forecast.", bull_case: "Higher demand.", base_case: "Near consensus.", bear_case: "Lower demand.", reasoning_summary: "Evidence is mixed.", key_evidence_ids: ["consensus-1"], uncertainties: ["Reporting timing"], resolution_rule: candidate.resolutionRule });
  const first = await runCandidateGenerationJob({ providers, ensemble, store, writer, buildNarrative, asOf: "2026-08-30T12:00:00Z" });
  const replay = await runCandidateGenerationJob({ providers, ensemble, store, writer, buildNarrative, asOf: "2026-08-30T12:00:00Z" });
  assert.equal(first.processed, 1);
  assert.equal(replay.skipped, 1);
  assert.equal(created.length, 1);
});

test("resolution batch records deterministic outcomes and disputes", async () => {
  const store = { async claim() { return true; }, async complete() {}, async fail() {} };
  const decisions: string[] = [];
  const resolver = new StructuredThresholdResolver({ id: "issuer", version: "1", categories: ["earnings"], loadMetric: async () => ({ source: "Official issuer release", sourceId: "release-1", observedAt: "2026-11-01T12:00:00Z", value: 105, authoritative: true }) });
  const result = await runResolutionBatch({
    forecasts: [{ forecastId: "qf_1", value: { forecastId: "qf_1", category: "earnings", question: candidate.question, resolutionRule: candidate.resolutionRule, resolutionSource: candidate.resolutionSource, resolutionDeadline: candidate.resolutionDeadline, resolverConfig: { threshold: 100, operator: "gt" } } }],
    resolvers: [resolver],
    store,
    writer: { async applyResolution(_id, output) { decisions.push(output.decision); } },
  });
  assert.equal(result.processed, 1);
  assert.deepEqual(decisions, ["yes"]);
});
