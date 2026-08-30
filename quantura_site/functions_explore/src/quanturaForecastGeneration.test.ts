import assert from "node:assert/strict";
import test from "node:test";

import { StaticStructuredForecastProvider, WeightedForecastEnsemble, validateLlmNarrative } from "./quanturaForecastGeneration";
import { StructuredThresholdResolver, resolveWithRegistry } from "./quanturaForecastResolvers";

const context = {
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

test("weighted ensemble traces each structured numerical contribution", async () => {
  const result = await new WeightedForecastEnsemble([
    new StaticStructuredForecastProvider({ id: "chronos", providerType: "time_series", numerical: true, probability: 0.7, weight: 2, modelName: "chronos-2", modelVersion: "2", method: "calibrated_threshold", reasoning: "Time-series threshold distribution.", evidenceIds: ["consensus-1"] }),
    new StaticStructuredForecastProvider({ id: "earnings-model", providerType: "structured_model", numerical: true, probability: 0.5, weight: 1, modelName: "earnings-logit", modelVersion: "1", method: "walk_forward_logit", reasoning: "Point-in-time earnings features.", evidenceIds: ["consensus-1"] }),
  ]).forecast(context);
  assert.ok(Math.abs(result.probability - 0.6333333333333333) < 1e-12);
  assert.equal(result.contributions.length, 2);
  assert.equal(result.forecastMethod, "weighted_provider_ensemble");
});

test("LLM cannot be the sole numerical forecast provider", async () => {
  const provider = {
    id: "llm",
    async forecast() { return { providerId: "llm", providerType: "llm_synthesis" as const, numerical: false, probability: 0.8, weight: 1, modelName: "gpt", modelVersion: "1", method: "synthesis", reasoning: "Narrative", evidenceIds: [] }; },
  };
  await assert.rejects(new WeightedForecastEnsemble([provider]).forecast(context), /structured_numerical_provider_required/);
});

test("LLM synthesis never changes the numerical ensemble probability", async () => {
  const numerical = new StaticStructuredForecastProvider({ id: "structured", providerType: "structured_model", numerical: true, probability: 0.6, weight: 1, modelName: "structured", modelVersion: "1", method: "point_in_time", reasoning: "Structured result.", evidenceIds: ["consensus-1"] });
  const llm = { id: "llm", async forecast() { return { providerId: "llm", providerType: "llm_synthesis" as const, numerical: false, probability: 0.99, weight: 100, modelName: "gpt", modelVersion: "1", method: "narrative_only", reasoning: "Narrative synthesis.", evidenceIds: ["consensus-1"] }; } };
  const result = await new WeightedForecastEnsemble([numerical, llm]).forecast(context);
  assert.equal(result.probability, 0.6);
  assert.equal(result.contributions.some((item) => item.providerId === "llm"), false);
});

test("LLM narrative cannot rewrite the formal question, rule, or invent evidence", () => {
  const valid = { question: context.question, possible_future_headline: "Example Company may exceed the stored threshold", short_summary: "Prospective forecast.", bull_case: "Higher demand.", base_case: "Near consensus.", bear_case: "Lower demand.", reasoning_summary: "Evidence is mixed.", key_evidence_ids: ["consensus-1"], uncertainties: ["Reporting timing"], resolution_rule: context.resolutionRule };
  assert.equal(validateLlmNarrative(valid, context).question, context.question);
  assert.throws(() => validateLlmNarrative({ ...valid, question: "Different?" }, context), /cannot_rewrite/);
  assert.throws(() => validateLlmNarrative({ ...valid, key_evidence_ids: ["invented"] }, context), /invented_evidence/);
});

test("structured resolver applies a frozen threshold and authoritative evidence", async () => {
  const resolver = new StructuredThresholdResolver({
    id: "issuer-revenue",
    version: "1",
    categories: ["earnings"],
    loadMetric: async () => ({ source: "Official release", sourceId: "release-1", observedAt: "2026-11-01T12:00:00Z", value: 105, authoritative: true }),
  });
  const result = await resolveWithRegistry({ forecastId: "qf_1", category: "earnings", question: context.question, resolutionRule: context.resolutionRule, resolutionSource: context.resolutionSource, resolutionDeadline: context.resolutionDeadline, resolverConfig: { threshold: 100, operator: "gt" } }, [resolver]);
  assert.equal(result.decision, "yes");
});

test("conflicting resolvers produce a dispute instead of forced resolution", async () => {
  const make = (id: string, value: number) => new StructuredThresholdResolver({ id, version: "1", categories: ["earnings"], loadMetric: async () => ({ source: id, sourceId: id, observedAt: "2026-11-01T12:00:00Z", value, authoritative: true }) });
  const result = await resolveWithRegistry({ forecastId: "qf_1", category: "earnings", question: context.question, resolutionRule: context.resolutionRule, resolutionSource: context.resolutionSource, resolutionDeadline: context.resolutionDeadline, resolverConfig: { threshold: 100, operator: "gt" } }, [make("a", 105), make("b", 95)]);
  assert.equal(result.decision, "disputed");
});
