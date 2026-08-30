import crypto from "crypto";

import { EventContext, LlmForecastNarrative, WeightedForecastEnsemble, validateLlmNarrative } from "./quanturaForecastGeneration";
import { ForecastResolver, ResolverForecast, resolveWithRegistry } from "./quanturaForecastResolvers";
import { ForecastCategory, validateEvidenceCutoff } from "./quanturaForecasts";

export interface ForecastCandidate {
  providerId: string;
  sourceCandidateId: string;
  category: ForecastCategory;
  question: string;
  entity: { type: string; id: string; name: string; ticker?: string };
  inputCutoffAt: string;
  resolutionDeadline: string;
  resolutionRule: string;
  resolutionSource: string;
  evidence: EventContext["evidence"];
  structuredInputs: Record<string, unknown>;
}

export interface CandidateDiscoveryProvider {
  id: string;
  discover(asOf: string): Promise<ForecastCandidate[]>;
}

export interface ForecastJobStore {
  claim(idempotencyKey: string, metadata: Record<string, unknown>): Promise<boolean>;
  complete(idempotencyKey: string, result: Record<string, unknown>): Promise<void>;
  fail(idempotencyKey: string, safeErrorCode: string): Promise<void>;
}

export interface ForecastDraftWriter {
  createGeneratedDraft(
    candidate: ForecastCandidate,
    result: Awaited<ReturnType<WeightedForecastEnsemble["forecast"]>>,
    narrative: LlmForecastNarrative
  ): Promise<string>;
}

export interface ResolutionWriter {
  applyResolution(forecastId: string, result: Awaited<ReturnType<typeof resolveWithRegistry>>): Promise<void>;
}

export type ForecastJobSummary = {
  jobId: string;
  startedAt: string;
  completedAt: string;
  discovered: number;
  processed: number;
  skipped: number;
  failed: number;
  errors: Array<{ providerId: string; sourceCandidateId?: string; code: string }>;
};

function iso(value: string, field: string): string {
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) throw new Error(`${field}_invalid`);
  return new Date(parsed).toISOString();
}

function safeCode(error: unknown): string {
  return String((error as Error)?.message || "provider_failed")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .slice(0, 120) || "provider_failed";
}

export function candidateIdempotencyKey(candidate: ForecastCandidate): string {
  const stable = `${candidate.providerId}\u0000${candidate.sourceCandidateId}\u0000${candidate.question}\u0000${candidate.resolutionDeadline}`;
  return `candidate_${crypto.createHash("sha256").update(stable).digest("hex")}`;
}

export function validateCandidate(candidate: ForecastCandidate, asOf: string): ForecastCandidate {
  const cutoff = iso(candidate.inputCutoffAt, "candidate_input_cutoff");
  const deadline = iso(candidate.resolutionDeadline, "candidate_resolution_deadline");
  const normalizedAsOf = iso(asOf, "job_as_of");
  if (Date.parse(cutoff) > Date.parse(normalizedAsOf)) throw new Error("candidate_cutoff_after_job_as_of");
  if (Date.parse(deadline) <= Date.parse(cutoff)) throw new Error("candidate_deadline_must_follow_cutoff");
  if (!candidate.question.trim().endsWith("?")) throw new Error("candidate_formal_question_required");
  if (candidate.resolutionRule.trim().length < 30 || candidate.resolutionSource.trim().length < 3) {
    throw new Error("candidate_resolution_rule_required");
  }
  validateEvidenceCutoff(candidate.evidence, cutoff);
  return { ...candidate, inputCutoffAt: cutoff, resolutionDeadline: deadline };
}

export async function runCandidateGenerationJob(input: {
  providers: CandidateDiscoveryProvider[];
  ensemble: WeightedForecastEnsemble;
  store: ForecastJobStore;
  writer: ForecastDraftWriter;
  buildNarrative: (
    candidate: ForecastCandidate,
    result: Awaited<ReturnType<WeightedForecastEnsemble["forecast"]>>
  ) => Promise<unknown>;
  asOf: string;
  maxCandidates?: number;
}): Promise<ForecastJobSummary> {
  const startedAt = new Date().toISOString();
  const jobId = `forecast_generation_${crypto.createHash("sha256").update(iso(input.asOf, "job_as_of")).digest("hex").slice(0, 20)}`;
  const summary: ForecastJobSummary = { jobId, startedAt, completedAt: "", discovered: 0, processed: 0, skipped: 0, failed: 0, errors: [] };
  const maxCandidates = Math.min(Math.max(Math.floor(input.maxCandidates || 100), 1), 1000);
  for (const provider of input.providers) {
    let candidates: ForecastCandidate[] = [];
    try {
      candidates = (await provider.discover(iso(input.asOf, "job_as_of"))).slice(0, maxCandidates - summary.discovered);
    } catch (error) {
      summary.failed += 1;
      summary.errors.push({ providerId: provider.id, code: safeCode(error) });
      continue;
    }
    summary.discovered += candidates.length;
    for (const raw of candidates) {
      const key = candidateIdempotencyKey(raw);
      try {
        const candidate = validateCandidate(raw, input.asOf);
        const claimed = await input.store.claim(key, { provider_id: provider.id, source_candidate_id: candidate.sourceCandidateId, job_id: jobId });
        if (!claimed) {
          summary.skipped += 1;
          continue;
        }
        const context: EventContext = {
          category: candidate.category,
          question: candidate.question,
          entity: candidate.entity,
          inputCutoffAt: candidate.inputCutoffAt,
          resolutionDeadline: candidate.resolutionDeadline,
          resolutionRule: candidate.resolutionRule,
          resolutionSource: candidate.resolutionSource,
          evidence: candidate.evidence,
          structuredInputs: candidate.structuredInputs,
        };
        const result = await input.ensemble.forecast(context);
        const narrative = validateLlmNarrative(await input.buildNarrative(candidate, result), context);
        const forecastId = await input.writer.createGeneratedDraft(candidate, result, narrative);
        await input.store.complete(key, { forecast_id: forecastId, probability: result.probability, completed_at: new Date().toISOString() });
        summary.processed += 1;
      } catch (error) {
        const code = safeCode(error);
        await input.store.fail(key, code).catch(() => undefined);
        summary.failed += 1;
        summary.errors.push({ providerId: provider.id, sourceCandidateId: raw.sourceCandidateId, code });
      }
    }
    if (summary.discovered >= maxCandidates) break;
  }
  summary.completedAt = new Date().toISOString();
  return summary;
}

export async function runResolutionBatch(input: {
  forecasts: Array<{ forecastId: string; value: ResolverForecast }>;
  resolvers: ForecastResolver[];
  store: ForecastJobStore;
  writer: ResolutionWriter;
}): Promise<{ processed: number; skipped: number; failed: number; disputed: number }> {
  const summary = { processed: 0, skipped: 0, failed: 0, disputed: 0 };
  for (const forecast of input.forecasts) {
    const key = `resolution_${forecast.forecastId}_${crypto.createHash("sha256").update(forecast.value.resolutionDeadline).digest("hex").slice(0, 16)}`;
    try {
      if (!await input.store.claim(key, { forecast_id: forecast.forecastId })) {
        summary.skipped += 1;
        continue;
      }
      const result = await resolveWithRegistry(forecast.value, input.resolvers);
      if (result.decision === "unresolved") {
        await input.store.complete(key, { decision: "unresolved", retryable: true });
        summary.skipped += 1;
        continue;
      }
      await input.writer.applyResolution(forecast.forecastId, result);
      await input.store.complete(key, { decision: result.decision, completed_at: new Date().toISOString() });
      summary.processed += 1;
      if (result.decision === "disputed") summary.disputed += 1;
    } catch (error) {
      await input.store.fail(key, safeCode(error)).catch(() => undefined);
      summary.failed += 1;
    }
  }
  return summary;
}
