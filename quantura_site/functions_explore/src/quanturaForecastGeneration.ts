import { ForecastCategory, ForecastEvidence, validateEvidenceCutoff, validateProbability } from "./quanturaForecasts";

export type EventContext = {
  category: ForecastCategory;
  question: string;
  entity: { type: string; id: string; name: string; ticker?: string; league?: string; team?: string };
  inputCutoffAt: string;
  resolutionDeadline: string;
  resolutionRule: string;
  resolutionSource: string;
  evidence: ForecastEvidence[];
  structuredInputs: Record<string, unknown>;
};

export type ProviderForecast = {
  providerId: string;
  providerType: "structured_model" | "market_data" | "time_series" | "llm_synthesis";
  numerical: boolean;
  probability: number;
  weight: number;
  modelName: string;
  modelVersion: string;
  method: string;
  reasoning: string;
  evidenceIds: string[];
  privateDetails?: Record<string, unknown>;
};

export type EventForecastResult = {
  probability: number;
  confidenceLabel: "low" | "medium" | "high";
  forecastMethod: "weighted_provider_ensemble";
  contributions: Array<{
    providerId: string;
    providerType: ProviderForecast["providerType"];
    probability: number;
    normalizedWeight: number;
    modelName: string;
    modelVersion: string;
    method: string;
    evidenceIds: string[];
  }>;
  reasoningSummary: string;
  providerFailures: Array<{ providerId: string; error: string }>;
};

export interface ForecastProvider {
  readonly id: string;
  forecast(context: EventContext): Promise<ProviderForecast>;
}

export type LlmForecastNarrative = {
  question: string;
  possible_future_headline: string;
  short_summary: string;
  bull_case: string;
  base_case: string;
  bear_case: string;
  reasoning_summary: string;
  key_evidence_ids: string[];
  uncertainties: string[];
  resolution_rule: string;
};

function text(value: unknown, max: number): string {
  return String(value ?? "").trim().slice(0, max);
}

export function validateEventContext(context: EventContext): EventContext {
  if (!context.question.trim().endsWith("?") || context.question.trim().length < 20) throw new Error("formal_event_question_required");
  if (context.resolutionRule.trim().length < 30 || !context.resolutionSource.trim()) throw new Error("formal_resolution_required");
  if (Date.parse(context.resolutionDeadline) <= Date.parse(context.inputCutoffAt)) throw new Error("resolution_deadline_invalid");
  return { ...context, evidence: validateEvidenceCutoff(context.evidence, context.inputCutoffAt) };
}

export function validateLlmNarrative(value: unknown, context: EventContext): LlmForecastNarrative {
  const row = value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
  const narrative: LlmForecastNarrative = {
    question: text(row.question, 1000),
    possible_future_headline: text(row.possible_future_headline, 400),
    short_summary: text(row.short_summary, 700),
    bull_case: text(row.bull_case, 1200),
    base_case: text(row.base_case, 1200),
    bear_case: text(row.bear_case, 1200),
    reasoning_summary: text(row.reasoning_summary, 2500),
    key_evidence_ids: Array.isArray(row.key_evidence_ids) ? row.key_evidence_ids.map((item) => text(item, 220)).filter(Boolean).slice(0, 30) : [],
    uncertainties: Array.isArray(row.uncertainties) ? row.uncertainties.map((item) => text(item, 500)).filter(Boolean).slice(0, 20) : [],
    resolution_rule: text(row.resolution_rule, 3000),
  };
  if (narrative.question !== context.question) throw new Error("llm_cannot_rewrite_formal_question");
  if (narrative.resolution_rule !== context.resolutionRule) throw new Error("llm_cannot_rewrite_resolution_rule");
  if (!narrative.possible_future_headline || !narrative.reasoning_summary || !narrative.bull_case || !narrative.base_case || !narrative.bear_case) {
    throw new Error("llm_narrative_schema_invalid");
  }
  const availableIds = new Set(context.evidence.map((item) => item.source_id).filter(Boolean));
  if (narrative.key_evidence_ids.some((id) => !availableIds.has(id))) throw new Error("llm_invented_evidence_reference");
  return narrative;
}

export class WeightedForecastEnsemble {
  constructor(private readonly providers: ForecastProvider[]) {
    if (!providers.length) throw new Error("forecast_provider_required");
  }

  async forecast(contextInput: EventContext): Promise<EventForecastResult> {
    const context = validateEventContext(contextInput);
    const settled = await Promise.allSettled(this.providers.map((provider) => provider.forecast(context)));
    const successful: ProviderForecast[] = [];
    const failures: EventForecastResult["providerFailures"] = [];
    settled.forEach((result, index) => {
      const providerId = this.providers[index].id;
      if (result.status === "rejected") {
        failures.push({ providerId, error: "provider_failed" });
        return;
      }
      const value = result.value;
      if (value.providerId !== providerId) throw new Error("provider_identity_mismatch");
      successful.push({ ...value, probability: validateProbability(value.probability), weight: Number(value.weight) });
    });
    if (!successful.some((item) => item.numerical && item.providerType !== "llm_synthesis")) {
      throw new Error("structured_numerical_provider_required");
    }
    const weighted = successful.filter((item) => item.numerical && item.providerType !== "llm_synthesis" && Number.isFinite(item.weight) && item.weight > 0);
    const totalWeight = weighted.reduce((sum, item) => sum + item.weight, 0);
    if (totalWeight <= 0) throw new Error("provider_weight_required");
    const contributions = weighted.map((item) => ({
      providerId: item.providerId,
      providerType: item.providerType,
      probability: item.probability,
      normalizedWeight: item.weight / totalWeight,
      modelName: item.modelName,
      modelVersion: item.modelVersion,
      method: item.method,
      evidenceIds: item.evidenceIds.slice(0, 50),
    }));
    const probability = contributions.reduce((sum, item) => sum + item.probability * item.normalizedWeight, 0);
    const dispersion = Math.max(...contributions.map((item) => item.probability)) - Math.min(...contributions.map((item) => item.probability));
    const confidenceLabel = dispersion <= 0.1 && weighted.length >= 2 ? "high" : dispersion <= 0.25 ? "medium" : "low";
    return {
      probability,
      confidenceLabel,
      forecastMethod: "weighted_provider_ensemble",
      contributions,
      reasoningSummary: weighted.map((item) => `${item.providerId}: ${text(item.reasoning, 400)}`).join(" "),
      providerFailures: failures,
    };
  }
}

export class StaticStructuredForecastProvider implements ForecastProvider {
  readonly id: string;

  constructor(private readonly configuration: Omit<ProviderForecast, "providerId"> & { id: string }) {
    this.id = configuration.id;
  }

  async forecast(_context: EventContext): Promise<ProviderForecast> {
    if (this.configuration.providerType === "llm_synthesis") throw new Error("static_llm_provider_not_allowed");
    return { ...this.configuration, providerId: this.id };
  }
}
