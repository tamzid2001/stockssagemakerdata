import { ForecastCategory } from "./quanturaForecasts";

export type ResolutionDecision = "yes" | "no" | "partial" | "void" | "disputed" | "unresolved";

export type ResolverForecast = {
  forecastId: string;
  category: ForecastCategory;
  question: string;
  resolutionRule: string;
  resolutionSource: string;
  resolutionDeadline: string;
  resolverConfig: Record<string, unknown>;
};

export type ResolutionEvidence = {
  source: string;
  sourceId: string;
  url?: string;
  observedAt: string;
  value?: string | number | boolean;
  authoritative: boolean;
};

export type ResolutionResult = {
  decision: ResolutionDecision;
  actualOutcome: string | null;
  notes: string;
  evidence: ResolutionEvidence[];
  resolverId: string;
  resolverVersion: string;
};

export interface ForecastResolver {
  readonly id: string;
  readonly version: string;
  readonly categories: readonly ForecastCategory[];
  resolve(forecast: ResolverForecast): Promise<ResolutionResult>;
}

function validEvidence(evidence: ResolutionEvidence[]): ResolutionEvidence[] {
  return evidence.filter((item) => item.authoritative && item.source.trim() && item.sourceId.trim() && Number.isFinite(Date.parse(item.observedAt)));
}

export async function resolveWithRegistry(
  forecast: ResolverForecast,
  resolvers: ForecastResolver[]
): Promise<ResolutionResult> {
  const eligible = resolvers.filter((resolver) => resolver.categories.includes(forecast.category));
  if (!eligible.length) return {
    decision: "unresolved",
    actualOutcome: null,
    notes: "No approved deterministic resolver is registered for this category.",
    evidence: [],
    resolverId: "registry",
    resolverVersion: "1",
  };
  const results = await Promise.all(eligible.map((resolver) => resolver.resolve(forecast)));
  const conclusive = results.filter((item) => ["yes", "no", "partial", "void"].includes(item.decision));
  const decisions = new Set(conclusive.map((item) => item.decision));
  if (decisions.size > 1) {
    return {
      decision: "disputed",
      actualOutcome: null,
      notes: "Approved resolvers returned conflicting outcomes; manual review is required.",
      evidence: results.flatMap((item) => validEvidence(item.evidence)),
      resolverId: "registry",
      resolverVersion: "1",
    };
  }
  if (!conclusive.length) return results[0];
  const selected = conclusive[0];
  if (!validEvidence(selected.evidence).length && selected.decision !== "void") {
    return { ...selected, decision: "disputed", actualOutcome: null, notes: "A conclusive outcome lacked authoritative structured evidence." };
  }
  return { ...selected, evidence: validEvidence(selected.evidence) };
}

export class StructuredThresholdResolver implements ForecastResolver {
  readonly id: string;
  readonly version: string;
  readonly categories: readonly ForecastCategory[];

  constructor(input: { id: string; version: string; categories: ForecastCategory[]; loadMetric: (forecast: ResolverForecast) => Promise<ResolutionEvidence> }) {
    this.id = input.id;
    this.version = input.version;
    this.categories = input.categories;
    this.loadMetric = input.loadMetric;
  }

  private readonly loadMetric: (forecast: ResolverForecast) => Promise<ResolutionEvidence>;

  async resolve(forecast: ResolverForecast): Promise<ResolutionResult> {
    const threshold = Number(forecast.resolverConfig.threshold);
    const operator = String(forecast.resolverConfig.operator || "");
    if (!Number.isFinite(threshold) || !["gt", "gte", "lt", "lte", "eq"].includes(operator)) {
      return { decision: "disputed", actualOutcome: null, notes: "The frozen threshold resolver configuration is invalid.", evidence: [], resolverId: this.id, resolverVersion: this.version };
    }
    const evidence = await this.loadMetric(forecast);
    const value = Number(evidence.value);
    if (!evidence.authoritative || !Number.isFinite(value)) {
      return { decision: "unresolved", actualOutcome: null, notes: "Authoritative structured outcome data is not available.", evidence: [evidence], resolverId: this.id, resolverVersion: this.version };
    }
    const yes = operator === "gt" ? value > threshold : operator === "gte" ? value >= threshold : operator === "lt" ? value < threshold : operator === "lte" ? value <= threshold : value === threshold;
    return {
      decision: yes ? "yes" : "no",
      actualOutcome: String(value),
      notes: `Resolved deterministically using frozen operator ${operator} and threshold ${threshold}.`,
      evidence: [evidence],
      resolverId: this.id,
      resolverVersion: this.version,
    };
  }
}
