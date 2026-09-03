import config from "./planEntitlements.json";

export type PlanKey = "free" | "pro" | "quant" | "research";
export type PlanEntitlement = {
  label: string;
  monthlyCents: number;
  annualCents: number;
  apiReadPerMinute: number;
  forecastComputePerDay: number;
  bulkExportsPerMonth: number;
  backtestsPerMonth: number;
  workspaceLimit: number;
  collaboratorSeats: number;
  features: string[];
};

export const PLAN_ENTITLEMENTS = config.plans as Record<PlanKey, PlanEntitlement>;
export const PLAN_ENTITLEMENTS_SCHEMA_VERSION = config.schemaVersion;

export function normalizePlan(value: unknown): PlanKey {
  const clean = String(value || "").trim().toLowerCase();
  if (clean in PLAN_ENTITLEMENTS) return clean as PlanKey;
  const alias = (config.legacyAliases as Record<string, PlanKey>)[clean];
  return alias || "free";
}

export function planHasFeature(plan: unknown, feature: string): boolean {
  return PLAN_ENTITLEMENTS[normalizePlan(plan)].features.includes(feature);
}

export function publicPlanEntitlements(): Record<string, unknown> {
  return {
    schemaVersion: PLAN_ENTITLEMENTS_SCHEMA_VERSION,
    plans: PLAN_ENTITLEMENTS,
  };
}
