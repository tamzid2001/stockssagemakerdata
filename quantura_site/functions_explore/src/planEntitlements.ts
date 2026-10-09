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

// Existing free preview access and historical billing identifiers stay intact.
// New Pro checkout prices come from the server-owned configuration below.
const allFeatures = config.plans.free.features;
export const PLAN_ENTITLEMENTS = Object.fromEntries(Object.entries(config.plans).map(([key, plan]) => [key, {
  ...plan, monthlyCents: 0, annualCents: 0, features: allFeatures,
  // -1 is the internal unlimited sentinel. Public responses use null + a flag.
  forecastComputePerDay: plan.forecastComputePerDay<0?-1:Math.max(config.plans.free.forecastComputePerDay, plan.forecastComputePerDay),
  bulkExportsPerMonth: Math.max(config.plans.free.bulkExportsPerMonth, plan.bulkExportsPerMonth),
  backtestsPerMonth: Math.max(config.plans.free.backtestsPerMonth, plan.backtestsPerMonth),
  collaboratorSeats: Math.max(config.plans.free.collaboratorSeats, plan.collaboratorSeats),
}])) as Record<PlanKey, PlanEntitlement>;
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
    access_model: "metered_and_pro_with_free_trial",
    plans: { metered: { ...PLAN_ENTITLEMENTS.pro,label:"Pay as you go",forecastComputePerDay:null,unlimitedForecasts:true,trialDays:14,
      forecastConcurrentLimit:3,forecastStartsPerMinute:3,monthlyCents:0,annualCents:0,completedForecastCents:50,defaultMonthlyBudgetCents:5000 }, pro:{...PLAN_ENTITLEMENTS.pro,forecastComputePerDay:null,unlimitedForecasts:true,trialDays:14,forecastConcurrentLimit:3,forecastStartsPerMinute:3,monthlyCents:config.plans.pro.monthlyCents,annualCents:config.plans.pro.annualCents} },
  };
}
