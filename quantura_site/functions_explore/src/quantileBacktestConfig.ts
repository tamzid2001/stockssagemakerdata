/** Versioned research rules. Every decision threshold is a produced forecast quantile. */
export type EntryCondition = "crosses_above" | "crosses_below" | "at_or_above" | "at_or_below";
export type EntryRule = { id: string; kind: "entry"; condition: EntryCondition; quantile: number };
export type ExitRule =
  | { id: string; kind: "take_profit" | "stop_loss"; target_mode: "quantile"; quantile: number }
  | { id: string; kind: "take_profit" | "stop_loss"; target_mode: "percent"; percent: number }
  | { id: string; kind: "trailing_stop"; percent: number };
export type QuantileStrategy = { schema_version: 2; type: "quantile_rules"; entry_logic: "all" | "any"; rules: Array<EntryRule | ExitRule> };
export type QuantileReplay = { context_rows: number; evaluation_windows: number };

export const DEFAULT_QUANTILE_STRATEGY: QuantileStrategy = {
  schema_version: 2,
  type: "quantile_rules",
  entry_logic: "all",
  rules: [
    { id: "entry_p10", kind: "entry", condition: "crosses_above", quantile: 0.1 },
    { id: "take_p50", kind: "take_profit", target_mode: "quantile", quantile: 0.5 },
    { id: "stop_p01", kind: "stop_loss", target_mode: "quantile", quantile: 0.01 },
  ],
};
export const DEFAULT_QUANTILE_REPLAY: QuantileReplay = { context_rows: 128, evaluation_windows: 2 };

export const QUANTILE_STRATEGY_SCHEMA = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  title: "Quantura quantile backtest strategy v2",
  description: "Research-only long strategy; exits are OR-combined. Stops take precedence over trailing stops, then take-profit when several signal on the same completed bar.",
  type: "object", additionalProperties: false,
  required: ["schema_version", "type", "entry_logic", "rules"],
  properties: {
    schema_version: { const: 2 }, type: { const: "quantile_rules" },
    entry_logic: { enum: ["all", "any"] },
    rules: { type: "array", minItems: 2, maxItems: 10, items: { oneOf: [
      { type: "object", additionalProperties: false, required: ["id", "kind", "condition", "quantile"],
        properties: { id: { type: "string", pattern: "^[a-z][a-z0-9_]{0,39}$" }, kind: { const: "entry" },
          condition: { enum: ["crosses_above", "crosses_below", "at_or_above", "at_or_below"] }, quantile: { type: "number", exclusiveMinimum: 0, exclusiveMaximum: 1 } } },
      { type: "object", additionalProperties: false, required: ["id", "kind", "target_mode", "quantile"],
        properties: { id: { type: "string", pattern: "^[a-z][a-z0-9_]{0,39}$" }, kind: { enum: ["take_profit", "stop_loss"] },
          target_mode: { const: "quantile" }, quantile: { type: "number", exclusiveMinimum: 0, exclusiveMaximum: 1 } } },
      { type: "object", additionalProperties: false, required: ["id", "kind", "target_mode", "percent"],
        properties: { id: { type: "string", pattern: "^[a-z][a-z0-9_]{0,39}$" }, kind: { enum: ["take_profit", "stop_loss"] },
          target_mode: { const: "percent" }, percent: { type: "number", minimum: 0.1, maximum: 100 } } },
      { type: "object", additionalProperties: false, required: ["id", "kind", "percent"],
        properties: { id: { type: "string", pattern: "^[a-z][a-z0-9_]{0,39}$" }, kind: { const: "trailing_stop" },
          percent: { type: "number", minimum: 0.1, maximum: 90 } } },
    ] } },
  },
} as const;

function invalid(message: string): never { throw new Error(`BACKTEST_STRATEGY_INVALID: ${message}`); }
function record(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : invalid("Expected a strategy object.");
}
function exactKeys(row: Record<string, unknown>, expected: readonly string[]): void {
  if (Object.keys(row).length !== expected.length || Object.keys(row).some(key => !expected.includes(key))) invalid("Rule fields do not match the selected block type.");
}
function validQuantile(value: unknown, available: readonly number[]): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0 || value >= 1 || !available.some(q => Math.abs(q - value) < 1e-12)) {
    invalid("Every rule quantile must be one of the requested, supported forecast quantiles.");
  }
  return value as number;
}

export function validateQuantileStrategy(value: unknown, quantiles: readonly number[]): QuantileStrategy {
  const row = record(value);
  exactKeys(row, ["schema_version", "type", "entry_logic", "rules"]);
  if (row.schema_version !== 2 || row.type !== "quantile_rules" || !["all", "any"].includes(String(row.entry_logic))) invalid("Use quantile strategy schema version 2.");
  if (!Array.isArray(row.rules) || row.rules.length < 2 || row.rules.length > 10) invalid("Stack 1–4 entry blocks and 1–6 exit blocks (10 total maximum).");
  const rules: Array<EntryRule | ExitRule> = [];
  const ids = new Set<string>();
  const signatures = new Set<string>();
  for (const input of row.rules) {
    const rule = record(input);
    const id = rule.id;
    if (typeof id !== "string" || !/^[a-z][a-z0-9_]{0,39}$/.test(id) || ids.has(id)) invalid("Rule IDs must be unique safe identifiers.");
    ids.add(id);
    let parsed: EntryRule | ExitRule;
    if (rule.kind === "entry") {
      exactKeys(rule, ["id", "kind", "condition", "quantile"]);
      if (!["crosses_above", "crosses_below", "at_or_above", "at_or_below"].includes(String(rule.condition))) invalid("Choose a supported entry crossing or level condition.");
      parsed = { id, kind: "entry", condition: rule.condition as EntryCondition, quantile: validQuantile(rule.quantile, quantiles) };
    } else if (rule.kind === "take_profit" || rule.kind === "stop_loss") {
      if (rule.target_mode === "quantile") {
        exactKeys(rule, ["id", "kind", "target_mode", "quantile"]);
        parsed = { id, kind: rule.kind, target_mode: "quantile", quantile: validQuantile(rule.quantile, quantiles) };
      } else if (rule.target_mode === "percent") {
        exactKeys(rule, ["id", "kind", "target_mode", "percent"]);
        if (typeof rule.percent !== "number" || !Number.isFinite(rule.percent) || rule.percent < 0.1 || rule.percent > 100) invalid("Percent targets must be 0.1–100%.");
        parsed = { id, kind: rule.kind, target_mode: "percent", percent: rule.percent };
      } else invalid("Choose a quantile or percent exit target.");
    } else if (rule.kind === "trailing_stop") {
      exactKeys(rule, ["id", "kind", "percent"]);
      if (typeof rule.percent !== "number" || !Number.isFinite(rule.percent) || rule.percent < 0.1 || rule.percent > 90) invalid("Trailing stop must be 0.1–90% below the observed peak close.");
      parsed = { id, kind: "trailing_stop", percent: rule.percent };
    } else invalid("Unknown rule block type.");
    const signature = JSON.stringify({ ...parsed, id: undefined });
    if (signatures.has(signature)) invalid("Duplicate rule blocks do not add a new condition.");
    signatures.add(signature);
    rules.push(parsed);
  }
  const entries = rules.filter((rule): rule is EntryRule => rule.kind === "entry");
  const exits = rules.filter(rule => rule.kind !== "entry");
  if (entries.length < 1 || entries.length > 4 || exits.length < 1 || exits.length > 6) invalid("Stack 1–4 entry blocks and 1–6 exit blocks.");
  if (rules.filter(rule => rule.kind === "trailing_stop").length > 1) invalid("Only one trailing stop can be active at a time.");
  if (row.entry_logic === "all") {
    // With monotone quantiles, these pairs cannot both hold on the same bar.
    const above = entries.filter(rule => ["crosses_above", "at_or_above"].includes(rule.condition));
    const below = entries.filter(rule => ["crosses_below", "at_or_below"].includes(rule.condition));
    if (above.some(a => below.some(b => a.quantile >= b.quantile))) invalid("ALL entry blocks contain incompatible upper and lower quantile conditions.");
  }
  const lowestEntry = Math.min(...entries.map(rule => rule.quantile));
  const highestEntry = Math.max(...entries.map(rule => rule.quantile));
  for (const exit of exits) {
    if ((exit.kind === "take_profit" || exit.kind === "stop_loss") && exit.target_mode === "quantile") {
      if (exit.kind === "take_profit" && exit.quantile <= highestEntry) invalid("A long take-profit quantile must be above every entry quantile.");
      if (exit.kind === "stop_loss" && exit.quantile >= lowestEntry) invalid("A long stop-loss quantile must be below every entry quantile.");
    }
  }
  return { schema_version: 2, type: "quantile_rules", entry_logic: row.entry_logic as "all" | "any", rules };
}

export function validateQuantileReplay(value: unknown, enabledModelCount: number): QuantileReplay {
  const row = record(value);
  exactKeys(row, ["context_rows", "evaluation_windows"]);
  const context = row.context_rows, windows = row.evaluation_windows;
  if (!Number.isInteger(context) || Number(context) < 2 || Number(context) > 500 ||
      !Number.isInteger(windows) || Number(windows) < 1 || Number(windows) > 8 || Number(windows) * enabledModelCount > 20) {
    throw new Error("BACKTEST_REPLAY_INVALID: Use 2–500 context rows, 1–8 windows, and at most 20 model-window runs.");
  }
  return { context_rows: Number(context), evaluation_windows: Number(windows) };
}
