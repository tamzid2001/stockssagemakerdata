import type { FiscalDataMeta } from "./FiscalDataClient";

export type FiscalFieldKind =
  | "date"
  | "number"
  | "string"
  | "currency"
  | "percent"
  | "boolean"
  | "unknown";

const NULLISH_VALUES = new Set(["", "null", "undefined", "n/a", "na", "-"]);

export function inferFieldKind(field: string, dataType: unknown): FiscalFieldKind {
  const fieldName = String(field || "").toLowerCase();
  const type = String(dataType || "").toLowerCase();
  if (type.includes("date") || fieldName.endsWith("_date")) return "date";
  if (type.includes("currency") || fieldName.includes("amount") || fieldName.includes("balance")) return "currency";
  if (type.includes("percent") || fieldName.includes("percent") || fieldName.includes("rate")) return "percent";
  if (type.includes("number") || type.includes("integer") || type.includes("float") || type.includes("double")) return "number";
  if (type.includes("bool")) return "boolean";
  if (type.includes("string") || type.includes("text")) return "string";
  return "unknown";
}

export function normalizeRow(
  row: Record<string, unknown>,
  dataTypes: Record<string, string> = {}
): Record<string, unknown> {
  const source = row && typeof row === "object" ? row : {};
  const out: Record<string, unknown> = {};
  for (const [field, raw] of Object.entries(source)) {
    const kind = inferFieldKind(field, dataTypes[field]);
    out[field] = normalizeByKind(raw, kind);
  }
  return out;
}

function normalizeByKind(value: unknown, kind: FiscalFieldKind): unknown {
  if (value == null) return null;
  if (typeof value === "string" && NULLISH_VALUES.has(value.trim().toLowerCase())) return null;

  if (kind === "number" || kind === "currency" || kind === "percent") {
    if (typeof value === "number") return Number.isFinite(value) ? value : null;
    const parsed = Number(String(value).replace(/,/g, "").trim());
    return Number.isFinite(parsed) ? parsed : null;
  }

  if (kind === "boolean") {
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    if (normalized === "true" || normalized === "1" || normalized === "yes") return true;
    if (normalized === "false" || normalized === "0" || normalized === "no") return false;
    return null;
  }

  if (kind === "date") {
    const text = String(value).trim();
    if (!text) return null;
    const parsed = Date.parse(text);
    if (!Number.isFinite(parsed)) return text;
    return new Date(parsed).toISOString();
  }

  if (typeof value === "string") return value.trim();
  return value;
}

export function chooseDisplayColumns(meta: Pick<FiscalDataMeta, "labels">, preferredOrder: string[] = []): string[] {
  const labels = meta && typeof meta === "object" && meta.labels ? meta.labels : {};
  const allColumns = Object.keys(labels);
  if (!allColumns.length) return [];

  const seen = new Set<string>();
  const ordered: string[] = [];

  preferredOrder.forEach((field) => {
    if (!allColumns.includes(field) || seen.has(field)) return;
    seen.add(field);
    ordered.push(field);
  });

  allColumns.forEach((field) => {
    if (seen.has(field)) return;
    seen.add(field);
    ordered.push(field);
  });

  return ordered.slice(0, 10);
}
