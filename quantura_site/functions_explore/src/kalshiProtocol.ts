/** Kalshi Trade API 3.30.0, reviewed against official docs 2026-09-09. */
export const KALSHI_API_BASE = "https://external-api.kalshi.com/trade-api/v2";

/** Units come from the field name, never from its magnitude. One cent is 0.01. */
export function kalshiPrice(record: Record<string, unknown> | null | undefined, field: string): number | null {
  if (!record) return null;
  const dollars = record[`${field}_dollars`];
  const raw = dollars !== undefined && dollars !== null ? dollars : record[field];
  if (raw === undefined || raw === null || raw === "" || typeof raw === "boolean") return null;
  const amount = Number(raw);
  const price = dollars !== undefined && dollars !== null ? amount : amount / 100;
  return Number.isFinite(price) && price >= 0 && price <= 1 ? Number(price.toFixed(6)) : null;
}

export function kalshiMilestoneStart(eventTicker: string, milestones: unknown): string | null {
  if (!Array.isArray(milestones)) return null;
  const times = milestones.filter(m => m && [ ...(Array.isArray(m.primary_event_tickers) ? m.primary_event_tickers : []), ...(Array.isArray(m.related_event_tickers) ? m.related_event_tickers : []) ].includes(eventTicker))
    .map(m => Date.parse(String(m.start_date || ""))).filter(Number.isFinite);
  // Conflicting occurrence starts need review; do not invent a single cutoff.
  const unique = [...new Set(times)];
  return unique.length === 1 ? new Date(unique[0]).toISOString() : null;
}
