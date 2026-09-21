import type { QuantScreenerRow } from "./quantScreener";
import { screenerCutoff } from "./screenerHistory";

export type ScreenerForecastRow = { date: string; timestamp: string; session_open: string; session_close: string; p10: number; p50: number; p90: number; [key: string]: unknown };
export type ScreenerQuote = { price: number; timestamp: string; source: string; session?: string };
export type ScreenerSignal = { value: "buy" | "sell" | "neutral"; price: number; quote_timestamp: string; forecast_date: string; p10: number; p90: number; source: string; provisional: boolean };
export type SavedScreenerSignal = { closing_signal?: ScreenerSignal; last_non_neutral_signal?: ScreenerSignal; previous_non_neutral_signal?: ScreenerSignal };
export function newYorkDate(timestamp: string): string {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(new Date(timestamp));
  const part = (type: string) => parts.find(p => p.type === type)!.value;
  return `${part("year")}-${part("month")}-${part("day")}`;
}
export function forecastRows(row: QuantScreenerRow): ScreenerForecastRow[] {
  return (Array.isArray(row.forecast_rows) ? row.forecast_rows : []).filter((r): r is ScreenerForecastRow =>
    r && /^\d{4}-\d{2}-\d{2}$/.test(r.date) && [r.p10,r.p50,r.p90].every(Number.isFinite) && r.p10 <= r.p50 && r.p50 <= r.p90)
    .sort((a,b) => a.date.localeCompare(b.date));
}
export function signalForQuote(forecast: ScreenerForecastRow, quote: ScreenerQuote, provisional = true): ScreenerSignal {
  return { value: quote.price < forecast.p10 ? "buy" : quote.price > forecast.p90 ? "sell" : "neutral", price: quote.price,
    quote_timestamp: quote.timestamp, forecast_date: forecast.date, p10: forecast.p10, p90: forecast.p90, source: quote.source, provisional };
}
export function decorateScreenerRow(row: QuantScreenerRow, quote?: ScreenerQuote, saved: SavedScreenerSignal = {}, now = Date.now()): QuantScreenerRow {
  const historical: ScreenerQuote | undefined = typeof row.actual_price === "number" && Number.isFinite(row.actual_price) && row.actual_price > 0 && typeof row.actual_price_timestamp === "string" && Date.parse(row.actual_price_timestamp) <= now
    ? {price: row.actual_price, timestamp: row.actual_price_timestamp, source: "historical_daily_close"} : undefined;
  const eligible = quote && quote.price > 0 && Number.isFinite(quote.price) && Date.parse(quote.timestamp) + 60_000 <= now;
  const selected = eligible && Date.parse(quote.timestamp) > Date.parse(historical?.timestamp || "1970-01-01") ? quote : historical;
  const rows = forecastRows(row);
  const date = selected ? selected.source === "historical_daily_close" ? selected.timestamp.slice(0,10) : newYorkDate(selected.timestamp) : "";
  const forecast = rows.find(r => r.date === date) || (date && date < rows[0]?.date ? rows[0] : undefined);
  const splitMismatch = row.split_status === "requires_refresh" || row.split_status === "unverified";
  const cutoff = screenerCutoff(row.forecast_input_gzip);
  const first = rows[0];
  const cutoffSignal = !splitMismatch && cutoff && first && typeof first.p99 === "number" && Number.isFinite(first.p99) && first.p99 >= first.p90 &&
    Date.parse(cutoff.timestamp) < Date.parse(first.timestamp) && (!row.history_cutoff_at || Date.parse(String(row.history_cutoff_at)) === Date.parse(cutoff.timestamp))
    ? {value: cutoff.target > first.p99 ? "buy" : "neutral", price: cutoff.target, quote_timestamp: cutoff.timestamp,
      forecast_date: first.date, p99: first.p99, rule: "cutoff_close_above_first_p99"} : null;
  const signal = forecast && selected && !splitMismatch ? signalForQuote(forecast, selected) : null;
  const levels = forecast || rows[0];
  const output: QuantScreenerRow = { ...row, ...saved, cutoff_p99_signal: cutoffSignal, current_signal: signal, signal: signal?.value || "unavailable",
    signal_status: splitMismatch ? "split_basis_requires_verification" : signal ? "provisional" : "forecast_row_unavailable",
    actual_price: selected?.price ?? null, actual_price_timestamp: selected?.timestamp ?? null, quote_source: selected?.source || "unavailable",
    quote_session: selected?.session || "historical", forecast_comparison_date: forecast?.date || null,
    signal_comparison: signal ? date < forecast!.date ? "before_first_forecast_session" : "matching_forecast_session" : null,
    quote_age_seconds: selected ? Math.max(0, Math.floor((now - Date.parse(selected.timestamp)) / 1000)) : null,
    quantile_position: signal ? selected!.price < forecast!.p10 ? "below_p10" : selected!.price < forecast!.p50 ? "between_p10_p50" : selected!.price <= forecast!.p90 ? "between_p50_p90" : "above_p90" : "unavailable" };
  if (levels) for (const q of ["p01", "p10", "p25", "p50", "p75", "p90", "p99"] as const) {
    const value = levels[q];
    output[q] = typeof value === "number" && Number.isFinite(value) ? value : null;
    output[`distance_${q}_pct`] = selected && typeof value === "number" && value > 0 ? (selected.price / value - 1) * 100 : null;
  }
  return output;
}

/** Only the exchange's final completed minute and a forecast made before it are saveable. */
export function finalizedClosingSignal(row: QuantScreenerRow, quote: ScreenerQuote, now = Date.now()): ScreenerSignal | null {
  if (row.split_status === "requires_refresh" || row.split_status === "unverified" || !(quote.price > 0) || !Number.isFinite(quote.price)) return null;
  const barStart = Date.parse(quote.timestamp);
  const forecast = forecastRows(row).find(r => Date.parse(r.session_close) === barStart + 60_000);
  if (!forecast || now < Date.parse(forecast.session_close) || !(Date.parse(String(row.last_forecast_update)) < barStart)) return null;
  return signalForQuote(forecast, quote, false);
}
export function advanceClosingSignal(previous: SavedScreenerSignal, next: ScreenerSignal): SavedScreenerSignal {
  if (next.provisional || (previous.closing_signal && next.forecast_date <= previous.closing_signal.forecast_date)) return previous;
  return { ...previous, closing_signal: next, ...(next.value !== "neutral" ? {
    last_non_neutral_signal: next, ...(previous.last_non_neutral_signal ? {previous_non_neutral_signal: previous.last_non_neutral_signal} : {}),
  } : {}) };
}
