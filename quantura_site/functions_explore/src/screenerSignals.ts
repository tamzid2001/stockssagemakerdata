import type { QuantScreenerRow } from "./quantScreener";
import { screenerCutoff } from "./screenerHistory";

export type ScreenerForecastRow = { date: string; timestamp: string; session_open: string; session_close: string; p10: number; p50: number; p90: number; [key: string]: unknown };
export type ScreenerQuote = { price: number; timestamp: string; source: string; session?: string };
export type ScreenerSignal = { value: "buy" | "sell" | "neutral"; price: number; quote_timestamp: string; forecast_date: string; p10: number; p90: number; source: string; provisional: boolean; rule?:string; p99?:number; price_target?:number; input_date?:string; target_date?:string };
export type SavedScreenerSignal = { closing_signal?: ScreenerSignal; last_non_neutral_signal?: ScreenerSignal; previous_non_neutral_signal?: ScreenerSignal; daily_evaluation?:ScreenerSignal; last_buy_signal?:ScreenerSignal; previous_buy_signal?:ScreenerSignal };
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

export function decorateScreenerRow(row: QuantScreenerRow, _quote?: ScreenerQuote, saved: SavedScreenerSignal = {}, now = Date.now()): QuantScreenerRow {
  const rows=forecastRows(row),first=rows[0],last=rows.at(-1),cutoff=screenerCutoff(row.forecast_input_gzip);
  const config=row.forecast_config as Record<string,any>|undefined;
  const names=["prophet","toto","granite","chronos","timesfm"];
  const configured=config?.history_lag_sessions===0 && config?.toto_variant==="4m" && names.every(name=>config?.models?.[name]?.enabled===true && config.models[name].weight===0.2) && Object.keys(config?.models||{}).length===5;
  const splitMismatch=row.split_status==="requires_refresh"||row.split_status==="unverified";
  const historical=typeof row.actual_price==="number" && Number.isFinite(row.actual_price) ? row.actual_price : null;
  const closeTime=Date.parse(String(row.daily_close_at||""));
  const quantileKeys=["p01","p10","p25","p50","p75","p90","p99"];
  const aligned=rows.every((r,i)=>quantileKeys.every((key,j)=>typeof r[key]==="number" && Number.isFinite(r[key]) && (j===0 || Number(r[key])>=Number(r[quantileKeys[j-1]]))) && (!i || r.date>rows[i-1].date));
  const valid=row.forecast_engine==="quantura_weekly_ensemble_v2" && configured && aligned && !splitMismatch && rows.length===7 && cutoff && first && last &&
    typeof first.p99==="number" && Number.isFinite(first.p99) && first.p99>=first.p90 && typeof last.p99==="number" && Number.isFinite(last.p99) &&
    cutoff.target>0 && historical!==null && Math.abs(cutoff.target-historical)<=0.000001 && String(row.actual_price_timestamp).slice(0,10)===cutoff.timestamp.slice(0,10) &&
    Date.parse(cutoff.timestamp)<Date.parse(first.timestamp) && Date.parse(String(row.history_cutoff_at))===Date.parse(cutoff.timestamp) && closeTime<=now && closeTime<=Date.parse(String(row.last_forecast_update));
  const signal:ScreenerSignal|null=valid?{value:cutoff.target>(first.p99 as number)?"buy":"neutral",price:cutoff.target,quote_timestamp:String(row.daily_close_at),
    forecast_date:first.date,input_date:cutoff.timestamp.slice(0,10),target_date:last.date,p10:first.p10,p90:first.p90,p99:first.p99 as number,price_target:last.p99 as number,
    rule:"daily_close_above_first_p99_v2",source:"split_adjusted_daily_close",provisional:false}:null;
  const output:QuantScreenerRow={...row,...saved,cutoff_p99_signal:signal,current_signal:signal?.value==="buy"?signal:null,signal:signal?.value==="buy"?"buy":"none",
    signal_status:splitMismatch?"split_basis_requires_refresh":signal?"daily_close_evaluated":"daily_scan_requires_refresh",
    actual_price:historical,actual_price_timestamp:row.daily_close_at||row.actual_price_timestamp||null,quote_source:"split_adjusted_daily_close",quote_session:"regular",
    forecast_comparison_date:first?.date||null,signal_comparison:"latest_completed_daily_close_to_first_p99",
    buy_price_target:signal?.value==="buy"?signal.price_target:null,buy_target_date:signal?.value==="buy"?signal.target_date:null,
    quote_age_seconds:Number.isFinite(closeTime)?Math.max(0,Math.floor((now-closeTime)/1000)):null};
  if(first)for(const q of ["p01","p10","p25","p50","p75","p90","p99"] as const){
    const value=first[q];output[q]=typeof value==="number"&&Number.isFinite(value)?value:null;
    output[`distance_${q}_pct`]=historical!==null&&typeof value==="number"&&value>0?(historical/value-1)*100:null;
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
  if(next.rule==="daily_close_above_first_p99_v2") {
    if(next.provisional || !next.input_date || (previous.daily_evaluation?.input_date||"")>=next.input_date)return previous;
    return {...previous,daily_evaluation:next,...(next.value==="buy"?{last_buy_signal:next,...(previous.last_buy_signal?{previous_buy_signal:previous.last_buy_signal}:{})}:{})};
  }
  if (next.provisional || (previous.closing_signal && next.forecast_date <= previous.closing_signal.forecast_date)) return previous;
  return { ...previous, closing_signal: next, ...(next.value !== "neutral" ? {
    last_non_neutral_signal: next, ...(previous.last_non_neutral_signal ? {previous_non_neutral_signal: previous.last_non_neutral_signal} : {}),
  } : {}) };
}
