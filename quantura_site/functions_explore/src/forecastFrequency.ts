import type { AlpacaBar } from "./alpacaClient";

export const FORECAST_FREQUENCIES = ["1min", "5min", "15min", "30min", "1h", "4h", "1D", "1W-MON", "1MS"] as const;
export type ForecastFrequency = typeof FORECAST_FREQUENCIES[number];
export const FORECAST_FREQUENCY_INPUTS = [...new Set([...FORECAST_FREQUENCIES,"1Min","5Min","15Min","30Min","1Hour","4Hour","1Day","1Week","1Month","1m","5m","15m","30m","1w"])];
const ALIASES: Record<string, ForecastFrequency> = {
  "1m":"1min", "1min":"1min", "5m":"5min", "5min":"5min", "15m":"15min", "15min":"15min", "30m":"30min", "30min":"30min",
  "1h":"1h", "1hour":"1h", "4h":"4h", "4hour":"4h", "1d":"1D", "1day":"1D",
  "1w":"1W-MON", "1week":"1W-MON", "1w-mon":"1W-MON", "1month":"1MS", "1mo":"1MS", "1ms":"1MS",
};
export function forecastFrequency(value: unknown): ForecastFrequency {
  if(String(value).trim()==="1M")throw new Error("frequency_unsupported");
  const result = ALIASES[String(value || "1D").trim().toLowerCase()];
  if (!result) throw new Error("frequency_unsupported");
  return result;
}
export function frequencyMinutes(value: unknown): number {
  return {"1min":1,"5min":5,"15min":15,"30min":30,"1h":60,"4h":240,"1D":1440,"1W-MON":10080,"1MS":44640}[forecastFrequency(value)];
}
export function frequencyTimeframe(value: unknown): string {
  return {"1min":"1Min","5min":"5Min","15min":"15Min","30min":"30Min","1h":"1Hour","4h":"4Hour","1D":"1Day","1W-MON":"1Week","1MS":"1Month"}[forecastFrequency(value)];
}
/** UTC weeks start Monday; months use real calendar boundaries, never 30 days. */
export function frequencyBounds(timestamp: number, value: unknown): [number, number] {
  const frequency = forecastFrequency(value), date = new Date(timestamp);
  if (!Number.isFinite(timestamp)) throw new Error("frequency_timestamp_invalid");
  if (frequency === "1MS") return [Date.UTC(date.getUTCFullYear(),date.getUTCMonth(),1),Date.UTC(date.getUTCFullYear(),date.getUTCMonth()+1,1)];
  if (frequency === "1W-MON") {
    const start=Date.UTC(date.getUTCFullYear(),date.getUTCMonth(),date.getUTCDate()-(date.getUTCDay()+6)%7);
    return [start,start+7*86400_000];
  }
  const size=frequencyMinutes(frequency)*60000, start=Math.floor(timestamp/size)*size;
  return [start,start+size];
}
export function frequencyEnd(timestamp: number, value: unknown): number {
  const frequency=forecastFrequency(value);
  return frequency==="1MS" || frequency==="1W-MON" ? frequencyBounds(timestamp,frequency)[1] : timestamp+frequencyMinutes(frequency)*60000;
}
export function predictionPeriods(last: number, end: number, value: unknown): number {
  if(["1ME","1QE-DEC","1YE-DEC"].includes(String(value))) {
    const step=value==="1ME"?1:value==="1QE-DEC"?3:12;
    let count=0,anchor=new Date(last);
    let next=Date.UTC(anchor.getUTCFullYear(),anchor.getUTCMonth()+step+1,0);
    while(next<=end && count<=1024){count++;anchor=new Date(next);next=Date.UTC(anchor.getUTCFullYear(),anchor.getUTCMonth()+step+1,0);}
    return count;
  }
  let frequency;
  try{frequency=forecastFrequency(value);}catch{
    const match=String(value).match(/^(\d+)(min|h|D)$/i);
    if(!match || Number(match[1])<1)throw new Error("frequency_unsupported");
    return Math.floor((end-last)/(Number(match[1])*({min:60000,h:3600000,d:86400000}[match[2].toLowerCase()] || 0)));
  }
  if (frequency!=="1MS" && frequency!=="1W-MON") return Math.floor((end-last)/(frequencyMinutes(frequency)*60000));
  let next=frequencyEnd(last,frequency), count=0;
  while(next<=end && count<=1024){count++;next=frequencyEnd(next,frequency);}
  return count;
}
/** Aggregate only genuine, completed native bars; missing buckets stay absent. */
export function aggregateObservedBars(rows: AlpacaBar[], value: unknown, start: number, end: number, baseMinutes: number): AlpacaBar[] {
  const buckets=new Map<number,AlpacaBar>();
  for(const row of [...rows].sort((a,b)=>Date.parse(a.timestamp)-Date.parse(b.timestamp))) {
    const time=Date.parse(row.timestamp), [bucket,finish]=frequencyBounds(time,value);
    if(bucket<start || finish>end || time+baseMinutes*60000>Math.min(end,finish))continue;
    const saved=buckets.get(bucket);
    if(!saved)buckets.set(bucket,{...row,timestamp:new Date(bucket).toISOString()});
    else {saved.high=Math.max(saved.high,row.high);saved.low=Math.min(saved.low,row.low);saved.close=row.close;saved.volume+=row.volume;}
  }
  return [...buckets.values()];
}
export function forecastFrequencyCapabilities() {
  return FORECAST_FREQUENCIES.map(frequency=>({frequency,timeframe:frequencyTimeframe(frequency),
    bucket_timezone:"UTC", calendar_period:frequency==="1MS"?"month":frequency==="1W-MON"?"week":null,
    week_start:frequency==="1W-MON"?"Monday":undefined, completed_bars_only:true, missing_intervals:"not_filled"}));
}
