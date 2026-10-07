import type { Router } from "express";
import { PredictionMarketDataError } from "./predictionMarketData";
import type { QuantScreenerDataset } from "./quantScreener";
import {FORECAST_FREQUENCIES,forecastFrequency,frequencyBounds,frequencyMinutes,type ForecastFrequency} from "./forecastFrequency";

// Public market-data mirror verified against Kalshi's perps OpenAPI. Never send
// account credentials here, and never route margin contracts through binary APIs.
const ORIGIN = "https://api.elections.kalshi.com/trade-api/v2";
export const PERPS_SOURCE = "kalshi_perps";
export const PERPS_INTERVALS = Object.fromEntries(FORECAST_FREQUENCIES.map(f=>[f,frequencyMinutes(f)])) as Record<ForecastFrequency,number>;
type RecordData = Record<string, any>;
const record = (v: unknown): RecordData => v && typeof v === "object" && !Array.isArray(v) ? v as RecordData : {};
const decimal = (v: unknown): number | null => {
  if ((typeof v !== "string" && typeof v !== "number") || v === "") return null;
  const n = Number(v); return Number.isFinite(n) ? n : null;
};
function underlyingUnits(contractSize: unknown, underlyingMultiplier: unknown): number {
  const size = decimal(contractSize), multiplier = decimal(underlyingMultiplier);
  if (size === null || multiplier === null || size <= 0 || multiplier <= 0) throw new Error("perp_price_scale_invalid");
  const units = size * multiplier;
  if (!Number.isFinite(units) || units <= 0) throw new Error("perp_price_scale_invalid");
  return units;
}
function unitPrice(value: unknown, units: number): number | null {
  const price = decimal(value);
  return price === null ? null : Number((price / units).toPrecision(15));
}
export function perpTicker(v: unknown): string {
  const ticker = String(v || "").toUpperCase();
  if (!/^KX[A-Z0-9]{1,36}PERP$/.test(ticker)) throw new PredictionMarketDataError("perp_ticker_invalid", "Choose a Kalshi perpetual contract ticker.", 422);
  return ticker;
}
export function perpFrequency(v: unknown): keyof typeof PERPS_INTERVALS {
  try{return forecastFrequency(v || "1h");}catch{throw new PredictionMarketDataError("perp_interval_invalid", "Choose minute, hourly, daily, weekly or monthly bars.", 422);}
}
export function normalizePerpMarket(input: unknown) {
  const m = record(input), symbol = perpTicker(m.ticker);
  if (!["active","inactive","closed"].includes(m.status) || typeof m.title !== "string") throw new Error("perp_market_schema_invalid");
  const contractSize=decimal(m.contract_size), underlyingMultiplier=decimal(m.underlying_multiplier);
  const units=underlyingUnits(contractSize,underlyingMultiplier), reference=record(m.reference_price);
  const referenceTimestamp=Number(reference.ts_ms);
  return { resource_type: "perpetual_contract", resource_id: `${PERPS_SOURCE}:${symbol}`, symbol,
    name: `${m.title} perpetual`, source: PERPS_SOURCE, asset_class: "perpetual", exchange: "Kalshi", currency: "USD",
    unit: "USD per underlying unit", timezone: "UTC", status: m.status,
    history_available: true, forecast_available: true, available_granularities: Object.keys(PERPS_INTERVALS),
    contract_size: contractSize, underlying_multiplier: underlyingMultiplier, underlying_units_per_contract: units,
    provider_asset_class: typeof m.asset_class === "string" ? m.asset_class : null,
    // Kalshi documents reference_price as the underlying reference scaled per
    // contract. Divide every price by the complete underlying exposure so UI,
    // downloads and forecasts share one per-underlying-unit price basis.
    spot_reference_price: unitPrice(reference.price,units),
    spot_reference_timestamp: Number.isSafeInteger(referenceTimestamp) ? new Date(referenceTimestamp).toISOString() : null,
    reference_contract_price: decimal(reference.price),
    // Last trade is not necessarily a completed candle. Preserve it separately.
    last_trade_price: unitPrice(m.price,units), last_trade_contract_price: decimal(m.price),
    redistribution_status: "review_required" };
}

export function normalizePerpCandles(input: unknown, start: number, end: number, units=1) {
  if (!Array.isArray(input)) throw new Error("perp_candles_schema_invalid");
  if (!Number.isFinite(units) || units <= 0) throw new Error("perp_price_scale_invalid");
  const rows = new Map<number, {timestamp:string;open:number|null;high:number|null;low:number|null;close:number;volume:number|null}>();
  for (const value of input) {
    const c = record(value), p = record(c.price), ts = c.end_period_ts, close = decimal(p.close);
    // Never replace null trade closes with previous, bid, ask, mark or spot.
    if (!Number.isSafeInteger(ts) || ts < start || ts > end || close === null || close <= 0) continue;
    const row = { timestamp: new Date(ts * 1000).toISOString(), open: unitPrice(p.open,units), high: unitPrice(p.high,units), low: unitPrice(p.low,units), close: unitPrice(close,units)!, volume: decimal(c.volume) };
    if (rows.has(ts) && JSON.stringify(rows.get(ts)) !== JSON.stringify(row)) throw new Error("perp_candle_conflict");
    rows.set(ts,row);
  }
  return [...rows.values()].sort((a,b) => a.timestamp.localeCompare(b.timestamp));
}

export class KalshiPerpsService {
  private cache = new Map<string,{until:number;promise:Promise<any>}>();
  constructor(private request: typeof fetch = fetch, private now = () => Date.now()) {}
  private async get(path: string, deadline=Date.now()+25_000) {
    const request=()=>{
      const remaining=deadline-Date.now();
      if(remaining<=0)throw new PredictionMarketDataError("perps_deadline_exceeded","Perpetual data exceeded the request time budget. Try a smaller range.",503);
      return this.request(`${ORIGIN}${path}`,{headers:{Accept:"application/json"},signal:AbortSignal.timeout(Math.min(10_000,remaining)),redirect:"error"});
    };
    let response = await request();
    // Public data only: bounded retry of safe GETs; never retry account orders.
    for(let attempt=0;response.status===429 && attempt<2;attempt++) {
      await new Promise(resolve=>setTimeout(resolve,Math.min(5000,Math.max(1000,Number(response.headers.get("retry-after"))*1000||1000)*(attempt+1))));
      response=await request();
    }
    if (!response.ok) throw new PredictionMarketDataError("perps_provider_unavailable", "Kalshi perpetual data is unavailable. Retry later; no other price source was substituted.", response.status === 429 ? 429 : 502);
    return record(await response.json());
  }
  private cached<T>(key: string, ttl: number, loader: () => Promise<T>): Promise<T> {
    const hit=this.cache.get(key);if(hit && hit.until>this.now())return hit.promise;
    const promise=loader().catch(error=>{this.cache.delete(key);throw error;});
    this.cache.set(key,{until:this.now()+ttl,promise});
    while(this.cache.size>64)this.cache.delete(this.cache.keys().next().value!);
    return promise;
  }
  markets() {
    return this.cached("catalog",300_000,async()=>{
      const markets=new Map<string,ReturnType<typeof normalizePerpMarket>>(),seen=new Set<string>();
      let cursor="";const deadline=Date.now()+25_000;
      do {
        const params=new URLSearchParams({limit:"1000",...(cursor?{cursor}:{})});
        const body=await this.get(`/margin/markets?${params}`,deadline);
        if(!Array.isArray(body.markets)||body.markets.length>1000)throw new Error("perp_catalog_schema_invalid");
        for(const raw of body.markets) {
          // One malformed listing must not hide every other genuine contract.
          try {const market=normalizePerpMarket(raw);markets.set(market.symbol,market);} catch {}
        }
        cursor=typeof body.cursor==="string"?body.cursor:"";
        if(cursor && (seen.has(cursor)||seen.size>=20))throw Error("perp_catalog_pagination_invalid");
        if(cursor)seen.add(cursor);
      }while(cursor);
      return [...markets.values()];
    });
  }
  async search(query: string, limit=20) {
    const q=query.toLowerCase();return (await this.markets()).filter(m=>`${m.symbol} ${m.name} ${m.provider_asset_class}`.toLowerCase().includes(q)).slice(0,limit);
  }
  async history(input: {symbol:unknown;frequency?:unknown;start?:unknown;end?:unknown;limit?:unknown}): Promise<{provider:string;symbol:string;frequency:string;timeframe:string;market:ReturnType<typeof normalizePerpMarket>;rows:ReturnType<typeof normalizePerpCandles>;count:number;metadata:RecordData;warnings:string[]}> {
    const symbol=perpTicker(input.symbol), frequency=perpFrequency(input.frequency);
    const limit=input.limit===undefined?500:Number(input.limit);
    if(!Number.isInteger(limit)||limit<1||limit>5000)throw new PredictionMarketDataError("perp_row_limit_invalid","Choose 1–5000 observations per request.",422);
    const parse=(v:unknown)=>typeof v === "number" ? v : Date.parse(String(v));
    const until=input.end ? parse(input.end) : this.now();
    const interval=PERPS_INTERVALS[frequency]*60;
    const end=Math.floor(Math.min(until,this.now())/1000);
    // Calendar aggregates need UTC-hour closes: native daily perpetual bars
    // can end away from midnight and straddle a UTC month/week boundary.
    const baseFrequency=["5min","15min","30min"].includes(frequency)?"1min":["4h","1W-MON","1MS"].includes(frequency)?"1h":frequency;
    const baseInterval=PERPS_INTERVALS[baseFrequency]*60;
    const start=input.start ? Math.floor(parse(input.start)/1000) : Math.max(0,end-Math.min(interval*Math.max(1000,limit*3),baseInterval*20000));
    if(!Number.isFinite(start)||!Number.isFinite(end)||start>=end||start<0||end-start>interval*20000)
      throw new PredictionMarketDataError("perp_date_range_invalid","Use a valid date range of at most 20,000 intervals. Split larger downloads into date ranges.",422);
    if(!["1min","1h","1D"].includes(frequency)) {
      const base=baseFrequency;
      const history=await this.history({...input,frequency:base,start:start*1000,end:end*1000,limit:5000});
      const buckets=new Map<number,typeof history.rows[number]>();
      const coveredStart=Math.max(start*1000,Date.parse(history.rows[0]?.timestamp || "")-PERPS_INTERVALS[base]*60000);
      for(const row of history.rows) {
        const [bucket,finish]=frequencyBounds(Date.parse(row.timestamp)-1,frequency);
        if(bucket<coveredStart || finish>end*1000)continue;
        const previous=buckets.get(finish);
        if(!previous)buckets.set(finish,{...row,timestamp:new Date(finish).toISOString()});
        else {previous.high=previous.high===null||row.high===null?null:Math.max(previous.high,row.high);previous.low=previous.low===null||row.low===null?null:Math.min(previous.low,row.low);previous.close=row.close;previous.volume=previous.volume===null||row.volume===null?null:previous.volume+row.volume;}
      }
      const rows=[...buckets.values()].slice(-limit);
      return {...history,frequency,timeframe:frequency,rows,count:rows.length,
        metadata:{...history.metadata,base_frequency:base,calendar_week_start:"Monday UTC",calendar_month_start:"first UTC day",completed_bars_only:true,base_history_bounded:history.rows.length===5000},
        warnings:[...history.warnings,...(rows.length<limit?[`Only ${rows.length} completed ${frequency} observations are available in this bounded request.`]:[])]};
    }
    const market=(await this.markets()).find(m=>m.symbol===symbol);
    if(!market)throw new PredictionMarketDataError("perp_not_found","This perpetual contract is not listed by Kalshi.",404);
    return this.cached(`history:${symbol}:${frequency}:${start}:${end}:${limit}`,60_000,async()=>{
      const rows: ReturnType<typeof normalizePerpCandles> = [];
      const deadline=Date.now()+40_000;
      let cursor=end, pages=0;
      // Endpoint has no cursor: traverse bounded time windows backwards. Only
      // completed timestamps <= cutoff enter either forecast or download.
      while(cursor>=start && rows.length<limit && pages<20) {
        const from=Math.max(start,cursor-999*interval);
        const params=new URLSearchParams({start_ts:String(from),end_ts:String(cursor),period_interval:String(PERPS_INTERVALS[frequency]),include_latest_before_start:"false"});
        const body=await this.get(`/margin/markets/${encodeURIComponent(symbol)}/candlesticks?${params}`,deadline);
        if(body.ticker!==symbol)throw new Error("perp_history_ticker_mismatch");
        rows.unshift(...normalizePerpCandles(body.candlesticks,from,cursor,market.underlying_units_per_contract));
        cursor=from-1;pages++;
      }
      const unique=[...new Map(rows.map(r=>[r.timestamp,r])).values()].sort((a,b)=>a.timestamp.localeCompare(b.timestamp)).slice(-limit);
      return {provider:PERPS_SOURCE,symbol,frequency,timeframe:frequency,market,rows:unique,count:unique.length,
        metadata:{field:"price.close / (contract_size * underlying_multiplier)",provider_field:"price.close",units:"USD per underlying unit",timezone:"UTC",timestamp_convention:"end_period_ts",requested_start:new Date(start*1000).toISOString(),requested_end:new Date(end*1000).toISOString(),
          contract_size:market.contract_size,underlying_multiplier:market.underlying_multiplier,redistribution_status:"review_required",missing_intervals:"not_filled",requested_limit:limit,pages},
        warnings:unique.length<limit?[`Only ${unique.length} completed trade closes available in the requested range; gaps are not filled.`]:[]};
    });
  }
  async screener(): Promise<QuantScreenerDataset> {
    return this.cached("screener",60_000,async()=>{
      const markets=await this.markets(), now=new Date(this.now()).toISOString();
      const items: QuantScreenerDataset["items"]=[];
      // Bounded concurrency, not one request per viewer per market.
      for(let index=0;index<markets.length;index+=2) {
      if(index)await new Promise(resolve=>setTimeout(resolve,350));
      await Promise.all(markets.slice(index,index+2).map(async m=>{
        let row: Awaited<ReturnType<KalshiPerpsService["history"]>>["rows"][number]|undefined;
        let status="available";
        const referenceAvailable=m.spot_reference_price!==null && m.spot_reference_timestamp!==null;
        if(!referenceAvailable)try{row=(await this.history({symbol:m.symbol,frequency:"1min",limit:1})).rows.at(-1);}catch{status="unavailable";}
        items.push({ticker:m.symbol,company_name:m.name,asset_class:"perpetual",provider:PERPS_SOURCE,market_status:m.status,
          actual_price:referenceAvailable?m.spot_reference_price:row?.close??null,actual_price_timestamp:referenceAvailable?m.spot_reference_timestamp:row?.timestamp??null,
          quote_source:referenceAvailable?"kalshi_perps_reference_spot":"kalshi_perps_trade_close_spot_equivalent",quote_session:"perpetual",quote_status:referenceAvailable||row?status:"unavailable",
          contract_size:m.contract_size,underlying_multiplier:m.underlying_multiplier,units:m.unit,signal_status:"Forecast on demand; no scheduled quantiles published",forecast_engine:null,
          forecast_view_url:`/forecasting?panel=forecast&marketSource=kalshi_perps&ticker=${encodeURIComponent(m.symbol)}`,forecast_action:"create",
          p10:null,p50:null,p90:null,redistribution_status:"review_required"});
      }));
      }
      return {schema_version:"kalshi_perps_catalog_v1",scan_id:`perps-${Math.floor(this.now()/60000)}`,scan_date:now.slice(0,10),generated_at:now,items,
        manifest:{coverage_percentage:markets.length?100*items.filter(r=>r.actual_price!==null).length/markets.length:0,successfully_processed:items.filter(r=>r.actual_price!==null).length,failed:items.filter(r=>r.actual_price===null).length,forecast_engine:"on_demand",warnings:["Kalshi reference prices and trade candles are normalized to USD per underlying unit. No NYSE closing session or scheduled quantile scan is applied."]}};
    });
  }
}
export const kalshiPerps = new KalshiPerpsService();
export function registerKalshiPerpsRoutes(router: Router, service=kalshiPerps) {
  router.get("/market-data/perps/markets",async(_req,res)=>{
    try{res.set("Cache-Control","public, max-age=60, s-maxage=300").json({provider:PERPS_SOURCE,markets:await service.markets()});}
    catch{res.status(502).json({error:"perps_catalog_unavailable"});}
  });
  const history=async(req:any,res:any)=>{
    try{
      const p=req.method==="GET"?req.query:req.body||{};
      const result=await service.history({symbol:p.symbol,frequency:p.frequency||p.timeframe,start:p.start,end:p.end,limit:p.limit});
      res.set("Cache-Control","public, max-age=30, s-maxage=60");
      if(p.format==="csv"){
        const keys=["timestamp","open","high","low","close","volume"] as const;
        const csv=[keys.join(","),...result.rows.map(row=>keys.map(k=>row[k]??"").join(","))].join("\r\n");
        res.set({"Content-Type":"text/csv; charset=utf-8","Content-Disposition":`attachment; filename="${result.symbol}-${result.frequency}-underlying-price.csv"`,"X-Data-Provider":PERPS_SOURCE,"X-Price-Unit":"USD per underlying unit","X-Data-Timezone":"UTC"}).send(csv);
      }else res.json({ok:true,...result});
    }catch(error){const e=error instanceof PredictionMarketDataError?error:null;res.status(e?.status||502).json({error:e?.code||"perps_history_unavailable",message:e?.message||"Perpetual history is unavailable. No substitute prices were returned."});}
  };
  router.get("/market-data/perps/history",history);router.post("/market-data/perps/history",history);
}
