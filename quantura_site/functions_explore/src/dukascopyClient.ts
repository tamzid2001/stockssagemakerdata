import crypto from "node:crypto";
import { AlpacaError, type AlpacaBar } from "./alpacaClient";
import snapshot from "./dukascopyInstruments.json";
import {aggregateObservedBars,forecastFrequency,frequencyMinutes,frequencyTimeframe,frequencyBounds} from "./forecastFrequency";

// These are the first-party endpoints used by Dukascopy's Historical Data Export
// widget. Its delta JSON candle feed avoids downloading/decompressing every tick.
const BASE = "https://jetta.dukascopy.com/v1";
const MINUTE = 60_000, DAY = 86_400_000;
type RecordValue = Record<string, any>;
export type DukascopyInstrument = { code: string; name: string; description: string; priceScale: number | null; pipValue: number; platformGroupId: string; countryCode?: string };
const GROUPS: Record<string,string> = { FX_MAJOR:"fx", FX_CROSS:"fx", FX_RSRV:"fx", FX_METAL:"metal", FX_METALS:"metal", COM_SPOT:"commodity", IDX_CASH:"index", STK_CASH:"equity_cfd", STOCKS:"equity_cfd", ETF:"etf_cfd", MONEY_MRKT:"etf_cfd", BONDS:"bond_cfd", FUTURES:"futures_cfd", MUTUAL_FUNDS:"fund_cfd", CRYPTO_CURR:"crypto_cfd", CRYPTO_DC:"crypto_cfd" };
const FREQUENCIES: Record<string,[string,number]> = {
  m1:["1Min",1], "1m":["1Min",1], "1min":["1Min",1], m5:["5Min",5], "5m":["5Min",5], "5min":["5Min",5],
  m15:["15Min",15], "15m":["15Min",15], "15min":["15Min",15], m30:["30Min",30], "30m":["30Min",30], "30min":["30Min",30],
  h1:["1Hour",60], "1h":["1Hour",60], "1hour":["1Hour",60], h4:["4Hour",240], "4h":["4Hour",240], "4hour":["4Hour",240],
  d1:["1Day",1440], "1d":["1Day",1440], "1day":["1Day",1440],
  "1w":["1Week",10080], "1week":["1Week",10080], "1w-mon":["1Week",10080],
  "1month":["1Month",44640], "1mo":["1Month",44640], "1ms":["1Month",44640],
};
export function dukascopyFrequency(value: unknown): [string,number] {
  const input=String(value || "1Hour").toLowerCase();
  let result = FREQUENCIES[input];
  if(!result)try {const f=forecastFrequency(input);result=[frequencyTimeframe(f),frequencyMinutes(f)];}catch{}
  if (!result) throw new AlpacaError("invalid_request", "Dukascopy supports minute, hourly, daily, Monday UTC weekly and calendar-month candles.",422);
  return result;
}
const fail = () => new AlpacaError("upstream","Dukascopy returned invalid candle data. Retry; no incomplete download was saved.",502);
const identity = (value: unknown) => String(value || "").toUpperCase().replace(/[^A-Z0-9]/g,"");
export function dukascopyResource(item: DukascopyInstrument) {
  const currency = item.name.includes("/") ? item.name.split("/").at(-1) : null;
  return { resource_type:"instrument", resource_id:`dukascopy:${item.code}`, symbol:item.code, name:item.description || item.name,
    display_symbol:item.name, asset_class:GROUPS[item.platformGroupId] || "cfd", source:"dukascopy", exchange:"Dukascopy",
    currency, unit:currency ? `${currency} quote price` : "provider quote price", history_available:true, forecast_available:true,
    price_scale:item.priceScale, instrument_kind:item.platformGroupId };
}

/** Decode the widget's signed deltas, not integers with a guessed FX divisor. */
export function decodeDukascopyCandles(payload: RecordValue, priceScale: number): AlpacaBar[] {
  const names = ["times","opens","highs","lows","closes","volumes"];
  if (!payload || names.some(k=>!Array.isArray(payload[k]))) throw fail();
  const count = payload.times.length;
  if (count > 200_000 || names.some(k=>payload[k].length!==count)) throw fail();
  if (!count) return [];
  const multiplier = Number(payload.multiplier), shift = Number(payload.shift), scale = 10 ** priceScale;
  let time = Number(payload.timestamp), prices = [payload.open,payload.high,payload.low,payload.close].map(Number);
  if (!Number.isInteger(priceScale) || priceScale<0 || priceScale>8 || !Number.isFinite(time) || !Number.isFinite(multiplier) || multiplier<=0 || !Number.isFinite(shift) || shift<=0 || prices.some(v=>!Number.isFinite(v))) throw fail();
  const rows: AlpacaBar[] = [];
  for (let i=0;i<count;i++) {
    const delta = payload.times[i], volume = payload.volumes[i];
    if (!Number.isFinite(delta) || delta<0 || (i>0 && delta===0) || !Number.isFinite(volume) || volume<0) throw fail();
    time += delta*shift;
    prices = prices.map((p,j)=>{
      const change = payload[names[j+1]][i];
      if (!Number.isFinite(change)) throw fail();
      return Math.round((p+change*multiplier)*scale)/scale;
    });
    const [open,high,low,close] = prices;
    if (time<0 || time>8.64e15 || high<Math.max(open,close) || low>Math.min(open,close) || high<low) throw fail();
    rows.push({timestamp:new Date(time).toISOString(),open,high,low,close,volume,tradeCount:null,vwap:null,session:"regular"});
  }
  return rows;
}

/** UTC buckets contain genuine observed bars only. No forward filling. */
export function aggregateDukascopy(rows: AlpacaBar[], minutes: number, start: number, end: number, baseMinutes: number): AlpacaBar[] {
  const size=minutes*MINUTE, buckets=new Map<number,AlpacaBar>();
  for (const row of rows.sort((a,b)=>Date.parse(a.timestamp)-Date.parse(b.timestamp))) {
    const time=Date.parse(row.timestamp), bucket=Math.floor(time/size)*size;
    // A close is usable only after its native bar AND requested UTC bucket end.
    if (bucket<start || bucket+size>end || time+baseMinutes*MINUTE>end) continue;
    const saved=buckets.get(bucket);
    if (!saved) buckets.set(bucket,{...row,timestamp:new Date(bucket).toISOString()});
    else {saved.high=Math.max(saved.high,row.high);saved.low=Math.min(saved.low,row.low);saved.close=row.close;saved.volume+=row.volume;}
  }
  return [...buckets.values()];
}

function date(value: unknown, end=false): number {
  let raw=String(value || "").trim();
  if (/^\d{2}-\d{2}-\d{4}$/.test(raw)) raw=`${raw.slice(6)}-${raw.slice(0,2)}-${raw.slice(3,5)}`;
  const dateOnly=/^\d{4}-\d{2}-\d{2}$/.test(raw);
  if (!dateOnly && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,3})?)?(?:Z|[+-]\d{2}:\d{2})$/.test(raw)) throw new AlpacaError("invalid_request","Use an ISO date or a timestamp with an explicit UTC offset.",422);
  const parsed=Date.parse(raw);
  if (!Number.isFinite(parsed) || (dateOnly && new Date(parsed).toISOString().slice(0,10)!==raw)) throw new AlpacaError("invalid_request","Choose a valid date.",422);
  return parsed+(end&&dateOnly?DAY:0);
}
function ranges(start: number,end: number,hourly: boolean,daily=false) {
  const result:Array<{start:number,end:number,path:string}>=[];
  const current=new Date(start);current.setUTCHours(0,0,0,0);if(hourly || daily)current.setUTCDate(1);if(daily)current.setUTCMonth(0);
  while(current.getTime()<end) {
    const a=current.getTime(), y=current.getUTCFullYear(), m=current.getUTCMonth()+1, d=current.getUTCDate();
    if(daily)current.setUTCFullYear(y+1);else if(hourly)current.setUTCMonth(current.getUTCMonth()+1);else current.setUTCDate(current.getUTCDate()+1);
    result.push({start:a,end:current.getTime(),path:daily?`/candles/day/{code}/{side}/${y}`:hourly?`/candles/trade/hour/{code}/{side}/${y}/${m}`:`/candles/minute/{code}/{side}/${y}/${m}/${d}`});
    if(result.length>20_000)throw new AlpacaError("invalid_request","This range is too large. Download a smaller date range.",422);
  }
  return result;
}
export class DukascopyClient {
  private cache=new Map<string,{at:number,bytes:number,data:RecordValue}>();
  private pending=new Map<string,Promise<RecordValue>>();
  private cooldown=0;
  constructor(private request: typeof fetch = ((...args)=>fetch(...args)) as typeof fetch) {}
  private async json(path: string, ttl=30_000): Promise<RecordValue> {
    const cached=this.cache.get(path);
    if(cached && Date.now()-cached.at<ttl)return cached.data;
    if(this.pending.has(path))return this.pending.get(path)!;
    if(Date.now()<this.cooldown)throw new AlpacaError("rate_limit","Dukascopy is rate limited. Wait a minute before retrying.",429);
    const promise=(async()=>{
      let response:Response;
      try {response=await this.request(BASE+path,{headers:{Accept:"application/json","User-Agent":"Quantura/1.0 (+https://quantura.studio)"},signal:AbortSignal.timeout(15_000)});}
      catch {throw new AlpacaError("network","Dukascopy could not be reached. Retry this download.",502);}
      if(response.status===429){const header=response.headers.get("Retry-After")||"60",delay=/^\d+$/.test(header)?Number(header)*1000:Date.parse(header)-Date.now();this.cooldown=Date.now()+Math.max(60_000,Number.isFinite(delay)?delay:60_000);throw new AlpacaError("rate_limit","Dukascopy is rate limited. Wait before retrying.",429);}
      if(response.status===404)throw new AlpacaError("no_data","Dukascopy has no data for this instrument or date range.",404);
      if(!response.ok)throw new AlpacaError("upstream","Dukascopy could not return this data. Retry; no incomplete export was saved.",502);
      const raw=await response.text();if(raw.length>12_000_000)throw fail();
      let data:RecordValue;try{data=JSON.parse(raw);}catch{throw fail();}
      if(!data || typeof data!=="object" || Array.isArray(data))throw fail();
      this.cache.delete(path);this.cache.set(path,{at:Date.now(),bytes:raw.length,data});
      while(this.cache.size>128 || [...this.cache.values()].reduce((n,v)=>n+v.bytes,0)>16_000_000)this.cache.delete(this.cache.keys().next().value!);
      return data;
    })().finally(()=>this.pending.delete(path));
    this.pending.set(path,promise);return promise;
  }
  async catalog(): Promise<{instruments:DukascopyInstrument[];stale:boolean}> {
    try {
      const data=await this.json("/instruments",3600_000);
      if(!Array.isArray(data.instruments) || data.instruments.length<100 || data.instruments.some((x:any)=>!this.validInstrument(x)))throw fail();
      return {instruments:data.instruments,stale:false};
    } catch {return {instruments:snapshot.instruments as DukascopyInstrument[],stale:true};}
  }
  private validInstrument(item: any): boolean {return !!item && /^[A-Z0-9][A-Z0-9._-]{0,70}$/i.test(item.code) && typeof item.name==="string" && (item.priceScale==null || Number.isInteger(item.priceScale) && item.priceScale>=0 && item.priceScale<=8);}
  async search(query:string,limit=20) {
    const catalog=await this.catalog(), needle=query.trim().toLowerCase(), alias=identity(query);
    const matches=catalog.instruments.filter(x=>!needle || identity(x.code).includes(alias) && alias.length>0 || `${x.name} ${x.description}`.toLowerCase().includes(needle));
    matches.sort((a,b)=>Number(identity(b.code)===alias)-Number(identity(a.code)===alias)||a.name.localeCompare(b.name));
    return {rows:matches.slice(0,limit).map(dukascopyResource),total:matches.length,catalog_count:catalog.instruments.length,stale:catalog.stale};
  }
  async history(input: RecordValue, page=false): Promise<any> {
    const [timeframe,minutes]=dukascopyFrequency(input.timeframe || input.interval);
    const requestedSide=String(input.price_side || input.side || "bid").toLowerCase();
    if(!["bid","ask"].includes(requestedSide))throw new AlpacaError("invalid_request","Choose bid or ask prices.",422);
    if(input.adjustment && input.adjustment!=="raw")throw new AlpacaError("invalid_request","Dukascopy supplies provider quotes; corporate action adjustment is not available.",422);
    if(input.field && input.field!=="close")throw new AlpacaError("invalid_request","Dukascopy forecasts use observed close prices.",422);
    const symbol=String(input.symbol || input.ticker || "");
    if(!symbol || symbol.length>80 || !/^[A-Za-z0-9._\/-]+$/.test(symbol))throw new AlpacaError("unsupported_symbol","Choose a Dukascopy instrument from Search.",422);
    const catalog=await this.catalog(), item=catalog.instruments.find(x=>identity(x.code)===identity(symbol));
    if(!item)throw new AlpacaError("unsupported_symbol","This ticker is not in Dukascopy's published instrument catalog.",422);
    const metadata=await this.json(`/instruments/${item.code}`,3600_000);
    if(!this.validInstrument(metadata) || metadata.code!==item.code || !Array.isArray(metadata.histories) || !Number.isInteger(metadata.priceScale))throw fail();
    const limit=input.limit===0 || input.limit==="all"?0:Number(input.limit || 2000);
    if(!Number.isInteger(limit) || limit<0 || limit>50_000)throw new AlpacaError("invalid_request","Choose a row limit between 1 and 50,000, or all.",422);
    const now=Date.now(), requestedEnd=input.end?date(input.end,true):null;
    let cursor:RecordValue|null=null;
    if(input.cursor){try{if(String(input.cursor).length>300)throw Error();cursor=JSON.parse(Buffer.from(String(input.cursor),"base64url").toString());if(!Number.isFinite(cursor?.e)||cursor!.e>now || requestedEnd!==null&&cursor!.e>requestedEnd)throw Error();}catch{throw new AlpacaError("invalid_request","This download cursor is invalid.",422);}}
    const end=cursor?.e ?? (requestedEnd!==null?Math.min(requestedEnd,now):Math.floor(now/MINUTE)*MINUTE);
    let start=input.start?date(input.start):end-Math.max(7*DAY,(limit||50000)*minutes*MINUTE*3);
    if(start>=end)throw new AlpacaError("invalid_request","End must be after start.",422);
    // Trade-session hour candles may start at :30 or :15. For such instruments
    // derive UTC H1/H4/D1 from minutes, rather than relabeling session candles.
    const aligned=Array.isArray(metadata.tradeSchedule) && metadata.tradeSchedule.length>0 && metadata.tradeSchedule.every((s:any)=>Object.values(s.sessions || {}).every((v:any)=>Array.isArray(v)&&v.every((t:any)=>/^[0-9]{2}:00(?::00)?$/.test(t.start)&&/^[0-9]{2}:00(?::00)?$/.test(t.end))));
    // A local whole hour can still be a UTC half hour (India, Australia, Nepal).
    // Check both seasonal offsets; uncertain zones use the minute feed.
    let timezoneAligned=true;
    if(metadata.defaultTimezone) {
      try {
        const formatter=new Intl.DateTimeFormat("en-GB",{timeZone:metadata.defaultTimezone,minute:"2-digit"});
        timezoneAligned=["2025-01-15T00:00:00Z","2025-07-15T00:00:00Z"].every(t=>formatter.format(new Date(t))==="00");
      } catch {timezoneAligned=false;}
    }
    const calendarPeriod=["1Week","1Month"].includes(timeframe);
    const daily=calendarPeriod && metadata.histories.some((h:any)=>h.period==="DAY" && Number.isFinite(Number(h.from)));
    const hourly=!daily && minutes>=60 && aligned && timezoneAligned && metadata.histories.some((h:any)=>h.period==="HOUR" && Number.isFinite(Number(h.from)));
    const period=daily?"DAY":hourly?"HOUR":"MINUTE", first=metadata.histories.find((h:any)=>h.period===period);
    if(!first || !Number.isFinite(Number(first.from)))throw new AlpacaError("no_data","Dukascopy does not publish candles for this instrument.",422);
    const requestedStart=start;start=Math.max(start,Number(first.from));
    if(start>=end)throw new AlpacaError("no_data","This range ends before Dukascopy's available history.",404);
    const plan=ranges(start,end,hourly,daily), hash=crypto.createHash("sha256").update(JSON.stringify([item.code,requestedSide,start,end,timeframe,limit,requestedEnd])).digest("hex").slice(0,24);
    let offset=0;
    if(cursor){if(cursor.h!==hash || !Number.isInteger(cursor.o)||cursor.o<0||cursor.o>=plan.length)throw new AlpacaError("invalid_request","This download cursor does not match the current settings.",422);offset=cursor.o;}
    const chunkCount=hourly?12:7, requestedFiles=page?plan.slice(offset,offset+chunkCount):plan;
    // A Monday week can cross a year-file/page boundary. Include its prior
    // daily file and defer incomplete page-end buckets to the next page.
    const overlap=page && daily && timeframe==="1Week" && offset>0;
    const selected=overlap?[plan[offset-1],...requestedFiles]:requestedFiles;
    if(!page && selected.length>180)throw new AlpacaError("invalid_request","Use the paged download for this date range, or a smaller forecast history.",422);
    const output:AlpacaBar[][]=new Array(selected.length);let position=0,failed=false;const deadline=Date.now()+40_000;
    await Promise.all(Array.from({length:Math.min(4,selected.length)},async()=>{
      while(!failed && position<selected.length){const index=position++, chunk=selected[index];
        if(Date.now()>deadline){failed=true;throw new AlpacaError("network","Dukascopy took too long. Retry using a smaller history window.",504);}
        try {
        const path=chunk.path.replace("{code}",item.code).replace("{side}",requestedSide.toUpperCase());
        const data=await this.json(path,chunk.end<now-DAY?1800_000:30_000), rows=decodeDukascopyCandles(data,metadata.priceScale);
        if(hourly && rows.some(r=>Date.parse(r.timestamp)%3600_000!==0))throw new AlpacaError("upstream","Provider hour candles are not UTC aligned. Choose minute bars for this instrument.",422);
        output[index]=rows.filter(r=>Date.parse(r.timestamp)>=chunk.start && Date.parse(r.timestamp)<chunk.end);
        } catch(error) {failed=true;throw error;}
      }
    }));
    const aggregationStart=overlap?Math.max(start,frequencyBounds(requestedFiles[0].start-1,timeframe)[0]):start;
    const aggregationEnd=page && calendarPeriod?Math.min(end,requestedFiles.at(-1)!.end):end;
    const rows=calendarPeriod?aggregateObservedBars(output.flat(),timeframe,aggregationStart,aggregationEnd,daily?1440:hourly?60:1):aggregateDukascopy(output.flat(),minutes,start,end,hourly?60:1);
    const next=page&&offset+requestedFiles.length<plan.length?Buffer.from(JSON.stringify({h:hash,o:offset+requestedFiles.length,e:end})).toString("base64url"):null;
    return {provider:"dukascopy",sourceRequested:"dukascopy",fallbackUsed:false,symbol:item.code,timeframe,feed:requestedSide,priceSide:requestedSide,adjustment:"raw",session:"provider",exchangeTimezone:"UTC",barIntervalMinutes:minutes,
      rows:page||!limit?rows:rows.slice(-limit),next_cursor:next,
      metadata:{instrument:dukascopyResource(item),price_scale:metadata.priceScale,source:BASE,bucket_timezone:"UTC",timestamp_convention:"bucket_start",close_available_at:calendarPeriod?"calendar bucket end":"bucket_start + interval",volume_unit:"provider quote volume (not exchange traded share volume)",base_interval:daily?"1Day":hourly?"1Hour":"1Min",range_start:new Date(start).toISOString(),range_end:new Date(end).toISOString(),available_from:new Date(Number(first.from)).toISOString(),catalog_stale:catalog.stale,completed_files:offset+requestedFiles.length,total_files:plan.length},
      warnings:["Dukascopy bid/ask quotes; CFDs are not exchange share prices. Gaps are not filled. Only completed UTC candles are included.",...(catalog.stale?["Instrument catalog uses the last verified snapshot; history is retrieved from Dukascopy."]:[]),...(start>requestedStart?["The range begins before available history; the returned start is shown in metadata."]:[])]};
  }
}
export const dukascopy = new DukascopyClient();
