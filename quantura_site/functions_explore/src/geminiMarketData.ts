import type {Router} from "express";
import rateLimit from "express-rate-limit";
import { isIP } from "node:net";
import {forecastFrequency,frequencyEnd,aggregateObservedBars} from "./forecastFrequency";
import type {AlpacaBar} from "./alpacaClient";
import {EconomicDataError,dateInstant} from "./economicData";

/** Gemini exchange public data. This is separate from Google Gemini models. */
export function createGeminiMarketClient(request:typeof fetch=fetch) {
  const cache=new Map<string,{until:number;promise:Promise<any>}>();
  async function get(path:string,ttl=60_000):Promise<any> {
    const hit=cache.get(path);if(hit&&hit.until>Date.now())return hit.promise;
    const promise=(async()=>{
      for(let attempt=0;attempt<3;attempt++) {
        const response=await request("https://api.gemini.com"+path,{headers:{Accept:"application/json"},signal:AbortSignal.timeout(12000)});
        if((response.status===429||response.status>=500)&&attempt<2){await new Promise(r=>setTimeout(r,200*(attempt+1)));continue;}
        if(!response.ok)throw new EconomicDataError("gemini_market_unavailable","Gemini could not return this market or interval.",response.status===404?404:502);
        return response.json();
      }
    })().catch(e=>{cache.delete(path);throw e;});
    cache.set(path,{until:Date.now()+ttl,promise});if(cache.size>128)cache.delete(cache.keys().next().value!);return promise;
  }
  async function search(q:string,limit=10,mode="open") {
    const symbols=await get("/v1/symbols",3600_000);
    if(!Array.isArray(symbols))throw new EconomicDataError("gemini_response_invalid","Gemini returned an invalid symbol catalog.",502);
    const normalized=q.toLowerCase().replace(/[ /_-]/g,"");
    const rows:Record<string,any>[]=symbols.filter(s=>typeof s==="string"&&/^[a-z0-9]{2,40}$/i.test(s)&&!s.endsWith("perp")&&s.includes(normalized)).slice(0,limit).map(symbol=>({resource_type:"instrument",resource_id:"gemini:"+symbol.toUpperCase(),symbol:symbol.toUpperCase(),name:symbol.toUpperCase()+" spot market",asset_class:"crypto",source:"gemini",exchange:"Gemini",history_available:true,forecast_available:true,unit:"quote currency per base unit"}));
    const params=new URLSearchParams({search:q,limit:String(limit),offset:"0"});
    if(mode!=="any")params.set("status","active");
    const events=await get("/v1/prediction-markets/events?"+params);
    if(!Array.isArray(events?.data))throw new EconomicDataError("gemini_response_invalid","Gemini returned an invalid event catalog.",502);
    function visit(event:any) {
      for(const c of event.contracts||[]) {
        if(!/^[A-Z0-9-]{1,180}$/.test(c.instrumentSymbol||""))continue;
        rows.push({resource_type:"gemini_prediction_contract",resource_id:"gemini:"+c.instrumentSymbol,symbol:c.instrumentSymbol,name:c.label||c.instrumentSymbol,market_title:event.title,asset_class:"prediction_market",source:"gemini",exchange:"Gemini",event_id:event.ticker,contract_id:c.id,status:c.status,unit:"decimal probability (0–1)",prices:c.prices||null,
          history_available:Array.isArray(c.priceHistory)&&c.priceHistory.length>0,forecast_available:false,
          coverage:"Public event and contract pricing. Historical price arrays may be absent; no synthetic history or inferred kickoff from expiry.",event_start_at:c.startTime||event.startTime||null,expiry_at:c.expiryDate||event.expiryDate||null});
      }
      for(const child of event.events||[])visit(child);
    }
    events.data.forEach(visit);
    return rows.slice(0,limit);
  }
  async function history(body:Record<string,any>) {
    const symbol=String(body.symbol||"").toLowerCase();
    if(!/^[a-z0-9]{2,40}$/.test(symbol))throw new EconomicDataError("gemini_selection_invalid","Choose a Gemini spot symbol.");
    const symbols=await get("/v1/symbols",3600_000);
    if(!symbols.includes(symbol)||symbol.endsWith("perp"))throw new EconomicDataError("gemini_selection_invalid","Choose a verified Gemini spot symbol.");
    const frequency=forecastFrequency(body.frequency||body.timeframe||"1Hour");
    const limit=Number(body.limit??500);if(!Number.isInteger(limit)||limit<2||limit>50000)throw new EconomicDataError("gemini_selection_invalid","Choose 2–50,000 observations.");
    const end=body.end?Math.min(Date.now(),dateInstant(body.end,true)):Date.now(),start=body.start?dateInstant(body.start):0;
    if(start>=end)throw new EconomicDataError("gemini_selection_invalid","The history start must be before its end.");
    // The live API advertises 1hr/1day. Preserve those exact provider aliases;
    // the documentation's 1h/1d currently returns InvalidParameterValue.
    const base=frequency==="4h"?"1h":["1W-MON","1MS"].includes(frequency)?"1D":frequency;
    const frame=({"1min":"1m","5min":"5m","15min":"15m","30min":"30m","1h":"1hr","1D":"1day"} as Record<string,string>)[base];
    const raw=await get(`/v2/candles/${symbol}/${frame}`);
    if(!Array.isArray(raw))throw new EconomicDataError("gemini_response_invalid","Gemini returned an invalid candle response.",502);
    const rows=normalizeGeminiCandles(raw,base,end);
    const completed=frequency===base?rows.filter(r=>Date.parse(r.timestamp)>=start):aggregateObservedBars(rows,frequency,start,end,base==="1h"?60:1440);
    return {provider:"gemini",symbol:symbol.toUpperCase(),frequency,rows:completed.slice(-limit),metadata:{provider_url:"https://developer.gemini.com/trading/rest-api/market-data/list-candles",requested_start:body.start||null,requested_end:body.end||null,earliest_available:rows[0]?.timestamp||null,latest_available:rows.at(-1)?.timestamp||null,timestamp_convention:"bucket_start",missing_intervals:"not_filled",history_coverage:"Gemini candle endpoint returns a bounded recent window and has no documented history pagination."},warnings:["Only the history actually returned by Gemini is available; arbitrary past ranges are not guaranteed."]};
  }
  async function predictionContract(body:Record<string,any>) {
    const event=String(body.event_id||"");if(!/^[A-Z0-9-]{1,180}$/.test(event))throw new EconomicDataError("gemini_selection_invalid","Choose a verified Gemini event ticker.");
    const result=await get("/v1/prediction-markets/events/"+encodeURIComponent(event));
    const find=(e:any):any=>[...(e.contracts||[])].find(c=>c.instrumentSymbol===body.symbol&&String(c.id)===String(body.contract_id))||(e.events||[]).map(find).find(Boolean);
    const contract=find(result);if(!contract)throw new EconomicDataError("gemini_selection_invalid","The contract does not belong to this Gemini event.");
    return {provider:"gemini",event_id:event,symbol:contract.instrumentSymbol,contract_id:contract.id,prices:contract.prices||null,rows:Array.isArray(contract.priceHistory)?contract.priceHistory:[],metadata:{units:"decimal probability (0–1)",history_coverage:"Provider-supplied priceHistory only; it may be empty. No forecast is offered without a verified historical side and sufficient observations."}};
  }
  return {search,history,predictionContract};
}
export function normalizeGeminiCandles(raw:unknown[],frequency:string,end:number):AlpacaBar[] {
  const rows=new Map<number,AlpacaBar>();
  for(const r of raw) {
    if(!Array.isArray(r)||r.length<6||r.slice(0,6).some(x=>typeof x!=="number"||!Number.isFinite(x)))throw new EconomicDataError("gemini_response_invalid","Gemini returned malformed candle data.",502);
    const [timestamp,open,high,low,close,volume]=r;
    if(timestamp<=0||volume<0||low>Math.min(open,close)||high<Math.max(open,close)||low>high)throw new EconomicDataError("gemini_response_invalid","Gemini returned inconsistent candle data.",502);
    if(frequencyEnd(timestamp,frequency)>end)continue;
    if(rows.has(timestamp))throw new EconomicDataError("gemini_response_invalid","Gemini returned duplicate candle timestamps.",502);
    rows.set(timestamp,{timestamp:new Date(timestamp).toISOString(),open,high,low,close,volume,tradeCount:null,vwap:null,session:"regular"});
  }
  return [...rows.values()].sort((a,b)=>Date.parse(a.timestamp)-Date.parse(b.timestamp));
}
export const geminiMarketClient=createGeminiMarketClient();
export function registerGeminiMarketRoutes(router:Router) {
  router.use("/market-data/gemini",rateLimit({windowMs:60000,limit:30,standardHeaders:true,legacyHeaders:false,keyGenerator:req=>{const ip=req.headers["x-vercel-forwarded-for"];return process.env.VERCEL==="1"&&typeof ip==="string"&&isIP(ip)?ip:req.ip||req.socket.remoteAddress||"unknown";}}));
  for(const [path,handler] of [["history",geminiMarketClient.history],["prediction-contract",geminiMarketClient.predictionContract]] as const)router.post("/market-data/gemini/"+path,async(req,res)=>{
    try{res.json({ok:true,...await handler(req.body)});}catch(error){const e=error instanceof EconomicDataError?error:new EconomicDataError("gemini_market_unavailable","Gemini is temporarily unavailable.",502);res.status(e.status).json({ok:false,error:e.code,message:e.message});}
  });
}
