import type { Router } from "express";
import rateLimit from "express-rate-limit";
import { fiscalEndpointRegistry, fetchFiscalPayload, buildFiscalQuery } from "./fiscaldata/core";

type RecordData = Record<string, any>;
export type EconomicSource = {
  type: "economic_series"; provider: "fiscaldata" | "worldbank_data360";
  series_id?: string; value_field?: string; dataset_id?: string; indicator_id?: string;
  ref_area?: string; dimensions?: Record<string,string>; limit?: number; start?: string; end?: string;
};
export type EconomicHistory = {
  provider: string; rows: RecordData[]; frequency: string; source: EconomicSource;
  metadata: RecordData; warnings: string[];
};
export class EconomicDataError extends Error {
  constructor(public code: string, message: string, public status = 422) { super(message); }
}
const WORLD_BANK = "https://data360api.worldbank.org/data360/";
const WB_DIMENSIONS = ["FREQ","SEX","AGE","URBANISATION","COMP_BREAKDOWN_1","COMP_BREAKDOWN_2","COMP_BREAKDOWN_3","UNIT_MEASURE","UNIT_TYPE","UNIT_MULT"];
export const FISCAL_SERIES: Record<string,{dimensions:string[]; values:string[]; frequency:string; units:string; url:string}> = {
  debt_to_penny: {dimensions:[],values:["tot_pub_debt_out_amt","debt_held_public_amt","intragov_hold_amt"],frequency:"1D",units:"USD",url:"https://fiscaldata.treasury.gov/datasets/debt-to-the-penny/"},
  rates_of_exchange: {dimensions:["country_currency_desc"],values:["exchange_rate"],frequency:"1QE-DEC",units:"foreign currency units per USD",url:"https://fiscaldata.treasury.gov/datasets/treasury-reporting-rates-exchange/"},
  avg_interest_rates: {dimensions:["security_type_desc","security_desc"],values:["avg_interest_rate_amt"],frequency:"1ME",units:"percent",url:"https://fiscaldata.treasury.gov/datasets/average-interest-rates-treasury-securities/"},
  operating_cash_balance: {dimensions:["account_type"],values:["open_today_bal","close_today_bal","open_month_bal","open_fiscal_year_bal"],frequency:"1D",units:"million USD",url:"https://fiscaldata.treasury.gov/datasets/daily-treasury-statement/"},
};

/** Public data only. Coalesce repeated requests without Firestore reads/writes. */
export function createEconomicClient(request: typeof fetch = fetch) {
  const cache = new Map<string,{expires:number;bytes:number;payload:any}>();
  const pending = new Map<string,Promise<any>>();
  let bytes = 0;
  async function cached(key:string, work:()=>Promise<any>, ttl=3600_000):Promise<any> {
    const hit=cache.get(key);
    if(hit && hit.expires>Date.now())return hit.payload;
    if(pending.has(key))return pending.get(key)!;
    const task=work().then(payload=>{
      if(hit){bytes-=hit.bytes;cache.delete(key);}
      const size=Buffer.byteLength(JSON.stringify(payload));
      if(size<=2_000_000){
        while(cache.size>=64 || bytes+size>8_000_000){const oldest=cache.keys().next().value!;bytes-=cache.get(oldest)!.bytes;cache.delete(oldest);}
        cache.set(key,{payload,bytes:size,expires:Date.now()+ttl});bytes+=size;
      }
      return payload;
    }).finally(()=>pending.delete(key));
    pending.set(key,task);return task;
  }
  async function wb(path:string, query:URLSearchParams|RecordData):Promise<any> {
    const isGet=query instanceof URLSearchParams;
    const key=path+JSON.stringify(isGet?[...query.entries()]:query);
    return cached(key,async()=>{
      for(let attempt=0;attempt<3;attempt++) {
        let response:Response;
        try {
          response=await request(WORLD_BANK+path+(isGet?"?"+query.toString():""),{
            method:isGet?"GET":"POST",signal:AbortSignal.timeout(12000),
            headers:{Accept:"application/json","User-Agent":"Mozilla/5.0 (compatible; Quantura/1.0)",Origin:"https://data360.worldbank.org",...(!isGet?{"Content-Type":"application/json"}:{})},
            ...(!isGet?{body:JSON.stringify(query)}:{}),
          });
        } catch(error) {if(attempt<2){await pause(attempt);continue;}throw new EconomicDataError("economic_provider_unavailable","World Bank Data360 could not be reached. Retry shortly.",502);}
        if((response.status===429 || response.status>=500) && attempt<2){await pause(attempt);continue;}
        if(!response.ok)throw new EconomicDataError("economic_provider_unavailable","World Bank Data360 could not return this selection. Check the dataset and indicator.",502);
        const payload=await response.json();
        if(path==="data" && (!Array.isArray(payload?.value) || !Number.isInteger(payload?.count)))throw new EconomicDataError("economic_response_invalid","Data360 returned an invalid page.",502);
        return payload;
      }
    });
  }
  async function fiscal(series:string,page=1,filter="",size=5000) {
    const entry=fiscalEndpointRegistry.find(e=>e.id===series);
    if(!entry || !FISCAL_SERIES[series])throw invalid("Choose a supported Treasury dataset.");
    // All fields preserve row identity. Omitting dimensions from fields can
    // make Treasury automatically sum distinct rows into a fabricated series.
    const query=buildFiscalQuery({sort:["-record_date"],filter,pageSize:size,pageNumber:page});
    return cached(entry.endpoint+"?"+query,()=>fetchFiscalPayload(entry.endpoint,query,{fetchImpl:request}));
  }
  async function search(provider:string,q:string,limit=10,skip=0) {
    if(provider==="fiscaldata")return {count:fiscalEndpointRegistry.length,results:fiscalEndpointRegistry.filter(e=>[e.id,e.title,e.category].join(" ").toLowerCase().includes(q.toLowerCase())).map(e=>({id:e.id,name:e.title,provider,...FISCAL_SERIES[e.id]}))};
    if(provider!=="worldbank_data360")throw invalid("Choose Treasury Fiscal Data or World Bank Data360.");
    const payload=await wb("searchv2",{count:true,filter:"type eq 'indicator'",search:q,select:"series_description/idno,series_description/name,series_description/database_id",top:limit,skip});
    if(!Array.isArray(payload?.value))throw new EconomicDataError("economic_response_invalid","Data360 returned an invalid search response.",502);
    return {count:Number(payload["@odata.count"]||0),next_offset:skip+payload.value.length<Number(payload["@odata.count"])?skip+payload.value.length:null,results:payload.value.flatMap((row:RecordData)=>{
      const s=row.series_description;
      if(!s || !validCode(s.idno) || !validCode(s.database_id))return [];
      return [{id:s.idno,name:String(s.name||s.idno),dataset_id:s.database_id,indicator_id:s.idno,provider,url:"https://data360.worldbank.org/en/indicator/"+encodeURIComponent(s.idno)}];
    })};
  }
  async function describe(sourceValue:unknown) {
    const source=validateEconomicSource(sourceValue);
    if(source.provider==="fiscaldata") {
      const spec=FISCAL_SERIES[source.series_id!];
      const payload=await fiscal(source.series_id!,1,"",1000);
      const entry=fiscalEndpointRegistry.find(e=>e.id===source.series_id)!;
      return {name:entry.title,frequency:spec.frequency,units:spec.units,url:spec.url,values:spec.values.map(field=>({field,label:payload.meta.labels[field]||field})),dimensions:spec.dimensions.map(field=>({field,label:payload.meta.labels[field]||field,values:[...new Set(payload.data.map((r:RecordData)=>String(r[field]??"")).filter(Boolean))].sort()})),metadata:payload.meta,coverage:"Dimension options sampled from the latest 1,000 records; the API also accepts an exact historical dimension value."};
    }
    const [metadata,dimensions]=await Promise.all([
      wb("metadata",{query:"&$filter=series_description/idno eq '"+source.indicator_id+"' and series_description/database_id eq '"+source.dataset_id+"'"}),
      wb("disaggregation",new URLSearchParams({datasetId:source.dataset_id!,indicatorId:source.indicator_id!})),
    ]);
    const match=metadata?.value?.find((r:RecordData)=>r.series_description?.idno===source.indicator_id && r.series_description?.database_id===source.dataset_id);
    if(!match || !Array.isArray(dimensions))throw new EconomicDataError("economic_series_not_found","The indicator was not found in that Data360 dataset.",404);
    return {name:match.series_description.name,units:match.series_description.measurement_unit,frequency:match.series_description.periodicity,url:"https://data360.worldbank.org/en/indicator/"+source.indicator_id,metadata:match,
      dimensions:dimensions.filter((r:RecordData)=>[...WB_DIMENSIONS,"REF_AREA"].includes(r.field_name)).map((r:RecordData)=>({field:r.field_name,label:r.label_name||r.field_name,values:(r.field_value||[]).map(String)})),values:[{field:"OBS_VALUE",label:"Observation value"}]};
  }
  async function history(sourceValue:unknown, cutoff=Date.now()):Promise<EconomicHistory> {
    const source=validateEconomicSource(sourceValue),limit=source.limit??500;
    const end=Math.min(cutoff,source.end?dateInstant(source.end,true):cutoff);
    const start=source.start?dateInstant(source.start):null;
    if(start!==null && start>end)throw invalid("The start must be before the data cutoff.");
    const info=await describe(source);
    let raw:RecordData[]=[];
    if(source.provider==="fiscaldata") {
      const spec=FISCAL_SERIES[source.series_id!];
      source.value_field ||= spec.values[0];
      for(const field of spec.dimensions)if(!source.dimensions?.[field])throw invalid("Choose a single "+field+" for this Treasury series.");
      const filters=["record_date:lte:"+new Date(end).toISOString().slice(0,10),...(start===null?[]:["record_date:gte:"+new Date(start).toISOString().slice(0,10)]),...Object.entries(source.dimensions||{}).map(([field,value])=>field+":eq:"+value)].join(",");
      for(let page=1;page<=10;page++) {
        const payload=await fiscal(source.series_id!,page,filters);
        raw.push(...payload.data);
        if(raw.length>=limit && start===null || !payload.links?.next || page>=(payload.meta.totalPages||Infinity))break;
        if(!payload.data.length || page===10)throw new EconomicDataError("economic_history_too_large","This selection needs a smaller date range (maximum 50,000 provider records).",413);
      }
      const rows=normalizeFiscalSeries(raw,source,end);
      return {provider:source.provider,rows:rows.slice(-limit),frequency:spec.frequency,source,metadata:{name:info.name,units:spec.units,provider_url:spec.url,fields:info.metadata,dimensions:source.dimensions,raw_rows:raw.length,timestamp_convention:"reporting_date",missing_intervals:"not_filled",point_in_time:false},warnings:["Reporting dates are not publication dates. This is the current revised history, not a point-in-time dataset.",...(start===null?[]:rows.length>limit?["The selected range contains more than the row limit; only the latest selected observations are returned."]:[])]};
    }
    const dimensions=source.dimensions||{};
    // Auto-select only dimensions with one possible value. Never guess among
    // genders, units, disaggregations or frequencies, even if dates coincide.
    for(const dim of info.dimensions) {
      if(dim.field==="REF_AREA") {
        if(!dim.values.includes(source.ref_area))throw invalid("Choose an available country or economy.");
      } else if(!dimensions[dim.field] && dim.values.length===1)dimensions[dim.field]=dim.values[0];
      else if(!dimensions[dim.field] && dim.values.length>1)throw invalid("Choose one "+dim.field+" for this indicator.");
      if(dimensions[dim.field] && !dim.values.includes(dimensions[dim.field]))throw invalid("Choose a valid "+dim.field+" value.");
    }
    source.dimensions=dimensions;
    const query=new URLSearchParams({DATABASE_ID:source.dataset_id!,INDICATOR:source.indicator_id!,REF_AREA:source.ref_area!,...dimensions});
    // Date filtering is local at exact period boundaries. Using a YYYY-MM-DD
    // filter against annual/quarterly SDMX periods would omit valid records.
    for(let skip=0;skip<50000;) {
      query.set("skip",String(skip));
      const payload=await wb("data",query);
      if(payload.count>50000)throw new EconomicDataError("economic_history_too_large","Select a single country, frequency and disaggregation (maximum 50,000 provider records).",413);
      raw.push(...payload.value);skip+=payload.value.length;
      if(skip>=payload.count)break;
      if(!payload.value.length)throw new EconomicDataError("economic_history_incomplete","Data360 returned an incomplete paged response. Retry shortly.",502);
    }
    const normalized=normalizeWorldBankSeries(raw,source,end,start);
    return {provider:source.provider,source,frequency:normalized.frequency,rows:normalized.rows.slice(-limit),metadata:{name:info.name,units:normalized.units,unit_measure:normalized.unitMeasure,unit_mult:normalized.multiplier,dimensions,provider_url:info.url,attribution:"World Bank Data360 and the original indicator source",license:info.metadata.series_description?.license||info.metadata.series_description?.license_name||null,indicator_metadata:info.metadata,raw_rows:raw.length,timestamp_convention:"reporting_period_end",missing_intervals:"not_filled",point_in_time:false},warnings:["Current revised observations; reporting period end is not the publication date. Historical cutoffs do not reconstruct data vintages.",...(normalized.skipped?[`${normalized.skipped} missing, forecast, future, or out-of-range records were excluded.`]:[]),...(normalized.rows.length>limit && start!==null?["The selected range contains more than the row limit; only the latest selected observations are returned."]:[])]};
  }
  return {search,describe,history};
}
const pause=(attempt:number)=>new Promise(resolve=>setTimeout(resolve,200*(attempt+1)));
const invalid=(message:string)=>new EconomicDataError("economic_selection_invalid",message);
const validCode=(value:unknown)=>typeof value==="string" && /^[A-Za-z0-9_.-]{1,180}$/.test(value);
export function dateInstant(value:string,end=false):number {
  const text=String(value);
  const iso=/^\d{4}-\d{2}-\d{2}$/.test(text)?text+(end?"T23:59:59.999Z":"T00:00:00.000Z"):text;
  if(!/(Z|[+-]\d{2}:\d{2})$/.test(iso) || !Number.isFinite(Date.parse(iso)))throw invalid("Use an ISO date or an absolute time with a timezone.");
  if(/^\d{4}-\d{2}-\d{2}$/.test(text) && new Date(Date.parse(iso)).toISOString().slice(0,10)!==text)throw invalid("Choose a valid date.");
  return Date.parse(iso);
}
export function validateEconomicSource(value:unknown):EconomicSource {
  if(!value || typeof value!=="object" || Array.isArray(value))throw invalid("Choose an economic data source.");
  const s=value as RecordData;
  const allowed=["type","provider","series_id","value_field","dataset_id","indicator_id","ref_area","dimensions","limit","start","end"];
  if(Object.keys(s).some(k=>!allowed.includes(k)) || !["fiscaldata","worldbank_data360"].includes(s.provider) || s.type && s.type!=="economic_series")throw invalid("Choose a supported economic series configuration.");
  const limit=s.limit===undefined?500:Number(s.limit);
  if(!Number.isInteger(limit)||limit<2||limit>50000)throw invalid("Choose 2–50,000 observations.");
  const dimensions=s.dimensions??{};
  if(typeof dimensions!=="object" || Array.isArray(dimensions))throw invalid("Dimensions must be field/value pairs.");
  const dimensionKeys=s.provider==="fiscaldata"?FISCAL_SERIES[s.series_id]?.dimensions:WB_DIMENSIONS;
  if(!dimensionKeys)throw invalid("Choose a supported Treasury dataset.");
  for(const [key,val] of Object.entries(dimensions))if(!dimensionKeys.includes(key) || typeof val!=="string" || !val || val.length>180 || /[,:'"\x00-\x1f]/.test(val))throw invalid("Choose a valid dimension value.");
  if(s.provider==="worldbank_data360" && (!validCode(s.dataset_id)||!validCode(s.indicator_id)||s.ref_area!==undefined&&!validCode(s.ref_area)))throw invalid("Choose a Data360 dataset, indicator and country.");
  if(s.provider==="fiscaldata" && s.value_field && !FISCAL_SERIES[s.series_id].values.includes(s.value_field))throw invalid("Choose a supported numeric Treasury field.");
  if(s.start)dateInstant(s.start);if(s.end)dateInstant(s.end,true);
  return {type:"economic_series",provider:s.provider,...(s.provider==="fiscaldata"?{series_id:s.series_id,...(s.value_field?{value_field:s.value_field}:{})}:{dataset_id:s.dataset_id,indicator_id:s.indicator_id,...(s.ref_area?{ref_area:s.ref_area}:{})}),dimensions:{...dimensions},limit,...(s.start?{start:s.start}:{}),...(s.end?{end:s.end}:{})};
}
function numeric(value:unknown):number|null {
  if(value===null || value===undefined || typeof value==="string" && !value.trim())return null;
  if(typeof value!=="number" && typeof value!=="string")return null;
  const number=Number(value);return Number.isFinite(number)?number:null;
}
export function normalizeFiscalSeries(raw:RecordData[],source:EconomicSource,end:number):RecordData[] {
  const rows:RecordData[]=[];
  for(const row of raw) {
    if(Object.entries(source.dimensions||{}).some(([k,v])=>row[k]!==v))throw new EconomicDataError("economic_response_invalid","Treasury returned a different selected dimension.",502);
    const target=numeric(row[source.value_field!]);if(target===null)continue;
    const timestamp=new Date(dateInstant(row.record_date)).toISOString();
    if(Date.parse(timestamp)<=end)rows.push({...row,timestamp,target,units:FISCAL_SERIES[source.series_id!].units});
  }
  return uniqueRows(rows);
}
/** Label SDMX periods at their end; never interpret annual data as daily bars. */
export function worldBankPeriod(period:string,freq:string):{timestamp:string;frequency:string} {
  let match:RegExpMatchArray|null,date:number,frequency:string;
  if(freq==="A" && (match=period.match(/^(\d{4})$/))){date=Date.UTC(Number(match[1]),11,31);frequency="1YE-DEC";}
  else if(freq==="Q" && (match=period.match(/^(\d{4})-?Q([1-4])$/))){date=Date.UTC(Number(match[1]),Number(match[2])*3,0);frequency="1QE-DEC";}
  else if(freq==="M" && (match=period.match(/^(\d{4})-(?:M)?(0[1-9]|1[0-2])$/))){date=Date.UTC(Number(match[1]),Number(match[2]),0);frequency="1ME";}
  else if(freq==="D" && /^\d{4}-\d{2}-\d{2}$/.test(period)){date=dateInstant(period);frequency="1D";}
  else throw new EconomicDataError("economic_frequency_unsupported","Choose an indicator with annual, quarterly, monthly, or daily reporting periods.");
  return {timestamp:new Date(date).toISOString(),frequency};
}
export function normalizeWorldBankSeries(raw:RecordData[],source:EconomicSource,end:number,start:number|null=null) {
  if(!source.ref_area)throw invalid("Select one country or economy.");
  const rows:RecordData[]=[],identities=new Set<string>();let frequency="",units="",unitMeasure="",multiplier=0,skipped=0;
  for(const row of raw) {
    if(row.DATABASE_ID!==source.dataset_id || row.INDICATOR!==source.indicator_id || row.REF_AREA!==source.ref_area || Object.entries(source.dimensions||{}).some(([k,v])=>String(row[k])!==v))throw new EconomicDataError("economic_response_invalid","Data360 returned a different selected series.",502);
    identities.add(JSON.stringify([...WB_DIMENSIONS.map(k=>row[k]??null)]));
    const point=worldBankPeriod(String(row.TIME_PERIOD),String(row.FREQ));
    const base=numeric(row.OBS_VALUE),mult=Number(row.UNIT_MULT??0);
    if(!Number.isInteger(mult)||Math.abs(mult)>12)throw invalid("This unit multiplier is unsupported.");
    if(base===null || row.OBS_STATUS==="F" || Date.parse(point.timestamp)>end || start!==null&&Date.parse(point.timestamp)<start){skipped++;continue;}
    const target=base*10**mult;if(!Number.isFinite(target))throw invalid("The observation value is not finite.");
    frequency=point.frequency;unitMeasure=String(row.UNIT_MEASURE||"");multiplier=mult;units=unitMeasure;
    rows.push({...row,timestamp:point.timestamp,target});
  }
  if(identities.size>1)throw invalid("Select a single frequency, unit and disaggregation; this selection contains multiple series.");
  if(!frequency)throw new EconomicDataError("economic_history_empty","No numeric observations were available before this cutoff.",422);
  return {rows:uniqueRows(rows),frequency,units,unitMeasure,multiplier,skipped};
}
function uniqueRows(rows:RecordData[]) {
  const seen=new Map<string,RecordData>();
  for(const row of rows){if(seen.has(row.timestamp))throw invalid("Multiple records have the same reporting period. Select additional dimensions or a different series.");seen.set(row.timestamp,row);}
  return [...seen.values()].sort((a,b)=>Date.parse(a.timestamp)-Date.parse(b.timestamp));
}
export const economicClient=createEconomicClient();
export function registerEconomicDataRoutes(router:Router,client=economicClient) {
  router.use("/economic-data",rateLimit({windowMs:60_000,limit:30,standardHeaders:true,legacyHeaders:false}));
  router.get("/economic-data/search",async(req,res)=>{
    try{
      const provider=String(req.query.provider||"fiscaldata"),q=String(req.query.q||"").trim();
      const limit=Number(req.query.limit||10),skip=Number(req.query.offset||0);
      if(q.length>100 || !Number.isInteger(limit)||limit<1||limit>20||!Number.isInteger(skip)||skip<0||skip>100000)throw invalid("Choose a query up to 100 characters and a valid search page.");
      res.setHeader("Cache-Control","public, max-age=300, s-maxage=3600");res.json({ok:true,...await client.search(provider,q,limit,skip)});
    }catch(error){respond(error,res);}
  });
  router.post("/economic-data/describe",async(req,res)=>{
    try{res.setHeader("Cache-Control","no-store");res.json({ok:true,...await client.describe(req.body)});}catch(error){respond(error,res);}
  });
  router.post("/economic-data/history",async(req,res)=>{
    try{res.setHeader("Cache-Control","no-store");const result=await client.history(req.body);res.json({ok:true,...result,count:result.rows.length});}catch(error){respond(error,res);}
  });
}
function respond(error:unknown,res:any) {
  const safe=error instanceof EconomicDataError?error:new EconomicDataError("economic_provider_unavailable","The provider could not return this series. Retry shortly.",502);
  res.status(safe.status).json({ok:false,error:safe.code,message:safe.message});
}
