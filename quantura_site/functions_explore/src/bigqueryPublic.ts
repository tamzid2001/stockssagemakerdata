import { GoogleAuth } from "google-auth-library";
import crypto from "node:crypto";

export class BigQueryPublicError extends Error {
  constructor(public code:string,message:string,public status=422){super(message);}
}
export type BigQuerySource={type:"economic_series";provider:"bigquery";dataset_id:string;table_id:string;time_field?:string;value_field?:string;dimensions?:Record<string,string>;aggregation?:"none"|"avg"|"sum"|"min"|"max";frequency?:string;start?:string;end?:string;limit?:number};
type Json=Record<string,any>;
type Transport=(path:string,method?:string,body?:Json)=>Promise<Json>;
const PUBLIC="bigquery-public-data",BASE="https://bigquery.googleapis.com/bigquery/v2/";
const numeric=new Set(["INTEGER","INT64","FLOAT","FLOAT64","NUMERIC","BIGNUMERIC"]);
const dates=new Set(["DATE","DATETIME","TIMESTAMP"]);
const identifier=(v:unknown)=>typeof v==="string"&&/^[A-Za-z_][A-Za-z0-9_]{0,1023}$/.test(v);
const invalid=(message:string)=>new BigQueryPublicError("bigquery_selection_invalid",message);
function instant(value:string,end=false){
  const onlyDate=/^\d{4}-\d{2}-\d{2}$/.test(value),iso=onlyDate?value+(end?"T23:59:59.999Z":"T00:00:00Z"):value;
  if(!/(Z|[+-]\d{2}:\d{2})$/.test(iso)||!Number.isFinite(Date.parse(iso))||onlyDate&&new Date(iso).toISOString().slice(0,10)!==value)throw invalid("Choose a valid ISO date or time with a timezone.");
  return Date.parse(iso);
}
const splitDate=(schema:Json[])=>["year","mo","da"].every(name=>schema.some(f=>f.name===name&&f.mode!=="REPEATED"&&["STRING",...numeric].includes(f.type)));
export function validateBigQuerySource(value:unknown):BigQuerySource {
  if(!value||typeof value!=="object"||Array.isArray(value))throw invalid("Choose a public BigQuery table.");
  const s=value as Json;
  if(Object.keys(s).some(k=>!["type","provider","dataset_id","table_id","time_field","value_field","dimensions","aggregation","frequency","start","end","limit"].includes(k))||s.provider!=="bigquery"||s.type&&s.type!=="economic_series"||!identifier(s.dataset_id)||!identifier(s.table_id))throw invalid("Choose a table in bigquery-public-data.");
  for(const key of ["time_field","value_field"])if(s[key]!==undefined&&!identifier(s[key]))throw invalid("Choose a supported top-level column.");
  const dimensions=s.dimensions||{};
  if(Array.isArray(dimensions)||typeof dimensions!=="object"||Object.keys(dimensions).length>8)throw invalid("Choose at most eight exact filters.");
  for(const [k,v] of Object.entries(dimensions))if(!identifier(k)||typeof v!=="string"||!v||v.length>180)throw invalid("Use a column and an exact filter value.");
  const aggregation=s.aggregation||"none",frequency=s.frequency||"1D",limit=Number(s.limit??500);
  if(!["none","avg","sum","min","max"].includes(aggregation)||!["1h","1D","1W-MON","1MS"].includes(frequency)||!Number.isInteger(limit)||limit<2||limit>50000)throw invalid("Choose a supported interval, aggregation and 2–50,000 observations.");
  for(const key of ["start","end"])if(s[key]!==undefined){if(typeof s[key]!=="string")throw invalid("Choose an ISO date.");instant(s[key],key==="end");}
  if(s.start&&s.end&&instant(s.start)>instant(s.end,true))throw invalid("Start must be before the cutoff.");
  return {...s,dataset_id:s.dataset_id,table_id:s.table_id,type:"economic_series",provider:"bigquery",dimensions:{...dimensions},aggregation,frequency,limit};
}

export function buildBigQueryTimeSeries(source:BigQuerySource,schema:Json[],cutoff=Date.now()) {
  const field=(name:string|undefined)=>schema.find(f=>f.name===name&&f.mode!=="REPEATED");
  const dateParts=source.time_field==="__year_month_day"&&splitDate(schema);
  if(!dateParts&&!dates.has(field(source.time_field)?.type)||!numeric.has(field(source.value_field)?.type))throw invalid("Select one supported date column and one numeric column.");
  const end=Math.min(cutoff,source.end?instant(source.end,true):cutoff);
  if(source.start&&instant(source.start)>end)throw invalid("Start must be before the cutoff.");
  const params:Json[]=[{name:"end",parameterType:{type:"TIMESTAMP"},parameterValue:{value:new Date(end).toISOString()}},{name:"limit",parameterType:{type:"INT64"},parameterValue:{value:String(source.limit)}}];
  const conditions=["observed_at <= @end","target IS NOT NULL"];
  if(source.start){params.push({name:"start",parameterType:{type:"TIMESTAMP"},parameterValue:{value:new Date(Date.parse(source.start)).toISOString()}});conditions.push("observed_at >= @start");}
  const filterConditions:string[]=[];
  for(const [name,val] of Object.entries(source.dimensions||{})){
    const f=field(name);if(!f||["RECORD","STRUCT","GEOGRAPHY","BYTES"].includes(f.type))throw invalid("Choose a scalar filter column.");
    const key=`d${filterConditions.length}`;filterConditions.push(`CAST(\`${name}\` AS STRING) = @${key}`);params.push({name:key,parameterType:{type:"STRING"},parameterValue:{value:val}});
  }
  const ref=`\`${PUBLIC}.${source.dataset_id}.${source.table_id}\``;
  const time=dateParts?"TIMESTAMP(SAFE.PARSE_DATE('%Y%m%d', CONCAT(CAST(`year` AS STRING), LPAD(CAST(`mo` AS STRING),2,'0'), LPAD(CAST(`da` AS STRING),2,'0'))))":`TIMESTAMP(\`${source.time_field}\`)`;
  const base=`SELECT ${time} AS observed_at, SAFE_CAST(\`${source.value_field}\` AS FLOAT64) AS target FROM ${ref}${filterConditions.length?' WHERE '+filterConditions.join(' AND '):''}`;
  let select="SELECT observed_at AS time, target FROM selected";
  if(source.aggregation!=="none"){
    const unit={"1h":"HOUR","1D":"DAY","1W-MON":"WEEK(MONDAY)","1MS":"MONTH"}[source.frequency!];
    const interval={"1h":"1 HOUR","1D":"1 DAY","1W-MON":"1 WEEK","1MS":"1 MONTH"}[source.frequency!];
    // Label completed aggregation buckets at their exclusive end. This avoids
    // leaking a partially observed hour/day/month into a historical cutoff.
    const bucket=`TIMESTAMP_TRUNC(observed_at, ${unit}, 'UTC')`;
    const time=source.frequency==="1MS"?`TIMESTAMP(DATE_ADD(DATE(${bucket}), INTERVAL ${interval}))`:`TIMESTAMP_ADD(${bucket}, INTERVAL ${interval})`;
    select=`SELECT ${time} AS time, ${source.aggregation!.toUpperCase()}(target) AS target FROM selected GROUP BY time HAVING time <= @end`;
  }
  return {query:`WITH base AS (${base}), selected AS (SELECT * FROM base WHERE ${conditions.join(' AND ')}), series AS (${select}) SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', time, 'UTC') AS timestamp, target FROM series ORDER BY timestamp DESC LIMIT @limit`,queryParameters:params,parameterMode:"NAMED",useLegacySql:false};
}

export function createBigQueryPublicClient(options:{transport?:Transport;projectId?:string;maxBytes?:number;reserve?:(userId:string,bytes:number)=>Promise<void>}={}) {
  let auth:GoogleAuth;
  function credentials(){const raw=process.env.FIREBASE_SERVICE_ACCOUNT_JSON;return raw?JSON.parse(raw):undefined;}
  const project=options.projectId||process.env.BIGQUERY_BILLING_PROJECT||process.env.GOOGLE_CLOUD_PROJECT||process.env.GCLOUD_PROJECT||credentials()?.project_id||"quantura-e2e3d";
  const maxBytes=options.maxBytes??100*1024*1024;
  const transport:Transport=options.transport||(async(path,method="GET",body)=>{
    auth ||= new GoogleAuth({credentials:credentials(),projectId:project,scopes:["https://www.googleapis.com/auth/bigquery"]});
    try{return (await auth.request<Json>({url:BASE+path,method:method as "GET"|"POST",...(body?{data:body}:{}),timeout:20000})).data;}
    catch(error:any){const status=error?.response?.status;throw new BigQueryPublicError(status===403?"bigquery_access_unavailable":"bigquery_provider_unavailable",status===403?"The configured backend identity cannot read or query this public dataset. Check BigQuery API, billing and Job User access.":"BigQuery could not return this selection. Retry shortly.",status===403?503:502);}
  });
  const cache=new Map<string,{expires:number;value:Json}>(),pending=new Map<string,Promise<any>>();
  async function cached<T>(key:string,work:()=>Promise<T>,ttl=3600000):Promise<T>{
    const hit=cache.get(key);if(hit&&hit.expires>Date.now())return structuredClone(hit.value) as T;
    const running=pending.get(key);if(running)return structuredClone(await running);
    const task=work().then(value=>{if(cache.size>=32)cache.delete(cache.keys().next().value!);if(Buffer.byteLength(JSON.stringify(value))<2000000)cache.set(key,{expires:Date.now()+ttl,value:value as Json});return value;}).finally(()=>pending.delete(key));
    pending.set(key,task);return structuredClone(await task);
  }
  async function catalog(){return cached("catalog",async()=>{
    const results:Json[]=[];let token="";
    for(let page=0;page<20;page++){
      const data=await transport(`projects/${PUBLIC}/datasets?maxResults=1000${token?'&pageToken='+encodeURIComponent(token):''}`);
      for(const d of data.datasets||[])results.push({id:d.datasetReference.datasetId,name:d.friendlyName||d.datasetReference.datasetId,location:d.location});
      token=data.nextPageToken||"";if(!token)return {results,complete:true};
    }
    return {results,complete:false};
  },86400000);}
  async function describe(value:unknown){const s=validateBigQuerySource(value);return cached(`schema:${s.dataset_id}.${s.table_id}`,async()=>{
    const [dataset,table]=await Promise.all([transport(`projects/${PUBLIC}/datasets/${s.dataset_id}`),transport(`projects/${PUBLIC}/datasets/${s.dataset_id}/tables/${s.table_id}`)]);
    const schema=(table.schema?.fields||[]).filter((f:Json)=>f.mode!=="REPEATED"&&!f.fields);
    const timeFields=schema.filter((f:Json)=>dates.has(f.type)),values=schema.filter((f:Json)=>numeric.has(f.type)).map((f:Json)=>({field:f.name,label:f.description||f.name}));
    if(splitDate(schema))timeFields.push({name:"__year_month_day",description:"UTC date from year, mo and da"});
    return {name:table.friendlyName||`${s.dataset_id}.${s.table_id}`,frequency:"1D",units:"Selected numeric column",url:`https://console.cloud.google.com/bigquery?project=${PUBLIC}&p=${PUBLIC}&d=${s.dataset_id}&t=${s.table_id}&page=table`,metadata:{location:dataset.location,description:table.description||dataset.description||""},location:dataset.location,description:table.description||dataset.description||"",values,time_fields:timeFields.map((f:Json)=>({field:f.name,label:f.description||f.name})),dimensions:[],filter_fields:schema.filter((f:Json)=>!["RECORD","STRUCT","GEOGRAPHY","BYTES"].includes(f.type)).map((f:Json)=>({field:f.name,type:f.type})),schema,forecastable:timeFields.length>0&&values.length>0};
  },86400000);}
  async function search(q:string,limit=10){
    const explicit=q.match(/^(?:bigquery-public-data\.)?([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)$/);
    if(explicit){const source:BigQuerySource={type:"economic_series",provider:"bigquery",dataset_id:explicit[1],table_id:explicit[2]};const info=await describe(source);return {count:1,results:[{id:`${explicit[1]}.${explicit[2]}`,name:info.name,provider:"bigquery",bigquery_source:source,forecast_available:info.forecastable}]};}
    const data=await catalog(),words=q.toLowerCase().split(/\s+/).filter(Boolean);
    const tags:Record<string,string>={noaa_gsod:"weather temperature climate",noaa_global_temp:"weather temperature climate",crypto_bitcoin:"crypto bitcoin blockchain",crypto_ethereum:"crypto ethereum blockchain",google_trends:"search trends",covid19_open_data:"covid health infections"};
    const datasets=data.results.filter((d:Json)=>words.every(w=>`${d.id} ${d.name} ${tags[d.id]||''}`.toLowerCase().includes(w)));
    const results:Json[]=[];
    for(const d of datasets.slice(0,4)){
      const tables=await cached(`tables:${d.id}`,()=>transport(`projects/${PUBLIC}/datasets/${d.id}/tables?maxResults=200`),86400000);
      for(const t of tables.tables||[])results.push({id:`${d.id}.${t.tableReference.tableId}`,name:t.friendlyName||`${d.name} · ${t.tableReference.tableId}`,provider:"bigquery",bigquery_source:{type:"economic_series",provider:"bigquery",dataset_id:d.id,table_id:t.tableReference.tableId},forecast_available:true});
    }
    const selected=await Promise.all(results.slice(-limit).reverse().map(async row=>{const info=await describe(row.bigquery_source);return {...row,forecast_available:info.forecastable};}));
    return {count:results.length,results:selected,catalog_complete:data.complete,matching_datasets:datasets.length,note:"Search a dataset name or enter dataset.table. Table schema determines whether a time series can be forecast."};
  }
  async function history(value:unknown,cutoff?:number,userId=""){
    const source=validateBigQuerySource(value);if(!userId)throw new BigQueryPublicError("bigquery_sign_in_required","Sign in before running a BigQuery query.",401);
    // Latest queries share a stable cutoff and cache. Explicit historical
    // cutoffs remain exact and cannot reuse a later snapshot.
    cutoff ??= Math.floor(Date.now()/3600000)*3600000;
    const info=await describe(source),sql=buildBigQueryTimeSeries(source,info.schema,cutoff);
    const key=crypto.createHash("sha256").update(JSON.stringify(sql)).digest("hex");
    return cached(`query:${key}`,async()=>{
      const body={...sql,location:info.location,maximumBytesBilled:String(maxBytes),useQueryCache:true,timeoutMs:10000,jobTimeoutMs:"30000",maxResults:10000,labels:{application:"quantura",purpose:"public_timeseries"}};
      const dry=await transport(`projects/${project}/queries`,"POST",{...body,dryRun:true});
      const estimated=Number(dry.totalBytesProcessed??dry.statistics?.query?.totalBytesProcessed);
      if(!Number.isFinite(estimated)||estimated>maxBytes)throw new BigQueryPublicError("bigquery_query_too_large","This selection exceeds the 100 MiB query budget. Choose a smaller table or a partition-filtered series.");
      if(options.reserve)await options.reserve(userId,Math.max(estimated,10000000));
      let response=await transport(`projects/${project}/queries`,"POST",body),rows:Json[]=[];
      const cacheHit=response.cacheHit===true;
      const job=response.jobReference,started=Date.now();
      while(!response.jobComplete&&job&&Date.now()-started<25000){response=await transport(`projects/${project}/queries/${encodeURIComponent(job.jobId)}?location=${encodeURIComponent(info.location)}&timeoutMs=1000&maxResults=10000`);}
      if(!response.jobComplete){if(job)await transport(`projects/${project}/jobs/${encodeURIComponent(job.jobId)}/cancel?location=${encodeURIComponent(info.location)}`,"POST",{}).catch(()=>{});throw new BigQueryPublicError("bigquery_query_timeout","The query exceeded its time budget. Narrow the selection.",504);}
      if(response.errors?.length)throw new BigQueryPublicError("bigquery_query_failed","The public table query failed. Check your selection.",502);
      const push=(page:Json)=>{if(page.errors?.length)throw new BigQueryPublicError("bigquery_query_failed","The public table query failed.",502);for(const r of page.rows||[]){const timestamp=String(r.f?.[0]?.v||""),target=Number(r.f?.[1]?.v);if(r.f?.[1]?.v===null||!Number.isFinite(target)||!Number.isFinite(Date.parse(timestamp)))throw new BigQueryPublicError("bigquery_response_invalid","BigQuery returned an invalid time-series row.",502);rows.push({timestamp:new Date(timestamp).toISOString(),target});}};push(response);
      while(response.pageToken&&job&&rows.length<source.limit!){response=await transport(`projects/${project}/queries/${encodeURIComponent(job.jobId)}?location=${encodeURIComponent(info.location)}&pageToken=${encodeURIComponent(response.pageToken)}&maxResults=10000`);push(response);}
      rows=rows.slice(0,source.limit).reverse();
      if(new Set(rows.map(r=>r.timestamp)).size!==rows.length)throw invalid("Several rows share the same timestamp. Select additional exact filters or an explicit aggregation.");
      return {provider:"bigquery",source,frequency:source.frequency!,rows,metadata:{name:info.name,units:"Provider units; see column description",value_field:source.value_field,value_description:info.schema.find((f:Json)=>f.name===source.value_field)?.description||null,provider_url:info.url,attribution:PUBLIC,location:info.location,estimated_bytes:estimated,maximum_bytes_billed:maxBytes,cache_hit:cacheHit,aggregation:source.aggregation,filters:source.dimensions,missing_intervals:"not_filled",timestamp_convention:source.aggregation==="none"?"source_time":"completed_bucket_end",point_in_time:false},warnings:["Public datasets can be revised. A historical cutoff does not reconstruct publication-time vintages.","Aggregation and exact filters are applied only as selected. Missing observations are not filled. Preview values and provider-specific missing-value codes before forecasting."]};
    });
  }
  return {search,describe,history,catalog};
}
let configured:ReturnType<typeof createBigQueryPublicClient>|undefined;
export function configureBigQueryPublic(db:FirebaseFirestore.Firestore){
  configured=createBigQueryPublicClient({reserve:async(userId,bytes)=>{
    const day=new Date().toISOString().slice(0,10),global=db.collection("bigquery_query_budgets").doc(day),user=db.collection("bigquery_query_budgets").doc(`${day}_${crypto.createHash('sha256').update(userId).digest('hex')}`);
    await db.runTransaction(async tx=>{const [a,b]=await Promise.all([tx.get(global),tx.get(user)]);const total=Number(a.data()?.bytes||0),count=Number(b.data()?.queries||0);if(total+bytes>10*1024**3||count>=10)throw new BigQueryPublicError("bigquery_daily_budget_reached","Today's query budget has been reached. Cached series remain available; retry tomorrow.",429);tx.set(global,{bytes:total+bytes});tx.set(user,{queries:count+1});});
  }});
}
export const bigqueryPublic={search:(q:string,n?:number)=>(configured ||=createBigQueryPublicClient()).search(q,n),describe:(s:unknown)=>(configured ||=createBigQueryPublicClient()).describe(s),history:(s:unknown,t?:number,u?:string)=>(configured ||=createBigQueryPublicClient()).history(s,t,u)};
