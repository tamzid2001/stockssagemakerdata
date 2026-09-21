import test from "node:test";
import assert from "node:assert/strict";
import { parseSavedAlert, requireAlertAccount, closingRows, digestMatches, buildScreenerDigest, runScreenerDigests } from "./screenerAlerts";
import { PLATFORM_API_SCOPES, type ApiPrincipal } from "./apiAccess";

const principal:ApiPrincipal={userId:"owner",tokenId:null,tokenName:"web",tokenScopes:[...PLATFORM_API_SCOPES],plan:"free",authMethod:"firebase_session"};
const input={name:"Average P50 > 10%",email:false,filters:{positions:["below-p10","below-p50"],signal:"buy",quantileRules:[{quantile:"p50",statistic:"avg",operator:"gt",percent:10}]}};
test("saved screener filters require a real account and independent API scopes",()=>{
  assert.throws(()=>requireAlertAccount({...principal,guest:true},true),/ACCOUNT_REQUIRED/);
  assert.throws(()=>requireAlertAccount({...principal,tokenScopes:["alerts:read"]},true),/insufficient_scope/);
  assert.doesNotThrow(()=>requireAlertAccount(principal,true));
});
test("save preserves every AND position and normalized quantile filter",()=>{
  const a=parseSavedAlert(input);assert.deepEqual(a.filters.positions,["below-p10","below-p50"]);
  assert.equal(a.id,parseSavedAlert({...input,name:"Renamed",email:true}).id);assert.equal(a.filters.quantileRules?.[0].percent,10);
});
test("no arbitrary recipient, mutable owner or unsupported filters accepted",()=>{
  for(const body of [{...input,email:"true"},{...input,to:"another@example.com"},{...input,user_id:"victim"},{...input,filters:{bad:1}},{...input,name:""},{...input,filters:{quantileRules:[{quantile:"p50",statistic:"avg",operator:"gt",percent:NaN}]}}]) assert.throws(()=>parseSavedAlert(body),/INVALID/);
});
const date="2026-09-18";
const signal={value:"buy",price:80,quote_timestamp:date+"T19:59:00Z",forecast_date:date,p10:90,p90:120,source:"alpaca_iex_minute_close",provisional:false};
const row={ticker:"TEST",actual_price:150,signal:"sell",forecast_rows:[{date,timestamp:date+"T00:00:00Z",session_open:date+"T13:30:00Z",session_close:date+"T20:00:00Z",p10:90,p50:100,p90:120}],closing_signal:signal,quantile_stats:{p50:{avg:100}}};
test("digest uses finalized close rather than after-hours price and excludes stale/provisional closes",()=>{
  const rows=closingRows([row],date);assert.equal(rows[0].actual_price,80);assert.equal(rows[0].signal,"buy");
  assert.equal(closingRows([row],"2026-09-21").length,0);
  assert.equal(closingRows([{...row,closing_signal:{...signal,provisional:true}}],date).length,0);
  assert.equal(closingRows([{...row,split_status:"requires_refresh"}],date).length,0);
  assert.equal(digestMatches([parseSavedAlert(input)],rows,date).length,1);
});
test("email HTML is escaped, bounded, idempotent and links to controls",()=>{
  const matches=digestMatches([parseSavedAlert({...input,name:'<img src=x onerror=alert(1)>'})],closingRows([row],date),date);
  const email=buildScreenerDigest("owner",date,matches,"https://quantura.studio");
  assert.ok(!email.html.includes("<img"));assert.ok(email.html.includes("&lt;img"));assert.ok(email.html.includes("/screener#saved-alerts"));
  assert.equal(email.id,buildScreenerDigest("owner",date,matches,"https://quantura.studio").id);
});

function digestHarness(users:number){
  const records=new Map<string,any>();
  const snapshot=(ref:any)=>({id:ref.id,ref,exists:records.has(ref.path),data:()=>structuredClone(records.get(ref.path))});
  const ref=(path:string):any=>({path,id:path.split("/").at(-1),get:async()=>snapshot(ref(path)),set:async(value:any,options?:any)=>records.set(path,options?.merge?{...(records.get(path)||{}),...structuredClone(value)}:structuredClone(value)),collection:(name:string)=>collection(`${path}/${name}`)});
  const collection=(path:string):any=>({
    path,doc:(id:string)=>ref(`${path}/${id}`),orderBy(){return query(path);},
  });
  const query=(path:string,after="",maximum=100):any=>({
    orderBy(){return this;},limit(value:number){return query(path,after,value);},startAfter(value:string){return query(path,value,maximum);},
    async get(){const prefix=`${path}/`;const docs=[...records.keys()].filter(key=>key.startsWith(prefix)&&!key.slice(prefix.length).includes("/")).map(key=>ref(key)).filter(item=>item.id>after).sort((a,b)=>a.id.localeCompare(b.id)).slice(0,maximum).map(snapshot);return {docs,size:docs.length,empty:docs.length===0};},
  });
  const db:any={collection,runTransaction:async(fn:any)=>fn({get:(item:any)=>item.get(),set:(item:any,value:any,options?:any)=>item.set(value,options),create:(item:any,value:any)=>{if(records.has(item.path))throw new Error("already_exists");records.set(item.path,structuredClone(value));},update:(item:any,value:any)=>item.set(value,{merge:true})})};
  for(let index=0;index<users;index++)records.set(`screener_saved_alerts/user-${String(index).padStart(2,"0")}`,{alerts:{only:parseSavedAlert(input)}});
  return {db,records,auth:{getUser:async(uid:string)=>({uid,email:`${uid}@example.com`,emailVerified:true,disabled:false})}};
}

test("one daily invocation pages across every saved-filter account and records inbox/evaluation state",async()=>{
  const {db,records,auth}=digestHarness(12);
  const result=await runScreenerDigests({db,auth:auth as any,publicOrigin:"https://quantura.studio"},{scan_id:"scan-1"} as any,[row],Date.parse(date+"T20:06:00Z"));
  assert.deepEqual({processed:result.processed,done:result.done,emails:result.emails},{processed:12,done:true,emails:0});
  assert.equal([...records.keys()].filter(key=>key.startsWith("screener_daily_digests/")).length,12);
  assert.equal([...records.keys()].filter(key=>key.includes("/items/")).length,12);
  for(let index=0;index<12;index++)assert.equal(records.get(`screener_saved_alerts/user-${String(index).padStart(2,"0")}`).last_evaluation.matched_filters,1);
});
