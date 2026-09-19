import test from "node:test";
import assert from "node:assert/strict";
import {ScreenerMarketService} from "./screenerMarketService";
import {registerScreenerAlertRoutes} from "./screenerAlerts";
const date=new Date().toISOString().slice(0,10);
const row={ticker:"TEST",actual_price:100,actual_price_timestamp:date+"T00:00:00Z",last_forecast_update:date+"T00:00:00Z",forecast_rows:[{date,timestamp:date+"T00:00:00Z",session_close:date+"T20:00:00Z",p10:90,p50:100,p90:110}]};
const dataset:any={scan_id:"a",generated_at:new Date().toISOString(),items:[row]};
test("shared quote hydration deduplicates same-scan requests and never returns another scan",async()=>{
  let calls=0;const provider:any={getLatestStockPrices:async()=>{calls++;return new Map();},getStockSplits:async()=>[]};
  const store:any={read:async()=>new Map(),save:async()=>0};const service=new ScreenerMarketService(provider,store);
  await Promise.all([service.current(dataset),service.current(dataset)]);assert.equal(calls,1);
  const next=await service.current({...dataset,scan_id:"b",items:[{...row,ticker:"OTHER"}]});assert.equal(next.items[0].ticker,"OTHER");assert.equal(calls,2);
});
test("unavailable split check withholds newer cross-session prices",async()=>{
  const yesterday=new Date(Date.now()-2*86400000).toISOString().slice(0,10);
  const provider:any={getLatestStockPrices:async()=>new Map([["TEST",{price:50,timestamp:new Date(Date.now()-120000).toISOString()}]]),getStockSplits:async()=>{throw new Error("unavailable");}};
  const service=new ScreenerMarketService(provider,{read:async()=>new Map(),save:async()=>0});
  const output=await service.current({...dataset,items:[{...row,actual_price_timestamp:yesterday+"T00:00:00Z",last_forecast_update:yesterday+"T00:00:00Z"}]});
  assert.equal(output.items[0].actual_price,100);assert.equal(output.warnings.length,1);
});
function harness(){
  const data=new Map<string,any>();const handlers=new Map<string,any>();
  const ref=(path:string):any=>({path,get:async()=>({exists:data.has(path),data:()=>structuredClone(data.get(path))})});
  const db:any={collection:(name:string)=>({doc:(id:string)=>ref(name+"/"+id)}),runTransaction:async(fn:any)=>fn({get:(r:any)=>r.get(),set:(r:any,value:any)=>data.set(r.path,structuredClone(value))})};
  const auth:any={verifyIdToken:async(token:string)=>({uid:token,firebase:{sign_in_provider:token==="guest"?"anonymous":"password"}}),getUser:async(uid:string)=>({uid,email:"verified@example.com",emailVerified:uid!=="unverified"})};
  const router:any=Object.fromEntries(["get","post","delete"].map(method=>[method,(path:string,handler:any)=>handlers.set(method+path,handler)]));
  registerScreenerAlertRoutes(router,{db,auth,publicOrigin:"https://quantura.studio"});
  const call=async(method:string,path:string,user:string,body:any={},params:any={})=>{
    let status=200,result:any;const res:any={setHeader(){},status(s:number){status=s;return this;},json(v:any){result=v;}};
    await handlers.get(method+path)({headers:{authorization:user?"Bearer "+user:""},body,params},res);
    return {status,result};
  };return {call,data};
}
test("saved-alert HTTP handlers isolate accounts, enforce consent and bound filters",async()=>{
  const {call}=harness();const path="/v1/me/screener-alerts";const body={name:"Test",email:false,filters:{search:"TEST"}};
  assert.equal((await call("post",path,"",body)).status,401);
  assert.equal((await call("post",path,"guest",body)).status,403);
  assert.equal((await call("post",path,"unverified",{...body,email:true})).status,403);
  const created=await call("post",path,"owner",body);assert.equal(created.status,201);
  assert.equal((await call("get",path,"other")).result.data.length,0);
  assert.equal((await call("delete",path+"/:alertId","other",{},{alertId:created.result.data.id})).status,404);
  for(let n=1;n<10;n++)assert.equal((await call("post",path,"owner",{...body,filters:{search:"T"+n}})).status,201);
  assert.equal((await call("post",path,"owner",{...body,filters:{search:"eleventh"}})).status,429);
  assert.equal((await call("delete",path+"/:alertId","owner",{},{alertId:created.result.data.id})).status,200);
  assert.equal((await call("get",path,"owner")).result.data.length,9);
});
