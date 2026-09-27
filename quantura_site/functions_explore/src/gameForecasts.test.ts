import assert from "node:assert/strict";
import test from "node:test";
import {gameDate,publicGameForecast,registerGameForecastRoutes} from "./gameForecasts";
const now=Date.parse("2026-09-26T22:00:00Z");
const fixture=()=>({id:"a".repeat(32),provider:"kalshi",event_title:"A vs B",outcome:"A",game_date:"2026-09-26",
 game_start:"2026-09-26T23:30:00Z",forecast_end:"2026-09-27T03:30:00Z",generated_at:"2026-09-26T21:10:00Z",input_cutoff:"2026-09-26T21:00:00Z",
 predictions:[{timestamp:"2026-09-27T03:30:00Z",quantiles:{"0.1":.2,"0.5":.4,"0.9":.8}}],secret:"never public"});
test("today is New York date; valid snapshots remain final after start hour",()=>{
 assert.equal(gameDate(Date.parse("2026-09-27T02:00:00Z")),"2026-09-26");
 const row=publicGameForecast(fixture(),now)!;assert.equal(row.secret,undefined);assert.equal(row.predictions,undefined);
 assert.equal(row.status,"updating_pregame");assert.equal(publicGameForecast(fixture(),now+3600000)?.status,"final_pregame");
 assert.equal(publicGameForecast(fixture(),Date.parse("2026-09-27T12:00:00Z")),null);
 assert.ok(publicGameForecast(fixture(),now,true)?.predictions);
});
test("late, future, wrong-day, malformed and crossed-band forecasts are hidden",()=>{
 for(const patch of [{generated_at:"2026-09-26T23:00:00Z"},{game_date:"2026-09-27"},{input_cutoff:"2026-09-26T22:30:00Z"},
 {forecast_end:"2026-09-27T03:31:00Z"},{predictions:[{timestamp:"2026-09-27T03:30:00Z",quantiles:{"0.1":.8,"0.5":.4,"0.9":.7}}]}])assert.equal(publicGameForecast({...fixture(),...patch},now),null);
});
test("six-quantile refresh retains tails, four/five models, and honest retrospective timing",()=>{
 const modern={...fixture(),schema_version:2,history_count:82,schedule_verified_at:"2026-09-26T21:00:00Z",models:["prophet","granite","chronos","timesfm","toto"],
 predictions:[{timestamp:"2026-09-27T03:30:00Z",quantiles:{"0.01":.01,"0.25":.2,"0.5":.4,"0.75":.6,"0.9":.8,"0.99":.99}}]};
 assert.deepEqual(Object.keys((publicGameForecast(modern,now,true)!.predictions as any[])[0].quantiles),["0.01","0.25","0.5","0.75","0.9","0.99"]);
 assert.equal(publicGameForecast({...modern,models:["prophet","chronos"]},now),null);
 const replay={...modern,generated_at:"2026-09-27T02:00:00Z",recomputed_at:"2026-09-27T02:00:00Z",original_generated_at:modern.generated_at};
 assert.ok(publicGameForecast(replay,Date.parse(replay.generated_at),true));
 assert.equal(publicGameForecast({...replay,input_cutoff:"2026-09-26T23:00:00Z"},Date.parse(replay.generated_at)),null);
});
test("public contract metadata pairs actual binary sides without leaking worker fields",()=>{
 const row=publicGameForecast({...fixture(),symbol:"KXMLBGAME-26SEP261915CHCBOS-CHC",contract_id:"KXMLBGAME-26SEP261915CHCBOS-CHC:no",event_id:"KXMLBGAME-26SEP261915CHCBOS",market_id:"CHC",market_title:"Chicago C to win",worker_token:"private"},now)!;
 assert.equal(row.side,"no");assert.equal(row.symbol,"KXMLBGAME-26SEP261915CHCBOS-CHC");assert.equal(row.worker_token,undefined);
});
test("paged game catalog reaches outcomes beyond 1,000 and rejects malformed cursors",async t=>{
 t.mock.method(Date,"now",()=>now);
 const routes=new Map<string,Function>(),rows=Array.from({length:1002},(_,n)=>({id:n.toString(16).padStart(32,"0"),data:()=>({...fixture(),id:n.toString(16).padStart(32,"0"),side:"yes"})}));
 let after="",limit=0,queries=0;
 const query:any={where:()=>query,orderBy:()=>query,startAfter:(cursor:string)=>{after=cursor;return query;},limit:(n:number)=>{limit=n;return query;},get:async()=>{queries++;const docs=rows.filter(row=>!after||row.id>after).slice(0,limit);return {docs,size:docs.length};}};
 const db:any={collection:(name:string)=>name==="game_forecast_catalog"?query:{get:async()=>({docs:[]})}};
 registerGameForecastRoutes({get:(path:string,handler:Function)=>routes.set(path,handler),post:()=>{}}as any,db);
 const fetchPage=async(cursor="")=>{let value:any;const res:any={setHeader:()=>{},status:(code:number)=>{res.statusCode=code;return res;},json:(payload:any)=>{value=payload;}};await routes.get("/screener/games")!({query:{cursor}},res);return {value,status:res.statusCode||200};};
 const first=await fetchPage();assert.equal(first.value.items.length,500);
 const second=await fetchPage(first.value.next_cursor);assert.equal(second.value.items.length,500);
 const third=await fetchPage(second.value.next_cursor);assert.equal(third.value.items.length,2);assert.equal(third.value.next_cursor,null);assert.equal(third.value.bounded,false);
 const invalid=await fetchPage("../../private");assert.equal(invalid.status,400);assert.equal(queries,3);
});

test("detail exposes genuine observations and safe market links while the paged catalog omits history",()=>{
 const input={...fixture(),symbol:"KXMLBGAME-26SEP261915CHCBOS-CHC",event_id:"KXMLBGAME-26SEP261915CHCBOS",
 observations:[{timestamp:"2026-09-26T20:00:00Z",price:.4}],private_token:"hidden"};
 const detail=publicGameForecast(input,now,true)!;
 assert.equal((detail.observations as any[]).length,1);assert.equal(detail.private_token,undefined);
 assert.equal(new URL(String(detail.market_url)).hostname,"kalshi.com");
 assert.equal(publicGameForecast(input,now)!.observations,undefined);
});

test("saved history remains private and rejects a different user's snapshot",async()=>{
 const routes=new Map<string,Function>();let savedReads=0;
 const db:any={collection:(name:string)=>({doc:()=>({get:async()=>{
  if(name==='user_game_forecasts'){savedReads++;return {exists:true,data:()=>({ownerUid:'owner',forecast:fixture()})};}
  return {exists:true,data:()=>({plan:'free'})};
 }})})};
 const auth:any={verifyIdToken:async()=>({uid:'someone_else',firebase:{sign_in_provider:'password'}})};
 registerGameForecastRoutes({get:(path:string,handler:Function)=>routes.set(path,handler),post:()=>{}}as any,db,auth);
 let status=200,payload:any;const res:any={setHeader:()=>{},status:(n:number)=>{status=n;return res;},json:(value:any)=>{payload=value;}};
 const route=routes.get('/screener/games/saved/:id/history')!;
 await route({params:{id:'a'.repeat(40)},headers:{}},res);assert.equal(status,401);assert.equal(savedReads,0);
 await route({params:{id:'a'.repeat(40)},headers:{authorization:'Bearer session'}},res);assert.equal(status,404);assert.equal(payload.error,'not_found');
});
