import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { eventMarketPage, contractGroup } from "./qSearchEvents";
import { rankVerifiedCandidates } from "./qSearchRanking";
import { JEV_MODEL, verifiedChoice } from "./jevClient";
import { normalizePolymarketEvents } from "./predictionMarketData";
import { registerMarketSearchRoutes } from "./marketSearch";

const response=(body:unknown)=>new Response(JSON.stringify(body),{headers:{"Content-Type":"application/json"}});
const pmEvent=(count:number)=>({id:"e",slug:"game",title:"Home vs Away",markets:Array.from({length:count},(_,i)=>({id:`m${i}`,slug:`game-${i}`,title:`Player ${i} total points`,sportsMarketTypeV2:i?"PLAYER_POINTS":"SPORTS_MARKET_TYPE_MONEYLINE",marketSides:[{id:`${i}-yes`,long:true,description:"Yes"},{id:`${i}-no`,long:false,description:"No"}]}))});

test("Q Search pages every prop/outcome without the legacy 100-side moneyline truncation",async()=>{
  const request=(async()=>response({event:pmEvent(153)})) as typeof fetch;
  const seen=new Set();let cursor="",pages=0;
  do{const p=await eventMarketPage("polymarket_us","game",cursor,request);for(const c of p.contracts)seen.add(c.contractId);cursor=p.next_cursor||"";pages++;assert.ok(p.contracts.length<=100);assert.equal(p.total_contracts,306);}while(cursor);
  assert.equal(pages,4);assert.equal(seen.size,306);
});
test("soccer retains six distinct outcome contracts and grouping doesn't modify identity",()=>{
  const event=pmEvent(3);event.markets.forEach(m=>m.sportsMarketTypeV2="SPORTS_MARKET_TYPE_DRAWABLE_OUTCOME");
  const rows=normalizePolymarketEvents({events:[event]},{id:"soccer",label:"Soccer",sport:"Soccer",providerId:""});
  assert.equal(new Set(rows.map(c=>c.contractId)).size,6);assert.ok(rows.every(c=>contractGroup(c)==="Moneyline"));
  assert.equal(rows[0].eventSlug,"game");assert.notEqual(rows[0].outcome,rows[1].outcome);
});
test("Kalshi relationships use official game milestones, exclude tournament-only links, page archived contracts",async()=>{
  const urls:string[]=[];
  const request=(async(input:any)=>{
    const u=new URL(String(input));urls.push(u.toString());
    if(u.pathname.includes("/events/"))return response({event:{event_ticker:"KXGAME-A",title:"A vs B",series_ticker:"KXGAME"}});
    if(u.pathname.endsWith("/milestones"))return response({milestones:[{type:"football_game",related_event_tickers:["KXGAME-A","KXPLAYER-A"]},{type:"football_tournament",related_event_tickers:["KXGAME-A","KXOTHER-SEASON"]}]});
    return response({markets:[{event_ticker:"KXGAME-A",ticker:u.pathname.includes("historical")?"KXGAME-A-OLD":"KXGAME-A-NEW",market_type:"binary",last_price_dollars:"0.45"},{event_ticker:"FOREIGN",ticker:"FOREIGN"}]});
  }) as typeof fetch;
  const first=await eventMarketPage("kalshi","KXGAME-A","",request);
  assert.deepEqual(first.related_events.map(r=>r.event_id),["KXPLAYER-A"]);assert.equal(first.contracts.length,2);assert.ok(first.next_cursor);
  const archived=await eventMarketPage("kalshi","KXGAME-A",first.next_cursor!,request);
  assert.equal(archived.contracts[0].marketId,"KXGAME-A-OLD");assert.equal(archived.next_cursor,null);assert.ok(urls.every(u=>!u.includes("FOREIGN")));
});
test("event queries reject path traversal, cross-event cursors and unknown providers before network",async()=>{
  const request=(async()=>{throw Error("unexpected network");}) as typeof fetch;
  for(const [source,id,cursor] of [["kalshi","../secret",""],["evil","game",""],["polymarket_us","game",Buffer.from(JSON.stringify({source:"kalshi",id:"other",phase:"live",offset:0,cursor:""})).toString("base64url")]])await assert.rejects(eventMarketPage(source,id,cursor,request),/Choose a provider/);
});
test("Jev typed choice rejects unknown IDs, NaN, wrong model and probability mismatch",()=>{
  const valid={model:JEV_MODEL,answers:{market:{type:"choice",choice:"one",confidence:.9,probabilities:{one:.9,unknown:.1}}}};
  assert.equal(verifiedChoice(valid,"market",["one","unknown"]),"one");
  for(const result of [{...valid,model:"latest"},{...valid,answers:{market:{...valid.answers.market,choice:"invented"}}},{...valid,answers:{market:{...valid.answers.market,confidence:NaN}}},{...valid,answers:{market:{...valid.answers.market,probabilities:{one:.2,unknown:.1}}}}])assert.equal(verifiedChoice(result,"market",["one","unknown"]),null);
});
test("Jev ranks only verified candidates; outage and exact symbols preserve deterministic selection",async()=>{
  const rows=[{resource_id:"kalshi:one",name:"Home points",symbol:"ONE"},{resource_id:"polymarket_us:two",name:"Away points",symbol:"TWO"}];let calls=0;
  const decide=async()=>{calls++;return {model:JEV_MODEL,answers:{market:{type:"choice",choice:"candidate_1",confidence:.9,probabilities:{unknown:.05,candidate_0:.05,candidate_1:.9}}}};};
  assert.equal(await rankVerifiedCandidates("Away player points",rows,{decide}),"polymarket_us:two");
  assert.equal(await rankVerifiedCandidates("ONE",rows,{decide}),null);assert.equal(calls,1);
  assert.equal(await rankVerifiedCandidates("different ambiguous player",rows,{decide:async()=>{throw Error("timeout");}}),null);
});
test("Q Search HTTP errors and capabilities expose no secrets or arbitrary source URLs",async()=>{
  const app=express();registerMarketSearchRoutes(app);const server=app.listen(0,"127.0.0.1");await new Promise<void>(r=>server.once("listening",r));
  const port=(server.address() as any).port;
  try{const res=await fetch(`http://127.0.0.1:${port}/market-search/event?source=evil&event_id=../secret`);assert.equal(res.status,422);assert.equal((await res.json() as any).error,"event_query_invalid");
    const caps=await fetch(`http://127.0.0.1:${port}/market-search/capabilities`);assert.equal(caps.status,200);assert.equal(Object.keys((await caps.json() as any).providers).length,5);
  }finally{server.close();}
});
