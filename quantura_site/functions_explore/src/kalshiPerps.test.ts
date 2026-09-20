import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import { KalshiPerpsService, normalizePerpCandles, normalizePerpMarket, perpFrequency, perpTicker, registerKalshiPerpsRoutes } from "./kalshiPerps";
const market={ticker:"KXBTCPERP",title:"0.0001 BTC",status:"active",price:"8.0950",contract_size:"0.000100",underlying_multiplier:"1.000000"};
const bar=(ts:number,close:unknown="8.1")=>({end_period_ts:ts,price:{open:"8",high:"8.2",low:"7.9",close,previous:"500"},volume:"20"});

test("perpetual prices retain USD contract units, not spot or binary probability",()=>{
  const normalized=normalizePerpMarket(market);assert.equal(normalized.last_trade_price,8.095);assert.equal(normalized.contract_size,0.0001);
  assert.equal(normalized.asset_class,"perpetual");assert.equal(normalized.resource_type,"perpetual_contract");
  for(const v of ["../../orders","BTC","KXBTC15M-OTHER"])assert.throws(()=>perpTicker(v));
  assert.equal(perpFrequency("1Day"),"1D");assert.throws(()=>perpFrequency("5min"));
});
test("trade closes are sorted, cutoff-bounded, duplicate checked, without null/previous/quote filling",()=>{
  const rows=normalizePerpCandles([bar(180),bar(60),bar(120,null),bar(240),bar(60)],60,180);
  assert.deepEqual(rows.map(r=>r.timestamp),[new Date(60000).toISOString(),new Date(180000).toISOString()]);
  assert.deepEqual(rows.map(r=>r.close),[8.1,8.1]);
  assert.throws(()=>normalizePerpCandles([bar(60),bar(60,"9")],0,100),/conflict/);
  assert.equal(normalizePerpCandles([bar(60,"NaN"),bar(120,"")],0,200).length,0);
});
test("history uses bounded time-window paging, matching ticker and coalesced cache",async()=>{
  const now=1789919400000;let calls=0;
  const request:typeof fetch=async(url,init)=>{
    calls++;const u=new URL(String(url));assert.equal(init?.redirect,"error");assert.equal(new Headers(init?.headers).get("Authorization"),null);
    if(u.pathname.endsWith("/margin/markets"))return Response.json({markets:[market]});
    assert.equal(u.searchParams.get("include_latest_before_start"),"false");
    return Response.json({ticker:market.ticker,candlesticks:[bar(Number(u.searchParams.get("end_ts")))]});
  };
  const service=new KalshiPerpsService(request,()=>now);
  const [a,b]=await Promise.all([service.history({symbol:market.ticker,frequency:"1min",limit:2}),service.history({symbol:market.ticker,frequency:"1min",limit:2})]);
  assert.deepEqual(a,b);assert.equal(a.rows.length,2);assert.equal(a.metadata.pages,2);assert.equal(calls,3);
  assert.equal((await service.search("btc"))[0].symbol,market.ticker);
  await assert.rejects(service.history({symbol:market.ticker,limit:50001}),/1–5000/);
  const bad=new KalshiPerpsService(async url=>Response.json(String(url).endsWith("/markets")?{markets:[market]}:{ticker:"KXETHPERP",candlesticks:[]}),()=>now);
  await assert.rejects(bad.history({symbol:market.ticker}),/ticker_mismatch/);
});
test("catalog screener never labels latest trades as closes or fabricates quantiles",async()=>{
  const service=new KalshiPerpsService(async url=>Response.json(String(url).endsWith("/markets")?{markets:[market]}:{ticker:market.ticker,candlesticks:[bar(1789919400)]}),()=>1789919400000);
  const dataset=await service.screener();assert.equal(dataset.items[0].actual_price,8.1);assert.equal(dataset.items[0].p50,null);
  assert.match(String(dataset.items[0].forecast_view_url),/marketSource=kalshi_perps/);
});
test("perpetual history HTTP JSON/CSV and structured errors use same service",async()=>{
  const service=new KalshiPerpsService(async url=>Response.json(String(url).endsWith("/markets")?{markets:[market]}:{ticker:market.ticker,candlesticks:[bar(1789919400)]}),()=>1789919400000);
  const app=express();app.use(express.json());registerKalshiPerpsRoutes(app,service);
  const server=app.listen(0,"127.0.0.1");await new Promise<void>(r=>server.once("listening",r));
  const url=`http://127.0.0.1:${(server.address() as any).port}/market-data/perps/history`;
  try{
    const response=await fetch(`${url}?symbol=KXBTCPERP&limit=1`);assert.equal(response.status,200);assert.equal((await response.json()).rows[0].close,8.1);
    const csv=await fetch(`${url}?symbol=KXBTCPERP&limit=1&format=csv`);assert.match(await csv.text(),/timestamp,open,high,low,close,volume/);assert.equal(csv.headers.get("x-price-unit"),"USD per contract");
    assert.equal((await fetch(`${url}?symbol=KXBTCPERP&frequency=2min`)).status,422);
  }finally{await new Promise<void>(r=>server.close(()=>r()));}
});
