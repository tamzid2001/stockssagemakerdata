import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import { KalshiPerpsService, normalizePerpCandles, normalizePerpMarket, perpFrequency, perpTicker, registerKalshiPerpsRoutes } from "./kalshiPerps";
const market={ticker:"KXBTCPERP",title:"0.0001 BTC",status:"active",price:"8.0950",contract_size:"0.000100",underlying_multiplier:"1.000000",reference_price:{price:"8.1000",ts_ms:1789919400000}};
const bar=(ts:number,close:unknown="8.1")=>({end_period_ts:ts,price:{open:"8",high:"8.2",low:"7.9",close,previous:"500"},volume:"20"});

test("perpetual prices normalize contract values to the underlying spot scale",()=>{
  const normalized=normalizePerpMarket(market);assert.equal(normalized.last_trade_price,80950);assert.equal(normalized.spot_reference_price,81000);assert.equal(normalized.last_trade_contract_price,8.095);assert.equal(normalized.contract_size,0.0001);
  assert.equal(normalized.underlying_units_per_contract,0.0001);assert.equal(normalized.unit,"USD per underlying unit");
  assert.equal(normalized.asset_class,"perpetual");assert.equal(normalized.resource_type,"perpetual_contract");
  for(const v of ["../../orders","BTC","KXBTC15M-OTHER"])assert.throws(()=>perpTicker(v));
  assert.equal(perpFrequency("1Day"),"1D");assert.equal(perpFrequency("5min"),"5min");assert.equal(perpFrequency("1Month"),"1MS");assert.throws(()=>perpFrequency("2min"));
});
test("trade closes are sorted, cutoff-bounded, duplicate checked, without null/previous/quote filling",()=>{
  const rows=normalizePerpCandles([bar(180),bar(60),bar(120,null),bar(240),bar(60)],60,180,.0001);
  assert.deepEqual(rows.map(r=>r.timestamp),[new Date(60000).toISOString(),new Date(180000).toISOString()]);
  assert.deepEqual(rows.map(r=>r.close),[81000,81000]);
  assert.throws(()=>normalizePerpCandles([bar(60),bar(60,"9")],0,100,.0001),/conflict/);
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
  assert.deepEqual(a,b);assert.equal(a.rows.length,2);assert.equal(a.rows[0].close,81000);assert.equal(a.metadata.units,"USD per underlying unit");assert.equal(a.metadata.pages,2);assert.equal(calls,3);
  assert.equal((await service.search("btc"))[0].symbol,market.ticker);
  await assert.rejects(service.history({symbol:market.ticker,limit:50001}),/1–5000/);
  const bad=new KalshiPerpsService(async url=>Response.json(String(url).endsWith("/markets")?{markets:[market]}:{ticker:"KXETHPERP",candlesticks:[]}),()=>now);
  await assert.rejects(bad.history({symbol:market.ticker}),/ticker_mismatch/);
});
test("catalog screener displays timestamped reference spot and never fabricates quantiles",async()=>{
  const service=new KalshiPerpsService(async url=>Response.json(String(url).endsWith("/markets")?{markets:[market]}:{ticker:market.ticker,candlesticks:[bar(1789919400)]}),()=>1789919400000);
  const dataset=await service.screener();assert.equal(dataset.items[0].actual_price,81000);assert.equal(dataset.items[0].quote_source,"kalshi_perps_reference_spot");assert.equal(dataset.items[0].p50,null);
  assert.match(String(dataset.items[0].forecast_view_url),/marketSource=kalshi_perps/);
});
test("five-minute perpetual bars use completed native closes and omit empty and open buckets",async()=>{
  const start=Date.parse("2026-09-21T00:00:00Z"),end=start+12*60000;
  const service=new KalshiPerpsService(async url=>{
    if(String(url).endsWith("/markets"))return Response.json({markets:[market]});
    assert.equal(new URL(String(url)).searchParams.get("period_interval"),"1");
    return Response.json({ticker:market.ticker,candlesticks:[bar(start/1000+60,"8.1"),bar(start/1000+300,"8.2"),bar(start/1000+660,"8.3")]});
  },()=>end);
  const result=await service.history({symbol:market.ticker,frequency:"5m",start,end,limit:5});
  assert.equal(result.frequency,"5min");assert.equal(result.rows.length,1);
  assert.equal(result.rows[0].timestamp,new Date(start+300000).toISOString());assert.equal(result.rows[0].close,82000);
});
test("default monthly perpetual history stays bounded and reads UTC hour closes",async()=>{
  const now=Date.parse("2026-09-21T00:00Z");let candleCalls=0;
  const service=new KalshiPerpsService(async url=>{
    if(String(url).endsWith("/markets"))return Response.json({markets:[market]});
    const query=new URL(String(url)).searchParams;
    assert.equal(query.get("period_interval"),"60");assert.ok(Number(query.get("start_ts"))>=0);candleCalls++;
    return Response.json({ticker:market.ticker,candlesticks:[]});
  },()=>now);
  const result=await service.history({symbol:market.ticker,frequency:"1Month"});
  assert.equal(result.frequency,"1MS");assert.equal(result.rows.length,0);assert.equal(result.metadata.base_frequency,"1h");assert.ok(candleCalls<=20);
});
test("perpetual history HTTP JSON/CSV and structured errors use same service",async()=>{
  const service=new KalshiPerpsService(async url=>Response.json(String(url).endsWith("/markets")?{markets:[market]}:{ticker:market.ticker,candlesticks:[bar(1789919400)]}),()=>1789919400000);
  const app=express();app.use(express.json());registerKalshiPerpsRoutes(app,service);
  const server=app.listen(0,"127.0.0.1");await new Promise<void>(r=>server.once("listening",r));
  const url=`http://127.0.0.1:${(server.address() as any).port}/market-data/perps/history`;
  try{
    const response=await fetch(`${url}?symbol=KXBTCPERP&limit=1`);assert.equal(response.status,200);assert.equal((await response.json()).rows[0].close,81000);
    const csv=await fetch(`${url}?symbol=KXBTCPERP&limit=1&format=csv`);assert.match(await csv.text(),/timestamp,open,high,low,close,volume/);assert.equal(csv.headers.get("x-price-unit"),"USD per underlying unit");
    assert.equal((await fetch(`${url}?symbol=KXBTCPERP&frequency=2min`)).status,422);
  }finally{await new Promise<void>(r=>server.close(()=>r()));}
});
