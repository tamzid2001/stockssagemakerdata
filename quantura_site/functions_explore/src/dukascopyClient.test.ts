import test from "node:test";
import assert from "node:assert/strict";
import { DukascopyClient, decodeDukascopyCandles, aggregateDukascopy, dukascopyFrequency, dukascopy } from "./dukascopyClient";
import snapshot from "./dukascopyInstruments.json";
import { fetchStockHistoryData } from "./marketDataRoutes";
import { tickerOverlayRows } from "./ensembleForecastRoutes";

const encoded=(prices:number[],time=Date.parse("2025-01-02T00:00Z"),step=1,scale=3)=>({
  timestamp:time,multiplier:10**-scale,shift:60_000,open:prices[0],high:prices[0],low:prices[0],close:prices[0],
  times:prices.map((_,i)=>i?step:0),opens:prices.map((p,i)=>i?Math.round((p-prices[i-1])*10**scale):0),
  highs:prices.map((p,i)=>i?Math.round((p-prices[i-1])*10**scale):0),lows:prices.map((p,i)=>i?Math.round((p-prices[i-1])*10**scale):0),
  closes:prices.map((p,i)=>i?Math.round((p-prices[i-1])*10**scale):0),volumes:prices.map(()=>1),
});
const gold=snapshot.instruments.find(x=>x.code==="XAU-USD")!;
const meta={...gold,histories:[{period:"MINUTE",from:Date.parse("2003-05-05")},{period:"HOUR",from:Date.parse("2003-05-05")}],tradeSchedule:[{sessions:{MONDAY:[{start:"18:00:00",end:"17:00:00"}]}}]};
function fixture(history=(path:string)=>encoded([2623.655,2625.185,2632.735],Date.parse("2025-01-02"),60),instrumentMetadata=meta) {
  const calls:string[]=[];
  const request=(async(url:any)=>{const path=new URL(String(url)).pathname.replace("/v1","");calls.push(path);
    if(path==="/instruments")return Response.json(snapshot);
    if(path==="/instruments/XAU-USD")return Response.json(instrumentMetadata);
    return Response.json(history(path));}) as typeof fetch;
  return {client:new DukascopyClient(request),calls};
}
test("signed deltas preserve gold, JPY and ordinary FX decimal scales",()=>{
  for(const [prices,scale] of [[[2623.655,2625.185,2622.828],3],[[157.7,157.427,157.5],3],[[1.03508,1.0352,1.03472],5]] as [number[],number][]){
    assert.deepEqual(decodeDukascopyCandles(encoded(prices,undefined,1,scale),scale).map(x=>x.close),prices);
  }
  const payload=encoded([100,101]);payload.closes[1]=NaN;assert.throws(()=>decodeDukascopyCandles(payload,3),/invalid candle/);
  const truncated=encoded([100,101]);truncated.times.pop();assert.throws(()=>decodeDukascopyCandles(truncated,3),/invalid candle/);
});
test("weekly daily archives preserve cross-year weeks when download pages are combined",async()=>{
  const {client,calls}=fixture(path=>{
    assert.match(path,/^\/candles\/day\/XAU-USD\/BID\/\d{4}$/);
    const year=Number(path.split("/").at(-1)),start=Date.UTC(year,0,1),days=(Date.UTC(year+1,0,1)-start)/86400000;
    return encoded(Array.from({length:days},(_,i)=>100+i),start,1440);
  },{...meta,histories:[{period:"DAY",from:Date.parse("2003-05-05")}]});
  const input={symbol:"XAUUSD",start:"2014-01-01",end:"2026-01-05T00:00:00Z",timeframe:"1Week",limit:0};
  const full=await client.history(input),combined=[];
  let cursor;
  do{const page=await client.history({...input,cursor},true);combined.push(...page.rows);cursor=page.next_cursor;}while(cursor);
  assert.deepEqual(combined,full.rows);
  assert.equal(new Set(combined.map(row=>row.timestamp)).size,combined.length);
  assert.ok(combined.some(row=>row.timestamp==="2020-12-28T00:00:00.000Z"));
  assert.ok(calls.filter(path=>path.includes("/candles/")).every(path=>path.includes("/candles/day/")));
});
test("UTC aggregation takes the chronological last observed close, skips open buckets and leaves gaps",()=>{
  const start=Date.parse("2025-01-02"),rows=decodeDukascopyCandles(encoded([100,102,101],start,14),3);
  const output=aggregateDukascopy([...rows].reverse(),15,start,start+30*60_000,1);
  assert.deepEqual(output.map(x=>[x.timestamp,x.open,x.high,x.low,x.close]),[["2025-01-02T00:00:00.000Z",100,102,100,102],["2025-01-02T00:15:00.000Z",101,101,101,101]]);
  assert.equal(aggregateDukascopy(rows,15,start,start+29*60_000,1).length,1);
  assert.equal(aggregateDukascopy(rows,15,start+60_000,start+30*60_000,1).length,1);
});
test("the full published catalog is searchable, including compact symbols and CFD distinctions",async()=>{
  const {client}=fixture();
  assert.equal((await client.catalog()).instruments.length,1504);
  for(const symbol of ["XAUUSD","USDJPY","EURUSD","USA500IDXUSD","AAPLUSUSD","BTCUSD"]){
    const found=await client.search(symbol);assert.ok(found.rows.length);assert.equal(found.rows[0].source,"dukascopy");
  }
  assert.equal((await client.search("APPLE")).rows[0].asset_class,"equity_cfd");
  await assert.rejects(client.history({symbol:"FAKEUNKNOWN",start:"2025-01-02",end:"2025-01-03"}),/published instrument catalog/);
});
test("H1 uses monthly candles, date-only ends are inclusive, bid/ask and price provenance are preserved",async()=>{
  const {client,calls}=fixture();
  const r=await client.history({symbol:"XAUUSD",start:"01-02-2025",end:"01-02-2025",timeframe:"H1",price_side:"ask",limit:0});
  assert.equal(r.rows.length,3);assert.equal(r.priceSide,"ask");assert.equal(r.metadata.price_scale,3);
  assert.equal(r.metadata.range_end,"2025-01-03T00:00:00.000Z");assert.ok(calls.includes("/candles/trade/hour/XAU-USD/ASK/2025/1"));
  assert.deepEqual(dukascopyFrequency("H4"),["4Hour",240]);
  await assert.rejects(client.history({symbol:"XAUUSD",start:"2025-02-30",end:"2025-03-03"}),/valid date/);
  await assert.rejects(client.history({symbol:"XAUUSD",start:"2025-01-02",end:"2025-01-03",price_side:"mid"}),/bid or ask/);
});
test("gold forecasts bound the historical range to N and use verified hourly files without schedule metadata",async()=>{
  const {client,calls}=fixture(undefined,{...meta,tradeSchedule:[]});
  await client.history({symbol:"XAUUSD",start:"2003-01-01",end:"2025-01-04",timeframe:"1Day",limit:500},false,true);
  const files=calls.filter(path=>path.includes("/candles/"));
  assert.ok(files.length<60,"does not download two decades of minute files");
  assert.ok(files.every(path=>path.includes("/trade/hour/")));
  assert.ok(files.every(path=>Number(path.split("/").at(-2))>=2020));
});
test("nonaligned provider hours fall back to real minute candles instead of relabeling closes",async()=>{
  const {client,calls}=fixture(path=>encoded([100,101],Date.parse(path.includes("/trade/hour/")?"2025-01-02T00:30Z":"2025-01-02T00:00Z"),60));
  const result=await client.history({symbol:"XAUUSD",start:"2025-01-02",end:"2025-01-03",timeframe:"1Hour",limit:500},false,true);
  assert.equal(result.metadata.base_interval,"1Min");
  assert.ok(calls.some(path=>path.includes("/candles/minute/")));
  assert.equal(result.rows[0].timestamp,"2025-01-02T00:00:00.000Z");
});
test("paged ranges carry a settings-bound cursor and never treat upstream failures as empty history",async()=>{
  const {client,calls}=fixture(()=>encoded([]));
  const input={symbol:"XAUUSD",start:"2023-01-01",end:"2024-01-31",timeframe:"H1",limit:0};
  const first=await client.history(input,true);assert.equal(first.metadata.completed_files,12);assert.ok(first.next_cursor);
  const last=await client.history({...input,cursor:first.next_cursor},true);assert.equal(last.next_cursor,null);assert.equal(last.metadata.completed_files,13);
  assert.equal(calls.filter(x=>x.includes("/candles/")).length,13);
  await assert.rejects(client.history({...input,cursor:first.next_cursor,price_side:"ask"},true),/cursor/);
  let upstream=0;
  const bad=new DukascopyClient((async(url:any)=>{if(String(url).endsWith("/instruments"))return Response.json(snapshot);if(String(url).includes("/instruments/"))return Response.json(meta);upstream++;return new Response("failure",{status:500});}) as typeof fetch);
  await assert.rejects(bad.history({symbol:"XAUUSD",start:"2025-01-02",end:"2025-01-03"}),/incomplete export/);assert.equal(upstream,1);
});
test("rate limits respect Retry-After and shared history requests coalesce",async()=>{
  let count=0;
  const c=new DukascopyClient((async()=>{count++;return new Response("busy",{status:429,headers:{"Retry-After":"3600"}});}) as typeof fetch);
  await Promise.all([c.catalog(),c.catalog()]);assert.equal(count,1);
  await assert.rejects(c.history({symbol:"XAUUSD",start:"2025-01-02",end:"2025-01-03"}),/rate limited/);assert.equal(count,1);
});
test("a paged download ending today freezes its cutoff while the clock advances",async()=>{
  const original=Date.now;
  let now=Date.parse("2026-09-26T12:00:00Z");Date.now=()=>now;
  try {
    const {client}=fixture(()=>encoded([]));
    const input={symbol:"XAUUSD",start:"2025-01-01",end:"2026-09-26",timeframe:"H1",limit:0};
    const first=await client.history(input,true);assert.ok(first.next_cursor);
    now+=120_000;
    const last=await client.history({...input,cursor:first.next_cursor},true);
    assert.equal(last.metadata.range_end,first.metadata.range_end);
    assert.equal(last.next_cursor,null);
    await assert.rejects(client.history({...input,end:"2026-09-25",cursor:first.next_cursor},true),/cursor/);
  } finally {Date.now=original;}
});
test("forecast overlays use completed UTC day ends, not NYSE dates, and keep the selected ask side",async()=>{
  const original=dukascopy.history,seen:any[]=[];
  dukascopy.history=async(input:any)=>{seen.push(input);return {provider:"dukascopy",sourceRequested:"dukascopy",fallbackUsed:false,exchangeTimezone:"UTC",barIntervalMinutes:input.timeframe==="1Min"?1:1440,timeframe:input.timeframe,symbol:"XAU-USD",adjustment:"raw",feed:"ask",session:"provider",rows:decodeDukascopyCandles(encoded([100],Date.parse("2025-01-02")),3)};};
  try{
    const r=await fetchStockHistoryData({source:"dukascopy",symbol:"XAUUSD",timeframe:"1Day",price_side:"ask"});assert.equal(r.provider,"dukascopy");
    const rows=await tickerOverlayRows({provider:"dukascopy",symbol:"XAU-USD",price_side:"ask"},"1D",Date.parse("2025-01-02T12:00Z"),Date.parse("2025-01-03T00:00Z"));
    assert.equal(rows.length,1);assert.equal(rows[0].timestamp,"2025-01-03T00:00:00.000Z");assert.equal(seen.at(-1).price_side,"ask");
  }finally{dukascopy.history=original;}
});
