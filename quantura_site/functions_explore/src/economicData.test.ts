import test from "node:test";
import assert from "node:assert/strict";
import {createEconomicClient,normalizeWorldBankSeries,normalizeFiscalSeries,validateEconomicSource,worldBankPeriod,type EconomicSource} from "./economicData";
import {createGeminiMarketClient,normalizeGeminiCandles} from "./geminiMarketData";
import {predictionPeriods} from "./forecastFrequency";

const source:EconomicSource={type:"economic_series",provider:"worldbank_data360",dataset_id:"WB_WDI",indicator_id:"WB_WDI_TEST",ref_area:"USA",dimensions:{FREQ:"A"}};
const row=(period:string,value:any,extra={})=>({DATABASE_ID:"WB_WDI",INDICATOR:"WB_WDI_TEST",REF_AREA:"USA",FREQ:"A",TIME_PERIOD:period,OBS_VALUE:value,UNIT_MEASURE:"USD",UNIT_MULT:3,OBS_STATUS:"A",...extra});
test("Data360 sorts real periods, applies unit multiplier, excludes missing/future/forecast and preserves zero",()=>{
  const r=normalizeWorldBankSeries([row("2024","3"),row("2020","0"),row("2023",null),row("2021","null"),row("2022","1",{OBS_STATUS:"F"}),row("2026","4")],source,Date.parse("2025-10-01"));
  assert.deepEqual(r.rows.map(r=>[r.TIME_PERIOD,r.target]),[["2020",0],["2024",3000]]);assert.equal(r.skipped,4);assert.equal(r.frequency,"1YE-DEC");
});
test("Data360 rejects mixed dimensions, incorrect series and duplicate periods rather than summing them",()=>{
  assert.throws(()=>normalizeWorldBankSeries([row("2020",1,{SEX:"M"}),row("2021",2,{SEX:"F"})],source,Date.now()),/multiple series/);
  assert.throws(()=>normalizeWorldBankSeries([row("2020",1,{REF_AREA:"CAN"})],source,Date.now()),/different selected/);
  assert.throws(()=>normalizeWorldBankSeries([row("2020",1),row("2020",1)],source,Date.now()),/same reporting/);
});
test("reporting period ends and prediction horizons use real calendar month, quarter and year lengths",()=>{
  assert.equal(worldBankPeriod("2024-M02","M").timestamp,"2024-02-29T00:00:00.000Z");
  assert.equal(worldBankPeriod("2024-Q1","Q").timestamp,"2024-03-31T00:00:00.000Z");
  assert.equal(predictionPeriods(Date.parse("2024-01-31"),Date.parse("2024-03-31"),"1ME"),2);
  assert.equal(predictionPeriods(Date.parse("2024-03-31"),Date.parse("2024-12-30"),"1QE-DEC"),2);
  assert.equal(predictionPeriods(Date.parse("2024-12-31"),Date.parse("2027-12-31"),"1YE-DEC"),3);
});
test("Treasury string nulls remain missing, selected dimensions are enforced, and cash units are million USD",()=>{
  const s:EconomicSource={type:"economic_series",provider:"fiscaldata",series_id:"operating_cash_balance",value_field:"open_today_bal",dimensions:{account_type:"TGA"}};
  const r=normalizeFiscalSeries([{record_date:"2024-01-02",account_type:"TGA",open_today_bal:"null"},{record_date:"2024-01-01",account_type:"TGA",open_today_bal:"0"}],s,Date.now());
  assert.equal(r.length,1);assert.equal(r[0].target,0);assert.equal(r[0].units,"million USD");
  assert.throws(()=>normalizeFiscalSeries([{record_date:"2024-01-01",account_type:"other",open_today_bal:"2"}],s,Date.now()),/different selected/);
});
test("Economic input rejects arbitrary endpoints and filter injection",()=>{
  assert.throws(()=>validateEconomicSource({...source,url:"http://localhost"}),/supported/);
  assert.throws(()=>validateEconomicSource({...source,dimensions:{SEX:"_T,AGE:eq:any"}}),/dimension/);
  assert.throws(()=>validateEconomicSource({...source,start:"2024-02-30"}),/valid date/);
});
test("Data360 pagination follows count and offset, does not assume returned ordering, and coalesces requests",async()=>{
  const calls:string[]=[];
  const request:typeof fetch=async(url,init)=>{
    const u=new URL(String(url));calls.push(u.pathname+u.search);const path=u.pathname.split("/").at(-1);
    if(path==="metadata")return Response.json({value:[{series_description:{idno:"WB_WDI_TEST",database_id:"WB_WDI",name:"Test",periodicity:"Annual"}}]});
    if(path==="disaggregation")return Response.json([{field_name:"FREQ",field_value:["A"]},{field_name:"REF_AREA",field_value:["USA"]}]);
    assert.equal(init?.headers && (init.headers as any).Origin,"https://data360.worldbank.org");
    const skip=Number(u.searchParams.get("skip"));return Response.json({count:2,value:skip===0?[row("2021","2")]:[row("2020","1")]});
  };
  const client=createEconomicClient(request);
  const [a,b]=await Promise.all([client.history(source),client.history(source)]);
  assert.equal(calls.length,4);assert.deepEqual(a.rows.map(r=>r.TIME_PERIOD),["2020","2021"]);assert.deepEqual(a,b);
});
test("Gemini market discovery includes nested prediction contracts and never calls a trading endpoint",async()=>{
  const calls:string[]=[];const client=createGeminiMarketClient(async url=>{
    const u=new URL(String(url));calls.push(u.pathname);
    return Response.json(u.pathname==="/v1/symbols"?["btcusd"]:{data:[{ticker:"TEST",title:"Example",contracts:[],events:[{ticker:"TEST-CHILD",contracts:[{id:"123",instrumentSymbol:"GEMI-TEST-YES",label:"Example Yes",prices:{buy:{yes:".6",no:".4"}}}]}]}]});
  });
  const rows=await client.search("BTC");assert.equal(rows.length,2);assert.equal(rows[1].forecast_available,false);assert.equal(rows[1].event_id,"TEST-CHILD");assert.deepEqual(calls,["/v1/symbols","/v1/prediction-markets/events"]);
});
test("Gemini uses live provider interval aliases and excludes incomplete candles",async()=>{
  const time=Date.parse("2024-01-01T00:00:00Z"),calls:string[]=[];
  const client=createGeminiMarketClient(async url=>{calls.push(String(url));return Response.json(String(url).endsWith("/symbols")?["btcusd"]:[[time,1,2,.5,1.5,3],[time+3600000,2,3,1,2,4]]);});
  const history=await client.history({symbol:"BTCUSD",frequency:"1h",end:"2024-01-01T01:30:00Z"});
  assert.equal(history.rows.length,1);assert.ok(calls[1].endsWith("/1hr"));
  assert.throws(()=>normalizeGeminiCandles([[time,1,2,.5,1.5,3],[time,1,2,.5,1.5,3]],"1h",Date.now()),/duplicate/);
});
