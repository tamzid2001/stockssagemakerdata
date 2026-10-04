import test from "node:test";
import assert from "node:assert/strict";
import {forecastFrequency,frequencyBounds,frequencyEnd,predictionPeriods,aggregateObservedBars} from "./forecastFrequency";
import {fetchStockHistoryData} from "./marketDataRoutes";
import {publicModelCapabilities,resolvePredictionEnd} from "./ensembleForecastRoutes";
import {buildOpenApiDocument} from "./openapi";
const stamp=Date.parse;
const bar=(timestamp:string,close:number)=>({timestamp,open:close,high:close+1,low:close-1,close,volume:1,tradeCount:null,vwap:null,session:"regular" as const});
test("market aliases distinguish minutes, Monday weeks and calendar months",()=>{
  for(const [input,expected] of [["5m","5min"],["15m","15min"],["30m","30min"],["4Hour","4h"],["1w","1W-MON"],["1Month","1MS"]])assert.equal(forecastFrequency(input),expected);
  assert.throws(()=>forecastFrequency("1M"),/frequency_unsupported/);
  assert.deepEqual(frequencyBounds(stamp("2024-02-29T23:59Z"),"1Month"),[stamp("2024-02-01"),stamp("2024-03-01")]);
  assert.deepEqual(frequencyBounds(stamp("2026-01-01"),"1w"),[stamp("2025-12-29"),stamp("2026-01-05")]);
  assert.equal(frequencyEnd(stamp("2025-01-02T10:02Z"),"5m"),stamp("2025-01-02T10:07Z"));
});
test("end-date forecasts count complete calendar periods without a 30-day month",()=>{
  assert.equal(predictionPeriods(stamp("2024-01-01"),stamp("2024-03-01"),"1Month"),2);
  assert.equal(predictionPeriods(stamp("2024-02-01"),stamp("2024-02-29T23:59Z"),"1MS"),0);
  assert.equal(resolvePredictionEnd({prediction_end_at:"2024-03-01T00:00:00Z",calendar:"NONE"},"2024-01-01T00:00Z","1MS").prediction_length,2);
  assert.equal(predictionPeriods(stamp("2024-01-01"),stamp("2024-01-01T06:00Z"),"2h"),3);
});
test("aggregation excludes partial months and native bars crossing a bucket boundary",()=>{
  const rows=[bar("2024-01-02T00:00Z",10),bar("2024-01-31T00:00Z",12),bar("2024-02-01T00:00Z",20)];
  const output=aggregateObservedBars(rows,"1Month",stamp("2024-01-01"),stamp("2024-02-15"),1440);
  assert.equal(output.length,1);assert.equal(output[0].close,12);assert.equal(output[0].open,10);assert.equal(rows[0].close,10);
  assert.equal(aggregateObservedBars([bar("2024-01-01T03:30Z",99)],"4h",stamp("2024-01-01"),stamp("2024-01-01T08:00Z"),60).length,0);
});
test("stock service derives genuine completed months and preserves provider provenance",async()=>{
  const calls:any[]=[];
  const alpaca:any={getStockBars:async(input:any)=>{calls.push(input);return {symbol:"SPY",timeframe:"1Day",feed:"iex",adjustment:"split",session:"regular",rows:[bar("2024-01-02T05:00Z",10),bar("2024-01-31T05:00Z",12),bar("2024-02-02T05:00Z",20)]};}};
  const result=await fetchStockHistoryData({source:"alpaca",symbol:"SPY",timeframe:"1Month",start:"2024-01-01",end:"2024-02-15",limit:500,adjustment:"split"},{alpaca});
  assert.equal(calls[0].timeframe,"1Day");assert.equal(result.provider,"alpaca");assert.equal(result.timeframe,"1Month");assert.equal(result.rows.length,1);assert.equal(result.rows[0].close,12);assert.equal(result.metadata?.completed_bars_only,true);
  assert.equal((publicModelCapabilities().frequencies as unknown[]).length,9);
});
test("API source enums accept every published canonical interval and native UI value",()=>{
  const doc=buildOpenApiDocument() as any;
  const enums=doc.components.schemas.EnsembleForecastRequest.allOf[1].properties.source.oneOf.filter((source:any)=>source.properties?.type?.const==="ticker"||source.properties?.type?.const==="prediction_market").map((source:any)=>source.properties.frequency.enum);
  for(const entry of publicModelCapabilities().frequencies as any[])for(const values of enums){assert.ok(values.includes(entry.frequency));assert.ok(values.includes(entry.timeframe));}
});
