import test from "node:test";
import assert from "node:assert/strict";
import { gzipSync } from "node:zlib";
import { screenerForecastSnapshot } from "./screenerForecast";
const quantiles=[.01,.1,.25,.5,.75,.9,.99];
function dataset():any {
  const history=Array.from({length:40},(_,i)=>[new Date(Date.UTC(2026,0,i+1)).toISOString(),100+i]);
  return {scan_id:"scan",items:[{ticker:"TEST",status:"success",forecast_engine:"quantura_weekly_ensemble_v1",forecast_input_gzip:gzipSync(JSON.stringify(history)).toString("base64"),forecast_config:{quantiles,models:{prophet:{enabled:true,weight:.2}},context_length:512,transform:"log"},forecast_provenance:{runtime:{mock:false}},forecast_rows:Array.from({length:7},(_,i)=>({date:`2026-02-${String(10+i).padStart(2,"0")}`,timestamp:`2026-02-${String(10+i).padStart(2,"0")}T00:00:00Z`,p01:80,p10:90,p25:95,p50:100,p75:105,p90:110,p99:120})),price_source:"alpaca",last_forecast_update:"2026-02-10T01:00:00Z"}]};
}
test("public screener snapshot preserves exact history and quantiles with no new inference",()=>{
  const result=screenerForecastSnapshot(dataset(),"test","scan");assert.equal(result.history.length,40);assert.equal(result.result.predictions.length,7);assert.equal(result.result.predictions[0].quantiles["0.01"],80);assert.equal(result.job.request.toto_variant,"4m");assert.equal(result.job.source.adjustment,"split");
});
test("old scan, absent ticker, mock payload or oversized history cannot become a saved forecast",()=>{
  assert.throws(()=>screenerForecastSnapshot(dataset(),"TEST","old"),/not_found/);
  assert.throws(()=>screenerForecastSnapshot(dataset(),"OTHER","scan"),/not_found/);
  const d=dataset();d.items[0].forecast_provenance.runtime.mock=true;assert.throws(()=>screenerForecastSnapshot(d,"TEST","scan"),/invalid/);
  d.items[0].forecast_provenance.runtime.mock=false;d.items[0].forecast_input_gzip=gzipSync(' '.repeat(200000)).toString("base64");assert.throws(()=>screenerForecastSnapshot(d,"TEST","scan"));
});
