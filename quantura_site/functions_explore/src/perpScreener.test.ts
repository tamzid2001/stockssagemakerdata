import test from "node:test";
import assert from "node:assert/strict";
import { gzipSync } from "node:zlib";
import { PERP_ENGINE, validPerpSnapshot, perpScreenerDataset } from "./perpScreener";
import { perpForecastSnapshot } from "./screenerForecast";
import { parseQuantScreenerQuery, rowMatchesQuery } from "./quantScreener";
function fixture():any {
  const cutoff=Math.floor(Date.now()/86400000)*86400000;
  const history=Array.from({length:40},(_,i)=>[new Date(cutoff-(39-i)*86400000).toISOString(),100]);
  const names=["prophet","toto","granite","chronos","timesfm"];
  const q={p01:80,p25:90,p50:100,p75:110,p90:120,p99:130};
  return {ticker:"KXBTCPERP",scan_id:"perps-test",forecast_engine:PERP_ENGINE,status:"success",forecast_input_gzip:gzipSync(JSON.stringify(history)).toString("base64"),
    last_forecast_update:new Date(cutoff).toISOString(),forecast_provenance:{runtime:{mock:false}},forecast_config:{prediction_length:7,frequency:"1D",calendar:"NONE",horizon_mode:"frequency_periods",quantiles:[.01,.25,.5,.75,.9,.99],models:Object.fromEntries(names.map(id=>[id,{enabled:true,weight:.2}]))},
    forecast_models:names.map(id=>({id,status:"completed"})),forecast_rows:Array.from({length:7},(_,i)=>({timestamp:new Date(cutoff+(i+1)*86400000).toISOString(),...q})),quantile_stats:Object.fromEntries(Object.entries(q).map(([k,v])=>[k,{min:v,max:v,avg:v}])),...q};
}
test("current real snapshots enable six quantile filters and preserve history in Forecast",async()=>{
  const row=fixture(),db:any={collection:()=>({limit:()=>({get:async()=>({docs:[{data:()=>row}]})})})};
  const service:any={screener:async()=>({items:[{ticker:row.ticker,actual_price:105,p50:null}],manifest:{}})};
  const data=await perpScreenerDataset(db,service),merged=data.items[0];
  assert.equal(merged.p50,100);assert.match(String(merged.forecast_view_url),/screenerScan=perps-test/);
  assert.equal(rowMatchesQuery(merged,parseQuantScreenerQuery({quantileRules:JSON.stringify([{quantile:"p50",statistic:"max",operator:"lt",percent:0}])}).query),true);
  const snapshot=perpForecastSnapshot(data,row.ticker,row.scan_id);
  assert.equal(snapshot.history.length,40);assert.equal(snapshot.result.predictions.length,7);assert.equal(snapshot.result.predictions[0].quantiles["0.99"],130);
});
test("expired, reduced-model, mock and malformed snapshots cannot become forecasts",()=>{
  for(const mutation of [(r:any)=>r.forecast_models.pop(),(r:any)=>r.forecast_provenance.runtime.mock=true,(r:any)=>r.forecast_rows[0].p01=200,(r:any)=>r.forecast_rows[0].timestamp=r.forecast_rows[1].timestamp]){
    const row=fixture();mutation(row);assert.equal(validPerpSnapshot(row),false);
  }
  const row=fixture();assert.equal(validPerpSnapshot(row,Date.now()+25*86400000),false);
  assert.throws(()=>perpForecastSnapshot({items:[row]} as any,row.ticker,"perps-old"),/not_found/);
});
