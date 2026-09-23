import test from "node:test";
import assert from "node:assert/strict";
import {gzipSync} from "node:zlib";
import {advanceClosingSignal,decorateScreenerRow} from "./screenerSignals";
import {parseQuantScreenerQuery,rowMatchesQuery,QuantScreenerRow} from "./quantScreener";
const now=Date.parse("2026-09-30T21:00:00Z");
const makeRow=(price=131):QuantScreenerRow=>({
  ticker:"TEST",status:"success",actual_price:price,actual_price_timestamp:"2026-09-18T00:00:00Z",
  history_cutoff_at:"2026-09-18T00:00:00Z",daily_close_at:"2026-09-18T20:00:00Z",last_forecast_update:"2026-09-18T22:00:00Z",
  forecast_engine:"quantura_weekly_ensemble_v2",forecast_config:{history_lag_sessions:0,toto_variant:"4m",
    models:Object.fromEntries(["prophet","toto","granite","chronos","timesfm"].map(n=>[n,{enabled:true,weight:.2}]))},
  forecast_input_gzip:gzipSync(JSON.stringify([["2026-09-17T00:00:00Z",100],["2026-09-18T00:00:00Z",price]])).toString("base64"),
  forecast_rows:[21,22,23,24,25,28,29].map((d,i)=>({date:`2026-09-${d}`,timestamp:`2026-09-${d}T00:00:00Z`,session_open:`2026-09-${d}T13:30:00Z`,session_close:`2026-09-${d}T20:00:00Z`,p01:70,p10:80,p25:90,p50:100,p75:110,p90:120,p99:130+i})),
  quantile_stats:{p50:{min:100,avg:160,max:180}}
});
test("Buy is strictly latest input daily close > FIRST P99; target is LAST P99",()=>{
  for(const [price,value] of [[131,"buy"],[130,"none"],[129,"none"],[1,"none"]] as const){
    const result=decorateScreenerRow(makeRow(price),{price:2000,timestamp:"2026-09-30T20:30:00Z",source:"after_hours"},{},now);
    assert.equal(result.signal,value);assert.equal(result.actual_price,price);
    assert.equal(result.buy_price_target,value==="buy"?136:null);
    assert.equal(result.quote_source,"split_adjusted_daily_close");
    assert.equal(result.actual_price_timestamp,"2026-09-18T20:00:00Z");
    assert.equal(parseQuantScreenerQuery({signal:"buy"}).errors.length>0,true);
  }
});
test("old policy, wrong split basis, mismatched input, missing rows or late price cannot produce Buy",()=>{
  for(const invalid of [
    {forecast_engine:"quantura_weekly_ensemble_v1"},{forecast_input_gzip:"invalid"},{split_status:"unverified"},{split_status:"requires_refresh"},
    {history_cutoff_at:"2026-09-17T00:00:00Z"},{actual_price:999},{actual_price_timestamp:"2026-09-19T00:00:00Z"},{daily_close_at:"2026-10-01T20:00:00Z"},
    {forecast_rows:(makeRow().forecast_rows as any[]).slice(1)},{forecast_config:{...makeRow().forecast_config as any,history_lag_sessions:1}},
    {forecast_config:{...makeRow().forecast_config as any,toto_variant:"313m"}}
  ])assert.equal(decorateScreenerRow({...makeRow(),...invalid},undefined,{},now).current_signal,null);
});
test("new screener queries reject all trade-signal filters",()=>{
  for(const signal of ["buy","sell","neutral","unavailable","cutoff_buy"])assert.ok(parseQuantScreenerQuery({signal}).errors.length);
  assert.ok(parseQuantScreenerQuery({signalChanged:"true"}).errors.length);
});
test("latest Buy persists across non-buy days, is idempotent, and cannot regress",()=>{
  const buy=decorateScreenerRow(makeRow(),undefined,{},now).current_signal as any;
  const first=advanceClosingSignal({},buy);
  assert.equal(first.last_buy_signal?.input_date,"2026-09-18");
  assert.equal(advanceClosingSignal(first,buy),first);
  const next=advanceClosingSignal(first,{...buy,value:"neutral",input_date:"2026-09-21"});
  assert.deepEqual(next.last_buy_signal,first.last_buy_signal);
  const second=advanceClosingSignal(next,{...buy,input_date:"2026-09-22",price_target:140});
  assert.equal(second.last_buy_signal?.price_target,140);assert.deepEqual(second.previous_buy_signal,first.last_buy_signal);
  assert.equal(advanceClosingSignal(second,buy),second);
});
test("min/max/avg and tail-position filters remain independent of Buy rule",()=>{
  for(const position of ["above-p99","below-p99","above-p01","below-p01"]){
    const {query,errors}=parseQuantScreenerQuery({position});assert.deepEqual(errors,[]);
    assert.equal(rowMatchesQuery({ticker:"X",actual_price:position.startsWith("above")?200:1,p01:10,p99:100},query),true);
  }
  const result=decorateScreenerRow(makeRow(),undefined,{},now);
  const parsed=parseQuantScreenerQuery({quantileRules:JSON.stringify([{quantile:"p50",statistic:"avg",operator:"gt",percent:20}])});
  assert.deepEqual(parsed.errors,[]);assert.equal(rowMatchesQuery(result,parsed.query),true);
  assert.equal(rowMatchesQuery({...result,actual_price:150},parsed.query),false);
});
