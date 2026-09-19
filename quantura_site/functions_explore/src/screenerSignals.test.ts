import test from "node:test";
import assert from "node:assert/strict";
import { advanceClosingSignal, decorateScreenerRow, finalizedClosingSignal, ScreenerQuote } from "./screenerSignals";
import { parseQuantScreenerQuery, rowMatchesQuery, QuantScreenerRow } from "./quantScreener";

const now = Date.parse("2026-09-19T16:00:00Z");
const row: QuantScreenerRow = { ticker: "PLTR", actual_price: 100, actual_price_timestamp: "2026-09-18T00:00:00Z", last_forecast_update: "2026-09-18T01:00:00Z",
  forecast_rows: [{date:"2026-09-21",timestamp:"2026-09-21T00:00:00Z",session_open:"2026-09-21T13:30:00Z",session_close:"2026-09-21T20:00:00Z",p10:95,p50:110,p90:125}],
  quantile_stats: {p50:{min:105,avg:115,max:130}} };
const quote: ScreenerQuote = {price:90,timestamp:"2026-09-18T23:59:00Z",session:"after_hours",source:"alpaca_iex_minute_close"};
test("before next trading day use newer after-hours completed minute against first forecast row", () => {
  const result=decorateScreenerRow(row,quote,{},now);
  assert.equal(result.signal,"buy"); assert.equal(result.actual_price,90); assert.equal(result.forecast_comparison_date,"2026-09-21");
  assert.equal(result.signal_comparison,"before_first_forecast_session"); assert.equal(result.quote_session,"after_hours");
});
test("fallback to historical close; invalid, older and uncompleted minutes cannot replace it", () => {
  for(const next of [undefined,{...quote,price:NaN},{...quote,timestamp:"invalid"},{...quote,timestamp:"2026-09-17T20:00:00Z"},{...quote,timestamp:new Date(now-10_000).toISOString()}]) {
    const result=decorateScreenerRow(row,next,{},now);
    assert.equal(result.actual_price,100); assert.equal(result.signal,"neutral"); assert.equal(result.quote_source,"historical_daily_close");
  }
});
test("strict lower/upper comparisons; equality neutral, not an invented crossing", () => {
  for(const [price,signal] of [[94,"buy"],[95,"neutral"],[125,"neutral"],[126,"sell"]] as const)
    assert.equal(decorateScreenerRow(row,{...quote,price},{},now).signal,signal);
});
test("expired forecast and unverified split basis do not fabricate neutral or signals", () => {
  assert.equal(decorateScreenerRow(row,{...quote,timestamp:"2026-09-22T16:00:00Z"},{},Date.parse("2026-09-22T17:00:00Z")).signal,"unavailable");
  assert.equal(decorateScreenerRow({...row,split_status:"requires_refresh"},quote,{},now).signal,"unavailable");
});
test("finalize only last completed exchange minute, respecting early close and forecast creation time", () => {
  const session={...(row.forecast_rows as any[])[0],date:"2026-11-27",session_close:"2026-11-27T18:00:00Z"};
  const early={...row,forecast_rows:[session]}; const last={...quote,timestamp:"2026-11-27T17:59:00Z"}; const closed=Date.parse("2026-11-27T18:01:00Z");
  assert.equal(finalizedClosingSignal(early,last,closed)?.value,"buy");
  assert.equal(finalizedClosingSignal(early,{...last,timestamp:"2026-11-27T18:01:00Z"},closed),null);
  assert.equal(finalizedClosingSignal({...early,last_forecast_update:"2026-11-27T18:00:00Z"},last,closed),null);
});
test("provisional after-hours signal never changes saved EOD or previous signal", () => {
  const saved={closing_signal:{...decorateScreenerRow(row,quote,{},now).current_signal as any,provisional:false},last_non_neutral_signal:{...decorateScreenerRow(row,quote,{},now).current_signal as any,provisional:false}};
  const next=decorateScreenerRow(row,{...quote,price:130},saved,now);
  assert.equal(next.signal,"sell");assert.deepEqual(next.last_non_neutral_signal,saved.last_non_neutral_signal);
  assert.deepEqual(advanceClosingSignal(saved,next.current_signal as any),saved);
});
test("min/max/avg rules use current quote as denominator and combine with current signal", () => {
  const result=decorateScreenerRow(row,quote,{},now);
  const parsed=parseQuantScreenerQuery({signal:"buy",quantileRules:JSON.stringify([{quantile:"p50",statistic:"avg",operator:"gt",percent:20}])});
  assert.deepEqual(parsed.errors,[]);assert.equal(rowMatchesQuery(result,parsed.query),true);
  assert.equal(rowMatchesQuery({...result,actual_price:110},parsed.query),false);
  assert.notEqual(parseQuantScreenerQuery({quantileRules:'[{"quantile":"bad"}]'}).errors.length,0);
});
