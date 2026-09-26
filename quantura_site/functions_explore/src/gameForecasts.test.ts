import assert from "node:assert/strict";
import test from "node:test";
import {gameDate,publicGameForecast} from "./gameForecasts";
const now=Date.parse("2026-09-26T22:00:00Z");
const fixture=()=>({id:"a".repeat(32),provider:"kalshi",event_title:"A vs B",outcome:"A",game_date:"2026-09-26",
 game_start:"2026-09-26T23:30:00Z",forecast_end:"2026-09-27T03:30:00Z",generated_at:"2026-09-26T21:10:00Z",input_cutoff:"2026-09-26T21:00:00Z",
 predictions:[{timestamp:"2026-09-27T03:30:00Z",quantiles:{"0.1":.2,"0.5":.4,"0.9":.8}}],secret:"never public"});
test("today is New York date; valid snapshots remain final after start hour",()=>{
 assert.equal(gameDate(Date.parse("2026-09-27T02:00:00Z")),"2026-09-26");
 const row=publicGameForecast(fixture(),now)!;assert.equal(row.secret,undefined);assert.equal(row.predictions,undefined);
 assert.equal(row.status,"updating_pregame");assert.equal(publicGameForecast(fixture(),now+3600000)?.status,"final_pregame");
 assert.equal(publicGameForecast(fixture(),Date.parse("2026-09-27T12:00:00Z")),null);
 assert.ok(publicGameForecast(fixture(),now,true)?.predictions);
});
test("late, future, wrong-day, malformed and crossed-band forecasts are hidden",()=>{
 for(const patch of [{generated_at:"2026-09-26T23:00:00Z"},{game_date:"2026-09-27"},{input_cutoff:"2026-09-26T22:30:00Z"},
 {forecast_end:"2026-09-27T03:31:00Z"},{predictions:[{timestamp:"2026-09-27T03:30:00Z",quantiles:{"0.1":.8,"0.5":.4,"0.9":.7}}]}])assert.equal(publicGameForecast({...fixture(),...patch},now),null);
});
