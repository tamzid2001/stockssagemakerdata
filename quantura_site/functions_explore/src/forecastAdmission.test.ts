import test from "node:test";
import assert from "node:assert/strict";
import { reserveForecast,releaseForecast,ADMISSION_LEASE_MS } from "./forecastAdmission";
import { PLAN_ENTITLEMENTS,publicPlanEntitlements } from "./planEntitlements";

test("Pro has no daily cap, but concurrent admission and burst controls remain",()=>{
  assert.equal(PLAN_ENTITLEMENTS.pro.forecastComputePerDay,-1);
  const pro=(publicPlanEntitlements().plans as any).pro;
  assert.equal(pro.forecastComputePerDay,null);assert.equal(pro.unlimitedForecasts,true);
  let state=reserveForecast({},"pro","a",9999,1000);
  state=reserveForecast(state,"pro","b",10000,1000);
  state=reserveForecast(state,"pro","c",10001,1000);
  assert.throws(()=>reserveForecast(state,"pro","d",10002,1000),/concurrent/);
  state.leases=releaseForecast(state,"a");
  assert.throws(()=>reserveForecast(state,"pro","d",10002,1001),/rate_limit/);
  assert.ok(reserveForecast(state,"pro","d",10002,21000).leases.d);
  assert.throws(()=>reserveForecast({},"free","x",10,1000),/daily_quota/);
});
test("releases use exact job IDs across midnight and reclaim abandoned leases",()=>{
  const now=Date.parse("2026-10-03T23:59:59Z");
  let state=reserveForecast({},"pro","first",0,now);
  state=reserveForecast(state,"pro","second",1,now);
  state.leases=releaseForecast(state,"first");
  assert.deepEqual(Object.keys(state.leases),["second"]);
  assert.deepEqual(releaseForecast(state,"first"),state.leases,"duplicate release is harmless");
  state=reserveForecast(state,"pro","next-day",0,now+61000);
  assert.ok(state.leases.second);assert.ok(state.leases["next-day"]);
  state=reserveForecast(state,"pro","recovered",0,now+ADMISSION_LEASE_MS+61001);
  assert.deepEqual(Object.keys(state.leases),["recovered"]);
});
