import test from "node:test";
import assert from "node:assert/strict";
import { parseSavedAlert, requireAlertAccount, closingRows, digestMatches, buildScreenerDigest } from "./screenerAlerts";
import { PLATFORM_API_SCOPES, type ApiPrincipal } from "./apiAccess";

const principal:ApiPrincipal={userId:"owner",tokenId:null,tokenName:"web",tokenScopes:[...PLATFORM_API_SCOPES],plan:"free",authMethod:"firebase_session"};
const input={name:"Average P50 > 10%",email:false,filters:{positions:["below-p10","below-p50"],signal:"buy",quantileRules:[{quantile:"p50",statistic:"avg",operator:"gt",percent:10}]}};
test("saved screener filters require a real account and independent API scopes",()=>{
  assert.throws(()=>requireAlertAccount({...principal,guest:true},true),/ACCOUNT_REQUIRED/);
  assert.throws(()=>requireAlertAccount({...principal,tokenScopes:["alerts:read"]},true),/insufficient_scope/);
  assert.doesNotThrow(()=>requireAlertAccount(principal,true));
});
test("save preserves every AND position and normalized quantile filter",()=>{
  const a=parseSavedAlert(input);assert.deepEqual(a.filters.positions,["below-p10","below-p50"]);
  assert.equal(a.id,parseSavedAlert({...input,name:"Renamed",email:true}).id);assert.equal(a.filters.quantileRules?.[0].percent,10);
});
test("no arbitrary recipient, mutable owner or unsupported filters accepted",()=>{
  for(const body of [{...input,email:"true"},{...input,to:"another@example.com"},{...input,user_id:"victim"},{...input,filters:{bad:1}},{...input,name:""},{...input,filters:{quantileRules:[{quantile:"p50",statistic:"avg",operator:"gt",percent:NaN}]}}]) assert.throws(()=>parseSavedAlert(body),/INVALID/);
});
const date="2026-09-18";
const signal={value:"buy",price:80,quote_timestamp:date+"T19:59:00Z",forecast_date:date,p10:90,p90:120,source:"alpaca_iex_minute_close",provisional:false};
const row={ticker:"TEST",actual_price:150,signal:"sell",forecast_rows:[{date,timestamp:date+"T00:00:00Z",session_open:date+"T13:30:00Z",session_close:date+"T20:00:00Z",p10:90,p50:100,p90:120}],closing_signal:signal,quantile_stats:{p50:{avg:100}}};
test("digest uses finalized close rather than after-hours price and excludes stale/provisional closes",()=>{
  const rows=closingRows([row],date);assert.equal(rows[0].actual_price,80);assert.equal(rows[0].signal,"buy");
  assert.equal(closingRows([row],"2026-09-21").length,0);
  assert.equal(closingRows([{...row,closing_signal:{...signal,provisional:true}}],date).length,0);
  assert.equal(closingRows([{...row,split_status:"requires_refresh"}],date).length,0);
  assert.equal(digestMatches([parseSavedAlert(input)],rows,date).length,1);
});
test("email HTML is escaped, bounded, idempotent and links to controls",()=>{
  const matches=digestMatches([parseSavedAlert({...input,name:'<img src=x onerror=alert(1)>'})],closingRows([row],date),date);
  const email=buildScreenerDigest("owner",date,matches,"https://quantura.studio");
  assert.ok(!email.html.includes("<img"));assert.ok(email.html.includes("&lt;img"));assert.ok(email.html.includes("/screener#saved-alerts"));
  assert.equal(email.id,buildScreenerDigest("owner",date,matches,"https://quantura.studio").id);
});
