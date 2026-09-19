import assert from "node:assert/strict";
import crypto from "node:crypto";
import test from "node:test";
import {
  normalizeEnsembleConfiguration,
  normalizeEnsemblePreset,
  normalizeRequestedQuantiles,
  publicEnsembleJob,
  publicModelCapabilities,
  timesFmState,
  validateWorkerResult,
  apiError,
  validateModelHistory,
  historyCutoffAt,
  absoluteHistoryCutoff,
  resolvePredictionEnd,
  expiredEnsembleJobCode,
  approvedModelCheckpoints,
  approvedModelRevisions,
  validateGuestForecastClaim,
  validateUploadedSeries,
  tickerOverlayRows,
} from "./ensembleForecastRoutes";
import {automaticSportsHistoryPhase, eventHistoryRange} from "./eventHistory";

test("sports auto switches at 32 elapsed in-game minutes, including cutoff replays", () => {
  const start=Date.parse('2026-09-16T20:00:00Z');
  for (const minutes of [-60,0,31.99]) assert.equal(automaticSportsHistoryPhase(start,start+minutes*60000),'both');
  for (const minutes of [32,33,120]) {
    const phase=automaticSportsHistoryPhase(start,start+minutes*60000);
    assert.equal(phase,'in_game');
    assert.equal(eventHistoryRange(start-100*60000,start+minutes*60000,start,{history_phase:phase,history_lookback_minutes:0}).start,start);
  }
  assert.equal(automaticSportsHistoryPhase(NaN,start),'both');
  const preset=normalizeEnsemblePreset({source:{type:'prediction_market',history_phase:'auto'},models:{prophet:{enabled:true,weight:1}}},'free');
  assert.equal((preset.history_controls as any).history_phase,'auto');
});

test("guest save is bound to the completed owner's job, one-use and expiring", () => {
  const token='a'.repeat(64), id='forecast-example';
  const job={guest_session:true,status:'completed',user_id:'guest',workspace_id:'guest',guest_save_expires_at:2000,guest_save_hash:crypto.createHash('sha256').update(token).digest('hex')};
  validateGuestForecastClaim(id,`${id}.${token}`,job,1000);
  for(const patch of [{status:'running'},{guest_session:false},{workspace_id:'other'},{guest_save_expires_at:999},{guest_save_hash:null}]) assert.throws(()=>validateGuestForecastClaim(id,`${id}.${token}`,{...job,...patch},1000),/forbidden/);
  for(const cookie of ['',`other.${token}`,`${id}.${'b'.repeat(64)}`,`${id}.${token}.extra`]) assert.throws(()=>validateGuestForecastClaim(id,cookie,job,1000),/forbidden/);
});

test("uploaded rows reject invalid targets and dates rather than silently dropping them",()=>{
  validateUploadedSeries([{time:'2026-09-16T20:00:00Z',value:0}], 'time','value');
  for(const row of [{time:'bad',value:1},{time:'2026-09-16',value:''},{time:'2026-09-16',value:NaN},{time:'2026-09-16',value:false}]) assert.throws(()=>validateUploadedSeries([row],'time','value'),/source_series_row_invalid/);
  assert.equal(resolvePredictionEnd({prediction_end_at:'2026-09-16T12:59:00Z',calendar:'NONE'},'2026-09-16T10:00:00Z','1h').prediction_length,2);
});
import { AlpacaError } from "./alpacaClient";
import { HISTORICAL_VALIDATION_POLICY, publicHistoricalValidation, validateHistoricalValidation } from "./ensembleForecastRoutes";

const validationReport = () => ({policy:HISTORICAL_VALIDATION_POLICY,method:"chronological_holdout",status:"completed",
  minimum_training_rows:2,training_rows:73,holdout_rows:7,training_end_at:"2026-09-10T00:00:00Z",validation_start_at:"2026-09-11T00:00:00Z",validation_end_at:"2026-09-17T00:00:00Z",
  metrics:{count:7,point_count:7,mae:1,rmse:1.5,smape:.01,average_wql:.03},evidence:{actuals:[100],predictions:[101]},model_runs:[{internal:true}]});
test("historical metrics are validated and exposed without private evaluation arrays",()=>{
  const report=validationReport();validateHistoricalValidation(report);
  const summary=publicHistoricalValidation(report);
  assert.equal(summary.evidence,undefined);assert.equal(summary.model_runs,undefined);assert.deepEqual(summary.metrics,report.metrics);
  const job=publicEnsembleJob('test',{request:{}},{predictions:[],historical_validation:report});
  assert.deepEqual(job.historical_validation,summary);
  assert.equal(publicEnsembleJob('legacy',{request:{}},{predictions:[]}).historical_validation,undefined);
});
test("validation rejects non-finite scores, impossible sample sizes and a leaking split",()=>{
  const valid=validationReport();
  for(const update of [{metrics:{...valid.metrics,mae:NaN}},{metrics:{...valid.metrics,mae:'1'}},{metrics:{...valid.metrics,count:8}},{metrics:{...valid.metrics,smape:3}},
    {training_rows:1},{holdout_rows:31},{training_end_at:valid.validation_start_at},{status:'training_fit'},{metrics:{...valid.metrics,rmse:-1}}])
    assert.throws(()=>validateHistoricalValidation({...valid,...update}),/validation_metrics_invalid/);
  validateHistoricalValidation({policy:HISTORICAL_VALIDATION_POLICY,method:'chronological_holdout',status:'failed',metrics:null});
  validateHistoricalValidation(undefined);
});
test("stock overlays include completed minute closes beside hourly forecasts, without partial bars",async()=>{
  const cutoff=Date.parse('2026-09-16T14:00:00Z'), now=Date.parse('2026-09-16T16:01:30Z');
  const calls:string[]=[];
  const fetchHistory:any=async(request:any)=>{
    calls.push(request.timeframe);
    return {rows:request.timeframe==='1Min' ? [{timestamp:'2026-09-16T16:00:00Z',close:101},{timestamp:'2026-09-16T16:01:00Z',close:999}] : [{timestamp:'2026-09-16T14:00:00Z',close:100},{timestamp:'2026-09-16T16:00:00Z',close:999}]};
  };
  const rows=await tickerOverlayRows({symbol:'PLTR',provider:'alpaca'},'1h',cutoff,now,fetchHistory);
  assert.deepEqual(calls,['1Min','1Hour']);
  assert.deepEqual(rows.map(r=>[r.timestamp,r.target]),[['2026-09-16T15:00:00.000Z',100],['2026-09-16T16:01:00.000Z',101]]);
});
test("stock overlays preserve price basis and daily closes despite minute rate limits",async()=>{
  const cutoff=Date.parse('2026-09-14T13:30Z'),now=Date.parse('2026-09-16T15:01Z');
  const requests:any[]=[];
  const fetchHistory:any=async(request:any)=>{
    requests.push(request);
    if(request.timeframe==='1Min')throw new AlpacaError('rate_limit','Provider cooldown',429);
    return {provider:'yahoo',exchangeTimezone:'America/New_York',adjustment:'all',feed:'yahoo',rows:[{timestamp:'2026-09-15T13:30:00Z',close:102},{timestamp:'2026-09-16T13:30:00Z',close:999},{timestamp:'2026-09-15T14:30:00Z',close:null}]};
  };
  const rows=await tickerOverlayRows({symbol:'PLTR',provider:'yahoo',adjustment:'all',session:'regular',feed:'yahoo'},'1D',cutoff,now,fetchHistory);
  assert.equal(rows.length,1);assert.equal(rows[0].target,102);assert.equal(rows[0].session_date,'2026-09-15');
  assert.ok(requests.every(r=>r.adjustment==='all'&&r.feed==='yahoo'&&r.session==='regular'));
});
test("daily history failure does not discard available minute closing prices",async()=>{
  const fetchHistory:any=async(request:any)=>{
    if(request.timeframe==='1Day')throw new AlpacaError('no_data','No daily history',404);
    return {provider:'alpaca',rows:[{timestamp:'2026-09-16T15:00:00Z',close:104}]};
  };
  const rows=await tickerOverlayRows({symbol:'PLTR',provider:'alpaca'},'1D',Date.parse('2026-09-15T04:00Z'),Date.parse('2026-09-16T15:01Z'),fetchHistory);
  assert.equal(rows.length,1);assert.equal(rows[0].timestamp,'2026-09-16T15:01:00.000Z');assert.equal(rows[0].target,104);
  await assert.rejects(()=>tickerOverlayRows({symbol:'PLTR'},'1D',1,2,async()=>{throw new AlpacaError('rate_limit','cooldown',429);}),/cooldown/);
});
test("first prediction quote survives the bounded latest-500 overlay",()=>{
  const start=Date.parse('2026-09-15T12:00Z');
  const data=Array.from({length:510},(_,i)=>({timestamp:new Date(start+i*60000).toISOString(),price:.5}));
  const normal=forecastObservationWindow(data,60000,start+510*60000,0,500);
  const overlay=forecastObservationWindow(data,60000,start+510*60000,0,500,start);
  assert.equal(normal.rows.length,500);assert.equal(overlay.rows.length,501);assert.equal(Date.parse(overlay.rows[0].timestamp),start);
  assert.equal(forecastObservationWindow(data.slice(1),60000,start+510*60000,0,500,start).rows.length,500);
});
test("minute stock overlays recover only the exact first prediction close outside the latest 500",async()=>{
  const cutoff=Date.parse('2026-09-15T13:30Z'),now=cutoff+600*60000;
  const calls:any[]=[];
  const fetchHistory:any=async(request:any)=>{
    calls.push(request);
    return {provider:'alpaca',rows:calls.length===1 ? Array.from({length:500},(_,i)=>({timestamp:new Date(cutoff+(i+100)*60000).toISOString(),close:100+i})) : [{timestamp:new Date(cutoff).toISOString(),close:99},{timestamp:new Date(cutoff+60000).toISOString(),close:999}]};
  };
  const rows=await tickerOverlayRows({symbol:'PLTR',provider:'alpaca'},'1min',cutoff,now,fetchHistory);
  assert.equal(calls.length,2);assert.equal(rows.length,501);assert.equal(rows[0].target,99);assert.equal(Date.parse(rows[0].timestamp),cutoff+60000);
  assert.equal(calls[1].end,new Date(cutoff+60000).toISOString());
});
test("server cutoff accepts the current minute and advances beyond an earlier page-load clock",()=>{
  const cutoff='2026-09-16T10:15:00-04:00';
  assert.throws(()=>absoluteHistoryCutoff({history_cutoff_at:cutoff},Date.parse('2026-09-16T12:48Z')));
  assert.equal(absoluteHistoryCutoff({history_cutoff_at:cutoff},Date.parse('2026-09-16T14:16Z')),Date.parse(cutoff));
});
test("all five approved Toto variants are free, pinned and distinct cache configurations", () => {
  const rows=(publicModelCapabilities('free').models as any[]).find(m=>m.id==='toto');
  assert.equal(rows.available,true);assert.equal(rows.variants.length,5);
  const identities=new Set<string>();
  for(const variant of rows.variants){
    const config=normalizeEnsembleConfiguration({toto_variant:variant.id,models:{prophet:{enabled:false},toto:{enabled:true,weight:1}},quantiles:[.1,.5,.9]},'free');
    assert.equal(approvedModelCheckpoints(config).toto,variant.checkpoint);
    assert.match(approvedModelRevisions(config).toto!,/^[a-f0-9]{40}$/);
    identities.add(JSON.stringify(config));
  }
  assert.equal(identities.size,5);
  for(const value of ['../secret','Datadog/arbitrary',{},''])assert.throws(()=>normalizeEnsembleConfiguration({toto_variant:value},'free'),/toto_variant_unsupported/);
  assert.throws(()=>normalizeEnsembleConfiguration({prediction_length:1.5},'free'),/prediction_length/);
});
import { parseMarketLink } from "./marketLink";
import { watchdogAuthorized, shouldRecoverScreener } from "./marketResearchWatchdog";
import { forecastObservationWindow, forecastObservationLimit, PredictionMarketDataError, isMoneyline, gameTiming, resolveMarketLink, normalizePolymarketEvents } from "./predictionMarketData";

test("website Toto is revision-pinned and user input cannot override model identity", () => {
  const config=normalizeEnsembleConfiguration({models:{prophet:{enabled:false,weight:0},toto:{enabled:true,weight:1}},quantiles:[.1,.5,.9]},'research');
  const revision='51a2812bbe449437c01b79c0e425ed578f335f5b';
  assert.deepEqual(approvedModelCheckpoints(config),{toto:'Datadog/Toto-2.0-2.5B'});
  assert.deepEqual(approvedModelRevisions(config),{toto:revision});
  const model=(publicModelCapabilities('research').models as any[]).find(m=>m.id==='toto');
  assert.equal(model.name,'Toto 2.0 2.5B');
  assert.equal(model.checkpoint_revision,revision);
  assert.equal(model.minimum_observed_context,32);
  assert.throws(()=>normalizeEnsembleConfiguration({models:{toto:{enabled:true,weight:1}},quantiles:[.5],model_revisions:{toto:revision}},'research'),/configuration_field_unsupported/);
  const legacy=publicEnsembleJob('old',{request:{},model_checkpoints:{toto:'Datadog/Toto-2.0-4m'}});
  assert.deepEqual(legacy.model_checkpoints,{toto:'Datadog/Toto-2.0-4m'});
  assert.deepEqual(legacy.model_revisions,{});
  const previous=publicEnsembleJob('previous',{request:{},model_checkpoints:{toto:'Datadog/Toto-2.0-313m'},model_revisions:{toto:'a7bab288f5e95f8606f8306f86659357e1c001ef'}});
  assert.deepEqual(previous.model_checkpoints,{toto:'Datadog/Toto-2.0-313m'});
  assert.deepEqual(previous.model_revisions,{toto:'a7bab288f5e95f8606f8306f86659357e1c001ef'});
});

test("research watchdog requires a dedicated bearer secret and never duplicates active screeners", () => {
  const secret="fixture-watchdog-secret-at-least-32-characters";
  assert.equal(watchdogAuthorized(`Bearer ${secret}`,secret),true);
  assert.equal(watchdogAuthorized("wrong",secret),false);
  assert.equal(watchdogAuthorized("", ""),false);
  assert.equal(shouldRecoverScreener([]),true);
  assert.equal(shouldRecoverScreener([{status:"queued"}]),false);
  assert.equal(shouldRecoverScreener([{status:"completed",created_at:new Date().toISOString()}]),false);
  assert.equal(shouldRecoverScreener([{status:"completed",created_at:"2020-01-01T00:00:00Z"}]),true);
});

test("only genuinely expired queued/running jobs time out; completed results remain immutable", () => {
  const now=Date.parse('2026-09-12T20:00:00Z');
  assert.equal(expiredEnsembleJobCode({status:'queued',created_at:'2026-09-12T12:00:00Z'},now),'WORKER_QUEUE_TIMEOUT');
  assert.equal(expiredEnsembleJobCode({status:'queued',created_at:'2026-09-12T19:59:00Z'},now),null);
  assert.equal(expiredEnsembleJobCode({status:'running',lease_expires_at:'2026-09-12T19:00:00Z'},now),'WORKER_LEASE_EXPIRED');
  assert.equal(expiredEnsembleJobCode({status:'running',lease_expires_at:'2026-09-12T21:00:00Z'},now),null);
  assert.equal(expiredEnsembleJobCode({status:'completed',lease_expires_at:'2026-09-12T19:00:00Z'},now),null);
});

test("minute cutoffs are strict, exclude the latest observations before choosing the 500 inputs", () => {
  const now = Date.parse("2026-09-12T16:30:35Z");
  const cutoff = historyCutoffAt(30, now)!;
  assert.equal(new Date(cutoff).toISOString(), "2026-09-12T16:00:00.000Z");
  assert.equal(historyCutoffAt(0, now), undefined);
  assert.equal(historyCutoffAt(2 * 1440, now), Math.floor(now / 60000) * 60000 - 2 * 86400000);
  for (const v of [NaN, Infinity, -1, 1.5, 129601, "30", true, null]) assert.throws(() => historyCutoffAt(v, now));
  const rows = Array.from({length:600}, (_,i) => ({timestamp:new Date(now-35_000-(599-i)*60_000).toISOString(),price:.4}));
  const selected = forecastObservationWindow(rows, 60_000, cutoff);
  assert.equal(selected.rows.length, 500);
  assert.equal(Date.parse(selected.rows.at(-1)!.timestamp), cutoff);
  assert.ok(selected.rows.every(r => Date.parse(r.timestamp) <= cutoff));
});

test("school names supplement provider nicknames without changing contract identities or prop YES/NO", () => {
  const event = {id:"97962",title:"Oklahoma vs. Michigan",markets:[{id:"579992",slug:"aec-cfb-okl-mich-2026-09-12",title:"Sooners vs Wolverines",sportsMarketTypeV2:"SPORTS_MARKET_TYPE_MONEYLINE",marketSides:[
    {id:"1159496",long:true,team:{name:"Sooners",safeName:"Oklahoma"}},
    {id:"1159497",long:false,team:{name:"Wolverines",safeName:"Michigan"}}]}]};
  const category = {id:"cfb",label:"CFB",sport:"football",providerId:"6"};
  const rows = normalizePolymarketEvents({events:[event]}, category);
  assert.deepEqual(rows.map(r => [r.contractId,r.outcome,r.side]), [["1159496","Oklahoma Sooners","long"],["1159497","Michigan Wolverines","short"]]);
  assert.ok(rows.every(r => r.league === "CFB" && r.eventTitle === "Oklahoma vs. Michigan"));
  event.markets[0].sportsMarketTypeV2 = "SPORTS_MARKET_TYPE_PROP";
  Object.assign(event.markets[0].marketSides[0], {description:"Yes"});
  assert.equal(normalizePolymarketEvents({events:[event]}, category)[0].outcome, "Yes");
});

test("market links reject SSRF, lookalike origins, credentials, ports and traversal", () => {
  assert.deepEqual(parseMarketLink("https://kalshi.com/markets/kxmlbgame/mlb/kxmlbgame-26sep11abc"), { source: "kalshi", identifier: "KXMLBGAME-26SEP11ABC", kind: "either" });
  assert.equal(parseMarketLink("https://www.polymarket.us/event/game-123?utm_source=test").identifier, "game-123");
  for (const url of ["http://polymarket.us/event/game-1", "https://polymarket.us.evil.test/event/x", "https://localhost/event/x", "https://polymarket.com/event/game-1", "https://x:y@kalshi.com/markets/test", "https://kalshi.com:444/markets/test", "https://polymarket.us/event/%2e%2e%2fsecret"]) assert.throws(() => parseMarketLink(url));
});

test("observed forecast window caps 500 bars, retains both endpoints, never fills gaps or future observations", () => {
  const now = Date.parse("2026-09-11T12:00:00Z");
  const rows = Array.from({ length: 600 }, (_, i) => ({ timestamp: new Date(now - (599-i)*60_000).toISOString(), price: i % 2 ? 1 : 0 }));
  const full = forecastObservationWindow(rows, 60_000, now);
  assert.equal(full.rows.length, 500);
  assert.equal(full.rows.at(-1)?.timestamp, new Date(now).toISOString());
  assert.equal(full.rows[0].target, 0);
  const gaps = [...rows.slice(0, -3), ...rows.slice(-2), { timestamp: new Date(now+60_000).toISOString(), price: .5 }];
  assert.equal(forecastObservationWindow(gaps, 60_000, now).rows.length, 500);
  assert.equal(forecastObservationWindow(gaps, 60_000, now).gap_count, 1);
  assert.throws(() => forecastObservationWindow([{ timestamp: new Date(now).toISOString(), price: null }], 60_000, now), /two observed/);
});

test("last N observations is strict and applied after cutoff, excludes filled rows, keeps gaps", () => {
  assert.equal(forecastObservationLimit(undefined), 500);
  for (const value of [0, 1, -1, 501, 2.5, NaN, Infinity, "60", null, true]) assert.throws(() => forecastObservationLimit(value));
  const cutoff = Date.parse("2026-09-13T12:00:00Z");
  const rows = Array.from({length:100}, (_,i) => ({timestamp:new Date(cutoff+(i-80)*120000).toISOString(),price:.4+i/1000}));
  rows.push({...rows[80], price:.9, is_forward_filled:true} as any);
  const result = forecastObservationWindow(rows, 60000, cutoff, 2, 30);
  assert.equal(result.rows.length,30);
  assert.equal(result.rows.at(-1)?.timestamp,new Date(cutoff).toISOString());
  assert.ok(Math.abs(result.rows.at(-1)!.target-.48)<1e-12);
  assert.equal(result.observed_rows,81);
  assert.equal(result.gap_count,29);
  assert.equal(forecastObservationWindow(rows.slice(0,5),60000,cutoff,2,60).rows.length,5);
});

test("saved presets preserve bounded history controls, not resource data or authority", () => {
  const request = {source:{type:"prediction_market",limit:45,history_phase:"in_game",history_lookback_minutes:120,contract_id:"fixture-private-id"},history_lag_minutes:15,models:{prophet:{enabled:true,weight:1}}};
  const saved = normalizeEnsemblePreset(request, "research");
  assert.deepEqual(saved.history_controls,{limit:45,history_phase:"in_game",history_lookback_minutes:120,history_lag_minutes:15});
  assert.equal(saved.source,undefined);
  assert.equal(saved.workspace_id,undefined);
  for(const limit of [0,1,501,NaN,"45"]) assert.throws(()=>normalizeEnsemblePreset({...request,source:{...request.source,limit}},"research"));
  assert.throws(()=>normalizeEnsemblePreset({...request,history_lag_minutes:-1},"research"));
});

test("open Kalshi is not a live game without an official start; props are not moneylines", () => {
  const contract = { source: "kalshi", status: "open", eventStart: null, league: "KXMLBGAME" } as any;
  assert.equal(gameTiming(contract), "open"); assert.equal(isMoneyline(contract), true);
  assert.equal(isMoneyline({ ...contract, league: "KXMLBTOTAL" }), false);
  assert.equal(isMoneyline({ ...contract, league: "KX1STHOMEGAME", eventTitle: "Buffalo's 1st Opponent at Highmark Stadium" }), false);
  assert.equal(isMoneyline({ ...contract, league: "KXCFBGAME", eventTitle: "Oklahoma at Michigan" }), true);
  assert.equal(gameTiming({ ...contract, eventStart: new Date(Date.now()-60_000).toISOString() }), "in_progress");
  assert.equal(gameTiming({ ...contract, status: "settled", live: true }), "closed");
});

test("safe provider errors retain actionable HTTP status instead of generic invalid request", () => {
  assert.deepEqual(apiError(new AlpacaError("rate_limit", "Provider rate limited. Retry later.", 429)), { status: 429, code: "RATE_LIMIT", message: "Provider rate limited. Retry later." });
  assert.equal(apiError(new PredictionMarketDataError("contract_not_found", "Select the side again.", 422)).message, "Select the side again.");
});

test("event URL resolution preserves team sides and selects moneylines over unrelated props", async () => {
  const original = globalThis.fetch;
  const urls: string[] = [];
  globalThis.fetch = (async (input: any) => {
    urls.push(String(input));
    return Response.json({ event: { id: "event-fixture", title: "A vs B", markets: [
      { id: "m1", slug: "game-fixture", sportsMarketTypeV2: "SPORTS_MARKET_TYPE_MONEYLINE", marketSides: [{ id: "side-a", long: true, description: "A" }, { id: "side-b", long: false, description: "B" }] },
      { id: "m2", slug: "prop-fixture", sportsMarketTypeV2: "SPORTS_MARKET_TYPE_PROP", marketSides: [{ id: "prop", long: true, description: "Prop" }] },
    ] } });
  }) as typeof fetch;
  try {
    const rows = await resolveMarketLink("https://polymarket.us/event/test-event-fixture");
    assert.deepEqual(rows.map(c => [c.contractId, c.side]), [["side-a", "long"], ["side-b", "short"]]);
    assert.equal(new URL(urls[0]).origin, "https://gateway.polymarket.us");
    assert.equal(rows[0].eventId, "event-fixture");
  } finally { globalThis.fetch = original; }
});

const balancedModels = Object.fromEntries(
  ["prophet", "toto", "granite", "chronos", "timesfm"].map((id) => [id, { enabled: true, weight: 0.2 }])
);

test("short observed histories fail before dispatch unless the user permits a viable reduced ensemble", () => {
  const models = { prophet: { enabled: true, weight: 1 }, toto: { enabled: true, weight: 1 } };
  const strict = normalizeEnsembleConfiguration({ models }, "quant");
  assert.throws(() => validateModelHistory(strict, 2), (error: unknown) => {
    const response = apiError(error);
    return response.status === 422 && response.code === "MODEL_CONTEXT_TOO_SHORT" && response.message.includes("32 observed values");
  });
  assert.doesNotThrow(() => validateModelHistory(strict, 32));
  const flexible = normalizeEnsembleConfiguration({ models, failure_policy: "renormalize" }, "quant");
  assert.doesNotThrow(() => validateModelHistory(flexible, 2));
  assert.equal(flexible.models.toto.enabled, true, "preflight must not silently rewrite the requested models");
  const onlyToto = normalizeEnsembleConfiguration({ models: { prophet: { enabled: false }, toto: { enabled: true, weight: 1 } }, quantiles: [.1, .5, .9], failure_policy: "renormalize" }, "quant");
  assert.throws(() => validateModelHistory(onlyToto, 2), /No enabled positive-weight model/);
  const zeroToto = normalizeEnsembleConfiguration({ models: { ...models, toto: { enabled: true, weight: 0 } } }, "quant");
  assert.doesNotThrow(() => validateModelHistory(zeroToto, 2));
  assert.equal((publicModelCapabilities("quant").models as any[]).find(m => m.id === "toto").minimum_observed_context, 32);
});

test("custom quantiles are sorted and deduplicated without rounding collisions", () => {
  assert.deepEqual(normalizeRequestedQuantiles([0.75, 0.123456, 0.1, 0.123456, 0.5]), [0.1, 0.123456, 0.5, 0.75]);
  assert.throws(() => normalizeRequestedQuantiles([0, 0.5]), /quantile_invalid/);
});

test("balanced central weights normalize to 20 percent each", () => {
  process.env.TIMESFM_HF_ACCESS_APPROVED = "true";
  process.env.TIMESFM_COMMERCIAL_LICENSED = "true";
  const config = normalizeEnsembleConfiguration({ models: balancedModels, quantiles: [0.01, 0.5, 0.99], prediction_length: 30 }, "quant");
  assert.deepEqual(config.effective_central_weights, {
    prophet: 0.2,
    toto: 0.2,
    granite: 0.2,
    chronos: 0.2,
    timesfm: 0.2,
  });
});

test("Toto and TimesFM alone cannot satisfy a P01 request", () => {
  process.env.TIMESFM_HF_ACCESS_APPROVED = "true";
  process.env.TIMESFM_COMMERCIAL_LICENSED = "true";
  const models = Object.fromEntries(
    ["prophet", "toto", "granite", "chronos", "timesfm"].map((id) => [id, { enabled: id === "toto" || id === "timesfm", weight: 1 }])
  );
  assert.throws(
    () => normalizeEnsembleConfiguration({ models, quantiles: [0.01], prediction_length: 5 }, "quant"),
    /quantile_0.01_unsupported/
  );
});

test("weights reject negative and NaN values", () => {
  assert.throws(
    () => normalizeEnsembleConfiguration({ models: { prophet: { enabled: true, weight: -1 } }, quantiles: [0.5] }, "free"),
    /prophet_weight_invalid/
  );
  assert.throws(
    () => normalizeEnsembleConfiguration({ models: { prophet: { enabled: true, weight: Number.NaN } }, quantiles: [0.5] }, "free"),
    /prophet_weight_invalid/
  );
  assert.throws(
    () => normalizeEnsembleConfiguration({ models: { prophet: { enabled: true, weight: 1 } }, quantiles: [0.5], model_checkpoints: { prophet: "unapproved/checkpoint" } }, "free"),
    /configuration_field_unsupported/
  );
});

test("free access includes configured foundation models without bypassing model constraints", () => {
  for (const plan of ["free", "pro", "quant", "research"] as const) {
    const config = normalizeEnsembleConfiguration({ models: { toto: { enabled: true, weight: 1 } }, quantiles: [0.5], prediction_length: 60 }, plan);
    assert.equal(config.models.toto.enabled, true);
    assert.throws(() => normalizeEnsembleConfiguration({ prediction_length: 513 }, plan), /prediction_length/);
  }
});

test("absolute local-time cutoffs become UTC with future, old and conflicting values rejected", () => {
  const now = Date.parse("2026-09-15T20:00:00Z");
  assert.equal(absoluteHistoryCutoff({history_cutoff_at:"2026-09-15T15:30:00-04:00"}, now), Date.parse("2026-09-15T19:30:00Z"));
  for (const value of ["2026-09-15T21:00:00Z", "2020-01-01T00:00:00Z", "2026-09-15T15:30:00", "bad"]) assert.throws(() => absoluteHistoryCutoff({history_cutoff_at:value},now));
  assert.throws(() => absoluteHistoryCutoff({history_cutoff_at:"2026-09-15T15:30:00-04:00",history_lag_minutes:30},now), /conflict/);
  const minute = resolvePredictionEnd({prediction_end_at:"2026-09-15T16:00:00-04:00",calendar:"NONE"},"2026-09-15T19:15:00Z","1min");
  assert.equal(minute.prediction_length,45);
  assert.equal(minute.horizon_mode,"frequency_periods");
  const daily = resolvePredictionEnd({prediction_end_at:"2026-09-18T16:00:00-04:00",calendar:"NYSE"},"2026-09-11T00:00:00Z","1D");
  assert.equal(daily.prediction_length,7); assert.equal(daily.horizon_mode,"calendar_days");
  assert.throws(() => resolvePredictionEnd({prediction_end_at:"2026-09-10T00:00:00Z"},"2026-09-11T00:00:00Z","1D"));
});

test("TimesFM production license flag is independent from access approval", () => {
  const originalNodeEnv = process.env.NODE_ENV;
  process.env.NODE_ENV = "production";
  process.env.TIMESFM_HF_ACCESS_APPROVED = "true";
  delete process.env.TIMESFM_COMMERCIAL_LICENSED;
  process.env.ALLOW_NONCOMMERCIAL_TIMESFM = "true";
  assert.deepEqual(timesFmState("production"), { available: false, unavailable_reason: "commercial_license_required", evaluation_only: false });
  const capability = publicModelCapabilities("research");
  const timesfm = (capability.models as Array<Record<string, unknown>>).find((model) => model.id === "timesfm");
  assert.equal(timesfm?.available, false);
  assert.equal(timesfm?.unavailable_reason, "commercial_license_required");
  process.env.NODE_ENV = originalNodeEnv;
});

test("public job schema exposes ensemble output without component arrays or inputs", () => {
  const job = publicEnsembleJob(
    "job_1",
    {
      status: "completed",
      workspace_id: "workspace_1",
      request: { prediction_length: 2, horizon_mode: "trading_sessions", quantiles: [0.25, 0.5, 0.75], transform: "log", models: balancedModels },
      source: { type: "ticker", symbol: "AAPL" },
      requested_weights: {},
      effective_central_weights: {},
    },
    { predictions: [{ timestamp: "2026-09-02", quantiles: { "0.5": 100 } }], effective_weights_by_quantile: {}, models: [{ id: "prophet" }] }
  );
  assert.ok(Array.isArray(job.predictions));
  assert.equal("input" in job, false);
  assert.equal("model_runs" in job, false);
});

test("worker result validation rejects crossed quantiles and invalid effective weights", () => {
  const job = { request: { prediction_length: 1, horizon_mode: "trading_sessions", quantiles: [0.25, 0.5, 0.75] } };
  const valid = {
    quantiles: [0.25, 0.5, 0.75],
    transform: "none",
    predictions: [{ timestamp: "2026-09-02T00:00:00Z", quantiles: { "0.25": 90, "0.5": 100, "0.75": 110 } }],
    effective_weights_by_quantile: {
      "0.25": { prophet: 1 }, "0.5": { prophet: 1 }, "0.75": { prophet: 1 },
    },
  };
  assert.equal(validateWorkerResult(valid, job).predictions.length, 1);
  assert.throws(() => validateWorkerResult({ ...valid, predictions: [{ timestamp: "2026-09-02T00:00:00Z", quantiles: { "0.25": 110, "0.5": 100, "0.75": 90 } }] }, job), /ordering/);
  assert.throws(() => validateWorkerResult({ ...valid, effective_weights_by_quantile: { ...valid.effective_weights_by_quantile, "0.5": { prophet: 0.8 } } }, job), /weights/);
});
