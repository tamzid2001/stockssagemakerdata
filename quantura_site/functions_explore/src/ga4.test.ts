import test from 'node:test';
import assert from 'node:assert/strict';
import {analyticsContext,forecastCompletionPayload,reportGa4ForecastCompletion} from './ga4';
const now=Date.now();
const context={analytics_consent:'granted',client_id:'123456789.987654321',session_id:Math.floor(now/1000),consent_at:new Date(now).toISOString()};
const job=()=>({status:'completed',user_id:'private-user',email:'never@example.com',analytics_context:analyticsContext(context,now),source:{provider:'dukascopy',symbol:'PRIVATE-SYMBOL'},request:{prediction_length:7,quantiles:[.01,.25,.5,.75,.9,.99]},progress:{completed_models:5},predictions:[{secret:'private'}]});
test('Measurement Protocol context requires granted consent, tag identifiers and a current session',()=>{
 assert.ok(analyticsContext(context,now));
 for(const patch of [{analytics_consent:'denied'},{client_id:'private@example.com'},{session_id:0},{session_id:context.session_id-86401},{consent_at:'not-a-date'}])assert.equal(analyticsContext({...context,...patch},now),null);
 assert.equal(forecastCompletionPayload({...job(),status:'queued'},now),null);
 assert.equal(forecastCompletionPayload(job(),now+86401000),null);
});
test('server completion payload contains only the allowlisted research funnel fields',()=>{
 const payload=forecastCompletionPayload(job(),now)!;
 assert.equal(payload.events[0].name,'forecast_completed');assert.equal(payload.events[0].params.quantile_count,6);
 assert.deepEqual(payload.consent,{ad_user_data:'DENIED',ad_personalization:'DENIED'});
 assert.doesNotMatch(JSON.stringify(payload),/private-user|never@example|PRIVATE-SYMBOL|predictions/);
});
test('completion delivery checks current consent, claims once and never fails an already saved result',async()=>{
 const oldId=process.env.GA4_MEASUREMENT_ID,oldSecret=process.env.GA4_API_SECRET;
 process.env.GA4_MEASUREMENT_ID='G-FIXTURE';process.env.GA4_API_SECRET='unit-test-only';
 try {
  let current:any=job(),consent='accepted',calls=0;
  const ref:any={set:async(data:any)=>{current={...current,...data};}};
  const userRef={};const db:any={collection:()=>({doc:()=>userRef}),runTransaction:async(fn:any)=>fn({get:async(r:any)=>({data:()=>r===ref?current:{analyticsConsent:consent}}),set:(_r:any,data:any)=>{current={...current,...data};}})};
  const request:any=async(url:URL,options:any)=>{calls++;assert.equal(url.hostname,'www.google-analytics.com');assert.equal(options.redirect,'error');return new Response(null,{status:204});};
  await reportGa4ForecastCompletion(db,ref,request);await reportGa4ForecastCompletion(db,ref,request);
  assert.equal(calls,1);assert.equal(current.analytics_context,null);assert.equal(current.analytics_delivery.status,'accepted');assert.equal(current.status,'completed');
  current=job();consent='denied';await reportGa4ForecastCompletion(db,ref,request);assert.equal(calls,1);
  current=job();consent='accepted';await reportGa4ForecastCompletion(db,ref,async()=>{throw Error('do not expose the request URL');});assert.equal(current.status,'completed');assert.equal(current.analytics_delivery.status,'unconfirmed');
 }finally{if(oldId===undefined)delete process.env.GA4_MEASUREMENT_ID;else process.env.GA4_MEASUREMENT_ID=oldId;if(oldSecret===undefined)delete process.env.GA4_API_SECRET;else process.env.GA4_API_SECRET=oldSecret;}
});
