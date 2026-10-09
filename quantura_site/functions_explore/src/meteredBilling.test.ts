import test from "node:test";
import assert from "node:assert/strict";
import {activeMeteredAccount,createMeteredForecast,meteredCheckoutOptions,prepareMeteredSettlement,publishMeteredEvent,reserveMeteredUsage} from "./meteredBilling";

function memoryDb(initial:Record<string,any>={}) {
  const rows=new Map(Object.entries(initial));let tail=Promise.resolve();
  const ref=(path:string):any=>({id:path.split('/').at(-1),path,get:async()=>snapshot(path)});
  const snapshot=(path:string):any=>({exists:rows.has(path),data:()=>structuredClone(rows.get(path))});
  const db:any={collection:(name:string)=>({doc:(id:string)=>ref(`${name}/${id}`)}),runTransaction:(fn:any)=>{
    const result=tail.then(async()=>{const writes:Array<()=>void>=[];let wrote=false;
      const tx={get:async(r:any)=>{assert.equal(wrote,false,'Firestore reads precede writes');return snapshot(r.path);},
        create:(r:any,v:any)=>{assert.equal(rows.has(r.path),false);wrote=true;writes.push(()=>rows.set(r.path,structuredClone(v)));},
        set:(r:any,v:any)=>{wrote=true;writes.push(()=>rows.set(r.path,structuredClone(v)));},
        update:(r:any,v:any)=>{wrote=true;writes.push(()=>rows.set(r.path,{...rows.get(r.path),...structuredClone(v)}));}};
      const value=await fn(tx);writes.forEach(write=>write());return value;});tail=result.then(()=>{},()=>{});return result;
    }};
  return {db,rows,ref};
}
const account=()=>({billingMode:'metered',subscriptionStatus:'active',stripeCustomerId:'cus_verified',stripeSubscriptionId:'sub_verified',periodStart:1,periodEnd:Date.now()/1000+100000,monthlyBudgetCents:100});

test('metered checkout ignores browser prices, has no fixed fee, requires a card, and retains the 14-day trial',()=>{
  const checkout=meteredCheckoutOptions('verified','owner@example.com','https://quantura.studio',true);
  assert.equal(checkout.line_items?.length,1);assert.match(checkout.line_items![0].price!,/^price_/);
  assert.equal(checkout.line_items![0].quantity,undefined);assert.equal(checkout.payment_method_collection,'always');
  assert.equal(checkout.subscription_data?.trial_period_days,14);assert.equal(checkout.subscription_data?.metadata?.uid,'verified');
  assert.ok(checkout.discounts?.[0].promotion_code);
  const paid=meteredCheckoutOptions('verified','','https://quantura.studio',false,'cus_verified',false);
  assert.equal(paid.subscription_data?.trial_period_days,undefined);assert.equal(paid.discounts,undefined);assert.equal(paid.customer,'cus_verified');
});
test('concurrent starts reserve the budget atomically, failures release it, and completion queues one charge',async()=>{
  const {db,rows,ref}=memoryDb({'billing_accounts/owner':account()});
  const results=await Promise.allSettled(['a','b','c'].map(id=>createMeteredForecast(db,ref(`jobs/${id}`),{user_id:'owner'})));
  assert.equal(results.filter(r=>r.status==='fulfilled').length,2);assert.equal(rows.has('jobs/c'),false);
  const a=rows.get('jobs/a'),b=rows.get('jobs/b');
  await db.runTransaction(async(tx:any)=>{const settle=await prepareMeteredSettlement(db,tx,'a',a,false);settle();});
  await createMeteredForecast(db,ref('jobs/d'),{user_id:'owner'});
  await db.runTransaction(async(tx:any)=>{const settle=await prepareMeteredSettlement(db,tx,'b',b,true);settle();});
  assert.equal(rows.get('metered_billing_events/b').value,1);assert.equal(rows.has('metered_billing_events/a'),false);
  await assert.rejects(createMeteredForecast(db,ref('jobs/e'),{user_id:'owner'}),/monthly_spend_limit/);
});
test('free trial jobs stay free after their completion crosses the trial boundary; legacy users are never metered',async()=>{
  const value={...account(),subscriptionStatus:'trialing',trialEnd:Date.now()/1000+10};
  const {db,rows,ref}=memoryDb({'billing_accounts/owner':value});
  const job=await createMeteredForecast(db,ref('jobs/trial'),{user_id:'owner'});
  assert.equal(job.billing.amount_cents,0);
  rows.set('billing_accounts/owner',{...value,subscriptionStatus:'active'});
  await db.runTransaction(async(tx:any)=>{const settle=await prepareMeteredSettlement(db,tx,'trial',job,true);settle();});
  assert.equal(rows.has('metered_billing_events/trial'),false);
  const legacy=await createMeteredForecast(db,ref('jobs/legacy'),{user_id:'legacy'});assert.equal(legacy.billing,undefined);
  assert.equal(activeMeteredAccount({...value,trialEnd:1}),false);
  assert.equal(activeMeteredAccount({...account(),subscriptionStatus:'past_due'}),false);
});
test('transient meter delivery retries the same identifier, acknowledgements stop retries, ambiguous old sends are held',async()=>{
  const now=Date.now(),{db,rows}=memoryDb({'metered_billing_events/job':{status:'pending',created_at:now,customer_id:'cus_verified',lease_until:0}});
  const calls:any[]=[],stripe:any={billing:{meterEvents:{create:async(params:any)=>{calls.push(params);if(calls.length===1)throw Error('upstream unavailable');}}}};
  await assert.rejects(publishMeteredEvent(db,'job',stripe,now));await publishMeteredEvent(db,'job',stripe,now+1000);
  await publishMeteredEvent(db,'job',stripe,now+2000);
  assert.equal(calls.length,2);assert.equal(calls[0].identifier,calls[1].identifier);assert.equal(rows.get('metered_billing_events/job').status,'sent');
  rows.set('metered_billing_events/old',{status:'pending',created_at:now-86400000,first_attempt_at:now-86400000,lease_until:0});
  await publishMeteredEvent(db,'old',stripe,now);assert.equal(calls.length,2);assert.equal(rows.get('metered_billing_events/old').status,'review_required');
});
test('expired reservations do not consume budget and lowering budget never removes already completed usage',()=>{
  assert.equal(Object.keys(reserveMeteredUsage({completed:0,reservations:{old:1}},50,'new',1000).reservations).length,1);
  assert.throws(()=>reserveMeteredUsage({completed:2},50,'new'),/monthly_spend_limit/);
});
test('pending jobs carry their budget reservation through renewal and bill in the completion period',async()=>{
  const {db,rows,ref}=memoryDb({'billing_accounts/owner':account()});
  const prior=await createMeteredForecast(db,ref('jobs/prior'),{user_id:'owner'});
  rows.set('billing_accounts/owner',{...rows.get('billing_accounts/owner'),periodStart:2});
  await createMeteredForecast(db,ref('jobs/next'),{user_id:'owner'});
  await assert.rejects(createMeteredForecast(db,ref('jobs/over'),{user_id:'owner'}),/monthly_spend_limit/);
  await db.runTransaction(async(tx:any)=>{const settle=await prepareMeteredSettlement(db,tx,'prior',prior,true);settle();});
  await assert.rejects(createMeteredForecast(db,ref('jobs/over'),{user_id:'owner'}),/monthly_spend_limit/);
  assert.equal(Object.keys(rows.get('billing_accounts/owner').meteredReservations).length,1);
});
