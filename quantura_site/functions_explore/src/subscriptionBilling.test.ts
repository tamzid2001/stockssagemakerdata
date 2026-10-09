import test from "node:test";
import assert from "node:assert/strict";
import { proSubscriptionPlan, proCheckoutOptions, subscriptionAccess,billingAccess } from "./subscriptionBilling";
import { publicPlanEntitlements } from "./planEntitlements";

test("Pro checkout uses authenticated identity and server prices for both cycles",()=>{
  for(const [cycle,amount,interval] of [["monthly",19999,"month"],["yearly",199992,"year"]] as const){
    const plan=proSubscriptionPlan({tier:"pro",cycle,price:1,uid:"attacker"})!;
    const checkout=proCheckoutOptions(plan,"verified-uid","owner@example.com","https://quantura.studio");
    assert.equal(checkout.line_items![0].price_data!.unit_amount,amount);
    assert.equal(checkout.line_items![0].price_data!.recurring!.interval,interval);
    assert.equal(checkout.subscription_data!.metadata!.uid,"verified-uid");
    assert.equal(checkout.client_reference_id,"verified-uid");
  }
  assert.equal(proSubscriptionPlan({tier:"quant",cycle:"monthly"}),null);
  assert.equal(proSubscriptionPlan({tier:"pro",cycle:"daily"}),null);
  assert.deepEqual(Object.keys(publicPlanEntitlements().plans as object),["metered","pro"]);
});
test("14-day trial applies only when eligible and expires without trusting editable profile plan",()=>{
  const plan=proSubscriptionPlan({tier:"pro",cycle:"yearly"})!;
  const checkout=proCheckoutOptions(plan,"verified","owner@example.com","https://quantura.studio",true);
  assert.equal(checkout.subscription_data!.trial_period_days,14);
  assert.equal(checkout.payment_method_collection,"always");
  assert.equal(proCheckoutOptions(plan,"verified","","https://quantura.studio").subscription_data!.trial_period_days,undefined);
  const now=Date.parse("2026-10-03T00:00Z");
  assert.equal(billingAccess({plan:"pro"},now).docs_available,false);
  assert.equal(billingAccess({subscriptionStatus:"trialing",trialEnd:(now+86400000)/1000,hasUsedTrial:true},now).docs_available,true);
  assert.equal(billingAccess({subscriptionStatus:"trialing",trialEnd:now/1000,hasUsedTrial:true},now).docs_available,false);
  assert.equal(billingAccess({subscriptionStatus:"canceled",hasUsedTrial:true},now).can_trial,false);
});
test("subscription access follows current Stripe status and renewal cancellation",()=>{
  const subscription:any={id:"sub_own",customer:"cus_own",status:"active",cancel_at_period_end:true,metadata:{uid:"owner",source:"quantura_pricing",tier:"pro"}};
  assert.equal(subscriptionAccess(subscription)?.plan,"pro");
  subscription.status="canceled";assert.equal(subscriptionAccess(subscription)?.plan,"free");
  subscription.status="unpaid";assert.equal(subscriptionAccess(subscription)?.plan,"free");
  subscription.metadata.source="shop";assert.equal(subscriptionAccess(subscription),null);
});
