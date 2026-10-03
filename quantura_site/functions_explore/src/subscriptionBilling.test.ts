import test from "node:test";
import assert from "node:assert/strict";
import { proSubscriptionPlan, proCheckoutOptions, subscriptionAccess } from "./subscriptionBilling";
import { publicPlanEntitlements } from "./planEntitlements";

test("Pro checkout uses authenticated identity and server prices for both cycles",()=>{
  for(const [cycle,amount,interval] of [["monthly",20000,"month"],["yearly",200000,"year"]] as const){
    const plan=proSubscriptionPlan({tier:"pro",cycle,price:1,uid:"attacker"})!;
    const checkout=proCheckoutOptions(plan,"verified-uid","owner@example.com","https://quantura.studio");
    assert.equal(checkout.line_items![0].price_data!.unit_amount,amount);
    assert.equal(checkout.line_items![0].price_data!.recurring!.interval,interval);
    assert.equal(checkout.subscription_data!.metadata!.uid,"verified-uid");
    assert.equal(checkout.client_reference_id,"verified-uid");
  }
  assert.equal(proSubscriptionPlan({tier:"quant",cycle:"monthly"}),null);
  assert.equal(proSubscriptionPlan({tier:"pro",cycle:"daily"}),null);
  assert.deepEqual(Object.keys(publicPlanEntitlements().plans as object),["pro"]);
});
test("subscription access follows current Stripe status and renewal cancellation",()=>{
  const subscription:any={id:"sub_own",customer:"cus_own",status:"active",cancel_at_period_end:true,metadata:{uid:"owner",source:"quantura_pricing",tier:"pro"}};
  assert.equal(subscriptionAccess(subscription)?.plan,"pro");
  subscription.status="canceled";assert.equal(subscriptionAccess(subscription)?.plan,"free");
  subscription.status="unpaid";assert.equal(subscriptionAccess(subscription)?.plan,"free");
  subscription.metadata.source="shop";assert.equal(subscriptionAccess(subscription),null);
});
