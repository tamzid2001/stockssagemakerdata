import type Stripe from "stripe";
import plans from "./planEntitlements.json";

/** New sales have exactly one tier; old invoices/subscriptions are not repriced. */
export function proSubscriptionPlan(value:Record<string,unknown>) {
  if(value.tier!=="pro" || !["monthly","yearly"].includes(String(value.cycle)))return null;
  const cycle=value.cycle as "monthly"|"yearly";
  return {tier:"pro" as const,cycle,amountCents:cycle==="yearly"?plans.plans.pro.annualCents:plans.plans.pro.monthlyCents,
    label:cycle==="yearly"?"Quantura Pro Annual":"Quantura Pro Monthly",description:"Forecasts, screening, historical data, saved research, and API access."};
}

export function proCheckoutOptions(plan:NonNullable<ReturnType<typeof proSubscriptionPlan>>,uid:string,email:string,origin:string):Stripe.Checkout.SessionCreateParams {
  const metadata={uid,source:"quantura_pricing",tier:plan.tier,cycle:plan.cycle,amountCents:String(plan.amountCents)};
  return {mode:"subscription",client_reference_id:uid,customer_email:email||undefined,
    success_url:`${origin}/pricing?checkout=success&session_id={CHECKOUT_SESSION_ID}`,cancel_url:`${origin}/pricing?checkout=cancel`,
    line_items:[{quantity:1,price_data:{currency:"usd",unit_amount:plan.amountCents,recurring:{interval:plan.cycle==="yearly"?"year":"month"},product_data:{name:plan.label,description:plan.description}}}],
    metadata,subscription_data:{metadata}};
}

export function subscriptionAccess(subscription:Stripe.Subscription) {
  if(subscription.metadata?.source!=="quantura_pricing" || subscription.metadata.tier!=="pro" || !subscription.metadata.uid)return null;
  return {uid:subscription.metadata.uid,plan:["active","trialing"].includes(subscription.status)?"pro":"free",subscriptionId:subscription.id,
    status:subscription.status,cancelAtPeriodEnd:subscription.cancel_at_period_end,createdAt:subscription.created,
    customerId:typeof subscription.customer==="string"?subscription.customer:subscription.customer.id};
}
