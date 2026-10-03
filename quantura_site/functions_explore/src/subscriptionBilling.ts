import type Stripe from "stripe";
import plans from "./planEntitlements.json";

/** New sales have exactly one tier; old invoices/subscriptions are not repriced. */
export function proSubscriptionPlan(value:Record<string,unknown>) {
  if(value.tier!=="pro" || !["monthly","yearly"].includes(String(value.cycle)))return null;
  const cycle=value.cycle as "monthly"|"yearly";
  return {tier:"pro" as const,cycle,amountCents:cycle==="yearly"?plans.plans.pro.annualCents:plans.plans.pro.monthlyCents,
    label:cycle==="yearly"?"Quantura Pro Annual":"Quantura Pro Monthly",description:"Forecasts, screening, historical data, saved research, and API access."};
}

export const PRO_TRIAL_DAYS=14;
export const BILLING_ACCOUNTS="billing_accounts";

export function billingAccess(value:Record<string,any>,now=Date.now()) {
  const trialEnd=Number(value.trialEnd || 0)*1000;
  const active=value.subscriptionStatus==="active" || (value.subscriptionStatus==="trialing" && trialEnd>now);
  return {plan:active?"pro":"free",docs_available:active,can_trial:value.hasUsedTrial!==true,
    trial_ends_at:trialEnd?new Date(trialEnd).toISOString():null,subscription_status:value.subscriptionStatus||"none"};
}

export function proCheckoutOptions(plan:NonNullable<ReturnType<typeof proSubscriptionPlan>>,uid:string,email:string,origin:string,trial=false):Stripe.Checkout.SessionCreateParams {
  const metadata={uid,source:"quantura_pricing",tier:plan.tier,cycle:plan.cycle,amountCents:String(plan.amountCents)};
  return {mode:"subscription",client_reference_id:uid,customer_email:email||undefined,
    success_url:`${origin}/pricing?checkout=success&session_id={CHECKOUT_SESSION_ID}`,cancel_url:`${origin}/pricing?checkout=cancel`,
    line_items:[{quantity:1,price_data:{currency:"usd",unit_amount:plan.amountCents,recurring:{interval:plan.cycle==="yearly"?"year":"month"},product_data:{name:plan.label,description:plan.description}}}],
    payment_method_collection:"always",metadata,subscription_data:{metadata,...(trial?{trial_period_days:PRO_TRIAL_DAYS,trial_settings:{end_behavior:{missing_payment_method:"cancel" as const}}}:{})}};
}

export function subscriptionAccess(subscription:Stripe.Subscription,now=Date.now()) {
  if(subscription.metadata?.source!=="quantura_pricing" || subscription.metadata.tier!=="pro" || !subscription.metadata.uid)return null;
  return {uid:subscription.metadata.uid,plan:subscription.status==="active" || (subscription.status==="trialing" && Number(subscription.trial_end)*1000>now)?"pro":"free",subscriptionId:subscription.id,
    status:subscription.status,cancelAtPeriodEnd:subscription.cancel_at_period_end,createdAt:subscription.created,
    trialEnd:subscription.trial_end || null,hasUsedTrial:Boolean(subscription.trial_start || subscription.trial_end),
    customerId:typeof subscription.customer==="string"?subscription.customer:subscription.customer.id};
}
