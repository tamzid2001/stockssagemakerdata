import crypto from "node:crypto";
import type Stripe from "stripe";
import catalog from "./meteredCatalog.json";

export const METERED_PRICE_CENTS = 50;
export const DEFAULT_BUDGET_CENTS = 5000;
export const METERED_EVENTS = "metered_billing_events";
const USAGE = "metered_billing_usage";
const RESERVATION_MS = 11 * 60 * 60_000;
const RETRY_WINDOW_MS = 23 * 60 * 60_000;
type RecordValue = Record<string, any>;
function usageId(uid:string,account:RecordValue,now=Date.now()) {
  // A completion can cross renewal before the signed webhook reaches us.
  const start=Number(account.periodEnd)*1000<=now?account.periodEnd:account.periodStart;
  return crypto.createHash("sha256").update(`${uid}:${start||account.trialEnd}`).digest("hex");
}

export function meteredCatalog() {
  return {plan:"metered",label:"Pay as you go",unit:"completed custom ensemble forecast",unit_price_cents:METERED_PRICE_CENTS,
    currency:"usd",base_fee_cents:0,billing_interval:"month",trial_days:14,default_budget_cents:DEFAULT_BUDGET_CENTS};
}

export function meteredCheckoutOptions(uid:string,email:string,origin:string,trial:boolean,customer?:string,promotion=true):Stripe.Checkout.SessionCreateParams {
  const metadata={uid,source:"quantura_metered",tier:"metered",cycle:"monthly"};
  return {mode:"subscription",client_reference_id:uid,...(customer?{customer}:{customer_email:email||undefined}),
    success_url:`${origin}/pricing?checkout=success`,cancel_url:`${origin}/pricing?checkout=cancel`,
    line_items:[{price:catalog.price_id}],payment_method_collection:"always",metadata,
    custom_text:{submit:{message:promotion?"WELCOME50 applied: 50% off your first paid monthly usage invoice after the 14-day trial. Default usage budget: $50/month before discount and tax.":"$0.50 per completed custom ensemble forecast, billed monthly. Default usage budget: $50/month before tax."}},
    ...(promotion?{discounts:[{promotion_code:catalog.promotion_id}]}:{}),
    subscription_data:{metadata,...(trial?{trial_period_days:14,trial_settings:{end_behavior:{missing_payment_method:"cancel"}}}:{})}};
}

export function activeMeteredAccount(value:RecordValue,now=Date.now()) {
  return value.billingMode==="metered" && Boolean(value.stripeCustomerId && value.stripeSubscriptionId)
    && (value.subscriptionStatus==="active" && Number(value.periodEnd)*1000>now
      || value.subscriptionStatus==="trialing" && Number(value.trialEnd)*1000>now);
}

export function reserveMeteredUsage(usage:RecordValue,budget:number,jobId:string,now=Date.now()) {
  const reservations=Object.fromEntries(Object.entries(usage.reservations||{}).filter(([,expiry])=>Number(expiry)>now));
  const spent=Math.max(0,Number(usage.completed||0))*METERED_PRICE_CENTS;
  if(spent+(Object.keys(reservations).length+1)*METERED_PRICE_CENTS>budget)throw Error("monthly_spend_limit_reached");
  reservations[jobId]=now+RESERVATION_MS;
  return {...usage,reservations,completed:Number(usage.completed||0)};
}

export async function createMeteredForecast(db:FirebaseFirestore.Firestore,ref:FirebaseFirestore.DocumentReference,job:RecordValue) {
  return db.runTransaction(async tx=>{
    const billingUserId=job.billing_user_id||job.user_id;
    const account=(await tx.get(db.collection("billing_accounts").doc(billingUserId))).data()||{};
    if(account.billingMode!=="metered" || job.guest_session===true){tx.create(ref,job);return job;}
    if(!activeMeteredAccount(account))throw Error("paid_api_required");
    const trialFree=account.subscriptionStatus==="trialing";
    const periodId=usageId(billingUserId,account);
    if(!trialFree){
      const usageRef=db.collection(USAGE).doc(periodId),usage=(await tx.get(usageRef)).data()||{};
      const reserved=reserveMeteredUsage({...usage,reservations:account.meteredReservations||{}},account.monthlyBudgetCents??DEFAULT_BUDGET_CENTS,ref.id);
      tx.set(db.collection("billing_accounts").doc(billingUserId),{...account,meteredReservations:reserved.reservations});
    }
    const metered={...job,billing:{mode:"metered",trial_free:trialFree,user_id:billingUserId,customer_id:account.stripeCustomerId,
      subscription_id:account.stripeSubscriptionId,amount_cents:trialFree?0:METERED_PRICE_CENTS}};
    tx.create(ref,metered);return metered;
  });
}

/** Read before any transaction writes; completion and its outbox entry commit together. */
export async function prepareMeteredSettlement(db:FirebaseFirestore.Firestore,tx:FirebaseFirestore.Transaction,jobId:string,job:RecordValue,success:boolean) {
  const billing=job.billing;
  if(!billing || billing.mode!=="metered" || billing.trial_free)return ()=>{};
  const accountRef=db.collection("billing_accounts").doc(billing.user_id),account=(await tx.get(accountRef)).data()||{};
  const ref=db.collection(USAGE).doc(usageId(billing.user_id,account)),usage=(await tx.get(ref)).data()||{};
  const reservations={...(account.meteredReservations||{})};delete reservations[jobId];
  return ()=>{
    tx.set(accountRef,{...account,meteredReservations:reservations});
    if(success)tx.set(ref,{...usage,completed:Number(usage.completed||0)+1});
    if(success)tx.create(db.collection(METERED_EVENTS).doc(jobId),{status:"pending",job_id:jobId,customer_id:billing.customer_id,
      value:1,created_at:Date.now(),first_attempt_at:null,lease_until:0});
  };
}

/** Durable outbox + Stripe identifier. Ambiguous sends older than Stripe's dedupe
 * window are held for review rather than risking a second charge. */
export async function publishMeteredEvent(db:FirebaseFirestore.Firestore,jobId:string,stripe:Stripe,now=Date.now()) {
  const ref=db.collection(METERED_EVENTS).doc(jobId),owner=crypto.randomUUID();
  const event=await db.runTransaction(async tx=>{
    const value=(await tx.get(ref)).data();if(!value || value.status!=="pending" || Number(value.lease_until)>now)return null;
    if(value.first_attempt_at && now-Number(value.first_attempt_at)>RETRY_WINDOW_MS){tx.update(ref,{status:"review_required"});return null;}
    tx.update(ref,{first_attempt_at:value.first_attempt_at||now,lease_until:now+120_000,owner});return value;
  });
  if(!event)return;
  try{
    await stripe.billing.meterEvents.create({event_name:catalog.event_name,identifier:`quantura-forecast-${jobId}`,
      timestamp:Math.floor(event.created_at/1000),payload:{stripe_customer_id:event.customer_id,value:"1"}},
      {idempotencyKey:`quantura-forecast-${jobId}`,timeout:20000,maxNetworkRetries:1});
    await db.runTransaction(async tx=>{const current=(await tx.get(ref)).data();if(current?.owner===owner)tx.update(ref,{status:"sent",sent_at:Date.now(),lease_until:0});});
  }catch(error){
    await db.runTransaction(async tx=>{const current=(await tx.get(ref)).data();if(current?.owner===owner)tx.update(ref,{lease_until:0});});
    throw error;
  }
}

export async function flushMeteredEvents(db:FirebaseFirestore.Firestore,stripe:Stripe) {
  const rows=await db.collection(METERED_EVENTS).where("status","==","pending").limit(50).get();
  for(const row of rows.docs)await publishMeteredEvent(db,row.id,stripe);
  return {checked:rows.size};
}

export async function meteredUsageSummary(db:FirebaseFirestore.Firestore,uid:string,account:RecordValue) {
  const id=usageId(uid,account);
  const usage=(await db.collection(USAGE).doc(id).get()).data()||{};
  const reserved=Object.values(account.meteredReservations||{}).filter(expiry=>Number(expiry)>Date.now()).length;
  return {...meteredCatalog(),completed_forecasts:Number(usage.completed||0),usage_cents:Number(usage.completed||0)*METERED_PRICE_CENTS,
    pending_forecasts:reserved,monthly_budget_cents:account.monthlyBudgetCents??DEFAULT_BUDGET_CENTS,
    period_start:account.periodStart?new Date(account.periodStart*1000).toISOString():null,
    period_end:account.periodEnd?new Date(account.periodEnd*1000).toISOString():null,
    trial_free:account.subscriptionStatus==="trialing",discount_note:"Eligible WELCOME50 discount is applied by Stripe to the first paid invoice. Totals here are before discounts and tax."};
}
