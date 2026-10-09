import type { BillingSubscription } from "@clerk/backend";
import { clerkClient, getClerkUser, verifiedQuanturaAdmin } from "./clerkAuth";

export function subscriptionEntitlements(subscription: Pick<BillingSubscription, "subscriptionItems" | "eligibleForFreeTrial"> | null, now = Date.now()) {
  const item = subscription?.subscriptionItems.find(item =>
    ["pro", "pro_org"].includes(item.plan?.slug || "")
    && ["active", "canceled"].includes(item.status)
    && item.periodStart <= now && Number(item.periodEnd) > now);
  return { plan: item ? "pro" : "free", docs_available: Boolean(item),
    can_trial: !subscription || subscription.eligibleForFreeTrial === true,
    has_previous_subscription:Boolean(subscription?.subscriptionItems.some(item=>["pro","pro_org"].includes(item.plan?.slug||""))),
    access_ends_at:item?.periodEnd?new Date(item.periodEnd).toISOString():null,
    trial_ends_at: item?.isFreeTrial ? new Date(item.periodEnd!).toISOString() : null,
    subscription_status: item ? item.isFreeTrial ? "trialing" : item.status : "none", billing_provider: "clerk" };
}

const cache = new Map<string, { expires: number; value: Promise<ReturnType<typeof subscriptionEntitlements>> }>();
export async function clerkSubscriptionAccess(userId: string, organizationId?: string) {
  if(!organizationId&&verifiedQuanturaAdmin(await getClerkUser(userId)))return {...subscriptionEntitlements(null),plan:"pro",docs_available:true,can_trial:false,subscription_status:"admin",billing_provider:"admin"};
  const key = organizationId || userId;
  const found = cache.get(key);
  if (found && found.expires > Date.now()) {
    const access=await found.value;
    if(!access.access_ends_at || Date.parse(access.access_ends_at)>Date.now())return access;
  }
  if(cache.size>=2000)cache.delete(cache.keys().next().value!);
  const value=(organizationId ? clerkClient().billing.getOrganizationBillingSubscription(organizationId) : clerkClient().billing.getUserBillingSubscription(userId))
    .then(subscription => subscriptionEntitlements(subscription))
    .catch(error => { if(error?.status === 404)return subscriptionEntitlements(null);throw error; });
  cache.set(key,{expires:Date.now()+5000,value});
  value.catch(()=>{if(cache.get(key)?.value===value)cache.delete(key);});
  return value;
}

export function invalidateClerkBilling(payerId: string) { cache.delete(payerId); }
