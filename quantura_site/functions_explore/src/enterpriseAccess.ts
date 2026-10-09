import { getClerkUser, verifiedQuanturaAdmin } from "./clerkAuth";
import { clerkSubscriptionAccess } from "./clerkBilling";
import { billingAccess } from "./subscriptionBilling";

/** Grants are written only by the backend. Editable user profile fields are
 * never accepted as evidence of an enterprise API subscription. */
export function enterpriseGrant(value: Record<string, any> | undefined, now = Date.now()): boolean {
  if (!value || value.status !== "active" || value.tier !== "enterprise") return false;
  return !value.expires_at || (typeof value.expires_at === "string" && Date.parse(value.expires_at) > now);
}

export async function hasApiAccess(db: FirebaseFirestore.Firestore, userId: string, clerkUserId?: string): Promise<boolean> {
  if (clerkUserId) {
    const user = await getClerkUser(clerkUserId);
    if (user.banned || user.locked || (user.externalId||user.id)!==userId) return false;
    if (verifiedQuanturaAdmin(user,userId)) return true;
    if (enterpriseGrant(user.privateMetadata.quantura_api as Record<string, any>)) return true;
    const access=await clerkSubscriptionAccess(clerkUserId);
    if(paidApiEntitlement(access))return true;
  }
  if(enterpriseGrant((await db.collection("enterprise_api_accounts").doc(userId).get()).data()))return true;
  const ledger=(await db.collection("billing_accounts").doc(userId).get()).data();
  return paidApiEntitlement(billingAccess(ledger || {}));
}

export async function requirePaidApiAccess(db: FirebaseFirestore.Firestore, userId: string, clerkUserId?: string) {
  if (!await hasApiAccess(db, userId, clerkUserId)) throw new Error("paid_api_required");
}

/** Pro includes programmatic access during its valid free trial. The caller
 * supplies server-verified subscription periods, never browser metadata. */
export function paidApiEntitlement(access:{docs_available:boolean;subscription_status:string;trial_ends_at?:string|null;access_ends_at?:string|null}, now=Date.now()):boolean {
  if (!access.docs_available) return false;
  if (access.access_ends_at && !(Date.parse(access.access_ends_at)>now)) return false;
  if (access.subscription_status==="trialing") {
    return Boolean(access.trial_ends_at && Date.parse(access.trial_ends_at)>now);
  }
  return ["active","canceled","admin"].includes(access.subscription_status);
}
