import { getClerkUser } from "./clerkAuth";

/** Grants are written only by the backend. Editable user profile fields are
 * never accepted as evidence of an enterprise API subscription. */
export function enterpriseGrant(value: Record<string, any> | undefined, now = Date.now()): boolean {
  if (!value || value.status !== "active" || value.tier !== "enterprise") return false;
  return !value.expires_at || (typeof value.expires_at === "string" && Date.parse(value.expires_at) > now);
}

export async function hasEnterpriseApiAccess(db: FirebaseFirestore.Firestore, userId: string, clerkUserId?: string): Promise<boolean> {
  if (clerkUserId) {
    const user = await getClerkUser(clerkUserId);
    if (user.banned || user.locked) return false;
    if (enterpriseGrant(user.privateMetadata.quantura_api as Record<string, any>)) return true;
  }
  return enterpriseGrant((await db.collection("enterprise_api_accounts").doc(userId).get()).data());
}

export async function requireEnterpriseApiAccess(db: FirebaseFirestore.Firestore, userId: string, clerkUserId?: string) {
  if (!await hasEnterpriseApiAccess(db, userId, clerkUserId)) throw new Error("enterprise_upgrade_required");
}
