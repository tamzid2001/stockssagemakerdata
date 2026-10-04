import { createClerkClient, verifyToken } from "@clerk/backend";
import type admin from "firebase-admin";
import type { Router } from "express";

export const CLERK_ISSUER = "https://clerk.quantura.studio";
export type QuanturaIdentity = admin.auth.DecodedIdToken & {
  clerk_user_id?: string;
  clerk_session_id?: string;
  clerk_organization_id?: string;
};

export function clerkClient() {
  if (!process.env.CLERK_SECRET_KEY) throw new Error("clerk_not_configured");
  return createClerkClient({ secretKey: process.env.CLERK_SECRET_KEY });
}

function authorizedParties(): string[] {
  const origins = ["https://quantura.studio", "https://www.quantura.studio"];
  for (const value of [process.env.PUBLIC_ORIGIN, ...(process.env.CLERK_AUTHORIZED_PARTIES || "").split(",")]) {
    if (value) origins.push(new URL(value).origin);
  }
  if (process.env.VERCEL_URL) origins.push(`https://${process.env.VERCEL_URL}`);
  if (process.env.NODE_ENV !== "production") origins.push("http://localhost:9020", "http://127.0.0.1:9020");
  return [...new Set(origins)];
}

// The payload is used ONLY to select a verifier. It never establishes identity.
// A token claiming to be from Clerk cannot fall back to Firebase on failure.
export function isClerkToken(token: string): boolean {
  try {
    const payload = JSON.parse(Buffer.from(token.split(".")[1], "base64url").toString());
    if(String(payload.iss || "").startsWith("https://securetoken.google.com/"))return false;
    return payload.iss === CLERK_ISSUER || String(payload.sub || "").startsWith("user_") || Boolean(payload.sid);
  } catch { return false; }
}

const users = new Map<string, { expires: number; value: Promise<Awaited<ReturnType<ReturnType<typeof clerkClient>["users"]["getUser"]>>> }>();
export async function getClerkUser(id: string) {
  const existing = users.get(id);
  if (existing && existing.expires > Date.now()) return existing.value;
  if (users.size >= 2000) users.delete(users.keys().next().value!);
  const value = clerkClient().users.getUser(id);
  users.set(id, { expires: Date.now() + 60_000, value });
  value.catch(() => { if (users.get(id)?.value === value) users.delete(id); });
  return value;
}

export async function verifyClerkIdentity(token: string): Promise<QuanturaIdentity> {
  const claims = await verifyToken(token, {
    secretKey: process.env.CLERK_SECRET_KEY,
    jwtKey: process.env.CLERK_JWT_KEY?.replace(/\\n/g, "\n"),
    authorizedParties: authorizedParties(),
  });
  if (claims.iss !== CLERK_ISSUER || !claims.azp || !authorizedParties().includes(claims.azp) || !/^user_[A-Za-z0-9]+$/.test(claims.sub) || !/^sess_[A-Za-z0-9]+$/.test(String(claims.sid || ""))) {
    throw new Error("unauthenticated");
  }
  const user = await getClerkUser(claims.sub);
  if (user.banned || user.locked) throw new Error("unauthenticated");
  // externalId is set only by the migration/backend, never client metadata.
  const uid = user.externalId || user.id;
  if (!/^[A-Za-z0-9_-]{1,128}$/.test(uid)) throw new Error("identity_invalid");
  const email = user.emailAddresses.find(address => address.id === user.primaryEmailAddressId);
  const verified = email?.verification?.status === "verified";
  const org = String((claims as any).o?.id || (claims as any).org_id || "");
  if (org && !/^org_[A-Za-z0-9]+$/.test(org)) throw new Error("identity_invalid");
  return {
    ...claims, uid, sub: uid, user_id: uid,
    email: verified ? email?.emailAddress : undefined, email_verified: verified,
    name: user.fullName || undefined, picture: user.imageUrl,
    auth_time: Number(claims.iat), admin: false,
    firebase: { identities: {}, sign_in_provider: "clerk" },
    clerk_user_id: user.id, clerk_session_id: String(claims.sid), clerk_organization_id: org || undefined,
  } as QuanturaIdentity;
}

/** One verifier for callable, REST, forecasts, MCP API keys and billing routes.
 * Firebase verification remains available for native clients and data SDKs.
 */
export function createQuanturaAuth(legacy: admin.auth.Auth): admin.auth.Auth {
  return new Proxy(legacy, {
    get(target, property) {
      if (property === "verifyIdToken") return async (token: string, revoked = false) => {
        if (!isClerkToken(token)) return target.verifyIdToken(token, revoked);
        const identity = await verifyClerkIdentity(token);
        if (revoked) {
          const session = await clerkClient().sessions.getSession(identity.clerk_session_id!);
          if (session.status !== "active" || session.userId !== identity.clerk_user_id) throw new Error("unauthenticated");
        }
        return identity;
      };
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

export function registerClerkAuthRoutes(router: Router, db: FirebaseFirestore.Firestore, legacy: admin.auth.Auth) {
  router.post("/auth/clerk/session", async (req, res) => {
    res.set("Cache-Control", "private, no-store");
    try {
      const token = String(req.headers.authorization || "").match(/^Bearer\s+(.+)$/i)?.[1];
      if (!token || !isClerkToken(token)) throw new Error("unauthenticated");
      const identity = await createQuanturaAuth(legacy).verifyIdToken(token, true) as QuanturaIdentity;
      const forward = db.collection("auth_identities").doc(identity.clerk_user_id!);
      const reverse = db.collection("auth_accounts").doc(identity.uid);
      await db.runTransaction(async tx => {
        const [a, b] = await Promise.all([tx.get(forward), tx.get(reverse)]);
        if ((a.exists && a.data()?.uid !== identity.uid) || (b.exists && b.data()?.clerk_user_id !== identity.clerk_user_id)) throw new Error("identity_conflict");
        if (!a.exists) tx.create(forward, { uid: identity.uid, clerk_user_id: identity.clerk_user_id });
        if (!b.exists) tx.create(reverse, { uid: identity.uid, clerk_user_id: identity.clerk_user_id });
      });
      // Compatibility credentials authorize existing Firestore/Storage rules.
      // API callers must send the Clerk session token, not this custom token.
      const customToken = await legacy.createCustomToken(identity.uid, {
        clerk_user_id: identity.clerk_user_id, clerk_session_id: identity.clerk_session_id,
        ...(identity.email_verified ? { email: identity.email, email_verified: true } : {}),
      });
      res.json({ data: { uid: identity.uid, organization_id: identity.clerk_organization_id || null, custom_token: customToken } });
    } catch { res.status(401).json({ error: "unauthenticated", message: "Sign in again to restore your account." }); }
  });
}
