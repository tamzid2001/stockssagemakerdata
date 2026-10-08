import { clerkClient, CLERK_ISSUER, getClerkUser } from "./clerkAuth";

// The REST API and MCP are one Quantura protected resource. Never accept a
// token minted for another Clerk application/resource, even from this issuer.
export const QUANTURA_OAUTH_RESOURCE = "https://quantura.studio";
export const QUANTURA_OAUTH_SCOPES = ["openid", "profile", "email"] as const;
export const QUANTURA_CLI_CLIENT_ID = "pOqhsrxJxggZ8m8a";
export const QUANTURA_CLI_REDIRECT = "http://127.0.0.1:8766/callback";

/** Unverified parsing only selects the verifier; it never establishes identity. */
export function isClerkOAuthToken(token: string): boolean {
  if (token.startsWith("oat_")) return true;
  try {
    const header = JSON.parse(Buffer.from(token.split(".")[0], "base64url").toString());
    const payload = JSON.parse(Buffer.from(token.split(".")[1], "base64url").toString());
    return ["at+jwt", "application/at+jwt"].includes(header.typ)
      || (payload.iss === CLERK_ISSUER && String(payload.jti || "").startsWith("oat_"));
  } catch { return false; }
}

type OAuthToken = { subject: string; clientId: string; id: string; scopes: string[];
  revoked: boolean; expired: boolean; expiration: number | null; aud?: string[] };
type Dependencies = {
  verify: (token: string, audience: string) => Promise<OAuthToken>;
  user: typeof getClerkUser;
  now: () => number;
};
const defaults: Dependencies = {
  verify: (token, audience) => clerkClient().idPOAuthAccessToken.verify(token, { audience }),
  user: getClerkUser, now: Date.now,
};

export async function verifyQuanturaOAuth(token: string, deps: Dependencies = defaults) {
  const access = await deps.verify(token, QUANTURA_OAUTH_RESOURCE).catch(() => null);
  if (!access || access.revoked || access.expired
      || access.expiration !== null && access.expiration <= deps.now() / 1000
      || !access.aud?.includes(QUANTURA_OAUTH_RESOURCE)
      || !/^user_[A-Za-z0-9]+$/.test(access.subject) || !access.clientId
      || !QUANTURA_OAUTH_SCOPES.every(scope => access.scopes.includes(scope))) {
    throw new Error("api_key_invalid");
  }
  const user = await deps.user(access.subject);
  const userId = user.externalId || user.id;
  if (user.id !== access.subject || user.banned || user.locked || !/^[A-Za-z0-9_-]{1,128}$/.test(userId)) {
    throw new Error("api_key_invalid");
  }
  return { access, user, userId };
}
