import crypto from "node:crypto";
import type { Request } from "express";
import type admin from "firebase-admin";
import { normalizePlan, PLAN_ENTITLEMENTS, planHasFeature, type PlanKey } from "./planEntitlements";

export const PLATFORM_API_SCOPES = [
  "account:read", "workspaces:read", "forecasts:read", "forecasts:write",
  "forecasts:history", "forecasts:resolved", "forecasts:bulk",
  "predictions:read", "predictions:write", "screener:read", "market_data:read",
  "options:read", "sports:read", "datasets:read", "datasets:write",
  "alerts:read", "alerts:write", "backtests:read", "backtests:run",
  "api_usage:read", "sagemaker:read", "sagemaker:execute"
] as const;
export type PlatformApiScope = typeof PLATFORM_API_SCOPES[number];
export type WorkspaceRole = "owner" | "editor" | "viewer";

export type ApiPrincipal = {
  userId: string;
  tokenId: string | null;
  tokenName: string;
  tokenScopes: PlatformApiScope[];
  plan: PlanKey;
  authMethod: "api_key" | "firebase_session";
};

export type WorkspaceAccess = {
  workspaceId: string;
  role: WorkspaceRole;
  plan: PlanKey;
  capabilities: string[];
};

const API_KEYS = "quantura_api_keys";
const API_AUDIT = "quantura_api_audit";
const KEY_PREFIX = "qnt_live_";

function clean(value: unknown, max = 500): string {
  return String(value ?? "").trim().slice(0, max);
}

export function validatePlatformScopes(value: unknown): PlatformApiScope[] {
  if (!Array.isArray(value)) throw new Error("invalid_scopes");
  const allowed = new Set<string>(PLATFORM_API_SCOPES);
  const scopes = [...new Set(value.map((item) => clean(item, 80)).filter((item) => allowed.has(item)))] as PlatformApiScope[];
  if (!scopes.length || scopes.length !== value.length) throw new Error("invalid_scopes");
  return scopes;
}

export function generatePlatformApiKey(): { rawKey: string; prefix: string } {
  const rawKey = `${KEY_PREFIX}${crypto.randomBytes(32).toString("base64url")}`;
  return { rawKey, prefix: rawKey.slice(0, KEY_PREFIX.length + 8) };
}

export function hashPlatformApiKey(rawKey: string, pepper = process.env.QUANTURA_API_KEY_PEPPER || ""): string {
  if (!clean(rawKey, 1000).startsWith(KEY_PREFIX)) throw new Error("api_key_invalid");
  const secretPepper = clean(pepper, 4000);
  if (secretPepper.length < 32) throw new Error("api_key_pepper_not_configured");
  return crypto.createHmac("sha256", secretPepper).update(rawKey, "utf8").digest("hex");
}

export function extractBearer(req: Request): string {
  const authorization = clean(req.headers.authorization, 1200);
  return authorization.match(/^Bearer\s+(.+)$/i)?.[1]?.trim() || "";
}

async function userPlan(db: FirebaseFirestore.Firestore, userId: string): Promise<PlanKey> {
  const snapshot = await db.collection("users").doc(userId).get();
  const value = (snapshot.data() || {}) as Record<string, any>;
  return normalizePlan(value.plan || value.subscriptionTier || value.profile?.plan);
}

export async function authenticatePlatformRequest(
  req: Request,
  options: { db: FirebaseFirestore.Firestore; auth: admin.auth.Auth }
): Promise<ApiPrincipal> {
  const bearer = extractBearer(req);
  if (!bearer) throw new Error("api_key_missing");

  if (!bearer.startsWith(KEY_PREFIX)) {
    const decoded = await options.auth.verifyIdToken(bearer).catch(() => null);
    if (!decoded?.uid) throw new Error("api_key_invalid");
    return {
      userId: decoded.uid,
      tokenId: null,
      tokenName: "Web session",
      tokenScopes: [...PLATFORM_API_SCOPES],
      plan: await userPlan(options.db, decoded.uid),
      authMethod: "firebase_session",
    };
  }

  const tokenId = hashPlatformApiKey(bearer);
  const snapshot = await options.db.collection(API_KEYS).doc(tokenId).get();
  if (!snapshot.exists) throw new Error("api_key_invalid");
  const value = (snapshot.data() || {}) as Record<string, any>;
  if (value.revoked_at) throw new Error("api_key_revoked");
  if (value.expires_at && Date.parse(String(value.expires_at)) <= Date.now()) throw new Error("api_key_expired");
  const userId = clean(value.user_id, 220);
  if (!userId) throw new Error("api_key_invalid");
  const scopes = validatePlatformScopes(value.scopes);
  void snapshot.ref.set({ last_used_at: new Date().toISOString() }, { merge: true }).catch(() => undefined);
  return {
    userId,
    tokenId,
    tokenName: clean(value.name, 120),
    tokenScopes: scopes,
    plan: await userPlan(options.db, userId),
    authMethod: "api_key",
  };
}

export function requireScope(principal: ApiPrincipal, scope: PlatformApiScope): void {
  if (!principal.tokenScopes.includes(scope)) throw new Error("insufficient_scope");
}

export async function resolveWorkspaceAccess(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  workspaceIdValue: unknown
): Promise<WorkspaceAccess> {
  const workspaceId = clean(workspaceIdValue, 220);
  if (!workspaceId) throw new Error("workspace_id_required");
  if (workspaceId === principal.userId) {
    return {
      workspaceId,
      role: "owner",
      plan: principal.plan,
      capabilities: PLAN_ENTITLEMENTS[principal.plan].features,
    };
  }
  const membership = await db.collection("users").doc(workspaceId).collection("collaborators").doc(principal.userId).get();
  if (!membership.exists) throw new Error("workspace_forbidden");
  const rawRole = clean(membership.data()?.role, 20).toLowerCase();
  const role: WorkspaceRole = rawRole === "editor" ? "editor" : "viewer";
  const ownerPlan = await userPlan(db, workspaceId);
  return {
    workspaceId,
    role,
    plan: ownerPlan,
    capabilities: PLAN_ENTITLEMENTS[ownerPlan].features,
  };
}

export function authorizeWorkspaceAction(
  principal: ApiPrincipal,
  access: WorkspaceAccess,
  scope: PlatformApiScope,
  action: "read" | "write" | "delete"
): void {
  requireScope(principal, scope);
  if (principal.authMethod === "api_key" && !planHasFeature(access.plan, "api")) {
    // A collaborator's personal plan must not erase legitimate read access to
    // a shared workspace. Standalone access to the token owner's workspace and
    // every state-changing API operation still require the workspace plan's
    // API entitlement.
    const sharedReadException = action === "read" && access.role !== "owner";
    if (!sharedReadException) throw new Error("plan_upgrade_required");
  }
  if (action !== "read" && access.role === "viewer") throw new Error("workspace_read_only");
  if (action === "delete" && access.role !== "owner") throw new Error("workspace_owner_required");
  if (action === "read") return;
  if (scope === "backtests:run" && !planHasFeature(access.plan, "backtesting")) throw new Error("plan_upgrade_required");
  if (scope === "datasets:write" && !planHasFeature(access.plan, "bulk_exports")) throw new Error("plan_upgrade_required");
}

export async function listAccessibleWorkspaces(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal
): Promise<WorkspaceAccess[]> {
  const own = await resolveWorkspaceAccess(db, principal, principal.userId);
  const shared = await db.collection("users").doc(principal.userId).collection("shared_workspaces").limit(100).get();
  const workspaceIds = [...new Set(shared.docs.map((doc) => clean(doc.data()?.workspaceUserId || doc.id, 220)).filter(Boolean))];
  const memberships = await Promise.all(workspaceIds.map((workspaceId) => resolveWorkspaceAccess(db, principal, workspaceId).catch(() => null)));
  return [own, ...memberships.filter((item): item is WorkspaceAccess => Boolean(item))];
}

export async function createPersonalApiKey(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  input: { name: unknown; scopes: unknown; expiresAt?: unknown }
): Promise<{ id: string; key: string; prefix: string; name: string; scopes: PlatformApiScope[]; createdAt: string; expiresAt: string | null }> {
  const name = clean(input.name, 120);
  if (!name) throw new Error("api_key_name_required");
  const scopes = validatePlatformScopes(input.scopes);
  const expiresRaw = clean(input.expiresAt, 80);
  const expiresAt = expiresRaw ? new Date(expiresRaw).toISOString() : null;
  if (expiresAt && Date.parse(expiresAt) <= Date.now()) throw new Error("api_key_expiration_invalid");
  const { rawKey, prefix } = generatePlatformApiKey();
  const id = hashPlatformApiKey(rawKey);
  const createdAt = new Date().toISOString();
  await db.collection(API_KEYS).doc(id).create({
    user_id: principal.userId,
    name,
    prefix,
    scopes,
    created_at: createdAt,
    last_used_at: null,
    expires_at: expiresAt,
    revoked_at: null,
  });
  return { id, key: rawKey, prefix, name, scopes, createdAt, expiresAt };
}

export async function writeApiAudit(
  db: FirebaseFirestore.Firestore,
  input: { principal?: ApiPrincipal; workspaceId?: string | null; endpoint: string; method: string; status: number; resource?: string | null; requestId: string; latencyMs: number }
): Promise<void> {
  await db.collection(API_AUDIT).doc(input.requestId).set({
    request_id: input.requestId,
    token_id: input.principal?.tokenId || null,
    user_id: input.principal?.userId || null,
    workspace_id: input.workspaceId || null,
    endpoint: input.endpoint,
    action: input.method,
    resource: input.resource || null,
    timestamp: new Date().toISOString(),
    response_status: input.status,
    success: input.status < 400,
    latency_ms: input.latencyMs,
  });
}
