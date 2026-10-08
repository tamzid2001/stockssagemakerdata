import crypto from "node:crypto";
import type { Request } from "express";
import type admin from "firebase-admin";
import { normalizePlan, PLAN_ENTITLEMENTS, planHasFeature, type PlanKey } from "./planEntitlements";
import { clerkClient, getClerkUser, type QuanturaIdentity } from "./clerkAuth";
import { clerkSubscriptionAccess } from "./clerkBilling";
import { rapidApiPrincipal } from "./rapidApiAuth";
import { requirePaidApiAccess } from "./enterpriseAccess";
import { isClerkOAuthToken, verifyQuanturaOAuth } from "./clerkOAuth";

export const PLATFORM_API_SCOPES = [
  "account:read", "workspaces:read", "workspaces:write", "forecasts:read", "forecasts:write",
  "forecasts:history", "forecasts:resolved", "forecasts:bulk",
  "predictions:read", "predictions:write", "screener:read", "market_data:read",
  "options:read", "sports:read", "datasets:read", "datasets:write",
  "alerts:read", "alerts:write", "backtests:read", "backtests:run",
  "api_usage:read", "sagemaker:read", "sagemaker:execute"
] as const;
export type PlatformApiScope = typeof PLATFORM_API_SCOPES[number];
export const WORKSPACE_PERMISSIONS = [
  "workspace.read", "workspace.settings.read", "workspace.settings.write",
  "workspace.members.read", "workspace.members.invite", "workspace.members.update", "workspace.members.remove",
  "csv.list", "csv.read", "csv.download", "csv.upload", "csv.rename", "csv.copy", "csv.move", "csv.delete",
  "forecast.read", "forecast.create", "forecast.delete", "analysis.read", "analysis.create",
  "screener.read", "historical_data.read", "options.read", "sports.read", "api.read", "exports.create",
] as const;
export type WorkspacePermission = typeof WORKSPACE_PERMISSIONS[number];
export type WorkspaceRole = "owner" | "admin" | "editor" | "analyst" | "viewer" | "custom";
export type WorkspaceResourceScope = {
  csv: { mode: "all" | "selected" | "none"; ids: string[] };
  forecasts: { mode: "all" | "selected" | "none"; ids: string[] };
};

const VIEWER_PERMISSIONS: WorkspacePermission[] = [
  "workspace.read", "workspace.settings.read", "workspace.members.read",
  "csv.list", "csv.read", "csv.download", "forecast.read", "analysis.read",
];
const ANALYST_PERMISSIONS: WorkspacePermission[] = [
  ...VIEWER_PERMISSIONS, "csv.upload", "forecast.create", "analysis.create", "exports.create",
  "screener.read", "historical_data.read", "options.read", "sports.read", "api.read",
];
const EDITOR_PERMISSIONS: WorkspacePermission[] = [
  ...ANALYST_PERMISSIONS, "csv.rename", "csv.copy", "csv.move",
];
const ADMIN_PERMISSIONS: WorkspacePermission[] = WORKSPACE_PERMISSIONS.filter((permission) => permission !== "csv.delete");

export const WORKSPACE_PERMISSION_PRESETS: Record<Exclude<WorkspaceRole, "owner" | "custom">, WorkspacePermission[]> = {
  viewer: VIEWER_PERMISSIONS,
  analyst: ANALYST_PERMISSIONS,
  editor: EDITOR_PERMISSIONS,
  admin: ADMIN_PERMISSIONS,
};

export type ApiPrincipal = {
  userId: string;
  tokenId: string | null;
  tokenName: string;
  tokenScopes: PlatformApiScope[];
  plan: PlanKey;
  authMethod: "api_key" | "firebase_session" | "clerk_session" | "clerk_oauth" | "rapidapi";
  clerkUserId?: string;
  organizationId?: string;
  platformAdmin?: boolean;
  guest?: boolean;
};

export type WorkspaceAccess = {
  workspaceId: string;
  role: WorkspaceRole;
  plan: PlanKey;
  capabilities: string[];
  permissions: WorkspacePermission[];
  resourceScope: WorkspaceResourceScope;
  ownerUserId: string;
  name: string;
  slug: string;
  description?: string;
  createdAt?: string | null;
  updatedAt?: string | null;
  settings?: { default_csv_visibility: "workspace" | "private"; allow_member_invites: boolean };
  legacyPersonal: boolean;
};

const API_KEYS = "quantura_api_keys";
const API_AUDIT = "quantura_api_audit";
const KEY_PREFIX = "qnt_live_";
const WORKSPACES = "workspaces";
const WORKSPACE_MEMBERSHIPS = "workspace_memberships";

function defaultResourceScope(): WorkspaceResourceScope {
  return { csv: { mode: "all", ids: [] }, forecasts: { mode: "all", ids: [] } };
}

export function workspaceMembershipId(workspaceId: string, userId: string): string {
  return `wm_${crypto.createHash("sha256").update(`${workspaceId}\u0000${userId}`).digest("hex")}`;
}

export function validateWorkspacePermissions(value: unknown): WorkspacePermission[] {
  if (!Array.isArray(value)) throw new Error("workspace_permissions_invalid");
  const allowed = new Set<string>(WORKSPACE_PERMISSIONS);
  const permissions = [...new Set(value.map((item) => clean(item, 80)))];
  if (permissions.some((item) => !allowed.has(item))) throw new Error("workspace_permissions_invalid");
  return permissions as WorkspacePermission[];
}

export function permissionsForRole(roleValue: unknown, explicitValue?: unknown): WorkspacePermission[] {
  const role = clean(roleValue, 20).toLowerCase() as WorkspaceRole;
  if (role === "owner") return [...WORKSPACE_PERMISSIONS];
  if (role === "custom") return validateWorkspacePermissions(explicitValue);
  if (role in WORKSPACE_PERMISSION_PRESETS) return [...WORKSPACE_PERMISSION_PRESETS[role as keyof typeof WORKSPACE_PERMISSION_PRESETS]];
  throw new Error("workspace_role_invalid");
}

export function validateWorkspaceResourceScope(value: unknown): WorkspaceResourceScope {
  const input = value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, any> : {};
  const normalize = (raw: unknown): { mode: "all" | "selected" | "none"; ids: string[] } => {
    const item = raw && typeof raw === "object" && !Array.isArray(raw) ? raw as Record<string, any> : {};
    const mode = clean(item.mode || "all", 20).toLowerCase();
    if (!(["all", "selected", "none"] as string[]).includes(mode)) throw new Error("workspace_resource_scope_invalid");
    const ids = Array.isArray(item.ids) ? [...new Set(item.ids.map((id) => clean(id, 220)).filter(Boolean))].slice(0, 500) : [];
    return { mode: mode as "all" | "selected" | "none", ids };
  };
  return { csv: normalize(input.csv), forecasts: normalize(input.forecasts) };
}

export function assertGrantablePermissions(actor: WorkspaceAccess, requested: WorkspacePermission[]): void {
  if (actor.role === "owner") return;
  const grantable = new Set(actor.permissions);
  if (requested.some((permission) => !grantable.has(permission))) throw new Error("workspace_permission_escalation");
}

export function assertGrantableResourceScope(actor: WorkspaceAccess, requested: WorkspaceResourceScope): void {
  if (actor.role === "owner") return;
  for (const family of ["csv", "forecasts"] as const) {
    const current = actor.resourceScope[family];
    const proposed = requested[family];
    if (current.mode === "none" && proposed.mode !== "none") throw new Error("workspace_resource_scope_escalation");
    if (current.mode === "selected") {
      if (proposed.mode === "all") throw new Error("workspace_resource_scope_escalation");
      if (proposed.mode === "selected" && proposed.ids.some((id) => !current.ids.includes(id))) {
        throw new Error("workspace_resource_scope_escalation");
      }
    }
  }
}

export function requireWorkspacePermission(access: WorkspaceAccess, permission: WorkspacePermission, resourceId?: string): void {
  if (!access.permissions.includes(permission)) throw new Error("workspace_permission_denied");
  const family = permission.startsWith("csv.") ? "csv" : permission.startsWith("forecast.") ? "forecasts" : null;
  if (!family || !resourceId) return;
  const scope = access.resourceScope[family];
  if (scope.mode === "none" || (scope.mode === "selected" && !scope.ids.includes(resourceId))) {
    throw new Error("workspace_resource_forbidden");
  }
}

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
  const authorization = clean(req.headers.authorization, 16384);
  return authorization.match(/^Bearer\s+(.+)$/i)?.[1]?.trim() || "";
}

async function userPlan(db: FirebaseFirestore.Firestore, userId: string, clerkUserId?: string): Promise<PlanKey> {
  if(clerkUserId && (await clerkSubscriptionAccess(clerkUserId)).docs_available)return "pro";
  const billing=await db.collection("billing_accounts").doc(userId).get();
  if(billing.exists){
    const value=billing.data()||{};
    return value.subscriptionStatus==="active" || (value.subscriptionStatus==="trialing" && Number(value.trialEnd)*1000>Date.now())?"pro":"free";
  }
  const snapshot = await db.collection("users").doc(userId).get();
  const value = (snapshot.data() || {}) as Record<string, any>;
  const plan=normalizePlan(value.plan || value.subscriptionTier || value.profile?.plan);
  // Pro entitlements originate only from signed Stripe events, never editable
  // profile fields. Historical Quant/Research records retain their old behavior.
  return plan==="pro"?"free":plan;
}

export async function authenticatePlatformRequest(
  req: Request,
  options: { db: FirebaseFirestore.Firestore; auth: admin.auth.Auth; adminEmails?: readonly string[] }
): Promise<ApiPrincipal> {
  const rapid = rapidApiPrincipal(req);
  if (rapid) return rapid;
  const bearer = extractBearer(req);
  if (!bearer) throw new Error("api_key_missing");

  if (isClerkOAuthToken(bearer)) {
    const {access, user, userId} = await verifyQuanturaOAuth(bearer);
    await requirePaidApiAccess(options.db, userId, user.id);
    return {userId, clerkUserId:user.id, tokenId:access.id, tokenName:"OAuth application",
      tokenScopes:[...PLATFORM_API_SCOPES],
      plan:(await userPlan(options.db,userId,user.id))==="pro"?"pro":"research",
      authMethod:"clerk_oauth",
      platformAdmin:await verifiedPlatformAdmin(options.auth,userId,options.adminEmails,user.id)};
  }

  if(bearer.startsWith("ak_")) {
    const key=await clerkClient().apiKeys.verify(bearer).catch(()=>null);
    if(!key)throw new Error("api_key_invalid");
    if(key.revoked)throw new Error("api_key_revoked");
    if(key.expired||key.expiration!==null&&key.expiration<=Date.now())throw new Error("api_key_expired");
    // Personal keys cannot impersonate the creator of an organization key.
    if(!key.subject.startsWith("user_"))throw new Error("api_key_invalid");
    const user=await getClerkUser(key.subject);
    if(user.banned||user.locked)throw new Error("api_key_invalid");
    const userId=user.externalId||user.id;
    await requirePaidApiAccess(options.db,userId,user.id);
    const scopes=key.scopes.length?validatePlatformScopes(key.scopes):[...PLATFORM_API_SCOPES];
    return {userId,clerkUserId:user.id,tokenId:key.id,tokenName:key.name,tokenScopes:scopes,plan:(await userPlan(options.db,userId,user.id))==="pro"?"pro":"research",authMethod:"api_key",platformAdmin:await verifiedPlatformAdmin(options.auth,userId,options.adminEmails,user.id)};
  }

  if (!bearer.startsWith(KEY_PREFIX)) {
    const decoded = await options.auth.verifyIdToken(bearer).catch(() => null) as QuanturaIdentity | null;
    if (!decoded?.uid) throw new Error("api_key_invalid");
    return {
      userId: decoded.uid,
      tokenId: null,
      tokenName: "Web session",
      tokenScopes: [...PLATFORM_API_SCOPES],
      plan: decoded.firebase?.sign_in_provider === "anonymous" ? "free" : await userPlan(options.db, decoded.uid, decoded.clerk_user_id),
      authMethod: decoded.clerk_user_id ? "clerk_session" : "firebase_session",
      clerkUserId: decoded.clerk_user_id,
      organizationId: decoded.clerk_organization_id,
      guest: decoded.firebase?.sign_in_provider === "anonymous",
      platformAdmin: await verifiedPlatformAdmin(options.auth, decoded.uid, options.adminEmails, decoded.clerk_user_id),
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
  let clerkUserId=clean(value.clerk_user_id,128) || undefined;
  if(!clerkUserId && process.env.CLERK_SECRET_KEY)clerkUserId=(await options.db.collection("auth_accounts").doc(userId).get()).data()?.clerk_user_id;
  await requirePaidApiAccess(options.db, userId, clerkUserId);
  // Revocation is checked on every request. Coalesce concurrent metadata
  // updates and persist at most one last-used timestamp per five minutes.
  touchApiKey(tokenId,snapshot.ref,value.last_used_at);
  return {
    userId,
    tokenId,
    tokenName: clean(value.name, 120),
    tokenScopes: scopes,
    plan: (await userPlan(options.db,userId,clerkUserId))==="pro"?"pro":"research",
    clerkUserId,
    authMethod: "api_key",
    platformAdmin: await verifiedPlatformAdmin(options.auth, userId, options.adminEmails, clerkUserId),
  };
}

const keyTouches=new Map<string,number>();
function touchApiKey(id:string,ref:FirebaseFirestore.DocumentReference,last:unknown){
  const now=Date.now(),persisted=Date.parse(String(last));
  if(Number.isFinite(persisted)&&now-persisted<300000||now-(keyTouches.get(id)||0)<300000)return;
  if(keyTouches.size>=2000)keyTouches.delete(keyTouches.keys().next().value!);
  keyTouches.set(id,now);
  void ref.set({last_used_at:new Date(now).toISOString()},{merge:true}).catch(()=>{if(keyTouches.get(id)===now)keyTouches.delete(id);});
}

export async function verifiedPlatformAdmin(auth: admin.auth.Auth, userId: string, allowed: readonly string[] = [], clerkUserId?: string): Promise<boolean> {
  if (!allowed.length) return false;
  if (clerkUserId) {
    const user = await getClerkUser(clerkUserId).catch(() => null);
    if (!user || user.banned || user.locked || (user.externalId || user.id) !== userId) return false;
    const email = user.emailAddresses.find(address => address.id === user.primaryEmailAddressId);
    return email?.verification?.status === "verified" && allowed.some(value => value.toLowerCase() === email.emailAddress.toLowerCase());
  }
  // Native clients retain their authoritative Firebase identity. Neither path
  // accepts editable metadata or stale role claims as administrator authority.
  // This role never bypasses workspace membership.
  const user = await auth.getUser(userId).catch(() => null);
  return Boolean(user?.uid === userId && !user.disabled && user.emailVerified
    && allowed.some(email => email.toLowerCase() === user.email?.toLowerCase()));
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
  if (principal.authMethod === "rapidapi" && workspaceId !== principal.userId) throw new Error("workspace_forbidden");
  if(workspaceId.startsWith("org_")) {
    let clerkUserId=principal.clerkUserId;
    if(!clerkUserId)clerkUserId=(await db.collection("auth_accounts").doc(principal.userId).get()).data()?.clerk_user_id;
    if(!clerkUserId)throw new Error("workspace_forbidden");
    // Check current membership for every request, including personal API keys.
    // A selected org, request body, or stale JWT role is not an authorization.
    const members=await clerkClient().organizations.getOrganizationMembershipList({organizationId:workspaceId,userId:[clerkUserId],limit:1});
    const member=members.data.find(item=>item.publicUserData?.userId===clerkUserId);
    if(!member)throw new Error("workspace_forbidden");
    const organization=member.organization;
    const role:WorkspaceRole=organization.createdBy===clerkUserId && member.role==="org:admin"?"owner":member.role==="org:admin"?"admin":member.role==="org:member"?"analyst":"viewer";
    const subscription=await clerkSubscriptionAccess(clerkUserId,workspaceId);
    const plan:PlanKey=subscription.docs_available?"pro":"free";
    return {workspaceId,role,plan,capabilities:PLAN_ENTITLEMENTS[plan].features,permissions:permissionsForRole(role),resourceScope:defaultResourceScope(),
      ownerUserId:workspaceId,name:organization.name,slug:organization.slug||workspaceId,legacyPersonal:false};
  }
  const workspaceSnapshot = await db.collection(WORKSPACES).doc(workspaceId).get();
  const workspace = (workspaceSnapshot.data() || {}) as Record<string, any>;
  const ownerUserId = clean(workspace.owner_user_id, 220);
  if (workspaceSnapshot.exists && workspace.archived_at) throw new Error("workspace_archived");
  if ((workspaceSnapshot.exists && ownerUserId === principal.userId) || (!workspaceSnapshot.exists && workspaceId === principal.userId)) {
    // Server-configured platform admins may exercise configured model features
    // in their own workspaces under bounded Research quotas, without changing
    // their actual subscription or granting access to other owners' resources.
    const effectivePlan = principal.platformAdmin ? "research" : principal.plan;
    return {
      workspaceId,
      role: "owner",
      plan: effectivePlan,
      capabilities: PLAN_ENTITLEMENTS[effectivePlan].features,
      permissions: [...WORKSPACE_PERMISSIONS],
      resourceScope: defaultResourceScope(),
      ownerUserId: principal.userId,
      name: clean(workspace.name, 120) || "Personal Workspace",
      slug: clean(workspace.slug, 120) || "personal",
      description: clean(workspace.description, 500),
      createdAt: clean(workspace.created_at, 80) || null,
      updatedAt: clean(workspace.updated_at, 80) || null,
      settings: {
        default_csv_visibility: clean(workspace.settings?.default_csv_visibility, 20) === "private" ? "private" : "workspace",
        allow_member_invites: Boolean(workspace.settings?.allow_member_invites),
      },
      legacyPersonal: !workspaceSnapshot.exists,
    };
  }
  const explicitMembership = workspaceSnapshot.exists
    ? await db.collection(WORKSPACE_MEMBERSHIPS).doc(workspaceMembershipId(workspaceId, principal.userId)).get()
    : null;
  let membershipData = explicitMembership?.exists ? (explicitMembership.data() || {}) as Record<string, any> : null;
  let resolvedOwnerId = ownerUserId;
  if (!membershipData) {
    const legacyMembership = await db.collection("users").doc(workspaceId).collection("collaborators").doc(principal.userId).get();
    if (!legacyMembership.exists) throw new Error("workspace_forbidden");
    membershipData = (legacyMembership.data() || {}) as Record<string, any>;
    resolvedOwnerId = workspaceId;
  }
  if (clean(membershipData.status || "active", 20).toLowerCase() !== "active") throw new Error("workspace_forbidden");
  const rawRole = clean(membershipData.role || "viewer", 20).toLowerCase() as WorkspaceRole;
  const permissions = permissionsForRole(rawRole, membershipData.permissions);
  const ownerPlan = await userPlan(db, resolvedOwnerId);
  return {
    workspaceId,
    role: rawRole,
    plan: ownerPlan,
    capabilities: PLAN_ENTITLEMENTS[ownerPlan].features,
    permissions,
    resourceScope: validateWorkspaceResourceScope(membershipData.resource_scope),
    ownerUserId: resolvedOwnerId,
    name: clean(workspace.name, 120) || clean(membershipData.workspace_name, 120) || "Shared Workspace",
    slug: clean(workspace.slug, 120),
    description: clean(workspace.description, 500),
    createdAt: clean(workspace.created_at, 80) || null,
    updatedAt: clean(workspace.updated_at, 80) || null,
    settings: {
      default_csv_visibility: clean(workspace.settings?.default_csv_visibility, 20) === "private" ? "private" : "workspace",
      allow_member_invites: Boolean(workspace.settings?.allow_member_invites),
    },
    legacyPersonal: !workspaceSnapshot.exists,
  };
}

export function authorizeWorkspaceAction(
  principal: ApiPrincipal,
  access: WorkspaceAccess,
  scope: PlatformApiScope,
  action: "read" | "write" | "delete"
): void {
  requireScope(principal, scope);
  if (["api_key", "clerk_oauth"].includes(principal.authMethod) && !planHasFeature(access.plan, "api")) {
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
  const ownedSnapshot = await db.collection(WORKSPACES).where("owner_user_id", "==", principal.userId).limit(100).get();
  const membershipSnapshot = await db.collection(WORKSPACE_MEMBERSHIPS).where("user_id", "==", principal.userId).limit(200).get();
  const shared = await db.collection("users").doc(principal.userId).collection("shared_workspaces").limit(100).get();
  const workspaceIds = [...new Set([
    ...ownedSnapshot.docs.map((doc) => doc.id),
    ...membershipSnapshot.docs.filter((doc) => clean(doc.data()?.status || "active", 20) === "active").map((doc) => clean(doc.data()?.workspace_id, 220)),
    ...shared.docs.map((doc) => clean(doc.data()?.workspaceUserId || doc.id, 220)),
  ].filter(Boolean))];
  const memberships = await Promise.all(workspaceIds.map((workspaceId) => resolveWorkspaceAccess(db, principal, workspaceId).catch(() => null)));
  const combined = [own, ...memberships.filter((item): item is WorkspaceAccess => Boolean(item))];
  if(principal.organizationId)combined.push(await resolveWorkspaceAccess(db,principal,principal.organizationId));
  return [...new Map(combined.map((workspace) => [workspace.workspaceId, workspace])).values()];
}

export async function createPersonalApiKey(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  input: { name: unknown; scopes: unknown; expiresAt?: unknown }
): Promise<{ id: string; key: string; prefix: string; name: string; scopes: PlatformApiScope[]; createdAt: string; expiresAt: string | null }> {
  await requirePaidApiAccess(db, principal.userId, principal.clerkUserId);
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
    ...(principal.clerkUserId ? {clerk_user_id:principal.clerkUserId} : {}),
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
  const record={
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
  };
  // Successful polling reads are observable in platform logs without a billed
  // document write. Keep durable mutation and failure audit records.
  if(["GET","HEAD","OPTIONS"].includes(input.method.toUpperCase())&&input.status<400){console.info(JSON.stringify({event:"quantura_api_read",...record}));return;}
  await db.collection(API_AUDIT).doc(input.requestId).set(record);
}
