import crypto from "node:crypto";
import type { Request } from "express";
import type admin from "firebase-admin";
import { normalizePlan, PLAN_ENTITLEMENTS, planHasFeature, type PlanKey } from "./planEntitlements";

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
  authMethod: "api_key" | "firebase_session";
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
  const workspaceSnapshot = await db.collection(WORKSPACES).doc(workspaceId).get();
  const workspace = (workspaceSnapshot.data() || {}) as Record<string, any>;
  const ownerUserId = clean(workspace.owner_user_id, 220);
  if (workspaceSnapshot.exists && workspace.archived_at) throw new Error("workspace_archived");
  if ((workspaceSnapshot.exists && ownerUserId === principal.userId) || (!workspaceSnapshot.exists && workspaceId === principal.userId)) {
    return {
      workspaceId,
      role: "owner",
      plan: principal.plan,
      capabilities: PLAN_ENTITLEMENTS[principal.plan].features,
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
  return [...new Map(combined.map((workspace) => [workspace.workspaceId, workspace])).values()];
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
