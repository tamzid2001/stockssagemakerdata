import crypto from "node:crypto";
import type admin from "firebase-admin";
import {
  WORKSPACE_PERMISSIONS,
  assertGrantablePermissions,
  assertGrantableResourceScope,
  permissionsForRole,
  requireWorkspacePermission,
  resolveWorkspaceAccess,
  validateWorkspacePermissions,
  validateWorkspaceResourceScope,
  workspaceMembershipId,
  type ApiPrincipal,
  type WorkspaceAccess,
  type WorkspacePermission,
  type WorkspaceRole,
} from "./apiAccess";
import { PLAN_ENTITLEMENTS } from "./planEntitlements";

const WORKSPACES = "workspaces";
const MEMBERSHIPS = "workspace_memberships";
const AUDIT_LOGS = "workspace_audit_logs";
const WORKSPACE_OWNER_COUNTERS = "workspace_owner_counters";
const WORKSPACE_MEMBER_COUNTERS = "workspace_member_counters";

function clean(value: unknown, max = 500): string {
  return String(value ?? "").trim().slice(0, max);
}

function slugify(value: string): string {
  return value.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toLowerCase()
    .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 72) || "workspace";
}

function generatedWorkspaceId(): string {
  const time = Date.now().toString(36).padStart(9, "0");
  return `ws_${time}${crypto.randomBytes(10).toString("base64url")}`;
}

function validRole(value: unknown): WorkspaceRole {
  const role = clean(value || "viewer", 20).toLowerCase();
  if (!["admin", "editor", "analyst", "viewer", "custom"].includes(role)) throw new Error("workspace_role_invalid");
  return role as WorkspaceRole;
}

export async function writeWorkspaceAudit(
  db: FirebaseFirestore.Firestore,
  input: {
    workspaceId: string;
    actorUserId: string;
    action: string;
    resourceType?: string;
    resourceId?: string;
    sourceWorkspaceId?: string;
    destinationWorkspaceId?: string;
    metadata?: Record<string, unknown>;
  }
): Promise<void> {
  const timestamp = new Date().toISOString();
  await db.collection(AUDIT_LOGS).doc().create({
    workspace_id: clean(input.workspaceId, 220),
    actor_user_id: clean(input.actorUserId, 220),
    action: clean(input.action, 80),
    resource_type: clean(input.resourceType, 80) || null,
    resource_id: clean(input.resourceId, 220) || null,
    source_workspace_id: clean(input.sourceWorkspaceId, 220) || null,
    destination_workspace_id: clean(input.destinationWorkspaceId, 220) || null,
    metadata: input.metadata || {},
    timestamp,
  });
}

export async function ensurePersonalWorkspace(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  profile: { name?: string | null; email?: string | null } = {}
): Promise<string> {
  const ref = db.collection(WORKSPACES).doc(principal.userId);
  const now = new Date().toISOString();
  await ref.create({
    id: principal.userId,
    name: "Personal Workspace",
    slug: "personal",
    description: "",
    owner_user_id: principal.userId,
    owner_name: clean(profile.name, 120) || null,
    owner_email: clean(profile.email, 254).toLowerCase() || null,
    created_at: now,
    updated_at: now,
    archived_at: null,
    legacy_personal_id: true,
    settings: { default_csv_visibility: "workspace", allow_member_invites: false },
  }).catch((error: any) => {
    const message = String(error?.message || "").toLowerCase();
    if (Number(error?.code) !== 6 && !message.includes("already exists")) throw error;
  });
  return principal.userId;
}

export async function createWorkspace(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  input: { name: unknown; description?: unknown }
): Promise<WorkspaceAccess> {
  const name = clean(input.name, 80).replace(/\s+/g, " ");
  if (name.length < 2) throw new Error("workspace_name_invalid");
  const description = clean(input.description, 500);
  const owned = await db.collection(WORKSPACES).where("owner_user_id", "==", principal.userId).limit(101).get();
  const limit = Math.max(1, Number(PLAN_ENTITLEMENTS[principal.plan].workspaceLimit || 1));
  const duplicate = owned.docs.some((doc) => clean(doc.data()?.name, 80).toLowerCase() === name.toLowerCase() && !doc.data()?.archived_at);
  if (duplicate) throw new Error("workspace_name_exists");

  const id = generatedWorkspaceId();
  const now = new Date().toISOString();
  const workspaceRef = db.collection(WORKSPACES).doc(id);
  const counterRef = db.collection(WORKSPACE_OWNER_COUNTERS).doc(principal.userId);
  const workspaceRecord = {
    id, name, slug: `${slugify(name)}-${id.slice(-6).toLowerCase()}`, description,
    owner_user_id: principal.userId, created_at: now, updated_at: now, archived_at: null,
    settings: { default_csv_visibility: "workspace", allow_member_invites: false },
  };
  await db.runTransaction(async (transaction) => {
    const counter = await transaction.get(counterRef);
    const current = counter.exists ? Math.max(0, Number(counter.data()?.active_count) || 0) : owned.size;
    if (current >= limit) throw new Error("workspace_limit_reached");
    transaction.create(workspaceRef, workspaceRecord);
    transaction.set(counterRef, { owner_user_id: principal.userId, active_count: current + 1, updated_at: now }, { merge: true });
  });
  await writeWorkspaceAudit(db, { workspaceId: id, actorUserId: principal.userId, action: "workspace_created", resourceType: "workspace", resourceId: id });
  return resolveWorkspaceAccess(db, principal, id);
}

export async function updateWorkspace(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  workspaceId: string,
  input: { name?: unknown; description?: unknown; settings?: unknown }
): Promise<WorkspaceAccess> {
  const access = await resolveWorkspaceAccess(db, principal, workspaceId);
  requireWorkspacePermission(access, "workspace.settings.write");
  const patch: Record<string, unknown> = { updated_at: new Date().toISOString() };
  if (input.name !== undefined) {
    const name = clean(input.name, 80).replace(/\s+/g, " ");
    if (name.length < 2) throw new Error("workspace_name_invalid");
    patch.name = name;
    patch.slug = access.legacyPersonal ? "personal" : `${slugify(name)}-${workspaceId.slice(-6).toLowerCase()}`;
  }
  if (input.description !== undefined) patch.description = clean(input.description, 500);
  if (input.settings !== undefined) {
    const raw = input.settings && typeof input.settings === "object" && !Array.isArray(input.settings) ? input.settings as Record<string, unknown> : {};
    patch.settings = {
      default_csv_visibility: clean(raw.default_csv_visibility || "workspace", 20) === "workspace" ? "workspace" : "private",
      allow_member_invites: Boolean(raw.allow_member_invites),
    };
  }
  await db.collection(WORKSPACES).doc(access.workspaceId).set(patch, { merge: true });
  await writeWorkspaceAudit(db, { workspaceId: access.workspaceId, actorUserId: principal.userId, action: "workspace_updated", resourceType: "workspace", resourceId: access.workspaceId });
  return resolveWorkspaceAccess(db, principal, access.workspaceId);
}

export type PublicMembership = {
  id: string;
  user_id: string;
  email: string | null;
  display_name: string | null;
  role: WorkspaceRole;
  permissions: WorkspacePermission[];
  resource_scope: ReturnType<typeof validateWorkspaceResourceScope>;
  status: string;
  added_at: string | null;
  updated_at: string | null;
};

function publicMembership(id: string, data: Record<string, any>): PublicMembership {
  return {
    id,
    user_id: clean(data.user_id, 220),
    email: clean(data.email, 254).toLowerCase() || null,
    display_name: clean(data.display_name, 120) || null,
    role: clean(data.role, 20) as WorkspaceRole,
    permissions: permissionsForRole(data.role, data.permissions),
    resource_scope: validateWorkspaceResourceScope(data.resource_scope),
    status: clean(data.status || "active", 20),
    added_at: clean(data.added_at, 80) || null,
    updated_at: clean(data.updated_at, 80) || null,
  };
}

export async function listWorkspaceCollaborators(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  workspaceId: string
): Promise<PublicMembership[]> {
  const access = await resolveWorkspaceAccess(db, principal, workspaceId);
  requireWorkspacePermission(access, "workspace.members.read");
  const snapshot = await db.collection(MEMBERSHIPS).where("workspace_id", "==", access.workspaceId).limit(500).get();
  return snapshot.docs.map((doc) => publicMembership(doc.id, (doc.data() || {}) as Record<string, any>));
}

export async function addWorkspaceCollaborator(
  db: FirebaseFirestore.Firestore,
  auth: admin.auth.Auth,
  principal: ApiPrincipal,
  workspaceId: string,
  input: { email: unknown; role?: unknown; permissions?: unknown; resource_scope?: unknown }
): Promise<PublicMembership> {
  const access = await resolveWorkspaceAccess(db, principal, workspaceId);
  requireWorkspacePermission(access, "workspace.members.invite");
  const email = clean(input.email, 254).toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw new Error("collaborator_email_invalid");
  const role = validRole(input.role);
  const permissions = role === "custom" ? validateWorkspacePermissions(input.permissions) : permissionsForRole(role);
  assertGrantablePermissions(access, permissions);
  const target = await auth.getUserByEmail(email).catch(() => null);
  if (!target?.uid) throw new Error("collaborator_invitation_required");
  if (target.uid === access.ownerUserId) throw new Error("workspace_owner_cannot_be_collaborator");
  const seatLimit = Math.max(0, Number(PLAN_ENTITLEMENTS[access.plan].collaboratorSeats || 0));
  const existing = await db.collection(MEMBERSHIPS).where("workspace_id", "==", access.workspaceId).limit(seatLimit + 2).get();
  const ref = db.collection(MEMBERSHIPS).doc(workspaceMembershipId(access.workspaceId, target.uid));
  const resourceScope = validateWorkspaceResourceScope(input.resource_scope);
  assertGrantableResourceScope(access, resourceScope);
  const now = new Date().toISOString();
  const counterRef = db.collection(WORKSPACE_MEMBER_COUNTERS).doc(access.workspaceId);
  let record: Record<string, any> = {};
  let updatedExisting = false;
  await db.runTransaction(async (transaction) => {
    const [prior, counter] = await Promise.all([transaction.get(ref), transaction.get(counterRef)]);
    updatedExisting = prior.exists;
    if (updatedExisting) requireWorkspacePermission(access, "workspace.members.update");
    const current = counter.exists ? Math.max(0, Number(counter.data()?.active_count) || 0) : existing.size;
    if (!updatedExisting && current >= seatLimit) throw new Error("workspace_seat_limit_reached");
    record = {
      workspace_id: access.workspaceId, user_id: target.uid, email,
      display_name: clean(target.displayName, 120) || null, role, permissions,
      resource_scope: resourceScope,
      status: "active", added_at: clean(prior.data()?.added_at, 80) || now, updated_at: now,
      added_by_user_id: principal.userId,
    };
    transaction.set(ref, record, { merge: false });
    if (!updatedExisting) {
      transaction.set(counterRef, { workspace_id: access.workspaceId, active_count: current + 1, updated_at: now }, { merge: true });
    }
  });
  await writeWorkspaceAudit(db, { workspaceId: access.workspaceId, actorUserId: principal.userId, action: updatedExisting ? "collaborator_permissions_updated" : "collaborator_added", resourceType: "membership", resourceId: ref.id, metadata: { role } });
  return publicMembership(ref.id, record);
}

async function resolveMembershipRef(
  db: FirebaseFirestore.Firestore,
  workspaceId: string,
  collaboratorId: string
): Promise<FirebaseFirestore.DocumentReference> {
  const direct = db.collection(MEMBERSHIPS).doc(clean(collaboratorId, 220));
  const directSnapshot = await direct.get();
  if (directSnapshot.exists && clean(directSnapshot.data()?.workspace_id, 220) === workspaceId) return direct;
  return db.collection(MEMBERSHIPS).doc(workspaceMembershipId(workspaceId, clean(collaboratorId, 220)));
}

export async function updateWorkspaceCollaborator(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  workspaceId: string,
  collaboratorId: string,
  input: { role?: unknown; permissions?: unknown; resource_scope?: unknown; status?: unknown }
): Promise<PublicMembership> {
  const access = await resolveWorkspaceAccess(db, principal, workspaceId);
  requireWorkspacePermission(access, "workspace.members.update");
  const ref = await resolveMembershipRef(db, access.workspaceId, collaboratorId);
  const snapshot = await ref.get();
  if (!snapshot.exists) throw new Error("collaborator_not_found");
  const prior = (snapshot.data() || {}) as Record<string, any>;
  if (clean(prior.user_id, 220) === access.ownerUserId || clean(prior.role, 20) === "owner") throw new Error("workspace_owner_protected");
  const role = input.role === undefined ? validRole(prior.role) : validRole(input.role);
  const permissions = role === "custom"
    ? validateWorkspacePermissions(input.permissions === undefined ? prior.permissions : input.permissions)
    : permissionsForRole(role);
  assertGrantablePermissions(access, permissions);
  const resourceScope = input.resource_scope === undefined
    ? validateWorkspaceResourceScope(prior.resource_scope)
    : validateWorkspaceResourceScope(input.resource_scope);
  assertGrantableResourceScope(access, resourceScope);
  const status = input.status === undefined ? clean(prior.status || "active", 20) : clean(input.status, 20);
  if (!["active", "removed"].includes(status)) throw new Error("collaborator_status_invalid");
  const record = {
    ...prior, role, permissions,
    resource_scope: resourceScope,
    status, updated_at: new Date().toISOString(), updated_by_user_id: principal.userId,
  };
  await ref.set(record, { merge: false });
  await writeWorkspaceAudit(db, { workspaceId: access.workspaceId, actorUserId: principal.userId, action: "collaborator_permissions_updated", resourceType: "membership", resourceId: ref.id, metadata: { role, status } });
  return publicMembership(ref.id, record);
}

export async function removeWorkspaceCollaborator(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  workspaceId: string,
  collaboratorId: string
): Promise<{ removed: true; id: string; user_id: string }> {
  const access = await resolveWorkspaceAccess(db, principal, workspaceId);
  requireWorkspacePermission(access, "workspace.members.remove");
  const ref = await resolveMembershipRef(db, access.workspaceId, collaboratorId);
  const existing = await db.collection(MEMBERSHIPS).where("workspace_id", "==", access.workspaceId).limit(501).get();
  const counterRef = db.collection(WORKSPACE_MEMBER_COUNTERS).doc(access.workspaceId);
  let userId = "";
  await db.runTransaction(async (transaction) => {
    const [snapshot, counter] = await Promise.all([transaction.get(ref), transaction.get(counterRef)]);
    if (!snapshot.exists) throw new Error("collaborator_not_found");
    const data = (snapshot.data() || {}) as Record<string, any>;
    userId = clean(data.user_id, 220);
    if (!userId || userId === access.ownerUserId || clean(data.role, 20) === "owner") throw new Error("workspace_owner_protected");
    const current = counter.exists ? Math.max(0, Number(counter.data()?.active_count) || 0) : existing.size;
    transaction.delete(ref);
    transaction.set(counterRef, { workspace_id: access.workspaceId, active_count: Math.max(0, current - 1), updated_at: new Date().toISOString() }, { merge: true });
  });
  await writeWorkspaceAudit(db, { workspaceId: access.workspaceId, actorUserId: principal.userId, action: "collaborator_removed", resourceType: "membership", resourceId: ref.id, metadata: { user_id: userId } });
  return { removed: true, id: ref.id, user_id: userId };
}

export async function listWorkspaceAudit(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  workspaceId: string,
  limitValue: unknown
): Promise<Record<string, unknown>[]> {
  const access = await resolveWorkspaceAccess(db, principal, workspaceId);
  requireWorkspacePermission(access, "workspace.settings.read");
  const limit = Math.min(Math.max(Number(limitValue) || 50, 1), 200);
  const snapshot = await db.collection(AUDIT_LOGS).where("workspace_id", "==", access.workspaceId).orderBy("timestamp", "desc").limit(limit).get();
  return snapshot.docs.map((doc) => ({ id: doc.id, ...(doc.data() || {}) }));
}

export function publicWorkspace(access: WorkspaceAccess): Record<string, unknown> {
  return {
    id: access.workspaceId,
    name: access.name,
    slug: access.slug,
    description: access.description || "",
    created_at: access.createdAt || null,
    updated_at: access.updatedAt || null,
    settings: access.settings || { default_csv_visibility: "workspace", allow_member_invites: false },
    owner_user_id: access.ownerUserId,
    role: access.role,
    permissions: access.permissions,
    resource_scope: access.resourceScope,
    capabilities: access.capabilities,
    legacy_personal: access.legacyPersonal,
  };
}

export const ALL_WORKSPACE_PERMISSIONS = [...WORKSPACE_PERMISSIONS];
