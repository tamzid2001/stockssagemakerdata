import assert from "node:assert/strict";
import test from "node:test";
import {
  authenticatePlatformRequest,
  authorizeWorkspaceAction,
  generatePlatformApiKey,
  hashPlatformApiKey,
  resolveWorkspaceAccess,
  validatePlatformScopes,
  type ApiPrincipal,
  permissionsForRole,
} from "./apiAccess";

type RecordValue = Record<string, any>;

class FakeDocument {
  constructor(private readonly records: Map<string, RecordValue>, public readonly path: string) {}
  get id(): string { return this.path.split("/").at(-1) || ""; }
  get ref(): FakeDocument { return this; }
  async get(): Promise<any> {
    const value = this.records.get(this.path);
    return { exists: Boolean(value), id: this.id, data: () => value, ref: this };
  }
  collection(name: string): FakeCollection { return new FakeCollection(this.records, `${this.path}/${name}`); }
  async set(value: RecordValue, options?: { merge?: boolean }): Promise<void> {
    this.records.set(this.path, options?.merge ? { ...(this.records.get(this.path) || {}), ...value } : value);
  }
}

class FakeCollection {
  constructor(private readonly records: Map<string, RecordValue>, private readonly path: string) {}
  doc(id: string): FakeDocument { return new FakeDocument(this.records, `${this.path}/${id}`); }
}

class FakeDb {
  constructor(public readonly records: Map<string, RecordValue>) {}
  collection(name: string): FakeCollection { return new FakeCollection(this.records, name); }
}

function principal(userId = "viewer"): ApiPrincipal {
  return {
    userId,
    tokenId: "token",
    tokenName: "Test",
    tokenScopes: ["account:read", "workspaces:read", "forecasts:read", "forecasts:write"],
    plan: "free",
    authMethod: "api_key",
  };
}

test("API keys use a stable HMAC identifier without storing the raw key", () => {
  const generated = generatePlatformApiKey();
  assert.match(generated.rawKey, /^qnt_live_[A-Za-z0-9_-]{40,}$/);
  const first = hashPlatformApiKey(generated.rawKey, "test-only-pepper-that-is-longer-than-thirty-two-characters");
  const second = hashPlatformApiKey(generated.rawKey, "test-only-pepper-that-is-longer-than-thirty-two-characters");
  assert.equal(first, second);
  assert.equal(first.length, 64);
  assert.ok(!first.includes(generated.rawKey));
  assert.throws(() => hashPlatformApiKey(generated.rawKey, "short"), /pepper_not_configured/);
});

test("scope validation rejects unknown or duplicate scope input", () => {
  assert.deepEqual(validatePlatformScopes(["account:read", "forecasts:read"]), ["account:read", "forecasts:read"]);
  assert.throws(() => validatePlatformScopes(["account:read", "unknown:write"]), /invalid_scopes/);
  assert.throws(() => validatePlatformScopes(["account:read", "account:read"]), /invalid_scopes/);
});

test("viewer and editor permissions remain workspace-scoped", () => {
  const token = principal();
  const viewerAccess = {
    workspaceId: "owner", role: "viewer" as const, plan: "research" as const, capabilities: [],
    permissions: permissionsForRole("viewer"), resourceScope: { csv: { mode: "all" as const, ids: [] }, forecasts: { mode: "all" as const, ids: [] } },
    ownerUserId: "owner", name: "Owner", slug: "owner", legacyPersonal: true,
  };
  authorizeWorkspaceAction(token, viewerAccess, "forecasts:read", "read");
  assert.throws(() => authorizeWorkspaceAction(token, viewerAccess, "forecasts:write", "write"), /read_only/);
  const editorAccess = { ...viewerAccess, role: "editor" as const, permissions: permissionsForRole("editor") };
  authorizeWorkspaceAction(token, editorAccess, "forecasts:write", "write");
  const analystAccess = { ...viewerAccess, role: "analyst" as const, permissions: permissionsForRole("analyst") };
  authorizeWorkspaceAction(token, analystAccess, "forecasts:write", "write");
  const ownAccess = { ...viewerAccess, workspaceId: token.userId, role: "owner" as const, plan: token.plan };
  assert.throws(() => authorizeWorkspaceAction(token, ownAccess, "forecasts:write", "write"), /plan_upgrade_required/);
  assert.throws(() => authorizeWorkspaceAction(token, ownAccess, "forecasts:read", "read"), /plan_upgrade_required/);

  const browserSession = { ...token, authMethod: "firebase_session" as const };
  authorizeWorkspaceAction(browserSession, ownAccess, "forecasts:write", "write");
});

test("a free collaborator API key keeps shared read access but not shared writes", () => {
  const token = principal();
  const sharedViewer = {
    workspaceId: "owner", role: "viewer" as const, plan: "free" as const, capabilities: [],
    permissions: permissionsForRole("viewer"), resourceScope: { csv: { mode: "all" as const, ids: [] }, forecasts: { mode: "all" as const, ids: [] } },
    ownerUserId: "owner", name: "Owner", slug: "owner", legacyPersonal: true,
  };
  authorizeWorkspaceAction(token, sharedViewer, "forecasts:read", "read");
  assert.throws(() => authorizeWorkspaceAction(token, sharedViewer, "forecasts:write", "write"), /plan_upgrade_required/);
});

test("workspace membership and role are evaluated dynamically on every request", async () => {
  const records = new Map<string, RecordValue>([
    ["users/viewer", { plan: "free" }],
    ["users/owner", { plan: "research" }],
    ["users/owner/collaborators/viewer", { role: "viewer" }],
  ]);
  const db = new FakeDb(records) as any;
  const token = principal();
  assert.equal((await resolveWorkspaceAccess(db, token, "owner")).role, "viewer");
  records.set("users/owner/collaborators/viewer", { role: "editor" });
  assert.equal((await resolveWorkspaceAccess(db, token, "owner")).role, "editor");
  records.delete("users/owner/collaborators/viewer");
  await assert.rejects(resolveWorkspaceAccess(db, token, "owner"), /workspace_forbidden/);
  assert.equal((await resolveWorkspaceAccess(db, token, "viewer")).role, "owner");
});

test("revoked and expired keys immediately fail authentication", async () => {
  const priorPepper = process.env.QUANTURA_API_KEY_PEPPER;
  process.env.QUANTURA_API_KEY_PEPPER = "test-only-pepper-that-is-longer-than-thirty-two-characters";
  try {
    const generated = generatePlatformApiKey();
    const tokenId = hashPlatformApiKey(generated.rawKey);
    const records = new Map<string, RecordValue>([
      ["users/user-1", { plan: "quant" }],
      [`quantura_api_keys/${tokenId}`, { user_id: "user-1", name: "CI", scopes: ["account:read"], revoked_at: null, expires_at: null }],
    ]);
    const db = new FakeDb(records) as any;
    const req = { headers: { authorization: `Bearer ${generated.rawKey}` } } as any;
    const auth = { verifyIdToken: async () => null } as any;
    assert.equal((await authenticatePlatformRequest(req, { db, auth })).userId, "user-1");
    records.set(`quantura_api_keys/${tokenId}`, { ...records.get(`quantura_api_keys/${tokenId}`), revoked_at: new Date().toISOString() });
    await assert.rejects(authenticatePlatformRequest(req, { db, auth }), /api_key_revoked/);
    records.set(`quantura_api_keys/${tokenId}`, { user_id: "user-1", name: "CI", scopes: ["account:read"], revoked_at: null, expires_at: "2020-01-01T00:00:00.000Z" });
    await assert.rejects(authenticatePlatformRequest(req, { db, auth }), /api_key_expired/);
  } finally {
    if (priorPepper === undefined) delete process.env.QUANTURA_API_KEY_PEPPER;
    else process.env.QUANTURA_API_KEY_PEPPER = priorPepper;
  }
});
