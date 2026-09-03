import assert from "node:assert/strict";
import test from "node:test";
import {
  assertGrantablePermissions,
  assertGrantableResourceScope,
  permissionsForRole,
  requireWorkspacePermission,
  validateWorkspacePermissions,
  validateWorkspaceResourceScope,
  type WorkspaceAccess,
} from "./apiAccess";

function access(role: WorkspaceAccess["role"], permissions = permissionsForRole(role === "owner" ? "owner" : role)): WorkspaceAccess {
  return {
    workspaceId: "ws_test", role, plan: "research", capabilities: [], permissions,
    resourceScope: { csv: { mode: "all", ids: [] }, forecasts: { mode: "all", ids: [] } },
    ownerUserId: "owner", name: "Test", slug: "test", legacyPersonal: false,
  };
}

test("workspace permission presets are explicit and mutation-safe", () => {
  const viewer = permissionsForRole("viewer");
  assert.ok(viewer.includes("csv.download"));
  assert.ok(!viewer.includes("csv.upload"));
  const analyst = permissionsForRole("analyst");
  assert.ok(analyst.includes("forecast.create"));
  assert.ok(!analyst.includes("csv.move"));
  const editor = permissionsForRole("editor");
  assert.ok(editor.includes("csv.copy"));
  assert.ok(editor.includes("csv.move"));
  assert.ok(!editor.includes("csv.delete"));
  assert.ok(permissionsForRole("owner").includes("workspace.members.remove"));
});

test("custom permissions reject unknown capabilities and deduplicate known values", () => {
  assert.deepEqual(validateWorkspacePermissions(["csv.read", "csv.read", "csv.download"]), ["csv.read", "csv.download"]);
  assert.throws(() => validateWorkspacePermissions(["csv.read", "root.everything"]), /permissions_invalid/);
});

test("selected CSV resource scope prevents access to every other CSV", () => {
  const restricted = {
    ...access("custom", ["workspace.read", "csv.list", "csv.read", "csv.download"]),
    resourceScope: validateWorkspaceResourceScope({ csv: { mode: "selected", ids: ["csv_allowed"] }, forecasts: { mode: "none" } }),
  };
  requireWorkspacePermission(restricted, "csv.download", "csv_allowed");
  assert.throws(() => requireWorkspacePermission(restricted, "csv.download", "csv_blocked"), /resource_forbidden/);
});

test("non-owner administrators cannot grant a capability they do not possess", () => {
  const actor = access("custom", ["workspace.members.update", "csv.read"]);
  assertGrantablePermissions(actor, ["csv.read"]);
  assert.throws(() => assertGrantablePermissions(actor, ["csv.delete"]), /permission_escalation/);
  assert.doesNotThrow(() => assertGrantablePermissions(access("owner"), ["csv.delete"]));
});

test("non-owners cannot expand selected or empty resource scopes", () => {
  const selected = {
    ...access("custom", ["workspace.members.update", "csv.read", "forecast.read"]),
    resourceScope: validateWorkspaceResourceScope({
      csv: { mode: "selected", ids: ["csv_allowed"] },
      forecasts: { mode: "none" },
    }),
  };
  assert.doesNotThrow(() => assertGrantableResourceScope(selected, validateWorkspaceResourceScope({
    csv: { mode: "selected", ids: ["csv_allowed"] },
    forecasts: { mode: "none" },
  })));
  assert.throws(() => assertGrantableResourceScope(selected, validateWorkspaceResourceScope({
    csv: { mode: "all" }, forecasts: { mode: "none" },
  })), /resource_scope_escalation/);
  assert.throws(() => assertGrantableResourceScope(selected, validateWorkspaceResourceScope({
    csv: { mode: "selected", ids: ["csv_other"] }, forecasts: { mode: "none" },
  })), /resource_scope_escalation/);
  assert.throws(() => assertGrantableResourceScope(selected, validateWorkspaceResourceScope({
    csv: { mode: "none" }, forecasts: { mode: "all" },
  })), /resource_scope_escalation/);
});

test("none resource scope denies an otherwise granted permission", () => {
  const denied = {
    ...access("custom", ["csv.list", "csv.read"]),
    resourceScope: validateWorkspaceResourceScope({ csv: { mode: "none" } }),
  };
  assert.throws(() => requireWorkspacePermission(denied, "csv.read", "csv_any"), /resource_forbidden/);
});
