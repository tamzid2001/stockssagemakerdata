import assert from "node:assert/strict";
import test from "node:test";
import { buildOpenApiDocument } from "./openapi";

test("OpenAPI documents workspace and uploaded CSV lifecycle routes", () => {
  const document = buildOpenApiDocument() as any;
  const paths = document.paths || {};
  [
    "/workspaces",
    "/workspaces/{workspace_id}",
    "/workspaces/{workspace_id}/collaborators",
    "/workspaces/{workspace_id}/collaborators/{collaborator_id}",
    "/uploads/csv",
    "/uploads/csv/{csv_id}",
    "/uploads/csv/{csv_id}/download",
    "/uploads/csv/{csv_id}/move",
    "/uploads/csv/{csv_id}/copy",
    "/uploads/csv/bulk-move",
    "/uploads/csv/bulk-copy",
  ].forEach((path) => assert.ok(paths[path], `missing ${path}`));
  assert.equal(paths["/workspaces"].post["x-quantura-scope"], "workspaces:write");
  assert.equal(paths["/uploads/csv"].post["x-quantura-scope"], "datasets:write");
});

test("Mintlify MCP exposes only the approved read-only allowlist", () => {
  const document = buildOpenApiDocument() as any;
  const tools: Array<{ method: string; name: string }> = [];
  Object.values(document.paths || {}).forEach((pathItem: any) => {
    Object.entries(pathItem || {}).forEach(([method, operation]: [string, any]) => {
      if (operation?.["x-mint"]?.mcp?.enabled) tools.push({ method, name: operation["x-mint"].mcp.name });
    });
  });
  assert.deepEqual(tools.sort((a, b) => a.name.localeCompare(b.name)), [
    { method: "get", name: "quantura_get_my_access" },
    { method: "get", name: "quantura_get_uploaded_csv" },
    { method: "get", name: "quantura_list_uploaded_csvs" },
    { method: "get", name: "quantura_list_workspaces" },
  ]);
});

test("every documented endpoint has a descriptive title and a visible success example", () => {
  const document = buildOpenApiDocument() as any;
  const methods = new Set(["get", "post", "put", "patch", "delete"]);
  let operationCount = 0;
  Object.entries(document.paths || {}).forEach(([path, pathItem]: [string, any]) => {
    Object.entries(pathItem || {}).forEach(([method, operation]: [string, any]) => {
      if (!methods.has(method)) return;
      operationCount += 1;
      assert.ok(operation.operationId, `${method.toUpperCase()} ${path} is missing operationId`);
      assert.ok(
        typeof operation.summary === "string" && operation.summary.split(/\s+/).length >= 7,
        `${operation.operationId} needs a clearer, more descriptive endpoint title`,
      );
      const successResponses = Object.entries(operation.responses || {}).filter(([status]) => /^2\d\d$/.test(status));
      assert.ok(successResponses.length > 0, `${operation.operationId} has no documented success response`);
      successResponses.forEach(([status, responseValue]) => {
        const response = responseValue as any;
        const mediaEntries = Object.entries(response.content || {});
        assert.ok(mediaEntries.length > 0, `${operation.operationId} ${status} has no response content`);
        assert.ok(
          mediaEntries.some(([, mediaValue]: [string, any]) => mediaValue?.example !== undefined || mediaValue?.examples !== undefined),
          `${operation.operationId} ${status} has no visible success example`,
        );
      });
    });
  });
  assert.ok(operationCount >= 51, "public API operation inventory unexpectedly shrank");
});
