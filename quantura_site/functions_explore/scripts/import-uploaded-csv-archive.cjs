#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const admin = require("firebase-admin");
const AdmZip = require("adm-zip");
const { PLATFORM_API_SCOPES } = require("../dist/apiAccess.js");
const { normalizePlan } = require("../dist/planEntitlements.js");
const { createManagedUploadedCsv } = require("../dist/uploadedCsv.js");
const { inspectCsvBytes } = require("../dist/uploadedCsv.js");
const { ensurePersonalWorkspace } = require("../dist/workspaces.js");

function required(value, label) {
  const clean = String(value || "").trim();
  if (!clean) throw new Error(`${label}_required`);
  return clean;
}

function storageBucketName() {
  const projectId = String(process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT || "").trim();
  return String(process.env.FIREBASE_STORAGE_BUCKET || process.env.STORAGE_BUCKET || (projectId ? `${projectId}.firebasestorage.app` : ""))
    .trim()
    .replace(/^gs:\/\//i, "")
    .replace(/^\/+|\/+$/g, "");
}

function initializeFirebase() {
  if (admin.apps.length) return;
  const raw = String(process.env.FIREBASE_SERVICE_ACCOUNT_JSON || "").trim();
  const projectId = required(process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT, "GOOGLE_CLOUD_PROJECT");
  const accessToken = String(process.env.GOOGLE_OAUTH_ACCESS_TOKEN || "").trim();
  let credential;
  if (raw && raw !== "[SENSITIVE]") {
    const parsed = JSON.parse(raw);
    credential = admin.credential.cert({
      projectId: parsed.project_id || parsed.projectId,
      clientEmail: parsed.client_email || parsed.clientEmail,
      privateKey: parsed.private_key || parsed.privateKey,
    });
  } else if (accessToken) {
    credential = {
      getAccessToken: async () => ({ access_token: accessToken, expires_in: 3600 }),
    };
  } else {
    throw new Error("firebase_administrative_credential_required");
  }
  const options = { credential, projectId };
  const bucket = storageBucketName();
  if (bucket) options.storageBucket = bucket;
  admin.initializeApp(options);
}

function firestoreValue(value) {
  if (value === null || value === undefined) return { nullValue: null };
  if (typeof value === "string") return { stringValue: value };
  if (typeof value === "boolean") return { booleanValue: value };
  if (typeof value === "number") return Number.isInteger(value) ? { integerValue: String(value) } : { doubleValue: value };
  if (Array.isArray(value)) return { arrayValue: { values: value.map(firestoreValue) } };
  if (typeof value === "object") {
    return { mapValue: { fields: Object.fromEntries(Object.entries(value).map(([key, item]) => [key, firestoreValue(item)])) } };
  }
  return { stringValue: String(value) };
}

function firestoreFields(value) {
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, firestoreValue(item)]));
}

function fromFirestoreValue(value) {
  if (!value || typeof value !== "object") return null;
  if ("stringValue" in value) return value.stringValue;
  if ("integerValue" in value) return Number(value.integerValue);
  if ("doubleValue" in value) return Number(value.doubleValue);
  if ("booleanValue" in value) return Boolean(value.booleanValue);
  if ("nullValue" in value) return null;
  if (value.arrayValue) return (value.arrayValue.values || []).map(fromFirestoreValue);
  if (value.mapValue) return Object.fromEntries(Object.entries(value.mapValue.fields || {}).map(([key, item]) => [key, fromFirestoreValue(item)]));
  return null;
}

function fromFirestoreDocument(document) {
  return Object.fromEntries(Object.entries((document || {}).fields || {}).map(([key, value]) => [key, fromFirestoreValue(value)]));
}

async function googleRequest(url, options = {}, allowStatuses = []) {
  const token = required(process.env.GOOGLE_OAUTH_ACCESS_TOKEN, "GOOGLE_OAUTH_ACCESS_TOKEN");
  const projectId = required(process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT, "GOOGLE_CLOUD_PROJECT");
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      "X-Goog-User-Project": projectId,
      ...(options.headers || {}),
    },
  });
  if (response.ok || allowStatuses.includes(response.status)) return response;
  const payload = await response.json().catch(() => ({}));
  throw new Error(`google_api_${response.status}:${String(payload?.error?.status || payload?.error?.message || "request_failed").slice(0, 180)}`);
}

function archiveEntries(archivePath, expectedCount) {
  const archive = new AdmZip(archivePath);
  const entries = archive.getEntries()
    .filter((entry) => !entry.isDirectory && /\/csv\/.+\.csv$/i.test(entry.entryName))
    .sort((left, right) => left.entryName.localeCompare(right.entryName));
  if (expectedCount > 0 && entries.length !== expectedCount) {
    throw new Error(`archive_csv_count_mismatch:${entries.length}:${expectedCount}`);
  }
  return entries;
}

function inspectArchiveCsvBytes(bytes, filename) {
  try {
    return inspectCsvBytes(bytes, filename, { source_type: "prediction_csv", forecast_kind: "ensemble" });
  } catch (error) {
    if (String(error && error.message ? error.message : error) !== "csv_header_invalid") throw error;
    const text = bytes.toString("utf8").replace(/^\uFEFF/, "");
    const newlineIndex = text.search(/\r?\n/);
    if (newlineIndex < 0) throw error;
    const newlineLength = text.slice(newlineIndex).startsWith("\r\n") ? 2 : 1;
    const header = text.slice(0, newlineIndex).split(",");
    if (!header.some((column) => !column.trim()) || !header.some((column) => column.trim())) throw error;
    const normalizedHeader = header.map((column, index) => column.trim() || `unnamed_column_${index + 1}`);
    const inspectionBytes = Buffer.from(`${normalizedHeader.join(",")}\n${text.slice(newlineIndex + newlineLength)}`, "utf8");
    const metadata = inspectCsvBytes(inspectionBytes, filename, { source_type: "prediction_csv", forecast_kind: "ensemble" });
    return {
      ...metadata,
      size_bytes: bytes.length,
      sha256: crypto.createHash("sha256").update(bytes).digest("hex"),
      metadata_status: "partial",
      metadata_warnings: [
        ...((metadata.metadata_warnings || [])),
        "Original CSV contained unnamed header columns; API metadata uses deterministic unnamed_column_N labels while downloads preserve the original bytes.",
      ],
    };
  }
}

async function importWithGoogleRest(archivePath, userEmail, expectedCount) {
  const projectId = required(process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT, "GOOGLE_CLOUD_PROJECT");
  const bucket = required(storageBucketName(), "FIREBASE_STORAGE_BUCKET");
  const firestoreBase = `https://firestore.googleapis.com/v1/projects/${encodeURIComponent(projectId)}/databases/(default)/documents`;
  const identityResponse = await googleRequest(
    `https://identitytoolkit.googleapis.com/v1/projects/${encodeURIComponent(projectId)}/accounts:lookup`,
    { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email: [userEmail] }) }
  );
  const identity = await identityResponse.json();
  const user = identity.users?.[0];
  const workspaceId = required(user?.localId, "firebase_user");

  const workspaceUrl = `${firestoreBase}/workspaces/${encodeURIComponent(workspaceId)}`;
  const workspaceResponse = await googleRequest(workspaceUrl, {}, [404]);
  if (workspaceResponse.status === 404) {
    const now = new Date().toISOString();
    const createResponse = await googleRequest(
      `${firestoreBase}/workspaces?documentId=${encodeURIComponent(workspaceId)}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fields: firestoreFields({
          id: workspaceId,
          name: "Personal Workspace",
          slug: "personal",
          description: "",
          owner_user_id: workspaceId,
          owner_name: user.displayName || null,
          owner_email: user.email || userEmail,
          created_at: now,
          updated_at: now,
          archived_at: null,
          legacy_personal_id: true,
          settings: { default_csv_visibility: "workspace", allow_member_invites: false },
        }) }),
      },
      [409]
    );
    if (![200, 409].includes(createResponse.status)) throw new Error("workspace_create_failed");
  }

  const queryResponse = await googleRequest(
    `https://firestore.googleapis.com/v1/projects/${encodeURIComponent(projectId)}/databases/(default)/documents:runQuery`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ structuredQuery: {
        from: [{ collectionId: "uploaded_csvs" }],
        where: { fieldFilter: { field: { fieldPath: "workspace_id" }, op: "EQUAL", value: { stringValue: workspaceId } } },
        limit: 5000,
      } }),
    }
  );
  const queryRows = await queryResponse.json();
  const existingHashes = new Set(queryRows.map((row) => fromFirestoreDocument(row.document).sha256).filter(Boolean));
  const entries = archiveEntries(archivePath, expectedCount);
  let created = 0;
  let skipped = 0;
  const failures = [];

  for (const entry of entries) {
    const bytes = entry.getData();
    const hash = crypto.createHash("sha256").update(bytes).digest("hex");
    if (existingHashes.has(hash)) {
      skipped += 1;
      continue;
    }
    const id = `csv_${Date.now().toString(36)}${crypto.randomBytes(10).toString("base64url")}`;
    const filename = path.basename(entry.entryName).replace(/[/\\\0\r\n\t]/g, "-").slice(0, 240);
    const storagePath = `workspace_uploads/${workspaceId}/${id}/${filename}`;
    const objectUrl = `https://storage.googleapis.com/upload/storage/v1/b/${encodeURIComponent(bucket)}/o?uploadType=media&name=${encodeURIComponent(storagePath)}`;
    const documentUrl = `${firestoreBase}/uploaded_csvs/${encodeURIComponent(id)}`;
    try {
      await googleRequest(objectUrl, { method: "POST", headers: { "Content-Type": "text/csv", "Cache-Control": "private, max-age=0, no-store" }, body: bytes });
      const now = new Date().toISOString();
      const metadata = inspectArchiveCsvBytes(bytes, filename);
      const record = {
        id,
        workspace_id: workspaceId,
        storage_path: storagePath,
        uploaded_by_user_id: workspaceId,
        uploaded_at: now,
        updated_at: now,
        copied_from_csv_id: null,
        copied_from_workspace_id: null,
        copied_at: null,
        ...metadata,
        provenance: {
          import_batch_id: "quantura_134_unique_prediction_csvs",
          archive_filename: path.basename(archivePath),
          archive_entry: entry.entryName,
        },
      };
      await googleRequest(`${firestoreBase}/uploaded_csvs?documentId=${encodeURIComponent(id)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fields: firestoreFields(record) }),
      });
      const audit = {
        workspace_id: workspaceId,
        actor_user_id: workspaceId,
        action: "csv_uploaded",
        resource_type: "csv",
        resource_id: id,
        source_workspace_id: null,
        destination_workspace_id: null,
        metadata: { size_bytes: bytes.length, import_batch_id: "quantura_134_unique_prediction_csvs" },
        timestamp: now,
      };
      await googleRequest(`${firestoreBase}/workspace_audit_logs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fields: firestoreFields(audit) }),
      });
      existingHashes.add(hash);
      created += 1;
    } catch (error) {
      await googleRequest(documentUrl, { method: "DELETE" }, [404]).catch(() => undefined);
      await googleRequest(`https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(bucket)}/o/${encodeURIComponent(storagePath)}`, { method: "DELETE" }, [404]).catch(() => undefined);
      failures.push({ entry: entry.entryName, error: String(error && error.message ? error.message : error).slice(0, 300) });
    }
  }

  process.stdout.write(`${JSON.stringify({ workspace_id: workspaceId, candidates: entries.length, created, skipped, failed: failures.length })}\n`);
  if (failures.length) {
    failures.forEach((failure) => process.stderr.write(`${JSON.stringify(failure)}\n`));
    process.exitCode = 1;
  }
}

async function main() {
  const archivePath = path.resolve(required(process.argv[2], "archive_path"));
  const userEmail = required(process.argv[3], "user_email").toLowerCase();
  const expectedCount = Number(process.argv[4] || 0);
  if (!fs.statSync(archivePath).isFile()) throw new Error("archive_not_found");

  const rawServiceAccount = String(process.env.FIREBASE_SERVICE_ACCOUNT_JSON || "").trim();
  if ((!rawServiceAccount || rawServiceAccount === "[SENSITIVE]") && process.env.GOOGLE_OAUTH_ACCESS_TOKEN) {
    await importWithGoogleRest(archivePath, userEmail, expectedCount);
    return;
  }

  initializeFirebase();
  const db = admin.firestore();
  const user = await admin.auth().getUserByEmail(userEmail);
  const profileSnapshot = await db.collection("users").doc(user.uid).get();
  const profile = profileSnapshot.data() || {};
  const principal = {
    userId: user.uid,
    tokenId: null,
    tokenName: "Administrative CSV archive import",
    tokenScopes: [...PLATFORM_API_SCOPES],
    plan: normalizePlan(profile.plan || profile.subscriptionTier || (profile.profile || {}).plan),
    authMethod: "firebase_session",
  };
  const workspaceId = await ensurePersonalWorkspace(db, principal, { name: user.displayName, email: user.email });

  const entries = archiveEntries(archivePath, expectedCount);

  const existingSnapshot = await db.collection("uploaded_csvs").where("workspace_id", "==", workspaceId).limit(5000).get();
  const existingHashes = new Set(existingSnapshot.docs.map((doc) => String(doc.data().sha256 || "")).filter(Boolean));
  let created = 0;
  let skipped = 0;
  const failures = [];

  for (const entry of entries) {
    try {
      const bytes = entry.getData();
      const hash = crypto.createHash("sha256").update(bytes).digest("hex");
      if (existingHashes.has(hash)) {
        skipped += 1;
        continue;
      }
      await createManagedUploadedCsv(
        db,
        principal,
        workspaceId,
        path.basename(entry.entryName),
        bytes,
        { source_type: "prediction_csv", forecast_kind: "ensemble" },
        {
          import_batch_id: "quantura_134_unique_prediction_csvs",
          archive_filename: path.basename(archivePath),
          archive_entry: entry.entryName,
        }
      );
      existingHashes.add(hash);
      created += 1;
    } catch (error) {
      failures.push({ entry: entry.entryName, error: String(error && error.message ? error.message : error).slice(0, 300) });
    }
  }

  const result = { workspace_id: workspaceId, candidates: entries.length, created, skipped, failed: failures.length };
  process.stdout.write(`${JSON.stringify(result)}\n`);
  if (failures.length) {
    failures.forEach((failure) => process.stderr.write(`${JSON.stringify(failure)}\n`));
    process.exitCode = 1;
  }
}

main().catch((error) => {
  process.stderr.write(`CSV archive import failed: ${String(error && error.message ? error.message : error).slice(0, 500)}\n`);
  process.exitCode = 1;
});
