import crypto from "node:crypto";
import admin from "firebase-admin";
import {
  authorizeWorkspaceAction,
  listAccessibleWorkspaces,
  requireScope,
  requireWorkspacePermission,
  resolveWorkspaceAccess,
  type ApiPrincipal,
  type PlatformApiScope,
  type WorkspaceAccess,
  type WorkspacePermission,
} from "./apiAccess";
import { writeWorkspaceAudit } from "./workspaces";

const UPLOADS = "uploaded_csvs";
const MAX_CSV_BYTES = 25 * 1024 * 1024;
const MAX_CSV_ROWS = 250_000;
export const MAX_BULK_CSV_OPERATION = 100;

type CsvMetadataHints = {
  ticker?: unknown;
  asset_class?: unknown;
  source_type?: unknown;
  forecast_kind?: unknown;
  display_filename?: unknown;
};

type UploadedCsvRecord = Record<string, any> & {
  id: string;
  workspace_id: string;
  storage_path: string;
};

function clean(value: unknown, max = 500): string { return String(value ?? "").trim().slice(0, max); }
function plain(value: unknown): Record<string, any> { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, any> : {}; }
function generatedCsvId(): string { return `csv_${Date.now().toString(36)}${crypto.randomBytes(10).toString("base64url")}`; }
function safeFileName(value: unknown): string {
  const base = clean(value, 240).replace(/[/\\\0]/g, "-").replace(/[\r\n\t]/g, " ").replace(/\s+/g, " ");
  const withExtension = base.toLowerCase().endsWith(".csv") ? base : `${base || "dataset"}.csv`;
  return withExtension.slice(0, 240);
}
function safePathPart(value: unknown): string { return clean(value, 240).replace(/[^A-Za-z0-9._-]/g, "-").replace(/-+/g, "-").replace(/^[-.]+|[-.]+$/g, "") || "file"; }
function bucket() { return admin.storage().bucket(); }

export function isAllowedLegacyCsvStoragePath(pathValue: unknown, ownerUserId: unknown, runId: unknown): boolean {
  const path = clean(pathValue, 1000).replace(/^\/+/, "");
  const owner = safePathPart(ownerUserId);
  const run = safePathPart(runId);
  if (!path || owner === "file" || run === "file") return false;
  return [
    `forecast_reports/${owner}/foundry/${run}/`,
    `predictions/${owner}/foundry/${run}/`,
  ].some((prefix) => path.startsWith(prefix) && path.length > prefix.length);
}

export function isAllowedUserCsvImportStoragePath(pathValue: unknown, userId: unknown): boolean {
  const path = clean(pathValue, 1000).replace(/^\/+/, "");
  const owner = safePathPart(userId);
  if (!path || owner === "file" || path.includes("..") || !path.toLowerCase().endsWith(".csv")) return false;
  return [
    `forecast_reports/${owner}/`,
    `predictions/${owner}/`,
  ].some((prefix) => path.startsWith(prefix) && path.length > prefix.length);
}

function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let quoted = false;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (quoted) {
      if (char === '"' && text[index + 1] === '"') { field += '"'; index += 1; }
      else if (char === '"') quoted = false;
      else field += char;
      continue;
    }
    if (char === '"') quoted = true;
    else if (char === ",") { row.push(field); field = ""; }
    else if (char === "\n") { row.push(field.replace(/\r$/, "")); rows.push(row); row = []; field = ""; }
    else field += char;
    if (rows.length > MAX_CSV_ROWS + 1) throw new Error("csv_row_limit_exceeded");
  }
  if (quoted) throw new Error("csv_unclosed_quote");
  if (field.length || row.length) { row.push(field.replace(/\r$/, "")); rows.push(row); }
  while (rows.length && rows.at(-1)?.every((cell) => !cell.trim())) rows.pop();
  return rows;
}

function isoTimestamp(value: unknown): string | null {
  const raw = clean(value, 100);
  if (!raw || !/^\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[T\s].*)?$/.test(raw)) return null;
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}

function inferGranularity(values: string[]): string | null {
  const times = values.map((value) => Date.parse(value)).filter(Number.isFinite).sort((a, b) => a - b);
  const deltas = times.slice(1).map((value, index) => value - times[index]).filter((value) => value > 0);
  if (!deltas.length) return null;
  const median = [...deltas].sort((a, b) => a - b)[Math.floor(deltas.length / 2)];
  const minute = 60_000;
  if (median <= 1.5 * minute) return "1m";
  if (median <= 7.5 * minute) return "5m";
  if (median <= 22.5 * minute) return "15m";
  if (median <= 45 * minute) return "30m";
  if (median <= 90 * minute) return "1h";
  if (median <= 36 * 60 * minute) return "1d";
  if (median <= 10 * 24 * 60 * minute) return "1w";
  return null;
}

export function inspectCsvBytes(bytes: Buffer, originalFilename: string, hints: CsvMetadataHints = {}): Record<string, unknown> {
  if (!bytes.length) throw new Error("csv_empty");
  if (bytes.length > MAX_CSV_BYTES) throw new Error("csv_size_limit_exceeded");
  const text = bytes.toString("utf8").replace(/^\uFEFF/, "");
  const rows = parseCsv(text);
  if (rows.length < 2) throw new Error("csv_requires_header_and_rows");
  const columns = rows[0].map((column) => clean(column, 160));
  if (!columns.length || columns.some((column) => !column)) throw new Error("csv_header_invalid");
  if (new Set(columns.map((column) => column.toLowerCase())).size !== columns.length) throw new Error("csv_duplicate_columns");
  const dataRows = rows.slice(1);
  const dateIndex = columns.findIndex((column) => /^(date|datetime|timestamp|time|ds)$/i.test(column));
  const parsedDates = dateIndex >= 0 ? dataRows.map((row) => isoTimestamp(row[dateIndex])).filter((value): value is string => Boolean(value)) : [];
  const unambiguousDates = dateIndex >= 0 && parsedDates.length === dataRows.filter((row) => clean(row[dateIndex])).length;
  const quantileColumns = columns.filter((column) => /^(?:p|q)(?:0?(?:1|5)|10|20|25|30|40|50|60|70|75|80|90|95|99)$/i.test(column));
  const populated = dataRows.reduce((sum, row) => sum + columns.reduce((count, _column, index) => count + (clean(row[index]).length ? 1 : 0), 0), 0);
  const filenameTicker = safeFileName(originalFilename).match(/(?:^|__)([A-Z][A-Z0-9.-]{0,11})(?:__|[_.-])/i)?.[1]?.toUpperCase() || "";
  const hintedTicker = clean(hints.ticker, 16).toUpperCase();
  const ticker = /^[A-Z][A-Z0-9.-]{0,11}$/.test(hintedTicker) ? hintedTicker : filenameTicker && filenameTicker !== "UNLABELED" ? filenameTicker : null;
  const warnings: string[] = [];
  if (dateIndex >= 0 && !unambiguousDates) warnings.push("Some timestamps were missing, malformed, or ambiguous; absolute date coverage was not inferred.");
  return {
    original_filename: safeFileName(originalFilename),
    display_filename: safeFileName(hints.display_filename || originalFilename),
    content_type: "text/csv",
    size_bytes: bytes.length,
    sha256: crypto.createHash("sha256").update(bytes).digest("hex"),
    ticker,
    asset_class: clean(hints.asset_class, 80) || null,
    source_type: clean(hints.source_type, 80) || "prediction_csv",
    forecast_brand: quantileColumns.length ? "Quantura Forecast" : null,
    forecast_kind: clean(hints.forecast_kind, 80) || null,
    row_count: dataRows.length,
    column_count: columns.length,
    data_cell_count: dataRows.length * columns.length,
    populated_data_cell_count: populated,
    columns,
    date_column: dateIndex >= 0 ? columns[dateIndex] : null,
    first_timestamp: unambiguousDates && parsedDates.length ? [...parsedDates].sort()[0] : null,
    last_timestamp: unambiguousDates && parsedDates.length ? [...parsedDates].sort().at(-1) || null : null,
    inferred_granularity: unambiguousDates ? inferGranularity(parsedDates) : null,
    quantile_columns: quantileColumns,
    metadata_status: warnings.length ? "partial" : "complete",
    metadata_warnings: warnings,
  };
}

function toIso(value: any): string | null {
  if (!value) return null;
  if (typeof value === "string") return value;
  if (typeof value.toDate === "function") return value.toDate().toISOString();
  if (value instanceof Date) return value.toISOString();
  return null;
}

export function publicUploadedCsv(record: UploadedCsvRecord): Record<string, unknown> {
  const blocked = new Set(["storage_path", "legacy_file", "deleted_at"]);
  const output = Object.fromEntries(Object.entries(record).filter(([key]) => !blocked.has(key)));
  output.uploaded_at = toIso(record.uploaded_at) || clean(record.uploaded_at, 80) || null;
  output.updated_at = toIso(record.updated_at) || clean(record.updated_at, 80) || null;
  output.download_endpoint = `/api/v1/uploads/csv/${encodeURIComponent(record.id)}/download`;
  return output;
}

export async function createManagedUploadedCsv(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  workspaceId: string,
  filename: string,
  bytes: Buffer,
  hints: CsvMetadataHints = {},
  provenance: Record<string, unknown> = {}
): Promise<Record<string, unknown>> {
  const access = await resolveWorkspaceAccess(db, principal, workspaceId);
  requireScope(principal, "datasets:write");
  if (principal.authMethod === "api_key") authorizeWorkspaceAction(principal, access, "datasets:write", "write");
  requireWorkspacePermission(access, "csv.upload");
  const metadata = inspectCsvBytes(bytes, filename, hints);
  const id = generatedCsvId();
  const storagePath = `workspace_uploads/${safePathPart(access.workspaceId)}/${id}/${safePathPart(metadata.original_filename)}`;
  await bucket().file(storagePath).save(bytes, { resumable: false, contentType: "text/csv", metadata: { cacheControl: "private, max-age=0, no-store" } });
  const now = new Date().toISOString();
  const record: UploadedCsvRecord = {
    id, workspace_id: access.workspaceId, storage_path: storagePath,
    uploaded_by_user_id: principal.userId, uploaded_at: now, updated_at: now,
    copied_from_csv_id: null, copied_from_workspace_id: null, copied_at: null,
    ...metadata, provenance,
  };
  try {
    await db.collection(UPLOADS).doc(id).create(record);
  } catch (error) {
    await bucket().file(storagePath).delete({ ignoreNotFound: true }).catch(() => undefined);
    throw error;
  }
  await writeWorkspaceAudit(db, { workspaceId: access.workspaceId, actorUserId: principal.userId, action: "csv_uploaded", resourceType: "csv", resourceId: id, metadata: { size_bytes: bytes.length } });
  return publicUploadedCsv(record);
}

function authorizeCsv(principal: ApiPrincipal, access: WorkspaceAccess, apiScope: PlatformApiScope, action: "read" | "write" | "delete", permission: WorkspacePermission, resourceId?: string): void {
  requireScope(principal, apiScope);
  if (principal.authMethod === "api_key") authorizeWorkspaceAction(principal, access, apiScope, action);
  requireWorkspacePermission(access, permission, resourceId);
}

async function legacyCsvRecord(db: FirebaseFirestore.Firestore, workspaceId: string, runId: string): Promise<UploadedCsvRecord | null> {
  const snapshot = await db.collection("autopilot_requests").doc(runId).get();
  if (!snapshot.exists) return null;
  const data = plain(snapshot.data());
  const effectiveWorkspace = clean(data.workspaceId || data.userId, 220);
  if (effectiveWorkspace !== workspaceId || data.csv_api_hidden === true) return null;
  const file = plain(plain(data.files).uploadedCsv || plain(data.files).predictionsCsv);
  if (!Object.keys(file).length) return null;
  const ownerUserId = clean(data.userId, 220);
  const artifactStore = clean(file.artifactStore, 40);
  const firestoreKey = clean(file.firestoreKey || file.fileKey, 80);
  const storagePath = clean(file.storagePath, 1000).replace(/^\/+/, "");
  if (artifactStore === "firestore") {
    if (!/^(uploadedCsv|predictionsCsv)$/.test(firestoreKey)) return null;
  } else if (!isAllowedLegacyCsvStoragePath(storagePath, ownerUserId, runId)) {
    return null;
  }
  const dataset = plain(data.dataset);
  const columns = Array.isArray(dataset.columns) ? dataset.columns.map((value) => clean(value, 160)) : [];
  return {
    id: `legacy_${runId}`,
    workspace_id: workspaceId,
    storage_path: storagePath,
    legacy_file: { run_id: runId, owner_user_id: ownerUserId, artifact_store: artifactStore, firestore_key: firestoreKey },
    original_filename: safeFileName(file.fileName || data.title || `${runId}.csv`),
    display_filename: safeFileName(data.displayFilename || data.title || file.fileName || `${runId}.csv`),
    content_type: "text/csv",
    size_bytes: Number(file.sizeBytes) || null,
    sha256: clean(file.sha256, 128) || null,
    uploaded_by_user_id: clean(data.userId, 220) || null,
    uploaded_at: toIso(data.createdAt), updated_at: toIso(data.updatedAt),
    ticker: clean(dataset.ticker || data.ticker, 16) || null,
    asset_class: clean(dataset.assetClass, 80) || null,
    source_type: clean(data.sourceType, 80) || "prediction_csv",
    forecast_brand: columns.some((column) => /^(p|q)\d+/i.test(column)) ? "Quantura Forecast" : null,
    forecast_kind: null,
    row_count: Number(dataset.rowCount) || null,
    column_count: columns.length || null,
    data_cell_count: Number(dataset.rowCount) && columns.length ? Number(dataset.rowCount) * columns.length : null,
    populated_data_cell_count: null,
    columns, date_column: clean(dataset.sourceTimeColumn, 160) || null,
    first_timestamp: null, last_timestamp: null,
    inferred_granularity: clean(dataset.interval, 40) || null,
    quantile_columns: columns.filter((column) => /^(p|q)\d+/i.test(column)),
    metadata_status: "partial", metadata_warnings: ["Legacy upload metadata is preserved; fields not present at upload time remain null."],
  };
}

async function legacyRecordsForWorkspace(db: FirebaseFirestore.Firestore, workspaceId: string): Promise<UploadedCsvRecord[]> {
  const snapshots = await Promise.all([
    db.collection("autopilot_requests").where("workspaceId", "==", workspaceId).limit(500).get().catch(() => null),
    db.collection("autopilot_requests").where("userId", "==", workspaceId).limit(500).get().catch(() => null),
  ]);
  const ids = [...new Set(snapshots.flatMap((snapshot) => snapshot?.docs.map((doc) => doc.id) || []))];
  const records = await Promise.all(ids.map((id) => legacyCsvRecord(db, workspaceId, id)));
  return records.filter((record): record is UploadedCsvRecord => Boolean(record));
}

async function managedRecordsForWorkspace(db: FirebaseFirestore.Firestore, workspaceId: string): Promise<UploadedCsvRecord[]> {
  const snapshot = await db.collection(UPLOADS).where("workspace_id", "==", workspaceId).limit(1000).get();
  return snapshot.docs.map((doc) => ({ id: doc.id, ...(doc.data() || {}) } as UploadedCsvRecord)).filter((record) => !record.deleted_at);
}

export async function listAuthorizedCsvs(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  workspaceIdValue?: unknown
): Promise<{ records: UploadedCsvRecord[]; accesses: WorkspaceAccess[] }> {
  const accesses = workspaceIdValue
    ? [await resolveWorkspaceAccess(db, principal, workspaceIdValue)]
    : await listAccessibleWorkspaces(db, principal);
  const records: UploadedCsvRecord[] = [];
  for (const access of accesses) {
    authorizeCsv(principal, access, "datasets:read", "read", "csv.list");
    const [managed, legacy] = await Promise.all([managedRecordsForWorkspace(db, access.workspaceId), legacyRecordsForWorkspace(db, access.workspaceId)]);
    const scope = access.resourceScope.csv;
    const visible = [...managed, ...legacy].filter((record) => scope.mode === "all" || (scope.mode === "selected" && scope.ids.includes(record.id)));
    records.push(...visible);
  }
  records.sort((a, b) => String(b.uploaded_at || "").localeCompare(String(a.uploaded_at || "")) || a.id.localeCompare(b.id));
  return { records, accesses };
}

export async function findAuthorizedCsv(
  db: FirebaseFirestore.Firestore,
  principal: ApiPrincipal,
  csvIdValue: unknown,
  permission: WorkspacePermission,
  apiScope: PlatformApiScope = "datasets:read",
  workspaceIdValue?: unknown
): Promise<{ record: UploadedCsvRecord; access: WorkspaceAccess }> {
  const csvId = clean(csvIdValue, 220);
  if (!/^csv_[A-Za-z0-9_-]+$/.test(csvId) && !/^legacy_[A-Za-z0-9_-]+$/.test(csvId)) throw new Error("csv_not_found");
  let record: UploadedCsvRecord | null = null;
  if (csvId.startsWith("legacy_")) {
    const workspaceId = clean(workspaceIdValue, 220);
    if (!workspaceId) {
      const accesses = await listAccessibleWorkspaces(db, principal);
      for (const candidate of accesses) {
        record = await legacyCsvRecord(db, candidate.workspaceId, csvId.slice(7));
        if (record) break;
      }
    } else record = await legacyCsvRecord(db, workspaceId, csvId.slice(7));
  } else {
    const snapshot = await db.collection(UPLOADS).doc(csvId).get();
    if (snapshot.exists && !snapshot.data()?.deleted_at) record = { id: snapshot.id, ...(snapshot.data() || {}) } as UploadedCsvRecord;
  }
  if (!record) throw new Error("csv_not_found");
  if (workspaceIdValue && clean(workspaceIdValue, 220) !== record.workspace_id) throw new Error("csv_not_found");
  const access = await resolveWorkspaceAccess(db, principal, record.workspace_id);
  const action = permission === "csv.delete" ? "delete" : permission === "csv.read" || permission === "csv.download" || permission === "csv.list" ? "read" : "write";
  authorizeCsv(principal, access, apiScope, action, permission, record.id);
  return { record, access };
}

export async function readUploadedCsvBytes(db: FirebaseFirestore.Firestore, record: UploadedCsvRecord, maxBytes = MAX_CSV_BYTES): Promise<Buffer> {
  const byteLimit = Math.min(Math.max(Math.floor(Number(maxBytes) || MAX_CSV_BYTES), 1), 250 * 1024 * 1024);
  const legacy = plain(record.legacy_file);
  if (clean(legacy.artifact_store, 40) === "firestore") {
    const runId = clean(legacy.run_id, 220);
    const key = clean(legacy.firestore_key, 80);
    if (!/^(uploadedCsv|predictionsCsv)$/.test(key)) throw new Error("csv_file_not_found");
    const artifact = await db.collection("autopilot_requests").doc(runId).collection("text_artifacts").doc(key).get();
    if (!artifact.exists) throw new Error("csv_file_not_found");
    const declaredSize = Math.max(0, Math.floor(Number(artifact.data()?.sizeBytes) || 0));
    if (declaredSize > byteLimit) throw new Error("csv_file_too_large");
    const count = Math.max(0, Math.floor(Number(artifact.data()?.chunkCount) || 0));
    if (count > Math.ceil(byteLimit / 180_000) + 1) throw new Error("csv_file_too_large");
    const refs = Array.from({ length: count }, (_, index) => artifact.ref.collection("chunks").doc(String(index).padStart(4, "0")));
    const chunks = refs.length ? await db.getAll(...refs) : [];
    const bytes = Buffer.from(chunks.map((snapshot) => clean(snapshot.data()?.text, 250_000)).join(""), "utf8");
    if (bytes.length > byteLimit) throw new Error("csv_file_too_large");
    return bytes;
  }
  const path = clean(record.storage_path, 1000).replace(/^\/+/, "");
  if (!path || path.includes("..")) throw new Error("csv_file_not_found");
  if (Object.keys(legacy).length && !isAllowedLegacyCsvStoragePath(path, legacy.owner_user_id, legacy.run_id)) throw new Error("csv_file_not_found");
  const file = bucket().file(path);
  const [metadata] = await file.getMetadata();
  if (Math.max(0, Number(metadata.size) || 0) > byteLimit) throw new Error("csv_file_too_large");
  const [bytes] = await file.download();
  if (bytes.length > byteLimit) throw new Error("csv_file_too_large");
  return bytes;
}

export async function renameAuthorizedCsv(db: FirebaseFirestore.Firestore, principal: ApiPrincipal, csvId: string, displayFilename: unknown): Promise<Record<string, unknown>> {
  const { record, access } = await findAuthorizedCsv(db, principal, csvId, "csv.rename", "datasets:write");
  const next = safeFileName(displayFilename);
  if (record.id.startsWith("legacy_")) {
    await db.collection("autopilot_requests").doc(record.id.slice(7)).set({ displayFilename: next, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    record.display_filename = next;
  } else {
    await db.collection(UPLOADS).doc(record.id).set({ display_filename: next, updated_at: new Date().toISOString() }, { merge: true });
    record.display_filename = next;
  }
  await writeWorkspaceAudit(db, { workspaceId: access.workspaceId, actorUserId: principal.userId, action: "csv_renamed", resourceType: "csv", resourceId: record.id });
  return publicUploadedCsv(record);
}

export async function copyAuthorizedCsv(db: FirebaseFirestore.Firestore, principal: ApiPrincipal, csvId: string, destinationWorkspaceId: unknown): Promise<Record<string, unknown>> {
  const source = await findAuthorizedCsv(db, principal, csvId, "csv.copy", "datasets:write");
  const destination = await resolveWorkspaceAccess(db, principal, destinationWorkspaceId);
  authorizeCsv(principal, destination, "datasets:write", "write", "csv.upload");
  const bytes = await readUploadedCsvBytes(db, source.record);
  const copied = await createManagedUploadedCsv(db, principal, destination.workspaceId, source.record.original_filename, bytes, {
    ticker: source.record.ticker, asset_class: source.record.asset_class, source_type: source.record.source_type,
    forecast_kind: source.record.forecast_kind, display_filename: source.record.display_filename,
  }, { copied_from_csv_id: source.record.id, copied_from_workspace_id: source.access.workspaceId });
  const copyId = clean((copied as any).id, 220);
  await db.collection(UPLOADS).doc(copyId).set({ copied_from_csv_id: source.record.id, copied_from_workspace_id: source.access.workspaceId, copied_at: new Date().toISOString() }, { merge: true });
  await writeWorkspaceAudit(db, { workspaceId: destination.workspaceId, actorUserId: principal.userId, action: "csv_copied", resourceType: "csv", resourceId: copyId, sourceWorkspaceId: source.access.workspaceId, destinationWorkspaceId: destination.workspaceId });
  return { ...copied, copied_from_csv_id: source.record.id, copied_from_workspace_id: source.access.workspaceId };
}

export async function moveAuthorizedCsv(db: FirebaseFirestore.Firestore, principal: ApiPrincipal, csvId: string, destinationWorkspaceId: unknown): Promise<Record<string, unknown>> {
  const source = await findAuthorizedCsv(db, principal, csvId, "csv.move", "datasets:write");
  const destination = await resolveWorkspaceAccess(db, principal, destinationWorkspaceId);
  authorizeCsv(principal, destination, "datasets:write", "write", "csv.upload");
  if (destination.workspaceId === source.access.workspaceId) throw new Error("csv_destination_same_workspace");
  if (source.record.id.startsWith("legacy_")) {
    const bytes = await readUploadedCsvBytes(db, source.record);
    const migrated = await createManagedUploadedCsv(db, principal, destination.workspaceId, source.record.original_filename, bytes, {
      ticker: source.record.ticker, asset_class: source.record.asset_class, source_type: source.record.source_type,
      forecast_kind: source.record.forecast_kind, display_filename: source.record.display_filename,
    }, { migrated_from_legacy_csv_id: source.record.id });
    await db.collection("autopilot_requests").doc(source.record.id.slice(7)).set({ workspaceId: destination.workspaceId, csv_api_hidden: true, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    await writeWorkspaceAudit(db, { workspaceId: source.access.workspaceId, actorUserId: principal.userId, action: "csv_moved", resourceType: "csv", resourceId: source.record.id, sourceWorkspaceId: source.access.workspaceId, destinationWorkspaceId: destination.workspaceId });
    return migrated;
  }
  const now = new Date().toISOString();
  await db.runTransaction(async (transaction) => {
    const ref = db.collection(UPLOADS).doc(source.record.id);
    const fresh = await transaction.get(ref);
    if (!fresh.exists || clean(fresh.data()?.workspace_id, 220) !== source.access.workspaceId) throw new Error("csv_move_conflict");
    transaction.update(ref, { workspace_id: destination.workspaceId, updated_at: now, moved_from_workspace_id: source.access.workspaceId, moved_at: now });
  });
  const moved = { ...source.record, workspace_id: destination.workspaceId, updated_at: now };
  await Promise.all([
    writeWorkspaceAudit(db, { workspaceId: source.access.workspaceId, actorUserId: principal.userId, action: "csv_moved", resourceType: "csv", resourceId: source.record.id, sourceWorkspaceId: source.access.workspaceId, destinationWorkspaceId: destination.workspaceId }),
    writeWorkspaceAudit(db, { workspaceId: destination.workspaceId, actorUserId: principal.userId, action: "csv_received", resourceType: "csv", resourceId: source.record.id, sourceWorkspaceId: source.access.workspaceId, destinationWorkspaceId: destination.workspaceId }),
  ]);
  return publicUploadedCsv(moved);
}

export async function deleteAuthorizedCsv(db: FirebaseFirestore.Firestore, principal: ApiPrincipal, csvId: string): Promise<{ deleted: true; id: string }> {
  const { record, access } = await findAuthorizedCsv(db, principal, csvId, "csv.delete", "datasets:write");
  if (record.id.startsWith("legacy_")) throw new Error("legacy_csv_delete_requires_migration");
  await db.collection(UPLOADS).doc(record.id).set({ deleted_at: new Date().toISOString(), deleted_by_user_id: principal.userId }, { merge: true });
  await bucket().file(record.storage_path).delete({ ignoreNotFound: true });
  await writeWorkspaceAudit(db, { workspaceId: access.workspaceId, actorUserId: principal.userId, action: "csv_deleted", resourceType: "csv", resourceId: record.id });
  return { deleted: true, id: record.id };
}
