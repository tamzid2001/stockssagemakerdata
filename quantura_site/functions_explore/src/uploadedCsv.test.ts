import assert from "node:assert/strict";
import test from "node:test";
import { inspectCsvBytes, isAllowedLegacyCsvStoragePath, isAllowedUserCsvImportStoragePath, publicUploadedCsv } from "./uploadedCsv";

test("uploaded CSV inspection derives dimensions, quantiles, checksum, and UTC coverage from bytes", () => {
  const bytes = Buffer.from("date,p1,p25,p50,p75,p99,note\n2026-09-01,1,2,3,4,5,\"a,b\"\n2026-09-02,2,3,4,5,6,ok\n");
  const result = inspectCsvBytes(bytes, "001__AAPL__forecast.csv", { source_type: "prediction_csv", forecast_kind: "ensemble" });
  assert.equal(result.row_count, 2);
  assert.equal(result.column_count, 7);
  assert.equal(result.data_cell_count, 14);
  assert.equal(result.populated_data_cell_count, 14);
  assert.equal(result.ticker, "AAPL");
  assert.deepEqual(result.quantile_columns, ["p1", "p25", "p50", "p75", "p99"]);
  assert.equal(result.first_timestamp, "2026-09-01T00:00:00.000Z");
  assert.equal(result.last_timestamp, "2026-09-02T00:00:00.000Z");
  assert.equal(result.inferred_granularity, "1d");
  assert.match(String(result.sha256), /^[a-f0-9]{64}$/);
});

test("ambiguous timestamps remain null instead of being fabricated", () => {
  const result = inspectCsvBytes(Buffer.from("date,value\nSep 1,1\nSep 2,2\n"), "unlabeled.csv");
  assert.equal(result.first_timestamp, null);
  assert.equal(result.last_timestamp, null);
  assert.equal(result.metadata_status, "partial");
  assert.ok((result.metadata_warnings as string[]).length > 0);
});

test("CSV validation rejects malformed and duplicated headers", () => {
  assert.throws(() => inspectCsvBytes(Buffer.from("a,a\n1,2\n"), "bad.csv"), /duplicate_columns/);
  assert.throws(() => inspectCsvBytes(Buffer.from("a,b\n\"broken,2\n"), "bad.csv"), /unclosed_quote/);
});

test("public CSV serialization never exposes private storage paths", () => {
  const value = publicUploadedCsv({
    id: "csv_one", workspace_id: "ws_one", storage_path: "private/path.csv", legacy_file: { secret: true },
    original_filename: "one.csv", display_filename: "one.csv", uploaded_at: "2026-09-03T00:00:00.000Z", updated_at: "2026-09-03T00:00:00.000Z",
  } as any);
  assert.equal(value.storage_path, undefined);
  assert.equal(value.legacy_file, undefined);
  assert.equal(value.download_endpoint, "/api/v1/uploads/csv/csv_one/download");
});

test("legacy CSV storage paths remain bound to the producing owner and run", () => {
  assert.equal(isAllowedLegacyCsvStoragePath("forecast_reports/user-1/foundry/run-1/predictions.csv", "user-1", "run-1"), true);
  assert.equal(isAllowedLegacyCsvStoragePath("predictions/user-1/foundry/run-1/dataset.csv", "user-1", "run-1"), true);
  assert.equal(isAllowedLegacyCsvStoragePath("predictions/user-2/foundry/run-2/private.csv", "user-1", "run-1"), false);
  assert.equal(isAllowedLegacyCsvStoragePath("forecast_reports/user-1/foundry/run-10/predictions.csv", "user-1", "run-1"), false);
  assert.equal(isAllowedLegacyCsvStoragePath("arbitrary/object.csv", "user-1", "run-1"), false);
});

test("storage-backed CSV imports remain inside the authenticated user's namespace", () => {
  assert.equal(isAllowedUserCsvImportStoragePath("predictions/user-1/incoming/data.csv", "user-1"), true);
  assert.equal(isAllowedUserCsvImportStoragePath("forecast_reports/user-1/export.csv", "user-1"), true);
  assert.equal(isAllowedUserCsvImportStoragePath("predictions/user-2/private.csv", "user-1"), false);
  assert.equal(isAllowedUserCsvImportStoragePath("predictions/user-1/../user-2/private.csv", "user-1"), false);
  assert.equal(isAllowedUserCsvImportStoragePath("predictions/user-1/private.json", "user-1"), false);
});
