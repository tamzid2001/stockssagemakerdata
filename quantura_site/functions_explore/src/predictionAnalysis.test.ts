import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { analyzePredictionCsv, classifyUploadedCsv } from "./autopilot";

const fixture = (name: string): string =>
  readFileSync(path.join(__dirname, "..", "src", "test-fixtures", name), "utf8");

const businessDates = (count: number, start = "2026-03-02"): string[] => {
  const dates: string[] = [];
  const cursor = new Date(`${start}T00:00:00Z`);
  while (dates.length < count) {
    const weekday = cursor.getUTCDay();
    if (weekday !== 0 && weekday !== 6) dates.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return dates;
};

const buildPredictionCsv = (
  p50Values: number[],
  options: { p10Values?: Array<number | null>; dates?: string[]; includeP10?: boolean; includeP50?: boolean } = {}
): string => {
  const includeP10 = options.includeP10 !== false;
  const includeP50 = options.includeP50 !== false;
  const dates = options.dates || businessDates(p50Values.length);
  const headers = ["date", "ticker", ...(includeP10 ? ["P10"] : []), ...(includeP50 ? ["P50"] : []), "P90"];
  const rows = p50Values.map((p50, index) => {
    const p10 = options.p10Values ? options.p10Values[index] : p50 - 10;
    const values: Array<string | number> = [dates[index], "PLTR"];
    if (includeP10) values.push(p10 === null ? "" : p10);
    if (includeP50) values.push(p50);
    values.push(p50 + 10);
    return values.join(",");
  });
  return [headers.join(","), ...rows].join("\n");
};

test("normal P50 values remain inside the statistical anomaly band", async () => {
  const result = await analyzePredictionCsv(fixture("predictions_normal.csv"), { ticker: "PLTR" });
  assert.equal(result.status, "ok");
  assert.equal(result.metrics.p50BoundaryMethod, "statistical_95_band");
  assert.equal(result.metrics.p50UnusualCount, 0);
});

test("P50 above the statistical upper boundary is flagged", async () => {
  const result = await analyzePredictionCsv(buildPredictionCsv([...Array(20).fill(100), 1000]));
  assert.equal(result.status, "ok");
  assert.equal(result.metrics.p50UnusualCount, 1);
  assert.equal((result.analysis.p50Anomalies as Array<{ direction: string }>)[0].direction, "high");
});

test("P50 below the statistical lower boundary is flagged", async () => {
  const result = await analyzePredictionCsv(buildPredictionCsv([...Array(20).fill(100), -800]));
  assert.equal(result.status, "ok");
  assert.equal(result.metrics.p50UnusualCount, 1);
  assert.equal((result.analysis.p50Anomalies as Array<{ direction: string }>)[0].direction, "low");
});

test("majority unusual P50 observations above average creates Selling Bias", async () => {
  const result = await analyzePredictionCsv(buildPredictionCsv([...Array(20).fill(100), 1000, 1000, 1000]));
  assert.equal(result.metrics.generalBias, "Selling Bias");
  assert.equal(result.metrics.p50UnusualAboveAverageCount, 3);
  assert.equal(result.metrics.p50UnusualBelowAverageCount, 0);
});

test("majority unusual P50 observations below average creates Buying Bias", async () => {
  const result = await analyzePredictionCsv(buildPredictionCsv([...Array(20).fill(100), -800, -800, -800]));
  assert.equal(result.metrics.generalBias, "Buying Bias");
  assert.equal(result.metrics.p50UnusualAboveAverageCount, 0);
  assert.equal(result.metrics.p50UnusualBelowAverageCount, 3);
});

test("equal high and low unusual P50 observations remain Neutral / Mixed", async () => {
  const result = await analyzePredictionCsv(buildPredictionCsv([...Array(20).fill(100), 1000, 1000, -800, -800]));
  assert.equal(result.metrics.generalBias, "Neutral / Mixed");
  assert.equal(result.metrics.p50UnusualAboveAverageCount, 2);
  assert.equal(result.metrics.p50UnusualBelowAverageCount, 2);
});

test("last two unusually low weekday P10 observations activate the two-week signal", async () => {
  const p50 = Array(22).fill(100);
  const p10 = [...Array(20).fill(90), 10, 10];
  const result = await analyzePredictionCsv(buildPredictionCsv(p50, { p10Values: p10 }));
  assert.equal(result.metrics.lastTwoP10Valid, true);
  assert.equal(result.metrics.lastTwoBusinessDays, true);
  assert.equal(result.metrics.lastTwoP10UnusuallyLow, true);
  assert.equal(result.metrics.extendedP10BuyBiasActive, true);
  assert.match(String(result.metrics.biasWindowStartDate), /^2026-\d{2}-\d{2}$/);
  assert.match(String(result.metrics.biasWindowEndDate), /^2026-\d{2}-\d{2}$/);
});

test("last two P10 values do not activate when one is not unusual", async () => {
  const p50 = Array(22).fill(100);
  const p10 = [...Array(20).fill(90), 10, 90];
  const result = await analyzePredictionCsv(buildPredictionCsv(p50, { p10Values: p10 }));
  assert.equal(result.metrics.lastTwoP10UnusuallyLow, false);
  assert.equal(result.metrics.extendedP10BuyBiasActive, false);
});

test("weekend final observations never activate the P10 tail signal", async () => {
  const result = await analyzePredictionCsv(fixture("predictions_weekend_tail.csv"), { ticker: "PLTR" });
  assert.equal(result.metrics.lastTwoP10UnusuallyLow, true);
  assert.equal(result.metrics.lastTwoBusinessDays, false);
  assert.equal(result.metrics.extendedP10BuyBiasActive, false);
});

test("missing P10 is accepted with an actionable warning and no tail signal", async () => {
  const result = await analyzePredictionCsv(buildPredictionCsv([100, 101, 102, 103], { includeP10: false }));
  assert.equal(result.status, "ok");
  assert.equal(result.metrics.p10Average, null);
  assert.equal(result.metrics.extendedP10BuyBiasActive, false);
  assert.match((result.analysis.validationWarnings as string[]).join(" "), /P10 was not supplied/);
});

test("missing P50 returns a useful validation error", async () => {
  const result = await analyzePredictionCsv(buildPredictionCsv([100, 101, 102], { includeP50: false }));
  assert.equal(result.status, "error");
  assert.match((result.analysis.validationErrors as string[]).join(" "), /P50 or median is required/);
});

test("invalid CSV is rejected as a CSV validation failure", async () => {
  await assert.rejects(
    () => analyzePredictionCsv(fixture("predictions_invalid.csv")),
    /csv_validation: CSV must include a header row and at least one data row/
  );
});

test("explicit model-provided 95% interval columns are used instead of a derived P50 band", async () => {
  const result = await analyzePredictionCsv(fixture("predictions_explicit_95.csv"), { ticker: "AAPL" });
  assert.equal(result.status, "ok");
  assert.equal(result.metrics.explicit95Interval, true);
  assert.equal(result.metrics.p50BoundaryMethod, "model_95_interval");
  assert.equal(result.metrics.p50LowerBoundary, null);
  assert.equal(result.metrics.p50UnusualCount, 1);
  assert.equal((result.analysis.p50Anomalies as Array<{ date: string }>)[0].date, "2026-01-09");
});

test("out-of-order rows are sorted chronologically and reported", async () => {
  const csv = [
    "date,ticker,P10,P50,P90",
    "2026-03-04,PLTR,92,102,112",
    "2026-03-02,PLTR,90,100,110",
    "2026-03-03,PLTR,91,101,111",
  ].join("\n");
  const result = await analyzePredictionCsv(csv);
  assert.equal(result.status, "ok");
  assert.equal(result.metrics.forecastStartDate, "2026-03-02");
  assert.match((result.analysis.validationWarnings as string[]).join(" "), /sorted from oldest to newest/);
  assert.match(String(result.businessDayCsvText), /2026-03-02[\s\S]*2026-03-03[\s\S]*2026-03-04/);
});

test("duplicate timestamps are rejected", async () => {
  const csv = [
    "date,ticker,P10,P50,P90",
    "2026-03-02,PLTR,90,100,110",
    "2026-03-02,PLTR,91,101,111",
  ].join("\n");
  const result = await analyzePredictionCsv(csv);
  assert.equal(result.status, "error");
  assert.match((result.analysis.validationErrors as string[]).join(" "), /duplicate date\/timestamp/);
});

test("non-numeric quantiles and invalid tickers are rejected", async () => {
  const csv = [
    "date,ticker,P10,P50,P90",
    "2026-03-02,BAD TICKER,90,not-a-number,110",
    "2026-03-03,BAD TICKER,91,101,111",
  ].join("\n");
  const result = await analyzePredictionCsv(csv);
  assert.equal(result.status, "error");
  const errors = (result.analysis.validationErrors as string[]).join(" ");
  assert.match(errors, /non-numeric/);
  assert.match(errors, /invalid ticker/);
});

test("quantile crossing is rejected with an ordering error", async () => {
  const csv = [
    "date,ticker,P10,P50,P90",
    "2026-03-02,PLTR,120,100,110",
    "2026-03-03,PLTR,90,101,111",
  ].join("\n");
  const result = await analyzePredictionCsv(csv);
  assert.equal(result.status, "error");
  assert.match((result.analysis.validationErrors as string[]).join(" "), /ascending quantile order/);
});

test("prediction quantiles take priority when actual price is named close", async () => {
  const csv = [
    "date,ticker,P10,P50,P90,close",
    "2026-03-02,PLTR,90,100,110,101",
    "2026-03-03,PLTR,91,101,111,102",
  ].join("\n");
  const classified = await classifyUploadedCsv(csv);
  assert.equal(classified.kind, "prediction_output");
  if (classified.kind !== "prediction_output") return;
  assert.equal(classified.analysis.analysis.actualPriceColumn, "close");
});

test("invalid calendar dates are rejected instead of normalized", async () => {
  const csv = [
    "date,ticker,P10,P50,P90",
    "2026-02-30,PLTR,90,100,110",
    "2026-03-03,PLTR,91,101,111",
  ].join("\n");
  const result = await analyzePredictionCsv(csv);
  assert.equal(result.status, "error");
  assert.match((result.analysis.validationErrors as string[]).join(" "), /invalid or missing date/);
});
