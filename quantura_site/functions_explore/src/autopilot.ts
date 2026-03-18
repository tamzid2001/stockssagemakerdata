import { SecretManagerServiceClient } from "@google-cloud/secret-manager";
import {
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import {
  CreateAutoMLJobV2Command,
  CreateModelCommand,
  CreateTransformJobCommand,
  DescribeAutoMLJobV2Command,
  DescribeTransformJobCommand,
  SageMakerClient,
} from "@aws-sdk/client-sagemaker";
import { Readable } from "stream";

export type SupportedDatasetInterval = "1m" | "1h" | "1d";

export type CanonicalDatasetRow = {
  item_id: string;
  timestamp: string;
  closing_price: number;
};

export type CanonicalDataset = {
  kind: "historical_dataset";
  ticker: string;
  interval: SupportedDatasetInterval;
  rowCount: number;
  columns: string[];
  rows: CanonicalDatasetRow[];
  previewRows: CanonicalDatasetRow[];
  csvText: string;
  trainingEligible: boolean;
  sourceTimeColumn: string;
  sourceValueColumn: string;
  sourceItemColumn: string;
};

export type CurrentPriceInfo = {
  ticker: string;
  currentPrice: number | null;
  quoteTime: string;
  source: string;
  error: string;
};

export type PredictionAnalysisResult = {
  kind: "prediction_output";
  status: "ok" | "error";
  ticker: string;
  rowCount: number;
  columns: string[];
  summary: string;
  markdown: string;
  metrics: Record<string, number | string | null>;
  analysis: Record<string, unknown>;
  previewRows: Array<Record<string, string>>;
};

export type UploadClassificationResult =
  | {
      kind: "historical_dataset";
      dataset: CanonicalDataset;
      originalHeaders: string[];
      previewRows: Array<Record<string, string>>;
    }
  | {
      kind: "prediction_output";
      analysis: PredictionAnalysisResult;
      originalHeaders: string[];
      previewRows: Array<Record<string, string>>;
    };

export type AutopilotAwsConfig = {
  region: string;
  roleArn: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken: string;
  transformInstanceType: string;
};

export type StartAutopilotTrainingInput = {
  runId: string;
  userId: string;
  ticker: string;
  interval: unknown;
  horizon: number;
  quantiles: unknown;
  runtimeSeconds?: number | null;
  csvText: string;
};

export type StartAutopilotTrainingResult = {
  jobName: string;
  jobArn: string;
  inputS3Uri: string;
  outputS3Uri: string;
  forecastFrequency: "1D" | "1H";
  quantiles: string[];
  algorithms: string[];
  runtimeSeconds: number | null;
};

export type RefreshAutopilotRunInput = {
  runId: string;
  userId: string;
  ticker: string;
  datasetS3Uri: string;
  autopilot: Record<string, unknown>;
};

export type RefreshAutopilotRunResult = {
  status: "queued" | "running" | "transforming" | "completed" | "failed";
  autopilotPatch: Record<string, unknown>;
  predictionsCsvText: string;
  predictionsFileName: string;
};

type ParsedCsv = {
  headers: string[];
  rows: string[][];
};

type QuantileColumn = {
  index: number;
  header: string;
  quantile: number;
};

type BusinessBoundaryRow = {
  row: string[];
  rowIndex: number;
  parsedDate: Date;
  sessionDate: Date;
};

type BusinessBoundaryInfo = {
  timeIndex: number;
  timeHeader: string;
  validRows: BusinessBoundaryRow[];
  firstValid: BusinessBoundaryRow;
  lastValid: BusinessBoundaryRow;
};

const GCP_PROJECT_ID = asString(
  process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT
).trim();
const SECRET_MANAGER = GCP_PROJECT_ID ? new SecretManagerServiceClient() : null;
const SECRET_NAME_CACHE = new Map<string, string>();
const AUTOPILOT_ALGORITHMS = Object.freeze(["cnn-qr", "deepar", "prophet", "arima", "npts", "ets"]);
let cachedAwsConfigPromise: Promise<AutopilotAwsConfig> | null = null;

function asString(value: unknown, fallback = ""): string {
  if (typeof value === "string") return value;
  if (value === null || value === undefined) return fallback;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return fallback;
}

function asFinite(value: unknown, fallback = 0): number {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function sanitizeText(value: unknown, maxLen = 600): string {
  return asString(value)
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLen);
}

function normalizeTicker(value: unknown): string {
  return sanitizeText(value, 32)
    .toUpperCase()
    .replace(/[^A-Z0-9.=^/-]/g, "")
    .slice(0, 24);
}

function normalizeAwsNamePart(value: unknown, maxLen = 24): string {
  const clean = sanitizeText(value, maxLen * 2)
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
  return clean.slice(0, maxLen).replace(/^-+|-+$/g, "");
}

function normalizeCsvHeader(value: unknown): string {
  return sanitizeText(value, 80).toLowerCase().replace(/[^a-z0-9]/g, "");
}

function pad2(value: number): string {
  return String(value).padStart(2, "0");
}

function roundTo10(value: number): number {
  return Math.round(value / 10) * 10;
}

const DAY_MS = 24 * 60 * 60 * 1000;
const YAHOO_INTRADAY_RETENTION_DAYS = 60;
const YAHOO_MINUTE_CHUNK_DAYS = 7;
const US_MARKET_TIMEZONE = "America/New_York";
const TIME_ZONE_PARTS_FORMATTERS = new Map<string, Intl.DateTimeFormat>();
const TIME_ZONE_OFFSET_FORMATTERS = new Map<string, Intl.DateTimeFormat>();

function formatDateYmd(date: Date): string {
  return `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`;
}

function formatDateTimeUtc(date: Date): string {
  return `${formatDateYmd(date)} ${pad2(date.getUTCHours())}:${pad2(date.getUTCMinutes())}:${pad2(date.getUTCSeconds())}`;
}

function toUtcHourStart(date: Date): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours()));
}

function formatHourlyTimestampUtc(date: Date): string {
  const aligned = toUtcHourStart(date);
  return `${formatDateYmd(aligned)} ${pad2(aligned.getUTCHours())}:00:00`;
}

function getTimeZonePartsFormatter(timeZone: string): Intl.DateTimeFormat {
  const cached = TIME_ZONE_PARTS_FORMATTERS.get(timeZone);
  if (cached) return cached;
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });
  TIME_ZONE_PARTS_FORMATTERS.set(timeZone, formatter);
  return formatter;
}

function getTimeZoneOffsetFormatter(timeZone: string): Intl.DateTimeFormat {
  const cached = TIME_ZONE_OFFSET_FORMATTERS.get(timeZone);
  if (cached) return cached;
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    timeZoneName: "shortOffset",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });
  TIME_ZONE_OFFSET_FORMATTERS.set(timeZone, formatter);
  return formatter;
}

function getTimeZoneParts(date: Date, timeZone: string): Record<string, string> {
  const parts = getTimeZonePartsFormatter(timeZone).formatToParts(date);
  const read = (type: string) => parts.find((part) => part.type === type)?.value || "";
  return {
    year: read("year"),
    month: read("month"),
    day: read("day"),
    hour: read("hour"),
    minute: read("minute"),
    second: read("second"),
  };
}

function getTimeZoneOffsetMinutes(date: Date, timeZone: string): number {
  const parts = getTimeZoneOffsetFormatter(timeZone).formatToParts(date);
  const label = parts.find((part) => part.type === "timeZoneName")?.value || "GMT+0";
  const match = label.match(/^GMT([+-])(\d{1,2})(?::?(\d{2}))?$/i);
  if (!match) return 0;
  const sign = match[1] === "-" ? -1 : 1;
  return sign * (Number(match[2]) * 60 + Number(match[3] || 0));
}

function zonedPartsToUtc(
  year: number,
  month: number,
  day: number,
  hour: number,
  minute: number,
  second: number,
  timeZone: string
): Date {
  const baseUtcMs = Date.UTC(year, month - 1, day, hour, minute, second);
  let candidateUtcMs = baseUtcMs;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const offsetMinutes = getTimeZoneOffsetMinutes(new Date(candidateUtcMs), timeZone);
    const correctedUtcMs = baseUtcMs - offsetMinutes * 60 * 1000;
    if (correctedUtcMs === candidateUtcMs) break;
    candidateUtcMs = correctedUtcMs;
  }
  return new Date(candidateUtcMs);
}

function parseDateBoundaryInTimeZone(value: unknown, timeZone: string, boundary: "start" | "endExclusive"): Date | null {
  const text = asString(value).trim();
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const parsed = parseFlexibleDate(text);
    if (!parsed) return null;
    const boundaryDate = boundary === "endExclusive" ? addUtcDays(parsed, 1) : parsed;
    return zonedPartsToUtc(
      boundaryDate.getUTCFullYear(),
      boundaryDate.getUTCMonth() + 1,
      boundaryDate.getUTCDate(),
      0,
      0,
      0,
      timeZone
    );
  }
  return parseFlexibleDate(text);
}

function formatTimeZoneHourBucket(date: Date, timeZone: string): string {
  const parts = getTimeZoneParts(date, timeZone);
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:00:00`;
}

function parseFlexibleDate(value: unknown): Date | null {
  const text = asString(value).trim();
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const parsed = new Date(`${text}T00:00:00Z`);
    return Number.isFinite(parsed.getTime()) ? parsed : null;
  }
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(text)) {
    const parsed = new Date(text.replace(" ", "T") + "Z");
    return Number.isFinite(parsed.getTime()) ? parsed : null;
  }
  const parsed = new Date(text);
  return Number.isFinite(parsed.getTime()) ? parsed : null;
}

function formatAnalysisTimestamp(value: unknown): string {
  const parsed = parseFlexibleDate(value);
  if (!parsed) return "N/A";
  if (parsed.getUTCHours() === 0 && parsed.getUTCMinutes() === 0 && parsed.getUTCSeconds() === 0) {
    return formatDateYmd(parsed);
  }
  return formatDateTimeUtc(parsed);
}

function toUtcMidnight(date: Date): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function addUtcDays(date: Date, days: number): Date {
  return new Date(date.getTime() + days * 24 * 60 * 60 * 1000);
}

function formatUtcDateKey(date: Date): string {
  return formatDateYmd(toUtcMidnight(date));
}

function nthWeekdayOfMonthUtc(year: number, monthIndex: number, weekday: number, occurrence: number): Date {
  const firstDay = new Date(Date.UTC(year, monthIndex, 1));
  const offset = (weekday - firstDay.getUTCDay() + 7) % 7;
  return new Date(Date.UTC(year, monthIndex, 1 + offset + (occurrence - 1) * 7));
}

function lastWeekdayOfMonthUtc(year: number, monthIndex: number, weekday: number): Date {
  const lastDay = new Date(Date.UTC(year, monthIndex + 1, 0));
  const offset = (lastDay.getUTCDay() - weekday + 7) % 7;
  return new Date(Date.UTC(year, monthIndex, lastDay.getUTCDate() - offset));
}

function observedHolidayUtc(year: number, monthIndex: number, day: number): Date {
  const holiday = new Date(Date.UTC(year, monthIndex, day));
  if (holiday.getUTCDay() === 0) return addUtcDays(holiday, 1);
  if (holiday.getUTCDay() === 6) return addUtcDays(holiday, -1);
  return holiday;
}

function calculateWesternEasterSundayUtc(year: number): Date {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return new Date(Date.UTC(year, month - 1, day));
}

function buildUsEquityHolidayKeys(year: number): Set<string> {
  const holidays = new Set<string>();
  holidays.add(formatUtcDateKey(observedHolidayUtc(year, 0, 1)));
  holidays.add(formatUtcDateKey(nthWeekdayOfMonthUtc(year, 0, 1, 3)));
  holidays.add(formatUtcDateKey(nthWeekdayOfMonthUtc(year, 1, 1, 3)));
  holidays.add(formatUtcDateKey(addUtcDays(calculateWesternEasterSundayUtc(year), -2)));
  holidays.add(formatUtcDateKey(lastWeekdayOfMonthUtc(year, 4, 1)));
  if (year >= 2022) holidays.add(formatUtcDateKey(observedHolidayUtc(year, 5, 19)));
  holidays.add(formatUtcDateKey(observedHolidayUtc(year, 6, 4)));
  holidays.add(formatUtcDateKey(nthWeekdayOfMonthUtc(year, 8, 1, 1)));
  holidays.add(formatUtcDateKey(nthWeekdayOfMonthUtc(year, 10, 4, 4)));
  holidays.add(formatUtcDateKey(observedHolidayUtc(year, 11, 25)));
  return holidays;
}

function isUsEquityBusinessDay(date: Date): boolean {
  const normalized = toUtcMidnight(date);
  const weekday = normalized.getUTCDay();
  if (weekday === 0 || weekday === 6) return false;
  return !buildUsEquityHolidayKeys(normalized.getUTCFullYear()).has(formatUtcDateKey(normalized));
}

function readEnvSecret(keys: string[]): string {
  for (const key of keys) {
    const value = asString(process.env[key]).trim();
    if (value) return value;
  }
  return "";
}

async function readSecretManager(secretName: string): Promise<string> {
  if (!SECRET_MANAGER || !GCP_PROJECT_ID || !secretName) return "";
  try {
    const resource = `projects/${GCP_PROJECT_ID}/secrets/${secretName}/versions/latest`;
    const [version] = await SECRET_MANAGER.accessSecretVersion({ name: resource });
    const rawBytes = version.payload?.data;
    const raw = rawBytes ? Buffer.from(rawBytes).toString("utf8") : "";
    return raw.trim();
  } catch (error: any) {
    const message = asString(error?.message).toLowerCase();
    if (message.includes("not found") || message.includes("permission")) return "";
    console.warn(`[Autopilot] secret lookup failed for ${secretName}:`, error?.message || error);
    return "";
  }
}

async function discoverSecretValueByPattern(pattern: RegExp): Promise<string> {
  if (!SECRET_MANAGER || !GCP_PROJECT_ID) return "";
  const cachedName = SECRET_NAME_CACHE.get(pattern.source);
  if (cachedName) {
    return readSecretManager(cachedName);
  }
  try {
    const [secrets] = await SECRET_MANAGER.listSecrets({
      parent: `projects/${GCP_PROJECT_ID}`,
    });
    const matched = (secrets || [])
      .map((entry) => asString(entry.name).split("/").pop() || "")
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b))
      .find((name) => pattern.test(name));
    if (!matched) return "";
    SECRET_NAME_CACHE.set(pattern.source, matched);
    return readSecretManager(matched);
  } catch (error: any) {
    console.warn("[Autopilot] secret discovery failed:", error?.message || error);
    return "";
  }
}

async function resolveSecretValue(
  envKeys: string[],
  secretNames: string[],
  patterns: RegExp[] = []
): Promise<string> {
  const fromEnv = readEnvSecret(envKeys);
  if (fromEnv) return fromEnv;
  for (const secretName of secretNames) {
    const fromManager = await readSecretManager(secretName);
    if (fromManager) return fromManager;
  }
  for (const pattern of patterns) {
    const discovered = await discoverSecretValueByPattern(pattern);
    if (discovered) return discovered;
  }
  return "";
}

export async function resolveAutopilotAwsConfig(): Promise<AutopilotAwsConfig> {
  if (cachedAwsConfigPromise) return cachedAwsConfigPromise;
  cachedAwsConfigPromise = (async () => {
    const [
      accessKeyId,
      secretAccessKey,
      sessionToken,
      region,
      roleArn,
      bucket,
      transformInstanceType,
    ] = await Promise.all([
      resolveSecretValue(["AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY"], ["AWS_ACCESS_KEY_ID"], [/^aws[-_]?access[-_]?key[-_]?id$/i]),
      resolveSecretValue(
        ["AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY"],
        ["AWS_SECRET_ACCESS_KEY"],
        [/^aws[-_]?secret[-_]?access[-_]?key$/i]
      ),
      resolveSecretValue(["AWS_SESSION_TOKEN"], ["AWS_SESSION_TOKEN"], [/^aws[-_]?session[-_]?token$/i]),
      resolveSecretValue(["AWS_REGION"], ["AWS_REGION"], [/^aws[-_]?region$/i]),
      resolveSecretValue(
        ["AUTOPILOT_ROLE_ARN", "SAGEMAKER_EXECUTION_ROLE_ARN"],
        ["AUTOPILOT_ROLE_ARN", "SAGEMAKER_EXECUTION_ROLE_ARN"],
        [/^(autopilot|sagemaker).*(role|execution).*(arn)$/i]
      ),
      resolveSecretValue(
        ["AUTOPILOT_S3_BUCKET", "SAGEMAKER_AUTOPILOT_S3_BUCKET"],
        ["AUTOPILOT_S3_BUCKET", "SAGEMAKER_AUTOPILOT_S3_BUCKET"],
        [/^(autopilot|sagemaker).*(bucket)$/i]
      ),
      Promise.resolve(sanitizeText(process.env.AUTOPILOT_TRANSFORM_INSTANCE_TYPE, 80) || "ml.m5.2xlarge"),
    ]);

    const cfg: AutopilotAwsConfig = {
      region: region || "us-east-1",
      roleArn: roleArn.trim(),
      bucket: sanitizeText(bucket, 200),
      accessKeyId: accessKeyId.trim(),
      secretAccessKey: secretAccessKey.trim(),
      sessionToken: sessionToken.trim(),
      transformInstanceType: transformInstanceType,
    };

    if (!cfg.region || !cfg.roleArn || !cfg.bucket || !cfg.accessKeyId || !cfg.secretAccessKey) {
      throw new Error("Autopilot AWS runtime configuration is incomplete. Check Google Secret Manager bindings.");
    }

    return cfg;
  })();
  return cachedAwsConfigPromise;
}

function buildAwsCredentials(
  config: AutopilotAwsConfig
): { accessKeyId: string; secretAccessKey: string; sessionToken?: string } | undefined {
  if (!config.accessKeyId || !config.secretAccessKey) return undefined;
  return {
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    ...(config.sessionToken ? { sessionToken: config.sessionToken } : {}),
  };
}

function createS3Client(config: AutopilotAwsConfig): S3Client {
  return new S3Client({
    region: config.region,
    credentials: buildAwsCredentials(config),
  });
}

function createSageMakerClient(config: AutopilotAwsConfig): SageMakerClient {
  return new SageMakerClient({
    region: config.region,
    credentials: buildAwsCredentials(config),
  });
}

function parseCsvTable(csvText: string, maxRows = 200000): ParsedCsv {
  const text = asString(csvText);
  if (!text.trim()) throw new Error("CSV file is empty.");

  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  const pushField = () => {
    row.push(field);
    field = "";
  };

  const pushRow = () => {
    if (row.length === 1 && !asString(row[0]).trim()) {
      row = [];
      return;
    }
    rows.push(row);
    row = [];
  };

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];
    if (inQuotes) {
      if (char === "\"" && next === "\"") {
        field += "\"";
        index += 1;
        continue;
      }
      if (char === "\"") {
        inQuotes = false;
        continue;
      }
      field += char;
      continue;
    }
    if (char === "\"") {
      inQuotes = true;
      continue;
    }
    if (char === ",") {
      pushField();
      continue;
    }
    if (char === "\n") {
      pushField();
      pushRow();
      if (rows.length >= maxRows + 1) break;
      continue;
    }
    if (char === "\r") continue;
    field += char;
  }

  if (field.length || row.length) {
    pushField();
    pushRow();
  }

  if (rows.length < 2) {
    throw new Error("CSV must include a header row and at least one data row.");
  }

  return {
    headers: rows[0].map((header) => sanitizeText(header, 120)),
    rows: rows.slice(1).filter((values) => values.some((cell) => asString(cell).trim())),
  };
}

function csvEscape(value: unknown): string {
  const raw = value === null || value === undefined ? "" : String(value);
  if (/["\n,\r]/.test(raw)) {
    return `"${raw.replace(/"/g, "\"\"")}"`;
  }
  return raw;
}

function serializeCsv(headers: string[], rows: Array<Array<string | number>>): string {
  const lines = [headers.map((header) => csvEscape(header)).join(",")];
  rows.forEach((row) => {
    lines.push(row.map((cell) => csvEscape(cell)).join(","));
  });
  return lines.join("\n");
}

function datasetPreviewRows(rows: CanonicalDatasetRow[], limit = 24): CanonicalDatasetRow[] {
  return rows.slice(0, Math.max(1, Math.min(limit, rows.length)));
}

function rowsToObjectPreview(headers: string[], rows: string[][], limit = 20): Array<Record<string, string>> {
  return rows.slice(0, Math.max(1, Math.min(limit, rows.length))).map((row) => {
    const out: Record<string, string> = {};
    headers.forEach((header, index) => {
      out[header] = asString(row[index]);
    });
    return out;
  });
}

function extractQuantileColumns(headers: string[]): QuantileColumn[] {
  const cols: QuantileColumn[] = [];
  headers.forEach((header, index) => {
    const normalized = normalizeCsvHeader(header);
    let quantile = Number.NaN;
    let match = normalized.match(/^(?:p|q)(\d{1,2})$/);
    if (match) {
      quantile = Number(match[1]) / 100;
    } else {
      match = normalized.match(/^(?:p|q)(0?\.\d+)$/);
      if (match) quantile = Number(match[1]);
    }
    if (!Number.isFinite(quantile) || quantile <= 0 || quantile >= 1) return;
    cols.push({ index, header, quantile });
  });
  return cols.sort((left, right) => left.quantile - right.quantile);
}

function findPreferredColumn(headers: string[], candidates: string[]): number {
  const wanted = new Set(candidates.map((entry) => normalizeCsvHeader(entry)));
  return headers.findIndex((header) => wanted.has(normalizeCsvHeader(header)));
}

function inferIntervalFromRows(rows: CanonicalDatasetRow[]): SupportedDatasetInterval {
  if (rows.length < 2) return "1d";
  const diffs = rows
    .map((row) => parseFlexibleDate(row.timestamp)?.getTime() || Number.NaN)
    .slice(1)
    .map((timestamp, index) => timestamp - (parseFlexibleDate(rows[index].timestamp)?.getTime() || Number.NaN))
    .filter((diff) => Number.isFinite(diff) && diff > 0)
    .sort((left, right) => left - right);
  if (!diffs.length) return "1d";
  const medianMs = diffs[Math.floor(diffs.length / 2)];
  if (medianMs <= 5 * 60 * 1000) return "1m";
  if (medianMs <= 2 * 60 * 60 * 1000) return "1h";
  return "1d";
}

function normalizeSupportedInterval(value: unknown): SupportedDatasetInterval {
  const normalized = sanitizeText(value, 20).toLowerCase();
  if (normalized === "1m" || normalized === "minute" || normalized === "min") return "1m";
  if (normalized === "1h" || normalized === "hour" || normalized === "hourly") return "1h";
  return "1d";
}

function normalizeForecastFrequency(interval: SupportedDatasetInterval): "1D" | "1H" {
  return interval === "1h" ? "1H" : "1D";
}

function normalizeForecastQuantiles(values: unknown): string[] {
  const rawValues = Array.isArray(values)
    ? values
    : asString(values)
        .split(/[,\s]+/)
        .map((entry) => entry.trim())
        .filter(Boolean);
  if (!rawValues.length) return ["p10", "p50", "p90"];

  const normalized = rawValues.map((entry) => {
    const raw = sanitizeText(entry, 16).toLowerCase();
    if (!raw) throw new Error("invalid_quantiles: Empty quantile values are not allowed.");
    let percent = Number.NaN;
    let match = raw.match(/^(?:p|q)(\d{1,2})$/);
    if (match) {
      percent = Number(match[1]);
    } else {
      const numeric = Number(raw);
      if (!Number.isFinite(numeric)) {
        throw new Error(`invalid_quantiles: Invalid quantile value "${sanitizeText(entry, 32)}".`);
      }
      percent = numeric > 1 ? numeric : numeric * 100;
    }
    if (!Number.isFinite(percent) || !(percent > 0 && percent < 100)) {
      throw new Error("invalid_quantiles: Forecast Foundry quantiles must be between 0.01 and 0.99.");
    }
    if (Math.abs(percent - Math.round(percent)) > 1e-6) {
      throw new Error(
        "invalid_quantiles: Forecast Foundry quantiles must use 0.01 steps such as 0.1, 0.25, 0.5, 0.75, and 0.9."
      );
    }
    return `p${Math.round(percent)}`;
  });

  const unique = Array.from(new Set(normalized))
    .map((entry) => ({ entry, numeric: Number(entry.replace(/^p/i, "")) }))
    .filter((entry) => Number.isFinite(entry.numeric))
    .sort((left, right) => left.numeric - right.numeric);

  if (unique.length < 3) {
    throw new Error("invalid_quantiles: Forecast Foundry requires at least 3 quantiles.");
  }
  if (unique.length > 5) {
    throw new Error("invalid_quantiles: Forecast Foundry supports up to 5 quantiles.");
  }
  if (unique.length % 2 === 0) {
    throw new Error("invalid_quantiles: Forecast Foundry quantiles must use an odd count so a middle median quantile exists.");
  }
  if (!unique.some((entry) => entry.entry === "p50")) {
    throw new Error("invalid_quantiles: Forecast Foundry quantiles must include 0.5.");
  }
  return unique.map((entry) => entry.entry);
}

function normalizeRuntimeSeconds(value: unknown): number | null {
  const numeric = asFinite(value, Number.NaN);
  if (!Number.isFinite(numeric)) return null;
  return Math.max(1800, Math.min(86_400, Math.floor(numeric)));
}

function normalizeHorizon(value: unknown): number {
  return Math.max(1, Math.min(2_500, Math.floor(asFinite(value, 30) || 30)));
}

function buildCanonicalDataset(
  rows: CanonicalDatasetRow[],
  options: {
    ticker: string;
    interval: SupportedDatasetInterval;
    sourceTimeColumn: string;
    sourceValueColumn: string;
    sourceItemColumn: string;
  }
): CanonicalDataset {
  const cleanTicker = normalizeTicker(options.ticker);
  const normalizedRows = rows
    .slice()
    .sort((left, right) => {
      const leftMs = parseFlexibleDate(left.timestamp)?.getTime() || 0;
      const rightMs = parseFlexibleDate(right.timestamp)?.getTime() || 0;
      return leftMs - rightMs;
    });
  const csvText = serializeCsv(
    ["item_id", "timestamp", "closing_price"],
    normalizedRows.map((row) => [row.item_id, row.timestamp, row.closing_price])
  );
  return {
    kind: "historical_dataset",
    ticker: cleanTicker,
    interval: options.interval,
    rowCount: normalizedRows.length,
    columns: ["item_id", "timestamp", "closing_price"],
    rows: normalizedRows,
    previewRows: datasetPreviewRows(normalizedRows),
    csvText,
    trainingEligible: options.interval !== "1m",
    sourceTimeColumn: options.sourceTimeColumn,
    sourceValueColumn: options.sourceValueColumn,
    sourceItemColumn: options.sourceItemColumn,
  };
}

function normalizeHistoricalUpload(parsed: ParsedCsv, tickerHint = "", intervalHint = ""): CanonicalDataset {
  const headers = parsed.headers || [];
  const itemIdIndex = findPreferredColumn(headers, ["item_id", "itemid", "ticker", "symbol"]);
  const timeIndex = findPreferredColumn(headers, ["timestamp", "datetime", "date", "time"]);
  const valueIndex = findPreferredColumn(headers, ["closing_price", "closingprice", "close", "price", "adj close", "adjclose"]);

  if (timeIndex < 0 || valueIndex < 0) {
    throw new Error("Historical datasets need time/date and closing price columns.");
  }

  const cleanTicker = normalizeTicker(tickerHint);
  const rows: CanonicalDatasetRow[] = [];
  parsed.rows.forEach((values) => {
    const itemId = normalizeTicker(itemIdIndex >= 0 ? values[itemIdIndex] : cleanTicker);
    const parsedDate = parseFlexibleDate(values[timeIndex]);
    const closingPrice = Number(asString(values[valueIndex]).replace(/,/g, ""));
    if (!parsedDate || !Number.isFinite(closingPrice)) return;
    if (!itemId) return;
    const interval = normalizeSupportedInterval(intervalHint);
    rows.push({
      item_id: itemId,
      timestamp:
        interval === "1d"
          ? formatDateYmd(parsedDate)
          : interval === "1h"
            ? formatHourlyTimestampUtc(parsedDate)
            : formatDateTimeUtc(parsedDate),
      closing_price: Number(closingPrice.toFixed(6)),
    });
  });

  if (!rows.length) {
    throw new Error("No valid historical rows were found after normalization.");
  }

  const inferredInterval = intervalHint ? normalizeSupportedInterval(intervalHint) : inferIntervalFromRows(rows);
  const fallbackTicker = normalizeTicker(rows[0]?.item_id || cleanTicker);
  return buildCanonicalDataset(rows, {
    ticker: fallbackTicker,
    interval: inferredInterval,
    sourceTimeColumn: headers[timeIndex] || "timestamp",
    sourceValueColumn: headers[valueIndex] || "closing_price",
    sourceItemColumn: itemIdIndex >= 0 ? headers[itemIdIndex] || "item_id" : "ticker_hint",
  });
}

async function fetchYahooHistorySegment(input: {
  ticker: string;
  interval: SupportedDatasetInterval;
  startSeconds: number;
  endSeconds: number;
  fetchImpl: typeof fetch;
}): Promise<CanonicalDatasetRow[]> {
  const response = await input.fetchImpl(
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(input.ticker)}?period1=${input.startSeconds}&period2=${input.endSeconds}&interval=${encodeURIComponent(
      input.interval
    )}&includePrePost=false&events=div%2Csplits&corsDomain=finance.yahoo.com`,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
        "User-Agent": "QuanturaForecastFoundry/1.0",
      },
    }
  );
  if (!response.ok) {
    throw new Error(`Yahoo history request failed (${response.status}).`);
  }
  const payload = (await response.json().catch(() => ({}))) as Record<string, any>;
  const result = Array.isArray(payload?.chart?.result) ? payload.chart.result[0] : null;
  const timestamps = Array.isArray(result?.timestamp) ? result.timestamp : [];
  const quote = Array.isArray(result?.indicators?.quote) ? result.indicators.quote[0] || {} : {};
  const closes = Array.isArray(quote?.close) ? quote.close : [];

  const rows: CanonicalDatasetRow[] = [];
  timestamps.forEach((timestamp: number, index: number) => {
    const close = Number(closes[index]);
    const date = new Date(Number(timestamp) * 1000);
    if (!Number.isFinite(close) || !Number.isFinite(date.getTime())) return;
    rows.push({
      item_id: input.ticker,
      timestamp:
        input.interval === "1d"
          ? formatDateYmd(date)
          : input.interval === "1h"
            ? formatHourlyTimestampUtc(date)
            : formatDateTimeUtc(date),
      closing_price: Number(close.toFixed(6)),
    });
  });
  return rows;
}

type MinutePricePoint = {
  timestampMs: number;
  close: number;
};

function clipYahooIntradayWindow(start: Date, endExclusive: Date): { start: Date; endExclusive: Date } {
  if (start.getTime() >= endExclusive.getTime()) {
    throw new Error("Start and end dates must define a positive range.");
  }
  const earliestAllowed = new Date(Date.now() - YAHOO_INTRADAY_RETENTION_DAYS * DAY_MS);
  if (endExclusive.getTime() <= earliestAllowed.getTime()) {
    throw new Error(
      `Hourly history is built from Yahoo 1-minute data and is limited to roughly the last ${YAHOO_INTRADAY_RETENTION_DAYS} days.`
    );
  }
  const clippedStart = start.getTime() < earliestAllowed.getTime() ? earliestAllowed : start;
  if (clippedStart.getTime() >= endExclusive.getTime()) {
    throw new Error("Start and end dates must define a positive range after applying Yahoo intraday retention limits.");
  }
  return { start: clippedStart, endExclusive };
}

async function fetchYahooMinuteHistorySegment(input: {
  ticker: string;
  startMs: number;
  endMs: number;
  fetchImpl: typeof fetch;
}): Promise<MinutePricePoint[]> {
  const response = await input.fetchImpl(
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(input.ticker)}?period1=${Math.floor(
      input.startMs / 1000
    )}&period2=${Math.floor(input.endMs / 1000)}&interval=1m&includePrePost=true&events=div%2Csplits&corsDomain=finance.yahoo.com`,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
        "User-Agent": "QuanturaForecastFoundry/1.0",
      },
    }
  );
  if (!response.ok) {
    throw new Error(`Yahoo minute history request failed (${response.status}).`);
  }
  const payload = (await response.json().catch(() => ({}))) as Record<string, any>;
  const result = Array.isArray(payload?.chart?.result) ? payload.chart.result[0] : null;
  const timestamps = Array.isArray(result?.timestamp) ? result.timestamp : [];
  const quote = Array.isArray(result?.indicators?.quote) ? result.indicators.quote[0] || {} : {};
  const closes = Array.isArray(quote?.close) ? quote.close : [];

  const rows: MinutePricePoint[] = [];
  timestamps.forEach((timestamp: number, index: number) => {
    const close = Number(closes[index]);
    const timestampMs = Number(timestamp) * 1000;
    if (!Number.isFinite(close) || !Number.isFinite(timestampMs)) return;
    rows.push({ timestampMs, close: Number(close.toFixed(6)) });
  });
  return rows;
}

function minuteRowsToExtendedHourlyRows(ticker: string, minuteRows: MinutePricePoint[]): CanonicalDatasetRow[] {
  const dedupedMinutes = new Map<number, number>();
  minuteRows
    .slice()
    .sort((left, right) => left.timestampMs - right.timestampMs)
    .forEach((row) => {
      dedupedMinutes.set(row.timestampMs, row.close);
    });

  const hourlyBuckets = new Map<string, number>();
  Array.from(dedupedMinutes.entries())
    .sort((left, right) => left[0] - right[0])
    .forEach(([timestampMs, close]) => {
      const bucketTime = new Date(timestampMs);
      const parts = getTimeZoneParts(bucketTime, US_MARKET_TIMEZONE);
      const hour = Number(parts.hour);
      if (!Number.isFinite(hour) || hour < 4 || hour > 19) return;
      hourlyBuckets.set(formatTimeZoneHourBucket(bucketTime, US_MARKET_TIMEZONE), Number(close.toFixed(6)));
    });

  return Array.from(hourlyBuckets.entries()).map(([timestamp, close]) => ({
    item_id: ticker,
    timestamp,
    closing_price: close,
  }));
}

export async function downloadHistoricalStockDataset(input: {
  ticker: string;
  interval: unknown;
  start?: string;
  end?: string;
  useAllHistory?: boolean;
  fetchImpl?: typeof fetch;
}): Promise<CanonicalDataset> {
  const ticker = normalizeTicker(input.ticker);
  const interval = normalizeSupportedInterval(input.interval);
  if (!ticker) throw new Error("Ticker is required.");
  const useAllHistory = Boolean(input.useAllHistory);
  const startDate =
    interval === "1h"
      ? useAllHistory
        ? null
        : parseDateBoundaryInTimeZone(input.start, US_MARKET_TIMEZONE, "start")
      : useAllHistory
        ? null
        : parseFlexibleDate(input.start);
  const endDate =
    interval === "1h"
      ? parseDateBoundaryInTimeZone(input.end, US_MARKET_TIMEZONE, "endExclusive") || new Date()
      : parseFlexibleDate(input.end) || new Date();
  if (!endDate || (!useAllHistory && !startDate)) {
    throw new Error(useAllHistory ? "End date is required." : "Start and end dates are required.");
  }
  if (startDate && endDate.getTime() < startDate.getTime()) {
    throw new Error("End date must be on or after the start date.");
  }

  const fetchImpl = input.fetchImpl || fetch;
  const endExclusive = interval === "1h" ? endDate : addUtcDays(toUtcMidnight(endDate), 1);
  let rows: CanonicalDatasetRow[] = [];

  if (interval === "1h") {
    const requestedStart = useAllHistory ? new Date(Date.now() - YAHOO_INTRADAY_RETENTION_DAYS * DAY_MS) : (startDate as Date);
    const clippedWindow = clipYahooIntradayWindow(requestedStart, endExclusive);
    const minuteRows: MinutePricePoint[] = [];
    for (
      let cursor = clippedWindow.start.getTime();
      cursor < clippedWindow.endExclusive.getTime();
      cursor += YAHOO_MINUTE_CHUNK_DAYS * DAY_MS
    ) {
      const chunkRows = await fetchYahooMinuteHistorySegment({
        ticker,
        startMs: cursor,
        endMs: Math.min(clippedWindow.endExclusive.getTime(), cursor + YAHOO_MINUTE_CHUNK_DAYS * DAY_MS),
        fetchImpl,
      });
      minuteRows.push(...chunkRows);
    }
    rows = minuteRowsToExtendedHourlyRows(ticker, minuteRows);
  } else {
    const chunkRows = await fetchYahooHistorySegment({
      ticker,
      interval,
      startSeconds: useAllHistory ? 0 : Math.floor((startDate as Date).getTime() / 1000),
      endSeconds: Math.floor(endExclusive.getTime() / 1000),
      fetchImpl,
    });
    rows = chunkRows;
  }

  if (!rows.length) {
    throw new Error(`No ${interval} history rows were returned for ${ticker}.`);
  }

  return buildCanonicalDataset(rows, {
    ticker,
    interval,
    sourceTimeColumn: "timestamp",
    sourceValueColumn: "close",
    sourceItemColumn: "ticker",
  });
}

async function fetchCurrentPriceInfo(ticker: string, fetchImpl: typeof fetch = fetch): Promise<CurrentPriceInfo> {
  const cleanTicker = normalizeTicker(ticker);
  const fallback: CurrentPriceInfo = {
    ticker: cleanTicker,
    currentPrice: null,
    quoteTime: "",
    source: "",
    error: "",
  };
  if (!cleanTicker) return fallback;

  try {
    const intraday = await fetchImpl(
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(cleanTicker)}?range=1d&interval=1m&includePrePost=true`,
      { headers: { Accept: "application/json", "User-Agent": "QuanturaForecastFoundry/1.0" } }
    );
    if (intraday.ok) {
      const payload = (await intraday.json().catch(() => ({}))) as Record<string, any>;
      const result = Array.isArray(payload?.chart?.result) ? payload.chart.result[0] : null;
      const timestamps = Array.isArray(result?.timestamp) ? result.timestamp : [];
      const quote = Array.isArray(result?.indicators?.quote) ? result.indicators.quote[0] || {} : {};
      const closes = Array.isArray(quote?.close) ? quote.close : [];
      for (let index = closes.length - 1; index >= 0; index -= 1) {
        const close = Number(closes[index]);
        const timestamp = Number(timestamps[index]);
        if (!Number.isFinite(close) || !Number.isFinite(timestamp)) continue;
        return {
          ticker: cleanTicker,
          currentPrice: Number(close.toFixed(4)),
          quoteTime: formatAnalysisTimestamp(new Date(timestamp * 1000).toISOString()),
          source: "Yahoo intraday 1m",
          error: "",
        };
      }
    }
  } catch (_error) {
    // Ignore and fall through.
  }

  try {
    const quote = await fetchImpl(
      `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(cleanTicker)}`,
      { headers: { Accept: "application/json", "User-Agent": "QuanturaForecastFoundry/1.0" } }
    );
    if (quote.ok) {
      const payload = (await quote.json().catch(() => ({}))) as Record<string, any>;
      const result = Array.isArray(payload?.quoteResponse?.result) ? payload.quoteResponse.result[0] : null;
      const price = Number(result?.regularMarketPrice ?? result?.postMarketPrice ?? result?.preMarketPrice);
      const quoteTime = Number(result?.regularMarketTime);
      if (Number.isFinite(price)) {
        return {
          ticker: cleanTicker,
          currentPrice: Number(price.toFixed(4)),
          quoteTime: Number.isFinite(quoteTime) ? formatAnalysisTimestamp(new Date(quoteTime * 1000).toISOString()) : "",
          source: "Yahoo quote",
          error: "",
        };
      }
    }
  } catch (_error) {
    // Ignore and fall through.
  }

  try {
    const daily = await fetchImpl(
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(cleanTicker)}?range=5d&interval=1d`,
      { headers: { Accept: "application/json", "User-Agent": "QuanturaForecastFoundry/1.0" } }
    );
    if (daily.ok) {
      const payload = (await daily.json().catch(() => ({}))) as Record<string, any>;
      const result = Array.isArray(payload?.chart?.result) ? payload.chart.result[0] : null;
      const timestamps = Array.isArray(result?.timestamp) ? result.timestamp : [];
      const quote = Array.isArray(result?.indicators?.quote) ? result.indicators.quote[0] || {} : {};
      const closes = Array.isArray(quote?.close) ? quote.close : [];
      for (let index = closes.length - 1; index >= 0; index -= 1) {
        const close = Number(closes[index]);
        const timestamp = Number(timestamps[index]);
        if (!Number.isFinite(close) || !Number.isFinite(timestamp)) continue;
        return {
          ticker: cleanTicker,
          currentPrice: Number(close.toFixed(4)),
          quoteTime: formatAnalysisTimestamp(new Date(timestamp * 1000).toISOString()),
          source: "Yahoo daily fallback",
          error: "",
        };
      }
    }
  } catch (error: any) {
    fallback.error = sanitizeText(error?.message, 240);
  }

  fallback.error = fallback.error || "Unable to fetch current price from Yahoo.";
  return fallback;
}

function buildPredictionAnalysisError(
  parsed: ParsedCsv,
  ticker: string,
  message: string,
  summary?: string
): PredictionAnalysisResult {
  const cleanMessage = sanitizeText(message, 500) || "Prediction CSV analysis failed.";
  return {
    kind: "prediction_output",
    status: "error",
    ticker,
    rowCount: parsed.rows.length,
    columns: parsed.headers,
    summary: sanitizeText(summary || cleanMessage, 500),
    markdown: `## Quantile Analysis\n\n**Ticker:** \`${ticker || "UNKNOWN"}\`\n\n**Error:** ${cleanMessage}`,
    metrics: {},
    analysis: {
      message: cleanMessage,
    },
    previewRows: rowsToObjectPreview(parsed.headers, parsed.rows),
  };
}

function resolveBusinessBoundaryRows(headers: string[], rows: string[][]): BusinessBoundaryInfo {
  const timeIndex = findPreferredColumn(headers, ["date", "datetime", "timestamp", "time"]);
  if (timeIndex < 0) {
    throw new Error("No parsable date or time column was found, so business-day boundary filtering could not be applied.");
  }

  const datedRows = rows
    .map((row, rowIndex) => {
      const parsedDate = parseFlexibleDate(row[timeIndex]);
      if (!parsedDate) return null;
      return {
        row,
        rowIndex,
        parsedDate,
        sessionDate: toUtcMidnight(parsedDate),
      } satisfies BusinessBoundaryRow;
    })
    .filter(Boolean) as BusinessBoundaryRow[];

  if (!datedRows.length) {
    throw new Error(`Date column "${headers[timeIndex] || "timestamp"}" has no valid parsed timestamps.`);
  }

  const validRows = datedRows.filter((entry) => isUsEquityBusinessDay(entry.sessionDate));
  if (!validRows.length) {
    throw new Error("No valid weekday business-day rows were found in the CSV.");
  }

  return {
    timeIndex,
    timeHeader: headers[timeIndex] || "timestamp",
    validRows,
    firstValid: validRows[0],
    lastValid: validRows[validRows.length - 1],
  };
}

export async function analyzePredictionCsv(
  csvText: string,
  options: { ticker?: string; fetchImpl?: typeof fetch } = {}
): Promise<PredictionAnalysisResult> {
  const parsed = parseCsvTable(csvText, 50000);
  const headers = parsed.headers || [];
  const quantileColumns = extractQuantileColumns(headers);
  const ticker = normalizeTicker(options.ticker);
  if (quantileColumns.length < 3) {
    return buildPredictionAnalysisError(
      parsed,
      ticker,
      `Need at least 3 columns starting with \`P\` or \`Q\`. Found columns: ${headers.join(", ") || "none"}`,
      "Prediction CSV analysis failed because fewer than three quantile columns were detected."
    );
  }

  if (quantileColumns.length % 2 === 0) {
    return buildPredictionAnalysisError(
      parsed,
      ticker,
      "Quantile columns must be an odd count so a middle median column exists.",
      "Prediction CSV analysis failed because the quantile column count must be odd."
    );
  }

  try {
    const medianIndex = Math.floor(quantileColumns.length / 2);
    const medianCol = quantileColumns[medianIndex];
    const lowerCols = quantileColumns.slice(0, medianIndex);
    const upperCols = quantileColumns.slice(medianIndex + 1);
    const boundary = resolveBusinessBoundaryRows(headers, parsed.rows);
    const firstRow = boundary.firstValid.row;
    const lastRow = boundary.lastValid.row;
    const firstMedianRaw = Number(firstRow[medianCol.index]);
    const lastMedianRaw = Number(lastRow[medianCol.index]);

    if (!Number.isFinite(firstMedianRaw) || !Number.isFinite(lastMedianRaw)) {
      throw new Error("Median quantile column is missing numeric values in the first or last valid business-day row.");
    }

    const firstMedianRounded = roundTo10(firstMedianRaw);
    const lastMedianRounded = roundTo10(lastMedianRaw);
    const branch = firstMedianRounded > lastMedianRounded ? "upper" : "lower";
    const returnedCols = branch === "upper" ? upperCols : lowerCols;
    const returnedValues = Object.fromEntries(
      returnedCols
        .map((col) => [col.header, Number(lastRow[col.index])])
        .filter((entry) => Number.isFinite(entry[1] as number))
        .map(([header, value]) => [String(header), Number((value as number).toFixed(6))])
    );

    const firstValidTimestamp = formatAnalysisTimestamp(boundary.firstValid.parsedDate.toISOString());
    const lastValidTimestamp = formatAnalysisTimestamp(boundary.lastValid.parsedDate.toISOString());
    const currentPriceInfo = ticker ? await fetchCurrentPriceInfo(ticker, options.fetchImpl || fetch) : null;
    const orderedLabels = quantileColumns.map((col) => col.header).join(", ");
    const returnedLines = Object.entries(returnedValues)
      .map(([label, value]) => `- **${label}**: ${Number(value).toLocaleString(undefined, { maximumFractionDigits: 6 })}`)
      .join("\n");
    const currentPriceHeading =
      currentPriceInfo && (currentPriceInfo.currentPrice !== null || currentPriceInfo.error) ? "### Current Price" : "";
    const currentPriceLine =
      currentPriceInfo && currentPriceInfo.currentPrice !== null
        ? `- Current price: **${currentPriceInfo.currentPrice.toFixed(4)}**`
        : currentPriceInfo?.error
          ? `- Unable to fetch price: **${currentPriceInfo.error}**`
          : "";
    const currentPriceQuoteTime = currentPriceInfo?.quoteTime ? `- Quote time: **${currentPriceInfo.quoteTime}**` : "";
    const currentPriceSource = currentPriceInfo?.source ? `- Source: **${currentPriceInfo.source}**` : "";
    const summary = [
      `${ticker || "Uploaded"} quantile analysis selected the ${branch} branch.`,
      `Rounded median moved from ${firstMedianRounded} on ${firstValidTimestamp} to ${lastMedianRounded} on ${lastValidTimestamp}.`,
      `Point-side rule: if the last rounded median is below the first rounded median, use the upper bound; otherwise use the lower bound.`,
      Object.keys(returnedValues).length
        ? `Returned last business-day values: ${Object.entries(returnedValues)
            .map(([label, value]) => `${label}=${value}`)
            .join(", ")}.`
        : "No terminal branch quantiles were returned from the last valid business-day row.",
    ]
      .join(" ")
      .trim();

    const markdown = [
      "## Quantile Analysis",
      "",
      `**Ticker:** \`${ticker || "UNKNOWN"}\``,
      `**Branch:** **${branch.toUpperCase()}**`,
      "",
      "### Business-Day Boundary Rule",
      "Compare only the nearest first valid business-day row and the nearest last valid business-day row. Weekends and observed U.S. equity-market holidays are skipped.",
      "",
      `- Time column used: **${boundary.timeHeader}**`,
      `- First valid row index: **${boundary.firstValid.rowIndex}**`,
      `- First valid date/time: **${firstValidTimestamp}**`,
      `- Last valid row index: **${boundary.lastValid.rowIndex}**`,
      `- Last valid date/time: **${lastValidTimestamp}**`,
      `- Valid business-day rows found: **${boundary.validRows.length}**`,
      "",
      "### Comparison Rule",
      "Compare the first valid row median with the last valid row median after rounding both to the nearest 10.",
      "",
      `- First valid median raw: **${firstMedianRaw.toFixed(6)}**`,
      `- First valid median rounded: **${firstMedianRounded.toFixed(0)}**`,
      `- Last valid median raw: **${lastMedianRaw.toFixed(6)}**`,
      `- Last valid median rounded: **${lastMedianRounded.toFixed(0)}**`,
      "",
      `Since **${firstMedianRounded.toFixed(0)} ${firstMedianRounded > lastMedianRounded ? ">" : "<="} ${lastMedianRounded.toFixed(
        0
      )}**, the logic selects the **${branch}** branch.`,
      "",
      "### Research Context",
      "AWS Forecast documents how Average wQL, wQL, WAPE, RMSE, MAPE, and MASE should be interpreted for time-series models. Lower values indicate better models. See [AWS Forecast metrics documentation](https://docs.aws.amazon.com/forecast/latest/dg/metrics.html?utm_source=chatgpt.com).",
      "",
      `- Point-forecast anchor: **${medianCol.header}** (the middle quantile, usually **P50** / **0.5**)`,
      `- Decision rule: if the last rounded median is below the first rounded median, use the **upper** bound from the last valid business-day row; otherwise use the **lower** bound.`,
      "",
      "### Quantile Structure",
      `- Ordered quantile columns: **${orderedLabels}**`,
      `- Median column: **${medianCol.header}**`,
      `- Lower columns: **${lowerCols.map((col) => col.header).join(", ") || "None"}**`,
      `- Upper columns: **${upperCols.map((col) => col.header).join(", ") || "None"}**`,
      "",
      currentPriceHeading,
      currentPriceLine,
      currentPriceQuoteTime,
      currentPriceSource,
      "",
      "### Conclusion",
      summary,
      "",
      "### Returned Columns from the Last Valid Business-Day Row",
      returnedLines || "- None",
    ]
      .filter(Boolean)
      .join("\n");

    return {
      kind: "prediction_output",
      status: "ok",
      ticker,
      rowCount: parsed.rows.length,
      columns: headers,
      summary,
      markdown,
      metrics: {
        branch,
        firstMedianRaw: Number(firstMedianRaw.toFixed(6)),
        firstMedianRounded,
        lastMedianRaw: Number(lastMedianRaw.toFixed(6)),
        lastMedianRounded,
        currentPrice: currentPriceInfo?.currentPrice ?? null,
        firstValidRowIndex: boundary.firstValid.rowIndex,
        lastValidRowIndex: boundary.lastValid.rowIndex,
        validBusinessDayRows: boundary.validRows.length,
        firstValidTimestamp,
        lastValidTimestamp,
      },
      analysis: {
        orderedQuantileColumns: quantileColumns.map((col) => col.header),
        medianColumn: medianCol.header,
        lowerColumns: lowerCols.map((col) => col.header),
        upperColumns: upperCols.map((col) => col.header),
        branch,
        returnedValues,
        currentPriceInfo,
        timeColumn: boundary.timeHeader,
        firstValidRowIndex: boundary.firstValid.rowIndex,
        lastValidRowIndex: boundary.lastValid.rowIndex,
        firstValidTimestamp,
        lastValidTimestamp,
        validBusinessDayRows: boundary.validRows.length,
      },
      previewRows: rowsToObjectPreview(headers, parsed.rows),
    };
  } catch (error: any) {
    return buildPredictionAnalysisError(
      parsed,
      ticker,
      sanitizeText(error?.message, 500) || "Prediction CSV analysis failed.",
      "Prediction CSV analysis could not resolve valid business-day boundaries."
    );
  }
}

export async function classifyUploadedCsv(
  csvText: string,
  options: { tickerHint?: string; intervalHint?: string; fetchImpl?: typeof fetch } = {}
): Promise<UploadClassificationResult> {
  const parsed = parseCsvTable(csvText, 200000);
  const previewRows = rowsToObjectPreview(parsed.headers, parsed.rows);

  const timeIndex = findPreferredColumn(parsed.headers, ["timestamp", "datetime", "date", "time"]);
  const valueIndex = findPreferredColumn(parsed.headers, ["closing_price", "closingprice", "close", "price", "adj close", "adjclose"]);

  if (timeIndex >= 0 && valueIndex >= 0) {
    const dataset = normalizeHistoricalUpload(parsed, options.tickerHint || "", options.intervalHint || "");
    return {
      kind: "historical_dataset",
      dataset,
      originalHeaders: parsed.headers,
      previewRows,
    };
  }

  const quantileColumns = extractQuantileColumns(parsed.headers);
  if (quantileColumns.length >= 3) {
    const analysis = await analyzePredictionCsv(csvText, {
      ticker: options.tickerHint,
      fetchImpl: options.fetchImpl,
    });
    return {
      kind: "prediction_output",
      analysis,
      originalHeaders: parsed.headers,
      previewRows,
    };
  }

  throw new Error(
    "Invalid CSV schema. Expected a historical dataset with item/date/close fields or a prediction output with quantile columns like P10/P50/P90."
  );
}

function buildAwsBasePrefix(userId: string, runId: string): string {
  const cleanUser = normalizeAwsNamePart(userId, 24) || "user";
  const cleanRun = normalizeAwsNamePart(runId, 24) || "run";
  return `forecast-foundry/${cleanUser}/${cleanRun}`;
}

function buildAutopilotJobName(runId: string): string {
  const suffix = Date.now().toString(36).slice(-6);
  const seed = normalizeAwsNamePart(runId, 12) || "run";
  return `${`ff-${seed}-${suffix}`.slice(0, 32)}`.replace(/-+$/g, "");
}

function buildModelName(runId: string): string {
  const suffix = Date.now().toString(36).slice(-6);
  const seed = normalizeAwsNamePart(runId, 36) || "run";
  return `${`ff-model-${seed}-${suffix}`.slice(0, 63)}`.replace(/-+$/g, "");
}

function buildTransformJobName(runId: string): string {
  const suffix = Date.now().toString(36).slice(-6);
  const seed = normalizeAwsNamePart(runId, 33) || "run";
  return `${`ff-transform-${seed}-${suffix}`.slice(0, 63)}`.replace(/-+$/g, "");
}

function summarizeBestCandidate(bestCandidateRaw: Record<string, any>): Record<string, unknown> {
  const bestCandidate = bestCandidateRaw && typeof bestCandidateRaw === "object" ? bestCandidateRaw : {};
  const finalMetric = bestCandidate.FinalAutoMLJobObjectiveMetric || {};
  const properties = bestCandidate.CandidateProperties || {};
  return {
    candidateName: sanitizeText(bestCandidate.CandidateName, 120),
    objectiveMetricName: sanitizeText(finalMetric.MetricName, 120),
    objectiveMetricValue: Number.isFinite(Number(finalMetric.Value)) ? Number(finalMetric.Value) : null,
    candidateStatus: sanitizeText(bestCandidate.CandidateStatus, 80),
    creationTime: asString(bestCandidate.CreationTime),
    endTime: asString(bestCandidate.EndTime),
    artifactLocations: properties.CandidateArtifactLocations || {},
    inferenceContainersCount: Array.isArray(bestCandidate.InferenceContainers) ? bestCandidate.InferenceContainers.length : 0,
  };
}

async function putS3Text(s3: S3Client, bucket: string, key: string, body: string, contentType: string): Promise<string> {
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: contentType,
    })
  );
  return `s3://${bucket}/${key}`;
}

async function streamToString(body: unknown): Promise<string> {
  if (!body) return "";
  if (typeof body === "string") return body;
  if (Buffer.isBuffer(body)) return body.toString("utf8");
  if (body instanceof Uint8Array) return Buffer.from(body).toString("utf8");
  if (typeof (body as any).transformToString === "function") {
    return (body as any).transformToString();
  }
  if (body instanceof Readable) {
    const chunks: Buffer[] = [];
    for await (const chunk of body) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks).toString("utf8");
  }
  return "";
}

async function readFirstTransformOutputCsv(s3: S3Client, outputS3Uri: string): Promise<{ key: string; text: string }> {
  const parsed = parseS3Uri(outputS3Uri);
  const listed = await s3.send(
    new ListObjectsV2Command({
      Bucket: parsed.bucket,
      Prefix: parsed.key,
      MaxKeys: 25,
    })
  );
  const key = (listed.Contents || [])
    .map((entry) => asString(entry.Key))
    .filter(Boolean)
    .find((entry) => entry.endsWith(".out") || entry.endsWith(".csv") || entry.endsWith(".csv.out"));
  if (!key) {
    throw new Error("Transform output file was not found in S3.");
  }
  const result = await s3.send(
    new GetObjectCommand({
      Bucket: parsed.bucket,
      Key: key,
    })
  );
  return {
    key,
    text: await streamToString(result.Body),
  };
}

function parseS3Uri(s3Uri: string): { bucket: string; key: string } {
  const match = asString(s3Uri).match(/^s3:\/\/([^/]+)\/?(.*)$/);
  if (!match) throw new Error("Invalid S3 URI.");
  return {
    bucket: match[1],
    key: match[2] || "",
  };
}

export async function startAutopilotTraining(
  input: StartAutopilotTrainingInput
): Promise<StartAutopilotTrainingResult> {
  const config = await resolveAutopilotAwsConfig();
  const s3 = createS3Client(config);
  const sagemaker = createSageMakerClient(config);
  const interval = normalizeSupportedInterval(input.interval);
  if (interval === "1m") {
    throw new Error("Minute data can be previewed and analyzed, but SageMaker Autopilot training is limited to hourly and daily datasets.");
  }
  const horizon = normalizeHorizon(input.horizon);
  const quantiles = normalizeForecastQuantiles(input.quantiles);
  const runtimeSeconds = normalizeRuntimeSeconds(input.runtimeSeconds);
  const basePrefix = buildAwsBasePrefix(input.userId, input.runId);
  const jobName = buildAutopilotJobName(input.runId);
  const inputKey = `${basePrefix}/input/${jobName}.csv`;
  const inputS3Uri = await putS3Text(s3, config.bucket, inputKey, input.csvText, "text/csv");
  const outputS3Uri = `s3://${config.bucket}/${basePrefix}/autopilot-output/`;

  const timeSeriesForecastingJobConfig: any = {
    ForecastFrequency: normalizeForecastFrequency(interval),
    ForecastHorizon: horizon,
    ForecastQuantiles: quantiles,
    TimeSeriesConfig: {
      TargetAttributeName: "closing_price",
      TimestampAttributeName: "timestamp",
      ItemIdentifierAttributeName: "item_id",
    },
    CandidateGenerationConfig: {
      AlgorithmsConfig: [{ AutoMLAlgorithms: [...AUTOPILOT_ALGORITHMS] }],
    },
  };
  if (runtimeSeconds) {
    timeSeriesForecastingJobConfig.CompletionCriteria = {
      MaxAutoMLJobRuntimeInSeconds: runtimeSeconds,
    };
  }

  const request: any = {
    AutoMLJobName: jobName,
    AutoMLJobInputDataConfig: [
      {
        ChannelType: "training",
        ContentType: "text/csv",
        DataSource: {
          S3DataSource: {
            S3DataType: "S3Prefix",
            S3Uri: inputS3Uri,
          },
        },
      },
    ],
    AutoMLJobObjective: {
      MetricName: "AverageWeightedQuantileLoss",
    },
    AutoMLProblemTypeConfig: {
      TimeSeriesForecastingJobConfig: timeSeriesForecastingJobConfig,
    },
    OutputDataConfig: {
      S3OutputPath: outputS3Uri,
    },
    RoleArn: config.roleArn,
    Tags: [
      { Key: "app", Value: "quantura" },
      { Key: "surface", Value: "forecast-foundry" },
      { Key: "ticker", Value: normalizeTicker(input.ticker) || "UNKNOWN" },
      { Key: "run_id", Value: sanitizeText(input.runId, 60) },
      { Key: "user_id", Value: sanitizeText(input.userId, 60) },
    ],
  };

  const response = await sagemaker.send(new CreateAutoMLJobV2Command(request));
  return {
    jobName,
    jobArn: asString((response as any)?.AutoMLJobArn),
    inputS3Uri,
    outputS3Uri,
    forecastFrequency: normalizeForecastFrequency(interval),
    quantiles,
    algorithms: [...AUTOPILOT_ALGORITHMS],
    runtimeSeconds,
  };
}

export async function refreshAutopilotRun(
  input: RefreshAutopilotRunInput
): Promise<RefreshAutopilotRunResult> {
  const config = await resolveAutopilotAwsConfig();
  const sagemaker = createSageMakerClient(config);
  const s3 = createS3Client(config);
  const autopilot = input.autopilot && typeof input.autopilot === "object" ? input.autopilot : {};
  const jobName = sanitizeText(autopilot.jobName, 120);
  if (!jobName) {
    throw new Error("Autopilot job name is missing.");
  }

  const described: any = await sagemaker.send(
    new DescribeAutoMLJobV2Command({
      AutoMLJobName: jobName,
    })
  );
  const jobStatus = sanitizeText(described.AutoMLJobStatus, 80);
  const bestCandidate = described.BestCandidate && typeof described.BestCandidate === "object" ? described.BestCandidate : {};
  const bestSummary = summarizeBestCandidate(bestCandidate);
  const basePatch: Record<string, unknown> = {
    jobName,
    jobArn: sanitizeText(described.AutoMLJobArn, 220),
    status: jobStatus,
    failureReason: sanitizeText(described.FailureReason, 500),
    bestCandidate: bestSummary,
    objectiveMetric: {
      name: sanitizeText((bestSummary.objectiveMetricName as string) || "AverageWeightedQuantileLoss", 120),
      value: bestSummary.objectiveMetricValue ?? null,
    },
    updatedAtIso: new Date().toISOString(),
  };

  if (jobStatus === "Failed" || jobStatus === "Stopped") {
    return {
      status: "failed",
      autopilotPatch: basePatch,
      predictionsCsvText: "",
      predictionsFileName: "",
    };
  }

  if (jobStatus !== "Completed") {
    return {
      status: jobStatus === "InProgress" ? "running" : "queued",
      autopilotPatch: basePatch,
      predictionsCsvText: "",
      predictionsFileName: "",
    };
  }

  const transformJobName = sanitizeText(autopilot.transformJobName, 120);
  const transformOutputS3Uri =
    sanitizeText(autopilot.transformOutputS3Uri, 400) ||
    `s3://${config.bucket}/${buildAwsBasePrefix(input.userId, input.runId)}/transform-output/`;

  if (!transformJobName) {
    const modelName = sanitizeText(autopilot.modelName, 120) || buildModelName(input.runId);
    const nextTransformJobName = buildTransformJobName(input.runId);
    const containers = Array.isArray((bestCandidate as any)?.InferenceContainers)
      ? (bestCandidate as any).InferenceContainers
      : [];
    if (!containers.length) {
      throw new Error("Best candidate inference containers are missing.");
    }

    try {
      await sagemaker.send(
        new CreateModelCommand({
          ModelName: modelName,
          ExecutionRoleArn: config.roleArn,
          Containers: containers,
        } as any)
      );
    } catch (error: any) {
      const message = asString(error?.message).toLowerCase();
      if (!message.includes("exists") && !message.includes("in use")) {
        throw error;
      }
    }

    try {
      await sagemaker.send(
        new CreateTransformJobCommand({
          TransformJobName: nextTransformJobName,
          ModelName: modelName,
          TransformInput: {
            DataSource: {
              S3DataSource: {
                S3DataType: "S3Prefix",
                S3Uri: input.datasetS3Uri,
              },
            },
            ContentType: "text/csv",
            SplitType: "None",
          },
          TransformOutput: {
            S3OutputPath: transformOutputS3Uri,
            AssembleWith: "Line",
          },
          TransformResources: {
            InstanceType: config.transformInstanceType,
            InstanceCount: 1,
          },
        } as any)
      );
    } catch (error: any) {
      const message = asString(error?.message).toLowerCase();
      if (!message.includes("exists") && !message.includes("in use")) {
        throw error;
      }
    }

    return {
      status: "transforming",
      autopilotPatch: {
        ...basePatch,
        modelName,
        transformJobName: nextTransformJobName,
        transformOutputS3Uri,
        transformStatus: "InProgress",
      },
      predictionsCsvText: "",
      predictionsFileName: "",
    };
  }

  const describedTransform: any = await sagemaker.send(
    new DescribeTransformJobCommand({
      TransformJobName: transformJobName,
    })
  );
  const transformStatus = sanitizeText(describedTransform.TransformJobStatus, 80);
  const transformPatch: Record<string, unknown> = {
    ...basePatch,
    modelName: sanitizeText(autopilot.modelName, 120),
    transformJobName,
    transformStatus,
    transformOutputS3Uri,
    transformFailureReason: sanitizeText(describedTransform.FailureReason, 500),
  };

  if (transformStatus === "Failed" || transformStatus === "Stopped") {
    return {
      status: "failed",
      autopilotPatch: transformPatch,
      predictionsCsvText: "",
      predictionsFileName: "",
    };
  }

  if (transformStatus !== "Completed") {
    return {
      status: "transforming",
      autopilotPatch: transformPatch,
      predictionsCsvText: "",
      predictionsFileName: "",
    };
  }

  const output = await readFirstTransformOutputCsv(s3, transformOutputS3Uri);
  return {
    status: "completed",
    autopilotPatch: {
      ...transformPatch,
      transformOutputKey: output.key,
    },
    predictionsCsvText: output.text,
    predictionsFileName: output.key.split("/").pop() || `${transformJobName}.csv.out`,
  };
}
