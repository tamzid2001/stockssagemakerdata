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
import { AlpacaClient } from "./alpacaClient";

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

export type PredictionAnalysisResult = {
  kind: "prediction_output";
  status: "ok" | "error";
  ticker: string;
  rowCount: number;
  columns: string[];
  summary: string;
  markdown: string;
  metrics: Record<string, number | string | boolean | null>;
  analysis: Record<string, unknown>;
  previewRows: Array<Record<string, string>>;
  businessDayCsvText?: string;
  alertBoundarySchedule?: Array<{
    timestamp: string;
    dateKey: string;
    values: Record<string, number>;
  }>;
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
  credentials?: () => Promise<{ accessKeyId: string; secretAccessKey: string; sessionToken?: string; expiration?: Date }>;
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
  awsConfig?: AutopilotAwsConfig;
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
  awsConfig?: AutopilotAwsConfig;
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

type PredictionObservation = {
  row: string[];
  rowIndex: number;
  parsedDate: Date;
  timestamp: string;
  values: Map<number, number | null>;
};

type AnomalyObservation = {
  rowIndex: number;
  date: string;
  value: number;
  direction: "high" | "low";
  lowerBoundary: number;
  upperBoundary: number;
};

type SeriesAnomalyStats = {
  count: number;
  average: number | null;
  minimum: number | null;
  maximum: number | null;
  standardDeviation: number | null;
  lowerBoundary: number | null;
  upperBoundary: number | null;
  lowerBoundaryAverage: number | null;
  upperBoundaryAverage: number | null;
  boundaryMethod: "statistical_95_band" | "model_95_interval";
  unusual: AnomalyObservation[];
  unusualPercentage: number;
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

function quantileBoundaryLabel(quantile: number): string {
  const percent = Number((quantile * 100).toFixed(4));
  return `P${String(percent).replace(/\.0+$/, "")}`;
}

function pad2(value: number): string {
  return String(value).padStart(2, "0");
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
  const dateMatch = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (dateMatch) {
    const [, yearText, monthText, dayText] = dateMatch;
    const year = Number(yearText);
    const month = Number(monthText);
    const day = Number(dayText);
    const parsed = new Date(Date.UTC(year, month - 1, day));
    return parsed.getUTCFullYear() === year && parsed.getUTCMonth() === month - 1 && parsed.getUTCDate() === day
      ? parsed
      : null;
  }
  const dateTimeMatch = text.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
  if (dateTimeMatch) {
    const [, yearText, monthText, dayText, hourText, minuteText, secondText] = dateTimeMatch;
    const year = Number(yearText);
    const month = Number(monthText);
    const day = Number(dayText);
    const hour = Number(hourText);
    const minute = Number(minuteText);
    const second = Number(secondText);
    const parsed = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
    return parsed.getUTCFullYear() === year && parsed.getUTCMonth() === month - 1 && parsed.getUTCDate() === day &&
      parsed.getUTCHours() === hour && parsed.getUTCMinutes() === minute && parsed.getUTCSeconds() === second
      ? parsed
      : null;
  }
  const isoDatePrefix = text.match(/^(\d{4})-(\d{2})-(\d{2})[Tt]/);
  if (isoDatePrefix) {
    const [, yearText, monthText, dayText] = isoDatePrefix;
    const calendarCheck = new Date(Date.UTC(Number(yearText), Number(monthText) - 1, Number(dayText)));
    if (
      calendarCheck.getUTCFullYear() !== Number(yearText) ||
      calendarCheck.getUTCMonth() !== Number(monthText) - 1 ||
      calendarCheck.getUTCDate() !== Number(dayText)
    ) return null;
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
    credentials: config.credentials || buildAwsCredentials(config),
  });
}

function createSageMakerClient(config: AutopilotAwsConfig): SageMakerClient {
  return new SageMakerClient({
    region: config.region,
    credentials: config.credentials || buildAwsCredentials(config),
  });
}

function parseCsvTable(csvText: string, maxRows = 200000): ParsedCsv {
  const text = asString(csvText).replace(/^\uFEFF/, "");
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

function parseQuantileHeader(header: unknown): number | null {
  const compact = asString(header)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/%/g, "")
    .replace(/^(?:forecast|prediction)[_-]?/, "")
    .replace(/^quantile[_-]?/, "q")
    .replace(/^percentile[_-]?/, "p")
    .replace(/^([pq])[_-]/, "$1");
  const match = compact.match(/^[pq](\d+(?:\.\d+)?)$/);
  if (!match) return null;
  const raw = Number(match[1]);
  if (!Number.isFinite(raw)) return null;
  const quantile = !match[1].includes(".") && match[1].length === 3
    ? raw / 1000
    : raw > 1
      ? raw / 100
      : raw;
  return quantile > 0 && quantile < 1 ? quantile : null;
}

function extractQuantileColumns(headers: string[]): QuantileColumn[] {
  const cols: QuantileColumn[] = [];
  headers.forEach((header, index) => {
    const quantile = parseQuantileHeader(header);
    if (quantile === null) return;
    cols.push({ index, header, quantile });
  });

  if (!cols.some((column) => Math.abs(column.quantile - 0.5) < 0.000001)) {
    const medianIndex = headers.findIndex((header) =>
      new Set(["median", "forecastmedian", "medianforecast", "pointforecast"]).has(normalizeCsvHeader(header))
    );
    if (medianIndex >= 0) {
      cols.push({ index: medianIndex, header: headers[medianIndex], quantile: 0.5 });
    }
  }

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
  let containsIntradayTimestamps = false;
  parsed.rows.forEach((values) => {
    const itemId = normalizeTicker(itemIdIndex >= 0 ? values[itemIdIndex] : cleanTicker);
    const parsedDate = parseFlexibleDate(values[timeIndex]);
    const closingPrice = Number(asString(values[valueIndex]).replace(/,/g, ""));
    if (!parsedDate || !Number.isFinite(closingPrice)) return;
    if (!itemId) return;
    if (
      parsedDate.getUTCHours() !== 0 ||
      parsedDate.getUTCMinutes() !== 0 ||
      parsedDate.getUTCSeconds() !== 0 ||
      /[T\s]\d{2}:\d{2}/.test(asString(values[timeIndex]).trim())
    ) {
      containsIntradayTimestamps = true;
    }
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

  if (containsIntradayTimestamps) {
    throw new Error("Forecast Foundry currently supports daily historical CSV datasets only.");
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
  rowLimit?: number | null;
  fetchImpl?: typeof fetch;
}): Promise<CanonicalDataset> {
  const ticker = normalizeTicker(input.ticker);
  const interval = normalizeSupportedInterval(input.interval);
  if (!ticker) throw new Error("Ticker is required.");
  if (interval !== "1d") {
    throw new Error("Historical downloader currently supports daily data only.");
  }
  const useAllHistory = Boolean(input.useAllHistory);
  const rowLimit = Number.isFinite(input.rowLimit)
    ? Math.max(1, Math.floor(Number(input.rowLimit)))
    : null;
  const fetchAllAvailable = useAllHistory || rowLimit !== null;
  const startDate = fetchAllAvailable ? null : parseFlexibleDate(input.start);
  const endDate = parseFlexibleDate(input.end) || new Date();
  if (!endDate || (!fetchAllAvailable && !startDate)) {
    throw new Error(fetchAllAvailable ? "End date is required." : "Start and end dates are required.");
  }
  if (startDate && endDate.getTime() < startDate.getTime()) {
    throw new Error("End date must be on or after the start date.");
  }

  const endExclusive = addUtcDays(toUtcMidnight(endDate), 1);
  const alpaca = new AlpacaClient({ fetchImpl: input.fetchImpl || fetch });
  const history = await alpaca.getStockBars({
    symbol: ticker,
    timeframe: "1Day",
    start: fetchAllAvailable ? "2016-01-01T00:00:00.000Z" : (startDate as Date).toISOString(),
    end: endExclusive.toISOString(),
    feed: "iex",
    adjustment: "all",
    session: "regular",
    limit: rowLimit || 0,
  });
  const chunkRows: CanonicalDatasetRow[] = history.rows.map((row) => ({
    item_id: ticker,
    timestamp: row.timestamp,
    closing_price: row.close,
  }));
  const sortedRows = chunkRows.slice().sort((left, right) => {
    const leftMs = parseFlexibleDate(left.timestamp)?.getTime() || 0;
    const rightMs = parseFlexibleDate(right.timestamp)?.getTime() || 0;
    return leftMs - rightMs;
  });
  const rows: CanonicalDatasetRow[] = rowLimit === null ? sortedRows : sortedRows.slice(-rowLimit);

  if (!rows.length) {
    throw new Error(`No ${interval} history rows were returned for ${ticker}.`);
  }

  return buildCanonicalDataset(rows, {
    ticker,
    interval,
    sourceTimeColumn: "timestamp",
    sourceValueColumn: "closing_price",
    sourceItemColumn: "ticker",
  });
}

function buildPredictionAnalysisError(
  parsed: ParsedCsv,
  ticker: string,
  message: string,
  summary?: string,
  details: { errors?: string[]; warnings?: string[] } = {}
): PredictionAnalysisResult {
  const cleanMessage = sanitizeText(message, 500) || "Prediction CSV analysis failed.";
  const errors = (details.errors || [cleanMessage]).map((item) => sanitizeText(item, 500)).filter(Boolean);
  const warnings = (details.warnings || []).map((item) => sanitizeText(item, 500)).filter(Boolean);
  return {
    kind: "prediction_output",
    status: "error",
    ticker,
    rowCount: parsed.rows.length,
    columns: parsed.headers,
    summary: sanitizeText(summary || cleanMessage, 500),
    markdown: [
      "## Forecast Summary",
      "",
      `**Ticker:** \`${ticker || "UNKNOWN"}\``,
      "",
      "### CSV validation failed",
      "",
      ...errors.map((item) => `- ${item}`),
      ...(warnings.length ? ["", "### Warnings", "", ...warnings.map((item) => `- ${item}`)] : []),
    ].join("\n"),
    metrics: {},
    analysis: {
      message: cleanMessage,
      validationErrors: errors,
      validationWarnings: warnings,
    },
    previewRows: rowsToObjectPreview(parsed.headers, parsed.rows),
    businessDayCsvText: "",
  };
}

function findExactQuantileColumn(columns: QuantileColumn[], target: number): QuantileColumn | null {
  return columns.find((column) => Math.abs(column.quantile - target) < 0.000001) || null;
}

function findExplicit95IntervalColumns(
  headers: string[],
  quantileColumns: QuantileColumn[]
): { lowerIndex: number; upperIndex: number; lowerHeader: string; upperHeader: string; hinted: boolean } {
  const normalized = headers.map((header) => normalizeCsvHeader(header));
  const hasSideAnd95 = (value: string, side: "lower" | "upper") => {
    const sidePattern = side === "lower" ? /(lower|low|lcl|min)/ : /(upper|high|ucl|max)/;
    return sidePattern.test(value) && /(95|095)/.test(value);
  };
  const lowerIndex = normalized.findIndex((value) => hasSideAnd95(value, "lower"));
  const upperIndex = normalized.findIndex((value) => hasSideAnd95(value, "upper"));
  if (lowerIndex >= 0 || upperIndex >= 0) {
    return {
      lowerIndex,
      upperIndex,
      lowerHeader: lowerIndex >= 0 ? headers[lowerIndex] : "",
      upperHeader: upperIndex >= 0 ? headers[upperIndex] : "",
      hinted: true,
    };
  }

  const lowerQuantile = findExactQuantileColumn(quantileColumns, 0.025);
  const upperQuantile = findExactQuantileColumn(quantileColumns, 0.975);
  return {
    lowerIndex: lowerQuantile?.index ?? -1,
    upperIndex: upperQuantile?.index ?? -1,
    lowerHeader: lowerQuantile?.header || "",
    upperHeader: upperQuantile?.header || "",
    hinted: Boolean(lowerQuantile || upperQuantile),
  };
}

function predictionSeriesStats(values: number[]): {
  average: number | null;
  minimum: number | null;
  maximum: number | null;
  standardDeviation: number | null;
} {
  if (!values.length) {
    return { average: null, minimum: null, maximum: null, standardDeviation: null };
  }
  const average = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance =
    values.length > 1
      ? values.reduce((sum, value) => sum + (value - average) ** 2, 0) / (values.length - 1)
      : 0;
  return {
    average,
    minimum: Math.min(...values),
    maximum: Math.max(...values),
    standardDeviation: Math.sqrt(Math.max(0, variance)),
  };
}

function calculateStatisticalAnomalyStats(
  observations: PredictionObservation[],
  column: QuantileColumn | null
): SeriesAnomalyStats {
  const entries = column
    ? observations
        .map((observation) => ({ observation, value: observation.values.get(column.index) }))
        .filter((entry): entry is { observation: PredictionObservation; value: number } =>
          typeof entry.value === "number" && Number.isFinite(entry.value)
        )
    : [];
  const base = predictionSeriesStats(entries.map((entry) => entry.value));
  const lowerBoundary = base.average !== null && base.standardDeviation !== null
    ? base.average - 1.96 * base.standardDeviation
    : null;
  const upperBoundary = base.average !== null && base.standardDeviation !== null
    ? base.average + 1.96 * base.standardDeviation
    : null;
  const unusual =
    lowerBoundary === null || upperBoundary === null
      ? []
      : entries
          .filter((entry) => entry.value < lowerBoundary || entry.value > upperBoundary)
          .map((entry) => ({
            rowIndex: entry.observation.rowIndex,
            date: entry.observation.timestamp,
            value: entry.value,
            direction: entry.value < lowerBoundary ? "low" as const : "high" as const,
            lowerBoundary,
            upperBoundary,
          }));
  return {
    count: entries.length,
    ...base,
    lowerBoundary,
    upperBoundary,
    lowerBoundaryAverage: lowerBoundary,
    upperBoundaryAverage: upperBoundary,
    boundaryMethod: "statistical_95_band",
    unusual,
    unusualPercentage: entries.length ? (unusual.length / entries.length) * 100 : 0,
  };
}

function calculateModelIntervalAnomalyStats(
  observations: PredictionObservation[],
  p50Column: QuantileColumn,
  lowerIndex: number,
  upperIndex: number
): SeriesAnomalyStats {
  const values = observations
    .map((observation) => observation.values.get(p50Column.index))
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  const base = predictionSeriesStats(values);
  const usable = observations
    .map((observation) => ({
      observation,
      value: observation.values.get(p50Column.index),
      lower: observation.values.get(lowerIndex),
      upper: observation.values.get(upperIndex),
    }))
    .filter((entry): entry is { observation: PredictionObservation; value: number; lower: number; upper: number } =>
      typeof entry.value === "number" && Number.isFinite(entry.value) &&
      typeof entry.lower === "number" && Number.isFinite(entry.lower) &&
      typeof entry.upper === "number" && Number.isFinite(entry.upper)
    );
  const lowerStats = predictionSeriesStats(usable.map((entry) => entry.lower));
  const upperStats = predictionSeriesStats(usable.map((entry) => entry.upper));
  const unusual = usable
    .filter((entry) => entry.value < entry.lower || entry.value > entry.upper)
    .map((entry) => ({
      rowIndex: entry.observation.rowIndex,
      date: entry.observation.timestamp,
      value: entry.value,
      direction: entry.value < entry.lower ? "low" as const : "high" as const,
      lowerBoundary: entry.lower,
      upperBoundary: entry.upper,
    }));
  return {
    count: values.length,
    ...base,
    lowerBoundary: null,
    upperBoundary: null,
    lowerBoundaryAverage: lowerStats.average,
    upperBoundaryAverage: upperStats.average,
    boundaryMethod: "model_95_interval",
    unusual,
    unusualPercentage: usable.length ? (unusual.length / usable.length) * 100 : 0,
  };
}

function nextUsEquityBusinessDay(date: Date): Date {
  let cursor = addUtcDays(toUtcMidnight(date), 1);
  while (!isUsEquityBusinessDay(cursor)) cursor = addUtcDays(cursor, 1);
  return cursor;
}

function addUsEquityBusinessDays(date: Date, count: number): Date {
  let cursor = toUtcMidnight(date);
  let remaining = Math.max(0, Math.floor(count));
  while (remaining > 0) {
    cursor = addUtcDays(cursor, 1);
    if (isUsEquityBusinessDay(cursor)) remaining -= 1;
  }
  return cursor;
}

function roundAnalysisMetric(value: number | null): number | null {
  return value === null || !Number.isFinite(value) ? null : Number(value.toFixed(6));
}

function formatAnomalyMarkdown(observations: AnomalyObservation[], label: string): string[] {
  if (!observations.length) return [`- No unusual ${label} observations were detected.`];
  const visible = observations.slice(0, 60).map(
    (item) =>
      `- **${item.date}**: ${item.value.toLocaleString(undefined, { maximumFractionDigits: 6 })} (${item.direction}; boundaries ${item.lowerBoundary.toLocaleString(undefined, { maximumFractionDigits: 6 })} to ${item.upperBoundary.toLocaleString(undefined, { maximumFractionDigits: 6 })})`
  );
  if (observations.length > visible.length) {
    visible.push(`- ${observations.length - visible.length} additional unusual observation(s) omitted from this summary.`);
  }
  return visible;
}

export async function analyzePredictionCsv(
  csvText: string,
  options: { ticker?: string; fetchImpl?: typeof fetch } = {}
): Promise<PredictionAnalysisResult> {
  let parsed: ParsedCsv;
  try {
    parsed = parseCsvTable(csvText, 50000);
  } catch (error: any) {
    throw new Error(`csv_validation: ${sanitizeText(error?.message, 500) || "Unable to parse CSV."}`);
  }

  const headers = parsed.headers || [];
  const quantileColumns = extractQuantileColumns(headers);
  const validationErrors: string[] = [];
  const validationWarnings: string[] = [];
  const rawTickerHint = asString(options.ticker).trim().toUpperCase();
  const isValidTicker = (value: string) => /^(?=.*[A-Z])[A-Z0-9^][A-Z0-9.^=\/-]{0,23}$/.test(value);
  if (rawTickerHint && !isValidTicker(rawTickerHint)) {
    validationErrors.push(`Ticker hint "${sanitizeText(rawTickerHint, 32)}" is invalid.`);
  }

  const duplicateHeaders = headers.filter(
    (header, index) => headers.findIndex((candidate) => normalizeCsvHeader(candidate) === normalizeCsvHeader(header)) !== index
  );
  if (duplicateHeaders.length) {
    validationErrors.push(`Duplicate CSV headers detected: ${Array.from(new Set(duplicateHeaders)).join(", ")}.`);
  }
  const duplicateQuantiles = quantileColumns.filter(
    (column, index) => quantileColumns.findIndex((candidate) => Math.abs(candidate.quantile - column.quantile) < 0.000001) !== index
  );
  if (duplicateQuantiles.length) {
    validationErrors.push(`Duplicate quantile levels detected: ${Array.from(new Set(duplicateQuantiles.map((column) => column.header))).join(", ")}.`);
  }
  if (!quantileColumns.length) {
    validationErrors.push("No forecast quantile columns were detected. Use headers such as P10, P50/median, P90, q0.1, or quantile_0.9.");
  }

  const p10Column = findExactQuantileColumn(quantileColumns, 0.1);
  const p50Column = findExactQuantileColumn(quantileColumns, 0.5);
  const p90Column = findExactQuantileColumn(quantileColumns, 0.9);
  if (!p50Column) validationErrors.push("P50 or median is required for median anomaly and bias analysis.");
  if (!p10Column) validationWarnings.push("P10 was not supplied; P10 anomaly and last-two-observation analysis are unavailable.");
  if (!p90Column) validationWarnings.push("P90 was not supplied; the forecast summary will omit its average.");

  const timeIndex = findPreferredColumn(headers, ["date", "ds", "datetime", "timestamp", "time"]);
  if (timeIndex < 0) validationErrors.push("A date or timestamp column is required.");
  const tickerIndex = findPreferredColumn(headers, ["ticker", "symbol", "item_id", "itemid", "asset"]);
  const actualIndex = findPreferredColumn(headers, [
    "actual_price",
    "actualprice",
    "actual",
    "observed_price",
    "observedprice",
    "observed",
    "ground_truth",
    "groundtruth",
    "target",
    "close",
    "closing_price",
  ]);
  const interval95 = findExplicit95IntervalColumns(headers, quantileColumns);
  if (interval95.hinted && (interval95.lowerIndex < 0 || interval95.upperIndex < 0)) {
    validationErrors.push("A model-provided 95% interval must include both lower and upper boundary columns.");
  }
  const hasExplicit95Interval = interval95.lowerIndex >= 0 && interval95.upperIndex >= 0;

  const numericIndexes = new Set<number>(quantileColumns.map((column) => column.index));
  if (actualIndex >= 0) numericIndexes.add(actualIndex);
  if (interval95.lowerIndex >= 0) numericIndexes.add(interval95.lowerIndex);
  if (interval95.upperIndex >= 0) numericIndexes.add(interval95.upperIndex);
  const invalidNumericCounts = new Map<number, number>();
  const missingNumericCounts = new Map<number, number>();
  const tickerValues = new Set<string>();
  let invalidTickerCount = 0;
  let missingTickerCount = 0;
  let invalidDateCount = 0;

  const observations: PredictionObservation[] = [];
  parsed.rows.forEach((row, rowIndex) => {
    const parsedDate = timeIndex >= 0 ? parseFlexibleDate(row[timeIndex]) : null;
    if (!parsedDate) {
      invalidDateCount += 1;
    }
    const values = new Map<number, number | null>();
    numericIndexes.forEach((index) => {
      const raw = asString(row[index]).trim();
      if (!raw) {
        values.set(index, null);
        missingNumericCounts.set(index, (missingNumericCounts.get(index) || 0) + 1);
        return;
      }
      const numeric = Number(raw.replace(/,/g, ""));
      if (!Number.isFinite(numeric)) {
        values.set(index, null);
        invalidNumericCounts.set(index, (invalidNumericCounts.get(index) || 0) + 1);
        return;
      }
      values.set(index, numeric);
    });

    if (tickerIndex >= 0) {
      const rawTicker = asString(row[tickerIndex]).trim().toUpperCase();
      if (!rawTicker) missingTickerCount += 1;
      else if (!isValidTicker(rawTicker)) invalidTickerCount += 1;
      else tickerValues.add(rawTicker);
    }
    if (parsedDate) {
      observations.push({
        row,
        rowIndex,
        parsedDate,
        timestamp: formatAnalysisTimestamp(parsedDate.toISOString()),
        values,
      });
    }
  });

  if (invalidDateCount) validationErrors.push(`${invalidDateCount} row(s) contain an invalid or missing date/timestamp.`);
  invalidNumericCounts.forEach((count, index) => {
    validationErrors.push(`${count} non-numeric value(s) found in "${headers[index] || `column ${index + 1}`}".`);
  });
  missingNumericCounts.forEach((count, index) => {
    validationWarnings.push(`${count} missing value(s) found in "${headers[index] || `column ${index + 1}`}"; available numeric rows were analyzed.`);
  });
  if (invalidTickerCount) validationErrors.push(`${invalidTickerCount} row(s) contain an invalid ticker.`);
  if (missingTickerCount) validationWarnings.push(`${missingTickerCount} row(s) have no ticker; ticker is optional when the upload form supplies one.`);
  if (tickerValues.size > 1) validationErrors.push("Prediction analysis supports one ticker per CSV; multiple tickers were detected.");

  const tickerFromRows = Array.from(tickerValues)[0] || "";
  if (rawTickerHint && tickerFromRows && rawTickerHint !== tickerFromRows) {
    validationErrors.push(`Ticker hint ${rawTickerHint} does not match CSV ticker ${tickerFromRows}.`);
  }
  const ticker = normalizeTicker(tickerFromRows || rawTickerHint);

  const originalOrder = observations.map((observation) => observation.parsedDate.getTime());
  observations.sort((left, right) => left.parsedDate.getTime() - right.parsedDate.getTime());
  const chronological = originalOrder.every((timestamp, index) => timestamp === observations[index]?.parsedDate.getTime());
  if (!chronological) validationWarnings.push("Rows were not chronological and were sorted from oldest to newest for analysis and export.");
  const duplicateDateCount = observations.reduce((count, observation, index) =>
    index > 0 && observation.parsedDate.getTime() === observations[index - 1].parsedDate.getTime() ? count + 1 : count
  , 0);
  if (duplicateDateCount) validationErrors.push(`${duplicateDateCount} duplicate date/timestamp row(s) were detected.`);

  let quantileOrderViolationCount = 0;
  let intervalOrderViolationCount = 0;
  observations.forEach((observation) => {
    const rowQuantiles = quantileColumns
      .map((column) => ({ column, value: observation.values.get(column.index) }))
      .filter((entry): entry is { column: QuantileColumn; value: number } =>
        typeof entry.value === "number" && Number.isFinite(entry.value)
      );
    if (rowQuantiles.some((entry, index) => index > 0 && entry.value < rowQuantiles[index - 1].value)) {
      quantileOrderViolationCount += 1;
    }
    if (hasExplicit95Interval) {
      const lower = observation.values.get(interval95.lowerIndex);
      const upper = observation.values.get(interval95.upperIndex);
      if (typeof lower === "number" && typeof upper === "number" && lower > upper) intervalOrderViolationCount += 1;
    }
  });
  if (quantileOrderViolationCount) {
    validationErrors.push(`${quantileOrderViolationCount} row(s) violate ascending quantile order (for example P10 ≤ P50 ≤ P90).`);
  }
  if (intervalOrderViolationCount) {
    validationErrors.push(`${intervalOrderViolationCount} row(s) have a 95% interval lower boundary above the upper boundary.`);
  }

  if (validationErrors.length) {
    return buildPredictionAnalysisError(
      parsed,
      ticker,
      validationErrors[0],
      "Prediction CSV validation found issues that must be corrected before anomaly analysis can run.",
      { errors: validationErrors, warnings: validationWarnings }
    );
  }

  const p50Stats = hasExplicit95Interval && p50Column
    ? calculateModelIntervalAnomalyStats(observations, p50Column, interval95.lowerIndex, interval95.upperIndex)
    : calculateStatisticalAnomalyStats(observations, p50Column);
  const p10Stats = calculateStatisticalAnomalyStats(observations, p10Column);
  const p90Stats = calculateStatisticalAnomalyStats(observations, p90Column);
  if (!p50Stats.count) {
    return buildPredictionAnalysisError(
      parsed,
      ticker,
      "P50/median contains no valid numeric values.",
      "Prediction CSV analysis could not calculate the P50 series.",
      { errors: ["P50/median contains no valid numeric values."], warnings: validationWarnings }
    );
  }

  const unusualAboveAverage = p50Stats.average === null
    ? 0
    : p50Stats.unusual.filter((item) => item.value > p50Stats.average!).length;
  const unusualBelowAverage = p50Stats.average === null
    ? 0
    : p50Stats.unusual.filter((item) => item.value < p50Stats.average!).length;
  const unusualTotal = p50Stats.unusual.length;
  const unusualAbovePercentage = unusualTotal ? (unusualAboveAverage / unusualTotal) * 100 : 0;
  const unusualBelowPercentage = unusualTotal ? (unusualBelowAverage / unusualTotal) * 100 : 0;
  const generalBias = unusualAbovePercentage > 50
    ? "Selling Bias"
    : unusualBelowPercentage > 50
      ? "Buying Bias"
      : "Neutral / Mixed";
  const biasExplanation = generalBias === "Selling Bias"
    ? "The forecast's unusual median observations are predominantly elevated relative to the model's typical median forecast level."
    : generalBias === "Buying Bias"
      ? "The forecast's unusual median observations are predominantly depressed relative to the model's typical median forecast level."
      : "Unusual median observations do not show a majority above or below the model's typical median forecast level.";

  const finalTwo = observations.slice(-2);
  const finalTwoP10Values = p10Column
    ? finalTwo.map((observation) => observation.values.get(p10Column.index))
    : [];
  const lastTwoP10Valid = Boolean(
    p10Column && finalTwo.length === 2 && finalTwoP10Values.every((value) => typeof value === "number" && Number.isFinite(value))
  );
  const lastTwoBusinessDays = finalTwo.length === 2 && finalTwo.every((observation) => isUsEquityBusinessDay(observation.parsedDate));
  const unusuallyLowP10Rows = new Set(
    p10Stats.unusual.filter((observation) => observation.direction === "low").map((observation) => observation.rowIndex)
  );
  const lastTwoP10UnusuallyLow = Boolean(
    lastTwoP10Valid && finalTwo.every((observation) => unusuallyLowP10Rows.has(observation.rowIndex))
  );
  const extendedP10BuyBiasActive = lastTwoP10Valid && lastTwoBusinessDays && lastTwoP10UnusuallyLow;
  const forecastStartDate = observations[0]?.timestamp || "";
  const forecastEndDate = observations[observations.length - 1]?.timestamp || "";
  const horizonEnd = observations[observations.length - 1]?.parsedDate || null;
  const biasWindowStartDate = horizonEnd ? formatDateYmd(nextUsEquityBusinessDay(horizonEnd)) : "";
  const biasWindowEndDate = horizonEnd ? formatDateYmd(addUsEquityBusinessDays(horizonEnd, 10)) : "";

  const businessDayRows = observations.filter((observation) => isUsEquityBusinessDay(observation.parsedDate));
  const businessDayCsvText = serializeCsv(headers, businessDayRows.map((observation) => observation.row));
  const alertBoundarySchedule = observations.map((observation) => {
    const values: Record<string, number> = {};
    quantileColumns.forEach((column) => {
      const value = observation.values.get(column.index);
      if (typeof value === "number" && Number.isFinite(value)) {
        values[quantileBoundaryLabel(column.quantile)] = value;
      }
    });
    return {
      timestamp: observation.parsedDate.toISOString(),
      dateKey: formatDateYmd(observation.parsedDate),
      values,
    };
  });
  const boundaryDescription = p50Stats.boundaryMethod === "model_95_interval"
    ? `Supplied model 95% interval (${interval95.lowerHeader} to ${interval95.upperHeader})`
    : "P50 95% Statistical Anomaly Band (mean ± 1.96 sample standard deviations)";
  const summary = `${ticker || "Uploaded forecast"} contains ${observations.length.toLocaleString()} chronological observation(s). ${p50Stats.unusual.length} P50 observation(s) are unusual under the ${boundaryDescription}. General Bias: ${generalBias}.${extendedP10BuyBiasActive ? " Extended P10 Buy Bias is active." : ""}`;

  const markdown = [
    "## Forecast Summary",
    "",
    `**Ticker:** \`${ticker || "Not supplied"}\``,
    `**Forecast Horizon:** **${forecastStartDate} → ${forecastEndDate}**`,
    `**Rows analyzed:** **${observations.length}** (${businessDayRows.length} U.S. equity business-day rows)`,
    `**P10 Average:** **${p10Stats.average === null ? "N/A" : p10Stats.average.toFixed(6)}**`,
    `**P50 Average:** **${p50Stats.average === null ? "N/A" : p50Stats.average.toFixed(6)}**`,
    `**P90 Average:** **${p90Stats.average === null ? "N/A" : p90Stats.average.toFixed(6)}**`,
    "",
    "### Median Anomalies",
    "",
    `- Method: **${boundaryDescription}**`,
    `- P50 minimum / maximum: **${p50Stats.minimum?.toFixed(6)} / ${p50Stats.maximum?.toFixed(6)}**`,
    `- P50 sample standard deviation: **${p50Stats.standardDeviation?.toFixed(6)}**`,
    p50Stats.boundaryMethod === "model_95_interval"
      ? `- Average supplied lower / upper boundaries: **${p50Stats.lowerBoundaryAverage?.toFixed(6)} / ${p50Stats.upperBoundaryAverage?.toFixed(6)}** (each row is evaluated against its own supplied interval)`
      : `- Lower / upper anomaly boundaries: **${p50Stats.lowerBoundary?.toFixed(6)} / ${p50Stats.upperBoundary?.toFixed(6)}**`,
    `- Unusual P50 observations: **${unusualTotal} (${p50Stats.unusualPercentage.toFixed(2)}%)**`,
    `- Above P50 average: **${unusualAboveAverage} (${unusualAbovePercentage.toFixed(2)}%)**`,
    `- Below P50 average: **${unusualBelowAverage} (${unusualBelowPercentage.toFixed(2)}%)**`,
    `- General Bias: **${generalBias}**`,
    `- Interpretation: ${biasExplanation}`,
    "",
    ...formatAnomalyMarkdown(p50Stats.unusual, "P50"),
    "",
    "### P10 Anomalies",
    "",
    `- P10 average: **${p10Stats.average === null ? "N/A" : p10Stats.average.toFixed(6)}**`,
    `- P10 minimum / maximum: **${p10Stats.minimum === null ? "N/A" : p10Stats.minimum.toFixed(6)} / ${p10Stats.maximum === null ? "N/A" : p10Stats.maximum.toFixed(6)}**`,
    `- P10 sample standard deviation: **${p10Stats.standardDeviation === null ? "N/A" : p10Stats.standardDeviation.toFixed(6)}**`,
    `- P10 statistical anomaly boundaries: **${p10Stats.lowerBoundary === null ? "N/A" : p10Stats.lowerBoundary.toFixed(6)} / ${p10Stats.upperBoundary === null ? "N/A" : p10Stats.upperBoundary.toFixed(6)}**`,
    `- Unusual P10 observations: **${p10Stats.unusual.length} (${p10Stats.unusualPercentage.toFixed(2)}%)**`,
    "",
    ...formatAnomalyMarkdown(p10Stats.unusual, "P10"),
    "",
    "### Tail Signal",
    "",
    `- Final two P10 values valid: **${lastTwoP10Valid ? "Yes" : "No"}**`,
    `- Both final observations are U.S. equity business days: **${lastTwoBusinessDays ? "Yes" : "No"}**`,
    `- Both final P10 values are unusually low: **${lastTwoP10UnusuallyLow ? "Yes" : "No"}**`,
    `- Extended P10 Buy Bias: **${extendedP10BuyBiasActive ? "Active" : "Inactive"}**`,
    extendedP10BuyBiasActive
      ? "- Two unusually low P10 forecasts occur on the final two business-day observations."
      : "- The special last-two-P10 rule was not fully satisfied.",
    `- Model forecast end date: **${forecastEndDate}**`,
    `- First business day after the forecast: **${biasWindowStartDate || "N/A"}**`,
    `- Model-derived buy-bias window: **${biasWindowStartDate || "N/A"} → ${biasWindowEndDate || "N/A"}** (approximately two weeks after the forecast horizon)`,
    "",
    "This is a model-derived statistical heuristic, not a guaranteed trading outcome or financial advice.",
    ...(validationWarnings.length ? ["", "### Parsing Notes", "", ...validationWarnings.map((warning) => `- ${warning}`)] : []),
  ].join("\n");

  return {
    kind: "prediction_output",
    status: "ok",
    ticker,
    rowCount: parsed.rows.length,
    columns: headers,
    summary,
    markdown,
    metrics: {
      forecastStartDate,
      forecastEndDate,
      observationCount: observations.length,
      businessDayRows: businessDayRows.length,
      p10Average: roundAnalysisMetric(p10Stats.average),
      p10Minimum: roundAnalysisMetric(p10Stats.minimum),
      p10Maximum: roundAnalysisMetric(p10Stats.maximum),
      p10StandardDeviation: roundAnalysisMetric(p10Stats.standardDeviation),
      p10LowerBoundary: roundAnalysisMetric(p10Stats.lowerBoundary),
      p10UpperBoundary: roundAnalysisMetric(p10Stats.upperBoundary),
      p10UnusualCount: p10Stats.unusual.length,
      p10UnusualPercentage: roundAnalysisMetric(p10Stats.unusualPercentage),
      p50Average: roundAnalysisMetric(p50Stats.average),
      p50Minimum: roundAnalysisMetric(p50Stats.minimum),
      p50Maximum: roundAnalysisMetric(p50Stats.maximum),
      p50StandardDeviation: roundAnalysisMetric(p50Stats.standardDeviation),
      p50BoundaryMethod: p50Stats.boundaryMethod,
      p50LowerBoundary: roundAnalysisMetric(p50Stats.lowerBoundary),
      p50UpperBoundary: roundAnalysisMetric(p50Stats.upperBoundary),
      p50ModelLowerBoundaryAverage: roundAnalysisMetric(p50Stats.lowerBoundaryAverage),
      p50ModelUpperBoundaryAverage: roundAnalysisMetric(p50Stats.upperBoundaryAverage),
      p50UnusualCount: unusualTotal,
      p50UnusualPercentage: roundAnalysisMetric(p50Stats.unusualPercentage),
      p50UnusualAboveAverageCount: unusualAboveAverage,
      p50UnusualAboveAveragePercentage: roundAnalysisMetric(unusualAbovePercentage),
      p50UnusualBelowAverageCount: unusualBelowAverage,
      p50UnusualBelowAveragePercentage: roundAnalysisMetric(unusualBelowPercentage),
      p90Average: roundAnalysisMetric(p90Stats.average),
      generalBias,
      explicit95Interval: hasExplicit95Interval,
      lastTwoP10Valid,
      lastTwoBusinessDays,
      lastTwoP10UnusuallyLow,
      extendedP10BuyBiasActive,
      biasWindowStartDate,
      biasWindowEndDate,
    },
    analysis: {
      methodology: p50Stats.boundaryMethod,
      quantileColumns: quantileColumns.map((column) => ({
        header: column.header,
        quantile: column.quantile,
      })),
      timeColumn: headers[timeIndex],
      tickerColumn: tickerIndex >= 0 ? headers[tickerIndex] : "",
      actualPriceColumn: actualIndex >= 0 ? headers[actualIndex] : "",
      interval95LowerColumn: interval95.lowerHeader,
      interval95UpperColumn: interval95.upperHeader,
      validationErrors: [],
      validationWarnings,
      p50Anomalies: p50Stats.unusual,
      p10Anomalies: p10Stats.unusual,
      generalBias,
      biasExplanation,
      lastTwoP10: {
        values: finalTwoP10Values.map((value) => typeof value === "number" ? value : null),
        dates: finalTwo.map((observation) => observation.timestamp),
        valid: lastTwoP10Valid,
        businessDays: lastTwoBusinessDays,
        unusuallyLow: lastTwoP10UnusuallyLow,
        active: extendedP10BuyBiasActive,
        windowStart: biasWindowStartDate,
        windowEnd: biasWindowEndDate,
      },
    },
    previewRows: rowsToObjectPreview(headers, observations.map((observation) => observation.row)),
    businessDayCsvText,
    alertBoundarySchedule,
  };
}

export async function classifyUploadedCsv(
  csvText: string,
  options: { tickerHint?: string; intervalHint?: string; fetchImpl?: typeof fetch } = {}
): Promise<UploadClassificationResult> {
  const parsed = parseCsvTable(csvText, 200000);
  const previewRows = rowsToObjectPreview(parsed.headers, parsed.rows);
  const quantileColumns = extractQuantileColumns(parsed.headers);

  // Quantiles are the most specific schema signal. Check them before generic
  // historical value names such as `close`, which may be an optional actual
  // price alongside P10/P50/P90 in a prediction export.
  if (quantileColumns.length >= 1) {
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

  throw new Error(
    "csv_validation: Invalid CSV schema. Expected a historical dataset with item/date/close fields or a prediction output with quantile columns such as P10, P50/median, or P90."
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
  const config = input.awsConfig || await resolveAutopilotAwsConfig();
  const s3 = createS3Client(config);
  const sagemaker = createSageMakerClient(config);
  const interval = normalizeSupportedInterval(input.interval);
  if (interval !== "1d") {
    throw new Error("Forecast Foundry currently supports daily historical datasets only.");
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
  const config = input.awsConfig || await resolveAutopilotAwsConfig();
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
