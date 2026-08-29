import crypto from "crypto";

export type HistoryInterval = "1d" | "1h";

type JsonRecord = Record<string, unknown>;

type QuickQuoteRow = {
  symbol: string;
  shortName: string;
  longName: string;
  region: string;
  exchange: string;
  fullExchangeName: string;
  marketCap: number | null;
  price: number | null;
  previousClose: number | null;
  changePercent: number | null;
  volume: number | null;
  averageVolume: number | null;
  trailingPe: number | null;
  forwardPe: number | null;
  priceToBook: number | null;
  dividendYieldPct: number | null;
  analystRecommendationMean: number | null;
  sharesOutstanding: number | null;
  earningsDate: string;
  marketCapLabel: string;
};

export type HistoryResponse = {
  ticker: string;
  interval: HistoryInterval;
  rows: Array<Record<string, unknown>>;
  source: string;
  actualStart: string;
  actualEnd: string;
  clipped: boolean;
};

export type ForecastRunResponse = {
  ticker: string;
  interval: HistoryInterval;
  horizon: number;
  quantiles: number[];
  forecastRows: Array<Record<string, unknown>>;
  forecastPreview: Array<Record<string, unknown>>;
  forecastQuantilesEnd: Record<string, number>;
  metrics: Record<string, unknown>;
  serviceMessage: string;
  tradeRationale: string;
  historyRows: Array<Record<string, unknown>>;
  engine: string;
};

export type ScreenerRunResponse = {
  title: string;
  results: Array<Record<string, unknown>>;
  serviceMessage: string;
  resultsFound: number;
  topSymbols: string[];
  appliedFilters: string[];
  ignoredFilters: string[];
};

export type MarketDataScreenerResponse = {
  items: Array<Record<string, unknown>>;
  mode: string;
  serviceMessage: string;
};

export const META_PROPHET_FORECAST_QUANTILES = Object.freeze([0.01, 0.25, 0.5, 0.75, 0.99]);

const DAY_MS = 24 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;
const DEFAULT_TIMEOUT_MS = 12000;
const DEFAULT_USER_AGENT = "QuanturaApi/1.0";
const HOURLY_RETENTION_DAYS = 729;
const DAILY_DEFAULT_LOOKBACK_DAYS = 730;
const HOURLY_DEFAULT_LOOKBACK_DAYS = 120;
const HISTORY_SYMBOL_LIMIT = 36;
const LIST_CACHE_TTL_MS = 6 * 60 * 60 * 1000;

const COMMON_STOPWORDS = new Set([
  "AI",
  "AND",
  "ARE",
  "BUT",
  "FOR",
  "FROM",
  "HOLD",
  "LONG",
  "NOT",
  "NOW",
  "RUN",
  "SELL",
  "SHORT",
  "THE",
  "THIS",
  "WEEK",
  "WITH",
]);

let listedUniverseCache: { expiresAtMs: number; value: Array<{ symbol: string; exchange: string; securityName: string }> } | null = null;
const historyCache = new Map<string, { expiresAtMs: number; value: HistoryResponse }>();

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function asFinite(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function sanitizeText(value: unknown, maxLen = 600): string {
  const raw = asString(value).replace(/\s+/g, " ").trim();
  if (!raw) return "";
  return raw.slice(0, maxLen);
}

function normalizeTicker(value: unknown): string {
  const raw = asString(value).trim().toUpperCase();
  if (!raw) return "";
  return raw.replace(/[^A-Z0-9.\-]/g, "").slice(0, 12);
}

function extractYahooFieldValue(value: unknown): unknown {
  if (!value || typeof value !== "object") return value;
  const record = value as JsonRecord;
  if (record.raw !== undefined && record.raw !== null) return record.raw;
  if (record.fmt !== undefined && record.fmt !== null) return record.fmt;
  if (record.longFmt !== undefined && record.longFmt !== null) return record.longFmt;
  return value;
}

function extractYahooNumber(value: unknown): number | null {
  const parsed = Number(extractYahooFieldValue(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function toJsonRecord(value: unknown): JsonRecord {
  return value && typeof value === "object" ? (value as JsonRecord) : {};
}

function normalizeHistoryInterval(value: unknown): HistoryInterval {
  return String(value || "").trim().toLowerCase() === "1h" ? "1h" : "1d";
}

function parseFlexibleDate(value: unknown): Date | null {
  const raw = sanitizeText(value, 40);
  if (!raw) return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function isDateOnlyString(value: unknown): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(sanitizeText(value, 40));
}

function formatDayKey(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function formatHourKey(date: Date): string {
  return `${date.toISOString().slice(0, 13)}:00:00Z`;
}

function formatMarketCap(value: number | null): string {
  if (!Number.isFinite(value as number)) return "—";
  const numeric = Number(value);
  if (numeric >= 1_000_000_000_000) return `$${(numeric / 1_000_000_000_000).toFixed(2)}T`;
  if (numeric >= 1_000_000_000) return `$${(numeric / 1_000_000_000).toFixed(2)}B`;
  if (numeric >= 1_000_000) return `$${(numeric / 1_000_000).toFixed(2)}M`;
  return `$${numeric.toFixed(0)}`;
}

async function fetchTextWithTimeout(url: string, timeoutMs = DEFAULT_TIMEOUT_MS): Promise<string> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "text/plain, text/csv, text/html, application/json;q=0.9",
        "User-Agent": DEFAULT_USER_AGENT,
      },
    });
    if (!response.ok) throw new Error(`Upstream request failed (${response.status}).`);
    return await response.text();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchJsonWithTimeout(url: string, timeoutMs = DEFAULT_TIMEOUT_MS): Promise<unknown> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        "User-Agent": DEFAULT_USER_AGENT,
      },
    });
    if (!response.ok) throw new Error(`Upstream request failed (${response.status}).`);
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

function chunk<T>(items: T[], size = 25): T[][] {
  const out: T[][] = [];
  const step = Math.max(1, Math.floor(size));
  for (let index = 0; index < items.length; index += step) {
    out.push(items.slice(index, index + step));
  }
  return out;
}

async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  worker: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const concurrency = Math.max(1, Math.floor(limit));
  const results: R[] = new Array(items.length);
  let cursor = 0;
  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }).map(async () => {
      while (true) {
        const index = cursor;
        cursor += 1;
        if (index >= items.length) break;
        results[index] = await worker(items[index], index);
      }
    })
  );
  return results;
}

function hashScore(text: string): number {
  const digest = crypto.createHash("sha1").update(text).digest();
  return digest.readUInt32BE(0);
}

function parseListedText(
  rawText: string,
  source: "nasdaq" | "other"
): Array<{ symbol: string; exchange: string; securityName: string }> {
  const lines = String(rawText || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return [];
  const header = lines[0].split("|").map((part) => part.trim());
  const rows = lines.slice(1);
  const out: Array<{ symbol: string; exchange: string; securityName: string }> = [];
  rows.forEach((line) => {
    const cols = line.split("|");
    if (!cols.length) return;
    const record: Record<string, string> = {};
    header.forEach((name, index) => {
      record[name] = String(cols[index] || "").trim();
    });
    const symbol = normalizeTicker(source === "nasdaq" ? record.Symbol : record["ACT Symbol"] || record.Symbol);
    if (!symbol || symbol === "FILE") return;
    const securityName = sanitizeText(record["Security Name"] || record.Name || "", 220);
    const etf = String(record.ETF || "").trim().toUpperCase() === "Y";
    const testIssue = String(record["Test Issue"] || "").trim().toUpperCase() === "Y";
    if (etf || testIssue) return;
    if (/\b(unit|units|right|rights|warrant|warrants|preferred|depositary|notes)\b/i.test(securityName)) return;
    const exchangeCode = source === "nasdaq" ? "NASDAQ" : String(record.Exchange || "").trim().toUpperCase();
    const exchange =
      exchangeCode === "N"
        ? "NYSE"
        : exchangeCode === "A"
          ? "AMEX"
          : exchangeCode === "P"
            ? "NYSE ARCA"
            : exchangeCode === "Z"
              ? "BATS"
              : exchangeCode || "NASDAQ";
    out.push({ symbol, exchange, securityName });
  });
  return out;
}

async function fetchListedUniverse(): Promise<Array<{ symbol: string; exchange: string; securityName: string }>> {
  if (listedUniverseCache && listedUniverseCache.expiresAtMs > Date.now()) {
    return listedUniverseCache.value;
  }
  const [nasdaqText, otherText] = await Promise.all([
    fetchTextWithTimeout("https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"),
    fetchTextWithTimeout("https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"),
  ]);
  const combined = [...parseListedText(nasdaqText, "nasdaq"), ...parseListedText(otherText, "other")];
  const unique = Array.from(new Map(combined.map((row) => [row.symbol, row])).values()).sort((left, right) => {
    const delta = hashScore(left.symbol) - hashScore(right.symbol);
    if (delta !== 0) return delta;
    return left.symbol.localeCompare(right.symbol);
  });
  listedUniverseCache = {
    expiresAtMs: Date.now() + LIST_CACHE_TTL_MS,
    value: unique,
  };
  return unique;
}

async function fetchTrendingSymbols(region = "US", limit = 24): Promise<string[]> {
  const url = `https://query1.finance.yahoo.com/v1/finance/trending/${encodeURIComponent(region.toUpperCase())}`;
  const payload = toJsonRecord(await fetchJsonWithTimeout(url, 7000).catch(() => ({})));
  const result = Array.isArray((payload.finance as any)?.result) ? (payload.finance as any).result[0] : null;
  const quotes = Array.isArray(result?.quotes) ? result.quotes : [];
  const symbols = quotes
    .map((row: JsonRecord) => normalizeTicker(row.symbol))
    .filter((symbol: string): symbol is string => Boolean(symbol));
  return Array.from(new Set<string>(symbols)).slice(0, Math.max(1, Math.min(40, Math.floor(limit))));
}

function formatEpochDate(value: unknown): string {
  const numeric = Number(extractYahooFieldValue(value));
  if (!Number.isFinite(numeric) || numeric <= 0) return "";
  const date = new Date(numeric * 1000);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString().slice(0, 10);
}

function parseAnalystRecommendation(value: unknown): number | null {
  const raw = sanitizeText(extractYahooFieldValue(value), 80);
  const match = raw.match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;
  const parsed = Number(match[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

function toQuickQuoteRow(entry: JsonRecord): QuickQuoteRow {
  const symbol = normalizeTicker(entry.symbol);
  const price = extractYahooNumber(entry.regularMarketPrice);
  const previousClose = extractYahooNumber(entry.regularMarketPreviousClose);
  const changePercent = extractYahooNumber(entry.regularMarketChangePercent);
  const marketCap = extractYahooNumber(entry.marketCap);
  const volume = extractYahooNumber(entry.regularMarketVolume);
  const averageVolume = extractYahooNumber(entry.averageDailyVolume3Month);
  const dividendYieldRaw = extractYahooNumber(entry.dividendYield);
  return {
    symbol,
    shortName: sanitizeText(entry.shortName, 180),
    longName: sanitizeText(entry.longName, 180),
    region: sanitizeText(entry.region, 20).toLowerCase(),
    exchange: sanitizeText(entry.exchange, 40),
    fullExchangeName: sanitizeText(entry.fullExchangeName, 80),
    marketCap,
    price,
    previousClose,
    changePercent,
    volume,
    averageVolume,
    trailingPe: extractYahooNumber(entry.trailingPE),
    forwardPe: extractYahooNumber(entry.forwardPE),
    priceToBook: extractYahooNumber(entry.priceToBook),
    dividendYieldPct:
      dividendYieldRaw !== null ? (dividendYieldRaw > 1 ? dividendYieldRaw : Number((dividendYieldRaw * 100).toFixed(4))) : null,
    analystRecommendationMean: parseAnalystRecommendation(entry.averageAnalystRating),
    sharesOutstanding: extractYahooNumber(entry.sharesOutstanding),
    earningsDate: formatEpochDate(entry.earningsTimestampStart || entry.earningsTimestamp || entry.earningsTimestampEnd),
    marketCapLabel: formatMarketCap(marketCap),
  };
}

function dedupeQuickQuotes(rows: QuickQuoteRow[]): QuickQuoteRow[] {
  const bySymbol = new Map<string, QuickQuoteRow>();
  rows.forEach((row) => {
    if (!row.symbol) return;
    const existing = bySymbol.get(row.symbol);
    if (!existing) {
      bySymbol.set(row.symbol, row);
      return;
    }
    const nextScore =
      (Number.isFinite(row.marketCap as number) ? 4 : 0) +
      (Number.isFinite(row.price as number) ? 2 : 0) +
      (Number.isFinite(row.volume as number) ? 1 : 0);
    const existingScore =
      (Number.isFinite(existing.marketCap as number) ? 4 : 0) +
      (Number.isFinite(existing.price as number) ? 2 : 0) +
      (Number.isFinite(existing.volume as number) ? 1 : 0);
    if (nextScore >= existingScore) {
      bySymbol.set(row.symbol, { ...existing, ...row });
    }
  });
  return Array.from(bySymbol.values());
}

async function fetchPredefinedScreenerQuotes(scrId: string, count = 60): Promise<QuickQuoteRow[]> {
  const cleanId = sanitizeText(scrId, 80).toLowerCase();
  if (!cleanId) return [];
  const size = Math.max(1, Math.min(250, Math.floor(count)));
  const url = `https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?count=${size}&scrIds=${encodeURIComponent(cleanId)}`;
  const payload = toJsonRecord(await fetchJsonWithTimeout(url, 9000).catch(() => ({})));
  const result = Array.isArray((payload.finance as any)?.result) ? (payload.finance as any).result[0] : null;
  const quotes = Array.isArray(result?.quotes) ? result.quotes : [];
  return quotes
    .map((entry: JsonRecord) => toQuickQuoteRow(entry))
    .filter((row: QuickQuoteRow) => Boolean(row.symbol));
}

function buildCandidateScreenerIds(universe: string): string[] {
  if (universe === "small-cap") {
    return ["aggressive_small_caps", "small_cap_gainers", "most_shorted_stocks", "day_gainers"];
  }
  if (universe === "mid-cap") {
    return ["day_gainers", "most_actives", "undervalued_growth_stocks", "most_shorted_stocks"];
  }
  if (universe === "large-cap") {
    return ["undervalued_growth_stocks", "growth_technology_stocks", "most_actives", "day_gainers"];
  }
  return ["most_actives", "day_gainers", "day_losers", "growth_technology_stocks"];
}

function formatHistoryKey(date: Date, interval: HistoryInterval): string {
  return interval === "1h" ? formatHourKey(date) : formatDayKey(date);
}

function normalizeHistoryWindow(input: {
  interval: HistoryInterval;
  start?: unknown;
  end?: unknown;
}): { startDate: Date; endDate: Date; clipped: boolean } {
  const interval = normalizeHistoryInterval(input.interval);
  const now = new Date();
  const fallbackStart =
    interval === "1h"
      ? new Date(now.getTime() - HOURLY_DEFAULT_LOOKBACK_DAYS * DAY_MS)
      : new Date(now.getTime() - DAILY_DEFAULT_LOOKBACK_DAYS * DAY_MS);
  const parsedStart = parseFlexibleDate(input.start) || fallbackStart;
  const parsedEnd = parseFlexibleDate(input.end) || now;
  const endDate = parsedEnd.getTime() > now.getTime() ? now : parsedEnd;
  let startDate = parsedStart;
  let clipped = false;
  if (interval === "1h") {
    const earliest = new Date(now.getTime() - HOURLY_RETENTION_DAYS * DAY_MS);
    if (startDate.getTime() < earliest.getTime()) {
      startDate = earliest;
      clipped = true;
    }
  }
  if (startDate.getTime() >= endDate.getTime()) {
    startDate = interval === "1h" ? new Date(endDate.getTime() - 14 * DAY_MS) : new Date(endDate.getTime() - 120 * DAY_MS);
    clipped = true;
  }
  return { startDate, endDate, clipped };
}

export async function fetchYahooHistoryBars(input: {
  ticker: unknown;
  interval?: unknown;
  start?: unknown;
  end?: unknown;
  timeoutMs?: number;
}): Promise<HistoryResponse> {
  const ticker = normalizeTicker(input.ticker);
  const interval = normalizeHistoryInterval(input.interval);
  if (!ticker) throw new Error("Ticker is required.");
  const cacheKey = `${ticker}:${interval}:${sanitizeText(input.start, 40)}:${sanitizeText(input.end, 40)}`;
  const cached = historyCache.get(cacheKey);
  if (cached && cached.expiresAtMs > Date.now()) return cached.value;

  const { startDate, endDate, clipped } = normalizeHistoryWindow({
    interval,
    start: input.start,
    end: input.end,
  });
  const startSeconds = Math.floor(startDate.getTime() / 1000);
  const endExclusive = isDateOnlyString(input.end)
    ? new Date(endDate.getTime() + DAY_MS)
    : interval === "1h"
      ? new Date(endDate.getTime() + HOUR_MS)
      : new Date(endDate.getTime() + DAY_MS);
  const endSeconds = Math.floor(endExclusive.getTime() / 1000);
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?period1=${startSeconds}&period2=${endSeconds}&interval=${encodeURIComponent(
    interval
  )}&includePrePost=false&events=div%2Csplits&corsDomain=finance.yahoo.com`;
  const payload = toJsonRecord(await fetchJsonWithTimeout(url, input.timeoutMs || DEFAULT_TIMEOUT_MS));
  const result = Array.isArray((payload.chart as any)?.result) ? (payload.chart as any).result[0] : null;
  const timestamps = Array.isArray(result?.timestamp) ? result.timestamp : [];
  const quote = Array.isArray(result?.indicators?.quote) ? result.indicators.quote[0] || {} : {};
  const adjcloseWrap = Array.isArray(result?.indicators?.adjclose) ? result.indicators.adjclose[0] || {} : {};
  const opens = Array.isArray(quote?.open) ? quote.open : [];
  const highs = Array.isArray(quote?.high) ? quote.high : [];
  const lows = Array.isArray(quote?.low) ? quote.low : [];
  const closes = Array.isArray(quote?.close) ? quote.close : [];
  const volumes = Array.isArray(quote?.volume) ? quote.volume : [];
  const adjCloses = Array.isArray(adjcloseWrap?.adjclose) ? adjcloseWrap.adjclose : [];
  const rows: Array<Record<string, unknown>> = [];
  timestamps.forEach((timestamp: number, index: number) => {
    const tsMs = Number(timestamp) * 1000;
    const date = new Date(tsMs);
    if (!Number.isFinite(tsMs) || Number.isNaN(date.getTime())) return;
    const close = Number(closes[index]);
    if (!Number.isFinite(close)) return;
    const open = Number(opens[index]);
    const high = Number(highs[index]);
    const low = Number(lows[index]);
    const volume = Number(volumes[index]);
    const adjClose = Number(adjCloses[index]);
    rows.push({
      [interval === "1h" ? "Datetime" : "Date"]: formatHistoryKey(date, interval),
      Open: Number.isFinite(open) ? Number(open.toFixed(6)) : Number(close.toFixed(6)),
      High: Number.isFinite(high) ? Number(high.toFixed(6)) : Number(close.toFixed(6)),
      Low: Number.isFinite(low) ? Number(low.toFixed(6)) : Number(close.toFixed(6)),
      Close: Number(close.toFixed(6)),
      "Adj Close": Number.isFinite(adjClose) ? Number(adjClose.toFixed(6)) : Number(close.toFixed(6)),
      Volume: Number.isFinite(volume) ? Math.max(0, Math.round(volume)) : 0,
    });
  });
  if (!rows.length) {
    throw new Error(`No price history returned for ${ticker}.`);
  }
  const response: HistoryResponse = {
    ticker,
    interval,
    rows,
    source: "yahoo_chart_v8",
    actualStart: String(rows[0][interval === "1h" ? "Datetime" : "Date"] || ""),
    actualEnd: String(rows[rows.length - 1][interval === "1h" ? "Datetime" : "Date"] || ""),
    clipped,
  };
  historyCache.set(cacheKey, {
    expiresAtMs: Date.now() + 10 * 60 * 1000,
    value: response,
  });
  return response;
}

function extractCloseSeries(rows: Array<Record<string, unknown>>): number[] {
  return rows
    .map((row) => Number(row.Close ?? row.close))
    .filter((value) => Number.isFinite(value) && value > 0);
}

function extractDateSeries(rows: Array<Record<string, unknown>>, interval: HistoryInterval): Date[] {
  const key = interval === "1h" ? "Datetime" : "Date";
  return rows
    .map((row) => new Date(String(row[key] || row.ds || "")))
    .filter((value) => !Number.isNaN(value.getTime()));
}

function average(values: number[]): number {
  if (!values.length) return 0;
  return values.reduce((total, value) => total + value, 0) / values.length;
}

function stddev(values: number[]): number {
  if (values.length < 2) return 0;
  const mean = average(values);
  const variance = values.reduce((total, value) => total + (value - mean) ** 2, 0) / values.length;
  return Math.sqrt(Math.max(0, variance));
}

function inverseStandardNormal(p: number): number {
  if (!(p > 0 && p < 1)) {
    throw new Error("Probability must be between 0 and 1.");
  }
  const a = [-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.357751867269, -30.66479806614716, 2.506628277459239];
  const b = [-54.47609879822406, 161.5858368580409, -155.6989798598866, 66.80131188771972, -13.28068155288572];
  const c = [-0.007784894002430293, -0.3223964580411365, -2.400758277161838, -2.549732539343734, 4.374664141464968, 2.938163982698783];
  const d = [0.007784695709041462, 0.3224671290700398, 2.445134137142996, 3.754408661907416];
  const pLow = 0.02425;
  const pHigh = 1 - pLow;
  let q = 0;
  let r = 0;
  if (p < pLow) {
    q = Math.sqrt(-2 * Math.log(p));
    return (
      (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
      ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    );
  }
  if (p <= pHigh) {
    q = p - 0.5;
    r = q * q;
    return (
      (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q /
      (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    );
  }
  q = Math.sqrt(-2 * Math.log(1 - p));
  return -(
    (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
    ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
  );
}

function nextForecastDates(lastDate: Date, horizon: number, interval: HistoryInterval): string[] {
  const out: string[] = [];
  let cursor = new Date(lastDate.getTime());
  while (out.length < horizon) {
    if (interval === "1h") {
      cursor = new Date(cursor.getTime() + HOUR_MS);
      out.push(formatHourKey(cursor));
      continue;
    }
    cursor = new Date(cursor.getTime() + DAY_MS);
    const weekday = cursor.getUTCDay();
    if (weekday === 0 || weekday === 6) continue;
    out.push(formatDayKey(cursor));
  }
  return out;
}

function computeBacktestMetrics(closes: number[]): {
  mae: number;
  rmse: number;
  mape: number;
  coverage25_75: number;
  coverage1_99: number;
  drift: number;
  volatility: number;
} {
  if (closes.length < 3) {
    return { mae: 0, rmse: 0, mape: 0, coverage25_75: 0, coverage1_99: 0, drift: 0, volatility: 0 };
  }
  const returns = closes
    .slice(1)
    .map((close, index) => Math.log(close / closes[index]))
    .filter((value) => Number.isFinite(value));
  const drift = average(returns);
  const volatility = stddev(returns);
  const z1 = inverseStandardNormal(0.01);
  const z25 = inverseStandardNormal(0.25);
  const z75 = inverseStandardNormal(0.75);
  const z99 = inverseStandardNormal(0.99);
  const errors: number[] = [];
  const pctErrors: number[] = [];
  let coveredCentral = 0;
  let coveredExtreme = 0;
  for (let index = 1; index < closes.length; index += 1) {
    const previous = closes[index - 1];
    const actual = closes[index];
    const meanLog = Math.log(previous) + drift;
    const median = Math.exp(meanLog);
    const extremeLow = Math.exp(meanLog + z1 * volatility);
    const centralLow = Math.exp(meanLog + z25 * volatility);
    const centralHigh = Math.exp(meanLog + z75 * volatility);
    const extremeHigh = Math.exp(meanLog + z99 * volatility);
    const error = actual - median;
    errors.push(error);
    pctErrors.push(previous > 0 ? Math.abs(error) / previous : 0);
    if (actual >= centralLow && actual <= centralHigh) coveredCentral += 1;
    if (actual >= extremeLow && actual <= extremeHigh) coveredExtreme += 1;
  }
  const mae = average(errors.map((value) => Math.abs(value)));
  const rmse = Math.sqrt(average(errors.map((value) => value ** 2)));
  const mape = average(pctErrors);
  const coverage25_75 = errors.length ? coveredCentral / errors.length : 0;
  const coverage1_99 = errors.length ? coveredExtreme / errors.length : 0;
  return { mae, rmse, mape, coverage25_75, coverage1_99, drift, volatility };
}

function normalizeForecastQuantiles(raw: unknown): number[] {
  const parts = Array.isArray(raw) ? raw : [raw];
  const seen = new Set<number>();
  const values = parts
    .map((entry) => Number(entry))
    .filter((entry) => Number.isFinite(entry) && entry > 0 && entry < 1)
    .filter((entry) => {
      const key = Math.round(entry * 10000);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((left, right) => left - right);
  if (!values.length) return [...META_PROPHET_FORECAST_QUANTILES];
  return values;
}

function buildTradeRationale(lastClose: number, finalMedian: number, volatility: number, interval: HistoryInterval): string {
  const deltaPct = lastClose > 0 ? ((finalMedian / lastClose) - 1) * 100 : 0;
  const volatilityPct = volatility * 100;
  const direction = deltaPct >= 0 ? "above" : "below";
  const horizonFrame = interval === "1h" ? "hourly" : "daily";
  return `Median ${horizonFrame} path ends ${Math.abs(deltaPct).toFixed(2)}% ${direction} the last close. Historical volatility was ${volatilityPct.toFixed(
    2
  )}% per step, so use the outer quantile bands as the uncertainty frame rather than a single target.`;
}

export function buildForecastFromHistory(input: {
  ticker: unknown;
  interval?: unknown;
  horizon: unknown;
  quantiles: unknown;
  historyRows: Array<Record<string, unknown>>;
}): ForecastRunResponse {
  const ticker = normalizeTicker(input.ticker);
  const interval = normalizeHistoryInterval(input.interval);
  const horizon = Math.max(1, Math.min(interval === "1h" ? 240 : 365, Math.floor(asFinite(input.horizon, 0))));
  const quantiles = normalizeForecastQuantiles(input.quantiles);
  const closes = extractCloseSeries(input.historyRows);
  const dates = extractDateSeries(input.historyRows, interval);
  if (!ticker) throw new Error("Ticker is required.");
  if (closes.length < 30 || dates.length < 30) {
    throw new Error("At least 30 history rows are required to generate a forecast.");
  }
  const lastClose = closes[closes.length - 1];
  const lastDate = dates[dates.length - 1];
  const returns = closes
    .slice(1)
    .map((close, index) => Math.log(close / closes[index]))
    .filter((value) => Number.isFinite(value));
  const drift = average(returns);
  const volatility = Math.max(stddev(returns), 0.0001);
  const futureDates = nextForecastDates(lastDate, horizon, interval);

  // Quantiles are closed-form percentiles of the existing log-normal return model:
  // exp(log(lastClose) + drift * h + z(q) * volatility * sqrt(h)).
  const forecastRows = futureDates.map((ds, stepIndex) => {
    const step = stepIndex + 1;
    const meanLog = Math.log(lastClose) + drift * step;
    const sigma = volatility * Math.sqrt(step);
    const row: Record<string, unknown> = { ds };
    quantiles.forEach((quantile) => {
      const key = `q${Math.round(quantile * 100)}`;
      row[key] = Number(Math.exp(meanLog + inverseStandardNormal(quantile) * sigma).toFixed(6));
    });
    if (row.q50 === undefined) {
      row.q50 = Number(Math.exp(meanLog).toFixed(6));
    }
    const orderedValues = quantiles.map((quantile) => Number(row[`q${Math.round(quantile * 100)}`]));
    if (
      orderedValues.some((value) => !Number.isFinite(value)) ||
      orderedValues.some((value, index) => index > 0 && orderedValues[index - 1] > value)
    ) {
      throw new Error(`Forecast quantile ordering failed for ${ds}.`);
    }
    return row;
  });

  const metrics = computeBacktestMetrics(closes);
  const finalRow = forecastRows[forecastRows.length - 1] || {};
  const finalMedian = asFinite(finalRow.q50, lastClose);
  const forecastQuantilesEnd = Object.fromEntries(
    Object.entries(finalRow)
      .filter(([key, value]) => /^q\d+$/.test(key) && Number.isFinite(Number(value)))
      .map(([key, value]) => [key, Number(Number(value).toFixed(6))])
  );

  return {
    ticker,
    interval,
    horizon,
    quantiles,
    forecastRows,
    forecastPreview: forecastRows.slice(0, 16),
    forecastQuantilesEnd,
    metrics: {
      horizon,
      historyPoints: closes.length,
      lastClose: Number(lastClose.toFixed(6)),
      medianEnd: Number(finalMedian.toFixed(6)),
      mae: Number(metrics.mae.toFixed(6)),
      rmse: Number(metrics.rmse.toFixed(6)),
      mape: Number(metrics.mape.toFixed(6)),
      coverage25_75: Number(metrics.coverage25_75.toFixed(6)),
      coverage1_99: Number(metrics.coverage1_99.toFixed(6)),
      drift: Number(metrics.drift.toFixed(6)),
      volatility: Number(metrics.volatility.toFixed(6)),
    },
    serviceMessage: `Quantura Horizon generated ${horizon} forward ${interval === "1h" ? "hourly" : "daily"} steps with canonical P1, P25, P50, P75, and P99 forecast-distribution quantiles from ${closes.length} historical bars.`,
    tradeRationale: buildTradeRationale(lastClose, finalMedian, metrics.volatility, interval),
    historyRows: input.historyRows,
    engine: "quantura_quantile_drift_v1",
  };
}

function parseTickerMentions(text: unknown): string[] {
  const raw = asString(text).toUpperCase();
  if (!raw) return [];
  const matches = raw.match(/\b[A-Z]{1,5}(?:\.[A-Z])?\b/g) || [];
  return Array.from(
    new Set(
      matches
        .map((token) => normalizeTicker(token))
        .filter((token) => token && !COMMON_STOPWORDS.has(token))
    )
  ).slice(0, 24);
}

function matchesComparison(value: number | null, filterValue: string): boolean {
  const raw = String(filterValue || "").trim().toLowerCase();
  if (!raw) return true;
  if (raw === "none") return (value || 0) === 0;
  if (raw === "pos" || raw === "positive") return value !== null && value > 0;
  if (raw === "neg" || raw === "negative") return value !== null && value < 0;
  const rangeMatch = raw.match(/^(\d+(?:\.\d+)?)to(\d+(?:\.\d+)?)$/);
  if (rangeMatch) {
    const min = Number(rangeMatch[1]);
    const max = Number(rangeMatch[2]);
    return value !== null && value >= min && value <= max;
  }
  const match = raw.match(/^([ouab])(\d+(?:\.\d+)?)$/);
  if (!match) return true;
  const op = match[1];
  const threshold = Number(match[2]);
  if (value === null) return false;
  if (op === "u" || op === "b") return value < threshold;
  if (op === "o" || op === "a") return value > threshold;
  return true;
}

function mapCapProfileBounds(profile: string): { min: number | null; max: number | null } {
  const key = String(profile || "").trim().toLowerCase();
  if (!key) return { min: null, max: null };
  const map: Record<string, { min: number | null; max: number | null }> = {
    mega: { min: 200_000_000_000, max: null },
    large: { min: 10_000_000_000, max: 200_000_000_000 },
    mid: { min: 2_000_000_000, max: 10_000_000_000 },
    small: { min: 300_000_000, max: 2_000_000_000 },
    micro: { min: 50_000_000, max: 300_000_000 },
    nano: { min: null, max: 50_000_000 },
    largeover: { min: 10_000_000_000, max: null },
    midover: { min: 2_000_000_000, max: null },
    smallover: { min: 300_000_000, max: null },
    microover: { min: 50_000_000, max: null },
    largeunder: { min: null, max: 200_000_000_000 },
    midunder: { min: null, max: 10_000_000_000 },
    smallunder: { min: null, max: 2_000_000_000 },
  };
  return map[key] || { min: null, max: null };
}

function matchesCapProfile(marketCap: number | null, profile: string): boolean {
  const bounds = mapCapProfileBounds(profile);
  if (!bounds.min && !bounds.max) return true;
  if (marketCap === null) return false;
  if (bounds.min !== null && marketCap < bounds.min) return false;
  if (bounds.max !== null && marketCap > bounds.max) return false;
  return true;
}

function normalizeExchangeLabel(value: string): string {
  const raw = String(value || "").trim().toLowerCase();
  if (raw.includes("nasdaq") || raw === "nms" || raw === "nas") return "nasdaq";
  if (raw.includes("nyse") || raw === "nyq") return "nyse";
  if (raw.includes("amex") || raw === "ase") return "amex";
  if (raw.includes("cboe") || raw.includes("bats")) return "cboe";
  return raw;
}

function computeRsi14(closes: number[]): number | null {
  if (closes.length < 15) return null;
  let gains = 0;
  let losses = 0;
  for (let index = closes.length - 14; index < closes.length; index += 1) {
    const prev = closes[index - 1];
    const current = closes[index];
    const delta = current - prev;
    if (delta >= 0) gains += delta;
    else losses += Math.abs(delta);
  }
  if (losses === 0) return 100;
  const rs = gains / losses;
  return 100 - 100 / (1 + rs);
}

function computeHistoryMetrics(rows: Array<Record<string, unknown>>): {
  lastClose: number | null;
  return1m: number | null;
  return3m: number | null;
  rsi14: number | null;
  volatility: number | null;
} {
  const closes = extractCloseSeries(rows);
  if (!closes.length) {
    return { lastClose: null, return1m: null, return3m: null, rsi14: null, volatility: null };
  }
  const lastClose = closes[closes.length - 1];
  const percentFrom = (lookback: number): number | null => {
    if (closes.length <= lookback) return null;
    const base = closes[closes.length - 1 - lookback];
    if (!Number.isFinite(base) || base <= 0) return null;
    return ((lastClose / base) - 1) * 100;
  };
  const returns = closes
    .slice(1)
    .map((close, index) => Math.log(close / closes[index]))
    .filter((value) => Number.isFinite(value));
  return {
    lastClose: Number(lastClose.toFixed(6)),
    return1m: percentFrom(21),
    return3m: percentFrom(63),
    rsi14: computeRsi14(closes),
    volatility: returns.length ? stddev(returns) * Math.sqrt(252) : null,
  };
}

function scoreCandidate(input: {
  row: QuickQuoteRow;
  metrics: { lastClose: number | null; return1m: number | null; return3m: number | null; rsi14: number | null; volatility: number | null };
  noteTickers: string[];
  trendingSymbols: string[];
}): number {
  let score = 0;
  if (input.metrics.return3m !== null) score += input.metrics.return3m * 1.4;
  if (input.metrics.return1m !== null) score += input.metrics.return1m * 0.8;
  if (input.metrics.rsi14 !== null) score += Math.max(0, 70 - Math.abs(input.metrics.rsi14 - 55));
  if (input.metrics.volatility !== null) score -= input.metrics.volatility * 25;
  if (input.row.marketCap !== null) score += Math.log10(Math.max(1, input.row.marketCap)) * 2;
  if (input.trendingSymbols.includes(input.row.symbol)) score += 8;
  if (input.noteTickers.includes(input.row.symbol)) score += 12;
  return Number(score.toFixed(4));
}

function matchesUpcomingEarnings(filterValue: string, nextEarningsDate: string): boolean {
  const key = String(filterValue || "").trim().toLowerCase();
  if (!key) return true;
  if (!nextEarningsDate) return false;
  const target = new Date(nextEarningsDate);
  if (Number.isNaN(target.getTime())) return false;
  const today = new Date();
  const start = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
  const diffDays = Math.floor((target.getTime() - start.getTime()) / DAY_MS);
  if (key === "today") return diffDays === 0;
  if (key === "tomorrow") return diffDays === 1;
  if (key === "nextdays5") return diffDays >= 0 && diffDays <= 5;
  if (key === "thisweek") return diffDays >= 0 && diffDays <= 7;
  return true;
}

function matchesTargetPrice(filterValue: string, targetMeanPrice: number | null, price: number | null): boolean {
  const key = String(filterValue || "").trim().toLowerCase();
  if (!key) return true;
  if (targetMeanPrice === null || price === null || price <= 0) return false;
  const deltaPct = ((targetMeanPrice / price) - 1) * 100;
  if (key === "above") return deltaPct > 0;
  if (key === "below") return deltaPct < 0;
  if (key === "a10") return deltaPct >= 10;
  if (key === "a20") return deltaPct >= 20;
  if (key === "b10") return deltaPct <= -10;
  return true;
}

function matchesFloatFilter(filterValue: string, floatShares: number | null, sharesOutstanding: number | null): boolean {
  const key = String(filterValue || "").trim().toLowerCase();
  if (!key) return true;
  if (key === "u50") return floatShares !== null && floatShares < 50_000_000;
  if (key === "o50") return floatShares !== null && floatShares > 50_000_000;
  if (key === "o100") return floatShares !== null && floatShares > 100_000_000;
  if (key === "u50p") return floatShares !== null && sharesOutstanding !== null && sharesOutstanding > 0 && floatShares / sharesOutstanding < 0.5;
  if (key === "o70p") return floatShares !== null && sharesOutstanding !== null && sharesOutstanding > 0 && floatShares / sharesOutstanding > 0.7;
  return true;
}

function matchesAnalystRecommendation(filterValue: string, value: number | null): boolean {
  const key = String(filterValue || "").trim().toLowerCase();
  if (!key) return true;
  if (value === null) return false;
  if (key === "strongbuy") return value <= 1.5;
  if (key === "buybetter") return value <= 2.5;
  if (key === "holdbetter") return value <= 3.5;
  if (key === "holdworse") return value >= 3;
  if (key === "strongsell") return value >= 4.5;
  return true;
}

function buildScreenerTitle(input: { universe: string; notes: string }): string {
  const notes = sanitizeText(input.notes, 120);
  if (notes) {
    return notes.length > 70 ? `${notes.slice(0, 67)}...` : notes;
  }
  const universe = sanitizeText(input.universe, 40) || "Trending";
  return `${universe} screener run`;
}

export async function runQuanturaScreener(input: {
  universe?: unknown;
  market?: unknown;
  minCap?: unknown;
  maxNames?: unknown;
  notes?: unknown;
  filters?: unknown;
}): Promise<ScreenerRunResponse> {
  const universe = sanitizeText(input.universe, 40).toLowerCase() || "trending";
  const market = sanitizeText(input.market, 20).toLowerCase() || "us";
  const minCap = Number.isFinite(asFinite(input.minCap, Number.NaN)) ? asFinite(input.minCap, Number.NaN) : null;
  const maxNames = Math.max(5, Math.min(25, Math.floor(asFinite(input.maxNames, 10))));
  const notes = sanitizeText(input.notes, 2000);
  const filters = toJsonRecord(input.filters);
  const noteTickers = parseTickerMentions(notes);
  const trendingSymbols = await fetchTrendingSymbols(market === "global" ? "US" : market.toUpperCase(), 24).catch(() => []);
  const screenIds = buildCandidateScreenerIds(universe);
  const quickQuotes = dedupeQuickQuotes(
    (
      await Promise.all(screenIds.map((screenId) => fetchPredefinedScreenerQuotes(screenId, 100).catch(() => [])))
    ).flat()
  );

  const appliedFilters: string[] = [];
  const ignoredFilters: string[] = [];
  const unsupportedFilterKeys = [
    "filterSector",
    "filterIndustry",
    "filterCountry",
    "filterTheme",
    "filterSubtheme",
    "filterPeg",
    "filterPs",
    "filterRoe",
    "filterRoa",
    "filterDebtEq",
    "filterNetMargin",
    "filterShortFloat",
    "filterTargetPrice",
    "filterFloat",
    "filterOptionShort",
    "filterIpoDate",
    "filterIndex",
  ];
  unsupportedFilterKeys.forEach((key) => {
    if (sanitizeText(filters[key], 120)) {
      ignoredFilters.push(key);
    }
  });

  const quickFiltered = quickQuotes.filter((row) => {
    if (universe === "large-cap") {
      appliedFilters.push("universe");
      if (row.marketCap === null || row.marketCap < 10_000_000_000) return false;
    } else if (universe === "mid-cap") {
      appliedFilters.push("universe");
      if (row.marketCap === null || row.marketCap < 2_000_000_000 || row.marketCap > 10_000_000_000) return false;
    } else if (universe === "small-cap") {
      appliedFilters.push("universe");
      if (row.marketCap === null || row.marketCap < 50_000_000 || row.marketCap > 2_000_000_000) return false;
    }
    if (minCap !== null) {
      appliedFilters.push("minCap");
      if (row.marketCap === null || row.marketCap < minCap) return false;
    }
    const capProfile = sanitizeText(filters.filterCapProfile, 40);
    if (capProfile) {
      appliedFilters.push("filterCapProfile");
      if (!matchesCapProfile(row.marketCap, capProfile)) return false;
    }
    const exchangeFilter = sanitizeText(filters.filterExchange, 40).toLowerCase();
    if (exchangeFilter) {
      appliedFilters.push("filterExchange");
      const exchangeText = normalizeExchangeLabel(`${row.exchange} ${row.fullExchangeName}`);
      if (!exchangeText.includes(exchangeFilter)) return false;
    }
    if (market === "global") {
      ignoredFilters.push("market");
    }
    if (filters.filterPrice && !matchesComparison(row.price, sanitizeText(filters.filterPrice, 40).replace(/\$/g, ""))) {
      appliedFilters.push("filterPrice");
      return false;
    }
    if (filters.filterAverageVolume) {
      appliedFilters.push("filterAverageVolume");
      const threshold = sanitizeText(filters.filterAverageVolume, 40).replace(/^o/, "");
      const numeric = Number(threshold) * 1000;
      if (!Number.isFinite(numeric) || row.averageVolume === null || row.averageVolume < numeric) return false;
    }
    if (filters.filterCurrentVolume) {
      appliedFilters.push("filterCurrentVolume");
      const threshold = sanitizeText(filters.filterCurrentVolume, 40).replace(/^o/, "");
      const numeric = Number(threshold) * 1000;
      if (!Number.isFinite(numeric) || row.volume === null || row.volume < numeric) return false;
    }
    if (filters.filterRelativeVolume) {
      appliedFilters.push("filterRelativeVolume");
      const threshold = Number(sanitizeText(filters.filterRelativeVolume, 40).replace(/^o/, ""));
      const relativeVolume =
        row.volume !== null && row.averageVolume !== null && row.averageVolume > 0 ? row.volume / row.averageVolume : null;
      if (!Number.isFinite(threshold) || relativeVolume === null || relativeVolume <= threshold) return false;
    }
    if (filters.filterPe) {
      appliedFilters.push("filterPe");
      if (!matchesComparison(row.trailingPe, sanitizeText(filters.filterPe, 40))) return false;
    }
    if (filters.filterForwardPe) {
      appliedFilters.push("filterForwardPe");
      if (!matchesComparison(row.forwardPe, sanitizeText(filters.filterForwardPe, 40))) return false;
    }
    if (filters.filterPb) {
      appliedFilters.push("filterPb");
      if (!matchesComparison(row.priceToBook, sanitizeText(filters.filterPb, 40))) return false;
    }
    if (filters.filterDividendYield) {
      appliedFilters.push("filterDividendYield");
      if (!matchesComparison(row.dividendYieldPct, sanitizeText(filters.filterDividendYield, 40))) return false;
    }
    if (filters.filterAnalystRecom) {
      appliedFilters.push("filterAnalystRecom");
      if (!matchesAnalystRecommendation(sanitizeText(filters.filterAnalystRecom, 40), row.analystRecommendationMean)) return false;
    }
    if (filters.filterEarningsDate) {
      appliedFilters.push("filterEarningsDate");
      if (!matchesUpcomingEarnings(sanitizeText(filters.filterEarningsDate, 40), row.earningsDate)) return false;
    }
    if (filters.filterSharesOutstanding) {
      appliedFilters.push("filterSharesOutstanding");
      const sharesM = row.sharesOutstanding !== null && row.sharesOutstanding !== undefined ? row.sharesOutstanding / 1_000_000 : null;
      if (!matchesComparison(sharesM, sanitizeText(filters.filterSharesOutstanding, 40).replace(/p$/i, ""))) return false;
    }
      return true;
    });

  const historyCandidates = quickFiltered.slice(0, Math.max(HISTORY_SYMBOL_LIMIT, maxNames * 3));
  const historyRows = await mapWithConcurrency(historyCandidates, 6, async (row) => {
    try {
      const history = await fetchYahooHistoryBars({
        ticker: row.symbol,
        interval: "1d",
        start: new Date(Date.now() - 220 * DAY_MS).toISOString().slice(0, 10),
        end: new Date().toISOString().slice(0, 10),
      });
      return history.rows;
    } catch {
      return [];
    }
  });

  const results = historyCandidates
    .map((row, index) => {
      const metrics = computeHistoryMetrics(historyRows[index] || []);
      const score = scoreCandidate({ row, metrics, noteTickers, trendingSymbols });
      return {
        symbol: row.symbol,
        companyName: row.longName || row.shortName,
        lastClose: metrics.lastClose !== null ? Number(metrics.lastClose.toFixed(2)) : null,
        return1m: metrics.return1m !== null ? Number(metrics.return1m.toFixed(2)) : null,
        return3m: metrics.return3m !== null ? Number(metrics.return3m.toFixed(2)) : null,
        rsi14: metrics.rsi14 !== null ? Number(metrics.rsi14.toFixed(2)) : null,
        volatility: metrics.volatility !== null ? Number(metrics.volatility.toFixed(4)) : null,
        score,
        marketCap: row.marketCap,
        marketCapLabel: row.marketCapLabel,
        sector: "",
        industry: "",
        country: "",
        exchange: row.fullExchangeName || row.exchange,
      };
    })
    .filter((row) => Number.isFinite(Number(row.score)))
    .sort((left, right) => Number(right.score) - Number(left.score))
    .slice(0, maxNames);

  const topSymbols = results.map((row) => String(row.symbol || "")).filter(Boolean);
  const uniqueApplied = Array.from(new Set(appliedFilters)).filter(Boolean);
  const uniqueIgnored = Array.from(new Set(ignoredFilters)).filter(Boolean);
  const serviceMessage = results.length
    ? `Quantura screener ranked ${results.length} symbols from ${quickFiltered.length} quick matches using price history, market-cap, liquidity, and selected supported filters.${
        uniqueIgnored.length ? ` Ignored filters: ${uniqueIgnored.join(", ")}.` : ""
      }`
    : `No symbols matched the current screener criteria.${uniqueIgnored.length ? ` Ignored filters: ${uniqueIgnored.join(", ")}.` : ""}`;

  return {
    title: buildScreenerTitle({ universe, notes }),
    results,
    serviceMessage,
    resultsFound: results.length,
    topSymbols,
    appliedFilters: uniqueApplied,
    ignoredFilters: uniqueIgnored,
  };
}

function extractDslFilters(query: JsonRecord): {
  exchange: string;
  region: string;
  minMarketCap: number | null;
  maxPe: number | null;
  minVolume: number | null;
} {
  const out = {
    exchange: "",
    region: "",
    minMarketCap: null as number | null,
    maxPe: null as number | null,
    minVolume: null as number | null,
  };
  const operands = Array.isArray(query.operands) ? query.operands : [];
  operands.forEach((entry) => {
    const row = toJsonRecord(entry);
    const op = sanitizeText(row.operator, 20).toLowerCase();
    const parts = Array.isArray(row.operands) ? row.operands : [];
    const field = sanitizeText(parts[0], 80).toLowerCase();
    const value = parts[1];
    if (op === "eq" && field === "exchange") out.exchange = sanitizeText(value, 40);
    if (op === "eq" && field === "region") out.region = sanitizeText(value, 20).toLowerCase();
    if (op === "gte" && field === "intradaymarketcap") out.minMarketCap = asFinite(value, Number.NaN);
    if (op === "lte" && field === "peratio.lasttwelvemonths") out.maxPe = asFinite(value, Number.NaN);
    if (op === "gte" && field === "dayvolume") out.minVolume = asFinite(value, Number.NaN);
  });
  return out;
}

export async function runMarketDataScreener(input: {
  preset?: unknown;
  size?: unknown;
  query?: unknown;
}): Promise<MarketDataScreenerResponse> {
  const preset = sanitizeText(input.preset, 60).toLowerCase();
  const size = Math.max(1, Math.min(150, Math.floor(asFinite(input.size, 20))));
  const dsl = toJsonRecord(input.query);
  const dslFilters = Object.keys(dsl).length
    ? extractDslFilters(dsl)
    : { exchange: "", region: "", minMarketCap: null, maxPe: null, minVolume: null };
  const presetIds =
    preset === "day_gainers"
      ? ["day_gainers"]
      : preset === "day_losers"
        ? ["day_losers"]
        : preset === "aggressive_small_caps"
          ? ["aggressive_small_caps", "small_cap_gainers"]
          : preset === "undervalued_large_caps"
            ? ["undervalued_growth_stocks", "growth_technology_stocks", "most_actives"]
            : ["most_actives", "day_gainers", "day_losers"];
  const quotes = dedupeQuickQuotes((await Promise.all(presetIds.map((id) => fetchPredefinedScreenerQuotes(id, 100).catch(() => [])))).flat());
  let rows = quotes.filter((row) => {
    if (dslFilters.exchange) {
      const exchangeText = normalizeExchangeLabel(`${row.exchange} ${row.fullExchangeName}`);
      if (!exchangeText.includes(dslFilters.exchange.toLowerCase())) return false;
    }
    if (dslFilters.region && row.region && row.region.toLowerCase() !== dslFilters.region.toLowerCase()) return false;
    if (dslFilters.minMarketCap !== null && Number.isFinite(dslFilters.minMarketCap) && (row.marketCap === null || row.marketCap < dslFilters.minMarketCap)) {
      return false;
    }
    if (dslFilters.maxPe !== null && Number.isFinite(dslFilters.maxPe) && (row.trailingPe === null || row.trailingPe > dslFilters.maxPe)) {
      return false;
    }
    if (dslFilters.minVolume !== null && Number.isFinite(dslFilters.minVolume) && (row.volume === null || row.volume < dslFilters.minVolume)) {
      return false;
    }
    return true;
  });

  let mode = preset || (Object.keys(dsl).length ? "custom" : "default");
  if (preset === "most_actives") {
    rows = rows.sort((left, right) => (right.volume || 0) - (left.volume || 0));
  } else if (preset === "day_gainers") {
    rows = rows.sort((left, right) => (right.changePercent || -Infinity) - (left.changePercent || -Infinity));
  } else if (preset === "day_losers") {
    rows = rows.sort((left, right) => (left.changePercent || Infinity) - (right.changePercent || Infinity));
  } else if (preset === "aggressive_small_caps") {
    rows = rows
      .filter((row) => row.marketCap !== null && row.marketCap >= 50_000_000 && row.marketCap <= 2_000_000_000)
      .sort((left, right) => (right.changePercent || -Infinity) - (left.changePercent || -Infinity));
  } else if (preset === "undervalued_large_caps") {
    rows = rows
      .filter((row) => row.marketCap !== null && row.marketCap >= 10_000_000_000 && row.trailingPe !== null && row.trailingPe <= 20)
      .sort((left, right) => {
        const peDelta = (left.trailingPe || 999) - (right.trailingPe || 999);
        if (peDelta !== 0) return peDelta;
        return (right.marketCap || 0) - (left.marketCap || 0);
      });
  } else {
    mode = Object.keys(dsl).length ? "query" : "preset";
    rows = rows.sort((left, right) => {
      const scoreLeft = (left.changePercent || 0) + Math.log10(Math.max(1, left.marketCap || 1));
      const scoreRight = (right.changePercent || 0) + Math.log10(Math.max(1, right.marketCap || 1));
      return scoreRight - scoreLeft;
    });
  }

  return {
    items: rows.slice(0, size).map((row) => ({
      symbol: row.symbol,
      shortName: row.shortName,
      longName: row.longName,
      price: row.price,
      changePercent: row.changePercent,
      volume: row.volume,
      marketCap: row.marketCap,
      exchange: row.fullExchangeName || row.exchange,
    })),
    mode,
    serviceMessage: `Market-data screener returned ${Math.min(size, rows.length)} rows from Yahoo quote data.`,
  };
}
