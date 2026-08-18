const KALSHI_API_BASE = "https://external-api.kalshi.com/trade-api/v2";
const MINUTE_SECONDS = 60;
const MAX_MINUTES_PER_REQUEST = 4_500;
export const KALSHI_MINUTE_HISTORY_MAX_MINUTES = 7 * 24 * 60;

type FetchLike = typeof fetch;
type JsonRecord = Record<string, unknown>;

export type KalshiMinuteHistoryRow = {
  ticker: string;
  candleStart: string;
  candleEnd: string;
  interval: "1m";
  priceOpen: number | null;
  priceHigh: number | null;
  priceLow: number | null;
  priceClose: number | null;
  priceMean: number | null;
  pricePrevious: number | null;
  yesBidOpen: number | null;
  yesBidHigh: number | null;
  yesBidLow: number | null;
  yesBidClose: number | null;
  yesAskOpen: number | null;
  yesAskHigh: number | null;
  yesAskLow: number | null;
  yesAskClose: number | null;
  volume: number | null;
  openInterest: number | null;
  sourceTier: "live" | "historical";
};

export type KalshiMinuteHistory = {
  ticker: string;
  provider: "Kalshi";
  interval: "1m";
  startTime: string;
  endTime: string;
  rowCount: number;
  requestedMinuteCount: number;
  missingMinuteCount: number;
  sourceTiers: Array<"live" | "historical">;
  headers: string[];
  rows: KalshiMinuteHistoryRow[];
  previewRows: KalshiMinuteHistoryRow[];
  csvText: string;
  fileName: string;
};

const CSV_HEADERS: Array<keyof KalshiMinuteHistoryRow> = [
  "ticker",
  "candleStart",
  "candleEnd",
  "interval",
  "priceOpen",
  "priceHigh",
  "priceLow",
  "priceClose",
  "priceMean",
  "pricePrevious",
  "yesBidOpen",
  "yesBidHigh",
  "yesBidLow",
  "yesBidClose",
  "yesAskOpen",
  "yesAskHigh",
  "yesAskLow",
  "yesAskClose",
  "volume",
  "openInterest",
  "sourceTier",
];

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : null;
}

function finiteNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function cleanTicker(value: unknown): string {
  const ticker = String(value || "").trim().toUpperCase();
  if (!ticker || ticker.length > 180 || !/^[A-Z0-9._-]+$/.test(ticker)) {
    throw new Error("invalid_kalshi_market_ticker");
  }
  return ticker;
}

function minuteBoundary(value: unknown, field: "start" | "end"): Date {
  const timestamp = Date.parse(String(value || "").trim());
  if (!Number.isFinite(timestamp)) throw new Error(`invalid_kalshi_${field}_time`);
  return new Date(Math.floor(timestamp / (MINUTE_SECONDS * 1000)) * MINUTE_SECONDS * 1000);
}

function amountField(value: unknown, field: string): number | null {
  const record = asRecord(value);
  if (!record) return null;
  return finiteNumber(record[`${field}_dollars`] ?? record[field]);
}

function csvCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  const text = String(value);
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function rowsToCsv(rows: KalshiMinuteHistoryRow[]): string {
  return [
    CSV_HEADERS.join(","),
    ...rows.map((row) => CSV_HEADERS.map((header) => csvCell(row[header])).join(",")),
  ].join("\r\n");
}

async function fetchKalshiJson(
  fetchImpl: FetchLike,
  path: string,
  params: Record<string, string | number>
): Promise<{ status: number; payload: JsonRecord }> {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => query.set(key, String(value)));
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25_000);
  try {
    const response = await fetchImpl(`${KALSHI_API_BASE}${path}?${query.toString()}`, {
      method: "GET",
      headers: { Accept: "application/json", "User-Agent": "QuanturaSportsHistory/1.0" },
      signal: controller.signal,
    });
    const payload = asRecord(await response.json().catch(() => ({}))) || {};
    if (!response.ok && response.status !== 404 && response.status !== 400 && response.status !== 422) {
      if (response.status === 429) throw new Error("kalshi_rate_limited");
      throw new Error(`kalshi_upstream_failed:${response.status}`);
    }
    return { status: response.status, payload };
  } catch (error: unknown) {
    if ((error as { name?: string })?.name === "AbortError") throw new Error("kalshi_upstream_timeout");
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function normalizeCandles(
  ticker: string,
  rawCandles: unknown,
  sourceTier: "live" | "historical"
): KalshiMinuteHistoryRow[] {
  if (!Array.isArray(rawCandles)) return [];
  const rows: KalshiMinuteHistoryRow[] = [];
  rawCandles.forEach((rawCandle) => {
    const candle = asRecord(rawCandle);
    if (!candle) return;
    const candleEndSeconds = finiteNumber(candle.end_period_ts);
    if (candleEndSeconds === null) return;
    const candleEndMs = candleEndSeconds * 1000;
    const price = candle.price;
    const yesBid = candle.yes_bid;
    const yesAsk = candle.yes_ask;
    rows.push({
      ticker,
      candleStart: new Date(candleEndMs - MINUTE_SECONDS * 1000).toISOString(),
      candleEnd: new Date(candleEndMs).toISOString(),
      interval: "1m",
      priceOpen: amountField(price, "open"),
      priceHigh: amountField(price, "high"),
      priceLow: amountField(price, "low"),
      priceClose: amountField(price, "close"),
      priceMean: amountField(price, "mean"),
      pricePrevious: amountField(price, "previous"),
      yesBidOpen: amountField(yesBid, "open"),
      yesBidHigh: amountField(yesBid, "high"),
      yesBidLow: amountField(yesBid, "low"),
      yesBidClose: amountField(yesBid, "close"),
      yesAskOpen: amountField(yesAsk, "open"),
      yesAskHigh: amountField(yesAsk, "high"),
      yesAskLow: amountField(yesAsk, "low"),
      yesAskClose: amountField(yesAsk, "close"),
      volume: finiteNumber(candle.volume_fp ?? candle.volume),
      openInterest: finiteNumber(candle.open_interest_fp ?? candle.open_interest),
      sourceTier,
    });
  });
  return rows;
}

async function fetchLiveChunk(
  fetchImpl: FetchLike,
  ticker: string,
  startSeconds: number,
  endSeconds: number
): Promise<{ found: boolean; rows: KalshiMinuteHistoryRow[] }> {
  const response = await fetchKalshiJson(fetchImpl, "/markets/candlesticks", {
    market_tickers: ticker,
    start_ts: startSeconds,
    end_ts: endSeconds,
    period_interval: 1,
  });
  if (response.status !== 200) return { found: false, rows: [] };
  const markets = Array.isArray(response.payload.markets) ? response.payload.markets : [];
  const market = markets
    .map(asRecord)
    .find((item) => String(item?.market_ticker || item?.ticker || "").trim().toUpperCase() === ticker);
  return {
    found: Boolean(market),
    rows: normalizeCandles(ticker, market?.candlesticks, "live"),
  };
}

async function fetchHistoricalChunk(
  fetchImpl: FetchLike,
  ticker: string,
  startSeconds: number,
  endSeconds: number
): Promise<{ found: boolean; rows: KalshiMinuteHistoryRow[] }> {
  const response = await fetchKalshiJson(
    fetchImpl,
    `/historical/markets/${encodeURIComponent(ticker)}/candlesticks`,
    { start_ts: startSeconds, end_ts: endSeconds, period_interval: 1 }
  );
  return {
    found: response.status === 200,
    rows: response.status === 200 ? normalizeCandles(ticker, response.payload.candlesticks, "historical") : [],
  };
}

export async function downloadKalshiMinuteHistory(
  input: { ticker: unknown; startTime: unknown; endTime: unknown },
  options: { fetchImpl?: FetchLike } = {}
): Promise<KalshiMinuteHistory> {
  const ticker = cleanTicker(input.ticker);
  const start = minuteBoundary(input.startTime, "start");
  const end = minuteBoundary(input.endTime, "end");
  const requestedMinuteCount = Math.floor((end.getTime() - start.getTime()) / (MINUTE_SECONDS * 1000)) + 1;
  if (requestedMinuteCount < 1) throw new Error("invalid_kalshi_time_range");
  if (requestedMinuteCount > KALSHI_MINUTE_HISTORY_MAX_MINUTES) throw new Error("kalshi_time_range_too_large");

  const fetchImpl = options.fetchImpl || fetch;
  const rows: KalshiMinuteHistoryRow[] = [];
  let marketWasFound = false;
  for (let offset = 0; offset < requestedMinuteCount; offset += MAX_MINUTES_PER_REQUEST) {
    const chunkMinutes = Math.min(MAX_MINUTES_PER_REQUEST, requestedMinuteCount - offset);
    const chunkStartSeconds = Math.floor(start.getTime() / 1000) + offset * MINUTE_SECONDS;
    const chunkEndSeconds = chunkStartSeconds + (chunkMinutes - 1) * MINUTE_SECONDS;
    const live = await fetchLiveChunk(fetchImpl, ticker, chunkStartSeconds, chunkEndSeconds);
    marketWasFound = marketWasFound || live.found;
    if (live.rows.length) {
      marketWasFound = true;
      rows.push(...live.rows);
      continue;
    }
    const historical = await fetchHistoricalChunk(fetchImpl, ticker, chunkStartSeconds, chunkEndSeconds);
    marketWasFound = marketWasFound || historical.found;
    rows.push(...historical.rows);
  }
  if (!marketWasFound) throw new Error("kalshi_market_not_found");

  const uniqueRows = Array.from(new Map(rows.map((row) => [row.candleEnd, row])).values()).sort(
    (left, right) => Date.parse(left.candleEnd) - Date.parse(right.candleEnd)
  );
  const sourceTiers = Array.from(new Set(uniqueRows.map((row) => row.sourceTier)));
  const startTime = start.toISOString();
  const endTime = end.toISOString();
  const fileName = `${ticker}_1m_${startTime.slice(0, 16).replace(/[:T]/g, "-")}_${endTime
    .slice(0, 16)
    .replace(/[:T]/g, "-")}.csv`;
  return {
    ticker,
    provider: "Kalshi",
    interval: "1m",
    startTime,
    endTime,
    rowCount: uniqueRows.length,
    requestedMinuteCount,
    missingMinuteCount: Math.max(0, requestedMinuteCount - uniqueRows.length),
    sourceTiers,
    headers: CSV_HEADERS.map(String),
    rows: uniqueRows,
    previewRows: uniqueRows.slice(-25).reverse(),
    csvText: rowsToCsv(uniqueRows),
    fileName,
  };
}
