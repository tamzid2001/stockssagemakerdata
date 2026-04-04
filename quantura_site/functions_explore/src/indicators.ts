export type IndicatorCode =
  | "RSI"
  | "MACD"
  | "SMA"
  | "EMA"
  | "BBANDS"
  | "ATR"
  | "ADX"
  | "CCI"
  | "MFI"
  | "OBV"
  | "ROC"
  | "STOCH"
  | "WILLR";

export type IndicatorLatestRow = {
  name: string;
  value: number | null;
  display: string;
};

export type IndicatorSeriesItem = {
  name: string;
  values: Array<number | null>;
};

export type IndicatorPrediction = {
  direction: "bullish" | "bearish" | "neutral";
  targetPrice: number | null;
  timeline: string;
  timelineDays: number;
  confidence: "low" | "medium" | "high";
};

export type IndicatorAnalysis = {
  summary: string;
  keySignals: string[];
  prediction: IndicatorPrediction;
  text: string;
  provider: string;
  model: string;
  disclaimer: string;
};

export type IndicatorAnalyzeResponse = {
  ticker: string;
  interval: "1d" | "1h";
  lookback: number;
  selectedIndicators: IndicatorCode[];
  latest: IndicatorLatestRow[];
  series: {
    dates: string[];
    items: IndicatorSeriesItem[];
  };
  analysis: IndicatorAnalysis;
  meta: {
    asOf: string;
    historyPoints: number;
    lastClose: number;
    previousClose: number | null;
    source: string;
  };
};

export type IndicatorAnalyzeInput = {
  ticker?: unknown;
  interval?: unknown;
  lookback?: unknown;
  indicators?: unknown;
  maxPoints?: unknown;
  provider?: unknown;
  model?: unknown;
  fallbackProviders?: unknown;
  userTier?: unknown;
};

export type IndicatorAnalyzeOptions = {
  openAiApiKey: string;
  defaultModel: string;
  timeoutMs: number;
  invokeLlm?: (payload: {
    provider: string;
    model: string;
    fallbackProviders?: string[];
    userTier?: string;
    messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
    params?: Record<string, unknown>;
    jsonSchema?: unknown;
  }) => Promise<{
    provider: string;
    model: string;
    text: string;
    usage?: Record<string, unknown>;
  }>;
};

type CandlePoint = {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

type IndicatorComputation = {
  ticker: string;
  interval: "1d" | "1h";
  lookback: number;
  selectedIndicators: IndicatorCode[];
  candles: CandlePoint[];
  dates: string[];
  close: number[];
  high: number[];
  low: number[];
  volume: number[];
  seriesItems: IndicatorSeriesItem[];
  latestRows: IndicatorLatestRow[];
  latestNumeric: Record<string, number>;
  asOf: string;
  lastClose: number;
  previousClose: number | null;
};

type CoreIndicatorSnapshot = {
  rsi14: number | null;
  macdLine: number | null;
  macdSignal: number | null;
  macdHist: number | null;
  atr14: number | null;
  atrPct: number | null;
  adx14: number | null;
  plusDi14: number | null;
  minusDi14: number | null;
  ema20: number | null;
  sma20: number | null;
  bbandsUpper: number | null;
  bbandsMiddle: number | null;
  bbandsLower: number | null;
  cci20: number | null;
  mfi14: number | null;
  roc12: number | null;
  stochK: number | null;
  stochD: number | null;
  willr14: number | null;
  realizedVol20: number | null;
  recentRangePct20: number | null;
  obvSlope10: number | null;
};

const SUPPORTED_INDICATORS: IndicatorCode[] = [
  "RSI",
  "MACD",
  "SMA",
  "EMA",
  "BBANDS",
  "ATR",
  "ADX",
  "CCI",
  "MFI",
  "OBV",
  "ROC",
  "STOCH",
  "WILLR",
];

const SUPPORTED_SET = new Set<IndicatorCode>(SUPPORTED_INDICATORS);

const LLM_DISCLAIMER = "LLMs can sometimes make mistakes.";

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function asFinite(value: unknown, fallback = 0): number {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function sanitizeText(value: unknown, maxLen = 600): string {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  if (!text) return "";
  return text.slice(0, Math.max(0, maxLen));
}

function normalizeTicker(value: unknown): string {
  return sanitizeText(value, 24)
    .toUpperCase()
    .replace(/[^A-Z0-9.\-=^]/g, "")
    .slice(0, 20);
}

function normalizeInterval(value: unknown): "1d" | "1h" {
  const raw = sanitizeText(value, 8).toLowerCase();
  return raw === "1h" ? "1h" : "1d";
}

function normalizeLlmProvider(value: unknown): string {
  const raw = sanitizeText(value, 40).toLowerCase().replace(/[^a-z0-9_-]/g, "");
  if (raw === "anthropic") return "claude";
  if (raw === "amazon-nova" || raw === "nova") return "amazon_nova";
  if (raw) return raw;
  return "openai";
}

function normalizeLlmModel(value: unknown, fallback = "gpt-5-mini"): string {
  return sanitizeText(value, 120) || sanitizeText(fallback, 120) || "gpt-5-mini";
}

function normalizeFallbackProviders(value: unknown): string[] {
  const rows = Array.isArray(value) ? value : [];
  return Array.from(new Set(rows.map((item) => normalizeLlmProvider(item)).filter(Boolean)));
}

function normalizeLlmTier(value: unknown): string {
  const raw = sanitizeText(value, 40).toLowerCase();
  return raw === "premium" || raw === "pro" || raw === "business" ? "premium" : "free";
}

function normalizeLookback(value: unknown): number {
  const numeric = Math.floor(asFinite(value, 180));
  return Math.max(30, Math.min(5000, numeric || 180));
}

function normalizeMaxPoints(value: unknown): number {
  const numeric = Math.floor(asFinite(value, 320));
  return Math.max(80, Math.min(1500, numeric || 320));
}

function normalizeIndicators(value: unknown): IndicatorCode[] {
  const rows = Array.isArray(value) ? value : [];
  const normalized = rows
    .map((entry) => sanitizeText(entry, 20).toUpperCase())
    .filter((entry): entry is IndicatorCode => SUPPORTED_SET.has(entry as IndicatorCode));
  const deduped = Array.from(new Set(normalized));
  if (deduped.length) return deduped;
  return ["RSI", "MACD", "EMA", "SMA"];
}

function chooseYahooRange(interval: "1d" | "1h", lookbackDays: number): string {
  if (interval === "1h") {
    if (lookbackDays <= 7) return "7d";
    if (lookbackDays <= 30) return "1mo";
    if (lookbackDays <= 90) return "3mo";
    if (lookbackDays <= 180) return "6mo";
    if (lookbackDays <= 365) return "1y";
    return "2y";
  }
  if (lookbackDays <= 30) return "1mo";
  if (lookbackDays <= 90) return "3mo";
  if (lookbackDays <= 180) return "6mo";
  if (lookbackDays <= 365) return "1y";
  if (lookbackDays <= 730) return "2y";
  if (lookbackDays <= 1825) return "5y";
  return "10y";
}

async function fetchYahooCandles(input: {
  ticker: string;
  interval: "1d" | "1h";
  lookback: number;
  maxPoints: number;
}): Promise<CandlePoint[]> {
  const range = chooseYahooRange(input.interval, input.lookback);
  const endpoint = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(input.ticker)}?interval=${encodeURIComponent(
    input.interval
  )}&range=${encodeURIComponent(range)}&events=history`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 12000);
  try {
    const response = await fetch(endpoint, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        "User-Agent": "QuanturaIndicators/1.0",
      },
    });
    if (!response.ok) {
      throw new Error(`Market data request failed (${response.status}).`);
    }
    const payload = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    const result = Array.isArray((payload.chart as any)?.result) ? ((payload.chart as any).result[0] as Record<string, unknown>) : null;
    if (!result) throw new Error("No market data returned.");
    const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
    const quote = Array.isArray((result.indicators as any)?.quote) ? ((result.indicators as any).quote[0] as Record<string, unknown>) : null;
    const opens = Array.isArray(quote?.open) ? quote?.open : [];
    const highs = Array.isArray(quote?.high) ? quote?.high : [];
    const lows = Array.isArray(quote?.low) ? quote?.low : [];
    const closes = Array.isArray(quote?.close) ? quote?.close : [];
    const volumes = Array.isArray(quote?.volume) ? quote?.volume : [];

    const rows: CandlePoint[] = [];
    const count = Math.min(timestamps.length, opens.length || timestamps.length, highs.length || timestamps.length, lows.length || timestamps.length, closes.length || timestamps.length);
    for (let idx = 0; idx < count; idx += 1) {
      const ts = Number(timestamps[idx]);
      const close = Number(closes[idx]);
      if (!Number.isFinite(ts) || !Number.isFinite(close) || close <= 0) continue;
      const open = Number(opens[idx]);
      const high = Number(highs[idx]);
      const low = Number(lows[idx]);
      const volume = Number(volumes[idx]);
      const safeOpen = Number.isFinite(open) ? open : close;
      const safeHigh = Number.isFinite(high) ? Math.max(high, safeOpen, close) : Math.max(safeOpen, close);
      const safeLow = Number.isFinite(low) ? Math.min(low, safeOpen, close) : Math.min(safeOpen, close);
      rows.push({
        date: new Date(ts * 1000).toISOString(),
        open: safeOpen,
        high: safeHigh,
        low: safeLow,
        close,
        volume: Number.isFinite(volume) && volume >= 0 ? volume : 0,
      });
    }
    if (rows.length < 40) throw new Error("Not enough market history for indicators.");

    const desiredPoints = input.interval === "1h" ? Math.max(input.maxPoints + 240, input.lookback * 7) : Math.max(input.maxPoints + 180, input.lookback);
    return rows.slice(-Math.min(rows.length, desiredPoints));
  } finally {
    clearTimeout(timer);
  }
}

function createEmptySeries(length: number): Array<number | null> {
  return Array.from({ length }, () => null);
}

function latestFinite(values: Array<number | null>): number | null {
  for (let idx = values.length - 1; idx >= 0; idx -= 1) {
    const value = values[idx];
    if (typeof value === "number" && Number.isFinite(value)) return value;
  }
  return null;
}

function formatIndicatorValue(name: string, value: number | null): string {
  if (!Number.isFinite(value as number)) return "—";
  const numeric = Number(value);
  if (name.startsWith("OBV")) return numeric.toFixed(0);
  if (name.startsWith("MACD") || name.startsWith("ROC") || name.startsWith("WILLR")) return numeric.toFixed(3);
  if (Math.abs(numeric) >= 1000) return numeric.toFixed(2);
  return numeric.toFixed(4);
}

function roundFinite(value: unknown, digits = 4): number | null {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  return Number(numeric.toFixed(digits));
}

function percentDelta(base: number | null, current: number | null, digits = 2): number | null {
  if (!Number.isFinite(base as number) || !Number.isFinite(current as number) || Number(base) === 0) return null;
  return Number((((Number(current) / Number(base)) - 1) * 100).toFixed(digits));
}

function sma(values: number[], period: number): Array<number | null> {
  const out = createEmptySeries(values.length);
  if (!Number.isFinite(period) || period <= 1) return values.map((value) => (Number.isFinite(value) ? value : null));
  let running = 0;
  for (let idx = 0; idx < values.length; idx += 1) {
    running += values[idx] || 0;
    if (idx >= period) running -= values[idx - period] || 0;
    if (idx >= period - 1) out[idx] = running / period;
  }
  return out;
}

function ema(values: number[], period: number): Array<number | null> {
  const out = createEmptySeries(values.length);
  if (!values.length) return out;
  const alpha = 2 / (period + 1);
  let prev: number | null = null;
  for (let idx = 0; idx < values.length; idx += 1) {
    const value = values[idx];
    if (!Number.isFinite(value)) {
      out[idx] = prev;
      continue;
    }
    if (prev === null) {
      prev = value;
    } else {
      prev = value * alpha + prev * (1 - alpha);
    }
    out[idx] = prev;
  }
  return out;
}

function rsi(values: number[], period = 14): Array<number | null> {
  const out = createEmptySeries(values.length);
  if (values.length <= period) return out;
  let gain = 0;
  let loss = 0;
  for (let idx = 1; idx <= period; idx += 1) {
    const delta = values[idx] - values[idx - 1];
    if (delta >= 0) gain += delta;
    else loss += Math.abs(delta);
  }
  let avgGain = gain / period;
  let avgLoss = loss / period;
  out[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);

  for (let idx = period + 1; idx < values.length; idx += 1) {
    const delta = values[idx] - values[idx - 1];
    const up = delta > 0 ? delta : 0;
    const down = delta < 0 ? Math.abs(delta) : 0;
    avgGain = (avgGain * (period - 1) + up) / period;
    avgLoss = (avgLoss * (period - 1) + down) / period;
    out[idx] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  }
  return out;
}

function stddev(values: number[]): number {
  if (!values.length) return 0;
  const mean = values.reduce((sum, item) => sum + item, 0) / values.length;
  const variance = values.reduce((sum, item) => sum + (item - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

function bbands(values: number[], period = 20, multiplier = 2): { upper: Array<number | null>; middle: Array<number | null>; lower: Array<number | null> } {
  const middle = sma(values, period);
  const upper = createEmptySeries(values.length);
  const lower = createEmptySeries(values.length);
  for (let idx = period - 1; idx < values.length; idx += 1) {
    const window = values.slice(idx - period + 1, idx + 1);
    const sigma = stddev(window);
    const mid = middle[idx];
    if (!Number.isFinite(mid as number)) continue;
    upper[idx] = Number(mid) + multiplier * sigma;
    lower[idx] = Number(mid) - multiplier * sigma;
  }
  return { upper, middle, lower };
}

function trueRange(high: number[], low: number[], close: number[]): number[] {
  const out: number[] = [];
  for (let idx = 0; idx < high.length; idx += 1) {
    if (idx === 0) {
      out.push(high[idx] - low[idx]);
      continue;
    }
    const hl = high[idx] - low[idx];
    const hc = Math.abs(high[idx] - close[idx - 1]);
    const lc = Math.abs(low[idx] - close[idx - 1]);
    out.push(Math.max(hl, hc, lc));
  }
  return out;
}

function wilder(values: number[], period: number): Array<number | null> {
  const out = createEmptySeries(values.length);
  if (values.length < period) return out;
  let sum = 0;
  for (let idx = 0; idx < period; idx += 1) sum += values[idx] || 0;
  out[period - 1] = sum / period;
  let prev = out[period - 1] as number;
  for (let idx = period; idx < values.length; idx += 1) {
    prev = (prev * (period - 1) + (values[idx] || 0)) / period;
    out[idx] = prev;
  }
  return out;
}

function atr(high: number[], low: number[], close: number[], period = 14): Array<number | null> {
  return wilder(trueRange(high, low, close), period);
}

function adx(high: number[], low: number[], close: number[], period = 14): {
  adx: Array<number | null>;
  plusDi: Array<number | null>;
  minusDi: Array<number | null>;
} {
  const length = close.length;
  const plusDm: number[] = Array.from({ length }, () => 0);
  const minusDm: number[] = Array.from({ length }, () => 0);
  for (let idx = 1; idx < length; idx += 1) {
    const upMove = high[idx] - high[idx - 1];
    const downMove = low[idx - 1] - low[idx];
    plusDm[idx] = upMove > downMove && upMove > 0 ? upMove : 0;
    minusDm[idx] = downMove > upMove && downMove > 0 ? downMove : 0;
  }

  const tr = trueRange(high, low, close);
  const atrValues = wilder(tr, period);
  const plusSmoothed = wilder(plusDm, period);
  const minusSmoothed = wilder(minusDm, period);

  const plusDi = createEmptySeries(length);
  const minusDi = createEmptySeries(length);
  const dxValues = createEmptySeries(length);
  for (let idx = 0; idx < length; idx += 1) {
    const atrNow = atrValues[idx];
    const plusNow = plusSmoothed[idx];
    const minusNow = minusSmoothed[idx];
    if (!Number.isFinite(atrNow as number) || Number(atrNow) <= 0) continue;
    const plus = (100 * Number(plusNow || 0)) / Number(atrNow);
    const minus = (100 * Number(minusNow || 0)) / Number(atrNow);
    plusDi[idx] = plus;
    minusDi[idx] = minus;
    const denom = plus + minus;
    if (denom > 0) dxValues[idx] = (100 * Math.abs(plus - minus)) / denom;
  }

  const dxNumeric = dxValues.map((item) => (Number.isFinite(item as number) ? Number(item) : 0));
  const adxValues = wilder(dxNumeric, period);
  return { adx: adxValues, plusDi, minusDi };
}

function cci(high: number[], low: number[], close: number[], period = 20): Array<number | null> {
  const tp = close.map((item, idx) => (high[idx] + low[idx] + item) / 3);
  const smaTp = sma(tp, period);
  const out = createEmptySeries(close.length);
  for (let idx = period - 1; idx < close.length; idx += 1) {
    const mean = smaTp[idx];
    if (!Number.isFinite(mean as number)) continue;
    const window = tp.slice(idx - period + 1, idx + 1);
    const avgDev = window.reduce((sum, item) => sum + Math.abs(item - Number(mean)), 0) / period;
    if (avgDev === 0) continue;
    out[idx] = (tp[idx] - Number(mean)) / (0.015 * avgDev);
  }
  return out;
}

function mfi(high: number[], low: number[], close: number[], volume: number[], period = 14): Array<number | null> {
  const out = createEmptySeries(close.length);
  const typical = close.map((item, idx) => (high[idx] + low[idx] + item) / 3);
  const positive: number[] = Array.from({ length: close.length }, () => 0);
  const negative: number[] = Array.from({ length: close.length }, () => 0);
  for (let idx = 1; idx < close.length; idx += 1) {
    const flow = typical[idx] * volume[idx];
    if (typical[idx] > typical[idx - 1]) positive[idx] = flow;
    else if (typical[idx] < typical[idx - 1]) negative[idx] = flow;
  }
  for (let idx = period; idx < close.length; idx += 1) {
    const pos = positive.slice(idx - period + 1, idx + 1).reduce((sum, item) => sum + item, 0);
    const neg = negative.slice(idx - period + 1, idx + 1).reduce((sum, item) => sum + item, 0);
    if (neg === 0 && pos === 0) continue;
    if (neg === 0) {
      out[idx] = 100;
      continue;
    }
    const ratio = pos / neg;
    out[idx] = 100 - 100 / (1 + ratio);
  }
  return out;
}

function obv(close: number[], volume: number[]): Array<number | null> {
  const out = createEmptySeries(close.length);
  let running = 0;
  out[0] = 0;
  for (let idx = 1; idx < close.length; idx += 1) {
    if (close[idx] > close[idx - 1]) running += volume[idx] || 0;
    else if (close[idx] < close[idx - 1]) running -= volume[idx] || 0;
    out[idx] = running;
  }
  return out;
}

function roc(close: number[], period = 12): Array<number | null> {
  const out = createEmptySeries(close.length);
  for (let idx = period; idx < close.length; idx += 1) {
    const prev = close[idx - period];
    if (!Number.isFinite(prev) || prev === 0) continue;
    out[idx] = ((close[idx] - prev) / prev) * 100;
  }
  return out;
}

function stoch(high: number[], low: number[], close: number[], period = 14, smooth = 3): { k: Array<number | null>; d: Array<number | null> } {
  const k = createEmptySeries(close.length);
  for (let idx = period - 1; idx < close.length; idx += 1) {
    const highWindow = Math.max(...high.slice(idx - period + 1, idx + 1));
    const lowWindow = Math.min(...low.slice(idx - period + 1, idx + 1));
    const range = highWindow - lowWindow;
    if (range === 0) continue;
    k[idx] = ((close[idx] - lowWindow) / range) * 100;
  }
  const kNumeric = k.map((item) => (Number.isFinite(item as number) ? Number(item) : 0));
  const d = sma(kNumeric, smooth);
  return { k, d };
}

function willr(high: number[], low: number[], close: number[], period = 14): Array<number | null> {
  const out = createEmptySeries(close.length);
  for (let idx = period - 1; idx < close.length; idx += 1) {
    const highWindow = Math.max(...high.slice(idx - period + 1, idx + 1));
    const lowWindow = Math.min(...low.slice(idx - period + 1, idx + 1));
    const range = highWindow - lowWindow;
    if (range === 0) continue;
    out[idx] = ((highWindow - close[idx]) / range) * -100;
  }
  return out;
}

function pushSeries(
  rows: IndicatorSeriesItem[],
  latest: IndicatorLatestRow[],
  latestNumeric: Record<string, number>,
  name: string,
  values: Array<number | null>
): void {
  rows.push({ name, values });
  const current = latestFinite(values);
  if (typeof current === "number" && Number.isFinite(current)) {
    latestNumeric[name] = current;
  }
  latest.push({
    name,
    value: typeof current === "number" && Number.isFinite(current) ? current : null,
    display: formatIndicatorValue(name, typeof current === "number" && Number.isFinite(current) ? current : null),
  });
}

function computeIndicators(input: {
  ticker: string;
  interval: "1d" | "1h";
  lookback: number;
  selectedIndicators: IndicatorCode[];
  candles: CandlePoint[];
  maxPoints: number;
}): IndicatorComputation {
  const candles = input.candles;
  const dates = candles.map((item) => item.date);
  const close = candles.map((item) => item.close);
  const high = candles.map((item) => item.high);
  const low = candles.map((item) => item.low);
  const volume = candles.map((item) => item.volume);

  const seriesItems: IndicatorSeriesItem[] = [];
  const latestRows: IndicatorLatestRow[] = [];
  const latestNumeric: Record<string, number> = {};

  input.selectedIndicators.forEach((indicator) => {
    if (indicator === "RSI") {
      pushSeries(seriesItems, latestRows, latestNumeric, "RSI_14", rsi(close, 14));
      return;
    }
    if (indicator === "MACD") {
      const ema12 = ema(close, 12);
      const ema26 = ema(close, 26);
      const macdLine = createEmptySeries(close.length);
      for (let idx = 0; idx < close.length; idx += 1) {
        const a = ema12[idx];
        const b = ema26[idx];
        if (Number.isFinite(a as number) && Number.isFinite(b as number)) macdLine[idx] = Number(a) - Number(b);
      }
      const macdNumeric = macdLine.map((item) => (Number.isFinite(item as number) ? Number(item) : 0));
      const signal = ema(macdNumeric, 9);
      const hist = createEmptySeries(close.length);
      for (let idx = 0; idx < close.length; idx += 1) {
        const line = macdLine[idx];
        const sig = signal[idx];
        if (Number.isFinite(line as number) && Number.isFinite(sig as number)) hist[idx] = Number(line) - Number(sig);
      }
      pushSeries(seriesItems, latestRows, latestNumeric, "MACD_LINE", macdLine);
      pushSeries(seriesItems, latestRows, latestNumeric, "MACD_SIGNAL", signal);
      pushSeries(seriesItems, latestRows, latestNumeric, "MACD_HIST", hist);
      return;
    }
    if (indicator === "SMA") {
      pushSeries(seriesItems, latestRows, latestNumeric, "SMA_20", sma(close, 20));
      return;
    }
    if (indicator === "EMA") {
      pushSeries(seriesItems, latestRows, latestNumeric, "EMA_20", ema(close, 20));
      return;
    }
    if (indicator === "BBANDS") {
      const bands = bbands(close, 20, 2);
      pushSeries(seriesItems, latestRows, latestNumeric, "BBANDS_UPPER", bands.upper);
      pushSeries(seriesItems, latestRows, latestNumeric, "BBANDS_MIDDLE", bands.middle);
      pushSeries(seriesItems, latestRows, latestNumeric, "BBANDS_LOWER", bands.lower);
      return;
    }
    if (indicator === "ATR") {
      pushSeries(seriesItems, latestRows, latestNumeric, "ATR_14", atr(high, low, close, 14));
      return;
    }
    if (indicator === "ADX") {
      const adxPack = adx(high, low, close, 14);
      pushSeries(seriesItems, latestRows, latestNumeric, "ADX_14", adxPack.adx);
      pushSeries(seriesItems, latestRows, latestNumeric, "PLUS_DI_14", adxPack.plusDi);
      pushSeries(seriesItems, latestRows, latestNumeric, "MINUS_DI_14", adxPack.minusDi);
      return;
    }
    if (indicator === "CCI") {
      pushSeries(seriesItems, latestRows, latestNumeric, "CCI_20", cci(high, low, close, 20));
      return;
    }
    if (indicator === "MFI") {
      pushSeries(seriesItems, latestRows, latestNumeric, "MFI_14", mfi(high, low, close, volume, 14));
      return;
    }
    if (indicator === "OBV") {
      pushSeries(seriesItems, latestRows, latestNumeric, "OBV", obv(close, volume));
      return;
    }
    if (indicator === "ROC") {
      pushSeries(seriesItems, latestRows, latestNumeric, "ROC_12", roc(close, 12));
      return;
    }
    if (indicator === "STOCH") {
      const stochPack = stoch(high, low, close, 14, 3);
      pushSeries(seriesItems, latestRows, latestNumeric, "STOCH_K", stochPack.k);
      pushSeries(seriesItems, latestRows, latestNumeric, "STOCH_D", stochPack.d);
      return;
    }
    if (indicator === "WILLR") {
      pushSeries(seriesItems, latestRows, latestNumeric, "WILLR_14", willr(high, low, close, 14));
    }
  });

  const seriesTrim = Math.max(60, Math.min(input.maxPoints, dates.length));
  const from = Math.max(0, dates.length - seriesTrim);
  const trimmedDates = dates.slice(from);
  const trimmedItems = seriesItems.map((item) => ({
    name: item.name,
    values: item.values.slice(from),
  }));

  const lastClose = close[close.length - 1];
  const previousClose = close.length > 1 ? close[close.length - 2] : null;

  return {
    ticker: input.ticker,
    interval: input.interval,
    lookback: input.lookback,
    selectedIndicators: input.selectedIndicators,
    candles,
    dates: trimmedDates,
    close,
    high,
    low,
    volume,
    seriesItems: trimmedItems,
    latestRows,
    latestNumeric,
    asOf: candles[candles.length - 1]?.date || new Date().toISOString(),
    lastClose,
    previousClose,
  };
}

function latestNumericOrComputed(
  latestNumeric: Record<string, number>,
  key: string,
  fallbackSeries: Array<number | null>
): number | null {
  if (Number.isFinite(latestNumeric[key])) return Number(latestNumeric[key]);
  return latestFinite(fallbackSeries);
}

function deriveCoreIndicatorSnapshot(computed: IndicatorComputation): CoreIndicatorSnapshot {
  const rsiSeries = rsi(computed.close, 14);
  const ema20Series = ema(computed.close, 20);
  const sma20Series = sma(computed.close, 20);
  const atrSeries = atr(computed.high, computed.low, computed.close, 14);
  const adxPack = adx(computed.high, computed.low, computed.close, 14);
  const bbandsPack = bbands(computed.close, 20, 2);
  const cciSeries = cci(computed.high, computed.low, computed.close, 20);
  const mfiSeries = mfi(computed.high, computed.low, computed.close, computed.volume, 14);
  const rocSeries = roc(computed.close, 12);
  const stochPack = stoch(computed.high, computed.low, computed.close, 14, 3);
  const willrSeries = willr(computed.high, computed.low, computed.close, 14);
  const obvSeries = obv(computed.close, computed.volume);
  const ema12Series = ema(computed.close, 12);
  const ema26Series = ema(computed.close, 26);
  const macdLineSeries = createEmptySeries(computed.close.length);
  for (let idx = 0; idx < computed.close.length; idx += 1) {
    const fast = ema12Series[idx];
    const slow = ema26Series[idx];
    if (Number.isFinite(fast as number) && Number.isFinite(slow as number)) {
      macdLineSeries[idx] = Number(fast) - Number(slow);
    }
  }
  const macdSignalSeries = ema(
    macdLineSeries.map((item) => (Number.isFinite(item as number) ? Number(item) : 0)),
    9
  );
  const macdHistSeries = createEmptySeries(computed.close.length);
  for (let idx = 0; idx < computed.close.length; idx += 1) {
    const line = macdLineSeries[idx];
    const signal = macdSignalSeries[idx];
    if (Number.isFinite(line as number) && Number.isFinite(signal as number)) {
      macdHistSeries[idx] = Number(line) - Number(signal);
    }
  }

  const recentReturns = computed.close
    .slice(-21)
    .map((close, idx, arr) => {
      if (idx === 0) return null;
      const prev = arr[idx - 1];
      if (!Number.isFinite(prev) || prev <= 0 || !Number.isFinite(close) || close <= 0) return null;
      return Math.log(close / prev);
    })
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  const realizedVol20 = recentReturns.length ? stddev(recentReturns) : null;
  const recentHighs = computed.high.slice(-20).filter((value) => Number.isFinite(value));
  const recentLows = computed.low.slice(-20).filter((value) => Number.isFinite(value));
  const recentRangePct20 =
    Number.isFinite(computed.lastClose) && computed.lastClose > 0 && recentHighs.length && recentLows.length
      ? (Math.max(...recentHighs) - Math.min(...recentLows)) / computed.lastClose
      : null;
  const obvNow = latestFinite(obvSeries);
  const obvPast =
    obvSeries.length > 10
      ? latestFinite(obvSeries.slice(0, Math.max(0, obvSeries.length - 10)))
      : null;
  const obvSlope10 =
    Number.isFinite(obvNow as number) && Number.isFinite(obvPast as number) ? Number(obvNow) - Number(obvPast) : null;

  const atr14 = latestNumericOrComputed(computed.latestNumeric, "ATR_14", atrSeries);
  return {
    rsi14: latestNumericOrComputed(computed.latestNumeric, "RSI_14", rsiSeries),
    macdLine: latestNumericOrComputed(computed.latestNumeric, "MACD_LINE", macdLineSeries),
    macdSignal: latestNumericOrComputed(computed.latestNumeric, "MACD_SIGNAL", macdSignalSeries),
    macdHist: latestNumericOrComputed(computed.latestNumeric, "MACD_HIST", macdHistSeries),
    atr14,
    atrPct:
      Number.isFinite(atr14 as number) && Number.isFinite(computed.lastClose) && computed.lastClose > 0
        ? Number(atr14) / computed.lastClose
        : null,
    adx14: latestNumericOrComputed(computed.latestNumeric, "ADX_14", adxPack.adx),
    plusDi14: latestNumericOrComputed(computed.latestNumeric, "PLUS_DI_14", adxPack.plusDi),
    minusDi14: latestNumericOrComputed(computed.latestNumeric, "MINUS_DI_14", adxPack.minusDi),
    ema20: latestNumericOrComputed(computed.latestNumeric, "EMA_20", ema20Series),
    sma20: latestNumericOrComputed(computed.latestNumeric, "SMA_20", sma20Series),
    bbandsUpper: latestNumericOrComputed(computed.latestNumeric, "BBANDS_UPPER", bbandsPack.upper),
    bbandsMiddle: latestNumericOrComputed(computed.latestNumeric, "BBANDS_MIDDLE", bbandsPack.middle),
    bbandsLower: latestNumericOrComputed(computed.latestNumeric, "BBANDS_LOWER", bbandsPack.lower),
    cci20: latestNumericOrComputed(computed.latestNumeric, "CCI_20", cciSeries),
    mfi14: latestNumericOrComputed(computed.latestNumeric, "MFI_14", mfiSeries),
    roc12: latestNumericOrComputed(computed.latestNumeric, "ROC_12", rocSeries),
    stochK: latestNumericOrComputed(computed.latestNumeric, "STOCH_K", stochPack.k),
    stochD: latestNumericOrComputed(computed.latestNumeric, "STOCH_D", stochPack.d),
    willr14: latestNumericOrComputed(computed.latestNumeric, "WILLR_14", willrSeries),
    realizedVol20,
    recentRangePct20,
    obvSlope10,
  };
}

function hasSelectedIndicator(computed: IndicatorComputation, indicator: IndicatorCode): boolean {
  return computed.selectedIndicators.includes(indicator);
}

function buildSelectedIndicatorSignals(computed: IndicatorComputation, snapshot: CoreIndicatorSnapshot): string[] {
  const signals: string[] = [];
  const lastClose = Number(computed.lastClose);

  if (hasSelectedIndicator(computed, "RSI") && Number.isFinite(snapshot.rsi14 as number)) {
    const rsiValue = Number(snapshot.rsi14);
    const tone =
      rsiValue <= 30 ? "is oversold and can support a reflex bounce if momentum stabilizes" :
      rsiValue >= 70 ? "is overbought and vulnerable to mean reversion" :
      rsiValue >= 55 ? "leans bullish" :
      rsiValue <= 45 ? "leans bearish" :
      "is neutral";
    signals.push(`RSI(14) ${rsiValue.toFixed(2)} ${tone}.`);
  }

  if (hasSelectedIndicator(computed, "MACD") && Number.isFinite(snapshot.macdHist as number)) {
    const hist = Number(snapshot.macdHist);
    const line = Number(snapshot.macdLine || 0);
    const signal = Number(snapshot.macdSignal || 0);
    const tone = hist > 0 ? "momentum is improving above the signal line" : hist < 0 ? "momentum remains below the signal line" : "momentum is flat";
    signals.push(`MACD histogram ${hist.toFixed(4)} while MACD line ${line.toFixed(4)} vs signal ${signal.toFixed(4)} means ${tone}.`);
  }

  if (hasSelectedIndicator(computed, "EMA") && Number.isFinite(snapshot.ema20 as number) && Number.isFinite(lastClose)) {
    const gapPct = percentDelta(snapshot.ema20, lastClose, 2);
    signals.push(`Price is ${formatSignedPctText(gapPct)} versus EMA(20), which ${Number(gapPct || 0) >= 0 ? "keeps short-term trend support intact" : "shows price is still below short-term trend"}.`);
  }

  if (hasSelectedIndicator(computed, "SMA") && Number.isFinite(snapshot.sma20 as number) && Number.isFinite(lastClose)) {
    const gapPct = percentDelta(snapshot.sma20, lastClose, 2);
    signals.push(`Price is ${formatSignedPctText(gapPct)} versus SMA(20), reinforcing a ${Number(gapPct || 0) >= 0 ? "constructive" : "defensive"} baseline.`);
  }

  if (hasSelectedIndicator(computed, "BBANDS") && Number.isFinite(snapshot.bbandsUpper as number) && Number.isFinite(snapshot.bbandsLower as number)) {
    const upper = Number(snapshot.bbandsUpper);
    const lower = Number(snapshot.bbandsLower);
    const middle = Number(snapshot.bbandsMiddle || (upper + lower) / 2);
    const tone =
      lastClose >= upper ? "price is pressing the upper band" :
      lastClose <= lower ? "price is testing the lower band" :
      lastClose >= middle ? "price is holding the upper half of the band structure" :
      "price is trading in the lower half of the band structure";
    signals.push(`Bollinger Bands place the middle line at ${middle.toFixed(2)} and ${tone}.`);
  }

  if (hasSelectedIndicator(computed, "ATR") && Number.isFinite(snapshot.atr14 as number)) {
    const atrPct = snapshot.atrPct !== null ? snapshot.atrPct * 100 : null;
    signals.push(`ATR(14) is ${Number(snapshot.atr14).toFixed(2)}${atrPct !== null ? `, about ${atrPct.toFixed(2)}% of price` : ""}, which frames the current daily swing size.`);
  }

  if (hasSelectedIndicator(computed, "ADX") && Number.isFinite(snapshot.adx14 as number)) {
    const adxText =
      Number(snapshot.adx14) >= 25 ? "trend strength is confirmed" : "trend strength is modest";
    const diBias =
      Number.isFinite(snapshot.plusDi14 as number) && Number.isFinite(snapshot.minusDi14 as number)
        ? Number(snapshot.plusDi14) > Number(snapshot.minusDi14)
          ? "with +DI above -DI"
          : Number(snapshot.plusDi14) < Number(snapshot.minusDi14)
            ? "with -DI above +DI"
            : "with DI lines balanced"
        : "";
    signals.push(`ADX(14) is ${Number(snapshot.adx14).toFixed(2)} ${diBias ? `${diBias}, so ` : "so "}${adxText}.`);
  }

  if (hasSelectedIndicator(computed, "CCI") && Number.isFinite(snapshot.cci20 as number)) {
    const cciValue = Number(snapshot.cci20);
    const tone = cciValue >= 100 ? "shows strong upside extension" : cciValue <= -100 ? "shows strong downside extension" : "is near its mid-range";
    signals.push(`CCI(20) ${cciValue.toFixed(2)} ${tone}.`);
  }

  if (hasSelectedIndicator(computed, "MFI") && Number.isFinite(snapshot.mfi14 as number)) {
    const mfiValue = Number(snapshot.mfi14);
    const tone = mfiValue >= 80 ? "points to heavy buying pressure" : mfiValue <= 20 ? "points to heavy selling pressure" : "shows balanced money flow";
    signals.push(`MFI(14) ${mfiValue.toFixed(2)} ${tone}.`);
  }

  if (hasSelectedIndicator(computed, "OBV") && Number.isFinite(snapshot.obvSlope10 as number)) {
    const slope = Number(snapshot.obvSlope10);
    signals.push(`OBV changed by ${Math.round(slope).toLocaleString()} over the last 10 bars, which ${slope >= 0 ? "supports accumulation" : "points to distribution"}.`);
  }

  if (hasSelectedIndicator(computed, "ROC") && Number.isFinite(snapshot.roc12 as number)) {
    const rocValue = Number(snapshot.roc12);
    signals.push(`ROC(12) ${rocValue.toFixed(2)}% keeps price momentum ${rocValue >= 0 ? "positive" : "negative"}.`);
  }

  if (hasSelectedIndicator(computed, "STOCH") && Number.isFinite(snapshot.stochK as number) && Number.isFinite(snapshot.stochD as number)) {
    const k = Number(snapshot.stochK);
    const d = Number(snapshot.stochD);
    const tone = k <= 20 ? "is oversold" : k >= 80 ? "is overbought" : "is mid-range";
    signals.push(`Stochastic K/D is ${k.toFixed(2)}/${d.toFixed(2)} and ${tone}${k > d ? " with an improving cross" : k < d ? " with a weakening cross" : ""}.`);
  }

  if (hasSelectedIndicator(computed, "WILLR") && Number.isFinite(snapshot.willr14 as number)) {
    const willrValue = Number(snapshot.willr14);
    const tone = willrValue <= -80 ? "sits in oversold territory" : willrValue >= -20 ? "sits in overbought territory" : "is in a neutral zone";
    signals.push(`Williams %R(14) ${willrValue.toFixed(2)} ${tone}.`);
  }

  if (signals.length < 3) {
    const volPct = Number.isFinite(snapshot.realizedVol20 as number) ? Number(snapshot.realizedVol20) * 100 : null;
    const rangePct = Number.isFinite(snapshot.recentRangePct20 as number) ? Number(snapshot.recentRangePct20) * 100 : null;
    signals.push(
      `Recent price context shows ${volPct !== null ? `${volPct.toFixed(2)}%` : "n/a"} realized 20-bar volatility${
        rangePct !== null ? ` and a ${rangePct.toFixed(2)}% 20-bar high-low range` : ""
      }.`
    );
  }

  return Array.from(new Set(signals)).slice(0, 6);
}

function formatSignedPctText(value: number | null): string {
  if (!Number.isFinite(value as number)) return "flat";
  const numeric = Number(value);
  return `${numeric >= 0 ? "+" : ""}${numeric.toFixed(2)}%`;
}

function estimateIndicatorTimelineDays(computed: IndicatorComputation, conviction: number): number {
  const interval = computed.interval;
  const trendSignals = ["EMA", "SMA", "ADX", "BBANDS"].filter((item) => hasSelectedIndicator(computed, item as IndicatorCode)).length;
  const oscillatorSignals = ["RSI", "MACD", "CCI", "MFI", "STOCH", "WILLR", "ROC"].filter((item) =>
    hasSelectedIndicator(computed, item as IndicatorCode)
  ).length;
  let base = interval === "1h" ? 4 : 9;
  if (trendSignals > oscillatorSignals) base += interval === "1h" ? 2 : 4;
  if (oscillatorSignals > trendSignals) base -= interval === "1h" ? 1 : 1;
  if (conviction >= 0.7) base += interval === "1h" ? 1 : 3;
  if (conviction <= 0.25) base -= interval === "1h" ? 1 : 2;
  const bounds = indicatorTimelineBounds(interval);
  return Math.max(bounds.min, Math.min(bounds.max, Math.round(base)));
}

function estimateIndicatorMovePct(
  computed: IndicatorComputation,
  snapshot: CoreIndicatorSnapshot,
  timelineDays: number,
  conviction: number
): number {
  const lastClose = Number(computed.lastClose);
  const atrPct = Number.isFinite(snapshot.atrPct as number) ? Number(snapshot.atrPct) : 0;
  const realizedVol = Number.isFinite(snapshot.realizedVol20 as number) ? Number(snapshot.realizedVol20) : 0;
  const recentRangePct = Number.isFinite(snapshot.recentRangePct20 as number) ? Number(snapshot.recentRangePct20) : 0;
  const perStepVolPct = Math.max(
    atrPct,
    realizedVol * 1.35,
    recentRangePct > 0 ? recentRangePct / 10 : 0,
    computed.interval === "1h" ? 0.0035 : 0.006
  );
  const horizonScale = Math.sqrt(Math.max(1, timelineDays));
  const convictionScale = 0.9 + conviction * 0.9;
  const movePct = perStepVolPct * horizonScale * convictionScale;
  const minMove = computed.interval === "1h" ? 0.003 : 0.005;
  const maxMove = computed.interval === "1h" ? 0.12 : 0.2;
  if (!Number.isFinite(lastClose) || lastClose <= 0) return minMove;
  return Math.max(minMove, Math.min(maxMove, movePct));
}

function scoreIndicatorDirection(
  computed: IndicatorComputation,
  snapshot: CoreIndicatorSnapshot
): { direction: IndicatorPrediction["direction"]; conviction: number; confidence: IndicatorPrediction["confidence"] } {
  let bullish = 0;
  let bearish = 0;

  const addBullish = (value: number) => {
    bullish += Math.max(0, value);
  };
  const addBearish = (value: number) => {
    bearish += Math.max(0, value);
  };

  if (hasSelectedIndicator(computed, "RSI") && Number.isFinite(snapshot.rsi14 as number)) {
    const rsiValue = Number(snapshot.rsi14);
    if (rsiValue <= 30) addBullish(0.9 + (30 - rsiValue) / 20);
    else if (rsiValue >= 70) addBearish(0.9 + (rsiValue - 70) / 20);
    else if (rsiValue < 45) addBearish(0.45);
    else if (rsiValue > 55) addBullish(0.45);
  }

  if (hasSelectedIndicator(computed, "MACD") && Number.isFinite(snapshot.macdHist as number)) {
    const histPct = Number.isFinite(snapshot.macdHist as number) && Number.isFinite(computed.lastClose) && computed.lastClose > 0
      ? Math.abs(Number(snapshot.macdHist) / computed.lastClose) * 100
      : 0;
    const weight = Math.min(1.8, 0.9 + histPct * 8);
    if (Number(snapshot.macdHist) > 0) addBullish(weight);
    else if (Number(snapshot.macdHist) < 0) addBearish(weight);
  }

  if (hasSelectedIndicator(computed, "EMA") && Number.isFinite(snapshot.ema20 as number) && Number.isFinite(computed.lastClose)) {
    const gap = Number(computed.lastClose) - Number(snapshot.ema20);
    if (gap > 0) addBullish(0.8);
    else if (gap < 0) addBearish(0.8);
  }

  if (hasSelectedIndicator(computed, "SMA") && Number.isFinite(snapshot.sma20 as number) && Number.isFinite(computed.lastClose)) {
    const gap = Number(computed.lastClose) - Number(snapshot.sma20);
    if (gap > 0) addBullish(0.7);
    else if (gap < 0) addBearish(0.7);
  }

  if (hasSelectedIndicator(computed, "BBANDS") && Number.isFinite(snapshot.bbandsUpper as number) && Number.isFinite(snapshot.bbandsLower as number)) {
    if (computed.lastClose <= Number(snapshot.bbandsLower)) addBullish(0.8);
    else if (computed.lastClose >= Number(snapshot.bbandsUpper)) addBearish(0.8);
    else if (Number.isFinite(snapshot.bbandsMiddle as number)) {
      if (computed.lastClose >= Number(snapshot.bbandsMiddle)) addBullish(0.4);
      else addBearish(0.4);
    }
  }

  if (hasSelectedIndicator(computed, "ADX") && Number.isFinite(snapshot.adx14 as number) && Number.isFinite(snapshot.plusDi14 as number) && Number.isFinite(snapshot.minusDi14 as number)) {
    const strengthScale = Math.min(1.2, Math.max(0.3, Number(snapshot.adx14) / 30));
    if (Number(snapshot.plusDi14) > Number(snapshot.minusDi14)) addBullish(strengthScale);
    else if (Number(snapshot.minusDi14) > Number(snapshot.plusDi14)) addBearish(strengthScale);
  }

  if (hasSelectedIndicator(computed, "CCI") && Number.isFinite(snapshot.cci20 as number)) {
    if (Number(snapshot.cci20) >= 100) addBullish(0.85);
    else if (Number(snapshot.cci20) <= -100) addBearish(0.85);
  }

  if (hasSelectedIndicator(computed, "MFI") && Number.isFinite(snapshot.mfi14 as number)) {
    if (Number(snapshot.mfi14) >= 80) addBullish(0.6);
    else if (Number(snapshot.mfi14) <= 20) addBearish(0.6);
  }

  if (hasSelectedIndicator(computed, "OBV") && Number.isFinite(snapshot.obvSlope10 as number)) {
    if (Number(snapshot.obvSlope10) > 0) addBullish(0.55);
    else if (Number(snapshot.obvSlope10) < 0) addBearish(0.55);
  }

  if (hasSelectedIndicator(computed, "ROC") && Number.isFinite(snapshot.roc12 as number)) {
    if (Number(snapshot.roc12) > 0) addBullish(0.7);
    else if (Number(snapshot.roc12) < 0) addBearish(0.7);
  }

  if (hasSelectedIndicator(computed, "STOCH") && Number.isFinite(snapshot.stochK as number) && Number.isFinite(snapshot.stochD as number)) {
    const bullishCross = Number(snapshot.stochK) > Number(snapshot.stochD);
    if (Number(snapshot.stochK) <= 20) addBullish(bullishCross ? 0.8 : 0.55);
    else if (Number(snapshot.stochK) >= 80) addBearish(!bullishCross ? 0.8 : 0.55);
    else if (bullishCross) addBullish(0.35);
    else addBearish(0.35);
  }

  if (hasSelectedIndicator(computed, "WILLR") && Number.isFinite(snapshot.willr14 as number)) {
    if (Number(snapshot.willr14) <= -80) addBullish(0.65);
    else if (Number(snapshot.willr14) >= -20) addBearish(0.65);
  }

  const total = bullish + bearish;
  const net = bullish - bearish;
  const conviction = total > 0 ? Math.min(1, Math.abs(net) / total) : 0;
  const direction: IndicatorPrediction["direction"] =
    conviction < 0.18 ? "neutral" : net > 0 ? "bullish" : net < 0 ? "bearish" : "neutral";
  const confidence: IndicatorPrediction["confidence"] =
    conviction >= 0.65 && total >= 2.2 ? "high" : conviction >= 0.32 && total >= 1.2 ? "medium" : "low";
  return { direction, conviction, confidence };
}

function parseJsonObject(text: string): Record<string, unknown> | null {
  const raw = String(text || "").trim();
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") return parsed as Record<string, unknown>;
  } catch {
    // fall through
  }
  const first = raw.indexOf("{");
  const last = raw.lastIndexOf("}");
  if (first >= 0 && last > first) {
    const slice = raw.slice(first, last + 1);
    try {
      const parsed = JSON.parse(slice);
      if (parsed && typeof parsed === "object") return parsed as Record<string, unknown>;
    } catch {
      return null;
    }
  }
  return null;
}

function buildIndicatorPromptContext(computed: IndicatorComputation): Record<string, unknown> {
  const snapshot = deriveCoreIndicatorSnapshot(computed);
  const lastClose = roundFinite(computed.lastClose, 4);
  const previousClose = roundFinite(computed.previousClose, 4);
  const context = {
    asOf: computed.asOf,
    lastClose,
    previousClose,
    oneBarChangePct: percentDelta(previousClose, lastClose, 2),
    selectedIndicators: computed.selectedIndicators,
    latestIndicators: computed.latestRows.slice(0, 24).map((entry) => ({
      name: entry.name,
      value: roundFinite(entry.value, 6),
      display: entry.display,
    })),
    derivedSignals: {
      rsi14: roundFinite(snapshot.rsi14, 2),
      macdLine: roundFinite(snapshot.macdLine, 4),
      macdSignal: roundFinite(snapshot.macdSignal, 4),
      macdHist: roundFinite(snapshot.macdHist, 4),
      adx14: roundFinite(snapshot.adx14, 2),
      plusDi14: roundFinite(snapshot.plusDi14, 2),
      minusDi14: roundFinite(snapshot.minusDi14, 2),
      atr14: roundFinite(snapshot.atr14, 4),
      atrPct: roundFinite(snapshot.atrPct !== null ? snapshot.atrPct * 100 : null, 2),
      ema20: roundFinite(snapshot.ema20, 4),
      sma20: roundFinite(snapshot.sma20, 4),
      closeVsEma20Pct: percentDelta(snapshot.ema20, lastClose, 2),
      closeVsSma20Pct: percentDelta(snapshot.sma20, lastClose, 2),
      bbandsUpper: roundFinite(snapshot.bbandsUpper, 4),
      bbandsMiddle: roundFinite(snapshot.bbandsMiddle, 4),
      bbandsLower: roundFinite(snapshot.bbandsLower, 4),
      cci20: roundFinite(snapshot.cci20, 2),
      mfi14: roundFinite(snapshot.mfi14, 2),
      roc12: roundFinite(snapshot.roc12, 2),
      stochK: roundFinite(snapshot.stochK, 2),
      stochD: roundFinite(snapshot.stochD, 2),
      willr14: roundFinite(snapshot.willr14, 2),
      realizedVol20Pct: roundFinite(snapshot.realizedVol20 !== null ? snapshot.realizedVol20 * 100 : null, 2),
      recentRangePct20: roundFinite(snapshot.recentRangePct20 !== null ? snapshot.recentRangePct20 * 100 : null, 2),
    },
  };
  return context;
}

function extractResponsesOutputText(payload: Record<string, unknown>): string {
  const direct = sanitizeText((payload as any).output_text, 24000);
  if (direct) return direct;
  const directParsed = (payload as any)?.output_parsed ?? (payload as any)?.parsed;
  if (directParsed && typeof directParsed === "object") {
    const serialized = JSON.stringify(directParsed);
    const text = sanitizeText(serialized, 24000);
    if (text) return text;
  }
  const output = Array.isArray((payload as any)?.output) ? ((payload as any).output as any[]) : [];
  const chunks: string[] = [];
  output.forEach((item) => {
    if (item?.parsed && typeof item.parsed === "object") {
      const serialized = sanitizeText(JSON.stringify(item.parsed), 24000);
      if (serialized) chunks.push(serialized);
    }
    const content = Array.isArray(item?.content) ? item.content : [];
    content.forEach((part: any) => {
      if (part?.parsed && typeof part.parsed === "object") {
        const serialized = sanitizeText(JSON.stringify(part.parsed), 24000);
        if (serialized) chunks.push(serialized);
      }
      if (part?.json && typeof part.json === "object") {
        const serialized = sanitizeText(JSON.stringify(part.json), 24000);
        if (serialized) chunks.push(serialized);
      }
      const text = sanitizeText(part?.text?.value ?? part?.text ?? part?.output_text ?? "", 24000);
      if (text) chunks.push(text);
    });
  });
  return sanitizeText(chunks.join("\n").trim(), 24000);
}

function extractFunctionCalls(payload: Record<string, unknown>): Array<{ callId: string; name: string; args: Record<string, unknown> }> {
  const output = Array.isArray((payload as any)?.output) ? ((payload as any).output as any[]) : [];
  const calls: Array<{ callId: string; name: string; args: Record<string, unknown> }> = [];
  const pushCall = (raw: any) => {
    const name = sanitizeText(raw?.name, 120);
    const callId = sanitizeText(raw?.call_id || raw?.id, 180);
    if (!name || !callId) return;
    const rawArgs = typeof raw?.arguments === "string" ? raw.arguments : JSON.stringify(raw?.arguments || {});
    const args = parseJsonObject(rawArgs) || {};
    calls.push({ callId, name, args });
  };
  output.forEach((item) => {
    if (item?.type === "function_call") pushCall(item);
    const content = Array.isArray(item?.content) ? item.content : [];
    content.forEach((part: any) => {
      if (part?.type === "function_call") pushCall(part);
    });
  });
  return calls;
}

function buildHeuristicAnalysis(computed: IndicatorComputation): IndicatorAnalysis {
  const snapshot = deriveCoreIndicatorSnapshot(computed);
  const lastClose = computed.lastClose;
  const directionalScore = scoreIndicatorDirection(computed, snapshot);
  const timelineDays = estimateIndicatorTimelineDays(computed, directionalScore.conviction);
  const projectedMove = estimateIndicatorMovePct(computed, snapshot, timelineDays, directionalScore.conviction);
  const signedMove =
    directionalScore.direction === "bullish"
      ? projectedMove
      : directionalScore.direction === "bearish"
        ? -projectedMove
        : 0;
  const targetPrice = Number.isFinite(lastClose) ? Number((lastClose * (1 + signedMove)).toFixed(2)) : null;

  const keySignals = buildSelectedIndicatorSignals(computed, snapshot);
  const summary = `Computed ${computed.latestRows.length} indicator values for ${computed.ticker}. Selected indicators lean ${directionalScore.direction}, with a ${timelineDays}-day tactical horizon.`;
  const prediction: IndicatorPrediction = {
    direction: directionalScore.direction,
    targetPrice,
    timeline: `${timelineDays} trading days`,
    timelineDays,
    confidence: directionalScore.confidence,
  };
  const text = buildIndicatorNarrative(summary, keySignals, prediction, computed);

  return {
    summary,
    keySignals,
    prediction,
    text,
    provider: "heuristic",
    model: "finta-style-rules",
    disclaimer: LLM_DISCLAIMER,
  };
}

function clampIndicatorTargetPrice(
  targetPrice: number,
  direction: IndicatorPrediction["direction"],
  computed: IndicatorComputation,
  fallbackTarget: number
): number {
  const lastClose = Number(computed.lastClose);
  if (!Number.isFinite(lastClose) || lastClose <= 0) {
    return Number.isFinite(targetPrice) && targetPrice > 0 ? Number(targetPrice.toFixed(2)) : Number(fallbackTarget.toFixed(2));
  }
  const snapshot = deriveCoreIndicatorSnapshot(computed);
  const bandPct = Math.max(
    computed.interval === "1h" ? 0.05 : 0.08,
    Math.min(
      computed.interval === "1h" ? 0.18 : 0.25,
      estimateIndicatorMovePct(computed, snapshot, indicatorTimelineBounds(computed.interval).max, 0.85) * 1.75
    )
  );
  const lower = lastClose * (1 - bandPct);
  const upper = lastClose * (1 + bandPct);
  let bounded = Number.isFinite(targetPrice) && targetPrice > 0 ? targetPrice : fallbackTarget;
  bounded = Math.min(upper, Math.max(lower, bounded));
  if (direction === "bullish" && bounded < lastClose) bounded = lastClose;
  if (direction === "bearish" && bounded > lastClose) bounded = lastClose;
  return Number(bounded.toFixed(2));
}

function normalizeIndicatorKeySignals(raw: unknown, fallback: string[]): string[] {
  const candidate = Array.isArray(raw)
    ? raw.map((item) => sanitizeText(item, 180)).filter(Boolean)
    : [];
  if (candidate.length >= 3) return candidate.slice(0, 6);
  const merged = [...candidate, ...fallback.map((item) => sanitizeText(item, 180)).filter(Boolean)];
  return Array.from(new Set(merged)).slice(0, 6);
}

function indicatorSignalMentionsUnselectedIndicator(text: string, computed: IndicatorComputation): boolean {
  const normalized = sanitizeText(text, 220).toUpperCase();
  if (!normalized) return false;
  const indicatorLabels: Array<{ code: IndicatorCode; tokens: string[] }> = [
    { code: "RSI", tokens: ["RSI"] },
    { code: "MACD", tokens: ["MACD"] },
    { code: "SMA", tokens: ["SMA"] },
    { code: "EMA", tokens: ["EMA"] },
    { code: "BBANDS", tokens: ["BOLLINGER", "BBANDS"] },
    { code: "ATR", tokens: ["ATR"] },
    { code: "ADX", tokens: ["ADX", "+DI", "-DI"] },
    { code: "CCI", tokens: ["CCI"] },
    { code: "MFI", tokens: ["MFI"] },
    { code: "OBV", tokens: ["OBV"] },
    { code: "ROC", tokens: ["ROC"] },
    { code: "STOCH", tokens: ["STOCHASTIC", "STOCH"] },
    { code: "WILLR", tokens: ["WILLR", "WILLIAMS %R", "WILLIAMS"] },
  ];
  return indicatorLabels.some(
    (entry) =>
      !hasSelectedIndicator(computed, entry.code) && entry.tokens.some((token) => normalized.includes(token))
  );
}

function buildIndicatorNarrative(
  summary: string,
  keySignals: string[],
  prediction: IndicatorPrediction,
  computed: IndicatorComputation
): string {
  const targetText = Number.isFinite(prediction.targetPrice as number) ? `$${Number(prediction.targetPrice).toFixed(2)}` : "the current price area";
  const riskLine =
    prediction.direction === "bullish"
      ? "Risk frame: bullish conviction weakens if momentum rolls over or price loses its short-term trend support."
      : prediction.direction === "bearish"
        ? "Risk frame: bearish conviction weakens if momentum turns up or price reclaims short-term trend support."
        : "Risk frame: signals are mixed, so avoid over-weighting any single indicator reading.";
  const setupLine = `Setup: ${summary}`;
  const pathLine = `Path: ${prediction.direction} bias toward ${targetText} over ${prediction.timeline} with ${prediction.confidence} confidence.`;
  const signalLines = keySignals.slice(0, 4).map((item) => `- ${item}`);
  const asOfLine = `As of ${computed.asOf}, last close was $${Number(computed.lastClose).toFixed(2)} on the ${computed.interval} interval.`;
  return [setupLine, asOfLine, "Signal stack:", ...signalLines, pathLine, riskLine].join("\n");
}

function indicatorTimelineBounds(interval: "1d" | "1h"): { min: number; max: number } {
  return interval === "1h" ? { min: 1, max: 10 } : { min: 3, max: 30 };
}

function hasStructuredIndicatorNarrative(text: string): boolean {
  const normalized = sanitizeText(text, 8000);
  if (!normalized) return false;
  return /setup:/i.test(normalized) && /risk frame:/i.test(normalized);
}

function narrativeMentionsUnselectedIndicators(text: string, computed: IndicatorComputation): boolean {
  const lines = String(text || "")
    .split(/\n+/)
    .map((line) => sanitizeText(line, 240))
    .filter(Boolean);
  return lines.some((line) => indicatorSignalMentionsUnselectedIndicator(line, computed));
}

function normalizeIndicatorAnalysisPayload(
  parsed: Record<string, unknown>,
  rawText: string,
  computed: IndicatorComputation,
  model: string,
  provider = "openai"
): IndicatorAnalysis {
  const heuristic = buildHeuristicAnalysis(computed);
  const summary = sanitizeText(parsed.summary, 800) || heuristic.summary;
  const llmSignals = Array.isArray(parsed.keySignals)
    ? parsed.keySignals
        .map((item) => sanitizeText(item, 180))
        .filter(Boolean)
        .filter((item) => !indicatorSignalMentionsUnselectedIndicator(item, computed))
    : [];
  const keySignals = normalizeIndicatorKeySignals(llmSignals, heuristic.keySignals);
  const prediction = coercePrediction((parsed.prediction as Record<string, unknown>) || {}, computed.lastClose, computed.interval);
  prediction.targetPrice = clampIndicatorTargetPrice(
    Number(prediction.targetPrice || 0),
    prediction.direction,
    computed,
    Number(heuristic.prediction.targetPrice || computed.lastClose)
  );
  const textCandidate = sanitizeText(parsed.text || rawText, 8000);
  const text = hasStructuredIndicatorNarrative(textCandidate) && !narrativeMentionsUnselectedIndicators(textCandidate, computed)
    ? textCandidate
    : buildIndicatorNarrative(summary, keySignals, prediction, computed);
  return {
    summary,
    keySignals,
    prediction,
    text,
    provider,
    model,
    disclaimer: LLM_DISCLAIMER,
  };
}

async function postResponses(
  apiKey: string,
  timeoutMs: number,
  body: Record<string, unknown>
): Promise<Record<string, unknown>> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(5000, Math.min(timeoutMs, 120000)));
  try {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify(body),
    });
    const payload = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) {
      const detail = sanitizeText((payload as any)?.error?.message || payload?.error || "", 220);
      throw new Error(detail || `OpenAI Responses failed (${response.status}).`);
    }
    return payload;
  } finally {
    clearTimeout(timer);
  }
}

function buildIndicatorAnalysisRequest(computed: IndicatorComputation): {
  heuristic: IndicatorAnalysis;
  systemPrompt: string;
  userPayload: Record<string, unknown>;
  responseSchema: Record<string, unknown>;
} {
  const promptContext = buildIndicatorPromptContext(computed);
  const heuristic = buildHeuristicAnalysis(computed);
  const timelineBounds = indicatorTimelineBounds(computed.interval);
  const timelineMin = timelineBounds.min;
  const timelineMax = timelineBounds.max;
  const lastClose = Number(computed.lastClose);
  const atrValue = Number(computed.latestNumeric.ATR_14 || 0);
  const atrPct = Number.isFinite(atrValue) && Number.isFinite(lastClose) && lastClose > 0 ? atrValue / lastClose : 0;
  const targetBandPct = Math.max(
    computed.interval === "1h" ? 0.05 : 0.1,
    Math.min(computed.interval === "1h" ? 0.18 : 0.3, atrPct > 0 ? atrPct * 4 : computed.interval === "1h" ? 0.05 : 0.1)
  );
  const targetBand = {
    lower: Number((lastClose * (1 - targetBandPct)).toFixed(2)),
    upper: Number((lastClose * (1 + targetBandPct)).toFixed(2)),
  };

  const responseSchema = {
    type: "object",
    additionalProperties: false,
    properties: {
      summary: {
        type: "string",
        minLength: 1,
        maxLength: 800,
      },
      keySignals: {
        type: "array",
        minItems: 3,
        maxItems: 6,
        items: {
          type: "string",
          minLength: 1,
          maxLength: 180,
        },
      },
      prediction: {
        type: "object",
        additionalProperties: false,
        properties: {
          direction: {
            type: "string",
            enum: ["bullish", "bearish", "neutral"],
          },
          targetPrice: {
            type: "number",
          },
          timeline: {
            type: "string",
            minLength: 1,
            maxLength: 80,
          },
          timelineDays: {
            type: "integer",
            minimum: timelineMin,
            maximum: timelineMax,
          },
          confidence: {
            type: "string",
            enum: ["low", "medium", "high"],
          },
        },
        required: ["direction", "targetPrice", "timeline", "timelineDays", "confidence"],
      },
      text: {
        type: "string",
        minLength: 1,
        maxLength: 8000,
      },
    },
    required: ["summary", "keySignals", "prediction", "text"],
  } as const;

  const systemPrompt = [
    "You are Quantura Horizon's technical-indicator analyst.",
    "Use only the provided market context.",
    "Only cite indicators that were explicitly selected for this request, plus directly derived price context such as last close or realized range.",
    "Do not invent catalysts, fundamentals, news, support zones, or macro drivers that were not supplied.",
    "This is tactical decision support, not investment advice or a guarantee.",
    "Return valid JSON only with exact keys:",
    '{"summary":"string","keySignals":["string"],"prediction":{"direction":"bullish|bearish|neutral","targetPrice":0,"timeline":"string","timelineDays":0,"confidence":"low|medium|high"},"text":"string"}',
    "Rules:",
    "- summary: 1 or 2 concise sentences grounded in the indicator stack.",
    "- keySignals: 3 to 6 items, each must cite a selected indicator and what it implies.",
    "- prediction.direction: use bullish or bearish only when multiple signals align; otherwise neutral.",
    `- prediction.timelineDays: integer between ${timelineMin} and ${timelineMax}.`,
    `- prediction.targetPrice: numeric and realistic, staying near the current regime. Keep it inside ${targetBand.lower} to ${targetBand.upper} unless the context explicitly justifies a tighter bound.`,
    "- prediction.confidence: high only if momentum and trend strength confirm each other; low if signals conflict.",
    "- text: concise markdown-safe narrative with sections named Setup, Signal stack, Path, and Risk frame.",
    "- Never mention ATR or ADX unless ATR or ADX was selected for this run.",
    "- Avoid guaranteed language, buy/sell directives, and exaggerated certainty.",
  ].join(" ");

  const userPayload = {
    task: "Analyze the selected technical indicators and provide a tactical scenario update.",
    ticker: computed.ticker,
    interval: computed.interval,
    lookback: computed.lookback,
    selectedIndicators: computed.selectedIndicators,
    marketContext: promptContext,
    guardrails: {
      timelineDaysRange: [timelineMin, timelineMax],
      targetPriceRange: targetBand,
      fallbackPrediction: heuristic.prediction,
    },
  };

  return {
    heuristic,
    systemPrompt,
    userPayload,
    responseSchema,
  };
}

async function runSharedIndicatorAnalysis(
  computed: IndicatorComputation,
  input: IndicatorAnalyzeInput,
  opts: IndicatorAnalyzeOptions
): Promise<IndicatorAnalysis> {
  if (typeof opts.invokeLlm !== "function") {
    return runOpenAiIndicatorAnalysis(computed, opts);
  }

  const provider = normalizeLlmProvider(input.provider);
  const model = normalizeLlmModel(input.model, opts.defaultModel);
  const fallbackProviders = normalizeFallbackProviders(input.fallbackProviders).filter((item) => item !== provider);
  const userTier = normalizeLlmTier(input.userTier);
  const { heuristic, systemPrompt, userPayload, responseSchema } = buildIndicatorAnalysisRequest(computed);

  try {
    const result = await opts.invokeLlm({
      provider,
      model,
      fallbackProviders,
      userTier,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: JSON.stringify(userPayload) },
      ],
      params: {
        temperature: 0.2,
        maxTokens: 900,
        webSearch: false,
        background: false,
      },
      jsonSchema: {
        name: "indicator_analysis",
        schema: responseSchema,
      },
    });
    const text = sanitizeText(result?.text, 24000);
    const parsed = parseJsonObject(text);
    const actualProvider = normalizeLlmProvider(result?.provider || provider);
    const actualModel = normalizeLlmModel(result?.model, model);
    if (!parsed) {
      return {
        ...heuristic,
        text: hasStructuredIndicatorNarrative(text) ? text : heuristic.text,
        provider: actualProvider,
        model: actualModel,
      };
    }
    return normalizeIndicatorAnalysisPayload(parsed, text, computed, actualModel, actualProvider);
  } catch (_error) {
    if (provider === "openai") {
      return runOpenAiIndicatorAnalysis(computed, {
        ...opts,
        defaultModel: model,
      });
    }
    return {
      ...heuristic,
      provider,
      model,
    };
  }
}

function coercePrediction(
  payload: Record<string, unknown>,
  lastClose: number,
  interval: "1d" | "1h"
): IndicatorPrediction {
  const timelineBounds = indicatorTimelineBounds(interval);
  const raw = asString(payload.direction).trim().toLowerCase();
  const direction: IndicatorPrediction["direction"] = raw === "bullish" || raw === "bearish" || raw === "neutral" ? (raw as any) : "neutral";
  const timelineDays = Math.max(
    timelineBounds.min,
    Math.min(timelineBounds.max, Math.floor(asFinite(payload.timelineDays, interval === "1h" ? 5 : 10)))
  );
  const timeline = sanitizeText(payload.timeline, 80) || `${timelineDays} trading days`;
  const confidenceRaw = asString(payload.confidence).trim().toLowerCase();
  const confidence: IndicatorPrediction["confidence"] =
    confidenceRaw === "low" || confidenceRaw === "medium" || confidenceRaw === "high" ? (confidenceRaw as any) : "medium";
  const targetRaw = asFinite(payload.targetPrice, NaN);
  const targetPrice = Number.isFinite(targetRaw) && targetRaw > 0 ? Number(targetRaw.toFixed(2)) : Number(lastClose.toFixed(2));
  return {
    direction,
    targetPrice,
    timeline,
    timelineDays,
    confidence,
  };
}

async function runOpenAiIndicatorAnalysis(
  computed: IndicatorComputation,
  opts: IndicatorAnalyzeOptions
): Promise<IndicatorAnalysis> {
  if (!opts.openAiApiKey) return buildHeuristicAnalysis(computed);

  const model = sanitizeText(opts.defaultModel, 120) || "gpt-5-mini";
  const { heuristic, systemPrompt, userPayload, responseSchema } = buildIndicatorAnalysisRequest(computed);
  const toolDef = {
    type: "function",
    name: "finta_calculate_indicators",
    description:
      "Calculate selected technical indicators from OHLCV time-series using finta-style formulas and return latest numeric values.",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        ticker: { type: "string" },
        interval: { type: "string", enum: ["1d", "1h"] },
        lookback: { type: "integer", minimum: 30, maximum: 5000 },
        indicators: { type: "array", items: { type: "string" } },
      },
      required: ["ticker", "interval", "indicators"],
    },
  } as const;
  const responseFormat = {
    format: {
      type: "json_schema",
      name: "indicator_analysis",
      strict: true,
      schema: responseSchema,
    },
  } as const;

  let responsePayload = await postResponses(opts.openAiApiKey, opts.timeoutMs, {
    model,
    input: [
      {
        role: "system",
        content: [{ type: "input_text", text: systemPrompt }],
      },
      {
        role: "user",
        content: [{ type: "input_text", text: JSON.stringify(userPayload) }],
      },
    ],
    tools: [toolDef],
    tool_choice: "required",
    text: responseFormat,
    max_output_tokens: 900,
    background: false,
    stream: false,
    metadata: {
      quantura_workflow: "indicator_analysis",
      quantura_prompt_caching: "enabled",
    },
  });

  let loopCount = 0;
  while (loopCount < 3) {
    const calls = extractFunctionCalls(responsePayload).filter((item) => item.name === "finta_calculate_indicators");
    if (!calls.length) break;

    const outputs = calls.map((item) => {
      const requestedIndicators = normalizeIndicators(item.args.indicators);
      const allowed = requestedIndicators.length
        ? computed.latestRows.filter((entry) => requestedIndicators.some((code) => entry.name.startsWith(code) || entry.name.includes(code)))
        : computed.latestRows;
      const result = {
        ticker: computed.ticker,
        interval: computed.interval,
        asOf: computed.asOf,
        lastClose: computed.lastClose,
        previousClose: computed.previousClose,
        selectedIndicators: computed.selectedIndicators,
        latest: (allowed.length ? allowed : computed.latestRows).map((entry) => ({
          name: entry.name,
          value: entry.value,
          display: entry.display,
        })),
      };
      return {
        type: "function_call_output",
        call_id: item.callId,
        output: JSON.stringify(result),
      };
    });

    const previousResponseId = sanitizeText((responsePayload as any).id, 200);
    if (!previousResponseId) break;

    responsePayload = await postResponses(opts.openAiApiKey, opts.timeoutMs, {
      model,
      previous_response_id: previousResponseId,
      input: outputs,
      text: responseFormat,
      max_output_tokens: 900,
      background: false,
      stream: false,
      metadata: {
        quantura_workflow: "indicator_analysis",
        quantura_prompt_caching: "enabled",
      },
    });
    loopCount += 1;
  }

  const outputText = extractResponsesOutputText(responsePayload);
  const parsed = parseJsonObject(outputText);
  if (!parsed) {
    const fallback = buildHeuristicAnalysis(computed);
    return {
      ...fallback,
      text: hasStructuredIndicatorNarrative(outputText) ? outputText : fallback.text,
      provider: "openai",
      model,
    };
  }
  return normalizeIndicatorAnalysisPayload(parsed, outputText, computed, model);
}

export async function runIndicatorAnalysis(
  input: IndicatorAnalyzeInput,
  opts: IndicatorAnalyzeOptions
): Promise<IndicatorAnalyzeResponse> {
  const ticker = normalizeTicker(input.ticker);
  if (!ticker) throw new Error("Ticker is required.");
  const interval = normalizeInterval(input.interval);
  const lookback = normalizeLookback(input.lookback);
  const maxPoints = normalizeMaxPoints(input.maxPoints);
  const selectedIndicators = normalizeIndicators(input.indicators);

  const candles = await fetchYahooCandles({
    ticker,
    interval,
    lookback,
    maxPoints,
  });
  const computed = computeIndicators({
    ticker,
    interval,
    lookback,
    selectedIndicators,
    candles,
    maxPoints,
  });

  if (!Number.isFinite(computed.lastClose) || computed.latestRows.length === 0) {
    throw new Error("Unable to compute indicators for this ticker and interval.");
  }

  const analysis = await runSharedIndicatorAnalysis(computed, input, opts);

  return {
    ticker,
    interval,
    lookback,
    selectedIndicators,
    latest: computed.latestRows,
    series: {
      dates: computed.dates,
      items: computed.seriesItems,
    },
    analysis,
    meta: {
      asOf: computed.asOf,
      historyPoints: computed.candles.length,
      lastClose: computed.lastClose,
      previousClose: computed.previousClose,
      source: "yahoo_finance",
    },
  };
}
