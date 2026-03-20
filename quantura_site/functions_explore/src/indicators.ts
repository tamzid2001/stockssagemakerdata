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
};

export type IndicatorAnalyzeOptions = {
  openAiApiKey: string;
  defaultModel: string;
  timeoutMs: number;
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
  const lastClose = roundFinite(computed.lastClose, 4);
  const previousClose = roundFinite(computed.previousClose, 4);
  const atr14 = roundFinite(computed.latestNumeric.ATR_14, 4);
  const ema20 = roundFinite(computed.latestNumeric.EMA_20, 4);
  const sma20 = roundFinite(computed.latestNumeric.SMA_20, 4);
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
      rsi14: roundFinite(computed.latestNumeric.RSI_14, 2),
      macdLine: roundFinite(computed.latestNumeric.MACD_LINE, 4),
      macdSignal: roundFinite(computed.latestNumeric.MACD_SIGNAL, 4),
      macdHist: roundFinite(computed.latestNumeric.MACD_HIST, 4),
      adx14: roundFinite(computed.latestNumeric.ADX_14, 2),
      plusDi14: roundFinite(computed.latestNumeric.PLUS_DI_14, 2),
      minusDi14: roundFinite(computed.latestNumeric.MINUS_DI_14, 2),
      atr14,
      atrPct: lastClose && atr14 ? roundFinite((atr14 / lastClose) * 100, 2) : null,
      ema20,
      sma20,
      closeVsEma20Pct: percentDelta(ema20, lastClose, 2),
      closeVsSma20Pct: percentDelta(sma20, lastClose, 2),
      bbandsUpper: roundFinite(computed.latestNumeric.BBANDS_UPPER, 4),
      bbandsMiddle: roundFinite(computed.latestNumeric.BBANDS_MIDDLE, 4),
      bbandsLower: roundFinite(computed.latestNumeric.BBANDS_LOWER, 4),
      cci20: roundFinite(computed.latestNumeric.CCI_20, 2),
      mfi14: roundFinite(computed.latestNumeric.MFI_14, 2),
      roc12: roundFinite(computed.latestNumeric.ROC_12, 2),
      stochK: roundFinite(computed.latestNumeric.STOCH_K, 2),
      stochD: roundFinite(computed.latestNumeric.STOCH_D, 2),
      willr14: roundFinite(computed.latestNumeric.WILLR_14, 2),
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
  const lastClose = computed.lastClose;
  const atrValue = Number(computed.latestNumeric.ATR_14 || 0);
  const atrPct = lastClose > 0 ? atrValue / lastClose : 0;
  const rsiValue = Number(computed.latestNumeric.RSI_14 || 50);
  const macdHist = Number(computed.latestNumeric.MACD_HIST || 0);
  const adxValue = Number(computed.latestNumeric.ADX_14 || 20);

  let direction: IndicatorPrediction["direction"] = "neutral";
  if (rsiValue < 40 && macdHist >= 0) direction = "bullish";
  else if (rsiValue > 60 && macdHist <= 0) direction = "bearish";
  else if (macdHist > 0) direction = "bullish";
  else if (macdHist < 0) direction = "bearish";

  const timelineDays = computed.interval === "1h" ? 5 : 10;
  const moveMultiplier = Math.max(0.7, Math.min(2.2, adxValue >= 25 ? 1.6 : 1.1));
  const projectedMove = Math.max(0.01, atrPct * moveMultiplier);
  const signedMove = direction === "bullish" ? projectedMove : direction === "bearish" ? -projectedMove : 0;
  const targetPrice = Number.isFinite(lastClose) ? Number((lastClose * (1 + signedMove)).toFixed(2)) : null;

  const confidence: IndicatorPrediction["confidence"] = adxValue >= 30 ? "high" : adxValue >= 20 ? "medium" : "low";
  const keySignals = [
    `RSI(14): ${Number.isFinite(rsiValue) ? rsiValue.toFixed(2) : "—"}`,
    `MACD histogram: ${Number.isFinite(macdHist) ? macdHist.toFixed(4) : "—"}`,
    `ADX(14): ${Number.isFinite(adxValue) ? adxValue.toFixed(2) : "—"}`,
    `ATR(14): ${Number.isFinite(atrValue) ? atrValue.toFixed(4) : "—"}`,
  ];

  const summary = `Computed ${computed.latestRows.length} indicator values for ${computed.ticker}. Momentum is ${direction}, with a ${timelineDays}-day tactical horizon.`;
  const prediction: IndicatorPrediction = {
    direction,
    targetPrice,
    timeline: `${timelineDays} trading days`,
    timelineDays,
    confidence,
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
  const atrValue = Number(computed.latestNumeric.ATR_14 || 0);
  const atrPct = Number.isFinite(atrValue) && atrValue > 0 ? atrValue / lastClose : 0;
  const defaultBandPct = computed.interval === "1h" ? 0.05 : 0.1;
  const hardCapPct = computed.interval === "1h" ? 0.18 : 0.3;
  const bandPct = Math.max(defaultBandPct, Math.min(hardCapPct, atrPct > 0 ? atrPct * 4 : defaultBandPct));
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

function normalizeIndicatorAnalysisPayload(
  parsed: Record<string, unknown>,
  rawText: string,
  computed: IndicatorComputation,
  model: string
): IndicatorAnalysis {
  const heuristic = buildHeuristicAnalysis(computed);
  const summary = sanitizeText(parsed.summary, 800) || heuristic.summary;
  const keySignals = normalizeIndicatorKeySignals(parsed.keySignals, heuristic.keySignals);
  const prediction = coercePrediction((parsed.prediction as Record<string, unknown>) || {}, computed.lastClose, computed.interval);
  prediction.targetPrice = clampIndicatorTargetPrice(
    Number(prediction.targetPrice || 0),
    prediction.direction,
    computed,
    Number(heuristic.prediction.targetPrice || computed.lastClose)
  );
  const textCandidate = sanitizeText(parsed.text || rawText, 8000);
  const text = hasStructuredIndicatorNarrative(textCandidate)
    ? textCandidate
    : buildIndicatorNarrative(summary, keySignals, prediction, computed);
  return {
    summary,
    keySignals,
    prediction,
    text,
    provider: "openai",
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
  const promptContext = buildIndicatorPromptContext(computed);
  const heuristic = buildHeuristicAnalysis(computed);
  const timelineBounds = indicatorTimelineBounds(computed.interval);
  const timelineMin = timelineBounds.min;
  const timelineMax = timelineBounds.max;
  const lastClose = Number(computed.lastClose);
  const atrValue = Number(computed.latestNumeric.ATR_14 || 0);
  const atrPct = Number.isFinite(atrValue) && Number.isFinite(lastClose) && lastClose > 0 ? atrValue / lastClose : 0;
  const targetBandPct = Math.max(computed.interval === "1h" ? 0.05 : 0.1, Math.min(computed.interval === "1h" ? 0.18 : 0.3, atrPct > 0 ? atrPct * 4 : computed.interval === "1h" ? 0.05 : 0.1));
  const targetBand = {
    lower: Number((lastClose * (1 - targetBandPct)).toFixed(2)),
    upper: Number((lastClose * (1 + targetBandPct)).toFixed(2)),
  };
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
      schema: {
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
      },
    },
  } as const;

  const systemPrompt = [
    "You are Quantura Horizon's technical-indicator analyst.",
    "Always call finta_calculate_indicators before your final answer.",
    "Use only the provided market context and the tool output.",
    "Do not invent catalysts, fundamentals, news, support zones, or macro drivers that were not supplied.",
    "This is tactical decision support, not investment advice or a guarantee.",
    "Return valid JSON only with exact keys:",
    '{"summary":"string","keySignals":["string"],"prediction":{"direction":"bullish|bearish|neutral","targetPrice":0,"timeline":"string","timelineDays":0,"confidence":"low|medium|high"},"text":"string"}',
    "Rules:",
    "- summary: 1 or 2 concise sentences grounded in the indicator stack.",
    "- keySignals: 3 to 6 items, each must cite a specific indicator and what it implies.",
    "- prediction.direction: use bullish or bearish only when multiple signals align; otherwise neutral.",
    `- prediction.timelineDays: integer between ${timelineMin} and ${timelineMax}.`,
    `- prediction.targetPrice: numeric and realistic, staying near the current regime. Keep it inside ${targetBand.lower} to ${targetBand.upper} unless the context explicitly justifies a tighter bound.`,
    "- prediction.confidence: high only if momentum and trend strength confirm each other; low if signals conflict.",
    "- text: concise markdown-safe narrative with sections named Setup, Signal stack, Path, and Risk frame.",
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

  const analysis = await runOpenAiIndicatorAnalysis(computed, opts);

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
