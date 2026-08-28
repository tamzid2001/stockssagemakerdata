import crypto from "crypto";

export type ForecastQuantileName = "P10" | "P50" | "P90";
export type ForecastBias = "bullish" | "bearish" | "neutral";

export type ForecastSeriesPoint = {
  date: string;
  value: number;
};

export type ForecastQuantileSummary = {
  series: ForecastSeriesPoint[];
  average: number;
  minimum: number;
  maximum: number;
  final: number;
  unusualCount: number;
  unusual: ForecastSeriesPoint[];
};

export type ForecastScenario = {
  label: "Bear" | "Base" | "Bull";
  quantile: ForecastQuantileName;
  target: number;
  percentChange: number;
  forecastEndDate: string;
};

export type ForecastAnalysisContext = {
  ticker: string;
  currentPrice: number;
  marketDataTimestamp: string;
  forecast: {
    startDate: string;
    endDate: string;
    horizon: number;
    interval: "1d" | "1h";
    quantiles: Record<ForecastQuantileName, ForecastQuantileSummary>;
    additionalQuantiles: Record<string, ForecastQuantileSummary>;
  };
  scenarios: {
    bear: ForecastScenario;
    base: ForecastScenario;
    bull: ForecastScenario;
  };
  uncertainty: {
    series: Array<{ date: string; range: number; rangePercentOfCurrent: number }>;
    startRange: number;
    finalRange: number;
    averageRange: number;
    finalRangePercentOfCurrent: number;
    changePercent: number;
    direction: "expanding" | "contracting" | "stable";
  };
  currentPosition: "below_p10" | "between_p10_p50" | "between_p50_p90" | "above_p90";
  indicators: {
    selected: string[];
    latest: Array<{ name: string; value: number; display: string }>;
    derived: Record<string, number | null>;
    direction: ForecastBias;
    confidence: "low" | "medium" | "high";
    keySignals: string[];
  };
  indicatorAgreement: {
    status: "confirm" | "conflict" | "mixed";
    forecastDirection: ForecastBias;
    indicatorDirection: ForecastBias;
    explanation: string;
  };
};

export type ForecastAgentSections = {
  forecastSummary: string;
  bullCase: string;
  bearCase: string;
  baseCase: string;
  technicalConfirmation: string;
  riskUncertainty: string;
  overallBias: "Bullish" | "Moderately Bullish" | "Neutral / Mixed" | "Moderately Bearish" | "Bearish";
  overallAssessment: string;
};

type RawObject = Record<string, unknown>;

const MAJOR_QUANTILES: ForecastQuantileName[] = ["P10", "P50", "P90"];

function plainObject(value: unknown): RawObject {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as RawObject) : {};
}

function finite(value: unknown): number | null {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function rounded(value: number, digits = 6): number {
  return Number(value.toFixed(digits));
}

function text(value: unknown, maxLength = 400): string {
  return String(value ?? "").replace(/\s+/g, " ").trim().slice(0, maxLength);
}

function normalizeTicker(value: unknown): string {
  return text(value, 24).toUpperCase().replace(/[^A-Z0-9.\-=^]/g, "").slice(0, 20);
}

function normalizeDate(value: unknown): string {
  const raw = text(value, 80);
  if (!raw) return "";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toISOString();
}

function quantileNameFromKey(value: unknown): string {
  const raw = text(value, 20).toUpperCase().replace(/^Q/, "P");
  const match = raw.match(/^P(\d{1,3})$/);
  if (!match) return "";
  const level = Number(match[1]);
  if (!Number.isInteger(level) || level < 0 || level > 100) return "";
  return `P${level}`;
}

function readQuantile(row: RawObject, name: string): number | null {
  const level = name.replace(/^P/, "");
  const padded = level.padStart(2, "0");
  const keys = [name, name.toLowerCase(), `Q${level}`, `q${level}`, `P${padded}`, `p${padded}`, `Q${padded}`, `q${padded}`];
  for (const key of keys) {
    const value = finite(row[key]);
    if (value !== null) return value;
  }
  return null;
}

function quantileStats(series: ForecastSeriesPoint[]): ForecastQuantileSummary {
  if (!series.length) throw new Error("Forecast quantile series is empty.");
  const values = series.map((item) => item.value);
  const average = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance = values.reduce((sum, value) => sum + (value - average) ** 2, 0) / values.length;
  const standardDeviation = Math.sqrt(variance);
  const lower = average - 1.96 * standardDeviation;
  const upper = average + 1.96 * standardDeviation;
  const unusual = series.filter((item) => item.value < lower || item.value > upper);
  return {
    series,
    average: rounded(average),
    minimum: rounded(Math.min(...values)),
    maximum: rounded(Math.max(...values)),
    final: rounded(values[values.length - 1]),
    unusualCount: unusual.length,
    unusual,
  };
}

export function forecastPercentChange(target: number, currentPrice: number): number {
  if (!Number.isFinite(target) || !Number.isFinite(currentPrice) || currentPrice <= 0) {
    throw new Error("A positive current price and valid forecast target are required.");
  }
  return rounded(((target - currentPrice) / currentPrice) * 100);
}

function normalizeIndicatorContext(raw: unknown): ForecastAnalysisContext["indicators"] {
  const indicators = plainObject(raw);
  const analysis = plainObject(indicators.analysis);
  const prediction = plainObject(analysis.prediction);
  const context = plainObject(indicators.context);
  const derivedSource = plainObject(context.derivedSignals || indicators.derived);
  const derived: Record<string, number | null> = {};
  Object.entries(derivedSource).slice(0, 80).forEach(([key, value]) => {
    const numeric = finite(value);
    derived[text(key, 80)] = numeric === null ? null : rounded(numeric);
  });
  const latest = (Array.isArray(indicators.latest) ? indicators.latest : [])
    .map((item) => {
      const row = plainObject(item);
      const value = finite(row.value);
      const name = text(row.name, 80);
      if (!name || value === null) return null;
      return { name, value: rounded(value), display: text(row.display, 100) || String(rounded(value)) };
    })
    .filter((item): item is { name: string; value: number; display: string } => Boolean(item))
    .slice(0, 40);
  const directionRaw = text(prediction.direction || indicators.direction, 30).toLowerCase();
  const direction: ForecastBias = directionRaw === "bullish" || directionRaw === "bearish" ? directionRaw : "neutral";
  const confidenceRaw = text(prediction.confidence || indicators.confidence, 30).toLowerCase();
  const confidence = confidenceRaw === "high" || confidenceRaw === "medium" ? confidenceRaw : "low";
  return {
    selected: (Array.isArray(indicators.selectedIndicators) ? indicators.selectedIndicators : [])
      .map((item) => text(item, 30).toUpperCase())
      .filter(Boolean)
      .slice(0, 30),
    latest,
    derived,
    direction,
    confidence,
    keySignals: (Array.isArray(analysis.keySignals) ? analysis.keySignals : [])
      .map((item) => text(item, 240))
      .filter(Boolean)
      .slice(0, 8),
  };
}

function currentPosition(price: number, p10: number, p50: number, p90: number): ForecastAnalysisContext["currentPosition"] {
  if (price < p10) return "below_p10";
  if (price < p50) return "between_p10_p50";
  if (price <= p90) return "between_p50_p90";
  return "above_p90";
}

function buildIndicatorAgreement(
  basePercentChange: number,
  indicators: ForecastAnalysisContext["indicators"]
): ForecastAnalysisContext["indicatorAgreement"] {
  const forecastDirection: ForecastBias = basePercentChange > 0.5 ? "bullish" : basePercentChange < -0.5 ? "bearish" : "neutral";
  const indicatorDirection = indicators.direction;
  const rsi = finite(indicators.derived.rsi14) ?? finite(indicators.latest.find((item) => /^RSI/i.test(item.name))?.value);
  const contradiction =
    (forecastDirection === "bullish" && rsi !== null && rsi >= 70) ||
    (forecastDirection === "bearish" && rsi !== null && rsi <= 30);
  if (forecastDirection === "neutral" || indicatorDirection === "neutral") {
    return {
      status: "mixed",
      forecastDirection,
      indicatorDirection,
      explanation: "The median forecast or indicator stack is neutral, so confirmation is inconclusive.",
    };
  }
  if (forecastDirection !== indicatorDirection) {
    return {
      status: "conflict",
      forecastDirection,
      indicatorDirection,
      explanation: "The indicator direction opposes the final P50 scenario.",
    };
  }
  if (contradiction) {
    return {
      status: "mixed",
      forecastDirection,
      indicatorDirection,
      explanation: "Trend direction agrees, but an extreme RSI reading introduces exhaustion risk.",
    };
  }
  return {
    status: "confirm",
    forecastDirection,
    indicatorDirection,
    explanation: "The indicator stack and final P50 scenario point in the same direction without an extreme RSI contradiction.",
  };
}

export function buildForecastAnalysisContext(rawInput: unknown): ForecastAnalysisContext {
  const raw = plainObject(rawInput);
  const ticker = normalizeTicker(raw.ticker);
  if (!ticker) throw new Error("Ticker is required.");
  const currentPrice = finite(raw.currentPrice);
  if (currentPrice === null || currentPrice <= 0) throw new Error("Latest actual price is required.");
  const interval: "1d" | "1h" = text(raw.interval, 10).toLowerCase() === "1h" ? "1h" : "1d";
  const inputRows = Array.isArray(raw.forecastRows) ? raw.forecastRows : [];
  if (!inputRows.length) throw new Error("Forecast rows are required.");

  const normalizedRows = inputRows
    .map((item) => {
      const row = plainObject(item);
      const date = normalizeDate(row.date || row.ds || row.datetime || row.timestamp);
      if (!date) return null;
      const quantiles: Record<string, number> = {};
      Object.keys(row).forEach((key) => {
        const name = quantileNameFromKey(key);
        const value = name ? readQuantile(row, name) : null;
        if (name && value !== null) quantiles[name] = rounded(value);
      });
      MAJOR_QUANTILES.forEach((name) => {
        const value = readQuantile(row, name);
        if (value !== null) quantiles[name] = rounded(value);
      });
      return { date, quantiles };
    })
    .filter((item): item is { date: string; quantiles: Record<string, number> } => Boolean(item))
    .sort((left, right) => left.date.localeCompare(right.date))
    .slice(0, 500);
  if (!normalizedRows.length) throw new Error("Forecast rows do not contain valid dates.");

  normalizedRows.forEach((row) => {
    const p10 = finite(row.quantiles.P10);
    const p50 = finite(row.quantiles.P50);
    const p90 = finite(row.quantiles.P90);
    if (p10 === null || p50 === null || p90 === null) throw new Error("P10, P50, and P90 are required for every forecast row.");
    if (p10 > p50 || p50 > p90) throw new Error(`Forecast quantiles are out of order on ${row.date.slice(0, 10)}.`);
  });

  const quantileNames = Array.from(new Set(normalizedRows.flatMap((row) => Object.keys(row.quantiles))));
  const summaries: Record<string, ForecastQuantileSummary> = {};
  quantileNames.forEach((name) => {
    const series = normalizedRows
      .map((row) => ({ date: row.date, value: row.quantiles[name] }))
      .filter((point) => Number.isFinite(point.value));
    if (series.length === normalizedRows.length) summaries[name] = quantileStats(series);
  });
  const p10 = summaries.P10;
  const p50 = summaries.P50;
  const p90 = summaries.P90;
  if (!p10 || !p50 || !p90) throw new Error("Complete P10, P50, and P90 series are required.");

  const startDate = normalizedRows[0].date;
  const endDate = normalizedRows[normalizedRows.length - 1].date;
  const scenario = (label: ForecastScenario["label"], quantile: ForecastQuantileName, target: number): ForecastScenario => ({
    label,
    quantile,
    target: rounded(target),
    percentChange: forecastPercentChange(target, currentPrice),
    forecastEndDate: endDate,
  });
  const scenarios = {
    bear: scenario("Bear", "P10", p10.final),
    base: scenario("Base", "P50", p50.final),
    bull: scenario("Bull", "P90", p90.final),
  };
  const uncertaintySeries = normalizedRows.map((row) => {
    const range = row.quantiles.P90 - row.quantiles.P10;
    return {
      date: row.date,
      range: rounded(range),
      rangePercentOfCurrent: rounded((range / currentPrice) * 100),
    };
  });
  const startRange = uncertaintySeries[0].range;
  const finalRange = uncertaintySeries[uncertaintySeries.length - 1].range;
  const rangeChangePercent = startRange > 0 ? rounded(((finalRange - startRange) / startRange) * 100) : 0;
  const uncertaintyDirection = rangeChangePercent > 5 ? "expanding" : rangeChangePercent < -5 ? "contracting" : "stable";
  const indicators = normalizeIndicatorContext(raw.indicators);
  const indicatorAgreement = buildIndicatorAgreement(scenarios.base.percentChange, indicators);

  const additionalQuantiles: Record<string, ForecastQuantileSummary> = {};
  Object.entries(summaries).forEach(([name, summary]) => {
    if (!MAJOR_QUANTILES.includes(name as ForecastQuantileName)) additionalQuantiles[name] = summary;
  });
  return {
    ticker,
    currentPrice: rounded(currentPrice),
    marketDataTimestamp: normalizeDate(raw.marketDataTimestamp) || "",
    forecast: {
      startDate,
      endDate,
      horizon: normalizedRows.length,
      interval,
      quantiles: { P10: p10, P50: p50, P90: p90 },
      additionalQuantiles,
    },
    scenarios,
    uncertainty: {
      series: uncertaintySeries,
      startRange,
      finalRange,
      averageRange: rounded(uncertaintySeries.reduce((sum, item) => sum + item.range, 0) / uncertaintySeries.length),
      finalRangePercentOfCurrent: rounded((finalRange / currentPrice) * 100),
      changePercent: rangeChangePercent,
      direction: uncertaintyDirection,
    },
    currentPosition: currentPosition(currentPrice, p10.final, p50.final, p90.final),
    indicators,
    indicatorAgreement,
  };
}

export const FORECAST_ANALYSIS_SYSTEM_PROMPT = [
  "You are Quantura's GPT-5.6 Luna quantitative forecast-analysis assistant.",
  "You receive structured actual market data, Meta Prophet forecast quantiles, deterministic statistics, scenario percentages, uncertainty measures, anomaly flags, and technical indicators.",
  "Use only the supplied data and analyze it objectively. Never start from a directional conclusion or evaluate only one thesis.",
  "P10 is the lower forecast quantile, P50 is the median/base forecast, and P90 is the upper forecast quantile; none is guaranteed.",
  "Every response must independently evaluate the Bull Case, Bear Case, and Base Case before selecting an overall bias.",
  "The Bull Case must cite the final P90 target and its mathematically signed percentage change from current price, even if negative.",
  "The Bear Case must cite the final P10 target and its mathematically signed percentage change from current price, even if positive.",
  "The Base Case must cite the final P50 target and its mathematically signed percentage change from current price.",
  "Technical Confirmation must explain confirmation, conflict, and contradictions such as bullish forecasts with overbought RSI or bearish forecasts with oversold RSI.",
  "Risk / Uncertainty must analyze the full P10-P90 width, whether it expands, contracts, or remains stable, and unusual quantile observations.",
  "Compare bullish and bearish evidence explicitly. Do not ignore contradictory evidence or imply certainty, guaranteed returns, or investment advice.",
  "Choose Overall Bias only after both directional cases: Bullish, Moderately Bullish, Neutral / Mixed, Moderately Bearish, or Bearish.",
  "Return valid JSON matching the supplied schema and do not add unsupported facts, news, fundamentals, or prices.",
].join(" ");

export const FORECAST_ANALYSIS_RESPONSE_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    forecastSummary: { type: "string", minLength: 1, maxLength: 1400 },
    bullCase: { type: "string", minLength: 1, maxLength: 1400 },
    bearCase: { type: "string", minLength: 1, maxLength: 1400 },
    baseCase: { type: "string", minLength: 1, maxLength: 1400 },
    technicalConfirmation: { type: "string", minLength: 1, maxLength: 1400 },
    riskUncertainty: { type: "string", minLength: 1, maxLength: 1400 },
    overallBias: {
      type: "string",
      enum: ["Bullish", "Moderately Bullish", "Neutral / Mixed", "Moderately Bearish", "Bearish"],
    },
    overallAssessment: { type: "string", minLength: 1, maxLength: 1400 },
  },
  required: [
    "forecastSummary",
    "bullCase",
    "bearCase",
    "baseCase",
    "technicalConfirmation",
    "riskUncertainty",
    "overallBias",
    "overallAssessment",
  ],
} as const;

export function buildForecastAgentRequest(context: ForecastAnalysisContext): {
  systemPrompt: string;
  userPayload: Record<string, unknown>;
  responseSchema: typeof FORECAST_ANALYSIS_RESPONSE_SCHEMA;
} {
  return {
    systemPrompt: FORECAST_ANALYSIS_SYSTEM_PROMPT,
    userPayload: {
      task: "Analyze the complete Meta Prophet forecast distribution and technical confirmation using balanced bull, bear, and base cases.",
      ...context,
    },
    responseSchema: FORECAST_ANALYSIS_RESPONSE_SCHEMA,
  };
}

export function parseForecastAgentSections(value: unknown): ForecastAgentSections {
  let parsed: RawObject = {};
  if (typeof value === "string") {
    const raw = value.trim();
    try {
      parsed = plainObject(JSON.parse(raw));
    } catch {
      const first = raw.indexOf("{");
      const last = raw.lastIndexOf("}");
      if (first >= 0 && last > first) {
        try {
          parsed = plainObject(JSON.parse(raw.slice(first, last + 1)));
        } catch {
          parsed = {};
        }
      }
    }
  } else {
    parsed = plainObject(value);
  }
  const allowedBiases = new Set(["Bullish", "Moderately Bullish", "Neutral / Mixed", "Moderately Bearish", "Bearish"]);
  const result = {
    forecastSummary: text(parsed.forecastSummary, 1400),
    bullCase: text(parsed.bullCase, 1400),
    bearCase: text(parsed.bearCase, 1400),
    baseCase: text(parsed.baseCase, 1400),
    technicalConfirmation: text(parsed.technicalConfirmation, 1400),
    riskUncertainty: text(parsed.riskUncertainty, 1400),
    overallBias: text(parsed.overallBias, 40),
    overallAssessment: text(parsed.overallAssessment, 1400),
  };
  if (
    !result.forecastSummary ||
    !result.bullCase ||
    !result.bearCase ||
    !result.baseCase ||
    !result.technicalConfirmation ||
    !result.riskUncertainty ||
    !result.overallAssessment ||
    !allowedBiases.has(result.overallBias)
  ) {
    throw new Error("AI provider returned an invalid structured forecast analysis.");
  }
  return result as ForecastAgentSections;
}

export function renderForecastAgentMarkdown(sections: ForecastAgentSections): string {
  return [
    "### Forecast Summary",
    sections.forecastSummary,
    "### Bull Case",
    sections.bullCase,
    "### Bear Case",
    sections.bearCase,
    "### Base Case",
    sections.baseCase,
    "### Technical Confirmation",
    sections.technicalConfirmation,
    "### Risk / Uncertainty",
    sections.riskUncertainty,
    `### Overall Bias: ${sections.overallBias}`,
    sections.overallAssessment,
  ].join("\n\n");
}

export function forecastAnalysisHash(context: ForecastAnalysisContext): string {
  return crypto.createHash("sha256").update(JSON.stringify(context)).digest("hex");
}
