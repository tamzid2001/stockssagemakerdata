import crypto from "crypto";

export type ForecastQuantileName = "P1" | "P25" | "P50" | "P75" | "P99";
export type ForecastBias = "bullish" | "bearish" | "neutral";
export type RangeDirection = "expanding" | "contracting" | "stable";

export type ForecastSeriesPoint = {
  date: string;
  value: number;
};

export type ForecastQuantileSummary = {
  series: ForecastSeriesPoint[];
  average: number;
  minimum: number;
  maximum: number;
  standardDeviation: number;
  lowerBoundary: number;
  upperBoundary: number;
  final: number;
  dollarChange: number;
  percentChange: number;
  unusualCount: number;
  unusual: ForecastSeriesPoint[];
};

export type ForecastScenario = {
  label: "Extreme Bear" | "Bear" | "Base" | "Bull" | "Extreme Bull";
  quantile: ForecastQuantileName;
  target: number;
  dollarChange: number;
  percentChange: number;
  forecastEndDate: string;
};

export type ForecastRangeSummary = {
  label: string;
  lowerQuantile: ForecastQuantileName;
  upperQuantile: ForecastQuantileName;
  series: Array<{ date: string; range: number; rangePercentOfCurrent: number }>;
  startRange: number;
  finalRange: number;
  averageRange: number;
  finalRangePercentOfCurrent: number;
  changePercent: number;
  direction: RangeDirection;
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
    trend: {
      quantile: "P50";
      startValue: number;
      finalValue: number;
      dollarChange: number;
      percentChange: number;
      averageChangePerStep: number;
      direction: ForecastBias;
    };
  };
  scenarios: {
    extremeBear: ForecastScenario;
    bear: ForecastScenario;
    base: ForecastScenario;
    bull: ForecastScenario;
    extremeBull: ForecastScenario;
  };
  ranges: {
    central: ForecastRangeSummary;
    extreme: ForecastRangeSummary;
  };
  currentPosition:
    | "below_p1"
    | "between_p1_p25"
    | "between_p25_p50"
    | "between_p50_p75"
    | "between_p75_p99"
    | "above_p99";
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
  forecastDistribution: string;
  marketContext: string;
  extremeBearCase: string;
  bearCase: string;
  baseCase: string;
  bullCase: string;
  extremeBullCase: string;
  bullishStoryScenario: string;
  baseStoryScenario: string;
  bearishStoryScenario: string;
  technicalConfirmation: string;
  forecastUncertainty: string;
  overallBias: "Strongly Bullish" | "Moderately Bullish" | "Neutral / Mixed" | "Moderately Bearish" | "Strongly Bearish";
  overallAssessment: string;
};

type RawObject = Record<string, unknown>;

export const META_PROPHET_QUANTILE_NAMES: ForecastQuantileName[] = ["P1", "P25", "P50", "P75", "P99"];

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

function quantileStats(series: ForecastSeriesPoint[], currentPrice: number): ForecastQuantileSummary {
  if (!series.length) throw new Error("Forecast quantile series is empty.");
  const values = series.map((item) => item.value);
  const average = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance = values.reduce((sum, value) => sum + (value - average) ** 2, 0) / values.length;
  const standardDeviation = Math.sqrt(variance);
  const lower = average - 1.96 * standardDeviation;
  const upper = average + 1.96 * standardDeviation;
  const unusual = series.filter((item) => item.value < lower || item.value > upper);
  const final = values[values.length - 1];
  return {
    series,
    average: rounded(average),
    minimum: rounded(Math.min(...values)),
    maximum: rounded(Math.max(...values)),
    standardDeviation: rounded(standardDeviation),
    lowerBoundary: rounded(lower),
    upperBoundary: rounded(upper),
    final: rounded(final),
    dollarChange: rounded(final - currentPrice),
    percentChange: forecastPercentChange(final, currentPrice),
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

function currentPosition(
  price: number,
  quantiles: Record<ForecastQuantileName, ForecastQuantileSummary>
): ForecastAnalysisContext["currentPosition"] {
  if (price < quantiles.P1.final) return "below_p1";
  if (price < quantiles.P25.final) return "between_p1_p25";
  if (price < quantiles.P50.final) return "between_p25_p50";
  if (price < quantiles.P75.final) return "between_p50_p75";
  if (price <= quantiles.P99.final) return "between_p75_p99";
  return "above_p99";
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

function buildRangeSummary(
  label: string,
  lowerQuantile: ForecastQuantileName,
  upperQuantile: ForecastQuantileName,
  normalizedRows: Array<{ date: string; quantiles: Record<string, number> }>,
  currentPrice: number
): ForecastRangeSummary {
  const series = normalizedRows.map((row) => {
    const range = row.quantiles[upperQuantile] - row.quantiles[lowerQuantile];
    return { date: row.date, range: rounded(range), rangePercentOfCurrent: rounded((range / currentPrice) * 100) };
  });
  const startRange = series[0].range;
  const finalRange = series[series.length - 1].range;
  const changePercent = startRange > 0 ? rounded(((finalRange - startRange) / startRange) * 100) : 0;
  const direction: RangeDirection = changePercent > 5 ? "expanding" : changePercent < -5 ? "contracting" : "stable";
  return {
    label,
    lowerQuantile,
    upperQuantile,
    series,
    startRange,
    finalRange,
    averageRange: rounded(series.reduce((sum, item) => sum + item.range, 0) / series.length),
    finalRangePercentOfCurrent: rounded((finalRange / currentPrice) * 100),
    changePercent,
    direction,
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
      META_PROPHET_QUANTILE_NAMES.forEach((name) => {
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
    const values = META_PROPHET_QUANTILE_NAMES.map((name) => finite(row.quantiles[name]));
    if (values.some((value) => value === null)) {
      throw new Error("P1, P25, P50, P75, and P99 are required for every Meta Prophet forecast row. Regenerate legacy forecasts.");
    }
    if (values.some((value, index) => index > 0 && Number(values[index - 1]) > Number(value))) {
      throw new Error(`Forecast quantiles are out of order on ${row.date.slice(0, 10)}.`);
    }
  });

  const quantiles = Object.fromEntries(
    META_PROPHET_QUANTILE_NAMES.map((name) => {
      const series = normalizedRows.map((row) => ({ date: row.date, value: row.quantiles[name] }));
      return [name, quantileStats(series, currentPrice)];
    })
  ) as Record<ForecastQuantileName, ForecastQuantileSummary>;
  const startDate = normalizedRows[0].date;
  const endDate = normalizedRows[normalizedRows.length - 1].date;
  const scenario = (label: ForecastScenario["label"], quantile: ForecastQuantileName): ForecastScenario => {
    const target = quantiles[quantile].final;
    return {
      label,
      quantile,
      target,
      dollarChange: rounded(target - currentPrice),
      percentChange: forecastPercentChange(target, currentPrice),
      forecastEndDate: endDate,
    };
  };
  const scenarios = {
    extremeBear: scenario("Extreme Bear", "P1"),
    bear: scenario("Bear", "P25"),
    base: scenario("Base", "P50"),
    bull: scenario("Bull", "P75"),
    extremeBull: scenario("Extreme Bull", "P99"),
  };
  const central = buildRangeSummary("Central forecast range", "P25", "P75", normalizedRows, currentPrice);
  const extreme = buildRangeSummary("Extreme forecast range", "P1", "P99", normalizedRows, currentPrice);
  const p50Start = quantiles.P50.series[0].value;
  const p50Final = quantiles.P50.final;
  const p50DollarChange = rounded(p50Final - p50Start);
  const p50PercentChange = p50Start > 0 ? forecastPercentChange(p50Final, p50Start) : 0;
  const trendDirection: ForecastBias = p50PercentChange > 0.5 ? "bullish" : p50PercentChange < -0.5 ? "bearish" : "neutral";
  const indicators = normalizeIndicatorContext(raw.indicators);
  const indicatorAgreement = buildIndicatorAgreement(scenarios.base.percentChange, indicators);

  return {
    ticker,
    currentPrice: rounded(currentPrice),
    marketDataTimestamp: normalizeDate(raw.marketDataTimestamp) || "",
    forecast: {
      startDate,
      endDate,
      horizon: normalizedRows.length,
      interval,
      quantiles,
      trend: {
        quantile: "P50",
        startValue: rounded(p50Start),
        finalValue: p50Final,
        dollarChange: p50DollarChange,
        percentChange: p50PercentChange,
        averageChangePerStep: rounded(p50DollarChange / Math.max(1, normalizedRows.length - 1)),
        direction: trendDirection,
      },
    },
    scenarios,
    ranges: { central, extreme },
    currentPosition: currentPosition(currentPrice, quantiles),
    indicators,
    indicatorAgreement,
  };
}

export const FORECAST_ANALYSIS_SYSTEM_PROMPT = [
  "You are Quantura's GPT-5.6 Luna quantitative Meta Prophet forecast-analysis assistant.",
  "You receive structured actual market data, canonical forecast-distribution quantiles, deterministic statistics, scenario percentages, uncertainty ranges, anomaly flags, forecast trend, and technical indicators.",
  "Use only the supplied data and analyze it objectively. Never start from a directional conclusion or evaluate only one thesis.",
  "P1 is an extreme lower-tail scenario, P25 is the lower quartile, P50 is the median/base scenario, P75 is the upper quartile, and P99 is an extreme upper-tail scenario.",
  "P99 does not mean the stock will reach P99. P1 does not mean the stock will fall to P1. P50 is a median forecast, not a guaranteed price. Quantiles represent distribution scenarios and extreme quantiles are tail scenarios.",
  "Forecast Distribution must explain where current price sits relative to P1, P25, P50, P75, and P99.",
  "Extreme Bear Case must cite P1 and its mathematically signed percentage change from current price.",
  "Bear Case must cite P25 and its mathematically signed percentage change from current price.",
  "Base Case must cite P50 and its mathematically signed percentage change from current price.",
  "Bull Case must cite P75 and its mathematically signed percentage change from current price.",
  "Extreme Bull Case must cite P99 and its mathematically signed percentage change from current price.",
  "Technical Confirmation / Conflict must explain whether indicators confirm or contradict the distribution, including exhaustion risks.",
  "Forecast Uncertainty must analyze both the P25-P75 central range and P1-P99 extreme range, including whether each expands, contracts, or remains stable.",
  "Compare upside, downside, and contradictory evidence before choosing Overall Bias: Strongly Bullish, Moderately Bullish, Neutral / Mixed, Moderately Bearish, or Strongly Bearish.",
  "When current web context is explicitly enabled, Market Context must separate current, cited search evidence from the numerical model evidence and call out contradictions. When it is disabled, say that no live context was requested.",
  "Bullish, base, and bearish story scenarios are hypothetical narratives only. Label them Hypothetical Headline Scenario, never BREAKING NEWS or an event that has already occurred, and never confuse them with actual searched headlines.",
  "Never imply certainty, guaranteed returns, support/resistance guarantees, or investment advice. Do not add unsupported facts, news, fundamentals, or prices.",
  "Return valid JSON matching the supplied schema.",
].join(" ");

export const FORECAST_ANALYSIS_RESPONSE_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    forecastDistribution: { type: "string", minLength: 1, maxLength: 1400 },
    marketContext: { type: "string", minLength: 1, maxLength: 1400 },
    extremeBearCase: { type: "string", minLength: 1, maxLength: 1400 },
    bearCase: { type: "string", minLength: 1, maxLength: 1400 },
    baseCase: { type: "string", minLength: 1, maxLength: 1400 },
    bullCase: { type: "string", minLength: 1, maxLength: 1400 },
    extremeBullCase: { type: "string", minLength: 1, maxLength: 1400 },
    bullishStoryScenario: { type: "string", minLength: 1, maxLength: 700 },
    baseStoryScenario: { type: "string", minLength: 1, maxLength: 700 },
    bearishStoryScenario: { type: "string", minLength: 1, maxLength: 700 },
    technicalConfirmation: { type: "string", minLength: 1, maxLength: 1400 },
    forecastUncertainty: { type: "string", minLength: 1, maxLength: 1400 },
    overallBias: {
      type: "string",
      enum: ["Strongly Bullish", "Moderately Bullish", "Neutral / Mixed", "Moderately Bearish", "Strongly Bearish"],
    },
    overallAssessment: { type: "string", minLength: 1, maxLength: 1400 },
  },
  required: [
    "forecastDistribution",
    "marketContext",
    "extremeBearCase",
    "bearCase",
    "baseCase",
    "bullCase",
    "extremeBullCase",
    "bullishStoryScenario",
    "baseStoryScenario",
    "bearishStoryScenario",
    "technicalConfirmation",
    "forecastUncertainty",
    "overallBias",
    "overallAssessment",
  ],
} as const;

export function buildForecastAgentRequest(context: ForecastAnalysisContext, options: { enableMarketContext?: boolean } = {}): {
  systemPrompt: string;
  userPayload: Record<string, unknown>;
  responseSchema: typeof FORECAST_ANALYSIS_RESPONSE_SCHEMA;
} {
  return {
    systemPrompt: FORECAST_ANALYSIS_SYSTEM_PROMPT,
    userPayload: {
      task: "Analyze the complete five-quantile Meta Prophet distribution, its two uncertainty ranges, technical confirmation, and all five scenarios before selecting a bias.",
      marketContextPolicy: options.enableMarketContext
        ? "LIVE MODE: use the approved web-search tool for current, relevant market context. Cite sources and distinguish retrieved facts from model-derived hypothetical scenarios."
        : "QUANTITATIVE-ONLY MODE: no live search was requested. Do not invent current news or external context.",
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
  const allowedBiases = new Set([
    "Strongly Bullish",
    "Moderately Bullish",
    "Neutral / Mixed",
    "Moderately Bearish",
    "Strongly Bearish",
  ]);
  const result = {
    forecastDistribution: text(parsed.forecastDistribution, 1400),
    marketContext: text(parsed.marketContext, 1400),
    extremeBearCase: text(parsed.extremeBearCase, 1400),
    bearCase: text(parsed.bearCase, 1400),
    baseCase: text(parsed.baseCase, 1400),
    bullCase: text(parsed.bullCase, 1400),
    extremeBullCase: text(parsed.extremeBullCase, 1400),
    bullishStoryScenario: text(parsed.bullishStoryScenario, 700),
    baseStoryScenario: text(parsed.baseStoryScenario, 700),
    bearishStoryScenario: text(parsed.bearishStoryScenario, 700),
    technicalConfirmation: text(parsed.technicalConfirmation, 1400),
    forecastUncertainty: text(parsed.forecastUncertainty, 1400),
    overallBias: text(parsed.overallBias, 40),
    overallAssessment: text(parsed.overallAssessment, 1400),
  };
  if (
    !result.forecastDistribution ||
    !result.marketContext ||
    !result.extremeBearCase ||
    !result.bearCase ||
    !result.baseCase ||
    !result.bullCase ||
    !result.extremeBullCase ||
    !result.bullishStoryScenario ||
    !result.baseStoryScenario ||
    !result.bearishStoryScenario ||
    !result.technicalConfirmation ||
    !result.forecastUncertainty ||
    !result.overallAssessment ||
    !allowedBiases.has(result.overallBias)
  ) {
    throw new Error("AI provider returned an invalid structured forecast analysis.");
  }
  return result as ForecastAgentSections;
}

export function renderForecastAgentMarkdown(sections: ForecastAgentSections): string {
  return [
    "### Forecast Distribution",
    sections.forecastDistribution,
    "### Current Market Context",
    sections.marketContext,
    "### Extreme Bear Case",
    sections.extremeBearCase,
    "### Bear Case",
    sections.bearCase,
    "### Base Case",
    sections.baseCase,
    "### Bull Case",
    sections.bullCase,
    "### Extreme Bull Case",
    sections.extremeBullCase,
    "### Hypothetical Headline Scenario — Bullish",
    sections.bullishStoryScenario,
    "### Hypothetical Headline Scenario — Base",
    sections.baseStoryScenario,
    "### Hypothetical Headline Scenario — Bearish",
    sections.bearishStoryScenario,
    "### Technical Confirmation / Conflict",
    sections.technicalConfirmation,
    "### Forecast Uncertainty",
    sections.forecastUncertainty,
    `### Overall Bias: ${sections.overallBias}`,
    sections.overallAssessment,
  ].join("\n\n");
}

export function forecastAnalysisHash(context: ForecastAnalysisContext): string {
  return crypto.createHash("sha256").update(JSON.stringify(context)).digest("hex");
}
