import assert from "node:assert/strict";
import test from "node:test";
import {
  buildForecastAgentRequest,
  buildForecastAnalysisContext,
  forecastPercentChange,
  parseForecastAgentSections,
  renderForecastAgentMarkdown,
} from "./forecastAnalysis";
import { buildForecastFromHistory, META_PROPHET_FORECAST_QUANTILES } from "./forecastingScreener";

type FiveQuantiles = [number, number, number, number, number];

function rows(values: FiveQuantiles[]) {
  return values.map(([p1, p25, p50, p75, p99], index) => ({
    ds: `2026-09-${String(index + 1).padStart(2, "0")}T00:00:00Z`,
    q1: p1,
    q25: p25,
    q50: p50,
    q75: p75,
    q99: p99,
  }));
}

function context(currentPrice: number, values: FiveQuantiles[], direction = "neutral") {
  return buildForecastAnalysisContext({
    ticker: "PLTR",
    currentPrice,
    marketDataTimestamp: "2026-08-24T20:00:00Z",
    interval: "1d",
    forecastRows: rows(values),
    indicators: {
      selectedIndicators: ["RSI", "MACD", "SMA"],
      latest: [
        { name: "RSI_14", value: 55, display: "55.00" },
        { name: "MACD_HIST", value: 1.2, display: "1.2000" },
      ],
      context: { derivedSignals: { rsi14: 55, macdHist: 1.2, sma20: 101 } },
      analysis: { prediction: { direction, confidence: "medium" }, keySignals: ["RSI is constructive."] },
    },
  });
}

test("percentage changes preserve positive and negative signs", () => {
  assert.equal(forecastPercentChange(90, 100), -10);
  assert.equal(forecastPercentChange(110, 100), 10);
  assert.throws(() => forecastPercentChange(110, 0), /positive current price/i);
});

test("canonical five-level scenario mapping preserves mathematical direction", () => {
  const result = context(100, [[70, 90, 105, 115, 140], [75, 95, 110, 125, 150]], "bullish");
  assert.equal(result.scenarios.extremeBear.quantile, "P1");
  assert.equal(result.scenarios.extremeBear.percentChange, -25);
  assert.equal(result.scenarios.bear.quantile, "P25");
  assert.equal(result.scenarios.bear.percentChange, -5);
  assert.equal(result.scenarios.base.percentChange, 10);
  assert.equal(result.scenarios.bull.percentChange, 25);
  assert.equal(result.scenarios.extremeBull.percentChange, 50);
  assert.equal(result.indicatorAgreement.status, "confirm");
});

test("bearish distribution does not invent upside for P99", () => {
  const result = context(120, [[65, 80, 95, 105, 115], [60, 75, 90, 100, 110]], "bearish");
  assert.equal(result.scenarios.extremeBear.percentChange, -50);
  assert.equal(result.scenarios.base.percentChange, -25);
  assert.equal(result.scenarios.extremeBull.percentChange, -8.333333);
  assert.equal(result.indicatorAgreement.status, "confirm");
});

test("entire forecast above current price leaves P1 mathematically positive", () => {
  const result = context(80, [[82, 90, 100, 110, 125], [85, 95, 105, 120, 140]], "bullish");
  assert.equal(result.scenarios.extremeBear.percentChange, 6.25);
  assert.equal(result.scenarios.extremeBull.percentChange, 75);
  assert.equal(result.currentPosition, "below_p1");
});

test("current-price position covers mixed and extreme locations", () => {
  assert.equal(context(100, [[70, 90, 100, 110, 130]]).currentPosition, "between_p50_p75");
  assert.equal(context(140, [[70, 90, 100, 110, 130]]).currentPosition, "above_p99");
  assert.equal(context(80, [[70, 90, 100, 110, 130]]).currentPosition, "between_p1_p25");
  assert.equal(context(105, [[70, 90, 100, 110, 130]]).currentPosition, "between_p50_p75");
});

test("central and extreme ranges are calculated and classified independently", () => {
  const result = context(100, [
    [70, 90, 100, 110, 130],
    [50, 85, 100, 115, 150],
  ]);
  assert.equal(result.ranges.central.finalRange, 30);
  assert.equal(result.ranges.central.finalRangePercentOfCurrent, 30);
  assert.equal(result.ranges.central.direction, "expanding");
  assert.equal(result.ranges.extreme.finalRange, 100);
  assert.equal(result.ranges.extreme.finalRangePercentOfCurrent, 100);
  assert.equal(result.ranges.extreme.direction, "expanding");

  const narrow = context(100, [
    [80, 90, 100, 110, 120],
    [81, 90.5, 100, 109.5, 119],
  ]);
  assert.equal(narrow.ranges.central.direction, "stable");
  assert.equal(narrow.ranges.extreme.direction, "stable");
});

test("quantile summaries include standard deviation and current-price changes", () => {
  const result = context(100, [[70, 90, 100, 110, 130], [75, 95, 110, 120, 140]]);
  assert.equal(result.forecast.quantiles.P50.average, 105);
  assert.equal(result.forecast.quantiles.P50.minimum, 100);
  assert.equal(result.forecast.quantiles.P50.maximum, 110);
  assert.equal(result.forecast.quantiles.P50.standardDeviation, 5);
  assert.equal(result.forecast.quantiles.P50.dollarChange, 10);
  assert.equal(result.forecast.quantiles.P50.percentChange, 10);
  assert.equal(result.forecast.trend.direction, "bullish");
});

test("missing or inverted canonical quantiles fail with actionable validation", () => {
  assert.throws(
    () => buildForecastAnalysisContext({ ticker: "AAPL", currentPrice: 100, forecastRows: [{ ds: "2026-09-01", q25: 95, q50: 101, q75: 105, q99: 110 }] }),
    /P1, P25, P50, P75, and P99/i
  );
  assert.throws(
    () => buildForecastAnalysisContext({ ticker: "AAPL", currentPrice: 100, forecastRows: [{ ds: "2026-09-01", q1: 80, q25: 105, q50: 100, q75: 110, q99: 120 }] }),
    /out of order/i
  );
});

test("agent request contains all five series, scenarios, ranges, horizon, price, trend, and indicators", () => {
  const result = context(100, [[70, 90, 100, 110, 130], [72, 92, 104, 118, 145]], "bullish");
  const request = buildForecastAgentRequest(result);
  const payload = request.userPayload as any;
  assert.equal(payload.currentPrice, 100);
  assert.equal(payload.forecast.horizon, 2);
  for (const name of ["P1", "P25", "P50", "P75", "P99"]) {
    assert.equal(payload.forecast.quantiles[name].series.length, 2);
    assert.equal(typeof payload.forecast.quantiles[name].percentChange, "number");
  }
  assert.equal(payload.scenarios.extremeBear.quantile, "P1");
  assert.equal(payload.scenarios.extremeBull.quantile, "P99");
  assert.equal(payload.ranges.central.lowerQuantile, "P25");
  assert.equal(payload.ranges.extreme.upperQuantile, "P99");
  assert.equal(payload.indicators.derived.rsi14, 55);
  assert.equal(payload.forecast.trend.quantile, "P50");
  for (const requirement of [
    "Forecast Distribution",
    "Extreme Bear Case",
    "Bear Case",
    "Base Case",
    "Bull Case",
    "Extreme Bull Case",
    "Technical Confirmation / Conflict",
    "Forecast Uncertainty",
    "Overall Bias",
  ]) {
    assert.match(request.systemPrompt, new RegExp(requirement.replace("/", "\\/"), "i"));
  }
});

test("structured agent output requires every five-scenario section", () => {
  const complete = {
    forecastDistribution: "Current price is within the central distribution.",
    extremeBearCase: "P1 tail scenario.",
    bearCase: "P25 lower scenario.",
    baseCase: "P50 median scenario.",
    bullCase: "P75 upper scenario.",
    extremeBullCase: "P99 tail scenario.",
    technicalConfirmation: "Indicators are mixed.",
    forecastUncertainty: "Central and extreme ranges are stable.",
    overallBias: "Neutral / Mixed",
    overallAssessment: "Upside and downside remain plausible.",
  };
  const parsed = parseForecastAgentSections(JSON.stringify(complete));
  const markdown = renderForecastAgentMarkdown(parsed);
  assert.match(markdown, /### Extreme Bear Case/);
  assert.match(markdown, /### Extreme Bull Case/);
  assert.match(markdown, /### Forecast Uncertainty/);
  assert.throws(() => parseForecastAgentSections(JSON.stringify({ ...complete, bearCase: "" })), /invalid structured/i);
});

test("forecast pipeline emits and serializes only canonical ordered quantiles", () => {
  const historyRows = Array.from({ length: 90 }, (_, index) => {
    const date = new Date(Date.UTC(2026, 0, 1 + index));
    const close = 100 * Math.exp(0.001 * index + Math.sin(index / 5) * 0.01);
    return { Date: date.toISOString().slice(0, 10), Close: close };
  });
  const forecast = buildForecastFromHistory({
    ticker: "AAPL",
    interval: "1d",
    horizon: 12,
    quantiles: META_PROPHET_FORECAST_QUANTILES,
    historyRows,
  });
  assert.deepEqual(forecast.quantiles, [0.01, 0.25, 0.5, 0.75, 0.99]);
  assert.equal(forecast.forecastRows.length, 12);
  for (const row of forecast.forecastRows) {
    const values = [row.q1, row.q25, row.q50, row.q75, row.q99].map(Number);
    assert.ok(values.every(Number.isFinite));
    assert.ok(values.every((value, index) => index === 0 || values[index - 1] <= value));
    assert.deepEqual(Object.keys(row).filter((key) => /^q\d+$/.test(key)), ["q1", "q25", "q50", "q75", "q99"]);
  }
  const serialized = JSON.parse(JSON.stringify(forecast));
  assert.equal(serialized.forecastRows[0].q1, forecast.forecastRows[0].q1);
  assert.equal(typeof serialized.metrics.coverage25_75, "number");
  assert.equal(typeof serialized.metrics.coverage1_99, "number");
});
