import assert from "node:assert/strict";
import test from "node:test";
import {
  buildForecastAgentRequest,
  buildForecastAnalysisContext,
  forecastPercentChange,
  parseForecastAgentSections,
  renderForecastAgentMarkdown,
} from "./forecastAnalysis";

function rows(values: Array<[number, number, number]>) {
  return values.map(([p10, p50, p90], index) => ({
    ds: `2026-09-${String(index + 1).padStart(2, "0")}T00:00:00Z`,
    q10: p10,
    q50: p50,
    q90: p90,
    q25: (p10 + p50) / 2,
    q75: (p50 + p90) / 2,
  }));
}

function context(currentPrice: number, values: Array<[number, number, number]>, direction = "neutral") {
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

test("bullish distribution produces signed bear, base, and bull scenarios", () => {
  const result = context(100, [[95, 105, 115], [98, 110, 125]], "bullish");
  assert.equal(result.scenarios.bear.percentChange, -2);
  assert.equal(result.scenarios.base.percentChange, 10);
  assert.equal(result.scenarios.bull.percentChange, 25);
  assert.equal(result.indicatorAgreement.status, "confirm");
});

test("bearish distribution still preserves the upper-tail scenario without inventing upside", () => {
  const result = context(120, [[90, 100, 110], [82, 92, 105]], "bearish");
  assert.equal(result.scenarios.bear.percentChange, -31.666667);
  assert.equal(result.scenarios.base.percentChange, -23.333333);
  assert.equal(result.scenarios.bull.percentChange, -12.5);
  assert.equal(result.indicatorAgreement.status, "confirm");
});

test("entire forecast above current price leaves P10 mathematically positive", () => {
  const result = context(80, [[85, 95, 110], [90, 100, 120]], "bullish");
  assert.equal(result.scenarios.bear.percentChange, 12.5);
  assert.equal(result.scenarios.base.percentChange, 25);
  assert.equal(result.scenarios.bull.percentChange, 50);
});

test("mixed distribution and neutral indicators produce mixed agreement", () => {
  const result = context(100, [[85, 99.8, 115], [80, 100.2, 120]], "neutral");
  assert.equal(result.currentPosition, "between_p10_p50");
  assert.equal(result.indicatorAgreement.status, "mixed");
});

test("opposed forecast and indicator directions are a conflict", () => {
  const result = context(100, [[90, 104, 112], [92, 108, 118]], "bearish");
  assert.equal(result.indicatorAgreement.status, "conflict");
});

test("wide and narrow ranges classify uncertainty movement", () => {
  const expanding = context(100, [[95, 100, 105], [70, 100, 130]]);
  const contracting = context(100, [[70, 100, 130], [95, 100, 105]]);
  const stable = context(100, [[90, 100, 110], [89.5, 100, 110.5]]);
  assert.equal(expanding.uncertainty.direction, "expanding");
  assert.equal(contracting.uncertainty.direction, "contracting");
  assert.equal(stable.uncertainty.direction, "stable");
  assert.equal(expanding.uncertainty.finalRangePercentOfCurrent, 60);
});

test("additional quantiles remain available to the agent architecture", () => {
  const result = context(100, [[90, 100, 110], [91, 101, 111]]);
  assert.ok(result.forecast.additionalQuantiles.P25);
  assert.ok(result.forecast.additionalQuantiles.P75);
});

test("missing or inverted major quantiles fail with actionable validation", () => {
  assert.throws(
    () => buildForecastAnalysisContext({ ticker: "AAPL", currentPrice: 100, forecastRows: [{ ds: "2026-09-01", q50: 101, q90: 110 }] }),
    /P10, P50, and P90/i
  );
  assert.throws(
    () => buildForecastAnalysisContext({ ticker: "AAPL", currentPrice: 100, forecastRows: [{ ds: "2026-09-01", q10: 105, q50: 100, q90: 110 }] }),
    /out of order/i
  );
});

test("agent request contains full quantile series, scenarios, horizon, current price, and indicators", () => {
  const result = context(100, [[90, 100, 110], [92, 104, 118]], "bullish");
  const request = buildForecastAgentRequest(result);
  const payload = request.userPayload as any;
  assert.equal(payload.currentPrice, 100);
  assert.equal(payload.forecast.horizon, 2);
  assert.equal(payload.forecast.quantiles.P10.series.length, 2);
  assert.equal(payload.forecast.quantiles.P50.series.length, 2);
  assert.equal(payload.forecast.quantiles.P90.series.length, 2);
  assert.equal(payload.scenarios.bear.quantile, "P10");
  assert.equal(payload.scenarios.base.quantile, "P50");
  assert.equal(payload.scenarios.bull.quantile, "P90");
  assert.equal(payload.indicators.derived.rsi14, 55);
  for (const requirement of ["Bull Case", "Bear Case", "Base Case", "Technical Confirmation", "Risk / Uncertainty", "Overall Bias"]) {
    assert.match(request.systemPrompt, new RegExp(requirement.replace("/", "\\/"), "i"));
  }
});

test("structured agent output requires every balanced section", () => {
  const complete = {
    forecastSummary: "Distribution summary.",
    bullCase: "P90 upside evidence.",
    bearCase: "P10 downside evidence.",
    baseCase: "P50 median evidence.",
    technicalConfirmation: "Indicators are mixed.",
    riskUncertainty: "Range is wide.",
    overallBias: "Neutral / Mixed",
    overallAssessment: "Both cases remain plausible.",
  };
  const parsed = parseForecastAgentSections(JSON.stringify(complete));
  const markdown = renderForecastAgentMarkdown(parsed);
  assert.match(markdown, /### Bull Case/);
  assert.match(markdown, /### Bear Case/);
  assert.match(markdown, /### Base Case/);
  assert.throws(() => parseForecastAgentSections(JSON.stringify({ ...complete, bearCase: "" })), /invalid structured/i);
});
