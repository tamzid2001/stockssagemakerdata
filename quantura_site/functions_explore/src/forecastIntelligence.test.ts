import assert from "node:assert/strict";
import test from "node:test";

import {
  analyzeForecastIntelligence,
  normalizeForecastIntelligenceTimeframe,
  resolveForecastIntelligenceConfig,
  runForecastIntelligence,
} from "./forecastIntelligence";

const ENV = {
  FORECAST_INTELLIGENCE_SERVICE_URL: "https://forecast.internal.example",
  FORECAST_INTELLIGENCE_SERVICE_TOKEN: "test-token",
};

const history = Array.from({ length: 40 }, (_, index) => ({
  timestamp: new Date(Date.UTC(2026, 0, index + 1)).toISOString(),
  open: 100 + index,
  high: 101 + index,
  low: 99 + index,
  close: 100 + index,
  volume: 1000,
}));

const response = {
  ok: true,
  result: { current_price: 139, selected_model: "ensemble" },
  forecast_series: [
    { ds: "2026-03-02T00:00:00Z", p1: 100, p25: 110, p50: 120, p75: 130, p99: 140 },
  ],
  gpt_payload: { ticker: "AAPL", scenarios: {} },
  ensemble_selection: { prophet_weight: 0.6, chronos_weight: 0.4 },
};

test("timeframe aliases and canonical values are validated", () => {
  assert.equal(normalizeForecastIntelligenceTimeframe("2w"), "2_weeks");
  assert.equal(normalizeForecastIntelligenceTimeframe("3_months"), "3_months");
  assert.throws(() => normalizeForecastIntelligenceTimeframe("90 calendar days"), /invalid_forecast_timeframe/);
});

test("service credentials are required and never returned", () => {
  assert.throws(() => resolveForecastIntelligenceConfig({}), /not_configured/);
  const config = resolveForecastIntelligenceConfig(ENV);
  assert.equal(config.url, ENV.FORECAST_INTELLIGENCE_SERVICE_URL);
  assert.equal(Object.keys(config).includes("token"), true);
});

test("forecast request forwards point-in-time history and validates five-quantile ordering", async () => {
  let sent: Record<string, unknown> = {};
  const fetchImpl = (async (_url: string | URL | Request, init?: RequestInit) => {
    sent = JSON.parse(String(init?.body || "{}"));
    return new Response(JSON.stringify(response), { status: 200, headers: { "Content-Type": "application/json" } });
  }) as typeof fetch;
  const result = await runForecastIntelligence(
    { ticker: "aapl", timeframe: "5_trading_days", asOf: history.at(-1)!.timestamp, history },
    { env: ENV, fetchImpl }
  );
  assert.equal(sent.ticker, "AAPL");
  assert.equal(sent.timeframe, "5_trading_days");
  assert.equal((sent.history as unknown[]).length, 40);
  assert.equal(result.result.selected_model, "ensemble");

  const invalidFetch = (async () => new Response(JSON.stringify({
    ...response,
    forecast_series: [{ ds: "2026-03-02", p1: 100, p25: 120, p50: 110, p75: 130, p99: 140 }],
  }), { status: 200 })) as typeof fetch;
  await assert.rejects(
    runForecastIntelligence(
      { ticker: "AAPL", timeframe: "5_trading_days", asOf: history.at(-1)!.timestamp, history },
      { env: ENV, fetchImpl: invalidFetch }
    ),
    /quantile_order_invalid/
  );
});

test("GPT analysis is a separate explicit service request", async () => {
  let calls = 0;
  const fetchImpl = (async () => {
    calls += 1;
    return new Response(JSON.stringify({ ok: true, analysis: { signal: "HOLD" }, model: "gpt-5.6-luna" }), { status: 200 });
  }) as typeof fetch;
  const result = await analyzeForecastIntelligence({ payload: { ticker: "AAPL" } }, { env: ENV, fetchImpl });
  assert.equal(calls, 1);
  assert.equal(result.analysis.signal, "HOLD");
});
