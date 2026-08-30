import crypto from "crypto";

export const FORECAST_INTELLIGENCE_TIMEFRAMES = [
  "1_trading_day",
  "3_trading_days",
  "5_trading_days",
  "10_trading_days",
  "2_weeks",
  "1_month",
  "3_months",
] as const;

export type ForecastIntelligenceTimeframe = (typeof FORECAST_INTELLIGENCE_TIMEFRAMES)[number];
export type ForecastIntelligenceFetch = typeof fetch;

export interface ForecastHistoryRow {
  timestamp: string;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  close: number;
  volume?: number | null;
}

export interface ForecastIntelligenceRunInput {
  ticker: string;
  timeframe: ForecastIntelligenceTimeframe;
  asOf: string;
  history: ForecastHistoryRow[];
  runProphet?: boolean;
  runChronos?: boolean;
  runValidation?: boolean;
  chronosFinetuneMode?: "none" | "lora" | "full";
  maxValidationOrigins?: number;
}

export interface ForecastIntelligenceServiceConfig {
  url: string;
  token: string;
  timeoutMs: number;
}

export interface ForecastIntelligenceRunResponse {
  ok: true;
  result: Record<string, unknown>;
  forecast_series: Array<Record<string, unknown>>;
  gpt_payload: Record<string, unknown>;
  ensemble_selection: Record<string, unknown> | null;
}

const QUANTILES = ["p1", "p25", "p50", "p75", "p99"] as const;

export function normalizeForecastIntelligenceTimeframe(value: unknown): ForecastIntelligenceTimeframe {
  const normalized = String(value || "").trim().toLowerCase();
  const aliases: Record<string, ForecastIntelligenceTimeframe> = {
    "1d": "1_trading_day",
    "3d": "3_trading_days",
    "5d": "5_trading_days",
    "10d": "10_trading_days",
    "2w": "2_weeks",
    "1m": "1_month",
    "3m": "3_months",
  };
  const candidate = aliases[normalized] || normalized;
  if (!FORECAST_INTELLIGENCE_TIMEFRAMES.includes(candidate as ForecastIntelligenceTimeframe)) {
    throw new Error("invalid_forecast_timeframe");
  }
  return candidate as ForecastIntelligenceTimeframe;
}

export function resolveForecastIntelligenceConfig(env: NodeJS.ProcessEnv = process.env): ForecastIntelligenceServiceConfig {
  const url = String(env.FORECAST_INTELLIGENCE_SERVICE_URL || "").trim().replace(/\/+$/, "");
  const token = String(env.FORECAST_INTELLIGENCE_SERVICE_TOKEN || "").trim();
  const parsedTimeout = Number(env.FORECAST_INTELLIGENCE_TIMEOUT_MS || 285000);
  if (!url || !/^https?:\/\//i.test(url)) throw new Error("forecast_intelligence_service_not_configured");
  if (!token) throw new Error("forecast_intelligence_service_token_not_configured");
  return {
    url,
    token,
    timeoutMs: Math.min(Math.max(Number.isFinite(parsedTimeout) ? parsedTimeout : 285000, 5000), 295000),
  };
}

function finite(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function validateForecastSeries(rows: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(rows) || !rows.length || rows.length > 365) {
    throw new Error("forecast_intelligence_series_invalid");
  }
  return rows.map((raw, index) => {
    const row = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
    const timestamp = String(row.ds || "").trim();
    if (!timestamp || Number.isNaN(Date.parse(timestamp))) {
      throw new Error(`forecast_intelligence_timestamp_invalid:${index}`);
    }
    const values = QUANTILES.map((name) => finite(row[name]));
    if (values.some((value) => value === null || !(value as number > 0))) {
      throw new Error(`forecast_intelligence_quantile_invalid:${index}`);
    }
    for (let offset = 1; offset < values.length; offset += 1) {
      if ((values[offset - 1] as number) > (values[offset] as number)) {
        throw new Error(`forecast_intelligence_quantile_order_invalid:${index}`);
      }
    }
    return row;
  });
}

function validateRunResponse(payload: unknown): ForecastIntelligenceRunResponse {
  const value = payload && typeof payload === "object" ? (payload as Record<string, unknown>) : {};
  const result = value.result && typeof value.result === "object" ? (value.result as Record<string, unknown>) : null;
  const gptPayload = value.gpt_payload && typeof value.gpt_payload === "object" ? (value.gpt_payload as Record<string, unknown>) : null;
  if (value.ok !== true || !result || !gptPayload) throw new Error("forecast_intelligence_response_invalid");
  const currentPrice = finite(result.current_price);
  if (currentPrice === null || currentPrice <= 0) throw new Error("forecast_intelligence_current_price_invalid");
  const selectedModel = String(result.selected_model || "").trim();
  if (!["prophet", "chronos_2", "ensemble"].includes(selectedModel)) {
    throw new Error("forecast_intelligence_selected_model_invalid");
  }
  const series = validateForecastSeries(value.forecast_series);
  return {
    ok: true,
    result,
    forecast_series: series,
    gpt_payload: gptPayload,
    ensemble_selection:
      value.ensemble_selection && typeof value.ensemble_selection === "object"
        ? (value.ensemble_selection as Record<string, unknown>)
        : null,
  };
}

async function servicePost(
  path: string,
  body: Record<string, unknown>,
  config: ForecastIntelligenceServiceConfig,
  fetchImpl: ForecastIntelligenceFetch
): Promise<unknown> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), config.timeoutMs);
  try {
    const response = await fetchImpl(`${config.url}${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const detail = String((payload as Record<string, unknown>)?.detail || "").trim();
      if (response.status === 401) throw new Error("forecast_intelligence_service_unauthorized");
      if (response.status === 429) throw new Error("forecast_intelligence_service_rate_limited");
      if (response.status === 503) throw new Error(detail || "forecast_intelligence_service_unavailable");
      throw new Error(detail || "forecast_intelligence_service_failed");
    }
    return payload;
  } catch (error: any) {
    if (error?.name === "AbortError") throw new Error("forecast_intelligence_service_timeout");
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

export async function runForecastIntelligence(
  input: ForecastIntelligenceRunInput,
  options: { env?: NodeJS.ProcessEnv; fetchImpl?: ForecastIntelligenceFetch } = {}
): Promise<ForecastIntelligenceRunResponse> {
  const config = resolveForecastIntelligenceConfig(options.env);
  const ticker = String(input.ticker || "").trim().toUpperCase();
  if (!/^[A-Z][A-Z0-9.\-]{0,11}$/.test(ticker)) throw new Error("invalid_ticker");
  if (!Array.isArray(input.history) || input.history.length < 40) throw new Error("insufficient_point_in_time_history");
  const body = {
    ticker,
    timeframe: normalizeForecastIntelligenceTimeframe(input.timeframe),
    as_of: input.asOf,
    history: input.history.slice(-5000),
    run_prophet: input.runProphet !== false,
    run_chronos: input.runChronos !== false,
    run_validation: input.runValidation !== false,
    chronos_finetune_mode: input.chronosFinetuneMode || "none",
    max_validation_origins: Math.min(Math.max(Math.floor(Number(input.maxValidationOrigins) || 8), 1), 50),
  };
  return validateRunResponse(await servicePost("/v1/forecast", body, config, options.fetchImpl || fetch));
}

export async function analyzeForecastIntelligence(
  input: {
    payload: Record<string, unknown>;
    reasoningEffort?: string;
    promptVersion?: string;
    fewshotVersion?: string;
  },
  options: { env?: NodeJS.ProcessEnv; fetchImpl?: ForecastIntelligenceFetch } = {}
): Promise<{ analysis: Record<string, unknown>; model: string }> {
  const config = resolveForecastIntelligenceConfig(options.env);
  const raw = await servicePost(
    "/v1/analyze",
    {
      payload: input.payload,
      reasoning_effort: input.reasoningEffort || "medium",
      prompt_version: input.promptVersion || "v1-adversarial",
      fewshot_version: input.fewshotVersion || "v1",
    },
    config,
    options.fetchImpl || fetch
  );
  const value = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
  const analysis = value.analysis && typeof value.analysis === "object" ? (value.analysis as Record<string, unknown>) : null;
  if (value.ok !== true || !analysis) throw new Error("forecast_intelligence_analysis_invalid");
  return { analysis, model: String(value.model || "gpt-5.6-luna") };
}

export function forecastIntelligenceHash(payload: Record<string, unknown>): string {
  return crypto.createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}
