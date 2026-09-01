import type { Router } from "express";
import { searchPredictionMarkets, type PredictionMarketSource } from "./predictionMarketData";
import { AlpacaClient } from "./alpacaClient";

type JsonRecord = Record<string, unknown>;

export const PROVIDER_CAPABILITIES = {
  alpaca: {
    label: "Alpaca",
    assetClasses: ["equity", "etf", "option"],
    search: false,
    history: true,
    forecasting: ["equity", "etf"],
    redistributionStatus: "review_required",
  },
  yahoo: {
    label: "Yahoo Finance",
    assetClasses: ["equity", "etf", "fx", "commodity_proxy", "rate_proxy", "crypto", "option"],
    search: true,
    history: true,
    forecasting: ["equity", "etf", "fx", "commodity_proxy", "rate_proxy", "crypto"],
    redistributionStatus: "review_required",
  },
  polymarket_us: {
    label: "Polymarket US",
    assetClasses: ["prediction_market"],
    search: true,
    history: true,
    forecasting: ["prediction_market"],
    redistributionStatus: "review_required",
  },
  kalshi: {
    label: "Kalshi",
    assetClasses: ["prediction_market"],
    search: true,
    history: true,
    forecasting: ["prediction_market"],
    redistributionStatus: "review_required",
  },
} as const;

function text(value: unknown, max = 160): string {
  return String(value ?? "").trim().slice(0, max);
}

function yahooAssetClass(quoteType: unknown, symbol: string): string {
  const type = text(quoteType, 40).toUpperCase();
  if (type === "ETF") return "etf";
  if (type === "CURRENCY") return "fx";
  if (type === "FUTURE") return "commodity_proxy";
  if (type === "CRYPTOCURRENCY") return "crypto";
  if (type === "INDEX" && /^\^(TNX|TYX|FVX|IRX)/.test(symbol)) return "rate_proxy";
  if (type === "INDEX") return "index";
  return "equity";
}

async function searchYahoo(query: string, limit: number): Promise<JsonRecord[]> {
  const url = new URL("https://query2.finance.yahoo.com/v1/finance/search");
  url.searchParams.set("q", query);
  url.searchParams.set("quotesCount", String(limit));
  url.searchParams.set("newsCount", "0");
  url.searchParams.set("enableFuzzyQuery", "true");
  const response = await fetch(url, {
    headers: { Accept: "application/json", "User-Agent": "quantura-market-search/1.0" },
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw new Error("yahoo_search_unavailable");
  const payload = await response.json() as JsonRecord;
  const quotes = Array.isArray(payload.quotes) ? payload.quotes : [];
  return quotes.slice(0, limit).flatMap((value) => {
    const item = value && typeof value === "object" ? value as JsonRecord : {};
    const symbol = text(item.symbol, 32).toUpperCase();
    if (!symbol || !/^[A-Z0-9.^=\-]{1,32}$/.test(symbol)) return [];
    const assetClass = yahooAssetClass(item.quoteType, symbol);
    if (!PROVIDER_CAPABILITIES.yahoo.forecasting.includes(assetClass as any)) return [];
    return [{
      resource_type: "instrument",
      resource_id: `yahoo:${symbol}`,
      symbol,
      name: text(item.longname || item.shortname || item.name, 220) || symbol,
      asset_class: assetClass,
      source: "yahoo",
      exchange: text(item.exchDisp || item.exchange, 80) || null,
      currency: text(item.currency, 16) || null,
      unit: assetClass === "fx" ? "quote currency per base currency" : null,
      history_available: true,
      forecast_available: true,
    }];
  });
}

async function searchAlpaca(query: string): Promise<JsonRecord[]> {
  const symbol = query.trim().toUpperCase();
  if (!/^[A-Z][A-Z0-9.\-]{0,14}$/.test(symbol)) return [];
  const asset = await new AlpacaClient().getAsset(symbol);
  if (!asset.symbol || asset.status === "inactive") return [];
  return [{
    resource_type: "instrument",
    resource_id: `alpaca:${asset.symbol}`,
    symbol: asset.symbol,
    name: asset.name,
    asset_class: asset.assetClass === "us_equity" ? "equity" : asset.assetClass,
    source: "alpaca",
    exchange: asset.exchange || null,
    currency: "USD",
    unit: "USD per share",
    history_available: true,
    forecast_available: ["us_equity", "equity"].includes(asset.assetClass),
  }];
}

function predictionResult(source: PredictionMarketSource, contract: any): JsonRecord {
  return {
    resource_type: "prediction_market_contract",
    resource_id: `${source}:${contract.contractId}`,
    symbol: contract.providerSymbol,
    name: contract.marketTitle || contract.eventTitle,
    asset_class: "prediction_market",
    source,
    exchange: source === "kalshi" ? "Kalshi" : "Polymarket US",
    currency: null,
    unit: "decimal probability (0–1)",
    history_available: true,
    forecast_available: true,
    event_id: contract.eventId,
    market_id: contract.marketId,
    contract_id: contract.contractId,
    sport: contract.sport,
    league: contract.league,
    event_start: contract.eventStart,
    status: contract.status,
  };
}

export function registerMarketSearchRoutes(router: Router): void {
  router.get("/market-search/capabilities", (_req, res) => {
    res.setHeader("Cache-Control", "public, max-age=300, stale-while-revalidate=900");
    res.status(200).json({ ok: true, providers: PROVIDER_CAPABILITIES });
  });

  router.get("/market-search", async (req, res) => {
    const query = text(req.query.q, 100);
    const requested = text(req.query.source || "auto", 40).toLowerCase();
    const limit = Math.min(Math.max(Number(req.query.limit) || 8, 1), 20);
    if (query.length < 2) {
      res.status(400).json({ ok: false, error: "search_query_too_short", message: "Enter at least two characters." });
      return;
    }
    const groups: Record<string, JsonRecord[]> = {};
    const errors: Record<string, string> = {};
    const tasks: Array<Promise<void>> = [];
    if (["auto", "yahoo"].includes(requested)) {
      tasks.push(searchYahoo(query, limit).then((rows) => { groups.yahoo = rows; }).catch(() => { errors.yahoo = "temporarily_unavailable"; }));
    }
    if (["auto", "alpaca"].includes(requested)) {
      tasks.push(searchAlpaca(query).then((rows) => { groups.alpaca = rows; }).catch(() => { errors.alpaca = "unavailable_or_not_found"; }));
    }
    for (const source of ["polymarket_us", "kalshi"] as PredictionMarketSource[]) {
      if (requested !== "auto" && requested !== source) continue;
      tasks.push(searchPredictionMarkets(source, query, limit)
        .then((rows) => { groups[source] = rows.map((row) => predictionResult(source, row)); })
        .catch(() => { errors[source] = "temporarily_unavailable"; }));
    }
    await Promise.all(tasks);
    const results = Object.values(groups).flat();
    res.setHeader("Cache-Control", "public, max-age=30, stale-while-revalidate=120");
    res.status(200).json({ ok: true, query, count: results.length, groups, errors, capabilities: PROVIDER_CAPABILITIES });
  });
}
