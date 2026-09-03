import { Router } from "express";
import { AlpacaClient, AlpacaError, barsToCsv, publicAlpacaError, type AlpacaBar } from "./alpacaClient";
import { YahooFinanceClient } from "./yahooMarketData";

function filePart(value: unknown): string {
  return String(value || "data").replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 80) || "data";
}

function requestedLimit(value: unknown): number {
  if (String(value || "").toLowerCase() === "all") return 0;
  const number = Number(value);
  return [500, 1000, 1500, 2000, 50000].includes(number) ? number : 2000;
}

function stockSource(value: unknown): "auto" | "alpaca" | "yahoo" {
  const source = String(value || "auto").trim().toLowerCase();
  return source === "alpaca" || source === "yahoo" ? source : "auto";
}

function inferredRange(timeframeValue: unknown, limit: number, endValue: unknown): { start: string; end: string } {
  const endParsed = Date.parse(String(endValue || ""));
  const end = Number.isFinite(endParsed) ? new Date(endParsed) : new Date();
  if (limit === 0) return { start: "1970-01-01T00:00:00.000Z", end: end.toISOString() };
  const timeframe = String(timeframeValue || "1Day").toLowerCase();
  const minutesPerRow = timeframe.includes("day") || timeframe === "1d"
    ? 60 * 24 * 2
    : timeframe.includes("hour") || timeframe === "1h"
      ? 60 * 3
      : Math.max(1, Number.parseInt(timeframe, 10) || 1) * 3;
  const lookbackMs = Math.max(7 * 86400000, Math.min(20 * 365 * 86400000, limit * minutesPerRow * 60000));
  return { start: new Date(end.getTime() - lookbackMs).toISOString(), end: end.toISOString() };
}

export type StockHistoryResult = {
  provider: "alpaca" | "yahoo";
  sourceRequested: "auto" | "alpaca" | "yahoo";
  fallbackUsed: boolean;
  symbol: string;
  timeframe: string;
  feed: string;
  adjustment: string;
  session: string;
  rows: AlpacaBar[];
};

/** Shared provider-aware history service used by downloads and forecast jobs. */
export async function fetchStockHistoryData(body: Record<string, unknown>): Promise<StockHistoryResult> {
  const source = stockSource(body.source || body.provider);
  const limit = requestedLimit(body.limit);
  const range = inferredRange(body.timeframe || body.interval, limit, body.end);
  const input = {
    symbol: String(body.symbol || body.ticker || ""),
    start: String(body.start || range.start),
    end: String(body.end || range.end),
    timeframe: String(body.timeframe || body.interval || "1Day"),
    adjustment: String(body.adjustment || "raw"),
    feed: String(body.feed || ""),
    session: String(body.session || "regular"),
    limit,
  };
  const alpaca = new AlpacaClient();
  const yahoo = new YahooFinanceClient();
  let provider: "alpaca" | "yahoo" = source === "yahoo" ? "yahoo" : "alpaca";
  let fallbackUsed = false;
  let result;
  if (source === "yahoo") {
    result = await yahoo.getStockBars(input);
  } else if (source === "alpaca") {
    result = await alpaca.getStockBars(input);
  } else if (/[=^]/.test(String(input.symbol || ""))) {
    provider = "yahoo";
    fallbackUsed = true;
    result = await yahoo.getStockBars(input);
  } else {
    try {
      result = await alpaca.getStockBars(input);
    } catch (error) {
      if (error instanceof AlpacaError && error.code === "invalid_request") throw error;
      provider = "yahoo";
      fallbackUsed = true;
      result = await yahoo.getStockBars(input);
    }
  }
  return { provider, sourceRequested: source, fallbackUsed, ...result };
}

export function registerMarketDataRoutes(router: Router): void {
  router.get("/market-data/history/status", (_req, res) => {
    const alpaca = new AlpacaClient();
    res.status(200).json({
      ok: true,
      defaultSource: "auto",
      sources: {
        alpaca: { available: alpaca.isConfigured(), label: "Alpaca" },
        yahoo: { available: true, label: "Yahoo Finance" },
      },
    });
  });

  router.get("/market-data/alpaca/status", async (_req, res) => {
    const client = new AlpacaClient();
    if (!client.isConfigured()) {
      res.status(503).json({ ok: false, error: "configuration", message: "Alpaca credentials have not been configured for this deployment." });
      return;
    }
    try {
      const checks = await client.testConnection();
      res.status(200).json({ ok: true, provider: "alpaca", checks });
    } catch (error) {
      const safe = publicAlpacaError(error);
      res.status(safe.status).json(safe.body);
    }
  });

  const stockHistory = async (req: any, res: any) => {
    try {
      const body = req.method === "GET" ? req.query : req.body || {};
      const { provider, sourceRequested, fallbackUsed, ...result } = await fetchStockHistoryData(body);
      if (String(body.format || req.query?.format || "").toLowerCase() === "csv") {
        const filename = `${filePart(result.symbol)}-${provider}-${filePart(result.timeframe)}-${filePart(body.end || "latest")}.csv`;
        res.setHeader("Content-Type", "text/csv; charset=utf-8");
        res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
        res.status(200).send(barsToCsv(result.symbol, result.rows));
        return;
      }
      res.status(200).json({
        ok: true,
        provider,
        sourceRequested,
        fallbackUsed,
        ...result,
        count: result.rows.length,
      });
    } catch (error) {
      const safe = publicAlpacaError(error);
      res.status(safe.status).json(safe.body);
    }
  };
  router.get("/ticker/history", stockHistory);
  router.post("/market-data/stocks/history", stockHistory);

  router.get("/market-data/options/expirations", async (req, res) => {
    try {
      const underlying = String(req.query.underlying || req.query.symbol || "");
      const source = stockSource(req.query.source || req.query.provider);
      const alpaca = new AlpacaClient();
      const yahoo = new YahooFinanceClient();
      let provider: "alpaca" | "yahoo" = source === "yahoo" ? "yahoo" : "alpaca";
      let fallbackUsed = false;
      let expirations: string[];
      if (source === "yahoo") {
        try {
          expirations = await yahoo.listOptionExpirations(underlying);
        } catch (error) {
          if (error instanceof AlpacaError && error.code === "invalid_request") throw error;
          provider = "alpaca";
          fallbackUsed = true;
          expirations = await alpaca.listOptionExpirations(underlying);
        }
      }
      else if (source === "alpaca") expirations = await alpaca.listOptionExpirations(underlying);
      else {
        try {
          expirations = await alpaca.listOptionExpirations(underlying);
        } catch (error) {
          if (error instanceof AlpacaError && error.code === "invalid_request") throw error;
          provider = "yahoo";
          fallbackUsed = true;
          expirations = await yahoo.listOptionExpirations(underlying);
        }
      }
      res.status(200).json({ ok: true, provider, sourceRequested: source, fallbackUsed, underlying: underlying.toUpperCase(), expirations });
    } catch (error) {
      const safe = publicAlpacaError(error);
      res.status(safe.status).json(safe.body);
    }
  });

  router.get("/market-data/options/chain", async (req, res) => {
    try {
      const underlying = String(req.query.underlying || req.query.symbol || "");
      const source = stockSource(req.query.source || req.query.provider);
      const input = {
        underlying,
        expiration: String(req.query.expiration || ""),
        type: String(req.query.type || ""),
        feed: String(req.query.feed || ""),
      };
      const alpaca = new AlpacaClient();
      const yahoo = new YahooFinanceClient();
      let provider: "alpaca" | "yahoo" = source === "yahoo" ? "yahoo" : "alpaca";
      let fallbackUsed = false;
      let contracts;
      if (source === "yahoo") {
        try {
          contracts = await yahoo.getOptionChain(input);
        } catch (error) {
          if (error instanceof AlpacaError && error.code === "invalid_request") throw error;
          provider = "alpaca";
          fallbackUsed = true;
          contracts = await alpaca.getOptionChain(input);
        }
      }
      else if (source === "alpaca") contracts = await alpaca.getOptionChain(input);
      else {
        try {
          contracts = await alpaca.getOptionChain(input);
        } catch (error) {
          if (error instanceof AlpacaError && error.code === "invalid_request") throw error;
          provider = "yahoo";
          fallbackUsed = true;
          contracts = await yahoo.getOptionChain(input);
        }
      }
      res.status(200).json({ ok: true, provider, sourceRequested: source, fallbackUsed, underlying: underlying.toUpperCase(), count: contracts.length, contracts });
    } catch (error) {
      const safe = publicAlpacaError(error);
      res.status(safe.status).json(safe.body);
    }
  });

  router.post("/market-data/options/history", async (req, res) => {
    try {
      const body = req.body || {};
      const source = stockSource(body.source || body.provider);
      const input = {
        contractSymbol: body.contractSymbol,
        start: body.start,
        end: body.end,
        timeframe: body.timeframe,
        feed: body.feed,
        limit: requestedLimit(body.limit),
      };
      const alpaca = new AlpacaClient();
      const yahoo = new YahooFinanceClient();
      let provider: "alpaca" | "yahoo" = source === "yahoo" ? "yahoo" : "alpaca";
      let fallbackUsed = false;
      let result;
      if (source === "yahoo") result = await yahoo.getOptionBars(input);
      else if (source === "alpaca") result = await alpaca.getOptionBars(input);
      else {
        try {
          result = await alpaca.getOptionBars(input);
        } catch (error) {
          if (error instanceof AlpacaError && error.code === "invalid_request") throw error;
          provider = "yahoo";
          fallbackUsed = true;
          result = await yahoo.getOptionBars(input);
        }
      }
      if (String(body.format || "").toLowerCase() === "csv") {
        const filename = `${filePart(result.contractSymbol)}-${provider}-${filePart(result.timeframe)}-${filePart(body.start)}-${filePart(body.end)}.csv`;
        res.setHeader("Content-Type", "text/csv; charset=utf-8");
        res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
        res.status(200).send(barsToCsv(result.contractSymbol, result.rows));
        return;
      }
      res.status(200).json({ ok: true, provider, sourceRequested: source, fallbackUsed, ...result, count: result.rows.length });
    } catch (error) {
      const safe = publicAlpacaError(error);
      res.status(safe.status).json(safe.body);
    }
  });
}
