import { Router } from "express";
import { AlpacaClient, barsToCsv, publicAlpacaError } from "./alpacaClient";

function filePart(value: unknown): string {
  return String(value || "data").replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 80) || "data";
}

function requestedLimit(value: unknown): number {
  if (String(value || "").toLowerCase() === "all") return 0;
  const number = Number(value);
  return [500, 1000, 1500, 2000, 50000].includes(number) ? number : 2000;
}

export function registerMarketDataRoutes(router: Router): void {
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
      const client = new AlpacaClient();
      const result = await client.getStockBars({
        symbol: body.symbol || body.ticker,
        start: body.start,
        end: body.end,
        timeframe: body.timeframe || body.interval,
        adjustment: body.adjustment,
        feed: body.feed,
        session: body.session,
        limit: requestedLimit(body.limit),
      });
      if (String(body.format || req.query?.format || "").toLowerCase() === "csv") {
        const filename = `${filePart(result.symbol)}-${filePart(result.timeframe)}-${filePart(body.start)}-${filePart(body.end)}.csv`;
        res.setHeader("Content-Type", "text/csv; charset=utf-8");
        res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
        res.status(200).send(barsToCsv(result.symbol, result.rows));
        return;
      }
      res.status(200).json({ ok: true, provider: "alpaca", ...result, count: result.rows.length });
    } catch (error) {
      const safe = publicAlpacaError(error);
      res.status(safe.status).json(safe.body);
    }
  };
  router.get("/ticker/history", stockHistory);
  router.post("/market-data/stocks/history", stockHistory);

  router.get("/market-data/options/expirations", async (req, res) => {
    try {
      const client = new AlpacaClient();
      const underlying = String(req.query.underlying || req.query.symbol || "");
      const expirations = await client.listOptionExpirations(underlying);
      res.status(200).json({ ok: true, provider: "alpaca", underlying: underlying.toUpperCase(), expirations });
    } catch (error) {
      const safe = publicAlpacaError(error);
      res.status(safe.status).json(safe.body);
    }
  });

  router.get("/market-data/options/chain", async (req, res) => {
    try {
      const client = new AlpacaClient();
      const underlying = String(req.query.underlying || req.query.symbol || "");
      const contracts = await client.getOptionChain({
        underlying,
        expiration: String(req.query.expiration || ""),
        type: String(req.query.type || ""),
        feed: String(req.query.feed || ""),
      });
      res.status(200).json({ ok: true, provider: "alpaca", underlying: underlying.toUpperCase(), count: contracts.length, contracts });
    } catch (error) {
      const safe = publicAlpacaError(error);
      res.status(safe.status).json(safe.body);
    }
  });

  router.post("/market-data/options/history", async (req, res) => {
    try {
      const body = req.body || {};
      const client = new AlpacaClient();
      const result = await client.getOptionBars({
        contractSymbol: body.contractSymbol,
        start: body.start,
        end: body.end,
        timeframe: body.timeframe,
        feed: body.feed,
        limit: requestedLimit(body.limit),
      });
      if (String(body.format || "").toLowerCase() === "csv") {
        const filename = `${filePart(result.contractSymbol)}-${filePart(result.timeframe)}-${filePart(body.start)}-${filePart(body.end)}.csv`;
        res.setHeader("Content-Type", "text/csv; charset=utf-8");
        res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
        res.status(200).send(barsToCsv(result.contractSymbol, result.rows));
        return;
      }
      res.status(200).json({ ok: true, provider: "alpaca", ...result, count: result.rows.length });
    } catch (error) {
      const safe = publicAlpacaError(error);
      res.status(safe.status).json(safe.body);
    }
  });
}
