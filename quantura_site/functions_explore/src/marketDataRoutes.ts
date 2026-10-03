import { yahooRetrySeconds } from "./yahooRequests";
import { Router } from "express";
import {aggregateObservedBars,forecastFrequency,frequencyMinutes,frequencyTimeframe} from "./forecastFrequency";
import { AlpacaClient, AlpacaError, barsToCsv, publicAlpacaError, type AlpacaBar } from "./alpacaClient";
import { YahooFinanceClient } from "./yahooMarketData";
import { dukascopy } from "./dukascopyClient";
import { registerCompanyLogoRoutes } from "./companyLogos";

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
  const end = Number.isFinite(endParsed) ? new Date(endParsed) : new Date(Math.floor(Date.now() / 60000) * 60000);
  if (limit === 0) return { start: "1970-01-01T00:00:00.000Z", end: end.toISOString() };
  const frequency=forecastFrequency(timeframeValue || "1Day");
  const minutesPerRow=frequencyMinutes(frequency)*(frequency==="1MS"?1.5:3);
  const lookbackMs = Math.max(7 * 86400000, Math.min(100 * 366 * 86400000, limit * minutesPerRow * 60000));
  return { start: new Date(end.getTime() - lookbackMs).toISOString(), end: end.toISOString() };
}

export type StockHistoryResult = {
  provider: "alpaca" | "yahoo" | "dukascopy";
  sourceRequested: "auto" | "alpaca" | "yahoo" | "dukascopy";
  fallbackUsed: boolean;
  symbol: string;
  timeframe: string;
  feed: string;
  adjustment: string;
  session: string;
  exchangeTimezone?: string;
  barIntervalMinutes?: number;
  priceSide?: string;
  metadata?: Record<string,unknown>;
  warnings?: string[];
  rows: AlpacaBar[];
};

/** Shared provider-aware history service used by downloads and forecast jobs. */
export async function fetchStockHistoryData(body: Record<string, unknown>, clients: { alpaca?: AlpacaClient; yahoo?: YahooFinanceClient } = {}): Promise<StockHistoryResult> {
  if (String(body.source || body.provider).toLowerCase() === "dukascopy") return dukascopy.history(body,false,body.forecast_mode===true);
  let frequency;
  try{frequency=forecastFrequency(body.timeframe || body.interval || "1Day");}
  catch{throw new AlpacaError("invalid_request","Choose 1, 5, 15 or 30 minutes; 1 or 4 hours; daily, weekly or monthly bars.",422);}
  if(["4h","1W-MON","1MS"].includes(frequency)) {
    const limit=requestedLimit(body.limit), range=inferredRange(frequency,limit,body.end);
    const start=Date.parse(String(body.start || range.start)),end=Math.min(Date.parse(String(body.end || range.end)),Date.now());
    if(!Number.isFinite(start)||!Number.isFinite(end)||start>=end)throw new AlpacaError("invalid_request","Choose a valid history range.",422);
    const base=frequency==="4h"?"1Hour":"1Day";
    const result=await fetchStockHistoryData({...body,timeframe:base,start:new Date(start).toISOString(),end:new Date(end).toISOString(),limit:50000},clients);
    // Daily equity bars label an exchange session (Alpaca: NY midnight;
    // Yahoo: local session opening). Group by its actual local date, not
    // timestamp + 24h, which loses the last US session of a month. The
    // next UTC date boundary is a conservative close availability time.
    const sessionDate=base==="1Day"?new Intl.DateTimeFormat("en-CA",{timeZone:result.exchangeTimezone || "UTC",year:"numeric",month:"2-digit",day:"2-digit"}):null;
    const nativeRows=sessionDate?result.rows.map(row=>({...row,timestamp:sessionDate.format(new Date(row.timestamp))+"T00:00:00.000Z"})):result.rows;
    const rows=aggregateObservedBars(nativeRows,frequency,start,end,base==="1Hour"?60:1440);
    return {...result,timeframe:frequencyTimeframe(frequency),rows:limit?rows.slice(-limit):rows,
      metadata:{...result.metadata,bucket_timezone:"UTC",timestamp_convention:"bucket_start",close_available_at:"calendar bucket end",base_interval:base,daily_bucket_basis:sessionDate?"exchange_session_date":undefined,completed_bars_only:true,missing_intervals:"not_filled",base_history_bounded:result.rows.length===50000},
      warnings:[...(result.warnings||[]),...(result.rows.length===50000?["Base history reached the 50,000-observation bound; narrow or split this download range."]:[]),"Calendar weeks begin Monday UTC; months begin on the first UTC day. Daily stock bars group by exchange session date. Partial periods and missing buckets are not filled."]};
  }
  const source = stockSource(body.source || body.provider);
  const limit = requestedLimit(body.limit);
  const range = inferredRange(body.timeframe || body.interval, limit, body.end);
  const input = {
    symbol: String(body.symbol || body.ticker || ""),
    start: String(body.start || range.start),
    end: String(body.end || range.end),
    timeframe: frequencyTimeframe(frequency),
    adjustment: String(body.adjustment || "raw"),
    feed: String(body.feed || ""),
    session: String(body.session || "regular"),
    limit,
  };
  const alpaca = clients.alpaca || new AlpacaClient();
  const yahoo = clients.yahoo || new YahooFinanceClient();
  let provider: "alpaca" | "yahoo" = source === "yahoo" ? "yahoo" : "alpaca";
  let fallbackUsed = false;
  let result;
  if (source === "yahoo") {
    try { result = await yahoo.getStockBars(input); }
    catch (error) {
      if (!(error instanceof AlpacaError) || error.code !== "rate_limit" || !alpaca.isConfigured() || !/^[A-Z][A-Z0-9.-]{0,14}$/.test(input.symbol.toUpperCase())) throw error;
      // A real verified US asset can use the existing provider during Yahoo's cooldown.
      // International stocks, FX, indices and unsupported assets never become US proxies.
      try {
        const asset = await alpaca.getAsset(input.symbol);
        if (asset.status === "inactive" || !["us_equity", "equity"].includes(asset.assetClass)) throw error;
        result = await alpaca.getStockBars({ ...input, feed: "" });
        provider = "alpaca"; fallbackUsed = true;
      } catch { throw error; }
    }
  } else if (source === "alpaca") {
    result = await alpaca.getStockBars(input);
  } else if (/[=^]|\.(?:[C-Z]|[A-Z]{2,})$|-(?:USD|EUR|GBP|JPY|USDT)$/i.test(String(input.symbol || ""))) {
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
  return { provider, sourceRequested: source, fallbackUsed, exchangeTimezone: provider === "alpaca" ? "America/New_York" : "UTC", ...result };
}

export function registerMarketDataRoutes(router: Router): void {
  registerCompanyLogoRoutes(router);
  router.get("/market-data/history/status", (_req, res) => {
    const alpaca = new AlpacaClient();
    res.status(200).json({
      ok: true,
      defaultSource: "auto",
      sources: {
        alpaca: { available: alpaca.isConfigured(), label: "Alpaca" },
        yahoo: { available: true, label: "Yahoo Finance" },
        dukascopy: { available: true, label: "Dukascopy", priceSides: ["bid","ask"] },
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
      if (safe.status === 429 && yahooRetrySeconds()) res.setHeader("Retry-After", String(yahooRetrySeconds()));
      res.status(safe.status).json(safe.body);
    }
  });

  const stockHistory = async (req: any, res: any) => {
    try {
      const body = req.method === "GET" ? req.query : req.body || {};
      const { provider, sourceRequested, fallbackUsed, ...result } = String(body.source || body.provider).toLowerCase()==="dukascopy" && body.page_mode===true
        ? await dukascopy.history(body,true) : await fetchStockHistoryData(body);
      if (String(body.format || req.query?.format || "").toLowerCase() === "csv") {
        if((result as any).next_cursor)throw new AlpacaError("invalid_request","Finish all paged JSON requests before exporting CSV, or request an unpaged range.",422);
        const filename = `${filePart(result.symbol)}-${provider}-${filePart(result.timeframe)}-${filePart(body.end || "latest")}.csv`;
        res.setHeader("Content-Type", "text/csv; charset=utf-8");
        res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
        res.status(200).send(provider==="dukascopy" ? "timestamp,close\r\n"+result.rows.map((r:AlpacaBar)=>`${r.timestamp},${r.close}`).join("\r\n") : barsToCsv(result.symbol, result.rows));
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
      if (safe.status === 429 && yahooRetrySeconds()) res.setHeader("Retry-After", String(yahooRetrySeconds()));
      res.status(safe.status).json(safe.body);
    }
  };
  router.get("/ticker/history", stockHistory);
  router.post("/market-data/stocks/history", stockHistory);

  router.get("/market-data/dukascopy/instruments", async (req,res) => {
    const catalog=await dukascopy.catalog(),query=String(req.query.search || "").trim();
    const matches=(await dukascopy.search(query,catalog.instruments.length)).rows;
    const offset=Number(req.query.offset || 0), limit=Math.min(200,Math.max(1,Number(req.query.limit)||100));
    if(!Number.isInteger(offset)||offset<0||offset>matches.length){res.status(422).json({ok:false,message:"Choose a valid catalog offset."});return;}
    res.json({ok:true,provider:"dukascopy",instruments:matches.slice(offset,offset+limit),total:matches.length,catalog_count:catalog.instruments.length,stale:catalog.stale,next_offset:offset+limit<matches.length?offset+limit:null});
  });

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
      if (safe.status === 429 && yahooRetrySeconds()) res.setHeader("Retry-After", String(yahooRetrySeconds()));
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
      if (safe.status === 429 && yahooRetrySeconds()) res.setHeader("Retry-After", String(yahooRetrySeconds()));
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
      if (safe.status === 429 && yahooRetrySeconds()) res.setHeader("Retry-After", String(yahooRetrySeconds()));
      res.status(safe.status).json(safe.body);
    }
  });
}
