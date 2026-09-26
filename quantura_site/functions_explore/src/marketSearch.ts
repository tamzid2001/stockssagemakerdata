import { yahooJson } from "./yahooRequests";
import type { Request, Router } from "express";
import { isIP } from "node:net";
import { searchPredictionMarkets, discoverForecastMarkets, resolveMarketLink, gameTiming, PredictionMarketDataError, type PredictionMarketSource } from "./predictionMarketData";
import { AlpacaClient } from "./alpacaClient";
import { kalshiPerps } from "./kalshiPerps";
import { contractGroup, eventMarketPage } from "./qSearchEvents";
import { rankVerifiedCandidates } from "./qSearchRanking";
import rateLimit from "express-rate-limit";

type JsonRecord = Record<string, unknown>;

/** Vercel overwrites this edge header. Never trust it on a direct/local server. */
export function searchClientAddress(req: Pick<Request,"headers"|"ip"|"socket">, vercel = process.env.VERCEL === "1"): string {
  if (vercel) {
    const value=req.headers["x-vercel-forwarded-for"] || req.headers["x-forwarded-for"];
    if (typeof value === "string" && isIP(value.trim())) return value.trim();
  }
  return req.ip || req.socket?.remoteAddress || "unknown";
}

export const PROVIDER_CAPABILITIES = {
  kalshi_perps: { label: "Kalshi Perpetuals", assetClasses: ["perpetual"], search: true, history: true, forecasting: ["perpetual"], granularities: ["1min","1h","1D"], redistributionStatus: "review_required" },
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

const yahooSearchCache = new Map<string, { at: number; rows: JsonRecord[] }>();
const yahooSearchInflight = new Map<string, Promise<JsonRecord[]>>();
const alpacaAssetCache = new Map<string, { at: number; rows: JsonRecord[] }>();


async function searchYahoo(query: string, limit: number): Promise<JsonRecord[]> {
  const key = `${query.trim().toLowerCase()}:${limit}`;
  const cached = yahooSearchCache.get(key);
  if (cached && Date.now() - cached.at < 15 * 60_000) return cached.rows;
  if (yahooSearchInflight.has(key)) return yahooSearchInflight.get(key)!;
  const request = fetchYahoo().then(rows => {
    yahooSearchCache.delete(key);
    yahooSearchCache.set(key, { at: Date.now(), rows });
    if (yahooSearchCache.size > 256) yahooSearchCache.delete(yahooSearchCache.keys().next().value!);
    return rows;
  }).catch(error => {
    // Company metadata can be served briefly while the upstream rate-limits.
    if (cached && Date.now() - cached.at < 24 * 3600_000) return cached.rows;
    throw error;
  }).finally(() => yahooSearchInflight.delete(key));
  yahooSearchInflight.set(key, request);
  return request;

  async function fetchYahoo(): Promise<JsonRecord[]> {
    const url = new URL("https://query2.finance.yahoo.com/v1/finance/search");
    url.searchParams.set("q", query);
    url.searchParams.set("quotesCount", String(limit));
    url.searchParams.set("newsCount", "0");
    url.searchParams.set("enableFuzzyQuery", "true");
    const payload = await yahooJson(url.toString(), fetch, { "User-Agent": "quantura-market-search/1.0" }, 15 * 60_000) as JsonRecord;
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

async function verifiedAlpacaAsset(symbol: string): Promise<JsonRecord[]> {
  const cached = alpacaAssetCache.get(symbol);
  if (cached && Date.now() - cached.at < 24 * 3600_000) return cached.rows;
  const rows = await searchAlpaca(symbol);
  alpacaAssetCache.set(symbol, { at: Date.now(), rows });
  if (alpacaAssetCache.size > 256) alpacaAssetCache.delete(alpacaAssetCache.keys().next().value!);
  return rows;
}

export function alpacaCandidateSymbols(yahooRows: JsonRecord[], alpacaRows: JsonRecord[], limit = 3): string[] {
  const existing = new Set(alpacaRows.map(row => String(row.symbol || "")));
  return [...new Set(yahooRows.filter(row => row.asset_class === "equity")
    .map(row => String(row.symbol || "")))].filter(symbol => symbol && !existing.has(symbol)).slice(0, limit);
}

export function predictionResult(source: PredictionMarketSource, contract: any): JsonRecord {
  return {
    resource_type: "prediction_market_contract",
    resource_id: `${source}:${contract.contractId}`,
    symbol: contract.providerSymbol,
    name: `${contract.outcome} · ${contract.eventTitle || contract.marketTitle}`,
    asset_class: "prediction_market",
    source,
    exchange: source === "kalshi" ? "Kalshi" : "Polymarket US",
    currency: null,
    unit: "decimal probability (0–1)",
    history_available: true,
    forecast_available: true,
    event_id: contract.eventId,
    event_slug: contract.eventSlug || null,
    market_group: contractGroup(contract),
    market_title: contract.marketTitle,
    market_id: contract.marketId,
    contract_id: contract.contractId,
    sport: contract.sport,
    league: contract.league,
    event_start: contract.eventStart,
    status: contract.status,
    timing: gameTiming(contract),
    outcome: contract.outcome,
    side: contract.side,
    current_price: contract.currentPrice,
    contract,
  };
}

export function registerMarketSearchRoutes(router: Router, options: {db?:FirebaseFirestore.Firestore} = {}): void {
  router.use("/market-search", rateLimit({windowMs:60_000,limit:60,standardHeaders:true,legacyHeaders:false,keyGenerator:req=>searchClientAddress(req),message:{ok:false,error:"search_rate_limited",message:"Please wait before searching again."}}));
  router.get("/market-search/event", async (req,res)=>{
    try {
      const page=await eventMarketPage(String(req.query.source||""),String(req.query.event_id||""),String(req.query.cursor||""));
      res.setHeader("Cache-Control","public, max-age=30");
      const {contracts,...metadata}=page;
      res.json({ok:true,...metadata,count:contracts.length,groups:{[page.source]:contracts.map(c=>predictionResult(c.source,c))}});
    } catch(error) {
      const safe=error instanceof PredictionMarketDataError?error:new PredictionMarketDataError("event_unavailable","Unable to load event markets. Please retry.",502);
      res.status(safe.status).json({ok:false,error:safe.code,message:safe.message});
    }
  });
  router.get("/market-search/resolve", async (req, res) => {
    try {
      const rows = await resolveMarketLink(req.query.url);
      const source = rows[0].source;
      res.setHeader("Cache-Control", "public, max-age=15");
      res.json({ ok: true, count: rows.length, groups: { [source]: rows.map(c => predictionResult(source, c)) }, errors: {}, coverage: "Provider-verified contracts for this link. Choose the intended team or side." });
    } catch (error) {
      const safe = error instanceof PredictionMarketDataError ? error : new PredictionMarketDataError("market_link_unavailable", "Unable to resolve this link. Check the URL or retry the provider.", 502);
      res.status(safe.status).json({ ok: false, error: safe.code, message: safe.message });
    }
  });
  router.get("/market-search/capabilities", (_req, res) => {
    res.setHeader("Cache-Control", "public, max-age=300, stale-while-revalidate=900");
    res.status(200).json({ ok: true, providers: PROVIDER_CAPABILITIES });
  });

  router.get("/market-search", async (req, res) => {
    const query = text(req.query.q, 100);
    const requested = text(req.query.source || "auto", 40).toLowerCase();
    const mode = text(req.query.mode || "open", 20);
    const limit = Math.min(Math.max(Number(req.query.limit) || 8, 1), 20);
    if (!["auto", "yahoo", "alpaca", "polymarket_us", "kalshi", "kalshi_perps"].includes(requested) || !["open", "live", "any"].includes(mode)) {
      res.status(422).json({ ok: false, error: "search_filter_invalid", message: "Choose a supported source and market status." }); return;
    }
    if (query.length < 2 && mode !== "live") {
      res.status(400).json({ ok: false, error: "search_query_too_short", message: "Enter at least two characters." });
      return;
    }
    const groups: Record<string, JsonRecord[]> = {};
    const errors: Record<string, string> = {};
    const tasks: Array<Promise<void>> = [];
    if (["auto","kalshi_perps"].includes(requested)) tasks.push(kalshiPerps.search(query,limit)
      .then(rows=>{groups.kalshi_perps=mode==="any"?rows:rows.filter(r=>r.status==="active");})
      .catch(()=>{errors.kalshi_perps="temporarily_unavailable";}));
    if (mode !== "live" && ["auto", "yahoo"].includes(requested)) {
      tasks.push(searchYahoo(query, limit).then((rows) => { groups.yahoo = rows; }).catch(() => { errors.yahoo = "temporarily_unavailable"; }));
    }
    if (mode !== "live" && ["auto", "alpaca"].includes(requested)) {
      tasks.push(verifiedAlpacaAsset(query.toUpperCase()).then((rows) => { groups.alpaca = rows; }).catch(() => { errors.alpaca = "unavailable_or_not_found"; }));
    }
    for (const source of ["polymarket_us", "kalshi"] as PredictionMarketSource[]) {
      if (requested !== "auto" && requested !== source) continue;
      tasks.push((mode === "any" ? searchPredictionMarkets(source, query, limit) : discoverForecastMarkets(source, query, mode as "live" | "open", true))
        .then((rows) => { groups[source] = rows.filter(row => !query || [row.eventTitle, row.marketTitle, row.outcome, row.providerSymbol, row.league].join(" ").toLowerCase().includes(query.toLowerCase())).slice(0, limit).map((row) => predictionResult(source, row)); })
        .catch(() => { errors[source] = "temporarily_unavailable"; }));
    }
    await Promise.all(tasks);
    if (requested === "auto" && mode !== "live" && groups.yahoo?.length) {
      const symbols = alpacaCandidateSymbols(groups.yahoo, groups.alpaca || []);
      const verified = await Promise.all(symbols.map(symbol => verifiedAlpacaAsset(symbol).catch(() => [])));
      groups.alpaca = [...(groups.alpaca || []), ...verified.flat()].slice(0, limit);
      if (groups.alpaca.length) delete errors.alpaca;
    }
    const results = Object.values(groups).flat();
    const recommended = req.query.rank === "true" ? await rankVerifiedCandidates(query, results, {...options,ip:searchClientAddress(req)}) : null;
    if (recommended) for (const rows of Object.values(groups)) rows.sort((a,b)=>Number(b.resource_id===recommended)-Number(a.resource_id===recommended));
    res.setHeader("Cache-Control", "public, max-age=30, stale-while-revalidate=120");
    res.status(200).json({ ok: true, query, count: results.length, groups, errors, capabilities: PROVIDER_CAPABILITIES, coverage: "Bounded provider discovery, not exhaustive coverage. Paste an event link for exact lookup. Kalshi in-progress is inferred from official start time and open status, not a live score feed." });
  });
}
