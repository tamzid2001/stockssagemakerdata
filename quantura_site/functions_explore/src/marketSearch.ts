import {economicClient} from "./economicData";
import {geminiMarketClient} from "./geminiMarketData";
import type { Request, Router } from "express";
import { isIP } from "node:net";
import { searchPredictionMarkets, discoverForecastMarkets, resolveMarketLink, gameTiming, PredictionMarketDataError, type PredictionMarketSource } from "./predictionMarketData";
import { AlpacaClient } from "./alpacaClient";
import { kalshiPerps } from "./kalshiPerps";
import { contractGroup, eventMarketPage } from "./qSearchEvents";
import { rankVerifiedCandidates } from "./qSearchRanking";
import rateLimit from "express-rate-limit";
import { dukascopy } from "./dukascopyClient";
import { FORECAST_FREQUENCIES } from "./forecastFrequency";

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
  worldbank_data360: {label:"World Bank Data360",assetClasses:["economic_series"],search:true,history:true,forecasting:["economic_series"],granularities:["1D","1ME","1QE-DEC","1YE-DEC"],redistributionStatus:"indicator_license"},
  fiscaldata: {label:"Treasury Fiscal Data",assetClasses:["economic_series"],search:true,history:true,forecasting:["economic_series"],granularities:["1D","1ME","1QE-DEC"],redistributionStatus:"open_data"},
  gemini: {label:"Gemini",assetClasses:["crypto","prediction_market"],search:true,history:true,forecasting:["crypto"],granularities:FORECAST_FREQUENCIES,redistributionStatus:"review_required"},
  dukascopy: { label:"Dukascopy",assetClasses:["fx","metal","commodity","index","equity_cfd","etf_cfd","bond_cfd","futures_cfd","fund_cfd","crypto_cfd","cfd"],search:true,history:true,forecasting:["fx","metal","commodity","index","equity_cfd","etf_cfd","bond_cfd","futures_cfd","fund_cfd","crypto_cfd","cfd"],granularities:["1min","5min","15min","30min","1h","4h","1D","1W-MON","1MS"],redistributionStatus:"review_required" },
  kalshi_perps: { label: "Kalshi Perpetuals", assetClasses: ["perpetual"], search: true, history: true, forecasting: ["perpetual"], granularities: ["1min","5min","15min","30min","1h","4h","1D","1W-MON","1MS"], redistributionStatus: "review_required" },
  alpaca: {
    label: "Alpaca",
    assetClasses: ["equity", "etf", "option"],
    search: true,
    history: true,
    forecasting: ["equity", "etf"],
    granularities: FORECAST_FREQUENCIES,
    optionGranularities: ["1min", "1h", "1D"],
    redistributionStatus: "review_required",
  },
  polymarket_us: {
    label: "Polymarket US",
    assetClasses: ["prediction_market"],
    search: true,
    history: true,
    forecasting: ["prediction_market"],
    granularities: FORECAST_FREQUENCIES,
    redistributionStatus: "review_required",
  },
  kalshi: {
    label: "Kalshi",
    assetClasses: ["prediction_market"],
    search: true,
    history: true,
    forecasting: ["prediction_market"],
    granularities: FORECAST_FREQUENCIES,
    redistributionStatus: "review_required",
  },
} as const;

function text(value: unknown, max = 160): string {
  return String(value ?? "").trim().slice(0, max);
}

const alpacaAssetCache = new Map<string, { at: number; rows: JsonRecord[] }>();
let assetCatalog: { at: number; rows: Awaited<ReturnType<AlpacaClient["listAssets"]>> } | null = null;
let assetCatalogRequest: Promise<Awaited<ReturnType<AlpacaClient["listAssets"]>>> | null = null;
async function searchAlpacaCatalog(query: string, limit: number): Promise<JsonRecord[]> {
  if (!assetCatalog || Date.now() - assetCatalog.at > 86400000) {
    if (!assetCatalogRequest) assetCatalogRequest = new AlpacaClient().listAssets().then(rows => {
      assetCatalog = { at: Date.now(), rows }; return rows;
    }).finally(() => { assetCatalogRequest = null; });
    await assetCatalogRequest;
  }
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  return assetCatalog!.rows.filter(a => words.every(w => `${a.symbol} ${a.name}`.toLowerCase().includes(w)))
    .sort((a,b) => Number(b.symbol.toLowerCase() === query.toLowerCase()) - Number(a.symbol.toLowerCase() === query.toLowerCase()))
    .slice(0,limit).map(a => ({ resource_type: "instrument", resource_id: "alpaca:"+a.symbol, symbol:a.symbol,
      name:a.name, asset_class:"equity", source:"alpaca", exchange:a.exchange, currency:"USD", unit:"USD per share", history_available:true, forecast_available:true }));
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

export function alpacaCandidateSymbols(candidateRows: JsonRecord[], alpacaRows: JsonRecord[], limit = 3): string[] {
  const existing = new Set(alpacaRows.map(row => String(row.symbol || "")));
  return [...new Set(candidateRows.filter(row => row.asset_class === "equity")
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
    if (!["auto", "alpaca", "dukascopy", "polymarket_us", "kalshi", "kalshi_perps", "gemini", "fiscaldata", "worldbank_data360"].includes(requested) || !["open", "live", "any"].includes(mode)) {
      res.status(422).json({ ok: false, error: "search_filter_invalid", message: "Choose a supported source and market status." }); return;
    }
    if (query.length < 2 && mode !== "live" && requested!=="dukascopy") {
      res.status(400).json({ ok: false, error: "search_query_too_short", message: "Enter at least two characters." });
      return;
    }
    const groups: Record<string, JsonRecord[]> = {};
    const errors: Record<string, string> = {};
    const tasks: Array<Promise<void>> = [];
    if(mode!=="live" && ["fiscaldata","worldbank_data360"].includes(requested))tasks.push(economicClient.search(requested,query,limit).then(result=>{
      groups[requested]=result.results.map((row:any)=>({resource_type:"economic_series",resource_id:requested+":"+row.id,symbol:row.id,name:row.name,asset_class:"economic_series",source:requested,exchange:requested==="fiscaldata"?"U.S. Treasury":"World Bank Data360",history_available:true,forecast_available:true,
        economic_source:{type:"economic_series",provider:requested,...(requested==="fiscaldata"?{series_id:row.id}:{dataset_id:row.dataset_id,indicator_id:row.indicator_id})}}));
    }).catch(()=>{errors[requested]="temporarily_unavailable";}));
    if(["auto","gemini"].includes(requested))tasks.push(geminiMarketClient.search(query,limit,mode).then(rows=>{groups.gemini=rows;}).catch(()=>{errors.gemini="temporarily_unavailable";}));
    if(mode!=="live" && ["auto","dukascopy"].includes(requested)) tasks.push(dukascopy.search(query,limit).then(result=>{groups.dukascopy=result.rows;if(result.stale)errors.dukascopy="catalog_snapshot";}).catch(()=>{errors.dukascopy="temporarily_unavailable";}));
    if (["auto","kalshi_perps"].includes(requested)) tasks.push(kalshiPerps.search(query,limit)
      .then(rows=>{groups.kalshi_perps=mode==="any"?rows:rows.filter(r=>r.status==="active");})
      .catch(()=>{errors.kalshi_perps="temporarily_unavailable";}));
    if (mode !== "live" && ["auto", "alpaca"].includes(requested)) {
      tasks.push(searchAlpacaCatalog(query,limit).catch(() => verifiedAlpacaAsset(query.toUpperCase()))
        .then(rows => {groups.alpaca=rows;}).catch(() => {errors.alpaca="temporarily_unavailable";}));
    }
    for (const source of ["polymarket_us", "kalshi"] as PredictionMarketSource[]) {
      if (requested !== "auto" && requested !== source) continue;
      tasks.push((mode === "any" ? searchPredictionMarkets(source, query, limit) : discoverForecastMarkets(source, query, mode as "live" | "open", true))
        .then((rows) => { groups[source] = rows.filter(row => !query || [row.eventTitle, row.marketTitle, row.outcome, row.providerSymbol, row.league].join(" ").toLowerCase().includes(query.toLowerCase())).slice(0, limit).map((row) => predictionResult(source, row)); })
        .catch(() => { errors[source] = "temporarily_unavailable"; }));
    }
    await Promise.all(tasks);
    const results = Object.values(groups).flat();
    const recommended = req.query.rank === "true" ? await rankVerifiedCandidates(query, results, {...options,ip:searchClientAddress(req)}) : null;
    if (recommended) for (const rows of Object.values(groups)) rows.sort((a,b)=>Number(b.resource_id===recommended)-Number(a.resource_id===recommended));
    res.setHeader("Cache-Control", "public, max-age=30, stale-while-revalidate=120");
    res.status(200).json({ ok: true, query, count: results.length, groups, errors, capabilities: PROVIDER_CAPABILITIES, coverage: "Bounded provider discovery, not exhaustive coverage. Paste an event link for exact lookup. Kalshi in-progress is inferred from official start time and open status, not a live score feed." });
  });
}
