import { constants, createPrivateKey, sign } from "node:crypto";
import { Router } from "express";
import {
  PolymarketMlbError,
  fetchPolymarketPricePoints,
  type PolymarketPricePoint,
} from "./polymarketMlb";

const POLYMARKET_GATEWAY = "https://gateway.polymarket.us";
const POLYMARKET_API = "https://api.polymarket.us";
const KALSHI_API = "https://external-api.kalshi.com/trade-api/v2";
const USER_AGENT = "quantura-prediction-market-data/1.0";
const MAX_SELECTED_CONTRACTS = 25;
const MAX_OUTPUT_ROWS = 100_000;
const MAX_RANGE_MS = 90 * 24 * 60 * 60 * 1000;
const CACHE_TTL_MS = 30 * 60 * 1000;

export type PredictionMarketSource = "polymarket_us" | "kalshi";
export type PredictionMarketFrequency = "raw" | "1m" | "5m" | "15m" | "30m" | "1h" | "1d" | "final";
export type MissingIntervalMode = "leave" | "forward_fill" | "drop";
export type PredictionMarketExportMode = "raw" | "normalized" | "canvas";
export type PredictionMarketTarget = "price" | "bid" | "ask" | "midpoint";

type JsonRecord = Record<string, unknown>;
type Primitive = string | number | boolean | null;

export type ProviderCategory = {
  id: string;
  label: string;
  providerId: string;
  sport: string;
  seriesCount?: number;
};

export type PredictionMarketContract = {
  source: PredictionMarketSource;
  sport: string;
  league: string;
  eventId: string;
  marketId: string;
  contractId: string;
  providerSymbol: string;
  eventTitle: string;
  marketTitle: string;
  outcome: string;
  side: "long" | "short" | "yes" | "no";
  eventStart: string | null;
  expirationTime: string | null;
  status: "upcoming" | "open" | "closed" | "settled";
  homeTeam: string | null;
  awayTeam: string | null;
  currentPrice: number | null;
  bid: number | null;
  ask: number | null;
  volume: number | null;
  openInterest: number | null;
  liquidity: number | null;
  availableFrom: string | null;
  availableTo: string | null;
};

export type NormalizedPredictionObservation = {
  source: PredictionMarketSource;
  sport: string;
  league: string;
  event_id: string;
  market_id: string;
  contract_id: string;
  item_id: string;
  market_title: string;
  outcome: string;
  event_start: string | null;
  timestamp: string;
  price: number | null;
  bid: number | null;
  ask: number | null;
  midpoint: number | null;
  last_trade: number | null;
  spread: number | null;
  spread_pct: number | null;
  volume: number | null;
  open_interest: number | null;
  liquidity: number | null;
  minutes_to_event: number | null;
  seconds_to_event: number | null;
  status: string;
  is_forward_filled: boolean;
  raw: Record<string, Primitive>;
};

export type DatasetValidation = {
  ready: boolean;
  markets: number;
  observations: number;
  missingTargetRowsRemoved: number;
  duplicateTimestampsResolved: number;
  invalidTimestampsRemoved: number;
  invalidProbabilityRowsRemoved: number;
  postStartRowsRemoved: number;
  futureRowsRemoved: number;
  timezone: "UTC";
  targetRange: { min: number | null; max: number | null };
  messages: string[];
};

export type PredictionMarketDataset = {
  source: PredictionMarketSource;
  mode: PredictionMarketExportMode;
  frequency: PredictionMarketFrequency;
  target: PredictionMarketTarget;
  missing: MissingIntervalMode;
  pregameOnly: boolean;
  headers: string[];
  rows: Array<Record<string, Primitive>>;
  previewRows: Array<Record<string, Primitive>>;
  validation: DatasetValidation;
  dateRange: { start: string | null; end: string | null };
};

export class PredictionMarketDataError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly status = 400
  ) {
    super(message);
    this.name = "PredictionMarketDataError";
  }
}

const cache = new Map<string, { expiresAt: number; value: unknown }>();

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : null;
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function text(value: unknown, max = 240): string {
  return String(value ?? "").trim().slice(0, max);
}

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

export function normalizeProbability(value: unknown): number | null {
  const number = finite(value);
  if (number === null) return null;
  const normalized = number > 1 && number <= 100 ? number / 100 : number;
  return normalized >= 0 && normalized <= 1 ? Number(normalized.toFixed(6)) : null;
}

function complement(value: number | null): number | null {
  return value === null ? null : Number((1 - value).toFixed(6));
}

function iso(value: unknown): string | null {
  const timestamp = Date.parse(text(value, 100));
  return Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : null;
}

function cleanIdentifier(value: unknown, max = 240): string {
  const clean = text(value, max);
  if (!clean || !/^[A-Za-z0-9._:-]+$/.test(clean)) {
    throw new PredictionMarketDataError("invalid_market_identifier", "A selected market identifier is invalid.");
  }
  return clean;
}

function safeFilename(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9._-]+/g, "-").replace(/-{2,}/g, "-").replace(/^-|-$/g, "").slice(0, 160) || "prediction-market-data";
}

function cached<T>(key: string): T | null {
  const entry = cache.get(key);
  if (!entry || entry.expiresAt <= Date.now()) {
    if (entry) cache.delete(key);
    return null;
  }
  return entry.value as T;
}

function remember<T>(key: string, value: T, ttl = CACHE_TTL_MS): T {
  cache.set(key, { expiresAt: Date.now() + ttl, value });
  return value;
}

async function pause(milliseconds: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function fetchJson(
  url: string,
  options: RequestInit = {},
  allowedStatuses: number[] = []
): Promise<{ status: number; payload: JsonRecord }> {
  let lastStatus = 0;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    let response: Response;
    try {
      response = await fetch(url, {
        ...options,
        headers: { Accept: "application/json", "User-Agent": USER_AGENT, ...(options.headers || {}) },
        signal: AbortSignal.timeout(25_000),
      });
    } catch (_error) {
      if (attempt < 2) {
        await pause(250 * 2 ** attempt);
        continue;
      }
      throw new PredictionMarketDataError("provider_unavailable", "The selected provider could not be reached.", 502);
    }
    lastStatus = response.status;
    const payload = asRecord(await response.json().catch(() => ({}))) || {};
    if (response.ok || allowedStatuses.includes(response.status)) return { status: response.status, payload };
    if ((response.status === 429 || response.status >= 500) && attempt < 2) {
      await pause(300 * 2 ** attempt);
      continue;
    }
    if (response.status === 401 || response.status === 403) {
      throw new PredictionMarketDataError("authentication_failed", "The provider rejected the server-side credentials.", response.status);
    }
    if (response.status === 429) {
      throw new PredictionMarketDataError("rate_limited", "The provider rate-limited this request. Try again shortly.", 429);
    }
    throw new PredictionMarketDataError("provider_unavailable", "The provider could not complete this request.", response.status >= 500 ? 502 : response.status);
  }
  throw new PredictionMarketDataError("provider_unavailable", `The provider returned HTTP ${lastStatus || "error"}.`, 502);
}

function queryUrl(base: string, params: Record<string, string | number | boolean | null | undefined>): string {
  const url = new URL(base);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== "") url.searchParams.set(key, String(value));
  });
  return url.toString();
}

function providerSource(value: unknown): PredictionMarketSource {
  const source = text(value, 40).toLowerCase();
  if (source === "polymarket_us" || source === "kalshi") return source;
  throw new PredictionMarketDataError("invalid_provider", "Choose Polymarket US or Kalshi.");
}

function frequency(value: unknown): PredictionMarketFrequency {
  const selected = text(value, 20).toLowerCase() as PredictionMarketFrequency;
  if (["raw", "1m", "5m", "15m", "30m", "1h", "1d", "final"].includes(selected)) return selected;
  throw new PredictionMarketDataError("invalid_frequency", "Choose a supported historical frequency.");
}

function exportMode(value: unknown): PredictionMarketExportMode {
  const mode = text(value, 20).toLowerCase() as PredictionMarketExportMode;
  if (["raw", "normalized", "canvas"].includes(mode)) return mode;
  throw new PredictionMarketDataError("invalid_export_mode", "Choose raw, normalized, or SageMaker Canvas-ready data.");
}

function targetField(value: unknown): PredictionMarketTarget {
  const target = text(value, 20).toLowerCase() as PredictionMarketTarget;
  if (["price", "bid", "ask", "midpoint"].includes(target)) return target;
  throw new PredictionMarketDataError("invalid_target", "Choose an available target field.");
}

function missingMode(value: unknown): MissingIntervalMode {
  const mode = text(value, 30).toLowerCase() as MissingIntervalMode;
  if (["leave", "forward_fill", "drop"].includes(mode)) return mode;
  throw new PredictionMarketDataError("invalid_missing_mode", "Choose how missing intervals should be handled.");
}

export function stableItemId(source: PredictionMarketSource, contractId: string): string {
  return `${source}:${cleanIdentifier(contractId, 300).toLowerCase()}`;
}

function primitiveRecord(value: unknown): Record<string, Primitive> {
  const raw = asRecord(value) || {};
  const result: Record<string, Primitive> = {};
  Object.entries(raw).forEach(([key, item]) => {
    if (item === null || typeof item === "string" || typeof item === "number" || typeof item === "boolean") {
      result[key.slice(0, 80)] = item as Primitive;
    }
  });
  return result;
}

function eventTeams(event: JsonRecord): { home: string | null; away: string | null } {
  const teams = asArray(event.teams).map(asRecord).filter(Boolean) as JsonRecord[];
  const home = teams.find((team) => text(team.ordering, 20).toLowerCase() === "home");
  const away = teams.find((team) => text(team.ordering, 20).toLowerCase() === "away");
  return {
    home: text(home?.name || home?.alias, 120) || null,
    away: text(away?.name || away?.alias, 120) || null,
  };
}

function polymarketStatus(event: JsonRecord, market: JsonRecord): PredictionMarketContract["status"] {
  const raw = text(market.status || event.eventState, 80).toLowerCase();
  if (raw.includes("resolved") || raw.includes("settled")) return "settled";
  if (market.closed === true || event.closed === true || raw.includes("closed")) return "closed";
  const start = Date.parse(text(market.gameStartTime || event.startTime || event.startDate, 100));
  if (Number.isFinite(start) && start > Date.now()) return "upcoming";
  return "open";
}

export function normalizePolymarketEvents(
  payload: JsonRecord,
  category: ProviderCategory
): PredictionMarketContract[] {
  const rows: PredictionMarketContract[] = [];
  const events = asArray(payload.events).map(asRecord).filter((event): event is JsonRecord => Boolean(event));
  events.forEach((event) => {
    const eventId = text(event.id || event.ticker || event.slug, 180);
    const eventTitle = text(event.title || event.ticker, 260);
    const teams = eventTeams(event);
    const markets = asArray(event.markets).map(asRecord).filter((market): market is JsonRecord => Boolean(market));
    markets.forEach((market) => {
      const marketId = text(market.id || market.slug, 180);
      const providerSymbol = text(market.slug, 220);
      if (!eventId || !marketId || !providerSymbol) return;
      const marketTitle = text(market.title || market.question || eventTitle, 300);
      const sides = asArray(market.marketSides).map(asRecord).filter((side): side is JsonRecord => Boolean(side));
      if (!sides.length) return;
      const status = polymarketStatus(event, market);
      sides.forEach((side) => {
        const contractId = text(side.id || side.identifier, 220);
        const position = side.long === false ? "short" : "long";
        const team = asRecord(side.team);
        const outcome = text(team?.name || side.description || (position === "long" ? "Long" : "Short"), 180);
        if (!contractId || !outcome) return;
        const sidePrice = normalizeProbability(asRecord(side.quote)?.value ?? side.price);
        rows.push({
          source: "polymarket_us",
          sport: category.sport,
          league: category.label,
          eventId,
          marketId,
          contractId,
          providerSymbol,
          eventTitle,
          marketTitle,
          outcome,
          side: position,
          eventStart: iso(market.gameStartTime || event.startTime || event.startDate),
          expirationTime: iso(market.endDate || event.endDate),
          status,
          homeTeam: teams.home,
          awayTeam: teams.away,
          currentPrice: sidePrice,
          bid: null,
          ask: null,
          volume: finite(market.volume || event.volume),
          openInterest: finite(market.openInterest || event.openInterest),
          liquidity: finite(market.liquidity || event.liquidity),
          availableFrom: iso(market.createdAt || event.createdAt),
          availableTo: iso(market.endDate || event.endDate || market.gameStartTime || event.startTime),
        });
      });
    });
  });
  return rows;
}

async function polymarketCategories(): Promise<ProviderCategory[]> {
  const key = "polymarket:categories";
  const existing = cached<ProviderCategory[]>(key);
  if (existing) return existing;
  const { payload } = await fetchJson(`${POLYMARKET_GATEWAY}/v2/leagues`);
  const categories = asArray(payload.leagues).map(asRecord).filter(Boolean).flatMap((league) => {
    const id = text(league?.slug, 80).toLowerCase();
    const label = text(league?.name || league?.abbreviation || id, 120);
    if (!id || !label || league?.isOperational === false || /(^|[-_ ])test($|[-_ ])/i.test(`${id} ${label}`)) return [];
    return [{ id, providerId: text(league?.tagId || league?.id, 40), label, sport: label }];
  }).sort((left, right) => left.label.localeCompare(right.label));
  return remember(key, categories);
}

async function polymarketContracts(input: {
  category: string;
  status: string;
  search: string;
  dateFrom: string;
  dateTo: string;
  page: number;
  pageSize: number;
}): Promise<{ items: PredictionMarketContract[]; total: number; scanned: number }> {
  const categories = await polymarketCategories();
  const category = categories.find((item) => item.id === input.category);
  if (!category) throw new PredictionMarketDataError("invalid_category", "Choose a currently supported Polymarket US league.");
  const events: unknown[] = [];
  if (!["closed", "settled"].includes(input.status)) {
    const { payload } = await fetchJson(queryUrl(`${POLYMARKET_GATEWAY}/v2/leagues/${encodeURIComponent(category.id)}/events`, { limit: 1000, active: true, closed: false }));
    events.push(...asArray(payload.events));
  }
  if (["any", "closed", "settled"].includes(input.status) && category.providerId) {
    const { payload } = await fetchJson(queryUrl(`${POLYMARKET_GATEWAY}/v1/events`, { limit: 250, offset: 0, closed: true, tagIds: category.providerId }));
    events.push(...asArray(payload.events));
  }
  const contracts = normalizePolymarketEvents({ events }, category);
  const search = input.search.toLowerCase();
  const from = Date.parse(input.dateFrom);
  const to = Date.parse(input.dateTo);
  const filtered = contracts.filter((contract) => {
    if (input.status !== "any" && contract.status !== input.status) return false;
    if (search && !`${contract.eventTitle} ${contract.marketTitle} ${contract.outcome} ${contract.providerSymbol}`.toLowerCase().includes(search)) return false;
    const eventTime = contract.eventStart ? Date.parse(contract.eventStart) : NaN;
    if (Number.isFinite(from) && (!Number.isFinite(eventTime) || eventTime < from)) return false;
    if (Number.isFinite(to) && (!Number.isFinite(eventTime) || eventTime > to + 86_399_999)) return false;
    return true;
  });
  const unique = [...new Map(filtered.map((item) => [`${item.providerSymbol}:${item.contractId}`, item])).values()]
    .sort((left, right) => {
      const statusPriority = { open: 0, upcoming: 1, closed: 2, settled: 3 };
      const priority = statusPriority[left.status] - statusPriority[right.status];
      if (priority) return priority;
      const leftTime = Date.parse(left.eventStart || left.expirationTime || "");
      const rightTime = Date.parse(right.eventStart || right.expirationTime || "");
      return left.status === "open" || left.status === "upcoming" ? leftTime - rightTime : rightTime - leftTime;
    });
  const offset = (input.page - 1) * input.pageSize;
  return { items: unique.slice(offset, offset + input.pageSize), total: unique.length, scanned: contracts.length };
}

type KalshiSeriesIndex = { categories: ProviderCategory[]; tagsBySeries: Map<string, string[]>; seriesByTag: Map<string, string[]> };

async function kalshiSeriesIndex(): Promise<KalshiSeriesIndex> {
  const key = "kalshi:series-index";
  const existing = cached<KalshiSeriesIndex>(key);
  if (existing) return existing;
  const { payload } = await fetchJson(queryUrl(`${KALSHI_API}/series`, { category: "Sports" }));
  const tagsBySeries = new Map<string, string[]>();
  const seriesByTag = new Map<string, string[]>();
  asArray(payload.series).map(asRecord).filter(Boolean).forEach((series) => {
    if (text(series?.category, 80).toLowerCase() !== "sports") return;
    const ticker = text(series?.ticker, 120).toUpperCase();
    if (!ticker) return;
    const tags = asArray(series?.tags).map((tag) => text(tag, 80)).filter(Boolean);
    tagsBySeries.set(ticker, tags);
    tags.forEach((tag) => seriesByTag.set(tag, [...(seriesByTag.get(tag) || []), ticker]));
  });
  const categories = [...seriesByTag.entries()].map(([label, tickers]) => ({
    id: label.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
    label,
    providerId: label,
    sport: label,
    seriesCount: new Set(tickers).size,
  })).sort((left, right) => left.label.localeCompare(right.label));
  return remember(key, { categories, tagsBySeries, seriesByTag });
}

function kalshiStatus(raw: unknown): PredictionMarketContract["status"] {
  const status = text(raw, 80).toLowerCase();
  if (["settled", "finalized", "determined"].some((value) => status.includes(value))) return "settled";
  if (["closed", "inactive"].some((value) => status.includes(value))) return "closed";
  if (["unopened", "initialized", "upcoming"].some((value) => status.includes(value))) return "upcoming";
  return "open";
}

export function normalizeKalshiEvent(eventPayload: JsonRecord, sport: string): PredictionMarketContract[] {
  const event = asRecord(eventPayload.event) || eventPayload;
  const markets = asArray(eventPayload.markets).length ? asArray(eventPayload.markets) : asArray(event.markets);
  const eventId = text(event.event_ticker || event.ticker, 180);
  const eventTitle = text(event.title || event.sub_title || eventId, 300);
  const league = text(event.series_ticker || "Kalshi Sports", 180);
  const rows: PredictionMarketContract[] = [];
  markets.map(asRecord).filter(Boolean).forEach((market) => {
    const ticker = text(market?.ticker, 180).toUpperCase();
    if (!ticker || !eventId) return;
    const yesPrice = normalizeProbability(market?.last_price_dollars ?? market?.last_price);
    const yesBid = normalizeProbability(market?.yes_bid_dollars ?? market?.yes_bid);
    const yesAsk = normalizeProbability(market?.yes_ask_dollars ?? market?.yes_ask);
    const yesOutcome = text(market?.yes_sub_title || "Yes", 180);
    const suppliedNoOutcome = text(market?.no_sub_title || "No", 180);
    const noOutcome = suppliedNoOutcome.toLowerCase() === yesOutcome.toLowerCase() ? "No" : suppliedNoOutcome;
    const eventStart = iso(market?.occurrence_datetime || market?.expected_expiration_time || market?.close_time);
    const base = {
      source: "kalshi" as const,
      sport,
      league,
      eventId,
      marketId: ticker,
      providerSymbol: ticker,
      eventTitle,
      marketTitle: text(market?.title || market?.subtitle || eventTitle, 320),
      eventStart,
      expirationTime: iso(market?.expiration_time || market?.latest_expiration_time || market?.close_time),
      status: kalshiStatus(market?.status),
      homeTeam: null,
      awayTeam: null,
      volume: finite(market?.volume_fp ?? market?.volume),
      openInterest: finite(market?.open_interest_fp ?? market?.open_interest),
      liquidity: finite(market?.liquidity_dollars ?? market?.liquidity),
      availableFrom: iso(market?.open_time || market?.created_time),
      availableTo: iso(market?.expiration_time || market?.close_time),
    };
    rows.push({
      ...base,
      contractId: `${ticker}:yes`,
      outcome: yesOutcome,
      side: "yes",
      currentPrice: yesPrice,
      bid: yesBid,
      ask: yesAsk,
    });
    rows.push({
      ...base,
      contractId: `${ticker}:no`,
      outcome: noOutcome,
      side: "no",
      currentPrice: complement(yesPrice),
      bid: complement(yesAsk),
      ask: complement(yesBid),
    });
  });
  return rows;
}

function kalshiSeriesPriority(ticker: string, search: string): number {
  const upper = ticker.toUpperCase();
  if (search && upper.includes(search.toUpperCase())) return -2;
  if (/(GAME|MATCH|MONEYLINE)/.test(upper)) return 0;
  if (/(SPREAD|TOTAL|RFI|F3|F5|BTTS)/.test(upper)) return 1;
  if (/(WIN|CHAMP|LEADER|MVP|AWARD)/.test(upper)) return 2;
  return 3;
}

async function kalshiSeriesEvents(seriesTicker: string, apiStatus: string): Promise<JsonRecord[]> {
  const cacheKey = `kalshi:events:${seriesTicker}:${apiStatus}`;
  const existing = cached<JsonRecord[]>(cacheKey);
  if (existing) return existing;
  const { payload } = await fetchJson(queryUrl(`${KALSHI_API}/events`, {
    series_ticker: seriesTicker,
    status: apiStatus,
    limit: 200,
    with_nested_markets: true,
  }));
  return remember(cacheKey, asArray(payload.events).map(asRecord).filter((event): event is JsonRecord => Boolean(event)), 5 * 60 * 1000);
}

async function kalshiContracts(input: {
  category: string;
  status: string;
  search: string;
  dateFrom: string;
  dateTo: string;
  page: number;
  pageSize: number;
}): Promise<{ items: PredictionMarketContract[]; total: number; scanned: number; scanLimited: boolean; seriesScanned: number; seriesAvailable: number; providerFailures: number }> {
  const index = await kalshiSeriesIndex();
  const category = index.categories.find((item) => item.id === input.category);
  if (!category) throw new PredictionMarketDataError("invalid_category", "Choose a current Kalshi sports category.");
  const allMatchingSeries = [...new Set(index.seriesByTag.get(category.providerId) || [])];
  const search = input.search.toLowerCase();
  const seriesLimit = 36;
  const matchingSeries = allMatchingSeries
    .sort((left, right) => kalshiSeriesPriority(left, search) - kalshiSeriesPriority(right, search) || left.localeCompare(right))
    .slice(0, seriesLimit);
  const apiStatuses = input.status === "any" ? ["open", "settled"] : ["upcoming", "open"].includes(input.status) ? ["open"] : [input.status];
  const candidates: JsonRecord[] = [];
  let providerFailures = 0;
  for (let offset = 0; offset < matchingSeries.length; offset += 6) {
    const batch = matchingSeries.slice(offset, offset + 6);
    const settled = await Promise.allSettled(batch.flatMap((seriesTicker) => apiStatuses.map((apiStatus) => kalshiSeriesEvents(seriesTicker, apiStatus))));
    settled.forEach((result) => {
      if (result.status === "fulfilled") candidates.push(...result.value);
      else providerFailures += 1;
    });
  }
  const details = candidates.map((event) => normalizeKalshiEvent({ event, markets: event.markets }, category.label));
  const from = Date.parse(input.dateFrom);
  const to = Date.parse(input.dateTo);
  const contracts = details.flat().filter((contract) => {
    if (input.status !== "any" && contract.status !== input.status && !(input.status === "upcoming" && contract.status === "open" && contract.eventStart && Date.parse(contract.eventStart) > Date.now())) return false;
    if (search && !`${contract.eventTitle} ${contract.marketTitle} ${contract.outcome} ${contract.providerSymbol}`.toLowerCase().includes(search)) return false;
    const eventTime = contract.eventStart ? Date.parse(contract.eventStart) : NaN;
    if (Number.isFinite(from) && (!Number.isFinite(eventTime) || eventTime < from)) return false;
    if (Number.isFinite(to) && (!Number.isFinite(eventTime) || eventTime > to + 86_399_999)) return false;
    return true;
  });
  const unique = [...new Map(contracts.map((contract) => [`${contract.providerSymbol}:${contract.side}`, contract])).values()]
    .sort((left, right) => {
      const leftTime = Date.parse(left.eventStart || left.expirationTime || "");
      const rightTime = Date.parse(right.eventStart || right.expirationTime || "");
      const leftFuture = Number.isFinite(leftTime) && leftTime >= Date.now();
      const rightFuture = Number.isFinite(rightTime) && rightTime >= Date.now();
      if (leftFuture !== rightFuture) return leftFuture ? -1 : 1;
      return leftFuture ? leftTime - rightTime : rightTime - leftTime;
    });
  const pageOffset = (input.page - 1) * input.pageSize;
  return {
    items: unique.slice(pageOffset, pageOffset + input.pageSize),
    total: unique.length,
    scanned: candidates.length,
    scanLimited: allMatchingSeries.length > matchingSeries.length || providerFailures > 0,
    seriesScanned: matchingSeries.length,
    seriesAvailable: allMatchingSeries.length,
    providerFailures,
  };
}

function midpoint(bid: number | null, ask: number | null): number | null {
  return bid !== null && ask !== null ? Number(((bid + ask) / 2).toFixed(6)) : null;
}

function observation(
  contract: PredictionMarketContract,
  timestamp: string,
  values: {
    price?: number | null;
    bid?: number | null;
    ask?: number | null;
    lastTrade?: number | null;
    volume?: number | null;
    openInterest?: number | null;
    raw?: Record<string, Primitive>;
  }
): NormalizedPredictionObservation {
  const eventMs = contract.eventStart ? Date.parse(contract.eventStart) : NaN;
  const rowMs = Date.parse(timestamp);
  const seconds = Number.isFinite(eventMs) && Number.isFinite(rowMs) ? Math.round((eventMs - rowMs) / 1000) : null;
  const bid = normalizeProbability(values.bid);
  const ask = normalizeProbability(values.ask);
  const price = normalizeProbability(values.price);
  const spread = bid !== null && ask !== null ? Number((ask - bid).toFixed(6)) : null;
  const mid = midpoint(bid, ask);
  return {
    source: contract.source,
    sport: contract.sport,
    league: contract.league,
    event_id: contract.eventId,
    market_id: contract.marketId,
    contract_id: contract.contractId,
    item_id: stableItemId(contract.source, contract.contractId),
    market_title: contract.marketTitle,
    outcome: contract.outcome,
    event_start: contract.eventStart,
    timestamp: new Date(rowMs).toISOString(),
    price,
    bid,
    ask,
    midpoint: mid,
    last_trade: normalizeProbability(values.lastTrade),
    spread,
    spread_pct: spread !== null && mid !== null && mid > 0 ? Number((spread / mid * 100).toFixed(4)) : null,
    volume: finite(values.volume),
    open_interest: finite(values.openInterest),
    liquidity: contract.liquidity,
    minutes_to_event: seconds === null ? null : Number((seconds / 60).toFixed(2)),
    seconds_to_event: seconds,
    status: contract.status,
    is_forward_filled: false,
    raw: values.raw || {},
  };
}

async function polymarketHistory(contract: PredictionMarketContract, startMs: number, endMs: number): Promise<NormalizedPredictionObservation[]> {
  let points: PolymarketPricePoint[];
  try {
    points = await fetchPolymarketPricePoints(cleanIdentifier(contract.providerSymbol, 220), startMs, endMs, 1);
  } catch (error) {
    if (error instanceof PolymarketMlbError) {
      throw new PredictionMarketDataError(
        error.status === 429 ? "rate_limited" : error.status === 404 ? "no_data" : "provider_unavailable",
        error.message,
        error.status
      );
    }
    throw error;
  }
  return points.map((point) => {
    const price = contract.side === "short" ? point.shortPrice : point.longPrice;
    return observation(contract, new Date(point.timestamp * 1000).toISOString(), {
      price,
      lastTrade: price,
      raw: {
        provider_timestamp: point.timestamp,
        long_price: point.longPrice,
        short_price: point.shortPrice,
        selected_position: contract.side,
      },
    });
  });
}

function kalshiCandleRows(contract: PredictionMarketContract, rawCandles: unknown): NormalizedPredictionObservation[] {
  return asArray(rawCandles).map(asRecord).filter(Boolean).flatMap((candle) => {
    const endSeconds = finite(candle?.end_period_ts);
    if (endSeconds === null) return [];
    const priceRecord = asRecord(candle?.price) || {};
    const bidRecord = asRecord(candle?.yes_bid) || {};
    const askRecord = asRecord(candle?.yes_ask) || {};
    const yesPrice = normalizeProbability(priceRecord.close_dollars ?? priceRecord.close);
    const yesBid = normalizeProbability(bidRecord.close_dollars ?? bidRecord.close);
    const yesAsk = normalizeProbability(askRecord.close_dollars ?? askRecord.close);
    const isNo = contract.side === "no";
    return [observation(contract, new Date(endSeconds * 1000).toISOString(), {
      price: isNo ? complement(yesPrice) : yesPrice,
      lastTrade: isNo ? complement(yesPrice) : yesPrice,
      bid: isNo ? complement(yesAsk) : yesBid,
      ask: isNo ? complement(yesBid) : yesAsk,
      volume: finite(candle?.volume_fp ?? candle?.volume),
      openInterest: finite(candle?.open_interest_fp ?? candle?.open_interest),
      raw: {
        end_period_ts: endSeconds,
        yes_price_open: normalizeProbability(priceRecord.open_dollars ?? priceRecord.open),
        yes_price_high: normalizeProbability(priceRecord.high_dollars ?? priceRecord.high),
        yes_price_low: normalizeProbability(priceRecord.low_dollars ?? priceRecord.low),
        yes_price_close: yesPrice,
        yes_bid_close: yesBid,
        yes_ask_close: yesAsk,
        volume: finite(candle?.volume_fp ?? candle?.volume),
        open_interest: finite(candle?.open_interest_fp ?? candle?.open_interest),
      },
    })];
  });
}

async function kalshiCandles(contract: PredictionMarketContract, startMs: number, endMs: number, requestedFrequency: PredictionMarketFrequency): Promise<NormalizedPredictionObservation[]> {
  const period = requestedFrequency === "1d" ? 1440 : requestedFrequency === "1h" ? 60 : 1;
  const stepSeconds = period === 1 ? 4_000 * 60 : period === 60 ? 2_000 * 3600 : 1_000 * 86400;
  const ticker = cleanIdentifier(contract.providerSymbol, 180).toUpperCase();
  const rows: NormalizedPredictionObservation[] = [];
  for (let startSeconds = Math.floor(startMs / 1000); startSeconds <= Math.floor(endMs / 1000); startSeconds += stepSeconds) {
    const chunkEnd = Math.min(Math.floor(endMs / 1000), startSeconds + stepSeconds - 1);
    const live = await fetchJson(queryUrl(`${KALSHI_API}/markets/candlesticks`, {
      market_tickers: ticker,
      start_ts: startSeconds,
      end_ts: chunkEnd,
      period_interval: period,
    }), {}, [400, 404, 422]);
    const liveMarkets = asArray(live.payload.markets).map(asRecord).filter(Boolean) as JsonRecord[];
    const liveMarket = liveMarkets.find((item) => text(item.market_ticker || item.ticker, 180).toUpperCase() === ticker);
    const liveRows = kalshiCandleRows(contract, liveMarket?.candlesticks);
    if (liveRows.length) {
      rows.push(...liveRows);
      continue;
    }
    const historical = await fetchJson(queryUrl(`${KALSHI_API}/historical/markets/${encodeURIComponent(ticker)}/candlesticks`, {
      start_ts: startSeconds,
      end_ts: chunkEnd,
      period_interval: period,
    }), {}, [400, 404, 422]);
    rows.push(...kalshiCandleRows(contract, historical.payload.candlesticks));
  }
  return rows;
}

async function pagedKalshiTrades(endpoint: string, contract: PredictionMarketContract, startMs: number, endMs: number): Promise<NormalizedPredictionObservation[]> {
  const ticker = cleanIdentifier(contract.providerSymbol, 180).toUpperCase();
  let cursor = "";
  const rows: NormalizedPredictionObservation[] = [];
  for (let page = 0; page < 20 && rows.length < MAX_OUTPUT_ROWS; page += 1) {
    const response = await fetchJson(queryUrl(`${KALSHI_API}${endpoint}`, {
      ticker,
      min_ts: Math.floor(startMs / 1000),
      max_ts: Math.floor(endMs / 1000),
      limit: 1000,
      cursor,
    }), {}, [400, 404, 422]);
    if (response.status !== 200) break;
    asArray(response.payload.trades).map(asRecord).filter(Boolean).forEach((trade) => {
      const timestamp = iso(trade?.created_time || trade?.created_ts);
      const yesPrice = normalizeProbability(trade?.yes_price_dollars ?? trade?.yes_price);
      if (!timestamp || yesPrice === null) return;
      const selectedPrice = contract.side === "no" ? complement(yesPrice) : yesPrice;
      rows.push(observation(contract, timestamp, {
        price: selectedPrice,
        lastTrade: selectedPrice,
        volume: finite(trade?.count_fp ?? trade?.count),
        raw: {
          trade_id: text(trade?.trade_id, 180),
          created_time: timestamp,
          yes_price: yesPrice,
          no_price: normalizeProbability(trade?.no_price_dollars ?? trade?.no_price),
          count: finite(trade?.count_fp ?? trade?.count),
          taker_side: text(trade?.taker_side || trade?.taker_outcome_side, 30),
          is_block_trade: trade?.is_block_trade === true,
        },
      }));
    });
    cursor = text(response.payload.cursor, 600);
    if (!cursor) break;
  }
  return rows;
}

async function kalshiTrades(contract: PredictionMarketContract, startMs: number, endMs: number): Promise<NormalizedPredictionObservation[]> {
  const [live, historical] = await Promise.all([
    pagedKalshiTrades("/markets/trades", contract, startMs, endMs),
    pagedKalshiTrades("/historical/trades", contract, startMs, endMs),
  ]);
  return [...live, ...historical];
}

const FREQUENCY_MS: Record<Exclude<PredictionMarketFrequency, "raw" | "final">, number> = {
  "1m": 60_000,
  "5m": 5 * 60_000,
  "15m": 15 * 60_000,
  "30m": 30 * 60_000,
  "1h": 60 * 60_000,
  "1d": 24 * 60 * 60_000,
};

function dedupeAndSort(rows: NormalizedPredictionObservation[]): { rows: NormalizedPredictionObservation[]; duplicates: number } {
  const map = new Map<string, NormalizedPredictionObservation>();
  let duplicates = 0;
  rows.sort((left, right) => Date.parse(left.timestamp) - Date.parse(right.timestamp)).forEach((row) => {
    const key = `${row.item_id}:${row.timestamp}`;
    if (map.has(key)) duplicates += 1;
    map.set(key, row);
  });
  return { rows: [...map.values()].sort((left, right) => left.item_id.localeCompare(right.item_id) || Date.parse(left.timestamp) - Date.parse(right.timestamp)), duplicates };
}

export function resamplePredictionObservations(
  inputRows: NormalizedPredictionObservation[],
  selectedFrequency: PredictionMarketFrequency,
  missing: MissingIntervalMode
): NormalizedPredictionObservation[] {
  const clean = dedupeAndSort(inputRows).rows;
  if (selectedFrequency === "raw") return clean;
  if (selectedFrequency === "final") {
    const finalRows = new Map<string, NormalizedPredictionObservation>();
    clean.forEach((row) => finalRows.set(row.item_id, row));
    return [...finalRows.values()];
  }
  const intervalMs = FREQUENCY_MS[selectedFrequency];
  const buckets = new Map<string, NormalizedPredictionObservation & { volumeAccumulator: number | null }>();
  clean.forEach((row) => {
    const timestamp = Date.parse(row.timestamp);
    if (!Number.isFinite(timestamp)) return;
    const bucketMs = Math.floor(timestamp / intervalMs) * intervalMs;
    const key = `${row.item_id}:${bucketMs}`;
    const previous = buckets.get(key);
    const volumeAccumulator = row.volume === null ? previous?.volumeAccumulator ?? null : (previous?.volumeAccumulator ?? 0) + row.volume;
    buckets.set(key, {
      ...row,
      timestamp: new Date(bucketMs).toISOString(),
      volume: volumeAccumulator,
      volumeAccumulator,
      raw: { ...row.raw, resampled_frequency: selectedFrequency },
    });
  });
  let rows = [...buckets.values()].map(({ volumeAccumulator: _volume, ...row }) => row)
    .sort((left, right) => left.item_id.localeCompare(right.item_id) || Date.parse(left.timestamp) - Date.parse(right.timestamp));
  if (missing === "drop") rows = rows.filter((row) => row.price !== null);
  if (missing !== "forward_fill" || !rows.length) return rows;
  const groups = new Map<string, NormalizedPredictionObservation[]>();
  rows.forEach((row) => groups.set(row.item_id, [...(groups.get(row.item_id) || []), row]));
  const filled: NormalizedPredictionObservation[] = [];
  groups.forEach((group) => {
    const byTimestamp = new Map(group.map((row) => [Date.parse(row.timestamp), row]));
    const start = Math.min(...byTimestamp.keys());
    const end = Math.max(...byTimestamp.keys());
    let previous: NormalizedPredictionObservation | null = null;
    for (let timestamp = start; timestamp <= end; timestamp += intervalMs) {
      const current = byTimestamp.get(timestamp);
      if (current) previous = current;
      if (!previous) continue;
      filled.push(current || {
        ...previous,
        timestamp: new Date(timestamp).toISOString(),
        volume: null,
        is_forward_filled: true,
        raw: { ...previous.raw, forward_filled: true },
      });
      if (filled.length > MAX_OUTPUT_ROWS) throw new PredictionMarketDataError("dataset_too_large", "The selected forward-filled dataset exceeds 100,000 rows. Narrow the range or markets.", 413);
    }
  });
  return filled;
}

function selectedContract(value: unknown, expectedSource: PredictionMarketSource): PredictionMarketContract {
  const raw = asRecord(value);
  if (!raw || providerSource(raw.source) !== expectedSource) throw new PredictionMarketDataError("invalid_contract", "A selected contract does not match the provider.");
  const side = text(raw.side, 20).toLowerCase();
  if (!["long", "short", "yes", "no"].includes(side)) throw new PredictionMarketDataError("invalid_contract", "A selected contract side is invalid.");
  return {
    source: expectedSource,
    sport: text(raw.sport, 100) || "Sports",
    league: text(raw.league, 180) || "Sports",
    eventId: cleanIdentifier(raw.eventId, 220),
    marketId: cleanIdentifier(raw.marketId, 220),
    contractId: cleanIdentifier(raw.contractId, 300),
    providerSymbol: cleanIdentifier(raw.providerSymbol, 220),
    eventTitle: text(raw.eventTitle, 320),
    marketTitle: text(raw.marketTitle, 320),
    outcome: text(raw.outcome, 180),
    side: side as PredictionMarketContract["side"],
    eventStart: iso(raw.eventStart),
    expirationTime: iso(raw.expirationTime),
    status: ["upcoming", "open", "closed", "settled"].includes(text(raw.status, 20)) ? text(raw.status, 20) as PredictionMarketContract["status"] : "open",
    homeTeam: text(raw.homeTeam, 160) || null,
    awayTeam: text(raw.awayTeam, 160) || null,
    currentPrice: normalizeProbability(raw.currentPrice),
    bid: normalizeProbability(raw.bid),
    ask: normalizeProbability(raw.ask),
    volume: finite(raw.volume),
    openInterest: finite(raw.openInterest),
    liquidity: finite(raw.liquidity),
    availableFrom: iso(raw.availableFrom),
    availableTo: iso(raw.availableTo),
  };
}

function timeRange(body: JsonRecord): { startMs: number; endMs: number } {
  const startMs = Date.parse(text(body.start, 100));
  const requestedEnd = Date.parse(text(body.end, 100));
  const endMs = Math.min(requestedEnd, Date.now());
  if (!Number.isFinite(startMs) || !Number.isFinite(requestedEnd) || endMs <= startMs) {
    throw new PredictionMarketDataError("invalid_date_range", "Choose a valid historical start and end time.");
  }
  if (endMs - startMs > MAX_RANGE_MS) throw new PredictionMarketDataError("date_range_too_large", "Historical requests are limited to 90 days per export.", 413);
  return { startMs, endMs };
}

function targetValue(row: NormalizedPredictionObservation, target: PredictionMarketTarget): number | null {
  return normalizeProbability(row[target]);
}

const NORMALIZED_HEADERS = [
  "source", "sport", "league", "event_id", "market_id", "contract_id", "item_id", "market_title", "outcome", "event_start", "timestamp",
  "price", "bid", "ask", "midpoint", "last_trade", "spread", "spread_pct", "volume", "open_interest", "liquidity",
  "minutes_to_event", "seconds_to_event", "status", "is_forward_filled",
];

const CANVAS_FEATURES = new Set([
  "source", "sport", "league", "event_start", "minutes_to_event", "seconds_to_event", "bid", "ask", "midpoint", "spread", "spread_pct",
  "volume", "open_interest", "liquidity", "status", "outcome", "is_forward_filled",
]);

function flattenRaw(row: NormalizedPredictionObservation): Record<string, Primitive> {
  return {
    source: row.source,
    event_id: row.event_id,
    market_id: row.market_id,
    contract_id: row.contract_id,
    outcome: row.outcome,
    timestamp: row.timestamp,
    ...primitiveRecord(row.raw),
  };
}

export function buildPredictionMarketDataset(
  inputRows: NormalizedPredictionObservation[],
  options: {
    source: PredictionMarketSource;
    mode: PredictionMarketExportMode;
    frequency: PredictionMarketFrequency;
    target: PredictionMarketTarget;
    missing: MissingIntervalMode;
    pregameOnly: boolean;
    features?: string[];
  }
): PredictionMarketDataset {
  const nowLimit = Date.now() + 5 * 60_000;
  let invalidTimestampsRemoved = 0;
  let invalidProbabilityRowsRemoved = 0;
  let postStartRowsRemoved = 0;
  let futureRowsRemoved = 0;
  const filtered = inputRows.filter((row) => {
    const timestamp = Date.parse(row.timestamp);
    if (!Number.isFinite(timestamp)) { invalidTimestampsRemoved += 1; return false; }
    if (timestamp > nowLimit) { futureRowsRemoved += 1; return false; }
    if (row.price !== null && normalizeProbability(row.price) === null) { invalidProbabilityRowsRemoved += 1; return false; }
    if (options.pregameOnly) {
      const eventStart = row.event_start ? Date.parse(row.event_start) : NaN;
      if (!Number.isFinite(eventStart)) throw new PredictionMarketDataError("event_start_unavailable", "Pregame-only export requires an event start time for every selected contract.");
      if (timestamp >= eventStart) { postStartRowsRemoved += 1; return false; }
    }
    return true;
  });
  const deduped = dedupeAndSort(filtered);
  const processed = resamplePredictionObservations(deduped.rows, options.frequency, options.missing);
  if (processed.length > MAX_OUTPUT_ROWS) throw new PredictionMarketDataError("dataset_too_large", "The processed dataset exceeds 100,000 rows. Narrow the range or markets.", 413);
  let missingTargetRowsRemoved = 0;
  let headers: string[];
  let rows: Array<Record<string, Primitive>>;
  if (options.mode === "raw") {
    rows = processed.map(flattenRaw);
    headers = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  } else if (options.mode === "normalized") {
    headers = NORMALIZED_HEADERS;
    rows = processed.map((row) => Object.fromEntries(headers.map((header) => [header, (row as unknown as Record<string, Primitive>)[header] ?? null])));
  } else {
    const features = [...new Set((options.features || []).filter((feature) => CANVAS_FEATURES.has(feature)))];
    headers = ["item_id", "timestamp", "target", ...features];
    rows = processed.flatMap((row) => {
      const target = targetValue(row, options.target);
      if (target === null) { missingTargetRowsRemoved += 1; return []; }
      const result: Record<string, Primitive> = { item_id: row.item_id, timestamp: row.timestamp, target };
      features.forEach((feature) => { result[feature] = (row as unknown as Record<string, Primitive>)[feature] ?? null; });
      return [result];
    });
  }
  rows.sort((left, right) => String(left.item_id || left.contract_id || "").localeCompare(String(right.item_id || right.contract_id || "")) || Date.parse(String(left.timestamp)) - Date.parse(String(right.timestamp)));
  const timestamps = rows.map((row) => Date.parse(String(row.timestamp || ""))).filter(Number.isFinite);
  const targets = options.mode === "canvas" ? rows.map((row) => finite(row.target)).filter((value): value is number => value !== null) : processed.map((row) => row.price).filter((value): value is number => value !== null);
  const validation: DatasetValidation = {
    ready: rows.length > 0 && invalidTimestampsRemoved === 0 && invalidProbabilityRowsRemoved === 0,
    markets: new Set(processed.map((row) => row.item_id)).size,
    observations: rows.length,
    missingTargetRowsRemoved,
    duplicateTimestampsResolved: deduped.duplicates,
    invalidTimestampsRemoved,
    invalidProbabilityRowsRemoved,
    postStartRowsRemoved,
    futureRowsRemoved,
    timezone: "UTC",
    targetRange: { min: targets.length ? Math.min(...targets) : null, max: targets.length ? Math.max(...targets) : null },
    messages: [
      "Timestamps are normalized to UTC and ordered chronologically per contract.",
      options.pregameOnly ? "Observations at or after event start were excluded." : "Full selected history is included.",
      options.missing === "forward_fill" ? "Missing fixed intervals were explicitly forward-filled after the first real observation." : options.missing === "leave" ? "Missing fixed intervals remain missing; no price was invented." : "Rows missing the selected target were dropped.",
      "Canvas target values use decimal probability units from 0.00 to 1.00.",
    ],
  };
  return {
    source: options.source,
    mode: options.mode,
    frequency: options.frequency,
    target: options.target,
    missing: options.missing,
    pregameOnly: options.pregameOnly,
    headers,
    rows,
    previewRows: rows.slice(0, 100),
    validation,
    dateRange: {
      start: timestamps.length ? new Date(Math.min(...timestamps)).toISOString() : null,
      end: timestamps.length ? new Date(Math.max(...timestamps)).toISOString() : null,
    },
  };
}

function csvCell(value: Primitive): string {
  if (value === null || value === undefined) return "";
  const valueText = String(value);
  return /[",\r\n]/.test(valueText) ? `"${valueText.replace(/"/g, '""')}"` : valueText;
}

export function predictionDatasetCsv(dataset: PredictionMarketDataset): string {
  return [dataset.headers.join(","), ...dataset.rows.map((row) => dataset.headers.map((header) => csvCell(row[header] ?? null)).join(","))].join("\r\n") + "\r\n";
}

async function prepareDataset(body: JsonRecord): Promise<PredictionMarketDataset> {
  const source = providerSource(body.source);
  const selectedFrequency = frequency(body.frequency || "1m");
  const mode = exportMode(body.mode || "normalized");
  const target = targetField(body.target || "price");
  const missing = missingMode(body.missing || "leave");
  const pregameOnly = body.pregameOnly !== false;
  const contracts = asArray(body.contracts).map((item) => selectedContract(item, source));
  if (!contracts.length) throw new PredictionMarketDataError("no_contracts_selected", "Select at least one market contract.");
  if (contracts.length > MAX_SELECTED_CONTRACTS) throw new PredictionMarketDataError("too_many_contracts", `Select no more than ${MAX_SELECTED_CONTRACTS} contracts per export.`, 413);
  const { startMs, endMs } = timeRange(body);
  const allRows: NormalizedPredictionObservation[] = [];
  for (const contract of contracts) {
    const contractEnd = pregameOnly && contract.eventStart ? Math.min(endMs, Date.parse(contract.eventStart)) : endMs;
    if (contractEnd <= startMs) continue;
    const rows = source === "polymarket_us"
      ? await polymarketHistory(contract, startMs, contractEnd)
      : selectedFrequency === "raw"
      ? await kalshiTrades(contract, startMs, contractEnd)
      : await kalshiCandles(contract, startMs, contractEnd, selectedFrequency);
    allRows.push(...rows);
    if (allRows.length > MAX_OUTPUT_ROWS * 2) throw new PredictionMarketDataError("dataset_too_large", "The provider returned too many observations. Narrow the range or selected contracts.", 413);
  }
  if (!allRows.length) throw new PredictionMarketDataError("no_data", "No historical observations are available for this selection and range.", 404);
  return buildPredictionMarketDataset(allRows, {
    source,
    mode,
    frequency: selectedFrequency,
    target,
    missing,
    pregameOnly,
    features: asArray(body.features).map((item) => text(item, 60)),
  });
}

function polymarketAuthHeaders(method: string, path: string): Record<string, string> | null {
  const keyId = text(process.env.POLYMARKET_PUBLIC_KEY, 500);
  const secret = text(process.env.POLYMARKET_SECRET_KEY, 2000);
  if (!keyId || !secret) return null;
  let raw: Buffer;
  try { raw = Buffer.from(secret, "base64"); } catch (_error) { return null; }
  if (raw.length === 64) raw = raw.subarray(0, 32);
  if (raw.length !== 32) return null;
  const timestamp = String(Date.now());
  const pkcs8 = Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), raw]);
  const signature = sign(null, Buffer.from(`${timestamp}${method}${path}`), createPrivateKey({ key: pkcs8, format: "der", type: "pkcs8" })).toString("base64");
  return { "X-PM-Access-Key": keyId, "X-PM-Timestamp": timestamp, "X-PM-Signature": signature };
}

export function kalshiAuthHeaders(method: string, path: string, now = Date.now()): Record<string, string> | null {
  const keyId = text(process.env.KALSHI_PROD_API_KEY || process.env.KALSHI_API_KEY_ID, 500);
  const privateKey = String(process.env.KALSHI_PRIVATE_KEY || "").replace(/\\n/g, "\n").trim();
  if (!keyId || !privateKey) return null;
  const timestamp = String(now);
  const cleanPath = path.split("?")[0];
  const signature = sign("sha256", Buffer.from(`${timestamp}${method}${cleanPath}`), {
    key: privateKey,
    padding: constants.RSA_PKCS1_PSS_PADDING,
    saltLength: constants.RSA_PSS_SALTLEN_DIGEST,
  }).toString("base64");
  return { "KALSHI-ACCESS-KEY": keyId, "KALSHI-ACCESS-TIMESTAMP": timestamp, "KALSHI-ACCESS-SIGNATURE": signature };
}

async function authStatus(source: PredictionMarketSource): Promise<"connected" | "missing" | "authentication_failed" | "rate_limited" | "unavailable"> {
  const path = source === "polymarket_us" ? "/v1/account/balances" : "/trade-api/v2/api_keys";
  let headers: Record<string, string> | null;
  try { headers = source === "polymarket_us" ? polymarketAuthHeaders("GET", path) : kalshiAuthHeaders("GET", path); }
  catch (_error) { return "authentication_failed"; }
  if (!headers) return "missing";
  const url = source === "polymarket_us" ? `${POLYMARKET_API}${path}` : `https://external-api.kalshi.com${path}`;
  try {
    const response = await fetch(url, { headers: { Accept: "application/json", ...headers }, signal: AbortSignal.timeout(20_000) });
    if (response.ok) return "connected";
    if (response.status === 401 || response.status === 403) return "authentication_failed";
    if (response.status === 429) return "rate_limited";
    return "unavailable";
  } catch (_error) { return "unavailable"; }
}

async function historicalAccessStatus(source: PredictionMarketSource): Promise<JsonRecord> {
  try {
    if (source === "polymarket_us") {
      const result = await polymarketContracts({ category: "mlb", status: "open", search: "", dateFrom: "", dateTo: "", page: 1, pageSize: 12 });
      const contract = result.items.find((item) => item.availableFrom && item.eventStart);
      if (!contract) return { status: "no_data", observations: 0 };
      const endMs = Math.min(Date.now(), Date.parse(contract.eventStart || ""));
      const startMs = Math.max(Date.parse(contract.availableFrom || ""), endMs - 24 * 60 * 60 * 1000);
      if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return { status: "no_data", observations: 0 };
      const rows = await polymarketHistory(contract, startMs, endMs);
      return { status: rows.length ? "connected" : "no_data", observations: rows.length };
    }
    const result = await kalshiContracts({ category: "baseball", status: "open", search: "", dateFrom: "", dateTo: "", page: 1, pageSize: 12 });
    const contract = result.items[0];
    if (!contract) return { status: "no_data", observations: 0 };
    const endMs = Math.min(Date.now(), contract.eventStart ? Date.parse(contract.eventStart) : Date.now());
    const startMs = endMs - 24 * 60 * 60 * 1000;
    const rows = await kalshiTrades(contract, startMs, endMs);
    return { status: rows.length ? "connected" : "no_data", observations: rows.length };
  } catch (error) {
    if (error instanceof PredictionMarketDataError) {
      return { status: error.code === "rate_limited" ? "rate_limited" : error.code === "no_data" ? "no_data" : "unavailable", observations: 0 };
    }
    return { status: "unavailable", observations: 0 };
  }
}

async function providerStatus(source: PredictionMarketSource, deep: boolean): Promise<JsonRecord> {
  const cacheKey = `status:${source}:${deep}`;
  const existing = cached<JsonRecord>(cacheKey);
  if (existing) return existing;
  let publicConnection: "connected" | "rate_limited" | "unavailable" = "unavailable";
  let categoryCount = 0;
  try {
    const categories = source === "polymarket_us" ? await polymarketCategories() : (await kalshiSeriesIndex()).categories;
    categoryCount = categories.length;
    publicConnection = "connected";
  } catch (error) {
    publicConnection = error instanceof PredictionMarketDataError && error.code === "rate_limited" ? "rate_limited" : "unavailable";
  }
  const configured = source === "polymarket_us"
    ? Boolean(process.env.POLYMARKET_PUBLIC_KEY && process.env.POLYMARKET_SECRET_KEY)
    : Boolean((process.env.KALSHI_PROD_API_KEY || process.env.KALSHI_API_KEY_ID) && process.env.KALSHI_PRIVATE_KEY);
  const authentication = deep ? await authStatus(source) : configured ? "configured_not_tested" : "missing";
  const historicalConnection = deep
    ? await historicalAccessStatus(source)
    : { status: publicConnection === "connected" ? "available_not_tested" : "unavailable", observations: 0 };
  return remember(cacheKey, {
    source,
    publicConnection,
    authentication,
    categoryCount,
    marketDiscovery: publicConnection === "connected",
    historicalData: deep ? historicalConnection.status === "connected" : publicConnection === "connected",
    historicalConnection,
    checkedAt: new Date().toISOString(),
    credentialsRequiredForPublicHistory: false,
  }, deep ? 5 * 60_000 : CACHE_TTL_MS);
}

export const PREDICTION_MARKET_CAPABILITIES = {
  polymarket_us: {
    source: "polymarket_us",
    label: "Polymarket US",
    discovery: true,
    historicalPrice: true,
    rawPriceObservations: true,
    historicalCandles: false,
    bidAskHistory: false,
    volumeHistory: false,
    openInterestHistory: false,
    nativeFrequencies: ["raw"],
    applicationAggregations: ["1m", "5m", "15m", "30m", "1h", "1d", "final"],
    targets: ["price"],
  },
  kalshi: {
    source: "kalshi",
    label: "Kalshi",
    discovery: true,
    historicalPrice: true,
    rawTrades: true,
    historicalCandles: true,
    bidAskHistory: true,
    volumeHistory: true,
    openInterestHistory: true,
    nativeFrequencies: ["raw", "1m", "1h", "1d"],
    applicationAggregations: ["5m", "15m", "30m", "final"],
    targets: ["price", "bid", "ask", "midpoint"],
  },
} as const;

export function registerPredictionMarketDataRoutes(router: Router): void {
  router.get("/sports/prediction-markets/status", async (req, res) => {
    try {
      const deep = String(req.query.deep || "") === "1";
      const [polymarket, kalshi] = await Promise.all([providerStatus("polymarket_us", deep), providerStatus("kalshi", deep)]);
      res.status(200).json({ ok: true, checkedAt: new Date().toISOString(), capabilities: PREDICTION_MARKET_CAPABILITIES, providers: { polymarket_us: polymarket, kalshi } });
    } catch (_error) {
      res.status(502).json({ ok: false, error: "provider_status_unavailable", message: "Provider status could not be checked safely." });
    }
  });

  router.get("/sports/prediction-markets/categories", async (req, res) => {
    try {
      const source = providerSource(req.query.source);
      const categories = source === "polymarket_us" ? await polymarketCategories() : (await kalshiSeriesIndex()).categories;
      res.status(200).json({ ok: true, source, count: categories.length, categories, capabilities: PREDICTION_MARKET_CAPABILITIES[source] });
    } catch (error) { sendPredictionMarketError(res, error); }
  });

  router.get("/sports/prediction-markets/markets", async (req, res) => {
    try {
      const source = providerSource(req.query.source);
      const category = text(req.query.category, 100).toLowerCase();
      if (!category) throw new PredictionMarketDataError("invalid_category", "Choose a sport or market category.");
      const status = ["any", "upcoming", "open", "closed", "settled"].includes(text(req.query.status, 20).toLowerCase()) ? text(req.query.status, 20).toLowerCase() : "any";
      const page = Math.max(1, Math.min(100, Math.floor(finite(req.query.page) || 1)));
      const pageSize = Math.max(5, Math.min(100, Math.floor(finite(req.query.pageSize) || 30)));
      const input = { category, status, search: text(req.query.search, 160), dateFrom: text(req.query.dateFrom, 20), dateTo: text(req.query.dateTo, 20), page, pageSize };
      const result = source === "polymarket_us" ? await polymarketContracts(input) : await kalshiContracts(input);
      res.status(200).json({ ok: true, source, category, page, pageSize, count: result.items.length, ...result });
    } catch (error) { sendPredictionMarketError(res, error); }
  });

  router.post("/sports/prediction-markets/preview", async (req, res) => {
    try {
      const dataset = await prepareDataset(asRecord(req.body) || {});
      res.status(200).json({
        ok: true,
        source: dataset.source,
        mode: dataset.mode,
        frequency: dataset.frequency,
        target: dataset.target,
        missing: dataset.missing,
        pregameOnly: dataset.pregameOnly,
        headers: dataset.headers,
        rowCount: dataset.rows.length,
        previewRows: dataset.previewRows,
        validation: dataset.validation,
        dateRange: dataset.dateRange,
      });
    } catch (error) { sendPredictionMarketError(res, error); }
  });

  router.post("/sports/prediction-markets/export", async (req, res) => {
    try {
      const body = asRecord(req.body) || {};
      const dataset = await prepareDataset(body);
      const format = text(body.format || "csv", 20).toLowerCase();
      if (!["csv", "json"].includes(format)) throw new PredictionMarketDataError("invalid_download_format", "Choose CSV or JSON.");
      const date = new Date().toISOString().slice(0, 10);
      const filename = safeFilename(`${dataset.source}-${dataset.mode}-${dataset.frequency}-${date}`);
      res.setHeader("Cache-Control", "private, no-store");
      res.setHeader("X-Content-Type-Options", "nosniff");
      if (format === "json") {
        res.setHeader("Content-Type", "application/json; charset=utf-8");
        res.setHeader("Content-Disposition", `attachment; filename="${filename}.json"`);
        res.status(200).send(JSON.stringify({ metadata: { source: dataset.source, mode: dataset.mode, frequency: dataset.frequency, target: dataset.target, validation: dataset.validation }, rows: dataset.rows }, null, 2));
        return;
      }
      res.setHeader("Content-Type", "text/csv; charset=utf-8");
      res.setHeader("Content-Disposition", `attachment; filename="${filename}.csv"`);
      res.status(200).send(predictionDatasetCsv(dataset));
    } catch (error) { sendPredictionMarketError(res, error); }
  });
}

function sendPredictionMarketError(res: { status: (status: number) => { json: (payload: JsonRecord) => void } }, error: unknown): void {
  const safe = error instanceof PredictionMarketDataError
    ? error
    : new PredictionMarketDataError("prediction_market_data_failed", "Prediction-market data could not be prepared.", 500);
  if (!(error instanceof PredictionMarketDataError)) console.error("[PredictionMarketData] request failed", error instanceof Error ? error.name : "unknown_error");
  res.status(safe.status).json({ ok: false, error: safe.code, message: safe.message });
}
