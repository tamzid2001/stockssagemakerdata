export type AlpacaErrorCode =
  | "configuration"
  | "authentication"
  | "rate_limit"
  | "unsupported_symbol"
  | "no_data"
  | "entitlement"
  | "invalid_request"
  | "network"
  | "upstream";

export class AlpacaError extends Error {
  readonly code: AlpacaErrorCode;
  readonly status: number;

  constructor(code: AlpacaErrorCode, message: string, status = 502) {
    super(message);
    this.name = "AlpacaError";
    this.code = code;
    this.status = status;
  }
}

export type AlpacaBar = {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  tradeCount: number | null;
  vwap: number | null;
  session: "premarket" | "regular" | "after_hours" | "overnight";
};

export type AlpacaLatestPrice = {
  symbol: string;
  price: number;
  timestamp: string;
  session: AlpacaBar["session"];
};

export type AlpacaAsset = {
  symbol: string;
  name: string;
  exchange: string;
  assetClass: string;
  status: string;
  tradable: boolean;
};

export type StockHistoryInput = {
  symbol: string;
  start: string;
  end: string;
  timeframe: string;
  adjustment?: string;
  feed?: string;
  session?: string;
  limit?: number;
};

export type OptionHistoryInput = {
  contractSymbol: string;
  start: string;
  end: string;
  timeframe: string;
  feed?: string;
  limit?: number;
};

export type OptionContract = {
  symbol: string;
  underlying: string;
  expiration: string;
  strike: number;
  type: "call" | "put";
  bid: number | null;
  ask: number | null;
  last: number | null;
  volume: number | null;
  openInterest: number | null;
  impliedVolatility: number | null;
  delta: number | null;
  gamma: number | null;
  theta: number | null;
  vega: number | null;
};

type FetchLike = typeof fetch;

const TIMEFRAMES = new Map([
  ["1min", "1Min"],
  ["1m", "1Min"],
  ["5min", "5Min"],
  ["5m", "5Min"],
  ["15min", "15Min"],
  ["15m", "15Min"],
  ["30min", "30Min"],
  ["30m", "30Min"],
  ["1hour", "1Hour"],
  ["1h", "1Hour"],
  ["1day", "1Day"],
  ["1d", "1Day"],
]);
const STOCK_FEEDS = new Set(["iex", "sip", "boats", "otc"]);
const OPTION_FEEDS = new Set(["indicative", "opra"]);
const ADJUSTMENTS = new Set(["raw", "split", "dividend", "spin-off", "all"]);

function cleanBase(value: string | undefined, fallback: string): string {
  return String(value || fallback).trim().replace(/\/+$/, "");
}

function normalizeSymbol(value: unknown, option = false): string {
  const symbol = String(value || "").trim().toUpperCase();
  const pattern = option ? /^[A-Z0-9.\-]{10,32}$/ : /^[A-Z][A-Z0-9.\-]{0,14}$/;
  if (!pattern.test(symbol)) {
    throw new AlpacaError("invalid_request", option ? "Select a valid options contract." : "Enter a valid ticker symbol.", 400);
  }
  return symbol;
}

function normalizeDate(value: unknown, label: string): string {
  const raw = String(value || "").trim();
  const timestamp = Date.parse(raw);
  if (!raw || !Number.isFinite(timestamp)) {
    throw new AlpacaError("invalid_request", `${label} is not a valid date.`, 400);
  }
  return new Date(timestamp).toISOString();
}

function normalizeRange(startValue: unknown, endValue: unknown): { start: string; end: string } {
  const start = normalizeDate(startValue, "Start date");
  const end = normalizeDate(endValue, "End date");
  if (Date.parse(start) >= Date.parse(end)) {
    throw new AlpacaError("invalid_request", "End date must be after start date.", 400);
  }
  return { start, end };
}

function normalizeTimeframe(value: unknown): string {
  const normalized = TIMEFRAMES.get(String(value || "").trim().toLowerCase());
  if (!normalized) {
    throw new AlpacaError("invalid_request", "Choose a supported timeframe: 1, 5, 15, or 30 minutes; 1 hour; or 1 day.", 400);
  }
  return normalized;
}

function finiteOrNull(value: unknown): number | null {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function requiredNumber(value: unknown): number {
  return finiteOrNull(value) ?? 0;
}

function nyClock(timestamp: string): { hour: number; minute: number } {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(new Date(timestamp));
  const map = new Map(parts.map((part) => [part.type, part.value]));
  return { hour: Number(map.get("hour") || 0), minute: Number(map.get("minute") || 0) };
}

export function classifyEquitySession(timestamp: string): AlpacaBar["session"] {
  const { hour, minute } = nyClock(timestamp);
  const total = hour * 60 + minute;
  if (total >= 4 * 60 && total < 9 * 60 + 30) return "premarket";
  if (total >= 9 * 60 + 30 && total < 16 * 60) return "regular";
  if (total >= 16 * 60 && total < 20 * 60) return "after_hours";
  return "overnight";
}

function mapBar(raw: Record<string, unknown>): AlpacaBar {
  const timestamp = String(raw.t || raw.timestamp || "");
  return {
    timestamp,
    open: requiredNumber(raw.o ?? raw.open),
    high: requiredNumber(raw.h ?? raw.high),
    low: requiredNumber(raw.l ?? raw.low),
    close: requiredNumber(raw.c ?? raw.close),
    volume: requiredNumber(raw.v ?? raw.volume),
    tradeCount: finiteOrNull(raw.n ?? raw.trade_count),
    vwap: finiteOrNull(raw.vw ?? raw.vwap),
    session: classifyEquitySession(timestamp),
  };
}

export function barsToCsv(symbol: string, bars: AlpacaBar[]): string {
  const escape = (value: unknown) => {
    const raw = value === null || value === undefined ? "" : String(value);
    return /[",\r\n]/.test(raw) ? `"${raw.replace(/"/g, '""')}"` : raw;
  };
  const header = ["symbol", "timestamp", "open", "high", "low", "close", "volume", "trade_count", "vwap", "session"];
  const rows = bars.map((bar) => [
    symbol,
    bar.timestamp,
    bar.open,
    bar.high,
    bar.low,
    bar.close,
    bar.volume,
    bar.tradeCount,
    bar.vwap,
    bar.session,
  ]);
  return [header, ...rows].map((row) => row.map(escape).join(",")).join("\n") + "\n";
}

export class AlpacaClient {
  private readonly key: string;
  private readonly secret: string;
  private readonly tradingBase: string;
  private readonly dataBase: string;
  private readonly fetchImpl: FetchLike;

  constructor(options: { fetchImpl?: FetchLike } = {}) {
    this.key = String(process.env.ALPACA_API_KEY || "").trim();
    this.secret = String(process.env.ALPACA_SECRET_KEY || "").trim();
    this.tradingBase = cleanBase(process.env.ALPACA_BASE_URL || process.env.ALPACA_API_BASE, "https://paper-api.alpaca.markets");
    this.dataBase = cleanBase(process.env.ALPACA_DATA_URL || process.env.ALPACA_DATA_BASE, "https://data.alpaca.markets");
    this.fetchImpl = options.fetchImpl || fetch;
  }

  isConfigured(): boolean {
    return Boolean(this.key && this.secret);
  }

  private async request(path: string, options: { trading?: boolean; query?: URLSearchParams } = {}): Promise<Record<string, unknown>> {
    if (!this.isConfigured()) {
      throw new AlpacaError("configuration", "Alpaca credentials have not been configured for this deployment.", 503);
    }
    const base = options.trading ? this.tradingBase : this.dataBase;
    const url = `${base}${path}${options.query?.toString() ? `?${options.query.toString()}` : ""}`;
    let response: Response;
    try {
      response = await this.fetchImpl(url, {
        headers: {
          "APCA-API-KEY-ID": this.key,
          "APCA-API-SECRET-KEY": this.secret,
          Accept: "application/json",
        },
        signal: AbortSignal.timeout(25000),
      });
    } catch (_error) {
      throw new AlpacaError("network", "The market-data service could not be reached. Try again.", 502);
    }
    if (!response.ok) {
      if (response.status === 401 || response.status === 403) {
        const code: AlpacaErrorCode = response.status === 403 ? "entitlement" : "authentication";
        const message = response.status === 403
          ? "This Alpaca account is not entitled to the selected feed or dataset."
          : "Alpaca authentication failed. The deployment credentials need to be updated.";
        throw new AlpacaError(code, message, response.status);
      }
      if (response.status === 404) throw new AlpacaError("unsupported_symbol", "The symbol or contract was not found.", 404);
      if (response.status === 429) throw new AlpacaError("rate_limit", "Alpaca rate-limited this request. Wait briefly and retry.", 429);
      if (response.status === 400 || response.status === 422) {
        throw new AlpacaError("invalid_request", "Alpaca rejected the selected symbol, dates, timeframe, or feed.", 400);
      }
      throw new AlpacaError("upstream", "Alpaca could not complete the request. Try again later.", 502);
    }
    try {
      return (await response.json()) as Record<string, unknown>;
    } catch (_error) {
      throw new AlpacaError("upstream", "Alpaca returned an unreadable response.", 502);
    }
  }

  async testConnection(): Promise<{ account: boolean; marketData: boolean; feed: string }> {
    await this.request("/v2/account", { trading: true });
    const end = new Date();
    const start = new Date(end.getTime() - 14 * 24 * 60 * 60 * 1000);
    const result = await this.getStockBars({
      symbol: "AAPL",
      start: start.toISOString(),
      end: end.toISOString(),
      timeframe: "1Day",
      feed: "iex",
      limit: 1,
    });
    return { account: true, marketData: result.rows.length > 0, feed: result.feed };
  }

  async getAsset(symbolValue: string): Promise<AlpacaAsset> {
    const symbol = normalizeSymbol(symbolValue);
    const payload = await this.request(`/v2/assets/${encodeURIComponent(symbol)}`, { trading: true });
    return {
      symbol: String(payload.symbol || symbol).toUpperCase(),
      name: String(payload.name || symbol).trim(),
      exchange: String(payload.exchange || "").trim(),
      assetClass: String(payload.class || payload.asset_class || "us_equity").trim(),
      status: String(payload.status || "").trim(),
      tradable: payload.tradable === true,
    };
  }

  async getStockBars(input: StockHistoryInput): Promise<{ symbol: string; timeframe: string; feed: string; adjustment: string; session: string; rows: AlpacaBar[] }> {
    const symbol = normalizeSymbol(input.symbol);
    const { start, end } = normalizeRange(input.start, input.end);
    const timeframe = normalizeTimeframe(input.timeframe);
    const feed = STOCK_FEEDS.has(String(input.feed || "").toLowerCase()) ? String(input.feed).toLowerCase() : "iex";
    const adjustment = ADJUSTMENTS.has(String(input.adjustment || "").toLowerCase()) ? String(input.adjustment).toLowerCase() : "raw";
    const session = String(input.session || "extended").toLowerCase() === "regular" ? "regular" : "extended";
    const requestedRows = Number(input.limit);
    const maxRows = requestedRows === 0 ? Number.POSITIVE_INFINITY : Math.max(1, Math.min(requestedRows || 2000, 50000));
    const rows: AlpacaBar[] = [];
    let pageToken = "";
    do {
      const query = new URLSearchParams({
        timeframe,
        start,
        end,
        limit: String(Math.min(10000, maxRows - rows.length)),
        adjustment,
        feed,
        sort: "desc",
      });
      if (pageToken) query.set("page_token", pageToken);
      const payload = await this.request(`/v2/stocks/${encodeURIComponent(symbol)}/bars`, { query });
      const rawBars = Array.isArray(payload.bars) ? payload.bars : [];
      rows.push(...rawBars.map((bar) => mapBar((bar || {}) as Record<string, unknown>)));
      pageToken = String(payload.next_page_token || "");
    } while (pageToken && rows.length < maxRows);
    const filtered = rows
      .filter((row) => row.timestamp && (session !== "regular" || row.session === "regular"))
      .sort((left, right) => Date.parse(left.timestamp) - Date.parse(right.timestamp))
      .slice(0, maxRows);
    if (!filtered.length) throw new AlpacaError("no_data", "No observations were available for this symbol and date range.", 404);
    return { symbol, timeframe, feed, adjustment, session, rows: filtered };
  }

  async getLatestStockPrices(symbolValues: string[], feedValue = "iex"): Promise<Map<string, AlpacaLatestPrice>> {
    const symbols = [...new Set(symbolValues.map((value) => normalizeSymbol(value)))];
    if (!symbols.length) return new Map();
    const feed = STOCK_FEEDS.has(String(feedValue || "").toLowerCase()) ? String(feedValue).toLowerCase() : "iex";
    const prices = new Map<string, AlpacaLatestPrice>();
    for (let index = 0; index < symbols.length; index += 100) {
      const chunk = symbols.slice(index, index + 100);
      const query = new URLSearchParams({ symbols: chunk.join(","), feed });
      const payload = await this.request("/v2/stocks/bars/latest", { query });
      const root = payload.bars && typeof payload.bars === "object"
        ? payload.bars as Record<string, Record<string, unknown>>
        : {};
      for (const symbol of chunk) {
        const raw = root[symbol];
        if (!raw || typeof raw !== "object") continue;
        const bar = mapBar(raw);
        if (!bar.timestamp || !Number.isFinite(bar.close) || bar.close <= 0) continue;
        prices.set(symbol, {
          symbol,
          price: bar.close,
          timestamp: bar.timestamp,
          session: bar.session,
        });
      }
    }
    return prices;
  }

  async listOptionContracts(input: { underlying: string; expiration?: string; type?: string; limit?: number }): Promise<Array<Record<string, unknown>>> {
    const underlying = normalizeSymbol(input.underlying);
    const maxRows = Math.max(1, Math.min(Number(input.limit) || 2000, 10000));
    const query = new URLSearchParams({ underlying_symbols: underlying, status: "active", limit: String(maxRows) });
    if (input.expiration) {
      query.set("expiration_date", String(input.expiration));
    } else {
      // Alpaca otherwise defaults expiration_date_lte to the upcoming weekend,
      // which makes a healthy option chain look like it has only one expiry.
      const today = new Date();
      const latest = new Date(today.getTime());
      latest.setUTCFullYear(latest.getUTCFullYear() + 5);
      query.set("expiration_date_gte", today.toISOString().slice(0, 10));
      query.set("expiration_date_lte", latest.toISOString().slice(0, 10));
    }
    const type = String(input.type || "").toLowerCase();
    if (type === "call" || type === "put") query.set("type", type);
    const contracts: Array<Record<string, unknown>> = [];
    let pageToken = "";
    const seenPageTokens = new Set<string>();
    do {
      if (pageToken) query.set("page_token", pageToken);
      const payload = await this.request("/v2/options/contracts", { trading: true, query });
      contracts.push(...(Array.isArray(payload.option_contracts) ? payload.option_contracts as Array<Record<string, unknown>> : []));
      const nextPageToken = String(payload.next_page_token || "");
      if (nextPageToken && seenPageTokens.has(nextPageToken)) {
        throw new AlpacaError("upstream", "Alpaca returned a repeated options pagination token.", 502);
      }
      if (nextPageToken) seenPageTokens.add(nextPageToken);
      pageToken = nextPageToken;
    } while (pageToken && contracts.length < maxRows);
    return contracts.slice(0, maxRows);
  }

  async listOptionExpirations(underlyingValue: string): Promise<string[]> {
    const contracts = await this.listOptionContracts({ underlying: underlyingValue, limit: 10000 });
    const expirations = [...new Set(contracts.map((contract) => String(contract.expiration_date || "")).filter(Boolean))].sort();
    if (!expirations.length) throw new AlpacaError("no_data", "No active options expirations were found for this underlying.", 404);
    return expirations;
  }

  async getOptionChain(input: { underlying: string; expiration: string; type?: string; feed?: string }): Promise<OptionContract[]> {
    const underlying = normalizeSymbol(input.underlying);
    const expiration = String(input.expiration || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(expiration)) throw new AlpacaError("invalid_request", "Select a valid expiration date.", 400);
    const type = String(input.type || "").toLowerCase();
    const contracts = await this.listOptionContracts({ underlying, expiration, type, limit: 10000 });
    if (!contracts.length) throw new AlpacaError("no_data", "No matching option contracts were found.", 404);
    const feed = OPTION_FEEDS.has(String(input.feed || "").toLowerCase()) ? String(input.feed).toLowerCase() : "indicative";
    const query = new URLSearchParams({ feed, limit: "1000" });
    const snapshots = new Map<string, Record<string, unknown>>();
    let pageToken = "";
    do {
      if (pageToken) query.set("page_token", pageToken);
      const payload = await this.request(`/v1beta1/options/snapshots/${encodeURIComponent(underlying)}`, { query });
      const raw = payload.snapshots && typeof payload.snapshots === "object" ? payload.snapshots as Record<string, Record<string, unknown>> : {};
      Object.entries(raw).forEach(([symbol, snapshot]) => snapshots.set(symbol, snapshot));
      pageToken = String(payload.next_page_token || "");
    } while (pageToken);
    return contracts.map((contract) => {
      const symbol = String(contract.symbol || "");
      const snapshot = snapshots.get(symbol) || {};
      const quote = (snapshot.latestQuote || snapshot.latest_quote || {}) as Record<string, unknown>;
      const trade = (snapshot.latestTrade || snapshot.latest_trade || {}) as Record<string, unknown>;
      const greeks = (snapshot.greeks || {}) as Record<string, unknown>;
      return {
        symbol,
        underlying,
        expiration: String(contract.expiration_date || expiration),
        strike: requiredNumber(contract.strike_price),
        type: String(contract.type || "call").toLowerCase() === "put" ? ("put" as const) : ("call" as const),
        bid: finiteOrNull(quote.bp ?? quote.bid_price),
        ask: finiteOrNull(quote.ap ?? quote.ask_price),
        last: finiteOrNull(trade.p ?? trade.price),
        volume: finiteOrNull(snapshot.dailyBar && (snapshot.dailyBar as Record<string, unknown>).v),
        openInterest: finiteOrNull(snapshot.openInterest ?? snapshot.open_interest),
        impliedVolatility: finiteOrNull(snapshot.impliedVolatility ?? snapshot.implied_volatility),
        delta: finiteOrNull(greeks.delta),
        gamma: finiteOrNull(greeks.gamma),
        theta: finiteOrNull(greeks.theta),
        vega: finiteOrNull(greeks.vega),
      };
    }).sort((left, right) => left.strike - right.strike || left.symbol.localeCompare(right.symbol));
  }

  async getOptionBars(input: OptionHistoryInput): Promise<{ contractSymbol: string; timeframe: string; feed: string; rows: AlpacaBar[] }> {
    const contractSymbol = normalizeSymbol(input.contractSymbol, true);
    const { start, end } = normalizeRange(input.start, input.end);
    const timeframe = normalizeTimeframe(input.timeframe);
    const requestedRows = Number(input.limit);
    const maxRows = requestedRows === 0 ? Number.POSITIVE_INFINITY : Math.max(1, Math.min(requestedRows || 2000, 50000));
    const rows: AlpacaBar[] = [];
    let pageToken = "";
    do {
      const query = new URLSearchParams({
        symbols: contractSymbol,
        timeframe,
        start,
        end,
        limit: String(Math.min(10000, maxRows - rows.length)),
        sort: "desc",
      });
      if (pageToken) query.set("page_token", pageToken);
      const payload = await this.request("/v1beta1/options/bars", { query });
      const barsRoot = payload.bars && typeof payload.bars === "object" ? payload.bars as Record<string, unknown> : {};
      const rawBars = Array.isArray(barsRoot[contractSymbol]) ? barsRoot[contractSymbol] as Array<Record<string, unknown>> : [];
      rows.push(...rawBars.map(mapBar));
      pageToken = String(payload.next_page_token || "");
    } while (pageToken && rows.length < maxRows);
    const sorted = rows.filter((row) => row.timestamp).sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp)).slice(0, maxRows);
    if (!sorted.length) throw new AlpacaError("no_data", "No historical observations are available for this contract and date range.", 404);
    // Alpaca chooses the entitled options feed for historical bars. Unlike the
    // snapshots endpoint, /v1beta1/options/bars does not accept a feed query.
    return { contractSymbol, timeframe, feed: "entitlement-default", rows: sorted };
  }
}

export function publicAlpacaError(error: unknown): { status: number; body: { ok: false; error: AlpacaErrorCode; message: string } } {
  if (error instanceof AlpacaError) {
    return { status: error.status, body: { ok: false, error: error.code, message: error.message } };
  }
  return { status: 500, body: { ok: false, error: "upstream", message: "The market-data request failed unexpectedly." } };
}
