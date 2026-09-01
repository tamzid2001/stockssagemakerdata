import { AlpacaBar, AlpacaError, OptionContract, classifyEquitySession } from "./alpacaClient";

type FetchLike = typeof fetch;

type YahooOptionSession = {
  cookie: string;
  crumb: string;
  expiresAt: number;
};

export type YahooStockHistoryInput = {
  symbol: unknown;
  start?: unknown;
  end?: unknown;
  timeframe?: unknown;
  adjustment?: unknown;
  session?: unknown;
  limit?: number;
};

const INTERVALS = new Map([
  ["1min", { canonical: "1Min", yahoo: "1m", range: "7d" }],
  ["1m", { canonical: "1Min", yahoo: "1m", range: "7d" }],
  ["5min", { canonical: "5Min", yahoo: "5m", range: "60d" }],
  ["5m", { canonical: "5Min", yahoo: "5m", range: "60d" }],
  ["15min", { canonical: "15Min", yahoo: "15m", range: "60d" }],
  ["15m", { canonical: "15Min", yahoo: "15m", range: "60d" }],
  ["30min", { canonical: "30Min", yahoo: "30m", range: "60d" }],
  ["30m", { canonical: "30Min", yahoo: "30m", range: "60d" }],
  ["1hour", { canonical: "1Hour", yahoo: "60m", range: "730d" }],
  ["1h", { canonical: "1Hour", yahoo: "60m", range: "730d" }],
  ["1day", { canonical: "1Day", yahoo: "1d", range: "max" }],
  ["1d", { canonical: "1Day", yahoo: "1d", range: "max" }],
]);

const YAHOO_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36";
const YAHOO_OPTION_SESSION_TTL_MS = 15 * 60 * 1000;

function responseCookies(response: Response): string[] {
  const headers = response.headers as Headers & { getSetCookie?: () => string[] };
  const values = typeof headers.getSetCookie === "function"
    ? headers.getSetCookie()
    : [headers.get("set-cookie") || ""].filter(Boolean);
  return values.flatMap((value) => {
    const match = String(value).match(/^\s*([^=;,\s]+=[^;,]+)/);
    return match ? [match[1].trim()] : [];
  });
}

function cookieHeader(values: string[]): string {
  const cookies = new Map<string, string>();
  for (const value of values) {
    const separator = value.indexOf("=");
    if (separator <= 0) continue;
    cookies.set(value.slice(0, separator), value);
  }
  return [...cookies.values()].join("; ");
}

function symbol(value: unknown, optionContract = false): string {
  const result = String(value || "").trim().toUpperCase();
  const pattern = optionContract ? /^[A-Z0-9.\-]{10,32}$/ : /^(?:\^[A-Z0-9.\-]{1,23}|[A-Z0-9][A-Z0-9.^=\-]{0,23})$/;
  if (!pattern.test(result)) {
    throw new AlpacaError("invalid_request", optionContract ? "Select a valid options contract." : "Enter a valid ticker symbol.", 400);
  }
  return result;
}

function interval(value: unknown): { canonical: string; yahoo: string; range: string } {
  const result = INTERVALS.get(String(value || "1Day").trim().toLowerCase());
  if (!result) {
    throw new AlpacaError("invalid_request", "Choose a supported timeframe: 1, 5, 15, or 30 minutes; 1 hour; or 1 day.", 400);
  }
  return result;
}

function unixSeconds(value: unknown): number | null {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const parsed = Date.parse(raw);
  if (!Number.isFinite(parsed)) throw new AlpacaError("invalid_request", "Choose a valid date range.", 400);
  return Math.floor(parsed / 1000);
}

function finite(value: unknown): number | null {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function adjustedOhlc(raw: { open: number; high: number; low: number; close: number }, adjustedClose: number | null, adjustment: string) {
  if (adjustment === "raw" || adjustedClose === null || raw.close === 0) return raw;
  const factor = adjustedClose / raw.close;
  return {
    open: raw.open * factor,
    high: raw.high * factor,
    low: raw.low * factor,
    close: adjustedClose,
  };
}

export function parseYahooChartResponse(
  payload: Record<string, unknown>,
  options: { adjustment?: string; session?: string; limit?: number } = {}
): AlpacaBar[] {
  const chart = payload.chart && typeof payload.chart === "object" ? (payload.chart as Record<string, unknown>) : {};
  const results = Array.isArray(chart.result) ? chart.result : [];
  const result = results[0] && typeof results[0] === "object" ? (results[0] as Record<string, unknown>) : {};
  const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
  const indicators = result.indicators && typeof result.indicators === "object" ? (result.indicators as Record<string, unknown>) : {};
  const quotes = Array.isArray(indicators.quote) ? indicators.quote : [];
  const quote = quotes[0] && typeof quotes[0] === "object" ? (quotes[0] as Record<string, unknown>) : {};
  const adjustedRows = Array.isArray(indicators.adjclose) ? indicators.adjclose : [];
  const adjusted = adjustedRows[0] && typeof adjustedRows[0] === "object" ? (adjustedRows[0] as Record<string, unknown>) : {};
  const opens = Array.isArray(quote.open) ? quote.open : [];
  const highs = Array.isArray(quote.high) ? quote.high : [];
  const lows = Array.isArray(quote.low) ? quote.low : [];
  const closes = Array.isArray(quote.close) ? quote.close : [];
  const volumes = Array.isArray(quote.volume) ? quote.volume : [];
  const adjustedCloses = Array.isArray(adjusted.adjclose) ? adjusted.adjclose : [];
  const adjustment = String(options.adjustment || "raw").toLowerCase();
  const regularOnly = String(options.session || "extended").toLowerCase() === "regular";

  const rows = timestamps.flatMap((rawTimestamp, index) => {
    const timestampSeconds = finite(rawTimestamp);
    const open = finite(opens[index]);
    const high = finite(highs[index]);
    const low = finite(lows[index]);
    const close = finite(closes[index]);
    if (timestampSeconds === null || open === null || high === null || low === null || close === null) return [];
    const timestamp = new Date(timestampSeconds * 1000).toISOString();
    const marketSession = classifyEquitySession(timestamp);
    if (regularOnly && marketSession !== "regular") return [];
    const prices = adjustedOhlc({ open, high, low, close }, finite(adjustedCloses[index]), adjustment);
    return [{
      timestamp,
      open: prices.open,
      high: prices.high,
      low: prices.low,
      close: prices.close,
      volume: finite(volumes[index]) ?? 0,
      tradeCount: null,
      vwap: null,
      session: marketSession,
    } satisfies AlpacaBar];
  }).sort((left, right) => left.timestamp.localeCompare(right.timestamp));

  const requested = Number(options.limit);
  return requested > 0 ? rows.slice(-Math.min(requested, 50000)) : rows;
}

export class YahooFinanceClient {
  private readonly fetchImpl: FetchLike;
  private optionSession: YahooOptionSession | null = null;
  private optionSessionPromise: Promise<YahooOptionSession> | null = null;

  constructor(options: { fetchImpl?: FetchLike } = {}) {
    this.fetchImpl = options.fetchImpl || fetch;
  }

  private async requestJson(url: string, extraHeaders: Record<string, string> = {}): Promise<Record<string, unknown>> {
    let response: Response;
    try {
      response = await this.fetchImpl(url, {
        headers: { Accept: "application/json", "User-Agent": YAHOO_USER_AGENT, ...extraHeaders },
        signal: AbortSignal.timeout(25000),
      });
    } catch {
      throw new AlpacaError("network", "Yahoo Finance market data could not be reached. Try again.", 502);
    }
    if (response.status === 404) throw new AlpacaError("unsupported_symbol", "Yahoo Finance could not find this symbol.", 404);
    if (response.status === 429) throw new AlpacaError("rate_limit", "Yahoo Finance rate-limited this request. Wait briefly and retry.", 429);
    if (response.status === 401 || response.status === 403) {
      throw new AlpacaError("authentication", "Yahoo Finance requires a refreshed provider session.", 502);
    }
    if (!response.ok) throw new AlpacaError("upstream", "Yahoo Finance could not complete the request.", 502);
    const payload = await response.json().catch(() => null) as Record<string, unknown> | null;
    if (!payload) throw new AlpacaError("upstream", "Yahoo Finance returned an unreadable response.", 502);
    return payload;
  }

  private async createOptionSession(force = false): Promise<YahooOptionSession> {
    if (!force && this.optionSession && this.optionSession.expiresAt > Date.now()) return this.optionSession;
    if (!force && this.optionSessionPromise) return this.optionSessionPromise;

    const pending = (async () => {
      let bootstrap: Response;
      try {
        bootstrap = await this.fetchImpl("https://fc.yahoo.com", {
          headers: { Accept: "text/html", "User-Agent": YAHOO_USER_AGENT },
          redirect: "follow",
          signal: AbortSignal.timeout(15000),
        });
      } catch {
        throw new AlpacaError("network", "Yahoo Finance options data could not establish a provider session.", 502);
      }
      const bootstrapCookies = responseCookies(bootstrap);
      const initialCookie = cookieHeader(bootstrapCookies);
      if (!initialCookie) throw new AlpacaError("upstream", "Yahoo Finance options data could not establish a provider session.", 502);

      let crumbResponse: Response;
      try {
        crumbResponse = await this.fetchImpl("https://query1.finance.yahoo.com/v1/test/getcrumb", {
          headers: { Accept: "text/plain", Cookie: initialCookie, "User-Agent": YAHOO_USER_AGENT },
          signal: AbortSignal.timeout(15000),
        });
      } catch {
        throw new AlpacaError("network", "Yahoo Finance options data could not refresh its provider session.", 502);
      }
      if (crumbResponse.status === 429) throw new AlpacaError("rate_limit", "Yahoo Finance rate-limited this request. Wait briefly and retry.", 429);
      if (!crumbResponse.ok) throw new AlpacaError("upstream", "Yahoo Finance options data could not refresh its provider session.", 502);
      const crumb = (await crumbResponse.text()).trim();
      if (!crumb || crumb.length > 200 || /\s/.test(crumb) || /too many requests/i.test(crumb)) {
        throw new AlpacaError("upstream", "Yahoo Finance returned an invalid options session.", 502);
      }
      const cookie = cookieHeader([...bootstrapCookies, ...responseCookies(crumbResponse)]);
      const session = { cookie, crumb, expiresAt: Date.now() + YAHOO_OPTION_SESSION_TTL_MS };
      this.optionSession = session;
      return session;
    })();

    this.optionSessionPromise = pending;
    try {
      return await pending;
    } finally {
      if (this.optionSessionPromise === pending) this.optionSessionPromise = null;
    }
  }

  private async requestOptionJson(url: string): Promise<Record<string, unknown>> {
    try {
      return await this.requestJson(url);
    } catch (error) {
      if (!(error instanceof AlpacaError) || error.code !== "authentication") throw error;
    }

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const session = await this.createOptionSession(attempt > 0);
      const authenticatedUrl = new URL(url);
      authenticatedUrl.searchParams.set("crumb", session.crumb);
      try {
        return await this.requestJson(authenticatedUrl.toString(), { Cookie: session.cookie });
      } catch (error) {
        if (!(error instanceof AlpacaError) || error.code !== "authentication" || attempt > 0) throw error;
        this.optionSession = null;
      }
    }
    throw new AlpacaError("upstream", "Yahoo Finance options data could not refresh its provider session.", 502);
  }

  private optionResult(payload: Record<string, unknown>): Record<string, unknown> {
    const root = payload.optionChain && typeof payload.optionChain === "object" ? payload.optionChain as Record<string, unknown> : {};
    const results = Array.isArray(root.result) ? root.result : [];
    const result = results[0] && typeof results[0] === "object" ? results[0] as Record<string, unknown> : {};
    if (!Object.keys(result).length) throw new AlpacaError("no_data", "Yahoo Finance returned no options data for this underlying.", 404);
    return result;
  }

  async listOptionExpirations(underlyingValue: unknown): Promise<string[]> {
    const underlying = symbol(underlyingValue);
    const payload = await this.requestOptionJson(`https://query1.finance.yahoo.com/v7/finance/options/${encodeURIComponent(underlying)}`);
    const result = this.optionResult(payload);
    const expirations = (Array.isArray(result.expirationDates) ? result.expirationDates : [])
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value) && value > 0)
      .map((value) => new Date(value * 1000).toISOString().slice(0, 10));
    const unique = [...new Set(expirations)].sort();
    if (!unique.length) throw new AlpacaError("no_data", "No active options expirations were found for this underlying.", 404);
    return unique;
  }

  async getOptionChain(input: { underlying: unknown; expiration: unknown; type?: unknown }): Promise<OptionContract[]> {
    const underlying = symbol(input.underlying);
    const expiration = String(input.expiration || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(expiration)) throw new AlpacaError("invalid_request", "Select a valid expiration date.", 400);
    const expirationSeconds = Math.floor(Date.parse(`${expiration}T00:00:00.000Z`) / 1000);
    const payload = await this.requestOptionJson(`https://query1.finance.yahoo.com/v7/finance/options/${encodeURIComponent(underlying)}?date=${expirationSeconds}`);
    const result = this.optionResult(payload);
    const optionSets = Array.isArray(result.options) ? result.options : [];
    const optionSet = optionSets[0] && typeof optionSets[0] === "object" ? optionSets[0] as Record<string, unknown> : {};
    const requestedType = String(input.type || "call").toLowerCase() === "put" ? "put" : "call";
    const rawContracts = Array.isArray(optionSet[requestedType === "put" ? "puts" : "calls"])
      ? optionSet[requestedType === "put" ? "puts" : "calls"] as Array<Record<string, unknown>>
      : [];
    const contracts = rawContracts.flatMap((contract) => {
      const contractSymbol = String(contract.contractSymbol || "").trim().toUpperCase();
      const strike = finite(contract.strike);
      if (!contractSymbol || strike === null) return [];
      return [{
        symbol: contractSymbol,
        underlying,
        expiration,
        strike,
        type: requestedType,
        bid: finite(contract.bid),
        ask: finite(contract.ask),
        last: finite(contract.lastPrice),
        volume: finite(contract.volume),
        openInterest: finite(contract.openInterest),
        impliedVolatility: finite(contract.impliedVolatility),
        delta: null,
        gamma: null,
        theta: null,
        vega: null,
      } satisfies OptionContract];
    }).sort((left, right) => left.strike - right.strike || left.symbol.localeCompare(right.symbol));
    if (!contracts.length) throw new AlpacaError("no_data", "No matching Yahoo Finance option contracts were found.", 404);
    return contracts;
  }

  async getOptionBars(input: { contractSymbol: unknown; start?: unknown; end?: unknown; timeframe?: unknown; limit?: number }): Promise<{
    contractSymbol: string;
    timeframe: string;
    feed: string;
    rows: AlpacaBar[];
  }> {
    const contractSymbol = symbol(input.contractSymbol, true);
    const result = await this.getStockBars({
      symbol: contractSymbol,
      start: input.start,
      end: input.end,
      timeframe: input.timeframe,
      adjustment: "raw",
      session: "regular",
      limit: input.limit,
    }, true);
    return { contractSymbol, timeframe: result.timeframe, feed: "yahoo", rows: result.rows };
  }

  async getStockBars(input: YahooStockHistoryInput, optionContract = false): Promise<{
    symbol: string;
    timeframe: string;
    feed: string;
    adjustment: string;
    session: string;
    rows: AlpacaBar[];
  }> {
    const ticker = symbol(input.symbol, optionContract);
    const timeframe = interval(input.timeframe);
    const start = unixSeconds(input.start);
    const end = unixSeconds(input.end);
    if (start !== null && end !== null && start >= end) {
      throw new AlpacaError("invalid_request", "End date must be after start date.", 400);
    }
    const query = new URLSearchParams({
      interval: timeframe.yahoo,
      events: "history",
      includeAdjustedClose: "true",
      includePrePost: String(input.session || "extended").toLowerCase() === "regular" ? "false" : "true",
    });
    if (start !== null) {
      query.set("period1", String(start));
      query.set("period2", String(end ?? Math.floor(Date.now() / 1000)));
    } else {
      query.set("range", timeframe.range);
    }
    const payload = await this.requestJson(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?${query.toString()}`);
    const rows = parseYahooChartResponse(payload, {
      adjustment: String(input.adjustment || "raw"),
      session: String(input.session || "extended"),
      limit: Number(input.limit),
    });
    if (!rows.length) throw new AlpacaError("no_data", "Yahoo Finance returned no observations for this ticker and timeframe.", 404);
    return {
      symbol: ticker,
      timeframe: timeframe.canonical,
      feed: "yahoo",
      adjustment: String(input.adjustment || "raw").toLowerCase(),
      session: String(input.session || "extended").toLowerCase() === "regular" ? "regular" : "extended",
      rows,
    };
  }
}
