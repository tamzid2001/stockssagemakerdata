import assert from "node:assert/strict";
import test from "node:test";
import { AlpacaClient, AlpacaError, barsToCsv, classifyEquitySession } from "./alpacaClient";
import { buildMlbMinuteRows, discoverMlbMarkets, encodePriceHistoryRequest } from "./polymarketMlb";
import { userFromRequest, validateAwsIntegration } from "./awsIntegration";

function withAlpacaEnvironment(): void {
  process.env.ALPACA_API_KEY = ["unit", "test", "key"].join("-");
  process.env.ALPACA_SECRET_KEY = ["unit", "test", "credential"].join("-");
  process.env.ALPACA_BASE_URL = "https://paper.example.test";
  process.env.ALPACA_DATA_URL = "https://data.example.test";
}

test("Alpaca stock history preserves pagination, chronological order, and CSV fields", async () => {
  withAlpacaEnvironment();
  const urls: string[] = [];
  const mockFetch = async (input: string | URL | Request): Promise<Response> => {
    const url = String(input);
    urls.push(url);
    if (url.includes("page_token=next")) {
      return Response.json({ bars: [{ t: "2026-08-21T14:31:00Z", o: 101, h: 102, l: 100, c: 101.5, v: 900, n: 8, vw: 101.3 }], next_page_token: null });
    }
    return Response.json({ bars: [{ t: "2026-08-21T14:30:00Z", o: 100, h: 101, l: 99, c: 100.5, v: 1000, n: 10, vw: 100.3 }], next_page_token: "next" });
  };
  const client = new AlpacaClient({ fetchImpl: mockFetch as typeof fetch });
  const result = await client.getStockBars({ symbol: "aapl", timeframe: "1Min", start: "2026-08-21", end: "2026-08-22", feed: "iex", session: "regular", limit: 500 });
  assert.equal(urls.length, 2);
  assert.deepEqual(result.rows.map((row) => row.timestamp), ["2026-08-21T14:30:00Z", "2026-08-21T14:31:00Z"]);
  assert.equal(result.rows[0].session, "regular");
  const csv = barsToCsv(result.symbol, result.rows);
  assert.match(csv, /^symbol,timestamp,open,high,low,close,volume,trade_count,vwap,session\n/);
  assert.equal(csv.trim().split("\n").length, 3);
});

test("Alpaca all-history selection follows every provider page", async () => {
  withAlpacaEnvironment();
  let calls = 0;
  const mockFetch = async (): Promise<Response> => {
    calls += 1;
    return Response.json({
      bars: [{ t: `2026-08-${String(20 + calls).padStart(2, "0")}T14:30:00Z`, o: calls, h: calls, l: calls, c: calls, v: calls }],
      next_page_token: calls < 3 ? `page-${calls}` : null,
    });
  };
  const result = await new AlpacaClient({ fetchImpl: mockFetch as typeof fetch }).getStockBars({
    symbol: "MSFT", timeframe: "1Day", start: "2026-08-01", end: "2026-08-30", limit: 0,
  });
  assert.equal(calls, 3);
  assert.equal(result.rows.length, 3);
});

test("Alpaca session classification distinguishes extended-hours observations", () => {
  assert.equal(classifyEquitySession("2026-08-21T12:00:00Z"), "premarket");
  assert.equal(classifyEquitySession("2026-08-21T15:00:00Z"), "regular");
  assert.equal(classifyEquitySession("2026-08-21T21:00:00Z"), "after_hours");
});

test("Alpaca errors classify authentication without exposing provider payloads", async () => {
  withAlpacaEnvironment();
  const client = new AlpacaClient({ fetchImpl: (async () => new Response(JSON.stringify({ message: "raw secret diagnostic" }), { status: 401, headers: { "content-type": "application/json" } })) as typeof fetch });
  await assert.rejects(
    () => client.getStockBars({ symbol: "AAPL", timeframe: "1Day", start: "2026-08-01", end: "2026-08-20" }),
    (error: unknown) => error instanceof AlpacaError && error.code === "authentication" && !error.message.includes("raw secret diagnostic")
  );
});

test("Alpaca options chain joins contracts to supported snapshot fields", async () => {
  withAlpacaEnvironment();
  const mockFetch = async (input: string | URL | Request): Promise<Response> => {
    const url = String(input);
    if (url.startsWith("https://paper.example.test/v2/options/contracts")) {
      return Response.json({ option_contracts: [{ symbol: "AAPL260918C00200000", underlying_symbol: "AAPL", expiration_date: "2026-09-18", strike_price: "200", type: "call" }], next_page_token: null });
    }
    return Response.json({ snapshots: { AAPL260918C00200000: { latestQuote: { bp: 12.1, ap: 12.4 }, latestTrade: { p: 12.2 }, dailyBar: { v: 150 }, openInterest: 900, impliedVolatility: 0.31, greeks: { delta: 0.55, gamma: 0.03, theta: -0.08, vega: 0.19 } } }, next_page_token: null });
  };
  const chain = await new AlpacaClient({ fetchImpl: mockFetch as typeof fetch }).getOptionChain({ underlying: "AAPL", expiration: "2026-09-18", type: "call", feed: "indicative" });
  assert.equal(chain.length, 1);
  assert.deepEqual(chain[0], { symbol: "AAPL260918C00200000", underlying: "AAPL", expiration: "2026-09-18", strike: 200, type: "call", bid: 12.1, ask: 12.4, last: 12.2, volume: 150, openInterest: 900, impliedVolatility: 0.31, delta: 0.55, gamma: 0.03, theta: -0.08, vega: 0.19 });
});

test("MLB discovery and minute normalization preserve the downloader schema semantics", () => {
  const payload = { events: [{ ticker: "mlb-test", title: "New York at Boston", createdAt: "2026-08-20T10:00:00Z", startTime: "2026-08-20T12:05:00Z", markets: [{ id: "market-1", slug: "aec-mlb-test", sportsMarketType: "baseball_team_full_game_moneyline", createdAt: "2026-08-20T10:00:00Z", gameStartTime: "2026-08-20T12:05:00Z", marketSides: [{ id: "ny", long: true, team: { name: "New York" } }, { id: "bos", long: false, team: { name: "Boston" } }] }] }] };
  const [market] = discoverMlbMarkets(payload, new Date("2026-08-20T11:00:00Z"));
  assert.equal(market.status, "upcoming");
  const built = buildMlbMinuteRows([
    { timestamp: Date.parse("2026-08-20T10:01:10Z") / 1000, longPrice: 0.51, shortPrice: 0.49 },
    { timestamp: Date.parse("2026-08-20T10:03:20Z") / 1000, longPrice: 0.53, shortPrice: 0.47 },
  ], market.sides[0], market, Date.parse("2026-08-20T10:05:00Z"));
  assert.equal(built.observedMinutes, 2);
  assert.deepEqual(built.rows.map((row) => row.price), ["0.5100", "0.5100", "0.5300", "0.5300"]);
  assert.deepEqual(Object.keys(built.rows[0]), ["item_id", "datetime", "price", "minutes_before_start"]);
  assert.ok(encodePriceHistoryRequest(market.marketSlug, Date.parse(market.createdAt), Date.parse(market.gameStart)).length > 0);
});

test("AWS integration validation enforces same-account least-privilege role structure", () => {
  const config = validateAwsIntegration({ accountId: "123456789012", region: "us-east-1", roleArn: "arn:aws:iam::123456789012:role/QuanturaVercel", executionRoleArn: "arn:aws:iam::123456789012:role/QuanturaSageMaker", s3Bucket: "quantura-forecast-123456789012" });
  assert.equal(config.status, "not_tested");
  assert.throws(() => validateAwsIntegration({ ...config, executionRoleArn: "arn:aws:iam::999999999999:role/WrongAccount" }), /same|belong/i);
});

test("AWS integration authentication rejects anonymous Firebase sessions", async () => {
  const request = { headers: { authorization: "Bearer firebase-id-token" } };
  const anonymousAuth = { verifyIdToken: async () => ({ uid: "anonymous-user", firebase: { sign_in_provider: "anonymous" } }) };
  const passwordAuth = { verifyIdToken: async () => ({ uid: "private-user", firebase: { sign_in_provider: "password" } }) };
  await assert.rejects(
    () => userFromRequest(request as never, anonymousAuth as never),
    /unauthenticated/
  );
  const user = await userFromRequest(request as never, passwordAuth as never);
  assert.equal(user.uid, "private-user");
});
