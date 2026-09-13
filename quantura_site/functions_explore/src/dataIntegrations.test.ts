import assert from "node:assert/strict";
import test from "node:test";
import { AlpacaClient, AlpacaError, barsToCsv, classifyEquitySession } from "./alpacaClient";
import { buildMlbMinuteRows, discoverMlbMarkets, encodePriceHistoryRequest, fetchPolymarketPricePoints } from "./polymarketMlb";
import {
  buildPredictionMarketDataset,
  kalshiAuthHeaders,
  normalizeKalshiEvent,
  normalizePolymarketEvents,
  normalizeProbability,
  predictionDatasetCsv,
  resamplePredictionObservations,
  polymarketHistory,
  stableItemId,
  type NormalizedPredictionObservation,
  type PredictionMarketSource,
} from "./predictionMarketData";
import { historySelection, eventHistoryRange, quoteHistoryQuality } from "./eventHistory";

test("event history phase and bounded lookback intersect before cutoff; unknown start fails closed", () => {
  const start=Date.parse("2026-09-12T10:00Z"), event=start+4*3600000, end=event+2*3600000;
  assert.deepEqual(eventHistoryRange(start,end,event,historySelection({history_phase:"both"})),{start,end});
  assert.deepEqual(eventHistoryRange(start,end,event,historySelection({history_phase:"pregame",history_lookback_minutes:60})),{start:event-3600000,end:event});
  assert.deepEqual(eventHistoryRange(start,end,event,historySelection({history_phase:"in_game",history_lookback_minutes:60})),{start:end-3600000,end});
  assert.deepEqual(eventHistoryRange(start,event+30*60000,event,historySelection({history_phase:"in_game",history_lookback_minutes:60})),{start:event,end:event+30*60000});
  assert.equal(historySelection({pregameOnly:true}).history_phase,"pregame");
  assert.throws(()=>eventHistoryRange(start,end,NaN,historySelection({history_phase:"in_game"})),/event_start/);
  for(const input of [{history_phase:"unknown"},{history_lookback_minutes:-1},{history_lookback_minutes:NaN},{history_lookback_minutes:"60"},{history_lookback_minutes:129601}]) assert.throws(()=>historySelection(input));
});

test("flat history is preserved but blocks inference; changing in-game data remains usable", () => {
  const start=Date.parse("2026-09-12T10:00Z");
  const rows=Array.from({length:500},(_,i)=>({timestamp:new Date(start+i*60000).toISOString(),target:.545}));
  assert.equal(quoteHistoryQuality(rows).forecast_blocked,true);
  const moving=rows.map((r,i)=>({...r,target:i>=450?.545+(i-450)/1000:r.target}));
  const q=quoteHistoryQuality(moving,start+400*60000);
  assert.equal(q.forecast_blocked,false);assert.equal(q.observations,500);assert.equal(q.price_changes,49);assert.equal(q.pregame_observations,401);
  moving[0].target=.5;
  const stale=moving.map((r,i)=>({...r,target:i>100?.55:r.target}));assert.equal(quoteHistoryQuality(stale).forecast_blocked,true);
});

test("Polymarket explicitly downloads pregame and in-game raw quotes without filling gaps", async () => {
  const original=global.fetch;const start=Date.parse("2026-09-12T10:00Z"), event=start+3600000, end=event+3600000;const ranges:number[][]=[];
  global.fetch=(async input=>{const u=new URL(String(input));const from=Number(u.searchParams.get("timestamp.startTimestamp")),to=Number(u.searchParams.get("timestamp.endTimestamp"));ranges.push([from,to]);
    return Response.json({history:[{timestamp:from+15,longPrice:.45,shortPrice:.56},{timestamp:from+180,longPrice:.46,shortPrice:.55}]});}) as typeof fetch;
  try {
    const contract={source:"polymarket_us",providerSymbol:"fixture-game-phases",contractId:"short-id",side:"short",eventStart:new Date(event).toISOString()} as Parameters<typeof polymarketHistory>[0];
    const rows=await polymarketHistory(contract,start,end);assert.deepEqual(ranges,[[start/1000,event/1000],[event/1000,end/1000]]);
    assert.equal(rows.length,4);assert.deepEqual(rows.map(r=>r.price),[.56,.55,.56,.55]);assert.equal(rows[0].raw.history_phase,"pregame");assert.equal(rows[2].raw.history_phase,"in_game");assert.ok(rows.every(r=>!r.is_forward_filled));
  } finally {global.fetch=original;}
});

test("Kalshi hourly aggregation does not move candle observations to an earlier timestamp", () => {
  const row=predictionObservation("kalshi","fixture:yes","2026-08-20T10:15:00Z",{price:.55});row.raw.end_period_ts=Date.parse(row.timestamp)/1000;
  const hourly=resamplePredictionObservations([row],"1h","leave");assert.equal(hourly[0].timestamp,"2026-08-20T11:00:00.000Z");
});
import { userFromRequest, validateAwsIntegration } from "./awsIntegration";

function withAlpacaEnvironment(): void {
  process.env.ALPACA_API_KEY = ["unit", "test", "key"].join("-");
  process.env.ALPACA_SECRET_KEY = ["unit", "test", "credential"].join("-");
  process.env.ALPACA_BASE_URL = "https://paper.example.test";
  process.env.ALPACA_DATA_URL = "https://data.example.test";
}

test("Polymarket official one-minute history splits 24h ranges and preserves spread", async () => {
  const urls: URL[] = [];
  const start=Date.parse("2026-09-01T00:00:00Z");
  const points=await fetchPolymarketPricePoints("sports-fixture",start,start+2*86400000,1,(async input=>{
    const url=new URL(String(input));urls.push(url);
    assert.equal(url.pathname,"/v1/price-history");
    assert.equal(url.searchParams.get("fidelity"),"1");
    const from=Number(url.searchParams.get("timestamp.startTimestamp"));
    const end=Number(url.searchParams.get("timestamp.endTimestamp"));
    assert.equal(end-from,86400);
    return Response.json({history:[{timestamp:from+10,longPrice:.6,shortPrice:.43},{timestamp:from+10,longPrice:.6,shortPrice:.43}]});
  }) as typeof fetch);
  assert.equal(urls.length,2);assert.equal(points.length,2);
  assert.equal(points[0].longPrice+points[0].shortPrice,1.03);
  await assert.rejects(()=>fetchPolymarketPricePoints("../private",start,start+1000));
  await assert.rejects(()=>fetchPolymarketPricePoints("fixture",start,start+1000,5));
});

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

test("Alpaca daily regular-session bars retain midnight timestamps and 500 observations", async () => {
  withAlpacaEnvironment();
  const client = new AlpacaClient({ fetchImpl: (async () => Response.json({ bars: Array.from({length:500}, (_,i) => ({ t:new Date(Date.UTC(2024,0,1+i,5)).toISOString(), o:1,h:2,l:1,c:2,v:1 })) })) as typeof fetch });
  const result = await client.getStockBars({symbol:"PLTR",timeframe:"1Day",session:"regular",limit:500,start:"2024-01-01",end:"2026-09-11"});
  assert.equal(result.rows.length,500);
});

test("Alpaca filters intraday extended bars before counting the requested limit", async () => {
  withAlpacaEnvironment();let calls=0;
  const client = new AlpacaClient({fetchImpl: (async () => { calls++;return Response.json({bars:[{t: calls===1 ? "2026-09-10T05:00:00Z":"2026-09-10T15:00:00Z",o:1,h:1,l:1,c:1,v:1}],next_page_token:calls===1?"second":null}); }) as typeof fetch});
  const result=await client.getStockBars({symbol:"PLTR",timeframe:"1Min",session:"regular",limit:1,start:"2026-09-10",end:"2026-09-11"});
  assert.equal(calls,2);assert.equal(result.rows.length,1);assert.equal(result.rows[0].session,"regular");
});

test("Alpaca session classification distinguishes extended-hours observations", () => {
  assert.equal(classifyEquitySession("2026-08-21T12:00:00Z"), "premarket");
  assert.equal(classifyEquitySession("2026-08-21T15:00:00Z"), "regular");
  assert.equal(classifyEquitySession("2026-08-21T21:00:00Z"), "after_hours");
});

test("Alpaca latest-price lookup batches tickers and uses completed one-minute bar closes", async () => {
  withAlpacaEnvironment();
  let requestedUrl = "";
  const client = new AlpacaClient({ fetchImpl: (async (input: string | URL | Request) => {
    requestedUrl = String(input);
    return Response.json({ bars: {
      AAPL: { t: "2026-08-24T14:59:00Z", c: 201.25 },
      MSFT: { t: "2026-08-24T14:59:00Z", c: 512.5 },
    } });
  }) as typeof fetch });
  const prices = await client.getLatestStockPrices(["aapl", "MSFT", "AAPL"], "iex");
  assert.match(requestedUrl, /\/v2\/stocks\/bars\/latest/);
  assert.match(requestedUrl, /symbols=AAPL%2CMSFT/);
  assert.equal(prices.get("AAPL")?.price, 201.25);
  assert.equal(prices.get("MSFT")?.session, "regular");
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

test("Alpaca option expirations override the provider's upcoming-weekend default and follow pagination", async () => {
  withAlpacaEnvironment();
  const requestedUrls: string[] = [];
  const client = new AlpacaClient({ fetchImpl: (async (input: string | URL | Request) => {
    const url = String(input);
    requestedUrls.push(url);
    const parsed = new URL(url);
    if (!parsed.searchParams.get("page_token")) {
      return Response.json({
        option_contracts: [
          { symbol: "AAPL260904C00200000", expiration_date: "2026-09-04" },
          { symbol: "AAPL260918C00200000", expiration_date: "2026-09-18" },
        ],
        next_page_token: "next-expirations",
      });
    }
    return Response.json({
      option_contracts: [
        { symbol: "AAPL261016C00200000", expiration_date: "2026-10-16" },
        { symbol: "AAPL260918P00200000", expiration_date: "2026-09-18" },
      ],
      next_page_token: null,
    });
  }) as typeof fetch });

  const expirations = await client.listOptionExpirations("AAPL");
  assert.deepEqual(expirations, ["2026-09-04", "2026-09-18", "2026-10-16"]);
  assert.equal(requestedUrls.length, 2);
  const first = new URL(requestedUrls[0]);
  assert.ok(first.searchParams.get("expiration_date_gte"));
  assert.ok(first.searchParams.get("expiration_date_lte"));
  assert.equal(first.searchParams.get("limit"), "10000");
  assert.equal(new URL(requestedUrls[1]).searchParams.get("page_token"), "next-expirations");
});

test("Alpaca option history uses the supported bars contract without a snapshot feed parameter", async () => {
  withAlpacaEnvironment();
  let requestedUrl = "";
  const client = new AlpacaClient({ fetchImpl: (async (input: string | URL | Request) => {
    requestedUrl = String(input);
    return Response.json({
      bars: {
        AAPL260918C00200000: [
          { t: "2026-08-24T14:31:00Z", o: 11.9, h: 12.4, l: 11.8, c: 12.2, v: 150, n: 18, vw: 12.1 },
        ],
      },
      next_page_token: null,
    });
  }) as typeof fetch });
  const result = await client.getOptionBars({
    contractSymbol: "AAPL260918C00200000",
    timeframe: "1Min",
    start: "2026-08-24",
    end: "2026-08-25",
    feed: "indicative",
    limit: 500,
  });
  const parsed = new URL(requestedUrl);
  assert.equal(parsed.pathname, "/v1beta1/options/bars");
  assert.equal(parsed.searchParams.get("symbols"), "AAPL260918C00200000");
  assert.equal(parsed.searchParams.has("feed"), false);
  assert.equal(result.feed, "entitlement-default");
  assert.equal(result.rows[0].close, 12.2);
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

function predictionObservation(
  source: PredictionMarketSource,
  contractId: string,
  timestamp: string,
  values: Partial<NormalizedPredictionObservation> = {}
): NormalizedPredictionObservation {
  const eventStart = values.event_start === undefined ? "2026-08-20T12:00:00.000Z" : values.event_start;
  const bid = values.bid === undefined ? 0.48 : values.bid;
  const ask = values.ask === undefined ? 0.52 : values.ask;
  return {
    source,
    sport: "Baseball",
    league: "MLB",
    event_id: `${contractId}-event`,
    market_id: `${contractId}-market`,
    contract_id: contractId,
    item_id: stableItemId(source, contractId),
    market_title: "Representative sports contract",
    outcome: "Home team",
    event_start: eventStart,
    timestamp,
    price: values.price === undefined ? 0.5 : values.price,
    bid,
    ask,
    midpoint: bid !== null && ask !== null ? (bid + ask) / 2 : null,
    last_trade: values.last_trade === undefined ? 0.5 : values.last_trade,
    spread: bid !== null && ask !== null ? ask - bid : null,
    spread_pct: bid !== null && ask !== null && bid + ask > 0 ? ((ask - bid) / ((bid + ask) / 2)) * 100 : null,
    volume: values.volume === undefined ? 10 : values.volume,
    open_interest: values.open_interest === undefined ? 50 : values.open_interest,
    liquidity: values.liquidity === undefined ? 100 : values.liquidity,
    minutes_to_event: (Date.parse(eventStart || timestamp) - Date.parse(timestamp)) / 60000,
    seconds_to_event: (Date.parse(eventStart || timestamp) - Date.parse(timestamp)) / 1000,
    status: "open",
    is_forward_filled: values.is_forward_filled || false,
    raw: values.raw || { provider_price: values.price === undefined ? 0.5 : values.price },
  };
}

test("Prediction providers normalize cents and decimals into one probability unit", () => {
  assert.equal(normalizeProbability(53), 0.53);
  assert.equal(normalizeProbability("0.53"), 0.53);
  assert.equal(normalizeProbability(101), null);
  assert.equal(normalizeProbability(-0.01), null);
});

test("Polymarket US discovery produces selectable provider-neutral contracts", () => {
  const contracts = normalizePolymarketEvents({ events: [{
    id: "event-1", title: "New York at Boston", startTime: "2026-08-20T12:00:00Z",
    markets: [{ id: "market-1", slug: "mlb-ny-bos", title: "Moneyline", marketSides: [
      { id: "new-york", long: true, description: "New York", quote: { value: 0.57 } },
      { id: "boston", long: false, description: "Boston", quote: { value: 0.43 } },
    ] }],
  }] }, { id: "mlb", label: "MLB", providerId: "1", sport: "Baseball" });
  assert.equal(contracts.length, 2);
  assert.equal(contracts[0].source, "polymarket_us");
  assert.equal(contracts[0].eventStart, "2026-08-20T12:00:00.000Z");
  assert.equal(contracts[1].side, "short");
});

test("Kalshi discovery creates YES and NO contracts with correctly complemented quotes", () => {
  const contracts = normalizeKalshiEvent({
    event: { event_ticker: "KXMLB-26AUG20", title: "New York at Boston", series_ticker: "KXMLBGAME" },
    milestones: [{ related_event_tickers: ["KXMLB-26AUG20"], start_date: "2026-08-20T23:00:00Z" }],
    markets: [{ ticker: "KXMLB-26AUG20-NY", title: "New York wins", status: "active", occurrence_datetime: "2026-08-20T23:00:00Z", last_price: 61, yes_bid: 60, yes_ask: 62, volume_fp: "120", open_interest_fp: "75" }],
  }, "Baseball");
  assert.equal(contracts.length, 2);
  assert.equal(contracts[0].currentPrice, 0.61);
  assert.equal(contracts[1].currentPrice, 0.39);
  assert.equal(contracts[1].bid, 0.38);
  assert.equal(contracts[1].ask, 0.4);
  assert.equal(contracts[0].eventStart, "2026-08-20T23:00:00.000Z");
});

test("Prediction time-series processing deduplicates, orders, resamples, and explicitly forward-fills", () => {
  const rows = [
    predictionObservation("kalshi", "market:yes", "2026-08-20T10:02:35Z", { price: 0.55, volume: 4 }),
    predictionObservation("kalshi", "market:yes", "2026-08-20T10:00:30Z", { price: 0.5, volume: 2 }),
    predictionObservation("kalshi", "market:yes", "2026-08-20T10:00:30Z", { price: 0.51, volume: 3 }),
  ];
  const sampled = resamplePredictionObservations(rows, "1m", "forward_fill");
  assert.deepEqual(sampled.map((row) => row.timestamp), ["2026-08-20T10:00:00.000Z", "2026-08-20T10:01:00.000Z", "2026-08-20T10:02:00.000Z"]);
  assert.deepEqual(sampled.map((row) => row.price), [0.51, 0.51, 0.55]);
  assert.equal(sampled[1].is_forward_filled, true);
  assert.equal(sampled[1].volume, null);
});

test("Pregame Canvas export excludes post-start observations and validates cross-provider rows", () => {
  const rows = [
    predictionObservation("polymarket_us", "pm-contract", "2026-08-20T11:58:00Z", { price: 0.48 }),
    predictionObservation("polymarket_us", "pm-contract", "2026-08-20T12:00:00Z", { price: 0.51 }),
    predictionObservation("kalshi", "kalshi-contract:yes", "2026-08-20T11:59:00Z", { price: 0.62 }),
  ];
  const dataset = buildPredictionMarketDataset(rows, {
    source: "polymarket_us",
    mode: "canvas",
    frequency: "raw",
    target: "price",
    missing: "leave",
    pregameOnly: true,
    features: ["source", "sport", "minutes_to_event", "bid", "ask", "spread"],
  });
  assert.equal(dataset.rows.length, 2);
  assert.equal(dataset.validation.postStartRowsRemoved, 1);
  assert.equal(dataset.validation.timezone, "UTC");
  assert.equal(dataset.validation.targetRange.min, 0.48);
  assert.equal(dataset.validation.targetRange.max, 0.62);
  assert.match(String(dataset.rows[0].item_id), /^(kalshi|polymarket_us):/);
  assert.deepEqual(dataset.headers.slice(0, 3), ["item_id", "timestamp", "target"]);
});

test("Normalized and raw prediction exports preserve supported fields without credentials", () => {
  const row = predictionObservation("kalshi", "market:yes", "2026-08-20T10:00:00Z", { bid: 0.45, ask: 0.5, raw: { trade_id: "trade-1", yes_price: 0.48 } });
  const normalized = buildPredictionMarketDataset([row], { source: "kalshi", mode: "normalized", frequency: "raw", target: "price", missing: "leave", pregameOnly: false });
  assert.ok(normalized.headers.includes("item_id"));
  assert.equal(normalized.rows[0].spread, 0.04999999999999999);
  const raw = buildPredictionMarketDataset([row], { source: "kalshi", mode: "raw", frequency: "raw", target: "price", missing: "leave", pregameOnly: false });
  const csv = predictionDatasetCsv(raw);
  assert.match(csv, /^source,event_id,market_id,contract_id,outcome,timestamp,/);
  assert.match(csv, /trade-1/);
  assert.doesNotMatch(csv.toLowerCase(), /private.key|access.key|secret/);
});

test("Stable IDs cannot collide across providers and Kalshi signing is absent without server credentials", () => {
  assert.notEqual(stableItemId("polymarket_us", "same-contract"), stableItemId("kalshi", "same-contract"));
  const previousId = process.env.KALSHI_API_KEY_ID;
  const previousAlias = process.env.KALSHI_PROD_API_KEY;
  const previousKey = process.env.KALSHI_PRIVATE_KEY;
  delete process.env.KALSHI_API_KEY_ID;
  delete process.env.KALSHI_PROD_API_KEY;
  delete process.env.KALSHI_PRIVATE_KEY;
  assert.equal(kalshiAuthHeaders("GET", "/trade-api/v2/api_keys"), null);
  if (previousId !== undefined) process.env.KALSHI_API_KEY_ID = previousId;
  if (previousAlias !== undefined) process.env.KALSHI_PROD_API_KEY = previousAlias;
  if (previousKey !== undefined) process.env.KALSHI_PRIVATE_KEY = previousKey;
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
