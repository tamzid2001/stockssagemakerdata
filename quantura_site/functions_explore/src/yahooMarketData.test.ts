import assert from "node:assert/strict";
import test from "node:test";
import { parseYahooChartResponse, YahooFinanceClient } from "./yahooMarketData";

function payload() {
  return {
    chart: {
      result: [{
        timestamp: [1787581800, 1787585400],
        indicators: {
          quote: [{
            open: [100, 102],
            high: [103, 105],
            low: [99, 101],
            close: [102, 104],
            volume: [1000, 1200],
          }],
          adjclose: [{ adjclose: [51, 52] }],
        },
      }],
    },
  };
}

test("Yahoo chart normalization is chronological and preserves unavailable provider-only fields", () => {
  const rows = parseYahooChartResponse(payload(), { adjustment: "raw", session: "extended", limit: 500 });
  assert.equal(rows.length, 2);
  assert.equal(rows[0].close, 102);
  assert.equal(rows[0].tradeCount, null);
  assert.equal(rows[0].vwap, null);
  assert.ok(rows[0].timestamp < rows[1].timestamp);
});

test("Yahoo adjusted history scales OHLC consistently", () => {
  const rows = parseYahooChartResponse(payload(), { adjustment: "all", session: "extended", limit: 500 });
  assert.equal(rows[0].open, 50);
  assert.equal(rows[0].close, 51);
});

test("Yahoo client maps supported timeframes and returns the requested source schema", async () => {
  const client = new YahooFinanceClient({
    fetchImpl: async () => new Response(JSON.stringify(payload()), { status: 200, headers: { "content-type": "application/json" } }),
  });
  const result = await client.getStockBars({ symbol: "PLTR", timeframe: "1Day", limit: 1 });
  assert.equal(result.symbol, "PLTR");
  assert.equal(result.timeframe, "1Day");
  assert.equal(result.feed, "yahoo");
  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0].close, 104);
});

test("Yahoo client classifies missing symbols without exposing upstream bodies", async () => {
  const client = new YahooFinanceClient({ fetchImpl: async () => new Response("not found", { status: 404 }) });
  await assert.rejects(() => client.getStockBars({ symbol: "ZZZZ", timeframe: "1Day" }), /could not find/i);
});

test("Yahoo options expirations and chains normalize genuine provider fields without inventing Greeks", async () => {
  const optionPayload = {
    optionChain: {
      result: [{
        expirationDates: [1787875200, 1788480000],
        options: [{
          calls: [{
            contractSymbol: "PLTR260828C00180000",
            strike: 180,
            bid: 4.5,
            ask: 4.8,
            lastPrice: 4.6,
            volume: 120,
            openInterest: 900,
            impliedVolatility: 0.42,
          }],
          puts: [],
        }],
      }],
    },
  };
  const client = new YahooFinanceClient({
    fetchImpl: async () => new Response(JSON.stringify(optionPayload), { status: 200, headers: { "content-type": "application/json" } }),
  });
  const expirations = await client.listOptionExpirations("PLTR");
  assert.equal(expirations.length, 2);
  const contracts = await client.getOptionChain({ underlying: "PLTR", expiration: expirations[0], type: "call" });
  assert.equal(contracts[0].symbol, "PLTR260828C00180000");
  assert.equal(contracts[0].strike, 180);
  assert.equal(contracts[0].delta, null);
  assert.equal(contracts[0].openInterest, 900);
});

test("Yahoo option history accepts OCC contract symbols and returns chronological bars", async () => {
  const client = new YahooFinanceClient({
    fetchImpl: async (url) => {
      assert.match(String(url), /PLTR260828C00180000/);
      return new Response(JSON.stringify(payload()), { status: 200, headers: { "content-type": "application/json" } });
    },
  });
  const result = await client.getOptionBars({
    contractSymbol: "PLTR260828C00180000",
    start: "2026-08-20",
    end: "2026-08-25",
    timeframe: "1Day",
    limit: 500,
  });
  assert.equal(result.contractSymbol, "PLTR260828C00180000");
  assert.equal(result.feed, "yahoo");
  assert.equal(result.rows.length, 2);
});
