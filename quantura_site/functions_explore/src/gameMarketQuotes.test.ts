import assert from "node:assert/strict";
import test from "node:test";
import {gameMarketPrices, gameMarketUrl} from "./gameMarketQuotes";

test("original game URLs use only validated native provider identifiers and selected side", () => {
  const url = new URL(gameMarketUrl({provider: "kalshi", symbol: "KXMLBGAME-26SEP261915CHCBOS-CHC", event_id: "KXMLBGAME-26SEP261915CHCBOS", side: "no"})!);
  assert.equal(url.hostname, "kalshi.com"); assert.equal(url.searchParams.get("op_order_side"), "no");
  assert.equal(gameMarketUrl({provider: "polymarket_us", symbol: "aec-mlb-chc-bos-2026-09-26"}), "https://polymarket.us/sports/mlb/aec-mlb-chc-bos-2026-09-26");
  assert.equal(gameMarketUrl({provider: "kalshi", symbol: "https://evil.test", event_id: "EVENT"}), null);
});

test("quotes use independent actual contracts, repeated slug parameters, and preserve unavailable sides", async t => {
  const requests: URL[] = [];
  t.mock.method(globalThis, "fetch", async (input: any) => {
    const url = new URL(input); requests.push(url);
    return {ok: true, json: async () => ({markets: url.hostname.includes("kalshi") ? [
      {ticker: "GAME-ONE", status: "active", yes_ask_dollars: ".63", no_ask_dollars: ".38", last_price_dollars: ".61"},
      {ticker: "GAME-TWO", status: "active", last_price_dollars: ".4"},
      {ticker: "FOREIGN", status: "active", yes_ask_dollars: ".9"},
    ] : [
      {slug: "mlb-chc-bos", marketSides: [{id: "101", long: true, price: ".635"}, {id: "102", long: false, price: ".37"}]},
      {slug: "mlb-nyy-bos", marketSides: [{id: "103", long: true, price: "0"}, {id: "104", long: false, price: null}]},
    ]})} as any;
  });
  const rows = [
    {id: "k1y", provider: "kalshi", symbol: "GAME-ONE", side: "yes"},
    {id: "k1n", provider: "kalshi", symbol: "GAME-ONE", side: "no"},
    {id: "k2y", provider: "kalshi", symbol: "GAME-TWO", side: "yes"},
    {id: "k2n", provider: "kalshi", symbol: "GAME-TWO", side: "no"},
    {id: "p1", provider: "polymarket_us", symbol: "mlb-chc-bos", side: "long", contract_id: "101"},
    {id: "p2", provider: "polymarket_us", symbol: "mlb-chc-bos", side: "short", contract_id: "102"},
    {id: "p3", provider: "polymarket_us", symbol: "mlb-nyy-bos", side: "long", contract_id: "103"},
    {id: "p4", provider: "polymarket_us", symbol: "mlb-nyy-bos", side: "short", contract_id: "104"},
    {id: "wrong-side", provider: "polymarket_us", symbol: "mlb-chc-bos", side: "short", contract_id: "101"},
  ];
  const prices = await gameMarketPrices(rows);
  assert.deepEqual(prices.map(r => r.latest_price), [.63, .38, .4, null, .635, .37, 0, null, null]);
  assert.equal(prices[2].price_kind, "last_trade"); assert.equal(prices[3].price_kind, null);
  assert.deepEqual(requests.find(url => url.hostname.includes("polymarket"))!.searchParams.getAll("slug"), ["mlb-chc-bos", "mlb-nyy-bos"]);
  assert.ok(prices[0].price_checked_at);
});
