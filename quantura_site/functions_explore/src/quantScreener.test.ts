import test from "node:test";
import assert from "node:assert/strict";
import {
  filterSortPaginateRows,
  parseQuantScreenerQuery,
  rowMatchesQuery,
  type QuantScreenerQuery,
  type QuantScreenerRow,
} from "./quantScreener";

const TODAY = new Date("2026-08-24T12:00:00Z");

function row(overrides: Partial<QuantScreenerRow> = {}): QuantScreenerRow {
  return {
    ticker: "PLTR",
    company_name: "Palantir Technologies Inc.",
    actual_price: 120,
    p10: 90,
    p50: 110,
    p90: 130,
    market_cap: 250_000_000_000,
    market_cap_bucket: "mega",
    next_earnings_date: "2026-08-31",
    is_sp500: true,
    is_nasdaq: true,
    is_etf: false,
    general_bias: "Buying Bias",
    p10_signal_active: true,
    distance_p10_pct: 33.33,
    distance_p50_pct: 9.09,
    distance_p90_pct: -7.69,
    last_forecast_update: "2026-08-24T22:00:00Z",
    status: "success",
    ...overrides,
  };
}

function query(overrides: Partial<QuantScreenerQuery> = {}): QuantScreenerQuery {
  return {
    search: "",
    universe: "all",
    marketCap: "all",
    minMarketCap: null,
    maxMarketCap: null,
    positions: [],
    bias: "all",
    earnings: "all",
    specialP10: false,
    page: 1,
    pageSize: 50,
    sort: "ticker",
    direction: "asc",
    ...overrides,
  };
}

test("search matches ticker", () => assert.equal(rowMatchesQuery(row(), query({ search: "PLTR" }), TODAY), true));
test("search matches company", () => assert.equal(rowMatchesQuery(row(), query({ search: "Palantir" }), TODAY), true));
test("S&P 500 universe membership is independent of Nasdaq membership", () => {
  assert.equal(rowMatchesQuery(row({ is_sp500: true, is_nasdaq: false }), query({ universe: "sp500" }), TODAY), true);
});
test("Nasdaq universe matches broad listed membership", () => {
  assert.equal(rowMatchesQuery(row({ is_sp500: false, is_nasdaq: true }), query({ universe: "nasdaq" }), TODAY), true);
});
test("ETF universe includes SPY without assigning a company cap bucket", () => {
  const spy = row({ ticker: "SPY", is_sp500: false, is_nasdaq: false, is_etf: true, market_cap: null, market_cap_bucket: null, next_earnings_date: "N/A — ETF" });
  assert.equal(rowMatchesQuery(spy, query({ universe: "etf" }), TODAY), true);
  assert.equal(rowMatchesQuery(spy, query({ marketCap: "mega" }), TODAY), false);
});
test("market-cap presets match documented buckets", () => {
  for (const cap of ["mega", "large", "mid", "small", "micro"] as const) {
    assert.equal(rowMatchesQuery(row({ market_cap_bucket: cap }), query({ marketCap: cap }), TODAY), true);
  }
});
test("custom market-cap bounds combine", () => {
  assert.equal(rowMatchesQuery(row({ market_cap: 5_000_000_000 }), query({ minMarketCap: 2_000_000_000, maxMarketCap: 10_000_000_000 }), TODAY), true);
  assert.equal(rowMatchesQuery(row({ market_cap: null }), query({ minMarketCap: 1 }), TODAY), false);
});
test("below P10", () => assert.equal(rowMatchesQuery(row({ actual_price: 80 }), query({ positions: ["below-p10"] }), TODAY), true));
test("above P10", () => assert.equal(rowMatchesQuery(row(), query({ positions: ["above-p10"] }), TODAY), true));
test("below P50", () => assert.equal(rowMatchesQuery(row({ actual_price: 100 }), query({ positions: ["below-p50"] }), TODAY), true));
test("above P50", () => assert.equal(rowMatchesQuery(row(), query({ positions: ["above-p50"] }), TODAY), true));
test("below P90", () => assert.equal(rowMatchesQuery(row(), query({ positions: ["below-p90"] }), TODAY), true));
test("above P90", () => assert.equal(rowMatchesQuery(row({ actual_price: 140 }), query({ positions: ["above-p90"] }), TODAY), true));
test("combined quantile range requires every condition", () => {
  const between = query({ positions: ["above-p50", "below-p90"] });
  assert.equal(rowMatchesQuery(row({ actual_price: 120 }), between, TODAY), true);
  assert.equal(rowMatchesQuery(row({ actual_price: 140 }), between, TODAY), false);
});
test("buying bias filter", () => assert.equal(rowMatchesQuery(row({ general_bias: "Buying Bias" }), query({ bias: "buying" }), TODAY), true));
test("selling bias filter", () => assert.equal(rowMatchesQuery(row({ general_bias: "Selling Bias" }), query({ bias: "selling" }), TODAY), true));
test("neutral bias filter", () => assert.equal(rowMatchesQuery(row({ general_bias: "Neutral / Mixed" }), query({ bias: "neutral" }), TODAY), true));
test("special P10 signal filter", () => {
  assert.equal(rowMatchesQuery(row({ p10_signal_active: true }), query({ specialP10: true }), TODAY), true);
  assert.equal(rowMatchesQuery(row({ p10_signal_active: false }), query({ specialP10: true }), TODAY), false);
});
test("earnings today", () => assert.equal(rowMatchesQuery(row({ next_earnings_date: "2026-08-24" }), query({ earnings: "today" }), TODAY), true));
test("earnings next 7 days", () => assert.equal(rowMatchesQuery(row({ next_earnings_date: "2026-08-31" }), query({ earnings: "7" }), TODAY), true));
test("earnings next 30 days", () => assert.equal(rowMatchesQuery(row({ next_earnings_date: "2026-09-23" }), query({ earnings: "30" }), TODAY), true));
test("missing earnings is filterable and SPY remains N/A", () => {
  assert.equal(rowMatchesQuery(row({ next_earnings_date: null }), query({ earnings: "unknown" }), TODAY), true);
  assert.equal(rowMatchesQuery(row({ ticker: "SPY", is_etf: true, next_earnings_date: "N/A — ETF" }), query({ earnings: "unknown" }), TODAY), true);
});
test("missing forecast fails quantile filters without hiding an unfiltered row", () => {
  const missing = row({ actual_price: null, p10: null, p50: null, p90: null, forecast_available: false });
  assert.equal(rowMatchesQuery(missing, query(), TODAY), true);
  assert.equal(rowMatchesQuery(missing, query({ positions: ["below-p10"] }), TODAY), false);
});
test("missing actual price fails quantile filters", () => {
  assert.equal(rowMatchesQuery(row({ actual_price: null }), query({ positions: ["above-p50"] }), TODAY), false);
});
test("pagination returns bounded pages", () => {
  const rows = Array.from({ length: 25 }, (_, index) => row({ ticker: `T${String(index).padStart(2, "0")}` }));
  const page = filterSortPaginateRows(rows, query({ page: 2, pageSize: 10 }), TODAY);
  assert.equal(page.items.length, 10);
  assert.equal(page.items[0].ticker, "T10");
  assert.equal(page.total, 25);
  assert.equal(page.pageCount, 3);
});
test("sorting supports quantile distances and keeps missing values last", () => {
  const page = filterSortPaginateRows(
    [row({ ticker: "A", distance_p50_pct: 1 }), row({ ticker: "B", distance_p50_pct: 8 }), row({ ticker: "C", distance_p50_pct: null })],
    query({ sort: "distanceP50", direction: "desc" }),
    TODAY
  );
  assert.deepEqual(page.items.map((item) => item.ticker), ["B", "A", "C"]);
});
test("sort by next earnings keeps unavailable dates last", () => {
  const page = filterSortPaginateRows(
    [row({ ticker: "B", next_earnings_date: null }), row({ ticker: "A", next_earnings_date: "2026-08-25" })],
    query({ sort: "nextEarnings" }),
    TODAY
  );
  assert.deepEqual(page.items.map((item) => item.ticker), ["A", "B"]);
});
test("provider/API query validation rejects unsupported values", () => {
  const parsed = parseQuantScreenerQuery({ universe: "dow", position: "inside-p50", sort: "magic" });
  assert.equal(parsed.errors.length, 3);
});
test("URL query parser preserves valid combined filters", () => {
  const parsed = parseQuantScreenerQuery({ universe: "sp500", position: "above-p50,below-p90", marketCap: "large", page: "3", pageSize: "25" });
  assert.deepEqual(parsed.errors, []);
  assert.deepEqual(parsed.query.positions, ["above-p50", "below-p90"]);
  assert.equal(parsed.query.page, 3);
});
