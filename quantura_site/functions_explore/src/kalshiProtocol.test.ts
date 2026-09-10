import assert from "node:assert/strict";
import test from "node:test";
import { kalshiMilestoneStart, kalshiPrice } from "./kalshiProtocol";
import { kalshiSeriesEvents, kalshiStatus, normalizeKalshiEvent, resolveKalshiContract } from "./predictionMarketData";
import { downloadKalshiMinuteHistory } from "./kalshiMinuteHistory";

test("Kalshi units are field-based: one cent, one dollar, subcent and malformed prices", () => {
  assert.equal(kalshiPrice({ close: 1 }, "close"), .01);
  assert.equal(kalshiPrice({ close_dollars: "1.0000", close: 1 }, "close"), 1);
  assert.equal(kalshiPrice({ close_dollars: "0.0001" }, "close"), .0001);
  assert.equal(kalshiPrice({ close_dollars: "1.2000", close: 50 }, "close"), null);
  for (const value of [null, "", false, "NaN", -1, 101]) assert.equal(kalshiPrice({ close: value }, "close"), null);
});

test("Kalshi pregame cutoffs require an unambiguous linked milestone, not expiration/occurrence", () => {
  assert.equal(kalshiMilestoneStart("E", [{ primary_event_tickers: ["E"], start_date: "2026-09-08T19:00:00Z" }]), "2026-09-08T19:00:00.000Z");
  assert.equal(kalshiMilestoneStart("OTHER", [{ primary_event_tickers: ["E"], start_date: "2026-09-08T19:00:00Z" }]), null);
  assert.equal(kalshiMilestoneStart("E", [19,20].map(hour => ({ related_event_tickers: ["E"], start_date: `2026-09-08T${hour}:00:00Z` }))), null);
  const contract = normalizeKalshiEvent({ event: { event_ticker: "E" }, markets: [{ ticker: "M", occurrence_datetime: "2026-09-08T22:00:00Z", expected_expiration_time: "2026-09-08T23:00:00Z", close_time: "2026-09-08T23:00:00Z", liquidity_dollars: "0.0000" }] }, "Sports")[0];
  assert.equal(contract.eventStart, null); assert.equal(contract.liquidity, null);
  assert.equal(kalshiStatus("disputed"), "closed"); assert.equal(kalshiStatus("determined"), "closed"); assert.equal(kalshiStatus("finalized"), "settled");
});

test("Kalshi event discovery follows cursors, carries milestones, and joins archived markets", async () => {
  const original = global.fetch; const urls: URL[] = [];
  global.fetch = async input => {
    const url = new URL(String(input)); urls.push(url);
    if (url.pathname.endsWith("/historical/markets")) return Response.json({ markets: [{ ticker: "OLD", event_ticker: "E1" }], cursor: "" });
    if (url.searchParams.get("cursor")) return Response.json({ events: [{ event_ticker: "E2", markets: [] }], cursor: "" });
    return Response.json({ events: [{ event_ticker: "E1", markets: [] }], milestones: [{ primary_event_tickers: ["E1"], start_date: "2026-09-08T19:00:00Z" }], cursor: "second" });
  };
  try {
    const result = await kalshiSeriesEvents("TEST-ARCHIVE", "settled");
    assert.equal(result.events.length, 2); assert.equal(result.limited, false);
    assert.equal((result.events[0].markets as any[])[0].ticker, "OLD");
    assert.equal((result.events[0].milestones as any[]).length, 1);
    assert.equal(urls[1].searchParams.get("cursor"), "second");
    assert.equal(urls[0].searchParams.get("with_milestones"), "true");
  } finally { global.fetch = original; }
});

test("Kalshi repeated discovery cursors fail explicitly", async () => {
  const original = global.fetch;
  global.fetch = async () => Response.json({ events: [], cursor: "repeated" });
  try { await assert.rejects(kalshiSeriesEvents("TEST-CURSOR", "open"), /repeated page cursor/); }
  finally { global.fetch = original; }
});

test("Kalshi verifies contract metadata server-side and falls back to historical market lookup", async () => {
  const original = global.fetch; const paths: string[] = [];
  global.fetch = async input => {
    const path = new URL(String(input)).pathname; paths.push(path);
    if (path.endsWith("/historical/markets/VERIFIED")) return Response.json({ market: { ticker: "VERIFIED", event_ticker: "REAL", market_type: "binary", last_price_dollars: "0.4200" } });
    if (path.endsWith("/markets/VERIFIED")) return Response.json({}, { status: 404 });
    if (path.endsWith("/milestones")) return Response.json({ milestones: [{ related_event_tickers: ["REAL"], start_date: "2026-09-08T19:00:00Z" }] });
    return Response.json({ event: { event_ticker: "REAL", title: "Provider event", category: "Sports" } });
  };
  try {
    const verified = await resolveKalshiContract({ providerSymbol: "VERIFIED", side: "yes", eventStart: "2099-01-01", eventId: "FORGED" } as any);
    assert.equal(verified.eventId, "REAL"); assert.equal(verified.eventStart, "2026-09-08T19:00:00.000Z"); assert.equal(verified.currentPrice, .42);
    assert.ok(paths.some(p => p.includes("/historical/markets/")));
  } finally { global.fetch = original; }
});

test("minute CSV preserves fixed-point quantities and normalizes legacy cents", async () => {
  const result = await downloadKalshiMinuteHistory({ ticker: "TEST", startTime: "2026-09-08T12:00:00Z", endTime: "2026-09-08T12:00:00Z" }, { fetchImpl: async () => Response.json({ markets: [{ market_ticker: "TEST", candlesticks: [{ end_period_ts: Date.parse("2026-09-08T12:00:00Z")/1000, price: { close: 1 }, yes_bid: { close_dollars: "0.0001" }, yes_ask: { close_dollars: "0.0200" }, volume_fp: "1.25", open_interest_fp: "20.50" }] }] }) });
  assert.equal(result.rows[0].priceClose, .01); assert.equal(result.rows[0].volume, 1.25);
  assert.match(result.csvText, /0\.0001/); assert.equal(result.rowCount, 1);
});
