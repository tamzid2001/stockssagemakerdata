import assert from "node:assert/strict";
import test from "node:test";
import {gameHistory, observedGameHistory} from "./gameHistory";
const fixture = (id: string, provider = "polymarket_us") => ({id, provider, symbol: "mlb-chc-bos-2026-09-27", contract_id: "123", side: "long",
  input_cutoff: "2026-09-27T18:00:00Z", forecast_end: "2026-09-27T23:05:00Z", game_start: "2026-09-27T19:05:00Z"});

test("saved inputs reject future, filled, nonnumeric and unordered prices", () => {
  const item = fixture("saved"), observation = {timestamp: "2026-09-27T17:00:00Z", price: .42};
  assert.deepEqual(observedGameHistory({...item, observations: [observation]}), [{...observation,timestamp:"2026-09-27T17:00:00.000Z"}]);
  for (const patch of [{timestamp: "2026-09-27T19:00:00Z"}, {is_forward_filled: true}, {observed: false}, {price: null}, {price: 1.1}]) {
    assert.deepEqual(observedGameHistory({...item, observations: [{...observation, ...patch}]}), []);
  }
  assert.deepEqual(observedGameHistory({...item, observations: [observation, observation]}), []);
});

test("Polymarket legacy history aligns hourly ends and overlays completed outcomes without filling gaps", async t => {
  t.mock.method(Date, "now", () => Date.parse("2026-09-27T20:30:00Z"));
  let request: any;
  const load: any = async (body: any) => {request = body; return {rows: [
    {timestamp: "2026-09-27T16:00:00Z", price: .3},
    {timestamp: "2026-09-27T17:00:00Z", price: .4},
    {timestamp: "2026-09-27T18:00:00Z", price: .5, is_forward_filled: true},
    {timestamp: "2026-09-27T19:00:00Z", price: .6},
    {timestamp: "2026-09-27T20:00:00Z", price: .7}, // incomplete hour
  ]};};
  const result = await gameHistory(fixture("legacy-poly"), load);
  assert.equal(request.start, "2026-09-17T18:00:00.000Z");
  assert.equal(request.end, "2026-09-27T20:00:00.000Z");
  assert.equal(request.history_phase, "both"); assert.equal(request.missing, "leave"); assert.equal(request.frequency, "1h");
  assert.deepEqual(result.observations.map(r => [r.timestamp, r.price]), [
    ["2026-09-27T17:00:00.000Z", .3], ["2026-09-27T18:00:00.000Z", .4], ["2026-09-27T20:00:00.000Z", .6],
  ]);
});

test("Kalshi overlay keeps exact saved inputs and uses actual ask closes without complementing prices", async t => {
  t.mock.method(Date, "now", () => Date.parse("2026-09-28T01:00:00Z"));
  let request: any;
  const load: any = async (body: any) => {request = body; return {rows: [
    {timestamp: "2026-09-27T18:00:00Z", ask: .99, price: .8},
    {timestamp: "2026-09-27T19:00:00Z", ask: .47, price: .8},
    {timestamp: "2026-09-27T20:00:00Z", ask: null, price: .8},
    {timestamp: "2026-09-28T00:00:00Z", ask: .9},
  ]};};
  const result = await gameHistory({...fixture("saved-kalshi", "kalshi"), observations: [{timestamp: "2026-09-27T18:00:00Z", price: .4}]}, load);
  assert.equal(request.start, "2026-09-27T18:00:00.000Z");
  assert.equal(request.end, "2026-09-27T23:05:00.000Z");
  assert.deepEqual(result.observations.map(r => r.price), [.4, .47]);
  assert.equal(result.history_source, "saved_model_input_and_provider_outcomes");
});

test("saved inputs are available before a subsequent hour completes without a provider request", async t => {
  t.mock.method(Date, "now", () => Date.parse("2026-09-27T18:30:00Z"));
  const result = await gameHistory({...fixture("before-next-hour"), observations: [{timestamp: "2026-09-27T18:00:00Z", price: .4}]}, (() => {throw Error("must not fetch");}) as any);
  assert.equal(result.observations.length, 1); assert.equal(result.history_source, "saved_model_input");
});
