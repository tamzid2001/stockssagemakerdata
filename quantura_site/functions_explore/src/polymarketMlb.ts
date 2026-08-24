import { Router } from "express";

const SPORTS_EVENTS_URL = "https://gateway.polymarket.us/v2/leagues/mlb/events";
const CLOSED_EVENTS_URL = "https://gateway.polymarket.us/v1/events";
const PRICE_HISTORY_URL = "https://gateway.polymarket.us/gateway.price_history.v1.PriceHistoryService/GetPriceHistory";
const USER_AGENT = "quantura-polymarket-us-mlb-pregame-history/1.0";

type Side = { itemId: string; team: string; position: "long" | "short" };
export type MlbMarket = {
  eventTicker: string;
  eventTitle: string;
  marketId: string;
  marketSlug: string;
  createdAt: string;
  gameStart: string;
  status: "upcoming" | "previous";
  sides: [Side, Side];
};
type PricePoint = { timestamp: number; longPrice: number; shortPrice: number };
export type MlbMinuteRow = { item_id: string; datetime: string; price: string; minutes_before_start: number };

export class PolymarketMlbError extends Error {
  constructor(message: string, readonly status = 502) {
    super(message);
    this.name = "PolymarketMlbError";
  }
}

function parsedIso(value: unknown): string {
  const ms = Date.parse(String(value || ""));
  return Number.isFinite(ms) ? new Date(ms).toISOString() : "";
}

function marketFrom(event: Record<string, unknown>, raw: Record<string, unknown>, nowMs: number): MlbMarket | null {
  const marketType = String(raw.sportsMarketType || "");
  if (!["baseball_team_full_game_winner", "baseball_team_full_game_moneyline", "moneyline"].includes(marketType)) return null;
  const createdAt = parsedIso(raw.createdAt || event.createdAt);
  const gameStart = parsedIso(raw.gameStartTime || event.startTime);
  const marketSlug = String(raw.slug || "").trim();
  const marketId = String(raw.id || "").trim();
  if (!createdAt || !gameStart || !marketSlug || !marketId) return null;
  const rawSides = Array.isArray(raw.marketSides) ? raw.marketSides : [];
  if (rawSides.length !== 2) return null;
  const sides = rawSides.map((item) => {
    const side = (item || {}) as Record<string, unknown>;
    const teamObject = side.team && typeof side.team === "object" ? side.team as Record<string, unknown> : {};
    const team = String(teamObject.name || "").trim();
    const itemId = String(side.id || "").trim();
    if (!team || !itemId) return null;
    return { itemId, team, position: side.long === true ? ("long" as const) : ("short" as const) };
  });
  if (sides.some((side) => !side) || new Set(sides.map((side) => side?.position)).size !== 2) return null;
  return {
    eventTicker: String(event.ticker || event.slug || ""),
    eventTitle: String(event.title || raw.title || marketSlug),
    marketId,
    marketSlug,
    createdAt,
    gameStart,
    status: Date.parse(gameStart) >= nowMs ? "upcoming" : "previous",
    sides: sides as [Side, Side],
  };
}

export function discoverMlbMarkets(payload: Record<string, unknown>, now = new Date()): MlbMarket[] {
  const markets: MlbMarket[] = [];
  const events = Array.isArray(payload.events) ? payload.events : [];
  events.forEach((rawEvent) => {
    const event = (rawEvent || {}) as Record<string, unknown>;
    const eventMarkets = Array.isArray(event.markets) ? event.markets : [];
    eventMarkets.forEach((rawMarket) => {
      const market = marketFrom(event, (rawMarket || {}) as Record<string, unknown>, now.getTime());
      if (market) markets.push(market);
    });
  });
  const unique = new Map(markets.map((market) => [market.marketSlug, market]));
  return [...unique.values()].sort((left, right) => {
    if (left.status !== right.status) return left.status === "upcoming" ? -1 : 1;
    const delta = Date.parse(left.gameStart) - Date.parse(right.gameStart);
    return left.status === "upcoming" ? delta : -delta;
  });
}

async function fetchJson(url: string, query: URLSearchParams): Promise<Record<string, unknown>> {
  let response: Response;
  try {
    response = await fetch(`${url}?${query.toString()}`, { headers: { "User-Agent": USER_AGENT, Accept: "application/json" }, signal: AbortSignal.timeout(25000) });
  } catch (_error) {
    throw new PolymarketMlbError("Polymarket US could not be reached. Try again.");
  }
  if (!response.ok) {
    if (response.status === 429) throw new PolymarketMlbError("Polymarket US rate-limited this request. Wait briefly and retry.", 429);
    throw new PolymarketMlbError("Polymarket US could not load MLB markets.", response.status >= 500 ? 502 : response.status);
  }
  return await response.json() as Record<string, unknown>;
}

export async function listMlbMarkets(scope = "any"): Promise<MlbMarket[]> {
  const events: unknown[] = [];
  if (scope !== "previous") {
    const upcoming = await fetchJson(SPORTS_EVENTS_URL, new URLSearchParams({ limit: "1000", active: "true", closed: "false" }));
    if (Array.isArray(upcoming.events)) events.push(...upcoming.events);
  }
  if (scope !== "upcoming") {
    const previous = await fetchJson(CLOSED_EVENTS_URL, new URLSearchParams({ limit: "250", offset: "0", closed: "true", tagIds: "4" }));
    if (Array.isArray(previous.events)) events.push(...previous.events);
  }
  const markets = discoverMlbMarkets({ events });
  const scoped = markets.filter((market) => scope === "any" || market.status === scope);
  return scoped.slice(0, 100);
}

function encodeVarint(input: number): Buffer {
  let value = Math.max(0, Math.floor(input));
  const bytes: number[] = [];
  while (value > 0x7f) {
    bytes.push((value & 0x7f) | 0x80);
    value = Math.floor(value / 128);
  }
  bytes.push(value);
  return Buffer.from(bytes);
}

function stringField(number: number, value: string): Buffer {
  const raw = Buffer.from(value, "utf8");
  return Buffer.concat([encodeVarint((number << 3) | 2), encodeVarint(raw.length), raw]);
}

function integerField(number: number, value: number): Buffer {
  return Buffer.concat([encodeVarint(number << 3), encodeVarint(value)]);
}

function messageField(number: number, value: Buffer): Buffer {
  return Buffer.concat([encodeVarint((number << 3) | 2), encodeVarint(value.length), value]);
}

export function encodePriceHistoryRequest(symbol: string, startMs: number, endMs: number): Buffer {
  if (!symbol || !Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) throw new PolymarketMlbError("The market history range is invalid.", 400);
  const interval = Buffer.concat([integerField(1, startMs / 1000), integerField(2, endMs / 1000)]);
  return Buffer.concat([stringField(1, symbol), messageField(2, interval), integerField(4, 1)]);
}

function readVarint(buffer: Buffer, initialOffset: number): [number, number] {
  let result = 0;
  let shift = 0;
  let offset = initialOffset;
  while (offset < buffer.length && shift < 53) {
    const value = buffer[offset++];
    result += (value & 0x7f) * 2 ** shift;
    if (value < 0x80) return [result, offset];
    shift += 7;
  }
  throw new PolymarketMlbError("Polymarket US returned malformed price history.");
}

type WireField = { number: number; type: number; value: number | Buffer };
function wireFields(buffer: Buffer): WireField[] {
  const fields: WireField[] = [];
  let offset = 0;
  while (offset < buffer.length) {
    let tag: number;
    [tag, offset] = readVarint(buffer, offset);
    const number = tag >> 3;
    const type = tag & 7;
    if (!number) throw new PolymarketMlbError("Polymarket US returned malformed price history.");
    if (type === 0) {
      let value: number;
      [value, offset] = readVarint(buffer, offset);
      fields.push({ number, type, value });
    } else if (type === 2) {
      let length: number;
      [length, offset] = readVarint(buffer, offset);
      const end = offset + length;
      if (end > buffer.length) throw new PolymarketMlbError("Polymarket US returned truncated price history.");
      fields.push({ number, type, value: buffer.subarray(offset, end) });
      offset = end;
    } else if (type === 5) {
      const end = offset + 4;
      if (end > buffer.length) throw new PolymarketMlbError("Polymarket US returned truncated price history.");
      fields.push({ number, type, value: buffer.subarray(offset, end) });
      offset = end;
    } else if (type === 1) {
      const end = offset + 8;
      if (end > buffer.length) throw new PolymarketMlbError("Polymarket US returned truncated price history.");
      fields.push({ number, type, value: buffer.subarray(offset, end) });
      offset = end;
    } else {
      throw new PolymarketMlbError("Polymarket US returned an unsupported price-history field.");
    }
  }
  return fields;
}

export function decodePriceHistoryResponse(buffer: Buffer): PricePoint[] {
  const points: PricePoint[] = [];
  wireFields(buffer).filter((field) => field.number === 1 && field.type === 2).forEach((field) => {
    const values = new Map(wireFields(field.value as Buffer).map((child) => [child.number, child]));
    const timestamp = values.get(1);
    const long = values.get(2);
    const short = values.get(3);
    if (!timestamp || timestamp.type !== 0 || !long || long.type !== 5 || !short || short.type !== 5) return;
    const longPrice = Number((long.value as Buffer).readFloatLE(0).toFixed(4));
    const shortPrice = Number((short.value as Buffer).readFloatLE(0).toFixed(4));
    if (longPrice < 0 || longPrice > 1 || shortPrice < 0 || shortPrice > 1) return;
    points.push({ timestamp: Number(timestamp.value), longPrice, shortPrice });
  });
  return points.sort((left, right) => left.timestamp - right.timestamp);
}

export function buildMlbMinuteRows(points: PricePoint[], side: Side, market: MlbMarket, endMs: number): { rows: MlbMinuteRow[]; observedMinutes: number } {
  const hardEnd = Math.min(endMs, Date.parse(market.gameStart));
  const lastCompleteMinute = Math.floor(hardEnd / 60000) * 60 - 60;
  const closes = new Map<number, number>();
  const createdSeconds = Date.parse(market.createdAt) / 1000;
  const gameStartSeconds = Date.parse(market.gameStart) / 1000;
  points.forEach((point) => {
    if (point.timestamp < createdSeconds || point.timestamp >= gameStartSeconds) return;
    const minute = Math.floor(point.timestamp / 60) * 60;
    const price = side.position === "long" ? point.longPrice : point.shortPrice;
    if (minute <= lastCompleteMinute && price >= 0.01 && price <= 0.99) closes.set(minute, price);
  });
  if (!closes.size) return { rows: [], observedMinutes: 0 };
  const rows: MlbMinuteRow[] = [];
  let previous: number | null = null;
  const firstMinute = Math.min(...closes.keys());
  for (let minute = firstMinute; minute <= lastCompleteMinute; minute += 60) {
    if (closes.has(minute)) previous = closes.get(minute) as number;
    if (previous === null) continue;
    rows.push({
      item_id: side.itemId,
      datetime: new Date(minute * 1000).toISOString().replace(".000Z", "Z"),
      price: previous.toFixed(4),
      minutes_before_start: Math.max(0, Math.round((gameStartSeconds - minute) / 60)),
    });
  }
  return { rows, observedMinutes: closes.size };
}

async function fetchHistory(market: MlbMarket, side: Side): Promise<{ rows: MlbMinuteRow[]; observedMinutes: number; rawPoints: number }> {
  const endMs = Math.min(Date.now(), Date.parse(market.gameStart));
  const request = encodePriceHistoryRequest(market.marketSlug, Date.parse(market.createdAt), endMs);
  let response: Response;
  try {
    response = await fetch(PRICE_HISTORY_URL, {
      method: "POST",
      headers: { "User-Agent": USER_AGENT, "Content-Type": "application/proto", Accept: "application/proto" },
      body: new Uint8Array(request),
      signal: AbortSignal.timeout(30000),
    });
  } catch (_error) {
    throw new PolymarketMlbError("Polymarket US price history could not be reached. Try again.");
  }
  if (!response.ok) throw new PolymarketMlbError(response.status === 429 ? "Polymarket US rate-limited this request. Wait briefly and retry." : "Polymarket US could not load this market's price history.", response.status === 429 ? 429 : 502);
  const points = decodePriceHistoryResponse(Buffer.from(await response.arrayBuffer()));
  if (!points.length) throw new PolymarketMlbError("No price observations are available for this market.", 404);
  const built = buildMlbMinuteRows(points, side, market, endMs);
  if (!built.rows.length) throw new PolymarketMlbError("No completed pregame minute observations are available for this outcome.", 404);
  return { ...built, rawPoints: points.length };
}

function csv(rows: MlbMinuteRow[]): string {
  return ["item_id,datetime,price", ...rows.map((row) => `${row.item_id},${row.datetime},${row.price}`)].join("\n") + "\n";
}

export function registerPolymarketMlbRoutes(router: Router): void {
  router.get("/sports/mlb/games", async (req, res) => {
    try {
      const scope = ["upcoming", "previous", "any"].includes(String(req.query.scope)) ? String(req.query.scope) : "any";
      const markets = await listMlbMarkets(scope);
      res.status(200).json({ ok: true, source: "polymarket_us", count: markets.length, markets });
    } catch (error) {
      const safe = error instanceof PolymarketMlbError ? error : new PolymarketMlbError("MLB market discovery failed.");
      res.status(safe.status).json({ ok: false, error: "sports_data_unavailable", message: safe.message });
    }
  });

  router.post("/sports/mlb/history", async (req, res) => {
    try {
      const body = req.body || {};
      const markets = await listMlbMarkets("any");
      const market = markets.find((item) => item.marketSlug === String(body.marketSlug || ""));
      if (!market) throw new PolymarketMlbError("Select an available MLB moneyline market.", 404);
      const side = market.sides.find((item) => item.itemId === String(body.itemId || ""));
      if (!side) throw new PolymarketMlbError("Select a valid outcome for this market.", 400);
      const history = await fetchHistory(market, side);
      const limit = String(body.limit || "").toLowerCase() === "all" ? history.rows.length : Math.max(1, Math.min(Number(body.limit) || 500, 2000));
      const rows = history.rows.slice(-limit);
      const metadata = {
        schema: "polymarket_us_mlb_pregame_minute_prices_v1",
        retrieved_at: new Date().toISOString(),
        event_ticker: market.eventTicker,
        event_title: market.eventTitle,
        market_id: market.marketId,
        market_slug: market.marketSlug,
        market_created_at: market.createdAt,
        game_start: market.gameStart,
        team: side.team,
        position: side.position,
        item_id: side.itemId,
        raw_price_points: history.rawPoints,
        observed_minutes: history.observedMinutes,
        forward_filled_minutes: history.rows.length - history.observedMinutes,
        available_rows: history.rows.length,
        output_rows: rows.length,
        first_datetime: rows[0].datetime,
        last_datetime: rows[rows.length - 1].datetime,
        strictly_pregame: true,
        fidelity_minutes: 1,
        data_freshness: rows[rows.length - 1].datetime,
      };
      if (String(body.format || "").toLowerCase() === "csv") {
        const filename = `${market.marketSlug}-${side.itemId}-pregame-1m.csv`.replace(/[^A-Za-z0-9._-]/g, "-");
        res.setHeader("Content-Type", "text/csv; charset=utf-8");
        res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
        res.status(200).send(csv(rows));
        return;
      }
      res.status(200).json({ ok: true, source: "polymarket_us", metadata, rows });
    } catch (error) {
      const safe = error instanceof PolymarketMlbError ? error : new PolymarketMlbError("MLB price history failed.");
      res.status(safe.status).json({ ok: false, error: "sports_data_unavailable", message: safe.message });
    }
  });
}
