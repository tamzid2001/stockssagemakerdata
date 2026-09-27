import {prepareDataset} from "./predictionMarketData";

type Row = Record<string, unknown>;
export type GameObservation = {timestamp: string; price: number};
type GameHistory = {observations: GameObservation[]; history_source: string; observed_at: string};

/** Exact saved model inputs must be chronological, genuine and at or before cutoff. */
export function observedGameHistory(item: Row): GameObservation[] {
  const cutoff = Date.parse(String(item.input_cutoff)), start = cutoff - 10 * 86400000;
  if (!Number.isFinite(cutoff) || !Array.isArray(item.observations) || item.observations.length > 512) return [];
  const rows: GameObservation[] = [];
  let prior = start;
  for (const row of item.observations) {
    const t = Date.parse(row?.timestamp);
    if (!Number.isFinite(t) || t <= prior || t > cutoff || row.observed === false || row.is_forward_filled ||
        typeof row.price !== "number" || !Number.isFinite(row.price) || row.price < 0 || row.price > 1) return [];
    rows.push({timestamp: new Date(t).toISOString(), price: row.price});
    prior = t;
  }
  return rows;
}

const cache = new Map<string, {until: number; promise: Promise<GameHistory>}>();
/** Preserve model inputs and overlay only genuine completed hourly outcomes, through the forecast end. */
export async function gameHistory(item: Row, load = prepareDataset): Promise<GameHistory> {
  const saved = observedGameHistory(item), cutoff = Date.parse(String(item.input_cutoff));
  const now = Date.now(), end = Math.min(Math.floor(now / 3600000) * 3600000, Date.parse(String(item.forecast_end)));
  const observedAt = new Date(now).toISOString();
  if (saved.length && end <= cutoff) return {observations: saved, history_source: "saved_model_input", observed_at: observedAt};
  const key = String(item.id) + ":" + item.generated_at + ":" + item.input_cutoff + ":" + end;
  const existing = cache.get(key);
  if (existing && existing.until > now) return existing.promise;
  const promise = (async () => {
    const source = String(item.provider), start = saved.length ? cutoff : cutoff - 10 * 86400000;
    let data;
    try { data = await load({source, contracts: [{source, providerSymbol: item.symbol, contractId: item.contract_id, side: item.side,
      eventId: item.event_id, marketId: item.market_id || item.symbol, eventTitle: item.event_title,
      marketTitle: item.market_title, outcome: item.outcome, eventStart: item.game_start}],
      start: new Date(start).toISOString(), end: new Date(Math.max(cutoff, end)).toISOString(), frequency: "1h",
      mode: "normalized", target: "price", missing: "leave", history_phase: "both", pregameOnly: false});
    } catch (error) {
      if (saved.length && (error as {code?:string}).code === "no_data") return {observations: saved, history_source: "saved_model_input", observed_at: observedAt};
      throw error;
    }
    const observations = new Map(saved.map(row => [Date.parse(row.timestamp), row]));
    for (const row of data.rows) {
      // Kalshi stamps candle ends; Polymarket stamps bucket starts. Align completed bars.
      const t = Date.parse(String(row.timestamp)) + (source === "polymarket_us" ? 3600000 : 0);
      const price = source === "kalshi" ? row.ask : row.price;
      if (t > start && t <= Math.max(cutoff, end) && !row.is_forward_filled && row.observed !== false &&
          typeof price === "number" && Number.isFinite(price) && price >= 0 && price <= 1) {
        observations.set(t, {timestamp: new Date(t).toISOString(), price});
      }
    }
    return {observations: [...observations.values()].sort((a, b) => a.timestamp.localeCompare(b.timestamp)),
      history_source: saved.length ? "saved_model_input_and_provider_outcomes" : "provider_history", observed_at: observedAt};
  })();
  if (cache.size >= 200) cache.delete(cache.keys().next().value!);
  const entry = {until: now + 60000, promise};
  cache.set(key, entry);
  promise.catch(() => {if (cache.get(key) === entry) cache.delete(key);});
  return promise;
}
