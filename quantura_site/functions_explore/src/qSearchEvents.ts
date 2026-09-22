import { KALSHI_API_BASE } from "./kalshiProtocol";
import { normalizeKalshiEvent, normalizePolymarketEvents, isMoneyline, PredictionMarketDataError, type PredictionMarketContract } from "./predictionMarketData";

type RecordValue = Record<string, any>;
const memo = new Map<string, {until: number; value: RecordValue}>();
const pending = new Map<string, Promise<RecordValue>>();
const identifier = (v: unknown) => typeof v === "string" && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,219}$/.test(v);
const invalid = () => new PredictionMarketDataError("event_query_invalid", "Choose a provider event and a valid continuation cursor.", 422);

async function get(url: string, request: typeof fetch): Promise<RecordValue> {
  const saved = memo.get(url);
  if (saved && saved.until > Date.now()) return saved.value;
  if (pending.has(url)) return pending.get(url)!;
  const promise = (async () => {
    const response = await request(url, {headers: {Accept: "application/json"}, signal: AbortSignal.timeout(12_000), redirect: "error"});
    if (!response.ok) throw new PredictionMarketDataError("event_provider_unavailable", "The provider could not load this event. Retry without changing the selected contract.", response.status === 404 ? 404 : 502);
    const value = await response.json() as RecordValue;
    // Tests inject their own transport; only real provider responses enter shared cache.
    if (request === fetch) { memo.set(url, {until: Date.now()+30_000, value}); if (memo.size > 150) memo.delete(memo.keys().next().value!); }
    return value;
  })();
  pending.set(url, promise);
  try { return await promise; } finally { pending.delete(url); }
}

/** Labels organize provider facts; they never alter identity or history routing. */
export function contractGroup(contract: PredictionMarketContract): string {
  if (isMoneyline(contract)) return "Moneyline";
  return marketGroup([contract.marketType, contract.league, contract.marketTitle].join(" "));
}
export function marketGroup(value: string): string {
  if (/PLAYER|PASSING|RUSHING|RECEIVING|STRIKEOUT|TOUCHDOWN|REBOUND|ASSIST|HOMERUN|HOME RUN|SCORER/i.test(value)) return "Player props";
  if (/SPREAD|HANDICAP/i.test(value)) return "Spreads";
  if (/TOTAL|OVER.?UNDER/i.test(value)) return "Totals";
  if (/HALF|QUARTER|PERIOD|FIRST.?\d|1H|2H|F3|F5/i.test(value)) return "Periods";
  if (/MONEYLINE|GAME$|MATCH$/i.test(value)) return "Moneyline";
  return "Other markets";
}

type Cursor = {source: string; id: string; phase: "live" | "historical"; cursor: string; offset: number};
function decode(source: string, id: string, cursor: string): Cursor {
  if (!cursor) return {source, id, phase: "live", cursor: "", offset: 0};
  try {
    if (cursor.length > 5000 || !/^[A-Za-z0-9_-]+$/.test(cursor)) throw invalid();
    const c = JSON.parse(Buffer.from(cursor, "base64url").toString());
    if (c.source !== source || c.id !== id || !["live","historical"].includes(c.phase) || typeof c.cursor !== "string" || c.cursor.length > 2048 || /[\x00-\x1f]/.test(c.cursor) || !Number.isInteger(c.offset) || c.offset < 0 || c.offset > 100000) throw invalid();
    return c;
  } catch { throw invalid(); }
}
const encode = (c: Cursor) => Buffer.from(JSON.stringify(c)).toString("base64url");

/** One provider-verified branch/page. Never fan out hundreds of history calls. */
export async function eventMarketPage(source: string, id: string, cursor = "", request: typeof fetch = fetch) {
  if (!["kalshi","polymarket_us"].includes(source) || !identifier(id)) throw invalid();
  const state = decode(source,id,cursor);
  if (source === "polymarket_us") {
    const payload = await get(`https://gateway.polymarket.us/v1/events/slug/${encodeURIComponent(id)}`, request);
    const event = payload.event;
    if (!event || String(event.slug) !== id) throw new PredictionMarketDataError("event_not_found", "The provider did not confirm this event.", 404);
    const all = normalizePolymarketEvents({events:[event]}, {id:"sports",providerId:"",label:String(event.primaryTag?.league?.name || "Sports"),sport:String(event.primaryTag?.sport?.name || "Sports")});
    const unique = [...new Map(all.map(c=>[c.contractId,c])).values()];
    return {source, event_id:id, title:String(event.title || id), contracts:unique.slice(state.offset,state.offset+100),
      related_events:[], related_complete:true, next_cursor:state.offset+100<unique.length ? encode({...state,offset:state.offset+100}) : null,
      total_contracts:unique.length, coverage:"Contracts returned by this provider event; not other venues or inferred matches."};
  }
  const base = KALSHI_API_BASE;
  const eventPayload = await get(`${base}/events/${encodeURIComponent(id)}?with_nested_markets=false`, request);
  const event = eventPayload.event;
  if (!event || event.event_ticker !== id) throw new PredictionMarketDataError("event_not_found", "The provider did not confirm this event.", 404);
  let relatedComplete = true;
  const milestones = await get(`${base}/milestones?${new URLSearchParams({related_event_ticker:id,limit:"500"})}`, request).catch(()=>{relatedComplete=false;return {milestones:[]} as RecordValue;});
  if (milestones.cursor) relatedComplete = false;
  // Only direct, explicitly linked game/match milestones; never join a season or
  // tournament merely because it shares a team name or date.
  const linked = (Array.isArray(milestones.milestones) ? milestones.milestones : []).filter((m: RecordValue) =>
    /(?:_game|_match)$/.test(String(m.type)) && [...(m.related_event_tickers || []),...(m.primary_event_tickers || [])].includes(id));
  const related = [...new Set<string>(linked.flatMap((m: RecordValue)=>[...(m.related_event_tickers || []),...(m.primary_event_tickers || [])]))]
    .filter(t=>identifier(t) && t!==id).sort();
  const path = state.phase === "historical" ? "historical/markets" : "markets";
  const data = await get(`${base}/${path}?${new URLSearchParams({event_ticker:id,limit:"50",...(state.cursor ? {cursor:state.cursor}:{})})}`,request);
  const markets = (Array.isArray(data.markets)?data.markets:[]).filter((m:RecordValue)=>m.event_ticker===id);
  const contracts = normalizeKalshiEvent({event,markets,milestones:milestones.milestones}, String(event.category || ""));
  if (data.cursor && String(data.cursor) === state.cursor) throw new PredictionMarketDataError("repeated_cursor", "Provider repeated an event page. Retry later.", 502);
  const next = data.cursor ? {...state,cursor:String(data.cursor)} : state.phase === "live" ? {...state,phase:"historical" as const,cursor:""} : null;
  return {source,event_id:id,title:String(event.title || id),contracts,related_events:related.map(t=>({event_id:t,label:t,group:marketGroup(t.split("-")[0])})),
    related_complete:relatedComplete,next_cursor:next?encode(next):null,total_contracts:null,
    coverage:"Paged live and archived contracts for this event. Related branches use official game relationships; each branch loads on demand."};
}
