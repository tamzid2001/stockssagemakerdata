import { AlpacaError } from "./alpacaClient";

type FetchLike = typeof fetch;
type Entry = { until: number; bytes: number; value: Record<string, unknown> };
type State = { cooldown: number; bytes: number; cache: Map<string, Entry>; pending: Map<string, Promise<Record<string, unknown>>> };
const states = new WeakMap<FetchLike, State>();
function stateFor(request: FetchLike): State {
  let state = states.get(request);
  if (!state) { state = { cooldown: 0, bytes: 0, cache: new Map(), pending: new Map() }; states.set(request, state); }
  return state;
}
export function yahooRetrySeconds(request: FetchLike = fetch): number {
  return Math.max(0, Math.ceil((stateFor(request).cooldown - Date.now()) / 1000));
}
export function assertYahooReady(request: FetchLike): void {
  const seconds = yahooRetrySeconds(request);
  if (seconds) throw new AlpacaError("rate_limit", `Yahoo Finance is cooling down. Retry in ${seconds} seconds or choose Auto / Alpaca for US stocks.`, 429);
}
export function recordYahooRateLimit(response: Response, request: FetchLike): void {
  if (response.status !== 429) return;
  const header = response.headers.get("retry-after") || "60";
  const delay = /^\d+$/.test(header) ? Number(header) * 1000 : Date.parse(header) - Date.now();
  const state = stateFor(request);
  // Respect the provider's delay; never cap it to an earlier retry.
  state.cooldown = Math.max(state.cooldown, Date.now() + Math.max(60_000, Number.isFinite(delay) ? delay : 60_000));
  throw new AlpacaError("rate_limit", `Yahoo Finance rate-limited this request. Retry in ${yahooRetrySeconds(request)} seconds or choose Auto / Alpaca for US stocks.`, 429);
}

/** Public provider JSON only. No user tokens, private datasets, stale prices or failed responses are cached. */
export async function yahooJson(url: string, request: FetchLike, headers: Record<string, string> = {}, ttl = 60_000): Promise<Record<string, unknown>> {
  const state = stateFor(request), key = url + JSON.stringify(headers);
  const cached = state.cache.get(key);
  if (cached && cached.until > Date.now()) return structuredClone(cached.value);
  if (cached) { state.cache.delete(key); state.bytes -= cached.bytes; }
  const pending = state.pending.get(key);
  if (pending) return structuredClone(await pending);
  assertYahooReady(request);
  const task = (async () => {
    let response: Response;
    try { response = await request(url, { method: "GET", headers: { Accept: "application/json", ...headers }, signal: AbortSignal.timeout(25000) }); }
    catch { throw new AlpacaError("network", "Yahoo Finance market data could not be reached. Try again.", 502); }
    recordYahooRateLimit(response, request);
    if (response.status === 404) throw new AlpacaError("unsupported_symbol", "Yahoo Finance could not find this symbol.", 404);
    if ([401, 403].includes(response.status)) throw new AlpacaError("authentication", "Yahoo Finance requires a refreshed provider session.", 502);
    if (!response.ok) throw new AlpacaError("upstream", "Yahoo Finance could not complete the request.", 502);
    const value = await response.json().catch(() => null) as Record<string, unknown> | null;
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new AlpacaError("upstream", "Yahoo Finance returned an unreadable response.", 502);
    const bytes = Buffer.byteLength(JSON.stringify(value));
    if (bytes <= 4 * 1024 * 1024) {
      while (state.cache.size && (state.cache.size >= 32 || state.bytes + bytes > 16 * 1024 * 1024)) {
        const oldest = state.cache.keys().next().value!;
        state.bytes -= state.cache.get(oldest)!.bytes; state.cache.delete(oldest);
      }
      state.cache.set(key, { value, bytes, until: Date.now() + ttl }); state.bytes += bytes;
    }
    return value;
  })();
  state.pending.set(key, task);
  try { return structuredClone(await task); }
  finally { if (state.pending.get(key) === task) state.pending.delete(key); }
}
