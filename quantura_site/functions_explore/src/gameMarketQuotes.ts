import {KALSHI_API_BASE, kalshiPrice} from "./kalshiProtocol";

type Row = Record<string, unknown>;
const identifier = (value: unknown) => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9_-]{1,219}$/.test(value) ? value : "";
const probability = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "" || typeof value === "boolean") return null;
  const n = Number(value); return Number.isFinite(n) && n >= 0 && n <= 1 ? n : null;
};

/** Construct links from provider identifiers, never from untrusted URLs. */
export function gameMarketUrl(row: Row): string | null {
  const symbol = identifier(row.symbol), event = identifier(row.event_id);
  if (row.provider === "kalshi" && symbol && event) {
    const url = new URL(`https://kalshi.com/markets/${symbol.split("-")[0].toLowerCase()}/event/${event.toLowerCase()}`);
    url.searchParams.set("op_market_ticker", symbol);
    url.searchParams.set("op_order_side", row.side === "no" || String(row.contract_id).endsWith(":no") ? "no" : "yes");
    return url.href;
  }
  if (row.provider === "polymarket_us" && symbol) {
    const slug = identifier(row.event_slug) || symbol;
    const league = slug.replace(/^aec-/, "").split("-")[0];
    return `https://polymarket.us/sports/${league}/${slug}`;
  }
  return null;
}

export type GamePrice = {id: unknown; latest_price: number | null; price_kind: "ask" | "last_trade" | "market_quote" | null; price_checked_at: string | null};

/** Bounded public read batches; independently quoted sides are never synthesized. */
export async function gameMarketPrices(rows: Row[]): Promise<GamePrice[]> {
  const markets = new Map<string, Row>();
  const symbols = (provider: string) => [...new Set(rows.filter(r => r.provider === provider).map(r => identifier(r.symbol)).filter(Boolean))];
  const tasks: {provider: string; url: URL; requested: Set<string>}[] = [];
  for (const provider of ["kalshi", "polymarket_us"]) {
    const all = symbols(provider), size = provider === "kalshi" ? 100 : 50;
    for (let start = 0; start < all.length; start += size) {
      const batch = all.slice(start, start + size), url = new URL(provider === "kalshi" ? `${KALSHI_API_BASE}/markets` : "https://gateway.polymarket.us/v1/markets");
      url.searchParams.set("limit", String(size));
      if (provider === "kalshi") url.searchParams.set("tickers", batch.join(","));
      else batch.forEach(slug => url.searchParams.append("slug", slug));
      tasks.push({provider, url, requested: new Set(batch)});
    }
  }
  const deadline = AbortSignal.timeout(12000); let next = 0;
  await Promise.all(Array.from({length: Math.min(6, tasks.length)}, async () => {
    while (next < tasks.length && !deadline.aborted) {
      const task = tasks[next++];
      try {
        const response = await fetch(task.url, {headers:{Accept:"application/json", "User-Agent":"Quantura-game-quotes/1"}, signal:AbortSignal.any([deadline, AbortSignal.timeout(5000)])});
        if (!response.ok) continue;
        const payload = await response.json();
        const checked = new Date().toISOString();
        for (const market of Array.isArray(payload.markets) ? payload.markets : []) {
          if (!market || typeof market !== "object") continue;
          const symbol = task.provider === "kalshi" ? market.ticker : market.slug;
          if (task.requested.has(symbol)) markets.set(task.provider + ":" + symbol, {...market, checked});
        }
      } catch { /* Missing quotes remain explicitly unavailable; forecasts still load. */ }
    }
  }));
  return rows.map(row => {
    const market = markets.get(row.provider + ":" + row.symbol);
    let price: number | null = null, kind: GamePrice["price_kind"] = null;
    if (market && row.provider === "kalshi") {
      const side = row.side === "no" ? "no" : "yes", ask = kalshiPrice(market, side + "_ask");
      const size = market[side + "_ask_size_fp"];
      if (["open", "active"].includes(String(market.status)) && ask !== null && ask > 0 && (size == null || Number(size) > 0)) { price = ask; kind = "ask"; }
      // The provider's last_price is a YES trade; never label its complement an observed NO trade.
      else if (side === "yes") { const last = kalshiPrice(market, "last_price"); if (last !== null) {price = last; kind = "last_trade";} }
    } else if (market && row.provider === "polymarket_us") {
      const side = (Array.isArray(market.marketSides) ? market.marketSides : []).find((s: Row) => s && String(s.id) === String(row.contract_id) && (s.long === false ? "short" : "long") === row.side);
      price = probability(side?.quote?.value ?? side?.price); if (price !== null) kind = "market_quote";
    }
    return {id:row.id, latest_price:price, price_kind:kind, price_checked_at:market ? String(market.checked) : null};
  });
}
