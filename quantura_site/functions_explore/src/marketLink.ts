/** Parse identifiers only. Never fetch a user-supplied URL (SSRF boundary). */
export function parseMarketLink(value: unknown): { source: "polymarket_us" | "kalshi"; identifier: string; kind: "event" | "market" | "either" } {
  const raw = String(value || "").trim();
  if (raw.length > 2048) throw new Error("market_link_invalid");
  let url: URL;
  try { url = new URL(raw); } catch { throw new Error("market_link_invalid"); }
  if (url.protocol !== "https:" || url.username || url.password || url.port) throw new Error("market_link_invalid");
  const host = url.hostname.toLowerCase().replace(/^www\./, "");
  if (!["polymarket.us", "kalshi.com"].includes(host)) throw new Error("market_link_provider_unsupported");
  const parts = url.pathname.split("/").filter(Boolean);
  const identifier = parts.at(-1) || "";
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_-]{1,219}$/.test(identifier)) throw new Error("market_link_invalid");
  if (host === "kalshi.com") {
    if (parts[0] !== "markets" || parts.length < 2) throw new Error("market_link_invalid");
    return { source: "kalshi", identifier: identifier.toUpperCase(), kind: "either" };
  }
  if (!["event", "events", "market", "markets", "sports"].includes(parts[0]) || parts.length < 2) throw new Error("market_link_invalid");
  return { source: "polymarket_us", identifier, kind: /^markets?$/.test(parts[0]) ? "market" : "either" };
}
