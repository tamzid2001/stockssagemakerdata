import crypto from "node:crypto";
import type { Request } from "express";
import type { ApiPrincipal, PlatformApiScope } from "./apiAccess";

const SCOPES: PlatformApiScope[] = ["account:read", "forecasts:read", "forecasts:write", "forecasts:history", "market_data:read", "screener:read", "datasets:read"];
const PATHS = [
  /^\/v1\/(?:me\/access|capabilities|forecast\/models|market-search(?:\/(?:resolve|event|capabilities))?|market-data\/dukascopy\/instruments|market-data\/stocks\/history|market-data\/perps\/(?:markets|history))\/?$/,
  /^\/v1\/ensemble-forecasts(?:\/[A-Za-z0-9_-]+(?:\/(?:download|observations|reproduce))?)?\/?$/,
  /^\/v1\/screener\/forecasts\/[A-Za-z0-9._-]+(?:\/observations)?\/?$/,
  /^\/(?:v1\/)?(?:economic-data\/(?:search|describe|history)|market-data\/gemini\/(?:history|prediction-contract))\/?$/,
];

/** Only the API-specific secret injected by Rapid's gateway authenticates users.
 * Consumer application keys, usernames and subscription headers alone do not. */
export function rapidApiPrincipal(req: Request): ApiPrincipal | null {
  const supplied = req.headers["x-rapidapi-proxy-secret"];
  if (supplied === undefined) return null;
  const expected = process.env.RAPIDAPI_PROXY_SECRET || "";
  if (typeof supplied !== "string" || expected.length < 32 || supplied.length > 512) throw Error("api_key_invalid");
  const actualHash = crypto.createHash("sha256").update(supplied).digest();
  const expectedHash = crypto.createHash("sha256").update(expected).digest();
  if (!crypto.timingSafeEqual(actualHash, expectedHash)) throw Error("api_key_invalid");
  const username = req.headers["x-rapidapi-user"];
  const subscription = req.headers["x-rapidapi-subscription"];
  if (typeof username !== "string" || !/^[A-Za-z0-9_.-]{1,128}$/.test(username)) throw Error("api_key_invalid");
  if (subscription !== "CUSTOM" && username !== process.env.RAPIDAPI_PROVIDER_USER) throw Error("paid_api_required");
  const path = req.path.replace(/^\/api(?=\/)/, "");
  if (!["GET", "POST"].includes(req.method) || !PATHS.some(pattern => pattern.test(path))) throw Error("insufficient_scope");
  const apiId = process.env.RAPIDAPI_API_ID;
  if (!apiId || !/^api_[A-Za-z0-9-]+$/.test(apiId)) throw Error("api_key_invalid");
  const userId = "rapid_" + crypto.createHash("sha256").update(`${apiId}\0${username}`).digest("hex");
  return { userId, tokenId: null, tokenName: "RapidAPI Enterprise", tokenScopes: [...SCOPES], plan: "research", authMethod: "rapidapi", platformAdmin: false, guest: false };
}
