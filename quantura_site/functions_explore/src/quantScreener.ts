export type QuantScreenerRow = Record<string, unknown> & {
  ticker: string;
  company_name?: string | null;
  actual_price?: number | null;
  p10?: number | null;
  p50?: number | null;
  p90?: number | null;
  market_cap?: number | null;
  next_earnings_date?: string | null;
  is_sp500?: boolean;
  is_nasdaq?: boolean;
  is_etf?: boolean;
  p10_signal_active?: boolean;
  general_bias?: string | null;
};

export type QuantScreenerDataset = {
  schema_version: string;
  scan_id: string;
  scan_date: string;
  generated_at: string;
  manifest: Record<string, unknown>;
  items: QuantScreenerRow[];
};

export type QuantScreenerQuery = {
  search: string;
  universe: "all" | "sp500" | "nasdaq" | "etf";
  marketCap: "all" | "mega" | "large" | "mid" | "small" | "micro";
  minMarketCap: number | null;
  maxMarketCap: number | null;
  positions: string[];
  bias: "all" | "buying" | "selling" | "neutral";
  earnings: "all" | "today" | "7" | "14" | "30" | "unknown";
  specialP10: boolean;
  page: number;
  pageSize: number;
  sort: string;
  direction: "asc" | "desc";
};

export type QuantScreenerPage = {
  items: QuantScreenerRow[];
  total: number;
  page: number;
  pageSize: number;
  pageCount: number;
  query: QuantScreenerQuery;
};

const RELEASE_TAG = "screener-latest";
const JSON_ASSET = "quantura-screener-latest.json";
const CSV_ASSET = "quantura-screener-latest.csv";
const CACHE_TTL_MS = 5 * 60 * 1000;
const ALLOWED_POSITIONS = new Set(["below-p10", "above-p10", "below-p50", "above-p50", "below-p90", "above-p90"]);
const SORT_FIELDS = new Set([
  "ticker",
  "company",
  "actualPrice",
  "marketCap",
  "p10",
  "p50",
  "p90",
  "distanceP10",
  "distanceP50",
  "distanceP90",
  "nextEarnings",
  "lastUpdate",
]);

let datasetCache: { expiresAt: number; value: QuantScreenerDataset } | null = null;
let releaseCache: { expiresAt: number; assets: Array<Record<string, unknown>> } | null = null;

function firstValue(value: unknown): string {
  const resolved = Array.isArray(value) ? value[0] : value;
  return String(resolved ?? "").trim();
}

function finiteOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function positiveInteger(value: unknown, fallback: number, maximum: number): number {
  const parsed = Math.floor(Number(firstValue(value)));
  return Number.isFinite(parsed) && parsed > 0 ? Math.min(maximum, parsed) : fallback;
}

function normalizeEnum<T extends string>(value: unknown, allowed: readonly T[], fallback: T): T {
  const clean = firstValue(value).toLowerCase();
  return allowed.includes(clean as T) ? (clean as T) : fallback;
}

export function parseQuantScreenerQuery(raw: Record<string, unknown>): { query: QuantScreenerQuery; errors: string[] } {
  const errors: string[] = [];
  const search = firstValue(raw.search || raw.q).slice(0, 80);
  const universeRaw = firstValue(raw.universe).toLowerCase();
  const marketCapRaw = firstValue(raw.marketCap || raw.cap).toLowerCase();
  const biasRaw = firstValue(raw.bias).toLowerCase();
  const earningsRaw = firstValue(raw.earnings).toLowerCase();
  const directionRaw = firstValue(raw.direction || raw.dir).toLowerCase();
  const sortRaw = firstValue(raw.sort) || "ticker";
  const positionsRaw = Array.isArray(raw.position)
    ? raw.position.flatMap((value) => String(value).split(","))
    : firstValue(raw.position || raw.positions).split(",");
  const positions = Array.from(new Set(positionsRaw.map((value) => value.trim().toLowerCase()).filter(Boolean)));

  if (universeRaw && !["all", "sp500", "nasdaq", "etf"].includes(universeRaw)) errors.push("Invalid universe filter.");
  if (marketCapRaw && !["all", "mega", "large", "mid", "small", "micro"].includes(marketCapRaw)) errors.push("Invalid market-cap filter.");
  if (biasRaw && !["all", "buying", "selling", "neutral"].includes(biasRaw)) errors.push("Invalid model-bias filter.");
  if (earningsRaw && !["all", "today", "7", "14", "30", "unknown"].includes(earningsRaw)) errors.push("Invalid earnings filter.");
  if (directionRaw && directionRaw !== "asc" && directionRaw !== "desc") errors.push("Invalid sort direction.");
  if (!SORT_FIELDS.has(sortRaw)) errors.push("Invalid sort field.");
  positions.forEach((position) => {
    if (!ALLOWED_POSITIONS.has(position)) errors.push(`Invalid quantile-position filter: ${position}.`);
  });

  const minMarketCapRaw = firstValue(raw.minMarketCap);
  const maxMarketCapRaw = firstValue(raw.maxMarketCap);
  const minMarketCap = minMarketCapRaw ? finiteOrNull(minMarketCapRaw) : null;
  const maxMarketCap = maxMarketCapRaw ? finiteOrNull(maxMarketCapRaw) : null;
  if (minMarketCapRaw && (minMarketCap === null || minMarketCap < 0)) errors.push("Minimum market cap must be a non-negative number.");
  if (maxMarketCapRaw && (maxMarketCap === null || maxMarketCap < 0)) errors.push("Maximum market cap must be a non-negative number.");
  if (minMarketCap !== null && maxMarketCap !== null && minMarketCap > maxMarketCap) {
    errors.push("Minimum market cap cannot exceed maximum market cap.");
  }

  return {
    query: {
      search,
      universe: normalizeEnum(universeRaw, ["all", "sp500", "nasdaq", "etf"] as const, "all"),
      marketCap: normalizeEnum(marketCapRaw, ["all", "mega", "large", "mid", "small", "micro"] as const, "all"),
      minMarketCap,
      maxMarketCap,
      positions: positions.filter((position) => ALLOWED_POSITIONS.has(position)),
      bias: normalizeEnum(biasRaw, ["all", "buying", "selling", "neutral"] as const, "all"),
      earnings: normalizeEnum(earningsRaw, ["all", "today", "7", "14", "30", "unknown"] as const, "all"),
      specialP10: ["1", "true", "yes", "active"].includes(firstValue(raw.specialP10 || raw.signal).toLowerCase()),
      page: positiveInteger(raw.page, 1, 100000),
      pageSize: positiveInteger(raw.pageSize || raw.limit, 50, 100),
      sort: SORT_FIELDS.has(sortRaw) ? sortRaw : "ticker",
      direction: directionRaw === "desc" ? "desc" : "asc",
    },
    errors,
  };
}

function asNumber(row: QuantScreenerRow, key: string): number | null {
  return finiteOrNull(row[key]);
}

function matchesPosition(row: QuantScreenerRow, condition: string): boolean {
  const price = asNumber(row, "actual_price");
  const boundaryKey = condition.slice(-3).replace("-", "");
  const boundary = asNumber(row, boundaryKey);
  if (price === null || boundary === null) return false;
  return condition.startsWith("below-") ? price < boundary : price > boundary;
}

function earningsDiffDays(value: unknown, today: dtShim = new Date()): number | null {
  const raw = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return null;
  const parsed = new Date(`${raw}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return null;
  const start = Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate());
  return Math.floor((parsed.getTime() - start) / 86400000);
}

type dtShim = Pick<Date, "getUTCFullYear" | "getUTCMonth" | "getUTCDate">;

export function rowMatchesQuery(row: QuantScreenerRow, query: QuantScreenerQuery, today: Date = new Date()): boolean {
  const ticker = String(row.ticker || "").toUpperCase();
  const company = String(row.company_name || "").toUpperCase();
  const search = query.search.toUpperCase();
  if (search && !ticker.includes(search) && !company.includes(search)) return false;
  if (query.universe === "sp500" && !row.is_sp500) return false;
  if (query.universe === "nasdaq" && !row.is_nasdaq) return false;
  if (query.universe === "etf" && !row.is_etf) return false;
  if (query.marketCap !== "all" && String(row.market_cap_bucket || "") !== query.marketCap) return false;
  const marketCap = asNumber(row, "market_cap");
  if (query.minMarketCap !== null && (marketCap === null || marketCap < query.minMarketCap)) return false;
  if (query.maxMarketCap !== null && (marketCap === null || marketCap > query.maxMarketCap)) return false;
  if (!query.positions.every((condition) => matchesPosition(row, condition))) return false;

  const bias = String(row.general_bias || "").toLowerCase();
  if (query.bias === "buying" && bias !== "buying bias") return false;
  if (query.bias === "selling" && bias !== "selling bias") return false;
  if (query.bias === "neutral" && bias !== "neutral / mixed") return false;
  if (query.specialP10 && !row.p10_signal_active) return false;

  const diff = earningsDiffDays(row.next_earnings_date, today);
  if (query.earnings === "unknown" && diff !== null) return false;
  if (query.earnings === "today" && diff !== 0) return false;
  if (["7", "14", "30"].includes(query.earnings)) {
    const limit = Number(query.earnings);
    if (diff === null || diff < 0 || diff > limit) return false;
  }
  return true;
}

const SORT_KEYS: Record<string, string> = {
  ticker: "ticker",
  company: "company_name",
  actualPrice: "actual_price",
  marketCap: "market_cap",
  p10: "p10",
  p50: "p50",
  p90: "p90",
  distanceP10: "distance_p10_pct",
  distanceP50: "distance_p50_pct",
  distanceP90: "distance_p90_pct",
  nextEarnings: "next_earnings_date",
  lastUpdate: "last_forecast_update",
};

function compareValues(left: unknown, right: unknown, direction: "asc" | "desc"): number {
  const leftMissing = left === null || left === undefined || left === "" || String(left).startsWith("N/A");
  const rightMissing = right === null || right === undefined || right === "" || String(right).startsWith("N/A");
  if (leftMissing && rightMissing) return 0;
  if (leftMissing) return 1;
  if (rightMissing) return -1;
  const leftNumber = finiteOrNull(left);
  const rightNumber = finiteOrNull(right);
  const base = leftNumber !== null && rightNumber !== null
    ? leftNumber - rightNumber
    : String(left).localeCompare(String(right), undefined, { sensitivity: "base", numeric: true });
  return direction === "desc" ? -base : base;
}

export function filterSortPaginateRows(rows: QuantScreenerRow[], query: QuantScreenerQuery, today: Date = new Date()): QuantScreenerPage {
  const filtered = rows.filter((row) => rowMatchesQuery(row, query, today));
  const key = SORT_KEYS[query.sort] || "ticker";
  filtered.sort((left, right) => {
    const compared = compareValues(left[key], right[key], query.direction);
    return compared || String(left.ticker).localeCompare(String(right.ticker));
  });
  const pageCount = Math.max(1, Math.ceil(filtered.length / query.pageSize));
  const page = Math.min(query.page, pageCount);
  const start = (page - 1) * query.pageSize;
  return {
    items: filtered.slice(start, start + query.pageSize),
    total: filtered.length,
    page,
    pageSize: query.pageSize,
    pageCount,
    query: { ...query, page },
  };
}

function githubHeaders(): Record<string, string> {
  const token = String(process.env.GITHUB_ACTIONS_TOKEN || process.env.GITHUB_TOKEN || "").trim();
  return {
    Accept: "application/vnd.github+json",
    "User-Agent": "quantura-studio",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  };
}

async function fetchWithTimeout(url: string, init: RequestInit = {}, timeoutMs = 15000): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...init, signal: controller.signal });
    if (!response.ok) throw new Error(`screener_dataset_http_${response.status}`);
    return response;
  } finally {
    clearTimeout(timer);
  }
}

async function getReleaseAssets(owner: string, repo: string): Promise<Array<Record<string, unknown>>> {
  if (releaseCache && releaseCache.expiresAt > Date.now()) return releaseCache.assets;
  const response = await fetchWithTimeout(
    `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/releases/tags/${RELEASE_TAG}`,
    { headers: githubHeaders() }
  );
  const release = (await response.json()) as Record<string, unknown>;
  const assets = Array.isArray(release.assets) ? (release.assets as Array<Record<string, unknown>>) : [];
  releaseCache = { expiresAt: Date.now() + CACHE_TTL_MS, assets };
  return assets;
}

async function fetchReleaseAsset(owner: string, repo: string, name: string): Promise<Response> {
  const override = name === JSON_ASSET
    ? String(process.env.SCREENER_DATA_URL || "").trim()
    : name === CSV_ASSET
      ? String(process.env.SCREENER_CSV_URL || "").trim()
      : "";
  if (override) return fetchWithTimeout(override, { headers: { "Cache-Control": "no-cache" } }, 20000);
  const assets = await getReleaseAssets(owner, repo);
  const asset = assets.find((candidate) => String(candidate.name || "") === name);
  const url = String(asset?.browser_download_url || "").trim();
  if (!url) throw new Error("screener_dataset_not_published");
  return fetchWithTimeout(`${url}?v=${Math.floor(Date.now() / CACHE_TTL_MS)}`, { headers: { "Cache-Control": "no-cache" } }, 30000);
}

export async function loadPublishedScreenerDataset(owner: string, repo: string): Promise<QuantScreenerDataset> {
  if (datasetCache && datasetCache.expiresAt > Date.now()) return datasetCache.value;
  const response = await fetchReleaseAsset(owner, repo, JSON_ASSET);
  const payload = (await response.json()) as QuantScreenerDataset;
  if (payload?.schema_version !== "quantura-screener-v2" || !Array.isArray(payload.items) || !payload.manifest) {
    throw new Error("screener_dataset_invalid");
  }
  const validItems = payload.items.filter((row) => row && typeof row === "object" && String(row.ticker || "").trim());
  const value = { ...payload, items: validItems };
  datasetCache = { expiresAt: Date.now() + CACHE_TTL_MS, value };
  return value;
}

export async function loadPublishedScreenerCsv(owner: string, repo: string): Promise<Buffer> {
  const response = await fetchReleaseAsset(owner, repo, CSV_ASSET);
  return Buffer.from(await response.arrayBuffer());
}

export function clearPublishedScreenerCache(): void {
  datasetCache = null;
  releaseCache = null;
}
