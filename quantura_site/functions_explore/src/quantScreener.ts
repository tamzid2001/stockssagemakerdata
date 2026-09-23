import { gunzipSync } from "node:zlib";

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
  signal?: "all" | "buy" | "cutoff_buy";
  signalChanged?: boolean;
  quantileRules?: Array<{ quantile: string; statistic: "min" | "max" | "avg"; operator: "gt" | "gte" | "lt" | "lte"; percent: number }>;
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
const ARCHIVE_DAYS = 14;
const ARCHIVE_ASSET = /^quantura-screener-(\d{4}-\d{2}-\d{2})\.json\.gz$/;
const CACHE_TTL_MS = 5 * 60 * 1000;
const ALLOWED_POSITIONS = new Set(["below-p01", "above-p01", "below-p10", "above-p10", "below-p50", "above-p50", "below-p90", "above-p90", "below-p99", "above-p99"]);
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

const datasetCache = new Map<string, { expiresAt: number; value: QuantScreenerDataset }>();
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
  const signal = firstValue(raw.signal).toLowerCase();
  if (signal && signal !== "all") errors.push("Buy/sell signal filters are no longer available. Use quantile comparisons instead.");
  if (["true", "1"].includes(firstValue(raw.signalChanged))) errors.push("Signal-change filters are no longer available.");
  let quantileRules: NonNullable<QuantScreenerQuery["quantileRules"]> = [];
  if (raw.quantileRules) {
    try {
      const input = typeof raw.quantileRules === "string" ? JSON.parse(raw.quantileRules) : raw.quantileRules;
      if (!Array.isArray(input) || input.length > 12) throw new Error();
      for (const rule of input) {
        if (!rule || !["p01","p10","p25","p50","p75","p90","p99"].includes(rule.quantile) ||
          !["min","max","avg"].includes(rule.statistic) || !["gt","gte","lt","lte"].includes(rule.operator) ||
          typeof rule.percent !== "number" || !Number.isFinite(rule.percent) || Math.abs(rule.percent) > 100000) throw new Error();
      }
      quantileRules = input.map(({quantile,statistic,operator,percent}) => ({quantile,statistic,operator,percent}));
    } catch { errors.push("Quantile rules must contain up to 12 valid min/max/avg percentage comparisons."); }
  }
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
  if (biasRaw && biasRaw !== "all") errors.push("Model-bias filters are no longer available.");
  if (["1", "true", "yes", "active"].includes(firstValue(raw.specialP10).toLowerCase())) errors.push("Legacy P10 signal filters are no longer available.");
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
      signal: "all",
      signalChanged: false,
      quantileRules,
      search,
      universe: normalizeEnum(universeRaw, ["all", "sp500", "nasdaq", "etf"] as const, "all"),
      marketCap: normalizeEnum(marketCapRaw, ["all", "mega", "large", "mid", "small", "micro"] as const, "all"),
      minMarketCap,
      maxMarketCap,
      positions: positions.filter((position) => ALLOWED_POSITIONS.has(position)),
      bias: "all",
      earnings: normalizeEnum(earningsRaw, ["all", "today", "7", "14", "30", "unknown"] as const, "all"),
      specialP10: false,
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
  if (query.signal === "cutoff_buy") {
    if ((row.cutoff_p99_signal as {value?: string} | null)?.value !== "buy") return false;
  } else if (query.signal && query.signal !== "all" && row.signal !== query.signal) return false;
  const prior = row.last_non_neutral_signal as {value?: string} | undefined;
  if (query.signalChanged && (!prior || !["buy","sell"].includes(String(row.signal)) || row.signal === prior.value)) return false;
  for (const rule of query.quantileRules || []) {
    const value = (row.quantile_stats as Record<string, Record<string, unknown>> | undefined)?.[rule.quantile]?.[rule.statistic];
    const price = row.actual_price;
    if (typeof value !== "number" || !Number.isFinite(value) || typeof price !== "number" || !(price > 0)) return false;
    // Quantile value relative to price, NOT price relative to the quantile.
    const percent = (value - price) / price * 100;
    if (!(rule.operator === "gt" ? percent > rule.percent : rule.operator === "gte" ? percent >= rule.percent : rule.operator === "lt" ? percent < rule.percent : percent <= rule.percent)) return false;
  }
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

function validArchiveDate(date: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(date) && !Number.isNaN(Date.parse(`${date}T00:00:00Z`)) &&
    new Date(`${date}T00:00:00Z`).toISOString().slice(0, 10) === date;
}

export function screenerArchiveDates(assets: Array<Record<string, unknown>>, latestDate: string, today = new Date()): string[] {
  const cutoff = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() - (ARCHIVE_DAYS - 1))).toISOString().slice(0, 10);
  const available = assets.map(asset => ARCHIVE_ASSET.exec(String(asset.name || ""))?.[1] || "").filter(date =>
    validArchiveDate(date) && date >= cutoff && date <= today.toISOString().slice(0, 10) && date <= latestDate);
  // Older releases had only a rolling asset. Keep that readable even before
  // the first dated publication; never invent any missing daily scans.
  if (validArchiveDate(latestDate)) available.push(latestDate);
  return Array.from(new Set(available)).sort().reverse();
}

export async function listPublishedScreenerDates(owner: string, repo: string, latestDate: string): Promise<string[]> {
  try {
    return screenerArchiveDates(await getReleaseAssets(owner, repo), latestDate);
  } catch (error) {
    // Private staging can serve the current dataset from an override URL with
    // no public GitHub release. It still gets the current date, never fake days.
    if (process.env.SCREENER_DATA_URL) return screenerArchiveDates([], latestDate);
    throw error;
  }
}

export async function loadPublishedScreenerDataset(owner: string, repo: string, date?: string): Promise<QuantScreenerDataset> {
  const key = date || "latest";
  const cached = datasetCache.get(key);
  if (cached && cached.expiresAt > Date.now()) return cached.value;
  if (date && !validArchiveDate(date)) throw new Error("screener_snapshot_not_found");
  const name = date ? `quantura-screener-${date}.json.gz` : JSON_ASSET;
  let response: Response;
  if (date) {
    const latest = await loadPublishedScreenerDataset(owner, repo);
    const dates = await listPublishedScreenerDates(owner, repo, latest.scan_date);
    if (!dates.includes(date)) throw new Error("screener_snapshot_not_found");
    if (date === latest.scan_date) return latest;
    response = await fetchReleaseAsset(owner, repo, name);
  } else response = await fetchReleaseAsset(owner, repo, name);
  const payload = (date
    ? JSON.parse(gunzipSync(Buffer.from(await response.arrayBuffer())).toString("utf8"))
    : await response.json()) as QuantScreenerDataset;
  if (!["quantura-screener-v2", "quantura-screener-v3"].includes(payload?.schema_version) || !Array.isArray(payload.items) || !payload.manifest) {
    throw new Error("screener_dataset_invalid");
  }
  if (date && payload.scan_date !== date) throw new Error("screener_snapshot_date_mismatch");
  const validItems = payload.items.filter((row) => row && typeof row === "object" && String(row.ticker || "").trim());
  const value = { ...payload, items: validItems };
  datasetCache.set(key, { expiresAt: Date.now() + CACHE_TTL_MS, value });
  for (const [cacheKey, entry] of datasetCache) if (entry.expiresAt <= Date.now()) datasetCache.delete(cacheKey);
  return value;
}

export async function loadPublishedScreenerCsv(owner: string, repo: string): Promise<Buffer> {
  const response = await fetchReleaseAsset(owner, repo, CSV_ASSET);
  return Buffer.from(await response.arrayBuffer());
}

/** Remove legacy trade-signal fields from current public research results. */
export function publicScreenerRow(row: QuantScreenerRow): QuantScreenerRow {
  return Object.fromEntries(Object.entries(row).filter(([key]) => !/(signal|bias|daily_evaluation|buy_price_target|buy_target_date)/i.test(key))) as QuantScreenerRow;
}

/** Export the same filtered price/quantile snapshot, with formula-safe text fields. */
export function screenerRowsCsv(rows: QuantScreenerRow[]): string {
  const levels=["p01","p10","p25","p50","p75","p90","p99"];
  const keys=["ticker","company_name","actual_price","actual_price_timestamp","quote_source","quote_session","forecast_comparison_date",...levels,...levels.flatMap(q=>["min","max","avg"].map(s=>`${q}_${s}`)),"last_forecast_update","forecast_engine","split_status"];
  const cell=(value:unknown)=>{let text=String(value??"");if(typeof value!=="number" && /^[\s]*[=+@-]/.test(text))text=`'${text}`;return `"${text.replace(/"/g,'""')}"`;};
  return [keys.join(","),...rows.map(row=>{
    const values:Record<string,unknown>=publicScreenerRow(row);
    for(const q of levels)for(const stat of ["min","max","avg"])values[`${q}_${stat}`]=(row.quantile_stats as Record<string,Record<string,unknown>>|undefined)?.[q]?.[stat];
    return keys.map(k=>cell(values[k])).join(",");
  })].join("\r\n")+"\r\n";
}

export function clearPublishedScreenerCache(): void {
  datasetCache.clear();
  releaseCache = null;
}
