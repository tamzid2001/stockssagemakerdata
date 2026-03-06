import crypto from "crypto";

import registryJson from "./endpoints.registry.json";

export type FiscalRegistryEntry = {
  id: string;
  endpoint: string;
  title: string;
  category: string;
  defaultQuery?: {
    fields?: string[];
    sort?: string[];
    page?: {
      number?: number;
      size?: number;
    };
  };
  updateCadence?: string;
  ttlSeconds?: number;
  ui?: {
    view?: string;
    primaryDateField?: string;
    primaryValueField?: string;
  };
};

type FiscalPayload = {
  data: Record<string, unknown>[];
  meta: {
    labels: Record<string, string>;
    dataTypes: Record<string, string>;
    dataFormats: Record<string, string>;
    totalCount?: number;
    totalPages?: number;
    count?: number;
  };
  links?: Record<string, string | null>;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  retries?: number;
  timeoutMs?: number;
};

type CacheReadResult = {
  payload: FiscalPayload;
  isFresh: boolean;
  fetchedAtMs: number;
};

const FISCALDATA_BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service";
const DEFAULT_TIMEOUT_MS = 12000;
const DEFAULT_RETRIES = 2;
const MAX_PAGE_SIZE = 5000;

const parsedRegistry = Array.isArray(registryJson) ? (registryJson as FiscalRegistryEntry[]) : [];
export const fiscalEndpointRegistry: FiscalRegistryEntry[] = parsedRegistry
  .filter((entry) => entry && typeof entry === "object")
  .filter((entry) => typeof entry.endpoint === "string" && /^\/v[12]\//.test(entry.endpoint))
  .map((entry) => ({
    ...entry,
    endpoint: String(entry.endpoint).trim(),
    ttlSeconds: clampTtl(entry.ttlSeconds),
    defaultQuery: {
      fields: Array.isArray(entry.defaultQuery?.fields)
        ? entry.defaultQuery?.fields?.map((field) => sanitizeToken(field)).filter(Boolean)
        : [],
      sort: Array.isArray(entry.defaultQuery?.sort)
        ? entry.defaultQuery?.sort
            ?.map((sortField) => String(sortField || "").trim())
            .map((sortField) => {
              if (sortField.startsWith("-")) return `-${sanitizeToken(sortField.slice(1))}`;
              return sanitizeToken(sortField);
            })
            .filter(Boolean)
        : [],
      page: {
        number: clampPositiveInt(entry.defaultQuery?.page?.number, 1),
        size: clampPageSize(entry.defaultQuery?.page?.size),
      },
    },
  }));

export const fiscalEndpointRegistryByEndpoint = new Map(
  fiscalEndpointRegistry.map((entry) => [entry.endpoint, entry] as const)
);

export function sanitizeToken(input: unknown): string {
  return String(input || "")
    .trim()
    .replace(/[^A-Za-z0-9_.-]/g, "");
}

export function sanitizeEndpoint(input: unknown): string {
  const endpoint = String(input || "").trim();
  if (!endpoint) return "";
  if (!endpoint.startsWith("/")) return "";
  if (!/^\/v[12]\/[a-z0-9/_-]+$/i.test(endpoint)) return "";
  return endpoint;
}

export function clampPositiveInt(input: unknown, fallback = 1): number {
  const parsed = Number(input);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
}

export function clampPageSize(input: unknown, fallback = 100): number {
  const parsed = clampPositiveInt(input, fallback);
  return Math.max(1, Math.min(MAX_PAGE_SIZE, parsed));
}

export function sanitizeRawFilter(input: unknown): string {
  const value = String(input || "").trim();
  if (!value) return "";
  return value.replace(/[^A-Za-z0-9_.:(),\-<>= ]/g, "");
}

export function parseCsv(value: unknown): string[] {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export function buildFiscalQuery(params: {
  fields?: string[];
  filter?: string;
  sort?: string[];
  pageNumber?: number;
  pageSize?: number;
}): URLSearchParams {
  const query = new URLSearchParams();
  const fields = Array.isArray(params.fields) ? params.fields.map((field) => sanitizeToken(field)).filter(Boolean) : [];
  const sort = Array.isArray(params.sort)
    ? params.sort
        .map((item) => String(item || "").trim())
        .filter(Boolean)
        .map((item) => (item.startsWith("-") ? `-${sanitizeToken(item.slice(1))}` : sanitizeToken(item)))
        .filter(Boolean)
    : [];
  const filter = sanitizeRawFilter(params.filter || "");
  const pageNumber = clampPositiveInt(params.pageNumber, 1);
  const pageSize = clampPageSize(params.pageSize, 100);

  if (fields.length) query.set("fields", fields.join(","));
  if (filter) query.set("filter", filter);
  if (sort.length) query.set("sort", sort.join(","));
  query.set("format", "json");
  query.set("page[number]", String(pageNumber));
  query.set("page[size]", String(pageSize));
  return query;
}

export function buildFiscalApiUrl(endpoint: string, query: URLSearchParams): string {
  const cleanEndpoint = sanitizeEndpoint(endpoint);
  if (!cleanEndpoint) throw new Error("invalid_fiscal_endpoint");
  const serializedQuery = query.toString();
  return `${FISCALDATA_BASE_URL}${cleanEndpoint}${serializedQuery ? `?${serializedQuery}` : ""}`;
}

export function buildFiscalCacheDocId(endpoint: string, queryString: string): string {
  const key = `${endpoint}|${queryString}`;
  return crypto.createHash("sha256").update(key).digest("hex").slice(0, 48);
}

export function normalizeFiscalPayload(raw: unknown): FiscalPayload {
  const source = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
  const metaRaw = source.meta && typeof source.meta === "object" ? (source.meta as Record<string, unknown>) : {};
  const linksRaw = source.links && typeof source.links === "object" ? (source.links as Record<string, unknown>) : null;
  const labels = asStringRecord(metaRaw.labels);
  const dataTypes = asStringRecord(metaRaw.dataTypes);
  const dataFormats = asStringRecord(metaRaw.dataFormats);
  const links = linksRaw
    ? Object.fromEntries(Object.entries(linksRaw).map(([key, value]) => [String(key), value == null ? null : String(value)]))
    : undefined;

  return {
    data: Array.isArray(source.data) ? (source.data as Record<string, unknown>[]) : [],
    meta: {
      labels,
      dataTypes,
      dataFormats,
      totalCount: parseNumericMeta(metaRaw["total-count"]),
      totalPages: parseNumericMeta(metaRaw["total-pages"]),
      count: parseNumericMeta(metaRaw.count),
    },
    links,
  };
}

export async function fetchFiscalPayload(endpoint: string, query: URLSearchParams, options: FetchOptions = {}): Promise<FiscalPayload> {
  const fetchImpl = options.fetchImpl || fetch;
  const retries = Math.max(0, Math.min(4, Number(options.retries ?? DEFAULT_RETRIES)));
  const timeoutMs = Math.max(2000, Number(options.timeoutMs || DEFAULT_TIMEOUT_MS));
  const url = buildFiscalApiUrl(endpoint, query);
  let attempt = 0;
  while (attempt <= retries) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetchImpl(url, {
        method: "GET",
        signal: controller.signal,
        headers: { Accept: "application/json" },
      });
      if (!response.ok) {
        const retriable = response.status >= 500 || response.status === 429 || response.status === 408;
        if (retriable && attempt < retries) {
          await backoff(attempt);
          attempt += 1;
          continue;
        }
        const detail = await response.text().catch(() => "");
        throw new Error(`fiscaldata_request_failed_${response.status}${detail ? `:${detail.slice(0, 240)}` : ""}`);
      }
      const payload = await response.json().catch(() => ({}));
      return normalizeFiscalPayload(payload);
    } catch (error: unknown) {
      const isAbort = error instanceof Error && error.name === "AbortError";
      if ((isAbort || error instanceof TypeError) && attempt < retries) {
        await backoff(attempt);
        attempt += 1;
        continue;
      }
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }
  throw new Error("fiscaldata_request_exhausted");
}

export async function readFiscalCache(
  db: FirebaseFirestore.Firestore,
  cacheDocId: string,
  ttlSeconds: number
): Promise<CacheReadResult | null> {
  const snapshot = await db.collection("fiscaldata_cache").doc(cacheDocId).get();
  if (!snapshot.exists) return null;
  const data = (snapshot.data() || {}) as Record<string, unknown>;
  const payload = normalizeFiscalPayload(data.payload || {});
  const fetchedAtMs = timestampToMs(data.fetchedAt);
  const isFresh = Date.now() - fetchedAtMs < Math.max(60, ttlSeconds) * 1000;
  return {
    payload,
    isFresh,
    fetchedAtMs,
  };
}

export async function writeFiscalCache(
  db: FirebaseFirestore.Firestore,
  cacheDocId: string,
  config: {
    endpoint: string;
    query: string;
    ttlSeconds: number;
    payload: FiscalPayload;
  }
): Promise<void> {
  await db
    .collection("fiscaldata_cache")
    .doc(cacheDocId)
    .set(
      {
        endpoint: config.endpoint,
        query: config.query,
        ttlSeconds: Math.max(60, config.ttlSeconds),
        fetchedAt: new Date(),
        payload: config.payload,
      },
      { merge: true }
    );
}

export function buildQueryFromRegistry(entry: FiscalRegistryEntry, overrides: {
  fields?: string[];
  filter?: string;
  sort?: string[];
  pageNumber?: number;
  pageSize?: number;
} = {}): URLSearchParams {
  const fields = overrides.fields && overrides.fields.length ? overrides.fields : entry.defaultQuery?.fields || [];
  const sort = overrides.sort && overrides.sort.length ? overrides.sort : entry.defaultQuery?.sort || [];
  const pageNumber = overrides.pageNumber || entry.defaultQuery?.page?.number || 1;
  const pageSize = overrides.pageSize || entry.defaultQuery?.page?.size || 100;

  return buildFiscalQuery({
    fields,
    filter: overrides.filter || "",
    sort,
    pageNumber,
    pageSize,
  });
}

function clampTtl(input: unknown): number {
  const parsed = clampPositiveInt(input, 3600);
  return Math.max(60, Math.min(7 * 24 * 60 * 60, parsed));
}

function parseNumericMeta(value: unknown): number | undefined {
  if (value == null) return undefined;
  const parsed = Number(String(value).replace(/,/g, ""));
  if (!Number.isFinite(parsed)) return undefined;
  return parsed;
}

function asStringRecord(value: unknown): Record<string, string> {
  if (!value || typeof value !== "object") return {};
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, item]) => [String(key), String(item ?? "")])
  );
}

function timestampToMs(value: unknown): number {
  if (!value) return 0;
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  if (typeof value === "object" && value !== null) {
    const maybeTimestamp = value as { toMillis?: () => number; _seconds?: number };
    if (typeof maybeTimestamp.toMillis === "function") return maybeTimestamp.toMillis();
    if (Number.isFinite(maybeTimestamp._seconds)) return Number(maybeTimestamp._seconds) * 1000;
  }
  return 0;
}

async function backoff(attempt: number): Promise<void> {
  const waitMs = Math.min(2500, 250 * 2 ** attempt);
  await new Promise((resolve) => setTimeout(resolve, waitMs));
}
