export type FiscalFilterOp = "gt" | "lt" | "gte" | "lte" | "eq" | "in";

export type FiscalFilter = {
  field: string;
  op: FiscalFilterOp;
  value: string | string[];
};

export type FiscalDataRequestParams = {
  endpoint: string;
  fields?: string[];
  filters?: FiscalFilter[];
  sort?: string[];
  page?: {
    number?: number;
    size?: number;
  };
  format?: "json";
};

export type FiscalDataMeta = {
  labels: Record<string, string>;
  dataTypes: Record<string, string>;
  dataFormats: Record<string, string>;
  totalCount?: number;
  totalPages?: number;
  count?: number;
};

export type FiscalDataNormalizedResponse<T = Record<string, unknown>> = {
  data: T[];
  meta: FiscalDataMeta;
  links?: Record<string, string | null>;
};

export type FiscalDataClientOptions = {
  baseUrl?: string;
  timeoutMs?: number;
  maxRetries?: number;
  fetchImpl?: typeof fetch;
};

const DEFAULT_BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service";
const DEFAULT_TIMEOUT_MS = 10000;
const DEFAULT_MAX_RETRIES = 2;

const OPERATOR_TOKEN: Record<FiscalFilterOp, string> = {
  gt: "gt",
  lt: "lt",
  gte: "gte",
  lte: "lte",
  eq: "eq",
  in: "in",
};

const RETRYABLE_HTTP = new Set([408, 425, 429, 500, 502, 503, 504]);

export class FiscalDataClientError extends Error {
  status?: number;
  retriable: boolean;

  constructor(message: string, status?: number, retriable = false) {
    super(message);
    this.name = "FiscalDataClientError";
    this.status = status;
    this.retriable = retriable;
  }
}

export class FiscalDataClient {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly maxRetries: number;
  private readonly fetchImpl: typeof fetch;

  constructor(options: FiscalDataClientOptions = {}) {
    this.baseUrl = String(options.baseUrl || DEFAULT_BASE_URL).replace(/\/$/, "");
    this.timeoutMs = Math.max(2000, Number(options.timeoutMs || DEFAULT_TIMEOUT_MS));
    this.maxRetries = Math.max(0, Math.min(6, Number(options.maxRetries ?? DEFAULT_MAX_RETRIES)));
    this.fetchImpl = options.fetchImpl || fetch;
  }

  static buildFilterString(filters: FiscalFilter[] = []): string {
    const parts = filters
      .map((filter) => FiscalDataClient.buildFilterPart(filter))
      .filter((item) => item.length > 0);
    return parts.join(",");
  }

  static buildFilterPart(filter: FiscalFilter): string {
    const field = FiscalDataClient.sanitizeToken(filter.field);
    const op = OPERATOR_TOKEN[filter.op];
    if (!field || !op) return "";

    if (filter.op === "in") {
      const rawValues = Array.isArray(filter.value) ? filter.value : [filter.value];
      const values = rawValues
        .map((item) => String(item ?? "").trim())
        .filter(Boolean)
        .map((item) => item.replace(/[(),]/g, ""));
      if (!values.length) return "";
      return `${field}:in:(${values.join(",")})`;
    }

    const value = String(Array.isArray(filter.value) ? filter.value[0] : filter.value ?? "").trim();
    if (!value) return "";
    return `${field}:${op}:${value}`;
  }

  static buildUrl(baseUrl: string, params: FiscalDataRequestParams): string {
    const endpoint = FiscalDataClient.normalizeEndpoint(params.endpoint);
    if (!endpoint) {
      throw new FiscalDataClientError("Fiscal Data endpoint is required.");
    }
    const query = FiscalDataClient.buildQuery(params);
    return `${String(baseUrl).replace(/\/$/, "")}${endpoint}?${query.toString()}`;
  }

  static buildQuery(params: FiscalDataRequestParams): URLSearchParams {
    const query = new URLSearchParams();
    const fields = Array.isArray(params.fields)
      ? params.fields.map((field) => FiscalDataClient.sanitizeToken(field)).filter(Boolean)
      : [];
    const sort = Array.isArray(params.sort)
      ? params.sort
          .map((item) => String(item || "").trim())
          .filter(Boolean)
          .map((item) => item.replace(/[^A-Za-z0-9_.-]/g, ""))
          .filter(Boolean)
      : [];
    const filter = FiscalDataClient.buildFilterString(params.filters || []);

    if (fields.length) query.set("fields", fields.join(","));
    if (filter) query.set("filter", filter);
    if (sort.length) query.set("sort", sort.join(","));
    query.set("format", "json");

    const pageNumber = Number(params.page?.number);
    const pageSize = Number(params.page?.size);
    if (Number.isFinite(pageNumber) && pageNumber > 0) query.set("page[number]", String(Math.floor(pageNumber)));
    if (Number.isFinite(pageSize) && pageSize > 0) query.set("page[size]", String(Math.floor(pageSize)));

    return query;
  }

  static normalizeResponse(payload: unknown): FiscalDataNormalizedResponse {
    const objectPayload =
      payload && typeof payload === "object" ? (payload as Record<string, unknown>) : ({} as Record<string, unknown>);
    const dataRaw = Array.isArray(objectPayload.data) ? objectPayload.data : [];
    const linksRaw =
      objectPayload.links && typeof objectPayload.links === "object"
        ? (objectPayload.links as Record<string, unknown>)
        : undefined;
    const links = linksRaw
      ? Object.fromEntries(
          Object.entries(linksRaw).map(([key, value]) => [String(key), value == null ? null : String(value)])
        )
      : undefined;

    return {
      data: dataRaw as Record<string, unknown>[],
      meta: FiscalDataClient.normalizeMeta(objectPayload.meta),
      links,
    };
  }

  static normalizeMeta(metaRaw: unknown): FiscalDataMeta {
    const meta = metaRaw && typeof metaRaw === "object" ? (metaRaw as Record<string, unknown>) : {};
    const labels = FiscalDataClient.asRecord(meta.labels);
    const dataTypes = FiscalDataClient.asRecord(meta.dataTypes);
    const dataFormats = FiscalDataClient.asRecord(meta.dataFormats);
    const totalCount = FiscalDataClient.parseMetaNumber(meta["total-count"]);
    const totalPages = FiscalDataClient.parseMetaNumber(meta["total-pages"]);
    const count = FiscalDataClient.parseMetaNumber(meta["count"]);
    return {
      labels,
      dataTypes,
      dataFormats,
      totalCount,
      totalPages,
      count,
    };
  }

  async request<T = Record<string, unknown>>(params: FiscalDataRequestParams): Promise<FiscalDataNormalizedResponse<T>> {
    const url = FiscalDataClient.buildUrl(this.baseUrl, params);
    const response = await this.fetchWithRetry(url, 0);
    const normalized = FiscalDataClient.normalizeResponse(response);
    return normalized as FiscalDataNormalizedResponse<T>;
  }

  private async fetchWithRetry(url: string, attempt: number): Promise<unknown> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await this.fetchImpl(url, {
        method: "GET",
        signal: controller.signal,
        headers: {
          Accept: "application/json",
        },
      });
      if (!response.ok) {
        const retriable = RETRYABLE_HTTP.has(response.status);
        if (retriable && attempt < this.maxRetries) {
          await FiscalDataClient.backoff(attempt);
          return this.fetchWithRetry(url, attempt + 1);
        }
        const detail = await response.text().catch(() => "");
        throw new FiscalDataClientError(
          `Fiscal Data API request failed with status ${response.status}${detail ? `: ${detail.slice(0, 200)}` : ""}`,
          response.status,
          retriable
        );
      }
      return response.json();
    } catch (error: unknown) {
      const isAbort = error instanceof Error && error.name === "AbortError";
      const retriable = isAbort || error instanceof TypeError;
      if (retriable && attempt < this.maxRetries) {
        await FiscalDataClient.backoff(attempt);
        return this.fetchWithRetry(url, attempt + 1);
      }
      if (error instanceof FiscalDataClientError) throw error;
      throw new FiscalDataClientError(
        `Fiscal Data API network error: ${error instanceof Error ? error.message : String(error)}`,
        undefined,
        retriable
      );
    } finally {
      clearTimeout(timeout);
    }
  }

  private static normalizeEndpoint(input: string): string {
    const value = String(input || "").trim();
    if (!value.startsWith("/")) return "";
    if (!/^\/v[12]\/[a-z0-9/_-]+$/i.test(value)) return "";
    return value;
  }

  private static sanitizeToken(input: string): string {
    return String(input || "")
      .trim()
      .replace(/[^A-Za-z0-9_.-]/g, "");
  }

  private static parseMetaNumber(value: unknown): number | undefined {
    if (value == null) return undefined;
    const parsed = Number(String(value).replace(/,/g, ""));
    if (!Number.isFinite(parsed)) return undefined;
    return parsed;
  }

  private static asRecord(value: unknown): Record<string, string> {
    if (!value || typeof value !== "object") return {};
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, val]) => [String(key), String(val ?? "")])
    );
  }

  private static async backoff(attempt: number): Promise<void> {
    const delay = Math.min(2000, 250 * 2 ** attempt);
    await new Promise((resolve) => setTimeout(resolve, delay));
  }
}
