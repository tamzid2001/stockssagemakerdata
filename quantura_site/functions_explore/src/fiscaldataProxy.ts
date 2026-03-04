import type { Request, Response, Router } from "express";

import {
  buildFiscalCacheDocId,
  buildQueryFromRegistry,
  clampPageSize,
  clampPositiveInt,
  fetchFiscalPayload,
  fiscalEndpointRegistry,
  fiscalEndpointRegistryByEndpoint,
  parseCsv,
  readFiscalCache,
  sanitizeEndpoint,
  sanitizeRawFilter,
  sanitizeToken,
  writeFiscalCache,
} from "./fiscaldata/core";

type RegisterOptions = {
  db: FirebaseFirestore.Firestore;
  fetchImpl?: typeof fetch;
};

type ResolveResult = {
  endpoint: string;
  fields: string[];
  filter: string;
  sort: string[];
  pageNumber: number;
  pageSize: number;
  ttlSeconds: number;
  registryEntry: ReturnType<typeof resolveRegistryEntry>;
};

const CACHE_CONTROL_MAX_AGE = 300;

export function registerFiscalDataRoutes(router: Router, options: RegisterOptions): void {
  router.get("/fiscaldata/registry", (_req, res) => {
    res.status(200).json({
      generatedAt: new Date().toISOString(),
      count: fiscalEndpointRegistry.length,
      endpoints: fiscalEndpointRegistry,
    });
  });

  router.get("/fiscaldata", async (req, res) => {
    let resolved: ResolveResult | null = null;
    try {
      resolved = resolveProxyRequest(req);
      if (!resolved.registryEntry) {
        res.status(400).json({ error: "endpoint_not_allowed" });
        return;
      }

      const query = buildQueryFromRegistry(resolved.registryEntry, {
        fields: resolved.fields,
        filter: resolved.filter,
        sort: resolved.sort,
        pageNumber: resolved.pageNumber,
        pageSize: resolved.pageSize,
      });
      const queryString = query.toString();
      const cacheDocId = buildFiscalCacheDocId(resolved.endpoint, queryString);
      const cached = await readFiscalCache(options.db, cacheDocId, resolved.ttlSeconds);
      if (cached?.isFresh) {
        setCachingHeaders(res, resolved.ttlSeconds, true);
        res.status(200).json(cached.payload);
        return;
      }

      const payload = await fetchFiscalPayload(resolved.endpoint, query, {
        fetchImpl: options.fetchImpl,
      });

      await writeFiscalCache(options.db, cacheDocId, {
        endpoint: resolved.endpoint,
        query: queryString,
        ttlSeconds: resolved.ttlSeconds,
        payload,
      });

      setCachingHeaders(res, resolved.ttlSeconds, false);
      res.status(200).json(payload);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      const status = message.includes("endpoint_not_allowed") ? 400 : 502;
      res.status(status).json({
        error: status === 400 ? "endpoint_not_allowed" : "fiscaldata_proxy_failed",
        detail: message.slice(0, 240),
      });
    }
  });
}

export async function refreshFiscalRegistryDefaults(
  db: FirebaseFirestore.Firestore,
  fetchImpl?: typeof fetch
): Promise<{
  refreshed: number;
  failed: Array<{ id: string; error: string }>;
}> {
  let refreshed = 0;
  const failed: Array<{ id: string; error: string }> = [];

  for (const entry of fiscalEndpointRegistry) {
    try {
      const query = buildQueryFromRegistry(entry, {});
      const queryString = query.toString();
      const cacheDocId = buildFiscalCacheDocId(entry.endpoint, queryString);
      const payload = await fetchFiscalPayload(entry.endpoint, query, { fetchImpl });
      await writeFiscalCache(db, cacheDocId, {
        endpoint: entry.endpoint,
        query: queryString,
        ttlSeconds: Number(entry.ttlSeconds || 3600),
        payload,
      });
      refreshed += 1;
    } catch (error: unknown) {
      failed.push({
        id: entry.id,
        error: (error instanceof Error ? error.message : String(error)).slice(0, 220),
      });
    }
  }

  return {
    refreshed,
    failed,
  };
}

function resolveProxyRequest(req: Request): ResolveResult {
  const endpoint = sanitizeEndpoint(req.query.endpoint);
  if (!endpoint) throw new Error("endpoint_not_allowed");
  const registryEntry = resolveRegistryEntry(endpoint);
  if (!registryEntry) throw new Error("endpoint_not_allowed");

  const fields = parseCsv(req.query.fields).map((field) => sanitizeToken(field)).filter(Boolean);
  const sort = parseCsv(req.query.sort)
    .map((item) => String(item || "").trim())
    .map((item) => (item.startsWith("-") ? `-${sanitizeToken(item.slice(1))}` : sanitizeToken(item)))
    .filter(Boolean);
  const filter = sanitizeRawFilter(req.query.filter);
  const pageNumber = clampPositiveInt(req.query["page[number]"], 1);
  const pageSize = clampPageSize(req.query["page[size]"], registryEntry.defaultQuery?.page?.size || 100);

  return {
    endpoint,
    fields: fields.length ? fields : registryEntry.defaultQuery?.fields || [],
    filter,
    sort: sort.length ? sort : registryEntry.defaultQuery?.sort || [],
    pageNumber,
    pageSize,
    ttlSeconds: Number(registryEntry.ttlSeconds || 3600),
    registryEntry,
  };
}

function resolveRegistryEntry(endpoint: string) {
  return fiscalEndpointRegistryByEndpoint.get(endpoint) || null;
}

function setCachingHeaders(res: Response, ttlSeconds: number, cacheHit: boolean): void {
  const ttl = Math.max(60, Number(ttlSeconds || 3600));
  const maxAge = Math.min(CACHE_CONTROL_MAX_AGE, ttl);
  res.setHeader("Cache-Control", `public, max-age=${maxAge}, s-maxage=${ttl}`);
  res.setHeader("X-Fiscaldata-Cache", cacheHit ? "HIT" : "MISS");
}
