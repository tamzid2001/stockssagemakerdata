#!/usr/bin/env node
/* eslint-disable no-console */
import fs from "node:fs/promises";
import path from "node:path";

type RegistryEntry = {
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
};

const BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service";
const registryPath = path.resolve(process.cwd(), "src/lib/fiscaldata/endpoints.registry.json");

const ensure = (condition: boolean, message: string): void => {
  if (!condition) {
    throw new Error(message);
  }
};

const pingEnabled = process.argv.includes("--ping");

const main = async () => {
  const text = await fs.readFile(registryPath, "utf8");
  const parsed = JSON.parse(text);
  ensure(Array.isArray(parsed), "Registry must be an array.");
  ensure(parsed.length > 0, "Registry must contain at least one endpoint entry.");

  const seenIds = new Set<string>();
  for (const raw of parsed as RegistryEntry[]) {
    ensure(raw && typeof raw === "object", "Registry entry must be an object.");
    ensure(typeof raw.id === "string" && raw.id.trim().length > 0, "Each registry entry needs a non-empty id.");
    ensure(!seenIds.has(raw.id), `Duplicate registry id: ${raw.id}`);
    seenIds.add(raw.id);
    ensure(typeof raw.endpoint === "string" && /^\/v[12]\//.test(raw.endpoint), `${raw.id}: endpoint must start with /v1/ or /v2/.`);
    ensure(typeof raw.title === "string" && raw.title.trim().length > 0, `${raw.id}: title is required.`);
    ensure(typeof raw.category === "string" && raw.category.trim().length > 0, `${raw.id}: category is required.`);
    if (raw.ttlSeconds != null) {
      ensure(Number.isFinite(raw.ttlSeconds) && raw.ttlSeconds > 0, `${raw.id}: ttlSeconds must be positive.`);
    }
    if (raw.defaultQuery?.page?.size != null) {
      ensure(raw.defaultQuery.page.size > 0 && raw.defaultQuery.page.size <= 5000, `${raw.id}: default page size must be 1..5000.`);
    }
  }

  console.log(`Registry shape check passed (${parsed.length} endpoints).`);

  if (!pingEnabled) return;
  console.log("Pinging Fiscal Data endpoints with page[size]=1...");
  for (const row of parsed as RegistryEntry[]) {
    const query = new URLSearchParams();
    query.set("format", "json");
    query.set("page[size]", "1");
    query.set("page[number]", "1");
    if (Array.isArray(row.defaultQuery?.fields) && row.defaultQuery?.fields?.length) {
      query.set("fields", row.defaultQuery.fields.join(","));
    }
    const url = `${BASE_URL}${row.endpoint}?${query.toString()}`;
    const response = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
    ensure(response.ok, `${row.id}: ping failed with status ${response.status}`);
    console.log(`- ${row.id}: ok`);
  }
};

main().catch((error) => {
  console.error(`Fiscal Data registry verification failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});
