import React, { useEffect, useMemo, useState } from "react";

import { MacroCard } from "./MacroCard";

type RegistryEntry = {
  id: string;
  title: string;
  category: string;
  endpoint: string;
  updateCadence?: string;
  defaultQuery?: {
    fields?: string[];
    sort?: string[];
    page?: {
      number?: number;
      size?: number;
    };
  };
};

type FiscalPayload = {
  data: Array<Record<string, unknown>>;
  meta?: {
    labels?: Record<string, string>;
    totalPages?: number;
  };
};

export function MacroDashboard() {
  const [registry, setRegistry] = useState<RegistryEntry[]>([]);
  const [rowsByCard, setRowsByCard] = useState<Record<string, FiscalPayload>>({});
  const [error, setError] = useState<string>("");

  useEffect(() => {
    let mounted = true;
    const load = async () => {
      try {
        const registryRes = await fetch("/api/fiscaldata/registry");
        const registryJson = await registryRes.json();
        if (!registryRes.ok) throw new Error("Unable to load Fiscal Data registry.");
        const endpoints = Array.isArray(registryJson?.endpoints) ? registryJson.endpoints : [];
        if (!mounted) return;
        setRegistry(endpoints);
      } catch (err) {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : "Unable to load macros.");
      }
    };
    load();
    return () => {
      mounted = false;
    };
  }, []);

  useEffect(() => {
    if (!registry.length) return;
    let mounted = true;
    const run = async () => {
      const next: Record<string, FiscalPayload> = {};
      for (const card of registry) {
        const pageSize = Number(card.defaultQuery?.page?.size || 100);
        const params = new URLSearchParams({
          endpoint: card.endpoint,
          "page[number]": "1",
          "page[size]": String(pageSize),
          format: "json",
        });
        if (Array.isArray(card.defaultQuery?.fields) && card.defaultQuery?.fields.length) {
          params.set("fields", card.defaultQuery.fields.join(","));
        }
        if (Array.isArray(card.defaultQuery?.sort) && card.defaultQuery?.sort.length) {
          params.set("sort", card.defaultQuery.sort.join(","));
        }
        const res = await fetch(`/api/fiscaldata?${params.toString()}`);
        const payload = await res.json().catch(() => ({}));
        next[card.id] = res.ok ? payload : { data: [] };
      }
      if (!mounted) return;
      setRowsByCard(next);
    };
    run().catch((err) => {
      if (!mounted) return;
      setError(err instanceof Error ? err.message : "Unable to load macros.");
    });
    return () => {
      mounted = false;
    };
  }, [registry]);

  const grouped = useMemo(() => {
    const map = new Map<string, RegistryEntry[]>();
    registry.forEach((entry) => {
      const key = entry.category || "Other";
      if (!map.has(key)) map.set(key, []);
      map.get(key)?.push(entry);
    });
    return Array.from(map.entries());
  }, [registry]);

  if (error) return <div className="small muted">{error}</div>;
  if (!registry.length) return <div className="small muted">Loading macro cards...</div>;

  return (
    <div>
      {grouped.map(([category, cards]) => (
        <section key={category} style={{ marginBottom: 18 }}>
          <h3>{category}</h3>
          <div className="content-grid">
            {cards.map((card) => (
              <MacroCard key={card.id} title={card.title} endpoint={card.endpoint} cadence={card.updateCadence}>
                <div className="small muted">Rows loaded: {rowsByCard[card.id]?.data?.length || 0}</div>
              </MacroCard>
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}
