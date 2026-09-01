export type RedistributionStatus = "allowed_derived_only" | "review_required" | "not_redistributable";

export type DatasetCatalogEntry = {
  id: string;
  name: string;
  description: string;
  sourceProvider: string;
  sourceDataset: string;
  dataType: "raw" | "normalized" | "derived";
  derived: boolean;
  redistributionStatus: RedistributionStatus;
  licenseStatus: "quantura_owned" | "provider_terms_review_required";
  sourceTermsVersion: string | null;
  assetClasses: string[];
  frequency: string[];
  forecastQuantiles: string[];
  updateFrequency: string;
  apiAvailable: boolean;
  downloadAvailable: boolean;
  requiredScope: string;
  schema: Array<{ name: string; type: string; nullable: boolean; description: string }>;
};

export const DATASET_CATALOG: DatasetCatalogEntry[] = [
  {
    id: "quantura-forecast-quantiles",
    name: "Quantura Forecast Quantiles",
    description: "Quantura-created point-in-time forecast distributions with model and input-cutoff provenance.",
    sourceProvider: "quantura",
    sourceDataset: "forecast_outputs",
    dataType: "derived",
    derived: true,
    redistributionStatus: "allowed_derived_only",
    licenseStatus: "quantura_owned",
    sourceTermsVersion: null,
    assetClasses: ["equity", "etf", "fx", "commodity_proxy", "rate_proxy", "prediction_market"],
    frequency: ["1d", "1h", "provider_native"],
    forecastQuantiles: ["P1", "P25", "P50", "P75", "P99"],
    updateFrequency: "on_forecast_generation",
    apiAvailable: true,
    downloadAvailable: true,
    requiredScope: "forecasts:read",
    schema: [
      { name: "forecast_id", type: "string", nullable: false, description: "Immutable forecast identifier." },
      { name: "as_of", type: "date-time", nullable: false, description: "Point-in-time input cutoff in UTC." },
      { name: "resource", type: "object", nullable: false, description: "Provider-aware instrument or contract identity." },
      { name: "quantiles", type: "object", nullable: false, description: "Ordered P1/P25/P50/P75/P99 forecast values." },
      { name: "provenance", type: "object", nullable: false, description: "Model, version, source, units, and timestamps." }
    ]
  },
  {
    id: "prediction-market-canvas",
    name: "Prediction Market Canvas Time Series",
    description: "Normalized long-format prediction-market time series prepared for SageMaker Canvas.",
    sourceProvider: "polymarket_us,kalshi",
    sourceDataset: "provider_market_history",
    dataType: "normalized",
    derived: true,
    redistributionStatus: "review_required",
    licenseStatus: "provider_terms_review_required",
    sourceTermsVersion: null,
    assetClasses: ["prediction_market"],
    frequency: ["raw", "1m", "5m", "15m", "30m", "1h", "1d"],
    forecastQuantiles: [],
    updateFrequency: "on_demand",
    apiAvailable: true,
    downloadAvailable: true,
    requiredScope: "datasets:read",
    schema: [
      { name: "item_id", type: "string", nullable: false, description: "Stable provider-qualified contract ID." },
      { name: "timestamp", type: "date-time", nullable: false, description: "UTC observation timestamp." },
      { name: "target", type: "number", nullable: false, description: "Normalized decimal probability from 0 through 1." },
      { name: "minutes_to_event", type: "number", nullable: true, description: "Minutes before the scheduled event." },
      { name: "source", type: "string", nullable: false, description: "Provider provenance." }
    ]
  },
  {
    id: "historical-market-data",
    name: "Historical Market Data",
    description: "Provider-sourced historical observations with normalized timestamps and explicit units.",
    sourceProvider: "alpaca,yahoo_finance",
    sourceDataset: "provider_history",
    dataType: "raw",
    derived: false,
    redistributionStatus: "review_required",
    licenseStatus: "provider_terms_review_required",
    sourceTermsVersion: null,
    assetClasses: ["equity", "etf", "fx", "commodity_proxy", "rate_proxy", "option"],
    frequency: ["provider_dependent"],
    forecastQuantiles: [],
    updateFrequency: "on_demand",
    apiAvailable: true,
    downloadAvailable: true,
    requiredScope: "market_data:read",
    schema: [
      { name: "timestamp", type: "date-time", nullable: false, description: "UTC observation timestamp." },
      { name: "open", type: "number", nullable: true, description: "Provider-reported open." },
      { name: "high", type: "number", nullable: true, description: "Provider-reported high." },
      { name: "low", type: "number", nullable: true, description: "Provider-reported low." },
      { name: "close", type: "number", nullable: false, description: "Provider-reported close." },
      { name: "volume", type: "number", nullable: true, description: "Provider-reported volume." }
    ]
  }
];

export function datasetById(id: unknown): DatasetCatalogEntry | null {
  const clean = String(id || "").trim().toLowerCase();
  return DATASET_CATALOG.find((entry) => entry.id === clean) || null;
}
