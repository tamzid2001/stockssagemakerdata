import cors from "cors";
import express, { Request, Response } from "express";
import admin from "firebase-admin";
import crypto from "crypto";
import { GoogleAuth } from "google-auth-library";
import { SecretManagerServiceClient } from "@google-cloud/secret-manager";
import { registerFiscalDataRoutes } from "./fiscaldataProxy";
import { runScheduledFiscaldataRefresh } from "./schedules/refreshFiscaldata";
import { runIndicatorAnalysis } from "./indicators";
import {
  analyzePredictionCsv,
  classifyUploadedCsv,
  downloadHistoricalStockDataset,
  refreshAutopilotRun,
  startAutopilotTraining,
} from "./autopilot";
import {
  buildForecastFromHistory,
  DEFAULT_FORECAST_QUANTILES,
  fetchYahooHistoryBars,
  runMarketDataScreener,
} from "./forecastingScreener";
import {
  analyzeSportsPredictionCsv,
  buildSportsAutopilotDatasetCsv,
  buildSportsHistoricalCsv,
  buildSportsTeamGameTotalsSnapshot,
  buildSportsPlayerContext,
  buildSportsRunPayloadExport,
  listSportsPlayers,
  listSportsTeams,
  type NormalizedSportsHistoryRow,
  type SportsLeagueKey,
  SPORTS_LEAGUES,
} from "./sports";
export { shopApi } from "./shopApi";
// eslint-disable-next-line @typescript-eslint/no-var-requires
const AdmZip = require("adm-zip");

function normalizeStorageBucketName(value: unknown): string {
  return String(value || "")
    .trim()
    .replace(/^gs:\/\//i, "")
    .replace(/^\/+/, "")
    .replace(/\/+$/, "");
}

function resolveFirebaseConfigStorageBucket(): string {
  const raw = String(process.env.FIREBASE_CONFIG || "").trim();
  if (!raw || !raw.startsWith("{")) return "";
  try {
    const parsed = JSON.parse(raw) as { storageBucket?: unknown };
    return normalizeStorageBucketName(parsed.storageBucket);
  } catch (_error) {
    return "";
  }
}

function resolveDefaultStorageBucketName(): string {
  const projectId = String(
    process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || ""
  ).trim();
  const candidates = [
    process.env.FIREBASE_STORAGE_BUCKET,
    process.env.STORAGE_BUCKET,
    process.env.GCLOUD_STORAGE_BUCKET,
    resolveFirebaseConfigStorageBucket(),
    projectId ? `${projectId}.firebasestorage.app` : "",
    projectId ? `${projectId}.appspot.com` : "",
  ];
  for (const candidate of candidates) {
    const normalized = normalizeStorageBucketName(candidate);
    if (normalized) return normalized;
  }
  return "";
}

const DEFAULT_STORAGE_BUCKET = resolveDefaultStorageBucketName();
const GCP_PROJECT_ID = String(
  process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || process.env.PROJECT_ID || ""
).trim();
const SECRET_MANAGER_CLIENT = GCP_PROJECT_ID ? new SecretManagerServiceClient() : null;
const SECRET_NAME_CACHE = new Map<string, string>();
const SECRET_VALUE_PROMISE_CACHE = new Map<string, Promise<string>>();

if (!admin.apps.length) {
  const rawServiceAccount = String(process.env.FIREBASE_SERVICE_ACCOUNT_JSON || "").trim();
  const options: admin.AppOptions = DEFAULT_STORAGE_BUCKET ? { storageBucket: DEFAULT_STORAGE_BUCKET } : {};
  if (rawServiceAccount) {
    const parsed = JSON.parse(rawServiceAccount) as Record<string, string>;
    options.credential = admin.credential.cert({
      projectId: parsed.project_id || parsed.projectId,
      clientEmail: parsed.client_email || parsed.clientEmail,
      privateKey: parsed.private_key || parsed.privateKey,
    });
  }
  admin.initializeApp(options);
}

const db = admin.firestore();
const auth = admin.auth();
const messaging = admin.messaging();

const app = express();
app.disable("x-powered-by");
app.use(cors({ origin: true }));
app.use(express.json({ limit: "25mb" }));

type PostType = "forecast" | "backtest" | "agent" | "screener";

type Visibility = "public" | "unlisted" | "deleted";

type PostCounts = {
  likes: number;
  comments: number;
  reposts: number;
  shares: number;
  reports: number;
};

type PostDoc = {
  id: string;
  type: PostType;
  authorUid: string;
  authorHandle: string;
  authorPhotoURL: string;
  title: string;
  caption: string;
  body?: string;
  bodyFormat?: "markdown" | "text";
  tickers: string[];
  tags: string[];
  preview: {
    kind: "image" | "summary";
    imageUrl?: string;
    metrics?: Record<string, string | number>;
  };
  sourceRef?: {
    collection?: string;
    id?: string;
  };
  targetUrl: string;
  visibility: Visibility;
  createdAt: admin.firestore.Timestamp;
  updatedAt: admin.firestore.Timestamp;
  counts: PostCounts;
  score: number;
  lastEngagedAt: admin.firestore.Timestamp;
};

type ExploreCursor = {
  id: string;
  createdAtMs: number;
  score?: number;
};

const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 40;
const PUBLIC_ORIGIN = asString(process.env.PUBLIC_ORIGIN, "https://quantura.studio").replace(/\/$/, "");
const ADMIN_EMAIL = "tamzid257@gmail.com";
const MODEL_COUNCIL_RESPONSE_COLLECTION = "model_council_responses";
const OPENAI_API_KEY = resolveEnvSecret(["OPENAI_API_KEY", "OPENAI_SECRET_KEY", "OPENAI_KEY"], /^OPENAI_.*KEY$/i);
const CLAUDE_API_KEY = resolveEnvSecret(["CLAUDE_API_KEY", "ANTHROPIC_API_KEY"], /^(CLAUDE|ANTHROPIC)_.*KEY$/i);
const GEMINI_API_KEY = resolveEnvSecret(["GEMINI_API_KEY", "GOOGLE_GENAI_API_KEY"], /^GEMINI_.*KEY$/i);
const DEEPSEEK_API_KEY = resolveEnvSecret(["DEEPSEEK_API_KEY", "DEEPSEEK_SECRET_KEY"], /^DEEPSEEK_.*KEY$/i);
const MISTRAL_API_KEY = resolveEnvSecret(["MISTRAL_API_KEY", "MISTRAL_SECRET_KEY"], /^MISTRAL_.*KEY$/i);
const PERPLEXITY_API_KEY = resolveEnvSecret(["PERPLEXITY_API_KEY", "PERPLEXITY_SECRET_KEY"], /^PERPLEXITY_.*KEY$/i);
const QWEN_API_KEY = resolveEnvSecret(["QWEN_API_KEY", "QWEN_SECRET_KEY"], /^QWEN_.*KEY$/i);
const AMAZON_NOVA_API_KEY = resolveEnvSecret(["AMAZON_NOVA_API_KEY", "BEDROCK_API_KEY"], /^(AMAZON_NOVA|BEDROCK)_.*KEY$/i);
const AMAZON_NOVA_BASE_URL = asString(process.env.AMAZON_NOVA_BASE_URL).trim().replace(/\/$/, "");
const CLAUDE_API_VERSION = asString(process.env.CLAUDE_API_VERSION, "2023-06-01").trim();
const MODEL_COUNCIL_OTHER_API_KEY = resolveEnvSecret(
  ["MODEL_COUNCIL_OTHER_API_KEY", "MODEL_COUNCIL_OTHER_KEY"],
  /^MODEL_COUNCIL_OTHER_.*KEY$/i
);
const MODEL_COUNCIL_OTHER_BASE_URL = asString(process.env.MODEL_COUNCIL_OTHER_BASE_URL).trim().replace(/\/$/, "");
const NOTIFICATION_REWRITE_MODEL = asString(process.env.NOTIFICATION_REWRITE_MODEL, "gpt-4o-mini").trim();
const FMP_API_KEY = resolveEnvSecret(["FMP_API_KEY", "FMP_SECRET_KEY", "FMP_KEY"], /^FMP_.*_KEY$/i);
const PLAY_INTEGRITY_ANDROID_PACKAGE = asString(process.env.PLAY_INTEGRITY_ANDROID_PACKAGE).trim();
const REQUIRE_PLAY_INTEGRITY = asBoolean(process.env.REQUIRE_PLAY_INTEGRITY, false);
const IOS_IAP_WEBHOOK_SECRET = asString(process.env.IOS_IAP_WEBHOOK_SECRET).trim();
const APPLE_NOTIFICATIONS_WEBHOOK_SECRET = asString(process.env.APPLE_NOTIFICATIONS_WEBHOOK_SECRET).trim();
const ADMOB_SSV_WEBHOOK_SECRET = asString(process.env.ADMOB_SSV_WEBHOOK_SECRET).trim();
const GITHUB_ACTIONS_TOKEN = resolveEnvSecret(["GITHUB_ACTIONS_TOKEN", "GITHUB_TOKEN", "GH_TOKEN"], /^(GITHUB|GH).*TOKEN$/i);
const GITHUB_REPO_OWNER = asString(process.env.GITHUB_REPO_OWNER, "tamzid2001").trim() || "tamzid2001";
const GITHUB_REPO_NAME = asString(process.env.GITHUB_REPO_NAME, "stockssagemakerdata").trim() || "stockssagemakerdata";
const GITHUB_SCREENER_WORKFLOW = asString(process.env.GITHUB_SCREENER_WORKFLOW, "stock-screener.yml").trim() || "stock-screener.yml";
const GITHUB_ACTIONS_BRANCH = asString(process.env.GITHUB_ACTIONS_BRANCH, "main").trim() || "main";
const GITHUB_ACTIONS_API_BASE = `https://api.github.com/repos/${encodeURIComponent(GITHUB_REPO_OWNER)}/${encodeURIComponent(GITHUB_REPO_NAME)}`;
const DEFAULT_LLM_MODEL = asString(process.env.DEFAULT_LLM_MODEL, "gpt-5-mini").trim();
const LLM_TIMEOUT_MS = Math.max(5000, Math.min(120000, Math.floor(asFinite(process.env.LLM_TIMEOUT_MS, 30000))));
const PROMO_ID = asString(process.env.PROMO_ID, "quantura_generic_50_off").trim();
const PROMO_CODE = asString(process.env.PROMO_CODE, "QUANTURA50").trim().toUpperCase();
const PROMO_DISCOUNT_PERCENT = Math.max(1, Math.min(95, asFinite(process.env.PROMO_DISCOUNT_PERCENT, 50)));
const PROMO_ACTIVE = asBoolean(process.env.PROMO_ACTIVE, true);
const PROMO_DURATION_DAYS = Math.max(1, Math.min(120, Math.floor(asFinite(process.env.PROMO_DURATION_DAYS, 30))));
const GAMMA_API_BASE = "https://gamma-api.polymarket.com";
const POLYMARKET_CACHE_TTL_MS = 10 * 60 * 1000;
const POLYMARKET_CACHE_MAX_ENTRIES = 160;
const TICKER_INTEL_CACHE_TTL_MS = 10 * 60 * 1000;
const TICKER_INTEL_CACHE_MAX_ENTRIES = 200;
const TICKER_TRENDING_CACHE_TTL_MS = 5 * 60 * 1000;
const TICKER_TRENDING_CACHE_MAX_ENTRIES = 64;
const FX_RATE_CACHE_TTL_MS = 60 * 60 * 1000;
const FX_RATE_CACHE_MAX_ENTRIES = 120;
const FX_RATE_FETCH_TIMEOUT_MS = 7000;
const OUTPUT_META_RICH_TEXT_KEYS = new Set([
  "answer",
  "answerfull",
  "fullanswer",
  "body",
  "bodymarkdown",
  "markdown",
  "analysismarkdown",
  "narrative",
]);
const PROMO_START_MS = (() => {
  const raw = asString(process.env.PROMO_START_AT).trim();
  if (!raw) return Date.now() - 24 * 60 * 60 * 1000;
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : Date.now() - 24 * 60 * 60 * 1000;
})();
const PROMO_END_MS = (() => {
  const raw = asString(process.env.PROMO_END_AT).trim();
  if (raw) {
    const parsed = Date.parse(raw);
    if (Number.isFinite(parsed)) return parsed;
  }
  return PROMO_START_MS + PROMO_DURATION_DAYS * 24 * 60 * 60 * 1000;
})();

type SavedItemType = "forecast" | "screener" | "model_council" | "post";
type MyRequestType = "forecast" | "screener" | "indicator" | "modelCouncil";
type MyRequestShareVisibility = "private" | "unlisted" | "public";

type SystemFolderConfig = {
  id: string;
  displayName: string;
  flag: "liked" | "reposted" | "saved" | "shared";
};

type NotificationCategory = "watchlist" | "explore" | "earnings" | "ipo" | "daily" | "weekly" | "inactive";

type NotificationPrefs = {
  global: boolean;
  following: boolean;
  tickers: boolean;
  watchlist: boolean;
  explore: boolean;
  earnings: boolean;
  ipos: boolean;
  daily: boolean;
  weekly: boolean;
  inactiveHidden: boolean;
};

type PolymarketTopOutcome = {
  label: string;
  prob: number;
};

type PolymarketMarketRecord = {
  id: string;
  question: string;
  slug?: string;
  groupItemTitle?: string;
  description?: string;
  endDate?: string;
  category?: string;
  image?: string;
  icon?: string;
  volumeUsd?: number;
  liquidityUsd?: number;
  outcomes: string[];
  outcomePrices: number[];
  clobTokenIds: string[];
  isBinary: boolean;
  yesProb?: number;
  topOutcomes: PolymarketTopOutcome[];
  closed: boolean;
  active: boolean;
};

type PolymarketEventRecord = {
  id: string;
  title: string;
  slug?: string;
  ticker?: string;
  markets: PolymarketMarketRecord[];
};

type PolymarketSearchResponse = {
  query: string;
  fetchedAt: string;
  events: PolymarketEventRecord[];
};

type PolymarketPriceRecord = {
  eventId: string;
  eventTitle: string;
  eventSlug?: string;
  id: string;
  question: string;
  slug?: string;
  groupItemTitle?: string;
  description?: string;
  category?: string;
  endDate?: string;
  volumeUsd?: number;
  liquidityUsd?: number;
  outcomes: string[];
  outcomePrices: number[];
  clobTokenIds: string[];
  isBinary: boolean;
  yesProb?: number;
  topOutcomes: PolymarketTopOutcome[];
  active: boolean;
  closed: boolean;
};

type PolymarketCacheEntry = {
  expiresAtMs: number;
  value: PolymarketSearchResponse;
};

type FxRateCacheEntry = {
  expiresAtMs: number;
  value: {
    rate: number;
    asOf: string;
    symbolUsed: string;
    source: string;
  };
};

type MarketHeadlineFeedConfig = {
  id: string;
  providerId: string;
  providerLabel: string;
  feedLabel: string;
  url: string;
  sourceUrl: string;
  directoryUrl?: string;
  termsUrl?: string;
  attributionNote?: string;
};

type MarketHeadlineArticle = {
  title: string;
  summary: string;
  link: string;
  publisher: string;
  sourceLabel: string;
  publishedAt: string;
  thumbnailUrl?: string;
};

type TickerIntelCacheEntry = {
  expiresAtMs: number;
  value: Record<string, unknown>;
};

type TickerTrendingCacheEntry = {
  expiresAtMs: number;
  value: Record<string, unknown>;
};

const SYSTEM_FOLDERS: SystemFolderConfig[] = [
  { id: "liked-posts", displayName: "Liked posts", flag: "liked" },
  { id: "reposted-posts", displayName: "Reposted posts", flag: "reposted" },
  { id: "saved-posts", displayName: "Saved posts", flag: "saved" },
  { id: "shared-posts", displayName: "Shared posts", flag: "shared" },
];

const DEFAULT_NOTIFICATION_PREFS: NotificationPrefs = {
  global: true,
  following: true,
  tickers: true,
  watchlist: true,
  explore: true,
  earnings: true,
  ipos: true,
  daily: true,
  weekly: true,
  inactiveHidden: true,
};

const MY_REQUEST_TYPE_SET = new Set<MyRequestType>(["forecast", "screener", "indicator", "modelCouncil"]);
const MY_REQUEST_SHARE_VISIBILITY_SET = new Set<MyRequestShareVisibility>(["private", "unlisted", "public"]);
const MY_REQUEST_TYPE_LABEL: Record<MyRequestType, string> = {
  forecast: "Forecast",
  screener: "Screener",
  indicator: "Indicator",
  modelCouncil: "Model Council",
};

const ROUTES = express.Router();
const polymarketCache = new Map<string, PolymarketCacheEntry>();
const tickerIntelCache = new Map<string, TickerIntelCacheEntry>();
const tickerTrendingCache = new Map<string, TickerTrendingCacheEntry>();
const fxRateCache = new Map<string, FxRateCacheEntry>();
const MARKET_HEADLINE_DEFAULT_FEED_ID = "marketwatch_topstories";
const MARKET_HEADLINE_FETCH_TIMEOUT_MS = 12000;
const MARKET_HEADLINE_STALE_THRESHOLD_MS = 1000 * 60 * 60 * 24 * 7;
const MARKET_HEADLINE_PROVIDER_DEFAULTS = {
  cnn: "cnn_topstories",
  spglobal: "spglobal_research",
  mql5: "mql5_blogs",
  cnbc: "cnbc_market_insider",
  economictimes: "economictimes_stocks",
  investing: "investing_markets",
  seekingalpha: "seekingalpha_top",
  marketwatch: "marketwatch_topstories",
} as const;
const MARKET_HEADLINE_FEEDS: Record<string, MarketHeadlineFeedConfig> = {
  cnn_topstories: {
    id: "cnn_topstories",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "Top Stories",
    url: "http://rss.cnn.com/rss/cnn_topstories.rss",
    sourceUrl: "http://rss.cnn.com/rss/cnn_topstories.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_world: {
    id: "cnn_world",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "World",
    url: "http://rss.cnn.com/rss/cnn_world.rss",
    sourceUrl: "http://rss.cnn.com/rss/cnn_world.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_us: {
    id: "cnn_us",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "U.S.",
    url: "http://rss.cnn.com/rss/cnn_us.rss",
    sourceUrl: "http://rss.cnn.com/rss/cnn_us.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_business: {
    id: "cnn_business",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "Business",
    url: "http://rss.cnn.com/rss/money_latest.rss",
    sourceUrl: "http://rss.cnn.com/rss/money_latest.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_politics: {
    id: "cnn_politics",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "Politics",
    url: "http://rss.cnn.com/rss/cnn_allpolitics.rss",
    sourceUrl: "http://rss.cnn.com/rss/cnn_allpolitics.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_technology: {
    id: "cnn_technology",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "Technology",
    url: "http://rss.cnn.com/rss/cnn_tech.rss",
    sourceUrl: "http://rss.cnn.com/rss/cnn_tech.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_health: {
    id: "cnn_health",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "Health",
    url: "http://rss.cnn.com/rss/cnn_health.rss",
    sourceUrl: "http://rss.cnn.com/rss/cnn_health.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_entertainment: {
    id: "cnn_entertainment",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "Entertainment",
    url: "http://rss.cnn.com/rss/cnn_showbiz.rss",
    sourceUrl: "http://rss.cnn.com/rss/cnn_showbiz.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_travel: {
    id: "cnn_travel",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "Travel",
    url: "http://rss.cnn.com/rss/cnn_travel.rss",
    sourceUrl: "http://rss.cnn.com/rss/cnn_travel.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_video: {
    id: "cnn_video",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "Video",
    url: "http://rss.cnn.com/rss/cnn_freevideo.rss",
    sourceUrl: "http://rss.cnn.com/rss/cnn_freevideo.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn10: {
    id: "cnn10",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "CNN 10",
    url: "http://rss.cnn.com/services/podcasting/cnn10/rss.xml",
    sourceUrl: "http://rss.cnn.com/services/podcasting/cnn10/rss.xml",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_latest: {
    id: "cnn_latest",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "Most Recent",
    url: "http://rss.cnn.com/rss/cnn_latest.rss",
    sourceUrl: "http://rss.cnn.com/rss/cnn_latest.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  cnn_underscored: {
    id: "cnn_underscored",
    providerId: "cnn",
    providerLabel: "CNN RSS",
    feedLabel: "CNN Underscored",
    url: "http://rss.cnn.com/cnn-underscored.rss",
    sourceUrl: "http://rss.cnn.com/cnn-underscored.rss",
    directoryUrl: "http://www.cnn.com/services/rss/",
    termsUrl: "https://www.cnn.com/terms",
    attributionNote: "CNN RSS feed access is subject to CNN terms of use.",
  },
  spglobal_research: {
    id: "spglobal_research",
    providerId: "spglobal",
    providerLabel: "S&P DJI RSS",
    feedLabel: "Research",
    url: "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=research",
    sourceUrl: "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=research",
    directoryUrl: "https://www.spglobal.com/spdji/en/rss/",
    attributionNote: "S&P DJI may block automated access for some RSS endpoints.",
  },
  spglobal_commentary: {
    id: "spglobal_commentary",
    providerId: "spglobal",
    providerLabel: "S&P DJI RSS",
    feedLabel: "Commentary",
    url: "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=commentary",
    sourceUrl: "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=commentary",
    directoryUrl: "https://www.spglobal.com/spdji/en/rss/",
    attributionNote: "S&P DJI may block automated access for some RSS endpoints.",
  },
  spglobal_index_launches: {
    id: "spglobal_index_launches",
    providerId: "spglobal",
    providerLabel: "S&P DJI RSS",
    feedLabel: "Index launches",
    url: "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=index-launches",
    sourceUrl: "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=index-launches",
    directoryUrl: "https://www.spglobal.com/spdji/en/rss/",
    attributionNote: "S&P DJI may block automated access for some RSS endpoints.",
  },
  spglobal_index_announcements: {
    id: "spglobal_index_announcements",
    providerId: "spglobal",
    providerLabel: "S&P DJI RSS",
    feedLabel: "Index announcements",
    url: "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=index-announcements",
    sourceUrl: "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=index-announcements",
    directoryUrl: "https://www.spglobal.com/spdji/en/rss/",
    attributionNote: "S&P DJI may block automated access for some RSS endpoints.",
  },
  spglobal_consultations: {
    id: "spglobal_consultations",
    providerId: "spglobal",
    providerLabel: "S&P DJI RSS",
    feedLabel: "Consultations",
    url: "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=consultations",
    sourceUrl: "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=consultations",
    directoryUrl: "https://www.spglobal.com/spdji/en/rss/",
    attributionNote: "S&P DJI may block automated access for some RSS endpoints.",
  },
  mql5_blogs: {
    id: "mql5_blogs",
    providerId: "mql5",
    providerLabel: "MQL5 Blogs RSS",
    feedLabel: "Latest blogs",
    url: "https://www.mql5.com/en/blogs/rss",
    sourceUrl: "https://www.mql5.com/en/blogs/rss",
    attributionNote: "MQL5 may restrict automated RSS access from some environments.",
  },
  cnbc_market_insider: {
    id: "cnbc_market_insider",
    providerId: "cnbc",
    providerLabel: "CNBC Market Insider",
    feedLabel: "Market Insider",
    url: "https://www.cnbc.com/id/20409666/device/rss/rss.html?x=1",
    sourceUrl: "https://www.cnbc.com/id/20409666/device/rss/rss.html?x=1",
    attributionNote: "CNBC Market Insider feed provided through CNBC RSS.",
  },
  economictimes_stocks: {
    id: "economictimes_stocks",
    providerId: "economictimes",
    providerLabel: "Economic Times Stocks",
    feedLabel: "Stocks",
    url: "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
    sourceUrl: "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
  },
  investing_markets: {
    id: "investing_markets",
    providerId: "investing",
    providerLabel: "Investing.com Markets",
    feedLabel: "Market news",
    url: "https://www.investing.com/rss/news_25.rss",
    sourceUrl: "https://www.investing.com/rss/news_25.rss",
  },
  seekingalpha_top: {
    id: "seekingalpha_top",
    providerId: "seekingalpha",
    providerLabel: "Seeking Alpha",
    feedLabel: "Top feed",
    url: "https://seekingalpha.com/feed.xml",
    sourceUrl: "https://seekingalpha.com/feed.xml",
  },
  marketwatch_topstories: {
    id: "marketwatch_topstories",
    providerId: "marketwatch",
    providerLabel: "MarketWatch Top Stories",
    feedLabel: "Top stories",
    url: "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    sourceUrl: "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    attributionNote: "MarketWatch top stories are distributed through the Dow Jones public RSS feed.",
  },
};
const PLAY_INTEGRITY_AUTH = new GoogleAuth({
  scopes: ["https://www.googleapis.com/auth/playintegrity"],
});
const GOOGLE_PLAY_PUBLISHER_AUTH = new GoogleAuth({
  scopes: ["https://www.googleapis.com/auth/androidpublisher"],
});

const AUTOMATION_PRODUCT_ID = "quantura_automation_unlock";
const AUTOMATION_MAX_ACTIVE = 2;
const AUTOMATION_COLLECTION = "mobile_automations";
const AUTOMATION_ENTITLEMENT_COLLECTION = "mobile_automation_entitlements";
const AUTOMATION_HISTORY_SUBCOLLECTION = "runs";
const AUTOMATION_ALLOWED_CADENCES = new Set(["daily"]);
const AUTOMATION_ALLOWED_HORIZONS = new Set([
  "3_days",
  "1_week",
  "2_weeks",
  "3_weeks",
  "1_month",
  "3_months",
  "6_months",
  "1_year",
]);
const AUTOMATION_ALLOWED_PROFILES = new Set(["balanced", "avg_wql", "conservative", "aggressive"]);
const AUTOMATION_ALLOWED_MODELS = new Set(["autopilot", "avg_wql_all_algorithms"]);
const APPLE_IAP_BUNDLE_ID = asString(process.env.APPLE_IAP_BUNDLE_ID, "com.quantura.quanturaapp").trim();
const APPLE_IAP_ENVIRONMENT = asString(process.env.APPLE_IAP_ENVIRONMENT, "Production").trim() || "Production";
const APPLE_IAP_ISSUER_ID = asString(process.env.APPLE_IAP_ISSUER_ID).trim();
const APPLE_IAP_KEY_ID = asString(process.env.APPLE_IAP_KEY_ID).trim();
const APPLE_IAP_PRIVATE_KEY = asString(process.env.APPLE_IAP_PRIVATE_KEY)
  .replace(/\\n/g, "\n")
  .trim();
const GOOGLE_PLAY_ANDROID_PACKAGE = asString(process.env.GOOGLE_PLAY_ANDROID_PACKAGE, "com.quantura.quanturaapp").trim();
const AUTOMATION_EMAIL_FROM = asString(process.env.AUTOMATION_EMAIL_FROM, "hell@quantura.studio").trim() || "hell@quantura.studio";
const AUTOMATION_EMAIL_REPLY_TO = asString(process.env.AUTOMATION_EMAIL_REPLY_TO, AUTOMATION_EMAIL_FROM).trim() || AUTOMATION_EMAIL_FROM;
const RESEND_API_KEY = asString(process.env.RESEND_API_KEY).trim();

registerFiscalDataRoutes(ROUTES, { db });

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function asFinite(value: unknown, fallback = 0): number {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function asBoolean(value: unknown, fallback = false): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
  }
  return fallback;
}

function resolveEnvSecret(preferredKeys: string[], pattern?: RegExp): string {
  for (const key of preferredKeys) {
    const value = asString(process.env[key]).trim();
    if (value) return value;
  }
  if (!pattern) return "";
  const matches = Object.entries(process.env)
    .filter(([key, value]) => pattern.test(key) && asString(value).trim().length > 0)
    .sort(([a], [b]) => a.localeCompare(b));
  if (!matches.length) return "";
  return asString(matches[0][1]).trim();
}

async function readSecretManagerValue(secretName: string): Promise<string> {
  if (!SECRET_MANAGER_CLIENT || !GCP_PROJECT_ID || !secretName) return "";
  try {
    const resource = `projects/${GCP_PROJECT_ID}/secrets/${secretName}/versions/latest`;
    const [version] = await SECRET_MANAGER_CLIENT.accessSecretVersion({ name: resource });
    const rawBytes = version.payload?.data;
    const raw = rawBytes ? Buffer.from(rawBytes).toString("utf8") : "";
    return raw.trim();
  } catch (error: any) {
    const message = asString(error?.message).toLowerCase();
    if (message.includes("not found") || message.includes("permission")) return "";
    console.warn(`[LLM] secret lookup failed for ${secretName}:`, error?.message || error);
    return "";
  }
}

async function discoverSecretValueByPattern(pattern: RegExp): Promise<string> {
  if (!SECRET_MANAGER_CLIENT || !GCP_PROJECT_ID) return "";
  const cacheKey = pattern.source;
  const cachedName = SECRET_NAME_CACHE.get(cacheKey);
  if (cachedName) {
    return readSecretManagerValue(cachedName);
  }
  try {
    const [secrets] = await SECRET_MANAGER_CLIENT.listSecrets({
      parent: `projects/${GCP_PROJECT_ID}`,
    });
    const names = (secrets || [])
      .map((secret) => asString(secret.name).split("/").pop() || "")
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b));
    const matched = names.find((name) => pattern.test(name));
    if (!matched) return "";
    SECRET_NAME_CACHE.set(cacheKey, matched);
    return readSecretManagerValue(matched);
  } catch (error: any) {
    console.warn("[LLM] secret discovery failed:", error?.message || error);
    return "";
  }
}

async function resolveRuntimeSecretValue(
  cacheKey: string,
  envKeys: string[],
  secretNames: string[],
  discoverPatterns: RegExp[] = []
): Promise<string> {
  const cached = SECRET_VALUE_PROMISE_CACHE.get(cacheKey);
  if (cached) return cached;

  const promise = (async () => {
    const fromEnv = readEnvSecret(envKeys);
    if (fromEnv) return fromEnv;

    for (const secretName of secretNames) {
      const fromManager = await readSecretManagerValue(secretName);
      if (fromManager) return fromManager;
    }

    for (const pattern of discoverPatterns) {
      const discovered = await discoverSecretValueByPattern(pattern);
      if (discovered) return discovered;
    }
    return "";
  })();

  SECRET_VALUE_PROMISE_CACHE.set(cacheKey, promise);
  return promise;
}

function readEnvSecret(keys: string[]): string {
  for (const key of keys) {
    const value = asString(process.env[key]).trim();
    if (value) return value;
  }
  return "";
}

function requestIpAddress(req: Request): string {
  const forwarded = asString(req.headers["x-forwarded-for"]);
  if (forwarded) {
    const first = forwarded.split(",")[0]?.trim() || "";
    if (first) return first.slice(0, 120);
  }
  const real = asString(req.headers["x-real-ip"]);
  if (real) return real.slice(0, 120);
  const socketIp = asString((req.socket as any)?.remoteAddress);
  return socketIp.slice(0, 120);
}

function normalizeTimezone(value: unknown): string {
  return sanitizeText(value, 80).replace(/[^A-Za-z0-9_./+\-]/g, "");
}

function normalizeCoarseLocation(
  value: unknown
): { lat: number | null; lon: number | null; countryCode: string; accuracyM: number | null; capturedAt: string } | null {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  const latNum = asFinite(payload.lat, NaN);
  const lonNum = asFinite(payload.lon, NaN);
  const accNum = asFinite(payload.accuracyM, NaN);
  const countryRaw = asString(payload.countryCode).trim().toUpperCase();
  const countryCode = /^[A-Z]{2}$/.test(countryRaw) ? countryRaw : "";
  const captured = asString(payload.capturedAt);
  const capturedAt = Number.isFinite(Date.parse(captured)) ? new Date(captured).toISOString() : new Date().toISOString();
  return {
    lat: Number.isFinite(latNum) ? Number(latNum.toFixed(1)) : null,
    lon: Number.isFinite(lonNum) ? Number(lonNum.toFixed(1)) : null,
    countryCode,
    accuracyM: Number.isFinite(accNum) ? Math.max(0, Math.round(accNum)) : null,
    capturedAt,
  };
}

async function fetchIpDerivedRegion(ipAddress: string): Promise<{ region: string; countryCode: string }> {
  const ip = String(ipAddress || "").trim();
  if (!ip) return { region: "", countryCode: "" };
  const safeIp = ip.replace(/[^0-9a-fA-F:.]/g, "");
  if (!safeIp) return { region: "", countryCode: "" };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 2500);
  try {
    const response = await fetch(`https://ipapi.co/${encodeURIComponent(safeIp)}/json/`, {
      method: "GET",
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
    if (!response.ok) return { region: "", countryCode: "" };
    const payload = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    const region = sanitizeText(payload.region || payload.region_code || payload.city || "", 80);
    const countryRaw = asString(payload.country_code || payload.country).trim().toUpperCase();
    const countryCode = /^[A-Z]{2}$/.test(countryRaw) ? countryRaw : "";
    return { region, countryCode };
  } catch {
    return { region: "", countryCode: "" };
  } finally {
    clearTimeout(timer);
  }
}

function parseJsonObject(text: string): Record<string, unknown> | null {
  const raw = String(text || "").trim();
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") return parsed as Record<string, unknown>;
  } catch {
    // fall through
  }
  const firstBrace = raw.indexOf("{");
  const lastBrace = raw.lastIndexOf("}");
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    const slice = raw.slice(firstBrace, lastBrace + 1);
    try {
      const parsed = JSON.parse(slice);
      if (parsed && typeof parsed === "object") return parsed as Record<string, unknown>;
    } catch {
      return null;
    }
  }
  return null;
}

type NotificationRewriteInput = {
  title: string;
  body: string;
  source: string;
  context?: Record<string, unknown>;
};

async function rewriteNotificationWithLlm(input: NotificationRewriteInput): Promise<{
  title: string;
  body: string;
  nextSteps: string[];
  personalized: boolean;
}> {
  const title = sanitizeText(input.title, 160) || "Quantura update";
  const body = sanitizeText(input.body, 500);
  const fallback = {
    title,
    body: body || "You have a new Quantura notification.",
    nextSteps: [] as string[],
    personalized: false,
  };
  if (!OPENAI_API_KEY) return fallback;

  const timezone = sanitizeText(input.context?.timezone, 80);
  const country = sanitizeText(input.context?.countryCode, 12).toUpperCase();
  const region = sanitizeText(input.context?.region, 80);
  const source = sanitizeText(input.source, 40);

  const prompt = {
    title,
    body,
    source,
    context: {
      timezone,
      countryCode: country,
      region,
    },
    style: "Keep it short and practical. Return JSON only.",
    outputSchema: {
      title: "string (<=90 chars)",
      body: "string (<=180 chars)",
      nextSteps: ["array of 0-3 short suggestions"],
    },
  };

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: NOTIFICATION_REWRITE_MODEL,
        temperature: 0.2,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content:
              "You rewrite notification text for a finance app. Keep language plain and concise. Never mention policy text. Respond as valid JSON.",
          },
          {
            role: "user",
            content: JSON.stringify(prompt),
          },
        ],
      }),
    });
    if (!response.ok) return fallback;
    const payload = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    const content = asString((payload?.choices as any)?.[0]?.message?.content);
    const parsed = parseJsonObject(content);
    if (!parsed) return fallback;
    const nextTitle = sanitizeText(parsed.title, 90) || fallback.title;
    const nextBody = sanitizeText(parsed.body, 180) || fallback.body;
    const nextStepsRaw = Array.isArray(parsed.nextSteps) ? parsed.nextSteps : [];
    const nextSteps = nextStepsRaw.map((item) => sanitizeText(item, 90)).filter(Boolean).slice(0, 3);
    return {
      title: nextTitle,
      body: nextBody,
      nextSteps,
      personalized: true,
    };
  } catch {
    return fallback;
  }
}

function parseLimit(input: unknown): number {
  const parsed = Number(input);
  if (!Number.isFinite(parsed)) return DEFAULT_LIMIT;
  return Math.max(1, Math.min(MAX_LIMIT, Math.floor(parsed)));
}

function normalizeTicker(value: unknown): string {
  const raw = asString(value).trim().toUpperCase();
  if (!raw) return "";
  return raw.replace(/[^A-Z0-9.\-]/g, "").slice(0, 12);
}

function extractYahooFieldValue(value: unknown): unknown {
  if (!value || typeof value !== "object") return value;
  const payload = value as Record<string, unknown>;
  if (payload.raw !== undefined && payload.raw !== null) return payload.raw;
  if (payload.fmt !== undefined && payload.fmt !== null) return payload.fmt;
  if (payload.longFmt !== undefined && payload.longFmt !== null) return payload.longFmt;
  return value;
}

function extractYahooNumber(value: unknown): number | null {
  const raw = extractYahooFieldValue(value);
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function extractYahooText(value: unknown, maxLen = 280): string {
  return sanitizeText(extractYahooFieldValue(value), maxLen);
}

function buildLogoUrlFromWebsite(website: unknown): string {
  const text = sanitizeText(website, 300);
  if (!text) return "";
  try {
    const parsed = new URL(text.startsWith("http") ? text : `https://${text}`);
    const host = sanitizeText(parsed.hostname, 220).replace(/^www\./i, "");
    if (!host) return "";
    return `https://logo.clearbit.com/${host}`;
  } catch {
    return "";
  }
}

function computeSignalScore(value: number | null, min: number, max: number, invert = false): number | null {
  if (value === null || !Number.isFinite(value)) return null;
  if (max <= min) return null;
  const normalized = Math.max(0, Math.min(1, (value - min) / (max - min)));
  const score = invert ? 100 - normalized * 100 : normalized * 100;
  return Math.max(0, Math.min(100, Number(score.toFixed(1))));
}

function getTickerIntelCache(ticker: string): Record<string, unknown> | null {
  const key = normalizeTicker(ticker);
  if (!key) return null;
  const cached = tickerIntelCache.get(key);
  if (!cached) return null;
  if (cached.expiresAtMs <= Date.now()) {
    tickerIntelCache.delete(key);
    return null;
  }
  return cached.value;
}

function setTickerIntelCache(ticker: string, value: Record<string, unknown>): void {
  const key = normalizeTicker(ticker);
  if (!key) return;
  tickerIntelCache.set(key, {
    expiresAtMs: Date.now() + TICKER_INTEL_CACHE_TTL_MS,
    value,
  });
  while (tickerIntelCache.size > TICKER_INTEL_CACHE_MAX_ENTRIES) {
    const oldestKey = tickerIntelCache.keys().next().value;
    if (!oldestKey) break;
    tickerIntelCache.delete(oldestKey);
  }
}

function normalizeRemoteLogoUrl(value: unknown): string {
  const text = sanitizeText(value, 500);
  if (!/^https?:\/\//i.test(text)) return "";
  return text;
}

function getTickerTrendingCache(cacheKey: string): Record<string, unknown> | null {
  const key = sanitizeText(cacheKey, 120);
  if (!key) return null;
  const cached = tickerTrendingCache.get(key);
  if (!cached) return null;
  if (cached.expiresAtMs <= Date.now()) {
    tickerTrendingCache.delete(key);
    return null;
  }
  return cached.value;
}

function setTickerTrendingCache(cacheKey: string, value: Record<string, unknown>): void {
  const key = sanitizeText(cacheKey, 120);
  if (!key) return;
  tickerTrendingCache.set(key, {
    expiresAtMs: Date.now() + TICKER_TRENDING_CACHE_TTL_MS,
    value,
  });
  while (tickerTrendingCache.size > TICKER_TRENDING_CACHE_MAX_ENTRIES) {
    const oldestKey = tickerTrendingCache.keys().next().value;
    if (!oldestKey) break;
    tickerTrendingCache.delete(oldestKey);
  }
}

function chunkSymbols(symbols: string[], chunkSize = 12): string[][] {
  const size = Math.max(1, Math.floor(chunkSize));
  const out: string[][] = [];
  for (let index = 0; index < symbols.length; index += size) {
    out.push(symbols.slice(index, index + size));
  }
  return out;
}

async function fetchYahooTrendingSymbols(region: string, limit: number): Promise<string[]> {
  const safeRegion = sanitizeText(region, 10).toUpperCase().replace(/[^A-Z]/g, "") || "US";
  const url = `https://query1.finance.yahoo.com/v1/finance/trending/${encodeURIComponent(safeRegion)}`;
  const payloadRaw = await fetchJsonWithTimeout(url, 7000);
  const payload = payloadRaw && typeof payloadRaw === "object" ? (payloadRaw as Record<string, unknown>) : {};
  const quotesRaw = ((((payload.finance as any)?.result as Array<Record<string, unknown>> | undefined) || [])[0] as
    | Record<string, unknown>
    | undefined)?.quotes;
  const quotes = Array.isArray(quotesRaw) ? quotesRaw : [];
  const symbols = Array.from(
    new Set(
      quotes
        .map((row) => normalizeTicker((row as Record<string, unknown>)?.symbol))
        .filter((symbol) => Boolean(symbol))
    )
  );
  return symbols.slice(0, Math.max(1, Math.min(40, Math.floor(limit))));
}

async function fetchYahooSparkMetaBySymbols(symbols: string[]): Promise<Map<string, Record<string, unknown>>> {
  const out = new Map<string, Record<string, unknown>>();
  const unique = Array.from(new Set((Array.isArray(symbols) ? symbols : []).map((symbol) => normalizeTicker(symbol)).filter(Boolean)));
  if (!unique.length) return out;

  const chunks = chunkSymbols(unique, 12);
  await Promise.all(
    chunks.map(async (chunk) => {
      if (!chunk.length) return;
      const url = `https://query1.finance.yahoo.com/v7/finance/spark?symbols=${encodeURIComponent(
        chunk.join(",")
      )}&range=1d&interval=5m`;
      const payloadRaw = await fetchJsonWithTimeout(url, 7000).catch(() => null);
      if (!payloadRaw || typeof payloadRaw !== "object") return;
      const payload = payloadRaw as Record<string, unknown>;
      const result = (((payload.spark as any)?.result as Array<Record<string, unknown>> | undefined) || []).filter(Boolean);
      result.forEach((entry) => {
        const symbol = normalizeTicker(entry.symbol || (entry as any)?.meta?.symbol);
        if (!symbol) return;
        const response = Array.isArray(entry.response) ? entry.response : [];
        const meta =
          response.length && response[0] && typeof response[0] === "object"
            ? ((response[0] as Record<string, unknown>).meta as Record<string, unknown>) || {}
            : {};
        if (meta && typeof meta === "object" && Object.keys(meta).length) {
          out.set(symbol, meta);
        }
      });
    })
  );

  return out;
}

async function fetchFmpTickerLogoMap(
  symbols: string[]
): Promise<Map<string, { logoUrl: string; website: string; companyName: string }>> {
  const out = new Map<string, { logoUrl: string; website: string; companyName: string }>();
  if (!FMP_API_KEY) return out;
  const unique = Array.from(new Set((Array.isArray(symbols) ? symbols : []).map((symbol) => normalizeTicker(symbol)).filter(Boolean))).slice(
    0,
    24
  );
  if (!unique.length) return out;

  await Promise.all(
    unique.map(async (symbol) => {
      const url = `https://financialmodelingprep.com/stable/profile?symbol=${encodeURIComponent(symbol)}&apikey=${encodeURIComponent(
        FMP_API_KEY
      )}`;
      const payloadRaw = await fetchJsonWithTimeout(url, 7000).catch(() => null);
      const rows = Array.isArray(payloadRaw)
        ? (payloadRaw as Array<Record<string, unknown>>)
        : payloadRaw && typeof payloadRaw === "object"
          ? [payloadRaw as Record<string, unknown>]
          : [];
      if (!rows.length) return;
      const matched = rows.find((row) => normalizeTicker(row.symbol || row.ticker) === symbol) || rows[0] || {};
      const website = sanitizeText(matched.website, 280);
      const directLogo =
        normalizeRemoteLogoUrl(matched.image) ||
        normalizeRemoteLogoUrl(matched.logoUrl) ||
        normalizeRemoteLogoUrl(matched.logo_url) ||
        normalizeRemoteLogoUrl(matched.logo);
      const logoUrl = directLogo || buildLogoUrlFromWebsite(website);
      const companyName = sanitizeText(matched.companyName || matched.name || matched.longName || "", 180);
      if (!logoUrl && !website && !companyName) return;
      out.set(symbol, { logoUrl, website, companyName });
    })
  );

  return out;
}

async function fetchFmpRows(urls: string[], timeoutMs = 7000): Promise<Array<Record<string, unknown>>> {
  for (const url of urls) {
    const payloadRaw = await fetchJsonWithTimeout(url, timeoutMs).catch(() => null);
    const rows = Array.isArray(payloadRaw)
      ? (payloadRaw as Array<Record<string, unknown>>)
      : payloadRaw && typeof payloadRaw === "object"
        ? [payloadRaw as Record<string, unknown>]
        : [];
    if (rows.length) return rows;
  }
  return [];
}

function parseFmpRangeEdge(rangeText: unknown, index: 0 | 1): number | null {
  const raw = sanitizeText(rangeText, 80);
  if (!raw.includes("-")) return null;
  const values = raw.split("-").map((part) => Number(String(part).trim()));
  const value = values[index];
  return Number.isFinite(value) ? value : null;
}

function normalizeDividendYield(value: unknown): number | null {
  const num = asFinite(value, NaN);
  if (!Number.isFinite(num) || num < 0) return null;
  return num > 1 ? num / 100 : num;
}

async function fetchFmpTickerIntelFallback(ticker: string): Promise<Record<string, unknown> | null> {
  if (!FMP_API_KEY) return null;
  const cleanTicker = normalizeTicker(ticker);
  if (!cleanTicker) return null;

  const profileRows = await fetchFmpRows([
    `https://financialmodelingprep.com/stable/profile?symbol=${encodeURIComponent(cleanTicker)}&apikey=${encodeURIComponent(FMP_API_KEY)}`,
    `https://financialmodelingprep.com/api/v3/profile/${encodeURIComponent(cleanTicker)}?apikey=${encodeURIComponent(FMP_API_KEY)}`,
  ]);
  const quoteRows = await fetchFmpRows([
    `https://financialmodelingprep.com/stable/quote?symbol=${encodeURIComponent(cleanTicker)}&apikey=${encodeURIComponent(FMP_API_KEY)}`,
    `https://financialmodelingprep.com/api/v3/quote/${encodeURIComponent(cleanTicker)}?apikey=${encodeURIComponent(FMP_API_KEY)}`,
  ]);

  const profileRow = profileRows.find((row) => normalizeTicker(row.symbol || row.ticker) === cleanTicker) || profileRows[0] || {};
  const quoteRow = quoteRows.find((row) => normalizeTicker(row.symbol || row.ticker) === cleanTicker) || quoteRows[0] || {};
  if (!Object.keys(profileRow).length && !Object.keys(quoteRow).length) return null;

  const website = sanitizeText(profileRow.website, 280);
  const logoUrl =
    normalizeRemoteLogoUrl(profileRow.image) ||
    normalizeRemoteLogoUrl(profileRow.logoUrl) ||
    normalizeRemoteLogoUrl(profileRow.logo_url) ||
    normalizeRemoteLogoUrl(profileRow.logo) ||
    buildLogoUrlFromWebsite(website);
  const companyName =
    sanitizeText(profileRow.companyName || profileRow.name || quoteRow.name || quoteRow.companyName || "", 180) || cleanTicker;
  const marketCapRaw = asFinite(quoteRow.marketCap ?? profileRow.mktCap ?? profileRow.marketCap, NaN);
  const marketCap = Number.isFinite(marketCapRaw) ? marketCapRaw : null;
  const lastRaw = asFinite(quoteRow.price ?? profileRow.price, NaN);
  const last = Number.isFinite(lastRaw) ? lastRaw : null;
  const prevCloseRaw = asFinite(quoteRow.previousClose ?? quoteRow.previous_close, NaN);
  const prevClose = Number.isFinite(prevCloseRaw) ? prevCloseRaw : null;
  const dayLowRaw = asFinite(quoteRow.dayLow ?? quoteRow.low, NaN);
  const dayLow = Number.isFinite(dayLowRaw) ? dayLowRaw : null;
  const dayHighRaw = asFinite(quoteRow.dayHigh ?? quoteRow.high, NaN);
  const dayHigh = Number.isFinite(dayHighRaw) ? dayHighRaw : null;
  const volumeRaw = asFinite(quoteRow.volume ?? quoteRow.avgVolume, NaN);
  const volume = Number.isFinite(volumeRaw) ? volumeRaw : null;
  const avgVolumeRaw = asFinite(quoteRow.avgVolume ?? quoteRow.volume, NaN);
  const avgVolume = Number.isFinite(avgVolumeRaw) ? avgVolumeRaw : null;
  const yearLow = (() => {
    const num = asFinite(quoteRow.yearLow, NaN);
    if (Number.isFinite(num)) return num;
    return parseFmpRangeEdge(profileRow.range, 0);
  })();
  const yearHigh = (() => {
    const num = asFinite(quoteRow.yearHigh, NaN);
    if (Number.isFinite(num)) return num;
    return parseFmpRangeEdge(profileRow.range, 1);
  })();
  const betaRaw = asFinite(quoteRow.beta ?? profileRow.beta, NaN);
  const beta = Number.isFinite(betaRaw) ? betaRaw : null;
  const trailingPeRaw = asFinite(quoteRow.pe ?? profileRow.pe, NaN);
  const trailingPE = Number.isFinite(trailingPeRaw) ? trailingPeRaw : null;
  const priceToBookRaw = asFinite(profileRow.priceToBookRatio ?? profileRow.priceToBook, NaN);
  const priceToBook = Number.isFinite(priceToBookRaw) ? priceToBookRaw : null;
  const sharesOutstandingRaw = asFinite(quoteRow.sharesOutstanding ?? profileRow.sharesOutstanding, NaN);
  const sharesOutstanding = Number.isFinite(sharesOutstandingRaw) ? sharesOutstandingRaw : null;
  const dividendRateRaw = asFinite(profileRow.lastDiv ?? quoteRow.lastDiv, NaN);
  const dividendRate = Number.isFinite(dividendRateRaw) ? dividendRateRaw : null;
  const dividendYield = normalizeDividendYield(profileRow.dividendYield ?? quoteRow.dividendYield);
  const currency = sanitizeText(quoteRow.currency || profileRow.currency, 20);
  const exchange = sanitizeText(profileRow.exchangeShortName || profileRow.exchange || quoteRow.exchange, 120);
  const sector = sanitizeText(profileRow.sector, 120);
  const industry = sanitizeText(profileRow.industry, 120);
  const country = sanitizeText(profileRow.country, 120);
  const summary = sanitizeText(profileRow.description || profileRow.descriptionShort || "", 2000);

  return {
    ticker: cleanTicker,
    source: "fmp_quote_profile_fallback",
    fetchedAt: new Date().toISOString(),
    price: {
      last,
      prevClose,
      dayLow,
      dayHigh,
      volume,
      currency,
    },
    logoUrl,
    logo_url: logoUrl,
    profile: {
      name: companyName,
      sector,
      industry,
      exchange,
      currency,
      website,
      summary,
      marketCap,
      fiftyTwoWeekLow: yearLow,
      fiftyTwoWeekHigh: yearHigh,
      trailingPE,
      forwardPE: null,
      beta,
      dividendYield,
      logoUrl,
      logo_url: logoUrl,
    },
    profileDetails: {
      longName: companyName,
      sector,
      industry,
      country,
      website,
      longBusinessSummary: summary,
      logoUrl,
      logo_url: logoUrl,
    },
    valuation: {
      marketCap,
      trailingPE,
      forwardPE: null,
      priceToBook,
      enterpriseValue: null,
    },
    fundamentals: {
      revenueTTM: null,
      grossMargins: null,
      profitMargins: null,
      operatingMargins: null,
      ebitdaMargins: null,
      returnOnAssets: null,
      returnOnEquity: null,
    },
    risk: {
      beta,
      shortRatio: null,
    },
    dividends: {
      dividendRate,
      dividendYield,
      payoutRatio: null,
      exDividendDate: "",
    },
    trading: {
      beta,
      fiftyTwoWeekLow: yearLow,
      fiftyTwoWeekHigh: yearHigh,
      avgVolume,
      sharesOutstanding,
    },
    events: [],
    analyst: {
      recommendationKey: "",
      recommendationMean: null,
      analystOpinions: null,
      targetMeanPrice: null,
      targetLowPrice: null,
      targetHighPrice: null,
    },
    recommendationTrend: [],
    executiveSummary: {
      ticker: cleanTicker,
      exchange,
      sector,
      priceTarget12m: null,
    },
    fundamentalDeepDive: {
      revenueMechanics: {
        totalRevenue: null,
        grossProfit: null,
        segmentBreakdown: "Detailed segment data was unavailable in the fallback market data source.",
      },
      profitability: {
        netMargin: null,
        roi: null,
      },
      capitalAllocation: {
        dividendPolicy: dividendRate === null ? "No dividend policy reported in the fallback source." : `Recent dividend rate reported: ${dividendRate}`,
        shareBuybacks: "Buyback detail was unavailable in the fallback market data source.",
      },
    },
    riskAndEsg: {
      riskMitigation: "Fallback profile sourced from FMP because Yahoo quote summary was unavailable.",
      liquidity: {
        totalCash: null,
        totalDebt: null,
        currentRatio: null,
      },
      esg: {
        environmental: null,
        social: null,
        governance: null,
        overall: null,
      },
    },
    balanceSheetHeatmap: [],
    peerComparison: [],
  };
}

async function fetchJsonWithTimeout(url: string, timeoutMs = 7500): Promise<unknown> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        Pragma: "no-cache",
        Referer: "https://quantura.studio/",
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 QuanturaExploreApi/1.0",
      },
    });
    if (!response.ok) {
      throw new Error(`upstream_http_${response.status}`);
    }
    return await response.json().catch(() => ({}));
  } finally {
    clearTimeout(timer);
  }
}

async function fetchTextWithTimeout(url: string, timeoutMs = MARKET_HEADLINE_FETCH_TIMEOUT_MS): Promise<string> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, text/html;q=0.3, */*;q=0.2",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        Pragma: "no-cache",
        Referer: "https://quantura.studio/market-headlines",
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 QuanturaMarketHeadlines/1.0",
      },
    });
    if (!response.ok) {
      throw new Error(`upstream_http_${response.status}`);
    }
    return await response.text();
  } finally {
    clearTimeout(timer);
  }
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function decodeXmlEntities(value: string): string {
  const withCdata = value.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1");
  const named: Record<string, string> = {
    amp: "&",
    lt: "<",
    gt: ">",
    quot: '"',
    apos: "'",
  };
  return withCdata
    .replace(/&#x([0-9a-f]+);/gi, (_match, hex: string) => {
      const code = Number.parseInt(hex, 16);
      return Number.isFinite(code) ? String.fromCodePoint(code) : "";
    })
    .replace(/&#([0-9]+);/g, (_match, num: string) => {
      const code = Number.parseInt(num, 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : "";
    })
    .replace(/&([a-z]+);/gi, (match, name: string) => named[name.toLowerCase()] || match);
}

function stripFeedMarkup(value: string, maxLen = 420): string {
  const raw = decodeXmlEntities(value).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  return raw.slice(0, maxLen);
}

function extractXmlTagText(block: string, tagNames: readonly string[]): string {
  for (const tagName of tagNames) {
    const pattern = new RegExp(`<${escapeRegExp(tagName)}(?:\\s[^>]*)?>([\\s\\S]*?)</${escapeRegExp(tagName)}>`, "i");
    const match = block.match(pattern);
    if (match && match[1]) return match[1];
  }
  return "";
}

function extractXmlAttribute(block: string, tagName: string, attributeName: string): string {
  const pattern = new RegExp(
    `<${escapeRegExp(tagName)}\\b[^>]*\\b${escapeRegExp(attributeName)}=["']([^"']+)["'][^>]*>`,
    "i"
  );
  const match = block.match(pattern);
  return match && match[1] ? decodeXmlEntities(match[1]).trim() : "";
}

function normalizeExternalUrl(value: string): string {
  const text = decodeXmlEntities(asString(value)).trim();
  return /^https?:\/\//i.test(text) ? text.slice(0, 1000) : "";
}

function extractFeedImageUrl(block: string): string {
  const mediaThumb = normalizeExternalUrl(extractXmlAttribute(block, "media:thumbnail", "url"));
  if (mediaThumb) return mediaThumb;
  const mediaContent = normalizeExternalUrl(extractXmlAttribute(block, "media:content", "url"));
  if (mediaContent) return mediaContent;
  const enclosureUrl = normalizeExternalUrl(extractXmlAttribute(block, "enclosure", "url"));
  const enclosureType = extractXmlAttribute(block, "enclosure", "type").toLowerCase();
  if (enclosureUrl && enclosureType.startsWith("image/")) return enclosureUrl;
  const summaryMatch = decodeXmlEntities(block).match(/<img[^>]+src=["']([^"']+)["']/i);
  return summaryMatch && summaryMatch[1] ? normalizeExternalUrl(summaryMatch[1]) : "";
}

function toPublishedIso(value: string): string {
  const parsed = Date.parse(decodeXmlEntities(value).trim());
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : "";
}

function parseMarketHeadlineFeedXml(
  xml: string,
  feedConfig: MarketHeadlineFeedConfig
): { feedTitle: string; headlines: MarketHeadlineArticle[] } {
  const feedTitle = stripFeedMarkup(
    extractXmlTagText(xml, ["channel:title", "title"]).replace(/^.*?<title(?:\s[^>]*)?>/i, ""),
    180
  );
  const itemBlocks = xml.match(/<item\b[\s\S]*?<\/item>/gi) || xml.match(/<entry\b[\s\S]*?<\/entry>/gi) || [];
  const seen = new Set<string>();
  const headlines: MarketHeadlineArticle[] = [];
  for (const block of itemBlocks) {
    const title = stripFeedMarkup(extractXmlTagText(block, ["title"]), 220);
    const link = normalizeExternalUrl(extractXmlAttribute(block, "link", "href") || extractXmlTagText(block, ["link", "id"]));
    if (!title || !link) continue;
    const dedupeKey = `${title.toLowerCase()}__${link.toLowerCase()}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    const summary = stripFeedMarkup(
      extractXmlTagText(block, ["description", "summary", "content:encoded", "content"]),
      420
    );
    const sourceText = stripFeedMarkup(extractXmlTagText(block, ["source"]), 140);
    const authorText = stripFeedMarkup(extractXmlTagText(block, ["dc:creator", "creator", "author"]), 140);
    const publisher = sanitizeText(sourceText || authorText || feedTitle || feedConfig.providerLabel, 140) || feedConfig.providerLabel;
    const publishedAt = toPublishedIso(extractXmlTagText(block, ["pubDate", "published", "updated", "dc:date"]));
    const thumbnailUrl = extractFeedImageUrl(block);
    headlines.push({
      title,
      summary,
      link,
      publisher,
      sourceLabel: `${feedConfig.providerLabel} · ${feedConfig.feedLabel}`,
      publishedAt,
      thumbnailUrl: thumbnailUrl || undefined,
    });
  }
  headlines.sort((left, right) => {
    const leftMs = left.publishedAt ? Date.parse(left.publishedAt) : 0;
    const rightMs = right.publishedAt ? Date.parse(right.publishedAt) : 0;
    return (Number.isFinite(rightMs) ? rightMs : 0) - (Number.isFinite(leftMs) ? leftMs : 0);
  });
  return {
    feedTitle: feedTitle || feedConfig.feedLabel,
    headlines,
  };
}

function resolveMarketHeadlineFeedConfig(providerIdRaw: unknown, feedIdRaw: unknown): MarketHeadlineFeedConfig {
  const providerId = sanitizeText(providerIdRaw, 80).toLowerCase();
  const feedId = sanitizeText(feedIdRaw, 80).toLowerCase();
  const exact = MARKET_HEADLINE_FEEDS[feedId];
  if (exact && (!providerId || exact.providerId === providerId)) return exact;
  const fallbackId = MARKET_HEADLINE_PROVIDER_DEFAULTS[providerId as keyof typeof MARKET_HEADLINE_PROVIDER_DEFAULTS];
  return MARKET_HEADLINE_FEEDS[fallbackId || MARKET_HEADLINE_DEFAULT_FEED_ID];
}

function getLatestMarketHeadlineTimestamp(headlines: readonly MarketHeadlineArticle[]): number {
  let latestMs = 0;
  for (const item of headlines) {
    const parsed = item?.publishedAt ? Date.parse(item.publishedAt) : Number.NaN;
    if (Number.isFinite(parsed) && parsed > latestMs) latestMs = parsed;
  }
  return latestMs;
}

function shouldFallbackMarketHeadlineFeed(
  feedConfig: MarketHeadlineFeedConfig,
  headlines: readonly MarketHeadlineArticle[]
): boolean {
  if (feedConfig.providerId !== "cnn") return false;
  if (!headlines.length) return true;
  const latestMs = getLatestMarketHeadlineTimestamp(headlines);
  if (!latestMs) return false;
  return Date.now() - latestMs > MARKET_HEADLINE_STALE_THRESHOLD_MS;
}

function describeMarketHeadlineFallback(
  requestedFeedConfig: MarketHeadlineFeedConfig,
  fallbackFeedConfig: MarketHeadlineFeedConfig,
  headlines: readonly MarketHeadlineArticle[]
): string {
  const latestMs = getLatestMarketHeadlineTimestamp(headlines);
  if (latestMs) {
    return `${requestedFeedConfig.providerLabel} ${requestedFeedConfig.feedLabel} is stale (latest article ${new Date(
      latestMs
    ).toISOString().slice(0, 10)}). Showing ${fallbackFeedConfig.providerLabel} ${fallbackFeedConfig.feedLabel} instead.`;
  }
  return `${requestedFeedConfig.providerLabel} ${requestedFeedConfig.feedLabel} did not return current headlines. Showing ${fallbackFeedConfig.providerLabel} ${fallbackFeedConfig.feedLabel} instead.`;
}

function describeMarketHeadlineFetchError(feedConfig: MarketHeadlineFeedConfig, error: unknown): string {
  const detail = sanitizeText((error as Error | null)?.message || error, 180).toLowerCase();
  if (detail.includes("403")) {
    return `${feedConfig.providerLabel} blocked automated access for ${feedConfig.feedLabel} right now.`;
  }
  if (detail.includes("404")) {
    return `${feedConfig.providerLabel} returned not found for ${feedConfig.feedLabel}.`;
  }
  if (detail.includes("abort")) {
    return `${feedConfig.providerLabel} timed out while loading ${feedConfig.feedLabel}.`;
  }
  return `Unable to fetch ${feedConfig.providerLabel} for ${feedConfig.feedLabel} right now.`;
}

function normalizeHandle(value: unknown): string {
  return asString(value).trim().toLowerCase().replace(/[^a-z0-9_.-]/g, "").slice(0, 40);
}

function sanitizeText(value: unknown, maxLen = 600): string {
  const raw = asString(value).replace(/\s+/g, " ").trim();
  if (!raw) return "";
  return raw.slice(0, maxLen);
}

function normalizeEmail(value: unknown): string {
  const raw = asString(value).trim().toLowerCase();
  if (!raw) return "";
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(raw)) return "";
  return raw.slice(0, 320);
}

function sanitizeRichText(value: unknown, maxLen = 20000): string {
  const raw = asString(value).replace(/\r\n?/g, "\n").replace(/\u0000/g, "").trim();
  if (!raw) return "";
  return raw.slice(0, maxLen);
}

function isRichTextOutputKey(key: string): boolean {
  const normalized = sanitizeText(key, 80).toLowerCase().replace(/[^a-z0-9]/g, "");
  return OUTPUT_META_RICH_TEXT_KEYS.has(normalized);
}

function normalizeFolderId(value: unknown): string {
  const raw = asString(value).trim().toLowerCase();
  if (!raw) return "";
  return raw.replace(/[^a-z0-9_-]/g, "-").replace(/-+/g, "-").slice(0, 80);
}

function normalizeSavedItemType(value: unknown): SavedItemType | "" {
  const raw = asString(value).trim().toLowerCase();
  if (raw === "forecast" || raw === "screener" || raw === "model_council" || raw === "post") return raw;
  return "";
}

function normalizeSourceId(value: unknown): string {
  return sanitizeText(value, 220).replace(/[^A-Za-z0-9._:\-]/g, "");
}

function normalizeShareId(value: unknown): string {
  const raw = asString(value).trim();
  if (!raw) return "";
  return /^[A-Za-z0-9_-]{8,220}$/.test(raw) ? raw : "";
}

function normalizeNotificationCategory(value: unknown): NotificationCategory {
  const raw = sanitizeText(value, 40).toLowerCase();
  if (raw === "watchlist") return "watchlist";
  if (raw === "explore") return "explore";
  if (raw === "earnings") return "earnings";
  if (raw === "ipo" || raw === "ipos") return "ipo";
  if (raw === "daily") return "daily";
  if (raw === "weekly") return "weekly";
  if (raw === "inactive" || raw === "inactive_user") return "inactive";
  return "explore";
}

function normalizeNotificationDeepLink(value: unknown): string {
  const raw = sanitizeText(value, 500);
  if (!raw) return "/notifications";
  if (/^https?:\/\//i.test(raw)) return raw;
  const prefixed = raw.startsWith("/") ? raw : `/${raw}`;
  return prefixed.slice(0, 500);
}

function absoluteNotificationLink(deepLink: string): string {
  if (/^https?:\/\//i.test(deepLink)) return deepLink;
  const path = deepLink.startsWith("/") ? deepLink : `/${deepLink}`;
  return `${PUBLIC_ORIGIN}${path}`;
}

function normalizeNotificationPrefs(
  input: Record<string, unknown>,
  existing: Record<string, unknown> = {}
): NotificationPrefs {
  const merged = {
    ...DEFAULT_NOTIFICATION_PREFS,
    ...existing,
  } as Record<string, unknown>;
  const resolved = { ...merged, ...input } as Record<string, unknown>;
  return {
    global: asBoolean(resolved.global, DEFAULT_NOTIFICATION_PREFS.global),
    following: asBoolean(resolved.following, DEFAULT_NOTIFICATION_PREFS.following),
    tickers: asBoolean(resolved.tickers, DEFAULT_NOTIFICATION_PREFS.tickers),
    watchlist: asBoolean(resolved.watchlist, DEFAULT_NOTIFICATION_PREFS.watchlist),
    explore: asBoolean(resolved.explore, DEFAULT_NOTIFICATION_PREFS.explore),
    earnings: asBoolean(resolved.earnings, DEFAULT_NOTIFICATION_PREFS.earnings),
    ipos: asBoolean(resolved.ipos, DEFAULT_NOTIFICATION_PREFS.ipos),
    daily: asBoolean(resolved.daily, DEFAULT_NOTIFICATION_PREFS.daily),
    weekly: asBoolean(resolved.weekly, DEFAULT_NOTIFICATION_PREFS.weekly),
    inactiveHidden: asBoolean(resolved.inactiveHidden, DEFAULT_NOTIFICATION_PREFS.inactiveHidden),
  };
}

function isNotificationCategoryEnabled(prefs: NotificationPrefs, category: NotificationCategory): boolean {
  if (category === "watchlist") return prefs.watchlist && prefs.tickers;
  if (category === "explore") return prefs.explore && prefs.following;
  if (category === "earnings") return prefs.earnings;
  if (category === "ipo") return prefs.ipos;
  if (category === "daily") return prefs.daily;
  if (category === "weekly") return prefs.weekly;
  if (category === "inactive") return prefs.daily || prefs.weekly;
  return true;
}

function notificationCategoryLabel(category: NotificationCategory): string {
  if (category === "watchlist") return "Watchlist";
  if (category === "explore") return "Explore Feed";
  if (category === "earnings") return "Earnings";
  if (category === "ipo") return "IPO";
  if (category === "daily") return "Daily";
  if (category === "weekly") return "Weekly";
  if (category === "inactive") return "Inactive user";
  return "Notification";
}

function buildFolderItemDocId(itemType: SavedItemType, sourceId: string): string {
  const cleanSource = normalizeSourceId(sourceId).replace(/[^A-Za-z0-9_-]/g, "_").slice(0, 180);
  return `${itemType}__${cleanSource || "item"}`;
}

function getTimestampMs(value: unknown): number {
  if (value instanceof admin.firestore.Timestamp) return value.toMillis();
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return Date.now();
}

function getOptionalTimestampMs(value: unknown): number | null {
  if (value instanceof admin.firestore.Timestamp) return value.toMillis();
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function timestampFromMs(ms: number): admin.firestore.Timestamp {
  return admin.firestore.Timestamp.fromMillis(ms);
}

function normalizeMyRequestType(value: unknown): MyRequestType | "" {
  const raw = sanitizeText(value, 40).trim();
  if (!raw) return "";
  const lower = raw.toLowerCase();
  if (lower === "forecast" || lower === "screener" || lower === "indicator") return lower;
  if (lower === "modelcouncil" || lower === "model_council" || lower === "model-council") return "modelCouncil";
  return "";
}

function normalizeMyRequestVisibility(value: unknown, fallback: MyRequestShareVisibility = "private"): MyRequestShareVisibility {
  const raw = sanitizeText(value, 20).toLowerCase();
  if (raw === "public" || raw === "unlisted" || raw === "private") return raw;
  return fallback;
}

function normalizeMyRequestId(value: unknown): string {
  const raw = sanitizeText(value, 220);
  if (!raw) return "";
  return raw.replace(/[^A-Za-z0-9._:\-]/g, "_").slice(0, 220);
}

function buildMyRequestDocId(type: MyRequestType, sourceId: string): string {
  const cleanType = normalizeMyRequestType(type) || "forecast";
  const cleanSourceId = normalizeSourceId(sourceId).replace(/[^A-Za-z0-9._:\-]/g, "_").slice(0, 180);
  return `${cleanType}__${cleanSourceId || "item"}`;
}

function defaultMyRequestTitle(type: MyRequestType, payload: Record<string, unknown> = {}): string {
  const ticker = normalizeTicker(payload.ticker);
  if (type === "forecast") {
    return sanitizeText(payload.title, 160) || `${ticker || "Ticker"} forecast`;
  }
  if (type === "screener") {
    return sanitizeText(payload.title, 160) || "Screener run";
  }
  if (type === "indicator") {
    return sanitizeText(payload.title, 160) || `${ticker || "Ticker"} indicators`;
  }
  return sanitizeText(payload.title, 160) || `${ticker || "Ticker"} Model Council`;
}

function ensureMyRequestShareSlug(seed = ""): string {
  const existing = normalizeShareId(seed);
  if (existing) return existing;
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let out = "";
  for (let i = 0; i < 16; i += 1) {
    out += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return out;
}

async function ensureUniqueMyRequestShareSlug(ownerUid: string, requestId: string, seed = ""): Promise<string> {
  const preferred = ensureMyRequestShareSlug(seed);
  const preferredRef = db.collection("request_shares").doc(preferred);
  const preferredSnap = await preferredRef.get();
  if (!preferredSnap.exists) return preferred;
  const preferredData = (preferredSnap.data() || {}) as Record<string, unknown>;
  if (asString(preferredData.ownerUid) === ownerUid && asString(preferredData.requestId) === requestId) return preferred;

  for (let i = 0; i < 12; i += 1) {
    const candidate = ensureMyRequestShareSlug();
    const candidateRef = db.collection("request_shares").doc(candidate);
    const candidateSnap = await candidateRef.get();
    if (!candidateSnap.exists) return candidate;
    const candidateData = (candidateSnap.data() || {}) as Record<string, unknown>;
    if (asString(candidateData.ownerUid) === ownerUid && asString(candidateData.requestId) === requestId) return candidate;
  }
  return ensureMyRequestShareSlug(`${ownerUid.slice(0, 6)}${requestId.slice(0, 6)}${Date.now()}`);
}

function normalizeMyRequestShareObject(input: unknown): { visibility: MyRequestShareVisibility; slug: string; createdAt: unknown } {
  const raw = asPlainObject(input);
  return {
    visibility: normalizeMyRequestVisibility(raw.visibility, "private"),
    slug: normalizeShareId(raw.slug),
    createdAt: raw.createdAt || null,
  };
}

function trimOutputsMeta(input: unknown): Record<string, unknown> {
  const raw = asPlainObject(input);
  const out: Record<string, unknown> = {};
  Object.entries(raw)
    .slice(0, 18)
    .forEach(([key, value]) => {
      const cleanKey = sanitizeText(key, 60);
      if (!cleanKey) return;
      if (typeof value === "string") {
        out[cleanKey] = isRichTextOutputKey(cleanKey) ? sanitizeRichText(value, 20000) : sanitizeText(value, 2400);
        return;
      }
      if (typeof value === "number" || typeof value === "boolean") {
        out[cleanKey] = value;
        return;
      }
      if (Array.isArray(value)) {
        out[cleanKey] = value
          .slice(0, 24)
          .map((item) => sanitizeText(item, 120))
          .filter(Boolean);
        return;
      }
      if (value && typeof value === "object") {
        const childOut: Record<string, unknown> = {};
        Object.entries(value as Record<string, unknown>)
          .slice(0, 12)
          .forEach(([childKey, childValue]) => {
            const cleanChildKey = sanitizeText(childKey, 40);
            if (!cleanChildKey) return;
            if (typeof childValue === "number" || typeof childValue === "boolean") {
              childOut[cleanChildKey] = childValue;
              return;
            }
            if (typeof childValue === "string") {
              childOut[cleanChildKey] = isRichTextOutputKey(cleanChildKey)
                ? sanitizeRichText(childValue, 6000)
                : sanitizeText(childValue, 220);
            }
          });
        if (Object.keys(childOut).length) out[cleanKey] = childOut;
      }
    });
  return out;
}

function buildMyRequestExploreBody(type: MyRequestType, outputsMeta: Record<string, unknown>): string {
  if (!outputsMeta || typeof outputsMeta !== "object") return "";
  const preferredValue =
    outputsMeta.bodyMarkdown ||
    outputsMeta.analysisMarkdown ||
    outputsMeta.fullAnswer ||
    outputsMeta.answerFull ||
    outputsMeta.answer ||
    outputsMeta.body ||
    outputsMeta.markdown ||
    outputsMeta.narrative;
  const richBody = sanitizeRichText(preferredValue, type === "modelCouncil" ? 24000 : 16000);
  if (richBody) return richBody;
  if (type === "modelCouncil") {
    return sanitizeRichText(outputsMeta.summary, 8000);
  }
  return "";
}

function normalizeMyRequestInput(input: unknown): Record<string, unknown> {
  const raw = asPlainObject(input);
  const out: Record<string, unknown> = {};
  Object.entries(raw)
    .slice(0, 30)
    .forEach(([key, value]) => {
      const cleanKey = sanitizeText(key, 60);
      if (!cleanKey) return;
      if (typeof value === "string") {
        out[cleanKey] = sanitizeText(value, 4000);
        return;
      }
      if (typeof value === "number" || typeof value === "boolean") {
        out[cleanKey] = value;
        return;
      }
      if (Array.isArray(value)) {
        out[cleanKey] = value
          .slice(0, 40)
          .map((item) => (typeof item === "number" ? item : sanitizeText(item, 120)))
          .filter((item) => (typeof item === "number" ? Number.isFinite(item) : Boolean(item)));
        return;
      }
      if (value && typeof value === "object") {
        out[cleanKey] = trimOutputsMeta(value);
      }
    });
  return out;
}

function firstTickerFromRequest(input: Record<string, unknown>, sourceRef: Record<string, unknown>, outputsMeta: Record<string, unknown>): string {
  const direct = normalizeTicker(input.ticker || outputsMeta.ticker);
  if (direct) return direct;
  const topSymbols = Array.isArray(outputsMeta.topSymbols) ? outputsMeta.topSymbols : [];
  const first = normalizeTicker(topSymbols[0]);
  if (first) return first;
  return normalizeTicker(sourceRef.ticker);
}

function buildMyRequestSearchText(
  title: string,
  type: MyRequestType,
  ticker: string,
  input: Record<string, unknown>,
  outputsMeta: Record<string, unknown>
): string {
  const parts = [
    title,
    type,
    ticker,
    asString(input.question),
    asString(input.notes),
    asString(input.universe),
    asString(outputsMeta.summary),
  ]
    .join(" ")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
  return parts.slice(0, 2400);
}

function myRequestShareUrl(slug: string, data: Record<string, unknown> = {}): string {
  const cleanSlug = normalizeShareId(slug);
  if (!cleanSlug) return "";
  const sourceRef = asPlainObject(data.sourceRef);
  const sourceCollection = sanitizeText(sourceRef.collection, 80);
  const type = normalizeMyRequestType(data.type) || "forecast";
  const input = normalizeMyRequestInput(data.input);
  const outputsMeta = trimOutputsMeta(data.outputsMeta);
  if (sanitizeText(sourceRef.collection, 80) === "autopilot_requests") {
    const sportsPanel =
      sanitizeText(input.panel || outputsMeta.panel, 80).toLowerCase() === "sports-autopilot" ||
      sanitizeText(input.sourceGroup || outputsMeta.sourceGroup, 40).toLowerCase() === "sports";
    if (sportsPanel) {
      return `${PUBLIC_ORIGIN}/dashboard?panel=sports-autopilot&requestShare=${encodeURIComponent(cleanSlug)}`;
    }
    return `${PUBLIC_ORIGIN}/autopilot?requestShare=${encodeURIComponent(cleanSlug)}`;
  }
  if (type === "screener" || sourceCollection === "screener_runs") {
    return `${PUBLIC_ORIGIN}/screener?requestShare=${encodeURIComponent(cleanSlug)}`;
  }
  if (type === "indicator") {
    return `${PUBLIC_ORIGIN}/indicators?requestShare=${encodeURIComponent(cleanSlug)}`;
  }
  if (type === "modelCouncil") {
    return `${PUBLIC_ORIGIN}/model-council?requestShare=${encodeURIComponent(cleanSlug)}`;
  }
  return `${PUBLIC_ORIGIN}/forecasting?requestShare=${encodeURIComponent(cleanSlug)}`;
}

function buildSharedScreenerRunPayload(
  runId: string,
  source: Record<string, unknown>,
  extras: Record<string, unknown> = {}
): Record<string, unknown> {
  const results = Array.isArray(source.results) ? source.results.slice(0, 300) : [];
  const workflowRunNumberRaw = asFinite(source.workflowRunNumber, NaN);
  const createdAtMs = getOptionalTimestampMs(source.createdAt);
  const updatedAtMs = getOptionalTimestampMs(source.updatedAt || source.createdAt);
  const workflowSteps = Array.isArray(source.workflowSteps)
    ? source.workflowSteps.map((item) => sanitizeText(item, 180)).filter(Boolean).slice(0, 10)
    : [];
  const workflowJobs = Array.isArray(source.workflowJobs)
    ? source.workflowJobs.slice(0, 8).map((item) => asPlainObject(item))
    : [];
  const workflowRunId = Math.max(0, Math.floor(asFinite(source.workflowRunId, 0)));
  const workflowArtifactId = Math.max(0, Math.floor(asFinite(source.workflowArtifactId, 0)));
  const workflowArtifactName = asString(source.workflowArtifactName, "");
  const workflowArtifactsRaw = Array.isArray(source.workflowArtifacts) ? source.workflowArtifacts : [];
  const workflowArtifacts = workflowArtifactsRaw.length
    ? workflowArtifactsRaw
        .map((item) => {
          const record = asPlainObject(item);
          const artifactId = Math.max(0, Math.floor(asFinite(record.id, 0)));
          if (!artifactId) return null;
          return {
            id: artifactId,
            name: asString(record.name, "artifact"),
            sizeInBytes: Math.max(0, Math.floor(asFinite(record.sizeInBytes, 0))),
            expired: asBoolean(record.expired, false),
            downloadPath:
              asString(record.downloadPath, "") || buildGithubArtifactDownloadPath(workflowRunId, artifactId),
            downloadUrl:
              asString(record.downloadUrl, "") || buildGithubArtifactDownloadUrl(workflowRunId, artifactId),
            githubUrl: asString(record.githubUrl, ""),
          };
        })
        .filter(
          (
            item
          ): item is {
            id: number;
            name: string;
            sizeInBytes: number;
            expired: boolean;
            downloadPath: string;
            downloadUrl: string;
            githubUrl: string;
          } => Boolean(item)
        )
        .slice(0, 8)
    : workflowRunId && workflowArtifactId
    ? [
        {
          id: workflowArtifactId,
          name: workflowArtifactName || "daily-prophet-signal-tracker",
          sizeInBytes: Math.max(0, Math.floor(asFinite(source.workflowArtifactSizeInBytes, 0))),
          expired: false,
          downloadPath: buildGithubArtifactDownloadPath(workflowRunId, workflowArtifactId),
          downloadUrl: buildGithubArtifactDownloadUrl(workflowRunId, workflowArtifactId),
          githubUrl: `https://github.com/${encodeURIComponent(GITHUB_REPO_OWNER)}/${encodeURIComponent(
            GITHUB_REPO_NAME
          )}/actions/runs/${encodeURIComponent(String(workflowRunId))}/artifacts/${encodeURIComponent(
            String(workflowArtifactId)
          )}`,
        },
      ]
    : [];
  return {
    id: sanitizeText(runId, 220),
    title: asString(source.title, "Screener run"),
    notes: asString(source.notes, ""),
    market: asString(source.market, ""),
    universe: asString(source.universe, ""),
    userId: asString(source.userId),
    isPublic: asBoolean(source.isPublic, false) || asBoolean(source.published, false),
    status: asString(source.status, "completed"),
    serviceMessage: asString(source.serviceMessage, ""),
    minMarketCap: Math.max(0, Math.floor(asFinite(source.minMarketCap || source.minCap, 0))),
    results,
    resultsFound: Array.isArray(source.results) ? source.results.length : Math.max(0, Math.floor(asFinite(source.resultsFound, results.length))),
    appliedFilters: Array.isArray(source.appliedFilters)
      ? source.appliedFilters.map((item) => sanitizeText(item, 160)).filter(Boolean).slice(0, 24)
      : [],
    workflowRunId: workflowRunId || null,
    workflowRunUrl: asString(source.workflowRunUrl, ""),
    workflowRunNumber: Number.isFinite(workflowRunNumberRaw) ? Math.floor(workflowRunNumberRaw) : null,
    workflowStatus: asString(source.workflowStatus, ""),
    workflowConclusion: asString(source.workflowConclusion, ""),
    workflowArtifactId: workflowArtifactId || null,
    workflowArtifactName,
    workflowArtifactSizeInBytes: Math.max(0, Math.floor(asFinite(source.workflowArtifactSizeInBytes, 0))),
    workflowArtifacts,
    workflowProgress: asString(source.workflowProgress, ""),
    workflowActiveStep: asString(source.workflowActiveStep, ""),
    workflowSteps,
    workflowJobs,
    workflowLogExcerpt: asString(source.workflowLogExcerpt, ""),
    screenedCount: Math.max(0, Math.floor(asFinite(source.screenedCount, 0))),
    createdAt: createdAtMs ? new Date(createdAtMs).toISOString() : null,
    updatedAt: updatedAtMs ? new Date(updatedAtMs).toISOString() : null,
    ...extras,
  };
}

async function buildSharedAutopilotFileResponse(
  shareSlug: string,
  runId: string,
  fileKey: string,
  fileRaw: unknown
): Promise<Record<string, unknown>> {
  const response = await buildStorageFileResponse(fileRaw);
  const file = asPlainObject(fileRaw);
  if (sanitizeText(file.artifactStore, 40) === "firestore") {
    response.apiTextPath = `/api/my-requests/shared/${encodeURIComponent(shareSlug)}/files/${encodeURIComponent(fileKey)}/text`;
  }
  return response;
}

async function buildSharedAutopilotRunPayload(
  shareSlug: string,
  runId: string,
  source: Record<string, unknown>,
  extras: Record<string, unknown> = {}
): Promise<Record<string, unknown>> {
  const payload = await toAutopilotRunResponse(runId, source);
  const filesRaw = asPlainObject(source.files);
  const fileEntries = await Promise.all(
    Object.entries(filesRaw).map(async ([key, value]) =>
      [key, await buildSharedAutopilotFileResponse(shareSlug, runId, key, value)] as const
    )
  );
  return {
    ...payload,
    files: Object.fromEntries(fileEntries),
    ...extras,
  };
}

function toMyRequestResponse(
  docId: string,
  data: Record<string, unknown>,
  opts: { includePayload?: boolean } = {}
): Record<string, unknown> {
  const includePayload = Boolean(opts.includePayload);
  const type = normalizeMyRequestType(data.type) || "forecast";
  const title = sanitizeText(data.title, 180) || defaultMyRequestTitle(type, data);
  const input = normalizeMyRequestInput(data.input);
  const outputsMeta = trimOutputsMeta(data.outputsMeta);
  const sourceRef = asPlainObject(data.sourceRef);
  const ticker = firstTickerFromRequest(input, sourceRef, outputsMeta);
  const createdAtMs = getTimestampMs(data.createdAt);
  const updatedAtMs = getTimestampMs(data.updatedAt || data.createdAt);
  const share = normalizeMyRequestShareObject(data.share);
  const visibility = normalizeMyRequestVisibility(data.visibility, share.visibility);
  const published = asBoolean(data.published, false);
  const deleted = asBoolean(data.deleted, false);
  const shareUrl =
    share.slug && share.visibility !== "private"
      ? myRequestShareUrl(share.slug, data)
      : "";

  const response: Record<string, unknown> = {
    id: docId,
    type,
    typeLabel: MY_REQUEST_TYPE_LABEL[type],
    title,
    ticker,
    ownerUid: asString(data.ownerUid),
    published,
    publishedAtMs: data.publishedAt ? getTimestampMs(data.publishedAt) : null,
    explorePostId: asString(data.explorePostId),
    deleted,
    createdAtMs,
    updatedAtMs,
    createdAt: new Date(createdAtMs).toISOString(),
    updatedAt: new Date(updatedAtMs).toISOString(),
    sourceRef: {
      collection: sanitizeText(sourceRef.collection, 80),
      id: sanitizeText(sourceRef.id, 220),
    },
    status: asString(outputsMeta.status || outputsMeta.workflowStatus, ""),
    workflowRunUrl: asString(outputsMeta.workflowRunUrl, ""),
    workflowRunNumber: Number.isFinite(asFinite(outputsMeta.workflowRunNumber, Number.NaN))
      ? Math.floor(asFinite(outputsMeta.workflowRunNumber, 0))
      : null,
    workflowProgress: asString(outputsMeta.workflowProgress, ""),
    workflowActiveStep: asString(outputsMeta.workflowActiveStep, ""),
    share: {
      visibility,
      slug: share.slug,
      shareUrl,
    },
  };

  if (includePayload) {
    response.input = input;
    response.outputsMeta = outputsMeta;
  } else {
    response.outputsMeta = trimOutputsMeta({
      summary: outputsMeta.summary,
      resultsCount: outputsMeta.resultsCount,
      forecastRowsCount: outputsMeta.forecastRowsCount,
      provider: outputsMeta.provider,
      model: outputsMeta.model,
      topSymbols: outputsMeta.topSymbols,
      status: outputsMeta.status,
      workflowStatus: outputsMeta.workflowStatus,
      workflowRunUrl: outputsMeta.workflowRunUrl,
      workflowRunNumber: outputsMeta.workflowRunNumber,
      workflowProgress: outputsMeta.workflowProgress,
      workflowActiveStep: outputsMeta.workflowActiveStep,
      workflowSteps: outputsMeta.workflowSteps,
    });
  }

  return response;
}

async function resolveSharedMyRequestContext(
  slugRaw: string,
  viewer: { uid?: string } | null
): Promise<{
  slug: string;
  visibility: MyRequestShareVisibility;
  ownerUid: string;
  requestId: string;
  requestSnap: FirebaseFirestore.DocumentSnapshot<FirebaseFirestore.DocumentData>;
  requestData: Record<string, unknown>;
  readOnly: boolean;
  sourceCollection: string;
  sourceId: string;
}> {
  const slug = normalizeShareId(slugRaw);
  if (!slug) {
    throw new Error("invalid_share_slug");
  }

  const shareSnap = await db.collection("request_shares").doc(slug).get();
  if (!shareSnap.exists) {
    throw new Error("share_not_found");
  }
  const shareData = (shareSnap.data() || {}) as Record<string, unknown>;
  const visibility = normalizeMyRequestVisibility(shareData.visibility, "private");
  const ownerUid = sanitizeText(shareData.ownerUid, 220);
  const requestId = normalizeMyRequestId(shareData.requestId);
  if (!ownerUid || !requestId) {
    throw new Error("share_invalid");
  }

  const canRead =
    visibility === "public" ||
    visibility === "unlisted" ||
    (viewer?.uid && viewer.uid === ownerUid);
  if (!canRead) {
    throw new Error("forbidden");
  }

  const requestSnap = await db.collection("users").doc(ownerUid).collection("requests").doc(requestId).get();
  if (!requestSnap.exists) {
    throw new Error("request_not_found");
  }
  const requestData = (requestSnap.data() || {}) as Record<string, unknown>;
  if (asBoolean(requestData.deleted, false)) {
    throw new Error("request_not_found");
  }
  const sourceRef = asPlainObject(requestData.sourceRef);
  return {
    slug,
    visibility,
    ownerUid,
    requestId,
    requestSnap,
    requestData,
    readOnly: !(viewer?.uid && viewer.uid === ownerUid),
    sourceCollection: sanitizeText(sourceRef.collection, 80),
    sourceId: sanitizeText(sourceRef.id, 220),
  };
}

function normalizeMyRequestPublishedFilter(input: unknown): "all" | "published" | "unpublished" {
  const raw = sanitizeText(input, 20).toLowerCase();
  if (raw === "published") return "published";
  if (raw === "unpublished" || raw === "draft") return "unpublished";
  return "all";
}

function extractScreenerTopSymbols(resultsRaw: unknown): string[] {
  const results = Array.isArray(resultsRaw) ? resultsRaw : [];
  const out: string[] = [];
  results.slice(0, 24).forEach((row) => {
    const symbol = normalizeTicker((row as Record<string, unknown>)?.symbol);
    if (!symbol || out.includes(symbol)) return;
    out.push(symbol);
  });
  return out.slice(0, 12);
}

function fmtCompactCurrency(value: number): string {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return "";
  if (numeric >= 1_000_000_000_000) return `$${(numeric / 1_000_000_000_000).toFixed(2)}T`;
  if (numeric >= 1_000_000_000) return `$${(numeric / 1_000_000_000).toFixed(0)}B`;
  if (numeric >= 1_000_000) return `$${(numeric / 1_000_000).toFixed(0)}M`;
  return `$${Math.round(numeric).toLocaleString()}`;
}

function roundFinite(value: unknown, digits = 2): number | null {
  const numeric = asFinite(value, Number.NaN);
  if (!Number.isFinite(numeric)) return null;
  return Number(numeric.toFixed(digits));
}

type GithubWorkflowRunRecord = {
  id: number;
  htmlUrl: string;
  path: string;
  workflowName: string;
  headBranch: string;
  event: string;
  status: string;
  conclusion: string;
  runNumber: number;
  createdAt: string;
  updatedAt: string;
  displayTitle: string;
};

type GithubWorkflowStepRecord = {
  number: number;
  name: string;
  status: string;
  conclusion: string;
  startedAt: string;
  completedAt: string;
};

type GithubWorkflowJobRecord = {
  id: number;
  name: string;
  status: string;
  conclusion: string;
  startedAt: string;
  completedAt: string;
  steps: GithubWorkflowStepRecord[];
};

type GithubArtifactRecord = {
  id: number;
  name: string;
  sizeInBytes: number;
  archiveDownloadUrl: string;
  expired: boolean;
};

type GithubScreenerArtifactPayload = {
  manifest: Record<string, unknown>;
  activeSignals: Array<Record<string, unknown>>;
  allLatestRows: Array<Record<string, unknown>>;
  errors: Array<Record<string, unknown>>;
  workflowLog: string;
};

function githubActionsConfigured(): boolean {
  return Boolean(GITHUB_ACTIONS_TOKEN && GITHUB_REPO_OWNER && GITHUB_REPO_NAME && GITHUB_SCREENER_WORKFLOW);
}

async function githubApiRequest(path: string, init: RequestInit = {}): Promise<globalThis.Response> {
  if (!githubActionsConfigured()) {
    throw new Error("GitHub Actions token is not configured.");
  }
  const headers = new Headers(init.headers || {});
  headers.set("Accept", "application/vnd.github+json");
  headers.set("Authorization", `Bearer ${GITHUB_ACTIONS_TOKEN}`);
  headers.set("User-Agent", "quantura-studio");
  if (init.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  const response = await fetch(`${GITHUB_ACTIONS_API_BASE}${path}`, {
    ...init,
    headers,
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(sanitizeText(text || `GitHub API ${response.status}`, 260) || `GitHub API ${response.status}`);
  }
  return response;
}

async function githubApiJson(path: string, init: RequestInit = {}): Promise<Record<string, unknown>> {
  const response = await githubApiRequest(path, init);
  return asPlainObject(await response.json().catch(() => ({})));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildScreenerWorkflowRunKey(runId: string): string {
  const clean = sanitizeText(runId, 80).replace(/[^A-Za-z0-9_-]/g, "");
  return `quantura_${clean || Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

async function dispatchGithubScreenerWorkflow(input: {
  runKey: string;
  minMarketCap: number;
}): Promise<void> {
  await githubApiRequest(`/actions/workflows/${encodeURIComponent(GITHUB_SCREENER_WORKFLOW)}/dispatches`, {
    method: "POST",
    body: JSON.stringify({
      ref: GITHUB_ACTIONS_BRANCH,
      inputs: {
        run_key: input.runKey,
        min_market_cap: String(Math.max(0, Math.floor(input.minMarketCap || 0))),
      },
    }),
  });
}

function normalizeGithubWorkflowRun(raw: unknown): GithubWorkflowRunRecord | null {
  const record = asPlainObject(raw);
  const id = Math.floor(asFinite(record.id, 0));
  if (!id) return null;
  return {
    id,
    htmlUrl: sanitizeText(record.html_url, 600),
    path: sanitizeText(record.path, 320),
    workflowName: sanitizeText(record.name, 240),
    headBranch: sanitizeText(record.head_branch, 120),
    event: sanitizeText(record.event, 80).toLowerCase(),
    status: sanitizeText(record.status, 80).toLowerCase(),
    conclusion: sanitizeText(record.conclusion, 80).toLowerCase(),
    runNumber: Math.floor(asFinite(record.run_number, 0)),
    createdAt: sanitizeText(record.created_at, 120),
    updatedAt: sanitizeText(record.updated_at, 120),
    displayTitle: sanitizeText(record.display_title || record.name, 240),
  };
}

async function findGithubWorkflowRunByKey(runKey: string): Promise<GithubWorkflowRunRecord | null> {
  const payload = await githubApiJson(
    `/actions/workflows/${encodeURIComponent(GITHUB_SCREENER_WORKFLOW)}/runs?event=workflow_dispatch&branch=${encodeURIComponent(
      GITHUB_ACTIONS_BRANCH
    )}&per_page=25`
  );
  const runs = Array.isArray(payload.workflow_runs) ? payload.workflow_runs : [];
  for (const item of runs) {
    const normalized = normalizeGithubWorkflowRun(item);
    if (!normalized) continue;
    if (normalized.displayTitle.includes(runKey)) return normalized;
  }
  return null;
}

async function waitForGithubWorkflowRun(runKey: string, timeoutMs = 20000): Promise<GithubWorkflowRunRecord | null> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const run = await findGithubWorkflowRunByKey(runKey).catch(() => null);
    if (run) return run;
    await delay(1500);
  }
  return null;
}

async function getGithubWorkflowRun(runId: number): Promise<GithubWorkflowRunRecord | null> {
  if (!runId) return null;
  const payload = await githubApiJson(`/actions/runs/${encodeURIComponent(String(runId))}`);
  return normalizeGithubWorkflowRun(payload);
}

async function listGithubWorkflowRuns(input: { perPage?: number; status?: string; branch?: string; event?: string } = {}): Promise<GithubWorkflowRunRecord[]> {
  const perPage = Math.max(1, Math.min(100, Math.floor(asFinite(input.perPage, 20))));
  const params = new URLSearchParams();
  params.set("per_page", String(perPage));
  if (sanitizeText(input.status, 40)) params.set("status", sanitizeText(input.status, 40));
  if (sanitizeText(input.branch, 120)) params.set("branch", sanitizeText(input.branch, 120));
  if (sanitizeText(input.event, 40)) params.set("event", sanitizeText(input.event, 40));
  const payload = await githubApiJson(`/actions/workflows/${encodeURIComponent(GITHUB_SCREENER_WORKFLOW)}/runs?${params.toString()}`);
  const runs = Array.isArray(payload.workflow_runs) ? payload.workflow_runs : [];
  return runs.map((item) => normalizeGithubWorkflowRun(item)).filter((item): item is GithubWorkflowRunRecord => Boolean(item));
}

async function listRecentScheduledGithubScreenerRuns(limit = 12): Promise<GithubWorkflowRunRecord[]> {
  const desired = Math.max(1, Math.min(60, Math.floor(asFinite(limit, 12))));
  const perPage = Math.max(desired * 2, 20);
  const runs = await listGithubWorkflowRuns({
    perPage,
    branch: GITHUB_ACTIONS_BRANCH,
    event: "schedule",
  });
  return runs
    .filter((item) => item.event === "schedule")
    .filter((item) => item.status === "queued" || item.status === "in_progress" || item.status === "completed")
    .slice(0, desired);
}

function normalizeGithubWorkflowStep(raw: unknown): GithubWorkflowStepRecord | null {
  const record = asPlainObject(raw);
  const number = Math.max(0, Math.floor(asFinite(record.number, 0)));
  const name = sanitizeText(record.name, 240);
  if (!number && !name) return null;
  return {
    number,
    name,
    status: sanitizeText(record.status, 80).toLowerCase(),
    conclusion: sanitizeText(record.conclusion, 80).toLowerCase(),
    startedAt: sanitizeText(record.started_at, 120),
    completedAt: sanitizeText(record.completed_at, 120),
  };
}

function normalizeGithubWorkflowJob(raw: unknown): GithubWorkflowJobRecord | null {
  const record = asPlainObject(raw);
  const id = Math.max(0, Math.floor(asFinite(record.id, 0)));
  const name = sanitizeText(record.name, 240);
  if (!id && !name) return null;
  const stepsRaw = Array.isArray(record.steps) ? record.steps : [];
  return {
    id,
    name,
    status: sanitizeText(record.status, 80).toLowerCase(),
    conclusion: sanitizeText(record.conclusion, 80).toLowerCase(),
    startedAt: sanitizeText(record.started_at, 120),
    completedAt: sanitizeText(record.completed_at, 120),
    steps: stepsRaw
      .map((item) => normalizeGithubWorkflowStep(item))
      .filter((item): item is GithubWorkflowStepRecord => Boolean(item))
      .slice(0, 24),
  };
}

async function listGithubJobsForRun(runId: number): Promise<GithubWorkflowJobRecord[]> {
  if (!runId) return [];
  const payload = await githubApiJson(`/actions/runs/${encodeURIComponent(String(runId))}/jobs?per_page=100`);
  const jobs = Array.isArray(payload.jobs) ? payload.jobs : [];
  return jobs
    .map((item) => normalizeGithubWorkflowJob(item))
    .filter((item): item is GithubWorkflowJobRecord => Boolean(item))
    .slice(0, 12);
}

function formatGithubWorkflowStateLabel(status: unknown, conclusion: unknown = ""): string {
  const cleanStatus = sanitizeText(status, 80).toLowerCase();
  const cleanConclusion = sanitizeText(conclusion, 80).toLowerCase();
  if (cleanStatus === "completed" && cleanConclusion) {
    return cleanConclusion.replace(/_/g, " ");
  }
  if (cleanStatus === "in_progress") return "running";
  if (cleanStatus === "queued") return "queued";
  return cleanStatus ? cleanStatus.replace(/_/g, " ") : "pending";
}

function buildGithubWorkflowProgressSnapshot(jobs: GithubWorkflowJobRecord[]): Record<string, unknown> {
  const normalizedJobs = Array.isArray(jobs) ? jobs : [];
  if (!normalizedJobs.length) return {};

  const activeJob =
    normalizedJobs.find((job) => job.status === "in_progress") ||
    normalizedJobs.find((job) => job.status === "queued") ||
    null;
  const activeStep =
    activeJob?.steps.find((step) => step.status === "in_progress") ||
    activeJob?.steps.find((step) => step.status === "queued") ||
    null;
  const completedJobs = normalizedJobs.filter((job) => job.status === "completed").length;
  const workflowProgress = activeStep
    ? `${activeJob?.name || "Workflow"} · ${activeStep.name}`
    : activeJob
    ? `${activeJob.name || "Workflow"} is ${formatGithubWorkflowStateLabel(activeJob.status, activeJob.conclusion)}`
    : `${completedJobs}/${normalizedJobs.length} workflow job${normalizedJobs.length === 1 ? "" : "s"} completed`;

  const workflowSteps = normalizedJobs
    .flatMap((job) =>
      (Array.isArray(job.steps) ? job.steps : []).map((step) => {
        const stepState = formatGithubWorkflowStateLabel(step.status, step.conclusion);
        return `${job.name || "Workflow"} · ${step.name || "Step"} · ${stepState}`;
      })
    )
    .filter(Boolean)
    .slice(-10);

  return {
    workflowProgress: sanitizeText(workflowProgress, 240),
    workflowActiveStep: sanitizeText(activeStep ? `${activeJob?.name || "Workflow"} · ${activeStep.name}` : "", 240),
    workflowSteps,
    workflowJobs: normalizedJobs.slice(0, 8).map((job) => ({
      id: job.id,
      name: sanitizeText(job.name, 180),
      status: sanitizeText(job.status, 40),
      conclusion: sanitizeText(job.conclusion, 40),
      startedAt: sanitizeText(job.startedAt, 120),
      completedAt: sanitizeText(job.completedAt, 120),
      steps: (Array.isArray(job.steps) ? job.steps : []).slice(0, 12).map((step) => ({
        number: Math.max(0, Math.floor(asFinite(step.number, 0))),
        name: sanitizeText(step.name, 180),
        status: sanitizeText(step.status, 40),
        conclusion: sanitizeText(step.conclusion, 40),
        startedAt: sanitizeText(step.startedAt, 120),
        completedAt: sanitizeText(step.completedAt, 120),
      })),
    })),
  };
}

function normalizeGithubArtifact(raw: unknown): GithubArtifactRecord | null {
  const record = asPlainObject(raw);
  const id = Math.floor(asFinite(record.id, 0));
  if (!id) return null;
  return {
    id,
    name: sanitizeText(record.name, 180),
    sizeInBytes: Math.max(0, Math.floor(asFinite(record.size_in_bytes, 0))),
    archiveDownloadUrl: sanitizeText(record.archive_download_url, 1000),
    expired: asBoolean(record.expired, false),
  };
}

async function listGithubArtifactsForRun(runId: number): Promise<GithubArtifactRecord[]> {
  if (!runId) return [];
  const payload = await githubApiJson(`/actions/runs/${encodeURIComponent(String(runId))}/artifacts?per_page=50`);
  const artifacts = Array.isArray(payload.artifacts) ? payload.artifacts : [];
  return artifacts.map((item) => normalizeGithubArtifact(item)).filter((item): item is GithubArtifactRecord => Boolean(item));
}

function buildGithubArtifactDownloadPath(runId: unknown, artifactId: unknown): string {
  const cleanRunId = Math.max(0, Math.floor(asFinite(runId, 0)));
  const cleanArtifactId = Math.max(0, Math.floor(asFinite(artifactId, 0)));
  if (!cleanRunId || !cleanArtifactId) return "";
  return `/api/screener/github-history/${encodeURIComponent(String(cleanRunId))}/artifacts/${encodeURIComponent(
    String(cleanArtifactId)
  )}/download`;
}

function buildGithubArtifactDownloadUrl(runId: unknown, artifactId: unknown): string {
  const path = buildGithubArtifactDownloadPath(runId, artifactId);
  return path ? `${PUBLIC_ORIGIN}${path}` : "";
}

function buildGithubArtifactLinkPayload(runId: unknown, artifact: GithubArtifactRecord): Record<string, unknown> {
  const cleanRunId = Math.max(0, Math.floor(asFinite(runId, 0)));
  return {
    id: artifact.id,
    name: sanitizeText(artifact.name, 180),
    sizeInBytes: Math.max(0, Math.floor(asFinite(artifact.sizeInBytes, 0))),
    expired: asBoolean(artifact.expired, false),
    downloadPath: buildGithubArtifactDownloadPath(cleanRunId, artifact.id),
    downloadUrl: buildGithubArtifactDownloadUrl(cleanRunId, artifact.id),
    githubUrl: cleanRunId
      ? `https://github.com/${encodeURIComponent(GITHUB_REPO_OWNER)}/${encodeURIComponent(
          GITHUB_REPO_NAME
        )}/actions/runs/${encodeURIComponent(String(cleanRunId))}/artifacts/${encodeURIComponent(String(artifact.id))}`
      : "",
  };
}

function parseCsvLine(line: string): string[] {
  const cells: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let idx = 0; idx < line.length; idx += 1) {
    const char = line[idx];
    if (char === '"') {
      if (inQuotes && line[idx + 1] === '"') {
        current += '"';
        idx += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === "," && !inQuotes) {
      cells.push(current);
      current = "";
      continue;
    }
    current += char;
  }
  cells.push(current);
  return cells;
}

function parseCsvRecords(csvText: string): Array<Record<string, unknown>> {
  const text = String(csvText || "").trim();
  if (!text) return [];
  const lines = text.split(/\r?\n/).filter(Boolean);
  if (!lines.length) return [];
  const headers = parseCsvLine(lines[0]).map((item) => sanitizeText(item, 80));
  const rows: Array<Record<string, unknown>> = [];
  for (const line of lines.slice(1)) {
    const cells = parseCsvLine(line);
    const row: Record<string, unknown> = {};
    headers.forEach((header, index) => {
      if (!header) return;
      const raw = String(cells[index] ?? "").trim();
      if (!raw) {
        row[header] = "";
        return;
      }
      const numeric = Number(raw);
      row[header] = Number.isFinite(numeric) && /^-?\d+(\.\d+)?$/.test(raw) ? numeric : raw;
    });
    rows.push(row);
  }
  return rows;
}

function readZipEntryText(zip: any, entryName: string): string {
  const entry = zip.getEntry(entryName);
  if (!entry) return "";
  return String(zip.readAsText(entry) || "");
}

function normalizeWorkflowScreenerRows(rowsRaw: unknown): Array<Record<string, unknown>> {
  const rows = Array.isArray(rowsRaw) ? rowsRaw : [];
  const normalized: Array<Record<string, unknown>> = [];
  for (const item of rows) {
    const row = asPlainObject(item);
    const symbol = normalizeTicker(row.ticker || row.symbol);
    if (!symbol) continue;
    normalized.push({
      symbol,
      ticker: symbol,
      status: sanitizeText(row.status, 60).toLowerCase(),
      lastClose: roundFinite(row.last_price, 2),
      gapToBandPct: roundFinite(row.gap_to_band_pct, 2),
      p1: roundFinite(row.p1, 2),
      p10: roundFinite(row.p10, 2),
      p50: roundFinite(row.p50, 2),
      p90: roundFinite(row.p90, 2),
      p99: roundFinite(row.p99, 2),
      centralDelta: roundFinite(row.central_delta, 2),
      centralDeltaLabel: sanitizeText(row.central_delta_label, 80),
      tailDelta: roundFinite(row.tail_delta, 2),
      tailDeltaLabel: sanitizeText(row.tail_delta_label, 80),
      marketCap: asFinite(row.market_cap, Number.NaN),
      marketCapLabel: sanitizeText(row.market_cap_fmt, 40),
      lastEarningsDate: sanitizeText(row.last_earnings_date, 40),
      lastReportPeriod: sanitizeText(row.last_report_period, 80),
      nextEarningsDate: sanitizeText(row.next_earnings_date, 40),
      nextReportPeriod: sanitizeText(row.next_report_period, 80),
      universe: sanitizeText(row.universe, 80),
    });
  }
  return normalized;
}

async function downloadGithubArtifactPayload(artifact: GithubArtifactRecord): Promise<GithubScreenerArtifactPayload> {
  const response = await githubApiRequest(`/actions/artifacts/${encodeURIComponent(String(artifact.id))}/zip`, {
    method: "GET",
  });
  const buffer = Buffer.from(await response.arrayBuffer());
  const zip = new AdmZip(buffer);
  const manifestText =
    readZipEntryText(zip, "artifacts/run_manifest.json") || readZipEntryText(zip, "run_manifest.json");
  const activeSignalsText =
    readZipEntryText(zip, "artifacts/active_signals.csv") || readZipEntryText(zip, "active_signals.csv");
  const allLatestRowsText =
    readZipEntryText(zip, "artifacts/all_latest_rows.csv") || readZipEntryText(zip, "all_latest_rows.csv");
  const errorsText = readZipEntryText(zip, "artifacts/errors.csv") || readZipEntryText(zip, "errors.csv");
  const workflowLog =
    readZipEntryText(zip, "artifacts/workflow-run.log") || readZipEntryText(zip, "workflow-run.log");
  const manifest = manifestText ? asPlainObject(JSON.parse(manifestText)) : {};
  return {
    manifest,
    activeSignals: parseCsvRecords(activeSignalsText),
    allLatestRows: parseCsvRecords(allLatestRowsText),
    errors: parseCsvRecords(errorsText),
    workflowLog,
  };
}

function buildGithubScreenerServiceMessage(manifest: Record<string, unknown>, minMarketCap: number, resultCount: number): string {
  const status = sanitizeText(manifest.status, 80).toLowerCase();
  const floorLabel = fmtCompactCurrency(minMarketCap) || `$${Number(minMarketCap || 0).toLocaleString()}`;
  if (status.startsWith("skipped_")) {
    return sanitizeText(manifest.reason, 240) || `GitHub Actions skipped this screener run above the ${floorLabel} floor.`;
  }
  if (resultCount > 0) {
    return `GitHub Actions found ${resultCount} active Prophet signal${resultCount === 1 ? "" : "s"} above the ${floorLabel} market-cap floor.`;
  }
  return `GitHub Actions completed successfully, but no active Prophet signals cleared the ${floorLabel} market-cap floor.`;
}

function buildScreenerMyRequestOutputsMeta(source: Record<string, unknown>): Record<string, unknown> {
  const minMarketCap = Math.max(0, Math.floor(asFinite(source.minMarketCap || source.minCap, 0)));
  const results = Array.isArray(source.results) ? source.results : [];
  const resultsCount = Array.isArray(source.results)
    ? results.length
    : Math.max(0, Math.floor(asFinite(source.resultsFound, 0)));
  const topSymbols = Array.isArray(source.topSymbols)
    ? source.topSymbols.map((item) => normalizeTicker(item)).filter(Boolean).slice(0, 12)
    : extractScreenerTopSymbols(results);
  const workflowSteps = Array.isArray(source.workflowSteps)
    ? source.workflowSteps.map((item) => sanitizeText(item, 180)).filter(Boolean).slice(0, 10)
    : [];
  const workflowRunNumber = Math.floor(asFinite(source.workflowRunNumber, 0));
  const screenedCount = Math.max(0, Math.floor(asFinite(source.screenedCount, 0)));
  return trimOutputsMeta({
    summary: asString(source.serviceMessage, ""),
    resultsCount,
    topSymbols,
    modelUsed: asString(source.modelUsed, "daily_prophet_signal_tracker"),
    status: asString(source.status, "queued"),
    workflowStatus: asString(source.workflowStatus, ""),
    workflowConclusion: asString(source.workflowConclusion, ""),
    workflowRunId: asString(source.workflowRunId, ""),
    workflowRunUrl: asString(source.workflowRunUrl, ""),
    workflowRunNumber: workflowRunNumber || 0,
    workflowProgress: asString(source.workflowProgress, ""),
    workflowActiveStep: asString(source.workflowActiveStep, ""),
    workflowSteps,
    workflowLogExcerpt: asString(source.workflowLogExcerpt, ""),
    screenedCount,
    metrics: {
      Status: asString(source.status, "queued"),
      Floor: fmtCompactCurrency(minMarketCap),
      Matches: resultsCount,
      Screened: screenedCount || 0,
      Workflow: workflowRunNumber || asString(source.workflowRunId, ""),
    },
  });
}

async function syncScreenerMyRequestFromRun(
  ownerUid: string,
  runId: string,
  source: Record<string, unknown>,
  published = false
): Promise<Record<string, unknown> | null> {
  const requestId = buildMyRequestDocId("screener", runId);
  const minMarketCap = Math.max(0, Math.floor(asFinite(source.minMarketCap || source.minCap, 100_000_000_000)));
  await upsertOwnedMyRequestFromSystem(ownerUid, {
    requestId,
    type: "screener",
    title: sanitizeText(source.title, 180) || "GitHub stock screener",
    input: {
      minMarketCap,
      source: "github_actions",
      workflow: GITHUB_SCREENER_WORKFLOW,
      branch: GITHUB_ACTIONS_BRANCH,
    },
    outputsMeta: buildScreenerMyRequestOutputsMeta(source),
    sourceRef: {
      collection: "screener_runs",
      id: sanitizeText(runId, 220),
    },
    published,
  });
  const requestSnap = await db.collection("users").doc(ownerUid).collection("requests").doc(requestId).get().catch(() => null);
  if (!requestSnap?.exists) return null;
  return toMyRequestResponse(requestSnap.id, (requestSnap.data() || {}) as Record<string, unknown>, { includePayload: true });
}

async function findScreenerRunRecordByWorkflowRunId(workflowRunId: number): Promise<{ id: string; data: Record<string, unknown> } | null> {
  const cleanWorkflowRunId = Math.max(0, Math.floor(asFinite(workflowRunId, 0)));
  if (!cleanWorkflowRunId) return null;
  const snapshot = await db.collection("screener_runs").where("workflowRunId", "==", cleanWorkflowRunId).limit(1).get();
  const doc = snapshot.docs[0];
  if (!doc) return null;
  return {
    id: doc.id,
    data: (doc.data() || {}) as Record<string, unknown>,
  };
}

function buildPublicGithubScreenerRunSummary(
  workflowRun: GithubWorkflowRunRecord,
  sourceMatch: { id: string; data: Record<string, unknown> } | null,
  artifacts: GithubArtifactRecord[]
): Record<string, unknown> {
  const source = sourceMatch?.data || {};
  const createdAtMs = getOptionalTimestampMs(source.createdAt);
  const updatedAtMs = getOptionalTimestampMs(source.updatedAt || source.createdAt);
  const results = Array.isArray(source.results) ? source.results : [];
  const resultsFound = Array.isArray(source.results)
    ? results.length
    : Math.max(0, Math.floor(asFinite(source.resultsFound, 0)));
  const screenedCount = Math.max(0, Math.floor(asFinite(source.screenedCount, 0)));
  const minMarketCap = Math.max(0, Math.floor(asFinite(source.minMarketCap || source.minCap, 0)));
  const topSymbols = Array.isArray(source.topSymbols)
    ? source.topSymbols.map((item) => normalizeTicker(item)).filter(Boolean).slice(0, 12)
    : extractScreenerTopSymbols(results);
  const visibleArtifacts = (Array.isArray(artifacts) ? artifacts : [])
    .filter((artifact) => !asBoolean(artifact?.expired, false))
    .slice(0, 8)
    .map((artifact) => buildGithubArtifactLinkPayload(workflowRun.id, artifact));
  const title =
    sanitizeText(source.title, 180) ||
    sanitizeText(workflowRun.displayTitle, 180).replace(/\bquantura_[a-z0-9_-]+\b/gi, "").trim() ||
    `GitHub screener workflow #${workflowRun.runNumber || workflowRun.id}`;
  return {
    runId: workflowRun.id,
    sourceRunId: sanitizeText(sourceMatch?.id, 220),
    title,
    displayTitle: sanitizeText(workflowRun.displayTitle, 240),
    workflowName: sanitizeText(workflowRun.workflowName, 240),
    workflowRunId: workflowRun.id,
    workflowRunNumber: workflowRun.runNumber || null,
    workflowRunUrl: sanitizeText(workflowRun.htmlUrl, 600),
    workflowEvent: sanitizeText(workflowRun.event, 80) || "schedule",
    workflowStatus: sanitizeText(workflowRun.status, 80) || "completed",
    workflowConclusion: sanitizeText(workflowRun.conclusion, 80) || "success",
    createdAt: createdAtMs ? new Date(createdAtMs).toISOString() : workflowRun.createdAt || null,
    updatedAt: updatedAtMs ? new Date(updatedAtMs).toISOString() : workflowRun.updatedAt || null,
    completedAt: workflowRun.updatedAt || null,
    minMarketCap,
    resultsFound,
    screenedCount,
    topSymbols,
    serviceMessage:
      sanitizeText(source.serviceMessage, 320) ||
      (workflowRun.status === "in_progress"
        ? `Scheduled GitHub Actions screener workflow #${workflowRun.runNumber || workflowRun.id} is running now.`
        : workflowRun.status === "queued"
        ? `Scheduled GitHub Actions screener workflow #${workflowRun.runNumber || workflowRun.id} is queued.`
        : workflowRun.conclusion && workflowRun.conclusion !== "success"
        ? `Scheduled GitHub Actions screener workflow #${workflowRun.runNumber || workflowRun.id} finished with ${workflowRun.conclusion}.`
        : `Scheduled GitHub Actions screener workflow #${workflowRun.runNumber || workflowRun.id} completed successfully.`),
    artifacts: visibleArtifacts,
    artifactCount: visibleArtifacts.length,
  };
}

async function syncGithubScreenerRunRecord(runId: string, viewerUid: string): Promise<Record<string, unknown>> {
  const cleanRunId = sanitizeText(runId, 220);
  if (!cleanRunId) throw new Error("invalid_run_id");
  const ref = db.collection("screener_runs").doc(cleanRunId);
  const snap = await ref.get();
  if (!snap.exists) throw new Error("screener_not_found");
  const data = (snap.data() || {}) as Record<string, unknown>;
  if (sanitizeText(data.userId, 220) !== viewerUid) throw new Error("forbidden");
  if (sanitizeText(data.source, 80) !== "github_actions") {
    return { id: snap.id, ...data };
  }

  const minMarketCap = Math.max(0, asFinite(data.minMarketCap || data.minCap, 100_000_000_000));
  const runKey = sanitizeText(data.workflowRunKey, 180);
  let workflowRunId = Math.floor(asFinite(data.workflowRunId, 0));
  let workflowRun = workflowRunId ? await getGithubWorkflowRun(workflowRunId).catch(() => null) : null;
  if (!workflowRun && runKey) {
    workflowRun = await findGithubWorkflowRunByKey(runKey).catch(() => null);
    workflowRunId = workflowRun?.id || 0;
  }

  const patch: Record<string, unknown> = {
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  if (workflowRun) {
    patch.workflowRunId = workflowRun.id;
    patch.workflowRunUrl = workflowRun.htmlUrl;
    patch.workflowRunNumber = workflowRun.runNumber || null;
    patch.workflowStatus = workflowRun.status;
    patch.workflowConclusion = workflowRun.conclusion || "";
  }

  if (!workflowRun) {
    patch.status = sanitizeText(data.status, 40) || "queued";
    patch.serviceMessage =
      sanitizeText(data.serviceMessage, 240) ||
      `Dispatching GitHub Actions stock screener above the ${fmtCompactCurrency(minMarketCap)} floor.`;
    await ref.set(patch, { merge: true });
    const refreshed = await ref.get();
    const merged = { id: refreshed.id, ...(refreshed.data() || {}) } as Record<string, unknown>;
    await syncScreenerMyRequestFromRun(viewerUid, cleanRunId, merged, false);
    return merged;
  }

  const workflowJobs = await listGithubJobsForRun(workflowRun.id).catch(() => []);
  if (workflowJobs.length) {
    Object.assign(patch, buildGithubWorkflowProgressSnapshot(workflowJobs));
  }

  if (workflowRun.status !== "completed") {
    patch.status = workflowRun.status === "queued" ? "queued" : "running";
    patch.serviceMessage =
      sanitizeText(patch.workflowProgress, 240) ||
      `GitHub Actions is ${patch.status} for the ${fmtCompactCurrency(minMarketCap)} market-cap floor.`;
    await ref.set(patch, { merge: true });
    const refreshed = await ref.get();
    const merged = { id: refreshed.id, ...(refreshed.data() || {}) } as Record<string, unknown>;
    await syncScreenerMyRequestFromRun(viewerUid, cleanRunId, merged, false);
    return merged;
  }

  if (workflowRun.conclusion !== "success") {
    patch.status = "failed";
    patch.autoPublishPending = false;
    patch.serviceMessage = `GitHub Actions finished with ${workflowRun.conclusion || "failure"}.`;
    await ref.set(patch, { merge: true });
    const refreshed = await ref.get();
    const merged = { id: refreshed.id, ...(refreshed.data() || {}) } as Record<string, unknown>;
    await syncScreenerMyRequestFromRun(viewerUid, cleanRunId, merged, false);
    return merged;
  }

  const artifacts = await listGithubArtifactsForRun(workflowRun.id).catch(() => []);
  const artifact = artifacts.find((item) => item.name === "daily-prophet-signal-tracker" && !item.expired) || artifacts[0] || null;
  if (!artifact) {
    patch.status = "running";
    patch.serviceMessage = "GitHub Actions finished, but Quantura is still waiting for artifacts.";
    await ref.set(patch, { merge: true });
    const refreshed = await ref.get();
    const merged = { id: refreshed.id, ...(refreshed.data() || {}) } as Record<string, unknown>;
    await syncScreenerMyRequestFromRun(viewerUid, cleanRunId, merged, false);
    return merged;
  }

  const artifactPayload = await downloadGithubArtifactPayload(artifact);
  const results = normalizeWorkflowScreenerRows(artifactPayload.activeSignals);
  const allLatestRows = normalizeWorkflowScreenerRows(artifactPayload.allLatestRows);
  const manifest = artifactPayload.manifest;
  const serviceMessage = buildGithubScreenerServiceMessage(manifest, minMarketCap, results.length);
  const topSymbols = extractScreenerTopSymbols(results);
  const shouldAutoPublishNow = asBoolean(data.autoPublishPending, false);
  patch.status = sanitizeText(manifest.status, 80).toLowerCase().startsWith("skipped_")
    ? "completed"
    : "completed";
  patch.autoPublishPending = false;
  patch.serviceMessage = serviceMessage;
  patch.results = results;
  patch.resultsFound = results.length;
  patch.topSymbols = topSymbols;
  patch.appliedFilters = [`Market cap >= ${fmtCompactCurrency(minMarketCap)}`];
  patch.ignoredFilters = [];
  patch.workflowArtifactId = artifact.id;
  patch.workflowArtifactName = artifact.name;
  patch.workflowArtifactSizeInBytes = artifact.sizeInBytes;
  patch.workflowManifest = trimOutputsMeta(manifest);
  patch.workflowLogExcerpt = sanitizeText(artifactPayload.workflowLog.split(/\r?\n/).slice(-20).join(" "), 2400);
  patch.screenedCount = Math.max(0, Math.floor(asFinite(manifest.screenedCount, allLatestRows.length)));
  patch.marketCapSkipped = Math.max(0, Math.floor(asFinite(manifest.marketCapSkipped, 0)));
  patch.allLatestRowsCount = allLatestRows.length;

  await ref.set(patch, { merge: true });
  const refreshed = await ref.get();
  const merged = (refreshed.data() || {}) as Record<string, unknown>;
  await syncScreenerMyRequestFromRun(viewerUid, cleanRunId, { ...merged, id: cleanRunId }, shouldAutoPublishNow);
  const requestSnap = await db
    .collection("users")
    .doc(viewerUid)
    .collection("requests")
    .doc(buildMyRequestDocId("screener", cleanRunId))
    .get();
  const requestData = (requestSnap.data() || {}) as Record<string, unknown>;
  const published = asBoolean(requestData.published, shouldAutoPublishNow);
  await ref.set(
    {
      isPublic: published,
      published,
      publishedAt: published ? requestData.publishedAt || admin.firestore.FieldValue.serverTimestamp() : null,
      explorePostId: published ? sanitizeText(requestData.explorePostId, 220) : "",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  const finalized = await ref.get();
  return { id: finalized.id, ...(finalized.data() || {}) };
}

function trimModelCouncilAnswer(answerRaw: unknown): string {
  const answer = sanitizeText(answerRaw, 2600);
  if (answer.length <= 700) return answer;
  return `${answer.slice(0, 697)}...`;
}

async function syncLegacyRequestsForUser(uid: string): Promise<void> {
  const cleanUid = sanitizeText(uid, 220);
  if (!cleanUid) return;

  const requestsRef = db.collection("users").doc(cleanUid).collection("requests");
  const [existingSnap, forecastSnap, screenerSnap, councilSnap] = await Promise.all([
    requestsRef.limit(240).get(),
    db.collection("forecast_requests").where("userId", "==", cleanUid).limit(120).get(),
    db.collection("screener_runs").where("userId", "==", cleanUid).limit(120).get(),
    db.collection(MODEL_COUNCIL_RESPONSE_COLLECTION).where("userId", "==", cleanUid).limit(120).get(),
  ]);

  const existingMap = new Map<string, Record<string, unknown>>();
  existingSnap.docs.forEach((doc) => {
    existingMap.set(doc.id, (doc.data() || {}) as Record<string, unknown>);
  });

  const postIds = [
    ...forecastSnap.docs.map((doc) => `forecast_${doc.id}`),
    ...screenerSnap.docs.map((doc) => `screener_${doc.id}`),
  ];
  const postMap = new Map<string, Record<string, unknown>>();
  if (postIds.length) {
    const postDocs = await db.getAll(...postIds.map((postId) => db.collection("posts").doc(postId)));
    postDocs.forEach((doc) => {
      if (!doc.exists) return;
      postMap.set(doc.id, (doc.data() || {}) as Record<string, unknown>);
    });
  }

  const writes: Array<Promise<unknown>> = [];
  const queueSync = (
    requestId: string,
    seed: {
      type: MyRequestType;
      title: string;
      input: Record<string, unknown>;
      outputsMeta: Record<string, unknown>;
      sourceRef: Record<string, unknown>;
      published: boolean;
      publishedAt: unknown;
      explorePostId: string;
      createdAt: unknown;
      updatedAt: unknown;
    }
  ) => {
    const existing = existingMap.get(requestId) || {};
    const existingShare = normalizeMyRequestShareObject(existing.share);
    const existingTitleEdited = asBoolean(existing.titleEdited, false);
    const existingTitle = sanitizeText(existing.title, 180);
    const nextTitle = existingTitleEdited && existingTitle ? existingTitle : sanitizeText(seed.title, 180) || defaultMyRequestTitle(seed.type, seed.input);
    const existingPublished = asBoolean(existing.published, false);
    const nextPublished = existingPublished || seed.published;
    const ticker = firstTickerFromRequest(seed.input, seed.sourceRef, seed.outputsMeta);
    const nextInput = normalizeMyRequestInput(seed.input);
    const nextOutputsMeta = trimOutputsMeta({
      ...seed.outputsMeta,
      ticker,
    });
    const nextShare = {
      visibility: normalizeMyRequestVisibility(existingShare.visibility, "private"),
      slug: normalizeShareId(existingShare.slug),
      createdAt: existingShare.createdAt || null,
    };
    const nextCreatedAt = existing.createdAt || seed.createdAt || admin.firestore.FieldValue.serverTimestamp();
    const seedUpdatedMs = getTimestampMs(seed.updatedAt || seed.createdAt);
    const existingUpdatedMs = getTimestampMs(existing.updatedAt || existing.createdAt);
    const nextUpdatedAt = existingUpdatedMs > seedUpdatedMs ? existing.updatedAt || existing.createdAt : timestampFromMs(seedUpdatedMs);

    const payload: Record<string, unknown> = {
      type: seed.type,
      ownerUid: cleanUid,
      title: nextTitle,
      titleEdited: existingTitleEdited,
      input: nextInput,
      outputsMeta: nextOutputsMeta,
      sourceRef: {
        collection: sanitizeText(seed.sourceRef.collection, 80),
        id: sanitizeText(seed.sourceRef.id, 220),
      },
      searchText: buildMyRequestSearchText(nextTitle, seed.type, ticker, nextInput, nextOutputsMeta),
      published: nextPublished,
      publishedAt:
        nextPublished
          ? existing.publishedAt || seed.publishedAt || seed.createdAt || admin.firestore.FieldValue.serverTimestamp()
          : null,
      explorePostId: nextPublished ? sanitizeText(existing.explorePostId || seed.explorePostId, 220) : "",
      deleted: asBoolean(existing.deleted, false),
      share: nextShare,
      createdAt: nextCreatedAt,
      updatedAt: nextUpdatedAt || admin.firestore.FieldValue.serverTimestamp(),
      visibility: normalizeMyRequestVisibility(existing.visibility, nextShare.visibility),
    };

    writes.push(requestsRef.doc(requestId).set(payload, { merge: true }));
  };

  forecastSnap.docs.forEach((doc) => {
    const data = (doc.data() || {}) as Record<string, unknown>;
    const ticker = normalizeTicker(data.ticker);
    const postId = `forecast_${doc.id}`;
    const postData = postMap.get(postId) || {};
    const postVisibility = sanitizeText(postData.visibility, 20).toLowerCase();
    const published = postVisibility === "public";
    const metricsRaw = asPlainObject(data.metrics);
    const metrics: Record<string, unknown> = {};
    ["lastClose", "medianEnd", "mae", "rmse", "mape", "coverage10_90", "historyPoints", "drift", "volatility"].forEach((key) => {
      const value = metricsRaw[key];
      if (value === null || value === undefined || value === "") return;
      if (typeof value === "number" && Number.isFinite(value)) {
        metrics[key] = value;
        return;
      }
      if (typeof value === "string") metrics[key] = sanitizeText(value, 80);
    });
    const requestId = buildMyRequestDocId("forecast", doc.id);
    queueSync(requestId, {
      type: "forecast",
      title: sanitizeText(data.title, 160) || `${ticker || "Ticker"} forecast`,
      input: {
        ticker,
        interval: sanitizeText(data.interval, 20),
        horizon: asFinite(data.horizon, 0) || null,
        service: sanitizeText(data.service, 40),
        quantiles: Array.isArray(data.quantiles) ? data.quantiles.slice(0, 12) : [],
      },
      outputsMeta: {
        summary: sanitizeText(data.serviceMessage || data.notes || "", 320),
        serviceMessage: sanitizeText(data.serviceMessage, 320),
        service: sanitizeText(data.service, 40),
        interval: sanitizeText(data.interval, 20),
        forecastRowsCount: Array.isArray(data.forecastRows) ? data.forecastRows.length : asFinite(data.forecastRowsCount, 0),
        metrics,
      },
      sourceRef: {
        collection: "forecast_requests",
        id: doc.id,
      },
      published,
      publishedAt: published ? postData.createdAt || data.createdAt : null,
      explorePostId: published ? postId : "",
      createdAt: data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: data.updatedAt || data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    });
  });

  screenerSnap.docs.forEach((doc) => {
    const data = (doc.data() || {}) as Record<string, unknown>;
    const topSymbols = extractScreenerTopSymbols(data.results);
    const postId = `screener_${doc.id}`;
    const postData = postMap.get(postId) || {};
    const postVisibility = sanitizeText(postData.visibility, 20).toLowerCase();
    const published = postVisibility === "public";
    const requestId = buildMyRequestDocId("screener", doc.id);
    queueSync(requestId, {
      type: "screener",
      title: sanitizeText(data.title, 160) || "Screener run",
      input: {
        universe: sanitizeText(data.universe, 40),
        market: sanitizeText(data.market, 20),
        maxNames: asFinite(data.maxNames, 0) || null,
        notes: sanitizeText(data.notes, 1200),
        model: sanitizeText(data.modelUsed || data.model, 80),
        filters: trimOutputsMeta(data.filters),
      },
      outputsMeta: {
        summary: sanitizeText(data.notes, 320),
        resultsCount: Array.isArray(data.results) ? data.results.length : asFinite(data.resultsFound, 0),
        topSymbols,
        modelUsed: sanitizeText(data.modelUsed || data.model, 80),
      },
      sourceRef: {
        collection: "screener_runs",
        id: doc.id,
      },
      published,
      publishedAt: published ? postData.createdAt || data.createdAt : null,
      explorePostId: published ? postId : "",
      createdAt: data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: data.updatedAt || data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    });
  });

  councilSnap.docs.forEach((doc) => {
    const data = (doc.data() || {}) as Record<string, unknown>;
    const ticker = normalizeTicker(data.ticker);
    const requestId = buildMyRequestDocId("modelCouncil", doc.id);
    queueSync(requestId, {
      type: "modelCouncil",
      title: sanitizeText(data.title, 160) || `${ticker || "Ticker"} Model Council`,
      input: {
        ticker,
        question: sanitizeText(data.question, 4000),
        provider: sanitizeText(data.provider, 80),
        model: sanitizeText(data.model, 120),
        language: sanitizeText(data.language, 20),
        modules: Array.isArray(data.selectedModules) ? data.selectedModules.slice(0, 24) : [],
      },
      outputsMeta: {
        summary: trimModelCouncilAnswer(data.answer),
        answer: sanitizeRichText(data.answer, 12000),
        bodyMarkdown: sanitizeRichText(data.answer, 12000),
        provider: sanitizeText(data.provider, 80),
        model: sanitizeText(data.model, 120),
        citationsCount: Array.isArray(data.citations) ? data.citations.length : 0,
      },
      sourceRef: {
        collection: MODEL_COUNCIL_RESPONSE_COLLECTION,
        id: doc.id,
      },
      published: false,
      publishedAt: null,
      explorePostId: "",
      createdAt: data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: data.updatedAt || data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    });
  });

  if (writes.length) {
    await Promise.all(writes);
  }
}

async function readMyRequestForOwner(uid: string, requestId: string): Promise<{ id: string; data: Record<string, unknown> } | null> {
  const cleanUid = sanitizeText(uid, 220);
  const cleanRequestId = normalizeMyRequestId(requestId);
  if (!cleanUid || !cleanRequestId) return null;
  const snap = await db.collection("users").doc(cleanUid).collection("requests").doc(cleanRequestId).get();
  if (!snap.exists) return null;
  const data = (snap.data() || {}) as Record<string, unknown>;
  if (asString(data.ownerUid) && asString(data.ownerUid) !== cleanUid) return null;
  return { id: snap.id, data };
}

function requestPostType(type: MyRequestType): PostType {
  if (type === "forecast") return "forecast";
  if (type === "screener") return "screener";
  return "agent";
}

function deriveMyRequestExplorePostId(requestId: string, data: Record<string, unknown>): string {
  const existing = sanitizeText(data.explorePostId, 220);
  if (existing) return existing;

  const type = normalizeMyRequestType(data.type) || "forecast";
  const sourceRef = asPlainObject(data.sourceRef);
  const sourceCollection = sanitizeText(sourceRef.collection, 80);
  const sourceId = sanitizeText(sourceRef.id, 220);
  if (type === "forecast" && sourceCollection === "forecast_requests" && sourceId) return `forecast_${sourceId}`;
  if (type === "forecast" && sourceCollection === "autopilot_requests" && sourceId) return `autopilot_${sourceId}`;
  if (type === "screener" && sourceCollection === "screener_runs" && sourceId) return `screener_${sourceId}`;
  if (type === "modelCouncil" && sourceId) return `model_council_${sourceId}`;
  if (type === "indicator" && sourceId) return `indicator_${sourceId}`;
  return `request_${normalizeMyRequestId(requestId)}`;
}

function buildMyRequestTargetUrl(requestId: string, data: Record<string, unknown>): string {
  const type = normalizeMyRequestType(data.type) || "forecast";
  const sourceRef = asPlainObject(data.sourceRef);
  const sourceCollection = sanitizeText(sourceRef.collection, 80);
  const sourceId = sanitizeText(sourceRef.id, 220);
  const input = normalizeMyRequestInput(data.input);
  const outputsMeta = trimOutputsMeta(data.outputsMeta);

  if (type === "forecast") {
    if (sourceCollection === "autopilot_requests" && sourceId) {
      const sportsPanel =
        sanitizeText(input.panel || outputsMeta.panel, 80).toLowerCase() === "sports-autopilot" ||
        sanitizeText(input.sourceGroup || outputsMeta.sourceGroup, 40).toLowerCase() === "sports";
      if (sportsPanel) {
        return `/dashboard?panel=sports-autopilot&runId=${encodeURIComponent(sourceId)}`;
      }
      return `/autopilot?runId=${encodeURIComponent(sourceId)}`;
    }
    if (sourceCollection === "forecast_requests" && sourceId) {
      return `/forecasting?forecastId=${encodeURIComponent(sourceId)}`;
    }
    if (sourceCollection === "autopilot_requests") {
      const sportsPanel =
        sanitizeText(input.panel || outputsMeta.panel, 80).toLowerCase() === "sports-autopilot" ||
        sanitizeText(input.sourceGroup || outputsMeta.sourceGroup, 40).toLowerCase() === "sports";
      if (sportsPanel) {
        return `/dashboard?panel=sports-autopilot&requestId=${encodeURIComponent(requestId)}`;
      }
      return `/autopilot?requestId=${encodeURIComponent(requestId)}`;
    }
    return `/forecasting?requestId=${encodeURIComponent(requestId)}`;
  }
  if (type === "screener") {
    if (sourceCollection === "screener_runs" && sourceId) {
      return `/screener?runId=${encodeURIComponent(sourceId)}`;
    }
    return `/screener?requestId=${encodeURIComponent(requestId)}`;
  }
  if (type === "indicator") {
    return `/indicators?requestId=${encodeURIComponent(requestId)}`;
  }
  if (sourceCollection === MODEL_COUNCIL_RESPONSE_COLLECTION && sourceId) {
    return `/model-council?responseId=${encodeURIComponent(sourceId)}`;
  }
  return `/model-council?requestId=${encodeURIComponent(requestId)}`;
}

async function upsertExplorePostFromMyRequest(
  ownerUid: string,
  requestId: string,
  requestData: Record<string, unknown>,
  visibility: Visibility
): Promise<string> {
  const type = normalizeMyRequestType(requestData.type) || "forecast";
  const input = normalizeMyRequestInput(requestData.input);
  const outputsMeta = trimOutputsMeta(requestData.outputsMeta);
  const sourceRef = asPlainObject(requestData.sourceRef);
  const ticker = firstTickerFromRequest(input, sourceRef, outputsMeta);
  const title = sanitizeText(requestData.title, 180) || defaultMyRequestTitle(type, input);
  const caption = buildMyRequestExploreCaption(type, title, input, outputsMeta, ticker);
  const postId = deriveMyRequestExplorePostId(requestId, requestData);
  const postType = requestPostType(type);
  const body = buildMyRequestExploreBody(type, outputsMeta);
  const sourceCollection = sanitizeText(sourceRef.collection, 80);
  const sourceId = sanitizeText(sourceRef.id, 220);
  const topSymbols = Array.isArray(outputsMeta.topSymbols) ? outputsMeta.topSymbols : [];
  const tickers = Array.from(
    new Set(
      [ticker, ...topSymbols.map((item) => normalizeTicker(item))]
        .map((item) => normalizeTicker(item))
        .filter(Boolean)
    )
  ).slice(0, 8) as string[];
  const tags = Array.from(new Set([type.toLowerCase(), ...tickers.map((item) => item.toLowerCase())]))
    .filter(Boolean)
    .slice(0, 12);
  const targetUrl = buildMyRequestTargetUrl(requestId, requestData);
  const { handle, photoURL } = await readAuthorProfile(ownerUid);
  const postRef = db.collection("posts").doc(postId);
  const existingSnap = await postRef.get();
  const existingData = existingSnap.exists ? ((existingSnap.data() || {}) as Record<string, unknown>) : {};
  const existingCounts = normalizeCounts(existingData.counts);
  const publishTimestamp = admin.firestore.FieldValue.serverTimestamp();
  const createdAt = existingData.createdAt || publishTimestamp;
  const updatedAt = publishTimestamp;
  const createdAtMsForScore = existingData.createdAt ? getTimestampMs(existingData.createdAt) : Date.now();
  const mergedPreviewMetrics = {
    ...buildMyRequestPreviewMetrics({ ...input, ...outputsMeta }, postType),
    ...compactPreviewMetrics(asPlainObject(outputsMeta.metrics)),
  };
  const preview = extractPreview(
    {
      ...input,
      ...outputsMeta,
      metrics: Object.keys(mergedPreviewMetrics).length ? mergedPreviewMetrics : outputsMeta.metrics,
      summary: caption,
    },
    postType
  );

  const postPatch: Record<string, unknown> = {
    id: postId,
    type: postType,
    authorUid: ownerUid,
    authorHandle: handle,
    authorPhotoURL: photoURL,
    title,
    caption,
    tickers,
    tags,
    preview,
    targetUrl,
    visibility,
    updatedAt,
    createdAt,
    counts: existingCounts,
    score: Number.isFinite(asFinite(existingData.score, NaN))
      ? asFinite(existingData.score, 0)
      : computeScore(existingCounts, createdAtMsForScore),
    lastEngagedAt: publishTimestamp,
  };
  if (body) {
    postPatch.body = body;
    postPatch.bodyFormat = "markdown";
  }
  if (sourceCollection || sourceId) {
    postPatch.sourceRef = {
      collection: sourceCollection,
      id: sourceId,
    };
  }

  await postRef.set(postPatch, { merge: true });

  return postId;
}

async function ensurePublishedMyRequestExplorePost(
  ownerUid: string,
  requestId: string,
  requestData: Record<string, unknown>
): Promise<Record<string, unknown>> {
  const published = asBoolean(requestData.published, false);
  const explorePostId = sanitizeText(requestData.explorePostId, 220);
  const visibility = normalizeMyRequestVisibility(requestData.visibility, "private");
  if (!published || (explorePostId && visibility === "public")) {
    return requestData;
  }

  const postId = await upsertExplorePostFromMyRequest(
    ownerUid,
    requestId,
    { ...requestData, visibility: "public" },
    "public"
  );
  const requestRef = db.collection("users").doc(ownerUid).collection("requests").doc(requestId);
  await requestRef.set(
    {
      published: true,
      publishedAt: requestData.publishedAt || admin.firestore.FieldValue.serverTimestamp(),
      explorePostId: postId,
      visibility: "public",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  const refreshed = await requestRef.get();
  return (refreshed.data() || requestData) as Record<string, unknown>;
}

function isAnonymousDecodedUser(user: admin.auth.DecodedIdToken | null): boolean {
  return sanitizeText(user?.firebase?.sign_in_provider, 40) === "anonymous";
}

async function requireFoundryUser(req: Request): Promise<admin.auth.DecodedIdToken> {
  const user = await verifyRequestUser(req, true);
  if (!user) throw new Error("unauthenticated");
  if (isAnonymousDecodedUser(user)) {
    throw new Error("full_account_required");
  }
  return user;
}

function safePathSegment(value: unknown, maxLen = 80): string {
  return sanitizeText(value, maxLen)
    .replace(/[^A-Za-z0-9._-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, maxLen);
}

function storageBucket() {
  const configuredBucket =
    normalizeStorageBucketName((admin.app().options as { storageBucket?: unknown })?.storageBucket) ||
    DEFAULT_STORAGE_BUCKET;
  return configuredBucket ? admin.storage().bucket(configuredBucket) : admin.storage().bucket();
}

const FOUNDRY_TEXT_ARTIFACT_CHUNK_CHARS = 180_000;

function foundryTextArtifactRef(runId: string, fileKey: string) {
  return db.collection("autopilot_requests").doc(runId).collection("text_artifacts").doc(fileKey);
}

function splitFoundryTextArtifact(text: string): string[] {
  const chunks: string[] = [];
  for (let index = 0; index < text.length; index += FOUNDRY_TEXT_ARTIFACT_CHUNK_CHARS) {
    chunks.push(text.slice(index, index + FOUNDRY_TEXT_ARTIFACT_CHUNK_CHARS));
  }
  return chunks.length ? chunks : [""];
}

async function writeFoundryFirestoreTextArtifact(
  runId: string,
  fileKeyRaw: string,
  text: string,
  contentType: string,
  fileName = ""
): Promise<Record<string, unknown>> {
  const cleanRunId = sanitizeText(runId, 220);
  const cleanFileKey = safePathSegment(fileKeyRaw, 80) || "artifact";
  if (!cleanRunId) throw new Error("Run ID is required for Firestore artifacts.");
  const chunks = splitFoundryTextArtifact(text || "");
  const artifactRef = foundryTextArtifactRef(cleanRunId, cleanFileKey);
  const batch = db.batch();
  batch.set(
    artifactRef,
    {
      runId: cleanRunId,
      fileKey: cleanFileKey,
      fileName: sanitizeText(fileName, 240) || `${cleanFileKey}.txt`,
      contentType: sanitizeText(contentType, 120) || "text/plain",
      sizeBytes: Buffer.byteLength(text || "", "utf8"),
      chunkCount: chunks.length,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  chunks.forEach((chunk, index) => {
    batch.set(artifactRef.collection("chunks").doc(String(index).padStart(4, "0")), {
      text: chunk,
      order: index,
    });
  });
  await batch.commit();
  return {
    artifactStore: "firestore",
    firestoreKey: cleanFileKey,
    fileName: sanitizeText(fileName, 240) || `${cleanFileKey}.txt`,
    contentType: sanitizeText(contentType, 120) || "text/plain",
    sizeBytes: Buffer.byteLength(text || "", "utf8"),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

async function readFoundryFirestoreTextArtifact(runId: string, fileKeyRaw: unknown): Promise<string> {
  const cleanRunId = sanitizeText(runId, 220);
  const cleanFileKey = safePathSegment(fileKeyRaw, 80);
  if (!cleanRunId || !cleanFileKey) return "";
  const artifactRef = foundryTextArtifactRef(cleanRunId, cleanFileKey);
  const artifactSnap = await artifactRef.get();
  if (!artifactSnap.exists) return "";
  const chunkCount = Math.max(0, Math.floor(asFinite((artifactSnap.data() || {}).chunkCount, 0)));
  if (!chunkCount) return "";
  const chunkRefs = Array.from({ length: chunkCount }, (_, index) =>
    artifactRef.collection("chunks").doc(String(index).padStart(4, "0"))
  );
  const chunkSnaps = await db.getAll(...chunkRefs);
  return chunkSnaps
    .map((snap) => asString((snap.data() || {}).text))
    .join("");
}

async function readFoundryTextArtifact(runId: string, fileRaw: unknown): Promise<string> {
  const file = asPlainObject(fileRaw);
  const firestoreKey = safePathSegment(file.firestoreKey || file.fileKey, 80);
  if (sanitizeText(file.artifactStore, 40) === "firestore" && firestoreKey) {
    return readFoundryFirestoreTextArtifact(runId, firestoreKey);
  }
  const storagePath = sanitizeText(file.storagePath, 1000).replace(/^\/+/, "");
  if (!storagePath) return "";
  return readStorageTextArtifact(storagePath);
}

async function writeStorageTextArtifact(
  storagePath: string,
  text: string,
  contentType: string
): Promise<Record<string, unknown>> {
  const cleanPath = sanitizeText(storagePath, 1000).replace(/^\/+/, "");
  if (!cleanPath) throw new Error("Storage path is required.");
  const bucket = storageBucket();
  await bucket.file(cleanPath).save(text, {
    resumable: false,
    contentType,
    metadata: {
      cacheControl: "private, max-age=0, no-cache",
    },
  });
  return {
    storagePath: cleanPath,
    fileName: cleanPath.split("/").pop() || "",
    contentType,
    sizeBytes: Buffer.byteLength(text || "", "utf8"),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

async function readStorageTextArtifact(storagePath: string): Promise<string> {
  const cleanPath = sanitizeText(storagePath, 1000).replace(/^\/+/, "");
  if (!cleanPath) return "";
  const [buffer] = await storageBucket().file(cleanPath).download();
  return buffer.toString("utf8");
}

async function buildStorageFileResponse(fileRaw: unknown, runId = "", fileKey = ""): Promise<Record<string, unknown>> {
  const file = asPlainObject(fileRaw);
  const storagePath = sanitizeText(file.storagePath, 1000).replace(/^\/+/, "");
  const response: Record<string, unknown> = {
    fileName: sanitizeText(file.fileName, 240),
    storagePath,
    contentType: sanitizeText(file.contentType, 120),
    sizeBytes: Number.isFinite(asFinite(file.sizeBytes, NaN)) ? asFinite(file.sizeBytes, 0) : null,
    s3Uri: sanitizeText(file.s3Uri, 500),
    artifactStore: sanitizeText(file.artifactStore, 40),
    firestoreKey: safePathSegment(file.firestoreKey || file.fileKey, 80),
  };
  if (sanitizeText(file.artifactStore, 40) === "firestore" && runId && fileKey) {
    response.apiTextPath = `/api/autopilot/runs/${encodeURIComponent(runId)}/files/${encodeURIComponent(fileKey)}/text`;
    return response;
  }
  const existingDownloadUrl = sanitizeText(file.downloadUrl, 2000);
  if (existingDownloadUrl) {
    response.downloadUrl = existingDownloadUrl;
    return response;
  }
  if (!storagePath) return response;
  try {
    const [url] = await storageBucket().file(storagePath).getSignedUrl({
      action: "read",
      expires: Date.now() + 6 * 60 * 60 * 1000,
    });
    response.downloadUrl = url;
  } catch (_error) {
    // Ignore signed URL generation failures.
  }
  return response;
}

function isSportsAutopilotData(data: Record<string, unknown>): boolean {
  return sanitizeText(data.sourceGroup, 40).toLowerCase() === "sports" || sanitizeText(data.sourceType, 60).toLowerCase() === "sports_timeseries";
}

function buildSportsSyntheticTicker(sportsRaw: unknown, datasetRaw: unknown): string {
  const sports = asPlainObject(sportsRaw);
  const team = asPlainObject(sports.team);
  const player = asPlainObject(sports.player);
  const league = sanitizeText(sports.leagueKey || sports.league, 20).toUpperCase();
  const teamAbbreviation = sanitizeText(team.abbreviation, 20).toUpperCase();
  const playerId = sanitizeText(player.id, 40);
  const value = `${league}-${teamAbbreviation}-${playerId}`.replace(/[^A-Z0-9.=^/-]/gi, "-");
  return normalizeTicker(value || datasetRaw);
}

function buildSportsRunTitle(data: Record<string, unknown>): string {
  const explicit = sanitizeText(data.title, 180);
  if (explicit) return explicit;
  const sports = asPlainObject(data.sports);
  const team = asPlainObject(sports.team);
  const player = asPlainObject(sports.player);
  const stat = asPlainObject(sports.stat);
  const targetGame = asPlainObject(sports.targetGame);
  const playerName = sanitizeText(player.displayName, 120) || "Player";
  const statLabel = sanitizeText(stat.label, 80) || "Stat";
  const opponent = sanitizeText(targetGame.opponentAbbreviation, 20).toUpperCase() || sanitizeText(targetGame.opponentDisplayName, 80);
  const teamAbbreviation = sanitizeText(team.abbreviation, 20).toUpperCase();
  return sanitizeText(`${playerName} ${statLabel} vs ${opponent || "Opponent"} (${teamAbbreviation || "Sports"})`, 180);
}

function buildAutopilotTitle(data: Record<string, unknown>): string {
  if (isSportsAutopilotData(data)) {
    return buildSportsRunTitle(data);
  }
  const explicit = sanitizeText(data.title, 180);
  if (explicit) return explicit;
  const dataset = asPlainObject(data.dataset);
  const ticker = normalizeTicker(dataset.ticker || data.ticker || data.symbol);
  const sourceType = sanitizeText(data.sourceType, 40);
  if (sourceType === "prediction_csv") {
    return `${ticker || "Prediction"} Forecast Foundry analysis`;
  }
  return `${ticker || "Market"} Forecast Foundry`;
}

const FOUNDRY_MODEL_METRIC_FIELDS = [
  { key: "status", label: "Model status", aliases: ["status", "modelStatus", "model_status"] },
  { key: "avgWql", label: "Avg. wQL", aliases: ["avgWql", "avg_wql", "avgWQL", "averageWeightedQuantileLoss"] },
  { key: "mape", label: "MAPE", aliases: ["mape"] },
  { key: "wape", label: "WAPE", aliases: ["wape"] },
  { key: "rmse", label: "RMSE", aliases: ["rmse"] },
  { key: "mase", label: "MASE", aliases: ["mase"] },
] as const;

function firstFoundryMetricValue(source: Record<string, unknown>, aliases: readonly string[]): unknown {
  for (const alias of aliases) {
    if (Object.prototype.hasOwnProperty.call(source, alias)) {
      return source[alias];
    }
  }
  return undefined;
}

function normalizeFoundryModelMetrics(raw: unknown): Record<string, string | number> {
  const source = asPlainObject(raw);
  const metrics: Record<string, string | number> = {};
  const rawStatus = sanitizeText(firstFoundryMetricValue(source, ["status", "modelStatus", "model_status"]), 120);
  const statusLower = rawStatus.toLowerCase();
  if (statusLower === "standard" || statusLower === "standard build") metrics.status = "Standard Build";
  else if (statusLower === "quick" || statusLower === "quick build") metrics.status = "Quick Build";
  else if (rawStatus) metrics.status = rawStatus;
  FOUNDRY_MODEL_METRIC_FIELDS.filter((field) => field.key !== "status").forEach((field) => {
    const value = asFinite(firstFoundryMetricValue(source, field.aliases), Number.NaN);
    if (Number.isFinite(value)) metrics[field.key] = Number(value.toFixed(6));
  });
  return metrics;
}

function buildFoundryModelMetricsDisplay(raw: unknown): Record<string, string | number> {
  const normalized = normalizeFoundryModelMetrics(raw);
  const display: Record<string, string | number> = {};
  FOUNDRY_MODEL_METRIC_FIELDS.forEach((field) => {
    const value = normalized[field.key];
    if (typeof value === "number" && Number.isFinite(value)) {
      display[field.label] = value;
      return;
    }
    const text = sanitizeText(value, 120);
    if (text) display[field.label] = text;
  });
  return display;
}

function buildAutopilotSummary(data: Record<string, unknown>): string {
  if (isSportsAutopilotData(data)) {
    const analysis = asPlainObject(data.analysis);
    const sports = asPlainObject(data.sports);
    const player = asPlainObject(sports.player);
    const stat = asPlainObject(sports.stat);
    const targetGame = asPlainObject(sports.targetGame);
    const playerName = sanitizeText(player.displayName, 120) || "Player";
    const statLabel = sanitizeText(stat.label, 80) || "Stat";
    const opponent = sanitizeText(targetGame.opponentAbbreviation, 20).toUpperCase() || sanitizeText(targetGame.opponentDisplayName, 80);
    const status = sanitizeText(data.status, 40).toLowerCase();
    const analysisSummary = sanitizeText(analysis.summary, 600);
    if (analysisSummary) return analysisSummary;
    if (status === "completed") {
      return sanitizeText(`${playerName} ${statLabel.toLowerCase()} forecast against ${opponent || "the selected opponent"} is ready.`, 600);
    }
    if (status === "failed") {
      return sanitizeText(`${playerName} sports forecast failed. ${sanitizeText(asPlainObject(data.autopilot).failureReason, 320)}`, 600);
    }
    return sanitizeText(`${playerName} ${statLabel.toLowerCase()} forecast is ${status || "in progress"} for ${opponent || "the selected opponent"}.`, 600);
  }
  const analysis = asPlainObject(data.analysis);
  const autopilot = asPlainObject(data.autopilot);
  const dataset = asPlainObject(data.dataset);
  const ticker = normalizeTicker(dataset.ticker || data.ticker || data.symbol);
  const status = sanitizeText(data.status, 40).toLowerCase();
  const analysisSummary = sanitizeText(analysis.summary, 600);
  if (analysisSummary) return analysisSummary;
  if (status === "completed") {
    const objective = asPlainObject(autopilot.objectiveMetric);
    const metricName = sanitizeText(objective.name, 120) || "AverageWeightedQuantileLoss";
    const metricValue = asFinite(objective.value, NaN);
    return sanitizeText(
      `${ticker || "Forecast"} Forecast Foundry run completed.${Number.isFinite(metricValue) ? ` ${metricName}: ${metricValue}.` : ""}`,
      600
    );
  }
  if (status === "failed") {
    return sanitizeText(
      `${ticker || "Forecast"} Forecast Foundry run failed. ${sanitizeText(autopilot.failureReason || autopilot.transformFailureReason, 320)}`,
      600
    );
  }
  if (status === "analysis_ready") {
    return sanitizeText(`${ticker || "Forecast"} Forecast Foundry prediction analysis is ready.`, 600);
  }
  if (status === "dataset_ready") {
    return sanitizeText(`${ticker || "Forecast"} historical dataset is ready for Forecast Foundry.`, 600);
  }
  return sanitizeText(`${ticker || "Forecast"} Forecast Foundry run is ${status || "in progress"}.`, 600);
}

function buildAutopilotInputPayload(data: Record<string, unknown>): Record<string, unknown> {
  if (isSportsAutopilotData(data)) {
    const sports = asPlainObject(data.sports);
    const team = asPlainObject(sports.team);
    const player = asPlainObject(sports.player);
    const stat = asPlainObject(sports.stat);
    const targetGame = asPlainObject(sports.targetGame);
    return {
      ticker: sanitizeText(team.abbreviation, 20).toUpperCase(),
      sourceType: sanitizeText(data.sourceType, 40),
      sourceGroup: "sports",
      panel: "sports-autopilot",
      league: sanitizeText(sports.leagueKey || sports.league, 20).toLowerCase(),
      leagueLabel: sanitizeText(sports.leagueLabel, 40),
      teamId: sanitizeText(team.id, 40),
      teamAbbreviation: sanitizeText(team.abbreviation, 20).toUpperCase(),
      teamName: sanitizeText(team.displayName, 120),
      playerId: sanitizeText(player.id, 40),
      playerName: sanitizeText(player.displayName, 120),
      statKey: sanitizeText(stat.key, 120),
      statLabel: sanitizeText(stat.label, 80),
      gameId: sanitizeText(targetGame.id, 40),
      gameDate: sanitizeText(targetGame.date, 80),
      opponent: sanitizeText(targetGame.opponentAbbreviation, 20).toUpperCase() || sanitizeText(targetGame.opponentDisplayName, 120),
      notes: sanitizeText(data.notes, 2000),
      mode: sanitizeText(data.mode, 60),
    };
  }
  const dataset = asPlainObject(data.dataset);
  const autopilot = asPlainObject(data.autopilot);
  const modelMetrics = buildFoundryModelMetricsDisplay(data.modelMetrics);
  return {
    ticker: normalizeTicker(dataset.ticker || data.ticker),
    interval: sanitizeText(dataset.interval, 20),
    horizon: Number.isFinite(asFinite(autopilot.forecastHorizon, NaN)) ? Math.floor(asFinite(autopilot.forecastHorizon, 0)) : null,
    quantiles: Array.isArray(autopilot.quantiles) ? autopilot.quantiles.slice(0, 5) : [],
    notes: sanitizeText(data.notes, 2000),
    sourceType: sanitizeText(data.sourceType, 40),
    mode: sanitizeText(data.mode, 60),
    modelMetrics,
  };
}

function buildAutopilotOutputsMeta(data: Record<string, unknown>): Record<string, unknown> {
  if (isSportsAutopilotData(data)) {
    const sports = asPlainObject(data.sports);
    const team = asPlainObject(sports.team);
    const player = asPlainObject(sports.player);
    const stat = asPlainObject(sports.stat);
    const targetGame = asPlainObject(sports.targetGame);
    const analysis = asPlainObject(data.analysis);
    const autopilot = asPlainObject(data.autopilot);
    const files = asPlainObject(data.files);
    const analysisMetrics = asPlainObject(analysis.metrics);
    const forecastValue = asFinite(analysisMetrics.forecastValue, Number.NaN);
    const lowerBound = asFinite(analysisMetrics.lowerBound, Number.NaN);
    const upperBound = asFinite(analysisMetrics.upperBound, Number.NaN);
    const metrics: Record<string, unknown> = {
      League: sanitizeText(sports.leagueLabel, 40),
      Team: sanitizeText(team.abbreviation, 20).toUpperCase(),
      Opponent: sanitizeText(targetGame.opponentAbbreviation, 20).toUpperCase() || sanitizeText(targetGame.opponentDisplayName, 80),
      Stat: sanitizeText(stat.label, 80),
      Games: Math.max(0, Math.floor(asFinite(asPlainObject(data.dataset).rowCount, 0))),
    };
    if (Number.isFinite(forecastValue)) metrics.Forecast = Number(forecastValue.toFixed(4));
    if (Number.isFinite(lowerBound)) metrics.Low = Number(lowerBound.toFixed(4));
    if (Number.isFinite(upperBound)) metrics.High = Number(upperBound.toFixed(4));
    if (sanitizeText(autopilot.status, 40)) metrics.Status = sanitizeText(autopilot.status, 40);
    const fileNames = Object.values(files)
      .map((entry) => sanitizeText(asPlainObject(entry).fileName, 240))
      .filter(Boolean)
      .slice(0, 10);
    return trimOutputsMeta({
      summary: buildAutopilotSummary(data),
      service: "aws-sagemaker-autopilot",
      provider: "aws-sagemaker-autopilot",
      sourceGroup: "sports",
      panel: "sports-autopilot",
      league: sanitizeText(sports.leagueKey || sports.league, 20).toLowerCase(),
      leagueLabel: sanitizeText(sports.leagueLabel, 40),
      teamAbbreviation: sanitizeText(team.abbreviation, 20).toUpperCase(),
      playerName: sanitizeText(player.displayName, 120),
      statKey: sanitizeText(stat.key, 120),
      statLabel: sanitizeText(stat.label, 80),
      gameDate: sanitizeText(targetGame.date, 80),
      opponent: sanitizeText(targetGame.opponentAbbreviation, 20).toUpperCase() || sanitizeText(targetGame.opponentDisplayName, 80),
      ticker: sanitizeText(team.abbreviation, 20).toUpperCase(),
      topSymbols: [sanitizeText(team.abbreviation, 20).toUpperCase()].filter(Boolean),
      metrics,
      analysisMarkdown: asString(analysis.markdown).slice(0, 32000),
      analysisStatus: sanitizeText(analysis.status, 40),
      fileNames,
    });
  }
  const dataset = asPlainObject(data.dataset);
  const autopilot = asPlainObject(data.autopilot);
  const analysis = asPlainObject(data.analysis);
  const files = asPlainObject(data.files);
  const modelMetrics = buildFoundryModelMetricsDisplay(data.modelMetrics);
  const objective = asPlainObject(autopilot.objectiveMetric);
  const bestCandidate = asPlainObject(autopilot.bestCandidate);
  const metrics: Record<string, unknown> = {};
  const objectiveValue = asFinite(objective.value, NaN);
  if (Number.isFinite(objectiveValue)) {
    metrics[sanitizeText(objective.name, 120) || "AverageWeightedQuantileLoss"] = objectiveValue;
  }
  const branch = sanitizeText(analysis.metrics && asPlainObject(analysis.metrics).branch, 40);
  if (branch) metrics.Branch = branch;
  const recommendation = sanitizeText(analysis.metrics && asPlainObject(analysis.metrics).recommendation, 40).toUpperCase();
  if (recommendation) metrics.Signal = recommendation;
  const endDateBias = sanitizeText(analysis.metrics && asPlainObject(analysis.metrics).endDateBias, 80);
  if (endDateBias) metrics["End Bias"] = endDateBias.replace(/_/g, " ");
  const rowCount = asFinite(dataset.rowCount, 0);
  if (rowCount > 0) metrics.Rows = Math.floor(rowCount);
  const candidateName = sanitizeText(bestCandidate.candidateName, 120);
  if (candidateName) metrics.Candidate = candidateName;
  Object.entries(modelMetrics).forEach(([key, value]) => {
    if (!(key in metrics)) metrics[key] = value;
  });
  const fileNames = Object.values(files)
    .map((entry) => sanitizeText(asPlainObject(entry).fileName, 240))
    .filter(Boolean)
    .slice(0, 8);
  return trimOutputsMeta({
    summary: buildAutopilotSummary(data),
    service: "aws-sagemaker-autopilot",
    provider: "aws-sagemaker-autopilot",
    model: candidateName,
    interval: sanitizeText(dataset.interval, 20),
    forecastRowsCount: asFinite(analysis.rowCount, 0) || null,
    topSymbols: [normalizeTicker(dataset.ticker || data.ticker)].filter(Boolean),
    ticker: normalizeTicker(dataset.ticker || data.ticker),
    metrics,
    analysisMarkdown: asString(analysis.markdown).slice(0, 32000),
    analysisStatus: sanitizeText(analysis.status, 40),
    fileNames,
  });
}

async function upsertOwnedMyRequestFromSystem(
  ownerUid: string,
  seed: {
    requestId: string;
    type: MyRequestType;
    title: string;
    input: Record<string, unknown>;
    outputsMeta: Record<string, unknown>;
    sourceRef: Record<string, unknown>;
    published?: boolean;
  }
): Promise<string> {
  const requestId = normalizeMyRequestId(seed.requestId);
  if (!ownerUid || !requestId) throw new Error("My Request upsert requires owner UID and request ID.");
  const requestRef = db.collection("users").doc(ownerUid).collection("requests").doc(requestId);
  const existingSnap = await requestRef.get();
  const existing = (existingSnap.data() || {}) as Record<string, unknown>;
  const type = normalizeMyRequestType(seed.type) || "forecast";
  const input = normalizeMyRequestInput(seed.input);
  const outputsMeta = trimOutputsMeta(seed.outputsMeta);
  const titleEdited = asBoolean(existing.titleEdited, false);
  const title = titleEdited
    ? sanitizeText(existing.title, 180) || defaultMyRequestTitle(type, input)
    : sanitizeText(seed.title, 180) || sanitizeText(existing.title, 180) || defaultMyRequestTitle(type, input);
  const share = normalizeMyRequestShareObject(existing.share);
  const sourceRef = {
    collection: sanitizeText(seed.sourceRef.collection, 80),
    id: sanitizeText(seed.sourceRef.id, 220),
  };
  const ticker = firstTickerFromRequest(input, sourceRef, outputsMeta);
  const published =
    typeof seed.published === "boolean" ? asBoolean(seed.published, false) : asBoolean(existing.published, false);
  const visibility = published
    ? "public"
    : normalizeMyRequestVisibility(existing.visibility, normalizeMyRequestVisibility(share.visibility, "private"));

  const payload: Record<string, unknown> = {
    type,
    ownerUid,
    title,
    titleEdited,
    input,
    outputsMeta,
    sourceRef,
    searchText: buildMyRequestSearchText(title, type, ticker, input, outputsMeta),
    published,
    publishedAt: published ? existing.publishedAt || admin.firestore.FieldValue.serverTimestamp() : null,
    explorePostId: published ? sanitizeText(existing.explorePostId, 220) : "",
    deleted: asBoolean(existing.deleted, false),
    share: {
      visibility: normalizeMyRequestVisibility(share.visibility, "private"),
      slug: normalizeShareId(share.slug),
      createdAt: share.createdAt || null,
    },
    visibility,
    createdAt: existing.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  await requestRef.set(payload, { merge: true });
  if (published) {
    const merged = { ...existing, ...payload };
    const postId = await upsertExplorePostFromMyRequest(ownerUid, requestId, merged, "public");
    await requestRef.set(
      {
        published: true,
        publishedAt: existing.publishedAt || admin.firestore.FieldValue.serverTimestamp(),
        explorePostId: postId,
        visibility: "public",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
  }
  return requestId;
}

async function syncAutopilotMyRequest(ownerUid: string, runId: string, data: Record<string, unknown>): Promise<string> {
  const requestId = buildMyRequestDocId("forecast", runId);
  const shouldAutoPublishSportsRun =
    isSportsAutopilotData(data) &&
    asBoolean(data.autoPublishToExplore, true) &&
    (sanitizeText(data.status, 60).toLowerCase() === "completed" ||
      sanitizeText(asPlainObject(data.analysis).status, 40).toLowerCase() === "ok");
  await upsertOwnedMyRequestFromSystem(ownerUid, {
    requestId,
    type: "forecast",
    title: buildAutopilotTitle(data),
    input: buildAutopilotInputPayload(data),
    outputsMeta: buildAutopilotOutputsMeta(data),
    sourceRef: {
      collection: "autopilot_requests",
      id: runId,
    },
    published: shouldAutoPublishSportsRun,
  });
  return requestId;
}

const MAX_FOUNDRY_CONCURRENT_RUNS = 2;
const ACTIVE_FOUNDRY_STATUSES = new Set(["queued", "running", "transforming"]);

async function countActiveAutopilotRunsForOwner(ownerUid: string, excludeRunId = ""): Promise<number> {
  const cleanOwnerUid = sanitizeText(ownerUid, 220);
  const cleanExcludeRunId = sanitizeText(excludeRunId, 220);
  if (!cleanOwnerUid) return 0;
  const snap = await db
    .collection("autopilot_requests")
    .where("userId", "==", cleanOwnerUid)
    .orderBy("createdAt", "desc")
    .get();
  return snap.docs.reduce((count, doc) => {
    if (cleanExcludeRunId && doc.id === cleanExcludeRunId) return count;
    const data = (doc.data() || {}) as Record<string, unknown>;
    const status = sanitizeText(data.status, 60).toLowerCase();
    return ACTIVE_FOUNDRY_STATUSES.has(status) ? count + 1 : count;
  }, 0);
}

async function readAutopilotRunForOwner(
  ownerUid: string,
  runId: string
): Promise<{ id: string; data: Record<string, unknown> } | null> {
  const cleanRunId = sanitizeText(runId, 220);
  if (!ownerUid || !cleanRunId) return null;
  const snap = await db.collection("autopilot_requests").doc(cleanRunId).get();
  if (!snap.exists) return null;
  const data = (snap.data() || {}) as Record<string, unknown>;
  if (sanitizeText(data.userId, 220) !== ownerUid) return null;
  return { id: snap.id, data };
}

async function toAutopilotRunResponse(docId: string, data: Record<string, unknown>): Promise<Record<string, unknown>> {
  const dataset = asPlainObject(data.dataset);
  const autopilot = asPlainObject(data.autopilot);
  const analysis = asPlainObject(data.analysis);
  const sports = asPlainObject(data.sports);
  const filesRaw = asPlainObject(data.files);
  const filesEntries = await Promise.all(
    Object.entries(filesRaw).map(async ([key, value]) => [key, await buildStorageFileResponse(value, docId, key)] as const)
  );
  const files = Object.fromEntries(filesEntries);
  const createdAtMs = getTimestampMs(data.createdAt);
  const updatedAtMs = getTimestampMs(data.updatedAt || data.createdAt);
  return {
    id: docId,
    title: buildAutopilotTitle(data),
    status: sanitizeText(data.status, 60),
    mode: sanitizeText(data.mode, 60),
    sourceType: sanitizeText(data.sourceType, 60),
    userId: sanitizeText(data.userId, 220),
    workspaceId: sanitizeText(data.workspaceId, 220),
    notes: sanitizeText(data.notes, 2000),
    modelMetrics: buildFoundryModelMetricsDisplay(data.modelMetrics),
    exploreRequestId: sanitizeText(data.exploreRequestId, 220),
    createdAtMs,
    updatedAtMs,
    createdAt: createdAtMs ? new Date(createdAtMs).toISOString() : "",
    updatedAt: updatedAtMs ? new Date(updatedAtMs).toISOString() : "",
    dataset: {
      ticker: normalizeTicker(dataset.ticker || data.ticker),
      interval: sanitizeText(dataset.interval, 20),
      rowCount: Math.max(0, Math.floor(asFinite(dataset.rowCount, 0))),
      trainingEligible: asBoolean(dataset.trainingEligible, false),
      start: sanitizeText(dataset.start, 40),
      end: sanitizeText(dataset.end, 40),
      useAllHistory: asBoolean(dataset.useAllHistory, false),
      columns: Array.isArray(dataset.columns) ? dataset.columns.slice(0, 20) : [],
      previewRows: Array.isArray(dataset.previewRows) ? dataset.previewRows.slice(0, 30) : [],
      sourceTimeColumn: sanitizeText(dataset.sourceTimeColumn, 120),
      sourceValueColumn: sanitizeText(dataset.sourceValueColumn, 120),
      sourceItemColumn: sanitizeText(dataset.sourceItemColumn, 120),
      originalHeaders: Array.isArray(dataset.originalHeaders) ? dataset.originalHeaders.slice(0, 30) : [],
    },
    autopilot: {
      status: sanitizeText(autopilot.status, 60),
      jobName: sanitizeText(autopilot.jobName, 120),
      jobArn: sanitizeText(autopilot.jobArn, 220),
      forecastFrequency: sanitizeText(autopilot.forecastFrequency, 20),
      forecastHorizon: Math.max(0, Math.floor(asFinite(autopilot.forecastHorizon, 0))),
      quantiles: Array.isArray(autopilot.quantiles) ? autopilot.quantiles.slice(0, 5) : [],
      algorithms: Array.isArray(autopilot.algorithms) ? autopilot.algorithms.slice(0, 12) : [],
      runtimeSeconds: Number.isFinite(asFinite(autopilot.runtimeSeconds, Number.NaN))
        ? Math.max(0, Math.floor(asFinite(autopilot.runtimeSeconds, Number.NaN)))
        : null,
      objectiveMetric: asPlainObject(autopilot.objectiveMetric),
      bestCandidate: asPlainObject(autopilot.bestCandidate),
      modelName: sanitizeText(autopilot.modelName, 120),
      transformJobName: sanitizeText(autopilot.transformJobName, 120),
      transformStatus: sanitizeText(autopilot.transformStatus, 60),
      transformOutputS3Uri: sanitizeText(autopilot.transformOutputS3Uri, 500),
      failureReason: sanitizeText(autopilot.failureReason || autopilot.transformFailureReason, 500),
    },
    analysis: {
      status: sanitizeText(analysis.status, 40),
      summary: sanitizeText(analysis.summary, 2000),
      markdown: asString(analysis.markdown).slice(0, 32000),
      metrics: trimOutputsMeta(analysis.metrics),
      data: trimOutputsMeta(analysis.data),
      previewRows: Array.isArray(analysis.previewRows) ? analysis.previewRows.slice(0, 20) : [],
      generatedAtMs: getTimestampMs(analysis.generatedAt),
    },
    sports: isSportsAutopilotData(data)
      ? {
          league: sanitizeText(sports.leagueKey || sports.league, 20).toLowerCase(),
          leagueLabel: sanitizeText(sports.leagueLabel, 40),
          team: trimOutputsMeta(asPlainObject(sports.team)),
          player: trimOutputsMeta(asPlainObject(sports.player)),
          stat: trimOutputsMeta(asPlainObject(sports.stat)),
          targetGame: trimOutputsMeta(asPlainObject(sports.targetGame)),
          seasonsUsed: Array.isArray(sports.seasonsUsed) ? sports.seasonsUsed.slice(0, 4) : [],
          historicalPreviewRows: Array.isArray(sports.historicalPreviewRows) ? sports.historicalPreviewRows.slice(0, 20) : [],
          recentHistoryRows: Array.isArray(sports.recentHistoryRows) ? sports.recentHistoryRows.slice(-20) : [],
          futureGames: Array.isArray(sports.futureGames) ? sports.futureGames.slice(0, 20) : [],
          statCatalog: Array.isArray(sports.statCatalog) ? sports.statCatalog.slice(0, 40) : [],
        }
      : {},
    files,
  };
}

function buildAutopilotAnalysisPatch(analysis: Record<string, unknown>): Record<string, unknown> {
  return {
    ...analysis,
    generatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

async function persistAutopilotFirestoreAnalysisArtifacts(
  runId: string,
  analysis: Record<string, unknown>,
  options: { businessDayCsvText?: string } = {}
): Promise<{ analysisPatch: Record<string, unknown>; filePatches: Record<string, unknown> }> {
  const filePatches: Record<string, unknown> = {};
  const markdown = asString(analysis.markdown).slice(0, 32000);
  const jsonText = JSON.stringify(analysis.data || {}, null, 2);
  const businessDayCsvText = asString(options.businessDayCsvText);

  if (markdown) {
    filePatches.analysisMarkdown = await writeFoundryFirestoreTextArtifact(
      runId,
      "analysisMarkdown",
      markdown,
      "text/markdown",
      "analysis.md"
    );
  }

  filePatches.analysisJson = await writeFoundryFirestoreTextArtifact(
    runId,
    "analysisJson",
    jsonText,
    "application/json",
    "analysis.json"
  );
  if (businessDayCsvText.trim()) {
    filePatches.businessDaysCsv = await writeFoundryFirestoreTextArtifact(
      runId,
      "businessDaysCsv",
      businessDayCsvText,
      "text/csv",
      "business_days_predictions.csv"
    );
  }

  return {
    analysisPatch: buildAutopilotAnalysisPatch(analysis),
    filePatches,
  };
}

async function persistAutopilotAnalysisArtifacts(
  ownerUid: string,
  runId: string,
  analysis: Record<string, unknown>,
  predictionsCsvText = "",
  options: { businessDayCsvText?: string } = {}
): Promise<{ analysisPatch: Record<string, unknown>; filePatches: Record<string, unknown> }> {
  const owner = safePathSegment(ownerUid, 120) || "user";
  const run = safePathSegment(runId, 120) || "run";
  const reportBase = `forecast_reports/${owner}/foundry/${run}`;
  const filePatches: Record<string, unknown> = {};
  const businessDayCsvText = asString(options.businessDayCsvText);

  if (predictionsCsvText.trim()) {
    filePatches.predictionsCsv = await writeStorageTextArtifact(
      `${reportBase}/predictions.csv`,
      predictionsCsvText,
      "text/csv"
    );
  }
  if (businessDayCsvText.trim()) {
    filePatches.businessDaysCsv = await writeStorageTextArtifact(
      `${reportBase}/business_days_predictions.csv`,
      businessDayCsvText,
      "text/csv"
    );
  }

  const markdown = asString(analysis.markdown).slice(0, 32000);
  const jsonText = JSON.stringify(analysis.data || {}, null, 2);
  if (markdown) {
    filePatches.analysisMarkdown = await writeStorageTextArtifact(
      `${reportBase}/analysis.md`,
      markdown,
      "text/markdown"
    );
  }
  filePatches.analysisJson = await writeStorageTextArtifact(
    `${reportBase}/analysis.json`,
    jsonText,
    "application/json"
  );

  return {
    analysisPatch: buildAutopilotAnalysisPatch(analysis),
    filePatches,
  };
}

async function persistSportsForecastPayloadArtifact(
  ownerUid: string,
  runId: string,
  payload: Record<string, unknown>
): Promise<Record<string, unknown>> {
  const owner = safePathSegment(ownerUid, 120) || "user";
  const run = safePathSegment(runId, 120) || "run";
  return writeStorageTextArtifact(
    `forecast_reports/${owner}/foundry/${run}/sports_forecast_payload.json`,
    JSON.stringify(payload, null, 2),
    "application/json"
  );
}

async function reconcileAutopilotRunDocument(
  runId: string,
  existingData: Record<string, unknown>
): Promise<Record<string, unknown>> {
  const ownerUid = sanitizeText(existingData.userId, 220);
  if (!ownerUid) return existingData;
  const currentStatus = sanitizeText(existingData.status, 60).toLowerCase();
  const files = asPlainObject(existingData.files);
  const dataset = asPlainObject(existingData.dataset);
  const autopilot = asPlainObject(existingData.autopilot);
  let nextPatch: Record<string, unknown> = {};

  if (currentStatus === "completed" && files.predictionsCsv && asPlainObject(existingData.analysis).status === "ok") {
    const requestId = await syncAutopilotMyRequest(ownerUid, runId, existingData);
    if (sanitizeText(existingData.exploreRequestId, 220) !== requestId) {
      nextPatch.exploreRequestId = requestId;
      await db.collection("autopilot_requests").doc(runId).set(
        {
          exploreRequestId: requestId,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      await syncAutomationRunProjection(runId, {
        ...existingData,
        exploreRequestId: requestId,
      });
      return {
        ...existingData,
        exploreRequestId: requestId,
      };
    }
    await syncAutomationRunProjection(runId, existingData);
    return existingData;
  }

  if (sanitizeText(autopilot.jobName, 120)) {
    const refresh = await refreshAutopilotRun({
      runId,
      userId: ownerUid,
      ticker: normalizeTicker(dataset.ticker || existingData.ticker),
      datasetS3Uri: sanitizeText(asPlainObject(files.datasetCsv).s3Uri || autopilot.inputS3Uri, 500),
      autopilot,
    });
    nextPatch.autopilot = {
      ...autopilot,
      ...refresh.autopilotPatch,
    };
    nextPatch.status = refresh.status;

    if (refresh.status === "completed" && refresh.predictionsCsvText.trim()) {
      if (isSportsAutopilotData(existingData)) {
        const sports = asPlainObject(existingData.sports);
        const team = asPlainObject(sports.team);
        const player = asPlainObject(sports.player);
        const stat = asPlainObject(sports.stat);
        const targetGame = asPlainObject(sports.targetGame);
        const recentHistoryRows = Array.isArray(sports.recentHistoryRows)
          ? (sports.recentHistoryRows as NormalizedSportsHistoryRow[])
          : [];
        const analysis = analyzeSportsPredictionCsv(refresh.predictionsCsvText, {
          syntheticTicker: buildSportsSyntheticTicker(sports, dataset),
          statKey: sanitizeText(stat.key, 120),
          statLabel: sanitizeText(stat.label, 80),
          leagueLabel: sanitizeText(sports.leagueLabel, 40),
          playerName: sanitizeText(player.displayName, 120),
          teamAbbreviation: sanitizeText(team.abbreviation, 20).toUpperCase(),
          opponentAbbreviation: sanitizeText(targetGame.opponentAbbreviation, 20).toUpperCase(),
          targetGameDate: sanitizeText(targetGame.date, 80),
          targetGameLabel: sanitizeText(targetGame.label, 160) || sanitizeText(targetGame.displayDate, 120),
          historicalRows: recentHistoryRows,
        });
        const persisted = await persistAutopilotAnalysisArtifacts(
          ownerUid,
          runId,
          {
            status: analysis.status,
            summary: analysis.summary,
            markdown: analysis.markdown,
            metrics: analysis.metrics,
            data: analysis.analysis,
            previewRows: analysis.previewRows,
            rowCount: analysis.rowCount,
            columns: analysis.columns,
          },
          refresh.predictionsCsvText,
          {
            businessDayCsvText: asString((analysis as any).businessDayCsvText),
          }
        );
        const forecastPayloadFile = await persistSportsForecastPayloadArtifact(ownerUid, runId, {
          input: buildAutopilotInputPayload(existingData),
          output: analysis.analysis,
          metrics: analysis.metrics,
        });
        nextPatch.analysis = persisted.analysisPatch;
        nextPatch.files = {
          ...files,
          ...persisted.filePatches,
          forecastPayloadJson: forecastPayloadFile,
        };
        nextPatch.status = "completed";
      } else {
        const analysis = await analyzePredictionCsv(refresh.predictionsCsvText, {
          ticker: normalizeTicker(dataset.ticker || existingData.ticker),
        });
        const persisted = await persistAutopilotAnalysisArtifacts(
          ownerUid,
          runId,
          {
            status: analysis.status,
            summary: analysis.summary,
            markdown: analysis.markdown,
            metrics: analysis.metrics,
            data: analysis.analysis,
            previewRows: analysis.previewRows,
            rowCount: analysis.rowCount,
            columns: analysis.columns,
          },
          refresh.predictionsCsvText,
          {
            businessDayCsvText: asString((analysis as any).businessDayCsvText),
          }
        );

        nextPatch.analysis = persisted.analysisPatch;
        nextPatch.files = {
          ...files,
          ...persisted.filePatches,
        };
        nextPatch.status = "completed";
      }
    }
  } else if (sanitizeText(existingData.sourceType, 40) === "prediction_csv" && sanitizeText(asPlainObject(existingData.analysis).status, 40) !== "ok") {
    const uploadedCsv = asPlainObject(files.uploadedCsv);
    const csvText = await readFoundryTextArtifact(runId, uploadedCsv);
    if (csvText.trim()) {
      const analysis = await analyzePredictionCsv(csvText, {
        ticker: normalizeTicker(dataset.ticker || existingData.ticker),
      });
      const persisted = await persistAutopilotFirestoreAnalysisArtifacts(runId, {
        status: analysis.status,
        summary: analysis.summary,
        markdown: analysis.markdown,
        metrics: analysis.metrics,
        data: analysis.analysis,
        previewRows: analysis.previewRows,
        rowCount: analysis.rowCount,
        columns: analysis.columns,
      }, {
        businessDayCsvText: asString((analysis as any).businessDayCsvText),
      });
      nextPatch.analysis = persisted.analysisPatch;
      nextPatch.files = {
        ...files,
        ...persisted.filePatches,
      };
      nextPatch.status = "analysis_ready";
    }
  }

  if (!Object.keys(nextPatch).length) {
    await syncAutomationRunProjection(runId, existingData);
    return existingData;
  }

  nextPatch.updatedAt = admin.firestore.FieldValue.serverTimestamp();
  await db.collection("autopilot_requests").doc(runId).set(nextPatch, { merge: true });
  const refreshedSnap = await db.collection("autopilot_requests").doc(runId).get();
  const refreshedData = (refreshedSnap.data() || { ...existingData, ...nextPatch }) as Record<string, unknown>;
  const requestId = await syncAutopilotMyRequest(ownerUid, runId, refreshedData);
  if (sanitizeText(refreshedData.exploreRequestId, 220) !== requestId) {
    await db.collection("autopilot_requests").doc(runId).set(
      {
        exploreRequestId: requestId,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    await syncAutomationRunProjection(runId, {
      ...refreshedData,
      exploreRequestId: requestId,
    });
    return {
      ...refreshedData,
      exploreRequestId: requestId,
    };
  }
  await syncAutomationRunProjection(runId, refreshedData);
  return refreshedData;
}

function automationEntitlementRef(ownerUid: string) {
  return db.collection(AUTOMATION_ENTITLEMENT_COLLECTION).doc(sanitizeText(ownerUid, 220));
}

function automationRef(automationId?: string) {
  const cleanId = sanitizeText(automationId, 220);
  return cleanId ? db.collection(AUTOMATION_COLLECTION).doc(cleanId) : db.collection(AUTOMATION_COLLECTION).doc();
}

function automationHistoryCollection(automationId: string) {
  return automationRef(automationId).collection(AUTOMATION_HISTORY_SUBCOLLECTION);
}

function normalizeAutomationCadence(value: unknown): string {
  const normalized = sanitizeText(value, 40).toLowerCase() || "daily";
  return AUTOMATION_ALLOWED_CADENCES.has(normalized) ? normalized : "daily";
}

function normalizeAutomationHorizon(value: unknown): string {
  const normalized = sanitizeText(value, 40).toLowerCase() || "1_month";
  return AUTOMATION_ALLOWED_HORIZONS.has(normalized) ? normalized : "1_month";
}

function normalizeAutomationProfile(value: unknown): string {
  const normalized = sanitizeText(value, 40).toLowerCase() || "avg_wql";
  return AUTOMATION_ALLOWED_PROFILES.has(normalized) ? normalized : "avg_wql";
}

function normalizeAutomationModel(value: unknown): string {
  const normalized = sanitizeText(value, 60).toLowerCase() || "avg_wql_all_algorithms";
  return AUTOMATION_ALLOWED_MODELS.has(normalized) ? normalized : "avg_wql_all_algorithms";
}

function automationHorizonPeriods(horizon: string): number {
  switch (normalizeAutomationHorizon(horizon)) {
    case "3_days":
      return 3;
    case "1_week":
      return 5;
    case "2_weeks":
      return 10;
    case "3_weeks":
      return 15;
    case "3_months":
      return 63;
    case "6_months":
      return 126;
    case "1_year":
      return 252;
    case "1_month":
    default:
      return 21;
  }
}

function nextAutomationRunAtMs(fromMs = Date.now()): number {
  return fromMs + 24 * 60 * 60 * 1000;
}

async function countActiveAutomationsForOwner(ownerUid: string, excludeAutomationId = ""): Promise<number> {
  const cleanOwnerUid = sanitizeText(ownerUid, 220);
  const cleanExcludeId = sanitizeText(excludeAutomationId, 220);
  if (!cleanOwnerUid) return 0;
  const snap = await db
    .collection(AUTOMATION_COLLECTION)
    .where("ownerUid", "==", cleanOwnerUid)
    .where("active", "==", true)
    .get();
  return snap.docs.reduce((count, doc) => {
    if (cleanExcludeId && doc.id === cleanExcludeId) return count;
    return count + 1;
  }, 0);
}

async function readAutomationEntitlement(ownerUid: string): Promise<Record<string, unknown>> {
  const snap = await automationEntitlementRef(ownerUid).get();
  return (snap.data() || {}) as Record<string, unknown>;
}

async function resolveAutomationContactEmail(
  ownerUid: string,
  decodedUser: admin.auth.DecodedIdToken,
  explicitEmail?: unknown
): Promise<string> {
  const provided = normalizeEmail(explicitEmail);
  if (provided) return provided;
  const tokenEmail = normalizeEmail(decodedUser.email);
  if (tokenEmail) return tokenEmail;
  const entitlement = await readAutomationEntitlement(ownerUid);
  return normalizeEmail(entitlement.contactEmail);
}

async function sendAutomationUnlockEmail(input: {
  to: string;
  ownerUid: string;
  productId: string;
  purchaseSource: string;
  purchaseReference: string;
}): Promise<boolean> {
  const to = normalizeEmail(input.to);
  if (!to) return false;
  if (!RESEND_API_KEY) {
    console.warn("[Automation] unlock email skipped: RESEND_API_KEY is not configured", {
      ownerUid: sanitizeText(input.ownerUid, 220),
      to,
    });
    return false;
  }
  const sourceLabel = sanitizeText(input.purchaseSource, 40) || "native store";
  const productId = sanitizeText(input.productId, 120) || AUTOMATION_PRODUCT_ID;
  const purchaseReference = sanitizeText(input.purchaseReference, 240);
  const subject = "Quantura Automation unlocked";
  const html = `
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;line-height:1.6;color:#111827;">
      <h2 style="margin:0 0 12px;">Quantura Automation unlocked</h2>
      <p style="margin:0 0 12px;">Your native Forecast Foundry automation access is now active.</p>
      <ul style="margin:0 0 16px 18px;padding:0;">
        <li>Permanent unlock</li>
        <li>Up to ${AUTOMATION_MAX_ACTIVE} active automations</li>
        <li>Daily automation cadence</li>
        <li>Forecast history and status tracking</li>
      </ul>
      <p style="margin:0 0 12px;"><strong>Source:</strong> ${sourceLabel}</p>
      <p style="margin:0 0 12px;"><strong>Product:</strong> ${productId}</p>
      ${purchaseReference ? `<p style="margin:0 0 12px;"><strong>Reference:</strong> ${purchaseReference}</p>` : ""}
      <p style="margin:0;">Open the Quantura mobile app to create or manage your automations.</p>
    </div>
  `.trim();
  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: AUTOMATION_EMAIL_FROM,
      to: [to],
      reply_to: AUTOMATION_EMAIL_REPLY_TO,
      subject,
      html,
    }),
  });
  if (!response.ok) {
    const detail = sanitizeText(await response.text(), 500);
    throw new Error(`automation_email_send_failed:${response.status}${detail ? `:${detail}` : ""}`);
  }
  return true;
}

function buildAutomationEntitlementResponse(
  ownerUid: string,
  entitlement: Record<string, unknown>,
  activeAutomationCount: number
): Record<string, unknown> {
  const unlockedAtMs = getOptionalTimestampMs(entitlement.unlockedAt || entitlement.createdAt);
  const refreshedAtMs = getOptionalTimestampMs(entitlement.updatedAt || entitlement.unlockedAt || entitlement.createdAt);
  return {
    ownerUid: sanitizeText(ownerUid, 220),
    automationUnlocked: asBoolean(entitlement.automationUnlocked, false),
    purchaseSource: sanitizeText(entitlement.purchaseSource, 40),
    unlockedAtMs,
    unlockedAt: unlockedAtMs != null ? new Date(unlockedAtMs).toISOString() : "",
    refreshedAtMs,
    refreshedAt: refreshedAtMs != null ? new Date(refreshedAtMs).toISOString() : "",
    productId: sanitizeText(entitlement.productId, 120) || AUTOMATION_PRODUCT_ID,
    maxActiveAutomations: Math.max(1, Math.floor(asFinite(entitlement.maxActiveAutomations, AUTOMATION_MAX_ACTIVE))),
    activeAutomationCount,
    purchaseReference: sanitizeText(entitlement.purchaseReference, 240),
    contactEmail: normalizeEmail(entitlement.contactEmail),
  };
}

function buildAutomationResponse(docId: string, data: Record<string, unknown>): Record<string, unknown> {
  const createdAtMs = getOptionalTimestampMs(data.createdAt);
  const updatedAtMs = getOptionalTimestampMs(data.updatedAt || data.createdAt);
  const lastRunAtMs = getOptionalTimestampMs(data.lastRunAt);
  const nextRunAtMs = getOptionalTimestampMs(data.nextRunAt);
  return {
    id: docId,
    ticker: normalizeTicker(data.ticker),
    forecastProfile: normalizeAutomationProfile(data.forecastProfile),
    model: normalizeAutomationModel(data.model),
    cadence: normalizeAutomationCadence(data.cadence),
    horizon: normalizeAutomationHorizon(data.horizon),
    active: asBoolean(data.active, false),
    createdAtMs,
    updatedAtMs,
    lastRunAtMs,
    nextRunAtMs,
    createdAt: createdAtMs != null ? new Date(createdAtMs).toISOString() : "",
    updatedAt: updatedAtMs != null ? new Date(updatedAtMs).toISOString() : "",
    lastRunAt: lastRunAtMs != null ? new Date(lastRunAtMs).toISOString() : "",
    nextRunAt: nextRunAtMs != null ? new Date(nextRunAtMs).toISOString() : "",
    lastStatus: sanitizeText(data.lastStatus, 80),
    lastRunId: sanitizeText(data.lastRunId, 220),
    ownerUid: sanitizeText(data.ownerUid, 220),
  };
}

async function readAutomationForOwner(
  ownerUid: string,
  automationId: string
): Promise<{ id: string; data: Record<string, unknown> } | null> {
  const ref = automationRef(automationId);
  const snap = await ref.get();
  if (!snap.exists) return null;
  const data = (snap.data() || {}) as Record<string, unknown>;
  if (sanitizeText(data.ownerUid, 220) !== sanitizeText(ownerUid, 220)) return null;
  return {
    id: snap.id,
    data,
  };
}

function decodeJwtPayloadSegment(input: string): Record<string, unknown> {
  const parts = asString(input).split(".");
  if (parts.length < 2) return {};
  try {
    const payload = Buffer.from(parts[1].replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
    const parsed = JSON.parse(payload);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function base64UrlEncode(input: Buffer | string): string {
  return Buffer.from(input)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function createAppleServerJwt(): string {
  if (!APPLE_IAP_ISSUER_ID || !APPLE_IAP_KEY_ID || !APPLE_IAP_PRIVATE_KEY) {
    throw new Error("apple_iap_server_credentials_missing");
  }
  const header = {
    alg: "ES256",
    kid: APPLE_IAP_KEY_ID,
    typ: "JWT",
  };
  const now = Math.floor(Date.now() / 1000);
  const claims = {
    iss: APPLE_IAP_ISSUER_ID,
    iat: now,
    exp: now + 300,
    aud: "appstoreconnect-v1",
    bid: APPLE_IAP_BUNDLE_ID,
  };
  const signingInput = `${base64UrlEncode(JSON.stringify(header))}.${base64UrlEncode(JSON.stringify(claims))}`;
  const signature = crypto.sign("sha256", Buffer.from(signingInput), crypto.createPrivateKey(APPLE_IAP_PRIVATE_KEY));
  return `${signingInput}.${base64UrlEncode(signature)}`;
}

async function verifyAppleAutomationPurchase(input: {
  transactionId: string;
  productId: string;
}): Promise<{
  productId: string;
  purchaseReference: string;
  unlockedAtMs: number;
  source: "apple";
}> {
  const transactionId = sanitizeText(input.transactionId, 220);
  const requestedProductId = sanitizeText(input.productId, 120) || AUTOMATION_PRODUCT_ID;
  if (!transactionId) {
    throw new Error("missing_transaction_id");
  }
  if (!APPLE_IAP_ISSUER_ID || !APPLE_IAP_KEY_ID || !APPLE_IAP_PRIVATE_KEY) {
    throw new Error("apple_iap_server_credentials_missing");
  }
  const endpoint =
    APPLE_IAP_ENVIRONMENT.toLowerCase() === "sandbox"
      ? `https://api.storekit-sandbox.itunes.apple.com/inApps/v1/transactions/${encodeURIComponent(transactionId)}`
      : `https://api.storekit.itunes.apple.com/inApps/v1/transactions/${encodeURIComponent(transactionId)}`;
  const response = await fetch(endpoint, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${createAppleServerJwt()}`,
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`apple_iap_validation_failed:${response.status}`);
  }
  const payload = (await response.json()) as Record<string, unknown>;
  const signedTransactionInfo = asString(payload.signedTransactionInfo);
  const decoded = decodeJwtPayloadSegment(signedTransactionInfo);
  const bundleId = sanitizeText(decoded.bundleId, 220);
  const productId = sanitizeText(decoded.productId, 120);
  const revocationReason = sanitizeText(decoded.revocationReason, 40);
  const revocationDate = sanitizeText(decoded.revocationDate, 80);
  if (bundleId && bundleId !== APPLE_IAP_BUNDLE_ID) {
    throw new Error("apple_bundle_mismatch");
  }
  if (productId !== requestedProductId || productId !== AUTOMATION_PRODUCT_ID) {
    throw new Error("apple_product_mismatch");
  }
  if (revocationReason || revocationDate) {
    throw new Error("apple_purchase_revoked");
  }
  const purchaseDateMs = Math.max(0, Math.floor(asFinite(decoded.purchaseDate, Date.now())));
  return {
    productId,
    purchaseReference: sanitizeText(decoded.originalTransactionId || decoded.transactionId || transactionId, 240) || transactionId,
    unlockedAtMs: purchaseDateMs || Date.now(),
    source: "apple",
  };
}

async function fetchGooglePlayProductPurchase(input: {
  packageName: string;
  productId: string;
  purchaseToken: string;
}): Promise<Record<string, unknown>> {
  const client = await GOOGLE_PLAY_PUBLISHER_AUTH.getClient();
  const endpoint = `https://androidpublisher.googleapis.com/androidpublisher/v3/applications/${encodeURIComponent(
    input.packageName
  )}/purchases/products/${encodeURIComponent(input.productId)}/tokens/${encodeURIComponent(input.purchaseToken)}`;
  const response = await client.request({
    url: endpoint,
    method: "GET",
  });
  return (response.data || {}) as Record<string, unknown>;
}

async function acknowledgeGooglePlayProductPurchase(input: {
  packageName: string;
  productId: string;
  purchaseToken: string;
  developerPayload: string;
}): Promise<void> {
  const client = await GOOGLE_PLAY_PUBLISHER_AUTH.getClient();
  const endpoint = `https://androidpublisher.googleapis.com/androidpublisher/v3/applications/${encodeURIComponent(
    input.packageName
  )}/purchases/products/${encodeURIComponent(input.productId)}/tokens/${encodeURIComponent(input.purchaseToken)}:acknowledge`;
  await client.request({
    url: endpoint,
    method: "POST",
    data: {
      developerPayload: sanitizeText(input.developerPayload, 220),
    },
  });
}

async function verifyGoogleAutomationPurchase(input: {
  packageName: string;
  productId: string;
  purchaseToken: string;
  ownerUid: string;
}): Promise<{
  productId: string;
  purchaseReference: string;
  unlockedAtMs: number;
  source: "google";
}> {
  const productId = sanitizeText(input.productId, 120) || AUTOMATION_PRODUCT_ID;
  const purchaseToken = sanitizeText(input.purchaseToken, 400);
  const packageName = sanitizeText(input.packageName, 220) || GOOGLE_PLAY_ANDROID_PACKAGE;
  if (!purchaseToken) {
    throw new Error("missing_purchase_token");
  }
  const payload = await fetchGooglePlayProductPurchase({
    packageName,
    productId,
    purchaseToken,
  });
  const purchaseState = Math.floor(asFinite(payload.purchaseState, NaN));
  if (!Number.isFinite(purchaseState) || purchaseState !== 0) {
    throw new Error("google_purchase_not_completed");
  }
  const acknowledgementState = Math.floor(asFinite(payload.acknowledgementState, NaN));
  if (!Number.isFinite(acknowledgementState) || acknowledgementState === 0) {
    await acknowledgeGooglePlayProductPurchase({
      packageName,
      productId,
      purchaseToken,
      developerPayload: input.ownerUid,
    });
  }
  return {
    productId,
    purchaseReference: sanitizeText(payload.orderId, 240) || purchaseToken,
    unlockedAtMs: Math.max(0, Math.floor(asFinite(payload.purchaseTimeMillis, Date.now()))),
    source: "google",
  };
}

async function syncAutomationEntitlementState(
  ownerUid: string,
  patch: Record<string, unknown>
): Promise<Record<string, unknown>> {
  const activeAutomationCount = await countActiveAutomationsForOwner(ownerUid);
  await automationEntitlementRef(ownerUid).set(
    {
      automationUnlocked: asBoolean(patch.automationUnlocked, false),
      purchaseSource: sanitizeText(patch.purchaseSource, 40),
      unlockedAt: patch.unlockedAt || admin.firestore.FieldValue.serverTimestamp(),
      productId: sanitizeText(patch.productId, 120) || AUTOMATION_PRODUCT_ID,
      maxActiveAutomations: AUTOMATION_MAX_ACTIVE,
      activeAutomationCount,
      purchaseReference: sanitizeText(patch.purchaseReference, 240),
      contactEmail: normalizeEmail(patch.contactEmail),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  return readAutomationEntitlement(ownerUid);
}

async function requireAutomationUnlocked(ownerUid: string): Promise<Record<string, unknown>> {
  const entitlement = await readAutomationEntitlement(ownerUid);
  if (!asBoolean(entitlement.automationUnlocked, false)) {
    throw new Error("automation_locked");
  }
  return entitlement;
}

async function startAutomationForecastRun(
  ownerUid: string,
  automationId: string,
  automationData: Record<string, unknown>,
  trigger: "manual" | "scheduled" | "activation"
): Promise<Record<string, unknown>> {
  await requireAutomationUnlocked(ownerUid);
  const ticker = normalizeTicker(automationData.ticker);
  if (!ticker) {
    throw new Error("invalid_ticker");
  }
  const horizonKey = normalizeAutomationHorizon(automationData.horizon);
  const today = new Date().toISOString().slice(0, 10);
  const dataset = await downloadHistoricalStockDataset({
    ticker,
    interval: "1d",
    start: "",
    end: today,
    useAllHistory: true,
  });
  const runRef = db.collection("autopilot_requests").doc();
  const owner = safePathSegment(ownerUid, 120) || "user";
  const run = safePathSegment(runRef.id, 120) || "run";
  const datasetFile = await writeStorageTextArtifact(
    `predictions/${owner}/automation/${safePathSegment(automationId, 120) || "automation"}/${run}/dataset.csv`,
    dataset.csvText,
    "text/csv"
  );
  const baseDoc: Record<string, unknown> = {
    userId: ownerUid,
    workspaceId: ownerUid,
    title: `${ticker} Quantura Automation`,
    notes: "",
    mode: "mobile_automation_run",
    sourceType: "mobile_automation",
    status: "dataset_ready",
    ticker,
    dataset: {
      ticker: dataset.ticker,
      interval: dataset.interval,
      rowCount: dataset.rowCount,
      columns: dataset.columns,
      previewRows: dataset.previewRows,
      trainingEligible: dataset.trainingEligible,
      sourceTimeColumn: dataset.sourceTimeColumn,
      sourceValueColumn: dataset.sourceValueColumn,
      sourceItemColumn: dataset.sourceItemColumn,
      originalHeaders: dataset.columns,
      start: "",
      end: today,
      useAllHistory: true,
    },
    autopilot: {},
    analysis: {},
    automation: {
      automationId,
      trigger,
      cadence: normalizeAutomationCadence(automationData.cadence),
      horizon: horizonKey,
      forecastProfile: normalizeAutomationProfile(automationData.forecastProfile),
      model: normalizeAutomationModel(automationData.model),
    },
    files: {
      datasetCsv: {
        ...datasetFile,
      },
    },
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  await runRef.set(baseDoc, { merge: false });
  const started = await startAutopilotTraining({
    runId: runRef.id,
    userId: ownerUid,
    ticker,
    interval: "1d",
    horizon: automationHorizonPeriods(horizonKey),
    quantiles: ["0.1", "0.5", "0.9"],
    csvText: dataset.csvText,
  });
  const patch: Record<string, unknown> = {
    status: "running",
    autopilot: {
      jobName: started.jobName,
      jobArn: started.jobArn,
      inputS3Uri: started.inputS3Uri,
      outputS3Uri: started.outputS3Uri,
      forecastFrequency: started.forecastFrequency,
      forecastHorizon: automationHorizonPeriods(horizonKey),
      quantiles: started.quantiles,
      algorithms: started.algorithms,
      runtimeSeconds: started.runtimeSeconds,
      objectiveMetric: {
        name: "AverageWeightedQuantileLoss",
        value: null,
      },
      status: "InProgress",
    },
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  await runRef.set(patch, { merge: true });
  const refreshedSnap = await runRef.get();
  const refreshedData = (refreshedSnap.data() || { ...baseDoc, ...patch }) as Record<string, unknown>;
  const requestId = await syncAutopilotMyRequest(ownerUid, runRef.id, refreshedData);
  await runRef.set(
    {
      exploreRequestId: requestId,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  await automationRef(automationId).collection(AUTOMATION_HISTORY_SUBCOLLECTION).doc(runRef.id).set(
    {
      ownerUid,
      automationId,
      autopilotRunId: runRef.id,
      trigger,
      status: "running",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  await automationRef(automationId).set(
    {
      lastRunAt: admin.firestore.FieldValue.serverTimestamp(),
      lastRunId: runRef.id,
      lastStatus: "running",
      nextRunAt: admin.firestore.Timestamp.fromMillis(nextAutomationRunAtMs()),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  const finalSnap = await runRef.get();
  return toAutopilotRunResponse(runRef.id, (finalSnap.data() || refreshedData) as Record<string, unknown>);
}

async function listAutomationHistoryForOwner(
  ownerUid: string,
  automationId: string,
  limit = 20
): Promise<Record<string, unknown>[]> {
  const snap = await automationRef(automationId)
    .collection(AUTOMATION_HISTORY_SUBCOLLECTION)
    .orderBy("createdAt", "desc")
    .limit(Math.max(1, Math.min(limit, 40)))
    .get();
  const items = await Promise.all(
    snap.docs.map(async (doc) => {
      const data = (doc.data() || {}) as Record<string, unknown>;
      if (sanitizeText(data.ownerUid, 220) !== sanitizeText(ownerUid, 220)) {
        return null;
      }
      const runId = sanitizeText(data.autopilotRunId || doc.id, 220);
      const ownedRun = runId ? await readAutopilotRunForOwner(ownerUid, runId) : null;
      return {
        id: doc.id,
        trigger: sanitizeText(data.trigger, 40),
        status: sanitizeText(asPlainObject(ownedRun?.data || {}).status || data.status, 60),
        createdAtMs: getTimestampMs(data.createdAt),
        updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
        autopilotRunId: runId,
        autopilotRun: ownedRun ? await toAutopilotRunResponse(ownedRun.id, ownedRun.data) : null,
      };
    })
  );
  return items.filter(Boolean) as Record<string, unknown>[];
}

async function syncAutomationRunProjection(runId: string, data: Record<string, unknown>): Promise<void> {
  const automation = asPlainObject(data.automation);
  const automationId = sanitizeText(automation.automationId, 220);
  const ownerUid = sanitizeText(data.userId, 220);
  if (!automationId || !ownerUid) return;
  const status = sanitizeText(data.status, 60) || sanitizeText(asPlainObject(data.autopilot).status, 60) || "queued";
  await automationRef(automationId).set(
    {
      lastRunId: runId,
      lastStatus: status,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      ...(status === "completed" || status === "failed"
        ? {}
        : { nextRunAt: admin.firestore.Timestamp.fromMillis(nextAutomationRunAtMs()) }),
    },
    { merge: true }
  );
  await automationHistoryCollection(automationId).doc(runId).set(
    {
      ownerUid,
      automationId,
      autopilotRunId: runId,
      trigger: sanitizeText(automation.trigger, 40),
      status,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

function computeDecay(recencyHours: number): number {
  const safe = Math.max(0, recencyHours);
  return Math.max(0.05, Math.exp(-safe / 48));
}

function normalizeCounts(raw: unknown): PostCounts {
  const data = (raw || {}) as Partial<PostCounts>;
  return {
    likes: Math.max(0, Math.floor(asFinite(data.likes, 0))),
    comments: Math.max(0, Math.floor(asFinite(data.comments, 0))),
    reposts: Math.max(0, Math.floor(asFinite(data.reposts, 0))),
    shares: Math.max(0, Math.floor(asFinite(data.shares, 0))),
    reports: Math.max(0, Math.floor(asFinite(data.reports, 0))),
  };
}

function computeScore(counts: PostCounts, createdAtMs: number, nowMs = Date.now()): number {
  const base = counts.likes * 3 + counts.comments * 4 + counts.reposts * 5 + counts.shares * 2;
  if (base <= 0) return 0;
  const recencyHours = (nowMs - createdAtMs) / (1000 * 60 * 60);
  const score = base * computeDecay(recencyHours);
  return Number(score.toFixed(6));
}

function encodeCursor(cursor: ExploreCursor | null): string | null {
  if (!cursor) return null;
  const json = JSON.stringify(cursor);
  return Buffer.from(json, "utf8").toString("base64url");
}

function decodeCursor(raw: unknown): ExploreCursor | null {
  if (!raw || typeof raw !== "string") return null;
  try {
    const decoded = Buffer.from(raw, "base64url").toString("utf8");
    const parsed = JSON.parse(decoded) as Partial<ExploreCursor>;
    if (!parsed || typeof parsed !== "object") return null;
    if (!parsed.id || !Number.isFinite(parsed.createdAtMs)) return null;
    const cursor: ExploreCursor = {
      id: String(parsed.id),
      createdAtMs: Number(parsed.createdAtMs),
    };
    if (Number.isFinite(parsed.score)) cursor.score = Number(parsed.score);
    return cursor;
  } catch {
    return null;
  }
}

function decodeFirestoreValue(value: any): unknown {
  if (!value || typeof value !== "object") return null;
  if ("stringValue" in value) return String(value.stringValue);
  if ("booleanValue" in value) return Boolean(value.booleanValue);
  if ("integerValue" in value) return Number(value.integerValue);
  if ("doubleValue" in value) return Number(value.doubleValue);
  if ("timestampValue" in value) return String(value.timestampValue);
  if ("nullValue" in value) return null;
  if ("arrayValue" in value) {
    const values = Array.isArray(value.arrayValue?.values) ? value.arrayValue.values : [];
    return values.map((entry: any) => decodeFirestoreValue(entry));
  }
  if ("mapValue" in value) {
    const fields = value.mapValue?.fields || {};
    const out: Record<string, unknown> = {};
    Object.entries(fields).forEach(([key, child]) => {
      out[key] = decodeFirestoreValue(child);
    });
    return out;
  }
  return null;
}

function decodeFirestoreFields(fields: Record<string, unknown> | undefined): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  if (!fields || typeof fields !== "object") return out;
  Object.entries(fields).forEach(([key, value]) => {
    out[key] = decodeFirestoreValue(value);
  });
  return out;
}

function parseDocumentPath(cloudEvent: any): string {
  const valueName = asString(cloudEvent?.data?.value?.name);
  if (valueName.includes("/documents/")) {
    return valueName.split("/documents/")[1] || "";
  }
  const subject = asString(cloudEvent?.subject);
  if (subject.startsWith("documents/")) {
    return subject.replace(/^documents\//, "");
  }
  return "";
}

function buildMetaPayload(post: PostDoc): Record<string, string> {
  return {
    type: post.type,
    postId: post.id,
    ticker: post.tickers[0] || "",
    url: post.targetUrl,
  };
}

async function sendTopicNotification(topic: string, post: PostDoc, title: string, body: string): Promise<void> {
  try {
    await messaging.send({
      topic,
      notification: {
        title,
        body,
      },
      data: buildMetaPayload(post),
      webpush: {
        fcmOptions: {
          link: post.targetUrl,
        },
      },
      android: {
        notification: {
          channelId: "quantura-default",
          clickAction: post.targetUrl,
        },
      },
      apns: {
        payload: {
          aps: {
            sound: "default",
          },
        },
      },
    });
  } catch (error) {
    console.warn(`[Explore] Topic notification failed for ${topic}:`, error);
  }
}

function extractTickers(payload: Record<string, unknown>): string[] {
  const tickers = new Set<string>();
  const directFields = [payload.ticker, payload.symbol, payload.primaryTicker];
  directFields.forEach((value) => {
    const ticker = normalizeTicker(value);
    if (ticker) tickers.add(ticker);
  });

  const listFields = [payload.tickers, payload.symbols, payload.briefTickers];
  listFields.forEach((entry) => {
    if (!Array.isArray(entry)) return;
    entry.forEach((value) => {
      const ticker = normalizeTicker(value);
      if (ticker) tickers.add(ticker);
    });
  });

  const rows = Array.isArray(payload.results) ? payload.results : Array.isArray(payload.rows) ? payload.rows : [];
  rows.forEach((row) => {
    if (!row || typeof row !== "object") return;
    const ticker = normalizeTicker((row as any).ticker || (row as any).symbol);
    if (ticker) tickers.add(ticker);
  });

  return Array.from(tickers).slice(0, 8);
}

function compactPreviewMetrics(source: Record<string, unknown>): Record<string, string | number> {
  const metrics: Record<string, string | number> = {};
  Object.entries(source || {})
    .slice(0, 6)
    .forEach(([key, value]) => {
      const cleanKey = sanitizeText(key, 40);
      if (!cleanKey) return;
      if (typeof value === "number" && Number.isFinite(value)) {
        metrics[cleanKey] = value;
        return;
      }
      const text = sanitizeText(value, 120);
      if (text) metrics[cleanKey] = text;
    });
  return metrics;
}

function buildMyRequestPreviewMetrics(payload: Record<string, unknown>, postType: PostType): Record<string, string | number> {
  const fallback: Record<string, unknown> = {};
  if (postType === "screener") {
    const count = Array.isArray(payload.results) ? payload.results.length : asFinite(payload.resultsCount || payload.resultsFound, 0);
    if (count > 0) fallback.Results = Math.floor(count);
    const topSymbols = Array.isArray(payload.topSymbols)
      ? payload.topSymbols.map((value) => normalizeTicker(value)).filter(Boolean).slice(0, 3)
      : [];
    if (topSymbols.length) fallback.Top = topSymbols.join(", ");
    const modelUsed = sanitizeText(payload.modelUsed || payload.model, 80);
    if (modelUsed) fallback.Model = modelUsed;
  } else if (postType === "agent") {
    const provider = sanitizeText(payload.provider, 80);
    if (provider) fallback.Provider = provider;
    const model = sanitizeText(payload.model, 80);
    if (model) fallback.Model = model;
    const prediction = sanitizeText(payload.prediction || payload.direction, 80);
    if (prediction) fallback.Direction = prediction;
    const target = sanitizeText(payload.targetPrice || payload.target || payload.Target, 80);
    if (target) fallback.Target = target;
    const confidence = sanitizeText(payload.confidence || payload.Confidence, 80);
    if (confidence) fallback.Confidence = confidence;
    const latencyMs = asFinite(payload.latencyMs, NaN);
    if (Number.isFinite(latencyMs) && latencyMs > 0) {
      fallback.Latency = `${Math.max(1, Math.round(latencyMs / 1000))}s`;
    }
  }
  return compactPreviewMetrics(fallback);
}

function buildMyRequestExploreCaption(
  type: MyRequestType,
  title: string,
  input: Record<string, unknown>,
  outputsMeta: Record<string, unknown>,
  ticker: string
): string {
  const summary = sanitizeText(outputsMeta.summary || outputsMeta.answer || input.question || input.notes, 400);
  if (summary) return summary;
  if (type === "screener") {
    const count = asFinite(outputsMeta.resultsCount || outputsMeta.resultsFound, 0);
    const topSymbols = Array.isArray(outputsMeta.topSymbols)
      ? outputsMeta.topSymbols.map((value) => normalizeTicker(value)).filter(Boolean).slice(0, 4)
      : [];
    return sanitizeText(
      `${title} surfaced ${Math.max(0, Math.floor(count))} ranked candidates.${topSymbols.length ? ` Top names: ${topSymbols.join(", ")}.` : ""}`,
      400
    );
  }
  if (type === "indicator") {
    const direction = sanitizeText(outputsMeta.prediction, 80);
    return sanitizeText(
      `${ticker || title} indicator analysis is ready${direction ? ` with a ${direction} bias.` : "."}`,
      400
    );
  }
  if (type === "modelCouncil") {
    return sanitizeText(`${ticker || title} Model Council response is ready for review.`, 400);
  }
  return sanitizeText(title, 400);
}

function extractPreview(payload: Record<string, unknown>, postType: PostType): PostDoc["preview"] {
  const imageUrl = sanitizeText(payload.imageUrl || payload.chartUrl || payload.previewImage || payload.thumbnailUrl, 1000);
  const metricsSource = payload.metrics && typeof payload.metrics === "object"
    ? (payload.metrics as Record<string, unknown>)
    : payload.summary && typeof payload.summary === "object"
    ? (payload.summary as Record<string, unknown>)
    : null;

  const metrics: Record<string, string | number> = metricsSource ? compactPreviewMetrics(metricsSource) : {};

  if (postType === "screener" && !Object.keys(metrics).length) {
    const count = Array.isArray(payload.results) ? payload.results.length : asFinite(payload.resultsFound, 0);
    if (count > 0) metrics.results = Math.floor(count);
  }

  if (!Object.keys(metrics).length) {
    Object.assign(metrics, buildMyRequestPreviewMetrics(payload, postType));
  }

  if (imageUrl) {
    return Object.keys(metrics).length
      ? { kind: "image", imageUrl, metrics }
      : { kind: "image", imageUrl };
  }

  return Object.keys(metrics).length
    ? { kind: "summary", metrics }
    : { kind: "summary" };
}

function buildTargetUrl(postType: PostType, sourceDocId: string): string {
  switch (postType) {
    case "forecast":
      return `/forecasting?forecastId=${encodeURIComponent(sourceDocId)}`;
    case "backtest":
      return `/indicators?runId=${encodeURIComponent(sourceDocId)}`;
    case "screener":
      return `/screener?runId=${encodeURIComponent(sourceDocId)}`;
    case "agent":
      return `/ticker-query?agentRunId=${encodeURIComponent(sourceDocId)}`;
    default:
      return "/explore";
  }
}

function buildTitle(postType: PostType, payload: Record<string, unknown>, tickers: string[]): string {
  const ticker = tickers[0] || "Market";
  const candidate = sanitizeText(payload.title || payload.agentName || payload.universe || "", 120);
  if (candidate) return candidate;
  switch (postType) {
    case "forecast":
      return `${ticker} forecast update`;
    case "backtest": {
      const strategy = sanitizeText(payload.strategy || "strategy", 40);
      return `${ticker} strategy run (${strategy})`;
    }
    case "screener":
      return `${ticker} screener run`;
    case "agent":
      return `${ticker} AI agent run`;
    default:
      return "New Quantura insight";
  }
}

async function readAuthorProfile(authorUid: string): Promise<{ handle: string; photoURL: string }> {
  try {
    const snap = await db.collection("users").doc(authorUid).get();
    const data = (snap.data() || {}) as Record<string, unknown>;
    const profile = (data.profile || {}) as Record<string, unknown>;

    const handle =
      normalizeHandle(data.handle) ||
      normalizeHandle(profile.username) ||
      normalizeHandle(data.displayName) ||
      `user-${authorUid.slice(0, 8)}`;

    const photoURL = sanitizeText(data.photoURL || profile.photoURL || profile.avatarUrl || "", 1000);

    return { handle, photoURL };
  } catch {
    return {
      handle: `user-${authorUid.slice(0, 8)}`,
      photoURL: "",
    };
  }
}

async function publishAutoPost(post: PostDoc): Promise<void> {
  const title = post.title;
  const subtitle = `${post.authorHandle} • ${post.type.toUpperCase()}`;

  await sendTopicNotification("explore-global", post, title, subtitle);

  for (const ticker of post.tickers.slice(0, 5)) {
    await sendTopicNotification(`ticker-${ticker}`, post, `${ticker}: ${title}`, subtitle);
  }

  await sendTopicNotification(`author-${post.authorUid}`, post, title, subtitle);
}

async function createPostFromResult(postType: PostType, sourceDocId: string, payload: Record<string, unknown>): Promise<void> {
  const authorUid = sanitizeText(payload.authorUid || payload.userId || payload.uid || payload.ownerUid, 120);
  if (!authorUid) {
    console.warn(`[Explore] Skip ${postType}/${sourceDocId}: missing author uid`);
    return;
  }

  const postId = `${postType}_${sourceDocId}`;
  const postRef = db.collection("posts").doc(postId);
  const existing = await postRef.get();
  if (existing.exists) {
    return;
  }

  const { handle, photoURL } = await readAuthorProfile(authorUid);
  const tickers = extractTickers(payload);
  const tags = [postType, ...tickers.map((ticker) => ticker.toLowerCase())].slice(0, 12);
  const title = buildTitle(postType, payload, tickers);
  const caption = sanitizeText(
    payload.caption || payload.notes || payload.summary || payload.agentSummary || payload.description,
    400
  );
  const createdAtMs = getTimestampMs(payload.createdAt || payload.updatedAt);
  const createdAt = timestampFromMs(createdAtMs);

  const post: PostDoc = {
    id: postId,
    type: postType,
    authorUid,
    authorHandle: handle,
    authorPhotoURL: photoURL,
    title,
    caption,
    tickers,
    tags,
    preview: extractPreview(payload, postType),
    targetUrl: buildTargetUrl(postType, sourceDocId),
    visibility: "public",
    createdAt,
    updatedAt: createdAt,
    counts: {
      likes: 0,
      comments: 0,
      reposts: 0,
      shares: 0,
      reports: 0,
    },
    score: 0,
    lastEngagedAt: createdAt,
  };

  await postRef.set(post, { merge: false });
  await publishAutoPost(post);
}

async function verifyRequestUser(req: Request, required = false): Promise<admin.auth.DecodedIdToken | null> {
  const authHeader = asString(req.headers.authorization);
  if (!authHeader) {
    if (required) throw new Error("unauthenticated");
    return null;
  }
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    if (required) throw new Error("unauthenticated");
    return null;
  }
  const token = match[1];
  try {
    return await auth.verifyIdToken(token);
  } catch {
    throw new Error("invalid_token");
  }
}

function getBearerToken(req: Request): string {
  const authHeader = asString(req.headers["authorization"] || (req.headers as any)["Authorization"]).trim();
  if (!authHeader) return "";
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : "";
}

function normalizeAdFormat(value: unknown): string {
  const normalized = sanitizeText(value, 40)
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "");
  if (!normalized) return "unknown";
  return normalized;
}

function normalizeCurrency(value: unknown): string {
  const normalized = sanitizeText(value, 8).toUpperCase();
  if (/^[A-Z]{3}$/.test(normalized)) return normalized;
  return "USD";
}

function asPlainObject(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function normalizePolymarketSort(value: unknown): "relevance" | "volume" {
  return sanitizeText(value, 24).toLowerCase() === "volume" ? "volume" : "relevance";
}

function parseGammaArray(raw: unknown): unknown[] {
  if (Array.isArray(raw)) return raw;
  if (typeof raw !== "string") return [];
  const text = raw.trim();
  if (!text) return [];
  try {
    const parsed = JSON.parse(text);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function clampProbability(value: unknown): number | null {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (num < 0) return 0;
  if (num > 1) return 1;
  return num;
}

function parseUsdNumber(raw: unknown): number | undefined {
  const num = Number(raw);
  if (!Number.isFinite(num)) return undefined;
  return Math.max(0, num);
}

function parsePolymarketOutcomes(raw: unknown): string[] {
  return parseGammaArray(raw)
    .map((value) => sanitizeText(value, 120))
    .filter(Boolean)
    .slice(0, 16);
}

function parsePolymarketOutcomePrices(raw: unknown): number[] {
  return parseGammaArray(raw)
    .map((value) => clampProbability(value))
    .filter((value): value is number => typeof value === "number")
    .slice(0, 16);
}

function parsePolymarketClobTokenIds(raw: unknown): string[] {
  return parseGammaArray(raw)
    .map((value) => sanitizeText(value, 180))
    .filter(Boolean)
    .slice(0, 16);
}

function polymarketUrlFromSlug(slug: unknown): string {
  const clean = sanitizeText(slug, 240).replace(/^\/+|\/+$/g, "");
  if (!clean) return "";
  return `https://polymarket.com/event/${encodeURIComponent(clean)}`;
}

function normalizePolymarketMarket(raw: unknown, event: Record<string, unknown>): PolymarketMarketRecord | null {
  const market = asPlainObject(raw);
  const id = sanitizeText(market.id || market.marketId || market.conditionId || market.market_id, 120);
  const question = sanitizeText(market.question || market.title, 320);
  if (!id || !question) return null;

  const parsedOutcomes = parsePolymarketOutcomes(market.outcomes);
  const parsedPrices = parsePolymarketOutcomePrices(market.outcomePrices);
  const parsedTokenIds = parsePolymarketClobTokenIds(market.clobTokenIds);
  const alignedLength = Math.min(parsedOutcomes.length, parsedPrices.length);
  const outcomes = alignedLength > 0 ? parsedOutcomes.slice(0, alignedLength) : [];
  const outcomePrices = alignedLength > 0 ? parsedPrices.slice(0, alignedLength) : [];
  const clobTokenIds = alignedLength > 0 ? parsedTokenIds.slice(0, alignedLength) : [];

  const topOutcomes = outcomes
    .map((label, index) => ({ label, prob: outcomePrices[index] }))
    .filter((entry) => typeof entry.label === "string" && Number.isFinite(entry.prob))
    .sort((a, b) => b.prob - a.prob)
    .slice(0, 6);

  let closed = asBoolean(market.closed, false);
  const status = sanitizeText(market.status, 40).toLowerCase();
  if (status === "closed" || status === "resolved" || status === "ended") closed = true;
  const active = asBoolean(market.active, !closed);

  let yesProb: number | undefined = undefined;
  if (outcomes.length === 2) {
    const yesIndex = outcomes.findIndex((label) => /^yes$/i.test(label));
    if (yesIndex >= 0 && Number.isFinite(outcomePrices[yesIndex])) {
      yesProb = outcomePrices[yesIndex];
    }
  }

  return {
    id,
    question,
    slug: sanitizeText(market.slug, 220) || undefined,
    groupItemTitle: sanitizeText(market.groupItemTitle || market.group_item_title || market.targetLabel, 160) || undefined,
    description: sanitizeText(market.description || market.subtitle, 500) || undefined,
    endDate: sanitizeText(market.endDate || market.end_date, 40) || undefined,
    category: sanitizeText(market.category || event.category, 80) || undefined,
    image: sanitizeText(market.image, 600) || undefined,
    icon: sanitizeText(market.icon, 600) || undefined,
    volumeUsd: parseUsdNumber(market.volume),
    liquidityUsd: parseUsdNumber(market.liquidity),
    outcomes,
    outcomePrices,
    clobTokenIds,
    isBinary: outcomes.length === 2,
    yesProb,
    topOutcomes,
    closed,
    active,
  };
}

function normalizePolymarketResponse(
  rawPayload: unknown,
  opts: { query: string; sort: "relevance" | "volume"; includeClosed: boolean }
): PolymarketSearchResponse {
  const root = asPlainObject(rawPayload);
  const rawEvents = Array.isArray(root.events) ? root.events : Array.isArray(rawPayload) ? (rawPayload as unknown[]) : [];
  const events: PolymarketEventRecord[] = [];

  rawEvents.forEach((rawEvent, index) => {
    const event = asPlainObject(rawEvent);
    const eventId = sanitizeText(event.id || event.eventId, 120) || `${opts.query || "event"}_${index + 1}`;
    const eventTitle = sanitizeText(event.title || event.name || event.question, 240) || "Prediction markets";
    const eventSlug = sanitizeText(event.slug, 220) || undefined;
    const eventTicker = sanitizeText(event.ticker, 24).toUpperCase() || undefined;
    const marketRows = Array.isArray(event.markets) ? event.markets : [];
    const markets = marketRows
      .map((rawMarket) => normalizePolymarketMarket(rawMarket, event))
      .filter((market): market is PolymarketMarketRecord => Boolean(market))
      .filter((market) => (opts.includeClosed ? true : !market.closed && market.active !== false));

    if (opts.sort === "volume") {
      markets.sort((a, b) => {
        const volumeDelta = (b.volumeUsd || 0) - (a.volumeUsd || 0);
        if (volumeDelta !== 0) return volumeDelta;
        return (b.liquidityUsd || 0) - (a.liquidityUsd || 0);
      });
    }

    if (!markets.length) return;
    events.push({
      id: eventId,
      title: eventTitle,
      slug: eventSlug,
      ticker: eventTicker,
      markets,
    });
  });

  return {
    query: sanitizeText(opts.query, 120),
    fetchedAt: new Date().toISOString(),
    events,
  };
}

function flattenPolymarketMarkets(
  response: PolymarketSearchResponse,
  filters: { marketId: string; slug: string; eventId: string }
): PolymarketPriceRecord[] {
  const cleanMarketId = sanitizeText(filters.marketId, 120);
  const cleanSlug = sanitizeText(filters.slug, 220).replace(/^\/+|\/+$/g, "");
  const cleanEventId = sanitizeText(filters.eventId, 120);
  const rows: PolymarketPriceRecord[] = [];
  response.events.forEach((event) => {
    const eventId = sanitizeText(event.id, 120);
    const eventTitle = sanitizeText(event.title, 260) || "Prediction markets";
    const eventSlug = sanitizeText(event.slug, 220) || undefined;
    event.markets.forEach((market) => {
      if (cleanEventId && cleanEventId !== eventId) return;
      const marketId = sanitizeText(market.id, 120);
      const marketSlug = sanitizeText(market.slug, 220).replace(/^\/+|\/+$/g, "");
      if (cleanMarketId && marketId !== cleanMarketId) return;
      if (cleanSlug && cleanSlug !== marketSlug && cleanSlug !== (eventSlug || "")) return;
      rows.push({
        eventId,
        eventTitle,
        eventSlug,
        id: marketId,
        question: sanitizeText(market.question, 320),
        slug: marketSlug || undefined,
        groupItemTitle: sanitizeText(market.groupItemTitle, 160) || undefined,
        description: sanitizeText(market.description, 500) || undefined,
        category: sanitizeText(market.category, 80) || undefined,
        endDate: sanitizeText(market.endDate, 40) || undefined,
        volumeUsd: market.volumeUsd,
        liquidityUsd: market.liquidityUsd,
        outcomes: Array.isArray(market.outcomes) ? market.outcomes : [],
        outcomePrices: Array.isArray(market.outcomePrices) ? market.outcomePrices : [],
        clobTokenIds: Array.isArray(market.clobTokenIds) ? market.clobTokenIds : [],
        isBinary: Boolean(market.isBinary),
        yesProb: typeof market.yesProb === "number" ? market.yesProb : undefined,
        topOutcomes: Array.isArray(market.topOutcomes) ? market.topOutcomes : [],
        active: market.active !== false,
        closed: Boolean(market.closed),
      });
    });
  });
  rows.sort((a, b) => {
    const volumeDelta = (b.volumeUsd || 0) - (a.volumeUsd || 0);
    if (volumeDelta !== 0) return volumeDelta;
    const liqDelta = (b.liquidityUsd || 0) - (a.liquidityUsd || 0);
    if (liqDelta !== 0) return liqDelta;
    return a.question.localeCompare(b.question);
  });
  return rows;
}

function prunePolymarketCache(nowMs = Date.now()): void {
  for (const [key, entry] of polymarketCache.entries()) {
    if (entry.expiresAtMs <= nowMs) polymarketCache.delete(key);
  }
  while (polymarketCache.size > POLYMARKET_CACHE_MAX_ENTRIES) {
    const oldestKey = polymarketCache.keys().next().value as string | undefined;
    if (!oldestKey) break;
    polymarketCache.delete(oldestKey);
  }
}

function getPolymarketCache(cacheKey: string): PolymarketSearchResponse | null {
  const entry = polymarketCache.get(cacheKey);
  if (!entry) return null;
  if (entry.expiresAtMs <= Date.now()) {
    polymarketCache.delete(cacheKey);
    return null;
  }
  polymarketCache.delete(cacheKey);
  polymarketCache.set(cacheKey, entry);
  return entry.value;
}

function setPolymarketCache(cacheKey: string, value: PolymarketSearchResponse): void {
  prunePolymarketCache();
  polymarketCache.set(cacheKey, {
    expiresAtMs: Date.now() + POLYMARKET_CACHE_TTL_MS,
    value,
  });
  prunePolymarketCache();
}

async function fetchGammaJson(path: string, params: Record<string, string | number | boolean>): Promise<unknown> {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined) return;
    const serialized = String(value).trim();
    if (!serialized) return;
    query.set(key, serialized);
  });
  const url = `${GAMMA_API_BASE}${path}${query.toString() ? `?${query.toString()}` : ""}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 12000);
  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        "User-Agent": "QuanturaPolymarket/1.0",
      },
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const detail = sanitizeText((payload as any)?.error || (payload as any)?.message || "", 180);
      throw new Error(detail || `Gamma request failed (${response.status})`);
    }
    return payload;
  } finally {
    clearTimeout(timer);
  }
}

function normalizeFxCode(value: unknown, fallback = "USD"): string {
  const normalized = sanitizeText(value, 8)
    .toUpperCase()
    .replace(/[^A-Z]/g, "")
    .slice(0, 6);
  if (normalized) return normalized;
  return sanitizeText(fallback, 8)
    .toUpperCase()
    .replace(/[^A-Z]/g, "")
    .slice(0, 6);
}

function pruneFxRateCache(nowMs = Date.now()): void {
  for (const [key, entry] of fxRateCache.entries()) {
    if (entry.expiresAtMs <= nowMs) fxRateCache.delete(key);
  }
  while (fxRateCache.size > FX_RATE_CACHE_MAX_ENTRIES) {
    const oldestKey = fxRateCache.keys().next().value as string | undefined;
    if (!oldestKey) break;
    fxRateCache.delete(oldestKey);
  }
}

function getFxRateCache(cacheKey: string): FxRateCacheEntry["value"] | null {
  const entry = fxRateCache.get(cacheKey);
  if (!entry) return null;
  if (entry.expiresAtMs <= Date.now()) {
    fxRateCache.delete(cacheKey);
    return null;
  }
  fxRateCache.delete(cacheKey);
  fxRateCache.set(cacheKey, entry);
  return entry.value;
}

function setFxRateCache(cacheKey: string, value: FxRateCacheEntry["value"]): void {
  pruneFxRateCache();
  fxRateCache.set(cacheKey, {
    expiresAtMs: Date.now() + FX_RATE_CACHE_TTL_MS,
    value,
  });
  pruneFxRateCache();
}

async function fetchYahooFxRate(symbol: string): Promise<{ rate: number; asOf: string } | null> {
  const cleanSymbol = sanitizeText(symbol, 24);
  if (!cleanSymbol) return null;
  const endpoint = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(cleanSymbol)}?interval=1d&range=5d`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FX_RATE_FETCH_TIMEOUT_MS);
  try {
    const response = await fetch(endpoint, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        "User-Agent": "QuanturaFx/1.0",
      },
    });
    if (!response.ok) return null;
    const payload = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    const result = Array.isArray((payload.chart as any)?.result) ? ((payload.chart as any).result[0] as Record<string, unknown>) : null;
    if (!result) return null;
    const meta = asPlainObject(result.meta);
    const quoteSeries = Array.isArray((result.indicators as any)?.quote) ? ((result.indicators as any).quote[0] as Record<string, unknown>) : null;
    const closes = Array.isArray(quoteSeries?.close) ? quoteSeries?.close : [];
    let close = Number(meta.regularMarketPrice);
    if (!Number.isFinite(close)) {
      for (let idx = closes.length - 1; idx >= 0; idx -= 1) {
        const candidate = Number(closes[idx]);
        if (Number.isFinite(candidate) && candidate > 0) {
          close = candidate;
          break;
        }
      }
    }
    if (!Number.isFinite(close) || close <= 0) return null;
    let asOfMs = Number(meta.regularMarketTime) * 1000;
    if (!Number.isFinite(asOfMs) || asOfMs <= 0) {
      const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
      const latestTs = Number(timestamps[timestamps.length - 1]);
      asOfMs = Number.isFinite(latestTs) ? latestTs * 1000 : Date.now();
    }
    return {
      rate: close,
      asOf: new Date(asOfMs).toISOString(),
    };
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function resolveFxRate(baseCode: string, quoteCode: string): Promise<{
  rate: number;
  asOf: string;
  symbolUsed: string;
  source: string;
}> {
  const base = normalizeFxCode(baseCode, "USD");
  const quote = normalizeFxCode(quoteCode, "USD");
  if (!base || !quote) {
    throw new Error("invalid_currency_code");
  }
  if (base === quote) {
    return {
      rate: 1,
      asOf: new Date().toISOString(),
      symbolUsed: `${base}${quote}=X`,
      source: "identity",
    };
  }

  const cacheKey = `${base}_${quote}`;
  const cached = getFxRateCache(cacheKey);
  if (cached) {
    return {
      rate: cached.rate,
      asOf: cached.asOf,
      symbolUsed: cached.symbolUsed,
      source: cached.source,
    };
  }

  const directSymbols = [`${base}${quote}=X`, `${base}-${quote}`];
  for (const symbol of directSymbols) {
    const quoteData = await fetchYahooFxRate(symbol);
    if (!quoteData) continue;
    const value = {
      rate: quoteData.rate,
      asOf: quoteData.asOf,
      symbolUsed: symbol,
      source: "yahoo_finance",
    };
    setFxRateCache(cacheKey, value);
    return value;
  }

  const inverseSymbols = [`${quote}${base}=X`, `${quote}-${base}`];
  for (const symbol of inverseSymbols) {
    const quoteData = await fetchYahooFxRate(symbol);
    if (!quoteData || !Number.isFinite(quoteData.rate) || quoteData.rate <= 0) continue;
    const value = {
      rate: 1 / quoteData.rate,
      asOf: quoteData.asOf,
      symbolUsed: symbol,
      source: "yahoo_finance_inverse",
    };
    setFxRateCache(cacheKey, value);
    return value;
  }

  throw new Error("fx_rate_unavailable");
}

function summarizeWebhookPayload(req: Request): Record<string, unknown> {
  const body = asPlainObject(req.body);
  const query = asPlainObject(req.query);
  return {
    path: req.path,
    method: req.method,
    ip: requestIpAddress(req),
    userAgent: sanitizeText(req.headers["user-agent"], 300),
    query,
    body,
    receivedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

function normalizeLlmMessages(raw: unknown): Array<{ role: "system" | "user" | "assistant"; content: string }> {
  const rows = Array.isArray(raw) ? raw : [];
  return rows
    .map((entry) => {
      const item = (entry || {}) as Record<string, unknown>;
      const roleRaw = asString(item.role).trim().toLowerCase();
      const role: "system" | "user" | "assistant" =
        roleRaw === "system" || roleRaw === "assistant" || roleRaw === "user" ? (roleRaw as any) : "user";
      const content = sanitizeText(item.content, 12000);
      return { role, content };
    })
    .filter((item) => item.content.length > 0)
    .slice(0, 40);
}

type LlmProviderId =
  | "openai"
  | "claude"
  | "gemini"
  | "deepseek"
  | "mistral"
  | "perplexity"
  | "qwen"
  | "amazon_nova"
  | "other";
type LlmUserTier = "free" | "premium";

type LlmProviderPolicy = {
  freeModels: string[];
  premiumModels: string[];
};

type ModelCouncilProviderConfig = {
  id: LlmProviderId;
  label: string;
  envName: string;
  supportsModelList: boolean;
  isConfigured: () => boolean;
};

type ModelCouncilProviderView = {
  id: LlmProviderId;
  label: string;
  displayName: string;
  available: boolean;
  supportsModelList: boolean;
};

type ModelCouncilModelView = {
  id: string;
  label: string;
  provider: LlmProviderId;
  group: string;
  hint: string;
};

const LLM_PROVIDER_POLICY: Record<LlmProviderId, LlmProviderPolicy> = {
  openai: {
    freeModels: ["gpt-5-nano", "gpt-5-mini", "gpt-4o-mini"],
    premiumModels: ["gpt-5", "gpt-5.1", "gpt-5.2", "gpt-5.4", "gpt-5.4-pro", "gpt-4.1"],
  },
  claude: {
    freeModels: ["claude-haiku*", "claude-3-haiku*"],
    premiumModels: ["claude-sonnet*", "claude-opus*"],
  },
  gemini: {
    freeModels: ["gemini-2.5-flash-lite", "gemini-2.5-flash", "gemini-2.0-flash"],
    premiumModels: ["gemini-2.5-pro", "gemini-1.5-pro"],
  },
  deepseek: {
    freeModels: ["deepseek-chat"],
    premiumModels: ["deepseek-reasoner"],
  },
  mistral: {
    freeModels: ["mistral-small*", "mistral-small-latest"],
    premiumModels: ["mistral-medium*", "mistral-large*", "mistral-large-latest"],
  },
  perplexity: {
    freeModels: ["sonar"],
    premiumModels: ["sonar-pro", "sonar-reasoning-pro", "sonar-deep-research"],
  },
  qwen: {
    freeModels: ["qwen-flash"],
    premiumModels: ["qwen-plus", "qwen-max"],
  },
  amazon_nova: {
    freeModels: ["amazon.nova-micro-v1:0", "amazon.nova-lite-v1:0"],
    premiumModels: ["amazon.nova-pro-v1:0", "amazon.nova-premier-v1:0"],
  },
  other: {
    freeModels: [DEFAULT_LLM_MODEL],
    premiumModels: ["*"],
  },
};

const MODEL_COUNCIL_PROVIDER_REGISTRY: ModelCouncilProviderConfig[] = [
  {
    id: "openai",
    label: "ChatGPT",
    envName: "OPENAI_API_KEY",
    supportsModelList: true,
    isConfigured: () => Boolean(OPENAI_API_KEY),
  },
  {
    id: "claude",
    label: "Claude",
    envName: "CLAUDE_API_KEY",
    supportsModelList: true,
    isConfigured: () => Boolean(CLAUDE_API_KEY),
  },
  {
    id: "gemini",
    label: "Gemini",
    envName: "GEMINI_API_KEY",
    supportsModelList: true,
    isConfigured: () => Boolean(GEMINI_API_KEY),
  },
  {
    id: "deepseek",
    label: "DeepSeek",
    envName: "DEEPSEEK_API_KEY",
    supportsModelList: true,
    isConfigured: () => Boolean(DEEPSEEK_API_KEY),
  },
  {
    id: "mistral",
    label: "Mistral",
    envName: "MISTRAL_API_KEY",
    supportsModelList: true,
    isConfigured: () => Boolean(MISTRAL_API_KEY),
  },
  {
    id: "perplexity",
    label: "Perplexity Sonar",
    envName: "PERPLEXITY_API_KEY",
    supportsModelList: true,
    isConfigured: () => Boolean(PERPLEXITY_API_KEY),
  },
  {
    id: "qwen",
    label: "Qwen",
    envName: "QWEN_API_KEY",
    supportsModelList: true,
    isConfigured: () => Boolean(QWEN_API_KEY),
  },
  {
    id: "amazon_nova",
    label: "Amazon Nova",
    envName: "AMAZON_NOVA_API_KEY + AMAZON_NOVA_BASE_URL",
    supportsModelList: true,
    isConfigured: () => Boolean(AMAZON_NOVA_API_KEY && AMAZON_NOVA_BASE_URL),
  },
  {
    id: "other",
    label: "Other",
    envName: "MODEL_COUNCIL_OTHER_API_KEY + MODEL_COUNCIL_OTHER_BASE_URL",
    supportsModelList: true,
    isConfigured: () => Boolean(MODEL_COUNCIL_OTHER_API_KEY && MODEL_COUNCIL_OTHER_BASE_URL),
  },
];

async function getOpenAiApiKey(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:openai:key",
    ["OPENAI_API_KEY", "OPENAI_SECRET_KEY", "OPENAI_KEY"],
    ["OPENAI_API_KEY", "OPENAI_SECRET_KEY", "OPENAI_KEY"],
    [/^openai.*key$/i]
  );
}

async function getClaudeApiKey(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:claude:key",
    ["CLAUDE_API_KEY", "ANTHROPIC_API_KEY"],
    ["CLAUDE_API_KEY", "ANTHROPIC_API_KEY"],
    [/^(claude|anthropic).*key$/i]
  );
}

async function getGeminiApiKey(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:gemini:key",
    ["GEMINI_API_KEY", "GOOGLE_GENAI_API_KEY"],
    ["GEMINI_API_KEY", "GOOGLE_GENAI_API_KEY"],
    [/^(gemini|google[-_]?genai).*key$/i]
  );
}

async function getDeepseekApiKey(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:deepseek:key",
    ["DEEPSEEK_API_KEY", "DEEPSEEK_SECRET_KEY"],
    ["DEEPSEEK_API_KEY", "DEEPSEEK_SECRET_KEY"],
    [/^deepseek.*key$/i]
  );
}

async function getMistralApiKey(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:mistral:key",
    ["MISTRAL_API_KEY", "MISTRAL_SECRET_KEY"],
    ["MISTRAL_API_KEY", "MISTRAL_SECRET_KEY"],
    [/^mistral.*key$/i]
  );
}

async function getPerplexityApiKey(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:perplexity:key",
    ["PERPLEXITY_API_KEY", "PERPLEXITY_SECRET_KEY"],
    ["PERPLEXITY_API_KEY", "PERPLEXITY_SECRET_KEY"],
    [/^perplexity.*key$/i]
  );
}

async function getQwenApiKey(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:qwen:key",
    ["QWEN_API_KEY", "QWEN_SECRET_KEY"],
    ["QWEN_API_KEY", "QWEN_SECRET_KEY"],
    [/^qwen.*key$/i]
  );
}

async function getAmazonNovaApiKey(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:amazon_nova:key",
    ["AMAZON_NOVA_API_KEY", "BEDROCK_API_KEY"],
    ["AMAZON_NOVA_API_KEY", "BEDROCK_API_KEY", "AMAZON_NOVA_KEY"],
    [/^(amazon[-_]?nova|bedrock).*key$/i]
  );
}

async function getAmazonNovaBaseUrl(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:amazon_nova:base_url",
    ["AMAZON_NOVA_BASE_URL", "BEDROCK_BASE_URL"],
    ["AMAZON_NOVA_BASE_URL", "BEDROCK_BASE_URL", "AMAZON_NOVA_ENDPOINT"],
    [/^(amazon[-_]?nova|bedrock).*(base|url|endpoint)/i]
  );
}

async function getOtherProviderApiKey(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:other:key",
    ["MODEL_COUNCIL_OTHER_API_KEY", "MODEL_COUNCIL_OTHER_KEY"],
    ["MODEL_COUNCIL_OTHER_API_KEY", "MODEL_COUNCIL_OTHER_KEY"],
    [/^model[-_]?council[-_]?other.*key$/i]
  );
}

async function getOtherProviderBaseUrl(): Promise<string> {
  return resolveRuntimeSecretValue(
    "llm:other:base_url",
    ["MODEL_COUNCIL_OTHER_BASE_URL"],
    ["MODEL_COUNCIL_OTHER_BASE_URL", "MODEL_COUNCIL_OTHER_URL", "MODEL_COUNCIL_OTHER_ENDPOINT"],
    [/^model[-_]?council[-_]?other.*(base|url|endpoint)/i]
  );
}

async function isModelCouncilProviderConfigured(provider: LlmProviderId): Promise<boolean> {
  if (provider === "openai") return Boolean(await getOpenAiApiKey());
  if (provider === "claude") return Boolean(await getClaudeApiKey());
  if (provider === "gemini") return Boolean(await getGeminiApiKey());
  if (provider === "deepseek") return Boolean(await getDeepseekApiKey());
  if (provider === "mistral") return Boolean(await getMistralApiKey());
  if (provider === "perplexity") return Boolean(await getPerplexityApiKey());
  if (provider === "qwen") return Boolean(await getQwenApiKey());
  if (provider === "amazon_nova") return Boolean((await getAmazonNovaApiKey()) && (await getAmazonNovaBaseUrl()));
  if (provider === "other") return Boolean((await getOtherProviderApiKey()) && (await getOtherProviderBaseUrl()));
  return false;
}

const MODEL_COUNCIL_MODELS: Record<LlmProviderId, ModelCouncilModelView[]> = {
  openai: [
    { id: "gpt-5-nano", label: "GPT-5 Nano", provider: "openai", group: "Fast", hint: "Lowest latency for quick triage." },
    { id: "gpt-5-mini", label: "GPT-5 Mini", provider: "openai", group: "Balanced", hint: "Best default for most prompts." },
    { id: "gpt-5", label: "GPT-5", provider: "openai", group: "Reasoning", hint: "High-depth reasoning and synthesis." },
    { id: "gpt-5.4", label: "GPT-5.4", provider: "openai", group: "Research", hint: "Premium research-grade analysis." },
  ],
  claude: [
    { id: "claude-haiku-4-5", label: "Claude Haiku 4.5", provider: "claude", group: "Fast", hint: "Lower-latency Claude path." },
    { id: "claude-sonnet-4-5", label: "Claude Sonnet 4.5", provider: "claude", group: "Balanced", hint: "Strong synthesis for council runs." },
    { id: "claude-opus-4-5", label: "Claude Opus 4.5", provider: "claude", group: "Research", hint: "Premium long-form research model." },
  ],
  gemini: [
    { id: "gemini-2.5-flash-lite", label: "Gemini 2.5 Flash-Lite", provider: "gemini", group: "Fast", hint: "Lowest-cost Gemini path." },
    { id: "gemini-2.5-flash", label: "Gemini 2.5 Flash", provider: "gemini", group: "Balanced", hint: "Fast and balanced Gemini path." },
    { id: "gemini-2.5-pro", label: "Gemini 2.5 Pro", provider: "gemini", group: "Research", hint: "Higher-depth Gemini analysis." },
  ],
  deepseek: [
    { id: "deepseek-chat", label: "DeepSeek Chat", provider: "deepseek", group: "Balanced", hint: "Cost-efficient baseline analysis." },
    { id: "deepseek-reasoner", label: "DeepSeek Reasoner", provider: "deepseek", group: "Reasoning", hint: "Higher-depth reasoning path." },
  ],
  mistral: [
    { id: "mistral-small-latest", label: "Mistral Small", provider: "mistral", group: "Fast", hint: "Low-latency Mistral route." },
    { id: "mistral-large-latest", label: "Mistral Large", provider: "mistral", group: "Reasoning", hint: "Higher-depth Mistral reasoning." },
  ],
  perplexity: [
    { id: "sonar", label: "Sonar", provider: "perplexity", group: "Research", hint: "Web-grounded baseline research path." },
    { id: "sonar-pro", label: "Sonar Pro", provider: "perplexity", group: "Research", hint: "Higher-depth fresh web research." },
    { id: "sonar-deep-research", label: "Sonar Deep Research", provider: "perplexity", group: "Research", hint: "Deep current-events analysis." },
  ],
  qwen: [
    { id: "qwen-flash", label: "Qwen Flash", provider: "qwen", group: "Fast", hint: "Low-latency Qwen path." },
    { id: "qwen-plus", label: "Qwen Plus", provider: "qwen", group: "Balanced", hint: "Balanced Qwen reasoning path." },
    { id: "qwen-max", label: "Qwen Max", provider: "qwen", group: "Reasoning", hint: "Premium Qwen reasoning path." },
  ],
  amazon_nova: [
    { id: "amazon.nova-lite-v1:0", label: "Nova Lite", provider: "amazon_nova", group: "Balanced", hint: "Amazon Nova lightweight route." },
    { id: "amazon.nova-pro-v1:0", label: "Nova Pro", provider: "amazon_nova", group: "Reasoning", hint: "Amazon Nova high-depth route." },
  ],
  other: [
    { id: DEFAULT_LLM_MODEL, label: DEFAULT_LLM_MODEL, provider: "other", group: "Custom", hint: "Custom provider model from backend config." },
  ],
};

async function listModelCouncilProviders(opts: { includeUnavailable?: boolean } = {}): Promise<ModelCouncilProviderView[]> {
  const includeUnavailable = Boolean(opts.includeUnavailable);
  const rows = await Promise.all(
    MODEL_COUNCIL_PROVIDER_REGISTRY.map(async (provider) => ({
      id: provider.id,
      label: provider.label,
      displayName: provider.label,
      available: await isModelCouncilProviderConfigured(provider.id),
      supportsModelList: provider.supportsModelList,
    }))
  );
  if (includeUnavailable) return rows;
  return rows.filter((row) => row.available);
}

function listModelCouncilModels(provider: LlmProviderId): ModelCouncilModelView[] {
  const seeded = Array.isArray(MODEL_COUNCIL_MODELS[provider]) ? MODEL_COUNCIL_MODELS[provider] : [];
  if (seeded.length) return seeded;
  const policy = LLM_PROVIDER_POLICY[provider] || LLM_PROVIDER_POLICY.openai;
  const ids = [...policy.freeModels, ...policy.premiumModels]
    .map((id) => sanitizeText(id, 120))
    .filter((id) => id && !id.includes("*"));
  return Array.from(new Set(ids)).map((id) => ({
    id,
    label: id,
    provider,
    group: "Balanced",
    hint: "Server-side model routing path.",
  }));
}

function normalizeProvider(raw: unknown): LlmProviderId {
  const value = asString(raw).trim().toLowerCase().replace(/[^a-z0-9_-]/g, "");
  if (value === "openai") return "openai";
  if (value === "claude" || value === "anthropic") return "claude";
  if (value === "gemini") return "gemini";
  if (value === "deepseek") return "deepseek";
  if (value === "mistral") return "mistral";
  if (value === "perplexity") return "perplexity";
  if (value === "qwen") return "qwen";
  if (value === "amazon_nova" || value === "amazon-nova" || value === "nova") return "amazon_nova";
  if (value === "other") return "other";
  return "openai";
}

function normalizeLlmTier(raw: unknown): LlmUserTier {
  const value = asString(raw).trim().toLowerCase();
  return value === "premium" || value === "pro" || value === "business" ? "premium" : "free";
}

function modelMatchesPattern(modelId: string, pattern: string): boolean {
  if (!pattern || pattern === "*") return true;
  const cleanPattern = pattern.trim().toLowerCase();
  const cleanModel = modelId.trim().toLowerCase();
  if (!cleanPattern) return false;
  if (cleanPattern.endsWith("*")) {
    return cleanModel.startsWith(cleanPattern.slice(0, -1));
  }
  return cleanModel === cleanPattern;
}

function pickModelForProvider(
  provider: LlmProviderId,
  requestedModel: string,
  tier: LlmUserTier
): { model: string; adjustedFrom: string } {
  const policy = LLM_PROVIDER_POLICY[provider] || LLM_PROVIDER_POLICY.openai;
  const free = Array.isArray(policy.freeModels) ? policy.freeModels : [];
  const premium = Array.isArray(policy.premiumModels) ? policy.premiumModels : [];
  const allowed = tier === "premium" ? [...free, ...premium] : [...free];
  const requested = sanitizeText(requestedModel, 120);
  const defaultModel = allowed[0] || DEFAULT_LLM_MODEL;
  if (!requested) return { model: defaultModel, adjustedFrom: "" };
  const isAllowed = allowed.some((pattern) => modelMatchesPattern(requested, pattern));
  if (isAllowed) return { model: requested, adjustedFrom: "" };
  return { model: defaultModel, adjustedFrom: requested };
}

function parseWebhookSecret(req: Request): string {
  return sanitizeText(req.headers["x-quantura-webhook-secret"] || req.query.secret, 500);
}

function checkWebhookSecret(req: Request, expected: string): boolean {
  if (!expected) return true;
  const provided = parseWebhookSecret(req);
  return provided.length > 0 && provided === expected;
}

function llmTimeoutSignal(timeoutMs = LLM_TIMEOUT_MS): { signal: AbortSignal; clear: () => void } {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return {
    signal: controller.signal,
    clear: () => clearTimeout(timer),
  };
}

function isOpenAiReasoningModel(model: string): boolean {
  const normalized = sanitizeText(model, 120).toLowerCase();
  return normalized.startsWith("gpt-5");
}

function getOpenAiReasoningEffort(model: string, allowWebSearch = false): "minimal" | "low" {
  if (allowWebSearch) return "low";
  const normalized = sanitizeText(model, 120).toLowerCase();
  if (normalized === "gpt-5" || normalized.startsWith("gpt-5.4")) return "low";
  return "minimal";
}

function getOpenAiMaxOutputTokens(model: string, requestedMaxTokens: number): number {
  const safeMaxTokens = Math.max(64, Math.floor(asFinite(requestedMaxTokens, 600)));
  if (!isOpenAiReasoningModel(model)) return safeMaxTokens;
  return Math.max(safeMaxTokens, 300);
}

async function invokeOpenAiLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
  allowWebSearch: boolean;
  stream: boolean;
  background: boolean;
  tools?: unknown[];
  jsonSchema?: unknown;
}): Promise<{ text: string; usage: Record<string, unknown>; responseId: string; status: string }> {
  const openAiApiKey = await getOpenAiApiKey();
  if (!openAiApiKey) throw new Error("OPENAI_API_KEY is not configured.");
  const requestedTools = Array.isArray(payload.tools) ? payload.tools : [];
  const tools: Array<Record<string, unknown>> = [];
  const pushWebSearch = () => {
    if (!tools.some((tool) => sanitizeText(tool.type, 80) === "web_search_preview")) {
      tools.push({ type: "web_search_preview" });
    }
  };
  if (payload.allowWebSearch) pushWebSearch();
  requestedTools.forEach((tool) => {
    const row = tool && typeof tool === "object" ? (tool as Record<string, unknown>) : {};
    const type = sanitizeText(row.type, 80).toLowerCase();
    if (!type) return;
    if (type === "web_search" || type === "web_search_preview") {
      pushWebSearch();
      return;
    }
    if (type === "file_search") {
      tools.push({ type: "file_search" });
    }
  });

  const schemaSource = payload.jsonSchema && typeof payload.jsonSchema === "object" ? (payload.jsonSchema as Record<string, unknown>) : null;
  const schemaNameRaw = schemaSource ? sanitizeText(schemaSource.name || "quantura_structured_output", 120) : "";
  const schemaObject =
    schemaSource && schemaSource.schema && typeof schemaSource.schema === "object"
      ? (schemaSource.schema as Record<string, unknown>)
      : schemaSource;
  const maxOutputTokens = getOpenAiMaxOutputTokens(payload.model, payload.maxTokens);
  const reasoning = isOpenAiReasoningModel(payload.model)
    ? {
        effort: getOpenAiReasoningEffort(payload.model, payload.allowWebSearch),
      }
    : undefined;
  const textFormat =
    schemaObject && typeof schemaObject === "object"
      ? {
          format: {
            type: "json_schema",
            name: schemaNameRaw || "quantura_structured_output",
            schema: schemaObject,
            strict: true,
          },
        }
      : undefined;

  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${openAiApiKey}`,
      },
      body: JSON.stringify({
        model: payload.model,
        input: payload.messages.map((item) => ({
          role: item.role,
          content: [{ type: "input_text", text: item.content }],
        })),
        max_output_tokens: maxOutputTokens,
        stream: Boolean(payload.stream),
        background: Boolean(payload.background),
        reasoning,
        tools,
        text: textFormat,
        metadata: {
          quantura_workflow: "model_council",
          quantura_prompt_caching: "enabled",
        },
      }),
    });
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) {
      const detail = sanitizeText((body as any)?.error?.message || "", 180);
      throw new Error(detail || `OpenAI request failed (${response.status}).`);
    }
    const text = sanitizeText(extractResponsesOutputText(body), 20000);
    const responseId = sanitizeText((body as any)?.id, 120);
    const status = sanitizeText((body as any)?.status, 80);
    if (!text) {
      const statusLabel = status || "unknown";
      throw new Error(`OpenAI returned an empty response (status: ${statusLabel}).`);
    }
    return {
      text,
      usage: extractResponsesUsage(body),
      responseId,
      status,
    };
  } finally {
    clear();
  }
}

function extractResponsesOutputText(payload: Record<string, unknown>): string {
  const rawDirect = (payload as any)?.output_text;
  const directParts: string[] = [];
  if (typeof rawDirect === "string" || typeof rawDirect === "number" || typeof rawDirect === "boolean") {
    const text = sanitizeText(rawDirect, 24000);
    if (text) directParts.push(text);
  } else if (Array.isArray(rawDirect)) {
    rawDirect.forEach((part) => {
      const text = sanitizeText(
        (part as any)?.text ?? (part as any)?.value ?? (part as any)?.output_text ?? part,
        24000
      );
      if (text) directParts.push(text);
    });
  } else if (rawDirect && typeof rawDirect === "object") {
    const text = sanitizeText((rawDirect as any).text ?? (rawDirect as any).value ?? "", 24000);
    if (text) directParts.push(text);
  }
  const direct = sanitizeText(directParts.join("\n").trim(), 24000);
  if (direct) return direct;
  const output = Array.isArray((payload as any)?.output) ? ((payload as any).output as any[]) : [];
  const chunks: string[] = [];
  output.forEach((item) => {
    const content = Array.isArray(item?.content) ? item.content : [];
    content.forEach((part: any) => {
      const text = sanitizeText(
        part?.text?.value ??
          part?.text ??
          part?.output_text?.value ??
          part?.output_text ??
          part?.value?.text ??
          part?.value,
        24000
      );
      if (text) chunks.push(text);
    });
  });
  return sanitizeText(chunks.join("\n").trim(), 24000);
}

function extractResponsesUsage(payload: Record<string, unknown>): Record<string, unknown> {
  const usage = ((payload as any)?.usage || {}) as Record<string, unknown>;
  const inputTokens = asFinite((usage as any).input_tokens, 0);
  const outputTokens = asFinite((usage as any).output_tokens, 0);
  const totalTokens = asFinite((usage as any).total_tokens, inputTokens + outputTokens);
  const cachedTokens = asFinite(((usage as any).input_tokens_details || {}).cached_tokens, 0);
  return {
    prompt_tokens: Math.max(0, Math.floor(inputTokens)),
    completion_tokens: Math.max(0, Math.floor(outputTokens)),
    total_tokens: Math.max(0, Math.floor(totalTokens)),
    cached_tokens: Math.max(0, Math.floor(cachedTokens)),
  };
}

function extractResponsesCitations(payload: Record<string, unknown>): Array<Record<string, unknown>> {
  const output = Array.isArray((payload as any)?.output) ? ((payload as any).output as any[]) : [];
  const seen = new Set<string>();
  const citations: Array<Record<string, unknown>> = [];
  output.forEach((item) => {
    const content = Array.isArray(item?.content) ? item.content : [];
    content.forEach((part: any) => {
      const annotations = Array.isArray(part?.annotations) ? part.annotations : [];
      annotations.forEach((annotation: any) => {
        const url = sanitizeText(annotation?.url || annotation?.source || "", 500);
        const title = sanitizeText(annotation?.title || annotation?.text || url, 300);
        if (!url || seen.has(url)) return;
        seen.add(url);
        citations.push({
          url,
          title: title || url,
        });
      });
    });
  });
  return citations.slice(0, 16);
}

async function invokeGeminiLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
}): Promise<{ text: string; usage: Record<string, unknown> }> {
  const geminiApiKey = await getGeminiApiKey();
  if (!geminiApiKey) throw new Error("GEMINI_API_KEY is not configured.");
  const prompt = payload.messages.map((item) => `${item.role.toUpperCase()}: ${item.content}`).join("\n\n");
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(payload.model)}:generateContent?key=${encodeURIComponent(
        geminiApiKey
      )}`,
      {
        method: "POST",
        signal,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ role: "user", parts: [{ text: prompt }] }],
          generationConfig: {
            temperature: payload.temperature,
            maxOutputTokens: payload.maxTokens,
          },
        }),
      }
    );
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) throw new Error(`Gemini request failed (${response.status}).`);
    const parts = ((((body.candidates as any)?.[0] || {}).content || {}).parts || []) as Array<Record<string, unknown>>;
    const text = sanitizeText(parts.map((part) => asString(part.text)).join("\n"), 20000);
    if (!text) throw new Error("Gemini returned an empty response.");
    return { text, usage: ((body.usageMetadata as any) || {}) as Record<string, unknown> };
  } finally {
    clear();
  }
}

async function invokeMistralLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
}): Promise<{ text: string; usage: Record<string, unknown> }> {
  const mistralApiKey = await getMistralApiKey();
  if (!mistralApiKey) throw new Error("MISTRAL_API_KEY is not configured.");
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch("https://api.mistral.ai/v1/chat/completions", {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${mistralApiKey}`,
      },
      body: JSON.stringify({
        model: payload.model,
        messages: payload.messages,
        temperature: payload.temperature,
        max_tokens: payload.maxTokens,
      }),
    });
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) throw new Error(`Mistral request failed (${response.status}).`);
    const text = sanitizeText((((body.choices as any)?.[0] || {}).message || {}).content, 20000);
    if (!text) throw new Error("Mistral returned an empty response.");
    return { text, usage: ((body.usage as any) || {}) as Record<string, unknown> };
  } finally {
    clear();
  }
}

async function invokePerplexityLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
}): Promise<{ text: string; usage: Record<string, unknown>; citations: unknown[] }> {
  const perplexityApiKey = await getPerplexityApiKey();
  if (!perplexityApiKey) throw new Error("PERPLEXITY_API_KEY is not configured.");
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch("https://api.perplexity.ai/chat/completions", {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${perplexityApiKey}`,
      },
      body: JSON.stringify({
        model: payload.model,
        messages: payload.messages,
        temperature: payload.temperature,
        max_tokens: payload.maxTokens,
      }),
    });
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) throw new Error(`Perplexity request failed (${response.status}).`);
    const text = sanitizeText((((body.choices as any)?.[0] || {}).message || {}).content, 20000);
    if (!text) throw new Error("Perplexity returned an empty response.");
    return {
      text,
      usage: ((body.usage as any) || {}) as Record<string, unknown>,
      citations: Array.isArray(body.citations) ? body.citations : [],
    };
  } finally {
    clear();
  }
}

async function invokeClaudeLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
}): Promise<{ text: string; usage: Record<string, unknown> }> {
  const claudeApiKey = await getClaudeApiKey();
  if (!claudeApiKey) throw new Error("CLAUDE_API_KEY is not configured.");
  const system = payload.messages
    .filter((item) => item.role === "system")
    .map((item) => item.content)
    .join("\n\n")
    .trim();
  const messages = payload.messages
    .filter((item) => item.role !== "system")
    .map((item) => ({
      role: item.role === "assistant" ? "assistant" : "user",
      content: item.content,
    }));
  if (!messages.length) messages.push({ role: "user", content: "Summarize the provided context clearly." });
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
        "x-api-key": claudeApiKey,
        "anthropic-version": CLAUDE_API_VERSION,
      },
      body: JSON.stringify({
        model: payload.model,
        max_tokens: payload.maxTokens,
        temperature: payload.temperature,
        system,
        messages,
      }),
    });
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) {
      const detail = sanitizeText((body as any)?.error?.message || "", 180);
      throw new Error(detail || `Claude request failed (${response.status}).`);
    }
    const content = Array.isArray((body as any)?.content) ? ((body as any).content as Array<Record<string, unknown>>) : [];
    const text = sanitizeText(
      content
        .map((part) => sanitizeText(part?.text, 8000))
        .filter(Boolean)
        .join("\n"),
      20000
    );
    if (!text) throw new Error("Claude returned an empty response.");
    return { text, usage: ((body.usage as any) || {}) as Record<string, unknown> };
  } finally {
    clear();
  }
}

async function invokeOpenAiCompatibleChatLlm(payload: {
  apiKey: string;
  baseUrl: string;
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
  providerLabel: string;
}): Promise<{ text: string; usage: Record<string, unknown> }> {
  const baseUrl = payload.baseUrl.replace(/\/$/, "");
  if (!baseUrl) throw new Error(`${payload.providerLabel} base URL is not configured.`);
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch(`${baseUrl}/chat/completions`, {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${payload.apiKey}`,
      },
      body: JSON.stringify({
        model: payload.model,
        messages: payload.messages,
        temperature: payload.temperature,
        max_tokens: payload.maxTokens,
      }),
    });
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) {
      const detail = sanitizeText((body as any)?.error?.message || "", 180);
      throw new Error(detail || `${payload.providerLabel} request failed (${response.status}).`);
    }
    const text = sanitizeText((((body.choices as any)?.[0] || {}).message || {}).content, 20000);
    if (!text) throw new Error(`${payload.providerLabel} returned an empty response.`);
    return {
      text,
      usage: ((body.usage as any) || {}) as Record<string, unknown>,
    };
  } finally {
    clear();
  }
}

async function invokeDeepseekLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
}): Promise<{ text: string; usage: Record<string, unknown> }> {
  const deepseekApiKey = await getDeepseekApiKey();
  if (!deepseekApiKey) throw new Error("DEEPSEEK_API_KEY is not configured.");
  return invokeOpenAiCompatibleChatLlm({
    apiKey: deepseekApiKey,
    baseUrl: "https://api.deepseek.com/v1",
    providerLabel: "DeepSeek",
    ...payload,
  });
}

async function invokeQwenLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
}): Promise<{ text: string; usage: Record<string, unknown> }> {
  const qwenApiKey = await getQwenApiKey();
  if (!qwenApiKey) throw new Error("QWEN_API_KEY is not configured.");
  return invokeOpenAiCompatibleChatLlm({
    apiKey: qwenApiKey,
    baseUrl: "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
    providerLabel: "Qwen",
    ...payload,
  });
}

async function invokeAmazonNovaLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
}): Promise<{ text: string; usage: Record<string, unknown> }> {
  const amazonNovaApiKey = await getAmazonNovaApiKey();
  const amazonNovaBaseUrl = await getAmazonNovaBaseUrl();
  if (!amazonNovaApiKey) throw new Error("AMAZON_NOVA_API_KEY is not configured.");
  if (!amazonNovaBaseUrl) throw new Error("AMAZON_NOVA_BASE_URL is not configured.");
  return invokeOpenAiCompatibleChatLlm({
    apiKey: amazonNovaApiKey,
    baseUrl: amazonNovaBaseUrl,
    providerLabel: "Amazon Nova",
    ...payload,
  });
}

async function streamOpenAiLlmSse(
  req: Request,
  res: Response,
  payload: {
    model: string;
    messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
    maxTokens: number;
    allowWebSearch: boolean;
    background: boolean;
    tools?: unknown[];
    jsonSchema?: unknown;
  }
): Promise<void> {
  const openAiApiKey = await getOpenAiApiKey();
  if (!openAiApiKey) throw new Error("OPENAI_API_KEY is not configured.");
  const requestedTools = Array.isArray(payload.tools) ? payload.tools : [];
  const tools: Array<Record<string, unknown>> = [];
  const pushWebSearch = () => {
    if (!tools.some((tool) => sanitizeText(tool.type, 80) === "web_search_preview")) {
      tools.push({ type: "web_search_preview" });
    }
  };
  if (payload.allowWebSearch) pushWebSearch();
  requestedTools.forEach((tool) => {
    const row = tool && typeof tool === "object" ? (tool as Record<string, unknown>) : {};
    const type = sanitizeText(row.type, 80).toLowerCase();
    if (type === "web_search" || type === "web_search_preview") pushWebSearch();
    if (type === "file_search") tools.push({ type: "file_search" });
  });

  const schemaSource = payload.jsonSchema && typeof payload.jsonSchema === "object" ? (payload.jsonSchema as Record<string, unknown>) : null;
  const schemaNameRaw = schemaSource ? sanitizeText(schemaSource.name || "quantura_structured_output", 120) : "";
  const schemaObject =
    schemaSource && schemaSource.schema && typeof schemaSource.schema === "object"
      ? (schemaSource.schema as Record<string, unknown>)
      : schemaSource;
  const maxOutputTokens = getOpenAiMaxOutputTokens(payload.model, payload.maxTokens);
  const reasoning = isOpenAiReasoningModel(payload.model)
    ? {
        effort: getOpenAiReasoningEffort(payload.model, payload.allowWebSearch),
      }
    : undefined;
  const textFormat =
    schemaObject && typeof schemaObject === "object"
      ? {
          format: {
            type: "json_schema",
            name: schemaNameRaw || "quantura_structured_output",
            schema: schemaObject,
            strict: true,
          },
        }
      : undefined;

  const abort = new AbortController();
  const closeListener = () => abort.abort();
  req.on("close", closeListener);

  try {
    const upstream = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      signal: abort.signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${openAiApiKey}`,
      },
      body: JSON.stringify({
        model: payload.model,
        input: payload.messages.map((item) => ({
          role: item.role,
          content: [{ type: "input_text", text: item.content }],
        })),
        max_output_tokens: maxOutputTokens,
        stream: true,
        background: Boolean(payload.background),
        reasoning,
        tools,
        text: textFormat,
        metadata: {
          quantura_workflow: "model_council",
          quantura_stream: "sse",
        },
      }),
    });

    if (!upstream.ok) {
      const bodyText = sanitizeText(await upstream.text().catch(() => ""), 800);
      throw new Error(bodyText || `OpenAI stream request failed (${upstream.status}).`);
    }

    res.status(200);
    res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    if (typeof (res as any).flushHeaders === "function") {
      (res as any).flushHeaders();
    }

    if (!upstream.body) {
      res.write(`event: error\ndata: ${JSON.stringify({ error: "empty_stream_body" })}\n\n`);
      res.end();
      return;
    }

    const reader = upstream.body.getReader();
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      if (chunk.value && chunk.value.length) {
        res.write(Buffer.from(chunk.value));
      }
    }
    res.end();
  } finally {
    req.off("close", closeListener);
  }
}

async function invokeOtherLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
  allowWebSearch: boolean;
  stream: boolean;
  background: boolean;
}): Promise<{ text: string; usage: Record<string, unknown>; citations: unknown[] }> {
  const otherProviderApiKey = await getOtherProviderApiKey();
  const otherProviderBaseUrl = await getOtherProviderBaseUrl();
  if (!otherProviderApiKey) throw new Error("MODEL_COUNCIL_OTHER_API_KEY is not configured.");
  if (!otherProviderBaseUrl) throw new Error("MODEL_COUNCIL_OTHER_BASE_URL is not configured.");
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch(`${otherProviderBaseUrl}/v1/responses`, {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${otherProviderApiKey}`,
      },
      body: JSON.stringify({
        model: payload.model,
        input: payload.messages.map((item) => ({
          role: item.role,
          content: [{ type: "input_text", text: item.content }],
        })),
        max_output_tokens: payload.maxTokens,
        stream: Boolean(payload.stream),
        background: Boolean(payload.background),
        tools: payload.allowWebSearch ? [{ type: "web_search_preview" }] : [],
      }),
    });
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) {
      const detail = sanitizeText((body as any)?.error?.message || "", 180);
      throw new Error(detail || `Other provider request failed (${response.status}).`);
    }
    const text = sanitizeText(extractResponsesOutputText(body), 20000);
    if (!text) throw new Error("Other provider returned an empty response.");
    return {
      text,
      usage: extractResponsesUsage(body),
      citations: extractResponsesCitations(body),
    };
  } finally {
    clear();
  }
}

async function invokeLlmWithFallback(rawPayload: Record<string, unknown>): Promise<{
  provider: string;
  model: string;
  text: string;
  latencyMs: number;
  usage: Record<string, unknown>;
  citations?: unknown[];
  attempted: string[];
  modelAdjustedFrom?: string;
  responseId?: string;
  status?: string;
}> {
  const provider = normalizeProvider(rawPayload.provider);
  const fallbackProviders = Array.isArray(rawPayload.fallbackProviders)
    ? rawPayload.fallbackProviders.map((item) => normalizeProvider(item))
    : [];
  const providers = Array.from(new Set([provider, ...fallbackProviders]));
  const messages = normalizeLlmMessages(rawPayload.messages);
  if (!messages.length) throw new Error("messages are required.");
  const requestedModel = sanitizeText(rawPayload.model, 120) || DEFAULT_LLM_MODEL;
  const params = (rawPayload.params || {}) as Record<string, unknown>;
  const temperature = Math.max(0, Math.min(2, asFinite(params.temperature, 0.2)));
  const maxTokens = Math.max(64, Math.min(4000, Math.floor(asFinite(params.maxTokens, 600))));
  const allowWebSearch = asBoolean(params.webSearch ?? params.allowWebSearch ?? rawPayload.webSearch, true);
  // `/api/llm/run` returns JSON (not SSE), so streaming is intentionally disabled on this code path.
  const stream = false;
  const background = asBoolean(params.background ?? rawPayload.background, false);
  const tools: unknown[] = Array.isArray(params.tools ?? rawPayload.tools)
    ? ((params.tools ?? rawPayload.tools) as unknown[])
    : [];
  const jsonSchema = params.jsonSchema ?? rawPayload.jsonSchema;
  const userTier = normalizeLlmTier(rawPayload.userTier ?? rawPayload.tier ?? params.userTier ?? params.tier);

  const errors: string[] = [];
  const startedAt = Date.now();
  const ensureProviderText = (providerName: string, textValue: unknown) => {
    const text = sanitizeText(textValue, 20000);
    if (!text) throw new Error(`${providerName} returned an empty response.`);
    return text;
  };
  for (const currentProvider of providers) {
    const modelPick = pickModelForProvider(currentProvider, requestedModel, userTier);
    const model = modelPick.model;
    try {
      if (currentProvider === "openai") {
        const result = await invokeOpenAiLlm({
          model,
          messages,
          temperature,
          maxTokens,
          allowWebSearch,
          stream,
          background,
          tools,
          jsonSchema,
        });
        return {
          provider: currentProvider,
          model,
          text: ensureProviderText(currentProvider, result.text),
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          attempted: [...errors],
          modelAdjustedFrom: modelPick.adjustedFrom || undefined,
          responseId: result.responseId || undefined,
          status: result.status || undefined,
        };
      }
      if (currentProvider === "claude") {
        const result = await invokeClaudeLlm({ model, messages, temperature, maxTokens });
        return {
          provider: currentProvider,
          model,
          text: ensureProviderText(currentProvider, result.text),
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          attempted: [...errors],
          modelAdjustedFrom: modelPick.adjustedFrom || undefined,
        };
      }
      if (currentProvider === "gemini") {
        const result = await invokeGeminiLlm({ model, messages, temperature, maxTokens });
        return {
          provider: currentProvider,
          model,
          text: ensureProviderText(currentProvider, result.text),
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          attempted: [...errors],
          modelAdjustedFrom: modelPick.adjustedFrom || undefined,
        };
      }
      if (currentProvider === "deepseek") {
        const result = await invokeDeepseekLlm({ model, messages, temperature, maxTokens });
        return {
          provider: currentProvider,
          model,
          text: ensureProviderText(currentProvider, result.text),
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          attempted: [...errors],
          modelAdjustedFrom: modelPick.adjustedFrom || undefined,
        };
      }
      if (currentProvider === "mistral") {
        const result = await invokeMistralLlm({ model, messages, temperature, maxTokens });
        return {
          provider: currentProvider,
          model,
          text: ensureProviderText(currentProvider, result.text),
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          attempted: [...errors],
          modelAdjustedFrom: modelPick.adjustedFrom || undefined,
        };
      }
      if (currentProvider === "perplexity") {
        const result = await invokePerplexityLlm({ model, messages, temperature, maxTokens });
        return {
          provider: currentProvider,
          model,
          text: ensureProviderText(currentProvider, result.text),
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          citations: result.citations,
          attempted: [...errors],
          modelAdjustedFrom: modelPick.adjustedFrom || undefined,
        };
      }
      if (currentProvider === "qwen") {
        const result = await invokeQwenLlm({ model, messages, temperature, maxTokens });
        return {
          provider: currentProvider,
          model,
          text: ensureProviderText(currentProvider, result.text),
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          attempted: [...errors],
          modelAdjustedFrom: modelPick.adjustedFrom || undefined,
        };
      }
      if (currentProvider === "amazon_nova") {
        const result = await invokeAmazonNovaLlm({ model, messages, temperature, maxTokens });
        return {
          provider: currentProvider,
          model,
          text: ensureProviderText(currentProvider, result.text),
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          attempted: [...errors],
          modelAdjustedFrom: modelPick.adjustedFrom || undefined,
        };
      }
      if (currentProvider === "other") {
        const result = await invokeOtherLlm({
          model,
          messages,
          temperature,
          maxTokens,
          allowWebSearch,
          stream,
          background,
        });
        return {
          provider: currentProvider,
          model,
          text: ensureProviderText(currentProvider, result.text),
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          citations: result.citations,
          attempted: [...errors],
          modelAdjustedFrom: modelPick.adjustedFrom || undefined,
        };
      }
    } catch (error: any) {
      errors.push(`${currentProvider}:${sanitizeText(error?.message || error, 180) || "failed"}`);
    }
  }

  throw new Error(errors.join(" | ") || "No provider succeeded.");
}

async function resolveLlmTierForRequest(req: Request, payload: Record<string, unknown>): Promise<LlmUserTier> {
  const requestedTier = normalizeLlmTier(payload.userTier || payload.tier || (payload.params as Record<string, unknown> | undefined)?.tier);
  let user: admin.auth.DecodedIdToken | null = null;
  try {
    user = await verifyRequestUser(req, false);
  } catch (error: any) {
    if (String(error?.message || "") === "invalid_token") {
      throw new Error("invalid_token");
    }
  }
  if (!user?.uid) return requestedTier;
  const premium = await inferPremiumUser(user.uid);
  return premium ? "premium" : requestedTier;
}

async function decodePlayIntegrityToken(input: {
  integrityToken: string;
  packageName: string;
}): Promise<{
  ok: boolean;
  packageName: string;
  appRecognitionVerdict: string;
  deviceRecognitionVerdicts: string[];
  licensingVerdict: string;
  nonce: string;
  timestampMillis: string;
  raw: Record<string, unknown>;
}> {
  const client = await PLAY_INTEGRITY_AUTH.getClient();
  const endpoint = `https://playintegrity.googleapis.com/v1/${encodeURIComponent(input.packageName)}:decodeIntegrityToken`;
  const response = await client.request({
    url: endpoint,
    method: "POST",
    data: {
      integrity_token: input.integrityToken,
    },
  });
  const payload = ((response.data as any)?.tokenPayloadExternal || {}) as Record<string, unknown>;
  const appIntegrity = (payload.appIntegrity || {}) as Record<string, unknown>;
  const deviceIntegrity = (payload.deviceIntegrity || {}) as Record<string, unknown>;
  const accountDetails = (payload.accountDetails || {}) as Record<string, unknown>;
  const requestDetails = (payload.requestDetails || {}) as Record<string, unknown>;

  const appRecognitionVerdict = sanitizeText(appIntegrity.appRecognitionVerdict, 120);
  const deviceRecognitionVerdicts = Array.isArray(deviceIntegrity.deviceRecognitionVerdict)
    ? deviceIntegrity.deviceRecognitionVerdict.map((item: unknown) => sanitizeText(item, 120)).filter(Boolean)
    : [];
  const licensingVerdict = sanitizeText(accountDetails.appLicensingVerdict, 120);
  const nonce = sanitizeText(requestDetails.nonce, 320);
  const timestampMillis = sanitizeText(requestDetails.timestampMillis, 40);

  const ok =
    appRecognitionVerdict === "PLAY_RECOGNIZED" &&
    deviceRecognitionVerdicts.length > 0 &&
    licensingVerdict !== "UNLICENSED";

  return {
    ok,
    packageName: input.packageName,
    appRecognitionVerdict,
    deviceRecognitionVerdicts,
    licensingVerdict,
    nonce,
    timestampMillis,
    raw: payload,
  };
}

function isPostVisibleToViewer(post: Record<string, unknown>, viewerUid: string | null): boolean {
  const visibility = asString(post.visibility, "public");
  if (visibility === "public") return true;
  if (!viewerUid) return false;
  return viewerUid === asString(post.authorUid);
}

function postSupportsExpandedBody(postId: string, post: Record<string, unknown>): boolean {
  if (Boolean(sanitizeRichText(post.body, 200))) return true;
  const sourceRef = asPlainObject(post.sourceRef);
  if (sanitizeText(sourceRef.collection, 80) === MODEL_COUNCIL_RESPONSE_COLLECTION && sanitizeText(sourceRef.id, 220)) return true;
  return sanitizeText(postId, 220).startsWith("model_council_");
}

async function resolvePostBody(postId: string, post: Record<string, unknown>): Promise<{ body: string; bodyFormat: "markdown" | "text"; hasBody: boolean }> {
  const directBody = sanitizeRichText(post.body, 24000);
  const directFormat = sanitizeText(post.bodyFormat, 20).toLowerCase() === "text" ? "text" : "markdown";
  if (directBody) {
    return {
      body: directBody,
      bodyFormat: directFormat,
      hasBody: true,
    };
  }

  const sourceRef = asPlainObject(post.sourceRef);
  const sourceCollection = sanitizeText(sourceRef.collection, 80);
  const sourceId = sanitizeText(sourceRef.id, 220);
  const fallbackSourceId =
    sourceCollection === MODEL_COUNCIL_RESPONSE_COLLECTION && sourceId
      ? sourceId
      : sanitizeText(postId, 220).startsWith("model_council_")
      ? sanitizeText(postId, 220).slice("model_council_".length)
      : "";
  if (fallbackSourceId) {
    const sourceSnap = await db.collection(MODEL_COUNCIL_RESPONSE_COLLECTION).doc(fallbackSourceId).get().catch(() => null);
    if (sourceSnap?.exists) {
      const sourceData = (sourceSnap.data() || {}) as Record<string, unknown>;
      const sourceBody = sanitizeRichText(sourceData.answer, 24000);
      if (sourceBody) {
        return {
          body: sourceBody,
          bodyFormat: "markdown",
          hasBody: true,
        };
      }
    }
  }

  const authorUid = sanitizeText(post.authorUid, 220);
  if (authorUid) {
    const requestSnap = await db
      .collection("users")
      .doc(authorUid)
      .collection("requests")
      .where("explorePostId", "==", sanitizeText(postId, 220))
      .limit(1)
      .get()
      .catch(() => null);
    if (requestSnap && !requestSnap.empty) {
      const requestDoc = requestSnap.docs[0];
      const requestData = (requestDoc.data() || {}) as Record<string, unknown>;
      const type = normalizeMyRequestType(requestData.type) || "forecast";
      const outputsMeta = trimOutputsMeta(requestData.outputsMeta);
      const requestBody = buildMyRequestExploreBody(type, outputsMeta);
      if (requestBody) {
        return {
          body: requestBody,
          bodyFormat: "markdown",
          hasBody: true,
        };
      }
    }
  }

  return {
    body: "",
    bodyFormat: "markdown",
    hasBody: postSupportsExpandedBody(postId, post),
  };
}

function toPostResponse(
  snap: admin.firestore.QueryDocumentSnapshot | admin.firestore.DocumentSnapshot,
  viewerState: { liked: boolean; reposted: boolean; saved: boolean } = { liked: false, reposted: false, saved: false }
): Record<string, unknown> {
  const data = (snap.data() || {}) as Record<string, unknown>;
  const createdAtMs = getTimestampMs(data.createdAt);
  const updatedAtMs = getTimestampMs(data.updatedAt || data.createdAt);
  const counts = normalizeCounts(data.counts);

  return {
    id: snap.id,
    type: asString(data.type, "forecast"),
    authorUid: asString(data.authorUid),
    authorHandle: asString(data.authorHandle),
    authorPhotoURL: asString(data.authorPhotoURL),
    title: asString(data.title),
    caption: asString(data.caption),
    tickers: Array.isArray(data.tickers) ? data.tickers : [],
    tags: Array.isArray(data.tags) ? data.tags : [],
    preview: data.preview || { kind: "summary" },
    targetUrl: asString(data.targetUrl, "/explore"),
    visibility: asString(data.visibility, "public"),
    hasBody: postSupportsExpandedBody(snap.id, data),
    bodyFormat: sanitizeText(data.bodyFormat, 20).toLowerCase() === "text" ? "text" : "markdown",
    createdAt: new Date(createdAtMs).toISOString(),
    createdAtMs,
    updatedAt: new Date(updatedAtMs).toISOString(),
    counts,
    score: asFinite(data.score, 0),
    lastEngagedAtMs: getTimestampMs(data.lastEngagedAt || data.createdAt),
    viewer: {
      liked: viewerState.liked,
      reposted: viewerState.reposted,
      saved: viewerState.saved,
    },
  };
}

async function fetchViewerEngagement(
  postIds: string[],
  viewerUid: string | null
): Promise<Map<string, { liked: boolean; reposted: boolean; saved: boolean }>> {
  const engagement = new Map<string, { liked: boolean; reposted: boolean; saved: boolean }>();
  postIds.forEach((id) => engagement.set(id, { liked: false, reposted: false, saved: false }));

  if (!viewerUid || postIds.length === 0) return engagement;

  const likeRefs = postIds.map((postId) => db.collection("postLikes").doc(postId).collection("users").doc(viewerUid));
  const repostRefs = postIds.map((postId) => db.collection("postReposts").doc(postId).collection("users").doc(viewerUid));
  const savedRefs = postIds.map((postId) => db.collection("users").doc(viewerUid).collection("saved_post_state").doc(postId));

  const [likeDocs, repostDocs, savedDocs] = await Promise.all([
    db.getAll(...likeRefs),
    db.getAll(...repostRefs),
    db.getAll(...savedRefs),
  ]);

  likeDocs.forEach((doc, index) => {
    const postId = postIds[index];
    const current = engagement.get(postId) || { liked: false, reposted: false, saved: false };
    if (doc.exists) current.liked = true;
    engagement.set(postId, current);
  });

  repostDocs.forEach((doc, index) => {
    const postId = postIds[index];
    const current = engagement.get(postId) || { liked: false, reposted: false, saved: false };
    if (doc.exists) current.reposted = true;
    engagement.set(postId, current);
  });

  savedDocs.forEach((doc, index) => {
    const postId = postIds[index];
    const current = engagement.get(postId) || { liked: false, reposted: false, saved: false };
    const data = (doc.data() || {}) as Record<string, unknown>;
    current.saved = asBoolean(data.saved, false);
    engagement.set(postId, current);
  });

  return engagement;
}

function buildNextCursor(item: Record<string, unknown>, includeScore: boolean): ExploreCursor {
  const cursor: ExploreCursor = {
    id: asString(item.id),
    createdAtMs: asFinite(item.createdAtMs, Date.now()),
  };
  if (includeScore) cursor.score = asFinite(item.score, 0);
  return cursor;
}

async function listFollowingPosts(
  viewerUid: string,
  limit: number,
  cursor: ExploreCursor | null,
  tickerFilter: string,
  queryText: string
): Promise<{ posts: admin.firestore.QueryDocumentSnapshot[]; nextCursor: string | null }> {
  const followsSnap = await db.collection("users").doc(viewerUid).collection("follows").limit(250).get();
  const followedAuthorUids = followsSnap.docs.map((doc) => doc.id).filter(Boolean);

  if (followedAuthorUids.length === 0) {
    return { posts: [], nextCursor: null };
  }

  const chunks: string[][] = [];
  for (let i = 0; i < followedAuthorUids.length; i += 10) {
    chunks.push(followedAuthorUids.slice(i, i + 10));
  }

  const perChunkLimit = Math.max(limit, 20);
  const snapshots = await Promise.all(
    chunks.map((authorChunk) =>
      db
        .collection("posts")
        .where("visibility", "==", "public")
        .where("authorUid", "in", authorChunk)
        .orderBy("createdAt", "desc")
        .orderBy(admin.firestore.FieldPath.documentId(), "desc")
        .limit(perChunkLimit)
        .get()
    )
  );

  const dedup = new Map<string, admin.firestore.QueryDocumentSnapshot>();
  snapshots.forEach((snap) => {
    snap.docs.forEach((doc) => {
      if (!dedup.has(doc.id)) dedup.set(doc.id, doc);
    });
  });

  let docs = Array.from(dedup.values());

  if (tickerFilter) {
    docs = docs.filter((doc) => {
      const tickers = (doc.data().tickers || []) as string[];
      return tickers.includes(tickerFilter);
    });
  }

  if (queryText.startsWith("@")) {
    const handle = normalizeHandle(queryText.slice(1));
    docs = docs.filter((doc) => normalizeHandle(doc.data().authorHandle) === handle);
  }

  docs.sort((a, b) => {
    const aMs = getTimestampMs(a.data().createdAt);
    const bMs = getTimestampMs(b.data().createdAt);
    if (aMs !== bMs) return bMs - aMs;
    return b.id.localeCompare(a.id);
  });

  if (cursor) {
    docs = docs.filter((doc) => {
      const ms = getTimestampMs(doc.data().createdAt);
      if (ms < cursor.createdAtMs) return true;
      if (ms > cursor.createdAtMs) return false;
      return doc.id < cursor.id;
    });
  }

  const sliced = docs.slice(0, limit + 1);
  const hasMore = sliced.length > limit;
  const page = hasMore ? sliced.slice(0, limit) : sliced;
  const lastDoc = page[page.length - 1];

  const nextCursor = hasMore && lastDoc
    ? encodeCursor({
        id: lastDoc.id,
        createdAtMs: getTimestampMs(lastDoc.data().createdAt),
      })
    : null;

  return {
    posts: page,
    nextCursor,
  };
}

async function updatePostEngagement(
  postId: string,
  updateFn: (tx: admin.firestore.Transaction, postRef: admin.firestore.DocumentReference, data: Record<string, unknown>) => Promise<Record<string, unknown>>
): Promise<Record<string, unknown>> {
  const postRef = db.collection("posts").doc(postId);

  return db.runTransaction(async (tx) => {
    const postSnap = await tx.get(postRef);
    if (!postSnap.exists) {
      throw new Error("not_found");
    }

    const postData = (postSnap.data() || {}) as Record<string, unknown>;
    const visibility = asString(postData.visibility, "public");
    if (visibility === "deleted") {
      throw new Error("gone");
    }

    const next = await updateFn(tx, postRef, postData);
    return next;
  });
}

async function syncTopicsForUser(uid: string): Promise<void> {
  const userRef = db.collection("users").doc(uid);
  const [userSnap, tokenSnap, followsSnap, watchSnap] = await Promise.all([
    userRef.get(),
    userRef.collection("fcmTokens").limit(100).get(),
    userRef.collection("follows").limit(200).get(),
    userRef.collection("watchTickers").limit(200).get(),
  ]);

  if (tokenSnap.empty) return;

  const prefs = normalizeNotificationPrefs({}, (((userSnap.data() || {}) as any).notificationPrefs || {}) as Record<string, unknown>);
  const enableGlobal = prefs.global;
  const enableFollowing = prefs.following;
  const enableTickers = prefs.tickers;

  const desiredTopics = new Set<string>();
  if (enableGlobal) desiredTopics.add("explore-global");
  if (enableFollowing) {
    followsSnap.docs.forEach((doc) => {
      desiredTopics.add(`author-${doc.id}`);
    });
  }
  if (enableTickers) {
    watchSnap.docs.forEach((doc) => {
      const ticker = normalizeTicker(doc.id || doc.data().ticker);
      if (ticker) desiredTopics.add(`ticker-${ticker}`);
    });
  }

  for (const tokenDoc of tokenSnap.docs) {
    const token = tokenDoc.id;
    if (!token) continue;

    const currentTopics = new Set<string>(Array.isArray(tokenDoc.data().topics) ? tokenDoc.data().topics : []);
    const toSubscribe = Array.from(desiredTopics).filter((topic) => !currentTopics.has(topic));
    const toUnsubscribe = Array.from(currentTopics).filter((topic) => !desiredTopics.has(topic));

    for (const topic of toSubscribe) {
      try {
        await messaging.subscribeToTopic([token], topic);
      } catch (error) {
        console.warn(`[Explore] subscribeToTopic failed (${topic})`, error);
      }
    }

    for (const topic of toUnsubscribe) {
      try {
        await messaging.unsubscribeFromTopic([token], topic);
      } catch (error) {
        console.warn(`[Explore] unsubscribeFromTopic failed (${topic})`, error);
      }
    }

    await tokenDoc.ref.set(
      {
        topics: Array.from(desiredTopics),
        lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
  }
}

async function writeNotificationHistoryItem(params: {
  uid: string;
  category: NotificationCategory;
  title: string;
  body: string;
  deepLink: string;
  hidden?: boolean;
  nextSteps?: string[];
  metadata?: Record<string, unknown>;
}): Promise<string> {
  const ref = db.collection("notifications").doc(params.uid).collection("items").doc();
  await ref.set(
    {
      category: params.category,
      title: sanitizeText(params.title, 160) || `${notificationCategoryLabel(params.category)} update`,
      body: sanitizeText(params.body, 400),
      deepLink: normalizeNotificationDeepLink(params.deepLink),
      hidden: Boolean(params.hidden),
      read: false,
      nextSteps: Array.isArray(params.nextSteps)
        ? params.nextSteps.map((item) => sanitizeText(item, 120)).filter(Boolean).slice(0, 4)
        : [],
      metadata: params.metadata && typeof params.metadata === "object" ? params.metadata : {},
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  return ref.id;
}

async function sendPushToUserTokens(params: {
  uid: string;
  category: NotificationCategory;
  title: string;
  body: string;
  deepLink: string;
  hidden?: boolean;
  nextSteps?: string[];
  metadata?: Record<string, unknown>;
  force?: boolean;
}): Promise<{ uid: string; delivered: number; attempted: number; skipped: boolean; reason: string; historyId: string }> {
  const uid = sanitizeText(params.uid, 220);
  if (!uid) {
    return { uid: "", delivered: 0, attempted: 0, skipped: true, reason: "invalid_uid", historyId: "" };
  }

  const userRef = db.collection("users").doc(uid);
  const [userSnap, tokenSnap] = await Promise.all([userRef.get(), userRef.collection("fcmTokens").limit(250).get()]);
  const userData = (userSnap.data() || {}) as Record<string, unknown>;
  const prefs = normalizeNotificationPrefs({}, (userData.notificationPrefs || {}) as Record<string, unknown>);
  const category = normalizeNotificationCategory(params.category);
  if (!params.force && !isNotificationCategoryEnabled(prefs, category)) {
    return { uid, delivered: 0, attempted: 0, skipped: true, reason: "pref_disabled", historyId: "" };
  }

  const deepLink = normalizeNotificationDeepLink(params.deepLink);
  const absoluteLink = absoluteNotificationLink(deepLink);
  const path = deepLink.startsWith("/") ? deepLink : (() => {
    try {
      const parsed = new URL(deepLink);
      return `${parsed.pathname || "/"}${parsed.search || ""}${parsed.hash || ""}`;
    } catch {
      return "/notifications";
    }
  })();

  const historyId = await writeNotificationHistoryItem({
    uid,
    category,
    title: params.title,
    body: params.body,
    deepLink,
    hidden: params.hidden ?? (category === "inactive"),
    nextSteps: params.nextSteps,
    metadata: params.metadata,
  });

  const tokens = tokenSnap.docs.map((doc) => sanitizeText(doc.id, 4096)).filter(Boolean);
  if (!tokens.length) {
    return { uid, delivered: 0, attempted: 0, skipped: true, reason: "no_tokens", historyId };
  }

  const message = {
    tokens,
    notification: {
      title: sanitizeText(params.title, 160) || `${notificationCategoryLabel(category)} update`,
      body: sanitizeText(params.body, 300) || "You have a new Quantura update.",
    },
    data: {
      category,
      path,
      deepLink,
      url: absoluteLink,
      historyId,
    },
    webpush: {
      fcmOptions: {
        link: absoluteLink,
      },
    },
    android: {
      notification: {
        channelId: "quantura_push",
      },
    },
    apns: {
      payload: {
        aps: {
          sound: "default",
        },
      },
    },
  };

  const result = await messaging.sendEachForMulticast(message);
  const invalidCodes = new Set([
    "messaging/registration-token-not-registered",
    "messaging/invalid-registration-token",
    "messaging/invalid-argument",
  ]);

  await Promise.all(
    result.responses.map((response, index) => {
      if (response.success) return Promise.resolve();
      const code = sanitizeText((response.error as any)?.code, 80);
      if (!invalidCodes.has(code)) return Promise.resolve();
      const token = tokens[index];
      if (!token) return Promise.resolve();
      return userRef.collection("fcmTokens").doc(token).delete().catch(() => undefined);
    })
  );

  return {
    uid,
    delivered: result.successCount,
    attempted: tokens.length,
    skipped: false,
    reason: "",
    historyId,
  };
}

async function collectUsersByFollowedAuthor(authorUid: string): Promise<Set<string>> {
  const clean = sanitizeText(authorUid, 220);
  const out = new Set<string>();
  if (!clean) return out;
  const followsSnap = await db
    .collectionGroup("follows")
    .where(admin.firestore.FieldPath.documentId(), "==", clean)
    .limit(600)
    .get();
  followsSnap.docs.forEach((doc) => {
    const uid = doc.ref.parent.parent?.id || "";
    if (uid) out.add(uid);
  });
  return out;
}

async function collectUsersByWatchTickers(tickers: string[]): Promise<Set<string>> {
  const cleanTickers = Array.from(new Set((Array.isArray(tickers) ? tickers : []).map((item) => normalizeTicker(item)).filter(Boolean)))
    .slice(0, 6);
  const out = new Set<string>();
  for (const ticker of cleanTickers) {
    const watchSnap = await db
      .collectionGroup("watchTickers")
      .where(admin.firestore.FieldPath.documentId(), "==", ticker)
      .limit(800)
      .get();
    watchSnap.docs.forEach((doc) => {
      const uid = doc.ref.parent.parent?.id || "";
      if (uid) out.add(uid);
    });
  }
  return out;
}

async function collectUsersForIpoNotifications(limit = 1200): Promise<Set<string>> {
  const out = new Set<string>();
  const snap = await db.collection("users").limit(Math.max(1, Math.min(limit, 2000))).get();
  snap.docs.forEach((doc) => out.add(doc.id));
  return out;
}

async function deleteCollectionDocs(query: admin.firestore.Query, batchSize = 200): Promise<void> {
  while (true) {
    const snap = await query.limit(batchSize).get();
    if (snap.empty) return;
    const batch = db.batch();
    snap.docs.forEach((doc) => batch.delete(doc.ref));
    await batch.commit();
    if (snap.size < batchSize) return;
  }
}

function isAdminEmail(email: unknown): boolean {
  return asString(email).trim().toLowerCase() === ADMIN_EMAIL;
}

function systemFolderById(folderId: string): SystemFolderConfig | null {
  return SYSTEM_FOLDERS.find((folder) => folder.id === folderId) || null;
}

async function inferPremiumUser(uid: string): Promise<boolean> {
  try {
    const orders = await db
      .collection("orders")
      .where("userId", "==", uid)
      .orderBy("createdAt", "desc")
      .limit(30)
      .get();
    return orders.docs.some((doc) => {
      const data = doc.data() || {};
      const paymentStatus = asString(data.paymentStatus).trim().toLowerCase();
      const stripePaymentStatus = asString(data.stripePaymentStatus).trim().toLowerCase();
      const status = asString(data.status).trim().toLowerCase();
      return ["paid", "succeeded", "complete", "completed", "active"].includes(paymentStatus)
        || ["paid", "succeeded", "complete", "completed", "active"].includes(stripePaymentStatus)
        || ["paid", "completed", "active"].includes(status);
    });
  } catch {
    return false;
  }
}

async function buildProfilePayload(
  userDocId: string,
  userData: Record<string, unknown>,
  viewerUid: string | null
): Promise<Record<string, unknown>> {
  const profile = (userData.profile || {}) as Record<string, unknown>;
  const handle = normalizeHandle(userData.handle || profile.username || userDocId.slice(0, 12)) || `user-${userDocId.slice(0, 8)}`;
  const isOwner = viewerUid === userDocId;
  const email = asString(userData.email).trim().toLowerCase();
  const isAdmin = isAdminEmail(email);
  const explicitPremium =
    asBoolean(profile.premium, false)
    || asBoolean(userData.premium, false)
    || asBoolean(profile.verified, false)
    || ["go", "plus", "pro", "business", "desk", "premium"].includes(
      asString(profile.plan || userData.plan || userData.subscriptionTier).trim().toLowerCase()
    );
  const premium = explicitPremium ? true : await inferPremiumUser(userDocId);
  const verified = isAdmin || premium || asBoolean(profile.verified, false);
  const publicEmailOptIn = asBoolean(profile.publicEmailOptIn, false);
  const publicProfile = asBoolean(profile.publicProfile, false);
  const photoURL = asString(userData.photoURL || profile.photoURL || profile.avatarUrl || "");
  const name = sanitizeText(userData.name || userData.displayName || profile.name || "", 120);
  const username = normalizeHandle(profile.username || userData.handle || "") || handle;
  const profileUrl = `${PUBLIC_ORIGIN}/u/${encodeURIComponent(handle)}`;

  return {
    uid: userDocId,
    handle,
    username,
    name,
    photoURL,
    bio: sanitizeText(profile.bio || "", 400),
    publicProfile,
    publicEmailOptIn,
    email: isOwner || publicEmailOptIn ? email : "",
    emailVisible: Boolean(isOwner || publicEmailOptIn),
    verified,
    premium,
    isAdmin,
    profileUrl,
    canEdit: isOwner,
  };
}

async function upsertSavedPostState(
  uid: string,
  postId: string,
  patch: Partial<{ liked: boolean; reposted: boolean; saved: boolean; shared: boolean }>
): Promise<void> {
  if (!uid || !postId) return;
  const ref = db.collection("users").doc(uid).collection("saved_post_state").doc(postId);
  const payload: Record<string, unknown> = {
    postId,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  if (typeof patch.liked === "boolean") payload.liked = patch.liked;
  if (typeof patch.reposted === "boolean") payload.reposted = patch.reposted;
  if (typeof patch.saved === "boolean") payload.saved = patch.saved;
  if (typeof patch.shared === "boolean") payload.shared = patch.shared;
  await ref.set(payload, { merge: true });
}

async function listSystemFolderCounts(uid: string): Promise<Record<string, number>> {
  const counts: Record<string, number> = {};
  const base = db.collection("users").doc(uid).collection("saved_post_state");
  await Promise.all(
    SYSTEM_FOLDERS.map(async (folder) => {
      const snap = await base.where(folder.flag, "==", true).limit(400).get();
      counts[folder.id] = snap.size;
    })
  );
  return counts;
}

function buildPostSavedItem(postId: string, data: Record<string, unknown>): Record<string, unknown> {
  const createdAtMs = getTimestampMs(data.createdAt);
  const updatedAtMs = getTimestampMs(data.updatedAt || data.createdAt);
  const tickers = Array.isArray(data.tickers) ? data.tickers : [];
  return {
    itemType: "post",
    sourceId: postId,
    itemId: `post__${postId}`,
    title: asString(data.title, "Explore post"),
    subtitle: asString(data.caption, ""),
    ticker: asString(tickers[0] || ""),
    targetUrl: `/explore?post=${encodeURIComponent(postId)}`,
    createdAtMs,
    updatedAtMs,
    visibility: asString(data.visibility, "public"),
  };
}

async function resolveSavedItem(
  uid: string,
  itemType: SavedItemType,
  sourceId: string
): Promise<Record<string, unknown> | null> {
  const cleanSourceId = normalizeSourceId(sourceId);
  if (!uid || !cleanSourceId) return null;

  if (itemType === "forecast") {
    const snap = await db.collection("forecast_requests").doc(cleanSourceId).get();
    if (!snap.exists) return null;
    const data = (snap.data() || {}) as Record<string, unknown>;
    if (asString(data.userId) !== uid) return null;
    return {
      itemType,
      sourceId: snap.id,
      itemId: buildFolderItemDocId(itemType, snap.id),
      title: asString(data.title, `${normalizeTicker(data.ticker)} forecast`),
      subtitle: asString(data.serviceMessage || data.notes || ""),
      ticker: normalizeTicker(data.ticker),
      targetUrl: `/forecasting?forecastId=${encodeURIComponent(snap.id)}`,
      createdAtMs: getTimestampMs(data.createdAt),
      updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
    };
  }

  if (itemType === "screener") {
    const snap = await db.collection("screener_runs").doc(cleanSourceId).get();
    if (!snap.exists) return null;
    const data = (snap.data() || {}) as Record<string, unknown>;
    if (asString(data.userId) !== uid) return null;
    return {
      itemType,
      sourceId: snap.id,
      itemId: buildFolderItemDocId(itemType, snap.id),
      title: asString(data.title, "Screener run"),
      subtitle: asString(data.notes || ""),
      ticker: normalizeTicker(((data.results as Array<Record<string, unknown>> | undefined) || [])[0]?.symbol),
      targetUrl: `/screener?runId=${encodeURIComponent(snap.id)}`,
      createdAtMs: getTimestampMs(data.createdAt),
      updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
    };
  }

  if (itemType === "model_council") {
    const snap = await db.collection(MODEL_COUNCIL_RESPONSE_COLLECTION).doc(cleanSourceId).get();
    if (!snap.exists) return null;
    const data = (snap.data() || {}) as Record<string, unknown>;
    if (asString(data.userId) !== uid) return null;
    return {
      itemType,
      sourceId: snap.id,
      itemId: buildFolderItemDocId(itemType, snap.id),
      title: `${normalizeTicker(data.ticker) || "Ticker"} Model Council`,
      subtitle: asString(data.question || ""),
      ticker: normalizeTicker(data.ticker),
      targetUrl: `/model-council`,
      createdAtMs: getTimestampMs(data.createdAt),
      updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
    };
  }

  const snap = await db.collection("posts").doc(cleanSourceId).get();
  if (!snap.exists) return null;
  const data = (snap.data() || {}) as Record<string, unknown>;
  if (!isPostVisibleToViewer(data, uid)) return null;
  return buildPostSavedItem(snap.id, data);
}

async function listSystemFolderItems(uid: string, folderId: string, limit: number): Promise<Record<string, unknown>[]> {
  const folder = systemFolderById(folderId);
  if (!folder) return [];
  const stateSnap = await db
    .collection("users")
    .doc(uid)
    .collection("saved_post_state")
    .where(folder.flag, "==", true)
    .limit(Math.max(limit, 80))
    .get();
  if (stateSnap.empty) return [];

  const postRefs = stateSnap.docs.map((doc) => db.collection("posts").doc(doc.id));
  const postDocs = await db.getAll(...postRefs);

  const out = postDocs
    .filter((snap) => snap.exists)
    .map((snap) => {
      const data = (snap.data() || {}) as Record<string, unknown>;
      return buildPostSavedItem(snap.id, data);
    })
    .sort((a, b) => asFinite(b.updatedAtMs, 0) - asFinite(a.updatedAtMs, 0));

  return out.slice(0, limit);
}

function matchesSearchQuery(item: Record<string, unknown>, query: string): boolean {
  const normalized = sanitizeText(query, 140).toLowerCase();
  if (!normalized) return true;
  const haystack = [
    asString(item.title),
    asString(item.subtitle),
    asString(item.ticker),
    asString(item.itemType),
  ]
    .join(" ")
    .toLowerCase();
  return haystack.includes(normalized);
}

ROUTES.get("/health", (_req, res) => {
  res.status(200).json({ ok: true, service: "quantura-explore-api", ts: new Date().toISOString() });
});

ROUTES.get("/ticker/history", async (req, res) => {
  try {
    const query = asPlainObject(req.query);
    const ticker = normalizeTicker(query.ticker || query.symbol);
    if (!ticker) {
      res.status(400).json({ error: "invalid_ticker" });
      return;
    }
    const payload = await fetchYahooHistoryBars({
      ticker,
      interval: query.interval,
      start: query.start,
      end: query.end,
    });
    res.status(200).json(payload);
  } catch (error: any) {
    const detail = sanitizeText(error?.message || error, 260) || "history_lookup_failed";
    const status = /ticker is required|invalid_ticker|start|end/i.test(detail) ? 400 : 502;
    res.status(status).json({ error: status === 400 ? "invalid_history_request" : "history_lookup_failed", detail });
  }
});

ROUTES.post("/forecast/run", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const body = asPlainObject(req.body);
    const ticker = normalizeTicker(body.ticker);
    const interval = sanitizeText(body.interval, 10).toLowerCase() === "1h" ? "1h" : "1d";
    const horizonLimit = interval === "1h" ? 240 : 365;
    const horizon = Math.max(1, Math.min(horizonLimit, Math.floor(asFinite(body.horizon, 0))));
    if (!ticker) {
      res.status(400).json({ error: "invalid_ticker" });
      return;
    }
    if (!horizon) {
      res.status(400).json({ error: "invalid_horizon" });
      return;
    }

    const history = await fetchYahooHistoryBars({
      ticker,
      interval,
      start: body.start,
      end: body.end,
    });
    const forecast = buildForecastFromHistory({
      ticker,
      interval,
      horizon,
      quantiles: DEFAULT_FORECAST_QUANTILES,
      historyRows: history.rows,
    });

    const createdAt = admin.firestore.FieldValue.serverTimestamp();
    const title = `${ticker} forecast`;
    const docRef = db.collection("forecast_requests").doc();
    const payload: Record<string, unknown> = {
      userId: user.uid,
      userEmail: sanitizeText((user as any)?.email, 320),
      workspaceId: sanitizeText(body.workspaceId, 220) || user.uid,
      ticker,
      title,
      interval,
      horizon,
      service: "prophet",
      engine: forecast.engine,
      status: "completed",
      quantiles: forecast.quantiles,
      start: sanitizeText(body.start, 40),
      end: history.actualEnd,
      forecastRows: forecast.forecastRows,
      forecastPreview: forecast.forecastPreview,
      forecastQuantilesEnd: forecast.forecastQuantilesEnd,
      metrics: forecast.metrics,
      serviceMessage: forecast.serviceMessage,
      tradeRationale: forecast.tradeRationale,
      createdAt,
      updatedAt: createdAt,
      meta: trimOutputsMeta(body.meta),
    };
    await docRef.set(payload, { merge: false });
    try {
      await createPostFromResult("forecast", docRef.id, {
        ...payload,
        createdAt: admin.firestore.Timestamp.now(),
      });
    } catch (postError) {
      console.error("[Forecast] post creation failed", { requestId: docRef.id, postError });
    }

    await upsertOwnedMyRequestFromSystem(user.uid, {
      requestId: buildMyRequestDocId("forecast", docRef.id),
      type: "forecast",
      title,
      input: {
        ticker,
        interval,
        horizon,
        service: "prophet",
        quantiles: forecast.quantiles,
      },
      outputsMeta: {
        summary: forecast.serviceMessage,
        serviceMessage: forecast.serviceMessage,
        service: "prophet",
        interval,
        forecastRowsCount: forecast.forecastRows.length,
        metrics: trimOutputsMeta(forecast.metrics),
        topSymbols: [ticker],
      },
      sourceRef: {
        collection: "forecast_requests",
        id: docRef.id,
      },
    });

    res.status(200).json({
      ok: true,
      requestId: docRef.id,
      ticker,
      interval,
      horizon,
      service: "prophet",
      engine: forecast.engine,
      status: "completed",
      quantiles: forecast.quantiles,
      forecastRows: forecast.forecastRows,
      forecastSeries: forecast.forecastRows,
      forecastPreview: forecast.forecastPreview,
      forecastQuantilesEnd: forecast.forecastQuantilesEnd,
      metrics: forecast.metrics,
      serviceMessage: forecast.serviceMessage,
      tradeRationale: forecast.tradeRationale,
    });
  } catch (error: any) {
    const detail = sanitizeText(error?.message || error, 260) || "forecast_run_failed";
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (/quantile|required|history|horizon/i.test(detail)) {
      res.status(400).json({ error: "forecast_run_failed", detail });
      return;
    }
    console.error("[Forecast] run failed", error);
    res.status(500).json({ error: "forecast_run_failed", detail });
  }
});

ROUTES.delete("/forecast/:forecastId", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const forecastId = sanitizeText(req.params.forecastId, 220);
    if (!forecastId) {
      res.status(400).json({ error: "invalid_forecast_id" });
      return;
    }
    const ref = db.collection("forecast_requests").doc(forecastId);
    const snap = await ref.get();
    if (!snap.exists) {
      res.status(404).json({ error: "forecast_not_found" });
      return;
    }
    const data = (snap.data() || {}) as Record<string, unknown>;
    if (sanitizeText(data.userId, 220) !== user.uid) {
      res.status(403).json({ error: "forbidden" });
      return;
    }
    await ref.delete();
    res.status(200).json({ ok: true, deleted: true, forecastId });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Forecast] delete failed", error);
    res.status(500).json({ error: "forecast_delete_failed" });
  }
});

ROUTES.post("/screener/run", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    if (!githubActionsConfigured()) {
      res.status(500).json({ error: "screener_workflow_not_configured", detail: "GitHub Actions token is not configured." });
      return;
    }
    const body = asPlainObject(req.body);
    const marketCapValue = body.minMarketCap ?? body.minCap ?? asPlainObject(body.marketCapFilter).value;
    const minMarketCap = Math.max(0, Math.floor(asFinite(marketCapValue, 100_000_000_000)));
    const autoPublishRequested = asBoolean(body.autoPublish, true);
    const createdAt = admin.firestore.FieldValue.serverTimestamp();
    const docRef = db.collection("screener_runs").doc();
    const runKey = buildScreenerWorkflowRunKey(docRef.id);
    const payload: Record<string, unknown> = {
      userId: user.uid,
      userEmail: sanitizeText((user as any)?.email, 320),
      workspaceId: sanitizeText(body.workspaceId, 220) || user.uid,
      source: "github_actions",
      market: "us",
      universe: "both",
      minMarketCap,
      title: `Stock screener · ${fmtCompactCurrency(minMarketCap) || "$100B"} floor`,
      notes: "",
      status: "queued",
      results: [],
      resultsFound: 0,
      serviceMessage: `Queued GitHub Actions stock screener above the ${fmtCompactCurrency(minMarketCap)} floor.`,
      filters: {},
      appliedFilters: [`Market cap >= ${fmtCompactCurrency(minMarketCap)}`],
      ignoredFilters: [],
      modelUsed: "daily_prophet_signal_tracker",
      modelProvider: "github_actions",
      modelTier: "workflow",
      autoPublishRequested,
      autoPublishPending: autoPublishRequested,
      workflowRunKey: runKey,
      workflowStatus: "queued",
      workflowConclusion: "",
      workflowName: GITHUB_SCREENER_WORKFLOW,
      workflowBranch: GITHUB_ACTIONS_BRANCH,
      isPublic: false,
      createdAt,
      updatedAt: createdAt,
      meta: trimOutputsMeta(body.meta),
    };
    await docRef.set(payload, { merge: false });
    try {
      await createPostFromResult("screener", docRef.id, {
        ...payload,
        createdAt: admin.firestore.Timestamp.now(),
      });
    } catch (postError) {
      console.error("[Screener] post creation failed", { runId: docRef.id, postError });
    }

    let requestResponse = await syncScreenerMyRequestFromRun(user.uid, docRef.id, { ...payload, id: docRef.id }, false);

    try {
      await dispatchGithubScreenerWorkflow({ runKey, minMarketCap });
      const workflowRun = await waitForGithubWorkflowRun(runKey, 12000).catch(() => null);
      const dispatchPatch: Record<string, unknown> = {
        status: workflowRun ? "running" : "queued",
        workflowStatus: workflowRun?.status || "queued",
        serviceMessage: workflowRun
          ? `GitHub Actions is running the stock screener above the ${fmtCompactCurrency(minMarketCap)} floor.`
          : `GitHub Actions dispatch accepted for the ${fmtCompactCurrency(minMarketCap)} floor.`,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      if (workflowRun) {
        dispatchPatch.workflowRunId = workflowRun.id;
        dispatchPatch.workflowRunUrl = workflowRun.htmlUrl;
        dispatchPatch.workflowRunNumber = workflowRun.runNumber || null;
        const workflowJobs = await listGithubJobsForRun(workflowRun.id).catch(() => []);
        if (workflowJobs.length) {
          Object.assign(dispatchPatch, buildGithubWorkflowProgressSnapshot(workflowJobs));
          if (sanitizeText(dispatchPatch.workflowProgress, 240)) {
            dispatchPatch.serviceMessage = sanitizeText(dispatchPatch.workflowProgress, 240);
          }
        }
      }
      await docRef.set(dispatchPatch, { merge: true });
    } catch (dispatchError: any) {
      const detail = sanitizeText(dispatchError?.message || dispatchError, 260) || "github_workflow_dispatch_failed";
      await docRef.set(
        {
          status: "failed",
          workflowStatus: "failed",
          workflowConclusion: "dispatch_failed",
          serviceMessage: detail,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      const failedSnap = await docRef.get().catch(() => null);
      if (failedSnap?.exists) {
        requestResponse =
          (await syncScreenerMyRequestFromRun(
            user.uid,
            docRef.id,
            { id: failedSnap.id, ...(failedSnap.data() || {}) } as Record<string, unknown>,
            false
          )) || requestResponse;
      }
      res.status(500).json({ error: "screener_run_failed", detail });
      return;
    }

    const refreshed = await docRef.get();
    const result: Record<string, unknown> = { id: refreshed.id, ...(refreshed.data() || {}) };
    requestResponse = (await syncScreenerMyRequestFromRun(user.uid, docRef.id, result, false)) || requestResponse;

    res.status(200).json({
      ok: true,
      runId: result.id,
      run: buildSharedScreenerRunPayload(String(result.id || docRef.id), result),
      request: requestResponse,
      title: result.title,
      status: result.status,
      serviceMessage: result.serviceMessage,
      minMarketCap,
      workflowRunId: result.workflowRunId || null,
      workflowRunUrl: result.workflowRunUrl || "",
      workflowRunNumber: result.workflowRunNumber || null,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    const detail = sanitizeText(error?.message || error, 260) || "screener_run_failed";
    console.error("[Screener] run failed", error);
    res.status(500).json({ error: "screener_run_failed", detail });
  }
});

ROUTES.get("/screener/github-history", async (req, res) => {
  try {
    if (!githubActionsConfigured()) {
      res.status(503).json({ error: "screener_workflow_not_configured" });
      return;
    }
    const requestedLimit = Math.floor(asFinite(req.query.limit, 12));
    const limit = Math.max(1, Math.min(60, requestedLimit || 60));
    const workflowRuns = await listRecentScheduledGithubScreenerRuns(limit);
    const items = await Promise.all(
      workflowRuns.map(async (workflowRun) => {
        const [sourceMatch, artifacts] = await Promise.all([
          findScreenerRunRecordByWorkflowRunId(workflowRun.id).catch(() => null),
          listGithubArtifactsForRun(workflowRun.id).catch(() => []),
        ]);
        return buildPublicGithubScreenerRunSummary(workflowRun, sourceMatch, artifacts);
      })
    );
    res.status(200).json({
      ok: true,
      items,
    });
  } catch (error) {
    console.error("[Screener] github history failed", error);
    res.status(500).json({ error: "screener_github_history_failed" });
  }
});

ROUTES.get("/screener/github-history/:workflowRunId/artifacts/:artifactId/download", async (req, res) => {
  try {
    if (!githubActionsConfigured()) {
      res.status(503).json({ error: "screener_workflow_not_configured" });
      return;
    }
    const workflowRunId = Math.max(0, Math.floor(asFinite(req.params.workflowRunId, 0)));
    const artifactId = Math.max(0, Math.floor(asFinite(req.params.artifactId, 0)));
    if (!workflowRunId || !artifactId) {
      res.status(400).json({ error: "invalid_github_artifact_request" });
      return;
    }
    const workflowRun = await getGithubWorkflowRun(workflowRunId);
    if (!workflowRun) {
      res.status(404).json({ error: "workflow_run_not_found" });
      return;
    }
    if (
      workflowRun.path &&
      !workflowRun.path.toLowerCase().includes(String(GITHUB_SCREENER_WORKFLOW || "").trim().toLowerCase())
    ) {
      res.status(404).json({ error: "workflow_run_not_found" });
      return;
    }
    const artifacts = await listGithubArtifactsForRun(workflowRunId);
    const artifact = artifacts.find((item) => item.id === artifactId) || null;
    if (!artifact) {
      res.status(404).json({ error: "artifact_not_found" });
      return;
    }
    if (artifact.expired) {
      res.status(410).json({ error: "artifact_expired" });
      return;
    }
    const response = await githubApiRequest(`/actions/artifacts/${encodeURIComponent(String(artifactId))}/zip`, {
      method: "GET",
    });
    const buffer = Buffer.from(await response.arrayBuffer());
    const safeBaseName =
      sanitizeText(artifact.name, 140)
        .replace(/[^A-Za-z0-9._-]+/g, "-")
        .replace(/^-+|-+$/g, "") || "github-artifact";
    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Cache-Control", "private, max-age=300");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${safeBaseName}-${encodeURIComponent(String(workflowRun.runNumber || workflowRunId))}.zip"`
    );
    res.status(200).send(buffer);
  } catch (error) {
    console.error("[Screener] github artifact download failed", error);
    res.status(500).json({ error: "screener_github_artifact_download_failed" });
  }
});

ROUTES.get("/screener/:runId", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const runId = sanitizeText(req.params.runId, 220);
    if (!runId) {
      res.status(400).json({ error: "invalid_run_id" });
      return;
    }
    const runDoc = await syncGithubScreenerRunRecord(runId, user.uid);
    res.status(200).json({
      ok: true,
      run: buildSharedScreenerRunPayload(runId, runDoc),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "screener_not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "forbidden") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Screener] load failed", error);
    res.status(500).json({ error: "screener_load_failed" });
  }
});

ROUTES.patch("/screener/:runId", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const runId = sanitizeText(req.params.runId, 220);
    const nextTitle = sanitizeText(asPlainObject(req.body).title, 180);
    if (!runId || !nextTitle) {
      res.status(400).json({ error: "invalid_screener_update" });
      return;
    }
    const ref = db.collection("screener_runs").doc(runId);
    const snap = await ref.get();
    if (!snap.exists) {
      res.status(404).json({ error: "screener_not_found" });
      return;
    }
    const data = (snap.data() || {}) as Record<string, unknown>;
    if (sanitizeText(data.userId, 220) !== user.uid) {
      res.status(403).json({ error: "forbidden" });
      return;
    }
    await ref.set(
      {
        title: nextTitle,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    const refreshedSnap = await ref.get();
    const refreshed = (refreshedSnap.data() || {}) as Record<string, unknown>;
    await upsertOwnedMyRequestFromSystem(user.uid, {
      requestId: buildMyRequestDocId("screener", runId),
      type: "screener",
      title: nextTitle,
      input: {
        universe: sanitizeText(refreshed.universe, 40),
        market: sanitizeText(refreshed.market, 20),
        maxNames: asFinite(refreshed.maxNames, 0) || null,
        notes: sanitizeText(refreshed.notes, 1200),
        model: sanitizeText(refreshed.modelUsed || refreshed.model, 120),
        filters: trimOutputsMeta(refreshed.filters),
      },
      outputsMeta: {
        summary: sanitizeText(refreshed.serviceMessage || refreshed.notes, 320),
        resultsCount: Array.isArray(refreshed.results) ? refreshed.results.length : asFinite(refreshed.resultsFound, 0),
        topSymbols: extractScreenerTopSymbols(refreshed.results),
        modelUsed: sanitizeText(refreshed.modelUsed || refreshed.model, 120),
      },
      sourceRef: {
        collection: "screener_runs",
        id: runId,
      },
    });
    res.status(200).json({ ok: true, runId, title: nextTitle });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Screener] update failed", error);
    res.status(500).json({ error: "screener_update_failed" });
  }
});

ROUTES.delete("/screener/:runId", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const runId = sanitizeText(req.params.runId, 220);
    if (!runId) {
      res.status(400).json({ error: "invalid_run_id" });
      return;
    }
    const ref = db.collection("screener_runs").doc(runId);
    const snap = await ref.get();
    if (!snap.exists) {
      res.status(404).json({ error: "screener_not_found" });
      return;
    }
    const data = (snap.data() || {}) as Record<string, unknown>;
    if (sanitizeText(data.userId, 220) !== user.uid) {
      res.status(403).json({ error: "forbidden" });
      return;
    }
    await ref.delete();
    res.status(200).json({ ok: true, deleted: true, runId });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Screener] delete failed", error);
    res.status(500).json({ error: "screener_delete_failed" });
  }
});

ROUTES.post("/stocks/screener", async (req, res) => {
  try {
    const body = asPlainObject(req.body);
    const payload = await runMarketDataScreener({
      preset: body.preset,
      size: body.size,
      query: body.query,
    });
    res.status(200).json(payload);
  } catch (error: any) {
    const detail = sanitizeText(error?.message || error, 260) || "market_data_screener_failed";
    res.status(500).json({ error: "market_data_screener_failed", detail });
  }
});

ROUTES.get("/ticker/trending", async (req, res) => {
  try {
    const query = asPlainObject(req.query);
    const force = asBoolean(query.force, false);
    const regionRaw = sanitizeText(query.region || "US", 12).toUpperCase().replace(/[^A-Z]/g, "");
    const region = regionRaw || "US";
    const limit = Math.max(1, Math.min(36, Math.floor(asFinite(query.limit, 18))));
    const cacheKey = `${region}:${limit}`;

    if (!force) {
      const cached = getTickerTrendingCache(cacheKey);
      if (cached) {
        res.status(200).json({ ...cached, cached: true });
        return;
      }
    }

    const symbols = await fetchYahooTrendingSymbols(region, limit);
    if (!symbols.length) {
      const emptyPayload = {
        region,
        tickers: [],
        items: [],
        source: "yahoo_trending_v1",
        fetchedAt: new Date().toISOString(),
        cached: false,
      };
      setTickerTrendingCache(cacheKey, emptyPayload);
      res.status(200).json(emptyPayload);
      return;
    }

    const [sparkMetaBySymbol, fmpProfileBySymbol] = await Promise.all([
      fetchYahooSparkMetaBySymbols(symbols),
      fetchFmpTickerLogoMap(symbols),
    ]);

    const rows = symbols.map((symbol) => {
      const sparkMeta = sparkMetaBySymbol.get(symbol) || {};
      const fmpProfile = fmpProfileBySymbol.get(symbol);
      const lastClose =
        extractYahooNumber(sparkMeta.regularMarketPrice) ??
        extractYahooNumber(sparkMeta.previousClose) ??
        extractYahooNumber(sparkMeta.chartPreviousClose);
      const previousClose = extractYahooNumber(sparkMeta.previousClose) ?? extractYahooNumber(sparkMeta.chartPreviousClose);
      const directChange =
        extractYahooNumber(sparkMeta.regularMarketChange) ??
        extractYahooNumber(sparkMeta.change);
      const computedChange =
        lastClose !== null && previousClose !== null ? Number((lastClose - previousClose).toFixed(4)) : null;
      const change = directChange !== null ? Number(directChange.toFixed(4)) : computedChange;
      const directChangePct =
        extractYahooNumber(sparkMeta.regularMarketChangePercent) ??
        extractYahooNumber(sparkMeta.percentChange);
      const computedChangePct =
        change !== null && previousClose !== null && Math.abs(previousClose) > 1e-9
          ? Number(((change / Math.abs(previousClose)) * 100).toFixed(4))
          : null;
      const changePct =
        directChangePct !== null
          ? Number(
              (
                Math.abs(directChangePct) < 0.00005 && computedChangePct !== null ? computedChangePct : directChangePct
              ).toFixed(4)
            )
          : computedChangePct;

      const website = sanitizeText(fmpProfile?.website, 280);
      const logoUrl =
        normalizeRemoteLogoUrl(fmpProfile?.logoUrl) || buildLogoUrlFromWebsite(website);
      const companyName =
        sanitizeText(sparkMeta.longName || sparkMeta.shortName, 180) ||
        sanitizeText(fmpProfile?.companyName, 180);

      return {
        symbol,
        ticker: symbol,
        companyName,
        lastClose,
        previousClose,
        change,
        changePct,
        website,
        logoUrl,
        logo_url: logoUrl,
      };
    });

    const payload = {
      region,
      tickers: rows.map((row) => row.symbol),
      items: rows,
      source: "yahoo_trending_v1+fmp_profile",
      fetchedAt: new Date().toISOString(),
      cached: false,
    };

    setTickerTrendingCache(cacheKey, payload);
    res.status(200).json(payload);
  } catch (error: any) {
    const detail = sanitizeText(error?.message || error, 220) || "trending_fetch_failed";
    res.status(500).json({ error: "trending_fetch_failed", detail });
  }
});

ROUTES.get("/ticker/intel", async (req, res) => {
  try {
    const query = asPlainObject(req.query);
    const ticker = normalizeTicker(query.ticker || query.symbol);
    const force = asBoolean(query.force, false);
    if (!ticker) {
      res.status(400).json({ error: "invalid_ticker" });
      return;
    }

    if (!force) {
      const cached = getTickerIntelCache(ticker);
      if (cached) {
        res.status(200).json({
          ...cached,
          cached: true,
        });
        return;
      }
    }

    const modules = [
      "assetProfile",
      "summaryDetail",
      "defaultKeyStatistics",
      "financialData",
      "calendarEvents",
      "recommendationTrend",
    ].join(",");
    const summaryUrl = `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(
      ticker
    )}?modules=${encodeURIComponent(modules)}`;
    const quoteUrl = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(ticker)}`;

    const [summaryPayloadRaw, quotePayloadRaw] = await Promise.all([
      fetchJsonWithTimeout(summaryUrl, 8000).catch(() => null),
      fetchJsonWithTimeout(quoteUrl, 7000).catch(() => null),
    ]);

    const summaryPayload =
      summaryPayloadRaw && typeof summaryPayloadRaw === "object"
        ? (summaryPayloadRaw as Record<string, unknown>)
        : {};
    const quotePayload =
      quotePayloadRaw && typeof quotePayloadRaw === "object"
        ? (quotePayloadRaw as Record<string, unknown>)
        : {};
    const summaryRoot =
      (((summaryPayload.quoteSummary as any)?.result as Array<Record<string, unknown>> | undefined) || [])[0] || {};
    const quoteRow =
      (((quotePayload.quoteResponse as any)?.result as Array<Record<string, unknown>> | undefined) || [])[0] || {};

    if (!Object.keys(summaryRoot).length && !Object.keys(quoteRow).length) {
      const fallbackPayload = await fetchFmpTickerIntelFallback(ticker);
      if (fallbackPayload) {
        setTickerIntelCache(ticker, fallbackPayload);
        res.status(200).json({
          ...fallbackPayload,
          cached: false,
        });
        return;
      }
      res.status(502).json({ error: "ticker_intel_upstream_empty", ticker });
      return;
    }

    const assetProfile =
      summaryRoot.assetProfile && typeof summaryRoot.assetProfile === "object"
        ? (summaryRoot.assetProfile as Record<string, unknown>)
        : {};
    const summaryDetail =
      summaryRoot.summaryDetail && typeof summaryRoot.summaryDetail === "object"
        ? (summaryRoot.summaryDetail as Record<string, unknown>)
        : {};
    const defaultStats =
      summaryRoot.defaultKeyStatistics && typeof summaryRoot.defaultKeyStatistics === "object"
        ? (summaryRoot.defaultKeyStatistics as Record<string, unknown>)
        : {};
    const financialData =
      summaryRoot.financialData && typeof summaryRoot.financialData === "object"
        ? (summaryRoot.financialData as Record<string, unknown>)
        : {};
    const calendarEvents =
      summaryRoot.calendarEvents && typeof summaryRoot.calendarEvents === "object"
        ? (summaryRoot.calendarEvents as Record<string, unknown>)
        : {};
    const recommendationTrendRaw =
      summaryRoot.recommendationTrend && typeof summaryRoot.recommendationTrend === "object"
        ? (summaryRoot.recommendationTrend as Record<string, unknown>)
        : {};

    const website = extractYahooText(assetProfile.website || quoteRow.website, 260);
    const logoUrl = buildLogoUrlFromWebsite(website);
    const marketCap =
      extractYahooNumber(quoteRow.marketCap) ??
      extractYahooNumber(summaryDetail.marketCap) ??
      extractYahooNumber(defaultStats.marketCap) ??
      extractYahooNumber(financialData.marketCap);
    const trailingPe = extractYahooNumber(summaryDetail.trailingPE) ?? extractYahooNumber(quoteRow.trailingPE);
    const forwardPe = extractYahooNumber(summaryDetail.forwardPE) ?? extractYahooNumber(quoteRow.forwardPE);
    const priceToBook = extractYahooNumber(defaultStats.priceToBook) ?? extractYahooNumber(quoteRow.priceToBook);
    const beta = extractYahooNumber(summaryDetail.beta) ?? extractYahooNumber(defaultStats.beta) ?? extractYahooNumber(quoteRow.beta);
    const fiftyTwoWeekLow = extractYahooNumber(summaryDetail.fiftyTwoWeekLow) ?? extractYahooNumber(quoteRow.fiftyTwoWeekLow);
    const fiftyTwoWeekHigh =
      extractYahooNumber(summaryDetail.fiftyTwoWeekHigh) ?? extractYahooNumber(quoteRow.fiftyTwoWeekHigh);
    const avgVolume = extractYahooNumber(summaryDetail.averageVolume) ?? extractYahooNumber(quoteRow.averageDailyVolume3Month);
    const sharesOutstanding = extractYahooNumber(defaultStats.sharesOutstanding) ?? extractYahooNumber(quoteRow.sharesOutstanding);

    const profile = {
      name:
        extractYahooText(quoteRow.shortName || quoteRow.longName, 180) ||
        extractYahooText(assetProfile.longName, 180) ||
        ticker,
      sector: extractYahooText(assetProfile.sector, 120),
      industry: extractYahooText(assetProfile.industry, 120),
      exchange: extractYahooText(quoteRow.fullExchangeName || quoteRow.exchange, 120),
      currency: extractYahooText(quoteRow.currency, 20),
      website,
      summary: extractYahooText(assetProfile.longBusinessSummary, 2000),
      marketCap,
      fiftyTwoWeekLow,
      fiftyTwoWeekHigh,
      trailingPE: trailingPe,
      forwardPE: forwardPe,
      beta,
      dividendYield: extractYahooNumber(summaryDetail.dividendYield),
      logoUrl,
      logo_url: logoUrl,
    };

    const price = {
      last: extractYahooNumber(quoteRow.regularMarketPrice),
      prevClose: extractYahooNumber(quoteRow.regularMarketPreviousClose),
      dayLow: extractYahooNumber(quoteRow.regularMarketDayLow),
      dayHigh: extractYahooNumber(quoteRow.regularMarketDayHigh),
      volume: extractYahooNumber(quoteRow.regularMarketVolume),
      currency: extractYahooText(quoteRow.currency, 20),
    };

    const profileDetails = {
      longName: extractYahooText(quoteRow.longName || quoteRow.shortName, 180) || profile.name,
      sector: profile.sector,
      industry: profile.industry,
      country: extractYahooText(assetProfile.country, 120),
      website,
      longBusinessSummary: profile.summary,
      logoUrl,
      logo_url: logoUrl,
    };

    const valuation = {
      marketCap,
      trailingPE: trailingPe,
      forwardPE: forwardPe,
      priceToBook,
      enterpriseValue: extractYahooNumber(defaultStats.enterpriseValue) ?? extractYahooNumber(quoteRow.enterpriseValue),
    };

    const trading = {
      beta,
      fiftyTwoWeekLow,
      fiftyTwoWeekHigh,
      avgVolume,
      sharesOutstanding,
    };

    const earningsDateRaw = ((calendarEvents.earnings as any)?.earningsDate as Array<unknown> | undefined) || [];
    const earningsDate = earningsDateRaw
      .map((entry) => extractYahooText(entry, 60))
      .filter(Boolean)
      .slice(0, 2)
      .join(" to ");
    const events: Array<{ label: string; value: string }> = [
      { label: "Earnings date", value: earningsDate || "—" },
      { label: "Ex-dividend date", value: extractYahooText(summaryDetail.exDividendDate, 60) || "—" },
      { label: "Dividend rate", value: extractYahooText(summaryDetail.dividendRate, 60) || "—" },
      {
        label: "52-week change",
        value:
          extractYahooNumber(summaryDetail["52WeekChange"]) === null
            ? "—"
            : `${(extractYahooNumber(summaryDetail["52WeekChange"]) as number * 100).toFixed(2)}%`,
      },
    ];

    const analyst = {
      recommendationKey: extractYahooText(financialData.recommendationKey, 80),
      recommendationMean: extractYahooNumber(financialData.recommendationMean),
      analystOpinions: extractYahooNumber(financialData.numberOfAnalystOpinions),
      targetMeanPrice: extractYahooNumber(financialData.targetMeanPrice),
      targetLowPrice: extractYahooNumber(financialData.targetLowPrice),
      targetHighPrice: extractYahooNumber(financialData.targetHighPrice),
    };

    const recommendationTrend = (Array.isArray(recommendationTrendRaw.trend) ? recommendationTrendRaw.trend : [])
      .map((entry) => {
        const row = entry && typeof entry === "object" ? (entry as Record<string, unknown>) : {};
        return {
          period: extractYahooText(row.period, 24),
          strongBuy: extractYahooNumber(row.strongBuy) ?? 0,
          buy: extractYahooNumber(row.buy) ?? 0,
          hold: extractYahooNumber(row.hold) ?? 0,
          sell: extractYahooNumber(row.sell) ?? 0,
          strongSell: extractYahooNumber(row.strongSell) ?? 0,
        };
      })
      .filter((row) => row.period);

    const totalRevenue = extractYahooNumber(financialData.totalRevenue);
    const grossProfits = extractYahooNumber(financialData.grossProfits);
    const grossMargins = extractYahooNumber(financialData.grossMargins);
    const profitMargin = extractYahooNumber(financialData.profitMargins);
    const operatingMargins = extractYahooNumber(financialData.operatingMargins);
    const ebitdaMargins = extractYahooNumber(financialData.ebitdaMargins);
    const roe = extractYahooNumber(financialData.returnOnEquity);
    const roa = extractYahooNumber(financialData.returnOnAssets);
    const totalCash = extractYahooNumber(financialData.totalCash);
    const totalDebt = extractYahooNumber(financialData.totalDebt);
    const currentRatio = extractYahooNumber(financialData.currentRatio);
    const debtToEquity = extractYahooNumber(financialData.debtToEquity);
    const revenueGrowth = extractYahooNumber(financialData.revenueGrowth);

    const liquidityCoverage =
      totalCash !== null && totalDebt !== null && totalDebt > 0 ? totalCash / totalDebt : totalCash !== null ? 1.4 : null;
    const heatmap = [
      {
        label: "Liquidity",
        score: computeSignalScore(liquidityCoverage, 0.25, 2.5),
        hint: "Cash vs debt coverage",
      },
      {
        label: "Leverage",
        score: computeSignalScore(debtToEquity, 30, 220, true),
        hint: "Lower debt-to-equity scores higher",
      },
      {
        label: "Profitability",
        score: computeSignalScore(profitMargin, 0.02, 0.35),
        hint: "Net margin trend quality",
      },
      {
        label: "Growth",
        score: computeSignalScore(revenueGrowth, -0.1, 0.4),
        hint: "Revenue growth profile",
      },
      {
        label: "Valuation",
        score: computeSignalScore(forwardPe, 8, 50, true),
        hint: "Forward P/E relative comfort",
      },
    ];

    const responsePayload: Record<string, unknown> = {
      ticker,
      source: "yahoo_quote_summary",
      fetchedAt: new Date().toISOString(),
      price,
      logoUrl,
      logo_url: logoUrl,
      profile,
      profileDetails,
      valuation,
      fundamentals: {
        revenueTTM: totalRevenue,
        grossMargins,
        profitMargins: profitMargin,
        operatingMargins,
        ebitdaMargins,
        returnOnAssets: roa,
        returnOnEquity: roe,
      },
      risk: {
        beta,
        shortRatio: extractYahooNumber(defaultStats.shortRatio) ?? extractYahooNumber(quoteRow.shortRatio),
      },
      dividends: {
        dividendRate: extractYahooNumber(summaryDetail.dividendRate),
        dividendYield: extractYahooNumber(summaryDetail.dividendYield),
        payoutRatio: extractYahooNumber(summaryDetail.payoutRatio),
        exDividendDate: extractYahooText(summaryDetail.exDividendDate, 60),
      },
      trading,
      events,
      analyst,
      recommendationTrend,
      executiveSummary: {
        ticker,
        exchange: profile.exchange,
        sector: profile.sector,
        priceTarget12m: analyst.targetMeanPrice,
      },
      fundamentalDeepDive: {
        revenueMechanics: {
          totalRevenue,
          grossProfit: grossProfits,
          segmentBreakdown: "Segment-level breakout depends on issuer disclosure quality.",
        },
        profitability: {
          netMargin: profitMargin,
          roi: roe ?? roa,
        },
        capitalAllocation: {
          dividendPolicy:
            extractYahooText(summaryDetail.dividendRate, 120) || "No recurring cash dividend currently reported.",
          shareBuybacks: "Review latest filings for authorization cadence and dilution impact.",
        },
      },
      riskAndEsg: {
        riskMitigation: "Cross-check earnings guidance, leverage, and liquidity before sizing.",
        liquidity: {
          totalCash,
          totalDebt,
          currentRatio,
        },
        esg: {
          environmental: null,
          social: null,
          governance: null,
          overall: null,
        },
      },
      balanceSheetHeatmap: heatmap,
      peerComparison: [],
    };

    setTickerIntelCache(ticker, responsePayload);
    res.status(200).json(responsePayload);
  } catch (error: any) {
    const detail = sanitizeText(error?.message || error, 220) || "ticker_intel_failed";
    res.status(500).json({ error: "ticker_intel_failed", detail });
  }
});

async function handleFxConvert(req: Request, res: Response): Promise<void> {
  let viewer: admin.auth.DecodedIdToken | null = null;
  try {
    viewer = await verifyRequestUser(req, false);
  } catch (error: any) {
    if (String(error?.message) === "invalid_token") {
      res.status(401).json({ error: "invalid_token" });
      return;
    }
  }

  const body = req.method === "GET" ? asPlainObject(req.query) : asPlainObject(req.body);
  const amount = asFinite(body.amount, NaN);
  const base = normalizeFxCode(body.base || body.from, "USD");
  const quote = normalizeFxCode(body.quote || body.to, "USD");
  if (!Number.isFinite(amount) || amount <= 0) {
    res.status(400).json({ error: "invalid_amount", detail: "Amount must be greater than zero." });
    return;
  }
  if (!base || !quote) {
    res.status(400).json({ error: "invalid_currency", detail: "Base and quote currencies are required." });
    return;
  }

  try {
    const resolved = await resolveFxRate(base, quote);
    const amountOut = Number((amount * resolved.rate).toFixed(8));
    const payload = {
      base,
      quote,
      amountIn: amount,
      rate: resolved.rate,
      amountOut,
      asOf: resolved.asOf,
      symbolUsed: resolved.symbolUsed,
      source: resolved.source,
      cachedTtlSeconds: Math.round(FX_RATE_CACHE_TTL_MS / 1000),
    };

    if (viewer?.uid) {
      await db
        .collection("users")
        .doc(viewer.uid)
        .collection("fxHistory")
        .add({
          ...payload,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        });
    }
    res.status(200).json(payload);
  } catch (error: any) {
    const detail = sanitizeText(error?.message || error, 200) || "fx_convert_failed";
    res.status(502).json({ error: "fx_convert_failed", detail });
  }
}

ROUTES.post("/fx/convert", async (req, res) => {
  await handleFxConvert(req, res);
});

ROUTES.get("/fx/convert", async (req, res) => {
  await handleFxConvert(req, res);
});

ROUTES.get("/market-headlines", async (req, res) => {
  const requestedFeedConfig = resolveMarketHeadlineFeedConfig(req.query.provider, req.query.feed);
  const limitRaw = asFinite(req.query.limit, 18);
  const limit = Number.isFinite(limitRaw) ? Math.max(5, Math.min(40, Math.floor(limitRaw))) : 18;
  const warnings: string[] = [];
  let headlines: MarketHeadlineArticle[] = [];
  let effectiveFeedConfig = requestedFeedConfig;
  let detectedTitle = requestedFeedConfig.feedLabel;

  try {
    const xml = await fetchTextWithTimeout(requestedFeedConfig.url, MARKET_HEADLINE_FETCH_TIMEOUT_MS);
    const parsed = parseMarketHeadlineFeedXml(xml, requestedFeedConfig);
    detectedTitle = parsed.feedTitle || detectedTitle;
    headlines = parsed.headlines;
    if (!headlines.length) {
      warnings.push(`${requestedFeedConfig.providerLabel} returned a response but no readable headlines were parsed.`);
    }
  } catch (error) {
    warnings.push(describeMarketHeadlineFetchError(requestedFeedConfig, error));
  }

  const fallbackFeedConfig = MARKET_HEADLINE_FEEDS[MARKET_HEADLINE_DEFAULT_FEED_ID];
  if (
    fallbackFeedConfig &&
    fallbackFeedConfig.id !== requestedFeedConfig.id &&
    shouldFallbackMarketHeadlineFeed(requestedFeedConfig, headlines)
  ) {
    warnings.push(describeMarketHeadlineFallback(requestedFeedConfig, fallbackFeedConfig, headlines));
    try {
      const xml = await fetchTextWithTimeout(fallbackFeedConfig.url, MARKET_HEADLINE_FETCH_TIMEOUT_MS);
      const parsed = parseMarketHeadlineFeedXml(xml, fallbackFeedConfig);
      effectiveFeedConfig = fallbackFeedConfig;
      detectedTitle = parsed.feedTitle || fallbackFeedConfig.feedLabel;
      headlines = parsed.headlines;
      if (!headlines.length) {
        warnings.push(`${fallbackFeedConfig.providerLabel} returned a response but no readable headlines were parsed.`);
      }
    } catch (error) {
      warnings.push(describeMarketHeadlineFetchError(fallbackFeedConfig, error));
    }
  }

  headlines = headlines.slice(0, limit);

  res.status(200).json({
    provider: {
      id: effectiveFeedConfig.providerId,
      label: effectiveFeedConfig.providerLabel,
      sourceUrl: effectiveFeedConfig.sourceUrl,
      directoryUrl: effectiveFeedConfig.directoryUrl || "",
      termsUrl: effectiveFeedConfig.termsUrl || "",
    },
    feed: {
      id: effectiveFeedConfig.id,
      label: effectiveFeedConfig.feedLabel,
      detectedTitle,
      url: effectiveFeedConfig.sourceUrl,
    },
    requestedFeed:
      effectiveFeedConfig.id !== requestedFeedConfig.id
        ? {
            id: requestedFeedConfig.id,
            label: requestedFeedConfig.feedLabel,
            providerId: requestedFeedConfig.providerId,
            providerLabel: requestedFeedConfig.providerLabel,
          }
        : null,
    attribution: {
      note: effectiveFeedConfig.attributionNote || "",
    },
    fetchedAt: new Date().toISOString(),
    count: headlines.length,
    headlines,
    warnings,
  });
});

ROUTES.post("/indicators/analyze", async (req, res) => {
  try {
    const payload = asPlainObject(req.body);
    try {
      payload.userTier = await resolveLlmTierForRequest(req, payload);
    } catch (error: any) {
      if (String(error?.message || "") === "invalid_token") {
        res.status(401).json({ error: "invalid_token" });
        return;
      }
      payload.userTier = normalizeLlmTier(payload.userTier);
    }
    const result = await runIndicatorAnalysis(payload, {
      openAiApiKey: await getOpenAiApiKey(),
      defaultModel: sanitizeText(payload.model, 120) || DEFAULT_LLM_MODEL,
      timeoutMs: LLM_TIMEOUT_MS,
      invokeLlm: async (llmPayload) => {
        const llmResult = await invokeLlmWithFallback({
          provider: llmPayload.provider,
          model: llmPayload.model,
          fallbackProviders: Array.isArray(llmPayload.fallbackProviders) ? llmPayload.fallbackProviders : [],
          userTier: llmPayload.userTier,
          messages: llmPayload.messages,
          params: {
            ...(llmPayload.params || {}),
            jsonSchema: llmPayload.jsonSchema,
          },
        });
        return {
          provider: llmResult.provider,
          model: llmResult.model,
          text: llmResult.text,
          usage: llmResult.usage,
        };
      },
    });
    res.status(200).json({
      ok: true,
      ...result,
    });
  } catch (error: any) {
    const detail = sanitizeText(error?.message || error, 220) || "indicator_analysis_failed";
    const lower = detail.toLowerCase();
    if (lower.includes("ticker is required")) {
      res.status(400).json({ error: "invalid_ticker", detail });
      return;
    }
    if (lower.includes("market data request failed")) {
      res.status(502).json({ error: "market_data_failed", detail });
      return;
    }
    res.status(500).json({ error: "indicator_analysis_failed", detail });
  }
});

function normalizeModelCouncilLanguage(value: unknown): string {
  const raw = sanitizeText(value, 20).toLowerCase();
  return raw || "en";
}

function normalizeModelCouncilModules(value: unknown): string[] {
  const rows = Array.isArray(value) ? value : [];
  return Array.from(new Set(rows.map((item) => sanitizeText(item, 80)).filter(Boolean))).slice(0, 24);
}

function buildModelCouncilQuestion(payload: Record<string, unknown>): string {
  const direct = sanitizeText(payload.question || payload.prompt, 4000);
  if (direct) return direct;
  const messages = normalizeLlmMessages(payload.messages);
  const questions = messages
    .filter((item) => item.role === "user")
    .map((item) => sanitizeText(item.content, 4000))
    .filter(Boolean);
  return questions[questions.length - 1] || "";
}

function buildModelCouncilContext(payload: Record<string, unknown>): Record<string, unknown> {
  return {
    ticker: normalizeTicker(payload.ticker),
    language: normalizeModelCouncilLanguage(payload.language),
    selectedModules: normalizeModelCouncilModules(payload.modules),
    technicalContext: trimOutputsMeta(asPlainObject(payload.technicalContext)),
  };
}

function buildModelCouncilSystemPrompt(payload: Record<string, unknown>): string {
  const context = buildModelCouncilContext(payload);
  return [
    "You are Quantura Model Council, a multi-model equity research copilot.",
    "Use the provided structured ticker context and cite uncertainty clearly.",
    "Return a concise, structured response with thesis, risks, and next steps.",
    `Structured context JSON:\n${JSON.stringify(context)}`,
  ].join("\n\n");
}

async function persistModelCouncilResponse(
  viewer: admin.auth.DecodedIdToken | null,
  payload: Record<string, unknown>,
  result: {
    text: string;
    provider: string;
    model: string;
    usage?: Record<string, unknown>;
    citations?: unknown[];
    latencyMs?: number;
  }
): Promise<string> {
  if (!viewer?.uid) return "";
  const ticker = normalizeTicker(payload.ticker);
  const responseRef = db.collection(MODEL_COUNCIL_RESPONSE_COLLECTION).doc();
  await responseRef.set({
    userId: viewer.uid,
    title: `${ticker || "Ticker"} Model Council`,
    ticker,
    question: buildModelCouncilQuestion(payload),
    answer: sanitizeRichText(result.text, 20000),
    provider: normalizeProvider(result.provider || payload.provider || "openai"),
    model: sanitizeText(result.model || payload.model, 120),
    language: normalizeModelCouncilLanguage(payload.language),
    selectedModules: normalizeModelCouncilModules(payload.modules),
    context: buildModelCouncilContext(payload),
    citations: Array.isArray(result.citations) ? result.citations.slice(0, 24) : [],
    usage: trimOutputsMeta(result.usage),
    latencyMs: Number.isFinite(Number(result.latencyMs)) ? Number(result.latencyMs) : null,
    meta: trimOutputsMeta(asPlainObject(payload.meta)),
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  });
  return responseRef.id;
}

ROUTES.post("/model-council/query", async (req, res) => {
  const startedAt = Date.now();
  try {
    const payload = asPlainObject(req.body);
    const ticker = normalizeTicker(payload.ticker);
    const question = buildModelCouncilQuestion(payload);
    if (!ticker) {
      res.status(400).json({ error: "ticker_required" });
      return;
    }
    if (!question) {
      res.status(400).json({ error: "question_required" });
      return;
    }

    let viewer: admin.auth.DecodedIdToken | null = null;
    try {
      viewer = await verifyRequestUser(req, false);
      payload.userTier = await resolveLlmTierForRequest(req, payload);
    } catch (error: any) {
      if (String(error?.message || "") === "invalid_token") {
        res.status(401).json({ error: "invalid_token" });
        return;
      }
      payload.userTier = normalizeLlmTier(payload.userTier);
    }

    const result = await invokeLlmWithFallback({
      provider: normalizeProvider(payload.provider || "openai"),
      model: sanitizeText(payload.model, 120) || DEFAULT_LLM_MODEL,
      fallbackProviders: [],
      userTier: payload.userTier,
      messages: [
        { role: "system", content: buildModelCouncilSystemPrompt(payload) },
        { role: "user", content: question },
      ],
      params: {
        temperature: 0.2,
        maxTokens: 900,
        webSearch: true,
        background: false,
      },
    });

    const responseId = await persistModelCouncilResponse(viewer, payload, {
      text: result.text,
      provider: result.provider,
      model: result.model,
      usage: result.usage,
      citations: result.citations,
      latencyMs: result.latencyMs,
    });

    res.status(200).json({
      answer: result.text,
      text: result.text,
      model: result.model,
      provider: result.provider,
      usage: result.usage,
      latencyMs: Number.isFinite(Number(result.latencyMs)) ? Number(result.latencyMs) : Date.now() - startedAt,
      context: buildModelCouncilContext(payload),
      moduleData: {},
      selectedModules: normalizeModelCouncilModules(payload.modules),
      responseId,
      citations: Array.isArray(result.citations) ? result.citations : [],
    });
  } catch (error: any) {
    const message = sanitizeText(error?.message || error, 220) || "model_council_query_failed";
    const configMissing = /not configured/i.test(message);
    res.status(502).json({
      error: message,
      detail: configMissing
        ? "Provider secret is missing in runtime. Quantura now resolves model keys from Secret Manager or runtime env bindings."
        : "",
    });
  }
});

ROUTES.post("/model-council/improve-prompt", async (req, res) => {
  try {
    const payload = asPlainObject(req.body);
    const ticker = normalizeTicker(payload.ticker);
    const question = buildModelCouncilQuestion(payload);
    if (!ticker) {
      res.status(400).json({ error: "ticker_required" });
      return;
    }
    if (!question) {
      res.status(400).json({ error: "question_required" });
      return;
    }

    try {
      payload.userTier = await resolveLlmTierForRequest(req, payload);
    } catch (error: any) {
      if (String(error?.message || "") === "invalid_token") {
        res.status(401).json({ error: "invalid_token" });
        return;
      }
      payload.userTier = normalizeLlmTier(payload.userTier);
    }

    const result = await invokeLlmWithFallback({
      provider: normalizeProvider(payload.provider || "openai"),
      model: sanitizeText(payload.model, 120) || DEFAULT_LLM_MODEL,
      fallbackProviders: [],
      userTier: payload.userTier,
      messages: [
        {
          role: "system",
          content: "Rewrite the user prompt for clarity and specificity for financial analysis. Return plain text only. Preserve intent.",
        },
        {
          role: "user",
          content: `Ticker: ${ticker}\nLanguage: ${normalizeModelCouncilLanguage(payload.language)}\nModules: ${normalizeModelCouncilModules(payload.modules).join(", ")}\nPrompt:\n${question}`,
        },
      ],
      params: {
        temperature: 0.1,
        maxTokens: 420,
        webSearch: false,
        background: false,
      },
    });

    res.status(200).json({
      improvedPrompt: sanitizeText(result.text, 4000) || question,
      model: result.model,
      provider: result.provider,
    });
  } catch (error: any) {
    const message = sanitizeText(error?.message || error, 220) || "model_council_improve_prompt_failed";
    const configMissing = /not configured/i.test(message);
    res.status(502).json({
      error: message,
      detail: configMissing
        ? "Provider secret is missing in runtime. Quantura now resolves model keys from Secret Manager or runtime env bindings."
        : "",
    });
  }
});

ROUTES.get("/model-council/providers", async (_req, res) => {
  try {
    const available = await listModelCouncilProviders();
    const all = await listModelCouncilProviders({ includeUnavailable: true });
    console.info(
      "[ModelCouncil] provider availability",
      all.map((item) => `${item.id}:${item.available ? "ready" : "missing"}`).join(", ")
    );
    if (!available.length) {
      res.status(200).json({
        providers: all,
        defaultProvider: "openai",
        warning: "no_provider_secrets_configured",
      });
      return;
    }
    res.status(200).json({
      providers: available,
      defaultProvider: available[0].id,
      fetchedAt: new Date().toISOString(),
    });
  } catch (error: any) {
    const detail = sanitizeText(error?.message || error, 220) || "provider_lookup_failed";
    res.status(500).json({
      providers: [],
      defaultProvider: "openai",
      error: "provider_lookup_failed",
      detail,
    });
  }
});

ROUTES.get("/model-council/models", async (req, res) => {
  try {
    const availableProviders = await listModelCouncilProviders();
    const allProviders = await listModelCouncilProviders({ includeUnavailable: true });
    const requestedProvider = normalizeProvider(req.query.provider || req.query.providerId || "openai");
    const availableSet = new Set(availableProviders.map((item) => item.id));
    const provider: LlmProviderId =
      availableSet.has(requestedProvider) ? requestedProvider : (availableProviders[0]?.id as LlmProviderId) || "openai";
    const models = listModelCouncilModels(provider);
    if (!models.length) {
      res.status(200).json({
        provider,
        providers: availableProviders.length ? availableProviders : allProviders,
        models: [],
        warning: "model_catalog_empty",
      });
      return;
    }
    res.status(200).json({
      provider,
      providers: availableProviders.length ? availableProviders : allProviders,
      models,
      fetchedAt: new Date().toISOString(),
    });
  } catch (error: any) {
    const detail = sanitizeText(error?.message || error, 220) || "model_lookup_failed";
    res.status(500).json({
      provider: "openai",
      providers: [],
      models: [],
      error: "model_lookup_failed",
      detail,
    });
  }
});

async function handleLlmRunRoute(req: Request, res: Response, providerOverride: LlmProviderId | null): Promise<void> {
  const startedAt = Date.now();
  const requestPayload = asPlainObject(req.body);
  if (providerOverride) requestPayload.provider = providerOverride;
  const requestedProvider = normalizeProvider(requestPayload.provider || "openai");
  requestPayload.provider = requestedProvider;
  const fallbackProviders = Array.isArray(requestPayload.fallbackProviders)
    ? requestPayload.fallbackProviders.map((item) => normalizeProvider(item))
    : [];
  const providerChain = Array.from(new Set([requestedProvider, ...fallbackProviders]));
  const retryProvider = providerChain.find((item) => item !== requestedProvider) || "openai";
  const requestedModel = sanitizeText(requestPayload.model, 120) || DEFAULT_LLM_MODEL;
  const params = (requestPayload.params || {}) as Record<string, unknown>;
  const allowWebSearch = asBoolean(params.webSearch ?? params.allowWebSearch ?? requestPayload.webSearch, true);
  const maxTokens = Math.max(64, Math.min(4000, Math.floor(asFinite(params.maxTokens, 900))));
  const background = asBoolean(params.background ?? requestPayload.background, false);
  const wantsStream = Boolean(
    asBoolean(requestPayload.sse, false) ||
      asBoolean(params.sse, false) ||
      sanitizeText(requestPayload.responseMode, 20).toLowerCase() === "sse" ||
      asString(req.headers.accept).toLowerCase().includes("text/event-stream")
  );
  try {
    try {
      requestPayload.userTier = await resolveLlmTierForRequest(req, requestPayload);
    } catch (error: any) {
      if (String(error?.message || "") === "invalid_token") {
        res.status(401).json({ error: "invalid_token" });
        return;
      }
      requestPayload.userTier = normalizeLlmTier(requestPayload.userTier);
    }

    if (wantsStream) {
      if (requestedProvider !== "openai") {
        res.status(400).json({
          error: "streaming_supported_for_openai_only",
          detail: "Use /api/llm/openai for SSE streaming.",
        });
        return;
      }
      const messages = normalizeLlmMessages(requestPayload.messages);
      if (!messages.length) {
        res.status(400).json({ error: "messages_required" });
        return;
      }
      const modelPick = pickModelForProvider(
        "openai",
        requestedModel,
        normalizeLlmTier(requestPayload.userTier || "free")
      );
      await streamOpenAiLlmSse(req, res, {
        model: modelPick.model,
        messages,
        maxTokens,
        allowWebSearch,
        background,
        tools: Array.isArray(params.tools ?? requestPayload.tools)
          ? ((params.tools ?? requestPayload.tools) as unknown[])
          : undefined,
        jsonSchema: params.jsonSchema ?? requestPayload.jsonSchema,
      });
      return;
    }

    const result = await invokeLlmWithFallback(requestPayload);
    res.status(200).json({
      text: result.text,
      model: result.model,
      provider: result.provider,
      latencyMs: Number.isFinite(result.latencyMs) ? result.latencyMs : Date.now() - startedAt,
      usage: result.usage,
      citations: result.citations || [],
      attempted: result.attempted || [],
      modelAdjustedFrom: result.modelAdjustedFrom || "",
      responseId: result.responseId || "",
      status: result.status || "",
      disclaimer: "LLMs can sometimes make mistakes.",
    });
  } catch (error: any) {
    const message = sanitizeText(error?.message || error, 220) || "llm_run_failed";
    const configMissing = /not configured/i.test(message);
    res.status(502).json({
      text: "",
      model: requestedModel,
      provider: requestedProvider,
      latencyMs: Date.now() - startedAt,
      usage: {},
      citations: [],
      error: message,
      detail: configMissing
        ? "Provider secret is missing in runtime. Bind OpenAI/Gemini/Claude/DeepSeek/Mistral/Perplexity/Qwen secrets via Secret Manager."
        : "",
      retryProvider,
      retryModel: requestedModel,
    });
  }
}

ROUTES.post("/llm/run", async (req, res) => {
  await handleLlmRunRoute(req, res, null);
});

ROUTES.post("/llm/:provider", async (req, res) => {
  const provider = normalizeProvider(req.params.provider || "openai");
  await handleLlmRunRoute(req, res, provider);
});

ROUTES.post("/mobile/play-integrity/verify", async (req, res) => {
  try {
    let user: admin.auth.DecodedIdToken | null = null;
    try {
      user = await verifyRequestUser(req, false);
    } catch (error: any) {
      if (String(error?.message) === "invalid_token") {
        res.status(401).json({ error: "invalid_token" });
        return;
      }
      throw error;
    }

    const body = asPlainObject(req.body);
    const integrityToken = sanitizeText(body.integrityToken || body.token, 12000);
    const providedPackage = sanitizeText(body.packageName, 220);
    const packageName = providedPackage || PLAY_INTEGRITY_ANDROID_PACKAGE;
    const expectedNonce = sanitizeText(body.nonce || body.integrityNonce, 320);

    if (!integrityToken) {
      res.status(400).json({ error: "missing_integrity_token" });
      return;
    }
    if (!packageName) {
      res.status(400).json({ error: "missing_package_name" });
      return;
    }

    const verdict = await decodePlayIntegrityToken({
      integrityToken,
      packageName,
    });
    const packageMatches = !PLAY_INTEGRITY_ANDROID_PACKAGE || packageName === PLAY_INTEGRITY_ANDROID_PACKAGE;
    const nonceMatches = !expectedNonce || verdict.nonce === expectedNonce;
    const ok = verdict.ok && packageMatches && nonceMatches;

    await db.collection("mobile_play_integrity_events").add({
      uid: user?.uid || "",
      packageName,
      expectedNonce,
      nonceMatches,
      packageMatches,
      ok,
      verdict: {
        appRecognitionVerdict: verdict.appRecognitionVerdict,
        deviceRecognitionVerdicts: verdict.deviceRecognitionVerdicts,
        licensingVerdict: verdict.licensingVerdict,
        nonce: verdict.nonce,
        timestampMillis: verdict.timestampMillis,
      },
      ipAddress: requestIpAddress(req),
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    if (REQUIRE_PLAY_INTEGRITY && !ok) {
      res.status(403).json({
        error: "play_integrity_failed",
        ok: false,
        packageMatches,
        nonceMatches,
      });
      return;
    }

    res.status(200).json({
      ok,
      packageMatches,
      nonceMatches,
      verdict: {
        appRecognitionVerdict: verdict.appRecognitionVerdict,
        deviceRecognitionVerdicts: verdict.deviceRecognitionVerdicts,
        licensingVerdict: verdict.licensingVerdict,
        nonce: verdict.nonce,
        timestampMillis: verdict.timestampMillis,
      },
    });
  } catch (error: any) {
    console.error("[Mobile] play integrity verify failed", error);
    const detail = sanitizeText(error?.message || error, 220);
    res.status(500).json({ error: "play_integrity_verify_failed", detail });
  }
});

ROUTES.post("/mobile/auth/exchange", async (req, res) => {
  try {
    const nativeIdToken = getBearerToken(req);
    if (!nativeIdToken) {
      res.status(400).json({ error: "missing_bearer_token" });
      return;
    }

    let decoded: admin.auth.DecodedIdToken;
    try {
      decoded = await auth.verifyIdToken(nativeIdToken);
    } catch {
      res.status(401).json({ error: "invalid_token" });
      return;
    }

    const body = asPlainObject(req.body);
    const integrityToken = sanitizeText(body.integrityToken, 12000);
    const expectedNonce = sanitizeText(body.integrityNonce || body.nonce, 320);
    const providedPackage = sanitizeText(body.packageName, 220);
    const packageName = providedPackage || PLAY_INTEGRITY_ANDROID_PACKAGE;

    let integrityChecked = false;
    let integrityOk = true;
    let integrityReason = "not_required";
    let packageMatches = true;
    let nonceMatches = true;
    let integritySummary: Record<string, unknown> = {};

    if (integrityToken) {
      if (!packageName) {
        res.status(400).json({ error: "missing_package_name" });
        return;
      }

      integrityChecked = true;
      const verdict = await decodePlayIntegrityToken({
        integrityToken,
        packageName,
      });
      packageMatches = !PLAY_INTEGRITY_ANDROID_PACKAGE || packageName === PLAY_INTEGRITY_ANDROID_PACKAGE;
      nonceMatches = !expectedNonce || verdict.nonce === expectedNonce;
      integrityOk = verdict.ok && packageMatches && nonceMatches;
      integrityReason = integrityOk ? "passed" : "failed";
      integritySummary = {
        appRecognitionVerdict: verdict.appRecognitionVerdict,
        deviceRecognitionVerdicts: verdict.deviceRecognitionVerdicts,
        licensingVerdict: verdict.licensingVerdict,
        nonce: verdict.nonce,
        timestampMillis: verdict.timestampMillis,
      };
    } else if (REQUIRE_PLAY_INTEGRITY) {
      integrityChecked = true;
      integrityOk = false;
      integrityReason = "missing_integrity_token";
    }

    if (REQUIRE_PLAY_INTEGRITY && !integrityOk) {
      await db.collection("mobile_auth_exchange_events").add({
        uid: decoded.uid,
        status: "blocked",
        reason: integrityReason,
        packageName,
        expectedNonce,
        packageMatches,
        nonceMatches,
        ipAddress: requestIpAddress(req),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      res.status(403).json({
        error: "play_integrity_failed",
        reason: integrityReason,
        packageMatches,
        nonceMatches,
      });
      return;
    }

    const customTokenRaw = await auth.createCustomToken(decoded.uid);
    const customToken = typeof customTokenRaw === "string" ? customTokenRaw : String(customTokenRaw);

    await db.collection("mobile_auth_exchange_events").add({
      uid: decoded.uid,
      status: "issued",
      provider: sanitizeText(decoded.firebase?.sign_in_provider, 80),
      packageName,
      expectedNonce,
      integrityChecked,
      integrityOk,
      integrityReason,
      packageMatches,
      nonceMatches,
      ipAddress: requestIpAddress(req),
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    res.status(200).json({
      customToken,
      uid: decoded.uid,
      integrity: {
        checked: integrityChecked,
        ok: integrityOk,
        reason: integrityReason,
        packageMatches,
        nonceMatches,
        summary: integritySummary,
      },
    });
  } catch (error: any) {
    console.error("[Mobile] auth exchange failed", error);
    const detail = sanitizeText(error?.message || error, 220);
    res.status(500).json({ error: "auth_exchange_failed", detail });
  }
});

ROUTES.post("/analytics/ad-impression", async (req, res) => {
  try {
    let user: admin.auth.DecodedIdToken | null = null;
    try {
      user = await verifyRequestUser(req, false);
    } catch (error: any) {
      if (String(error?.message) === "invalid_token") {
        res.status(401).json({ error: "invalid_token" });
        return;
      }
      throw error;
    }

    const body = asPlainObject(req.body);
    const adFormat = normalizeAdFormat(body.adFormat);
    const adUnitId = sanitizeText(body.adUnitId || body.adUnitName, 220);
    const adPlatform = sanitizeText(body.adPlatform || "admob", 60).toLowerCase();
    const adSource = sanitizeText(body.adSource || "admob", 120);
    const placement = sanitizeText(body.placement || body.source || "", 120);
    const impressionId = sanitizeText(body.impressionId, 220);
    const rewardType = sanitizeText(body.rewardType, 120);
    const rewardAmount = Number.isFinite(Number(body.rewardAmount)) ? Number(body.rewardAmount) : null;
    const currency = normalizeCurrency(body.currency);
    const value = Number.isFinite(Number(body.value)) ? Number(body.value) : null;
    const platform = sanitizeText(body.platform, 30).toLowerCase() || "unknown";

    if (!adFormat || adFormat === "unknown") {
      res.status(400).json({ error: "invalid_ad_format" });
      return;
    }

    const docId = impressionId ? sanitizeText(impressionId, 180) : "";
    const collection = db.collection("ad_impressions");
    const docRef = docId ? collection.doc(docId) : collection.doc();
    await docRef.set(
      {
        uid: user?.uid || sanitizeText(body.uid, 220),
        adFormat,
        adUnitId,
        adPlatform,
        adSource,
        placement,
        platform,
        rewardType,
        rewardAmount,
        currency,
        value,
        impressionId: docId || docRef.id,
        deviceId: sanitizeText(body.deviceId, 220),
        appVersion: sanitizeText(body.appVersion, 80),
        ipAddress: requestIpAddress(req),
        userAgent: sanitizeText(req.headers["user-agent"], 300),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    res.status(200).json({ ok: true, id: docRef.id });
  } catch (error) {
    console.error("[Ads] ad impression callback failed", error);
    res.status(500).json({ error: "ad_impression_failed" });
  }
});

ROUTES.post("/notify/sendTest", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const body = asPlainObject(req.body);
    const explicitTokens = Array.isArray(body.tokens)
      ? body.tokens.map((item) => sanitizeText(item, 4096)).filter(Boolean)
      : [];
    const singleToken = sanitizeText(body.token, 4096);
    if (singleToken) explicitTokens.push(singleToken);

    let tokens = Array.from(new Set(explicitTokens));
    if (!tokens.length) {
      const tokenSnap = await db.collection("users").doc(user.uid).collection("fcmTokens").limit(100).get();
      tokens = tokenSnap.docs.map((doc) => sanitizeText(doc.id, 4096)).filter(Boolean);
    }

    if (!tokens.length) {
      res.status(404).json({ error: "no_tokens_available" });
      return;
    }

    const title = sanitizeText(body.title, 120) || "Quantura test notification";
    const message = sanitizeText(body.message, 240) || "Push delivery check from Quantura.";
    const targetPath = sanitizeText(body.path || "/notifications", 280);

    const result = await messaging.sendEachForMulticast({
      tokens,
      notification: {
        title,
        body: message,
      },
      data: {
        type: "test",
        path: targetPath,
      },
    });

    res.status(200).json({
      ok: true,
      requested: tokens.length,
      successCount: result.successCount,
      failureCount: result.failureCount,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] sendTest failed", error);
    res.status(500).json({ error: "notify_send_test_failed" });
  }
});

ROUTES.post("/earnings/refresh", async (req, res) => {
  const requestId = `earnings_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
  try {
    const hasFmpKey = Boolean(FMP_API_KEY);
    console.info("[Earnings] env_check", { requestId, hasFmpKey });
    if (!FMP_API_KEY) {
      res.status(503).json({
        error: "missing_fmp_api_key",
        detail: "FMP API key is not configured in function runtime. Bind FMP_API_KEY (or FMP_*_KEY) via Secret Manager.",
        requestId,
      });
      return;
    }

    const body = asPlainObject(req.body);
    const symbol = normalizeTicker(body.symbol || body.ticker);
    const datePattern = /^\d{4}-\d{2}-\d{2}$/;
    const fallbackStart = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const fallbackEnd = new Date(Date.now() + 120 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const startRaw = sanitizeText(body.start || body.from, 20);
    const endRaw = sanitizeText(body.end || body.to, 20);
    const start = datePattern.test(startRaw) ? startRaw : fallbackStart;
    const end = datePattern.test(endRaw) ? endRaw : fallbackEnd;
    console.info("[Earnings] fetch_start", { requestId, start, end, symbol: symbol || null });

    if (start > end) {
      res.status(400).json({ error: "invalid_range", requestId });
      return;
    }

    const toNullableNumber = (value: unknown): number | null => {
      if (value === null || value === undefined) return null;
      const text = sanitizeText(value, 40);
      if (!text || text.toLowerCase() === "null" || text === "—") return null;
      const parsed = Number(text.replace(/,/g, ""));
      return Number.isFinite(parsed) ? parsed : null;
    };

    const normalizeDate = (value: unknown): string => {
      const text = sanitizeText(value, 30);
      if (!text) return "";
      if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
      const parsed = Date.parse(text);
      if (!Number.isFinite(parsed)) return "";
      return new Date(parsed).toISOString().slice(0, 10);
    };

    const normalizeItem = (raw: Record<string, unknown>, symbolHint = "") => {
      const rowSymbol = normalizeTicker(raw.symbol || raw.ticker || symbolHint);
      const date = normalizeDate(raw.date || (raw as any).reportDate);
      if (!rowSymbol || !date) return null;
      return {
        symbol: rowSymbol,
        company: sanitizeText((raw as any).name || (raw as any).company || (raw as any).companyName, 180) || rowSymbol,
        date,
        epsEstimated: toNullableNumber((raw as any).epsEstimated ?? (raw as any).epsEstimate ?? (raw as any).estimate),
        epsActual: toNullableNumber((raw as any).epsActual ?? (raw as any).eps),
        revenueEstimated: toNullableNumber((raw as any).revenueEstimated),
        revenueActual: toNullableNumber((raw as any).revenueActual),
        lastUpdated: normalizeDate((raw as any).lastUpdated || (raw as any).updatedFromDate),
      };
    };
    type NormalizedEarningsItem = NonNullable<ReturnType<typeof normalizeItem>>;

    const buildDays = (from: string, to: string, normalizedItems: Array<Record<string, unknown>>) => {
      const startDate = new Date(`${from}T00:00:00Z`);
      const endDate = new Date(`${to}T00:00:00Z`);
      const byDate = new Map<string, Array<Record<string, unknown>>>();
      normalizedItems.forEach((item) => {
        const date = sanitizeText((item as any).date, 20);
        if (!date) return;
        if (!byDate.has(date)) byDate.set(date, []);
        byDate.get(date)!.push(item);
      });
      const out: Array<{ date: string; count: number; items: Array<Record<string, unknown>> }> = [];
      for (let cursor = startDate.getTime(); cursor <= endDate.getTime(); cursor += 24 * 60 * 60 * 1000) {
        const date = new Date(cursor).toISOString().slice(0, 10);
        const rows = byDate.get(date) || [];
        rows.sort((a, b) =>
          String((a as any).symbol || "").localeCompare(String((b as any).symbol || ""))
        );
        out.push({ date, count: rows.length, items: rows });
      }
      return out;
    };

    const extractRows = (payload: unknown): Array<Record<string, unknown>> => {
      if (Array.isArray(payload)) {
        return payload.filter((row) => row && typeof row === "object") as Array<Record<string, unknown>>;
      }
      if (!payload || typeof payload !== "object") return [];
      const maybeObject = payload as Record<string, unknown>;
      const candidates = [maybeObject.earningsCalendar, maybeObject.data, maybeObject.items];
      for (const candidate of candidates) {
        if (Array.isArray(candidate)) {
          return candidate.filter((row) => row && typeof row === "object") as Array<Record<string, unknown>>;
        }
      }
      return [];
    };

    const cacheDocId = symbol ? `symbol_${symbol}_${start}_${end}` : `range_${start}_${end}`;
    const docRef = db.collection("earningsCalendar").doc(cacheDocId);
    const existingSnap = await docRef.get();
    const existing = (existingSnap.data() || {}) as Record<string, unknown>;
    const lastFetchedAtMs = getTimestampMs(existing.lastFetchedAt);
    const hasRecentCache = Array.isArray(existing.items) && Date.now() - lastFetchedAtMs < 7 * 24 * 60 * 60 * 1000;
    if (hasRecentCache) {
      const normalizedCachedItems = (Array.isArray(existing.items) ? existing.items : [])
        .map((item) => normalizeItem(asPlainObject(item), symbol))
        .filter(Boolean) as Array<Record<string, unknown>>;
      const days = buildDays(start, end, normalizedCachedItems);
      const lastUpdated =
        normalizedCachedItems
          .map((item) => sanitizeText((item as any).lastUpdated, 20))
          .filter(Boolean)
          .sort()
          .pop() || sanitizeText(existing.lastUpdated, 20);
      console.info("[Earnings] cache_hit", {
        requestId,
        cacheDocId,
        itemCount: normalizedCachedItems.length,
        dayCount: days.length,
      });
      res.status(200).json({
        ok: true,
        symbol,
        cacheDocId,
        cached: true,
        start,
        end,
        range: { from: start, to: end },
        lastFetchedAtMs,
        items: normalizedCachedItems,
        days,
        lastUpdated,
        requestId,
      });
      return;
    }

    const stableUrl = (() => {
      const base = `https://financialmodelingprep.com/stable/earnings-calendar?from=${encodeURIComponent(start)}&to=${encodeURIComponent(
        end
      )}&apikey=${encodeURIComponent(FMP_API_KEY)}`;
      if (!symbol) return base;
      return `${base}&symbol=${encodeURIComponent(symbol)}`;
    })();
    const candidateUrls: Array<{ label: string; url: string }> = [
      { label: "stable", url: stableUrl },
      {
        label: "v3",
        url: `https://financialmodelingprep.com/api/v3/earning_calendar?from=${encodeURIComponent(start)}&to=${encodeURIComponent(end)}&apikey=${encodeURIComponent(FMP_API_KEY)}`,
      },
    ];

    let records: Array<Record<string, unknown>> = [];
    let fetchedFrom = "";
    const fetchDiagnostics: string[] = [];

    for (const candidate of candidateUrls) {
      const startedAt = Date.now();
      try {
        console.info("[Earnings] provider_attempt", { requestId, source: candidate.label });
        const response = await fetch(candidate.url, {
          method: "GET",
          headers: {
            Accept: "application/json",
          },
        });
        if (!response.ok) {
          fetchDiagnostics.push(`${candidate.label}:status_${response.status}`);
          console.warn("[Earnings] provider_status", {
            requestId,
            source: candidate.label,
            status: response.status,
          });
          continue;
        }
        const payload = (await response.json().catch(() => null)) as unknown;
        const allRows = extractRows(payload);
        fetchDiagnostics.push(`${candidate.label}:ok:${allRows.length}`);
        const filtered = symbol
          ? allRows.filter((row) => normalizeTicker((row as any).symbol || (row as any).ticker) === symbol)
          : allRows;
        records = filtered;
        fetchedFrom = candidate.label;
        console.info("[Earnings] provider_success", {
          requestId,
          source: candidate.label,
          rowCount: filtered.length,
          durationMs: Date.now() - startedAt,
        });
        if (records.length || candidate === candidateUrls[candidateUrls.length - 1]) break;
      } catch (error: any) {
        const message = sanitizeText(error?.message, 120) || "fetch_failed";
        fetchDiagnostics.push(`${candidate.label}:error:${message}`);
        console.warn("[Earnings] provider_error", {
          requestId,
          source: candidate.label,
          error: message,
        });
      }
    }

    const items = records
      .map((row) => normalizeItem(row, symbol))
      .filter((item): item is NormalizedEarningsItem => item !== null)
      .sort((a, b) => {
        const dateA = sanitizeText(a.date, 20);
        const dateB = sanitizeText(b.date, 20);
        if (dateA === dateB) {
          return sanitizeText(a.symbol, 24).localeCompare(sanitizeText(b.symbol, 24));
        }
        return dateA.localeCompare(dateB);
      })
      .slice(0, symbol ? 500 : 7000);

    const successfulFetch = fetchDiagnostics.some((entry) => entry.includes(":ok:"));
    if (!items.length && !successfulFetch) {
      const staleItems = (Array.isArray(existing.items) ? existing.items : [])
        .map((item) => normalizeItem(asPlainObject(item), symbol))
        .filter(Boolean) as Array<Record<string, unknown>>;
      if (staleItems.length) {
        const staleDays = buildDays(start, end, staleItems);
        console.warn("[Earnings] provider_failed_using_stale_cache", {
          requestId,
          cacheDocId,
          staleItemCount: staleItems.length,
          diagnostics: fetchDiagnostics.slice(0, 4),
        });
        res.status(200).json({
          ok: true,
          symbol,
          cacheDocId,
          cached: true,
          stale: true,
          warning: "provider_unavailable_showing_cached_data",
          range: { from: start, to: end },
          start,
          end,
          lastFetchedAtMs,
          fetchedCount: staleItems.length,
          items: staleItems,
          days: staleDays,
          lastUpdated: sanitizeText(existing.lastUpdated, 20),
          diagnostics: fetchDiagnostics.slice(0, 4),
          requestId,
        });
        return;
      }
      console.error("[Earnings] provider_failed_no_data", {
        requestId,
        diagnostics: fetchDiagnostics.slice(0, 4),
      });
      res.status(502).json({
        error: "earnings_provider_unavailable",
        detail: "Unable to load earnings data from provider.",
        diagnostics: fetchDiagnostics.slice(0, 4),
        requestId,
      });
      return;
    }

    const days = buildDays(start, end, items as Array<Record<string, unknown>>);

    const lastUpdated = items
      .map((item) => sanitizeText((item as any).lastUpdated, 20))
      .filter(Boolean)
      .sort()
      .pop() || "";

    await docRef.set(
      {
        cacheDocId,
        symbol: symbol || null,
        start,
        end,
        source: "fmp",
        sourceVariant: fetchedFrom || "none",
        lastUpdated,
        itemCount: items.length,
        range: { from: start, to: end },
        diagnostics: fetchDiagnostics.slice(0, 8),
        lastFetchedAt: admin.firestore.FieldValue.serverTimestamp(),
        items,
      },
      { merge: true }
    );

    console.info("[Earnings] fetch_complete", {
      requestId,
      cacheDocId,
      fetchedFrom: fetchedFrom || "none",
      itemCount: items.length,
      dayCount: days.length,
    });

    res.status(200).json({
      ok: true,
      symbol,
      cacheDocId,
      start,
      end,
      range: { from: start, to: end },
      cached: false,
      fetchedCount: items.length,
      lastUpdated,
      lastFetchedAtMs: Date.now(),
      items,
      days,
      diagnostics: fetchDiagnostics.slice(0, 4),
      requestId,
    });
  } catch (error: any) {
    console.error("[Earnings] refresh_failed", { requestId, error: sanitizeText(error?.message, 220) || "unknown" });
    res.status(500).json({ error: "earnings_refresh_failed", requestId });
  }
});

ROUTES.post("/polymarket/search", async (req, res) => {
  try {
    const body = asPlainObject(req.body);
    const query = sanitizeText(body.q, 120);
    if (!query) {
      res.status(400).json({ error: "query_required" });
      return;
    }
    const limitPerType = Math.max(1, Math.min(60, Math.floor(asFinite(body.limitPerType, 20))));
    const includeClosed = asBoolean(body.includeClosed, false);
    const sort = normalizePolymarketSort(body.sort);
    const cacheKey = `search::${query.toLowerCase()}::${limitPerType}::${includeClosed ? 1 : 0}::${sort}`;
    const cached = getPolymarketCache(cacheKey);
    if (cached) {
      res.status(200).json(cached);
      return;
    }

    const payload = await fetchGammaJson("/public-search", {
      q: query,
      limit_per_type: limitPerType,
      keep_closed_markets: includeClosed ? 1 : 0,
    });
    const normalized = normalizePolymarketResponse(payload, {
      query,
      sort,
      includeClosed,
    });
    setPolymarketCache(cacheKey, normalized);
    res.status(200).json(normalized);
  } catch (error: any) {
    console.error("[Polymarket] search failed", error);
    res.status(502).json({
      error: "polymarket_search_failed",
      detail: sanitizeText(error?.message, 180) || "Unable to fetch Polymarket markets.",
    });
  }
});

ROUTES.post("/polymarket/active", async (req, res) => {
  try {
    const body = asPlainObject(req.body);
    const limit = Math.max(1, Math.min(60, Math.floor(asFinite(body.limit, 24))));
    const offset = Math.max(0, Math.min(2000, Math.floor(asFinite(body.offset, 0))));
    const sort = normalizePolymarketSort(body.sort || "volume");
    const cacheKey = `active::${limit}::${offset}::${sort}`;
    const cached = getPolymarketCache(cacheKey);
    if (cached) {
      res.status(200).json(cached);
      return;
    }

    const payload = await fetchGammaJson("/events", {
      active: true,
      closed: false,
      limit,
      offset,
    });
    const normalized = normalizePolymarketResponse(payload, {
      query: "top-active",
      sort,
      includeClosed: false,
    });
    setPolymarketCache(cacheKey, normalized);
    res.status(200).json(normalized);
  } catch (error: any) {
    console.error("[Polymarket] active failed", error);
    res.status(502).json({
      error: "polymarket_active_failed",
      detail: sanitizeText(error?.message, 180) || "Unable to fetch active markets.",
    });
  }
});

ROUTES.post("/polymarket/price", async (req, res) => {
  try {
    const body = asPlainObject(req.body);
    const query = sanitizeText(body.q || body.query, 120);
    const includeClosed = asBoolean(body.includeClosed, false);
    const sort = normalizePolymarketSort(body.sort || "volume");
    const marketId = sanitizeText(body.marketId, 120);
    const slug = sanitizeText(body.slug, 220);
    const eventId = sanitizeText(body.eventId, 120);
    const limit = Math.max(1, Math.min(80, Math.floor(asFinite(body.limit, 20))));
    const limitPerType = Math.max(1, Math.min(60, Math.floor(asFinite(body.limitPerType, 20))));

    let normalized: PolymarketSearchResponse;
    if (query) {
      const cacheKey = `price::search::${query.toLowerCase()}::${includeClosed ? 1 : 0}::${sort}::${limitPerType}`;
      const cached = getPolymarketCache(cacheKey);
      if (cached) {
        normalized = cached;
      } else {
        const payload = await fetchGammaJson("/public-search", {
          q: query,
          limit_per_type: limitPerType,
          keep_closed_markets: includeClosed ? 1 : 0,
        });
        normalized = normalizePolymarketResponse(payload, {
          query,
          sort,
          includeClosed,
        });
        setPolymarketCache(cacheKey, normalized);
      }
    } else {
      const offset = Math.max(0, Math.min(2000, Math.floor(asFinite(body.offset, 0))));
      const activeLimit = Math.max(limit, 24);
      const cacheKey = `price::active::${activeLimit}::${offset}::${sort}`;
      const cached = getPolymarketCache(cacheKey);
      if (cached) {
        normalized = cached;
      } else {
        const payload = await fetchGammaJson("/events", {
          active: true,
          closed: false,
          limit: activeLimit,
          offset,
        });
        normalized = normalizePolymarketResponse(payload, {
          query: "top-active",
          sort,
          includeClosed: false,
        });
        setPolymarketCache(cacheKey, normalized);
      }
    }

    const markets = flattenPolymarketMarkets(normalized, {
      marketId,
      slug,
      eventId,
    }).slice(0, limit);

    res.status(200).json({
      query: normalized.query || query,
      fetchedAt: normalized.fetchedAt,
      count: markets.length,
      markets,
    });
  } catch (error: any) {
    console.error("[Polymarket] price failed", error);
    res.status(502).json({
      error: "polymarket_price_failed",
      detail: sanitizeText(error?.message, 180) || "Unable to fetch Polymarket implied prices.",
    });
  }
});

ROUTES.post("/webhooks/inapppurchasesios", async (req, res) => {
  try {
    if (!checkWebhookSecret(req, IOS_IAP_WEBHOOK_SECRET)) {
      res.status(401).json({ error: "invalid_webhook_secret" });
      return;
    }
    await db.collection("webhook_ios_iap").add(summarizeWebhookPayload(req));
    res.status(200).json({ ok: true });
  } catch (error) {
    console.error("[Webhook] inapppurchasesios failed", error);
    res.status(500).json({ error: "webhook_store_failed" });
  }
});

ROUTES.post("/webhooks/applenotifications", async (req, res) => {
  try {
    if (!checkWebhookSecret(req, APPLE_NOTIFICATIONS_WEBHOOK_SECRET)) {
      res.status(401).json({ error: "invalid_webhook_secret" });
      return;
    }
    await db.collection("webhook_apple_notifications").add(summarizeWebhookPayload(req));
    res.status(200).json({ ok: true });
  } catch (error) {
    console.error("[Webhook] applenotifications failed", error);
    res.status(500).json({ error: "webhook_store_failed" });
  }
});

async function handleAdmobRewardWebhook(req: Request, res: Response): Promise<void> {
  try {
    const query = asPlainObject(req.query);
    const body = asPlainObject(req.body);
    const secretValid = checkWebhookSecret(req, ADMOB_SSV_WEBHOOK_SECRET);
    const rewardAmount = asFinite(query.reward_amount || body.reward_amount, NaN);
    const rewardType = sanitizeText(query.reward_type || body.reward_type || query.reward_item || body.reward_item, 120);
    const adUnit = sanitizeText(query.ad_unit || body.ad_unit, 220);
    const userId = sanitizeText(query.user_id || body.user_id, 220);
    const customData = sanitizeText(query.custom_data || body.custom_data, 1600);

    await db.collection("webhook_admob_ssv").add({
      ...summarizeWebhookPayload(req),
      callbackAccepted: secretValid,
      callbackMethod: sanitizeText(req.method, 12),
      callbackPath: sanitizeText(req.path, 180),
      rewardAmount: Number.isFinite(rewardAmount) ? rewardAmount : null,
      rewardType,
      rewardItem: sanitizeText(query.reward_item || body.reward_item, 120),
      adUnit,
      userId,
      customData,
      transactionId: sanitizeText(query.transaction_id || body.transaction_id, 220),
      adNetwork: sanitizeText(query.ad_network || body.ad_network, 120),
      timestamp: sanitizeText(query.timestamp || body.timestamp, 60),
      signature: sanitizeText(query.signature || body.signature, 900),
      keyId: sanitizeText(query.key_id || body.key_id, 120),
      mediationGroupName: sanitizeText(query.mediation_group_name || body.mediation_group_name, 220),
      mediationAbTestName: sanitizeText(query.mediation_ab_test_name || body.mediation_ab_test_name, 220),
      mediationAbTestVariant: sanitizeText(query.mediation_ab_test_variant || body.mediation_ab_test_variant, 120),
      adSourceId: sanitizeText(query.ad_source_id || body.ad_source_id, 120),
      adSourceInstanceId: sanitizeText(query.ad_source_instance_id || body.ad_source_instance_id, 180),
      rawQuery: query,
    });

    // Always ACK 200 to prevent repeated retries from AdMob SSV callback delivery.
    res.status(200).send("ok");
  } catch (error) {
    console.error("[Webhook] admob reward failed", error);
    res.status(200).send("ok");
  }
}

ROUTES.get("/webhooks/admob/reward", async (req, res) => {
  await handleAdmobRewardWebhook(req, res);
});

ROUTES.post("/webhooks/admob/reward", async (req, res) => {
  await handleAdmobRewardWebhook(req, res);
});

ROUTES.get("/webhook/admob/reward", async (req, res) => {
  await handleAdmobRewardWebhook(req, res);
});

ROUTES.post("/webhook/admob/reward", async (req, res) => {
  await handleAdmobRewardWebhook(req, res);
});

ROUTES.get("/admob/reward", async (req, res) => {
  await handleAdmobRewardWebhook(req, res);
});

ROUTES.post("/admob/reward", async (req, res) => {
  await handleAdmobRewardWebhook(req, res);
});

ROUTES.get("/explore/suggestions", async (req, res) => {
  try {
    const query = sanitizeText(req.query.query, 32).toUpperCase();
    const viewer = await verifyRequestUser(req, false).catch(() => null);

    const popSnap = await db
      .collection("posts")
      .where("visibility", "==", "public")
      .orderBy("createdAt", "desc")
      .limit(120)
      .get();

    const counts = new Map<string, number>();
    popSnap.docs.forEach((doc) => {
      const tickers = Array.isArray(doc.data().tickers) ? (doc.data().tickers as string[]) : [];
      tickers.forEach((ticker) => {
        const clean = normalizeTicker(ticker);
        if (!clean) return;
        counts.set(clean, (counts.get(clean) || 0) + 1);
      });
    });

    if (viewer?.uid) {
      const watchSnap = await db.collection("users").doc(viewer.uid).collection("watchTickers").limit(100).get();
      watchSnap.docs.forEach((doc) => {
        const clean = normalizeTicker(doc.id || doc.data().ticker);
        if (!clean) return;
        counts.set(clean, (counts.get(clean) || 0) + 20);
      });
    }

    const suggestions = Array.from(counts.entries())
      .filter(([ticker]) => (query ? ticker.startsWith(query) : true))
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 15)
      .map(([ticker]) => ticker);

    res.status(200).json({ suggestions });
  } catch (error) {
    console.error("[Explore] suggestions failed", error);
    res.status(500).json({ error: "suggestions_failed" });
  }
});

ROUTES.get("/explore", async (req, res) => {
  try {
    const modeRaw = sanitizeText(req.query.mode, 24).toLowerCase();
    const mode: "trending" | "latest" | "following" | "tickers" =
      modeRaw === "latest" || modeRaw === "following" || modeRaw === "tickers" ? (modeRaw as any) : "trending";

    const limit = parseLimit(req.query.limit);
    const cursor = decodeCursor(req.query.cursor);
    const tickerFilter = normalizeTicker(req.query.ticker);
    const queryText = sanitizeText(req.query.q, 80);

    const viewer = await verifyRequestUser(req, false).catch((err) => {
      if (String(err?.message) === "invalid_token") {
        throw new Error("invalid_token");
      }
      return null;
    });

    if (mode === "following" && !viewer?.uid) {
      res.status(401).json({ error: "auth_required_for_following" });
      return;
    }

    let docs: admin.firestore.QueryDocumentSnapshot[] = [];
    let nextCursor: string | null = null;

    if (mode === "following" && viewer?.uid) {
      const followingPage = await listFollowingPosts(viewer.uid, limit, cursor, tickerFilter, queryText);
      docs = followingPage.posts;
      nextCursor = followingPage.nextCursor;
    } else {
      const searchMode = queryText.startsWith("#")
        ? "tag"
        : queryText.startsWith("@")
        ? "author"
        : /^[A-Za-z.\-]{1,12}$/.test(queryText)
        ? "ticker"
        : "none";

      let queryRef: admin.firestore.Query = db.collection("posts").where("visibility", "==", "public");

      if (mode === "tickers") {
        if (tickerFilter) {
          queryRef = queryRef.where("tickers", "array-contains", tickerFilter);
        }
      } else if (searchMode === "ticker") {
        queryRef = queryRef.where("tickers", "array-contains", normalizeTicker(queryText));
      } else if (searchMode === "tag") {
        const tag = normalizeHandle(queryText.slice(1));
        if (tag) queryRef = queryRef.where("tags", "array-contains", tag);
      } else if (searchMode === "author") {
        const handle = normalizeHandle(queryText.slice(1));
        if (handle) queryRef = queryRef.where("authorHandle", "==", handle);
      } else if (tickerFilter) {
        queryRef = queryRef.where("tickers", "array-contains", tickerFilter);
      }

      const usingTrending = mode === "trending";
      if (usingTrending) {
        queryRef = queryRef
          .orderBy("score", "desc")
          .orderBy("createdAt", "desc")
          .orderBy(admin.firestore.FieldPath.documentId(), "desc");

        if (cursor) {
          queryRef = queryRef.startAfter(
            asFinite(cursor.score, 0),
            timestampFromMs(cursor.createdAtMs),
            cursor.id
          );
        }
      } else {
        queryRef = queryRef
          .orderBy("createdAt", "desc")
          .orderBy(admin.firestore.FieldPath.documentId(), "desc");

        if (cursor) {
          queryRef = queryRef.startAfter(timestampFromMs(cursor.createdAtMs), cursor.id);
        }
      }

      const snap = await queryRef.limit(limit + 1).get();
      const pageDocs = snap.docs.slice(0, limit);
      const hasMore = snap.docs.length > limit;
      docs = pageDocs;

      if (hasMore && pageDocs.length) {
        const last = pageDocs[pageDocs.length - 1];
        const postData = toPostResponse(last);
        nextCursor = encodeCursor(buildNextCursor(postData, usingTrending));
      }
    }

    const visibleDocs = docs.filter((doc) => isPostVisibleToViewer(doc.data() as Record<string, unknown>, viewer?.uid || null));
    const postIds = visibleDocs.map((doc) => doc.id);
    const engagement = await fetchViewerEngagement(postIds, viewer?.uid || null);

    const posts = visibleDocs.map((doc) => {
      const viewerState = engagement.get(doc.id) || { liked: false, reposted: false, saved: false };
      return toPostResponse(doc, viewerState);
    });

    res.status(200).json({
      mode,
      count: posts.length,
      cursor: nextCursor,
      posts,
    });
  } catch (error: any) {
    if (String(error?.message) === "invalid_token") {
      res.status(401).json({ error: "invalid_token" });
      return;
    }
    console.error("[Explore] list failed", error);
    res.status(500).json({ error: "explore_fetch_failed" });
  }
});

ROUTES.get("/posts/:postId", async (req, res) => {
  try {
    const postId = sanitizeText(req.params.postId, 180);
    if (!postId) {
      res.status(400).json({ error: "invalid_post_id" });
      return;
    }

    const viewer = await verifyRequestUser(req, false).catch(() => null);
    const postSnap = await db.collection("posts").doc(postId).get();
    if (!postSnap.exists) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const data = (postSnap.data() || {}) as Record<string, unknown>;
    if (!isPostVisibleToViewer(data, viewer?.uid || null)) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    const [commentsSnap, engagement] = await Promise.all([
      db.collection("posts").doc(postId).collection("comments").orderBy("createdAt", "desc").limit(100).get(),
      fetchViewerEngagement([postId], viewer?.uid || null),
    ]);

    const comments = commentsSnap.docs.map((commentDoc) => {
      const comment = commentDoc.data() || {};
      const createdAtMs = getTimestampMs(comment.createdAt);
      return {
        id: commentDoc.id,
        authorUid: asString(comment.authorUid),
        authorHandle: asString(comment.authorHandle),
        text: asString(comment.text),
        createdAt: new Date(createdAtMs).toISOString(),
        createdAtMs,
      };
    });

    const viewerState = engagement.get(postId) || { liked: false, reposted: false, saved: false };
    const postResponse = toPostResponse(postSnap, viewerState);
    const resolvedBody = await resolvePostBody(postId, data);
    if (resolvedBody.body) {
      postResponse.body = resolvedBody.body;
      postResponse.bodyFormat = resolvedBody.bodyFormat;
    }
    postResponse.hasBody = resolvedBody.hasBody;

    res.status(200).json({
      post: postResponse,
      comments,
    });
  } catch (error) {
    console.error("[Explore] detail failed", error);
    res.status(500).json({ error: "post_detail_failed" });
  }
});

ROUTES.post("/posts/:postId/like", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    if (!user || !postId) {
      res.status(400).json({ error: "invalid_request" });
      return;
    }

    const likeRef = db.collection("postLikes").doc(postId).collection("users").doc(user.uid);

    const result = await updatePostEngagement(postId, async (tx, postRef, postData) => {
      const likeSnap = await tx.get(likeRef);
      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);

      let liked = false;
      if (likeSnap.exists) {
        tx.delete(likeRef);
        counts.likes = Math.max(0, counts.likes - 1);
      } else {
        liked = true;
        tx.set(likeRef, {
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        });
        counts.likes += 1;
      }

      const score = computeScore(counts, createdAtMs);
      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastEngagedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return {
        liked,
        counts,
        score,
      };
    });

    await upsertSavedPostState(user.uid, postId, { liked: asBoolean((result as any).liked, false) });

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "gone") {
      res.status(410).json({ error: code });
      return;
    }
    console.error("[Explore] like failed", error);
    res.status(500).json({ error: "like_failed" });
  }
});

ROUTES.post("/posts/:postId/repost", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    if (!user || !postId) {
      res.status(400).json({ error: "invalid_request" });
      return;
    }

    const repostRef = db.collection("postReposts").doc(postId).collection("users").doc(user.uid);

    const result = await updatePostEngagement(postId, async (tx, postRef, postData) => {
      const repostSnap = await tx.get(repostRef);
      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);

      let reposted = false;
      if (repostSnap.exists) {
        tx.delete(repostRef);
        counts.reposts = Math.max(0, counts.reposts - 1);
      } else {
        reposted = true;
        tx.set(repostRef, {
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        });
        counts.reposts += 1;
      }

      const score = computeScore(counts, createdAtMs);
      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastEngagedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return {
        reposted,
        counts,
        score,
      };
    });

    await upsertSavedPostState(user.uid, postId, { reposted: asBoolean((result as any).reposted, false) });

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "gone") {
      res.status(410).json({ error: code });
      return;
    }
    console.error("[Explore] repost failed", error);
    res.status(500).json({ error: "repost_failed" });
  }
});

ROUTES.post("/posts/:postId/share", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, false);
    const postId = sanitizeText(req.params.postId, 180);
    if (!postId) {
      res.status(400).json({ error: "invalid_post_id" });
      return;
    }

    const shareEventRef = db.collection("postShareEvents").doc();

    const result = await updatePostEngagement(postId, async (tx, postRef, postData) => {
      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);
      counts.shares += 1;
      const score = computeScore(counts, createdAtMs);

      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastEngagedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      tx.set(shareEventRef, {
        postId,
        uid: user?.uid || null,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        source: sanitizeText((req.body || {}).source, 80) || "web",
      });

      return {
        shared: true,
        counts,
        score,
      };
    });

    if (user?.uid) {
      await upsertSavedPostState(user.uid, postId, { shared: true });
    }

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "gone") {
      res.status(410).json({ error: code });
      return;
    }
    console.error("[Explore] share failed", error);
    res.status(500).json({ error: "share_failed" });
  }
});

ROUTES.post("/posts/:postId/save", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    if (!user || !postId) {
      res.status(400).json({ error: "invalid_post_id" });
      return;
    }

    const postSnap = await db.collection("posts").doc(postId).get();
    if (!postSnap.exists) {
      res.status(404).json({ error: "not_found" });
      return;
    }
    const postData = (postSnap.data() || {}) as Record<string, unknown>;
    if (!isPostVisibleToViewer(postData, user.uid)) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    const stateRef = db.collection("users").doc(user.uid).collection("saved_post_state").doc(postId);
    const stateSnap = await stateRef.get();
    const currentSaved = asBoolean((stateSnap.data() || {}).saved, false);
    const explicit = (req.body || {}).save;
    const nextSaved = typeof explicit === "boolean" ? explicit : !currentSaved;

    await upsertSavedPostState(user.uid, postId, { saved: nextSaved });
    res.status(200).json({ ok: true, saved: nextSaved });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] save post failed", error);
    res.status(500).json({ error: "save_post_failed" });
  }
});

ROUTES.post("/posts/:postId/comment", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    const text = sanitizeText((req.body || {}).text, 500);

    if (!user || !postId || !text) {
      res.status(400).json({ error: "invalid_comment" });
      return;
    }

    const profile = await readAuthorProfile(user.uid);
    const commentRef = db.collection("posts").doc(postId).collection("comments").doc();

    const result = await updatePostEngagement(postId, async (tx, postRef, postData) => {
      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);

      tx.set(commentRef, {
        authorUid: user.uid,
        authorHandle: profile.handle,
        text,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      counts.comments += 1;
      const score = computeScore(counts, createdAtMs);

      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastEngagedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return {
        commentId: commentRef.id,
        counts,
        score,
      };
    });

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "gone") {
      res.status(410).json({ error: code });
      return;
    }
    console.error("[Explore] comment failed", error);
    res.status(500).json({ error: "comment_failed" });
  }
});

ROUTES.delete("/posts/:postId/comment/:commentId", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    const commentId = sanitizeText(req.params.commentId, 180);

    if (!user || !postId || !commentId) {
      res.status(400).json({ error: "invalid_request" });
      return;
    }

    const postRef = db.collection("posts").doc(postId);
    const commentRef = postRef.collection("comments").doc(commentId);

    const result = await db.runTransaction(async (tx) => {
      const [postSnap, commentSnap] = await Promise.all([tx.get(postRef), tx.get(commentRef)]);

      if (!postSnap.exists) throw new Error("not_found");
      if (!commentSnap.exists) throw new Error("comment_not_found");

      const postData = (postSnap.data() || {}) as Record<string, unknown>;
      const commentData = (commentSnap.data() || {}) as Record<string, unknown>;

      if (asString(commentData.authorUid) !== user.uid) {
        throw new Error("forbidden");
      }

      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);
      counts.comments = Math.max(0, counts.comments - 1);
      const score = computeScore(counts, createdAtMs);

      tx.delete(commentRef);
      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastEngagedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return { deleted: true, counts, score };
    });

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "forbidden") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "not_found" || code === "comment_not_found") {
      res.status(404).json({ error: code });
      return;
    }
    console.error("[Explore] delete comment failed", error);
    res.status(500).json({ error: "comment_delete_failed" });
  }
});

ROUTES.post("/posts/:postId/report", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    const reason = sanitizeText((req.body || {}).reason, 120);
    const details = sanitizeText((req.body || {}).details, 1000);

    if (!user || !postId || !reason) {
      res.status(400).json({ error: "invalid_report" });
      return;
    }

    const reportRef = db.collection("reports").doc(`${postId}_${user.uid}`);

    const result = await updatePostEngagement(postId, async (tx, postRef, postData) => {
      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);
      const reportSnap = await tx.get(reportRef);

      if (!reportSnap.exists) {
        tx.set(reportRef, {
          postId,
          reporterUid: user.uid,
          reason,
          details,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          status: "open",
        });
        counts.reports += 1;
      }

      const score = computeScore(counts, createdAtMs);
      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return {
        reported: true,
        counts,
        score,
      };
    });

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "gone") {
      res.status(410).json({ error: code });
      return;
    }
    console.error("[Explore] report failed", error);
    res.status(500).json({ error: "report_failed" });
  }
});

ROUTES.patch("/posts/:postId/visibility", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    const visibility = sanitizeText((req.body || {}).visibility, 20) as Visibility;

    if (!user || !postId || !["public", "unlisted"].includes(visibility)) {
      res.status(400).json({ error: "invalid_visibility" });
      return;
    }

    const postRef = db.collection("posts").doc(postId);
    const postSnap = await postRef.get();
    if (!postSnap.exists) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const post = (postSnap.data() || {}) as Record<string, unknown>;
    if (asString(post.authorUid) !== user.uid) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    await postRef.set(
      {
        visibility,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    res.status(200).json({ ok: true, visibility });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] visibility update failed", error);
    res.status(500).json({ error: "visibility_update_failed" });
  }
});

ROUTES.delete("/posts/:postId", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);

    if (!user || !postId) {
      res.status(400).json({ error: "invalid_request" });
      return;
    }

    const postRef = db.collection("posts").doc(postId);
    const postSnap = await postRef.get();
    if (!postSnap.exists) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const postData = (postSnap.data() || {}) as Record<string, unknown>;
    if (asString(postData.authorUid) !== user.uid) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    await postRef.set(
      {
        visibility: "deleted",
        deletedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    await Promise.all([
      deleteCollectionDocs(postRef.collection("comments")),
      deleteCollectionDocs(db.collection("postLikes").doc(postId).collection("users")),
      deleteCollectionDocs(db.collection("postReposts").doc(postId).collection("users")),
      deleteCollectionDocs(db.collection("postShareEvents").where("postId", "==", postId)),
      deleteCollectionDocs(db.collection("reports").where("postId", "==", postId)),
    ]);

    await Promise.all([
      db.collection("postLikes").doc(postId).delete().catch(() => undefined),
      db.collection("postReposts").doc(postId).delete().catch(() => undefined),
    ]);

    await postRef.delete();

    res.status(200).json({ ok: true, deleted: true });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] delete failed", error);
    res.status(500).json({ error: "post_delete_failed" });
  }
});

ROUTES.get("/profile/handle/:handle", async (req, res) => {
  try {
    const handle = normalizeHandle(req.params.handle);
    if (!handle) {
      res.status(400).json({ error: "invalid_handle" });
      return;
    }

    const viewer = await verifyRequestUser(req, false).catch(() => null);

    let snap = await db.collection("users").where("handle", "==", handle).limit(1).get();
    if (snap.empty) {
      snap = await db.collection("users").where("profile.username", "==", handle).limit(1).get();
    }

    if (snap.empty) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const userDoc = snap.docs[0];
    const userData = userDoc.data() || {};
    const payload = await buildProfilePayload(userDoc.id, userData, viewer?.uid || null);

    if (!asBoolean(payload.publicProfile, false) && viewer?.uid !== userDoc.id) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    res.status(200).json(payload);
  } catch (error) {
    console.error("[Explore] profile by handle failed", error);
    res.status(500).json({ error: "profile_lookup_failed" });
  }
});

ROUTES.get("/profile/:uid/posts", async (req, res) => {
  try {
    const uid = sanitizeText(req.params.uid, 140);
    const limit = parseLimit(req.query.limit);
    const cursor = decodeCursor(req.query.cursor);
    if (!uid) {
      res.status(400).json({ error: "invalid_uid" });
      return;
    }

    const viewer = await verifyRequestUser(req, false).catch(() => null);
    const isOwner = viewer?.uid === uid;

    let queryRef: admin.firestore.Query;
    if (isOwner) {
      queryRef = db
        .collection("posts")
        .where("authorUid", "==", uid)
        .orderBy("createdAt", "desc")
        .orderBy(admin.firestore.FieldPath.documentId(), "desc");
    } else {
      queryRef = db
        .collection("posts")
        .where("authorUid", "==", uid)
        .where("visibility", "==", "public")
        .orderBy("createdAt", "desc")
        .orderBy(admin.firestore.FieldPath.documentId(), "desc");
    }

    if (cursor) {
      queryRef = queryRef.startAfter(timestampFromMs(cursor.createdAtMs), cursor.id);
    }

    const snap = await queryRef.limit(limit + 1).get();
    let docs = snap.docs;
    if (isOwner) {
      docs = docs.filter((doc) => asString(doc.data().visibility) !== "deleted");
    }

    const page = docs.slice(0, limit);
    const hasMore = docs.length > limit;
    const postIds = page.map((doc) => doc.id);
    const engagement = await fetchViewerEngagement(postIds, viewer?.uid || null);

    const posts = page.map((doc) => toPostResponse(doc, engagement.get(doc.id) || { liked: false, reposted: false, saved: false }));
    const next = hasMore && posts.length
      ? encodeCursor(buildNextCursor(posts[posts.length - 1] as Record<string, unknown>, false))
      : null;

    res.status(200).json({ posts, cursor: next, owner: isOwner });
  } catch (error) {
    console.error("[Explore] profile posts failed", error);
    res.status(500).json({ error: "profile_posts_failed" });
  }
});

ROUTES.get("/me/profile", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const userSnap = await db.collection("users").doc(viewer.uid).get();
    if (!userSnap.exists) {
      res.status(404).json({ error: "not_found" });
      return;
    }
    const payload = await buildProfilePayload(viewer.uid, (userSnap.data() || {}) as Record<string, unknown>, viewer.uid);
    res.status(200).json(payload);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] me profile failed", error);
    res.status(500).json({ error: "me_profile_failed" });
  }
});

ROUTES.patch("/me/profile", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const body = (req.body || {}) as Record<string, unknown>;
    const profilePatch: Record<string, unknown> = {};
    if (typeof body.publicEmailOptIn === "boolean") {
      profilePatch.publicEmailOptIn = body.publicEmailOptIn;
    }
    if (typeof body.publicProfile === "boolean") {
      profilePatch.publicProfile = body.publicProfile;
    }
    if (!Object.keys(profilePatch).length) {
      res.status(400).json({ error: "no_supported_fields" });
      return;
    }

    await db.collection("users").doc(viewer.uid).set(
      {
        profile: profilePatch,
        profileUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    const userSnap = await db.collection("users").doc(viewer.uid).get();
    const payload = await buildProfilePayload(viewer.uid, (userSnap.data() || {}) as Record<string, unknown>, viewer.uid);
    res.status(200).json({ ok: true, profile: payload });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] me profile update failed", error);
    res.status(500).json({ error: "me_profile_update_failed" });
  }
});

ROUTES.get("/me/notification-settings", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const userRef = db.collection("users").doc(user.uid);
    const [userSnap, followsSnap, watchSnap, tokenSnap] = await Promise.all([
      userRef.get(),
      userRef.collection("follows").limit(200).get(),
      userRef.collection("watchTickers").limit(200).get(),
      userRef.collection("fcmTokens").limit(100).get(),
    ]);

    const userData = (userSnap.data() || {}) as Record<string, unknown>;
    const prefs = normalizeNotificationPrefs({}, (userData.notificationPrefs || {}) as Record<string, unknown>);
    const privacyRaw = (userData.notificationPrivacy || {}) as Record<string, unknown>;
    const coarseLocation = normalizeCoarseLocation(privacyRaw.coarseLocation);

    res.status(200).json({
      notificationPrefs: {
        global: prefs.global,
        following: prefs.following,
        tickers: prefs.tickers,
        watchlist: prefs.watchlist,
        explore: prefs.explore,
        earnings: prefs.earnings,
        ipos: prefs.ipos,
        daily: prefs.daily,
        weekly: prefs.weekly,
        inactiveHidden: prefs.inactiveHidden,
      },
      notificationPrivacy: {
        locationConsent: asBoolean(privacyRaw.locationConsent, false),
        ipRegionConsent: asBoolean(privacyRaw.ipRegionConsent, false),
        timezone: normalizeTimezone(privacyRaw.timezone),
        ipRegion: sanitizeText(privacyRaw.ipRegion, 80),
        coarseLocation,
        updatedAtMs: getTimestampMs(privacyRaw.updatedAt || Date.now()),
      },
      follows: followsSnap.docs.map((doc) => doc.id),
      watchTickers: watchSnap.docs
        .map((doc) => normalizeTicker(doc.id || doc.data().ticker))
        .filter(Boolean),
      tokenCount: tokenSnap.size,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] me notification settings failed", error);
    res.status(500).json({ error: "notification_settings_failed" });
  }
});

ROUTES.post("/notifications/register-token", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const token = sanitizeText((req.body || {}).token, 4096);
    const platform = sanitizeText((req.body || {}).platform, 32) || "web";
    if (!token || token.length < 20) {
      res.status(400).json({ error: "invalid_token" });
      return;
    }

    const tokenRef = db.collection("users").doc(user.uid).collection("fcmTokens").doc(token);
    const privacyInput = ((req.body || {}) as Record<string, unknown>).notificationPrivacy;
    const locationConsent = asBoolean((privacyInput as Record<string, unknown>)?.locationConsent, false);
    const ipRegionConsent = asBoolean((privacyInput as Record<string, unknown>)?.ipRegionConsent, false);
    const tokenMeta: Record<string, unknown> = {};
    if (locationConsent) {
      const timezone = normalizeTimezone((privacyInput as Record<string, unknown>)?.timezone);
      const coarseLocation = normalizeCoarseLocation((privacyInput as Record<string, unknown>)?.coarseLocation);
      tokenMeta.timezone = timezone;
      tokenMeta.coarseLocation = coarseLocation;
      tokenMeta.locationConsent = true;
      tokenMeta.ipRegionConsent = ipRegionConsent;
      let ipRegion = sanitizeText((privacyInput as Record<string, unknown>)?.ipRegion, 80);
      if (ipRegionConsent && !ipRegion) {
        const derived = await fetchIpDerivedRegion(requestIpAddress(req));
        ipRegion = derived.region;
      }
      tokenMeta.ipRegion = ipRegion;
    }
    await tokenRef.set(
      {
        token,
        platform,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
        ...tokenMeta,
      },
      { merge: true }
    );

    await syncTopicsForUser(user.uid);

    res.status(200).json({ ok: true, tokenSuffix: token.slice(-10) });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] register token failed", error);
    res.status(500).json({ error: "register_token_failed" });
  }
});

ROUTES.post("/notifications/unregister-token", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const token = sanitizeText((req.body || {}).token, 4096);
    if (!token || token.length < 20) {
      res.status(400).json({ error: "invalid_token" });
      return;
    }
    await db.collection("users").doc(user.uid).collection("fcmTokens").doc(token).delete().catch(() => undefined);
    res.status(200).json({ ok: true, tokenSuffix: token.slice(-10) });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] unregister token failed", error);
    res.status(500).json({ error: "unregister_token_failed" });
  }
});

ROUTES.post("/notifications/preferences", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const input = (req.body || {}) as Record<string, unknown>;
    const userRef = db.collection("users").doc(user.uid);
    const userSnap = await userRef.get();
    const userData = (userSnap.data() || {}) as Record<string, unknown>;
    const existingPrefs = normalizeNotificationPrefs({}, (userData.notificationPrefs || {}) as Record<string, unknown>);
    const notificationPrefs = normalizeNotificationPrefs(input, existingPrefs as unknown as Record<string, unknown>);

    const existingPrivacy = (userData.notificationPrivacy || {}) as Record<string, unknown>;
    const locationConsent =
      typeof input.locationConsent === "boolean"
        ? asBoolean(input.locationConsent, false)
        : asBoolean(existingPrivacy.locationConsent, false);
    const ipRegionConsent =
      locationConsent &&
      (typeof input.ipRegionConsent === "boolean"
        ? asBoolean(input.ipRegionConsent, false)
        : asBoolean(existingPrivacy.ipRegionConsent, false));
    const timezone = locationConsent
      ? normalizeTimezone(input.timezone || existingPrivacy.timezone || "")
      : "";
    const coarseLocation = locationConsent
      ? normalizeCoarseLocation(input.coarseLocation || existingPrivacy.coarseLocation)
      : null;
    let ipRegion = locationConsent && ipRegionConsent ? sanitizeText(input.ipRegion || existingPrivacy.ipRegion, 80) : "";
    if (locationConsent && ipRegionConsent && !ipRegion) {
      const derived = await fetchIpDerivedRegion(requestIpAddress(req));
      ipRegion = derived.region;
      if (!coarseLocation?.countryCode && derived.countryCode) {
        if (coarseLocation) coarseLocation.countryCode = derived.countryCode;
      }
    }
    const notificationPrivacy = {
      locationConsent,
      ipRegionConsent,
      timezone,
      ipRegion,
      coarseLocation: locationConsent ? coarseLocation : null,
      ipAddress: locationConsent && ipRegionConsent ? sanitizeText(requestIpAddress(req), 120) : "",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await db
      .collection("users")
      .doc(user.uid)
      .set(
        {
          notificationPrefs,
          notificationPrivacy,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

    await syncTopicsForUser(user.uid);

    res.status(200).json({
      ok: true,
      notificationPrefs,
      notificationPrivacy: {
        locationConsent,
        ipRegionConsent,
        timezone,
        ipRegion,
        coarseLocation: locationConsent ? coarseLocation : null,
        updatedAtMs: Date.now(),
      },
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] preferences failed", error);
    res.status(500).json({ error: "preferences_update_failed" });
  }
});

ROUTES.post("/notifications/personalize", async (req, res) => {
  try {
    const input = (req.body || {}) as Record<string, unknown>;
    const title = sanitizeText(input.title, 160) || "Quantura update";
    const body = sanitizeText(input.body, 500);
    const source = sanitizeText(input.source, 40) || "notification";
    const context = input.context && typeof input.context === "object" ? (input.context as Record<string, unknown>) : {};

    const rewritten = await rewriteNotificationWithLlm({
      title,
      body,
      source,
      context,
    });

    res.status(200).json({
      notification: {
        title: rewritten.title,
        body: rewritten.body,
        nextSteps: rewritten.nextSteps,
        personalized: rewritten.personalized,
        disclaimer: "LLMs can sometimes make mistakes.",
      },
    });
  } catch (error) {
    console.error("[Explore] notification personalize failed", error);
    res.status(500).json({ error: "notification_personalize_failed" });
  }
});

ROUTES.post("/notifications/sync-topics", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    await syncTopicsForUser(user.uid);
    res.status(200).json({ ok: true });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] sync topics failed", error);
    res.status(500).json({ error: "sync_topics_failed" });
  }
});

ROUTES.get("/notifications/config", (_req, res) => {
  const vapidPublicKey = sanitizeText(process.env.FCM_WEB_VAPID_KEY || "", 4096);
  res.status(200).json({ vapidPublicKey });
});

ROUTES.post("/notifications/session/ping", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const input = asPlainObject(req.body);
    const isAnonymous =
      typeof input.isAnonymous === "boolean"
        ? asBoolean(input.isAnonymous, false)
        : sanitizeText(user.firebase?.sign_in_provider, 40) === "anonymous";
    const userRef = db.collection("users").doc(user.uid);
    const userSnap = await userRef.get();
    const userData = (userSnap.data() || {}) as Record<string, unknown>;
    const existingPrefs = normalizeNotificationPrefs({}, (userData.notificationPrefs || {}) as Record<string, unknown>);
    const rawPrefs =
      input.notificationPrefs && typeof input.notificationPrefs === "object"
        ? (input.notificationPrefs as Record<string, unknown>)
        : {};
    const notificationPrefs = normalizeNotificationPrefs(rawPrefs, existingPrefs as unknown as Record<string, unknown>);

    await userRef.set(
      {
        isAnonymous,
        lastActiveAt: admin.firestore.FieldValue.serverTimestamp(),
        notificationPrefs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    res.status(200).json({
      ok: true,
      uid: user.uid,
      isAnonymous,
      notificationPrefs,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] session ping failed", error);
    res.status(500).json({ error: "notification_session_ping_failed" });
  }
});

ROUTES.post("/notifications/iap-event", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const input = asPlainObject(req.body);
    const productId = sanitizeText(input.productId, 120);
    if (!productId) {
      res.status(400).json({ error: "invalid_product_id" });
      return;
    }
    const orderId = sanitizeText(input.orderId, 120);
    const status = sanitizeText(input.status || "purchased", 40).toLowerCase() || "purchased";
    const platform = sanitizeText(input.platform || "unknown", 20).toLowerCase() || "unknown";
    const source = sanitizeText(input.source || "native_iap", 80) || "native_iap";
    const sourceUid = sanitizeText(input.sourceUid, 220);
    const purchasedAtMs = Math.max(0, Math.floor(asFinite(input.purchasedAtMs, Date.now())));
    const requestedEventId = sanitizeText(input.eventId, 220).replace(/[^A-Za-z0-9._-]/g, "");
    const generatedEventId = `evt_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
    const eventId = requestedEventId || generatedEventId;
    const isAnonymous = sanitizeText(user.firebase?.sign_in_provider, 40) === "anonymous";
    const userRef = db.collection("users").doc(user.uid);

    await userRef.collection("iapEvents").doc(eventId).set(
      {
        eventId,
        uid: user.uid,
        sourceUid,
        productId,
        orderId,
        status,
        platform,
        source,
        isAnonymous,
        purchasedAtMs,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    res.status(200).json({
      ok: true,
      eventId,
      uid: user.uid,
      isAnonymous,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] iap-event failed", error);
    res.status(500).json({ error: "notification_iap_event_failed" });
  }
});

ROUTES.post("/notifications/merge-anon-data", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const targetUid = sanitizeText(user.uid, 220);
    const input = asPlainObject(req.body);
    const sourceUid = sanitizeText(input.sourceUid, 220);
    if (!sourceUid || !targetUid || sourceUid === targetUid) {
      res.status(400).json({ error: "invalid_source_uid" });
      return;
    }

    const sourceRef = db.collection("users").doc(sourceUid);
    const targetRef = db.collection("users").doc(targetUid);
    const [sourceSnap, targetSnap] = await Promise.all([sourceRef.get(), targetRef.get()]);
    if (!sourceSnap.exists) {
      res.status(404).json({ error: "source_user_not_found" });
      return;
    }

    const sourceData = (sourceSnap.data() || {}) as Record<string, unknown>;
    const targetData = (targetSnap.data() || {}) as Record<string, unknown>;
    const sourceIsAnonymous = asBoolean(sourceData.isAnonymous, true);
    if (!sourceIsAnonymous) {
      res.status(400).json({ error: "source_user_not_anonymous" });
      return;
    }

    const [sourceTokensSnap, sourceIapSnap] = await Promise.all([
      sourceRef.collection("fcmTokens").limit(200).get(),
      sourceRef.collection("iapEvents").limit(300).get(),
    ]);

    const mergeBatch = db.batch();
    let mergedTokenCount = 0;
    sourceTokensSnap.docs.forEach((doc) => {
      const token = sanitizeText(doc.id, 512);
      if (!token) return;
      const data = (doc.data() || {}) as Record<string, unknown>;
      mergeBatch.set(
        targetRef.collection("fcmTokens").doc(token),
        {
          ...data,
          mergedFromAnonymousUid: sourceUid,
          lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      mergedTokenCount += 1;
    });

    let mergedIapCount = 0;
    sourceIapSnap.docs.forEach((doc) => {
      const sourceEventId = sanitizeText(doc.id, 220).replace(/[^A-Za-z0-9._-]/g, "");
      if (!sourceEventId) return;
      const mergedEventId = `m_${sourceUid.slice(0, 20)}_${sourceEventId}`.slice(0, 220);
      const data = (doc.data() || {}) as Record<string, unknown>;
      mergeBatch.set(
        targetRef.collection("iapEvents").doc(mergedEventId),
        {
          ...data,
          uid: targetUid,
          sourceUid,
          mergedFromAnonymousUid: sourceUid,
          mergedAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      mergedIapCount += 1;
    });
    await mergeBatch.commit();

    const sourcePrefs = normalizeNotificationPrefs({}, (sourceData.notificationPrefs || {}) as Record<string, unknown>);
    const targetPrefs = normalizeNotificationPrefs({}, (targetData.notificationPrefs || {}) as Record<string, unknown>);
    const mergedPrefs = normalizeNotificationPrefs(
      sourcePrefs as unknown as Record<string, unknown>,
      targetPrefs as unknown as Record<string, unknown>
    );
    await targetRef.set(
      {
        notificationPrefs: mergedPrefs,
        mergedAnonymousUids: admin.firestore.FieldValue.arrayUnion(sourceUid),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    await sourceRef.set(
      {
        mergedIntoUid: targetUid,
        mergedAt: admin.firestore.FieldValue.serverTimestamp(),
        mergeStatus: "merged",
      },
      { merge: true }
    );

    res.status(200).json({
      ok: true,
      sourceUid,
      targetUid,
      mergedTokenCount,
      mergedIapCount,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] merge-anon-data failed", error);
    res.status(500).json({ error: "notification_merge_anon_failed" });
  }
});

ROUTES.get("/notifications/items", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestedCategory = sanitizeText((req.query as Record<string, unknown>).category, 40).toLowerCase();
    const categoryFilter = requestedCategory && requestedCategory !== "all" ? normalizeNotificationCategory(requestedCategory) : null;
    const unreadOnly = asBoolean((req.query as Record<string, unknown>).unread, false);
    const includeHidden = asBoolean((req.query as Record<string, unknown>).includeHidden, false);
    const limitValue = Math.max(1, Math.min(120, Math.floor(asFinite((req.query as Record<string, unknown>).limit, 40))));
    const fetchLimit = Math.max(limitValue * 3, 120);

    let query: admin.firestore.Query = db
      .collection("notifications")
      .doc(user.uid)
      .collection("items")
      .orderBy("createdAt", "desc")
      .limit(fetchLimit);
    const snap = await query.get();
    const items = snap.docs
      .map((doc) => {
        const data = (doc.data() || {}) as Record<string, unknown>;
        const category = normalizeNotificationCategory(data.category);
        const hidden = asBoolean(data.hidden, false);
        return {
          id: doc.id,
          category,
          title: sanitizeText(data.title, 160) || `${notificationCategoryLabel(category)} update`,
          body: sanitizeText(data.body, 400),
          deepLink: normalizeNotificationDeepLink(data.deepLink),
          hidden,
          read: asBoolean(data.read, false),
          nextSteps: Array.isArray(data.nextSteps)
            ? data.nextSteps.map((item) => sanitizeText(item, 120)).filter(Boolean).slice(0, 4)
            : [],
          createdAtMs: getTimestampMs(data.createdAt),
        };
      })
      .filter((item) => (unreadOnly ? !item.read : true))
      .filter((item) => (includeHidden ? true : !item.hidden))
      .filter((item) => (categoryFilter ? item.category === categoryFilter : true));

    const unreadCount = items.filter((item) => !item.read).length;
    res.status(200).json({
      ok: true,
      unreadCount,
      count: items.length,
      items: items.slice(0, limitValue),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] list items failed", error);
    res.status(500).json({ error: "notification_items_failed" });
  }
});

ROUTES.post("/notifications/items/:itemId/read", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const itemId = sanitizeText(req.params.itemId, 220).replace(/[^A-Za-z0-9._-]/g, "");
    if (!itemId) {
      res.status(400).json({ error: "invalid_item_id" });
      return;
    }
    const readValue = typeof (req.body || {}).read === "boolean" ? asBoolean((req.body || {}).read, true) : true;
    const itemRef = db.collection("notifications").doc(user.uid).collection("items").doc(itemId);
    const itemSnap = await itemRef.get();
    if (!itemSnap.exists) {
      res.status(404).json({ error: "notification_item_not_found" });
      return;
    }
    await itemRef.set(
      {
        read: readValue,
        readAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    res.status(200).json({ ok: true, id: itemId, read: readValue });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] mark item read failed", error);
    res.status(500).json({ error: "notification_item_update_failed" });
  }
});

ROUTES.post("/notifications/items/read-all", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const includeHidden = asBoolean((req.body || {}).includeHidden, false);
    let updated = 0;
    while (true) {
      const query: admin.firestore.Query = db
        .collection("notifications")
        .doc(user.uid)
        .collection("items")
        .where("read", "==", false)
        .limit(200);
      const snap = await query.get();
      if (snap.empty) break;
      const batch = db.batch();
      snap.docs.forEach((doc) => {
        const hidden = asBoolean((doc.data() || {}).hidden, false);
        if (!includeHidden && hidden) return;
        batch.set(
          doc.ref,
          {
            read: true,
            readAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
        updated += 1;
      });
      await batch.commit();
      if (snap.size < 200) break;
    }
    res.status(200).json({ ok: true, updated });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] mark all read failed", error);
    res.status(500).json({ error: "notification_read_all_failed" });
  }
});

ROUTES.post("/notify/event", async (req, res) => {
  try {
    const actor = await verifyRequestUser(req, true);
    if (!actor) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const body = asPlainObject(req.body);
    const category = normalizeNotificationCategory(body.category);
    const title = sanitizeText(body.title, 160) || `${notificationCategoryLabel(category)} update`;
    const message = sanitizeText(body.body || body.message, 320) || "You have a new Quantura update.";
    const deepLink = normalizeNotificationDeepLink(body.deepLink || body.path || "/notifications");
    const nextSteps = Array.isArray(body.nextSteps) ? body.nextSteps.map((item) => sanitizeText(item, 120)).filter(Boolean) : [];
    const metadata = body.metadata && typeof body.metadata === "object" ? (body.metadata as Record<string, unknown>) : {};
    const force = asBoolean(body.force, false);
    const hidden = typeof body.hidden === "boolean" ? asBoolean(body.hidden, false) : category === "inactive";

    const explicitUserIds = Array.isArray(body.userIds)
      ? body.userIds.map((item) => sanitizeText(item, 220)).filter(Boolean)
      : [];
    const targetUsers = new Set<string>(explicitUserIds);

    const inputTickers = Array.isArray(body.tickers)
      ? body.tickers.map((item) => normalizeTicker(item)).filter(Boolean)
      : [normalizeTicker(body.ticker)].filter(Boolean);
    if (!targetUsers.size) {
      if (category === "watchlist" || category === "earnings") {
        const byTicker = await collectUsersByWatchTickers(inputTickers);
        byTicker.forEach((uid) => targetUsers.add(uid));
      } else if (category === "explore") {
        const authorUid = sanitizeText(body.authorUid || body.userId, 220);
        const byFollow = await collectUsersByFollowedAuthor(authorUid);
        byFollow.forEach((uid) => targetUsers.add(uid));
      } else if (category === "ipo") {
        const allUsers = await collectUsersForIpoNotifications();
        allUsers.forEach((uid) => targetUsers.add(uid));
      } else {
        targetUsers.add(actor.uid);
      }
    }

    const excludeUid = sanitizeText(body.excludeUid, 220);
    if (excludeUid) targetUsers.delete(excludeUid);

    if (!targetUsers.size) {
      res.status(404).json({ error: "no_target_users" });
      return;
    }

    const deliverResults = await Promise.all(
      Array.from(targetUsers)
        .slice(0, 2000)
        .map((uid) =>
          sendPushToUserTokens({
            uid,
            category,
            title,
            body: message,
            deepLink,
            hidden,
            nextSteps,
            metadata: {
              ...metadata,
              actorUid: actor.uid,
              tickers: inputTickers,
            },
            force,
          })
        )
    );

    const deliveredUsers = deliverResults.filter((row) => row.delivered > 0).length;
    const attemptedTokens = deliverResults.reduce((sum, row) => sum + row.attempted, 0);
    const deliveredTokens = deliverResults.reduce((sum, row) => sum + row.delivered, 0);
    res.status(200).json({
      ok: true,
      category,
      targetUsers: targetUsers.size,
      deliveredUsers,
      attemptedTokens,
      deliveredTokens,
      skippedUsers: deliverResults.filter((row) => row.skipped).length,
      results: deliverResults.slice(0, 60),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] event send failed", error);
    res.status(500).json({ error: "notify_event_failed" });
  }
});

ROUTES.post("/notify/watchlist", async (req, res) => {
  try {
    const actor = await verifyRequestUser(req, true);
    if (!actor) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const body = asPlainObject(req.body);
    const title = sanitizeText(body.title, 160) || "Watchlist update";
    const message = sanitizeText(body.body || body.message, 320) || "A watchlist update is available.";
    const deepLink = normalizeNotificationDeepLink(body.deepLink || body.path || "/watchlist");
    const nextSteps = Array.isArray(body.nextSteps) ? body.nextSteps.map((item) => sanitizeText(item, 120)).filter(Boolean) : [];
    const metadata = body.metadata && typeof body.metadata === "object" ? (body.metadata as Record<string, unknown>) : {};
    const force = asBoolean(body.force, false);
    const hidden = asBoolean(body.hidden, false);

    const explicitUserIds = Array.isArray(body.userIds)
      ? body.userIds.map((item) => sanitizeText(item, 220)).filter(Boolean)
      : [];
    const targetUsers = new Set<string>(explicitUserIds);

    const inputTickers = Array.isArray(body.tickers)
      ? body.tickers.map((item) => normalizeTicker(item)).filter(Boolean)
      : [normalizeTicker(body.ticker)].filter(Boolean);

    if (!targetUsers.size) {
      const byTicker = await collectUsersByWatchTickers(inputTickers);
      byTicker.forEach((uid) => targetUsers.add(uid));
    }

    const excludeUid = sanitizeText(body.excludeUid, 220);
    if (excludeUid) targetUsers.delete(excludeUid);

    if (!targetUsers.size) {
      res.status(404).json({ error: "no_target_users" });
      return;
    }

    const deliverResults = await Promise.all(
      Array.from(targetUsers)
        .slice(0, 2000)
        .map((uid) =>
          sendPushToUserTokens({
            uid,
            category: "watchlist",
            title,
            body: message,
            deepLink,
            hidden,
            nextSteps,
            metadata: {
              ...metadata,
              actorUid: actor.uid,
              tickers: inputTickers,
            },
            force,
          })
        )
    );

    const deliveredUsers = deliverResults.filter((row) => row.delivered > 0).length;
    const attemptedTokens = deliverResults.reduce((sum, row) => sum + row.attempted, 0);
    const deliveredTokens = deliverResults.reduce((sum, row) => sum + row.delivered, 0);

    res.status(200).json({
      ok: true,
      category: "watchlist",
      targetUsers: targetUsers.size,
      deliveredUsers,
      attemptedTokens,
      deliveredTokens,
      skippedUsers: deliverResults.filter((row) => row.skipped).length,
      results: deliverResults.slice(0, 60),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] watchlist send failed", error);
    res.status(500).json({ error: "notify_watchlist_failed" });
  }
});

ROUTES.get(["/promo/status", "/explore/promo/status"], (_req, res) => {
  const serverTimeMs = Date.now();
  const startsAtMs = PROMO_START_MS;
  const endsAtMs = PROMO_END_MS;
  const active = PROMO_ACTIVE && serverTimeMs >= startsAtMs && serverTimeMs < endsAtMs;
  res.status(200).json({
    serverTimeMs,
    promo: {
      id: PROMO_ID,
      active,
      code: PROMO_CODE,
      discountPercent: PROMO_DISCOUNT_PERCENT,
      headline: "Upgrade your research workflow with a limited-time offer",
      body: `Apply code ${PROMO_CODE} for ${PROMO_DISCOUNT_PERCENT}% off your first cycle.`,
      startsAtMs,
      endsAtMs,
      serverTimeMs,
    },
  });
});

ROUTES.post("/follows/:authorUid", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const authorUid = sanitizeText(req.params.authorUid, 140);
    if (!authorUid || authorUid === user.uid) {
      res.status(400).json({ error: "invalid_author_uid" });
      return;
    }

    const ref = db.collection("users").doc(user.uid).collection("follows").doc(authorUid);
    const snap = await ref.get();
    const explicitFollow = (req.body || {}).follow;

    const shouldFollow = typeof explicitFollow === "boolean" ? explicitFollow : !snap.exists;
    if (shouldFollow) {
      await ref.set(
        {
          authorUid,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    } else {
      await ref.delete();
    }

    await syncTopicsForUser(user.uid);

    res.status(200).json({ ok: true, following: shouldFollow });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] follow toggle failed", error);
    res.status(500).json({ error: "follow_toggle_failed" });
  }
});

ROUTES.post("/watch-tickers/:ticker", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const ticker = normalizeTicker(req.params.ticker);
    if (!ticker) {
      res.status(400).json({ error: "invalid_ticker" });
      return;
    }

    const ref = db.collection("users").doc(user.uid).collection("watchTickers").doc(ticker);
    const snap = await ref.get();
    const explicitWatch = (req.body || {}).watch;

    const shouldWatch = typeof explicitWatch === "boolean" ? explicitWatch : !snap.exists;

    if (shouldWatch) {
      await ref.set(
        {
          ticker,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    } else {
      await ref.delete();
    }

    await syncTopicsForUser(user.uid);

    res.status(200).json({ ok: true, watching: shouldWatch, ticker });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] watch ticker failed", error);
    res.status(500).json({ error: "watch_ticker_failed" });
  }
});

ROUTES.get("/autopilot/sports/teams", async (req, res) => {
  try {
    await requireFoundryUser(req);
    const league = sanitizeText(req.query.league, 20).toLowerCase() as SportsLeagueKey;
    const teams = await listSportsTeams(league);
    res.status(200).json({
      league,
      leagueLabel: SPORTS_LEAGUES[league]?.label || league.toUpperCase(),
      items: teams,
      count: teams.length,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "invalid_sports_league") {
      res.status(400).json({ error: code });
      return;
    }
    console.error("[Sports] list teams failed", error);
    res.status(500).json({ error: "sports_teams_failed", detail: sanitizeText(error?.message, 240) });
  }
});

ROUTES.get("/autopilot/sports/players", async (req, res) => {
  try {
    await requireFoundryUser(req);
    const league = sanitizeText(req.query.league, 20).toLowerCase() as SportsLeagueKey;
    const teamId = sanitizeText(req.query.teamId, 40);
    if (!teamId) {
      res.status(400).json({ error: "invalid_team_id" });
      return;
    }
    const players = await listSportsPlayers(league, teamId);
    res.status(200).json({
      league,
      teamId,
      items: players,
      count: players.length,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "invalid_sports_league" || code === "invalid_team_id" || code === "team_not_found") {
      res.status(400).json({ error: code });
      return;
    }
    console.error("[Sports] list players failed", error);
    res.status(500).json({ error: "sports_players_failed", detail: sanitizeText(error?.message, 240) });
  }
});

ROUTES.get("/autopilot/sports/context", async (req, res) => {
  try {
    await requireFoundryUser(req);
    const league = sanitizeText(req.query.league, 20).toLowerCase() as SportsLeagueKey;
    const teamId = sanitizeText(req.query.teamId, 40);
    const playerId = sanitizeText(req.query.playerId, 40);
    if (!teamId || !playerId) {
      res.status(400).json({ error: "invalid_sports_context_request" });
      return;
    }
    const context = await buildSportsPlayerContext(league, teamId, playerId);
    const historical = buildSportsHistoricalCsv(context);
    res.status(200).json({
      league: {
        key: context.league.key,
        label: context.league.label,
      },
      team: context.team,
      player: context.player,
      statCatalog: context.statCatalog,
      defaultStatKey: context.defaultStatKey,
      futureGames: context.futureGames,
      historical: {
        rowCount: context.historicalRows.length,
        rows: historical.rows,
        headers: historical.headers,
        seasonsUsed: context.seasonsUsed,
      },
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (
      code === "invalid_sports_league" ||
      code === "invalid_team_id" ||
      code === "invalid_player_id" ||
      code === "team_not_found" ||
      code === "player_not_found" ||
      code === "sports_history_unavailable" ||
      code === "sports_stats_unavailable"
    ) {
      res.status(400).json({ error: code });
      return;
    }
    console.error("[Sports] context failed", error);
    res.status(500).json({ error: "sports_context_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.get("/autopilot/sports/team-totals", async (req, res) => {
  try {
    await requireFoundryUser(req);
    const league = sanitizeText(req.query.league, 20).toLowerCase() as SportsLeagueKey;
    const teamId = sanitizeText(req.query.teamId, 40);
    const gameDate = sanitizeText(req.query.gameDate, 40);
    const homeAway = sanitizeText(req.query.homeAway, 20).toLowerCase();
    const timeZone = sanitizeText(req.query.timeZone, 80);
    if (!teamId) {
      res.status(400).json({ error: "invalid_sports_team_totals_request" });
      return;
    }
    const snapshot = await buildSportsTeamGameTotalsSnapshot(league, teamId, gameDate, homeAway, timeZone);
    res.status(200).json({
      league: {
        key: snapshot.league.key,
        label: snapshot.league.label,
      },
      team: snapshot.team,
      filters: snapshot.filters,
      teamTotals: {
        rowCount: snapshot.rows.length,
        headers: snapshot.headers,
        rows: snapshot.rows,
        csvText: snapshot.csvText,
      },
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (
      code === "invalid_sports_league" ||
      code === "invalid_team_id" ||
      code === "team_not_found" ||
      code === "invalid_sports_game_date"
    ) {
      res.status(400).json({ error: code });
      return;
    }
    console.error("[Sports] team totals failed", error);
    res.status(500).json({ error: "sports_team_totals_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.get("/autopilot/sports/runs", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const limit = Math.max(1, Math.min(80, Math.floor(asFinite(req.query.limit, 40))));
    const snap = await db
      .collection("autopilot_requests")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .limit(Math.max(limit * 3, 80))
      .get();
    const docs = snap.docs.filter((doc) => isSportsAutopilotData((doc.data() || {}) as Record<string, unknown>)).slice(0, limit);
    const items = await Promise.all(docs.map((doc) => toAutopilotRunResponse(doc.id, (doc.data() || {}) as Record<string, unknown>)));
    res.status(200).json({
      items,
      count: items.length,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Sports] list runs failed", error);
    res.status(500).json({ error: "sports_runs_list_failed", detail: sanitizeText(error?.message, 240) });
  }
});

ROUTES.post("/autopilot/sports/runs", async (req, res) => {
  let user: admin.auth.DecodedIdToken | null = null;
  let runId = "";
  try {
    user = await requireFoundryUser(req);
    const activeConcurrentRuns = await countActiveAutopilotRunsForOwner(user.uid);
    if (activeConcurrentRuns >= MAX_FOUNDRY_CONCURRENT_RUNS) {
      res.status(429).json({
        error: "foundry_instance_limit_reached",
        detail: `Forecast Foundry allows ${MAX_FOUNDRY_CONCURRENT_RUNS} concurrent active instances. Wait for one run to finish before starting another.`,
        limits: {
          maxConcurrentRuns: MAX_FOUNDRY_CONCURRENT_RUNS,
          activeConcurrentRuns,
        },
      });
      return;
    }

    const body = asPlainObject(req.body);
    const league = sanitizeText(body.league, 20).toLowerCase() as SportsLeagueKey;
    const teamId = sanitizeText(body.teamId, 40);
    const playerId = sanitizeText(body.playerId, 40);
    const statKey = sanitizeText(body.statKey, 120);
    const targetGameId = sanitizeText(body.targetGameId, 40);
    const workspaceId = sanitizeText(body.workspaceId, 220) || user.uid;
    const notes = sanitizeText(body.notes, 2000);

    if (!teamId || !playerId || !statKey || !targetGameId) {
      res.status(400).json({ error: "invalid_sports_run_request" });
      return;
    }

    const context = await buildSportsPlayerContext(league, teamId, playerId);
    const stat = context.statCatalog.find((entry) => sanitizeText(entry.key, 120) === statKey);
    if (!stat) {
      res.status(400).json({ error: "sports_stat_not_found" });
      return;
    }
    const targetGame = context.futureGames.find((entry) => sanitizeText(entry.id, 40) === targetGameId);
    if (!targetGame) {
      res.status(400).json({ error: "sports_future_game_not_found" });
      return;
    }

    const datasetExport = buildSportsAutopilotDatasetCsv(context, stat.key);
    const historicalExport = buildSportsHistoricalCsv(context, stat.key);
    const targetGameMs = Date.parse(targetGame.date);
    const lastHistoryMs = Date.parse(datasetExport.lastHistoryDate);
    const forecastHorizon = Math.max(1, Math.ceil((targetGameMs - lastHistoryMs) / (24 * 60 * 60 * 1000)));
    const syntheticTicker = buildSportsSyntheticTicker(
      {
        league: context.league.key,
        team: context.team,
        player: context.player,
      },
      {}
    );
    const title =
      sanitizeText(body.title, 180) ||
      sanitizeText(`${context.player.displayName} ${stat.label} vs ${targetGame.opponentAbbreviation}`, 180);
    const runRef = db.collection("autopilot_requests").doc();
    runId = runRef.id;
    const owner = safePathSegment(user.uid, 120) || "user";
    const run = safePathSegment(runId, 120) || "run";
    const datasetCsvFile = await writeStorageTextArtifact(
      `predictions/${owner}/foundry/${run}/dataset.csv`,
      datasetExport.csvText,
      "text/csv"
    );
    const historicalCsvFile = await writeStorageTextArtifact(
      `predictions/${owner}/foundry/${run}/historical.csv`,
      historicalExport.csvText,
      "text/csv"
    );
    const inputPayload = buildSportsRunPayloadExport(context, stat, targetGame, datasetExport);
    const inputJsonFile = await writeStorageTextArtifact(
      `predictions/${owner}/foundry/${run}/sports_input.json`,
      JSON.stringify(inputPayload, null, 2),
      "application/json"
    );

    const baseDoc: Record<string, unknown> = {
      userId: user.uid,
      workspaceId,
      title,
      notes,
      mode: "sports_autopilot_run",
      sourceType: "sports_timeseries",
      sourceGroup: "sports",
      autoPublishToExplore: true,
      status: "dataset_ready",
      dataset: {
        ticker: syntheticTicker,
        interval: "1d",
        rowCount: datasetExport.rowCount,
        columns: ["item_id", "timestamp", "closing_price"],
        previewRows: datasetExport.previewRows,
        trainingEligible: true,
        sourceTimeColumn: "timestamp",
        sourceValueColumn: "closing_price",
        sourceItemColumn: "item_id",
        originalHeaders: historicalExport.headers,
        start: context.historicalRows[0]?.gameDate || "",
        end: context.historicalRows[context.historicalRows.length - 1]?.gameDate || "",
        useAllHistory: true,
      },
      sports: {
        leagueKey: context.league.key,
        leagueLabel: context.league.label,
        team: context.team,
        player: context.player,
        stat,
        targetGame,
        futureGames: context.futureGames.slice(0, 24),
        statCatalog: context.statCatalog.slice(0, 48),
        historicalPreviewRows: historicalExport.rows.slice(-24),
        recentHistoryRows: context.historicalRows.slice(-24),
        seasonsUsed: context.seasonsUsed,
      },
      autopilot: {
        forecastHorizon,
        quantiles: ["p10", "p50", "p90"],
      },
      analysis: {},
      files: {
        datasetCsv: datasetCsvFile,
        historicalCsv: historicalCsvFile,
        forecastInputJson: inputJsonFile,
      },
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await runRef.set(baseDoc, { merge: false });
    let requestId = await syncAutopilotMyRequest(user.uid, runId, baseDoc);
    await runRef.set(
      {
        exploreRequestId: requestId,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    try {
      const started = await startAutopilotTraining({
        runId,
        userId: user.uid,
        ticker: syntheticTicker,
        interval: "1d",
        horizon: forecastHorizon,
        quantiles: [0.1, 0.5, 0.9],
        csvText: datasetExport.csvText,
      });
      const runningPatch: Record<string, unknown> = {
        status: "running",
        autopilot: {
          ...(asPlainObject(baseDoc.autopilot) || {}),
          jobName: started.jobName,
          jobArn: started.jobArn,
          inputS3Uri: started.inputS3Uri,
          outputS3Uri: started.outputS3Uri,
          forecastFrequency: started.forecastFrequency,
          forecastHorizon,
          quantiles: started.quantiles,
          algorithms: started.algorithms,
          runtimeSeconds: started.runtimeSeconds,
          objectiveMetric: {
            name: "AverageWeightedQuantileLoss",
            value: null,
          },
          status: "InProgress",
        },
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      await runRef.set(runningPatch, { merge: true });
      const refreshedSnap = await runRef.get();
      const refreshedData = (refreshedSnap.data() || { ...baseDoc, ...runningPatch }) as Record<string, unknown>;
      requestId = await syncAutopilotMyRequest(user.uid, runId, refreshedData);
      await runRef.set(
        {
          exploreRequestId: requestId,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      const finalSnap = await runRef.get();
      res.status(200).json({
        ok: true,
        run: await toAutopilotRunResponse(runId, (finalSnap.data() || refreshedData) as Record<string, unknown>),
      });
    } catch (startError: any) {
      const failedPatch: Record<string, unknown> = {
        status: "failed",
        autopilot: {
          ...(asPlainObject(baseDoc.autopilot) || {}),
          status: "Failed",
          failureReason: sanitizeText(startError?.message, 500) || "sports_autopilot_start_failed",
        },
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      await runRef.set(failedPatch, { merge: true });
      const failedSnap = await runRef.get();
      const failedData = (failedSnap.data() || { ...baseDoc, ...failedPatch }) as Record<string, unknown>;
      requestId = await syncAutopilotMyRequest(user.uid, runId, failedData);
      await runRef.set(
        {
          exploreRequestId: requestId,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      throw startError;
    }
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (
      code === "invalid_sports_league" ||
      code === "invalid_team_id" ||
      code === "invalid_player_id" ||
      code === "team_not_found" ||
      code === "player_not_found" ||
      code === "sports_stat_not_found" ||
      code === "sports_future_game_not_found" ||
      code === "sports_history_unavailable" ||
      code === "sports_stats_unavailable" ||
      code === "sports_history_too_short"
    ) {
      res.status(400).json({ error: code });
      return;
    }
    console.error("[Sports] create run failed", { runId, error });
    res.status(500).json({ error: "sports_run_create_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.post("/autopilot/datasets/history", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const body = asPlainObject(req.body);
    const persist = asBoolean(body.persist, true);
    if (persist && isAnonymousDecodedUser(user)) {
      res.status(403).json({ error: "full_account_required" });
      return;
    }

    const ticker = normalizeTicker(body.ticker);
    const interval = sanitizeText(body.interval, 20) || "1d";
    const start = sanitizeText(body.start, 40);
    const end = sanitizeText(body.end, 40);
    const useAllHistory = asBoolean(body.useAllHistory, false);
    const modelMetrics = normalizeFoundryModelMetrics(body.modelMetrics);
    if (!ticker || !end || (!useAllHistory && !start)) {
      res.status(400).json({ error: "invalid_history_request" });
      return;
    }

    const dataset = await downloadHistoricalStockDataset({ ticker, interval, start, end, useAllHistory });
    const filename = `${ticker}_${sanitizeText(interval, 12)}_${useAllHistory ? "full_history" : start}_${end}.csv`.replace(
      /[^A-Za-z0-9._-]/g,
      "_"
    );

    if (!persist) {
      res.status(200).json({
        ok: true,
        filename,
        rowCount: dataset.rowCount,
        csv: dataset.csvText,
        dataset: {
          ticker: dataset.ticker,
          interval: dataset.interval,
          rowCount: dataset.rowCount,
          columns: dataset.columns,
          previewRows: dataset.previewRows,
          trainingEligible: dataset.trainingEligible,
          start: useAllHistory ? "" : start,
          end,
          useAllHistory,
        },
      });
      return;
    }

    const runRef = db.collection("autopilot_requests").doc();
    const owner = safePathSegment(user.uid, 120) || "user";
    const run = safePathSegment(runRef.id, 120) || "run";
    const datasetFile = await writeStorageTextArtifact(
      `predictions/${owner}/foundry/${run}/dataset.csv`,
      dataset.csvText,
      "text/csv"
    );

    const doc: Record<string, unknown> = {
      userId: user.uid,
      workspaceId: sanitizeText(body.workspaceId, 220) || user.uid,
      notes: sanitizeText(body.notes, 2000),
      modelMetrics,
      mode: "dataset",
      sourceType: "history_downloader",
      status: "dataset_ready",
      dataset: {
        ticker: dataset.ticker,
        interval: dataset.interval,
        rowCount: dataset.rowCount,
        columns: dataset.columns,
        previewRows: dataset.previewRows,
        trainingEligible: dataset.trainingEligible,
        sourceTimeColumn: dataset.sourceTimeColumn,
        sourceValueColumn: dataset.sourceValueColumn,
        sourceItemColumn: dataset.sourceItemColumn,
        originalHeaders: dataset.columns,
        start: useAllHistory ? "" : start,
        end,
        useAllHistory,
      },
      autopilot: {},
      analysis: {},
      files: {
        datasetCsv: {
          ...datasetFile,
        },
      },
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    await runRef.set(doc, { merge: false });
    const requestId = await syncAutopilotMyRequest(user.uid, runRef.id, doc);
    await runRef.set(
      {
        exploreRequestId: requestId,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    const snap = await runRef.get();
    res.status(200).json({
      ok: true,
      run: await toAutopilotRunResponse(snap.id, (snap.data() || doc) as Record<string, unknown>),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Autopilot] history dataset failed", error);
    res.status(500).json({ error: "autopilot_history_dataset_failed", detail: sanitizeText(error?.message, 240) });
  }
});

ROUTES.post("/autopilot/datasets/upload", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const body = asPlainObject(req.body);
    const filePath = sanitizeText(body.filePath, 1000).replace(/^\/+/, "");
    let csvText = asString(body.csvText);
    if (!csvText.trim()) {
      if (!filePath) {
        res.status(400).json({ error: "missing_csv_text" });
        return;
      }
      csvText = await readStorageTextArtifact(filePath);
    }
    if (!csvText.trim()) {
      res.status(400).json({ error: "empty_csv_text" });
      return;
    }
    const tickerHint = normalizeTicker(body.ticker);
    const intervalHint = sanitizeText(body.interval, 20);
    const modelMetrics = normalizeFoundryModelMetrics(body.modelMetrics);
    const classified = await classifyUploadedCsv(csvText, { tickerHint, intervalHint });
    const runRef = db.collection("autopilot_requests").doc();
    const owner = safePathSegment(user.uid, 120) || "user";
    const run = safePathSegment(runRef.id, 120) || "run";
    const uploadedFileName =
      sanitizeText(body.fileName, 240) ||
      filePath.split("/").pop() ||
      (classified.kind === "prediction_output" ? "predictions.csv" : "upload.csv");
    const uploadedCsvFile = csvText.trim()
      ? await writeFoundryFirestoreTextArtifact(runRef.id, "uploadedCsv", csvText, "text/csv", uploadedFileName)
      : {
          storagePath: filePath,
          fileName: uploadedFileName,
          contentType: "text/csv",
          sizeBytes: Buffer.byteLength(csvText || "", "utf8"),
        };

    const baseDoc: Record<string, unknown> = {
      userId: user.uid,
      workspaceId: sanitizeText(body.workspaceId, 220) || user.uid,
      notes: sanitizeText(body.notes, 2000),
      modelMetrics,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      autopilot: {},
      exploreRequestId: "",
    };

    if (classified.kind === "historical_dataset") {
      const normalizedFile = await writeStorageTextArtifact(
        `predictions/${owner}/foundry/${run}/dataset.csv`,
        classified.dataset.csvText,
        "text/csv"
      );
      const doc: Record<string, unknown> = {
        ...baseDoc,
        mode: "dataset",
        sourceType: "historical_csv",
        status: "dataset_ready",
        dataset: {
          ticker: classified.dataset.ticker,
          interval: classified.dataset.interval,
          rowCount: classified.dataset.rowCount,
          columns: classified.dataset.columns,
          previewRows: classified.dataset.previewRows,
          trainingEligible: classified.dataset.trainingEligible,
          sourceTimeColumn: classified.dataset.sourceTimeColumn,
          sourceValueColumn: classified.dataset.sourceValueColumn,
          sourceItemColumn: classified.dataset.sourceItemColumn,
          originalHeaders: classified.originalHeaders,
        },
        analysis: {},
        files: {
          uploadedCsv: uploadedCsvFile,
          datasetCsv: normalizedFile,
        },
      };
      await runRef.set(doc, { merge: false });
      const requestId = await syncAutopilotMyRequest(user.uid, runRef.id, doc);
      await runRef.set(
        {
          exploreRequestId: requestId,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      const snap = await runRef.get();
      res.status(200).json({
        ok: true,
        run: await toAutopilotRunResponse(snap.id, (snap.data() || doc) as Record<string, unknown>),
      });
      return;
    }

    const analysis = classified.analysis;
    const persisted = await persistAutopilotFirestoreAnalysisArtifacts(runRef.id, {
      status: analysis.status,
      summary: analysis.summary,
      markdown: analysis.markdown,
      metrics: analysis.metrics,
      data: analysis.analysis,
      previewRows: analysis.previewRows,
      rowCount: analysis.rowCount,
      columns: analysis.columns,
    }, {
      businessDayCsvText: asString((analysis as any).businessDayCsvText),
    });
    const doc: Record<string, unknown> = {
      ...baseDoc,
      mode: "upload_analysis",
      sourceType: "prediction_csv",
      status: "analysis_ready",
      dataset: {
        ticker: analysis.ticker || tickerHint,
        interval: intervalHint,
        rowCount: analysis.rowCount,
        columns: analysis.columns,
        previewRows: analysis.previewRows,
        trainingEligible: false,
        originalHeaders: classified.originalHeaders,
      },
      analysis: persisted.analysisPatch,
      files: {
        uploadedCsv: uploadedCsvFile,
        ...persisted.filePatches,
      },
    };
    await runRef.set(doc, { merge: false });
    const requestId = await syncAutopilotMyRequest(user.uid, runRef.id, doc);
    await runRef.set(
      {
        exploreRequestId: requestId,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    const snap = await runRef.get();
    res.status(200).json({
      ok: true,
      run: await toAutopilotRunResponse(snap.id, (snap.data() || doc) as Record<string, unknown>),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Autopilot] upload dataset failed", error);
    res.status(500).json({ error: "autopilot_upload_dataset_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.post("/autopilot/runs", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const body = asPlainObject(req.body);
    const runId = sanitizeText(body.runId || body.requestId || body.datasetId, 220);
    if (!runId) {
      res.status(400).json({ error: "missing_run_id" });
      return;
    }
    const owned = await readAutopilotRunForOwner(user.uid, runId);
    if (!owned) {
      res.status(404).json({ error: "run_not_found" });
      return;
    }

    const current = owned.data;
    const dataset = asPlainObject(current.dataset);
    const files = asPlainObject(current.files);
    const sourceType = sanitizeText(current.sourceType, 40);
    const interval = sanitizeText(dataset.interval, 20).toLowerCase();
    if (!(sourceType === "history_downloader" || sourceType === "historical_csv")) {
      res.status(400).json({ error: "run_source_not_trainable" });
      return;
    }
    if (interval !== "1d") {
      res.status(400).json({
        error: "daily_training_only",
        detail: "Forecast Foundry currently supports daily historical datasets only.",
      });
      return;
    }
    if (sanitizeText(asPlainObject(current.autopilot).jobName, 120)) {
      res.status(400).json({ error: "autopilot_job_already_started" });
      return;
    }
    const datasetCsvPath = sanitizeText(asPlainObject(files.datasetCsv).storagePath, 1000);
    if (!datasetCsvPath) {
      res.status(400).json({ error: "dataset_file_missing" });
      return;
    }
    const activeConcurrentRuns = await countActiveAutopilotRunsForOwner(user.uid);
    if (activeConcurrentRuns >= MAX_FOUNDRY_CONCURRENT_RUNS) {
      res.status(429).json({
        error: "foundry_instance_limit_reached",
        detail: `Forecast Foundry allows ${MAX_FOUNDRY_CONCURRENT_RUNS} concurrent active instances. Wait for one run to finish before starting another.`,
        limits: {
          maxConcurrentRuns: MAX_FOUNDRY_CONCURRENT_RUNS,
          activeConcurrentRuns,
        },
      });
      return;
    }
    const csvText = await readStorageTextArtifact(datasetCsvPath);
    const started = await startAutopilotTraining({
      runId,
      userId: user.uid,
      ticker: normalizeTicker(dataset.ticker || current.ticker),
      interval: interval || "1d",
      horizon: asFinite(body.horizon, asFinite(asPlainObject(current.autopilot).forecastHorizon, 30)),
      quantiles: body.quantiles || asPlainObject(current.autopilot).quantiles || ["p10", "p50", "p90"],
      csvText,
    });
    const patch: Record<string, unknown> = {
      mode: "autopilot_run",
      status: "running",
      autopilot: {
        ...(asPlainObject(current.autopilot) || {}),
        jobName: started.jobName,
        jobArn: started.jobArn,
        inputS3Uri: started.inputS3Uri,
        outputS3Uri: started.outputS3Uri,
        forecastFrequency: started.forecastFrequency,
        forecastHorizon: Math.floor(asFinite(body.horizon, 30)),
        quantiles: started.quantiles,
        algorithms: started.algorithms,
        runtimeSeconds: started.runtimeSeconds,
        objectiveMetric: {
          name: "AverageWeightedQuantileLoss",
          value: null,
        },
        status: "InProgress",
      },
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    await db.collection("autopilot_requests").doc(runId).set(patch, { merge: true });
    const refreshedSnap = await db.collection("autopilot_requests").doc(runId).get();
    const refreshedData = (refreshedSnap.data() || { ...current, ...patch }) as Record<string, unknown>;
    const requestId = await syncAutopilotMyRequest(user.uid, runId, refreshedData);
    await db.collection("autopilot_requests").doc(runId).set(
      {
        exploreRequestId: requestId,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    const finalSnap = await db.collection("autopilot_requests").doc(runId).get();
    res.status(200).json({
      ok: true,
      run: await toAutopilotRunResponse(runId, (finalSnap.data() || refreshedData) as Record<string, unknown>),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (code.startsWith("invalid_quantiles:")) {
      res.status(400).json({ error: "invalid_quantiles", detail: sanitizeText(code.replace(/^invalid_quantiles:\s*/i, ""), 260) });
      return;
    }
    console.error("[Autopilot] create run failed", error);
    res.status(500).json({ error: "autopilot_run_create_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.get("/autopilot/runs", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const limit = Math.max(1, Math.min(80, Math.floor(asFinite(req.query.limit, 40))));
    const activeConcurrentRuns = await countActiveAutopilotRunsForOwner(user.uid);
    const snap = await db
      .collection("autopilot_requests")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .limit(limit)
      .get();
    const items = await Promise.all(
      snap.docs.map((doc) => toAutopilotRunResponse(doc.id, (doc.data() || {}) as Record<string, unknown>))
    );
    res.status(200).json({
      items,
      count: items.length,
      limits: {
        maxConcurrentRuns: MAX_FOUNDRY_CONCURRENT_RUNS,
        activeConcurrentRuns,
      },
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Autopilot] list runs failed", error);
    res.status(500).json({ error: "autopilot_runs_list_failed" });
  }
});

ROUTES.get("/autopilot/runs/:runId", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const runId = sanitizeText(req.params.runId, 220);
    if (!runId) {
      res.status(400).json({ error: "invalid_run_id" });
      return;
    }
    const owned = await readAutopilotRunForOwner(user.uid, runId);
    if (!owned) {
      res.status(404).json({ error: "run_not_found" });
      return;
    }
    res.status(200).json({
      run: await toAutopilotRunResponse(owned.id, owned.data),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Autopilot] read run failed", error);
    res.status(500).json({ error: "autopilot_run_read_failed" });
  }
});

ROUTES.get("/autopilot/runs/:runId/files/:fileKey/text", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const runId = sanitizeText(req.params.runId, 220);
    const fileKey = safePathSegment(req.params.fileKey, 80);
    if (!runId || !fileKey) {
      res.status(400).json({ error: "invalid_file_request" });
      return;
    }
    const owned = await readAutopilotRunForOwner(user.uid, runId);
    if (!owned) {
      res.status(404).json({ error: "run_not_found" });
      return;
    }
    const file = asPlainObject(asPlainObject(owned.data.files)[fileKey]);
    if (!Object.keys(file).length) {
      res.status(404).json({ error: "run_file_not_found" });
      return;
    }
    const text = await readFoundryTextArtifact(runId, file);
    res.set("Cache-Control", "private, no-store");
    res.type(sanitizeText(file.contentType, 120) || "text/plain");
    res.status(200).send(text);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Autopilot] read run file failed", error);
    res.status(500).json({ error: "autopilot_run_file_read_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.post("/autopilot/runs/:runId/refresh", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const runId = sanitizeText(req.params.runId, 220);
    if (!runId) {
      res.status(400).json({ error: "invalid_run_id" });
      return;
    }
    const owned = await readAutopilotRunForOwner(user.uid, runId);
    if (!owned) {
      res.status(404).json({ error: "run_not_found" });
      return;
    }
    const reconciled = await reconcileAutopilotRunDocument(runId, owned.data);
    res.status(200).json({
      ok: true,
      run: await toAutopilotRunResponse(runId, reconciled),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Autopilot] refresh run failed", error);
    res.status(500).json({ error: "autopilot_run_refresh_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.post("/autopilot/runs/:runId/analyze", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const runId = sanitizeText(req.params.runId, 220);
    if (!runId) {
      res.status(400).json({ error: "invalid_run_id" });
      return;
    }
    const owned = await readAutopilotRunForOwner(user.uid, runId);
    if (!owned) {
      res.status(404).json({ error: "run_not_found" });
      return;
    }
    const data = owned.data;
    const files = asPlainObject(data.files);
    const dataset = asPlainObject(data.dataset);
    const csvFile = files.predictionsCsv || files.uploadedCsv;
    if (!csvFile) {
      res.status(400).json({ error: "analysis_source_missing" });
      return;
    }
    const csvText = await readFoundryTextArtifact(runId, csvFile);
    if (!csvText.trim()) {
      res.status(400).json({ error: "analysis_source_missing" });
      return;
    }
    const analysis = isSportsAutopilotData(data)
      ? analyzeSportsPredictionCsv(csvText, {
          syntheticTicker: buildSportsSyntheticTicker(data.sports, dataset),
          statKey: sanitizeText(asPlainObject(asPlainObject(data.sports).stat).key, 120),
          statLabel: sanitizeText(asPlainObject(asPlainObject(data.sports).stat).label, 80),
          leagueLabel: sanitizeText(asPlainObject(data.sports).leagueLabel, 40),
          playerName: sanitizeText(asPlainObject(asPlainObject(data.sports).player).displayName, 120),
          teamAbbreviation: sanitizeText(asPlainObject(asPlainObject(data.sports).team).abbreviation, 20).toUpperCase(),
          opponentAbbreviation: sanitizeText(asPlainObject(asPlainObject(data.sports).targetGame).opponentAbbreviation, 20).toUpperCase(),
          targetGameDate: sanitizeText(asPlainObject(asPlainObject(data.sports).targetGame).date, 80),
          targetGameLabel:
            sanitizeText(asPlainObject(asPlainObject(data.sports).targetGame).label, 160) ||
            sanitizeText(asPlainObject(asPlainObject(data.sports).targetGame).displayDate, 120),
          historicalRows: Array.isArray(asPlainObject(data.sports).recentHistoryRows)
            ? ((asPlainObject(data.sports).recentHistoryRows as unknown[]) as NormalizedSportsHistoryRow[])
            : [],
        })
      : await analyzePredictionCsv(csvText, {
          ticker: normalizeTicker(dataset.ticker || data.ticker),
        });
    const analysisPayload = {
      status: analysis.status,
      summary: analysis.summary,
      markdown: analysis.markdown,
      metrics: analysis.metrics,
      data: analysis.analysis,
      previewRows: analysis.previewRows,
      rowCount: analysis.rowCount,
      columns: analysis.columns,
    };
    const persisted =
      sanitizeText(data.sourceType, 40) === "prediction_csv"
        ? await persistAutopilotFirestoreAnalysisArtifacts(runId, analysisPayload, {
            businessDayCsvText: asString((analysis as any).businessDayCsvText),
          })
        : await persistAutopilotAnalysisArtifacts(user.uid, runId, analysisPayload, "", {
            businessDayCsvText: asString((analysis as any).businessDayCsvText),
          });
    const nextStatus =
      sanitizeText(asPlainObject(data.autopilot).transformStatus, 60) === "Completed" ||
      sanitizeText(data.status, 60) === "completed"
        ? "completed"
        : "analysis_ready";
    const patch: Record<string, unknown> = {
      analysis: persisted.analysisPatch,
      files: {
        ...files,
        ...persisted.filePatches,
      },
      status: nextStatus,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    if (isSportsAutopilotData(data)) {
      patch.files = {
        ...files,
        ...persisted.filePatches,
        forecastPayloadJson: await persistSportsForecastPayloadArtifact(user.uid, runId, {
          input: buildAutopilotInputPayload(data),
          output: analysis.analysis,
          metrics: analysis.metrics,
        }),
      };
    }
    await db.collection("autopilot_requests").doc(runId).set(patch, { merge: true });
    const snap = await db.collection("autopilot_requests").doc(runId).get();
    const refreshed = (snap.data() || { ...data, ...patch }) as Record<string, unknown>;
    const requestId = await syncAutopilotMyRequest(user.uid, runId, refreshed);
    await db.collection("autopilot_requests").doc(runId).set(
      {
        exploreRequestId: requestId,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    const finalSnap = await db.collection("autopilot_requests").doc(runId).get();
    res.status(200).json({
      ok: true,
      run: await toAutopilotRunResponse(runId, (finalSnap.data() || refreshed) as Record<string, unknown>),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Autopilot] analyze run failed", error);
    res.status(500).json({ error: "autopilot_run_analyze_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.get("/mobile/automation/entitlement", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const [entitlement, activeAutomationCount] = await Promise.all([
      readAutomationEntitlement(user.uid),
      countActiveAutomationsForOwner(user.uid),
    ]);
    res.status(200).json({
      ok: true,
      entitlement: buildAutomationEntitlementResponse(user.uid, entitlement, activeAutomationCount),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Automation] entitlement read failed", error);
    res.status(500).json({ error: "automation_entitlement_read_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.post("/mobile/automation/apple/verify", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const body = asPlainObject(req.body);
    const contactEmail = await resolveAutomationContactEmail(user.uid, user, body.contactEmail);
    if (!contactEmail) {
      res.status(400).json({
        error: "contact_email_required",
        detail: "A valid email is required before Quantura Automation can be unlocked.",
      });
      return;
    }
    const verified = await verifyAppleAutomationPurchase({
      transactionId: sanitizeText(body.transactionId, 220),
      productId: sanitizeText(body.productId, 120) || AUTOMATION_PRODUCT_ID,
    });
    const entitlement = await syncAutomationEntitlementState(user.uid, {
      automationUnlocked: true,
      purchaseSource: verified.source,
      unlockedAt: admin.firestore.Timestamp.fromMillis(verified.unlockedAtMs),
      productId: verified.productId,
      purchaseReference: verified.purchaseReference,
      contactEmail,
    });
    try {
      await sendAutomationUnlockEmail({
        to: contactEmail,
        ownerUid: user.uid,
        productId: verified.productId,
        purchaseSource: verified.source,
        purchaseReference: verified.purchaseReference,
      });
    } catch (mailError) {
      console.error("[Automation] apple unlock email failed", { uid: user.uid, mailError });
    }
    const activeAutomationCount = await countActiveAutomationsForOwner(user.uid);
    res.status(200).json({
      ok: true,
      entitlement: buildAutomationEntitlementResponse(user.uid, entitlement, activeAutomationCount),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "apple_iap_server_credentials_missing") {
      res.status(501).json({ error: code, detail: "Apple server-side purchase verification is not configured." });
      return;
    }
    if (
      code === "contact_email_required" ||
      code === "missing_transaction_id" ||
      code === "apple_bundle_mismatch" ||
      code === "apple_product_mismatch" ||
      code === "apple_purchase_revoked" ||
      code.startsWith("apple_iap_validation_failed:")
    ) {
      res.status(400).json({ error: code });
      return;
    }
    console.error("[Automation] apple verification failed", error);
    res.status(500).json({ error: "automation_apple_verify_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.post("/mobile/automation/google/verify", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const body = asPlainObject(req.body);
    const contactEmail = await resolveAutomationContactEmail(user.uid, user, body.contactEmail);
    if (!contactEmail) {
      res.status(400).json({
        error: "contact_email_required",
        detail: "A valid email is required before Quantura Automation can be unlocked.",
      });
      return;
    }
    const verified = await verifyGoogleAutomationPurchase({
      packageName: sanitizeText(body.packageName, 220) || GOOGLE_PLAY_ANDROID_PACKAGE,
      productId: sanitizeText(body.productId, 120) || AUTOMATION_PRODUCT_ID,
      purchaseToken: sanitizeText(body.purchaseToken, 400),
      ownerUid: user.uid,
    });
    const entitlement = await syncAutomationEntitlementState(user.uid, {
      automationUnlocked: true,
      purchaseSource: verified.source,
      unlockedAt: admin.firestore.Timestamp.fromMillis(verified.unlockedAtMs),
      productId: verified.productId,
      purchaseReference: verified.purchaseReference,
      contactEmail,
    });
    try {
      await sendAutomationUnlockEmail({
        to: contactEmail,
        ownerUid: user.uid,
        productId: verified.productId,
        purchaseSource: verified.source,
        purchaseReference: verified.purchaseReference,
      });
    } catch (mailError) {
      console.error("[Automation] google unlock email failed", { uid: user.uid, mailError });
    }
    const activeAutomationCount = await countActiveAutomationsForOwner(user.uid);
    res.status(200).json({
      ok: true,
      entitlement: buildAutomationEntitlementResponse(user.uid, entitlement, activeAutomationCount),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "contact_email_required" || code === "missing_purchase_token" || code === "google_purchase_not_completed") {
      res.status(400).json({ error: code });
      return;
    }
    console.error("[Automation] google verification failed", error);
    res.status(500).json({ error: "automation_google_verify_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.get("/mobile/automation", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const [entitlement, activeAutomationCount, snap] = await Promise.all([
      readAutomationEntitlement(user.uid),
      countActiveAutomationsForOwner(user.uid),
      db.collection(AUTOMATION_COLLECTION).where("ownerUid", "==", user.uid).get(),
    ]);
    const automations = snap.docs
      .map((doc) => buildAutomationResponse(doc.id, (doc.data() || {}) as Record<string, unknown>))
      .sort((left, right) => (right.updatedAtMs as number) - (left.updatedAtMs as number));
    res.status(200).json({
      ok: true,
      entitlement: buildAutomationEntitlementResponse(user.uid, entitlement, activeAutomationCount),
      automations,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Automation] list failed", error);
    res.status(500).json({ error: "automation_list_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.post("/mobile/automation", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const body = asPlainObject(req.body);
    const ticker = normalizeTicker(body.ticker);
    if (!ticker) {
      res.status(400).json({ error: "invalid_ticker" });
      return;
    }
    const active = asBoolean(body.active, true);
    const ref = automationRef();
    await db.runTransaction(async (transaction) => {
      const entitlementSnap = await transaction.get(automationEntitlementRef(user.uid));
      const entitlement = (entitlementSnap.data() || {}) as Record<string, unknown>;
      if (!asBoolean(entitlement.automationUnlocked, false)) {
        throw new Error("automation_locked");
      }
      const activeSnap = await transaction.get(
        db.collection(AUTOMATION_COLLECTION).where("ownerUid", "==", user.uid).where("active", "==", true)
      );
      const activeCount = activeSnap.size;
      if (active && activeCount >= AUTOMATION_MAX_ACTIVE) {
        throw new Error("automation_active_limit_reached");
      }
      transaction.set(ref, {
        ticker,
        forecastProfile: normalizeAutomationProfile(body.forecastProfile),
        model: normalizeAutomationModel(body.model),
        cadence: normalizeAutomationCadence(body.cadence),
        horizon: normalizeAutomationHorizon(body.horizon),
        active,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        nextRunAt: active ? admin.firestore.Timestamp.fromMillis(nextAutomationRunAtMs()) : null,
        lastRunAt: null,
        lastStatus: active ? "scheduled" : "inactive",
        lastRunId: "",
        ownerUid: user.uid,
      });
      transaction.set(
        automationEntitlementRef(user.uid),
        {
          maxActiveAutomations: AUTOMATION_MAX_ACTIVE,
          activeAutomationCount: active ? activeCount + 1 : activeCount,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    });
    const created = await readAutomationForOwner(user.uid, ref.id);
    res.status(200).json({
      ok: true,
      automation: created ? buildAutomationResponse(created.id, created.data) : null,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "automation_locked") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "automation_active_limit_reached") {
      res.status(429).json({
        error: code,
        detail: `Quantura Automation allows ${AUTOMATION_MAX_ACTIVE} active automations at a time. Deactivate one first.`,
      });
      return;
    }
    console.error("[Automation] create failed", error);
    res.status(500).json({ error: "automation_create_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.get("/mobile/automation/:automationId", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const owned = await readAutomationForOwner(user.uid, req.params.automationId);
    if (!owned) {
      res.status(404).json({ error: "automation_not_found" });
      return;
    }
    res.status(200).json({
      ok: true,
      automation: buildAutomationResponse(owned.id, owned.data),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Automation] read failed", error);
    res.status(500).json({ error: "automation_read_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.patch("/mobile/automation/:automationId", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const owned = await readAutomationForOwner(user.uid, req.params.automationId);
    if (!owned) {
      res.status(404).json({ error: "automation_not_found" });
      return;
    }
    const body = asPlainObject(req.body);
    const nextTicker = normalizeTicker(body.ticker || owned.data.ticker);
    if (!nextTicker) {
      res.status(400).json({ error: "invalid_ticker" });
      return;
    }
    const nextActive =
      typeof body.active === "boolean" ? asBoolean(body.active, false) : asBoolean(owned.data.active, false);
    await db.runTransaction(async (transaction) => {
      const entitlementSnap = await transaction.get(automationEntitlementRef(user.uid));
      const entitlement = (entitlementSnap.data() || {}) as Record<string, unknown>;
      if (!asBoolean(entitlement.automationUnlocked, false)) {
        throw new Error("automation_locked");
      }
      const activeSnap = await transaction.get(
        db.collection(AUTOMATION_COLLECTION).where("ownerUid", "==", user.uid).where("active", "==", true)
      );
      const activeIds = new Set(activeSnap.docs.map((doc) => doc.id));
      const alreadyActive = activeIds.has(owned.id);
      const activeCountExcludingCurrent = alreadyActive ? Math.max(0, activeSnap.size - 1) : activeSnap.size;
      if (nextActive && activeCountExcludingCurrent >= AUTOMATION_MAX_ACTIVE) {
        throw new Error("automation_active_limit_reached");
      }
      transaction.set(
        automationRef(owned.id),
        {
          ticker: nextTicker,
          forecastProfile: normalizeAutomationProfile(body.forecastProfile || owned.data.forecastProfile),
          model: normalizeAutomationModel(body.model || owned.data.model),
          cadence: normalizeAutomationCadence(body.cadence || owned.data.cadence),
          horizon: normalizeAutomationHorizon(body.horizon || owned.data.horizon),
          active: nextActive,
          nextRunAt: nextActive
            ? owned.data.nextRunAt || admin.firestore.Timestamp.fromMillis(nextAutomationRunAtMs())
            : null,
          lastStatus: nextActive ? sanitizeText(owned.data.lastStatus, 80) || "scheduled" : "inactive",
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      transaction.set(
        automationEntitlementRef(user.uid),
        {
          maxActiveAutomations: AUTOMATION_MAX_ACTIVE,
          activeAutomationCount: nextActive ? activeCountExcludingCurrent + 1 : activeCountExcludingCurrent,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    });
    const updated = await readAutomationForOwner(user.uid, owned.id);
    res.status(200).json({
      ok: true,
      automation: updated ? buildAutomationResponse(updated.id, updated.data) : null,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "automation_locked") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "automation_active_limit_reached") {
      res.status(429).json({
        error: code,
        detail: `Quantura Automation allows ${AUTOMATION_MAX_ACTIVE} active automations at a time. Deactivate one first.`,
      });
      return;
    }
    console.error("[Automation] update failed", error);
    res.status(500).json({ error: "automation_update_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.get("/mobile/automation/:automationId/history", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const owned = await readAutomationForOwner(user.uid, req.params.automationId);
    if (!owned) {
      res.status(404).json({ error: "automation_not_found" });
      return;
    }
    const history = await listAutomationHistoryForOwner(user.uid, owned.id);
    res.status(200).json({
      ok: true,
      automation: buildAutomationResponse(owned.id, owned.data),
      history,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Automation] history failed", error);
    res.status(500).json({ error: "automation_history_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.post("/mobile/automation/:automationId/run", async (req, res) => {
  try {
    const user = await requireFoundryUser(req);
    const owned = await readAutomationForOwner(user.uid, req.params.automationId);
    if (!owned) {
      res.status(404).json({ error: "automation_not_found" });
      return;
    }
    if (!asBoolean(owned.data.active, false)) {
      res.status(400).json({ error: "automation_inactive", detail: "Activate this automation before running it." });
      return;
    }
    const activeAutomationCount = await countActiveAutomationsForOwner(user.uid);
    if (activeAutomationCount > AUTOMATION_MAX_ACTIVE) {
      res.status(429).json({
        error: "automation_active_limit_reached",
        detail: `Quantura Automation allows ${AUTOMATION_MAX_ACTIVE} active automations at a time. Deactivate one first.`,
      });
      return;
    }
    const lastRunId = sanitizeText(owned.data.lastRunId, 220);
    if (lastRunId) {
      const currentRun = await readAutopilotRunForOwner(user.uid, lastRunId);
      const currentStatus = sanitizeText(asPlainObject(currentRun?.data || {}).status, 60).toLowerCase();
      if (ACTIVE_FOUNDRY_STATUSES.has(currentStatus)) {
        res.status(409).json({ error: "automation_run_already_active" });
        return;
      }
    }
    const run = await startAutomationForecastRun(user.uid, owned.id, owned.data, "manual");
    res.status(200).json({ ok: true, run });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "full_account_required") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "automation_locked") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "invalid_ticker") {
      res.status(400).json({ error: code });
      return;
    }
    if (code.startsWith("invalid_quantiles:")) {
      res.status(400).json({ error: "invalid_quantiles", detail: sanitizeText(code.replace(/^invalid_quantiles:\s*/i, ""), 260) });
      return;
    }
    console.error("[Automation] run failed", error);
    res.status(500).json({ error: "automation_run_failed", detail: sanitizeText(error?.message, 260) });
  }
});

ROUTES.get("/my-requests/shared/:slug", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, false).catch(() => null);
    const context = await resolveSharedMyRequestContext(req.params.slug, viewer);
    const { slug, visibility, ownerUid, requestSnap, requestData, readOnly, sourceCollection, sourceId } = context;
    const type = normalizeMyRequestType(requestData.type) || "forecast";
    const responseItem = toMyRequestResponse(requestSnap.id, requestData, { includePayload: true });
    let screenerPayload: Record<string, unknown> | null = null;
    let autopilotRunPayload: Record<string, unknown> | null = null;
    if (type === "screener" && sourceCollection === "screener_runs" && sourceId) {
      const sourceSnap = await db.collection(sourceCollection).doc(sourceId).get();
      if (sourceSnap.exists) {
        screenerPayload = buildSharedScreenerRunPayload(sourceSnap.id, (sourceSnap.data() || {}) as Record<string, unknown>, {
          isPublic: asBoolean(requestData.published, false),
        });
      }
    }
    if (sourceCollection === "autopilot_requests" && sourceId) {
      const sourceSnap = await db.collection(sourceCollection).doc(sourceId).get();
      if (sourceSnap.exists) {
        autopilotRunPayload = await buildSharedAutopilotRunPayload(slug, sourceSnap.id, (sourceSnap.data() || {}) as Record<string, unknown>, {
          isSharedView: true,
          readOnly,
        });
      }
    }
    res.status(200).json({
      request: responseItem,
      readOnly,
      canImport: Boolean(viewer?.uid && viewer.uid !== ownerUid && !isAnonymousDecodedUser(viewer)),
      share: {
        slug,
        visibility,
        shareUrl: myRequestShareUrl(slug, requestData),
      },
      screener: screenerPayload,
      autopilotRun: autopilotRunPayload,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "invalid_share_slug") {
      res.status(400).json({ error: code });
      return;
    }
    if (code === "share_not_found" || code === "share_invalid" || code === "request_not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "forbidden") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Explore] read shared request failed", error);
    res.status(500).json({ error: "request_share_lookup_failed" });
  }
});

ROUTES.get("/my-requests/shared/:slug/files/:fileKey/text", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, false).catch(() => null);
    const context = await resolveSharedMyRequestContext(req.params.slug, viewer);
    const fileKey = safePathSegment(req.params.fileKey, 80);
    if (!fileKey) {
      res.status(400).json({ error: "invalid_file_request" });
      return;
    }
    if (context.sourceCollection !== "autopilot_requests" || !context.sourceId) {
      res.status(404).json({ error: "run_file_not_found" });
      return;
    }
    const runSnap = await db.collection(context.sourceCollection).doc(context.sourceId).get();
    if (!runSnap.exists) {
      res.status(404).json({ error: "run_not_found" });
      return;
    }
    const runData = (runSnap.data() || {}) as Record<string, unknown>;
    const file = asPlainObject(asPlainObject(runData.files)[fileKey]);
    if (!Object.keys(file).length) {
      res.status(404).json({ error: "run_file_not_found" });
      return;
    }
    const text = await readFoundryTextArtifact(context.sourceId, file);
    res.set("Cache-Control", "public, max-age=300");
    res.type(sanitizeText(file.contentType, 120) || "text/plain");
    res.status(200).send(text);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "invalid_share_slug") {
      res.status(400).json({ error: code });
      return;
    }
    if (code === "share_not_found" || code === "share_invalid" || code === "request_not_found" || code === "run_not_found" || code === "run_file_not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "forbidden") {
      res.status(403).json({ error: code });
      return;
    }
    console.error("[Explore] read shared request file failed", error);
    res.status(500).json({ error: "request_share_file_failed" });
  }
});

ROUTES.get("/my-requests", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    await syncLegacyRequestsForUser(viewer.uid);

    const typeFilter = normalizeMyRequestType(req.query.type);
    const publishedFilter = normalizeMyRequestPublishedFilter(req.query.published);
    const queryText = sanitizeText(req.query.q, 140).toLowerCase();
    const limit = parseLimit(req.query.limit);

    const snap = await db
      .collection("users")
      .doc(viewer.uid)
      .collection("requests")
      .orderBy("updatedAt", "desc")
      .limit(Math.max(limit * 4, 140))
      .get();

    const requestDocs = await Promise.all(
      snap.docs.map(async (doc) => {
        const data = (doc.data() || {}) as Record<string, unknown>;
        const repaired = await ensurePublishedMyRequestExplorePost(viewer.uid, doc.id, data);
        return { id: doc.id, data: repaired };
      })
    );

    const rows = requestDocs
      .map((doc) => toMyRequestResponse(doc.id, doc.data, { includePayload: true }))
      .filter((item) => !asBoolean(item.deleted, false))
      .filter((item) => {
        const itemType = normalizeMyRequestType(item.type);
        if (typeFilter && itemType !== typeFilter) return false;
        if (publishedFilter === "published" && !asBoolean(item.published, false)) return false;
        if (publishedFilter === "unpublished" && asBoolean(item.published, false)) return false;
        if (!queryText) return true;
        const haystack = [
          asString(item.title),
          asString(item.ticker),
          asString(item.typeLabel),
          asString((item.outputsMeta as Record<string, unknown>)?.summary || ""),
          asString((item.input as Record<string, unknown>)?.question || ""),
          asString((item.input as Record<string, unknown>)?.notes || ""),
          asString(item.createdAt),
        ]
          .join(" ")
          .toLowerCase();
        return haystack.includes(queryText);
      })
      .slice(0, limit);

    res.status(200).json({
      items: rows,
      count: rows.length,
      type: typeFilter || "all",
      published: publishedFilter,
      q: queryText,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] list my requests failed", error);
    res.status(500).json({ error: "my_requests_list_failed" });
  }
});

ROUTES.post("/my-requests", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const body = asPlainObject(req.body);
    const type = normalizeMyRequestType(body.type);
    if (!type || !MY_REQUEST_TYPE_SET.has(type)) {
      res.status(400).json({ error: "invalid_request_type" });
      return;
    }

    const sourceRefRaw = asPlainObject(body.sourceRef);
    const sourceCollection = sanitizeText(sourceRefRaw.collection, 80);
    const sourceId = sanitizeText(sourceRefRaw.id, 220);
    const requestIdFromBody = normalizeMyRequestId(body.requestId);
    const requestId = requestIdFromBody || (sourceCollection && sourceId ? buildMyRequestDocId(type, sourceId) : `req_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`);
    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const existingSnap = await requestRef.get();
    const existing = (existingSnap.data() || {}) as Record<string, unknown>;

    const input = normalizeMyRequestInput(body.input);
    const outputsMeta = trimOutputsMeta(body.outputsMeta);
    const titleEdited = asBoolean(existing.titleEdited, false);
    const title = titleEdited
      ? sanitizeText(existing.title, 180) || defaultMyRequestTitle(type, input)
      : sanitizeText(body.title, 180) || sanitizeText(existing.title, 180) || defaultMyRequestTitle(type, input);
    const shareExisting = normalizeMyRequestShareObject(existing.share);
    const shareRequested = normalizeMyRequestShareObject(body.share);
    const share: Record<string, unknown> = {
      visibility: normalizeMyRequestVisibility(shareRequested.visibility, shareExisting.visibility),
      slug: normalizeShareId(shareRequested.slug || shareExisting.slug),
      createdAt: shareExisting.createdAt || shareRequested.createdAt || null,
    };
    const ticker = firstTickerFromRequest(input, { collection: sourceCollection, id: sourceId }, outputsMeta);
    const nextPublished = asBoolean(body.published, asBoolean(existing.published, false));
    const requestedVisibility = normalizeMyRequestVisibility(
      body.visibility,
      normalizeMyRequestVisibility(existing.visibility, share.visibility as MyRequestShareVisibility)
    );
    const effectiveVisibility = nextPublished ? "public" : requestedVisibility;

    const payload: Record<string, unknown> = {
      type,
      ownerUid: viewer.uid,
      title,
      titleEdited,
      input,
      outputsMeta,
      sourceRef: {
        collection: sourceCollection,
        id: sourceId,
      },
      searchText: buildMyRequestSearchText(title, type, ticker, input, outputsMeta),
      published: nextPublished,
      publishedAt: nextPublished ? existing.publishedAt || admin.firestore.FieldValue.serverTimestamp() : null,
      explorePostId: nextPublished ? sanitizeText(body.explorePostId || existing.explorePostId, 220) : "",
      deleted: false,
      share,
      visibility: effectiveVisibility,
      createdAt: existing.createdAt || admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await requestRef.set(payload, { merge: true });
    if (nextPublished) {
      const merged = { ...existing, ...payload };
      const postId = await upsertExplorePostFromMyRequest(viewer.uid, requestId, merged, "public");
      await requestRef.set(
        {
          published: true,
          publishedAt: existing.publishedAt || admin.firestore.FieldValue.serverTimestamp(),
          explorePostId: postId,
          visibility: "public",
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    }
    const refreshed = await requestRef.get();
    res.status(200).json({
      ok: true,
      request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] upsert my request failed", error);
    res.status(500).json({ error: "my_request_upsert_failed" });
  }
});

ROUTES.get("/my-requests/:requestId", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const item = await readMyRequestForOwner(viewer.uid, requestId);
    if (!item || asBoolean(item.data.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const repaired = await ensurePublishedMyRequestExplorePost(viewer.uid, item.id, item.data);
    res.status(200).json({
      request: toMyRequestResponse(item.id, repaired, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] read my request failed", error);
    res.status(500).json({ error: "my_request_read_failed" });
  }
});

ROUTES.patch("/my-requests/:requestId", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const body = asPlainObject(req.body);
    const title = sanitizeText(body.title, 180);
    if (!title) {
      res.status(400).json({ error: "invalid_title" });
      return;
    }

    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const snap = await requestRef.get();
    if (!snap.exists) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const existing = (snap.data() || {}) as Record<string, unknown>;
    if (asBoolean(existing.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }

    await requestRef.set(
      {
        title,
        titleEdited: true,
        searchText: buildMyRequestSearchText(
          title,
          normalizeMyRequestType(existing.type) || "forecast",
          normalizeTicker((existing.input as Record<string, unknown>)?.ticker),
          normalizeMyRequestInput(existing.input),
          trimOutputsMeta(existing.outputsMeta)
        ),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    const share = normalizeMyRequestShareObject(existing.share);
    if (share.slug && share.visibility !== "private") {
      await db
        .collection("request_shares")
        .doc(share.slug)
        .set(
          {
            title,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
    }

    const refreshed = await requestRef.get();
    res.status(200).json({
      ok: true,
      request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] rename my request failed", error);
    res.status(500).json({ error: "my_request_rename_failed" });
  }
});

ROUTES.post("/my-requests/:requestId/share", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }

    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const snap = await requestRef.get();
    if (!snap.exists) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const existing = (snap.data() || {}) as Record<string, unknown>;
    if (asBoolean(existing.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }

    const body = asPlainObject(req.body);
    const visibility = normalizeMyRequestVisibility(body.visibility, "unlisted");
    const shareExisting = normalizeMyRequestShareObject(existing.share);

    if (visibility === "private") {
      await requestRef.set(
        {
          share: {
            visibility: "private",
            slug: shareExisting.slug || "",
            createdAt: shareExisting.createdAt || null,
          },
          visibility: "private",
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      if (shareExisting.slug) {
        await db.collection("request_shares").doc(shareExisting.slug).delete().catch(() => undefined);
      }
      const refreshed = await requestRef.get();
      res.status(200).json({
        ok: true,
        share: {
          visibility: "private",
          slug: shareExisting.slug || "",
          shareUrl: "",
        },
        request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
      });
      return;
    }

    const slug = await ensureUniqueMyRequestShareSlug(viewer.uid, requestId, shareExisting.slug);
    const sharePayload = {
      visibility,
      slug,
      createdAt: shareExisting.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    };
    const type = normalizeMyRequestType(existing.type) || "forecast";
    const title = sanitizeText(existing.title, 180) || defaultMyRequestTitle(type, normalizeMyRequestInput(existing.input));
    const ticker = firstTickerFromRequest(
      normalizeMyRequestInput(existing.input),
      asPlainObject(existing.sourceRef),
      trimOutputsMeta(existing.outputsMeta)
    );

    await Promise.all([
      requestRef.set(
        {
          share: sharePayload,
          visibility,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      ),
      db
        .collection("request_shares")
        .doc(slug)
        .set(
          {
            slug,
            ownerUid: viewer.uid,
            requestId,
            type,
            title,
            ticker,
            visibility,
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        ),
    ]);

    const shareUrl = myRequestShareUrl(slug, existing);
    const refreshed = await requestRef.get();
    res.status(200).json({
      ok: true,
      share: {
        visibility,
        slug,
        shareUrl,
      },
      request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] share my request failed", error);
    res.status(500).json({ error: "my_request_share_failed" });
  }
});

ROUTES.post("/my-requests/:requestId/publish", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const snap = await requestRef.get();
    if (!snap.exists) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const existing = (snap.data() || {}) as Record<string, unknown>;
    if (asBoolean(existing.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }

    const body = asPlainObject(req.body);
    const requestedVisibility = normalizeMyRequestVisibility(body.visibility, "public");
    const visibility = requestedVisibility === "public" ? "public" : "unlisted";
    const postId = await upsertExplorePostFromMyRequest(viewer.uid, requestId, existing, visibility);

    await requestRef.set(
      {
        published: true,
        publishedAt: admin.firestore.FieldValue.serverTimestamp(),
        explorePostId: postId,
        visibility: requestedVisibility === "private" ? "unlisted" : requestedVisibility,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    const sourceRef = asPlainObject(existing.sourceRef);
    const sourceCollection = sanitizeText(sourceRef.collection, 80);
    const sourceId = sanitizeText(sourceRef.id, 220);
    if ((normalizeMyRequestType(existing.type) || "forecast") === "screener" && sourceCollection === "screener_runs" && sourceId) {
      await db
        .collection(sourceCollection)
        .doc(sourceId)
        .set(
          {
            isPublic: true,
            published: true,
            publishedAt: admin.firestore.FieldValue.serverTimestamp(),
            explorePostId: postId,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
    }

    const refreshed = await requestRef.get();
    res.status(200).json({
      ok: true,
      request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
      post: {
        id: postId,
        visibility,
      },
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] publish my request failed", error);
    res.status(500).json({ error: "my_request_publish_failed" });
  }
});

ROUTES.post("/my-requests/:requestId/unpublish", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const snap = await requestRef.get();
    if (!snap.exists) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const existing = (snap.data() || {}) as Record<string, unknown>;
    if (asBoolean(existing.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }

    const sourceRef = asPlainObject(existing.sourceRef);
    const sourceCollection = sanitizeText(sourceRef.collection, 80);
    const sourceId = sanitizeText(sourceRef.id, 220);
    const type = normalizeMyRequestType(existing.type) || "forecast";
    const candidatePostIds = [
      sanitizeText(existing.explorePostId, 220),
      deriveMyRequestExplorePostId(requestId, existing),
      type === "forecast" && sourceCollection === "forecast_requests" ? `forecast_${sourceId}` : "",
      type === "screener" && sourceCollection === "screener_runs" ? `screener_${sourceId}` : "",
    ]
      .map((item) => sanitizeText(item, 220))
      .filter(Boolean);

    await Promise.all(
      candidatePostIds.map((postId) =>
        db
          .collection("posts")
          .doc(postId)
          .set(
            {
              visibility: "hidden",
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          )
          .catch(() => undefined)
      )
    );

    await requestRef.set(
      {
        published: false,
        publishedAt: null,
        explorePostId: "",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    if (type === "screener" && sourceCollection === "screener_runs" && sourceId) {
      await db
        .collection(sourceCollection)
        .doc(sourceId)
        .set(
          {
            isPublic: false,
            published: false,
            publishedAt: null,
            explorePostId: "",
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
    }

    const refreshed = await requestRef.get();
    res.status(200).json({
      ok: true,
      request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] unpublish my request failed", error);
    res.status(500).json({ error: "my_request_unpublish_failed" });
  }
});

ROUTES.post("/my-requests/:requestId/duplicate", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const source = await readMyRequestForOwner(viewer.uid, requestId);
    if (!source || asBoolean(source.data.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const type = normalizeMyRequestType(source.data.type) || "forecast";
    const cloneId = `req_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    const cloneTitle = sanitizeText(source.data.title, 160) || defaultMyRequestTitle(type, normalizeMyRequestInput(source.data.input));

    await db
      .collection("users")
      .doc(viewer.uid)
      .collection("requests")
      .doc(cloneId)
      .set(
        {
          ...source.data,
          ownerUid: viewer.uid,
          title: `${cloneTitle} (Copy)`,
          titleEdited: true,
          published: false,
          publishedAt: null,
          explorePostId: "",
          share: {
            visibility: "private",
            slug: "",
            createdAt: null,
          },
          visibility: "private",
          deleted: false,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: false }
      );

    const cloneSnap = await db.collection("users").doc(viewer.uid).collection("requests").doc(cloneId).get();
    res.status(200).json({
      ok: true,
      request: toMyRequestResponse(cloneSnap.id, (cloneSnap.data() || {}) as Record<string, unknown>, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] duplicate my request failed", error);
    res.status(500).json({ error: "my_request_duplicate_failed" });
  }
});

ROUTES.delete("/my-requests/:requestId", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const snap = await requestRef.get();
    if (!snap.exists) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const existing = (snap.data() || {}) as Record<string, unknown>;
    const share = normalizeMyRequestShareObject(existing.share);

    await requestRef.set(
      {
        deleted: true,
        published: false,
        publishedAt: null,
        explorePostId: "",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    if (share.slug) {
      await db.collection("request_shares").doc(share.slug).delete().catch(() => undefined);
    }
    res.status(200).json({ ok: true, deleted: true, requestId });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] delete my request failed", error);
    res.status(500).json({ error: "my_request_delete_failed" });
  }
});

ROUTES.get("/saved/folders", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const userRef = db.collection("users").doc(viewer.uid);
    const [customSnap, systemCounts] = await Promise.all([
      userRef.collection("saved_folders").orderBy("updatedAt", "desc").limit(80).get(),
      listSystemFolderCounts(viewer.uid),
    ]);

    const folders = [
      ...SYSTEM_FOLDERS.map((folder) => ({
        id: folder.id,
        name: folder.displayName,
        isSystem: true,
        itemCount: asFinite(systemCounts[folder.id], 0),
      })),
      ...customSnap.docs
        .map((doc) => ({ id: doc.id, ...(doc.data() || {}) }))
        .filter((row) => !asBoolean((row as Record<string, unknown>).isSystem, false))
        .map((row) => ({
          id: asString((row as Record<string, unknown>).id),
          name: asString((row as Record<string, unknown>).name, "Untitled folder"),
          isSystem: false,
          itemCount: asFinite((row as Record<string, unknown>).itemCount, 0),
          createdAtMs: getTimestampMs((row as Record<string, unknown>).createdAt),
          updatedAtMs: getTimestampMs((row as Record<string, unknown>).updatedAt),
        })),
    ];

    res.status(200).json({ folders });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] list saved folders failed", error);
    res.status(500).json({ error: "saved_folders_failed" });
  }
});

ROUTES.post("/saved/folders", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const requestedName = sanitizeText((req.body || {}).name, 80);
    const requestedId = normalizeFolderId((req.body || {}).id);
    if (!requestedName || requestedName.length < 2) {
      res.status(400).json({ error: "invalid_folder_name" });
      return;
    }

    let folderId = requestedId || normalizeFolderId(requestedName);
    if (!folderId || systemFolderById(folderId)) {
      folderId = `folder-${Date.now()}`;
    }

    const folderRef = db.collection("users").doc(viewer.uid).collection("saved_folders").doc(folderId);
    const existing = await folderRef.get();
    if (existing.exists) {
      folderId = `${folderId}-${Math.floor(Date.now() / 1000)}`;
    }

    await db
      .collection("users")
      .doc(viewer.uid)
      .collection("saved_folders")
      .doc(folderId)
      .set(
        {
          name: requestedName,
          isSystem: false,
          itemCount: 0,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

    res.status(200).json({
      ok: true,
      folder: {
        id: folderId,
        name: requestedName,
        isSystem: false,
        itemCount: 0,
      },
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] create saved folder failed", error);
    res.status(500).json({ error: "saved_folder_create_failed" });
  }
});

ROUTES.get("/saved/folders/:folderId/items", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const folderId = normalizeFolderId(req.params.folderId);
    const limit = parseLimit(req.query.limit);
    if (!folderId) {
      res.status(400).json({ error: "invalid_folder_id" });
      return;
    }

    if (systemFolderById(folderId)) {
      const items = await listSystemFolderItems(viewer.uid, folderId, limit);
      res.status(200).json({ folderId, isSystem: true, items });
      return;
    }

    const folderRef = db.collection("users").doc(viewer.uid).collection("saved_folders").doc(folderId);
    const folderSnap = await folderRef.get();
    if (!folderSnap.exists) {
      res.status(404).json({ error: "folder_not_found" });
      return;
    }

    const itemsSnap = await folderRef.collection("items").orderBy("updatedAt", "desc").limit(limit).get();
    const items = itemsSnap.docs.map((doc) => ({ itemId: doc.id, ...(doc.data() || {}) }));
    res.status(200).json({ folderId, isSystem: false, items });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] list folder items failed", error);
    res.status(500).json({ error: "saved_folder_items_failed" });
  }
});

ROUTES.post("/saved/folders/:folderId/items", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const folderId = normalizeFolderId(req.params.folderId);
    if (!folderId || systemFolderById(folderId)) {
      res.status(400).json({ error: "invalid_folder_id" });
      return;
    }
    const itemType = normalizeSavedItemType((req.body || {}).itemType);
    const sourceId = normalizeSourceId((req.body || {}).sourceId);
    if (!itemType || !sourceId) {
      res.status(400).json({ error: "invalid_item_payload" });
      return;
    }

    const folderRef = db.collection("users").doc(viewer.uid).collection("saved_folders").doc(folderId);
    const folderSnap = await folderRef.get();
    if (!folderSnap.exists) {
      res.status(404).json({ error: "folder_not_found" });
      return;
    }

    const resolved = await resolveSavedItem(viewer.uid, itemType, sourceId);
    if (!resolved) {
      res.status(404).json({ error: "source_not_found" });
      return;
    }

    const itemId = buildFolderItemDocId(itemType, sourceId);
    const itemRef = folderRef.collection("items").doc(itemId);

    await db.runTransaction(async (tx) => {
      const existing = await tx.get(itemRef);
      tx.set(
        itemRef,
        {
          ...resolved,
          itemType,
          sourceId,
          itemId,
          createdAt: existing.exists ? existing.get("createdAt") || admin.firestore.FieldValue.serverTimestamp() : admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      if (!existing.exists) {
        tx.set(
          folderRef,
          {
            itemCount: admin.firestore.FieldValue.increment(1),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
      }
    });

    res.status(200).json({ ok: true, folderId, itemId, item: resolved });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] save folder item failed", error);
    res.status(500).json({ error: "saved_folder_item_create_failed" });
  }
});

ROUTES.delete("/saved/folders/:folderId/items/:itemType/:sourceId", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const folderId = normalizeFolderId(req.params.folderId);
    const itemType = normalizeSavedItemType(req.params.itemType);
    const sourceId = normalizeSourceId(req.params.sourceId);
    if (!folderId || !itemType || !sourceId || systemFolderById(folderId)) {
      res.status(400).json({ error: "invalid_request" });
      return;
    }

    const folderRef = db.collection("users").doc(viewer.uid).collection("saved_folders").doc(folderId);
    const itemId = buildFolderItemDocId(itemType, sourceId);
    const itemRef = folderRef.collection("items").doc(itemId);

    await db.runTransaction(async (tx) => {
      const existing = await tx.get(itemRef);
      if (!existing.exists) return;
      tx.delete(itemRef);
      tx.set(
        folderRef,
        {
          itemCount: admin.firestore.FieldValue.increment(-1),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    });

    res.status(200).json({ ok: true, folderId, itemId });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] delete folder item failed", error);
    res.status(500).json({ error: "saved_folder_item_delete_failed" });
  }
});

ROUTES.get("/saved/search", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const queryText = sanitizeText(req.query.q, 140);
    const limit = parseLimit(req.query.limit);

    const [forecastSnap, screenerSnap, councilSnap, ownPostsSnap, savedStateSnap] = await Promise.all([
      db.collection("forecast_requests").where("userId", "==", viewer.uid).orderBy("createdAt", "desc").limit(60).get(),
      db.collection("screener_runs").where("userId", "==", viewer.uid).orderBy("createdAt", "desc").limit(60).get(),
      db.collection(MODEL_COUNCIL_RESPONSE_COLLECTION).where("userId", "==", viewer.uid).limit(60).get(),
      db.collection("posts").where("authorUid", "==", viewer.uid).orderBy("createdAt", "desc").limit(60).get(),
      db.collection("users").doc(viewer.uid).collection("saved_post_state").limit(140).get(),
    ]);

    const items: Record<string, unknown>[] = [];
    forecastSnap.docs.forEach((doc) => {
      const data = doc.data() || {};
      items.push({
        itemType: "forecast",
        sourceId: doc.id,
        itemId: buildFolderItemDocId("forecast", doc.id),
        title: asString(data.title, `${normalizeTicker(data.ticker)} forecast`),
        subtitle: asString(data.serviceMessage || ""),
        ticker: normalizeTicker(data.ticker),
        targetUrl: `/forecasting?forecastId=${encodeURIComponent(doc.id)}`,
        createdAtMs: getTimestampMs(data.createdAt),
        updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
      });
    });

    screenerSnap.docs.forEach((doc) => {
      const data = doc.data() || {};
      items.push({
        itemType: "screener",
        sourceId: doc.id,
        itemId: buildFolderItemDocId("screener", doc.id),
        title: asString(data.title, "Screener run"),
        subtitle: asString(data.notes || ""),
        ticker: normalizeTicker(((data.results as Array<Record<string, unknown>> | undefined) || [])[0]?.symbol),
        targetUrl: `/screener?runId=${encodeURIComponent(doc.id)}`,
        createdAtMs: getTimestampMs(data.createdAt),
        updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
      });
    });

    councilSnap.docs.forEach((doc) => {
      const data = doc.data() || {};
      items.push({
        itemType: "model_council",
        sourceId: doc.id,
        itemId: buildFolderItemDocId("model_council", doc.id),
        title: `${normalizeTicker(data.ticker) || "Ticker"} Model Council`,
        subtitle: asString(data.question || ""),
        ticker: normalizeTicker(data.ticker),
        targetUrl: "/model-council",
        createdAtMs: getTimestampMs(data.createdAt),
        updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
      });
    });

    ownPostsSnap.docs.forEach((doc) => {
      const data = (doc.data() || {}) as Record<string, unknown>;
      if (asString(data.visibility) === "deleted") return;
      items.push(buildPostSavedItem(doc.id, data));
    });

    if (!savedStateSnap.empty) {
      const refs = savedStateSnap.docs.map((doc) => db.collection("posts").doc(doc.id));
      const postDocs = await db.getAll(...refs);
      postDocs.forEach((doc) => {
        if (!doc.exists) return;
        const data = (doc.data() || {}) as Record<string, unknown>;
        if (!isPostVisibleToViewer(data, viewer.uid)) return;
        items.push(buildPostSavedItem(doc.id, data));
      });
    }

    const dedup = new Map<string, Record<string, unknown>>();
    items.forEach((item) => {
      const key = `${asString(item.itemType)}:${asString(item.sourceId)}`;
      if (!dedup.has(key)) dedup.set(key, item);
    });

    const filtered = Array.from(dedup.values())
      .filter((item) => matchesSearchQuery(item, queryText))
      .sort((a, b) => asFinite(b.updatedAtMs, 0) - asFinite(a.updatedAtMs, 0));

    res.status(200).json({
      q: queryText,
      count: filtered.length,
      items: filtered.slice(0, limit),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] saved search failed", error);
    res.status(500).json({ error: "saved_search_failed" });
  }
});

ROUTES.get("/shares/:shareId", async (req, res) => {
  try {
    const shareId = normalizeShareId(req.params.shareId);
    if (!shareId) {
      res.status(400).json({ error: "invalid_share_id" });
      return;
    }

    const viewer = await verifyRequestUser(req, false).catch(() => null);
    const shareSnap = await db.collection("shares").doc(shareId).get();
    if (!shareSnap.exists) {
      res.status(404).json({ error: "share_not_found" });
      return;
    }
    const shareDoc = (shareSnap.data() || {}) as Record<string, unknown>;
    const kind = asString(shareDoc.kind).trim().toLowerCase();
    const sourceCollection = asString(shareDoc.sourceCollection);
    const sourceId = asString(shareDoc.sourceId);
    if (!kind || !sourceCollection || !sourceId) {
      res.status(404).json({ error: "share_invalid" });
      return;
    }

    const sourceSnap = await db.collection(sourceCollection).doc(sourceId).get();
    if (!sourceSnap.exists) {
      res.status(404).json({ error: "source_not_found" });
      return;
    }

    if (kind !== "screener") {
      res.status(200).json({
        shareId,
        kind,
        sourceId,
        sourceCollection,
        readOnly: true,
        unsupported: true,
      });
      return;
    }

    const source = (sourceSnap.data() || {}) as Record<string, unknown>;
    const ownerUid = asString(source.userId);
    const readOnly = !(viewer?.uid && ownerUid && viewer.uid === ownerUid);
    const ownerProfile = ownerUid ? await readAuthorProfile(ownerUid) : { handle: "", photoURL: "" };

    res.status(200).json({
      shareId,
      kind: "screener",
      sourceId: sourceSnap.id,
      readOnly,
      canImport: Boolean(viewer?.uid && readOnly),
      screener: buildSharedScreenerRunPayload(sourceSnap.id, source, {
        userId: ownerUid,
        ownerUsername: asString(source.ownerUsername || ownerProfile.handle),
        ownerAvatar: asString(source.ownerAvatar || "bull"),
      }),
    });
  } catch (error) {
    console.error("[Explore] share lookup failed", error);
    res.status(500).json({ error: "share_lookup_failed" });
  }
});

function cronRequestAuthorized(req: Request): boolean {
  const expected = sanitizeText(process.env.CRON_SECRET, 1000);
  if (!expected) return false;
  const authorization = asString(req.headers.authorization);
  const supplied = authorization.match(/^Bearer\s+(.+)$/i)?.[1]?.trim() || asString(req.headers["x-cron-secret"]);
  if (!supplied) return false;
  const expectedBuffer = Buffer.from(expected);
  const suppliedBuffer = Buffer.from(supplied);
  return expectedBuffer.length === suppliedBuffer.length && crypto.timingSafeEqual(expectedBuffer, suppliedBuffer);
}

async function reconcileCreatedPosts(limitPerCollection = 100): Promise<Record<string, number>> {
  const sources: Array<{ postType: PostType; collection: string }> = [
    { postType: "forecast", collection: "forecast_requests" },
    { postType: "backtest", collection: "backtests" },
    { postType: "agent", collection: "agent_runs" },
    { postType: "screener", collection: "screener_runs" },
  ];
  const checked: Record<string, number> = {};
  for (const source of sources) {
    let snapshot: admin.firestore.QuerySnapshot;
    try {
      snapshot = await db.collection(source.collection).orderBy("createdAt", "desc").limit(limitPerCollection).get();
    } catch (_error) {
      snapshot = await db.collection(source.collection).limit(limitPerCollection).get();
    }
    checked[source.collection] = snapshot.size;
    for (const doc of snapshot.docs) {
      try {
        await createPostFromResult(source.postType, doc.id, (doc.data() || {}) as Record<string, unknown>);
      } catch (error) {
        console.error("[Explore] post reconciliation failed", {
          collection: source.collection,
          sourceId: doc.id,
          error,
        });
      }
    }
  }
  return checked;
}

ROUTES.all("/internal/cron/:jobName", async (req, res) => {
  if (!cronRequestAuthorized(req)) {
    res.status(401).json({ ok: false, error: "unauthorized" });
    return;
  }
  try {
    const jobName = sanitizeText(req.params.jobName, 100);
    if (jobName === "reconcile-posts") {
      const checked = await reconcileCreatedPosts();
      res.status(200).json({ ok: true, job: jobName, checked });
      return;
    }
    if (jobName === "autopilot-reconcile") {
      await reconcileAutopilotRuns({});
      res.status(200).json({ ok: true, job: jobName });
      return;
    }
    if (jobName === "automation-schedules") {
      await runAutomationSchedules({});
      res.status(200).json({ ok: true, job: jobName });
      return;
    }
    if (jobName === "fiscaldata-refresh") {
      await refreshFiscaldataDefaults({});
      res.status(200).json({ ok: true, job: jobName });
      return;
    }
    res.status(404).json({ ok: false, error: "job_not_found" });
  } catch (error) {
    console.error("[Explore] cron job failed", { jobName: req.params.jobName, error });
    res.status(500).json({ ok: false, error: "job_failed" });
  }
});

app.use("/api", ROUTES);
app.use("/", ROUTES);

app.use((_req, res) => {
  res.status(404).json({ error: "not_found" });
});

app.use((error: any, _req: Request, res: Response, _next: any) => {
  console.error("[Explore] unhandled route error", error);
  res.status(500).json({ error: "internal_error" });
});

export const quanturaExploreApi = app;

async function handleCreateTrigger(postType: PostType, cloudEvent: any): Promise<void> {
  try {
    const docPath = parseDocumentPath(cloudEvent);
    if (!docPath) {
      console.warn(`[Explore] ${postType} trigger missing doc path`);
      return;
    }
    const sourceDocId = docPath.split("/").pop() || "";
    if (!sourceDocId) return;

    const fields = cloudEvent?.data?.value?.fields;
    const payload = decodeFirestoreFields(fields || {});

    if (!Object.keys(payload).length) {
      console.warn(`[Explore] ${postType} trigger had empty payload: ${docPath}`);
      return;
    }

    if (!payload.createdAt && cloudEvent?.data?.value?.createTime) {
      payload.createdAt = cloudEvent.data.value.createTime;
    }

    await createPostFromResult(postType, sourceDocId, payload);
  } catch (error) {
    console.error(`[Explore] ${postType} trigger failed`, error);
  }
}

export async function onForecastCreated(cloudEvent: any): Promise<void> {
  await handleCreateTrigger("forecast", cloudEvent);
}

export async function onBacktestCreated(cloudEvent: any): Promise<void> {
  await handleCreateTrigger("backtest", cloudEvent);
}

export async function onAgentRunCreated(cloudEvent: any): Promise<void> {
  await handleCreateTrigger("agent", cloudEvent);
}

export async function onScreenerRunCreated(cloudEvent: any): Promise<void> {
  await handleCreateTrigger("screener", cloudEvent);
}

export async function refreshFiscaldataDefaults(_cloudEvent: any): Promise<void> {
  await runScheduledFiscaldataRefresh({ db });
}

export async function reconcileAutopilotRuns(_cloudEvent: any): Promise<void> {
  const statuses = ["queued", "running", "transforming", "analysis_ready"];
  try {
    const snap = await db.collection("autopilot_requests").where("status", "in", statuses).limit(24).get();
    for (const doc of snap.docs) {
      try {
        await reconcileAutopilotRunDocument(doc.id, (doc.data() || {}) as Record<string, unknown>);
      } catch (error) {
        console.error("[Autopilot] scheduled reconcile failed", { runId: doc.id, error });
      }
    }
  } catch (error) {
    console.error("[Autopilot] reconcile job failed", error);
  }
}

export async function runAutomationSchedules(_cloudEvent: any): Promise<void> {
  try {
    const weekday = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      weekday: "short",
    }).format(new Date());
    if (weekday === "Sat" || weekday === "Sun") {
      return;
    }
    const nowMs = Date.now();
    const snap = await db.collection(AUTOMATION_COLLECTION).where("active", "==", true).get();
    for (const doc of snap.docs) {
      const data = (doc.data() || {}) as Record<string, unknown>;
      const ownerUid = sanitizeText(data.ownerUid, 220);
      if (!ownerUid) continue;
      const nextRunAtMs = getOptionalTimestampMs(data.nextRunAt);
      if (nextRunAtMs != null && nextRunAtMs > nowMs) continue;
      try {
        const entitlement = await readAutomationEntitlement(ownerUid);
        if (!asBoolean(entitlement.automationUnlocked, false)) continue;
        const lastRunId = sanitizeText(data.lastRunId, 220);
        if (lastRunId) {
          const currentRun = await readAutopilotRunForOwner(ownerUid, lastRunId);
          const currentStatus = sanitizeText(asPlainObject(currentRun?.data || {}).status, 60).toLowerCase();
          if (ACTIVE_FOUNDRY_STATUSES.has(currentStatus)) continue;
        }
        await startAutomationForecastRun(ownerUid, doc.id, data, "scheduled");
      } catch (error) {
        console.error("[Automation] scheduled run failed", { automationId: doc.id, error });
      }
    }
  } catch (error) {
    console.error("[Automation] scheduler failed", error);
  }
}
